// ublk frontend: exposes a Linux block device via the ublk kernel module.
// This module is only compiled on Linux (cfg(target_os = "linux")).

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use libublk::ctrl::{UblkCtrl, UblkCtrlBuilder};
use libublk::io::{BufDescList, UblkDev, UblkIOCtx, UblkQueue};
use libublk::{sys, BufDesc, UblkError, UblkFlags, UblkIORes};

use std::sync::atomic::Ordering;

use crate::config::UblkConfig;
use crate::error::{OnyxError, OnyxResult};
use crate::types::{Lba, VolumeConfig, BLOCK_SIZE, SECTOR_SIZE};
use crate::zone::manager::ZoneManager;

/// ublk target that routes IO through the ZoneManager
pub struct OnyxUblkTarget {
    config: UblkConfig,
    zone_manager: Arc<ZoneManager>,
    vol_id: String,
    device_size_bytes: u64,
    vol_created_at: u64,
}

impl OnyxUblkTarget {
    pub fn new(
        config: &UblkConfig,
        zone_manager: Arc<ZoneManager>,
        vol: &VolumeConfig,
    ) -> OnyxResult<Self> {
        Ok(Self {
            config: config.clone(),
            zone_manager,
            vol_id: vol.id.0.clone(),
            device_size_bytes: vol.size_bytes,
            vol_created_at: vol.created_at,
        })
    }

    /// Kill a running ublk device by its kernel device ID.
    /// Safe to call from any thread — creates a temporary control handle.
    pub fn kill_device(dev_id: u32) -> OnyxResult<()> {
        let ctrl = UblkCtrl::new_simple(dev_id as i32).map_err(|e| {
            OnyxError::Ublk(format!("failed to open ublk ctrl {}: {:?}", dev_id, e))
        })?;
        ctrl.kill_dev()
            .map_err(|e| OnyxError::Ublk(format!("failed to kill ublk dev {}: {:?}", dev_id, e)))?;
        Ok(())
    }

    /// Start the ublk device. Blocks until the device is stopped.
    /// Reports the kernel-assigned device ID via `dev_id_tx` before blocking.
    pub fn run(&self, dev_id_tx: Option<std::sync::mpsc::Sender<u32>>) -> OnyxResult<()> {
        let nr_queues = self.config.nr_queues;
        let depth = self.config.queue_depth;
        let io_buf_bytes = self.config.io_buf_bytes;
        let dev_size = self.device_size_bytes;
        let vol_id = self.vol_id.clone();
        let vol_created_at = self.vol_created_at;
        let zm = self.zone_manager.clone();
        let dev_name = format!("onyx-{}", self.vol_id);

        let sess = UblkCtrlBuilder::default()
            .name(&dev_name)
            .nr_queues(nr_queues)
            .depth(depth)
            .io_buf_bytes(io_buf_bytes)
            .dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV)
            .build()
            .map_err(|e| OnyxError::Ublk(format!("failed to create ublk ctrl: {:?}", e)))?;

        let tgt_init = move |dev: &mut UblkDev| {
            let info = dev.dev_info;
            dev.tgt.dev_size = dev_size;
            dev.tgt.params = sys::ublk_params {
                types: sys::UBLK_PARAM_TYPE_BASIC | sys::UBLK_PARAM_TYPE_DISCARD,
                basic: sys::ublk_param_basic {
                    attrs: sys::UBLK_ATTR_VOLATILE_CACHE,
                    logical_bs_shift: 9,
                    physical_bs_shift: 12,
                    io_opt_shift: 12,
                    io_min_shift: 12,
                    max_sectors: info.max_io_buf_bytes >> 9,
                    dev_sectors: dev_size >> 9,
                    ..Default::default()
                },
                discard: sys::ublk_param_discard {
                    discard_alignment: BLOCK_SIZE as u32,
                    discard_granularity: BLOCK_SIZE as u32,
                    max_discard_sectors: (2 * 1024 * 1024 / SECTOR_SIZE) as u32, // 2MB
                    max_write_zeroes_sectors: 0,
                    max_discard_segments: 1,
                    reserved0: 0,
                },
                ..Default::default()
            };
            Ok::<(), UblkError>(())
        };

        let block_size = BLOCK_SIZE as u64;
        let sector_size = SECTOR_SIZE as u64;

        let q_handler = move |qid: u16, dev: &UblkDev| {
            let bufs = Rc::new(RefCell::new(dev.alloc_queue_io_bufs()));
            let io_bufs = bufs.clone();
            let io_handler = move |q: &UblkQueue, tag: u16, _io: &UblkIOCtx| {
                let iod = q.get_iod(tag);
                let op = iod.op_flags & 0xFF;
                let start_sector = iod.start_sector;
                let nr_sectors = iod.nr_sectors as u64;
                let offset_bytes = start_sector * sector_size;
                let io_bytes = nr_sectors * sector_size;
                let io_len = io_bytes as usize;

                let metrics = zm.metrics();
                let io_start = std::time::Instant::now();
                let res = {
                    let mut bufs = io_bufs.borrow_mut();
                    let io_buf = &mut bufs[tag as usize];
                    let io_slice = io_buf.as_mut_slice();
                    if io_len > io_slice.len() {
                        tracing::error!(
                            tag,
                            io_len,
                            buf_len = io_slice.len(),
                            "ublk request exceeds queue buffer size"
                        );
                        -(libc::EIO as i32)
                    } else {
                        match op {
                            sys::UBLK_IO_OP_READ => {
                                let mut buf_offset = 0usize;
                                let mut remaining = io_bytes;
                                let mut cur_offset = offset_bytes;
                                let mut status = io_bytes as i32;

                                while remaining > 0 {
                                    let block_lba = Lba(cur_offset / block_size);
                                    let offset_in_block = (cur_offset % block_size) as usize;
                                    let avail_in_block = block_size as usize - offset_in_block;
                                    let copy_len = (remaining as usize).min(avail_in_block);

                                    match zm.submit_read_with_generation(
                                        &vol_id,
                                        block_lba,
                                        vol_created_at,
                                    ) {
                                        Ok(Some(data)) => {
                                            let src_end =
                                                (offset_in_block + copy_len).min(data.len());
                                            let actual_copy =
                                                src_end.saturating_sub(offset_in_block);
                                            if actual_copy > 0 {
                                                io_slice[buf_offset..buf_offset + actual_copy]
                                                    .copy_from_slice(
                                                        &data[offset_in_block
                                                            ..offset_in_block + actual_copy],
                                                    );
                                            }
                                            if actual_copy < copy_len {
                                                io_slice[buf_offset + actual_copy
                                                    ..buf_offset + copy_len]
                                                    .fill(0);
                                            }
                                        }
                                        Ok(None) => {
                                            io_slice[buf_offset..buf_offset + copy_len].fill(0);
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                lba = block_lba.0,
                                                error = %e,
                                                "read failed"
                                            );
                                            status = -(libc::EIO as i32);
                                            break;
                                        }
                                    }
                                    buf_offset += copy_len;
                                    cur_offset += copy_len as u64;
                                    remaining -= copy_len as u64;
                                }

                                status
                            }
                            sys::UBLK_IO_OP_WRITE => {
                                let req = &io_slice[..io_len];
                                #[allow(clippy::collapsible_if)]
                                if offset_bytes % block_size == 0 && io_bytes % block_size == 0 {
                                    let start_lba = Lba(offset_bytes / block_size);
                                    let lba_count = (io_bytes / block_size) as u32;
                                    if let Err(e) = zm.submit_write(
                                        &vol_id,
                                        start_lba,
                                        lba_count,
                                        req,
                                        vol_created_at,
                                    ) {
                                        tracing::error!(
                                            lba = start_lba.0,
                                            count = lba_count,
                                            error = %e,
                                            "write failed"
                                        );
                                        -(libc::EIO as i32)
                                    } else {
                                        io_bytes as i32
                                    }
                                } else {
                                    let mut buf_offset = 0usize;
                                    let mut remaining = io_bytes;
                                    let mut cur_offset = offset_bytes;
                                    let mut status = io_bytes as i32;

                                    while remaining > 0 {
                                        let block_lba = Lba(cur_offset / block_size);
                                        let offset_in_block = (cur_offset % block_size) as usize;
                                        let avail_in_block = block_size as usize - offset_in_block;
                                        let write_len = (remaining as usize).min(avail_in_block);

                                        let mut block = match zm.submit_read_with_generation(
                                            &vol_id,
                                            block_lba,
                                            vol_created_at,
                                        ) {
                                            Ok(Some(data)) => {
                                                let mut b = data;
                                                b.resize(block_size as usize, 0);
                                                b
                                            }
                                            Ok(None) => vec![0u8; block_size as usize],
                                            Err(e) => {
                                                tracing::error!(
                                                    lba = block_lba.0,
                                                    error = %e,
                                                    "RMW read failed"
                                                );
                                                status = -(libc::EIO as i32);
                                                break;
                                            }
                                        };

                                        block[offset_in_block..offset_in_block + write_len]
                                            .copy_from_slice(
                                                &req[buf_offset..buf_offset + write_len],
                                            );

                                        if let Err(e) = zm.submit_write(
                                            &vol_id,
                                            block_lba,
                                            1,
                                            &block,
                                            vol_created_at,
                                        ) {
                                            tracing::error!(
                                                lba = block_lba.0,
                                                error = %e,
                                                "write failed"
                                            );
                                            status = -(libc::EIO as i32);
                                            break;
                                        }

                                        buf_offset += write_len;
                                        cur_offset += write_len as u64;
                                        remaining -= write_len as u64;
                                    }

                                    status
                                }
                            }
                            sys::UBLK_IO_OP_FLUSH => 0,
                            sys::UBLK_IO_OP_DISCARD => {
                                let start_lba = Lba(offset_bytes / block_size);
                                let lba_count = (io_bytes / block_size) as u32;
                                if lba_count == 0 {
                                    0
                                } else {
                                    match zm.submit_discard(&vol_id, start_lba, lba_count) {
                                        Ok(()) => io_bytes as i32,
                                        Err(e) => {
                                            tracing::error!(
                                                lba = start_lba.0,
                                                count = lba_count,
                                                error = %e,
                                                "discard failed"
                                            );
                                            -(libc::EIO as i32)
                                        }
                                    }
                                }
                            }
                            _ => -(libc::ENOTSUP as i32),
                        }
                    }
                };

                // Record volume-level IO metrics for successful operations.
                if res > 0 {
                    let bytes = res as u64;
                    let elapsed_ns = io_start.elapsed().as_nanos() as u64;
                    match op {
                        sys::UBLK_IO_OP_READ => {
                            metrics.volume_read_ops.fetch_add(1, Ordering::Relaxed);
                            metrics
                                .volume_read_bytes
                                .fetch_add(bytes, Ordering::Relaxed);
                            metrics
                                .volume_read_total_ns
                                .fetch_add(elapsed_ns, Ordering::Relaxed);
                        }
                        sys::UBLK_IO_OP_WRITE => {
                            metrics.volume_write_ops.fetch_add(1, Ordering::Relaxed);
                            metrics
                                .volume_write_bytes
                                .fetch_add(bytes, Ordering::Relaxed);
                            metrics
                                .volume_write_total_ns
                                .fetch_add(elapsed_ns, Ordering::Relaxed);
                        }
                        _ => {}
                    }
                }

                let bufs = io_bufs.borrow();
                if let Err(err) = q.complete_io_cmd_unified(
                    tag,
                    BufDesc::Slice(bufs[tag as usize].as_slice()),
                    Ok(UblkIORes::Result(res)),
                ) {
                    tracing::error!(error = ?err, "ublk completion failed");
                }
            };

            let queue = match {
                let bufs = bufs.borrow();
                UblkQueue::new(qid, dev)
                    .unwrap()
                    .submit_fetch_commands_unified(BufDescList::Slices(Some(&bufs)))
            } {
                Ok(q) => q,
                Err(e) => {
                    tracing::error!(error = ?e, "submit_fetch_commands_unified failed");
                    return;
                }
            };

            queue.wait_and_handle_io(io_handler);
        };

        let dev_handler = move |ctrl: &UblkCtrl| {
            let id = ctrl.dev_info().dev_id;
            tracing::info!(dev_id = id, "ublk device ready");
            if let Some(tx) = dev_id_tx {
                let _ = tx.send(id);
            }
        };

        sess.run_target(tgt_init, q_handler, dev_handler)
            .map_err(|e| OnyxError::Ublk(format!("ublk run_target failed: {:?}", e)))?;

        Ok(())
    }
}
