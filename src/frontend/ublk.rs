// ublk frontend: exposes a Linux block device via the ublk kernel module.
// This module is only compiled on Linux (cfg(target_os = "linux")).

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use libublk::ctrl::UblkCtrlBuilder;
use libublk::io::{BufDescList, UblkDev, UblkIOCtx, UblkQueue};
use libublk::{sys, BufDesc, UblkError, UblkFlags, UblkIORes};

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

    /// Start the ublk device. Blocks until the device is stopped.
    pub fn run(&self) -> OnyxResult<()> {
        let nr_queues = self.config.nr_queues;
        let depth = self.config.queue_depth;
        let io_buf_bytes = self.config.io_buf_bytes;
        let dev_size = self.device_size_bytes;
        let vol_id = self.vol_id.clone();
        let vol_created_at = self.vol_created_at;
        let zm = self.zone_manager.clone();

        let sess = UblkCtrlBuilder::default()
            .name("onyx")
            .nr_queues(nr_queues)
            .depth(depth)
            .io_buf_bytes(io_buf_bytes)
            .dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV)
            .build()
            .map_err(|e| OnyxError::Ublk(format!("failed to create ublk ctrl: {:?}", e)))?;

        let tgt_init = move |dev: &mut UblkDev| {
            dev.set_default_params(dev_size);
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
                            sys::UBLK_IO_OP_DISCARD => 0,
                            _ => -(libc::ENOTSUP as i32),
                        }
                    }
                };

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

        let dev_handler = |_ctrl: &_| {
            tracing::info!("ublk device ready");
        };

        sess.run_target(tgt_init, q_handler, dev_handler)
            .map_err(|e| OnyxError::Ublk(format!("ublk run_target failed: {:?}", e)))?;

        Ok(())
    }
}
