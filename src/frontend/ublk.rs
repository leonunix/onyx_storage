// ublk frontend: exposes a Linux block device via the ublk kernel module.
// This module is only compiled on Linux (cfg(target_os = "linux")).

use std::sync::Arc;

use libublk::ctrl::UblkCtrlBuilder;
use libublk::io::{UblkDev, UblkIOCtx, UblkQueue};
use libublk::{sys, UblkError, UblkFlags};

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
        })
    }

    /// Start the ublk device. Blocks until the device is stopped.
    pub fn run(&self) -> OnyxResult<()> {
        let nr_queues = self.config.nr_queues;
        let depth = self.config.queue_depth;
        let io_buf_bytes = self.config.io_buf_bytes;
        let dev_size = self.device_size_bytes;
        let vol_id = self.vol_id.clone();
        let zm = self.zone_manager.clone();

        let sess = UblkCtrlBuilder::default()
            .name("onyx")
            .nr_queues(nr_queues)
            .depth(depth)
            .io_buf_bytes(io_buf_bytes)
            .dev_flags(UblkFlags::empty())
            .build()
            .map_err(|e| OnyxError::Ublk(format!("failed to create ublk ctrl: {:?}", e)))?;

        let tgt_init = move |dev: &mut UblkDev| {
            dev.set_default_params(dev_size);
        };

        let block_size = BLOCK_SIZE as u64;
        let sector_size = SECTOR_SIZE as u64;

        let q_handler = move |q: &UblkQueue, tag: u16, _io: &UblkIOCtx| {
            let iod = q.get_iod(tag);
            let op = unsafe { (*iod).op_flags } & 0xFF;
            let start_sector = unsafe { (*iod).start_sector };
            let nr_sectors = unsafe { (*iod).nr_sectors } as u64;
            let offset_bytes = start_sector * sector_size;
            let io_bytes = nr_sectors * sector_size;

            match op {
                sys::UBLK_IO_OP_READ => {
                    let buf_addr = q.get_io_buf_addr(tag);
                    let mut buf_offset = 0usize;
                    let mut remaining = io_bytes;
                    let mut cur_offset = offset_bytes;

                    while remaining > 0 {
                        let block_lba = Lba(cur_offset / block_size);
                        let offset_in_block = (cur_offset % block_size) as usize;
                        let avail_in_block = block_size as usize - offset_in_block;
                        let copy_len = (remaining as usize).min(avail_in_block);

                        match zm.submit_read(&vol_id, block_lba) {
                            Ok(Some(data)) => {
                                // Mapped block — copy requested slice
                                let src_end = (offset_in_block + copy_len).min(data.len());
                                let actual_copy = src_end.saturating_sub(offset_in_block);
                                unsafe {
                                    if actual_copy > 0 {
                                        std::ptr::copy_nonoverlapping(
                                            data.as_ptr().add(offset_in_block),
                                            buf_addr.add(buf_offset),
                                            actual_copy,
                                        );
                                    }
                                    if actual_copy < copy_len {
                                        std::ptr::write_bytes(
                                            buf_addr.add(buf_offset + actual_copy),
                                            0,
                                            copy_len - actual_copy,
                                        );
                                    }
                                }
                            }
                            Ok(None) => {
                                // Unmapped block — block device returns zeros
                                unsafe {
                                    std::ptr::write_bytes(buf_addr.add(buf_offset), 0, copy_len);
                                }
                            }
                            Err(e) => {
                                // Real I/O error — not unmapped, genuinely broken
                                tracing::error!(lba = block_lba.0, error = %e, "read failed");
                                return -(libc::EIO as i32);
                            }
                        }
                        buf_offset += copy_len;
                        cur_offset += copy_len as u64;
                        remaining -= copy_len as u64;
                    }

                    io_bytes as i32
                }
                sys::UBLK_IO_OP_WRITE => {
                    let buf_addr = q.get_io_buf_addr(tag);
                    let mut buf_offset = 0usize;
                    let mut remaining = io_bytes;
                    let mut cur_offset = offset_bytes;

                    while remaining > 0 {
                        let block_lba = Lba(cur_offset / block_size);
                        let offset_in_block = (cur_offset % block_size) as usize;
                        let avail_in_block = block_size as usize - offset_in_block;
                        let write_len = (remaining as usize).min(avail_in_block);

                        let block_data = if write_len == block_size as usize && offset_in_block == 0
                        {
                            // Full block write — no RMW needed
                            unsafe {
                                std::slice::from_raw_parts(
                                    buf_addr.add(buf_offset),
                                    block_size as usize,
                                )
                                .to_vec()
                            }
                        } else {
                            // Partial block write — read-modify-write
                            let mut block = match zm.submit_read(&vol_id, block_lba) {
                                Ok(Some(data)) => {
                                    let mut b = data;
                                    b.resize(block_size as usize, 0);
                                    b
                                }
                                Ok(None) => {
                                    // Unmapped — start from zeros
                                    vec![0u8; block_size as usize]
                                }
                                Err(e) => {
                                    tracing::error!(lba = block_lba.0, error = %e,
                                        "RMW read failed");
                                    return -(libc::EIO as i32);
                                }
                            };
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    buf_addr.add(buf_offset),
                                    block.as_mut_ptr().add(offset_in_block),
                                    write_len,
                                );
                            }
                            block
                        };

                        if let Err(e) = zm.submit_write(&vol_id, block_lba, block_data) {
                            tracing::error!(lba = block_lba.0, error = %e, "write failed");
                            return -(libc::EIO as i32);
                        }

                        buf_offset += write_len;
                        cur_offset += write_len as u64;
                        remaining -= write_len as u64;
                    }

                    io_bytes as i32
                }
                sys::UBLK_IO_OP_FLUSH => 0,
                sys::UBLK_IO_OP_DISCARD => 0,
                _ => -(libc::ENOTSUP as i32),
            }
        };

        let dev_handler = |_ctrl: &_| {
            tracing::info!("ublk device ready");
        };

        sess.run_target(tgt_init, q_handler, dev_handler)
            .map_err(|e| OnyxError::Ublk(format!("ublk run_target failed: {:?}", e)))?;

        Ok(())
    }
}
