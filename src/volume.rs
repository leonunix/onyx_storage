use std::sync::Arc;

use crate::error::{OnyxError, OnyxResult};
use crate::types::{Lba, BLOCK_SIZE};
use crate::zone::manager::ZoneManager;

/// Per-volume IO handle (librbd-style).
///
/// Thread-safe: multiple threads can call read/write concurrently.
/// The ZoneManager's channel-based dispatch serializes per-zone.
pub struct OnyxVolume {
    vol_id: String,
    size_bytes: u64,
    zone_manager: Arc<ZoneManager>,
}

impl OnyxVolume {
    pub(crate) fn new(
        vol_id: String,
        size_bytes: u64,
        zone_manager: Arc<ZoneManager>,
    ) -> Self {
        Self {
            vol_id,
            size_bytes,
            zone_manager,
        }
    }

    pub fn name(&self) -> &str {
        &self.vol_id
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// Write data at a byte offset. Handles alignment automatically.
    ///
    /// - Block-aligned writes go directly to ZoneManager (fast path).
    /// - Non-aligned writes perform read-modify-write on head/tail blocks.
    pub fn write(&self, offset_bytes: u64, data: &[u8]) -> OnyxResult<()> {
        if data.is_empty() {
            return Ok(());
        }
        let len = data.len() as u64;
        if offset_bytes + len > self.size_bytes {
            return Err(OnyxError::OutOfBounds {
                offset: offset_bytes,
                len,
                size: self.size_bytes,
            });
        }

        let bs = BLOCK_SIZE as u64;

        // Fast path: fully block-aligned
        if offset_bytes % bs == 0 && len % bs == 0 {
            let start_lba = Lba(offset_bytes / bs);
            let lba_count = (len / bs) as u32;
            return self
                .zone_manager
                .submit_write(&self.vol_id, start_lba, lba_count, data.to_vec());
        }

        // Slow path: handle non-aligned head/tail with RMW
        let mut buf_offset = 0usize;
        let mut remaining = len;
        let mut cur_offset = offset_bytes;

        while remaining > 0 {
            let block_lba = Lba(cur_offset / bs);
            let offset_in_block = (cur_offset % bs) as usize;
            let avail = BLOCK_SIZE as usize - offset_in_block;
            let write_len = (remaining as usize).min(avail);

            if offset_in_block == 0 && write_len == BLOCK_SIZE as usize {
                // Full block — no RMW needed
                let chunk = data[buf_offset..buf_offset + write_len].to_vec();
                self.zone_manager
                    .submit_write(&self.vol_id, block_lba, 1, chunk)?;
            } else {
                // Partial block — read-modify-write
                let mut block = match self.zone_manager.submit_read(&self.vol_id, block_lba)? {
                    Some(d) => {
                        let mut b = d;
                        b.resize(BLOCK_SIZE as usize, 0);
                        b
                    }
                    None => vec![0u8; BLOCK_SIZE as usize],
                };
                block[offset_in_block..offset_in_block + write_len]
                    .copy_from_slice(&data[buf_offset..buf_offset + write_len]);
                self.zone_manager
                    .submit_write(&self.vol_id, block_lba, 1, block)?;
            }

            buf_offset += write_len;
            cur_offset += write_len as u64;
            remaining -= write_len as u64;
        }

        Ok(())
    }

    /// Read `len` bytes from a byte offset. Unmapped blocks return zeros.
    pub fn read(&self, offset_bytes: u64, len: usize) -> OnyxResult<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let len64 = len as u64;
        if offset_bytes + len64 > self.size_bytes {
            return Err(OnyxError::OutOfBounds {
                offset: offset_bytes,
                len: len64,
                size: self.size_bytes,
            });
        }

        let bs = BLOCK_SIZE as u64;
        let mut result = vec![0u8; len];
        let mut buf_offset = 0usize;
        let mut remaining = len64;
        let mut cur_offset = offset_bytes;

        while remaining > 0 {
            let block_lba = Lba(cur_offset / bs);
            let offset_in_block = (cur_offset % bs) as usize;
            let avail = BLOCK_SIZE as usize - offset_in_block;
            let copy_len = (remaining as usize).min(avail);

            match self.zone_manager.submit_read(&self.vol_id, block_lba)? {
                Some(data) => {
                    let src_end = (offset_in_block + copy_len).min(data.len());
                    let actual = src_end.saturating_sub(offset_in_block);
                    if actual > 0 {
                        result[buf_offset..buf_offset + actual]
                            .copy_from_slice(&data[offset_in_block..offset_in_block + actual]);
                    }
                    // Any remaining bytes in copy_len stay zero (already initialized)
                }
                None => {
                    // Unmapped — zeros (already initialized)
                }
            }

            buf_offset += copy_len;
            cur_offset += copy_len as u64;
            remaining -= copy_len as u64;
        }

        Ok(result)
    }
}
