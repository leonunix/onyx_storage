use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::engine::VolumeAliveFlag;
use crate::error::{OnyxError, OnyxResult};
use crate::metrics::{EngineMetrics, VolumeMetrics};
use crate::types::{Lba, BLOCK_SIZE};
use crate::zone::manager::ZoneManager;

/// Per-volume IO handle (librbd-style).
///
/// Thread-safe: multiple threads can call read/write concurrently.
/// Reads are dispatched through the ZoneManager; aligned writes go directly to
/// the durable write buffer fast path.
///
/// Each handle has its own `alive` flag (Arc<AtomicBool>) which is set to
/// false when delete_volume() is called. Once dead, the flag is never reset —
/// even if a same-name volume is later recreated, this handle stays invalid.
/// Callers must open a fresh handle via `engine.open_volume()`.
pub struct OnyxVolume {
    vol_id: String,
    size_bytes: u64,
    /// Volume generation epoch, passed to buffer entries so the flusher can
    /// detect and discard stale entries from a prior generation.
    created_at: u64,
    zone_manager: Arc<ZoneManager>,
    alive: VolumeAliveFlag,
    /// Cached per-volume lifecycle lock — avoids global Mutex<HashMap> lookup on every IO.
    vol_lock: Arc<RwLock<()>>,
    metrics: Arc<EngineMetrics>,
    vol_metrics: Arc<VolumeMetrics>,
}

impl OnyxVolume {
    pub(crate) fn new(
        vol_id: String,
        size_bytes: u64,
        created_at: u64,
        zone_manager: Arc<ZoneManager>,
        alive: VolumeAliveFlag,
        vol_lock: Arc<RwLock<()>>,
        metrics: Arc<EngineMetrics>,
    ) -> Self {
        let vol_metrics = metrics.get_volume_metrics(&vol_id);
        Self {
            vol_id,
            size_bytes,
            created_at,
            zone_manager,
            alive,
            vol_lock,
            metrics,
            vol_metrics,
        }
    }

    /// Check if this handle is still valid. Returns Err(VolumeDeleted) if the
    /// volume was deleted after this handle was opened.
    fn check_alive(&self) -> OnyxResult<()> {
        if !self.alive.load(Ordering::Acquire) {
            Err(OnyxError::VolumeDeleted(self.vol_id.clone()))
        } else {
            Ok(())
        }
    }

    pub fn name(&self) -> &str {
        &self.vol_id
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// Discard (TRIM) a byte range. Unmaps the LBAs and frees physical space.
    ///
    /// Only full 4KB blocks within the range are discarded. Sub-block
    /// head/tail bytes are ignored (partial-block DISCARD is a no-op).
    pub fn discard(&self, offset_bytes: u64, len: u64) -> OnyxResult<()> {
        if len == 0 {
            return Ok(());
        }
        if offset_bytes + len > self.size_bytes {
            return Err(OnyxError::OutOfBounds {
                offset: offset_bytes,
                len,
                size: self.size_bytes,
            });
        }

        let _guard = self.vol_lock.read().unwrap();
        self.check_alive()?;

        let bs = BLOCK_SIZE as u64;
        // Round start up to next block boundary, round end down
        let start_lba = Lba(offset_bytes.div_ceil(bs));
        let end_lba = Lba((offset_bytes + len) / bs);

        if end_lba.0 <= start_lba.0 {
            // Range doesn't cover any full block
            return Ok(());
        }

        let lba_count = (end_lba.0 - start_lba.0) as u32;
        self.zone_manager
            .submit_discard(&self.vol_id, start_lba, lba_count)?;

        self.metrics
            .volume_discard_ops
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .volume_discard_lbas
            .fetch_add(lba_count as u64, Ordering::Relaxed);

        Ok(())
    }

    /// Write data at a byte offset. Handles alignment automatically.
    ///
    /// - Block-aligned writes go directly to the write buffer fast path.
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

        let start = Instant::now();
        let bs = BLOCK_SIZE as u64;
        if offset_bytes % bs == 0 && len % bs == 0 {
            self.check_alive()?;
            let start_lba = Lba(offset_bytes / bs);
            let lba_count = (len / bs) as u32;
            self.zone_manager.submit_write(
                &self.vol_id,
                start_lba,
                lba_count,
                data,
                self.created_at,
            )?;
            self.metrics
                .volume_write_ops
                .fetch_add(1, Ordering::Relaxed);
            self.metrics
                .volume_write_bytes
                .fetch_add(data.len() as u64, Ordering::Relaxed);
            self.metrics
                .volume_write_total_ns
                .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            self.vol_metrics.write_ops.fetch_add(1, Ordering::Relaxed);
            self.vol_metrics
                .write_bytes
                .fetch_add(data.len() as u64, Ordering::Relaxed);
            return Ok(());
        }

        let _guard = self.vol_lock.read().unwrap();
        let result = self.write_locked(offset_bytes, data);
        if result.is_ok() {
            self.metrics
                .volume_write_total_ns
                .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
        result
    }

    /// Read `len` bytes from a byte offset. Unmapped blocks return zeros.
    pub fn read(&self, offset_bytes: u64, len: usize) -> OnyxResult<Vec<u8>> {
        let start = Instant::now();
        let _guard = self.vol_lock.read().unwrap();
        let result = self.read_locked(offset_bytes, len);
        if result.is_ok() {
            self.metrics
                .volume_read_total_ns
                .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
        result
    }

    fn write_locked(&self, offset_bytes: u64, data: &[u8]) -> OnyxResult<()> {
        self.check_alive()?;
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
            self.zone_manager.submit_write(
                &self.vol_id,
                start_lba,
                lba_count,
                data,
                self.created_at,
            )?;
            self.metrics
                .volume_write_ops
                .fetch_add(1, Ordering::Relaxed);
            self.metrics
                .volume_write_bytes
                .fetch_add(data.len() as u64, Ordering::Relaxed);
            return Ok(());
        }

        self.metrics
            .volume_partial_write_ops
            .fetch_add(1, Ordering::Relaxed);

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
                self.zone_manager.submit_write(
                    &self.vol_id,
                    block_lba,
                    1,
                    &data[buf_offset..buf_offset + write_len],
                    self.created_at,
                )?;
            } else {
                // Partial block — read-modify-write
                let mut block = match self.zone_manager.submit_read_with_generation(
                    &self.vol_id,
                    block_lba,
                    self.created_at,
                )? {
                    Some(d) => {
                        let mut b = d;
                        b.resize(BLOCK_SIZE as usize, 0);
                        b
                    }
                    None => vec![0u8; BLOCK_SIZE as usize],
                };
                block[offset_in_block..offset_in_block + write_len]
                    .copy_from_slice(&data[buf_offset..buf_offset + write_len]);
                self.zone_manager.submit_write(
                    &self.vol_id,
                    block_lba,
                    1,
                    &block,
                    self.created_at,
                )?;
            }

            buf_offset += write_len;
            cur_offset += write_len as u64;
            remaining -= write_len as u64;
        }

        self.metrics
            .volume_write_ops
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .volume_write_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.vol_metrics.write_ops.fetch_add(1, Ordering::Relaxed);
        self.vol_metrics
            .write_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);

        Ok(())
    }

    fn read_locked(&self, offset_bytes: u64, len: usize) -> OnyxResult<Vec<u8>> {
        self.check_alive()?;
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
        if offset_bytes % bs != 0 || len64 % bs != 0 {
            self.metrics
                .volume_partial_read_ops
                .fetch_add(1, Ordering::Relaxed);
        }
        let mut result = vec![0u8; len];
        let mut buf_offset = 0usize;
        let mut remaining = len64;
        let mut cur_offset = offset_bytes;

        while remaining > 0 {
            let block_lba = Lba(cur_offset / bs);
            let offset_in_block = (cur_offset % bs) as usize;
            let avail = BLOCK_SIZE as usize - offset_in_block;
            let copy_len = (remaining as usize).min(avail);

            match self.zone_manager.submit_read_with_generation(
                &self.vol_id,
                block_lba,
                self.created_at,
            )? {
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

        self.metrics.volume_read_ops.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .volume_read_bytes
            .fetch_add(len as u64, Ordering::Relaxed);
        self.vol_metrics.read_ops.fetch_add(1, Ordering::Relaxed);
        self.vol_metrics
            .read_bytes
            .fetch_add(len as u64, Ordering::Relaxed);

        Ok(result)
    }
}
