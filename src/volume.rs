use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::buffer::commit_log::BufferAppendTicket;
use crate::engine::VolumeAliveFlag;
use crate::error::{OnyxError, OnyxResult};
use crate::metrics::{EngineMetrics, VolumeMetrics};
use crate::types::{Lba, BLOCK_SIZE};
use crate::zone::manager::ZoneManager;
use onyx_metadb::VolumeOrdinal;

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
    vol_ord: VolumeOrdinal,
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

/// An aligned volume write that has entered LV2 but is not necessarily durable.
/// Waiting preserves the normal acknowledgement boundary: all covered buffer
/// shards must advance their fdatasync watermark before the write completes.
/// Dropping the ticket abandons acknowledgement and completion metrics; it does
/// not cancel an append that has already entered LV2.
#[must_use = "a deferred write must be waited or explicitly dropped"]
pub struct VolumeWriteTicket {
    tickets: Vec<BufferAppendTicket>,
    metrics: Arc<EngineMetrics>,
    vol_metrics: Arc<VolumeMetrics>,
    bytes: u64,
    volume_started: Instant,
    zone_started: Instant,
}

impl VolumeWriteTicket {
    pub fn is_durable(&self) -> bool {
        self.tickets.iter().all(BufferAppendTicket::is_durable)
    }

    /// Abandon frontend acknowledgement without waiting for durability.
    ///
    /// The LV2 append has already been accepted by the engine and is not
    /// cancelled by this operation. The normal engine shutdown/recovery path
    /// remains responsible for it; callers must report the write as failed or
    /// indeterminate rather than successful.
    pub(crate) fn abandon(self) {}

    /// Delay between the last covered shard becoming durable and an async
    /// frontend observing the completed volume write.
    pub(crate) fn completion_dispatch_delay_ns(&self, observed_at: Instant) -> Option<u64> {
        if self.tickets.is_empty() || !self.is_durable() {
            return None;
        }
        let mut min_delay = u64::MAX;
        for ticket in &self.tickets {
            min_delay = min_delay.min(ticket.completion_dispatch_delay_ns(observed_at)?);
        }
        Some(min_delay)
    }

    /// Register an edge-coalesced completion wakeup. The return value is true
    /// when all shard tickets are already durable; otherwise at least one
    /// future watermark advance will notify `tx`.
    pub fn arm_wakeup(&self, tx: &crossbeam_channel::Sender<()>) -> bool {
        for ticket in &self.tickets {
            ticket.arm_wakeup(tx);
        }
        self.is_durable()
    }

    pub fn wait(self) {
        let Self {
            tickets,
            metrics,
            vol_metrics,
            bytes,
            volume_started,
            zone_started,
        } = self;
        for ticket in tickets {
            ticket.wait();
        }
        Self::record_completion(&metrics, &vol_metrics, bytes, volume_started, zone_started);
    }

    /// Complete a ticket that has already reached its durability watermark.
    /// This is the non-blocking reap path used by asynchronous frontends. A
    /// premature caller falls back to the normal durable wait rather than
    /// acknowledging an unsynced write.
    pub fn finish(self) {
        if !self.is_durable() {
            self.wait();
            return;
        }
        let Self {
            tickets,
            metrics,
            vol_metrics,
            bytes,
            volume_started,
            zone_started,
        } = self;
        for ticket in tickets {
            ticket.finish_dispatched();
        }
        Self::record_completion(&metrics, &vol_metrics, bytes, volume_started, zone_started);
    }

    fn record_completion(
        metrics: &EngineMetrics,
        vol_metrics: &VolumeMetrics,
        bytes: u64,
        volume_started: Instant,
        zone_started: Instant,
    ) {
        let volume_elapsed_ns = volume_started.elapsed().as_nanos() as u64;
        let zone_elapsed_ns = zone_started.elapsed().as_nanos() as u64;
        metrics.volume_write_ops.fetch_add(1, Ordering::Relaxed);
        metrics
            .volume_write_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        metrics
            .volume_write_total_ns
            .fetch_add(volume_elapsed_ns, Ordering::Relaxed);
        metrics
            .zone_submit_write_ns
            .fetch_add(zone_elapsed_ns, Ordering::Relaxed);
        vol_metrics.write_ops.fetch_add(1, Ordering::Relaxed);
        vol_metrics.write_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

impl OnyxVolume {
    pub(crate) fn new(
        vol_id: String,
        vol_ord: VolumeOrdinal,
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
            vol_ord,
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
        if offset_bytes
            .checked_add(len)
            .is_none_or(|end| end > self.size_bytes)
        {
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
            self.write_aligned_deferred_started(offset_bytes, data, start)?
                .wait();
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

    /// Submit one block-aligned write without waiting for LV2 durability.
    /// Call [`VolumeWriteTicket::wait`] to reach the same completion boundary
    /// as [`Self::write`]. This is intended for asynchronous frontends and
    /// load generators that maintain multiple durable writes in flight.
    pub fn write_aligned_deferred(
        &self,
        offset_bytes: u64,
        data: &[u8],
    ) -> OnyxResult<VolumeWriteTicket> {
        self.write_aligned_deferred_started(offset_bytes, data, Instant::now())
    }

    fn write_aligned_deferred_started(
        &self,
        offset_bytes: u64,
        data: &[u8],
        volume_started: Instant,
    ) -> OnyxResult<VolumeWriteTicket> {
        if data.is_empty() {
            return Err(OnyxError::Config(
                "deferred aligned write data must not be empty".into(),
            ));
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
        if offset_bytes % bs != 0 || len % bs != 0 {
            return Err(OnyxError::Config(format!(
                "deferred write requires {BLOCK_SIZE}-byte aligned offset and length"
            )));
        }

        self.check_alive()?;
        let zone_started = Instant::now();
        let tickets = self.zone_manager.submit_write_deferred(
            &self.vol_id,
            Lba(offset_bytes / bs),
            (len / bs) as u32,
            data,
            self.created_at,
        )?;
        Ok(VolumeWriteTicket {
            tickets,
            metrics: self.metrics.clone(),
            vol_metrics: self.vol_metrics.clone(),
            bytes: len,
            volume_started,
            zone_started,
        })
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

    /// Read into a caller-owned buffer. Unmapped blocks return zeros.
    ///
    /// This avoids one allocation per read for benchmark/protocol-front-end
    /// paths that already own an IO buffer.
    pub fn read_into(&self, offset_bytes: u64, out: &mut [u8]) -> OnyxResult<()> {
        let start = Instant::now();
        let _guard = self.vol_lock.read().unwrap();
        let result = self.read_locked_into(offset_bytes, out);
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
        let mut result = vec![0u8; len];
        self.read_locked_into(offset_bytes, &mut result)?;
        Ok(result)
    }

    fn read_locked_into(&self, offset_bytes: u64, result: &mut [u8]) -> OnyxResult<()> {
        self.check_alive()?;
        let len = result.len();
        if len == 0 {
            return Ok(());
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
        result.fill(0);

        // Fast path: block-aligned → one vectorized call. Unit coalescing on
        // the backend means one io_uring read + one decompress per unique
        // compression unit, regardless of how many LBAs the read spans.
        if offset_bytes % bs == 0 && len64 % bs == 0 {
            let start_lba = Lba(offset_bytes / bs);
            let lba_count = (len64 / bs) as u32;
            self.zone_manager.submit_reads_with_ordinal(
                &self.vol_id,
                Some(self.vol_ord),
                start_lba,
                lba_count,
                self.created_at,
                result,
            )?;
        } else {
            self.metrics
                .volume_partial_read_ops
                .fetch_add(1, Ordering::Relaxed);

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
                    }
                    None => {
                        // Unmapped — zeros (already initialized)
                    }
                }

                buf_offset += copy_len;
                cur_offset += copy_len as u64;
                remaining -= copy_len as u64;
            }
        }

        self.metrics.volume_read_ops.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .volume_read_bytes
            .fetch_add(len as u64, Ordering::Relaxed);
        self.vol_metrics.read_ops.fetch_add(1, Ordering::Relaxed);
        self.vol_metrics
            .read_bytes
            .fetch_add(len as u64, Ordering::Relaxed);

        Ok(())
    }
}
