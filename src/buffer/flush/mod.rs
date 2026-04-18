use std::collections::{HashMap, VecDeque};
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, unbounded, Receiver, Sender};

use crate::buffer::pipeline::{coalesce_pending, CoalesceUnit, CompressedUnit};
use crate::buffer::pool::WriteBufferPool;
use crate::compress::codec::create_compressor;
use crate::config::FlushConfig;
use crate::dedup::config::DedupConfig;
use crate::error::OnyxResult;
use crate::io::engine::IoEngine;
use crate::lifecycle::VolumeLifecycleManager;
use crate::meta::schema::{BlockmapValue, ContentHash, DedupEntry, FLAG_DEDUP_SKIPPED};
use crate::meta::store::{DedupHitResult, MetaStore};
use crate::metrics::EngineMetrics;
use crate::packer::packer::{PackResult, Packer, SealedSlot};
use crate::space::allocator::SpaceAllocator;
use crate::space::extent::Extent;
use crate::types::{CompressionAlgo, Lba, Pba, VolumeId, BLOCK_SIZE};

/// 3-stage flusher pipeline:
///   Stage 1 (coalescer): drain ready queue → filter in-flight → coalesce → dispatch
///   Stage 2 (N compress workers): parallel compression
///   Stage 3 (writer): write LV3 → update metadata → report completed seqs
///
/// The coalescer maintains an in-flight set of seq numbers currently being
/// processed by stages 2+3. This prevents the same entry from being dispatched
/// twice when the coalescer loops faster than the writer commits.
pub struct BufferFlusher {
    running: Arc<AtomicBool>,
    lanes: Vec<FlusherLane>,
    in_flight: Arc<FlusherInFlightTracker>,
}

struct FlusherLane {
    coalesce_handle: Option<JoinHandle<()>>,
    dedup_handles: Vec<JoinHandle<()>>,
    compress_handles: Vec<JoinHandle<()>>,
    writer_handle: Option<JoinHandle<()>>,
    cleanup_handle: Option<JoinHandle<()>>,
}

#[derive(Debug, Clone)]
struct ActiveSeq {
    vol_id: String,
    vol_created_at: u64,
}

struct PackedSlotRetry {
    sealed: SealedSlot,
    buffered_seqs: Vec<u64>,
    buffered_completions: Vec<Arc<crate::buffer::pipeline::DedupCompletion>>,
    retry_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EnqueuePendingSeq {
    Queued,
    Skipped,
    WindowFull,
}

#[derive(Default)]
struct FlusherInFlightTracker {
    active: Mutex<HashMap<u64, ActiveSeq>>,
    retry_after: Mutex<HashMap<u64, Instant>>,
    cv: Condvar,
}

impl FlusherInFlightTracker {
    fn track_seq_start(&self, seq: u64, vol_id: &str, vol_created_at: u64) {
        let mut active = self.active.lock().unwrap();
        active.entry(seq).or_insert_with(|| ActiveSeq {
            vol_id: vol_id.to_string(),
            vol_created_at,
        });
    }

    fn track_seq_done(&self, seq: u64) {
        let mut active = self.active.lock().unwrap();
        if active.remove(&seq).is_some() {
            self.cv.notify_all();
        }
    }

    fn defer_retry(&self, seqs: &[u64], delay: Duration) {
        if seqs.is_empty() {
            return;
        }
        let deadline = Instant::now() + delay;
        let mut retry_after = self.retry_after.lock().unwrap();
        for seq in seqs {
            retry_after.insert(*seq, deadline);
        }
    }

    fn retry_ready(&self, seq: u64) -> bool {
        let mut retry_after = self.retry_after.lock().unwrap();
        match retry_after.get(&seq).copied() {
            Some(deadline) if deadline > Instant::now() => false,
            Some(_) => {
                retry_after.remove(&seq);
                true
            }
            None => true,
        }
    }

    fn wait_volume_generation_idle(
        &self,
        vol_id: &str,
        vol_created_at: u64,
        timeout: Duration,
    ) -> bool {
        let deadline = Instant::now() + timeout;
        let mut active = self.active.lock().unwrap();
        loop {
            let still_active = active
                .values()
                .any(|seq| seq.vol_id == vol_id && seq.vol_created_at == vol_created_at);
            if !still_active {
                return true;
            }

            let now = Instant::now();
            if now >= deadline {
                return false;
            }

            let wait = deadline.saturating_duration_since(now);
            let (guard, _) = self.cv.wait_timeout(active, wait).unwrap();
            active = guard;
        }
    }
}

static PBA_LOCKS: OnceLock<Mutex<HashMap<Pba, Arc<Mutex<()>>>>> = OnceLock::new();
#[cfg(test)]
static CLEANUP_FREE_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);

mod cleanup;
mod failpoints;
mod stages;
mod writer;

#[cfg(test)]
mod tests;

pub use failpoints::{
    clear_test_dedup_hit_failpoint, clear_test_failpoint, clear_test_packed_pause_hook,
    install_test_dedup_hit_failpoint, install_test_failpoint, install_test_packed_pause_hook,
    release_test_packed_pause_hook, wait_for_test_packed_pause_hit, FlushFailStage,
    PackedPauseState,
};

use failpoints::{
    maybe_inject_dedup_hit_failure, maybe_inject_test_failure, maybe_inject_test_failure_packed,
    maybe_pause_before_packed_meta_write,
};

enum Allocation {
    Single(Pba),
    Extent(Extent),
}

impl Allocation {
    fn start_pba(&self) -> Pba {
        match self {
            Self::Single(pba) => *pba,
            Self::Extent(extent) => extent.start,
        }
    }

    fn block_count(&self) -> u32 {
        match self {
            Self::Single(_) => 1,
            Self::Extent(extent) => extent.count,
        }
    }

    fn free(&self, allocator: &SpaceAllocator) -> OnyxResult<()> {
        match self {
            Self::Single(pba) => allocator.free_one(*pba),
            Self::Extent(extent) => allocator.free_extent(*extent),
        }
    }
}

impl BufferFlusher {
    const HEAD_RETRY_AGE_THRESHOLD: Duration = Duration::from_millis(500);
    const COALESCE_READY_WINDOW_BYTES: usize = 16 * 1024 * 1024;

    /// compress_workers / dedup.workers are now **per-lane** counts.
    /// No division — each lane gets the configured number of workers.
    fn per_lane_worker_count(configured: usize, _lane_count: usize) -> usize {
        configured.max(1)
    }

    fn elapsed_ns(start: Instant) -> u64 {
        start.elapsed().as_nanos().min(u64::MAX as u128) as u64
    }

    fn record_elapsed(counter: &std::sync::atomic::AtomicU64, start: Instant) {
        counter.fetch_add(Self::elapsed_ns(start), Ordering::Relaxed);
    }

    /// Once blockmap/refcount metadata has committed, reclaiming the old PBA is
    /// strictly best-effort. Failing this cleanup must not turn the flush into a
    /// retry loop, or the buffer head will stay pinned behind work that already
    /// committed successfully.
    fn latest_seq_for_lba(seq_lba_ranges: &[(u64, Lba, u32)], lba: Lba) -> u64 {
        seq_lba_ranges
            .iter()
            .filter_map(|(seq, start, count)| {
                (lba.0 >= start.0 && lba.0 < start.0 + *count as u64).then_some(*seq)
            })
            .max()
            .unwrap_or(0)
    }

    fn pending_entry_bytes(entry: &crate::buffer::commit_log::PendingEntry) -> usize {
        entry.lba_count as usize * BLOCK_SIZE as usize
    }

    fn try_enqueue_pending_seq(
        seq: u64,
        pool: &WriteBufferPool,
        in_flight: &HashMap<u64, u32>,
        in_flight_tracker: &FlusherInFlightTracker,
        seen: &mut std::collections::HashSet<u64>,
        queued_bytes: &mut usize,
        new_entries: &mut Vec<Arc<crate::buffer::commit_log::PendingEntry>>,
        metrics: &EngineMetrics,
        skip_fully_superseded: bool,
    ) -> EnqueuePendingSeq {
        if in_flight.contains_key(&seq) || !in_flight_tracker.retry_ready(seq) || !seen.insert(seq)
        {
            return EnqueuePendingSeq::Skipped;
        }

        let Some(meta) = pool.get_pending_arc(seq) else {
            return EnqueuePendingSeq::Skipped;
        };

        // Fast-path drop: if every LBA in this entry has already been
        // superseded by a later seq still in the ring, the writer would
        // discard it at the very end anyway — we just do all the hashing /
        // compression / dedup_index churn in between for nothing. Retire
        // this seq now so the ring tail can advance past it.
        if skip_fully_superseded
            && pool.is_entry_fully_superseded(
                &meta.vol_id,
                meta.start_lba,
                meta.lba_count,
                seq,
                meta.vol_created_at,
            )
        {
            if let Err(err) = pool.mark_flushed(seq, meta.start_lba, meta.lba_count) {
                tracing::warn!(
                    seq,
                    vol = %meta.vol_id,
                    error = %err,
                    "mark_flushed failed for superseded entry; falling back to full flush"
                );
                // Fall through and enqueue normally so nothing is lost.
            } else {
                metrics
                    .coalesce_superseded_entries
                    .fetch_add(1, Ordering::Relaxed);
                metrics
                    .coalesce_superseded_lbas
                    .fetch_add(meta.lba_count as u64, Ordering::Relaxed);
                return EnqueuePendingSeq::Skipped;
            }
        }

        let estimated_bytes = Self::pending_entry_bytes(meta.as_ref());
        if !new_entries.is_empty()
            && queued_bytes.saturating_add(estimated_bytes) > Self::COALESCE_READY_WINDOW_BYTES
        {
            return EnqueuePendingSeq::WindowFull;
        }

        if let Some(entry) = pool.pending_entry_arc(seq) {
            *queued_bytes = queued_bytes.saturating_add(Self::pending_entry_bytes(entry.as_ref()));
            new_entries.push(entry);
            EnqueuePendingSeq::Queued
        } else {
            EnqueuePendingSeq::Skipped
        }
    }

    fn live_positions_for_unit(
        unit: &CompressedUnit,
        pool: &WriteBufferPool,
    ) -> OnyxResult<Vec<usize>> {
        let mut live = Vec::with_capacity(unit.lba_count as usize);
        for idx in 0..unit.lba_count as usize {
            let lba = Lba(unit.start_lba.0 + idx as u64);
            let latest_seq = Self::latest_seq_for_lba(&unit.seq_lba_ranges, lba);
            if pool.is_latest_lba_seq(&unit.vol_id, lba, latest_seq, unit.vol_created_at) {
                live.push(idx);
            }
        }
        Ok(live)
    }

    pub fn start(
        pool: Arc<WriteBufferPool>,
        meta: Arc<MetaStore>,
        lifecycle: Arc<VolumeLifecycleManager>,
        allocator: Arc<SpaceAllocator>,
        io_engine: Arc<IoEngine>,
        config: &FlushConfig,
        dedup_config: &DedupConfig,
    ) -> Self {
        Self::start_with_metrics(
            pool,
            meta,
            lifecycle,
            allocator,
            io_engine,
            config,
            dedup_config,
            Arc::new(EngineMetrics::default()),
        )
    }

    pub fn start_with_metrics(
        pool: Arc<WriteBufferPool>,
        meta: Arc<MetaStore>,
        lifecycle: Arc<VolumeLifecycleManager>,
        allocator: Arc<SpaceAllocator>,
        io_engine: Arc<IoEngine>,
        config: &FlushConfig,
        dedup_config: &DedupConfig,
        metrics: Arc<EngineMetrics>,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let in_flight = Arc::new(FlusherInFlightTracker::default());
        let lane_count = pool.shard_count().max(1);
        let compress_workers =
            Self::per_lane_worker_count(config.compress_workers.max(1), lane_count);
        let max_raw = config.coalesce_max_raw_bytes;
        let max_lbas = config.coalesce_max_lbas;
        let dedup_enabled = dedup_config.enabled;
        let dedup_workers = Self::per_lane_worker_count(dedup_config.workers.max(1), lane_count);
        let dedup_skip_threshold = dedup_config.buffer_skip_threshold_pct;
        let mut lanes = Vec::with_capacity(lane_count);

        for shard_idx in 0..lane_count {
            // Stage 1 → Stage 1.5 (dedup) or Stage 2 (compress)
            let (dedup_tx, dedup_rx) = bounded::<CoalesceUnit>(dedup_workers * 4);
            // Stage 1.5 → Stage 2
            let (compress_tx, compress_rx) = bounded::<CoalesceUnit>(compress_workers * 4);
            // Stage 2 → Stage 3
            let (write_tx, write_rx) = bounded::<CompressedUnit>(compress_workers * 4);
            // Stage 3 → Stage 1 (feedback: completed seqs)
            let (done_tx, done_rx) = unbounded::<Vec<u64>>();
            // Writer/dedup → cleanup thread (async dead PBA reclamation)
            let (cleanup_tx, cleanup_rx) = unbounded::<Vec<(Pba, u32)>>();

            let running_c = running.clone();
            let pool_c = pool.clone();
            let meta_c = meta.clone();
            let metrics_c = metrics.clone();
            let in_flight_c = in_flight.clone();
            let coalesce_out_tx = if dedup_enabled {
                dedup_tx.clone()
            } else {
                compress_tx.clone()
            };
            let coalesce_handle = thread::Builder::new()
                .name(format!("flusher-coalesce-{}", shard_idx))
                .spawn(move || {
                    Self::coalesce_loop(
                        shard_idx,
                        &pool_c,
                        &meta_c,
                        &coalesce_out_tx,
                        &done_rx,
                        &running_c,
                        &in_flight_c,
                        &metrics_c,
                        max_raw,
                        max_lbas,
                    );
                })
                .expect("failed to spawn coalescer thread");

            let mut dedup_handles = Vec::new();
            if dedup_enabled {
                for worker_idx in 0..dedup_workers {
                    let rx = dedup_rx.clone();
                    let miss_tx = compress_tx.clone();
                    let running_d = running.clone();
                    let meta_d = meta.clone();
                    let pool_d = pool.clone();
                    let lifecycle_d = lifecycle.clone();
                    let allocator_d = allocator.clone();
                    let done_tx_d = done_tx.clone();
                    let metrics_d = metrics.clone();
                    let cleanup_tx_d = cleanup_tx.clone();
                    let h = thread::Builder::new()
                        .name(format!("flusher-dedup-{}-{}", shard_idx, worker_idx))
                        .spawn(move || {
                            Self::dedup_loop(
                                shard_idx,
                                &rx,
                                &miss_tx,
                                &meta_d,
                                &pool_d,
                                &lifecycle_d,
                                &allocator_d,
                                &done_tx_d,
                                &running_d,
                                dedup_skip_threshold,
                                &metrics_d,
                                &cleanup_tx_d,
                            );
                        })
                        .expect("failed to spawn dedup worker");
                    dedup_handles.push(h);
                }
            }
            drop(dedup_rx);
            drop(dedup_tx);
            drop(compress_tx);

            let mut compress_handles = Vec::with_capacity(compress_workers);
            for worker_idx in 0..compress_workers {
                let rx = compress_rx.clone();
                let tx = write_tx.clone();
                let running_w = running.clone();
                let metrics_w = metrics.clone();
                let h = thread::Builder::new()
                    .name(format!("flusher-compress-{}-{}", shard_idx, worker_idx))
                    .spawn(move || {
                        Self::compress_loop(&rx, &tx, &running_w, &metrics_w);
                    })
                    .expect("failed to spawn compress worker");
                compress_handles.push(h);
            }
            drop(compress_rx);
            drop(write_tx);

            let running_w = running.clone();
            let pool_w = pool.clone();
            let meta_w = meta.clone();
            let lifecycle_w = lifecycle.clone();
            let allocator_w = allocator.clone();
            let io_engine_w = io_engine.clone();
            let metrics_w = metrics.clone();
            let in_flight_w = in_flight.clone();
            let writer_handle = thread::Builder::new()
                .name(format!("flusher-writer-{}", shard_idx))
                .spawn(move || {
                    let mut packer = Packer::new_with_lane(allocator_w.clone(), shard_idx);
                    Self::writer_loop(
                        shard_idx,
                        &write_rx,
                        &pool_w,
                        &meta_w,
                        &lifecycle_w,
                        &allocator_w,
                        &io_engine_w,
                        &done_tx,
                        &running_w,
                        &in_flight_w,
                        &mut packer,
                        &metrics_w,
                        &cleanup_tx,
                    );
                })
                .expect("failed to spawn writer thread");

            let running_cl = running.clone();
            let meta_cl = meta.clone();
            let allocator_cl = allocator.clone();
            let metrics_cl = metrics.clone();
            let cleanup_handle = thread::Builder::new()
                .name(format!("flusher-cleanup-{}", shard_idx))
                .spawn(move || {
                    Self::cleanup_loop(
                        shard_idx,
                        &cleanup_rx,
                        &meta_cl,
                        &allocator_cl,
                        &running_cl,
                        &metrics_cl,
                    );
                })
                .expect("failed to spawn cleanup thread");

            lanes.push(FlusherLane {
                coalesce_handle: Some(coalesce_handle),
                dedup_handles,
                compress_handles,
                writer_handle: Some(writer_handle),
                cleanup_handle: Some(cleanup_handle),
            });
        }

        Self {
            running,
            lanes,
            in_flight,
        }
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        self.join_lanes();
    }

    pub(crate) fn wait_volume_generation_idle(
        &self,
        vol_id: &str,
        vol_created_at: u64,
        timeout: Duration,
    ) -> bool {
        self.in_flight
            .wait_volume_generation_idle(vol_id, vol_created_at, timeout)
    }

    /// Wait for all pending buffer entries to be flushed, then stop.
    /// Used during graceful shutdown to ensure the buffer device is clean
    /// (e.g. before a shard count change on next startup).
    pub fn drain_and_stop(&mut self, pool: &crate::buffer::pool::WriteBufferPool) {
        // Keep flusher running while there are pending entries
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            let pending = pool.pending_count();
            if pending == 0 {
                tracing::info!("flusher drain complete — buffer is clean");
                break;
            }
            if std::time::Instant::now() > deadline {
                tracing::warn!(
                    pending,
                    "flusher drain timeout after 60s — stopping with unflushed entries"
                );
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        self.running.store(false, Ordering::Relaxed);
        self.join_lanes();
    }

    fn join_lanes(&mut self) {
        for lane in &mut self.lanes {
            if let Some(h) = lane.coalesce_handle.take() {
                let _ = h.join();
            }
            for h in lane.dedup_handles.drain(..) {
                let _ = h.join();
            }
            for h in lane.compress_handles.drain(..) {
                let _ = h.join();
            }
            if let Some(h) = lane.writer_handle.take() {
                let _ = h.join();
            }
            // Cleanup thread drains after writer stops (writer drop closes cleanup_tx).
            if let Some(h) = lane.cleanup_handle.take() {
                let _ = h.join();
            }
        }
    }
}

impl Drop for BufferFlusher {
    fn drop(&mut self) {
        self.stop();
    }
}
