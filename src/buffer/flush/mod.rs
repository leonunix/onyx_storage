use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, unbounded, Receiver, Sender};

use crate::affinity::{self, ThreadRole};
use crate::buffer::pipeline::{coalesce_pending, CoalesceUnit, CompressedUnit};
use crate::buffer::pool::WriteBufferPool;
use crate::config::FlushConfig;
use crate::dedup::config::DedupConfig;
use crate::error::OnyxResult;
use crate::io::engine::IoEngine;
use crate::lifecycle::VolumeLifecycleManager;
use crate::meta::schema::{BlockmapValue, ContentHash, DedupEntry, FLAG_DEDUP_SKIPPED};
use crate::meta::store::{DedupHitResult, MetaStore, RemapCleanup};
use crate::metrics::EngineMetrics;
use crate::packer::packer::{PackResult, Packer, SealedSlot};
use crate::space::allocator::SpaceAllocator;
use crate::space::extent::Extent;
use crate::space::hazard::{PbaHazardGuard, PbaHazards};
use crate::types::{CompressionAlgo, Lba, Pba, VolumeId, BLOCK_SIZE};

type CleanupBatch = Vec<RemapCleanup>;

pub(crate) const DEFAULT_PACKED_META_BATCH_LBA_LIMIT: usize = 1024;

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
    /// Per-shard RAM candidate cache. The first occurrence of a
    /// fingerprint lands here instead of dedup_index; the next
    /// sighting (verified by LV3 read-back) is promoted into the
    /// persistent dedup tables in the writer's atomic batch. The
    /// writer/dedup-worker integration that actually consumes this
    /// cache is staged as follow-up commits — this struct slot is
    /// pre-wired so the rest of the engine can already construct a
    /// flusher with a real cache and the integration commits don't
    /// have to re-touch the public constructor signature.
    candidate: crate::dedup::CandidateCache,
    /// Per-volume commit workers. Each shard writer hands a
    /// [`writer::CommitJob`] off to `commit_worker_handles[hash %
    /// NUM_COMMIT_WORKERS]` after IO; the worker handles the metadb
    /// commit, cleanup, and `mark_flushed`. See
    /// `.claude/plans/per-volume-commit-worker.md`.
    commit_worker_handles: Vec<JoinHandle<()>>,
    /// Drop these on shutdown to signal commit workers their queues
    /// are draining; sender clones held by shard writers are dropped
    /// when the writer threads join.
    commit_worker_txs: Vec<Sender<writer::CommitJob>>,
    /// Phase 2.2: per-worker post_commit threads run mark_flushed +
    /// candidate insert + stale dedup repair off the commit_worker
    /// hot path. Joined after commit workers (their senders drop on
    /// commit-worker exit, signalling drain).
    post_commit_handles: Vec<JoinHandle<()>>,
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
enum SkipReason {
    InFlight,
    RetryDeferred,
    AlreadySeen,
    NoPendingEntry,
    Superseded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EnqueuePendingSeq {
    Queued,
    Skipped(SkipReason),
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

mod cleanup;
mod failpoints;
mod stages;
mod writer;

#[cfg(test)]
mod tests;

pub(crate) use writer::TARGET_OPS_PER_COMMIT;

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

    fn is_full_raw_unit(unit: &CompressedUnit) -> bool {
        let bs = BLOCK_SIZE as usize;
        unit.compression == 0
            && unit.compressed_data.len() == unit.original_size as usize
            && unit.compressed_data.len() == unit.lba_count as usize * bs
            && unit.compressed_data.len().is_multiple_of(bs)
    }

    fn blockmap_for_unit_position(
        unit: &CompressedUnit,
        base_pba: Pba,
        position: usize,
        slot_offset: u16,
        flags: u8,
    ) -> BlockmapValue {
        if slot_offset == 0 && Self::is_full_raw_unit(unit) {
            let bs = BLOCK_SIZE as usize;
            let start = position * bs;
            let end = start + bs;
            let block = &unit.compressed_data[start..end];
            BlockmapValue {
                pba: Pba(base_pba.0 + position as u64),
                compression: 0,
                unit_compressed_size: BLOCK_SIZE,
                unit_original_size: BLOCK_SIZE,
                unit_lba_count: 1,
                offset_in_unit: 0,
                crc32: crc32fast::hash(block),
                slot_offset: 0,
                flags,
            }
        } else {
            BlockmapValue {
                pba: base_pba,
                compression: unit.compression,
                unit_compressed_size: unit.compressed_data.len() as u32,
                unit_original_size: unit.original_size,
                unit_lba_count: unit.lba_count as u16,
                offset_in_unit: position as u16,
                crc32: unit.crc32,
                slot_offset,
                flags,
            }
        }
    }

    fn free_unreferenced_raw_blocks(
        unit: &CompressedUnit,
        base_pba: Pba,
        live_positions: &[usize],
        allocator: &SpaceAllocator,
        context: &'static str,
    ) {
        if !Self::is_full_raw_unit(unit) {
            return;
        }

        let mut live = vec![false; unit.lba_count as usize];
        for &pos in live_positions {
            if let Some(slot) = live.get_mut(pos) {
                *slot = true;
            }
        }

        for (pos, is_live) in live.into_iter().enumerate() {
            if is_live {
                continue;
            }
            let pba = Pba(base_pba.0 + pos as u64);
            if let Err(e) = allocator.free_one(pba) {
                tracing::warn!(
                    pba = pba.0,
                    context,
                    error = %e,
                    "failed to free unreferenced raw block after metadata commit"
                );
            }
        }
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
        if in_flight.contains_key(&seq) {
            return EnqueuePendingSeq::Skipped(SkipReason::InFlight);
        }
        if !in_flight_tracker.retry_ready(seq) {
            return EnqueuePendingSeq::Skipped(SkipReason::RetryDeferred);
        }
        if !seen.insert(seq) {
            return EnqueuePendingSeq::Skipped(SkipReason::AlreadySeen);
        }

        let Some(meta) = pool.get_pending_arc(seq) else {
            return EnqueuePendingSeq::Skipped(SkipReason::NoPendingEntry);
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
                return EnqueuePendingSeq::Skipped(SkipReason::Superseded);
            }
        }

        let estimated_bytes = Self::pending_entry_bytes(meta.as_ref());
        if !new_entries.is_empty()
            && queued_bytes.saturating_add(estimated_bytes) > Self::COALESCE_READY_WINDOW_BYTES
        {
            return EnqueuePendingSeq::WindowFull;
        }

        *queued_bytes = queued_bytes.saturating_add(Self::pending_entry_bytes(meta.as_ref()));
        new_entries.push(meta);
        EnqueuePendingSeq::Queued
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
            None,
            config,
            dedup_config,
            Arc::new(EngineMetrics::default()),
        )
    }

    /// `read_pool` is the LV3 read pool used for dedup verify-on-hit.
    /// Pass `None` to run the dedup pipeline in trust-hash mode
    /// (xxh3_64 collisions of ~1.5e-8 may produce occasional false
    /// dedups); production deployments should always set
    /// `read_pool_workers > 0`.
    pub fn start_with_metrics(
        pool: Arc<WriteBufferPool>,
        meta: Arc<MetaStore>,
        lifecycle: Arc<VolumeLifecycleManager>,
        allocator: Arc<SpaceAllocator>,
        io_engine: Arc<IoEngine>,
        read_pool: Option<Arc<crate::io::read_pool::ReadPool>>,
        config: &FlushConfig,
        dedup_config: &DedupConfig,
        metrics: Arc<EngineMetrics>,
    ) -> Self {
        // Build a candidate cache sized from the dedup config. The
        // shard count tracks the metadb dedup_shards routing so that a
        // candidate hit and the eventual promote commit always land in
        // the same metadb shard, preserving the inline-dedup commit
        // fast path. Per-shard capacity defaults to
        // CandidateCache::DEFAULT_PER_SHARD_CAPACITY when the dedup
        // config does not pin a value. CandidateCache is itself an
        // Arc<Inner> wrapper — `.clone()` is cheap and shares the
        // same backing storage across every flusher thread that
        // captures a copy.
        let candidate = crate::dedup::CandidateCache::new(
            dedup_config
                .candidate_shards
                .unwrap_or(8)
                .next_power_of_two(),
            dedup_config
                .candidate_per_shard_capacity
                .unwrap_or(crate::dedup::candidate::DEFAULT_PER_SHARD_CAPACITY),
        );
        let running = Arc::new(AtomicBool::new(true));
        let in_flight = Arc::new(FlusherInFlightTracker::default());
        let lane_count = pool.shard_count().max(1);
        let compress_workers =
            Self::per_lane_worker_count(config.compress_workers.max(1), lane_count);
        let max_raw = config.coalesce_max_raw_bytes;
        let max_lbas = config.coalesce_max_lbas;
        let min_compression_savings_pct = config.min_compression_savings_pct.min(100);
        let skip_fully_superseded = config.skip_fully_superseded;
        let packed_meta_batch_max_lbas = if config.packed_meta_batch_max_lbas == 0 {
            DEFAULT_PACKED_META_BATCH_LBA_LIMIT
        } else {
            config.packed_meta_batch_max_lbas
        };
        let commit_workers_per_volume = config
            .commit_workers_per_volume
            .max(1)
            .min(writer::NUM_COMMIT_WORKERS);
        let writer_read_active_batch_size = config
            .writer_read_active_batch_size
            .max(1)
            .min(Self::WRITER_BATCH_SIZE);
        let commit_target_lbas_per_tx = config.commit_target_lbas_per_tx.max(1);
        let commit_coalesce_lba_budget = config.commit_coalesce_lba_budget;
        let commit_coalesce_timeout = Duration::from_micros(config.commit_coalesce_timeout_us);
        let packed_commit_try_drain_lba_budget = config.packed_commit_try_drain_lba_budget;
        let dedup_enabled = dedup_config.enabled;
        let dedup_workers = Self::per_lane_worker_count(dedup_config.workers.max(1), lane_count);
        let dedup_skip_threshold = dedup_config.buffer_skip_threshold_pct;
        let dedup_pending_skip_threshold = dedup_config.pending_skip_threshold_entries;
        let mut lanes = Vec::with_capacity(lane_count);

        // Per-shard `done_tx` / `cleanup_tx` channels are created
        // below in the lane loop; we collect clones here so the
        // commit workers can route by `CommitJob.shard_idx`. Pre-size
        // the storage so the lane loop can `push` into stable
        // indices.
        let mut lane_done_txs: Vec<Sender<Vec<u64>>> = Vec::with_capacity(lane_count);
        let mut lane_cleanup_txs: Vec<Sender<CleanupBatch>> = Vec::with_capacity(lane_count);

        // Spawn N per-volume commit workers up front (channels only
        // — actual threads are spawned after the lane loop, once we
        // have lane_done_txs / lane_cleanup_txs filled). Shard
        // writers get a `Vec<Sender<CommitJob>>` clone and route by
        // `hash(vol_id) % N`. Per-worker queue is bounded so a slow
        // worker provides backpressure to the shard writers.
        let mut commit_worker_txs: Vec<Sender<writer::CommitJob>> =
            Vec::with_capacity(writer::NUM_COMMIT_WORKERS);
        let mut commit_worker_rxs: Vec<Receiver<writer::CommitJob>> =
            Vec::with_capacity(writer::NUM_COMMIT_WORKERS);
        for _ in 0..writer::NUM_COMMIT_WORKERS {
            let (tx, rx) = bounded::<writer::CommitJob>(writer::COMMIT_WORKER_QUEUE_CAP);
            commit_worker_txs.push(tx);
            commit_worker_rxs.push(rx);
        }

        // Phase 2.2 post-commit pairing. One channel per commit_worker
        // so mark_flushed traffic for any one volume stays serialised
        // (matches the commit_worker's per-volume FIFO).
        let mut post_commit_txs: Vec<Sender<writer::PostCommitJob>> =
            Vec::with_capacity(writer::NUM_COMMIT_WORKERS);
        let mut post_commit_rxs: Vec<Receiver<writer::PostCommitJob>> =
            Vec::with_capacity(writer::NUM_COMMIT_WORKERS);
        for _ in 0..writer::NUM_COMMIT_WORKERS {
            let (tx, rx) = bounded::<writer::PostCommitJob>(writer::POST_COMMIT_QUEUE_CAP);
            post_commit_txs.push(tx);
            post_commit_rxs.push(rx);
        }

        for shard_idx in 0..lane_count {
            // Inter-stage channel sizes — sized to keep the writer's
            // per-cycle drain (Self::WRITER_BATCH_SIZE) from starving
            // when an upstream stage briefly stalls. Multipliers picked
            // so write_rx exactly fits one full writer batch and the
            // upstream stages have ~4 batches' worth of slack.
            // Pre-2026-04-27 sizes were workers*4 (~8 slots), which
            // capped writer drain at 8 units regardless of
            // WRITER_BATCH_SIZE — bumping the const alone was a no-op.
            //
            // Stage 1 → Stage 1.5 (dedup) or Stage 2 (compress)
            let (dedup_tx, dedup_rx) = bounded::<CoalesceUnit>(dedup_workers * 32);
            // Stage 1.5 → Stage 2
            let (compress_tx, compress_rx) = bounded::<CoalesceUnit>(compress_workers * 32);
            // Stage 2 → Stage 3 — sized to one full writer batch so a
            // single writer cycle can drain to capacity.
            let (write_tx, write_rx) =
                bounded::<CompressedUnit>(Self::WRITER_BATCH_SIZE.max(compress_workers * 4));
            // Stage 3 → Stage 1 (feedback: completed seqs)
            let (done_tx, done_rx) = unbounded::<Vec<u64>>();
            // Writer/dedup → cleanup thread (async dead PBA reclamation)
            let (cleanup_tx, cleanup_rx) = unbounded::<CleanupBatch>();

            // Capture lane-local senders for the commit workers (they
            // route done_tx / cleanup_tx by `CommitJob.shard_idx`).
            lane_done_txs.push(done_tx.clone());
            lane_cleanup_txs.push(cleanup_tx.clone());

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
                    affinity::bind_current(ThreadRole::FlusherCoalesce, shard_idx);
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
                        skip_fully_superseded,
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
                    let candidate_d = candidate.clone();
                    let read_pool_d = read_pool.clone();
                    let h = thread::Builder::new()
                        .name(format!("flusher-dedup-{}-{}", shard_idx, worker_idx))
                        .spawn(move || {
                            affinity::bind_current(
                                ThreadRole::FlusherDedup,
                                shard_idx * dedup_workers + worker_idx,
                            );
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
                                dedup_pending_skip_threshold,
                                &metrics_d,
                                &cleanup_tx_d,
                                &candidate_d,
                                read_pool_d.as_deref(),
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
                        affinity::bind_current(
                            ThreadRole::FlusherCompress,
                            shard_idx * compress_workers + worker_idx,
                        );
                        Self::compress_loop(
                            &rx,
                            &tx,
                            &running_w,
                            &metrics_w,
                            min_compression_savings_pct,
                        );
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
            let candidate_w = candidate.clone();
            let commit_worker_txs_w = commit_worker_txs.clone();
            let writer_handle = thread::Builder::new()
                .name(format!("flusher-writer-{}", shard_idx))
                .spawn(move || {
                    affinity::bind_current(ThreadRole::FlusherWriter, shard_idx);
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
                        &candidate_w,
                        packed_meta_batch_max_lbas,
                        &commit_worker_txs_w,
                        commit_workers_per_volume,
                        writer_read_active_batch_size,
                    );
                })
                .expect("failed to spawn writer thread");

            let running_cl = running.clone();
            let allocator_cl = allocator.clone();
            let candidate_cl = candidate.clone();
            let metrics_cl = metrics.clone();
            let cleanup_handle = thread::Builder::new()
                .name(format!("flusher-cleanup-{}", shard_idx))
                .spawn(move || {
                    affinity::bind_current(ThreadRole::FlusherCleanup, shard_idx);
                    Self::cleanup_loop(
                        shard_idx,
                        &cleanup_rx,
                        &allocator_cl,
                        &candidate_cl,
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

        // Spawn the per-volume commit workers now that lane channels
        // exist. Each worker indexes `lane_done_txs` / `lane_cleanup_txs`
        // by `CommitJob.shard_idx` to fire `done_tx` and queue
        // cleanup payloads back into the originating shard's lane.
        let mut commit_worker_handles: Vec<JoinHandle<()>> =
            Vec::with_capacity(writer::NUM_COMMIT_WORKERS);
        for (worker_idx, rx) in commit_worker_rxs.into_iter().enumerate() {
            let pool_c = pool.clone();
            let meta_c = meta.clone();
            let lifecycle_c = lifecycle.clone();
            let allocator_c = allocator.clone();
            let in_flight_c = in_flight.clone();
            let metrics_c = metrics.clone();
            let candidate_c = candidate.clone();
            let running_c = running.clone();
            let lane_done_txs_c = lane_done_txs.clone();
            let lane_cleanup_txs_c = lane_cleanup_txs.clone();
            let post_commit_tx_c = post_commit_txs[worker_idx].clone();
            let h = thread::Builder::new()
                .name(format!("flusher-commit-{}", worker_idx))
                .spawn(move || {
                    affinity::bind_current(ThreadRole::CommitWorker, worker_idx);
                    Self::commit_worker_loop(
                        worker_idx,
                        &rx,
                        &pool_c,
                        &meta_c,
                        &lifecycle_c,
                        &allocator_c,
                        &in_flight_c,
                        &metrics_c,
                        &lane_cleanup_txs_c,
                        &candidate_c,
                        &lane_done_txs_c,
                        &post_commit_tx_c,
                        commit_target_lbas_per_tx,
                        commit_coalesce_lba_budget,
                        commit_coalesce_timeout,
                        packed_commit_try_drain_lba_budget,
                        &running_c,
                    );
                })
                .expect("failed to spawn commit worker");
            commit_worker_handles.push(h);
        }

        // Drop our extra clones of the post_commit_txs — only the
        // commit_workers hold senders now. When the commit workers
        // exit on shutdown, these channels disconnect and the
        // post_commit threads will drain and exit.
        drop(post_commit_txs);

        let mut post_commit_handles: Vec<JoinHandle<()>> =
            Vec::with_capacity(writer::NUM_COMMIT_WORKERS);
        for (worker_idx, rx) in post_commit_rxs.into_iter().enumerate() {
            let pool_c = pool.clone();
            let meta_c = meta.clone();
            let candidate_c = candidate.clone();
            let metrics_c = metrics.clone();
            let lane_done_txs_c = lane_done_txs.clone();
            let h = thread::Builder::new()
                .name(format!("flusher-post-commit-{}", worker_idx))
                .spawn(move || {
                    affinity::bind_current(ThreadRole::FlusherCleanup, worker_idx);
                    Self::post_commit_loop(
                        worker_idx,
                        &rx,
                        &pool_c,
                        &meta_c,
                        &candidate_c,
                        &metrics_c,
                        &lane_done_txs_c,
                    );
                })
                .expect("failed to spawn post-commit worker");
            post_commit_handles.push(h);
        }

        Self {
            running,
            lanes,
            in_flight,
            candidate,
            commit_worker_handles,
            commit_worker_txs,
            post_commit_handles,
        }
    }

    /// Handle to the per-shard RAM candidate cache. Exposed so the
    /// engine can wire the cleanup hook (refcount→0 → candidate
    /// remove) and the dedup scanner can warm the cache during
    /// background rescans. Cheap clone — shares the same backing
    /// shards.
    pub fn candidate_cache(&self) -> crate::dedup::CandidateCache {
        self.candidate.clone()
    }

    pub fn cleanup_mappings_now(
        &self,
        allocator: &SpaceAllocator,
        cleanups: &[RemapCleanup],
        context: &'static str,
    ) {
        Self::cleanup_dead_pbas_batch(
            allocator,
            &self.candidate,
            cleanups,
            context,
        );
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
        }
        // Shard writers have stopped — drop the commit_worker_txs we
        // hold so the worker rx sides disconnect once their per-lane
        // sender clones (held by writer threads) drop. This signals
        // workers to drain and exit.
        self.commit_worker_txs.clear();
        for h in self.commit_worker_handles.drain(..) {
            let _ = h.join();
        }
        // Phase 2.2: post_commit threads exit when commit_worker
        // post_commit_tx senders drop (the only senders are inside
        // commit_worker stack frames, freed when the threads above
        // joined). Join them next so mark_flushed/candidate work for
        // the last batch of commits is durable before downstream
        // cleanup runs.
        for h in self.post_commit_handles.drain(..) {
            let _ = h.join();
        }
        // Per-lane cleanup workers drain after the commit workers
        // finish (commit workers may push cleanup payloads through
        // each lane's cleanup_tx during their own drain).
        for lane in &mut self.lanes {
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
