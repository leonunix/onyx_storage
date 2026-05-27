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
    watermark_handle: Option<JoinHandle<()>>,
}

struct FlusherLane {
    coalesce_handle: Option<JoinHandle<()>>,
    dedup_handles: Vec<JoinHandle<()>>,
    compress_handles: Vec<JoinHandle<()>>,
    writer_handle: Option<JoinHandle<()>>,
    cleanup_handle: Option<JoinHandle<()>>,
}

/// Buffer-as-sole-journal Phase A drain summary. Returned from
/// [`BufferFlusher::drain_with_timeout`] and the higher-level
/// [`replay_buffer_pending`] helper so callers can confirm replay
/// quiescence before accepting client IO or comparing shadow state.
#[derive(Debug, Clone)]
pub struct BufferReplayStats {
    /// Pending-entry count observed when the drain loop started.
    pub pending_at_start: u64,
    /// Pending-entry count at exit. Zero iff the drain completed
    /// cleanly; non-zero iff `timed_out` is `true`.
    pub pending_at_exit: u64,
    /// Wall-clock duration spent inside the drain loop (not including
    /// flusher startup or teardown).
    pub elapsed: std::time::Duration,
    /// Set if the loop hit the supplied deadline before reaching
    /// quiescence. The caller decides whether to retry or escalate.
    pub timed_out: bool,
}

impl BufferReplayStats {
    /// `true` iff replay reached quiescence inside the supplied
    /// timeout. Callers gating client IO on a successful replay use
    /// this as the go/no-go signal.
    pub fn drained_clean(&self) -> bool {
        !self.timed_out && self.pending_at_exit == 0
    }
}

/// Buffer-as-sole-journal Phase A: drive the flusher pipeline once
/// against the buffer's currently-pending entries and stop, returning
/// drain statistics.
///
/// Used:
/// - On engine open in `metadb_journal_mode = "buffer"` (Phase C), to
///   bring metadb in-memory state up to the buffer tail before
///   accepting clients.
/// - In shadow validation (Phase B), to drive the shadow metadb's
///   in-memory state from the same buffer stream the WAL replay is
///   reconstructing, then assert state equivalence.
/// - In replay tests that need deterministic flusher quiescence.
///
/// `timeout` bounds the wait. Returns the [`BufferReplayStats`]
/// snapshot; the caller decides what to do with a `timed_out` result
/// (typically: retry once with a larger budget, or fail engine open).
#[allow(clippy::too_many_arguments)]
pub fn replay_buffer_pending(
    pool: Arc<WriteBufferPool>,
    meta: Arc<MetaStore>,
    lifecycle: Arc<VolumeLifecycleManager>,
    allocator: Arc<SpaceAllocator>,
    io_engine: Arc<IoEngine>,
    read_pool: Option<Arc<crate::io::read_pool::ReadPool>>,
    config: &FlushConfig,
    dedup_config: &DedupConfig,
    metrics: Arc<EngineMetrics>,
    timeout: std::time::Duration,
) -> BufferReplayStats {
    let mut flusher = BufferFlusher::start_with_metrics(
        pool.clone(),
        meta,
        lifecycle,
        allocator,
        io_engine,
        read_pool,
        config,
        dedup_config,
        metrics,
    );
    flusher.drain_with_timeout(&pool, timeout)
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
mod runtime;
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
        Self::blockmap_for_unit_position_with_raw_split(
            unit,
            base_pba,
            position,
            slot_offset,
            flags,
            false,
        )
    }

    fn blockmap_for_unit_position_with_raw_split(
        unit: &CompressedUnit,
        base_pba: Pba,
        position: usize,
        slot_offset: u16,
        flags: u8,
        split_full_raw_unit: bool,
    ) -> BlockmapValue {
        if split_full_raw_unit && slot_offset == 0 && Self::is_full_raw_unit(unit) {
            let bs = BLOCK_SIZE as usize;
            let start = position * bs;
            let end = start + bs;
            let block = &unit.compressed_data[start..end];
            return BlockmapValue {
                pba: Pba(base_pba.0 + position as u64),
                compression: 0,
                unit_compressed_size: BLOCK_SIZE,
                unit_original_size: BLOCK_SIZE,
                unit_lba_count: 1,
                offset_in_unit: 0,
                crc32: crc32fast::hash(block),
                slot_offset: 0,
                flags,
            };
        }

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
}
