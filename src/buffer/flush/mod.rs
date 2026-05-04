use std::collections::{HashMap, HashSet, VecDeque};
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, unbounded, Receiver, Sender};

use crate::affinity::{self, ThreadRole};
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

type PbaLockKey = (usize, Pba);

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
    #[allow(dead_code)]
    candidate: Arc<crate::dedup::CandidateCache>,
}

struct FlusherLane {
    coalesce_handle: Option<JoinHandle<()>>,
    dedup_handles: Vec<JoinHandle<()>>,
    compress_handles: Vec<JoinHandle<()>>,
    writer_handle: Option<JoinHandle<()>>,
    dedup_register_handle: Option<JoinHandle<()>>,
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

#[derive(Debug, Clone)]
struct DedupRegistration {
    vol_id: VolumeId,
    lba: Lba,
    hash: ContentHash,
    entry: DedupEntry,
    expected: BlockmapValue,
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

static PBA_LOCKS: OnceLock<Mutex<HashMap<PbaLockKey, Arc<Mutex<()>>>>> = OnceLock::new();
static PBA_CLEANING: OnceLock<Mutex<HashSet<PbaLockKey>>> = OnceLock::new();
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
    const DEDUP_REGISTER_BATCH_MAX: usize = 8192;
    const DEDUP_REGISTER_DRAIN_TIMEOUT: Duration = Duration::from_millis(50);

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

    fn record_max(counter: &std::sync::atomic::AtomicU64, value: u64) {
        let mut current = counter.load(Ordering::Relaxed);
        while value > current {
            match counter.compare_exchange_weak(
                current,
                value,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
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
        // Build a candidate cache sized from the dedup config. The
        // shard count tracks the metadb dedup_shards routing so that a
        // candidate hit and the eventual promote commit always land in
        // the same metadb shard, preserving the inline-dedup commit
        // fast path. Per-shard capacity defaults to
        // CandidateCache::DEFAULT_PER_SHARD_CAPACITY when the dedup
        // config does not pin a value (the field is optional so
        // existing configs keep working).
        let candidate = Arc::new(crate::dedup::CandidateCache::new(
            dedup_config
                .candidate_shards
                .unwrap_or(8)
                .next_power_of_two(),
            dedup_config
                .candidate_per_shard_capacity
                .unwrap_or(crate::dedup::candidate::DEFAULT_PER_SHARD_CAPACITY),
        ));
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
        let dedup_enabled = dedup_config.enabled;
        let dedup_workers = Self::per_lane_worker_count(dedup_config.workers.max(1), lane_count);
        let dedup_skip_threshold = dedup_config.buffer_skip_threshold_pct;
        let dedup_pending_skip_threshold = dedup_config.pending_skip_threshold_entries;
        let mut lanes = Vec::with_capacity(lane_count);

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
            let (cleanup_tx, cleanup_rx) = unbounded::<Vec<(Pba, u32)>>();
            // Writer → dedup registration thread. New dedup rows are
            // opportunistic, so keep their WAL/apply work off the writer's
            // critical path and batch them independently.
            let (dedup_register_tx, dedup_register_rx) = unbounded::<Vec<DedupRegistration>>();

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
            let dedup_register_tx_w = dedup_register_tx.clone();
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
                        &dedup_register_tx_w,
                        packed_meta_batch_max_lbas,
                    );
                })
                .expect("failed to spawn writer thread");
            drop(dedup_register_tx);

            let running_dr = running.clone();
            let meta_dr = meta.clone();
            let metrics_dr = metrics.clone();
            let dedup_register_handle = thread::Builder::new()
                .name(format!("flusher-dedup-register-{}", shard_idx))
                .spawn(move || {
                    affinity::bind_current(ThreadRole::FlusherDedupRegister, shard_idx);
                    Self::dedup_register_loop(
                        shard_idx,
                        &dedup_register_rx,
                        &meta_dr,
                        &running_dr,
                        &metrics_dr,
                    );
                })
                .expect("failed to spawn dedup registration thread");

            let running_cl = running.clone();
            let meta_cl = meta.clone();
            let allocator_cl = allocator.clone();
            let metrics_cl = metrics.clone();
            let cleanup_handle = thread::Builder::new()
                .name(format!("flusher-cleanup-{}", shard_idx))
                .spawn(move || {
                    affinity::bind_current(ThreadRole::FlusherCleanup, shard_idx);
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
                dedup_register_handle: Some(dedup_register_handle),
                cleanup_handle: Some(cleanup_handle),
            });
        }

        Self {
            running,
            lanes,
            in_flight,
            candidate,
        }
    }

    /// Handle to the per-shard RAM candidate cache. Exposed so the
    /// engine can wire the cleanup hook (refcount→0 → candidate
    /// remove) and the dedup scanner can warm the cache during
    /// background rescans.
    pub fn candidate_cache(&self) -> Arc<crate::dedup::CandidateCache> {
        self.candidate.clone()
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

    fn enqueue_dedup_registrations(
        meta: &MetaStore,
        metrics: &EngineMetrics,
        tx: &Sender<Vec<DedupRegistration>>,
        registrations: Vec<DedupRegistration>,
    ) {
        if registrations.is_empty() {
            return;
        }
        let entries = registrations.len();
        if tx.send(registrations.clone()).is_ok() {
            metrics
                .dedup_register_batches
                .fetch_add(1, Ordering::Relaxed);
            metrics
                .dedup_register_entries
                .fetch_add(entries as u64, Ordering::Relaxed);
            Self::record_max(&metrics.dedup_register_batch_max_entries, entries as u64);
            return;
        }
        Self::persist_dedup_registrations(meta, metrics, registrations);
    }

    fn persist_dedup_registrations(
        meta: &MetaStore,
        metrics: &EngineMetrics,
        registrations: Vec<DedupRegistration>,
    ) {
        if registrations.is_empty() {
            return;
        }

        let mut filtered = Vec::with_capacity(registrations.len());
        let validate_start = Instant::now();
        for reg in registrations {
            match meta.dedup_registration_is_current(&reg.vol_id, reg.lba, &reg.expected) {
                Ok(true) => filtered.push((reg.hash, reg.entry)),
                Ok(false) => {}
                Err(e) => {
                    tracing::debug!(
                        vol = %reg.vol_id.0,
                        lba = reg.lba.0,
                        error = %e,
                        "dedup register: validation failed; dropping stale registration"
                    );
                }
            }
        }
        Self::record_elapsed(&metrics.dedup_register_validate_blockmap_ns, validate_start);
        if filtered.is_empty() {
            return;
        }

        let commit_start = Instant::now();
        if let Err(e) = meta.put_dedup_entries_guarded(&filtered) {
            tracing::warn!(
                entries = filtered.len(),
                error = %e,
                "dedup register: failed to persist entries"
            );
        }
        Self::record_elapsed(&metrics.dedup_register_commit_ns, commit_start);
    }

    fn dedup_register_loop(
        shard_idx: usize,
        rx: &Receiver<Vec<DedupRegistration>>,
        meta: &MetaStore,
        running: &AtomicBool,
        metrics: &EngineMetrics,
    ) {
        let mut batch = Vec::new();
        while running.load(Ordering::Relaxed) || !rx.is_empty() {
            let first = match rx.recv_timeout(Self::DEDUP_REGISTER_DRAIN_TIMEOUT) {
                Ok(regs) => regs,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            };
            batch.extend(first);
            while batch.len() < Self::DEDUP_REGISTER_BATCH_MAX {
                match rx.try_recv() {
                    Ok(regs) => batch.extend(regs),
                    Err(_) => break,
                }
            }

            let regs = std::mem::take(&mut batch);
            tracing::trace!(
                lane = shard_idx,
                entries = regs.len(),
                "dedup register: batch"
            );
            Self::persist_dedup_registrations(meta, metrics, regs);
        }

        for regs in rx.try_iter() {
            batch.extend(regs);
            if batch.len() >= Self::DEDUP_REGISTER_BATCH_MAX {
                let regs = std::mem::take(&mut batch);
                Self::persist_dedup_registrations(meta, metrics, regs);
            }
        }
        Self::persist_dedup_registrations(meta, metrics, batch);
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
            // Dedup registration drains after writer stops and drops its sender.
            if let Some(h) = lane.dedup_register_handle.take() {
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
