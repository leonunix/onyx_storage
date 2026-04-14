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
use crate::packer::packer::{HoleFill, HoleMap, PackResult, Packer, SealedSlot};
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
}

#[derive(Debug, Clone, Copy)]
struct OldFragmentRef {
    decrement: u32,
    mapping: BlockmapValue,
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

fn same_fragment_identity(lhs: &BlockmapValue, rhs: &BlockmapValue) -> bool {
    lhs.pba == rhs.pba
        && lhs.slot_offset == rhs.slot_offset
        && lhs.unit_compressed_size == rhs.unit_compressed_size
        && lhs.unit_original_size == rhs.unit_original_size
        && lhs.unit_lba_count == rhs.unit_lba_count
        && lhs.compression == rhs.compression
        && lhs.crc32 == rhs.crc32
}

fn record_old_fragment_ref(
    old_frag_meta: &mut HashMap<(Pba, u16), OldFragmentRef>,
    old: BlockmapValue,
) {
    let frag = old_frag_meta
        .entry((old.pba, old.slot_offset))
        .or_insert(OldFragmentRef {
            decrement: 0,
            mapping: old,
        });
    debug_assert!(
        same_fragment_identity(&frag.mapping, &old),
        "fragment identity changed within one packed-slot fragment bucket"
    );
    frag.decrement += 1;
}

fn record_dead_pba_cleanup(dead_pbas: &mut HashMap<Pba, u32>, decremented: Option<(Pba, u32)>) {
    let Some((old_pba, old_blocks)) = decremented else {
        return;
    };
    dead_pbas
        .entry(old_pba)
        .and_modify(|blocks| *blocks = (*blocks).max(old_blocks))
        .or_insert(old_blocks);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum FlushFailStage {
    BeforeIoWrite,
    BeforeMetaWrite,
}

#[derive(Debug, Clone)]
struct FlushFailRule {
    vol_id: String,
    start_lba: Lba,
    stage: FlushFailStage,
    remaining_hits: Option<u32>,
}

static TEST_FAIL_RULES: OnceLock<Mutex<Vec<FlushFailRule>>> = OnceLock::new();
static TEST_DEDUP_HIT_FAIL_RULES: OnceLock<Mutex<Vec<DedupHitFailRule>>> = OnceLock::new();
static HOLE_FILL_LOCKS: OnceLock<Mutex<HashMap<Pba, Arc<Mutex<()>>>>> = OnceLock::new();
#[cfg(test)]
static CLEANUP_FREE_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
#[doc(hidden)]
pub struct PackedPauseState {
    hit: bool,
    released: bool,
}

struct PackedPauseHook {
    vol_id: String,
    state: Arc<(Mutex<PackedPauseState>, Condvar)>,
}

#[derive(Debug, Clone)]
struct DedupHitFailRule {
    vol_id: String,
    lba: Lba,
    remaining_hits: Option<u32>,
}

static TEST_PACKED_PAUSE_HOOK: OnceLock<Mutex<Option<PackedPauseHook>>> = OnceLock::new();

fn test_fail_rules() -> &'static Mutex<Vec<FlushFailRule>> {
    TEST_FAIL_RULES.get_or_init(|| Mutex::new(Vec::new()))
}

fn test_dedup_hit_fail_rules() -> &'static Mutex<Vec<DedupHitFailRule>> {
    TEST_DEDUP_HIT_FAIL_RULES.get_or_init(|| Mutex::new(Vec::new()))
}

fn test_packed_pause_hook() -> &'static Mutex<Option<PackedPauseHook>> {
    TEST_PACKED_PAUSE_HOOK.get_or_init(|| Mutex::new(None))
}

fn hole_fill_locks() -> &'static Mutex<HashMap<Pba, Arc<Mutex<()>>>> {
    HOLE_FILL_LOCKS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn parse_env_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn soak_debug_guards_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("ONYX_ENABLE_SOAK_DEBUG_GUARDS")
            .ok()
            .and_then(|value| parse_env_bool(&value))
            .unwrap_or(cfg!(debug_assertions))
    })
}

fn maybe_inject_test_failure(
    vol_id: &str,
    start_lba: Lba,
    stage: FlushFailStage,
) -> OnyxResult<()> {
    let mut rules = test_fail_rules().lock().unwrap();
    if let Some(idx) = rules.iter().position(|rule| {
        rule.vol_id == vol_id && rule.start_lba == start_lba && rule.stage == stage
    }) {
        let mut remove = false;
        if let Some(remaining) = rules[idx].remaining_hits.as_mut() {
            if *remaining > 0 {
                *remaining -= 1;
            }
            if *remaining == 0 {
                remove = true;
            }
        }
        if remove {
            rules.remove(idx);
        }
        return Err(crate::error::OnyxError::Io(std::io::Error::other(format!(
            "injected flush failure at {:?} for {}:{}",
            stage, vol_id, start_lba.0
        ))));
    }
    Ok(())
}

fn maybe_inject_test_failure_packed(
    fragments: &[crate::packer::packer::SlotFragment],
    stage: FlushFailStage,
) -> OnyxResult<()> {
    for frag in fragments {
        maybe_inject_test_failure(&frag.unit.vol_id, frag.unit.start_lba, stage)?;
    }
    Ok(())
}

fn maybe_inject_dedup_hit_failure(vol_id: &str, lba: Lba) -> OnyxResult<()> {
    let mut rules = test_dedup_hit_fail_rules().lock().unwrap();
    if let Some(idx) = rules
        .iter()
        .position(|rule| rule.vol_id == vol_id && rule.lba == lba)
    {
        let mut remove = false;
        if let Some(remaining) = rules[idx].remaining_hits.as_mut() {
            if *remaining > 0 {
                *remaining -= 1;
            }
            if *remaining == 0 {
                remove = true;
            }
        }
        if remove {
            rules.remove(idx);
        }
        return Err(crate::error::OnyxError::Io(std::io::Error::other(format!(
            "injected dedup hit failure for {}:{}",
            vol_id, lba.0
        ))));
    }
    Ok(())
}

fn maybe_pause_before_packed_meta_write(
    fragments: &[crate::packer::packer::SlotFragment],
) -> OnyxResult<()> {
    let state = {
        let hook = test_packed_pause_hook().lock().unwrap();
        let Some(hook) = hook.as_ref() else {
            return Ok(());
        };
        if !fragments.iter().any(|f| f.unit.vol_id == hook.vol_id) {
            return Ok(());
        }
        hook.state.clone()
    };

    let (lock, cv) = &*state;
    let mut guard = lock.lock().unwrap();
    guard.hit = true;
    cv.notify_all();
    while !guard.released {
        guard = cv.wait(guard).unwrap();
    }
    Ok(())
}

#[doc(hidden)]
pub fn install_test_failpoint(
    vol_id: &str,
    start_lba: Lba,
    stage: FlushFailStage,
    remaining_hits: Option<u32>,
) {
    let mut rules = test_fail_rules().lock().unwrap();
    rules.push(FlushFailRule {
        vol_id: vol_id.to_string(),
        start_lba,
        stage,
        remaining_hits,
    });
}

#[doc(hidden)]
pub fn clear_test_failpoint(vol_id: &str, start_lba: Lba, stage: FlushFailStage) {
    let mut rules = test_fail_rules().lock().unwrap();
    rules.retain(|rule| {
        !(rule.vol_id == vol_id && rule.start_lba == start_lba && rule.stage == stage)
    });
}

#[doc(hidden)]
pub fn install_test_dedup_hit_failpoint(vol_id: &str, lba: Lba, remaining_hits: Option<u32>) {
    let mut rules = test_dedup_hit_fail_rules().lock().unwrap();
    rules.push(DedupHitFailRule {
        vol_id: vol_id.to_string(),
        lba,
        remaining_hits,
    });
}

#[doc(hidden)]
pub fn clear_test_dedup_hit_failpoint(vol_id: &str, lba: Lba) {
    let mut rules = test_dedup_hit_fail_rules().lock().unwrap();
    rules.retain(|rule| !(rule.vol_id == vol_id && rule.lba == lba));
}

#[doc(hidden)]
pub fn install_test_packed_pause_hook(vol_id: &str) -> Arc<(Mutex<PackedPauseState>, Condvar)> {
    let state = Arc::new((
        Mutex::new(PackedPauseState {
            hit: false,
            released: false,
        }),
        Condvar::new(),
    ));
    let mut hook = test_packed_pause_hook().lock().unwrap();
    *hook = Some(PackedPauseHook {
        vol_id: vol_id.to_string(),
        state: state.clone(),
    });
    state
}

#[doc(hidden)]
pub fn clear_test_packed_pause_hook() {
    let mut hook = test_packed_pause_hook().lock().unwrap();
    *hook = None;
}

#[doc(hidden)]
pub fn wait_for_test_packed_pause_hit(
    state: &Arc<(Mutex<PackedPauseState>, Condvar)>,
    timeout: Duration,
) -> bool {
    let (lock, cv) = &**state;
    let guard = lock.lock().unwrap();
    let (guard, _) = cv.wait_timeout_while(guard, timeout, |s| !s.hit).unwrap();
    guard.hit
}

#[doc(hidden)]
pub fn release_test_packed_pause_hook(state: &Arc<(Mutex<PackedPauseState>, Condvar)>) {
    let (lock, cv) = &**state;
    let mut guard = lock.lock().unwrap();
    guard.released = true;
    cv.notify_all();
}

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

    fn purge_holes_for_pba(hole_map: &HoleMap, pba: Pba) {
        crate::packer::packer::remove_holes_for_pba(hole_map, pba);
    }

    fn purge_holes_for_extent(hole_map: &HoleMap, start: Pba, count: u32) {
        crate::packer::packer::remove_holes_for_extent(hole_map, start, count);
    }

    /// Once blockmap/refcount metadata has committed, reclaiming the old PBA is
    /// strictly best-effort. Failing this cleanup must not turn the flush into a
    /// retry loop, or the buffer head will stay pinned behind work that already
    /// committed successfully.
    pub(crate) fn cleanup_dead_pba_post_commit(
        meta: &MetaStore,
        allocator: &SpaceAllocator,
        hole_map: &HoleMap,
        old_pba: Pba,
        old_blocks: u32,
        context: &'static str,
    ) {
        let cleanup_lock = Self::cleanup_lock(old_pba);
        let _cleanup_guard = cleanup_lock.lock().unwrap();

        let remaining = match Self::live_refcount_with_reconcile(meta, old_pba, context) {
            Ok(remaining) => remaining,
            Err(e) => {
                tracing::error!(
                    pba = old_pba.0,
                    old_blocks,
                    error = %e,
                    context,
                    "post-commit cleanup: failed to confirm dead PBA; leaving allocator reservation"
                );
                return;
            }
        };
        if remaining != 0 {
            return;
        }

        if let Err(e) = meta.cleanup_dedup_for_pba_standalone(old_pba) {
            tracing::error!(
                pba = old_pba.0,
                old_blocks,
                error = %e,
                context,
                "post-commit cleanup: failed to cleanup dedup metadata; leaving allocator reservation"
            );
            return;
        }

        let already_free = if old_blocks <= 1 {
            allocator.is_free(old_pba)
        } else {
            allocator.is_extent_free(Extent::new(old_pba, old_blocks))
        };
        if already_free {
            if old_blocks <= 1 {
                Self::purge_holes_for_pba(hole_map, old_pba);
            } else {
                Self::purge_holes_for_extent(hole_map, old_pba, old_blocks);
            }
            return;
        }

        #[cfg(test)]
        CLEANUP_FREE_ATTEMPTS.fetch_add(1, Ordering::SeqCst);

        let free_result = if old_blocks <= 1 {
            Self::purge_holes_for_pba(hole_map, old_pba);
            allocator.free_one(old_pba)
        } else {
            Self::purge_holes_for_extent(hole_map, old_pba, old_blocks);
            allocator.free_extent(Extent::new(old_pba, old_blocks))
        };
        if let Err(e) = free_result {
            tracing::warn!(
                pba = old_pba.0,
                old_blocks,
                error = %e,
                context,
                "post-commit cleanup: allocator free failed after metadata commit (benign if already freed by another path); continuing without retry"
            );
        }
    }

    // TEMP(soak-debug): heavy protective guard. In normal release runs we trust
    // the refcount that was just updated in the metadata write path. The full
    // reconcile scan stays available behind ONYX_ENABLE_SOAK_DEBUG_GUARDS=1.
    fn live_refcount_with_reconcile(
        meta: &MetaStore,
        pba: Pba,
        context: &'static str,
    ) -> OnyxResult<u32> {
        let remaining = meta.get_refcount(pba)?;
        if remaining != 0 || !soak_debug_guards_enabled() {
            return Ok(remaining);
        }
        let reconciled = meta.reconcile_refcount_for_pba(pba)?;
        if reconciled != 0 {
            tracing::error!(
                pba = pba.0,
                reconciled_refcount = reconciled,
                context,
                "detected refcount drift while preparing to free or inspect a PBA"
            );
        }
        Ok(reconciled)
    }

    // TEMP(soak-debug): early fail-fast for overlapping live fragments inside a
    // shared packed slot. Remove or gate after the root cause is fixed.
    fn fail_if_pba_has_fragment_overlap(
        meta: &MetaStore,
        pba: Pba,
        context: &'static str,
    ) -> OnyxResult<()> {
        let Some((lhs, rhs)) = meta.first_fragment_overlap_at_pba(pba)? else {
            return Ok(());
        };
        tracing::error!(
            pba = pba.0,
            lhs = ?format!(
                "{}:{} off{} size{} crc={:#x}",
                lhs.0, lhs.1.0, lhs.2.slot_offset, lhs.2.unit_compressed_size, lhs.2.crc32
            ),
            rhs = ?format!(
                "{}:{} off{} size{} crc={:#x}",
                rhs.0, rhs.1.0, rhs.2.slot_offset, rhs.2.unit_compressed_size, rhs.2.crc32
            ),
            context,
            "detected overlapping live fragments at a shared PBA"
        );
        Err(crate::error::OnyxError::Io(std::io::Error::other(format!(
            "overlapping live fragments detected at {pba:?} ({context})"
        ))))
    }

    fn hole_fill_lock(pba: Pba) -> Arc<Mutex<()>> {
        let mut locks = hole_fill_locks().lock().unwrap();
        // Prune stale entries where the map is the sole holder (strong_count == 1).
        // Amortised: only prune when the map exceeds a reasonable threshold.
        if locks.len() > 4096 {
            locks.retain(|_, arc| Arc::strong_count(arc) > 1);
        }
        locks
            .entry(pba)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub(crate) fn cleanup_lock(pba: Pba) -> Arc<Mutex<()>> {
        Self::hole_fill_lock(pba)
    }

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
    ) -> EnqueuePendingSeq {
        if in_flight.contains_key(&seq) || !in_flight_tracker.retry_ready(seq) || !seen.insert(seq)
        {
            return EnqueuePendingSeq::Skipped;
        }

        let Some(meta) = pool.get_pending_arc(seq) else {
            return EnqueuePendingSeq::Skipped;
        };
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
        hole_map: HoleMap,
        dedup_config: &DedupConfig,
    ) -> Self {
        Self::start_with_metrics(
            pool,
            meta,
            lifecycle,
            allocator,
            io_engine,
            config,
            hole_map,
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
        hole_map: HoleMap,
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
                    let hole_map_d = hole_map.clone();
                    let done_tx_d = done_tx.clone();
                    let metrics_d = metrics.clone();
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
                                &hole_map_d,
                                &done_tx_d,
                                &running_d,
                                dedup_skip_threshold,
                                &metrics_d,
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
            let hole_map_w = hole_map.clone();
            let in_flight_w = in_flight.clone();
            let writer_handle = thread::Builder::new()
                .name(format!("flusher-writer-{}", shard_idx))
                .spawn(move || {
                    let mut packer =
                        Packer::new_with_lane(allocator_w.clone(), hole_map_w, shard_idx);
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
                    );
                })
                .expect("failed to spawn writer thread");

            lanes.push(FlusherLane {
                coalesce_handle: Some(coalesce_handle),
                dedup_handles,
                compress_handles,
                writer_handle: Some(writer_handle),
            });
        }

        Self {
            running,
            lanes,
            in_flight,
        }
    }

    fn coalesce_loop(
        shard_idx: usize,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        tx: &Sender<CoalesceUnit>,
        done_rx: &Receiver<Vec<u64>>,
        running: &AtomicBool,
        in_flight_tracker: &FlusherInFlightTracker,
        metrics: &EngineMetrics,
        max_raw: usize,
        max_lbas: u32,
    ) {
        // in_flight tracks how many pipeline units still reference each seq.
        // A multi-LBA entry split into 2 units → refcount=2 for that seq.
        // Only when refcount hits 0 does the seq leave in_flight.
        let mut in_flight: HashMap<u64, u32> = HashMap::new();

        // Cache per-volume compression to avoid repeated MetaStore lookups.
        let mut vol_compression_cache: HashMap<String, CompressionAlgo> = HashMap::new();
        let vol_compression = |vol_id: &str| -> CompressionAlgo {
            // Can't use cache from closure due to borrow rules — inlined below
            if let Ok(Some(vc)) = meta.get_volume(&crate::types::VolumeId(vol_id.to_string())) {
                vc.compression
            } else {
                CompressionAlgo::None
            }
        };
        let ready_timeout = Duration::from_millis(10);
        let retry_snapshot_interval = Duration::from_millis(100);
        let retry_snapshot_topup_limit = 64usize;
        let mut last_retry_snapshot = Instant::now();

        while running.load(Ordering::Relaxed) {
            // Drain completed seqs from writer feedback — decrement refcounts
            while let Ok(seqs) = done_rx.try_recv() {
                for seq in seqs {
                    if let Some(count) = in_flight.get_mut(&seq) {
                        *count -= 1;
                        if *count == 0 {
                            in_flight.remove(&seq);
                            in_flight_tracker.track_seq_done(seq);
                        }
                    }
                }
            }

            let mut new_entries = Vec::new();
            let mut seen = std::collections::HashSet::new();
            let mut queued_bytes = 0usize;

            // Always give the front of log_order a retry chance first. A single
            // partially flushed seq can otherwise starve behind newer ready work
            // and pin tail reclamation for minutes.
            if let Some(entry) = pool
                .head_stuck_pending_entry_arc_for_shard(shard_idx, Self::HEAD_RETRY_AGE_THRESHOLD)
            {
                let seq = entry.seq;
                if !in_flight.contains_key(&seq)
                    && in_flight_tracker.retry_ready(seq)
                    && seen.insert(seq)
                {
                    queued_bytes =
                        queued_bytes.saturating_add(Self::pending_entry_bytes(entry.as_ref()));
                    new_entries.push(entry);
                }
            }

            if new_entries.is_empty() {
                match pool.recv_ready_timeout_for_shard(shard_idx, ready_timeout) {
                    Ok(seq) => {
                        let _ = Self::try_enqueue_pending_seq(
                            seq,
                            pool,
                            &in_flight,
                            in_flight_tracker,
                            &mut seen,
                            &mut queued_bytes,
                            &mut new_entries,
                        );
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
                }
            }

            while queued_bytes < Self::COALESCE_READY_WINDOW_BYTES {
                let Ok(seq) = pool.try_recv_ready_for_shard(shard_idx) else {
                    break;
                };
                if matches!(
                    Self::try_enqueue_pending_seq(
                        seq,
                        pool,
                        &in_flight,
                        in_flight_tracker,
                        &mut seen,
                        &mut queued_bytes,
                        &mut new_entries,
                    ),
                    EnqueuePendingSeq::WindowFull
                ) {
                    break;
                }
            }

            // Safety net for recovered / retried entries: periodically snapshot the
            // in-memory pending set instead of rescanning the on-disk log on every
            // loop. This must run even while foreground writes keep producing new
            // ready seqs, otherwise payload-less recovered entries that were skipped
            // once under memory pressure can starve indefinitely.
            if last_retry_snapshot.elapsed() >= retry_snapshot_interval
                && queued_bytes < Self::COALESCE_READY_WINDOW_BYTES
            {
                last_retry_snapshot = Instant::now();
                let mut topped_up = 0usize;
                for entry in pool.ready_pending_entries_arc_snapshot_for_shard(shard_idx) {
                    if topped_up >= retry_snapshot_topup_limit
                        || queued_bytes >= Self::COALESCE_READY_WINDOW_BYTES
                    {
                        break;
                    }
                    if matches!(
                        Self::try_enqueue_pending_seq(
                            entry.seq,
                            pool,
                            &in_flight,
                            in_flight_tracker,
                            &mut seen,
                            &mut queued_bytes,
                            &mut new_entries,
                        ),
                        EnqueuePendingSeq::Queued
                    ) {
                        topped_up += 1;
                    }
                }
            }

            if new_entries.is_empty() {
                continue;
            }

            // Build per-volume compression lookup using cache
            for entry in &new_entries {
                vol_compression_cache
                    .entry(entry.vol_id.clone())
                    .or_insert_with(|| vol_compression(&entry.vol_id));
            }
            // Build skip map: already-flushed LBA offsets that the coalescer
            // should not re-include.  Prevents the head-of-line starvation bug
            // where a partially-flushed entry keeps re-coalescing done LBAs.
            let mut skip_offsets: HashMap<u64, std::collections::HashSet<u16>> = HashMap::new();
            for entry in &new_entries {
                if let Some(flushed) = pool.flushed_offsets_for_shard(shard_idx, entry.seq) {
                    if !flushed.is_empty() {
                        skip_offsets.insert(entry.seq, flushed);
                    }
                }
            }

            let cache_ref = &vol_compression_cache;
            let units = coalesce_pending(
                &new_entries,
                max_raw,
                max_lbas,
                &|vid| cache_ref.get(vid).copied().unwrap_or(CompressionAlgo::None),
                &skip_offsets,
            );

            // Payload ownership has been moved into Arc-backed block refs inside
            // the coalesced units, so the pending-entry copy can be evicted
            // immediately without losing the bytes needed by dedup/compress.
            let consumed_seqs: Vec<u64> = new_entries.iter().map(|e| e.seq).collect();
            drop(new_entries);
            pool.evict_hydrated_payloads_for_shard(shard_idx, &consumed_seqs);

            if !units.is_empty() {
                metrics.coalesce_runs.fetch_add(1, Ordering::Relaxed);
                metrics
                    .coalesced_units
                    .fetch_add(units.len() as u64, Ordering::Relaxed);
                metrics.coalesced_lbas.fetch_add(
                    units.iter().map(|u| u.lba_count as u64).sum::<u64>(),
                    Ordering::Relaxed,
                );
                metrics.coalesced_bytes.fetch_add(
                    units.iter().map(|u| u.raw_len() as u64).sum::<u64>(),
                    Ordering::Relaxed,
                );
            }

            // Count how many units reference each seq
            for unit in &units {
                for (seq, _, _) in &unit.seq_lba_ranges {
                    let count = in_flight.entry(*seq).or_insert(0);
                    if *count == 0 {
                        in_flight_tracker.track_seq_start(*seq, &unit.vol_id, unit.vol_created_at);
                    }
                    *count += 1;
                }
            }

            for unit in units {
                if tx.send(unit).is_err() {
                    return;
                }
            }
        }
    }

    fn compress_loop(
        rx: &Receiver<CoalesceUnit>,
        tx: &Sender<CompressedUnit>,
        running: &AtomicBool,
        metrics: &EngineMetrics,
    ) {
        while running.load(Ordering::Relaxed) {
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(unit) => {
                    let CoalesceUnit {
                        vol_id,
                        start_lba,
                        lba_count,
                        raw_blocks,
                        compression: algo,
                        vol_created_at,
                        seq_lba_ranges,
                        dedup_skipped,
                        block_hashes,
                        dedup_completion,
                    } = unit;

                    let original_size = raw_blocks.len() * BLOCK_SIZE as usize;
                    let mut raw_data = Vec::with_capacity(original_size);
                    for block in &raw_blocks {
                        raw_data.extend_from_slice(block.bytes());
                    }
                    let (compression_byte, compressed_data) = match algo {
                        CompressionAlgo::None => (0u8, raw_data),
                        _ => {
                            let compressor = create_compressor(algo);
                            let max_out = compressor.max_compressed_size(original_size);
                            let mut compressed_buf = vec![0u8; max_out];
                            match compressor.compress(&raw_data, &mut compressed_buf) {
                                Some(size) => (algo.to_u8(), compressed_buf[..size].to_vec()),
                                None => (0u8, raw_data),
                            }
                        }
                    };
                    metrics.compress_units.fetch_add(1, Ordering::Relaxed);
                    metrics
                        .compress_input_bytes
                        .fetch_add(original_size as u64, Ordering::Relaxed);
                    metrics
                        .compress_output_bytes
                        .fetch_add(compressed_data.len() as u64, Ordering::Relaxed);

                    let crc32 = crc32fast::hash(&compressed_data);

                    let cu = CompressedUnit {
                        vol_id,
                        start_lba,
                        lba_count,
                        original_size: original_size as u32,
                        compressed_data,
                        compression: compression_byte,
                        crc32,
                        vol_created_at,
                        seq_lba_ranges,
                        block_hashes,
                        dedup_skipped,
                        dedup_completion,
                    };

                    if tx.send(cu).is_err() {
                        return;
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    /// Dedup stage: hash 4KB blocks, check dedup index, handle hits inline.
    ///
    /// Seq lifecycle: the coalescer tracks one refcount per original unit. The
    /// dedup stage handles hits directly (metadata update + mark_flushed + done_tx)
    /// and sends only miss sub-units to the compress pipeline. If an original unit
    /// has both hits and misses, the miss sub-units inherit seq_lba_ranges for their
    /// LBA range, and the writer does done_tx for them. If ALL blocks are hits,
    /// the dedup worker does done_tx for the whole unit.
    ///
    /// To avoid double-counting seqs in done_tx: each seq from the original unit
    /// is sent to done_tx exactly once — either by the dedup worker (for hit-only seqs)
    /// or by the writer (for seqs that have miss blocks flowing through the pipeline).
    fn dedup_loop(
        shard_idx: usize,
        rx: &Receiver<CoalesceUnit>,
        miss_tx: &Sender<CoalesceUnit>,
        meta: &MetaStore,
        pool: &WriteBufferPool,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        hole_map: &HoleMap,
        done_tx: &Sender<Vec<u64>>,
        running: &AtomicBool,
        skip_threshold_pct: u8,
        metrics: &EngineMetrics,
    ) {
        while running.load(Ordering::Relaxed) {
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(mut unit) => {
                    // Backpressure: skip dedup if this shard's buffer is filling up
                    if pool.fill_percentage_for_shard(shard_idx) > skip_threshold_pct as u8 {
                        unit.dedup_skipped = true;
                        metrics.dedup_skipped_units.fetch_add(1, Ordering::Relaxed);
                        if miss_tx.send(unit).is_err() {
                            return;
                        }
                        continue;
                    }

                    // Split into 4KB blocks, hash each, check dedup index
                    let lba_count = unit.lba_count as usize;
                    let mut is_hit: Vec<bool> = vec![false; lba_count];
                    let mut all_hashes: Vec<ContentHash> = Vec::with_capacity(lba_count);
                    // Collect hit info for processing
                    let mut hit_infos: Vec<(usize, BlockmapValue, ContentHash)> = Vec::new();

                    for i in 0..lba_count {
                        let Some(block) = unit.raw_blocks.get(i) else {
                            all_hashes.push([0u8; 32]);
                            continue;
                        };
                        let block_data = block.bytes();
                        let hash: ContentHash = *blake3::hash(block_data).as_bytes();
                        all_hashes.push(hash);

                        // Look up dedup index
                        metrics.dedup_lookup_ops.fetch_add(1, Ordering::Relaxed);
                        let lookup_start = Instant::now();
                        match meta.get_dedup_entry(&hash) {
                            Ok(Some(entry)) => {
                                Self::record_elapsed(&metrics.dedup_lookup_ns, lookup_start);
                                metrics.dedup_live_check_ops.fetch_add(1, Ordering::Relaxed);
                                let live_check_start = Instant::now();
                                match meta.dedup_entry_is_live(&hash, &entry) {
                                    Ok(true) => {
                                        Self::record_elapsed(
                                            &metrics.dedup_live_check_ns,
                                            live_check_start,
                                        );
                                        is_hit[i] = true;
                                        hit_infos.push((i, entry.to_blockmap_value(), hash));
                                    }
                                    Ok(false) => {
                                        Self::record_elapsed(
                                            &metrics.dedup_live_check_ns,
                                            live_check_start,
                                        );
                                        metrics
                                            .dedup_stale_index_entries
                                            .fetch_add(1, Ordering::Relaxed);
                                        let stale_delete_start = Instant::now();
                                        let _ = meta.delete_dedup_index(&hash);
                                        Self::record_elapsed(
                                            &metrics.dedup_stale_delete_ns,
                                            stale_delete_start,
                                        );
                                    }
                                    Err(e) => {
                                        Self::record_elapsed(
                                            &metrics.dedup_live_check_ns,
                                            live_check_start,
                                        );
                                        tracing::warn!(
                                            error = %e,
                                            pba = entry.pba.0,
                                            "dedup worker: failed to validate dedup entry liveness"
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                Self::record_elapsed(&metrics.dedup_lookup_ns, lookup_start);
                                tracing::warn!(error = %e, "dedup worker: failed to read dedup entry");
                            }
                            Ok(None) => {
                                Self::record_elapsed(&metrics.dedup_lookup_ns, lookup_start);
                            }
                        }
                    }

                    // Process hits in a single batch for this CoalesceUnit.
                    // Phase A: pre-filter stale hits (outside lock).
                    let mut successful_hit_indices: Vec<usize> = Vec::new();
                    let mut valid_hits: Vec<(usize, BlockmapValue, ContentHash)> = Vec::new();
                    for (i, existing_value, hash) in &hit_infos {
                        let lba = Lba(unit.start_lba.0 + *i as u64);
                        let vol_id_str = &unit.vol_id;

                        // Staleness guard: if a newer write superseded this LBA
                        // in the buffer, skip the dedup hit to avoid overwriting
                        // the newer blockmap entry.
                        let latest_seq = Self::latest_seq_for_lba(&unit.seq_lba_ranges, lba);
                        if !pool.is_latest_lba_seq(vol_id_str, lba, latest_seq, unit.vol_created_at)
                        {
                            successful_hit_indices.push(*i);
                            continue;
                        }
                        valid_hits.push((*i, *existing_value, *hash));
                    }

                    // Phase B: batch commit all valid hits under one lifecycle
                    // read lock + one refcount_lock acquisition.
                    if !valid_hits.is_empty() {
                        let vol_id_str = &unit.vol_id;
                        let vol_id = VolumeId(vol_id_str.clone());

                        metrics
                            .dedup_hit_commit_ops
                            .fetch_add(valid_hits.len() as u64, Ordering::Relaxed);
                        let hit_commit_start = Instant::now();

                        let batch_result = lifecycle.with_read_lock(
                            vol_id_str,
                            || -> OnyxResult<Vec<(usize, DedupHitResult)>> {
                                // Generation check (one call for entire batch)
                                let should_discard = match meta.get_volume(&vol_id)? {
                                    None => true,
                                    Some(vc) if vc.created_at != unit.vol_created_at => true,
                                    _ => false,
                                };
                                if should_discard {
                                    // Treat all as discarded (successfully handled)
                                    return Ok(valid_hits
                                        .iter()
                                        .map(|(i, _, _)| (*i, DedupHitResult::Accepted(None)))
                                        .collect());
                                }

                                // Build batch input, filtering out test-injected failures.
                                let mut batch_input: Vec<(Lba, BlockmapValue, ContentHash)> =
                                    Vec::with_capacity(valid_hits.len());
                                let mut input_map: Vec<usize> =
                                    Vec::with_capacity(valid_hits.len());
                                for &(i, ref existing_value, ref hash) in &valid_hits {
                                    let lba = Lba(unit.start_lba.0 + i as u64);
                                    if maybe_inject_dedup_hit_failure(vol_id_str, lba).is_ok() {
                                        batch_input.push((lba, *existing_value, *hash));
                                        input_map.push(i);
                                    }
                                }

                                let batch_results =
                                    meta.atomic_batch_dedup_hits(&vol_id, &batch_input)?;

                                let mut combined: Vec<(usize, DedupHitResult)> =
                                    Vec::with_capacity(valid_hits.len());
                                for (j, result) in batch_results.into_iter().enumerate() {
                                    combined.push((input_map[j], result));
                                }
                                // Injection-failed hits: demote to Rejected so they
                                // get is_hit[i]=false and go through the write path.
                                for &(i, _, _) in &valid_hits {
                                    if !input_map.contains(&i) {
                                        combined.push((i, DedupHitResult::Rejected));
                                    }
                                }
                                Ok(combined)
                            },
                        );
                        Self::record_elapsed(&metrics.dedup_hit_commit_ns, hit_commit_start);

                        // Phase C: process results
                        match batch_result {
                            Ok(results) => {
                                let mut dead_pbas: HashMap<Pba, u32> = HashMap::new();
                                for (i, result) in &results {
                                    match result {
                                        DedupHitResult::Accepted(decremented) => {
                                            successful_hit_indices.push(*i);
                                            record_dead_pba_cleanup(&mut dead_pbas, *decremented);
                                        }
                                        DedupHitResult::Rejected => {
                                            metrics
                                                .dedup_hit_failures
                                                .fetch_add(1, Ordering::Relaxed);
                                            is_hit[*i] = false;
                                            tracing::warn!(
                                                vol = unit.vol_id,
                                                lba = unit.start_lba.0 + *i as u64,
                                                "dedup worker: hit rejected (target PBA freed), demoting to miss"
                                            );
                                        }
                                    }
                                }
                                // Phase D: cleanup dead PBAs outside the lock
                                for (old_pba, old_blocks) in dead_pbas {
                                    Self::cleanup_dead_pba_post_commit(
                                        meta,
                                        allocator,
                                        hole_map,
                                        old_pba,
                                        old_blocks,
                                        "dedup_worker_hit_cleanup",
                                    );
                                }
                            }
                            Err(e) => {
                                // Entire batch failed — demote all valid_hits to misses.
                                metrics
                                    .dedup_hit_failures
                                    .fetch_add(valid_hits.len() as u64, Ordering::Relaxed);
                                for (i, _, _) in &valid_hits {
                                    is_hit[*i] = false;
                                }
                                tracing::error!(
                                    vol = unit.vol_id,
                                    count = valid_hits.len(),
                                    error = %e,
                                    "dedup worker: batch hit failed, demoting all to miss"
                                );
                            }
                        }
                    }

                    // Only mark_flushed for hits that actually succeeded
                    if !successful_hit_indices.is_empty() {
                        for i in &successful_hit_indices {
                            let lba = Lba(unit.start_lba.0 + *i as u64);
                            for (seq, range_start, range_count) in &unit.seq_lba_ranges {
                                if lba.0 >= range_start.0
                                    && lba.0 < range_start.0 + *range_count as u64
                                {
                                    let _ = pool.mark_flushed(*seq, lba, 1);
                                }
                            }
                        }
                        let _ = pool.advance_tail_for_shard(shard_idx);
                    }

                    // Recheck has_misses after potential demotions
                    let has_misses = is_hit.iter().any(|h| !h);
                    metrics
                        .dedup_hits
                        .fetch_add(successful_hit_indices.len() as u64, Ordering::Relaxed);
                    metrics.dedup_misses.fetch_add(
                        is_hit.iter().filter(|hit| !**hit).count() as u64,
                        Ordering::Relaxed,
                    );
                    if !has_misses {
                        // All blocks were hits — send done_tx for the original unit's seqs
                        let seqs: Vec<u64> =
                            unit.seq_lba_ranges.iter().map(|(s, _, _)| *s).collect();
                        let _ = done_tx.send(seqs);
                        continue;
                    }

                    // Re-coalesce consecutive miss blocks into CoalesceUnits.
                    // Count miss sub-units first to create a shared DedupCompletion.
                    let mut miss_ranges: Vec<(usize, usize)> = Vec::new();
                    let mut miss_start: Option<usize> = None;
                    for i in 0..lba_count {
                        if !is_hit[i] {
                            if miss_start.is_none() {
                                miss_start = Some(i);
                            }
                        } else if let Some(start) = miss_start.take() {
                            miss_ranges.push((start, i));
                        }
                    }
                    if let Some(start) = miss_start {
                        miss_ranges.push((start, lba_count));
                    }

                    // Create shared countdown: all miss sub-units share this.
                    // The LAST sub-unit to complete sends done_tx with the
                    // original unit's full seq list.
                    let all_seqs: Vec<u64> =
                        unit.seq_lba_ranges.iter().map(|(s, _, _)| *s).collect();
                    let completion = crate::buffer::pipeline::DedupCompletion::new(
                        miss_ranges.len() as u32,
                        all_seqs,
                    );

                    let mut miss_units: Vec<CoalesceUnit> = Vec::new();
                    for (start, end) in &miss_ranges {
                        miss_units.push(Self::build_miss_unit(
                            &unit,
                            *start,
                            *end,
                            &all_hashes,
                            Some(completion.clone()),
                        ));
                    }

                    // Send miss units to compress stage
                    for mu in miss_units {
                        if miss_tx.send(mu).is_err() {
                            return;
                        }
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    /// Build a CoalesceUnit from a contiguous range of miss blocks [start, end).
    fn build_miss_unit(
        original: &CoalesceUnit,
        start_idx: usize,
        end_idx: usize,
        hashes: &[ContentHash],
        dedup_completion: Option<Arc<crate::buffer::pipeline::DedupCompletion>>,
    ) -> CoalesceUnit {
        let start_lba = Lba(original.start_lba.0 + start_idx as u64);
        let lba_count = (end_idx - start_idx) as u32;
        let raw_blocks = original.raw_blocks[start_idx..end_idx].to_vec();

        // Build seq_lba_ranges for the sub-range
        let mut seq_lba_ranges = Vec::new();
        for i in start_idx..end_idx {
            let lba = Lba(original.start_lba.0 + i as u64);
            for (seq, range_start, range_count) in &original.seq_lba_ranges {
                if lba.0 >= range_start.0 && lba.0 < range_start.0 + *range_count as u64 {
                    // Use add_seq_lba logic: extend or start new range
                    if let Some(existing) = seq_lba_ranges.iter_mut().find(
                        |(s, start, count): &&mut (u64, Lba, u32)| {
                            *s == *seq && start.0 + *count as u64 == lba.0
                        },
                    ) {
                        existing.2 += 1;
                    } else {
                        seq_lba_ranges.push((*seq, lba, 1));
                    }
                    // Don't break: multiple seqs can reference the same LBA
                    // (e.g., overwrite dedup in coalescer keeps all seqs)
                }
            }
        }

        let block_hashes_slice = hashes[start_idx..end_idx].to_vec();
        CoalesceUnit {
            vol_id: original.vol_id.clone(),
            start_lba,
            lba_count,
            raw_blocks,
            compression: original.compression,
            vol_created_at: original.vol_created_at,
            seq_lba_ranges,
            dedup_skipped: false,
            block_hashes: Some(block_hashes_slice),
            dedup_completion,
        }
    }

    const WRITER_BATCH_SIZE: usize = 32;
    const RETRY_BACKOFF: Duration = Duration::from_secs(1);
    const PACKED_SLOT_MAX_AGE: Duration = Duration::from_millis(50);

    fn writer_loop(
        shard_idx: usize,
        rx: &Receiver<CompressedUnit>,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        done_tx: &Sender<Vec<u64>>,
        running: &AtomicBool,
        in_flight_tracker: &FlusherInFlightTracker,
        packer: &mut Packer,
        metrics: &EngineMetrics,
    ) {
        let mut buffered_seqs: Vec<u64> = Vec::new();
        let mut buffered_completions: Vec<Arc<crate::buffer::pipeline::DedupCompletion>> =
            Vec::new();
        let mut packed_retries: VecDeque<PackedSlotRetry> = VecDeque::new();
        let mut tail_dirty = false;

        /// Helper: flush accumulated Passthrough units through write_units_batch.
        macro_rules! flush_pt_batch {
            ($batch:expr, $batch_seqs:expr, $batch_completions:expr) => {
                if !$batch.is_empty() {
                    let results = Self::write_units_batch(
                        shard_idx, &$batch, pool, meta, lifecycle, allocator,
                        io_engine, packer.hole_map(), metrics,
                    );
                    for (idx, result) in results.into_iter().enumerate() {
                        if let Err(e) = result {
                            metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                            match &$batch_completions[idx] {
                                None => {
                                    in_flight_tracker
                                        .defer_retry(&$batch_seqs[idx], Self::RETRY_BACKOFF);
                                }
                                Some(dc) => {
                                    in_flight_tracker
                                        .defer_retry(dc.seqs(), Self::RETRY_BACKOFF);
                                }
                            }
                            tracing::error!(
                                vol = $batch[idx].vol_id,
                                start_lba = $batch[idx].start_lba.0,
                                error = %e,
                                "writer: failed to flush unit in batch"
                            );
                        }
                        match &$batch_completions[idx] {
                            None => { let _ = done_tx.send($batch_seqs[idx].clone()); }
                            Some(dc) => {
                                if let Some(original_seqs) = dc.decrement() {
                                    let _ = done_tx.send(original_seqs);
                                }
                            }
                        }
                    }
                    $batch.clear();
                    $batch_seqs.clear();
                    $batch_completions.clear();
                    tail_dirty = true;
                }
            };
        }

        while running.load(Ordering::Relaxed) {
            if Self::retry_one_packed_slot(
                shard_idx,
                &mut packed_retries,
                pool,
                meta,
                lifecycle,
                allocator,
                io_engine,
                done_tx,
                packer.hole_map(),
                metrics,
            ) {
                tail_dirty = true;
            }

            if Self::flush_aged_open_slot(
                shard_idx,
                packer,
                &mut buffered_seqs,
                &mut buffered_completions,
                &mut packed_retries,
                pool,
                meta,
                lifecycle,
                allocator,
                io_engine,
                done_tx,
                metrics,
            ) {
                tail_dirty = true;
            }

            let first = match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(unit) => unit,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if let Some(sealed) = packer.flush_open_slot() {
                        if let Err(e) = Self::write_packed_slot(
                            shard_idx,
                            &sealed,
                            pool,
                            meta,
                            lifecycle,
                            allocator,
                            io_engine,
                            packer.hole_map(),
                            metrics,
                        ) {
                            metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                            let failed_pba = sealed.pba;
                            Self::queue_packed_slot_retry(
                                &mut packed_retries,
                                sealed,
                                &mut buffered_seqs,
                                &mut buffered_completions,
                            );
                            tracing::error!(
                                pba = failed_pba.0,
                                error = %e,
                                "writer: failed to flush packed slot on idle; queued whole-slot retry"
                            );
                        } else {
                            Self::flush_buffered_done(
                                &mut buffered_seqs,
                                &mut buffered_completions,
                                done_tx,
                            );
                            tail_dirty = true;
                        }
                    }
                    if tail_dirty {
                        let _ = pool.advance_tail_for_shard(shard_idx);
                        tail_dirty = false;
                    }
                    continue;
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            };

            // Drain up to WRITER_BATCH_SIZE units.
            let mut incoming = vec![first];
            while incoming.len() < Self::WRITER_BATCH_SIZE {
                match rx.try_recv() {
                    Ok(unit) => incoming.push(unit),
                    Err(_) => break,
                }
            }

            // Run through packer, collect Passthrough units for batching.
            let mut pt_batch: Vec<CompressedUnit> = Vec::new();
            let mut pt_seqs: Vec<Vec<u64>> = Vec::new();
            let mut pt_completions: Vec<Option<Arc<crate::buffer::pipeline::DedupCompletion>>> =
                Vec::new();

            for unit in incoming {
                let seqs: Vec<u64> = unit.seq_lba_ranges.iter().map(|(s, _, _)| *s).collect();
                let completion = unit.dedup_completion.clone();

                match packer.pack_or_passthrough(unit) {
                    Ok(PackResult::Passthrough(unit)) => {
                        pt_batch.push(unit);
                        pt_seqs.push(seqs);
                        pt_completions.push(completion);
                    }
                    Ok(PackResult::Buffered) => match &completion {
                        None => buffered_seqs.extend(&seqs),
                        Some(dc) => buffered_completions.push(dc.clone()),
                    },
                    Ok(PackResult::SealedSlot(sealed)) => {
                        flush_pt_batch!(pt_batch, pt_seqs, pt_completions);
                        if let Err(e) = Self::write_packed_slot(
                            shard_idx,
                            &sealed,
                            pool,
                            meta,
                            lifecycle,
                            allocator,
                            io_engine,
                            packer.hole_map(),
                            metrics,
                        ) {
                            metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                            let failed_pba = sealed.pba;
                            Self::queue_packed_slot_retry(
                                &mut packed_retries,
                                sealed,
                                &mut buffered_seqs,
                                &mut buffered_completions,
                            );
                            tracing::error!(
                                pba = failed_pba.0,
                                error = %e,
                                "writer: failed to flush packed slot; queued whole-slot retry"
                            );
                        } else {
                            Self::flush_buffered_done(
                                &mut buffered_seqs,
                                &mut buffered_completions,
                                done_tx,
                            );
                            tail_dirty = true;
                        }
                        match &completion {
                            None => buffered_seqs.extend(&seqs),
                            Some(dc) => buffered_completions.push(dc.clone()),
                        }
                    }
                    Ok(PackResult::SealedSlotAndPassthrough(sealed, unit)) => {
                        flush_pt_batch!(pt_batch, pt_seqs, pt_completions);
                        if let Err(e) = Self::write_packed_slot(
                            shard_idx,
                            &sealed,
                            pool,
                            meta,
                            lifecycle,
                            allocator,
                            io_engine,
                            packer.hole_map(),
                            metrics,
                        ) {
                            metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                            let failed_pba = sealed.pba;
                            Self::queue_packed_slot_retry(
                                &mut packed_retries,
                                sealed,
                                &mut buffered_seqs,
                                &mut buffered_completions,
                            );
                            tracing::error!(
                                pba = failed_pba.0,
                                error = %e,
                                "writer: failed to flush packed slot (passthrough fallback); queued whole-slot retry"
                            );
                        } else {
                            Self::flush_buffered_done(
                                &mut buffered_seqs,
                                &mut buffered_completions,
                                done_tx,
                            );
                            tail_dirty = true;
                        }
                        pt_batch.push(unit);
                        pt_seqs.push(seqs);
                        pt_completions.push(completion);
                    }
                    Ok(PackResult::FillHole(fill)) => {
                        flush_pt_batch!(pt_batch, pt_seqs, pt_completions);
                        if let Err(e) = Self::write_hole_fill(
                            shard_idx,
                            &fill,
                            pool,
                            meta,
                            lifecycle,
                            allocator,
                            io_engine,
                            packer.hole_map(),
                            metrics,
                        ) {
                            metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                            match &completion {
                                None => in_flight_tracker.defer_retry(&seqs, Self::RETRY_BACKOFF),
                                Some(dc) => {
                                    in_flight_tracker.defer_retry(dc.seqs(), Self::RETRY_BACKOFF)
                                }
                            }
                            tracing::warn!(error = %e, "writer: hole fill rejected (will retry via normal path)");
                        }
                        match &completion {
                            None => {
                                let _ = done_tx.send(seqs);
                            }
                            Some(dc) => {
                                if let Some(original_seqs) = dc.decrement() {
                                    let _ = done_tx.send(original_seqs);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                        match &completion {
                            None => in_flight_tracker.defer_retry(&seqs, Self::RETRY_BACKOFF),
                            Some(dc) => {
                                in_flight_tracker.defer_retry(dc.seqs(), Self::RETRY_BACKOFF)
                            }
                        }
                        tracing::error!(error = %e, "writer: packer error");
                        match &completion {
                            None => {
                                let _ = done_tx.send(seqs);
                            }
                            Some(dc) => {
                                if let Some(original_seqs) = dc.decrement() {
                                    let _ = done_tx.send(original_seqs);
                                }
                            }
                        }
                    }
                }
            }

            flush_pt_batch!(pt_batch, pt_seqs, pt_completions);
        }

        // Drain remaining on shutdown (use per-unit path for simplicity).
        while let Ok(unit) = rx.try_recv() {
            Self::handle_compressed_unit(
                shard_idx,
                unit,
                pool,
                meta,
                lifecycle,
                allocator,
                io_engine,
                done_tx,
                packer,
                &mut buffered_seqs,
                &mut buffered_completions,
                metrics,
            );
            tail_dirty = true;
        }

        while Self::retry_one_packed_slot(
            shard_idx,
            &mut packed_retries,
            pool,
            meta,
            lifecycle,
            allocator,
            io_engine,
            done_tx,
            packer.hole_map(),
            metrics,
        ) {
            tail_dirty = true;
        }

        if let Some(sealed) = packer.flush_open_slot() {
            if let Err(e) = Self::write_packed_slot(
                shard_idx,
                &sealed,
                pool,
                meta,
                lifecycle,
                allocator,
                io_engine,
                packer.hole_map(),
                metrics,
            ) {
                metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                tracing::error!(pba = sealed.pba.0, error = %e,
                    "writer: failed to flush final packed slot on shutdown");
            }
            Self::flush_buffered_done(&mut buffered_seqs, &mut buffered_completions, done_tx);
            tail_dirty = true;
        }

        if tail_dirty {
            let _ = pool.advance_tail_for_shard(shard_idx);
        }
    }

    fn flush_aged_open_slot(
        shard_idx: usize,
        packer: &mut Packer,
        buffered_seqs: &mut Vec<u64>,
        buffered_completions: &mut Vec<Arc<crate::buffer::pipeline::DedupCompletion>>,
        packed_retries: &mut VecDeque<PackedSlotRetry>,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        done_tx: &Sender<Vec<u64>>,
        metrics: &EngineMetrics,
    ) -> bool {
        let Some(sealed) = packer.flush_open_slot_if_older_than(Self::PACKED_SLOT_MAX_AGE) else {
            return false;
        };
        if let Err(e) = Self::write_packed_slot(
            shard_idx,
            &sealed,
            pool,
            meta,
            lifecycle,
            allocator,
            io_engine,
            packer.hole_map(),
            metrics,
        ) {
            metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
            let failed_pba = sealed.pba;
            Self::queue_packed_slot_retry(
                packed_retries,
                sealed,
                buffered_seqs,
                buffered_completions,
            );
            tracing::error!(
                pba = failed_pba.0,
                error = %e,
                "writer: failed to flush aged packed slot; queued whole-slot retry"
            );
        } else {
            Self::flush_buffered_done(buffered_seqs, buffered_completions, done_tx);
        }
        true
    }

    /// Flush buffered done_tx for sealed packer slots.
    /// Handles both normal seqs and dedup completion counters.
    fn flush_buffered_done(
        buffered_seqs: &mut Vec<u64>,
        buffered_completions: &mut Vec<Arc<crate::buffer::pipeline::DedupCompletion>>,
        done_tx: &Sender<Vec<u64>>,
    ) {
        // Normal (non-dedup) buffered seqs
        let normal_seqs: Vec<u64> = buffered_seqs.drain(..).collect();
        if !normal_seqs.is_empty() {
            let _ = done_tx.send(normal_seqs);
        }
        // Dedup completion counters
        for dc in buffered_completions.drain(..) {
            if let Some(original_seqs) = dc.decrement() {
                let _ = done_tx.send(original_seqs);
            }
        }
    }

    fn queue_packed_slot_retry(
        retries: &mut VecDeque<PackedSlotRetry>,
        sealed: SealedSlot,
        buffered_seqs: &mut Vec<u64>,
        buffered_completions: &mut Vec<Arc<crate::buffer::pipeline::DedupCompletion>>,
    ) {
        retries.push_back(PackedSlotRetry {
            sealed,
            buffered_seqs: std::mem::take(buffered_seqs),
            buffered_completions: std::mem::take(buffered_completions),
            retry_at: Instant::now() + Self::RETRY_BACKOFF,
        });
    }

    fn retry_one_packed_slot(
        shard_idx: usize,
        retries: &mut VecDeque<PackedSlotRetry>,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        done_tx: &Sender<Vec<u64>>,
        hole_map: &HoleMap,
        metrics: &EngineMetrics,
    ) -> bool {
        let Some(retry_at) = retries.front().map(|retry| retry.retry_at) else {
            return false;
        };
        if retry_at > Instant::now() {
            return false;
        }

        let mut retry = retries.pop_front().expect("front checked above");
        let new_pba = match allocator.allocate_one_for_lane(shard_idx) {
            Ok(pba) => pba,
            Err(e) => {
                metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                retry.retry_at = Instant::now() + Self::RETRY_BACKOFF;
                retries.push_back(retry);
                tracing::warn!(
                    lane = shard_idx,
                    error = %e,
                    "writer: failed to allocate PBA for packed-slot retry"
                );
                return false;
            }
        };
        retry.sealed.pba = new_pba;

        match Self::write_packed_slot(
            shard_idx,
            &retry.sealed,
            pool,
            meta,
            lifecycle,
            allocator,
            io_engine,
            hole_map,
            metrics,
        ) {
            Ok(()) => {
                let mut buffered_seqs = retry.buffered_seqs;
                let mut buffered_completions = retry.buffered_completions;
                Self::flush_buffered_done(&mut buffered_seqs, &mut buffered_completions, done_tx);
                true
            }
            Err(e) => {
                metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                retry.retry_at = Instant::now() + Self::RETRY_BACKOFF;
                retries.push_back(retry);
                tracing::error!(
                    lane = shard_idx,
                    pba = new_pba.0,
                    error = %e,
                    "writer: packed-slot retry failed; will retry whole slot again"
                );
                false
            }
        }
    }

    /// Handle a compressed unit in the writer thread.
    fn handle_compressed_unit(
        shard_idx: usize,
        unit: CompressedUnit,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        done_tx: &Sender<Vec<u64>>,
        packer: &mut Packer,
        buffered_seqs: &mut Vec<u64>,
        buffered_completions: &mut Vec<Arc<crate::buffer::pipeline::DedupCompletion>>,
        metrics: &EngineMetrics,
    ) {
        let seqs: Vec<u64> = unit.seq_lba_ranges.iter().map(|(s, _, _)| *s).collect();
        let completion = unit.dedup_completion.clone();

        /// Send done_tx for this unit's completion.
        /// - Normal path (no dedup_completion): send seqs directly.
        /// - Dedup split path: decrement the shared counter; only the last
        ///   sub-unit to finish sends done_tx with the ORIGINAL unit's full seqs.
        macro_rules! signal_done {
            ($own_seqs:expr) => {
                match &completion {
                    None => {
                        let _ = done_tx.send($own_seqs);
                    }
                    Some(dc) => {
                        if let Some(original_seqs) = dc.decrement() {
                            let _ = done_tx.send(original_seqs);
                        }
                    }
                }
            };
        }

        match packer.pack_or_passthrough(unit) {
            Ok(PackResult::Passthrough(unit)) => {
                if let Err(e) = Self::write_unit(
                    shard_idx,
                    &unit,
                    pool,
                    meta,
                    lifecycle,
                    allocator,
                    io_engine,
                    packer.hole_map(),
                    metrics,
                ) {
                    metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        vol = unit.vol_id,
                        start_lba = unit.start_lba.0,
                        lba_count = unit.lba_count,
                        error = %e,
                        "writer: failed to flush unit"
                    );
                }
                signal_done!(seqs);
            }
            Ok(PackResult::Buffered) => match &completion {
                None => buffered_seqs.extend(&seqs),
                Some(dc) => buffered_completions.push(dc.clone()),
            },
            Ok(PackResult::SealedSlot(sealed)) => {
                if let Err(e) = Self::write_packed_slot(
                    shard_idx,
                    &sealed,
                    pool,
                    meta,
                    lifecycle,
                    allocator,
                    io_engine,
                    packer.hole_map(),
                    metrics,
                ) {
                    metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        pba = sealed.pba.0,
                        fragments = sealed.fragments.len(),
                        error = %e,
                        "writer: failed to flush packed slot"
                    );
                }
                // Flush done_tx for previously buffered units in the sealed slot
                Self::flush_buffered_done(buffered_seqs, buffered_completions, done_tx);
                // Current unit goes into the new open slot
                match &completion {
                    None => buffered_seqs.extend(&seqs),
                    Some(dc) => buffered_completions.push(dc.clone()),
                }
            }
            Ok(PackResult::SealedSlotAndPassthrough(sealed, unit)) => {
                if let Err(e) = Self::write_packed_slot(
                    shard_idx,
                    &sealed,
                    pool,
                    meta,
                    lifecycle,
                    allocator,
                    io_engine,
                    packer.hole_map(),
                    metrics,
                ) {
                    metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        pba = sealed.pba.0,
                        error = %e,
                        "writer: failed to flush packed slot (alloc fallback)"
                    );
                }
                Self::flush_buffered_done(buffered_seqs, buffered_completions, done_tx);

                if let Err(e) = Self::write_unit(
                    shard_idx,
                    &unit,
                    pool,
                    meta,
                    lifecycle,
                    allocator,
                    io_engine,
                    packer.hole_map(),
                    metrics,
                ) {
                    metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        vol = unit.vol_id,
                        error = %e,
                        "writer: failed to flush unit (alloc fallback)"
                    );
                }
                signal_done!(seqs);
            }
            Ok(PackResult::FillHole(fill)) => {
                if let Err(e) = Self::write_hole_fill(
                    shard_idx,
                    &fill,
                    pool,
                    meta,
                    lifecycle,
                    allocator,
                    io_engine,
                    packer.hole_map(),
                    metrics,
                ) {
                    metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        pba = fill.pba.0,
                        slot_offset = fill.slot_offset,
                        error = %e,
                        "writer: hole fill rejected (will retry via normal path)"
                    );
                }
                signal_done!(seqs);
            }
            Err(e) => {
                metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                tracing::error!(error = %e, "writer: packer error");
                signal_done!(seqs);
            }
        }
    }

    fn write_unit(
        shard_idx: usize,
        unit: &CompressedUnit,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        hole_map: &HoleMap,
        metrics: &EngineMetrics,
    ) -> OnyxResult<()> {
        lifecycle.with_read_lock(&unit.vol_id, || {
            let total_start = Instant::now();
            // Hold the lifecycle read lock from generation validation through
            // metadata commit so delete/create cannot interleave with this flush.
            let vol_id = VolumeId(unit.vol_id.clone());
            let should_discard = match meta.get_volume(&vol_id)? {
                None => true,
                Some(vc) if vc.created_at != unit.vol_created_at => {
                    tracing::debug!(
                        vol = unit.vol_id,
                        entry_gen = unit.vol_created_at,
                        current_gen = vc.created_at,
                        "write_unit: generation mismatch, discarding stale unit"
                    );
                    true
                }
                _ => false,
            };
            if should_discard {
                metrics.flush_stale_discards.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    vol = unit.vol_id,
                    "write_unit: discarding unit (volume deleted or generation mismatch)"
                );
                for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                    let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
                }
                let _ = pool.advance_tail_for_shard(shard_idx);
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                return Ok(());
            }

            let bs = BLOCK_SIZE as usize;
            let alloc_start = Instant::now();
            let blocks_needed = (unit.compressed_data.len() + bs - 1) / bs;

            let allocation = if blocks_needed == 1 {
                Allocation::Single(allocator.allocate_one_for_lane(shard_idx)?)
            } else {
                let extent = allocator.allocate_extent(blocks_needed as u32)?;
                if (extent.count as usize) < blocks_needed {
                    allocator.free_extent(extent)?;
                    Self::record_elapsed(&metrics.flush_writer_alloc_ns, alloc_start);
                    Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                    return Err(crate::error::OnyxError::SpaceExhausted);
                }
                Allocation::Extent(extent)
            };
            Self::record_elapsed(&metrics.flush_writer_alloc_ns, alloc_start);
            let pba = allocation.start_pba();

            // Serialize with concurrent hole fills targeting any PBA in this
            // allocation (each block may be recycled from a previous packed slot).
            let alloc_count = allocation.block_count();
            let pba_locks: Vec<_> = (0..alloc_count)
                .map(|i| Self::hole_fill_lock(Pba(pba.0 + i as u64)))
                .collect();
            let _pba_guards: Vec<_> = pba_locks.iter().map(|l| l.lock().unwrap()).collect();
            for i in 0..alloc_count {
                crate::packer::packer::remove_holes_for_pba(hole_map, Pba(pba.0 + i as u64));
            }

            let io_start = Instant::now();
            if let Err(e) = maybe_inject_test_failure(
                &unit.vol_id,
                unit.start_lba,
                FlushFailStage::BeforeIoWrite,
            ) {
                allocation.free(allocator)?;
                Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                return Err(e);
            }

            if let Err(e) = io_engine.write_blocks(pba, &unit.compressed_data) {
                allocation.free(allocator)?;
                Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                return Err(e);
            }
            Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);

            let meta_start = Instant::now();
            let live_positions = Self::live_positions_for_unit(unit, pool)?;
            if live_positions.is_empty() {
                allocation.free(allocator)?;
                let mark_start = Instant::now();
                for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                    if let Err(e) = pool.mark_flushed(*seq, *lba_start, *lba_count) {
                        tracing::warn!(seq, error = %e, "failed to mark stale entry flushed");
                    }
                }
                Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);
                let _ = pool.advance_tail_for_shard(shard_idx);
                Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                return Ok(());
            }
            // Per-fragment tracking for hole detection: (pba, slot_offset) → (decrement, unit_lba_count, compressed_size)
            let mut old_frag_meta: HashMap<(Pba, u16), OldFragmentRef> = HashMap::new();

            // Batch read old mappings for hole detection (stale reads are safe here;
            // refcount decrements are re-computed inside the lock by atomic_batch_write).
            let lbas: Vec<Lba> = live_positions
                .iter()
                .map(|idx| Lba(unit.start_lba.0 + *idx as u64))
                .collect();
            let old_mappings = meta.multi_get_mappings(&vol_id, &lbas)?;

            let mut batch_values = Vec::with_capacity(live_positions.len());
            for (i, old) in old_mappings.into_iter().enumerate() {
                if let Some(old) = old {
                    record_old_fragment_ref(&mut old_frag_meta, old);
                }
                let flags = if unit.dedup_skipped {
                    FLAG_DEDUP_SKIPPED
                } else {
                    0
                };
                batch_values.push((
                    lbas[i],
                    BlockmapValue {
                        pba,
                        compression: unit.compression,
                        unit_compressed_size: unit.compressed_data.len() as u32,
                        unit_original_size: unit.original_size,
                        unit_lba_count: unit.lba_count as u16,
                        offset_in_unit: live_positions[i] as u16,
                        crc32: unit.crc32,
                        slot_offset: 0,
                        flags,
                    },
                ));
            }

            if let Err(e) = maybe_inject_test_failure(
                &unit.vol_id,
                unit.start_lba,
                FlushFailStage::BeforeMetaWrite,
            ) {
                allocation.free(allocator)?;
                Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                return Err(e);
            }

            let actual_old_pba_meta = match meta.atomic_batch_write(
                &vol_id,
                &batch_values,
                live_positions.len() as u32,
            ) {
                Ok(m) => m,
                Err(e) => {
                    allocation.free(allocator)?;
                    Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
                    Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                    return Err(e);
                }
            };
            Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);

            let cleanup_start = Instant::now();
            for (old_pba, (_, old_blocks)) in &actual_old_pba_meta {
                Self::cleanup_dead_pba_post_commit(
                    meta,
                    allocator,
                    hole_map,
                    *old_pba,
                    *old_blocks,
                    "write_unit_cleanup",
                );
            }
            Self::record_elapsed(&metrics.flush_writer_cleanup_ns, cleanup_start);

            // Populate dedup index for newly written blocks
            let dedup_start = Instant::now();
            if let Some(ref hashes) = unit.block_hashes {
                let mut dedup_entries = Vec::new();
                for &pos in &live_positions {
                    let hash = hashes[pos];
                    if hash == [0u8; 32] {
                        continue; // Skip empty hashes
                    }
                    dedup_entries.push((
                        hash,
                        DedupEntry {
                            pba,
                            slot_offset: 0,
                            compression: unit.compression,
                            unit_compressed_size: unit.compressed_data.len() as u32,
                            unit_original_size: unit.original_size,
                            unit_lba_count: unit.lba_count as u16,
                            offset_in_unit: pos as u16,
                            crc32: unit.crc32,
                        },
                    ));
                }
                if !dedup_entries.is_empty() {
                    meta.put_dedup_entries(&dedup_entries)?;
                }
            }
            Self::record_elapsed(&metrics.flush_writer_dedup_index_ns, dedup_start);

            // Detect holes: fully-dead fragments in still-live packed PBAs
            let hole_start = Instant::now();
            Self::detect_holes(&old_frag_meta, meta, hole_map, metrics)?;
            Self::record_elapsed(&metrics.flush_writer_hole_detect_ns, hole_start);
            metrics.flush_units_written.fetch_add(1, Ordering::Relaxed);
            metrics
                .flush_unit_bytes
                .fetch_add(unit.compressed_data.len() as u64, Ordering::Relaxed);

            let mark_start = Instant::now();
            for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                if let Err(e) = pool.mark_flushed(*seq, *lba_start, *lba_count) {
                    tracing::warn!(seq, error = %e, "failed to mark entry flushed");
                }
            }
            Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);

            tracing::debug!(
                vol = unit.vol_id,
                start_lba = unit.start_lba.0,
                lba_count = unit.lba_count,
                pba = pba.0,
                compressed = unit.compressed_data.len(),
                original = unit.original_size,
                "flushed compression unit"
            );

            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            Ok(())
        })
    }

    /// Batch-write multiple Passthrough units in one go:
    /// 1. Acquire lifecycle read locks for all unique volumes (sorted, no deadlock)
    /// 2. Validate generations, discard stale units
    /// 3. Allocate PBAs for all units
    /// 4. Batch IO writes
    /// 5. Batch multi_get old mappings
    /// 6. ONE RocksDB WriteBatch for all blockmap + refcount updates
    /// 7. Batch cleanup + dedup index + mark_flushed
    fn write_units_batch(
        shard_idx: usize,
        units: &[CompressedUnit],
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        hole_map: &HoleMap,
        metrics: &EngineMetrics,
    ) -> Vec<OnyxResult<()>> {
        if units.is_empty() {
            return Vec::new();
        }
        let total_start = Instant::now();

        // Acquire lifecycle read locks for all unique volumes (sorted to prevent deadlock).
        let mut vol_ids: Vec<String> = units.iter().map(|u| u.vol_id.clone()).collect();
        vol_ids.sort();
        vol_ids.dedup();
        let locks: Vec<_> = vol_ids.iter().map(|vid| lifecycle.get_lock(vid)).collect();
        let _guards: Vec<_> = locks.iter().map(|l| l.read().unwrap()).collect();

        // Per-unit state tracking.
        let n = units.len();
        let mut results: Vec<OnyxResult<()>> = (0..n).map(|_| Ok(())).collect();
        let mut skip = vec![false; n]; // true = discard (stale volume)
        let mut pbas: Vec<Option<Pba>> = vec![None; n];
        let mut alloc_blocks: Vec<u32> = vec![0; n];

        // Phase 1: Validate generations.
        for (i, unit) in units.iter().enumerate() {
            let vol_id = VolumeId(unit.vol_id.clone());
            let should_discard = match meta.get_volume(&vol_id) {
                Ok(None) => true,
                Ok(Some(vc)) if vc.created_at != unit.vol_created_at => true,
                Ok(_) => false,
                Err(e) => {
                    results[i] = Err(e);
                    skip[i] = true;
                    continue;
                }
            };
            if should_discard {
                metrics.flush_stale_discards.fetch_add(1, Ordering::Relaxed);
                skip[i] = true;
                for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                    let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
                }
            }
        }

        // Phase 2: Allocate PBAs for non-skipped units.
        let alloc_start = Instant::now();
        for (i, unit) in units.iter().enumerate() {
            if skip[i] {
                continue;
            }
            let bs = BLOCK_SIZE as usize;
            let blocks_needed = (unit.compressed_data.len() + bs - 1) / bs;
            alloc_blocks[i] = blocks_needed as u32;

            let allocation = if blocks_needed == 1 {
                allocator
                    .allocate_one_for_lane(shard_idx)
                    .map(|pba| (pba, 1u32))
            } else {
                allocator
                    .allocate_extent(blocks_needed as u32)
                    .and_then(|ext| {
                        if (ext.count as usize) < blocks_needed {
                            allocator.free_extent(ext)?;
                            Err(crate::error::OnyxError::SpaceExhausted)
                        } else {
                            Ok((ext.start, ext.count))
                        }
                    })
            };
            match allocation {
                Ok((pba, _)) => pbas[i] = Some(pba),
                Err(e) => {
                    results[i] = Err(e);
                    skip[i] = true;
                }
            }
        }
        Self::record_elapsed(&metrics.flush_writer_alloc_ns, alloc_start);

        // Serialize with concurrent hole fills on any recycled PBA.
        // Expand each unit's extent into individual PBAs, sorted to prevent deadlock.
        let mut all_pba_list: Vec<Pba> = Vec::new();
        for i in 0..n {
            if skip[i] || pbas[i].is_none() {
                continue;
            }
            let start = pbas[i].unwrap();
            for b in 0..alloc_blocks[i] {
                all_pba_list.push(Pba(start.0 + b as u64));
            }
        }
        all_pba_list.sort_by_key(|p| p.0);
        all_pba_list.dedup();
        let pba_locks: Vec<_> = all_pba_list
            .iter()
            .map(|p| Self::hole_fill_lock(*p))
            .collect();
        let _pba_guards: Vec<_> = pba_locks.iter().map(|l| l.lock().unwrap()).collect();
        for p in &all_pba_list {
            crate::packer::packer::remove_holes_for_pba(hole_map, *p);
        }

        // Phase 3: Parallel IO writes (one scoped thread per unit for NVMe QD > 1).
        let io_start = Instant::now();
        {
            let io_errors: Vec<Option<crate::error::OnyxError>> = std::thread::scope(|s| {
                let handles: Vec<_> = (0..n)
                    .filter(|i| !skip[*i])
                    .map(|i| {
                        let pba = pbas[i].unwrap();
                        let data = &units[i].compressed_data;
                        let blk = alloc_blocks[i];
                        (i, blk, s.spawn(move || io_engine.write_blocks(pba, data)))
                    })
                    .collect();
                let mut errs: Vec<Option<crate::error::OnyxError>> = (0..n).map(|_| None).collect();
                for (i, blk, handle) in handles {
                    match handle.join() {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            let pba = pbas[i].unwrap();
                            if blk == 1 {
                                let _ = allocator.free_one(pba);
                            } else {
                                let _ = allocator.free_extent(Extent::new(pba, blk));
                            }
                            errs[i] = Some(e);
                        }
                        Err(_) => {
                            let pba = pbas[i].unwrap();
                            if blk == 1 {
                                let _ = allocator.free_one(pba);
                            } else {
                                let _ = allocator.free_extent(Extent::new(pba, blk));
                            }
                            errs[i] = Some(crate::error::OnyxError::Io(std::io::Error::other(
                                "IO thread panicked",
                            )));
                        }
                    }
                }
                errs
            });
            for (i, err) in io_errors.into_iter().enumerate() {
                if let Some(e) = err {
                    results[i] = Err(crate::error::OnyxError::Io(std::io::Error::other(format!(
                        "IO write failed: {e}"
                    ))));
                    skip[i] = true;
                }
            }
        }
        Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);

        // Phase 4: Batch multi_get old mappings (for hole detection only;
        // refcount decrements are re-computed inside the lock by atomic_batch_write_multi).
        let meta_start = Instant::now();
        struct UnitMeta {
            batch_values: Vec<(Lba, BlockmapValue)>,
            old_frag_meta: HashMap<(Pba, u16), OldFragmentRef>,
            live_positions: Vec<usize>,
        }
        let mut unit_metas: Vec<Option<UnitMeta>> = (0..n).map(|_| None).collect();

        for (i, unit) in units.iter().enumerate() {
            if skip[i] {
                continue;
            }
            let pba = pbas[i].unwrap();
            let vol_id = VolumeId(unit.vol_id.clone());
            let live_positions = match Self::live_positions_for_unit(unit, pool) {
                Ok(positions) => positions,
                Err(e) => {
                    if alloc_blocks[i] == 1 {
                        let _ = allocator.free_one(pba);
                    } else {
                        let _ = allocator.free_extent(Extent::new(pba, alloc_blocks[i]));
                    }
                    results[i] = Err(e);
                    skip[i] = true;
                    continue;
                }
            };

            if live_positions.is_empty() {
                unit_metas[i] = Some(UnitMeta {
                    batch_values: Vec::new(),
                    old_frag_meta: HashMap::new(),
                    live_positions,
                });
                continue;
            }

            let lbas: Vec<Lba> = live_positions
                .iter()
                .map(|idx| Lba(unit.start_lba.0 + *idx as u64))
                .collect();
            let old_mappings = match meta.multi_get_mappings(&vol_id, &lbas) {
                Ok(m) => m,
                Err(e) => {
                    if alloc_blocks[i] == 1 {
                        let _ = allocator.free_one(pba);
                    } else {
                        let _ = allocator.free_extent(Extent::new(pba, alloc_blocks[i]));
                    }
                    results[i] = Err(e);
                    skip[i] = true;
                    continue;
                }
            };

            let mut old_frag_meta: HashMap<(Pba, u16), OldFragmentRef> = HashMap::new();
            let mut batch_values = Vec::with_capacity(live_positions.len());

            for (j, old) in old_mappings.into_iter().enumerate() {
                if let Some(old) = old {
                    record_old_fragment_ref(&mut old_frag_meta, old);
                }
                let flags = if unit.dedup_skipped {
                    FLAG_DEDUP_SKIPPED
                } else {
                    0
                };
                batch_values.push((
                    lbas[j],
                    BlockmapValue {
                        pba,
                        compression: unit.compression,
                        unit_compressed_size: unit.compressed_data.len() as u32,
                        unit_original_size: unit.original_size,
                        unit_lba_count: unit.lba_count as u16,
                        offset_in_unit: live_positions[j] as u16,
                        crc32: unit.crc32,
                        slot_offset: 0,
                        flags,
                    },
                ));
            }
            unit_metas[i] = Some(UnitMeta {
                batch_values,
                old_frag_meta,
                live_positions,
            });
        }

        // Phase 5: ONE combined WriteBatch for all units.
        // old PBA decrements are computed inside the lock by atomic_batch_write_multi.
        let mut vol_ids_owned: Vec<VolumeId> = Vec::new();
        let mut meta_indices: Vec<usize> = Vec::new();

        for (i, unit) in units.iter().enumerate() {
            if skip[i] || unit_metas[i].is_none() {
                continue;
            }
            vol_ids_owned.push(VolumeId(unit.vol_id.clone()));
            meta_indices.push(i);
        }

        // actual_old_pba_meta: returned by atomic_batch_write_multi with accurate data
        let mut actual_old_pba_meta: HashMap<Pba, (u32, u32)> = HashMap::new();

        if !meta_indices.is_empty() {
            let batch_args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> = meta_indices
                .iter()
                .enumerate()
                .map(|(batch_idx, &unit_idx)| {
                    let um = unit_metas[unit_idx].as_ref().unwrap();
                    (
                        &vol_ids_owned[batch_idx],
                        um.batch_values.as_slice(),
                        um.live_positions.len() as u32,
                    )
                })
                .collect();

            match meta.atomic_batch_write_multi(&batch_args) {
                Ok(returned) => {
                    actual_old_pba_meta = returned;
                }
                Err(e) => {
                    // Entire batch failed — rollback all allocations.
                    for &unit_idx in &meta_indices {
                        let pba = pbas[unit_idx].unwrap();
                        if alloc_blocks[unit_idx] == 1 {
                            let _ = allocator.free_one(pba);
                        } else {
                            let _ = allocator.free_extent(Extent::new(pba, alloc_blocks[unit_idx]));
                        }
                        results[unit_idx] = Err(crate::error::OnyxError::Io(
                            std::io::Error::other(format!("batch write failed: {e}",)),
                        ));
                        skip[unit_idx] = true;
                    }
                }
            }
        }
        Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);

        // Phase 6: Cleanup (free old PBAs) + dedup index + hole detection.
        // Use actual_old_pba_meta returned by atomic_batch_write_multi (fresh data).
        let cleanup_start = Instant::now();

        // Batch-read refcounts for old PBAs returned by atomic_batch_write_multi.
        let old_pba_keys: Vec<Pba> = actual_old_pba_meta.keys().copied().collect();
        let refcounts = meta.multi_get_refcounts(&old_pba_keys).unwrap_or_default();
        let mut dead_pbas: Vec<Pba> = Vec::new();
        let mut dead_allocations: Vec<(Pba, u32)> = Vec::new();
        for (i, old_pba) in old_pba_keys.iter().enumerate() {
            let remaining = refcounts.get(i).copied().unwrap_or(0);
            let confirmed = if remaining == 0 && soak_debug_guards_enabled() {
                match meta.reconcile_refcount_for_pba(*old_pba) {
                    Ok(actual) => {
                        if actual != 0 {
                            tracing::error!(
                                pba = old_pba.0,
                                reconciled_refcount = actual,
                                context = "write_units_batch_cleanup",
                                "detected refcount drift while preparing batched free"
                            );
                        }
                        actual
                    }
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            pba = old_pba.0,
                            "writer: failed to reconcile zero refcount before free"
                        );
                        continue;
                    }
                }
            } else {
                remaining
            };
            if confirmed == 0 {
                let block_count = actual_old_pba_meta
                    .get(old_pba)
                    .map(|(_, bc)| *bc)
                    .unwrap_or(1);
                dead_pbas.push(*old_pba);
                dead_allocations.push((*old_pba, block_count));
            }
        }

        let mut all_dedup_entries: Vec<(ContentHash, DedupEntry)> = Vec::new();

        for &unit_idx in &meta_indices {
            if skip[unit_idx] {
                continue;
            }
            let um = unit_metas[unit_idx].as_ref().unwrap();
            let unit = &units[unit_idx];
            let pba = pbas[unit_idx].unwrap();
            if um.live_positions.is_empty() {
                if alloc_blocks[unit_idx] == 1 {
                    let _ = allocator.free_one(pba);
                } else {
                    let _ = allocator.free_extent(Extent::new(pba, alloc_blocks[unit_idx]));
                }
                continue;
            }

            // Collect dedup index entries for batch write
            if let Some(ref hashes) = unit.block_hashes {
                for &pos in &um.live_positions {
                    let hash = hashes[pos];
                    if hash == [0u8; 32] {
                        continue;
                    }
                    all_dedup_entries.push((
                        hash,
                        DedupEntry {
                            pba,
                            slot_offset: 0,
                            compression: unit.compression,
                            unit_compressed_size: unit.compressed_data.len() as u32,
                            unit_original_size: unit.original_size,
                            unit_lba_count: unit.lba_count as u16,
                            offset_in_unit: pos as u16,
                            crc32: unit.crc32,
                        },
                    ));
                }
            }

            // Hole detection
            let _ = Self::detect_holes(&um.old_frag_meta, meta, hole_map, metrics);

            metrics.flush_units_written.fetch_add(1, Ordering::Relaxed);
            metrics
                .flush_unit_bytes
                .fetch_add(unit.compressed_data.len() as u64, Ordering::Relaxed);
        }

        // Reclaim old PBAs through the shared cleanup helper so every path
        // (dedup worker/scanner, writer, GC-adjacent rewrites) serializes the
        // "refcount==0 -> dedup cleanup -> allocator free" transition per PBA.
        if !dead_pbas.is_empty() {
            for (old_pba, old_blocks) in dead_allocations {
                Self::cleanup_dead_pba_post_commit(
                    meta,
                    allocator,
                    hole_map,
                    old_pba,
                    old_blocks,
                    "write_units_batch_cleanup",
                );
            }
        }
        // One batch for all new dedup index entries
        if !all_dedup_entries.is_empty() {
            let _ = meta.put_dedup_entries(&all_dedup_entries);
        }
        Self::record_elapsed(&metrics.flush_writer_cleanup_ns, cleanup_start);

        // Phase 7: Batch mark_flushed (memory-only, fast).
        let mark_start = Instant::now();
        for (i, unit) in units.iter().enumerate() {
            if skip[i] {
                continue;
            }
            for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
            }
        }
        Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);
        let _ = pool.advance_tail_for_shard(shard_idx);

        Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
        results
    }

    fn write_packed_slot(
        shard_idx: usize,
        sealed: &SealedSlot,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        hole_map: &HoleMap,
        metrics: &EngineMetrics,
    ) -> OnyxResult<()> {
        let total_start = Instant::now();
        // Collect unique volume IDs and acquire read locks on ALL of them
        // BEFORE doing any work. Sorted to prevent deadlock. Held until
        // metadata commit completes — mirrors write_unit()'s with_read_lock
        // guarantee that delete/create cannot interleave with this flush.
        let mut vol_ids: Vec<String> = sealed
            .fragments
            .iter()
            .map(|f| f.unit.vol_id.clone())
            .collect();
        vol_ids.sort();
        vol_ids.dedup();

        let locks: Vec<_> = vol_ids.iter().map(|vid| lifecycle.get_lock(vid)).collect();
        let _guards: Vec<_> = locks.iter().map(|l| l.read().unwrap()).collect();

        // Under lifecycle read locks: check generation, build batch, IO, commit

        // Build blockmap entries; old mapping reads are for hole detection only
        // (refcount decrements are re-computed inside the lock by atomic_batch_write_packed).
        let mut batch_values: Vec<(VolumeId, Lba, BlockmapValue)> = Vec::new();
        let mut old_frag_meta: HashMap<(Pba, u16), OldFragmentRef> = HashMap::new();
        let mut total_refcount: u32 = 0;
        let mut all_seq_lba_ranges: Vec<(u64, Lba, u32)> = Vec::new();
        let mut any_discarded = false;

        for frag in &sealed.fragments {
            let unit = &frag.unit;
            let vol_id = VolumeId(unit.vol_id.clone());

            // Lifecycle check: verify volume still exists and generation matches
            let should_discard = match meta.get_volume(&vol_id)? {
                None => true,
                Some(vc) if unit.vol_created_at != 0 && vc.created_at != unit.vol_created_at => {
                    true
                }
                _ => false,
            };

            if should_discard {
                metrics.flush_stale_discards.fetch_add(1, Ordering::Relaxed);
                any_discarded = true;
                for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                    let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
                }
                continue;
            }

            let live_positions = Self::live_positions_for_unit(unit, pool)?;
            if live_positions.is_empty() {
                any_discarded = true;
                for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                    let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
                }
                continue;
            }

            // Batch read old mappings for hole detection (stale reads safe here)
            let frag_lbas: Vec<Lba> = live_positions
                .iter()
                .map(|idx| Lba(unit.start_lba.0 + *idx as u64))
                .collect();
            let old_mappings = meta.multi_get_mappings(&vol_id, &frag_lbas)?;

            for (i, old) in old_mappings.into_iter().enumerate() {
                if let Some(old) = old {
                    record_old_fragment_ref(&mut old_frag_meta, old);
                }
                let flags = if unit.dedup_skipped {
                    FLAG_DEDUP_SKIPPED
                } else {
                    0
                };
                batch_values.push((
                    vol_id.clone(),
                    frag_lbas[i],
                    BlockmapValue {
                        pba: sealed.pba,
                        compression: unit.compression,
                        unit_compressed_size: unit.compressed_data.len() as u32,
                        unit_original_size: unit.original_size,
                        unit_lba_count: unit.lba_count as u16,
                        offset_in_unit: live_positions[i] as u16,
                        crc32: unit.crc32,
                        slot_offset: frag.slot_offset,
                        flags,
                    },
                ));
            }
            total_refcount += live_positions.len() as u32;
            all_seq_lba_ranges.extend(unit.seq_lba_ranges.iter().cloned());
        }

        // If all fragments were discarded, free the slot PBA
        if batch_values.is_empty() {
            allocator.free_one(sealed.pba)?;
            let _ = pool.advance_tail_for_shard(shard_idx);
            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            return Ok(());
        }

        // Serialize with concurrent hole fills targeting this PBA.
        // Without this, a stale hole fill (targeting this PBA from a previous
        // incarnation) could read-modify-write LV3 data concurrently with our
        // write, corrupting the slot. The lock ensures: either the hole fill
        // finishes first (and its metadata guard rejects it when refcount=0),
        // or we finish first (and the hole fill sees our new refcount > 0 but
        // the overlap check in atomic_batch_write_hole_fill rejects it).
        let pba_lock = Self::hole_fill_lock(sealed.pba);
        let _pba_guard = pba_lock.lock().unwrap();
        // Purge any stale hole map entries from a previous incarnation of this PBA.
        crate::packer::packer::remove_holes_for_pba(hole_map, sealed.pba);

        let io_start = Instant::now();
        if let Err(e) =
            maybe_inject_test_failure_packed(&sealed.fragments, FlushFailStage::BeforeIoWrite)
        {
            allocator.free_one(sealed.pba)?;
            Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);
            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            return Err(e);
        }

        // Write the 4KB slot data to LV3
        if let Err(e) = io_engine.write_blocks(sealed.pba, &sealed.data) {
            allocator.free_one(sealed.pba)?;
            Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);
            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            return Err(e);
        }
        Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);

        let meta_start = Instant::now();

        if let Err(e) =
            maybe_inject_test_failure_packed(&sealed.fragments, FlushFailStage::BeforeMetaWrite)
        {
            allocator.free_one(sealed.pba)?;
            Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            return Err(e);
        }
        maybe_pause_before_packed_meta_write(&sealed.fragments)?;

        // Metadata commit — old PBA decrements re-computed inside the lock
        let actual_old_pba_meta =
            match meta.atomic_batch_write_packed(&batch_values, sealed.pba, total_refcount) {
                Ok(m) => m,
                Err(e) => {
                    allocator.free_one(sealed.pba)?;
                    Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
                    Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                    return Err(e);
                }
            };
        Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);

        // Free old PBAs whose refcount dropped to 0 (using fresh data from lock)
        let cleanup_start = Instant::now();
        for (old_pba, (_, old_blocks)) in &actual_old_pba_meta {
            Self::cleanup_dead_pba_post_commit(
                meta,
                allocator,
                hole_map,
                *old_pba,
                *old_blocks,
                "write_packed_slot_cleanup",
            );
        }
        Self::record_elapsed(&metrics.flush_writer_cleanup_ns, cleanup_start);

        // Populate dedup index for newly written fragments
        let dedup_start = Instant::now();
        for frag in &sealed.fragments {
            if let Some(ref hashes) = frag.unit.block_hashes {
                let live_positions = Self::live_positions_for_unit(&frag.unit, pool)?;
                let mut dedup_entries = Vec::new();
                for &pos in &live_positions {
                    let hash = hashes[pos];
                    if hash == [0u8; 32] {
                        continue;
                    }
                    dedup_entries.push((
                        hash,
                        DedupEntry {
                            pba: sealed.pba,
                            slot_offset: frag.slot_offset,
                            compression: frag.unit.compression,
                            unit_compressed_size: frag.unit.compressed_data.len() as u32,
                            unit_original_size: frag.unit.original_size,
                            unit_lba_count: frag.unit.lba_count as u16,
                            offset_in_unit: pos as u16,
                            crc32: frag.unit.crc32,
                        },
                    ));
                }
                if !dedup_entries.is_empty() {
                    meta.put_dedup_entries(&dedup_entries)?;
                }
            }
        }
        Self::record_elapsed(&metrics.flush_writer_dedup_index_ns, dedup_start);

        // Detect holes in packed slots
        let hole_start = Instant::now();
        Self::detect_holes(&old_frag_meta, meta, hole_map, metrics)?;
        Self::record_elapsed(&metrics.flush_writer_hole_detect_ns, hole_start);
        metrics
            .flush_packed_slots_written
            .fetch_add(1, Ordering::Relaxed);
        metrics
            .flush_packed_fragments_written
            .fetch_add(sealed.fragments.len() as u64, Ordering::Relaxed);
        metrics
            .flush_packed_bytes
            .fetch_add(sealed.data.len() as u64, Ordering::Relaxed);

        // Mark entries flushed
        let mark_start = Instant::now();
        for (seq, lba_start, lba_count) in &all_seq_lba_ranges {
            if let Err(e) = pool.mark_flushed(*seq, *lba_start, *lba_count) {
                tracing::warn!(seq, error = %e, "failed to mark entry flushed (packed)");
            }
        }
        Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);

        tracing::debug!(
            pba = sealed.pba.0,
            fragments = sealed.fragments.len(),
            total_lbas = total_refcount,
            discarded = any_discarded,
            "flushed packed slot"
        );

        Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
        Ok(())
    }

    /// Detect holes created by overwrites: when ALL LBAs of a fragment in a
    /// packed slot are overwritten in this batch, and the PBA still has other
    /// live fragments (refcount > 0), the dead fragment's space is a hole.
    ///
    /// Called from write_unit and write_packed_slot after metadata commit.
    fn detect_holes(
        old_frag_meta: &HashMap<(Pba, u16), OldFragmentRef>,
        meta: &MetaStore,
        hole_map: &HoleMap,
        metrics: &EngineMetrics,
    ) -> OnyxResult<()> {
        // Hole publication currently depends on full-blockmap validation to
        // avoid reviving stale packed-slot bytes. Keep that expensive path in
        // debug builds, but skip it by default in release so the writer does
        // not spend multiple CPU-seconds per status window on O(N) scans.
        //
        // Hole reuse remains available for investigation by setting
        // ONYX_ENABLE_SOAK_DEBUG_GUARDS=1.
        if !soak_debug_guards_enabled() {
            let _ = (old_frag_meta, meta, hole_map, metrics);
            return Ok(());
        }
        for (&(old_pba, slot_offset), frag) in old_frag_meta {
            // Only interested in packed fragments (unit_compressed_size < BLOCK_SIZE implies packed)
            // and only if we overwrote ALL LBAs of this fragment in this batch
            if frag.decrement < frag.mapping.unit_lba_count as u32 {
                continue;
            }

            // Check PBA still has other live fragments (not entirely freed)
            let remaining = Self::live_refcount_with_reconcile(meta, old_pba, "detect_holes")?;
            if remaining == 0 {
                continue; // PBA is freed, no hole to track
            }

            // Dedup hits can add extra blockmap references to the same fragment
            // without changing unit_lba_count. Publishing a hole here would let a
            // later hole fill overwrite bytes that are still live.
            if meta.has_live_fragment_ref(&frag.mapping)? {
                tracing::debug!(
                    pba = old_pba.0,
                    slot_offset,
                    remaining,
                    "skipping hole publication for fragment that still has live refs"
                );
                continue;
            }

            // This fragment is fully dead but PBA is still live → hole.
            // Validate against current metadata before publishing: old_frag_meta
            // is built from a stale read, so the actual byte layout may differ
            // if a concurrent hole fill already placed a new fragment here.
            // TODO(perf): has_overlap_at_pba is a full blockmap scan O(N).
            // Replace with a PBA-prefix scan once blockmap key layout supports it.
            let size = frag.mapping.unit_compressed_size as u16;
            if meta.has_overlap_at_pba(old_pba, slot_offset, size)? {
                tracing::debug!(
                    pba = old_pba.0,
                    slot_offset,
                    size,
                    "skipping hole publication: byte range overlaps with live fragment in current metadata"
                );
                continue;
            }
            crate::packer::packer::insert_hole_coalesced(hole_map, old_pba, slot_offset, size);
            metrics.hole_detections.fetch_add(1, Ordering::Relaxed);

            tracing::debug!(
                pba = old_pba.0,
                slot_offset,
                size,
                "detected hole in packed slot"
            );
        }
        Ok(())
    }

    /// Fill a hole in an existing packed slot: read-modify-write.
    ///
    /// Locks ALL volumes in the packed slot (not just the new fragment's volume)
    /// to prevent concurrent delete_volume from corrupting refcount.
    /// Validates the hole is still free before writing.
    fn write_hole_fill(
        shard_idx: usize,
        fill: &HoleFill,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        hole_map: &HoleMap,
        metrics: &EngineMetrics,
    ) -> OnyxResult<()> {
        let total_start = Instant::now();
        let unit = &fill.unit;
        let vol_id = VolumeId(unit.vol_id.clone());

        // Early bail-out: if the slot PBA is already dead, skip the entire
        // read-modify-write cycle. This is a best-effort check outside the
        // lock — the definitive guard is inside atomic_batch_write_hole_fill.
        if meta.get_refcount(fill.pba)? == 0 {
            crate::packer::packer::remove_holes_for_pba(hole_map, fill.pba);
            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            return Ok(());
        }

        // Lock ALL volumes in this packed slot, not just the new fragment's volume.
        // This prevents delete_volume(other_vol) from racing with our refcount update.
        let mut all_vol_ids = meta.find_volume_ids_by_pba(fill.pba)?;
        if !all_vol_ids.contains(&unit.vol_id) {
            all_vol_ids.push(unit.vol_id.clone());
        }
        all_vol_ids.sort();
        all_vol_ids.dedup();

        let locks: Vec<_> = all_vol_ids
            .iter()
            .map(|vid| lifecycle.get_lock(vid))
            .collect();
        let _guards: Vec<_> = locks.iter().map(|l| l.read().unwrap()).collect();
        let pba_lock = Self::hole_fill_lock(fill.pba);
        let _pba_guard = pba_lock.lock().unwrap();
        Self::fail_if_pba_has_fragment_overlap(meta, fill.pba, "write_hole_fill_precheck")?;

        // Generation check for the writing volume
        let should_discard = match meta.get_volume(&vol_id)? {
            None => true,
            Some(vc) if vc.created_at != unit.vol_created_at => true,
            _ => false,
        };
        if should_discard {
            metrics.flush_stale_discards.fetch_add(1, Ordering::Relaxed);
            for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
            }
            let _ = pool.advance_tail_for_shard(shard_idx);
            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            return Ok(());
        }

        // Validate the hole is still free: check that no existing fragment at this
        // PBA overlaps with [slot_offset, slot_offset + compressed_data.len()).
        let fill_size = unit.compressed_data.len() as u16;
        if meta.has_overlap_at_pba(fill.pba, fill.slot_offset, fill_size)? {
            tracing::debug!(
                pba = fill.pba.0,
                slot_offset = fill.slot_offset,
                "hole fill: byte range overlaps with live fragment, skipping"
            );
            // Don't mark flushed — the unit stays in the buffer and will be
            // re-dispatched by the coalescer on the next pass.
            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            return Ok(());
        }

        let io_start = Instant::now();
        // Read existing slot
        let mut slot_data = io_engine.read_blocks(fill.pba, BLOCK_SIZE as usize)?;

        // Overlay new fragment
        let start = fill.slot_offset as usize;
        let end = start + unit.compressed_data.len();
        if end > slot_data.len() {
            Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);
            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            return Err(crate::error::OnyxError::Compress(format!(
                "hole fill out of bounds: offset={} + size={} > {}",
                start,
                unit.compressed_data.len(),
                slot_data.len()
            )));
        }
        slot_data[start..end].copy_from_slice(&unit.compressed_data);

        // Write back
        io_engine.write_blocks(fill.pba, &slot_data)?;
        Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);

        // Read old mappings for hole detection (stale reads safe here;
        // refcount decrements are re-computed inside the lock by atomic_batch_write_hole_fill).
        let meta_start = Instant::now();
        let mut old_frag_meta: HashMap<(Pba, u16), OldFragmentRef> = HashMap::new();

        let live_positions = Self::live_positions_for_unit(unit, pool)?;
        if live_positions.is_empty() {
            let mark_start = Instant::now();
            for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                if let Err(e) = pool.mark_flushed(*seq, *lba_start, *lba_count) {
                    tracing::warn!(seq, error = %e, "failed to mark stale hole-fill entry flushed");
                }
            }
            Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);
            let _ = pool.advance_tail_for_shard(shard_idx);
            Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            return Ok(());
        }

        let mut batch_values = Vec::with_capacity(live_positions.len());
        for &pos in &live_positions {
            let lba = Lba(unit.start_lba.0 + pos as u64);
            if let Some(old) = meta.get_mapping(&vol_id, lba)? {
                record_old_fragment_ref(&mut old_frag_meta, old);
            }
            let flags = if unit.dedup_skipped {
                FLAG_DEDUP_SKIPPED
            } else {
                0
            };
            batch_values.push((
                lba,
                BlockmapValue {
                    pba: fill.pba,
                    compression: unit.compression,
                    unit_compressed_size: unit.compressed_data.len() as u32,
                    unit_original_size: unit.original_size,
                    unit_lba_count: unit.lba_count as u16,
                    offset_in_unit: pos as u16,
                    crc32: unit.crc32,
                    slot_offset: fill.slot_offset,
                    flags,
                },
            ));
        }

        // Self-referencing decrements (old_pba == fill.pba) and external decrements
        // are now computed inside the lock by atomic_batch_write_hole_fill.
        let actual_old_pba_meta = meta.atomic_batch_write_hole_fill(
            &vol_id,
            &batch_values,
            fill.pba,
            live_positions.len() as u32,
        )?;
        Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);

        // Free old PBAs whose refcount dropped to 0 (using fresh data from lock)
        let cleanup_start = Instant::now();
        for (old_pba, (_, old_blocks)) in &actual_old_pba_meta {
            Self::cleanup_dead_pba_post_commit(
                meta,
                allocator,
                hole_map,
                *old_pba,
                *old_blocks,
                "write_hole_fill_cleanup",
            );
        }
        Self::record_elapsed(&metrics.flush_writer_cleanup_ns, cleanup_start);

        // Populate dedup index for newly written blocks (same as write_unit path)
        let dedup_start = Instant::now();
        if let Some(ref hashes) = unit.block_hashes {
            let mut dedup_entries = Vec::new();
            for &pos in &live_positions {
                let hash = hashes[pos];
                if hash == [0u8; 32] {
                    continue;
                }
                dedup_entries.push((
                    hash,
                    DedupEntry {
                        pba: fill.pba,
                        slot_offset: fill.slot_offset,
                        compression: unit.compression,
                        unit_compressed_size: unit.compressed_data.len() as u32,
                        unit_original_size: unit.original_size,
                        unit_lba_count: unit.lba_count as u16,
                        offset_in_unit: pos as u16,
                        crc32: unit.crc32,
                    },
                ));
            }
            if !dedup_entries.is_empty() {
                meta.put_dedup_entries(&dedup_entries)?;
            }
        }
        Self::record_elapsed(&metrics.flush_writer_dedup_index_ns, dedup_start);

        // Detect any new holes from the old PBAs
        let hole_start = Instant::now();
        Self::detect_holes(&old_frag_meta, meta, hole_map, metrics)?;
        Self::record_elapsed(&metrics.flush_writer_hole_detect_ns, hole_start);
        metrics.flush_hole_fills.fetch_add(1, Ordering::Relaxed);
        metrics
            .flush_hole_fill_bytes
            .fetch_add(unit.compressed_data.len() as u64, Ordering::Relaxed);

        // Mark flushed
        let mark_start = Instant::now();
        for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
            if let Err(e) = pool.mark_flushed(*seq, *lba_start, *lba_count) {
                tracing::warn!(seq, error = %e, "failed to mark flushed (hole fill)");
            }
        }
        Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);

        // Inject remainder back into hole_map only after successful write.
        let frag_size = unit.compressed_data.len() as u16;
        let remainder = fill.hole_size.saturating_sub(frag_size);
        if remainder > 0 {
            crate::packer::packer::insert_hole_coalesced(
                hole_map,
                fill.pba,
                fill.slot_offset + frag_size,
                remainder,
            );
        }

        tracing::debug!(
            pba = fill.pba.0,
            slot_offset = fill.slot_offset,
            remainder,
            vol = unit.vol_id,
            lba_count = unit.lba_count,
            "filled hole in packed slot"
        );

        Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
        Ok(())
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
    }
}

impl Drop for BufferFlusher {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MetaConfig;
    use crate::io::device::RawDevice;
    use crate::meta::store::MetaStore;
    use crate::types::{VolumeConfig, ZoneId};
    use crate::zone::worker::ZoneWorker;
    use std::collections::{HashMap, HashSet};
    use tempfile::{tempdir, NamedTempFile};

    fn setup_flush_test_env() -> (
        Arc<MetaStore>,
        Arc<WriteBufferPool>,
        Arc<VolumeLifecycleManager>,
        Arc<SpaceAllocator>,
        Arc<IoEngine>,
        Arc<EngineMetrics>,
        tempfile::TempDir,
        NamedTempFile,
        NamedTempFile,
    ) {
        let meta_dir = tempdir().unwrap();
        let meta = Arc::new(
            MetaStore::open(&MetaConfig {
                rocksdb_path: Some(meta_dir.path().to_path_buf()),
                block_cache_mb: 8,
                wal_dir: None,
            })
            .unwrap(),
        );

        let buf_tmp = NamedTempFile::new().unwrap();
        let buf_size: u64 = 4096 + 4096 + 256 * 8192;
        buf_tmp.as_file().set_len(buf_size).unwrap();
        let pool = Arc::new(
            WriteBufferPool::open(RawDevice::open_or_create(buf_tmp.path(), buf_size).unwrap())
                .unwrap(),
        );

        let data_tmp = NamedTempFile::new().unwrap();
        let data_size: u64 = 4096 * 20000;
        data_tmp.as_file().set_len(data_size).unwrap();
        let io_engine = Arc::new(IoEngine::new(
            RawDevice::open(data_tmp.path()).unwrap(),
            false,
        ));

        let allocator = Arc::new(SpaceAllocator::new(data_size, 1));
        let lifecycle = Arc::new(VolumeLifecycleManager::default());
        let metrics = Arc::new(EngineMetrics::default());

        meta.put_volume(&VolumeConfig {
            id: VolumeId("flush-race".into()),
            size_bytes: 4096 * 1024,
            block_size: 4096,
            compression: CompressionAlgo::None,
            created_at: 1,
            zone_count: 1,
        })
        .unwrap();

        (
            meta, pool, lifecycle, allocator, io_engine, metrics, meta_dir, buf_tmp, data_tmp,
        )
    }

    fn make_unit(fill: u8, seq: u64) -> CompressedUnit {
        let data = vec![fill; BLOCK_SIZE as usize];
        CompressedUnit {
            vol_id: "flush-race".into(),
            start_lba: Lba(0),
            lba_count: 1,
            original_size: BLOCK_SIZE,
            compressed_data: data.clone(),
            compression: 0,
            crc32: crc32fast::hash(&data),
            vol_created_at: 1,
            seq_lba_ranges: vec![(seq, Lba(0), 1)],
            block_hashes: None,
            dedup_skipped: false,
            dedup_completion: None,
        }
    }

    fn make_packed_unit(fill: u8, seq: u64) -> CompressedUnit {
        let data = vec![fill; 512];
        CompressedUnit {
            vol_id: "flush-race".into(),
            start_lba: Lba(0),
            lba_count: 1,
            original_size: BLOCK_SIZE,
            compressed_data: data.clone(),
            compression: 0,
            crc32: crc32fast::hash(&data),
            vol_created_at: 1,
            seq_lba_ranges: vec![(seq, Lba(0), 1)],
            block_hashes: None,
            dedup_skipped: false,
            dedup_completion: None,
        }
    }

    fn make_packed_unit_at(fill: u8, seq: u64, lba: u64) -> CompressedUnit {
        let data = vec![fill; 128];
        CompressedUnit {
            vol_id: "flush-race".into(),
            start_lba: Lba(lba),
            lba_count: 1,
            original_size: BLOCK_SIZE,
            compressed_data: data.clone(),
            compression: 0,
            crc32: crc32fast::hash(&data),
            vol_created_at: 1,
            seq_lba_ranges: vec![(seq, Lba(lba), 1)],
            block_hashes: None,
            dedup_skipped: false,
            dedup_completion: None,
        }
    }

    #[test]
    fn old_write_unit_can_overwrite_newer_committed_mapping() {
        let (meta, pool, lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
            setup_flush_test_env();
        let hole_map = crate::packer::packer::new_hole_map();

        let newer = make_unit(0x33, 2);
        pool.note_latest_lba_seq_for_test("flush-race", Lba(0), 2, 1);
        BufferFlusher::write_unit(
            0, &newer, &pool, &meta, &lifecycle, &allocator, &io_engine, &hole_map, &metrics,
        )
        .unwrap();

        let older = make_unit(0x11, 1);
        BufferFlusher::write_unit(
            0, &older, &pool, &meta, &lifecycle, &allocator, &io_engine, &hole_map, &metrics,
        )
        .unwrap();

        let worker = ZoneWorker::new(ZoneId(0), meta.clone(), pool.clone(), io_engine.clone());
        let actual = worker.handle_read("flush-race", Lba(0)).unwrap().unwrap();

        assert_eq!(
            actual,
            vec![0x33; BLOCK_SIZE as usize],
            "older write committed after newer write must not win",
        );
    }

    #[test]
    fn packed_slot_flush_survives_already_freed_old_pba_cleanup() {
        let (meta, pool, lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
            setup_flush_test_env();
        let hole_map = crate::packer::packer::new_hole_map();

        let seq = pool
            .append("flush-race", Lba(0), 1, &vec![0x44; BLOCK_SIZE as usize], 1)
            .unwrap();

        let old_pba = allocator.allocate_one_for_lane(0).unwrap();
        let new_pba = allocator.allocate_one_for_lane(0).unwrap();
        let old_value = BlockmapValue {
            pba: old_pba,
            compression: 0,
            unit_compressed_size: 512,
            unit_original_size: BLOCK_SIZE,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: crc32fast::hash(&[0x22; 512]),
            slot_offset: 0,
            flags: 0,
        };
        meta.put_mapping(&VolumeId("flush-race".into()), Lba(0), &old_value)
            .unwrap();
        meta.set_refcount(old_pba, 1).unwrap();

        // Simulate the allocator drift we observed in soak: metadata still
        // points at old_pba, but the allocator already handed it back.
        allocator.free_one(old_pba).unwrap();

        let sealed = SealedSlot {
            pba: new_pba,
            data: vec![0xAB; BLOCK_SIZE as usize],
            fragments: vec![crate::packer::packer::SlotFragment {
                unit: make_packed_unit(0x11, seq),
                slot_offset: 0,
            }],
        };

        BufferFlusher::write_packed_slot(
            0, &sealed, &pool, &meta, &lifecycle, &allocator, &io_engine, &hole_map, &metrics,
        )
        .unwrap();

        let mapping = meta
            .get_mapping(&VolumeId("flush-race".into()), Lba(0))
            .unwrap()
            .unwrap();
        assert_eq!(mapping.pba, new_pba);
        assert_eq!(meta.get_refcount(new_pba).unwrap(), 1);
        assert_eq!(meta.get_refcount(old_pba).unwrap(), 0);
        assert!(
            pool.pending_entry_arc(seq).is_none(),
            "post-commit cleanup drift must not leave the seq stuck in the buffer"
        );
    }

    #[test]
    fn dedup_hit_cleanup_deduplicates_repeated_old_pbas() {
        let (
            meta,
            _pool,
            _lifecycle,
            _allocator,
            _io_engine,
            _metrics,
            _meta_dir,
            _buf_tmp,
            _data_tmp,
        ) = setup_flush_test_env();
        let vol = VolumeId("flush-race".into());
        let old_pba = Pba(100);
        let new_pba = Pba(200);

        for lba in 0..3u64 {
            meta.put_mapping(
                &vol,
                Lba(lba),
                &BlockmapValue {
                    pba: old_pba,
                    compression: 0,
                    unit_compressed_size: BLOCK_SIZE,
                    unit_original_size: BLOCK_SIZE,
                    unit_lba_count: 1,
                    offset_in_unit: 0,
                    crc32: 0,
                    slot_offset: 0,
                    flags: 0,
                },
            )
            .unwrap();
        }
        meta.set_refcount(old_pba, 3).unwrap();
        meta.set_refcount(new_pba, 8).unwrap();

        // Each LBA needs a dedup_reverse entry for the guard to accept the hit.
        let hash_0: ContentHash = [0x01; 32];
        let hash_1: ContentHash = [0x02; 32];
        let hash_2: ContentHash = [0x03; 32];
        meta.put_dedup_entries(&[
            (
                hash_0,
                DedupEntry {
                    pba: new_pba,
                    slot_offset: 0,
                    compression: 0,
                    unit_compressed_size: BLOCK_SIZE,
                    unit_original_size: BLOCK_SIZE,
                    unit_lba_count: 1,
                    offset_in_unit: 0,
                    crc32: 0,
                },
            ),
            (
                hash_1,
                DedupEntry {
                    pba: new_pba,
                    slot_offset: 0,
                    compression: 0,
                    unit_compressed_size: BLOCK_SIZE,
                    unit_original_size: BLOCK_SIZE,
                    unit_lba_count: 1,
                    offset_in_unit: 0,
                    crc32: 0,
                },
            ),
            (
                hash_2,
                DedupEntry {
                    pba: new_pba,
                    slot_offset: 0,
                    compression: 0,
                    unit_compressed_size: BLOCK_SIZE,
                    unit_original_size: BLOCK_SIZE,
                    unit_lba_count: 1,
                    offset_in_unit: 0,
                    crc32: 0,
                },
            ),
        ])
        .unwrap();

        let results = meta
            .atomic_batch_dedup_hits(
                &vol,
                &[
                    (
                        Lba(0),
                        BlockmapValue {
                            pba: new_pba,
                            compression: 0,
                            unit_compressed_size: BLOCK_SIZE,
                            unit_original_size: BLOCK_SIZE,
                            unit_lba_count: 1,
                            offset_in_unit: 0,
                            crc32: 0,
                            slot_offset: 0,
                            flags: 0,
                        },
                        hash_0,
                    ),
                    (
                        Lba(1),
                        BlockmapValue {
                            pba: new_pba,
                            compression: 0,
                            unit_compressed_size: BLOCK_SIZE,
                            unit_original_size: BLOCK_SIZE,
                            unit_lba_count: 1,
                            offset_in_unit: 0,
                            crc32: 0,
                            slot_offset: 0,
                            flags: 0,
                        },
                        hash_1,
                    ),
                    (
                        Lba(2),
                        BlockmapValue {
                            pba: new_pba,
                            compression: 0,
                            unit_compressed_size: BLOCK_SIZE,
                            unit_original_size: BLOCK_SIZE,
                            unit_lba_count: 1,
                            offset_in_unit: 0,
                            crc32: 0,
                            slot_offset: 0,
                            flags: 0,
                        },
                        hash_2,
                    ),
                ],
            )
            .unwrap();

        let mut dead_pbas = HashMap::new();
        for result in results {
            if let DedupHitResult::Accepted(decremented) = result {
                record_dead_pba_cleanup(&mut dead_pbas, decremented);
            }
        }

        assert_eq!(dead_pbas.len(), 1);
        assert_eq!(dead_pbas.get(&old_pba), Some(&1));
    }

    #[test]
    fn dedup_worker_cleanup_can_race_with_scanner_cleanup_on_same_dead_pba() {
        let (
            meta,
            _pool,
            _lifecycle,
            allocator,
            _io_engine,
            _metrics,
            _meta_dir,
            _buf_tmp,
            _data_tmp,
        ) = setup_flush_test_env();
        let hole_map = crate::packer::packer::new_hole_map();

        let pba = allocator.allocate_one_for_lane(0).unwrap();
        let hash: ContentHash = [0xAB; 32];
        meta.put_dedup_entries(&[(
            hash,
            DedupEntry {
                pba,
                slot_offset: 0,
                compression: 0,
                unit_compressed_size: BLOCK_SIZE,
                unit_original_size: BLOCK_SIZE,
                unit_lba_count: 1,
                offset_in_unit: 0,
                crc32: 0xDEAD_BEEF,
            },
        )])
        .unwrap();
        assert_eq!(meta.get_refcount(pba).unwrap(), 0);

        let meta_scanner = meta.clone();
        let allocator_scanner = allocator.clone();
        let (ready_tx, ready_rx) = bounded::<()>(1);
        let (resume_tx, resume_rx) = bounded::<()>(1);

        CLEANUP_FREE_ATTEMPTS.store(0, Ordering::SeqCst);

        let scanner = thread::spawn(move || {
            ready_tx.send(()).unwrap();
            resume_rx.recv().unwrap();
            BufferFlusher::cleanup_dead_pba_post_commit(
                &meta_scanner,
                &allocator_scanner,
                &crate::packer::packer::new_hole_map(),
                pba,
                1,
                "dedup_scanner_cleanup",
            );
        });

        ready_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("scanner-style cleanup should reach allocator handoff");

        BufferFlusher::cleanup_dead_pba_post_commit(
            &meta,
            &allocator,
            &hole_map,
            pba,
            1,
            "dedup_worker_hit_cleanup",
        );

        resume_tx.send(()).unwrap();
        scanner.join().unwrap();
        assert_eq!(
            CLEANUP_FREE_ATTEMPTS.load(Ordering::SeqCst),
            1,
            "shared cleanup helper should perform allocator free at most once"
        );
        assert!(meta.get_dedup_entry(&hash).unwrap().is_none());
    }

    #[test]
    fn duplicate_dead_pba_cleanup_callers_without_shared_lock_double_free() {
        let (
            meta,
            _pool,
            _lifecycle,
            allocator,
            _io_engine,
            _metrics,
            _meta_dir,
            _buf_tmp,
            _data_tmp,
        ) = setup_flush_test_env();

        let pba = allocator.allocate_one_for_lane(0).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(3));

        let run_cleanup = |meta: Arc<MetaStore>,
                           allocator: Arc<SpaceAllocator>,
                           barrier: Arc<std::sync::Barrier>| {
            thread::spawn(move || {
                assert_eq!(meta.get_refcount(pba).unwrap(), 0);
                barrier.wait();
                allocator.free_one(pba)
            })
        };

        let t1 = run_cleanup(meta.clone(), allocator.clone(), barrier.clone());
        let t2 = run_cleanup(meta.clone(), allocator.clone(), barrier.clone());
        barrier.wait();

        let results = [t1.join().unwrap(), t2.join().unwrap()];
        let ok = results.iter().filter(|r| r.is_ok()).count();
        let already_free = results
            .iter()
            .filter_map(|r| r.as_ref().err())
            .filter(|e| e.to_string().contains("already free"))
            .count();

        assert_eq!(ok, 1, "exactly one cleanup caller should win the free");
        assert_eq!(
            already_free, 1,
            "the second cleanup caller should hit allocator already-free"
        );
    }

    #[test]
    fn writer_flushes_packed_open_slot_while_lane_stays_busy() {
        let (meta, pool, lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
            setup_flush_test_env();
        let hole_map = crate::packer::packer::new_hole_map();
        let running = Arc::new(AtomicBool::new(true));
        let in_flight = FlusherInFlightTracker::default();
        let (tx, rx) = bounded::<CompressedUnit>(64);
        let (done_tx, done_rx) = unbounded::<Vec<u64>>();

        let running_w = running.clone();
        let pool_w = pool.clone();
        let meta_w = meta.clone();
        let lifecycle_w = lifecycle.clone();
        let allocator_w = allocator.clone();
        let io_engine_w = io_engine.clone();
        let metrics_w = metrics.clone();

        let handle = thread::spawn(move || {
            let mut packer = Packer::new_with_lane(allocator_w.clone(), hole_map, 0);
            BufferFlusher::writer_loop(
                0,
                &rx,
                &pool_w,
                &meta_w,
                &lifecycle_w,
                &allocator_w,
                &io_engine_w,
                &done_tx,
                &running_w,
                &in_flight,
                &mut packer,
                &metrics_w,
            );
        });

        // Keep the lane busy with small packed fragments and never leave a
        // 50ms idle gap. Without age-based flushing, buffered seqs would not
        // complete until traffic stops and recv_timeout finally fires.
        for i in 0..12u64 {
            tx.send(make_packed_unit_at(0x40 + i as u8, 10_000 + i, i))
                .unwrap();
            thread::sleep(Duration::from_millis(10));
        }

        let done = done_rx
            .recv_timeout(Duration::from_millis(150))
            .expect("busy writer lane should flush aged packed slot without waiting for idle");
        assert!(
            !done.is_empty(),
            "aged packed slot flush should signal at least one buffered seq"
        );

        running.store(false, Ordering::Relaxed);
        drop(tx);
        handle.join().unwrap();
    }

    #[test]
    fn coalesce_enqueue_caps_ready_window_bytes() {
        let tmp = NamedTempFile::new().unwrap();
        let size = 4096 + 4096 + 96 * 1024 * 1024;
        tmp.as_file().set_len(size).unwrap();
        let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
        let pool = WriteBufferPool::open(dev).unwrap();

        let payload = vec![0x5A; 256 * BLOCK_SIZE as usize];
        let mut seqs = Vec::new();
        for i in 0..20u64 {
            seqs.push(
                pool.append("window-vol", Lba(i * 256), 256, &payload, 0)
                    .unwrap(),
            );
        }

        let tracker = FlusherInFlightTracker::default();
        let in_flight = HashMap::new();
        let mut seen = HashSet::new();
        let mut queued_bytes = 0usize;
        let mut new_entries = Vec::new();
        let mut window_full = false;

        for seq in seqs {
            if matches!(
                BufferFlusher::try_enqueue_pending_seq(
                    seq,
                    &pool,
                    &in_flight,
                    &tracker,
                    &mut seen,
                    &mut queued_bytes,
                    &mut new_entries,
                ),
                EnqueuePendingSeq::WindowFull
            ) {
                window_full = true;
                break;
            }
        }

        assert!(
            window_full,
            "coalescer should stop once the ready window is full"
        );
        assert_eq!(queued_bytes, BufferFlusher::COALESCE_READY_WINDOW_BYTES);
        assert_eq!(new_entries.len(), 16);
    }

    /// Regression test: packed slot refcount drift when LBAs are overwritten
    /// via atomic_batch_write_multi.
    ///
    /// Reproduces the soak failure where PBA 248131 had stored refcount=50
    /// but actual blockmap refs=114. The drift occurs when a packed PBA's
    /// LBAs are overwritten, the refcount reaches 0, the PBA is freed and
    /// reused, but old blockmap entries are not cleaned up.
    #[test]
    fn packed_slot_refcount_drift_on_overwrite() {
        let (meta, _pool, _lifecycle, allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
            setup_flush_test_env();

        let vol = VolumeId("flush-race".into());
        let packed_pba = allocator.allocate_one_for_lane(0).unwrap();

        // --- Round 1: create a packed slot with 2 fragments (32 + 32 = 64 LBAs) ---
        let mut batch_values: Vec<(VolumeId, Lba, BlockmapValue)> = Vec::new();
        // Fragment A: 32 LBAs at slot_offset=0
        for i in 0u64..32 {
            batch_values.push((
                vol.clone(),
                Lba(1000 + i),
                BlockmapValue {
                    pba: packed_pba,
                    compression: 1,
                    unit_compressed_size: 1953,
                    unit_original_size: 131072,
                    unit_lba_count: 32,
                    offset_in_unit: i as u16,
                    crc32: 0xAAAAAAAA,
                    slot_offset: 0,
                    flags: 0,
                },
            ));
        }
        // Fragment B: 32 LBAs at slot_offset=1953
        for i in 0u64..32 {
            batch_values.push((
                vol.clone(),
                Lba(2000 + i),
                BlockmapValue {
                    pba: packed_pba,
                    compression: 1,
                    unit_compressed_size: 1113,
                    unit_original_size: 131072,
                    unit_lba_count: 32,
                    offset_in_unit: i as u16,
                    crc32: 0xBBBBBBBB,
                    slot_offset: 1953,
                    flags: 0,
                },
            ));
        }

        meta.atomic_batch_write_packed(&batch_values, packed_pba, 64)
            .unwrap();

        assert_eq!(meta.get_refcount(packed_pba).unwrap(), 64);
        assert_eq!(meta.count_blockmap_refs_for_pba(packed_pba).unwrap(), 64);

        // --- Overwrite ALL 64 LBAs via atomic_batch_write_multi (simulating normal writes) ---
        let new_pba_1 = allocator.allocate_one_for_lane(0).unwrap();
        let new_pba_2 = allocator.allocate_one_for_lane(0).unwrap();

        // Unit 1: overwrites LBAs 1000..1032 → new_pba_1
        let unit1_entries: Vec<(Lba, BlockmapValue)> = (0u64..32)
            .map(|i| {
                (
                    Lba(1000 + i),
                    BlockmapValue {
                        pba: new_pba_1,
                        compression: 0,
                        unit_compressed_size: BLOCK_SIZE,
                        unit_original_size: BLOCK_SIZE,
                        unit_lba_count: 1,
                        offset_in_unit: 0,
                        crc32: 0x11111111,
                        slot_offset: 0,
                        flags: 0,
                    },
                )
            })
            .collect();

        // Unit 2: overwrites LBAs 2000..2032 → new_pba_2
        let unit2_entries: Vec<(Lba, BlockmapValue)> = (0u64..32)
            .map(|i| {
                (
                    Lba(2000 + i),
                    BlockmapValue {
                        pba: new_pba_2,
                        compression: 0,
                        unit_compressed_size: BLOCK_SIZE,
                        unit_original_size: BLOCK_SIZE,
                        unit_lba_count: 1,
                        offset_in_unit: 0,
                        crc32: 0x22222222,
                        slot_offset: 0,
                        flags: 0,
                    },
                )
            })
            .collect();

        let batch_args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> = vec![
            (&vol, &unit1_entries, 32),
            (&vol, &unit2_entries, 32),
        ];

        let old_pba_meta = meta.atomic_batch_write_multi(&batch_args).unwrap();

        // --- Verify: packed_pba's refcount should be 0, and no blockmap refs should remain ---
        let rc_after = meta.get_refcount(packed_pba).unwrap();
        let refs_after = meta.count_blockmap_refs_for_pba(packed_pba).unwrap();

        println!("packed_pba after overwrite: refcount={}, blockmap_refs={}", rc_after, refs_after);

        // Both should be 0: all 64 LBAs were remapped to new PBAs
        assert_eq!(
            rc_after, 0,
            "packed_pba refcount should be 0 after all LBAs overwritten"
        );
        assert_eq!(
            refs_after, 0,
            "packed_pba should have 0 blockmap refs after all LBAs overwritten"
        );

        // Verify old PBAs were decremented
        assert!(
            old_pba_meta.contains_key(&packed_pba),
            "packed_pba should appear in old_pba_meta decrements"
        );

        // Verify new PBAs have correct refcounts
        assert_eq!(meta.get_refcount(new_pba_1).unwrap(), 32);
        assert_eq!(meta.get_refcount(new_pba_2).unwrap(), 32);
    }

    /// Regression test: packed slot + dedup hits + overwrite interaction.
    ///
    /// Scenario: packed PBA gets dedup hits (increasing refcount), then the
    /// ORIGINAL LBAs are overwritten. The dedup-added LBAs should keep the
    /// PBA alive.
    #[test]
    fn packed_slot_refcount_with_dedup_and_overwrite() {
        let (meta, _pool, _lifecycle, allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
            setup_flush_test_env();

        let vol = VolumeId("flush-race".into());
        let packed_pba = allocator.allocate_one_for_lane(0).unwrap();

        // --- Step 1: Create packed slot with 32 LBAs ---
        let mut batch_values: Vec<(VolumeId, Lba, BlockmapValue)> = Vec::new();
        for i in 0u64..32 {
            batch_values.push((
                vol.clone(),
                Lba(1000 + i),
                BlockmapValue {
                    pba: packed_pba,
                    compression: 1,
                    unit_compressed_size: 1953,
                    unit_original_size: 131072,
                    unit_lba_count: 32,
                    offset_in_unit: i as u16,
                    crc32: 0xAAAAAAAA,
                    slot_offset: 0,
                    flags: 0,
                },
            ));
        }
        meta.atomic_batch_write_packed(&batch_values, packed_pba, 32)
            .unwrap();
        assert_eq!(meta.get_refcount(packed_pba).unwrap(), 32);

        // --- Step 2: Dedup hits map 16 additional LBAs to the same packed PBA ---
        // Register dedup_reverse entries so the guard passes
        let dedup_hashes: Vec<ContentHash> = (0u8..16).map(|i| [i + 100; 32]).collect();
        let dedup_entries: Vec<(ContentHash, DedupEntry)> = dedup_hashes
            .iter()
            .enumerate()
            .map(|(i, h)| {
                (
                    *h,
                    DedupEntry {
                        pba: packed_pba,
                        slot_offset: 0,
                        compression: 1,
                        unit_compressed_size: 1953,
                        unit_original_size: 131072,
                        unit_lba_count: 32,
                        offset_in_unit: i as u16,
                        crc32: 0xAAAAAAAA,
                    },
                )
            })
            .collect();
        meta.put_dedup_entries(&dedup_entries).unwrap();

        let dedup_hits: Vec<(Lba, BlockmapValue, ContentHash)> = (0u64..16)
            .map(|i| {
                (
                    Lba(5000 + i), // different LBAs
                    BlockmapValue {
                        pba: packed_pba,
                        compression: 1,
                        unit_compressed_size: 1953,
                        unit_original_size: 131072,
                        unit_lba_count: 32,
                        offset_in_unit: i as u16,
                        crc32: 0xAAAAAAAA,
                        slot_offset: 0,
                        flags: 0,
                    },
                    dedup_hashes[i as usize],
                )
            })
            .collect();

        let results = meta
            .atomic_batch_dedup_hits(&vol, &dedup_hits)
            .unwrap();
        let accepted = results
            .iter()
            .filter(|r| matches!(r, DedupHitResult::Accepted(_)))
            .count();
        assert_eq!(accepted, 16, "all 16 dedup hits should be accepted");
        assert_eq!(
            meta.get_refcount(packed_pba).unwrap(),
            48,
            "refcount should be 32 + 16 = 48"
        );

        // --- Step 3: Overwrite the ORIGINAL 32 LBAs via atomic_batch_write_multi ---
        let new_pba = allocator.allocate_one_for_lane(0).unwrap();
        let overwrite_entries: Vec<(Lba, BlockmapValue)> = (0u64..32)
            .map(|i| {
                (
                    Lba(1000 + i),
                    BlockmapValue {
                        pba: new_pba,
                        compression: 0,
                        unit_compressed_size: BLOCK_SIZE,
                        unit_original_size: BLOCK_SIZE,
                        unit_lba_count: 1,
                        offset_in_unit: 0,
                        crc32: 0x11111111,
                        slot_offset: 0,
                        flags: 0,
                    },
                )
            })
            .collect();

        let batch_args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> =
            vec![(&vol, &overwrite_entries, 32)];
        meta.atomic_batch_write_multi(&batch_args).unwrap();

        // --- Verify: packed_pba should still have refcount 16 (dedup LBAs remain) ---
        let rc = meta.get_refcount(packed_pba).unwrap();
        let refs = meta.count_blockmap_refs_for_pba(packed_pba).unwrap();

        println!(
            "After dedup + overwrite: refcount={}, blockmap_refs={}",
            rc, refs
        );
        assert_eq!(
            refs, 16,
            "16 dedup-mapped LBAs should still reference packed_pba"
        );
        assert_eq!(
            rc, 16,
            "refcount should be 16 (original 32 overwritten, 16 dedup remain)"
        );
        assert_eq!(rc, refs, "refcount must match blockmap refs");
    }

    /// Concurrent stress test: multiple threads hammer packed slot creation,
    /// overwrite, and dedup hits on shared PBAs. Checks for refcount drift.
    #[test]
    fn packed_slot_concurrent_refcount_drift() {
        use std::sync::atomic::{AtomicU64, AtomicBool};
        use std::sync::Barrier;

        let (meta, _pool, _lifecycle, allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
            setup_flush_test_env();

        let vol = VolumeId("flush-race".into());
        let meta = &*meta;
        let allocator = &*allocator;
        let vol = &vol;

        let lba_counter = AtomicU64::new(0);
        let iteration_count = 200;
        let thread_count = 4;
        let barrier = Barrier::new(thread_count);
        let found_drift = AtomicBool::new(false);

        std::thread::scope(|s| {
            for tid in 0..thread_count {
                let barrier = &barrier;
                let lba_counter = &lba_counter;
                let found_drift = &found_drift;

                s.spawn(move || {
                    barrier.wait();

                    for _iter in 0..iteration_count {
                        // Each iteration: create a packed slot, then overwrite its LBAs

                        // Allocate a packed PBA
                        let packed_pba = match allocator.allocate_one_for_lane(0) {
                            Ok(p) => p,
                            Err(_) => return,
                        };

                        // Create 8 LBAs in a packed slot
                        let base_lba = lba_counter.fetch_add(16, Ordering::Relaxed);
                        let lba_count = 8u64;

                        let batch_values: Vec<(VolumeId, Lba, BlockmapValue)> = (0..lba_count)
                            .map(|i| {
                                (
                                    vol.clone(),
                                    Lba(base_lba + i),
                                    BlockmapValue {
                                        pba: packed_pba,
                                        compression: 1,
                                        unit_compressed_size: 500,
                                        unit_original_size: 4096 * lba_count as u32,
                                        unit_lba_count: lba_count as u16,
                                        offset_in_unit: i as u16,
                                        crc32: 0xAA000000 + tid as u32,
                                        slot_offset: 0,
                                        flags: 0,
                                    },
                                )
                            })
                            .collect();

                        if meta
                            .atomic_batch_write_packed(
                                &batch_values,
                                packed_pba,
                                lba_count as u32,
                            )
                            .is_err()
                        {
                            continue;
                        }

                        // Immediately overwrite those LBAs via atomic_batch_write_multi
                        // (simulating a concurrent flush from another lane)
                        let new_pba = match allocator.allocate_one_for_lane(0) {
                            Ok(p) => p,
                            Err(_) => continue,
                        };

                        let overwrite: Vec<(Lba, BlockmapValue)> = (0..lba_count)
                            .map(|i| {
                                (
                                    Lba(base_lba + i),
                                    BlockmapValue {
                                        pba: new_pba,
                                        compression: 0,
                                        unit_compressed_size: BLOCK_SIZE,
                                        unit_original_size: BLOCK_SIZE,
                                        unit_lba_count: 1,
                                        offset_in_unit: 0,
                                        crc32: 0xBB000000 + tid as u32,
                                        slot_offset: 0,
                                        flags: 0,
                                    },
                                )
                            })
                            .collect();

                        let args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> =
                            vec![(vol, &overwrite, lba_count as u32)];
                        let _ = meta.atomic_batch_write_multi(&args);

                        // Check: packed_pba should have refcount 0 and 0 blockmap refs
                        let rc = meta.get_refcount(packed_pba).unwrap();
                        let refs = meta.count_blockmap_refs_for_pba(packed_pba).unwrap();
                        if rc != refs {
                            eprintln!(
                                "[tid={}] DRIFT at PBA {}: refcount={} blockmap_refs={}",
                                tid, packed_pba.0, rc, refs
                            );
                            found_drift.store(true, Ordering::Relaxed);
                        }
                    }
                });
            }
        });

        assert!(
            !found_drift.load(Ordering::Relaxed),
            "refcount drift detected under concurrent packed slot operations"
        );
    }

    /// Concurrent stress test: interleaved packed writes, dedup hits, and
    /// overwrites on SHARED PBAs (dedup causes cross-thread PBA sharing).
    #[test]
    fn packed_slot_concurrent_dedup_refcount_drift() {
        use std::sync::atomic::{AtomicU64, AtomicBool};
        use std::sync::Barrier;

        let (meta, _pool, _lifecycle, allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
            setup_flush_test_env();

        let vol = VolumeId("flush-race".into());
        let meta = &*meta;
        let allocator = &*allocator;
        let vol = &vol;

        let lba_counter = AtomicU64::new(10000);
        let found_drift = AtomicBool::new(false);
        let barrier = Barrier::new(3);

        // Pre-create some packed PBAs that threads will share via dedup
        let shared_pbas: Vec<Pba> = (0..20)
            .map(|_| allocator.allocate_one_for_lane(0).unwrap())
            .collect();

        // Initialize shared PBAs with packed data
        for (idx, &pba) in shared_pbas.iter().enumerate() {
            let base_lba = lba_counter.fetch_add(8, Ordering::Relaxed);
            let batch: Vec<(VolumeId, Lba, BlockmapValue)> = (0..8u64)
                .map(|i| {
                    (
                        vol.clone(),
                        Lba(base_lba + i),
                        BlockmapValue {
                            pba,
                            compression: 1,
                            unit_compressed_size: 400,
                            unit_original_size: 32768,
                            unit_lba_count: 8,
                            offset_in_unit: i as u16,
                            crc32: 0xCC000000 + idx as u32,
                            slot_offset: 0,
                            flags: 0,
                        },
                    )
                })
                .collect();
            meta.atomic_batch_write_packed(&batch, pba, 8).unwrap();

            // Register dedup entries so dedup hits work
            let hashes: Vec<ContentHash> = (0..8u8)
                .map(|i| {
                    let mut h = [0u8; 32];
                    h[0] = idx as u8;
                    h[1] = i;
                    h
                })
                .collect();
            let dedup_entries: Vec<(ContentHash, DedupEntry)> = hashes
                .iter()
                .enumerate()
                .map(|(i, h)| {
                    (
                        *h,
                        DedupEntry {
                            pba,
                            slot_offset: 0,
                            compression: 1,
                            unit_compressed_size: 400,
                            unit_original_size: 32768,
                            unit_lba_count: 8,
                            offset_in_unit: i as u16,
                            crc32: 0xCC000000 + idx as u32,
                        },
                    )
                })
                .collect();
            meta.put_dedup_entries(&dedup_entries).unwrap();
        }

        std::thread::scope(|s| {
            // Thread 1: dedup hits — maps NEW LBAs to shared packed PBAs
            s.spawn(|| {
                barrier.wait();
                for round in 0..100u64 {
                    let pba_idx = (round as usize) % shared_pbas.len();
                    let pba = shared_pbas[pba_idx];
                    let base_lba = lba_counter.fetch_add(4, Ordering::Relaxed);

                    let hashes: Vec<ContentHash> = (0..4u8)
                        .map(|i| {
                            let mut h = [0u8; 32];
                            h[0] = pba_idx as u8;
                            h[1] = i;
                            h
                        })
                        .collect();

                    let hits: Vec<(Lba, BlockmapValue, ContentHash)> = (0..4u64)
                        .map(|i| {
                            (
                                Lba(base_lba + i),
                                BlockmapValue {
                                    pba,
                                    compression: 1,
                                    unit_compressed_size: 400,
                                    unit_original_size: 32768,
                                    unit_lba_count: 8,
                                    offset_in_unit: i as u16,
                                    crc32: 0xCC000000 + pba_idx as u32,
                                    slot_offset: 0,
                                    flags: 0,
                                },
                                hashes[i as usize],
                            )
                        })
                        .collect();

                    let _ = meta.atomic_batch_dedup_hits(vol, &hits);
                }
            });

            // Thread 2: overwrites — overwrites LBAs that point to shared packed PBAs
            s.spawn(|| {
                barrier.wait();
                for round in 0..100u64 {
                    let pba_idx = (round as usize) % shared_pbas.len();
                    // Find some LBAs pointing to this PBA and overwrite them
                    let pba = shared_pbas[pba_idx];
                    let new_pba = match allocator.allocate_one_for_lane(0) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };

                    // Overwrite the first 4 original LBAs of this PBA
                    // (base LBAs were: 10000 + pba_idx*8 .. 10000 + pba_idx*8 + 7)
                    let orig_base = 10000 + (pba_idx as u64) * 8;
                    let overwrite: Vec<(Lba, BlockmapValue)> = (0..4u64)
                        .map(|i| {
                            (
                                Lba(orig_base + i),
                                BlockmapValue {
                                    pba: new_pba,
                                    compression: 0,
                                    unit_compressed_size: BLOCK_SIZE,
                                    unit_original_size: BLOCK_SIZE,
                                    unit_lba_count: 1,
                                    offset_in_unit: 0,
                                    crc32: 0xDD000000,
                                    slot_offset: 0,
                                    flags: 0,
                                },
                            )
                        })
                        .collect();

                    let args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> =
                        vec![(vol, &overwrite, 4)];
                    let _ = meta.atomic_batch_write_multi(&args);
                }
            });

            // Thread 3: more packed slot writes that create NEW packed slots
            s.spawn(|| {
                barrier.wait();
                for _round in 0..100 {
                    let pba = match allocator.allocate_one_for_lane(0) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };
                    let base_lba = lba_counter.fetch_add(8, Ordering::Relaxed);
                    let batch: Vec<(VolumeId, Lba, BlockmapValue)> = (0..8u64)
                        .map(|i| {
                            (
                                vol.clone(),
                                Lba(base_lba + i),
                                BlockmapValue {
                                    pba,
                                    compression: 1,
                                    unit_compressed_size: 300,
                                    unit_original_size: 32768,
                                    unit_lba_count: 8,
                                    offset_in_unit: i as u16,
                                    crc32: 0xEE000000,
                                    slot_offset: 0,
                                    flags: 0,
                                },
                            )
                        })
                        .collect();
                    let _ = meta.atomic_batch_write_packed(&batch, pba, 8);
                }
            });
        });

        // Final check: scan shared PBAs for drift
        let mut drift_count = 0u32;
        for &pba in &shared_pbas {
            let stored = meta.get_refcount(pba).unwrap();
            let actual = meta.count_blockmap_refs_for_pba(pba).unwrap();
            if stored != actual {
                eprintln!(
                    "DRIFT PBA {}: stored={} actual={} diff={}",
                    pba.0,
                    stored,
                    actual,
                    actual as i64 - stored as i64
                );
                drift_count += 1;
                found_drift.store(true, Ordering::Relaxed);
            }
        }

        if drift_count > 0 {
            eprintln!("Total PBAs with drift: {}", drift_count);
        }
        assert!(
            !found_drift.load(Ordering::Relaxed),
            "refcount drift detected under concurrent packed + dedup + overwrite"
        );
    }

    /// High-pressure concurrent test: thread 1 calls write_packed_slot,
    /// thread 2 calls atomic_batch_write_multi on the SAME LBAs, racing the
    /// live_positions_for_unit check against blockmap updates.
    #[test]
    fn packed_slot_full_pipeline_concurrent_drift() {
        use std::sync::atomic::AtomicBool;
        use std::sync::Barrier;

        let (meta, pool, lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
            setup_flush_test_env();

        let vol_id = "flush-race";
        let vol = VolumeId(vol_id.into());
        let hole_map = crate::packer::packer::new_hole_map();
        let found_drift = AtomicBool::new(false);
        let barrier = Barrier::new(2);
        let rounds = 500;

        std::thread::scope(|s| {
            let meta1 = &meta;
            let pool1 = &pool;
            let lifecycle1 = &lifecycle;
            let allocator1 = &allocator;
            let io_engine1 = &io_engine;
            let metrics1 = &metrics;
            let hole_map1 = &hole_map;
            let barrier1 = &barrier;
            let vol1 = &vol;

            // Thread 1: create packed slots for LBAs 0..7 and commit via write_packed_slot
            s.spawn(move || {
                barrier1.wait();
                for _ in 0..rounds {
                    // Append 8 LBAs to buffer so live_positions_for_unit can find them
                    let data = vec![0xAAu8; BLOCK_SIZE as usize];
                    let mut seqs = Vec::new();
                    for lba in 0u64..8 {
                        if let Ok(seq) = pool1.append(vol_id, Lba(lba), 1, &data, 1) {
                            seqs.push((seq, Lba(lba), 1u32));
                        }
                    }
                    if seqs.len() != 8 {
                        continue;
                    }

                    let pba = match allocator1.allocate_one_for_lane(0) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };

                    let compressed = vec![0xAAu8; 500];
                    let crc = crc32fast::hash(&compressed);
                    let mut slot_data = vec![0u8; BLOCK_SIZE as usize];
                    slot_data[..500].copy_from_slice(&compressed);

                    let sealed = crate::packer::packer::SealedSlot {
                        pba,
                        data: slot_data,
                        fragments: vec![crate::packer::packer::SlotFragment {
                            unit: CompressedUnit {
                                vol_id: vol_id.to_string(),
                                start_lba: Lba(0),
                                lba_count: 8,
                                compressed_data: compressed,
                                original_size: BLOCK_SIZE * 8,
                                compression: 0,
                                crc32: crc,
                                seq_lba_ranges: seqs,
                                block_hashes: None,
                                dedup_skipped: false,
                                vol_created_at: 1,
                                dedup_completion: None,
                            },
                            slot_offset: 0,
                        }],
                    };

                    let _ = BufferFlusher::write_packed_slot(
                        0, &sealed, pool1, meta1, lifecycle1, allocator1,
                        io_engine1, hole_map1, metrics1,
                    );
                }
            });

            // Thread 2: concurrently overwrite same LBAs via atomic_batch_write_multi
            let meta2 = &meta;
            let pool2 = &pool;
            let allocator2 = &allocator;
            let vol2 = &vol;
            let barrier2 = &barrier;

            s.spawn(move || {
                barrier2.wait();
                for _ in 0..rounds {
                    // Append newer data so it supersedes thread 1's entries
                    let data = vec![0xBBu8; BLOCK_SIZE as usize];
                    for lba in 0u64..8 {
                        let _ = pool2.append(vol_id, Lba(lba), 1, &data, 1);
                    }

                    let new_pba = match allocator2.allocate_one_for_lane(0) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };

                    let entries: Vec<(Lba, BlockmapValue)> = (0u64..8)
                        .map(|i| {
                            (
                                Lba(i),
                                BlockmapValue {
                                    pba: new_pba,
                                    compression: 0,
                                    unit_compressed_size: BLOCK_SIZE,
                                    unit_original_size: BLOCK_SIZE,
                                    unit_lba_count: 1,
                                    offset_in_unit: 0,
                                    crc32: 0xBBBBBBBB,
                                    slot_offset: 0,
                                    flags: 0,
                                },
                            )
                        })
                        .collect();

                    let args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> =
                        vec![(vol2, &entries, 8)];
                    let _ = meta2.atomic_batch_write_multi(&args);
                }
            });
        });

        // Final drift check: scan all allocated PBAs
        let mut any_drift = false;
        let mut checked = 0u32;
        for pba_val in 0..20000u64 {
            let pba = Pba(pba_val + crate::types::RESERVED_BLOCKS);
            let rc = meta.get_refcount(pba).unwrap();
            let refs = meta.count_blockmap_refs_for_pba(pba).unwrap();
            if rc == 0 && refs == 0 {
                continue;
            }
            checked += 1;
            if rc != refs {
                eprintln!(
                    "DRIFT PBA {}: stored={} actual={} diff={}",
                    pba.0, rc, refs, refs as i64 - rc as i64
                );
                any_drift = true;
            }
        }
        eprintln!("Checked {} PBAs with non-zero state", checked);
        assert!(!any_drift, "refcount drift in full pipeline concurrent test");
    }

    /// Proof-of-concept: atomic_batch_write_packed uses PUT to set refcount.
    /// If the PBA already has additional references (from dedup or a previous
    /// incarnation), PUT overwrites the total, causing drift.
    #[test]
    fn packed_slot_put_overwrites_dedup_refcount() {
        let (meta, _pool, _lifecycle, _allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
            setup_flush_test_env();

        let vol = VolumeId("flush-race".into());
        let pba = Pba(999);

        // Step 1: Simulate dedup hits that already incremented this PBA's refcount.
        // In production this happens when dedup maps LBAs to an existing packed PBA.
        meta.set_refcount(pba, 4).unwrap();  // 4 dedup refs already exist
        // Create the 4 blockmap entries that these dedup refs represent
        for i in 0u64..4 {
            meta.put_mapping(
                &vol,
                Lba(100 + i),
                &BlockmapValue {
                    pba,
                    compression: 1,
                    unit_compressed_size: 500,
                    unit_original_size: 32768,
                    unit_lba_count: 8,
                    offset_in_unit: i as u16,
                    crc32: 0xAAAAAAAA,
                    slot_offset: 0,
                    flags: 0,
                },
            )
            .unwrap();
        }
        assert_eq!(meta.get_refcount(pba).unwrap(), 4);
        assert_eq!(meta.count_blockmap_refs_for_pba(pba).unwrap(), 4);

        // Step 2: write_packed_slot calls atomic_batch_write_packed with 8 NEW LBAs.
        // This simulates a sealed slot being written to an already-referenced PBA.
        let batch_values: Vec<(VolumeId, Lba, BlockmapValue)> = (0u64..8)
            .map(|i| {
                (
                    vol.clone(),
                    Lba(200 + i),  // DIFFERENT LBAs from the dedup ones
                    BlockmapValue {
                        pba,
                        compression: 1,
                        unit_compressed_size: 500,
                        unit_original_size: 32768,
                        unit_lba_count: 8,
                        offset_in_unit: i as u16,
                        crc32: 0xBBBBBBBB,
                        slot_offset: 0,
                        flags: 0,
                    },
                )
            })
            .collect();

        meta.atomic_batch_write_packed(&batch_values, pba, 8).unwrap();

        // Step 3: Check for drift
        let rc = meta.get_refcount(pba).unwrap();
        let refs = meta.count_blockmap_refs_for_pba(pba).unwrap();

        eprintln!("After packed write over dedup PBA: refcount={}, blockmap_refs={}", rc, refs);

        // BUG: refcount=8 (PUT overwrote the 4 dedup refs) but blockmap_refs=12 (4 dedup + 8 new)
        // EXPECTED (correct): refcount=12, blockmap_refs=12
        assert_eq!(refs, 12, "should have 4 dedup + 8 packed = 12 blockmap refs");
        assert_eq!(rc, refs, "refcount must match blockmap refs — PUT overwrites dedup increment!");
    }

    /// End-to-end regression: the full chain that led to CRC mismatch in soak.
    ///
    /// 1. Packed slot created at PBA P (refcount = 8)
    /// 2. Dedup adds 4 refs (refcount should be 12)
    /// 3. The 8 original LBAs are overwritten (decrement 8)
    /// 4. With the old PUT bug: refcount would be 8-8=0 → PBA freed → reuse → CRC mismatch
    /// 5. With the fix: refcount = 12-8=4 → PBA stays alive → no CRC mismatch
    #[test]
    fn packed_slot_full_chain_no_premature_free() {
        let (meta, _pool, _lifecycle, allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
            setup_flush_test_env();
        let vol = VolumeId("flush-race".into());
        let hole_map = crate::packer::packer::new_hole_map();

        let packed_pba = Pba(999);
        meta.set_refcount(packed_pba, 0).unwrap();

        // Step 1: create packed slot with 8 LBAs
        let packed_entries: Vec<(VolumeId, Lba, BlockmapValue)> = (0u64..8)
            .map(|i| {
                (
                    vol.clone(),
                    Lba(1000 + i),
                    BlockmapValue {
                        pba: packed_pba,
                        compression: 1,
                        unit_compressed_size: 500,
                        unit_original_size: 32768,
                        unit_lba_count: 8,
                        offset_in_unit: i as u16,
                        crc32: 0xAAAAAAAA,
                        slot_offset: 0,
                        flags: 0,
                    },
                )
            })
            .collect();
        meta.atomic_batch_write_packed(&packed_entries, packed_pba, 8)
            .unwrap();
        assert_eq!(meta.get_refcount(packed_pba).unwrap(), 8);

        // Step 2: dedup hits add 4 more refs
        let dedup_hashes: Vec<ContentHash> = (0u8..4).map(|i| [i + 50; 32]).collect();
        let dedup_entries: Vec<(ContentHash, DedupEntry)> = dedup_hashes
            .iter()
            .enumerate()
            .map(|(i, h)| (*h, DedupEntry {
                pba: packed_pba, slot_offset: 0, compression: 1,
                unit_compressed_size: 500, unit_original_size: 32768,
                unit_lba_count: 8, offset_in_unit: i as u16, crc32: 0xAAAAAAAA,
            }))
            .collect();
        meta.put_dedup_entries(&dedup_entries).unwrap();

        let hits: Vec<(Lba, BlockmapValue, ContentHash)> = (0u64..4)
            .map(|i| (
                Lba(5000 + i),
                BlockmapValue {
                    pba: packed_pba, compression: 1, unit_compressed_size: 500,
                    unit_original_size: 32768, unit_lba_count: 8,
                    offset_in_unit: i as u16, crc32: 0xAAAAAAAA,
                    slot_offset: 0, flags: 0,
                },
                dedup_hashes[i as usize],
            ))
            .collect();
        meta.atomic_batch_dedup_hits(&vol, &hits).unwrap();
        assert_eq!(meta.get_refcount(packed_pba).unwrap(), 12);

        // Step 3: overwrite ALL 8 original LBAs → decrement packed_pba by 8
        let new_pba = allocator.allocate_one_for_lane(0).unwrap();
        let overwrite: Vec<(Lba, BlockmapValue)> = (0u64..8)
            .map(|i| (
                Lba(1000 + i),
                BlockmapValue {
                    pba: new_pba, compression: 0,
                    unit_compressed_size: BLOCK_SIZE, unit_original_size: BLOCK_SIZE,
                    unit_lba_count: 1, offset_in_unit: 0,
                    crc32: 0x11111111, slot_offset: 0, flags: 0,
                },
            ))
            .collect();
        let args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> =
            vec![(&vol, &overwrite, 8)];
        meta.atomic_batch_write_multi(&args).unwrap();

        // Step 4: verify packed_pba is NOT prematurely freed
        let rc = meta.get_refcount(packed_pba).unwrap();
        let refs = meta.count_blockmap_refs_for_pba(packed_pba).unwrap();

        eprintln!("After full chain: refcount={}, blockmap_refs={}", rc, refs);

        assert_eq!(refs, 4, "4 dedup LBAs should still reference packed_pba");
        assert_eq!(rc, 4, "refcount should be 12 - 8 = 4, NOT 0 (premature free)");

        // Step 5: verify cleanup would NOT free this PBA
        let live_rc = BufferFlusher::live_refcount_with_reconcile(
            &meta, packed_pba, "test_chain",
        ).unwrap();
        assert_eq!(live_rc, 4, "cleanup must see refcount > 0 and NOT free the PBA");
    }

    /// Focused race: two threads each call write_packed_slot for DIFFERENT
    /// fragments that target the SAME LBAs. The second thread's fragments
    /// overwrite the first's blockmap entries — simulating what happens when
    /// two flush lanes pack the same LBAs (due to GC re-injection or
    /// overlapping coalesce output).
    #[test]
    fn packed_slot_overlapping_lba_race() {
        use std::sync::atomic::AtomicBool;
        use std::sync::Barrier;

        let (meta, pool, lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
            setup_flush_test_env();

        let vol_id = "flush-race";
        let hole_map = crate::packer::packer::new_hole_map();
        let found_drift = AtomicBool::new(false);
        let rounds = 1000;

        for _ in 0..rounds {
            // Both threads write to LBAs 0..3
            let data = vec![0xCCu8; BLOCK_SIZE as usize];
            let mut seqs = Vec::new();
            for lba in 0u64..4 {
                if let Ok(seq) = pool.append(vol_id, Lba(lba), 1, &data, 1) {
                    seqs.push((seq, Lba(lba), 1u32));
                }
            }
            if seqs.len() != 4 {
                continue;
            }

            let pba_a = match allocator.allocate_one_for_lane(0) {
                Ok(p) => p,
                Err(_) => continue,
            };
            let pba_b = match allocator.allocate_one_for_lane(0) {
                Ok(p) => p,
                Err(_) => continue,
            };

            let compressed = vec![0xCCu8; 300];
            let crc = crc32fast::hash(&compressed);
            let mut slot_a = vec![0u8; BLOCK_SIZE as usize];
            let mut slot_b = vec![0u8; BLOCK_SIZE as usize];
            slot_a[..300].copy_from_slice(&compressed);
            slot_b[..300].copy_from_slice(&compressed);

            let make_sealed = |pba: Pba, slot: Vec<u8>| {
                crate::packer::packer::SealedSlot {
                    pba,
                    data: slot,
                    fragments: vec![crate::packer::packer::SlotFragment {
                        unit: CompressedUnit {
                            vol_id: vol_id.to_string(),
                            start_lba: Lba(0),
                            lba_count: 4,
                            compressed_data: compressed.clone(),
                            original_size: BLOCK_SIZE * 4,
                            compression: 0,
                            crc32: crc,
                            seq_lba_ranges: seqs.clone(),
                            block_hashes: None,
                            dedup_skipped: false,
                            vol_created_at: 1,
                            dedup_completion: None,
                        },
                        slot_offset: 0,
                    }],
                }
            };

            let sealed_a = make_sealed(pba_a, slot_a);
            let sealed_b = make_sealed(pba_b, slot_b);

            // Spawn 16 threads, each with its own PBA, all targeting same LBAs
            let thread_count = 16;
            let mut all_pbas: Vec<Pba> = vec![pba_a, pba_b];
            let mut all_sealed: Vec<crate::packer::packer::SealedSlot> =
                vec![sealed_a, sealed_b];
            for _ in 2..thread_count {
                let p = match allocator.allocate_one_for_lane(0) {
                    Ok(p) => p,
                    Err(_) => break,
                };
                let mut sd = vec![0u8; BLOCK_SIZE as usize];
                sd[..300].copy_from_slice(&compressed);
                all_sealed.push(make_sealed(p, sd));
                all_pbas.push(p);
            }
            let actual_threads = all_sealed.len();
            let barrier = Barrier::new(actual_threads);

            std::thread::scope(|s| {
                for sealed in &all_sealed {
                    s.spawn(|| {
                        barrier.wait();
                        let _ = BufferFlusher::write_packed_slot(
                            0, sealed, &pool, &meta, &lifecycle, &allocator,
                            &io_engine, &hole_map, &metrics,
                        );
                    });
                }
            });

            // One thread wins, the rest should correctly decrement losers' PBAs
            for &pba in &all_pbas {
                let rc = meta.get_refcount(pba).unwrap();
                let refs = meta.count_blockmap_refs_for_pba(pba).unwrap();
                if rc != refs {
                    eprintln!(
                        "DRIFT PBA {}: stored={} actual={} diff={}",
                        pba.0, rc, refs, refs as i64 - rc as i64
                    );
                    found_drift.store(true, Ordering::Relaxed);
                }
            }
        }

        assert!(
            !found_drift.load(Ordering::Relaxed),
            "refcount drift when two write_packed_slot calls race on same LBAs"
        );
    }
}
