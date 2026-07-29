//! Defrag target selection — the physical-neighborhood compaction lever.
//!
//! On a fragmented pool the free list shatters into millions of sub-stripe
//! extents ("confetti") pinned in place by interleaved live blocks. The
//! per-unit dead-ratio compactor can never see this: dead_ratio is a property
//! INSIDE one compression unit, while defrag value is a property of the
//! physical neighborhood — how few live blocks pin how much free space. This
//! module walks the free list by ADDRESS, finds confetti clusters worth
//! evacuating, and publishes them as target ranges; the scanner then promotes
//! any live fragment whose PBA falls inside a target to a rewrite candidate
//! regardless of dead ratio (see `scanner.rs`), and the unchanged rewriter +
//! retire→reclaim path does the rest (reclaim coalesces, so once the pinners
//! move, the interleaved dead blocks merge into large stripe-capable runs).
//!
//! Direction is load-bearing: the walk runs DESCENDING from the device top
//! while allocation stays strictly first-fit-by-address (lowest first), so
//! evacuated blocks re-land in low-address holes — source and destination
//! separate naturally instead of treadmilling. Allocation policy itself is
//! NEVER touched (dense-PBA metadb leaf contract).
//!
//! Target lifecycle: select one exact stripe → quarantine its free fragments →
//! evacuate every live reference → wait for retire/reclaim to make the entire
//! stripe free → atomically publish it back as stripe reserve. Quarantine is an
//! allocation policy state, not persistent metadata: after a crash the folded
//! L2P/dedup index rebuild remains authoritative and merely loses cleaner
//! progress.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::metrics::EngineMetrics;
use crate::space::allocator::SpaceAllocator;
use crate::space::extent::Extent;
use crate::types::Pba;

use super::config::GcConfig;

/// Per-chunk bound on one free-lock snapshot (mirrors the allocator's batch
/// pattern; each hold copies ≤ this many extents, sub-millisecond).
const WALK_CHUNK: usize = 4096;
/// Inter-chunk breather so the unfair free mutex interleaves foreground
/// alloc/free between our snapshot holds (same value as the allocator's
/// retire/reclaim batches).
const WALK_BREATHER: std::time::Duration = std::time::Duration::from_micros(500);
/// Hard bound on one cluster's span (blocks; 2048 = 8 MiB at 4 KiB). On a
/// real confetti belt inter-extent gaps are almost always ≤ gap_max, so an
/// unbounded accumulator swallows the WHOLE belt into one cluster — and any
/// single stripe-capable member (or the belt-wide diluted density) then
/// rejects everything (box 2026-07-03: 1M extents walked, 16 giant clusters,
/// 0 qualified). Bounding the span makes qualification a LOCAL property and
/// each emitted window still yields multi-MiB contiguous runs.
const MAX_CLUSTER_SPAN: u64 = 2048;
/// Reject a window whose stripe-capable capacity already covers more than
/// this share of its span — it is mostly usable as-is; evacuating it buys
/// little contiguity per block moved.
const CLUSTER_CAPABLE_MAX_PCT: u64 = 25;

/// One cluster being accumulated across chunk/cycle boundaries. The walk is
/// descending, so the cluster grows downward: `lo` falls, `hi` is fixed at
/// the top member's end.
struct ClusterAcc {
    lo: u64,
    hi: u64,
    free_blocks: u64,
    /// Σ whole-stripe capacity over members — how much of this window the
    /// stripe allocator can already use without any defrag.
    capable_blocks: u64,
}

/// What the runner consumes each cycle.
pub(crate) struct DefragCycle {
    /// Ascending, non-overlapping target ranges (empty when inactive).
    pub targets: Arc<Vec<Extent>>,
    /// Trigger latched this cycle (drives the effort floor).
    pub active: bool,
}

impl DefragCycle {
    pub(crate) fn inactive() -> Self {
        Self {
            targets: Arc::new(Vec::new()),
            active: false,
        }
    }
}

pub(crate) struct DefragState {
    active: bool,
    /// Next walk visits free extents strictly below this address.
    cursor: Pba,
    pending: Option<ClusterAcc>,
    /// Active allocator-owned quarantine targets. Every target is exactly one
    /// stripe and remains here until all blocks are truly free (retired blocks
    /// do not count) or the no-progress watchdog cancels it.
    targets: BTreeMap<u64, ActiveTarget>,
    /// Σ span over `targets` (the budget).
    target_blocks: u64,
}

struct ActiveTarget {
    count: u32,
    last_free_blocks: u64,
    last_progress: Instant,
}

impl DefragState {
    pub(crate) fn new() -> Self {
        Self {
            active: false,
            cursor: Pba(u64::MAX),
            pending: None,
            targets: BTreeMap::new(),
            target_blocks: 0,
        }
    }

    /// Run one defrag maintenance step: evaluate the trigger, retire "done"
    /// targets, and (budget permitting) continue the descending cluster walk.
    /// Called once per GC cycle from the gc-runner thread (single writer).
    pub(crate) fn maintain(
        &mut self,
        allocator: &SpaceAllocator,
        cfg: &GcConfig,
        free_pct: u64,
        metrics: &EngineMetrics,
    ) -> DefragCycle {
        use std::sync::atomic::Ordering::Relaxed;

        // Completion is based on actual allocator ownership, not "free +
        // retired" coverage. Retired PBAs are still allocated until the hazard
        // and metadata gates reclaim them, and even a few scattered pinners can
        // make every stripe unusable. The allocator publishes a fully-free
        // quarantine atomically into the stripe reserve.
        let now = Instant::now();
        let stall_after = Duration::from_secs(cfg.defrag_target_stall_secs.max(1));
        let mut completed = Vec::new();
        let mut cancelled = Vec::new();
        for (&start, target) in self.targets.iter_mut() {
            let pba = Pba(start);
            let Some((free_blocks, total_blocks)) = allocator.defrag_quarantine_progress(pba)
            else {
                // Allocator state disappeared only after an explicit completion
                // or cancellation. Drop the stale scanner range defensively.
                completed.push(start);
                continue;
            };
            if free_blocks > target.last_free_blocks {
                target.last_free_blocks = free_blocks;
                target.last_progress = now;
            }
            if free_blocks == total_blocks {
                match allocator.complete_defrag_quarantine(pba) {
                    Ok(true) => completed.push(start),
                    Ok(false) => {}
                    Err(error) => {
                        tracing::warn!(start, error = %error, "defrag target publish failed");
                    }
                }
            } else if now.duration_since(target.last_progress) >= stall_after {
                if allocator.cancel_defrag_quarantine(pba) {
                    cancelled.push(start);
                }
            }
        }
        for start in completed {
            if let Some(target) = self.targets.remove(&start) {
                self.target_blocks -= u64::from(target.count);
                metrics.gc_defrag_segments_completed.fetch_add(1, Relaxed);
                tracing::info!(
                    start,
                    blocks = target.count,
                    "defrag stripe published to reserve"
                );
            }
        }
        for start in cancelled {
            if let Some(target) = self.targets.remove(&start) {
                self.target_blocks -= u64::from(target.count);
                metrics.gc_defrag_segments_cancelled.fetch_add(1, Relaxed);
                tracing::warn!(
                    start,
                    blocks = target.count,
                    "defrag target cancelled after no progress"
                );
            }
        }

        let stats = allocator.contiguity_stats();
        publish_contiguity(metrics, &stats);

        let geom = allocator.stripe_geometry();
        let capable_pct = match (stats.stripe_capable_blocks, stats.free_blocks_in_set) {
            (Some(cap), total) if total > 0 => cap * 100 / total,
            _ => 100, // no geometry / empty set: defrag is meaningless
        };

        // Trigger latch with +10 pct exit hysteresis.
        let enter = cfg.defrag_stripe_capable_min_pct as u64;
        if !cfg.defrag_enabled || geom.is_none() || free_pct < cfg.defrag_min_free_pct as u64 {
            self.deactivate(allocator, metrics);
        } else if self.active {
            if capable_pct >= enter + 10 {
                self.deactivate(allocator, metrics);
            }
        } else if capable_pct < enter {
            self.active = true;
            tracing::info!(
                capable_pct,
                free_extents = stats.free_extents,
                largest_run = stats.largest_run_blocks,
                "defrag mode ENTER: free pool no longer stripe-capable"
            );
        }
        metrics
            .gc_defrag_mode_active
            .store(self.active as u64, Relaxed);
        if !self.active {
            return DefragCycle::inactive();
        }
        let (stripe, phase) = geom.expect("active implies geometry");

        // Continue the descending walk while span budget remains.
        let mut walked = 0usize;
        while walked < cfg.defrag_scan_extents_per_cycle
            && self.target_blocks < cfg.defrag_max_target_blocks
            && self.targets.len() < cfg.defrag_max_active_targets.max(1)
        {
            if walked > 0 {
                std::thread::sleep(WALK_BREATHER);
            }
            let chunk_max = WALK_CHUNK.min(cfg.defrag_scan_extents_per_cycle - walked);
            let chunk = allocator.free_extents_below_desc(self.cursor, chunk_max);
            let exhausted = chunk.len() < chunk_max;
            walked += chunk.len();

            for e in &chunk {
                self.cursor = e.start;
                if self.target_blocks >= cfg.defrag_max_target_blocks
                    || self.targets.len() >= cfg.defrag_max_active_targets.max(1)
                {
                    break;
                }
                let closed = match &mut self.pending {
                    None => {
                        self.pending = Some(ClusterAcc::seed(*e, stripe, phase));
                        None
                    }
                    Some(acc) => {
                        // Descending walk: `e` is strictly below the cluster.
                        let gap = acc.lo.saturating_sub(e.end_pba().0);
                        if gap <= cfg.defrag_gap_max_blocks as u64
                            && acc.hi - e.start.0 <= MAX_CLUSTER_SPAN
                        {
                            acc.extend_down(*e, stripe, phase);
                            None
                        } else {
                            // Gap break OR span bound: close the window here
                            // and seed the next one from this extent, keeping
                            // qualification a local (≤ MAX_CLUSTER_SPAN)
                            // property even on a wall-to-wall confetti belt.
                            self.pending.replace(ClusterAcc::seed(*e, stripe, phase))
                        }
                    }
                };
                if let Some(acc) = closed {
                    self.qualify_and_emit(acc, allocator, cfg, metrics);
                }
            }

            if exhausted {
                // Walk lap complete: close the tail cluster, wrap to the top.
                // Targets are NOT cleared — the done-recheck above retires
                // them; still-fragmented regions simply re-qualify (the
                // overlap-skip in qualify_and_emit makes that idempotent).
                if let Some(acc) = self.pending.take() {
                    self.qualify_and_emit(acc, allocator, cfg, metrics);
                }
                self.cursor = Pba(u64::MAX);
                tracing::debug!(walked, "defrag walk lap complete; wrapping to device top");
                break;
            }
        }
        metrics
            .gc_defrag_walk_extents
            .fetch_add(walked as u64, Relaxed);
        self.cycle_output(metrics)
    }

    fn qualify_and_emit(
        &mut self,
        acc: ClusterAcc,
        allocator: &SpaceAllocator,
        cfg: &GcConfig,
        metrics: &EngineMetrics,
    ) {
        use std::sync::atomic::Ordering::Relaxed;
        let span = acc.hi - acc.lo;
        // Reject: pure free space with no interior pinners (nothing to
        // evacuate), or a window whose stripe-capable capacity is already
        // high (mostly usable as-is — evacuation buys little per block moved).
        if acc.free_blocks >= span || acc.capable_blocks * 100 > span * CLUSTER_CAPABLE_MAX_PCT {
            metrics.gc_defrag_clusters_rejected.fetch_add(1, Relaxed);
            return;
        }

        let Some((stripe, phase)) = allocator.stripe_geometry() else {
            return;
        };
        let stripe_u64 = u64::from(stripe);
        if stripe <= 1 || span < stripe_u64 {
            metrics.gc_defrag_clusters_rejected.fetch_add(1, Relaxed);
            return;
        }

        // Materialize exact full-stripe candidates inside the cluster and rank
        // them by relocation cost (most already-free/reclaimable first), then by
        // descending PBA to preserve the source/destination separation policy.
        let mut candidates = Vec::new();
        let mut start = align_up_with_phase(acc.lo, stripe_u64, u64::from(phase));
        while start.saturating_add(stripe_u64) <= acc.hi {
            let target = Extent::new(Pba(start), stripe);
            if !self.overlaps_existing(target) {
                let free = allocator.free_overlap_blocks(target);
                let retired = allocator.retired_overlap_blocks(target);
                let reclaimable = free.saturating_add(retired);
                if free < stripe_u64
                    && reclaimable * 100 >= stripe_u64 * u64::from(cfg.defrag_min_free_density_pct)
                {
                    candidates.push((reclaimable, start));
                }
            }
            start = start.saturating_add(stripe_u64);
        }
        candidates.sort_unstable_by(|a, b| b.cmp(a));

        let mut emitted = 0u64;
        for (_, start) in candidates {
            if self.target_blocks + stripe_u64 > cfg.defrag_max_target_blocks
                || self.targets.len() >= cfg.defrag_max_active_targets.max(1)
            {
                break;
            }
            let target = Extent::new(Pba(start), stripe);
            match allocator.begin_defrag_quarantine(target) {
                Ok(()) => {
                    let initial_free = allocator
                        .defrag_quarantine_progress(target.start)
                        .map_or(0, |(free, _)| free);
                    self.targets.insert(
                        start,
                        ActiveTarget {
                            count: stripe,
                            last_free_blocks: initial_free,
                            last_progress: Instant::now(),
                        },
                    );
                    self.target_blocks += stripe_u64;
                    emitted += 1;
                }
                Err(error) => {
                    tracing::debug!(start, error = %error, "defrag target quarantine rejected");
                }
            }
        }
        if emitted > 0 {
            metrics
                .gc_defrag_clusters_qualified
                .fetch_add(emitted, Relaxed);
        } else {
            metrics.gc_defrag_clusters_rejected.fetch_add(1, Relaxed);
        }
    }

    fn overlaps_existing(&self, t: Extent) -> bool {
        if let Some((&s, target)) = self.targets.range(..=t.start.0).next_back() {
            if s + u64::from(target.count) > t.start.0 {
                return true;
            }
        }
        self.targets
            .range(t.start.0..)
            .next()
            .is_some_and(|(&s, _)| s < t.end_pba().0)
    }

    pub(crate) fn deactivate(&mut self, allocator: &SpaceAllocator, metrics: &EngineMetrics) {
        use std::sync::atomic::Ordering::Relaxed;
        if self.active {
            tracing::info!("defrag mode EXIT: free pool stripe-capable again");
        }
        let starts: Vec<Pba> = self.targets.keys().copied().map(Pba).collect();
        for start in starts {
            if allocator.cancel_defrag_quarantine(start) {
                metrics.gc_defrag_segments_cancelled.fetch_add(1, Relaxed);
            }
        }
        self.active = false;
        self.pending = None;
        self.targets.clear();
        self.target_blocks = 0;
        self.cursor = Pba(u64::MAX);
        metrics.gc_defrag_targets_active.store(0, Relaxed);
        metrics.gc_defrag_target_blocks.store(0, Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn track_target_for_exit_test(&mut self, target: Extent) {
        self.active = true;
        self.target_blocks += u64::from(target.count);
        self.targets.insert(
            target.start.0,
            ActiveTarget {
                count: target.count,
                last_free_blocks: 0,
                last_progress: Instant::now(),
            },
        );
    }

    fn cycle_output(&self, metrics: &EngineMetrics) -> DefragCycle {
        use std::sync::atomic::Ordering::Relaxed;
        metrics
            .gc_defrag_targets_active
            .store(self.targets.len() as u64, Relaxed);
        metrics
            .gc_defrag_target_blocks
            .store(self.target_blocks, Relaxed);
        DefragCycle {
            targets: Arc::new(
                self.targets
                    .iter()
                    .map(|(&start, target)| Extent::new(Pba(start), target.count))
                    .collect(),
            ),
            active: true,
        }
    }
}

fn align_up_with_phase(from: u64, stripe: u64, phase: u64) -> u64 {
    let rem = (from + phase) % stripe;
    if rem == 0 {
        from
    } else {
        from + (stripe - rem)
    }
}

impl ClusterAcc {
    fn seed(e: Extent, stripe: u32, phase: u32) -> Self {
        Self {
            lo: e.start.0,
            hi: e.end_pba().0,
            free_blocks: e.count as u64,
            capable_blocks: extent_stripe_capacity(e, stripe, phase),
        }
    }

    fn extend_down(&mut self, e: Extent, stripe: u32, phase: u32) {
        debug_assert!(e.end_pba().0 <= self.lo);
        self.lo = e.start.0;
        self.free_blocks += e.count as u64;
        self.capable_blocks += extent_stripe_capacity(e, stripe, phase);
    }
}

/// Whole-stripe aligned capacity of `e` — mirrors `FreeSet::stripe_floor ∘
/// eff_count`.
fn extent_stripe_capacity(e: Extent, stripe: u32, phase: u32) -> u64 {
    let r = (e.start.0 + phase as u64) % stripe as u64;
    let head = if r == 0 { 0 } else { stripe as u64 - r };
    let eff = (e.count as u64).saturating_sub(head);
    eff / stripe as u64 * stripe as u64
}

/// Binary-search overlap test of a unit's physical footprint against the
/// ascending, non-overlapping target list. Shared with the scanner's
/// per-entry check — O(log T).
pub(crate) fn ranges_overlap_unit(sorted: &[Extent], start: Pba, phys_blocks: u32) -> bool {
    if sorted.is_empty() || phys_blocks == 0 {
        return false;
    }
    let end = start.0 + phys_blocks as u64;
    let idx = sorted.partition_point(|t| t.start.0 <= start.0);
    if idx > 0 && sorted[idx - 1].end_pba().0 > start.0 {
        return true;
    }
    idx < sorted.len() && sorted[idx].start.0 < end
}

/// Publish the allocator contiguity gauges (once per GC cycle).
fn publish_contiguity(metrics: &EngineMetrics, stats: &crate::space::allocator::ContiguityStats) {
    use std::sync::atomic::Ordering::Relaxed;
    metrics
        .allocator_free_extents
        .store(stats.free_extents, Relaxed);
    metrics
        .allocator_largest_free_run
        .store(stats.largest_run_blocks as u64, Relaxed);
    metrics
        .allocator_stripe_capable_blocks
        .store(stats.stripe_capable_blocks.unwrap_or(0), Relaxed);
    metrics
        .allocator_free_blocks_in_set
        .store(stats.free_blocks_in_set, Relaxed);
    metrics
        .allocator_stripe_reserve_blocks
        .store(stats.stripe_reserve_blocks, Relaxed);
    metrics
        .allocator_quarantine_target_blocks
        .store(stats.quarantine_target_blocks, Relaxed);
    metrics
        .allocator_quarantine_free_blocks
        .store(stats.quarantine_free_blocks, Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BLOCK_SIZE, RESERVED_BLOCKS};
    use std::sync::atomic::Ordering::Relaxed;

    const STRIPE: u32 = 6;
    const PHASE: u32 = 2; // aligned starts: (pba+2)%6 == 0

    fn cfg() -> GcConfig {
        GcConfig::default()
    }

    fn new_alloc(blocks: u64) -> SpaceAllocator {
        let a = SpaceAllocator::new(blocks * BLOCK_SIZE as u64, 0);
        a.set_stripe_geometry(STRIPE, PHASE);
        a
    }

    fn metrics() -> EngineMetrics {
        EngineMetrics::default()
    }

    /// Free `free_len`-block runs every `free_len + live_len` blocks within
    /// [lo, hi) of an allocated region — a confetti belt with live pinners.
    fn confetti(a: &SpaceAllocator, lo: u64, hi: u64, free_len: u64, live_len: u64) {
        let mut p = lo;
        while p + free_len <= hi {
            a.free_extent(Extent::new(Pba(p), free_len as u32)).unwrap();
            p += free_len + live_len;
        }
    }

    /// Allocate the whole pool so tests can carve free patterns explicitly.
    fn claim_all(a: &SpaceAllocator, blocks: u64) -> u64 {
        let total = blocks - RESERVED_BLOCKS;
        // Policy classes deliberately split one physical run at stripe
        // boundaries. Claim before enabling geometry so the setup itself does
        // not depend on the allocator's documented short-extent fallback.
        a.set_stripe_geometry(1, 0);
        // No single extent can be wider than one allocator address region, so
        // claim by repeated request; `allocate_extent` hands back the largest
        // available fragment when the exact width is unavailable.
        // The order the fragments come back in is NOT address order: with the
        // exact width unavailable, `allocate_extent` returns the LARGEST available
        // fragment. All that matters here is that nothing is left free.
        let mut claimed = 0u64;
        while claimed < total {
            let extent = a.allocate_extent((total - claimed) as u32).unwrap();
            claimed += u64::from(extent.count);
        }
        assert_eq!(a.free_block_count(), 0, "pool not fully claimed");
        a.set_stripe_geometry(STRIPE, PHASE);
        RESERVED_BLOCKS
    }

    #[test]
    fn trigger_latches_on_confetti_not_on_fresh_pool() {
        let a = new_alloc(4096);
        let base = claim_all(&a, 4096);
        // 5 free / 3 live → density 62.5% (≥50 qualifies) yet zero stripe
        // capability (5 < stripe).
        confetti(&a, base, base + 4088, 5, 3);
        let m = metrics();
        let mut st = DefragState::new();
        let c = st.maintain(&a, &cfg(), 50, &m);
        assert!(c.active, "confetti pool must latch defrag mode");
        assert!(!c.targets.is_empty(), "targets emitted");

        // Fresh single-run pool: stripe-capable ≈ 100% → never latches.
        let fresh = new_alloc(4096);
        let mut st2 = DefragState::new();
        let c2 = st2.maintain(&fresh, &cfg(), 90, &m);
        assert!(!c2.active);
        assert!(c2.targets.is_empty());
    }

    #[test]
    fn min_free_pct_gates_entry() {
        let a = new_alloc(4096);
        let base = claim_all(&a, 4096);
        confetti(&a, base, base + 4088, 3, 5);
        let m = metrics();
        let mut st = DefragState::new();
        let c = st.maintain(&a, &cfg(), 3, &m); // free_pct below the 5 floor
        assert!(!c.active, "space exhaustion belongs to the urgency ladder");
    }

    #[test]
    fn hysteresis_exits_only_past_enter_plus_10() {
        let a = new_alloc(65536);
        let base = claim_all(&a, 65536);
        confetti(&a, base, base + 30_000, 3, 3);
        let m = metrics();
        let mut st = DefragState::new();
        assert!(st.maintain(&a, &cfg(), 50, &m).active);
        // Free a huge aligned run: capability jumps far above enter+10 → exit.
        a.free_extent(Extent::new(Pba(base + 30_100), 30_000))
            .unwrap();
        let c = st.maintain(&a, &cfg(), 50, &m);
        assert!(!c.active, "must exit once stripe-capable again");
        assert!(c.targets.is_empty(), "deactivation clears targets");
    }

    #[test]
    fn clusters_qualify_by_density_and_stripe_capability() {
        let a = new_alloc(65536);
        let base = claim_all(&a, 65536);
        // Low region: dense confetti (free 4 / live 2 → density 66%).
        confetti(&a, base, base + 6000, 4, 2);
        // Mid region: sparse confetti (free 2 / live 30 → density ~6%, reject).
        confetti(&a, base + 10_000, base + 16_000, 2, 30);
        // High region: one big aligned run (hosts stripes → reject).
        a.free_extent(Extent::new(Pba(base + 40_000), 600)).unwrap();

        let m = metrics();
        let mut st = DefragState::new();
        let c = st.maintain(&a, &cfg(), 50, &m);
        assert!(c.active);
        assert!(!c.targets.is_empty(), "dense confetti must be targeted");
        for t in c.targets.iter() {
            assert!(
                t.end_pba().0 <= base + 6000 + 64,
                "target {t:?} escaped the dense region",
            );
        }
    }

    /// A wall-to-wall confetti belt (every gap ≤ gap_max — the REAL fragmented
    /// pool shape that made unbounded clusters swallow everything) must be
    /// split into ≤ MAX_CLUSTER_SPAN windows, each qualifying locally.
    #[test]
    fn belt_is_split_into_bounded_windows() {
        let a = new_alloc(65536);
        let base = claim_all(&a, 65536);
        confetti(&a, base, base + 65_528, 4, 2); // one belt, gaps 2 ≪ gap_max
        let m = metrics();
        let mut st = DefragState::new();
        let mut wide = cfg();
        wide.defrag_max_target_blocks = u64::MAX / 2;
        wide.defrag_max_active_targets = usize::MAX;
        let c = st.maintain(&a, &wide, 50, &m);
        assert!(
            c.targets.len() > 10,
            "belt must yield many bounded windows, got {}",
            c.targets.len()
        );
        for t in c.targets.iter() {
            assert_eq!(t.count, STRIPE, "every quarantine is one exact stripe");
            assert_eq!((t.start.0 + PHASE as u64) % STRIPE as u64, 0);
        }
        for w in c.targets.windows(2) {
            assert!(
                w[0].end_pba().0 <= w[1].start.0,
                "windows must stay disjoint"
            );
        }
    }

    /// A window whose stripe-capable capacity is already high is rejected —
    /// but ONLY that window, not its confetti neighbors (the giant-cluster
    /// failure mode).
    #[test]
    fn capable_window_rejected_without_poisoning_neighbors() {
        let a = new_alloc(65536);
        let base = claim_all(&a, 65536);
        // Low confetti island, then (within gap_max!) a big aligned run, then
        // more confetti — unbounded clustering would merge all three and the
        // big run's capacity would reject everything.
        confetti(&a, base, base + 6000, 4, 2);
        let capable_run = Extent::new(Pba(base + 6010), 1024);
        a.free_extent(capable_run).unwrap();
        confetti(&a, base + 8100, base + 14_100, 4, 2);
        let m = metrics();
        let mut st = DefragState::new();
        let mut wide = cfg();
        wide.defrag_max_active_targets = usize::MAX;
        let c = st.maintain(&a, &wide, 50, &m);
        assert!(c.active);
        let covers_confetti = c.targets.iter().any(|t| t.start.0 < base + 6000)
            && c.targets
                .iter()
                .any(|t| t.start.0 >= base + 8100 && t.start.0 < base + 14_100);
        assert!(
            covers_confetti,
            "both confetti islands must be targeted: {:?}",
            c.targets
        );
        let reserve_start = align_up_with_phase(capable_run.start.0, STRIPE as u64, PHASE as u64);
        let reserve_end = reserve_start
            + capable_run.end_pba().0.saturating_sub(reserve_start) / STRIPE as u64 * STRIPE as u64;
        for t in c.targets.iter() {
            assert!(
                t.end_pba().0 <= reserve_start || t.start.0 >= reserve_end,
                "target {t:?} must not overlap the already-capable reserve"
            );
        }
    }

    #[test]
    fn pure_free_clusters_are_not_targets() {
        let a = new_alloc(8192);
        let base = claim_all(&a, 8192);
        // One isolated misaligned free run — free but sub-stripe, no interior
        // pinners: nothing to evacuate, must be rejected.
        a.free_extent(Extent::new(Pba(base + 100), 3)).unwrap();
        let m = metrics();
        let mut st = DefragState::new();
        let c = st.maintain(&a, &cfg(), 50, &m);
        // (trigger may latch — capability is 0 — but no targets emitted)
        assert!(
            c.targets.is_empty(),
            "a bare free extent has no pinners to move"
        );
    }

    #[test]
    fn walk_is_descending_resumes_and_stays_disjoint() {
        let a = new_alloc(65536);
        let base = claim_all(&a, 65536);
        // Confetti islands separated by >gap_max live runs so clusters close
        // mid-walk: 40 islands of ~300 blocks, 200-block live separators.
        let mut lo = base;
        for _ in 0..40 {
            confetti(&a, lo, lo + 300, 3, 3);
            lo += 500;
        }
        let m = metrics();
        let mut st = DefragState::new();
        let mut small = cfg();
        small.defrag_scan_extents_per_cycle = 512; // force multi-cycle walk
        small.defrag_max_active_targets = usize::MAX;
        let c1 = st.maintain(&a, &small, 50, &m);
        let low1 = c1.targets.first().map(|t| t.start.0).unwrap_or(u64::MAX);
        let c2 = st.maintain(&a, &small, 50, &m);
        let low2 = c2.targets.first().map(|t| t.start.0).unwrap_or(u64::MAX);
        assert!(
            c2.targets.len() > c1.targets.len(),
            "walk must make progress"
        );
        assert!(low2 < low1, "later cycles reach lower addresses");
        for w in c2.targets.windows(2) {
            assert!(
                w[0].end_pba().0 <= w[1].start.0,
                "targets must stay disjoint"
            );
        }
    }

    #[test]
    fn span_budget_caps_targets() {
        let a = new_alloc(65536);
        let base = claim_all(&a, 65536);
        confetti(&a, base, base + 65_528, 4, 2);
        let m = metrics();
        let mut st = DefragState::new();
        let mut capped = cfg();
        capped.defrag_max_target_blocks = 1000;
        let c = st.maintain(&a, &capped, 50, &m);
        let span: u64 = c.targets.iter().map(|t| t.count as u64).sum();
        assert!(span <= 1000, "span {span} exceeds budget");
        assert!(span > 0);
        // Budget full → the next cycle's walk emits nothing new.
        let c2 = st.maintain(&a, &capped, 50, &m);
        let span2: u64 = c2.targets.iter().map(|t| t.count as u64).sum();
        assert!(span2 <= 1000);
    }

    #[test]
    fn target_completes_only_at_one_hundred_percent_free() {
        let a = new_alloc(8192);
        let base = claim_all(&a, 8192);
        confetti(&a, base, base + 8184, 3, 3);
        let m = metrics();
        let mut st = DefragState::new();
        let c = st.maintain(&a, &cfg(), 50, &m);
        assert!(c.active && !c.targets.is_empty());
        // Free all but one live PBA. Partial physical ownership must never be
        // published, even though the stripe is mostly free.
        let t0 = *c.targets.last().unwrap();
        let live: Vec<Pba> = (0..t0.count)
            .map(|offset| Pba(t0.start.0 + u64::from(offset)))
            .filter(|pba| !a.is_free(*pba))
            .collect();
        assert!(!live.is_empty());
        for pba in live.iter().take(live.len() - 1) {
            a.free_one(*pba).unwrap();
        }
        let c2 = st.maintain(&a, &cfg(), 50, &m);
        assert!(c2.targets.iter().any(|target| *target == t0));
        assert_eq!(
            m.gc_defrag_segments_completed.load(Relaxed),
            0,
            "partial stripe must remain quarantined"
        );

        a.free_one(*live.last().unwrap()).unwrap();
        let c3 = st.maintain(&a, &cfg(), 50, &m);
        for t in c3.targets.iter() {
            assert!(
                t.end_pba().0 <= t0.start.0 || t.start.0 >= t0.end_pba().0,
                "completed target must leave the active map"
            );
        }
        assert_eq!(m.gc_defrag_segments_completed.load(Relaxed), 1);
        assert!(a.contiguity_stats().stripe_reserve_blocks >= STRIPE as u64);
    }

    #[test]
    fn active_target_count_is_hard_bounded() {
        let a = new_alloc(65_536);
        let base = claim_all(&a, 65_536);
        confetti(&a, base, base + 65_528, 4, 2);
        let m = metrics();
        let mut st = DefragState::new();
        let mut capped = cfg();
        capped.defrag_max_active_targets = 3;
        capped.defrag_max_target_blocks = u64::MAX / 2;

        let cycle = st.maintain(&a, &capped, 50, &m);
        assert_eq!(cycle.targets.len(), 3);
        assert_eq!(m.gc_defrag_targets_active.load(Relaxed), 3);
    }

    #[test]
    fn stalled_target_is_cancelled_without_losing_free_blocks() {
        let a = new_alloc(8192);
        let base = claim_all(&a, 8192);
        confetti(&a, base, base + 8184, 3, 3);
        let m = metrics();
        let mut st = DefragState::new();
        let mut short_watchdog = cfg();
        short_watchdog.defrag_target_stall_secs = 1;
        let first = st.maintain(&a, &short_watchdog, 50, &m);
        let target = *first.targets.last().expect("target selected");
        let free_before = a.free_block_count();
        st.targets.get_mut(&target.start.0).unwrap().last_progress =
            Instant::now() - Duration::from_secs(2);
        short_watchdog.defrag_max_target_blocks = 0;

        let _ = st.maintain(&a, &short_watchdog, 50, &m);
        assert_eq!(m.gc_defrag_segments_cancelled.load(Relaxed), 1);
        assert!(!a.is_defrag_quarantined(target));
        assert_eq!(a.free_block_count(), free_before);
    }

    #[test]
    fn ranges_overlap_unit_binary_search() {
        let targets = vec![Extent::new(Pba(100), 50), Extent::new(Pba(300), 10)];
        assert!(ranges_overlap_unit(&targets, Pba(100), 1));
        assert!(ranges_overlap_unit(&targets, Pba(149), 1));
        assert!(!ranges_overlap_unit(&targets, Pba(150), 1));
        assert!(ranges_overlap_unit(&targets, Pba(95), 6), "tail reaches in");
        assert!(!ranges_overlap_unit(&targets, Pba(95), 5));
        assert!(ranges_overlap_unit(&targets, Pba(299), 2));
        assert!(!ranges_overlap_unit(&targets, Pba(310), 100));
        assert!(!ranges_overlap_unit(&[], Pba(100), 1));
        assert!(!ranges_overlap_unit(&targets, Pba(100), 0));
    }

    #[test]
    fn disable_or_no_geometry_deactivates() {
        let a = new_alloc(4096);
        let base = claim_all(&a, 4096);
        confetti(&a, base, base + 4088, 3, 5);
        let m = metrics();
        let mut st = DefragState::new();
        let active = st.maintain(&a, &cfg(), 50, &m);
        let target = *active.targets.first().expect("defrag target selected");
        let free_before = a.free_block_count();
        assert!(a.is_defrag_quarantined(target));

        let mut off = cfg();
        off.defrag_enabled = false;
        assert!(!st.maintain(&a, &off, 50, &m).active);
        assert!(!a.is_defrag_quarantined(target));
        assert_eq!(a.free_block_count(), free_before);
        assert!(m.gc_defrag_segments_cancelled.load(Relaxed) > 0);

        // No geometry (stripe=1 backend): never activates.
        let b = SpaceAllocator::new(4096 * BLOCK_SIZE as u64, 0);
        let base_b = b
            .allocate_extent((4096 - RESERVED_BLOCKS) as u32)
            .unwrap()
            .start
            .0;
        confetti(&b, base_b, base_b + 4088, 3, 5);
        let mut st_b = DefragState::new();
        assert!(!st_b.maintain(&b, &cfg(), 50, &m).active);
    }
}
