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
//! Target lifecycle: emit (cluster qualifies, span budget permitting) →
//! evacuate (scanner/rewriter drain the live pinners over L2P laps) → done
//! (range re-checked ≥`TARGET_DONE_PCT` free+retired → retired from the map,
//! budget handed back, walk continues) → walk lap wraps to the device top
//! (already-targeted or now-stripe-capable regions skip/re-qualify
//! idempotently). Stale targets are harmless: the scanner just finds nothing
//! there and the rewriter re-validates every LBA against the live L2P.

use std::collections::BTreeMap;
use std::sync::Arc;

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
/// A target is "done" (evacuated enough to retire from the active map) when
/// free+retired cover at least this share of its span. Not a config knob:
/// it only controls when budget is handed back to the walk, never safety.
const TARGET_DONE_PCT: u64 = 90;

/// One cluster being accumulated across chunk/cycle boundaries. The walk is
/// descending, so the cluster grows downward: `lo` falls, `hi` is fixed at
/// the top member's end.
struct ClusterAcc {
    lo: u64,
    hi: u64,
    free_blocks: u64,
    /// Any member already hosts an aligned stripe carve → the region is
    /// usable as-is; evacuating it wastes movement.
    has_stripe_host: bool,
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
        Self { targets: Arc::new(Vec::new()), active: false }
    }
}

pub(crate) struct DefragState {
    active: bool,
    /// Next walk visits free extents strictly below this address.
    cursor: Pba,
    pending: Option<ClusterAcc>,
    /// Active targets: start → span blocks. Ascending & non-overlapping by
    /// construction (insertions skip anything overlapping a neighbor).
    targets: BTreeMap<u64, u32>,
    /// Σ span over `targets` (the budget).
    target_blocks: u64,
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
            self.deactivate(metrics);
        } else if self.active {
            if capable_pct >= enter + 10 {
                self.deactivate(metrics);
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
        metrics.gc_defrag_mode_active.store(self.active as u64, Relaxed);
        if !self.active {
            return DefragCycle::inactive();
        }
        let (stripe, phase) = geom.expect("active implies geometry");

        // Retire evacuated targets: once free+retired cover ≥ TARGET_DONE_PCT
        // of the span, reclaim/coalescing owns the rest — hand the budget back.
        let done: Vec<u64> = self
            .targets
            .iter()
            .filter(|&(&start, &count)| {
                let t = Extent::new(Pba(start), count);
                let covered =
                    allocator.free_overlap_blocks(t) + allocator.retired_overlap_blocks(t);
                covered * 100 >= count as u64 * TARGET_DONE_PCT
            })
            .map(|(&start, _)| start)
            .collect();
        for start in done {
            let count = self.targets.remove(&start).expect("collected above");
            self.target_blocks -= count as u64;
            tracing::debug!(start, blocks = count, "defrag target done (evacuated)");
        }

        // Continue the descending walk while span budget remains.
        let mut walked = 0usize;
        while walked < cfg.defrag_scan_extents_per_cycle
            && self.target_blocks < cfg.defrag_max_target_blocks
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
                if self.target_blocks >= cfg.defrag_max_target_blocks {
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
                            && acc.hi - e.start.0 <= u32::MAX as u64
                        {
                            acc.extend_down(*e, stripe, phase);
                            None
                        } else {
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
        metrics.gc_defrag_walk_extents.fetch_add(walked as u64, Relaxed);
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
        // Reject: already usable (hosts an aligned stripe) — nothing to fix;
        // or pure free space with no interior pinners — nothing to evacuate.
        if acc.has_stripe_host || acc.free_blocks >= span {
            metrics.gc_defrag_clusters_rejected.fetch_add(1, Relaxed);
            return;
        }
        // Density gate: (free + retired) / span. The retired query takes only
        // the retired lock; no free lock is held here (snapshots released).
        let span_ext = Extent::new(Pba(acc.lo), span as u32);
        let reclaimable = acc.free_blocks + allocator.retired_overlap_blocks(span_ext);
        if reclaimable * 100 < span * cfg.defrag_min_free_density_pct as u64 {
            metrics.gc_defrag_clusters_rejected.fetch_add(1, Relaxed);
            return;
        }
        // Clamp to the remaining span budget, keeping the TOP of the cluster
        // (highest addresses evacuate first).
        let budget_left = cfg.defrag_max_target_blocks - self.target_blocks;
        let take = span.min(budget_left);
        if take == 0 {
            return;
        }
        let target = Extent::new(Pba(acc.hi - take), take as u32);
        // Skip anything overlapping an existing target (re-walks of a
        // still-active target re-qualify here) — keeps the map disjoint and
        // the budget accounting exact.
        if self.overlaps_existing(target) {
            return;
        }
        self.targets.insert(target.start.0, target.count);
        self.target_blocks += take;
        metrics.gc_defrag_clusters_qualified.fetch_add(1, Relaxed);
    }

    fn overlaps_existing(&self, t: Extent) -> bool {
        if let Some((&s, &c)) = self.targets.range(..=t.start.0).next_back() {
            if s + c as u64 > t.start.0 {
                return true;
            }
        }
        self.targets
            .range(t.start.0..)
            .next()
            .is_some_and(|(&s, _)| s < t.end_pba().0)
    }

    fn deactivate(&mut self, metrics: &EngineMetrics) {
        use std::sync::atomic::Ordering::Relaxed;
        if self.active {
            tracing::info!("defrag mode EXIT: free pool stripe-capable again");
        }
        self.active = false;
        self.pending = None;
        self.targets.clear();
        self.target_blocks = 0;
        self.cursor = Pba(u64::MAX);
        metrics.gc_defrag_targets_active.store(0, Relaxed);
        metrics.gc_defrag_target_blocks.store(0, Relaxed);
    }

    fn cycle_output(&self, metrics: &EngineMetrics) -> DefragCycle {
        use std::sync::atomic::Ordering::Relaxed;
        metrics
            .gc_defrag_targets_active
            .store(self.targets.len() as u64, Relaxed);
        metrics.gc_defrag_target_blocks.store(self.target_blocks, Relaxed);
        DefragCycle {
            targets: Arc::new(
                self.targets
                    .iter()
                    .map(|(&start, &count)| Extent::new(Pba(start), count))
                    .collect(),
            ),
            active: true,
        }
    }
}

impl ClusterAcc {
    fn seed(e: Extent, stripe: u32, phase: u32) -> Self {
        Self {
            lo: e.start.0,
            hi: e.end_pba().0,
            free_blocks: e.count as u64,
            has_stripe_host: extent_hosts_stripe(e, stripe, phase),
        }
    }

    fn extend_down(&mut self, e: Extent, stripe: u32, phase: u32) {
        debug_assert!(e.end_pba().0 <= self.lo);
        self.lo = e.start.0;
        self.free_blocks += e.count as u64;
        self.has_stripe_host |= extent_hosts_stripe(e, stripe, phase);
    }
}

/// Can `e` host one aligned stripe carve? Mirrors `FreeSet::eff_count >= stripe`.
fn extent_hosts_stripe(e: Extent, stripe: u32, phase: u32) -> bool {
    let r = (e.start.0 + phase as u64) % stripe as u64;
    let head = if r == 0 { 0 } else { stripe as u64 - r };
    (e.count as u64) >= head + stripe as u64
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
    metrics.allocator_free_extents.store(stats.free_extents, Relaxed);
    metrics
        .allocator_largest_free_run
        .store(stats.largest_run_blocks as u64, Relaxed);
    metrics
        .allocator_stripe_capable_blocks
        .store(stats.stripe_capable_blocks.unwrap_or(0), Relaxed);
    metrics
        .allocator_free_blocks_in_set
        .store(stats.free_blocks_in_set, Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BLOCK_SIZE, RESERVED_BLOCKS};

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
        a.allocate_extent(total as u32).unwrap().start.0
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
        a.free_extent(Extent::new(Pba(base + 30_100), 30_000)).unwrap();
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
                "target {t:?} escaped the dense region"
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
        assert!(c.targets.is_empty(), "a bare free extent has no pinners to move");
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
        let c1 = st.maintain(&a, &small, 50, &m);
        let low1 = c1.targets.first().map(|t| t.start.0).unwrap_or(u64::MAX);
        let c2 = st.maintain(&a, &small, 50, &m);
        let low2 = c2.targets.first().map(|t| t.start.0).unwrap_or(u64::MAX);
        assert!(c2.targets.len() > c1.targets.len(), "walk must make progress");
        assert!(low2 < low1, "later cycles reach lower addresses");
        for w in c2.targets.windows(2) {
            assert!(w[0].end_pba().0 <= w[1].start.0, "targets must stay disjoint");
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
    fn done_targets_are_retired_and_budget_returns() {
        let a = new_alloc(8192);
        let base = claim_all(&a, 8192);
        confetti(&a, base, base + 8184, 3, 3);
        let m = metrics();
        let mut st = DefragState::new();
        let c = st.maintain(&a, &cfg(), 50, &m);
        assert!(c.active && !c.targets.is_empty());
        // "Evacuate" one target by freeing its live gaps → ≥90% free.
        let t0 = *c.targets.last().unwrap();
        let mut p = t0.start.0;
        while p < t0.end_pba().0 {
            let _ = a.free_extent(Extent::single(Pba(p)));
            p += 1;
        }
        let c2 = st.maintain(&a, &cfg(), 50, &m);
        for t in c2.targets.iter() {
            assert!(
                t.end_pba().0 <= t0.start.0 || t.start.0 >= t0.end_pba().0,
                "evacuated target must be retired from the active map"
            );
        }
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
        let mut off = cfg();
        off.defrag_enabled = false;
        assert!(!st.maintain(&a, &off, 50, &m).active);

        // No geometry (stripe=1 backend): never activates.
        let b = SpaceAllocator::new(4096 * BLOCK_SIZE as u64, 0);
        let base_b = claim_all(&b, 4096);
        confetti(&b, base_b, base_b + 4088, 3, 5);
        let mut st_b = DefragState::new();
        assert!(!st_b.maintain(&b, &cfg(), 50, &m).active);
    }
}
