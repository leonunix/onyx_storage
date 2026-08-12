//! Defrag target selection — the physical-neighborhood compaction lever.
//!
//! On a fragmented pool the free list shatters into millions of sub-stripe
//! extents ("confetti") pinned in place by interleaved live blocks. The
//! per-unit dead-ratio compactor can never see this: dead_ratio is a property
//! INSIDE one compression unit, while defrag value is a property of the
//! physical neighborhood — how few live blocks pin how much free space.
//!
//! ## TWO selectors, split by whether a window has a LIVE pinner
//!
//! A stripe window that is not stripe-capable is pinned by one of two things,
//! and they need completely different machinery:
//!
//! 1. **Retired-pinned** — the window's non-free remainder is entirely
//!    RETIRED. Nothing has to be rewritten: reclaim alone will free it and
//!    `insert_classified` folds the whole stripe back into the reserve. All the
//!    defragger has to do is hold the window's free fragments OUT of allocation
//!    until that happens, or the foreground consumes them first and the window
//!    never completes. Discovering these needs no L2P scan at all — the retired
//!    set itself is the index ([`SpaceAllocator::retired_stripe_windows`]) — so
//!    this half runs on its own resident thread ([`super::defrag_runner`]) at
//!    its own cadence, and is pure allocator-side work:
//!    classify → quarantine → wait for reclaim → publish into the reserve.
//! 2. **Live-pinned** — a few live blocks pin the window forever, because
//!    folding only ever happens at free time (memory:
//!    `stripe_reserve_pinned_window_mechanism`). These need the pinners
//!    relocated, which needs a PBA → LBA answer, which only the compactor's
//!    forward L2P scan can give. That half stays SCAN-DRIVEN inside the
//!    compactor (below), now downshifted to one pass every
//!    `defrag_scan_interval_cycles` cycles with its own timebox: it is the
//!    expensive, optional optimiser, and it must never stretch the GC cycle
//!    that mandatory reclaim shares (memory:
//!    `gc_reclaim_defrag_criticality_inversion` — defrag stretching the cycle
//!    8.6 → 13.7 s cost 23.7 GiB of unallocatable `retired_depth`, i.e. defrag
//!    starved its own dependency).
//!
//! Reclaim is the PRIMARY contiguity producer; defrag is the optimiser that
//! finishes what reclaim cannot reach. The thread split is what keeps that
//! priority order structural instead of a matter of tuning.
//!
//! ## The live-pinned half is SCAN-DRIVEN, and the direction is the whole design
//!
//! There is no PBA → LBA reverse index anywhere in onyx: `referenced_extents`
//! is itself a full L2P scan, and under `rc_authoritative_reclaim` the reclaim
//! gate skips even that. The ONLY reverse map is the compactor's bounded L2P
//! window scan, streaming `(lba, pba)` pairs. So the "is this window worth
//! clearing" decision is made **on that scan**: the runner hands us every live
//! PBA the current window touched, we classify those PBAs' stripe windows
//! against the allocator, quarantine the best ones, and the very same scan pass
//! then promotes their live fragments to rewrite candidates.
//!
//! The pre-2026-08-06 design did the opposite: it walked the free list
//! DESCENDING, pre-picked ≤ `defrag_max_active_targets` (32) single-stripe
//! targets, and hoped a later scan lap would stumble on their live pinners.
//! That is a reverse lookup served by a full forward scan, and it measured as
//! literally zero work over a 480 s box window (`targets=32 candidates=0
//! completed=0` while `largest_run` fell 68340 → 42). Three reasons, all
//! structural: the 32-target count cap made the working set 192 blocks
//! (768 KiB) against a 1 GiB block budget; ranking candidate windows by
//! `free + retired` DESCENDING preferentially picked windows with the FEWEST
//! live pinners, i.e. the ones GC cannot help at all; and discovery latency was
//! one full L2P lap (11–37 min at defrag effort on a 256 GiB volume).
//!
//! Scan-driven selection fixes all three by construction rather than by tuning:
//! every candidate window is reached VIA a live mapping, so it always has at
//! least one live pinner to evacuate; discovery is zero-marginal-cost because
//! the scan runs anyway; and the working set is bounded by the relocation block
//! budget instead of an arbitrary slot count.
//!
//! Target lifecycle is unchanged: quarantine the window's free fragments →
//! evacuate its live references through the ordinary rewrite path → wait for
//! retire/reclaim to make the whole stripe free → publish it back atomically as
//! stripe reserve. Quarantine is an allocation policy state, not persistent
//! metadata: after a crash the folded L2P/dedup index rebuild remains
//! authoritative and merely loses cleaner progress. Allocation policy itself is
//! NEVER touched (dense-PBA metadb leaf contract).

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::metrics::EngineMetrics;
use crate::space::allocator::SpaceAllocator;
use crate::space::extent::Extent;
use crate::types::Pba;

use super::config::GcConfig;

/// Windows handed to `SpaceAllocator::classify_stripe_windows` per batch. Each
/// batch is one region-grouped lock pass, so this bounds a single hold; the
/// selector keeps consuming batches until its block budget is met.
const CLASSIFY_CHUNK: usize = 4096;

/// Upper bound on windows classified in ONE cycle.
///
/// ⚠ This is a bound on the SCAN, not an admission cap on qualified work — the
/// distinction matters (`coalesce_admission_cap_64_is_the_drain_wall`: an
/// arbitrary per-cycle count cap on admitted work becomes the next wall). The
/// selector always stops early once its relocation block budget is met, so this
/// only fires in the degenerate case where the trigger is latched yet almost
/// nothing qualifies (e.g. a wall-to-wall live region); without it a 1M-LBA
/// scan window would classify ~1M windows for zero selections.
const CLASSIFY_MAX_PER_CYCLE: usize = 65_536;

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
    /// When the trigger latch was last evaluated. The evaluation needs
    /// `contiguity_stats`, which is O(address regions) LOCKS (2048 by default),
    /// so it runs on `defrag_trigger_interval_ms` rather than on the resident
    /// thread's much faster selection cadence — see that knob's doc.
    last_trigger_eval: Option<Instant>,
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
            last_trigger_eval: None,
            targets: BTreeMap::new(),
            target_blocks: 0,
        }
    }

    /// Run one defrag maintenance step: retire "done" targets, cancel stalled
    /// ones, and evaluate the trigger latch. Called once per GC cycle from the
    /// gc-runner thread (single writer) BEFORE the compactor picks its scan
    /// window; [`Self::select_from_scan`] then does the actual selection.
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
        let mut stale = Vec::new();
        for (&start, target) in self.targets.iter_mut() {
            let pba = Pba(start);
            let Some((free_blocks, total_blocks)) = allocator.defrag_quarantine_progress(pba)
            else {
                // Allocator state disappeared without going through our own
                // completion or cancellation. Drop the stale scanner range
                // defensively — but do NOT count it as a completion: that
                // would inflate the one counter that says "defrag published a
                // stripe to the reserve".
                stale.push(start);
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
        // Per-target logging here is PER PUBLISHED STRIPE, i.e. once per
        // `stripe` blocks of reclaimed contiguity — at box rates that was
        // 43,855 INFO lines in 17 minutes, which drowns the log the operator is
        // actually reading. The counters (`gc_defrag_segments_*`) are the
        // interface; keep only a per-cycle roll-up at INFO and the individual
        // starts at trace for forensics.
        let mut published_stripes = 0u64;
        let mut published_blocks = 0u64;
        for start in completed {
            if let Some(blocks) = self.forget_target(start) {
                metrics.gc_defrag_segments_completed.fetch_add(1, Relaxed);
                published_stripes += 1;
                published_blocks += u64::from(blocks);
                tracing::trace!(start, blocks, "defrag stripe published to reserve");
            }
        }
        if published_stripes > 0 {
            tracing::info!(
                stripes = published_stripes,
                blocks = published_blocks,
                targets = self.targets.len(),
                "defrag published stripes to reserve"
            );
        }
        let mut cancelled_targets = 0u64;
        for start in cancelled {
            if let Some(blocks) = self.forget_target(start) {
                metrics.gc_defrag_segments_cancelled.fetch_add(1, Relaxed);
                cancelled_targets += 1;
                tracing::debug!(start, blocks, "defrag target cancelled after no progress");
            }
        }
        if cancelled_targets > 0 {
            tracing::warn!(
                targets = cancelled_targets,
                stall_secs = cfg.defrag_target_stall_secs,
                "defrag targets cancelled after no progress"
            );
        }
        for start in stale {
            if self.forget_target(start).is_some() {
                metrics.gc_defrag_targets_stale.fetch_add(1, Relaxed);
                tracing::warn!(start, "defrag target vanished from the allocator");
            }
        }

        // Cheap gates run EVERY cycle: they read atomics only, and a hot reload of
        // `defrag_enabled = false` or a slide below the free floor must release
        // every quarantine promptly rather than at the trigger cadence.
        let geom = allocator.stripe_geometry();
        if !cfg.defrag_enabled || geom.is_none() || free_pct < cfg.defrag_min_free_pct as u64 {
            self.deactivate(allocator, metrics);
            metrics.gc_defrag_mode_active.store(0, Relaxed);
            return DefragCycle::inactive();
        }

        // The latch itself needs `contiguity_stats`, which is one lock per address
        // region (2048 at the default `storage.allocator_regions`) — so it is on
        // its own slower clock. See `defrag_trigger_interval_ms`.
        let trigger_due = self.last_trigger_eval.is_none_or(|last| {
            now.duration_since(last) >= Duration::from_millis(cfg.defrag_trigger_interval_ms)
        });
        if trigger_due {
            self.last_trigger_eval = Some(now);
            let stats = allocator.contiguity_stats();
            publish_contiguity(metrics, &stats);
            let capable_pct = match (stats.stripe_capable_blocks, stats.free_blocks_in_set) {
                (Some(cap), total) if total > 0 => cap * 100 / total,
                _ => 100, // no geometry / empty set: defrag is meaningless
            };
            // Trigger latch with +10 pct exit hysteresis.
            let enter = cfg.defrag_stripe_capable_min_pct as u64;
            if self.active {
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
        }
        metrics
            .gc_defrag_mode_active
            .store(self.active as u64, Relaxed);
        if !self.active {
            return DefragCycle::inactive();
        }
        self.cycle_output(metrics)
    }

    /// Scan-driven target selection.
    ///
    /// `window_pbas` is every live mapping's physical footprint seen in the
    /// compactor's current LBA window — unsorted, duplicates fine, consumed in
    /// place. `live_block_budget` is Σ live blocks we are willing to relocate
    /// this cycle (the caller derives it from `defrag_max_rewrite_blocks_per_cycle`
    /// scaled by effort, i.e. the same budget that bounds the rewrite loop).
    ///
    /// Every window reached this way contains at least one live mapping by
    /// construction, so — unlike the old free-list walk — a selected target
    /// always has something for the rewriter to move. Windows are ranked by
    /// fewest live pinners first (best contiguity unlocked per block relocated),
    /// ties broken by descending address so evacuation still tends to run from
    /// the top while first-fit allocation refills from the bottom.
    ///
    /// Returns the cycle's target list: newly selected windows PLUS any
    /// carried-over targets whose pinners were not fully evacuated yet (the
    /// latter cost the scanner only an `O(log T)` overlap test each).
    ///
    /// `deadline` timeboxes the classify/quarantine loop. It runs on the
    /// gc-runner thread, which mandatory reclaim shares, so it must be able to
    /// give the cycle back mid-batch (memory:
    /// `gc_reclaim_defrag_criticality_inversion`).
    pub(crate) fn select_from_scan(
        &mut self,
        allocator: &SpaceAllocator,
        cfg: &GcConfig,
        window_pbas: &mut Vec<u64>,
        live_block_budget: u64,
        deadline: Option<Instant>,
        metrics: &EngineMetrics,
    ) -> DefragCycle {
        use std::sync::atomic::Ordering::Relaxed;
        if !self.active {
            return DefragCycle::inactive();
        }
        let Some((stripe, phase)) = allocator.stripe_geometry() else {
            return self.cycle_output(metrics);
        };
        if stripe <= 1 || live_block_budget == 0 || window_pbas.is_empty() {
            return self.cycle_output(metrics);
        }
        let stripe_u64 = u64::from(stripe);

        // PBAs -> distinct stripe-aligned window starts, ascending (ascending
        // input is what lets `classify_stripe_windows` group by region). A PBA
        // below the first aligned start on the grid has no whole window to
        // clear and drops out.
        window_pbas.retain_mut(
            |pba| match window_start(*pba, stripe_u64, u64::from(phase)) {
                Some(start) => {
                    *pba = start;
                    true
                }
                None => false,
            },
        );
        window_pbas.sort_unstable();
        window_pbas.dedup();
        if window_pbas.is_empty() {
            return self.cycle_output(metrics);
        }

        let mut classified = 0usize;
        let mut qualified = 0u64;
        let mut rejected = 0u64;
        let mut selected_live = 0u64;

        let mut timed_out = false;
        for chunk in window_pbas.chunks(CLASSIFY_CHUNK) {
            if selected_live >= live_block_budget || classified >= CLASSIFY_MAX_PER_CYCLE {
                break;
            }
            if deadline.is_some_and(|d| Instant::now() >= d) {
                timed_out = true;
                break;
            }
            // Windows already quarantined are neither classifiable nor
            // re-selectable; drop them before paying for the lock pass.
            let fresh: Vec<u64> = chunk
                .iter()
                .copied()
                .filter(|start| !self.targets.contains_key(start))
                .collect();
            if fresh.is_empty() {
                continue;
            }
            classified += fresh.len();
            let occupancy = allocator.classify_stripe_windows(&fresh, stripe);

            // Qualify: enough of the window is already free-or-reclaimable that
            // evacuating the rest buys real contiguity, and there IS a live
            // pinner to evacuate. `live == 0` windows need no rewrite at all —
            // reclaim alone will fold them back through `insert_classified` —
            // so spending a quarantine on them is exactly the mistake the old
            // descending-`reclaimable` ranking made.
            let mut ranked: Vec<(u32, Reverse<u64>)> = Vec::new();
            for (&start, &(free, retired)) in fresh.iter().zip(occupancy.iter()) {
                let reclaimable = u64::from(free) + u64::from(retired);
                let live = stripe_u64.saturating_sub(reclaimable);
                // `live == 0` also covers the already-free window (which lives
                // in the stripe reserve and would be rejected by
                // `begin_defrag_quarantine` anyway).
                if live == 0
                    || reclaimable * 100 < stripe_u64 * u64::from(cfg.defrag_min_free_density_pct)
                {
                    rejected += 1;
                    continue;
                }
                ranked.push((live as u32, Reverse(start)));
            }
            // Fewest pinners first = most contiguity unlocked per block moved.
            ranked.sort_unstable();

            for (live, Reverse(start)) in ranked {
                if selected_live >= live_block_budget
                    || self.target_blocks + stripe_u64 > cfg.defrag_max_target_blocks
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
                        selected_live += u64::from(live);
                        qualified += 1;
                    }
                    Err(error) => {
                        rejected += 1;
                        tracing::debug!(start, error = %error, "defrag quarantine rejected");
                    }
                }
            }
        }

        metrics
            .gc_defrag_windows_classified
            .fetch_add(classified as u64, Relaxed);
        if qualified > 0 {
            metrics
                .gc_defrag_clusters_qualified
                .fetch_add(qualified, Relaxed);
        }
        if rejected > 0 {
            metrics
                .gc_defrag_clusters_rejected
                .fetch_add(rejected, Relaxed);
        }
        if timed_out {
            metrics.gc_defrag_scan_timeboxed.fetch_add(1, Relaxed);
        }
        self.cycle_output(metrics)
    }

    /// Allocator-side target selection — the resident defragger's half.
    ///
    /// `windows` are stripe-aligned window starts that contain at least one
    /// RETIRED block, straight from [`SpaceAllocator::retired_stripe_windows`]
    /// (ascending, deduplicated). We keep only the ones whose ENTIRE remainder
    /// is free-or-retired: those have no live pinner, so reclaim finishes them
    /// on its own and the quarantine's only job is to stop the foreground from
    /// eating their free fragments in the meantime. No L2P scan, no rewrite, no
    /// dependency on the compactor's lap.
    ///
    /// Windows are taken in ADDRESS order rather than ranked. There is nothing
    /// to rank by: every candidate costs exactly one quarantine and zero
    /// relocated blocks, and the only real cost — `free` blocks held out of
    /// allocation until reclaim lands — is bounded globally by
    /// `defrag_max_target_blocks`, not per window. Address order also keeps
    /// evacuation and first-fit refill from chasing each other.
    ///
    /// Returns the number of windows newly quarantined.
    pub(crate) fn select_from_allocator(
        &mut self,
        allocator: &SpaceAllocator,
        cfg: &GcConfig,
        windows: &[u64],
        metrics: &EngineMetrics,
    ) -> u64 {
        use std::sync::atomic::Ordering::Relaxed;
        if !self.active || windows.is_empty() {
            return 0;
        }
        let Some((stripe, _)) = allocator.stripe_geometry() else {
            return 0;
        };
        if stripe <= 1 {
            return 0;
        }
        let stripe_u64 = u64::from(stripe);

        let mut classified = 0usize;
        let mut qualified = 0u64;
        let mut rejected = 0u64;

        'outer: for chunk in windows.chunks(CLASSIFY_CHUNK) {
            if self.target_blocks + stripe_u64 > cfg.defrag_max_target_blocks {
                break;
            }
            // Already quarantined: not classifiable, not re-selectable.
            let fresh: Vec<u64> = chunk
                .iter()
                .copied()
                .filter(|start| !self.targets.contains_key(start))
                .collect();
            if fresh.is_empty() {
                continue;
            }
            classified += fresh.len();
            let occupancy = allocator.classify_stripe_windows(&fresh, stripe);

            for (&start, &(free, retired)) in fresh.iter().zip(occupancy.iter()) {
                if self.target_blocks + stripe_u64 > cfg.defrag_max_target_blocks {
                    break 'outer;
                }
                // `retired == 0` means the window either folded already
                // (whole-free runs go to the reserve at free time) or is pinned
                // by LIVE data, which is the scan-driven selector's job — a
                // quarantine here would just park free blocks behind the stall
                // watchdog for a pinner nobody is going to move.
                //
                // A partial remainder (`free + retired < stripe`) is live-pinned
                // for the same reason.
                if retired == 0 || u64::from(free) + u64::from(retired) != stripe_u64 {
                    rejected += 1;
                    continue;
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
                        qualified += 1;
                    }
                    Err(error) => {
                        rejected += 1;
                        tracing::debug!(start, error = %error, "defrag quarantine rejected");
                    }
                }
            }
        }

        metrics
            .gc_defrag_windows_classified
            .fetch_add(classified as u64, Relaxed);
        if qualified > 0 {
            metrics
                .gc_defrag_clusters_qualified
                .fetch_add(qualified, Relaxed);
            metrics
                .gc_defrag_retired_windows_selected
                .fetch_add(qualified, Relaxed);
        }
        if rejected > 0 {
            metrics
                .gc_defrag_clusters_rejected
                .fetch_add(rejected, Relaxed);
        }
        qualified
    }

    /// Drop a target from the active map, returning its span in blocks.
    fn forget_target(&mut self, start: u64) -> Option<u32> {
        let target = self.targets.remove(&start)?;
        self.target_blocks -= u64::from(target.count);
        Some(target.count)
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
        self.targets.clear();
        self.target_blocks = 0;
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

    /// Publish the target gauges and snapshot the target list. `active` is the
    /// real latch state: every current caller has already established that it is
    /// set, but the snapshot is also what [`Defragger::publish`] stores, so it
    /// must not assert a latch it did not check.
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
            active: self.active,
        }
    }
}

/// The defrag target set, shared by its two selectors.
///
/// [`DefragState`] is a single-writer structure, but defrag now has two drivers
/// on two threads: the resident [`super::defrag_runner::DefragRunner`] owns the
/// lifecycle (latch, allocator-side selection, completion, cancellation) and the
/// gc-runner's compactor contributes scan-derived, live-pinned targets. They
/// share the state under one mutex — both are slow background loops, so the
/// mutex is uncontended in practice — and the one thing the gc-runner asks every
/// cycle ("is defrag latched?") is published outside it, because it is consulted
/// before the idle check on a thread that must never queue behind defrag.
pub(crate) struct Defragger {
    state: Mutex<DefragState>,
    /// Trigger latch. Published so the gc-runner can apply the effort floor and
    /// size the relocation budget without taking the state mutex.
    active: AtomicBool,
}

impl Defragger {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(DefragState::new()),
            active: AtomicBool::new(false),
        }
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Resident-thread step 1: retire completed targets, cancel stalled ones,
    /// evaluate the trigger latch. Returns whether defrag is latched.
    pub(crate) fn maintain(
        &self,
        allocator: &SpaceAllocator,
        cfg: &GcConfig,
        free_pct: u64,
        metrics: &EngineMetrics,
    ) -> bool {
        let cycle = self
            .state
            .lock()
            .maintain(allocator, cfg, free_pct, metrics);
        self.publish(&cycle);
        cycle.active
    }

    /// Resident-thread step 2: quarantine retired-pinned windows.
    pub(crate) fn select_from_allocator(
        &self,
        allocator: &SpaceAllocator,
        cfg: &GcConfig,
        windows: &[u64],
        metrics: &EngineMetrics,
    ) -> u64 {
        let mut state = self.state.lock();
        let selected = state.select_from_allocator(allocator, cfg, windows, metrics);
        let cycle = state.cycle_output(metrics);
        drop(state);
        self.publish(&cycle);
        selected
    }

    /// Compactor step: contribute live-pinned targets found on this cycle's L2P
    /// window scan, and return the full target list for the candidate scan.
    ///
    /// ⚠ **`try_lock`, never `lock`.** The resident thread can hold this mutex
    /// across `begin_defrag_quarantine`, which ends in
    /// `hazards.wait_extent_clear` — an UNBOUNDED wait for in-flight readers. The
    /// caller is the gc-runner, the thread mandatory reclaim runs on, so blocking
    /// it here would rebuild the exact criticality inversion the thread split
    /// exists to remove (memory: `gc_reclaim_defrag_criticality_inversion`), just
    /// through a mutex instead of a shared cycle. This selection is optional work
    /// on a 1-in-N cadence; skipping a contended cycle costs nothing.
    pub(crate) fn select_from_scan(
        &self,
        allocator: &SpaceAllocator,
        cfg: &GcConfig,
        window_pbas: &mut Vec<u64>,
        live_block_budget: u64,
        deadline: Option<Instant>,
        metrics: &EngineMetrics,
    ) -> Arc<Vec<Extent>> {
        let Some(mut state) = self.state.try_lock() else {
            metrics
                .gc_defrag_scan_lock_skipped
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Arc::new(Vec::new());
        };
        let cycle = state.select_from_scan(
            allocator,
            cfg,
            window_pbas,
            live_block_budget,
            deadline,
            metrics,
        );
        drop(state);
        self.publish(&cycle);
        cycle.targets
    }

    /// Release every quarantine and clear the latch. Idempotent; called on the
    /// resident thread's exit path (including panic unwind) and by the runtime
    /// kill-switches, because quarantined free blocks must be back in the
    /// allocatable pools before the flusher's shutdown drain.
    pub(crate) fn deactivate(&self, allocator: &SpaceAllocator, metrics: &EngineMetrics) {
        self.state.lock().deactivate(allocator, metrics);
        self.active
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    fn publish(&self, cycle: &DefragCycle) {
        self.active
            .store(cycle.active, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn track_target_for_exit_test(&self, target: Extent) {
        self.state.lock().track_target_for_exit_test(target);
        self.active.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Start of the stripe-aligned window containing `pba` (aligned starts satisfy
/// `(start + phase) % stripe == 0`), or `None` when that start would fall below
/// address 0 — i.e. `pba` sits in the grid's partial head window, which can
/// never be cleared as a whole stripe.
fn window_start(pba: u64, stripe: u64, phase: u64) -> Option<u64> {
    pba.checked_sub((pba + phase) % stripe)
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

    /// Unit tests drive many cycles inside one instant, so they evaluate the
    /// trigger on EVERY call (`0` = always due). The production 5000 ms sampling
    /// cadence is covered on its own by
    /// `trigger_latch_is_sampled_on_its_own_interval`.
    /// `defrag_enabled` is default-OFF in production (see
    /// `default_defrag_enabled`), so every test of the mechanism opts in
    /// explicitly rather than inheriting the default.
    fn cfg() -> GcConfig {
        GcConfig {
            defrag_trigger_interval_ms: 0,
            defrag_enabled: true,
            ..GcConfig::default()
        }
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

    /// Every allocated (live) PBA in `[lo, hi)` — stands in for "the PBAs the
    /// compactor's L2P window scan saw" in these unit tests.
    fn live_pbas(a: &SpaceAllocator, lo: u64, hi: u64) -> Vec<u64> {
        (lo..hi).filter(|&p| !a.is_free(Pba(p))).collect()
    }

    /// Drive one full cycle: trigger latch, then scan-driven selection.
    fn cycle(
        st: &mut DefragState,
        a: &SpaceAllocator,
        cfg: &GcConfig,
        free_pct: u64,
        pbas: &[u64],
        budget: u64,
        m: &EngineMetrics,
    ) -> DefragCycle {
        let latched = st.maintain(a, cfg, free_pct, m);
        if !latched.active {
            return latched;
        }
        let mut scan = pbas.to_vec();
        st.select_from_scan(a, cfg, &mut scan, budget, None, m)
    }

    /// Drive one full RESIDENT cycle: trigger latch, enumerate retired-pinned
    /// windows off the allocator, then allocator-side selection. No L2P scan
    /// stand-in anywhere — that is the point of this half.
    fn resident_cycle(
        st: &mut DefragState,
        a: &SpaceAllocator,
        cfg: &GcConfig,
        free_pct: u64,
        m: &EngineMetrics,
    ) -> (u64, DefragCycle) {
        let latched = st.maintain(a, cfg, free_pct, m);
        if !latched.active {
            return (0, latched);
        }
        let mut cursor = 0u64;
        let (windows, _) = a.retired_stripe_windows(
            &mut cursor,
            STRIPE,
            PHASE,
            cfg.defrag_classify_max_windows_per_cycle,
        );
        let selected = st.select_from_allocator(a, cfg, &windows, m);
        (selected, st.cycle_output(m))
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
        let pbas = live_pbas(&a, base, base + 4088);
        let c = cycle(&mut st, &a, &cfg(), 50, &pbas, 4096, &m);
        assert!(c.active, "confetti pool must latch defrag mode");
        assert!(!c.targets.is_empty(), "targets emitted");

        // Fresh single-run pool: stripe-capable ≈ 100% → never latches.
        let fresh = new_alloc(4096);
        let mut st2 = DefragState::new();
        let c2 = cycle(&mut st2, &fresh, &cfg(), 90, &[], 4096, &m);
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
        let pbas = live_pbas(&a, base, base + 4088);
        // free_pct below the 5 floor
        let c = cycle(&mut st, &a, &cfg(), 3, &pbas, 4096, &m);
        assert!(!c.active, "space exhaustion belongs to the urgency ladder");
    }

    #[test]
    fn hysteresis_exits_only_past_enter_plus_10() {
        let a = new_alloc(65536);
        let base = claim_all(&a, 65536);
        confetti(&a, base, base + 30_000, 3, 3);
        let m = metrics();
        let mut st = DefragState::new();
        let pbas = live_pbas(&a, base, base + 30_000);
        assert!(cycle(&mut st, &a, &cfg(), 50, &pbas, 4096, &m).active);
        // Free a huge aligned run: capability jumps far above enter+10 → exit.
        a.free_extent(Extent::new(Pba(base + 30_100), 30_000))
            .unwrap();
        let c = cycle(&mut st, &a, &cfg(), 50, &pbas, 4096, &m);
        assert!(!c.active, "must exit once stripe-capable again");
        assert!(c.targets.is_empty(), "deactivation clears targets");
    }

    /// HEADLINE of the 2026-08-06 redesign: selection only ever picks windows
    /// that HAVE a live pinner, and it prefers the ones with the fewest.
    ///
    /// The old descending-`reclaimable` ranking did the exact opposite: it
    /// ranked "5 free + 1 retired, zero live" windows first, so all 32 target
    /// slots filled with windows the rewriter could not touch and defrag did
    /// zero work for 480 s on the box.
    #[test]
    fn selection_needs_a_live_pinner_and_prefers_the_cheapest() {
        let a = new_alloc(8192);
        let base = claim_all(&a, 8192);
        let win = |w: u64| base_window(base) + w * u64::from(STRIPE);
        // w0: 5 free + 1 live  → cheapest real target (1 block to move).
        a.free_extent(Extent::new(Pba(win(0)), 5)).unwrap();
        // w1: 3 free + 3 live  → qualifies (density 50%) but costs 3 moves.
        a.free_extent(Extent::new(Pba(win(1)), 3)).unwrap();
        // w2: 5 free + 1 RETIRED, no live block → must NOT be selected.
        a.free_extent(Extent::new(Pba(win(2)), 5)).unwrap();
        a.retire_one(Pba(win(2) + 5)).unwrap();
        // w3: 2 free + 4 live → density 33% < 50 → rejected.
        a.free_extent(Extent::new(Pba(win(3)), 2)).unwrap();

        let m = metrics();
        let mut st = DefragState::new();
        let pbas = live_pbas(&a, win(0), win(4));
        // Budget of exactly 1 live block: only the single cheapest window fits.
        let c = cycle(&mut st, &a, &cfg(), 50, &pbas, 1, &m);
        assert!(c.active);
        assert_eq!(
            c.targets.as_slice(),
            &[Extent::new(Pba(win(0)), STRIPE)],
            "cheapest live-pinned window must win"
        );

        // Widen the budget: w1 joins, w2 (no live pinner) and w3 (too dense in
        // live blocks) never do.
        let c2 = cycle(&mut st, &a, &cfg(), 50, &pbas, 64, &m);
        let starts: Vec<u64> = c2.targets.iter().map(|t| t.start.0).collect();
        assert_eq!(starts, vec![win(0), win(1)], "got {starts:?}");
        assert_eq!(m.gc_defrag_clusters_qualified.load(Relaxed), 2);
        assert!(m.gc_defrag_windows_classified.load(Relaxed) >= 4);
    }

    /// A window is quarantined (its free blocks pulled out of the allocatable
    /// pools) BEFORE it is handed to the scanner, so the hole the rewriter is
    /// about to open cannot be refilled by the foreground while in flight.
    #[test]
    fn selected_windows_are_quarantined_before_emission() {
        let a = new_alloc(8192);
        let base = claim_all(&a, 8192);
        confetti(&a, base, base + 8184, 5, 1);
        let m = metrics();
        let mut st = DefragState::new();
        let pbas = live_pbas(&a, base, base + 8184);
        let free_before = a.free_block_count();
        let c = cycle(&mut st, &a, &cfg(), 50, &pbas, 64, &m);
        assert!(!c.targets.is_empty());
        for t in c.targets.iter() {
            assert_eq!(t.count, STRIPE, "every quarantine is one exact stripe");
            assert_eq!((t.start.0 + PHASE as u64) % STRIPE as u64, 0);
            assert!(a.is_defrag_quarantined(*t), "target {t:?} not quarantined");
        }
        for w in c.targets.windows(2) {
            assert!(
                w[0].end_pba().0 <= w[1].start.0,
                "targets must stay disjoint and ascending"
            );
        }
        // Quarantining moves free blocks out of the pools but never loses them.
        assert_eq!(a.free_block_count(), free_before);
    }

    /// Σ live blocks selected is bounded by the relocation budget — the ONLY
    /// per-cycle cap (no arbitrary target-count admission cap).
    #[test]
    fn live_block_budget_bounds_selection() {
        let a = new_alloc(65_536);
        let base = claim_all(&a, 65_536);
        confetti(&a, base, base + 65_528, 4, 2); // 4 free / 2 live per 6
        let m = metrics();
        let pbas = live_pbas(&a, base, base + 65_528);
        for budget in [1u64, 7, 40] {
            let mut st = DefragState::new();
            let c = cycle(&mut st, &a, &cfg(), 50, &pbas, budget, &m);
            let selected = c.targets.len() as u64;
            assert!(selected > 0, "budget {budget} made no progress");
            // Every selected window costs ≥ 1 live block and selection stops as
            // soon as the running total reaches the budget, so the window count
            // can never exceed the budget itself.
            assert!(
                selected <= budget,
                "budget {budget} selected {selected} windows"
            );
        }
    }

    /// The total quarantined span stays under `defrag_max_target_blocks`.
    #[test]
    fn span_budget_caps_targets() {
        let a = new_alloc(65536);
        let base = claim_all(&a, 65536);
        confetti(&a, base, base + 65_528, 4, 2);
        let m = metrics();
        let mut st = DefragState::new();
        let mut capped = cfg();
        capped.defrag_max_target_blocks = 60;
        let pbas = live_pbas(&a, base, base + 65_528);
        let c = cycle(&mut st, &a, &capped, 50, &pbas, u64::MAX / 2, &m);
        let span: u64 = c.targets.iter().map(|t| t.count as u64).sum();
        assert!(span > 0 && span <= 60, "span {span} exceeds budget");
        // Budget full → the next cycle adds nothing.
        let c2 = cycle(&mut st, &a, &capped, 50, &pbas, u64::MAX / 2, &m);
        let span2: u64 = c2.targets.iter().map(|t| t.count as u64).sum();
        assert!(span2 <= 60);
    }

    /// Already-quarantined windows are skipped by later cycles instead of being
    /// re-classified and re-quarantined (which `begin_defrag_quarantine` would
    /// reject anyway, but at the cost of a lock pass and a rejection count).
    #[test]
    fn active_targets_are_not_reselected() {
        let a = new_alloc(8192);
        let base = claim_all(&a, 8192);
        confetti(&a, base, base + 8184, 5, 1);
        let m = metrics();
        let mut st = DefragState::new();
        let pbas = live_pbas(&a, base, base + 8184);
        let first = cycle(&mut st, &a, &cfg(), 50, &pbas, 4, &m);
        let rejected_after_first = m.gc_defrag_clusters_rejected.load(Relaxed);
        let qualified_after_first = m.gc_defrag_clusters_qualified.load(Relaxed);
        assert!(!first.targets.is_empty());

        // Re-run with ONLY the already-selected targets' PBAs in the scan.
        let mut only_active: Vec<u64> = first
            .targets
            .iter()
            .flat_map(|t| (0..u64::from(t.count)).map(move |i| t.start.0 + i))
            .filter(|&p| !a.is_free(Pba(p)))
            .collect();
        let before = st.targets.len();
        st.select_from_scan(&a, &cfg(), &mut only_active, 64, None, &m);
        assert_eq!(st.targets.len(), before, "no new targets");
        assert_eq!(
            m.gc_defrag_clusters_rejected.load(Relaxed),
            rejected_after_first
        );
        assert_eq!(
            m.gc_defrag_clusters_qualified.load(Relaxed),
            qualified_after_first
        );
    }

    #[test]
    fn target_completes_only_at_one_hundred_percent_free() {
        let a = new_alloc(8192);
        let base = claim_all(&a, 8192);
        confetti(&a, base, base + 8184, 3, 3);
        let m = metrics();
        let mut st = DefragState::new();
        let pbas = live_pbas(&a, base, base + 8184);
        let c = cycle(&mut st, &a, &cfg(), 50, &pbas, 4096, &m);
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
    fn stalled_target_is_cancelled_without_losing_free_blocks() {
        let a = new_alloc(8192);
        let base = claim_all(&a, 8192);
        confetti(&a, base, base + 8184, 3, 3);
        let m = metrics();
        let mut st = DefragState::new();
        let mut short_watchdog = cfg();
        short_watchdog.defrag_target_stall_secs = 1;
        let pbas = live_pbas(&a, base, base + 8184);
        let first = cycle(&mut st, &a, &short_watchdog, 50, &pbas, 4096, &m);
        let target = *first.targets.last().expect("target selected");
        let free_before = a.free_block_count();
        st.targets.get_mut(&target.start.0).unwrap().last_progress =
            Instant::now() - Duration::from_secs(2);

        let _ = st.maintain(&a, &short_watchdog, 50, &m);
        assert!(m.gc_defrag_segments_cancelled.load(Relaxed) >= 1);
        assert!(!a.is_defrag_quarantined(target));
        assert_eq!(a.free_block_count(), free_before);
    }

    /// The trigger latch needs `contiguity_stats`, which is ONE LOCK PER ADDRESS
    /// REGION (2048 by default), so it is sampled on `defrag_trigger_interval_ms`
    /// and not on the resident thread's much faster selection cadence. Box
    /// 2026-08-10 measured `free_lock.audit acquisitions=354304` in ~170 s — 2048
    /// per second, 5× mainline — when the two shared a clock.
    #[test]
    fn trigger_latch_is_sampled_on_its_own_interval() {
        // Same setup as `hysteresis_exits_only_past_enter_plus_10`: confetti over
        // the low half, an untouched allocated run above it to free later.
        let a = new_alloc(65536);
        let base = claim_all(&a, 65536);
        confetti(&a, base, base + 30_000, 3, 3);
        let m = metrics();
        let mut st = DefragState::new();
        // Production cadence: the first call evaluates (no previous sample).
        let slow = GcConfig {
            defrag_enabled: true,
            ..GcConfig::default()
        };
        assert!(slow.defrag_trigger_interval_ms >= 5000);
        assert!(st.maintain(&a, &slow, 50, &m).active, "first call must latch");

        // Make the pool stripe-capable again. Within the interval the latch must
        // NOT be re-evaluated, so defrag stays latched.
        a.free_extent(Extent::new(Pba(base + 30_100), 30_000))
            .unwrap();
        assert!(
            st.maintain(&a, &slow, 50, &m).active,
            "latch must not be re-sampled inside the interval"
        );
        // Same state, interval elapsed (0 = always due) → exits.
        assert!(!st.maintain(&a, &cfg(), 50, &m).active);

        // The CHEAP gates are NOT on the slow clock: a hot reload of
        // `defrag_enabled = false` must release quarantines on the very next
        // cycle, not up to `defrag_trigger_interval_ms` later.
        let b = new_alloc(65536);
        let base_b = claim_all(&b, 65536);
        confetti(&b, base_b, base_b + 30_000, 3, 3);
        let mut st2 = DefragState::new();
        assert!(st2.maintain(&b, &slow, 50, &m).active);
        let mut off = slow.clone();
        off.defrag_enabled = false;
        assert!(
            !st2.maintain(&b, &off, 50, &m).active,
            "disable must not wait for the trigger interval"
        );
    }

    // ---- the RESIDENT (allocator-side, zero-scan) selector ----

    /// HEADLINE of the resident half: a window whose remainder is entirely
    /// RETIRED is selected with no L2P scan at all, and one whose remainder is
    /// LIVE is left alone — that one needs a rewrite, so it belongs to the
    /// scan-driven selector. Getting this wrong is what the old descending
    /// free-list walk did in reverse: it filled all its slots with windows the
    /// rewriter could not touch.
    #[test]
    fn resident_selects_retired_pinned_and_skips_live_pinned() {
        let a = new_alloc(8192);
        let base = claim_all(&a, 8192);
        let win = |w: u64| base_window(base) + w * u64::from(STRIPE);
        // w0: 5 free + 1 RETIRED → reclaim alone finishes it. SELECT.
        a.free_extent(Extent::new(Pba(win(0)), 5)).unwrap();
        a.retire_one(Pba(win(0) + 5)).unwrap();
        // w1: 5 free + 1 LIVE → needs the rewriter. SKIP (scan-driven's job).
        a.free_extent(Extent::new(Pba(win(1)), 5)).unwrap();
        // w2: 3 free + 2 retired + 1 LIVE → partial remainder, still live-pinned.
        a.free_extent(Extent::new(Pba(win(2)), 3)).unwrap();
        a.retire_one(Pba(win(2) + 3)).unwrap();
        a.retire_one(Pba(win(2) + 4)).unwrap();

        let m = metrics();
        let mut st = DefragState::new();
        let (selected, c) = resident_cycle(&mut st, &a, &cfg(), 50, &m);
        assert!(c.active);
        assert_eq!(selected, 1, "exactly the retired-pinned window");
        assert_eq!(
            c.targets.as_slice(),
            &[Extent::new(Pba(win(0)), STRIPE)],
            "got {:?}",
            c.targets
        );
        assert!(a.is_defrag_quarantined(Extent::new(Pba(win(0)), STRIPE)));
        assert!(!a.is_defrag_quarantined(Extent::new(Pba(win(1)), STRIPE)));
        assert!(!a.is_defrag_quarantined(Extent::new(Pba(win(2)), STRIPE)));
        assert_eq!(m.gc_defrag_retired_windows_selected.load(Relaxed), 1);
    }

    /// The full resident lifecycle with no rewriter in the picture: quarantine →
    /// reclaim frees the retired remainder → the whole stripe is published into
    /// the reserve. This is the mechanism that makes reclaim, not defrag, the
    /// primary contiguity producer.
    #[test]
    fn resident_target_completes_when_reclaim_frees_the_remainder() {
        let a = new_alloc(8192);
        let base = claim_all(&a, 8192);
        let win = base_window(base);
        a.free_extent(Extent::new(Pba(win), 4)).unwrap();
        a.retire_one(Pba(win + 4)).unwrap();
        a.retire_one(Pba(win + 5)).unwrap();

        let m = metrics();
        let mut st = DefragState::new();
        let (selected, _) = resident_cycle(&mut st, &a, &cfg(), 50, &m);
        assert_eq!(selected, 1);
        let target = Extent::new(Pba(win), STRIPE);
        assert!(a.is_defrag_quarantined(target));
        let reserve_before = a.contiguity_stats().stripe_reserve_blocks;

        // Reclaim the retired remainder — nothing was rewritten, no LBA moved.
        a.reclaim_retired_extent(Extent::new(Pba(win + 4), 2)).unwrap();
        let c = st.maintain(&a, &cfg(), 50, &m);
        assert!(
            !c.targets.iter().any(|t| *t == target),
            "completed target must leave the active map"
        );
        assert_eq!(m.gc_defrag_segments_completed.load(Relaxed), 1);
        assert_eq!(
            a.contiguity_stats().stripe_reserve_blocks,
            reserve_before + u64::from(STRIPE),
            "the whole stripe must land in the reserve"
        );
    }

    /// `defrag_max_target_blocks` is the standing exposure bound — the free space
    /// a quarantine can hold OUT of the allocatable pools at once. It was lowered
    /// 1 GiB → 128 MiB precisely because that exposure is what amplifies the
    /// flusher's `SpaceExhausted`, so the cap has to bind on this path too.
    #[test]
    fn resident_respects_the_span_budget() {
        let a = new_alloc(65_536);
        let base = claim_all(&a, 65_536);
        // Every window: 5 free + 1 retired → all of them qualify.
        let mut w = base_window(base);
        while w + u64::from(STRIPE) <= base + 65_500 {
            a.free_extent(Extent::new(Pba(w), 5)).unwrap();
            a.retire_one(Pba(w + 5)).unwrap();
            w += u64::from(STRIPE);
        }
        let m = metrics();
        let mut capped = cfg();
        capped.defrag_max_target_blocks = 60;
        let mut st = DefragState::new();
        let (_, c) = resident_cycle(&mut st, &a, &capped, 50, &m);
        let span: u64 = c.targets.iter().map(|t| u64::from(t.count)).sum();
        assert!(span > 0 && span <= 60, "span {span} exceeds budget");
        // Budget full → the next cycle adds nothing.
        let (selected2, c2) = resident_cycle(&mut st, &a, &capped, 50, &m);
        assert_eq!(selected2, 0);
        let span2: u64 = c2.targets.iter().map(|t| u64::from(t.count)).sum();
        assert!(span2 <= 60);
    }

    /// Nothing retired ⇒ nothing for the resident half to do, no matter how
    /// shattered the free list is. Those windows are either already folded or
    /// live-pinned; quarantining them would park free space behind the stall
    /// watchdog waiting for a pinner nobody is going to move.
    #[test]
    fn resident_selects_nothing_without_retired_blocks() {
        let a = new_alloc(8192);
        let base = claim_all(&a, 8192);
        confetti(&a, base, base + 8184, 5, 1);
        let m = metrics();
        let mut st = DefragState::new();
        let (selected, c) = resident_cycle(&mut st, &a, &cfg(), 50, &m);
        assert!(c.active, "the pool is still not stripe-capable");
        assert_eq!(selected, 0, "no retired block ⇒ no retired-pinned window");
        assert!(c.targets.is_empty());
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
    fn window_start_lands_on_window_starts_or_declines() {
        for pba in 0u64..40 {
            match window_start(pba, 6, 2) {
                Some(start) => {
                    assert_eq!((start + 2) % 6, 0, "pba {pba} -> {start}");
                    assert!(start <= pba && pba < start + 6);
                }
                // With phase 2 the grid starts at 4, so 0..4 is a partial head
                // window with no whole stripe to clear.
                None => assert!(pba < 4, "pba {pba} should have a window"),
            }
        }
        // Phase 0: every PBA has a window, and address 0 is its own start.
        assert_eq!(window_start(0, 6, 0), Some(0));
        assert_eq!(window_start(5, 6, 0), Some(0));
        assert_eq!(window_start(6, 6, 0), Some(6));
    }

    #[test]
    fn disable_or_no_geometry_deactivates() {
        let a = new_alloc(4096);
        let base = claim_all(&a, 4096);
        confetti(&a, base, base + 4088, 3, 5);
        let m = metrics();
        let mut st = DefragState::new();
        let pbas = live_pbas(&a, base, base + 4088);
        let active = cycle(&mut st, &a, &cfg(), 50, &pbas, 4096, &m);
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
        assert!(!cycle(&mut st_b, &b, &cfg(), 50, &[], 4096, &m).active);
    }

    /// First stripe-aligned window start at or above the reserved prefix.
    fn base_window(base: u64) -> u64 {
        let rem = (base + PHASE as u64) % STRIPE as u64;
        if rem == 0 {
            base
        } else {
            base + (STRIPE as u64 - rem)
        }
    }
}
