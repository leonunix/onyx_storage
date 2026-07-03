use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use crossbeam_channel::Sender;

use crate::buffer::pool::WriteBufferPool;
use crate::dedup::ColdTailTarget;
use crate::gc::config::GcConfig;
use crate::gc::heatmap::HeatMap;
use crate::gc::ref_bitmap::RefBitmap;
use crate::gc::rewriter::rewrite_candidate;
use crate::gc::scanner::{scan_gc_candidates_window, SlotEvacParams};
use crate::io::engine::IoEngine;
use crate::lifecycle::VolumeLifecycleManager;
use crate::meta::schema::{BlockmapValue, FLAG_DEDUP_SKIPPED};
use crate::meta::store::MetaStore;
use crate::metrics::EngineMetrics;
use crate::space::allocator::SpaceAllocator;
use crate::space::pba_lifecycle::PbaLifecycle;
use crate::space::extent::Extent;
use crate::types::{Lba, VolumeId, BLOCK_SIZE};

/// Per-cycle reclaim budget in BLOCKS (not extents). A per-extent cap collapsed
/// throughput under fragmented retires once the grace re-aging bug was fixed; a
/// block budget keeps reclaim able to keep up with the retire rate. Bounded so a
/// single cycle's Gate-2 fold-consistent rc rechecks (per-PBA) stay cheap.
///
/// 4 GiB/cycle: the old 262_144 (1 GiB) capped reclaim at ~52K blocks/s (5s
/// cycles) while sustained randwrite overwrite retires 60-150K blocks/s —
/// retired_depth grew monotonically (6.6M blocks on the 2026-07-03 capture)
/// until multi-block allocations hit SpaceExhausted with 16 GiB nominally
/// free but shattered into single-block fragments. The Gates scale linearly
/// and the allocator-side batch insert is chunked with inter-chunk breathers,
/// so a deeper cycle costs wall time on the GC thread only. Deeper cycles
/// also return MORE adjacent dead blocks together, so the freed extents
/// coalesce better (less single-block confetti).
const MAX_RETIRED_RECLAIM_BLOCKS_PER_CYCLE: usize = 1_048_576;

/// Free-space percentage at/below which the resident compactor's `urgency`
/// reaches full and overrides the idle backoff (compact even under foreground
/// load to keep the device from filling). Above this, pacing is purely
/// idle-driven. Mirrors the old `dynamic_threshold` ladder's pressure region.
const URGENCY_FREE_PCT: u64 = 50;

/// Floor for the per-cycle compactor scan window (LBAs). A window must hold at
/// least one whole compression unit (units span ≤ `coalesce_max_lbas`, default
/// 32) so a non-zero `effort` always makes real progress; 64 > 32 with margin.
const COMPACTOR_MIN_WINDOW_LBAS: u64 = 64;

/// Background GC runner thread.
pub struct GcRunner {
    running: Arc<AtomicBool>,
    config: Arc<ArcSwap<GcConfig>>,
    handle: Option<JoinHandle<()>>,
}

impl GcRunner {
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        meta: Arc<MetaStore>,
        io_engine: Arc<IoEngine>,
        buffer_pool: Arc<WriteBufferPool>,
        lifecycle: Arc<VolumeLifecycleManager>,
        allocator: Arc<SpaceAllocator>,
        heat: HeatMap,
        config: GcConfig,
    ) -> Self {
        let metrics = Arc::new(EngineMetrics::default());
        let pba_lifecycle = PbaLifecycle::new(
            allocator.clone(),
            crate::dedup::CandidateCache::new(1, 1),
            metrics.clone(),
        );
        Self::start_with_metrics(
            metrics,
            meta,
            io_engine,
            buffer_pool,
            lifecycle,
            allocator,
            heat,
            None,
            None,
            pba_lifecycle,
            config,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn start_with_metrics(
        metrics: Arc<EngineMetrics>,
        meta: Arc<MetaStore>,
        io_engine: Arc<IoEngine>,
        buffer_pool: Arc<WriteBufferPool>,
        lifecycle: Arc<VolumeLifecycleManager>,
        allocator: Arc<SpaceAllocator>,
        heat: HeatMap,
        // Stage-5: per-PBA referenced bitmap the heat sweep fills (None unless
        // per-PBA orphan reclaim is on). Filled here; read by the dedup scanner.
        ref_bitmap: Option<RefBitmap>,
        cold_tx: Option<Sender<ColdTailTarget>>,
        pba_lifecycle: PbaLifecycle,
        config: GcConfig,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();
        let config = Arc::new(ArcSwap::from_pointee(config));
        let config_clone = config.clone();

        let handle = thread::Builder::new()
            .name("gc-runner".into())
            .spawn(move || {
                crate::affinity::bind_current(crate::affinity::ThreadRole::Background, 1);
                Self::gc_loop(
                    &metrics,
                    &meta,
                    &io_engine,
                    &buffer_pool,
                    &lifecycle,
                    &allocator,
                    &heat,
                    ref_bitmap.as_ref(),
                    cold_tx.as_ref(),
                    &pba_lifecycle,
                    &config_clone,
                    &running_clone,
                );
            })
            .expect("failed to spawn gc runner thread");

        Self {
            running,
            config,
            handle: Some(handle),
        }
    }

    /// Hot-reload GC configuration.
    pub fn update_config(&self, new_config: GcConfig) {
        tracing::info!("gc: config updated");
        self.config.store(Arc::new(new_config));
    }

    /// Compute the dead_ratio threshold from space pressure.
    ///
    /// The compactor is ALWAYS resident; `free_pct` only tunes how aggressive it
    /// is, it is no longer an on/off switch. When space is plentiful only
    /// clearly-dead units (`compactor_resident_threshold`, default 0.85) are
    /// compacted — cheap, high-yield, and enough to keep packing-slack debt
    /// bounded instead of letting it grow forever. As space tightens the
    /// threshold lowers so more partially-dead units get reclaimed.
    fn dynamic_threshold(cfg: &GcConfig, free_pct: u64) -> f64 {
        if free_pct > 50 {
            cfg.compactor_resident_threshold // Plentiful — only clearly-dead units (debt-bounding).
        } else if free_pct > 30 {
            0.50 // Moderate pressure
        } else if free_pct > 10 {
            0.30 // Getting tight
        } else {
            cfg.dead_ratio_threshold // Critical — configured minimum (default 0.25)
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn gc_loop(
        metrics: &EngineMetrics,
        meta: &MetaStore,
        io_engine: &IoEngine,
        buffer_pool: &WriteBufferPool,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        heat: &HeatMap,
        ref_bitmap: Option<&RefBitmap>,
        cold_tx: Option<&Sender<ColdTailTarget>>,
        pba_lifecycle: &PbaLifecycle,
        config: &ArcSwap<GcConfig>,
        running: &AtomicBool,
    ) {
        let mut compactor_cursor = CompactorCursor::default();
        let mut heat_cursor = HeatCursor::default();
        // Reclaim-age grace now lives in the allocator's per-original-retire age
        // log (`aged_candidates`), which is immune to the coalesce re-aging the
        // old runner-side `retired_first_seen: BTreeMap<Extent, Instant>` map
        // suffered (a coalesced extent re-keyed → re-aged → starved reclaim).

        while running.load(Ordering::Relaxed) {
            let cfg = config.load();
            thread::sleep(Duration::from_millis(cfg.scan_interval_ms));

            if !running.load(Ordering::Relaxed) {
                break;
            }

            let cycle = metrics.gc_cycles.fetch_add(1, Ordering::Relaxed) + 1;

            // Stage-B: reclaim consumes the heat map only when both the refresh
            // (so the map is real + current) and the consumption flag are on.
            // Default-OFF — behavior is identical to today until flipped.
            let heat_ctx = if cfg.heat_enabled && cfg.heat_reclaim_enabled {
                Some(HeatReclaimCtx {
                    heat,
                    fresh_max_age: cfg.heat_fresh_max_age,
                    force_confirm_interval: cfg.heat_force_confirm_interval_cycles,
                    cycle,
                    yield_suppress_milli: u32::from(cfg.heat_defer_yield_suppress_pct.min(100))
                        * 10,
                    recalibrate_interval: cfg.heat_defer_recalibrate_interval_cycles,
                    min_free_pct: u64::from(cfg.heat_defer_min_free_pct),
                })
            } else {
                None
            };

            // Per-phase cycle timing (debug-level): reclaim vs heat vs compactor.
            // This is what localized the reclaim-latency runaway (per-extent
            // metadb verification + per-extent retire/free lock contention) to
            // `reclaim_ms`; kept at debug for regression diagnosis. GC cadence is
            // ~5s+ so even at debug the volume is low.
            let t_reclaim = Instant::now();
            Self::reclaim_retired_extents(
                metrics,
                meta,
                allocator,
                pba_lifecycle,
                MAX_RETIRED_RECLAIM_BLOCKS_PER_CYCLE,
                running,
                heat_ctx,
                Some(Duration::from_secs(cfg.reclaim_grace_secs)),
            );
            let reclaim_ms = t_reclaim.elapsed().as_millis();

            // Standing background heat-map refresh (observe-only, Stage A):
            // a bounded, lock-free-per-chunk slow scan that accumulates a
            // per-PBA-region live-mapping count. Runs even when rewrite GC is
            // disabled and even when reclaim found nothing — it is decoupled
            // from reclaim having work. Front-end IO never pays for it.
            let t_heat = Instant::now();
            if cfg.heat_enabled && cfg.heat_refresh_max_lbas_per_cycle > 0 {
                // Stage-4 fold: when `cold_tx` is wired (fold enabled), the
                // heat walk also emits cold candidates for the dedup scanner,
                // bounded by `heat_fold_push_max_per_cycle`. `cold_tx` is None
                // when the fold is off ⇒ pure observe-only heat refresh.
                Self::heat_refresh_step(
                    heat,
                    ref_bitmap,
                    metrics,
                    meta,
                    &mut heat_cursor,
                    cfg.heat_refresh_max_lbas_per_cycle,
                    running,
                    cfg.heat_adaptive_refresh_enabled,
                    cfg.heat_staleness_floor_sweeps,
                    cold_tx,
                    cfg.heat_fold_push_max_per_cycle,
                );
            }

            let heat_ms = t_heat.elapsed().as_millis();

            if !cfg.enabled || cfg.compactor_scan_max_lbas_per_cycle == 0 {
                tracing::debug!(
                    cycle, reclaim_ms, heat_ms, compactor_ms = 0u128,
                    depth = allocator.retired_block_count(),
                    "gc cycle timing (compactor skipped)"
                );
                continue; // GC off, or compactor kill-switch (reclaim+heat above still run)
            }

            // Resident, idle-paced compaction. Unlike the old free_pct on/off
            // gate (which never ran on a large/thin device, letting packing-slack
            // debt grow forever), the compactor ALWAYS runs; per-cycle `effort`
            // scales with how idle the write pipeline is, and space pressure
            // (low free%) overrides the idle backoff so the device cannot
            // silently fill. The candidate scan is bounded to one ~1M-LBA window
            // per cycle (a lap cursor sweeps the whole L2P over many cycles)
            // instead of a full ~80M-entry scan.
            let total_blocks = allocator.total_block_count();
            let free_pct = if total_blocks == 0 {
                100
            } else {
                allocator.free_block_count() * 100 / total_blocks
            };
            // Backpressure signal for the self-throttle. `fill_percentage()` is
            // "soft work in flight" vs ring capacity and reads ~0 even when the
            // durable payload cache has ballooned to several GB — which is
            // exactly the symptom of a throttled commit-apply (the flusher
            // can't drain, payloads stay resident). Using it alone left the
            // compactor pinned at effort=1.0 while it was itself the cause of
            // the balloon. Take the MAX with the resident payload depth so
            // either pressure source backs the compactor off, closing the
            // feedback loop (compactor scan throttles commit → buffer grows →
            // effort drops → commit recovers → buffer drains).
            let fill_pct = buffer_pool
                .fill_percentage()
                .max(buffer_pool.payload_fill_percentage());
            let effort = compute_effort(fill_pct, free_pct, cfg.buffer_usage_max_pct);
            if effort < 0.01 {
                // Busy AND plenty of space → idle the compactor; cursor untouched
                // so it resumes exactly where it left off when load drops.
                metrics.gc_paused_cycles.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    cycle, reclaim_ms, heat_ms, compactor_ms = 0u128, effort,
                    depth = allocator.retired_block_count(),
                    "gc cycle timing (compactor idled)"
                );
                continue;
            }

            let threshold = Self::dynamic_threshold(&cfg, free_pct);
            let scan_budget = std::cmp::max(
                (cfg.compactor_scan_max_lbas_per_cycle as f64 * effort) as u64,
                COMPACTOR_MIN_WINDOW_LBAS,
            );
            // `.ceil()` so any effort > 0 yields at least one rewrite (else tiny
            // effort would starve forward progress).
            let rewrite_budget = (cfg.max_rewrite_per_cycle as f64 * effort).ceil() as usize;

            // Slot-aware compaction: inert unless the flag is on AND rc is
            // authoritative (the completeness check relies on rc(P) == the
            // slot's live-LBA count). `max_live` is clamped to `rewrite_budget`
            // so a single slot can never exceed the per-cycle budget.
            let slot_evac = SlotEvacParams {
                enabled: cfg.compactor_slot_evac_enabled && meta.rc_authoritative_reclaim(),
                max_live: std::cmp::min(
                    cfg.compactor_slot_evac_max_live as usize,
                    rewrite_budget,
                ) as u16,
                block_size: BLOCK_SIZE,
            };

            let t_comp = Instant::now();
            Self::compactor_step(
                metrics,
                meta,
                io_engine,
                buffer_pool,
                lifecycle,
                allocator,
                &mut compactor_cursor,
                threshold,
                scan_budget,
                rewrite_budget,
                slot_evac,
                running,
            );
            tracing::debug!(
                cycle, reclaim_ms, heat_ms,
                compactor_ms = t_comp.elapsed().as_millis(),
                effort, scan_budget, rewrite_budget,
                depth = allocator.retired_block_count(),
                "gc cycle timing"
            );
        }
    }

    /// One resident-compactor step: scan a single bounded LBA window of the next
    /// volume (lap cursor), turn high-dead-ratio units into rewrite candidates,
    /// and rewrite up to `rewrite_budget` of them (live blocks go back through
    /// the buffer; the flusher remaps and the retire→reclaim path frees the old
    /// PBAs). Also accumulates the compactable-dead-block (debt) estimate and
    /// publishes it once per full sweep.
    #[allow(clippy::too_many_arguments)]
    fn compactor_step(
        metrics: &EngineMetrics,
        meta: &MetaStore,
        io_engine: &IoEngine,
        buffer_pool: &WriteBufferPool,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        cursor: &mut CompactorCursor,
        threshold: f64,
        scan_budget: u64,
        rewrite_budget: usize,
        slot_evac: SlotEvacParams,
        running: &AtomicBool,
    ) {
        if scan_budget == 0 || rewrite_budget == 0 {
            return;
        }
        let volumes = match meta.list_volumes() {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!(error = %e, "compactor: list_volumes failed");
                return;
            }
        };
        if volumes.is_empty() {
            return;
        }
        // Sweep target = total live LBA span across the volume set (a prior,
        // recomputed each cycle so create/drop degrades gracefully).
        let target: u64 = volumes
            .iter()
            .map(|v| v.size_bytes / u64::from(v.block_size.max(1)))
            .sum();
        if target == 0 {
            return;
        }
        // Drop cursor state for volumes that no longer exist.
        let live: HashSet<&str> = volumes.iter().map(|v| v.id.0.as_str()).collect();
        cursor.per_vol.retain(|k, _| live.contains(k.as_str()));

        // Pick the next non-empty volume (one window per cycle, round-robin).
        let mut picked = None;
        for _ in 0..volumes.len() {
            let cand = &volumes[cursor.vol_idx % volumes.len()];
            cursor.vol_idx = cursor.vol_idx.wrapping_add(1);
            let total_lbas = cand.size_bytes / u64::from(cand.block_size.max(1));
            if total_lbas > 0 {
                picked = Some((cand.clone(), total_lbas));
                break;
            }
        }
        let Some((vol, total_lbas)) = picked else {
            return; // every volume empty this cycle
        };

        // This cycle's contiguous LBA window from the volume's lap.
        let (phys_start, chunk) = {
            let lap = cursor.per_vol.entry(vol.id.0.clone()).or_insert_with(|| {
                let mut rng = heat_lap_seed(&vol.id.0);
                let lap_start = splitmix64(&mut rng) % total_lbas;
                CompactorLap {
                    lap_start,
                    scanned_in_lap: 0,
                    rng,
                }
            });
            next_compactor_window(lap, total_lbas, scan_budget)
        };
        if chunk == 0 {
            return;
        }

        let (candidates, dead_estimate, slot_stats) = match scan_gc_candidates_window(
            meta,
            &vol.id,
            Lba(phys_start),
            chunk,
            threshold,
            rewrite_budget,
            slot_evac,
        ) {
            Ok(r) => r,
            Err(e) => {
                metrics.gc_errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(vol = %vol.id.0, error = %e, "compactor: window scan failed");
                return;
            }
        };
        cursor.dead_estimate_acc = cursor.dead_estimate_acc.saturating_add(dead_estimate);

        // Slot-aware compaction selection stats (no-op when slot-evac is off).
        if slot_stats.candidates > 0 {
            metrics
                .gc_slot_evac_candidates
                .fetch_add(slot_stats.candidates, Ordering::Relaxed);
            metrics
                .gc_slot_evac_blocks
                .fetch_add(slot_stats.blocks, Ordering::Relaxed);
        }
        if slot_stats.incomplete_skips > 0 {
            metrics
                .gc_slot_evac_incomplete_skips
                .fetch_add(slot_stats.incomplete_skips, Ordering::Relaxed);
        }
        if slot_stats.cost_cap_skips > 0 {
            metrics
                .gc_slot_evac_cost_cap_skips
                .fetch_add(slot_stats.cost_cap_skips, Ordering::Relaxed);
        }

        // Sweep odometer: publish the debt estimate once a full sweep completes,
        // then reset for the next sweep.
        if let Some(published) = advance_sweep(cursor, chunk, target) {
            metrics
                .gc_compactable_dead_blocks
                .store(published, Ordering::Relaxed);
        }

        metrics
            .gc_candidates_found
            .fetch_add(candidates.len() as u64, Ordering::Relaxed);

        for candidate in &candidates {
            if !running.load(Ordering::Relaxed) {
                break;
            }
            metrics.gc_rewrite_attempts.fetch_add(1, Ordering::Relaxed);
            match rewrite_candidate(
                candidate,
                io_engine,
                buffer_pool,
                meta,
                lifecycle,
                Some(&allocator.hazards()),
            ) {
                Ok(rewritten) => {
                    metrics
                        .gc_blocks_rewritten
                        .fetch_add(rewritten as u64, Ordering::Relaxed);
                }
                Err(e) => {
                    metrics.gc_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        pba = candidate.pba.0,
                        vol = %candidate.vol_id,
                        error = %e,
                        "compactor: failed to rewrite candidate"
                    );
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn reclaim_retired_extents(
        metrics: &EngineMetrics,
        meta: &MetaStore,
        allocator: &SpaceAllocator,
        pba_lifecycle: &PbaLifecycle,
        limit_blocks: usize,
        running: &AtomicBool,
        heat_ctx: Option<HeatReclaimCtx<'_>>,
        // Reclaim grace: a retired PBA is only eligible once it has settled in
        // the retired set for `>= grace`, guaranteeing the in-flight/mid-fold
        // settle window before the reverify (no premature free). The age is now
        // tracked per-original-retire in the allocator (`aged_candidates`), so
        // coalescing the retired set can no longer re-age it (the old
        // runner-side `BTreeMap<Extent, Instant>` re-aged on every coalesce →
        // starved reclaim). `None` = immediate (tests).
        grace: Option<Duration>,
    ) -> usize {
        let grace = grace.unwrap_or(Duration::ZERO);
        // Backlog gauge: the full retired-set depth (NOT just this cycle's
        // budget). O(retired extents) under one lock, once per GC cycle — cheap
        // at the 5 s cadence, and the direct signal for "draining or filling".
        metrics
            .gc_retired_blocks_depth
            .store(allocator.retired_block_count(), Ordering::Relaxed);

        // Grace-satisfied (settled ≥ grace) retired sub-ranges, block-budgeted.
        let t_select = Instant::now();
        let (candidates, deferred_blocks) =
            allocator.aged_candidates(limit_blocks, grace, std::time::Instant::now());
        // Diagnostic: retired-but-still-young blocks held back by the grace. Once
        // the re-aging fix is in this is small (only the last `grace` window of
        // retires); its dominance vs `gc_reclaim_rc_rejected` is how the
        // re-aging bottleneck was localized.
        metrics
            .gc_reclaim_grace_deferred
            .fetch_add(deferred_blocks, Ordering::Relaxed);
        if candidates.is_empty() {
            return 0;
        }
        let n_candidates = candidates.len();
        let select_ms = t_select.elapsed().as_millis();

        // Gate 1 (refcount): keep only candidates whose every PBA has rc==0.
        //
        // BATCHED: flatten every candidate PBA into ONE `multi_get_refcounts`
        // and re-group per extent. The old per-extent call issued one metadb
        // round-trip per candidate extent; under scattered-overwrite churn the
        // retired set fragments into ~single-block extents, so a block-budgeted
        // cycle made up to `limit_blocks` separate calls → reclaim cost grew
        // super-linearly with retired depth (the capacity runaway). One batched
        // read makes the cost O(budget blocks) instead of O(#extents).
        let t_gate1 = Instant::now();
        let cand_pbas = Self::flatten_extent_pbas(&candidates);
        let cand_rcs = match meta.multi_get_refcounts(&cand_pbas) {
            Ok(v) => v,
            Err(e) => {
                metrics.gc_errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    candidates = n_candidates,
                    blocks = cand_pbas.len(),
                    error = %e,
                    "gc: batched Gate-1 refcount read failed; skipping cycle"
                );
                return 0;
            }
        };
        let mut survivors: Vec<Extent> = Vec::with_capacity(candidates.len());
        let mut cursor = 0usize;
        for extent in candidates {
            let n = extent.count as usize;
            let slice = &cand_rcs[cursor..cursor + n];
            cursor += n;
            if !running.load(Ordering::Relaxed) {
                break;
            }
            if slice.iter().all(|&refcount| refcount == 0) {
                survivors.push(extent);
            } else {
                // Diagnostic: grace-aged blocks still rejected because a PBA is
                // referenced (rc>0) — e.g. a dedup hit re-referenced a retired
                // PBA. High vs `gc_reclaim_grace_deferred` means rc>0 zombies
                // (not grace) wedge the retired set.
                metrics
                    .gc_reclaim_rc_rejected
                    .fetch_add(u64::from(extent.count), Ordering::Relaxed);
            }
        }
        let gate1_ms = t_gate1.elapsed().as_millis();
        if survivors.is_empty() {
            return 0;
        }

        // Stage-B heat pre-filter (between Gate 1 and Gate 2): when reclaim
        // consumes the heat map, defer the confirm scan of any survivor whose
        // whole region still looks hot+fresh — *if* deferring actually pays.
        //
        // The premise "hot region ⇒ extent still referenced ⇒ confirm scan
        // wasted" only holds when retired rc==0 extents are often still
        // referenced (dedup-heavy: a re-share incref not yet drained). Under
        // unique/discard churn the retired extents are genuinely dead, the
        // confirm scan is PRODUCTIVE, and deferring only loses real reclaim. The
        // yield gate self-corrects: confirm-all (no defer) whenever a recent
        // confirm scan reclaimed a high fraction of what it scanned (`yield_high`),
        // whenever free space is tight (`pressure`), periodically to recalibrate,
        // and on the force-confirm pass. Cold start (no measurement) trusts the
        // heat prior and defers. The free decision at the scan is unchanged —
        // heat only changes *whether* we scan, so staleness can only ever delay
        // reclaim, never free a live block.
        let confirm_all = if let Some(ctx) = &heat_ctx {
            let forced =
                ctx.force_confirm_interval != 0 && ctx.cycle % ctx.force_confirm_interval == 0;
            let recalibrate =
                ctx.recalibrate_interval != 0 && ctx.cycle % ctx.recalibrate_interval == 0;
            let pressure = ctx.min_free_pct > 0 && {
                let total = allocator.total_block_count();
                total > 0 && allocator.free_block_count() * 100 / total <= ctx.min_free_pct
            };
            let yield_high = ctx
                .heat
                .confirm_yield_milli()
                .is_some_and(|y| y >= ctx.yield_suppress_milli);
            if forced {
                metrics
                    .gc_heat_force_confirm_passes
                    .fetch_add(1, Ordering::Relaxed);
            }
            let ca = forced || recalibrate || pressure || yield_high;
            if ca && !forced {
                // Defer suppressed by the gate (recalibrate / pressure / a
                // productive recent scan) — not the anti-starvation force pass.
                metrics
                    .gc_heat_defer_suppressed
                    .fetch_add(1, Ordering::Relaxed);
            }
            ca
        } else {
            false
        };

        let survivors = if let (Some(ctx), false) = (&heat_ctx, confirm_all) {
            let mut to_confirm = Vec::with_capacity(survivors.len());
            let mut deferred = 0u64;
            for e in survivors {
                if ctx
                    .heat
                    .extent_hot_and_fresh(e.start, e.count, ctx.fresh_max_age)
                {
                    deferred += 1; // stays retired, re-presented later
                } else {
                    to_confirm.push(e);
                }
            }
            if deferred > 0 {
                metrics
                    .gc_heat_deferred_extents
                    .fetch_add(deferred, Ordering::Relaxed);
            }
            to_confirm
        } else {
            survivors
        };
        if heat_ctx.is_some() {
            metrics
                .gc_heat_confirmed_extents
                .fetch_add(survivors.len() as u64, Ordering::Relaxed);
            if survivors.is_empty() {
                // Every survivor deferred → skip the all-volume scan entirely
                // (the headline Stage-B win).
                metrics
                    .gc_heat_scans_skipped
                    .fetch_add(1, Ordering::Relaxed);
                return 0;
            }
        }
        // Did this cycle scan the FULL survivor set (incl. hot regions)? Only
        // then is its reclaim yield a valid measurement of "is deferring worth
        // it" — a pure-defer cycle confirms only cold extents and would bias the
        // yield high.
        let measured_all = heat_ctx.is_some() && confirm_all;
        let scanned_extents = survivors.len();

        // Hazard barrier BEFORE the Gate-2 scan (P0 premature-free fix).
        //
        // An unguarded candidate-promote (`atomic_batch_dedup_hits_with_promote`
        // with `guard=None`) can commit `L2pRemap L→P` for a survivor P that was
        // just demoted/retired, re-referencing it. Its hazard pin (taken in the
        // dedup worker's verify path and held across the remap commit) is the
        // only signal that such a remap is in flight. Draining those pins here —
        // *before* `referenced_extents` — guarantees the scan observes any
        // committed `L→P`, so a resurrected PBA is marked referenced and left
        // retired instead of being freed out from under the live mapping.
        //
        // Without this, the scan ran first and `reclaim_retired_extent`'s own
        // `wait_extent_clear` only delayed the free until *after* the promote
        // committed, then freed P anyway (no post-wait re-validation) → the
        // freed PBA was reused while `L→P` was live → foreground-read CRC
        // mismatch. Cold/background path: zero front-end cost. The dedup pin is
        // also extended to candidate-lookup time (see `candidate_lookup_pass`)
        // so the lookup→pin gap can't slip a promote past this barrier.
        for extent in &survivors {
            if !running.load(Ordering::Relaxed) {
                return 0;
            }
            allocator.wait_for_readers(extent.start, extent.count);
        }

        // Gate 2 (blockmap): a survivor is reclaimable iff no live blockmap
        // entry references any PBA inside it.
        //
        // rc-authoritative mode: refcount counts every live L2P reference, so
        // a `rc==0` reading is proof of no reference — the full-volume
        // `referenced_extents` reverify scan (which held metadb's per-shard
        // `tree.read()` across the whole volume and stalled the BFG
        // fold/checkpoint → multi-second commit spikes) is unnecessary and
        // skipped. BUT Gate-1 above used the cheap, fold-RACY
        // `multi_get_refcounts`, which can transiently floor a still-live
        // rc to a spurious 0 when it straddles a refcount fold's
        // publish-before-clear window (see metadb `merge_read_or_floor` /
        // `RcShard::get_consistent`). Freeing on that spurious 0 is the
        // rc_authoritative premature-free CRC (2026-06-12 r2 soak). So before
        // the irreversible free, re-read each survivor's rc CONSISTENTLY
        // (fold-coherent, post-barrier, survivors-only → bounded) and treat a
        // now-nonzero rc as "still referenced" — leave it retired for a later
        // cycle. This is the bounded backstop the skipped blockmap scan used
        // to provide. The grace + hazard barrier above still cover the
        // un-drained-incref / in-flight-promote windows.
        let t_gate2 = Instant::now();
        let referenced: Vec<bool> = if meta.rc_authoritative_reclaim() {
            // BATCHED consistent recheck: flatten survivor PBAs into ONE
            // `multi_get_refcounts_consistent`, which amortizes the per-shard
            // `fold_lock` (see metadb `get_consistent_into`). The old per-extent
            // call took `fold_lock` once PER PBA, contending the BFG fold under
            // sustained reclaim — the super-linear term in reclaim cost. Results
            // are re-grouped per extent; referenced iff ANY PBA rc != 0.
            let surv_pbas = Self::flatten_extent_pbas(&survivors);
            match meta.multi_get_refcounts_consistent(&surv_pbas) {
                Ok(refcounts) => {
                    let mut out = Vec::with_capacity(survivors.len());
                    let mut cur = 0usize;
                    for extent in &survivors {
                        let n = extent.count as usize;
                        let still_referenced =
                            refcounts[cur..cur + n].iter().any(|&refcount| refcount != 0);
                        cur += n;
                        if still_referenced {
                            // Gate-1 passed it (raced rc==0) but the consistent
                            // read caught a live ref → a premature free averted.
                            metrics
                                .gc_reclaim_premature_free_averted
                                .fetch_add(1, Ordering::Relaxed);
                            tracing::debug!(
                                pba = extent.start.0,
                                blocks = extent.count,
                                "gc: premature free averted — consistent rc recheck found a \
                                 live reference the Gate-1 fold-racy read floored to 0"
                            );
                        }
                        out.push(still_referenced);
                    }
                    out
                }
                Err(e) => {
                    metrics.gc_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        candidates = survivors.len(),
                        error = %e,
                        "gc: batched consistent refcount recheck failed; leaving extents retired"
                    );
                    // Conservative: a read error must NOT free any PBA → skip the
                    // whole cycle (same net effect as the old per-extent `true`).
                    return 0;
                }
            }
        } else {
            // Phase-5 fallback: rc is NOT authoritative for L2P references, so
            // ONE batched all-volume L2P scan is required to catch rc-untracked
            // (packed/multi-LBA shared-base) references.
            let extents: Vec<(crate::types::Pba, u32)> =
                survivors.iter().map(|e| (e.start, e.count)).collect();
            metrics
                .gc_reclaim_blockmap_scans
                .fetch_add(1, Ordering::Relaxed);
            match meta.referenced_extents(&extents) {
                Ok(referenced) => referenced,
                Err(e) => {
                    metrics.gc_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        candidates = survivors.len(),
                        error = %e,
                        "gc: failed to scan blockmap for retired physical extents"
                    );
                    return 0;
                }
            }
        };
        let gate2_ms = t_gate2.elapsed().as_millis();

        let t_free = Instant::now();
        let n_survivors = survivors.len();
        // Collect the Gate-2-confirmed (unreferenced) survivors and free them in
        // ONE batched, lock-amortized call. The old per-extent
        // `confirm_and_reclaim` loop paid ~2×num_lanes lane-cache mutex locks +
        // 3 contended allocator locks PER extent, which dominated reclaim under
        // sustained churn (the second O(depth) term in the capacity runaway).
        let to_reclaim: Vec<Extent> = survivors
            .into_iter()
            .zip(referenced)
            .filter_map(|(extent, is_referenced)| (!is_referenced).then_some(extent))
            .collect();
        let (reclaimed, reclaimed_extents) =
            match pba_lifecycle.confirm_and_reclaim_batch(&to_reclaim, running) {
                Ok((blocks, extents)) => (blocks as usize, extents),
                Err(e) => {
                    metrics.gc_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(error = %e, "gc: batched retired reclaim failed");
                    (0, 0)
                }
            };
        let free_ms = t_free.elapsed().as_millis();

        // Feed the yield gate only from a full-survivor (confirm-all) scan: how
        // productive was the scan we just paid for? High yield ⇒ deferring would
        // lose real reclaim ⇒ suppress defer next cycles.
        if measured_all {
            if let Some(ctx) = &heat_ctx {
                ctx.heat
                    .record_confirm_yield(scanned_extents, reclaimed_extents);
            }
        }

        if reclaimed > 0 {
            metrics
                .gc_retired_blocks_reclaimed
                .fetch_add(reclaimed as u64, Ordering::Relaxed);
            tracing::debug!(blocks = reclaimed, "gc: reclaimed retired physical blocks");
        }
        // Reclaim sub-phase breakdown (debug-level observability): select =
        // aged_candidates+gauge, gate1/gate2 = batched refcount reads, free =
        // batched confirm_and_reclaim. The batching keeps each phase ~flat as
        // retired depth grows; this is how a regression would be spotted. Only
        // emitted when a cycle did non-trivial reclaim work.
        if select_ms + gate1_ms + gate2_ms + free_ms >= 50 {
            tracing::debug!(
                select_ms, gate1_ms, gate2_ms, free_ms,
                n_candidates, n_survivors, reclaimed,
                "gc reclaim phase timing"
            );
        }
        reclaimed
    }

    /// Flatten a list of physical extents into their constituent PBAs, in
    /// extent order — the input for a single batched `multi_get_refcounts*`
    /// (one metadb round-trip instead of one per extent; see the reclaim gates).
    fn flatten_extent_pbas(extents: &[Extent]) -> Vec<crate::types::Pba> {
        let total: usize = extents.iter().map(|e| e.count as usize).sum();
        let mut pbas = Vec::with_capacity(total);
        for extent in extents {
            for offset in 0..extent.count {
                pbas.push(crate::types::Pba(extent.start.0 + offset as u64));
            }
        }
        pbas
    }

    /// Background heat-map refresh step (observe-only, Stage A). Walks up to
    /// `budget` live blockmap entries this cycle, round-robin across the volume
    /// set, bumping the covering PBA region for each physical block. When the
    /// per-cycle budgets have together covered the volume set's total LBA span
    /// (one full sweep), advances the heat epoch.
    ///
    /// Cost model: pure metadb decode + atomic bump, **no LV3 reads** — cheaper
    /// than the dedup cold-tail pass. Bounded per cycle, honors `running`, and
    /// holds no lock across the walk (`scan_blockmap_range` chunks internally).
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    fn heat_refresh_step(
        heat: &HeatMap,
        ref_bitmap: Option<&RefBitmap>,
        metrics: &EngineMetrics,
        meta: &MetaStore,
        cursor: &mut HeatCursor,
        budget: u64,
        running: &AtomicBool,
        adaptive: bool,
        staleness_floor: u32,
        cold_tx: Option<&Sender<ColdTailTarget>>,
        push_budget: usize,
    ) {
        if budget == 0 {
            return;
        }
        let volumes = match meta.list_volumes() {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!(error = %e, "heat refresh: list_volumes failed");
                return;
            }
        };
        if volumes.is_empty() {
            return;
        }

        // A "sweep" is `target` LBAs worth of walking — the total live LBA span
        // across the current volume set. Recomputed each call so mid-sweep
        // create/drop degrades gracefully (this is a prior, not exact
        // accounting). The odometer (`sweep_lbas_done`) persists across calls.
        let target: u64 = volumes
            .iter()
            .map(|v| v.size_bytes / u64::from(v.block_size.max(1)))
            .sum();
        if target == 0 {
            return;
        }
        // Drop cursor state for volumes that no longer exist.
        let live: HashSet<&str> = volumes.iter().map(|v| v.id.0.as_str()).collect();
        cursor.per_vol.retain(|k, _| live.contains(k.as_str()));

        metrics.heat_refresh_cycles.fetch_add(1, Ordering::Relaxed);

        // Stage-5: take the per-PBA fill buffer out of the cursor for the
        // duration of this cycle's walk so the scan closures can borrow it
        // mutably without conflicting with `cursor.per_vol` / `sweep_lbas_done`.
        // It is put back (or published) after the walk. `None` ⇒ per-PBA orphan
        // reclaim is off and nothing below touches the bitmap (zero-cost).
        let mut ref_fill: Option<Box<[u64]>> = ref_bitmap.map(|rb| {
            cursor
                .ref_fill
                .take()
                .unwrap_or_else(|| rb.fresh_fill_buffer())
        });

        // Walk at most one full sweep's worth of LBAs per cycle. When the
        // budget exceeds the whole volume set (small datasets), re-lapping the
        // same live mappings many times in one cycle is pure waste and would
        // hammer metadb read locks, starving the flusher/rewriter. In
        // production `target` (billions of LBAs) dwarfs the budget, so this cap
        // is inert and a sweep spans many cycles.
        let mut remaining = budget.min(target);
        let mut scanned_total = 0u64;
        let mut bumps_total = 0u64;
        // Stage-4 fold: cold candidates emitted to the dedup scanner this
        // cycle, capped at `push_budget` across all volumes (shared between
        // the adaptive and uniform branches; only one runs per cycle).
        let mut pushed = 0usize;

        // Stage-B2: when adaptive refresh is on AND there is more than one volume
        // to differentiate, split this cycle's budget across volumes weighted by
        // recent write churn (more budget to changing volumes), with
        // `staleness_floor` guaranteeing each volume is fully covered at least
        // that often regardless of churn. A single volume can't be
        // differentiated, so it falls through to the uniform round-robin path.
        let use_adaptive = adaptive && volumes.len() > 1;
        if use_adaptive {
            metrics
                .heat_refresh_adaptive_cycles
                .fetch_add(1, Ordering::Relaxed);
            let vol_lbas: Vec<u64> = volumes
                .iter()
                .map(|v| v.size_bytes / u64::from(v.block_size.max(1)))
                .collect();
            let churn: Vec<u64> = volumes
                .iter()
                .map(|v| {
                    let cur = metrics
                        .get_volume_metrics(&v.id.0)
                        .write_bytes
                        .load(Ordering::Relaxed);
                    // delta since last adaptive cycle = recent write churn
                    let prev = cursor.last_write_bytes.insert(v.id.0.clone(), cur);
                    cur.saturating_sub(prev.unwrap_or(cur))
                })
                .collect();
            let sub = split_refresh_budget(&vol_lbas, &churn, remaining, staleness_floor);
            for (i, vol) in volumes.iter().enumerate() {
                if !running.load(Ordering::Relaxed) || remaining == 0 {
                    break;
                }
                let total_lbas = vol_lbas[i];
                let want = sub[i].min(remaining);
                if total_lbas == 0 || want == 0 {
                    continue;
                }
                let (phys_start, chunk) = {
                    let lap = cursor.per_vol.entry(vol.id.0.clone()).or_insert_with(|| {
                        let mut rng = heat_lap_seed(&vol.id.0);
                        let lap_start = splitmix64(&mut rng) % total_lbas;
                        HeatLap {
                            lap_start,
                            scanned_in_lap: 0,
                            rng,
                            ref_lapped: false,
                        }
                    });
                    if lap.scanned_in_lap >= total_lbas {
                        lap.lap_start = splitmix64(&mut lap.rng) % total_lbas;
                        lap.scanned_in_lap = 0;
                        // Stage-5: a full lap into the current fill buffer
                        // completed → this volume satisfies the lap-barrier.
                        lap.ref_lapped = true;
                    }
                    let phys_start = (lap.lap_start + lap.scanned_in_lap) % total_lbas;
                    let lap_remaining = total_lbas - lap.scanned_in_lap;
                    let chunk = want.min(lap_remaining).min(total_lbas - phys_start);
                    lap.scanned_in_lap += chunk;
                    (phys_start, chunk)
                };
                if chunk == 0 {
                    continue;
                }
                let mut bumps = 0u64;
                let scan = meta.scan_blockmap_range(
                    &vol.id,
                    Lba(phys_start),
                    chunk,
                    &mut |lba, value| {
                        if value.is_zero() {
                            return;
                        }
                        for pba in value.physical_pbas(BLOCK_SIZE) {
                            heat.bump(pba);
                            if let Some(buf) = ref_fill.as_deref_mut() {
                                RefBitmap::mark(buf, pba);
                            }
                            bumps += 1;
                        }
                        try_push_cold_tail(
                            cold_tx,
                            push_budget,
                            &mut pushed,
                            metrics,
                            &vol.id,
                            lba,
                            &value,
                        );
                    },
                );
                if let Err(e) = scan {
                    tracing::debug!(vol = %vol.id.0, error = %e, "heat refresh (adaptive): scan_blockmap_range failed");
                }
                remaining = remaining.saturating_sub(chunk);
                scanned_total += chunk;
                bumps_total += bumps;
                cursor.sweep_lbas_done += chunk;
                while cursor.sweep_lbas_done >= target {
                    heat.advance_epoch();
                    metrics
                        .heat_sweeps_completed
                        .fetch_add(1, Ordering::Relaxed);
                    cursor.sweep_lbas_done -= target;
                }
            }
        }

        let mut consecutive_empty = 0usize;
        while !use_adaptive && remaining > 0 && running.load(Ordering::Relaxed) {
            let vol = volumes[cursor.vol_idx % volumes.len()].clone();
            let total_lbas = vol.size_bytes / u64::from(vol.block_size.max(1));
            if total_lbas == 0 {
                cursor.vol_idx = cursor.vol_idx.wrapping_add(1);
                consecutive_empty += 1;
                if consecutive_empty >= volumes.len() {
                    break; // every volume empty this cycle
                }
                continue;
            }
            consecutive_empty = 0;

            // Pick this cycle's contiguous physical range from the per-volume
            // lap (random phase, linear advance, wrap, re-randomize — mirrors
            // the dedup cold-tail cursor so coverage order varies run-to-run).
            let (phys_start, chunk) = {
                let lap = cursor.per_vol.entry(vol.id.0.clone()).or_insert_with(|| {
                    let mut rng = heat_lap_seed(&vol.id.0);
                    let lap_start = splitmix64(&mut rng) % total_lbas;
                    HeatLap {
                        lap_start,
                        scanned_in_lap: 0,
                        rng,
                        ref_lapped: false,
                    }
                });
                if lap.scanned_in_lap >= total_lbas {
                    lap.lap_start = splitmix64(&mut lap.rng) % total_lbas;
                    lap.scanned_in_lap = 0;
                    // Stage-5: a full lap into the current fill buffer completed
                    // → this volume satisfies the lap-barrier.
                    lap.ref_lapped = true;
                }
                let phys_start = (lap.lap_start + lap.scanned_in_lap) % total_lbas;
                let lap_remaining = total_lbas - lap.scanned_in_lap;
                let chunk = remaining.min(lap_remaining).min(total_lbas - phys_start);
                lap.scanned_in_lap += chunk;
                (phys_start, chunk)
            };
            cursor.vol_idx = cursor.vol_idx.wrapping_add(1);
            if chunk == 0 {
                continue;
            }

            let mut bumps = 0u64;
            let scan =
                meta.scan_blockmap_range(&vol.id, Lba(phys_start), chunk, &mut |lba, value| {
                    if value.is_zero() {
                        return;
                    }
                    for pba in value.physical_pbas(BLOCK_SIZE) {
                        heat.bump(pba);
                        if let Some(buf) = ref_fill.as_deref_mut() {
                            RefBitmap::mark(buf, pba);
                        }
                        bumps += 1;
                    }
                    try_push_cold_tail(
                        cold_tx,
                        push_budget,
                        &mut pushed,
                        metrics,
                        &vol.id,
                        lba,
                        &value,
                    );
                });
            if let Err(e) = scan {
                tracing::debug!(vol = %vol.id.0, error = %e, "heat refresh: scan_blockmap_range failed");
                // Still advance the odometer below so a flaky volume cannot
                // wedge the sweep from ever completing.
            }
            remaining = remaining.saturating_sub(chunk);
            scanned_total += chunk;
            bumps_total += bumps;
            cursor.sweep_lbas_done += chunk;

            // A full sweep's worth of LBAs has been walked: publish it by
            // advancing the epoch (which makes the next pass self-reset each
            // bucket on first touch). Done *inside* the loop so a budget larger
            // than the volume set still produces one epoch per sweep instead of
            // accumulating counts across laps.
            while cursor.sweep_lbas_done >= target {
                heat.advance_epoch();
                metrics
                    .heat_sweeps_completed
                    .fetch_add(1, Ordering::Relaxed);
                cursor.sweep_lbas_done -= target;
            }
        }

        // Stage-5 lap-barrier publish. A per-PBA fill buffer is a *complete*
        // cover — every 0 bit provably means "no live mapping was seen" — only
        // once EVERY live volume has completed a full lap into it. The heat
        // epoch is only approximate coverage, so the bitmap rotates on this
        // barrier, NOT on `advance_epoch`. At most one publish per cycle (the
        // check runs once, after the walk). On publish, recycle the buffer and
        // restart every volume's lap so the next fill is covered from scratch (a
        // lap straddling the publish boundary would only partially cover it). A
        // newly-created volume has no `ref_lapped` entry yet → blocks the
        // barrier until it laps; a dropped volume was already removed from
        // `per_vol` above → never blocks. Out-of-window staleness from either is
        // absorbed by requiring K consecutive clean barriers + the Gate-2 scan.
        if let (Some(rb), Some(buf)) = (ref_bitmap, ref_fill.take()) {
            let all_lapped = lap_barrier_satisfied(
                volumes
                    .iter()
                    .map(|v| (v.id.0.as_str(), v.size_bytes / u64::from(v.block_size.max(1)))),
                &cursor.per_vol,
            );
            if all_lapped {
                cursor.ref_fill = Some(rb.publish(buf));
                metrics
                    .dedup_ref_bitmap_published
                    .fetch_add(1, Ordering::Relaxed);
                for lap in cursor.per_vol.values_mut() {
                    lap.scanned_in_lap = 0;
                    lap.ref_lapped = false;
                }
            } else {
                cursor.ref_fill = Some(buf);
            }
        }

        metrics
            .heat_refresh_lbas_scanned
            .fetch_add(scanned_total, Ordering::Relaxed);
        metrics.heat_bumps.fetch_add(bumps_total, Ordering::Relaxed);

        // Publish a fresh summary for status to read O(1). Done here on the
        // single GC refresh thread (bounded 1×/cycle) so the O(n_buckets) scan
        // never fires on a foreground status poll.
        heat.refresh_summary_cache();
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for GcRunner {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Context handed to `reclaim_retired_extents` when Stage-B heat consumption is
/// enabled. Built once per GC cycle from the live `GcConfig`; `None` means
/// reclaim behaves exactly as before (no heat filtering).
pub(crate) struct HeatReclaimCtx<'a> {
    pub heat: &'a HeatMap,
    /// Max region age (sweeps) still trusted as "hot" enough to defer.
    pub fresh_max_age: u32,
    /// Force-confirm every Nth cycle (0 = never force).
    pub force_confirm_interval: u64,
    /// Current GC cycle number (drives the periodic force-confirm).
    pub cycle: u64,
    /// Yield gate: if the measured confirm-scan reclaim yield (‰) is ≥ this,
    /// stop deferring (the scan is productive, not wasted). 0..=1000.
    pub yield_suppress_milli: u32,
    /// Confirm-all every Nth cycle to (re)measure yield even while deferring
    /// (0 = never recalibrate beyond the force-confirm pass).
    pub recalibrate_interval: u64,
    /// Free-space pressure gate: if allocator free% ≤ this, stop deferring
    /// (0 = no pressure gate).
    pub min_free_pct: u64,
}

/// Per-volume lap state for the heat refresh (mirrors the dedup scanner's
/// `ColdTailCursor`): a lap is one full pass over the volume's LBA space,
/// starting at a random phase and advancing linearly, wrapping, then
/// re-randomizing — so coverage order varies run-to-run instead of always
/// sweeping `0..N`.
#[derive(Debug, Clone, Copy)]
struct HeatLap {
    lap_start: u64,
    scanned_in_lap: u64,
    rng: u64,
    /// Stage-5 lap-barrier: set when this volume completes a full lap *into the
    /// current per-PBA fill buffer*. Reset on every publish so the next barrier
    /// requires a fresh full lap (a lap straddling a publish would only
    /// partially cover the new buffer). Unused / always-false when per-PBA
    /// orphan reclaim is off.
    ref_lapped: bool,
}

/// Stage-5 lap-barrier predicate: has every live volume completed a full lap
/// into the current per-PBA fill buffer? Each item is `(volume_id, total_lbas)`.
/// A zero-length volume is trivially covered; a volume with no lap entry (never
/// walked into this fill — e.g. just created) is NOT covered → blocks the
/// barrier until it laps. A dropped volume is simply absent from `vols`.
fn lap_barrier_satisfied<'a>(
    vols: impl Iterator<Item = (&'a str, u64)>,
    per_vol: &HashMap<String, HeatLap>,
) -> bool {
    vols.into_iter()
        .all(|(id, total)| total == 0 || per_vol.get(id).is_some_and(|l| l.ref_lapped))
}

/// Global cross-volume cursor + sweep odometer for the heat refresh. Lives on
/// the GC thread stack (single-writer), so the heat-map epoch tick and all
/// bumps are serialized.
#[derive(Default)]
struct HeatCursor {
    per_vol: HashMap<String, HeatLap>,
    vol_idx: usize,
    /// LBAs walked toward the current sweep; laps the sweep `target` each time
    /// it reaches it (carrying the remainder).
    sweep_lbas_done: u64,
    /// Stage-B2: per-volume `write_bytes` at the previous adaptive cycle, so the
    /// churn weight is the *delta* (recent write activity), not the lifetime sum.
    last_write_bytes: HashMap<String, u64>,
    /// Stage-5: the in-progress per-PBA referenced fill buffer (GC-thread-local,
    /// single-writer). Accumulates referenced bits across cycles until the
    /// lap-barrier fires, then is handed to `RefBitmap::publish`. `None` when
    /// per-PBA orphan reclaim is off, or transiently while taken out for a walk.
    ref_fill: Option<Box<[u64]>>,
}

/// Per-volume lap state for the resident compactor scan (mirrors `HeatLap`):
/// a random-phase linear sweep across the volume's LBA space, wrapping and
/// re-randomizing the phase each lap so coverage order varies run-to-run.
#[derive(Debug, Clone, Copy)]
struct CompactorLap {
    lap_start: u64,
    scanned_in_lap: u64,
    rng: u64,
}

/// Cross-volume cursor + sweep odometer + debt accumulator for the resident
/// compactor. Lives on the GC thread stack (single writer), persists across
/// cycles so the bounded per-cycle window sweeps the whole L2P over time.
#[derive(Default)]
struct CompactorCursor {
    per_vol: HashMap<String, CompactorLap>,
    vol_idx: usize,
    /// LBAs walked toward the current full sweep; laps the sweep `target`.
    sweep_lbas_done: u64,
    /// Compactable-dead-block estimate accumulated over the in-progress sweep;
    /// published to `gc_compactable_dead_blocks` and reset on sweep completion.
    dead_estimate_acc: u64,
}

/// Per-cycle compactor effort in `[0,1]`: `max(idle_factor, urgency)`.
///
/// `idle_factor = (max_pct - fill_pct)/max_pct` — 1.0 when the buffer is empty,
/// ramping to 0 at `fill_pct >= buffer_usage_max_pct` (back off under foreground
/// load). `urgency = (URGENCY_FREE_PCT - free_pct)/URGENCY_FREE_PCT` — 0 when
/// space is plentiful, ramping to 1 as free space drops. Taking the `max` lets
/// space pressure override the idle backoff so the device cannot silently fill.
/// `buffer_usage_max_pct == 0` disables the buffer throttle (idle_factor = 1).
fn compute_effort(fill_pct: u8, free_pct: u64, buffer_usage_max_pct: u8) -> f64 {
    let max = f64::from(buffer_usage_max_pct);
    let idle_factor = if max <= 0.0 {
        1.0
    } else {
        ((max - f64::from(fill_pct)) / max).clamp(0.0, 1.0)
    };
    let floor = URGENCY_FREE_PCT as f64;
    let urgency = ((floor - free_pct as f64) / floor).clamp(0.0, 1.0);
    idle_factor.max(urgency)
}

/// Advance the compactor sweep odometer by `chunk`. When a full sweep
/// (`target` LBAs) is crossed, return the accumulated debt estimate to publish
/// and reset the accumulator (so the gauge reflects the just-finished sweep,
/// not a running sum). `target == 0` never publishes. Multiple wraps in one call
/// publish once (the accumulated value), then zero for the rest of that call.
fn advance_sweep(cursor: &mut CompactorCursor, chunk: u64, target: u64) -> Option<u64> {
    if target == 0 {
        return None;
    }
    cursor.sweep_lbas_done += chunk;
    let mut published = None;
    while cursor.sweep_lbas_done >= target {
        published = Some(cursor.dead_estimate_acc);
        cursor.dead_estimate_acc = 0;
        cursor.sweep_lbas_done -= target;
    }
    published
}

/// Pick this cycle's contiguous LBA window from a compactor lap: random-phase
/// linear sweep, wrapping at the volume end, re-randomizing the phase when a
/// full lap completes. Mirrors the heat-refresh lap. Returns `(phys_start,
/// chunk)`; `chunk == 0` only when `total_lbas == 0`. Advances the lap.
fn next_compactor_window(lap: &mut CompactorLap, total_lbas: u64, budget: u64) -> (u64, u64) {
    if total_lbas == 0 {
        return (0, 0);
    }
    if lap.scanned_in_lap >= total_lbas {
        lap.lap_start = splitmix64(&mut lap.rng) % total_lbas;
        lap.scanned_in_lap = 0;
    }
    let phys_start = (lap.lap_start + lap.scanned_in_lap) % total_lbas;
    let lap_remaining = total_lbas - lap.scanned_in_lap;
    let chunk = budget.min(lap_remaining).min(total_lbas - phys_start);
    lap.scanned_in_lap += chunk;
    (phys_start, chunk)
}

/// Stage-B2 adaptive refresh budget split. Each volume gets a guaranteed floor
/// (its size-proportional share divided by `staleness_floor`, so it is fully
/// covered at least every `staleness_floor` sweeps even at zero churn) plus a
/// bonus from the remaining budget proportional to its recent write churn.
/// Falls back to size-proportional (uniform) when no volume churned. Returns one
/// sub-budget (LBAs) per input volume; the sum is ≈ `budget` (integer-rounding
/// may drop a few LBAs, harmless for a prior).
fn split_refresh_budget(
    vol_lbas: &[u64],
    churn: &[u64],
    budget: u64,
    staleness_floor: u32,
) -> Vec<u64> {
    let n = vol_lbas.len();
    let total_lbas: u128 = vol_lbas.iter().map(|&l| l as u128).sum();
    if n == 0 || total_lbas == 0 || budget == 0 {
        return vec![0; n];
    }
    let budget = budget as u128;
    // Reserve `budget / staleness_floor` for guaranteed coverage; the rest is the
    // churn bonus pool. floor>=1 keeps the whole budget as bonus when no floor.
    let floor_div = u128::from(staleness_floor.max(1));
    let floor_pool = budget / floor_div;
    let bonus_pool = budget - floor_pool;
    let churn_sum: u128 = churn.iter().map(|&c| c as u128).sum();
    (0..n)
        .map(|i| {
            let lbas = vol_lbas[i] as u128;
            if lbas == 0 {
                return 0;
            }
            let floor_i = floor_pool * lbas / total_lbas;
            let bonus_i = if churn_sum > 0 {
                bonus_pool * churn[i] as u128 / churn_sum
            } else {
                bonus_pool * lbas / total_lbas
            };
            (floor_i + bonus_i) as u64
        })
        .collect()
}

/// splitmix64: a tiny, self-contained PRNG. The heat lap phase only needs
/// decorrelated coverage order, not crypto strength. (Kept local rather than
/// shared with the dedup scanner's private copy.)
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Seed a per-volume PRNG from wall-clock time mixed with the volume id
/// (FNV-1a), so different volumes and different process runs pick different
/// lap phases.
fn heat_lap_seed(vol_id: &str) -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in vol_id.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    nanos ^ h
}

/// Stage-4 fold producer: emit one cold candidate from the heat walk into the
/// fold channel, bounded by `push_budget` per cycle. No-op when the fold is
/// off (`cold_tx` is None) or the per-cycle budget is spent. `try_send` never
/// blocks the GC cycle; a full or disconnected channel just drops the
/// candidate (counted in `gc_heat_cold_tail_dropped`) — dropping only costs
/// dedup ratio, never correctness, because the consumer re-validates every
/// target before acting and the producer re-emits fresh cold entries each
/// cycle.
#[inline]
fn try_push_cold_tail(
    cold_tx: Option<&Sender<ColdTailTarget>>,
    push_budget: usize,
    pushed: &mut usize,
    metrics: &EngineMetrics,
    vol_id: &VolumeId,
    lba: Lba,
    value: &BlockmapValue,
) {
    let Some(tx) = cold_tx else {
        return;
    };
    if *pushed >= push_budget {
        return;
    }
    // FLAG_DEDUP_SKIPPED entries are owned by the dedup scanner's
    // DEDUP_SKIPPED backfill path (their content hashes are not yet known);
    // the legacy cold-tail scan skips them too, so the fold must match.
    if value.flags & FLAG_DEDUP_SKIPPED != 0 {
        return;
    }
    match tx.try_send(ColdTailTarget {
        vol_id: vol_id.clone(),
        lba,
        bv: *value,
    }) {
        Ok(()) => {
            *pushed += 1;
            metrics
                .gc_heat_cold_tail_pushed
                .fetch_add(1, Ordering::Relaxed);
        }
        Err(_) => {
            metrics
                .gc_heat_cold_tail_dropped
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        advance_sweep, compute_effort, lap_barrier_satisfied, next_compactor_window,
        split_refresh_budget, CompactorCursor, CompactorLap, HeatLap,
    };
    use std::collections::HashMap;

    // ---- resident compactor: effort pacing ----

    #[test]
    fn effort_idle_plentiful_is_full() {
        // Empty buffer (fill 0), tons of free space → full effort.
        assert!((compute_effort(0, 99, 80) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn effort_busy_plentiful_is_zero() {
        // Buffer at/above the max-pct, plenty of space → no compaction.
        assert_eq!(compute_effort(80, 99, 80), 0.0);
        assert_eq!(compute_effort(100, 99, 80), 0.0);
    }

    #[test]
    fn effort_busy_but_tight_is_urgency_driven() {
        // Buffer full but space critical → urgency overrides the idle backoff.
        // free_pct=0 → urgency=1.0 regardless of fill.
        assert!((compute_effort(100, 0, 80) - 1.0).abs() < 1e-9);
        // free_pct=25 → urgency=(50-25)/50=0.5; idle_factor=0 at fill=100 → 0.5.
        assert!((compute_effort(100, 25, 80) - 0.5).abs() < 1e-9);
    }

    #[test]
    fn effort_partial_idle_ramps() {
        // fill=40, max=80 → idle_factor=0.5; plentiful → urgency 0 → 0.5.
        assert!((compute_effort(40, 99, 80) - 0.5).abs() < 1e-9);
    }

    #[test]
    fn effort_zero_max_disables_buffer_throttle() {
        // buffer_usage_max_pct==0 → idle_factor pinned to 1.0 (never throttle on buffer).
        assert!((compute_effort(100, 99, 0) - 1.0).abs() < 1e-9);
    }

    // ---- resident compactor: dynamic threshold curve ----

    #[test]
    fn dynamic_threshold_curve() {
        let mut cfg = crate::gc::config::GcConfig::default();
        cfg.compactor_resident_threshold = 0.85;
        cfg.dead_ratio_threshold = 0.25;
        // Resident tier (plentiful): only clearly-dead units.
        assert_eq!(super::GcRunner::dynamic_threshold(&cfg, 99), 0.85);
        assert_eq!(super::GcRunner::dynamic_threshold(&cfg, 51), 0.85);
        // Pressure ladder.
        assert_eq!(super::GcRunner::dynamic_threshold(&cfg, 40), 0.50);
        assert_eq!(super::GcRunner::dynamic_threshold(&cfg, 20), 0.30);
        assert_eq!(super::GcRunner::dynamic_threshold(&cfg, 5), 0.25);
    }

    // ---- resident compactor: lap window ----

    fn clap() -> CompactorLap {
        CompactorLap {
            lap_start: 0,
            scanned_in_lap: 0,
            rng: 0x1234_5678,
        }
    }

    #[test]
    fn next_window_zero_volume_is_empty() {
        let mut lap = clap();
        assert_eq!(next_compactor_window(&mut lap, 0, 1000), (0, 0));
    }

    #[test]
    fn next_window_linear_advance_and_wrap() {
        let total = 1000u64;
        let budget = 400u64;
        let mut lap = clap();
        lap.lap_start = 0; // deterministic start for the assert
        let (s1, c1) = next_compactor_window(&mut lap, total, budget);
        assert_eq!((s1, c1), (0, 400));
        let (s2, c2) = next_compactor_window(&mut lap, total, budget);
        assert_eq!((s2, c2), (400, 400));
        // Third window clamps to the lap remainder (1000 - 800 = 200).
        let (s3, c3) = next_compactor_window(&mut lap, total, budget);
        assert_eq!((s3, c3), (800, 200));
        // Lap complete → next call re-randomizes the phase and resets.
        let before = lap.lap_start;
        let (_s4, c4) = next_compactor_window(&mut lap, total, budget);
        assert!(c4 > 0);
        assert_eq!(lap.scanned_in_lap, c4, "lap restarted");
        // Phase moved (re-randomized) — overwhelmingly likely with splitmix64.
        assert_ne!(lap.lap_start, before + 800);
    }

    #[test]
    fn next_window_phase_always_in_range() {
        let total = 777u64;
        let mut lap = clap();
        let mut scanned = 0u64;
        for _ in 0..50 {
            let (start, chunk) = next_compactor_window(&mut lap, total, 100);
            assert!(start < total, "phase {start} must be < {total}");
            assert!(chunk <= total);
            scanned += chunk;
        }
        assert!(scanned > 0);
    }

    // ---- resident compactor: debt odometer ----

    #[test]
    fn advance_sweep_publishes_and_resets_on_wrap() {
        let mut cur = CompactorCursor::default();
        cur.dead_estimate_acc = 1234;
        // Below target → no publish, accumulator intact.
        assert_eq!(advance_sweep(&mut cur, 300, 1000), None);
        assert_eq!(cur.dead_estimate_acc, 1234);
        assert_eq!(cur.sweep_lbas_done, 300);
        // Crossing target → publish the accumulated value, reset to 0.
        assert_eq!(advance_sweep(&mut cur, 800, 1000), Some(1234));
        assert_eq!(cur.dead_estimate_acc, 0);
        assert_eq!(cur.sweep_lbas_done, 100); // 1100 - 1000 carried
    }

    #[test]
    fn advance_sweep_target_zero_never_publishes() {
        let mut cur = CompactorCursor::default();
        cur.dead_estimate_acc = 5;
        assert_eq!(advance_sweep(&mut cur, 1_000_000, 0), None);
        assert_eq!(cur.dead_estimate_acc, 5);
    }

    fn lap(ref_lapped: bool) -> HeatLap {
        HeatLap {
            lap_start: 0,
            scanned_in_lap: 0,
            rng: 1,
            ref_lapped,
        }
    }

    #[test]
    fn lap_barrier_waits_for_all_volumes() {
        let mut per_vol: HashMap<String, HeatLap> = HashMap::new();
        per_vol.insert("a".into(), lap(true));
        // "b" present but not yet lapped → barrier NOT satisfied.
        per_vol.insert("b".into(), lap(false));
        let vols = || [("a", 100u64), ("b", 100u64)].into_iter();
        assert!(!lap_barrier_satisfied(vols(), &per_vol));
        // Both lapped → satisfied.
        per_vol.insert("b".into(), lap(true));
        assert!(lap_barrier_satisfied(vols(), &per_vol));
    }

    #[test]
    fn lap_barrier_missing_volume_blocks() {
        // A volume with no lap entry yet (e.g. just created, never walked into
        // this fill) must block the barrier until it laps.
        let mut per_vol: HashMap<String, HeatLap> = HashMap::new();
        per_vol.insert("a".into(), lap(true));
        let vols = [("a", 100u64), ("new", 100u64)];
        assert!(!lap_barrier_satisfied(vols.into_iter(), &per_vol));
    }

    #[test]
    fn lap_barrier_zero_length_volume_is_trivially_covered() {
        let mut per_vol: HashMap<String, HeatLap> = HashMap::new();
        per_vol.insert("a".into(), lap(true));
        // "z" has zero LBAs → no live mappings possible → does not block.
        let vols = [("a", 100u64), ("z", 0u64)];
        assert!(lap_barrier_satisfied(vols.into_iter(), &per_vol));
    }

    #[test]
    fn split_budget_no_churn_is_uniform() {
        // Equal sizes, no churn → ~equal split summing to ≈ budget.
        let sub = split_refresh_budget(&[1000, 1000], &[0, 0], 1000, 4);
        assert!(
            sub[0].abs_diff(sub[1]) <= 1,
            "uniform when no churn: {sub:?}"
        );
        let s: u64 = sub.iter().sum();
        assert!((990..=1000).contains(&s), "≈ budget: {sub:?}");
    }

    #[test]
    fn split_budget_biases_to_churn_but_keeps_floor() {
        // Equal sizes; only vol1 churned. floor pool = 1000/4 = 250 (125 each by
        // size); bonus pool 750 → all to vol1. Idle vol0 still gets its floor.
        let sub = split_refresh_budget(&[1000, 1000], &[0, 1_000_000], 1000, 4);
        assert!(sub[1] > sub[0], "churning volume gets more: {sub:?}");
        assert!(
            (100..=150).contains(&sub[0]),
            "idle volume keeps its staleness floor (~125): {sub:?}"
        );
    }

    #[test]
    fn split_budget_floor_scales_with_staleness() {
        // staleness_floor=1 → floor pool = whole budget → pure size-proportional
        // (idle vol gets half); larger floor divisor shrinks the guaranteed floor.
        let s4 = split_refresh_budget(&[1000, 1000], &[0, 1_000_000], 1000, 4);
        let s1 = split_refresh_budget(&[1000, 1000], &[0, 1_000_000], 1000, 1);
        assert_eq!(
            s1,
            vec![500, 500],
            "floor=1 ignores churn (all floor): {s1:?}"
        );
        assert!(s1[0] > s4[0], "smaller divisor → bigger guaranteed floor");
    }

    #[test]
    fn split_budget_handles_zero_size_and_empty() {
        assert_eq!(split_refresh_budget(&[], &[], 1000, 4), Vec::<u64>::new());
        assert_eq!(split_refresh_budget(&[0, 0], &[5, 5], 1000, 4), vec![0, 0]);
        let sub = split_refresh_budget(&[0, 1000], &[0, 0], 1000, 4);
        assert_eq!(sub[0], 0, "zero-size volume gets no budget");
        assert!(sub[1] >= 990, "the live volume gets ≈ all of it: {sub:?}");
    }
}

// Heavy-setup integration-style unit tests that need a real MetaStore live in
// their own module so the imports don't leak into the pure-function tests.
#[cfg(test)]
mod reclaim_consistency_tests {
    use super::GcRunner;
    use crate::config::MetaConfig;
    use crate::dedup::CandidateCache;
    use crate::meta::store::MetaStore;
    use crate::metrics::EngineMetrics;
    use crate::space::allocator::SpaceAllocator;
    use crate::space::extent::Extent;
    use crate::space::pba_lifecycle::PbaLifecycle;
    use crate::types::Pba;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    fn rc_auth_meta() -> (tempfile::TempDir, Arc<MetaStore>) {
        let dir = tempfile::tempdir().unwrap();
        let cfg = MetaConfig {
            path: Some(dir.path().to_path_buf()),
            block_cache_mb: 8,
            memtable_budget_mb: 0,
            index_pin_mb: 0,
            lsm_bloom_bits_per_entry: 10,
            checkpoint_interval_ms: 5000,
            group_commit_timeout_us: 1,
            wal_dir: None,
            dedup_shards: 8,
            dedup_cuckoo_buckets: 1_000_000,
            dedup_l1_cache_entries: 256_000,
            rc_authoritative_reclaim: true,
            ..MetaConfig::default()
        };
        let meta = Arc::new(MetaStore::open(&cfg).unwrap());
        assert!(
            meta.rc_authoritative_reclaim(),
            "test requires rc_authoritative mode"
        );
        (dir, meta)
    }

    #[test]
    fn rc_auth_reclaim_frees_rc_zero_but_never_referenced_extent() {
        let (_dir, meta) = rc_auth_meta();
        let allocator = Arc::new(SpaceAllocator::new(4096 * 4096, 0));
        let metrics = Arc::new(EngineMetrics::default());
        let candidate = CandidateCache::new(1, 1);
        let lifecycle = PbaLifecycle::new(allocator.clone(), candidate, metrics.clone());

        // Allocate a run and pick two NON-adjacent PBAs so the retired set
        // does not coalesce them into one mixed-rc extent.
        let pbas: Vec<Pba> = (0..6).map(|_| allocator.allocate_one().unwrap()).collect();
        let q = pbas[0]; // genuinely unreferenced (rc stays 0)
        let p = pbas[5]; // referenced (rc=2)
        assert!(p.0 - q.0 >= 2, "q and p must be non-adjacent");
        meta.set_refcount(p, 2).unwrap();
        assert_eq!(meta.multi_get_refcounts_consistent(&[q, p]).unwrap(), vec![0, 2]);

        // Retire both into the allocator's retired set (non-adjacent → two
        // distinct retired extents).
        assert!(allocator.retire_one(q).unwrap() > 0);
        assert!(allocator.retire_one(p).unwrap() > 0);
        assert!(allocator.is_retired(q) && allocator.is_retired(p));

        let running = AtomicBool::new(true);
        let reclaimed = GcRunner::reclaim_retired_extents(
            &metrics,
            &meta,
            &allocator,
            &lifecycle,
            64,
            &running,
            None, // heat_ctx
            None, // grace: immediate (test)
        );

        // Q (rc==0) freed via the consistent read; P (rc>0) left retired.
        assert_eq!(reclaimed, 1, "exactly the rc==0 extent reclaimed");
        assert!(allocator.is_free(q), "rc==0 retired extent must be reclaimed");
        assert!(
            allocator.is_retired(p),
            "rc>0 extent must NOT be freed under rc_authoritative reclaim"
        );
        assert!(
            !allocator.is_free(p),
            "referenced PBA must never return to the free list (premature-free CRC)"
        );
        assert_eq!(
            metrics
                .gc_retired_blocks_reclaimed
                .load(Ordering::Relaxed),
            1
        );
    }

    /// A retired extent whose every PBA reads rc==0 consistently is freed even
    /// when the batch mixes shards (multi_get_refcounts_consistent groups by
    /// shard and takes each shard's fold_lock).
    #[test]
    fn rc_auth_reclaim_multi_block_extent_all_zero_is_freed() {
        let (_dir, meta) = rc_auth_meta();
        let allocator = Arc::new(SpaceAllocator::new(4096 * 4096, 0));
        let metrics = Arc::new(EngineMetrics::default());
        let candidate = CandidateCache::new(1, 1);
        let lifecycle = PbaLifecycle::new(allocator.clone(), candidate, metrics.clone());

        // A contiguous 4-block extent, all rc==0.
        let base = allocator.allocate_one().unwrap();
        for _ in 0..3 {
            allocator.allocate_one().unwrap();
        }
        let extent = Extent {
            start: base,
            count: 4,
        };
        assert!(allocator.retire_extent(extent).unwrap() > 0);

        let running = AtomicBool::new(true);
        let reclaimed = GcRunner::reclaim_retired_extents(
            &metrics, &meta, &allocator, &lifecycle, 64, &running, None, None,
        );
        assert_eq!(reclaimed, 4);
        for off in 0..4 {
            assert!(allocator.is_free(Pba(base.0 + off)));
        }
    }
}
