use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arc_swap::ArcSwap;
use crossbeam_channel::Sender;

use crate::buffer::pool::WriteBufferPool;
use crate::dedup::ColdTailTarget;
use crate::gc::config::GcConfig;
use crate::gc::heatmap::HeatMap;
use crate::gc::ref_bitmap::RefBitmap;
use crate::gc::rewriter::rewrite_candidate;
use crate::gc::scanner::scan_gc_candidates;
use crate::io::engine::IoEngine;
use crate::lifecycle::VolumeLifecycleManager;
use crate::meta::schema::{BlockmapValue, FLAG_DEDUP_SKIPPED};
use crate::meta::store::MetaStore;
use crate::metrics::EngineMetrics;
use crate::space::allocator::SpaceAllocator;
use crate::space::pba_lifecycle::PbaLifecycle;
use crate::space::extent::Extent;
use crate::types::{Lba, VolumeId, BLOCK_SIZE};

const MAX_RETIRED_RECLAIM_PER_CYCLE: usize = 4096;

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

    /// Compute dynamic dead_ratio_threshold based on space pressure.
    ///
    /// When space is plentiful, only reclaim heavily fragmented slots.
    /// As space gets tighter, lower the threshold to reclaim more aggressively.
    fn dynamic_threshold(cfg: &GcConfig, allocator: &SpaceAllocator) -> Option<f64> {
        let total = allocator.total_block_count();
        if total == 0 {
            return Some(cfg.dead_ratio_threshold);
        }
        let free_pct = (allocator.free_block_count() * 100) / total;

        if free_pct > 50 {
            None // Plentiful — do not scan the whole blockmap just to compact.
        } else if free_pct > 30 {
            Some(0.50) // Moderate pressure
        } else if free_pct > 10 {
            Some(0.30) // Getting tight
        } else {
            Some(cfg.dead_ratio_threshold) // Critical — use configured minimum (default 0.25)
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
        let mut paused = false;
        let mut heat_cursor = HeatCursor::default();

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

            Self::reclaim_retired_extents(
                metrics,
                meta,
                allocator,
                pba_lifecycle,
                MAX_RETIRED_RECLAIM_PER_CYCLE,
                running,
                heat_ctx,
            );

            // Standing background heat-map refresh (observe-only, Stage A):
            // a bounded, lock-free-per-chunk slow scan that accumulates a
            // per-PBA-region live-mapping count. Runs even when rewrite GC is
            // disabled and even when reclaim found nothing — it is decoupled
            // from reclaim having work. Front-end IO never pays for it.
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

            if !cfg.enabled {
                continue;
            }

            // Back-pressure: check buffer usage
            let fill_pct = buffer_pool.fill_percentage();
            if fill_pct > cfg.buffer_usage_max_pct {
                metrics.gc_paused_cycles.fetch_add(1, Ordering::Relaxed);
                if !paused {
                    tracing::debug!(
                        fill_pct,
                        max = cfg.buffer_usage_max_pct,
                        "gc: pausing due to high buffer usage"
                    );
                    paused = true;
                }
                continue;
            }
            if paused && fill_pct <= cfg.buffer_usage_resume_pct {
                tracing::debug!(
                    fill_pct,
                    resume = cfg.buffer_usage_resume_pct,
                    "gc: resuming"
                );
                paused = false;
            }
            if paused {
                continue;
            }

            // Smart GC: dynamic dead_ratio_threshold based on space pressure.
            // More aggressive reclamation when space is tight.
            let Some(threshold) = Self::dynamic_threshold(&cfg, allocator) else {
                metrics.gc_paused_cycles.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("gc: skipping scan while free space is plentiful");
                continue;
            };

            // Scan for GC rewrite candidates
            let candidates = match scan_gc_candidates(meta, threshold, cfg.max_rewrite_per_cycle) {
                Ok(c) => c,
                Err(e) => {
                    metrics.gc_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(error = %e, "gc: scan failed");
                    continue;
                }
            };

            metrics
                .gc_candidates_found
                .fetch_add(candidates.len() as u64, Ordering::Relaxed);

            if candidates.is_empty() {
                continue;
            }

            tracing::debug!(
                candidates = candidates.len(),
                "gc: found candidates for reclamation"
            );

            for candidate in &candidates {
                if !running.load(Ordering::Relaxed) {
                    break;
                }

                // Re-check back-pressure before each candidate (re-load config for latest thresholds)
                let cfg = config.load();
                if buffer_pool.fill_percentage() > cfg.buffer_usage_max_pct {
                    metrics.gc_paused_cycles.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!("gc: pausing mid-cycle due to buffer pressure");
                    paused = true;
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
                            "gc: failed to rewrite candidate"
                        );
                    }
                }
            }
        }
    }

    pub(crate) fn reclaim_retired_extents(
        metrics: &EngineMetrics,
        meta: &MetaStore,
        allocator: &SpaceAllocator,
        pba_lifecycle: &PbaLifecycle,
        limit: usize,
        running: &AtomicBool,
        heat_ctx: Option<HeatReclaimCtx<'_>>,
    ) -> usize {
        let candidates = allocator.retired_candidates(limit);
        if candidates.is_empty() {
            return 0;
        }

        // Gate 1 (refcount): keep only candidates whose every PBA has rc==0.
        // Cheap per-extent paged-array / overlay lookups, no volume scan.
        let mut survivors: Vec<Extent> = Vec::with_capacity(candidates.len());
        for extent in candidates {
            if !running.load(Ordering::Relaxed) {
                break;
            }
            let pbas: Vec<crate::types::Pba> = (0..extent.count)
                .map(|offset| crate::types::Pba(extent.start.0 + offset as u64))
                .collect();
            match meta.multi_get_refcounts(&pbas) {
                Ok(refcounts) => {
                    if refcounts.into_iter().all(|refcount| refcount == 0) {
                        survivors.push(extent);
                    }
                }
                Err(e) => {
                    metrics.gc_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        pba = extent.start.0,
                        blocks = extent.count,
                        error = %e,
                        "gc: failed to read refcount for retired physical extent"
                    );
                }
            }
        }
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

        // Gate 2 (blockmap): ONE batched all-volume L2P scan for every survivor,
        // replacing the per-extent full scan (was O(retired × all_L2P)). A
        // survivor is reclaimable iff no live blockmap entry references any PBA
        // inside it.
        let extents: Vec<(crate::types::Pba, u32)> =
            survivors.iter().map(|e| (e.start, e.count)).collect();
        metrics
            .gc_reclaim_blockmap_scans
            .fetch_add(1, Ordering::Relaxed);
        let referenced = match meta.referenced_extents(&extents) {
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
        };

        let mut reclaimed = 0usize;
        let mut reclaimed_extents = 0usize;
        for (extent, is_referenced) in survivors.into_iter().zip(referenced) {
            if !running.load(Ordering::Relaxed) {
                break;
            }
            if is_referenced {
                continue;
            }
            match pba_lifecycle.confirm_and_reclaim(extent) {
                Ok(true) => {
                    reclaimed += extent.count as usize;
                    reclaimed_extents += 1;
                }
                Ok(false) => {}
                Err(e) => {
                    metrics.gc_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        pba = extent.start.0,
                        blocks = extent.count,
                        error = %e,
                        "gc: failed to release retired physical extent"
                    );
                }
            }
        }

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
        reclaimed
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
    use super::{lap_barrier_satisfied, split_refresh_budget, HeatLap};
    use std::collections::HashMap;

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
