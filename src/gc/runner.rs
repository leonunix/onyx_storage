use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arc_swap::ArcSwap;

use crate::buffer::pool::WriteBufferPool;
use crate::gc::config::GcConfig;
use crate::gc::heatmap::HeatMap;
use crate::gc::rewriter::rewrite_candidate;
use crate::gc::scanner::scan_gc_candidates;
use crate::io::engine::IoEngine;
use crate::lifecycle::VolumeLifecycleManager;
use crate::meta::store::MetaStore;
use crate::metrics::EngineMetrics;
use crate::space::allocator::SpaceAllocator;
use crate::space::extent::Extent;
use crate::types::{Lba, BLOCK_SIZE};

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
        Self::start_with_metrics(
            Arc::new(EngineMetrics::default()),
            meta,
            io_engine,
            buffer_pool,
            lifecycle,
            allocator,
            heat,
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
                })
            } else {
                None
            };

            Self::reclaim_retired_extents(
                metrics,
                meta,
                allocator,
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
                Self::heat_refresh_step(
                    heat,
                    metrics,
                    meta,
                    &mut heat_cursor,
                    cfg.heat_refresh_max_lbas_per_cycle,
                    running,
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
        // whole region still looks hot+fresh — it is very likely still
        // referenced, so a confirm scan would only say "referenced". Deferring
        // = simply not reclaiming it this cycle; it stays in the allocator's
        // retired set and is re-presented next cycle. The free decision is
        // unchanged (a block is still freed iff the confirm scan says no live
        // ref AND the rc/retire gate allows it) — heat only changes *whether we
        // bother to scan*, so staleness can only ever delay reclaim.
        let survivors = if let Some(ctx) = &heat_ctx {
            let forced = ctx.force_confirm_interval != 0
                && ctx.cycle % ctx.force_confirm_interval == 0;
            if forced {
                // Periodic belt-and-suspenders: confirm everything regardless
                // of heat so no deferred extent is starved.
                metrics
                    .gc_heat_force_confirm_passes
                    .fetch_add(1, Ordering::Relaxed);
                survivors
            } else {
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
            }
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
                metrics.gc_heat_scans_skipped.fetch_add(1, Ordering::Relaxed);
                return 0;
            }
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
        for (extent, is_referenced) in survivors.into_iter().zip(referenced) {
            if !running.load(Ordering::Relaxed) {
                break;
            }
            if is_referenced {
                continue;
            }
            match allocator.reclaim_retired_extent(extent) {
                Ok(true) => {
                    reclaimed += extent.count as usize;
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
    fn heat_refresh_step(
        heat: &HeatMap,
        metrics: &EngineMetrics,
        meta: &MetaStore,
        cursor: &mut HeatCursor,
        budget: u64,
        running: &AtomicBool,
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

        // Walk at most one full sweep's worth of LBAs per cycle. When the
        // budget exceeds the whole volume set (small datasets), re-lapping the
        // same live mappings many times in one cycle is pure waste and would
        // hammer metadb read locks, starving the flusher/rewriter. In
        // production `target` (billions of LBAs) dwarfs the budget, so this cap
        // is inert and a sweep spans many cycles.
        let mut remaining = budget.min(target);
        let mut scanned_total = 0u64;
        let mut bumps_total = 0u64;
        let mut consecutive_empty = 0usize;
        while remaining > 0 && running.load(Ordering::Relaxed) {
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
                    }
                });
                if lap.scanned_in_lap >= total_lbas {
                    lap.lap_start = splitmix64(&mut lap.rng) % total_lbas;
                    lap.scanned_in_lap = 0;
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
            let scan = meta.scan_blockmap_range(&vol.id, Lba(phys_start), chunk, &mut |_lba, value| {
                if value.is_zero() {
                    return;
                }
                for pba in value.physical_pbas(BLOCK_SIZE) {
                    heat.bump(pba);
                    bumps += 1;
                }
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
                metrics.heat_sweeps_completed.fetch_add(1, Ordering::Relaxed);
                cursor.sweep_lbas_done -= target;
            }
        }

        metrics
            .heat_refresh_lbas_scanned
            .fetch_add(scanned_total, Ordering::Relaxed);
        metrics.heat_bumps.fetch_add(bumps_total, Ordering::Relaxed);
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
