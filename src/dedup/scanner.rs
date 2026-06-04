use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arc_swap::ArcSwap;
use crossbeam_channel::Receiver;

use crate::buffer::flush::BufferFlusher;
use crate::buffer::pool::WriteBufferPool;
use crate::compress::codec::create_compressor;
use crate::dedup::config::DedupConfig;
use crate::dedup::{CandidateCache, ColdTailTarget};
use crate::error::OnyxResult;
use crate::gc::heatmap::HeatMap;
use crate::gc::ref_bitmap::RefBitmap;
use crate::io::engine::IoEngine;
use crate::io::read_pool::{ReadPool, ReadPurpose};
use crate::lifecycle::VolumeLifecycleManager;
use crate::meta::schema::*;
use crate::meta::store::MetaStore;
use crate::metrics::EngineMetrics;
use crate::space::allocator::SpaceAllocator;
use crate::types::{Lba, VolumeId, BLOCK_SIZE};

/// Background dedup scanner.
///
/// Runs two complementary passes per cycle:
///
/// 1. **DEDUP_SKIPPED rescan**: foreground writes that bypassed dedup
///    under buffer pressure get the `FLAG_DEDUP_SKIPPED` flag in their
///    blockmap. The scanner drains them: read LV3, hash the 4 KiB
///    block, look up the persistent dedup index, and either remap the
///    LBA to the existing live PBA (hit) or warm the candidate cache
///    so a *future* duplicate write triggers verify-and-promote.
///    Misses do **not** publish into `dedup_index` directly — that
///    would defeat the promote-on-verified-hit invariant introduced on
///    the dedup-promote-on-hit branch.
///
/// 2. **Cold-tail warming**: a per-volume cursor walks live blockmap
///    entries that are not flagged DEDUP_SKIPPED, hashes their
///    content, and inserts the fingerprint into the candidate cache.
///    This recovers dedup ratio after a process restart (the cache is
///    RAM-only) and on long-running engines whose dedup window has
///    moved past entries the writer originally cached. LV3 reads are
///    fanned out through the engine's `ReadPool` (io_uring batched at
///    high queue depth) so the cycle's IO is amortised across the
///    drain instead of paying serial round-trips per block.
pub struct DedupScanner {
    running: Arc<AtomicBool>,
    config: Arc<ArcSwap<DedupConfig>>,
    handle: Option<JoinHandle<()>>,
}

impl DedupScanner {
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        meta: Arc<MetaStore>,
        io_engine: Arc<IoEngine>,
        allocator: Arc<SpaceAllocator>,
        lifecycle: Arc<VolumeLifecycleManager>,
        buffer_pool: Arc<WriteBufferPool>,
        candidate: CandidateCache,
        read_pool: Option<Arc<ReadPool>>,
        cold_rx: Option<Receiver<ColdTailTarget>>,
        heat: Option<HeatMap>,
        ref_bitmap: Option<RefBitmap>,
        config: DedupConfig,
    ) -> Self {
        Self::start_with_metrics(
            Arc::new(EngineMetrics::default()),
            meta,
            io_engine,
            allocator,
            lifecycle,
            buffer_pool,
            candidate,
            read_pool,
            cold_rx,
            heat,
            ref_bitmap,
            config,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn start_with_metrics(
        metrics: Arc<EngineMetrics>,
        meta: Arc<MetaStore>,
        io_engine: Arc<IoEngine>,
        allocator: Arc<SpaceAllocator>,
        lifecycle: Arc<VolumeLifecycleManager>,
        buffer_pool: Arc<WriteBufferPool>,
        candidate: CandidateCache,
        read_pool: Option<Arc<ReadPool>>,
        cold_rx: Option<Receiver<ColdTailTarget>>,
        heat: Option<HeatMap>,
        // Stage-5: per-PBA referenced bitmap the GC heat sweep fills (None unless
        // per-PBA orphan reclaim is on). Read here as the orphan selector.
        ref_bitmap: Option<RefBitmap>,
        config: DedupConfig,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();
        let config = Arc::new(ArcSwap::from_pointee(config));
        let config_clone = config.clone();

        let handle = thread::Builder::new()
            .name("dedup-scanner".into())
            .spawn(move || {
                crate::affinity::bind_current(crate::affinity::ThreadRole::Background, 0);
                Self::scan_loop(
                    &metrics,
                    &meta,
                    &io_engine,
                    &allocator,
                    &lifecycle,
                    &buffer_pool,
                    &candidate,
                    read_pool.as_deref(),
                    cold_rx.as_ref(),
                    heat.as_ref(),
                    ref_bitmap.as_ref(),
                    &config_clone,
                    &running_clone,
                );
            })
            .expect("failed to spawn dedup scanner thread");

        Self {
            running,
            config,
            handle: Some(handle),
        }
    }

    /// Hot-reload dedup scanner configuration.
    pub fn update_config(&self, new_config: DedupConfig) {
        tracing::info!("dedup scanner: config updated");
        self.config.store(Arc::new(new_config));
    }

    #[allow(clippy::too_many_arguments)]
    fn scan_loop(
        metrics: &EngineMetrics,
        meta: &MetaStore,
        io_engine: &IoEngine,
        allocator: &SpaceAllocator,
        lifecycle: &VolumeLifecycleManager,
        buffer_pool: &WriteBufferPool,
        candidate: &CandidateCache,
        read_pool: Option<&ReadPool>,
        cold_rx: Option<&Receiver<ColdTailTarget>>,
        heat: Option<&HeatMap>,
        ref_bitmap: Option<&RefBitmap>,
        config: &ArcSwap<DedupConfig>,
        running: &AtomicBool,
    ) {
        let mut last_drained_skipped_units = metrics.dedup_skipped_units.load(Ordering::Relaxed);
        // Scan once on startup even if the in-memory skipped counter did not
        // move. This covers scanner restarts and tests that inject skipped
        // mappings before the scanner shares runtime metrics with the flusher.
        let mut rescan_debt = true;
        // Per-volume lap state for cold-tail warming. The scanner walks
        // live blockmap entries in chunked passes; each lap starts at a
        // random phase and advances `cold_tail_max_per_cycle` LBAs per
        // cycle, wrapping the volume, then re-randomizes (see
        // `ColdTailCursor`).
        let mut cold_tail_cursors: HashMap<String, ColdTailCursor> = HashMap::new();
        let mut index_scrub_cursor: usize = 0;
        let mut orphan_cursor: usize = 0;
        while running.load(Ordering::Relaxed) {
            let cfg = config.load();
            thread::sleep(Duration::from_millis(cfg.rescan_interval_ms));
            if !running.load(Ordering::Relaxed) {
                break;
            }

            metrics.dedup_rescan_cycles.fetch_add(1, Ordering::Relaxed);

            // Skip if buffer is under pressure
            if buffer_pool.fill_percentage() > cfg.buffer_skip_threshold_pct as u8 {
                metrics
                    .dedup_rescan_skipped_cycles
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }

            let skipped_units = metrics.dedup_skipped_units.load(Ordering::Relaxed);
            let need_skipped_pass = rescan_debt || skipped_units != last_drained_skipped_units;
            if need_skipped_pass {
                match Self::rescan_skipped_blocks(
                    metrics,
                    meta,
                    io_engine,
                    allocator,
                    lifecycle,
                    candidate,
                    cfg.max_rescan_per_cycle,
                ) {
                    Ok(stats) => {
                        metrics
                            .dedup_rescan_blocks
                            .fetch_add(stats.rescanned as u64, Ordering::Relaxed);
                        metrics
                            .dedup_rescan_hits
                            .fetch_add(stats.hits as u64, Ordering::Relaxed);
                        metrics
                            .dedup_rescan_misses
                            .fetch_add(stats.misses as u64, Ordering::Relaxed);
                        if stats.rescanned > 0 {
                            tracing::info!(
                                rescanned = stats.rescanned,
                                hits = stats.hits,
                                misses = stats.misses,
                                "dedup scanner: re-processed skipped blocks"
                            );
                        }
                        if cfg.max_rescan_per_cycle > 0
                            && stats.rescanned >= cfg.max_rescan_per_cycle
                        {
                            rescan_debt = true;
                        } else {
                            rescan_debt = false;
                            last_drained_skipped_units = skipped_units;
                        }
                    }
                    Err(e) => {
                        rescan_debt = true;
                        metrics.dedup_rescan_errors.fetch_add(1, Ordering::Relaxed);
                        tracing::error!(error = %e, "dedup scanner: rescan failed");
                    }
                }
            }

            // Cold-tail warming runs in cycles where DEDUP_SKIPPED debt
            // is drained. The skipped path is higher-value (it knows
            // which entries were never hashed); cold-tail is a
            // background sweep that recovers ratio for entries warmed
            // by prior runs of the engine.
            if !rescan_debt && cfg.cold_tail_max_per_cycle > 0 {
                // Stage-4 fold: when the GC heat-refresh walk feeds cold
                // candidates over `cold_rx`, drain that channel instead of
                // running our own independent `scan_blockmap_range` traversal.
                // The expensive LV3 read + hash + remap/warm tail
                // (`process_cold_tail_targets`) is identical either way.
                let result = if let Some(rx) = cold_rx {
                    Self::cold_tail_drain(
                        meta,
                        allocator,
                        lifecycle,
                        candidate,
                        read_pool,
                        rx,
                        cfg.cold_tail_max_per_cycle,
                    )
                } else {
                    Self::cold_tail_rescan(
                        meta,
                        allocator,
                        lifecycle,
                        candidate,
                        read_pool,
                        &mut cold_tail_cursors,
                        cfg.cold_tail_max_per_cycle,
                    )
                };
                match result {
                    Ok(stats) => {
                        metrics
                            .dedup_cold_tail_blocks
                            .fetch_add(stats.warmed as u64, Ordering::Relaxed);
                        metrics
                            .dedup_cold_tail_remaps
                            .fetch_add(stats.remapped as u64, Ordering::Relaxed);
                        metrics
                            .dedup_cold_tail_already_warm
                            .fetch_add(stats.already_warm as u64, Ordering::Relaxed);
                        metrics
                            .dedup_cold_tail_errors
                            .fetch_add(stats.errors as u64, Ordering::Relaxed);
                        metrics
                            .dedup_cold_tail_drained
                            .fetch_add(stats.drained as u64, Ordering::Relaxed);
                        if stats.warmed > 0 || stats.remapped > 0 || stats.already_warm > 0 {
                            tracing::debug!(
                                warmed = stats.warmed,
                                remapped = stats.remapped,
                                already_warm = stats.already_warm,
                                drained = stats.drained,
                                errors = stats.errors,
                                "dedup scanner: cold-tail pass"
                            );
                        }
                    }
                    Err(e) => {
                        metrics
                            .dedup_cold_tail_errors
                            .fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(error = %e, "dedup scanner: cold-tail pass failed");
                    }
                }
            }

            if !rescan_debt && cfg.index_scrub_max_per_cycle > 0 {
                match Self::scrub_dedup_index(
                    meta,
                    io_engine,
                    candidate,
                    allocator,
                    metrics,
                    &mut index_scrub_cursor,
                    cfg.index_scrub_max_per_cycle,
                ) {
                    Ok(stats) => {
                        metrics
                            .dedup_scrub_retired
                            .fetch_add(stats.retired as u64, Ordering::Relaxed);
                        if stats.checked > 0 || stats.deleted > 0 || stats.errors > 0 {
                            tracing::debug!(
                                checked = stats.checked,
                                deleted = stats.deleted,
                                errors = stats.errors,
                                "dedup scanner: forward-index scrub pass"
                            );
                        }
                    }
                    Err(e) => {
                        metrics.dedup_rescan_errors.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(error = %e, "dedup scanner: forward-index scrub failed");
                    }
                }
            }

            // Orphan dedup-PBA reclaim: demote dedup entries with no live L2P
            // reference, then let the GC confirm scan free them. Two selectors:
            //   • §6 region mode (default): the 1 MiB heat region is cold.
            //   • Stage-5 per-PBA mode (`orphan_reclaim_per_pba`): the per-PBA
            //     referenced bitmap shows the PBA unreferenced across the last K
            //     completed lap-barriers — reclaims orphans *interleaved* with
            //     live data that the region selector skips. Same helper, same
            //     retire→Gate-2 free path; only the selector differs.
            // Needs the relevant structure refreshed; default-OFF via
            // `orphan_reclaim_max_per_cycle == 0` / `orphan_reclaim_enabled`.
            if !rescan_debt && cfg.orphan_reclaim_enabled && cfg.orphan_reclaim_max_per_cycle > 0 {
                let result = if cfg.orphan_reclaim_per_pba {
                    ref_bitmap.map(|rb| {
                        Self::orphan_reclaim_dedup_index_per_pba(
                            meta,
                            candidate,
                            allocator,
                            rb,
                            &mut orphan_cursor,
                            cfg.orphan_reclaim_max_per_cycle,
                            cfg.orphan_reclaim_clean_sweeps.clamp(1, 4) as usize,
                        )
                    })
                } else {
                    heat.map(|h| {
                        Self::orphan_reclaim_dedup_index(
                            meta,
                            candidate,
                            allocator,
                            h,
                            &mut orphan_cursor,
                            cfg.orphan_reclaim_max_per_cycle,
                            cfg.orphan_reclaim_fresh_max_age,
                        )
                    })
                };
                if let Some(res) = result {
                    match res {
                        Ok(stats) => {
                            metrics
                                .dedup_orphan_demoted
                                .fetch_add(stats.demoted as u64, Ordering::Relaxed);
                            metrics
                                .dedup_orphan_retired
                                .fetch_add(stats.retired as u64, Ordering::Relaxed);
                            metrics
                                .dedup_orphan_skipped_hot
                                .fetch_add(stats.skipped_hot as u64, Ordering::Relaxed);
                            if stats.demoted > 0 || stats.errors > 0 {
                                tracing::debug!(
                                    demoted = stats.demoted,
                                    retired = stats.retired,
                                    skipped_hot = stats.skipped_hot,
                                    errors = stats.errors,
                                    per_pba = cfg.orphan_reclaim_per_pba,
                                    "dedup scanner: orphan reclaim pass"
                                );
                            }
                        }
                        Err(e) => {
                            metrics.dedup_rescan_errors.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(error = %e, "dedup scanner: orphan reclaim failed");
                        }
                    }
                }
            }
        }
    }

    fn rescan_skipped_blocks(
        _metrics: &EngineMetrics,
        meta: &MetaStore,
        io_engine: &IoEngine,
        allocator: &SpaceAllocator,
        lifecycle: &VolumeLifecycleManager,
        candidate: &CandidateCache,
        max_per_cycle: usize,
    ) -> OnyxResult<RescanStats> {
        let skipped = meta.scan_dedup_skipped(max_per_cycle)?;
        let mut stats = RescanStats::default();

        for (vol_id_str, lba, bv) in &skipped {
            if bv.is_zero() {
                continue;
            }
            let vol_id = VolumeId(vol_id_str.clone());

            let result = lifecycle.with_read_lock(vol_id_str, || -> OnyxResult<bool> {
                // Same-LBA concurrent commits are arbitrated by
                // metadb's per-LBA seq_guard CAS; no onyx-side
                // stripe lock here. The re-check below guards the
                // scanner-versus-buffer-commit race.
                let result_inner = (|| -> OnyxResult<bool> {
                    // Re-read the mapping to ensure it's still the same,
                    // capturing the committed seq so we can forward it as
                    // the seq_guard on the subsequent l2p_remap. The
                    // re-check is NOT atomic with the apply below: read +
                    // hash + dedup_index lookup can take hundreds of
                    // microseconds, during which a buffer-flusher commit
                    // can advance the L2P seq. Passing seq=0 would
                    // silently bypass `seq_guard_rejects` and clobber the
                    // newer write — see the
                    // `metadb_seq0_in_l2p_remap_bypasses_guard_and_clobbers_newer_write`
                    // regression test.
                    let Some((current, observed_seq)) = meta.get_mapping_with_seq(&vol_id, *lba)?
                    else {
                        return Ok(false);
                    };
                    if !(current.pba == bv.pba
                        && current.slot_offset == bv.slot_offset
                        && current.unit_compressed_size == bv.unit_compressed_size
                        && current.unit_original_size == bv.unit_original_size
                        && current.unit_lba_count == bv.unit_lba_count
                        && current.offset_in_unit == bv.offset_in_unit
                        && current.compression == bv.compression
                        && current.crc32 == bv.crc32
                        && current.flags & FLAG_DEDUP_SKIPPED != 0)
                    {
                        return Ok(false); // Changed or flag already cleared
                    }

                    let block = match read_lba_block(io_engine, &current)? {
                        Some(b) => b,
                        None => return Ok(false),
                    };
                    let hash: ContentHash = compute_content_hash(&block);

                    match meta.get_dedup_entry(&hash)? {
                        Some(existing) if meta.dedup_entry_is_live(&hash, &existing)? => {
                            // Persistent dedup hit: remap LBA to the live
                            // PBA and decrement the now-orphaned old PBA.
                            let new_bv = BlockmapValue {
                                flags: 0, // Clear DEDUP_SKIPPED
                                ..existing.to_blockmap_value()
                            };
                            // Forward the observed seq so apply's
                            // seq_guard rejects a losing race against a
                            // concurrent buffer-flusher commit. The old
                            // sentinel value of 0 bypassed the guard
                            // entirely; the re-check above is not enough
                            // because the dedup_index lookup + this
                            // tx.commit can take >100 us during which a
                            // newer commit may land.
                            let decremented =
                                meta.atomic_dedup_hit(&vol_id, *lba, &new_bv, &hash, observed_seq)?;
                            if let Some(cleanup) = decremented {
                                BufferFlusher::cleanup_dead_pba_post_commit(
                                    allocator,
                                    candidate,
                                    cleanup,
                                    "dedup_scanner_cleanup",
                                );
                            }
                            stats.hits += 1;
                        }
                        _ => {
                            // No live persistent entry. Promote-on-verified-hit
                            // means we do **not** write the dedup_index here:
                            // first-occurrence misses go to the candidate cache
                            // and only get promoted when a future duplicate
                            // write byte-verifies against this PBA. Drop the
                            // FLAG_DEDUP_SKIPPED bit so the scanner does not
                            // re-process this LBA forever.
                            candidate.insert(
                                hash,
                                BlockmapValue {
                                    flags: 0,
                                    ..current
                                },
                            );
                            meta.update_blockmap_flags(&vol_id, *lba, 0)?;
                            stats.misses += 1;
                        }
                    }

                    Ok(true)
                })();
                result_inner
            })?;

            if result {
                stats.rescanned += 1;
            }
        }

        Ok(stats)
    }

    /// Cold-tail warming pass: walk a chunk of live blockmap entries
    /// per cycle, hash the content, and either remap evicted-window
    /// duplicates onto their existing dedup PBA or warm the candidate
    /// cache for true misses. Each volume is walked one *lap* at a time
    /// (a full pass over its LBA space); the lap starts at a random
    /// phase and advances linearly, wrapping the volume, then
    /// re-randomizes — so coverage order varies run-to-run instead of
    /// always sweeping `0..N` (which starved the tail of huge volumes
    /// and re-scanned the head after every restart).
    ///
    /// LV3 reads go through the engine's `ReadPool` so the cycle's IO
    /// is fanned out via io_uring at high queue depth. When no
    /// `ReadPool` is configured the pass is skipped — serial blocking
    /// reads would dominate scanner runtime and squander budget.
    #[allow(clippy::too_many_arguments)]
    fn cold_tail_rescan(
        meta: &MetaStore,
        allocator: &SpaceAllocator,
        lifecycle: &VolumeLifecycleManager,
        candidate: &CandidateCache,
        read_pool: Option<&ReadPool>,
        cursors: &mut HashMap<String, ColdTailCursor>,
        budget: usize,
    ) -> OnyxResult<ColdTailStats> {
        let mut stats = ColdTailStats::default();
        let Some(pool) = read_pool else {
            // Without a ReadPool we cannot batch reads through io_uring.
            // Serial blocking reads here would compete with foreground
            // IO and rarely finish a meaningful budget per cycle, so
            // we skip cold-tail entirely.
            return Ok(stats);
        };
        if budget == 0 {
            return Ok(stats);
        }

        let volumes = meta.list_volumes()?;
        if volumes.is_empty() {
            return Ok(stats);
        }

        // Walk each volume in turn until we exhaust the per-cycle
        // budget. The cursor is per-volume so a small volume that
        // wraps quickly does not starve a large volume of progress.
        let mut remaining = budget;
        for vol in &volumes {
            if remaining == 0 {
                break;
            }
            let total_lbas = vol.size_bytes / u64::from(vol.block_size);
            if total_lbas == 0 {
                continue;
            }
            // Pick this cycle's contiguous physical range from the
            // per-volume lap state. A lap starts at a random phase and
            // advances linearly (wrapping the volume) until it has
            // covered `total_lbas`, then re-randomizes. `phys_start` is
            // the lap phase advanced by what we've already scanned,
            // wrapped into `[0, total_lbas)`; `chunk` is clamped so the
            // scan never crosses the volume's end boundary in one range.
            let (phys_start, chunk) = {
                let cur = cursors.entry(vol.id.0.clone()).or_insert_with(|| {
                    let mut rng = cold_tail_seed(&vol.id.0);
                    let lap_start = splitmix64(&mut rng) % total_lbas;
                    ColdTailCursor {
                        lap_start,
                        scanned_in_lap: 0,
                        rng,
                    }
                });
                if cur.scanned_in_lap >= total_lbas {
                    cur.lap_start = splitmix64(&mut cur.rng) % total_lbas;
                    cur.scanned_in_lap = 0;
                }
                let phys_start = (cur.lap_start + cur.scanned_in_lap) % total_lbas;
                let lap_remaining = total_lbas - cur.scanned_in_lap;
                let chunk = (remaining as u64)
                    .min(lap_remaining)
                    .min(total_lbas - phys_start);
                cur.scanned_in_lap += chunk;
                (phys_start, chunk)
            };
            remaining = remaining.saturating_sub(chunk as usize);
            if chunk == 0 {
                continue;
            }

            // Collect candidate entries to process in this chunk. We
            // reject entries that:
            //  - are flagged DEDUP_SKIPPED (the rescan_skipped path
            //    handles those — their hashes are not yet known so we
            //    do not want a duplicate read here),
            //  - already have a candidate cache entry pointing at
            //    their PBA (warmed by the writer or a previous cycle).
            // dedup_index membership is decided per-entry below (a hit
            // is *remapped*, a miss is *warmed*), so it is not a filter
            // here.
            let mut targets: Vec<(Lba, BlockmapValue)> = Vec::new();
            let mut already_warm = 0usize;
            meta.scan_blockmap_range(&vol.id, Lba(phys_start), chunk, &mut |lba, value| {
                if value.is_zero() {
                    return;
                }
                if value.flags & FLAG_DEDUP_SKIPPED != 0 {
                    return;
                }
                if candidate.has_pba(value.pba) {
                    already_warm += 1;
                    return;
                }
                targets.push((lba, value));
            })?;

            stats.already_warm += already_warm;

            if targets.is_empty() {
                continue;
            }

            // Read + hash + remap/warm the collected targets. Shared with
            // the Stage-4 fold drain path (`cold_tail_drain`) so the verified
            // ReadPool batch + dedup_index remap + candidate warm + dead-PBA
            // cleanup logic lives in exactly one place.
            Self::process_cold_tail_targets(
                meta,
                allocator,
                lifecycle,
                candidate,
                pool,
                &vol.id,
                &targets,
                &mut stats,
            );
        }

        Ok(stats)
    }

    /// Read each collected `(lba, BlockmapValue)` target through the
    /// `ReadPool`, hash the 4 KiB content, and either remap an
    /// evicted-window duplicate onto its existing live dedup_index PBA or
    /// warm the candidate cache for a true miss. Shared by the legacy
    /// `cold_tail_rescan` (target discovery via its own
    /// `scan_blockmap_range`) and the Stage-4 `cold_tail_drain` (targets
    /// arrive over the fold channel from the GC heat walk). All re-validation
    /// (`get_mapping_with_seq` / `same_physical_mapping`) and lifecycle
    /// read-locking is unchanged from the original cold-tail pass — callers
    /// differ only in how `targets` is sourced.
    #[allow(clippy::too_many_arguments)]
    fn process_cold_tail_targets(
        meta: &MetaStore,
        allocator: &SpaceAllocator,
        lifecycle: &VolumeLifecycleManager,
        candidate: &CandidateCache,
        pool: &ReadPool,
        vol_id: &VolumeId,
        targets: &[(Lba, BlockmapValue)],
        stats: &mut ColdTailStats,
    ) {
        if targets.is_empty() {
            return;
        }

        // Fan out the LV3 reads through ReadPool so io_uring can
        // keep multiple SQEs in flight for one drain.
        let mut receivers = Vec::with_capacity(targets.len());
        for (_lba, bv) in targets {
            match pool.submit_read_async_for(*bv, ReadPurpose::DedupScanner) {
                Ok(rx) => receivers.push(Some(rx)),
                Err(e) => {
                    stats.errors += 1;
                    tracing::debug!(
                        pba = bv.pba.0,
                        error = %e,
                        "cold-tail: failed to enqueue ReadPool request"
                    );
                    receivers.push(None);
                }
            }
        }

        for ((lba, bv), rx_opt) in targets.iter().zip(receivers.into_iter()) {
            let Some(rx) = rx_opt else { continue };
            let block = match rx.recv() {
                Ok(Ok(buf)) if buf.len() == BLOCK_SIZE as usize => buf,
                Ok(Ok(_)) => {
                    stats.errors += 1;
                    continue;
                }
                Ok(Err(e)) => {
                    stats.errors += 1;
                    tracing::debug!(
                        pba = bv.pba.0,
                        error = %e,
                        "cold-tail: ReadPool returned error"
                    );
                    continue;
                }
                Err(_) => {
                    stats.errors += 1;
                    tracing::debug!(
                        pba = bv.pba.0,
                        "cold-tail: ReadPool reply channel dropped"
                    );
                    continue;
                }
            };
            let hash = compute_content_hash(&block);

            // If the hash is already a *live* entry in the
            // persistent dedup index, this live block is an
            // evicted-window duplicate: its content was promoted
            // earlier, but its candidate slot was evicted before
            // the duplicate write arrived, so it was written
            // un-deduped against its own fresh PBA. Reclaim it by
            // remapping the LBA onto the existing dedup target and
            // decref'ing the orphaned old PBA — the same action the
            // FLAG_DEDUP_SKIPPED rescan path already takes. Warming
            // the candidate would only help a *future* write; it
            // does nothing for this already-written block.
            let index_entry = match meta.get_dedup_entry(&hash) {
                Ok(Some(existing)) => match meta.dedup_entry_is_live(&hash, &existing) {
                    Ok(true) => Some(existing),
                    Ok(false) => None,
                    Err(e) => {
                        stats.errors += 1;
                        tracing::debug!(
                            vol = %vol_id.0,
                            lba = lba.0,
                            error = %e,
                            "cold-tail: dedup_index liveness check failed"
                        );
                        continue;
                    }
                },
                Ok(None) => None,
                Err(e) => {
                    stats.errors += 1;
                    tracing::debug!(
                        vol = %vol_id.0,
                        lba = lba.0,
                        error = %e,
                        "cold-tail: dedup_index probe failed"
                    );
                    continue;
                }
            };

            if let Some(existing) = index_entry {
                // The live block already points at the dedup target
                // (it may even *be* the canonical copy). Nothing to
                // reclaim — do not self-remap.
                if existing.pba == bv.pba {
                    stats.already_warm += 1;
                    continue;
                }

                // Remap under the volume read lock so the volume
                // cannot be dropped mid-tx, mirroring
                // `rescan_skipped_blocks`. Re-read the mapping with
                // its committed seq and forward it to the
                // seq_guard: the async LV3 read + hash above is not
                // atomic with this commit, so a foreground write may
                // have landed. seq=0 would silently clobber it (see
                // the
                // `metadb_seq0_in_l2p_remap_bypasses_guard_and_clobbers_newer_write`
                // regression test).
                let outcome = lifecycle.with_read_lock(&vol_id.0, || -> OnyxResult<u8> {
                    let Some((current, observed_seq)) = meta.get_mapping_with_seq(vol_id, *lba)?
                    else {
                        return Ok(0); // gone
                    };
                    if !same_physical_mapping(&current, bv) {
                        return Ok(0); // changed under us
                    }
                    let new_bv = BlockmapValue {
                        flags: 0,
                        ..existing.to_blockmap_value()
                    };
                    let decremented =
                        meta.atomic_dedup_hit(vol_id, *lba, &new_bv, &hash, observed_seq)?;
                    if let Some(cleanup) = decremented {
                        BufferFlusher::cleanup_dead_pba_post_commit(
                            allocator,
                            candidate,
                            cleanup,
                            "dedup_cold_tail_cleanup",
                        );
                    }
                    Ok(1) // remapped
                });

                match outcome {
                    Ok(1) => stats.remapped += 1,
                    Ok(_) => stats.already_warm += 1,
                    Err(e) => {
                        stats.errors += 1;
                        tracing::debug!(
                            vol = %vol_id.0,
                            lba = lba.0,
                            error = %e,
                            "cold-tail: dedup_index-hit remap failed"
                        );
                    }
                }
                continue;
            }

            // True miss: warm the candidate cache so a *future*
            // duplicate write can verify-and-promote against this
            // fingerprint. Re-validate the mapping first so we do
            // not cache a stale (vol, lba) -> pba pair.
            match meta.get_mapping(vol_id, *lba) {
                Ok(Some(current)) if same_physical_mapping(&current, bv) => {}
                Ok(_) => {
                    stats.already_warm += 1;
                    continue;
                }
                Err(e) => {
                    stats.errors += 1;
                    tracing::debug!(
                        vol = %vol_id.0,
                        lba = lba.0,
                        error = %e,
                        "cold-tail: failed to revalidate mapping"
                    );
                    continue;
                }
            }

            candidate.insert(hash, BlockmapValue { flags: 0, ..*bv });
            stats.warmed += 1;
        }
    }

    /// Stage-4 fold consumer: drain cold candidates fed by the GC
    /// heat-refresh walk over `rx` (instead of running our own
    /// `scan_blockmap_range`), then process them through the shared
    /// `process_cold_tail_targets`. Drains up to `budget` *real* targets
    /// (not-already-warm) so the LV3 read IO stays bounded exactly as the
    /// legacy pass; already-warm entries are rejected cheaply here (clearing
    /// them from the channel) without counting toward the read budget. The
    /// producer re-sends fresh cold entries every heat cycle, so a stale or
    /// dropped target only costs dedup ratio, never correctness.
    fn cold_tail_drain(
        meta: &MetaStore,
        allocator: &SpaceAllocator,
        lifecycle: &VolumeLifecycleManager,
        candidate: &CandidateCache,
        read_pool: Option<&ReadPool>,
        rx: &Receiver<ColdTailTarget>,
        budget: usize,
    ) -> OnyxResult<ColdTailStats> {
        let mut stats = ColdTailStats::default();
        let Some(pool) = read_pool else {
            return Ok(stats);
        };
        if budget == 0 {
            return Ok(stats);
        }

        // Group drained targets by volume so each group shares one lifecycle
        // read lock + ReadPool batch in `process_cold_tail_targets`, matching
        // the legacy per-volume pass.
        let mut by_vol: HashMap<String, Vec<(Lba, BlockmapValue)>> = HashMap::new();
        let mut collected = 0usize;
        while collected < budget {
            match rx.try_recv() {
                Ok(t) => {
                    if candidate.has_pba(t.bv.pba) {
                        // Already warmed (writer or a prior cycle) — drop it
                        // cheaply, do not spend a read on it.
                        stats.already_warm += 1;
                        continue;
                    }
                    by_vol.entry(t.vol_id.0).or_default().push((t.lba, t.bv));
                    collected += 1;
                }
                // Empty (caught up) or Disconnected (producer gone, e.g. GC
                // stopped at shutdown) — either way, nothing more to drain.
                Err(_) => break,
            }
        }
        stats.drained = collected;

        for (vol_id_str, targets) in &by_vol {
            let vol_id = VolumeId(vol_id_str.clone());
            Self::process_cold_tail_targets(
                meta,
                allocator,
                lifecycle,
                candidate,
                pool,
                &vol_id,
                targets,
                &mut stats,
            );
        }

        Ok(stats)
    }

    fn scrub_dedup_index(
        meta: &MetaStore,
        io_engine: &IoEngine,
        candidate: &CandidateCache,
        allocator: &SpaceAllocator,
        metrics: &EngineMetrics,
        cursor: &mut usize,
        budget: usize,
    ) -> OnyxResult<IndexScrubStats> {
        let mut stats = IndexScrubStats::default();
        if budget == 0 {
            return Ok(stats);
        }

        let mut entries = meta.iter_dedup_entries()?;
        if entries.is_empty() {
            *cursor = 0;
            return Ok(stats);
        }
        entries.sort_unstable_by_key(|(hash, entry)| (*hash, entry.pba.0, entry.slot_offset));
        if *cursor >= entries.len() {
            *cursor = 0;
        }

        let count = budget.min(entries.len());
        for offset in 0..count {
            let idx = (*cursor + offset) % entries.len();
            let (hash, entry) = entries[idx];
            let mapping = entry.to_blockmap_value();
            let matched = match read_lba_block(io_engine, &mapping) {
                Ok(Some(block)) => compute_content_hash(&block) == hash,
                Ok(None) => false,
                Err(e) => {
                    stats.errors += 1;
                    tracing::debug!(
                        pba = entry.pba.0,
                        slot_offset = entry.slot_offset,
                        error = %e,
                        "dedup index scrub: failed to read entry"
                    );
                    continue;
                }
            };
            stats.checked += 1;
            if matched {
                continue;
            }

            // Route through the shared helper so a stale entry whose now-rc==0
            // PBA used to leak is retired for GC reclaim.
            match Self::delete_dedup_entry_and_retire(meta, candidate, allocator, &hash, &mapping) {
                Ok(out) if out.deleted => {
                    stats.deleted += 1;
                    if out.retired {
                        stats.retired += 1;
                    }
                    tracing::debug!(
                        pba = entry.pba.0,
                        slot_offset = entry.slot_offset,
                        retired = out.retired,
                        "dedup index scrub: removed stale forward entry"
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    stats.errors += 1;
                    metrics
                        .dedup_cleanup_delete_errors
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        pba = entry.pba.0,
                        slot_offset = entry.slot_offset,
                        error = %e,
                        "dedup index scrub: conditional delete failed"
                    );
                }
            }
        }
        *cursor = (*cursor + count) % entries.len();
        Ok(stats)
    }

    /// Delete a dedup_index entry and, when that drops the PBA's refcount to 0,
    /// retire the now-orphaned PBA so the GC confirm scan reclaims it.
    ///
    /// Phase 5: a dedup PBA's refcount is pure dedup-index *membership* (==1 in
    /// the index, independent of how many live LBAs reference it). So deleting
    /// the entry decrefs rc to 0 *iff* no other dedup/clone/snapshot account
    /// holds it — and `P` becomes an ordinary "rc==0, maybe-still-referenced"
    /// PBA, exactly the case `reclaim_retired_extents` already handles: its
    /// `referenced_extents` Gate-2 scan is the EXACT authority that frees `P`
    /// only if no live blockmap entry references it. So demoting a
    /// still-referenced entry is never data loss — `P` stays retired (not
    /// freed), losing only its dedup membership (recoverable via re-promote).
    ///
    /// Ordering: clear the candidate cache for `P` FIRST so a concurrent
    /// verifier cannot byte-verify against it and re-promote (re-referencing a
    /// PBA we are about to retire), mirroring `cleanup.rs`'s
    /// candidate-before-retire invariant. Shared by the orphan-reclaim pass and
    /// the stale-entry scrub (so scrub-freed PBAs are reclaimed too, closing a
    /// latent leak where a `DedupCompareDelete` decref's freed PBA was dropped).
    fn delete_dedup_entry_and_retire(
        meta: &MetaStore,
        candidate: &CandidateCache,
        allocator: &SpaceAllocator,
        hash: &ContentHash,
        mapping: &BlockmapValue,
    ) -> OnyxResult<DemoteOutcome> {
        let pba = mapping.pba;
        candidate.remove_by_pba(pba);
        let deleted = meta.delete_dedup_index_if_matches(hash, mapping)?;
        if !deleted {
            return Ok(DemoteOutcome {
                deleted: false,
                retired: false,
            });
        }
        let mut retired = false;
        if meta.get_refcount(pba)? == 0 && !allocator.is_retired(pba) {
            match allocator.retire_one(pba) {
                Ok(true) => retired = true,
                Ok(false) => {}
                Err(e) => {
                    tracing::warn!(pba = pba.0, error = %e, "orphan dedup reclaim: retire_one failed");
                }
            }
        }
        Ok(DemoteOutcome {
            deleted: true,
            retired,
        })
    }

    /// Orphan dedup-PBA reclaim (§6): walk a budgeted slice of the dedup_index
    /// and demote entries whose PBA region the heat map reports as COLD (no
    /// live L2P entry references the whole 1 MiB region → the entry is very
    /// likely orphaned). Demote = delete the index entry + retire the now-rc==0
    /// PBA via [`Self::delete_dedup_entry_and_retire`]; the GC confirm scan does
    /// the safe free. The heat map is only a *selector* (skip hot/shared
    /// regions to avoid dedup-ratio churn) — correctness is the Gate-2 scan, so
    /// a stale/wrong heat read can never free a live PBA, only cost a
    /// re-promote. Runs only with a refreshed heat map present.
    fn orphan_reclaim_dedup_index(
        meta: &MetaStore,
        candidate: &CandidateCache,
        allocator: &SpaceAllocator,
        heat: &HeatMap,
        cursor: &mut usize,
        budget: usize,
        fresh_max_age: u32,
    ) -> OnyxResult<OrphanReclaimStats> {
        let mut stats = OrphanReclaimStats::default();
        if budget == 0 {
            return Ok(stats);
        }
        // Convergence floor: don't trust the heat map until it has completed at
        // least one full sweep (FIRST_EPOCH==1, so epoch>=2). Before that every
        // region looks cold (nothing swept yet) and we'd churn dedup ratio for
        // not-yet-swept live entries (safe via the Gate-2 confirm scan, but
        // wasteful). The `fresh_max_age` staleness window in the selector below
        // adds further delay before a freshly-orphaned region is demoted.
        if heat.current_epoch() < 2 {
            return Ok(stats);
        }
        let mut entries = meta.iter_dedup_entries()?;
        if entries.is_empty() {
            *cursor = 0;
            return Ok(stats);
        }
        entries.sort_unstable_by_key(|(hash, entry)| (*hash, entry.pba.0, entry.slot_offset));
        if *cursor >= entries.len() {
            *cursor = 0;
        }
        let count = budget.min(entries.len());
        for offset in 0..count {
            let idx = (*cursor + offset) % entries.len();
            let (hash, entry) = entries[idx];
            // Selector: demote ONLY a STALE region — one that WAS live (count>0,
            // the heat refresh bumped it while the dedup PBA was referenced) but
            // has NOT been bumped for > fresh_max_age completed sweeps. That is
            // the overwrite-orphaned signature: every referrer was overwritten,
            // so the heat bucket keeps its stale count while its age grows.
            //
            // A NEVER_SCANNED region (count==0) is deliberately NOT demoted: it
            // has no PROOF of going cold (it may simply not have been swept yet,
            // e.g. before the heat map converges or right after a restart), so
            // demoting it would churn dedup ratio for live-but-unswept entries.
            // Requiring a prior bump makes cold-start churn impossible. (Safety
            // is the Gate-2 confirm scan regardless — this selector only governs
            // *which* entries we bother to demote, never whether a free is safe.)
            let (age, region_count) = heat.region(entry.pba);
            let stale = region_count > 0 && age != u32::MAX && age > fresh_max_age;
            if !stale {
                stats.skipped_hot += 1;
                continue;
            }
            let mapping = entry.to_blockmap_value();
            match Self::delete_dedup_entry_and_retire(meta, candidate, allocator, &hash, &mapping) {
                Ok(out) if out.deleted => {
                    stats.demoted += 1;
                    if out.retired {
                        stats.retired += 1;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    stats.errors += 1;
                    tracing::debug!(
                        pba = entry.pba.0,
                        error = %e,
                        "orphan dedup reclaim: demote failed"
                    );
                }
            }
        }
        *cursor = (*cursor + count) % entries.len();
        Ok(stats)
    }

    /// Stage-5 per-PBA orphan reclaim: like [`Self::orphan_reclaim_dedup_index`]
    /// but the selector is the per-PBA referenced bitmap instead of the 1 MiB
    /// heat region. A dedup entry is demoted when its PBA reads unreferenced
    /// (bit==0) across the last `clean_sweeps` completed lap-barriers — so
    /// orphans *interleaved* with live data (which the region selector skips
    /// because the shared 1 MiB region still counts live) are reclaimed too.
    ///
    /// Safety is identical to §6: the bitmap is only a *selector*; the GC Gate-2
    /// `referenced_extents` exact scan authorizes every free, so a wrong bit can
    /// only cost a re-promote, never data. Reuses the same
    /// [`Self::delete_dedup_entry_and_retire`] helper and retire→free path.
    fn orphan_reclaim_dedup_index_per_pba(
        meta: &MetaStore,
        candidate: &CandidateCache,
        allocator: &SpaceAllocator,
        ref_bitmap: &RefBitmap,
        cursor: &mut usize,
        budget: usize,
        clean_sweeps: usize,
    ) -> OnyxResult<OrphanReclaimStats> {
        let mut stats = OrphanReclaimStats::default();
        if budget == 0 {
            return Ok(stats);
        }
        // Convergence floor: need at least `clean_sweeps` completed lap-barriers
        // before any 0 bit is trustworthy. Before that the bitmap can't tell
        // "unreferenced" from "not yet covered", so demoting would churn dedup
        // ratio for live-but-uncovered entries (Gate-2-safe, but wasteful).
        if ref_bitmap.published_count() < clean_sweeps {
            return Ok(stats);
        }
        let mut entries = meta.iter_dedup_entries()?;
        if entries.is_empty() {
            *cursor = 0;
            return Ok(stats);
        }
        entries.sort_unstable_by_key(|(hash, entry)| (*hash, entry.pba.0, entry.slot_offset));
        if *cursor >= entries.len() {
            *cursor = 0;
        }
        let count = budget.min(entries.len());
        for offset in 0..count {
            let idx = (*cursor + offset) % entries.len();
            let (hash, entry) = entries[idx];
            // Selector: demote ONLY a PBA that read unreferenced (bit==0) in
            // EVERY one of the last `clean_sweeps` completed lap-barriers.
            //   None        → not yet converged → skip.
            //   Some(false) → referenced in a recent barrier (or out of range) → skip.
            //   Some(true)  → unreferenced across all K barriers → demote candidate.
            // Requiring K consecutive clean barriers absorbs the in-flight-write
            // race (a write landing after a referrer-LBA was walked but before
            // the PBA was orphaned). Gate-2 remains the only safety authority.
            let orphan = matches!(
                ref_bitmap.unreferenced_in_recent(entry.pba, clean_sweeps),
                Some(true)
            );
            if !orphan {
                stats.skipped_hot += 1;
                continue;
            }
            let mapping = entry.to_blockmap_value();
            match Self::delete_dedup_entry_and_retire(meta, candidate, allocator, &hash, &mapping) {
                Ok(out) if out.deleted => {
                    stats.demoted += 1;
                    if out.retired {
                        stats.retired += 1;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    stats.errors += 1;
                    tracing::debug!(
                        pba = entry.pba.0,
                        error = %e,
                        "orphan dedup reclaim (per-PBA): demote failed"
                    );
                }
            }
        }
        *cursor = (*cursor + count) % entries.len();
        Ok(stats)
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for DedupScanner {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct RescanStats {
    rescanned: usize,
    hits: usize,
    misses: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct ColdTailStats {
    /// Entries the scanner read, hashed, and inserted into the
    /// candidate cache (true misses against the persistent index).
    warmed: usize,
    /// Entries the scanner remapped onto an existing live dedup_index
    /// PBA: an evicted-window duplicate whose content was already in
    /// the persistent index but whose live block still pointed at its
    /// own fresh PBA. The remap decrefs the now-orphaned old PBA.
    remapped: usize,
    /// Entries the scanner left as-is because either the candidate
    /// cache already had a fingerprint for the PBA, the live block
    /// already pointed at the dedup target, or the mapping changed
    /// under us before the remap/warm.
    already_warm: usize,
    /// Entries the scanner could not process due to ReadPool errors,
    /// short reads, or dedup-index probe failures.
    errors: usize,
    /// Stage-4 fold only: real (not-already-warm) targets drained from the
    /// fold channel and fed to the ReadPool this cycle. Always 0 on the
    /// legacy `cold_tail_rescan` path.
    drained: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct IndexScrubStats {
    checked: usize,
    deleted: usize,
    /// Stale entries whose now-rc==0 PBA was retired for GC reclaim (was
    /// previously leaked — the `DedupCompareDelete` decref's freed PBA was
    /// dropped on the floor).
    retired: usize,
    errors: usize,
}

/// Outcome of [`DedupScanner::delete_dedup_entry_and_retire`].
#[derive(Debug, Clone, Copy, Default)]
struct DemoteOutcome {
    /// The guarded `delete_dedup_index_if_matches` applied (entry removed).
    deleted: bool,
    /// The now-rc==0 PBA was retired into the allocator's retired set.
    retired: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct OrphanReclaimStats {
    /// Cold-region dedup entries deleted from the index.
    demoted: usize,
    /// Of those, PBAs that dropped to rc==0 and were retired for GC reclaim.
    retired: usize,
    /// Entries skipped because their region was hot / never-scanned (selector).
    skipped_hot: usize,
    errors: usize,
}

/// Per-volume cold-tail walk state. A "lap" is one full pass over the
/// volume's LBA space. Each lap starts at a fresh pseudo-random phase
/// (`lap_start`) and advances linearly, wrapping around the volume,
/// until `scanned_in_lap` reaches `total_lbas` — at which point the
/// next lap re-randomizes. The old behaviour always swept `0..N` in the
/// same order, which on a huge volume (or across process restarts that
/// reset the cursor to 0) re-scanned the head while starving the tail.
/// A random phase per lap gives every region a fair chance regardless
/// of how far a single run gets through a lap.
#[derive(Debug, Clone, Copy)]
struct ColdTailCursor {
    lap_start: u64,
    scanned_in_lap: u64,
    rng: u64,
}

/// splitmix64: a tiny, fast, self-contained PRNG. The cold-tail phase
/// only needs decorrelated coverage order, not crypto strength, so we
/// avoid pulling `rand` into the runtime dependency set.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Seed a per-volume PRNG from wall-clock time mixed with the volume id
/// (FNV-1a), so different volumes — and different process runs — pick
/// different cold-tail phases.
fn cold_tail_seed(vol_id: &str) -> u64 {
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

/// Read the 4 KiB physical block backing one blockmap entry. Handles
/// packed slots (`slot_offset > 0`), compression, and unit-internal
/// LBA offsets. Returns `Ok(None)` for soft failures (CRC mismatch,
/// out-of-range slot, decode failure) so the scanner can skip the
/// entry instead of erroring out the whole pass.
fn read_lba_block(io_engine: &IoEngine, bv: &BlockmapValue) -> OnyxResult<Option<Vec<u8>>> {
    let bs = BLOCK_SIZE as usize;
    let read_size = if bv.unit_compressed_size < BLOCK_SIZE {
        bs // packed slot: always read full 4KB
    } else {
        ((bv.unit_compressed_size as usize + bs - 1) / bs) * bs
    };
    let raw = io_engine.read_blocks(bv.pba, read_size)?;

    let start = bv.slot_offset as usize;
    let end = start + bv.unit_compressed_size as usize;
    if end > raw.len() {
        return Ok(None);
    }
    let compressed = &raw[start..end];

    let actual_crc = crc32fast::hash(compressed);
    if actual_crc != bv.crc32 {
        tracing::debug!(
            pba = bv.pba.0,
            slot_offset = bv.slot_offset,
            expected_crc = bv.crc32,
            actual_crc,
            "dedup scanner: stale physical mapping, skipping block"
        );
        return Ok(None);
    }

    let algo = crate::types::CompressionAlgo::from_u8(bv.compression)
        .unwrap_or(crate::types::CompressionAlgo::None);
    let compressor = create_compressor(algo);
    let mut decompressed = vec![0u8; bv.unit_original_size as usize];
    if bv.compression != 0 {
        compressor.decompress(
            compressed,
            &mut decompressed,
            bv.unit_original_size as usize,
        )?;
    } else {
        decompressed[..compressed.len()].copy_from_slice(compressed);
    }

    let offset = bv.offset_in_unit as usize * bs;
    if offset + bs > decompressed.len() {
        return Ok(None);
    }
    Ok(Some(decompressed[offset..offset + bs].to_vec()))
}

fn same_physical_mapping(a: &BlockmapValue, b: &BlockmapValue) -> bool {
    a.pba == b.pba
        && a.slot_offset == b.slot_offset
        && a.unit_compressed_size == b.unit_compressed_size
        && a.unit_original_size == b.unit_original_size
        && a.unit_lba_count == b.unit_lba_count
        && a.offset_in_unit == b.offset_in_unit
        && a.compression == b.compression
        && a.crc32 == b.crc32
}
