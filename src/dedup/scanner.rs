use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arc_swap::ArcSwap;

use crate::buffer::flush::BufferFlusher;
use crate::buffer::pool::WriteBufferPool;
use crate::compress::codec::create_compressor;
use crate::dedup::config::DedupConfig;
use crate::dedup::CandidateCache;
use crate::error::OnyxResult;
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
        config: &ArcSwap<DedupConfig>,
        running: &AtomicBool,
    ) {
        let mut last_drained_skipped_units = metrics.dedup_skipped_units.load(Ordering::Relaxed);
        // Scan once on startup even if the in-memory skipped counter did not
        // move. This covers scanner restarts and tests that inject skipped
        // mappings before the scanner shares runtime metrics with the flusher.
        let mut rescan_debt = true;
        // Per-volume LBA cursor for cold-tail warming. The scanner walks
        // live blockmap entries in chunked passes; each cycle advances
        // the cursor by `cold_tail_max_per_cycle` LBAs and wraps at the
        // end of the volume.
        let mut cold_tail_cursors: HashMap<String, u64> = HashMap::new();
        let mut index_scrub_cursor: usize = 0;
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
                match Self::cold_tail_rescan(
                    meta,
                    candidate,
                    read_pool,
                    &mut cold_tail_cursors,
                    cfg.cold_tail_max_per_cycle,
                ) {
                    Ok(stats) => {
                        metrics
                            .dedup_cold_tail_blocks
                            .fetch_add(stats.warmed as u64, Ordering::Relaxed);
                        metrics
                            .dedup_cold_tail_already_warm
                            .fetch_add(stats.already_warm as u64, Ordering::Relaxed);
                        metrics
                            .dedup_cold_tail_errors
                            .fetch_add(stats.errors as u64, Ordering::Relaxed);
                        if stats.warmed > 0 || stats.already_warm > 0 {
                            tracing::debug!(
                                warmed = stats.warmed,
                                already_warm = stats.already_warm,
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
                    metrics,
                    &mut index_scrub_cursor,
                    cfg.index_scrub_max_per_cycle,
                ) {
                    Ok(stats) => {
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
                    // Re-read the mapping to ensure it's still the same
                    let current = meta.get_mapping(&vol_id, *lba)?;
                    let current = match current {
                        Some(c)
                            if c.pba == bv.pba
                                && c.slot_offset == bv.slot_offset
                                && c.unit_compressed_size == bv.unit_compressed_size
                                && c.unit_original_size == bv.unit_original_size
                                && c.unit_lba_count == bv.unit_lba_count
                                && c.offset_in_unit == bv.offset_in_unit
                                && c.compression == bv.compression
                                && c.crc32 == bv.crc32
                                && c.flags & FLAG_DEDUP_SKIPPED != 0 =>
                        {
                            c
                        }
                        _ => return Ok(false), // Changed or flag already cleared
                    };

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
                            // seq=0 is the legacy/no-guard sentinel: metadb's
                            // apply-time seq_guard bypasses the CAS check, so
                            // the scanner can overwrite a buffer commit even
                            // when its per-LBA seq is non-zero. Scanner's
                            // existing re-check loop catches the race.
                            let decremented =
                                meta.atomic_dedup_hit(&vol_id, *lba, &new_bv, &hash, 0)?;
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
    /// per cycle, hash the content, and warm the candidate cache. The
    /// cursor advances `chunk` LBAs each cycle; when it reaches the
    /// end of a volume it wraps to 0.
    ///
    /// LV3 reads go through the engine's `ReadPool` so the cycle's IO
    /// is fanned out via io_uring at high queue depth. When no
    /// `ReadPool` is configured the pass is skipped — serial blocking
    /// reads would dominate scanner runtime and squander budget.
    fn cold_tail_rescan(
        meta: &MetaStore,
        candidate: &CandidateCache,
        read_pool: Option<&ReadPool>,
        cursors: &mut HashMap<String, u64>,
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
            let cursor = cursors.entry(vol.id.0.clone()).or_insert(0);
            if *cursor >= total_lbas {
                *cursor = 0;
            }

            let chunk = (remaining as u64).min(total_lbas - *cursor);

            // Collect candidate entries to warm in this chunk. We
            // reject entries that:
            //  - are flagged DEDUP_SKIPPED (the rescan_skipped path
            //    handles those — their hashes are not yet known so we
            //    do not want a duplicate read here),
            //  - already have a candidate cache entry pointing at
            //    their PBA (warmed by the writer or a previous cycle),
            //  - already live in the persistent dedup_index (a future
            //    duplicate would hit the index directly without need
            //    of a candidate slot).
            let mut targets: Vec<(Lba, BlockmapValue)> = Vec::new();
            let mut already_warm = 0usize;
            meta.scan_blockmap_range(&vol.id, Lba(*cursor), chunk, &mut |lba, value| {
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
            *cursor = cursor.saturating_add(chunk);
            remaining = remaining.saturating_sub(chunk as usize);

            if targets.is_empty() {
                continue;
            }

            // Fan out the LV3 reads through ReadPool so io_uring can
            // keep multiple SQEs in flight for one drain.
            let mut receivers = Vec::with_capacity(targets.len());
            for (_lba, bv) in &targets {
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

                // If the hash is already in the persistent dedup
                // index, warming the candidate cache is wasted: future
                // duplicate writes hit dedup_index directly without
                // probing the candidate cache. Count as already_warm
                // and skip the insert so we do not push useful entries
                // out of the LRU.
                let already_in_index = matches!(meta.get_dedup_entry(&hash), Ok(Some(_)));
                if already_in_index {
                    stats.already_warm += 1;
                    continue;
                }

                match meta.get_mapping(&vol.id, *lba) {
                    Ok(Some(current)) if same_physical_mapping(&current, bv) => {}
                    Ok(_) => {
                        stats.already_warm += 1;
                        continue;
                    }
                    Err(e) => {
                        stats.errors += 1;
                        tracing::debug!(
                            vol = %vol.id.0,
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

        Ok(stats)
    }

    fn scrub_dedup_index(
        meta: &MetaStore,
        io_engine: &IoEngine,
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

            match meta.delete_dedup_index_if_matches(&hash, &mapping) {
                Ok(true) => {
                    stats.deleted += 1;
                    tracing::debug!(
                        pba = entry.pba.0,
                        slot_offset = entry.slot_offset,
                        "dedup index scrub: removed stale forward entry"
                    );
                }
                Ok(false) => {}
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
    /// candidate cache.
    warmed: usize,
    /// Entries the scanner skipped because either the candidate cache
    /// already had a fingerprint for the PBA or the persistent dedup
    /// index already had an entry for the hash.
    already_warm: usize,
    /// Entries the scanner could not warm due to ReadPool errors,
    /// short reads, or dedup-index probe failures.
    errors: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct IndexScrubStats {
    checked: usize,
    deleted: usize,
    errors: usize,
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
