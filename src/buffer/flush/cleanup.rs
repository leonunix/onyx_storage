use super::*;
use std::collections::hash_map::Entry;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct CleanupRawKey {
    pba: Pba,
    read_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct CleanupUnitKey {
    pba: Pba,
    slot_offset: u16,
    unit_compressed_size: u32,
    compression: u8,
    unit_original_size: u32,
    crc32: u32,
}

const CLEANUP_PBA_COMMIT_LIMIT: usize = 256;

impl BufferFlusher {
    fn meta_lock_id(meta: &MetaStore) -> usize {
        meta as *const MetaStore as usize
    }

    pub(crate) fn cleanup_dead_pba_post_commit(
        meta: &MetaStore,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        metrics: &EngineMetrics,
        candidate: &crate::dedup::CandidateCache,
        cleanup: RemapCleanup,
        context: &'static str,
    ) {
        Self::cleanup_old_mappings(meta, io_engine, metrics, &[cleanup.clone()], context);
        Self::free_dead_pbas(meta, allocator, candidate, &[cleanup], context);
    }

    /// Batch cleanup for replaced mappings. Dedup metadata is cleaned by
    /// recomputing the old 4 KiB payload hash and conditionally deleting the
    /// matching forward dedup_index entry. Allocator frees only run for PBAs
    /// whose refcount actually reached zero.
    pub(crate) fn cleanup_dead_pbas_batch(
        meta: &MetaStore,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        metrics: &EngineMetrics,
        candidate: &crate::dedup::CandidateCache,
        cleanups: &[RemapCleanup],
        context: &'static str,
    ) {
        if cleanups.is_empty() {
            return;
        }
        Self::cleanup_old_mappings(meta, io_engine, metrics, cleanups, context);
        Self::free_dead_pbas(meta, allocator, candidate, cleanups, context);
    }

    fn cleanup_old_mappings(
        meta: &MetaStore,
        io_engine: &IoEngine,
        metrics: &EngineMetrics,
        cleanups: &[RemapCleanup],
        context: &'static str,
    ) {
        // Scoped to one cleanup batch so repeated fragments in the same packed
        // slot or compression unit share one LV3 read/decode. Very large
        // delete/discard operations filter non-freed mappings before this
        // point; remaining batch size is bounded by writer/cleanup channel
        // cadence rather than the full volume size.
        let mut raw_cache: HashMap<CleanupRawKey, Vec<u8>> = HashMap::new();
        let mut unit_cache: HashMap<CleanupUnitKey, Vec<u8>> = HashMap::new();
        for cleanup in cleanups {
            if !cleanup.pba_freed {
                continue;
            }
            for mapping in &cleanup.mappings {
                if mapping.is_zero() {
                    continue;
                }
                let block = match Self::read_cleanup_lba_block(
                    io_engine,
                    metrics,
                    &mut raw_cache,
                    &mut unit_cache,
                    mapping,
                ) {
                    Ok(block) => block,
                    Err(e) => {
                        metrics
                            .dedup_cleanup_reconstruct_errors
                            .fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            pba = mapping.pba.0,
                            slot_offset = mapping.slot_offset,
                            context,
                            error = %e,
                            "dedup cleanup: failed to reconstruct old block; forward dedup entry is not currently reclaimed"
                        );
                        continue;
                    }
                };
                let hash = crate::meta::schema::compute_content_hash(&block);
                match meta.delete_dedup_index_if_matches(&hash, mapping) {
                    Ok(true) => {
                        tracing::debug!(
                            pba = mapping.pba.0,
                            slot_offset = mapping.slot_offset,
                            context,
                            "dedup cleanup: removed matching forward dedup entry"
                        );
                    }
                    Ok(false) => {}
                    Err(e) => {
                        metrics
                            .dedup_cleanup_delete_errors
                            .fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            pba = mapping.pba.0,
                            slot_offset = mapping.slot_offset,
                            context,
                            error = %e,
                            "dedup cleanup: failed conditional forward dedup delete"
                        );
                    }
                }
            }
        }
    }

    fn read_cleanup_lba_block(
        io_engine: &IoEngine,
        metrics: &EngineMetrics,
        raw_cache: &mut HashMap<CleanupRawKey, Vec<u8>>,
        unit_cache: &mut HashMap<CleanupUnitKey, Vec<u8>>,
        mapping: &BlockmapValue,
    ) -> OnyxResult<Vec<u8>> {
        let read_size = mapping.compressed_read_size(BLOCK_SIZE as usize);
        let raw_key = CleanupRawKey {
            pba: mapping.pba,
            read_size,
        };
        let raw = match raw_cache.entry(raw_key) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(io_engine.read_blocks(mapping.pba, read_size)?),
        };

        let unit_key = CleanupUnitKey {
            pba: mapping.pba,
            slot_offset: mapping.slot_offset,
            unit_compressed_size: mapping.unit_compressed_size,
            compression: mapping.compression,
            unit_original_size: mapping.unit_original_size,
            crc32: mapping.crc32,
        };
        let unit = match unit_cache.entry(unit_key) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let payload = crate::zone::read::decode_unit(raw, mapping, metrics)?;
                entry.insert(payload.into_owned())
            }
        };
        let payload = crate::zone::read::UnitPayload::Borrowed(unit.as_slice());
        crate::zone::read::slice_lba(&payload, mapping.offset_in_unit).map(|block| block.to_vec())
    }

    fn free_dead_pbas(
        meta: &MetaStore,
        allocator: &SpaceAllocator,
        candidate: &crate::dedup::CandidateCache,
        cleanups: &[RemapCleanup],
        context: &'static str,
    ) {
        let meta_lock_id = Self::meta_lock_id(meta);
        let mut candidates_by_pba: HashMap<Pba, RemapCleanup> = HashMap::new();
        for cleanup in cleanups.iter().filter(|cleanup| cleanup.pba_freed) {
            candidates_by_pba
                .entry(cleanup.pba)
                .and_modify(|existing| existing.merge(cleanup.clone()))
                .or_insert_with(|| cleanup.clone());
        }
        let mut candidates: Vec<RemapCleanup> = candidates_by_pba.into_values().collect();
        candidates.sort_unstable_by_key(|cleanup| cleanup.pba);

        for candidate_chunk in candidates.chunks(CLEANUP_PBA_COMMIT_LIMIT) {
            let mut locked = Vec::new();
            for cleanup in candidate_chunk {
                let pba = cleanup.pba;
                let cleanup_lock = Self::cleanup_lock(meta_lock_id, pba);
                let _cleanup_guard = cleanup_lock.lock().unwrap();
                if Self::try_mark_pba_cleaning(meta_lock_id, pba) {
                    locked.push(cleanup.clone());
                }
            }
            if locked.is_empty() {
                continue;
            }

            let pbas: Vec<Pba> = locked.iter().map(|cleanup| cleanup.pba).collect();
            let refcounts = match meta.multi_get_refcounts(&pbas) {
                Ok(refcounts) => refcounts,
                Err(e) => {
                    tracing::error!(
                        count = pbas.len(),
                        error = %e,
                        context,
                        "batch cleanup: failed to confirm dead PBAs; skipping chunk"
                    );
                    for pba in pbas {
                        Self::unmark_pba_cleaning(meta_lock_id, pba);
                    }
                    continue;
                }
            };

            for (cleanup, remaining) in locked.into_iter().zip(refcounts) {
                if remaining != 0 {
                    Self::unmark_pba_cleaning(meta_lock_id, cleanup.pba);
                    continue;
                }

                let already_free = if cleanup.blocks <= 1 {
                    allocator.is_free(cleanup.pba)
                } else {
                    allocator.is_extent_free(Extent::new(cleanup.pba, cleanup.blocks))
                };
                if already_free {
                    Self::unmark_pba_cleaning(meta_lock_id, cleanup.pba);
                    continue;
                }

                candidate.remove_by_pba(cleanup.pba);

                let cleanup_lock = Self::cleanup_lock(meta_lock_id, cleanup.pba);
                let _cleanup_guard = cleanup_lock.lock().unwrap();
                #[cfg(test)]
                CLEANUP_FREE_ATTEMPTS.fetch_add(1, Ordering::SeqCst);

                tracing::debug!(
                    pba = cleanup.pba.0,
                    blocks = cleanup.blocks,
                    context,
                    "cleanup_dead_pba: freeing PBA to allocator"
                );

                let free_result = if cleanup.blocks <= 1 {
                    allocator.free_one(cleanup.pba)
                } else {
                    allocator.free_extent(Extent::new(cleanup.pba, cleanup.blocks))
                };
                if let Err(e) = free_result {
                    tracing::warn!(
                        pba = cleanup.pba.0,
                        blocks = cleanup.blocks,
                        error = %e,
                        context,
                        "post-commit cleanup: allocator free failed after metadata commit (benign if already freed by another path); continuing without retry"
                    );
                }
                Self::unmark_pba_cleaning(meta_lock_id, cleanup.pba);
            }
        }
    }

    /// Async cleanup thread: receives old mappings from writer via channel,
    /// accumulates batches, and processes them off the write hot path.
    pub(super) fn cleanup_loop(
        shard_idx: usize,
        rx: &Receiver<CleanupBatch>,
        meta: &MetaStore,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        candidate: &crate::dedup::CandidateCache,
        running: &AtomicBool,
        metrics: &EngineMetrics,
    ) {
        while running.load(Ordering::Relaxed) {
            let first = match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(items) => items,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            };

            let mut all = first;
            while let Ok(more) = rx.try_recv() {
                all.extend(more);
            }

            let count = all.len();
            let start = Instant::now();
            Self::cleanup_dead_pbas_batch(
                meta,
                allocator,
                io_engine,
                metrics,
                candidate,
                &all,
                "cleanup_thread",
            );
            let elapsed_ns = start.elapsed().as_nanos() as u64;
            metrics
                .flush_writer_cleanup_ns
                .fetch_add(elapsed_ns, Ordering::Relaxed);
            tracing::debug!(
                shard = shard_idx,
                mappings = count,
                elapsed_us = elapsed_ns / 1000,
                "cleanup thread: batch processed"
            );
        }

        let mut remaining = Vec::new();
        while let Ok(batch) = rx.try_recv() {
            remaining.extend(batch);
        }
        if !remaining.is_empty() {
            Self::cleanup_dead_pbas_batch(
                meta,
                allocator,
                io_engine,
                metrics,
                candidate,
                &remaining,
                "cleanup_thread_drain",
            );
        }
    }

    pub(super) fn pba_lock(meta_lock_id: usize, pba: Pba) -> Arc<Mutex<()>> {
        let mut locks = PBA_LOCKS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap();
        // Prune stale entries where the map is the sole holder (strong_count == 1).
        // Amortised: only prune when the map exceeds a reasonable threshold.
        if locks.len() > 4096 {
            locks.retain(|_, arc| Arc::strong_count(arc) > 1);
        }
        locks
            .entry((meta_lock_id, pba))
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub(crate) fn cleanup_lock(meta_lock_id: usize, pba: Pba) -> Arc<Mutex<()>> {
        Self::pba_lock(meta_lock_id, pba)
    }

    fn pba_cleaning_set() -> &'static Mutex<HashSet<PbaLockKey>> {
        PBA_CLEANING.get_or_init(|| Mutex::new(HashSet::new()))
    }

    /// Returns true when this caller acquired cleanup ownership for `pba`.
    fn try_mark_pba_cleaning(meta_lock_id: usize, pba: Pba) -> bool {
        Self::pba_cleaning_set()
            .lock()
            .unwrap()
            .insert((meta_lock_id, pba))
    }

    fn unmark_pba_cleaning(meta_lock_id: usize, pba: Pba) {
        Self::pba_cleaning_set()
            .lock()
            .unwrap()
            .remove(&(meta_lock_id, pba));
    }
}
