use super::*;
const CLEANUP_PBA_COMMIT_LIMIT: usize = 256;

impl BufferFlusher {
    fn meta_lock_id(meta: &MetaStore) -> usize {
        meta as *const MetaStore as usize
    }

    pub(crate) fn cleanup_dead_pba_post_commit(
        meta: &MetaStore,
        allocator: &SpaceAllocator,
        _io_engine: &IoEngine,
        _metrics: &EngineMetrics,
        candidate: &crate::dedup::CandidateCache,
        cleanup: RemapCleanup,
        context: &'static str,
    ) {
        Self::free_dead_pbas(meta, allocator, candidate, &[cleanup], context);
    }

    pub(crate) fn repair_stale_dedup_index(
        meta: &MetaStore,
        metrics: &EngineMetrics,
        repairs: &[(ContentHash, DedupEntry, DedupEntry)],
        context: &'static str,
    ) {
        for (hash, old_entry, new_entry) in repairs {
            match meta.compare_put_dedup_index(hash, old_entry, new_entry) {
                Ok(true) => {
                    tracing::debug!(
                        old_pba = old_entry.pba.0,
                        new_pba = new_entry.pba.0,
                        context,
                        "dedup repair: replaced stale forward dedup entry"
                    );
                }
                Ok(false) => {}
                Err(e) => {
                    metrics
                        .dedup_cleanup_delete_errors
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        old_pba = old_entry.pba.0,
                        new_pba = new_entry.pba.0,
                        context,
                        error = %e,
                        "dedup repair: failed conditional forward dedup update"
                    );
                }
            }
        }
    }

    /// Batch cleanup for replaced mappings. `dedup_index` is a verified cache,
    /// so this path no longer tries to infer hashes by reading old PBAs; that
    /// is unsafe once a freed PBA can be reused. Foreground verify mismatch
    /// repair and the background scrubber maintain the forward cache.
    pub(crate) fn cleanup_dead_pbas_batch(
        meta: &MetaStore,
        allocator: &SpaceAllocator,
        _io_engine: &IoEngine,
        _metrics: &EngineMetrics,
        candidate: &crate::dedup::CandidateCache,
        cleanups: &[RemapCleanup],
        context: &'static str,
    ) {
        if cleanups.is_empty() {
            return;
        }
        Self::free_dead_pbas(meta, allocator, candidate, cleanups, context);
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
