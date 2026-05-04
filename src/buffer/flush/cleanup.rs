use super::*;

const CLEANUP_PBA_COMMIT_LIMIT: usize = 256;

impl BufferFlusher {
    fn meta_lock_id(meta: &MetaStore) -> usize {
        meta as *const MetaStore as usize
    }

    pub(crate) fn cleanup_dead_pba_post_commit(
        meta: &MetaStore,
        allocator: &SpaceAllocator,
        candidate: &crate::dedup::CandidateCache,
        old_pba: Pba,
        old_blocks: u32,
        context: &'static str,
    ) {
        let meta_lock_id = Self::meta_lock_id(meta);
        {
            let cleanup_lock = Self::cleanup_lock(meta_lock_id, old_pba);
            let _cleanup_guard = cleanup_lock.lock().unwrap();
            if Self::mark_pba_cleaning(meta_lock_id, old_pba) {
                return;
            }

            let remaining = match meta.get_refcount(old_pba) {
                Ok(remaining) => remaining,
                Err(e) => {
                    tracing::error!(
                        pba = old_pba.0,
                        old_blocks,
                        error = %e,
                        context,
                        "post-commit cleanup: failed to confirm dead PBA; leaving allocator reservation"
                    );
                    Self::unmark_pba_cleaning(meta_lock_id, old_pba);
                    return;
                }
            };
            if remaining != 0 {
                Self::unmark_pba_cleaning(meta_lock_id, old_pba);
                return;
            }
        }

        // Drop any RAM candidate-cache entries pointing at this PBA
        // BEFORE the allocator can hand the sector to a new owner.
        // Otherwise a future verify pass would read the new owner's
        // bytes through a stale candidate slot, miss the byte
        // compare, and waste the IO. Cleanup proceeds even if the
        // metadb dedup-table cleanup later fails — the candidate
        // entry is RAM-only and safe to drop unconditionally.
        candidate.remove_by_pba(old_pba);

        if let Err(e) = meta.cleanup_dedup_for_pba_standalone(old_pba) {
            tracing::error!(
                pba = old_pba.0,
                old_blocks,
                error = %e,
                context,
                "post-commit cleanup: failed to cleanup dedup metadata; leaving allocator reservation"
            );
            Self::unmark_pba_cleaning(meta_lock_id, old_pba);
            return;
        }

        {
            let cleanup_lock = Self::cleanup_lock(meta_lock_id, old_pba);
            let _cleanup_guard = cleanup_lock.lock().unwrap();
            let already_free = if old_blocks <= 1 {
                allocator.is_free(old_pba)
            } else {
                allocator.is_extent_free(Extent::new(old_pba, old_blocks))
            };
            if already_free {
                Self::unmark_pba_cleaning(meta_lock_id, old_pba);
                return;
            }

            #[cfg(test)]
            CLEANUP_FREE_ATTEMPTS.fetch_add(1, Ordering::SeqCst);

            tracing::debug!(
                pba = old_pba.0,
                old_blocks,
                context,
                "cleanup_dead_pba: freeing PBA to allocator"
            );

            let free_result = if old_blocks <= 1 {
                allocator.free_one(old_pba)
            } else {
                allocator.free_extent(Extent::new(old_pba, old_blocks))
            };
            if let Err(e) = free_result {
                tracing::warn!(
                    pba = old_pba.0,
                    old_blocks,
                    error = %e,
                    context,
                    "post-commit cleanup: allocator free failed after metadata commit (benign if already freed by another path); continuing without retry"
                );
            }
            Self::unmark_pba_cleaning(meta_lock_id, old_pba);
        }
    }

    /// Batch cleanup for multiple dead PBAs: one WriteBatch for all dedup
    /// metadata, then per-PBA allocator free. Used by the async cleanup thread.
    pub(super) fn cleanup_dead_pbas_batch(
        meta: &MetaStore,
        allocator: &SpaceAllocator,
        candidate: &crate::dedup::CandidateCache,
        dead_pbas: &[(Pba, u32)],
        context: &'static str,
    ) {
        if dead_pbas.is_empty() {
            return;
        }
        let meta_lock_id = Self::meta_lock_id(meta);

        let mut candidates_by_pba: HashMap<Pba, u32> = HashMap::new();
        for &(pba, blocks) in dead_pbas {
            candidates_by_pba
                .entry(pba)
                .and_modify(|existing| *existing = (*existing).max(blocks))
                .or_insert(blocks);
        }
        let mut candidates: Vec<(Pba, u32)> = candidates_by_pba.into_iter().collect();
        candidates.sort_unstable_by_key(|(pba, _)| *pba);

        // Process in bounded chunks. The slow reverse-index scan runs outside
        // PBA locks; locks only guard the transition into/out of the "cleaning"
        // set and allocator free. Dedup registration treats "cleaning" as a
        // best-effort skip for that PBA, which is safe because the register
        // path can always be reconstructed by later writes/scans.
        for candidate_chunk in candidates.chunks(CLEANUP_PBA_COMMIT_LIMIT) {
            let pbas: Vec<Pba> = candidate_chunk
                .iter()
                .map(|(pba, _)| *pba)
                .filter(|pba| {
                    let cleanup_lock = Self::cleanup_lock(meta_lock_id, *pba);
                    let _cleanup_guard = cleanup_lock.lock().unwrap();
                    !Self::mark_pba_cleaning(meta_lock_id, *pba)
                })
                .collect();
            if pbas.is_empty() {
                continue;
            }
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
            let refcount_by_pba: HashMap<Pba, u32> = pbas.iter().copied().zip(refcounts).collect();

            let mut truly_dead: Vec<(Pba, u32)> = Vec::new();
            for &(pba, blocks) in candidate_chunk {
                if !refcount_by_pba.contains_key(&pba) {
                    continue;
                }
                let remaining = refcount_by_pba[&pba];
                if remaining != 0 {
                    Self::unmark_pba_cleaning(meta_lock_id, pba);
                    continue;
                }

                let already_free = if blocks <= 1 {
                    allocator.is_free(pba)
                } else {
                    allocator.is_extent_free(Extent::new(pba, blocks))
                };
                if already_free {
                    Self::unmark_pba_cleaning(meta_lock_id, pba);
                    continue;
                }

                truly_dead.push((pba, blocks));
            }

            if truly_dead.is_empty() {
                continue;
            }

            // Drop candidate-cache entries pointing at the dead PBAs
            // BEFORE the allocator can hand any of them to a new
            // owner (mirrors the single-PBA path above). The cache
            // is RAM-only so the cleanup is unconditional and never
            // fails.
            for (pba, _) in &truly_dead {
                candidate.remove_by_pba(*pba);
            }

            let pbas: Vec<Pba> = truly_dead.iter().map(|(p, _)| *p).collect();
            if let Err(e) = meta.cleanup_dedup_for_pbas_batch(&pbas) {
                tracing::error!(
                    count = pbas.len(),
                    error = %e,
                    context,
                    "batch cleanup: dedup metadata cleanup failed; skipping allocator free for chunk"
                );
                for pba in pbas {
                    Self::unmark_pba_cleaning(meta_lock_id, pba);
                }
                continue;
            }

            for &(pba, blocks) in &truly_dead {
                let cleanup_lock = Self::cleanup_lock(meta_lock_id, pba);
                let _cleanup_guard = cleanup_lock.lock().unwrap();
                #[cfg(test)]
                CLEANUP_FREE_ATTEMPTS.fetch_add(1, Ordering::SeqCst);

                tracing::debug!(
                    pba = pba.0,
                    blocks,
                    context,
                    "batch cleanup: freeing PBA to allocator"
                );

                let free_result = if blocks <= 1 {
                    allocator.free_one(pba)
                } else {
                    allocator.free_extent(Extent::new(pba, blocks))
                };
                if let Err(e) = free_result {
                    tracing::warn!(
                        pba = pba.0,
                        blocks,
                        error = %e,
                        context,
                        "batch cleanup: allocator free failed (benign if already freed); continuing"
                    );
                }
                Self::unmark_pba_cleaning(meta_lock_id, pba);
            }
        }
    }

    /// Async cleanup thread: receives dead PBAs from writer via channel,
    /// accumulates batches, and processes them with one WriteBatch.
    pub(super) fn cleanup_loop(
        shard_idx: usize,
        rx: &Receiver<Vec<(Pba, u32)>>,
        meta: &MetaStore,
        allocator: &SpaceAllocator,
        candidate: &crate::dedup::CandidateCache,
        running: &AtomicBool,
        metrics: &EngineMetrics,
    ) {
        while running.load(Ordering::Relaxed) {
            // Block on first batch
            let first = match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(items) => items,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            };

            // Drain more batches non-blocking to accumulate
            let mut all: Vec<(Pba, u32)> = first;
            while let Ok(more) = rx.try_recv() {
                all.extend(more);
            }

            let count = all.len();
            let start = Instant::now();
            Self::cleanup_dead_pbas_batch(meta, allocator, candidate, &all, "cleanup_thread");
            let elapsed_ns = start.elapsed().as_nanos() as u64;
            metrics
                .flush_writer_cleanup_ns
                .fetch_add(elapsed_ns, Ordering::Relaxed);
            tracing::debug!(
                shard = shard_idx,
                pbas = count,
                elapsed_us = elapsed_ns / 1000,
                "cleanup thread: batch processed"
            );
        }

        // Drain remaining on shutdown
        let mut remaining: Vec<(Pba, u32)> = Vec::new();
        while let Ok(batch) = rx.try_recv() {
            remaining.extend(batch);
        }
        if !remaining.is_empty() {
            Self::cleanup_dead_pbas_batch(meta, allocator, candidate, &remaining, "cleanup_thread_drain");
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

    fn mark_pba_cleaning(meta_lock_id: usize, pba: Pba) -> bool {
        !Self::pba_cleaning_set()
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
