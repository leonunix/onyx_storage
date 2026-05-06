use super::*;

impl BufferFlusher {
    pub(crate) fn cleanup_dead_pba_post_commit(
        allocator: &SpaceAllocator,
        candidate: &crate::dedup::CandidateCache,
        cleanup: RemapCleanup,
        context: &'static str,
    ) {
        Self::retire_dead_pbas(allocator, candidate, &[cleanup], context);
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
        allocator: &SpaceAllocator,
        candidate: &crate::dedup::CandidateCache,
        cleanups: &[RemapCleanup],
        context: &'static str,
    ) {
        if cleanups.is_empty() {
            return;
        }
        Self::retire_dead_pbas(allocator, candidate, cleanups, context);
    }

    fn retire_dead_pbas(
        allocator: &SpaceAllocator,
        candidate: &crate::dedup::CandidateCache,
        cleanups: &[RemapCleanup],
        context: &'static str,
    ) {
        let mut candidates_by_pba: HashMap<Pba, RemapCleanup> = HashMap::new();
        for cleanup in cleanups.iter().filter(|cleanup| cleanup.pba_freed) {
            candidates_by_pba
                .entry(cleanup.pba)
                .and_modify(|existing| existing.merge(cleanup.clone()))
                .or_insert_with(|| cleanup.clone());
        }
        let mut candidates: Vec<RemapCleanup> = candidates_by_pba.into_values().collect();
        candidates.sort_unstable_by_key(|cleanup| cleanup.pba);

        for cleanup in candidates {
            if allocator.is_retired(cleanup.pba) {
                continue;
            }

            candidate.remove_by_pba(cleanup.pba);

            tracing::debug!(
                pba = cleanup.pba.0,
                blocks = cleanup.blocks,
                context,
                "cleanup_dead_pba: retired PBA for GC reclaim"
            );

            let retire_result = if cleanup.blocks <= 1 {
                allocator.retire_one(cleanup.pba)
            } else {
                allocator.retire_extent(Extent::new(cleanup.pba, cleanup.blocks))
            };
            if let Err(e) = retire_result {
                tracing::warn!(
                    pba = cleanup.pba.0,
                    blocks = cleanup.blocks,
                    error = %e,
                    context,
                    "post-commit cleanup: allocator retire failed after metadata commit; continuing without retry"
                );
            }
        }
    }

    /// Async cleanup thread: receives old mappings from writer via channel,
    /// accumulates batches, and processes them off the write hot path.
    pub(super) fn cleanup_loop(
        shard_idx: usize,
        rx: &Receiver<CleanupBatch>,
        allocator: &SpaceAllocator,
        candidate: &crate::dedup::CandidateCache,
        running: &AtomicBool,
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
                allocator,
                candidate,
                &all,
                "cleanup_thread",
            );
            let elapsed_ns = start.elapsed().as_nanos() as u64;
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
                allocator,
                candidate,
                &remaining,
                "cleanup_thread_drain",
            );
        }
    }
}
