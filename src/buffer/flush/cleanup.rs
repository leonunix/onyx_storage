use super::*;
use crate::space::pba_lifecycle::PbaLifecycle;

impl BufferFlusher {
    pub(crate) fn cleanup_dead_pba_post_commit(
        pba_lifecycle: &PbaLifecycle,
        cleanup: RemapCleanup,
        context: &'static str,
    ) {
        Self::retire_dead_pbas(pba_lifecycle, &[cleanup], context);
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
        pba_lifecycle: &PbaLifecycle,
        cleanups: &[RemapCleanup],
        context: &'static str,
    ) {
        // Re-drive any deferred retire retries even when this batch is empty,
        // so a transient retire failure does not leak space until restart.
        pba_lifecycle.drive_retire_retries();
        if cleanups.is_empty() {
            return;
        }
        Self::retire_dead_pbas(pba_lifecycle, cleanups, context);
    }

    fn retire_dead_pbas(
        pba_lifecycle: &PbaLifecycle,
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
            // candidate-evict → retire → defer-on-failure (with retry queue)
            // all live in PbaLifecycle::retire_committed now.
            let extent = if cleanup.blocks <= 1 {
                Extent::single(cleanup.pba)
            } else {
                Extent::new(cleanup.pba, cleanup.blocks)
            };
            pba_lifecycle.retire_committed(context, extent);
        }
    }

    /// Async cleanup thread: receives old mappings from writer via channel,
    /// accumulates batches, and processes them off the write hot path.
    ///
    /// Per-batch elapsed nanoseconds are accumulated into
    /// `metrics.flush_cleanup_thread_ns` (sibling to `flush_writer_cleanup_ns`,
    /// which records strictly the writer-inline post-commit work).
    /// `flush_cleanup_thread_batches` increments once per batch so dashboards
    /// can compute per-batch averages — `flush_units_written` is the wrong
    /// denominator here because batches coalesce across many units.
    pub(super) fn cleanup_loop(
        shard_idx: usize,
        rx: &Receiver<CleanupBatch>,
        pba_lifecycle: &PbaLifecycle,
        running: &AtomicBool,
        metrics: &EngineMetrics,
    ) {
        while running.load(Ordering::Relaxed) {
            let first = match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(items) => items,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    // Idle tick: re-drive deferred retire retries so a
                    // transient failure does not leak space indefinitely.
                    pba_lifecycle.drive_retire_retries();
                    continue;
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            };

            let mut all = first;
            while let Ok(more) = rx.try_recv() {
                all.extend(more);
            }

            let count = all.len();
            let start = Instant::now();
            Self::cleanup_dead_pbas_batch(pba_lifecycle, &all, "cleanup_thread");
            let elapsed_ns = start.elapsed().as_nanos() as u64;
            metrics
                .flush_cleanup_thread_ns
                .fetch_add(elapsed_ns, Ordering::Relaxed);
            metrics
                .flush_cleanup_thread_batches
                .fetch_add(1, Ordering::Relaxed);
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
            let start = Instant::now();
            Self::cleanup_dead_pbas_batch(pba_lifecycle, &remaining, "cleanup_thread_drain");
            let elapsed_ns = start.elapsed().as_nanos() as u64;
            metrics
                .flush_cleanup_thread_ns
                .fetch_add(elapsed_ns, Ordering::Relaxed);
            metrics
                .flush_cleanup_thread_batches
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}
