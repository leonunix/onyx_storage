use super::*;

impl BufferFlusher {
    pub(crate) fn cleanup_dead_pba_post_commit(
        meta: &MetaStore,
        allocator: &SpaceAllocator,
        old_pba: Pba,
        old_blocks: u32,
        context: &'static str,
    ) {
        let cleanup_lock = Self::cleanup_lock(old_pba);
        let _cleanup_guard = cleanup_lock.lock().unwrap();

        let remaining = match Self::live_refcount_with_reconcile(meta, old_pba, context) {
            Ok(remaining) => remaining,
            Err(e) => {
                tracing::error!(
                    pba = old_pba.0,
                    old_blocks,
                    error = %e,
                    context,
                    "post-commit cleanup: failed to confirm dead PBA; leaving allocator reservation"
                );
                return;
            }
        };
        if remaining != 0 {
            return;
        }

        if let Err(e) = meta.cleanup_dedup_for_pba_standalone(old_pba) {
            tracing::error!(
                pba = old_pba.0,
                old_blocks,
                error = %e,
                context,
                "post-commit cleanup: failed to cleanup dedup metadata; leaving allocator reservation"
            );
            return;
        }

        let already_free = if old_blocks <= 1 {
            allocator.is_free(old_pba)
        } else {
            allocator.is_extent_free(Extent::new(old_pba, old_blocks))
        };
        if already_free {
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
    }

    /// Batch cleanup for multiple dead PBAs: one WriteBatch for all dedup
    /// metadata, then per-PBA allocator free. Used by the async cleanup thread.
    pub(super) fn cleanup_dead_pbas_batch(
        meta: &MetaStore,
        allocator: &SpaceAllocator,
        dead_pbas: &[(Pba, u32)],
        context: &'static str,
    ) {
        if dead_pbas.is_empty() {
            return;
        }

        // Phase 1: per-PBA lock + refcount verify → filter to truly dead
        let mut truly_dead: Vec<(Pba, u32)> = Vec::new();
        for &(pba, blocks) in dead_pbas {
            let cleanup_lock = Self::cleanup_lock(pba);
            let _cleanup_guard = cleanup_lock.lock().unwrap();

            let remaining = match Self::live_refcount_with_reconcile(meta, pba, context) {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(
                        pba = pba.0,
                        blocks,
                        error = %e,
                        context,
                        "batch cleanup: failed to confirm dead PBA; skipping"
                    );
                    continue;
                }
            };
            if remaining != 0 {
                continue;
            }

            let already_free = if blocks <= 1 {
                allocator.is_free(pba)
            } else {
                allocator.is_extent_free(Extent::new(pba, blocks))
            };
            if already_free {
                continue;
            }

            truly_dead.push((pba, blocks));
        }

        if truly_dead.is_empty() {
            return;
        }

        // Phase 2: ONE WriteBatch for all dedup cleanup
        let pbas: Vec<Pba> = truly_dead.iter().map(|(p, _)| *p).collect();
        if let Err(e) = meta.cleanup_dedup_for_pbas_batch(&pbas) {
            tracing::error!(
                count = pbas.len(),
                error = %e,
                context,
                "batch cleanup: dedup metadata cleanup failed; skipping allocator free"
            );
            return;
        }

        // Phase 3: per-PBA allocator free
        for (pba, blocks) in truly_dead {
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
        }
    }

    /// Async cleanup thread: receives dead PBAs from writer via channel,
    /// accumulates batches, and processes them with one WriteBatch.
    pub(super) fn cleanup_loop(
        shard_idx: usize,
        rx: &Receiver<Vec<(Pba, u32)>>,
        meta: &MetaStore,
        allocator: &SpaceAllocator,
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
            Self::cleanup_dead_pbas_batch(meta, allocator, &all, "cleanup_thread");
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
            Self::cleanup_dead_pbas_batch(meta, allocator, &remaining, "cleanup_thread_drain");
        }
    }

    // In release: trust the refcount from the metadata write path.
    // In debug: full reconcile scan to catch any drift.
    #[allow(unused_variables)]
    pub(super) fn live_refcount_with_reconcile(
        meta: &MetaStore,
        pba: Pba,
        context: &'static str,
    ) -> OnyxResult<u32> {
        let remaining = meta.get_refcount(pba)?;
        #[cfg(debug_assertions)]
        if remaining == 0 {
            let reconciled = meta.reconcile_refcount_for_pba(pba)?;
            if reconciled != 0 {
                tracing::error!(
                    pba = pba.0,
                    reconciled_refcount = reconciled,
                    context,
                    "detected refcount drift while preparing to free or inspect a PBA"
                );
            }
            return Ok(reconciled);
        }
        Ok(remaining)
    }

    pub(super) fn pba_lock(pba: Pba) -> Arc<Mutex<()>> {
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
            .entry(pba)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub(crate) fn cleanup_lock(pba: Pba) -> Arc<Mutex<()>> {
        Self::pba_lock(pba)
    }
}
