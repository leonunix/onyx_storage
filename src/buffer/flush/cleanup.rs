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

            let remaining = match meta.get_refcount(pba) {
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

    /// Background dedup registration: writer commits blockmap/refcount first,
    /// then hands these best-effort rows here. We revalidate under the same PBA
    /// cleanup locks used by allocator free so a late registration cannot race
    /// with PBA reclamation and leave a stale dedup_index pointer behind.
    pub(super) fn dedup_register_loop(
        shard_idx: usize,
        rx: &Receiver<Vec<DedupRegistration>>,
        meta: &MetaStore,
        metrics: &EngineMetrics,
    ) {
        const BATCH_WAIT: Duration = Duration::from_micros(500);
        const BATCH_LIMIT: usize = 8192;

        loop {
            let first = match rx.recv() {
                Ok(items) => items,
                Err(_) => break,
            };
            let mut all = first;
            let deadline = Instant::now() + BATCH_WAIT;
            let mut disconnected = false;

            while all.len() < BATCH_LIMIT {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match rx.recv_timeout(remaining) {
                    Ok(mut more) => all.append(&mut more),
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => break,
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                        disconnected = true;
                        break;
                    }
                }
            }

            let count = all.len();
            let start = Instant::now();
            Self::register_dedup_batch(meta, &all, "dedup_register_thread");
            Self::record_elapsed(&metrics.flush_writer_dedup_index_ns, start);
            tracing::debug!(
                shard = shard_idx,
                registrations = count,
                "dedup register thread: batch processed"
            );

            if disconnected {
                break;
            }
        }

        let mut remaining = Vec::new();
        while let Ok(mut batch) = rx.try_recv() {
            remaining.append(&mut batch);
        }
        if !remaining.is_empty() {
            let start = Instant::now();
            Self::register_dedup_batch(meta, &remaining, "dedup_register_thread_drain");
            Self::record_elapsed(&metrics.flush_writer_dedup_index_ns, start);
        }
    }

    fn register_dedup_batch(
        meta: &MetaStore,
        registrations: &[DedupRegistration],
        context: &'static str,
    ) {
        if registrations.is_empty() {
            return;
        }

        let mut pbas: Vec<Pba> = registrations.iter().map(|reg| reg.entry.pba).collect();
        pbas.sort_unstable();
        pbas.dedup();
        let locks: Vec<_> = pbas.iter().map(|pba| Self::cleanup_lock(*pba)).collect();
        let _guards: Vec<_> = locks.iter().map(|lock| lock.lock().unwrap()).collect();

        let mut by_vol: HashMap<VolumeId, Vec<(usize, Lba)>> = HashMap::new();
        for (idx, reg) in registrations.iter().enumerate() {
            by_vol
                .entry(reg.vol_id.clone())
                .or_default()
                .push((idx, reg.lba));
        }

        let mut valid: Vec<(ContentHash, DedupEntry)> = Vec::new();
        for (vol_id, items) in by_vol {
            let lbas: Vec<Lba> = items.iter().map(|(_, lba)| *lba).collect();
            let mappings = match meta.multi_get_mappings(&vol_id, &lbas) {
                Ok(mappings) => mappings,
                Err(e) => {
                    tracing::warn!(
                        vol = %vol_id,
                        count = items.len(),
                        error = %e,
                        context,
                        "dedup register: failed to validate blockmap batch"
                    );
                    continue;
                }
            };

            for ((idx, _), mapping) in items.into_iter().zip(mappings.into_iter()) {
                let reg = &registrations[idx];
                if mapping != Some(reg.expected) {
                    continue;
                }
                match meta.get_refcount(reg.entry.pba) {
                    Ok(0) => continue,
                    Ok(_) => valid.push((reg.hash, reg.entry)),
                    Err(e) => {
                        tracing::warn!(
                            pba = reg.entry.pba.0,
                            error = %e,
                            context,
                            "dedup register: failed to validate refcount"
                        );
                    }
                }
            }
        }

        if valid.is_empty() {
            return;
        }
        if let Err(e) = meta.put_dedup_entries(&valid) {
            tracing::warn!(
                count = valid.len(),
                error = %e,
                context,
                "dedup register: batched put_dedup_entries failed"
            );
        }
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
