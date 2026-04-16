use super::*;

mod packed;
mod passthrough;

impl BufferFlusher {
    pub(super) const WRITER_BATCH_SIZE: usize = 32;
    pub(super) const RETRY_BACKOFF: Duration = Duration::from_secs(1);
    pub(super) const PACKED_SLOT_MAX_AGE: Duration = Duration::from_millis(50);

    pub(super) fn writer_loop(
        shard_idx: usize,
        rx: &Receiver<CompressedUnit>,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        done_tx: &Sender<Vec<u64>>,
        running: &AtomicBool,
        in_flight_tracker: &FlusherInFlightTracker,
        packer: &mut Packer,
        metrics: &EngineMetrics,
        cleanup_tx: &Sender<Vec<(Pba, u32)>>,
    ) {
        let mut buffered_seqs: Vec<u64> = Vec::new();
        let mut buffered_completions: Vec<Arc<crate::buffer::pipeline::DedupCompletion>> =
            Vec::new();
        let mut packed_retries: VecDeque<PackedSlotRetry> = VecDeque::new();
        let mut tail_dirty = false;

        /// Helper: flush accumulated Passthrough units through write_units_batch.
        macro_rules! flush_pt_batch {
            ($batch:expr, $batch_seqs:expr, $batch_completions:expr) => {
                if !$batch.is_empty() {
                    let results = Self::write_units_batch(
                        shard_idx, &$batch, pool, meta, lifecycle, allocator,
                        io_engine, metrics, cleanup_tx,
                    );
                    for (idx, result) in results.into_iter().enumerate() {
                        if let Err(e) = result {
                            metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                            match &$batch_completions[idx] {
                                None => {
                                    in_flight_tracker
                                        .defer_retry(&$batch_seqs[idx], Self::RETRY_BACKOFF);
                                }
                                Some(dc) => {
                                    in_flight_tracker
                                        .defer_retry(dc.seqs(), Self::RETRY_BACKOFF);
                                }
                            }
                            tracing::error!(
                                vol = $batch[idx].vol_id,
                                start_lba = $batch[idx].start_lba.0,
                                error = %e,
                                "writer: failed to flush unit in batch"
                            );
                        }
                        match &$batch_completions[idx] {
                            None => { let _ = done_tx.send($batch_seqs[idx].clone()); }
                            Some(dc) => {
                                if let Some(original_seqs) = dc.decrement() {
                                    let _ = done_tx.send(original_seqs);
                                }
                            }
                        }
                    }
                    $batch.clear();
                    $batch_seqs.clear();
                    $batch_completions.clear();
                    tail_dirty = true;
                }
            };
        }

        while running.load(Ordering::Relaxed) {
            if Self::retry_one_packed_slot(
                shard_idx,
                &mut packed_retries,
                pool,
                meta,
                lifecycle,
                allocator,
                io_engine,
                done_tx,
                metrics,
                cleanup_tx,
            ) {
                tail_dirty = true;
            }

            if Self::flush_aged_open_slot(
                shard_idx,
                packer,
                &mut buffered_seqs,
                &mut buffered_completions,
                &mut packed_retries,
                pool,
                meta,
                lifecycle,
                allocator,
                io_engine,
                done_tx,
                metrics,
                cleanup_tx,
            ) {
                tail_dirty = true;
            }

            let first = match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(unit) => unit,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if let Some(sealed) = packer.flush_open_slot() {
                        if let Err(e) = Self::write_packed_slot(
                            shard_idx, &sealed, pool, meta, lifecycle, allocator, io_engine,
                            metrics, cleanup_tx,
                        ) {
                            metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                            let failed_pba = sealed.pba;
                            Self::queue_packed_slot_retry(
                                &mut packed_retries,
                                sealed,
                                &mut buffered_seqs,
                                &mut buffered_completions,
                            );
                            tracing::error!(
                                pba = failed_pba.0,
                                error = %e,
                                "writer: failed to flush packed slot on idle; queued whole-slot retry"
                            );
                        } else {
                            Self::flush_buffered_done(
                                &mut buffered_seqs,
                                &mut buffered_completions,
                                done_tx,
                            );
                            tail_dirty = true;
                        }
                    }
                    if tail_dirty {
                        let _ = pool.advance_tail_for_shard(shard_idx);
                        tail_dirty = false;
                    }
                    continue;
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            };

            // Drain up to WRITER_BATCH_SIZE units.
            let mut incoming = vec![first];
            while incoming.len() < Self::WRITER_BATCH_SIZE {
                match rx.try_recv() {
                    Ok(unit) => incoming.push(unit),
                    Err(_) => break,
                }
            }

            // Run through packer, collect Passthrough units for batching.
            let mut pt_batch: Vec<CompressedUnit> = Vec::new();
            let mut pt_seqs: Vec<Vec<u64>> = Vec::new();
            let mut pt_completions: Vec<Option<Arc<crate::buffer::pipeline::DedupCompletion>>> =
                Vec::new();

            for unit in incoming {
                let seqs: Vec<u64> = unit.seq_lba_ranges.iter().map(|(s, _, _)| *s).collect();
                let completion = unit.dedup_completion.clone();

                match packer.pack_or_passthrough(unit) {
                    Ok(PackResult::Passthrough(unit)) => {
                        pt_batch.push(unit);
                        pt_seqs.push(seqs);
                        pt_completions.push(completion);
                    }
                    Ok(PackResult::Buffered) => match &completion {
                        None => buffered_seqs.extend(&seqs),
                        Some(dc) => buffered_completions.push(dc.clone()),
                    },
                    Ok(PackResult::SealedSlot(sealed)) => {
                        flush_pt_batch!(pt_batch, pt_seqs, pt_completions);
                        if let Err(e) = Self::write_packed_slot(
                            shard_idx, &sealed, pool, meta, lifecycle, allocator, io_engine,
                            metrics, cleanup_tx,
                        ) {
                            metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                            let failed_pba = sealed.pba;
                            Self::queue_packed_slot_retry(
                                &mut packed_retries,
                                sealed,
                                &mut buffered_seqs,
                                &mut buffered_completions,
                            );
                            tracing::error!(
                                pba = failed_pba.0,
                                error = %e,
                                "writer: failed to flush packed slot; queued whole-slot retry"
                            );
                        } else {
                            Self::flush_buffered_done(
                                &mut buffered_seqs,
                                &mut buffered_completions,
                                done_tx,
                            );
                            tail_dirty = true;
                        }
                        match &completion {
                            None => buffered_seqs.extend(&seqs),
                            Some(dc) => buffered_completions.push(dc.clone()),
                        }
                    }
                    Ok(PackResult::SealedSlotAndPassthrough(sealed, unit)) => {
                        flush_pt_batch!(pt_batch, pt_seqs, pt_completions);
                        if let Err(e) = Self::write_packed_slot(
                            shard_idx, &sealed, pool, meta, lifecycle, allocator, io_engine,
                            metrics, cleanup_tx,
                        ) {
                            metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                            let failed_pba = sealed.pba;
                            Self::queue_packed_slot_retry(
                                &mut packed_retries,
                                sealed,
                                &mut buffered_seqs,
                                &mut buffered_completions,
                            );
                            tracing::error!(
                                pba = failed_pba.0,
                                error = %e,
                                "writer: failed to flush packed slot (passthrough fallback); queued whole-slot retry"
                            );
                        } else {
                            Self::flush_buffered_done(
                                &mut buffered_seqs,
                                &mut buffered_completions,
                                done_tx,
                            );
                            tail_dirty = true;
                        }
                        pt_batch.push(unit);
                        pt_seqs.push(seqs);
                        pt_completions.push(completion);
                    }
                    Err(e) => {
                        metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                        match &completion {
                            None => in_flight_tracker.defer_retry(&seqs, Self::RETRY_BACKOFF),
                            Some(dc) => {
                                in_flight_tracker.defer_retry(dc.seqs(), Self::RETRY_BACKOFF)
                            }
                        }
                        tracing::error!(error = %e, "writer: packer error");
                        match &completion {
                            None => {
                                let _ = done_tx.send(seqs);
                            }
                            Some(dc) => {
                                if let Some(original_seqs) = dc.decrement() {
                                    let _ = done_tx.send(original_seqs);
                                }
                            }
                        }
                    }
                }
            }

            flush_pt_batch!(pt_batch, pt_seqs, pt_completions);
        }

        // Drain remaining on shutdown (use per-unit path for simplicity).
        while let Ok(unit) = rx.try_recv() {
            Self::handle_compressed_unit(
                shard_idx,
                unit,
                pool,
                meta,
                lifecycle,
                allocator,
                io_engine,
                done_tx,
                packer,
                &mut buffered_seqs,
                &mut buffered_completions,
                metrics,
                cleanup_tx,
            );
            tail_dirty = true;
        }

        while Self::retry_one_packed_slot(
            shard_idx,
            &mut packed_retries,
            pool,
            meta,
            lifecycle,
            allocator,
            io_engine,
            done_tx,
            metrics,
            cleanup_tx,
        ) {
            tail_dirty = true;
        }

        if let Some(sealed) = packer.flush_open_slot() {
            if let Err(e) = Self::write_packed_slot(
                shard_idx, &sealed, pool, meta, lifecycle, allocator, io_engine, metrics,
                cleanup_tx,
            ) {
                metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                tracing::error!(pba = sealed.pba.0, error = %e,
                    "writer: failed to flush final packed slot on shutdown");
            }
            Self::flush_buffered_done(&mut buffered_seqs, &mut buffered_completions, done_tx);
            tail_dirty = true;
        }

        if tail_dirty {
            let _ = pool.advance_tail_for_shard(shard_idx);
        }
    }

    pub(super) fn flush_aged_open_slot(
        shard_idx: usize,
        packer: &mut Packer,
        buffered_seqs: &mut Vec<u64>,
        buffered_completions: &mut Vec<Arc<crate::buffer::pipeline::DedupCompletion>>,
        packed_retries: &mut VecDeque<PackedSlotRetry>,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        done_tx: &Sender<Vec<u64>>,
        metrics: &EngineMetrics,
        cleanup_tx: &Sender<Vec<(Pba, u32)>>,
    ) -> bool {
        let Some(sealed) = packer.flush_open_slot_if_older_than(Self::PACKED_SLOT_MAX_AGE) else {
            return false;
        };
        if let Err(e) = Self::write_packed_slot(
            shard_idx, &sealed, pool, meta, lifecycle, allocator, io_engine, metrics, cleanup_tx,
        ) {
            metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
            let failed_pba = sealed.pba;
            Self::queue_packed_slot_retry(
                packed_retries,
                sealed,
                buffered_seqs,
                buffered_completions,
            );
            tracing::error!(
                pba = failed_pba.0,
                error = %e,
                "writer: failed to flush aged packed slot; queued whole-slot retry"
            );
        } else {
            Self::flush_buffered_done(buffered_seqs, buffered_completions, done_tx);
        }
        true
    }

    /// Flush buffered done_tx for sealed packer slots.
    /// Handles both normal seqs and dedup completion counters.
    pub(super) fn flush_buffered_done(
        buffered_seqs: &mut Vec<u64>,
        buffered_completions: &mut Vec<Arc<crate::buffer::pipeline::DedupCompletion>>,
        done_tx: &Sender<Vec<u64>>,
    ) {
        // Normal (non-dedup) buffered seqs
        let normal_seqs: Vec<u64> = buffered_seqs.drain(..).collect();
        if !normal_seqs.is_empty() {
            let _ = done_tx.send(normal_seqs);
        }
        // Dedup completion counters
        for dc in buffered_completions.drain(..) {
            if let Some(original_seqs) = dc.decrement() {
                let _ = done_tx.send(original_seqs);
            }
        }
    }

    pub(super) fn queue_packed_slot_retry(
        retries: &mut VecDeque<PackedSlotRetry>,
        sealed: SealedSlot,
        buffered_seqs: &mut Vec<u64>,
        buffered_completions: &mut Vec<Arc<crate::buffer::pipeline::DedupCompletion>>,
    ) {
        retries.push_back(PackedSlotRetry {
            sealed,
            buffered_seqs: std::mem::take(buffered_seqs),
            buffered_completions: std::mem::take(buffered_completions),
            retry_at: Instant::now() + Self::RETRY_BACKOFF,
        });
    }

    pub(super) fn retry_one_packed_slot(
        shard_idx: usize,
        retries: &mut VecDeque<PackedSlotRetry>,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        done_tx: &Sender<Vec<u64>>,
        metrics: &EngineMetrics,
        cleanup_tx: &Sender<Vec<(Pba, u32)>>,
    ) -> bool {
        let Some(retry_at) = retries.front().map(|retry| retry.retry_at) else {
            return false;
        };
        if retry_at > Instant::now() {
            return false;
        }

        let mut retry = retries.pop_front().expect("front checked above");
        let new_pba = match allocator.allocate_one_for_lane(shard_idx) {
            Ok(pba) => pba,
            Err(e) => {
                metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                retry.retry_at = Instant::now() + Self::RETRY_BACKOFF;
                retries.push_back(retry);
                tracing::warn!(
                    lane = shard_idx,
                    error = %e,
                    "writer: failed to allocate PBA for packed-slot retry"
                );
                return false;
            }
        };
        retry.sealed.pba = new_pba;

        match Self::write_packed_slot(
            shard_idx,
            &retry.sealed,
            pool,
            meta,
            lifecycle,
            allocator,
            io_engine,
            metrics,
            cleanup_tx,
        ) {
            Ok(()) => {
                let mut buffered_seqs = retry.buffered_seqs;
                let mut buffered_completions = retry.buffered_completions;
                Self::flush_buffered_done(&mut buffered_seqs, &mut buffered_completions, done_tx);
                true
            }
            Err(e) => {
                metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                retry.retry_at = Instant::now() + Self::RETRY_BACKOFF;
                retries.push_back(retry);
                tracing::error!(
                    lane = shard_idx,
                    pba = new_pba.0,
                    error = %e,
                    "writer: packed-slot retry failed; will retry whole slot again"
                );
                false
            }
        }
    }

    /// Handle a compressed unit in the writer thread.
    pub(super) fn handle_compressed_unit(
        shard_idx: usize,
        unit: CompressedUnit,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        done_tx: &Sender<Vec<u64>>,
        packer: &mut Packer,
        buffered_seqs: &mut Vec<u64>,
        buffered_completions: &mut Vec<Arc<crate::buffer::pipeline::DedupCompletion>>,
        metrics: &EngineMetrics,
        cleanup_tx: &Sender<Vec<(Pba, u32)>>,
    ) {
        let seqs: Vec<u64> = unit.seq_lba_ranges.iter().map(|(s, _, _)| *s).collect();
        let completion = unit.dedup_completion.clone();

        /// Send done_tx for this unit's completion.
        /// - Normal path (no dedup_completion): send seqs directly.
        /// - Dedup split path: decrement the shared counter; only the last
        ///   sub-unit to finish sends done_tx with the ORIGINAL unit's full seqs.
        macro_rules! signal_done {
            ($own_seqs:expr) => {
                match &completion {
                    None => {
                        let _ = done_tx.send($own_seqs);
                    }
                    Some(dc) => {
                        if let Some(original_seqs) = dc.decrement() {
                            let _ = done_tx.send(original_seqs);
                        }
                    }
                }
            };
        }

        match packer.pack_or_passthrough(unit) {
            Ok(PackResult::Passthrough(unit)) => {
                if let Err(e) = Self::write_unit(
                    shard_idx, &unit, pool, meta, lifecycle, allocator, io_engine, metrics,
                    cleanup_tx,
                ) {
                    metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        vol = unit.vol_id,
                        start_lba = unit.start_lba.0,
                        lba_count = unit.lba_count,
                        error = %e,
                        "writer: failed to flush unit"
                    );
                }
                signal_done!(seqs);
            }
            Ok(PackResult::Buffered) => match &completion {
                None => buffered_seqs.extend(&seqs),
                Some(dc) => buffered_completions.push(dc.clone()),
            },
            Ok(PackResult::SealedSlot(sealed)) => {
                if let Err(e) = Self::write_packed_slot(
                    shard_idx, &sealed, pool, meta, lifecycle, allocator, io_engine, metrics,
                    cleanup_tx,
                ) {
                    metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        pba = sealed.pba.0,
                        fragments = sealed.fragments.len(),
                        error = %e,
                        "writer: failed to flush packed slot"
                    );
                }
                // Flush done_tx for previously buffered units in the sealed slot
                Self::flush_buffered_done(buffered_seqs, buffered_completions, done_tx);
                // Current unit goes into the new open slot
                match &completion {
                    None => buffered_seqs.extend(&seqs),
                    Some(dc) => buffered_completions.push(dc.clone()),
                }
            }
            Ok(PackResult::SealedSlotAndPassthrough(sealed, unit)) => {
                if let Err(e) = Self::write_packed_slot(
                    shard_idx, &sealed, pool, meta, lifecycle, allocator, io_engine, metrics,
                    cleanup_tx,
                ) {
                    metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        pba = sealed.pba.0,
                        error = %e,
                        "writer: failed to flush packed slot (alloc fallback)"
                    );
                }
                Self::flush_buffered_done(buffered_seqs, buffered_completions, done_tx);

                if let Err(e) = Self::write_unit(
                    shard_idx, &unit, pool, meta, lifecycle, allocator, io_engine, metrics,
                    cleanup_tx,
                ) {
                    metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        vol = unit.vol_id,
                        error = %e,
                        "writer: failed to flush unit (alloc fallback)"
                    );
                }
                signal_done!(seqs);
            }
            Err(e) => {
                metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                tracing::error!(error = %e, "writer: packer error");
                signal_done!(seqs);
            }
        }
    }
}
