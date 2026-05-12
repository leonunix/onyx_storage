use super::*;

mod commit_worker;
mod packed;
mod passthrough;
mod post_commit;

pub(crate) use commit_worker::TARGET_OPS_PER_COMMIT;
pub(in crate::buffer::flush) use commit_worker::{
    route_volume_to_worker, CommitJob, PackedCommitJob, PassthroughCommitJob, UnitCommitData,
    COMMIT_WORKER_QUEUE_CAP, NUM_COMMIT_WORKERS,
};
pub(in crate::buffer::flush) use post_commit::{PostCommitJob, POST_COMMIT_QUEUE_CAP};

impl BufferFlusher {
    /// Maximum units a single writer cycle drains from `write_rx` and
    /// folds into one combined metadb commit (packed slots through
    /// `write_packed_slots_batch`, passthrough through `write_units_batch`).
    ///
    /// Bigger batch amortises the per-commit fixed cost — apply_gate
    /// dispatch FIFO + WAL fsync barrier + lane scheduling. Soak
    /// (2026-04-27) showed the bottleneck is **dispatch FIFO**: with
    /// 30 ms apply_wait per commit, throughput is gated by commits/sec
    /// not by per-op work, so 8x larger batches translate ~5x ops/sec.
    /// The NVMe mixed workload exposes metadb's per-commit tail more
    /// sharply than the old 4-lane soak: more buffer shards means more
    /// writer threads, so reducing commit count matters more than keeping
    /// each writer cycle tiny. 1024 keeps per-writer transient memory
    /// bounded while giving metadb enough work to amortise apply/WAL cost.
    ///
    /// Must stay paired with the bounded channel capacities in
    /// [`BufferFlusher::start_with_metrics`] — write_rx in particular
    /// caps at `WRITER_BATCH_SIZE`, so a smaller channel would silently
    /// starve the writer below this batch.
    pub(super) const WRITER_BATCH_SIZE: usize = 1024;
    /// Foreground reads are latency-sensitive, while flush writes are already
    /// decoupled by LV2. When reads are flowing, cap each writer drain so one
    /// background batch cannot monopolize LV3 IO and metadb apply for hundreds
    /// of milliseconds.
    ///
    /// The cap still has to be large enough to keep NVMe and metadb commits in
    /// their efficient batched regime. A 2026-05-09 mixed NVMe run showed 512
    /// starved background drain to ~2K LBA/s while 1024 lifted active drain
    /// into the 46-59K LBA/s range. Keep the read-active default at 1024.
    pub(crate) const WRITER_BATCH_SIZE_READ_ACTIVE: usize = 1024;
    /// After the first completed unit arrives, wait very briefly for the
    /// compress/dedup pipeline to hand over adjacent work. This keeps fast LV3
    /// writes from turning into many small metadb commits when the IO side gets
    /// cheaper than the upstream handoff cadence.
    pub(super) const WRITER_BATCH_COALESCE: Duration = Duration::from_micros(250);
    pub(super) const RETRY_BACKOFF: Duration = Duration::from_secs(1);
    pub(super) const PACKED_SLOT_MAX_AGE: Duration = Duration::from_millis(200);

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
        cleanup_tx: &Sender<CleanupBatch>,
        candidate: &crate::dedup::CandidateCache,
        // Retained for backward compat with the old per-batch
        // metadata commit cap; commit workers now use
        // TARGET_OPS_PER_COMMIT instead (per-volume sub-batch).
        _packed_meta_batch_max_lbas: usize,
        commit_worker_txs: &[Sender<CommitJob>],
        commit_workers_per_volume: usize,
        writer_read_active_batch_size: usize,
    ) {
        let mut buffered_seqs: Vec<u64> = Vec::new();
        let mut buffered_completions: Vec<Arc<crate::buffer::pipeline::DedupCompletion>> =
            Vec::new();
        let mut packed_retries: VecDeque<PackedSlotRetry> = VecDeque::new();
        let mut tail_dirty = false;
        let mut last_read_submit_calls = metrics.read_submit_calls.load(Ordering::Relaxed);

        /// Helper: hand off accumulated Passthrough units to the
        /// per-volume commit workers. Per-unit done_tx / defer_retry
        /// are now driven by the commit worker for successful IO and
        /// by `write_units_batch` itself for IO failures, so this
        /// macro only consumes the batch buffers and signals tail
        /// advance.
        macro_rules! flush_pt_batch {
            ($batch:expr, $batch_seqs:expr, $batch_completions:expr) => {
                if !$batch.is_empty() {
                    let units = std::mem::take(&mut $batch);
                    let seqs = std::mem::take(&mut $batch_seqs);
                    let completions = std::mem::take(&mut $batch_completions);
                    Self::write_units_batch(
                        shard_idx,
                        units,
                        seqs,
                        completions,
                        pool,
                        allocator,
                        io_engine,
                        metrics,
                        in_flight_tracker,
                        done_tx,
                        commit_worker_txs,
                        commit_workers_per_volume,
                    );
                    tail_dirty = true;
                }
            };
        }

        while running.load(Ordering::Relaxed) {
            if Self::retry_one_packed_slot(
                shard_idx,
                &mut packed_retries,
                pool,
                allocator,
                io_engine,
                done_tx,
                metrics,
                in_flight_tracker,
                commit_worker_txs,
                commit_workers_per_volume,
            ) {
                tail_dirty = true;
            }

            let first = match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(unit) => unit,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if let Some(sealed) = packer.flush_open_slot() {
                        let slot_buffers = (
                            std::mem::take(&mut buffered_seqs),
                            std::mem::take(&mut buffered_completions),
                        );
                        Self::write_packed_slots_batch(
                            shard_idx,
                            vec![sealed],
                            vec![slot_buffers],
                            pool,
                            allocator,
                            io_engine,
                            metrics,
                            in_flight_tracker,
                            done_tx,
                            &mut packed_retries,
                            commit_worker_txs,
                            commit_workers_per_volume,
                        );
                        tail_dirty = true;
                    }
                    if tail_dirty {
                        let _ = pool.advance_tail_for_shard(shard_idx);
                        tail_dirty = false;
                    }
                    continue;
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            };

            let read_submit_calls = metrics.read_submit_calls.load(Ordering::Relaxed);
            let read_active = read_submit_calls != last_read_submit_calls;
            last_read_submit_calls = read_submit_calls;
            let writer_batch_limit = if read_active {
                writer_read_active_batch_size
            } else {
                Self::WRITER_BATCH_SIZE
            };

            // Sample how much work is queued at cycle start. High values
            // mean compress/dedup is ahead of the writer — i.e. the
            // bottleneck is downstream (metadb commit / LV3 IO), not the
            // upstream pipeline.
            let rx_pending_at_start = rx.len() as u64 + 1; // +1 for `first`
            crate::metrics::record_counter_max(
                &metrics.flush_writer_rx_pending_max,
                rx_pending_at_start,
            );

            // Drain up to the current writer batch limit.
            let mut incoming = vec![first];
            let drain_deadline = Instant::now() + Self::WRITER_BATCH_COALESCE;
            while incoming.len() < writer_batch_limit {
                match rx.try_recv() {
                    Ok(unit) => incoming.push(unit),
                    Err(_) => {
                        let now = Instant::now();
                        if now >= drain_deadline {
                            break;
                        }
                        match rx.recv_timeout(drain_deadline.saturating_duration_since(now)) {
                            Ok(unit) => incoming.push(unit),
                            Err(_) => break,
                        }
                    }
                }
            }
            let drained_units = incoming.len() as u64;
            metrics.flush_writer_cycles.fetch_add(1, Ordering::Relaxed);
            if incoming.len() == writer_batch_limit {
                metrics
                    .flush_writer_cycles_full
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                metrics
                    .flush_writer_cycles_partial
                    .fetch_add(1, Ordering::Relaxed);
            }
            if read_active {
                metrics
                    .flush_writer_read_active_cycles
                    .fetch_add(1, Ordering::Relaxed);
            }
            metrics
                .flush_writer_drained_units
                .fetch_add(drained_units, Ordering::Relaxed);
            crate::metrics::record_counter_max(
                &metrics.flush_writer_drained_units_max,
                drained_units,
            );

            // Run through packer, collect Passthrough AND SealedSlot
            // batches. Each SealedSlot's metadata commit was previously
            // issued inline (one metadb tx per slot — soak shows ~1 ms
            // fixed cost in WAL fsync + apply-lane scheduling). The
            // packed-slot batch (`packed_batch`) folds N sealed slots
            // into one combined commit via `write_packed_slots_batch`.
            // `packed_buffered_per_slot` captures the buffered_seqs /
            // completions that belong to each sealed slot at the moment
            // it sealed, so done_tx still fires per-slot after the batch
            // commits.
            let mut pt_batch: Vec<CompressedUnit> = Vec::new();
            let mut pt_seqs: Vec<Vec<u64>> = Vec::new();
            let mut pt_completions: Vec<Option<Arc<crate::buffer::pipeline::DedupCompletion>>> =
                Vec::new();
            let mut packed_batch: Vec<SealedSlot> = Vec::new();
            let mut packed_buffered_per_slot: Vec<(
                Vec<u64>,
                Vec<Arc<crate::buffer::pipeline::DedupCompletion>>,
            )> = Vec::new();

            if let Some(sealed) = packer.flush_open_slot_if_older_than(Self::PACKED_SLOT_MAX_AGE) {
                // Do not commit an aged open slot by itself at the top of
                // the loop. Under steady load the next write_rx drain is
                // available immediately; folding the aged slot into this
                // packed_batch preserves done_tx ordering while avoiding a
                // one-slot metadb commit.
                packed_buffered_per_slot.push((
                    std::mem::take(&mut buffered_seqs),
                    std::mem::take(&mut buffered_completions),
                ));
                packed_batch.push(sealed);
            }

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
                        // Snapshot the buffered_seqs/completions that
                        // were destined for the now-sealed open slot;
                        // the current unit will accumulate into the
                        // freshly opened next slot below.
                        packed_buffered_per_slot.push((
                            std::mem::take(&mut buffered_seqs),
                            std::mem::take(&mut buffered_completions),
                        ));
                        packed_batch.push(sealed);
                        match &completion {
                            None => buffered_seqs.extend(&seqs),
                            Some(dc) => buffered_completions.push(dc.clone()),
                        }
                    }
                    Ok(PackResult::SealedSlotAndPassthrough(sealed, unit)) => {
                        packed_buffered_per_slot.push((
                            std::mem::take(&mut buffered_seqs),
                            std::mem::take(&mut buffered_completions),
                        ));
                        packed_batch.push(sealed);
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

            // Hand the packed slots off to the per-volume commit
            // workers. IO write happens inside `write_packed_slots_batch`;
            // on IO success the slot is routed to commit worker
            // `route_volume_to_worker(primary_vol)`. IO failures are
            // queued for whole-slot retry inline (preserves the
            // historic packing optimisation across retries).
            if !packed_batch.is_empty() {
                Self::write_packed_slots_batch(
                    shard_idx,
                    packed_batch,
                    packed_buffered_per_slot,
                    pool,
                    allocator,
                    io_engine,
                    metrics,
                    in_flight_tracker,
                    done_tx,
                    &mut packed_retries,
                    commit_worker_txs,
                    commit_workers_per_volume,
                );
                tail_dirty = true;
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
                &mut packed_retries,
                in_flight_tracker,
                metrics,
                cleanup_tx,
                candidate,
                commit_worker_txs,
                commit_workers_per_volume,
            );
            tail_dirty = true;
        }

        while Self::retry_one_packed_slot(
            shard_idx,
            &mut packed_retries,
            pool,
            allocator,
            io_engine,
            done_tx,
            metrics,
            in_flight_tracker,
            commit_worker_txs,
            commit_workers_per_volume,
        ) {
            tail_dirty = true;
        }

        if let Some(sealed) = packer.flush_open_slot() {
            let slot_buffers = (
                std::mem::take(&mut buffered_seqs),
                std::mem::take(&mut buffered_completions),
            );
            Self::write_packed_slots_batch(
                shard_idx,
                vec![sealed],
                vec![slot_buffers],
                pool,
                allocator,
                io_engine,
                metrics,
                in_flight_tracker,
                done_tx,
                &mut packed_retries,
                commit_worker_txs,
                commit_workers_per_volume,
            );
            tail_dirty = true;
        }

        if tail_dirty {
            let _ = pool.advance_tail_for_shard(shard_idx);
        }
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
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        done_tx: &Sender<Vec<u64>>,
        metrics: &EngineMetrics,
        in_flight_tracker: &FlusherInFlightTracker,
        commit_worker_txs: &[Sender<CommitJob>],
        commit_workers_per_volume: usize,
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

        let slot_buffers = (
            std::mem::take(&mut retry.buffered_seqs),
            std::mem::take(&mut retry.buffered_completions),
        );
        // Dispatch via the per-volume commit worker. IO failures inside
        // `write_packed_slots_batch` are re-queued on `retries` with the
        // buffered_seqs/completions preserved, matching the old loop's
        // semantics. IO success hands off to the commit worker which
        // publishes done_tx after the metadata commit completes.
        Self::write_packed_slots_batch(
            shard_idx,
            vec![retry.sealed],
            vec![slot_buffers],
            pool,
            allocator,
            io_engine,
            metrics,
            in_flight_tracker,
            done_tx,
            retries,
            commit_worker_txs,
            commit_workers_per_volume,
        );
        true
    }

    /// Handle a compressed unit in the writer thread. Only called from
    /// the shutdown drain; sealed slots get dispatched to commit workers
    /// to keep the l2p_commit_lock acquires on a single hot path.
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
        packed_retries: &mut VecDeque<PackedSlotRetry>,
        in_flight_tracker: &FlusherInFlightTracker,
        metrics: &EngineMetrics,
        cleanup_tx: &Sender<CleanupBatch>,
        candidate: &crate::dedup::CandidateCache,
        commit_worker_txs: &[Sender<CommitJob>],
        commit_workers_per_volume: usize,
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
                    cleanup_tx, candidate,
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
                // Previously buffered seqs/completions belong to the
                // just-sealed slot; hand them to the commit worker as
                // per-slot buffers so done_tx fires after metadata commit.
                let slot_buffers = (
                    std::mem::take(buffered_seqs),
                    std::mem::take(buffered_completions),
                );
                Self::write_packed_slots_batch(
                    shard_idx,
                    vec![sealed],
                    vec![slot_buffers],
                    pool,
                    allocator,
                    io_engine,
                    metrics,
                    in_flight_tracker,
                    done_tx,
                    packed_retries,
                    commit_worker_txs,
                    commit_workers_per_volume,
                );
                // Current unit goes into the new open slot
                match &completion {
                    None => buffered_seqs.extend(&seqs),
                    Some(dc) => buffered_completions.push(dc.clone()),
                }
            }
            Ok(PackResult::SealedSlotAndPassthrough(sealed, unit)) => {
                let slot_buffers = (
                    std::mem::take(buffered_seqs),
                    std::mem::take(buffered_completions),
                );
                Self::write_packed_slots_batch(
                    shard_idx,
                    vec![sealed],
                    vec![slot_buffers],
                    pool,
                    allocator,
                    io_engine,
                    metrics,
                    in_flight_tracker,
                    done_tx,
                    packed_retries,
                    commit_worker_txs,
                    commit_workers_per_volume,
                );

                if let Err(e) = Self::write_unit(
                    shard_idx, &unit, pool, meta, lifecycle, allocator, io_engine, metrics,
                    cleanup_tx, candidate,
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
