//! Per-volume commit worker — Phase 1 of the per-volume commit
//! architecture (see `.claude/plans/per-volume-commit-worker.md`).
//!
//! Shard writer threads now do alloc + LV3 IO and hand a `CommitJob`
//! to the worker indexed by `hash(vol_id) % NUM_COMMIT_WORKERS`. Each
//! worker drains its FIFO queue and runs the metadb commit + cleanup.
//! Passthrough `mark_flushed` + `done_tx` are handed to the paired
//! post_commit worker so the commit worker can return to metadb
//! sooner. Same volume always lands on the same worker, so per-volume
//! LSN dispatch contention drops to zero.

use super::*;

/// Default sub-batch cap inside a single `CommitJob`. New per-volume routing
/// has 0 LSN contention, so each commit only pays its own WAL fsync
/// + apply lane scheduling cost. metadb-onyx-soak (2026-05-08) shows
/// the per-op amortisation sweet spot at 50–200 ops; 150 sits in the
/// middle and still leaves apply lanes headroom for cross-volume
/// concurrency. Picking too small (e.g. 10) leaves WAL fsync (~100µs)
/// un-amortised; too large (e.g. 1000+) reproduces the 22ms apply
/// lane stall observed with `buffer.shards=1`.
pub(crate) const TARGET_OPS_PER_COMMIT: usize = 150;
/// Number of dedicated commit workers. Per-volume routing uses
/// `hash(vol_id) % NUM_COMMIT_WORKERS`. The buffer keeps its own 16
/// shards for LV2/dedup/compress parallelism; this is independent.
pub(in crate::buffer::flush) const NUM_COMMIT_WORKERS: usize = 16;

/// Channel capacity per commit worker. ~64 jobs of slack absorbs
/// shard-writer cycle bursts without unbounded memory growth.
pub(in crate::buffer::flush) const COMMIT_WORKER_QUEUE_CAP: usize = 64;

/// Owned per-unit data the shard writer has staged (alloc + IO done).
pub(in crate::buffer::flush) struct UnitCommitData {
    pub shard_idx: usize,
    pub unit: CompressedUnit,
    pub pba: Pba,
    pub alloc_blocks: u32,
    pub seqs: Vec<u64>,
    pub completion: Option<Arc<crate::buffer::pipeline::DedupCompletion>>,
}

pub(in crate::buffer::flush) struct PassthroughCommitJob {
    /// Single-volume routing key. All units in `units` belong to this
    /// volume — write_units_batch splits multi-volume batches into
    /// one PassthroughCommitJob per volume before pushing.
    pub vol_id: VolumeId,
    pub units: Vec<UnitCommitData>,
    pub enqueued_at: Instant,
}

pub(in crate::buffer::flush) struct PackedCommitJob {
    pub sealed: SealedSlot,
    pub shard_idx: usize,
    /// `done_tx` payload for fragments that the open-slot saw but
    /// hasn't acked yet. Forwarded to `done_tx` after metadata commit
    /// publishes the slot.
    pub buffered_seqs: Vec<u64>,
    pub buffered_completions: Vec<Arc<crate::buffer::pipeline::DedupCompletion>>,
    pub enqueued_at: Instant,
}

pub(in crate::buffer::flush) enum CommitJob {
    Passthrough(PassthroughCommitJob),
    Packed(PackedCommitJob),
}

/// Per-unit decisions made under the L2P commit lock; consumed by
/// chunked commit below. Module-scope so `commit_passthrough_chunk`
/// can take a `&[Option<UnitMeta>]`.
struct UnitMeta {
    batch_values: Vec<(Lba, BlockmapValue)>,
    seqs: Vec<u64>,
    live_positions: Vec<usize>,
    fresh_dedup_pairs: Vec<(ContentHash, BlockmapValue)>,
    stale_repairs: Vec<(ContentHash, DedupEntry, DedupEntry)>,
}

struct PassthroughChunkOutcome {
    old_pba_meta: HashMap<Pba, RemapCleanup>,
}

/// Stable hash + shard sub-route → worker index. With `per_vol = 1`
/// (default), a single volume always routes to a single worker and
/// LSN dispatch contention is zero. With `per_vol = N` (2 or 4 in
/// current experiments), the volume fans out to N consecutive workers
/// — `[base, base+1, ..., base+N-1] mod NUM_COMMIT_WORKERS` — and the
/// shard writer's `shard_idx` selects within that slot via
/// `shard_idx % per_vol`. This trades a small amount of apply-lane
/// contention (multiple workers committing for the same volume) for
/// up to N× per-volume commit throughput.
pub(in crate::buffer::flush) fn route_volume_to_worker(
    vol_id: &str,
    shard_idx: usize,
    per_vol: usize,
) -> usize {
    if NUM_COMMIT_WORKERS <= 1 {
        return 0;
    }
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    vol_id.as_bytes().hash(&mut hasher);
    let base = (hasher.finish() as usize) % NUM_COMMIT_WORKERS;
    let per_vol = per_vol.max(1).min(NUM_COMMIT_WORKERS);
    if per_vol == 1 {
        return base;
    }
    let offset = shard_idx % per_vol;
    (base + offset) % NUM_COMMIT_WORKERS
}

impl BufferFlusher {
    pub(in crate::buffer::flush) fn commit_worker_loop(
        worker_idx: usize,
        rx: &Receiver<CommitJob>,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        in_flight_tracker: &FlusherInFlightTracker,
        metrics: &EngineMetrics,
        lane_cleanup_txs: &[Sender<CleanupBatch>],
        candidate: &crate::dedup::CandidateCache,
        lane_done_txs: &[Sender<Vec<u64>>],
        post_commit_tx: &Sender<PostCommitJob>,
        target_lbas_per_tx: usize,
        coalesce_lba_budget: usize,
        coalesce_timeout: Duration,
        packed_try_drain_lba_budget: usize,
        running: &AtomicBool,
    ) {
        let _ = worker_idx;
        // Main loop. Poll with timeout so shutdown signal is observed
        // even when the queue is idle. The shard writers are joined
        // before us (BufferFlusher::join_lanes), then their commit_tx
        // clones drop, the rx side disconnects, and we drain remaining
        // jobs before exiting.
        while running.load(Ordering::Relaxed) {
            let recv_start = Instant::now();
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(first) => {
                    let idle_ns =
                        recv_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                    metrics
                        .flush_commit_worker_rx_idle_ns
                        .fetch_add(idle_ns, Ordering::Relaxed);
                    metrics
                        .flush_commit_worker_rx_idle_iters
                        .fetch_add(1, Ordering::Relaxed);
                    Self::dispatch_commit_batch(
                        Self::drain_commit_batch(
                            first,
                            rx,
                            coalesce_lba_budget,
                            coalesce_timeout,
                            packed_try_drain_lba_budget,
                        ),
                        pool,
                        meta,
                        lifecycle,
                        allocator,
                        in_flight_tracker,
                        metrics,
                        lane_cleanup_txs,
                        candidate,
                        lane_done_txs,
                        post_commit_tx,
                        target_lbas_per_tx,
                    );
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    let idle_ns =
                        recv_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                    metrics
                        .flush_commit_worker_rx_idle_ns
                        .fetch_add(idle_ns, Ordering::Relaxed);
                    metrics
                        .flush_commit_worker_rx_idle_iters
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }
        }
        // Drain anything queued at shutdown. The shard writers may
        // have pushed final jobs after we observed `running == false`
        // but before they joined.
        while let Ok(first) = rx.try_recv() {
            Self::dispatch_commit_batch(
                Self::drain_commit_batch(
                    first,
                    rx,
                    coalesce_lba_budget,
                    Duration::ZERO,
                    packed_try_drain_lba_budget,
                ),
                pool,
                meta,
                lifecycle,
                allocator,
                in_flight_tracker,
                metrics,
                lane_cleanup_txs,
                candidate,
                lane_done_txs,
                post_commit_tx,
                target_lbas_per_tx,
            );
        }
    }

    fn drain_commit_batch(
        first: CommitJob,
        rx: &Receiver<CommitJob>,
        coalesce_lba_budget: usize,
        coalesce_timeout: Duration,
        packed_try_drain_lba_budget: usize,
    ) -> Vec<CommitJob> {
        if coalesce_lba_budget == 0 {
            let mut batch = vec![first];
            if packed_try_drain_lba_budget > 0 && matches!(batch[0], CommitJob::Packed(_)) {
                let mut total_lbas = lbas_in_job(&batch[0]);
                while total_lbas < packed_try_drain_lba_budget {
                    match rx.try_recv() {
                        Ok(job) => {
                            let is_packed = matches!(job, CommitJob::Packed(_));
                            total_lbas = total_lbas.saturating_add(lbas_in_job(&job));
                            batch.push(job);
                            if !is_packed {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
            return batch;
        }
        let mut batch = vec![first];
        let mut total_lbas = lbas_in_job(&batch[0]);
        let deadline = Instant::now() + coalesce_timeout;
        while total_lbas < coalesce_lba_budget {
            match rx.try_recv() {
                Ok(job) => {
                    total_lbas = total_lbas.saturating_add(lbas_in_job(&job));
                    batch.push(job);
                }
                Err(crossbeam_channel::TryRecvError::Empty)
                    if coalesce_timeout > Duration::ZERO =>
                {
                    let now = Instant::now();
                    if now >= deadline {
                        break;
                    }
                    match rx.recv_timeout(deadline.saturating_duration_since(now)) {
                        Ok(job) => {
                            total_lbas = total_lbas.saturating_add(lbas_in_job(&job));
                            batch.push(job);
                        }
                        Err(_) => break,
                    }
                }
                Err(_) => break,
            }
        }
        batch
    }

    fn dispatch_commit_batch(
        jobs: Vec<CommitJob>,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        in_flight_tracker: &FlusherInFlightTracker,
        metrics: &EngineMetrics,
        lane_cleanup_txs: &[Sender<CleanupBatch>],
        candidate: &crate::dedup::CandidateCache,
        lane_done_txs: &[Sender<Vec<u64>>],
        post_commit_tx: &Sender<PostCommitJob>,
        target_lbas_per_tx: usize,
    ) {
        Self::record_commit_worker_drain(metrics, &jobs);
        let mut pending_pt: Option<PassthroughCommitJob> = None;
        let mut pending_packed: Vec<PackedCommitJob> = Vec::new();
        for job in jobs {
            match job {
                CommitJob::Passthrough(mut pj) => {
                    match pending_pt.as_mut() {
                        Some(existing) if existing.vol_id == pj.vol_id => {
                            existing.units.append(&mut pj.units);
                        }
                        Some(_) => {
                            let ready = pending_pt.take().expect("pending passthrough exists");
                            Self::dispatch_passthrough_job(
                                ready,
                                pool,
                                meta,
                                lifecycle,
                                allocator,
                                in_flight_tracker,
                                metrics,
                                lane_cleanup_txs,
                                candidate,
                                lane_done_txs,
                                post_commit_tx,
                                target_lbas_per_tx,
                            );
                            pending_pt = Some(pj);
                        }
                        None => pending_pt = Some(pj),
                    }
                }
                CommitJob::Packed(pj) => {
                    pending_packed.push(pj);
                }
            }
        }

        // Keep foreground-shaped passthrough completions ahead of packed metadata
        // work within a worker drain. NVMe mixed runs showed that globally
        // reordering packed before passthrough improves packed lbas/commit but
        // explodes commit-worker queue wait. Folding packed only at the drain
        // tail keeps PT moving while still collapsing many one-slot packed txs.
        if let Some(pj) = pending_pt {
            Self::dispatch_passthrough_job(
                pj,
                pool,
                meta,
                lifecycle,
                allocator,
                in_flight_tracker,
                metrics,
                lane_cleanup_txs,
                candidate,
                lane_done_txs,
                post_commit_tx,
                target_lbas_per_tx,
            );
        }

        if !pending_packed.is_empty() {
            Self::dispatch_packed_jobs(
                pending_packed,
                pool,
                meta,
                lifecycle,
                allocator,
                in_flight_tracker,
                metrics,
                lane_cleanup_txs,
                candidate,
                lane_done_txs,
                post_commit_tx,
            );
        }
    }

    fn record_commit_worker_drain(metrics: &EngineMetrics, jobs: &[CommitJob]) {
        let now = Instant::now();
        let drain_jobs = jobs.len() as u64;
        let drain_lbas = jobs.iter().map(lbas_in_job).sum::<usize>() as u64;
        let queue_wait_ns = jobs
            .iter()
            .map(|job| {
                now.saturating_duration_since(enqueued_at_for_job(job))
                    .as_nanos()
                    .min(u64::MAX as u128) as u64
            })
            .sum::<u64>();
        metrics
            .flush_commit_worker_drain_batches
            .fetch_add(1, Ordering::Relaxed);
        metrics
            .flush_commit_worker_drain_jobs
            .fetch_add(drain_jobs, Ordering::Relaxed);
        metrics
            .flush_commit_worker_drain_lbas
            .fetch_add(drain_lbas, Ordering::Relaxed);
        metrics
            .flush_commit_worker_queue_wait_ns
            .fetch_add(queue_wait_ns, Ordering::Relaxed);
        metrics
            .flush_commit_worker_jobs
            .fetch_add(drain_jobs, Ordering::Relaxed);
        metrics
            .flush_commit_worker_job_lbas
            .fetch_add(drain_lbas, Ordering::Relaxed);
        crate::metrics::record_counter_max(
            &metrics.flush_commit_worker_drain_jobs_max,
            drain_jobs,
        );
        crate::metrics::record_counter_max(
            &metrics.flush_commit_worker_drain_lbas_max,
            drain_lbas,
        );
    }

    fn dispatch_passthrough_job(
        pj: PassthroughCommitJob,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        in_flight_tracker: &FlusherInFlightTracker,
        metrics: &EngineMetrics,
        lane_cleanup_txs: &[Sender<CleanupBatch>],
        candidate: &crate::dedup::CandidateCache,
        lane_done_txs: &[Sender<Vec<u64>>],
        post_commit_tx: &Sender<PostCommitJob>,
        target_lbas_per_tx: usize,
    ) {
        let primary_shard = pj.units.first().map(|u| u.shard_idx).unwrap_or(0);
        let service_start = Instant::now();
        let cleanup_tx = lane_cleanup_txs
            .get(primary_shard)
            .or_else(|| lane_cleanup_txs.first());
        let (Some(cleanup_tx), true) = (cleanup_tx, !lane_done_txs.is_empty()) else {
            tracing::error!(
                shard_idx = primary_shard,
                "commit_worker: lane channels missing — dropping passthrough job"
            );
            return;
        };
        Self::commit_passthrough_job(
            pj,
            pool,
            meta,
            lifecycle,
            allocator,
            in_flight_tracker,
            metrics,
            cleanup_tx,
            candidate,
            lane_done_txs,
            post_commit_tx,
            target_lbas_per_tx,
        );
        Self::record_elapsed(&metrics.flush_commit_worker_service_ns, service_start);
    }

    fn dispatch_packed_jobs(
        jobs: Vec<PackedCommitJob>,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        in_flight_tracker: &FlusherInFlightTracker,
        metrics: &EngineMetrics,
        lane_cleanup_txs: &[Sender<CleanupBatch>],
        candidate: &crate::dedup::CandidateCache,
        lane_done_txs: &[Sender<Vec<u64>>],
        post_commit_tx: &Sender<PostCommitJob>,
    ) {
        let Some(first) = jobs.first() else {
            return;
        };
        let service_start = Instant::now();
        let cleanup_tx = lane_cleanup_txs
            .get(first.shard_idx)
            .or_else(|| lane_cleanup_txs.first());
        let Some(cleanup_tx) = cleanup_tx else {
            tracing::error!(
                shard_idx = first.shard_idx,
                "commit_worker: lane channels missing — dropping packed job"
            );
            return;
        };
        Self::commit_packed_jobs_batch(
            jobs,
            pool,
            meta,
            lifecycle,
            allocator,
            in_flight_tracker,
            metrics,
            cleanup_tx,
            candidate,
            lane_done_txs,
            post_commit_tx,
        );
        Self::record_elapsed(&metrics.flush_commit_worker_service_ns, service_start);
    }

    /// Commit a single-volume passthrough batch. Acquires the volume's
    /// lifecycle read lock + per-unit L2P commit locks for the whole
    /// job, then flushes commits in sub-batches of ≤ target_lbas_per_tx
    /// LBA ops. Same lock, multiple commits inside — no contention
    /// because this worker is the only committer for this volume.
    pub(in crate::buffer::flush) fn commit_passthrough_job(
        job: PassthroughCommitJob,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        in_flight_tracker: &FlusherInFlightTracker,
        metrics: &EngineMetrics,
        cleanup_tx: &Sender<CleanupBatch>,
        candidate: &crate::dedup::CandidateCache,
        lane_done_txs: &[Sender<Vec<u64>>],
        post_commit_tx: &Sender<PostCommitJob>,
        target_lbas_per_tx: usize,
    ) {
        let total_start = Instant::now();
        let PassthroughCommitJob { vol_id, units, .. } = job;

        // Acquire lifecycle read lock for this volume only — all units
        // in the job target the same volume by construction. Held
        // across the whole job so delete/create cannot interleave.
        let lifecycle_lock = lifecycle.get_lock(&vol_id.0);
        let _guard = lifecycle_lock.read().unwrap();

        // Volume-generation discard set (per-unit). A unit may have
        // been built before a delete + recreate; commit_worker is the
        // single point where we serialise the gen check under the
        // lifecycle lock.
        let cur_vol = match meta.get_volume(&vol_id) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(
                    vol = %vol_id.0,
                    error = %e,
                    "commit_worker: get_volume failed; rolling back batch"
                );
                Self::passthrough_rollback_job(
                    units,
                    allocator,
                    in_flight_tracker,
                    lane_done_txs,
                    metrics,
                );
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                return;
            }
        };
        let cur_created_at = cur_vol.as_ref().map(|v| v.created_at);
        let volume_present = cur_vol.is_some();

        // Commit lock ranges: every unit in the job. Sorted/deduped
        // inside with_l2p_commit_locks_for_ranges.
        let commit_ranges: Vec<(&str, Lba, u64)> = units
            .iter()
            .map(|u| (vol_id.0.as_str(), u.unit.start_lba, u.unit.lba_count as u64))
            .collect();

        let n = units.len();
        let mut unit_metas: Vec<Option<UnitMeta>> = (0..n).map(|_| None).collect();
        let mut discarded: Vec<bool> = vec![false; n];
        let mut commit_failed_indices: Vec<usize> = Vec::new();
        // Units that committed without an Err but had every L2pRemap
        // rejected by metadb's seq_guard. Their PBA was incref'd zero
        // times in the tx, so we must free it back to the allocator
        // in the post-commit pass — same shape as `discarded` units.
        let mut seq_rejected_indices: Vec<usize> = Vec::new();
        // Accumulators owned outside the L2P stripe lock — populated by
        // each `commit_passthrough_chunk` so candidate cache inserts and
        // stale dedup repairs ride into the post_commit thread instead
        // of pinning the lock for tens of milliseconds.
        let mut accum_candidate_pairs: Vec<(ContentHash, BlockmapValue)> = Vec::new();
        let mut accum_stale_repairs: Vec<(ContentHash, DedupEntry, DedupEntry)> = Vec::new();
        let actual_old_pba_meta = pool.with_l2p_commit_locks_for_ranges(commit_ranges, || {
            let build_start = Instant::now();
            for (i, ucd) in units.iter().enumerate() {
                let unit = &ucd.unit;
                let should_discard = !volume_present
                    || (unit.vol_created_at != 0
                        && cur_created_at != Some(unit.vol_created_at));
                if should_discard {
                    metrics.flush_stale_discards.fetch_add(1, Ordering::Relaxed);
                    discarded[i] = true;
                    continue;
                }
                let live_positions = match Self::live_positions_for_unit(unit, pool) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::error!(
                            vol = %vol_id.0,
                            start_lba = unit.start_lba.0,
                            error = %e,
                            "commit_worker: live_positions_for_unit failed; treating as commit failure"
                        );
                        commit_failed_indices.push(i);
                        continue;
                    }
                };
                if live_positions.is_empty() {
                    unit_metas[i] = Some(UnitMeta {
                        batch_values: Vec::new(),
                        seqs: Vec::new(),
                        live_positions,
                        fresh_dedup_pairs: Vec::new(),
                        stale_repairs: Vec::new(),
                    });
                    continue;
                }
                let lbas: Vec<Lba> = live_positions
                    .iter()
                    .map(|idx| Lba(unit.start_lba.0 + *idx as u64))
                    .collect();
                let mut batch_values: Vec<(Lba, BlockmapValue)> =
                    Vec::with_capacity(live_positions.len());
                let mut seqs: Vec<u64> = Vec::with_capacity(live_positions.len());
                let mut fresh_dedup_pairs: Vec<(ContentHash, BlockmapValue)> = Vec::new();
                let mut stale_repairs: Vec<(ContentHash, DedupEntry, DedupEntry)> = Vec::new();
                let flags = if unit.dedup_skipped {
                    FLAG_DEDUP_SKIPPED
                } else {
                    0
                };
                for j in 0..live_positions.len() {
                    let blockmap = Self::blockmap_for_unit_position(
                        unit,
                        ucd.pba,
                        live_positions[j],
                        0,
                        flags,
                    );
                    batch_values.push((lbas[j], blockmap));
                    seqs.push(Self::latest_seq_for_lba(&unit.seq_lba_ranges, lbas[j]));
                }
                if !unit.dedup_skipped {
                    if let Some(ref hashes) = unit.block_hashes {
                        fresh_dedup_pairs.reserve(live_positions.len());
                        for &pos in &live_positions {
                            let hash = hashes[pos];
                            if hash == [0u8; 8] {
                                continue;
                            }
                            let blockmap =
                                Self::blockmap_for_unit_position(unit, ucd.pba, pos, 0, 0);
                            fresh_dedup_pairs.push((hash, blockmap));
                            if let Some(repairs) = &unit.dedup_stale_repairs {
                                if let Some(Some(old_entry)) = repairs.get(pos) {
                                    stale_repairs.push((
                                        hash,
                                        *old_entry,
                                        blockmap.to_dedup_entry(),
                                    ));
                                }
                            }
                        }
                    }
                }
                unit_metas[i] = Some(UnitMeta {
                    batch_values,
                    seqs,
                    live_positions,
                    fresh_dedup_pairs,
                    stale_repairs,
                });
            }
            Self::record_elapsed(&metrics.flush_writer_meta_build_ns, build_start);

            // Sub-batch the commits to ≤ TARGET_OPS_PER_COMMIT total
            // LBAs. With 0 LSN contention each commit only pays its
            // own per-tx fixed cost; bigger sub-batches still amortise
            // WAL fsync.
            let mut accum_old_meta: HashMap<Pba, RemapCleanup> = HashMap::new();
            let mut chunk: Vec<usize> = Vec::new();
            let mut chunk_lbas: usize = 0;
            for i in 0..n {
                let Some(um) = unit_metas[i].as_ref() else {
                    continue;
                };
                let lbas = um.batch_values.len();
                if !chunk.is_empty()
                    && chunk_lbas.saturating_add(lbas) > target_lbas_per_tx
                {
                    Self::commit_passthrough_chunk(
                        &chunk,
                        &vol_id,
                        &unit_metas,
                        meta,
                        metrics,
                        &mut accum_old_meta,
                        &mut commit_failed_indices,
                        &mut seq_rejected_indices,
                        &mut accum_candidate_pairs,
                        &mut accum_stale_repairs,
                    );
                    chunk.clear();
                    chunk_lbas = 0;
                }
                chunk.push(i);
                chunk_lbas += lbas;
                if lbas > target_lbas_per_tx {
                    Self::commit_passthrough_chunk(
                        &chunk,
                        &vol_id,
                        &unit_metas,
                        meta,
                        metrics,
                        &mut accum_old_meta,
                        &mut commit_failed_indices,
                        &mut seq_rejected_indices,
                        &mut accum_candidate_pairs,
                        &mut accum_stale_repairs,
                    );
                    chunk.clear();
                    chunk_lbas = 0;
                }
            }
            if !chunk.is_empty() {
                Self::commit_passthrough_chunk(
                    &chunk,
                    &vol_id,
                    &unit_metas,
                    meta,
                    metrics,
                    &mut accum_old_meta,
                    &mut commit_failed_indices,
                    &mut seq_rejected_indices,
                    &mut accum_candidate_pairs,
                    &mut accum_stale_repairs,
                );
            }

            PassthroughChunkOutcome {
                old_pba_meta: accum_old_meta,
            }
        });

        // Post-commit work outside the L2P commit lock. Keep
        // mark_flushed ordered before done_tx so the coalescer cannot
        // re-enqueue the same pending seq while its buffer index entry
        // is still visible.
        let cleanup_start = Instant::now();
        let commit_failed_set: std::collections::HashSet<usize> =
            commit_failed_indices.iter().copied().collect();
        let seq_rejected_set: std::collections::HashSet<usize> =
            seq_rejected_indices.iter().copied().collect();
        let mut post_mark_ranges_by_shard: HashMap<usize, Vec<(u64, Lba, u32)>> = HashMap::new();

        for (i, ucd) in units.iter().enumerate() {
            if discarded[i] {
                if ucd.alloc_blocks == 1 {
                    let _ = allocator.free_one(ucd.pba);
                } else {
                    let _ = allocator.free_extent(Extent::new(ucd.pba, ucd.alloc_blocks));
                }
                post_mark_ranges_by_shard
                    .entry(ucd.shard_idx)
                    .or_default()
                    .extend(ucd.unit.seq_lba_ranges.iter().cloned());
                continue;
            }
            if commit_failed_set.contains(&i) {
                if ucd.alloc_blocks == 1 {
                    let _ = allocator.free_one(ucd.pba);
                } else {
                    let _ = allocator.free_extent(Extent::new(ucd.pba, ucd.alloc_blocks));
                }
                continue;
            }
            if seq_rejected_set.contains(&i) {
                // Every L2pRemap for this unit was rejected by seq_guard;
                // the freshly-allocated PBA is unreferenced. Mark the
                // buffer entry as flushed (a newer commit already owns
                // the LBAs) and return the PBA to the allocator.
                if ucd.alloc_blocks == 1 {
                    let _ = allocator.free_one(ucd.pba);
                } else {
                    let _ = allocator.free_extent(Extent::new(ucd.pba, ucd.alloc_blocks));
                }
                post_mark_ranges_by_shard
                    .entry(ucd.shard_idx)
                    .or_default()
                    .extend(ucd.unit.seq_lba_ranges.iter().cloned());
                continue;
            }
            let Some(um) = unit_metas[i].as_ref() else {
                continue;
            };
            if um.live_positions.is_empty() {
                if ucd.alloc_blocks == 1 {
                    let _ = allocator.free_one(ucd.pba);
                } else {
                    let _ = allocator.free_extent(Extent::new(ucd.pba, ucd.alloc_blocks));
                }
                post_mark_ranges_by_shard
                    .entry(ucd.shard_idx)
                    .or_default()
                    .extend(ucd.unit.seq_lba_ranges.iter().cloned());
                continue;
            }
            // Successful commit path.
            Self::free_unreferenced_raw_blocks(
                &ucd.unit,
                ucd.pba,
                &um.live_positions,
                allocator,
                "commit_worker_passthrough",
            );
            metrics.flush_units_written.fetch_add(1, Ordering::Relaxed);
            metrics
                .flush_unit_bytes
                .fetch_add(ucd.unit.compressed_data.len() as u64, Ordering::Relaxed);
            post_mark_ranges_by_shard
                .entry(ucd.shard_idx)
                .or_default()
                .extend(ucd.unit.seq_lba_ranges.iter().cloned());
        }

        if !actual_old_pba_meta.old_pba_meta.is_empty() {
            let _ = cleanup_tx.send(actual_old_pba_meta.old_pba_meta.into_values().collect());
        }
        Self::record_elapsed(&metrics.flush_writer_cleanup_ns, cleanup_start);

        // done_tx: success path → send only after post_commit has
        // run mark_flushed. Failure path → defer_retry, then send
        // immediately because there is no successful mark to wait for.
        let mut done_by_shard: HashMap<usize, Vec<u64>> = HashMap::new();
        let mut failed_done_by_shard: HashMap<usize, Vec<u64>> = HashMap::new();
        for (i, ucd) in units.into_iter().enumerate() {
            let UnitCommitData {
                shard_idx,
                seqs,
                completion,
                ..
            } = ucd;
            if commit_failed_set.contains(&i) {
                match &completion {
                    None => in_flight_tracker.defer_retry(&seqs, Self::RETRY_BACKOFF),
                    Some(dc) => in_flight_tracker.defer_retry(dc.seqs(), Self::RETRY_BACKOFF),
                }
                metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
            }
            match completion {
                None => {
                    let target = if commit_failed_set.contains(&i) {
                        &mut failed_done_by_shard
                    } else {
                        &mut done_by_shard
                    };
                    target.entry(shard_idx).or_default().extend(seqs);
                }
                Some(dc) => {
                    if let Some(original_seqs) = dc.decrement() {
                        let target = if commit_failed_set.contains(&i) {
                            &mut failed_done_by_shard
                        } else {
                            &mut done_by_shard
                        };
                        target.entry(shard_idx).or_default().extend(original_seqs);
                    }
                }
            }
        }

        // Publish candidate cache inserts + stale dedup repairs OUTSIDE
        // the L2P stripe lock but synchronously with the commit worker.
        // The test `commit_worker_publishes_candidates_before_post_commit`
        // pins candidate visibility before any post_commit drain so
        // subsequent dedup workers can find the fresh hash; we honour
        // that contract by inserting here on the commit_worker thread
        // *after* the metadb commit has landed and the lock is released.
        // Compared to the historical inline call inside
        // `commit_passthrough_chunk` (which ran under the L2P stripe
        // lock for the whole chunk), this shrinks the lock hold time by
        // the candidate insert + stale repair duration — that work is
        // not on the metadb path and never needed the lock.
        if !accum_candidate_pairs.is_empty() {
            let cand_start = Instant::now();
            candidate.insert_many(&accum_candidate_pairs);
            Self::record_elapsed(&metrics.flush_writer_meta_candidate_ns, cand_start);
        }
        if !accum_stale_repairs.is_empty() {
            let repair_start = Instant::now();
            Self::repair_stale_dedup_index(
                meta,
                metrics,
                &accum_stale_repairs,
                "commit_worker_passthrough",
            );
            Self::record_elapsed(&metrics.flush_writer_meta_repair_ns, repair_start);
        }

        let mut post_items: Vec<(usize, Vec<(u64, Lba, u32)>)> =
            post_mark_ranges_by_shard.into_iter().collect();
        post_items.sort_by_key(|(shard_idx, _)| *shard_idx);
        for (shard_idx, mark_ranges) in post_items {
            let done_seqs = done_by_shard.remove(&shard_idx).unwrap_or_default();
            let post_job = PostCommitJob {
                shard_idx,
                mark_ranges,
                candidate_pairs: Vec::new(),
                stale_repairs: Vec::new(),
                done_seqs,
            };
            if let Err(err) = post_commit_tx.send(post_job) {
                let job = err.0;
                tracing::warn!(
                    shard_idx = job.shard_idx,
                    "commit_worker: post_commit queue disconnected; falling back inline"
                );
                let mark_start = Instant::now();
                for (seq, lba_start, lba_count) in &job.mark_ranges {
                    if let Err(e) = pool.mark_flushed(*seq, *lba_start, *lba_count) {
                        tracing::warn!(
                            seq,
                            shard_idx = job.shard_idx,
                            error = %e,
                            "commit_worker: failed to mark entry flushed"
                        );
                    }
                }
                Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);
                let _ = pool.advance_tail_for_shard(job.shard_idx);
                if let Some(done_tx) = lane_done_txs
                    .get(job.shard_idx)
                    .or_else(|| lane_done_txs.first())
                {
                    let _ = done_tx.send(job.done_seqs);
                }
            }
        }
        // Any successful done seq without a mark range can be released
        // now. This is uncommon but covers empty/live-filtered units.
        for (shard_idx, seqs) in done_by_shard {
            if let Some(done_tx) = lane_done_txs
                .get(shard_idx)
                .or_else(|| lane_done_txs.first())
            {
                let _ = done_tx.send(seqs);
            }
        }
        for (shard_idx, seqs) in failed_done_by_shard {
            if let Some(done_tx) = lane_done_txs
                .get(shard_idx)
                .or_else(|| lane_done_txs.first())
            {
                let _ = done_tx.send(seqs);
            }
        }

        Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
    }

    /// Commit one sub-batch (chunk of unit indices). Records freed
    /// PBA metadata in `accum_old_meta`. On commit error appends each
    /// non-empty unit's index to `commit_failed_indices` so the
    /// post-commit loop frees PBAs and defer_retries seqs.
    ///
    /// Fresh candidate pairs and stale dedup repairs are accumulated
    /// into the caller-supplied vecs and dispatched on the post_commit
    /// thread, so neither stays under the L2P commit lock. The race
    /// against `cleanup_dead_pbas_batch` is benign: cleanup removes
    /// candidate entries by *old* PBA, while we insert against the new
    /// PBA returned by this commit (see post_commit.rs).
    fn commit_passthrough_chunk(
        chunk: &[usize],
        vol_id: &VolumeId,
        unit_metas: &[Option<UnitMeta>],
        meta: &MetaStore,
        metrics: &EngineMetrics,
        accum_old_meta: &mut HashMap<Pba, RemapCleanup>,
        commit_failed_indices: &mut Vec<usize>,
        seq_rejected_indices: &mut Vec<usize>,
        accum_candidate_pairs: &mut Vec<(ContentHash, BlockmapValue)>,
        accum_stale_repairs: &mut Vec<(ContentHash, DedupEntry, DedupEntry)>,
    ) {
        let mut batch_args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> =
            Vec::with_capacity(chunk.len());
        let mut all_seqs: Vec<u64> = Vec::new();
        let mut all_fresh_pairs: Vec<(ContentHash, BlockmapValue)> = Vec::new();
        let mut all_stale_repairs: Vec<(ContentHash, DedupEntry, DedupEntry)> = Vec::new();
        let mut sub_batch_lbas: u64 = 0;
        // (unit_index, len in flat accepted Vec<bool>) for each non-empty unit.
        let mut unit_spans: Vec<(usize, usize)> = Vec::with_capacity(chunk.len());

        for &i in chunk {
            if let Some(um) = unit_metas[i].as_ref() {
                if um.batch_values.is_empty() {
                    continue;
                }
                batch_args.push((
                    vol_id,
                    um.batch_values.as_slice(),
                    um.live_positions.len() as u32,
                ));
                all_seqs.extend_from_slice(&um.seqs);
                sub_batch_lbas += um.batch_values.len() as u64;
                all_fresh_pairs.extend_from_slice(&um.fresh_dedup_pairs);
                all_stale_repairs.extend_from_slice(&um.stale_repairs);
                unit_spans.push((i, um.batch_values.len()));
            }
        }

        if batch_args.is_empty() {
            return;
        }

        let commit_start = Instant::now();
        let result = meta.atomic_batch_write_multi_with_dedup(&batch_args, &[], &all_seqs);
        Self::record_elapsed(&metrics.flush_writer_meta_commit_ns, commit_start);
        metrics
            .flush_writer_meta_commits
            .fetch_add(1, Ordering::Relaxed);
        metrics
            .flush_writer_meta_lbas
            .fetch_add(sub_batch_lbas, Ordering::Relaxed);
        metrics
            .flush_writer_meta_pt_commits
            .fetch_add(1, Ordering::Relaxed);
        metrics
            .flush_writer_meta_pt_lbas
            .fetch_add(sub_batch_lbas, Ordering::Relaxed);

        match result {
            Ok((returned, accepted)) => {
                let rejects = accepted.iter().filter(|a| !**a).count() as u64;
                if rejects > 0 {
                    metrics.flush_seq_rejects.fetch_add(rejects, Ordering::Relaxed);
                    // Whole-unit rejection: every L2pRemap got refused,
                    // so refcount[pba] is still 0 and the freshly-
                    // allocated PBA is orphaned — defer the free to the
                    // post-commit pass (single-block or extent shape).
                    let mut offset = 0;
                    for (unit_idx, span) in &unit_spans {
                        let unit_accepted = &accepted[offset..offset + span];
                        if !unit_accepted.iter().any(|a| *a) {
                            seq_rejected_indices.push(*unit_idx);
                        }
                        offset += span;
                    }
                }
                for (pba, cleanup) in returned {
                    accum_old_meta
                        .entry(pba)
                        .and_modify(|entry| entry.merge(cleanup.clone()))
                        .or_insert(cleanup);
                }
                // Defer candidate cache insertion + stale dedup repair to
                // the post_commit thread. They no longer block PT commits
                // queued behind us on the L2P stripe lock — the L2P remap
                // and refcount updates already landed in the metadb tx
                // above, so future dedup-hit lookups are immediately
                // protected by the persistent index; candidate is a RAM
                // optimization only.
                if !all_fresh_pairs.is_empty() {
                    accum_candidate_pairs.extend(all_fresh_pairs);
                }
                if !all_stale_repairs.is_empty() {
                    accum_stale_repairs.extend(all_stale_repairs);
                }
            }
            Err(e) => {
                tracing::error!(
                    vol = %vol_id.0,
                    chunk_units = chunk.len(),
                    chunk_lbas = sub_batch_lbas,
                    error = %e,
                    "commit_worker: passthrough sub-batch commit failed"
                );
                for &i in chunk {
                    if let Some(um) = unit_metas[i].as_ref() {
                        if !um.batch_values.is_empty() {
                            commit_failed_indices.push(i);
                        }
                    }
                }
            }
        }
    }

    /// Roll back every unit in a passthrough job (failure outside the
    /// commit path — e.g. `meta.get_volume` failed). Frees PBAs and
    /// defer_retries seqs.
    fn passthrough_rollback_job(
        units: Vec<UnitCommitData>,
        allocator: &SpaceAllocator,
        in_flight_tracker: &FlusherInFlightTracker,
        lane_done_txs: &[Sender<Vec<u64>>],
        metrics: &EngineMetrics,
    ) {
        for ucd in units {
            if ucd.alloc_blocks == 1 {
                let _ = allocator.free_one(ucd.pba);
            } else {
                let _ = allocator.free_extent(Extent::new(ucd.pba, ucd.alloc_blocks));
            }
            metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
            match &ucd.completion {
                None => in_flight_tracker.defer_retry(&ucd.seqs, Self::RETRY_BACKOFF),
                Some(dc) => in_flight_tracker.defer_retry(dc.seqs(), Self::RETRY_BACKOFF),
            }
            match ucd.completion {
                None => {
                    if let Some(done_tx) = lane_done_txs
                        .get(ucd.shard_idx)
                        .or_else(|| lane_done_txs.first())
                    {
                        let _ = done_tx.send(ucd.seqs);
                    }
                }
                Some(dc) => {
                    if let Some(original_seqs) = dc.decrement() {
                        if let Some(done_tx) = lane_done_txs
                            .get(ucd.shard_idx)
                            .or_else(|| lane_done_txs.first())
                        {
                            let _ = done_tx.send(original_seqs);
                        }
                    }
                }
            }
        }
    }

    /// Commit a packed slot (single PBA, multiple fragments, possibly
    /// multi-volume). Routed by primary volume — this worker is the
    /// only committer for that volume's stream, so even multi-volume
    /// slots go through one worker FIFO.
    pub(in crate::buffer::flush) fn commit_packed_job(
        job: PackedCommitJob,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        in_flight_tracker: &FlusherInFlightTracker,
        metrics: &EngineMetrics,
        cleanup_tx: &Sender<CleanupBatch>,
        candidate: &crate::dedup::CandidateCache,
        done_tx: &Sender<Vec<u64>>,
        post_commit_tx: &Sender<PostCommitJob>,
    ) {
        let lane_done_txs = std::slice::from_ref(done_tx);
        Self::commit_packed_jobs_batch(
            vec![job],
            pool,
            meta,
            lifecycle,
            allocator,
            in_flight_tracker,
            metrics,
            cleanup_tx,
            candidate,
            lane_done_txs,
            post_commit_tx,
        );
    }

    pub(in crate::buffer::flush) fn commit_packed_jobs_batch(
        jobs: Vec<PackedCommitJob>,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        in_flight_tracker: &FlusherInFlightTracker,
        metrics: &EngineMetrics,
        cleanup_tx: &Sender<CleanupBatch>,
        candidate: &crate::dedup::CandidateCache,
        lane_done_txs: &[Sender<Vec<u64>>],
        post_commit_tx: &Sender<PostCommitJob>,
    ) {
        if jobs.is_empty() {
            return;
        }
        let total_start = Instant::now();

        // Lifecycle locks: union of every fragment's vol_id, sorted to
        // avoid deadlock with concurrent batches on overlapping volume sets.
        let mut vol_ids: Vec<String> = jobs
            .iter()
            .flat_map(|job| job.sealed.fragments.iter())
            .map(|f| f.unit.vol_id.clone())
            .collect();
        vol_ids.sort();
        vol_ids.dedup();
        let locks: Vec<_> = vol_ids.iter().map(|vid| lifecycle.get_lock(vid)).collect();
        let _guards: Vec<_> = locks.iter().map(|l| l.read().unwrap()).collect();

        let commit_ranges: Vec<(&str, Lba, u64)> = jobs
            .iter()
            .flat_map(|job| {
                job.sealed.fragments.iter().map(|frag| {
                    (
                        frag.unit.vol_id.as_str(),
                        frag.unit.start_lba,
                        frag.unit.lba_count as u64,
                    )
                })
            })
            .collect();

        let outcome: OnyxResult<PackedCommitOutcome> =
            pool.with_l2p_commit_locks_for_ranges(commit_ranges, || {
                let build_start = Instant::now();
                let mut batch_values: Vec<(VolumeId, Lba, BlockmapValue)> = Vec::new();
                let mut batch_seqs: Vec<u64> = Vec::new();
                let mut fresh_dedup_pairs: Vec<(ContentHash, BlockmapValue)> = Vec::new();
                let mut stale_repairs: Vec<(ContentHash, DedupEntry, DedupEntry)> = Vec::new();
                let mut all_seq_lba_ranges: Vec<(u64, Lba, u32)> = Vec::new();
                let mut live_pbas: Vec<Pba> = Vec::new();
                // Per-job span in batch_values, so we can detect a slot
                // whose every L2pRemap was rejected by metadb's seq_guard
                // and free its now-unreferenced PBA.
                let mut job_spans: Vec<(Pba, usize)> = Vec::new();
                let mut discarded_pbas: Vec<Pba> = Vec::new();
                let mut any_discarded = false;

                for job in &jobs {
                    let mut slot_has_live = false;
                    let job_start = batch_values.len();
                    for frag in &job.sealed.fragments {
                        let unit = &frag.unit;
                        let vol_id = VolumeId(unit.vol_id.clone());
                        let should_discard = match meta.get_volume(&vol_id)? {
                            None => true,
                            Some(vc)
                                if unit.vol_created_at != 0
                                    && vc.created_at != unit.vol_created_at =>
                            {
                                true
                            }
                            _ => false,
                        };
                        if should_discard {
                            metrics.flush_stale_discards.fetch_add(1, Ordering::Relaxed);
                            any_discarded = true;
                            all_seq_lba_ranges.extend(unit.seq_lba_ranges.iter().cloned());
                            continue;
                        }
                        let live_positions = Self::live_positions_for_unit(unit, pool)?;
                        if live_positions.is_empty() {
                            any_discarded = true;
                            all_seq_lba_ranges.extend(unit.seq_lba_ranges.iter().cloned());
                            continue;
                        }
                        let frag_lbas: Vec<Lba> = live_positions
                            .iter()
                            .map(|idx| Lba(unit.start_lba.0 + *idx as u64))
                            .collect();
                        let frag_flags = if unit.dedup_skipped {
                            FLAG_DEDUP_SKIPPED
                        } else {
                            0
                        };
                        let hashes_for_promote = if !unit.dedup_skipped {
                            unit.block_hashes.as_ref()
                        } else {
                            None
                        };
                        for (i, pos) in live_positions.iter().copied().enumerate() {
                            let blockmap = BlockmapValue {
                                pba: job.sealed.pba,
                                compression: unit.compression,
                                unit_compressed_size: unit.compressed_data.len() as u32,
                                unit_original_size: unit.original_size,
                                unit_lba_count: unit.lba_count as u16,
                                offset_in_unit: pos as u16,
                                crc32: unit.crc32,
                                slot_offset: frag.slot_offset,
                                flags: frag_flags,
                            };
                            batch_values.push((vol_id.clone(), frag_lbas[i], blockmap));
                            batch_seqs.push(Self::latest_seq_for_lba(
                                &unit.seq_lba_ranges,
                                frag_lbas[i],
                            ));
                            if let Some(hashes) = hashes_for_promote {
                                let hash = hashes[pos];
                                if hash != [0u8; 8] {
                                    fresh_dedup_pairs.push((
                                        hash,
                                        BlockmapValue {
                                            flags: 0,
                                            ..blockmap
                                        },
                                    ));
                                    if let Some(repairs) = &unit.dedup_stale_repairs {
                                        if let Some(Some(old_entry)) = repairs.get(pos) {
                                            stale_repairs.push((
                                                hash,
                                                *old_entry,
                                                BlockmapValue {
                                                    flags: 0,
                                                    ..blockmap
                                                }
                                                .to_dedup_entry(),
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                        slot_has_live = true;
                        all_seq_lba_ranges.extend(unit.seq_lba_ranges.iter().cloned());
                    }
                    if slot_has_live {
                        live_pbas.push(job.sealed.pba);
                        job_spans.push((job.sealed.pba, batch_values.len() - job_start));
                    } else {
                        discarded_pbas.push(job.sealed.pba);
                    }
                }
                Self::record_elapsed(&metrics.flush_writer_meta_build_ns, build_start);

                for pba in &discarded_pbas {
                    let _ = allocator.free_one(*pba);
                }

                if batch_values.is_empty() {
                    return Ok(PackedCommitOutcome::Discarded {
                        all_seq_lba_ranges,
                    });
                }

                let meta_start = Instant::now();
                // Packed commits carry their PBA/refcount in each BlockmapValue.
                // The legacy new_pba/new_refcount arguments are ignored on this
                // metadb path, which lets one tx cover multiple packed slots.
                let actual_old_pba_meta = match meta.atomic_batch_write_packed_with_dedup(
                    &batch_values,
                    Pba(0),
                    0,
                    &[],
                    &batch_seqs,
                ) {
                    Ok((m, accepted)) => {
                        let rejects = accepted.iter().filter(|a| !**a).count() as u64;
                        if rejects > 0 {
                            metrics
                                .flush_seq_rejects
                                .fetch_add(rejects, Ordering::Relaxed);
                            // Free slot PBAs whose every L2pRemap was
                            // rejected — refcount[slot.pba] never
                            // incremented, so the slot is unreferenced.
                            let mut offset = 0;
                            for (pba, span) in &job_spans {
                                let slot_accepted = &accepted[offset..offset + span];
                                if !slot_accepted.iter().any(|a| *a) {
                                    let _ = allocator.free_one(*pba);
                                }
                                offset += span;
                            }
                        }
                        m
                    }
                    Err(e) => {
                        for pba in &live_pbas {
                            let _ = allocator.free_one(*pba);
                        }
                        Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
                        return Err(e);
                    }
                };
                Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
                metrics
                    .flush_writer_meta_commits
                    .fetch_add(1, Ordering::Relaxed);
                metrics
                    .flush_writer_meta_lbas
                    .fetch_add(batch_values.len() as u64, Ordering::Relaxed);
                metrics
                    .flush_writer_meta_packed_commits
                    .fetch_add(1, Ordering::Relaxed);
                metrics
                    .flush_writer_meta_packed_lbas
                    .fetch_add(batch_values.len() as u64, Ordering::Relaxed);
                Ok(PackedCommitOutcome::Committed {
                    actual_old_pba_meta,
                    fresh_dedup_pairs,
                    stale_repairs,
                    all_seq_lba_ranges,
                    any_discarded,
                    slots_written: live_pbas.len() as u64,
                    fragments_written: jobs
                        .iter()
                        .filter(|job| live_pbas.contains(&job.sealed.pba))
                        .map(|job| job.sealed.fragments.len() as u64)
                        .sum(),
                    bytes_written: jobs
                        .iter()
                        .filter(|job| live_pbas.contains(&job.sealed.pba))
                        .map(|job| job.sealed.data.len() as u64)
                        .sum(),
                    total_refcount: batch_values.len() as u32,
                })
            });

        match outcome {
            Ok(PackedCommitOutcome::Discarded { all_seq_lba_ranges }) => {
                let mark_start = Instant::now();
                for (seq, lba_start, lba_count) in &all_seq_lba_ranges {
                    if let Err(e) = pool.mark_flushed(*seq, *lba_start, *lba_count) {
                        tracing::warn!(
                            seq,
                            error = %e,
                            "commit_worker: failed to mark discarded packed entry flushed"
                        );
                    }
                }
                Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);
                for shard_idx in jobs.iter().map(|job| job.shard_idx) {
                    let _ = pool.advance_tail_for_shard(shard_idx);
                }
                Self::deliver_packed_batch_done(jobs, lane_done_txs);
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            }
            Ok(PackedCommitOutcome::Committed {
                actual_old_pba_meta,
                fresh_dedup_pairs,
                stale_repairs,
                all_seq_lba_ranges,
                any_discarded,
                total_refcount,
                slots_written,
                fragments_written,
                bytes_written,
            }) => {
                if !actual_old_pba_meta.is_empty() {
                    let _ = cleanup_tx.send(actual_old_pba_meta.into_values().collect());
                }
                if !fresh_dedup_pairs.is_empty() {
                    let cand_start = Instant::now();
                    candidate.insert_many(&fresh_dedup_pairs);
                    Self::record_elapsed(&metrics.flush_writer_meta_candidate_ns, cand_start);
                }
                if !stale_repairs.is_empty() {
                    let repair_start = Instant::now();
                    Self::repair_stale_dedup_index(
                        meta,
                        metrics,
                        &stale_repairs,
                        "commit_worker_packed",
                    );
                    Self::record_elapsed(&metrics.flush_writer_meta_repair_ns, repair_start);
                }
                metrics
                    .flush_packed_slots_written
                    .fetch_add(slots_written, Ordering::Relaxed);
                metrics
                    .flush_packed_fragments_written
                    .fetch_add(fragments_written, Ordering::Relaxed);
                metrics
                    .flush_packed_bytes
                    .fetch_add(bytes_written, Ordering::Relaxed);
                let mark_start = Instant::now();
                for (seq, lba_start, lba_count) in &all_seq_lba_ranges {
                    if let Err(e) = pool.mark_flushed(*seq, *lba_start, *lba_count) {
                        tracing::warn!(
                            seq,
                            error = %e,
                            "commit_worker: failed to mark packed entry flushed"
                        );
                    }
                }
                Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);
                for shard_idx in jobs.iter().map(|job| job.shard_idx) {
                    let _ = pool.advance_tail_for_shard(shard_idx);
                }
                let _ = post_commit_tx;
                Self::deliver_packed_batch_done(jobs, lane_done_txs);
                tracing::debug!(
                    slots = slots_written,
                    fragments = fragments_written,
                    total_lbas = total_refcount,
                    discarded = any_discarded,
                    "commit_worker: flushed packed slot batch"
                );
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            }
            Err(e) => {
                metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                tracing::error!(
                    error = %e,
                    "commit_worker: packed slot batch commit failed; defer_retry buffered seqs"
                );
                for job in &jobs {
                    let _ = allocator.free_one(job.sealed.pba);
                    if !job.buffered_seqs.is_empty() {
                        in_flight_tracker.defer_retry(&job.buffered_seqs, Self::RETRY_BACKOFF);
                    }
                    for dc in &job.buffered_completions {
                        in_flight_tracker.defer_retry(dc.seqs(), Self::RETRY_BACKOFF);
                    }
                }
                Self::deliver_packed_batch_done(jobs, lane_done_txs);
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            }
        }
    }

    fn deliver_packed_batch_done(
        jobs: Vec<PackedCommitJob>,
        lane_done_txs: &[Sender<Vec<u64>>],
    ) {
        for job in jobs {
            if let Some(done_tx) = lane_done_txs
                .get(job.shard_idx)
                .or_else(|| lane_done_txs.first())
            {
                Self::deliver_packed_done(
                    job.buffered_seqs,
                    job.buffered_completions,
                    done_tx,
                );
            }
        }
    }

    fn deliver_packed_done(
        buffered_seqs: Vec<u64>,
        buffered_completions: Vec<Arc<crate::buffer::pipeline::DedupCompletion>>,
        done_tx: &Sender<Vec<u64>>,
    ) {
        if !buffered_seqs.is_empty() {
            let _ = done_tx.send(buffered_seqs);
        }
        for dc in buffered_completions {
            if let Some(original_seqs) = dc.decrement() {
                let _ = done_tx.send(original_seqs);
            }
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum PackedCommitOutcome {
    Discarded {
        all_seq_lba_ranges: Vec<(u64, Lba, u32)>,
    },
    Committed {
        actual_old_pba_meta: HashMap<Pba, RemapCleanup>,
        fresh_dedup_pairs: Vec<(ContentHash, BlockmapValue)>,
        stale_repairs: Vec<(ContentHash, DedupEntry, DedupEntry)>,
        all_seq_lba_ranges: Vec<(u64, Lba, u32)>,
        any_discarded: bool,
        total_refcount: u32,
        slots_written: u64,
        fragments_written: u64,
        bytes_written: u64,
    },
}

fn lbas_in_job(job: &CommitJob) -> usize {
    match job {
        CommitJob::Passthrough(pj) => pj.units.iter().map(|ucd| ucd.unit.lba_count as usize).sum(),
        CommitJob::Packed(pj) => pj
            .sealed
            .fragments
            .iter()
            .map(|frag| frag.unit.lba_count as usize)
            .sum(),
    }
}

fn enqueued_at_for_job(job: &CommitJob) -> Instant {
    match job {
        CommitJob::Passthrough(pj) => pj.enqueued_at,
        CommitJob::Packed(pj) => pj.enqueued_at,
    }
}
