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

mod packed;
mod passthrough;

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
        commit_worker_pipeline_depth: usize,
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
                    let idle_ns = recv_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
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
                        commit_worker_pipeline_depth,
                    );
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    let idle_ns = recv_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
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
                commit_worker_pipeline_depth,
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
        commit_worker_pipeline_depth: usize,
    ) {
        Self::record_commit_worker_drain(metrics, &jobs);
        let mut pending_pt: Option<PassthroughCommitJob> = None;
        let mut pending_packed: Vec<PackedCommitJob> = Vec::new();
        for job in jobs {
            match job {
                CommitJob::Passthrough(mut pj) => match pending_pt.as_mut() {
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
                            commit_worker_pipeline_depth,
                        );
                        pending_pt = Some(pj);
                    }
                    None => pending_pt = Some(pj),
                },
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
                commit_worker_pipeline_depth,
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
        crate::metrics::record_counter_max(&metrics.flush_commit_worker_drain_jobs_max, drain_jobs);
        crate::metrics::record_counter_max(&metrics.flush_commit_worker_drain_lbas_max, drain_lbas);
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
        commit_worker_pipeline_depth: usize,
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
            commit_worker_pipeline_depth,
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
