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
/// has 0 LSN contention, so transactions should reach the configured
/// 8K-LBA target before paying WAL + RC apply fixed costs. The global
/// aggregator bounds cross-writer accumulation and executor concurrency
/// preserves headroom for independent volumes.
pub(crate) const TARGET_OPS_PER_COMMIT: usize = 8192;
/// Number of dedicated commit workers. Per-volume routing uses
/// `hash(vol_id) % NUM_COMMIT_WORKERS`. The buffer keeps its own 16
/// shards for LV2/dedup/compress parallelism; this is independent.
pub(in crate::buffer::flush) const NUM_COMMIT_WORKERS: usize = 16;

/// Capacity of the shared commit-executor queue. All writer shards feed this
/// one bounded MPMC queue; executor threads clone its receiver and coalesce
/// across the global backlog. The old per-executor 64/1024-job queues split a
/// single volume into tiny transactions even when thousands of jobs were
/// waiting elsewhere.
pub(in crate::buffer::flush) const COMMIT_WORKER_QUEUE_CAP: usize = 8192;

/// Complete transaction batches allowed to wait behind the executors. The raw
/// job queue remains the primary backpressure boundary.
pub(in crate::buffer::flush) const COMMIT_EXECUTOR_QUEUE_CAP: usize = 64;

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

pub(in crate::buffer::flush) struct DedupHitCommitJob {
    pub vol_id: VolumeId,
    pub vol_created_at: u64,
    pub hits: Vec<(Lba, BlockmapValue, ContentHash)>,
    pub promote_entries: Vec<(ContentHash, DedupEntry)>,
    pub seqs: Vec<u64>,
    pub response_tx: Sender<DedupHitCommitResponse>,
    pub enqueued_at: Instant,
}

pub(in crate::buffer::flush) enum DedupHitCommitResponse {
    Committed {
        results: Vec<DedupHitResult>,
        newly_zeroed: HashMap<Pba, RemapCleanup>,
    },
    Failed(String),
}

pub(in crate::buffer::flush) enum CommitJob {
    Passthrough(PassthroughCommitJob),
    Packed(PackedCommitJob),
    DedupHit(DedupHitCommitJob),
}

/// Stable sender selection within the configured executor count. The senders
/// are clones of one shared MPMC queue, so this index no longer partitions the
/// backlog; retaining stable routing keeps the writer call sites and metrics
/// deterministic.
pub(in crate::buffer::flush) fn route_volume_to_worker(
    vol_id: &str,
    shard_idx: usize,
    per_vol: usize,
) -> usize {
    let worker_count = per_vol.max(1).min(NUM_COMMIT_WORKERS);
    if worker_count <= 1 {
        return 0;
    }
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    vol_id.as_bytes().hash(&mut hasher);
    let base = (hasher.finish() as usize) % worker_count;
    let offset = shard_idx % worker_count;
    (base + offset) % worker_count
}

impl BufferFlusher {
    /// Sole consumer of the raw commit-job queue. If every executor drains the
    /// MPMC queue itself, they race for individual jobs and fragment a deep
    /// backlog back into tiny metadb transactions.
    pub(in crate::buffer::flush) fn commit_aggregator_loop(
        rx: Receiver<CommitJob>,
        batch_tx: Sender<Vec<CommitJob>>,
        coalesce_lba_budget: usize,
        coalesce_timeout: Duration,
        packed_try_drain_lba_budget: usize,
    ) {
        while let Ok(first) = rx.recv() {
            let batch = Self::drain_commit_batch(
                first,
                &rx,
                coalesce_lba_budget,
                coalesce_timeout,
                packed_try_drain_lba_budget,
            );
            if batch_tx.send(batch).is_err() {
                break;
            }
        }
    }

    pub(in crate::buffer::flush) fn commit_worker_loop(
        worker_idx: usize,
        rx: &Receiver<Vec<CommitJob>>,
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
        let _ = worker_idx;
        // The aggregator closes this queue only after the raw writer queue is
        // disconnected and its final partial batch is forwarded. Receiving to
        // disconnect is therefore also the shutdown drain protocol.
        loop {
            let recv_start = Instant::now();
            let Ok(jobs) = rx.recv() else {
                break;
            };
            let idle_ns = recv_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            metrics
                .flush_commit_worker_rx_idle_ns
                .fetch_add(idle_ns, Ordering::Relaxed);
            metrics
                .flush_commit_worker_rx_idle_iters
                .fetch_add(1, Ordering::Relaxed);
            Self::dispatch_commit_batch(
                jobs,
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
            let first_is_packed = matches!(batch[0], CommitJob::Packed(_));
            let first_is_dedup = matches!(batch[0], CommitJob::DedupHit(_));
            if packed_try_drain_lba_budget > 0 && (first_is_packed || first_is_dedup) {
                let mut total_lbas = lbas_in_job(&batch[0]);
                while total_lbas < packed_try_drain_lba_budget {
                    match rx.try_recv() {
                        Ok(job) => {
                            let same_kind = matches!(job, CommitJob::Packed(_)) == first_is_packed
                                && matches!(job, CommitJob::DedupHit(_)) == first_is_dedup;
                            total_lbas = total_lbas.saturating_add(lbas_in_job(&job));
                            batch.push(job);
                            if !same_kind {
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
        let mut pending_pt: HashMap<String, PassthroughCommitJob> = HashMap::new();
        let mut pending_packed: Vec<PackedCommitJob> = Vec::new();
        let mut pending_dedup: Vec<DedupHitCommitJob> = Vec::new();
        for job in jobs {
            match job {
                CommitJob::Passthrough(mut pj) => {
                    pending_pt
                        .entry(pj.vol_id.0.clone())
                        .and_modify(|existing| existing.units.append(&mut pj.units))
                        .or_insert(pj);
                }
                CommitJob::Packed(pj) => {
                    pending_packed.push(pj);
                }
                CommitJob::DedupHit(job) => pending_dedup.push(job),
            }
        }

        // Keep foreground-shaped passthrough completions ahead of packed metadata
        // work within a worker drain. NVMe mixed runs showed that globally
        // reordering packed before passthrough improves packed lbas/commit but
        // explodes commit-worker queue wait. Folding packed only at the drain
        // tail keeps PT moving while still collapsing many one-slot packed txs.
        for pj in pending_pt.into_values() {
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

        if !pending_dedup.is_empty() {
            Self::dispatch_dedup_hit_jobs(pending_dedup, meta, lifecycle, metrics);
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

    pub(in crate::buffer::flush) fn dispatch_dedup_hit_jobs(
        jobs: Vec<DedupHitCommitJob>,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        metrics: &EngineMetrics,
    ) {
        let mut by_volume: HashMap<(String, u64), Vec<DedupHitCommitJob>> = HashMap::new();
        for job in jobs {
            by_volume
                .entry((job.vol_id.0.clone(), job.vol_created_at))
                .or_default()
                .push(job);
        }

        for ((vol_id_str, vol_created_at), jobs) in by_volume {
            let Some(first) = jobs.first() else {
                continue;
            };
            let vol_id = first.vol_id.clone();
            let mut hits = Vec::new();
            let mut promote_entries = Vec::new();
            let mut seqs = Vec::new();
            let mut spans = Vec::with_capacity(jobs.len());
            for job in &jobs {
                spans.push(job.hits.len());
                hits.extend_from_slice(&job.hits);
                promote_entries.extend_from_slice(&job.promote_entries);
                seqs.extend_from_slice(&job.seqs);
            }

            let service_start = Instant::now();
            let result = lifecycle.with_read_lock(&vol_id_str, || {
                let generation_matches = meta
                    .get_volume(&vol_id)?
                    .is_some_and(|volume| volume.created_at == vol_created_at);
                if !generation_matches {
                    return Err(crate::error::OnyxError::VolumeDeleted(vol_id_str.clone()));
                }
                meta.atomic_batch_dedup_hits_with_promote(&vol_id, &hits, &promote_entries, &seqs)
            });
            Self::record_elapsed(&metrics.dedup_hit_commit_ns, service_start);
            Self::record_elapsed(&metrics.flush_commit_worker_service_ns, service_start);

            match result {
                Ok((results, newly_zeroed)) => {
                    let mut offset = 0usize;
                    let mut newly_zeroed = Some(newly_zeroed);
                    for (job, span) in jobs.into_iter().zip(spans) {
                        let response = DedupHitCommitResponse::Committed {
                            results: results[offset..offset + span].to_vec(),
                            newly_zeroed: newly_zeroed.take().unwrap_or_default(),
                        };
                        offset += span;
                        let _ = job.response_tx.send(response);
                    }
                }
                Err(error) => {
                    let error = error.to_string();
                    for job in jobs {
                        let _ = job
                            .response_tx
                            .send(DedupHitCommitResponse::Failed(error.clone()));
                    }
                }
            }
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
        CommitJob::DedupHit(job) => job.hits.len(),
    }
}

fn enqueued_at_for_job(job: &CommitJob) -> Instant {
    match job {
        CommitJob::Passthrough(pj) => pj.enqueued_at,
        CommitJob::Packed(pj) => pj.enqueued_at,
        CommitJob::DedupHit(job) => job.enqueued_at,
    }
}
