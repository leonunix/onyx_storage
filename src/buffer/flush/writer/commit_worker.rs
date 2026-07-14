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

/// Emergency tail-flush threshold. Normal batching stays bounded by the 8K
/// transaction target; this only trades batching efficiency for faster LV2
/// consumption when a physical ring is already close to hard backpressure.
const COMMIT_AGGREGATOR_PRESSURE_PCT: u8 = 80;
const COMMIT_AGGREGATOR_PRESSURE_SAMPLE_INTERVAL: Duration = Duration::from_millis(10);

/// Two deadline-limited underfilled batches are enough to establish that the
/// current arrival rate cannot fill the configured transaction target. Once
/// active, keep the mode through one transient full batch and leave it after
/// the second consecutive full batch. This avoids oscillating at the boundary.
const COMMIT_ADAPTIVE_UNDERFILL_ENTER_BATCHES: u8 = 2;
const COMMIT_ADAPTIVE_UNDERFILL_EXIT_BATCHES: u8 = 2;
const COMMIT_ADAPTIVE_MIN_FILL_DIVISOR: usize = 4;
const COMMIT_ADAPTIVE_WINDOW_DIVISOR: u32 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommitSealReason {
    Target,
    Capacity,
    Deadline,
    AdaptiveUnderfill,
    Pressure,
    Shutdown,
}

/// Cross-batch state for the retain-tail aggregator. The configured coalesce
/// timeout is a hard residence ceiling measured from the oldest queued job,
/// not an idle timeout restarted by every arrival. Under sustained underfill,
/// a useful quarter-target batch can seal at half that ceiling.
struct AdaptiveCommitSeal {
    target_lbas: usize,
    max_window: Duration,
    underfill_deadline_batches: u8,
    full_recovery_batches: u8,
    underfill_active: bool,
    last_sealed_at: Option<Instant>,
}

impl AdaptiveCommitSeal {
    fn new(target_lbas: usize, max_window: Duration) -> Self {
        Self {
            target_lbas: target_lbas.max(1),
            max_window,
            underfill_deadline_batches: 0,
            full_recovery_batches: 0,
            underfill_active: false,
            last_sealed_at: None,
        }
    }

    fn begin_batch(&mut self, now: Instant) {
        if self
            .last_sealed_at
            .is_some_and(|last| now.saturating_duration_since(last) >= self.max_window)
        {
            self.reset_underfill_history();
        }
    }

    fn immediate_reason(&self, lbas: usize, under_pressure: bool) -> Option<CommitSealReason> {
        if lbas >= self.target_lbas {
            Some(CommitSealReason::Target)
        } else if under_pressure {
            Some(CommitSealReason::Pressure)
        } else {
            None
        }
    }

    fn timed_reason(
        &self,
        lbas: usize,
        oldest_enqueued_at: Instant,
        now: Instant,
    ) -> Option<CommitSealReason> {
        let age = now.saturating_duration_since(oldest_enqueued_at);
        if age >= self.max_window {
            return Some(CommitSealReason::Deadline);
        }
        if self.underfill_active
            && lbas >= self.adaptive_min_lbas()
            && age >= self.adaptive_window()
        {
            return Some(CommitSealReason::AdaptiveUnderfill);
        }
        None
    }

    fn next_timed_wake(&self, lbas: usize, oldest_enqueued_at: Instant, now: Instant) -> Duration {
        let hard_deadline = oldest_enqueued_at + self.max_window;
        let mut wake_at = hard_deadline;
        if self.underfill_active && lbas >= self.adaptive_min_lbas() {
            wake_at = wake_at.min(oldest_enqueued_at + self.adaptive_window());
        }
        wake_at.saturating_duration_since(now)
    }

    fn observe_seal(&mut self, reason: CommitSealReason, lbas: usize, now: Instant) {
        match reason {
            CommitSealReason::Deadline if lbas < self.target_lbas => {
                self.full_recovery_batches = 0;
                self.underfill_deadline_batches = self
                    .underfill_deadline_batches
                    .saturating_add(1)
                    .min(COMMIT_ADAPTIVE_UNDERFILL_ENTER_BATCHES);
                if self.underfill_deadline_batches >= COMMIT_ADAPTIVE_UNDERFILL_ENTER_BATCHES {
                    self.underfill_active = true;
                }
            }
            CommitSealReason::AdaptiveUnderfill => {
                self.full_recovery_batches = 0;
                self.underfill_deadline_batches = COMMIT_ADAPTIVE_UNDERFILL_ENTER_BATCHES;
                self.underfill_active = true;
            }
            CommitSealReason::Target => {
                self.underfill_deadline_batches = 0;
                if self.underfill_active {
                    self.full_recovery_batches = self.full_recovery_batches.saturating_add(1);
                    if self.full_recovery_batches >= COMMIT_ADAPTIVE_UNDERFILL_EXIT_BATCHES {
                        self.reset_underfill_history();
                    }
                }
            }
            CommitSealReason::Capacity => {
                // An indivisible next job crossed the target. This says
                // nothing about arrival rate and is not a full-target
                // recovery; it only breaks a consecutive recovery streak.
                self.full_recovery_batches = 0;
            }
            CommitSealReason::Deadline
            | CommitSealReason::Pressure
            | CommitSealReason::Shutdown => {}
        }
        self.last_sealed_at = Some(now);
    }

    fn adaptive_min_lbas(&self) -> usize {
        self.target_lbas
            .saturating_add(COMMIT_ADAPTIVE_MIN_FILL_DIVISOR - 1)
            / COMMIT_ADAPTIVE_MIN_FILL_DIVISOR
    }

    fn adaptive_window(&self) -> Duration {
        self.max_window / COMMIT_ADAPTIVE_WINDOW_DIVISOR
    }

    fn reset_underfill_history(&mut self) {
        self.underfill_deadline_batches = 0;
        self.full_recovery_batches = 0;
        self.underfill_active = false;
    }
}

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

pub(in crate::buffer::flush) struct CommitBatch {
    pub jobs: Vec<CommitJob>,
    sealed_at: Instant,
}

impl CommitBatch {
    fn new(jobs: Vec<CommitJob>) -> Self {
        Self::new_at(jobs, Instant::now())
    }

    fn new_at(jobs: Vec<CommitJob>, sealed_at: Instant) -> Self {
        Self { jobs, sealed_at }
    }
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
        batch_tx: Sender<CommitBatch>,
        pressure_pool: Option<Arc<WriteBufferPool>>,
        metrics: Option<Arc<EngineMetrics>>,
        retain_tail: bool,
        target_lbas_per_tx: usize,
        coalesce_lba_budget: usize,
        coalesce_timeout: Duration,
        packed_try_drain_lba_budget: usize,
    ) {
        if retain_tail && coalesce_lba_budget > 0 {
            let target_lbas = target_lbas_per_tx.max(1);
            let batch_lba_limit = coalesce_lba_budget.min(target_lbas).max(1);
            let mut seal_policy = AdaptiveCommitSeal::new(batch_lba_limit, coalesce_timeout);
            let mut batch = Vec::new();
            let mut total_lbas = 0usize;
            let mut oldest_enqueued_at = None;
            let mut pending_job = None;
            let mut last_pressure_sample = None;
            loop {
                let job = if let Some(job) = pending_job.take() {
                    job
                } else if batch.is_empty() {
                    match rx.recv() {
                        Ok(job) => job,
                        Err(_) => break,
                    }
                } else {
                    unreachable!("non-empty batch must choose its next receive below")
                };

                let now = Instant::now();
                let job_lbas = lbas_in_job(&job);
                if !batch.is_empty()
                    && total_lbas.saturating_add(job_lbas) > batch_lba_limit
                    && !seal_commit_batch(
                        &batch_tx,
                        &mut batch,
                        &mut total_lbas,
                        &mut oldest_enqueued_at,
                        &mut seal_policy,
                        pressure_pool.as_deref(),
                        metrics.as_deref(),
                        CommitSealReason::Capacity,
                    )
                {
                    break;
                }
                if batch.is_empty() {
                    seal_policy.begin_batch(now);
                }
                let job_enqueued_at = enqueued_at_for_job(&job);
                oldest_enqueued_at = Some(
                    oldest_enqueued_at.map_or(job_enqueued_at, |oldest: Instant| {
                        oldest.min(job_enqueued_at)
                    }),
                );
                total_lbas = total_lbas.saturating_add(job_lbas);
                batch.push(job);

                if let Some(reason) = seal_policy.immediate_reason(total_lbas, false) {
                    if !seal_commit_batch(
                        &batch_tx,
                        &mut batch,
                        &mut total_lbas,
                        &mut oldest_enqueued_at,
                        &mut seal_policy,
                        pressure_pool.as_deref(),
                        metrics.as_deref(),
                        reason,
                    ) {
                        break;
                    }
                    continue;
                }
                // A non-zero window is a hard oldest-job ceiling even when
                // producers keep the raw queue continuously non-empty. The
                // zero-window mode intentionally try-drains already queued
                // jobs up to the target before sealing.
                if coalesce_timeout > Duration::ZERO {
                    let oldest = oldest_enqueued_at.expect("non-empty batch has an enqueue time");
                    if let Some(reason) = seal_policy.timed_reason(total_lbas, oldest, now) {
                        if !seal_commit_batch(
                            &batch_tx,
                            &mut batch,
                            &mut total_lbas,
                            &mut oldest_enqueued_at,
                            &mut seal_policy,
                            pressure_pool.as_deref(),
                            metrics.as_deref(),
                            reason,
                        ) {
                            break;
                        }
                        continue;
                    }
                }

                match rx.try_recv() {
                    Ok(job) => {
                        pending_job = Some(job);
                        continue;
                    }
                    Err(crossbeam_channel::TryRecvError::Disconnected) => {
                        let _ = seal_commit_batch(
                            &batch_tx,
                            &mut batch,
                            &mut total_lbas,
                            &mut oldest_enqueued_at,
                            &mut seal_policy,
                            pressure_pool.as_deref(),
                            metrics.as_deref(),
                            CommitSealReason::Shutdown,
                        );
                        break;
                    }
                    Err(crossbeam_channel::TryRecvError::Empty) => {}
                }

                loop {
                    let now = Instant::now();
                    if sample_commit_aggregator_pressure(
                        pressure_pool.as_deref(),
                        &mut last_pressure_sample,
                        now,
                    ) && !seal_commit_batch(
                        &batch_tx,
                        &mut batch,
                        &mut total_lbas,
                        &mut oldest_enqueued_at,
                        &mut seal_policy,
                        pressure_pool.as_deref(),
                        metrics.as_deref(),
                        CommitSealReason::Pressure,
                    ) {
                        return;
                    } else if batch.is_empty() {
                        break;
                    }

                    let oldest = oldest_enqueued_at.expect("non-empty batch has an enqueue time");
                    if let Some(reason) = seal_policy.timed_reason(total_lbas, oldest, now) {
                        if !seal_commit_batch(
                            &batch_tx,
                            &mut batch,
                            &mut total_lbas,
                            &mut oldest_enqueued_at,
                            &mut seal_policy,
                            pressure_pool.as_deref(),
                            metrics.as_deref(),
                            reason,
                        ) {
                            return;
                        }
                        break;
                    }

                    let mut wait = seal_policy.next_timed_wake(total_lbas, oldest, now);
                    if pressure_pool.is_some() {
                        let pressure_wait = last_pressure_sample.map_or(Duration::ZERO, |last| {
                            COMMIT_AGGREGATOR_PRESSURE_SAMPLE_INTERVAL
                                .saturating_sub(now.saturating_duration_since(last))
                        });
                        wait = wait.min(pressure_wait);
                    }
                    match rx.recv_timeout(wait) {
                        Ok(job) => {
                            pending_job = Some(job);
                            break;
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                            let _ = seal_commit_batch(
                                &batch_tx,
                                &mut batch,
                                &mut total_lbas,
                                &mut oldest_enqueued_at,
                                &mut seal_policy,
                                pressure_pool.as_deref(),
                                metrics.as_deref(),
                                CommitSealReason::Shutdown,
                            );
                            return;
                        }
                    }
                }
            }
            return;
        }

        while let Ok(first) = rx.recv() {
            let batch = Self::drain_commit_batch(
                first,
                &rx,
                coalesce_lba_budget,
                coalesce_timeout,
                packed_try_drain_lba_budget,
            );
            if let Err(error) = batch_tx.send(CommitBatch::new(batch)) {
                fail_commit_batch_handoff(error.0, pressure_pool.as_deref(), metrics.as_deref());
                break;
            }
        }
    }

    pub(in crate::buffer::flush) fn commit_worker_loop(
        worker_idx: usize,
        rx: &Receiver<CommitBatch>,
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
            let Ok(batch) = rx.recv() else {
                break;
            };
            let idle_ns = recv_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            metrics
                .flush_commit_worker_rx_idle_ns
                .fetch_add(idle_ns, Ordering::Relaxed);
            metrics
                .flush_commit_worker_rx_idle_iters
                .fetch_add(1, Ordering::Relaxed);
            Self::record_commit_worker_drain(metrics, &batch.jobs, batch.sealed_at);
            Self::dispatch_commit_batch(
                batch.jobs,
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

    fn record_commit_worker_drain(metrics: &EngineMetrics, jobs: &[CommitJob], sealed_at: Instant) {
        let dequeued_at = Instant::now();
        let drain_jobs = jobs.len() as u64;
        let drain_lbas = jobs.iter().map(lbas_in_job).sum::<usize>() as u64;
        let (queue_wait_ns, aggregator_residence_ns, executor_queue_wait_ns) =
            commit_wait_components_ns(jobs.iter().map(enqueued_at_for_job), sealed_at, dequeued_at);
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
            .flush_commit_worker_aggregator_residence_ns
            .fetch_add(aggregator_residence_ns, Ordering::Relaxed);
        metrics
            .flush_commit_worker_executor_queue_wait_ns
            .fetch_add(executor_queue_wait_ns, Ordering::Relaxed);
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
        let done_tx = lane_done_txs
            .get(primary_shard)
            .or_else(|| lane_done_txs.first());
        let Some(cleanup_tx) = cleanup_tx else {
            tracing::error!(
                shard_idx = primary_shard,
                "commit_worker: cleanup channel missing; failing passthrough job"
            );
            Self::fail_undispatched_passthrough_job(
                pj,
                pool,
                allocator,
                in_flight_tracker,
                metrics,
                done_tx,
                "commit worker cleanup channel unavailable",
            );
            return;
        };
        if done_tx.is_none() {
            tracing::error!(
                shard_idx = primary_shard,
                "commit_worker: done channel missing; failing passthrough job"
            );
            Self::fail_undispatched_passthrough_job(
                pj,
                pool,
                allocator,
                in_flight_tracker,
                metrics,
                None,
                "commit worker done channel unavailable",
            );
            return;
        }
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
                "commit_worker: cleanup channel missing; failing packed jobs"
            );
            for job in jobs {
                let done_tx = lane_done_txs
                    .get(job.shard_idx)
                    .or_else(|| lane_done_txs.first());
                Self::fail_undispatched_packed_job(
                    job,
                    pool,
                    allocator,
                    in_flight_tracker,
                    metrics,
                    done_tx,
                    "commit worker cleanup channel unavailable",
                );
            }
            return;
        };
        if lane_done_txs.is_empty() {
            tracing::error!(
                shard_idx = first.shard_idx,
                "commit_worker: done channels missing; failing packed jobs"
            );
            for job in jobs {
                Self::fail_undispatched_packed_job(
                    job,
                    pool,
                    allocator,
                    in_flight_tracker,
                    metrics,
                    None,
                    "commit worker done channel unavailable",
                );
            }
            return;
        }
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

fn sample_commit_aggregator_pressure(
    pressure_pool: Option<&WriteBufferPool>,
    last_sample: &mut Option<Instant>,
    now: Instant,
) -> bool {
    let Some(pool) = pressure_pool else {
        return false;
    };
    if last_sample.is_some_and(|last| {
        now.saturating_duration_since(last) < COMMIT_AGGREGATOR_PRESSURE_SAMPLE_INTERVAL
    }) {
        return false;
    }
    *last_sample = Some(now);
    pool.physical_fill_percentage() >= COMMIT_AGGREGATOR_PRESSURE_PCT
}

fn seal_commit_batch(
    batch_tx: &Sender<CommitBatch>,
    batch: &mut Vec<CommitJob>,
    total_lbas: &mut usize,
    oldest_enqueued_at: &mut Option<Instant>,
    seal_policy: &mut AdaptiveCommitSeal,
    failure_pool: Option<&WriteBufferPool>,
    metrics: Option<&EngineMetrics>,
    reason: CommitSealReason,
) -> bool {
    if batch.is_empty() {
        return true;
    }
    let sealed_at = Instant::now();
    seal_policy.observe_seal(reason, *total_lbas, sealed_at);
    if let Some(metrics) = metrics {
        let counter = match reason {
            CommitSealReason::Target => &metrics.flush_commit_aggregator_seals_target,
            CommitSealReason::Capacity => &metrics.flush_commit_aggregator_seals_capacity,
            CommitSealReason::Deadline => &metrics.flush_commit_aggregator_seals_deadline,
            CommitSealReason::AdaptiveUnderfill => {
                &metrics.flush_commit_aggregator_seals_adaptive_underfill
            }
            CommitSealReason::Pressure => &metrics.flush_commit_aggregator_seals_pressure,
            CommitSealReason::Shutdown => &metrics.flush_commit_aggregator_seals_shutdown,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
    *total_lbas = 0;
    *oldest_enqueued_at = None;
    match batch_tx.send(CommitBatch::new_at(std::mem::take(batch), sealed_at)) {
        Ok(()) => true,
        Err(error) => {
            fail_commit_batch_handoff(error.0, failure_pool, metrics);
            false
        }
    }
}

/// All commit executors disappeared after writers had already transferred LV3
/// reservations into the aggregator. There is no safe in-process destination
/// left for passthrough/packed ownership, so enter fail-stop mode: fence new
/// durable appends and leave the LV2 records unapplied for restart replay.
/// Allocator rebuild then reclaims their uncommitted LV3 reservations. Dedup
/// jobs have an explicit response channel and can be demoted to misses now.
fn fail_commit_batch_handoff(
    batch: CommitBatch,
    pool: Option<&WriteBufferPool>,
    metrics: Option<&EngineMetrics>,
) {
    if let Some(pool) = pool {
        pool.fence_meta("all commit executors disconnected");
    }
    if let Some(metrics) = metrics {
        metrics
            .flush_errors
            .fetch_add(batch.jobs.len() as u64, Ordering::Relaxed);
    }
    for job in batch.jobs {
        if let CommitJob::DedupHit(job) = job {
            let _ = job.response_tx.send(DedupHitCommitResponse::Failed(
                "all commit executors disconnected".to_string(),
            ));
        }
    }
    tracing::error!("commit aggregator: all executors disconnected; persistence fenced");
}

fn commit_wait_components_ns(
    enqueued_at: impl IntoIterator<Item = Instant>,
    sealed_at: Instant,
    dequeued_at: Instant,
) -> (u64, u64, u64) {
    let mut jobs = 0u128;
    let mut total_ns = 0u128;
    let mut aggregator_ns = 0u128;
    for enqueued_at in enqueued_at {
        jobs += 1;
        total_ns = total_ns.saturating_add(
            dequeued_at
                .saturating_duration_since(enqueued_at)
                .as_nanos(),
        );
        aggregator_ns = aggregator_ns
            .saturating_add(sealed_at.saturating_duration_since(enqueued_at).as_nanos());
    }
    let executor_ns = dequeued_at
        .saturating_duration_since(sealed_at)
        .as_nanos()
        .saturating_mul(jobs);
    let cap = |value: u128| value.min(u64::MAX as u128) as u64;
    (cap(total_ns), cap(aggregator_ns), cap(executor_ns))
}

#[cfg(test)]
mod wait_component_tests {
    use super::*;

    #[test]
    fn adaptive_seal_enforces_oldest_job_deadline_and_target() {
        let start = Instant::now();
        let policy = AdaptiveCommitSeal::new(8_192, Duration::from_millis(75));

        assert_eq!(
            policy.immediate_reason(8_192, false),
            Some(CommitSealReason::Target)
        );
        assert_eq!(
            policy.timed_reason(3_000, start, start + Duration::from_millis(74)),
            None
        );
        assert_eq!(
            policy.timed_reason(3_000, start, start + Duration::from_millis(75)),
            Some(CommitSealReason::Deadline)
        );
        assert_eq!(
            policy.next_timed_wake(3_000, start, start + Duration::from_millis(20)),
            Duration::from_millis(55)
        );
    }

    #[test]
    fn adaptive_seal_activates_on_underfill_and_pressure_bypasses_wait() {
        let start = Instant::now();
        let mut policy = AdaptiveCommitSeal::new(8_192, Duration::from_millis(80));

        policy.begin_batch(start);
        policy.observe_seal(
            CommitSealReason::Deadline,
            3_000,
            start + Duration::from_millis(80),
        );
        policy.begin_batch(start + Duration::from_millis(81));
        policy.observe_seal(
            CommitSealReason::Deadline,
            3_100,
            start + Duration::from_millis(161),
        );
        assert!(policy.underfill_active);

        let oldest = start + Duration::from_millis(162);
        assert_eq!(
            policy.timed_reason(2_047, oldest, oldest + Duration::from_millis(60)),
            None
        );
        assert_eq!(
            policy.timed_reason(2_048, oldest, oldest + Duration::from_millis(40)),
            Some(CommitSealReason::AdaptiveUnderfill)
        );
        assert_eq!(
            policy.immediate_reason(1, true),
            Some(CommitSealReason::Pressure)
        );
    }

    #[test]
    fn adaptive_seal_uses_hysteresis_and_resets_after_idle() {
        let start = Instant::now();
        let mut policy = AdaptiveCommitSeal::new(8_192, Duration::from_millis(75));

        policy.observe_seal(
            CommitSealReason::Deadline,
            3_000,
            start + Duration::from_millis(75),
        );
        policy.observe_seal(
            CommitSealReason::Deadline,
            3_000,
            start + Duration::from_millis(150),
        );
        assert!(policy.underfill_active);

        policy.observe_seal(
            CommitSealReason::Target,
            8_192,
            start + Duration::from_millis(151),
        );
        assert!(
            policy.underfill_active,
            "one full batch must not flap the mode off"
        );
        policy.observe_seal(
            CommitSealReason::Capacity,
            6_324,
            start + Duration::from_millis(152),
        );
        assert!(
            policy.underfill_active,
            "3162 + 3162 carry seal must not masquerade as full recovery"
        );
        policy.observe_seal(
            CommitSealReason::Target,
            8_192,
            start + Duration::from_millis(153),
        );
        assert!(
            policy.underfill_active,
            "capacity must break the consecutive full-target recovery streak"
        );
        policy.observe_seal(
            CommitSealReason::Target,
            8_192,
            start + Duration::from_millis(154),
        );
        assert!(!policy.underfill_active);

        policy.observe_seal(
            CommitSealReason::Deadline,
            3_000,
            start + Duration::from_millis(225),
        );
        policy.observe_seal(
            CommitSealReason::Deadline,
            3_000,
            start + Duration::from_millis(300),
        );
        assert!(policy.underfill_active);
        policy.begin_batch(start + Duration::from_millis(375));
        assert!(
            !policy.underfill_active,
            "an idle max-window starts a fresh burst"
        );
    }

    #[test]
    fn commit_wait_components_split_total_by_job() {
        let start = Instant::now();
        let sealed_at = start + Duration::from_millis(30);
        let dequeued_at = start + Duration::from_millis(35);
        let (total, aggregator, executor) = commit_wait_components_ns(
            [start, start + Duration::from_millis(10)],
            sealed_at,
            dequeued_at,
        );

        assert_eq!(total, 60_000_000);
        assert_eq!(aggregator, 50_000_000);
        assert_eq!(executor, 10_000_000);
        assert_eq!(total, aggregator + executor);
    }
}
