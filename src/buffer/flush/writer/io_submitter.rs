//! Per-shard async IO submitter — Phase 4 of the per-volume commit
//! architecture (see `.claude/plans/per-volume-commit-worker.md`).
//!
//! After Phase 1, the shard writer's hot path still synchronously
//! waits on `io_engine.submit_batch` (~5 ms per cycle) before it can
//! build a CommitJob and dispatch the next batch of work. That keeps
//! per-shard cycle time around 5-6 ms even though the metadb commit
//! itself moved to the commit worker. Phase 4 lifts the LV3 IO out of
//! the shard writer's hot path: the shard writer hands an
//! `IoSubmitJob` (CommitJob owned, target commit worker remembered)
//! to its dedicated `io_submitter[shard_idx]` thread and returns
//! immediately. The submitter borrows the CommitJob's payloads,
//! drives `submit_batch`, applies IO success/failure to the units
//! in-place, and then forwards the surviving CommitJob to
//! `commit_worker_txs[target]`.
//!
//! The shard writer cycle drops from ~5-6 ms to ~1 ms (alloc +
//! dispatch only) so it can produce more CommitJobs per second; the
//! commit worker queue stays warm.

use super::*;

/// Wrapper sent shard-writer → io_submitter. The io_submitter borrows
/// the CommitJob's IO payloads (CompressedUnit::compressed_data /
/// SealedSlot::data), runs `submit_batch`, and on success forwards
/// the CommitJob to `commit_worker_txs[target_worker_idx]`. On
/// failure it frees the failed unit's PBA, defer_retries its seqs,
/// and signals `done_tx` inline — same semantics as the historic
/// shard-writer-inline IO failure path.
pub(in crate::buffer::flush) struct IoSubmitJob {
    pub job: CommitJob,
    pub target_worker_idx: usize,
}

impl BufferFlusher {
    /// Drain budget — how many additional IoSubmitJobs the submitter
    /// will pull off the queue and submit as one combined
    /// `submit_batch`. The io_uring backend benefits from larger
    /// batches (fewer SQE chains, better device queue depth); even
    /// with the syscall backend the scoped-thread fan-out is no
    /// worse for combined ops.
    const IO_SUBMIT_DRAIN_BUDGET: usize = 32;

    pub(in crate::buffer::flush) fn io_submitter_loop(
        shard_idx: usize,
        rx: &Receiver<IoSubmitJob>,
        io_engine: &IoEngine,
        allocator: &SpaceAllocator,
        in_flight_tracker: &FlusherInFlightTracker,
        metrics: &EngineMetrics,
        commit_worker_txs: &[Sender<CommitJob>],
        lane_done_txs: &[Sender<Vec<u64>>],
        running: &AtomicBool,
    ) {
        let _ = shard_idx;
        while running.load(Ordering::Relaxed) {
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(first) => {
                    let mut batch = vec![first];
                    while batch.len() < Self::IO_SUBMIT_DRAIN_BUDGET {
                        match rx.try_recv() {
                            Ok(more) => batch.push(more),
                            Err(_) => break,
                        }
                    }
                    Self::submit_io_batch(
                        batch,
                        io_engine,
                        allocator,
                        in_flight_tracker,
                        metrics,
                        commit_worker_txs,
                        lane_done_txs,
                    );
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }
        }
        // Drain remaining at shutdown.
        let mut tail: Vec<IoSubmitJob> = Vec::new();
        while let Ok(j) = rx.try_recv() {
            tail.push(j);
        }
        if !tail.is_empty() {
            Self::submit_io_batch(
                tail,
                io_engine,
                allocator,
                in_flight_tracker,
                metrics,
                commit_worker_txs,
                lane_done_txs,
            );
        }
    }

    /// Submit a batch of IoSubmitJobs as one combined `submit_batch`.
    /// Build a flat ops vector borrowing from each job's payload,
    /// remember which (job_idx, op_idx) each global op belongs to,
    /// then map per-op results back to per-job per-unit failure.
    fn submit_io_batch(
        mut batch: Vec<IoSubmitJob>,
        io_engine: &IoEngine,
        allocator: &SpaceAllocator,
        in_flight_tracker: &FlusherInFlightTracker,
        metrics: &EngineMetrics,
        commit_worker_txs: &[Sender<CommitJob>],
        lane_done_txs: &[Sender<Vec<u64>>],
    ) {
        if batch.is_empty() {
            return;
        }
        let io_start = Instant::now();

        // (job_idx, unit_idx_within_job). For Packed jobs unit_idx is 0.
        let mut owner: Vec<(usize, usize)> = Vec::new();
        let mut ops: Vec<crate::io::engine::LvOp<'_>> = Vec::new();
        for (job_idx, sj) in batch.iter().enumerate() {
            match &sj.job {
                CommitJob::Passthrough(pj) => {
                    for (u_idx, ucd) in pj.units.iter().enumerate() {
                        allocator.wait_for_readers(ucd.pba, ucd.alloc_blocks);
                        ops.push(crate::io::engine::LvOp::Write {
                            pba: ucd.pba,
                            payload: ucd.unit.compressed_data.as_slice(),
                        });
                        owner.push((job_idx, u_idx));
                    }
                }
                CommitJob::Packed(pj) => {
                    allocator.wait_for_readers(pj.sealed.pba, 1);
                    ops.push(crate::io::engine::LvOp::Write {
                        pba: pj.sealed.pba,
                        payload: pj.sealed.data.as_slice(),
                    });
                    owner.push((job_idx, 0));
                }
            }
        }

        let total_ops = ops.len();
        if total_ops == 0 {
            // Nothing to write — forward all jobs straight through.
            for sj in batch {
                Self::forward_job(sj, commit_worker_txs);
            }
            Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);
            return;
        }

        // Per-op failure flag, indexed by global op idx.
        let mut op_failed: Vec<bool> = vec![false; total_ops];
        match io_engine.submit_batch(ops, false) {
            Ok(per_op) => {
                use crate::io::engine::LvOpResult;
                for (i, r) in per_op.into_iter().enumerate() {
                    if let LvOpResult::Write(Err(e)) = r {
                        op_failed[i] = true;
                        tracing::error!(error = %e, "io_submitter: per-op IO write failed");
                    }
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "io_submitter: submit_batch failed wholesale");
                for slot in op_failed.iter_mut() {
                    *slot = true;
                }
            }
        }
        Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);

        // Map per-op failures back to (job_idx, unit_idx) and apply.
        let n_jobs = batch.len();
        let mut per_job_keep: Vec<Vec<bool>> = (0..n_jobs).map(|_| Vec::new()).collect();
        // Initialise keep = true for every (job, unit).
        for (i, &(job_idx, unit_idx)) in owner.iter().enumerate() {
            let v = &mut per_job_keep[job_idx];
            if v.len() <= unit_idx {
                v.resize(unit_idx + 1, true);
            }
            if op_failed[i] {
                v[unit_idx] = false;
            }
        }

        // Apply failures and forward survivors.
        for (job_idx, mut sj) in batch.into_iter().enumerate() {
            let keep = std::mem::take(&mut per_job_keep[job_idx]);
            let any_survived = Self::apply_io_failures(
                &mut sj.job,
                &keep,
                allocator,
                in_flight_tracker,
                metrics,
                lane_done_txs,
            );
            if any_survived {
                Self::forward_job(sj, commit_worker_txs);
            }
        }
    }

    /// Filter failed units out of a CommitJob in place. Frees PBAs
    /// and signals defer_retry / done_tx for failed units. Returns
    /// true if any units survived (caller forwards) or false if the
    /// whole job is a wash (caller drops it).
    fn apply_io_failures(
        job: &mut CommitJob,
        keep: &[bool],
        allocator: &SpaceAllocator,
        in_flight_tracker: &FlusherInFlightTracker,
        metrics: &EngineMetrics,
        lane_done_txs: &[Sender<Vec<u64>>],
    ) -> bool {
        match job {
            CommitJob::Passthrough(pj) => {
                let original = std::mem::take(&mut pj.units);
                for (i, ucd) in original.into_iter().enumerate() {
                    let kept = keep.get(i).copied().unwrap_or(true);
                    if kept {
                        pj.units.push(ucd);
                    } else {
                        Self::handle_failed_passthrough_unit(
                            ucd,
                            pj.shard_idx,
                            allocator,
                            in_flight_tracker,
                            metrics,
                            lane_done_txs,
                        );
                    }
                }
                !pj.units.is_empty()
            }
            CommitJob::Packed(pj) => {
                let kept = keep.first().copied().unwrap_or(true);
                if kept {
                    true
                } else {
                    // Whole packed slot failed. Free its PBA and
                    // defer_retry buffered seqs. We drop the slot
                    // (no whole-slot retry preserved across the
                    // async IO boundary; the seqs feed back through
                    // coalesce + compress and may re-pack
                    // differently next round).
                    let _ = allocator.free_one(pj.sealed.pba);
                    metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                    if !pj.buffered_seqs.is_empty() {
                        in_flight_tracker
                            .defer_retry(&pj.buffered_seqs, Self::RETRY_BACKOFF);
                    }
                    for dc in &pj.buffered_completions {
                        in_flight_tracker.defer_retry(dc.seqs(), Self::RETRY_BACKOFF);
                    }
                    let done_tx = lane_done_txs
                        .get(pj.shard_idx)
                        .or_else(|| lane_done_txs.first());
                    if let Some(done_tx) = done_tx {
                        if !pj.buffered_seqs.is_empty() {
                            let _ = done_tx.send(std::mem::take(&mut pj.buffered_seqs));
                        }
                        for dc in pj.buffered_completions.drain(..) {
                            if let Some(orig) = dc.decrement() {
                                let _ = done_tx.send(orig);
                            }
                        }
                    }
                    false
                }
            }
        }
    }

    fn handle_failed_passthrough_unit(
        ucd: UnitCommitData,
        shard_idx: usize,
        allocator: &SpaceAllocator,
        in_flight_tracker: &FlusherInFlightTracker,
        metrics: &EngineMetrics,
        lane_done_txs: &[Sender<Vec<u64>>],
    ) {
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
        let done_tx = lane_done_txs
            .get(shard_idx)
            .or_else(|| lane_done_txs.first());
        if let Some(done_tx) = done_tx {
            match ucd.completion {
                None => {
                    let _ = done_tx.send(ucd.seqs);
                }
                Some(dc) => {
                    if let Some(orig) = dc.decrement() {
                        let _ = done_tx.send(orig);
                    }
                }
            }
        }
    }

    fn forward_job(sj: IoSubmitJob, commit_worker_txs: &[Sender<CommitJob>]) {
        if commit_worker_txs.is_empty() {
            return;
        }
        let tx_idx = sj.target_worker_idx % commit_worker_txs.len();
        let _ = commit_worker_txs[tx_idx].send(sj.job);
    }
}
