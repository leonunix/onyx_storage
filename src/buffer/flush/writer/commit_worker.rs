//! Per-volume commit worker — Phase 1 of the per-volume commit
//! architecture (see `.claude/plans/per-volume-commit-worker.md`).
//!
//! Shard writer threads now do alloc + LV3 IO and hand a `CommitJob`
//! to the worker indexed by `hash(vol_id) % NUM_COMMIT_WORKERS`. Each
//! worker drains its FIFO queue and runs the metadb commit + cleanup
//! + `mark_flushed` + `done_tx` chain serially. Same volume always
//! lands on the same worker, so per-volume LSN dispatch contention
//! drops to zero. With 16 shard writers and one volume, this collapses
//! 16 concurrent committers to 1 — the metadb sweet spot.

use super::*;

/// Sub-batch cap inside a single `CommitJob`. New per-volume routing
/// has 0 LSN contention, so each commit only pays its own WAL fsync
/// + apply lane scheduling cost. metadb-onyx-soak (2026-05-08) shows
/// the per-op amortisation sweet spot at 50–200 ops; 150 sits in the
/// middle and still leaves apply lanes headroom for cross-volume
/// concurrency. Picking too small (e.g. 10) leaves WAL fsync (~100µs)
/// un-amortised; too large (e.g. 1000+) reproduces the 22ms apply
/// lane stall observed with `buffer.shards=1`.
pub(in crate::buffer::flush) const TARGET_OPS_PER_COMMIT: usize = 150;

/// Number of dedicated commit workers. Per-volume routing uses
/// `hash(vol_id) % NUM_COMMIT_WORKERS`. The buffer keeps its own 16
/// shards for LV2/dedup/compress parallelism; this is independent.
pub(in crate::buffer::flush) const NUM_COMMIT_WORKERS: usize = 16;

/// Channel capacity per commit worker. ~64 jobs of slack absorbs
/// shard-writer cycle bursts without unbounded memory growth.
pub(in crate::buffer::flush) const COMMIT_WORKER_QUEUE_CAP: usize = 64;

/// Owned per-unit data the shard writer has staged (alloc + IO done).
pub(in crate::buffer::flush) struct UnitCommitData {
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
    pub shard_idx: usize,
    pub units: Vec<UnitCommitData>,
}

pub(in crate::buffer::flush) struct PackedCommitJob {
    pub sealed: SealedSlot,
    pub shard_idx: usize,
    /// `done_tx` payload for fragments that the open-slot saw but
    /// hasn't acked yet. Forwarded to `done_tx` after metadata commit
    /// publishes the slot.
    pub buffered_seqs: Vec<u64>,
    pub buffered_completions: Vec<Arc<crate::buffer::pipeline::DedupCompletion>>,
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
    live_positions: Vec<usize>,
    fresh_dedup_pairs: Vec<(ContentHash, BlockmapValue)>,
    stale_repairs: Vec<(ContentHash, DedupEntry, DedupEntry)>,
}

/// Stable hash → worker index. Same `vol_id` always routes to the
/// same worker; multiple volumes can share a worker. Independent of
/// metadb's internal shard hashing — callers don't need
/// `volume_ordinal` lookups on the hot path.
pub(in crate::buffer::flush) fn route_volume_to_worker(vol_id: &str) -> usize {
    if NUM_COMMIT_WORKERS <= 1 {
        return 0;
    }
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    vol_id.as_bytes().hash(&mut hasher);
    (hasher.finish() as usize) % NUM_COMMIT_WORKERS
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
        running: &AtomicBool,
    ) {
        let _ = worker_idx;
        // Main loop. Poll with timeout so shutdown signal is observed
        // even when the queue is idle. The shard writers are joined
        // before us (BufferFlusher::join_lanes), then their commit_tx
        // clones drop, the rx side disconnects, and we drain remaining
        // jobs before exiting.
        while running.load(Ordering::Relaxed) {
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(job) => Self::dispatch_commit_job(
                    job,
                    pool,
                    meta,
                    lifecycle,
                    allocator,
                    in_flight_tracker,
                    metrics,
                    lane_cleanup_txs,
                    candidate,
                    lane_done_txs,
                ),
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }
        }
        // Drain anything queued at shutdown. The shard writers may
        // have pushed final jobs after we observed `running == false`
        // but before they joined.
        while let Ok(job) = rx.try_recv() {
            Self::dispatch_commit_job(
                job,
                pool,
                meta,
                lifecycle,
                allocator,
                in_flight_tracker,
                metrics,
                lane_cleanup_txs,
                candidate,
                lane_done_txs,
            );
        }
    }

    fn dispatch_commit_job(
        job: CommitJob,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        in_flight_tracker: &FlusherInFlightTracker,
        metrics: &EngineMetrics,
        lane_cleanup_txs: &[Sender<CleanupBatch>],
        candidate: &crate::dedup::CandidateCache,
        lane_done_txs: &[Sender<Vec<u64>>],
    ) {
        // Pick the cleanup / done senders that match the originating
        // shard. Falling back to lane 0 if the shard_idx is somehow
        // out of range — defensive against future config changes.
        let shard_idx = match &job {
            CommitJob::Passthrough(pj) => pj.shard_idx,
            CommitJob::Packed(pj) => pj.shard_idx,
        };
        let cleanup_tx = lane_cleanup_txs
            .get(shard_idx)
            .or_else(|| lane_cleanup_txs.first());
        let done_tx = lane_done_txs
            .get(shard_idx)
            .or_else(|| lane_done_txs.first());
        let (Some(cleanup_tx), Some(done_tx)) = (cleanup_tx, done_tx) else {
            tracing::error!(
                shard_idx,
                "commit_worker: lane channels missing — dropping job"
            );
            return;
        };
        match job {
            CommitJob::Passthrough(pj) => Self::commit_passthrough_job(
                pj,
                pool,
                meta,
                lifecycle,
                allocator,
                in_flight_tracker,
                metrics,
                cleanup_tx,
                candidate,
                done_tx,
            ),
            CommitJob::Packed(pj) => Self::commit_packed_job(
                pj,
                pool,
                meta,
                lifecycle,
                allocator,
                in_flight_tracker,
                metrics,
                cleanup_tx,
                candidate,
                done_tx,
            ),
        }
    }

    /// Commit a single-volume passthrough batch. Acquires the volume's
    /// lifecycle read lock + per-unit L2P commit locks for the whole
    /// job, then flushes commits in sub-batches of ≤ TARGET_OPS_PER_COMMIT
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
        done_tx: &Sender<Vec<u64>>,
    ) {
        let total_start = Instant::now();
        let PassthroughCommitJob {
            vol_id,
            shard_idx,
            units,
        } = job;

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
                    done_tx,
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
                    && chunk_lbas.saturating_add(lbas) > TARGET_OPS_PER_COMMIT
                {
                    Self::commit_passthrough_chunk(
                        &chunk,
                        &vol_id,
                        &unit_metas,
                        meta,
                        candidate,
                        metrics,
                        &mut accum_old_meta,
                        &mut commit_failed_indices,
                    );
                    chunk.clear();
                    chunk_lbas = 0;
                }
                chunk.push(i);
                chunk_lbas += lbas;
                if lbas > TARGET_OPS_PER_COMMIT {
                    Self::commit_passthrough_chunk(
                        &chunk,
                        &vol_id,
                        &unit_metas,
                        meta,
                        candidate,
                        metrics,
                        &mut accum_old_meta,
                        &mut commit_failed_indices,
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
                    candidate,
                    metrics,
                    &mut accum_old_meta,
                    &mut commit_failed_indices,
                );
            }

            accum_old_meta
        });

        // Post-commit work outside the L2P commit lock.
        let cleanup_start = Instant::now();
        let commit_failed_set: std::collections::HashSet<usize> =
            commit_failed_indices.iter().copied().collect();

        for (i, ucd) in units.iter().enumerate() {
            if discarded[i] {
                if ucd.alloc_blocks == 1 {
                    let _ = allocator.free_one(ucd.pba);
                } else {
                    let _ = allocator.free_extent(Extent::new(ucd.pba, ucd.alloc_blocks));
                }
                for (seq, lba_start, lba_count) in &ucd.unit.seq_lba_ranges {
                    let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
                }
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
            let Some(um) = unit_metas[i].as_ref() else {
                continue;
            };
            if um.live_positions.is_empty() {
                if ucd.alloc_blocks == 1 {
                    let _ = allocator.free_one(ucd.pba);
                } else {
                    let _ = allocator.free_extent(Extent::new(ucd.pba, ucd.alloc_blocks));
                }
                for (seq, lba_start, lba_count) in &ucd.unit.seq_lba_ranges {
                    let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
                }
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
            for (seq, lba_start, lba_count) in &ucd.unit.seq_lba_ranges {
                let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
            }
        }

        if !actual_old_pba_meta.is_empty() {
            let _ = cleanup_tx.send(actual_old_pba_meta.into_values().collect());
        }
        Self::record_elapsed(&metrics.flush_writer_cleanup_ns, cleanup_start);

        // done_tx: success path → just send seqs (or completion.decrement()).
        // Failure path → defer_retry, then send.
        for (i, ucd) in units.into_iter().enumerate() {
            let UnitCommitData {
                seqs, completion, ..
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
                    let _ = done_tx.send(seqs);
                }
                Some(dc) => {
                    if let Some(original_seqs) = dc.decrement() {
                        let _ = done_tx.send(original_seqs);
                    }
                }
            }
        }

        let _ = pool.advance_tail_for_shard(shard_idx);
        Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
    }

    /// Commit one sub-batch (chunk of unit indices). Records freed
    /// PBA metadata in `accum_old_meta`. On commit error appends each
    /// non-empty unit's index to `commit_failed_indices` so the
    /// post-commit loop frees PBAs and defer_retries seqs.
    fn commit_passthrough_chunk(
        chunk: &[usize],
        vol_id: &VolumeId,
        unit_metas: &[Option<UnitMeta>],
        meta: &MetaStore,
        candidate: &crate::dedup::CandidateCache,
        metrics: &EngineMetrics,
        accum_old_meta: &mut HashMap<Pba, RemapCleanup>,
        commit_failed_indices: &mut Vec<usize>,
    ) {
        let mut batch_args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> =
            Vec::with_capacity(chunk.len());
        let mut all_fresh_pairs: Vec<(ContentHash, BlockmapValue)> = Vec::new();
        let mut all_stale_repairs: Vec<(ContentHash, DedupEntry, DedupEntry)> = Vec::new();
        let mut sub_batch_lbas: u64 = 0;

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
                sub_batch_lbas += um.batch_values.len() as u64;
                all_fresh_pairs.extend_from_slice(&um.fresh_dedup_pairs);
                all_stale_repairs.extend_from_slice(&um.stale_repairs);
            }
        }

        if batch_args.is_empty() {
            return;
        }

        let commit_start = Instant::now();
        let result = meta.atomic_batch_write_multi_with_dedup(&batch_args, &[]);
        Self::record_elapsed(&metrics.flush_writer_meta_commit_ns, commit_start);
        metrics
            .flush_writer_meta_commits
            .fetch_add(1, Ordering::Relaxed);
        metrics
            .flush_writer_meta_lbas
            .fetch_add(sub_batch_lbas, Ordering::Relaxed);

        match result {
            Ok(returned) => {
                for (pba, cleanup) in returned {
                    accum_old_meta
                        .entry(pba)
                        .and_modify(|entry| entry.merge(cleanup.clone()))
                        .or_insert(cleanup);
                }
                let candidate_start = Instant::now();
                candidate.insert_many(&all_fresh_pairs);
                Self::record_elapsed(
                    &metrics.flush_writer_meta_candidate_ns,
                    candidate_start,
                );
                let repair_start = Instant::now();
                Self::repair_stale_dedup_index(
                    meta,
                    metrics,
                    &all_stale_repairs,
                    "commit_worker_passthrough_chunk",
                );
                Self::record_elapsed(&metrics.flush_writer_meta_repair_ns, repair_start);
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
        done_tx: &Sender<Vec<u64>>,
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
                    let _ = done_tx.send(ucd.seqs);
                }
                Some(dc) => {
                    if let Some(original_seqs) = dc.decrement() {
                        let _ = done_tx.send(original_seqs);
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
    ) {
        let total_start = Instant::now();
        let PackedCommitJob {
            sealed,
            shard_idx,
            buffered_seqs,
            buffered_completions,
        } = job;

        // Lifecycle locks: union of every fragment's vol_id, sorted to
        // avoid deadlock with concurrent batches on overlapping volume
        // sets. Same as the historic write_packed_slot path.
        let mut vol_ids: Vec<String> = sealed
            .fragments
            .iter()
            .map(|f| f.unit.vol_id.clone())
            .collect();
        vol_ids.sort();
        vol_ids.dedup();
        let locks: Vec<_> = vol_ids.iter().map(|vid| lifecycle.get_lock(vid)).collect();
        let _guards: Vec<_> = locks.iter().map(|l| l.read().unwrap()).collect();

        let commit_ranges: Vec<(&str, Lba, u64)> = sealed
            .fragments
            .iter()
            .map(|frag| {
                (
                    frag.unit.vol_id.as_str(),
                    frag.unit.start_lba,
                    frag.unit.lba_count as u64,
                )
            })
            .collect();

        let outcome: OnyxResult<PackedCommitOutcome> =
            pool.with_l2p_commit_locks_for_ranges(commit_ranges, || {
                let build_start = Instant::now();
                let mut batch_values: Vec<(VolumeId, Lba, BlockmapValue)> = Vec::new();
                let mut fresh_dedup_pairs: Vec<(ContentHash, BlockmapValue)> = Vec::new();
                let mut stale_repairs: Vec<(ContentHash, DedupEntry, DedupEntry)> = Vec::new();
                let mut total_refcount: u32 = 0;
                let mut all_seq_lba_ranges: Vec<(u64, Lba, u32)> = Vec::new();
                let mut any_discarded = false;

                for frag in &sealed.fragments {
                    let unit = &frag.unit;
                    let vol_id = VolumeId(unit.vol_id.clone());
                    let should_discard = match meta.get_volume(&vol_id)? {
                        None => true,
                        Some(vc)
                            if unit.vol_created_at != 0 && vc.created_at != unit.vol_created_at =>
                        {
                            true
                        }
                        _ => false,
                    };
                    if should_discard {
                        metrics.flush_stale_discards.fetch_add(1, Ordering::Relaxed);
                        any_discarded = true;
                        for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                            let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
                        }
                        continue;
                    }
                    let live_positions = Self::live_positions_for_unit(unit, pool)?;
                    if live_positions.is_empty() {
                        any_discarded = true;
                        for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                            let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
                        }
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
                            pba: sealed.pba,
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
                    total_refcount += live_positions.len() as u32;
                    all_seq_lba_ranges.extend(unit.seq_lba_ranges.iter().cloned());
                }
                Self::record_elapsed(&metrics.flush_writer_meta_build_ns, build_start);

                if batch_values.is_empty() {
                    let _ = allocator.free_one(sealed.pba);
                    return Ok(PackedCommitOutcome::Discarded);
                }

                let meta_start = Instant::now();
                let actual_old_pba_meta = match meta.atomic_batch_write_packed_with_dedup(
                    &batch_values,
                    sealed.pba,
                    total_refcount,
                    &[],
                ) {
                    Ok(m) => m,
                    Err(e) => {
                        let _ = allocator.free_one(sealed.pba);
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
                Ok(PackedCommitOutcome::Committed {
                    actual_old_pba_meta,
                    fresh_dedup_pairs,
                    stale_repairs,
                    all_seq_lba_ranges,
                    any_discarded,
                    total_refcount,
                })
            });

        match outcome {
            Ok(PackedCommitOutcome::Discarded) => {
                Self::deliver_packed_done(buffered_seqs, buffered_completions, done_tx);
                let _ = pool.advance_tail_for_shard(shard_idx);
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            }
            Ok(PackedCommitOutcome::Committed {
                actual_old_pba_meta,
                fresh_dedup_pairs,
                stale_repairs,
                all_seq_lba_ranges,
                any_discarded,
                total_refcount,
            }) => {
                candidate.insert_many(&fresh_dedup_pairs);
                Self::repair_stale_dedup_index(
                    meta,
                    metrics,
                    &stale_repairs,
                    "commit_worker_packed",
                );
                if !actual_old_pba_meta.is_empty() {
                    let _ = cleanup_tx.send(actual_old_pba_meta.into_values().collect());
                }
                metrics
                    .flush_packed_slots_written
                    .fetch_add(1, Ordering::Relaxed);
                metrics
                    .flush_packed_fragments_written
                    .fetch_add(sealed.fragments.len() as u64, Ordering::Relaxed);
                metrics
                    .flush_packed_bytes
                    .fetch_add(sealed.data.len() as u64, Ordering::Relaxed);
                let mark_start = Instant::now();
                for (seq, lba_start, lba_count) in &all_seq_lba_ranges {
                    let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
                }
                Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);
                Self::deliver_packed_done(buffered_seqs, buffered_completions, done_tx);
                let _ = pool.advance_tail_for_shard(shard_idx);
                tracing::debug!(
                    pba = sealed.pba.0,
                    fragments = sealed.fragments.len(),
                    total_lbas = total_refcount,
                    discarded = any_discarded,
                    "commit_worker: flushed packed slot"
                );
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            }
            Err(e) => {
                metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                tracing::error!(
                    pba = sealed.pba.0,
                    error = %e,
                    "commit_worker: packed slot commit failed; defer_retry buffered seqs"
                );
                if !buffered_seqs.is_empty() {
                    in_flight_tracker.defer_retry(&buffered_seqs, Self::RETRY_BACKOFF);
                }
                for dc in &buffered_completions {
                    in_flight_tracker.defer_retry(dc.seqs(), Self::RETRY_BACKOFF);
                }
                Self::deliver_packed_done(buffered_seqs, buffered_completions, done_tx);
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
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
    Discarded,
    Committed {
        actual_old_pba_meta: HashMap<Pba, RemapCleanup>,
        fresh_dedup_pairs: Vec<(ContentHash, BlockmapValue)>,
        stale_repairs: Vec<(ContentHash, DedupEntry, DedupEntry)>,
        all_seq_lba_ranges: Vec<(u64, Lba, u32)>,
        any_discarded: bool,
        total_refcount: u32,
    },
}
