use super::*;

/// Per-unit decisions consumed by chunked commit below. Module-scope
/// so `commit_passthrough_chunk` can take a `&mut [Option<UnitMeta>]`.
struct UnitMeta {
    start_lba: Lba,
    batch_values: Vec<(Lba, BlockmapValue)>,
    seqs: Vec<u64>,
    live_positions: Vec<usize>,
    /// Subset of `live_positions` that survived metadb's per-LBA
    /// seq_guard. Defaults to a clone of `live_positions`; the
    /// chunk that returns from the tx with a partial-reject
    /// `Vec<bool>` shrinks this so the post-commit pass releases
    /// the per-LBA PBAs of full-raw extent positions whose remap
    /// was refused.
    accepted_positions: Vec<usize>,
    fresh_dedup_pairs: Vec<(ContentHash, BlockmapValue)>,
    stale_repairs: Vec<(ContentHash, DedupEntry, DedupEntry)>,
}

struct PassthroughChunkOutcome {
    old_pba_meta: HashMap<Pba, RemapCleanup>,
}

impl BufferFlusher {
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

        let n = units.len();
        let mut unit_metas: Vec<Option<UnitMeta>> = (0..n).map(|_| None).collect();
        let mut discarded: Vec<bool> = vec![false; n];
        let mut commit_failed_indices: Vec<usize> = Vec::new();
        // Units that committed without an Err but had every L2pRemap
        // rejected by metadb's seq_guard. Their PBA was incref'd zero
        // times in the tx, so we must free it back to the allocator
        // in the post-commit pass — same shape as `discarded` units.
        let mut seq_rejected_indices: Vec<usize> = Vec::new();
        // Mutation accumulators owned outside the closure. Same-LBA
        // concurrent commits are arbitrated by metadb's per-LBA
        // seq_guard CAS, so we no longer take any onyx-side stripe
        // lock here.
        let mut accum_candidate_pairs: Vec<(ContentHash, BlockmapValue)> = Vec::new();
        let mut accum_stale_repairs: Vec<(ContentHash, DedupEntry, DedupEntry)> = Vec::new();
        let actual_old_pba_meta = {
            let build_start = Instant::now();
            for (i, ucd) in units.iter().enumerate() {
                let unit = &ucd.unit;
                let should_discard = !volume_present || cur_created_at != Some(unit.vol_created_at);
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
                        start_lba: unit.start_lba,
                        batch_values: Vec::new(),
                        seqs: Vec::new(),
                        live_positions,
                        accepted_positions: Vec::new(),
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
                let split_full_raw_unit = unit.compression_bypassed
                    || (Self::is_full_raw_unit(unit)
                        && live_positions.len() < unit.lba_count as usize);
                for j in 0..live_positions.len() {
                    let blockmap = Self::blockmap_for_unit_position_with_raw_split(
                        unit,
                        ucd.pba,
                        live_positions[j],
                        0,
                        flags,
                        split_full_raw_unit,
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
                            let blockmap = Self::blockmap_for_unit_position_with_raw_split(
                                unit,
                                ucd.pba,
                                pos,
                                0,
                                0,
                                split_full_raw_unit,
                            );
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
                let accepted_positions = live_positions.clone();
                unit_metas[i] = Some(UnitMeta {
                    start_lba: ucd.unit.start_lba,
                    batch_values,
                    seqs,
                    live_positions,
                    accepted_positions,
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
                if !chunk.is_empty() && chunk_lbas.saturating_add(lbas) > target_lbas_per_tx {
                    Self::commit_passthrough_chunk(
                        &chunk,
                        &vol_id,
                        &mut unit_metas,
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
                        &mut unit_metas,
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
                    &mut unit_metas,
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
        };

        // Keep mark_flushed ordered before done_tx so the coalescer
        // cannot re-enqueue the same pending seq while its buffer
        // index entry is still visible.
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
            // Successful commit path. Use `accepted_positions` so
            // partial seq_guard rejects on full-raw extents release
            // the per-LBA PBAs of refused positions instead of
            // leaking them.
            Self::free_unreferenced_raw_blocks(
                &ucd.unit,
                ucd.pba,
                &um.accepted_positions,
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
        unit_metas: &mut [Option<UnitMeta>],
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

        for &i in chunk {
            if let Some(um) = unit_metas[i].as_ref() {
                if um.batch_values.is_empty() {
                    continue;
                }
                if let Err(e) = maybe_inject_test_failure(
                    &vol_id.0,
                    um.start_lba,
                    FlushFailStage::BeforeMetaWrite,
                ) {
                    tracing::error!(
                        vol = %vol_id.0,
                        start_lba = um.start_lba.0,
                        chunk_units = chunk.len(),
                        error = %e,
                        "commit_worker: passthrough injected metadata failure"
                    );
                    for &j in chunk {
                        if let Some(failed_um) = unit_metas[j].as_ref() {
                            if !failed_um.batch_values.is_empty() {
                                commit_failed_indices.push(j);
                            }
                        }
                    }
                    return;
                }
            }
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
                    metrics
                        .flush_seq_rejects
                        .fetch_add(rejects, Ordering::Relaxed);
                    // Two cases per unit:
                    // - every L2pRemap rejected → mark whole-unit rejected;
                    //   refcount[pba] is still 0 and the freshly-allocated
                    //   PBA gets freed in the post-commit pass.
                    // - partial reject (some accepted, some not) → shrink
                    //   `accepted_positions` so `free_unreferenced_raw_blocks`
                    //   reclaims the per-LBA PBAs of rejected positions in
                    //   full-raw extents. Packed-slot fragments share one
                    //   PBA so the shrink is a no-op there (handled
                    //   separately in `commit_packed_jobs_batch`).
                    let mut offset = 0;
                    for (unit_idx, span) in &unit_spans {
                        let unit_accepted = &accepted[offset..offset + span];
                        let any_accepted = unit_accepted.iter().any(|a| *a);
                        let any_rejected = unit_accepted.iter().any(|a| !*a);
                        if !any_accepted {
                            seq_rejected_indices.push(*unit_idx);
                        } else if any_rejected {
                            if let Some(um) = unit_metas[*unit_idx].as_mut() {
                                let kept: Vec<usize> = um
                                    .live_positions
                                    .iter()
                                    .copied()
                                    .enumerate()
                                    .filter_map(|(k, pos)| {
                                        unit_accepted.get(k).copied().unwrap_or(true).then_some(pos)
                                    })
                                    .collect();
                                um.accepted_positions = kept;
                            }
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
}
