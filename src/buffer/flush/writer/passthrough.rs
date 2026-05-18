use super::*;

impl BufferFlusher {
    pub(in crate::buffer::flush) fn write_unit(
        shard_idx: usize,
        unit: &CompressedUnit,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        metrics: &EngineMetrics,
        cleanup_tx: &Sender<CleanupBatch>,
        candidate: &crate::dedup::CandidateCache,
    ) -> OnyxResult<()> {
        lifecycle.with_read_lock(&unit.vol_id, || {
            let total_start = Instant::now();
            // Hold the lifecycle read lock from generation validation through
            // metadata commit so delete/create cannot interleave with this flush.
            let vol_id = VolumeId(unit.vol_id.clone());
            let should_discard = match meta.get_volume(&vol_id)? {
                None => true,
                Some(vc) if vc.created_at != unit.vol_created_at => {
                    tracing::debug!(
                        vol = unit.vol_id,
                        entry_gen = unit.vol_created_at,
                        current_gen = vc.created_at,
                        "write_unit: generation mismatch, discarding stale unit"
                    );
                    true
                }
                _ => false,
            };
            if should_discard {
                metrics.flush_stale_discards.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    vol = unit.vol_id,
                    "write_unit: discarding unit (volume deleted or generation mismatch)"
                );
                for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                    let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
                }
                let _ = pool.advance_tail_for_shard(shard_idx);
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                return Ok(());
            }

            let bs = BLOCK_SIZE as usize;
            let alloc_start = Instant::now();
            let blocks_needed = (unit.compressed_data.len() + bs - 1) / bs;

            let allocation = if blocks_needed == 1 {
                Allocation::Single(allocator.allocate_one_for_lane(shard_idx)?)
            } else {
                let extent = allocator.allocate_extent_for_lane(shard_idx, blocks_needed as u32)?;
                if (extent.count as usize) < blocks_needed {
                    allocator.free_extent(extent)?;
                    Self::record_elapsed(&metrics.flush_writer_alloc_ns, alloc_start);
                    Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                    return Err(crate::error::OnyxError::SpaceExhausted);
                }
                Allocation::Extent(extent)
            };
            Self::record_elapsed(&metrics.flush_writer_alloc_ns, alloc_start);
            let pba = allocation.start_pba();

            let io_start = Instant::now();
            if let Err(e) = maybe_inject_test_failure(
                &unit.vol_id,
                unit.start_lba,
                FlushFailStage::BeforeIoWrite,
            ) {
                allocation.free(allocator)?;
                Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                return Err(e);
            }

            allocator.wait_for_readers(pba, blocks_needed as u32);

            if let Err(e) = io_engine.write_blocks(pba, &unit.compressed_data) {
                allocation.free(allocator)?;
                Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                return Err(e);
            }
            Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);

            let meta_start = Instant::now();

            // Same-LBA concurrent commits are arbitrated by metadb's
            // per-LBA seq_guard CAS; no onyx-side stripe lock here.
            let commit = (|| -> OnyxResult<Option<(Vec<usize>, HashMap<Pba, RemapCleanup>)>> {
                let live_positions = Self::live_positions_for_unit(unit, pool)?;
                if live_positions.is_empty() {
                    return Ok(None);
                }
                let lbas: Vec<Lba> = live_positions
                    .iter()
                    .map(|idx| Lba(unit.start_lba.0 + *idx as u64))
                    .collect();

                let mut batch_values = Vec::with_capacity(live_positions.len());
                let mut batch_seqs = Vec::with_capacity(live_positions.len());
                for i in 0..live_positions.len() {
                    let flags = if unit.dedup_skipped {
                        FLAG_DEDUP_SKIPPED
                    } else {
                        0
                    };
                    let blockmap =
                        Self::blockmap_for_unit_position(unit, pba, live_positions[i], 0, flags);
                    batch_values.push((lbas[i], blockmap));
                    batch_seqs.push(Self::latest_seq_for_lba(&unit.seq_lba_ranges, lbas[i]));
                }
                let mut fresh_dedup_pairs: Vec<(ContentHash, BlockmapValue)> = Vec::new();
                let mut stale_repairs: Vec<(ContentHash, DedupEntry, DedupEntry)> = Vec::new();
                if !unit.dedup_skipped {
                    if let Some(ref hashes) = unit.block_hashes {
                        fresh_dedup_pairs.reserve(live_positions.len());
                        for &pos in &live_positions {
                            let hash = hashes[pos];
                            if hash == [0u8; 8] {
                                continue;
                            }
                            let blockmap = Self::blockmap_for_unit_position(unit, pba, pos, 0, 0);
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

                maybe_inject_test_failure(
                    &unit.vol_id,
                    unit.start_lba,
                    FlushFailStage::BeforeMetaWrite,
                )?;

                let (actual_old_pba_meta, accepted) = meta.atomic_batch_write_with_dedup(
                    &vol_id,
                    &batch_values,
                    live_positions.len() as u32,
                    &[],
                    &batch_seqs,
                )?;
                // metadb seq_guard may reject some L2pRemaps. If every
                // remap in this unit was rejected, refcount[pba] is 0
                // and the freshly-allocated PBA is orphaned — surface
                // that as `Ok(None)` so the outer free path runs.
                if !accepted.iter().any(|a| *a) {
                    let rejects = accepted.len() as u64;
                    metrics
                        .flush_seq_rejects
                        .fetch_add(rejects, Ordering::Relaxed);
                    return Ok(None);
                }
                let rejects = accepted.iter().filter(|a| !**a).count() as u64;
                if rejects > 0 {
                    metrics
                        .flush_seq_rejects
                        .fetch_add(rejects, Ordering::Relaxed);
                }
                candidate.insert_many(&fresh_dedup_pairs);
                Self::repair_stale_dedup_index(meta, metrics, &stale_repairs, "write_unit");
                Ok(Some((live_positions, actual_old_pba_meta)))
            })();
            let Some((live_positions, actual_old_pba_meta)) = (match commit {
                Ok(v) => v,
                Err(e) => {
                    allocation.free(allocator)?;
                    Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
                    Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                    return Err(e);
                }
            }) else {
                allocation.free(allocator)?;
                let mark_start = Instant::now();
                for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                    if let Err(e) = pool.mark_flushed(*seq, *lba_start, *lba_count) {
                        tracing::warn!(seq, error = %e, "failed to mark stale entry flushed");
                    }
                }
                Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);
                let _ = pool.advance_tail_for_shard(shard_idx);
                Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                return Ok(());
            };
            Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
            Self::free_unreferenced_raw_blocks(unit, pba, &live_positions, allocator, "write_unit");

            if !actual_old_pba_meta.is_empty() {
                let _ = cleanup_tx.send(actual_old_pba_meta.into_values().collect());
            }

            metrics.flush_units_written.fetch_add(1, Ordering::Relaxed);
            metrics
                .flush_unit_bytes
                .fetch_add(unit.compressed_data.len() as u64, Ordering::Relaxed);

            let mark_start = Instant::now();
            for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                if let Err(e) = pool.mark_flushed(*seq, *lba_start, *lba_count) {
                    tracing::warn!(seq, error = %e, "failed to mark entry flushed");
                }
            }
            Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);

            tracing::debug!(
                vol = unit.vol_id,
                start_lba = unit.start_lba.0,
                lba_count = unit.lba_count,
                pba = pba.0,
                compressed = unit.compressed_data.len(),
                original = unit.original_size,
                "flushed compression unit"
            );

            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            Ok(())
        })
    }

    /// Batch a passthrough cycle and hand it off to the per-volume
    /// commit workers. Phase 1 of the per-volume commit architecture:
    /// the shard writer does alloc + LV3 IO synchronously (so PBA
    /// reservation and device queueing stay shard-local), then groups
    /// surviving units by volume and pushes one
    /// `PassthroughCommitJob` per volume into `commit_worker_txs[hash %
    /// N]`. The metadb commit, `actual_old_pba_meta` cleanup,
    /// `mark_flushed`, and `done_tx` all happen on the commit worker.
    ///
    /// IO failures are handled inline (free PBA, defer_retry, send
    /// `done_tx`) so the shard writer's retry path stays simple.
    pub(in crate::buffer::flush) fn write_units_batch(
        shard_idx: usize,
        units: Vec<CompressedUnit>,
        seqs_per_unit: Vec<Vec<u64>>,
        completions_per_unit: Vec<Option<Arc<crate::buffer::pipeline::DedupCompletion>>>,
        _pool: &WriteBufferPool,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        metrics: &EngineMetrics,
        in_flight_tracker: &FlusherInFlightTracker,
        done_tx: &Sender<Vec<u64>>,
        commit_worker_txs: &[Sender<CommitJob>],
        commit_workers_per_volume: usize,
    ) {
        if units.is_empty() {
            return;
        }
        let total_start = Instant::now();
        let n = units.len();
        debug_assert_eq!(seqs_per_unit.len(), n);
        debug_assert_eq!(completions_per_unit.len(), n);

        // No lifecycle / gen check here. The commit worker re-takes
        // the lifecycle read lock and runs the gen check under it
        // before publishing any blockmap.

        // Per-unit IO state. `failed[i]` tagges units we cannot push
        // to the commit worker; `pbas[i]` and `alloc_blocks[i]`
        // record what was reserved so we can free on failure or hand
        // ownership to the worker on success.
        let mut failed: Vec<bool> = vec![false; n];
        let mut pbas: Vec<Option<Pba>> = vec![None; n];
        let mut alloc_blocks: Vec<u32> = vec![0; n];
        let mut io_ops_count = 0u64;

        // Phase A: alloc PBAs.
        let alloc_start = Instant::now();
        for (i, unit) in units.iter().enumerate() {
            let bs = BLOCK_SIZE as usize;
            let blocks_needed = (unit.compressed_data.len() + bs - 1) / bs;
            alloc_blocks[i] = blocks_needed as u32;
            let allocation = if blocks_needed == 1 {
                allocator
                    .allocate_one_for_lane(shard_idx)
                    .map(|pba| (pba, 1u32))
            } else {
                allocator
                    .allocate_extent_for_lane(shard_idx, blocks_needed as u32)
                    .and_then(|ext| {
                        if (ext.count as usize) < blocks_needed {
                            allocator.free_extent(ext)?;
                            Err(crate::error::OnyxError::SpaceExhausted)
                        } else {
                            Ok((ext.start, ext.count))
                        }
                    })
            };
            match allocation {
                Ok((pba, _)) => pbas[i] = Some(pba),
                Err(e) => {
                    tracing::error!(
                        vol = %unit.vol_id,
                        start_lba = unit.start_lba.0,
                        error = %e,
                        "writer: passthrough alloc failed"
                    );
                    failed[i] = true;
                }
            }
        }
        let alloc_elapsed = alloc_start.elapsed();
        Self::record_elapsed(&metrics.flush_writer_alloc_ns, alloc_start);

        // Phase B: batch IO writes — one submit per batch.
        // io_uring backend: 1 io_uring_enter + 1 wait_for_completions(N).
        // Syscall backend: scoped threads inside submit_batch keep NVMe QD > 1.
        let io_start = Instant::now();
        {
            use crate::io::engine::{LvOp, LvOpResult};
            let mut ops: Vec<LvOp> = Vec::with_capacity(n);
            let mut op_to_unit: Vec<usize> = Vec::with_capacity(n);
            for i in 0..n {
                if failed[i] {
                    continue;
                }
                if let Err(e) = maybe_inject_test_failure(
                    &units[i].vol_id,
                    units[i].start_lba,
                    FlushFailStage::BeforeIoWrite,
                ) {
                    let pba = pbas[i].unwrap();
                    let blk = alloc_blocks[i];
                    if blk == 1 {
                        let _ = allocator.free_one(pba);
                    } else {
                        let _ = allocator.free_extent(Extent::new(pba, blk));
                    }
                    failed[i] = true;
                    tracing::error!(
                        vol = units[i].vol_id,
                        start_lba = units[i].start_lba.0,
                        error = %e,
                        "writer: passthrough injected IO write failure"
                    );
                    continue;
                }
                allocator.wait_for_readers(pbas[i].unwrap(), alloc_blocks[i]);
                ops.push(LvOp::Write {
                    pba: pbas[i].unwrap(),
                    payload: units[i].compressed_data.as_slice(),
                });
                op_to_unit.push(i);
            }
            if !ops.is_empty() {
                io_ops_count = ops.len() as u64;
                match io_engine.submit_batch(ops, false) {
                    Ok(write_results) => {
                        for (idx, r) in write_results.into_iter().enumerate() {
                            let unit_idx = op_to_unit[idx];
                            if let LvOpResult::Write(Err(e)) = r {
                                let pba = pbas[unit_idx].unwrap();
                                let blk = alloc_blocks[unit_idx];
                                if blk == 1 {
                                    let _ = allocator.free_one(pba);
                                } else {
                                    let _ = allocator.free_extent(Extent::new(pba, blk));
                                }
                                failed[unit_idx] = true;
                                tracing::error!(
                                    vol = units[unit_idx].vol_id,
                                    start_lba = units[unit_idx].start_lba.0,
                                    error = %e,
                                    "writer: passthrough IO write failed"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        for &unit_idx in &op_to_unit {
                            if failed[unit_idx] {
                                continue;
                            }
                            let pba = pbas[unit_idx].unwrap();
                            let blk = alloc_blocks[unit_idx];
                            if blk == 1 {
                                let _ = allocator.free_one(pba);
                            } else {
                                let _ = allocator.free_extent(Extent::new(pba, blk));
                            }
                            failed[unit_idx] = true;
                        }
                        tracing::error!(error = %e, "writer: passthrough IO batch submit failed");
                    }
                }
            }
        }
        let io_elapsed = io_start.elapsed();
        Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);

        // Counters for the surviving (committable) units. The actual
        // commit happens asynchronously on the commit worker, but the
        // shard writer's accounting matches the historic shape so
        // dashboards stay continuous.
        let surviving = failed.iter().filter(|f| !**f).count();
        let surviving_lbas: u64 = units
            .iter()
            .enumerate()
            .filter(|(i, _)| !failed[*i])
            .map(|(_, u)| u.lba_count as u64)
            .sum();
        metrics
            .flush_writer_pt_batches
            .fetch_add(1, Ordering::Relaxed);
        metrics
            .flush_writer_pt_units
            .fetch_add(surviving as u64, Ordering::Relaxed);
        metrics
            .flush_writer_pt_lbas
            .fetch_add(surviving_lbas, Ordering::Relaxed);
        metrics
            .flush_writer_pt_io_ops
            .fetch_add(io_ops_count, Ordering::Relaxed);
        crate::metrics::record_counter_max(&metrics.flush_writer_pt_units_max, surviving as u64);
        crate::metrics::record_counter_max(&metrics.flush_writer_pt_lbas_max, surviving_lbas);
        crate::metrics::record_counter_max(&metrics.flush_writer_pt_io_ops_max, io_ops_count);

        // Phase C: split surviving units by `vol_id` and dispatch a
        // PassthroughCommitJob to each volume's commit worker. Failed
        // units (alloc / IO) are handled inline below.
        let mut units_iter = units.into_iter();
        let mut seqs_iter = seqs_per_unit.into_iter();
        let mut completions_iter = completions_per_unit.into_iter();

        let mut per_volume: HashMap<String, Vec<UnitCommitData>> = HashMap::new();
        let mut failed_paylads: Vec<(
            Vec<u64>,
            Option<Arc<crate::buffer::pipeline::DedupCompletion>>,
        )> = Vec::new();
        for i in 0..n {
            let unit = units_iter.next().expect("units length matches n");
            let seqs = seqs_iter.next().expect("seqs length matches n");
            let completion = completions_iter
                .next()
                .expect("completions length matches n");
            if failed[i] {
                failed_paylads.push((seqs, completion));
                continue;
            }
            let pba = pbas[i].expect("pba present for non-failed unit");
            let blocks = alloc_blocks[i];
            let vol = unit.vol_id.clone();
            per_volume.entry(vol).or_default().push(UnitCommitData {
                shard_idx,
                unit,
                pba,
                alloc_blocks: blocks,
                seqs,
                completion,
            });
        }

        // IO failures: defer_retry + done_tx inline so the shard
        // writer's coalesce loop can pick them up after the backoff.
        for (seqs, completion) in failed_paylads {
            metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
            match &completion {
                None => in_flight_tracker.defer_retry(&seqs, Self::RETRY_BACKOFF),
                Some(dc) => in_flight_tracker.defer_retry(dc.seqs(), Self::RETRY_BACKOFF),
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

        // Dispatch one job per volume to its routed commit worker.
        // The `Sender::send` is bounded; if the worker queue is full
        // the shard writer will block briefly — that is the only
        // backpressure path in Phase 1 (per-volume admission lands in
        // Phase 2).
        if !commit_worker_txs.is_empty() {
            for (vol, units_for_vol) in per_volume {
                let worker_idx = route_volume_to_worker(&vol, shard_idx, commit_workers_per_volume);
                let tx_idx = worker_idx % commit_worker_txs.len();
                let job = CommitJob::Passthrough(PassthroughCommitJob {
                    vol_id: VolumeId(vol),
                    units: units_for_vol,
                    enqueued_at: Instant::now(),
                });
                let send_start = Instant::now();
                let _ = commit_worker_txs[tx_idx].send(job);
                Self::record_elapsed(&metrics.flush_writer_commit_send_ns, send_start);
                metrics
                    .flush_writer_commit_send_ops
                    .fetch_add(1, Ordering::Relaxed);
                crate::metrics::record_counter_max(
                    &metrics.flush_writer_commit_send_len_max,
                    commit_worker_txs[tx_idx].len() as u64,
                );
            }
        }

        let total_elapsed = total_start.elapsed();
        Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
        if total_elapsed >= Duration::from_secs(1) {
            tracing::debug!(
                shard = shard_idx,
                units = n,
                surviving,
                total_ms = total_elapsed.as_millis() as u64,
                alloc_ms = alloc_elapsed.as_millis() as u64,
                io_ms = io_elapsed.as_millis() as u64,
                "writer: slow passthrough batch (>=1s) — IO + dispatch only"
            );
        }
    }
}
