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

            let commit = pool.with_l2p_commit_locks_for_ranges(
                std::iter::once((unit.vol_id.as_str(), unit.start_lba, unit.lba_count as u64)),
                || -> OnyxResult<Option<(Vec<usize>, HashMap<Pba, RemapCleanup>)>> {
                    let live_positions = Self::live_positions_for_unit(unit, pool)?;
                    if live_positions.is_empty() {
                        return Ok(None);
                    }
                    let lbas: Vec<Lba> = live_positions
                        .iter()
                        .map(|idx| Lba(unit.start_lba.0 + *idx as u64))
                        .collect();

                    let mut batch_values = Vec::with_capacity(live_positions.len());
                    for i in 0..live_positions.len() {
                        let flags = if unit.dedup_skipped {
                            FLAG_DEDUP_SKIPPED
                        } else {
                            0
                        };
                        let blockmap = Self::blockmap_for_unit_position(
                            unit,
                            pba,
                            live_positions[i],
                            0,
                            flags,
                        );
                        batch_values.push((lbas[i], blockmap));
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
                                let blockmap =
                                    Self::blockmap_for_unit_position(unit, pba, pos, 0, 0);
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

                    let actual_old_pba_meta = meta.atomic_batch_write_with_dedup(
                        &vol_id,
                        &batch_values,
                        live_positions.len() as u32,
                        &[],
                    )?;
                    candidate.insert_many(&fresh_dedup_pairs);
                    Self::repair_stale_dedup_index(meta, metrics, &stale_repairs, "write_unit");
                    Ok(Some((live_positions, actual_old_pba_meta)))
                },
            );
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

    /// Batch a passthrough cycle and hand IO off to the per-shard
    /// io_submitter, which forwards surviving CommitJobs to the
    /// per-volume commit workers. Phase 4: the shard writer no longer
    /// blocks on `io_engine.submit_batch`; that step runs on a
    /// dedicated submitter thread so the writer cycle drops from
    /// ~5 ms (alloc + IO + dispatch) to ~1 ms (alloc + dispatch).
    ///
    /// Alloc failures are handled inline (free PBA, defer_retry, send
    /// `done_tx`); IO failures are handled on the io_submitter
    /// thread (same semantics, just on a different thread).
    pub(in crate::buffer::flush) fn write_units_batch(
        shard_idx: usize,
        units: Vec<CompressedUnit>,
        seqs_per_unit: Vec<Vec<u64>>,
        completions_per_unit: Vec<Option<Arc<crate::buffer::pipeline::DedupCompletion>>>,
        _pool: &WriteBufferPool,
        allocator: &SpaceAllocator,
        metrics: &EngineMetrics,
        in_flight_tracker: &FlusherInFlightTracker,
        done_tx: &Sender<Vec<u64>>,
        io_submitter_tx: &Sender<IoSubmitJob>,
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

        // Per-unit alloc state.
        let mut failed: Vec<bool> = vec![false; n];
        let mut pbas: Vec<Option<Pba>> = vec![None; n];
        let mut alloc_blocks: Vec<u32> = vec![0; n];

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

        // Counters for surviving (alloc-OK) units. IO success is
        // counted on the io_submitter side once it actually drives
        // the submit_batch.
        let surviving = failed.iter().filter(|f| !**f).count();
        let surviving_lbas: u64 = units
            .iter()
            .enumerate()
            .filter(|(i, _)| !failed[*i])
            .map(|(_, u)| u.lba_count as u64)
            .sum();
        metrics.flush_writer_pt_batches.fetch_add(1, Ordering::Relaxed);
        metrics
            .flush_writer_pt_units
            .fetch_add(surviving as u64, Ordering::Relaxed);
        metrics
            .flush_writer_pt_lbas
            .fetch_add(surviving_lbas, Ordering::Relaxed);
        crate::metrics::record_counter_max(
            &metrics.flush_writer_pt_units_max,
            surviving as u64,
        );
        crate::metrics::record_counter_max(&metrics.flush_writer_pt_lbas_max, surviving_lbas);

        // Phase B: split surviving units by `vol_id` and dispatch
        // IoSubmitJobs to the per-shard io_submitter. Each job
        // remembers its target commit worker (vol_id hash) so the
        // io_submitter can forward after the IO completes.
        let mut units_iter = units.into_iter();
        let mut seqs_iter = seqs_per_unit.into_iter();
        let mut completions_iter = completions_per_unit.into_iter();

        let mut per_volume: HashMap<String, Vec<UnitCommitData>> = HashMap::new();
        let mut failed_paylads: Vec<(Vec<u64>, Option<Arc<crate::buffer::pipeline::DedupCompletion>>)> =
            Vec::new();
        for i in 0..n {
            let unit = units_iter.next().expect("units length matches n");
            let seqs = seqs_iter.next().expect("seqs length matches n");
            let completion = completions_iter.next().expect("completions length matches n");
            if failed[i] {
                failed_paylads.push((seqs, completion));
                continue;
            }
            let pba = pbas[i].expect("pba present for non-failed unit");
            let blocks = alloc_blocks[i];
            let vol = unit.vol_id.clone();
            per_volume.entry(vol).or_default().push(UnitCommitData {
                unit,
                pba,
                alloc_blocks: blocks,
                seqs,
                completion,
            });
        }

        // Alloc failures: defer_retry + done_tx inline so the shard
        // writer's coalesce loop can pick them up after the backoff.
        // IO failures are handled by the io_submitter using the same
        // pattern.
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

        // Dispatch one IoSubmitJob per volume to the per-shard
        // io_submitter. Send is bounded; the writer briefly blocks
        // if the submitter queue is saturated — that is the new
        // back-pressure path in Phase 4.
        for (vol, units_for_vol) in per_volume {
            let target_worker_idx = route_volume_to_worker(&vol);
            let job = CommitJob::Passthrough(PassthroughCommitJob {
                vol_id: VolumeId(vol),
                shard_idx,
                units: units_for_vol,
            });
            let _ = io_submitter_tx.send(IoSubmitJob {
                job,
                target_worker_idx,
            });
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
                "writer: slow passthrough batch (>=1s) — alloc + dispatch only"
            );
        }
    }
}
