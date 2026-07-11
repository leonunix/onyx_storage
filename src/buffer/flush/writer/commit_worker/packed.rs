use super::*;

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

impl BufferFlusher {
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

        // Same-LBA concurrent commits are arbitrated by metadb's
        // per-LBA seq_guard CAS; no onyx-side stripe lock here.
        let jobs = jobs;
        let outcome: OnyxResult<PackedCommitOutcome> = loop {
            let commit_attempt: OnyxResult<PackedCommitOutcome> = (|| {
                let build_start = Instant::now();
                let mut batch_values: Vec<(VolumeId, Lba, BlockmapValue)> = Vec::new();
                let mut batch_seqs: Vec<u64> = Vec::new();
                // Candidate / repair pairs carry the index of their fragment's
                // entry in `batch_values` so they can be dropped if that
                // fragment's L2pRemap is seq_guard-rejected. Inserting a
                // rejected fragment's (hash → pba) into the candidate cache /
                // dedup_index is the premature-free CRC class: an all-rejected
                // slot's PBA is freed back to the allocator below, but its LV3
                // bytes still match the hash, so a later verify byte-compare
                // passes and promotes a live mapping onto a free-listed PBA.
                let mut fresh_dedup_pairs: Vec<(usize, ContentHash, BlockmapValue)> = Vec::new();
                let mut stale_repairs: Vec<(usize, ContentHash, DedupEntry, DedupEntry)> =
                    Vec::new();
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
                            Some(vc) if vc.created_at != unit.vol_created_at => true,
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
                            batch_seqs
                                .push(Self::latest_seq_for_lba(&unit.seq_lba_ranges, frag_lbas[i]));
                            if let Some(hashes) = hashes_for_promote {
                                let hash = hashes[pos];
                                if hash != [0u8; 8] {
                                    let batch_idx = batch_values.len() - 1;
                                    fresh_dedup_pairs.push((
                                        batch_idx,
                                        hash,
                                        BlockmapValue {
                                            flags: 0,
                                            ..blockmap
                                        },
                                    ));
                                    if let Some(repairs) = &unit.dedup_stale_repairs {
                                        if let Some(Some(old_entry)) = repairs.get(pos) {
                                            stale_repairs.push((
                                                batch_idx,
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

                if batch_values.is_empty() {
                    for pba in &discarded_pbas {
                        let _ =
                            crate::space::pba_lifecycle::rollback_uncommitted_one(allocator, *pba);
                    }
                    return Ok(PackedCommitOutcome::Discarded { all_seq_lba_ranges });
                }

                // Test-only failpoints — fire here, after the shard
                // writer's LV3 IO has landed but before the metadb tx
                // commits. Mirrors the historic write order in the
                // retired inline `write_packed_slot` path so the
                // integration tests can still observe pre-commit
                // states (pending entries, lifecycle locks held).
                for job in &jobs {
                    maybe_inject_test_failure_packed(
                        &job.sealed.fragments,
                        FlushFailStage::BeforeMetaWrite,
                    )?;
                }
                for job in &jobs {
                    maybe_pause_before_packed_meta_write(&job.sealed.fragments)?;
                }

                for pba in &discarded_pbas {
                    let _ = crate::space::pba_lifecycle::rollback_uncommitted_one(allocator, *pba);
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
                            // A rejected fragment's mapping was never published,
                            // so neither its candidate-cache pair nor its index
                            // repair may survive (see the declaration comment).
                            fresh_dedup_pairs
                                .retain(|(idx, _, _)| accepted.get(*idx).copied().unwrap_or(false));
                            stale_repairs.retain(|(idx, _, _, _)| {
                                accepted.get(*idx).copied().unwrap_or(false)
                            });
                            // Slot PBAs whose every L2pRemap was rejected are
                            // unreferenced (refcount never incremented) but
                            // their payload is on LV3 — retire instead of
                            // direct-free (see `retire_rejected_extent`).
                            let mut offset = 0;
                            for (pba, span) in &job_spans {
                                let slot_accepted = &accepted[offset..offset + span];
                                if !slot_accepted.iter().any(|a| *a) {
                                    Self::retire_rejected_extent(cleanup_tx, *pba, 1);
                                }
                                offset += span;
                            }
                        }
                        m
                    }
                    Err(e) => {
                        for pba in &live_pbas {
                            let _ = crate::space::pba_lifecycle::rollback_uncommitted_one(
                                allocator, *pba,
                            );
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
                    fresh_dedup_pairs: fresh_dedup_pairs
                        .into_iter()
                        .map(|(_, h, v)| (h, v))
                        .collect(),
                    stale_repairs: stale_repairs
                        .into_iter()
                        .map(|(_, h, old, new)| (h, old, new))
                        .collect(),
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
            })();
            break commit_attempt;
        };

        match outcome {
            Ok(PackedCommitOutcome::Discarded { all_seq_lba_ranges }) => {
                let mark_start = Instant::now();
                for (seq, lba_start, lba_count) in &all_seq_lba_ranges {
                    if let Err(e) = pool.mark_applied(*seq, *lba_start, *lba_count) {
                        tracing::warn!(
                            seq,
                            error = %e,
                            "commit_worker: failed to mark discarded packed entry applied"
                        );
                    }
                }
                Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);
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
                    if let Err(e) = pool.mark_applied(*seq, *lba_start, *lba_count) {
                        tracing::warn!(
                            seq,
                            error = %e,
                            "commit_worker: failed to mark packed entry applied"
                        );
                    }
                }
                Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);
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
                    let _ = crate::space::pba_lifecycle::rollback_uncommitted_one(
                        allocator,
                        job.sealed.pba,
                    );
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

    fn deliver_packed_batch_done(jobs: Vec<PackedCommitJob>, lane_done_txs: &[Sender<Vec<u64>>]) {
        for job in jobs {
            if let Some(done_tx) = lane_done_txs
                .get(job.shard_idx)
                .or_else(|| lane_done_txs.first())
            {
                Self::deliver_packed_done(job.buffered_seqs, job.buffered_completions, done_tx);
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
