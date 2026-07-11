use super::*;

struct PackedSlotCommit {
    actual_old_pba_meta: HashMap<Pba, RemapCleanup>,
    fresh_dedup_pairs: Vec<(ContentHash, BlockmapValue)>,
    stale_repairs: Vec<(ContentHash, DedupEntry, DedupEntry)>,
    total_refcount: u32,
    all_seq_lba_ranges: Vec<(u64, Lba, u32)>,
    any_discarded: bool,
}

enum PackedSlotCommitOutcome {
    Discarded,
    Committed(PackedSlotCommit),
}

impl BufferFlusher {
    // Legacy synchronous packed-slot write. Production callers route through
    // `write_packed_slots_batch` → commit_worker dispatch; that batch path also
    // owns the degraded fallback (when no commit-worker channel is available it
    // `defer_retry`s the seqs rather than calling this). `write_packed_slot`
    // itself is now exercised only by tests of the synchronous path — hence
    // `dead_code`. Do not wire it back into the hot path.
    #[allow(dead_code)]
    pub(in crate::buffer::flush) fn write_packed_slot(
        _shard_idx: usize,
        sealed: &SealedSlot,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        metrics: &EngineMetrics,
        cleanup_tx: &Sender<CleanupBatch>,
        candidate: &crate::dedup::CandidateCache,
    ) -> OnyxResult<()> {
        let total_start = Instant::now();

        // Collect unique volume IDs and acquire read locks on ALL of them
        // BEFORE doing any work. Sorted to prevent deadlock. Held until
        // metadata commit completes — mirrors write_unit()'s with_read_lock
        // guarantee that delete/create cannot interleave with this flush.
        let mut vol_ids: Vec<String> = sealed
            .fragments
            .iter()
            .map(|f| f.unit.vol_id.clone())
            .collect();
        vol_ids.sort();
        vol_ids.dedup();

        let locks: Vec<_> = vol_ids.iter().map(|vid| lifecycle.get_lock(vid)).collect();
        let _guards: Vec<_> = locks.iter().map(|l| l.read().unwrap()).collect();

        // Same-LBA concurrent commits are arbitrated by metadb's
        // per-LBA seq_guard CAS; no onyx-side stripe lock here.
        let outcome = (|| {
            // Under lifecycle read locks: check generation, build batch, IO, commit

            // Build blockmap entries.
            // Refcount decrements are re-computed inside the lock by atomic_batch_write_packed.
            let mut batch_values: Vec<(VolumeId, Lba, BlockmapValue)> = Vec::new();
            let mut batch_seqs: Vec<u64> = Vec::new();
            // Index-tagged so seq_guard-rejected fragments' pairs are dropped
            // before the candidate/index inserts — a rejected fragment's
            // mapping was never published (see commit_worker/packed.rs).
            let mut fresh_dedup_pairs: Vec<(usize, ContentHash, BlockmapValue)> = Vec::new();
            let mut stale_repairs: Vec<(usize, ContentHash, DedupEntry, DedupEntry)> = Vec::new();
            let mut total_refcount: u32 = 0;
            let mut all_seq_lba_ranges: Vec<(u64, Lba, u32)> = Vec::new();
            let mut any_discarded = false;

            for frag in &sealed.fragments {
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
                    for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                        let _ = pool.mark_applied(*seq, *lba_start, *lba_count);
                    }
                    continue;
                }

                let live_positions = Self::live_positions_for_unit(unit, pool)?;
                if live_positions.is_empty() {
                    any_discarded = true;
                    for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                        let _ = pool.mark_applied(*seq, *lba_start, *lba_count);
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
                        unit_compressed_size: unit.payload_len() as u32,
                        unit_original_size: unit.original_size,
                        unit_lba_count: unit.lba_count as u16,
                        offset_in_unit: pos as u16,
                        crc32: unit.crc32,
                        slot_offset: frag.slot_offset,
                        flags: frag_flags,
                    };
                    batch_values.push((vol_id.clone(), frag_lbas[i], blockmap));
                    batch_seqs.push(Self::latest_seq_for_lba(&unit.seq_lba_ranges, frag_lbas[i]));
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
                total_refcount += live_positions.len() as u32;
                all_seq_lba_ranges.extend(unit.seq_lba_ranges.iter().cloned());
            }

            if batch_values.is_empty() {
                crate::space::pba_lifecycle::rollback_uncommitted_one(allocator, sealed.pba)?;
                return Ok(PackedSlotCommitOutcome::Discarded);
            }

            let io_start = Instant::now();
            if let Err(e) =
                maybe_inject_test_failure_packed(&sealed.fragments, FlushFailStage::BeforeIoWrite)
            {
                crate::space::pba_lifecycle::rollback_uncommitted_one(allocator, sealed.pba)?;
                Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);
                return Err(e);
            }

            allocator.wait_for_readers(sealed.pba, 1);

            if let Err(e) = io_engine.write_blocks(sealed.pba, &sealed.data) {
                crate::space::pba_lifecycle::rollback_uncommitted_one(allocator, sealed.pba)?;
                Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);
                return Err(e);
            }
            Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);

            let meta_start = Instant::now();

            if let Err(e) =
                maybe_inject_test_failure_packed(&sealed.fragments, FlushFailStage::BeforeMetaWrite)
            {
                crate::space::pba_lifecycle::rollback_uncommitted_one(allocator, sealed.pba)?;
                Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
                return Err(e);
            }
            maybe_pause_before_packed_meta_write(&sealed.fragments)?;

            let actual_old_pba_meta = match meta.atomic_batch_write_packed_with_dedup(
                &batch_values,
                sealed.pba,
                total_refcount,
                &[],
                &batch_seqs,
            ) {
                Ok((m, accepted)) => {
                    let rejects = accepted.iter().filter(|a| !**a).count() as u64;
                    if rejects > 0 {
                        metrics
                            .flush_seq_rejects
                            .fetch_add(rejects, Ordering::Relaxed);
                        fresh_dedup_pairs
                            .retain(|(idx, _, _)| accepted.get(*idx).copied().unwrap_or(false));
                        stale_repairs
                            .retain(|(idx, _, _, _)| accepted.get(*idx).copied().unwrap_or(false));
                    }
                    // Every L2pRemap rejected → slot refcount stayed at 0 and
                    // nothing references it, but its payload is on LV3 —
                    // retire instead of direct-free (see
                    // `retire_rejected_extent`). Treat as a discarded slot.
                    if !accepted.iter().any(|a| *a) {
                        Self::retire_rejected_extent(cleanup_tx, sealed.pba, 1);
                        Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
                        return Ok(PackedSlotCommitOutcome::Discarded);
                    }
                    m
                }
                Err(e) => {
                    crate::space::pba_lifecycle::rollback_uncommitted_one(allocator, sealed.pba)?;
                    Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
                    return Err(e);
                }
            };
            Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
            Ok(PackedSlotCommitOutcome::Committed(PackedSlotCommit {
                actual_old_pba_meta,
                fresh_dedup_pairs: fresh_dedup_pairs
                    .into_iter()
                    .map(|(_, h, v)| (h, v))
                    .collect(),
                stale_repairs: stale_repairs
                    .into_iter()
                    .map(|(_, h, old, new)| (h, old, new))
                    .collect(),
                total_refcount,
                all_seq_lba_ranges,
                any_discarded,
            }))
        })();
        let commit = match outcome {
            Ok(PackedSlotCommitOutcome::Discarded) => {
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                return Ok(());
            }
            Ok(PackedSlotCommitOutcome::Committed(commit)) => commit,
            Err(e) => {
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                return Err(e);
            }
        };
        candidate.insert_many(&commit.fresh_dedup_pairs);
        Self::repair_stale_dedup_index(meta, metrics, &commit.stale_repairs, "write_packed_slot");

        if !commit.actual_old_pba_meta.is_empty() {
            let _ = cleanup_tx.send(commit.actual_old_pba_meta.into_values().collect());
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

        // Mark entries applied (ring release deferred until checkpoint).
        let mark_start = Instant::now();
        for (seq, lba_start, lba_count) in &commit.all_seq_lba_ranges {
            if let Err(e) = pool.mark_applied(*seq, *lba_start, *lba_count) {
                tracing::warn!(seq, error = %e, "failed to mark entry applied (packed)");
            }
        }
        Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);

        tracing::debug!(
            pba = sealed.pba.0,
            fragments = sealed.fragments.len(),
            total_lbas = commit.total_refcount,
            discarded = commit.any_discarded,
            "flushed packed slot"
        );

        Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
        Ok(())
    }

    /// Batch IO write for sealed slots, then dispatch each slot to a
    /// per-volume commit worker (routed by primary volume). Phase 1
    /// of the per-volume commit architecture: the shard writer keeps
    /// the batched IO submit so the device queue stays deep, but the
    /// metadata commit / cleanup / mark_flushed / done_tx all run on
    /// the commit worker.
    ///
    /// Failure semantics per slot:
    /// - IO failure → free PBA, queue whole-slot retry inline (preserves
    ///   the existing slot retry path).
    /// - Commit failure → handled on the commit worker (free PBA,
    ///   defer_retry buffered seqs). The shard writer does not see it.
    pub(in crate::buffer::flush) fn write_packed_slots_batch(
        shard_idx: usize,
        mut sealed_slots: Vec<SealedSlot>,
        per_slot_buffers: Vec<(Vec<u64>, Vec<Arc<crate::buffer::pipeline::DedupCompletion>>)>,
        pool: &WriteBufferPool,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        write_session: Option<&Arc<crate::io::uring::IoUringSession>>,
        metrics: &EngineMetrics,
        in_flight_tracker: &FlusherInFlightTracker,
        done_tx: &Sender<Vec<u64>>,
        packed_retries: &mut VecDeque<PackedSlotRetry>,
        commit_worker_txs: &[Sender<CommitJob>],
        commit_workers_per_volume: usize,
    ) {
        let _ = pool;
        let total_start = Instant::now();
        let n = sealed_slots.len();
        let mut slot_io_ok = vec![true; n];
        let mut io_ops_count = 0u64;

        // Stripe-batch full groups of `stripe` packed slots (each slot is
        // exactly one block) into one stripe-aligned extent → a single zero-RMW
        // full-stripe LV3 write. Reassign each member's PBA to its stripe
        // sub-PBA and free the packer's original per-slot PBA (uncommitted, so a
        // direct free is safe and it was never in the candidate cache). Leftover
        // slots (a partial final group, or a non-stripe backend) keep their own
        // PBA and the legacy per-slot path. On IO failure the whole stripe is
        // rolled back once; the member slots retry (re-allocating a fresh PBA).
        let stripe = io_engine.stripe_blocks();
        let phase = io_engine.stripe_phase();
        // group_of[i] = the stripe group slot i belongs to (else None = per-slot).
        let mut group_of: Vec<Option<usize>> = vec![None; n];
        let mut group_extents: Vec<Extent> = Vec::new();
        let mut group_slots: Vec<Vec<usize>> = Vec::new();
        if stripe > 1 {
            let sw = stripe as usize;
            let mut i = 0;
            while i + sw <= n {
                match allocator.allocate_stripe_extent_for_lane(shard_idx, stripe, stripe, phase) {
                    Ok(ext) => {
                        debug_assert_eq!(ext.count, stripe);
                        let members: Vec<usize> = (i..i + sw).collect();
                        for (j, &s) in members.iter().enumerate() {
                            let old = sealed_slots[s].pba;
                            sealed_slots[s].pba = Pba(ext.start.0 + j as u64);
                            let _ = crate::space::pba_lifecycle::rollback_uncommitted_one(
                                allocator, old,
                            );
                            group_of[s] = Some(group_extents.len());
                        }
                        group_extents.push(ext);
                        group_slots.push(members);
                        i += sw;
                    }
                    // Near-full alignment fragmentation: stop grouping; the
                    // remaining slots keep their packer PBAs (per-slot path).
                    Err(_) => break,
                }
            }
        }

        // The sealed PBA is private until metadata commit publishes
        // it, so the LV3 write runs without any lock here. The commit
        // worker re-takes lifecycle + L2P commit locks when it runs
        // the metadata phase.
        let io_start = Instant::now();
        {
            use crate::io::engine::{LvOp, LvOpResult};
            let bs = BLOCK_SIZE as usize;

            // Assemble one contiguous stripe buffer per group (each member slot
            // contributes its 4 KiB block at its stripe position). Held across
            // the submit — the grouped `LvOp` borrows it.
            let mut group_payloads: Vec<Vec<u8>> = Vec::with_capacity(group_slots.len());
            for members in &group_slots {
                let mut buf = vec![0u8; stripe as usize * bs];
                for (j, &s) in members.iter().enumerate() {
                    let data = sealed_slots[s].data.as_slice();
                    buf[j * bs..j * bs + data.len()].copy_from_slice(data);
                }
                group_payloads.push(buf);
            }

            enum OpTarget {
                Group(usize),
                Slot(usize),
            }
            let mut ops: Vec<LvOp> = Vec::with_capacity(n);
            let mut op_targets: Vec<OpTarget> = Vec::with_capacity(n);

            // Full-stripe group ops.
            for (gi, members) in group_slots.iter().enumerate() {
                let inject = members.iter().find_map(|&s| {
                    maybe_inject_test_failure_packed(
                        &sealed_slots[s].fragments,
                        FlushFailStage::BeforeIoWrite,
                    )
                    .err()
                });
                let ext = group_extents[gi];
                if let Some(e) = inject {
                    let _ = crate::space::pba_lifecycle::rollback_uncommitted(allocator, ext);
                    for &s in members {
                        slot_io_ok[s] = false;
                    }
                    tracing::error!(error = %e, "writer: packed full-stripe group injected IO write failure");
                    continue;
                }
                allocator.wait_for_readers(ext.start, stripe);
                ops.push(LvOp::Write {
                    pba: ext.start,
                    payload: group_payloads[gi].as_slice(),
                });
                op_targets.push(OpTarget::Group(gi));
            }

            // Per-slot ops (leftover / ungrouped slots).
            for i in 0..n {
                if group_of[i].is_some() {
                    continue;
                }
                let sealed = &sealed_slots[i];
                if let Err(e) = maybe_inject_test_failure_packed(
                    &sealed.fragments,
                    FlushFailStage::BeforeIoWrite,
                ) {
                    let _ = crate::space::pba_lifecycle::rollback_uncommitted_one(
                        allocator, sealed.pba,
                    );
                    slot_io_ok[i] = false;
                    tracing::error!(
                        pba = sealed.pba.0,
                        error = %e,
                        "writer: packed-slot injected IO write failure"
                    );
                    continue;
                }
                allocator.wait_for_readers(sealed.pba, 1);
                ops.push(LvOp::Write {
                    pba: sealed.pba,
                    payload: sealed.data.as_slice(),
                });
                op_targets.push(OpTarget::Slot(i));
            }

            if !ops.is_empty() {
                io_ops_count = ops.len() as u64;
                match io_engine.submit_batch_on(write_session, ops, false) {
                    Ok(write_results) => {
                        for (idx, r) in write_results.into_iter().enumerate() {
                            if let LvOpResult::Write(Err(e)) = r {
                                match op_targets[idx] {
                                    OpTarget::Group(gi) => {
                                        let _ = crate::space::pba_lifecycle::rollback_uncommitted(
                                            allocator,
                                            group_extents[gi],
                                        );
                                        for &s in &group_slots[gi] {
                                            slot_io_ok[s] = false;
                                        }
                                        tracing::error!(error = %e, "writer: packed full-stripe group IO write failed");
                                    }
                                    OpTarget::Slot(i) => {
                                        let _ =
                                            crate::space::pba_lifecycle::rollback_uncommitted_one(
                                                allocator,
                                                sealed_slots[i].pba,
                                            );
                                        slot_io_ok[i] = false;
                                        tracing::error!(
                                            pba = sealed_slots[i].pba.0,
                                            error = %e,
                                            "writer: packed-slot batch IO write failed"
                                        );
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        for t in &op_targets {
                            match *t {
                                OpTarget::Group(gi) => {
                                    let _ = crate::space::pba_lifecycle::rollback_uncommitted(
                                        allocator,
                                        group_extents[gi],
                                    );
                                    for &s in &group_slots[gi] {
                                        slot_io_ok[s] = false;
                                    }
                                }
                                OpTarget::Slot(i) => {
                                    let _ = crate::space::pba_lifecycle::rollback_uncommitted_one(
                                        allocator,
                                        sealed_slots[i].pba,
                                    );
                                    slot_io_ok[i] = false;
                                }
                            }
                        }
                        tracing::error!(
                            error = %e,
                            "writer: packed-slot batch IO submit failed"
                        );
                    }
                }
            }
        }
        let io_elapsed = io_start.elapsed();
        Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);

        // Dispatch each surviving slot to the per-volume commit
        // worker (routed by primary volume = first fragment). IO
        // failures stay on the shard writer's whole-slot retry queue,
        // mirroring the historic packed retry path.
        let mut surviving_slots: u64 = 0;
        let mut total_lbas: u64 = 0;
        let no_workers = commit_worker_txs.is_empty();
        for (i, (sealed, (mut buffered_seqs, mut buffered_completions))) in sealed_slots
            .into_iter()
            .zip(per_slot_buffers.into_iter())
            .enumerate()
        {
            if !slot_io_ok[i] {
                metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                Self::queue_packed_slot_retry(
                    packed_retries,
                    sealed,
                    &mut buffered_seqs,
                    &mut buffered_completions,
                );
                continue;
            }

            surviving_slots += 1;
            total_lbas += sealed
                .fragments
                .iter()
                .map(|f| f.unit.lba_count as u64)
                .sum::<u64>();

            if no_workers {
                // Defensive: if no worker channels are available, fall
                // back to defer_retry so seqs are not orphaned.
                in_flight_tracker.defer_retry(&buffered_seqs, Self::RETRY_BACKOFF);
                for dc in &buffered_completions {
                    in_flight_tracker.defer_retry(dc.seqs(), Self::RETRY_BACKOFF);
                }
                if !buffered_seqs.is_empty() {
                    let _ = done_tx.send(buffered_seqs);
                }
                for dc in buffered_completions {
                    if let Some(original_seqs) = dc.decrement() {
                        let _ = done_tx.send(original_seqs);
                    }
                }
                let _ =
                    crate::space::pba_lifecycle::rollback_uncommitted_one(allocator, sealed.pba);
                continue;
            }

            // Route by primary volume — first fragment's vol_id.
            let primary_vol = sealed
                .fragments
                .first()
                .map(|f| f.unit.vol_id.clone())
                .unwrap_or_default();
            let worker_idx =
                route_volume_to_worker(&primary_vol, shard_idx, commit_workers_per_volume);
            let tx_idx = worker_idx % commit_worker_txs.len();
            let job = CommitJob::Packed(PackedCommitJob {
                sealed,
                shard_idx,
                buffered_seqs,
                buffered_completions,
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

        // Counters tracking the IO + dispatch phase. Metadata-phase
        // counters are bumped on the commit worker.
        metrics
            .flush_writer_packed_batches
            .fetch_add(1, Ordering::Relaxed);
        metrics
            .flush_writer_packed_batch_slots
            .fetch_add(surviving_slots, Ordering::Relaxed);
        metrics
            .flush_writer_packed_batch_lbas
            .fetch_add(total_lbas, Ordering::Relaxed);
        metrics
            .flush_writer_packed_batch_io_ops
            .fetch_add(io_ops_count, Ordering::Relaxed);
        crate::metrics::record_counter_max(
            &metrics.flush_writer_packed_batch_slots_max,
            surviving_slots,
        );
        crate::metrics::record_counter_max(&metrics.flush_writer_packed_batch_lbas_max, total_lbas);
        crate::metrics::record_counter_max(
            &metrics.flush_writer_packed_batch_io_ops_max,
            io_ops_count,
        );
        let total_elapsed = total_start.elapsed();
        Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
        if total_elapsed >= Duration::from_secs(1) {
            tracing::debug!(
                slots = n,
                surviving = surviving_slots,
                io_ms = io_elapsed.as_millis() as u64,
                total_ms = total_elapsed.as_millis() as u64,
                "writer: slow packed-slot batch (>=1s) — IO + dispatch only"
            );
        }
    }
}
