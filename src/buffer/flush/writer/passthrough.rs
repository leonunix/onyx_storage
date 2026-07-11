use super::*;

/// Greedily pack unit indices into full-RAID-stripe bins for the zero-RMW
/// full-stripe write path. `blocks_per_unit[i]` is unit `i`'s block count;
/// `stripe` is the RAID stripe width in blocks (`io_engine.stripe_blocks()`,
/// 1 = no stripe). Returns `(groups, leftover)` where each group is a set of
/// unit indices whose block counts sum to at most `stripe`, and `leftover` is
/// every non-candidate (to take the per-unit path).
///
/// Only strictly-sub-stripe units (`1..stripe`) are packing candidates: a unit
/// that is already a whole stripe multiple (`>= stripe`) either aligns on its
/// own via [`BufferFlusher::alloc_passthrough`] or stays unaligned — we never
/// split a unit across a stripe boundary. First-fit-decreasing (largest block
/// count first, index tie-break) maximises exact fills while staying pure and
/// deterministic, so it is unit-testable in isolation and never touches the
/// allocator. A partial final bin is deliberately retained: the writer pads it
/// to a full stripe for parity IO, then returns the unused tail extent to the
/// allocator after successful IO. This is the behavior promised by
/// `raid_full_stripe_writes=true` and avoids exploding one partial bin back into
/// dozens of 4 KiB RAID writes.
fn plan_stripe_groups(blocks_per_unit: &[u32], stripe: u32) -> (Vec<Vec<usize>>, Vec<usize>) {
    let n = blocks_per_unit.len();
    if stripe <= 1 {
        return (Vec::new(), (0..n).collect());
    }

    // Candidates = strictly-sub-stripe units, largest-first for exact fills.
    let mut candidates: Vec<usize> = (0..n)
        .filter(|&i| blocks_per_unit[i] >= 1 && blocks_per_unit[i] < stripe)
        .collect();
    candidates.sort_by(|&a, &b| blocks_per_unit[b].cmp(&blocks_per_unit[a]).then(a.cmp(&b)));

    // Open bins: (remaining_capacity, members). First-fit placement.
    let mut bins: Vec<(u32, Vec<usize>)> = Vec::new();
    for idx in candidates {
        let b = blocks_per_unit[idx];
        let mut placed = false;
        for bin in bins.iter_mut() {
            if bin.0 >= b {
                bin.0 -= b;
                bin.1.push(idx);
                placed = true;
                break;
            }
        }
        if !placed {
            bins.push((stripe - b, vec![idx]));
        }
    }

    // Every non-empty bin becomes one padded full-stripe IO. The writer tracks
    // the used prefix and returns any unused tail after IO succeeds.
    let groups: Vec<Vec<usize>> = bins.into_iter().map(|(_, members)| members).collect();
    let mut leftover: Vec<usize> = Vec::new();
    // Non-candidates (0 blocks, or whole stripe multiples / oversize) are leftover.
    for i in 0..n {
        if blocks_per_unit[i] == 0 || blocks_per_unit[i] >= stripe {
            leftover.push(i);
        }
    }
    leftover.sort_unstable();
    (groups, leftover)
}

impl BufferFlusher {
    /// Legacy single-unit write path. NOT the steady-state hot path — that runs
    /// through the commit-worker pipeline (`write_units_batch` →
    /// `commit_worker/passthrough.rs`). `write_unit` survives as the
    /// **shutdown-drain** path: `handle_compressed_unit` calls it while draining
    /// the flusher on stop, when the commit workers are no longer accepting
    /// jobs. Keep it in sync with the commit-worker path's invariants
    /// (lifecycle read-lock coverage, rollback on IO/meta failure).
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
                    let _ = pool.mark_applied(*seq, *lba_start, *lba_count);
                }
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                return Ok(());
            }

            let bs = BLOCK_SIZE as usize;
            let alloc_start = Instant::now();
            let blocks_needed = (unit.compressed_data.len() + bs - 1) / bs;

            // Stripe-multiple units land on a stripe-aligned extent (full-stripe
            // write); others take the unaligned path. Same helper as the batch
            // hot path so both stay in lockstep.
            let stripe = io_engine.stripe_blocks();
            let phase = io_engine.stripe_phase();
            let pba = match Self::alloc_passthrough(
                allocator,
                shard_idx,
                blocks_needed as u32,
                stripe,
                phase,
            ) {
                Ok(pba) => pba,
                Err(e) => {
                    Self::record_elapsed(&metrics.flush_writer_alloc_ns, alloc_start);
                    Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                    return Err(e);
                }
            };
            let allocation = if blocks_needed == 1 {
                Allocation::Single(pba)
            } else {
                Allocation::Extent(Extent::new(pba, blocks_needed as u32))
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
            enum UnitDisposition {
                /// Every position superseded before the commit — the PBA was
                /// never written into any tx; direct free is safe.
                Stale,
                /// Tx committed but every L2pRemap was seq_guard-rejected —
                /// payload is on LV3; retire instead of direct-free (see
                /// `retire_rejected_extent`).
                Rejected,
                Committed(Vec<usize>, HashMap<Pba, RemapCleanup>),
            }
            let commit = (|| -> OnyxResult<UnitDisposition> {
                let live_positions = Self::live_positions_for_unit(unit, pool)?;
                if live_positions.is_empty() {
                    return Ok(UnitDisposition::Stale);
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
                    let blockmap = Self::blockmap_for_unit_position_with_raw_split(
                        unit,
                        pba,
                        live_positions[i],
                        0,
                        flags,
                        true,
                    );
                    batch_values.push((lbas[i], blockmap));
                    batch_seqs.push(Self::latest_seq_for_lba(&unit.seq_lba_ranges, lbas[i]));
                }
                // Position-tagged so seq_guard-rejected positions' pairs are
                // dropped before the candidate/index inserts: a rejected
                // position's mapping was never published, and for raw-split
                // units its per-LBA PBA is freed — inserting its (hash → pba)
                // would let a later verify byte-match a free-listed PBA and
                // promote a live mapping onto it (premature-free CRC class).
                let mut fresh_dedup_pairs: Vec<(usize, ContentHash, BlockmapValue)> = Vec::new();
                let mut stale_repairs: Vec<(usize, ContentHash, DedupEntry, DedupEntry)> =
                    Vec::new();
                if !unit.dedup_skipped {
                    if let Some(ref hashes) = unit.block_hashes {
                        fresh_dedup_pairs.reserve(live_positions.len());
                        for &pos in &live_positions {
                            let hash = hashes[pos];
                            if hash == [0u8; 8] {
                                continue;
                            }
                            let blockmap = Self::blockmap_for_unit_position_with_raw_split(
                                unit, pba, pos, 0, 0, true,
                            );
                            fresh_dedup_pairs.push((pos, hash, blockmap));
                            if let Some(repairs) = &unit.dedup_stale_repairs {
                                if let Some(Some(old_entry)) = repairs.get(pos) {
                                    stale_repairs.push((
                                        pos,
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
                // that so the outer retire path runs.
                if !accepted.iter().any(|a| *a) {
                    let rejects = accepted.len() as u64;
                    metrics
                        .flush_seq_rejects
                        .fetch_add(rejects, Ordering::Relaxed);
                    return Ok(UnitDisposition::Rejected);
                }
                let rejects = accepted.iter().filter(|a| !**a).count() as u64;
                if rejects > 0 {
                    metrics
                        .flush_seq_rejects
                        .fetch_add(rejects, Ordering::Relaxed);
                    let accepted_pos: std::collections::HashSet<usize> = live_positions
                        .iter()
                        .enumerate()
                        .filter_map(|(i, &pos)| {
                            accepted.get(i).copied().unwrap_or(false).then_some(pos)
                        })
                        .collect();
                    fresh_dedup_pairs.retain(|(pos, _, _)| accepted_pos.contains(pos));
                    stale_repairs.retain(|(pos, _, _, _)| accepted_pos.contains(pos));
                }
                let fresh_dedup_pairs: Vec<(ContentHash, BlockmapValue)> = fresh_dedup_pairs
                    .into_iter()
                    .map(|(_, h, v)| (h, v))
                    .collect();
                let stale_repairs: Vec<(ContentHash, DedupEntry, DedupEntry)> = stale_repairs
                    .into_iter()
                    .map(|(_, h, old, new)| (h, old, new))
                    .collect();
                candidate.insert_many(&fresh_dedup_pairs);
                Self::repair_stale_dedup_index(meta, metrics, &stale_repairs, "write_unit");
                Ok(UnitDisposition::Committed(
                    live_positions,
                    actual_old_pba_meta,
                ))
            })();
            let (live_positions, actual_old_pba_meta) = match commit {
                Ok(UnitDisposition::Committed(lp, m)) => (lp, m),
                Err(e) => {
                    allocation.free(allocator)?;
                    Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
                    Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                    return Err(e);
                }
                Ok(disposition) => {
                    match disposition {
                        UnitDisposition::Stale => allocation.free(allocator)?,
                        UnitDisposition::Rejected => {
                            Self::retire_rejected_extent(cleanup_tx, pba, blocks_needed as u32)
                        }
                        UnitDisposition::Committed(..) => unreachable!(),
                    }
                    let mark_start = Instant::now();
                    for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                        if let Err(e) = pool.mark_applied(*seq, *lba_start, *lba_count) {
                            tracing::warn!(seq, error = %e, "failed to mark stale entry applied");
                        }
                    }
                    Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);
                    Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
                    Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                    return Ok(());
                }
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
                if let Err(e) = pool.mark_applied(*seq, *lba_start, *lba_count) {
                    tracing::warn!(seq, error = %e, "failed to mark entry applied");
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

    /// Reserve `blocks_needed` blocks for one passthrough unit. On a stripe
    /// (RAID5/6) backend, a unit that is already a whole number of stripes gets
    /// a stripe-**aligned** extent: the engine block-pads its write to
    /// `blocks_needed` blocks and the aligned device offset makes chunklet take
    /// its zero-RMW full-stripe path. Every other unit — and any
    /// alignment-fragmentation `SpaceExhausted` — falls back to the normal
    /// unaligned path so IO never stalls (that unit just stays partial-RMW).
    ///
    /// The allocated width always equals `blocks_needed`: the aligned path is
    /// only taken when `blocks_needed % stripe == 0`, so `round_up` is a no-op
    /// and there is **no tail pad to leak** on reclaim (cleanup frees
    /// `physical_blocks` = the unpadded ceil size). Making units stripe-sized so
    /// more of them qualify is the coalesce config's job (roadmap ③); sub-stripe
    /// packing into full stripes is a documented follow-up.
    fn alloc_passthrough(
        allocator: &SpaceAllocator,
        lane: usize,
        blocks_needed: u32,
        stripe: u32,
        phase: u32,
    ) -> OnyxResult<Pba> {
        if stripe > 1 && blocks_needed % stripe == 0 {
            match allocator.allocate_stripe_extent_for_lane(lane, blocks_needed, stripe, phase) {
                Ok(ext) => {
                    debug_assert_eq!(ext.count, blocks_needed, "aligned multiple must not pad");
                    return Ok(ext.start);
                }
                // Alignment fragmentation near full: fall back to unaligned so
                // IO keeps flowing (this write misses the full-stripe path).
                Err(crate::error::OnyxError::SpaceExhausted) => {}
                Err(e) => return Err(e),
            }
        }
        if blocks_needed == 1 {
            allocator.allocate_one_for_lane(lane)
        } else {
            let ext = allocator.allocate_extent_for_lane(lane, blocks_needed)?;
            if ext.count < blocks_needed {
                crate::space::pba_lifecycle::rollback_uncommitted(allocator, ext)?;
                return Err(crate::error::OnyxError::SpaceExhausted);
            }
            Ok(ext.start)
        }
    }

    /// Direct-free an uncommitted per-unit reservation (rollback / IO-failure
    /// path). The PBA never entered metadb, so a direct free is safe. `blocks`
    /// picks the single-block vs extent free form.
    fn rollback_unit_alloc(allocator: &SpaceAllocator, pba: Pba, blocks: u32) {
        if blocks == 1 {
            let _ = crate::space::pba_lifecycle::rollback_uncommitted_one(allocator, pba);
        } else {
            let _ = crate::space::pba_lifecycle::rollback_uncommitted(
                allocator,
                Extent::new(pba, blocks),
            );
        }
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
        write_session: Option<&Arc<crate::io::uring::IoUringSession>>,
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

        // Phase A: alloc PBAs. `stripe`/`phase` come from the LV3 backend
        // geometry (1/0 = no stripe → off-chunklet or `raid_full_stripe_writes`
        // off, so grouping is empty and every unit takes the legacy path).
        //
        // Sub-stripe units are bin-packed into full RAID stripes: each group
        // reserves ONE stripe-aligned extent and is written as a single
        // zero-RMW full-stripe LV3 write, but each member keeps its own
        // block-granular mapping at a distinct sub-PBA (slot_offset stays 0),
        // so read/free are unchanged. Stripe-multiple / oversize / partial-bin
        // units fall through to the per-unit `alloc_passthrough` (which aligns
        // whole-stripe-multiple units on its own). `alloc_blocks[i]` always ==
        // the unit's block count, so reclaim frees exactly what was reserved.
        let stripe = io_engine.stripe_blocks();
        let phase = io_engine.stripe_phase();
        let alloc_start = Instant::now();
        let bs = BLOCK_SIZE as usize;
        let blocks_per_unit: Vec<u32> = units
            .iter()
            .map(|u| ((u.compressed_data.len() + bs - 1) / bs) as u32)
            .collect();
        for i in 0..n {
            alloc_blocks[i] = blocks_per_unit[i];
        }

        let (groups, leftover) = plan_stripe_groups(&blocks_per_unit, stripe);
        let group_used_blocks: Vec<u32> = groups
            .iter()
            .map(|members| members.iter().map(|&m| blocks_per_unit[m]).sum())
            .collect();
        // group_extents[gi] = Some(ext) when the stripe extent was reserved;
        // None means the group degraded to the per-unit path (alignment
        // fragmentation near-full) and its members were allocated individually.
        let mut group_extents: Vec<Option<Extent>> = Vec::with_capacity(groups.len());
        // group_of[i] = the group unit i belongs to (meaningful only when that
        // group's extent is Some — i.e. it wrote as a real full stripe).
        let mut group_of: Vec<Option<usize>> = vec![None; n];

        // Batch-level short-circuit for alignment starvation. A failed aligned
        // allocation on a fragmented free list is O(free-list length) — the
        // finder walks the whole address-ordered BTreeSet through the low-address
        // fragment belt while holding the global free lock. Retrying that scan
        // for EVERY group/unit in a 1024-unit batch (and every batch, on all 16
        // shard writers, serialised on one lock) collapses flusher throughput to
        // ~zero → buffer fills → the whole frontend stalls (reproduced on the
        // 2026-07-02 kill-replay capture, cycle 4). After the first
        // SpaceExhausted, stop attempting stripe alignment for the REST of this
        // batch (stripe=1 ⇒ alloc_passthrough skips the aligned path); the next
        // batch probes again, so alignment resumes once reclaim re-coalesces.
        let mut stripe_starved = false;

        for (gi, members) in groups.iter().enumerate() {
            if stripe_starved {
                group_extents.push(None);
                for &m in members {
                    match Self::alloc_passthrough(allocator, shard_idx, blocks_per_unit[m], 1, 0) {
                        Ok(pba) => pbas[m] = Some(pba),
                        Err(e) => {
                            tracing::error!(
                                vol = %units[m].vol_id,
                                start_lba = units[m].start_lba.0,
                                error = %e,
                                "writer: passthrough alloc failed (stripe-starved batch)"
                            );
                            failed[m] = true;
                        }
                    }
                }
                continue;
            }
            match allocator.allocate_stripe_extent_for_lane(shard_idx, stripe, stripe, phase) {
                Ok(ext) => {
                    debug_assert_eq!(ext.count, stripe, "stripe extent must be one stripe wide");
                    let mut off = 0u64;
                    for &m in members {
                        pbas[m] = Some(Pba(ext.start.0 + off));
                        group_of[m] = Some(gi);
                        off += blocks_per_unit[m] as u64;
                    }
                    debug_assert_eq!(
                        off, group_used_blocks[gi] as u64,
                        "member PBAs must cover the planned used prefix"
                    );
                    debug_assert!(off <= stripe as u64, "group must fit within one stripe");
                    group_extents.push(Some(ext));
                }
                Err(_) => {
                    // Alignment fragmentation near-full: degrade to per-unit so
                    // IO keeps flowing (these units just miss the full stripe),
                    // and stop probing alignment for the rest of the batch.
                    // Counted per batch transition (the short-circuit makes
                    // per-group counting meaningless): the rate ≈ share of
                    // flush batches running degraded → RAID partial-RMW.
                    stripe_starved = true;
                    metrics
                        .flush_writer_stripe_starved_batches
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    group_extents.push(None);
                    for &m in members {
                        match Self::alloc_passthrough(
                            allocator,
                            shard_idx,
                            blocks_per_unit[m],
                            1,
                            0,
                        ) {
                            Ok(pba) => pbas[m] = Some(pba),
                            Err(e) => {
                                tracing::error!(
                                    vol = %units[m].vol_id,
                                    start_lba = units[m].start_lba.0,
                                    error = %e,
                                    "writer: passthrough alloc failed (degraded group)"
                                );
                                failed[m] = true;
                            }
                        }
                    }
                }
            }
        }
        for &i in &leftover {
            let (eff_stripe, eff_phase) = if stripe_starved {
                (1, 0)
            } else {
                (stripe, phase)
            };
            match Self::alloc_passthrough(
                allocator,
                shard_idx,
                blocks_per_unit[i],
                eff_stripe,
                eff_phase,
            ) {
                Ok(pba) => pbas[i] = Some(pba),
                Err(e) => {
                    tracing::error!(
                        vol = %units[i].vol_id,
                        start_lba = units[i].start_lba.0,
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

            // Assemble one contiguous stripe buffer per reserved group. Each
            // member's compressed payload is copied to its block-aligned
            // sub-offset; the tail padding inside each member's last block stays
            // zero (the reader slices by `unit_compressed_size`). Held alive in
            // `group_payloads` across the submit — the grouped `LvOp` borrows it.
            let mut group_payloads: Vec<Vec<u8>> = Vec::with_capacity(groups.len());
            for (gi, members) in groups.iter().enumerate() {
                if group_extents[gi].is_none() {
                    group_payloads.push(Vec::new());
                    continue;
                }
                let mut buf = vec![0u8; stripe as usize * bs];
                let mut off_blocks = 0usize;
                for &m in members {
                    let data = units[m].compressed_data.as_slice();
                    let start = off_blocks * bs;
                    buf[start..start + data.len()].copy_from_slice(data);
                    off_blocks += alloc_blocks[m] as usize;
                }
                group_payloads.push(buf);
            }

            enum OpTarget {
                Group(usize),
                Unit(usize),
            }
            let mut ops: Vec<LvOp> = Vec::with_capacity(n);
            let mut op_targets: Vec<OpTarget> = Vec::with_capacity(n);

            // Full-stripe group ops: one `LvOp::Write` covers the whole 24 KiB
            // stripe → chunklet takes its zero-RMW full-stripe path.
            for (gi, members) in groups.iter().enumerate() {
                if group_extents[gi].is_none() {
                    continue;
                }
                // Fail the whole group if ANY member hits the injected failpoint.
                let inject = members.iter().find_map(|&m| {
                    maybe_inject_test_failure(
                        &units[m].vol_id,
                        units[m].start_lba,
                        FlushFailStage::BeforeIoWrite,
                    )
                    .err()
                });
                let ext = group_extents[gi].unwrap();
                if let Some(e) = inject {
                    let _ = crate::space::pba_lifecycle::rollback_uncommitted(allocator, ext);
                    for &m in members {
                        failed[m] = true;
                    }
                    tracing::error!(error = %e, group = gi, "writer: full-stripe group injected IO write failure");
                    continue;
                }
                allocator.wait_for_readers(ext.start, stripe);
                ops.push(LvOp::Write {
                    pba: ext.start,
                    payload: group_payloads[gi].as_slice(),
                });
                op_targets.push(OpTarget::Group(gi));
            }

            // Per-unit ops: leftover units + degraded-group members.
            for i in 0..n {
                if failed[i] || pbas[i].is_none() {
                    continue;
                }
                // Skip units already covered by a reserved full-stripe group.
                if group_of[i].is_some_and(|gi| group_extents[gi].is_some()) {
                    continue;
                }
                if let Err(e) = maybe_inject_test_failure(
                    &units[i].vol_id,
                    units[i].start_lba,
                    FlushFailStage::BeforeIoWrite,
                ) {
                    Self::rollback_unit_alloc(allocator, pbas[i].unwrap(), alloc_blocks[i]);
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
                op_targets.push(OpTarget::Unit(i));
            }

            if !ops.is_empty() {
                io_ops_count = ops.len() as u64;
                match io_engine.submit_batch_on(write_session, ops, false) {
                    Ok(write_results) => {
                        for (idx, r) in write_results.into_iter().enumerate() {
                            if let LvOpResult::Write(Err(e)) = r {
                                match op_targets[idx] {
                                    OpTarget::Group(gi) => {
                                        let ext = group_extents[gi].unwrap();
                                        let _ = crate::space::pba_lifecycle::rollback_uncommitted(
                                            allocator, ext,
                                        );
                                        for &m in &groups[gi] {
                                            failed[m] = true;
                                        }
                                        tracing::error!(error = %e, group = gi, "writer: full-stripe group IO write failed");
                                    }
                                    OpTarget::Unit(i) => {
                                        Self::rollback_unit_alloc(
                                            allocator,
                                            pbas[i].unwrap(),
                                            alloc_blocks[i],
                                        );
                                        failed[i] = true;
                                        tracing::error!(
                                            vol = units[i].vol_id,
                                            start_lba = units[i].start_lba.0,
                                            error = %e,
                                            "writer: passthrough IO write failed"
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
                                    let ext = group_extents[gi].unwrap();
                                    let _ = crate::space::pba_lifecycle::rollback_uncommitted(
                                        allocator, ext,
                                    );
                                    for &m in &groups[gi] {
                                        failed[m] = true;
                                    }
                                }
                                OpTarget::Unit(i) => {
                                    if !failed[i] {
                                        Self::rollback_unit_alloc(
                                            allocator,
                                            pbas[i].unwrap(),
                                            alloc_blocks[i],
                                        );
                                        failed[i] = true;
                                    }
                                }
                            }
                        }
                        tracing::error!(error = %e, "writer: passthrough IO batch submit failed");
                    }
                }
            }

            // Successful padded groups only publish mappings for their used
            // prefix. Return the never-mapped tail immediately; failed groups
            // already rolled their whole stripe back above.
            for (gi, extent) in group_extents.iter().enumerate() {
                let Some(extent) = *extent else {
                    continue;
                };
                if groups[gi].iter().any(|&m| failed[m]) {
                    continue;
                }
                let used = group_used_blocks[gi];
                if used < stripe {
                    let padding = Extent::new(Pba(extent.start.0 + used as u64), stripe - used);
                    if let Err(e) =
                        crate::space::pba_lifecycle::rollback_uncommitted(allocator, padding)
                    {
                        metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                        tracing::error!(
                            error = %e,
                            group = gi,
                            used_blocks = used,
                            stripe_blocks = stripe,
                            "writer: failed to return full-stripe padding extent"
                        );
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

#[cfg(test)]
mod stripe_group_tests {
    use super::plan_stripe_groups;

    /// Every group must fit within one stripe, and group members + leftover
    /// must partition `0..n` exactly once.
    fn assert_partition(blocks: &[u32], stripe: u32) {
        let (groups, leftover) = plan_stripe_groups(blocks, stripe);
        let mut seen = vec![false; blocks.len()];
        for g in &groups {
            let sum: u32 = g.iter().map(|&i| blocks[i]).sum();
            assert!(
                sum > 0 && sum <= stripe,
                "group {g:?} must fit stripe {stripe}, got {sum}"
            );
            for &i in g {
                assert!(!seen[i], "index {i} in two places");
                seen[i] = true;
            }
        }
        for &i in &leftover {
            assert!(!seen[i], "index {i} in group and leftover");
            seen[i] = true;
        }
        assert!(seen.iter().all(|&s| s), "every index placed once");
    }

    #[test]
    fn exact_fills_form_one_group() {
        // Each of these sums to exactly one 6-block stripe.
        for blocks in [
            vec![2, 2, 2],
            vec![3, 3],
            vec![1, 2, 3],
            vec![4, 2],
            vec![5, 1],
            vec![1, 1, 1, 1, 1, 1],
        ] {
            let (groups, leftover) = plan_stripe_groups(&blocks, 6);
            assert_eq!(groups.len(), 1, "{blocks:?} → exactly one full stripe");
            assert!(leftover.is_empty(), "{blocks:?} → no leftover");
            assert_partition(&blocks, 6);
        }
    }

    #[test]
    fn partial_bin_is_retained_for_full_stripe_padding() {
        // 3 + 2 = 5 < 6 → one padded stripe, no per-unit fallback.
        let (groups, leftover) = plan_stripe_groups(&[3, 2], 6);
        assert_eq!(groups, vec![vec![0, 1]]);
        assert!(leftover.is_empty());
    }

    #[test]
    fn whole_stripe_multiple_and_oversize_are_leftover() {
        // 6 = one stripe on its own (alloc_passthrough aligns it); 7 > stripe,
        // not a multiple → per-unit path. Neither is a packing candidate.
        let (groups, leftover) = plan_stripe_groups(&[6, 7, 12], 6);
        assert!(groups.is_empty());
        assert_eq!(leftover, vec![0, 1, 2]);
    }

    #[test]
    fn twelve_ones_form_two_stripes() {
        let blocks = vec![1u32; 12];
        let (groups, leftover) = plan_stripe_groups(&blocks, 6);
        assert_eq!(groups.len(), 2);
        assert!(leftover.is_empty());
        assert_partition(&blocks, 6);
    }

    #[test]
    fn mixed_batch_is_deterministic_and_partitions() {
        // 3,3,2,2,5,1 (blocks) + a lone 4 that can't be completed.
        let blocks = vec![3, 3, 2, 2, 5, 1, 4];
        let (g1, l1) = plan_stripe_groups(&blocks, 6);
        let (g2, l2) = plan_stripe_groups(&blocks, 6);
        assert_eq!(g1, g2, "pure + deterministic");
        assert_eq!(l1, l2);
        assert_partition(&blocks, 6);
        // FFD order 5,4,3,3,2,2,1 → stripes 5+1, 4+2, 3+3 (three full); the
        // last 2-block (idx 3) becomes one padded partial stripe.
        assert_eq!(g1.len(), 4);
        assert!(l1.is_empty());
    }

    #[test]
    fn stripe_one_is_all_leftover() {
        let blocks = vec![1, 2, 3];
        let (groups, leftover) = plan_stripe_groups(&blocks, 1);
        assert!(groups.is_empty());
        assert_eq!(leftover, vec![0, 1, 2]);
    }

    #[test]
    fn empty_units_pass_through() {
        // 0-block units are never candidates and always land in leftover.
        let (groups, leftover) = plan_stripe_groups(&[0, 0], 6);
        assert!(groups.is_empty());
        assert_eq!(leftover, vec![0, 1]);
    }
}
