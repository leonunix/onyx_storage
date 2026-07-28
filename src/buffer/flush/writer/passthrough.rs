use super::*;
use crate::error::OnyxError;

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
pub(super) fn plan_stripe_groups(
    blocks_per_unit: &[u32],
    affinity: Option<&[StripeAffinityKey<'_>]>,
    stripe: u32,
) -> (Vec<Vec<usize>>, Vec<usize>) {
    let n = blocks_per_unit.len();
    if stripe <= 1 {
        return (Vec::new(), (0..n).collect());
    }

    // Candidates = strictly-sub-stripe units, largest-first for exact fills.
    let candidates: Vec<usize> = (0..n)
        .filter(|&i| blocks_per_unit[i] >= 1 && blocks_per_unit[i] < stripe)
        .collect();

    // Every non-empty bin becomes one padded full-stripe IO. The writer tracks
    // the used prefix and returns any unused tail after IO succeeds.
    //
    // The affinity arm plans BOTH ways and keeps its own only when it costs no
    // pad space: affinity hands a SUBSET back to the size-first packer, and a
    // packer with fewer options can pack worse. Measured counterexample:
    // `[8,2,2,2,6,5,6,4,4,3,1,8,1]`, where taking three same-volume 2-block
    // units as their own stripe strands the 4/4/3 units that those 2s would
    // otherwise have topped up (pad 6 vs 0). Pad waste is space amplification --
    // the very thing this knob exists to reduce -- so a tie keeps affinity
    // (equal pad plus better co-location is a strict improvement) and a loss
    // falls back. The second plan and its clone stay INSIDE this arm so the
    // default-off path allocates and packs exactly once, as it did before.
    let groups = match affinity.filter(|keys| keys.len() == n) {
        Some(keys) => {
            let size_first = pack_size_first(candidates.clone(), blocks_per_unit, stripe);
            let mut affinity_groups: Vec<Vec<usize>> = Vec::new();
            let rest = pack_affinity_stripes(
                candidates,
                blocks_per_unit,
                keys,
                stripe,
                &mut affinity_groups,
            );
            affinity_groups.extend(pack_size_first(rest, blocks_per_unit, stripe));
            if pad_waste(&affinity_groups, blocks_per_unit, stripe)
                <= pad_waste(&size_first, blocks_per_unit, stripe)
            {
                affinity_groups
            } else {
                size_first
            }
        }
        None => pack_size_first(candidates, blocks_per_unit, stripe),
    };
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

/// First-fit-decreasing packing by block count alone: the original (and
/// fallback) policy. Largest-first with an index tie-break maximises exact fills
/// while staying pure and deterministic. A partial final bin is deliberately
/// retained — the writer pads it to a full stripe for parity IO and returns the
/// unused tail after IO, which beats exploding it into single-block RAID writes.
fn pack_size_first(
    mut candidates: Vec<usize>,
    blocks_per_unit: &[u32],
    stripe: u32,
) -> Vec<Vec<usize>> {
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
    bins.into_iter().map(|(_, members)| members).collect()
}

/// Blocks that will be written as stripe padding under this plan: every group
/// costs a whole stripe of physical space regardless of how much it uses.
fn pad_waste(groups: &[Vec<usize>], blocks_per_unit: &[u32], stripe: u32) -> u32 {
    groups
        .iter()
        .map(|members| {
            let used: u32 = members.iter().map(|&m| blocks_per_unit[m]).sum();
            stripe.saturating_sub(used)
        })
        .sum()
}

/// Where a unit lives logically, for lifetime-affinity stripe packing.
///
/// The allocator only gets a stripe window back into its reserve when every
/// block in that window is free at the same time, and a window is never
/// re-folded while a single live block pins it. So the packer's choice of
/// *which* units share a stripe decides how long that window stays pinned:
/// same-volume neighbours are overwritten (and freed) together, six unrelated
/// LBAs are not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct StripeAffinityKey<'a> {
    pub vol: &'a str,
    pub lba: u64,
}

/// Emit only the stripes an affinity run fills **exactly**; return every unit it
/// could not place so the size-first packer still gets a shot at them.
///
/// Units are visited volume-major, LBA-ascending, and accumulated into an open
/// bin. A volume change or an overflow closes the bin and releases its members
/// back — a partial bin is deliberately *not* padded here, because pad waste is
/// the one thing this pass must not add (measured `unused_blocks` is 0.004 % of
/// LBAs today). No absolute LBA-gap cutoff is applied: after sorting, the
/// neighbours in one flush batch are already the closest ones available, and
/// arriving in the same batch is itself temporal locality.
fn pack_affinity_stripes(
    candidates: Vec<usize>,
    blocks_per_unit: &[u32],
    keys: &[StripeAffinityKey<'_>],
    stripe: u32,
    groups: &mut Vec<Vec<usize>>,
) -> Vec<usize> {
    let mut ordered = candidates;
    ordered.sort_by(|&a, &b| {
        keys[a]
            .vol
            .cmp(keys[b].vol)
            .then(keys[a].lba.cmp(&keys[b].lba))
            .then(a.cmp(&b))
    });

    let mut unplaced: Vec<usize> = Vec::new();
    let mut bin: Vec<usize> = Vec::new();
    let mut used = 0u32;
    let mut bin_vol: Option<&str> = None;

    for idx in ordered {
        let blocks = blocks_per_unit[idx];
        if bin_vol != Some(keys[idx].vol) || used + blocks > stripe {
            unplaced.append(&mut bin);
            used = 0;
            bin_vol = Some(keys[idx].vol);
        }
        bin.push(idx);
        used += blocks;
        if used == stripe {
            groups.push(std::mem::take(&mut bin));
            used = 0;
            bin_vol = None;
        }
    }
    unplaced.append(&mut bin);
    unplaced.sort_unstable();
    unplaced
}

#[derive(Debug)]
struct WriteRun {
    /// The writer exclusively owns this reservation until LV3 IO succeeds.
    /// After success, the used prefix is transferred to the member units and
    /// any padded tail is returned to the allocator.
    extent: Extent,
    used_blocks: u32,
    members: Vec<usize>,
    full_stripe: bool,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct WriteRunAllocStats {
    short_extent_allocs: u64,
    unused_blocks: u64,
    /// Allocator calls this helper made. The caller charges them to the
    /// unaligned bucket; counting surviving runs instead would undercount the
    /// capacity miss that ends the loop and overstate the per-op mean.
    alloc_calls: u64,
}

/// Select whole units that fit in `capacity`, preserving the input order.
/// Units are never split across extents. Returns `(members, used_blocks)` and
/// removes selected members from `remaining`.
fn take_members_for_capacity(
    remaining: &mut Vec<usize>,
    blocks_per_unit: &[u32],
    capacity: u32,
) -> (Vec<usize>, u32) {
    let mut selected = Vec::new();
    let mut used = 0u32;
    remaining.retain(|&member| {
        let blocks = blocks_per_unit[member];
        if blocks <= capacity.saturating_sub(used) {
            selected.push(member);
            used += blocks;
            false
        } else {
            true
        }
    });
    (selected, used)
}

/// Reserve as few unaligned contiguous runs as the fragmented pool permits.
/// `allocate_extent_for_lane` is intentionally an up-to allocation: a short
/// result is consumed only up to whole-unit boundaries, and its unused tail is
/// returned in one lock-amortized batch. On a non-capacity allocator error, all
/// reservations made by this call are rolled back before returning the error.
fn allocate_unaligned_write_runs(
    allocator: &SpaceAllocator,
    lane: usize,
    mut remaining: Vec<usize>,
    blocks_per_unit: &[u32],
    stripe: u32,
    phase: u32,
) -> OnyxResult<(Vec<WriteRun>, Vec<usize>, WriteRunAllocStats)> {
    remaining.sort_by(|&a, &b| blocks_per_unit[b].cmp(&blocks_per_unit[a]).then(a.cmp(&b)));
    let mut runs = Vec::new();
    let mut unused_tails = Vec::new();
    let mut stats = WriteRunAllocStats::default();

    while !remaining.is_empty() {
        let remaining_blocks = remaining
            .iter()
            .map(|&member| blocks_per_unit[member])
            .fold(0u32, u32::saturating_add);
        let requested = remaining_blocks.min(stripe.max(1));
        stats.alloc_calls += 1;
        let extent = match allocator.allocate_extent_for_lane(lane, requested) {
            Ok(extent) => extent,
            Err(OnyxError::SpaceExhausted) => break,
            Err(error) => {
                let mut rollback: Vec<Extent> =
                    runs.iter().map(|run: &WriteRun| run.extent).collect();
                rollback.extend(unused_tails);
                crate::space::pba_lifecycle::rollback_uncommitted_batch(allocator, &rollback);
                return Err(error);
            }
        };
        if extent.count < requested {
            stats.short_extent_allocs += 1;
        }

        let (members, used_blocks) =
            take_members_for_capacity(&mut remaining, blocks_per_unit, extent.count);
        if members.is_empty() {
            // The largest available run cannot contain even the smallest
            // remaining unit. A unit's on-disk mapping requires one contiguous
            // extent, so leave those units for the exact per-unit fallback.
            unused_tails.push(extent);
            stats.unused_blocks += extent.count as u64;
            break;
        }

        debug_assert!(used_blocks <= extent.count);
        if used_blocks < extent.count {
            stats.unused_blocks += (extent.count - used_blocks) as u64;
            unused_tails.push(Extent::new(
                Pba(extent.start.0 + used_blocks as u64),
                extent.count - used_blocks,
            ));
        }
        let owned_extent = Extent::new(extent.start, used_blocks);
        let full_stripe = used_blocks == stripe
            && stripe > 1
            && (owned_extent.start.0 + phase as u64).is_multiple_of(stripe as u64);
        runs.push(WriteRun {
            extent: owned_extent,
            used_blocks,
            members,
            full_stripe,
        });
    }

    if !unused_tails.is_empty() {
        crate::space::pba_lifecycle::rollback_uncommitted_batch(allocator, &unused_tails);
    }
    Ok((runs, remaining, stats))
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
            let blocks_needed = (unit.payload_len() + bs - 1) / bs;

            // Stripe-multiple units land on a stripe-aligned extent (full-stripe
            // write); others take the unaligned path. Same helper as the batch
            // hot path so both stay in lockstep.
            let stripe = io_engine.stripe_blocks();
            let phase = io_engine.stripe_phase();
            let pba = match Self::alloc_passthrough(
                allocator,
                metrics,
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

            let materialized_payload;
            let payload = match unit.payload_contiguous() {
                Some(payload) => payload,
                None => {
                    materialized_payload = unit.materialize_payload();
                    &materialized_payload
                }
            };
            if let Err(e) = io_engine.write_blocks(pba, payload) {
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
                .fetch_add(unit.payload_len() as u64, Ordering::Relaxed);

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
                compressed = unit.payload_len(),
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
        metrics: &EngineMetrics,
        lane: usize,
        blocks_needed: u32,
        stripe: u32,
        phase: u32,
    ) -> OnyxResult<Pba> {
        if stripe > 1 && blocks_needed % stripe == 0 {
            let aligned_start = Instant::now();
            match allocator.allocate_stripe_extent_for_lane(lane, blocks_needed, stripe, phase) {
                Ok(ext) => {
                    debug_assert_eq!(ext.count, blocks_needed, "aligned multiple must not pad");
                    Self::record_alloc_path(
                        &metrics.flush_writer_alloc_aligned_ns,
                        &metrics.flush_writer_alloc_aligned_ops,
                        aligned_start,
                    );
                    return Ok(ext.start);
                }
                // Alignment fragmentation near full: fall back to unaligned so
                // IO keeps flowing (this write misses the full-stripe path).
                // The miss itself is the expensive case (lane-cache drain plus a
                // global-lock retry), so it gets its own bucket rather than
                // being charged to the aligned path that never served anything.
                Err(crate::error::OnyxError::SpaceExhausted) => {
                    Self::record_alloc_path(
                        &metrics.flush_writer_alloc_reserve_miss_ns,
                        &metrics.flush_writer_alloc_reserve_miss_ops,
                        aligned_start,
                    );
                }
                Err(e) => {
                    Self::record_alloc_path(
                        &metrics.flush_writer_alloc_aligned_ns,
                        &metrics.flush_writer_alloc_aligned_ops,
                        aligned_start,
                    );
                    return Err(e);
                }
            }
        }
        let unaligned_start = Instant::now();
        let result = if blocks_needed == 1 {
            allocator.allocate_one_for_lane(lane)
        } else {
            allocator
                .allocate_exact_extent_for_lane(lane, blocks_needed)
                .map(|ext| ext.start)
        };
        Self::record_alloc_path(
            &metrics.flush_writer_alloc_unaligned_ns,
            &metrics.flush_writer_alloc_unaligned_ops,
            unaligned_start,
        );
        result
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
        pool: &WriteBufferPool,
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
        // Sub-stripe units are first bin-packed into aligned full-stripe runs.
        // Once alignment is unavailable, the remaining members are repacked
        // into the largest contiguous runs the fragmented pool can provide.
        // Every member still owns a distinct block-granular sub-PBA and no unit
        // is split across runs. Stripe-multiple / oversize units stay on the
        // exact per-unit path. `alloc_blocks[i]` always equals the unit's block
        // count, so commit-worker rollback never reaches a neighbour's blocks.
        let stripe = io_engine.stripe_blocks();
        let phase = io_engine.stripe_phase();
        let alloc_start = Instant::now();
        let bs = BLOCK_SIZE as usize;
        let blocks_per_unit: Vec<u32> = units
            .iter()
            .map(|u| ((u.payload_len() + bs - 1) / bs) as u32)
            .collect();
        for i in 0..n {
            alloc_blocks[i] = blocks_per_unit[i];
        }

        // Lifetime affinity needs each unit's logical identity; borrow it from
        // `units` for the duration of planning only.
        let affinity_keys: Option<Vec<StripeAffinityKey>> =
            io_engine.stripe_lifetime_affinity().then(|| {
                units
                    .iter()
                    .map(|unit| StripeAffinityKey {
                        vol: unit.vol_id.as_str(),
                        lba: unit.start_lba.0,
                    })
                    .collect()
            });
        let (groups, leftover) =
            plan_stripe_groups(&blocks_per_unit, affinity_keys.as_deref(), stripe);
        let group_used_blocks: Vec<u32> = groups
            .iter()
            .map(|members| members.iter().map(|&m| blocks_per_unit[m]).sum())
            .collect();

        // Judge the plan by its output, not by which pass produced it: a stripe
        // that is exactly full AND single-volume is the one whose blocks can
        // plausibly die together and return the window to the stripe reserve.
        // Recorded on both A/B arms so the knob's effect is directly readable.
        if !groups.is_empty() {
            let single_volume = groups
                .iter()
                .zip(&group_used_blocks)
                .filter(|(members, &used)| {
                    used == stripe
                        && members
                            .iter()
                            .all(|&m| units[m].vol_id == units[members[0]].vol_id)
                })
                .count();
            metrics
                .flush_writer_stripe_groups_total
                .fetch_add(groups.len() as u64, Ordering::Relaxed);
            metrics
                .flush_writer_stripe_single_volume_groups
                .fetch_add(single_volume as u64, Ordering::Relaxed);
        }
        let mut write_runs: Vec<Option<WriteRun>> = Vec::with_capacity(groups.len());
        let mut run_of: Vec<Option<usize>> = vec![None; n];

        // A configured-geometry aligned lookup is indexed, but a miss can still
        // drain lane caches and serialize every writer on the global free lock.
        // Stop probing alignment for this batch after its first capacity miss.
        // Flatten all remaining sub-stripe members into unaligned runs that
        // safely consume short extents at whole-unit boundaries.
        let mut stripe_starved = false;
        let mut degraded_members = Vec::new();

        for (gi, members) in groups.iter().enumerate() {
            if stripe_starved {
                degraded_members.extend(members.iter().copied());
                continue;
            }
            let group_alloc_start = Instant::now();
            match allocator.allocate_stripe_extent_for_lane(shard_idx, stripe, stripe, phase) {
                Ok(extent) => {
                    debug_assert_eq!(extent.count, stripe);
                    Self::record_alloc_path(
                        &metrics.flush_writer_alloc_aligned_ns,
                        &metrics.flush_writer_alloc_aligned_ops,
                        group_alloc_start,
                    );
                    write_runs.push(Some(WriteRun {
                        extent,
                        used_blocks: group_used_blocks[gi],
                        members: members.clone(),
                        full_stripe: true,
                    }));
                }
                Err(OnyxError::SpaceExhausted) => {
                    Self::record_alloc_path(
                        &metrics.flush_writer_alloc_reserve_miss_ns,
                        &metrics.flush_writer_alloc_reserve_miss_ops,
                        group_alloc_start,
                    );
                    stripe_starved = true;
                    metrics
                        .flush_writer_stripe_starved_batches
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    degraded_members.extend(members.iter().copied());
                }
                Err(error) => {
                    Self::record_alloc_path(
                        &metrics.flush_writer_alloc_aligned_ns,
                        &metrics.flush_writer_alloc_aligned_ops,
                        group_alloc_start,
                    );
                    for &m in members {
                        failed[m] = true;
                        tracing::error!(
                            vol = %units[m].vol_id,
                            start_lba = units[m].start_lba.0,
                            error = %error,
                            "writer: aligned grouped allocation failed"
                        );
                    }
                }
            }
        }

        let mut unplaced_members = Vec::new();
        if !degraded_members.is_empty() {
            let degraded_for_error = degraded_members.clone();
            // Every run here comes out of the general pool, so the whole helper
            // is charged to the unaligned path as one op per surviving run.
            let degraded_alloc_start = Instant::now();
            let degraded_result = allocate_unaligned_write_runs(
                allocator,
                shard_idx,
                degraded_members,
                &blocks_per_unit,
                stripe,
                phase,
            );
            Self::record_elapsed(
                &metrics.flush_writer_alloc_unaligned_ns,
                degraded_alloc_start,
            );
            match degraded_result {
                Ok((runs, unplaced, stats)) => {
                    metrics
                        .flush_writer_alloc_unaligned_ops
                        .fetch_add(stats.alloc_calls, Ordering::Relaxed);
                    metrics
                        .flush_writer_group_short_extent_allocs
                        .fetch_add(stats.short_extent_allocs, Ordering::Relaxed);
                    metrics
                        .flush_writer_group_unused_blocks
                        .fetch_add(stats.unused_blocks, Ordering::Relaxed);
                    write_runs.extend(runs.into_iter().map(Some));
                    unplaced_members = unplaced;
                }
                Err(error) => {
                    // The helper only returns Err from an allocator call, so the
                    // elapsed time above belongs to at least one op; charging
                    // zero would inflate the surviving ops' mean.
                    metrics
                        .flush_writer_alloc_unaligned_ops
                        .fetch_add(1, Ordering::Relaxed);
                    for m in degraded_for_error {
                        failed[m] = true;
                        tracing::error!(
                            vol = %units[m].vol_id,
                            start_lba = units[m].start_lba.0,
                            error = %error,
                            "writer: unaligned grouped allocation failed"
                        );
                    }
                }
            }
        }

        // Assign each run member a disjoint subextent. The run owns the full
        // reservation through LV3 IO; successful IO transfers these exact
        // subextents to the per-unit commit jobs.
        for (run_idx, run) in write_runs.iter().enumerate() {
            let run = run.as_ref().expect("new write run owns its extent");
            let mut off = 0u64;
            for &member in &run.members {
                debug_assert!(run_of[member].is_none(), "unit belongs to one write run");
                pbas[member] = Some(Pba(run.extent.start.0 + off));
                run_of[member] = Some(run_idx);
                off += blocks_per_unit[member] as u64;
            }
            debug_assert_eq!(off, run.used_blocks as u64);
            debug_assert!(off <= run.extent.count as u64);
        }

        // A short run that cannot contain any remaining whole unit leaves those
        // units on the exact per-unit path. Exact allocation never hands back a
        // partial extent, so a large compressed unit cannot be split silently.
        metrics
            .flush_writer_group_fallback_units
            .fetch_add(unplaced_members.len() as u64, Ordering::Relaxed);
        for m in unplaced_members {
            match Self::alloc_passthrough(allocator, metrics, shard_idx, blocks_per_unit[m], 1, 0) {
                Ok(pba) => pbas[m] = Some(pba),
                Err(error) => {
                    failed[m] = true;
                    tracing::error!(
                        vol = %units[m].vol_id,
                        start_lba = units[m].start_lba.0,
                        error = %error,
                        "writer: passthrough exact alloc failed (degraded group)"
                    );
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
                metrics,
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
            use crate::io::engine::{LvOpResult, OwnedLvWrite};

            // Assemble directly into the O_DIRECT-aligned buffers that the
            // global chunklet combiner will own. This removes the old
            // full-stripe Vec -> IoEngine slab memcpy.
            let mut run_buffers: Vec<Option<crate::io::aligned::AlignedBuf>> =
                Vec::with_capacity(write_runs.len());
            for run_idx in 0..write_runs.len() {
                let Some(run) = write_runs[run_idx].as_ref() else {
                    run_buffers.push(None);
                    continue;
                };
                let extent = run.extent;
                if run.full_stripe {
                    debug_assert_eq!(extent.count, stripe);
                    debug_assert!(
                        (extent.start.0 + phase as u64).is_multiple_of(stripe as u64),
                        "full-stripe run must be device-offset aligned"
                    );
                }
                let bufalloc_start = Instant::now();
                let allocated = io_engine.allocate_owned_write_buffer(extent.count as usize * bs);
                Self::record_elapsed(&metrics.flush_writer_bufalloc_ns, bufalloc_start);
                let mut buf = match allocated {
                    Ok(buf) => buf,
                    Err(e) => {
                        let run = write_runs[run_idx]
                            .take()
                            .expect("buffer failure consumes run ownership");
                        let _ = crate::space::pba_lifecycle::rollback_uncommitted(
                            allocator, run.extent,
                        );
                        for member in run.members {
                            failed[member] = true;
                        }
                        tracing::error!(
                            error = %e,
                            run = run_idx,
                            "writer: grouped run buffer allocation failed"
                        );
                        run_buffers.push(None);
                        continue;
                    }
                };
                let bufzero_start = Instant::now();
                buf.as_mut_slice().fill(0);
                Self::record_elapsed(&metrics.flush_writer_bufzero_ns, bufzero_start);
                let assemble_start = Instant::now();
                let mut off_blocks = 0usize;
                for &m in &run.members {
                    let data_len = units[m].payload_len();
                    let start = off_blocks * bs;
                    units[m].copy_payload_to(&mut buf.as_mut_slice()[start..start + data_len]);
                    off_blocks += alloc_blocks[m] as usize;
                }
                Self::record_elapsed(&metrics.flush_writer_assemble_ns, assemble_start);
                debug_assert_eq!(off_blocks, run.used_blocks as usize);
                run_buffers.push(Some(buf));
            }

            enum OpTarget {
                Run(usize),
                Unit(usize),
            }
            // Alignment-starved and non-group units still get one aligned
            // owner, filled directly from contiguous or scatter payloads.
            let mut unit_buffers: Vec<Option<crate::io::aligned::AlignedBuf>> =
                (0..n).map(|_| None).collect();
            for i in 0..n {
                if run_of[i].is_none() && !failed[i] && pbas[i].is_some() {
                    let total = alloc_blocks[i] as usize * bs;
                    let bufalloc_start = Instant::now();
                    let allocated = io_engine.allocate_owned_write_buffer(total);
                    Self::record_elapsed(&metrics.flush_writer_bufalloc_ns, bufalloc_start);
                    match allocated {
                        Ok(mut buf) => {
                            let bufzero_start = Instant::now();
                            buf.as_mut_slice().fill(0);
                            Self::record_elapsed(&metrics.flush_writer_bufzero_ns, bufzero_start);
                            let assemble_start = Instant::now();
                            units[i]
                                .copy_payload_to(&mut buf.as_mut_slice()[..units[i].payload_len()]);
                            Self::record_elapsed(&metrics.flush_writer_assemble_ns, assemble_start);
                            unit_buffers[i] = Some(buf);
                        }
                        Err(e) => {
                            let pba = pbas[i]
                                .take()
                                .expect("buffer failure consumes unit ownership");
                            Self::rollback_unit_alloc(allocator, pba, alloc_blocks[i]);
                            failed[i] = true;
                            tracing::error!(
                                vol = units[i].vol_id,
                                start_lba = units[i].start_lba.0,
                                error = %e,
                                "writer: aligned unit buffer allocation failed"
                            );
                        }
                    }
                }
            }

            let submit_start = Instant::now();
            let mut ops: Vec<OwnedLvWrite> = Vec::with_capacity(n);
            let mut op_targets: Vec<OpTarget> = Vec::with_capacity(n);

            // One write op per actual run. Stripe-aligned runs retain their
            // padded width; short unaligned runs contain only whole units.
            for run_idx in 0..write_runs.len() {
                let Some(run) = write_runs[run_idx].as_ref() else {
                    continue;
                };
                if run_buffers[run_idx].is_none()
                    || run.members.iter().any(|&member| failed[member])
                {
                    continue;
                }
                // Fail the whole run if ANY member hits the injected failpoint.
                let inject = run.members.iter().find_map(|&member| {
                    maybe_inject_test_failure(
                        &units[member].vol_id,
                        units[member].start_lba,
                        FlushFailStage::BeforeIoWrite,
                    )
                    .err()
                });
                if let Some(e) = inject {
                    let run = write_runs[run_idx]
                        .take()
                        .expect("injected failure consumes run ownership");
                    let _ =
                        crate::space::pba_lifecycle::rollback_uncommitted(allocator, run.extent);
                    for member in run.members {
                        failed[member] = true;
                    }
                    tracing::error!(
                        error = %e,
                        run = run_idx,
                        "writer: grouped run injected IO write failure"
                    );
                    continue;
                }
                let extent = run.extent;
                allocator.wait_for_readers(extent.start, extent.count);
                let buffer = run_buffers[run_idx]
                    .take()
                    .expect("reserved run has an aligned buffer");
                let payload_len = buffer.len();
                ops.push(OwnedLvWrite {
                    pba: extent.start,
                    payload_len,
                    buffer,
                });
                if run.full_stripe {
                    metrics
                        .flush_writer_group_aligned_ops
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    metrics
                        .flush_writer_group_unaligned_ops
                        .fetch_add(1, Ordering::Relaxed);
                }
                op_targets.push(OpTarget::Run(run_idx));
            }

            // Per-unit ops: leftover units + degraded-group members.
            for i in 0..n {
                if failed[i] || pbas[i].is_none() {
                    continue;
                }
                // Run members are covered by their run's single IO op.
                if run_of[i].is_some() {
                    continue;
                }
                if let Err(e) = maybe_inject_test_failure(
                    &units[i].vol_id,
                    units[i].start_lba,
                    FlushFailStage::BeforeIoWrite,
                ) {
                    let pba = pbas[i]
                        .take()
                        .expect("injected failure consumes unit ownership");
                    Self::rollback_unit_alloc(allocator, pba, alloc_blocks[i]);
                    failed[i] = true;
                    tracing::error!(
                        vol = units[i].vol_id,
                        start_lba = units[i].start_lba.0,
                        error = %e,
                        "writer: passthrough injected IO write failure"
                    );
                    continue;
                }
                let pba = pbas[i].expect("per-unit reservation has PBA");
                allocator.wait_for_readers(pba, alloc_blocks[i]);
                let buffer = unit_buffers[i]
                    .take()
                    .expect("per-unit payload must have an aligned buffer");
                ops.push(OwnedLvWrite {
                    pba,
                    payload_len: units[i].payload_len(),
                    buffer,
                });
                op_targets.push(OpTarget::Unit(i));
            }

            Self::record_elapsed(&metrics.flush_writer_submit_ops_ns, submit_start);
            let submit_io_start = Instant::now();
            if !ops.is_empty() {
                io_ops_count = ops.len() as u64;
                let batch_result = io_engine.submit_owned_write_batch_on(write_session, ops, false);
                Self::record_elapsed(&metrics.flush_writer_submit_io_ns, submit_io_start);
                let rollback_start = Instant::now();
                match batch_result {
                    Ok(write_results) => {
                        for (idx, r) in write_results.into_iter().enumerate() {
                            if let LvOpResult::Write(Err(e)) = r {
                                match op_targets[idx] {
                                    OpTarget::Run(run_idx) => {
                                        let run = write_runs[run_idx]
                                            .take()
                                            .expect("IO failure consumes run ownership");
                                        let _ = crate::space::pba_lifecycle::rollback_uncommitted(
                                            allocator, run.extent,
                                        );
                                        for member in run.members {
                                            failed[member] = true;
                                        }
                                        tracing::error!(
                                            error = %e,
                                            run = run_idx,
                                            "writer: grouped run IO write failed"
                                        );
                                    }
                                    OpTarget::Unit(i) => {
                                        let pba = pbas[i]
                                            .take()
                                            .expect("IO failure consumes unit ownership");
                                        Self::rollback_unit_alloc(allocator, pba, alloc_blocks[i]);
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
                                OpTarget::Run(run_idx) => {
                                    let run = write_runs[run_idx]
                                        .take()
                                        .expect("batch failure consumes run ownership");
                                    let _ = crate::space::pba_lifecycle::rollback_uncommitted(
                                        allocator, run.extent,
                                    );
                                    for member in run.members {
                                        failed[member] = true;
                                    }
                                }
                                OpTarget::Unit(i) => {
                                    if !failed[i] {
                                        let pba = pbas[i]
                                            .take()
                                            .expect("batch failure consumes unit ownership");
                                        Self::rollback_unit_alloc(allocator, pba, alloc_blocks[i]);
                                        failed[i] = true;
                                    }
                                }
                            }
                        }
                        tracing::error!(error = %e, "writer: passthrough IO batch submit failed");
                    }
                }
                Self::record_elapsed(&metrics.flush_writer_submit_rollback_ns, rollback_start);
            } else {
                Self::record_elapsed(&metrics.flush_writer_submit_io_ns, submit_io_start);
            }
            let padding_start = Instant::now();

            // Successful run IO transfers the used prefix to disjoint per-unit
            // reservations. Return only the never-mapped aligned padding; short
            // run tails were already returned by the allocation planner.
            let mut successful_padding = Vec::new();
            let mut successful_padding_blocks = 0u64;
            for run_idx in 0..write_runs.len() {
                let Some(run) = write_runs[run_idx].take() else {
                    continue;
                };
                debug_assert!(run.members.iter().all(|&member| !failed[member]));
                if run.used_blocks < run.extent.count {
                    successful_padding_blocks += (run.extent.count - run.used_blocks) as u64;
                    successful_padding.push(Extent::new(
                        Pba(run.extent.start.0 + run.used_blocks as u64),
                        run.extent.count - run.used_blocks,
                    ));
                }
            }
            metrics
                .flush_writer_group_unused_blocks
                .fetch_add(successful_padding_blocks, Ordering::Relaxed);
            crate::space::pba_lifecycle::rollback_uncommitted_batch(allocator, &successful_padding);
            Self::record_elapsed(&metrics.flush_writer_submit_padding_ns, padding_start);
            Self::record_elapsed(&metrics.flush_writer_submit_ns, submit_start);
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

        // Dispatch one job per volume to its routed commit worker. A missing or
        // disconnected receiver is a persistence failure, not a best-effort
        // drop: recover the SendError ownership and close the LV3/LV2 lifecycle.
        for (vol, units_for_vol) in per_volume {
            let job = PassthroughCommitJob {
                vol_id: VolumeId(vol.clone()),
                units: units_for_vol,
                enqueued_at: Instant::now(),
            };
            if commit_worker_txs.is_empty() {
                Self::fail_undispatched_passthrough_job(
                    job,
                    pool,
                    allocator,
                    in_flight_tracker,
                    metrics,
                    Some(done_tx),
                    "commit aggregator channel unavailable",
                );
            } else {
                let worker_idx = route_volume_to_worker(&vol, shard_idx, commit_workers_per_volume);
                let tx_idx = worker_idx % commit_worker_txs.len();
                let send_start = Instant::now();
                let send_result = commit_worker_txs[tx_idx].send(CommitJob::Passthrough(job));
                Self::record_elapsed(&metrics.flush_writer_commit_send_ns, send_start);
                metrics
                    .flush_writer_commit_send_ops
                    .fetch_add(1, Ordering::Relaxed);
                crate::metrics::record_counter_max(
                    &metrics.flush_writer_commit_send_len_max,
                    commit_worker_txs[tx_idx].len() as u64,
                );
                if let Err(error) = send_result {
                    let CommitJob::Passthrough(failed_job) = error.0 else {
                        unreachable!("passthrough sender returned a different commit job kind")
                    };
                    Self::fail_undispatched_passthrough_job(
                        failed_job,
                        pool,
                        allocator,
                        in_flight_tracker,
                        metrics,
                        Some(done_tx),
                        "commit aggregator disconnected during passthrough handoff",
                    );
                }
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
    use super::{
        allocate_unaligned_write_runs, plan_stripe_groups, take_members_for_capacity,
        StripeAffinityKey,
    };
    use crate::space::allocator::SpaceAllocator;
    use crate::space::extent::Extent;
    use crate::types::{Pba, BLOCK_SIZE, RESERVED_BLOCKS};

    /// Every group must fit within one stripe, and group members + leftover
    /// must partition `0..n` exactly once.
    fn assert_partition(blocks: &[u32], stripe: u32) {
        let (groups, leftover) = plan_stripe_groups(blocks, None, stripe);
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
            let (groups, leftover) = plan_stripe_groups(&blocks, None, 6);
            assert_eq!(groups.len(), 1, "{blocks:?} → exactly one full stripe");
            assert!(leftover.is_empty(), "{blocks:?} → no leftover");
            assert_partition(&blocks, 6);
        }
    }

    #[test]
    fn partial_bin_is_retained_for_full_stripe_padding() {
        // 3 + 2 = 5 < 6 → one padded stripe, no per-unit fallback.
        let (groups, leftover) = plan_stripe_groups(&[3, 2], None, 6);
        assert_eq!(groups, vec![vec![0, 1]]);
        assert!(leftover.is_empty());
    }

    #[test]
    fn whole_stripe_multiple_and_oversize_are_leftover() {
        // 6 = one stripe on its own (alloc_passthrough aligns it); 7 > stripe,
        // not a multiple → per-unit path. Neither is a packing candidate.
        let (groups, leftover) = plan_stripe_groups(&[6, 7, 12], None, 6);
        assert!(groups.is_empty());
        assert_eq!(leftover, vec![0, 1, 2]);
    }

    #[test]
    fn twelve_ones_form_two_stripes() {
        let blocks = vec![1u32; 12];
        let (groups, leftover) = plan_stripe_groups(&blocks, None, 6);
        assert_eq!(groups.len(), 2);
        assert!(leftover.is_empty());
        assert_partition(&blocks, 6);
    }

    #[test]
    fn mixed_batch_is_deterministic_and_partitions() {
        // 3,3,2,2,5,1 (blocks) + a lone 4 that can't be completed.
        let blocks = vec![3, 3, 2, 2, 5, 1, 4];
        let (g1, l1) = plan_stripe_groups(&blocks, None, 6);
        let (g2, l2) = plan_stripe_groups(&blocks, None, 6);
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
        let (groups, leftover) = plan_stripe_groups(&blocks, None, 1);
        assert!(groups.is_empty());
        assert_eq!(leftover, vec![0, 1, 2]);
    }

    /// Build affinity keys from `(volume, lba)` pairs.
    fn keys<'a>(pairs: &'a [(&'a str, u64)]) -> Vec<StripeAffinityKey<'a>> {
        pairs
            .iter()
            .map(|&(vol, lba)| StripeAffinityKey { vol, lba })
            .collect()
    }

    /// Same invariant as `assert_partition`, with affinity on: every candidate
    /// still lands exactly once and no group overflows the stripe.
    fn assert_affinity_partition(blocks: &[u32], pairs: &[(&str, u64)], stripe: u32) {
        let k = keys(pairs);
        let (groups, leftover) = plan_stripe_groups(blocks, Some(&k), stripe);
        let mut seen = vec![false; blocks.len()];
        for g in &groups {
            let sum: u32 = g.iter().map(|&i| blocks[i]).sum();
            assert!(sum > 0 && sum <= stripe, "group {g:?} overflows {stripe}");
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
    fn affinity_none_is_byte_for_byte_the_size_first_plan() {
        // The knob defaults off, so `None` must reproduce the legacy plan
        // exactly for every shape the size-first tests above cover.
        for blocks in [
            vec![3, 3, 2, 2, 5, 1, 4],
            vec![1u32; 12],
            vec![2, 2, 2],
            vec![6, 7, 12],
            vec![0, 0],
            vec![5, 1, 5, 1],
        ] {
            let (g_legacy, l_legacy) = plan_stripe_groups(&blocks, None, 6);
            // An affinity slice of the wrong length is ignored (defensive
            // guard), which must also fall back to the legacy plan.
            let short = keys(&[("v", 0)]);
            let (g_guard, l_guard) = plan_stripe_groups(&blocks, Some(&short), 6);
            assert_eq!(g_legacy, g_guard, "{blocks:?} length guard → legacy plan");
            assert_eq!(l_legacy, l_guard);
        }
    }

    #[test]
    fn affinity_groups_same_volume_neighbours_into_one_stripe() {
        // Six 1-block units: three from vol-a at adjacent LBAs, three from
        // vol-b. Size-first would mix them (all are equal size, so first-fit
        // fills one bin in index order); affinity must not.
        let blocks = vec![1u32; 6];
        let pairs = [
            ("vol-a", 0),
            ("vol-b", 900),
            ("vol-a", 1),
            ("vol-b", 901),
            ("vol-a", 2),
            ("vol-b", 902),
        ];
        let k = keys(&pairs);
        let (groups, leftover) = plan_stripe_groups(&blocks, Some(&k), 3);
        assert!(leftover.is_empty());
        assert_eq!(groups.len(), 2, "one stripe per volume");
        // Each group is single-volume and LBA-ascending.
        for g in &groups {
            let vols: std::collections::BTreeSet<&str> = g.iter().map(|&i| pairs[i].0).collect();
            assert_eq!(vols.len(), 1, "group {g:?} mixes volumes");
            let lbas: Vec<u64> = g.iter().map(|&i| pairs[i].1).collect();
            let mut sorted = lbas.clone();
            sorted.sort_unstable();
            assert_eq!(lbas, sorted, "group {g:?} not LBA-ascending");
        }
        assert_affinity_partition(&blocks, &pairs, 3);
    }

    #[test]
    fn affinity_never_emits_a_partial_stripe_of_its_own() {
        // vol-a can only muster 2 of the 3 blocks a stripe needs. Affinity must
        // hand both back rather than pad a short stripe; the size-first pass
        // then packs them with vol-b's unit (pad waste unchanged).
        let blocks = vec![1, 1, 1];
        let pairs = [("vol-a", 0), ("vol-a", 1), ("vol-b", 500)];
        let k = keys(&pairs);
        let (groups, leftover) = plan_stripe_groups(&blocks, Some(&k), 3);
        assert!(leftover.is_empty());
        assert_eq!(groups, vec![vec![0, 1, 2]], "one mixed stripe, still exact");
        assert_affinity_partition(&blocks, &pairs, 3);
    }

    #[test]
    fn affinity_falls_back_for_units_it_cannot_fill_exactly() {
        // vol-a: 2+2 = 4 of 6 (short), vol-b: 5 of 6 (short). Neither fills a
        // stripe alone, so all four go to the size-first packer, which forms
        // 5+1 and 2+2 -- exactly the legacy grouping for these sizes.
        let blocks = vec![2, 2, 5, 1];
        let pairs = [("vol-a", 0), ("vol-a", 1), ("vol-b", 700), ("vol-b", 701)];
        let k = keys(&pairs);
        let (groups, leftover) = plan_stripe_groups(&blocks, Some(&k), 6);
        let (g_legacy, l_legacy) = plan_stripe_groups(&blocks, None, 6);
        assert_eq!(groups, g_legacy, "no exact affinity fill → legacy plan");
        assert_eq!(leftover, l_legacy);
        assert_affinity_partition(&blocks, &pairs, 6);
    }

    #[test]
    fn affinity_is_deterministic_and_index_order_independent() {
        // Same logical batch, units presented in a different order: the plan
        // must be the same set of (volume, lba) groupings.
        let blocks = vec![1u32; 6];
        let a = [
            ("vol-a", 10),
            ("vol-a", 11),
            ("vol-a", 12),
            ("vol-b", 20),
            ("vol-b", 21),
            ("vol-b", 22),
        ];
        let b = [
            ("vol-b", 22),
            ("vol-a", 12),
            ("vol-b", 20),
            ("vol-a", 10),
            ("vol-b", 21),
            ("vol-a", 11),
        ];
        let group_keys = |pairs: &[(&str, u64)]| -> std::collections::BTreeSet<Vec<(String, u64)>> {
            let k = keys(pairs);
            let (groups, leftover) = plan_stripe_groups(&blocks, Some(&k), 3);
            assert!(leftover.is_empty());
            groups
                .iter()
                .map(|g| {
                    let mut named: Vec<(String, u64)> = g
                        .iter()
                        .map(|&i| (pairs[i].0.to_string(), pairs[i].1))
                        .collect();
                    named.sort();
                    named
                })
                .collect()
        };
        assert_eq!(group_keys(&a), group_keys(&b));
        // And repeat runs are identical (pure function).
        assert_eq!(group_keys(&a), group_keys(&a));
    }

    #[test]
    fn affinity_pad_waste_never_exceeds_the_size_first_plan() {
        // Pad waste = padded stripe bytes - used bytes. Affinity only emits
        // exact fills, so its total waste must be <= legacy on every shape.
        let cases: [(Vec<u32>, Vec<(&str, u64)>); 4] = [
            (
                vec![1, 1, 1, 1, 1, 1, 1],
                vec![
                    ("a", 0),
                    ("a", 1),
                    ("a", 2),
                    ("b", 5),
                    ("b", 6),
                    ("b", 7),
                    ("c", 9),
                ],
            ),
            (
                vec![3, 3, 2, 2, 5, 1],
                vec![("a", 0), ("a", 4), ("b", 1), ("b", 2), ("c", 3), ("c", 8)],
            ),
            (
                vec![2, 4, 2, 4],
                vec![("a", 0), ("a", 1), ("b", 2), ("b", 3)],
            ),
            (vec![1, 5], vec![("a", 0), ("a", 1)]),
        ];
        let waste = |groups: &[Vec<usize>], blocks: &[u32]| -> u32 {
            groups
                .iter()
                .map(|g| {
                    let used: u32 = g.iter().map(|&i| blocks[i]).sum();
                    6 - used
                })
                .sum()
        };
        for (blocks, pairs) in cases {
            let k = keys(&pairs);
            let (g_aff, _) = plan_stripe_groups(&blocks, Some(&k), 6);
            let (g_legacy, _) = plan_stripe_groups(&blocks, None, 6);
            assert!(
                waste(&g_aff, &blocks) <= waste(&g_legacy, &blocks),
                "affinity added pad waste for {blocks:?}: {} vs {}",
                waste(&g_aff, &blocks),
                waste(&g_legacy, &blocks)
            );
            assert_affinity_partition(&blocks, &pairs, 6);
        }
    }

    #[test]
    fn affinity_pad_waste_property_over_random_batches() {
        // "Affinity never adds pad" is NOT proven: the affinity pass hands a
        // SUBSET back to the size-first packer, and a packer offered fewer
        // options can in principle do worse. Hand-picked shapes above found no
        // counterexample, so assert it empirically over a deterministic sweep --
        // a failure here is a real (if small) space regression, and should be
        // read as one rather than quietly relaxed.
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};

        const STRIPE: u32 = 6;
        let mut rng = StdRng::seed_from_u64(0x0aff_1114_79_5eed);
        let vols = ["vol-a", "vol-b", "vol-c", "vol-d"];
        let waste = |groups: &[Vec<usize>], blocks: &[u32]| -> u32 {
            groups
                .iter()
                .map(|g| STRIPE - g.iter().map(|&i| blocks[i]).sum::<u32>())
                .sum()
        };

        let mut plans_differed = 0usize;
        for round in 0..4_000 {
            let n = rng.gen_range(1..=24usize);
            // 1..=8 spans candidates (1..5), the exact stripe (6) and oversize
            // (7, 8), so the leftover path is exercised too.
            let blocks: Vec<u32> = (0..n).map(|_| rng.gen_range(1..=8u32)).collect();
            let pairs: Vec<(&str, u64)> = (0..n)
                .map(|_| {
                    (
                        vols[rng.gen_range(0..vols.len())],
                        rng.gen_range(0..2_000u64),
                    )
                })
                .collect();
            let k = keys(&pairs);
            let (g_aff, l_aff) = plan_stripe_groups(&blocks, Some(&k), STRIPE);
            let (g_leg, l_leg) = plan_stripe_groups(&blocks, None, STRIPE);

            assert_affinity_partition(&blocks, &pairs, STRIPE);
            // Leftover is a function of sizes alone, so affinity must not move it.
            assert_eq!(
                l_aff, l_leg,
                "round {round}: leftover changed for {blocks:?}"
            );
            let (wa, wl) = (waste(&g_aff, &blocks), waste(&g_leg, &blocks));
            assert!(
                wa <= wl,
                "round {round}: affinity added pad ({wa} > {wl})\n  blocks {blocks:?}\n  keys {pairs:?}"
            );
            if g_aff != g_leg {
                plans_differed += 1;
            }
        }
        // Guard against a vacuous sweep: affinity must actually be re-planning.
        assert!(
            plans_differed > 100,
            "sweep never exercised the affinity path ({plans_differed} of 4000 plans differed)"
        );
    }

    #[test]
    fn affinity_reshapes_the_plan_under_a_box_shaped_batch() {
        // The pad tie-break can only ever *reject* affinity, so guard the shape
        // we actually ship against it silently becoming a no-op: ONE volume (the
        // box runs a single fio-volume, so the volume dimension is constant and
        // every bit of signal comes from LBA adjacency) and mostly 1-block units,
        // which is what randrw 4k-32k through lz4 produces. Measured at the time
        // of writing: affinity re-plans 98 % of these batches and 88 % of the
        // stripes it emits are exactly full.
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};

        const STRIPE: u32 = 6;
        let mut rng = StdRng::seed_from_u64(0x0b0c_5eed_0000_0001);
        let mut differed = 0usize;
        let mut exact_full = 0usize;
        let mut emitted = 0usize;
        const ROUNDS: usize = 2_000;
        for _ in 0..ROUNDS {
            let n = rng.gen_range(8..=40usize);
            let blocks: Vec<u32> = (0..n)
                .map(|_| {
                    if rng.gen_range(0..10u32) < 7 {
                        1
                    } else {
                        rng.gen_range(2..=5u32)
                    }
                })
                .collect();
            let pairs: Vec<(&str, u64)> = (0..n)
                .map(|_| ("fio-volume", rng.gen_range(0..67_108_864u64)))
                .collect();
            let k = keys(&pairs);
            let (g_aff, _) = plan_stripe_groups(&blocks, Some(&k), STRIPE);
            let (g_leg, _) = plan_stripe_groups(&blocks, None, STRIPE);
            if g_aff != g_leg {
                differed += 1;
            }
            emitted += g_aff.len();
            exact_full += g_aff
                .iter()
                .filter(|g| g.iter().map(|&i| blocks[i]).sum::<u32>() == STRIPE)
                .count();
        }
        assert!(
            differed * 100 / ROUNDS >= 90,
            "affinity re-planned only {differed} of {ROUNDS} box-shaped batches — the pad \
             tie-break has turned the knob into a near no-op on the shipping shape"
        );
        assert!(
            exact_full * 100 / emitted >= 80,
            "only {exact_full} of {emitted} emitted stripes are exactly full"
        );
    }

    #[test]
    fn short_extent_is_split_only_at_whole_unit_boundaries() {
        const STRIPE: u32 = 6;
        const PHASE: u32 = 2;
        let allocator = SpaceAllocator::new(64 * BLOCK_SIZE as u64, 1);

        let whole = allocator
            .allocate_extent(64 - RESERVED_BLOCKS as u32)
            .unwrap();
        assert_eq!(whole.start, Pba(RESERVED_BLOCKS));
        allocator.set_stripe_geometry(STRIPE, PHASE);
        // No 6-block run exists. The 5-block run can hold two whole 2-block
        // units; its one-block tail is returned, and the third unit uses the
        // second run.
        allocator.free_extent(Extent::new(Pba(10), 5)).unwrap();
        allocator.free_extent(Extent::new(Pba(20), 2)).unwrap();
        let free_before = allocator.free_block_count();

        let (runs, unplaced, stats) =
            allocate_unaligned_write_runs(&allocator, 0, vec![0, 1, 2], &[2, 2, 2], STRIPE, PHASE)
                .unwrap();
        assert!(unplaced.is_empty());
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].extent, Extent::new(Pba(10), 4));
        assert_eq!(runs[0].members, vec![0, 1]);
        assert_eq!(runs[0].used_blocks, 4);
        assert_eq!(runs[1].extent, Extent::new(Pba(20), 2));
        assert_eq!(runs[1].members, vec![2]);
        assert_eq!(stats.short_extent_allocs, 1);
        assert_eq!(stats.unused_blocks, 1);
        assert!(allocator.is_free(Pba(14)), "unused short-run tail returned");
        assert_eq!(allocator.free_block_count(), free_before - 6);

        let extents: Vec<Extent> = runs.iter().map(|run| run.extent).collect();
        crate::space::pba_lifecycle::rollback_uncommitted_batch(&allocator, &extents);
        assert_eq!(allocator.free_block_count(), free_before);
    }

    #[test]
    fn short_extent_too_small_for_any_unit_is_returned() {
        const STRIPE: u32 = 6;
        const PHASE: u32 = 2;
        let allocator = SpaceAllocator::new(64 * BLOCK_SIZE as u64, 1);

        let whole = allocator
            .allocate_extent(64 - RESERVED_BLOCKS as u32)
            .unwrap();
        assert_eq!(whole.start, Pba(RESERVED_BLOCKS));
        allocator.set_stripe_geometry(STRIPE, PHASE);
        allocator.free_extent(Extent::new(Pba(10), 4)).unwrap();
        let free_before = allocator.free_block_count();

        let (runs, unplaced, stats) =
            allocate_unaligned_write_runs(&allocator, 0, vec![0], &[5], STRIPE, PHASE).unwrap();
        assert!(runs.is_empty());
        assert_eq!(unplaced, vec![0]);
        assert_eq!(stats.short_extent_allocs, 1);
        assert_eq!(stats.unused_blocks, 4);
        assert_eq!(allocator.free_block_count(), free_before);
        for pba in 10..14 {
            assert!(allocator.is_free(Pba(pba)));
        }
    }

    #[test]
    fn exact_lane_miss_does_not_consume_or_rollback_short_extent() {
        const STRIPE: u32 = 6;
        const PHASE: u32 = 2;
        let allocator = SpaceAllocator::new(64 * BLOCK_SIZE as u64, 1);

        let whole = allocator
            .allocate_extent(64 - RESERVED_BLOCKS as u32)
            .unwrap();
        assert_eq!(whole.start, Pba(RESERVED_BLOCKS));
        allocator.set_stripe_geometry(STRIPE, PHASE);
        allocator.free_extent(Extent::new(Pba(10), 5)).unwrap();

        let free_before = allocator.free_block_count();
        let stats_before = allocator.contiguity_stats();
        let result = allocator.allocate_exact_extent_for_lane(0, STRIPE);
        assert!(matches!(
            result,
            Err(crate::error::OnyxError::SpaceExhausted)
        ));

        let stats_after = allocator.contiguity_stats();
        assert_eq!(allocator.free_block_count(), free_before);
        assert_eq!(
            stats_after.free_blocks_in_set,
            stats_before.free_blocks_in_set
        );
        assert_eq!(stats_after.free_extents, stats_before.free_extents);
        assert_eq!(
            stats_after.largest_run_blocks,
            stats_before.largest_run_blocks
        );
        for pba in 10..15 {
            assert!(allocator.is_free(Pba(pba)));
        }
    }

    #[test]
    fn member_selection_never_splits_a_unit() {
        let blocks = [4, 2, 1];
        let mut remaining = vec![0, 1, 2];
        let (selected, used) = take_members_for_capacity(&mut remaining, &blocks, 5);
        assert_eq!(selected, vec![0, 2]);
        assert_eq!(used, 5);
        assert_eq!(remaining, vec![1]);
    }

    #[test]
    fn empty_units_pass_through() {
        // 0-block units are never candidates and always land in leftover.
        let (groups, leftover) = plan_stripe_groups(&[0, 0], None, 6);
        assert!(groups.is_empty());
        assert_eq!(leftover, vec![0, 1]);
    }
}
