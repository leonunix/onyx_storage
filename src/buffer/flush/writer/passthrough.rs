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
        cleanup_tx: &Sender<Vec<(Pba, u32)>>,
        _dedup_register_tx: &Sender<Vec<DedupRegistration>>,
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

            if let Err(e) = io_engine.write_blocks(pba, &unit.compressed_data) {
                allocation.free(allocator)?;
                Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                return Err(e);
            }
            Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);

            let meta_start = Instant::now();
            let live_positions = Self::live_positions_for_unit(unit, pool)?;
            if live_positions.is_empty() {
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
                let blockmap =
                    Self::blockmap_for_unit_position(unit, pba, live_positions[i], 0, flags);
                batch_values.push((lbas[i], blockmap));
            }
            let mut dedup_registrations: Vec<DedupRegistration> = Vec::new();
            if !unit.dedup_skipped {
                if let Some(ref hashes) = unit.block_hashes {
                    dedup_registrations.reserve(live_positions.len());
                    for &pos in &live_positions {
                        let hash = hashes[pos];
                        if hash == [0u8; 32] {
                            continue;
                        }
                        let blockmap = Self::blockmap_for_unit_position(unit, pba, pos, 0, 0);
                        dedup_registrations.push(Self::dedup_registration(
                            vol_id.clone(),
                            Lba(unit.start_lba.0 + pos as u64),
                            hash,
                            blockmap,
                        ));
                    }
                }
            }

            if let Err(e) = maybe_inject_test_failure(
                &unit.vol_id,
                unit.start_lba,
                FlushFailStage::BeforeMetaWrite,
            ) {
                allocation.free(allocator)?;
                Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                return Err(e);
            }

            let actual_old_pba_meta = match meta.atomic_batch_write_with_dedup(
                &vol_id,
                &batch_values,
                live_positions.len() as u32,
                &[],
            ) {
                Ok(m) => m,
                Err(e) => {
                    allocation.free(allocator)?;
                    Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
                    Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                    return Err(e);
                }
            };
            Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
            Self::enqueue_dedup_registrations(
                meta,
                metrics,
                _dedup_register_tx,
                dedup_registrations,
            );
            Self::free_unreferenced_raw_blocks(unit, pba, &live_positions, allocator, "write_unit");

            if !actual_old_pba_meta.is_empty() {
                let dead: Vec<(Pba, u32)> = actual_old_pba_meta
                    .iter()
                    .map(|(pba, (_, blocks))| (*pba, *blocks))
                    .collect();
                let _ = cleanup_tx.send(dead);
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

    /// Batch-write multiple Passthrough units in one go:
    /// 1. Acquire lifecycle read locks for all unique volumes (sorted, no deadlock)
    /// 2. Validate generations, discard stale units
    /// 3. Allocate PBAs for all units
    /// 4. Batch IO writes
    /// 5. Batch multi_get old mappings
    /// 6. ONE metadata batch for all blockmap + refcount updates
    /// 7. Batch cleanup + queue dedup registration + mark_flushed
    pub(in crate::buffer::flush) fn write_units_batch(
        shard_idx: usize,
        units: &[CompressedUnit],
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        metrics: &EngineMetrics,
        cleanup_tx: &Sender<Vec<(Pba, u32)>>,
        _dedup_register_tx: &Sender<Vec<DedupRegistration>>,
    ) -> Vec<OnyxResult<()>> {
        if units.is_empty() {
            return Vec::new();
        }
        let total_start = Instant::now();

        // Acquire lifecycle read locks for all unique volumes (sorted to prevent deadlock).
        let mut vol_ids: Vec<String> = units.iter().map(|u| u.vol_id.clone()).collect();
        vol_ids.sort();
        vol_ids.dedup();
        let locks: Vec<_> = vol_ids.iter().map(|vid| lifecycle.get_lock(vid)).collect();
        let _guards: Vec<_> = locks.iter().map(|l| l.read().unwrap()).collect();

        // Per-unit state tracking.
        let n = units.len();
        let mut results: Vec<OnyxResult<()>> = (0..n).map(|_| Ok(())).collect();
        let mut skip = vec![false; n]; // true = discard (stale volume)
        let mut pbas: Vec<Option<Pba>> = vec![None; n];
        let mut alloc_blocks: Vec<u32> = vec![0; n];

        // Phase 1: Validate generations.
        for (i, unit) in units.iter().enumerate() {
            let vol_id = VolumeId(unit.vol_id.clone());
            let should_discard = match meta.get_volume(&vol_id) {
                Ok(None) => true,
                Ok(Some(vc)) if vc.created_at != unit.vol_created_at => true,
                Ok(_) => false,
                Err(e) => {
                    results[i] = Err(e);
                    skip[i] = true;
                    continue;
                }
            };
            if should_discard {
                metrics.flush_stale_discards.fetch_add(1, Ordering::Relaxed);
                skip[i] = true;
                for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                    let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
                }
            }
        }

        // Phase 2: Allocate PBAs for non-skipped units.
        let alloc_start = Instant::now();
        for (i, unit) in units.iter().enumerate() {
            if skip[i] {
                continue;
            }
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
                Ok((pba, _)) => {
                    pbas[i] = Some(pba);
                }
                Err(e) => {
                    results[i] = Err(e);
                    skip[i] = true;
                }
            }
        }
        let alloc_elapsed = alloc_start.elapsed();
        Self::record_elapsed(&metrics.flush_writer_alloc_ns, alloc_start);

        // Phase 3: Batched IO writes — one submit per batch.
        // io_uring backend: 1 io_uring_enter + 1 wait_for_completions(N).
        // Syscall backend: scoped threads inside submit_batch keep NVMe QD > 1.
        let io_start = Instant::now();
        {
            use crate::io::engine::{LvOp, LvOpResult};

            let mut ops: Vec<LvOp> = Vec::with_capacity(n);
            let mut op_to_unit: Vec<usize> = Vec::with_capacity(n);
            for i in 0..n {
                if skip[i] {
                    continue;
                }
                ops.push(LvOp::Write {
                    pba: pbas[i].unwrap(),
                    payload: units[i].compressed_data.as_slice(),
                });
                op_to_unit.push(i);
            }

            if !ops.is_empty() {
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
                                results[unit_idx] = Err(crate::error::OnyxError::Io(
                                    std::io::Error::other(format!("IO write failed: {e}")),
                                ));
                                skip[unit_idx] = true;
                            }
                        }
                    }
                    Err(e) => {
                        // Whole submission failed — roll back all live units.
                        for &unit_idx in &op_to_unit {
                            if skip[unit_idx] {
                                continue;
                            }
                            let pba = pbas[unit_idx].unwrap();
                            let blk = alloc_blocks[unit_idx];
                            if blk == 1 {
                                let _ = allocator.free_one(pba);
                            } else {
                                let _ = allocator.free_extent(Extent::new(pba, blk));
                            }
                            results[unit_idx] = Err(crate::error::OnyxError::Io(
                                std::io::Error::other(format!("IO batch submit failed: {e}")),
                            ));
                            skip[unit_idx] = true;
                        }
                    }
                }
            }
        }
        let io_elapsed = io_start.elapsed();
        Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);

        // Phase 4: Build batch values + live positions.
        // Refcount decrements are re-computed inside the lock by atomic_batch_write_multi.
        let meta_start = Instant::now();
        struct UnitMeta {
            batch_values: Vec<(Lba, BlockmapValue)>,
            live_positions: Vec<usize>,
            dedup_registrations: Vec<DedupRegistration>,
        }
        let mut unit_metas: Vec<Option<UnitMeta>> = (0..n).map(|_| None).collect();

        for (i, unit) in units.iter().enumerate() {
            if skip[i] {
                continue;
            }
            let pba = pbas[i].unwrap();
            let live_positions = match Self::live_positions_for_unit(unit, pool) {
                Ok(positions) => positions,
                Err(e) => {
                    if alloc_blocks[i] == 1 {
                        let _ = allocator.free_one(pba);
                    } else {
                        let _ = allocator.free_extent(Extent::new(pba, alloc_blocks[i]));
                    }
                    results[i] = Err(e);
                    skip[i] = true;
                    continue;
                }
            };

            if live_positions.is_empty() {
                unit_metas[i] = Some(UnitMeta {
                    batch_values: Vec::new(),
                    live_positions,
                    dedup_registrations: Vec::new(),
                });
                continue;
            }

            let lbas: Vec<Lba> = live_positions
                .iter()
                .map(|idx| Lba(unit.start_lba.0 + *idx as u64))
                .collect();

            let mut batch_values = Vec::with_capacity(live_positions.len());
            let mut dedup_registrations = Vec::new();

            for j in 0..live_positions.len() {
                let flags = if unit.dedup_skipped {
                    FLAG_DEDUP_SKIPPED
                } else {
                    0
                };
                let blockmap =
                    Self::blockmap_for_unit_position(unit, pba, live_positions[j], 0, flags);
                batch_values.push((lbas[j], blockmap));
            }
            if !unit.dedup_skipped {
                if let Some(ref hashes) = unit.block_hashes {
                    dedup_registrations.reserve(live_positions.len());
                    for &pos in &live_positions {
                        let hash = hashes[pos];
                        if hash == [0u8; 32] {
                            continue;
                        }
                        let blockmap = Self::blockmap_for_unit_position(unit, pba, pos, 0, 0);
                        dedup_registrations.push(Self::dedup_registration(
                            VolumeId(unit.vol_id.clone()),
                            Lba(unit.start_lba.0 + pos as u64),
                            hash,
                            blockmap,
                        ));
                    }
                }
            }
            unit_metas[i] = Some(UnitMeta {
                batch_values,
                live_positions,
                dedup_registrations,
            });
        }

        // Phase 5: ONE combined WriteBatch for all units.
        // old PBA decrements are computed inside the lock by atomic_batch_write_multi.
        let mut vol_ids_owned: Vec<VolumeId> = Vec::new();
        let mut meta_indices: Vec<usize> = Vec::new();

        for (i, unit) in units.iter().enumerate() {
            if skip[i] || unit_metas[i].is_none() {
                continue;
            }
            vol_ids_owned.push(VolumeId(unit.vol_id.clone()));
            meta_indices.push(i);
        }

        // actual_old_pba_meta: returned by atomic_batch_write_multi with accurate data
        let mut actual_old_pba_meta: HashMap<Pba, (u32, u32)> = HashMap::new();

        if !meta_indices.is_empty() {
            let batch_args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> = meta_indices
                .iter()
                .enumerate()
                .map(|(batch_idx, &unit_idx)| {
                    let um = unit_metas[unit_idx].as_ref().unwrap();
                    (
                        &vol_ids_owned[batch_idx],
                        um.batch_values.as_slice(),
                        um.live_positions.len() as u32,
                    )
                })
                .collect();

            let mut dedup_registrations: Vec<DedupRegistration> = Vec::new();
            for &unit_idx in &meta_indices {
                let um = unit_metas[unit_idx].as_ref().unwrap();
                dedup_registrations.extend_from_slice(&um.dedup_registrations);
            }
            match meta.atomic_batch_write_multi_with_dedup(&batch_args, &[]) {
                Ok(returned) => {
                    actual_old_pba_meta = returned;
                    Self::enqueue_dedup_registrations(
                        meta,
                        metrics,
                        _dedup_register_tx,
                        dedup_registrations,
                    );
                }
                Err(e) => {
                    // Entire batch failed — rollback all allocations.
                    for &unit_idx in &meta_indices {
                        let pba = pbas[unit_idx].unwrap();
                        if alloc_blocks[unit_idx] == 1 {
                            let _ = allocator.free_one(pba);
                        } else {
                            let _ = allocator.free_extent(Extent::new(pba, alloc_blocks[unit_idx]));
                        }
                        results[unit_idx] = Err(crate::error::OnyxError::Io(
                            std::io::Error::other(format!("batch write failed: {e}",)),
                        ));
                        skip[unit_idx] = true;
                    }
                }
            }
        }
        let meta_elapsed = meta_start.elapsed();
        Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);

        // Phase 6: Cleanup (free old PBAs) + dedup registration queueing.
        //
        // `actual_old_pba_meta` now contains only PBAs that THIS batch actually
        // drove to refcount 0 (current_rc > 0 before, final_rc == 0 after).
        // PBAs that were already 0 are excluded — another batch/lane already
        // freed them, and double-freeing after re-allocation causes corruption.
        let cleanup_start = Instant::now();
        let empty_live_raw_units: Vec<usize> = meta_indices
            .iter()
            .copied()
            .filter(|&unit_idx| {
                unit_metas[unit_idx]
                    .as_ref()
                    .is_some_and(|um| um.live_positions.is_empty())
            })
            .collect();

        for &unit_idx in &empty_live_raw_units {
            let pba = pbas[unit_idx].unwrap();
            if alloc_blocks[unit_idx] == 1 {
                let _ = allocator.free_one(pba);
            } else {
                let _ = allocator.free_extent(Extent::new(pba, alloc_blocks[unit_idx]));
            }
        }

        for &unit_idx in &meta_indices {
            if skip[unit_idx] {
                continue;
            }
            let um = unit_metas[unit_idx].as_ref().unwrap();
            let unit = &units[unit_idx];
            let pba = pbas[unit_idx].unwrap();
            if um.live_positions.is_empty() {
                continue;
            }

            Self::free_unreferenced_raw_blocks(
                unit,
                pba,
                &um.live_positions,
                allocator,
                "write_units_batch",
            );

            metrics.flush_units_written.fetch_add(1, Ordering::Relaxed);
            metrics
                .flush_unit_bytes
                .fetch_add(unit.compressed_data.len() as u64, Ordering::Relaxed);
        }

        if !actual_old_pba_meta.is_empty() {
            let dead: Vec<(Pba, u32)> = actual_old_pba_meta
                .iter()
                .map(|(pba, (_, blocks))| (*pba, *blocks))
                .collect();
            let _ = cleanup_tx.send(dead);
        }
        Self::record_elapsed(&metrics.flush_writer_cleanup_ns, cleanup_start);

        // Phase 7: Batch mark_flushed (memory-only, fast).
        let mark_start = Instant::now();
        for (i, unit) in units.iter().enumerate() {
            if skip[i] {
                continue;
            }
            for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
            }
        }
        Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);
        let _ = pool.advance_tail_for_shard(shard_idx);

        let total_elapsed = total_start.elapsed();
        Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
        if total_elapsed >= Duration::from_secs(1) {
            tracing::debug!(
                shard = shard_idx,
                units = n,
                live_units = meta_indices.len(),
                total_ms = total_elapsed.as_millis() as u64,
                alloc_ms = alloc_elapsed.as_millis() as u64,
                io_ms = io_elapsed.as_millis() as u64,
                meta_ms = meta_elapsed.as_millis() as u64,
                "writer: slow passthrough batch (>=1s)"
            );
        }
        results
    }
}
