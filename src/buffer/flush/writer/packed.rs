use super::*;

struct PackedSlotMeta {
    batch_values: Vec<(VolumeId, Lba, BlockmapValue)>,
    all_seq_lba_ranges: Vec<(u64, Lba, u32)>,
    /// First-occurrence (hash, blockmap) pairs for the candidate
    /// cache; populated alongside batch_values.
    fresh_dedup_pairs: Vec<(ContentHash, BlockmapValue)>,
}

fn merge_dead_pbas(dst: &mut HashMap<Pba, RemapCleanup>, src: HashMap<Pba, RemapCleanup>) {
    for (pba, cleanup) in src {
        dst.entry(pba)
            .and_modify(|entry| entry.merge(cleanup.clone()))
            .or_insert(cleanup);
    }
}

fn commit_packed_meta_batch(
    batch_slots: &mut Vec<usize>,
    batch_lbas: &mut usize,
    sealed_slots: &[SealedSlot],
    slot_metas: &mut [Option<PackedSlotMeta>],
    meta: &MetaStore,
    allocator: &SpaceAllocator,
    results: &mut [OnyxResult<()>],
    actual_old_pba_meta: &mut HashMap<Pba, RemapCleanup>,
    candidate: &crate::dedup::CandidateCache,
) -> bool {
    if batch_slots.is_empty() {
        return false;
    }

    let mut combined_batch_values: Vec<(VolumeId, Lba, BlockmapValue)> =
        Vec::with_capacity(*batch_lbas);
    let mut combined_fresh_dedup: Vec<(ContentHash, BlockmapValue)> = Vec::new();
    for &slot_idx in batch_slots.iter() {
        if let Some(ref sm) = slot_metas[slot_idx] {
            combined_batch_values.extend_from_slice(&sm.batch_values);
            combined_fresh_dedup.extend_from_slice(&sm.fresh_dedup_pairs);
        }
    }
    match meta.atomic_batch_write_packed_with_dedup(
        &combined_batch_values,
        sealed_slots[batch_slots[0]].pba,
        0,
        &[],
    ) {
        Ok(dead) => {
            merge_dead_pbas(actual_old_pba_meta, dead);
            candidate.insert_many(&combined_fresh_dedup);
        }
        Err(e) => {
            for &slot_idx in batch_slots.iter() {
                if slot_metas[slot_idx].is_some() {
                    let _ = allocator.free_one(sealed_slots[slot_idx].pba);
                    results[slot_idx] = Err(crate::error::OnyxError::Io(std::io::Error::other(
                        format!("packed-slot batch metadata commit failed: {e}"),
                    )));
                    slot_metas[slot_idx] = None;
                }
            }
        }
    }

    batch_slots.clear();
    *batch_lbas = 0;
    true
}

impl BufferFlusher {
    pub(in crate::buffer::flush) fn write_packed_slot(
        shard_idx: usize,
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

        // Under lifecycle read locks: check generation, build batch, IO, commit

        // Build blockmap entries.
        // Refcount decrements are re-computed inside the lock by atomic_batch_write_packed.
        let mut batch_values: Vec<(VolumeId, Lba, BlockmapValue)> = Vec::new();
        // First-occurrence (hash, blockmap) pairs to insert into the
        // RAM candidate cache after the metadata commit succeeds.
        // These do *not* go into dedup_index — promote-on-verified-hit
        // moves them there only when a duplicate is later confirmed
        // by LV3 byte-compare in the dedup worker.
        let mut fresh_dedup_pairs: Vec<(ContentHash, BlockmapValue)> = Vec::new();
        let mut total_refcount: u32 = 0;
        let mut all_seq_lba_ranges: Vec<(u64, Lba, u32)> = Vec::new();
        let mut any_discarded = false;

        for frag in &sealed.fragments {
            let unit = &frag.unit;
            let vol_id = VolumeId(unit.vol_id.clone());

            // Lifecycle check: verify volume still exists and generation matches
            let should_discard = match meta.get_volume(&vol_id)? {
                None => true,
                Some(vc) if unit.vol_created_at != 0 && vc.created_at != unit.vol_created_at => {
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

            // Build the per-LBA BlockmapValue once and use it for both
            // the metadata commit and the candidate-cache insert. The
            // commit batch carries DEDUP_SKIPPED for skipped units;
            // candidate inserts use the same BlockmapValue with flags
            // forced to 0 (a cache entry is not the source of truth
            // for skip state).
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
                    }
                }
            }
            total_refcount += live_positions.len() as u32;
            all_seq_lba_ranges.extend(unit.seq_lba_ranges.iter().cloned());
        }

        // If all fragments were discarded, free the slot PBA
        if batch_values.is_empty() {
            allocator.free_one(sealed.pba)?;
            let _ = pool.advance_tail_for_shard(shard_idx);
            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            return Ok(());
        }

        let io_start = Instant::now();
        if let Err(e) =
            maybe_inject_test_failure_packed(&sealed.fragments, FlushFailStage::BeforeIoWrite)
        {
            allocator.free_one(sealed.pba)?;
            Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);
            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            return Err(e);
        }

        // Write the 4KB slot data to LV3
        if let Err(e) = io_engine.write_blocks(sealed.pba, &sealed.data) {
            allocator.free_one(sealed.pba)?;
            Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);
            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            return Err(e);
        }
        Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);

        let meta_start = Instant::now();

        if let Err(e) =
            maybe_inject_test_failure_packed(&sealed.fragments, FlushFailStage::BeforeMetaWrite)
        {
            allocator.free_one(sealed.pba)?;
            Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            return Err(e);
        }
        maybe_pause_before_packed_meta_write(&sealed.fragments)?;

        // Metadata commit — old PBA decrements re-computed inside the lock
        let actual_old_pba_meta = match meta.atomic_batch_write_packed_with_dedup(
            &batch_values,
            sealed.pba,
            total_refcount,
            &[],
        ) {
            Ok(m) => m,
            Err(e) => {
                allocator.free_one(sealed.pba)?;
                Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                return Err(e);
            }
        };
        Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
        candidate.insert_many(&fresh_dedup_pairs);

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

        // Mark entries flushed
        let mark_start = Instant::now();
        for (seq, lba_start, lba_count) in &all_seq_lba_ranges {
            if let Err(e) = pool.mark_flushed(*seq, *lba_start, *lba_count) {
                tracing::warn!(seq, error = %e, "failed to mark entry flushed (packed)");
            }
        }
        Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);

        tracing::debug!(
            pba = sealed.pba.0,
            fragments = sealed.fragments.len(),
            total_lbas = total_refcount,
            discarded = any_discarded,
            "flushed packed slot"
        );

        Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
        Ok(())
    }

    /// Batched counterpart of [`write_packed_slot`]. Folds N sealed
    /// slots into:
    /// - one set of lifecycle read locks (sorted union of all touched
    ///   volumes), held across the whole batch;
    /// - one batched IO submit (`io_engine.submit_batch`) so the device
    ///   queue depth grows with the batch instead of one write at a time;
    /// - **one** metadata transaction covering every surviving slot's L2P,
    ///   refcount, and dedup index entries — the metadb tx auto-derives
    ///   refcount deltas from the L2pRemap ops, so concatenating batches
    ///   from different slots is equivalent to issuing them serially.
    ///
    /// Soak shows ~1 ms per metadb commit fixed cost (WAL fsync barrier
    /// + per-shard apply lane scheduling); a sealed-slot batch of 10
    /// drops 9 commits → ~10x throughput in the writer's metadata phase.
    ///
    /// Returns one `OnyxResult<()>` per input slot. Failure semantics
    /// per slot:
    /// - IO failure → that slot's PBA freed, slot dropped from the
    ///   metadata commit, surviving slots still committed.
    /// - Combined metadata commit failure → every surviving slot's PBA
    ///   freed, all marked failed (caller queues whole-slot retries).
    /// - Dedup put failure → metadata commit fails and every surviving slot's
    ///   PBA is rolled back.
    pub(in crate::buffer::flush) fn write_packed_slots_batch(
        _shard_idx: usize,
        sealed_slots: &[SealedSlot],
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        metrics: &EngineMetrics,
        cleanup_tx: &Sender<CleanupBatch>,
        candidate: &crate::dedup::CandidateCache,
        packed_meta_batch_max_lbas: usize,
    ) -> Vec<OnyxResult<()>> {
        if sealed_slots.is_empty() {
            return Vec::new();
        }
        let packed_meta_batch_max_lbas = packed_meta_batch_max_lbas.max(1);
        let total_start = Instant::now();
        let n = sealed_slots.len();
        let mut results: Vec<OnyxResult<()>> = (0..n).map(|_| Ok(())).collect();

        // Lifecycle locks: union of every fragment's vol_id, sorted to
        // avoid deadlock with concurrent batches.
        let mut vol_ids: Vec<String> = sealed_slots
            .iter()
            .flat_map(|s| s.fragments.iter().map(|f| f.unit.vol_id.clone()))
            .collect();
        vol_ids.sort();
        vol_ids.dedup();
        let locks: Vec<_> = vol_ids.iter().map(|vid| lifecycle.get_lock(vid)).collect();
        let _guards: Vec<_> = locks.iter().map(|l| l.read().unwrap()).collect();

        let mut slot_metas: Vec<Option<PackedSlotMeta>> = (0..n).map(|_| None).collect();

        // Phase 1: per-slot validation + build batch_values + dedup registrations.
        // A slot whose every fragment is stale (volume deleted / version
        // mismatch / no live positions) is dropped here and its PBA freed.
        for (i, sealed) in sealed_slots.iter().enumerate() {
            let mut batch_values: Vec<(VolumeId, Lba, BlockmapValue)> = Vec::new();
            let mut all_seq_lba_ranges: Vec<(u64, Lba, u32)> = Vec::new();
            let mut fresh_dedup_pairs: Vec<(ContentHash, BlockmapValue)> = Vec::new();

            for frag in &sealed.fragments {
                let unit = &frag.unit;
                let vol_id = VolumeId(unit.vol_id.clone());

                let should_discard = match meta.get_volume(&vol_id) {
                    Ok(None) => true,
                    Ok(Some(vc))
                        if unit.vol_created_at != 0 && vc.created_at != unit.vol_created_at =>
                    {
                        true
                    }
                    Ok(_) => false,
                    Err(_) => false,
                };

                if should_discard {
                    metrics.flush_stale_discards.fetch_add(1, Ordering::Relaxed);
                    for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                        let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
                    }
                    continue;
                }

                let live_positions = match Self::live_positions_for_unit(unit, pool) {
                    Ok(p) => p,
                    Err(e) => {
                        // Fragment scan failed — abandon this whole slot's
                        // commit so we don't write a half-built batch_values
                        // entry. Mark and skip.
                        results[i] = Err(e);
                        batch_values.clear();
                        all_seq_lba_ranges.clear();
                        fresh_dedup_pairs.clear();
                        break;
                    }
                };
                if live_positions.is_empty() {
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
                for (j, pos) in live_positions.iter().copied().enumerate() {
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
                    batch_values.push((vol_id.clone(), frag_lbas[j], blockmap));
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
                        }
                    }
                }
                all_seq_lba_ranges.extend(unit.seq_lba_ranges.iter().cloned());
            }

            if batch_values.is_empty() {
                // Either every fragment was stale or live_positions_for_unit
                // failed. Free the PBA so the allocator reclaims it. If it
                // was a hard error we already recorded that in `results[i]`.
                let _ = allocator.free_one(sealed.pba);
                continue;
            }

            slot_metas[i] = Some(PackedSlotMeta {
                batch_values,
                all_seq_lba_ranges,
                fresh_dedup_pairs,
            });
        }

        // Phase 2: batched IO writes — one submit_batch keeps the NVMe
        // queue full instead of one fsync-style write per slot.
        let io_start = Instant::now();
        {
            use crate::io::engine::{LvOp, LvOpResult};
            let mut ops: Vec<LvOp> = Vec::new();
            let mut op_to_slot: Vec<usize> = Vec::new();
            for i in 0..n {
                if slot_metas[i].is_none() {
                    continue;
                }
                ops.push(LvOp::Write {
                    pba: sealed_slots[i].pba,
                    payload: sealed_slots[i].data.as_slice(),
                });
                op_to_slot.push(i);
            }
            if !ops.is_empty() {
                match io_engine.submit_batch(ops, false) {
                    Ok(write_results) => {
                        for (idx, r) in write_results.into_iter().enumerate() {
                            let slot_idx = op_to_slot[idx];
                            if let LvOpResult::Write(Err(e)) = r {
                                let _ = allocator.free_one(sealed_slots[slot_idx].pba);
                                results[slot_idx] =
                                    Err(crate::error::OnyxError::Io(std::io::Error::other(
                                        format!("packed-slot batch IO write failed: {e}"),
                                    )));
                                slot_metas[slot_idx] = None;
                            }
                        }
                    }
                    Err(e) => {
                        for &slot_idx in &op_to_slot {
                            if slot_metas[slot_idx].is_none() {
                                continue;
                            }
                            let _ = allocator.free_one(sealed_slots[slot_idx].pba);
                            results[slot_idx] =
                                Err(crate::error::OnyxError::Io(std::io::Error::other(format!(
                                    "packed-slot batch IO submit failed: {e}"
                                ))));
                            slot_metas[slot_idx] = None;
                        }
                    }
                }
            }
        }
        let io_elapsed = io_start.elapsed();
        Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);

        // Phase 3: bounded metadata batches over the surviving slots'
        // blockmap entries. Keep each WAL record safely below the record
        // limit while still amortising the fixed commit cost across many
        // slots. Dedup miss registrations are folded into the same
        // transaction so we avoid a second blockmap validation pass and a
        // second WAL/apply round for every miss batch.
        let meta_start = Instant::now();
        let mut actual_old_pba_meta: HashMap<Pba, RemapCleanup> = HashMap::new();
        let mut batch_slots: Vec<usize> = Vec::new();
        let mut batch_lbas = 0usize;
        let mut meta_commits = 0usize;
        let mut meta_lbas = 0usize;

        for i in 0..n {
            let Some(ref sm) = slot_metas[i] else {
                continue;
            };
            let slot_lbas = sm.batch_values.len();
            if !batch_slots.is_empty()
                && batch_lbas.saturating_add(slot_lbas) > packed_meta_batch_max_lbas
            {
                if commit_packed_meta_batch(
                    &mut batch_slots,
                    &mut batch_lbas,
                    sealed_slots,
                    &mut slot_metas,
                    meta,
                    allocator,
                    &mut results,
                    &mut actual_old_pba_meta,
                    candidate,
                ) {
                    meta_commits += 1;
                }
            }
            batch_slots.push(i);
            batch_lbas += slot_lbas;
            meta_lbas += slot_lbas;
            if slot_lbas > packed_meta_batch_max_lbas {
                if commit_packed_meta_batch(
                    &mut batch_slots,
                    &mut batch_lbas,
                    sealed_slots,
                    &mut slot_metas,
                    meta,
                    allocator,
                    &mut results,
                    &mut actual_old_pba_meta,
                    candidate,
                ) {
                    meta_commits += 1;
                }
            }
        }
        if commit_packed_meta_batch(
            &mut batch_slots,
            &mut batch_lbas,
            sealed_slots,
            &mut slot_metas,
            meta,
            allocator,
            &mut results,
            &mut actual_old_pba_meta,
            candidate,
        ) {
            meta_commits += 1;
        }
        if !actual_old_pba_meta.is_empty() {
            let _ = cleanup_tx.send(actual_old_pba_meta.into_values().collect());
        }
        let meta_elapsed = meta_start.elapsed();
        Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);

        // Phase 4: counters + mark_flushed per surviving slot.
        let mark_start = Instant::now();
        for i in 0..n {
            let Some(sm) = slot_metas[i].as_ref() else {
                continue;
            };
            let sealed = &sealed_slots[i];
            metrics
                .flush_packed_slots_written
                .fetch_add(1, Ordering::Relaxed);
            metrics
                .flush_packed_fragments_written
                .fetch_add(sealed.fragments.len() as u64, Ordering::Relaxed);
            metrics
                .flush_packed_bytes
                .fetch_add(sealed.data.len() as u64, Ordering::Relaxed);
            for (seq, lba_start, lba_count) in &sm.all_seq_lba_ranges {
                if let Err(e) = pool.mark_flushed(*seq, *lba_start, *lba_count) {
                    tracing::warn!(seq, error = %e, "failed to mark entry flushed (packed batch)");
                }
            }
        }
        Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);
        let total_elapsed = total_start.elapsed();
        Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
        if total_elapsed >= Duration::from_secs(1) {
            let surviving = slot_metas.iter().filter(|s| s.is_some()).count();
            match meta.memory_stats() {
                Ok(meta_stats) => {
                    tracing::debug!(
                        slots = n,
                        surviving,
                        meta_commits,
                        meta_lbas,
                        meta_batch_lba_limit = packed_meta_batch_max_lbas,
                        total_ms = total_elapsed.as_millis() as u64,
                        io_ms = io_elapsed.as_millis() as u64,
                        meta_ms = meta_elapsed.as_millis() as u64,
                        metadb_last_applied_lsn = meta_stats.last_applied_lsn,
                        metadb_high_water_pages = meta_stats.high_water_pages,
                        metadb_commit_max_ms = meta_stats.commit_total_max_us / 1_000,
                        metadb_commit_apply_wait_max_ms =
                            meta_stats.commit_apply_wait_max_us / 1_000,
                        metadb_commit_apply_gate_wait_max_ms =
                            meta_stats.commit_apply_gate_wait_max_us / 1_000,
                        metadb_commit_apply_max_ms = meta_stats.commit_apply_max_us / 1_000,
                        metadb_wal_write_max_ms = meta_stats.wal_write_max_us / 1_000,
                        metadb_wal_fsync_max_ms = meta_stats.wal_fsync_max_us / 1_000,
                        metadb_wal_batch_records_max = meta_stats.wal_batch_records_max,
                        metadb_pending_dispatch = meta_stats.pending_dispatch,
                        metadb_pending_dedup_lane_q = meta_stats.pending_dedup_lane_queue,
                        metadb_pending_l2p_apply_q = meta_stats.pending_l2p_apply_queue,
                        metadb_pending_l2p_dirty = meta_stats.pending_l2p_pagebuf_dirty,
                        metadb_pending_rc_apply_q = meta_stats.pending_rc_apply_queue,
                        metadb_pending_rc_dirty = meta_stats.pending_rc_pagebuf_dirty,
                        metadb_flush_total_max_ms = meta_stats.flush_total_max_us / 1_000,
                        metadb_flush_io_max_ms = meta_stats.flush_io_max_us / 1_000,
                        metadb_flush_install_max_ms = meta_stats.flush_install_max_us / 1_000,
                        metadb_flush_reclaim_max_ms = meta_stats.flush_reclaim_max_us / 1_000,
                        "writer: slow packed-slot batch (>=1s)"
                    );
                }
                Err(e) => {
                    tracing::debug!(
                        slots = n,
                        surviving,
                        meta_commits,
                        meta_lbas,
                        meta_batch_lba_limit = packed_meta_batch_max_lbas,
                        total_ms = total_elapsed.as_millis() as u64,
                        io_ms = io_elapsed.as_millis() as u64,
                        meta_ms = meta_elapsed.as_millis() as u64,
                        metadb_stats_error = %e,
                        "writer: slow packed-slot batch (>=1s)"
                    );
                }
            }
        }

        results
    }
}
