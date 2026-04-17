use super::*;

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
        cleanup_tx: &Sender<Vec<(Pba, u32)>>,
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

            for i in 0..live_positions.len() {
                let flags = if unit.dedup_skipped {
                    FLAG_DEDUP_SKIPPED
                } else {
                    0
                };
                batch_values.push((
                    vol_id.clone(),
                    frag_lbas[i],
                    BlockmapValue {
                        pba: sealed.pba,
                        compression: unit.compression,
                        unit_compressed_size: unit.compressed_data.len() as u32,
                        unit_original_size: unit.original_size,
                        unit_lba_count: unit.lba_count as u16,
                        offset_in_unit: live_positions[i] as u16,
                        crc32: unit.crc32,
                        slot_offset: frag.slot_offset,
                        flags,
                    },
                ));
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
        let actual_old_pba_meta =
            match meta.atomic_batch_write_packed(&batch_values, sealed.pba, total_refcount) {
                Ok(m) => m,
                Err(e) => {
                    allocator.free_one(sealed.pba)?;
                    Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
                    Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                    return Err(e);
                }
            };
        Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);

        if !actual_old_pba_meta.is_empty() {
            let dead: Vec<(Pba, u32)> = actual_old_pba_meta
                .iter()
                .map(|(pba, (_, blocks))| (*pba, *blocks))
                .collect();
            let _ = cleanup_tx.send(dead);
        }

        // Populate dedup index for newly written fragments.
        // Aggregate every fragment's hash → DedupEntry into one Vec and commit
        // them in a single `put_dedup_entries` call, so a packed slot with N
        // fragments costs 1 RocksDB write instead of N.
        let dedup_start = Instant::now();
        let mut dedup_entries: Vec<(ContentHash, DedupEntry)> = Vec::new();
        for frag in &sealed.fragments {
            let Some(ref hashes) = frag.unit.block_hashes else {
                continue;
            };
            let live_positions = Self::live_positions_for_unit(&frag.unit, pool)?;
            for &pos in &live_positions {
                let hash = hashes[pos];
                if hash == [0u8; 32] {
                    continue;
                }
                dedup_entries.push((
                    hash,
                    DedupEntry {
                        pba: sealed.pba,
                        slot_offset: frag.slot_offset,
                        compression: frag.unit.compression,
                        unit_compressed_size: frag.unit.compressed_data.len() as u32,
                        unit_original_size: frag.unit.original_size,
                        unit_lba_count: frag.unit.lba_count as u16,
                        offset_in_unit: pos as u16,
                        crc32: frag.unit.crc32,
                    },
                ));
            }
        }
        if !dedup_entries.is_empty() {
            meta.put_dedup_entries(&dedup_entries)?;
        }
        Self::record_elapsed(&metrics.flush_writer_dedup_index_ns, dedup_start);

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
}
