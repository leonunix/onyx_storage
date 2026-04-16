use super::*;

impl BufferFlusher {
    pub(super) fn coalesce_loop(
        shard_idx: usize,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        tx: &Sender<CoalesceUnit>,
        done_rx: &Receiver<Vec<u64>>,
        running: &AtomicBool,
        in_flight_tracker: &FlusherInFlightTracker,
        metrics: &EngineMetrics,
        max_raw: usize,
        max_lbas: u32,
    ) {
        // in_flight tracks how many pipeline units still reference each seq.
        // A multi-LBA entry split into 2 units → refcount=2 for that seq.
        // Only when refcount hits 0 does the seq leave in_flight.
        let mut in_flight: HashMap<u64, u32> = HashMap::new();

        // Cache per-volume compression to avoid repeated MetaStore lookups.
        let mut vol_compression_cache: HashMap<String, CompressionAlgo> = HashMap::new();
        let vol_compression = |vol_id: &str| -> CompressionAlgo {
            // Can't use cache from closure due to borrow rules — inlined below
            if let Ok(Some(vc)) = meta.get_volume(&crate::types::VolumeId(vol_id.to_string())) {
                vc.compression
            } else {
                CompressionAlgo::None
            }
        };
        let ready_timeout = Duration::from_millis(10);
        let retry_snapshot_interval = Duration::from_millis(100);
        let retry_snapshot_topup_limit = 64usize;
        let mut last_retry_snapshot = Instant::now();

        while running.load(Ordering::Relaxed) {
            // Drain completed seqs from writer feedback — decrement refcounts
            while let Ok(seqs) = done_rx.try_recv() {
                for seq in seqs {
                    if let Some(count) = in_flight.get_mut(&seq) {
                        *count -= 1;
                        if *count == 0 {
                            in_flight.remove(&seq);
                            in_flight_tracker.track_seq_done(seq);
                        }
                    }
                }
            }

            let mut new_entries = Vec::new();
            let mut seen = std::collections::HashSet::new();
            let mut queued_bytes = 0usize;

            // Always give the front of log_order a retry chance first. A single
            // partially flushed seq can otherwise starve behind newer ready work
            // and pin tail reclamation for minutes.
            if let Some(entry) = pool
                .head_stuck_pending_entry_arc_for_shard(shard_idx, Self::HEAD_RETRY_AGE_THRESHOLD)
            {
                let seq = entry.seq;
                if !in_flight.contains_key(&seq)
                    && in_flight_tracker.retry_ready(seq)
                    && seen.insert(seq)
                {
                    queued_bytes =
                        queued_bytes.saturating_add(Self::pending_entry_bytes(entry.as_ref()));
                    new_entries.push(entry);
                }
            }

            if new_entries.is_empty() {
                match pool.recv_ready_timeout_for_shard(shard_idx, ready_timeout) {
                    Ok(seq) => {
                        let _ = Self::try_enqueue_pending_seq(
                            seq,
                            pool,
                            &in_flight,
                            in_flight_tracker,
                            &mut seen,
                            &mut queued_bytes,
                            &mut new_entries,
                        );
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
                }
            }

            while queued_bytes < Self::COALESCE_READY_WINDOW_BYTES {
                let Ok(seq) = pool.try_recv_ready_for_shard(shard_idx) else {
                    break;
                };
                if matches!(
                    Self::try_enqueue_pending_seq(
                        seq,
                        pool,
                        &in_flight,
                        in_flight_tracker,
                        &mut seen,
                        &mut queued_bytes,
                        &mut new_entries,
                    ),
                    EnqueuePendingSeq::WindowFull
                ) {
                    break;
                }
            }

            // Safety net for recovered / retried entries: periodically snapshot the
            // in-memory pending set instead of rescanning the on-disk log on every
            // loop. This must run even while foreground writes keep producing new
            // ready seqs, otherwise payload-less recovered entries that were skipped
            // once under memory pressure can starve indefinitely.
            if last_retry_snapshot.elapsed() >= retry_snapshot_interval
                && queued_bytes < Self::COALESCE_READY_WINDOW_BYTES
            {
                last_retry_snapshot = Instant::now();
                let mut topped_up = 0usize;
                for entry in pool.ready_pending_entries_arc_snapshot_for_shard(shard_idx) {
                    if topped_up >= retry_snapshot_topup_limit
                        || queued_bytes >= Self::COALESCE_READY_WINDOW_BYTES
                    {
                        break;
                    }
                    if matches!(
                        Self::try_enqueue_pending_seq(
                            entry.seq,
                            pool,
                            &in_flight,
                            in_flight_tracker,
                            &mut seen,
                            &mut queued_bytes,
                            &mut new_entries,
                        ),
                        EnqueuePendingSeq::Queued
                    ) {
                        topped_up += 1;
                    }
                }
            }

            if new_entries.is_empty() {
                continue;
            }

            // Build per-volume compression lookup using cache
            for entry in &new_entries {
                vol_compression_cache
                    .entry(entry.vol_id.clone())
                    .or_insert_with(|| vol_compression(&entry.vol_id));
            }
            // Build skip map: already-flushed LBA offsets that the coalescer
            // should not re-include.  Prevents the head-of-line starvation bug
            // where a partially-flushed entry keeps re-coalescing done LBAs.
            let mut skip_offsets: HashMap<u64, std::collections::HashSet<u16>> = HashMap::new();
            for entry in &new_entries {
                if let Some(flushed) = pool.flushed_offsets_for_shard(shard_idx, entry.seq) {
                    if !flushed.is_empty() {
                        skip_offsets.insert(entry.seq, flushed);
                    }
                }
            }

            let cache_ref = &vol_compression_cache;
            let units = coalesce_pending(
                &new_entries,
                max_raw,
                max_lbas,
                &|vid| cache_ref.get(vid).copied().unwrap_or(CompressionAlgo::None),
                &skip_offsets,
            );

            // Payload ownership has been moved into Arc-backed block refs inside
            // the coalesced units, so the pending-entry copy can be evicted
            // immediately without losing the bytes needed by dedup/compress.
            let consumed_seqs: Vec<u64> = new_entries.iter().map(|e| e.seq).collect();
            drop(new_entries);
            pool.evict_hydrated_payloads_for_shard(shard_idx, &consumed_seqs);

            if !units.is_empty() {
                metrics.coalesce_runs.fetch_add(1, Ordering::Relaxed);
                metrics
                    .coalesced_units
                    .fetch_add(units.len() as u64, Ordering::Relaxed);
                metrics.coalesced_lbas.fetch_add(
                    units.iter().map(|u| u.lba_count as u64).sum::<u64>(),
                    Ordering::Relaxed,
                );
                metrics.coalesced_bytes.fetch_add(
                    units.iter().map(|u| u.raw_len() as u64).sum::<u64>(),
                    Ordering::Relaxed,
                );
            }

            // Count how many units reference each seq
            for unit in &units {
                for (seq, _, _) in &unit.seq_lba_ranges {
                    let count = in_flight.entry(*seq).or_insert(0);
                    if *count == 0 {
                        in_flight_tracker.track_seq_start(*seq, &unit.vol_id, unit.vol_created_at);
                    }
                    *count += 1;
                }
            }

            for unit in units {
                if tx.send(unit).is_err() {
                    return;
                }
            }
        }
    }

    pub(super) fn compress_loop(
        rx: &Receiver<CoalesceUnit>,
        tx: &Sender<CompressedUnit>,
        running: &AtomicBool,
        metrics: &EngineMetrics,
    ) {
        while running.load(Ordering::Relaxed) {
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(unit) => {
                    let CoalesceUnit {
                        vol_id,
                        start_lba,
                        lba_count,
                        raw_blocks,
                        compression: algo,
                        vol_created_at,
                        seq_lba_ranges,
                        dedup_skipped,
                        block_hashes,
                        dedup_completion,
                    } = unit;

                    let original_size = raw_blocks.len() * BLOCK_SIZE as usize;
                    let mut raw_data = Vec::with_capacity(original_size);
                    for block in &raw_blocks {
                        raw_data.extend_from_slice(block.bytes());
                    }
                    let (compression_byte, compressed_data) = match algo {
                        CompressionAlgo::None => (0u8, raw_data),
                        _ => {
                            let compressor = create_compressor(algo);
                            let max_out = compressor.max_compressed_size(original_size);
                            let mut compressed_buf = vec![0u8; max_out];
                            match compressor.compress(&raw_data, &mut compressed_buf) {
                                Some(size) => (algo.to_u8(), compressed_buf[..size].to_vec()),
                                None => (0u8, raw_data),
                            }
                        }
                    };
                    metrics.compress_units.fetch_add(1, Ordering::Relaxed);
                    metrics
                        .compress_input_bytes
                        .fetch_add(original_size as u64, Ordering::Relaxed);
                    metrics
                        .compress_output_bytes
                        .fetch_add(compressed_data.len() as u64, Ordering::Relaxed);

                    let crc32 = crc32fast::hash(&compressed_data);

                    let cu = CompressedUnit {
                        vol_id,
                        start_lba,
                        lba_count,
                        original_size: original_size as u32,
                        compressed_data,
                        compression: compression_byte,
                        crc32,
                        vol_created_at,
                        seq_lba_ranges,
                        block_hashes,
                        dedup_skipped,
                        dedup_completion,
                    };

                    if tx.send(cu).is_err() {
                        return;
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    /// Dedup stage: hash 4KB blocks, check dedup index, handle hits inline.
    ///
    /// Seq lifecycle: the coalescer tracks one refcount per original unit. The
    /// dedup stage handles hits directly (metadata update + mark_flushed + done_tx)
    /// and sends only miss sub-units to the compress pipeline. If an original unit
    /// has both hits and misses, the miss sub-units inherit seq_lba_ranges for their
    /// LBA range, and the writer does done_tx for them. If ALL blocks are hits,
    /// the dedup worker does done_tx for the whole unit.
    ///
    /// To avoid double-counting seqs in done_tx: each seq from the original unit
    /// is sent to done_tx exactly once — either by the dedup worker (for hit-only seqs)
    /// or by the writer (for seqs that have miss blocks flowing through the pipeline).
    pub(super) fn dedup_loop(
        shard_idx: usize,
        rx: &Receiver<CoalesceUnit>,
        miss_tx: &Sender<CoalesceUnit>,
        meta: &MetaStore,
        pool: &WriteBufferPool,
        lifecycle: &VolumeLifecycleManager,
        _allocator: &SpaceAllocator,
        done_tx: &Sender<Vec<u64>>,
        running: &AtomicBool,
        skip_threshold_pct: u8,
        metrics: &EngineMetrics,
        cleanup_tx: &Sender<Vec<(Pba, u32)>>,
    ) {
        while running.load(Ordering::Relaxed) {
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(mut unit) => {
                    // Backpressure: skip dedup if this shard's buffer is filling up
                    if pool.fill_percentage_for_shard(shard_idx) > skip_threshold_pct as u8 {
                        unit.dedup_skipped = true;
                        metrics.dedup_skipped_units.fetch_add(1, Ordering::Relaxed);
                        if miss_tx.send(unit).is_err() {
                            return;
                        }
                        continue;
                    }

                    // Split into 4KB blocks, hash each, check dedup index
                    let lba_count = unit.lba_count as usize;
                    let mut is_hit: Vec<bool> = vec![false; lba_count];
                    let mut all_hashes: Vec<ContentHash> = Vec::with_capacity(lba_count);
                    // Collect hit info for processing
                    let mut hit_infos: Vec<(usize, BlockmapValue, ContentHash)> = Vec::new();

                    for i in 0..lba_count {
                        let Some(block) = unit.raw_blocks.get(i) else {
                            all_hashes.push([0u8; 32]);
                            continue;
                        };
                        let block_data = block.bytes();
                        let hash: ContentHash = *blake3::hash(block_data).as_bytes();
                        all_hashes.push(hash);

                        // Look up dedup index
                        metrics.dedup_lookup_ops.fetch_add(1, Ordering::Relaxed);
                        let lookup_start = Instant::now();
                        match meta.get_dedup_entry(&hash) {
                            Ok(Some(entry)) => {
                                Self::record_elapsed(&metrics.dedup_lookup_ns, lookup_start);
                                metrics.dedup_live_check_ops.fetch_add(1, Ordering::Relaxed);
                                let live_check_start = Instant::now();
                                match meta.dedup_entry_is_live(&hash, &entry) {
                                    Ok(true) => {
                                        Self::record_elapsed(
                                            &metrics.dedup_live_check_ns,
                                            live_check_start,
                                        );
                                        is_hit[i] = true;
                                        hit_infos.push((i, entry.to_blockmap_value(), hash));
                                    }
                                    Ok(false) => {
                                        Self::record_elapsed(
                                            &metrics.dedup_live_check_ns,
                                            live_check_start,
                                        );
                                        metrics
                                            .dedup_stale_index_entries
                                            .fetch_add(1, Ordering::Relaxed);
                                        let stale_delete_start = Instant::now();
                                        let _ = meta.delete_dedup_index(&hash);
                                        Self::record_elapsed(
                                            &metrics.dedup_stale_delete_ns,
                                            stale_delete_start,
                                        );
                                    }
                                    Err(e) => {
                                        Self::record_elapsed(
                                            &metrics.dedup_live_check_ns,
                                            live_check_start,
                                        );
                                        tracing::warn!(
                                            error = %e,
                                            pba = entry.pba.0,
                                            "dedup worker: failed to validate dedup entry liveness"
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                Self::record_elapsed(&metrics.dedup_lookup_ns, lookup_start);
                                tracing::warn!(error = %e, "dedup worker: failed to read dedup entry");
                            }
                            Ok(None) => {
                                Self::record_elapsed(&metrics.dedup_lookup_ns, lookup_start);
                            }
                        }
                    }

                    // Process hits in a single batch for this CoalesceUnit.
                    // Phase A: pre-filter stale hits (outside lock).
                    let mut successful_hit_indices: Vec<usize> = Vec::new();
                    let mut valid_hits: Vec<(usize, BlockmapValue, ContentHash)> = Vec::new();
                    for (i, existing_value, hash) in &hit_infos {
                        let lba = Lba(unit.start_lba.0 + *i as u64);
                        let vol_id_str = &unit.vol_id;

                        // Staleness guard: if a newer write superseded this LBA
                        // in the buffer, skip the dedup hit to avoid overwriting
                        // the newer blockmap entry.
                        let latest_seq = Self::latest_seq_for_lba(&unit.seq_lba_ranges, lba);
                        if !pool.is_latest_lba_seq(vol_id_str, lba, latest_seq, unit.vol_created_at)
                        {
                            successful_hit_indices.push(*i);
                            continue;
                        }
                        valid_hits.push((*i, *existing_value, *hash));
                    }

                    // Phase B: batch commit all valid hits under one lifecycle
                    // read lock + one refcount_lock acquisition.
                    if !valid_hits.is_empty() {
                        let vol_id_str = &unit.vol_id;
                        let vol_id = VolumeId(vol_id_str.clone());

                        metrics
                            .dedup_hit_commit_ops
                            .fetch_add(valid_hits.len() as u64, Ordering::Relaxed);
                        let hit_commit_start = Instant::now();

                        let batch_result = lifecycle.with_read_lock(
                            vol_id_str,
                            || -> OnyxResult<(Vec<(usize, DedupHitResult)>, HashMap<Pba, u32>)> {
                                // Generation check (one call for entire batch)
                                let should_discard = match meta.get_volume(&vol_id)? {
                                    None => true,
                                    Some(vc) if vc.created_at != unit.vol_created_at => true,
                                    _ => false,
                                };
                                if should_discard {
                                    // Treat all as discarded (successfully handled)
                                    return Ok((
                                        valid_hits
                                            .iter()
                                            .map(|(i, _, _)| (*i, DedupHitResult::Accepted(None)))
                                            .collect(),
                                        HashMap::new(),
                                    ));
                                }

                                // Build batch input, filtering out test-injected failures.
                                let mut batch_input: Vec<(Lba, BlockmapValue, ContentHash)> =
                                    Vec::with_capacity(valid_hits.len());
                                let mut input_map: Vec<usize> =
                                    Vec::with_capacity(valid_hits.len());
                                for &(i, ref existing_value, ref hash) in &valid_hits {
                                    let lba = Lba(unit.start_lba.0 + i as u64);
                                    if maybe_inject_dedup_hit_failure(vol_id_str, lba).is_ok() {
                                        batch_input.push((lba, *existing_value, *hash));
                                        input_map.push(i);
                                    }
                                }

                                let (batch_results, batch_newly_zeroed) =
                                    meta.atomic_batch_dedup_hits(&vol_id, &batch_input)?;

                                let mut combined: Vec<(usize, DedupHitResult)> =
                                    Vec::with_capacity(valid_hits.len());
                                for (j, result) in batch_results.into_iter().enumerate() {
                                    combined.push((input_map[j], result));
                                }
                                // Injection-failed hits: demote to Rejected so they
                                // get is_hit[i]=false and go through the write path.
                                for &(i, _, _) in &valid_hits {
                                    if !input_map.contains(&i) {
                                        combined.push((i, DedupHitResult::Rejected));
                                    }
                                }
                                Ok((combined, batch_newly_zeroed))
                            },
                        );
                        Self::record_elapsed(&metrics.dedup_hit_commit_ns, hit_commit_start);

                        // Phase C: process results
                        match batch_result {
                            Ok((results, newly_zeroed)) => {
                                for (i, result) in &results {
                                    match result {
                                        DedupHitResult::Accepted(_) => {
                                            successful_hit_indices.push(*i);
                                        }
                                        DedupHitResult::Rejected => {
                                            metrics
                                                .dedup_hit_failures
                                                .fetch_add(1, Ordering::Relaxed);
                                            is_hit[*i] = false;
                                            tracing::warn!(
                                                vol = unit.vol_id,
                                                lba = unit.start_lba.0 + *i as u64,
                                                "dedup worker: hit rejected (target PBA freed), demoting to miss"
                                            );
                                        }
                                    }
                                }
                                // Phase D: offload dead PBA cleanup to async thread.
                                if !newly_zeroed.is_empty() {
                                    let dead: Vec<(Pba, u32)> = newly_zeroed.into_iter().collect();
                                    let _ = cleanup_tx.send(dead);
                                }
                            }
                            Err(e) => {
                                // Entire batch failed — demote all valid_hits to misses.
                                metrics
                                    .dedup_hit_failures
                                    .fetch_add(valid_hits.len() as u64, Ordering::Relaxed);
                                for (i, _, _) in &valid_hits {
                                    is_hit[*i] = false;
                                }
                                tracing::error!(
                                    vol = unit.vol_id,
                                    count = valid_hits.len(),
                                    error = %e,
                                    "dedup worker: batch hit failed, demoting all to miss"
                                );
                            }
                        }
                    }

                    // Only mark_flushed for hits that actually succeeded
                    if !successful_hit_indices.is_empty() {
                        for i in &successful_hit_indices {
                            let lba = Lba(unit.start_lba.0 + *i as u64);
                            for (seq, range_start, range_count) in &unit.seq_lba_ranges {
                                if lba.0 >= range_start.0
                                    && lba.0 < range_start.0 + *range_count as u64
                                {
                                    let _ = pool.mark_flushed(*seq, lba, 1);
                                }
                            }
                        }
                        let _ = pool.advance_tail_for_shard(shard_idx);
                    }

                    // Recheck has_misses after potential demotions
                    let has_misses = is_hit.iter().any(|h| !h);
                    metrics
                        .dedup_hits
                        .fetch_add(successful_hit_indices.len() as u64, Ordering::Relaxed);
                    metrics.dedup_misses.fetch_add(
                        is_hit.iter().filter(|hit| !**hit).count() as u64,
                        Ordering::Relaxed,
                    );
                    if !has_misses {
                        // All blocks were hits — send done_tx for the original unit's seqs
                        let seqs: Vec<u64> =
                            unit.seq_lba_ranges.iter().map(|(s, _, _)| *s).collect();
                        let _ = done_tx.send(seqs);
                        continue;
                    }

                    // Re-coalesce consecutive miss blocks into CoalesceUnits.
                    // Count miss sub-units first to create a shared DedupCompletion.
                    let mut miss_ranges: Vec<(usize, usize)> = Vec::new();
                    let mut miss_start: Option<usize> = None;
                    for i in 0..lba_count {
                        if !is_hit[i] {
                            if miss_start.is_none() {
                                miss_start = Some(i);
                            }
                        } else if let Some(start) = miss_start.take() {
                            miss_ranges.push((start, i));
                        }
                    }
                    if let Some(start) = miss_start {
                        miss_ranges.push((start, lba_count));
                    }

                    // Create shared countdown: all miss sub-units share this.
                    // The LAST sub-unit to complete sends done_tx with the
                    // original unit's full seq list.
                    let all_seqs: Vec<u64> =
                        unit.seq_lba_ranges.iter().map(|(s, _, _)| *s).collect();
                    let completion = crate::buffer::pipeline::DedupCompletion::new(
                        miss_ranges.len() as u32,
                        all_seqs,
                    );

                    let mut miss_units: Vec<CoalesceUnit> = Vec::new();
                    for (start, end) in &miss_ranges {
                        miss_units.push(Self::build_miss_unit(
                            &unit,
                            *start,
                            *end,
                            &all_hashes,
                            Some(completion.clone()),
                        ));
                    }

                    // Send miss units to compress stage
                    for mu in miss_units {
                        if miss_tx.send(mu).is_err() {
                            return;
                        }
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    /// Build a CoalesceUnit from a contiguous range of miss blocks [start, end).
    pub(super) fn build_miss_unit(
        original: &CoalesceUnit,
        start_idx: usize,
        end_idx: usize,
        hashes: &[ContentHash],
        dedup_completion: Option<Arc<crate::buffer::pipeline::DedupCompletion>>,
    ) -> CoalesceUnit {
        let start_lba = Lba(original.start_lba.0 + start_idx as u64);
        let lba_count = (end_idx - start_idx) as u32;
        let raw_blocks = original.raw_blocks[start_idx..end_idx].to_vec();

        // Build seq_lba_ranges for the sub-range
        let mut seq_lba_ranges = Vec::new();
        for i in start_idx..end_idx {
            let lba = Lba(original.start_lba.0 + i as u64);
            for (seq, range_start, range_count) in &original.seq_lba_ranges {
                if lba.0 >= range_start.0 && lba.0 < range_start.0 + *range_count as u64 {
                    // Use add_seq_lba logic: extend or start new range
                    if let Some(existing) = seq_lba_ranges.iter_mut().find(
                        |(s, start, count): &&mut (u64, Lba, u32)| {
                            *s == *seq && start.0 + *count as u64 == lba.0
                        },
                    ) {
                        existing.2 += 1;
                    } else {
                        seq_lba_ranges.push((*seq, lba, 1));
                    }
                    // Don't break: multiple seqs can reference the same LBA
                    // (e.g., overwrite dedup in coalescer keeps all seqs)
                }
            }
        }

        let block_hashes_slice = hashes[start_idx..end_idx].to_vec();
        CoalesceUnit {
            vol_id: original.vol_id.clone(),
            start_lba,
            lba_count,
            raw_blocks,
            compression: original.compression,
            vol_created_at: original.vol_created_at,
            seq_lba_ranges,
            dedup_skipped: false,
            block_hashes: Some(block_hashes_slice),
            dedup_completion,
        }
    }
}
