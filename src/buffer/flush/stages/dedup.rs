use super::*;

struct PreparedDedupUnit {
    unit: CoalesceUnit,
    is_hit: Vec<bool>,
    all_hashes: Vec<ContentHash>,
    lookup_indices: Vec<usize>,
    successful_hit_indices: Vec<usize>,
    zero_indices: Vec<usize>,
    /// Hits confirmed by `lookup_dedup_hits` (dedup_index source) or
    /// `candidate_lookup_pass` (candidate-cache source) AND surviving
    /// `verify_prepared_dedup_hits` (LV3 byte-compare). Each entry is
    /// `(lba_index_in_unit, blockmap_value, hash)`.
    valid_hits: Vec<(usize, BlockmapValue, ContentHash)>,
    /// Candidate-cache-sourced hits that, after verify, need to be
    /// promoted into the persistent `dedup_index`.
    /// Each entry's `lba_index_in_unit` matches a `valid_hits` entry;
    /// dedup_index-sourced hits do **not** appear here because they
    /// are already in the persistent layer.
    promote_candidates: Vec<(usize, ContentHash, DedupEntry)>,
    /// Persistent dedup_index hits that failed byte-verify. These are
    /// not deleted immediately: the LBA is written as a fresh miss,
    /// and the writer compare-puts the index to the fresh mapping
    /// only if it still points at this stale old entry.
    stale_index_repairs: Vec<Option<DedupEntry>>,
    /// Atomic quarantine-check + physical pins taken when a dedup target is
    /// copied out of either lookup layer. Held until the remap commit response
    /// is consumed, closing both lookup-to-pin and quarantine-publish races.
    target_guards: Vec<PbaHazardGuard>,
    /// One bit per logical block so a persistent-index reject followed by the
    /// same candidate-cache reject is counted once, not twice.
    dedup_target_rejected: Vec<bool>,
}

type DedupHitChunkEntry = (usize, usize, Lba, BlockmapValue, ContentHash);

struct PendingDedupHitChunk {
    chunk: Vec<DedupHitChunkEntry>,
    promote_hashes: Vec<ContentHash>,
    claimed_promotes: Vec<ContentHash>,
    response_rx: Receiver<writer::DedupHitCommitResponse>,
    batch_len: usize,
    vol_id_str: String,
}

struct PendingPreparedDedupBatch {
    prepared: Vec<PreparedDedupUnit>,
    chunks: Vec<PendingDedupHitChunk>,
}

impl BufferFlusher {
    pub(in crate::buffer::flush) fn dedup_loop(
        shard_idx: usize,
        rx: &Receiver<CoalesceUnit>,
        miss_tx: &Sender<CoalesceUnit>,
        meta: &MetaStore,
        pool: &WriteBufferPool,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        done_tx: &Sender<Vec<u64>>,
        _running: &AtomicBool,
        skip_threshold_pct: u8,
        pending_skip_threshold_entries: u64,
        metrics: &EngineMetrics,
        cleanup_tx: &Sender<CleanupBatch>,
        candidate: &crate::dedup::CandidateCache,
        read_pool: Option<&crate::io::read_pool::ReadPool>,
        commit_worker_txs: &[Sender<writer::CommitJob>],
        commit_workers_per_volume: usize,
        commit_pipeline_depth: usize,
    ) {
        let pipeline_depth = commit_pipeline_depth.max(1);
        let mut pending_batches: VecDeque<PendingPreparedDedupBatch> = VecDeque::new();
        loop {
            if pending_batches.len() >= pipeline_depth {
                let pending = pending_batches
                    .pop_front()
                    .expect("dedup commit pipeline depth checked non-zero");
                if !Self::finish_pending_prepared_batch(
                    shard_idx, pending, miss_tx, pool, done_tx, metrics, cleanup_tx, candidate,
                ) {
                    return;
                }
            }

            let recv_start = Instant::now();
            let first = match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(unit) => {
                    let idle_ns = recv_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                    metrics
                        .flush_dedup_worker_idle_ns
                        .fetch_add(idle_ns, Ordering::Relaxed);
                    unit
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    let idle_ns = recv_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                    metrics
                        .flush_dedup_worker_idle_ns
                        .fetch_add(idle_ns, Ordering::Relaxed);
                    if let Some(pending) = pending_batches.pop_front() {
                        if !Self::finish_pending_prepared_batch(
                            shard_idx, pending, miss_tx, pool, done_tx, metrics, cleanup_tx,
                            candidate,
                        ) {
                            return;
                        }
                    }
                    continue;
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    while let Some(pending) = pending_batches.pop_front() {
                        if !Self::finish_pending_prepared_batch(
                            shard_idx, pending, miss_tx, pool, done_tx, metrics, cleanup_tx,
                            candidate,
                        ) {
                            return;
                        }
                    }
                    return;
                }
            };
            let active_start = Instant::now();
            metrics
                .flush_dedup_worker_iters
                .fetch_add(1, Ordering::Relaxed);

            let mut batch = Vec::with_capacity(Self::DEDUP_WORKER_BATCH_MAX_UNITS);
            batch.push(first);
            while batch.len() < Self::DEDUP_WORKER_BATCH_MAX_UNITS {
                match rx.try_recv() {
                    Ok(unit) => batch.push(unit),
                    Err(_) => break,
                }
            }

            let mut prepared = Vec::with_capacity(batch.len());
            for mut unit in batch {
                // Backpressure: skip dedup if this shard's buffer is
                // filling up. Optionally also skip when the queue is deep:
                // large LV2 buffers can have tens of thousands of pending
                // entries while fill% is still low. Keep the pending gate
                // configurable so Optane/NVMe deployments can preserve a
                // stricter dedup-first foreground path.
                let pending_gate_tripped = pending_skip_threshold_entries > 0
                    && pool.pending_count_for_shard(shard_idx) > pending_skip_threshold_entries;
                if pool.fill_percentage_for_shard(shard_idx) > skip_threshold_pct as u8
                    || pending_gate_tripped
                {
                    unit.dedup_skipped = true;
                    metrics.dedup_skipped_units.fetch_add(1, Ordering::Relaxed);
                    let len_before = miss_tx.len();
                    let started = Instant::now();
                    let result = miss_tx.send(unit);
                    Self::record_stage_send(
                        &metrics.flush_stage_dedup_send_ns,
                        &metrics.flush_stage_dedup_send_ops,
                        &metrics.flush_stage_dedup_send_len_sum,
                        &metrics.flush_stage_dedup_send_len_max,
                        started,
                        len_before,
                    );
                    if result.is_err() {
                        return;
                    }
                    continue;
                }

                prepared.push(Self::prepare_dedup_unit(unit));
            }

            if prepared.is_empty() {
                let active_ns = active_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                metrics
                    .flush_dedup_worker_active_ns
                    .fetch_add(active_ns, Ordering::Relaxed);
                continue;
            }

            Self::lookup_dedup_hits(&mut prepared, meta, pool, allocator, metrics);
            Self::candidate_lookup_pass(&mut prepared, candidate, pool, allocator, metrics);
            // LV3 verify ALL hits (dedup_index- and candidate-sourced).
            // The xxh3_64 schema does not have crypto-strength
            // collision resistance, so verify is correctness, not
            // optimisation. read_pool=None disables verify (degrades
            // to trust-hash mode; see BufferFlusher::start_with_metrics
            // doc comment for the trade-off).
            if let Some(rp) = read_pool {
                Self::verify_prepared_dedup_hits(&mut prepared, rp, candidate, metrics);
            }
            let chunks = Self::issue_prepared_dedup_hits(
                shard_idx,
                &mut prepared,
                meta,
                pool,
                lifecycle,
                metrics,
                cleanup_tx,
                candidate,
                commit_worker_txs,
                commit_workers_per_volume,
            );

            let pending = PendingPreparedDedupBatch { prepared, chunks };
            if pending.chunks.is_empty() {
                if !Self::finish_pending_prepared_batch(
                    shard_idx, pending, miss_tx, pool, done_tx, metrics, cleanup_tx, candidate,
                ) {
                    return;
                }
            } else {
                pending_batches.push_back(pending);
            }

            let active_ns = active_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            metrics
                .flush_dedup_worker_active_ns
                .fetch_add(active_ns, Ordering::Relaxed);
        }
    }

    fn prepare_dedup_unit(unit: CoalesceUnit) -> PreparedDedupUnit {
        let lba_count = unit.lba_count as usize;
        let is_hit = vec![false; lba_count];
        let mut all_hashes: Vec<ContentHash> = Vec::with_capacity(lba_count);
        let mut lookup_indices: Vec<usize> = Vec::with_capacity(lba_count);
        let mut zero_indices: Vec<usize> = Vec::new();

        for i in 0..lba_count {
            let Some(block) = unit.raw_blocks.get(i) else {
                all_hashes.push([0u8; 8]);
                continue;
            };
            if block.bytes().iter().all(|b| *b == 0) {
                all_hashes.push([0u8; 8]);
                zero_indices.push(i);
                continue;
            }
            let hash: ContentHash = crate::meta::schema::compute_content_hash(block.bytes());
            all_hashes.push(hash);
            lookup_indices.push(i);
        }

        PreparedDedupUnit {
            unit,
            is_hit,
            all_hashes,
            lookup_indices,
            successful_hit_indices: Vec::new(),
            zero_indices,
            valid_hits: Vec::new(),
            promote_candidates: Vec::new(),
            stale_index_repairs: vec![None; lba_count],
            target_guards: Vec::new(),
            dedup_target_rejected: vec![false; lba_count],
        }
    }

    fn lookup_dedup_hits(
        prepared: &mut [PreparedDedupUnit],
        meta: &MetaStore,
        pool: &WriteBufferPool,
        allocator: &SpaceAllocator,
        metrics: &EngineMetrics,
    ) {
        let total_lookups = prepared
            .iter()
            .map(|unit| unit.lookup_indices.len())
            .sum::<usize>();
        if total_lookups == 0 {
            return;
        }

        let mut lookup_hashes = Vec::with_capacity(total_lookups);
        for unit in prepared.iter() {
            lookup_hashes.extend(unit.lookup_indices.iter().map(|&i| unit.all_hashes[i]));
        }

        let lookup_start = Instant::now();
        let lookup_result = meta.multi_get_dedup_entries(&lookup_hashes);
        Self::record_elapsed(&metrics.dedup_lookup_ns, lookup_start);
        metrics
            .dedup_lookup_ops
            .fetch_add(lookup_hashes.len() as u64, Ordering::Relaxed);

        let lookup_entries = match lookup_result {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "dedup worker: batched dedup lookup failed; treating units as all-miss"
                );
                return;
            }
        };

        let mut cursor = 0usize;
        for prepared_unit in prepared.iter_mut() {
            let len = prepared_unit.lookup_indices.len();
            let unit_lookup_entries = &lookup_entries[cursor..cursor + len];
            cursor += len;
            for (entry_pos, &i) in prepared_unit.lookup_indices.iter().enumerate() {
                if let Some(entry) = unit_lookup_entries[entry_pos] {
                    let hash = prepared_unit.all_hashes[i];
                    let value = entry.to_blockmap_value();
                    if Self::dedup_target_overlaps_relocation_source(
                        prepared_unit.unit.raw_blocks.get(i),
                        &value,
                    ) {
                        Self::record_dedup_target_rejected(
                            &mut prepared_unit.dedup_target_rejected,
                            i,
                            metrics,
                        );
                        continue;
                    }
                    let Some(guard) = allocator
                        .pin_dedup_target_if_allowed(value.pba, value.physical_blocks(BLOCK_SIZE))
                    else {
                        Self::record_dedup_target_rejected(
                            &mut prepared_unit.dedup_target_rejected,
                            i,
                            metrics,
                        );
                        continue;
                    };
                    prepared_unit.is_hit[i] = true;
                    let lba = Lba(prepared_unit.unit.start_lba.0 + i as u64);
                    let latest_seq =
                        Self::latest_seq_for_lba(&prepared_unit.unit.seq_lba_ranges, lba);
                    if !pool.is_latest_lba_seq(
                        &prepared_unit.unit.vol_id,
                        lba,
                        latest_seq,
                        prepared_unit.unit.vol_created_at,
                    ) {
                        prepared_unit.successful_hit_indices.push(i);
                        continue;
                    }
                    // No separate liveness probe here: the metadata remap
                    // below has a refcount guard and rejects dead targets
                    // atomically. Avoiding a second forward-index/refcount
                    // read is the hot-path win for high-hit workloads.
                    prepared_unit.valid_hits.push((i, value, hash));
                    prepared_unit.target_guards.push(guard);
                }
            }
        }
    }

    fn dedup_target_overlaps_relocation_source(
        block: Option<&crate::buffer::pipeline::RawBlockRef>,
        target: &BlockmapValue,
    ) -> bool {
        let Some(source) = block.and_then(|block| block.relocation_source) else {
            return false;
        };
        let blocks = target.physical_blocks(BLOCK_SIZE);
        if blocks == 0 {
            return false;
        }
        let target_end = target.pba.0.saturating_add(u64::from(blocks));
        source.start.0 < target_end && target.pba.0 < source.end_pba().0
    }

    fn record_dedup_target_rejected(
        rejected: &mut [bool],
        lba_idx: usize,
        metrics: &EngineMetrics,
    ) {
        if !rejected[lba_idx] {
            rejected[lba_idx] = true;
            metrics
                .gc_defrag_dedup_hits_rejected
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// After `lookup_dedup_hits` has marked persistent dedup_index
    /// hits, walk the leftover misses and ask the in-memory
    /// `CandidateCache`. A candidate hit means the same fingerprint
    /// was seen recently (still in the LRU window) but never made it
    /// into the persistent index — the perfect moment to confirm a
    /// real duplicate via LV3 byte-compare and promote.
    ///
    /// Promotion candidates are recorded in
    /// `prepared_unit.promote_candidates` — the writer feeds them
    /// into the next `atomic_batch_write_*_with_dedup` so dedup_index
    /// + blockmap remap + refcount land atomically.
    /// Verify mismatches in `verify_prepared_dedup_hits` filter both
    /// `valid_hits` and `promote_candidates` so we never promote a
    /// fingerprint whose stored content has drifted from the new
    /// write.

    fn candidate_lookup_pass(
        prepared: &mut [PreparedDedupUnit],
        candidate: &crate::dedup::CandidateCache,
        pool: &WriteBufferPool,
        allocator: &SpaceAllocator,
        metrics: &EngineMetrics,
    ) {
        for prepared_unit in prepared.iter_mut() {
            for &i in &prepared_unit.lookup_indices {
                if prepared_unit.is_hit[i] {
                    // Persistent dedup_index already claimed this
                    // LBA; do not double-route through promote.
                    continue;
                }
                let hash = prepared_unit.all_hashes[i];
                if hash == [0u8; 8] {
                    continue;
                }
                let Some(value) = candidate.lookup(&hash) else {
                    continue;
                };
                if Self::dedup_target_overlaps_relocation_source(
                    prepared_unit.unit.raw_blocks.get(i),
                    &value,
                ) {
                    Self::record_dedup_target_rejected(
                        &mut prepared_unit.dedup_target_rejected,
                        i,
                        metrics,
                    );
                    continue;
                }
                // The allocator checks quarantine and registers the hazard under
                // one lock, so target publication cannot slip between them.
                let Some(guard) = allocator
                    .pin_dedup_target_if_allowed(value.pba, value.physical_blocks(BLOCK_SIZE))
                else {
                    Self::record_dedup_target_rejected(
                        &mut prepared_unit.dedup_target_rejected,
                        i,
                        metrics,
                    );
                    continue;
                };
                let lba = Lba(prepared_unit.unit.start_lba.0 + i as u64);
                let latest_seq = Self::latest_seq_for_lba(&prepared_unit.unit.seq_lba_ranges, lba);
                if !pool.is_latest_lba_seq(
                    &prepared_unit.unit.vol_id,
                    lba,
                    latest_seq,
                    prepared_unit.unit.vol_created_at,
                ) {
                    // Stale LBA write; another write has already
                    // superseded this one. Don't promote and don't
                    // re-route to compress as a hit (the buffer pool
                    // will retire the stale entry on its own).
                    prepared_unit.successful_hit_indices.push(i);
                    prepared_unit.is_hit[i] = true;
                    continue;
                }
                prepared_unit.is_hit[i] = true;
                prepared_unit.valid_hits.push((i, value, hash));
                prepared_unit
                    .promote_candidates
                    .push((i, hash, value.to_dedup_entry()));
                prepared_unit.target_guards.push(guard);
            }
        }
    }

    /// Byte-verify every entry in `valid_hits` against the source
    /// 4 KiB block via the engine's batched io_uring read pool.
    /// Fingerprints under xxh3_64 do not provide cryptographic
    /// collision resistance, so this step is correctness — without
    /// it, ~1.5e-8 per-pair collision rate × 2.7e11 unique blocks at
    /// 1 PiB scale = ≈ 1900 collision pairs that would silently
    /// cause data corruption.
    ///
    /// Mismatches are dropped from `valid_hits` *and*
    /// `promote_candidates`. The corresponding `is_hit[i]` is cleared
    /// so the writer routes the LBA through the fresh-write path.
    /// IO failures collapse to mismatch (treat as miss); see
    /// `dedup::verify::batched_verify` for the per-target failure
    /// semantics.

    fn verify_prepared_dedup_hits(
        prepared: &mut [PreparedDedupUnit],
        read_pool: &crate::io::read_pool::ReadPool,
        candidate: &crate::dedup::CandidateCache,
        metrics: &EngineMetrics,
    ) {
        // Collect targets across all units. Track the (unit_idx,
        // lba_idx) for each one so we can apply the verify result
        // back to the right slot.
        let mut targets: Vec<crate::dedup::VerifyTarget<'_>> = Vec::new();
        let mut placement: Vec<(
            usize,
            usize,
            ContentHash,
            BlockmapValue,
            crate::io::read_pool::ReadPurpose,
        )> = Vec::new();
        for (unit_idx, prepared_unit) in prepared.iter().enumerate() {
            for (lba_idx, mapping, hash) in prepared_unit.valid_hits.iter() {
                let purpose = if prepared_unit
                    .promote_candidates
                    .iter()
                    .any(|(i, _, _)| i == lba_idx)
                {
                    crate::io::read_pool::ReadPurpose::DedupVerifyCandidate
                } else {
                    crate::io::read_pool::ReadPurpose::DedupVerifyIndex
                };
                let Some(block) = prepared_unit.unit.raw_blocks.get(*lba_idx) else {
                    // No raw bytes available — should not happen if
                    // valid_hits was populated correctly; defensively
                    // verify against an empty buffer so we drop the
                    // hit rather than promote on bad input.
                    targets.push(crate::dedup::VerifyTarget::new_with_purpose(
                        *mapping,
                        &[],
                        purpose,
                    ));
                    placement.push((unit_idx, *lba_idx, *hash, *mapping, purpose));
                    continue;
                };
                targets.push(crate::dedup::VerifyTarget::new_with_purpose(
                    *mapping,
                    block.bytes(),
                    purpose,
                ));
                placement.push((unit_idx, *lba_idx, *hash, *mapping, purpose));
            }
        }
        if targets.is_empty() {
            return;
        }
        let verify_start = Instant::now();
        let results = match crate::dedup::batched_verify(read_pool, &targets) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "dedup verify: batched_verify failed; treating all hits as mismatches"
                );
                vec![false; targets.len()]
            }
        };
        Self::record_elapsed(&metrics.dedup_lookup_ns, verify_start);
        drop(targets);
        // Group mismatches by unit. valid_hits / promote_candidates
        // can hold tens of entries per unit; using a HashSet keeps
        // the retain filters O(n) instead of O(n × |mismatches|).
        let mut mismatches_per_unit: HashMap<usize, std::collections::HashSet<usize>> =
            HashMap::new();
        for ((unit_idx, lba_idx, hash, mapping, purpose), matched) in
            placement.into_iter().zip(results)
        {
            if !matched {
                match purpose {
                    crate::io::read_pool::ReadPurpose::DedupVerifyIndex
                    | crate::io::read_pool::ReadPurpose::DedupVerify => {
                        prepared[unit_idx].stale_index_repairs[lba_idx] =
                            Some(mapping.to_dedup_entry());
                        tracing::debug!(
                            pba = mapping.pba.0,
                            slot_offset = mapping.slot_offset,
                            "dedup verify: queued stale forward dedup repair after mismatch"
                        );
                    }
                    crate::io::read_pool::ReadPurpose::DedupVerifyCandidate => {
                        candidate.remove_by_hash(&hash);
                    }
                    crate::io::read_pool::ReadPurpose::Foreground
                    | crate::io::read_pool::ReadPurpose::DedupScanner => {}
                }
                mismatches_per_unit
                    .entry(unit_idx)
                    .or_default()
                    .insert(lba_idx);
            }
        }
        for (unit_idx, mismatched_lbas) in mismatches_per_unit {
            let prepared_unit = &mut prepared[unit_idx];
            for lba_idx in &mismatched_lbas {
                prepared_unit.is_hit[*lba_idx] = false;
            }
            prepared_unit
                .valid_hits
                .retain(|(i, _, _)| !mismatched_lbas.contains(i));
            prepared_unit
                .promote_candidates
                .retain(|(i, _, _)| !mismatched_lbas.contains(i));
        }
    }

    fn issue_prepared_dedup_hits(
        shard_idx: usize,
        prepared: &mut [PreparedDedupUnit],
        meta: &MetaStore,
        pool: &WriteBufferPool,
        lifecycle: &VolumeLifecycleManager,
        metrics: &EngineMetrics,
        cleanup_tx: &Sender<CleanupBatch>,
        candidate: &crate::dedup::CandidateCache,
        commit_worker_txs: &[Sender<writer::CommitJob>],
        commit_workers_per_volume: usize,
    ) -> Vec<PendingDedupHitChunk> {
        let mut issued = Vec::new();
        let mut by_volume: HashMap<String, Vec<usize>> = HashMap::new();
        for (unit_idx, prepared_unit) in prepared.iter().enumerate() {
            if !prepared_unit.valid_hits.is_empty() {
                by_volume
                    .entry(prepared_unit.unit.vol_id.clone())
                    .or_default()
                    .push(unit_idx);
            }
        }

        for (vol_id_str, unit_indices) in by_volume {
            let vol_id = VolumeId(vol_id_str.clone());
            let mut generation_cache: HashMap<u64, OnyxResult<bool>> = HashMap::new();
            let mut pending: Vec<(usize, usize, Lba, BlockmapValue, ContentHash)> = Vec::new();

            lifecycle.with_read_lock(&vol_id_str, || {
                // Same-LBA concurrent commits are arbitrated by metadb's
                // per-LBA seq_guard CAS; the race against a concurrent
                // free of the target PBA is handled inside metadb by
                // `L2pRemap { guard: Some((pba, 1)) }`, which refuses
                // the remap atomically when refcount < 1.
                for unit_idx in unit_indices {
                    let unit = &prepared[unit_idx].unit;
                    let generation_alive = generation_cache
                        .entry(unit.vol_created_at)
                        .or_insert_with(|| match meta.get_volume(&vol_id) {
                            Ok(Some(vc)) => Ok(vc.created_at == unit.vol_created_at),
                            Ok(None) => Ok(false),
                            Err(e) => Err(e),
                        })
                        .as_ref()
                        .copied();

                    match generation_alive {
                        Ok(true) => {}
                        Ok(false) => {
                            let hits = std::mem::take(&mut prepared[unit_idx].valid_hits);
                            for (i, _, _) in &hits {
                                prepared[unit_idx].is_hit[*i] = true;
                            }
                            prepared[unit_idx]
                                .successful_hit_indices
                                .extend(hits.into_iter().map(|(i, _, _)| i));
                            continue;
                        }
                        Err(e) => {
                            let hits = std::mem::take(&mut prepared[unit_idx].valid_hits);
                            metrics
                                .dedup_hit_failures
                                .fetch_add(hits.len() as u64, Ordering::Relaxed);
                            for (i, _, _) in hits {
                                prepared[unit_idx].is_hit[i] = false;
                            }
                            tracing::warn!(
                                vol = %vol_id_str,
                                error = %e,
                                "dedup worker: failed to check volume generation; demoting hits to miss"
                            );
                            continue;
                        }
                    }

                    let hits = std::mem::take(&mut prepared[unit_idx].valid_hits);
                    for (i, existing_value, hash) in hits {
                        let unit = &prepared[unit_idx].unit;
                        let lba = Lba(unit.start_lba.0 + i as u64);
                        let latest_seq = Self::latest_seq_for_lba(&unit.seq_lba_ranges, lba);
                        if !pool.is_latest_lba_seq(
                            &unit.vol_id,
                            lba,
                            latest_seq,
                            unit.vol_created_at,
                        ) {
                            prepared[unit_idx].successful_hit_indices.push(i);
                            continue;
                        }
                        if maybe_inject_dedup_hit_failure(&vol_id_str, lba).is_ok() {
                            pending.push((unit_idx, i, lba, existing_value, hash));
                            if pending.len() >= Self::DEDUP_HIT_COMMIT_BATCH_SIZE {
                                issued.push(Self::issue_dedup_hit_chunk(
                                    shard_idx,
                                    prepared,
                                    &vol_id,
                                    &vol_id_str,
                                    &mut pending,
                                    meta,
                                    metrics,
                                    cleanup_tx,
                                    candidate,
                                    commit_worker_txs,
                                    commit_workers_per_volume,
                                ));
                            }
                        } else {
                            prepared[unit_idx].is_hit[i] = false;
                            metrics.dedup_hit_failures.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

                if !pending.is_empty() {
                    issued.push(Self::issue_dedup_hit_chunk(
                        shard_idx,
                        prepared,
                        &vol_id,
                        &vol_id_str,
                        &mut pending,
                        meta,
                        metrics,
                        cleanup_tx,
                        candidate,
                        commit_worker_txs,
                        commit_workers_per_volume,
                    ));
                }
            });
        }

        Self::commit_prepared_zero_blocks(prepared, meta, pool, lifecycle, metrics, cleanup_tx);
        issued
    }

    fn commit_prepared_zero_blocks(
        prepared: &mut [PreparedDedupUnit],
        meta: &MetaStore,
        pool: &WriteBufferPool,
        lifecycle: &VolumeLifecycleManager,
        metrics: &EngineMetrics,
        cleanup_tx: &Sender<CleanupBatch>,
    ) {
        let mut by_volume: HashMap<String, Vec<usize>> = HashMap::new();
        for (unit_idx, prepared_unit) in prepared.iter().enumerate() {
            if !prepared_unit.zero_indices.is_empty() {
                by_volume
                    .entry(prepared_unit.unit.vol_id.clone())
                    .or_default()
                    .push(unit_idx);
            }
        }

        for (vol_id_str, unit_indices) in by_volume {
            let vol_id = VolumeId(vol_id_str.clone());
            let mut generation_cache: HashMap<u64, OnyxResult<bool>> = HashMap::new();
            lifecycle.with_read_lock(&vol_id_str, || {
                // Same-LBA concurrent commits are arbitrated by
                // metadb's per-LBA seq_guard CAS; no onyx-side
                // stripe lock here.
                for unit_idx in unit_indices {
                    let unit = &prepared[unit_idx].unit;
                    let generation_alive = generation_cache
                        .entry(unit.vol_created_at)
                        .or_insert_with(|| match meta.get_volume(&vol_id) {
                            Ok(Some(vc)) => Ok(vc.created_at == unit.vol_created_at),
                            Ok(None) => Ok(false),
                            Err(e) => Err(e),
                        })
                        .as_ref()
                        .copied();

                    match generation_alive {
                        Ok(true) => {}
                        Ok(false) => {
                            let zeros = std::mem::take(&mut prepared[unit_idx].zero_indices);
                            for &i in &zeros {
                                prepared[unit_idx].is_hit[i] = true;
                            }
                            prepared[unit_idx].successful_hit_indices.extend(zeros);
                            continue;
                        }
                        Err(e) => {
                            tracing::warn!(
                                vol = %vol_id_str,
                                error = %e,
                                "dedup worker: failed to check volume generation for zero blocks"
                            );
                            continue;
                        }
                    }

                    let zero_indices = prepared[unit_idx].zero_indices.clone();
                    let mut batch_values: Vec<(Lba, BlockmapValue)> =
                        Vec::with_capacity(zero_indices.len());
                    let mut batch_seqs: Vec<u64> = Vec::with_capacity(zero_indices.len());
                    for &i in &zero_indices {
                        let lba = Lba(unit.start_lba.0 + i as u64);
                        let latest_seq = Self::latest_seq_for_lba(&unit.seq_lba_ranges, lba);
                        if !pool.is_latest_lba_seq(
                            &unit.vol_id,
                            lba,
                            latest_seq,
                            unit.vol_created_at,
                        ) {
                            prepared[unit_idx].is_hit[i] = true;
                            prepared[unit_idx].successful_hit_indices.push(i);
                            continue;
                        }
                        batch_values.push((lba, BlockmapValue::zero()));
                        batch_seqs.push(latest_seq);
                    }
                    if batch_values.is_empty() {
                        prepared[unit_idx].zero_indices.clear();
                        continue;
                    }

                    match meta.atomic_batch_write_with_dedup(
                        &vol_id,
                        &batch_values,
                        0,
                        &[],
                        &batch_seqs,
                    ) {
                        Ok((newly_zeroed, accepted_flags)) => {
                            for (k, (lba, _)) in batch_values.iter().enumerate() {
                                let i = (lba.0 - unit.start_lba.0) as usize;
                                if accepted_flags.get(k).copied().unwrap_or(true) {
                                    prepared[unit_idx].is_hit[i] = true;
                                    prepared[unit_idx].successful_hit_indices.push(i);
                                } else {
                                    metrics.dedup_hit_failures.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            prepared[unit_idx].zero_indices.clear();
                            if !newly_zeroed.is_empty() {
                                let _ = cleanup_tx.send(newly_zeroed.into_values().collect());
                            }
                        }
                        Err(e) => {
                            metrics
                                .dedup_hit_failures
                                .fetch_add(batch_values.len() as u64, Ordering::Relaxed);
                            tracing::error!(
                                vol = %vol_id_str,
                                count = batch_values.len(),
                                error = %e,
                                "dedup worker: zero block remap failed, demoting to miss"
                            );
                        }
                    }
                }
            });
        }
    }

    fn issue_dedup_hit_chunk(
        shard_idx: usize,
        prepared: &mut [PreparedDedupUnit],
        vol_id: &VolumeId,
        vol_id_str: &str,
        pending: &mut Vec<(usize, usize, Lba, BlockmapValue, ContentHash)>,
        meta: &MetaStore,
        metrics: &EngineMetrics,
        _cleanup_tx: &Sender<CleanupBatch>,
        candidate: &crate::dedup::CandidateCache,
        commit_worker_txs: &[Sender<writer::CommitJob>],
        commit_workers_per_volume: usize,
    ) -> PendingDedupHitChunk {
        let chunk = std::mem::take(pending);
        metrics
            .dedup_hit_commit_ops
            .fetch_add(chunk.len() as u64, Ordering::Relaxed);
        let batch_input: Vec<(Lba, BlockmapValue, ContentHash)> = chunk
            .iter()
            .map(|(_, _, lba, value, hash)| (*lba, *value, *hash))
            .collect();
        let batch_seqs: Vec<u64> = chunk
            .iter()
            .map(|(unit_idx, _, lba, _, _)| {
                Self::latest_seq_for_lba(&prepared[*unit_idx].unit.seq_lba_ranges, *lba)
            })
            .collect();
        let batch_len = batch_input.len();

        // Collect promote_candidates from the unit_idx values present
        // in this chunk. Each (lba_idx, hash, dedup_entry) tuple is a
        // confirmed candidate-source hit that we want to register
        // into dedup_index atomically with the LBA remap. Drain
        // `promote_candidates` so we don't re-promote
        // the same entry on a subsequent chunk.
        let mut chunk_unit_idxs: std::collections::HashSet<usize> =
            chunk.iter().map(|(u, _, _, _, _)| *u).collect();
        let mut promote_entries: Vec<(ContentHash, DedupEntry)> = Vec::new();
        for unit_idx in chunk_unit_idxs.drain() {
            let drained = std::mem::take(&mut prepared[unit_idx].promote_candidates);
            for (_, hash, entry) in drained {
                promote_entries.push((hash, entry));
            }
        }

        // Serialise same-hash promotes across the per-lane dedup workers.
        // Two workers can verify a candidate hit for the SAME content
        // hash concurrently; each `DedupPut`'s commit-time `old_pba`
        // resolution can then capture the same old PBA → both decref it →
        // global refcount underflow (metadb `resolve_dedup_old_pbas`
        // note; nvme-box dedup_drainer A/B 2026-06-01). Claim each hash
        // in the cross-lane in-flight gate and drop the `DedupPut` for any
        // hash already owned by another lane. Intra-batch duplicates are
        // kept — they are serialized within this single tx by
        // `resolve_dedup_old_pbas` and chained correctly. A dropped hash's
        // rc-neutral L2pRemap stays in `batch_input`: it lands once the
        // owning promote incref's the target, or self-heals to a miss via
        // its rc guard.
        let mut claimed_promotes: Vec<ContentHash> = Vec::new();
        {
            let mut this_batch: std::collections::HashSet<ContentHash> =
                std::collections::HashSet::new();
            let mut skipped = 0u64;
            promote_entries.retain(|(hash, _)| {
                if this_batch.contains(hash) {
                    true
                } else if candidate.try_claim_promote(hash) {
                    this_batch.insert(*hash);
                    claimed_promotes.push(*hash);
                    true
                } else {
                    skipped += 1;
                    false
                }
            });
            if skipped > 0 {
                metrics
                    .dedup_promote_skipped_inflight
                    .fetch_add(skipped, Ordering::Relaxed);
            }
        }

        let promote_hashes: Vec<ContentHash> =
            promote_entries.iter().map(|(hash, _)| *hash).collect();
        let vol_created_at = chunk
            .first()
            .map(|(unit_idx, ..)| prepared[*unit_idx].unit.vol_created_at)
            .unwrap_or_default();
        let (response_tx, response_rx) = bounded(1);
        if commit_worker_txs.is_empty() {
            let hit_commit_start = Instant::now();
            let result = meta
                .atomic_batch_dedup_hits_with_promote(
                    vol_id,
                    &batch_input,
                    &promote_entries,
                    &batch_seqs,
                )
                .map(
                    |(results, newly_zeroed)| writer::DedupHitCommitResponse::Committed {
                        results,
                        newly_zeroed,
                    },
                )
                .unwrap_or_else(|error| writer::DedupHitCommitResponse::Failed(error.to_string()));
            Self::record_elapsed(&metrics.dedup_hit_commit_ns, hit_commit_start);
            let _ = response_tx.send(result);
        } else {
            let worker_idx =
                writer::route_volume_to_worker(vol_id_str, shard_idx, commit_workers_per_volume);
            if let Some(tx) = commit_worker_txs.get(worker_idx) {
                let job = writer::CommitJob::DedupHit(writer::DedupHitCommitJob {
                    vol_id: vol_id.clone(),
                    vol_created_at,
                    hits: batch_input,
                    promote_entries,
                    seqs: batch_seqs,
                    response_tx,
                    enqueued_at: Instant::now(),
                });
                if let Err(error) = tx.send(job) {
                    let writer::CommitJob::DedupHit(failed_job) = error.0 else {
                        unreachable!("dedup sender returned a different commit job kind")
                    };
                    let _ = failed_job
                        .response_tx
                        .send(writer::DedupHitCommitResponse::Failed(
                            "dedup hit commit worker disconnected".to_string(),
                        ));
                }
            } else {
                let _ = response_tx.send(writer::DedupHitCommitResponse::Failed(
                    "dedup hit commit worker route missing".to_string(),
                ));
            }
        }

        PendingDedupHitChunk {
            chunk,
            promote_hashes,
            claimed_promotes,
            response_rx,
            batch_len,
            vol_id_str: vol_id_str.to_string(),
        }
    }

    fn finish_dedup_hit_chunk(
        pending: PendingDedupHitChunk,
        prepared: &mut [PreparedDedupUnit],
        metrics: &EngineMetrics,
        cleanup_tx: &Sender<CleanupBatch>,
        candidate: &crate::dedup::CandidateCache,
    ) {
        let PendingDedupHitChunk {
            chunk,
            promote_hashes,
            claimed_promotes,
            response_rx,
            batch_len,
            vol_id_str,
        } = pending;
        let batch_result = response_rx.recv().unwrap_or_else(|_| {
            writer::DedupHitCommitResponse::Failed(
                "dedup hit commit response disconnected".to_string(),
            )
        });

        match batch_result {
            writer::DedupHitCommitResponse::Committed {
                results,
                newly_zeroed,
            } => {
                // Promotion landed in the persistent dedup_index — drop
                // the now-redundant candidate slot. If promote_entries
                // was empty (no candidate hits in this chunk) this is
                // a no-op.
                for hash in &promote_hashes {
                    candidate.remove_by_hash(hash);
                }
                metrics
                    .dedup_promotions_committed
                    .fetch_add(promote_hashes.len() as u64, Ordering::Relaxed);

                for ((unit_idx, hit_idx, lba, _, _), result) in chunk.into_iter().zip(results) {
                    match result {
                        DedupHitResult::Accepted(_) => {
                            prepared[unit_idx].successful_hit_indices.push(hit_idx);
                        }
                        DedupHitResult::Rejected => {
                            metrics.dedup_hit_failures.fetch_add(1, Ordering::Relaxed);
                            prepared[unit_idx].is_hit[hit_idx] = false;
                            tracing::debug!(
                                vol = %vol_id_str,
                                lba = lba.0,
                                "dedup worker: hit rejected (target PBA freed), demoting to miss"
                            );
                        }
                    }
                }

                if !newly_zeroed.is_empty() {
                    let _ = cleanup_tx.send(newly_zeroed.into_values().collect());
                }
            }
            writer::DedupHitCommitResponse::Failed(error) => {
                metrics
                    .dedup_hit_failures
                    .fetch_add(chunk.len() as u64, Ordering::Relaxed);
                metrics
                    .dedup_promotions_failed
                    .fetch_add(promote_hashes.len() as u64, Ordering::Relaxed);
                for (unit_idx, hit_idx, _, _, _) in chunk {
                    prepared[unit_idx].is_hit[hit_idx] = false;
                }
                tracing::error!(
                    vol = %vol_id_str,
                    count = batch_len,
                    promotes = promote_hashes.len(),
                    error = %error,
                    "dedup worker: batch hit + promote commit failed, demoting all to miss"
                );
            }
        }

        // Release the in-flight promote claims taken above (covers both
        // the Ok and Err arms). A leaked claim would permanently block
        // future promotes of that hash, so this must run on every path.
        for hash in &claimed_promotes {
            candidate.release_promote(hash);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_pending_prepared_batch(
        shard_idx: usize,
        mut pending: PendingPreparedDedupBatch,
        miss_tx: &Sender<CoalesceUnit>,
        pool: &WriteBufferPool,
        done_tx: &Sender<Vec<u64>>,
        metrics: &EngineMetrics,
        cleanup_tx: &Sender<CleanupBatch>,
        candidate: &crate::dedup::CandidateCache,
    ) -> bool {
        for chunk in pending.chunks {
            Self::finish_dedup_hit_chunk(
                chunk,
                &mut pending.prepared,
                metrics,
                cleanup_tx,
                candidate,
            );
        }
        for prepared_unit in pending.prepared {
            if !Self::finish_prepared_dedup_unit(
                shard_idx,
                prepared_unit,
                miss_tx,
                pool,
                done_tx,
                metrics,
            ) {
                return false;
            }
        }
        true
    }

    fn finish_prepared_dedup_unit(
        _shard_idx: usize,
        prepared: PreparedDedupUnit,
        miss_tx: &Sender<CoalesceUnit>,
        pool: &WriteBufferPool,
        done_tx: &Sender<Vec<u64>>,
        metrics: &EngineMetrics,
    ) -> bool {
        let PreparedDedupUnit {
            mut unit,
            is_hit,
            all_hashes,
            stale_index_repairs,
            successful_hit_indices,
            ..
        } = prepared;

        let mut completed_indices = successful_hit_indices;
        completed_indices.sort_unstable();
        completed_indices.dedup();

        if !completed_indices.is_empty() {
            for i in &completed_indices {
                let lba = Lba(unit.start_lba.0 + *i as u64);
                for (seq, range_start, range_count) in &unit.seq_lba_ranges {
                    if lba.0 >= range_start.0 && lba.0 < range_start.0 + *range_count as u64 {
                        let _ = pool.mark_applied(*seq, lba, 1);
                    }
                }
            }
        }

        let has_misses = is_hit.iter().any(|h| !h);
        metrics
            .dedup_hits
            .fetch_add(completed_indices.len() as u64, Ordering::Relaxed);
        metrics.dedup_misses.fetch_add(
            is_hit.iter().filter(|hit| !**hit).count() as u64,
            Ordering::Relaxed,
        );
        if !has_misses {
            let seqs: Vec<u64> = unit.seq_lba_ranges.iter().map(|(s, _, _)| *s).collect();
            let _ = done_tx.send(seqs);
            return true;
        }

        let mut miss_ranges: Vec<(usize, usize)> = Vec::new();
        let mut miss_start: Option<usize> = None;
        for (i, hit) in is_hit.iter().enumerate() {
            if !*hit {
                if miss_start.is_none() {
                    miss_start = Some(i);
                }
            } else if let Some(start) = miss_start.take() {
                miss_ranges.push((start, i));
            }
        }
        if let Some(start) = miss_start {
            miss_ranges.push((start, is_hit.len()));
        }

        // The overwhelmingly common random-write case is one unsplit all-miss
        // unit. Wrapping it in a one-count DedupCompletion adds an unnecessary
        // second completion protocol between writer and coalescer. More
        // importantly, a lost/duplicated wrapper decrement can leave the
        // original buffer seq permanently in-flight even though LV3 and
        // metadb have both committed it. Preserve the normal direct done_tx
        // semantics whenever dedup did not actually split the unit.
        if completed_indices.is_empty()
            && miss_ranges.len() == 1
            && miss_ranges[0] == (0, is_hit.len())
        {
            unit.block_hashes = Some(all_hashes);
            unit.dedup_stale_repairs = if stale_index_repairs.iter().any(Option::is_some) {
                Some(stale_index_repairs)
            } else {
                None
            };
            let len_before = miss_tx.len();
            let started = Instant::now();
            let result = miss_tx.send(unit);
            Self::record_stage_send(
                &metrics.flush_stage_dedup_send_ns,
                &metrics.flush_stage_dedup_send_ops,
                &metrics.flush_stage_dedup_send_len_sum,
                &metrics.flush_stage_dedup_send_len_max,
                started,
                len_before,
            );
            return result.is_ok();
        }

        let all_seqs: Vec<u64> = unit.seq_lba_ranges.iter().map(|(s, _, _)| *s).collect();
        let completion =
            crate::buffer::pipeline::DedupCompletion::new(miss_ranges.len() as u32, all_seqs);

        for (start, end) in &miss_ranges {
            let miss_unit = Self::build_miss_unit(
                &unit,
                *start,
                *end,
                &all_hashes,
                &stale_index_repairs,
                Some(completion.clone()),
            );
            let len_before = miss_tx.len();
            let started = Instant::now();
            let result = miss_tx.send(miss_unit);
            Self::record_stage_send(
                &metrics.flush_stage_dedup_send_ns,
                &metrics.flush_stage_dedup_send_ops,
                &metrics.flush_stage_dedup_send_len_sum,
                &metrics.flush_stage_dedup_send_len_max,
                started,
                len_before,
            );
            if result.is_err() {
                return false;
            }
        }
        true
    }

    /// Build a CoalesceUnit from a contiguous range of miss blocks [start, end).

    pub(super) fn build_miss_unit(
        original: &CoalesceUnit,
        start_idx: usize,
        end_idx: usize,
        hashes: &[ContentHash],
        stale_repairs: &[Option<DedupEntry>],
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
        let repair_slice = stale_repairs[start_idx..end_idx].to_vec();
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
            dedup_stale_repairs: if repair_slice.iter().any(Option::is_some) {
                Some(repair_slice)
            } else {
                None
            },
            dedup_completion,
        }
    }
}

#[cfg(test)]
mod relocation_tests {
    use super::*;
    use crate::buffer::pipeline::RawBlockRef;
    use crate::space::extent::Extent;
    use crate::types::Pba;
    use std::sync::Arc;

    fn target(pba: u64, compressed_size: u32) -> BlockmapValue {
        BlockmapValue {
            pba: Pba(pba),
            compression: 0,
            unit_compressed_size: compressed_size,
            unit_original_size: compressed_size,
            unit_lba_count: (compressed_size / BLOCK_SIZE) as u16,
            offset_in_unit: 0,
            crc32: 0,
            slot_offset: 0,
            flags: 0,
        }
    }

    #[test]
    fn relocation_rejects_any_dedup_target_overlapping_source_extent() {
        let block = RawBlockRef {
            payload: Arc::from(vec![0x55; BLOCK_SIZE as usize]),
            offset: 0,
            relocation_source: Some(Extent::new(Pba(100), 2)),
        };

        assert!(BufferFlusher::dedup_target_overlaps_relocation_source(
            Some(&block),
            &target(100, BLOCK_SIZE)
        ));
        assert!(BufferFlusher::dedup_target_overlaps_relocation_source(
            Some(&block),
            &target(99, 2 * BLOCK_SIZE)
        ));
        assert!(!BufferFlusher::dedup_target_overlaps_relocation_source(
            Some(&block),
            &target(102, BLOCK_SIZE)
        ));
    }

    #[test]
    fn relocation_dedup_reject_metric_counts_once_per_block() {
        let metrics = EngineMetrics::default();
        let mut rejected = vec![false];

        BufferFlusher::record_dedup_target_rejected(&mut rejected, 0, &metrics);
        BufferFlusher::record_dedup_target_rejected(&mut rejected, 0, &metrics);

        assert_eq!(
            metrics
                .gc_defrag_dedup_hits_rejected
                .load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn dedup_target_guard_survives_until_commit_response_is_consumed() {
        let allocator = SpaceAllocator::new(16 * 1024 * 1024, 1);
        let hazards = allocator.hazards();
        let guarded_pba = Pba(100);
        let guard = allocator
            .pin_dedup_target_if_allowed(guarded_pba, 1)
            .unwrap();
        let unit = CoalesceUnit {
            vol_id: "guard-vol".into(),
            start_lba: Lba(1),
            lba_count: 1,
            raw_blocks: vec![RawBlockRef {
                payload: Arc::from(vec![0x44; BLOCK_SIZE as usize]),
                offset: 0,
                relocation_source: None,
            }],
            compression: CompressionAlgo::None,
            vol_created_at: 1,
            seq_lba_ranges: vec![(1, Lba(1), 1)],
            dedup_skipped: false,
            block_hashes: None,
            dedup_stale_repairs: None,
            dedup_completion: None,
        };
        let mut prepared = BufferFlusher::prepare_dedup_unit(unit);
        prepared.target_guards.push(guard);

        let (response_tx, response_rx) = bounded(1);
        let pending = PendingPreparedDedupBatch {
            prepared: vec![prepared],
            chunks: vec![PendingDedupHitChunk {
                chunk: Vec::new(),
                promote_hashes: Vec::new(),
                claimed_promotes: Vec::new(),
                response_rx,
                batch_len: 0,
                vol_id_str: "guard-vol".into(),
            }],
        };
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let size = 16 * 1024 * 1024;
        tmp.as_file().set_len(size).unwrap();
        let device = crate::io::device::RawDevice::open_or_create(tmp.path(), size).unwrap();
        let pool = WriteBufferPool::open_with_group_commit_wait(device, Duration::ZERO).unwrap();
        let (miss_tx, miss_rx) = bounded(1);
        let (done_tx, _done_rx) = bounded(1);
        let metrics = Arc::new(EngineMetrics::default());
        let (cleanup_tx, _cleanup_rx) = bounded(1);
        let candidate = crate::dedup::CandidateCache::new(1, 4);

        let worker_metrics = metrics.clone();
        let worker = std::thread::spawn(move || {
            BufferFlusher::finish_pending_prepared_batch(
                0,
                pending,
                &miss_tx,
                &pool,
                &done_tx,
                &worker_metrics,
                &cleanup_tx,
                &candidate,
            )
        });

        std::thread::sleep(Duration::from_millis(10));
        assert!(
            !worker.is_finished(),
            "worker should be waiting for commit response"
        );
        assert!(hazards.is_pinned(guarded_pba));

        response_tx
            .send(writer::DedupHitCommitResponse::Failed("injected".into()))
            .unwrap();
        assert!(worker.join().unwrap());
        assert!(!hazards.is_pinned(guarded_pba));
        assert_eq!(miss_rx.try_recv().unwrap().start_lba, Lba(1));
    }
}
