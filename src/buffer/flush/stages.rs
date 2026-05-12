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
    /// Physical pins for dedup targets that passed byte-verify and are
    /// waiting for the metadata remap. Without this, a verified target PBA
    /// can be freed and reused between verify completion and hit commit.
    verified_target_guards: Vec<PbaHazardGuard>,
}

impl BufferFlusher {
    const DEDUP_WORKER_BATCH_MAX_UNITS: usize = 64;
    const DEDUP_HIT_COMMIT_BATCH_SIZE: usize = 1024;

    fn record_stage_send(
        send_ns: &std::sync::atomic::AtomicU64,
        send_ops: &std::sync::atomic::AtomicU64,
        len_sum: &std::sync::atomic::AtomicU64,
        len_max: &std::sync::atomic::AtomicU64,
        started: Instant,
        len_before: usize,
    ) {
        let elapsed_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        send_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
        send_ops.fetch_add(1, Ordering::Relaxed);
        len_sum.fetch_add(len_before as u64, Ordering::Relaxed);
        crate::metrics::record_counter_max(len_max, len_before as u64);
    }

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
        skip_fully_superseded: bool,
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

        // Head-stuck diagnostic: emit at most one warn per shard every
        // DIAG_LOG_INTERVAL while the head is older than DIAG_AGE_THRESHOLD_MS.
        // This narrows the "head pinned for minutes" mystery to one of two
        // failure modes:
        //   in_flight_count > 0  → writer-side path forgot to send done_tx
        //   in_flight_count == 0 && flushed_count < lba_count → some LBA was
        //                          never mark_flushed (no enqueue, no supersede)
        let mut last_diag_log: Option<Instant> = None;
        const DIAG_LOG_INTERVAL: Duration = Duration::from_secs(30);
        const DIAG_AGE_THRESHOLD_MS: u64 = 3000;

        while running.load(Ordering::Relaxed) {
            let iter_start = Instant::now();
            let mut this_iter_idle_ns: u64 = 0;
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
            if let Some(seq) =
                pool.head_stuck_seq_for_shard(shard_idx, Self::HEAD_RETRY_AGE_THRESHOLD)
            {
                let diag_snapshot = pool.pending_diag_snapshot_for_shard(shard_idx, seq);
                let enqueue_result = Self::try_enqueue_pending_seq(
                    seq,
                    pool,
                    &in_flight,
                    in_flight_tracker,
                    &mut seen,
                    &mut queued_bytes,
                    &mut new_entries,
                    metrics,
                    skip_fully_superseded,
                );
                if let Some((lba_count, flushed_count, age_ms, vol_id)) = diag_snapshot {
                    if age_ms >= DIAG_AGE_THRESHOLD_MS {
                        let due = match last_diag_log {
                            Some(ts) => ts.elapsed() >= DIAG_LOG_INTERVAL,
                            None => true,
                        };
                        if due {
                            let in_flight_count = in_flight.get(&seq).copied().unwrap_or(0);
                            let outcome = match enqueue_result {
                                EnqueuePendingSeq::Queued => "Queued".to_string(),
                                EnqueuePendingSeq::WindowFull => "WindowFull".to_string(),
                                EnqueuePendingSeq::Skipped(r) => format!("Skipped({:?})", r),
                            };
                            tracing::debug!(
                                shard = shard_idx,
                                seq,
                                age_ms,
                                in_flight_count,
                                flushed_count,
                                lba_count,
                                vol = %vol_id,
                                outcome = %outcome,
                                "head stuck >{}ms — diagnostic",
                                DIAG_AGE_THRESHOLD_MS
                            );
                            last_diag_log = Some(Instant::now());
                        }
                    }
                }
            }

            if new_entries.is_empty() {
                let recv_start = Instant::now();
                match pool.recv_ready_timeout_for_shard(shard_idx, ready_timeout) {
                    Ok(seq) => {
                        let idle_ns =
                            recv_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                        this_iter_idle_ns = this_iter_idle_ns.saturating_add(idle_ns);
                        metrics
                            .flush_coalesce_idle_ns
                            .fetch_add(idle_ns, Ordering::Relaxed);
                        let _ = Self::try_enqueue_pending_seq(
                            seq,
                            pool,
                            &in_flight,
                            in_flight_tracker,
                            &mut seen,
                            &mut queued_bytes,
                            &mut new_entries,
                            metrics,
                            skip_fully_superseded,
                        );
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                        let idle_ns =
                            recv_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                        this_iter_idle_ns = this_iter_idle_ns.saturating_add(idle_ns);
                        metrics
                            .flush_coalesce_idle_ns
                            .fetch_add(idle_ns, Ordering::Relaxed);
                    }
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
                        metrics,
                        skip_fully_superseded,
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
                            metrics,
                            skip_fully_superseded,
                        ),
                        EnqueuePendingSeq::Queued
                    ) {
                        topped_up += 1;
                    }
                }
            }

            if new_entries.is_empty() {
                let iter_total = iter_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                metrics.flush_coalesce_active_ns.fetch_add(
                    iter_total.saturating_sub(this_iter_idle_ns),
                    Ordering::Relaxed,
                );
                continue;
            }

            new_entries = pool.hydrate_pending_entries_for_shard(shard_idx, new_entries);
            if new_entries.is_empty() {
                let iter_total = iter_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                metrics.flush_coalesce_active_ns.fetch_add(
                    iter_total.saturating_sub(this_iter_idle_ns),
                    Ordering::Relaxed,
                );
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
            // the coalesced units. Flusher hydration returns detached payload
            // clones, so there is no pending_entries/lba_index payload to evict
            // here; avoiding that synchronous index rewrite keeps coalescing
            // independent from foreground buffer reads.
            drop(new_entries);

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
                let len_before = tx.len();
                let started = Instant::now();
                let result = tx.send(unit);
                Self::record_stage_send(
                    &metrics.flush_stage_coalesce_send_ns,
                    &metrics.flush_stage_coalesce_send_ops,
                    &metrics.flush_stage_coalesce_send_len_sum,
                    &metrics.flush_stage_coalesce_send_len_max,
                    started,
                    len_before,
                );
                if result.is_err() {
                    return;
                }
            }

            let iter_total = iter_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            metrics.flush_coalesce_active_ns.fetch_add(
                iter_total.saturating_sub(this_iter_idle_ns),
                Ordering::Relaxed,
            );
        }
    }

    pub(super) fn compress_loop(
        rx: &Receiver<CoalesceUnit>,
        tx: &Sender<CompressedUnit>,
        running: &AtomicBool,
        metrics: &EngineMetrics,
        min_compression_savings_pct: u8,
    ) {
        while running.load(Ordering::Relaxed) {
            let recv_start = Instant::now();
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(unit) => {
                    let idle_ns = recv_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                    metrics
                        .flush_compress_worker_idle_ns
                        .fetch_add(idle_ns, Ordering::Relaxed);
                    let active_start = Instant::now();
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
                        dedup_stale_repairs,
                        dedup_completion,
                    } = unit;

                    let original_size = raw_blocks.len() * BLOCK_SIZE as usize;
                    let mut raw_data = Vec::with_capacity(original_size);
                    for block in &raw_blocks {
                        raw_data.extend_from_slice(block.bytes());
                    }
                    let mut compression_bypassed = false;
                    let (compression_byte, compressed_data) = match algo {
                        CompressionAlgo::None => (0u8, raw_data),
                        _ => {
                            let compressor = create_compressor(algo);
                            let max_out = compressor.max_compressed_size(original_size);
                            let mut compressed_buf = vec![0u8; max_out];
                            match compressor.compress(&raw_data, &mut compressed_buf) {
                                Some(size)
                                    if Self::compression_saves_enough(
                                        original_size,
                                        size,
                                        min_compression_savings_pct,
                                    ) =>
                                {
                                    (algo.to_u8(), compressed_buf[..size].to_vec())
                                }
                                None => {
                                    compression_bypassed = true;
                                    (0u8, raw_data)
                                }
                                _ => {
                                    compression_bypassed = true;
                                    (0u8, raw_data)
                                }
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
                    if compression_bypassed {
                        metrics
                            .compress_bypass_units
                            .fetch_add(1, Ordering::Relaxed);
                        metrics
                            .compress_bypass_bytes
                            .fetch_add(original_size as u64, Ordering::Relaxed);
                    }

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
                        dedup_stale_repairs,
                        dedup_skipped,
                        dedup_completion,
                    };

                    let len_before = tx.len();
                    let started = Instant::now();
                    let result = tx.send(cu);
                    Self::record_stage_send(
                        &metrics.flush_stage_compress_send_ns,
                        &metrics.flush_stage_compress_send_ops,
                        &metrics.flush_stage_compress_send_len_sum,
                        &metrics.flush_stage_compress_send_len_max,
                        started,
                        len_before,
                    );
                    if result.is_err() {
                        return;
                    }
                    let active_ns =
                        active_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                    metrics
                        .flush_compress_worker_active_ns
                        .fetch_add(active_ns, Ordering::Relaxed);
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    let idle_ns = recv_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                    metrics
                        .flush_compress_worker_idle_ns
                        .fetch_add(idle_ns, Ordering::Relaxed);
                    continue;
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    fn compression_saves_enough(original_size: usize, compressed_size: usize, min_pct: u8) -> bool {
        if original_size == 0 || compressed_size >= original_size {
            return false;
        }
        if min_pct == 0 {
            return true;
        }
        let saved = original_size - compressed_size;
        saved.saturating_mul(100) >= original_size.saturating_mul(min_pct as usize)
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
        allocator: &SpaceAllocator,
        done_tx: &Sender<Vec<u64>>,
        running: &AtomicBool,
        skip_threshold_pct: u8,
        pending_skip_threshold_entries: u64,
        metrics: &EngineMetrics,
        cleanup_tx: &Sender<CleanupBatch>,
        candidate: &crate::dedup::CandidateCache,
        read_pool: Option<&crate::io::read_pool::ReadPool>,
    ) {
        while running.load(Ordering::Relaxed) {
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
                    continue;
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
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

            Self::lookup_dedup_hits(&mut prepared, meta, pool, metrics);
            Self::candidate_lookup_pass(&mut prepared, candidate, pool);
            // LV3 verify ALL hits (dedup_index- and candidate-sourced).
            // The xxh3_64 schema does not have crypto-strength
            // collision resistance, so verify is correctness, not
            // optimisation. read_pool=None disables verify (degrades
            // to trust-hash mode; see BufferFlusher::start_with_metrics
            // doc comment for the trade-off).
            if let Some(rp) = read_pool {
                Self::verify_prepared_dedup_hits(
                    &mut prepared,
                    rp,
                    allocator.hazards(),
                    candidate,
                    metrics,
                );
            }
            Self::commit_prepared_dedup_hits(
                &mut prepared,
                meta,
                pool,
                lifecycle,
                metrics,
                cleanup_tx,
                candidate,
            );

            for prepared_unit in prepared {
                if !Self::finish_prepared_dedup_unit(
                    shard_idx,
                    prepared_unit,
                    miss_tx,
                    pool,
                    done_tx,
                    metrics,
                ) {
                    return;
                }
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
            verified_target_guards: Vec::new(),
        }
    }

    fn lookup_dedup_hits(
        prepared: &mut [PreparedDedupUnit],
        meta: &MetaStore,
        pool: &WriteBufferPool,
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
                    prepared_unit
                        .valid_hits
                        .push((i, entry.to_blockmap_value(), hash));
                }
            }
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
        hazards: PbaHazards,
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
            PbaHazardGuard,
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
                let guard = hazards.pin_many(mapping.physical_pbas(BLOCK_SIZE));
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
                    placement.push((unit_idx, *lba_idx, *hash, *mapping, purpose, guard));
                    continue;
                };
                targets.push(crate::dedup::VerifyTarget::new_with_purpose(
                    *mapping,
                    block.bytes(),
                    purpose,
                ));
                placement.push((unit_idx, *lba_idx, *hash, *mapping, purpose, guard));
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
        for ((unit_idx, lba_idx, hash, mapping, purpose, guard), matched) in
            placement.into_iter().zip(results)
        {
            if matched {
                prepared[unit_idx].verified_target_guards.push(guard);
            } else {
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

    fn commit_prepared_dedup_hits(
        prepared: &mut [PreparedDedupUnit],
        meta: &MetaStore,
        pool: &WriteBufferPool,
        lifecycle: &VolumeLifecycleManager,
        metrics: &EngineMetrics,
        cleanup_tx: &Sender<CleanupBatch>,
        candidate: &crate::dedup::CandidateCache,
    ) {
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
                // Dedup-hit commits do NOT take `with_l2p_commit_locks_for_ranges`:
                // the race against a concurrent free of the target PBA is
                // handled inside metadb by `L2pRemap { guard: Some((pba, 1)) }`,
                // which refuses the remap atomically when refcount < 1.
                // Holding the onyx stripe lock for the full metadb commit
                // serialised hit + PT commits on overlapping stripes and
                // pinned `l2p_commit_lock_hold` at ~423 ms / acquire under
                // skip=0 — see docs/metadb-nvme-drain-plan.md for the
                // bisect data. Each hit chunk inside
                // `commit_dedup_hit_chunk` is still atomic via the metadb
                // tx; we just no longer block PT on it.
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
                                Self::commit_dedup_hit_chunk(
                                    prepared,
                                    &vol_id,
                                    &vol_id_str,
                                    &mut pending,
                                    meta,
                                    metrics,
                                    cleanup_tx,
                                    candidate,
                                );
                            }
                        } else {
                            prepared[unit_idx].is_hit[i] = false;
                            metrics.dedup_hit_failures.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

                if !pending.is_empty() {
                    Self::commit_dedup_hit_chunk(
                        prepared,
                        &vol_id,
                        &vol_id_str,
                        &mut pending,
                        meta,
                        metrics,
                        cleanup_tx,
                        candidate,
                    );
                }
            });
        }

        Self::commit_prepared_zero_blocks(prepared, meta, pool, lifecycle, metrics, cleanup_tx);
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
                let commit_ranges: Vec<(&str, Lba, u64)> = unit_indices
                    .iter()
                    .map(|&unit_idx| {
                    let unit = &prepared[unit_idx].unit;
                    (
                        vol_id_str.as_str(),
                        unit.start_lba,
                        unit.lba_count as u64,
                    )
                    })
                    .collect();
                pool.with_l2p_commit_locks_for_ranges(commit_ranges, || {
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
                        }
                        if batch_values.is_empty() {
                            prepared[unit_idx].zero_indices.clear();
                            continue;
                        }

                        match meta.atomic_batch_write(&vol_id, &batch_values, 0) {
                            Ok(newly_zeroed) => {
                                let accepted: Vec<usize> = batch_values
                                    .iter()
                                    .map(|(lba, _)| (lba.0 - unit.start_lba.0) as usize)
                                    .collect();
                                for &i in &accepted {
                                    prepared[unit_idx].is_hit[i] = true;
                                }
                                prepared[unit_idx].successful_hit_indices.extend(accepted);
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
            });
        }
    }

    fn commit_dedup_hit_chunk(
        prepared: &mut [PreparedDedupUnit],
        vol_id: &VolumeId,
        vol_id_str: &str,
        pending: &mut Vec<(usize, usize, Lba, BlockmapValue, ContentHash)>,
        meta: &MetaStore,
        metrics: &EngineMetrics,
        cleanup_tx: &Sender<CleanupBatch>,
        candidate: &crate::dedup::CandidateCache,
    ) {
        let chunk = std::mem::take(pending);
        metrics
            .dedup_hit_commit_ops
            .fetch_add(chunk.len() as u64, Ordering::Relaxed);
        let batch_input: Vec<(Lba, BlockmapValue, ContentHash)> = chunk
            .iter()
            .map(|(_, _, lba, value, hash)| (*lba, *value, *hash))
            .collect();

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

        let hit_commit_start = Instant::now();
        let batch_result =
            meta.atomic_batch_dedup_hits_with_promote(vol_id, &batch_input, &promote_entries);
        Self::record_elapsed(&metrics.dedup_hit_commit_ns, hit_commit_start);

        match batch_result {
            Ok((results, newly_zeroed)) => {
                // Promotion landed in the persistent dedup_index — drop
                // the now-redundant candidate slot. If promote_entries
                // was empty (no candidate hits in this chunk) this is
                // a no-op.
                for (hash, _) in &promote_entries {
                    candidate.remove_by_hash(hash);
                }
                metrics
                    .dedup_promotions_committed
                    .fetch_add(promote_entries.len() as u64, Ordering::Relaxed);

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
            Err(e) => {
                metrics
                    .dedup_hit_failures
                    .fetch_add(chunk.len() as u64, Ordering::Relaxed);
                metrics
                    .dedup_promotions_failed
                    .fetch_add(promote_entries.len() as u64, Ordering::Relaxed);
                for (unit_idx, hit_idx, _, _, _) in chunk {
                    prepared[unit_idx].is_hit[hit_idx] = false;
                }
                tracing::error!(
                    vol = %vol_id_str,
                    count = batch_input.len(),
                    promotes = promote_entries.len(),
                    error = %e,
                    "dedup worker: batch hit + promote commit failed, demoting all to miss"
                );
            }
        }
    }

    fn finish_prepared_dedup_unit(
        shard_idx: usize,
        prepared: PreparedDedupUnit,
        miss_tx: &Sender<CoalesceUnit>,
        pool: &WriteBufferPool,
        done_tx: &Sender<Vec<u64>>,
        metrics: &EngineMetrics,
    ) -> bool {
        let PreparedDedupUnit {
            unit,
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
                        let _ = pool.mark_flushed(*seq, lba, 1);
                    }
                }
            }
            let _ = pool.advance_tail_for_shard(shard_idx);
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
