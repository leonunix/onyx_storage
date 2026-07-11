use super::*;

/// Advance the sliding residence cutoff often enough that mature work reaches
/// LV3 as a stream instead of a once-per-second burst. At 20k random IOPS,
/// 100 ms still exposes tens of MiB across the 16 shards, enough for the
/// cross-lane batcher to form full stripes.
const WRITE_WINDOW_RELEASE_QUANTUM: Duration = Duration::from_millis(100);
/// Preserve the overwrite-absorption window across short pauses. Once the
/// foreground has been quiet for longer than this grace period, throughput is
/// more valuable than residence and the remaining durable LV2 work drains at
/// full speed.
const WRITE_WINDOW_IDLE_BYPASS: Duration = Duration::from_secs(5);
/// The LV2 log is the durable source of truth, so pipeline admission must be
/// at-least-once rather than permanently trusting an in-memory completion
/// message. Only reclaim a lease after both a generous per-seq age and a
/// device-wide writer-idle interval; this avoids duplicating legitimately slow
/// batches while guaranteeing that a lost completion cannot pin the ring.
const IN_FLIGHT_LEASE_TIMEOUT: Duration = Duration::from_secs(30);
const IN_FLIGHT_RESCUE_IDLE: Duration = Duration::from_secs(5);

impl BufferFlusher {
    pub(in crate::buffer::flush) fn write_window_bypass_ready(
        physical_fill_pct: u8,
        pressure_pct: u8,
        foreground_idle: Duration,
    ) -> bool {
        physical_fill_pct >= pressure_pct || foreground_idle >= WRITE_WINDOW_IDLE_BYPASS
    }

    pub(in crate::buffer::flush) fn stalled_lease_ready(
        lease_age: Duration,
        writer_idle: Duration,
    ) -> bool {
        lease_age >= IN_FLIGHT_LEASE_TIMEOUT && writer_idle >= IN_FLIGHT_RESCUE_IDLE
    }

    pub(in crate::buffer::flush) fn coalesce_loop(
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
        write_window: Duration,
        write_window_pressure_pct: u8,
    ) {
        // in_flight tracks how many pipeline units still reference each seq.
        // A multi-LBA entry split into 2 units → refcount=2 for that seq.
        // Only when refcount hits 0 does the seq leave in_flight.
        let mut in_flight: HashMap<u64, u32> = HashMap::new();
        let mut in_flight_started: HashMap<u64, Instant> = HashMap::new();
        let mut last_writer_units = metrics.flush_units_written.load(Ordering::Relaxed);
        let mut last_writer_progress = Instant::now();
        let mut last_orphan_scan = Instant::now();

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
        // With a residence window, ready notifications are intentionally
        // consumed before entries mature. Once they do mature, recover the
        // full 16 MiB coalesce admission window per retry pass rather than the
        // legacy 64-entry starvation sample (which would cap 4 KiB draining).
        let retry_snapshot_topup_limit = if write_window.is_zero() {
            64usize
        } else {
            Self::COALESCE_READY_WINDOW_BYTES / BLOCK_SIZE as usize
        };
        let mut last_retry_snapshot = Instant::now();
        let mut write_window_cutoff = Instant::now().checked_sub(write_window);
        let mut next_cutoff_refresh = Instant::now() + WRITE_WINDOW_RELEASE_QUANTUM;
        let mut last_buffer_appends = metrics.buffer_appends.load(Ordering::Relaxed);
        let mut last_foreground_append = Instant::now();
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
                            in_flight_started.remove(&seq);
                            in_flight_tracker.track_seq_done(seq);
                        }
                    }
                }
            }

            let mut new_entries = Vec::new();
            let mut seen = std::collections::HashSet::new();
            let mut queued_bytes = 0usize;
            let writer_units = metrics.flush_units_written.load(Ordering::Relaxed);
            if writer_units != last_writer_units {
                last_writer_units = writer_units;
                last_writer_progress = Instant::now();
            }
            let buffer_appends = metrics.buffer_appends.load(Ordering::Relaxed);
            if buffer_appends != last_buffer_appends {
                last_buffer_appends = buffer_appends;
                last_foreground_append = Instant::now();
            }
            let foreground_idle = last_foreground_append.elapsed();
            let bypass_write_window = Self::write_window_bypass_ready(
                pool.physical_fill_percentage_for_shard(shard_idx),
                write_window_pressure_pct,
                foreground_idle,
            );
            let admission_window_bytes = if foreground_idle >= WRITE_WINDOW_IDLE_BYPASS {
                Self::COALESCE_IDLE_READY_WINDOW_BYTES
            } else {
                Self::COALESCE_READY_WINDOW_BYTES
            };
            let admission_topup_limit = if write_window.is_zero() {
                retry_snapshot_topup_limit
            } else {
                admission_window_bytes / BLOCK_SIZE as usize
            };
            if !write_window.is_zero() && !bypass_write_window {
                let now = Instant::now();
                if now >= next_cutoff_refresh {
                    write_window_cutoff = now.checked_sub(write_window);
                    next_cutoff_refresh = now + WRITE_WINDOW_RELEASE_QUANTUM;
                }
            } else {
                // Refresh immediately when pressure subsides so the sliding
                // window resumes from current time instead of a stale cutoff.
                next_cutoff_refresh = Instant::now();
            }

            // Under a residence window, seq order also orders enqueue time for
            // this shard. Probe only the oldest entry first: if it has not
            // matured, cloning a full 4096-entry Arc snapshot is pure work
            // because the admission loop will stop at that first entry. Once
            // the head is admissible, expand to the normal 16 MiB batch so
            // mature-window drain throughput is unchanged. Recovered entries
            // have no resident payload and must bypass the residence window.
            let oldest_admission_snapshot = || {
                let probe = pool.oldest_ready_pending_arcs_for_shard(shard_idx, 1);
                let head_is_admissible = probe.first().is_some_and(|entry| {
                    bypass_write_window
                        || write_window.is_zero()
                        || entry.payload.is_none()
                        || write_window_cutoff.is_some_and(|cutoff| entry.enqueued_at <= cutoff)
                });
                if head_is_admissible && admission_topup_limit > 1 {
                    pool.oldest_ready_pending_arcs_for_shard_with_budget(
                        shard_idx,
                        admission_topup_limit,
                        admission_window_bytes,
                    )
                } else {
                    probe
                }
            };

            // Completion feedback is an optimisation, not durability state.
            // If the whole writer has been idle long enough, any old in-flight
            // pending seq is orphaned: no downstream stage is still making
            // progress on it. Drop its local lease and enqueue the durable LV2
            // entry directly. Metadb seq guards make this replay idempotent.
            let writer_idle = last_writer_progress.elapsed();
            if writer_idle >= IN_FLIGHT_RESCUE_IDLE {
                let stalled: Vec<u64> = in_flight_started
                    .iter()
                    .filter_map(|(seq, started)| {
                        (Self::stalled_lease_ready(started.elapsed(), writer_idle)
                            && pool.get_pending_arc(*seq).is_some())
                        .then_some(*seq)
                    })
                    .collect();
                for seq in stalled {
                    let old_refs = in_flight.remove(&seq).unwrap_or(0);
                    in_flight_started.remove(&seq);
                    in_flight_tracker.track_seq_done(seq);
                    tracing::warn!(
                        shard = shard_idx,
                        seq,
                        old_refs,
                        idle_ms = last_writer_progress.elapsed().as_millis() as u64,
                        "rescuing stalled flusher lease from durable LV2 entry"
                    );
                    let _ = Self::try_enqueue_pending_seq(
                        seq,
                        pool,
                        &in_flight,
                        in_flight_tracker,
                        &mut seen,
                        &mut queued_bytes,
                        &mut new_entries,
                        metrics,
                        admission_window_bytes,
                        skip_fully_superseded,
                        write_window,
                        write_window_cutoff,
                        bypass_write_window,
                    );
                }
            }

            // The ring-side pending_seqs set is an acceleration index. If its
            // bounded lookup is empty while the shard counter is non-zero,
            // cross-check the authoritative pending DashMap at low frequency.
            // This is deliberately gated behind writer idle so a healthy
            // 30-second residence window never pays a full-map scan.
            if new_entries.is_empty()
                && writer_idle >= IN_FLIGHT_RESCUE_IDLE
                && last_orphan_scan.elapsed() >= IN_FLIGHT_RESCUE_IDLE
                && pool.pending_count_for_shard(shard_idx) > 0
                && pool
                    .oldest_ready_pending_arcs_for_shard(shard_idx, 1)
                    .is_empty()
            {
                last_orphan_scan = Instant::now();
                let authoritative = pool.ready_pending_entries_arc_snapshot_for_shard(shard_idx);
                if !authoritative.is_empty() {
                    tracing::warn!(
                        shard = shard_idx,
                        pending_counter = pool.pending_count_for_shard(shard_idx),
                        authoritative_ready = authoritative.len(),
                        "bounded pending index empty while shard reports pending work"
                    );
                }
                for entry in authoritative.into_iter().take(retry_snapshot_topup_limit) {
                    if queued_bytes >= admission_window_bytes {
                        break;
                    }
                    let _ = Self::try_enqueue_pending_seq(
                        entry.seq,
                        pool,
                        &in_flight,
                        in_flight_tracker,
                        &mut seen,
                        &mut queued_bytes,
                        &mut new_entries,
                        metrics,
                        admission_window_bytes,
                        skip_fully_superseded,
                        write_window,
                        write_window_cutoff,
                        bypass_write_window,
                    );
                }
            }

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
                    admission_window_bytes,
                    skip_fully_superseded,
                    write_window,
                    write_window_cutoff,
                    bypass_write_window,
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
                            tracing::warn!(
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
                        let idle_ns = recv_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
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
                            admission_window_bytes,
                            skip_fully_superseded,
                            write_window,
                            write_window_cutoff,
                            bypass_write_window,
                        );
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                        let idle_ns = recv_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                        this_iter_idle_ns = this_iter_idle_ns.saturating_add(idle_ns);
                        metrics
                            .flush_coalesce_idle_ns
                            .fetch_add(idle_ns, Ordering::Relaxed);
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
                }
            }

            // Fairness for recovered / retried entries: seed each cycle with
            // the oldest ready pending seqs before draining the unbounded ready
            // channel. A sustained foreground writer can otherwise keep the
            // channel non-empty forever while crash-recovered payload-less
            // entries rely on periodic snapshots to make progress.
            let mut queued_oldest_snapshot = false;
            for entry in oldest_admission_snapshot() {
                if queued_bytes >= admission_window_bytes {
                    break;
                }
                match Self::try_enqueue_pending_seq(
                    entry.seq,
                    pool,
                    &in_flight,
                    in_flight_tracker,
                    &mut seen,
                    &mut queued_bytes,
                    &mut new_entries,
                    metrics,
                    admission_window_bytes,
                    skip_fully_superseded,
                    write_window,
                    write_window_cutoff,
                    bypass_write_window,
                ) {
                    EnqueuePendingSeq::Queued => queued_oldest_snapshot = true,
                    EnqueuePendingSeq::WindowFull => break,
                    EnqueuePendingSeq::Skipped(SkipReason::WriteWindow) => {
                        // oldest_pending_arcs is seq ordered. A live oldest
                        // entry that has not matured proves newer live entries
                        // are not ready either; avoid repeatedly walking them.
                        break;
                    }
                    EnqueuePendingSeq::Skipped(_) => {}
                }
            }

            // If the oldest-pending snapshot produced work, keep this cycle
            // focused on that priority batch. Otherwise a sustained foreground
            // writer can fill the 16 MiB ready window every iteration and turn
            // recovered/retried entries into "eventually" work again.
            if !queued_oldest_snapshot {
                while queued_bytes < admission_window_bytes {
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
                            admission_window_bytes,
                            skip_fully_superseded,
                            write_window,
                            write_window_cutoff,
                            bypass_write_window,
                        ),
                        EnqueuePendingSeq::WindowFull
                    ) {
                        break;
                    }
                }
            }

            // Safety net for recovered / retried entries: periodically
            // sample the oldest pending seqs in case some never went
            // through the ready channel (e.g. payload-less recovered
            // entries that were skipped once under memory pressure).
            //
            // The previous unbounded `ready_pending_entries_arc_snapshot_for_shard`
            // walked the entire pending DashMap, cloned every Arc, and
            // sorted by seq just to take 64 of them. Under saturation
            // (~280 k pending entries per shard) this single call was
            // the largest contributor to coalesce-thread CPU. The
            // bounded variant walks the ring's `log_order` head for
            // the oldest seqs only — O(limit) instead of O(all).
            if last_retry_snapshot.elapsed() >= retry_snapshot_interval
                && queued_bytes < admission_window_bytes
            {
                last_retry_snapshot = Instant::now();
                let mut topped_up = 0usize;
                for entry in oldest_admission_snapshot() {
                    if topped_up >= admission_topup_limit || queued_bytes >= admission_window_bytes
                    {
                        break;
                    }
                    let outcome = Self::try_enqueue_pending_seq(
                        entry.seq,
                        pool,
                        &in_flight,
                        in_flight_tracker,
                        &mut seen,
                        &mut queued_bytes,
                        &mut new_entries,
                        metrics,
                        admission_window_bytes,
                        skip_fully_superseded,
                        write_window,
                        write_window_cutoff,
                        bypass_write_window,
                    );
                    match outcome {
                        EnqueuePendingSeq::Queued => topped_up += 1,
                        EnqueuePendingSeq::Skipped(SkipReason::WriteWindow) => break,
                        EnqueuePendingSeq::WindowFull => break,
                        EnqueuePendingSeq::Skipped(_) => {}
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
            // Time the inside-coalesce_pending CPU separately from the
            // outer `coalesce_active_ns` so we can distinguish "stuck on
            // channel send" from "actually burning CPU in coalesce_slices".
            let coalesce_pending_start = Instant::now();
            let units = coalesce_pending(
                &new_entries,
                max_raw,
                max_lbas,
                &|vid| cache_ref.get(vid).copied().unwrap_or(CompressionAlgo::None),
                &skip_offsets,
                Some(metrics),
            );
            let coalesce_pending_ns = coalesce_pending_start
                .elapsed()
                .as_nanos()
                .min(u64::MAX as u128) as u64;
            metrics
                .flush_coalesce_pending_ns
                .fetch_add(coalesce_pending_ns, Ordering::Relaxed);
            metrics
                .flush_coalesce_pending_ops
                .fetch_add(1, Ordering::Relaxed);

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
                        in_flight_started.insert(*seq, Instant::now());
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
}
