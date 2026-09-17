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
        physical_pressure_pct: u8,
        physical_threshold_pct: u8,
        payload_pressure_pct: u8,
        payload_threshold_pct: u8,
        foreground_idle: Duration,
    ) -> bool {
        physical_pressure_pct >= physical_threshold_pct
            || payload_pressure_pct >= payload_threshold_pct
            || foreground_idle >= WRITE_WINDOW_IDLE_BYPASS
    }

    pub(in crate::buffer::flush) fn stalled_lease_ready(
        lease_age: Duration,
        writer_idle: Duration,
    ) -> bool {
        lease_age >= IN_FLIGHT_LEASE_TIMEOUT && writer_idle >= IN_FLIGHT_RESCUE_IDLE
    }

    /// The admission walk, lifted out of `coalesce_loop` so it can be timed as
    /// one thing (`flush_coalesce_walk_*`).
    ///
    /// Probe the oldest entry first: if it has not matured, cloning a full
    /// snapshot is pure work because the admission loop stops at that first
    /// entry anyway. Once the head is admissible, expand to the normal byte
    /// window so mature-window drain throughput is unchanged. Recovered entries
    /// have no resident payload and bypass the residence window.
    #[allow(clippy::too_many_arguments)]
    fn admission_walk(
        shard_idx: usize,
        pool: &WriteBufferPool,
        admission_topup_limit: usize,
        admission_window_bytes: usize,
        bypass_write_window: bool,
        write_window: Duration,
        write_window_cutoff: Option<Instant>,
        after_seq: Option<u64>,
    ) -> AdmissionWalk {
        let probe = pool.oldest_ready_pending_arcs_for_shard(shard_idx, 1);
        let head_is_admissible = probe.first().is_some_and(|entry| {
            bypass_write_window
                || write_window.is_zero()
                || entry.payload.is_none()
                || write_window_cutoff.is_some_and(|cutoff| entry.enqueued_at <= cutoff)
        });
        if head_is_admissible && admission_topup_limit > 1 {
            pool.admission_walk_for_shard(
                shard_idx,
                admission_topup_limit,
                admission_window_bytes,
                after_seq,
            )
        } else if after_seq.is_some() {
            // The head probe is only about the RESIDENCE window, and the head is
            // by definition at or below the cursor. Falling back to `probe` here
            // would hand the caller an entry it has already passed, so the
            // cursored walk must go to the index instead.
            pool.admission_walk_for_shard(
                shard_idx,
                admission_topup_limit.max(1),
                admission_window_bytes,
                after_seq,
            )
        } else {
            // Residence-window probe, not a budgeted walk: the stop reason would
            // be meaningless, so report it as budget-limited to keep it out of
            // the RangeExhausted tally.
            AdmissionWalk {
                stop: crate::buffer::commit_log::AdmissionWalkStop::EntryLimit,
                undurable_debt: 0,
                entries: probe,
            }
        }
    }
}

/// The blocking wait a lane spends on its own ready channel when it found
/// nothing to admit. One dedicated thread per shard can afford to sit here:
/// there is no other shard it could be serving.
const PRIVATE_READY_WAIT: Duration = Duration::from_millis(10);
/// The same wait for a pooled driver, but charged only after a full rotation
/// came up empty (see `coalesce_shared_loop`). Under load the rotation always
/// finds work and a driver never blocks here at all.
const SHARED_IDLE_READY_WAIT: Duration = Duration::from_millis(10);

/// What one pass of the admission stage achieved. A pooled driver needs to
/// tell "this shard had nothing" apart from "this shard is finished", because
/// only the first is a reason to move on and come back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::buffer::flush) enum CoalesceCycle {
    /// Queued at least one unit downstream.
    Worked,
    /// Nothing admissible on this pass.
    Idle,
    /// A channel disconnected: this lane is done for good.
    Stop,
}

/// The admission-stage settings that are identical for every shard, bundled so
/// a pooled driver can hand one reference to whichever lane it claims.
pub(in crate::buffer::flush) struct CoalesceParams<'a> {
    pub max_raw: usize,
    pub max_lbas: u32,
    pub skip_fully_superseded: bool,
    pub write_window: Duration,
    pub write_window_pressure_pct: u8,
    pub write_window_payload_pressure_pct: u8,
    pub flush_admission_qos: &'a FlushAdmissionQos,
}

/// Everything one shard's admission stage carries between passes.
///
/// This used to be thirteen locals on `coalesce_loop`'s stack, which is why
/// the stage needed one dedicated thread per shard: the state was the thread.
/// Lifting it out is what lets `flush.shared_coalesce_pool` serve 16 shards
/// from a smaller pool of drivers that claim a lane, run one pass, and hand it
/// back. Nothing in here is shared BETWEEN shards, so a claimed lane behaves
/// exactly as its private thread did.
pub(in crate::buffer::flush) struct CoalesceLaneState {
    /// How many pipeline units still reference each seq. A multi-LBA entry
    /// split into 2 units means refcount 2; the seq leaves only at 0.
    in_flight: HashMap<u64, u32>,
    in_flight_started: HashMap<u64, Instant>,
    /// Per-volume compression, cached to avoid repeated `MetaStore` lookups.
    vol_compression_cache: HashMap<String, CompressionAlgo>,
    last_writer_units: u64,
    last_writer_progress: Instant,
    last_orphan_scan: Instant,
    last_retry_snapshot: Instant,
    write_window_cutoff: Option<Instant>,
    next_cutoff_refresh: Instant,
    last_foreground_writes: u64,
    last_foreground_append: Instant,
    /// Head-stuck diagnostic throttle: at most one warn per shard per
    /// `DIAG_LOG_INTERVAL` while the head is older than the threshold.
    last_diag_log: Option<Instant>,
    /// Admission cursor: exclusive lower bound for the fast-path walk. Reset
    /// to `None` by the `retry_snapshot_interval` pass, so it can delay an
    /// entry by at most that interval and cannot strand one.
    admit_cursor: Option<u64>,
}

impl CoalesceLaneState {
    pub(in crate::buffer::flush) fn new(metrics: &EngineMetrics, write_window: Duration) -> Self {
        let now = Instant::now();
        Self {
            in_flight: HashMap::new(),
            in_flight_started: HashMap::new(),
            vol_compression_cache: HashMap::new(),
            last_writer_units: metrics.flush_units_written.load(Ordering::Relaxed),
            last_writer_progress: now,
            last_orphan_scan: now,
            last_retry_snapshot: now,
            write_window_cutoff: now.checked_sub(write_window),
            next_cutoff_refresh: now + WRITE_WINDOW_RELEASE_QUANTUM,
            last_foreground_writes: metrics.zone_write_dispatches.load(Ordering::Relaxed),
            last_foreground_append: now,
            last_diag_log: None,
            admit_cursor: None,
        }
    }
}

/// One shard's admission lane: its channels plus its claimable state.
///
/// The mutex is the claim. Two drivers must never run the same shard
/// concurrently — `admit_cursor` and `in_flight` would race and the same seq
/// could be admitted twice — so every pass takes the lane exclusively.
pub(in crate::buffer::flush) struct CoalesceLane {
    pub shard_idx: usize,
    pub tx: Sender<CoalesceUnit>,
    pub done_rx: Receiver<Vec<u64>>,
    pub state: parking_lot::Mutex<CoalesceLaneState>,
    /// Set once this lane's channels disconnect, so drivers stop offering it
    /// work instead of re-claiming a dead lane every rotation.
    pub stopped: AtomicBool,
}

impl CoalesceLane {
    pub(in crate::buffer::flush) fn new(
        shard_idx: usize,
        tx: Sender<CoalesceUnit>,
        done_rx: Receiver<Vec<u64>>,
        metrics: &EngineMetrics,
        write_window: Duration,
    ) -> Self {
        Self {
            shard_idx,
            tx,
            done_rx,
            state: parking_lot::Mutex::new(CoalesceLaneState::new(metrics, write_window)),
            stopped: AtomicBool::new(false),
        }
    }
}

impl BufferFlusher {
    /// One dedicated admission thread per shard: the original shape, and still
    /// the default. `flush.shared_coalesce_pool` switches to
    /// `coalesce_shared_loop` instead.
    #[allow(clippy::too_many_arguments)]
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
        write_window_payload_pressure_pct: u8,
        flush_admission_qos: &FlushAdmissionQos,
    ) {
        let params = CoalesceParams {
            max_raw,
            max_lbas,
            skip_fully_superseded,
            write_window,
            write_window_pressure_pct,
            write_window_payload_pressure_pct,
            flush_admission_qos,
        };
        let mut state = CoalesceLaneState::new(metrics, write_window);
        while running.load(Ordering::Relaxed) {
            if Self::coalesce_cycle(
                shard_idx,
                tx,
                done_rx,
                pool,
                meta,
                running,
                in_flight_tracker,
                metrics,
                &params,
                &mut state,
                PRIVATE_READY_WAIT,
            ) == CoalesceCycle::Stop
            {
                return;
            }
        }
    }

    /// Serve every shard's admission stage from a pool of drivers, each of
    /// which claims one lane, runs a single pass, and releases it.
    ///
    /// ⚠ **Fairness is a correctness requirement here, not a latency
    /// nicety.** A shard that is never claimed never admits, and its durable
    /// LV2 entries strand: the ring tail cannot advance past a pending seq, so
    /// starving one lane eventually wedges the whole log. The guarantee is
    /// structural rather than best-effort: `next_lane` is ONE counter shared by
    /// every driver, so consecutive claims hand out consecutive shards and each
    /// shard is offered exactly once per `lanes.len()` increments. A lane whose
    /// `try_lock` fails is not skipped-and-forgotten — a failed lock means
    /// another driver is serving it right now, which is the outcome the offer
    /// wanted anyway.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::buffer::flush) fn coalesce_shared_loop(
        lanes: &[CoalesceLane],
        next_lane: &std::sync::atomic::AtomicUsize,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        running: &AtomicBool,
        in_flight_tracker: &FlusherInFlightTracker,
        metrics: &EngineMetrics,
        params: &CoalesceParams<'_>,
    ) {
        if lanes.is_empty() {
            return;
        }
        // Consecutive offers, by THIS driver, that produced nothing. Reaching a
        // full rotation means this driver found no work anywhere, and it earns
        // one blocking pass: it parks on the ready channel of the next lane it
        // draws, waking the moment that shard gets work instead of spinning
        // the rotation.
        //
        // ⚠ The counter resets when the wait is ARMED, not only when work is
        // found, and that detail is load-bearing. Letting it keep climbing
        // makes every later pass a blocking one, so with one hot shard among
        // many idle ones most drivers would sit parked on idle lanes and the
        // hot shard would only be served as fast as those parks expire.
        // Resetting bounds a driver to one park per `lanes.len() + 1` passes:
        // enough to stop the spin, not enough to stop serving.
        let mut barren = 0usize;
        while running.load(Ordering::Relaxed) {
            let idx = next_lane.fetch_add(1, Ordering::Relaxed) % lanes.len();
            let lane = &lanes[idx];
            if lane.stopped.load(Ordering::Relaxed) {
                barren = barren.saturating_add(1);
            } else if let Some(mut state) = lane.state.try_lock() {
                let ready_wait = {
                    let (wait, carry) = Self::shared_ready_wait(barren, lanes.len());
                    barren = carry;
                    wait
                };
                match Self::coalesce_cycle(
                    lane.shard_idx,
                    &lane.tx,
                    &lane.done_rx,
                    pool,
                    meta,
                    running,
                    in_flight_tracker,
                    metrics,
                    params,
                    &mut state,
                    ready_wait,
                ) {
                    CoalesceCycle::Worked => barren = 0,
                    CoalesceCycle::Idle => barren = barren.saturating_add(1),
                    CoalesceCycle::Stop => {
                        lane.stopped.store(true, Ordering::Relaxed);
                        barren = barren.saturating_add(1);
                    }
                }
            } else {
                // Held by another driver, i.e. already being served.
                barren = barren.saturating_add(1);
            }
            // Every lane disconnected: nothing left to drive. Checked only on
            // the idle path so the hot rotation pays nothing for it.
            if barren >= lanes.len() && lanes.iter().all(|l| l.stopped.load(Ordering::Relaxed)) {
                return;
            }
        }
    }

    /// Whether this pass may block on its lane's ready channel, plus the
    /// barren counter to carry forward. Lifted out of `coalesce_shared_loop`
    /// so the reset rule — which is what keeps one hot lane from being
    /// starved by drivers parked on idle ones — is testable without a pool.
    fn shared_ready_wait(barren: usize, lanes: usize) -> (Duration, usize) {
        if barren >= lanes {
            (SHARED_IDLE_READY_WAIT, 0)
        } else {
            (Duration::ZERO, barren)
        }
    }

    /// One pass of the admission stage over a single shard.
    ///
    /// `ready_wait` is how long this pass may block on the shard's ready
    /// channel when it finds nothing: `PRIVATE_READY_WAIT` for a dedicated
    /// thread, `ZERO` for a pooled driver that still has other lanes to offer.
    #[allow(clippy::too_many_arguments)]
    fn coalesce_cycle(
        shard_idx: usize,
        tx: &Sender<CoalesceUnit>,
        done_rx: &Receiver<Vec<u64>>,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        running: &AtomicBool,
        in_flight_tracker: &FlusherInFlightTracker,
        metrics: &EngineMetrics,
        params: &CoalesceParams<'_>,
        st: &mut CoalesceLaneState,
        ready_wait: Duration,
    ) -> CoalesceCycle {
        let CoalesceParams {
            max_raw,
            max_lbas,
            skip_fully_superseded,
            write_window,
            write_window_pressure_pct,
            write_window_payload_pressure_pct,
            flush_admission_qos,
        } = *params;
        let vol_compression = |vol_id: &str| -> CompressionAlgo {
            // Can't use cache from closure due to borrow rules — inlined below
            if let Ok(Some(vc)) = meta.get_volume(&crate::types::VolumeId(vol_id.to_string())) {
                vc.compression
            } else {
                CompressionAlgo::None
            }
        };
        let retry_snapshot_interval = Duration::from_millis(100);
        // A pass is bounded by the 16 MiB admission window; this entry count
        // only bounds how deep into the ring head we walk in order to fill it.
        // Deriving it from the window is therefore the only self-consistent
        // choice, and it used to hold for the residence-window case only: the
        // `write_window == 0` default kept a legacy 64-entry sample, which caps
        // 4 KiB draining at 64 entries per pass no matter how much is pending.
        // Most of those 64 are seqs still traversing the pipeline (skipped as
        // SkipReason::InFlight), so only ~4 fresh entries were admitted per
        // pass. Measured on nvme-box, 4 KiB random, 480 s windows: units per
        // coalesce run 3.89 -> 407, drain 6.20 -> 257.69 MiB/s, and metadata
        // write amplification 3.15x -> 1.11x because coalescing could finally
        // find adjacent work.
        let retry_snapshot_topup_limit = Self::COALESCE_READY_WINDOW_BYTES / BLOCK_SIZE as usize;
        // Mirror of `st.write_window_cutoff` for this pass. Kept as a plain
        // local because the admission-walk closure below captures it, and a
        // capture of `st` would collide with the `&mut st.in_flight` the
        // admission loop needs.
        let mut window_cutoff = st.write_window_cutoff;
        const DIAG_LOG_INTERVAL: Duration = Duration::from_secs(30);
        const DIAG_AGE_THRESHOLD_MS: u64 = 3000;
        let iter_start = Instant::now();
        let loop_start = iter_start;
        let mut this_iter_idle_ns: u64 = 0;
        // Drain completed seqs from writer feedback — decrement refcounts
        while let Ok(seqs) = done_rx.try_recv() {
            for seq in seqs {
                if let Some(count) = st.in_flight.get_mut(&seq) {
                    *count -= 1;
                    if *count == 0 {
                        st.in_flight.remove(&seq);
                        st.in_flight_started.remove(&seq);
                        in_flight_tracker.track_seq_done(seq);
                    }
                }
            }
        }

        let mut new_entries = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut queued_bytes = 0usize;
        let writer_units = metrics.flush_units_written.load(Ordering::Relaxed);
        if writer_units != st.last_writer_units {
            st.last_writer_units = writer_units;
            st.last_writer_progress = Instant::now();
        }
        let foreground_writes = metrics.zone_write_dispatches.load(Ordering::Relaxed);
        if foreground_writes != st.last_foreground_writes {
            st.last_foreground_writes = foreground_writes;
            st.last_foreground_append = Instant::now();
        }
        let foreground_idle = st.last_foreground_append.elapsed();
        let physical_pressure_pct = pool.physical_fill_percentage_for_shard(shard_idx);
        let payload_pressure_pct = pool.payload_fill_percentage();
        let bypass_write_window = Self::write_window_bypass_ready(
            physical_pressure_pct,
            write_window_pressure_pct,
            payload_pressure_pct,
            write_window_payload_pressure_pct,
            foreground_idle,
        );
        let admission_window_bytes = if foreground_idle >= WRITE_WINDOW_IDLE_BYPASS {
            Self::COALESCE_IDLE_READY_WINDOW_BYTES
        } else {
            Self::COALESCE_READY_WINDOW_BYTES
        };
        // Same reasoning as `retry_snapshot_topup_limit`: the window is the
        // real bound, so the entry count should just track it. Keeping a
        // separate constant for `write_window == 0` is what capped the
        // default configuration.
        let admission_topup_limit = admission_window_bytes / BLOCK_SIZE as usize;
        if !write_window.is_zero() && !bypass_write_window {
            let now = Instant::now();
            if now >= st.next_cutoff_refresh {
                window_cutoff = now.checked_sub(write_window);
                st.next_cutoff_refresh = now + WRITE_WINDOW_RELEASE_QUANTUM;
            }
        } else {
            // Refresh immediately when pressure subsides so the sliding
            // window resumes from current time instead of a stale cutoff.
            st.next_cutoff_refresh = Instant::now();
        }

        // Under a residence window, seq order also orders enqueue time for
        // this shard. Probe only the oldest entry first: if it has not
        // matured, cloning a full 4096-entry Arc snapshot is pure work
        // because the admission loop will stop at that first entry. Once
        // the head is admissible, expand to the normal 16 MiB batch so
        // mature-window drain throughput is unchanged. Recovered entries
        // have no resident payload and must bypass the residence window.
        // `after` is the admission cursor: `None` walks from the oldest
        // pending seq, `Some(seq)` resumes after it. See `st.admit_cursor`.
        let oldest_admission_snapshot = |after: Option<u64>| {
            // Timed because this is the walk that used to restart at the
            // oldest pending seq every cycle: `arcs` vs `admit_queued` is how
            // much of it was re-examining entries already in flight.
            let walk_start = Instant::now();
            let out = Self::admission_walk(shard_idx, pool, admission_topup_limit,
                admission_window_bytes, bypass_write_window, write_window,
                window_cutoff, after);
            metrics
                .flush_coalesce_walk_ns
                .fetch_add(walk_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                    Ordering::Relaxed);
            metrics.flush_coalesce_walk_calls.fetch_add(1, Ordering::Relaxed);
            metrics
                .flush_coalesce_walk_arcs
                .fetch_add(out.entries.len() as u64, Ordering::Relaxed);
            // The discriminator for "nothing is saturated and the ring still
            // backs up": `stop_exhausted` means the coalescer ran out of
            // LV2-durable work and is paced by durability, while
            // `stop_budget` means admissible work was left on the table and
            // the constraint is downstream. `undurable_debt` sizes the
            // appended-but-not-durable backlog it cannot touch.
            match out.stop {
                AdmissionWalkStop::RangeExhausted => &metrics.flush_coalesce_walk_stop_exhausted,
                AdmissionWalkStop::EntryLimit | AdmissionWalkStop::ByteLimit => {
                    &metrics.flush_coalesce_walk_stop_budget
                }
            }
            .fetch_add(1, Ordering::Relaxed);
            metrics
                .flush_coalesce_walk_undurable_debt_sum
                .fetch_add(out.undurable_debt, Ordering::Relaxed);
            out.entries
        };
        // Completion feedback is an optimisation, not durability state.
        // If the whole writer has been idle long enough, any old in-flight
        // pending seq is orphaned: no downstream stage is still making
        // progress on it. Drop its local lease and enqueue the durable LV2
        // entry directly. Metadb seq guards make this replay idempotent.
        let writer_idle = st.last_writer_progress.elapsed();
        if writer_idle >= IN_FLIGHT_RESCUE_IDLE {
            let stalled: Vec<u64> = st.in_flight_started
                .iter()
                .filter_map(|(seq, started)| {
                    (Self::stalled_lease_ready(started.elapsed(), writer_idle)
                        && pool.get_pending_arc(*seq).is_some())
                    .then_some(*seq)
                })
                .collect();
            for seq in stalled {
                let old_refs = st.in_flight.remove(&seq).unwrap_or(0);
                st.in_flight_started.remove(&seq);
                in_flight_tracker.track_seq_done(seq);
                tracing::warn!(
                    shard = shard_idx,
                    seq,
                    old_refs,
                    idle_ms = st.last_writer_progress.elapsed().as_millis() as u64,
                    "rescuing stalled flusher lease from durable LV2 entry"
                );
                let _ = Self::try_enqueue_pending_seq(
                    seq,
                    pool,
                    &st.in_flight,
                    in_flight_tracker,
                    &mut seen,
                    &mut queued_bytes,
                    &mut new_entries,
                    metrics,
                    admission_window_bytes,
                    skip_fully_superseded,
                    write_window,
                    window_cutoff,
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
            && st.last_orphan_scan.elapsed() >= IN_FLIGHT_RESCUE_IDLE
            && pool.pending_count_for_shard(shard_idx) > 0
            && pool
                .oldest_ready_pending_arcs_for_shard(shard_idx, 1)
                .is_empty()
        {
            st.last_orphan_scan = Instant::now();
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
                    &st.in_flight,
                    in_flight_tracker,
                    &mut seen,
                    &mut queued_bytes,
                    &mut new_entries,
                    metrics,
                    admission_window_bytes,
                    skip_fully_superseded,
                    write_window,
                    window_cutoff,
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
                &st.in_flight,
                in_flight_tracker,
                &mut seen,
                &mut queued_bytes,
                &mut new_entries,
                metrics,
                admission_window_bytes,
                skip_fully_superseded,
                write_window,
                window_cutoff,
                bypass_write_window,
            );
            if let Some((lba_count, flushed_count, age_ms, vol_id)) = diag_snapshot {
                if age_ms >= DIAG_AGE_THRESHOLD_MS {
                    let due = match st.last_diag_log {
                        Some(ts) => ts.elapsed() >= DIAG_LOG_INTERVAL,
                        None => true,
                    };
                    if due {
                        let in_flight_count = st.in_flight.get(&seq).copied().unwrap_or(0);
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
                        st.last_diag_log = Some(Instant::now());
                    }
                }
            }
        }

        if new_entries.is_empty() {
            let recv_start = Instant::now();
            match pool.recv_ready_timeout_for_shard(shard_idx, ready_wait) {
                Ok(seq) => {
                    let idle_ns = recv_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                    this_iter_idle_ns = this_iter_idle_ns.saturating_add(idle_ns);
                    metrics
                        .flush_coalesce_idle_ns
                        .fetch_add(idle_ns, Ordering::Relaxed);
                    let _ = Self::try_enqueue_pending_seq(
                        seq,
                        pool,
                        &st.in_flight,
                        in_flight_tracker,
                        &mut seen,
                        &mut queued_bytes,
                        &mut new_entries,
                        metrics,
                        admission_window_bytes,
                        skip_fully_superseded,
                        write_window,
                        window_cutoff,
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
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return CoalesceCycle::Stop,
            }
        }

        // Fairness for recovered / retried entries: seed each cycle with
        // the oldest ready pending seqs before draining the unbounded ready
        // channel. A sustained foreground writer can otherwise keep the
        // channel non-empty forever while crash-recovered payload-less
        // entries rely on periodic snapshots to make progress.
        let mut queued_oldest_snapshot = false;
        // Cursor bookkeeping for this pass. `passed` is the highest seq we
        // may safely resume after; `blocked` is the lowest seq that must be
        // re-examined next time, and it wins.
        let mut passed: Option<u64> = None;
        let mut blocked: Option<u64> = None;
        for entry in oldest_admission_snapshot(st.admit_cursor) {
            if queued_bytes >= admission_window_bytes {
                blocked = Some(blocked.map_or(entry.seq, |b: u64| b.min(entry.seq)));
                break;
            }
            match Self::try_enqueue_pending_seq(
                entry.seq,
                pool,
                &st.in_flight,
                in_flight_tracker,
                &mut seen,
                &mut queued_bytes,
                &mut new_entries,
                metrics,
                admission_window_bytes,
                skip_fully_superseded,
                write_window,
                window_cutoff,
                bypass_write_window,
            ) {
                EnqueuePendingSeq::Queued => {
                    queued_oldest_snapshot = true;
                    passed = Some(passed.map_or(entry.seq, |p: u64| p.max(entry.seq)));
                }
                EnqueuePendingSeq::WindowFull => {
                    // This entry did not fit; it is still admissible.
                    blocked = Some(blocked.map_or(entry.seq, |b: u64| b.min(entry.seq)));
                    break;
                }
                EnqueuePendingSeq::Skipped(SkipReason::WriteWindow) => {
                    // oldest_pending_arcs is seq ordered. A live oldest
                    // entry that has not matured proves newer live entries
                    // are not ready either; avoid repeatedly walking them.
                    // Transient: the cursor must not pass it.
                    blocked = Some(blocked.map_or(entry.seq, |b: u64| b.min(entry.seq)));
                    break;
                }
                EnqueuePendingSeq::Skipped(SkipReason::RetryDeferred) => {
                    // Also transient — a backoff timer, not a pipeline state.
                    blocked = Some(blocked.map_or(entry.seq, |b: u64| b.min(entry.seq)));
                }
                EnqueuePendingSeq::Skipped(_) => {
                    // InFlight / AlreadySeen / Superseded / NoPendingEntry:
                    // either the pipeline owns the seq and will hand it back
                    // through `done_rx`, or it is gone. Safe to resume after.
                    passed = Some(passed.map_or(entry.seq, |p: u64| p.max(entry.seq)));
                }
            }
        }
        // A transient block wins over anything we walked past, so the next
        // pass re-examines it. Otherwise resume after the furthest seq this
        // pass settled. `saturating_sub(1)` on seq 0 leaves the cursor at 0,
        // which the exclusive bound turns into "skip seq 0" — seq numbering
        // starts at 1, so nothing is lost.
        st.admit_cursor = match (blocked, passed) {
            (Some(b), _) => b.checked_sub(1),
            (None, Some(p)) => Some(p),
            (None, None) => st.admit_cursor,
        };

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
                        &st.in_flight,
                        in_flight_tracker,
                        &mut seen,
                        &mut queued_bytes,
                        &mut new_entries,
                        metrics,
                        admission_window_bytes,
                        skip_fully_superseded,
                        write_window,
                        window_cutoff,
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
        if st.last_retry_snapshot.elapsed() >= retry_snapshot_interval
            && queued_bytes < admission_window_bytes
        {
            st.last_retry_snapshot = Instant::now();
            // From the HEAD, unconditionally. This is the bound on how long
            // the cursor may hide an entry it walked past: at most
            // `retry_snapshot_interval`. Anything the cursored fast path
            // skipped for a reason that later stopped applying is picked up
            // here, which is why the cursor needs no liveness proof of its
            // own.
            st.admit_cursor = None;
            let mut topped_up = 0usize;
            for entry in oldest_admission_snapshot(None) {
                if topped_up >= admission_topup_limit || queued_bytes >= admission_window_bytes
                {
                    break;
                }
                let outcome = Self::try_enqueue_pending_seq(
                    entry.seq,
                    pool,
                    &st.in_flight,
                    in_flight_tracker,
                    &mut seen,
                    &mut queued_bytes,
                    &mut new_entries,
                    metrics,
                    admission_window_bytes,
                    skip_fully_superseded,
                    write_window,
                    window_cutoff,
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
            return CoalesceCycle::Idle;
        }

        new_entries = pool.hydrate_pending_entries_for_shard(shard_idx, new_entries);
        if new_entries.is_empty() {
            let iter_total = iter_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            metrics.flush_coalesce_active_ns.fetch_add(
                iter_total.saturating_sub(this_iter_idle_ns),
                Ordering::Relaxed,
            );
            return CoalesceCycle::Idle;
        }

        // Build per-volume compression lookup using cache
        for entry in &new_entries {
            st.vol_compression_cache
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

        let cache_ref = &st.vol_compression_cache;
        // Time the inside-coalesce_pending CPU separately from the
        // outer `coalesce_active_ns` so we can distinguish "stuck on
        // channel send" from "actually burning CPU in coalesce_slices".
        let coalesce_pending_start = Instant::now();
        let mut units = coalesce_pending(
            &new_entries,
            max_raw,
            max_lbas,
            &|vid| cache_ref.get(vid).copied().unwrap_or(CompressionAlgo::None),
            &skip_offsets,
            Some(metrics),
        );
        // Stamp this lane's identity so a shared compress pool
        // (FlushConfig::shared_compress_pool) can route its output back
        // to the right shard.
        for unit in &mut units {
            unit.shard_idx = shard_idx;
        }
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

        for unit in units {
            // The only flush QoS gate lives here, before any downstream
            // dedup/compress/writer work. Bounded channels propagate this
            // one global permit stream through the whole pipeline.
            flush_admission_qos.admit(unit.raw_len() as u64, running);

            // Count references immediately before publication so paced
            // units do not look like downstream in-flight work.
            for (seq, _, _) in &unit.seq_lba_ranges {
                let count = st.in_flight.entry(*seq).or_insert(0);
                if *count == 0 {
                    in_flight_tracker.track_seq_start(*seq, &unit.vol_id, unit.vol_created_at);
                    st.in_flight_started.insert(*seq, Instant::now());
                }
                *count += 1;
            }
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
                return CoalesceCycle::Stop;
            }
        }

        let iter_total = iter_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        metrics.flush_coalesce_active_ns.fetch_add(
            iter_total.saturating_sub(this_iter_idle_ns),
            Ordering::Relaxed,
        );
        // Whole-iteration wall. `active + idle` already claims to cover it,
        // but that pair is what disagreed with the OS (24-44% active vs 93%
        // thread CPU), so keep an independent total to anchor the residual.
        metrics.flush_coalesce_loop_ns.fetch_add(
            loop_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        metrics
            .flush_coalesce_loop_iters
            .fetch_add(1, Ordering::Relaxed);

        CoalesceCycle::Worked
    }
}

#[cfg(test)]
mod shared_coalesce_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// `coalesce_shared_loop`'s fairness guarantee is that ONE counter is
    /// shared by every driver, so consecutive claims hand out consecutive
    /// lanes and each lane is offered exactly once per rotation.
    ///
    /// The regression this guards is the obvious "optimisation": giving each
    /// driver its own cursor. That looks equivalent and is not — independent
    /// cursors sample the same lanes repeatedly and leave others unoffered for
    /// arbitrarily long, and an admission lane that is never offered never
    /// admits, so its durable LV2 entries strand and the ring tail cannot
    /// advance past them. That is a wedge, not a slowdown.
    #[test]
    fn one_shared_cursor_offers_every_lane_exactly_once_per_rotation() {
        const LANES: usize = 16;
        const DRIVERS: usize = 5;
        const ROTATIONS: usize = 40;

        let next = Arc::new(AtomicUsize::new(0));
        let draws = Arc::new(std::sync::Mutex::new(Vec::<(usize, usize)>::new()));
        let total = LANES * ROTATIONS;
        let remaining = Arc::new(AtomicUsize::new(total));

        let handles: Vec<_> = (0..DRIVERS)
            .map(|_| {
                let next = next.clone();
                let draws = draws.clone();
                let remaining = remaining.clone();
                std::thread::spawn(move || {
                    while remaining
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |r| {
                            r.checked_sub(1)
                        })
                        .is_ok()
                    {
                        // Exactly the draw `coalesce_shared_loop` performs.
                        let ticket = next.fetch_add(1, Ordering::Relaxed);
                        draws.lock().unwrap().push((ticket, ticket % LANES));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let mut draws = draws.lock().unwrap().clone();
        draws.sort_unstable();
        assert_eq!(draws.len(), total);
        // Every ticket handed out exactly once ACROSS drivers. Per-driver
        // cursors would repeat tickets and fail here.
        for (i, (ticket, _)) in draws.iter().enumerate() {
            assert_eq!(*ticket, i, "ticket {i} was handed out more than once");
        }
        // And each window of LANES consecutive offers is a permutation of the
        // lanes, i.e. no lane waits longer than one rotation to be offered.
        for rotation in draws.chunks(LANES) {
            let mut seen: Vec<usize> = rotation.iter().map(|(_, lane)| *lane).collect();
            seen.sort_unstable();
            assert_eq!(
                seen,
                (0..LANES).collect::<Vec<_>>(),
                "a rotation did not offer every lane exactly once"
            );
        }
    }

    /// The idle park must be armed *and disarmed*. If the barren counter kept
    /// climbing, every pass after the first idle rotation would block, so with
    /// one hot shard among many idle ones the drivers would spend nearly all
    /// their time parked on idle lanes and the hot shard would only be served
    /// as fast as those parks expired.
    #[test]
    fn an_armed_idle_park_resets_so_drivers_keep_rotating() {
        const LANES: usize = 16;
        let mut barren = 0usize;
        let mut parked = 0usize;
        let passes = 1_000;
        for _ in 0..passes {
            let (wait, carry) = BufferFlusher::shared_ready_wait(barren, LANES);
            barren = carry;
            if wait.is_zero() {
                // An idle pass that did not park still counts toward the next
                // rotation, exactly as the driver loop does.
                barren += 1;
            } else {
                parked += 1;
            }
        }
        // One park per LANES+1 passes, never more: bounded spin, and 16 of
        // every 17 passes still rotate and can pick up the hot lane.
        let expected = passes / (LANES + 1);
        assert!(
            parked <= expected + 1,
            "parked {parked} of {passes} passes, expected at most {}",
            expected + 1
        );
        assert!(parked > 0, "a permanently idle driver never parked at all");
    }

    /// The first pass is never a parking pass: a driver must offer every lane
    /// once before concluding there is no work anywhere.
    #[test]
    fn a_driver_rotates_a_full_lap_before_parking() {
        const LANES: usize = 16;
        for barren in 0..LANES {
            let (wait, carry) = BufferFlusher::shared_ready_wait(barren, LANES);
            assert!(wait.is_zero(), "parked after only {barren} empty offers");
            assert_eq!(carry, barren);
        }
        let (wait, carry) = BufferFlusher::shared_ready_wait(LANES, LANES);
        assert_eq!(wait, SHARED_IDLE_READY_WAIT);
        assert_eq!(carry, 0);
    }

    /// The pool hands `&[CoalesceLane]` to every driver, so the lane — and the
    /// claim mutex that makes a pass exclusive — must cross threads.
    #[test]
    fn a_lane_is_shareable_across_drivers() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CoalesceLane>();
    }
}
