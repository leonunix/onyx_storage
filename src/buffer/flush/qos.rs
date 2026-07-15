use super::*;

const MIB: u64 = 1024 * 1024;
const CONTROL_INTERVAL: Duration = Duration::from_millis(250);
// Keep protection through short completion droughts. A one-second grace can
// misclassify a saturated QD workload as idle precisely when LV2 durability is
// stalled and unleash the background work that caused the stall.
const FOREGROUND_IDLE_GRACE: Duration = Duration::from_secs(5);
const LOW_LATENCY_STREAK_TO_RECOVER: u8 = 5;
const MIN_FEEDBACK_SAMPLES: u64 = 64;
const MAX_SLEEP: Duration = Duration::from_millis(10);

const QOS_MODE_IDLE: u64 = 0;
const QOS_MODE_PROTECTED: u64 = 1;
const QOS_MODE_RECOVERY: u64 = 2;
const QOS_MODE_EMERGENCY: u64 = 3;

#[derive(Debug, Clone, Copy)]
pub(super) struct FlushAdmissionQosConfig {
    pub target_p99_ns: u64,
    pub active_rate_bps: u64,
    pub min_rate_bps: u64,
    pub recovery_rate_bps: u64,
    pub burst_bytes: u64,
    pub recovery_pct: u8,
    pub emergency_pct: u8,
}

impl FlushAdmissionQosConfig {
    pub(super) fn from_flush(config: &FlushConfig) -> Self {
        let active_mib = nonzero_or(config.foreground_flush_active_mib_s, 128);
        let min_mib = nonzero_or(config.foreground_flush_min_mib_s, 32).min(active_mib);
        let recovery_mib = nonzero_or(config.foreground_flush_max_mib_s, 384).max(active_mib);
        let recovery_pct = nonzero_or_u8(config.foreground_flush_recovery_pct, 40).min(99);
        let emergency_pct = nonzero_or_u8(config.foreground_flush_emergency_pct, 65)
            .clamp(recovery_pct.saturating_add(1), 100);
        let configured_burst = nonzero_or(config.foreground_flush_burst_mib, 8).saturating_mul(MIB);
        let max_unit_bytes = u64::try_from(config.coalesce_max_raw_bytes)
            .unwrap_or(u64::MAX)
            .max(BLOCK_SIZE as u64);
        Self {
            target_p99_ns: config
                .foreground_flush_target_p99_ms
                .saturating_mul(1_000_000),
            active_rate_bps: active_mib.saturating_mul(MIB),
            min_rate_bps: min_mib.saturating_mul(MIB),
            recovery_rate_bps: recovery_mib.saturating_mul(MIB),
            // A token cap below one indivisible unit can never satisfy that
            // unit. Clamp here rather than relying on every caller to split a
            // compression unit, whose on-disk mapping must stay contiguous.
            burst_bytes: configured_burst.max(max_unit_bytes),
            recovery_pct,
            emergency_pct,
        }
    }

    fn enabled(self) -> bool {
        self.target_p99_ns > 0 && self.active_rate_bps > 0
    }
}

fn nonzero_or(value: u64, default: u64) -> u64 {
    if value == 0 {
        default
    } else {
        value
    }
}

fn nonzero_or_u8(value: u8, default: u8) -> u8 {
    if value == 0 {
        default
    } else {
        value
    }
}

#[derive(Debug)]
struct QosState {
    last_control: Instant,
    last_refill: Instant,
    last_foreground_ops: u64,
    last_foreground_read_dispatches: u64,
    last_foreground_bytes: u64,
    last_foreground_seen: Option<Instant>,
    previous_durable_histogram: Vec<u64>,
    protected_rate_bps: u64,
    effective_rate_bps: u64,
    tokens: u64,
    low_latency_streak: u8,
    mode: u64,
    next_waiter_id: u64,
    waiters: VecDeque<u64>,
}

/// One device-wide admission controller shared by every flush lane.
///
/// Pacing at the coalescer output is early enough to stop dedup/compress CPU
/// bursts and naturally backpressures all bounded downstream channels. Idle
/// replay and physical-ring emergencies bypass the limiter: foreground QoS is
/// never allowed to compromise crash recovery or durability capacity.
pub(super) struct FlushAdmissionQos {
    config: FlushAdmissionQosConfig,
    pool: Arc<WriteBufferPool>,
    metrics: Arc<EngineMetrics>,
    state: parking_lot::Mutex<QosState>,
    waiters_changed: parking_lot::Condvar,
}

impl FlushAdmissionQos {
    pub(super) fn new(
        config: FlushAdmissionQosConfig,
        pool: Arc<WriteBufferPool>,
        metrics: Arc<EngineMetrics>,
    ) -> Self {
        let now = Instant::now();
        // Zone submission is recorded after LV2 append publication but before
        // durability wait. Unlike buffer_appends it excludes GC relocation,
        // and unlike volume_write_ops it cannot disappear while completions
        // are stalled by the very latency this controller is protecting.
        let foreground_ops = metrics.zone_write_dispatches.load(Ordering::Relaxed);
        let foreground_bytes = metrics
            .zone_write_lbas
            .load(Ordering::Relaxed)
            .saturating_mul(BLOCK_SIZE as u64);
        let foreground_read_dispatches = metrics.zone_read_dispatches.load(Ordering::Relaxed);
        let previous_durable_histogram =
            metrics.buffer_append_wait_durable_foreground_fine_snapshot();
        metrics
            .flush_qos_mode
            .store(QOS_MODE_IDLE, Ordering::Relaxed);
        metrics
            .flush_qos_rate_bytes_per_sec
            .store(0, Ordering::Relaxed);
        Self {
            config,
            pool,
            metrics,
            state: parking_lot::Mutex::new(QosState {
                last_control: now,
                last_refill: now,
                last_foreground_ops: foreground_ops,
                last_foreground_read_dispatches: foreground_read_dispatches,
                last_foreground_bytes: foreground_bytes,
                last_foreground_seen: None,
                previous_durable_histogram,
                protected_rate_bps: config.active_rate_bps,
                effective_rate_bps: 0,
                tokens: config.burst_bytes,
                low_latency_streak: 0,
                mode: QOS_MODE_IDLE,
                next_waiter_id: 0,
                waiters: VecDeque::new(),
            }),
            waiters_changed: parking_lot::Condvar::new(),
        }
    }

    pub(super) fn admit(&self, bytes: u64, running: &AtomicBool) {
        if bytes == 0 || !self.config.enabled() || !running.load(Ordering::Relaxed) {
            return;
        }
        let mut state = self.state.lock();
        if !running.load(Ordering::Relaxed) {
            return;
        }
        let waiter_id = state.next_waiter_id;
        state.next_waiter_id = state.next_waiter_id.wrapping_add(1);
        state.waiters.push_back(waiter_id);
        self.record_waiter_depth(state.waiters.len());

        let mut recorded_wait = false;
        let mut wait_started = None;
        loop {
            if !running.load(Ordering::Relaxed) {
                self.remove_waiter(&mut state, waiter_id);
                self.record_wait_max(wait_started);
                return;
            }
            let now = Instant::now();
            self.observe_foreground(&mut state, now);
            self.refill(&mut state, now);
            if now.duration_since(state.last_control) >= CONTROL_INTERVAL {
                self.update_control(&mut state, now);
            }

            let is_front = state.waiters.front().copied() == Some(waiter_id);
            let wait_duration = if is_front {
                match state.mode {
                    QOS_MODE_IDLE => {
                        self.metrics
                            .flush_qos_idle_bypasses
                            .fetch_add(1, Ordering::Relaxed);
                        self.finish_admission(&mut state, waiter_id, bytes, wait_started);
                        return;
                    }
                    QOS_MODE_EMERGENCY => {
                        self.metrics
                            .flush_qos_emergency_bypasses
                            .fetch_add(1, Ordering::Relaxed);
                        self.finish_admission(&mut state, waiter_id, bytes, wait_started);
                        return;
                    }
                    _ if state.tokens >= bytes => {
                        state.tokens -= bytes;
                        self.finish_admission(&mut state, waiter_id, bytes, wait_started);
                        return;
                    }
                    _ => {}
                }

                let deficit = bytes.saturating_sub(state.tokens);
                let rate = state.effective_rate_bps.max(1);
                Duration::from_nanos(
                    ((deficit as u128 * 1_000_000_000u128).div_ceil(rate as u128))
                        .min(MAX_SLEEP.as_nanos()) as u64,
                )
            } else {
                MAX_SLEEP
            };

            if !recorded_wait {
                self.metrics
                    .flush_qos_wait_events
                    .fetch_add(1, Ordering::Relaxed);
                recorded_wait = true;
                wait_started = Some(Instant::now());
            }
            let started = Instant::now();
            self.waiters_changed
                .wait_for(&mut state, wait_duration.max(Duration::from_nanos(1)));
            self.metrics.flush_qos_wait_ns.fetch_add(
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                Ordering::Relaxed,
            );
        }
    }

    fn finish_admission(
        &self,
        state: &mut QosState,
        waiter_id: u64,
        bytes: u64,
        wait_started: Option<Instant>,
    ) {
        debug_assert_eq!(state.waiters.front().copied(), Some(waiter_id));
        let removed = state.waiters.pop_front();
        debug_assert_eq!(removed, Some(waiter_id));
        self.record_waiter_depth(state.waiters.len());
        self.metrics
            .flush_qos_admitted_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        self.record_wait_max(wait_started);
        self.waiters_changed.notify_all();
    }

    fn remove_waiter(&self, state: &mut QosState, waiter_id: u64) {
        if let Some(position) = state.waiters.iter().position(|id| *id == waiter_id) {
            state.waiters.remove(position);
            self.record_waiter_depth(state.waiters.len());
            self.waiters_changed.notify_all();
        }
    }

    fn record_waiter_depth(&self, depth: usize) {
        let depth = depth as u64;
        self.metrics
            .flush_qos_waiters
            .store(depth, Ordering::Relaxed);
        crate::metrics::record_counter_max(&self.metrics.flush_qos_waiters_max, depth);
    }

    fn record_wait_max(&self, started: Option<Instant>) {
        if let Some(started) = started {
            crate::metrics::record_counter_max(
                &self.metrics.flush_qos_wait_max_ns,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
    }

    #[cfg(test)]
    fn waiter_count(&self) -> usize {
        self.state.lock().waiters.len()
    }

    fn observe_foreground(&self, state: &mut QosState, now: Instant) {
        let ops = self.metrics.zone_write_dispatches.load(Ordering::Relaxed);
        // `zone_read_dispatches` covers both the single-LBA path and batched
        // reads. `read_submit_calls` is a timing divisor for the batch path and
        // must not be repurposed as an activity signal.
        let read_dispatches = self.metrics.zone_read_dispatches.load(Ordering::Relaxed);
        let outstanding = self
            .metrics
            .foreground_io_outstanding
            .load(Ordering::Relaxed);
        if ops != state.last_foreground_ops
            || read_dispatches != state.last_foreground_read_dispatches
            || outstanding > 0
        {
            state.last_foreground_ops = ops;
            state.last_foreground_read_dispatches = read_dispatches;
            state.last_foreground_seen = Some(now);
            if state.mode == QOS_MODE_IDLE {
                state.mode = QOS_MODE_PROTECTED;
                state.effective_rate_bps = state.protected_rate_bps;
                self.metrics
                    .flush_qos_mode
                    .store(QOS_MODE_PROTECTED, Ordering::Relaxed);
                self.metrics
                    .flush_qos_rate_bytes_per_sec
                    .store(state.effective_rate_bps, Ordering::Relaxed);
            }
        }
    }

    fn refill(&self, state: &mut QosState, now: Instant) {
        let elapsed_ns = now
            .duration_since(state.last_refill)
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        state.last_refill = now;
        if elapsed_ns == 0 {
            return;
        }
        let added = (state.effective_rate_bps as u128 * elapsed_ns as u128 / 1_000_000_000u128)
            .min(u64::MAX as u128) as u64;
        state.tokens = state
            .tokens
            .saturating_add(added)
            .min(self.config.burst_bytes.max(1));
    }

    fn update_control(&self, state: &mut QosState, now: Instant) {
        let elapsed = now.duration_since(state.last_control);
        let elapsed_ns = elapsed.as_nanos().max(1);
        state.last_control = now;

        let foreground_bytes = self
            .metrics
            .zone_write_lbas
            .load(Ordering::Relaxed)
            .saturating_mul(BLOCK_SIZE as u64);
        let observed_bps = (foreground_bytes.saturating_sub(state.last_foreground_bytes) as u128
            * 1_000_000_000u128
            / elapsed_ns)
            .min(u64::MAX as u128) as u64;
        state.last_foreground_bytes = foreground_bytes;

        let p99 = self
            .metrics
            .buffer_append_wait_durable_foreground_p99_delta(&mut state.previous_durable_histogram);
        let foreground_outstanding = self
            .metrics
            .foreground_io_outstanding
            .load(Ordering::Relaxed)
            > 0;
        let foreground_active = foreground_outstanding
            || state
                .last_foreground_seen
                .is_some_and(|seen| now.duration_since(seen) < FOREGROUND_IDLE_GRACE);
        if foreground_active {
            if let Some((p99_ns, samples)) = p99 {
                self.metrics
                    .flush_qos_durable_p99_ns
                    .store(p99_ns, Ordering::Relaxed);
                if samples >= MIN_FEEDBACK_SAMPLES {
                    let (next, low_streak, direction) = adjust_protected_rate(
                        state.protected_rate_bps,
                        state.low_latency_streak,
                        p99_ns,
                        self.config,
                    );
                    state.protected_rate_bps = next;
                    state.low_latency_streak = low_streak;
                    match direction {
                        RateDirection::Increase => {
                            self.metrics
                                .flush_qos_rate_increases
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        RateDirection::Decrease => {
                            self.metrics
                                .flush_qos_rate_decreases
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        RateDirection::Hold => {}
                    }
                }
            }
        } else {
            state.protected_rate_bps = self.config.active_rate_bps;
            state.low_latency_streak = 0;
            self.metrics
                .flush_qos_durable_p99_ns
                .store(0, Ordering::Relaxed);
        }

        let logical_pct = self.pool.fill_percentage();
        let physical_pct = self.pool.physical_fill_percentage();
        let payload_pct = self.pool.payload_fill_percentage();
        let occupancy_pct = logical_pct.max(physical_pct);
        let (mode, effective_rate) = select_mode_and_rate(
            foreground_active,
            physical_pct,
            occupancy_pct,
            state.protected_rate_bps,
            self.config,
        );
        if mode == QOS_MODE_EMERGENCY && state.mode != QOS_MODE_EMERGENCY {
            self.metrics
                .flush_qos_emergency_transitions
                .fetch_add(1, Ordering::Relaxed);
        }
        state.mode = mode;
        state.effective_rate_bps = effective_rate;

        self.metrics.flush_qos_mode.store(mode, Ordering::Relaxed);
        self.metrics
            .flush_qos_rate_bytes_per_sec
            .store(effective_rate, Ordering::Relaxed);
        self.metrics
            .flush_qos_foreground_bytes_per_sec
            .store(observed_bps, Ordering::Relaxed);
        self.metrics
            .flush_qos_logical_fill_pct
            .store(logical_pct as u64, Ordering::Relaxed);
        self.metrics
            .flush_qos_physical_fill_pct
            .store(physical_pct as u64, Ordering::Relaxed);
        self.metrics
            .flush_qos_payload_fill_pct
            .store(payload_pct as u64, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RateDirection {
    Hold,
    Increase,
    Decrease,
}

fn adjust_protected_rate(
    current: u64,
    low_streak: u8,
    p99_ns: u64,
    config: FlushAdmissionQosConfig,
) -> (u64, u8, RateDirection) {
    if p99_ns > config.target_p99_ns {
        return (
            (current.saturating_mul(4) / 5).max(config.min_rate_bps),
            0,
            RateDirection::Decrease,
        );
    }
    if p99_ns.saturating_mul(4) <= config.target_p99_ns.saturating_mul(3) {
        let next_streak = low_streak.saturating_add(1);
        if next_streak >= LOW_LATENCY_STREAK_TO_RECOVER && current < config.active_rate_bps {
            return (
                current
                    .saturating_mul(105)
                    .div_ceil(100)
                    .min(config.active_rate_bps),
                0,
                RateDirection::Increase,
            );
        }
        return (current, next_streak, RateDirection::Hold);
    }
    (current, 0, RateDirection::Hold)
}

fn select_mode_and_rate(
    foreground_active: bool,
    physical_pct: u8,
    occupancy_pct: u8,
    protected_rate_bps: u64,
    config: FlushAdmissionQosConfig,
) -> (u64, u64) {
    if !foreground_active {
        return (QOS_MODE_IDLE, 0);
    }
    if physical_pct >= config.emergency_pct {
        return (QOS_MODE_EMERGENCY, 0);
    }
    if occupancy_pct <= config.recovery_pct {
        return (QOS_MODE_PROTECTED, protected_rate_bps);
    }
    let width = config.emergency_pct.saturating_sub(config.recovery_pct) as u64;
    let progress = occupancy_pct
        .saturating_sub(config.recovery_pct)
        .min(config.emergency_pct - config.recovery_pct) as u64;
    let recovery_floor = config.active_rate_bps.saturating_add(
        config
            .recovery_rate_bps
            .saturating_sub(config.active_rate_bps)
            .saturating_mul(progress)
            / width.max(1),
    );
    (QOS_MODE_RECOVERY, protected_rate_bps.max(recovery_floor))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::device::RawDevice;
    use tempfile::NamedTempFile;

    fn config() -> FlushAdmissionQosConfig {
        FlushAdmissionQosConfig {
            target_p99_ns: 100_000_000,
            active_rate_bps: 128 * MIB,
            min_rate_bps: 32 * MIB,
            recovery_rate_bps: 384 * MIB,
            burst_bytes: 8 * MIB,
            recovery_pct: 40,
            emergency_pct: 65,
        }
    }

    fn test_pool() -> Arc<WriteBufferPool> {
        let size = 16 * MIB;
        let tmp = NamedTempFile::new().unwrap();
        tmp.as_file().set_len(size).unwrap();
        let device = RawDevice::open_or_create(tmp.path(), size).unwrap();
        Arc::new(
            WriteBufferPool::open_with_group_commit_wait(device, Duration::from_millis(1)).unwrap(),
        )
    }

    fn wait_for_waiter_count(qos: &FlushAdmissionQos, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while qos.waiter_count() != expected {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {expected} QoS waiters; current={}",
                qos.waiter_count()
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn high_latency_backs_off_and_low_latency_recovers_slowly() {
        let cfg = config();
        let (backed_off, streak, direction) =
            adjust_protected_rate(cfg.active_rate_bps, 0, 120_000_000, cfg);
        assert_eq!(backed_off, cfg.active_rate_bps * 4 / 5);
        assert_eq!(streak, 0);
        assert_eq!(direction, RateDirection::Decrease);

        let mut rate = backed_off;
        let mut streak = 0;
        for _ in 0..LOW_LATENCY_STREAK_TO_RECOVER {
            (rate, streak, _) = adjust_protected_rate(rate, streak, 50_000_000, cfg);
        }
        assert!(rate > backed_off);
        assert!(rate <= cfg.active_rate_bps);
    }

    #[test]
    fn occupancy_ramps_rate_and_physical_emergency_bypasses() {
        let cfg = config();
        assert_eq!(
            select_mode_and_rate(true, 0, 20, cfg.active_rate_bps, cfg),
            (QOS_MODE_PROTECTED, cfg.active_rate_bps)
        );
        let (mode, midpoint) = select_mode_and_rate(true, 0, 53, cfg.active_rate_bps, cfg);
        assert_eq!(mode, QOS_MODE_RECOVERY);
        assert!(midpoint > cfg.active_rate_bps);
        assert!(midpoint < cfg.recovery_rate_bps);
        assert_eq!(
            select_mode_and_rate(true, 65, 65, cfg.active_rate_bps, cfg).0,
            QOS_MODE_EMERGENCY
        );
    }

    #[test]
    fn idle_replay_is_unlimited() {
        let cfg = config();
        assert_eq!(
            select_mode_and_rate(false, 0, 90, cfg.min_rate_bps, cfg).0,
            QOS_MODE_IDLE
        );
    }

    #[test]
    fn burst_is_never_smaller_than_one_coalesce_unit() {
        let flush = FlushConfig {
            coalesce_max_raw_bytes: 16 * MIB as usize,
            foreground_flush_burst_mib: 1,
            ..FlushConfig::default()
        };
        let cfg = FlushAdmissionQosConfig::from_flush(&flush);
        assert_eq!(cfg.burst_bytes, 16 * MIB);
    }

    #[test]
    fn zero_target_disables_backend_admission_limit() {
        let flush = FlushConfig {
            foreground_flush_target_p99_ms: 0,
            ..FlushConfig::default()
        };
        let cfg = FlushAdmissionQosConfig::from_flush(&flush);
        assert_eq!(cfg.target_p99_ns, 0);
        assert!(!cfg.enabled());

        let pool = test_pool();
        let metrics = Arc::new(EngineMetrics::default());
        let qos = FlushAdmissionQos::new(cfg, pool, metrics.clone());
        let running = AtomicBool::new(true);
        qos.admit(MIB, &running);
        assert_eq!(metrics.flush_qos_wait_events.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.flush_qos_admitted_bytes.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn concurrent_lanes_share_one_aggregate_rate() {
        let pool = test_pool();
        let metrics = Arc::new(EngineMetrics::default());
        let qos = Arc::new(FlushAdmissionQos::new(
            FlushAdmissionQosConfig {
                active_rate_bps: MIB,
                min_rate_bps: MIB,
                recovery_rate_bps: MIB,
                burst_bytes: 64 * 1024,
                ..config()
            },
            pool,
            metrics.clone(),
        ));
        metrics.zone_write_dispatches.store(1, Ordering::Relaxed);
        let running = Arc::new(AtomicBool::new(true));
        let barrier = Arc::new(std::sync::Barrier::new(5));
        let started = Instant::now();
        let mut workers = Vec::new();
        for _ in 0..4 {
            let qos = qos.clone();
            let running = running.clone();
            let barrier = barrier.clone();
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                qos.admit(64 * 1024, &running);
            }));
        }
        barrier.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        let elapsed = started.elapsed();
        assert!(elapsed >= Duration::from_millis(120), "elapsed={elapsed:?}");
        assert!(elapsed < Duration::from_secs(2), "elapsed={elapsed:?}");
        assert_eq!(
            metrics.flush_qos_admitted_bytes.load(Ordering::Relaxed),
            256 * 1024
        );
    }

    #[test]
    fn fifo_prevents_late_small_request_from_overtaking_large_request() {
        let pool = test_pool();
        let metrics = Arc::new(EngineMetrics::default());
        let qos = Arc::new(FlushAdmissionQos::new(
            FlushAdmissionQosConfig {
                active_rate_bps: 256 * 1024,
                min_rate_bps: 256 * 1024,
                recovery_rate_bps: 256 * 1024,
                burst_bytes: 64 * 1024,
                ..config()
            },
            pool,
            metrics.clone(),
        ));
        metrics.zone_write_dispatches.store(1, Ordering::Relaxed);
        let running = Arc::new(AtomicBool::new(true));
        qos.admit(64 * 1024, &running);

        let (completed_tx, completed_rx) = std::sync::mpsc::channel();
        let qos_large = qos.clone();
        let running_large = running.clone();
        let completed_large = completed_tx.clone();
        let large = std::thread::spawn(move || {
            qos_large.admit(64 * 1024, &running_large);
            completed_large.send("large").unwrap();
        });
        wait_for_waiter_count(&qos, 1);

        let qos_small = qos.clone();
        let running_small = running.clone();
        let small = std::thread::spawn(move || {
            qos_small.admit(1024, &running_small);
            completed_tx.send("small").unwrap();
        });
        wait_for_waiter_count(&qos, 2);

        assert_eq!(
            completed_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            "large"
        );
        assert_eq!(
            completed_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            "small"
        );
        large.join().unwrap();
        small.join().unwrap();
    }

    #[test]
    fn mixed_size_contention_gives_large_request_bounded_progress() {
        let pool = test_pool();
        let metrics = Arc::new(EngineMetrics::default());
        let qos = Arc::new(FlushAdmissionQos::new(
            FlushAdmissionQosConfig {
                active_rate_bps: 512 * 1024,
                min_rate_bps: 512 * 1024,
                recovery_rate_bps: 512 * 1024,
                burst_bytes: 64 * 1024,
                ..config()
            },
            pool,
            metrics.clone(),
        ));
        metrics.zone_write_dispatches.store(1, Ordering::Relaxed);
        let running = Arc::new(AtomicBool::new(true));
        qos.admit(64 * 1024, &running);

        let (large_done_tx, large_done_rx) = std::sync::mpsc::channel();
        let qos_large = qos.clone();
        let running_large = running.clone();
        let large = std::thread::spawn(move || {
            qos_large.admit(64 * 1024, &running_large);
            large_done_tx.send(()).unwrap();
        });
        wait_for_waiter_count(&qos, 1);

        let mut small_workers = Vec::new();
        for _ in 0..4 {
            let qos = qos.clone();
            let running = running.clone();
            small_workers.push(std::thread::spawn(move || {
                while running.load(Ordering::Relaxed) {
                    qos.admit(1024, &running);
                }
            }));
        }

        large_done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("large FIFO request was starved by small requests");
        running.store(false, Ordering::Relaxed);
        large.join().unwrap();
        for worker in small_workers {
            worker.join().unwrap();
        }
    }

    #[test]
    fn shutdown_removes_all_rate_limited_waiters() {
        let pool = test_pool();
        let metrics = Arc::new(EngineMetrics::default());
        let qos = Arc::new(FlushAdmissionQos::new(
            FlushAdmissionQosConfig {
                active_rate_bps: 1,
                min_rate_bps: 1,
                recovery_rate_bps: 1,
                burst_bytes: 1,
                ..config()
            },
            pool,
            metrics.clone(),
        ));
        metrics.zone_write_dispatches.store(1, Ordering::Relaxed);
        let running = Arc::new(AtomicBool::new(true));
        qos.admit(1, &running);

        let mut waiters = Vec::new();
        for _ in 0..8 {
            let qos = qos.clone();
            let running = running.clone();
            waiters.push(std::thread::spawn(move || qos.admit(1, &running)));
        }
        wait_for_waiter_count(&qos, 8);
        running.store(false, Ordering::Relaxed);
        let stopped_at = Instant::now();
        for waiter in waiters {
            waiter.join().unwrap();
        }
        assert!(stopped_at.elapsed() < Duration::from_millis(100));
        assert_eq!(qos.waiter_count(), 0);
        assert_eq!(metrics.flush_qos_waiters.load(Ordering::Relaxed), 0);
        assert!(metrics.flush_qos_waiters_max.load(Ordering::Relaxed) >= 8);
        assert!(metrics.flush_qos_wait_max_ns.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn outstanding_foreground_io_prevents_stalled_workload_from_becoming_idle() {
        let pool = test_pool();
        let metrics = Arc::new(EngineMetrics::default());
        let qos = FlushAdmissionQos::new(config(), pool, Arc::clone(&metrics));
        let lease = metrics.begin_foreground_io();
        {
            let mut state = qos.state.lock();
            let now = Instant::now();
            state.last_foreground_seen = now.checked_sub(FOREGROUND_IDLE_GRACE * 2);
            state.last_control = now - CONTROL_INTERVAL;
        }

        let running = AtomicBool::new(true);
        qos.admit(1, &running);
        assert_eq!(
            metrics.flush_qos_mode.load(Ordering::Relaxed),
            QOS_MODE_PROTECTED
        );
        assert_eq!(metrics.flush_qos_idle_bypasses.load(Ordering::Relaxed), 0);

        drop(lease);
        metrics
            .flush_qos_durable_p99_ns
            .store(123_000_000, Ordering::Relaxed);
        {
            let mut state = qos.state.lock();
            let now = Instant::now();
            state.last_foreground_seen = now.checked_sub(FOREGROUND_IDLE_GRACE * 2);
            state.last_control = now - CONTROL_INTERVAL;
        }
        qos.admit(1, &running);
        assert_eq!(
            metrics.flush_qos_mode.load(Ordering::Relaxed),
            QOS_MODE_IDLE
        );
        assert_eq!(metrics.flush_qos_durable_p99_ns.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn single_read_dispatch_enters_protected_mode_without_timing_divisor_change() {
        let pool = test_pool();
        let metrics = Arc::new(EngineMetrics::default());
        let qos = FlushAdmissionQos::new(config(), pool, Arc::clone(&metrics));
        metrics.zone_read_dispatches.fetch_add(1, Ordering::Relaxed);
        let running = AtomicBool::new(true);

        qos.admit(1, &running);

        assert_eq!(
            metrics.flush_qos_mode.load(Ordering::Relaxed),
            QOS_MODE_PROTECTED
        );
        assert_eq!(metrics.read_submit_calls.load(Ordering::Relaxed), 0);
    }
}
