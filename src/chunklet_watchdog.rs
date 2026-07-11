//! Runtime PD health watchdog (Phase 4d).
//!
//! # Why this lives in onyx, not chunklet
//!
//! chunklet exposes the detection *primitive* (`Pool::probe_pd_liveness`, a
//! lock-free O_DIRECT superblock read per PD) but runs no background probe
//! thread of its own — its recovery model is the external `run_recovery_cycle`
//! re-open loop. onyx already owns the engine lifecycle and the `GcRunner`
//! stop/join/Drop pattern, so the periodic probe loop is a natural fit here,
//! and its auto-failover rebuilds reuse the `chunklet_ops` background-job
//! registry (visible via `onyx chunklet job`).
//!
//! # What it does each sweep
//!
//! For every live PD that is not already Failed, probe it. A PD that answers
//! resets its miss counter; a PD that fails (or has vanished from the live set)
//! increments it. Once a PD crosses `fail_threshold` consecutive misses the
//! watchdog calls `Pool::mark_pd_failed` (idempotent, tolerant of the pulled
//! disk's own superblock write failing) and, if `auto_failover` is on, kicks a
//! background `auto_recover` job to rebuild the affected redundant LDs onto
//! spares.
//!
//! # Lifecycle
//!
//! `GcRunner`-style: an `AtomicBool` stop flag + a `JoinHandle`. `stop()` (and
//! `Drop`) flip the flag and join. The loop sleeps in short ticks so a stop
//! request is honored within one tick regardless of the probe interval.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use onyx_chunklet::pool::PdHealth;
use onyx_chunklet::{PdId, Pool};

/// How often the loop wakes to check the stop flag. The configured probe
/// interval is rounded up to a whole number of these ticks.
const STOP_CHECK_TICK: Duration = Duration::from_millis(500);

#[derive(Clone, Debug)]
pub struct WatchdogConfig {
    pub interval: Duration,
    pub fail_threshold: u32,
    pub auto_failover: bool,
    /// Scan `device_glob` each sweep for a returned disk (Failed tombstone's
    /// superblock present again) and auto-start a `reintegrate_wipe` job.
    pub auto_reintegrate: bool,
    /// After a reintegrate/failover leaves per-PD used-skew above target, kick a
    /// bounded online `rebalance` job (event-driven, thrash-free).
    pub auto_rebalance: bool,
    pub rebalance_target_skew_pct: f64,
    pub rebalance_max_moves: usize,
    /// Content-addressed candidate device glob (e.g. `/dev/nvme*n*`). Required
    /// for `auto_reintegrate` to find a returned disk by pool_id.
    pub device_glob: Option<String>,
}

/// Mutable per-loop state threaded through each `sweep`.
#[derive(Default)]
struct SweepState {
    /// Per-PD consecutive failed-probe counter.
    misses: HashMap<PdId, u32>,
    /// Set true when this loop starts an auto-failover or auto-reintegrate job
    /// (both raise per-PD skew); consumed by the auto-rebalance trigger once the
    /// pool is quiescent again. Event-driven so a stuck pool never re-thrashes.
    pending_rebalance: bool,
}

/// Background PD-health watchdog thread handle.
pub struct ChunkletWatchdog {
    running: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl ChunkletWatchdog {
    /// Start the watchdog against a live pool. Cheap: spawns one thread that
    /// sleeps most of the time.
    pub fn start(pool: Arc<Pool>, cfg: WatchdogConfig) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();
        // Snapshot the log fields before `cfg` moves into the worker closure
        // (WatchdogConfig is Clone, not Copy).
        let (interval_ms, fail_threshold, auto_failover, auto_reintegrate, auto_rebalance) = (
            cfg.interval.as_millis() as u64,
            cfg.fail_threshold,
            cfg.auto_failover,
            cfg.auto_reintegrate,
            cfg.auto_rebalance,
        );
        let handle = thread::Builder::new()
            .name("ck-watchdog".into())
            .spawn(move || run_loop(&pool, cfg, &running_clone))
            .expect("failed to spawn chunklet watchdog thread");
        tracing::info!(
            interval_ms,
            fail_threshold,
            auto_failover,
            auto_reintegrate,
            auto_rebalance,
            "chunklet PD health watchdog started"
        );
        Self {
            running,
            handle: Some(handle),
        }
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for ChunkletWatchdog {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Sleep `interval`, but wake every `STOP_CHECK_TICK` to honor a stop request.
/// Returns `false` if a stop was requested during the wait.
fn sleep_interruptible(interval: Duration, running: &AtomicBool) -> bool {
    let ticks = interval
        .as_millis()
        .div_ceil(STOP_CHECK_TICK.as_millis())
        .max(1);
    for _ in 0..ticks {
        if !running.load(Ordering::Relaxed) {
            return false;
        }
        thread::sleep(STOP_CHECK_TICK);
    }
    running.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sleep_interruptible_returns_immediately_when_stopped() {
        let running = AtomicBool::new(false);
        let start = std::time::Instant::now();
        // Already-stopped: must not sleep a full interval.
        assert!(!sleep_interruptible(Duration::from_secs(60), &running));
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn sleep_interruptible_completes_short_interval() {
        let running = AtomicBool::new(true);
        // One tick's worth of interval completes and reports still-running.
        assert!(sleep_interruptible(Duration::from_millis(1), &running));
    }
}

fn run_loop(pool: &Arc<Pool>, cfg: WatchdogConfig, running: &AtomicBool) {
    let mut state = SweepState::default();
    while sleep_interruptible(cfg.interval, running) {
        sweep(pool, &cfg, &mut state);
    }
}

fn sweep(pool: &Arc<Pool>, cfg: &WatchdogConfig, state: &mut SweepState) {
    detect_failures(pool, cfg, state);
    if cfg.auto_reintegrate {
        auto_reintegrate(pool, cfg, state);
    }
    if cfg.auto_rebalance {
        auto_rebalance(pool, cfg, state);
    }
}

/// Probe every non-Failed PD; mark one Failed after `fail_threshold` consecutive
/// misses and (if `auto_failover`) kick an `auto_recover` rebuild job.
fn detect_failures(pool: &Arc<Pool>, cfg: &WatchdogConfig, state: &mut SweepState) {
    // PDs the pool already considers Failed are skipped (and their counters
    // cleared) — no point probing a disk we already gave up on.
    let already_failed: std::collections::HashSet<PdId> = pool.failed_pds().into_iter().collect();

    for info in pool.list_pds() {
        let pd_id = info.pd_id;
        if already_failed.contains(&pd_id) {
            state.misses.remove(&pd_id);
            continue;
        }
        // Belt-and-suspenders: skip if health already reads Failed even if it
        // isn't in the snapshot above (race with a manual mark).
        if pool.pd_health(pd_id) == Some(PdHealth::Failed) {
            state.misses.remove(&pd_id);
            continue;
        }

        // `None` (no live handle) is treated as a miss, same as a failed read.
        let alive = pool.probe_pd_liveness(pd_id).unwrap_or(false);
        if alive {
            state.misses.remove(&pd_id);
            continue;
        }

        let n = state.misses.entry(pd_id).or_insert(0);
        *n += 1;
        tracing::warn!(
            pd = %pd_id,
            misses = *n,
            threshold = cfg.fail_threshold,
            "chunklet watchdog: PD failed liveness probe"
        );
        if *n < cfg.fail_threshold {
            continue;
        }

        // Threshold crossed → mark Failed. mark_pd_failed tolerates the pulled
        // disk's own superblock write failing (it persists the FAILED flag on
        // the survivors), so this succeeds even when the disk is truly gone.
        match pool.mark_pd_failed(pd_id) {
            Ok(()) => {
                state.misses.remove(&pd_id);
                tracing::error!(pd = %pd_id, "chunklet watchdog: PD marked Failed after repeated probe failures");
                if cfg.auto_failover {
                    let pool_arc = pool.clone();
                    match crate::chunklet_ops::start_auto_failover(&pool_arc, &pd_id.to_string()) {
                        Ok(job) => {
                            // Failover moves the failed member's data onto spares
                            // on surviving PDs → skew rises; queue a rebalance.
                            state.pending_rebalance = true;
                            tracing::warn!(
                                pd = %pd_id,
                                job,
                                "chunklet watchdog: auto-failover rebuild job started"
                            )
                        }
                        Err(e) => tracing::error!(
                            pd = %pd_id,
                            error = %e,
                            "chunklet watchdog: failed to start auto-failover job"
                        ),
                    }
                }
            }
            // Leave the counter at/above threshold so the next sweep retries.
            Err(e) => tracing::error!(
                pd = %pd_id,
                error = %e,
                "chunklet watchdog: mark_pd_failed failed; will retry next sweep"
            ),
        }
    }
}

/// Scan `device_glob` for a returned disk (a Failed tombstone's superblock
/// present again) and auto-start a `reintegrate_wipe` job for each — but never a
/// second job for a device already being reintegrated (idempotent; a successful
/// reintegrate clears the tombstone so it won't re-fire).
fn auto_reintegrate(pool: &Arc<Pool>, cfg: &WatchdogConfig, state: &mut SweepState) {
    let Some(glob) = &cfg.device_glob else {
        return;
    };
    let failed: std::collections::HashSet<PdId> = pool.failed_pds().into_iter().collect();
    if failed.is_empty() {
        return;
    }
    let in_flight: std::collections::HashSet<String> = crate::chunklet_ops::all_jobs()
        .into_iter()
        .filter(|j| j.kind == "reintegrate" && j.state == "running")
        .map(|j| j.ld_id)
        .collect();

    for (path, old_pd) in crate::chunklet_pool::find_returned_pool_disks(glob, pool.id(), &failed) {
        let path_str = path.display().to_string();
        if in_flight.contains(&path_str) {
            continue;
        }
        match crate::chunklet_ops::start_reintegrate(pool, &path_str) {
            Ok(job) => {
                // The reintegrated disk rejoins empty → skew rises; queue a
                // rebalance for once the job (and the pool) is quiescent.
                state.pending_rebalance = true;
                tracing::warn!(pd = %old_pd, device = %path_str, job, "chunklet watchdog: auto-reintegrate started");
            }
            Err(e) => tracing::error!(
                device = %path_str,
                error = %e,
                "chunklet watchdog: failed to start auto-reintegrate"
            ),
        }
    }
}

/// Event-driven auto-rebalance: only after a skew-raising event
/// (`pending_rebalance`) and once no chunklet job is in flight. Fires at most
/// one bounded cycle per event, so a stuck/partially-converged pool never
/// thrashes — the flag is cleared whether we start a cycle or find skew already
/// within target.
fn auto_rebalance(pool: &Arc<Pool>, cfg: &WatchdogConfig, state: &mut SweepState) {
    if !state.pending_rebalance || any_chunklet_job_running() {
        return;
    }
    match pool.metrics() {
        Ok(m) if m.used_skew_pct > cfg.rebalance_target_skew_pct => {
            match crate::chunklet_ops::start_rebalance(
                pool,
                cfg.rebalance_target_skew_pct,
                cfg.rebalance_max_moves,
            ) {
                Ok(job) => {
                    state.pending_rebalance = false;
                    tracing::warn!(
                        job,
                        skew_pct = m.used_skew_pct,
                        "chunklet watchdog: auto-rebalance started"
                    );
                }
                Err(e) => tracing::error!(
                    error = %e,
                    "chunklet watchdog: failed to start auto-rebalance"
                ),
            }
        }
        Ok(_) => {
            // Skew already within target (e.g. failover landed balanced) — done.
            state.pending_rebalance = false;
        }
        Err(e) => tracing::warn!(
            error = %e,
            "chunklet watchdog: metrics unavailable, will retry auto-rebalance next sweep"
        ),
    }
}

fn any_chunklet_job_running() -> bool {
    crate::chunklet_ops::all_jobs()
        .iter()
        .any(|j| j.state == "running")
}
