//! Fast PD isolation reactor (inline-degrade companion).
//!
//! # Why this exists
//!
//! chunklet's RAID LDs now ride through a member's runtime write EIO on the
//! surviving redundancy (`ld::degrade::absorb_degraded`) instead of failing the
//! write — this is what stops a single disk fault from fencing metadb before
//! the slow liveness watchdog can react. Riding through leaves a *torn* copy on
//! the failed member, so the member MUST be isolated quickly: `mark_pd_failed`
//! bumps the LD runtime epoch, which makes onyx's `ChunkletBackend` reopen the
//! LD degraded (dead leg skipped) and starts a rebuild-to-spare. This reactor is
//! the consumer that turns each absorbed-write event into that isolation, in
//! ~ms, versus the ~25 s the `ck-watchdog` probe loop takes.
//!
//! The write hot path only does a lock-free `try_send(SuspectMember)` (it holds
//! the LD's `io_lock.read()`, and `mark_pd_failed` needs `io_lock.write()`, so
//! the heavy isolation MUST run off the write path). This reactor is that off-
//! path consumer.
//!
//! # Not gated on the watchdog
//!
//! Inline-degrade correctness *depends* on isolation happening (else a torn leg
//! could be read after the absorb window), so the reactor starts whenever the
//! chunklet backend is active, independent of `[chunklet].watchdog_enabled`. The
//! `mark_pd_failed` isolation is unconditional; the follow-on rebuild-to-spare
//! is gated on `auto_failover` (matching the watchdog), so an operator who
//! disabled auto-failover still gets the safety isolation but keeps manual
//! control of the rebuild.
//!
//! # Lifecycle
//!
//! `ChunkletWatchdog`-style: an `AtomicBool` stop flag + a `JoinHandle`. The
//! loop blocks on the suspect channel with a short timeout so a stop request is
//! honored promptly. `stop()` / `Drop` flip the flag and join.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError};

use onyx_chunklet::pool::PdHealth;
use onyx_chunklet::{Pool, SuspectMember};

/// How long the reactor blocks on the channel before re-checking the stop flag.
const RECV_TICK: Duration = Duration::from_millis(200);

/// Background isolation reactor handle.
pub struct ChunkletIsolationReactor {
    running: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl ChunkletIsolationReactor {
    /// Start the reactor against a live pool. `auto_failover` gates only the
    /// rebuild-to-spare; isolation (`mark_pd_failed`) is always performed.
    pub fn start(pool: Arc<Pool>, auto_failover: bool) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();
        let rx = pool.suspect_events();
        let handle = thread::Builder::new()
            .name("ck-isolate".into())
            .spawn(move || run_loop(&pool, &rx, auto_failover, &running_clone))
            .expect("failed to spawn chunklet isolation reactor thread");
        tracing::info!(
            auto_failover,
            "chunklet inline-degrade isolation reactor started"
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

impl Drop for ChunkletIsolationReactor {
    fn drop(&mut self) {
        self.stop();
    }
}

fn run_loop(
    pool: &Arc<Pool>,
    rx: &Receiver<SuspectMember>,
    auto_failover: bool,
    running: &AtomicBool,
) {
    // PDs we've already isolated this session, so a storm of suspects for the
    // same disk (the serial write path re-emits per stripe) only drives one
    // mark_pd_failed + one failover job. mark_pd_failed is idempotent anyway;
    // this just avoids log spam and redundant job launches.
    let mut isolated: HashSet<onyx_chunklet::PdId> = HashSet::new();

    while running.load(Ordering::Relaxed) {
        match rx.recv_timeout(RECV_TICK) {
            Ok(suspect) => isolate(pool, suspect, auto_failover, &mut isolated),
            Err(RecvTimeoutError::Timeout) => continue,
            // Pool (and its Sender) dropped → nothing left to react to.
            Err(RecvTimeoutError::Disconnected) => return,
        }
    }
}

fn isolate(
    pool: &Arc<Pool>,
    suspect: SuspectMember,
    auto_failover: bool,
    isolated: &mut HashSet<onyx_chunklet::PdId>,
) {
    let pd = suspect.pd_id;
    if !isolated.insert(pd) {
        return; // already handled this PD this session
    }
    if pool.pd_health(pd) == Some(PdHealth::Failed) {
        return; // already Failed (e.g. the slow watchdog beat us to it)
    }

    // mark_pd_failed tolerates the pulled disk's own superblock write failing
    // (it records the FAILED flag on the survivors) and bumps the LD epoch, so
    // onyx's ChunkletBackend reopens degraded on its next IO.
    match pool.mark_pd_failed(pd) {
        Ok(()) => {
            tracing::error!(
                pd = %pd,
                "chunklet isolation: PD marked Failed after an inline-degraded write (fast path)"
            );
            if auto_failover {
                match crate::chunklet_ops::start_auto_failover(pool, &pd.to_string()) {
                    Ok(job) => tracing::warn!(
                        pd = %pd,
                        job,
                        "chunklet isolation: auto-failover rebuild job started"
                    ),
                    Err(e) => tracing::error!(
                        pd = %pd,
                        error = %e,
                        "chunklet isolation: failed to start auto-failover job"
                    ),
                }
            }
        }
        Err(e) => {
            // Un-isolate so a later suspect for this PD retries.
            isolated.remove(&pd);
            tracing::error!(
                pd = %pd,
                error = %e,
                "chunklet isolation: mark_pd_failed failed; will retry on the next suspect"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use onyx_chunklet::io::RawDevice;
    use onyx_chunklet::pool::LdSpec;
    use onyx_chunklet::PoolConfig;

    fn raid6_pool() -> (Arc<Pool>, onyx_chunklet::LdId) {
        let dir = tempfile::tempdir().unwrap();
        // Leak the dir so the sparse files outlive the pool for the test body.
        let dir = Box::leak(Box::new(dir));
        let mut raws = Vec::new();
        for i in 0..5 {
            let p = dir.path().join(format!("pd{i}"));
            raws.push(RawDevice::open_or_create(&p, 4 << 30).unwrap());
        }
        let pool = Pool::create(
            raws,
            PoolConfig {
                spare_pct: 0,
                ..Default::default()
            },
        )
        .unwrap();
        let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
        (pool, id)
    }

    /// A suspect for a live member PD isolates it (`mark_pd_failed`) so onyx's
    /// backend reopens degraded. With `auto_failover=false` no rebuild job runs,
    /// keeping the test fast, and the per-session dedup set makes a repeat suspect
    /// a no-op.
    #[test]
    fn isolate_marks_member_failed_and_dedups() {
        let (pool, id) = raid6_pool();
        let member = pool.find_ld(id).unwrap().members[0].pd;
        assert_eq!(pool.pd_health(member), Some(PdHealth::Healthy));

        let mut isolated = HashSet::new();
        isolate(
            &pool,
            SuspectMember {
                pd_id: member,
                chunklet_index: 0,
            },
            false, // no rebuild → fast unit test
            &mut isolated,
        );
        assert_eq!(
            pool.pd_health(member),
            Some(PdHealth::Failed),
            "reactor must isolate the suspect member"
        );
        assert!(isolated.contains(&member));

        // A second suspect for the same PD is a no-op (already isolated).
        isolate(
            &pool,
            SuspectMember {
                pd_id: member,
                chunklet_index: 0,
            },
            false,
            &mut isolated,
        );
        assert_eq!(pool.pd_health(member), Some(PdHealth::Failed));
    }
}
