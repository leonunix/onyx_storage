use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

use onyx_metadb::Db;

use crate::error::{OnyxError, OnyxResult};
use crate::meta::store::DurableCheckpointOutcome;

pub(super) struct AsyncCheckpoint {
    state: Arc<(Mutex<CheckpointState>, Condvar)>,
    thread: Mutex<Option<JoinHandle<()>>>,
    /// Test-only fault injection: when armed, the worker skips the real flush
    /// and reports a fatal `CapacityExhausted`, so tests can exercise the
    /// durability-thread fence path without physically filling a meta device.
    /// Never armed in production (the only setters are `#[cfg(test)]`), and
    /// per-instance so it can't leak into other tests' checkpoints.
    #[cfg_attr(not(test), allow(dead_code))]
    fail_capacity: Arc<AtomicBool>,
}

#[derive(Default)]
struct CheckpointState {
    requested: u64,
    completed: u64,
    checkpointed: u64,
    durable_buffer_seq: u64,
    force_requested: u64,
    failures: Vec<CheckpointFailure>,
    shutdown: bool,
}

struct CheckpointFailure {
    start: u64,
    end: u64,
    message: String,
    /// Fatal (durable-corruption / unrecoverable) failures stay sticky across a
    /// later successful checkpoint; transient ones are pruned on success so the
    /// vec stays bounded and stale errors don't linger.
    fatal: bool,
}

impl CheckpointState {
    /// Drop transient failures on a successful full checkpoint (it supersedes
    /// them), keeping only latched fatal ones.
    fn prune_transient_failures(&mut self) {
        self.failures.retain(|failure| failure.fatal);
    }
}
impl AsyncCheckpoint {
    pub(super) fn start(db: Arc<Db>) -> OnyxResult<Self> {
        let state = Arc::new((Mutex::new(CheckpointState::default()), Condvar::new()));
        let worker_state = state.clone();
        let fail_capacity = Arc::new(AtomicBool::new(false));
        let worker_fail = fail_capacity.clone();
        let thread = std::thread::Builder::new()
            .name("metadb-checkpoint".into())
            .spawn(move || {
                crate::affinity::bind_current(crate::affinity::ThreadRole::MetadbCheckpoint, 0);
                loop {
                    let (start, target, force) = {
                        let (lock, cvar) = &*worker_state;
                        let mut state = lock.lock().unwrap();
                        while state.requested == state.completed && !state.shutdown {
                            state = cvar.wait(state).unwrap();
                        }
                        if state.shutdown && state.requested == state.completed {
                            return;
                        }
                        (
                            state.completed + 1,
                            state.requested,
                            state.force_requested > state.completed,
                        )
                    };

                    let result = if worker_fail.load(Ordering::Relaxed) {
                        // Test-only injected fatal failure (see `fail_capacity`).
                        Err(onyx_metadb::MetaDbError::CapacityExhausted {
                            requested_pages: 1,
                            capacity_pages: 0,
                        })
                    } else if force {
                        db.flush()
                            .map(|_| Some(db.durable_buffer_applied_watermark()))
                    } else {
                        db.try_flush()
                            .map(|flushed| flushed.then(|| db.durable_buffer_applied_watermark()))
                    };
                    let (lock, cvar) = &*worker_state;
                    let mut state = lock.lock().unwrap();
                    match result {
                        Ok(Some(durable_buffer_seq)) => {
                            state.checkpointed = state.checkpointed.max(target);
                            state.durable_buffer_seq =
                                state.durable_buffer_seq.max(durable_buffer_seq);
                            // A full checkpoint folds all dirty state, so it
                            // supersedes every prior transient failure. Keep only
                            // fatal ones latched (they never get superseded — the
                            // next attempt hits the same wall).
                            state.prune_transient_failures();
                        }
                        Ok(None) => {
                            tracing::debug!(
                                start,
                                target,
                                "metadb checkpoint skipped; apply gate busy"
                            );
                        }
                        Err(err) => {
                            let message = err.to_string();
                            let fatal = crate::meta::is_fatal_meta_failure(&message);
                            state.failures.push(CheckpointFailure {
                                start,
                                end: target,
                                message,
                                fatal,
                            });
                        }
                    }
                    state.completed = state.completed.max(target);
                    cvar.notify_all();
                }
            })
            .map_err(OnyxError::Io)?;
        Ok(Self {
            state,
            thread: Mutex::new(Some(thread)),
            fail_capacity,
        })
    }

    /// Test-only: arm the injected fatal-`CapacityExhausted` failpoint on this
    /// checkpoint instance. Per-instance so it never affects another test's Db.
    #[cfg(test)]
    pub(super) fn arm_capacity_fail(&self) {
        self.fail_capacity.store(true, Ordering::Relaxed);
    }

    pub(super) fn request_async(&self) -> OnyxResult<()> {
        self.request().map(|_| ())
    }

    pub(super) fn try_request_async(&self) -> OnyxResult<bool> {
        self.try_request_async_token().map(|token| token.is_some())
    }

    pub(super) fn try_request_async_token(&self) -> OnyxResult<Option<u64>> {
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock().unwrap();
        if state.shutdown {
            return Err(OnyxError::Config(
                "metadb checkpoint worker is shutting down".into(),
            ));
        }
        if state.requested != state.completed {
            return Ok(None);
        }
        state.requested = state
            .requested
            .checked_add(1)
            .ok_or_else(|| OnyxError::Config("metadb checkpoint token overflow".into()))?;
        let token = state.requested;
        cvar.notify_one();
        Ok(Some(token))
    }

    pub(super) fn checkpoint_outcome(
        &self,
        token: u64,
    ) -> OnyxResult<Option<DurableCheckpointOutcome>> {
        let (lock, _) = &*self.state;
        let state = lock.lock().unwrap();
        if let Some(failure) = state
            .failures
            .iter()
            .find(|failure| failure.start <= token && token <= failure.end)
        {
            return Err(OnyxError::Config(format!(
                "metadb checkpoint failed: {}",
                failure.message
            )));
        }
        if state.checkpointed >= token {
            return Ok(Some(DurableCheckpointOutcome::Durable {
                buffer_seq: state.durable_buffer_seq,
            }));
        }
        if state.completed >= token {
            return Ok(Some(DurableCheckpointOutcome::Skipped));
        }
        Ok(None)
    }

    pub(super) fn sync(&self) -> OnyxResult<u64> {
        let token = self.request()?;
        self.wait(token)
    }

    fn request(&self) -> OnyxResult<u64> {
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock().unwrap();
        if state.shutdown {
            return Err(OnyxError::Config(
                "metadb checkpoint worker is shutting down".into(),
            ));
        }
        state.requested = state
            .requested
            .checked_add(1)
            .ok_or_else(|| OnyxError::Config("metadb checkpoint token overflow".into()))?;
        let token = state.requested;
        state.force_requested = state.force_requested.max(token);
        cvar.notify_one();
        Ok(token)
    }

    fn wait(&self, token: u64) -> OnyxResult<u64> {
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock().unwrap();
        while state.completed < token {
            state = cvar.wait(state).unwrap();
        }
        if let Some(failure) = state
            .failures
            .iter()
            .find(|failure| failure.start <= token && token <= failure.end)
        {
            return Err(OnyxError::Config(format!(
                "metadb checkpoint failed: {}",
                failure.message
            )));
        }
        if state.checkpointed >= token {
            Ok(state.durable_buffer_seq)
        } else {
            Err(OnyxError::Config(
                "forced metadb checkpoint completed without a durable frontier".into(),
            ))
        }
    }
}

impl Drop for AsyncCheckpoint {
    fn drop(&mut self) {
        {
            let (lock, cvar) = &*self.state;
            let mut state = lock.lock().unwrap();
            state.shutdown = true;
            cvar.notify_all();
        }
        let handle = self.thread.lock().unwrap().take();
        if let Some(handle) = handle {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CheckpointFailure, CheckpointState};

    fn failure(start: u64, end: u64, fatal: bool) -> CheckpointFailure {
        CheckpointFailure {
            start,
            end,
            message: if fatal {
                "capacity exhausted"
            } else {
                "transient"
            }
            .into(),
            fatal,
        }
    }

    #[test]
    fn prune_keeps_fatal_drops_transient() {
        let mut state = CheckpointState {
            failures: vec![
                failure(1, 2, false),
                failure(3, 4, true),
                failure(5, 6, false),
            ],
            ..Default::default()
        };
        state.prune_transient_failures();
        assert_eq!(
            state.failures.len(),
            1,
            "only the fatal failure should remain"
        );
        assert!(state.failures[0].fatal);
        assert_eq!(state.failures[0].start, 3);

        // Idempotent: a fatal failure survives repeated successful checkpoints.
        state.prune_transient_failures();
        assert_eq!(state.failures.len(), 1);
    }
}
