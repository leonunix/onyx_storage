use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

use onyx_metadb::Db;

use crate::error::{OnyxError, OnyxResult};

pub(super) struct AsyncCheckpoint {
    state: Arc<(Mutex<CheckpointState>, Condvar)>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Default)]
struct CheckpointState {
    requested: u64,
    completed: u64,
    checkpointed: u64,
    force_requested: u64,
    failures: Vec<CheckpointFailure>,
    shutdown: bool,
}

struct CheckpointFailure {
    start: u64,
    end: u64,
    message: String,
}
impl AsyncCheckpoint {
    pub(super) fn start(db: Arc<Db>) -> OnyxResult<Self> {
        let state = Arc::new((Mutex::new(CheckpointState::default()), Condvar::new()));
        let worker_state = state.clone();
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

                    let result = if force {
                        db.flush().map(|_| true)
                    } else {
                        db.try_flush()
                    };
                    let (lock, cvar) = &*worker_state;
                    let mut state = lock.lock().unwrap();
                    match result {
                        Ok(true) => {
                            state.checkpointed = state.checkpointed.max(target);
                        }
                        Ok(false) => {
                            tracing::debug!(
                                start,
                                target,
                                "metadb checkpoint skipped; apply gate busy"
                            );
                        }
                        Err(err) => {
                            state.failures.push(CheckpointFailure {
                                start,
                                end: target,
                                message: err.to_string(),
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
        })
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

    pub(super) fn checkpoint_outcome(&self, token: u64) -> OnyxResult<Option<bool>> {
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
            return Ok(Some(true));
        }
        if state.completed >= token {
            return Ok(Some(false));
        }
        Ok(None)
    }

    pub(super) fn sync(&self) -> OnyxResult<()> {
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

    fn wait(&self, token: u64) -> OnyxResult<()> {
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
        Ok(())
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
