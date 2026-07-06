//! Online chunklet operator jobs (rebuild / scrub) driven over the IPC socket.
//!
//! # Why a background job model
//!
//! `Pool::rebuild_ld` / `Pool::scrub_ld` run for minutes (a full-chunklet
//! backfill / scan), so they MUST NOT execute on the 5 s IPC handler thread.
//! Each is spawned on a named worker thread and tracked in a process-global
//! registry; the operator kicks one off (getting a job id back immediately) and
//! polls completion via `chunklet-job <id>`.
//!
//! `rebuild_ld` is now ONLINE inside chunklet (holds only `io_lock.read()`
//! during the backfill, swapping the descriptor under a brief write lock), so
//! foreground IO keeps flowing throughout — a running rebuild no longer stalls
//! the volume. `scrub_ld` still holds `io_lock.write()` for its scan.
//!
//! The registry is a process singleton (there is exactly one chunklet `Pool`
//! per engine process), so it lives in a `OnceLock` rather than threading a
//! field through every `OnyxEngine` constructor.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use serde::Serialize;

use onyx_chunklet::Pool;

use crate::error::{OnyxError, OnyxResult};

/// A single operator job's state, serialized verbatim to `chunklet-job` clients.
#[derive(Clone, Debug, Serialize)]
pub struct ChunkletJobView {
    pub id: u64,
    /// "rebuild" | "scrub".
    pub kind: String,
    /// LD id string the op targets.
    pub ld_id: String,
    /// "running" | "done" | "error".
    pub state: String,
    /// Result summary when done, error message on failure, empty while running.
    pub detail: String,
}

struct Registry {
    next_id: AtomicU64,
    jobs: Mutex<HashMap<u64, ChunkletJobView>>,
}

static REGISTRY: OnceLock<Registry> = OnceLock::new();

fn registry() -> &'static Registry {
    REGISTRY.get_or_init(|| Registry {
        next_id: AtomicU64::new(1),
        jobs: Mutex::new(HashMap::new()),
    })
}

fn insert_running(kind: &str, ld_id: &str) -> u64 {
    let reg = registry();
    let id = reg.next_id.fetch_add(1, Ordering::Relaxed);
    reg.jobs.lock().unwrap().insert(
        id,
        ChunkletJobView {
            id,
            kind: kind.to_string(),
            ld_id: ld_id.to_string(),
            state: "running".to_string(),
            detail: String::new(),
        },
    );
    id
}

fn finish(id: u64, state: &str, detail: String) {
    if let Some(job) = registry().jobs.lock().unwrap().get_mut(&id) {
        job.state = state.to_string();
        job.detail = detail;
    }
}

/// One job by id, or `None` if unknown.
pub fn job_view(id: u64) -> Option<ChunkletJobView> {
    registry().jobs.lock().unwrap().get(&id).cloned()
}

/// All jobs, id-ascending.
pub fn all_jobs() -> Vec<ChunkletJobView> {
    let mut v: Vec<ChunkletJobView> = registry().jobs.lock().unwrap().values().cloned().collect();
    v.sort_by_key(|j| j.id);
    v
}

/// Spawn a background rebuild of `ld`'s failed members onto spares. Returns the
/// job id to poll; the LD id string is validated before the worker starts so a
/// bad id fails synchronously.
pub fn start_rebuild(pool: &Arc<Pool>, ld: &str) -> OnyxResult<u64> {
    let ld_id = onyx_chunklet::ops::parse_ld_id(ld)?;
    let id = insert_running("rebuild", ld);
    let pool = pool.clone();
    let ld_label = ld.to_string();
    std::thread::Builder::new()
        .name(format!("ck-rebuild-{id}"))
        .spawn(move || {
            match pool.rebuild_ld(ld_id) {
                Ok(r) => finish(
                    id,
                    "done",
                    format!("rebuilt_members={} skipped={}", r.rebuilt_members, r.skipped),
                ),
                Err(e) => finish(id, "error", e.to_string()),
            }
            tracing::info!(job = id, ld = %ld_label, "chunklet rebuild job finished");
        })
        .map_err(OnyxError::Io)?;
    Ok(id)
}

/// Spawn a background auto-failover after a PD was marked Failed: rebuild every
/// affected redundant LD onto spares via `Pool::auto_recover`. Returns the job
/// id to poll. `pd_label` is only for the job's `ld_id` display column (the op
/// spans all affected LDs, not one). Reuses the same registry as manual
/// rebuild/scrub so `chunklet job` surfaces it uniformly.
///
/// Like `rebuild_ld`, `auto_recover` holds each LD's write lock for that LD's
/// rebuild; running it on this dedicated worker keeps the watchdog thread free
/// to keep probing.
pub fn start_auto_failover(pool: &Arc<Pool>, pd_label: &str) -> OnyxResult<u64> {
    let id = insert_running("auto-failover", pd_label);
    let pool = pool.clone();
    let pd_label = pd_label.to_string();
    std::thread::Builder::new()
        .name(format!("ck-failover-{id}"))
        .spawn(move || {
            let report = pool.auto_recover(/* scrub_first */ false);
            finish(
                id,
                if report.failed == 0 { "done" } else { "error" },
                format!(
                    "attempted={} recovered={} failed={}",
                    report.attempted, report.recovered, report.failed
                ),
            );
            tracing::info!(
                job = id,
                pd = %pd_label,
                attempted = report.attempted,
                recovered = report.recovered,
                failed = report.failed,
                "chunklet auto-failover job finished"
            );
        })
        .map_err(OnyxError::Io)?;
    Ok(id)
}

/// Spawn a background scrub (parity verify + quarantine) of `ld`. Returns the
/// job id to poll.
pub fn start_scrub(pool: &Arc<Pool>, ld: &str) -> OnyxResult<u64> {
    let ld_id = onyx_chunklet::ops::parse_ld_id(ld)?;
    let id = insert_running("scrub", ld);
    let pool = pool.clone();
    let ld_label = ld.to_string();
    std::thread::Builder::new()
        .name(format!("ck-scrub-{id}"))
        .spawn(move || {
            match pool.scrub_ld(ld_id) {
                Ok(r) => finish(
                    id,
                    "done",
                    format!(
                        "batches_checked={} mismatches={} marked_bad={} sets_skipped_degraded={}",
                        r.batches_checked,
                        r.mismatches.len(),
                        r.marked_bad,
                        r.sets_skipped_degraded
                    ),
                ),
                Err(e) => finish(id, "error", e.to_string()),
            }
            tracing::info!(job = id, ld = %ld_label, "chunklet scrub job finished");
        })
        .map_err(OnyxError::Io)?;
    Ok(id)
}
