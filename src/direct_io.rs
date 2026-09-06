//! Local binary data-plane API for driving an already-running engine.
//!
//! The control socket remains line-oriented.  This module owns a separate
//! `<control socket>.io` Unix stream so benchmark traffic cannot interfere
//! with control-plane commands.

use std::collections::HashSet;
use std::ffi::OsString;
use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use crossbeam_channel::{Receiver, Sender};

use crate::affinity::{self, ThreadRole};
use crate::engine::OnyxEngine;
use crate::error::OnyxError;
use crate::types::BLOCK_SIZE;
use crate::volume::{OnyxVolume, VolumeWriteTicket};
use crate::worker_queue::WorkerQueue;

pub const DIRECT_IO_MAGIC: [u8; 4] = *b"ONIO";
pub const DIRECT_IO_VERSION: u16 = 2;
pub const REQUEST_HEADER_LEN: usize = 48;
pub const RESPONSE_HEADER_LEN: usize = 96;
pub const MAX_DIRECT_IO_BYTES: usize = BLOCK_SIZE as usize;
pub const MAX_DIRECT_IO_OUTSTANDING: usize = 256;
pub const MAX_VOLUME_NAME_BYTES: usize = 255;

pub const OP_HELLO: u16 = 1;
pub const OP_WRITE: u16 = 2;
pub const OP_READ: u16 = 3;
pub const OP_CLOSE: u16 = 4;

const MAX_DIRECT_IO_SESSIONS: usize = 64;
const IO_POLL_TIMEOUT: Duration = Duration::from_millis(100);
const IO_WRITE_TIMEOUT: Duration = Duration::from_secs(1);
const DIRECT_IO_SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

struct ShutdownState {
    requested: AtomicBool,
    deadline: OnceLock<Instant>,
}

impl ShutdownState {
    fn new() -> Self {
        Self {
            requested: AtomicBool::new(false),
            deadline: OnceLock::new(),
        }
    }

    fn request(&self) {
        self.request_with_grace(DIRECT_IO_SHUTDOWN_GRACE);
    }

    fn request_with_grace(&self, grace: Duration) {
        self.deadline.get_or_init(|| Instant::now() + grace);
        self.requested.store(true, Ordering::Release);
    }

    fn is_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }

    fn deadline_reached(&self) -> bool {
        self.deadline
            .get()
            .is_some_and(|deadline| Instant::now() >= *deadline)
    }
}

/// Raw `CLOCK_MONOTONIC` nanoseconds.
///
/// `Instant` cannot cross a process boundary, but `CLOCK_MONOTONIC` is
/// system-wide on Linux, so a stamp taken by the client and one taken by the
/// server are directly comparable as long as both run on this machine.  That
/// is the only way to measure the two windows the protocol's own `Instant`
/// timings structurally cannot see — the transit from the client's `write`
/// into the session reader, and the transit from the writer thread's `write`
/// back into the client's reap loop.  Everything else was already covered,
/// which is why the previous round of work could only eliminate hypotheses
/// (see memory `direct_io_qd1024_gap_elimination_chain`) instead of naming
/// where the missing ~10.6 ms at QD1024 actually goes.
pub fn monotonic_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a live, exclusively borrowed `timespec`.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) } != 0 {
        return 0;
    }
    (ts.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.tv_nsec as u64)
}

/// Nanoseconds a request spent between leaving the client and being fully
/// read by its session thread.
///
/// A `client_submit_ns` of 0 means the client did not stamp, and a stamp from
/// the future means the two readings raced a clock adjustment (or the client
/// is not on this machine).  Both report 0 rather than a fabricated number,
/// so a zero here always reads as "unmeasured", never as "instant".
fn measure_intake_ns(client_submit_ns: u64, received_ns: u64) -> u64 {
    if client_submit_ns == 0 {
        return 0;
    }
    received_ns.saturating_sub(client_submit_ns)
}

#[inline(always)]
fn stage_monotonic_ns() -> u64 {
    #[cfg(any(test, feature = "diagnostic-metrics"))]
    {
        return monotonic_ns();
    }
    #[cfg(not(any(test, feature = "diagnostic-metrics")))]
    {
        0
    }
}

/// Fixed-size little-endian request header.
///
/// `payload_len` is the number of bytes following the header.  `io_len` is
/// the requested IO size, which differs from `payload_len` for reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestHeader {
    pub opcode: u16,
    pub flags: u32,
    pub payload_len: u32,
    pub request_id: u64,
    pub offset: u64,
    pub io_len: u32,
    /// `monotonic_ns` at the moment the client handed these bytes to `write`,
    /// or 0 when the client does not stamp.  Clients must stamp as late as
    /// possible — a stamp taken when the request was *staged* would fold the
    /// client's own staging delay into the measured transit.
    pub client_submit_ns: u64,
}

impl RequestHeader {
    pub fn encode(self) -> [u8; REQUEST_HEADER_LEN] {
        let mut out = [0u8; REQUEST_HEADER_LEN];
        out[0..4].copy_from_slice(&DIRECT_IO_MAGIC);
        out[4..6].copy_from_slice(&DIRECT_IO_VERSION.to_le_bytes());
        out[6..8].copy_from_slice(&self.opcode.to_le_bytes());
        out[8..12].copy_from_slice(&self.flags.to_le_bytes());
        out[12..16].copy_from_slice(&self.payload_len.to_le_bytes());
        out[16..24].copy_from_slice(&self.request_id.to_le_bytes());
        out[24..32].copy_from_slice(&self.offset.to_le_bytes());
        out[32..36].copy_from_slice(&self.io_len.to_le_bytes());
        out[40..48].copy_from_slice(&self.client_submit_ns.to_le_bytes());
        out
    }

    pub fn decode(buf: &[u8]) -> io::Result<Self> {
        validate_header_prefix(buf, REQUEST_HEADER_LEN)?;
        if buf[36..40] != [0; 4] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "direct IO request reserved field is nonzero",
            ));
        }
        Ok(Self {
            opcode: u16::from_le_bytes(buf[6..8].try_into().unwrap()),
            flags: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            payload_len: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
            request_id: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            offset: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            io_len: u32::from_le_bytes(buf[32..36].try_into().unwrap()),
            client_submit_ns: u64::from_le_bytes(buf[40..48].try_into().unwrap()),
        })
    }
}

/// Fixed-size little-endian response header.  A successful read is followed
/// by `payload_len` bytes; all other responses currently have no payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseHeader {
    pub opcode: u16,
    pub status: i32,
    pub request_id: u64,
    pub bytes: u32,
    pub payload_len: u32,
    pub server_total_ns: u64,
    pub submit_queue_ns: u64,
    pub engine_submit_ns: u64,
    pub durable_wait_ns: u64,
    pub completion_dispatch_ns: u64,
    /// Client `write` -> session thread finished reading the request.  See
    /// `measure_intake_ns`; 0 means unmeasured.
    ///
    /// The boundary is "the request is fully read", so a write's intake
    /// covers header AND payload while a read's covers only the header.  A
    /// write reading higher than a read is therefore expected, and comparing
    /// the two directly measures nothing.
    pub intake_ns: u64,
    /// Response built -> `write_response` about to hit the socket.  This is
    /// the third window nothing measured: a completion is handed to the
    /// per-session writer thread through a channel, and `server_total_ns`
    /// stops when the response is *constructed*, not when it is sent.  With
    /// ~550 threads on ~44 pinned cores (memory
    /// `cpu_oversubscription_is_the_real_wake_floor`) a descheduled writer
    /// thread is exactly the kind of delay that would be invisible today.
    pub response_queue_ns: u64,
    /// `monotonic_ns` immediately before the response bytes hit the socket,
    /// so the client can measure the return transit the same way the server
    /// measures `intake_ns`.
    pub server_send_ns: u64,
}

impl ResponseHeader {
    pub fn encode(self) -> [u8; RESPONSE_HEADER_LEN] {
        let mut out = [0u8; RESPONSE_HEADER_LEN];
        out[0..4].copy_from_slice(&DIRECT_IO_MAGIC);
        out[4..6].copy_from_slice(&DIRECT_IO_VERSION.to_le_bytes());
        out[6..8].copy_from_slice(&self.opcode.to_le_bytes());
        out[8..12].copy_from_slice(&self.status.to_le_bytes());
        out[16..24].copy_from_slice(&self.request_id.to_le_bytes());
        out[24..28].copy_from_slice(&self.bytes.to_le_bytes());
        out[28..32].copy_from_slice(&self.payload_len.to_le_bytes());
        out[32..40].copy_from_slice(&self.server_total_ns.to_le_bytes());
        out[40..48].copy_from_slice(&self.submit_queue_ns.to_le_bytes());
        out[48..56].copy_from_slice(&self.engine_submit_ns.to_le_bytes());
        out[56..64].copy_from_slice(&self.durable_wait_ns.to_le_bytes());
        out[64..72].copy_from_slice(&self.completion_dispatch_ns.to_le_bytes());
        out[72..80].copy_from_slice(&self.intake_ns.to_le_bytes());
        out[80..88].copy_from_slice(&self.response_queue_ns.to_le_bytes());
        out[88..96].copy_from_slice(&self.server_send_ns.to_le_bytes());
        out
    }

    pub fn decode(buf: &[u8]) -> io::Result<Self> {
        validate_header_prefix(buf, RESPONSE_HEADER_LEN)?;
        if buf[12..16] != [0; 4] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "direct IO response reserved field is nonzero",
            ));
        }
        Ok(Self {
            opcode: u16::from_le_bytes(buf[6..8].try_into().unwrap()),
            status: i32::from_le_bytes(buf[8..12].try_into().unwrap()),
            request_id: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            bytes: u32::from_le_bytes(buf[24..28].try_into().unwrap()),
            payload_len: u32::from_le_bytes(buf[28..32].try_into().unwrap()),
            server_total_ns: u64::from_le_bytes(buf[32..40].try_into().unwrap()),
            submit_queue_ns: u64::from_le_bytes(buf[40..48].try_into().unwrap()),
            engine_submit_ns: u64::from_le_bytes(buf[48..56].try_into().unwrap()),
            durable_wait_ns: u64::from_le_bytes(buf[56..64].try_into().unwrap()),
            completion_dispatch_ns: u64::from_le_bytes(buf[64..72].try_into().unwrap()),
            intake_ns: u64::from_le_bytes(buf[72..80].try_into().unwrap()),
            response_queue_ns: u64::from_le_bytes(buf[80..88].try_into().unwrap()),
            server_send_ns: u64::from_le_bytes(buf[88..96].try_into().unwrap()),
        })
    }
}

/// Every server-side stage timing a response carries, so the helpers that
/// build responses take one value instead of a growing positional tail.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct StageTimings {
    server_total_ns: u64,
    submit_queue_ns: u64,
    engine_submit_ns: u64,
    durable_wait_ns: u64,
    completion_dispatch_ns: u64,
    intake_ns: u64,
}

/// A zero-sized clock in normal builds. This preserves the Direct IO wire
/// layout while compiling all server-side stage clock reads out of production.
#[derive(Debug, Clone, Copy)]
struct StageInstant {
    #[cfg(any(test, feature = "diagnostic-metrics"))]
    inner: Instant,
}

impl StageInstant {
    #[inline(always)]
    fn now() -> Self {
        #[cfg(any(test, feature = "diagnostic-metrics"))]
        {
            return Self {
                inner: Instant::now(),
            };
        }
        #[cfg(not(any(test, feature = "diagnostic-metrics")))]
        {
            Self {}
        }
    }

    #[inline(always)]
    fn elapsed(self) -> Duration {
        #[cfg(any(test, feature = "diagnostic-metrics"))]
        {
            return self.inner.elapsed();
        }
        #[cfg(not(any(test, feature = "diagnostic-metrics")))]
        {
            Duration::ZERO
        }
    }

    #[inline(always)]
    fn saturating_duration_since(self, earlier: Self) -> Duration {
        #[cfg(any(test, feature = "diagnostic-metrics"))]
        {
            return self.inner.saturating_duration_since(earlier.inner);
        }
        #[cfg(not(any(test, feature = "diagnostic-metrics")))]
        {
            let _ = earlier;
            Duration::ZERO
        }
    }
}

#[inline(always)]
fn completion_dispatch_delay_ns(ticket: &VolumeWriteTicket, completed_at: StageInstant) -> u64 {
    #[cfg(any(test, feature = "diagnostic-metrics"))]
    {
        return ticket
            .completion_dispatch_delay_ns(completed_at.inner)
            .unwrap_or(0);
    }
    #[cfg(not(any(test, feature = "diagnostic-metrics")))]
    {
        let _ = (ticket, completed_at);
        0
    }
}

fn validate_header_prefix(buf: &[u8], expected_len: usize) -> io::Result<()> {
    if buf.len() != expected_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "invalid direct IO header length {}, expected {expected_len}",
                buf.len()
            ),
        ));
    }
    if buf[0..4] != DIRECT_IO_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid direct IO protocol magic",
        ));
    }
    let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
    if version != DIRECT_IO_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported direct IO protocol version {version}"),
        ));
    }
    Ok(())
}

pub fn direct_io_socket_path(control_socket_path: &Path) -> PathBuf {
    let mut path = OsString::from(control_socket_path.as_os_str());
    path.push(".io");
    PathBuf::from(path)
}

fn bind_direct_io_thread(cpus: &[usize], ordinal: usize) {
    if cpus.is_empty() {
        affinity::bind_current(ThreadRole::Ublk, ordinal);
        return;
    }
    if let Err(error) = affinity::bind_current_thread_to(cpus) {
        tracing::warn!(?cpus, ordinal, %error, "failed to bind direct IO thread");
    }
}

struct SubmitLanes {
    lanes: Arc<Vec<WorkerQueue<SubmitTask>>>,
    worker_handles: Vec<JoinHandle<()>>,
    workers_per_lane: usize,
    groups_per_lane: usize,
}

impl SubmitLanes {
    /// `shared` collapses the per-lane partition into ONE queue served by all
    /// `nr_queues * queue_workers` threads. A session is otherwise pinned to
    /// `session_id % nr_queues` for life, so one fio job could never use more
    /// than `queue_workers` (4) threads no matter its iodepth — the same static
    /// partition that bounded the ublk frontend at 3-4 queues' worth of workers
    /// (see `UblkConfig::shared_io_workers`). Keeping the harness's own
    /// concurrency unbounded is what makes it usable as an instrument.
    ///
    /// "ONE queue" is now one queue *logically*: see
    /// `worker_queue::MAX_RECEIVERS_PER_CHANNEL` for why the lane is physically
    /// several channels with a per-request rotor over them.
    fn start(
        nr_queues: usize,
        queue_workers: usize,
        shared: bool,
        submit_workers_override: Option<usize>,
        direct_io_cpus: Arc<Vec<usize>>,
    ) -> io::Result<Self> {
        let lanes = if shared { 1 } else { nr_queues };
        let sessions_per_lane = MAX_DIRECT_IO_SESSIONS.div_ceil(lanes);
        let lane_capacity = MAX_DIRECT_IO_OUTSTANDING.saturating_mul(sessions_per_lane.max(1));
        // `submit_workers_override` only applies in shared mode, where the
        // whole pool is one number by construction (`lanes == 1`) — see
        // `ServiceConfig::direct_io_workers`'s doc comment for why this
        // pool's size is being decoupled from `nr_queues * queue_workers`.
        let workers_per_lane = match (shared, submit_workers_override) {
            (true, Some(override_count)) => override_count.max(1),
            (true, None) => nr_queues.saturating_mul(queue_workers),
            (false, _) => queue_workers,
        };
        let mut built_lanes = Vec::with_capacity(lanes);
        let mut worker_handles = Vec::with_capacity(lanes.saturating_mul(workers_per_lane));
        let mut groups_per_lane = 0;

        for lane_id in 0..lanes {
            // `lane_capacity` is the TOTAL across the lane's groups, so this
            // keeps exactly the admission capacity one shared channel had.
            let (queue, receivers) =
                WorkerQueue::<SubmitTask>::build(workers_per_lane, Some(lane_capacity));
            groups_per_lane = queue.groups();
            for (worker_id, worker_rx) in receivers.into_iter().enumerate() {
                let worker_cpus = direct_io_cpus.clone();
                let ordinal = lane_id
                    .saturating_mul(workers_per_lane)
                    .saturating_add(worker_id);
                let handle = thread::Builder::new()
                    .name(format!("direct-io-submit-q{lane_id}-w{worker_id}"))
                    .spawn(move || {
                        bind_direct_io_thread(&worker_cpus, ordinal);
                        submit_worker_loop(worker_rx);
                    });
                match handle {
                    Ok(handle) => worker_handles.push(handle),
                    Err(error) => {
                        drop(queue);
                        drop(built_lanes);
                        for handle in worker_handles {
                            let _ = handle.join();
                        }
                        return Err(error);
                    }
                }
            }
            built_lanes.push(queue);
        }

        Ok(Self {
            lanes: Arc::new(built_lanes),
            worker_handles,
            workers_per_lane,
            groups_per_lane,
        })
    }

    fn shutdown_and_join(self) {
        drop(self.lanes);
        for handle in self.worker_handles {
            if let Err(error) = handle.join() {
                tracing::error!(?error, "direct IO submit worker panicked");
            }
        }
    }
}

/// Lifetime handle for the direct-IO listener and all accepted sessions.
/// `shutdown_and_join` must run before `OnyxEngine::shutdown`.
pub struct DirectIoServer {
    socket_path: PathBuf,
    shutdown: Arc<ShutdownState>,
    listener_handle: Option<JoinHandle<()>>,
    submit_lanes: Option<SubmitLanes>,
}

impl DirectIoServer {
    pub fn start(
        control_socket_path: &Path,
        engine: Arc<ArcSwap<Option<OnyxEngine>>>,
        nr_queues: usize,
        queue_workers: usize,
        shared_submit_pool: bool,
        submit_workers_override: Option<usize>,
        direct_io_cpus: Vec<usize>,
    ) -> io::Result<Self> {
        let nr_queues = nr_queues.max(1);
        let queue_workers = queue_workers.max(1);
        let socket_path = direct_io_socket_path(control_socket_path);
        if let Some(parent) = socket_path.parent() {
            fs::create_dir_all(parent)?;
        }
        if socket_path.exists() {
            fs::remove_file(&socket_path)?;
        }
        let listener = UnixListener::bind(&socket_path)?;
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))?;
        listener.set_nonblocking(true)?;

        let direct_io_cpus = Arc::new(direct_io_cpus);
        let logged_direct_io_cpus = direct_io_cpus.clone();
        let submit_lanes = SubmitLanes::start(
            nr_queues,
            queue_workers,
            shared_submit_pool,
            submit_workers_override,
            direct_io_cpus.clone(),
        )?;
        let lane_handles = submit_lanes.lanes.clone();
        let shutdown = Arc::new(ShutdownState::new());
        let thread_shutdown = shutdown.clone();
        let listener_handle = thread::Builder::new()
            .name("direct-io-listener".into())
            .spawn(move || {
                bind_direct_io_thread(&direct_io_cpus, nr_queues.saturating_mul(queue_workers));
                listener_loop(
                    listener,
                    engine,
                    lane_handles,
                    queue_workers,
                    direct_io_cpus,
                    thread_shutdown,
                )
            });
        let listener_handle = match listener_handle {
            Ok(handle) => handle,
            Err(error) => {
                submit_lanes.shutdown_and_join();
                let _ = fs::remove_file(&socket_path);
                return Err(error);
            }
        };

        tracing::info!(
            path = %socket_path.display(),
            submit_lanes = submit_lanes.lanes.len(),
            groups_per_lane = submit_lanes.groups_per_lane,
            receivers_per_group = crate::worker_queue::MAX_RECEIVERS_PER_CHANNEL,
            workers_per_lane = submit_lanes.workers_per_lane,
            direct_io_cpus = ?logged_direct_io_cpus,
            "direct IO socket listening"
        );
        Ok(Self {
            socket_path,
            shutdown,
            listener_handle: Some(listener_handle),
            submit_lanes: Some(submit_lanes),
        })
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub fn shutdown_and_join(&mut self) {
        self.shutdown.request();
        let _ = UnixStream::connect(&self.socket_path);
        if let Some(handle) = self.listener_handle.take() {
            if let Err(error) = handle.join() {
                tracing::error!(?error, "direct IO listener panicked");
            }
        }
        if let Some(submit_lanes) = self.submit_lanes.take() {
            submit_lanes.shutdown_and_join();
        }
    }
}

impl Drop for DirectIoServer {
    fn drop(&mut self) {
        self.shutdown_and_join();
    }
}

fn listener_loop(
    listener: UnixListener,
    engine: Arc<ArcSwap<Option<OnyxEngine>>>,
    submit_lanes: Arc<Vec<WorkerQueue<SubmitTask>>>,
    queue_workers: usize,
    direct_io_cpus: Arc<Vec<usize>>,
    shutdown: Arc<ShutdownState>,
) {
    let mut sessions: Vec<JoinHandle<()>> = Vec::new();
    let mut next_session_id = 0usize;
    while !shutdown.is_requested() {
        match listener.accept() {
            Ok((stream, _)) => {
                reap_finished_sessions(&mut sessions);
                if sessions.len() >= MAX_DIRECT_IO_SESSIONS {
                    tracing::warn!(
                        limit = MAX_DIRECT_IO_SESSIONS,
                        "direct IO session limit reached"
                    );
                    let _ = stream.shutdown(std::net::Shutdown::Both);
                    continue;
                }
                let session_id = next_session_id;
                next_session_id = next_session_id.wrapping_add(1);
                let session_engine = engine.clone();
                let session_shutdown = shutdown.clone();
                let lane_id = session_id % submit_lanes.len();
                let lane = submit_lanes.clone();
                let session_cpus = direct_io_cpus.clone();
                match thread::Builder::new()
                    .name(format!("direct-io-session-{session_id}"))
                    .spawn(move || {
                        handle_session(
                            stream,
                            session_engine,
                            session_shutdown,
                            session_id,
                            lane,
                            lane_id,
                            queue_workers,
                            session_cpus,
                        )
                    }) {
                    Ok(handle) => sessions.push(handle),
                    Err(error) => tracing::warn!(%error, "failed to spawn direct IO session"),
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                reap_finished_sessions(&mut sessions);
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => {
                tracing::warn!(%error, "direct IO accept failed");
                thread::sleep(Duration::from_millis(10));
            }
        }
    }

    for handle in sessions {
        if let Err(error) = handle.join() {
            tracing::error!(?error, "direct IO session panicked");
        }
    }
}

fn reap_finished_sessions(sessions: &mut Vec<JoinHandle<()>>) {
    let mut idx = 0;
    while idx < sessions.len() {
        if sessions[idx].is_finished() {
            let handle = sessions.swap_remove(idx);
            if let Err(error) = handle.join() {
                tracing::error!(?error, "direct IO session panicked");
            }
        } else {
            idx += 1;
        }
    }
}

struct PendingWrite {
    request_id: u64,
    ticket: VolumeWriteTicket,
    server_started: StageInstant,
    submitted_at: StageInstant,
    submit_queue_ns: u64,
    engine_submit_ns: u64,
    intake_ns: u64,
    bytes: u32,
}

/// Per-session request IDs that have not completed their response write.
///
/// A legal depth-256 client can receive one completion and immediately submit
/// its replacement before the writer thread removes the completed ID. Waiting
/// for that transient slot closes the race without holding this mutex across a
/// socket write or weakening duplicate-ID detection.
struct ActiveIds {
    ids: Mutex<HashSet<u64>>,
    slot_available: Condvar,
}

impl ActiveIds {
    fn new() -> Self {
        Self {
            ids: Mutex::new(HashSet::new()),
            slot_available: Condvar::new(),
        }
    }

    fn reserve(
        &self,
        request_id: u64,
        alive: &AtomicBool,
        shutdown: &ShutdownState,
    ) -> Result<(), i32> {
        let mut ids = self.ids.lock().unwrap();
        loop {
            if ids.contains(&request_id) {
                return Err(-libc::EALREADY);
            }
            if ids.len() < MAX_DIRECT_IO_OUTSTANDING {
                ids.insert(request_id);
                return Ok(());
            }
            if !alive.load(Ordering::Acquire) || shutdown.is_requested() {
                return Err(-libc::ESHUTDOWN);
            }
            let (next, _) = self
                .slot_available
                .wait_timeout(ids, IO_POLL_TIMEOUT)
                .unwrap();
            ids = next;
        }
    }

    fn contains(&self, request_id: u64) -> bool {
        self.ids.lock().unwrap().contains(&request_id)
    }

    fn release(&self, request_id: u64) {
        if self.ids.lock().unwrap().remove(&request_id) {
            self.slot_available.notify_one();
        }
    }

    fn wake_all(&self) {
        self.slot_available.notify_all();
    }
}

struct Outbound {
    header: ResponseHeader,
    payload: Vec<u8>,
    clear_active_id: bool,
    /// When this response was handed to the writer thread — turned into
    /// `ResponseHeader::response_queue_ns` by `write_response`.
    queued_at: StageInstant,
}

impl Outbound {
    fn new(header: ResponseHeader, payload: Vec<u8>, clear_active_id: bool) -> Self {
        Self {
            header,
            payload,
            clear_active_id,
            queued_at: StageInstant::now(),
        }
    }

    fn header_only(header: ResponseHeader, clear_active_id: bool) -> Self {
        Self::new(header, Vec::new(), clear_active_id)
    }
}

struct SubmitTask {
    volume: Arc<OnyxVolume>,
    header: RequestHeader,
    payload: Vec<u8>,
    server_started: StageInstant,
    queued_at: StageInstant,
    intake_ns: u64,
    pending_tx: Sender<PendingWrite>,
    /// Header-only responses (write acks, errors, close) — the fast lane.
    ack_tx: Sender<Outbound>,
    /// Read completions, which carry a 4 KiB payload — see `writer_loop`'s
    /// doc comment for why these are kept off the ack lane.
    payload_tx: Sender<Outbound>,
    recycle_tx: Sender<Vec<u8>>,
}

fn handle_session(
    mut stream: UnixStream,
    engine: Arc<ArcSwap<Option<OnyxEngine>>>,
    shutdown: Arc<ShutdownState>,
    session_id: usize,
    submit_lane: Arc<Vec<WorkerQueue<SubmitTask>>>,
    lane_id: usize,
    lane_worker_count: usize,
    direct_io_cpus: Arc<Vec<usize>>,
) {
    bind_direct_io_thread(&direct_io_cpus, session_id.saturating_mul(3));
    let _ = stream.set_read_timeout(Some(IO_POLL_TIMEOUT));
    let _ = stream.set_write_timeout(Some(IO_WRITE_TIMEOUT));

    let (hello, volume_name) = match read_hello(&mut stream, &shutdown) {
        Ok(Some(value)) => value,
        Ok(None) => return,
        Err((header, status)) => {
            let _ = write_response(
                &mut stream,
                &mut Outbound::header_only(
                    response(
                        header.opcode,
                        header.request_id,
                        status,
                        0,
                        StageTimings::default(),
                    ),
                    false,
                ),
            );
            return;
        }
    };

    let engine_guard = engine.load_full();
    let volume = match engine_guard.as_ref() {
        Some(engine) if engine.is_full_mode() => match engine.open_volume(&volume_name) {
            Ok(volume) => volume,
            Err(error) => {
                let _ = write_response(
                    &mut stream,
                    &mut Outbound::header_only(
                        response(
                            OP_HELLO,
                            hello.request_id,
                            status_from_error(&error),
                            0,
                            StageTimings::default(),
                        ),
                        false,
                    ),
                );
                return;
            }
        },
        Some(_) | None => {
            let _ = write_response(
                &mut stream,
                &mut Outbound::header_only(
                    response(
                        OP_HELLO,
                        hello.request_id,
                        -libc::ENODEV,
                        0,
                        StageTimings::default(),
                    ),
                    false,
                ),
            );
            return;
        }
    };
    let volume = Arc::new(volume);

    if write_response(
        &mut stream,
        &mut Outbound::header_only(
            response(
                OP_HELLO,
                hello.request_id,
                0,
                lane_worker_count as u32,
                StageTimings::default(),
            ),
            false,
        ),
    )
    .is_err()
    {
        return;
    }

    let alive = Arc::new(AtomicBool::new(true));
    let active_ids = Arc::new(ActiveIds::new());
    // Two lanes so a large read payload in flight can't strand a pending
    // write ack behind it — see `writer_loop`'s doc comment.
    let (ack_tx, ack_rx) = crossbeam_channel::bounded(MAX_DIRECT_IO_OUTSTANDING);
    let (payload_tx, payload_rx) = crossbeam_channel::bounded(MAX_DIRECT_IO_OUTSTANDING);
    let writer_stream = match stream.try_clone() {
        Ok(stream) => stream,
        Err(_) => return,
    };
    let writer_alive = alive.clone();
    let writer_active_ids = active_ids.clone();
    let writer_cpus = direct_io_cpus.clone();
    let writer_handle = thread::Builder::new()
        .name(format!("direct-io-writer-{session_id}"))
        .spawn(move || {
            bind_direct_io_thread(&writer_cpus, session_id.saturating_mul(3).saturating_add(1));
            writer_loop(
                writer_stream,
                ack_rx,
                payload_rx,
                writer_alive,
                writer_active_ids,
            )
        });
    let writer_handle = match writer_handle {
        Ok(handle) => handle,
        Err(_) => return,
    };

    let (pending_tx, pending_rx) = crossbeam_channel::bounded(MAX_DIRECT_IO_OUTSTANDING);
    let (recycle_tx, recycle_rx) = crossbeam_channel::bounded(MAX_DIRECT_IO_OUTSTANDING);
    let dispatcher_tx = ack_tx.clone();
    let dispatcher_alive = alive.clone();
    let dispatcher_shutdown = shutdown.clone();
    let dispatcher_cpus = direct_io_cpus.clone();
    let dispatcher_handle = thread::Builder::new()
        .name(format!("direct-io-durable-{session_id}"))
        .spawn(move || {
            bind_direct_io_thread(
                &dispatcher_cpus,
                session_id.saturating_mul(3).saturating_add(2),
            );
            durability_loop(
                pending_rx,
                dispatcher_tx,
                dispatcher_alive,
                dispatcher_shutdown,
            )
        });
    let dispatcher_handle = match dispatcher_handle {
        Ok(handle) => handle,
        Err(_) => {
            alive.store(false, Ordering::Release);
            drop(ack_tx);
            drop(payload_tx);
            let _ = writer_handle.join();
            return;
        }
    };

    let mut close_request = None;
    let mut payload = Vec::new();
    while alive.load(Ordering::Acquire) && !shutdown.is_requested() {
        let header = match read_request_header(&mut stream, &shutdown, &alive) {
            Ok(Some(header)) => header,
            Ok(None) => break,
            Err(error) => {
                tracing::debug!(%error, "direct IO session request header failed");
                break;
            }
        };
        if header.payload_len as usize > MAX_DIRECT_IO_BYTES {
            break;
        }
        if let Ok(recycled) = recycle_rx.try_recv() {
            payload = recycled;
        }
        payload.resize(header.payload_len as usize, 0);
        match read_exact_interruptible(&mut stream, &mut payload, &shutdown, &alive) {
            Ok(true) => {}
            Ok(false) | Err(_) => break,
        }
        // Both stamps mark the same instant — the request is fully read.  The
        // `Instant` drives every relative stage timing; the raw monotonic
        // reading is what can be differenced against the client's stamp.
        let received_ns = stage_monotonic_ns();
        let server_started = StageInstant::now();
        let stages = StageTimings {
            intake_ns: measure_intake_ns(header.client_submit_ns, received_ns),
            ..StageTimings::default()
        };

        if header.flags != 0 {
            send_error(
                &ack_tx,
                &header,
                -libc::EINVAL,
                server_started,
                stages,
                false,
            );
            continue;
        }
        match header.opcode {
            OP_WRITE | OP_READ => {
                if header.io_len as usize > MAX_DIRECT_IO_BYTES
                    || header.offset.checked_add(header.io_len as u64).is_none()
                {
                    send_error(
                        &ack_tx,
                        &header,
                        -libc::EINVAL,
                        server_started,
                        stages,
                        false,
                    );
                    continue;
                }

                if let Err(status) = active_ids.reserve(header.request_id, &alive, &shutdown) {
                    send_error(
                        &ack_tx,
                        &header,
                        status,
                        server_started,
                        stages,
                        false,
                    );
                    continue;
                }
            }
            OP_CLOSE => {
                if header.payload_len != 0 || header.io_len != 0 || header.offset != 0 {
                    send_error(
                        &ack_tx,
                        &header,
                        -libc::EINVAL,
                        server_started,
                        stages,
                        false,
                    );
                    continue;
                }
                if active_ids.contains(header.request_id) {
                    send_error(
                        &ack_tx,
                        &header,
                        -libc::EALREADY,
                        server_started,
                        stages,
                        false,
                    );
                    continue;
                }
                close_request = Some((header.request_id, server_started, stages));
                break;
            }
            _ => {
                send_error(
                    &ack_tx,
                    &header,
                    -libc::EPROTO,
                    server_started,
                    stages,
                    false,
                );
                break;
            }
        }

        let task = SubmitTask {
            volume: volume.clone(),
            header,
            payload,
            server_started,
            queued_at: StageInstant::now(),
            intake_ns: stages.intake_ns,
            pending_tx: pending_tx.clone(),
            ack_tx: ack_tx.clone(),
            payload_tx: payload_tx.clone(),
            recycle_tx: recycle_tx.clone(),
        };
        if let Err(error) = submit_lane[lane_id].send(task) {
            let mut task = error.0;
            let payload = std::mem::take(&mut task.payload);
            let _ = task.recycle_tx.try_send(payload);
            send_error(
                &task.ack_tx,
                &task.header,
                -libc::ESHUTDOWN,
                task.server_started,
                StageTimings {
                    intake_ns: task.intake_ns,
                    ..StageTimings::default()
                },
                true,
            );
        }
        payload = Vec::new();
    }

    drop(pending_tx);
    if let Err(error) = dispatcher_handle.join() {
        tracing::error!(?error, "direct IO durability dispatcher panicked");
    }

    if let Some((request_id, started, mut stages)) = close_request {
        stages.server_total_ns = started.elapsed().as_nanos() as u64;
        let _ = ack_tx.send(Outbound::header_only(
            response(OP_CLOSE, request_id, 0, 0, stages),
            false,
        ));
    }
    drop(ack_tx);
    drop(payload_tx);
    if let Err(error) = writer_handle.join() {
        tracing::error!(?error, "direct IO writer panicked");
    }
    alive.store(false, Ordering::Release);
}

fn read_hello(
    stream: &mut UnixStream,
    shutdown: &ShutdownState,
) -> Result<Option<(RequestHeader, String)>, (RequestHeader, i32)> {
    let always_alive = AtomicBool::new(true);
    let header = match read_request_header(stream, shutdown, &always_alive) {
        Ok(Some(header)) => header,
        Ok(None) | Err(_) => return Ok(None),
    };
    if header.opcode != OP_HELLO
        || header.flags != 0
        || header.offset != 0
        || header.io_len != 0
        || header.payload_len == 0
        || header.payload_len as usize > MAX_VOLUME_NAME_BYTES
    {
        return Err((header, -libc::EPROTO));
    }
    let mut payload = vec![0u8; header.payload_len as usize];
    match read_exact_interruptible(stream, &mut payload, shutdown, &always_alive) {
        Ok(true) => {}
        Ok(false) | Err(_) => return Ok(None),
    }
    match String::from_utf8(payload) {
        Ok(volume) => Ok(Some((header, volume))),
        Err(_) => Err((header, -libc::EINVAL)),
    }
}

fn submit_worker_loop(input: Receiver<SubmitTask>) {
    while let Ok(task) = input.recv() {
        let stages = StageTimings {
            submit_queue_ns: task.queued_at.elapsed().as_nanos() as u64,
            intake_ns: task.intake_ns,
            ..StageTimings::default()
        };
        match task.header.opcode {
            OP_WRITE => handle_submit_write(task, stages),
            OP_READ => handle_submit_read(task, stages),
            _ => unreachable!("only IO requests enter direct IO submit lanes"),
        }
    }
}

fn handle_submit_write(mut task: SubmitTask, mut stages: StageTimings) {
    let header = task.header;
    if header.io_len == 0
        || header.payload_len != header.io_len
        || task.payload.len() != header.io_len as usize
        || header.offset % BLOCK_SIZE as u64 != 0
        || header.io_len % BLOCK_SIZE != 0
    {
        let payload = std::mem::take(&mut task.payload);
        let _ = task.recycle_tx.try_send(payload);
        send_error(
            &task.ack_tx,
            &header,
            -libc::EINVAL,
            task.server_started,
            stages,
            true,
        );
        return;
    }

    let submit_started = StageInstant::now();
    let result = task
        .volume
        .write_aligned_deferred(header.offset, &task.payload);
    let submitted_at = StageInstant::now();
    stages.engine_submit_ns = submitted_at
        .saturating_duration_since(submit_started)
        .as_nanos() as u64;
    let payload = std::mem::take(&mut task.payload);
    let _ = task.recycle_tx.try_send(payload);

    match result {
        Ok(ticket) => {
            let pending = PendingWrite {
                request_id: header.request_id,
                ticket,
                server_started: task.server_started,
                submitted_at,
                submit_queue_ns: stages.submit_queue_ns,
                engine_submit_ns: stages.engine_submit_ns,
                intake_ns: stages.intake_ns,
                bytes: header.io_len,
            };
            if let Err(error) = task.pending_tx.send(pending) {
                error.0.ticket.abandon();
                send_error(
                    &task.ack_tx,
                    &header,
                    -libc::ESHUTDOWN,
                    task.server_started,
                    stages,
                    true,
                );
            }
        }
        Err(error) => send_error(
            &task.ack_tx,
            &header,
            status_from_error(&error),
            task.server_started,
            stages,
            true,
        ),
    }
}

fn handle_submit_read(mut task: SubmitTask, mut stages: StageTimings) {
    let header = task.header;
    if header.payload_len != 0
        || !task.payload.is_empty()
        || header.io_len == 0
        || header.io_len as usize > MAX_DIRECT_IO_BYTES
        || header.offset % BLOCK_SIZE as u64 != 0
        || header.io_len % BLOCK_SIZE != 0
    {
        let payload = std::mem::take(&mut task.payload);
        let _ = task.recycle_tx.try_send(payload);
        send_error(
            &task.ack_tx,
            &header,
            -libc::EINVAL,
            task.server_started,
            stages,
            true,
        );
        return;
    }

    let request_payload = std::mem::take(&mut task.payload);
    let _ = task.recycle_tx.try_send(request_payload);
    let mut data = vec![0u8; header.io_len as usize];
    let submit_started = StageInstant::now();
    let result = task.volume.read_into(header.offset, &mut data);
    stages.engine_submit_ns = submit_started.elapsed().as_nanos() as u64;
    match result {
        Ok(()) => {
            stages.server_total_ns = task.server_started.elapsed().as_nanos() as u64;
            let _ = task.payload_tx.send(Outbound::new(
                response(OP_READ, header.request_id, 0, header.io_len, stages),
                data,
                true,
            ));
        }
        Err(error) => send_error(
            &task.ack_tx,
            &header,
            status_from_error(&error),
            task.server_started,
            stages,
            true,
        ),
    }
}

fn durability_loop(
    input: Receiver<PendingWrite>,
    output: Sender<Outbound>,
    alive: Arc<AtomicBool>,
    shutdown: Arc<ShutdownState>,
) {
    let (wake_tx, wake_rx) = crossbeam_channel::bounded::<()>(1);
    let mut pending = Vec::<PendingWrite>::new();
    let mut input_open = true;

    while input_open || !pending.is_empty() {
        if shutdown.deadline_reached() {
            while let Ok(item) = input.try_recv() {
                pending.push(item);
            }
            abort_undurable_writes(pending, &output, &alive);
            return;
        }

        if pending.is_empty() {
            match input.recv_timeout(IO_POLL_TIMEOUT) {
                Ok(item) => arm_pending(item, &wake_tx, &mut pending),
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => input_open = false,
            }
        } else if input_open {
            crossbeam_channel::select! {
                recv(input) -> item => match item {
                    Ok(item) => arm_pending(item, &wake_tx, &mut pending),
                    Err(_) => input_open = false,
                },
                recv(wake_rx) -> _ => {},
                default(IO_POLL_TIMEOUT) => {},
            }
        } else {
            let _ = wake_rx.recv_timeout(IO_POLL_TIMEOUT);
        }

        while let Ok(item) = input.try_recv() {
            arm_pending(item, &wake_tx, &mut pending);
        }
        while wake_rx.try_recv().is_ok() {}

        let mut idx = 0;
        while idx < pending.len() {
            if pending[idx].ticket.is_durable() {
                let item = pending.swap_remove(idx);
                complete_durable_write(item, &output, &alive, false);
            } else {
                idx += 1;
            }
        }
    }
}

fn abort_undurable_writes(
    pending: Vec<PendingWrite>,
    output: &Sender<Outbound>,
    alive: &AtomicBool,
) {
    for item in pending {
        if item.ticket.is_durable() {
            complete_durable_write(item, output, alive, true);
            continue;
        }

        item.ticket.abandon();
        let outbound = Outbound::header_only(
            response(
                OP_WRITE,
                item.request_id,
                -libc::ESHUTDOWN,
                0,
                StageTimings {
                    server_total_ns: item.server_started.elapsed().as_nanos() as u64,
                    submit_queue_ns: item.submit_queue_ns,
                    engine_submit_ns: item.engine_submit_ns,
                    intake_ns: item.intake_ns,
                    ..StageTimings::default()
                },
            ),
            true,
        );
        if output.try_send(outbound).is_err() {
            alive.store(false, Ordering::Release);
        }
    }
}

fn complete_durable_write(
    item: PendingWrite,
    output: &Sender<Outbound>,
    alive: &AtomicBool,
    nonblocking: bool,
) {
    let completed_at = StageInstant::now();
    let observed_wait_ns = completed_at
        .saturating_duration_since(item.submitted_at)
        .as_nanos() as u64;
    let completion_dispatch_ns = completion_dispatch_delay_ns(&item.ticket, completed_at)
        .min(observed_wait_ns);
    let durable_wait_ns = observed_wait_ns.saturating_sub(completion_dispatch_ns);
    item.ticket.finish();
    let outbound = Outbound::header_only(
        response(
            OP_WRITE,
            item.request_id,
            0,
            item.bytes,
            StageTimings {
                server_total_ns: item.server_started.elapsed().as_nanos() as u64,
                submit_queue_ns: item.submit_queue_ns,
                engine_submit_ns: item.engine_submit_ns,
                durable_wait_ns,
                completion_dispatch_ns,
                intake_ns: item.intake_ns,
            },
        ),
        true,
    );
    let sent = if nonblocking {
        output.try_send(outbound).is_ok()
    } else {
        output.send(outbound).is_ok()
    };
    if !sent {
        alive.store(false, Ordering::Release);
    }
}

fn arm_pending(item: PendingWrite, wake_tx: &Sender<()>, pending: &mut Vec<PendingWrite>) {
    item.ticket.arm_wakeup(wake_tx);
    pending.push(item);
}

/// Writes one response and clears its `active_ids` entry. Returns `false` on
/// a write failure, at which point the caller must stop (the session is
/// being torn down).
fn write_and_clear(
    stream: &mut UnixStream,
    mut outbound: Outbound,
    alive: &AtomicBool,
    active_ids: &ActiveIds,
) -> bool {
    let request_id = outbound.header.request_id;
    let clear_active_id = outbound.clear_active_id;
    if write_response(stream, &mut outbound).is_err() {
        alive.store(false, Ordering::Release);
        active_ids.wake_all();
        let _ = stream.shutdown(std::net::Shutdown::Both);
        return false;
    }
    if clear_active_id {
        active_ids.release(request_id);
    }
    true
}

/// Drains `ack` (header-only: write completions, errors, close) ahead of
/// `payload` (read completions, which carry a 4 KiB body) whenever both are
/// ready.
///
/// Both lanes funnel into ONE socket via one thread, so without this split a
/// read's payload write — several times the byte count of an ack — can sit
/// ahead of an already-ready write ack in strict arrival order. That ack is
/// what lets the CLIENT reuse the fio slot the completed write occupied,
/// so delaying it throttles the client's effective queue depth on every
/// write in flight behind a read, not just its own latency. This only shows
/// up under a mixed read+write workload — measured on nvme-box at QD1024:
/// pure read and pure write each matched ublk, but randrw 70/30 lagged it
/// ~1.7x before this split (see memory `direct_io_diverges_from_ublk_above_qd256`).
fn writer_loop(
    mut stream: UnixStream,
    ack_rx: Receiver<Outbound>,
    payload_rx: Receiver<Outbound>,
    alive: Arc<AtomicBool>,
    active_ids: Arc<ActiveIds>,
) {
    let _ = stream.set_write_timeout(Some(IO_WRITE_TIMEOUT));
    let mut ack_open = true;
    let mut payload_open = true;
    loop {
        let mut drained_ack = false;
        loop {
            match ack_rx.try_recv() {
                Ok(outbound) => {
                    drained_ack = true;
                    if !write_and_clear(&mut stream, outbound, &alive, &active_ids) {
                        return;
                    }
                }
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    ack_open = false;
                    break;
                }
            }
        }
        if drained_ack {
            continue;
        }

        match (ack_open, payload_open) {
            (false, false) => return,
            (true, false) => match ack_rx.recv() {
                Ok(outbound) => {
                    if !write_and_clear(&mut stream, outbound, &alive, &active_ids) {
                        return;
                    }
                }
                Err(_) => return,
            },
            (false, true) => match payload_rx.recv() {
                Ok(outbound) => {
                    if !write_and_clear(&mut stream, outbound, &alive, &active_ids) {
                        return;
                    }
                }
                Err(_) => return,
            },
            (true, true) => crossbeam_channel::select! {
                recv(ack_rx) -> outbound => match outbound {
                    Ok(outbound) => {
                        if !write_and_clear(&mut stream, outbound, &alive, &active_ids) {
                            return;
                        }
                    }
                    Err(_) => ack_open = false,
                },
                recv(payload_rx) -> outbound => match outbound {
                    Ok(outbound) => {
                        if !write_and_clear(&mut stream, outbound, &alive, &active_ids) {
                            return;
                        }
                    }
                    Err(_) => payload_open = false,
                },
            },
        }
    }
}

/// Fails a request, filling in `server_total_ns` from `started` and keeping
/// whatever stages the caller had already measured.
fn send_error(
    output: &Sender<Outbound>,
    header: &RequestHeader,
    status: i32,
    started: StageInstant,
    mut stages: StageTimings,
    clear_active_id: bool,
) {
    stages.server_total_ns = started.elapsed().as_nanos() as u64;
    let _ = output.send(Outbound::header_only(
        response(header.opcode, header.request_id, status, 0, stages),
        clear_active_id,
    ));
}

fn response(
    opcode: u16,
    request_id: u64,
    status: i32,
    bytes: u32,
    stages: StageTimings,
) -> ResponseHeader {
    ResponseHeader {
        opcode,
        status,
        request_id,
        bytes,
        payload_len: if opcode == OP_READ && status == 0 {
            bytes
        } else {
            0
        },
        server_total_ns: stages.server_total_ns,
        submit_queue_ns: stages.submit_queue_ns,
        engine_submit_ns: stages.engine_submit_ns,
        durable_wait_ns: stages.durable_wait_ns,
        completion_dispatch_ns: stages.completion_dispatch_ns,
        intake_ns: stages.intake_ns,
        // Both are stamped by `write_response`, the single funnel every
        // response goes through on its way to the socket.
        response_queue_ns: 0,
        server_send_ns: 0,
    }
}

fn write_response(stream: &mut UnixStream, outbound: &mut Outbound) -> io::Result<()> {
    if outbound.header.payload_len as usize != outbound.payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "direct IO response payload length mismatch",
        ));
    }
    outbound.header.response_queue_ns = outbound.queued_at.elapsed().as_nanos() as u64;
    outbound.header.server_send_ns = stage_monotonic_ns();
    stream.write_all(&outbound.header.encode())?;
    stream.write_all(&outbound.payload)?;
    Ok(())
}

fn read_request_header(
    stream: &mut UnixStream,
    shutdown: &ShutdownState,
    alive: &AtomicBool,
) -> io::Result<Option<RequestHeader>> {
    let mut buf = [0u8; REQUEST_HEADER_LEN];
    if !read_exact_interruptible(stream, &mut buf, shutdown, alive)? {
        return Ok(None);
    }
    RequestHeader::decode(&buf).map(Some)
}

fn read_exact_interruptible(
    stream: &mut UnixStream,
    buf: &mut [u8],
    shutdown: &ShutdownState,
    alive: &AtomicBool,
) -> io::Result<bool> {
    let mut offset = 0;
    while offset < buf.len() {
        if shutdown.is_requested() || !alive.load(Ordering::Acquire) {
            return Ok(false);
        }
        match stream.read(&mut buf[offset..]) {
            Ok(0) if offset == 0 => return Ok(false),
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "direct IO frame ended early",
                ));
            }
            Ok(read) => offset += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                continue;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(true)
}

fn status_from_error(error: &OnyxError) -> i32 {
    let errno = match error {
        OnyxError::Io(error) => error.raw_os_error().unwrap_or(libc::EIO),
        OnyxError::SpaceExhausted => libc::ENOSPC,
        OnyxError::VolumeNotFound(_) => libc::ENOENT,
        OnyxError::VolumeDeleted(_) => libc::ENODEV,
        OnyxError::OutOfBounds { .. } | OnyxError::InvalidLba { .. } => libc::EINVAL,
        OnyxError::BufferPoolFull(_) => libc::EAGAIN,
        OnyxError::MetaFenced(_) => libc::EROFS,
        _ => libc::EIO,
    };
    -errno
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn request_header_round_trips_little_endian() {
        let header = RequestHeader {
            opcode: OP_WRITE,
            flags: 0x1122_3344,
            payload_len: 4096,
            request_id: 0x0102_0304_0506_0708,
            offset: 0x1112_1314_1516_1718,
            io_len: 4096,
            client_submit_ns: 0x2122_2324_2526_2728,
        };
        let encoded = header.encode();
        assert_eq!(&encoded[0..8], b"ONIO\x02\x00\x02\x00");
        assert_eq!(&encoded[36..40], &[0; 4], "reserved stays zero");
        assert_eq!(RequestHeader::decode(&encoded).unwrap(), header);
    }

    #[test]
    fn response_header_round_trips_little_endian() {
        let header = ResponseHeader {
            opcode: OP_WRITE,
            status: -libc::EIO,
            request_id: 99,
            bytes: 4096,
            payload_len: 0,
            server_total_ns: 10,
            submit_queue_ns: 20,
            engine_submit_ns: 30,
            durable_wait_ns: 40,
            completion_dispatch_ns: 50,
            intake_ns: 60,
            response_queue_ns: 70,
            server_send_ns: 80,
        };
        assert_eq!(ResponseHeader::decode(&header.encode()).unwrap(), header);
    }

    /// A zero stamp means "the client does not stamp", and a stamp from the
    /// future means the two readings raced a clock adjustment.  Neither may
    /// turn into a fabricated latency — the whole point of this field is that
    /// a number in it can be trusted.
    #[test]
    fn intake_is_unmeasured_rather_than_fabricated() {
        assert_eq!(measure_intake_ns(0, 10_000), 0, "unstamped client");
        assert_eq!(measure_intake_ns(10_000, 5_000), 0, "stamp from the future");
        assert_eq!(measure_intake_ns(5_000, 12_000), 7_000);
    }

    #[test]
    fn active_id_limit_waits_for_completion_slot_instead_of_returning_eagain() {
        let active_ids = Arc::new(ActiveIds::new());
        let alive = Arc::new(AtomicBool::new(true));
        let shutdown = Arc::new(ShutdownState::new());
        for request_id in 0..MAX_DIRECT_IO_OUTSTANDING as u64 {
            active_ids
                .reserve(request_id, &alive, &shutdown)
                .unwrap();
        }
        assert_eq!(
            active_ids.reserve(0, &alive, &shutdown),
            Err(-libc::EALREADY),
            "duplicate IDs must still fail immediately at full depth"
        );

        let waiter_ids = active_ids.clone();
        let waiter_alive = alive.clone();
        let waiter_shutdown = shutdown.clone();
        let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
        let (done_tx, done_rx) = crossbeam_channel::bounded(1);
        let waiter = thread::spawn(move || {
            entered_tx.send(()).unwrap();
            let result = waiter_ids.reserve(
                MAX_DIRECT_IO_OUTSTANDING as u64,
                &waiter_alive,
                &waiter_shutdown,
            );
            done_tx.send(result).unwrap();
        });

        entered_rx.recv().unwrap();
        assert!(
            done_rx.recv_timeout(Duration::from_millis(10)).is_err(),
            "a full legal-depth session must apply backpressure"
        );
        active_ids.release(0);
        assert_eq!(
            done_rx.recv_timeout(IO_POLL_TIMEOUT * 5).unwrap(),
            Ok(())
        );
        assert!(active_ids.contains(MAX_DIRECT_IO_OUTSTANDING as u64));
        waiter.join().unwrap();
    }

    /// A stage measured but dropped by one of the several response builders
    /// is indistinguishable from a stage that is genuinely zero, and every
    /// builder now funnels through `response`.  Pin the whole set across the
    /// wire so a new field cannot be added to `StageTimings` and quietly go
    /// nowhere.
    #[test]
    fn error_responses_carry_every_measured_stage() {
        let (tx, rx) = crossbeam_channel::bounded(1);
        let request = RequestHeader {
            opcode: OP_WRITE,
            flags: 0,
            payload_len: 4096,
            request_id: 11,
            offset: 0,
            io_len: 4096,
            client_submit_ns: 1,
        };
        send_error(
            &tx,
            &request,
            -libc::ENOSPC,
            StageInstant::now(),
            StageTimings {
                server_total_ns: 0,
                submit_queue_ns: 2,
                engine_submit_ns: 3,
                durable_wait_ns: 4,
                completion_dispatch_ns: 5,
                intake_ns: 6,
            },
            true,
        );

        let outbound = rx.try_recv().expect("error response was queued");
        let header = ResponseHeader::decode(&outbound.header.encode()).unwrap();
        assert_eq!(header.status, -libc::ENOSPC);
        assert_eq!(header.submit_queue_ns, 2);
        assert_eq!(header.engine_submit_ns, 3);
        assert_eq!(header.durable_wait_ns, 4);
        assert_eq!(header.completion_dispatch_ns, 5);
        assert_eq!(header.intake_ns, 6, "intake must survive the error path");
        assert!(outbound.clear_active_id);
    }

    /// `server_total_ns` stops when a response is *built*; the per-session
    /// writer thread then has to be scheduled before the bytes leave.  That
    /// gap was invisible until now, so assert the funnel actually stamps it.
    #[test]
    fn write_response_stamps_the_writer_queue_delay() {
        let (mut server, mut client) = UnixStream::pair().unwrap();
        let mut outbound = Outbound::header_only(
            response(OP_WRITE, 7, 0, 4096, StageTimings::default()),
            false,
        );
        thread::sleep(Duration::from_millis(2));
        let before = monotonic_ns();
        write_response(&mut server, &mut outbound).unwrap();

        let mut buf = [0u8; RESPONSE_HEADER_LEN];
        client.read_exact(&mut buf).unwrap();
        let decoded = ResponseHeader::decode(&buf).unwrap();
        assert!(
            decoded.response_queue_ns >= 2_000_000,
            "queue delay {} ns should cover the 2 ms the response waited",
            decoded.response_queue_ns
        );
        assert!(decoded.server_send_ns >= before);
    }

    #[test]
    fn derives_separate_data_socket_path() {
        assert_eq!(
            direct_io_socket_path(Path::new("/tmp/onyx.sock")),
            Path::new("/tmp/onyx.sock.io")
        );
    }

    #[test]
    fn listener_rejects_hello_when_engine_is_bare() {
        let dir = tempfile::tempdir().unwrap();
        let control_path = dir.path().join("control.sock");
        let engine = Arc::new(ArcSwap::from_pointee(None::<OnyxEngine>));
        let mut server =
            DirectIoServer::start(&control_path, engine, 2, 3, true, None, Vec::new()).unwrap();

        let mut client = UnixStream::connect(server.socket_path()).unwrap();
        let volume = b"test-volume";
        let hello = RequestHeader {
            opcode: OP_HELLO,
            flags: 0,
            payload_len: volume.len() as u32,
            request_id: 7,
            offset: 0,
            io_len: 0,
            client_submit_ns: monotonic_ns(),
        };
        client.write_all(&hello.encode()).unwrap();
        client.write_all(volume).unwrap();

        let mut response_buf = [0u8; RESPONSE_HEADER_LEN];
        client.read_exact(&mut response_buf).unwrap();
        let response = ResponseHeader::decode(&response_buf).unwrap();
        assert_eq!(response.opcode, OP_HELLO);
        assert_eq!(response.request_id, 7);
        assert_eq!(response.status, -libc::ENODEV);

        server.shutdown_and_join();
    }

    #[test]
    fn durability_dispatcher_deadline_ignores_open_input_channel() {
        let shutdown = Arc::new(ShutdownState::new());
        shutdown.request_with_grace(Duration::ZERO);
        let (_input_tx, input_rx) = crossbeam_channel::bounded::<PendingWrite>(1);
        let (output_tx, _output_rx) = crossbeam_channel::bounded::<Outbound>(1);
        let alive = Arc::new(AtomicBool::new(true));
        let (done_tx, done_rx) = crossbeam_channel::bounded(1);

        let handle = thread::spawn(move || {
            durability_loop(input_rx, output_tx, alive, shutdown);
            let _ = done_tx.send(());
        });

        done_rx
            .recv_timeout(IO_POLL_TIMEOUT * 5)
            .expect("dispatcher waited despite an expired global shutdown deadline");
        handle.join().unwrap();
    }
}
