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
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use crossbeam_channel::{Receiver, Sender};

use crate::affinity::{self, ThreadRole};
use crate::engine::OnyxEngine;
use crate::error::OnyxError;
use crate::types::BLOCK_SIZE;
use crate::volume::{OnyxVolume, VolumeWriteTicket};

pub const DIRECT_IO_MAGIC: [u8; 4] = *b"ONIO";
pub const DIRECT_IO_VERSION: u16 = 1;
pub const REQUEST_HEADER_LEN: usize = 40;
pub const RESPONSE_HEADER_LEN: usize = 72;
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
        })
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
    senders: Arc<Vec<Sender<SubmitTask>>>,
    worker_handles: Vec<JoinHandle<()>>,
}

impl SubmitLanes {
    fn start(
        nr_queues: usize,
        queue_workers: usize,
        direct_io_cpus: Arc<Vec<usize>>,
    ) -> io::Result<Self> {
        let sessions_per_lane = MAX_DIRECT_IO_SESSIONS.div_ceil(nr_queues);
        let lane_capacity = MAX_DIRECT_IO_OUTSTANDING.saturating_mul(sessions_per_lane.max(1));
        let mut senders = Vec::with_capacity(nr_queues);
        let mut worker_handles = Vec::with_capacity(nr_queues.saturating_mul(queue_workers));

        for lane_id in 0..nr_queues {
            let (tx, rx) = crossbeam_channel::bounded::<SubmitTask>(lane_capacity);
            senders.push(tx);
            for worker_id in 0..queue_workers {
                let worker_rx = rx.clone();
                let worker_cpus = direct_io_cpus.clone();
                let handle = thread::Builder::new()
                    .name(format!("direct-io-submit-q{lane_id}-w{worker_id}"))
                    .spawn(move || {
                        bind_direct_io_thread(
                            &worker_cpus,
                            lane_id
                                .saturating_mul(queue_workers)
                                .saturating_add(worker_id),
                        );
                        submit_worker_loop(worker_rx);
                    });
                match handle {
                    Ok(handle) => worker_handles.push(handle),
                    Err(error) => {
                        drop(rx);
                        drop(senders);
                        for handle in worker_handles {
                            let _ = handle.join();
                        }
                        return Err(error);
                    }
                }
            }
        }

        Ok(Self {
            senders: Arc::new(senders),
            worker_handles,
        })
    }

    fn shutdown_and_join(self) {
        drop(self.senders);
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
        let submit_lanes = SubmitLanes::start(nr_queues, queue_workers, direct_io_cpus.clone())?;
        let lane_senders = submit_lanes.senders.clone();
        let shutdown = Arc::new(ShutdownState::new());
        let thread_shutdown = shutdown.clone();
        let listener_handle = thread::Builder::new()
            .name("direct-io-listener".into())
            .spawn(move || {
                bind_direct_io_thread(&direct_io_cpus, nr_queues.saturating_mul(queue_workers));
                listener_loop(
                    listener,
                    engine,
                    lane_senders,
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
            submit_lanes = nr_queues,
            workers_per_lane = queue_workers,
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
    submit_lanes: Arc<Vec<Sender<SubmitTask>>>,
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
                let lane_tx = submit_lanes[lane_id].clone();
                let session_cpus = direct_io_cpus.clone();
                match thread::Builder::new()
                    .name(format!("direct-io-session-{session_id}"))
                    .spawn(move || {
                        handle_session(
                            stream,
                            session_engine,
                            session_shutdown,
                            session_id,
                            lane_tx,
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
    server_started: Instant,
    submitted_at: Instant,
    submit_queue_ns: u64,
    engine_submit_ns: u64,
    bytes: u32,
}

struct Outbound {
    header: ResponseHeader,
    payload: Vec<u8>,
    clear_active_id: bool,
}

struct SubmitTask {
    volume: Arc<OnyxVolume>,
    header: RequestHeader,
    payload: Vec<u8>,
    server_started: Instant,
    queued_at: Instant,
    pending_tx: Sender<PendingWrite>,
    outbound_tx: Sender<Outbound>,
    recycle_tx: Sender<Vec<u8>>,
}

fn handle_session(
    mut stream: UnixStream,
    engine: Arc<ArcSwap<Option<OnyxEngine>>>,
    shutdown: Arc<ShutdownState>,
    session_id: usize,
    submit_tx: Sender<SubmitTask>,
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
                &Outbound {
                    header: response(header.opcode, header.request_id, status, 0, 0, 0, 0, 0),
                    payload: Vec::new(),
                    clear_active_id: false,
                },
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
                    &Outbound {
                        header: response(
                            OP_HELLO,
                            hello.request_id,
                            status_from_error(&error),
                            0,
                            0,
                            0,
                            0,
                            0,
                        ),
                        payload: Vec::new(),
                        clear_active_id: false,
                    },
                );
                return;
            }
        },
        Some(_) | None => {
            let _ = write_response(
                &mut stream,
                &Outbound {
                    header: response(OP_HELLO, hello.request_id, -libc::ENODEV, 0, 0, 0, 0, 0),
                    payload: Vec::new(),
                    clear_active_id: false,
                },
            );
            return;
        }
    };
    let volume = Arc::new(volume);

    if write_response(
        &mut stream,
        &Outbound {
            header: response(
                OP_HELLO,
                hello.request_id,
                0,
                lane_worker_count as u32,
                0,
                0,
                0,
                0,
            ),
            payload: Vec::new(),
            clear_active_id: false,
        },
    )
    .is_err()
    {
        return;
    }

    let alive = Arc::new(AtomicBool::new(true));
    let active_ids = Arc::new(Mutex::new(HashSet::<u64>::new()));
    let (outbound_tx, outbound_rx) = crossbeam_channel::bounded(MAX_DIRECT_IO_OUTSTANDING);
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
            writer_loop(writer_stream, outbound_rx, writer_alive, writer_active_ids)
        });
    let writer_handle = match writer_handle {
        Ok(handle) => handle,
        Err(_) => return,
    };

    let (pending_tx, pending_rx) = crossbeam_channel::bounded(MAX_DIRECT_IO_OUTSTANDING);
    let (recycle_tx, recycle_rx) = crossbeam_channel::bounded(MAX_DIRECT_IO_OUTSTANDING);
    let dispatcher_tx = outbound_tx.clone();
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
            drop(outbound_tx);
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
        let server_started = Instant::now();

        if header.flags != 0 {
            send_immediate_error(&outbound_tx, &header, -libc::EINVAL, server_started, false);
            continue;
        }
        match header.opcode {
            OP_WRITE | OP_READ => {
                if header.io_len as usize > MAX_DIRECT_IO_BYTES
                    || header.offset.checked_add(header.io_len as u64).is_none()
                {
                    send_immediate_error(
                        &outbound_tx,
                        &header,
                        -libc::EINVAL,
                        server_started,
                        false,
                    );
                    continue;
                }

                let mut ids = active_ids.lock().unwrap();
                if ids.contains(&header.request_id) {
                    drop(ids);
                    send_immediate_error(
                        &outbound_tx,
                        &header,
                        -libc::EALREADY,
                        server_started,
                        false,
                    );
                    continue;
                }
                if ids.len() >= MAX_DIRECT_IO_OUTSTANDING {
                    drop(ids);
                    send_immediate_error(
                        &outbound_tx,
                        &header,
                        -libc::EAGAIN,
                        server_started,
                        false,
                    );
                    continue;
                }
                ids.insert(header.request_id);
            }
            OP_CLOSE => {
                if header.payload_len != 0 || header.io_len != 0 || header.offset != 0 {
                    send_immediate_error(
                        &outbound_tx,
                        &header,
                        -libc::EINVAL,
                        server_started,
                        false,
                    );
                    continue;
                }
                if active_ids.lock().unwrap().contains(&header.request_id) {
                    send_immediate_error(
                        &outbound_tx,
                        &header,
                        -libc::EALREADY,
                        server_started,
                        false,
                    );
                    continue;
                }
                close_request = Some((header.request_id, server_started));
                break;
            }
            _ => {
                send_immediate_error(&outbound_tx, &header, -libc::EPROTO, server_started, false);
                break;
            }
        }

        let task = SubmitTask {
            volume: volume.clone(),
            header,
            payload,
            server_started,
            queued_at: Instant::now(),
            pending_tx: pending_tx.clone(),
            outbound_tx: outbound_tx.clone(),
            recycle_tx: recycle_tx.clone(),
        };
        if let Err(error) = submit_tx.send(task) {
            let mut task = error.0;
            let payload = std::mem::take(&mut task.payload);
            let _ = task.recycle_tx.try_send(payload);
            send_immediate_error(
                &task.outbound_tx,
                &task.header,
                -libc::ESHUTDOWN,
                task.server_started,
                true,
            );
        }
        payload = Vec::new();
    }

    drop(pending_tx);
    if let Err(error) = dispatcher_handle.join() {
        tracing::error!(?error, "direct IO durability dispatcher panicked");
    }

    if let Some((request_id, started)) = close_request {
        let _ = outbound_tx.send(Outbound {
            header: response(
                OP_CLOSE,
                request_id,
                0,
                0,
                started.elapsed().as_nanos() as u64,
                0,
                0,
                0,
            ),
            payload: Vec::new(),
            clear_active_id: false,
        });
    }
    drop(outbound_tx);
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
        let submit_queue_ns = task.queued_at.elapsed().as_nanos() as u64;
        match task.header.opcode {
            OP_WRITE => handle_submit_write(task, submit_queue_ns),
            OP_READ => handle_submit_read(task, submit_queue_ns),
            _ => unreachable!("only IO requests enter direct IO submit lanes"),
        }
    }
}

fn handle_submit_write(mut task: SubmitTask, submit_queue_ns: u64) {
    let header = task.header;
    if header.io_len == 0
        || header.payload_len != header.io_len
        || task.payload.len() != header.io_len as usize
        || header.offset % BLOCK_SIZE as u64 != 0
        || header.io_len % BLOCK_SIZE != 0
    {
        let payload = std::mem::take(&mut task.payload);
        let _ = task.recycle_tx.try_send(payload);
        send_timed_error(
            &task.outbound_tx,
            &header,
            -libc::EINVAL,
            task.server_started,
            submit_queue_ns,
            0,
            true,
        );
        return;
    }

    let submit_started = Instant::now();
    let result = task
        .volume
        .write_aligned_deferred(header.offset, &task.payload);
    let submitted_at = Instant::now();
    let engine_submit_ns = submitted_at
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
                submit_queue_ns,
                engine_submit_ns,
                bytes: header.io_len,
            };
            if let Err(error) = task.pending_tx.send(pending) {
                error.0.ticket.abandon();
                send_timed_error(
                    &task.outbound_tx,
                    &header,
                    -libc::ESHUTDOWN,
                    task.server_started,
                    submit_queue_ns,
                    engine_submit_ns,
                    true,
                );
            }
        }
        Err(error) => send_timed_error(
            &task.outbound_tx,
            &header,
            status_from_error(&error),
            task.server_started,
            submit_queue_ns,
            engine_submit_ns,
            true,
        ),
    }
}

fn handle_submit_read(mut task: SubmitTask, submit_queue_ns: u64) {
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
        send_timed_error(
            &task.outbound_tx,
            &header,
            -libc::EINVAL,
            task.server_started,
            submit_queue_ns,
            0,
            true,
        );
        return;
    }

    let request_payload = std::mem::take(&mut task.payload);
    let _ = task.recycle_tx.try_send(request_payload);
    let mut data = vec![0u8; header.io_len as usize];
    let submit_started = Instant::now();
    match task.volume.read_into(header.offset, &mut data) {
        Ok(()) => {
            let engine_submit_ns = submit_started.elapsed().as_nanos() as u64;
            let _ = task.outbound_tx.send(Outbound {
                header: response(
                    OP_READ,
                    header.request_id,
                    0,
                    header.io_len,
                    task.server_started.elapsed().as_nanos() as u64,
                    submit_queue_ns,
                    engine_submit_ns,
                    0,
                ),
                payload: data,
                clear_active_id: true,
            });
        }
        Err(error) => {
            let engine_submit_ns = submit_started.elapsed().as_nanos() as u64;
            send_timed_error(
                &task.outbound_tx,
                &header,
                status_from_error(&error),
                task.server_started,
                submit_queue_ns,
                engine_submit_ns,
                true,
            )
        }
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
        let outbound = Outbound {
            header: response(
                OP_WRITE,
                item.request_id,
                -libc::ESHUTDOWN,
                0,
                item.server_started.elapsed().as_nanos() as u64,
                item.submit_queue_ns,
                item.engine_submit_ns,
                0,
            ),
            payload: Vec::new(),
            clear_active_id: true,
        };
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
    let completed_at = Instant::now();
    let observed_wait_ns = completed_at
        .saturating_duration_since(item.submitted_at)
        .as_nanos() as u64;
    let completion_dispatch_ns = item
        .ticket
        .completion_dispatch_delay_ns(completed_at)
        .unwrap_or(0)
        .min(observed_wait_ns);
    let durable_wait_ns = observed_wait_ns.saturating_sub(completion_dispatch_ns);
    item.ticket.finish();
    let mut header = response(
        OP_WRITE,
        item.request_id,
        0,
        item.bytes,
        item.server_started.elapsed().as_nanos() as u64,
        item.submit_queue_ns,
        item.engine_submit_ns,
        durable_wait_ns,
    );
    header.completion_dispatch_ns = completion_dispatch_ns;
    let outbound = Outbound {
        header,
        payload: Vec::new(),
        clear_active_id: true,
    };
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

fn writer_loop(
    mut stream: UnixStream,
    input: Receiver<Outbound>,
    alive: Arc<AtomicBool>,
    active_ids: Arc<Mutex<HashSet<u64>>>,
) {
    let _ = stream.set_write_timeout(Some(IO_WRITE_TIMEOUT));
    while let Ok(outbound) = input.recv() {
        let request_id = outbound.header.request_id;
        let clear_active_id = outbound.clear_active_id;
        if write_response(&mut stream, &outbound).is_err() {
            alive.store(false, Ordering::Release);
            let _ = stream.shutdown(std::net::Shutdown::Both);
            break;
        }
        if clear_active_id {
            active_ids.lock().unwrap().remove(&request_id);
        }
    }
}

fn send_immediate_error(
    output: &Sender<Outbound>,
    header: &RequestHeader,
    status: i32,
    started: Instant,
    clear_active_id: bool,
) {
    send_timed_error(output, header, status, started, 0, 0, clear_active_id);
}

fn send_timed_error(
    output: &Sender<Outbound>,
    header: &RequestHeader,
    status: i32,
    started: Instant,
    submit_queue_ns: u64,
    engine_submit_ns: u64,
    clear_active_id: bool,
) {
    let _ = output.send(Outbound {
        header: response(
            header.opcode,
            header.request_id,
            status,
            0,
            started.elapsed().as_nanos() as u64,
            submit_queue_ns,
            engine_submit_ns,
            0,
        ),
        payload: Vec::new(),
        clear_active_id,
    });
}

fn response(
    opcode: u16,
    request_id: u64,
    status: i32,
    bytes: u32,
    server_total_ns: u64,
    submit_queue_ns: u64,
    engine_submit_ns: u64,
    durable_wait_ns: u64,
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
        server_total_ns,
        submit_queue_ns,
        engine_submit_ns,
        durable_wait_ns,
        completion_dispatch_ns: 0,
    }
}

fn write_response(stream: &mut UnixStream, outbound: &Outbound) -> io::Result<()> {
    if outbound.header.payload_len as usize != outbound.payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "direct IO response payload length mismatch",
        ));
    }
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
                ))
            }
            Ok(read) => offset += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                continue
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
        };
        let encoded = header.encode();
        assert_eq!(&encoded[0..8], b"ONIO\x01\x00\x02\x00");
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
        };
        assert_eq!(ResponseHeader::decode(&header.encode()).unwrap(), header);
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
        let mut server = DirectIoServer::start(&control_path, engine, 2, 3, Vec::new()).unwrap();

        let mut client = UnixStream::connect(server.socket_path()).unwrap();
        let volume = b"test-volume";
        let hello = RequestHeader {
            opcode: OP_HELLO,
            flags: 0,
            payload_len: volume.len() as u32,
            request_id: 7,
            offset: 0,
            io_len: 0,
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
