use std::collections::HashMap;
use std::os::fd::RawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, Receiver, Sender};

use crate::error::{OnyxError, OnyxResult};
use crate::io::aligned::AlignedBuf;
use crate::io::block_backend::BlockBackend;
use crate::io::device::RawDevice;
use crate::io::uring::{IoUringSession, UringOp, UringOpResult};
use crate::metrics::EngineMetrics;
use crate::types::{Pba, BLOCK_SIZE, RESERVED_BLOCKS};

static VERIFY_LV3_WRITES: AtomicBool = AtomicBool::new(false);
static VERIFY_LV3_WRITES_INIT: AtomicBool = AtomicBool::new(false);
static SERIAL_SYSCALL_WRITES: AtomicBool = AtomicBool::new(false);
static SERIAL_SYSCALL_WRITES_INIT: AtomicBool = AtomicBool::new(false);
static TRACE_LV3_WRITES: AtomicBool = AtomicBool::new(false);
static TRACE_LV3_WRITES_INIT: AtomicBool = AtomicBool::new(false);
static LV3_WRITE_SEQ: AtomicU64 = AtomicU64::new(0);
/// Sharded by pba so 16 concurrent flusher writers don't serialise on one
/// global mutex (a single-map version measurably collapsed write throughput
/// during the 2026-07-02 replay-CRC capture).
const LV3_WRITE_RECORD_SHARDS: usize = 64;
static LV3_WRITE_RECORDS: OnceLock<Vec<Mutex<HashMap<u64, Lv3WriteRecord>>>> = OnceLock::new();

// One 8+2 RAID6 full stripe is 2 MiB. Keep each synchronous chunklet call to
// two stripes so foreground LV2 writes can interleave every ~4 ms instead of
// sitting behind a 9-10 ms, ~9 MiB background call. A single-stripe quantum
// capped measured idle drain below 2 GB/s; two stripes amortise the submit
// overhead while six executor rings retain device concurrency.
const CHUNKLET_BATCH_TARGET_BYTES: usize = 4 * 1024 * 1024;
const CHUNKLET_BATCH_EXECUTORS: usize = 6;
// Writer lanes arrive a few milliseconds apart after compression/allocation.
// This is a background-only drain path: waiting here never delays the LV2
// durable acknowledgement. Keep the cap short because one or two producer
// slabs are enough to reach the byte target with 2 MiB full stripes.
const CHUNKLET_BATCH_COALESCE: Duration = Duration::from_millis(2);
const CHUNKLET_BATCH_QUEUE_CAP: usize = 256;

// Both aggregation limits are runtime-overridable so they can be A/B'd without
// a rebuild. Measured 2026-07-25 on nvme-box: the aggregator emits 259 batches/s
// carrying ~300 KiB each — 13x short of the byte target — so every batch leaves
// on the coalesce timeout, while the six executors sit at 3.3 % utilisation and
// 4.4 writer lanes stay blocked in `submit`.
static LV3_BATCH_COALESCE_US: AtomicU64 = AtomicU64::new(0);
static LV3_BATCH_TARGET_BYTES: AtomicUsize = AtomicUsize::new(0);

/// Override the LV3 aggregation window / byte target. `0` keeps the compiled
/// default. Applies to subsequently started engines.
pub fn set_lv3_batch_tuning(coalesce_us: u64, target_bytes: usize) {
    LV3_BATCH_COALESCE_US.store(coalesce_us, Ordering::Relaxed);
    LV3_BATCH_TARGET_BYTES.store(target_bytes, Ordering::Relaxed);
}

fn lv3_batch_coalesce() -> Duration {
    match LV3_BATCH_COALESCE_US.load(Ordering::Relaxed) {
        0 => CHUNKLET_BATCH_COALESCE,
        us => Duration::from_micros(us),
    }
}

fn lv3_batch_target_bytes() -> usize {
    match LV3_BATCH_TARGET_BYTES.load(Ordering::Relaxed) {
        0 => CHUNKLET_BATCH_TARGET_BYTES,
        bytes => bytes,
    }
}

struct OwnedBatchOp {
    device_offset: u64,
    slab_index: usize,
    slab_offset: usize,
    len: usize,
}

struct ChunkletBatchRequest {
    slabs: Vec<AlignedBuf>,
    ops: Vec<OwnedBatchOp>,
    done: Sender<(Result<(), String>, Vec<AlignedBuf>)>,
    /// Stamped by the producer before `request_tx.send`. Splits the caller's
    /// blocked time into queueing for the aggregator, sitting in the coalesce
    /// window, queueing for an executor, and the device write itself.
    queued_at: Instant,
    /// Stamped by the aggregator when it takes this request off `request_rx`.
    picked_at: Option<Instant>,
}

struct ChunkletBatchWork {
    requests: Vec<ChunkletBatchRequest>,
    dispatched_at: Instant,
}

/// Cross-lane combiner for chunklet writes. Buffer lanes keep preparing data
/// concurrently, while this layer restores the batch shape that the LD's
/// thread-local rings need: one full stripe per call across six fixed executor
/// threads. Callers remain synchronous and do not publish metadata until their
/// request's combined device call completes.
struct ChunkletWriteBatcher {
    request_tx: Option<Sender<ChunkletBatchRequest>>,
    aggregate_handle: Option<std::thread::JoinHandle<()>>,
    executor_handles: Vec<std::thread::JoinHandle<()>>,
}

impl ChunkletWriteBatcher {
    fn start(device: Arc<dyn BlockBackend>, metrics: Option<Arc<EngineMetrics>>) -> Self {
        let (request_tx, request_rx) = bounded(CHUNKLET_BATCH_QUEUE_CAP);
        let (work_tx, work_rx) = bounded(CHUNKLET_BATCH_EXECUTORS * 2);

        let aggregate_metrics = metrics.clone();
        let aggregate_handle = std::thread::Builder::new()
            .name("lv3-batch-aggregate".into())
            .spawn(move || Self::aggregate_loop(request_rx, work_tx, aggregate_metrics))
            .expect("failed to spawn LV3 batch aggregator");

        let mut executor_handles = Vec::with_capacity(CHUNKLET_BATCH_EXECUTORS);
        for idx in 0..CHUNKLET_BATCH_EXECUTORS {
            let work_rx = work_rx.clone();
            let device = device.clone();
            let metrics = metrics.clone();
            executor_handles.push(
                std::thread::Builder::new()
                    .name(format!("lv3-batch-exec-{idx}"))
                    .spawn(move || {
                        crate::affinity::bind_current(crate::affinity::ThreadRole::Lv3Batch, idx);
                        Self::executor_loop(work_rx, device, metrics)
                    })
                    .expect("failed to spawn LV3 batch executor"),
            );
        }
        drop(work_rx);

        Self {
            request_tx: Some(request_tx),
            aggregate_handle: Some(aggregate_handle),
            executor_handles,
        }
    }

    fn submit(
        &self,
        slabs: Vec<AlignedBuf>,
        ops: Vec<OwnedBatchOp>,
    ) -> OnyxResult<Vec<AlignedBuf>> {
        let (done_tx, done_rx) = bounded(1);
        self.request_tx
            .as_ref()
            .ok_or_else(|| OnyxError::Io(std::io::Error::other("LV3 batcher stopped")))?
            .send(ChunkletBatchRequest {
                slabs,
                ops,
                done: done_tx,
                queued_at: Instant::now(),
                picked_at: None,
            })
            .map_err(|_| OnyxError::Io(std::io::Error::other("LV3 batcher disconnected")))?;
        let (result, slabs) = done_rx
            .recv()
            .map_err(|_| OnyxError::Io(std::io::Error::other("LV3 batch result lost")))?;
        result
            .map_err(|message| OnyxError::Io(std::io::Error::other(message)))
            .map(|()| slabs)
    }

    /// Queue every request before waiting so an oversized writer cycle can be
    /// split across all fixed executors instead of monopolising one executor.
    fn submit_many(
        &self,
        requests: Vec<(Vec<AlignedBuf>, Vec<OwnedBatchOp>)>,
        metrics: Option<&Arc<EngineMetrics>>,
    ) -> OnyxResult<Vec<Vec<AlignedBuf>>> {
        let request_tx = self
            .request_tx
            .as_ref()
            .ok_or_else(|| OnyxError::Io(std::io::Error::other("LV3 batcher stopped")))?;
        let mut receivers = Vec::with_capacity(requests.len());
        // Enqueue is separate from the wait below: a full `request_tx` means
        // the single aggregator is the constraint, and that blocking would
        // otherwise be indistinguishable from device time.
        let enqueue_started = Instant::now();
        for (slabs, ops) in requests {
            let (done_tx, done_rx) = bounded(1);
            request_tx
                .send(ChunkletBatchRequest {
                    slabs,
                    ops,
                    done: done_tx,
                    queued_at: Instant::now(),
                    picked_at: None,
                })
                .map_err(|_| OnyxError::Io(std::io::Error::other("LV3 batcher disconnected")))?;
            receivers.push(done_rx);
        }
        let wait_started = Instant::now();
        if let Some(metrics) = metrics {
            metrics.lv3_batch_enqueue_ns.fetch_add(
                wait_started.saturating_duration_since(enqueue_started).as_nanos() as u64,
                Ordering::Relaxed,
            );
        }

        let mut returned = Vec::with_capacity(receivers.len());
        let mut first_error = None;
        for done_rx in receivers {
            let (result, slabs) = done_rx
                .recv()
                .map_err(|_| OnyxError::Io(std::io::Error::other("LV3 batch result lost")))?;
            if let Err(message) = result {
                if first_error.is_none() {
                    first_error = Some(message);
                }
            }
            returned.push(slabs);
        }
        if let Some(metrics) = metrics {
            metrics.lv3_batch_wait_ns.fetch_add(
                wait_started.elapsed().as_nanos() as u64,
                Ordering::Relaxed,
            );
            metrics.lv3_batch_wait_calls.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(message) = first_error {
            return Err(OnyxError::Io(std::io::Error::other(message)));
        }
        Ok(returned)
    }

    fn aggregate_loop(
        request_rx: Receiver<ChunkletBatchRequest>,
        work_tx: Sender<ChunkletBatchWork>,
        metrics: Option<Arc<EngineMetrics>>,
    ) {
        while let Ok(mut first) = request_rx.recv() {
            first.picked_at = Some(Instant::now());
            let mut byte_count: usize = first.ops.iter().map(|op| op.len).sum();
            let mut requests = vec![first];
            let mut hit_target = false;
            let deadline = std::time::Instant::now() + lv3_batch_coalesce();
            let target_bytes = lv3_batch_target_bytes();
            while byte_count < target_bytes {
                match request_rx.try_recv() {
                    Ok(mut request) => {
                        request.picked_at = Some(Instant::now());
                        byte_count += request.ops.iter().map(|op| op.len).sum::<usize>();
                        requests.push(request);
                    }
                    Err(crossbeam_channel::TryRecvError::Empty) => {
                        let now = std::time::Instant::now();
                        if now >= deadline {
                            break;
                        }
                        match request_rx.recv_timeout(deadline.saturating_duration_since(now)) {
                            Ok(mut request) => {
                                request.picked_at = Some(Instant::now());
                                byte_count += request.ops.iter().map(|op| op.len).sum::<usize>();
                                requests.push(request);
                            }
                            Err(_) => break,
                        }
                    }
                    Err(crossbeam_channel::TryRecvError::Disconnected) => break,
                }
            }
            if byte_count >= target_bytes {
                hit_target = true;
            }
            let dispatched_at = Instant::now();
            if let Some(metrics) = &metrics {
                // `pickup` is time spent in `request_rx` behind the single
                // aggregator; `window` is the coalesce wait this request then
                // sat through. A batch that leaves on the timeout with far
                // less than `target_bytes` paid that window for nothing.
                for request in &requests {
                    let picked = request.picked_at.unwrap_or(request.queued_at);
                    metrics.lv3_batch_pickup_ns.fetch_add(
                        picked.saturating_duration_since(request.queued_at).as_nanos() as u64,
                        Ordering::Relaxed,
                    );
                    metrics.lv3_batch_window_ns.fetch_add(
                        dispatched_at.saturating_duration_since(picked).as_nanos() as u64,
                        Ordering::Relaxed,
                    );
                }
                metrics
                    .lv3_batch_requests
                    .fetch_add(requests.len() as u64, Ordering::Relaxed);
                metrics
                    .lv3_batch_bytes_at_dispatch
                    .fetch_add(byte_count as u64, Ordering::Relaxed);
                if hit_target {
                    metrics.lv3_batch_target_hits.fetch_add(1, Ordering::Relaxed);
                } else {
                    metrics
                        .lv3_batch_window_timeouts
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            if work_tx
                .send(ChunkletBatchWork {
                    requests,
                    dispatched_at,
                })
                .is_err()
            {
                break;
            }
        }
    }

    fn executor_loop(
        work_rx: Receiver<ChunkletBatchWork>,
        device: Arc<dyn BlockBackend>,
        metrics: Option<Arc<EngineMetrics>>,
    ) {
        while let Ok(work) = work_rx.recv() {
            if let Some(metrics) = &metrics {
                metrics.lv3_batch_exec_queue_ns.fetch_add(
                    work.dispatched_at.elapsed().as_nanos() as u64,
                    Ordering::Relaxed,
                );
            }
            let op_count: usize = work.requests.iter().map(|request| request.ops.len()).sum();
            let byte_count: usize = work
                .requests
                .iter()
                .flat_map(|request| request.ops.iter())
                .map(|op| op.len)
                .sum();
            let write_ops: Vec<(u64, &[u8])> = work
                .requests
                .iter()
                .flat_map(|request| {
                    request.ops.iter().map(|op| {
                        (
                            op.device_offset,
                            &request.slabs[op.slab_index].as_slice()
                                [op.slab_offset..op.slab_offset + op.len],
                        )
                    })
                })
                .collect();

            if let Some(metrics) = &metrics {
                metrics
                    .lv3_write_batch_calls
                    .fetch_add(1, Ordering::Relaxed);
                metrics
                    .lv3_write_batch_ops
                    .fetch_add(op_count as u64, Ordering::Relaxed);
                metrics
                    .lv3_write_batch_bytes
                    .fetch_add(byte_count as u64, Ordering::Relaxed);
                let inflight = metrics
                    .lv3_write_batch_inflight
                    .fetch_add(1, Ordering::Relaxed)
                    + 1;
                crate::metrics::record_counter_max(&metrics.lv3_write_batch_inflight_max, inflight);
            }
            let call_start = std::time::Instant::now();
            let result = device
                .write_many_at(&write_ops)
                .map_err(|error| error.to_string());
            if let Some(metrics) = &metrics {
                let elapsed_ns = call_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                metrics
                    .lv3_write_batch_ns
                    .fetch_add(elapsed_ns, Ordering::Relaxed);
                crate::metrics::record_counter_max(&metrics.lv3_write_batch_ns_max, elapsed_ns);
                metrics
                    .lv3_write_batch_inflight
                    .fetch_sub(1, Ordering::Relaxed);
            }
            drop(write_ops);
            for mut request in work.requests {
                let reply = match &result {
                    Ok(()) => Ok(()),
                    Err(message) => Err(message.clone()),
                };
                let slabs = std::mem::take(&mut request.slabs);
                let _ = request.done.send((reply, slabs));
            }
        }
    }
}

impl Drop for ChunkletWriteBatcher {
    fn drop(&mut self) {
        self.request_tx.take();
        if let Some(handle) = self.aggregate_handle.take() {
            let _ = handle.join();
        }
        for handle in self.executor_handles.drain(..) {
            let _ = handle.join();
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Lv3WriteRecord {
    pub seq: u64,
    pub start_pba: u64,
    pub block_offset: u32,
    pub payload_len: u32,
    pub total_size: u32,
    pub payload_crc: u32,
    pub padded_crc: u32,
}

pub fn lookup_lv3_write_record(pba: Pba) -> Option<Lv3WriteRecord> {
    LV3_WRITE_RECORDS.get().and_then(|shards| {
        shards[(pba.0 as usize) % LV3_WRITE_RECORD_SHARDS]
            .lock()
            .ok()?
            .get(&pba.0)
            .copied()
    })
}

/// Selects how `IoEngine` issues IO under the hood.
///
/// `Syscall`: classic pread/pwrite via `RawDevice` (used by today's tests and
/// pre-io_uring deployments).
/// `Uring(session)`: pushes SQEs into the supplied io_uring session and waits
/// for completions.
#[derive(Clone)]
pub enum IoBackend {
    Syscall,
    Uring(Arc<IoUringSession>),
}

impl std::fmt::Debug for IoBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IoBackend::Syscall => f.write_str("Syscall"),
            IoBackend::Uring(_) => f.write_str("Uring"),
        }
    }
}

/// IO engine for reading/writing raw data blocks on LV3.
///
/// LV3 slots are pure payload — no on-disk header. All metadata (compression,
/// crc32, original_size) lives in metadb BlockmapValue, which is crash-consistent
/// via WriteBatch. This allows a full 4096-byte payload per slot.
///
/// PBA addresses are translated by `pba_offset` (default `RESERVED_BLOCKS`)
/// so that PBA 0 from the allocator maps to device offset `pba_offset * BLOCK_SIZE`.
/// Blocks 0..pba_offset are reserved for superblock, heartbeat, and HA lock.
pub struct IoEngine {
    /// LV3 device seam. A `RawDeviceBackend` (file/blockdev, exposes a fd for
    /// the io_uring hot path) or a `ChunkletBackend` (RAID over PDs, no fd —
    /// `IoBackend::Syscall` routes its read_at/write_at/flush, while chunklet
    /// owns the cross-PD io_uring internally).
    device: Arc<dyn BlockBackend>,
    use_hugepages: bool,
    block_size: u32,
    pba_offset: u64,
    metrics: Option<Arc<EngineMetrics>>,
    backend: IoBackend,
    /// When true (and backend is `Uring`), `new_write_session` hands each flusher
    /// writer its own LV3 io_uring ring instead of the shared `backend` ring.
    /// Set from `storage.lv3_per_shard_write_rings`; false reproduces the legacy
    /// single-shared-ring behavior.
    per_shard_write_sessions: bool,
    /// When true, the flush writer allocates + pads LV3 writes to whole RAID
    /// stripes so a parity backend takes its zero-RMW full-stripe path. Set from
    /// `storage.raid_full_stripe_writes`; false makes `stripe_blocks()` report 1
    /// (no alignment, no padding) so the writer is byte-for-byte unchanged.
    full_stripe_writes: bool,
    chunklet_write_batcher: Option<ChunkletWriteBatcher>,
}

/// One operation in a batched LV3 IO submission.
pub enum LvOp<'a> {
    Read {
        pba: Pba,
        size: usize,
    },
    /// Write `payload` to LV3 starting at `pba`. Last slot is zero-padded to
    /// BLOCK_SIZE.
    Write {
        pba: Pba,
        payload: &'a [u8],
    },
}

/// Per-op result of `IoEngine::submit_batch`.
pub enum LvOpResult {
    Read(OnyxResult<Vec<u8>>),
    Write(OnyxResult<()>),
}

/// An aligned write buffer whose ownership can cross the global chunklet batch
/// executor and return to the submitting writer thread for pool reuse.
pub(crate) struct OwnedLvWrite {
    pub pba: Pba,
    pub payload_len: usize,
    pub buffer: AlignedBuf,
}

impl IoEngine {
    fn with_options(
        device: Arc<dyn BlockBackend>,
        use_hugepages: bool,
        pba_offset: u64,
        metrics: Option<Arc<EngineMetrics>>,
        backend: IoBackend,
    ) -> Self {
        let chunklet_write_batcher = device
            .uring_target()
            .is_none()
            .then(|| ChunkletWriteBatcher::start(device.clone(), metrics.clone()));
        Self {
            device,
            use_hugepages,
            block_size: BLOCK_SIZE,
            pba_offset,
            metrics,
            backend,
            per_shard_write_sessions: false,
            full_stripe_writes: false,
            chunklet_write_batcher,
        }
    }

    /// Funnel for every existing `RawDevice`-taking constructor: `RawDevice`
    /// itself implements `BlockBackend`, so we just wrap it in an `Arc`. Keeps
    /// those constructor signatures and all their call sites unchanged.
    fn with_raw_device(
        data_device: RawDevice,
        use_hugepages: bool,
        pba_offset: u64,
        metrics: Option<Arc<EngineMetrics>>,
        backend: IoBackend,
    ) -> Self {
        Self::with_options(
            Arc::new(data_device),
            use_hugepages,
            pba_offset,
            metrics,
            backend,
        )
    }

    /// Build an IoEngine over an arbitrary `BlockBackend` with an explicit
    /// submission mode. Used by the engine startup to construct over either a
    /// `RawDevice`-backed Arc (Syscall/Uring) or a chunklet backend (Syscall).
    pub fn new_block(
        device: Arc<dyn BlockBackend>,
        use_hugepages: bool,
        metrics: Arc<EngineMetrics>,
        backend: IoBackend,
    ) -> Self {
        Self::with_options(
            device,
            use_hugepages,
            RESERVED_BLOCKS,
            Some(metrics),
            backend,
        )
    }

    /// Build an IoEngine over a chunklet `BlockBackend`. Uses the `Syscall`
    /// submission mode: chunklet has no single fd, so io_uring lives inside the
    /// chunklet LD, and `Syscall` simply routes `read_at`/`write_at`/`flush`
    /// through the backend.
    pub fn new_chunklet(
        device: Arc<dyn BlockBackend>,
        use_hugepages: bool,
        metrics: Arc<EngineMetrics>,
    ) -> Self {
        Self::new_block(device, use_hugepages, metrics, IoBackend::Syscall)
    }

    /// `(fd, base_offset)` for io_uring SQE construction. Only valid when the
    /// backend is a single-fd device; the `Uring` submission mode is only ever
    /// paired with such a backend, so this is infallible in practice.
    fn uring_fd_base(&self) -> OnyxResult<(RawFd, u64)> {
        self.device.uring_target().ok_or_else(|| {
            OnyxError::Config(
                "io_uring backend requires a single-fd device; chunklet uses the syscall path"
                    .into(),
            )
        })
    }

    /// Enable/disable handing each flusher writer its own LV3 write ring.
    /// Builder style so existing constructors stay unchanged.
    pub fn with_per_shard_write_sessions(mut self, enabled: bool) -> Self {
        self.per_shard_write_sessions = enabled;
        self
    }

    /// Enable/disable RAID full-stripe-aligned LV3 writes (roadmap ③). When on,
    /// `stripe_blocks()` reports the backend's stripe width so the flush writer
    /// pads writes to whole stripes; when off it reports 1 (legacy behavior).
    pub fn with_full_stripe_writes(mut self, enabled: bool) -> Self {
        self.full_stripe_writes = enabled;
        self
    }

    /// Create a fresh, dedicated io_uring session for one flusher writer shard's
    /// LV3 writes, sized like the engine's shared backend ring. Returns `None`
    /// for the syscall backend. Each writer owns its own ring so that writers no
    /// longer serialize on the single shared `backend` ring mutex held across
    /// `submit_and_wait` (off-CPU profiling pinned ~38% of writer off-CPU time to
    /// that lock). Mirrors the per-worker session design the read path already
    /// uses (`io::read_pool`).
    ///
    /// IMPORTANT: call this from *inside* the writer thread (after the NUMA
    /// affinity bind) so the ring's pages fault in NUMA-local to that writer —
    /// do not cross NUMA nodes by creating all rings up front on the init thread.
    pub fn new_write_session(&self) -> OnyxResult<Option<Arc<IoUringSession>>> {
        if !self.per_shard_write_sessions {
            return Ok(None);
        }
        match &self.backend {
            IoBackend::Syscall => Ok(None),
            IoBackend::Uring(default) => {
                Ok(Some(Arc::new(IoUringSession::new(default.sq_entries())?)))
            }
        }
    }

    /// Create an IoEngine with standard PBA offset (RESERVED_BLOCKS) and the
    /// classic syscall backend (pread/pwrite).
    pub fn new(data_device: RawDevice, use_hugepages: bool) -> Self {
        Self::with_raw_device(
            data_device,
            use_hugepages,
            RESERVED_BLOCKS,
            None,
            IoBackend::Syscall,
        )
    }

    /// Create an IoEngine with metrics attached (syscall backend).
    pub fn new_with_metrics(
        data_device: RawDevice,
        use_hugepages: bool,
        metrics: Arc<EngineMetrics>,
    ) -> Self {
        Self::with_raw_device(
            data_device,
            use_hugepages,
            RESERVED_BLOCKS,
            Some(metrics),
            IoBackend::Syscall,
        )
    }

    /// Create an IoEngine backed by the supplied io_uring session.
    pub fn new_uring(
        data_device: RawDevice,
        use_hugepages: bool,
        ring: Arc<IoUringSession>,
    ) -> Self {
        Self::with_raw_device(
            data_device,
            use_hugepages,
            RESERVED_BLOCKS,
            None,
            IoBackend::Uring(ring),
        )
    }

    /// Create an IoEngine with metrics attached and io_uring backend.
    pub fn new_with_metrics_uring(
        data_device: RawDevice,
        use_hugepages: bool,
        metrics: Arc<EngineMetrics>,
        ring: Arc<IoUringSession>,
    ) -> Self {
        Self::with_raw_device(
            data_device,
            use_hugepages,
            RESERVED_BLOCKS,
            Some(metrics),
            IoBackend::Uring(ring),
        )
    }

    /// Create an IoEngine without PBA offset (PBA 0 = device offset 0).
    /// For testing only — production code should use `new()`.
    pub fn new_raw(data_device: RawDevice, use_hugepages: bool) -> Self {
        Self::with_raw_device(data_device, use_hugepages, 0, None, IoBackend::Syscall)
    }

    pub fn backend(&self) -> &IoBackend {
        &self.backend
    }

    fn record_lv3_read(&self, bytes: usize) {
        if let Some(metrics) = &self.metrics {
            metrics.lv3_read_ops.fetch_add(1, Ordering::Relaxed);
            metrics
                .lv3_read_compressed_bytes
                .fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }

    fn record_lv3_write(&self, bytes: usize) {
        if let Some(metrics) = &self.metrics {
            metrics.lv3_write_ops.fetch_add(1, Ordering::Relaxed);
            metrics
                .lv3_write_compressed_bytes
                .fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }

    fn record_lv3_write_batch(&self, ops: usize, bytes: usize, slab_allocated: bool) {
        if let Some(metrics) = &self.metrics {
            metrics
                .lv3_write_batch_calls
                .fetch_add(1, Ordering::Relaxed);
            metrics
                .lv3_write_batch_ops
                .fetch_add(ops as u64, Ordering::Relaxed);
            metrics
                .lv3_write_batch_bytes
                .fetch_add(bytes as u64, Ordering::Relaxed);
            if slab_allocated {
                metrics
                    .lv3_write_slab_allocs
                    .fetch_add(1, Ordering::Relaxed);
                metrics
                    .lv3_write_slab_bytes
                    .fetch_add(bytes as u64, Ordering::Relaxed);
            }
        }
    }

    fn verify_lv3_writes_enabled() -> bool {
        if !VERIFY_LV3_WRITES_INIT.load(Ordering::Relaxed) {
            let enabled = Self::env_flag("ONYX_VERIFY_LV3_WRITES");
            VERIFY_LV3_WRITES.store(enabled, Ordering::Relaxed);
            VERIFY_LV3_WRITES_INIT.store(true, Ordering::Relaxed);
        }
        VERIFY_LV3_WRITES.load(Ordering::Relaxed)
    }

    fn syscall_write_serial_enabled() -> bool {
        if !SERIAL_SYSCALL_WRITES_INIT.load(Ordering::Relaxed) {
            let enabled = Self::env_flag("ONYX_SYSCALL_WRITE_SERIAL");
            SERIAL_SYSCALL_WRITES.store(enabled, Ordering::Relaxed);
            SERIAL_SYSCALL_WRITES_INIT.store(true, Ordering::Relaxed);
        }
        SERIAL_SYSCALL_WRITES.load(Ordering::Relaxed)
    }

    fn trace_lv3_writes_enabled() -> bool {
        if !TRACE_LV3_WRITES_INIT.load(Ordering::Relaxed) {
            let enabled = Self::env_flag("ONYX_TRACE_LV3_WRITES");
            TRACE_LV3_WRITES.store(enabled, Ordering::Relaxed);
            TRACE_LV3_WRITES_INIT.store(true, Ordering::Relaxed);
        }
        TRACE_LV3_WRITES.load(Ordering::Relaxed)
    }

    fn env_flag(name: &str) -> bool {
        std::env::var(name)
            .map(|value| {
                matches!(
                    value.as_str(),
                    "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
                )
            })
            .unwrap_or(false)
    }

    fn verify_write_payload(&self, pba: Pba, payload: &[u8], total_size: usize) -> OnyxResult<()> {
        if !Self::verify_lv3_writes_enabled() {
            return Ok(());
        }

        let offset = self.pba_to_offset(pba);
        let mut expected = vec![0u8; total_size];
        expected[..payload.len()].copy_from_slice(payload);
        let mut actual = AlignedBuf::new(total_size, self.use_hugepages)?;
        let mut first_diff = 0usize;
        let mut recovered_after_retries = None;
        for attempt in 0..=3 {
            self.device.read_at(actual.as_mut_slice(), offset)?;
            if actual.as_slice() == expected.as_slice() {
                recovered_after_retries = Some(attempt);
                break;
            }
            first_diff = actual
                .as_slice()
                .iter()
                .zip(expected.iter())
                .position(|(a, e)| a != e)
                .unwrap_or(0);
            if attempt < 3 {
                std::thread::sleep(Duration::from_micros(100u64 << attempt));
            }
        }

        if let Some(attempt) = recovered_after_retries {
            if attempt > 0 {
                tracing::warn!(
                    pba = pba.0,
                    offset,
                    len = payload.len(),
                    total_size,
                    retry = attempt,
                    "LV3 post-write verification recovered after retry"
                );
            }
            return Ok(());
        }

        let actual_byte = actual.as_slice()[first_diff];
        let expected_byte = expected[first_diff];
        tracing::error!(
            pba = pba.0,
            offset,
            len = payload.len(),
            total_size,
            first_diff,
            actual_byte,
            expected_byte,
            attempts = 4,
            actual_crc = crc32fast::hash(actual.as_slice()),
            expected_crc = crc32fast::hash(expected.as_slice()),
            "LV3 post-write verification failed"
        );
        Err(OnyxError::Device {
            path: PathBuf::from(self.device.label()),
            reason: format!(
                "LV3 post-write verification failed pba={} first_diff={} actual={} expected={}",
                pba.0, first_diff, actual_byte, expected_byte
            ),
        })
    }

    fn record_lv3_write_trace(&self, pba: Pba, payload: &[u8], total_size: usize) {
        if !Self::trace_lv3_writes_enabled() {
            return;
        }

        let seq = LV3_WRITE_SEQ.fetch_add(1, Ordering::Relaxed) + 1;
        let mut padded = vec![0u8; total_size];
        padded[..payload.len()].copy_from_slice(payload);
        let record = Lv3WriteRecord {
            seq,
            start_pba: pba.0,
            block_offset: 0,
            payload_len: payload.len() as u32,
            total_size: total_size as u32,
            payload_crc: crc32fast::hash(payload),
            padded_crc: crc32fast::hash(&padded),
        };
        let blocks = (total_size / self.block_size as usize).max(1);
        let shards = LV3_WRITE_RECORDS.get_or_init(|| {
            (0..LV3_WRITE_RECORD_SHARDS)
                .map(|_| Mutex::new(HashMap::new()))
                .collect()
        });
        for block_offset in 0..blocks {
            let block_pba = pba.0 + block_offset as u64;
            if let Ok(mut shard) = shards[(block_pba as usize) % LV3_WRITE_RECORD_SHARDS].lock() {
                shard.insert(
                    block_pba,
                    Lv3WriteRecord {
                        block_offset: block_offset as u32,
                        ..record
                    },
                );
            }
        }
    }

    fn pba_to_offset(&self, pba: Pba) -> u64 {
        (pba.0 + self.pba_offset) * self.block_size as u64
    }

    /// RAID full-stripe width in blocks for the underlying backend (`1` = no
    /// stripe constraint). The flush writer uses this to allocate + pad LV3
    /// writes to whole stripes so a chunklet RAID5/6 LD takes its zero-RMW
    /// full-stripe path. Reports `1` unless `full_stripe_writes` is enabled — the
    /// single gate for the whole feature, so the writer/allocator paths no-op
    /// when the flag is off.
    pub fn stripe_blocks(&self) -> u32 {
        if !self.full_stripe_writes {
            return 1;
        }
        self.device.stripe_blocks().max(1)
    }

    /// Phase to add before the stripe-alignment modulo, in blocks. Device offset
    /// is `(pba + pba_offset) * block_size`, so a stripe-aligned *device* offset
    /// needs `(pba + pba_offset) % stripe_blocks == 0`, i.e. the allocator must
    /// align PBAs against `pba_offset % stripe_blocks`, not 0. With
    /// `pba_offset = RESERVED_BLOCKS = 8` and a 6-block stripe this phase is 2.
    pub fn stripe_phase(&self) -> u32 {
        let s = self.stripe_blocks();
        if s <= 1 {
            0
        } else {
            (self.pba_offset % s as u64) as u32
        }
    }

    fn validate_uring_result(
        &self,
        op: &str,
        offset: u64,
        expected: u32,
        result: &UringOpResult,
    ) -> OnyxResult<()> {
        if let Some(errno) = result.errno() {
            return Err(OnyxError::Device {
                path: PathBuf::from(self.device.label()),
                reason: format!(
                    "io_uring {op} failed at offset={offset}: errno={errno} ({})",
                    std::io::Error::from_raw_os_error(errno)
                ),
            });
        }
        let bytes = result.bytes().unwrap_or(0);
        if bytes != expected {
            return Err(OnyxError::Device {
                path: PathBuf::from(self.device.label()),
                reason: format!(
                    "io_uring {op} short transfer at offset={offset}: got {bytes} of {expected}"
                ),
            });
        }
        Ok(())
    }

    /// Write raw payload to LV3 at the given PBA slot.
    /// Payload is zero-padded to BLOCK_SIZE if shorter.
    pub fn write_block(&self, pba: Pba, payload: &[u8]) -> OnyxResult<()> {
        if payload.len() > self.block_size as usize {
            return Err(OnyxError::Compress(format!(
                "payload too large: {} > {}",
                payload.len(),
                self.block_size
            )));
        }
        self.write_blocks(pba, payload)
    }

    /// Read raw payload from LV3 at the given PBA slot.
    /// Returns exactly `size` bytes (must be <= BLOCK_SIZE).
    pub fn read_block(&self, pba: Pba, size: usize) -> OnyxResult<Vec<u8>> {
        if size > self.block_size as usize {
            return Err(OnyxError::Compress(format!(
                "requested read size {} > block_size {}",
                size, self.block_size
            )));
        }
        self.read_blocks(pba, size)
    }

    /// Write payload spanning multiple contiguous 4KB slots starting at `pba`.
    /// Last slot is zero-padded to BLOCK_SIZE.
    pub fn write_blocks(&self, pba: Pba, payload: &[u8]) -> OnyxResult<()> {
        if payload.is_empty() {
            return Ok(());
        }
        let bs = self.block_size as usize;
        let total_size = ((payload.len() + bs - 1) / bs) * bs; // round up
        let offset = self.pba_to_offset(pba);

        let mut buf = AlignedBuf::new(total_size, self.use_hugepages)?;
        let slice = buf.as_mut_slice();
        slice[..payload.len()].copy_from_slice(payload);
        // Padding is already zero

        match &self.backend {
            IoBackend::Syscall => {
                self.device.write_at(buf.as_slice(), offset)?;
            }
            IoBackend::Uring(session) => {
                let (fd, base) = self.uring_fd_base()?;
                let op = UringOp::Write {
                    fd,
                    ptr: buf.as_ptr(),
                    len: total_size as u32,
                    offset: base + offset,
                };
                let results = unsafe { session.submit_batch(std::slice::from_ref(&op))? };
                self.validate_uring_result("write", offset, total_size as u32, &results[0])?;
            }
        }
        self.verify_write_payload(pba, payload, total_size)?;
        self.record_lv3_write_trace(pba, payload, total_size);
        self.record_lv3_write(total_size);
        Ok(())
    }

    /// Read `size` bytes spanning multiple contiguous 4KB slots starting at `pba`.
    pub fn read_blocks(&self, pba: Pba, size: usize) -> OnyxResult<Vec<u8>> {
        if size == 0 {
            return Ok(Vec::new());
        }
        let bs = self.block_size as usize;
        let read_size = ((size + bs - 1) / bs) * bs; // round up to block boundary
        let offset = self.pba_to_offset(pba);

        let mut buf = AlignedBuf::new(read_size, self.use_hugepages)?;
        match &self.backend {
            IoBackend::Syscall => {
                self.device.read_at(buf.as_mut_slice(), offset)?;
            }
            IoBackend::Uring(session) => {
                let (fd, base) = self.uring_fd_base()?;
                let op = UringOp::Read {
                    fd,
                    ptr: buf.as_mut_ptr(),
                    len: read_size as u32,
                    offset: base + offset,
                };
                let results = unsafe { session.submit_batch(std::slice::from_ref(&op))? };
                self.validate_uring_result("read", offset, read_size as u32, &results[0])?;
            }
        }
        self.record_lv3_read(read_size);

        Ok(buf.as_slice()[..size].to_vec())
    }

    /// Submit a batch of LV3 operations. With the io_uring backend, all SQEs
    /// are pushed and the call waits for every CQE before returning. With the
    /// syscall backend, ops are executed sequentially.
    ///
    /// `fsync_after` appends a barrier-fdatasync SQE (only meaningful for the
    /// uring backend; the syscall path always issues a `sync()` if requested).
    pub fn submit_batch(
        &self,
        ops: Vec<LvOp<'_>>,
        fsync_after: bool,
    ) -> OnyxResult<Vec<LvOpResult>> {
        match &self.backend {
            IoBackend::Syscall => self.submit_batch_syscall(ops, fsync_after),
            IoBackend::Uring(session) => self.submit_batch_uring(session, ops, fsync_after),
        }
    }

    /// Like [`submit_batch`], but when `session` is `Some` (a writer's own
    /// per-shard ring from [`new_write_session`]) and the backend is `Uring`,
    /// submits the batch on that dedicated ring instead of the single shared
    /// `backend` ring. This removes the engine-wide serialization where all
    /// flusher writers contend on one `Mutex<IoUring>` held across
    /// `submit_and_wait`. Falls back to [`submit_batch`] when `session` is `None`
    /// (syscall backend / single-ring A/B baseline / non-writer callers).
    pub fn submit_batch_on(
        &self,
        session: Option<&Arc<IoUringSession>>,
        ops: Vec<LvOp<'_>>,
        fsync_after: bool,
    ) -> OnyxResult<Vec<LvOpResult>> {
        match (&self.backend, session) {
            (IoBackend::Uring(_), Some(s)) => self.submit_batch_uring(s, ops, fsync_after),
            _ => self.submit_batch(ops, fsync_after),
        }
    }

    pub(crate) fn allocate_owned_write_buffer(&self, size: usize) -> OnyxResult<AlignedBuf> {
        AlignedBuf::new(size, self.use_hugepages)
    }

    /// Submit buffers the caller already assembled in O_DIRECT-aligned memory.
    /// Chunklet transfers ownership through the global combiner and returns the
    /// buffers to this thread after completion, avoiding a second payload copy
    /// and preserving the writer thread's local allocation pool. Single-device
    /// backends retain the existing per-session submission path.
    pub(crate) fn submit_owned_write_batch_on(
        &self,
        session: Option<&Arc<IoUringSession>>,
        writes: Vec<OwnedLvWrite>,
        fsync_after: bool,
    ) -> OnyxResult<Vec<LvOpResult>> {
        if writes.is_empty() {
            return Ok(Vec::new());
        }
        if self.chunklet_write_batcher.is_none() {
            let ops = writes
                .iter()
                .map(|write| LvOp::Write {
                    pba: write.pba,
                    payload: &write.buffer.as_slice()[..write.payload_len],
                })
                .collect();
            return self.submit_batch_on(session, ops, fsync_after);
        }

        let write_count = writes.len();
        let mut meta_chunks = Vec::new();
        let mut request_chunks = Vec::new();
        let mut metas = Vec::new();
        let mut slabs = Vec::new();
        let mut ops = Vec::new();
        let mut chunk_bytes = 0usize;
        let mut total_bytes = 0usize;
        for write in writes {
            if write.payload_len == 0 || write.payload_len > write.buffer.len() {
                return Err(OnyxError::Config(format!(
                    "invalid owned LV3 payload length {} for {}-byte buffer",
                    write.payload_len,
                    write.buffer.len()
                )));
            }
            let total = write.buffer.len();
            let device_offset = self.pba_to_offset(write.pba);
            total_bytes = total_bytes.checked_add(total).ok_or_else(|| {
                OnyxError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "chunklet owned write batch is too large",
                ))
            })?;
            if !slabs.is_empty() && chunk_bytes.saturating_add(total) > CHUNKLET_BATCH_TARGET_BYTES
            {
                meta_chunks.push(std::mem::take(&mut metas));
                request_chunks.push((std::mem::take(&mut slabs), std::mem::take(&mut ops)));
                chunk_bytes = 0;
            }
            let slab_index = slabs.len();
            metas.push((write.pba, write.payload_len, total));
            ops.push(OwnedBatchOp {
                device_offset,
                slab_index,
                slab_offset: 0,
                len: total,
            });
            slabs.push(write.buffer);
            chunk_bytes += total;
        }
        if !slabs.is_empty() {
            meta_chunks.push(metas);
            request_chunks.push((slabs, ops));
        }

        if let Some(metrics) = &self.metrics {
            metrics
                .lv3_write_slab_allocs
                .fetch_add(write_count as u64, Ordering::Relaxed);
            metrics
                .lv3_write_slab_bytes
                .fetch_add(total_bytes as u64, Ordering::Relaxed);
        }
        let returned_chunks = self
            .chunklet_write_batcher
            .as_ref()
            .expect("checked above")
            .submit_many(request_chunks, self.metrics.as_ref())?;

        for (metas, returned) in meta_chunks.iter().zip(returned_chunks.iter()) {
            for ((pba, payload_len, total), slab) in metas.iter().zip(returned.iter()) {
                let payload = &slab.as_slice()[..*payload_len];
                self.verify_write_payload(*pba, payload, *total)?;
                self.record_lv3_write_trace(*pba, payload, *total);
                self.record_lv3_write(*total);
            }
        }
        if fsync_after {
            self.device.flush()?;
        }
        Ok((0..write_count)
            .map(|_| LvOpResult::Write(Ok(())))
            .collect())
    }

    fn submit_batch_syscall(
        &self,
        ops: Vec<LvOp<'_>>,
        fsync_after: bool,
    ) -> OnyxResult<Vec<LvOpResult>> {
        // Mixed read/write batches stay sequential — uncommon enough that
        // parallelising is not worth the complexity.
        let all_writes = !ops.is_empty() && ops.iter().all(|op| matches!(op, LvOp::Write { .. }));

        // A striped backend (chunklet LD: no single fd → `uring_target` is None)
        // owns its own cross-PD io_uring batching, so the whole write batch must
        // go through ONE `write_many_at`. The thread-per-op `thread::scope`
        // fan-out below is a single-fd-RawDevice optimisation (keep NVMe QD > 1
        // with plain pwrite) — running it against chunklet spawns an unbounded
        // thread storm that each pins a chunklet thread-local ring (the nvme-box
        // EMFILE + flusher-writer thread explosion). Route chunklet here instead.
        if all_writes && self.device.uring_target().is_none() {
            let out = self.write_batch_many(&ops)?;
            if fsync_after {
                self.device.flush()?;
            }
            return Ok(out);
        }

        let parallelize = all_writes && ops.len() > 1 && !Self::syscall_write_serial_enabled();

        let out: Vec<LvOpResult> = if parallelize {
            // Parallel pwrite via scoped threads to keep NVMe queue depth > 1
            // on the single-fd syscall backend. Mirrors the pre-io_uring
            // passthrough path. (Chunklet took the batched path above.)
            std::thread::scope(|s| {
                let handles: Vec<_> = ops
                    .iter()
                    .map(|op| match op {
                        LvOp::Write { pba, payload } => {
                            let pba = *pba;
                            let payload = *payload;
                            s.spawn(move || self.write_blocks(pba, payload))
                        }
                        _ => unreachable!("filtered by all_writes check"),
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| match h.join() {
                        Ok(r) => LvOpResult::Write(r),
                        Err(_) => LvOpResult::Write(Err(OnyxError::Io(std::io::Error::other(
                            "IO worker thread panicked",
                        )))),
                    })
                    .collect()
            })
        } else {
            let mut out = Vec::with_capacity(ops.len());
            for op in ops {
                match op {
                    LvOp::Read { pba, size } => {
                        out.push(LvOpResult::Read(self.read_blocks(pba, size)))
                    }
                    LvOp::Write { pba, payload } => {
                        out.push(LvOpResult::Write(self.write_blocks(pba, payload)))
                    }
                }
            }
            out
        };

        if fsync_after {
            self.device.flush()?;
        }
        Ok(out)
    }

    /// Encode every write op into one pooled `AlignedBuf` slab and submit the whole
    /// batch through `BlockBackend::write_many_at` in one call. Used for striped
    /// (chunklet) backends, which fan the batch across their member PDs in a
    /// single internal io_uring submit — no per-op thread spawn, no per-thread
    /// ring. Durability is the caller's `flush` (mirrors `write_blocks`).
    fn write_batch_many(&self, ops: &[LvOp<'_>]) -> OnyxResult<Vec<LvOpResult>> {
        let bs = self.block_size as usize;
        // (pba, payload, device offset, slab offset, padded size), kept for
        // write-op construction plus post-write verify/metrics.
        let mut metas: Vec<(Pba, &[u8], u64, usize, usize)> = Vec::with_capacity(ops.len());
        let mut total_bytes = 0usize;
        for op in ops {
            let (pba, payload) = match op {
                LvOp::Write { pba, payload } => (*pba, *payload),
                _ => unreachable!("write_batch_many is writes-only"),
            };
            if payload.is_empty() {
                continue;
            }
            let total = payload.len().div_ceil(bs) * bs;
            let offset = self.pba_to_offset(pba);
            let slab_offset = total_bytes;
            total_bytes = total_bytes.checked_add(total).ok_or_else(|| {
                OnyxError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "chunklet write batch is too large",
                ))
            })?;
            metas.push((pba, payload, offset, slab_offset, total));
        }

        if total_bytes > 0 {
            let mut slab = AlignedBuf::new(total_bytes, self.use_hugepages)?;
            for (_, payload, _, slab_offset, total) in &metas {
                let start = *slab_offset;
                let dst = &mut slab.as_mut_slice()[start..start + *total];
                dst[..payload.len()].copy_from_slice(payload);
                // AlignedBuf is pooled and may contain a prior batch. Clear only
                // this op's O_DIRECT tail rather than memset the whole slab.
                dst[payload.len()..].fill(0);
            }
            if let Some(batcher) = &self.chunklet_write_batcher {
                if let Some(metrics) = &self.metrics {
                    metrics
                        .lv3_write_slab_allocs
                        .fetch_add(1, Ordering::Relaxed);
                    metrics
                        .lv3_write_slab_bytes
                        .fetch_add(total_bytes as u64, Ordering::Relaxed);
                }
                let owned_ops = metas
                    .iter()
                    .map(|(_, _, offset, slab_offset, total)| OwnedBatchOp {
                        device_offset: *offset,
                        slab_index: 0,
                        slab_offset: *slab_offset,
                        len: *total,
                    })
                    .collect();
                let returned = batcher.submit(vec![slab], owned_ops)?;
                debug_assert_eq!(returned.len(), 1);
                drop(returned);
            } else {
                let write_ops: Vec<(u64, &[u8])> = metas
                    .iter()
                    .map(|(_, _, offset, slab_offset, total)| {
                        let start = *slab_offset;
                        (*offset, &slab.as_slice()[start..start + *total])
                    })
                    .collect();
                self.record_lv3_write_batch(metas.len(), total_bytes, true);
                self.device.write_many_at(&write_ops)?;
            }
        }

        // Post-write verify + LV3 metrics, per op (matches write_blocks).
        for (pba, payload, _offset, _slab_offset, total) in &metas {
            self.verify_write_payload(*pba, payload, *total)?;
            self.record_lv3_write_trace(*pba, payload, *total);
            self.record_lv3_write(*total);
        }
        // write_many_at is all-or-first-error; a returned Ok means every op landed.
        Ok(ops.iter().map(|_| LvOpResult::Write(Ok(()))).collect())
    }

    fn submit_batch_uring(
        &self,
        session: &Arc<IoUringSession>,
        ops: Vec<LvOp<'_>>,
        fsync_after: bool,
    ) -> OnyxResult<Vec<LvOpResult>> {
        if ops.is_empty() && !fsync_after {
            return Ok(Vec::new());
        }
        let all_writes = !ops.is_empty() && ops.iter().all(|op| matches!(op, LvOp::Write { .. }));
        if all_writes {
            return self.submit_write_batch_uring(session, ops, fsync_after);
        }

        let bs = self.block_size as usize;
        let (fd, base) = self.uring_fd_base()?;

        // Allocate AlignedBuf for each op upfront, holding them through submit.
        // Read ops keep their target buffer here; write ops copy their payload
        // into a fresh AlignedBuf so the SQE's pointer remains valid and aligned.
        let mut owned_bufs: Vec<AlignedBuf> = Vec::with_capacity(ops.len());
        let mut metas: Vec<(usize, u32, u64, bool)> = Vec::with_capacity(ops.len()); // (op_idx, expected_bytes, offset, is_read)
        let mut uring_ops: Vec<UringOp> = Vec::with_capacity(ops.len() + 1);

        for (i, op) in ops.iter().enumerate() {
            match op {
                LvOp::Read { pba, size } => {
                    if *size == 0 {
                        // Defer: empty read produces an empty Vec; no SQE needed.
                        // Track a sentinel; we'll fill in results after submit.
                        metas.push((i, 0, 0, true));
                        continue;
                    }
                    let read_size = ((*size + bs - 1) / bs) * bs;
                    let offset = self.pba_to_offset(*pba);
                    let mut buf = AlignedBuf::new(read_size, self.use_hugepages)?;
                    let ptr = buf.as_mut_ptr();
                    owned_bufs.push(buf);
                    uring_ops.push(UringOp::Read {
                        fd,
                        ptr,
                        len: read_size as u32,
                        offset: base + offset,
                    });
                    metas.push((i, read_size as u32, offset, true));
                }
                LvOp::Write { pba, payload } => {
                    if payload.is_empty() {
                        metas.push((i, 0, 0, false));
                        continue;
                    }
                    let total = ((payload.len() + bs - 1) / bs) * bs;
                    let offset = self.pba_to_offset(*pba);
                    let mut buf = AlignedBuf::new(total, self.use_hugepages)?;
                    buf.as_mut_slice()[..payload.len()].copy_from_slice(payload);
                    let ptr = buf.as_ptr();
                    owned_bufs.push(buf);
                    uring_ops.push(UringOp::Write {
                        fd,
                        ptr,
                        len: total as u32,
                        offset: base + offset,
                    });
                    metas.push((i, total as u32, offset, false));
                }
            }
        }

        let data_op_count = uring_ops.len();
        if fsync_after {
            uring_ops.push(UringOp::FsyncDataBarrier { fd });
        }

        let mut results = Vec::with_capacity(uring_ops.len());
        if data_op_count > 0 {
            let max_ops = (session.sq_entries() as usize).max(1);
            for chunk in uring_ops[..data_op_count].chunks(max_ops) {
                results.extend(unsafe { session.submit_batch(chunk)? });
            }
        }
        if fsync_after {
            // The data chunks above are fully completed before this fdatasync is
            // submitted, so a single-op barrier preserves submit_batch's
            // "writes then sync" contract even when the write batch is larger
            // than the ring's SQ depth.
            results.extend(unsafe { session.submit_batch(&uring_ops[data_op_count..])? });
        }

        let fsync_offset = if fsync_after {
            results.len() - 1
        } else {
            usize::MAX
        };

        // Walk back over the input ops, pulling owned_bufs / results in submit order.
        let mut out: Vec<Option<LvOpResult>> = (0..ops.len()).map(|_| None).collect();
        let mut buf_iter = owned_bufs.into_iter();
        let mut result_idx = 0usize;

        for (op_idx, expected, offset, is_read) in metas {
            let op = &ops[op_idx];
            match op {
                LvOp::Read { size, .. } => {
                    if *size == 0 {
                        out[op_idx] = Some(LvOpResult::Read(Ok(Vec::new())));
                        continue;
                    }
                    debug_assert!(is_read);
                    let buf = buf_iter.next().expect("owned_bufs / metas mismatch");
                    let r = &results[result_idx];
                    result_idx += 1;
                    let slot = match self.validate_uring_result("read", offset, expected, r) {
                        Ok(()) => {
                            self.record_lv3_read(expected as usize);
                            Ok(buf.as_slice()[..*size].to_vec())
                        }
                        Err(e) => Err(e),
                    };
                    out[op_idx] = Some(LvOpResult::Read(slot));
                }
                LvOp::Write { payload, .. } => {
                    if payload.is_empty() {
                        out[op_idx] = Some(LvOpResult::Write(Ok(())));
                        continue;
                    }
                    debug_assert!(!is_read);
                    let _buf = buf_iter.next().expect("owned_bufs / metas mismatch");
                    let r = &results[result_idx];
                    result_idx += 1;
                    let slot = match self.validate_uring_result("write", offset, expected, r) {
                        Ok(()) => {
                            self.record_lv3_write(expected as usize);
                            Ok(())
                        }
                        Err(e) => Err(e),
                    };
                    out[op_idx] = Some(LvOpResult::Write(slot));
                }
            }
        }

        if fsync_after {
            let r = &results[fsync_offset];
            if let Some(errno) = r.errno() {
                return Err(OnyxError::Device {
                    path: PathBuf::from(self.device.label()),
                    reason: format!(
                        "io_uring fdatasync failed: errno={errno} ({})",
                        std::io::Error::from_raw_os_error(errno)
                    ),
                });
            }
        }

        Ok(out
            .into_iter()
            .map(|s| s.expect("all slots filled"))
            .collect())
    }

    fn submit_write_batch_uring(
        &self,
        session: &Arc<IoUringSession>,
        ops: Vec<LvOp<'_>>,
        fsync_after: bool,
    ) -> OnyxResult<Vec<LvOpResult>> {
        let bs = self.block_size as usize;
        let (fd, base) = self.uring_fd_base()?;
        let mut metas: Vec<(usize, u32, u64)> = Vec::with_capacity(ops.len());
        let mut total_bytes = 0usize;

        for (idx, op) in ops.iter().enumerate() {
            let LvOp::Write { pba, payload } = op else {
                unreachable!("submit_write_batch_uring only accepts writes");
            };
            if payload.is_empty() {
                continue;
            }
            let total = ((payload.len() + bs - 1) / bs) * bs;
            total_bytes = total_bytes.checked_add(total).ok_or_else(|| {
                OnyxError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "io_uring write batch is too large",
                ))
            })?;
            metas.push((idx, total as u32, self.pba_to_offset(*pba)));
        }

        let mut slab = if total_bytes > 0 {
            Some(AlignedBuf::new(total_bytes, self.use_hugepages)?)
        } else {
            None
        };
        self.record_lv3_write_batch(metas.len(), total_bytes, slab.is_some());
        let mut uring_ops: Vec<UringOp> =
            Vec::with_capacity(metas.len() + usize::from(fsync_after));
        let mut cursor = 0usize;

        if let Some(slab) = slab.as_mut() {
            for op in &ops {
                let LvOp::Write { payload, .. } = op else {
                    unreachable!("submit_write_batch_uring only accepts writes");
                };
                if payload.is_empty() {
                    continue;
                }
                let total = ((payload.len() + bs - 1) / bs) * bs;
                slab.as_mut_slice()[cursor..cursor + payload.len()].copy_from_slice(payload);
                uring_ops.push(UringOp::Write {
                    fd,
                    ptr: unsafe { slab.as_ptr().add(cursor) },
                    len: total as u32,
                    offset: base + metas[uring_ops.len()].2,
                });
                cursor += total;
            }
        }

        let data_op_count = uring_ops.len();
        if fsync_after {
            uring_ops.push(UringOp::FsyncDataBarrier { fd });
        }

        let mut results = Vec::with_capacity(uring_ops.len());
        if data_op_count > 0 {
            let max_ops = (session.sq_entries() as usize).max(1);
            for chunk in uring_ops[..data_op_count].chunks(max_ops) {
                results.extend(unsafe { session.submit_batch(chunk)? });
            }
        }
        if fsync_after {
            results.extend(unsafe { session.submit_batch(&uring_ops[data_op_count..])? });
        }

        let mut out: Vec<Option<LvOpResult>> = (0..ops.len()).map(|_| None).collect();
        for (idx, op) in ops.iter().enumerate() {
            if matches!(op, LvOp::Write { payload, .. } if payload.is_empty()) {
                out[idx] = Some(LvOpResult::Write(Ok(())));
            }
        }

        for (result_idx, (op_idx, expected, offset)) in metas.into_iter().enumerate() {
            let r = &results[result_idx];
            let slot = match self.validate_uring_result("write", offset, expected, r) {
                Ok(()) => {
                    self.record_lv3_write(expected as usize);
                    Ok(())
                }
                Err(e) => Err(e),
            };
            out[op_idx] = Some(LvOpResult::Write(slot));
        }

        if fsync_after {
            let r = &results[results.len() - 1];
            if let Some(errno) = r.errno() {
                return Err(OnyxError::Device {
                    path: PathBuf::from(self.device.label()),
                    reason: format!(
                        "io_uring fdatasync failed: errno={errno} ({})",
                        std::io::Error::from_raw_os_error(errno)
                    ),
                });
            }
        }

        drop(slab);
        Ok(out
            .into_iter()
            .map(|s| s.expect("all slots filled"))
            .collect())
    }

    /// Borrow the LV3 device seam. Used by the engine shutdown path to stamp
    /// the `FLAG_CLEAN_SHUTDOWN` bit in the LV3 superblock.
    pub fn device(&self) -> &Arc<dyn BlockBackend> {
        &self.device
    }

    pub fn device_size(&self) -> u64 {
        self.device.size()
    }

    pub fn total_blocks(&self) -> u64 {
        self.device.size() / self.block_size as u64 - self.pba_offset
    }

    pub fn sync(&self) -> OnyxResult<()> {
        match &self.backend {
            IoBackend::Syscall => self.device.flush(),
            IoBackend::Uring(session) => {
                let (fd, _base) = self.uring_fd_base()?;
                let op = UringOp::FsyncData { fd };
                let results = unsafe { session.submit_batch(std::slice::from_ref(&op))? };
                if let Some(errno) = results[0].errno() {
                    return Err(OnyxError::Device {
                        path: PathBuf::from(self.device.label()),
                        reason: format!(
                            "io_uring fdatasync failed: errno={errno} ({})",
                            std::io::Error::from_raw_os_error(errno)
                        ),
                    });
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests;
