//! `ReadPool` — dedicated worker threads that batch LV3 reads through io_uring,
//! decompress in parallel, and reply to callers via oneshot channels.
//!
//! ## Why a separate pool
//!
//! `IoEngine` has a single `IoUringSession` shared by every caller. With high
//! concurrent reads (e.g. ublk callbacks fanned across N queue threads) every
//! caller serialises through one ring mutex, and each call submits a single SQE
//! and immediately waits for its CQE — the io_uring SQ depth is wasted.
//!
//! `ReadPool` fixes both problems:
//!
//! * Each worker owns its own `IoUringSession` (zero mutex contention between
//!   workers).
//! * Workers drain a request channel, push N SQEs into one `submit_batch`
//!   (NVMe sees N concurrent IOs from one syscall), then CRC + decompress on
//!   the worker thread (decompression scales with worker count).
//!
//! Routing: requests go through one shared bounded MPMC queue. Faster workers
//! naturally pull more work, which avoids per-PBA shard hotspots while each
//! worker still owns its own io_uring instance.
//!
//! Buffer hits and unmapped reads are *not* sent through the pool — those
//! paths are zero-IO and stay inline on the caller thread.

use std::os::fd::RawFd;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, Receiver, Sender};

use crate::affinity::{self, ThreadRole};
use crate::error::{OnyxError, OnyxResult};
use crate::io::aligned::AlignedBuf;
use crate::io::block_backend::BlockBackend;
use crate::io::device::RawDevice;
use crate::io::uring::{IoUringSession, UringOp};
use crate::meta::schema::BlockmapValue;
use crate::metrics::EngineMetrics;
use crate::types::BLOCK_SIZE;
use crate::zone::read::{
    decode_unit_with_crc_accounting, extract_lba_from_compressed_with_crc_accounting,
};

/// Maximum requests folded into one `submit_batch` per worker iteration.
const BATCH_MAX: usize = 32;

/// Tiny coalescing window used after the first request wakes a worker.
///
/// 4 KB NVMe reads complete fast enough that "recv one, submit one, wait one"
/// leaves most of the device queue depth unused. Waiting a few microseconds
/// lets sibling ublk queues enqueue adjacent requests so one read-pool worker
/// can submit a real io_uring batch. The bound is intentionally small so light
/// read traffic does not pay a visible latency tax.
const BATCH_COALESCE_WINDOW: Duration = Duration::from_micros(8);

/// Per-worker request channel capacity. With BATCH_MAX=32 and bounded(128) the
/// channel can hold ~4 batches worth of in-flight requests, giving callers
/// some slack while preserving back-pressure under sustained overload.
const REQUEST_CHANNEL_CAP: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadPurpose {
    Foreground,
    DedupVerify,
    DedupVerifyIndex,
    DedupVerifyCandidate,
    DedupScanner,
}

struct ReadRequest {
    mapping: BlockmapValue,
    reply: Sender<OnyxResult<Vec<u8>>>,
    enqueued_at: Instant,
    purpose: ReadPurpose,
    /// `false` → worker returns one 4 KB LBA at `mapping.offset_in_unit`
    /// (legacy single-LBA path). `true` → worker returns the full decoded
    /// unit payload (`unit_original_size` bytes) so the caller can fan out
    /// multiple LBAs from one IO+decompress.
    return_unit: bool,
    raw_extent: Option<Vec<BlockmapValue>>,
}

impl ReadRequest {
    fn start_pba(&self) -> crate::types::Pba {
        self.mapping.pba
    }

    fn read_size(&self, block_size: usize) -> usize {
        if let Some(mappings) = &self.raw_extent {
            mappings.len() * block_size
        } else {
            self.mapping.compressed_read_size(block_size)
        }
    }
}

struct WorkerHandle {
    join: Option<JoinHandle<()>>,
}

pub struct ReadPool {
    sender: Option<Sender<ReadRequest>>,
    workers: Vec<WorkerHandle>,
}

impl ReadPool {
    /// Spawn `workers` reader threads, each with its own io_uring session of
    /// `sq_entries` slots and its own `RawDevice` handle (so each worker has
    /// an independent fd — no shared file state between workers).
    ///
    /// `device` is used as a template: the pool reads its path + base offset,
    /// then each worker opens a fresh handle to the same underlying file.
    /// The input `device` may be dropped after `start` returns.
    pub fn start(
        workers: usize,
        sq_entries: u32,
        device: &RawDevice,
        pba_offset: u64,
        block_size: u32,
        use_hugepages: bool,
        metrics: Arc<EngineMetrics>,
    ) -> OnyxResult<Self> {
        if workers == 0 {
            return Err(OnyxError::Config(
                "ReadPool requires at least 1 worker (set read_pool_workers >= 1)".into(),
            ));
        }
        let device_path = device.path().to_path_buf();
        let base_offset = device.base_offset();
        let channel_cap = workers
            .saturating_mul(REQUEST_CHANNEL_CAP)
            .max(REQUEST_CHANNEL_CAP);
        let (tx, rx) = bounded::<ReadRequest>(channel_cap);

        let mut handles = Vec::with_capacity(workers);
        for worker_idx in 0..workers {
            let rx = rx.clone();
            let session = IoUringSession::new(sq_entries)?;
            let worker_device = RawDevice::open(&device_path)?;
            let metrics = metrics.clone();
            let device_path_clone = device_path.clone();
            let join = thread::Builder::new()
                .name(format!("read-pool-{}", worker_idx))
                .spawn(move || {
                    affinity::bind_current(ThreadRole::ReadPool, worker_idx);
                    let fd = worker_device.as_raw_fd();
                    let ctx = WorkerCtx {
                        worker_idx,
                        io: WorkerIo::Uring {
                            ring: session,
                            fd,
                            base_offset,
                            _device: worker_device,
                        },
                        pba_offset,
                        block_size,
                        use_hugepages,
                        metrics,
                        device_path: device_path_clone,
                    };
                    worker_loop(ctx, rx);
                })
                .map_err(|e| {
                    OnyxError::Config(format!(
                        "failed to spawn read-pool worker {}: {}",
                        worker_idx, e
                    ))
                })?;
            handles.push(WorkerHandle { join: Some(join) });
        }

        tracing::info!(
            workers,
            sq_entries,
            channel_cap,
            "read pool started with shared queue"
        );

        Ok(Self {
            sender: Some(tx),
            workers: handles,
        })
    }

    /// Spawn `workers` reader threads sharing one chunklet `BlockBackend`. There
    /// is no per-worker fd or `io_uring` — chunklet owns the cross-PD io_uring,
    /// so each worker issues its coalesced batch via `read_many_at`. Workers
    /// still run in parallel for CRC + decompress and to keep multiple batches
    /// in flight to the LD.
    pub fn start_backend(
        workers: usize,
        backend: Arc<dyn BlockBackend>,
        pba_offset: u64,
        block_size: u32,
        use_hugepages: bool,
        metrics: Arc<EngineMetrics>,
    ) -> OnyxResult<Self> {
        if workers == 0 {
            return Err(OnyxError::Config(
                "ReadPool requires at least 1 worker (set read_pool_workers >= 1)".into(),
            ));
        }
        let channel_cap = workers
            .saturating_mul(REQUEST_CHANNEL_CAP)
            .max(REQUEST_CHANNEL_CAP);
        let (tx, rx) = bounded::<ReadRequest>(channel_cap);

        let mut handles = Vec::with_capacity(workers);
        for worker_idx in 0..workers {
            let rx = rx.clone();
            let metrics = metrics.clone();
            let backend = backend.clone();
            let join = thread::Builder::new()
                .name(format!("read-pool-{}", worker_idx))
                .spawn(move || {
                    affinity::bind_current(ThreadRole::ReadPool, worker_idx);
                    let ctx = WorkerCtx {
                        worker_idx,
                        io: WorkerIo::Backend { backend },
                        pba_offset,
                        block_size,
                        use_hugepages,
                        metrics,
                        device_path: std::path::PathBuf::from("chunklet"),
                    };
                    worker_loop(ctx, rx);
                })
                .map_err(|e| {
                    OnyxError::Config(format!(
                        "failed to spawn read-pool worker {}: {}",
                        worker_idx, e
                    ))
                })?;
            handles.push(WorkerHandle { join: Some(join) });
        }

        tracing::info!(workers, channel_cap, "read pool started (chunklet backend)");

        Ok(Self {
            sender: Some(tx),
            workers: handles,
        })
    }

    /// Submit a mapped LV3 read and block until the worker has read + CRC
    /// verified + decompressed the requested 4 KB LBA.
    pub fn submit_read(&self, mapping: BlockmapValue) -> OnyxResult<Vec<u8>> {
        let rx = self.enqueue(mapping, false, ReadPurpose::Foreground)?;
        rx.recv()
            .map_err(|_| OnyxError::Io(std::io::Error::other("read-pool reply dropped")))?
    }

    /// Non-blocking send of a single-LBA read. Returns the reply receiver so
    /// the caller can fan multiple requests out across workers before draining
    /// them, avoiding per-request round-trip serialization.
    pub fn submit_read_async(
        &self,
        mapping: BlockmapValue,
    ) -> OnyxResult<Receiver<OnyxResult<Vec<u8>>>> {
        self.enqueue(mapping, false, ReadPurpose::Foreground)
    }

    pub fn submit_read_async_for(
        &self,
        mapping: BlockmapValue,
        purpose: ReadPurpose,
    ) -> OnyxResult<Receiver<OnyxResult<Vec<u8>>>> {
        self.enqueue(mapping, false, purpose)
    }

    /// Submit a compression-unit read and block for the full decoded unit
    /// payload (`unit_original_size` bytes). Callers slice out multiple LBAs
    /// from the returned buffer via `offset_in_unit`.
    pub fn submit_unit_read(&self, mapping: BlockmapValue) -> OnyxResult<Vec<u8>> {
        let rx = self.enqueue(mapping, true, ReadPurpose::Foreground)?;
        rx.recv()
            .map_err(|_| OnyxError::Io(std::io::Error::other("read-pool reply dropped")))?
    }

    /// Non-blocking companion to `submit_unit_read`.
    pub fn submit_unit_read_async(
        &self,
        mapping: BlockmapValue,
    ) -> OnyxResult<Receiver<OnyxResult<Vec<u8>>>> {
        self.enqueue(mapping, true, ReadPurpose::Foreground)
    }

    /// Submit a contiguous raw extent read. Every mapping must be one
    /// uncompressed 4 KiB block and PBAs must be consecutive. The worker reads
    /// the whole span with one SQE, verifies each block's CRC, and returns the
    /// raw bytes.
    pub fn submit_raw_extent_read_async(
        &self,
        mappings: Vec<BlockmapValue>,
    ) -> OnyxResult<Receiver<OnyxResult<Vec<u8>>>> {
        let first = mappings.first().copied().ok_or_else(|| {
            OnyxError::Config("ReadPool raw extent requires at least one mapping".into())
        })?;
        validate_raw_extent(&mappings)?;
        self.enqueue_raw_extent(first, mappings)
    }

    fn enqueue(
        &self,
        mapping: BlockmapValue,
        return_unit: bool,
        purpose: ReadPurpose,
    ) -> OnyxResult<Receiver<OnyxResult<Vec<u8>>>> {
        let (reply_tx, reply_rx) = bounded::<OnyxResult<Vec<u8>>>(1);
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| OnyxError::Io(std::io::Error::other("read-pool already shut down")))?;
        sender
            .send(ReadRequest {
                mapping,
                reply: reply_tx,
                enqueued_at: Instant::now(),
                purpose,
                return_unit,
                raw_extent: None,
            })
            .map_err(|_| OnyxError::Io(std::io::Error::other("read-pool worker channel closed")))?;
        Ok(reply_rx)
    }

    fn enqueue_raw_extent(
        &self,
        first: BlockmapValue,
        mappings: Vec<BlockmapValue>,
    ) -> OnyxResult<Receiver<OnyxResult<Vec<u8>>>> {
        let (reply_tx, reply_rx) = bounded::<OnyxResult<Vec<u8>>>(1);
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| OnyxError::Io(std::io::Error::other("read-pool already shut down")))?;
        sender
            .send(ReadRequest {
                mapping: first,
                reply: reply_tx,
                enqueued_at: Instant::now(),
                purpose: ReadPurpose::Foreground,
                return_unit: true,
                raw_extent: Some(mappings),
            })
            .map_err(|_| OnyxError::Io(std::io::Error::other("read-pool worker channel closed")))?;
        Ok(reply_rx)
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Drop every sender so worker threads observe a closed channel and exit,
    /// then join. Idempotent — safe to call from `Drop`.
    pub fn shutdown(&mut self) {
        self.sender.take();
        for w in &mut self.workers {
            if let Some(join) = w.join.take() {
                let _ = join.join();
            }
        }
    }
}

impl Drop for ReadPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// How a read-pool worker issues IO. `Uring` is the single-fd file/blockdev
/// path (per-worker `io_uring` + fd). `Backend` is a chunklet LD shared across
/// workers — chunklet owns the cross-PD io_uring, so the worker just calls the
/// synchronous batched `read_many_at`.
enum WorkerIo {
    Uring {
        ring: IoUringSession,
        fd: RawFd,
        base_offset: u64,
        /// Keeps the `RawDevice` alive as long as the worker runs; `fd` is only
        /// valid while this field exists.
        _device: RawDevice,
    },
    Backend {
        backend: Arc<dyn BlockBackend>,
    },
}

struct WorkerCtx {
    worker_idx: usize,
    io: WorkerIo,
    pba_offset: u64,
    block_size: u32,
    use_hugepages: bool,
    metrics: Arc<EngineMetrics>,
    /// Label for error messages (device path, or "chunklet" for a backend).
    device_path: std::path::PathBuf,
}

impl WorkerCtx {
    /// Device byte offset added before the PBA mapping. A chunklet LD owns the
    /// whole linear space from 0, so its base is 0.
    fn base_offset(&self) -> u64 {
        match &self.io {
            WorkerIo::Uring { base_offset, .. } => *base_offset,
            WorkerIo::Backend { .. } => 0,
        }
    }
}

/// Per-batch scratch state — kept on the worker stack across iterations and
/// `clear()`-ed at the top of each batch so we don't pay allocator traffic for
/// five fresh `Vec`s per loop turn.
#[derive(Default)]
struct BatchScratch {
    bufs: Vec<ReadBuffer>,
    ops: Vec<UringOp>,
    requests: Vec<ReadRequest>,
    expected: Vec<u32>,
    offsets: Vec<u64>,
}

impl BatchScratch {
    fn with_capacity(cap: usize) -> Self {
        Self {
            bufs: Vec::with_capacity(cap),
            ops: Vec::with_capacity(cap),
            requests: Vec::with_capacity(cap),
            expected: Vec::with_capacity(cap),
            offsets: Vec::with_capacity(cap),
        }
    }

    fn clear(&mut self) {
        self.ops.clear();
        self.requests.clear();
        self.expected.clear();
        self.offsets.clear();
    }

    fn prepare_buffer(
        &mut self,
        slot: usize,
        read_size: usize,
        use_hugepages: bool,
    ) -> OnyxResult<&mut AlignedBuf> {
        if slot == self.bufs.len() {
            self.bufs.push(ReadBuffer {
                buf: AlignedBuf::new(read_size, use_hugepages)?,
            });
        } else if self.bufs[slot].buf.len() < read_size {
            self.bufs[slot].buf = AlignedBuf::new(read_size, use_hugepages)?;
        }
        Ok(&mut self.bufs[slot].buf)
    }
}

struct ReadBuffer {
    buf: AlignedBuf,
}

fn worker_loop(ctx: WorkerCtx, rx: Receiver<ReadRequest>) {
    let mut scratch = BatchScratch::with_capacity(BATCH_MAX);
    let mut batch: Vec<ReadRequest> = Vec::with_capacity(BATCH_MAX);
    loop {
        let first = match rx.recv() {
            Ok(req) => {
                record_queue_wait(&ctx.metrics, ctx.worker_idx, &req);
                req
            }
            Err(_) => return,
        };
        batch.clear();
        batch.push(first);
        let coalesce_start = Instant::now();
        let deadline = Instant::now() + BATCH_COALESCE_WINDOW;
        loop {
            while batch.len() < BATCH_MAX {
                match rx.try_recv() {
                    Ok(req) => {
                        record_queue_wait(&ctx.metrics, ctx.worker_idx, &req);
                        batch.push(req);
                    }
                    Err(_) => break,
                }
            }
            if batch.len() >= BATCH_MAX {
                break;
            }
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            match rx.recv_timeout(deadline.saturating_duration_since(now)) {
                Ok(req) => {
                    record_queue_wait(&ctx.metrics, ctx.worker_idx, &req);
                    batch.push(req);
                }
                Err(_) => break,
            }
        }
        ctx.metrics
            .read_pool_coalesce_wait_ns
            .fetch_add(elapsed_ns(coalesce_start), Ordering::Relaxed);
        process_batch(&ctx, &mut scratch, &mut batch);
    }
}

fn process_batch(ctx: &WorkerCtx, scratch: &mut BatchScratch, batch: &mut Vec<ReadRequest>) {
    let bs = ctx.block_size as usize;
    scratch.clear();
    let request_count = batch.len() as u64;
    ctx.metrics
        .read_pool_requests
        .fetch_add(request_count, Ordering::Relaxed);
    ctx.metrics
        .read_pool_batches
        .fetch_add(1, Ordering::Relaxed);
    ctx.metrics
        .read_pool_batch_ops
        .fetch_add(request_count, Ordering::Relaxed);
    ctx.metrics
        .record_read_pool_worker_batch(ctx.worker_idx, request_count);

    for req in batch.drain(..) {
        let read_size = req.read_size(bs);
        if read_size == 0 {
            let _ = req.reply.send(Err(OnyxError::Compress(
                "ReadPool: zero-length compressed unit".into(),
            )));
            continue;
        }
        let pba = req.start_pba();
        let offset = ctx.base_offset() + (pba.0 + ctx.pba_offset) * ctx.block_size as u64;

        let slot = scratch.requests.len();
        let alloc_start = Instant::now();
        if let Err(e) = scratch.prepare_buffer(slot, read_size, ctx.use_hugepages) {
            let _ = req.reply.send(Err(e));
            continue;
        }
        ctx.metrics
            .read_pool_alloc_ns
            .fetch_add(elapsed_ns(alloc_start), Ordering::Relaxed);
        scratch.expected.push(read_size as u32);
        scratch.offsets.push(offset);
        scratch.requests.push(req);
    }

    if scratch.requests.is_empty() {
        return;
    }

    // The SQE / read_many_at op is built per IO mode so no fd is baked in here.
    match &ctx.io {
        WorkerIo::Uring { ring, fd, .. } => process_uring_submit(ctx, scratch, ring, *fd),
        WorkerIo::Backend { backend } => process_backend_submit(ctx, scratch, backend),
    }
}

/// io_uring submission + harvest for the file/blockdev path: one SQE per
/// prepared buffer, a single `io_uring_enter` for the batch, then decode each
/// completed read.
fn process_uring_submit(
    ctx: &WorkerCtx,
    scratch: &mut BatchScratch,
    ring: &IoUringSession,
    fd: RawFd,
) {
    scratch.ops.clear();
    for i in 0..scratch.requests.len() {
        let ptr = scratch.bufs[i].buf.as_mut_ptr();
        scratch.ops.push(UringOp::Read {
            fd,
            ptr,
            len: scratch.expected[i],
            offset: scratch.offsets[i],
        });
    }

    let submit_start = Instant::now();
    let cqes = match unsafe { ring.submit_batch(&scratch.ops) } {
        Ok(c) => c,
        Err(e) => {
            ctx.metrics
                .record_read_pool_submit_wait_ns(ctx.worker_idx, elapsed_ns(submit_start));
            for req in &scratch.requests {
                let _ = req
                    .reply
                    .send(Err(OnyxError::Io(std::io::Error::other(format!(
                        "read-pool submit_batch failed: {e}"
                    )))));
            }
            return;
        }
    };
    ctx.metrics
        .record_read_pool_submit_wait_ns(ctx.worker_idx, elapsed_ns(submit_start));

    for i in 0..scratch.requests.len() {
        let exp_bytes = scratch.expected[i];
        let offset = scratch.offsets[i];
        let cqe = &cqes[i];
        if let Some(errno) = cqe.errno() {
            let _ = scratch.requests[i].reply.send(Err(OnyxError::Device {
                path: ctx.device_path.clone(),
                reason: format!("io_uring read failed at offset={offset}: errno={errno}"),
            }));
            continue;
        }
        let bytes = cqe.bytes().unwrap_or(0);
        if bytes != exp_bytes {
            let _ = scratch.requests[i].reply.send(Err(OnyxError::Device {
                path: ctx.device_path.clone(),
                reason: format!(
                    "io_uring short read at offset={offset}: got {bytes} of {exp_bytes}"
                ),
            }));
            continue;
        }
        finish_request(
            ctx,
            &scratch.requests[i],
            scratch.bufs[i].buf.as_slice(),
            exp_bytes,
        );
    }
}

/// Synchronous batched read for the chunklet path: hand the whole coalesced
/// batch to `read_many_at` (chunklet fans it across PDs in one submit), then
/// decode each buffer. A batch-level error fails every request in the batch.
fn process_backend_submit(
    ctx: &WorkerCtx,
    scratch: &mut BatchScratch,
    backend: &Arc<dyn BlockBackend>,
) {
    let n = scratch.requests.len();
    let offsets: Vec<u64> = scratch.offsets[..n].to_vec();
    let expected: Vec<u32> = scratch.expected[..n].to_vec();

    let submit_start = Instant::now();
    let res = {
        let mut ops: Vec<(u64, &mut [u8])> = scratch.bufs[..n]
            .iter_mut()
            .enumerate()
            .map(|(i, rb)| (offsets[i], &mut rb.buf.as_mut_slice()[..expected[i] as usize]))
            .collect();
        backend.read_many_at(&mut ops)
    };
    ctx.metrics
        .record_read_pool_submit_wait_ns(ctx.worker_idx, elapsed_ns(submit_start));

    if let Err(e) = res {
        for req in &scratch.requests {
            let _ = req.reply.send(Err(OnyxError::Device {
                path: ctx.device_path.clone(),
                reason: format!("chunklet read_many_at failed: {e}"),
            }));
        }
        return;
    }

    for i in 0..n {
        finish_request(
            ctx,
            &scratch.requests[i],
            scratch.bufs[i].buf.as_slice(),
            expected[i],
        );
    }
}

/// Shared CRC + decompress + reply for one successfully-read buffer. Identical
/// for the io_uring and chunklet paths once the bytes are in `buf`.
fn finish_request(ctx: &WorkerCtx, req: &ReadRequest, buf: &[u8], exp_bytes: u32) {
    ctx.metrics.lv3_read_ops.fetch_add(1, Ordering::Relaxed);
    ctx.metrics
        .lv3_read_compressed_bytes
        .fetch_add(exp_bytes as u64, Ordering::Relaxed);

    // Two output modes — both share the same CRC + decompress path:
    //   return_unit=false: slice out a single 4 KB LBA (legacy).
    //   return_unit=true:  hand back the full decoded unit so the caller can
    //                      fan out multiple LBAs from one IO.
    let decode_start = Instant::now();
    let count_crc_error = counts_as_crc_error(req.purpose);
    let result = if let Some(mappings) = req.raw_extent.as_ref() {
        decode_raw_extent(buf, mappings, &ctx.metrics)
    } else if req.return_unit {
        decode_unit_with_crc_accounting(buf, &req.mapping, &ctx.metrics, count_crc_error)
            .map(|payload| payload.into_owned())
    } else {
        extract_lba_from_compressed_with_crc_accounting(
            buf,
            &req.mapping,
            &ctx.metrics,
            count_crc_error,
        )
    };
    if let Err(OnyxError::CrcMismatch { expected, actual }) = &result {
        record_purpose_mismatch(&ctx.metrics, req.purpose);
        if matches!(
            req.purpose,
            ReadPurpose::DedupVerify
                | ReadPurpose::DedupVerifyIndex
                | ReadPurpose::DedupVerifyCandidate
        ) {
            tracing::debug!(
                worker = ctx.worker_idx,
                purpose = ?req.purpose,
                pba = req.mapping.pba.0,
                slot_offset = req.mapping.slot_offset,
                unit_compressed_size = req.mapping.unit_compressed_size,
                unit_original_size = req.mapping.unit_original_size,
                unit_lba_count = req.mapping.unit_lba_count,
                offset_in_unit = req.mapping.offset_in_unit,
                expected_crc = *expected,
                actual_crc = *actual,
                raw_extent_blocks = req.raw_extent.as_ref().map_or(0, Vec::len),
                "read-pool: dedup verify mismatch"
            );
        } else {
            tracing::warn!(
                worker = ctx.worker_idx,
                purpose = ?req.purpose,
                pba = req.mapping.pba.0,
                slot_offset = req.mapping.slot_offset,
                unit_compressed_size = req.mapping.unit_compressed_size,
                unit_original_size = req.mapping.unit_original_size,
                unit_lba_count = req.mapping.unit_lba_count,
                offset_in_unit = req.mapping.offset_in_unit,
                expected_crc = *expected,
                actual_crc = *actual,
                raw_extent_blocks = req.raw_extent.as_ref().map_or(0, Vec::len),
                "read-pool: CRC mismatch"
            );
        }
    }
    ctx.metrics.record_read_pool_decode_ns(elapsed_ns(decode_start));
    let _ = req.reply.send(result);
}

fn elapsed_ns(start: Instant) -> u64 {
    start.elapsed().as_nanos() as u64
}

fn record_queue_wait(metrics: &EngineMetrics, worker_idx: usize, req: &ReadRequest) {
    metrics.record_read_pool_worker_queue_wait_ns(
        worker_idx,
        Instant::now()
            .saturating_duration_since(req.enqueued_at)
            .as_nanos() as u64,
    );
}

fn counts_as_crc_error(purpose: ReadPurpose) -> bool {
    !matches!(
        purpose,
        ReadPurpose::DedupVerify
            | ReadPurpose::DedupVerifyIndex
            | ReadPurpose::DedupVerifyCandidate
    )
}

fn record_purpose_mismatch(metrics: &EngineMetrics, purpose: ReadPurpose) {
    match purpose {
        ReadPurpose::Foreground => &metrics.read_crc_errors_foreground,
        ReadPurpose::DedupVerify
        | ReadPurpose::DedupVerifyIndex
        | ReadPurpose::DedupVerifyCandidate => &metrics.dedup_verify_mismatches,
        ReadPurpose::DedupScanner => &metrics.read_crc_errors_dedup_scanner,
    }
    .fetch_add(1, Ordering::Relaxed);
}

fn decode_raw_extent(
    raw: &[u8],
    mappings: &[BlockmapValue],
    metrics: &EngineMetrics,
) -> OnyxResult<Vec<u8>> {
    let bs = BLOCK_SIZE as usize;
    let expected = mappings.len() * bs;
    if raw.len() < expected {
        return Err(OnyxError::Compress(format!(
            "raw extent too short: {} bytes, need {expected}",
            raw.len()
        )));
    }

    for (idx, mapping) in mappings.iter().enumerate() {
        let off = idx * bs;
        let end = off + bs;
        let block = &raw[off..end];
        let actual_crc = crc32fast::hash(block);
        if actual_crc != mapping.crc32 {
            metrics.read_crc_errors.fetch_add(1, Ordering::Relaxed);
            return Err(OnyxError::CrcMismatch {
                expected: mapping.crc32,
                actual: actual_crc,
            });
        }
    }

    metrics
        .read_lv3_hits
        .fetch_add(mappings.len() as u64, Ordering::Relaxed);
    metrics
        .lv3_read_decompressed_bytes
        .fetch_add(expected as u64, Ordering::Relaxed);
    Ok(raw[..expected].to_vec())
}

fn validate_raw_extent(mappings: &[BlockmapValue]) -> OnyxResult<()> {
    for (idx, mapping) in mappings.iter().enumerate() {
        let is_raw_block = mapping.compression == 0
            && mapping.slot_offset == 0
            && mapping.unit_compressed_size == BLOCK_SIZE
            && mapping.unit_original_size == BLOCK_SIZE
            && mapping.unit_lba_count == 1
            && mapping.offset_in_unit == 0;
        if !is_raw_block {
            return Err(OnyxError::Config(format!(
                "ReadPool raw extent contains non-raw mapping at index {idx}: {mapping:?}"
            )));
        }
        if idx > 0 && mapping.pba.0 != mappings[idx - 1].pba.0 + 1 {
            return Err(OnyxError::Config(format!(
                "ReadPool raw extent PBAs are not contiguous at index {idx}: {} after {}",
                mapping.pba.0,
                mappings[idx - 1].pba.0
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
