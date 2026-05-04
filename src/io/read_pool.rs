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
use crate::io::device::RawDevice;
use crate::io::uring::{IoUringSession, UringOp};
use crate::meta::schema::BlockmapValue;
use crate::metrics::EngineMetrics;
use crate::types::BLOCK_SIZE;
use crate::zone::read::{decode_unit, extract_lba_from_compressed};

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

struct ReadRequest {
    mapping: BlockmapValue,
    reply: Sender<OnyxResult<Vec<u8>>>,
    enqueued_at: Instant,
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
                        ring: session,
                        fd,
                        base_offset,
                        pba_offset,
                        block_size,
                        use_hugepages,
                        metrics,
                        device_path: device_path_clone,
                        _device: worker_device,
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

    /// Submit a mapped LV3 read and block until the worker has read + CRC
    /// verified + decompressed the requested 4 KB LBA.
    pub fn submit_read(&self, mapping: BlockmapValue) -> OnyxResult<Vec<u8>> {
        let rx = self.enqueue(mapping, false)?;
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
        self.enqueue(mapping, false)
    }

    /// Submit a compression-unit read and block for the full decoded unit
    /// payload (`unit_original_size` bytes). Callers slice out multiple LBAs
    /// from the returned buffer via `offset_in_unit`.
    pub fn submit_unit_read(&self, mapping: BlockmapValue) -> OnyxResult<Vec<u8>> {
        let rx = self.enqueue(mapping, true)?;
        rx.recv()
            .map_err(|_| OnyxError::Io(std::io::Error::other("read-pool reply dropped")))?
    }

    /// Non-blocking companion to `submit_unit_read`.
    pub fn submit_unit_read_async(
        &self,
        mapping: BlockmapValue,
    ) -> OnyxResult<Receiver<OnyxResult<Vec<u8>>>> {
        self.enqueue(mapping, true)
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

struct WorkerCtx {
    worker_idx: usize,
    ring: IoUringSession,
    fd: RawFd,
    base_offset: u64,
    pba_offset: u64,
    block_size: u32,
    use_hugepages: bool,
    metrics: Arc<EngineMetrics>,
    device_path: std::path::PathBuf,
    /// Keeps the `RawDevice` alive as long as the worker is running; the fd
    /// stored above is only valid while this field exists.
    _device: RawDevice,
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
        let offset = ctx.base_offset + (pba.0 + ctx.pba_offset) * ctx.block_size as u64;

        let slot = scratch.requests.len();
        let alloc_start = Instant::now();
        let buf = match scratch.prepare_buffer(slot, read_size, ctx.use_hugepages) {
            Ok(b) => b,
            Err(e) => {
                let _ = req.reply.send(Err(e));
                continue;
            }
        };
        ctx.metrics
            .read_pool_alloc_ns
            .fetch_add(elapsed_ns(alloc_start), Ordering::Relaxed);
        let ptr = buf.as_mut_ptr();
        scratch.ops.push(UringOp::Read {
            fd: ctx.fd,
            ptr,
            len: read_size as u32,
            offset,
        });
        scratch.expected.push(read_size as u32);
        scratch.offsets.push(offset);
        scratch.requests.push(req);
    }

    if scratch.ops.is_empty() {
        return;
    }

    // Single io_uring_enter for the whole batch — kernel processes the SQEs
    // in parallel, the worker waits for every CQE before harvesting.
    let submit_start = Instant::now();
    let cqes = match unsafe { ctx.ring.submit_batch(&scratch.ops) } {
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

    for (i, (req, exp_bytes)) in scratch
        .requests
        .iter()
        .zip(scratch.expected.iter().copied())
        .enumerate()
    {
        let buf = &scratch.bufs[i].buf;
        let cqe = &cqes[i];
        let offset = scratch.offsets[i];
        if let Some(errno) = cqe.errno() {
            let _ = req.reply.send(Err(OnyxError::Device {
                path: ctx.device_path.clone(),
                reason: format!("io_uring read failed at offset={offset}: errno={errno}"),
            }));
            continue;
        }
        let bytes = cqe.bytes().unwrap_or(0);
        if bytes != exp_bytes {
            let _ = req.reply.send(Err(OnyxError::Device {
                path: ctx.device_path.clone(),
                reason: format!(
                    "io_uring short read at offset={offset}: got {bytes} of {exp_bytes}"
                ),
            }));
            continue;
        }

        ctx.metrics.lv3_read_ops.fetch_add(1, Ordering::Relaxed);
        ctx.metrics
            .lv3_read_compressed_bytes
            .fetch_add(exp_bytes as u64, Ordering::Relaxed);

        // Two output modes — both share the same CRC + decompress path:
        //   return_unit=false: slice out a single 4 KB LBA (legacy).
        //   return_unit=true:  hand back the full decoded unit so the caller
        //                      can fan out multiple LBAs from one IO.
        let decode_start = Instant::now();
        let result = if let Some(mappings) = req.raw_extent.as_ref() {
            decode_raw_extent(buf.as_slice(), mappings, &ctx.metrics)
        } else if req.return_unit {
            decode_unit(buf.as_slice(), &req.mapping, &ctx.metrics)
                .map(|payload| payload.into_owned())
        } else {
            extract_lba_from_compressed(buf.as_slice(), &req.mapping, &ctx.metrics)
        };
        ctx.metrics
            .record_read_pool_decode_ns(elapsed_ns(decode_start));
        let _ = req.reply.send(result);
    }
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
mod tests {
    use super::*;
    use crate::io::engine::IoEngine;
    use crate::types::{Pba, BLOCK_SIZE};
    use std::sync::Arc;
    use std::sync::Barrier;
    use tempfile::NamedTempFile;

    fn fresh_device() -> (RawDevice, NamedTempFile) {
        let tmp = NamedTempFile::new().unwrap();
        tmp.as_file().set_len(4 * 1024 * 1024).unwrap();
        let dev = RawDevice::open_or_create(tmp.path(), 4 * 1024 * 1024).unwrap();
        (dev, tmp)
    }

    fn write_uncompressed(engine: &IoEngine, pba: Pba, payload: &[u8]) {
        engine.write_blocks(pba, payload).unwrap();
    }

    fn make_mapping(pba: Pba, payload_len: u32, crc: u32) -> BlockmapValue {
        BlockmapValue {
            pba,
            compression: 0,
            unit_compressed_size: payload_len,
            unit_original_size: payload_len,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: crc,
            slot_offset: 0,
            flags: 0,
        }
    }

    #[test]
    fn read_pool_round_trip_uncompressed() {
        let (dev, tmp) = fresh_device();
        let engine = IoEngine::new_raw(dev, false);

        let payload = vec![0xC3u8; BLOCK_SIZE as usize];
        write_uncompressed(&engine, Pba(0), &payload);
        let crc = crc32fast::hash(&payload);

        let pool_dev = RawDevice::open_or_create(tmp.path(), 4 * 1024 * 1024).unwrap();
        let metrics = Arc::new(EngineMetrics::default());
        let pool = ReadPool::start(2, 16, &pool_dev, 0, BLOCK_SIZE, false, metrics).unwrap();

        let got = pool
            .submit_read(make_mapping(Pba(0), BLOCK_SIZE, crc))
            .unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn read_pool_concurrent_reads_match_writes() {
        let (dev, tmp) = fresh_device();
        let engine = IoEngine::new_raw(dev, false);

        let payloads: Vec<Vec<u8>> = (0..32)
            .map(|i| vec![(i & 0xFF) as u8; BLOCK_SIZE as usize])
            .collect();
        for (i, p) in payloads.iter().enumerate() {
            write_uncompressed(&engine, Pba(i as u64), p);
        }
        let crcs: Vec<u32> = payloads.iter().map(|p| crc32fast::hash(p)).collect();

        let pool_dev = RawDevice::open_or_create(tmp.path(), 4 * 1024 * 1024).unwrap();
        let metrics = Arc::new(EngineMetrics::default());
        let pool =
            Arc::new(ReadPool::start(4, 64, &pool_dev, 0, BLOCK_SIZE, false, metrics).unwrap());

        let barrier = Arc::new(Barrier::new(8));
        let mut handles = Vec::new();
        for t in 0..8 {
            let pool = pool.clone();
            let payloads = payloads.clone();
            let crcs = crcs.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for round in 0..5 {
                    for i in 0..32usize {
                        let mapping = make_mapping(Pba(i as u64), BLOCK_SIZE, crcs[i]);
                        let got = pool.submit_read(mapping).unwrap();
                        assert_eq!(
                            got, payloads[i],
                            "thread {} round {} pba {} mismatch",
                            t, round, i
                        );
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    /// `submit_unit_read` must return the full decoded unit payload so that
    /// callers can fan out multiple LBAs from one io_uring read + decompress.
    #[test]
    fn submit_unit_read_returns_full_payload() {
        let (dev, tmp) = fresh_device();
        let engine = IoEngine::new_raw(dev, false);

        // A 4-LBA uncompressed "unit" — all four 4 KB slots laid out
        // contiguously, each filled with a distinct byte pattern.
        let unit_bytes = 4 * BLOCK_SIZE as usize;
        let mut unit = vec![0u8; unit_bytes];
        for (i, chunk) in unit.chunks_mut(BLOCK_SIZE as usize).enumerate() {
            chunk.fill((i as u8) + 0x30);
        }
        engine.write_blocks(Pba(0), &unit).unwrap();
        let crc = crc32fast::hash(&unit);

        let pool_dev = RawDevice::open_or_create(tmp.path(), 4 * 1024 * 1024).unwrap();
        let metrics = Arc::new(EngineMetrics::default());
        let pool =
            ReadPool::start(1, 16, &pool_dev, 0, BLOCK_SIZE, false, metrics.clone()).unwrap();

        let mut mapping = make_mapping(Pba(0), unit_bytes as u32, crc);
        mapping.unit_original_size = unit_bytes as u32;
        mapping.unit_lba_count = 4;

        let got = pool.submit_unit_read(mapping).unwrap();
        assert_eq!(got.len(), unit_bytes);
        for (i, chunk) in got.chunks(BLOCK_SIZE as usize).enumerate() {
            assert_eq!(chunk, &[(i as u8) + 0x30; BLOCK_SIZE as usize]);
        }
        // One io_uring op served the whole unit — the bedrock of coalescing.
        assert_eq!(metrics.lv3_read_ops.load(Ordering::Relaxed), 1);
        // `submit_unit_read` does NOT bump per-LBA hits; callers do that in
        // their fan-out loop.
        assert_eq!(metrics.read_lv3_hits.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn submit_raw_extent_read_async_reads_one_contiguous_span() {
        let (dev, tmp) = fresh_device();
        let engine = IoEngine::new_raw(dev, false);

        let mut mappings = Vec::new();
        let mut expected = Vec::new();
        for i in 0..4u64 {
            let payload = vec![0x40 + i as u8; BLOCK_SIZE as usize];
            write_uncompressed(&engine, Pba(i), &payload);
            expected.extend_from_slice(&payload);
            mappings.push(make_mapping(Pba(i), BLOCK_SIZE, crc32fast::hash(&payload)));
        }

        let pool_dev = RawDevice::open_or_create(tmp.path(), 4 * 1024 * 1024).unwrap();
        let metrics = Arc::new(EngineMetrics::default());
        let pool =
            ReadPool::start(1, 16, &pool_dev, 0, BLOCK_SIZE, false, metrics.clone()).unwrap();

        let rx = pool.submit_raw_extent_read_async(mappings).unwrap();
        let got = rx.recv().unwrap().unwrap();
        assert_eq!(got, expected);
        assert_eq!(metrics.lv3_read_ops.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.read_lv3_hits.load(Ordering::Relaxed), 4);
    }

    /// `submit_read_async` lets callers fire N requests before draining any
    /// reply — the worker batches them into one `submit_batch` io_uring enter.
    #[test]
    fn submit_read_async_fans_out_before_draining() {
        let (dev, tmp) = fresh_device();
        let engine = IoEngine::new_raw(dev, false);

        let payloads: Vec<Vec<u8>> = (0..8)
            .map(|i| vec![(i & 0xFF) as u8; BLOCK_SIZE as usize])
            .collect();
        for (i, p) in payloads.iter().enumerate() {
            write_uncompressed(&engine, Pba(i as u64), p);
        }
        let crcs: Vec<u32> = payloads.iter().map(|p| crc32fast::hash(p)).collect();

        // Single worker forces all requests to share one channel — this is
        // the scenario where pre-draining vs serial round-trip matters.
        let pool_dev = RawDevice::open_or_create(tmp.path(), 4 * 1024 * 1024).unwrap();
        let metrics = Arc::new(EngineMetrics::default());
        let pool = ReadPool::start(1, 16, &pool_dev, 0, BLOCK_SIZE, false, metrics).unwrap();

        let mut rxs = Vec::new();
        for i in 0..8 {
            let rx = pool
                .submit_read_async(make_mapping(Pba(i as u64), BLOCK_SIZE, crcs[i]))
                .unwrap();
            rxs.push(rx);
        }
        for (i, rx) in rxs.into_iter().enumerate() {
            let got = rx.recv().unwrap().unwrap();
            assert_eq!(got, payloads[i]);
        }
    }

    #[test]
    fn read_pool_crc_mismatch_returns_error() {
        let (dev, tmp) = fresh_device();
        let engine = IoEngine::new_raw(dev, false);

        let payload = vec![0x77u8; BLOCK_SIZE as usize];
        write_uncompressed(&engine, Pba(0), &payload);
        let bad_crc = crc32fast::hash(&payload).wrapping_add(1);

        let pool_dev = RawDevice::open_or_create(tmp.path(), 4 * 1024 * 1024).unwrap();
        let metrics = Arc::new(EngineMetrics::default());
        let pool = ReadPool::start(1, 8, &pool_dev, 0, BLOCK_SIZE, false, metrics.clone()).unwrap();

        let err = pool
            .submit_read(make_mapping(Pba(0), BLOCK_SIZE, bad_crc))
            .unwrap_err();
        match err {
            OnyxError::CrcMismatch { .. } => {}
            other => panic!("expected CrcMismatch, got {other:?}"),
        }
        assert_eq!(metrics.read_crc_errors.load(Ordering::Relaxed), 1);
    }
}
