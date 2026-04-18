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
//! Routing: requests are sharded by `hash(pba) % workers` so that future
//! optimisations (in-flight read coalescing for fragments sharing a slot) can
//! happen worker-locally.
//!
//! Buffer hits and unmapped reads are *not* sent through the pool — those
//! paths are zero-IO and stay inline on the caller thread.

use std::os::fd::RawFd;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam_channel::{bounded, Receiver, Sender};

use crate::error::{OnyxError, OnyxResult};
use crate::io::aligned::AlignedBuf;
use crate::io::device::RawDevice;
use crate::io::uring::{IoUringSession, UringOp};
use crate::meta::schema::BlockmapValue;
use crate::metrics::EngineMetrics;
use crate::zone::read::extract_lba_from_compressed;

/// Maximum requests folded into one `submit_batch` per worker iteration.
const BATCH_MAX: usize = 32;

/// Per-worker request channel capacity. With BATCH_MAX=32 and bounded(128) the
/// channel can hold ~4 batches worth of in-flight requests, giving callers
/// some slack while preserving back-pressure under sustained overload.
const REQUEST_CHANNEL_CAP: usize = 128;

struct ReadRequest {
    mapping: BlockmapValue,
    reply: Sender<OnyxResult<Vec<u8>>>,
}

struct WorkerHandle {
    sender: Option<Sender<ReadRequest>>,
    join: Option<JoinHandle<()>>,
}

pub struct ReadPool {
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

        let mut handles = Vec::with_capacity(workers);
        for worker_idx in 0..workers {
            let (tx, rx) = bounded::<ReadRequest>(REQUEST_CHANNEL_CAP);
            let session = IoUringSession::new(sq_entries)?;
            let worker_device = RawDevice::open(&device_path)?;
            let metrics = metrics.clone();
            let device_path_clone = device_path.clone();
            let join = thread::Builder::new()
                .name(format!("read-pool-{}", worker_idx))
                .spawn(move || {
                    let fd = worker_device.as_raw_fd();
                    let ctx = WorkerCtx {
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
            handles.push(WorkerHandle {
                sender: Some(tx),
                join: Some(join),
            });
        }

        tracing::info!(workers, sq_entries, "read pool started");

        Ok(Self { workers: handles })
    }

    /// Submit a mapped LV3 read. Blocks the caller on a oneshot until the
    /// worker has read + CRC-verified + decompressed the requested 4 KB.
    pub fn submit_read(&self, mapping: BlockmapValue) -> OnyxResult<Vec<u8>> {
        let worker_idx = (mapping.pba.0 as usize) % self.workers.len();
        let (reply_tx, reply_rx) = bounded::<OnyxResult<Vec<u8>>>(1);
        let sender = self.workers[worker_idx]
            .sender
            .as_ref()
            .ok_or_else(|| OnyxError::Io(std::io::Error::other("read-pool already shut down")))?;
        sender
            .send(ReadRequest {
                mapping,
                reply: reply_tx,
            })
            .map_err(|_| OnyxError::Io(std::io::Error::other("read-pool worker channel closed")))?;
        reply_rx
            .recv()
            .map_err(|_| OnyxError::Io(std::io::Error::other("read-pool reply dropped")))?
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Drop every sender so worker threads observe a closed channel and exit,
    /// then join. Idempotent — safe to call from `Drop`.
    pub fn shutdown(&mut self) {
        for w in &mut self.workers {
            w.sender.take();
        }
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
    bufs: Vec<AlignedBuf>,
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
        self.bufs.clear();
        self.ops.clear();
        self.requests.clear();
        self.expected.clear();
        self.offsets.clear();
    }
}

fn worker_loop(ctx: WorkerCtx, rx: Receiver<ReadRequest>) {
    let mut scratch = BatchScratch::with_capacity(BATCH_MAX);
    let mut batch: Vec<ReadRequest> = Vec::with_capacity(BATCH_MAX);
    loop {
        let first = match rx.recv() {
            Ok(req) => req,
            Err(_) => return,
        };
        batch.clear();
        batch.push(first);
        while batch.len() < BATCH_MAX {
            match rx.try_recv() {
                Ok(req) => batch.push(req),
                Err(_) => break,
            }
        }
        process_batch(&ctx, &mut scratch, &mut batch);
    }
}

fn process_batch(ctx: &WorkerCtx, scratch: &mut BatchScratch, batch: &mut Vec<ReadRequest>) {
    let bs = ctx.block_size as usize;
    scratch.clear();

    for req in batch.drain(..) {
        let m = &req.mapping;
        let read_size = m.compressed_read_size(bs);
        if read_size == 0 {
            let _ = req.reply.send(Err(OnyxError::Compress(
                "ReadPool: zero-length compressed unit".into(),
            )));
            continue;
        }
        let offset = ctx.base_offset + (m.pba.0 + ctx.pba_offset) * ctx.block_size as u64;

        let mut buf = match AlignedBuf::new(read_size, ctx.use_hugepages) {
            Ok(b) => b,
            Err(e) => {
                let _ = req.reply.send(Err(e));
                continue;
            }
        };
        let ptr = buf.as_mut_ptr();
        scratch.bufs.push(buf);
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
    let cqes = match unsafe { ctx.ring.submit_batch(&scratch.ops) } {
        Ok(c) => c,
        Err(e) => {
            for req in scratch.requests.drain(..) {
                let _ = req
                    .reply
                    .send(Err(OnyxError::Io(std::io::Error::other(format!(
                        "read-pool submit_batch failed: {e}"
                    )))));
            }
            return;
        }
    };

    let bufs = std::mem::take(&mut scratch.bufs);
    let requests = std::mem::take(&mut scratch.requests);
    for ((req, buf), (i, exp_bytes)) in requests
        .into_iter()
        .zip(bufs.into_iter())
        .zip(scratch.expected.iter().copied().enumerate())
    {
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

        // CRC + decompress + LBA slice — shared with the inline LV3 path so
        // the two cannot drift.
        let result = extract_lba_from_compressed(buf.as_slice(), &req.mapping, &ctx.metrics);
        let _ = req.reply.send(result);
    }
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

        let got = pool.submit_read(make_mapping(Pba(0), BLOCK_SIZE, crc)).unwrap();
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
        let pool = Arc::new(
            ReadPool::start(4, 64, &pool_dev, 0, BLOCK_SIZE, false, metrics).unwrap(),
        );

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
