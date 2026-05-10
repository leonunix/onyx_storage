use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use crate::error::{OnyxError, OnyxResult};
use crate::io::aligned::AlignedBuf;
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
static LV3_WRITE_RECORDS: OnceLock<Mutex<HashMap<u64, Lv3WriteRecord>>> = OnceLock::new();

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
    LV3_WRITE_RECORDS
        .get()
        .and_then(|records| records.lock().ok()?.get(&pba.0).copied())
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
    data_device: RawDevice,
    use_hugepages: bool,
    block_size: u32,
    pba_offset: u64,
    metrics: Option<Arc<EngineMetrics>>,
    backend: IoBackend,
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

impl IoEngine {
    fn with_options(
        data_device: RawDevice,
        use_hugepages: bool,
        pba_offset: u64,
        metrics: Option<Arc<EngineMetrics>>,
        backend: IoBackend,
    ) -> Self {
        Self {
            data_device,
            use_hugepages,
            block_size: BLOCK_SIZE,
            pba_offset,
            metrics,
            backend,
        }
    }

    /// Create an IoEngine with standard PBA offset (RESERVED_BLOCKS) and the
    /// classic syscall backend (pread/pwrite).
    pub fn new(data_device: RawDevice, use_hugepages: bool) -> Self {
        Self::with_options(
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
        Self::with_options(
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
        Self::with_options(
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
        Self::with_options(
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
        Self::with_options(data_device, use_hugepages, 0, None, IoBackend::Syscall)
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
            self.data_device.read_at(actual.as_mut_slice(), offset)?;
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
            path: self.data_device.path().to_path_buf(),
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
        let records = LV3_WRITE_RECORDS.get_or_init(|| Mutex::new(HashMap::new()));
        if let Ok(mut records) = records.lock() {
            for block_offset in 0..blocks {
                records.insert(
                    pba.0 + block_offset as u64,
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

    fn validate_uring_result(
        &self,
        op: &str,
        offset: u64,
        expected: u32,
        result: &UringOpResult,
    ) -> OnyxResult<()> {
        if let Some(errno) = result.errno() {
            return Err(OnyxError::Device {
                path: self.data_device.path().to_path_buf(),
                reason: format!(
                    "io_uring {op} failed at offset={offset}: errno={errno} ({})",
                    std::io::Error::from_raw_os_error(errno)
                ),
            });
        }
        let bytes = result.bytes().unwrap_or(0);
        if bytes != expected {
            return Err(OnyxError::Device {
                path: self.data_device.path().to_path_buf(),
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
                self.data_device.write_at(buf.as_slice(), offset)?;
            }
            IoBackend::Uring(session) => {
                let op = UringOp::Write {
                    fd: self.data_device.as_raw_fd(),
                    ptr: buf.as_ptr(),
                    len: total_size as u32,
                    offset: self.data_device.base_offset() + offset,
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
                self.data_device.read_at(buf.as_mut_slice(), offset)?;
            }
            IoBackend::Uring(session) => {
                let op = UringOp::Read {
                    fd: self.data_device.as_raw_fd(),
                    ptr: buf.as_mut_ptr(),
                    len: read_size as u32,
                    offset: self.data_device.base_offset() + offset,
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

    fn submit_batch_syscall(
        &self,
        ops: Vec<LvOp<'_>>,
        fsync_after: bool,
    ) -> OnyxResult<Vec<LvOpResult>> {
        // Mixed read/write batches stay sequential — uncommon enough that
        // parallelising is not worth the complexity.
        let all_writes = !ops.is_empty() && ops.iter().all(|op| matches!(op, LvOp::Write { .. }));
        let parallelize = all_writes && ops.len() > 1 && !Self::syscall_write_serial_enabled();

        let out: Vec<LvOpResult> = if parallelize {
            // Parallel pwrite via scoped threads to keep NVMe queue depth > 1
            // on the syscall backend. Mirrors the pre-io_uring passthrough path.
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
            self.data_device.sync()?;
        }
        Ok(out)
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
        let fd = self.data_device.as_raw_fd();

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
                        offset: self.data_device.base_offset() + offset,
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
                        offset: self.data_device.base_offset() + offset,
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
                    path: self.data_device.path().to_path_buf(),
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
        let fd = self.data_device.as_raw_fd();
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
                    offset: self.data_device.base_offset() + metas[uring_ops.len()].2,
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
                    path: self.data_device.path().to_path_buf(),
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

    /// Borrow the underlying data device. Used by the engine shutdown path
    /// to stamp the `FLAG_CLEAN_SHUTDOWN` bit in the LV3 superblock.
    pub fn data_device(&self) -> &RawDevice {
        &self.data_device
    }

    pub fn device_size(&self) -> u64 {
        self.data_device.size()
    }

    pub fn total_blocks(&self) -> u64 {
        self.data_device.size() / self.block_size as u64 - self.pba_offset
    }

    pub fn sync(&self) -> OnyxResult<()> {
        match &self.backend {
            IoBackend::Syscall => self.data_device.sync(),
            IoBackend::Uring(session) => {
                let op = UringOp::FsyncData {
                    fd: self.data_device.as_raw_fd(),
                };
                let results = unsafe { session.submit_batch(std::slice::from_ref(&op))? };
                if let Some(errno) = results[0].errno() {
                    return Err(OnyxError::Device {
                        path: self.data_device.path().to_path_buf(),
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
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn fresh_device(dir: &TempDir, name: &str, size: u64) -> RawDevice {
        let path: PathBuf = dir.path().join(name);
        RawDevice::open_or_create(&path, size).unwrap()
    }

    #[test]
    fn syscall_round_trip() {
        let dir = TempDir::new().unwrap();
        let dev = fresh_device(&dir, "lv3", 1024 * 1024);
        let engine = IoEngine::new_raw(dev, false);

        let payload = vec![0xABu8; 4096];
        engine.write_blocks(Pba(0), &payload).unwrap();
        let read = engine.read_blocks(Pba(0), 4096).unwrap();
        assert_eq!(read, payload);
    }

    #[test]
    fn uring_round_trip() {
        let dir = TempDir::new().unwrap();
        let dev = fresh_device(&dir, "lv3", 1024 * 1024);
        let session = Arc::new(IoUringSession::new(16).unwrap());
        let engine = IoEngine::with_options(dev, false, 0, None, IoBackend::Uring(session));

        let payload = vec![0xCDu8; 4096];
        engine.write_blocks(Pba(2), &payload).unwrap();
        let read = engine.read_blocks(Pba(2), 4096).unwrap();
        assert_eq!(read, payload);
    }

    #[test]
    fn uring_batch_writes_then_reads() {
        let dir = TempDir::new().unwrap();
        let dev = fresh_device(&dir, "lv3", 1024 * 1024);
        let session = Arc::new(IoUringSession::new(64).unwrap());
        let engine = IoEngine::with_options(dev, false, 0, None, IoBackend::Uring(session));

        let payloads: Vec<Vec<u8>> = (0..8).map(|i| vec![i as u8; 4096]).collect();
        let writes: Vec<LvOp> = payloads
            .iter()
            .enumerate()
            .map(|(i, p)| LvOp::Write {
                pba: Pba(i as u64),
                payload: p.as_slice(),
            })
            .collect();
        let results = engine.submit_batch(writes, true).unwrap();
        assert_eq!(results.len(), 8);
        for r in &results {
            match r {
                LvOpResult::Write(Ok(())) => {}
                _ => panic!("write should have succeeded"),
            }
        }

        let reads: Vec<LvOp> = (0..8)
            .map(|i| LvOp::Read {
                pba: Pba(i as u64),
                size: 4096,
            })
            .collect();
        let results = engine.submit_batch(reads, false).unwrap();
        for (i, r) in results.into_iter().enumerate() {
            match r {
                LvOpResult::Read(Ok(bytes)) => {
                    assert_eq!(bytes, payloads[i], "read {} mismatch", i);
                }
                _ => panic!("read {} failed", i),
            }
        }
    }

    #[test]
    fn uring_batch_chunks_when_ops_exceed_sq_entries() {
        let dir = TempDir::new().unwrap();
        let dev = fresh_device(&dir, "lv3", 1024 * 1024);
        let session = Arc::new(IoUringSession::new(4).unwrap());
        let engine = IoEngine::with_options(dev, false, 0, None, IoBackend::Uring(session));

        let payloads: Vec<Vec<u8>> = (0..10).map(|i| vec![(i + 1) as u8; 4096]).collect();
        let writes: Vec<LvOp> = payloads
            .iter()
            .enumerate()
            .map(|(i, p)| LvOp::Write {
                pba: Pba(i as u64),
                payload: p.as_slice(),
            })
            .collect();
        let results = engine.submit_batch(writes, true).unwrap();
        assert_eq!(results.len(), payloads.len());
        for result in results {
            assert!(matches!(result, LvOpResult::Write(Ok(()))));
        }

        let reads: Vec<LvOp> = (0..payloads.len())
            .map(|i| LvOp::Read {
                pba: Pba(i as u64),
                size: 4096,
            })
            .collect();
        let results = engine.submit_batch(reads, false).unwrap();
        for (i, result) in results.into_iter().enumerate() {
            match result {
                LvOpResult::Read(Ok(bytes)) => assert_eq!(bytes, payloads[i]),
                _ => panic!("read {i} failed"),
            }
        }
    }

    #[test]
    fn syscall_batch_writes_then_reads() {
        let dir = TempDir::new().unwrap();
        let dev = fresh_device(&dir, "lv3", 1024 * 1024);
        let engine = IoEngine::new_raw(dev, false);

        let payloads: Vec<Vec<u8>> = (0..4).map(|i| vec![(i + 0x10) as u8; 4096]).collect();
        let writes: Vec<LvOp> = payloads
            .iter()
            .enumerate()
            .map(|(i, p)| LvOp::Write {
                pba: Pba(i as u64),
                payload: p.as_slice(),
            })
            .collect();
        let _ = engine.submit_batch(writes, true).unwrap();

        let reads: Vec<LvOp> = (0..4)
            .map(|i| LvOp::Read {
                pba: Pba(i as u64),
                size: 4096,
            })
            .collect();
        let results = engine.submit_batch(reads, false).unwrap();
        for (i, r) in results.into_iter().enumerate() {
            match r {
                LvOpResult::Read(Ok(bytes)) => assert_eq!(bytes, payloads[i]),
                _ => panic!("read {} failed", i),
            }
        }
    }
}
