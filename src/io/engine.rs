use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::error::{OnyxError, OnyxResult};
use crate::io::aligned::AlignedBuf;
use crate::io::device::RawDevice;
use crate::io::uring::{IoUringSession, UringOp, UringOpResult};
use crate::metrics::EngineMetrics;
use crate::types::{Pba, BLOCK_SIZE, RESERVED_BLOCKS};

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
/// crc32, original_size) lives in RocksDB BlockmapValue, which is crash-consistent
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
    pub fn submit_batch(&self, ops: Vec<LvOp<'_>>, fsync_after: bool) -> OnyxResult<Vec<LvOpResult>> {
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
        let parallelize = all_writes && ops.len() > 1;

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

        if fsync_after {
            uring_ops.push(UringOp::FsyncDataBarrier { fd });
        }

        let results = if uring_ops.is_empty() {
            Vec::new()
        } else {
            unsafe { session.submit_batch(&uring_ops)? }
        };

        let fsync_offset = if fsync_after {
            uring_ops.len() - 1
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

        Ok(out.into_iter().map(|s| s.expect("all slots filled")).collect())
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
        let engine = IoEngine::with_options(
            dev,
            false,
            0,
            None,
            IoBackend::Uring(session),
        );

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
        let engine = IoEngine::with_options(
            dev,
            false,
            0,
            None,
            IoBackend::Uring(session),
        );

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
