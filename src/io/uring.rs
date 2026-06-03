//! Low-level `io_uring` session: batched submit + wait_for_completions.
//!
//! `IoUringSession` owns a single `io_uring::IoUring` behind a mutex. Callers
//! describe a batch of read/write/fsync operations against arbitrary fds and
//! offsets; the session pushes SQEs, calls `submit_and_wait(N)`, and harvests
//! N CQEs into a result vector.
//!
//! Higher-level wrappers (`IoEngine`, `BufferShard` sync loop, `HeartbeatWriter`,
//! `ReadPool`) own a session and translate their own logical operations into
//! `UringOp` descriptors. They are responsible for keeping any backing buffers
//! alive across the call (raw pointers in `UringOp` are not lifetime-checked).

use std::os::fd::RawFd;

use io_uring::{opcode, squeue, types, IoUring};
use parking_lot::Mutex;

use crate::error::{OnyxError, OnyxResult};

/// One operation in a batch. Pointers must remain valid until `submit_batch`
/// returns. `Read.ptr` must be writable; `Write.ptr` may be read-only.
pub enum UringOp {
    Read {
        fd: RawFd,
        ptr: *mut u8,
        len: u32,
        offset: u64,
    },
    Write {
        fd: RawFd,
        ptr: *const u8,
        len: u32,
        offset: u64,
    },
    /// fdatasync barrier: kernel waits for all earlier SQEs in the same submit
    /// to complete before issuing the fsync. Must come last in the batch.
    FsyncDataBarrier { fd: RawFd },
    /// Plain fdatasync (no ordering with other SQEs).
    FsyncData { fd: RawFd },
}

// SAFETY: UringOp carries raw pointers; the caller guarantees they outlive the
// submit_batch call. The pointers are only dereferenced by the kernel during the
// IO, and the session never retains them past `submit_batch`.
unsafe impl Send for UringOp {}
unsafe impl Sync for UringOp {}

/// One op in a `submit_linked_wait` batch. `link_next` sets `IOSQE_IO_LINK`, so
/// the NEXT pushed SQE will not start until this one completes — letting the
/// caller build a "writes → fsync" dependency chain whose fsync waits only for
/// its own writes (unlike `FsyncDataBarrier`'s `IO_DRAIN`, which is a whole-ring
/// barrier). A failed op in a link chain cancels the remaining linked ops with
/// `-ECANCELED` (each still posts a CQE). Ops with `link_next = false` terminate
/// the chain; any subsequent ops form an independent (concurrent) group.
pub struct LinkedOp {
    pub op: UringOp,
    pub link_next: bool,
}

/// Build the SQE for one op, tagging it with `user_data` (so CQEs map back even
/// if the kernel reorders completions) and the submission flags implied by the
/// op kind plus `link_next`. Shared by `submit_batch` (always `link_next=false`)
/// and `submit_linked_wait`.
fn build_sqe(op: &UringOp, user_data: u64, link_next: bool) -> squeue::Entry {
    let mut flags = squeue::Flags::empty();
    if link_next {
        flags |= squeue::Flags::IO_LINK;
    }
    let entry = match op {
        UringOp::Read {
            fd,
            ptr,
            len,
            offset,
        } => opcode::Read::new(types::Fd(*fd), *ptr, *len)
            .offset(*offset)
            .build(),
        UringOp::Write {
            fd,
            ptr,
            len,
            offset,
        } => opcode::Write::new(types::Fd(*fd), *ptr, *len)
            .offset(*offset)
            .build(),
        UringOp::FsyncDataBarrier { fd } => {
            flags |= squeue::Flags::IO_DRAIN;
            opcode::Fsync::new(types::Fd(*fd))
                .flags(types::FsyncFlags::DATASYNC)
                .build()
        }
        UringOp::FsyncData { fd } => opcode::Fsync::new(types::Fd(*fd))
            .flags(types::FsyncFlags::DATASYNC)
            .build(),
    };
    entry.user_data(user_data).flags(flags)
}

/// Result of a single op, in submission order. `>=0` is the kernel return value
/// (bytes for read/write, 0 for fsync); `<0` is the negated errno.
#[derive(Debug, Clone, Copy)]
pub struct UringOpResult {
    /// Raw kernel return value: `>=0` on success, `-errno` on failure.
    pub result: i32,
}

impl UringOpResult {
    pub fn is_ok(&self) -> bool {
        self.result >= 0
    }

    pub fn errno(&self) -> Option<i32> {
        if self.result < 0 {
            Some(-self.result)
        } else {
            None
        }
    }

    pub fn bytes(&self) -> Option<u32> {
        if self.result >= 0 {
            Some(self.result as u32)
        } else {
            None
        }
    }
}

pub struct IoUringSession {
    ring: Mutex<IoUring>,
    sq_entries: u32,
}

impl IoUringSession {
    /// Create a new ring. `sq_entries` is rounded up to a power of two by the
    /// kernel; CQ size defaults to 2x the SQ size.
    pub fn new(sq_entries: u32) -> OnyxResult<Self> {
        let ring = IoUring::new(sq_entries).map_err(|e| {
            OnyxError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("io_uring setup failed (entries={sq_entries}): {e}"),
            ))
        })?;
        Ok(Self {
            ring: Mutex::new(ring),
            sq_entries,
        })
    }

    pub fn sq_entries(&self) -> u32 {
        self.sq_entries
    }

    /// Submit a batch of ops and wait for all completions.
    ///
    /// SAFETY: All pointers in `ops` must be valid for the duration of the call.
    /// `Read.ptr` must point to writable memory of at least `len` bytes;
    /// `Write.ptr` must be readable for `len` bytes.
    ///
    /// Returns one `UringOpResult` per input op, in the same order. Errors at
    /// the submission layer (queue full, kernel returns a global error) bubble
    /// up as `OnyxError`. Per-op kernel errors (EIO, etc.) are encoded in
    /// individual `UringOpResult` entries — caller decides how to react.
    pub unsafe fn submit_batch(&self, ops: &[UringOp]) -> OnyxResult<Vec<UringOpResult>> {
        if ops.is_empty() {
            return Ok(Vec::new());
        }
        if ops.len() as u32 > self.sq_entries {
            return Err(OnyxError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "io_uring batch too large: {} ops > sq_entries {}",
                    ops.len(),
                    self.sq_entries
                ),
            )));
        }

        let mut ring = self.ring.lock();

        // Build SQEs. user_data carries the op index so we can map CQEs back
        // even if the kernel reorders completions (it usually doesn't, but the
        // contract allows it for non-LINK / non-DRAIN ops).
        for (idx, op) in ops.iter().enumerate() {
            let entry = build_sqe(op, idx as u64, false);

            // SAFETY: SQE references kernel-managed pointers from UringOp; the
            // caller's contract guarantees those pointers remain valid until we
            // harvest CQEs below.
            let mut sub = ring.submission();
            if sub.push(&entry).is_err() {
                drop(sub);
                return Err(OnyxError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "io_uring submission queue full at op {} of {}",
                        idx,
                        ops.len()
                    ),
                )));
            }
        }

        let want = ops.len();
        ring.submit_and_wait(want).map_err(|e| {
            OnyxError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("io_uring submit_and_wait({want}) failed: {e}"),
            ))
        })?;

        let mut results = vec![UringOpResult { result: 0 }; ops.len()];
        let mut harvested = 0usize;
        let mut cq = ring.completion();
        cq.sync();
        for cqe in &mut cq {
            let idx = cqe.user_data() as usize;
            if idx >= results.len() {
                return Err(OnyxError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "io_uring CQE user_data {} out of range (batch size {})",
                        idx,
                        ops.len()
                    ),
                )));
            }
            results[idx] = UringOpResult {
                result: cqe.result(),
            };
            harvested += 1;
        }

        if harvested != ops.len() {
            return Err(OnyxError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "io_uring harvested {} CQEs, expected {}",
                    harvested,
                    ops.len()
                ),
            )));
        }

        Ok(results)
    }

    /// Submit a batch where each op may carry an `IOSQE_IO_LINK` flag, then wait
    /// for all completions. Same submit-and-wait-all contract as `submit_batch`,
    /// but the caller controls inter-op ordering via `LinkedOp::link_next`:
    ///
    /// - A run of `link_next = true` ops followed by a `link_next = false` op
    ///   forms one ordered chain (each starts only after the prior completes).
    ///   This lets a trailing `FsyncData` wait for exactly its own writes —
    ///   without the whole-ring `IO_DRAIN` barrier — so collapsing the LV2 sync
    ///   path's N-writes + fsync into ONE submit preserves durability ordering.
    /// - Ops after a chain-terminating op form an independent (concurrent)
    ///   group (e.g. a best-effort checkpoint write the fsync need not gate on).
    /// - A failed op in a chain cancels the remaining linked ops with
    ///   `-ECANCELED`; each cancelled op still posts a CQE, so the harvested
    ///   count still equals `ops.len()` and the failure surfaces per-op.
    ///
    /// Results are returned in input order (user_data = op index, set
    /// internally). All of `ops` must fit in the SQ ring in one shot — the
    /// caller is responsible for keeping chains within `sq_entries` (a link
    /// chain cannot be split across submits without dangling the last LINK).
    ///
    /// SAFETY: identical to `submit_batch` — all pointers in `ops` must stay
    /// valid until this call returns.
    pub unsafe fn submit_linked_wait(&self, ops: &[LinkedOp]) -> OnyxResult<Vec<UringOpResult>> {
        if ops.is_empty() {
            return Ok(Vec::new());
        }
        if ops.len() as u32 > self.sq_entries {
            return Err(OnyxError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "io_uring linked batch too large: {} ops > sq_entries {}",
                    ops.len(),
                    self.sq_entries
                ),
            )));
        }

        let mut ring = self.ring.lock();

        for (idx, lop) in ops.iter().enumerate() {
            let entry = build_sqe(&lop.op, idx as u64, lop.link_next);
            // SAFETY: see submit_batch — pointers outlive the CQE harvest below.
            let mut sub = ring.submission();
            if sub.push(&entry).is_err() {
                drop(sub);
                return Err(OnyxError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "io_uring submission queue full at op {} of {}",
                        idx,
                        ops.len()
                    ),
                )));
            }
        }

        let want = ops.len();
        ring.submit_and_wait(want).map_err(|e| {
            OnyxError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("io_uring submit_and_wait({want}) failed: {e}"),
            ))
        })?;

        let mut results = vec![UringOpResult { result: 0 }; ops.len()];
        let mut harvested = 0usize;
        let mut cq = ring.completion();
        cq.sync();
        for cqe in &mut cq {
            let idx = cqe.user_data() as usize;
            if idx >= results.len() {
                return Err(OnyxError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "io_uring CQE user_data {} out of range (batch size {})",
                        idx,
                        ops.len()
                    ),
                )));
            }
            results[idx] = UringOpResult {
                result: cqe.result(),
            };
            harvested += 1;
        }

        if harvested != ops.len() {
            return Err(OnyxError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "io_uring harvested {} CQEs, expected {}",
                    harvested,
                    ops.len()
                ),
            )));
        }

        Ok(results)
    }

    /// Pipelined submit: push a link chain (or any op group) and submit it
    /// WITHOUT waiting, so multiple batches' fsync chains can be in flight at
    /// once. Each op's `user_data` is `base_user_data | op_index` — the caller
    /// packs a batch id into the high bits (e.g. `batch_id << 32`) and recovers
    /// `(batch_id, op_index)` from the harvested CQE.
    ///
    /// Whole-chain-or-nothing: if the SQ ring lacks room for the full group, it
    /// pushes NOTHING and returns `Ok(false)` so the caller can `harvest` to free
    /// CQ/SQ space and retry. A half-pushed chain would dangle the last
    /// `IOSQE_IO_LINK`, so partial submission is never allowed.
    ///
    /// SAFETY: identical to `submit_batch` — pointers in `ops` must stay valid
    /// until the matching CQEs are harvested (the caller keeps the backing
    /// buffers alive in its in-flight state until then).
    pub unsafe fn submit_linked_nowait(
        &self,
        ops: &[LinkedOp],
        base_user_data: u64,
    ) -> OnyxResult<bool> {
        if ops.is_empty() {
            return Ok(true);
        }
        if ops.len() as u32 > self.sq_entries {
            return Err(OnyxError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "io_uring linked batch too large: {} ops > sq_entries {}",
                    ops.len(),
                    self.sq_entries
                ),
            )));
        }
        // The packed user_data uses the low 32 bits for op index, so a single
        // chain must not exceed that — far above any real SQ ring.
        debug_assert!(ops.len() < (1usize << 32));

        let mut ring = self.ring.lock();
        {
            let sub = ring.submission();
            if sub.capacity() - sub.len() < ops.len() {
                return Ok(false);
            }
        }
        for (idx, lop) in ops.iter().enumerate() {
            let entry = build_sqe(&lop.op, base_user_data | idx as u64, lop.link_next);
            // SAFETY: see submit_batch — pointers outlive the CQE harvest.
            let mut sub = ring.submission();
            if sub.push(&entry).is_err() {
                // Capacity was checked above under the same lock, so this is
                // unreachable; bail without leaving a dangling LINK undriven.
                drop(sub);
                return Err(OnyxError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("io_uring SQ push failed mid-chain at op {idx}"),
                )));
            }
        }
        ring.submit().map_err(|e| {
            OnyxError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("io_uring submit() failed: {e}"),
            ))
        })?;
        Ok(true)
    }

    /// Drain completions for in-flight ops submitted via `submit_linked_nowait`.
    /// If `min_complete > 0`, block until at least that many CQEs are available
    /// (the caller must guarantee at least that many ops are in flight, else this
    /// blocks forever); if `0`, return whatever is ready without blocking.
    /// Returns `(user_data, raw_result)` per completion — `raw_result < 0` is
    /// `-errno`.
    pub fn harvest(&self, min_complete: usize) -> OnyxResult<Vec<(u64, i32)>> {
        let mut ring = self.ring.lock();
        if min_complete > 0 {
            ring.submit_and_wait(min_complete).map_err(|e| {
                OnyxError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("io_uring submit_and_wait({min_complete}) failed: {e}"),
                ))
            })?;
        }
        let mut out = Vec::new();
        let mut cq = ring.completion();
        cq.sync();
        for cqe in &mut cq {
            out.push((cqe.user_data(), cqe.result()));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::aligned::AlignedBuf;
    use std::os::fd::AsRawFd;

    #[test]
    fn submit_empty_batch_is_noop() {
        let session = IoUringSession::new(8).unwrap();
        let results = unsafe { session.submit_batch(&[]).unwrap() };
        assert!(results.is_empty());
    }

    #[test]
    fn round_trip_write_read() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmp.path())
            .unwrap();
        file.set_len(8192).unwrap();
        let fd = file.as_raw_fd();

        let session = IoUringSession::new(8).unwrap();

        let mut wbuf = AlignedBuf::new(4096, false).unwrap();
        for (i, b) in wbuf.as_mut_slice().iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        let write_op = UringOp::Write {
            fd,
            ptr: wbuf.as_ptr(),
            len: 4096,
            offset: 0,
        };
        let fsync_op = UringOp::FsyncDataBarrier { fd };
        let results = unsafe { session.submit_batch(&[write_op, fsync_op]).unwrap() };
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].bytes(), Some(4096));
        assert_eq!(results[1].result, 0, "fsync should return 0");

        let mut rbuf = AlignedBuf::new(4096, false).unwrap();
        let read_op = UringOp::Read {
            fd,
            ptr: rbuf.as_mut_ptr(),
            len: 4096,
            offset: 0,
        };
        let results = unsafe { session.submit_batch(&[read_op]).unwrap() };
        assert_eq!(results[0].bytes(), Some(4096));
        assert_eq!(rbuf.as_slice(), wbuf.as_slice());
    }

    #[test]
    fn batch_of_writes_then_drain_fsync() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmp.path())
            .unwrap();
        file.set_len(64 * 1024).unwrap();
        let fd = file.as_raw_fd();

        let session = IoUringSession::new(32).unwrap();

        let mut bufs: Vec<AlignedBuf> = (0..8)
            .map(|i| {
                let mut b = AlignedBuf::new(4096, false).unwrap();
                for byte in b.as_mut_slice() {
                    *byte = i as u8;
                }
                b
            })
            .collect();

        let mut ops: Vec<UringOp> = bufs
            .iter_mut()
            .enumerate()
            .map(|(i, buf)| UringOp::Write {
                fd,
                ptr: buf.as_ptr(),
                len: 4096,
                offset: i as u64 * 4096,
            })
            .collect();
        ops.push(UringOp::FsyncDataBarrier { fd });

        let results = unsafe { session.submit_batch(&ops).unwrap() };
        assert_eq!(results.len(), 9);
        for (i, r) in results.iter().take(8).enumerate() {
            assert_eq!(r.bytes(), Some(4096), "write {} should succeed", i);
        }
        assert_eq!(results[8].result, 0);

        // Read each back and verify.
        for i in 0..8 {
            let mut rbuf = AlignedBuf::new(4096, false).unwrap();
            let read = UringOp::Read {
                fd,
                ptr: rbuf.as_mut_ptr(),
                len: 4096,
                offset: i as u64 * 4096,
            };
            let r = unsafe { session.submit_batch(&[read]).unwrap() };
            assert_eq!(r[0].bytes(), Some(4096));
            assert!(rbuf.as_slice().iter().all(|b| *b == i as u8));
        }
    }

    #[test]
    fn batch_too_large_returns_error() {
        let session = IoUringSession::new(4).unwrap();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let fd = tmp.as_file().as_raw_fd();

        let mut buf = AlignedBuf::new(4096, false).unwrap();
        let ops: Vec<UringOp> = (0..16)
            .map(|i| UringOp::Write {
                fd,
                ptr: buf.as_mut_ptr(),
                len: 4096,
                offset: i * 4096,
            })
            .collect();
        let err = unsafe { session.submit_batch(&ops) }.unwrap_err();
        assert!(format!("{err}").contains("batch too large"));
    }

    /// `submit_linked_wait`: two IO_LINK-chained writes → terminal fsync, plus an
    /// independent (unlinked) checkpoint write. Mirrors the LV2 sync fast path.
    /// All five ops succeed, the data is on disk, and results are in input order.
    #[test]
    fn linked_writes_then_fsync_plus_unlinked_checkpoint() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmp.path())
            .unwrap();
        file.set_len(64 * 1024).unwrap();
        let fd = file.as_raw_fd();

        let session = IoUringSession::new(16).unwrap();

        let mut bufs: Vec<AlignedBuf> = (0..3)
            .map(|i| {
                let mut b = AlignedBuf::new(4096, false).unwrap();
                for byte in b.as_mut_slice() {
                    *byte = (10 + i) as u8;
                }
                b
            })
            .collect();
        let ckpt_off = 8 * 4096;

        // [W@0 (LINK), W@4096 (LINK), Fsync (terminal), W@ckpt (unlinked)]
        let ops = vec![
            LinkedOp {
                op: UringOp::Write {
                    fd,
                    ptr: bufs[0].as_ptr(),
                    len: 4096,
                    offset: 0,
                },
                link_next: true,
            },
            LinkedOp {
                op: UringOp::Write {
                    fd,
                    ptr: bufs[1].as_ptr(),
                    len: 4096,
                    offset: 4096,
                },
                link_next: true,
            },
            LinkedOp {
                op: UringOp::FsyncData { fd },
                link_next: false,
            },
            LinkedOp {
                op: UringOp::Write {
                    fd,
                    ptr: bufs[2].as_ptr(),
                    len: 4096,
                    offset: ckpt_off,
                },
                link_next: false,
            },
        ];

        let results = unsafe { session.submit_linked_wait(&ops).unwrap() };
        assert_eq!(results.len(), 4);
        assert_eq!(results[0].bytes(), Some(4096), "first linked write");
        assert_eq!(results[1].bytes(), Some(4096), "second linked write");
        assert_eq!(results[2].result, 0, "terminal fsync should succeed");
        assert_eq!(results[3].bytes(), Some(4096), "unlinked checkpoint write");

        // Verify every region is on disk with the right content.
        let _ = &mut bufs; // keep bufs alive until after harvest
        for (i, off) in [0u64, 4096, ckpt_off].into_iter().enumerate() {
            let mut rbuf = AlignedBuf::new(4096, false).unwrap();
            let r = unsafe {
                session
                    .submit_batch(&[UringOp::Read {
                        fd,
                        ptr: rbuf.as_mut_ptr(),
                        len: 4096,
                        offset: off,
                    }])
                    .unwrap()
            };
            assert_eq!(r[0].bytes(), Some(4096));
            assert!(rbuf.as_slice().iter().all(|b| *b == (10 + i) as u8));
        }
    }

    /// Pipelined path: two independent write→fsync chains submitted nowait, then
    /// harvested. Each op's `user_data` carries `(batch_id << 32) | op_idx`, both
    /// chains' data lands on disk, and every CQE reports success.
    #[test]
    fn nowait_two_chains_then_harvest() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmp.path())
            .unwrap();
        file.set_len(64 * 1024).unwrap();
        let fd = file.as_raw_fd();

        let session = IoUringSession::new(32).unwrap();
        let mut b0 = AlignedBuf::new(4096, false).unwrap();
        b0.as_mut_slice().fill(0xA1);
        let mut b1 = AlignedBuf::new(4096, false).unwrap();
        b1.as_mut_slice().fill(0xB2);

        // Chain for batch_id 0: write@0 (LINK) → fsync.
        let ops0 = vec![
            LinkedOp {
                op: UringOp::Write {
                    fd,
                    ptr: b0.as_ptr(),
                    len: 4096,
                    offset: 0,
                },
                link_next: true,
            },
            LinkedOp {
                op: UringOp::FsyncData { fd },
                link_next: false,
            },
        ];
        assert!(unsafe { session.submit_linked_nowait(&ops0, 0u64 << 32).unwrap() });

        // Chain for batch_id 1: write@4096 (LINK) → fsync.
        let ops1 = vec![
            LinkedOp {
                op: UringOp::Write {
                    fd,
                    ptr: b1.as_ptr(),
                    len: 4096,
                    offset: 4096,
                },
                link_next: true,
            },
            LinkedOp {
                op: UringOp::FsyncData { fd },
                link_next: false,
            },
        ];
        assert!(unsafe { session.submit_linked_nowait(&ops1, 1u64 << 32).unwrap() });

        // Harvest all 4 CQEs (2 per chain).
        let mut got: Vec<(u64, i32)> = Vec::new();
        while got.len() < 4 {
            got.extend(session.harvest(1).unwrap());
        }
        for (ud, res) in &got {
            assert!(*res >= 0, "op ud={ud} failed res={res}");
        }
        let batch_ids: std::collections::HashSet<u64> =
            got.iter().map(|(ud, _)| ud >> 32).collect();
        assert!(batch_ids.contains(&0) && batch_ids.contains(&1));

        for (off, val) in [(0u64, 0xA1u8), (4096, 0xB2)] {
            let mut rbuf = AlignedBuf::new(4096, false).unwrap();
            let r = unsafe {
                session
                    .submit_batch(&[UringOp::Read {
                        fd,
                        ptr: rbuf.as_mut_ptr(),
                        len: 4096,
                        offset: off,
                    }])
                    .unwrap()
            };
            assert_eq!(r[0].bytes(), Some(4096));
            assert!(rbuf.as_slice().iter().all(|b| *b == val));
        }
        let _ = (&b0, &b1);
    }

    /// `submit_linked_nowait` is whole-chain-or-nothing: if the SQ ring lacks
    /// room for the full chain it pushes nothing and returns `Ok(false)` rather
    /// than dangling a trailing IO_LINK. A chain longer than the ring is a hard
    /// error.
    #[test]
    fn nowait_chain_larger_than_ring_errors() {
        let session = IoUringSession::new(4).unwrap();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let buf = AlignedBuf::new(4096, false).unwrap();
        let ops: Vec<LinkedOp> = (0..8)
            .map(|i| LinkedOp {
                op: UringOp::Write {
                    fd: tmp.as_file().as_raw_fd(),
                    ptr: buf.as_ptr(),
                    len: 4096,
                    offset: i * 4096,
                },
                link_next: i < 7,
            })
            .collect();
        let err = unsafe { session.submit_linked_nowait(&ops, 0) }.unwrap_err();
        assert!(format!("{err}").contains("too large"));
    }

    /// A failed op in a link chain cancels the remaining linked ops with
    /// `-ECANCELED`, and every cancelled op still posts a CQE (so the harvested
    /// count matches and the failure surfaces per-op). Here the first write
    /// targets a read-only fd → `EBADF`, so the linked fsync is cancelled. This
    /// is what guarantees the LV2 sync path never falsely advances durability.
    #[test]
    fn failed_linked_write_cancels_fsync() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        // Read-only fd: writing through it fails with EBADF at completion.
        let ro = std::fs::OpenOptions::new()
            .read(true)
            .open(tmp.path())
            .unwrap();
        let ro_fd = ro.as_raw_fd();

        let session = IoUringSession::new(8).unwrap();
        let buf = AlignedBuf::new(4096, false).unwrap();

        let ops = vec![
            LinkedOp {
                op: UringOp::Write {
                    fd: ro_fd,
                    ptr: buf.as_ptr(),
                    len: 4096,
                    offset: 0,
                },
                link_next: true,
            },
            LinkedOp {
                op: UringOp::FsyncData { fd: ro_fd },
                link_next: false,
            },
        ];

        let results = unsafe { session.submit_linked_wait(&ops).unwrap() };
        assert_eq!(results.len(), 2, "both ops post CQEs even when cancelled");
        assert!(
            results[0].errno().is_some(),
            "write to read-only fd must fail, got {:?}",
            results[0].result
        );
        assert_eq!(
            results[1].errno(),
            Some(nix::libc::ECANCELED),
            "linked fsync must be cancelled after the failed write"
        );
    }
}
