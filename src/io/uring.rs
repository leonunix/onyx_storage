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
            let entry = match op {
                UringOp::Read {
                    fd,
                    ptr,
                    len,
                    offset,
                } => opcode::Read::new(types::Fd(*fd), *ptr, *len)
                    .offset(*offset)
                    .build()
                    .user_data(idx as u64),
                UringOp::Write {
                    fd,
                    ptr,
                    len,
                    offset,
                } => opcode::Write::new(types::Fd(*fd), *ptr, *len)
                    .offset(*offset)
                    .build()
                    .user_data(idx as u64),
                UringOp::FsyncDataBarrier { fd } => opcode::Fsync::new(types::Fd(*fd))
                    .flags(types::FsyncFlags::DATASYNC)
                    .build()
                    .user_data(idx as u64)
                    .flags(squeue::Flags::IO_DRAIN),
                UringOp::FsyncData { fd } => opcode::Fsync::new(types::Fd(*fd))
                    .flags(types::FsyncFlags::DATASYNC)
                    .build()
                    .user_data(idx as u64),
            };

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
}
