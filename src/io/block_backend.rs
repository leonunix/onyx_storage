//! `BlockBackend` — the device seam shared by LV2 (write buffer) and LV3
//! (data) so they can sit on either a single OS file/block device or a
//! chunklet `LogicalDisk` (RAID over many PDs) without either layer caring
//! which.
//!
//! # Why this exists
//!
//! Today LV2/LV3 take a concrete [`RawDevice`] and, on Linux, drive their own
//! `io_uring` directly against its fd (`RawDevice::as_raw_fd`). A chunklet LD
//! is striped across multiple PDs, so there is no single fd to hand to
//! `io_uring` — and chunklet already owns the cross-PD `io_uring` batching
//! internally. The integration contract is therefore: **onyx hands buffers to
//! the backend; the backend owns whatever device-level batching exists.**
//!
//! `uring_target` is the discriminator. A file/blockdev backend returns
//! `Some((fd, base_off))` so the existing onyx `io_uring` hot path keeps
//! working with zero regression; a chunklet backend returns `None`, and the
//! caller falls back to the synchronous batched [`BlockBackend::read_many_at`]
//! / [`BlockBackend::write_many_at`] + [`BlockBackend::flush`] path (chunklet
//! parallelises the batch across PDs under the hood).
//!
//! # Concurrency
//!
//! Implementations are `Send + Sync` and every method takes `&self`: the
//! backend is shared behind an `Arc` across all LV2 sync threads / LV3
//! ReadPool workers. `RawDevice` is internally fd-per-call-safe (`pread`/
//! `pwrite` are positional), and a chunklet LD serialises only at the stripe
//! level, so concurrent callers do not need external coordination here.

use std::os::fd::RawFd;
use std::sync::Arc;

use onyx_chunklet::ld::LogicalDisk;

use crate::error::OnyxResult;
use crate::io::device::RawDevice;

/// Linear block device exposed to LV2/LV3. Offsets and lengths are byte
/// quantities; alignment to the underlying block size is the implementation's
/// concern (`RawDevice` bounce-buffers unaligned IO, a chunklet LD requires
/// 4 KiB alignment from the caller).
pub trait BlockBackend: Send + Sync {
    /// Read exactly `buf.len()` bytes starting at `offset`.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> OnyxResult<()>;

    /// Write exactly `buf.len()` bytes starting at `offset`. The write is not
    /// guaranteed crash-durable until a subsequent [`BlockBackend::flush`].
    fn write_at(&self, buf: &[u8], offset: u64) -> OnyxResult<()>;

    /// Read several independent `(offset, buf)` pairs. The default loops
    /// `read_at`; a chunklet backend overrides this to fan the batch across
    /// PDs in one submit.
    fn read_many_at(&self, ops: &mut [(u64, &mut [u8])]) -> OnyxResult<()> {
        for (offset, buf) in ops.iter_mut() {
            self.read_at(buf, *offset)?;
        }
        Ok(())
    }

    /// Write several independent `(offset, buf)` pairs. The default loops
    /// `write_at`; a chunklet backend overrides this to fan the batch across
    /// PDs in one submit. Durability still requires a following `flush`.
    fn write_many_at(&self, ops: &[(u64, &[u8])]) -> OnyxResult<()> {
        for (offset, buf) in ops {
            self.write_at(buf, *offset)?;
        }
        Ok(())
    }

    /// Make every prior `write_at` / `write_many_at` crash-durable. This is
    /// the persistence barrier LV2's ack-after-durable gate waits on.
    fn flush(&self) -> OnyxResult<()>;

    /// Total addressable bytes. For a chunklet LD this is `capacity_bytes()`
    /// (already net of per-chunklet headers and parity).
    fn size(&self) -> u64;

    /// `Some((fd, base_offset))` when this backend is a single OS file / block
    /// device whose fd can be handed to `io_uring` SQE construction; `None`
    /// for striped/RAID backends (chunklet), which forces callers onto the
    /// synchronous `*_many_at` + `flush` path.
    fn uring_target(&self) -> Option<(RawFd, u64)> {
        None
    }
}

/// `BlockBackend` over a single OS file or block device. Preserves today's
/// behaviour for non-chunklet deployments (sparse-file tests, single-disk dev,
/// kernel md/LVM): `uring_target` exposes the fd, so the existing onyx
/// `io_uring` LV2/LV3 hot paths are untouched.
pub struct RawDeviceBackend {
    dev: RawDevice,
}

impl RawDeviceBackend {
    pub fn new(dev: RawDevice) -> Self {
        Self { dev }
    }

    /// Borrow the wrapped device (e.g. for superblock formatting that still
    /// works against the concrete `RawDevice`).
    pub fn device(&self) -> &RawDevice {
        &self.dev
    }
}

impl BlockBackend for RawDeviceBackend {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> OnyxResult<()> {
        self.dev.read_at(buf, offset)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> OnyxResult<()> {
        self.dev.write_at(buf, offset)
    }

    fn flush(&self) -> OnyxResult<()> {
        self.dev.sync()
    }

    fn size(&self) -> u64 {
        self.dev.size()
    }

    fn uring_target(&self) -> Option<(RawFd, u64)> {
        Some((self.dev.as_raw_fd(), self.dev.base_offset()))
    }
}

/// `BlockBackend` over a chunklet [`LogicalDisk`] — the production RAID path.
///
/// All io_uring lives inside chunklet (its `UringBackend` fans a batch across
/// the LD's member PDs), so this backend exposes no fd: `uring_target` returns
/// `None`, steering LV2/LV3 onto the synchronous `*_many_at` + `flush` path.
/// `read_many_at` / `write_many_at` forward the whole batch to the LD so
/// chunklet can parallelise it in one submit. `size` is the LD's
/// `capacity_bytes` (already net of per-chunklet headers + parity).
pub struct ChunkletBackend {
    ld: Arc<dyn LogicalDisk>,
    capacity: u64,
}

impl ChunkletBackend {
    pub fn new(ld: Arc<dyn LogicalDisk>) -> Self {
        let capacity = ld.capacity_bytes();
        Self { ld, capacity }
    }

    /// Borrow the underlying logical disk (e.g. to read `strip_size()` for
    /// packer alignment).
    pub fn logical_disk(&self) -> &Arc<dyn LogicalDisk> {
        &self.ld
    }
}

impl BlockBackend for ChunkletBackend {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> OnyxResult<()> {
        // chunklet's argument order is (offset, buf); ours is (buf, offset).
        self.ld.read_at(offset, buf)?;
        Ok(())
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> OnyxResult<()> {
        self.ld.write_at(offset, buf)?;
        Ok(())
    }

    fn read_many_at(&self, ops: &mut [(u64, &mut [u8])]) -> OnyxResult<()> {
        // Signature matches chunklet's `LogicalDisk::read_many_at` exactly.
        self.ld.read_many_at(ops)?;
        Ok(())
    }

    fn write_many_at(&self, ops: &[(u64, &[u8])]) -> OnyxResult<()> {
        self.ld.write_many_at(ops)?;
        Ok(())
    }

    fn flush(&self) -> OnyxResult<()> {
        self.ld.flush()?;
        Ok(())
    }

    fn size(&self) -> u64 {
        self.capacity
    }
    // uring_target defaults to None: striped across PDs, no single fd.
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn backend(size: u64) -> (RawDeviceBackend, NamedTempFile) {
        let tmp = NamedTempFile::new().unwrap();
        let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
        (RawDeviceBackend::new(dev), tmp)
    }

    #[test]
    fn round_trip_single() {
        let (b, _tmp) = backend(64 * 1024);
        let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        b.write_at(&payload, 8192).unwrap();
        b.flush().unwrap();
        let mut got = vec![0u8; payload.len()];
        b.read_at(&mut got, 8192).unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn round_trip_many() {
        let (b, _tmp) = backend(64 * 1024);
        let a: Vec<u8> = vec![0xaa; 4096];
        let c: Vec<u8> = vec![0x55; 4096];
        b.write_many_at(&[(0, &a), (4096, &c)]).unwrap();
        b.flush().unwrap();
        let mut ga = vec![0u8; 4096];
        let mut gc = vec![0u8; 4096];
        {
            let mut ops: Vec<(u64, &mut [u8])> = vec![(0, &mut ga), (4096, &mut gc)];
            b.read_many_at(&mut ops).unwrap();
        }
        assert_eq!(ga, a);
        assert_eq!(gc, c);
    }

    #[test]
    fn size_and_uring_target() {
        let (b, _tmp) = backend(64 * 1024);
        assert_eq!(b.size(), 64 * 1024);
        // A file/blockdev backend exposes its fd for the io_uring hot path.
        assert!(b.uring_target().is_some());
    }

    /// End-to-end through the production RAID6 path: a real chunklet Pool over
    /// sparse files, an `LdRaid6` LD wrapped in `ChunkletBackend`. Proves the
    /// onyx → chunklet seam (arg-order flip, error mapping, flush, no fd).
    #[test]
    fn chunklet_backend_round_trip_raid6() {
        use onyx_chunklet::io::RawDevice as CkRaw;
        use onyx_chunklet::pool::LdSpec;
        use onyx_chunklet::{Pool, PoolConfig};

        let dir = tempfile::tempdir().unwrap();
        let mut raws = Vec::new();
        for i in 0..5 {
            let p = dir.path().join(format!("pd{i}"));
            raws.push(CkRaw::open_or_create(&p, 4 << 30).unwrap());
        }
        let pool = Pool::create(
            raws,
            PoolConfig {
                spare_pct: 0,
                ..Default::default()
            },
        )
        .unwrap();
        let ld_id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
        let ld = pool.open_ld(ld_id).unwrap();

        let backend = ChunkletBackend::new(ld);
        assert!(backend.uring_target().is_none(), "RAID LD must not expose a fd");
        assert!(backend.size() > 0);

        let payload: Vec<u8> = (0..(64 << 10)).map(|i| (i % 251) as u8).collect();
        backend.write_at(&payload, 0).unwrap();
        backend.flush().unwrap();
        let mut got = vec![0u8; payload.len()];
        backend.read_at(&mut got, 0).unwrap();
        assert_eq!(got, payload);
    }
}
