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

    /// Whether the backend requires block-aligned offsets/lengths and uses the
    /// O_DIRECT-style on-disk encoding. Drives the buffer commit-log's compact
    /// "flushed" marker format ([`crate::buffer`]'s `mark_entry_flushed`): a
    /// direct backend writes a full aligned block, a buffered one a sub-block
    /// header. Defaults to `true` (aligned) — the safe choice for block devices
    /// and a chunklet LD (which is always 4 KiB-aligned). `RawDevice` overrides
    /// it with its actual `O_DIRECT` status (regular-file test backends fall
    /// back to buffered IO).
    fn direct_io(&self) -> bool {
        true
    }

    /// `Some((fd, base_offset))` when this backend is a single OS file / block
    /// device whose fd can be handed to `io_uring` SQE construction; `None`
    /// for striped/RAID backends (chunklet), which forces callers onto the
    /// synchronous `*_many_at` + `flush` path.
    fn uring_target(&self) -> Option<(RawFd, u64)> {
        None
    }

    /// RAID full-stripe width in **blocks** — the write-alignment granularity
    /// the flush writer targets to hit a parity backend's zero-RMW full-stripe
    /// path. `1` (the default) means "no stripe constraint": single files, block
    /// devices, and non-parity chunklet LDs (mirror/plain/raid0) impose no
    /// alignment beyond one block, so the writer's stripe padding is a no-op.
    /// A chunklet RAID5/6 LD overrides this with `full_stripe_bytes / block_size`
    /// (e.g. 6 for a 6+2 RAID6 at a 4 KiB strip).
    fn stripe_blocks(&self) -> u32 {
        1
    }

    /// Human-readable identifier for error/log messages (a device path for a
    /// `RawDevice`, an LD label for chunklet).
    fn label(&self) -> String {
        "block-device".to_string()
    }
}

/// `RawDevice` *is* a `BlockBackend`: a single OS file or block device. This
/// preserves today's behaviour for non-chunklet deployments (sparse-file
/// tests, single-disk dev, kernel md/LVM) and lets `&RawDevice` coerce to
/// `&dyn BlockBackend` at the many superblock / startup call sites. Because it
/// is a single fd, `uring_target` returns `Some`, keeping the existing onyx
/// `io_uring` LV2/LV3 hot paths untouched.
impl BlockBackend for RawDevice {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> OnyxResult<()> {
        RawDevice::read_at(self, buf, offset)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> OnyxResult<()> {
        RawDevice::write_at(self, buf, offset)
    }

    fn flush(&self) -> OnyxResult<()> {
        self.sync()
    }

    fn size(&self) -> u64 {
        RawDevice::size(self)
    }

    fn uring_target(&self) -> Option<(RawFd, u64)> {
        Some((self.as_raw_fd(), self.base_offset()))
    }

    fn direct_io(&self) -> bool {
        RawDevice::is_direct_io(self)
    }

    fn label(&self) -> String {
        self.path().display().to_string()
    }
}

/// A fixed `[base, base+len)` window over another `BlockBackend`, translating
/// every offset into the inner backend's address space. This replaces
/// [`RawDevice::slice`] for the device-seam abstraction: the LV2 buffer pool
/// carves per-shard sub-views (data area + checkpoint block) out of one root
/// backend, which may be a single file/blockdev *or* one chunklet `LogicalDisk`
/// shared by every shard.
///
/// `uring_target` adds the window base to the inner fd's base offset, so a
/// `RawDevice`-backed slice reports exactly the `(fd, shard_offset)` the old
/// `RawDevice::slice` did — keeping the io_uring LV2 hot path byte-for-byte
/// identical. A chunklet-backed slice inherits `None` and forces the
/// synchronous `*_many_at` + `flush` path.
pub struct BackendSlice {
    inner: Arc<dyn BlockBackend>,
    base: u64,
    len: u64,
}

impl BackendSlice {
    /// Build a window over `inner`. Errors if `base + len` overflows or exceeds
    /// the inner backend's size (mirrors `RawDevice::slice` bounds checks).
    pub fn new(inner: Arc<dyn BlockBackend>, base: u64, len: u64) -> OnyxResult<Self> {
        let end = base
            .checked_add(len)
            .ok_or_else(|| crate::error::OnyxError::Config(format!(
                "backend slice overflow: base={base} len={len}"
            )))?;
        if end > inner.size() {
            return Err(crate::error::OnyxError::Config(format!(
                "backend slice out of bounds: base={base} len={len} inner_size={}",
                inner.size()
            )));
        }
        Ok(Self { inner, base, len })
    }

    fn translate(&self, offset: u64, len: usize) -> OnyxResult<u64> {
        let end = offset
            .checked_add(len as u64)
            .ok_or_else(|| crate::error::OnyxError::Config(format!(
                "backend slice IO overflow: offset={offset} len={len}"
            )))?;
        if end > self.len {
            return Err(crate::error::OnyxError::Config(format!(
                "out-of-bounds slice IO: offset={offset} len={len} window={}",
                self.len
            )));
        }
        Ok(self.base + offset)
    }
}

impl BlockBackend for BackendSlice {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> OnyxResult<()> {
        let abs = self.translate(offset, buf.len())?;
        self.inner.read_at(buf, abs)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> OnyxResult<()> {
        let abs = self.translate(offset, buf.len())?;
        self.inner.write_at(buf, abs)
    }

    fn read_many_at(&self, ops: &mut [(u64, &mut [u8])]) -> OnyxResult<()> {
        // Translate each window-relative offset to the inner address space,
        // then fan the whole batch through the inner backend in one call so a
        // chunklet inner still parallelises across PDs.
        let mut abs_ops: Vec<(u64, &mut [u8])> = Vec::with_capacity(ops.len());
        for (offset, buf) in ops.iter_mut() {
            let abs = self.translate(*offset, buf.len())?;
            abs_ops.push((abs, &mut **buf));
        }
        self.inner.read_many_at(&mut abs_ops)
    }

    fn write_many_at(&self, ops: &[(u64, &[u8])]) -> OnyxResult<()> {
        let mut abs_ops: Vec<(u64, &[u8])> = Vec::with_capacity(ops.len());
        for (offset, buf) in ops {
            let abs = self.translate(*offset, buf.len())?;
            abs_ops.push((abs, *buf));
        }
        self.inner.write_many_at(&abs_ops)
    }

    fn flush(&self) -> OnyxResult<()> {
        self.inner.flush()
    }

    fn size(&self) -> u64 {
        self.len
    }

    fn uring_target(&self) -> Option<(RawFd, u64)> {
        self.inner
            .uring_target()
            .map(|(fd, inner_base)| (fd, inner_base + self.base))
    }

    fn stripe_blocks(&self) -> u32 {
        self.inner.stripe_blocks()
    }

    fn direct_io(&self) -> bool {
        self.inner.direct_io()
    }

    fn label(&self) -> String {
        format!("{}[+{}:{}]", self.inner.label(), self.base, self.len)
    }
}

/// Convenience constructor: a windowed sub-view as an `Arc<dyn BlockBackend>`,
/// the form every per-shard slice consumer wants.
pub fn slice_backend(
    inner: Arc<dyn BlockBackend>,
    base: u64,
    len: u64,
) -> OnyxResult<Arc<dyn BlockBackend>> {
    Ok(Arc::new(BackendSlice::new(inner, base, len)?))
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
    /// Keeps the owning `Pool` alive for the backend's lifetime (the LD holds
    /// its member PD `Arc`s, but the Pool owns background/management state and
    /// is the handle the Phase-4 ops surface needs). `None` when the caller
    /// manages the pool lifetime itself (tests).
    pool: Option<Arc<onyx_chunklet::Pool>>,
}

impl ChunkletBackend {
    pub fn new(ld: Arc<dyn LogicalDisk>) -> Self {
        let capacity = ld.capacity_bytes();
        Self {
            ld,
            capacity,
            pool: None,
        }
    }

    /// Build a backend that also keeps its owning `Pool` alive.
    pub fn with_pool(ld: Arc<dyn LogicalDisk>, pool: Arc<onyx_chunklet::Pool>) -> Self {
        let mut b = Self::new(ld);
        b.pool = Some(pool);
        b
    }

    /// Borrow the underlying logical disk (e.g. to read `strip_size()` for
    /// packer alignment).
    pub fn logical_disk(&self) -> &Arc<dyn LogicalDisk> {
        &self.ld
    }

    /// The owning pool, when this backend keeps it alive. The Phase-4 ops
    /// surface (status / rebuild / scrub / replace-disk) routes through here.
    pub fn pool(&self) -> Option<&Arc<onyx_chunklet::Pool>> {
        self.pool.as_ref()
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

    fn stripe_blocks(&self) -> u32 {
        // `LdRaid5/6::strip_size()` already returns full_stripe_bytes (data
        // strips × strip), so full_stripe_blocks = strip_size / block_size.
        // Mirror/plain/raid0 report their block/strip size => 1..N with no
        // parity, and their `write_full_stripe` has no RMW to avoid, so the
        // writer's stripe padding on them is harmless (usually just 1).
        let bs = self.ld.block_size() as u64;
        if bs == 0 {
            return 1;
        }
        let strip = self.ld.strip_size() as u64;
        u32::try_from((strip / bs).max(1)).unwrap_or(1)
    }

    fn label(&self) -> String {
        format!("chunklet-ld:{:?}", self.ld.id())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn backend(size: u64) -> (RawDevice, NamedTempFile) {
        let tmp = NamedTempFile::new().unwrap();
        let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
        (dev, tmp)
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

    #[test]
    fn backend_slice_windows_and_forwards_uring_base() {
        let (b, _tmp) = backend(64 * 1024);
        let root_base = b.uring_target().unwrap().1;
        let root: Arc<dyn BlockBackend> = Arc::new(b);

        // A 4 KiB window starting at offset 8192.
        let win = BackendSlice::new(root.clone(), 8192, 4096).unwrap();
        assert_eq!(win.size(), 4096);
        // uring_target forwards the inner fd with the window base folded in, so
        // the io_uring path builds the same absolute SQE offset RawDevice::slice
        // would have.
        let (root_fd, _) = root.uring_target().unwrap();
        let (win_fd, win_base) = win.uring_target().unwrap();
        assert_eq!(win_fd, root_fd);
        assert_eq!(win_base, root_base + 8192);

        // Writes land at the translated absolute offset.
        let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        win.write_at(&payload, 0).unwrap();
        win.flush().unwrap();
        let mut via_root = vec![0u8; 4096];
        root.read_at(&mut via_root, 8192).unwrap();
        assert_eq!(via_root, payload);

        // Out-of-window IO is rejected.
        let mut overflow = vec![0u8; 4096];
        assert!(win.read_at(&mut overflow, 1).is_err());
        assert!(BackendSlice::new(root, 63 * 1024, 4096).is_err());
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

    /// stripe_blocks() derives the RAID6 full-stripe width (in blocks) from the
    /// LD geometry: 3 data + 2 parity at a 4 KiB strip => 3-block (12 KiB)
    /// stripe. This is the alignment granularity the flush writer targets.
    #[test]
    fn chunklet_backend_stripe_blocks_raid6() {
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
        assert_eq!(backend.stripe_blocks(), 3);
    }

    /// A single file/blockdev is not striped => stripe_blocks() is 1, so the
    /// writer's stripe logic is a no-op off-chunklet.
    #[test]
    fn raw_device_stripe_blocks_is_one() {
        let (b, _tmp) = backend(64 * 1024);
        assert_eq!(b.stripe_blocks(), 1);
    }
}
