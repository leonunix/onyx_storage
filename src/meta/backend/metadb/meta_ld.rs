//! Meta-LD device path: metadb's whole persistent surface on a chunklet meta
//! LogicalDisk (RAID10), so a single disk failure no longer bricks the metadata.
//!
//! # Layout (all offsets 4 KiB-aligned, self-described by the OMET superblock)
//!
//! ```text
//! block 0          OMET superblock (magic, instance UUID, region offsets)
//! [4 KiB, +4 MiB)  volume-catalog slot A   ┐ A/B generational slots replace the
//! [+4 MiB, +8 MiB) volume-catalog slot B   ┘ host-FS tmp+rename atomic_write
//! [+8 MiB, +J)     lifecycle-journal ring  (metadb RingJournal window)
//! [+J, capacity)   page window             (metadb BlockPageDevice window)
//! ```
//!
//! `onyx` owns the superblock + catalog slots; metadb only ever sees the two
//! windows (page window, journal ring), addressed from 0. All device IO on this
//! path is synchronous batched writes + `flush` — io_uring lives inside chunklet.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use onyx_metadb::{
    BlockPageDevice, Config as MetaDbConfig, Db, JournalDevice, MetaDbError, PageBlockIo,
    PageDevice, RING_BLOCK_SIZE,
};

use onyx_chunklet::ld::LogicalDisk;

use crate::error::{OnyxError, OnyxResult};
use crate::io::block_backend::{BackendSlice, BlockBackend, ChunkletBackend};

use super::catalog::VolumeCatalog;

// ─────────────────────────── layout constants ───────────────────────────

const OMET_MAGIC: u32 = 0x4F4D_4554; // "OMET"
const OMET_VERSION: u32 = 1;
const OMET_HEADER_SIZE: usize = 128;
const SB_BYTES: u64 = 4096;
const CATALOG_SLOT_BYTES: u64 = 4 * 1024 * 1024;
const CATALOG_A_OFF: u64 = SB_BYTES;
const CATALOG_B_OFF: u64 = CATALOG_A_OFF + CATALOG_SLOT_BYTES;
const JOURNAL_OFF: u64 = CATALOG_B_OFF + CATALOG_SLOT_BYTES; // 4 KiB + 8 MiB
const FLAG_METADB_INIT: u64 = 1 << 0;
const MIN_JOURNAL_BYTES: u64 = 1024 * 1024; // 256 ring blocks
const MAX_JOURNAL_BYTES: u64 = 1024 * 1024 * 1024; // plan D5 production size
/// Read batches stay large to amortise recovery/prewarm submissions.
const MAX_DEVICE_READ_BYTES: usize = 4 * 1024 * 1024;
/// The production meta mirror uses a 128 KiB strip. A 4 MiB logical batch
/// therefore expands to 32 strips and 64 mirror writes, exactly filling
/// chunklet's per-thread io_uring depth.
const MAX_DEVICE_WRITE_BYTES: usize = 4 * 1024 * 1024;
/// Number of independent 4 MiB chunks allowed in flight for one checkpoint
/// page write. Chunklet's io_uring backend owns one depth-64 ring per calling
/// thread; a single caller therefore serialises thousands of mirrored 4 KiB
/// writes through one shallow ring. Thirty-two scoped workers provide up to
/// 2048 aggregate SQEs while keeping a fixed thread bound and each worker's
/// range-lock footprint at 32 stripes.
const MAX_PARALLEL_DEVICE_WRITES: usize = 32;
/// Volume-catalog A/B slot header: `generation(8) | payload_len(4) | crc32(4)`.
const CATALOG_SLOT_HEADER: usize = 16;

fn align_up(x: u64, a: u64) -> u64 {
    (x + a - 1) & !(a - 1)
}

fn meta_err(e: OnyxError) -> MetaDbError {
    MetaDbError::Io(std::io::Error::other(e.to_string()))
}

fn onyx_err(e: MetaDbError) -> OnyxError {
    OnyxError::MetaDb(e)
}

// ─────────────────────────── OMET superblock ───────────────────────────

/// Block-0 superblock: identifies the meta LD as metadb-initialised and records
/// every region offset (self-describing — boot never re-derives the layout from
/// config).
#[derive(Debug, Clone)]
struct MetaSuperblock {
    uuid: [u8; 16],
    flags: u64,
    catalog_a_off: u64,
    catalog_b_off: u64,
    catalog_slot_bytes: u64,
    journal_off: u64,
    journal_bytes: u64,
    pages_off: u64,
    pages_bytes: u64,
}

impl MetaSuperblock {
    /// Compute a fresh layout for a device of `capacity` bytes.
    fn new_fresh(capacity: u64) -> OnyxResult<Self> {
        let journal_bytes = align_up(
            (capacity / 16).clamp(MIN_JOURNAL_BYTES, MAX_JOURNAL_BYTES),
            RING_BLOCK_SIZE as u64,
        );
        let pages_off = JOURNAL_OFF + journal_bytes;
        if pages_off + SB_BYTES > capacity {
            return Err(OnyxError::Config(format!(
                "meta LD too small: {capacity} bytes cannot hold superblock + catalog \
                 (8 MiB) + journal ({journal_bytes} bytes) + a page window"
            )));
        }
        let pages_bytes = (capacity - pages_off) & !(SB_BYTES - 1);
        Ok(Self {
            uuid: generate_uuid(),
            flags: FLAG_METADB_INIT,
            catalog_a_off: CATALOG_A_OFF,
            catalog_b_off: CATALOG_B_OFF,
            catalog_slot_bytes: CATALOG_SLOT_BYTES,
            journal_off: JOURNAL_OFF,
            journal_bytes,
            pages_off,
            pages_bytes,
        })
    }

    fn to_block(&self) -> Vec<u8> {
        let mut buf = vec![0u8; SB_BYTES as usize];
        buf[0..4].copy_from_slice(&OMET_MAGIC.to_le_bytes());
        buf[4..8].copy_from_slice(&OMET_VERSION.to_le_bytes());
        buf[8..24].copy_from_slice(&self.uuid);
        buf[24..32].copy_from_slice(&self.flags.to_le_bytes());
        buf[32..40].copy_from_slice(&self.catalog_a_off.to_le_bytes());
        buf[40..48].copy_from_slice(&self.catalog_b_off.to_le_bytes());
        buf[48..56].copy_from_slice(&self.catalog_slot_bytes.to_le_bytes());
        buf[56..64].copy_from_slice(&self.journal_off.to_le_bytes());
        buf[64..72].copy_from_slice(&self.journal_bytes.to_le_bytes());
        buf[72..80].copy_from_slice(&self.pages_off.to_le_bytes());
        buf[80..88].copy_from_slice(&self.pages_bytes.to_le_bytes());
        // crc32 over the header with the crc slot (88..92) zeroed.
        let crc = crc32fast::hash(&buf[0..OMET_HEADER_SIZE]);
        buf[88..92].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    fn from_block(buf: &[u8]) -> Option<Self> {
        if buf.len() < OMET_HEADER_SIZE {
            return None;
        }
        if u32::from_le_bytes(buf[0..4].try_into().ok()?) != OMET_MAGIC {
            return None;
        }
        if u32::from_le_bytes(buf[4..8].try_into().ok()?) != OMET_VERSION {
            return None;
        }
        let stored_crc = u32::from_le_bytes(buf[88..92].try_into().ok()?);
        let mut crc_buf = [0u8; OMET_HEADER_SIZE];
        crc_buf.copy_from_slice(&buf[0..OMET_HEADER_SIZE]);
        crc_buf[88..92].fill(0);
        if crc32fast::hash(&crc_buf) != stored_crc {
            return None;
        }
        let mut uuid = [0u8; 16];
        uuid.copy_from_slice(&buf[8..24]);
        Some(Self {
            uuid,
            flags: u64::from_le_bytes(buf[24..32].try_into().ok()?),
            catalog_a_off: u64::from_le_bytes(buf[32..40].try_into().ok()?),
            catalog_b_off: u64::from_le_bytes(buf[40..48].try_into().ok()?),
            catalog_slot_bytes: u64::from_le_bytes(buf[48..56].try_into().ok()?),
            journal_off: u64::from_le_bytes(buf[56..64].try_into().ok()?),
            journal_bytes: u64::from_le_bytes(buf[64..72].try_into().ok()?),
            pages_off: u64::from_le_bytes(buf[72..80].try_into().ok()?),
            pages_bytes: u64::from_le_bytes(buf[80..88].try_into().ok()?),
        })
    }
}

/// Non-crypto UUID from the process/thread + a couple of stable stack addresses.
/// Metadb `Date::now`-free constraints do not apply here (onyx side); we just
/// need a stable-per-instance identifier for the superblock.
fn generate_uuid() -> [u8; 16] {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let pid = std::process::id() as u64;
    let mut out = [0u8; 16];
    out[0..8].copy_from_slice(&nanos.to_le_bytes());
    out[8..16].copy_from_slice(&(pid ^ nanos.rotate_left(17)).to_le_bytes());
    out
}

// ─────────────────────────── page / journal windows ───────────────────────────

/// `PageBlockIo` over the meta LD's page window. Byte offsets are window-relative
/// (`page_id * PAGE_SIZE`); the `BackendSlice` adds the window base. Large batches
/// are split into bounded read/write chunks so one `write_many_at` cannot pin the
/// LD's stripe locks across a whole checkpoint.
struct MetaWindow {
    slice: BackendSlice,
}

impl PageBlockIo for MetaWindow {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), MetaDbError> {
        self.slice.read_at(buf, offset).map_err(meta_err)
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<(), MetaDbError> {
        self.slice.write_at(buf, offset).map_err(meta_err)
    }

    fn read_many_at(&self, ops: &mut [(u64, &mut [u8])]) -> Result<(), MetaDbError> {
        let mut i = 0;
        while i < ops.len() {
            let mut j = i;
            let mut bytes = 0usize;
            while j < ops.len() && (j == i || bytes + ops[j].1.len() <= MAX_DEVICE_READ_BYTES) {
                bytes += ops[j].1.len();
                j += 1;
            }
            self.slice.read_many_at(&mut ops[i..j]).map_err(meta_err)?;
            i = j;
        }
        Ok(())
    }

    fn write_many_at(&self, ops: &[(u64, &[u8])]) -> Result<(), MetaDbError> {
        let batches = write_batch_ranges(ops);
        if batches.len() <= 1 {
            return self.slice.write_many_at(ops).map_err(meta_err);
        }

        let worker_count = batches.len().min(MAX_PARALLEL_DEVICE_WRITES);
        let failed = AtomicBool::new(false);
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(worker_count);
            for worker_idx in 0..worker_count {
                let failed = &failed;
                let batches = &batches;
                handles.push(scope.spawn(move || {
                    for batch_idx in (worker_idx..batches.len()).step_by(worker_count) {
                        if failed.load(Ordering::Acquire) {
                            break;
                        }
                        let range = batches[batch_idx].clone();
                        if let Err(error) = self.slice.write_many_at(&ops[range]) {
                            failed.store(true, Ordering::Release);
                            return Err(meta_err(error));
                        }
                    }
                    Ok(())
                }));
            }

            let mut first_error = None;
            for handle in handles {
                match handle.join().expect("metadb device write worker panicked") {
                    Ok(()) => {}
                    Err(error) if first_error.is_none() => first_error = Some(error),
                    Err(_) => {}
                }
            }
            first_error.map_or(Ok(()), Err)
        })
    }

    fn flush(&self) -> Result<(), MetaDbError> {
        self.slice.flush().map_err(meta_err)
    }

    fn capacity_bytes(&self) -> u64 {
        self.slice.size()
    }

    fn grow_capacity_bytes(&self, new_bytes: u64) -> Result<(), MetaDbError> {
        // The inner `ChunkletBackend` has already been swapped to the extended
        // LD, so the page-window slice can now widen up to the new device size.
        self.slice.grow_len(new_bytes).map_err(meta_err)
    }
}

fn write_batch_ranges(ops: &[(u64, &[u8])]) -> Vec<std::ops::Range<usize>> {
    let mut batches = Vec::new();
    let mut i = 0;
    while i < ops.len() {
        let mut j = i;
        let mut bytes = 0usize;
        while j < ops.len() && (j == i || bytes + ops[j].1.len() <= MAX_DEVICE_WRITE_BYTES) {
            bytes += ops[j].1.len();
            j += 1;
        }
        batches.push(i..j);
        i = j;
    }
    batches
}

/// `JournalDevice` over the meta LD's lifecycle-journal ring window.
struct JournalWindow {
    slice: BackendSlice,
    block_count: u64,
}

impl JournalWindow {
    fn new(slice: BackendSlice) -> Self {
        let block_count = slice.size() / RING_BLOCK_SIZE as u64;
        Self { slice, block_count }
    }
}

impl JournalDevice for JournalWindow {
    fn block_count(&self) -> u64 {
        self.block_count
    }

    fn read_block(&self, idx: u64, buf: &mut [u8]) -> Result<(), MetaDbError> {
        self.slice
            .read_at(buf, idx * RING_BLOCK_SIZE as u64)
            .map_err(meta_err)
    }

    fn write_block(&self, idx: u64, buf: &[u8]) -> Result<(), MetaDbError> {
        self.slice
            .write_at(buf, idx * RING_BLOCK_SIZE as u64)
            .map_err(meta_err)
    }

    fn flush(&self) -> Result<(), MetaDbError> {
        self.slice.flush().map_err(meta_err)
    }
}

// ─────────────────────────── volume-catalog A/B slots ───────────────────────────

/// The onyx volume catalog (volumes + snapshot registry) persisted to two
/// generational slots on the meta LD, replacing the host-FS tmp+rename write.
/// The higher-generation slot with a valid CRC wins on load; each persist writes
/// the *other* slot then flushes, so a torn write never loses the last-good
/// catalog.
pub(super) struct MetaLdCatalog {
    backend: Arc<dyn BlockBackend>,
    a_off: u64,
    b_off: u64,
    slot_bytes: u64,
    /// `(current_generation, next_write_targets_slot_a)`.
    state: Mutex<(u64, bool)>,
}

impl MetaLdCatalog {
    fn read_slot(&self, off: u64) -> OnyxResult<Option<(u64, Vec<u8>)>> {
        // Read just enough to cover the header, then (if a payload is present)
        // the aligned span holding it. Fresh slots read back all-zero → gen 0.
        let mut head = vec![0u8; SB_BYTES as usize];
        self.backend.read_at(&mut head, off)?;
        let gen = u64::from_le_bytes(head[0..8].try_into().unwrap());
        if gen == 0 {
            return Ok(None);
        }
        let len = u32::from_le_bytes(head[8..12].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(head[12..16].try_into().unwrap());
        if CATALOG_SLOT_HEADER + len > self.slot_bytes as usize {
            return Ok(None);
        }
        let span = align_up((CATALOG_SLOT_HEADER + len) as u64, SB_BYTES);
        let payload = if span as usize <= head.len() {
            head[CATALOG_SLOT_HEADER..CATALOG_SLOT_HEADER + len].to_vec()
        } else {
            let mut buf = vec![0u8; span as usize];
            self.backend.read_at(&mut buf, off)?;
            buf[CATALOG_SLOT_HEADER..CATALOG_SLOT_HEADER + len].to_vec()
        };
        if crc32fast::hash(&payload) != crc {
            return Ok(None); // torn slot
        }
        Ok(Some((gen, payload)))
    }

    /// Load whichever slot won and return the catalog alongside the handle.
    fn load(
        backend: Arc<dyn BlockBackend>,
        sb: &MetaSuperblock,
    ) -> OnyxResult<(Self, VolumeCatalog)> {
        let this = Self {
            backend,
            a_off: sb.catalog_a_off,
            b_off: sb.catalog_b_off,
            slot_bytes: sb.catalog_slot_bytes,
            state: Mutex::new((0, true)),
        };
        let a = this.read_slot(this.a_off)?;
        let b = this.read_slot(this.b_off)?;
        let (gen, payload, winner_is_a) = match (a, b) {
            (None, None) => (0u64, None, false),
            (Some((ga, pa)), None) => (ga, Some(pa), true),
            (None, Some((gb, pb))) => (gb, Some(pb), false),
            (Some((ga, pa)), Some((gb, pb))) => {
                if ga >= gb {
                    (ga, Some(pa), true)
                } else {
                    (gb, Some(pb), false)
                }
            }
        };
        let catalog = match payload {
            Some(p) => VolumeCatalog::decode(&p)?,
            None => VolumeCatalog::default(),
        };
        // Next write targets the OTHER slot from the winner (fresh → slot A).
        *this.state.lock() = (gen, !winner_is_a);
        Ok((this, catalog))
    }

    /// Persist `catalog` to the non-current slot, flush, then flip.
    pub(super) fn persist(&self, catalog: &VolumeCatalog) -> OnyxResult<()> {
        let payload = catalog.encode()?;
        if CATALOG_SLOT_HEADER + payload.len() > self.slot_bytes as usize {
            return Err(OnyxError::Config(format!(
                "metadb volume catalog is {} bytes, exceeds meta-LD slot capacity {}",
                CATALOG_SLOT_HEADER + payload.len(),
                self.slot_bytes
            )));
        }
        let mut st = self.state.lock();
        let (gen, write_a) = *st;
        let new_gen = gen + 1;
        let off = if write_a { self.a_off } else { self.b_off };
        let total = align_up((CATALOG_SLOT_HEADER + payload.len()) as u64, SB_BYTES) as usize;
        let mut buf = vec![0u8; total];
        buf[0..8].copy_from_slice(&new_gen.to_le_bytes());
        buf[8..12].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        buf[12..16].copy_from_slice(&crc32fast::hash(&payload).to_le_bytes());
        buf[CATALOG_SLOT_HEADER..CATALOG_SLOT_HEADER + payload.len()].copy_from_slice(&payload);
        self.backend.write_at(&buf, off)?;
        self.backend.flush()?;
        *st = (new_gen, !write_a);
        Ok(())
    }
}

// ─────────────────────────── open / create ───────────────────────────

/// Online-grow handle for the meta LD: retains the concrete `ChunkletBackend`
/// (for the `swap_ld` after `extend_ld`) and a mutable copy of the OMET
/// superblock so the page-window size can be re-derived + rewritten in place.
pub(super) struct MetaLdGrower {
    backend: Arc<ChunkletBackend>,
    sb: Mutex<MetaSuperblock>,
}

impl MetaLdGrower {
    pub(super) fn io_scheduler(
        &self,
    ) -> Option<Arc<crate::io::block_backend::ChunkletIoScheduler>> {
        self.backend.io_scheduler()
    }

    /// Propagate an online meta-LD extend: swap the extended LD into the shared
    /// `ChunkletBackend` (so all windows see the larger device), then rewrite the
    /// OMET superblock's page-window size and widen the metadb page device to
    /// match — lifting metadb's `CapacityExhausted` ceiling. Grow-only: a swap to
    /// an LD that does not enlarge the page window is a no-op past the swap.
    pub(super) fn grow(&self, db: &Db, new_ld: Arc<dyn LogicalDisk>) -> OnyxResult<()> {
        let new_cap = new_ld.capacity_bytes();
        // 1) Install the extended LD. In-flight meta IO finishes on the old
        //    handle; the page-window `BackendSlice` (same inner `ChunkletBackend`)
        //    can now address up to `new_cap`.
        self.backend.swap_ld(new_ld);
        // 2) Recompute + persist the page-window span. The catalog + journal live
        //    at fixed lower offsets and are untouched; only the tail page window
        //    grows.
        let mut sb = self.sb.lock();
        let new_pages_bytes = new_cap.saturating_sub(sb.pages_off) & !(SB_BYTES - 1);
        if new_pages_bytes <= sb.pages_bytes {
            return Ok(());
        }
        sb.pages_bytes = new_pages_bytes;
        self.backend.write_at(&sb.to_block(), 0)?;
        self.backend.flush()?;
        drop(sb);
        // 3) Widen the metadb page device (grows the page-window slice + the
        //    BlockPageDevice ceiling), so stalled commits resume.
        db.grow_device_capacity(new_pages_bytes).map_err(onyx_err)?;
        tracing::info!(
            new_ld_capacity = new_cap,
            new_pages_bytes,
            "meta LD online grow: page window widened"
        );
        Ok(())
    }
}

/// Everything the metadb backend needs once its store is on the meta LD.
pub(super) struct MetaLd {
    pub(super) db: Arc<Db>,
    pub(super) catalog_store: MetaLdCatalog,
    pub(super) catalog: VolumeCatalog,
    pub(super) grower: MetaLdGrower,
}

/// The superblock plus the two device-generic windows metadb ever sees,
/// shared by the live open path ([`open_or_create`]) and the offline audit
/// path ([`open_for_offline_audit`]).
struct MetaDeviceParts {
    sb: MetaSuperblock,
    fresh: bool,
    backend_dyn: Arc<dyn BlockBackend>,
    page_device: Arc<dyn PageDevice>,
    journal_device: Arc<dyn JournalDevice>,
}

/// Read (or, if `create_if_missing`, first-time initialise) the OMET
/// superblock on `backend` and frame the page + journal windows over it.
/// `create_if_missing = false` is the audit-tool contract: a meta LD that was
/// never initialised is reported as "nothing to audit", never silently
/// stamped with a fresh superblock.
fn open_device_parts(
    backend: &Arc<ChunkletBackend>,
    create_if_missing: bool,
) -> OnyxResult<MetaDeviceParts> {
    let capacity = backend.size();
    if capacity < JOURNAL_OFF + MIN_JOURNAL_BYTES + SB_BYTES {
        return Err(OnyxError::Config(format!(
            "meta LD is only {capacity} bytes — too small for the metadb layout"
        )));
    }

    let mut sb_buf = vec![0u8; SB_BYTES as usize];
    backend.read_at(&mut sb_buf, 0)?;
    let (sb, fresh) = match MetaSuperblock::from_block(&sb_buf) {
        Some(sb) if sb.flags & FLAG_METADB_INIT != 0 => (sb, false),
        _ if create_if_missing => {
            let sb = MetaSuperblock::new_fresh(capacity)?;
            backend.write_at(&sb.to_block(), 0)?;
            backend.flush()?;
            tracing::info!(
                capacity,
                journal_bytes = sb.journal_bytes,
                pages_off = sb.pages_off,
                pages_bytes = sb.pages_bytes,
                "initialised fresh metadb layout on meta LD"
            );
            (sb, true)
        }
        _ => {
            return Err(OnyxError::Config(
                "no metadb found on this meta LD — nothing to audit".into(),
            ));
        }
    };

    // Upcast clones for the byte-window slices + catalog; the grower keeps the
    // concrete `Arc<ChunkletBackend>` so an online extend can `swap_ld` it.
    let backend_dyn: Arc<dyn BlockBackend> = backend.clone();
    let pages_slice = BackendSlice::new(backend_dyn.clone(), sb.pages_off, sb.pages_bytes)?;
    let journal_slice = BackendSlice::new(backend_dyn.clone(), sb.journal_off, sb.journal_bytes)?;
    let page_io: Arc<dyn PageBlockIo> = Arc::new(MetaWindow { slice: pages_slice });
    let page_device: Arc<dyn PageDevice> =
        Arc::new(BlockPageDevice::new(page_io).map_err(onyx_err)?);
    let journal_device: Arc<dyn JournalDevice> = Arc::new(JournalWindow::new(journal_slice));

    Ok(MetaDeviceParts {
        sb,
        fresh,
        backend_dyn,
        page_device,
        journal_device,
    })
}

/// Open (or first-time create) metadb on the meta LD `backend` (the concrete
/// chunklet backend, so an online extend can `swap_ld` it later). Reads /
/// initialises the OMET superblock, frames the page + journal windows, and
/// routes to `Db::open_on_device` / `Db::create_on_device`.
pub(super) fn open_or_create(
    backend: Arc<ChunkletBackend>,
    db_config: MetaDbConfig,
) -> OnyxResult<MetaLd> {
    let parts = open_device_parts(&backend, true)?;

    let db = if parts.fresh {
        Db::create_on_device(db_config, parts.page_device, parts.journal_device)
            .map_err(onyx_err)?
    } else {
        Db::open_on_device(db_config, parts.page_device, parts.journal_device).map_err(onyx_err)?
    };

    let (catalog_store, catalog) = MetaLdCatalog::load(parts.backend_dyn, &parts.sb)?;
    let grower = MetaLdGrower {
        backend,
        sb: Mutex::new(parts.sb),
    };
    Ok(MetaLd {
        db,
        catalog_store,
        catalog,
        grower,
    })
}

/// Open metadb on the meta LD `backend` for an offline audit. This never stamps
/// a fresh superblock (an uninitialised LD is an error), skips the Onyx volume
/// catalog/grower machinery, and forcibly disables MetaDB's continuous
/// background mutators.
///
/// This is intentionally not called "read-only": `Db::open_on_device` can
/// replay lifecycle records and persist a recovery commit. The chunklet pool
/// was also opened before this function and may have reconciled its own state.
/// Those one-time recovery writes are required for a coherent audit view and
/// remain part of the offline-only contract.
pub(super) fn open_for_offline_audit(
    backend: Arc<ChunkletBackend>,
    mut db_config: MetaDbConfig,
) -> OnyxResult<Arc<Db>> {
    // Defend this lowest audit-only entry point as well as its current callers:
    // a future tool cannot accidentally pass production background-worker
    // settings and mutate the store for the lifetime of a point query.
    super::sanitize_offline_audit_config(&mut db_config);
    let parts = open_device_parts(&backend, false)?;
    debug_assert!(
        !parts.fresh,
        "open_device_parts(create_if_missing=false) never returns fresh"
    );
    Db::open_on_device(db_config, parts.page_device, parts.journal_device).map_err(onyx_err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    struct RecordingBackend {
        size: u64,
        calls: AtomicUsize,
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
    }

    impl RecordingBackend {
        fn new(size: u64) -> Self {
            Self {
                size,
                calls: AtomicUsize::new(0),
                in_flight: AtomicUsize::new(0),
                max_in_flight: AtomicUsize::new(0),
            }
        }

        fn record_write(&self, bytes: usize) {
            assert!(bytes <= MAX_DEVICE_WRITE_BYTES);
            self.calls.fetch_add(1, Ordering::Relaxed);
            let current = self.in_flight.fetch_add(1, Ordering::AcqRel) + 1;
            self.max_in_flight.fetch_max(current, Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(10));
            self.in_flight.fetch_sub(1, Ordering::AcqRel);
        }
    }

    impl BlockBackend for RecordingBackend {
        fn read_at(&self, buf: &mut [u8], _offset: u64) -> OnyxResult<()> {
            buf.fill(0);
            Ok(())
        }

        fn write_at(&self, buf: &[u8], _offset: u64) -> OnyxResult<()> {
            self.record_write(buf.len());
            Ok(())
        }

        fn write_many_at(&self, ops: &[(u64, &[u8])]) -> OnyxResult<()> {
            self.record_write(ops.iter().map(|(_, buf)| buf.len()).sum());
            Ok(())
        }

        fn flush(&self) -> OnyxResult<()> {
            Ok(())
        }

        fn size(&self) -> u64 {
            self.size
        }
    }

    #[test]
    fn checkpoint_page_batches_are_written_with_bounded_parallelism() {
        const PAGE_BYTES: usize = 4096;
        const BATCH_COUNT: usize = MAX_PARALLEL_DEVICE_WRITES + 2;

        let page_count = BATCH_COUNT * (MAX_DEVICE_WRITE_BYTES / PAGE_BYTES);
        let backend_size = (page_count * PAGE_BYTES) as u64;
        let backend = Arc::new(RecordingBackend::new(backend_size));
        let backend_dyn: Arc<dyn BlockBackend> = backend.clone();
        let window = MetaWindow {
            slice: BackendSlice::new(backend_dyn, 0, backend.size).unwrap(),
        };
        let page = [0xA5; PAGE_BYTES];
        let ops: Vec<(u64, &[u8])> = (0..page_count)
            .map(|idx| ((idx * PAGE_BYTES) as u64, page.as_slice()))
            .collect();

        window.write_many_at(&ops).unwrap();

        assert_eq!(backend.calls.load(Ordering::Relaxed), BATCH_COUNT);
        let max_in_flight = backend.max_in_flight.load(Ordering::Relaxed);
        assert!(max_in_flight > 1, "large checkpoint writes must overlap");
        assert!(
            max_in_flight <= MAX_PARALLEL_DEVICE_WRITES,
            "checkpoint write concurrency exceeded its bound: {max_in_flight}"
        );
    }
}
