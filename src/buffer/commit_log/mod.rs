use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, unbounded, Receiver, RecvTimeoutError, Sender, TryRecvError};
use dashmap::DashMap;
use parking_lot::RwLock;

use crate::buffer::entry::{BufferEntry, BUFFER_ENTRY_MAGIC, MAX_ENTRY_SIZE, MIN_ENTRY_SIZE};
use crate::error::{OnyxError, OnyxResult};
use crate::io::aligned::{round_up, AlignedBuf};
use crate::io::block_backend::{slice_backend, BlockBackend};
use crate::io::device::RawDevice;
use crate::io::uring::{IoUringSession, LinkedOp, UringOp, UringOpResult};
use crate::meta::schema::MAX_VOLUME_ID_BYTES;
use crate::metrics::{BufferShardSnapshot, EngineMetrics, ForegroundIoLease};
use crate::types::{Lba, BLOCK_SIZE};

const COMMIT_LOG_MAGIC: u32 = 0x4F43_4C47; // "OCLG"
const COMMIT_LOG_VERSION: u32 = 3;
const COMMIT_LOG_VERSION_V2: u32 = 2;
const LAYOUT_MIGRATION_MAGIC: u32 = 0x4F4D_4947; // "OMIG"
const LAYOUT_MIGRATION_VERSION: u32 = 1;
const LAYOUT_MIGRATION_EXTENSION_OFFSET: usize = 64;
const COMMIT_LOG_SUPERBLOCK_SIZE: u64 = 4096;
const MAX_SHARDS_ON_DISK: usize = 64;
/// DashMap internal shard count — high value reduces contention under many writers.
const DASHMAP_SHARDS: usize = 256;
/// Fixed conflict domains for append ordering. Requests that overlap at any
/// `(volume, LBA)` acquire at least one common stripe; unrelated requests stay
/// parallel even when they share one physical LV2 shard.
const APPEND_ORDER_STRIPES: usize = 16 * 1024;

const SHARD_CHECKPOINT_MAGIC: u32 = 0x5348_434B; // "SHCK"
const SHARD_CHECKPOINT_VERSION: u32 = 1;
const SHARD_CHECKPOINT_SIZE: u64 = 4096;
const PACKED_CHECKPOINT_MAGIC: u32 = 0x5043_4B54; // "PCKT"
const PACKED_CHECKPOINT_VERSION: u32 = 1;
const PACKED_CHECKPOINT_HEADER_SIZE: usize = 32;
const PACKED_CHECKPOINT_RECORD_SIZE: usize = 32;
const PACKED_CHECKPOINT_CRC_OFFSET: usize = 24;
const PACKED_CHECKPOINT_SLOT_COUNT: usize = 2;
const BACKPRESSURE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const BACKEND_THROTTLE_ARM_NS: u64 = 30_000_000;
const BACKEND_THROTTLE_RELEASE_NS: u64 = 500_000_000;
const STAGING_CHANNEL_CAPACITY: usize = 32 * 1024;
/// Bound one sync epoch per shard. The previous unbounded drain could pull
/// millions of staged 4K writes into one Vec under fio, multiplying memory
/// before the device had a chance to fsync the first batch.
const SYNC_BATCH_MAX_ENTRIES: usize = 4096;
const SYNC_BATCH_MAX_BYTES: usize = 64 * 1024 * 1024;
/// Online payload hydration read-ahead. 128 KiB matches the common coalesce
/// unit while keeping foreground read tail latency bounded.
const HYDRATE_BATCH_MAX_BYTES: usize = 128 * 1024;
static LV2_METRIC_CLOCK_START: OnceLock<Instant> = OnceLock::new();

fn lv2_metric_timestamp_ns(now: Instant) -> u64 {
    let start = *LV2_METRIC_CLOCK_START.get_or_init(|| now);
    (now.saturating_duration_since(start).as_nanos() as u64).saturating_add(1)
}

/// Coarse read-path filter for pending buffer entries. A read range whose
/// buckets are absent can skip the DashMap LBA index entirely. Collisions and
/// stale buckets are harmless false positives; false negatives are avoided by
/// installing buckets before publishing LBA index entries and removing them
/// only after the entry is fully retired.
const PENDING_LBA_BUCKET_BLOCKS: u64 = 256;

/// Default number of LV2 fdatasync chains a sync thread keeps in flight.
/// 2 overlaps batch N's flush with batch N+1's writes without unbounded SQ/CQ
/// pressure; deeper is opt-in via `buffer.lv2_sync_pipeline_depth`.
const LV2_SYNC_PIPELINE_DEPTH: usize = 2;

/// Default ZFS `zfs_commit_timeout_pct` analog: the LV2 OPEN batch seals after
/// `ema_write_latency * pct/100` (floored) of accumulation. 10% matches ZFS.
const LV2_COMMIT_TIMEOUT_PCT: u64 = 10;

#[derive(Debug, Clone, Copy)]
pub struct BufferRuntimeLimits {
    pub staging_channel_capacity: usize,
    pub sync_batch_max_entries: usize,
    pub sync_batch_max_bytes: usize,
    /// Prepared-batch channel depth for each global root-write lane. Zero keeps
    /// the legacy dynamic depth of one slot per buffer shard.
    pub lv2_prepared_queue_depth_per_lane: usize,
    pub throttle: ThrottleSettings,
    pub throttle_backend_debt: bool,
    /// Drain ring backpressure before taking the append-order stripes.
    pub prewait_ring_space_outside_order: bool,
    /// Resolved ZFS-style adaptive commit-window percent for the LV2 sync
    /// pipeline (>= 1). See `LV2_COMMIT_TIMEOUT_PCT`.
    pub lv2_commit_timeout_pct: u64,
    /// Resolved LV2 sync pipeline depth (>= 1). See `LV2_SYNC_PIPELINE_DEPTH`.
    pub lv2_sync_pipeline_depth: usize,
}

/// ZFS-style hyperbolic write throttle on LV2 buffer fill.
///
/// All zero (the `Default`) disables the throttle: append fast-paths without
/// any clock read or atomic CAS. Activating the throttle requires both
/// `min_pct > 0` AND `scale_us > 0`.
///
/// Curve: `delay_us = scale_us * (fill - min_pct) / (max_pct - fill)`, capped
/// at `cap_us`. At `fill = max_pct` the gate falls through to the existing
/// condvar-based hard backpressure path inside `BufferShard::reserve_append`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ThrottleSettings {
    /// Fill % below which append pays no throttle.
    pub min_pct: u8,
    /// Fill % at which the hyperbolic divisor approaches zero (delay → cap).
    /// 0 means "use 100".
    pub max_pct: u8,
    /// Scale coefficient (microseconds). 0 disables.
    pub scale_us: u64,
    /// Per-call delay cap (microseconds). 0 means "use 100_000 = 100 ms".
    pub cap_us: u64,
}

impl ThrottleSettings {
    /// Resolve a configured value into the runtime effective curve. Returns
    /// `None` when the throttle is fully disabled, so callers can branch out
    /// of the slow path early.
    pub(crate) fn resolved(&self) -> Option<ThrottleSettings> {
        if self.min_pct == 0 || self.scale_us == 0 {
            return None;
        }
        let max_pct = if self.max_pct == 0 { 100 } else { self.max_pct };
        if max_pct <= self.min_pct {
            return None;
        }
        let cap_us = if self.cap_us == 0 {
            100_000
        } else {
            self.cap_us
        };
        Some(ThrottleSettings {
            min_pct: self.min_pct,
            max_pct,
            scale_us: self.scale_us,
            cap_us,
        })
    }

    /// Hyperbolic curve evaluation at whole-percent precision. Kept as a small
    /// convenience wrapper for configuration tests; the runtime path uses
    /// [`Self::delay_us_for_fill_basis_points`] so narrow dirty windows do not
    /// collapse into a one-percent step function.
    #[cfg(test)]
    pub(crate) fn delay_us_for_fill(&self, fill_pct: u8) -> u64 {
        self.delay_us_for_fill_basis_points(u32::from(fill_pct).saturating_mul(100))
    }

    pub(crate) fn delay_us_for_fill_basis_points(&self, fill_basis_points: u32) -> u64 {
        let min_basis_points = u32::from(self.min_pct).saturating_mul(100);
        let max_basis_points = u32::from(self.max_pct).saturating_mul(100);
        if fill_basis_points <= min_basis_points {
            return 0;
        }
        if fill_basis_points >= max_basis_points {
            return self.cap_us;
        }
        let num = u128::from(fill_basis_points - min_basis_points);
        let den = u128::from(max_basis_points - fill_basis_points);
        let delay = num.saturating_mul(u128::from(self.scale_us)) / den.max(1);
        delay.min(u128::from(self.cap_us)) as u64
    }

    /// Map a saturated commit-executor queue onto the same pressure interval
    /// used by the physical-ring curve. One queued batch per worker is normal
    /// pipeline occupancy; a second full wave means the metadata backend is no
    /// longer keeping up and reaches the throttle ceiling.
    pub(crate) fn executor_debt_basis_points(
        &self,
        worker_count: u64,
        active_workers: u64,
        queue_depth: u64,
    ) -> u32 {
        let workers = worker_count.max(1);
        if active_workers < workers || queue_depth <= workers {
            return 0;
        }
        let min_basis_points = u64::from(self.min_pct).saturating_mul(100);
        let max_basis_points = u64::from(self.max_pct).saturating_mul(100);
        let debt = queue_depth.saturating_sub(workers).min(workers + 1);
        let span = max_basis_points.saturating_sub(min_basis_points);
        min_basis_points
            .saturating_add(span.saturating_mul(debt) / (workers + 1))
            .min(max_basis_points) as u32
    }
}

impl BufferRuntimeLimits {
    pub fn from_config(
        _durable_payload_limit: u64,
        staging_channel_capacity: usize,
        sync_batch_max_entries: usize,
        sync_batch_max_bytes: usize,
        lv2_prepared_queue_depth_per_lane: usize,
        lv2_sync_pipeline_depth: usize,
        lv2_commit_timeout_pct: u64,
    ) -> Self {
        let defaults = Self::default();
        Self {
            staging_channel_capacity: if staging_channel_capacity == 0 {
                defaults.staging_channel_capacity
            } else {
                staging_channel_capacity
            },
            sync_batch_max_entries: if sync_batch_max_entries == 0 {
                defaults.sync_batch_max_entries
            } else {
                sync_batch_max_entries
            },
            sync_batch_max_bytes: if sync_batch_max_bytes == 0 {
                defaults.sync_batch_max_bytes
            } else {
                sync_batch_max_bytes
            },
            lv2_prepared_queue_depth_per_lane,
            throttle: defaults.throttle,
            throttle_backend_debt: defaults.throttle_backend_debt,
            prewait_ring_space_outside_order: defaults.prewait_ring_space_outside_order,
            lv2_commit_timeout_pct: if lv2_commit_timeout_pct == 0 {
                defaults.lv2_commit_timeout_pct
            } else {
                lv2_commit_timeout_pct
            },
            lv2_sync_pipeline_depth: if lv2_sync_pipeline_depth == 0 {
                defaults.lv2_sync_pipeline_depth
            } else {
                lv2_sync_pipeline_depth
            },
        }
    }

    pub fn with_throttle(mut self, throttle: ThrottleSettings) -> Self {
        self.throttle = throttle;
        self
    }

    pub fn with_backend_debt_throttle(mut self, enabled: bool) -> Self {
        self.throttle_backend_debt = enabled;
        self
    }

    pub fn with_prewait_ring_space(mut self, enabled: bool) -> Self {
        self.prewait_ring_space_outside_order = enabled;
        self
    }
}

impl Default for BufferRuntimeLimits {
    fn default() -> Self {
        Self {
            staging_channel_capacity: STAGING_CHANNEL_CAPACITY,
            sync_batch_max_entries: SYNC_BATCH_MAX_ENTRIES,
            sync_batch_max_bytes: SYNC_BATCH_MAX_BYTES,
            lv2_prepared_queue_depth_per_lane: 0,
            throttle: ThrottleSettings::default(),
            throttle_backend_debt: false,
            prewait_ring_space_outside_order: false,
            lv2_commit_timeout_pct: LV2_COMMIT_TIMEOUT_PCT,
            lv2_sync_pipeline_depth: LV2_SYNC_PIPELINE_DEPTH,
        }
    }
}

/// LV2 fdatasync watermark + targeted wakeup registry. Producers (`append`)
/// park here until the sync thread fdatasync's their seq.
///
/// `advance` wakes ONLY the appenders whose seq is now durable; the ones still
/// waiting on a later (in-flight or still-OPEN) batch stay parked and
/// untouched. This mirrors ZFS's per-`zil_commit_waiter` cv signalling
/// (`zil_lwb_flush_vdevs_done` signals just the completed lwb's waiters). A
/// shared `notify_all` instead woke EVERY parked appender on the shard on every
/// fdatasync (with ~16 appenders/shard and a ~4-6 batch, ~60-75% of wakeups
/// were spurious re-parks), and forced them all to re-acquire one mutex to
/// re-check the predicate — a serialized herd per cycle. Worse, `notify_all`
/// synchronized the appenders into a lockstep convoy (wake-together →
/// IO-together → stage-together → park-together), the bursty sawtooth that
/// starved the sync thread between bursts. Targeted unpark removes both.
pub(crate) struct Lv2DurabilityWaiter {
    /// Monotonic max seq whose payload is fdatasync'd on LV2 for this shard.
    pub(crate) synced_seq: AtomicU64,
    /// Parked appenders, each tagged with the seq it waits for. Locked only for
    /// the brief register / drain critical sections, never across a `park()`.
    waiters: parking_lot::Mutex<Vec<SeqWaiter>>,
}

/// One parked appender, tagged with the seq it is waiting for.
struct SeqWaiter {
    seq: u64,
    wake: DurabilityWake,
}

enum DurabilityWake {
    Thread(Arc<DurabilityParker>),
    Channel(Sender<()>),
}

/// A parked appender's wake handle. `done` is set (Release) before `unpark` so a
/// spurious `park()` return can't strand the appender after `advance` has
/// removed it from the registry; the appender's Acquire load pairs with it.
struct DurabilityParker {
    done: AtomicBool,
    thread: thread::Thread,
}

impl Lv2DurabilityWaiter {
    pub(crate) fn new(initial: u64) -> Self {
        Self {
            synced_seq: AtomicU64::new(initial),
            waiters: parking_lot::Mutex::new(Vec::new()),
        }
    }

    /// Block until `synced_seq >= seq`. Returns the wait duration so the
    /// caller can attribute it to `buffer_append_wait_durable_ns`.
    pub(crate) fn wait_for(&self, seq: u64) -> Duration {
        // Fast path: already durable — no registration, no lock, no park.
        if self.synced_seq.load(Ordering::Acquire) >= seq {
            return Duration::ZERO;
        }
        let start = Instant::now();
        let parker = Arc::new(DurabilityParker {
            done: AtomicBool::new(false),
            thread: thread::current(),
        });
        {
            let mut waiters = self.waiters.lock();
            // Re-check under the lock: `advance` takes this same lock to drain,
            // so if it advanced past `seq` before we registered we observe the
            // new watermark here and skip parking. Closes the lost-wakeup race.
            if self.synced_seq.load(Ordering::Acquire) >= seq {
                return start.elapsed();
            }
            waiters.push(SeqWaiter {
                seq,
                wake: DurabilityWake::Thread(parker.clone()),
            });
        }
        // Park until `advance` unparks us. The re-check defends against a
        // spurious `park()` return, which leaves us registered so we re-park.
        loop {
            thread::park();
            if parker.done.load(Ordering::Acquire) || self.synced_seq.load(Ordering::Acquire) >= seq
            {
                break;
            }
        }
        start.elapsed()
    }

    /// Advance the watermark and wake exactly the appenders whose seq is now
    /// durable. Called by the sync thread after fdatasync covers the batch.
    pub(crate) fn advance(&self, max_seq: u64) {
        let prev = self.synced_seq.fetch_max(max_seq, Ordering::Release);
        if prev >= max_seq {
            return;
        }
        // Collect the now-durable waiters under the lock; unpark AFTER releasing
        // it so woken appenders never touch this lock on their way out.
        let mut wake = Vec::new();
        {
            let mut waiters = self.waiters.lock();
            let mut i = 0;
            while i < waiters.len() {
                if waiters[i].seq <= max_seq {
                    wake.push(waiters.swap_remove(i).wake);
                } else {
                    i += 1;
                }
            }
        }
        for waiter in wake {
            match waiter {
                DurabilityWake::Thread(parker) => {
                    parker.done.store(true, Ordering::Release);
                    parker.thread.unpark();
                }
                DurabilityWake::Channel(tx) => {
                    let _ = tx.try_send(());
                }
            }
        }
    }

    fn arm_channel(&self, seq: u64, tx: &Sender<()>) -> bool {
        if self.synced_seq.load(Ordering::Acquire) >= seq {
            return true;
        }
        let mut waiters = self.waiters.lock();
        if self.synced_seq.load(Ordering::Acquire) >= seq {
            return true;
        }
        waiters.push(SeqWaiter {
            seq,
            wake: DurabilityWake::Channel(tx.clone()),
        });
        false
    }
}

#[derive(Debug)]
pub struct PendingEntry {
    pub seq: u64,
    pub vol_id: String,
    pub start_lba: Lba,
    pub lba_count: u32,
    pub payload_crc32: u32,
    pub vol_created_at: u64,
    /// Physical source for an in-memory GC relocation. This intent is not
    /// encoded in LV2; recovered entries deliberately fall back to ordinary
    /// writes and a later defrag sweep can retry any lost relocation intent.
    pub relocation_source: Option<crate::space::extent::Extent>,
    /// Payload data. `None` for recovered entries whose payload hasn't been
    /// loaded from the buffer device yet (lazy hydration to avoid OOM).
    pub payload: Option<Arc<[u8]>>,
    pub disk_offset: u64,
    pub disk_len: u32,
    /// In-memory residency start used for starvation diagnostics.
    pub enqueued_at: Instant,
    /// Monotonic timestamp recorded immediately before this entry's LV2
    /// durability watermark is advanced. Zero for recovered entries and for
    /// backends that do not use the global multi-shard sync path.
    durability_advanced_at_ns: AtomicU64,
    /// Older buffered ranges overwritten by this entry at append time.
    /// Once this entry is durable in the commit log, these ranges can be
    /// retired immediately instead of waiting for the flusher to rediscover
    /// that they are stale.
    pub superseded_ranges: Vec<(u64, Lba, u32)>,
}

impl Clone for PendingEntry {
    fn clone(&self) -> Self {
        Self {
            seq: self.seq,
            vol_id: self.vol_id.clone(),
            start_lba: self.start_lba,
            lba_count: self.lba_count,
            payload_crc32: self.payload_crc32,
            vol_created_at: self.vol_created_at,
            relocation_source: self.relocation_source,
            payload: self.payload.clone(),
            disk_offset: self.disk_offset,
            disk_len: self.disk_len,
            enqueued_at: self.enqueued_at,
            durability_advanced_at_ns: AtomicU64::new(
                self.durability_advanced_at_ns.load(Ordering::Relaxed),
            ),
            superseded_ranges: self.superseded_ranges.clone(),
        }
    }
}

#[cfg(test)]
impl PendingEntry {
    pub(crate) fn test_entry(
        seq: u64,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        payload: Arc<[u8]>,
        vol_created_at: u64,
        relocation_source: Option<crate::space::extent::Extent>,
    ) -> Self {
        Self {
            seq,
            vol_id: vol_id.to_string(),
            start_lba,
            lba_count,
            payload_crc32: crc32fast::hash(&payload),
            vol_created_at,
            relocation_source,
            payload: Some(payload),
            disk_offset: 0,
            disk_len: 0,
            enqueued_at: Instant::now(),
            durability_advanced_at_ns: AtomicU64::new(0),
            superseded_ranges: Vec::new(),
        }
    }
}

/// Lightweight recovery metadata — no payload clone.
#[derive(Debug, Clone)]
pub struct RecoveredMeta {
    pub seq: u64,
    pub vol_id: String,
    pub start_lba: Lba,
    pub lba_count: u32,
    pub vol_created_at: u64,
}

/// Compact LBA key using Arc<str> to avoid per-insert String clones.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LbaKey {
    vol_id: Arc<str>,
    lba: Lba,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PendingBucketKey {
    vol_hash: u64,
    bucket: u64,
}

#[derive(Debug, Clone, Copy)]
struct LogRecord {
    seq: u64,
    disk_offset: u64,
    slot_count: u32,
}

struct PreparedAppend {
    vol_id: String,
    vid: Arc<str>,
    start_lba: Lba,
    lba_count: u32,
    payload: Arc<[u8]>,
    payload_crc32: u32,
    payload_len: u64,
    vol_created_at: u64,
    disk_len: u32,
    slot_count: u32,
    keys: Vec<LbaKey>,
}

#[derive(Debug, Clone, Copy)]
struct AppendReservation {
    seq: u64,
    write_offset: u64,
    stage_order: u64,
}

#[derive(Default)]
struct AppendStageTurn {
    next: parking_lot::Mutex<u64>,
    changed: parking_lot::Condvar,
}

#[repr(align(64))]
struct AppendOrderStripe {
    lock: parking_lot::Mutex<()>,
}

impl Default for AppendOrderStripe {
    fn default() -> Self {
        Self {
            lock: parking_lot::Mutex::new(()),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct GlobalSuperblock {
    shard_count: u32,
    version: u32,
}

impl GlobalSuperblock {
    fn new(shard_count: usize) -> Self {
        Self {
            shard_count: shard_count as u32,
            version: COMMIT_LOG_VERSION,
        }
    }

    fn is_v3(&self) -> bool {
        self.version >= COMMIT_LOG_VERSION
    }

    fn encode(&self) -> [u8; COMMIT_LOG_SUPERBLOCK_SIZE as usize] {
        let mut buf = [0u8; COMMIT_LOG_SUPERBLOCK_SIZE as usize];
        buf[0..4].copy_from_slice(&COMMIT_LOG_MAGIC.to_le_bytes());
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..12].copy_from_slice(&self.shard_count.to_le_bytes());
        let crc = crc32fast::hash(&buf[16..]);
        buf[12..16].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    fn decode(buf: &[u8; COMMIT_LOG_SUPERBLOCK_SIZE as usize]) -> Option<Self> {
        let magic = u32::from_le_bytes(buf[0..4].try_into().ok()?);
        if magic != COMMIT_LOG_MAGIC {
            return None;
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().ok()?);
        if version != COMMIT_LOG_VERSION && version != COMMIT_LOG_VERSION_V2 {
            return None;
        }
        let expected_crc = u32::from_le_bytes(buf[12..16].try_into().ok()?);
        let actual_crc = crc32fast::hash(&buf[16..]);
        if expected_crc != actual_crc {
            return None;
        }
        let shard_count = u32::from_le_bytes(buf[8..12].try_into().ok()?);
        if shard_count == 0 || shard_count as usize > MAX_SHARDS_ON_DISK {
            return None;
        }
        Some(Self {
            shard_count,
            version,
        })
    }
}

/// Crash-resumable handoff between two empty shard layouts. The marker replaces
/// the old superblock only after MetaDB covers every prior buffer sequence and
/// carries the local sequence floor until the new checkpoints and superblock
/// have both been published.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LayoutMigrationMarker {
    shard_count: u32,
    max_seq: u64,
}

impl LayoutMigrationMarker {
    fn new(shard_count: usize, max_seq: u64) -> Self {
        Self {
            shard_count: shard_count as u32,
            max_seq,
        }
    }

    fn encode(&self) -> [u8; COMMIT_LOG_SUPERBLOCK_SIZE as usize] {
        let mut buf = [0u8; COMMIT_LOG_SUPERBLOCK_SIZE as usize];
        buf[0..4].copy_from_slice(&LAYOUT_MIGRATION_MAGIC.to_le_bytes());
        buf[4..8].copy_from_slice(&LAYOUT_MIGRATION_VERSION.to_le_bytes());
        buf[8..12].copy_from_slice(&self.shard_count.to_le_bytes());
        buf[16..24].copy_from_slice(&self.max_seq.to_le_bytes());
        let crc = Self::crc(&buf);
        buf[12..16].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Encode a redundant migration record that is also a valid empty shard-0
    /// checkpoint. The checkpoint half remains a normal recovery boundary after
    /// the final superblock is published; its extension disappears naturally on
    /// the next checkpoint update, so it can never become a stale migration
    /// command once the pool starts accepting writes.
    fn encode_checkpoint(&self) -> [u8; SHARD_CHECKPOINT_SIZE as usize] {
        let checkpoint = ShardCheckpoint {
            head_offset: 0,
            tail_offset: 0,
            max_seq: self.max_seq,
            used_bytes: 0,
        };
        let mut buf = checkpoint.encode();
        let offset = LAYOUT_MIGRATION_EXTENSION_OFFSET;
        buf[offset..offset + 4].copy_from_slice(&LAYOUT_MIGRATION_MAGIC.to_le_bytes());
        buf[offset + 4..offset + 8].copy_from_slice(&LAYOUT_MIGRATION_VERSION.to_le_bytes());
        buf[offset + 8..offset + 12].copy_from_slice(&self.shard_count.to_le_bytes());
        buf[offset + 16..offset + 24].copy_from_slice(&self.max_seq.to_le_bytes());
        let crc = Self::checkpoint_crc(&buf);
        buf[offset + 12..offset + 16].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    fn decode(buf: &[u8; COMMIT_LOG_SUPERBLOCK_SIZE as usize]) -> Option<Self> {
        let magic = u32::from_le_bytes(buf[0..4].try_into().ok()?);
        let version = u32::from_le_bytes(buf[4..8].try_into().ok()?);
        if magic != LAYOUT_MIGRATION_MAGIC || version != LAYOUT_MIGRATION_VERSION {
            return None;
        }
        let expected_crc = u32::from_le_bytes(buf[12..16].try_into().ok()?);
        if expected_crc != Self::crc(buf) {
            return None;
        }
        let shard_count = u32::from_le_bytes(buf[8..12].try_into().ok()?);
        let max_seq = u64::from_le_bytes(buf[16..24].try_into().ok()?);
        if shard_count == 0 || shard_count as usize > MAX_SHARDS_ON_DISK || max_seq == u64::MAX {
            return None;
        }
        Some(Self {
            shard_count,
            max_seq,
        })
    }

    fn decode_checkpoint(buf: &[u8; SHARD_CHECKPOINT_SIZE as usize]) -> Option<Self> {
        let checkpoint = ShardCheckpoint::decode(buf)?;
        if checkpoint.head_offset != 0
            || checkpoint.tail_offset != 0
            || checkpoint.used_bytes != 0
            || checkpoint.max_seq == u64::MAX
        {
            return None;
        }

        let offset = LAYOUT_MIGRATION_EXTENSION_OFFSET;
        let magic = u32::from_le_bytes(buf[offset..offset + 4].try_into().ok()?);
        let version = u32::from_le_bytes(buf[offset + 4..offset + 8].try_into().ok()?);
        if magic != LAYOUT_MIGRATION_MAGIC || version != LAYOUT_MIGRATION_VERSION {
            return None;
        }
        let expected_crc = u32::from_le_bytes(buf[offset + 12..offset + 16].try_into().ok()?);
        if expected_crc != Self::checkpoint_crc(buf) {
            return None;
        }
        let shard_count = u32::from_le_bytes(buf[offset + 8..offset + 12].try_into().ok()?);
        let max_seq = u64::from_le_bytes(buf[offset + 16..offset + 24].try_into().ok()?);
        if shard_count == 0
            || shard_count as usize > MAX_SHARDS_ON_DISK
            || max_seq != checkpoint.max_seq
        {
            return None;
        }
        Some(Self {
            shard_count,
            max_seq,
        })
    }

    fn crc(buf: &[u8; COMMIT_LOG_SUPERBLOCK_SIZE as usize]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf[..12]);
        hasher.update(&buf[16..]);
        hasher.finalize()
    }

    fn checkpoint_crc(buf: &[u8; SHARD_CHECKPOINT_SIZE as usize]) -> u32 {
        let offset = LAYOUT_MIGRATION_EXTENSION_OFFSET;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf[offset..offset + 12]);
        hasher.update(&buf[offset + 16..offset + 24]);
        hasher.finalize()
    }
}

// ── Per-shard checkpoint (recovery hint) ───────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ShardCheckpoint {
    head_offset: u64,
    tail_offset: u64,
    max_seq: u64,
    used_bytes: u64,
}

/// One complete checkpoint image for every shard in a global durability
/// domain. The 32-byte records keep all 64 supported shards inside one 4 KiB
/// block. Global/chunklet sync alternates two existing v3 checkpoint pages;
/// the v3 data-area offset therefore remains byte-for-byte unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PackedCheckpointTable {
    generation: u64,
    checkpoints: Vec<ShardCheckpoint>,
}

impl PackedCheckpointTable {
    #[cfg(test)]
    fn new(generation: u64, checkpoints: Vec<ShardCheckpoint>) -> OnyxResult<Self> {
        if checkpoints.len() < PACKED_CHECKPOINT_SLOT_COUNT
            || checkpoints.len() > MAX_SHARDS_ON_DISK
        {
            return Err(OnyxError::Config(format!(
                "packed checkpoint requires {}..={} shards, got {}",
                PACKED_CHECKPOINT_SLOT_COUNT,
                MAX_SHARDS_ON_DISK,
                checkpoints.len()
            )));
        }
        Ok(Self {
            generation,
            checkpoints,
        })
    }

    #[cfg(test)]
    fn encode(&self) -> [u8; SHARD_CHECKPOINT_SIZE as usize] {
        let mut buf = [0u8; SHARD_CHECKPOINT_SIZE as usize];
        Self::encode_into(self.generation, &self.checkpoints, &mut buf)
            .expect("packed checkpoint table was validated at construction");
        buf
    }

    fn encode_into(
        generation: u64,
        checkpoints: &[ShardCheckpoint],
        buf: &mut [u8],
    ) -> OnyxResult<()> {
        if checkpoints.len() < PACKED_CHECKPOINT_SLOT_COUNT
            || checkpoints.len() > MAX_SHARDS_ON_DISK
            || buf.len() != SHARD_CHECKPOINT_SIZE as usize
        {
            return Err(OnyxError::Config(format!(
                "invalid packed checkpoint encode: shards={} bytes={}",
                checkpoints.len(),
                buf.len()
            )));
        }
        buf.fill(0);
        buf[0..4].copy_from_slice(&PACKED_CHECKPOINT_MAGIC.to_le_bytes());
        buf[4..8].copy_from_slice(&PACKED_CHECKPOINT_VERSION.to_le_bytes());
        buf[8..12].copy_from_slice(&(checkpoints.len() as u32).to_le_bytes());
        buf[12..16].copy_from_slice(&(PACKED_CHECKPOINT_RECORD_SIZE as u32).to_le_bytes());
        buf[16..24].copy_from_slice(&generation.to_le_bytes());
        for (idx, checkpoint) in checkpoints.iter().enumerate() {
            let offset = PACKED_CHECKPOINT_HEADER_SIZE + idx * PACKED_CHECKPOINT_RECORD_SIZE;
            buf[offset..offset + 8].copy_from_slice(&checkpoint.head_offset.to_le_bytes());
            buf[offset + 8..offset + 16].copy_from_slice(&checkpoint.tail_offset.to_le_bytes());
            buf[offset + 16..offset + 24].copy_from_slice(&checkpoint.max_seq.to_le_bytes());
            buf[offset + 24..offset + 32].copy_from_slice(&checkpoint.used_bytes.to_le_bytes());
        }
        let crc = Self::crc(buf);
        buf[PACKED_CHECKPOINT_CRC_OFFSET..PACKED_CHECKPOINT_CRC_OFFSET + 4]
            .copy_from_slice(&crc.to_le_bytes());
        Ok(())
    }

    fn decode(buf: &[u8; SHARD_CHECKPOINT_SIZE as usize]) -> Option<Self> {
        if !Self::has_magic(buf) {
            return None;
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().ok()?);
        if version != PACKED_CHECKPOINT_VERSION {
            return None;
        }
        let shard_count = u32::from_le_bytes(buf[8..12].try_into().ok()?) as usize;
        let record_size = u32::from_le_bytes(buf[12..16].try_into().ok()?) as usize;
        if !(PACKED_CHECKPOINT_SLOT_COUNT..=MAX_SHARDS_ON_DISK).contains(&shard_count)
            || record_size != PACKED_CHECKPOINT_RECORD_SIZE
        {
            return None;
        }
        let expected_crc = u32::from_le_bytes(
            buf[PACKED_CHECKPOINT_CRC_OFFSET..PACKED_CHECKPOINT_CRC_OFFSET + 4]
                .try_into()
                .ok()?,
        );
        if expected_crc != Self::crc(buf) {
            return None;
        }
        let generation = u64::from_le_bytes(buf[16..24].try_into().ok()?);
        let mut checkpoints = Vec::with_capacity(shard_count);
        for idx in 0..shard_count {
            let offset = PACKED_CHECKPOINT_HEADER_SIZE + idx * PACKED_CHECKPOINT_RECORD_SIZE;
            checkpoints.push(ShardCheckpoint {
                head_offset: u64::from_le_bytes(buf[offset..offset + 8].try_into().ok()?),
                tail_offset: u64::from_le_bytes(buf[offset + 8..offset + 16].try_into().ok()?),
                max_seq: u64::from_le_bytes(buf[offset + 16..offset + 24].try_into().ok()?),
                used_bytes: u64::from_le_bytes(buf[offset + 24..offset + 32].try_into().ok()?),
            });
        }
        Some(Self {
            generation,
            checkpoints,
        })
    }

    fn has_magic(buf: &[u8; SHARD_CHECKPOINT_SIZE as usize]) -> bool {
        u32::from_le_bytes(buf[0..4].try_into().expect("packed checkpoint magic slice"))
            == PACKED_CHECKPOINT_MAGIC
    }

    fn crc(buf: &[u8]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf[..PACKED_CHECKPOINT_CRC_OFFSET]);
        hasher.update(&[0u8; 4]);
        hasher.update(&buf[PACKED_CHECKPOINT_CRC_OFFSET + 4..]);
        hasher.finalize()
    }

    fn slot_for_generation(generation: u64) -> usize {
        generation.saturating_sub(1) as usize % PACKED_CHECKPOINT_SLOT_COUNT
    }
}

struct PackedCheckpointState {
    generation: u64,
    checkpoints: Vec<ShardCheckpoint>,
    pending_checkpoints: Vec<ShardCheckpoint>,
    scratch: AlignedBuf,
}

impl PackedCheckpointState {
    fn new(generation: u64, checkpoints: Vec<ShardCheckpoint>) -> OnyxResult<Self> {
        let pending_checkpoints = checkpoints.clone();
        let scratch = AlignedBuf::new(SHARD_CHECKPOINT_SIZE as usize, false)?;
        Ok(Self {
            generation,
            checkpoints,
            pending_checkpoints,
            scratch,
        })
    }

    fn begin_next(&mut self) -> OnyxResult<u64> {
        self.pending_checkpoints.clone_from(&self.checkpoints);
        self.generation
            .checked_add(1)
            .ok_or_else(|| OnyxError::Config("packed checkpoint generation exhausted".into()))
    }

    fn encode_pending(&mut self, generation: u64) -> OnyxResult<()> {
        PackedCheckpointTable::encode_into(
            generation,
            &self.pending_checkpoints,
            self.scratch.as_mut_slice(),
        )
    }

    fn commit_pending(&mut self, generation: u64) {
        self.generation = generation;
        std::mem::swap(&mut self.checkpoints, &mut self.pending_checkpoints);
    }
}

enum PackedCheckpointLoad {
    /// Both reserved A/B pages are valid legacy shard checkpoints: read the
    /// original per-shard v3 pages and migrate on the first global durability
    /// epoch.
    Legacy,
    /// Highest-generation, CRC-valid full-shard table.
    Packed(PackedCheckpointTable),
    /// Packed format was present but neither slot was usable. Mixing in stale
    /// legacy pages after a packed migration can miss a wrapped epoch, so every
    /// shard must take the self-describing full-scan recovery path.
    Corrupt,
}

impl ShardCheckpoint {
    fn encode(&self) -> [u8; SHARD_CHECKPOINT_SIZE as usize] {
        let mut buf = [0u8; SHARD_CHECKPOINT_SIZE as usize];
        buf[0..4].copy_from_slice(&SHARD_CHECKPOINT_MAGIC.to_le_bytes());
        buf[4..8].copy_from_slice(&SHARD_CHECKPOINT_VERSION.to_le_bytes());
        buf[8..16].copy_from_slice(&self.head_offset.to_le_bytes());
        buf[16..24].copy_from_slice(&self.tail_offset.to_le_bytes());
        buf[24..32].copy_from_slice(&self.max_seq.to_le_bytes());
        buf[32..40].copy_from_slice(&self.used_bytes.to_le_bytes());
        let crc = crc32fast::hash(&buf[0..40]);
        buf[40..44].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    fn decode(buf: &[u8; SHARD_CHECKPOINT_SIZE as usize]) -> Option<Self> {
        let magic = u32::from_le_bytes(buf[0..4].try_into().ok()?);
        if magic != SHARD_CHECKPOINT_MAGIC {
            return None;
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().ok()?);
        if version != SHARD_CHECKPOINT_VERSION {
            return None;
        }
        let expected_crc = u32::from_le_bytes(buf[40..44].try_into().ok()?);
        let actual_crc = crc32fast::hash(&buf[0..40]);
        if expected_crc != actual_crc {
            return None;
        }
        Some(Self {
            head_offset: u64::from_le_bytes(buf[8..16].try_into().ok()?),
            tail_offset: u64::from_le_bytes(buf[16..24].try_into().ok()?),
            max_seq: u64::from_le_bytes(buf[24..32].try_into().ok()?),
            used_bytes: u64::from_le_bytes(buf[32..40].try_into().ok()?),
        })
    }
}

// ── Ring state: only pointer arithmetic, very brief lock ────────────

struct RingState {
    used_bytes: u64,
    capacity_bytes: u64,
    reclaim_ready: u64,
    head_offset: u64,
    tail_offset: u64,
    log_order: VecDeque<LogRecord>,
    flushed_seqs: HashSet<u64>,
    /// Sorted set of seqs that have been appended but not yet applied.
    /// Maintained as a sub-index of `log_order`: append inserts, the
    /// `mark_applied` → `note_applied` path removes. `release_below`
    /// touches `log_order` only — by then every reclaimed seq is
    /// already absent from this set (mark_applied precedes the
    /// checkpoint that admits the seq into the release_below cap).
    ///
    /// Why this exists: under buffer-as-sole-journal, `mark_applied`
    /// drops a seq's `pending_entries` slot but `log_order` keeps the
    /// `LogRecord` until `release_below` advances the ring head past
    /// it. Without this index, the coalescer's "next oldest pending
    /// entry" lookup has to walk the entire `log_order` (which is
    /// dominated by applied-but-not-released seqs in steady state)
    /// and DashMap-probe each one — perf showed 15.65% of all CPU
    /// burnt in `DashMap::_get` from that path.
    pending_seqs: BTreeSet<u64>,
    head_became_at: Option<Instant>,
}

// ── Lifecycle: cancelled tracking only ──────────────────────────────
// "inflight" was a HashSet gating the flusher off non-durable entries
// when append acked before LV2 fdatasync. The current design has
// append() block until `lv2_durability.synced_seq >= seq`, so the
// flusher's readiness check is a single atomic load — no set, no lock.

struct LifecycleState {
    cancelled: HashSet<u64>,
}

// ── BufferShard ─────────────────────────────────────────────────────

struct BufferShard {
    device: Arc<dyn BlockBackend>,
    ring: parking_lot::Mutex<RingState>,
    /// Signaled when ring space is freed (reclaim_log_prefix).
    ring_space_cv: parking_lot::Condvar,
    /// How long append() waits when the shard ring is temporarily full.
    backpressure_timeout: Duration,
    lba_index: DashMap<LbaKey, Arc<PendingEntry>>,
    latest_lba_seq: DashMap<LbaKey, (u64, u64)>,
    pending_lba_buckets: DashMap<PendingBucketKey, AtomicU32>,
    pending_entries: DashMap<u64, Arc<PendingEntry>>,
    pending_count: AtomicU64,
    append_ops: AtomicU64,
    append_bytes: AtomicU64,
    reserve_wait_ns: AtomicU64,
    /// Observe manifest-gated physical reclaim without changing its cadence.
    release_calls: AtomicU64,
    released_entries: AtomicU64,
    released_bytes: AtomicU64,
    last_release_cap: AtomicU64,
    /// Bytes of ring entries that have NOT yet been mark_applied. Increments
    /// on `reserve_log_space` by `slot_bytes(slot_count)` (== `disk_len` for
    /// the new entry); decrements when an entry leaves `pending_entries`
    /// (mark_applied / mark_flushed / discard / purge / supersede paths).
    /// Distinct from `RingState.used_bytes`, which is the physical ring
    /// occupancy and only drops on `release_below`. Drives the soft
    /// "in-flight pressure" metric (`fill_percentage`) used by dedup
    /// heuristics — post-Phase-D `used_bytes` only shrinks at checkpoint
    /// cadence, which would saturate `fill_percentage` under sustained
    /// load and starve those heuristics.
    pending_bytes: AtomicU64,
    flush_progress: DashMap<u64, HashSet<u16>>,
    staging_tx: Sender<StagedEntry>,
    staging_rx: Receiver<StagedEntry>,
    /// Reservation order is allocated while holding `ring`, so it has the same
    /// order as physical ring records and per-shard global seqs.
    next_reservation_order: AtomicU64,
    /// Multiple non-overlapping appenders may publish indices concurrently, but
    /// the sync channel must retain physical reservation order because
    /// `synced_seq` is a scalar watermark and guided recovery expects increasing
    /// seqs after the checkpoint head.
    stage_turn: AppendStageTurn,
    #[cfg(test)]
    fail_next_staging_send: AtomicBool,
    sync_batch_max_entries: usize,
    sync_batch_max_bytes: usize,
    /// FIFO tracking eviction order for the in-memory payload cache. Payloads
    /// live in `PendingEntry::payload` (Some) and are evicted from oldest to
    /// newest when `payload_bytes_in_memory` exceeds `max_payload_memory`.
    cached_payload_order: parking_lot::Mutex<VecDeque<u64>>,
    lifecycle: parking_lot::Mutex<LifecycleState>,
    /// LV2 fdatasync watermark. Advanced by the sync thread after each
    /// successful fdatasync; append tickets park on it before returning
    /// to the caller, so every ack implies the payload is durable on LV2.
    pub(crate) lv2_durability: Arc<Lv2DurabilityWaiter>,
    /// Sender for the flusher's global ready channel. Appender publishes
    /// its own seq here after waking from the durability cvar, replacing the
    /// previous sync-thread-driven publish.
    pub(crate) ready_tx: Sender<u64>,
    /// Per-shard variant of `ready_tx` (the flusher's coalesce loop picks
    /// the shard channel for fairness).
    pub(crate) shard_ready_tx: Sender<u64>,
    io_lock: parking_lot::Mutex<()>,
    /// Intern cache: vol_id → Arc<str>. Typically 1-10 entries.
    /// Avoids per-insert Arc::from() allocation for LbaKey.
    vol_id_cache: RwLock<Vec<Arc<str>>>,
    metrics: Arc<OnceLock<Arc<EngineMetrics>>>,
    /// V3 checkpoint device — covers the 4KB checkpoint block preceding this
    /// shard's data area. None for v2 layout (no checkpoint).
    checkpoint_device: Option<Arc<dyn BlockBackend>>,
    /// Shared counter for total payload bytes in memory (across all shards).
    payload_bytes_in_memory: Arc<AtomicU64>,
    /// Global budget for durable in-memory buffer payload cache. 0 disables
    /// the resident cache (forces lazy hydration from LV2 on demand).
    max_payload_memory: u64,
    /// Global upper bound of seqs that have been mark_flushed'd (across all
    /// shards). Updated in `free_seq_allocation` to `max(current, seq)`.
    max_flushed_seq: Arc<AtomicU64>,
    /// Watermark of seqs whose DB commits have been fsync'd. Set by the
    /// engine-owned durability-watermark thread. `reclaim_log_prefix` only
    /// advances the tail past entries whose seq ≤ this.
    durable_seq: Arc<AtomicU64>,
}

pub struct WriteBufferPool {
    root_device: Arc<dyn BlockBackend>,
    shards: Vec<BufferShardHandle>,
    next_seq: AtomicU64,
    /// Serialises a durability-frontier snapshot against the short interval
    /// between allocating a global seq and publishing it into a shard's
    /// `pending_seqs` index. Appends take the read side only through
    /// `publish_prepared`; frontier sampling takes the write side. This keeps
    /// concurrent appenders parallel while preventing a checkpoint from
    /// mistaking an allocated-but-not-yet-visible seq for an applied gap.
    frontier_gate: parking_lot::RwLock<()>,
    append_order_stripes: Box<[AppendOrderStripe]>,
    routing_zone_size_blocks: u64,
    ready_rx: Receiver<u64>,
    shard_ready_rxs: Vec<Receiver<u64>>,
    metrics: Arc<OnceLock<Arc<EngineMetrics>>>,
    /// Total payload bytes currently held in memory across all shards.
    payload_bytes_in_memory: Arc<AtomicU64>,
    /// Maximum allowed durable in-memory payload cache bytes. 0 disables the
    /// resident cache (used by tests that need forced lazy hydration).
    max_payload_memory: u64,
    /// On-disk layout version — persisted on Drop. Must match the actual disk layout.
    disk_version: u32,
    /// Global/chunklet v3 checkpoints packed into alternating 4 KiB pages.
    /// `None` for the per-shard raw/io_uring path and legacy v2 layout.
    packed_checkpoint: Option<Arc<parking_lot::Mutex<PackedCheckpointState>>>,
    /// Resolved hyperbolic throttle curve (`None` = disabled). Set once at
    /// pool open; each shard has an independent absolute-wakeup queue.
    throttle: Option<ThrottleSettings>,
    /// Monotonic anchor shared by the per-shard wakeup timestamps.
    throttle_anchor: Instant,
    /// Independent pacing state for each physical ring shard. Keeping wakeup
    /// queues separate prevents one hot shard from serializing producers that
    /// are headed to rings with available space.
    throttle_states: Vec<ShardThrottleState>,
    backend_debt_throttle_enabled: bool,
    /// Wait out LV2 ring backpressure before locking append-order stripes.
    prewait_ring_space: bool,
    /// Hysteresis for the device-wide commit-executor debt signal. Wakeup
    /// pacing remains per shard; only the advisory pressure state is global.
    backend_throttle_control: BackendThrottleControl,
    /// Highest seq that has ever been passed to `mark_flushed` across any shard.
    /// Updated by flusher writers when they ack a completed seq. Read by the
    /// durability-watermark background thread to decide how far `durable_seq`
    /// can be advanced after an fsync completes.
    ///
    /// NOTE: this is a global upper bound, not a "contiguous prefix". Use it
    /// as "this seq has been mark_flushed", not "all seqs up to this have
    /// been mark_flushed". The watermark thread captures this BEFORE calling
    /// sync, so any seq ≤ captured value is guaranteed to have had its DB
    /// writes issued before the fsync and is therefore durable afterwards.
    pub(crate) max_flushed_seq: Arc<AtomicU64>,
    /// Watermark of seqs that are guaranteed durable on metadb. Advanced
    /// only by the durability-watermark background thread after
    /// `MetaStore::sync_durable()` returns.
    ///
    /// Buffer ring reclaim path consults this: `reclaim_log_prefix` only
    /// advances the tail past entries whose seq ≤ `durable_seq`, even if
    /// `mark_flushed` has been called. This guarantees that a buffer entry
    /// is never physically overwritten before its DB writes hit disk.
    pub(crate) durable_seq: Arc<AtomicU64>,
    /// Metadb persistence fence. Empty = healthy. Set (once, latched) by the
    /// durability-watermark thread when metadb checkpoints have failed fatally
    /// (`CapacityExhausted` / "persistence subsystem failed") or repeatedly.
    /// Once set, [`append`](WriteBufferPool::append) fail-fasts new writes with
    /// the recorded reason instead of silently acking into a ring that can no
    /// longer be drained — the ENOSPC "ack is a lie" hole. Reads are never
    /// fenced. The fence only clears on process restart.
    meta_fence: OnceLock<String>,
}

/// A prepared LV2 append that becomes acknowledgeable once its shard's
/// durability watermark reaches `seq`. This lets frontends wait outside the
/// request worker while preserving ack-after-fdatasync semantics.
pub struct BufferAppendTicket {
    shard: Arc<BufferShard>,
    pending: Arc<PendingEntry>,
    seq: u64,
    append_started: Instant,
    durability_wait_started: Instant,
    /// Shared by every LV2 shard ticket belonging to one foreground request.
    /// The final ticket drop releases the request's outstanding-IO lease.
    foreground_io_lease: Option<Arc<ForegroundIoLease>>,
}

impl BufferAppendTicket {
    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn is_durable(&self) -> bool {
        self.shard.lv2_durability.synced_seq.load(Ordering::Acquire) >= self.seq
    }

    /// Time from the LV2 watermark advance that made this append durable until
    /// an asynchronous frontend observed it. `None` means the watermark has not
    /// advanced yet (or the entry came from recovery without a live timestamp).
    pub(crate) fn completion_dispatch_delay_ns(&self, observed_at: Instant) -> Option<u64> {
        let advanced_at = self
            .pending
            .durability_advanced_at_ns
            .load(Ordering::Acquire);
        (advanced_at != 0).then(|| lv2_metric_timestamp_ns(observed_at).saturating_sub(advanced_at))
    }

    pub(crate) fn arm_wakeup(&self, tx: &Sender<()>) -> bool {
        self.shard.lv2_durability.arm_channel(self.seq, tx)
    }

    pub(crate) fn attach_foreground_io_lease(&mut self, lease: Arc<ForegroundIoLease>) {
        debug_assert!(self.foreground_io_lease.is_none());
        self.foreground_io_lease = Some(lease);
    }

    pub fn wait(self) -> u64 {
        self.shard.wait_for_durable(self.seq);
        self.finish_at(Instant::now(), false)
    }

    pub fn finish(self) -> u64 {
        self.finish_at(Instant::now(), false)
    }

    pub(crate) fn finish_dispatched(self) -> u64 {
        self.finish_at(Instant::now(), true)
    }

    fn finish_at(self, finished_at: Instant, dispatched: bool) -> u64 {
        debug_assert!(self.is_durable());
        let durable_wait_ns = finished_at
            .saturating_duration_since(self.durability_wait_started)
            .as_nanos() as u64;
        if let Some(lease) = &self.foreground_io_lease {
            lease.record_buffer_append_wait_durable_ns(durable_wait_ns);
        }
        if let Some(metrics) = self.shard.metrics.get() {
            metrics.record_buffer_append_wait_durable_ns(durable_wait_ns);
            metrics.buffer_append_total_ns.fetch_add(
                finished_at
                    .saturating_duration_since(self.append_started)
                    .as_nanos() as u64,
                Ordering::Relaxed,
            );
            if dispatched {
                let advanced_at = self
                    .pending
                    .durability_advanced_at_ns
                    .load(Ordering::Acquire);
                if advanced_at != 0 {
                    metrics.record_buffer_lv2_watermark_dispatch_ns(
                        lv2_metric_timestamp_ns(finished_at).saturating_sub(advanced_at),
                    );
                }
            }
        }
        self.seq
    }
}

#[derive(Default)]
struct ShardThrottleState {
    /// Absolute wakeup time in ns since `WriteBufferPool::throttle_anchor`.
    last_wakeup_ns: AtomicU64,
    /// Cached physical fill for this shard in basis points (100 = 1%).
    cached_fill_basis_points: AtomicU32,
    /// Append counter that drives the cached-fill refresh cadence.
    sample_counter: AtomicU32,
}

#[derive(Default)]
struct BackendThrottleControl {
    armed: AtomicBool,
    saturation_started_ns: AtomicU64,
    recovery_started_ns: AtomicU64,
    /// Device-wide pacing clock for backend debt. Unlike physical ring
    /// pressure, this signal is shared by every shard and must produce one
    /// aggregate foreground admission stream.
    last_wakeup_ns: AtomicU64,
}

impl BackendThrottleControl {
    fn pressure_basis_points(
        &self,
        throttle: ThrottleSettings,
        worker_count: u64,
        active_workers: u64,
        queue_depth: u64,
        now_ns: u64,
    ) -> u32 {
        let workers = worker_count.max(1);
        let saturated = active_workers >= workers && queue_depth > workers;
        let now_ns = now_ns.max(1);

        if !self.armed.load(Ordering::Relaxed) {
            if !saturated {
                self.saturation_started_ns.store(0, Ordering::Relaxed);
                return 0;
            }
            let started = self.saturation_started_ns.load(Ordering::Relaxed);
            if started == 0 {
                let _ = self.saturation_started_ns.compare_exchange(
                    0,
                    now_ns,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                );
                return 0;
            }
            if now_ns.saturating_sub(started) < BACKEND_THROTTLE_ARM_NS {
                return 0;
            }
            self.armed.store(true, Ordering::Relaxed);
            self.recovery_started_ns.store(0, Ordering::Relaxed);
        }

        if queue_depth <= workers / 2 {
            let recovery_started = self.recovery_started_ns.load(Ordering::Relaxed);
            if recovery_started == 0 {
                let _ = self.recovery_started_ns.compare_exchange(
                    0,
                    now_ns,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                );
            } else if now_ns.saturating_sub(recovery_started) >= BACKEND_THROTTLE_RELEASE_NS {
                self.armed.store(false, Ordering::Relaxed);
                self.saturation_started_ns.store(0, Ordering::Relaxed);
                self.recovery_started_ns.store(0, Ordering::Relaxed);
                return 0;
            }
        } else {
            self.recovery_started_ns.store(0, Ordering::Relaxed);
        }

        throttle.executor_debt_basis_points(workers, active_workers, queue_depth)
    }
}

static TEST_PURGE_FAIL_VOLUMES: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
static TEST_SYNC_FAIL_REMAINING: OnceLock<Mutex<u32>> = OnceLock::new();

fn test_purge_fail_volumes() -> &'static Mutex<HashSet<String>> {
    TEST_PURGE_FAIL_VOLUMES.get_or_init(|| Mutex::new(HashSet::new()))
}

fn test_sync_fail_remaining() -> &'static Mutex<u32> {
    TEST_SYNC_FAIL_REMAINING.get_or_init(|| Mutex::new(0))
}

#[doc(hidden)]
pub fn install_purge_volume_failpoint(vol_id: &str) {
    test_purge_fail_volumes()
        .lock()
        .unwrap()
        .insert(vol_id.to_string());
}

#[doc(hidden)]
pub fn clear_purge_volume_failpoint(vol_id: &str) {
    test_purge_fail_volumes().lock().unwrap().remove(vol_id);
}

#[doc(hidden)]
pub fn install_buffer_sync_failpoint(remaining_hits: u32) {
    *test_sync_fail_remaining().lock().unwrap() = remaining_hits;
}

#[doc(hidden)]
pub fn clear_buffer_sync_failpoint() {
    *test_sync_fail_remaining().lock().unwrap() = 0;
}

struct ScanResult {
    max_seq: u64,
    used_bytes: u64,
    head_offset: u64,
    tail_offset: u64,
    log_order: VecDeque<LogRecord>,
    flushed_seqs: HashSet<u64>,
}

struct BufferShardHandle {
    shard: Arc<BufferShard>,
    sync_wake_tx: Sender<()>,
    sync_shutdown: Arc<AtomicBool>,
    sync_thread: Option<JoinHandle<()>>,
}

#[derive(Debug, Clone)]
struct StagedEntry {
    pending: Arc<PendingEntry>,
    payload: Arc<[u8]>,
    staged_at: Instant,
}

mod pool;
mod shard;

#[cfg(test)]
mod tests;
