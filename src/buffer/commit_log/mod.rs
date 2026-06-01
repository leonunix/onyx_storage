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
use crate::io::aligned::{round_up, AlignedBuf, AlignedBufPool};
use crate::io::device::RawDevice;
use crate::io::uring::{IoUringSession, LinkedOp, UringOp, UringOpResult};
use crate::meta::schema::MAX_VOLUME_ID_BYTES;
use crate::metrics::{BufferShardSnapshot, EngineMetrics};
use crate::types::{Lba, BLOCK_SIZE};

const COMMIT_LOG_MAGIC: u32 = 0x4F43_4C47; // "OCLG"
const COMMIT_LOG_VERSION: u32 = 3;
const COMMIT_LOG_VERSION_V2: u32 = 2;
const COMMIT_LOG_SUPERBLOCK_SIZE: u64 = 4096;
const MAX_SHARDS_ON_DISK: usize = 64;
/// DashMap internal shard count — high value reduces contention under many writers.
const DASHMAP_SHARDS: usize = 256;

const SHARD_CHECKPOINT_MAGIC: u32 = 0x5348_434B; // "SHCK"
const SHARD_CHECKPOINT_VERSION: u32 = 1;
const SHARD_CHECKPOINT_SIZE: u64 = 4096;
const BACKPRESSURE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const STAGING_CHANNEL_CAPACITY: usize = 32 * 1024;
/// Bound one sync epoch per shard. The previous unbounded drain could pull
/// millions of staged 4K writes into one Vec under fio, multiplying memory
/// before the device had a chance to fsync the first batch.
const SYNC_BATCH_MAX_ENTRIES: usize = 4096;
const SYNC_BATCH_MAX_BYTES: usize = 64 * 1024 * 1024;
/// Online payload hydration read-ahead. 128 KiB matches the common coalesce
/// unit while keeping foreground read tail latency bounded.
const HYDRATE_BATCH_MAX_BYTES: usize = 128 * 1024;
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
    pub throttle: ThrottleSettings,
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
/// condvar-based hard backpressure path inside `BufferShard::append_with_seq`.
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

    /// Hyperbolic curve evaluation. Pure function for unit tests.
    pub(crate) fn delay_us_for_fill(&self, fill_pct: u8) -> u64 {
        if fill_pct <= self.min_pct {
            return 0;
        }
        if fill_pct >= self.max_pct {
            return self.cap_us;
        }
        let num = (fill_pct - self.min_pct) as u64;
        let den = (self.max_pct - fill_pct) as u64;
        let delay = num.saturating_mul(self.scale_us) / den.max(1);
        delay.min(self.cap_us)
    }
}

impl BufferRuntimeLimits {
    pub fn from_config(
        _durable_payload_limit: u64,
        staging_channel_capacity: usize,
        sync_batch_max_entries: usize,
        sync_batch_max_bytes: usize,
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
            throttle: defaults.throttle,
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
}

impl Default for BufferRuntimeLimits {
    fn default() -> Self {
        Self {
            staging_channel_capacity: STAGING_CHANNEL_CAPACITY,
            sync_batch_max_entries: SYNC_BATCH_MAX_ENTRIES,
            sync_batch_max_bytes: SYNC_BATCH_MAX_BYTES,
            throttle: ThrottleSettings::default(),
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
    parker: Arc<DurabilityParker>,
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
                parker: parker.clone(),
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
        let mut wake: Vec<Arc<DurabilityParker>> = Vec::new();
        {
            let mut waiters = self.waiters.lock();
            let mut i = 0;
            while i < waiters.len() {
                if waiters[i].seq <= max_seq {
                    wake.push(waiters.swap_remove(i).parker);
                } else {
                    i += 1;
                }
            }
        }
        for parker in wake {
            parker.done.store(true, Ordering::Release);
            parker.thread.unpark();
        }
    }
}

#[derive(Debug, Clone)]
pub struct PendingEntry {
    pub seq: u64,
    pub vol_id: String,
    pub start_lba: Lba,
    pub lba_count: u32,
    pub payload_crc32: u32,
    pub vol_created_at: u64,
    /// Payload data. `None` for recovered entries whose payload hasn't been
    /// loaded from the buffer device yet (lazy hydration to avoid OOM).
    pub payload: Option<Arc<[u8]>>,
    pub disk_offset: u64,
    pub disk_len: u32,
    /// In-memory residency start used for starvation diagnostics.
    pub enqueued_at: Instant,
    /// Older buffered ranges overwritten by this entry at append time.
    /// Once this entry is durable in the commit log, these ranges can be
    /// retired immediately instead of waiting for the flusher to rediscover
    /// that they are stale.
    pub superseded_ranges: Vec<(u64, Lba, u32)>,
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

// ── Per-shard checkpoint (recovery hint) ───────────────────────────

#[derive(Debug, Clone, Copy)]
struct ShardCheckpoint {
    head_offset: u64,
    tail_offset: u64,
    max_seq: u64,
    used_bytes: u64,
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
    device: RawDevice,
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
    sync_batch_max_entries: usize,
    sync_batch_max_bytes: usize,
    /// FIFO tracking eviction order for the in-memory payload cache. Payloads
    /// live in `PendingEntry::payload` (Some) and are evicted from oldest to
    /// newest when `payload_bytes_in_memory` exceeds `max_payload_memory`.
    cached_payload_order: parking_lot::Mutex<VecDeque<u64>>,
    lifecycle: parking_lot::Mutex<LifecycleState>,
    /// LV2 fdatasync watermark. Advanced by the sync thread after each
    /// successful fdatasync; `append_with_seq` parks on it before returning
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
    checkpoint_device: Option<RawDevice>,
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
    root_device: RawDevice,
    shards: Vec<BufferShardHandle>,
    next_seq: AtomicU64,
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
    /// Resolved hyperbolic throttle curve (`None` = disabled). Set once at
    /// pool open; ZFS-style absolute-wakeup queue serializes producers.
    throttle: Option<ThrottleSettings>,
    /// Monotonic anchor for `throttle_last_wakeup_ns`.
    throttle_anchor: Instant,
    /// Absolute wakeup time (ns since `throttle_anchor`) of the latest slot
    /// claimed by a throttled producer. Each new throttle event atomically
    /// advances this past `max(now, last) + delay`, so N concurrent producers
    /// stack to N × delay rather than collapsing onto a single sleep window.
    throttle_last_wakeup_ns: AtomicU64,
    /// Cached `fill_percentage()` value, refreshed once every
    /// `THROTTLE_SAMPLE_INTERVAL` appends. Recomputing live takes one Mutex
    /// acquire per shard (16 by default), so caching keeps the hot path on
    /// pure atomics when the throttle is armed but inactive.
    throttle_cached_fill_pct: AtomicU32,
    /// Append counter that drives `throttle_cached_fill_pct` refresh cadence.
    throttle_sample_counter: AtomicU32,
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
}

mod pool;
mod shard;

#[cfg(test)]
mod tests;
