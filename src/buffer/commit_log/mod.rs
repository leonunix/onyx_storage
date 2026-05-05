use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, unbounded, Receiver, RecvTimeoutError, Sender, TryRecvError};
use dashmap::DashMap;
use parking_lot::{Condvar, RwLock};

use crate::buffer::entry::{BufferEntry, BUFFER_ENTRY_MAGIC, MAX_ENTRY_SIZE, MIN_ENTRY_SIZE};
use crate::error::{OnyxError, OnyxResult};
use crate::io::aligned::{round_up, AlignedBuf};
use crate::io::device::RawDevice;
use crate::io::uring::{IoUringSession, UringOp};
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
const MIN_VOLATILE_PAYLOAD_MEMORY: u64 = 64 * 1024 * 1024;
const MAX_VOLATILE_PAYLOAD_MEMORY: u64 = 8 * 1024 * 1024 * 1024;
/// Online payload hydration read-ahead. 128 KiB matches the common coalesce
/// unit while keeping foreground read tail latency bounded.
const HYDRATE_BATCH_MAX_BYTES: usize = 128 * 1024;
/// Coarse read-path filter for pending buffer entries. A read range whose
/// buckets are absent can skip the DashMap LBA index entirely. Collisions and
/// stale buckets are harmless false positives; false negatives are avoided by
/// installing buckets before publishing LBA index entries and removing them
/// only after the entry is fully retired.
const PENDING_LBA_BUCKET_BLOCKS: u64 = 256;

#[derive(Debug, Clone, Copy)]
pub struct BufferRuntimeLimits {
    pub staging_channel_capacity: usize,
    pub sync_batch_max_entries: usize,
    pub sync_batch_max_bytes: usize,
    pub volatile_payload_memory: u64,
}

impl BufferRuntimeLimits {
    pub fn from_config(
        durable_payload_limit: u64,
        staging_channel_capacity: usize,
        sync_batch_max_entries: usize,
        sync_batch_max_bytes: usize,
        volatile_payload_memory: u64,
    ) -> Self {
        let defaults = Self::for_durable_payload_limit(durable_payload_limit);
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
            volatile_payload_memory: if volatile_payload_memory == 0 {
                defaults.volatile_payload_memory
            } else {
                volatile_payload_memory
            },
        }
    }

    pub fn for_durable_payload_limit(durable_payload_limit: u64) -> Self {
        let volatile_payload_memory = if durable_payload_limit == 0 {
            0
        } else {
            (durable_payload_limit / 4)
                .clamp(MIN_VOLATILE_PAYLOAD_MEMORY, MAX_VOLATILE_PAYLOAD_MEMORY)
        };
        Self {
            staging_channel_capacity: STAGING_CHANNEL_CAPACITY,
            sync_batch_max_entries: SYNC_BATCH_MAX_ENTRIES,
            sync_batch_max_bytes: SYNC_BATCH_MAX_BYTES,
            volatile_payload_memory,
        }
    }
}

struct VolatilePayloadBudget {
    bytes: AtomicU64,
    limit: u64,
    lock: parking_lot::Mutex<()>,
    cv: Condvar,
}

impl VolatilePayloadBudget {
    fn new(limit: u64) -> Self {
        Self {
            bytes: AtomicU64::new(0),
            limit,
            lock: parking_lot::Mutex::new(()),
            cv: Condvar::new(),
        }
    }

    fn reserve(&self, bytes: u64) {
        if self.limit == 0 {
            self.bytes.fetch_add(bytes, Ordering::Relaxed);
            return;
        }

        let mut guard = self.lock.lock();
        loop {
            let current = self.bytes.load(Ordering::Relaxed);
            let fits = current.saturating_add(bytes) <= self.limit;
            let oversized_single_write = current == 0 && bytes > self.limit;
            if fits || oversized_single_write {
                if self
                    .bytes
                    .compare_exchange_weak(
                        current,
                        current.saturating_add(bytes),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return;
                }
                continue;
            }
            let _ = self.cv.wait_for(&mut guard, BACKPRESSURE_POLL_INTERVAL);
        }
    }

    fn release(&self, bytes: u64) {
        let mut current = self.bytes.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_sub(bytes);
            match self.bytes.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.cv.notify_all();
                    return;
                }
                Err(actual) => current = actual,
            }
        }
    }

    fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    fn limit(&self) -> u64 {
        self.limit
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
    head_became_at: Option<Instant>,
}

// ── Lifecycle: inflight/cancelled tracking ──────────────────────────

struct LifecycleState {
    inflight: HashSet<u64>,
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
    flush_progress: DashMap<u64, HashSet<u16>>,
    staging_tx: Sender<StagedEntry>,
    staging_rx: Receiver<StagedEntry>,
    sync_batch_max_entries: usize,
    sync_batch_max_bytes: usize,
    /// Durable, memory-resident payload cache. `volatile_payloads` covers the
    /// pre-fdatasync window; once sync publishes an entry as ready, payloads
    /// move into `PendingEntry::payload` and this FIFO tracks eviction order.
    cached_payload_order: parking_lot::Mutex<VecDeque<u64>>,
    volatile_payloads: DashMap<u64, Arc<[u8]>>,
    volatile_payload_budget: Arc<VolatilePayloadBudget>,
    lifecycle: parking_lot::Mutex<LifecycleState>,
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
    /// the resident cache; the transient pre-sync `volatile_payloads` path
    /// still serves read-after-write.
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
    /// Serializes background L2P commits. Flusher/scanner paths take this
    /// while checking `is_latest_lba_seq` and updating metadb. That closes the
    /// stale-commit window where an older seq could observe "latest", then
    /// race behind a newer flush and overwrite the newer L2P mapping.
    ///
    /// Foreground append does not take this lock: if a newer write arrives
    /// after an older commit has already passed the latest check, the older
    /// commit may land first, but the newer buffered write remains visible and
    /// its later flush will commit after it.
    l2p_commit_locks: DashMap<String, Arc<parking_lot::Mutex<()>>>,
    routing_zone_size_blocks: u64,
    ready_rx: Receiver<u64>,
    shard_ready_rxs: Vec<Receiver<u64>>,
    metrics: Arc<OnceLock<Arc<EngineMetrics>>>,
    /// Total payload bytes currently held in memory across all shards.
    payload_bytes_in_memory: Arc<AtomicU64>,
    /// Maximum allowed durable in-memory payload cache bytes. 0 disables the
    /// resident cache (used by tests that need forced lazy hydration).
    max_payload_memory: u64,
    /// Sync-before-publish payloads. These are intentionally separate from
    /// durable cache bytes because they are write-admission pressure, not a
    /// reusable read cache.
    volatile_payload_budget: Arc<VolatilePayloadBudget>,
    /// On-disk layout version — persisted on Drop. Must match the actual disk layout.
    disk_version: u32,
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
