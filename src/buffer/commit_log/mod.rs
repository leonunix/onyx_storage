use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{unbounded, Receiver, RecvTimeoutError, Sender, TryRecvError};
use dashmap::DashMap;
use parking_lot::RwLock;

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
    pending_entries: DashMap<u64, Arc<PendingEntry>>,
    flush_progress: DashMap<u64, HashSet<u16>>,
    staging_tx: Sender<StagedEntry>,
    staging_rx: Receiver<StagedEntry>,
    volatile_payloads: DashMap<u64, Arc<[u8]>>,
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
    /// Maximum allowed in-memory payload bytes (shared with pool). 0 = no limit.
    max_payload_memory_bytes: u64,
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
    /// Maximum allowed in-memory payload bytes. 0 means no limit (for tests).
    max_payload_memory: u64,
    /// On-disk layout version — persisted on Drop. Must match the actual disk layout.
    disk_version: u32,
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
