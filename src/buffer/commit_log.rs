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
    staging_tx: Sender<Arc<PendingEntry>>,
    staging_rx: Receiver<Arc<PendingEntry>>,
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

impl BufferShard {
    fn elapsed_ns(start: Instant) -> u64 {
        start.elapsed().as_nanos().min(u64::MAX as u128) as u64
    }

    fn record_metric(counter: &std::sync::atomic::AtomicU64, start: Instant) {
        counter.fetch_add(Self::elapsed_ns(start), Ordering::Relaxed);
    }

    fn slot_size() -> u64 {
        BLOCK_SIZE as u64
    }

    fn add_seq_lba_range(acc: &mut Vec<(u64, Lba, u32)>, seq: u64, lba: Lba) {
        if let Some((last_seq, last_start, last_count)) = acc.last_mut() {
            if *last_seq == seq && last_start.0 + *last_count as u64 == lba.0 {
                *last_count += 1;
                return;
            }
        }
        acc.push((seq, lba, 1));
    }

    fn total_slots(capacity_bytes: u64) -> u64 {
        capacity_bytes / Self::slot_size()
    }

    fn slot_bytes(slot_count: u32) -> u64 {
        slot_count as u64 * Self::slot_size()
    }

    /// Intern vol_id → Arc<str>. Read-lock fast path (common), write-lock
    /// only on first encounter of a new volume. Typically ≤10 volumes.
    fn intern_vol_id(&self, vol_id: &str) -> Arc<str> {
        {
            let cache = self.vol_id_cache.read();
            if let Some(arc) = cache.iter().find(|s| &***s == vol_id) {
                return arc.clone();
            }
        }
        let mut cache = self.vol_id_cache.write();
        // Double-check after acquiring write lock.
        if let Some(arc) = cache.iter().find(|s| &***s == vol_id) {
            return arc.clone();
        }
        let arc: Arc<str> = Arc::from(vol_id);
        cache.push(arc.clone());
        arc
    }

    fn reserve_log_space(ring: &mut RingState, seq: u64, slot_count: u32) -> Option<u64> {
        let len_bytes = Self::slot_bytes(slot_count);
        if len_bytes > ring.capacity_bytes {
            return None;
        }
        if ring.capacity_bytes.saturating_sub(ring.used_bytes) < len_bytes {
            return None;
        }

        let head = ring.head_offset;
        let tail = ring.tail_offset;
        let capacity = ring.capacity_bytes;

        let offset = if ring.used_bytes == 0 {
            head
        } else if head >= tail {
            let bytes_to_end = capacity - head;
            if len_bytes <= bytes_to_end {
                head
            } else if len_bytes <= tail {
                0
            } else {
                return None;
            }
        } else if len_bytes <= tail - head {
            head
        } else {
            return None;
        };

        ring.head_offset = (offset + len_bytes) % capacity;
        ring.used_bytes += len_bytes;
        ring.log_order.push_back(LogRecord {
            seq,
            disk_offset: offset,
            slot_count,
        });
        Some(offset)
    }

    fn reclaim_log_prefix(ring: &mut RingState) {
        loop {
            let Some(front) = ring.log_order.front().copied() else {
                ring.tail_offset = ring.head_offset;
                break;
            };
            if !ring.flushed_seqs.contains(&front.seq) {
                ring.tail_offset = front.disk_offset;
                break;
            }

            ring.log_order.pop_front();
            ring.flushed_seqs.remove(&front.seq);
            ring.used_bytes = ring
                .used_bytes
                .saturating_sub(Self::slot_bytes(front.slot_count));
            ring.reclaim_ready += 1;
            ring.tail_offset = ring
                .log_order
                .front()
                .map(|next| next.disk_offset)
                .unwrap_or(ring.head_offset);
        }
    }

    fn mark_entry_flushed(device: &RawDevice, pending: &PendingEntry) -> OnyxResult<()> {
        let payload_len = pending.lba_count as usize * BLOCK_SIZE as usize;
        if device.is_direct_io() {
            let bytes = BufferEntry::encode_direct_compact_header(
                pending.seq,
                &pending.vol_id,
                pending.start_lba,
                pending.lba_count,
                true,
                pending.vol_created_at,
                payload_len,
            )?;
            device.write_at(
                &bytes.as_slice()[..BLOCK_SIZE as usize],
                pending.disk_offset,
            )?;
        } else {
            // Compact parts only needs the payload for the header CRC, which
            // for compact format doesn't cover payload. Use empty placeholder.
            let empty_payload: &[u8] = &vec![0u8; payload_len];
            let (header, _) = BufferEntry::encode_compact_parts(
                pending.seq,
                &pending.vol_id,
                pending.start_lba,
                pending.lba_count,
                true,
                pending.vol_created_at,
                empty_payload,
            )?;
            let mut block = vec![0u8; BLOCK_SIZE as usize];
            block[..header.len()].copy_from_slice(&header);
            device.write_at(&block[..BLOCK_SIZE as usize], pending.disk_offset)?;
        }
        Ok(())
    }

    fn scan_entry(
        device: &RawDevice,
        offset: u64,
        capacity_bytes: u64,
    ) -> OnyxResult<Option<(BufferEntry, u32)>> {
        if offset + Self::slot_size() > capacity_bytes {
            return Ok(None);
        }

        let mut header = vec![0u8; BLOCK_SIZE as usize];
        device.read_at(&mut header, offset)?;
        let total_len = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let magic = u32::from_le_bytes(header[4..8].try_into().unwrap());
        if total_len < MIN_ENTRY_SIZE || total_len > MAX_ENTRY_SIZE || magic != BUFFER_ENTRY_MAGIC {
            return Ok(None);
        }

        let slot_count = round_up(total_len as usize, BLOCK_SIZE as usize) as u32 / BLOCK_SIZE;
        let slot_bytes = Self::slot_bytes(slot_count);
        if offset + slot_bytes > capacity_bytes {
            return Ok(None);
        }

        let mut buf = vec![0u8; slot_bytes as usize];
        device.read_at(&mut buf, offset)?;
        let Some(entry) = BufferEntry::from_bytes(&buf) else {
            return Ok(None);
        };
        Ok(Some((entry, slot_count)))
    }

    fn rebuild_indices(
        device: &RawDevice,
        capacity_bytes: u64,
        lba_index: &DashMap<LbaKey, Arc<PendingEntry>>,
        latest_lba_seq: &DashMap<LbaKey, (u64, u64)>,
        pending_entries: &DashMap<u64, Arc<PendingEntry>>,
    ) -> OnyxResult<ScanResult> {
        #[derive(Debug)]
        struct ScannedRecord {
            seq: u64,
            disk_offset: u64,
            slot_count: u32,
            flushed: bool,
            pending: Option<Arc<PendingEntry>>,
        }

        let total_slots = Self::total_slots(capacity_bytes);
        let mut slot = 0u64;
        let mut max_seq = 0u64;
        let mut scanned = Vec::new();

        while slot < total_slots {
            let offset = slot * Self::slot_size();
            match Self::scan_entry(device, offset, capacity_bytes)? {
                Some((entry, slot_count)) => {
                    let disk_len = Self::slot_bytes(slot_count) as u32;
                    max_seq = max_seq.max(entry.seq);
                    let pending = (!entry.flushed).then(|| {
                        Arc::new(PendingEntry {
                            seq: entry.seq,
                            vol_id: entry.vol_id.clone(),
                            start_lba: entry.start_lba,
                            lba_count: entry.lba_count,
                            payload_crc32: entry.payload_crc32,
                            vol_created_at: entry.vol_created_at,
                            payload: None, // lazy: hydrated from disk on demand
                            disk_offset: offset,
                            disk_len,
                            superseded_ranges: Vec::new(),
                        })
                    });
                    scanned.push(ScannedRecord {
                        seq: entry.seq,
                        disk_offset: offset,
                        slot_count,
                        flushed: entry.flushed,
                        pending,
                    });
                    slot += slot_count as u64;
                }
                None => {
                    slot += 1;
                }
            }
        }

        scanned.sort_by_key(|record| record.seq);

        for record in &scanned {
            let Some(pending) = record.pending.as_ref() else {
                continue;
            };
            let vid: Arc<str> = Arc::from(pending.vol_id.as_str());
            pending_entries.insert(pending.seq, pending.clone());
            for i in 0..pending.lba_count {
                let key = LbaKey {
                    vol_id: vid.clone(),
                    lba: Lba(pending.start_lba.0 + i as u64),
                };
                lba_index.insert(key.clone(), pending.clone());
                latest_lba_seq.insert(key, (pending.seq, pending.vol_created_at));
            }
        }

        let first_unreclaimed = scanned.iter().position(|record| !record.flushed);
        let mut log_order = VecDeque::new();
        let mut flushed_seqs = HashSet::new();
        let mut used_bytes = 0u64;

        if let Some(idx) = first_unreclaimed {
            for record in &scanned[idx..] {
                log_order.push_back(LogRecord {
                    seq: record.seq,
                    disk_offset: record.disk_offset,
                    slot_count: record.slot_count,
                });
                used_bytes += Self::slot_bytes(record.slot_count);
                if record.flushed {
                    flushed_seqs.insert(record.seq);
                }
            }
        }

        let head_offset = if let Some(last) = log_order.back() {
            (last.disk_offset + Self::slot_bytes(last.slot_count)) % capacity_bytes
        } else {
            scanned
                .last()
                .map(|last| (last.disk_offset + Self::slot_bytes(last.slot_count)) % capacity_bytes)
                .unwrap_or(0)
        };
        let tail_offset = log_order
            .front()
            .map(|first| first.disk_offset)
            .unwrap_or(head_offset);

        Ok(ScanResult {
            max_seq,
            used_bytes,
            head_offset,
            tail_offset,
            log_order,
            flushed_seqs,
        })
    }

    /// Guided recovery: scan only the occupied region [checkpoint.tail, checkpoint.head)
    /// plus a forward margin to catch entries written after the last checkpoint persist.
    /// Falls back to full scan if no entries are found in the guided region.
    fn rebuild_indices_guided(
        device: &RawDevice,
        capacity_bytes: u64,
        checkpoint: &ShardCheckpoint,
        lba_index: &DashMap<LbaKey, Arc<PendingEntry>>,
        latest_lba_seq: &DashMap<LbaKey, (u64, u64)>,
        pending_entries: &DashMap<u64, Arc<PendingEntry>>,
    ) -> OnyxResult<ScanResult> {
        #[derive(Debug)]
        struct ScannedRecord {
            seq: u64,
            disk_offset: u64,
            slot_count: u32,
            flushed: bool,
            pending: Option<Arc<PendingEntry>>,
        }

        // Validate checkpoint offsets are within bounds and block-aligned.
        let slot_sz = Self::slot_size();
        if checkpoint.tail_offset % slot_sz != 0
            || checkpoint.head_offset % slot_sz != 0
            || checkpoint.tail_offset >= capacity_bytes
            || checkpoint.head_offset >= capacity_bytes
        {
            tracing::warn!("shard checkpoint offsets invalid, falling back to full scan");
            return Self::rebuild_indices(
                device,
                capacity_bytes,
                lba_index,
                latest_lba_seq,
                pending_entries,
            );
        }

        let mut scanned = Vec::new();
        let mut max_seq = 0u64;
        let mut seen_offsets = HashSet::new();

        let mut record_scanned =
            |offset: u64, entry: BufferEntry, slot_count: u32, scanned: &mut Vec<ScannedRecord>| {
                if !seen_offsets.insert(offset) {
                    return;
                }
                let disk_len = Self::slot_bytes(slot_count) as u32;
                max_seq = max_seq.max(entry.seq);
                let pending = (!entry.flushed).then(|| {
                    Arc::new(PendingEntry {
                        seq: entry.seq,
                        vol_id: entry.vol_id.clone(),
                        start_lba: entry.start_lba,
                        lba_count: entry.lba_count,
                        payload_crc32: entry.payload_crc32,
                        vol_created_at: entry.vol_created_at,
                        payload: None, // lazy: hydrated from disk on demand
                        disk_offset: offset,
                        disk_len,
                        superseded_ranges: Vec::new(),
                    })
                });
                scanned.push(ScannedRecord {
                    seq: entry.seq,
                    disk_offset: offset,
                    slot_count,
                    flushed: entry.flushed,
                    pending,
                });
            };

        // Scan the occupied region, handling wrap-around.
        // Phase 1: scan [tail, head) (the known occupied region from checkpoint).
        // Phase 2: scan forward from checkpoint.head, but only while entries are
        // physically contiguous. Once we hit the first gap, later "valid-looking"
        // bytes are stale reclaimed history and must not be recovered.
        let mut occupied_ranges: Vec<(u64, u64)> = Vec::new();
        if checkpoint.used_bytes == 0 && checkpoint.head_offset == checkpoint.tail_offset {
            // Checkpoint says empty. Only do the contiguous forward scan below.
        } else if checkpoint.head_offset >= checkpoint.tail_offset {
            // No wrap in occupied region: [tail, head)
            occupied_ranges.push((checkpoint.tail_offset, checkpoint.head_offset));
        } else {
            // Wrap-around: [tail, capacity) + [0, head)
            occupied_ranges.push((checkpoint.tail_offset, capacity_bytes));
            occupied_ranges.push((0, checkpoint.head_offset));
        }

        // Scan the checkpoint-declared occupied region. Here we can tolerate
        // gaps and continue scanning slot-by-slot because corruption should not
        // hide later still-live entries inside the known used range.
        for (range_start, range_end) in &occupied_ranges {
            let mut offset = *range_start;
            while offset < *range_end {
                match Self::scan_entry(device, offset, capacity_bytes)? {
                    Some((entry, slot_count)) => {
                        record_scanned(offset, entry, slot_count, &mut scanned);
                        offset += Self::slot_bytes(slot_count);
                    }
                    None => {
                        offset += slot_sz;
                    }
                }
            }
        }

        // Scan forward from the checkpoint head to catch entries appended after
        // the last checkpoint write. These entries must be contiguous from the
        // old head and must keep increasing in seq beyond checkpoint.max_seq.
        //
        // A fixed forward-scan window is not safe here: a single committed
        // batch can easily exceed 1024 slots under heavy load, and truncating
        // recovery there loses still-live entries while rebuilding a too-small
        // ring that later reuses live buffer space.
        let mut forward_offset = checkpoint.head_offset;
        let mut forward_scanned_bytes = 0u64;
        let mut last_forward_seq = checkpoint.max_seq;
        while forward_scanned_bytes < capacity_bytes {
            match Self::scan_entry(device, forward_offset, capacity_bytes)? {
                Some((entry, slot_count)) => {
                    if entry.seq <= last_forward_seq {
                        break;
                    }
                    last_forward_seq = entry.seq;
                    record_scanned(forward_offset, entry, slot_count, &mut scanned);
                    let step = Self::slot_bytes(slot_count);
                    forward_offset = (forward_offset + step) % capacity_bytes;
                    forward_scanned_bytes = forward_scanned_bytes.saturating_add(step);
                }
                None => break,
            }
        }

        scanned.sort_by_key(|record| record.seq);

        for record in &scanned {
            let Some(pending) = record.pending.as_ref() else {
                continue;
            };
            let vid: Arc<str> = Arc::from(pending.vol_id.as_str());
            pending_entries.insert(pending.seq, pending.clone());
            for i in 0..pending.lba_count {
                let key = LbaKey {
                    vol_id: vid.clone(),
                    lba: Lba(pending.start_lba.0 + i as u64),
                };
                lba_index.insert(key.clone(), pending.clone());
                latest_lba_seq.insert(key, (pending.seq, pending.vol_created_at));
            }
        }

        let first_unreclaimed = scanned.iter().position(|record| !record.flushed);
        let mut log_order = VecDeque::new();
        let mut flushed_seqs = HashSet::new();
        let mut used_bytes = 0u64;

        if let Some(idx) = first_unreclaimed {
            for record in &scanned[idx..] {
                log_order.push_back(LogRecord {
                    seq: record.seq,
                    disk_offset: record.disk_offset,
                    slot_count: record.slot_count,
                });
                used_bytes += Self::slot_bytes(record.slot_count);
                if record.flushed {
                    flushed_seqs.insert(record.seq);
                }
            }
        }

        let head_offset = if let Some(last) = log_order.back() {
            (last.disk_offset + Self::slot_bytes(last.slot_count)) % capacity_bytes
        } else {
            scanned
                .last()
                .map(|last| (last.disk_offset + Self::slot_bytes(last.slot_count)) % capacity_bytes)
                .unwrap_or(checkpoint.head_offset)
        };
        let tail_offset = log_order
            .front()
            .map(|first| first.disk_offset)
            .unwrap_or(head_offset);

        Ok(ScanResult {
            max_seq,
            used_bytes,
            head_offset,
            tail_offset,
            log_order,
            flushed_seqs,
        })
    }

    fn open(
        device: RawDevice,
        backpressure_timeout: Duration,
        metrics: Arc<OnceLock<Arc<EngineMetrics>>>,
        checkpoint: Option<ShardCheckpoint>,
        checkpoint_device: Option<RawDevice>,
        payload_bytes_in_memory: Arc<AtomicU64>,
        max_payload_memory_bytes: u64,
    ) -> OnyxResult<(Self, u64)> {
        let capacity_bytes = device.size();
        if capacity_bytes < Self::slot_size() {
            return Err(OnyxError::Config(
                "persistent slot shard too small for any entries".into(),
            ));
        }

        let lba_index = DashMap::with_shard_amount(DASHMAP_SHARDS);
        let latest_lba_seq = DashMap::with_shard_amount(DASHMAP_SHARDS);
        let pending_entries = DashMap::with_shard_amount(DASHMAP_SHARDS);
        let recover_start = Instant::now();
        let scan = if let Some(ref ckpt) = checkpoint {
            let r = Self::rebuild_indices_guided(
                &device,
                capacity_bytes,
                ckpt,
                &lba_index,
                &latest_lba_seq,
                &pending_entries,
            )?;
            tracing::info!(
                elapsed_us = recover_start.elapsed().as_micros() as u64,
                pending = pending_entries.len(),
                head = ckpt.head_offset,
                tail = ckpt.tail_offset,
                "shard recovery (checkpoint-guided)"
            );
            r
        } else {
            let r = Self::rebuild_indices(
                &device,
                capacity_bytes,
                &lba_index,
                &latest_lba_seq,
                &pending_entries,
            )?;
            tracing::info!(
                elapsed_us = recover_start.elapsed().as_micros() as u64,
                pending = pending_entries.len(),
                capacity_bytes,
                "shard recovery (full scan)"
            );
            r
        };

        let (staging_tx, staging_rx) = unbounded();
        let mut log_order = VecDeque::with_capacity(scan.log_order.len());
        log_order.extend(scan.log_order);

        Ok((
            Self {
                device,
                ring: parking_lot::Mutex::new(RingState {
                    used_bytes: scan.used_bytes,
                    capacity_bytes,
                    reclaim_ready: 0,
                    head_offset: scan.head_offset,
                    tail_offset: scan.tail_offset,
                    log_order,
                    flushed_seqs: scan.flushed_seqs,
                }),
                ring_space_cv: parking_lot::Condvar::new(),
                backpressure_timeout,
                lba_index,
                latest_lba_seq,
                pending_entries,
                flush_progress: DashMap::with_shard_amount(DASHMAP_SHARDS),
                staging_tx,
                staging_rx,
                lifecycle: parking_lot::Mutex::new(LifecycleState {
                    inflight: HashSet::with_capacity(256),
                    cancelled: HashSet::with_capacity(64),
                }),
                io_lock: parking_lot::Mutex::new(()),
                vol_id_cache: RwLock::new(Vec::with_capacity(16)),
                metrics,
                checkpoint_device,
                payload_bytes_in_memory,
                max_payload_memory_bytes,
            },
            scan.max_seq,
        ))
    }

    /// Snapshot current ring state into a checkpoint structure.
    fn snapshot_checkpoint(&self) -> ShardCheckpoint {
        let ring = self.ring.lock();
        ShardCheckpoint {
            head_offset: ring.head_offset,
            tail_offset: ring.tail_offset,
            max_seq: 0, // updated by caller with global max_seq
            used_bytes: ring.used_bytes,
        }
    }

    /// Write the current checkpoint to disk (no sync — this is a hint).
    fn write_checkpoint(&self, max_seq: u64) {
        let Some(ref ckpt_dev) = self.checkpoint_device else {
            return;
        };
        let mut ckpt = self.snapshot_checkpoint();
        ckpt.max_seq = max_seq;
        if let Err(e) = ckpt_dev.write_at(&ckpt.encode(), 0) {
            tracing::debug!(error = %e, "failed to persist shard checkpoint (non-fatal)");
        }
    }

    /// After fdatasync, evict in-memory payloads if memory pressure is high.
    /// Entries remain in pending_entries with payload=None; reads/flusher
    /// will hydrate from the buffer device on demand.
    fn maybe_evict_payloads(&self, committed: &[Arc<PendingEntry>]) {
        let current = self.payload_bytes_in_memory.load(Ordering::Relaxed);
        let limit = self.max_payload_memory();
        if limit == 0 || current <= limit {
            return;
        }
        for pending in committed {
            if !self.pending_entries.contains_key(&pending.seq) {
                continue;
            }
            if let Some(ref p) = pending.payload {
                let payload_len = p.len() as u64;
                // Build an evicted copy (same metadata, payload=None).
                let evicted = Arc::new(PendingEntry {
                    seq: pending.seq,
                    vol_id: pending.vol_id.clone(),
                    start_lba: pending.start_lba,
                    lba_count: pending.lba_count,
                    payload_crc32: pending.payload_crc32,
                    vol_created_at: pending.vol_created_at,
                    payload: None,
                    disk_offset: pending.disk_offset,
                    disk_len: pending.disk_len,
                    superseded_ranges: pending.superseded_ranges.clone(),
                });
                // Replace in pending_entries.
                self.pending_entries.insert(pending.seq, evicted.clone());
                self.payload_bytes_in_memory
                    .fetch_sub(payload_len, Ordering::Relaxed);
                // Replace in lba_index entries.
                let vid = self.intern_vol_id(&pending.vol_id);
                for i in 0..pending.lba_count {
                    let key = LbaKey {
                        vol_id: vid.clone(),
                        lba: Lba(pending.start_lba.0 + i as u64),
                    };
                    if let Some(existing) = self.lba_index.get(&key) {
                        if existing.seq == pending.seq {
                            drop(existing);
                            self.lba_index.insert(key, evicted.clone());
                        }
                    }
                }
            }
            // Stop evicting once below limit.
            if self.payload_bytes_in_memory.load(Ordering::Relaxed) <= limit {
                break;
            }
        }
    }

    fn max_payload_memory(&self) -> u64 {
        self.max_payload_memory_bytes
    }

    fn backpressure_waits_forever(&self) -> bool {
        self.backpressure_timeout == Duration::MAX
    }

    fn retire_superseded_by_durable_entries(&self, committed: &[Arc<PendingEntry>]) {
        for pending in committed {
            for (old_seq, lba_start, lba_count) in &pending.superseded_ranges {
                if let Err(e) = self.mark_flushed(*old_seq, *lba_start, *lba_count) {
                    tracing::warn!(
                        new_seq = pending.seq,
                        old_seq,
                        start_lba = lba_start.0,
                        lba_count,
                        error = %e,
                        "failed to retire superseded buffered range"
                    );
                }
            }
        }
    }

    fn compact_recovered_stale_ranges(&self) {
        let mut entries: Vec<Arc<PendingEntry>> = self
            .pending_entries
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        entries.sort_by_key(|entry| entry.seq);

        for entry in entries {
            let mut stale_ranges = Vec::new();
            for i in 0..entry.lba_count {
                let lba = Lba(entry.start_lba.0 + i as u64);
                if !self.is_latest_lba_seq(&entry.vol_id, lba, entry.seq, entry.vol_created_at) {
                    Self::add_seq_lba_range(&mut stale_ranges, entry.seq, lba);
                }
            }

            for (seq, lba_start, lba_count) in stale_ranges {
                if let Err(e) = self.mark_flushed(seq, lba_start, lba_count) {
                    tracing::warn!(
                        seq,
                        start_lba = lba_start.0,
                        lba_count,
                        error = %e,
                        "failed to compact recovered stale buffered range"
                    );
                }
            }
        }
    }

    /// Hot-path append. No disk I/O, no CRC, no encoding.
    /// Locks: ring Mutex (~50ns), then DashMap inserts (concurrent).
    /// Channel send (lock-free ~30ns).
    fn append_with_seq(
        &self,
        seq: u64,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        payload: &[u8],
        vol_created_at: u64,
    ) -> OnyxResult<()> {
        if vol_id.is_empty() || vol_id.len() > MAX_VOLUME_ID_BYTES {
            return Err(OnyxError::Config(format!(
                "vol_id must be 1..{} bytes, got {}",
                MAX_VOLUME_ID_BYTES,
                vol_id.len()
            )));
        }
        if lba_count == 0 {
            return Err(OnyxError::Config("lba_count must be > 0".into()));
        }
        let expected_len = lba_count as usize * BLOCK_SIZE as usize;
        if payload.len() != expected_len {
            return Err(OnyxError::Config(format!(
                "payload must be {} bytes (lba_count={} * {}), got {}",
                expected_len,
                lba_count,
                BLOCK_SIZE,
                payload.len()
            )));
        }

        let raw_size = BufferEntry::raw_size_for(vol_id, payload.len());
        let disk_len = round_up(raw_size, BLOCK_SIZE as usize) as u32;
        let slot_count = disk_len / BLOCK_SIZE;
        if disk_len > MAX_ENTRY_SIZE {
            return Err(OnyxError::Config(format!(
                "entry too large: {} bytes (max {}). Reduce lba_count.",
                disk_len, MAX_ENTRY_SIZE
            )));
        }

        // ── Payload memory backpressure ──
        // If in-memory payload bytes exceed the limit, wait for flusher to drain
        // before accepting new writes. This prevents OOM when flusher is slow.
        let mem_limit = self.max_payload_memory();
        if mem_limit > 0 {
            let payload_size = payload.len() as u64;
            let deadline =
                if self.backpressure_timeout.is_zero() || self.backpressure_waits_forever() {
                    None
                } else {
                    Instant::now().checked_add(self.backpressure_timeout)
                };
            loop {
                let current = self.payload_bytes_in_memory.load(Ordering::Relaxed);
                if current + payload_size <= mem_limit {
                    break;
                }
                if self.backpressure_timeout.is_zero() {
                    return Err(OnyxError::BufferPoolFull(current as usize));
                }
                let wait_for = match deadline {
                    Some(deadline) => {
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        if remaining.is_zero() {
                            tracing::warn!(
                                current_mb = current / (1024 * 1024),
                                limit_mb = mem_limit / (1024 * 1024),
                                "append rejected: payload memory limit exceeded"
                            );
                            return Err(OnyxError::BufferPoolFull(current as usize));
                        }
                        remaining.min(BACKPRESSURE_POLL_INTERVAL)
                    }
                    None => BACKPRESSURE_POLL_INTERVAL,
                };
                // Wait for flusher to free memory (reuse ring_space_cv; flusher
                // notifies it when mark_flushed reclaims ring + payload space).
                let mut ring = self.ring.lock();
                let _ = self.ring_space_cv.wait_for(&mut ring, wait_for);
            }
        }

        // ── Ring lock: reserve space, wait if shard is temporarily full ──
        // The flush lane will drain entries and notify ring_space_cv.
        let write_offset = {
            let mut ring = self.ring.lock();
            loop {
                if let Some(offset) = Self::reserve_log_space(&mut ring, seq, slot_count) {
                    break offset;
                }
                // Entry physically cannot fit even in empty ring → real error.
                if Self::slot_bytes(slot_count) > ring.capacity_bytes {
                    return Err(OnyxError::BufferPoolFull(ring.used_bytes as usize));
                }
                // No backpressure configured (tests) → fail immediately.
                if self.backpressure_timeout.is_zero() {
                    return Err(OnyxError::BufferPoolFull(ring.used_bytes as usize));
                }
                if self.backpressure_waits_forever() {
                    let _ = self
                        .ring_space_cv
                        .wait_for(&mut ring, BACKPRESSURE_POLL_INTERVAL);
                    continue;
                }
                // Wait for flush lane to free space (condvar releases ring lock).
                let wait = self
                    .ring_space_cv
                    .wait_for(&mut ring, self.backpressure_timeout);
                if wait.timed_out() {
                    return Err(OnyxError::BufferPoolFull(ring.used_bytes as usize));
                }
            }
        };

        // Track in-memory payload bytes.
        self.payload_bytes_in_memory
            .fetch_add(payload.len() as u64, Ordering::Relaxed);

        let vid = self.intern_vol_id(vol_id);
        let mut keys = Vec::with_capacity(lba_count as usize);
        let mut superseded_ranges = Vec::new();
        for i in 0..lba_count {
            let lba = Lba(start_lba.0 + i as u64);
            let key = LbaKey {
                vol_id: vid.clone(),
                lba,
            };
            if let Some(existing) = self.lba_index.get(&key) {
                if existing.seq != seq {
                    Self::add_seq_lba_range(&mut superseded_ranges, existing.seq, lba);
                }
            }
            keys.push(key);
        }

        // Build PendingEntry — one String alloc (vol_id) + one Vec alloc (payload).
        let pending = Arc::new(PendingEntry {
            seq,
            vol_id: vol_id.to_string(),
            start_lba,
            lba_count,
            payload_crc32: 0,
            vol_created_at,
            payload: Some(Arc::from(payload)),
            disk_offset: write_offset,
            disk_len,
            superseded_ranges,
        });

        // ── DashMap inserts (concurrent sharded locks) ──
        // Interned Arc<str>: read-lock fast path, no alloc after first encounter.
        for key in keys {
            self.lba_index.insert(key.clone(), pending.clone());
            self.latest_lba_seq.insert(key, (seq, vol_created_at));
        }
        self.pending_entries.insert(seq, pending.clone());

        // ── Channel send (lock-free MPSC, ~30ns) ──
        let _ = self.staging_tx.send(pending);

        if let Some(metrics) = self.metrics.get() {
            metrics.buffer_appends.fetch_add(1, Ordering::Relaxed);
            metrics
                .buffer_append_bytes
                .fetch_add(payload.len() as u64, Ordering::Relaxed);
        }
        Ok(())
    }

    fn drain_staged(&self) -> Vec<Arc<PendingEntry>> {
        let mut batch = Vec::new();
        while let Ok(entry) = self.staging_rx.try_recv() {
            batch.push(entry);
        }
        if !batch.is_empty() {
            let mut lc = self.lifecycle.lock();
            for p in &batch {
                lc.inflight.insert(p.seq);
            }
        }
        batch
    }

    fn used_bytes(&self) -> u64 {
        self.ring.lock().used_bytes
    }

    fn capacity(&self) -> u64 {
        self.ring.lock().capacity_bytes
    }

    /// Read payload from the buffer device for a recovered entry.
    fn read_payload_from_disk(&self, pending: &PendingEntry) -> OnyxResult<Arc<[u8]>> {
        let slot_bytes = pending.disk_len as usize;
        let mut buf = vec![0u8; slot_bytes];
        self.device.read_at(&mut buf, pending.disk_offset)?;
        let entry = BufferEntry::from_bytes(&buf).ok_or_else(|| {
            OnyxError::Io(std::io::Error::other(format!(
                "failed to parse entry at offset {} during payload hydration",
                pending.disk_offset,
            )))
        })?;
        Ok(entry.payload)
    }

    /// Return a PendingEntry with payload guaranteed present. If the entry
    /// was recovered without payload (lazy), reads it from disk now.
    /// If hydration fails (corrupt disk region), the entry is evicted from
    /// all indices so subsequent reads fall through to the blockmap (LV3).
    fn lookup_hydrated(&self, vol_id: &str, lba: Lba) -> OnyxResult<Option<PendingEntry>> {
        let vid = self.intern_vol_id(vol_id);
        let Some(entry_ref) = self.lba_index.get(&LbaKey { vol_id: vid, lba }) else {
            return Ok(None);
        };
        let entry = &*entry_ref;
        if entry.payload.is_some() {
            return Ok(Some((**entry_ref).clone()));
        }
        // Lazy hydration: read payload from buffer device.
        let seq = entry.seq;
        match self.read_payload_from_disk(entry) {
            Ok(payload) => {
                let mut hydrated = (**entry_ref).clone();
                hydrated.payload = Some(payload);
                Ok(Some(hydrated))
            }
            Err(e) => {
                tracing::warn!(seq, error = %e, "read-path hydration failed, evicting corrupt entry");
                drop(entry_ref);
                self.evict_corrupt_entry(seq);
                // Return None — caller falls through to blockmap/LV3.
                Ok(None)
            }
        }
    }

    /// Remove LBA index entries for a range so reads see unmapped immediately.
    /// Does not remove pending_entries (flusher handles stale entries gracefully).
    fn invalidate_lba_range(&self, vol_id: &str, start_lba: Lba, lba_count: u32) {
        let vid = self.intern_vol_id(vol_id);
        let mut key = LbaKey {
            vol_id: vid,
            lba: start_lba,
        };
        for i in 0..lba_count as u64 {
            key.lba = Lba(start_lba.0 + i);
            self.lba_index.remove(&key);
        }
    }

    fn is_latest_lba_seq(&self, vol_id: &str, lba: Lba, seq: u64, vol_created_at: u64) -> bool {
        let vid = self.intern_vol_id(vol_id);
        self.latest_lba_seq
            .get(&LbaKey { vol_id: vid, lba })
            .map(|entry| {
                let (latest_seq, latest_created_at) = *entry;
                latest_seq == seq
                    && (latest_created_at == 0
                        || vol_created_at == 0
                        || latest_created_at == vol_created_at)
            })
            .unwrap_or(true)
    }

    fn pending_to_buffer_entry(pending: &PendingEntry) -> BufferEntry {
        BufferEntry {
            seq: pending.seq,
            vol_id: pending.vol_id.clone(),
            start_lba: pending.start_lba,
            lba_count: pending.lba_count,
            payload_crc32: pending.payload_crc32,
            flushed: false,
            vol_created_at: pending.vol_created_at,
            payload: pending
                .payload
                .clone()
                .unwrap_or_else(|| Arc::from(Vec::new())),
        }
    }

    fn pending_entry(&self, seq: u64) -> Option<BufferEntry> {
        self.pending_entries
            .get(&seq)
            .map(|entry| Self::pending_to_buffer_entry(&entry))
    }

    fn pending_entry_arc(&self, seq: u64) -> Option<Arc<PendingEntry>> {
        self.pending_entries
            .get(&seq)
            .map(|entry| entry.value().clone())
    }

    /// Evict a corrupt/unreadable pending entry: remove from all indices
    /// and reclaim ring space. Called when hydration fails (e.g. the disk
    /// region was overwritten by ring wrap-around or a partial write on crash).
    fn evict_corrupt_entry(&self, seq: u64) {
        let Some((_, pending)) = self.pending_entries.remove(&seq) else {
            return;
        };
        let vid = self.intern_vol_id(&pending.vol_id);
        for i in 0..pending.lba_count {
            let key = LbaKey {
                vol_id: vid.clone(),
                lba: Lba(pending.start_lba.0 + i as u64),
            };
            self.lba_index.remove_if(&key, |_, value| value.seq == seq);
            self.latest_lba_seq.remove_if(&key, |_, &(s, _)| s == seq);
        }
        if let Some(ref p) = pending.payload {
            self.payload_bytes_in_memory
                .fetch_sub(p.len() as u64, Ordering::Relaxed);
        }
        self.free_seq_allocation(seq, &pending);
        tracing::info!(
            seq,
            vol_id = %pending.vol_id,
            start_lba = pending.start_lba.0,
            lba_count = pending.lba_count,
            "evicted corrupt buffer entry (disk data unreadable)"
        );
    }

    /// Return Arc<PendingEntry> with payload hydrated from disk if needed.
    /// Returns None if payload memory limit is exceeded (flusher will retry later).
    fn pending_entry_arc_hydrated(&self, seq: u64) -> Option<Arc<PendingEntry>> {
        let entry_ref = self.pending_entries.get(&seq)?;
        let entry = entry_ref.value();
        if entry.payload.is_some() {
            return Some(entry.clone());
        }
        // Memory guard: refuse to hydrate if payload memory is already over limit.
        // The flusher's retry-snapshot mechanism will pick this entry up later
        // once in-flight entries have been drained and memory is freed.
        let limit = self.max_payload_memory();
        if limit > 0 {
            let current = self.payload_bytes_in_memory.load(Ordering::Relaxed);
            if current >= limit {
                tracing::debug!(
                    seq,
                    current_mb = current / (1024 * 1024),
                    limit_mb = limit / (1024 * 1024),
                    "skipping hydration: payload memory limit reached"
                );
                return None;
            }
        }
        // Lazy hydration: read payload from disk.
        match self.read_payload_from_disk(entry) {
            Ok(payload) => {
                let payload_len = payload.len() as u64;
                let mut hydrated = (**entry).clone();
                hydrated.payload = Some(payload);
                let hydrated = Arc::new(hydrated);
                // Track memory for hydrated payload.
                self.payload_bytes_in_memory
                    .fetch_add(payload_len, Ordering::Relaxed);
                // Update the DashMap entry so future accesses don't re-read.
                drop(entry_ref);
                self.pending_entries.insert(seq, hydrated.clone());
                Some(hydrated)
            }
            Err(e) => {
                tracing::warn!(seq, error = %e, "failed to hydrate pending entry payload, evicting corrupt entry");
                drop(entry_ref);
                self.evict_corrupt_entry(seq);
                None
            }
        }
    }

    fn pending_entries_snapshot(&self) -> Vec<BufferEntry> {
        self.pending_entries
            .iter()
            .map(|entry| Self::pending_to_buffer_entry(&entry))
            .collect()
    }

    fn pending_entries_arc_snapshot(&self) -> Vec<Arc<PendingEntry>> {
        self.pending_entries
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    fn has_seq(&self, seq: u64) -> bool {
        self.pending_entries.contains_key(&seq)
    }

    /// Memory-only: reclaim ring space, cancel write thread if needed.
    /// No disk write — metadata commit to RocksDB is the durable record.
    /// On crash recovery, stale "unflushed" entries are detected by
    /// cross-checking against the blockmap.
    fn free_seq_allocation(&self, seq: u64, pending: &PendingEntry) {
        let slot_count = pending.disk_len / BLOCK_SIZE;
        self.lifecycle.lock().cancelled.insert(seq);
        {
            let mut ring = self.ring.lock();
            debug_assert!(ring.log_order.iter().any(|record| record.seq == seq
                && record.disk_offset == pending.disk_offset
                && record.slot_count == slot_count));
            ring.flushed_seqs.insert(seq);
            let before = ring.used_bytes;
            Self::reclaim_log_prefix(&mut ring);
            if ring.used_bytes < before {
                self.ring_space_cv.notify_all();
            }
        }
        self.flush_progress.remove(&seq);
    }

    /// Durable mark: writes flushed header to disk. Only used by purge_volume
    /// which needs disk-durable state before returning.
    fn free_seq_allocation_durable(&self, seq: u64, pending: &PendingEntry) -> OnyxResult<()> {
        self.lifecycle.lock().cancelled.insert(seq);
        {
            let _guard = self.io_lock.lock();
            Self::mark_entry_flushed(&self.device, pending)?;
        }
        {
            let _slot_count = pending.disk_len / BLOCK_SIZE;
            let mut ring = self.ring.lock();
            ring.flushed_seqs.insert(seq);
            let before = ring.used_bytes;
            Self::reclaim_log_prefix(&mut ring);
            if ring.used_bytes < before {
                self.ring_space_cv.notify_all();
            }
        }
        self.flush_progress.remove(&seq);
        Ok(())
    }

    fn mark_flushed(
        &self,
        seq: u64,
        flushed_lba_start: Lba,
        flushed_lba_count: u32,
    ) -> OnyxResult<()> {
        let Some(pending) = self
            .pending_entries
            .get(&seq)
            .map(|e| Arc::clone(e.value()))
        else {
            return Ok(());
        };

        let entry_start = pending.start_lba.0;

        if pending.lba_count == 1 {
            let covers = entry_start >= flushed_lba_start.0
                && entry_start < flushed_lba_start.0 + flushed_lba_count as u64;
            if !covers {
                return Ok(());
            }
            let vid = self.intern_vol_id(&pending.vol_id);
            self.lba_index.remove_if(
                &LbaKey {
                    vol_id: vid,
                    lba: pending.start_lba,
                },
                |_, value| value.seq == seq,
            );
            self.pending_entries.remove(&seq);
            if let Some(ref p) = pending.payload {
                self.payload_bytes_in_memory
                    .fetch_sub(p.len() as u64, Ordering::Relaxed);
            }
            self.free_seq_allocation(seq, &pending);
            return Ok(());
        }

        let all_done = {
            let mut flushed_offsets = self.flush_progress.entry(seq).or_default();
            for i in 0..flushed_lba_count {
                let abs_lba = flushed_lba_start.0 + i as u64;
                if abs_lba >= entry_start {
                    flushed_offsets.insert((abs_lba - entry_start) as u16);
                }
            }
            flushed_offsets.len() >= pending.lba_count as usize
        };
        if !all_done {
            return Ok(());
        }

        let vid = self.intern_vol_id(&pending.vol_id);
        for i in 0..pending.lba_count {
            self.lba_index.remove_if(
                &LbaKey {
                    vol_id: vid.clone(),
                    lba: Lba(pending.start_lba.0 + i as u64),
                },
                |_, value| value.seq == seq,
            );
        }
        self.pending_entries.remove(&seq);
        if let Some(ref p) = pending.payload {
            self.payload_bytes_in_memory
                .fetch_sub(p.len() as u64, Ordering::Relaxed);
        }
        self.free_seq_allocation(seq, &pending);
        Ok(())
    }

    fn advance_tail(&self) -> OnyxResult<u64> {
        let mut ring = self.ring.lock();
        let advanced = ring.reclaim_ready;
        ring.reclaim_ready = 0;
        Ok(advanced)
    }

    fn recover(&self) -> OnyxResult<Vec<BufferEntry>> {
        Ok(self.pending_entries_snapshot())
    }

    fn recover_metadata(&self) -> Vec<RecoveredMeta> {
        self.pending_entries
            .iter()
            .map(|entry| RecoveredMeta {
                seq: entry.seq,
                vol_id: entry.vol_id.clone(),
                start_lba: entry.start_lba,
                lba_count: entry.lba_count,
                vol_created_at: entry.vol_created_at,
            })
            .collect()
    }

    fn get_pending_arc(&self, seq: u64) -> Option<Arc<PendingEntry>> {
        self.pending_entry_arc(seq)
    }

    fn pending_count(&self) -> u64 {
        self.pending_entries.len() as u64
    }

    fn purge_volume(&self, vol_id: &str) -> OnyxResult<Vec<u64>> {
        if test_purge_fail_volumes().lock().unwrap().contains(vol_id) {
            return Err(OnyxError::Io(std::io::Error::other(format!(
                "injected purge failure for volume {vol_id}"
            ))));
        }

        let to_purge: Vec<(u64, Arc<PendingEntry>)> = {
            let mut seqs = HashSet::new();
            for entry in self.lba_index.iter() {
                if &*entry.key().vol_id == vol_id {
                    seqs.insert(entry.value().seq);
                }
            }
            seqs.into_iter()
                .filter_map(|seq| self.pending_entries.get(&seq).map(|p| (seq, p.clone())))
                .collect()
        };

        if to_purge.is_empty() {
            return Ok(Vec::new());
        }

        self.lba_index.retain(|key, _| &*key.vol_id != vol_id);
        self.latest_lba_seq.retain(|key, _| &*key.vol_id != vol_id);
        for (seq, _) in &to_purge {
            self.pending_entries.remove(seq);
            self.flush_progress.remove(seq);
        }

        let seqs: Vec<u64> = to_purge.iter().map(|(seq, _)| *seq).collect();
        for (seq, pending) in &to_purge {
            self.free_seq_allocation_durable(*seq, pending)?;
        }

        Ok(seqs)
    }

    fn discard_pending_seq_durable(&self, seq: u64) -> OnyxResult<bool> {
        let Some(pending) = self.pending_entries.get(&seq).map(|e| (*e).clone()) else {
            return Ok(false);
        };

        let vid = self.intern_vol_id(&pending.vol_id);
        for i in 0..pending.lba_count {
            self.lba_index.remove_if(
                &LbaKey {
                    vol_id: vid.clone(),
                    lba: Lba(pending.start_lba.0 + i as u64),
                },
                |_, value| value.seq == seq,
            );
        }
        self.pending_entries.remove(&seq);
        self.flush_progress.remove(&seq);
        self.free_seq_allocation_durable(seq, &pending)?;
        Ok(true)
    }
}

impl WriteBufferPool {
    /// V2 data bytes: device - superblock.
    fn total_data_bytes(device_size: u64) -> OnyxResult<u64> {
        device_size
            .checked_sub(COMMIT_LOG_SUPERBLOCK_SIZE)
            .ok_or_else(|| OnyxError::Config("persistent slot device too small".into()))
    }

    /// V3 data bytes: device - superblock - per-shard checkpoint blocks.
    fn total_data_bytes_v3(device_size: u64, shard_count: usize) -> OnyxResult<u64> {
        let overhead = COMMIT_LOG_SUPERBLOCK_SIZE + shard_count as u64 * SHARD_CHECKPOINT_SIZE;
        device_size
            .checked_sub(overhead)
            .ok_or_else(|| OnyxError::Config("persistent slot device too small".into()))
    }

    /// Offset where shard data areas begin in v3 layout.
    fn v3_data_area_start(shard_count: usize) -> u64 {
        COMMIT_LOG_SUPERBLOCK_SIZE + shard_count as u64 * SHARD_CHECKPOINT_SIZE
    }

    /// Read the on-disk shard count from the superblock. Returns None if the
    /// device has no valid superblock (first use).
    pub fn read_disk_shard_count(device: &RawDevice) -> OnyxResult<Option<usize>> {
        let mut buf = [0u8; COMMIT_LOG_SUPERBLOCK_SIZE as usize];
        device.read_at(&mut buf, 0)?;
        Ok(GlobalSuperblock::decode(&buf).map(|sb| sb.shard_count as usize))
    }

    fn validate_shard_count(shard_count: usize) -> OnyxResult<()> {
        if shard_count == 0 || shard_count > MAX_SHARDS_ON_DISK {
            return Err(OnyxError::Config(format!(
                "persistent slot buffer supports 1..{} shards, got {}",
                MAX_SHARDS_ON_DISK, shard_count
            )));
        }
        Ok(())
    }

    /// Read shard checkpoint from disk. Returns None if invalid.
    fn read_shard_checkpoint(
        device: &RawDevice,
        shard_idx: usize,
    ) -> OnyxResult<Option<ShardCheckpoint>> {
        let offset = COMMIT_LOG_SUPERBLOCK_SIZE + shard_idx as u64 * SHARD_CHECKPOINT_SIZE;
        let mut buf = [0u8; SHARD_CHECKPOINT_SIZE as usize];
        device.read_at(&mut buf, offset)?;
        Ok(ShardCheckpoint::decode(&buf))
    }

    /// Initialize checkpoint blocks to zero (used during v2→v3 migration).
    fn init_checkpoint_blocks(device: &RawDevice, shard_count: usize) -> OnyxResult<()> {
        let empty = ShardCheckpoint {
            head_offset: 0,
            tail_offset: 0,
            max_seq: 0,
            used_bytes: 0,
        };
        let encoded = empty.encode();
        for i in 0..shard_count {
            let offset = COMMIT_LOG_SUPERBLOCK_SIZE + i as u64 * SHARD_CHECKPOINT_SIZE;
            device.write_at(&encoded, offset)?;
        }
        Ok(())
    }

    /// Check whether the buffer device with an old shard layout has zero
    /// unflushed entries, meaning it is safe to reinitialize with a different
    /// shard count (or migrate to v3).
    fn check_old_layout_empty(device: &RawDevice, sb: &GlobalSuperblock) -> OnyxResult<bool> {
        let old_shards = sb.shard_count as usize;
        let device_size = device.size();
        let total_data = if sb.is_v3() {
            Self::total_data_bytes_v3(device_size, old_shards)?
        } else {
            Self::total_data_bytes(device_size)?
        };
        let bytes_per_shard = total_data / old_shards as u64;
        let data_area_start = if sb.is_v3() {
            Self::v3_data_area_start(old_shards)
        } else {
            COMMIT_LOG_SUPERBLOCK_SIZE
        };

        let mut consumed = 0u64;
        for i in 0..old_shards {
            let shard_bytes = if i == old_shards - 1 {
                total_data - consumed
            } else {
                bytes_per_shard
            };
            let shard_offset = data_area_start + consumed;
            consumed += shard_bytes;

            let shard_dev = device.slice(shard_offset, shard_bytes)?;
            let lba_index = DashMap::with_shard_amount(4);
            let latest_lba_seq = DashMap::with_shard_amount(4);
            let pending = DashMap::with_shard_amount(4);
            BufferShard::rebuild_indices(
                &shard_dev,
                shard_bytes,
                &lba_index,
                &latest_lba_seq,
                &pending,
            )?;

            if !pending.is_empty() {
                tracing::warn!(
                    shard = i,
                    pending = pending.len(),
                    "buffer shard has unflushed entries — cannot reinit"
                );
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn persist_superblock(&self, sync: bool) -> OnyxResult<()> {
        let sb = GlobalSuperblock {
            shard_count: self.shards.len() as u32,
            version: self.disk_version,
        };
        let bytes = sb.encode();
        self.root_device.write_at(&bytes, 0)?;
        if sync {
            Self::sync_device_impl(&self.root_device)?;
        }
        Ok(())
    }

    fn sync_device_impl(device: &RawDevice) -> OnyxResult<()> {
        let mut remaining_failures = test_sync_fail_remaining().lock().unwrap();
        if *remaining_failures > 0 {
            *remaining_failures -= 1;
            return Err(OnyxError::Io(std::io::Error::other(
                "injected persistent slot sync failure",
            )));
        }
        drop(remaining_failures);
        device.sync()
    }

    fn sync_retry_backoff(consecutive_failures: u32) -> Duration {
        let shift = consecutive_failures.saturating_sub(1).min(4);
        Duration::from_millis((1u64 << shift).min(16))
    }

    fn write_batch(
        device: &RawDevice,
        io_lock: &parking_lot::Mutex<()>,
        entries: &[Arc<PendingEntry>],
        metrics: &Arc<OnceLock<Arc<EngineMetrics>>>,
    ) -> OnyxResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut encoded: Vec<(u64, Vec<u8>)> = Vec::with_capacity(entries.len());
        for pending in entries {
            let payload = pending
                .payload
                .as_ref()
                .expect("write_batch: payload must be present for new entries");
            let payload_crc = crc32fast::hash(payload);
            let data = BufferEntry::encode(
                pending.seq,
                &pending.vol_id,
                pending.start_lba,
                pending.lba_count,
                payload_crc,
                false,
                pending.vol_created_at,
                payload,
            )?;
            encoded.push((pending.disk_offset, data));
        }

        let write_start = Instant::now();
        let _guard = io_lock.lock();
        let mut start = 0usize;
        while start < encoded.len() {
            let mut end = start + 1;
            let mut next_offset = encoded[start].0 + encoded[start].1.len() as u64;
            while end < encoded.len() && encoded[end].0 == next_offset {
                next_offset += encoded[end].1.len() as u64;
                end += 1;
            }
            let span = &encoded[start..end];
            let total_len: usize = span.iter().map(|(_, data)| data.len()).sum();
            let mut batch = AlignedBuf::new(total_len, false)?;
            let mut cursor = 0;
            for (_, data) in span {
                batch.as_mut_slice()[cursor..cursor + data.len()].copy_from_slice(data);
                cursor += data.len();
            }
            device.write_at(&batch.as_slice()[..total_len], span[0].0)?;
            start = end;
        }
        if let Some(metrics) = metrics.get() {
            BufferShard::record_metric(&metrics.buffer_append_log_write_ns, write_start);
        }
        Ok(())
    }

    fn sync_loop(
        device: RawDevice,
        shard: Arc<BufferShard>,
        group_commit_wait: Duration,
        wake_rx: Receiver<()>,
        shutdown: Arc<AtomicBool>,
        metrics: Arc<OnceLock<Arc<EngineMetrics>>>,
        ready_tx: Sender<u64>,
        shard_ready_tx: Sender<u64>,
    ) {
        let mut consecutive_failures = 0u32;
        let mut retry_after: Option<Instant> = None;
        let mut inflight: Vec<Arc<PendingEntry>> = Vec::new();
        let mut writes_applied = false;
        let batch_wait = if group_commit_wait.is_zero() {
            Duration::from_millis(1)
        } else {
            group_commit_wait
        };

        loop {
            if inflight.is_empty() {
                match wake_rx.recv_timeout(Duration::from_millis(50)) {
                    Ok(()) => {}
                    Err(RecvTimeoutError::Timeout) => {
                        if shutdown.load(Ordering::Relaxed) && shard.staging_rx.is_empty() {
                            return;
                        }
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        if shutdown.load(Ordering::Relaxed) && shard.staging_rx.is_empty() {
                            return;
                        }
                        continue;
                    }
                }
                while wake_rx.try_recv().is_ok() {}
                if !batch_wait.is_zero() {
                    let sleep_start = Instant::now();
                    thread::sleep(batch_wait);
                    if let Some(metrics) = metrics.get() {
                        BufferShard::record_metric(&metrics.buffer_sync_sleep_ns, sleep_start);
                    }
                    while wake_rx.try_recv().is_ok() {}
                }

                inflight = shard.drain_staged();
                if inflight.is_empty() {
                    if shutdown.load(Ordering::Relaxed) && shard.staging_rx.is_empty() {
                        return;
                    }
                    continue;
                }
                writes_applied = false;
            }

            if let Some(deadline) = retry_after {
                let now = Instant::now();
                if deadline > now {
                    let wait = deadline.duration_since(now).min(Duration::from_millis(10));
                    let _ = wake_rx.recv_timeout(wait);
                    continue;
                }
            }

            let batch_start = Instant::now();
            if !writes_applied {
                let writes_to_persist: Vec<Arc<PendingEntry>> = {
                    let lc = shard.lifecycle.lock();
                    inflight
                        .iter()
                        .filter(|p| !lc.cancelled.contains(&p.seq))
                        .cloned()
                        .collect()
                };
                match Self::write_batch(&device, &shard.io_lock, &writes_to_persist, &metrics) {
                    Ok(()) => {
                        writes_applied = true;
                    }
                    Err(err) => {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        retry_after =
                            Some(Instant::now() + Self::sync_retry_backoff(consecutive_failures));
                        tracing::warn!(
                            error = %err,
                            consecutive_failures,
                            "persistent slot batch write failed; retrying"
                        );
                        continue;
                    }
                }
            }

            match Self::sync_device_impl(&device) {
                Ok(()) => {
                    consecutive_failures = 0;
                    retry_after = None;
                    shard.retire_superseded_by_durable_entries(&inflight);
                    // Persist checkpoint hint (best-effort, no sync).
                    let batch_max_seq = inflight.iter().map(|p| p.seq).max().unwrap_or(0);
                    shard.write_checkpoint(batch_max_seq);
                    {
                        let mut lc = shard.lifecycle.lock();
                        for pending in &inflight {
                            lc.inflight.remove(&pending.seq);
                            if lc.cancelled.remove(&pending.seq) {
                                continue;
                            }
                            let _ = ready_tx.send(pending.seq);
                            let _ = shard_ready_tx.send(pending.seq);
                        }
                    }
                    // Memory pressure: evict payloads that are now durable on disk.
                    shard.maybe_evict_payloads(&inflight);
                    if let Some(metrics) = metrics.get() {
                        metrics.buffer_sync_batches.fetch_add(1, Ordering::Relaxed);
                        metrics
                            .buffer_sync_epochs_committed
                            .fetch_add(inflight.len() as u64, Ordering::Relaxed);
                    }
                    inflight.clear();
                    writes_applied = false;
                }
                Err(err) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    retry_after =
                        Some(Instant::now() + Self::sync_retry_backoff(consecutive_failures));
                    tracing::warn!(
                        error = %err,
                        consecutive_failures,
                        "persistent slot sync failed; retrying"
                    );
                }
            }

            if let Some(metrics) = metrics.get() {
                BufferShard::record_metric(&metrics.buffer_sync_batch_ns, batch_start);
            }
        }
    }

    /// Open with defaults. Backpressure timeout = 0 (immediate fail) — suitable
    /// for tests or standalone usage without a flusher.
    pub fn open(device: RawDevice) -> OnyxResult<Self> {
        Self::open_with_group_commit_wait(device, Duration::ZERO)
    }

    pub fn open_with_group_commit_wait(
        device: RawDevice,
        group_commit_wait: Duration,
    ) -> OnyxResult<Self> {
        Self::open_with_options(device, group_commit_wait, 1, 256, Duration::ZERO)
    }

    pub fn open_with_options(
        device: RawDevice,
        group_commit_wait: Duration,
        shard_count: usize,
        routing_zone_size_blocks: u64,
        backpressure_timeout: Duration,
    ) -> OnyxResult<Self> {
        Self::open_with_options_and_memory_limit(
            device,
            group_commit_wait,
            shard_count,
            routing_zone_size_blocks,
            backpressure_timeout,
            0,
        )
    }

    pub fn open_with_options_and_memory_limit(
        device: RawDevice,
        group_commit_wait: Duration,
        shard_count: usize,
        routing_zone_size_blocks: u64,
        backpressure_timeout: Duration,
        max_payload_memory: u64,
    ) -> OnyxResult<Self> {
        Self::validate_shard_count(shard_count)?;
        let routing_zone_size_blocks = routing_zone_size_blocks.max(1);
        let device_size = device.size();

        // ── Read or initialize superblock ────────────────────────────
        let mut sb_buf = [0u8; COMMIT_LOG_SUPERBLOCK_SIZE as usize];
        device.read_at(&mut sb_buf, 0)?;

        // Determine if we're using v3 layout (with per-shard checkpoints).
        let (use_v3, superblock) = match GlobalSuperblock::decode(&sb_buf) {
            Some(sb) if sb.shard_count as usize == shard_count && sb.is_v3() => {
                // Happy path: v3 with matching shard count.
                (true, sb)
            }
            Some(sb) if sb.shard_count as usize == shard_count && !sb.is_v3() => {
                // V2 with matching shard count — try to migrate.
                let is_clean = Self::check_old_layout_empty(&device, &sb)?;
                if is_clean {
                    tracing::info!("buffer is clean — upgrading v2 → v3 layout");
                    let new_sb = GlobalSuperblock::new(shard_count);
                    Self::init_checkpoint_blocks(&device, shard_count)?;
                    device.write_at(&new_sb.encode(), 0)?;
                    device.sync()?;
                    (true, new_sb)
                } else {
                    tracing::info!(
                        "buffer has unflushed entries — using v2 layout (full scan); \
                         will upgrade to v3 on next clean restart"
                    );
                    (false, sb)
                }
            }
            Some(sb) => {
                // Shard count mismatch — check if clean for reinit.
                let is_clean = Self::check_old_layout_empty(&device, &sb)?;
                if is_clean {
                    tracing::info!(
                        old_shards = sb.shard_count,
                        new_shards = shard_count,
                        "buffer is clean — reinitializing with new shard layout (v3)"
                    );
                    let new_sb = GlobalSuperblock::new(shard_count);
                    Self::init_checkpoint_blocks(&device, shard_count)?;
                    device.write_at(&new_sb.encode(), 0)?;
                    device.sync()?;
                    (true, new_sb)
                } else {
                    return Err(OnyxError::Config(format!(
                        "buffer shard mismatch: disk={} config={}; unflushed entries exist",
                        sb.shard_count, shard_count
                    )));
                }
            }
            None => {
                // Fresh device — initialize as v3.
                let sb = GlobalSuperblock::new(shard_count);
                Self::init_checkpoint_blocks(&device, shard_count)?;
                device.write_at(&sb.encode(), 0)?;
                device.sync()?;
                (true, sb)
            }
        };

        // ── Compute shard layout ─────────────────────────────────────
        let total_data_bytes = if use_v3 {
            Self::total_data_bytes_v3(device_size, shard_count)?
        } else {
            Self::total_data_bytes(device_size)?
        };
        if total_data_bytes < shard_count as u64 * BLOCK_SIZE as u64 {
            return Err(OnyxError::Config(format!(
                "persistent slot device too small for {} shards",
                shard_count
            )));
        }
        let data_area_start = if use_v3 {
            Self::v3_data_area_start(shard_count)
        } else {
            COMMIT_LOG_SUPERBLOCK_SIZE
        };
        let bytes_per_shard = total_data_bytes / shard_count as u64;

        // Build per-shard config for parallel open.
        struct ShardOpenConfig {
            data_device: RawDevice,
            checkpoint: Option<ShardCheckpoint>,
            checkpoint_device: Option<RawDevice>,
        }

        let mut shard_configs = Vec::with_capacity(shard_count);
        let mut consumed = 0u64;
        for shard_idx in 0..shard_count {
            let shard_bytes = if shard_idx + 1 == shard_count {
                total_data_bytes.saturating_sub(consumed)
            } else {
                bytes_per_shard
            };
            let shard_offset = data_area_start + consumed;
            consumed += shard_bytes;

            let data_device = device.slice(shard_offset, shard_bytes)?;
            let (checkpoint, checkpoint_device) = if use_v3 {
                let ckpt = Self::read_shard_checkpoint(&device, shard_idx)?;
                let ckpt_offset =
                    COMMIT_LOG_SUPERBLOCK_SIZE + shard_idx as u64 * SHARD_CHECKPOINT_SIZE;
                let ckpt_dev = device.slice(ckpt_offset, SHARD_CHECKPOINT_SIZE)?;
                // Valid checkpoint → guided recovery.
                // Invalid/corrupt → None → full scan fallback.
                (ckpt, Some(ckpt_dev))
            } else {
                (None, None)
            };
            shard_configs.push(ShardOpenConfig {
                data_device,
                checkpoint,
                checkpoint_device,
            });
        }

        // ── Parallel shard recovery ──────────────────────────────────
        let metrics = Arc::new(OnceLock::new());
        let payload_bytes_in_memory = Arc::new(AtomicU64::new(0));
        let shard_results: Vec<OnyxResult<(BufferShard, u64)>> = if shard_count > 1 {
            std::thread::scope(|s| {
                let handles: Vec<_> = shard_configs
                    .into_iter()
                    .map(|cfg| {
                        let m = metrics.clone();
                        let pb = payload_bytes_in_memory.clone();
                        s.spawn(move || {
                            BufferShard::open(
                                cfg.data_device,
                                backpressure_timeout,
                                m,
                                cfg.checkpoint,
                                cfg.checkpoint_device,
                                pb,
                                max_payload_memory,
                            )
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().expect("shard open thread panicked"))
                    .collect()
            })
        } else {
            // Single shard — no need for thread overhead.
            shard_configs
                .into_iter()
                .map(|cfg| {
                    BufferShard::open(
                        cfg.data_device,
                        backpressure_timeout,
                        metrics.clone(),
                        cfg.checkpoint,
                        cfg.checkpoint_device,
                        payload_bytes_in_memory.clone(),
                        max_payload_memory,
                    )
                })
                .collect()
        };

        // ── Sequential setup: channels + sync threads ────────────────
        let (ready_tx, ready_rx) = unbounded();
        let mut shard_ready_txs = Vec::with_capacity(shard_count);
        let mut shard_ready_rxs = Vec::with_capacity(shard_count);
        let mut shards = Vec::with_capacity(shard_count);
        let mut max_seq = 0u64;

        // Recompute consumed for sync device slices.
        consumed = 0u64;
        for (shard_idx, result) in shard_results.into_iter().enumerate() {
            let (shard, shard_max_seq) = result?;
            shard.compact_recovered_stale_ranges();

            let (shard_ready_tx, shard_ready_rx) = unbounded();
            let mut recovered_seqs: Vec<u64> = shard
                .pending_entries
                .iter()
                .map(|entry| *entry.key())
                .collect();
            recovered_seqs.sort_unstable();
            for seq in recovered_seqs {
                let _ = ready_tx.send(seq);
                let _ = shard_ready_tx.send(seq);
            }
            max_seq = max_seq.max(shard_max_seq);

            let shard_bytes = if shard_idx + 1 == shard_count {
                total_data_bytes.saturating_sub(consumed)
            } else {
                bytes_per_shard
            };
            let shard_offset = data_area_start + consumed;
            consumed += shard_bytes;

            let sync_device = device.slice(shard_offset, shard_bytes)?;
            let shard = Arc::new(shard);
            let (sync_wake_tx, sync_wake_rx) = unbounded();
            let sync_shutdown = Arc::new(AtomicBool::new(false));
            let sync_thread = thread::Builder::new()
                .name(format!("persistent-slot-sync-{}", shard_idx))
                .spawn({
                    let metrics = metrics.clone();
                    let shard = shard.clone();
                    let shutdown = sync_shutdown.clone();
                    let ready_tx = ready_tx.clone();
                    let shard_ready_tx = shard_ready_tx.clone();
                    move || {
                        Self::sync_loop(
                            sync_device,
                            shard,
                            group_commit_wait,
                            sync_wake_rx,
                            shutdown,
                            metrics,
                            ready_tx,
                            shard_ready_tx,
                        );
                    }
                })
                .map_err(|e| {
                    OnyxError::Config(format!(
                        "failed to spawn persistent slot sync thread for shard {}: {}",
                        shard_idx, e
                    ))
                })?;

            shard_ready_txs.push(shard_ready_tx);
            shard_ready_rxs.push(shard_ready_rx);
            shards.push(BufferShardHandle {
                shard,
                sync_wake_tx,
                sync_shutdown,
                sync_thread: Some(sync_thread),
            });
        }

        let disk_version = if use_v3 {
            COMMIT_LOG_VERSION
        } else {
            COMMIT_LOG_VERSION_V2
        };
        let pool = Self {
            root_device: device,
            shards,
            next_seq: AtomicU64::new(max_seq + 1),
            routing_zone_size_blocks,
            ready_rx,
            shard_ready_rxs,
            metrics,
            payload_bytes_in_memory,
            max_payload_memory,
            disk_version,
        };

        let expected_sb = GlobalSuperblock {
            shard_count: shard_count as u32,
            version: disk_version,
        };
        if superblock.encode() != expected_sb.encode() {
            pool.persist_superblock(true)?;
        }

        Ok(pool)
    }

    pub fn attach_metrics(&self, metrics: Arc<EngineMetrics>) {
        let _ = self.metrics.set(metrics.clone());
        for shard in &self.shards {
            let _ = shard.shard.metrics.set(metrics.clone());
        }
    }

    fn shard_for_lba(&self, lba: Lba) -> usize {
        if self.shards.len() == 1 {
            0
        } else {
            ((lba.0 / self.routing_zone_size_blocks) % self.shards.len() as u64) as usize
        }
    }

    /// Find the shard that owns a seq by checking each shard's pending_entries.
    /// O(shard_count) DashMap lookups — fine for background mark_flushed path.
    fn shard_for_seq(&self, seq: u64) -> Option<usize> {
        self.shards
            .iter()
            .position(|shard| shard.shard.has_seq(seq))
    }

    pub fn append(
        &self,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        payload: &[u8],
        vol_created_at: u64,
    ) -> OnyxResult<u64> {
        let total_start = Instant::now();
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let shard_idx = self.shard_for_lba(start_lba);
        let shard = &self.shards[shard_idx];

        shard
            .shard
            .append_with_seq(seq, vol_id, start_lba, lba_count, payload, vol_created_at)?;

        let _ = shard.sync_wake_tx.send(());
        if let Some(metrics) = self.metrics.get() {
            BufferShard::record_metric(&metrics.buffer_append_total_ns, total_start);
        }
        Ok(seq)
    }

    pub fn lookup(&self, vol_id: &str, lba: Lba) -> OnyxResult<Option<PendingEntry>> {
        let primary = self.shard_for_lba(lba);
        let mut result = self.shards[primary].shard.lookup_hydrated(vol_id, lba)?;
        for (idx, shard) in self.shards.iter().enumerate() {
            if idx == primary {
                continue;
            }
            if let Ok(Some(candidate)) = shard.shard.lookup_hydrated(vol_id, lba) {
                let replace = result
                    .as_ref()
                    .map(|current| {
                        candidate.seq > current.seq
                            || (candidate.seq == current.seq
                                && candidate.vol_created_at > current.vol_created_at)
                    })
                    .unwrap_or(true);
                if replace {
                    result = Some(candidate);
                }
            }
        }
        if let Some(metrics) = self.metrics.get() {
            let counter = if result.is_some() {
                &metrics.buffer_lookup_hits
            } else {
                &metrics.buffer_lookup_misses
            };
            counter.fetch_add(1, Ordering::Relaxed);
        }
        Ok(result)
    }

    pub fn pending_entry(&self, seq: u64) -> Option<BufferEntry> {
        self.shard_for_seq(seq)
            .and_then(|idx| self.shards[idx].shard.pending_entry(seq))
    }

    pub fn pending_entry_arc(&self, seq: u64) -> Option<Arc<PendingEntry>> {
        self.shard_for_seq(seq)
            .and_then(|idx| self.shards[idx].shard.pending_entry_arc_hydrated(seq))
    }

    pub fn is_latest_lba_seq(&self, vol_id: &str, lba: Lba, seq: u64, vol_created_at: u64) -> bool {
        let shard_idx = self.shard_for_lba(lba);
        self.shards[shard_idx]
            .shard
            .is_latest_lba_seq(vol_id, lba, seq, vol_created_at)
    }

    pub fn pending_entries_snapshot(&self) -> Vec<BufferEntry> {
        let mut entries = Vec::new();
        for shard in &self.shards {
            entries.extend(shard.shard.pending_entries_snapshot());
        }
        entries.sort_by_key(|entry| entry.seq);
        entries
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn pending_entries_snapshot_for_shard(&self, shard_idx: usize) -> Vec<BufferEntry> {
        self.shards
            .get(shard_idx)
            .map(|shard| {
                let mut entries = shard.shard.pending_entries_snapshot();
                entries.sort_by_key(|entry| entry.seq);
                entries
            })
            .unwrap_or_default()
    }

    pub fn pending_entries_arc_snapshot_for_shard(
        &self,
        shard_idx: usize,
    ) -> Vec<Arc<PendingEntry>> {
        self.shards
            .get(shard_idx)
            .map(|shard| {
                let mut entries = shard.shard.pending_entries_arc_snapshot();
                entries.sort_by_key(|entry| entry.seq);
                entries
            })
            .unwrap_or_default()
    }

    pub fn recv_ready_timeout(&self, timeout: Duration) -> Result<u64, RecvTimeoutError> {
        self.ready_rx.recv_timeout(timeout)
    }

    pub fn try_recv_ready(&self) -> Result<u64, TryRecvError> {
        self.ready_rx.try_recv()
    }

    pub fn recv_ready_timeout_for_shard(
        &self,
        shard_idx: usize,
        timeout: Duration,
    ) -> Result<u64, RecvTimeoutError> {
        self.shard_ready_rxs
            .get(shard_idx)
            .ok_or(RecvTimeoutError::Disconnected)?
            .recv_timeout(timeout)
    }

    pub fn try_recv_ready_for_shard(&self, shard_idx: usize) -> Result<u64, TryRecvError> {
        self.shard_ready_rxs
            .get(shard_idx)
            .ok_or(TryRecvError::Disconnected)?
            .try_recv()
    }

    pub fn mark_flushed(
        &self,
        seq: u64,
        flushed_lba_start: Lba,
        flushed_lba_count: u32,
    ) -> OnyxResult<()> {
        let Some(shard_idx) = self.shard_for_seq(seq) else {
            return Ok(());
        };
        self.shards[shard_idx]
            .shard
            .mark_flushed(seq, flushed_lba_start, flushed_lba_count)?;
        Ok(())
    }

    pub fn advance_tail(&self) -> OnyxResult<u64> {
        let mut advanced = 0u64;
        for shard in &self.shards {
            advanced += shard.shard.advance_tail()?;
        }
        Ok(advanced)
    }

    pub fn advance_tail_for_shard(&self, shard_idx: usize) -> OnyxResult<u64> {
        let Some(shard) = self.shards.get(shard_idx) else {
            return Ok(0);
        };
        shard.shard.advance_tail()
    }

    pub fn recover(&self) -> OnyxResult<Vec<BufferEntry>> {
        let mut result = Vec::new();
        for shard in &self.shards {
            result.extend(shard.shard.recover()?);
        }
        result.sort_by_key(|entry| entry.seq);
        Ok(result)
    }

    /// Return pending entry metadata without cloning payloads.
    pub fn recover_metadata(&self) -> Vec<RecoveredMeta> {
        let mut result = Vec::new();
        for shard in &self.shards {
            result.extend(shard.shard.recover_metadata());
        }
        result.sort_by_key(|m| m.seq);
        result
    }

    /// Get a zero-copy Arc handle to a pending entry (for payload access without clone).
    pub fn get_pending_arc(&self, seq: u64) -> Option<Arc<PendingEntry>> {
        let shard_idx = self.shard_for_seq(seq)?;
        self.shards[shard_idx].shard.get_pending_arc(seq)
    }

    #[cfg(test)]
    pub(crate) fn note_latest_lba_seq_for_test(
        &self,
        vol_id: &str,
        lba: Lba,
        seq: u64,
        vol_created_at: u64,
    ) {
        let shard_idx = self.shard_for_lba(lba);
        let shard = &self.shards[shard_idx].shard;
        let vid = shard.intern_vol_id(vol_id);
        shard
            .latest_lba_seq
            .insert(LbaKey { vol_id: vid, lba }, (seq, vol_created_at));
    }

    pub fn pending_count(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| shard.shard.pending_count())
            .sum()
    }

    pub fn capacity(&self) -> u64 {
        self.shards.iter().map(|shard| shard.shard.capacity()).sum()
    }

    pub fn purge_volume(&self, vol_id: &str) -> OnyxResult<u64> {
        let mut total = 0u64;
        for shard in self.shards.iter() {
            let purged = shard.shard.purge_volume(vol_id)?;
            total += purged.len() as u64;
        }
        Ok(total)
    }

    /// Invalidate buffer index entries for an LBA range across all shards.
    /// After this call, reads to these LBAs will no longer find buffered data.
    pub fn invalidate_lba_range(&self, vol_id: &str, start_lba: Lba, lba_count: u32) {
        for shard in self.shards.iter() {
            shard
                .shard
                .invalidate_lba_range(vol_id, start_lba, lba_count);
        }
    }

    pub fn discard_pending_seq_durable(&self, seq: u64) -> OnyxResult<bool> {
        let Some(shard_idx) = self.shard_for_seq(seq) else {
            return Ok(false);
        };
        self.shards[shard_idx]
            .shard
            .discard_pending_seq_durable(seq)
    }

    pub fn fill_percentage(&self) -> u8 {
        let total_capacity = self.capacity();
        if total_capacity == 0 {
            return 100;
        }
        let total_used: u64 = self
            .shards
            .iter()
            .map(|shard| shard.shard.used_bytes())
            .sum();
        ((total_used * 100) / total_capacity) as u8
    }

    /// Per-shard fill percentage. Used by flush lane to make per-lane
    /// backpressure decisions (e.g. dedup skip threshold).
    pub fn fill_percentage_for_shard(&self, shard_idx: usize) -> u8 {
        let Some(shard) = self.shards.get(shard_idx) else {
            return 100;
        };
        let cap = shard.shard.capacity();
        if cap == 0 {
            return 100;
        }
        ((shard.shard.used_bytes() * 100) / cap) as u8
    }

    /// Total payload bytes currently kept resident in memory across all shards.
    pub fn payload_memory_bytes(&self) -> u64 {
        self.payload_bytes_in_memory.load(Ordering::Relaxed)
    }

    /// Configured in-memory payload ceiling. 0 means "no limit".
    pub fn payload_memory_limit_bytes(&self) -> u64 {
        self.max_payload_memory
    }

    /// Snapshot per-shard buffer statistics for monitoring.
    pub fn shard_snapshots(&self) -> Vec<BufferShardSnapshot> {
        self.shards
            .iter()
            .enumerate()
            .map(|(idx, handle)| {
                let s = &handle.shard;
                let (used, capacity, head, tail) = {
                    let ring = s.ring.lock();
                    (
                        ring.used_bytes,
                        ring.capacity_bytes,
                        ring.head_offset,
                        ring.tail_offset,
                    )
                };
                let fill_pct = if capacity > 0 {
                    ((used * 100) / capacity) as u8
                } else {
                    100
                };
                BufferShardSnapshot {
                    shard_idx: idx,
                    used_bytes: used,
                    capacity_bytes: capacity,
                    fill_pct,
                    pending_entries: s.pending_count(),
                    head_offset: head,
                    tail_offset: tail,
                }
            })
            .collect()
    }
}

impl Drop for WriteBufferPool {
    fn drop(&mut self) {
        for shard in &self.shards {
            shard.sync_shutdown.store(true, Ordering::Relaxed);
            let _ = shard.sync_wake_tx.send(());
        }
        for shard in &mut self.shards {
            if let Some(handle) = shard.sync_thread.take() {
                let _ = handle.join();
            }
        }
        // Persist final checkpoint for each shard so recovery is fast.
        let global_max_seq = self.next_seq.load(Ordering::Relaxed).saturating_sub(1);
        for shard in &self.shards {
            shard.shard.write_checkpoint(global_max_seq);
        }
        let _ = self.persist_superblock(true);
    }
}
