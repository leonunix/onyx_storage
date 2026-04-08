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
use crate::metrics::EngineMetrics;
use crate::types::{Lba, BLOCK_SIZE};

const COMMIT_LOG_MAGIC: u32 = 0x4F43_4C47; // "OCLG"
const COMMIT_LOG_VERSION: u32 = 2;
const COMMIT_LOG_SUPERBLOCK_SIZE: u64 = 4096;
const MAX_SHARDS_ON_DISK: usize = 64;
/// DashMap internal shard count — high value reduces contention under many writers.
const DASHMAP_SHARDS: usize = 256;

#[derive(Debug, Clone)]
pub struct PendingEntry {
    pub seq: u64,
    pub vol_id: String,
    pub start_lba: Lba,
    pub lba_count: u32,
    pub payload_crc32: u32,
    pub vol_created_at: u64,
    pub payload: Vec<u8>,
    pub disk_offset: u64,
    pub disk_len: u32,
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
}

impl GlobalSuperblock {
    fn new(shard_count: usize) -> Self {
        Self {
            shard_count: shard_count as u32,
        }
    }

    fn encode(&self) -> [u8; COMMIT_LOG_SUPERBLOCK_SIZE as usize] {
        let mut buf = [0u8; COMMIT_LOG_SUPERBLOCK_SIZE as usize];
        buf[0..4].copy_from_slice(&COMMIT_LOG_MAGIC.to_le_bytes());
        buf[4..8].copy_from_slice(&COMMIT_LOG_VERSION.to_le_bytes());
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
        if version != COMMIT_LOG_VERSION {
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
        Some(Self { shard_count })
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
}

pub struct WriteBufferPool {
    root_device: RawDevice,
    shards: Vec<BufferShardHandle>,
    next_seq: AtomicU64,
    routing_zone_size_blocks: u64,
    ready_rx: Receiver<u64>,
    shard_ready_rxs: Vec<Receiver<u64>>,
    metrics: Arc<OnceLock<Arc<EngineMetrics>>>,
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
        if device.is_direct_io() {
            let bytes = BufferEntry::encode_direct_compact_header(
                pending.seq,
                &pending.vol_id,
                pending.start_lba,
                pending.lba_count,
                true,
                pending.vol_created_at,
                pending.payload.len(),
            )?;
            device.write_at(
                &bytes.as_slice()[..BLOCK_SIZE as usize],
                pending.disk_offset,
            )?;
        } else {
            let (header, _) = BufferEntry::encode_compact_parts(
                pending.seq,
                &pending.vol_id,
                pending.start_lba,
                pending.lba_count,
                true,
                pending.vol_created_at,
                &pending.payload,
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
                            payload: entry.payload,
                            disk_offset: offset,
                            disk_len,
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
                lba_index.insert(
                    LbaKey {
                        vol_id: vid.clone(),
                        lba: Lba(pending.start_lba.0 + i as u64),
                    },
                    pending.clone(),
                );
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

    fn open(
        device: RawDevice,
        backpressure_timeout: Duration,
        metrics: Arc<OnceLock<Arc<EngineMetrics>>>,
    ) -> OnyxResult<(Self, u64)> {
        let capacity_bytes = device.size();
        if capacity_bytes < Self::slot_size() {
            return Err(OnyxError::Config(
                "persistent slot shard too small for any entries".into(),
            ));
        }

        let lba_index = DashMap::with_shard_amount(DASHMAP_SHARDS);
        let pending_entries = DashMap::with_shard_amount(DASHMAP_SHARDS);
        let scan =
            Self::rebuild_indices(&device, capacity_bytes, &lba_index, &pending_entries)?;

        let (staging_tx, staging_rx) = unbounded();
        // Pre-size ring collections to avoid allocs inside the lock.
        let max_entries = (capacity_bytes / Self::slot_size()) as usize;
        let mut log_order = VecDeque::with_capacity(max_entries);
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
            },
            scan.max_seq,
        ))
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
                expected_len, lba_count, BLOCK_SIZE, payload.len()
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
                // Wait for flush lane to free space (condvar releases ring lock).
                let wait = self
                    .ring_space_cv
                    .wait_for(&mut ring, self.backpressure_timeout);
                if wait.timed_out() {
                    return Err(OnyxError::BufferPoolFull(ring.used_bytes as usize));
                }
            }
        };

        // Build PendingEntry — one String alloc (vol_id) + one Vec alloc (payload).
        let pending = Arc::new(PendingEntry {
            seq,
            vol_id: vol_id.to_string(),
            start_lba,
            lba_count,
            payload_crc32: 0,
            vol_created_at,
            payload: payload.to_vec(),
            disk_offset: write_offset,
            disk_len,
        });

        // ── DashMap inserts (concurrent sharded locks) ──
        // Interned Arc<str>: read-lock fast path, no alloc after first encounter.
        let vid = self.intern_vol_id(vol_id);
        for i in 0..lba_count {
            self.lba_index.insert(
                LbaKey {
                    vol_id: vid.clone(),
                    lba: Lba(start_lba.0 + i as u64),
                },
                pending.clone(),
            );
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

    fn lookup(&self, vol_id: &str, lba: Lba) -> Option<PendingEntry> {
        let vid = self.intern_vol_id(vol_id);
        self.lba_index
            .get(&LbaKey { vol_id: vid, lba })
            .map(|entry| (**entry).clone())
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
            payload: pending.payload.clone(),
        }
    }

    fn pending_entry(&self, seq: u64) -> Option<BufferEntry> {
        self.pending_entries
            .get(&seq)
            .map(|entry| Self::pending_to_buffer_entry(&entry))
    }

    fn pending_entries_snapshot(&self) -> Vec<BufferEntry> {
        self.pending_entries
            .iter()
            .map(|entry| Self::pending_to_buffer_entry(&entry))
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
        let Some(pending) = self.pending_entries.get(&seq).map(|e| (*e).clone()) else {
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

        self.lba_index
            .retain(|key, _| &*key.vol_id != vol_id);
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
}

impl WriteBufferPool {
    fn total_data_bytes(device_size: u64) -> OnyxResult<u64> {
        device_size
            .checked_sub(COMMIT_LOG_SUPERBLOCK_SIZE)
            .ok_or_else(|| OnyxError::Config("persistent slot device too small".into()))
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

    fn persist_superblock(&self, sync: bool) -> OnyxResult<()> {
        let bytes = GlobalSuperblock::new(self.shards.len()).encode();
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
            let payload_crc = crc32fast::hash(&pending.payload);
            let data = BufferEntry::encode(
                pending.seq,
                &pending.vol_id,
                pending.start_lba,
                pending.lba_count,
                payload_crc,
                false,
                pending.vol_created_at,
                &pending.payload,
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
        Self::validate_shard_count(shard_count)?;
        let routing_zone_size_blocks = routing_zone_size_blocks.max(1);
        let device_size = device.size();
        let total_data_bytes = Self::total_data_bytes(device_size)?;
        if total_data_bytes < shard_count as u64 * BLOCK_SIZE as u64 {
            return Err(OnyxError::Config(format!(
                "persistent slot device too small for {} shards",
                shard_count
            )));
        }

        let mut sb_buf = [0u8; COMMIT_LOG_SUPERBLOCK_SIZE as usize];
        device.read_at(&mut sb_buf, 0)?;
        let superblock = match GlobalSuperblock::decode(&sb_buf) {
            Some(sb) if sb.shard_count as usize == shard_count => sb,
            Some(sb) => {
                return Err(OnyxError::Config(format!(
                    "persistent slot shard layout mismatch: disk has {} shards, config requests {}",
                    sb.shard_count, shard_count
                )));
            }
            None => {
                let sb = GlobalSuperblock::new(shard_count);
                device.write_at(&sb.encode(), 0)?;
                device.sync()?;
                sb
            }
        };

        let bytes_per_shard = total_data_bytes / shard_count as u64;
        let mut consumed = 0u64;
        let metrics = Arc::new(OnceLock::new());
        let (ready_tx, ready_rx) = unbounded();
        let mut shard_ready_txs = Vec::with_capacity(shard_count);
        let mut shard_ready_rxs = Vec::with_capacity(shard_count);
        let mut shards = Vec::with_capacity(shard_count);
        let mut max_seq = 0u64;

        for shard_idx in 0..shard_count {
            let (shard_ready_tx, shard_ready_rx) = unbounded();
            shard_ready_txs.push(shard_ready_tx);
            shard_ready_rxs.push(shard_ready_rx);

            let shard_bytes = if shard_idx + 1 == shard_count {
                total_data_bytes.saturating_sub(consumed)
            } else {
                bytes_per_shard
            };
            let shard_offset = COMMIT_LOG_SUPERBLOCK_SIZE + consumed;
            consumed += shard_bytes;

            let shard_device = device.slice(shard_offset, shard_bytes)?;
            let sync_device = device.slice(shard_offset, shard_bytes)?;
            let (shard, shard_max_seq) = BufferShard::open(shard_device, backpressure_timeout, metrics.clone())?;
            for seq in shard.pending_entries.iter().map(|entry| *entry.key()) {
                let _ = ready_tx.send(seq);
                let _ = shard_ready_txs[shard_idx].send(seq);
            }
            max_seq = max_seq.max(shard_max_seq);

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
                    let shard_ready_tx = shard_ready_txs[shard_idx].clone();
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

            shards.push(BufferShardHandle {
                shard,
                sync_wake_tx,
                sync_shutdown,
                sync_thread: Some(sync_thread),
            });
        }

        let pool = Self {
            root_device: device,
            shards,
            next_seq: AtomicU64::new(max_seq + 1),
            routing_zone_size_blocks,
            ready_rx,
            shard_ready_rxs,
            metrics,
        };

        if superblock.encode() != GlobalSuperblock::new(shard_count).encode() {
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
        let result = self.shards[primary].shard.lookup(vol_id, lba).or_else(|| {
            self.shards
                .iter()
                .enumerate()
                .filter(|(idx, _)| *idx != primary)
                .find_map(|(_, shard)| shard.shard.lookup(vol_id, lba))
        });
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
        let _ = self.persist_superblock(true);
    }
}
