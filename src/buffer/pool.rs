use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::buffer::entry::*;
use crate::error::{OnyxError, OnyxResult};
use crate::io::aligned::round_up;
use crate::io::device::RawDevice;
use crate::meta::schema::MAX_VOLUME_ID_BYTES;
use crate::metrics::EngineMetrics;
use crate::types::{Lba, BLOCK_SIZE};

/// In-memory representation of a pending (unflushed) buffer entry.
#[derive(Debug, Clone)]
pub struct PendingEntry {
    pub seq: u64,
    pub vol_id: String,
    pub start_lba: Lba,
    pub lba_count: u32,
    pub payload: Vec<u8>,
    /// Byte offset on LV2 where this entry is stored (for mark_flushed).
    pub disk_offset: u64,
    /// 4KB-aligned size on disk.
    pub disk_len: u32,
}

/// Key for per-LBA lookup index.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LbaKey {
    vol_id: String,
    lba: Lba,
}

/// Write buffer pool with variable-length entries.
///
/// On-disk format: 4KB superblock + circular log of variable-length 4KB-aligned entries.
/// In-memory: per-LBA HashMap index pointing to Arc<PendingEntry> for O(1) lookup.
pub struct WriteBufferPool {
    device: RawDevice,
    superblock: Mutex<BufferSuperblock>,
    next_seq: AtomicU64,
    /// Per-LBA index: (vol_id, lba) → pending entry covering that LBA.
    /// A multi-LBA entry has lba_count keys all pointing to the same Arc.
    lba_index: Mutex<HashMap<LbaKey, Arc<PendingEntry>>>,
    /// Seq → disk location, for mark_flushed to find the entry on disk.
    seq_index: Mutex<HashMap<u64, (u64, u32)>>, // seq → (disk_offset, disk_len)
    /// Tracks which specific LBA offsets within each entry have been flushed.
    /// Uses a set of offsets (not a counter) so retrying the same LBAs is idempotent.
    /// Only when all offsets 0..lba_count are present do we mark the entry as flushed.
    flush_progress: Mutex<HashMap<u64, HashSet<u16>>>, // seq → set of flushed offset_in_entry
    metrics: OnceLock<Arc<EngineMetrics>>,
}

static TEST_PURGE_FAIL_VOLUMES: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

fn test_purge_fail_volumes() -> &'static Mutex<HashSet<String>> {
    TEST_PURGE_FAIL_VOLUMES.get_or_init(|| Mutex::new(HashSet::new()))
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

impl WriteBufferPool {
    /// Open the buffer pool. Rebuilds in-memory indices from the on-disk log.
    pub fn open(device: RawDevice) -> OnyxResult<Self> {
        let device_size = device.size();
        let capacity_bytes = device_size.saturating_sub(BUFFER_SUPERBLOCK_SIZE);

        if capacity_bytes < MIN_ENTRY_SIZE as u64 {
            return Err(OnyxError::Config(
                "buffer device too small for any entries".into(),
            ));
        }

        let mut sb_buf = [0u8; 4096];
        device.read_at(&mut sb_buf, 0)?;

        let (sb, max_seq) = match BufferSuperblock::from_bytes(&sb_buf) {
            Some(sb) if sb.version >= 2 => {
                tracing::info!(
                    head = sb.head_offset,
                    tail = sb.tail_offset,
                    capacity = sb.capacity_bytes,
                    "loaded v2 buffer superblock"
                );
                // Scan to find max seq for next_seq counter
                let max_seq = Self::scan_max_seq(&device, &sb)?;
                (sb, max_seq)
            }
            _ => {
                tracing::info!(
                    capacity = capacity_bytes,
                    "initializing new v2 buffer superblock"
                );
                let sb = BufferSuperblock::new(capacity_bytes);
                device.write_at(&sb.to_bytes(), 0)?;
                device.sync()?;
                (sb, 0)
            }
        };

        // Rebuild in-memory indices
        let mut lba_index: HashMap<LbaKey, Arc<PendingEntry>> = HashMap::new();
        let mut seq_index: HashMap<u64, (u64, u32)> = HashMap::new();
        Self::rebuild_indices(&device, &sb, &mut lba_index, &mut seq_index)?;

        tracing::info!(
            pending_lbas = lba_index.len(),
            "buffer pool indices rebuilt"
        );

        Ok(Self {
            device,
            superblock: Mutex::new(sb),
            next_seq: AtomicU64::new(max_seq + 1),
            lba_index: Mutex::new(lba_index),
            seq_index: Mutex::new(seq_index),
            flush_progress: Mutex::new(HashMap::new()),
            metrics: OnceLock::new(),
        })
    }

    pub fn attach_metrics(&self, metrics: Arc<EngineMetrics>) {
        let _ = self.metrics.set(metrics);
    }

    fn scan_max_seq(device: &RawDevice, sb: &BufferSuperblock) -> OnyxResult<u64> {
        let mut max_seq = 0u64;
        let mut offset = sb.tail_offset;
        while offset != sb.head_offset {
            if offset + 4 > sb.data_end() {
                offset = BUFFER_SUPERBLOCK_SIZE;
                continue;
            }
            let mut len_buf = [0u8; 4];
            device.read_at(&mut len_buf, offset)?;
            let total_len = u32::from_le_bytes(len_buf) as u64;
            if total_len < MIN_ENTRY_SIZE as u64 || total_len > MAX_ENTRY_SIZE as u64 {
                break;
            }
            // Read just the seq field (offset 8..16 within entry)
            let mut seq_buf = [0u8; 8];
            device.read_at(&mut seq_buf, offset + 8)?;
            let seq = u64::from_le_bytes(seq_buf);
            max_seq = max_seq.max(seq);

            offset += total_len;
            if offset >= sb.data_end() {
                offset = BUFFER_SUPERBLOCK_SIZE;
            }
        }
        Ok(max_seq)
    }

    fn rebuild_indices(
        device: &RawDevice,
        sb: &BufferSuperblock,
        lba_index: &mut HashMap<LbaKey, Arc<PendingEntry>>,
        seq_index: &mut HashMap<u64, (u64, u32)>,
    ) -> OnyxResult<()> {
        let mut offset = sb.tail_offset;
        while offset != sb.head_offset {
            if offset + (MIN_ENTRY_SIZE as u64) > sb.data_end() {
                offset = BUFFER_SUPERBLOCK_SIZE;
                if offset == sb.head_offset {
                    break;
                }
                continue;
            }
            // Read total_len first
            let mut len_buf = [0u8; 4];
            device.read_at(&mut len_buf, offset)?;
            let total_len = u32::from_le_bytes(len_buf);
            if total_len < MIN_ENTRY_SIZE || total_len > MAX_ENTRY_SIZE {
                break; // corrupt
            }

            let mut entry_buf = vec![0u8; total_len as usize];
            device.read_at(&mut entry_buf, offset)?;

            match BufferEntry::from_bytes(&entry_buf) {
                Some(entry) if !entry.flushed => {
                    let pending = Arc::new(PendingEntry {
                        seq: entry.seq,
                        vol_id: entry.vol_id.clone(),
                        start_lba: entry.start_lba,
                        lba_count: entry.lba_count,
                        payload: entry.payload,
                        disk_offset: offset,
                        disk_len: total_len,
                    });
                    seq_index.insert(entry.seq, (offset, total_len));
                    for i in 0..entry.lba_count {
                        let key = LbaKey {
                            vol_id: entry.vol_id.clone(),
                            lba: Lba(entry.start_lba.0 + i as u64),
                        };
                        lba_index.insert(key, pending.clone());
                    }
                }
                Some(_) => {}  // flushed, skip
                None => break, // corrupt, stop
            }

            offset += total_len as u64;
            if offset >= sb.data_end() {
                offset = BUFFER_SUPERBLOCK_SIZE;
            }
        }
        Ok(())
    }

    /// Append raw data covering `lba_count` contiguous LBAs starting at `start_lba`.
    /// `payload` must be exactly `lba_count * BLOCK_SIZE` bytes.
    /// Returns the sequence number.
    pub fn append(
        &self,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        payload: &[u8],
        vol_created_at: u64,
    ) -> OnyxResult<u64> {
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

        // Check entry won't exceed MAX_ENTRY_SIZE (otherwise it's writable but unrecoverable)
        let estimated_size = round_up(
            48 + vol_id.len() + payload.len(), // FIXED_HEADER(v3) + vol_id + payload
            BLOCK_SIZE as usize,
        );
        if estimated_size > MAX_ENTRY_SIZE as usize {
            return Err(OnyxError::Config(format!(
                "entry too large: {} bytes (max {}). Reduce lba_count.",
                estimated_size, MAX_ENTRY_SIZE
            )));
        }

        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        let payload_crc = crc32fast::hash(payload);

        let entry = BufferEntry {
            seq,
            vol_id: vol_id.to_string(),
            start_lba,
            lba_count,
            payload_crc32: payload_crc,
            flushed: false,
            vol_created_at,
            payload: payload.to_vec(),
        };

        let entry_bytes = entry.to_bytes()?;
        let entry_len = entry_bytes.len() as u64;

        let mut sb = self.superblock.lock().unwrap();

        if !sb.has_room(entry_len) {
            return Err(OnyxError::BufferPoolFull(sb.used_bytes as usize));
        }

        // Handle wrap: if entry doesn't fit at end, skip to beginning
        let mut write_offset = sb.head_offset;
        let mut wasted = 0u64;
        if write_offset + entry_len > sb.data_end() {
            wasted = sb.data_end() - write_offset;
            write_offset = BUFFER_SUPERBLOCK_SIZE;
            if !sb.has_room(entry_len + wasted) {
                return Err(OnyxError::BufferPoolFull(sb.used_bytes as usize));
            }
        }

        // Write entry
        self.device.write_at(&entry_bytes, write_offset)?;

        // Update superblock
        sb.head_offset = write_offset + entry_len;
        if sb.head_offset >= sb.data_end() {
            sb.head_offset = BUFFER_SUPERBLOCK_SIZE;
        }
        sb.used_bytes += entry_len + wasted;
        sb.update_crc();
        self.device.write_at(&sb.to_bytes(), 0)?;

        // Sync to disk before ack
        self.device.sync()?;

        // Update in-memory indices
        let pending = Arc::new(PendingEntry {
            seq,
            vol_id: vol_id.to_string(),
            start_lba,
            lba_count,
            payload: payload.to_vec(),
            disk_offset: write_offset,
            disk_len: entry_len as u32,
        });

        {
            let mut lba_idx = self.lba_index.lock().unwrap();
            for i in 0..lba_count {
                let key = LbaKey {
                    vol_id: vol_id.to_string(),
                    lba: Lba(start_lba.0 + i as u64),
                };
                lba_idx.insert(key, pending.clone());
            }
        }
        self.seq_index
            .lock()
            .unwrap()
            .insert(seq, (write_offset, entry_len as u32));

        if let Some(metrics) = self.metrics.get() {
            metrics.buffer_appends.fetch_add(1, Ordering::Relaxed);
            metrics
                .buffer_append_bytes
                .fetch_add(payload.len() as u64, Ordering::Relaxed);
        }

        Ok(seq)
    }

    /// Look up the 4KB data for a single LBA. Returns the payload slice if found.
    /// O(1) HashMap lookup, zero disk IO.
    pub fn lookup(&self, vol_id: &str, lba: Lba) -> OnyxResult<Option<PendingEntry>> {
        let key = LbaKey {
            vol_id: vol_id.to_string(),
            lba,
        };
        let idx = self.lba_index.lock().unwrap();
        let result = idx.get(&key).map(|arc| (**arc).clone());
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

    /// Mark specific LBA offsets within entry `seq` as flushed.
    ///
    /// `flushed_lba_start` is the first LBA that was flushed (absolute),
    /// `flushed_lba_count` is how many contiguous LBAs were flushed.
    /// These are converted to offsets within the entry: `lba - entry.start_lba`.
    ///
    /// Uses a `HashSet<u16>` of offsets so retrying the same LBAs is idempotent —
    /// a failed-then-retried flush of the same half won't double-count.
    /// Only when ALL offsets 0..lba_count are present does the entry get marked flushed.
    pub fn mark_flushed(
        &self,
        seq: u64,
        flushed_lba_start: Lba,
        flushed_lba_count: u32,
    ) -> OnyxResult<()> {
        let loc = {
            let seq_idx = self.seq_index.lock().unwrap();
            match seq_idx.get(&seq) {
                Some(&loc) => loc,
                None => return Ok(()),
            }
        };
        let (disk_offset, disk_len) = loc;

        let mut buf = vec![0u8; disk_len as usize];
        self.device.read_at(&mut buf, disk_offset)?;

        let entry = match BufferEntry::from_bytes(&buf) {
            Some(e) if e.seq == seq => e,
            _ => return Ok(()),
        };

        // Compute offsets within the entry
        let entry_start = entry.start_lba.0;
        let all_done = {
            let mut progress = self.flush_progress.lock().unwrap();
            let flushed_offsets = progress.entry(seq).or_insert_with(HashSet::new);
            for i in 0..flushed_lba_count {
                let abs_lba = flushed_lba_start.0 + i as u64;
                if abs_lba >= entry_start {
                    flushed_offsets.insert((abs_lba - entry_start) as u16);
                }
            }
            flushed_offsets.len() >= entry.lba_count as usize
        };

        if !all_done {
            return Ok(());
        }

        // All LBAs flushed — clean up
        self.flush_progress.lock().unwrap().remove(&seq);

        // Remove from LBA index (only if still the latest for each LBA)
        {
            let mut lba_idx = self.lba_index.lock().unwrap();
            for i in 0..entry.lba_count {
                let key = LbaKey {
                    vol_id: entry.vol_id.clone(),
                    lba: Lba(entry.start_lba.0 + i as u64),
                };
                if let Some(existing) = lba_idx.get(&key) {
                    if existing.seq == seq {
                        lba_idx.remove(&key);
                    }
                }
            }
        }
        self.seq_index.lock().unwrap().remove(&seq);

        // Write flushed flag to disk
        let mut entry = entry;
        entry.flushed = true;
        let updated = entry.to_bytes()?;
        self.device.write_at(&updated, disk_offset)?;

        Ok(())
    }

    /// Advance tail past all contiguous flushed entries.
    pub fn advance_tail(&self) -> OnyxResult<u64> {
        let mut sb = self.superblock.lock().unwrap();
        let mut advanced = 0u64;

        let mut offset = sb.tail_offset;
        while offset != sb.head_offset {
            if offset + (MIN_ENTRY_SIZE as u64) > sb.data_end() {
                // Tail is at the end gap — reclaim it and wrap
                let gap = sb.data_end() - offset;
                sb.used_bytes = sb.used_bytes.saturating_sub(gap);
                offset = BUFFER_SUPERBLOCK_SIZE;
                if offset == sb.head_offset {
                    break;
                }
                continue;
            }

            // Read total_len
            let mut len_buf = [0u8; 4];
            self.device.read_at(&mut len_buf, offset)?;
            let total_len = u32::from_le_bytes(len_buf);
            if total_len < MIN_ENTRY_SIZE || total_len > MAX_ENTRY_SIZE {
                break;
            }

            // Read flushed flag (byte 30 within the entry)
            let mut flag_buf = [0u8; 1];
            self.device.read_at(&mut flag_buf, offset + 30)?;
            if flag_buf[0] == 0 {
                break; // not flushed, stop
            }

            // This entry is flushed — advance tail past it
            let bytes_freed = total_len as u64;
            sb.used_bytes = sb.used_bytes.saturating_sub(bytes_freed);
            offset += bytes_freed;
            advanced += 1;
        }

        if advanced > 0 {
            sb.tail_offset = offset;
            sb.update_crc();
            self.device.write_at(&sb.to_bytes(), 0)?;
        }

        Ok(advanced)
    }

    /// Recover all unflushed entries from the on-disk log.
    pub fn recover(&self) -> OnyxResult<Vec<BufferEntry>> {
        let sb = self.superblock.lock().unwrap();
        let mut unflushed = Vec::new();

        let mut offset = sb.tail_offset;
        while offset != sb.head_offset {
            if offset + (MIN_ENTRY_SIZE as u64) > sb.data_end() {
                offset = BUFFER_SUPERBLOCK_SIZE;
                if offset == sb.head_offset {
                    break;
                }
                continue;
            }

            let mut len_buf = [0u8; 4];
            self.device.read_at(&mut len_buf, offset)?;
            let total_len = u32::from_le_bytes(len_buf);
            if total_len < MIN_ENTRY_SIZE || total_len > MAX_ENTRY_SIZE {
                tracing::warn!(offset, total_len, "corrupt entry during recovery");
                break;
            }

            let mut entry_buf = vec![0u8; total_len as usize];
            self.device.read_at(&mut entry_buf, offset)?;

            match BufferEntry::from_bytes(&entry_buf) {
                Some(entry) if !entry.flushed => {
                    unflushed.push(entry);
                }
                Some(_) => {} // flushed
                None => {
                    tracing::warn!(offset, "corrupt entry during recovery");
                    break;
                }
            }

            offset += total_len as u64;
            if offset >= sb.data_end() {
                offset = BUFFER_SUPERBLOCK_SIZE;
            }
        }

        Ok(unflushed)
    }

    /// Number of unflushed LBAs in the index.
    pub fn pending_count(&self) -> u64 {
        // Count unique seqs (not LBAs, since one entry can cover many LBAs)
        self.seq_index.lock().unwrap().len() as u64
    }

    pub fn capacity(&self) -> u64 {
        let sb = self.superblock.lock().unwrap();
        sb.capacity_bytes
    }

    /// Purge all pending (unflushed) buffer entries for a specific volume.
    ///
    /// Removes entries from the in-memory LBA and seq indices, and marks them
    /// as flushed on disk so the flusher won't pick them up. This prevents
    /// deleted-volume data from being flushed back into metadata after deletion.
    pub fn purge_volume(&self, vol_id: &str) -> OnyxResult<u64> {
        if test_purge_fail_volumes().lock().unwrap().contains(vol_id) {
            return Err(OnyxError::Io(std::io::Error::other(format!(
                "injected purge failure for volume {vol_id}"
            ))));
        }

        // Collect seqs belonging to this volume from the LBA index
        let seqs_to_purge: Vec<u64> = {
            let lba_idx = self.lba_index.lock().unwrap();
            let mut seqs = std::collections::HashSet::new();
            for (key, entry) in lba_idx.iter() {
                if key.vol_id == vol_id {
                    seqs.insert(entry.seq);
                }
            }
            seqs.into_iter().collect()
        };

        if seqs_to_purge.is_empty() {
            return Ok(0);
        }

        let count = seqs_to_purge.len() as u64;

        // Remove from LBA index
        {
            let mut lba_idx = self.lba_index.lock().unwrap();
            lba_idx.retain(|key, _| key.vol_id != vol_id);
        }

        // Mark as flushed on disk + remove from seq index
        for seq in &seqs_to_purge {
            let loc = {
                let seq_idx = self.seq_index.lock().unwrap();
                seq_idx.get(seq).copied()
            };
            if let Some((disk_offset, disk_len)) = loc {
                // Read entry, set flushed flag, write back
                let mut buf = vec![0u8; disk_len as usize];
                self.device.read_at(&mut buf, disk_offset)?;
                if let Some(mut entry) = crate::buffer::entry::BufferEntry::from_bytes(&buf) {
                    if entry.seq == *seq && !entry.flushed {
                        entry.flushed = true;
                        let updated = entry.to_bytes()?;
                        self.device.write_at(&updated, disk_offset)?;
                    }
                }
            }
            self.seq_index.lock().unwrap().remove(seq);
            self.flush_progress.lock().unwrap().remove(seq);
        }

        tracing::info!(
            vol_id,
            purged_entries = count,
            "buffer entries purged for volume"
        );
        Ok(count)
    }

    pub fn fill_percentage(&self) -> u8 {
        let sb = self.superblock.lock().unwrap();
        if sb.capacity_bytes == 0 {
            return 100;
        }
        ((sb.used_bytes * 100) / sb.capacity_bytes) as u8
    }
}
