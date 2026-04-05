use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::buffer::entry::*;
use crate::error::{OnyxError, OnyxResult};
use crate::io::device::RawDevice;
use crate::meta::schema::MAX_VOLUME_ID_BYTES;
use crate::types::{Lba, BLOCK_SIZE};

/// Key for the in-memory pending index.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PendingKey {
    vol_id: String,
    lba: Lba,
}

/// Write buffer pool backed by a raw device (LV2).
///
/// Maintains an in-memory index of unflushed entries so that `lookup()` is O(1)
/// instead of scanning the entire on-disk log. The on-disk log remains the
/// crash-safe source of truth; the index is rebuilt from it on `open()`.
pub struct WriteBufferPool {
    device: RawDevice,
    superblock: Mutex<BufferSuperblock>,
    next_seq: AtomicU64,
    /// In-memory index: (vol_id, lba) → most recent unflushed entry.
    /// Protected by the same mutex as superblock to avoid split-brain between
    /// the index and the on-disk log.
    pending_index: Mutex<HashMap<PendingKey, BufferEntry>>,
}

impl WriteBufferPool {
    /// Open the buffer pool. Rebuilds the in-memory index from the on-disk log.
    pub fn open(device: RawDevice) -> OnyxResult<Self> {
        let device_size = device.size();
        let usable = device_size.saturating_sub(BUFFER_SUPERBLOCK_SIZE);
        let capacity_entries = usable / BUFFER_ENTRY_SIZE;

        if capacity_entries == 0 {
            return Err(OnyxError::Config(
                "buffer device too small for any entries".into(),
            ));
        }

        let mut sb_buf = [0u8; 4096];
        device.read_at(&mut sb_buf, 0)?;

        let (sb, next_seq) = match BufferSuperblock::from_bytes(&sb_buf) {
            Some(sb) => {
                tracing::info!(
                    head = sb.head_seq,
                    tail = sb.tail_seq,
                    capacity = sb.capacity_entries,
                    "loaded existing buffer superblock"
                );
                let next = sb.head_seq;
                (sb, next)
            }
            None => {
                tracing::info!(
                    capacity = capacity_entries,
                    "initializing new buffer superblock"
                );
                let sb = BufferSuperblock::new(capacity_entries);
                device.write_at(&sb.to_bytes(), 0)?;
                device.sync()?;
                (sb, 0)
            }
        };

        // Rebuild in-memory index from on-disk entries
        let mut index: HashMap<PendingKey, BufferEntry> = HashMap::new();
        for seq in sb.tail_seq..sb.head_seq {
            let offset = sb.offset_for_seq(seq);
            let mut buf = [0u8; BUFFER_ENTRY_SIZE as usize];
            device.read_at(&mut buf, offset)?;

            match BufferEntry::from_bytes(&buf) {
                Some(entry) if !entry.flushed => {
                    let key = PendingKey {
                        vol_id: entry.vol_id.clone(),
                        lba: entry.lba,
                    };
                    // Later entries overwrite earlier ones for the same key
                    index.insert(key, entry);
                }
                Some(_) => {}  // flushed, skip
                None => break, // corrupt, stop
            }
        }

        tracing::info!(pending_entries = index.len(), "buffer pool index rebuilt");

        Ok(Self {
            device,
            superblock: Mutex::new(sb),
            next_seq: AtomicU64::new(next_seq),
            pending_index: Mutex::new(index),
        })
    }

    /// Append raw (uncompressed) data to the buffer. Returns the sequence number.
    /// The entry is written to disk and synced before this returns (crash-safe).
    /// Compression is handled by the flusher pipeline, not here.
    pub fn append(&self, vol_id: &str, lba: Lba, payload: &[u8]) -> OnyxResult<u64> {
        if vol_id.is_empty() || vol_id.len() > MAX_VOLUME_ID_BYTES {
            return Err(OnyxError::Config(format!(
                "vol_id must be 1..{} bytes, got {}",
                MAX_VOLUME_ID_BYTES,
                vol_id.len()
            )));
        }
        if payload.len() > BLOCK_SIZE as usize {
            return Err(OnyxError::Config(format!(
                "buffer append payload too large: {} > {} bytes",
                payload.len(),
                BLOCK_SIZE
            )));
        }

        let mut sb = self.superblock.lock().unwrap();

        if sb.is_full() {
            return Err(OnyxError::BufferPoolFull(sb.pending_count() as usize));
        }

        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        let payload_crc = crc32fast::hash(payload);

        let entry = BufferEntry {
            seq,
            vol_id: vol_id.to_string(),
            lba,
            compression: 0, // always raw in buffer
            original_size: payload.len() as u32,
            compressed_size: payload.len() as u32,
            payload_crc32: payload_crc,
            flushed: false,
            payload: payload.to_vec(),
        };

        // Write to disk first (crash-safe)
        let offset = sb.offset_for_seq(seq);
        self.device.write_at(&entry.to_bytes()?, offset)?;

        sb.head_seq = seq + 1;
        sb.update_crc();
        self.device.write_at(&sb.to_bytes(), 0)?;

        self.device.sync()?;

        // Update in-memory index (after disk is durable)
        let key = PendingKey {
            vol_id: vol_id.to_string(),
            lba,
        };
        self.pending_index.lock().unwrap().insert(key, entry);

        Ok(seq)
    }

    /// Mark an entry as flushed (already written to LV3).
    /// Removes the entry from the in-memory index.
    pub fn mark_flushed(&self, seq: u64) -> OnyxResult<()> {
        let sb = self.superblock.lock().unwrap();
        let offset = sb.offset_for_seq(seq);

        let mut buf = [0u8; BUFFER_ENTRY_SIZE as usize];
        self.device.read_at(&mut buf, offset)?;

        if let Some(mut entry) = BufferEntry::from_bytes(&buf) {
            // Remove from index. Only remove if this seq is still the latest for this key —
            // a newer write to the same (vol_id, lba) may have already replaced it.
            let key = PendingKey {
                vol_id: entry.vol_id.clone(),
                lba: entry.lba,
            };
            let mut index = self.pending_index.lock().unwrap();
            if let Some(indexed) = index.get(&key) {
                if indexed.seq == seq {
                    index.remove(&key);
                }
            }

            entry.flushed = true;
            self.device.write_at(&entry.to_bytes()?, offset)?;
        }
        Ok(())
    }

    /// Advance the tail past all contiguous flushed entries.
    pub fn advance_tail(&self) -> OnyxResult<u64> {
        let mut sb = self.superblock.lock().unwrap();
        let mut advanced = 0u64;

        while sb.tail_seq < sb.head_seq {
            let offset = sb.offset_for_seq(sb.tail_seq);
            let mut buf = [0u8; BUFFER_ENTRY_SIZE as usize];
            self.device.read_at(&mut buf, offset)?;

            match BufferEntry::from_bytes(&buf) {
                Some(entry) if entry.flushed => {
                    sb.tail_seq += 1;
                    advanced += 1;
                }
                _ => break,
            }
        }

        if advanced > 0 {
            sb.update_crc();
            self.device.write_at(&sb.to_bytes(), 0)?;
        }

        Ok(advanced)
    }

    /// Recover unflushed entries after a crash.
    /// Returns entries from tail to head that are valid but not flushed.
    /// Note: the in-memory index is already rebuilt in `open()`, so this is
    /// primarily for the flusher to know what needs to be written to LV3.
    pub fn recover(&self) -> OnyxResult<Vec<BufferEntry>> {
        let sb = self.superblock.lock().unwrap();
        let mut unflushed = Vec::new();

        for seq in sb.tail_seq..sb.head_seq {
            let offset = sb.offset_for_seq(seq);
            let mut buf = [0u8; BUFFER_ENTRY_SIZE as usize];
            self.device.read_at(&mut buf, offset)?;

            match BufferEntry::from_bytes(&buf) {
                Some(entry) if !entry.flushed => {
                    unflushed.push(entry);
                }
                Some(_) => {}
                None => {
                    tracing::warn!(seq, "corrupt entry during recovery, stopping scan");
                    break;
                }
            }
        }

        tracing::info!(
            unflushed = unflushed.len(),
            tail = sb.tail_seq,
            head = sb.head_seq,
            "buffer recovery complete"
        );

        Ok(unflushed)
    }

    /// Look up the most recent unflushed entry for a given (vol_id, lba).
    /// O(1) in-memory HashMap lookup — no disk IO.
    pub fn lookup(&self, vol_id: &str, lba: Lba) -> OnyxResult<Option<BufferEntry>> {
        let key = PendingKey {
            vol_id: vol_id.to_string(),
            lba,
        };
        let index = self.pending_index.lock().unwrap();
        Ok(index.get(&key).cloned())
    }

    pub fn pending_count(&self) -> u64 {
        let sb = self.superblock.lock().unwrap();
        sb.pending_count()
    }

    pub fn capacity(&self) -> u64 {
        let sb = self.superblock.lock().unwrap();
        sb.capacity_entries
    }

    pub fn fill_percentage(&self) -> u8 {
        let sb = self.superblock.lock().unwrap();
        if sb.capacity_entries == 0 {
            return 100;
        }
        ((sb.pending_count() * 100) / sb.capacity_entries) as u8
    }
}
