use super::*;

/// Startup-scan chunk size. Recovery reads the commit-log ring in 16 MiB
/// windows and parses entries from memory. Chosen so that typical shards
/// finish in a handful of sequential pread() calls instead of one per 4 KiB
/// slot (MAX_ENTRY_SIZE is 2 MiB, leaving ≥ 14 MiB of new data per window).
const SCAN_CHUNK_BYTES: usize = 16 * 1024 * 1024;

/// Sliding read-ahead buffer for recovery scans. Holds one aligned window
/// from the commit-log device; refills when the caller's offset leaves the
/// window or when fewer than MAX_ENTRY_SIZE bytes remain ahead of the
/// cursor (so the next entry is always fully buffered unless we have truly
/// hit the end of the ring).
struct ChunkReader<'a> {
    device: &'a dyn BlockBackend,
    buf: AlignedBuf,
    buf_disk_start: u64,
    buf_valid_bytes: usize,
    capacity_bytes: u64,
}

impl<'a> ChunkReader<'a> {
    fn new(device: &'a dyn BlockBackend, capacity_bytes: u64) -> OnyxResult<Self> {
        let block = BLOCK_SIZE as usize;
        let cap_usize = usize::try_from(capacity_bytes).unwrap_or(usize::MAX);
        let mut chunk = SCAN_CHUNK_BYTES.min(cap_usize);
        chunk &= !(block - 1);
        if chunk < block {
            chunk = block;
        }
        let buf = AlignedBuf::new(chunk, false)?;
        Ok(Self {
            device,
            buf,
            buf_disk_start: 0,
            buf_valid_bytes: 0,
            capacity_bytes,
        })
    }

    /// Return a slice starting at `disk_offset` with as many valid bytes as
    /// are currently buffered (at least one full entry unless we have
    /// reached `capacity_bytes`).
    fn slice_at(&mut self, disk_offset: u64) -> OnyxResult<&[u8]> {
        debug_assert!(disk_offset.is_multiple_of(BLOCK_SIZE as u64));
        debug_assert!(disk_offset < self.capacity_bytes);

        let buf_end = self.buf_disk_start + self.buf_valid_bytes as u64;
        let in_window =
            self.buf_valid_bytes > 0 && disk_offset >= self.buf_disk_start && disk_offset < buf_end;
        let need_refill = if !in_window {
            true
        } else {
            let tail = buf_end - disk_offset;
            tail < MAX_ENTRY_SIZE as u64 && buf_end < self.capacity_bytes
        };

        if need_refill {
            self.load(disk_offset)?;
        }
        let local = (disk_offset - self.buf_disk_start) as usize;
        Ok(&self.buf.as_slice()[local..self.buf_valid_bytes])
    }

    fn load(&mut self, disk_offset: u64) -> OnyxResult<()> {
        debug_assert!(disk_offset.is_multiple_of(BLOCK_SIZE as u64));
        let remaining = self.capacity_bytes.saturating_sub(disk_offset);
        let cap = self.buf.len();
        let mut want = usize::try_from(remaining).unwrap_or(usize::MAX).min(cap);
        want &= !(BLOCK_SIZE as usize - 1);
        if want == 0 {
            self.buf_disk_start = disk_offset;
            self.buf_valid_bytes = 0;
            return Ok(());
        }
        self.device
            .read_at(&mut self.buf.as_mut_slice()[..want], disk_offset)?;
        self.buf_disk_start = disk_offset;
        self.buf_valid_bytes = want;
        Ok(())
    }
}

impl BufferShard {
    /// Parse a single entry from an already-buffered slice beginning at
    /// `disk_offset`. Returns `None` when there is no valid entry at the
    /// current offset (bad magic, bad length, or end-of-capacity).
    ///
    /// Callers must ensure `buf` contains at least `MAX_ENTRY_SIZE` bytes or
    /// all remaining bytes up to `capacity_bytes`. `ChunkReader::slice_at`
    /// guarantees this.
    pub(super) fn scan_entry_buf(
        buf: &[u8],
        disk_offset: u64,
        capacity_bytes: u64,
    ) -> Option<(BufferEntry, u32)> {
        if disk_offset + Self::slot_size() > capacity_bytes {
            return None;
        }
        if buf.len() < BLOCK_SIZE as usize {
            return None;
        }
        let total_len = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let magic = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if total_len < MIN_ENTRY_SIZE || total_len > MAX_ENTRY_SIZE || magic != BUFFER_ENTRY_MAGIC {
            return None;
        }
        let slot_count = round_up(total_len as usize, BLOCK_SIZE as usize) as u32 / BLOCK_SIZE;
        let slot_bytes = Self::slot_bytes(slot_count) as usize;
        if disk_offset + slot_bytes as u64 > capacity_bytes {
            return None;
        }
        if buf.len() < slot_bytes {
            return None;
        }
        BufferEntry::from_bytes(&buf[..slot_bytes]).map(|entry| (entry, slot_count))
    }

    pub(in crate::buffer::commit_log) fn rebuild_indices(
        device: &dyn BlockBackend,
        capacity_bytes: u64,
        lba_index: &DashMap<LbaKey, Arc<PendingEntry>>,
        latest_lba_seq: &DashMap<LbaKey, (u64, u64)>,
        pending_lba_buckets: &DashMap<PendingBucketKey, AtomicU32>,
        pending_entries: &DashMap<u64, Arc<PendingEntry>>,
        pending_count: &AtomicU64,
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
        let mut reader = ChunkReader::new(device, capacity_bytes)?;

        while slot < total_slots {
            let offset = slot * Self::slot_size();
            let window = reader.slice_at(offset)?;
            match Self::scan_entry_buf(window, offset, capacity_bytes) {
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
                            enqueued_at: Instant::now(),
                            durability_advanced_at_ns: AtomicU64::new(0),
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
            if pending_entries
                .insert(pending.seq, pending.clone())
                .is_none()
            {
                pending_count.fetch_add(1, Ordering::Relaxed);
            }
            Self::add_pending_buckets(
                pending_lba_buckets,
                &vid,
                pending.start_lba,
                pending.lba_count,
            );
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

    pub(in crate::buffer::commit_log) fn rebuild_indices_guided(
        device: &dyn BlockBackend,
        capacity_bytes: u64,
        checkpoint: &ShardCheckpoint,
        lba_index: &DashMap<LbaKey, Arc<PendingEntry>>,
        latest_lba_seq: &DashMap<LbaKey, (u64, u64)>,
        pending_lba_buckets: &DashMap<PendingBucketKey, AtomicU32>,
        pending_entries: &DashMap<u64, Arc<PendingEntry>>,
        pending_count: &AtomicU64,
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
                pending_lba_buckets,
                pending_entries,
                pending_count,
            );
        }

        if checkpoint.used_bytes == 0
            && checkpoint.max_seq == 0
            && checkpoint.head_offset == 0
            && checkpoint.tail_offset == 0
        {
            return Ok(ScanResult {
                max_seq: 0,
                used_bytes: 0,
                head_offset: 0,
                tail_offset: 0,
                log_order: VecDeque::new(),
                flushed_seqs: HashSet::new(),
            });
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
                        enqueued_at: Instant::now(),
                        durability_advanced_at_ns: AtomicU64::new(0),
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
        if checkpoint.used_bytes == 0 {
            // Checkpoint says empty. A head/tail mismatch here is stale pointer
            // drift from prior wrap/reclaim history, not a live occupied range.
            // Only do the contiguous forward scan below to catch post-checkpoint
            // appends.
            if checkpoint.head_offset != checkpoint.tail_offset {
                tracing::warn!(
                    head = checkpoint.head_offset,
                    tail = checkpoint.tail_offset,
                    "empty shard checkpoint has mismatched offsets; ignoring stale occupied range"
                );
            }
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
        let mut reader = ChunkReader::new(device, capacity_bytes)?;
        for (range_start, range_end) in &occupied_ranges {
            let mut offset = *range_start;
            while offset < *range_end {
                let window = reader.slice_at(offset)?;
                match Self::scan_entry_buf(window, offset, capacity_bytes) {
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
            let window = reader.slice_at(forward_offset)?;
            match Self::scan_entry_buf(window, forward_offset, capacity_bytes) {
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
            if pending_entries
                .insert(pending.seq, pending.clone())
                .is_none()
            {
                pending_count.fetch_add(1, Ordering::Relaxed);
            }
            Self::add_pending_buckets(
                pending_lba_buckets,
                &vid,
                pending.start_lba,
                pending.lba_count,
            );
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
}
