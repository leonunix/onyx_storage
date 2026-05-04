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
    device: &'a RawDevice,
    buf: AlignedBuf,
    buf_disk_start: u64,
    buf_valid_bytes: usize,
    capacity_bytes: u64,
}

impl<'a> ChunkReader<'a> {
    fn new(device: &'a RawDevice, capacity_bytes: u64) -> OnyxResult<Self> {
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
    pub(super) fn elapsed_ns(start: Instant) -> u64 {
        start.elapsed().as_nanos().min(u64::MAX as u128) as u64
    }

    pub(super) fn record_metric(counter: &std::sync::atomic::AtomicU64, start: Instant) {
        counter.fetch_add(Self::elapsed_ns(start), Ordering::Relaxed);
    }

    pub(super) fn release_payload_bytes(counter: &AtomicU64, bytes: u64) {
        let mut current = counter.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_sub(bytes);
            match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => {
                    if current < bytes {
                        tracing::warn!(
                            current_payload_bytes = current,
                            release_bytes = bytes,
                            "prevented payload memory accounting underflow"
                        );
                    }
                    return;
                }
                Err(actual) => current = actual,
            }
        }
    }

    pub(super) fn cache_committed_payload(&self, pending: &Arc<PendingEntry>, payload: Arc<[u8]>) {
        let payload_len = payload.len() as u64;
        if self.max_payload_memory == 0 || payload_len > self.max_payload_memory {
            return;
        }

        let mut cached = pending.as_ref().clone();
        cached.payload = Some(payload);
        let cached = Arc::new(cached);
        if !self.replace_pending_entry_if_current(pending, cached.clone()) {
            return;
        }

        self.payload_bytes_in_memory
            .fetch_add(payload_len, Ordering::Relaxed);
        self.replace_lba_index_if_current(pending, &cached);
        self.cached_payload_order.lock().push_back(pending.seq);
        self.evict_payload_cache_to_budget();
        self.compact_payload_cache_order_if_needed();
    }

    fn evict_payload_cache_to_budget(&self) {
        let budget = self.max_payload_memory;
        if budget == 0 {
            return;
        }
        while self.payload_bytes_in_memory.load(Ordering::Relaxed) > budget {
            let Some(seq) = self.cached_payload_order.lock().pop_front() else {
                return;
            };
            self.evict_cached_payload(seq);
        }
    }

    fn evict_cached_payload(&self, seq: u64) -> bool {
        let Some(pending) = self
            .pending_entries
            .get(&seq)
            .map(|entry| entry.value().clone())
        else {
            return false;
        };
        let Some(ref payload) = pending.payload else {
            return false;
        };
        let payload_len = payload.len() as u64;
        let evicted = Self::evicted_pending_entry(pending.as_ref());
        if !self.replace_pending_entry_if_current(&pending, evicted.clone()) {
            return false;
        }
        Self::release_payload_bytes(self.payload_bytes_in_memory.as_ref(), payload_len);
        self.replace_lba_index_if_current(&pending, &evicted);
        true
    }

    fn compact_payload_cache_order_if_needed(&self) {
        let live_pending = self.pending_entries.len();
        let max_order_len = live_pending.saturating_mul(2).max(1024);
        let mut order = self.cached_payload_order.lock();
        if order.len() <= max_order_len {
            return;
        }
        order.retain(|seq| {
            self.pending_entries
                .get(seq)
                .is_some_and(|entry| entry.payload.is_some())
        });
    }

    pub(super) fn slot_size() -> u64 {
        BLOCK_SIZE as u64
    }

    pub(super) fn add_seq_lba_range(acc: &mut Vec<(u64, Lba, u32)>, seq: u64, lba: Lba) {
        if let Some((last_seq, last_start, last_count)) = acc.last_mut() {
            if *last_seq == seq && last_start.0 + *last_count as u64 == lba.0 {
                *last_count += 1;
                return;
            }
        }
        acc.push((seq, lba, 1));
    }

    fn bucket_key(vid: &Arc<str>, bucket: u64) -> PendingBucketKey {
        use std::hash::{Hash, Hasher};

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        vid.hash(&mut hasher);
        PendingBucketKey {
            vol_hash: hasher.finish(),
            bucket,
        }
    }

    fn bucket_range(start_lba: Lba, lba_count: u32) -> Option<std::ops::RangeInclusive<u64>> {
        if lba_count == 0 {
            return None;
        }
        let first = start_lba.0 / PENDING_LBA_BUCKET_BLOCKS;
        let last_lba = start_lba.0.saturating_add(lba_count as u64 - 1);
        let last = last_lba / PENDING_LBA_BUCKET_BLOCKS;
        Some(first..=last)
    }

    fn add_pending_buckets(
        buckets: &DashMap<PendingBucketKey, AtomicU32>,
        vid: &Arc<str>,
        start_lba: Lba,
        lba_count: u32,
    ) {
        let Some(range) = Self::bucket_range(start_lba, lba_count) else {
            return;
        };
        for bucket in range {
            buckets
                .entry(Self::bucket_key(vid, bucket))
                .and_modify(|count| {
                    count
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                            Some(current.saturating_add(1))
                        })
                        .ok();
                })
                .or_insert_with(|| AtomicU32::new(1));
        }
    }

    fn remove_pending_buckets(&self, vid: &Arc<str>, start_lba: Lba, lba_count: u32) {
        let Some(range) = Self::bucket_range(start_lba, lba_count) else {
            return;
        };
        for bucket in range {
            let key = Self::bucket_key(vid, bucket);
            if let Some(count) = self.pending_lba_buckets.get(&key) {
                count
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                        Some(current.saturating_sub(1))
                    })
                    .ok();
            }
        }
    }

    fn pending_range_maybe_contains_interned(
        &self,
        vid: &Arc<str>,
        start_lba: Lba,
        lba_count: u32,
    ) -> bool {
        let Some(range) = Self::bucket_range(start_lba, lba_count) else {
            return false;
        };
        range.into_iter().any(|bucket| {
            self.pending_lba_buckets
                .get(&Self::bucket_key(vid, bucket))
                .is_some_and(|count| count.load(Ordering::Relaxed) > 0)
        })
    }

    pub(super) fn total_slots(capacity_bytes: u64) -> u64 {
        capacity_bytes / Self::slot_size()
    }

    pub(super) fn slot_bytes(slot_count: u32) -> u64 {
        slot_count as u64 * Self::slot_size()
    }

    /// Intern vol_id → Arc<str>. Read-lock fast path (common), write-lock
    /// only on first encounter of a new volume. Typically ≤10 volumes.
    pub(super) fn intern_vol_id(&self, vol_id: &str) -> Arc<str> {
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

    pub(super) fn reserve_log_space(
        ring: &mut RingState,
        seq: u64,
        slot_count: u32,
    ) -> Option<u64> {
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

        // When wrapping to offset 0, the space from head to the end of the
        // ring becomes a dead gap.  Track it in `gap` so used_bytes stays
        // accurate (prevents head==tail with used<capacity from being
        // misinterpreted as "has space").
        let (offset, gap) = if ring.used_bytes == 0 {
            // Ring is empty — the entire capacity is available, but we must
            // still check whether the entry fits between head and the end of
            // the device.  If it doesn't, wrap to offset 0.
            if len_bytes <= capacity - head {
                (head, 0)
            } else {
                (0, capacity - head)
            }
        } else if head > tail {
            let bytes_to_end = capacity - head;
            if len_bytes <= bytes_to_end {
                (head, 0)
            } else if len_bytes <= tail {
                (0, bytes_to_end)
            } else {
                return None;
            }
        } else if head < tail {
            if len_bytes <= tail - head {
                (head, 0)
            } else {
                return None;
            }
        } else {
            // head == tail && used_bytes > 0 → ring is full (entries + gap
            // fill the entire capacity).
            return None;
        };

        let was_empty = ring.log_order.is_empty();
        ring.head_offset = (offset + len_bytes) % capacity;
        ring.used_bytes += len_bytes + gap;
        ring.log_order.push_back(LogRecord {
            seq,
            disk_offset: offset,
            slot_count,
        });
        if was_empty {
            ring.head_became_at = Some(Instant::now());
        }
        Some(offset)
    }

    /// Advance the ring tail past contiguously-flushed-and-durable entries
    /// at the front of `log_order`. An entry is reclaimable only when
    /// BOTH conditions hold:
    ///   1. its seq is in `ring.flushed_seqs` (flusher has finished with it)
    ///   2. its seq ≤ `durable_seq` (the DB commits it drove have been fsync'd)
    ///
    /// Condition 2 closes the race where a flushed but not-yet-durable entry
    /// would otherwise be physically overwritten by a newer append and then
    /// lost on crash before its DB writes reached disk. Pass
    /// `u64::MAX` from legacy tests / paths that already guarantee durability
    /// separately (e.g. `mark_entry_flushed` in the purge path).
    pub(super) fn reclaim_log_prefix(ring: &mut RingState, durable_seq: u64) {
        loop {
            let Some(front) = ring.log_order.front().copied() else {
                ring.tail_offset = ring.head_offset;
                ring.head_became_at = None;
                // All entries reclaimed — any orphaned wrap gap is also gone.
                ring.used_bytes = 0;
                break;
            };
            if !ring.flushed_seqs.contains(&front.seq) {
                ring.tail_offset = front.disk_offset;
                break;
            }
            if front.seq > durable_seq {
                // Flushed but not yet durable — leave at front so its ring
                // slot stays intact until the watermark thread catches up.
                ring.tail_offset = front.disk_offset;
                break;
            }

            ring.log_order.pop_front();
            ring.flushed_seqs.remove(&front.seq);

            let entry_bytes = Self::slot_bytes(front.slot_count);
            let entry_end = front.disk_offset + entry_bytes;

            // Detect wrap gap: if this entry ends before the ring boundary
            // but the next entry (or head) is at a lower offset, there is a
            // dead gap at [entry_end, capacity) that was added to used_bytes
            // when the ring wrapped.  Free it together with the entry.
            let next_offset = ring
                .log_order
                .front()
                .map(|next| next.disk_offset)
                .unwrap_or(ring.head_offset);
            let gap = if entry_end < ring.capacity_bytes && next_offset < entry_end {
                ring.capacity_bytes - entry_end
            } else {
                0
            };

            ring.used_bytes = ring.used_bytes.saturating_sub(entry_bytes + gap);
            ring.reclaim_ready += 1;
            ring.tail_offset = ring
                .log_order
                .front()
                .map(|next| next.disk_offset)
                .unwrap_or(ring.head_offset);
            ring.head_became_at = ring.log_order.front().map(|_| Instant::now());
        }
    }

    pub(super) fn mark_entry_flushed(device: &RawDevice, pending: &PendingEntry) -> OnyxResult<()> {
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

    pub(super) fn rebuild_indices(
        device: &RawDevice,
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
    pub(super) fn rebuild_indices_guided(
        device: &RawDevice,
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

    pub(super) fn open(
        device: RawDevice,
        backpressure_timeout: Duration,
        metrics: Arc<OnceLock<Arc<EngineMetrics>>>,
        checkpoint: Option<ShardCheckpoint>,
        checkpoint_device: Option<RawDevice>,
        payload_bytes_in_memory: Arc<AtomicU64>,
        max_payload_memory: u64,
        volatile_payload_budget: Arc<VolatilePayloadBudget>,
        runtime_limits: BufferRuntimeLimits,
        max_flushed_seq: Arc<AtomicU64>,
        durable_seq: Arc<AtomicU64>,
    ) -> OnyxResult<(Self, u64)> {
        let capacity_bytes = device.size();
        if capacity_bytes < Self::slot_size() {
            return Err(OnyxError::Config(
                "persistent slot shard too small for any entries".into(),
            ));
        }

        let lba_index = DashMap::with_shard_amount(DASHMAP_SHARDS);
        let latest_lba_seq = DashMap::with_shard_amount(DASHMAP_SHARDS);
        let pending_lba_buckets = DashMap::with_shard_amount(DASHMAP_SHARDS);
        let pending_entries = DashMap::with_shard_amount(DASHMAP_SHARDS);
        let pending_count = AtomicU64::new(0);
        let recover_start = Instant::now();
        let scan = if let Some(ref ckpt) = checkpoint {
            let r = Self::rebuild_indices_guided(
                &device,
                capacity_bytes,
                ckpt,
                &lba_index,
                &latest_lba_seq,
                &pending_lba_buckets,
                &pending_entries,
                &pending_count,
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
                &pending_lba_buckets,
                &pending_entries,
                &pending_count,
            )?;
            tracing::info!(
                elapsed_us = recover_start.elapsed().as_micros() as u64,
                pending = pending_entries.len(),
                capacity_bytes,
                "shard recovery (full scan)"
            );
            r
        };

        let (staging_tx, staging_rx) = bounded(runtime_limits.staging_channel_capacity.max(1));
        let had_head = !scan.log_order.is_empty();
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
                    head_became_at: had_head.then(Instant::now),
                }),
                ring_space_cv: parking_lot::Condvar::new(),
                backpressure_timeout,
                lba_index,
                latest_lba_seq,
                pending_lba_buckets,
                pending_entries,
                pending_count,
                flush_progress: DashMap::with_shard_amount(DASHMAP_SHARDS),
                staging_tx,
                staging_rx,
                sync_batch_max_entries: runtime_limits.sync_batch_max_entries.max(1),
                sync_batch_max_bytes: runtime_limits.sync_batch_max_bytes.max(BLOCK_SIZE as usize),
                cached_payload_order: parking_lot::Mutex::new(VecDeque::with_capacity(1024)),
                volatile_payloads: DashMap::with_shard_amount(DASHMAP_SHARDS),
                volatile_payload_budget,
                lifecycle: parking_lot::Mutex::new(LifecycleState {
                    inflight: HashSet::with_capacity(256),
                    cancelled: HashSet::with_capacity(64),
                }),
                io_lock: parking_lot::Mutex::new(()),
                vol_id_cache: RwLock::new(Vec::with_capacity(16)),
                metrics,
                checkpoint_device,
                payload_bytes_in_memory,
                max_payload_memory,
                max_flushed_seq,
                durable_seq,
            },
            scan.max_seq,
        ))
    }

    /// Snapshot current ring state into a checkpoint structure.
    pub(super) fn snapshot_checkpoint(&self) -> ShardCheckpoint {
        let ring = self.ring.lock();
        let tail_offset = if ring.used_bytes == 0 {
            ring.head_offset
        } else {
            ring.tail_offset
        };
        ShardCheckpoint {
            head_offset: ring.head_offset,
            tail_offset,
            max_seq: 0, // updated by caller with global max_seq
            used_bytes: ring.used_bytes,
        }
    }

    /// Write the current checkpoint to disk (no sync — this is a hint).
    pub(super) fn write_checkpoint(&self, max_seq: u64) {
        let Some(ref ckpt_dev) = self.checkpoint_device else {
            return;
        };
        let mut ckpt = self.snapshot_checkpoint();
        ckpt.max_seq = max_seq;
        if let Err(e) = ckpt_dev.write_at(&ckpt.encode(), 0) {
            tracing::debug!(error = %e, "failed to persist shard checkpoint (non-fatal)");
        }
    }

    /// Encode the current checkpoint with the supplied max_seq, ready to be
    /// passed as a write SQE inside the sync_loop's batched io_uring submission.
    pub(super) fn encode_checkpoint_for_uring(&self, max_seq: u64) -> Option<Vec<u8>> {
        let _ = self.checkpoint_device.as_ref()?;
        let mut ckpt = self.snapshot_checkpoint();
        ckpt.max_seq = max_seq;
        Some(ckpt.encode().to_vec())
    }

    /// Returns the checkpoint device's raw fd plus the absolute offset where
    /// the checkpoint block is written. Used by the io_uring sync_loop path to
    /// piggyback the checkpoint write onto the same submit_batch as the entry
    /// writes + fsync. Returns None when no checkpoint device is attached.
    pub(super) fn checkpoint_target(&self) -> Option<(std::os::fd::RawFd, u64)> {
        let dev = self.checkpoint_device.as_ref()?;
        Some((dev.as_raw_fd(), dev.base_offset()))
    }

    /// Evict in-memory payloads from committed entries. The normal sync path
    /// now keeps durable payloads resident while budget allows; this helper is
    /// retained for tests and emergency cache trimming.
    #[allow(dead_code)]
    pub(super) fn evict_committed_payloads(&self, committed: &[Arc<PendingEntry>]) {
        for pending in committed {
            if let Some(ref p) = pending.payload {
                let payload_len = p.len() as u64;
                let evicted = Self::evicted_pending_entry(pending.as_ref());
                if self.replace_pending_entry_if_current(pending, evicted.clone()) {
                    Self::release_payload_bytes(self.payload_bytes_in_memory.as_ref(), payload_len);
                    self.replace_lba_index_if_current(pending, &evicted);
                }
            }
        }
    }

    /// After the coalescer has copied payload data into CoalesceUnits, evict
    /// the hydrated payloads from pending_entries so the memory budget is freed
    /// immediately — without waiting for mark_flushed at the end of the pipeline.
    pub(super) fn evict_hydrated_payloads(&self, seqs: &[u64]) {
        for &seq in seqs {
            let pending = match self.pending_entries.get(&seq) {
                Some(entry_ref) => Arc::clone(entry_ref.value()),
                None => continue,
            };
            let Some(ref p) = pending.payload else {
                continue;
            };
            let payload_len = p.len() as u64;
            let evicted = Self::evicted_pending_entry(pending.as_ref());
            if self.replace_pending_entry_if_current(&pending, evicted.clone()) {
                Self::release_payload_bytes(self.payload_bytes_in_memory.as_ref(), payload_len);
                self.replace_lba_index_if_current(&pending, &evicted);
            }
        }
    }

    pub(super) fn backpressure_waits_forever(&self) -> bool {
        self.backpressure_timeout == Duration::MAX
    }

    pub(super) fn retire_superseded_by_durable_entries(&self, committed: &[Arc<PendingEntry>]) {
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

    pub(super) fn compact_recovered_stale_ranges(&self) {
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
    pub(super) fn append_with_seq(
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

        let payload_len = payload.len() as u64;
        self.volatile_payload_budget.reserve(payload_len);

        let payload = Arc::<[u8]>::from(payload);
        let payload_crc32 = crc32fast::hash(&payload);

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

        // Build the metadata-only PendingEntry stored in the buffer indices.
        let pending = Arc::new(PendingEntry {
            seq,
            vol_id: vol_id.to_string(),
            start_lba,
            lba_count,
            payload_crc32,
            vol_created_at,
            payload: None,
            disk_offset: write_offset,
            disk_len,
            enqueued_at: Instant::now(),
            superseded_ranges,
        });

        // ── Mark inflight BEFORE any index insert ──────────────────
        // The flusher's retry-snapshot scans pending_entries and treats
        // entries with !inflight as ready.  If pending_entries is visible
        // before inflight is set, the flusher can race in and hydrate an
        // entry that hasn't been written to disk yet.  Setting inflight
        // first closes this window: by the time pending_entries.insert
        // makes the entry visible, inflight already contains the seq.
        {
            let mut lc = self.lifecycle.lock();
            lc.inflight.insert(seq);
        }

        // Publish the volatile payload before the LBA indices. Once an entry
        // appears in lba_index, foreground reads may need this payload before
        // the sync thread has made the ring slot durable.
        self.volatile_payloads.insert(seq, payload.clone());

        // ── DashMap inserts (concurrent sharded locks) ──
        // Interned Arc<str>: read-lock fast path, no alloc after first encounter.
        Self::add_pending_buckets(&self.pending_lba_buckets, &vid, start_lba, lba_count);
        for key in keys {
            self.lba_index.insert(key.clone(), pending.clone());
            self.latest_lba_seq.insert(key, (seq, vol_created_at));
        }
        if self.pending_entries.insert(seq, pending.clone()).is_none() {
            self.pending_count.fetch_add(1, Ordering::Relaxed);
        }

        // ── Channel send (lock-free MPSC, ~30ns) ──
        if self
            .staging_tx
            .send(StagedEntry { pending, payload })
            .is_err()
        {
            self.remove_volatile_payload(seq);
            return Err(OnyxError::Io(std::io::Error::other(
                "buffer sync thread is not accepting staged entries",
            )));
        }

        if let Some(metrics) = self.metrics.get() {
            metrics.buffer_appends.fetch_add(1, Ordering::Relaxed);
            metrics
                .buffer_append_bytes
                .fetch_add(payload_len, Ordering::Relaxed);
            metrics.buffer_write_ops.fetch_add(1, Ordering::Relaxed);
            metrics
                .buffer_write_bytes
                .fetch_add(payload_len, Ordering::Relaxed);
        }
        Ok(())
    }

    pub(super) fn drain_staged_limited(&self) -> Vec<StagedEntry> {
        let mut batch = Vec::new();
        let mut batch_bytes = 0usize;
        while let Ok(entry) = self.staging_rx.try_recv() {
            batch_bytes = batch_bytes.saturating_add(entry.payload.len());
            batch.push(entry);
            if batch.len() >= self.sync_batch_max_entries
                || batch_bytes >= self.sync_batch_max_bytes
            {
                break;
            }
        }
        batch
    }

    pub(super) fn used_bytes(&self) -> u64 {
        self.ring.lock().used_bytes
    }

    pub(super) fn capacity(&self) -> u64 {
        self.ring.lock().capacity_bytes
    }

    /// Read payload from the buffer device for a recovered entry.
    pub(super) fn read_payload_from_disk(&self, pending: &PendingEntry) -> OnyxResult<Arc<[u8]>> {
        let slot_bytes = pending.disk_len as usize;
        let mut buf = vec![0u8; slot_bytes];
        self.device.read_at(&mut buf, pending.disk_offset)?;
        self.decode_hydrated_payload(pending, &buf)
    }

    fn decode_hydrated_payload(
        &self,
        pending: &PendingEntry,
        bytes: &[u8],
    ) -> OnyxResult<Arc<[u8]>> {
        let entry = BufferEntry::from_bytes(bytes).ok_or_else(|| {
            tracing::error!(
                disk_offset = pending.disk_offset,
                disk_len = pending.disk_len,
                expected_seq = pending.seq,
                expected_lba = pending.start_lba.0,
                "failed to parse buffer entry during payload hydration"
            );
            OnyxError::Io(std::io::Error::other(format!(
                "failed to parse entry at offset {} during payload hydration",
                pending.disk_offset,
            )))
        })?;
        if entry.seq != pending.seq
            || entry.vol_id.as_str() != pending.vol_id.as_str()
            || entry.start_lba != pending.start_lba
            || entry.lba_count != pending.lba_count
            || entry.payload_crc32 != pending.payload_crc32
            || entry.vol_created_at != pending.vol_created_at
        {
            return Err(OnyxError::Io(std::io::Error::other(format!(
                "buffer entry metadata mismatch during payload hydration: disk_offset={} expected seq={} vol={} lba={} count={} created_at={}",
                pending.disk_offset,
                pending.seq,
                pending.vol_id,
                pending.start_lba.0,
                pending.lba_count,
                pending.vol_created_at,
            ))));
        }
        Ok(entry.payload)
    }

    fn record_lookup_hydrate_metric(&self, start: Instant) {
        if let Some(metrics) = self.metrics.get() {
            metrics
                .buffer_lookup_hydrate_ns
                .fetch_add(Self::elapsed_ns(start), Ordering::Relaxed);
            metrics
                .buffer_lookup_hydrate_ops
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn hydrate_missing_payloads_batched(
        &self,
        entries: &[Arc<PendingEntry>],
        record_lookup_metrics: bool,
    ) -> HashMap<u64, Arc<[u8]>> {
        let mut hydrated = HashMap::with_capacity(entries.len());
        if entries.is_empty() {
            return hydrated;
        }

        let mut sorted: Vec<_> = entries.to_vec();
        sorted.sort_by_key(|entry| entry.disk_offset);

        let mut start_idx = 0usize;
        while start_idx < sorted.len() {
            let start_offset = sorted[start_idx].disk_offset;
            let mut span_len = sorted[start_idx].disk_len as u64;
            let mut end_idx = start_idx + 1;

            while end_idx < sorted.len() {
                let next = &sorted[end_idx];
                if next.disk_offset != start_offset.saturating_add(span_len) {
                    break;
                }
                let next_len = next.disk_len as u64;
                let Some(candidate_len) = span_len.checked_add(next_len) else {
                    break;
                };
                if candidate_len as usize > HYDRATE_BATCH_MAX_BYTES {
                    break;
                }
                span_len = candidate_len;
                end_idx += 1;
            }

            let group = &sorted[start_idx..end_idx];
            if group.len() == 1 || span_len as usize > HYDRATE_BATCH_MAX_BYTES {
                for entry in group {
                    let hydrate_started = Instant::now();
                    let result = self.read_payload_from_disk(entry.as_ref());
                    if record_lookup_metrics {
                        self.record_lookup_hydrate_metric(hydrate_started);
                    }
                    match result {
                        Ok(payload) => {
                            hydrated.insert(entry.seq, payload);
                        }
                        Err(e) => {
                            tracing::debug!(
                                seq = entry.seq,
                                error = %e,
                                "failed to hydrate pending entry payload, evicting corrupt entry"
                            );
                            self.evict_corrupt_entry(entry.seq);
                        }
                    }
                }
                start_idx = end_idx;
                continue;
            }

            let Ok(total_len) = usize::try_from(span_len) else {
                start_idx = end_idx;
                continue;
            };
            let hydrate_started = Instant::now();
            let mut buf = match AlignedBuf::new(total_len, false) {
                Ok(buf) => buf,
                Err(e) => {
                    tracing::warn!(
                        start_offset,
                        span_len,
                        error = %e,
                        "batched payload hydration allocation failed; retrying individually"
                    );
                    for entry in group {
                        let one_started = Instant::now();
                        let result = self.read_payload_from_disk(entry.as_ref());
                        if record_lookup_metrics {
                            self.record_lookup_hydrate_metric(one_started);
                        }
                        match result {
                            Ok(payload) => {
                                hydrated.insert(entry.seq, payload);
                            }
                            Err(e) => {
                                tracing::debug!(
                                    seq = entry.seq,
                                    error = %e,
                                    "failed to hydrate pending entry payload, evicting corrupt entry"
                                );
                                self.evict_corrupt_entry(entry.seq);
                            }
                        }
                    }
                    start_idx = end_idx;
                    continue;
                }
            };

            let read_result = self
                .device
                .read_at(&mut buf.as_mut_slice()[..total_len], start_offset);
            if read_result.is_ok() {
                for entry in group {
                    let local = (entry.disk_offset - start_offset) as usize;
                    let end = local + entry.disk_len as usize;
                    match self.decode_hydrated_payload(entry.as_ref(), &buf.as_slice()[local..end])
                    {
                        Ok(payload) => {
                            hydrated.insert(entry.seq, payload);
                        }
                        Err(e) => {
                            tracing::warn!(
                                seq = entry.seq,
                                error = %e,
                                "failed to parse batched hydrated payload, evicting corrupt entry"
                            );
                            self.evict_corrupt_entry(entry.seq);
                        }
                    }
                }
                if record_lookup_metrics {
                    self.record_lookup_hydrate_metric(hydrate_started);
                }
            } else {
                let err = read_result.err().unwrap();
                if record_lookup_metrics {
                    self.record_lookup_hydrate_metric(hydrate_started);
                }
                tracing::warn!(
                    start_offset,
                    span_len,
                    entries = group.len(),
                    error = %err,
                    "batched payload hydration read failed; retrying individually"
                );
                for entry in group {
                    let one_started = Instant::now();
                    let result = self.read_payload_from_disk(entry.as_ref());
                    if record_lookup_metrics {
                        self.record_lookup_hydrate_metric(one_started);
                    }
                    match result {
                        Ok(payload) => {
                            hydrated.insert(entry.seq, payload);
                        }
                        Err(e) => {
                            tracing::debug!(
                                seq = entry.seq,
                                error = %e,
                                "failed to hydrate pending entry payload, evicting corrupt entry"
                            );
                            self.evict_corrupt_entry(entry.seq);
                        }
                    }
                }
            }

            start_idx = end_idx;
        }

        hydrated
    }

    pub(super) fn volatile_payload(&self, seq: u64) -> Option<Arc<[u8]>> {
        self.volatile_payloads
            .get(&seq)
            .map(|payload| payload.value().clone())
    }

    pub(super) fn remove_volatile_payload(&self, seq: u64) -> Option<Arc<[u8]>> {
        self.volatile_payloads.remove(&seq).map(|(_, payload)| {
            self.volatile_payload_budget.release(payload.len() as u64);
            payload
        })
    }

    pub(super) fn is_seq_inflight(&self, seq: u64) -> bool {
        self.lifecycle.lock().inflight.contains(&seq)
    }

    /// Return a PendingEntry with payload guaranteed present. If the entry
    /// was recovered without payload (lazy), reads it from disk now.
    /// If hydration fails (corrupt disk region), the entry is evicted from
    /// all indices so subsequent reads fall through to the blockmap (LV3).
    pub(super) fn lookup_hydrated(
        &self,
        vol_id: &str,
        lba: Lba,
    ) -> OnyxResult<Option<PendingEntry>> {
        let vid = self.intern_vol_id(vol_id);
        self.lookup_hydrated_interned(&vid, lba)
    }

    pub(super) fn lookup_hydrated_interned(
        &self,
        vid: &Arc<str>,
        lba: Lba,
    ) -> OnyxResult<Option<PendingEntry>> {
        if !self.pending_range_maybe_contains_interned(vid, lba, 1) {
            return Ok(None);
        }

        let index_started = Instant::now();
        // Clone the Arc and release the DashMap guard before disk hydration.
        // Holding lba_index across pread() can stall flusher mark_flushed under
        // heavy buffered reads.
        let entry = self
            .lba_index
            .get(&LbaKey {
                vol_id: vid.clone(),
                lba,
            })
            .map(|entry_ref| entry_ref.value().clone());
        let index_elapsed = Self::elapsed_ns(index_started);
        if let Some(metrics) = self.metrics.get() {
            metrics
                .buffer_lookup_index_ns
                .fetch_add(index_elapsed, Ordering::Relaxed);
        }
        let Some(entry) = entry else {
            return Ok(None);
        };
        if entry.payload.is_some() {
            return Ok(Some((*entry).clone()));
        }
        if let Some(payload) = self.volatile_payload(entry.seq) {
            let mut hydrated = (*entry).clone();
            hydrated.payload = Some(payload);
            return Ok(Some(hydrated));
        }
        if self.is_seq_inflight(entry.seq) {
            return Err(OnyxError::Io(std::io::Error::other(format!(
                "buffer payload for inflight seq {} is not memory-resident yet",
                entry.seq
            ))));
        }
        // Lazy hydration: read payload from buffer device.
        let seq = entry.seq;
        let hydrate_started = Instant::now();
        let result = self.read_payload_from_disk(entry.as_ref());
        let hydrate_elapsed = Self::elapsed_ns(hydrate_started);
        if let Some(metrics) = self.metrics.get() {
            metrics
                .buffer_lookup_hydrate_ns
                .fetch_add(hydrate_elapsed, Ordering::Relaxed);
            metrics
                .buffer_lookup_hydrate_ops
                .fetch_add(1, Ordering::Relaxed);
        }
        match result {
            Ok(payload) => {
                let mut hydrated = (*entry).clone();
                hydrated.payload = Some(payload);
                Ok(Some(hydrated))
            }
            Err(e) => {
                tracing::warn!(seq, error = %e, "read-path hydration failed, evicting corrupt entry");
                self.evict_corrupt_entry(seq);
                // Return None — caller falls through to blockmap/LV3.
                Ok(None)
            }
        }
    }

    pub(super) fn lookup_hydrated_range_interned(
        &self,
        vid: &Arc<str>,
        start_lba: Lba,
        lba_count: u32,
    ) -> OnyxResult<Vec<Option<PendingEntry>>> {
        if !self.pending_range_maybe_contains_interned(vid, start_lba, lba_count) {
            if let Some(metrics) = self.metrics.get() {
                metrics
                    .buffer_lookup_index_ns
                    .fetch_add(Self::elapsed_ns(Instant::now()), Ordering::Relaxed);
            }
            return Ok(vec![None; lba_count as usize]);
        }

        let mut indexed = Vec::with_capacity(lba_count as usize);
        let index_started = Instant::now();
        for i in 0..lba_count {
            let lba = Lba(start_lba.0 + i as u64);
            let entry = self
                .lba_index
                .get(&LbaKey {
                    vol_id: vid.clone(),
                    lba,
                })
                .map(|entry_ref| entry_ref.value().clone());
            indexed.push(entry);
        }
        let index_elapsed = Self::elapsed_ns(index_started);
        if let Some(metrics) = self.metrics.get() {
            metrics
                .buffer_lookup_index_ns
                .fetch_add(index_elapsed, Ordering::Relaxed);
        }

        let mut payloads: HashMap<u64, Arc<[u8]>> = HashMap::new();
        let mut missing = Vec::new();
        let mut seen_missing = HashSet::new();
        let mut missing_inflight_payload = None;
        for entry in indexed.iter().flatten() {
            if let Some(payload) = entry.payload.clone() {
                payloads.entry(entry.seq).or_insert(payload);
            } else if let Some(payload) = self.volatile_payload(entry.seq) {
                payloads.entry(entry.seq).or_insert(payload);
            } else if self.is_seq_inflight(entry.seq) {
                missing_inflight_payload = Some(entry.seq);
                break;
            } else if seen_missing.insert(entry.seq) {
                missing.push(entry.clone());
            }
        }
        if let Some(seq) = missing_inflight_payload {
            return Err(OnyxError::Io(std::io::Error::other(format!(
                "buffer payload for inflight seq {seq} is not memory-resident yet"
            ))));
        }
        payloads.extend(self.hydrate_missing_payloads_batched(&missing, true));

        let mut out = Vec::with_capacity(indexed.len());
        for entry in indexed {
            let Some(entry) = entry else {
                out.push(None);
                continue;
            };
            if let Some(payload) = payloads.get(&entry.seq) {
                let mut hydrated = (*entry).clone();
                hydrated.payload = Some(payload.clone());
                out.push(Some(hydrated));
            } else {
                out.push(None);
            }
        }
        Ok(out)
    }

    /// Remove LBA index entries for a range so reads see unmapped immediately.
    /// Does not remove pending_entries (flusher handles stale entries gracefully).
    pub(super) fn invalidate_lba_range(&self, vol_id: &str, start_lba: Lba, lba_count: u32) {
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

    pub(super) fn is_latest_lba_seq(
        &self,
        vol_id: &str,
        lba: Lba,
        seq: u64,
        vol_created_at: u64,
    ) -> bool {
        let vid = self.intern_vol_id(vol_id);
        self.latest_lba_seq
            .get(&LbaKey { vol_id: vid, lba })
            .map(|entry| {
                let (latest_seq, latest_created_at) = *entry;
                latest_seq == seq && latest_created_at == vol_created_at
            })
            .unwrap_or(true)
    }

    /// Return `true` iff every LBA covered by this entry has a strictly later
    /// seq in `latest_lba_seq` (same vol generation). That means a newer
    /// pending write will supply each LBA's content to the flusher, so this
    /// older entry can be dropped without ever compressing, hashing, or
    /// writing it to LV3.
    ///
    /// The check is conservative — we only return true when *all* LBAs are
    /// covered; a partially-superseded entry still goes through the pipeline
    /// (the writer's per-LBA `is_latest_lba_seq` filter handles that case).
    pub(super) fn is_entry_fully_superseded(
        &self,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        seq: u64,
        vol_created_at: u64,
    ) -> bool {
        if lba_count == 0 {
            return false;
        }
        let vid = self.intern_vol_id(vol_id);
        for offset in 0..lba_count {
            let key = LbaKey {
                vol_id: vid.clone(),
                lba: Lba(start_lba.0 + offset as u64),
            };
            match self.latest_lba_seq.get(&key) {
                Some(entry) => {
                    let (latest_seq, latest_created_at) = *entry;
                    // Map says *this* entry is still the latest for this LBA
                    // (or the generation changed — leave it to writer).
                    if latest_seq == seq || latest_created_at != vol_created_at {
                        return false;
                    }
                    // latest_seq > seq here → this LBA is superseded. Continue.
                }
                // Absent → the coalescer/writer has already processed the
                // latest seq for this LBA and retired it. Treat as superseded;
                // there's nothing useful left to flush.
                None => continue,
            }
        }
        true
    }

    pub(super) fn pending_to_buffer_entry(pending: &PendingEntry) -> BufferEntry {
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

    pub(super) fn pending_with_payload_to_buffer_entry(
        pending: &PendingEntry,
        payload: Arc<[u8]>,
    ) -> BufferEntry {
        BufferEntry {
            seq: pending.seq,
            vol_id: pending.vol_id.clone(),
            start_lba: pending.start_lba,
            lba_count: pending.lba_count,
            payload_crc32: pending.payload_crc32,
            flushed: false,
            vol_created_at: pending.vol_created_at,
            payload,
        }
    }

    pub(super) fn pending_entry(&self, seq: u64) -> Option<BufferEntry> {
        let entry = self.pending_entries.get(&seq)?;
        if let Some(payload) = entry.payload.clone().or_else(|| self.volatile_payload(seq)) {
            return Some(Self::pending_with_payload_to_buffer_entry(&entry, payload));
        }
        Some(Self::pending_to_buffer_entry(&entry))
    }

    pub(super) fn pending_entry_arc(&self, seq: u64) -> Option<Arc<PendingEntry>> {
        self.pending_entries
            .get(&seq)
            .map(|entry| entry.value().clone())
    }

    pub(super) fn evicted_pending_entry(pending: &PendingEntry) -> Arc<PendingEntry> {
        Arc::new(PendingEntry {
            seq: pending.seq,
            vol_id: pending.vol_id.clone(),
            start_lba: pending.start_lba,
            lba_count: pending.lba_count,
            payload_crc32: pending.payload_crc32,
            vol_created_at: pending.vol_created_at,
            payload: None,
            disk_offset: pending.disk_offset,
            disk_len: pending.disk_len,
            enqueued_at: pending.enqueued_at,
            superseded_ranges: pending.superseded_ranges.clone(),
        })
    }

    pub(super) fn replace_pending_entry_if_current(
        &self,
        expected: &Arc<PendingEntry>,
        replacement: Arc<PendingEntry>,
    ) -> bool {
        let Some(mut current) = self.pending_entries.get_mut(&expected.seq) else {
            return false;
        };
        if !Arc::ptr_eq(&*current, expected) {
            return false;
        }
        *current = replacement;
        true
    }

    pub(super) fn replace_lba_index_if_current(
        &self,
        expected: &Arc<PendingEntry>,
        replacement: &Arc<PendingEntry>,
    ) {
        let vid = self.intern_vol_id(&expected.vol_id);
        for i in 0..expected.lba_count {
            let key = LbaKey {
                vol_id: vid.clone(),
                lba: Lba(expected.start_lba.0 + i as u64),
            };
            if let Some(mut current) = self.lba_index.get_mut(&key) {
                if Arc::ptr_eq(&*current, expected) {
                    *current = replacement.clone();
                }
            }
        }
    }

    /// Evict a corrupt/unreadable pending entry: remove from all indices
    /// and reclaim ring space. Called when hydration fails (e.g. the disk
    /// region was overwritten by ring wrap-around or a partial write on crash).
    pub(super) fn evict_corrupt_entry(&self, seq: u64) {
        let Some((_, pending)) = self.pending_entries.remove(&seq) else {
            return;
        };
        self.pending_count.fetch_sub(1, Ordering::Relaxed);
        self.remove_volatile_payload(seq);
        let vid = self.intern_vol_id(&pending.vol_id);
        for i in 0..pending.lba_count {
            let key = LbaKey {
                vol_id: vid.clone(),
                lba: Lba(pending.start_lba.0 + i as u64),
            };
            self.lba_index.remove_if(&key, |_, value| value.seq == seq);
            self.latest_lba_seq.remove_if(&key, |_, &(s, _)| s == seq);
        }
        self.remove_pending_buckets(&vid, pending.start_lba, pending.lba_count);
        if let Some(ref p) = pending.payload {
            Self::release_payload_bytes(self.payload_bytes_in_memory.as_ref(), p.len() as u64);
        }
        let (ring_head, ring_tail, ring_used, ring_cap, log_len) = {
            let ring = self.ring.lock();
            (
                ring.head_offset,
                ring.tail_offset,
                ring.used_bytes,
                ring.capacity_bytes,
                ring.log_order.len(),
            )
        };
        self.free_seq_allocation(seq, &pending);
        tracing::info!(
            seq,
            vol_id = %pending.vol_id,
            start_lba = pending.start_lba.0,
            lba_count = pending.lba_count,
            disk_offset = pending.disk_offset,
            disk_len = pending.disk_len,
            ring_head,
            ring_tail,
            ring_used,
            ring_cap,
            log_len,
            "evicted corrupt buffer entry (disk data unreadable)"
        );
    }

    /// Return a detached PendingEntry with payload hydrated from the buffer
    /// device. The flusher only needs a transient payload copy to build
    /// CoalesceUnits; installing that copy back into pending_entries/lba_index
    /// makes the coalescer contend with foreground read lookups and can stall
    /// the whole flush pipeline under buffered-read pressure.
    pub(super) fn pending_entry_arc_hydrated(&self, seq: u64) -> Option<Arc<PendingEntry>> {
        let entry_ref = self.pending_entries.get(&seq)?;
        let entry = entry_ref.value().clone();
        if entry.payload.is_some() {
            return Some(entry);
        }
        drop(entry_ref);
        if let Some(payload) = self.volatile_payload(seq) {
            let mut hydrated = (*entry).clone();
            hydrated.payload = Some(payload);
            return Some(Arc::new(hydrated));
        }
        if self.is_seq_inflight(seq) {
            return None;
        }
        // Lazy hydration: read payload from disk, but keep it detached from
        // the shared indices. Coalescer window/channel bounds cap this
        // transient memory; persistent payload residency remains zero.
        match self.read_payload_from_disk(entry.as_ref()) {
            Ok(payload) => {
                let mut hydrated = (*entry).clone();
                hydrated.payload = Some(payload);
                Some(Arc::new(hydrated))
            }
            Err(e) => {
                tracing::debug!(seq, error = %e, "failed to hydrate pending entry payload, evicting corrupt entry");
                self.evict_corrupt_entry(seq);
                None
            }
        }
    }

    pub(super) fn pending_entry_arcs_hydrated(
        &self,
        entries: Vec<Arc<PendingEntry>>,
    ) -> Vec<Arc<PendingEntry>> {
        if entries.is_empty() {
            return entries;
        }

        let mut payloads: HashMap<u64, Arc<[u8]>> = HashMap::with_capacity(entries.len());
        let mut missing = Vec::new();
        let mut seen_missing = HashSet::new();
        for entry in &entries {
            if let Some(payload) = entry.payload.clone() {
                payloads.entry(entry.seq).or_insert(payload);
            } else if let Some(payload) = self.volatile_payload(entry.seq) {
                payloads.entry(entry.seq).or_insert(payload);
            } else if self.is_seq_inflight(entry.seq) {
                continue;
            } else if seen_missing.insert(entry.seq) {
                missing.push(entry.clone());
            }
        }
        payloads.extend(self.hydrate_missing_payloads_batched(&missing, false));

        let mut hydrated = Vec::with_capacity(entries.len());
        for entry in entries {
            if entry.payload.is_some() {
                hydrated.push(entry);
            } else if let Some(payload) = payloads.get(&entry.seq) {
                let mut with_payload = entry.as_ref().clone();
                with_payload.payload = Some(payload.clone());
                hydrated.push(Arc::new(with_payload));
            }
        }
        hydrated
    }

    pub(super) fn pending_entries_snapshot(&self) -> Vec<BufferEntry> {
        self.pending_entries
            .iter()
            .map(|entry| Self::pending_to_buffer_entry(&entry))
            .collect()
    }

    pub(super) fn pending_entries_arc_snapshot(&self) -> Vec<Arc<PendingEntry>> {
        self.pending_entries
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub(super) fn is_seq_ready_for_flush(&self, seq: u64) -> bool {
        !self.lifecycle.lock().inflight.contains(&seq)
    }

    pub(super) fn head_pending_seq_if_stuck(&self, min_age: Duration) -> Option<u64> {
        let (head_seq, head_became_at) = {
            let ring = self.ring.lock();
            (
                ring.log_order.front().map(|record| record.seq)?,
                ring.head_became_at,
            )
        };
        if !self.is_seq_ready_for_flush(head_seq) {
            return None;
        }
        let pending = self.pending_entries.get(&head_seq)?;
        let has_partial_progress = self.flush_progress.contains_key(&head_seq);
        drop(pending);
        let old_enough = head_became_at.is_some_and(|ts| ts.elapsed() >= min_age);
        if !has_partial_progress && !old_enough {
            return None;
        }
        Some(head_seq)
    }

    /// Cheap, non-hydrating diagnostic snapshot for a given seq. Returns
    /// (lba_count, flushed_count, age_ms, vol_id) so the flusher can correlate
    /// "head stuck" symptoms with in_flight refcount and per-LBA flush progress
    /// without paying the hydration cost of `pending_entry_arc_hydrated`.
    pub(super) fn pending_diag_snapshot(&self, seq: u64) -> Option<(u32, u32, u64, String)> {
        let entry = self.pending_entries.get(&seq)?;
        let lba_count = entry.lba_count;
        let age_ms = entry
            .enqueued_at
            .elapsed()
            .as_millis()
            .min(u64::MAX as u128) as u64;
        let vol_id = entry.vol_id.to_string();
        drop(entry);
        let flushed_count = self
            .flush_progress
            .get(&seq)
            .map(|set| set.len() as u32)
            .unwrap_or(0);
        Some((lba_count, flushed_count, age_ms, vol_id))
    }

    pub(super) fn head_seq_debug_state(
        &self,
        head_seq: Option<u64>,
        head_became_at: Option<Instant>,
    ) -> (Option<u32>, Option<u64>, Option<u64>) {
        let Some(seq) = head_seq else {
            return (None, None, None);
        };
        let Some(pending) = self.pending_entries.get(&seq) else {
            return (None, None, None);
        };
        let flushed = self
            .flush_progress
            .get(&seq)
            .map(|offsets| offsets.len() as u32)
            .unwrap_or(0);
        let remaining = pending
            .lba_count
            .saturating_sub(flushed.min(pending.lba_count));
        let age_ms = pending
            .enqueued_at
            .elapsed()
            .as_millis()
            .min(u64::MAX as u128) as u64;
        let residency_ms =
            head_became_at.map(|ts| ts.elapsed().as_millis().min(u64::MAX as u128) as u64);
        (Some(remaining), Some(age_ms), residency_ms)
    }

    pub(super) fn flushed_offsets_snapshot(&self, seq: u64) -> Option<HashSet<u16>> {
        self.flush_progress.get(&seq).map(|s| s.clone())
    }

    pub(super) fn has_seq(&self, seq: u64) -> bool {
        self.pending_entries.contains_key(&seq)
    }

    /// Memory-only: reclaim ring space, cancel write thread if needed.
    /// No disk write — metadata commit to metadb is the durable record.
    /// On crash recovery, stale "unflushed" entries are detected by
    /// cross-checking against the blockmap.
    ///
    /// The reclaim tail is gated on `durable_seq` so a flushed-but-not-yet-
    /// durable entry cannot be physically overwritten until the engine's
    /// watermark thread has fsync'd the DB commits that back it.
    pub(super) fn free_seq_allocation(&self, seq: u64, _pending: &PendingEntry) {
        {
            let mut lc = self.lifecycle.lock();
            // Only insert into cancelled if the sync thread still has this seq
            // in-flight (pending write + fsync). If the sync thread already
            // processed it, the insert would leak — nobody ever removes it.
            if lc.inflight.contains(&seq) {
                lc.cancelled.insert(seq);
            }
        }
        // Record that this seq has been mark_flushed'd so the durability
        // watermark thread can include it in its next sync cycle.
        self.max_flushed_seq.fetch_max(seq, Ordering::Relaxed);
        let durable_seq = self.durable_seq.load(Ordering::Acquire);
        {
            let mut ring = self.ring.lock();
            if !ring.flushed_seqs.insert(seq) {
                // Already freed by a concurrent purge_volume / free_seq_allocation_durable.
                // The ring space was already reclaimed — nothing left to do.
                return;
            }
            let before = ring.used_bytes;
            Self::reclaim_log_prefix(&mut ring, durable_seq);
            if ring.used_bytes < before {
                self.ring_space_cv.notify_all();
            }
        }
        self.flush_progress.remove(&seq);
    }

    /// Durable mark: writes flushed header to disk. Only used by purge_volume
    /// which needs disk-durable state before returning.
    ///
    /// This path writes the entry's flushed flag directly to disk via
    /// `mark_entry_flushed`, so the entry is durably "done" independent of
    /// the `durable_seq` watermark. Reclaim is therefore unconditional
    /// (`u64::MAX`).
    pub(super) fn free_seq_allocation_durable(
        &self,
        seq: u64,
        pending: &PendingEntry,
    ) -> OnyxResult<()> {
        {
            let mut lc = self.lifecycle.lock();
            if lc.inflight.contains(&seq) {
                lc.cancelled.insert(seq);
            }
        }
        {
            let _guard = self.io_lock.lock();
            Self::mark_entry_flushed(&self.device, pending)?;
        }
        {
            let mut ring = self.ring.lock();
            if !ring.flushed_seqs.insert(seq) {
                return Ok(());
            }
            let before = ring.used_bytes;
            Self::reclaim_log_prefix(&mut ring, u64::MAX);
            if ring.used_bytes < before {
                self.ring_space_cv.notify_all();
            }
        }
        self.flush_progress.remove(&seq);
        Ok(())
    }

    pub(super) fn mark_flushed(
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
                    vol_id: vid.clone(),
                    lba: pending.start_lba,
                },
                |_, value| value.seq == seq,
            );
            let Some((_, removed_pending)) = self.pending_entries.remove(&seq) else {
                return Ok(());
            };
            self.pending_count.fetch_sub(1, Ordering::Relaxed);
            self.remove_pending_buckets(&vid, pending.start_lba, pending.lba_count);
            self.remove_volatile_payload(seq);
            if let Some(ref p) = removed_pending.payload {
                Self::release_payload_bytes(self.payload_bytes_in_memory.as_ref(), p.len() as u64);
            }
            self.free_seq_allocation(seq, &removed_pending);
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
        let Some((_, removed_pending)) = self.pending_entries.remove(&seq) else {
            return Ok(());
        };
        self.pending_count.fetch_sub(1, Ordering::Relaxed);
        self.remove_pending_buckets(&vid, pending.start_lba, pending.lba_count);
        self.remove_volatile_payload(seq);
        if let Some(ref p) = removed_pending.payload {
            Self::release_payload_bytes(self.payload_bytes_in_memory.as_ref(), p.len() as u64);
        }
        self.free_seq_allocation(seq, &removed_pending);
        Ok(())
    }

    pub(super) fn advance_tail(&self) -> OnyxResult<u64> {
        // Re-run reclaim with the LATEST durable_seq. The original
        // `mark_flushed` → `free_seq_allocation` chain runs `reclaim_log_prefix`
        // inline, but only with whatever durable_seq was visible at that
        // instant. When the durability watermark thread advances durable_seq
        // after that point, no one re-visits the stuck prefix — so the tail
        // lags the actual durability envelope and a clean shutdown reopens
        // with phantom pending entries. Driving another reclaim pass here
        // closes that gap with no extra IO.
        let durable_seq = self.durable_seq.load(Ordering::Acquire);
        let mut ring = self.ring.lock();
        let before = ring.used_bytes;
        Self::reclaim_log_prefix(&mut ring, durable_seq);
        if ring.used_bytes < before {
            self.ring_space_cv.notify_all();
        }
        let advanced = ring.reclaim_ready;
        ring.reclaim_ready = 0;
        Ok(advanced)
    }

    pub(super) fn recover(&self) -> OnyxResult<Vec<BufferEntry>> {
        Ok(self.pending_entries_snapshot())
    }

    pub(super) fn recover_metadata(&self) -> Vec<RecoveredMeta> {
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

    pub(super) fn get_pending_arc(&self, seq: u64) -> Option<Arc<PendingEntry>> {
        self.pending_entry_arc(seq)
    }

    pub(super) fn pending_count(&self) -> u64 {
        self.pending_count.load(Ordering::Relaxed)
    }

    pub(super) fn purge_volume(&self, vol_id: &str) -> OnyxResult<Vec<u64>> {
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
        let mut removed_entries = Vec::with_capacity(to_purge.len());
        for (seq, _) in &to_purge {
            if let Some((_, pending)) = self.pending_entries.remove(seq) {
                self.pending_count.fetch_sub(1, Ordering::Relaxed);
                let vid = self.intern_vol_id(&pending.vol_id);
                self.remove_pending_buckets(&vid, pending.start_lba, pending.lba_count);
                self.remove_volatile_payload(*seq);
                if let Some(ref p) = pending.payload {
                    Self::release_payload_bytes(
                        self.payload_bytes_in_memory.as_ref(),
                        p.len() as u64,
                    );
                }
                removed_entries.push((*seq, pending));
            }
            self.flush_progress.remove(seq);
        }

        let seqs: Vec<u64> = removed_entries.iter().map(|(seq, _)| *seq).collect();
        for (seq, pending) in &removed_entries {
            self.free_seq_allocation_durable(*seq, pending)?;
        }

        Ok(seqs)
    }

    pub(super) fn discard_pending_seq_durable(&self, seq: u64) -> OnyxResult<bool> {
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
        let Some((_, removed_pending)) = self.pending_entries.remove(&seq) else {
            return Ok(false);
        };
        self.pending_count.fetch_sub(1, Ordering::Relaxed);
        self.remove_pending_buckets(&vid, pending.start_lba, pending.lba_count);
        self.remove_volatile_payload(seq);
        if let Some(ref p) = removed_pending.payload {
            Self::release_payload_bytes(self.payload_bytes_in_memory.as_ref(), p.len() as u64);
        }
        self.flush_progress.remove(&seq);
        self.free_seq_allocation_durable(seq, &removed_pending)?;
        Ok(true)
    }
}
