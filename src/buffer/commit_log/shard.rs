use super::*;

mod payload;
mod recovery;

impl BufferShard {
    pub(super) fn elapsed_ns(start: Instant) -> u64 {
        start.elapsed().as_nanos().min(u64::MAX as u128) as u64
    }

    pub(super) fn record_metric(counter: &std::sync::atomic::AtomicU64, start: Instant) {
        counter.fetch_add(Self::elapsed_ns(start), Ordering::Relaxed);
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
        // Track in the pending-only index so coalesce lookups don't
        // have to scan through already-applied seqs in log_order.
        // Paired with `note_applied`'s removal; release_below doesn't
        // need to touch this set (the seq is already absent by then).
        ring.pending_seqs.insert(seq);
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

    pub(super) fn mark_entry_flushed(
        device: &dyn BlockBackend,
        pending: &PendingEntry,
    ) -> OnyxResult<()> {
        let payload_len = pending.lba_count as usize * BLOCK_SIZE as usize;
        if device.direct_io() {
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

    pub(super) fn open(
        device: Arc<dyn BlockBackend>,
        backpressure_timeout: Duration,
        metrics: Arc<OnceLock<Arc<EngineMetrics>>>,
        checkpoint: Option<ShardCheckpoint>,
        checkpoint_device: Option<Arc<dyn BlockBackend>>,
        payload_bytes_in_memory: Arc<AtomicU64>,
        max_payload_memory: u64,
        runtime_limits: BufferRuntimeLimits,
        max_flushed_seq: Arc<AtomicU64>,
        durable_seq: Arc<AtomicU64>,
        lv2_durability: Arc<Lv2DurabilityWaiter>,
        ready_tx: Sender<u64>,
        shard_ready_tx: Sender<u64>,
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
        let mut scan = if let Some(ref ckpt) = checkpoint {
            let r = Self::rebuild_indices_guided(
                device.as_ref(),
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
                device.as_ref(),
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
        // The scan's max_seq only reflects entries still on disk; once the
        // tail advances past them, `scan.max_seq` drops back to 0. But
        // metadb's L2P still carries the higher seqs those entries
        // committed, and metadb's seq_guard rejects any subsequent
        // commit with new_seq < stored_seq. Carrying `checkpoint.max_seq`
        // forward keeps `next_seq` monotonically increasing across
        // restarts so new commits always have a strictly greater seq
        // than anything L2P has seen before.
        if let Some(ref ckpt) = checkpoint {
            scan.max_seq = scan.max_seq.max(ckpt.max_seq);
        }

        let (staging_tx, staging_rx) = bounded(runtime_limits.staging_channel_capacity.max(1));
        let had_head = !scan.log_order.is_empty();
        let mut log_order = VecDeque::with_capacity(scan.log_order.len());
        log_order.extend(scan.log_order);

        // Seed pending_bytes from recovered pending entries — recovery
        // populated `pending_entries` with `disk_len` matching the ring
        // log_order, so summing here mirrors what append() would have
        // accumulated.
        let pending_bytes_init: u64 = pending_entries
            .iter()
            .map(|e| e.value().disk_len as u64)
            .sum();

        // Build the pending_seqs index = seqs in log_order whose
        // pending_entries slot is still present (i.e. NOT in
        // flushed_seqs). Matches the open-time semantics: scan
        // hydrated `pending_entries` only for non-applied seqs;
        // pending_seqs mirrors that key set in sorted order so the
        // coalescer's "oldest pending" lookup is O(P) not O(L).
        let pending_seqs: BTreeSet<u64> = log_order
            .iter()
            .filter(|r| !scan.flushed_seqs.contains(&r.seq))
            .map(|r| r.seq)
            .collect();
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
                    pending_seqs,
                    head_became_at: had_head.then(Instant::now),
                }),
                ring_space_cv: parking_lot::Condvar::new(),
                backpressure_timeout,
                lba_index,
                latest_lba_seq,
                pending_lba_buckets,
                pending_entries,
                pending_count,
                pending_bytes: AtomicU64::new(pending_bytes_init),
                flush_progress: DashMap::with_shard_amount(DASHMAP_SHARDS),
                staging_tx,
                staging_rx,
                sync_batch_max_entries: runtime_limits.sync_batch_max_entries.max(1),
                sync_batch_max_bytes: runtime_limits.sync_batch_max_bytes.max(BLOCK_SIZE as usize),
                cached_payload_order: parking_lot::Mutex::new(VecDeque::with_capacity(1024)),
                lifecycle: parking_lot::Mutex::new(LifecycleState {
                    cancelled: HashSet::with_capacity(64),
                }),
                lv2_durability,
                ready_tx,
                shard_ready_tx,
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
        // Only the io_uring path consumes this (to piggyback the checkpoint
        // write onto the entry-writes+fsync submit). A chunklet-backed
        // checkpoint device has no fd → None, and the chunklet sync path
        // persists the checkpoint through `write_checkpoint` + `flush` instead.
        let dev = self.checkpoint_device.as_ref()?;
        dev.uring_target()
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
                    self.record_payload_cache_evict(payload_len);
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
                self.record_payload_cache_evict(payload_len);
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
                // Internal supersede path: the superseding seq is already
                // committed, so the old range's in-memory state is safe
                // to drop. Ring slot reclaim still waits for the next
                // checkpoint's release_below — the inline prefix attempt
                // inside mark_flushed is just an opportunistic no-op now.
                #[allow(deprecated)]
                let r = self.mark_flushed(*old_seq, *lba_start, *lba_count);
                if let Err(e) = r {
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
                // Recovery-time compaction: dropping ranges already
                // superseded on the next-seq side. Same rationale as
                // `retire_superseded_by_durable_entries` for using the
                // deprecated mark_flushed path.
                #[allow(deprecated)]
                let r = self.mark_flushed(seq, lba_start, lba_count);
                if let Err(e) = r {
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

        // Build PendingEntry with payload populated eagerly. This is the
        // post-volatile design: payload lives in the in-memory cache from
        // append time until the flusher retires it; reads consult
        // `entry.payload` (Some) or fall back to LV2 disk for crash-recovered
        // entries that were rebuilt with `payload: None`.
        let pending = Arc::new(PendingEntry {
            seq,
            vol_id: vol_id.to_string(),
            start_lba,
            lba_count,
            payload_crc32,
            vol_created_at,
            payload: Some(payload.clone()),
            disk_offset: write_offset,
            disk_len,
            enqueued_at: Instant::now(),
            superseded_ranges,
        });

        // ── DashMap inserts (concurrent sharded locks) ──
        // Entries are visible to readers immediately but their seq is
        // strictly greater than `lv2_durability.synced_seq` until the write
        // thread fdatasync's the batch — flusher gating is by watermark
        // comparison (see `is_seq_ready_for_flush`), not by membership set.
        Self::add_pending_buckets(&self.pending_lba_buckets, &vid, start_lba, lba_count);
        for key in keys {
            self.lba_index.insert(key.clone(), pending.clone());
            self.latest_lba_seq.insert(key, (seq, vol_created_at));
        }
        if self.pending_entries.insert(seq, pending.clone()).is_none() {
            self.pending_count.fetch_add(1, Ordering::Relaxed);
            self.pending_bytes
                .fetch_add(disk_len as u64, Ordering::Relaxed);
        }

        // Account payload bytes toward the in-memory cache budget. LRU
        // eviction (`evict_payload_cache_to_budget`) will strip oldest
        // entries' payload field if we exceed `max_payload_memory`.
        self.payload_bytes_in_memory
            .fetch_add(payload_len, Ordering::Relaxed);
        self.cached_payload_order.lock().push_back(seq);
        self.evict_payload_cache_to_budget();
        self.compact_payload_cache_order_if_needed();

        // ── Channel send (lock-free MPSC, ~30ns) ──
        if self
            .staging_tx
            .send(StagedEntry {
                pending: pending.clone(),
                payload,
            })
            .is_err()
        {
            // Back out the index inserts and the payload accounting.
            self.evict_pending_entry(seq, &pending);
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

    /// Block until the LV2 fdatasync watermark covers `seq`. The sync
    /// thread advances `lv2_durability.synced_seq` after each successful
    /// io_uring fdatasync barrier; `notify_all` then releases every parked
    /// appender whose seq is now durable. Called by `WriteBufferPool::append`
    /// before returning the ack to the caller.
    pub(super) fn wait_for_durable(&self, seq: u64) {
        let wait_elapsed = self.lv2_durability.wait_for(seq);
        if let Some(metrics) = self.metrics.get() {
            let ns = wait_elapsed.as_nanos().min(u64::MAX as u128) as u64;
            metrics
                .buffer_append_wait_durable_ns
                .fetch_add(ns, Ordering::Relaxed);
        }
    }

    /// Push the seq onto the flusher's ready channels. Called by the
    /// appender after `wait_for_durable` returns, so the flusher's
    /// push-notified path sees only seqs that are already (1) past LV2
    /// fdatasync and (2) present in `pending_entries`.
    pub(super) fn publish_ready(&self, seq: u64) {
        let _ = self.ready_tx.send(seq);
        let _ = self.shard_ready_tx.send(seq);
    }

    /// Drop the indices + cache state for an entry that was inserted but
    /// then failed to publish to the sync thread (staging channel closed
    /// during shutdown). Idempotent.
    fn evict_pending_entry(&self, seq: u64, pending: &Arc<PendingEntry>) {
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
        self.remove_pending_buckets(&vid, pending.start_lba, pending.lba_count);
        if let Some((_, removed)) = self.pending_entries.remove(&seq) {
            self.pending_count.fetch_sub(1, Ordering::Relaxed);
            self.pending_bytes
                .fetch_sub(removed.disk_len as u64, Ordering::Relaxed);
            if let Some(ref p) = removed.payload {
                Self::release_payload_bytes(self.payload_bytes_in_memory.as_ref(), p.len() as u64);
            }
            // Drop the seq from the pending-only index too. `append`
            // inserted into pending_seqs under the ring lock; failure
            // before staging_tx.send must roll back both halves.
            self.ring.lock().pending_seqs.remove(&seq);
        }
    }

    pub(super) fn drain_staged_limited(&self) -> Vec<StagedEntry> {
        self.drain_staged_capped(self.sync_batch_max_entries)
    }

    /// Like `drain_staged_limited` but also bounds the entry count at
    /// `max_entries` (clamped to the configured `sync_batch_max_entries`). The
    /// pipelined uring sync path uses this to keep one batch's IO_LINK chain
    /// (≤ `entries + 2` SQEs) within the ring's SQ depth — a chain cannot be
    /// split across submits without dangling the trailing LINK.
    pub(super) fn drain_staged_capped(&self, max_entries: usize) -> Vec<StagedEntry> {
        let cap = max_entries.min(self.sync_batch_max_entries).max(1);
        let mut batch = Vec::new();
        let mut batch_bytes = 0usize;
        while let Ok(entry) = self.staging_rx.try_recv() {
            batch_bytes = batch_bytes.saturating_add(entry.payload.len());
            batch.push(entry);
            if batch.len() >= cap || batch_bytes >= self.sync_batch_max_bytes {
                break;
            }
        }
        batch
    }

    pub(super) fn used_bytes(&self) -> u64 {
        self.ring.lock().used_bytes
    }

    /// Bytes of ring entries that have not yet been mark_applied.
    /// Distinct from physical `used_bytes` post-Phase-D: physical slots
    /// are only released on checkpoint, so `used_bytes` overstates the
    /// soft "work in flight" pressure. Heuristics (dedup skip thresholds,
    /// `fill_percentage`) want this view, not the physical one.
    pub(super) fn pending_bytes(&self) -> u64 {
        self.pending_bytes.load(Ordering::Relaxed)
    }

    pub(super) fn capacity(&self) -> u64 {
        self.ring.lock().capacity_bytes
    }

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
            .unwrap_or(false)
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

    pub(super) fn is_seq_ready_for_flush(&self, seq: u64) -> bool {
        // A seq becomes "ready for the flusher" once the LV2 sync thread
        // has fdatasync'd it. Pure atomic load — no lock, no set membership.
        seq <= self.lv2_durability.synced_seq.load(Ordering::Acquire)
    }

    /// Bounded variant of [`pending_entries_arc_snapshot`] that returns
    /// up to `limit` oldest-seq pending entries that are ready to
    /// flush, **without** walking the entire pending DashMap.
    ///
    /// The flusher's `coalesce_loop` calls this every 100 ms as a
    /// safety net for "payload-less recovered entries that were
    /// skipped once under memory pressure". The previous unbounded
    /// snapshot did `pending_entries.iter().clone(Arc).collect().sort()`
    /// on the entire DashMap — a 280 k-entry full-table scan + sort
    /// per shard per 100 ms in steady state. Profiling pinned that
    /// path as the largest single contributor (~19 G of 152 G) to the
    /// coalesce thread's CPU; this routine drops it to O(limit).
    ///
    /// Implementation:
    ///   1. take the ring lock briefly to copy the `limit * 2` oldest
    ///      seqs from `log_order` (already in insertion / seq order)
    ///   2. take the lifecycle lock once to filter inflight in batch
    ///   3. resolve survivors to `Arc<PendingEntry>` via per-seq
    ///      DashMap lookup, stopping at `limit` resolved entries
    ///
    /// `limit * 2` slack handles entries that fail the readiness
    /// filter or are missing from `pending_entries` (rare: entry
    /// retired between log_order capture and DashMap lookup).
    pub(super) fn oldest_pending_arcs(&self, limit: usize) -> Vec<Arc<PendingEntry>> {
        if limit == 0 || self.pending_count.load(Ordering::Relaxed) == 0 {
            return Vec::new();
        }
        // Snapshot the entire pending index under the ring lock and then
        // walk it outside. pending_seqs.len() is bounded by the buffer's
        // pending_count, which backpressure caps well below log_order's
        // length, so the snapshot stays cheap. A bounded `take(limit*N)`
        // would have to guess how many stale-front seqs to allow for —
        // even one regression in a non-mark_applied removal path could
        // wedge the head behind a tail of stale entries (the cfec549
        // regression). Walking the full index turns any such regression
        // into a few wasted DashMap probes, never a permanent stall.
        let candidates: Vec<u64> = {
            let ring = self.ring.lock();
            ring.pending_seqs.iter().copied().collect()
        };
        let synced = self.lv2_durability.synced_seq.load(Ordering::Acquire);
        let mut result = Vec::with_capacity(limit);
        for seq in candidates {
            if seq > synced {
                // pending_seqs is ascending — nothing past this point
                // can be flush-ready either.
                break;
            }
            if let Some(entry) = self.pending_entries.get(&seq) {
                result.push(entry.value().clone());
                if result.len() >= limit {
                    break;
                }
            }
        }
        result
    }

    pub(super) fn head_pending_seq_if_stuck(&self, min_age: Duration) -> Option<u64> {
        if self.pending_count.load(Ordering::Relaxed) == 0 {
            return None;
        }
        // Walk pending_seqs from smallest seq upward. Skip seqs whose
        // pending_entry has already been retired by a concurrent
        // mark_applied / free_seq_allocation (the index drop happens
        // under the ring lock, but pending_entries.remove runs outside
        // it, so a brief stale-front window is normal). If a non-stale
        // path EVER leaves stale entries in pending_seqs, this loop
        // still finds the genuine head — turning a permanent stall
        // into at most a cheap O(stale) probe per call.
        let (snapshot, head_became_at) = {
            let ring = self.ring.lock();
            (
                ring.pending_seqs.iter().copied().collect::<Vec<u64>>(),
                ring.head_became_at,
            )
        };
        let synced = self.lv2_durability.synced_seq.load(Ordering::Acquire);
        for seq in snapshot {
            if seq > synced {
                // pending_seqs is sorted ascending — everything beyond
                // is also above the durable watermark.
                return None;
            }
            if self.pending_entries.get(&seq).is_none() {
                continue;
            }
            let old_enough = head_became_at.is_some_and(|ts| ts.elapsed() >= min_age);
            let has_partial_progress = self.flush_progress.contains_key(&seq);
            if !has_partial_progress && !old_enough {
                return None;
            }
            return Some(seq);
        }
        None
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
        // With ack-after-LV2-fdatasync semantics, any seq reaching this
        // path was already ack'd to the caller (i.e. sync thread processed
        // it). No need to flag the seq as cancelled — sync will not see it
        // again.
        //
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
            // Mirror `note_applied`: pending_seqs must drop in lockstep
            // with pending_entries. cfec549 introduced the index but only
            // wired the mark_applied → note_applied path; supersede /
            // purge / corrupt-evict all flow through here and would
            // otherwise leave permanent stale seqs at the front of
            // pending_seqs, blocking the coalescer's bounded lookups.
            ring.pending_seqs.remove(&seq);
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
        // See `free_seq_allocation`: ack-after-LV2-fdatasync means no
        // cancellation handshake with the sync thread is required.
        {
            let _guard = self.io_lock.lock();
            Self::mark_entry_flushed(self.device.as_ref(), pending)?;
        }
        {
            let mut ring = self.ring.lock();
            if !ring.flushed_seqs.insert(seq) {
                return Ok(());
            }
            // See `free_seq_allocation`: pending_seqs is the coalescer's
            // bounded lookup index and must drop in lockstep with
            // pending_entries on every path, not just mark_applied.
            ring.pending_seqs.remove(&seq);
            let before = ring.used_bytes;
            Self::reclaim_log_prefix(&mut ring, u64::MAX);
            if ring.used_bytes < before {
                self.ring_space_cv.notify_all();
            }
        }
        self.flush_progress.remove(&seq);
        Ok(())
    }

    #[deprecated(
        note = "use mark_applied + release_below; mark_flushed conflates apply with checkpoint durability"
    )]
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
            self.pending_bytes
                .fetch_sub(removed_pending.disk_len as u64, Ordering::Relaxed);
            self.remove_pending_buckets(&vid, pending.start_lba, pending.lba_count);
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
        self.pending_bytes
            .fetch_sub(removed_pending.disk_len as u64, Ordering::Relaxed);
        self.remove_pending_buckets(&vid, pending.start_lba, pending.lba_count);
        if let Some(ref p) = removed_pending.payload {
            Self::release_payload_bytes(self.payload_bytes_in_memory.as_ref(), p.len() as u64);
        }
        self.free_seq_allocation(seq, &removed_pending);
        Ok(())
    }

    /// Buffer-as-sole-journal Phase A: in-memory bookkeeping for an
    /// applied buffer entry, without advancing the ring head.
    ///
    /// Identical to [`mark_flushed`] for the in-memory parts — drops
    /// from `lba_index`, `pending_entries`, pending buckets, volatile
    /// payload, and accounts the released payload bytes — but **does
    /// not** run `reclaim_log_prefix`. The ring slot stays held until
    /// a later [`release_below`] sweep covers this seq under both the
    /// data durability watermark and the metadb-checkpoint watermark.
    ///
    /// `flushed_seqs` is still populated so the eventual reclaim pass
    /// sees this seq as eligible. Multi-LBA partial-flush handling is
    /// mirrored from `mark_flushed` (the per-seq progress map advances
    /// until every LBA in the entry is covered, then the entry drops).
    ///
    /// Wired in Phase C cutover; in Phase A this is dead code outside
    /// tests.
    pub(super) fn mark_applied(
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
            self.pending_bytes
                .fetch_sub(removed_pending.disk_len as u64, Ordering::Relaxed);
            self.remove_pending_buckets(&vid, pending.start_lba, pending.lba_count);
            if let Some(ref p) = removed_pending.payload {
                Self::release_payload_bytes(self.payload_bytes_in_memory.as_ref(), p.len() as u64);
            }
            self.note_applied(seq);
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
        self.pending_bytes
            .fetch_sub(removed_pending.disk_len as u64, Ordering::Relaxed);
        self.remove_pending_buckets(&vid, pending.start_lba, pending.lba_count);
        if let Some(ref p) = removed_pending.payload {
            Self::release_payload_bytes(self.payload_bytes_in_memory.as_ref(), p.len() as u64);
        }
        self.note_applied(seq);
        Ok(())
    }

    /// Buffer-as-sole-journal Phase A: mirrors `free_seq_allocation` but
    /// only the bookkeeping half (record the seq as ring-reclaim-eligible
    /// without driving the reclaim pass). The ring-head advance comes
    /// later through [`release_below`].
    fn note_applied(&self, seq: u64) {
        // ack-after-fdatasync: no inflight set to consult; the seq was
        // already durable on LV2 when the appender returned.
        self.max_flushed_seq.fetch_max(seq, Ordering::Relaxed);
        let mut ring = self.ring.lock();
        ring.flushed_seqs.insert(seq);
        // Drop the seq from the pending-only index. Coalesce lookups
        // are then O(P) instead of O(log_order.len()) in the
        // applied-but-not-released steady state.
        ring.pending_seqs.remove(&seq);
        self.flush_progress.remove(&seq);
    }

    /// Buffer-as-sole-journal Phase A: advance the ring head past every
    /// already-applied seq whose checkpoint coverage is also assured.
    ///
    /// The reclaim cap is `min(durable_seq, checkpoint_seq)` — both data
    /// (the LV2 fdatasync watermark) and metadb checkpoint (the manifest
    /// commit watermark) must cover an entry before its ring bytes can
    /// be recycled. Returns the number of seqs released this pass.
    pub(super) fn release_below(&self, checkpoint_seq: u64) -> OnyxResult<u64> {
        let durable_seq = self.durable_seq.load(Ordering::Acquire);
        let cap = durable_seq.min(checkpoint_seq);
        let mut ring = self.ring.lock();
        let before = ring.used_bytes;
        Self::reclaim_log_prefix(&mut ring, cap);
        if ring.used_bytes < before {
            self.ring_space_cv.notify_all();
        }
        let advanced = ring.reclaim_ready;
        ring.reclaim_ready = 0;
        Ok(advanced)
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

        // Drop the volume's resident read-cache (lba_index + latest_lba_seq)
        // unconditionally — even when nothing is pending. A flushed entry stays
        // resident (and read-serving) until its ring slot is reclaimed; after an
        // in-place snapshot restore or a delete, those stale entries must not
        // keep masking the rewritten metadb L2P. The pending-entry freeing below
        // still only runs for entries actually in `pending_entries`.
        self.lba_index.retain(|key, _| &*key.vol_id != vol_id);
        self.latest_lba_seq.retain(|key, _| &*key.vol_id != vol_id);

        if to_purge.is_empty() {
            return Ok(Vec::new());
        }

        let mut removed_entries = Vec::with_capacity(to_purge.len());
        for (seq, _) in &to_purge {
            if let Some((_, pending)) = self.pending_entries.remove(seq) {
                self.pending_count.fetch_sub(1, Ordering::Relaxed);
                self.pending_bytes
                    .fetch_sub(pending.disk_len as u64, Ordering::Relaxed);
                let vid = self.intern_vol_id(&pending.vol_id);
                self.remove_pending_buckets(&vid, pending.start_lba, pending.lba_count);
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
        self.pending_bytes
            .fetch_sub(removed_pending.disk_len as u64, Ordering::Relaxed);
        self.remove_pending_buckets(&vid, pending.start_lba, pending.lba_count);
        if let Some(ref p) = removed_pending.payload {
            Self::release_payload_bytes(self.payload_bytes_in_memory.as_ref(), p.len() as u64);
        }
        self.flush_progress.remove(&seq);
        self.free_seq_allocation_durable(seq, &removed_pending)?;
        Ok(true)
    }
}
