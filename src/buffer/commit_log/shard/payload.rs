use super::*;

impl BufferShard {
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

    // NOTE: `cache_committed_payload` was deleted as part of the
    // ack-after-LV2-fdatasync refactor. Payload now lives in
    // `PendingEntry::payload` from append time onward (set eagerly in
    // `append_with_seq`); the sync thread no longer mutates index state
    // post-fdatasync, so the previous "swap Arc to inject payload" dance
    // disappears entirely.

    pub(in crate::buffer::commit_log) fn evict_payload_cache_to_budget(&self) {
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
        self.record_payload_cache_evict(payload_len);
        self.replace_lba_index_if_current(&pending, &evicted);
        true
    }

    pub(super) fn record_payload_cache_evict(&self, payload_len: u64) {
        if let Some(metrics) = self.metrics.get() {
            metrics
                .buffer_payload_cache_evict_entries
                .fetch_add(1, Ordering::Relaxed);
            metrics
                .buffer_payload_cache_evict_bytes
                .fetch_add(payload_len, Ordering::Relaxed);
        }
    }

    pub(in crate::buffer::commit_log) fn compact_payload_cache_order_if_needed(&self) {
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

    /// Read payload from the buffer device for a recovered entry.
    pub(in crate::buffer::commit_log) fn read_payload_from_disk(
        &self,
        pending: &PendingEntry,
    ) -> OnyxResult<Arc<[u8]>> {
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

    // `volatile_payload` / `remove_volatile_payload` / `is_seq_inflight`
    // were removed. Payload now lives in `PendingEntry::payload` from append
    // time onward; reads consult that field, falling back to LV2 disk only
    // for crash-recovered entries (which keep `payload: None`). The flusher's
    // readiness check is `seq <= lv2_durability.synced_seq` — see
    // `is_seq_ready_for_flush`.

    /// Return a PendingEntry with payload guaranteed present. If the entry
    /// was recovered without payload (lazy), reads it from disk now.
    /// If hydration fails (corrupt disk region), the entry is evicted from
    /// all indices so subsequent reads fall through to the blockmap (LV3).

    pub(in crate::buffer::commit_log) fn lookup_hydrated(
        &self,
        vol_id: &str,
        lba: Lba,
    ) -> OnyxResult<Option<PendingEntry>> {
        let vid = self.intern_vol_id(vol_id);
        self.lookup_hydrated_interned(&vid, lba)
    }

    pub(in crate::buffer::commit_log) fn lookup_hydrated_interned(
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
        // payload=None means crash-recovered entry: lazy-hydrate from LV2.
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

    pub(in crate::buffer::commit_log) fn lookup_hydrated_range_interned(
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
        for entry in indexed.iter().flatten() {
            if let Some(payload) = entry.payload.clone() {
                payloads.entry(entry.seq).or_insert(payload);
            } else if seen_missing.insert(entry.seq) {
                missing.push(entry.clone());
            }
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

    pub(in crate::buffer::commit_log) fn pending_entry(&self, seq: u64) -> Option<BufferEntry> {
        let entry = self.pending_entries.get(&seq)?;
        if let Some(payload) = entry.payload.clone() {
            return Some(Self::pending_with_payload_to_buffer_entry(&entry, payload));
        }
        Some(Self::pending_to_buffer_entry(&entry))
    }

    pub(in crate::buffer::commit_log) fn pending_entry_arc(
        &self,
        seq: u64,
    ) -> Option<Arc<PendingEntry>> {
        self.pending_entries
            .get(&seq)
            .map(|entry| entry.value().clone())
    }

    pub(in crate::buffer::commit_log) fn evicted_pending_entry(
        pending: &PendingEntry,
    ) -> Arc<PendingEntry> {
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
            durability_advanced_at_ns: AtomicU64::new(
                pending.durability_advanced_at_ns.load(Ordering::Relaxed),
            ),
            superseded_ranges: pending.superseded_ranges.clone(),
        })
    }

    pub(in crate::buffer::commit_log) fn replace_pending_entry_if_current(
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

    pub(in crate::buffer::commit_log) fn pending_entry_arc_hydrated(
        &self,
        seq: u64,
    ) -> Option<Arc<PendingEntry>> {
        let entry_ref = self.pending_entries.get(&seq)?;
        let entry = entry_ref.value().clone();
        if entry.payload.is_some() {
            return Some(entry);
        }
        drop(entry_ref);
        // payload=None means crash-recovered entry: lazy-hydrate from LV2,
        // returning a detached PendingEntry so the indices stay payload-less
        // and read budget is unbounded only for the call's lifetime.
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

    pub(in crate::buffer::commit_log) fn pending_entry_arcs_hydrated(
        &self,
        entries: Vec<Arc<PendingEntry>>,
    ) -> Vec<Arc<PendingEntry>> {
        if entries.is_empty() {
            return entries;
        }

        let hydrate_started = Instant::now();
        let total_entries = entries.len() as u64;
        let mut memory_entries = 0u64;
        let mut payloads: HashMap<u64, Arc<[u8]>> = HashMap::with_capacity(entries.len());
        let mut missing = Vec::new();
        let mut seen_missing = HashSet::new();
        for entry in &entries {
            if let Some(payload) = entry.payload.clone() {
                memory_entries += 1;
                payloads.entry(entry.seq).or_insert(payload);
            } else if seen_missing.insert(entry.seq) {
                missing.push(entry.clone());
            }
        }
        let disk_entries = missing.len() as u64;
        payloads.extend(self.hydrate_missing_payloads_batched(&missing, false));

        if let Some(metrics) = self.metrics.get() {
            metrics
                .buffer_coalesce_hydrate_ns
                .fetch_add(Self::elapsed_ns(hydrate_started), Ordering::Relaxed);
            metrics
                .buffer_coalesce_hydrate_ops
                .fetch_add(1, Ordering::Relaxed);
            metrics
                .buffer_coalesce_hydrate_entries
                .fetch_add(total_entries, Ordering::Relaxed);
            metrics
                .buffer_coalesce_hydrate_memory_entries
                .fetch_add(memory_entries, Ordering::Relaxed);
            metrics
                .buffer_coalesce_hydrate_disk_entries
                .fetch_add(disk_entries, Ordering::Relaxed);
        }

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

    pub(in crate::buffer::commit_log) fn pending_entries_snapshot(&self) -> Vec<BufferEntry> {
        self.pending_entries
            .iter()
            .map(|entry| Self::pending_to_buffer_entry(&entry))
            .collect()
    }

    pub(in crate::buffer::commit_log) fn pending_entries_arc_snapshot(
        &self,
    ) -> Vec<Arc<PendingEntry>> {
        self.pending_entries
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }
}
