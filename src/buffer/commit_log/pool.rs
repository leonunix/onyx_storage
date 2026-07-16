use super::*;

mod layout;
mod open;
mod sync;

struct AppendOrderGuardSet<'a> {
    _guards: Vec<parking_lot::MutexGuard<'a, ()>>,
    metrics: Option<&'a EngineMetrics>,
    hold_started: Instant,
}

impl Drop for AppendOrderGuardSet<'_> {
    fn drop(&mut self) {
        if let Some(metrics) = self.metrics {
            metrics.record_buffer_append_order_hold_ns(
                self.hold_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
    }
}

impl WriteBufferPool {
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

    fn append_order_hash(mut value: u64) -> u64 {
        // SplitMix64 finalizer: fast avalanche after hashing the volume once.
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn lock_append_order(
        &self,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
    ) -> AppendOrderGuardSet<'_> {
        use std::hash::{Hash, Hasher};

        debug_assert!(APPEND_ORDER_STRIPES.is_power_of_two());
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        vol_id.hash(&mut hasher);
        let volume_hash = hasher.finish();
        let mut indices = Vec::with_capacity(lba_count as usize);
        for offset in 0..lba_count {
            let lba = start_lba.0 + offset as u64;
            let mixed =
                Self::append_order_hash(volume_hash ^ lba.wrapping_mul(0x9e37_79b9_7f4a_7c15));
            indices.push((mixed as usize) & (APPEND_ORDER_STRIPES - 1));
        }
        indices.sort_unstable();
        indices.dedup();

        let wait_started = Instant::now();
        let guards = indices
            .into_iter()
            .map(|index| self.append_order_stripes[index].lock.lock())
            .collect();
        let metrics = self.metrics.get().map(Arc::as_ref);
        if let Some(metrics) = metrics {
            metrics.record_buffer_append_order_wait_ns(
                wait_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        AppendOrderGuardSet {
            _guards: guards,
            metrics,
            hold_started: Instant::now(),
        }
    }

    /// Maximum prefix of `[start_lba, start_lba + max_lbas)` that remains in
    /// one physical routing zone. Conditional relocation must not span append
    /// ordering domains; production foreground writes use the same boundary.
    pub(crate) fn lbas_until_routing_boundary(&self, start_lba: Lba, max_lbas: u32) -> u32 {
        if max_lbas == 0 {
            return 0;
        }
        let zone_size = self.routing_zone_size_blocks.max(1);
        let offset = start_lba.0 % zone_size;
        let available = zone_size - offset;
        max_lbas.min(available.min(u64::from(u32::MAX)) as u32)
    }

    /// Find the shard that owns a seq by checking each shard's pending_entries.
    /// O(shard_count) DashMap lookups — fine for background mark_flushed path.
    fn shard_for_seq(&self, seq: u64) -> Option<usize> {
        self.shards
            .iter()
            .position(|shard| shard.shard.has_seq(seq))
    }

    /// ZFS-style hyperbolic foreground throttle. Saturated metadata commit
    /// executors are mapped onto a virtual fill value and use one pool-wide
    /// wake queue; physical ring pressure remains per shard. The backend itself
    /// remains completion-driven.
    pub(super) fn apply_write_throttle(&self, shard_idx: usize, include_backend_debt: bool) {
        let Some(throttle) = self.throttle else {
            return;
        };
        let Some(state) = self.throttle_states.get(shard_idx) else {
            return;
        };
        // Recomputing physical fill acquires the target ring Mutex. Cache it;
        // refresh only every Nth append so the hot path stays on pure atomics
        // when the throttle is armed but inactive. Basis-point precision keeps
        // a narrow dirty window smooth even when LV2 is hundreds of GiB.
        const SAMPLE_INTERVAL: u32 = 32;
        let n = state.sample_counter.fetch_add(1, Ordering::Relaxed);
        let cached_fill_basis_points = state.cached_fill_basis_points.load(Ordering::Relaxed);
        let min_basis_points = u32::from(throttle.min_pct).saturating_mul(100);
        let fill_basis_points =
            if n % SAMPLE_INTERVAL == 0 || cached_fill_basis_points >= min_basis_points {
                let live = self.physical_fill_basis_points_for_shard(shard_idx);
                state
                    .cached_fill_basis_points
                    .store(live, Ordering::Relaxed);
                live
            } else {
                cached_fill_basis_points
            };
        let physical_delay_us = throttle.delay_us_for_fill_basis_points(fill_basis_points);
        let backend_delay_us = if include_backend_debt && self.backend_debt_throttle_enabled {
            self.metrics
                .get()
                .map(|metrics| {
                    let workers = metrics
                        .flush_commit_executors_limit
                        .load(Ordering::Relaxed)
                        .max(1);
                    let active = metrics
                        .flush_commit_executors_active
                        .load(Ordering::Relaxed);
                    let queued = metrics
                        .flush_commit_executor_queue_depth
                        .load(Ordering::Relaxed);
                    throttle.delay_us_for_fill_basis_points(
                        self.backend_throttle_control.pressure_basis_points(
                            throttle,
                            workers,
                            active,
                            queued,
                            self.throttle_anchor.elapsed().as_nanos() as u64,
                        ),
                    )
                })
                .unwrap_or(0)
        } else {
            0
        };
        let backend_debt_dominant = backend_delay_us >= physical_delay_us && backend_delay_us > 0;
        let delay_us = physical_delay_us.max(backend_delay_us);
        if physical_delay_us == 0 {
            state.last_wakeup_ns.store(0, Ordering::Relaxed);
        }
        if include_backend_debt && self.backend_debt_throttle_enabled && backend_delay_us == 0 {
            self.backend_throttle_control
                .last_wakeup_ns
                .store(0, Ordering::Relaxed);
        }
        if delay_us == 0 {
            return;
        }
        let delay_ns = delay_us.saturating_mul(1_000);
        let now_ns = self.throttle_anchor.elapsed().as_nanos() as u64;
        let wakeup_clock = if backend_debt_dominant {
            &self.backend_throttle_control.last_wakeup_ns
        } else {
            &state.last_wakeup_ns
        };
        let mut last = wakeup_clock.load(Ordering::Relaxed);
        let wakeup_ns = loop {
            let baseline = last.max(now_ns);
            let candidate = baseline.saturating_add(delay_ns);
            match wakeup_clock.compare_exchange_weak(
                last,
                candidate,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break candidate,
                Err(actual) => last = actual,
            }
        };
        // The thread may be descheduled while contending on the CAS above.
        // Re-reading the clock avoids sleeping that scheduler delay twice.
        let sleep_start_ns = self.throttle_anchor.elapsed().as_nanos() as u64;
        let sleep_ns = wakeup_ns.saturating_sub(sleep_start_ns);
        if sleep_ns > 0 {
            std::thread::sleep(Duration::from_nanos(sleep_ns));
            if let Some(metrics) = self.metrics.get() {
                metrics
                    .buffer_throttle_count
                    .fetch_add(1, Ordering::Relaxed);
                metrics
                    .buffer_throttle_us_total
                    .fetch_add(sleep_ns / 1_000, Ordering::Relaxed);
                // Track max single throttle delay observed for tail diagnosis.
                let cur_max = metrics.buffer_throttle_us_max.load(Ordering::Relaxed);
                let mine = sleep_ns / 1_000;
                if mine > cur_max {
                    let _ = metrics.buffer_throttle_us_max.compare_exchange(
                        cur_max,
                        mine,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    );
                }
                if backend_debt_dominant {
                    metrics
                        .buffer_backend_debt_throttle_count
                        .fetch_add(1, Ordering::Relaxed);
                    metrics
                        .buffer_backend_debt_throttle_us_total
                        .fetch_add(mine, Ordering::Relaxed);
                    let debt_max = metrics
                        .buffer_backend_debt_throttle_us_max
                        .load(Ordering::Relaxed);
                    if mine > debt_max {
                        let _ = metrics
                            .buffer_backend_debt_throttle_us_max
                            .compare_exchange(debt_max, mine, Ordering::Relaxed, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    /// Latch the persistence fence with `reason`. Idempotent: only the
    /// first call records the reason and logs; subsequent calls are no-ops.
    /// Called by the durability-watermark thread when metadb checkpoints fail
    /// fatally or repeatedly, so `append` stops handing out durable acks the
    /// system can no longer honor.
    pub(crate) fn fence_meta(&self, reason: impl Into<String>) {
        let reason = reason.into();
        if self.meta_fence.set(reason.clone()).is_ok() {
            tracing::error!(
                reason = %reason,
                "persistence pipeline fenced; rejecting new writes until restart"
            );
        }
    }

    /// True once the metadb persistence fence has tripped.
    pub fn is_meta_fenced(&self) -> bool {
        self.meta_fence.get().is_some()
    }

    /// The fence reason if tripped, else `None`. Surfaced by `onyx status`.
    pub fn meta_fence_reason(&self) -> Option<&str> {
        self.meta_fence.get().map(String::as_str)
    }

    pub fn append(
        &self,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        payload: &[u8],
        vol_created_at: u64,
    ) -> OnyxResult<u64> {
        Ok(self
            .append_deferred(vol_id, start_lba, lba_count, payload, vol_created_at)?
            .wait())
    }

    pub fn append_deferred(
        &self,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        payload: &[u8],
        vol_created_at: u64,
    ) -> OnyxResult<BufferAppendTicket> {
        self.append_deferred_inner(vol_id, start_lba, lba_count, payload, vol_created_at, None)
    }

    /// Conditionally append a GC relocation while retaining its old physical
    /// source in memory. `validate` runs inside the same per-shard append-order
    /// lock used by foreground writes, immediately before seq allocation and
    /// LBA-index publication. `Ok(None)` means the source became stale and no
    /// seq or ring space was consumed.
    pub(crate) fn append_relocation_deferred_checked(
        &self,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        payload: &[u8],
        vol_created_at: u64,
        relocation_source: crate::space::extent::Extent,
        validate: impl FnOnce() -> OnyxResult<bool>,
    ) -> OnyxResult<Option<BufferAppendTicket>> {
        self.append_deferred_inner_checked(
            vol_id,
            start_lba,
            lba_count,
            payload,
            vol_created_at,
            Some(relocation_source),
            validate,
        )
    }

    fn append_deferred_inner(
        &self,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        payload: &[u8],
        vol_created_at: u64,
        relocation_source: Option<crate::space::extent::Extent>,
    ) -> OnyxResult<BufferAppendTicket> {
        Ok(self
            .append_deferred_inner_checked(
                vol_id,
                start_lba,
                lba_count,
                payload,
                vol_created_at,
                relocation_source,
                || Ok(true),
            )?
            .expect("unconditional append validation always succeeds"))
    }

    fn append_deferred_inner_checked(
        &self,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        payload: &[u8],
        vol_created_at: u64,
        relocation_source: Option<crate::space::extent::Extent>,
        validate: impl FnOnce() -> OnyxResult<bool>,
    ) -> OnyxResult<Option<BufferAppendTicket>> {
        // Fail-fast when metadb persistence is fenced: the buffer ring is the
        // only durable record until a checkpoint folds it into manifest pages,
        // so if checkpoints are dead an ack here would be a lie (the ring fills
        // and never drains). Reads stay unfenced.
        if let Some(reason) = self.meta_fence.get() {
            return Err(OnyxError::MetaFenced(reason.clone()));
        }
        let total_start = Instant::now();
        let shard_idx = self.shard_for_lba(start_lba);
        if relocation_source.is_some()
            && self.lbas_until_routing_boundary(start_lba, lba_count) != lba_count
        {
            return Err(OnyxError::Config(
                "GC relocation append crosses a buffer routing boundary".into(),
            ));
        }
        self.apply_write_throttle(shard_idx, relocation_source.is_none());
        let shard = &self.shards[shard_idx];
        let prepared =
            shard
                .shard
                .prepare_append(vol_id, start_lba, lba_count, payload, vol_created_at)?;
        let append_order = self.lock_append_order(vol_id, start_lba, lba_count);
        // The fence may trip while this producer was throttled or queued behind
        // another append. Do not enter LV2 after fail-stop has been published.
        let append_result = (|| {
            if let Some(reason) = self.meta_fence.get() {
                return Err(OnyxError::MetaFenced(reason.clone()));
            }
            if !validate()? {
                return Ok(None);
            }
            let (reservation, frontier_guard) = shard.shard.reserve_append(
                &self.next_seq,
                &self.frontier_gate,
                prepared.slot_count,
            )?;
            let seq = reservation.seq;
            let pending = shard
                .shard
                .publish_prepared(reservation, prepared, relocation_source)?;
            // The seq is now either visible in `pending_seqs`, or the append
            // failed and no acknowledged write exists for this seq. Do not hold
            // the gate across fdatasync / ready publication.
            drop(frontier_guard);
            Ok(Some((seq, pending)))
        })();
        if let Err(OnyxError::MetaFenced(reason)) = &append_result {
            // Publish failures after a physical reservation are not locally
            // recoverable: later non-overlapping appends may already own the
            // following ring records. Fence before releasing the LBA stripes
            // so no overlapping writer can pass validation after the fault.
            self.fence_meta(reason.clone());
        }
        drop(append_order);
        if append_result.as_ref().is_ok_and(|result| result.is_some()) {
            shard.shard.maintain_payload_cache_after_append();
        }
        if let Some(metrics) = self.metrics.get() {
            metrics.record_buffer_append_prepare_ns(total_start.elapsed().as_nanos() as u64);
        }
        let Some((seq, pending)) = append_result? else {
            return Ok(None);
        };
        // Wake the per-shard sync thread so it drains the staging channel
        // promptly. The sync thread will fdatasync the batch and then
        // advance `lv2_durability.synced_seq` past our seq, which is what
        // `wait_for_durable` parks on below.
        let _ = shard.sync_wake_tx.send(());
        Ok(Some(BufferAppendTicket {
            shard: shard.shard.clone(),
            pending,
            seq,
            append_started: total_start,
            durability_wait_started: Instant::now(),
            foreground_io_lease: None,
        }))
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

    /// Fast lookup for the aligned batched read path.
    ///
    /// `ZoneManager::submit_write` splits writes at `routing_zone_size_blocks`
    /// boundaries before appending to the buffer, so every LBA covered by a
    /// pending entry maps back to the entry's primary shard. The full
    /// [`lookup`](Self::lookup) keeps its cross-shard safety net for recovery
    /// compatibility and odd direct callers; normal ublk reads use this method
    /// to avoid `shard_count` DashMap probes per 4 KiB block.
    pub fn lookup_primary(&self, vol_id: &str, lba: Lba) -> OnyxResult<Option<PendingEntry>> {
        let primary = self.shard_for_lba(lba);
        let result = self.shards[primary].shard.lookup_hydrated(vol_id, lba)?;
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

    /// Batched primary-shard lookup for a contiguous read span.
    ///
    /// This keeps read-after-write checks in the buffer layer, but removes
    /// the hottest avoidable overhead from large reads: repeated volume-id
    /// interning and routing work for every 4 KiB LBA. The span is split only
    /// where the buffer routing shard changes.
    pub fn lookup_primary_range(
        &self,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
    ) -> OnyxResult<Vec<Option<PendingEntry>>> {
        let mut out = Vec::with_capacity(lba_count as usize);
        if lba_count == 0 {
            return Ok(out);
        }

        let mut done = 0u32;
        while done < lba_count {
            let lba = Lba(start_lba.0 + done as u64);
            let shard_idx = self.shard_for_lba(lba);
            let shard = &self.shards[shard_idx].shard;
            let vid = shard.intern_vol_id(vol_id);

            let shard_end_lba =
                ((lba.0 / self.routing_zone_size_blocks) + 1) * self.routing_zone_size_blocks;
            let this_count = (lba_count - done)
                .min(shard_end_lba.saturating_sub(lba.0).min(u32::MAX as u64) as u32);
            out.extend(shard.lookup_hydrated_range_interned(&vid, lba, this_count)?);
            done += this_count;
        }

        if let Some(metrics) = self.metrics.get() {
            for result in &out {
                let counter = if result.is_some() {
                    &metrics.buffer_lookup_hits
                } else {
                    &metrics.buffer_lookup_misses
                };
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(out)
    }

    pub fn pending_entry(&self, seq: u64) -> Option<BufferEntry> {
        self.shard_for_seq(seq)
            .and_then(|idx| self.shards[idx].shard.pending_entry(seq))
    }

    pub fn pending_entry_arc(&self, seq: u64) -> Option<Arc<PendingEntry>> {
        self.shard_for_seq(seq)
            .and_then(|idx| self.shards[idx].shard.pending_entry_arc_hydrated(seq))
    }

    pub fn hydrate_pending_entries_for_shard(
        &self,
        shard_idx: usize,
        entries: Vec<Arc<PendingEntry>>,
    ) -> Vec<Arc<PendingEntry>> {
        self.shards
            .get(shard_idx)
            .map(|shard| shard.shard.pending_entry_arcs_hydrated(entries))
            .unwrap_or_default()
    }

    pub fn is_latest_lba_seq(&self, vol_id: &str, lba: Lba, seq: u64, vol_created_at: u64) -> bool {
        let shard_idx = self.shard_for_lba(lba);
        self.shards[shard_idx]
            .shard
            .is_latest_lba_seq(vol_id, lba, seq, vol_created_at)
    }

    /// Check whether every LBA in this entry has been superseded by a later
    /// pending write in the same volume generation. Used by the coalescer to
    /// drop fully-shadowed entries before hash/compress/metadata work.
    ///
    /// Entries that span multiple routing shards query the shard owning the
    /// `start_lba`; callers need to use this only for entries that were
    /// originally appended whole (`zone_manager::submit_write` already splits
    /// at zone boundaries, so pending entries never cross shards).
    pub fn is_entry_fully_superseded(
        &self,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        seq: u64,
        vol_created_at: u64,
    ) -> bool {
        let shard_idx = self.shard_for_lba(start_lba);
        self.shards[shard_idx].shard.is_entry_fully_superseded(
            vol_id,
            start_lba,
            lba_count,
            seq,
            vol_created_at,
        )
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

    pub fn ready_pending_entries_arc_snapshot_for_shard(
        &self,
        shard_idx: usize,
    ) -> Vec<Arc<PendingEntry>> {
        self.shards
            .get(shard_idx)
            .map(|shard| {
                let mut entries: Vec<_> = shard
                    .shard
                    .pending_entries_arc_snapshot()
                    .into_iter()
                    .filter(|entry| shard.shard.is_seq_ready_for_flush(entry.seq))
                    .collect();
                entries.sort_by_key(|entry| entry.seq);
                entries
            })
            .unwrap_or_default()
    }

    /// Bounded counterpart of [`ready_pending_entries_arc_snapshot_for_shard`].
    /// Returns up to `limit` oldest-seq ready pending entries without
    /// walking the entire shard pending set. See
    /// [`Shard::oldest_pending_arcs`] for the cost model.
    pub fn oldest_ready_pending_arcs_for_shard(
        &self,
        shard_idx: usize,
        limit: usize,
    ) -> Vec<Arc<PendingEntry>> {
        self.shards
            .get(shard_idx)
            .map(|shard| shard.shard.oldest_pending_arcs(limit))
            .unwrap_or_default()
    }

    pub fn oldest_ready_pending_arcs_for_shard_with_budget(
        &self,
        shard_idx: usize,
        limit: usize,
        byte_limit: usize,
    ) -> Vec<Arc<PendingEntry>> {
        self.shards
            .get(shard_idx)
            .map(|shard| {
                shard
                    .shard
                    .oldest_pending_arcs_with_budget(limit, byte_limit)
            })
            .unwrap_or_default()
    }

    pub fn head_stuck_seq_for_shard(&self, shard_idx: usize, min_age: Duration) -> Option<u64> {
        self.shards
            .get(shard_idx)
            .and_then(|shard| shard.shard.head_pending_seq_if_stuck(min_age))
    }

    pub fn flushed_offsets_for_shard(&self, shard_idx: usize, seq: u64) -> Option<HashSet<u16>> {
        self.shards
            .get(shard_idx)
            .and_then(|shard| shard.shard.flushed_offsets_snapshot(seq))
    }

    /// Cheap, non-hydrating diagnostic snapshot for a given (shard, seq).
    /// Returns (lba_count, flushed_count, age_ms, vol_id). Used by the flusher
    /// to log head-stuck states without triggering payload re-hydration.
    pub fn pending_diag_snapshot_for_shard(
        &self,
        shard_idx: usize,
        seq: u64,
    ) -> Option<(u32, u32, u64, String)> {
        self.shards
            .get(shard_idx)
            .and_then(|shard| shard.shard.pending_diag_snapshot(seq))
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

    /// Legacy "in-memory release + immediate ring reclaim against the
    /// current `durable_seq`" entry point. Production callers now use
    /// `mark_applied` (in-memory only) plus `release_below` driven by
    /// the durability-watermark thread on confirmed checkpoint
    /// outcomes. Retained for `shard.rs` internal failure-path cleanup
    /// and for the buffer-pool unit tests.
    #[deprecated(
        note = "use mark_applied + release_below; mark_flushed conflates apply with checkpoint durability"
    )]
    pub fn mark_flushed(
        &self,
        seq: u64,
        flushed_lba_start: Lba,
        flushed_lba_count: u32,
    ) -> OnyxResult<()> {
        let Some(shard_idx) = self.shard_for_seq(seq) else {
            return Ok(());
        };
        #[allow(deprecated)]
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

    /// Persist the current reclaim position for every shard and flush LV2.
    /// Clean shutdown must call this explicitly after its final tail advance;
    /// relying on `Drop` is insufficient while other `Arc` owners still exist.
    /// Callers must quiesce foreground appends first: the global seq floor is
    /// intentionally stamped into every shard and is not an online checkpoint
    /// protocol racing with partially prepared sync batches.
    pub fn persist_checkpoints(&self) -> OnyxResult<()> {
        let global_max_seq = self.next_seq.load(Ordering::Acquire).saturating_sub(1);
        if let Some(packed) = self.packed_checkpoint.as_ref() {
            let mut state = packed.lock();
            let generation = state.begin_next()?;
            for (idx, shard) in self.shards.iter().enumerate() {
                let mut checkpoint = shard.shard.snapshot_checkpoint();
                checkpoint.max_seq = global_max_seq;
                state.pending_checkpoints[idx] = checkpoint;
            }
            state.encode_pending(generation)?;
            Self::write_packed_checkpoint_page(
                self.root_device.as_ref(),
                generation,
                state.scratch.as_slice(),
            )?;
            Self::sync_device_impl(self.root_device.as_ref())?;
            state.commit_pending(generation);
            return Ok(());
        }
        for shard in &self.shards {
            shard.shard.persist_checkpoint(global_max_seq)?;
        }
        Self::sync_device_impl(self.root_device.as_ref())
    }

    /// Buffer-as-sole-journal Phase A: routes to the seq's owning shard
    /// and runs the in-memory half of `mark_flushed` without the ring
    /// reclaim. The ring head advances later through [`release_below`].
    /// Dead code outside tests until Phase C wires the flusher to it.
    pub fn mark_applied(
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
            .mark_applied(seq, flushed_lba_start, flushed_lba_count)?;
        Ok(())
    }

    /// Buffer-as-sole-journal Phase A: drive the ring-reclaim pass on
    /// every shard with the checkpoint watermark as an upper bound on
    /// reclaimable seqs. Returns the total number of seqs released
    /// across all shards this pass.
    pub fn release_below(&self, checkpoint_seq: u64) -> OnyxResult<u64> {
        let mut advanced = 0u64;
        for shard in &self.shards {
            advanced += shard.shard.release_below(checkpoint_seq)?;
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

    pub fn pending_count_for_shard(&self, shard_idx: usize) -> u64 {
        self.shards
            .get(shard_idx)
            .map(|shard| shard.shard.pending_count())
            .unwrap_or(0)
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

    /// Soft "work in flight" pressure as a percentage of total ring
    /// capacity. Reflects bytes of entries that haven't been mark_applied
    /// yet — i.e. real downstream work the flusher still owes metadb.
    /// Post-Phase-D this is NOT the same as physical ring fill, since
    /// applied entries linger in the ring until the next checkpoint runs
    /// `release_below`. Heuristics (dedup skip, scanner pressure gate)
    /// want this view; appender backpressure stays on physical ring state
    /// inside `reserve_log_space`.
    pub fn fill_percentage(&self) -> u8 {
        let total_capacity = self.capacity();
        if total_capacity == 0 {
            return 100;
        }
        let total_pending: u64 = self
            .shards
            .iter()
            .map(|shard| shard.shard.pending_bytes())
            .sum();
        ((total_pending * 100) / total_capacity).min(100) as u8
    }

    /// Per-shard variant of [`fill_percentage`]. Same soft "work in
    /// flight" semantics — see that method for the post-Phase-D
    /// distinction from physical ring fill.
    pub fn fill_percentage_for_shard(&self, shard_idx: usize) -> u8 {
        let Some(shard) = self.shards.get(shard_idx) else {
            return 100;
        };
        let cap = shard.shard.capacity();
        if cap == 0 {
            return 100;
        }
        ((shard.shard.pending_bytes() * 100) / cap).min(100) as u8
    }

    /// Hard-capacity pressure across the LV2 ring shards.
    ///
    /// Unlike [`fill_percentage`], this includes entries already applied to
    /// metadb but retained in LV2 until a checkpoint covers them. Append hard
    /// backpressure is per shard, so return the fullest shard rather than an
    /// aggregate that could hide a hot shard behind free space elsewhere.
    pub fn physical_fill_percentage(&self) -> u8 {
        (0..self.shards.len())
            .map(|idx| self.physical_fill_percentage_for_shard(idx))
            .max()
            .unwrap_or(0)
    }

    /// Exact physical-ring emptiness. Unlike [`Self::physical_fill_percentage`],
    /// this does not round sub-percent occupancy down to zero.
    pub fn physical_is_empty(&self) -> bool {
        self.shards
            .iter()
            .all(|shard| shard.shard.used_bytes() == 0)
    }

    pub fn physical_fill_percentage_for_shard(&self, shard_idx: usize) -> u8 {
        let Some(shard) = self.shards.get(shard_idx) else {
            return 100;
        };
        let ring = shard.shard.ring.lock();
        if ring.capacity_bytes == 0 {
            return 100;
        }
        ((u128::from(ring.used_bytes).saturating_mul(100) / u128::from(ring.capacity_bytes))
            .min(100)) as u8
    }

    pub(crate) fn physical_fill_basis_points_for_shard(&self, shard_idx: usize) -> u32 {
        let Some(shard) = self.shards.get(shard_idx) else {
            return 10_000;
        };
        let ring = shard.shard.ring.lock();
        if ring.capacity_bytes == 0 {
            return 10_000;
        }
        ((u128::from(ring.used_bytes).saturating_mul(10_000) / u128::from(ring.capacity_bytes))
            .min(10_000)) as u32
    }

    /// Evict hydrated payloads from pending_entries for the given shard.
    /// Called by the coalescer after payload data has been copied into
    /// CoalesceUnits, so the memory budget is freed without waiting for
    /// mark_flushed at the end of the pipeline.
    pub fn evict_hydrated_payloads_for_shard(&self, shard_idx: usize, seqs: &[u64]) {
        if let Some(shard) = self.shards.get(shard_idx) {
            shard.shard.evict_hydrated_payloads(seqs);
        }
    }

    /// Total payload bytes currently kept resident in memory across all shards.
    pub fn payload_memory_bytes(&self) -> u64 {
        self.payload_bytes_in_memory.load(Ordering::Relaxed)
    }

    /// Configured durable payload-cache ceiling. 0 disables resident caching.
    pub fn payload_memory_limit_bytes(&self) -> u64 {
        self.max_payload_memory
    }

    /// Resident-payload depth as a percentage of the configured ceiling
    /// (`payload_memory_bytes / payload_memory_limit_bytes`). Unlike
    /// [`fill_percentage`] (soft "work in flight" vs ring capacity, which reads
    /// ~0 even when the payload cache has ballooned to multiple GB because a
    /// throttled downstream is holding payloads resident), this directly tracks
    /// how full the durable payload cache is. Background pacing
    /// (the GC compactor's self-throttle) takes the max of the two so EITHER
    /// pressure source backs it off. Returns 0 when resident caching is
    /// disabled (`limit == 0`) so the caller falls back to the ring signal.
    pub fn payload_fill_percentage(&self) -> u8 {
        let limit = self.max_payload_memory;
        if limit == 0 {
            return 0;
        }
        let used = self.payload_bytes_in_memory.load(Ordering::Relaxed);
        ((used.saturating_mul(100)) / limit).min(100) as u8
    }

    /// Atomic shared with every shard that tracks the highest seq to have
    /// been mark_flushed'd. Intended for the durability-watermark thread
    /// to capture before invoking `MetaStore::sync_durable`.
    pub fn max_flushed_seq_handle(&self) -> Arc<AtomicU64> {
        self.max_flushed_seq.clone()
    }

    /// Greatest global seq for which every acknowledged buffer entry at or
    /// below it has completed its metadb apply.
    ///
    /// `max_flushed_seq` is only an upper bound and can jump over an older
    /// commit that is still pending on another shard. The manifest replay
    /// watermark and LV2 release boundary require a contiguous prefix, so we
    /// derive it from the minimum pending seq across all physical shards.
    pub fn applied_frontier(&self) -> u64 {
        let _frontier_guard = self.frontier_gate.write();
        let max_allocated = self.next_seq.load(Ordering::Acquire).saturating_sub(1);
        self.shards
            .iter()
            .filter_map(|shard| shard.shard.oldest_pending_seq())
            .min()
            .map(|oldest| oldest.saturating_sub(1).min(max_allocated))
            .unwrap_or(max_allocated)
    }

    /// Ensure future buffer records use a sequence strictly above a durable
    /// metadb manifest watermark.
    ///
    /// The LV2 superblock normally preserves this floor. A clean LV2 rebuild
    /// (replacement device, explicit reinitialization, or shard-layout change)
    /// has no local history, while metadb can still contain a much higher
    /// `last_processed_buffer_seq`. Reusing those lower sequence numbers would
    /// make the old manifest watermark appear to cover new buffer records.
    pub(crate) fn ensure_next_seq_above(&self, durable_manifest_seq: u64) -> OnyxResult<bool> {
        let required_next = durable_manifest_seq.checked_add(1).ok_or_else(|| {
            OnyxError::Config("durable buffer sequence exhausted u64 range".into())
        })?;
        let _frontier_guard = self.frontier_gate.write();
        let previous = self.next_seq.fetch_max(required_next, Ordering::AcqRel);
        Ok(previous < required_next)
    }

    pub(crate) fn next_sequence(&self) -> u64 {
        self.next_seq.load(Ordering::Acquire)
    }

    pub(crate) fn uses_legacy_v2_layout(&self) -> bool {
        self.disk_version == COMMIT_LOG_VERSION_V2
    }

    pub(crate) fn durable_sequence(&self) -> u64 {
        self.durable_seq.load(Ordering::Acquire)
    }

    /// Atomic shared with every shard that gates ring-reclaim: an entry is
    /// only truly reclaimable once its seq ≤ `durable_seq`. The durability
    /// watermark thread advances this after a successful sync.
    pub fn durable_seq_handle(&self) -> Arc<AtomicU64> {
        self.durable_seq.clone()
    }

    /// Snapshot per-shard buffer statistics for monitoring.
    pub fn shard_snapshots(&self) -> Vec<BufferShardSnapshot> {
        self.shards
            .iter()
            .enumerate()
            .map(|(idx, handle)| {
                let s = &handle.shard;
                let (
                    used,
                    capacity,
                    head,
                    tail,
                    log_order_len,
                    flushed_seqs_len,
                    head_seq,
                    head_became_at,
                ) = {
                    let ring = s.ring.lock();
                    (
                        ring.used_bytes,
                        ring.capacity_bytes,
                        ring.head_offset,
                        ring.tail_offset,
                        ring.log_order.len(),
                        ring.flushed_seqs.len(),
                        ring.log_order.front().map(|r| r.seq),
                        ring.head_became_at,
                    )
                };
                let (head_remaining_lbas, head_age_ms, head_residency_ms) =
                    s.head_seq_debug_state(head_seq, head_became_at);
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
                    append_ops: s.append_ops.load(Ordering::Relaxed),
                    append_bytes: s.append_bytes.load(Ordering::Relaxed),
                    reserve_wait_ns: s.reserve_wait_ns.load(Ordering::Relaxed),
                    head_offset: head,
                    tail_offset: tail,
                    log_order_len,
                    flushed_seqs_len,
                    head_seq,
                    head_remaining_lbas,
                    head_age_ms,
                    head_residency_ms,
                    staged_entries: s.staging_rx.len(),
                    volatile_payloads: 0,
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
        // Do not rewrite checkpoints or the immutable global superblock here.
        // Normal engine shutdown persists the final reclaim boundary explicitly;
        // sync batches already checkpoint standalone writes. Best-effort Drop
        // rewrites only add torn-write windows during process teardown.
    }
}
