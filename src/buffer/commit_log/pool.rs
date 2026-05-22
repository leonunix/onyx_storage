use super::*;

mod layout;
mod sync;

impl WriteBufferPool {
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
        Self::open_with_options_full(
            device,
            group_commit_wait,
            shard_count,
            routing_zone_size_blocks,
            backpressure_timeout,
            max_payload_memory,
            None,
        )
    }

    /// Variant that lets the caller request io_uring-backed sync threads.
    /// `uring_sq_entries=Some(n)` creates one io_uring session per shard with
    /// SQ depth `n`; `None` keeps the classic syscall (pread/pwrite + fsync)
    /// sync path.
    pub fn open_with_options_full(
        device: RawDevice,
        group_commit_wait: Duration,
        shard_count: usize,
        routing_zone_size_blocks: u64,
        backpressure_timeout: Duration,
        max_payload_memory: u64,
        uring_sq_entries: Option<u32>,
    ) -> OnyxResult<Self> {
        let runtime_limits = BufferRuntimeLimits::for_durable_payload_limit(max_payload_memory);
        Self::open_with_options_full_and_limits(
            device,
            group_commit_wait,
            shard_count,
            routing_zone_size_blocks,
            backpressure_timeout,
            max_payload_memory,
            uring_sq_entries,
            runtime_limits,
        )
    }

    pub fn open_with_options_full_and_limits(
        device: RawDevice,
        group_commit_wait: Duration,
        shard_count: usize,
        routing_zone_size_blocks: u64,
        backpressure_timeout: Duration,
        max_payload_memory: u64,
        uring_sq_entries: Option<u32>,
        runtime_limits: BufferRuntimeLimits,
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
        // Round down to block_size so every shard's base_offset stays
        // block-aligned.  Without this, shards 1..N get non-aligned global
        // offsets and silently fall back to buffered IO, which on the same
        // block device as shard 0's O_DIRECT causes page-cache coherency
        // corruption (mixed O_DIRECT + buffered IO on one file).
        let bytes_per_shard = (total_data_bytes / shard_count as u64) & !(BLOCK_SIZE as u64 - 1);

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
        let volatile_payload_budget = Arc::new(VolatilePayloadBudget::new(
            runtime_limits.volatile_payload_memory,
        ));
        // Durability-watermark atomics shared with every shard. `max_flushed_seq`
        // is bumped in free_seq_allocation; `durable_seq` is advanced by the
        // engine-owned watermark thread after MetaStore::sync_durable().
        let max_flushed_seq = Arc::new(AtomicU64::new(0));
        let durable_seq = Arc::new(AtomicU64::new(0));
        let shard_results: Vec<OnyxResult<(BufferShard, u64)>> = if shard_count > 1 {
            std::thread::scope(|s| {
                let handles: Vec<_> = shard_configs
                    .into_iter()
                    .map(|cfg| {
                        let m = metrics.clone();
                        let pb = payload_bytes_in_memory.clone();
                        let vb = volatile_payload_budget.clone();
                        let mfs = max_flushed_seq.clone();
                        let ds = durable_seq.clone();
                        s.spawn(move || {
                            BufferShard::open(
                                cfg.data_device,
                                backpressure_timeout,
                                m,
                                cfg.checkpoint,
                                cfg.checkpoint_device,
                                pb,
                                max_payload_memory,
                                vb,
                                runtime_limits,
                                mfs,
                                ds,
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
                        volatile_payload_budget.clone(),
                        runtime_limits,
                        max_flushed_seq.clone(),
                        durable_seq.clone(),
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
            // Per-shard io_uring session (one ring per sync thread, no
            // contention). Skipped when uring_sq_entries is None.
            let shard_uring = match uring_sq_entries {
                Some(entries) => Some(Arc::new(IoUringSession::new(entries)?)),
                None => None,
            };
            let sync_thread = thread::Builder::new()
                .name(format!("persistent-slot-sync-{}", shard_idx))
                .spawn({
                    let metrics = metrics.clone();
                    let shard = shard.clone();
                    let shutdown = sync_shutdown.clone();
                    let ready_tx = ready_tx.clone();
                    let shard_ready_tx = shard_ready_tx.clone();
                    let uring = shard_uring.clone();
                    move || {
                        crate::affinity::bind_current(
                            crate::affinity::ThreadRole::BufferSync,
                            shard_idx,
                        );
                        Self::sync_loop(
                            sync_device,
                            shard,
                            group_commit_wait,
                            sync_wake_rx,
                            shutdown,
                            metrics,
                            ready_tx,
                            shard_ready_tx,
                            uring,
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
        let throttle = runtime_limits.throttle.resolved();
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
            volatile_payload_budget,
            disk_version,
            max_flushed_seq,
            durable_seq,
            throttle,
            throttle_anchor: Instant::now(),
            throttle_last_wakeup_ns: AtomicU64::new(0),
            throttle_cached_fill_pct: AtomicU32::new(0),
            throttle_sample_counter: AtomicU32::new(0),
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

    /// ZFS-style hyperbolic write throttle on LV2 fill. Returns immediately
    /// when the throttle is disabled or fill is below the configured floor.
    /// Otherwise sleeps until an atomically-claimed slot, so N concurrent
    /// producers stack into N × delay rather than collapsing into one window
    /// (throughput is independent of producer thread count, matching ZFS
    /// `dmu_tx_delay`).
    fn apply_write_throttle(&self) {
        let Some(throttle) = self.throttle else {
            return;
        };
        // Recomputing fill_percentage() acquires one Mutex per shard. Cache
        // it; refresh only every Nth append so the hot path stays on pure
        // atomics when the throttle is armed but inactive. The curve is
        // continuous and the absolute-wakeup queue smooths over the sample
        // lag, so a few-append staleness in fill_pct is invisible end-to-end.
        const SAMPLE_INTERVAL: u32 = 32;
        let n = self.throttle_sample_counter.fetch_add(1, Ordering::Relaxed);
        let fill_pct = if n % SAMPLE_INTERVAL == 0 {
            let live = self.fill_percentage();
            self.throttle_cached_fill_pct
                .store(live as u32, Ordering::Relaxed);
            live
        } else {
            self.throttle_cached_fill_pct.load(Ordering::Relaxed) as u8
        };
        let delay_us = throttle.delay_us_for_fill(fill_pct);
        if delay_us == 0 {
            return;
        }
        let delay_ns = delay_us.saturating_mul(1_000);
        let now_ns = self.throttle_anchor.elapsed().as_nanos() as u64;
        let mut last = self.throttle_last_wakeup_ns.load(Ordering::Relaxed);
        let wakeup_ns = loop {
            let baseline = last.max(now_ns);
            let candidate = baseline.saturating_add(delay_ns);
            match self.throttle_last_wakeup_ns.compare_exchange_weak(
                last,
                candidate,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break candidate,
                Err(actual) => last = actual,
            }
        };
        let sleep_ns = wakeup_ns.saturating_sub(now_ns);
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
            }
        }
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
        self.apply_write_throttle();
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

    /// Bytes currently held only until the buffer sync thread fdatasyncs them.
    pub fn volatile_payload_memory_bytes(&self) -> u64 {
        self.volatile_payload_budget.bytes()
    }

    /// Write-admission budget for sync-before-publish payloads.
    pub fn volatile_payload_memory_limit_bytes(&self) -> u64 {
        self.volatile_payload_budget.limit()
    }

    /// Atomic shared with every shard that tracks the highest seq to have
    /// been mark_flushed'd. Intended for the durability-watermark thread
    /// to capture before invoking `MetaStore::sync_durable`.
    pub fn max_flushed_seq_handle(&self) -> Arc<AtomicU64> {
        self.max_flushed_seq.clone()
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
                    head_offset: head,
                    tail_offset: tail,
                    log_order_len,
                    flushed_seqs_len,
                    head_seq,
                    head_remaining_lbas,
                    head_age_ms,
                    head_residency_ms,
                    staged_entries: s.staging_rx.len(),
                    volatile_payloads: s.volatile_payloads.len(),
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
