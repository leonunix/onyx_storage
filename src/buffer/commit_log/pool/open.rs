use super::*;

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
        let runtime_limits = BufferRuntimeLimits::default();
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
        // Durability-watermark atomics shared with every shard. `max_flushed_seq`
        // is bumped in free_seq_allocation; `durable_seq` is advanced by the
        // engine-owned watermark thread after MetaStore::sync_durable().
        let max_flushed_seq = Arc::new(AtomicU64::new(0));
        let durable_seq = Arc::new(AtomicU64::new(0));

        // Per-shard LV2 fdatasync watermark + cvar — what `pool.append`
        // parks on before returning to the caller. One per shard so wakes
        // only release appenders whose write hit this shard.
        let lv2_durability_per_shard: Vec<Arc<Lv2DurabilityWaiter>> = (0..shard_count)
            .map(|_| Arc::new(Lv2DurabilityWaiter::new(0)))
            .collect();

        // Ready channels live up here so we can hand clones to each shard
        // before recovery — appenders publish ready seqs through them, and
        // we still need to feed any crash-recovered seqs into the channels
        // post-open below.
        let (ready_tx, ready_rx) = unbounded();
        let mut shard_ready_txs_for_open = Vec::with_capacity(shard_count);
        let mut shard_ready_rxs_for_pool = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let (tx, rx) = unbounded();
            shard_ready_txs_for_open.push(tx);
            shard_ready_rxs_for_pool.push(rx);
        }

        let shard_results: Vec<OnyxResult<(BufferShard, u64)>> = if shard_count > 1 {
            std::thread::scope(|s| {
                let handles: Vec<_> = shard_configs
                    .into_iter()
                    .enumerate()
                    .map(|(idx, cfg)| {
                        let m = metrics.clone();
                        let pb = payload_bytes_in_memory.clone();
                        let mfs = max_flushed_seq.clone();
                        let ds = durable_seq.clone();
                        let lv2 = lv2_durability_per_shard[idx].clone();
                        let rtx = ready_tx.clone();
                        let srtx = shard_ready_txs_for_open[idx].clone();
                        s.spawn(move || {
                            BufferShard::open(
                                cfg.data_device,
                                backpressure_timeout,
                                m,
                                cfg.checkpoint,
                                cfg.checkpoint_device,
                                pb,
                                max_payload_memory,
                                runtime_limits,
                                mfs,
                                ds,
                                lv2,
                                rtx,
                                srtx,
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
                .enumerate()
                .map(|(idx, cfg)| {
                    BufferShard::open(
                        cfg.data_device,
                        backpressure_timeout,
                        metrics.clone(),
                        cfg.checkpoint,
                        cfg.checkpoint_device,
                        payload_bytes_in_memory.clone(),
                        max_payload_memory,
                        runtime_limits,
                        max_flushed_seq.clone(),
                        durable_seq.clone(),
                        lv2_durability_per_shard[idx].clone(),
                        ready_tx.clone(),
                        shard_ready_txs_for_open[idx].clone(),
                    )
                })
                .collect()
        };

        // ── Sequential setup: channels + sync threads ────────────────
        let mut shard_ready_txs = Vec::with_capacity(shard_count);
        let mut shard_ready_rxs = Vec::with_capacity(shard_count);
        let mut shards = Vec::with_capacity(shard_count);
        let mut max_seq = 0u64;

        // Recompute consumed for sync device slices.
        consumed = 0u64;
        for (shard_idx, result) in shard_results.into_iter().enumerate() {
            let (shard, shard_max_seq) = result?;
            shard.compact_recovered_stale_ranges();

            // Recovered entries were durable on LV2 by definition, so seed
            // the LV2 watermark with the max recovered seq before publishing
            // them — otherwise `is_seq_ready_for_flush` would gate them off
            // immediately after open.
            shard
                .lv2_durability
                .advance(shard.lv2_durability.synced_seq.load(Ordering::Relaxed).max(shard_max_seq));

            let shard_ready_tx_for_loop = shard_ready_txs_for_open[shard_idx].clone();
            let mut recovered_seqs: Vec<u64> = shard
                .pending_entries
                .iter()
                .map(|entry| *entry.key())
                .collect();
            recovered_seqs.sort_unstable();
            for seq in recovered_seqs {
                let _ = ready_tx.send(seq);
                let _ = shard_ready_tx_for_loop.send(seq);
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
                    let shard_ready_tx = shard_ready_tx_for_loop.clone();
                    let uring = shard_uring.clone();
                    let pipeline_depth = runtime_limits.lv2_sync_pipeline_depth;
                    let commit_timeout_pct = runtime_limits.lv2_commit_timeout_pct;
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
                            pipeline_depth,
                            commit_timeout_pct,
                        );
                    }
                })
                .map_err(|e| {
                    OnyxError::Config(format!(
                        "failed to spawn persistent slot sync thread for shard {}: {}",
                        shard_idx, e
                    ))
                })?;

            shard_ready_txs.push(shard_ready_tx_for_loop);
            shard_ready_rxs.push(shard_ready_rxs_for_pool.remove(0));
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
}
