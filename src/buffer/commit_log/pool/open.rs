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
        // A single file/blockdev root backend (the non-chunklet path). Tests and
        // the migration helpers reach the pool through these `RawDevice`-taking
        // wrappers; chunklet callers go straight to `_and_limits` with their LD
        // backend.
        Self::open_with_options_full_and_limits(
            Arc::new(device),
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
        device: Arc<dyn BlockBackend>,
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
        let decoded_superblock = GlobalSuperblock::decode(&sb_buf);
        let primary_migration_marker = LayoutMigrationMarker::decode(&sb_buf);
        let backup_migration_marker = Self::read_layout_migration_backup(device.as_ref())?;
        let migration_marker = primary_migration_marker.or_else(|| {
            backup_migration_marker.filter(|marker| {
                decoded_superblock.is_none_or(|sb| {
                    !sb.is_v3() || sb.shard_count as usize != marker.shard_count as usize
                })
            })
        });

        // Determine if we're using v3 layout (with per-shard checkpoints).
        let (use_v3, superblock) = match (decoded_superblock, migration_marker) {
            (_, Some(marker)) => {
                tracing::warn!(
                    marker_shards = marker.shard_count,
                    requested_shards = shard_count,
                    max_seq = marker.max_seq,
                    "resuming interrupted buffer shard-layout migration"
                );
                let sb = GlobalSuperblock::new(shard_count);
                Self::reinitialize_empty_layout(device.as_ref(), shard_count, marker.max_seq)?;
                (true, sb)
            }
            (Some(sb), None) if sb.shard_count as usize == shard_count && sb.is_v3() => {
                // Happy path: v3 with matching shard count.
                (true, sb)
            }
            (Some(sb), None) if sb.shard_count as usize == shard_count && !sb.is_v3() => {
                // V2 has no reserved checkpoint page, so a standalone automatic
                // upgrade cannot make its first metadata write torn-safe. Keep
                // using v2; OnyxEngine performs an explicit drained migration
                // under the durable MetaDB sequence floor when shards change.
                (false, sb)
            }
            (Some(sb), None) if sb.is_v3() => {
                // Shard count mismatch — check if clean for reinit.
                let clean_max_seq = Self::check_old_layout_empty(&device, &sb)?;
                if let Some(max_seq) = clean_max_seq {
                    tracing::info!(
                        old_shards = sb.shard_count,
                        new_shards = shard_count,
                        "buffer is clean — reinitializing with new shard layout (v3)"
                    );
                    let new_sb = GlobalSuperblock::new(shard_count);
                    Self::reinitialize_empty_layout(device.as_ref(), shard_count, max_seq)?;
                    (true, new_sb)
                } else {
                    return Err(OnyxError::Config(format!(
                        "buffer shard mismatch: disk={} config={}; unflushed entries exist",
                        sb.shard_count, shard_count
                    )));
                }
            }
            (Some(sb), None) => {
                return Err(OnyxError::Config(format!(
                    "legacy v2 buffer shard mismatch: disk={} config={}; \
                     engine-coordinated drained migration required",
                    sb.shard_count, shard_count
                )));
            }
            (None, None) => {
                // Only an all-zero metadata prefix is fresh. Treating an
                // invalid/torn superblock as first use would overwrite a valid
                // checkpoint and hide pending LV2 entries behind authoritative
                // empty recovery boundaries.
                let mut checkpoint_zero = [0u8; SHARD_CHECKPOINT_SIZE as usize];
                device.read_at(&mut checkpoint_zero, COMMIT_LOG_SUPERBLOCK_SIZE)?;
                if sb_buf.iter().any(|byte| *byte != 0)
                    || checkpoint_zero.iter().any(|byte| *byte != 0)
                {
                    return Err(OnyxError::Config(
                        "buffer superblock is invalid on a non-empty device; \
                         refusing destructive fresh initialization"
                            .into(),
                    ));
                }
                let sb = GlobalSuperblock::new(shard_count);
                Self::reinitialize_empty_layout(device.as_ref(), shard_count, 0)?;
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
        // A chunklet LD is one durability domain but exposes no single fd. Its
        // multi-shard global sync path packs all shard checkpoints into the first
        // two already-reserved v3 pages. Raw/fd-backed paths retain their legacy
        // per-shard checkpoint pages and io_uring pipeline.
        let use_global_sync_loop = shard_count > 1 && device.uring_target().is_none();
        let packed_load = if use_v3 && use_global_sync_loop {
            Some(Self::read_packed_checkpoint(device.as_ref(), shard_count)?)
        } else {
            None
        };
        let packed_generation = match packed_load.as_ref() {
            Some(PackedCheckpointLoad::Packed(table)) => table.generation,
            _ => 0,
        };
        // Round down to block_size so every shard's base_offset stays
        // block-aligned.  Without this, shards 1..N get non-aligned global
        // offsets and silently fall back to buffered IO, which on the same
        // block device as shard 0's O_DIRECT causes page-cache coherency
        // corruption (mixed O_DIRECT + buffered IO on one file).
        let bytes_per_shard = (total_data_bytes / shard_count as u64) & !(BLOCK_SIZE as u64 - 1);

        // Build per-shard config for parallel open.
        struct ShardOpenConfig {
            data_device: Arc<dyn BlockBackend>,
            checkpoint: Option<ShardCheckpoint>,
            checkpoint_device: Option<Arc<dyn BlockBackend>>,
        }

        let mut shard_configs = Vec::with_capacity(shard_count);
        let mut missing_checkpoint = false;
        let mut consumed = 0u64;
        for shard_idx in 0..shard_count {
            let shard_bytes = if shard_idx + 1 == shard_count {
                total_data_bytes.saturating_sub(consumed)
            } else {
                bytes_per_shard
            };
            let shard_offset = data_area_start + consumed;
            consumed += shard_bytes;

            let data_device = slice_backend(device.clone(), shard_offset, shard_bytes)?;
            let (checkpoint, checkpoint_device) = if use_v3 {
                let ckpt = match packed_load.as_ref() {
                    Some(PackedCheckpointLoad::Packed(table)) => Some(table.checkpoints[shard_idx]),
                    Some(PackedCheckpointLoad::Corrupt) => None,
                    Some(PackedCheckpointLoad::Legacy) | None => {
                        Self::read_shard_checkpoint(device.as_ref(), shard_idx)?
                    }
                };
                missing_checkpoint |= ckpt.is_none();
                let ckpt_offset =
                    COMMIT_LOG_SUPERBLOCK_SIZE + shard_idx as u64 * SHARD_CHECKPOINT_SIZE;
                let ckpt_dev = slice_backend(device.clone(), ckpt_offset, SHARD_CHECKPOINT_SIZE)?;
                // A valid checkpoint enables guided recovery. Invalid/corrupt
                // metadata is rejected below because mutable ring history cannot
                // prove the global sequence floor of reclaimed records.
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
        if use_v3 && missing_checkpoint {
            return Err(OnyxError::Config(
                "buffer checkpoint is corrupt; refusing to infer the sequence floor \
                 from mutable ring history"
                    .into(),
            ));
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

        // Ready channels are lossy wake hints, not the authoritative work
        // queue. Durable pending_entries + pending_seqs hold every seq until
        // apply, so bounding these channels prevents a residence window from
        // accumulating one duplicate notification per foreground append.
        // Keep a modest global compatibility/debug stream; the per-shard
        // flusher wake channel needs only one outstanding hint.
        let (ready_tx, ready_rx) = bounded(1024);
        let mut shard_ready_txs_for_open = Vec::with_capacity(shard_count);
        let mut shard_ready_rxs_for_pool = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let (tx, rx) = bounded(1);
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
        let global_shutdown = Arc::new(AtomicBool::new(false));
        let mut global_members = Vec::with_capacity(shard_count);

        // Recompute consumed for sync device slices.
        consumed = 0u64;
        for (shard_idx, result) in shard_results.into_iter().enumerate() {
            let (shard, shard_max_seq) = result?;
            shard.compact_recovered_stale_ranges();

            // Recovered entries were durable on LV2 by definition, so seed
            // the LV2 watermark with the max recovered seq before publishing
            // them — otherwise `is_seq_ready_for_flush` would gate them off
            // immediately after open.
            shard.lv2_durability.advance(
                shard
                    .lv2_durability
                    .synced_seq
                    .load(Ordering::Relaxed)
                    .max(shard_max_seq),
            );

            let shard_ready_tx_for_loop = shard_ready_txs_for_open[shard_idx].clone();
            let mut recovered_seqs: Vec<u64> = shard
                .pending_entries
                .iter()
                .map(|entry| *entry.key())
                .collect();
            recovered_seqs.sort_unstable();
            for seq in recovered_seqs {
                let _ = ready_tx.try_send(seq);
                let _ = shard_ready_tx_for_loop.try_send(seq);
            }
            max_seq = max_seq.max(shard_max_seq);

            let shard_bytes = if shard_idx + 1 == shard_count {
                total_data_bytes.saturating_sub(consumed)
            } else {
                bytes_per_shard
            };
            let shard_offset = data_area_start + consumed;
            consumed += shard_bytes;

            let sync_device = slice_backend(device.clone(), shard_offset, shard_bytes)?;
            let shard = Arc::new(shard);
            let (sync_wake_tx, sync_wake_rx) = unbounded();
            let sync_shutdown = if use_global_sync_loop {
                global_shutdown.clone()
            } else {
                Arc::new(AtomicBool::new(false))
            };
            // Per-shard io_uring session (one ring per sync thread, no
            // contention). Skipped when uring_sq_entries is None.
            let shard_uring = match uring_sq_entries {
                Some(entries) => Some(Arc::new(IoUringSession::new(entries)?)),
                None => None,
            };
            let sync_thread = if use_global_sync_loop {
                global_members.push((shard_offset, shard.clone(), sync_wake_rx));
                None
            } else {
                Some(
                    thread::Builder::new()
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
                        })?,
                )
            };

            shard_ready_txs.push(shard_ready_tx_for_loop);
            shard_ready_rxs.push(shard_ready_rxs_for_pool.remove(0));
            shards.push(BufferShardHandle {
                shard,
                sync_wake_tx,
                sync_shutdown,
                sync_thread,
            });
        }

        let packed_checkpoint = if use_v3 && use_global_sync_loop {
            let checkpoints = global_members
                .iter()
                .map(|(_, shard, _)| {
                    let mut checkpoint = shard.snapshot_checkpoint();
                    checkpoint.max_seq = shard.lv2_durability.synced_seq.load(Ordering::Acquire);
                    checkpoint
                })
                .collect();
            Some(Arc::new(parking_lot::Mutex::new(
                PackedCheckpointState::new(packed_generation, checkpoints)?,
            )))
        } else {
            None
        };

        if use_global_sync_loop {
            let root_device = device.clone();
            let shutdown = global_shutdown.clone();
            let metrics_for_loop = metrics.clone();
            let packed_for_loop = packed_checkpoint.clone();
            let sync_thread = thread::Builder::new()
                .name("persistent-slot-sync-global".into())
                .spawn(move || {
                    crate::affinity::bind_current(crate::affinity::ThreadRole::BufferSync, 0);
                    Self::global_sync_loop(
                        root_device,
                        global_members,
                        group_commit_wait,
                        runtime_limits.lv2_prepared_queue_depth_per_lane,
                        shutdown,
                        metrics_for_loop,
                        packed_for_loop,
                    );
                })
                .map_err(|e| {
                    OnyxError::Config(format!(
                        "failed to spawn global persistent slot sync thread: {}",
                        e
                    ))
                })?;
            shards[0].sync_thread = Some(sync_thread);
        }

        let disk_version = if use_v3 {
            COMMIT_LOG_VERSION
        } else {
            COMMIT_LOG_VERSION_V2
        };
        let throttle = runtime_limits.throttle.resolved();
        let throttle_states = (0..shards.len())
            .map(|_| ShardThrottleState::default())
            .collect();
        let pool = Self {
            root_device: device,
            shards,
            next_seq: AtomicU64::new(max_seq + 1),
            frontier_gate: parking_lot::RwLock::new(()),
            append_order_stripes: (0..APPEND_ORDER_STRIPES)
                .map(|_| AppendOrderStripe::default())
                .collect(),
            routing_zone_size_blocks,
            ready_rx,
            shard_ready_rxs,
            metrics,
            payload_bytes_in_memory,
            max_payload_memory,
            disk_version,
            packed_checkpoint,
            max_flushed_seq,
            durable_seq,
            throttle,
            throttle_anchor: Instant::now(),
            throttle_states,
            backend_debt_throttle_enabled: runtime_limits.throttle_backend_debt,
            backend_throttle_control: BackendThrottleControl::default(),
            meta_fence: OnceLock::new(),
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
