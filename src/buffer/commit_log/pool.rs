use super::*;

impl WriteBufferPool {
    /// V2 data bytes: device - superblock.
    fn total_data_bytes(device_size: u64) -> OnyxResult<u64> {
        device_size
            .checked_sub(COMMIT_LOG_SUPERBLOCK_SIZE)
            .ok_or_else(|| OnyxError::Config("persistent slot device too small".into()))
    }

    /// V3 data bytes: device - superblock - per-shard checkpoint blocks.
    fn total_data_bytes_v3(device_size: u64, shard_count: usize) -> OnyxResult<u64> {
        let overhead = COMMIT_LOG_SUPERBLOCK_SIZE + shard_count as u64 * SHARD_CHECKPOINT_SIZE;
        device_size
            .checked_sub(overhead)
            .ok_or_else(|| OnyxError::Config("persistent slot device too small".into()))
    }

    /// Offset where shard data areas begin in v3 layout.
    fn v3_data_area_start(shard_count: usize) -> u64 {
        COMMIT_LOG_SUPERBLOCK_SIZE + shard_count as u64 * SHARD_CHECKPOINT_SIZE
    }

    /// Read the on-disk shard count from the superblock. Returns None if the
    /// device has no valid superblock (first use).
    pub fn read_disk_shard_count(device: &RawDevice) -> OnyxResult<Option<usize>> {
        let mut buf = [0u8; COMMIT_LOG_SUPERBLOCK_SIZE as usize];
        device.read_at(&mut buf, 0)?;
        Ok(GlobalSuperblock::decode(&buf).map(|sb| sb.shard_count as usize))
    }

    fn validate_shard_count(shard_count: usize) -> OnyxResult<()> {
        if shard_count == 0 || shard_count > MAX_SHARDS_ON_DISK {
            return Err(OnyxError::Config(format!(
                "persistent slot buffer supports 1..{} shards, got {}",
                MAX_SHARDS_ON_DISK, shard_count
            )));
        }
        Ok(())
    }

    /// Read shard checkpoint from disk. Returns None if invalid.
    fn read_shard_checkpoint(
        device: &RawDevice,
        shard_idx: usize,
    ) -> OnyxResult<Option<ShardCheckpoint>> {
        let offset = COMMIT_LOG_SUPERBLOCK_SIZE + shard_idx as u64 * SHARD_CHECKPOINT_SIZE;
        let mut buf = [0u8; SHARD_CHECKPOINT_SIZE as usize];
        device.read_at(&mut buf, offset)?;
        Ok(ShardCheckpoint::decode(&buf))
    }

    /// Initialize checkpoint blocks to zero (used during v2→v3 migration).
    fn init_checkpoint_blocks(device: &RawDevice, shard_count: usize) -> OnyxResult<()> {
        let empty = ShardCheckpoint {
            head_offset: 0,
            tail_offset: 0,
            max_seq: 0,
            used_bytes: 0,
        };
        let encoded = empty.encode();
        for i in 0..shard_count {
            let offset = COMMIT_LOG_SUPERBLOCK_SIZE + i as u64 * SHARD_CHECKPOINT_SIZE;
            device.write_at(&encoded, offset)?;
        }
        Ok(())
    }

    /// Check whether the buffer device with an old shard layout has zero
    /// unflushed entries, meaning it is safe to reinitialize with a different
    /// shard count (or migrate to v3).
    fn check_old_layout_empty(device: &RawDevice, sb: &GlobalSuperblock) -> OnyxResult<bool> {
        let old_shards = sb.shard_count as usize;
        let device_size = device.size();
        let total_data = if sb.is_v3() {
            Self::total_data_bytes_v3(device_size, old_shards)?
        } else {
            Self::total_data_bytes(device_size)?
        };
        let bytes_per_shard = (total_data / old_shards as u64) & !(BLOCK_SIZE as u64 - 1);
        let data_area_start = if sb.is_v3() {
            Self::v3_data_area_start(old_shards)
        } else {
            COMMIT_LOG_SUPERBLOCK_SIZE
        };

        let mut consumed = 0u64;
        for i in 0..old_shards {
            let shard_bytes = if i == old_shards - 1 {
                total_data - consumed
            } else {
                bytes_per_shard
            };
            let shard_offset = data_area_start + consumed;
            consumed += shard_bytes;

            let shard_dev = device.slice(shard_offset, shard_bytes)?;
            let lba_index = DashMap::with_shard_amount(4);
            let latest_lba_seq = DashMap::with_shard_amount(4);
            let pending = DashMap::with_shard_amount(4);
            BufferShard::rebuild_indices(
                &shard_dev,
                shard_bytes,
                &lba_index,
                &latest_lba_seq,
                &pending,
            )?;

            if !pending.is_empty() {
                tracing::warn!(
                    shard = i,
                    pending = pending.len(),
                    "buffer shard has unflushed entries — cannot reinit"
                );
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn persist_superblock(&self, sync: bool) -> OnyxResult<()> {
        let sb = GlobalSuperblock {
            shard_count: self.shards.len() as u32,
            version: self.disk_version,
        };
        let bytes = sb.encode();
        self.root_device.write_at(&bytes, 0)?;
        if sync {
            Self::sync_device_impl(&self.root_device)?;
        }
        Ok(())
    }

    fn sync_device_impl(device: &RawDevice) -> OnyxResult<()> {
        Self::consume_test_sync_failpoint()?;
        device.sync()
    }

    /// Pull one hit off the failpoint counter; returns Err if it was armed.
    /// Both the syscall and io_uring sync paths funnel through here so test
    /// failure injection still drives both.
    fn consume_test_sync_failpoint() -> OnyxResult<()> {
        let mut remaining_failures = test_sync_fail_remaining().lock().unwrap();
        if *remaining_failures > 0 {
            *remaining_failures -= 1;
            return Err(OnyxError::Io(std::io::Error::other(
                "injected persistent slot sync failure",
            )));
        }
        Ok(())
    }

    fn sync_retry_backoff(consecutive_failures: u32) -> Duration {
        let shift = consecutive_failures.saturating_sub(1).min(4);
        Duration::from_millis((1u64 << shift).min(16))
    }

    fn write_batch(
        device: &RawDevice,
        io_lock: &parking_lot::Mutex<()>,
        entries: &[StagedEntry],
        metrics: &Arc<OnceLock<Arc<EngineMetrics>>>,
    ) -> OnyxResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut encoded: Vec<(u64, Vec<u8>)> = Vec::with_capacity(entries.len());
        for entry in entries {
            let pending = &entry.pending;
            let payload = &entry.payload;
            let payload_crc = crc32fast::hash(payload);
            let data = BufferEntry::encode(
                pending.seq,
                &pending.vol_id,
                pending.start_lba,
                pending.lba_count,
                payload_crc,
                false,
                pending.vol_created_at,
                payload,
            )?;
            encoded.push((pending.disk_offset, data));
        }

        let write_start = Instant::now();
        let _guard = io_lock.lock();
        let mut start = 0usize;
        while start < encoded.len() {
            let mut end = start + 1;
            let mut next_offset = encoded[start].0 + encoded[start].1.len() as u64;
            while end < encoded.len() && encoded[end].0 == next_offset {
                next_offset += encoded[end].1.len() as u64;
                end += 1;
            }
            let span = &encoded[start..end];
            let total_len: usize = span.iter().map(|(_, data)| data.len()).sum();
            let mut batch = AlignedBuf::new(total_len, false)?;
            let mut cursor = 0;
            for (_, data) in span {
                batch.as_mut_slice()[cursor..cursor + data.len()].copy_from_slice(data);
                cursor += data.len();
            }
            device.write_at(&batch.as_slice()[..total_len], span[0].0)?;
            start = end;
        }
        if let Some(metrics) = metrics.get() {
            BufferShard::record_metric(&metrics.buffer_append_log_write_ns, write_start);
        }

        // Post-write verification: read back the first block of each entry
        // and check the magic number.  Catches silent write failures and
        // DMA ordering issues that would otherwise surface as mysterious
        // hydration failures minutes later.
        {
            use crate::buffer::entry::BUFFER_ENTRY_MAGIC;
            let mut verify_buf = vec![0u8; BLOCK_SIZE as usize];
            for (offset, data) in &encoded {
                if data.len() < 8 {
                    continue;
                }
                if let Err(e) = device.read_at(&mut verify_buf, *offset) {
                    tracing::error!(
                        offset,
                        error = %e,
                        "post-write read-back failed"
                    );
                    continue;
                }
                let magic = u32::from_le_bytes(verify_buf[4..8].try_into().unwrap());
                if magic != BUFFER_ENTRY_MAGIC {
                    let expected_magic_bytes = &data[4..8];
                    let disk_first_16: Vec<u8> = verify_buf[..16].to_vec();
                    let encoded_first_16: Vec<u8> = data[..16.min(data.len())].to_vec();
                    let write_base = device.base_offset();
                    let write_direct_io = device.is_direct_io();
                    tracing::error!(
                        offset,
                        data_len = data.len(),
                        disk_magic = magic,
                        expected_magic = u32::from_le_bytes(
                            expected_magic_bytes.try_into().unwrap()
                        ),
                        write_base,
                        write_global = write_base + offset,
                        write_direct_io,
                        disk_first_16 = ?disk_first_16,
                        encoded_first_16 = ?encoded_first_16,
                        "POST-WRITE VERIFICATION FAILED: entry not on disk after write_at"
                    );
                }
            }
        }

        Ok(())
    }

    /// io_uring variant of `write_batch` that also includes the checkpoint
    /// write and a barrier-fdatasync in the same `submit_batch` call. On
    /// success, both data and checkpoint are persisted with one
    /// `io_uring_enter` + one `wait_for_completions(N+2)`.
    ///
    /// The failpoint-driven test injection from `sync_device_impl` is checked
    /// after CQE harvest so existing recovery tests still cover this path.
    fn write_batch_and_sync_uring(
        device: &RawDevice,
        shard: &BufferShard,
        ring: &Arc<IoUringSession>,
        io_lock: &parking_lot::Mutex<()>,
        entries: &[StagedEntry],
        batch_max_seq: u64,
        metrics: &Arc<OnceLock<Arc<EngineMetrics>>>,
    ) -> OnyxResult<()> {
        if entries.is_empty() {
            // No entries → nothing to fsync either; mirrors syscall fast-path.
            return Ok(());
        }

        // 1. Encode entries (identical to write_batch).
        let mut encoded: Vec<(u64, Vec<u8>)> = Vec::with_capacity(entries.len());
        for entry in entries {
            let pending = &entry.pending;
            let payload = &entry.payload;
            let payload_crc = crc32fast::hash(payload);
            let data = BufferEntry::encode(
                pending.seq,
                &pending.vol_id,
                pending.start_lba,
                pending.lba_count,
                payload_crc,
                false,
                pending.vol_created_at,
                payload,
            )?;
            encoded.push((pending.disk_offset, data));
        }

        // 2. Coalesce contiguous entries into one AlignedBuf each, ready for
        //    a single SQE per coalesced span.
        struct CoalescedSpan {
            buf: AlignedBuf,
            offset: u64,
            len: u32,
        }
        let mut spans: Vec<CoalescedSpan> = Vec::new();
        let mut start = 0usize;
        while start < encoded.len() {
            let mut end = start + 1;
            let mut next_offset = encoded[start].0 + encoded[start].1.len() as u64;
            while end < encoded.len() && encoded[end].0 == next_offset {
                next_offset += encoded[end].1.len() as u64;
                end += 1;
            }
            let span = &encoded[start..end];
            let total_len: usize = span.iter().map(|(_, data)| data.len()).sum();
            let mut batch = AlignedBuf::new(total_len, false)?;
            let mut cursor = 0;
            for (_, data) in span {
                batch.as_mut_slice()[cursor..cursor + data.len()].copy_from_slice(data);
                cursor += data.len();
            }
            spans.push(CoalescedSpan {
                buf: batch,
                offset: span[0].0,
                len: total_len as u32,
            });
            start = end;
        }

        // 3. Optional checkpoint payload (only when the shard has a checkpoint
        //    device — same condition as `write_checkpoint`).
        let checkpoint_payload = shard.encode_checkpoint_for_uring(batch_max_seq);
        let checkpoint_target = shard.checkpoint_target();
        let mut ckpt_aligned: Option<AlignedBuf> = None;
        if let (Some(payload), Some(_)) = (&checkpoint_payload, checkpoint_target) {
            let mut buf = AlignedBuf::new(BLOCK_SIZE as usize, false)?;
            buf.as_mut_slice()[..payload.len()].copy_from_slice(payload);
            ckpt_aligned = Some(buf);
        }

        let data_fd = device.as_raw_fd();
        let data_base = device.base_offset();

        // 4. Build the SQE batch: writes → checkpoint → barrier fsync.
        let mut ops: Vec<UringOp> = Vec::with_capacity(spans.len() + 2);
        for span in &spans {
            ops.push(UringOp::Write {
                fd: data_fd,
                ptr: span.buf.as_ptr(),
                len: span.len,
                offset: data_base + span.offset,
            });
        }
        if let (Some(buf), Some((ckpt_fd, ckpt_base))) = (ckpt_aligned.as_ref(), checkpoint_target)
        {
            ops.push(UringOp::Write {
                fd: ckpt_fd,
                ptr: buf.as_ptr(),
                len: BLOCK_SIZE,
                offset: ckpt_base,
            });
        }
        ops.push(UringOp::FsyncDataBarrier { fd: data_fd });

        // 5. Submit + wait under the same io_lock that the syscall path uses,
        //    so concurrent writers see consistent ordering.
        let write_start = Instant::now();
        let _guard = io_lock.lock();
        let results = unsafe { ring.submit_batch(&ops)? };

        // 6. Validate per-op CQE results.
        let span_count = spans.len();
        for (i, span) in spans.iter().enumerate() {
            let r = &results[i];
            if let Some(errno) = r.errno() {
                return Err(OnyxError::Io(std::io::Error::other(format!(
                    "io_uring entry write failed at offset={} errno={}",
                    span.offset, errno
                ))));
            }
            let bytes = r.bytes().unwrap_or(0);
            if bytes != span.len {
                return Err(OnyxError::Io(std::io::Error::other(format!(
                    "io_uring short entry write at offset={}: got {} of {}",
                    span.offset, bytes, span.len
                ))));
            }
        }
        let mut next_idx = span_count;
        if ckpt_aligned.is_some() {
            let r = &results[next_idx];
            next_idx += 1;
            if let Some(errno) = r.errno() {
                tracing::debug!(errno, "io_uring checkpoint write failed (non-fatal)");
            }
        }
        // Final SQE is the fsync barrier.
        let fsync_r = &results[next_idx];
        if let Some(errno) = fsync_r.errno() {
            return Err(OnyxError::Io(std::io::Error::other(format!(
                "io_uring fdatasync failed: errno={errno}"
            ))));
        }

        if let Some(metrics) = metrics.get() {
            BufferShard::record_metric(&metrics.buffer_append_log_write_ns, write_start);
        }

        // 7. Honour the test failpoint AFTER successful CQE harvest so existing
        //    recovery tests cover the io_uring path too.
        Self::consume_test_sync_failpoint()?;

        // 8. Post-write verification — same magic check as `write_batch`. Done
        //    via syscall reads to keep the io_uring submit path tight.
        {
            use crate::buffer::entry::BUFFER_ENTRY_MAGIC;
            let mut verify_buf = vec![0u8; BLOCK_SIZE as usize];
            for (offset, data) in &encoded {
                if data.len() < 8 {
                    continue;
                }
                if let Err(e) = device.read_at(&mut verify_buf, *offset) {
                    tracing::error!(
                        offset,
                        error = %e,
                        "io_uring post-write read-back failed"
                    );
                    continue;
                }
                let magic = u32::from_le_bytes(verify_buf[4..8].try_into().unwrap());
                if magic != BUFFER_ENTRY_MAGIC {
                    let expected_magic_bytes = &data[4..8];
                    tracing::error!(
                        offset,
                        data_len = data.len(),
                        disk_magic = magic,
                        expected_magic =
                            u32::from_le_bytes(expected_magic_bytes.try_into().unwrap()),
                        "POST-WRITE VERIFICATION FAILED (io_uring path): entry not on disk"
                    );
                }
            }
        }

        Ok(())
    }

    fn sync_loop(
        device: RawDevice,
        shard: Arc<BufferShard>,
        group_commit_wait: Duration,
        wake_rx: Receiver<()>,
        shutdown: Arc<AtomicBool>,
        metrics: Arc<OnceLock<Arc<EngineMetrics>>>,
        ready_tx: Sender<u64>,
        shard_ready_tx: Sender<u64>,
        uring: Option<Arc<IoUringSession>>,
    ) {
        let mut consecutive_failures = 0u32;
        let mut retry_after: Option<Instant> = None;
        let mut inflight: Vec<StagedEntry> = Vec::new();
        let mut writes_applied = false;
        let batch_wait = if group_commit_wait.is_zero() {
            Duration::from_millis(1)
        } else {
            group_commit_wait
        };

        loop {
            if inflight.is_empty() {
                match wake_rx.recv_timeout(Duration::from_millis(50)) {
                    Ok(()) => {}
                    Err(RecvTimeoutError::Timeout) => {
                        if shutdown.load(Ordering::Relaxed) && shard.staging_rx.is_empty() {
                            return;
                        }
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        if shutdown.load(Ordering::Relaxed) && shard.staging_rx.is_empty() {
                            return;
                        }
                        continue;
                    }
                }
                while wake_rx.try_recv().is_ok() {}
                if !batch_wait.is_zero() {
                    let sleep_start = Instant::now();
                    thread::sleep(batch_wait);
                    if let Some(metrics) = metrics.get() {
                        BufferShard::record_metric(&metrics.buffer_sync_sleep_ns, sleep_start);
                    }
                    while wake_rx.try_recv().is_ok() {}
                }

                inflight = shard.drain_staged();
                if inflight.is_empty() {
                    if shutdown.load(Ordering::Relaxed) && shard.staging_rx.is_empty() {
                        return;
                    }
                    continue;
                }
                writes_applied = false;
            }

            if let Some(deadline) = retry_after {
                let now = Instant::now();
                if deadline > now {
                    let wait = deadline.duration_since(now).min(Duration::from_millis(10));
                    let _ = wake_rx.recv_timeout(wait);
                    continue;
                }
            }

            let batch_start = Instant::now();
            if !writes_applied {
                let (writes_to_persist, cancelled_in_batch): (Vec<StagedEntry>, Vec<u64>) = {
                    let lc = shard.lifecycle.lock();
                    let mut persist = Vec::with_capacity(inflight.len());
                    let mut cancelled = Vec::new();
                    for entry in &inflight {
                        if lc.cancelled.contains(&entry.pending.seq) {
                            cancelled.push(entry.pending.seq);
                        } else {
                            persist.push(entry.clone());
                        }
                    }
                    (persist, cancelled)
                };
                if !cancelled_in_batch.is_empty() {
                    tracing::warn!(
                        cancelled_count = cancelled_in_batch.len(),
                        total_inflight = inflight.len(),
                        persisted_count = writes_to_persist.len(),
                        first_cancelled_seq = cancelled_in_batch[0],
                        "sync batch has cancelled entries — these will NOT be written to disk"
                    );
                }
                let batch_max_seq_pre = writes_to_persist
                    .iter()
                    .map(|e| e.pending.seq)
                    .max()
                    .unwrap_or(0);

                let result = if let Some(ref ring) = uring {
                    // Batched io_uring path: N entry writes + 1 checkpoint write
                    // + 1 DRAIN-flagged fdatasync — all in one submit.
                    Self::write_batch_and_sync_uring(
                        &device,
                        &shard,
                        ring,
                        &shard.io_lock,
                        &writes_to_persist,
                        batch_max_seq_pre,
                        &metrics,
                    )
                } else {
                    Self::write_batch(&device, &shard.io_lock, &writes_to_persist, &metrics)
                };

                match result {
                    Ok(()) => {
                        writes_applied = true;
                    }
                    Err(err) => {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        retry_after =
                            Some(Instant::now() + Self::sync_retry_backoff(consecutive_failures));
                        tracing::warn!(
                            error = %err,
                            consecutive_failures,
                            "persistent slot batch write failed; retrying"
                        );
                        continue;
                    }
                }
            }

            // Persist the checkpoint hint before fdatasync so the same sync
            // makes both the batch payload and the updated recovery head/tail
            // durable. This keeps crash-restart recovery on the fast guided
            // path without adding an extra sync to the hot path.
            let batch_max_seq = inflight
                .iter()
                .map(|entry| entry.pending.seq)
                .max()
                .unwrap_or(0);

            // The uring path already checkpointed + fsynced inside
            // write_batch_and_sync_uring. The syscall path still needs both.
            let sync_result = if uring.is_some() {
                Ok(())
            } else {
                shard.write_checkpoint(batch_max_seq);
                Self::sync_device_impl(&device)
            };

            match sync_result {
                Ok(()) => {
                    consecutive_failures = 0;
                    retry_after = None;
                    let inflight_pending: Vec<Arc<PendingEntry>> =
                        inflight.iter().map(|entry| entry.pending.clone()).collect();
                    shard.retire_superseded_by_durable_entries(&inflight_pending);
                    {
                        let mut lc = shard.lifecycle.lock();
                        for entry in &inflight {
                            let seq = entry.pending.seq;
                            lc.inflight.remove(&seq);
                            shard.remove_volatile_payload(seq);
                            if lc.cancelled.remove(&seq) {
                                continue;
                            }
                            let _ = ready_tx.send(seq);
                            let _ = shard_ready_tx.send(seq);
                        }
                    }
                    if let Some(metrics) = metrics.get() {
                        metrics.buffer_sync_batches.fetch_add(1, Ordering::Relaxed);
                        metrics
                            .buffer_sync_epochs_committed
                            .fetch_add(inflight.len() as u64, Ordering::Relaxed);
                    }
                    inflight.clear();
                    writes_applied = false;

                    // Safety-net: periodically purge stale entries from
                    // cancelled that outlived their corresponding inflight
                    // seq. Use pending_entries as ground truth: if a seq is
                    // no longer pending, it has been fully flushed and the
                    // cancelled entry is stale.  Only sweep when cancelled
                    // grows past a threshold to amortise the DashMap lookups.
                    {
                        let lc = shard.lifecycle.lock();
                        if lc.cancelled.len() > 256 {
                            let stale: Vec<u64> = lc
                                .cancelled
                                .iter()
                                .filter(|seq| !shard.pending_entries.contains_key(seq))
                                .copied()
                                .collect();
                            drop(lc);
                            if !stale.is_empty() {
                                let mut lc = shard.lifecycle.lock();
                                for seq in &stale {
                                    lc.cancelled.remove(seq);
                                }
                            }
                        }
                    }
                }
                Err(err) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    retry_after =
                        Some(Instant::now() + Self::sync_retry_backoff(consecutive_failures));
                    tracing::warn!(
                        error = %err,
                        consecutive_failures,
                        "persistent slot sync failed; retrying"
                    );
                }
            }

            if let Some(metrics) = metrics.get() {
                BufferShard::record_metric(&metrics.buffer_sync_batch_ns, batch_start);
            }
        }
    }

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
        let shard_results: Vec<OnyxResult<(BufferShard, u64)>> = if shard_count > 1 {
            std::thread::scope(|s| {
                let handles: Vec<_> = shard_configs
                    .into_iter()
                    .map(|cfg| {
                        let m = metrics.clone();
                        let pb = payload_bytes_in_memory.clone();
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

    pub fn append(
        &self,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        payload: &[u8],
        vol_created_at: u64,
    ) -> OnyxResult<u64> {
        let total_start = Instant::now();
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

    pub fn pending_entry(&self, seq: u64) -> Option<BufferEntry> {
        self.shard_for_seq(seq)
            .and_then(|idx| self.shards[idx].shard.pending_entry(seq))
    }

    pub fn pending_entry_arc(&self, seq: u64) -> Option<Arc<PendingEntry>> {
        self.shard_for_seq(seq)
            .and_then(|idx| self.shards[idx].shard.pending_entry_arc_hydrated(seq))
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

    pub fn head_stuck_pending_entry_arc_for_shard(
        &self,
        shard_idx: usize,
        min_age: Duration,
    ) -> Option<Arc<PendingEntry>> {
        self.shards.get(shard_idx).and_then(|shard| {
            shard
                .shard
                .head_pending_entry_arc_hydrated_if_stuck(min_age)
        })
    }

    pub fn flushed_offsets_for_shard(&self, shard_idx: usize, seq: u64) -> Option<HashSet<u16>> {
        self.shards
            .get(shard_idx)
            .and_then(|shard| shard.shard.flushed_offsets_snapshot(seq))
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

    /// Configured in-memory payload ceiling. 0 means "no limit".
    pub fn payload_memory_limit_bytes(&self) -> u64 {
        self.max_payload_memory
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
