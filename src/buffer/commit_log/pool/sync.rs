use super::*;

const POST_WRITE_VERIFY: bool = false;

impl WriteBufferPool {
    pub(super) fn sync_device_impl(device: &RawDevice) -> OnyxResult<()> {
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
        // hydration failures minutes later. Gated behind POST_WRITE_VERIFY
        // — see the const for cost.
        if POST_WRITE_VERIFY {
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
    /// write and a barrier-fdatasync. On success, both data and checkpoint are
    /// persisted before returning. Large batches are split at the ring's SQ
    /// depth so group commit can grow past `uring_sq_entries` without turning
    /// into a retry loop.
    ///
    /// The failpoint-driven test injection from `sync_device_impl` is checked
    /// after CQE harvest so existing recovery tests still cover this path.

    pub(in crate::buffer::commit_log) fn write_batch_and_sync_uring(
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
        //    so concurrent writers see consistent ordering. Data writes may
        //    exceed the ring depth; checkpoint and fsync are submitted after
        //    all data chunks complete to preserve the durability order.
        let write_start = Instant::now();
        let _guard = io_lock.lock();
        let span_count = spans.len();
        let max_ops = (ring.sq_entries() as usize).max(1);
        let mut results = Vec::with_capacity(ops.len());
        for chunk in ops[..span_count].chunks(max_ops) {
            results.extend(unsafe { ring.submit_batch(chunk)? });
        }
        if ckpt_aligned.is_some() {
            results.extend(unsafe { ring.submit_batch(&ops[span_count..span_count + 1])? });
        }
        results.extend(unsafe { ring.submit_batch(&ops[ops.len() - 1..])? });

        // 6. Validate per-op CQE results.
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
        //    via syscall reads to keep the io_uring submit path tight. Gated
        //    behind POST_WRITE_VERIFY.
        if POST_WRITE_VERIFY {
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

    pub(super) fn sync_loop(
        device: RawDevice,
        shard: Arc<BufferShard>,
        group_commit_wait: Duration,
        wake_rx: Receiver<()>,
        shutdown: Arc<AtomicBool>,
        metrics: Arc<OnceLock<Arc<EngineMetrics>>>,
        _ready_tx: Sender<u64>,
        _shard_ready_tx: Sender<u64>,
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
                if shard.staging_rx.is_empty() {
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
                }

                inflight = shard.drain_staged_limited();
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
                    // Strip cancellation flags for this batch — appenders
                    // already returned errors and the indices were rolled
                    // back via `evict_pending_entry`, so any leftover
                    // cancellation markers are stale.
                    {
                        let mut lc = shard.lifecycle.lock();
                        for entry in &inflight {
                            lc.cancelled.remove(&entry.pending.seq);
                        }
                    }
                    // Advance the LV2 fdatasync watermark and wake every
                    // parked appender whose seq is now durable. Payload
                    // caching + ready-channel publishing both moved into
                    // the appender (see `pool.append`); the sync thread no
                    // longer touches index state post-fdatasync.
                    let batch_max_durable = inflight
                        .iter()
                        .map(|entry| entry.pending.seq)
                        .max()
                        .unwrap_or(0);
                    shard.lv2_durability.advance(batch_max_durable);
                    if let Some(metrics) = metrics.get() {
                        let batch_entries = inflight.len() as u64;
                        let batch_bytes = inflight
                            .iter()
                            .map(|entry| entry.payload.len() as u64)
                            .sum::<u64>();
                        metrics.buffer_sync_batches.fetch_add(1, Ordering::Relaxed);
                        metrics
                            .buffer_sync_entries
                            .fetch_add(batch_entries, Ordering::Relaxed);
                        metrics
                            .buffer_sync_bytes
                            .fetch_add(batch_bytes, Ordering::Relaxed);
                        crate::metrics::record_counter_max(
                            &metrics.buffer_sync_entries_max,
                            batch_entries,
                        );
                        crate::metrics::record_counter_max(
                            &metrics.buffer_sync_bytes_max,
                            batch_bytes,
                        );
                        metrics
                            .buffer_sync_epochs_committed
                            .fetch_add(batch_entries, Ordering::Relaxed);
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
}
