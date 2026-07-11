use super::*;
use std::collections::VecDeque;
use std::os::fd::RawFd;

const POST_WRITE_VERIFY: bool = false;

/// ZFS `zil_commit_waiter` floor: even with a cold/zero EMA (or a pathologically
/// fast device) the OPEN batch accumulates at least this long before sealing, so
/// the loop never busy-seals empty/tiny batches. The adaptive close window is
/// `max(ema_write_latency * commit_timeout_pct/100, this)`.
const LV2_COMMIT_TIMEOUT_FLOOR: Duration = Duration::from_micros(25);

/// One coalesced, contiguous run of staged entries encoded into a single
/// pooled `AlignedBuf`, ready for one pwrite (syscall path) or one write
/// SQE (io_uring path). `offset` is the LV2-relative byte offset of the
/// first entry; `len` is the exact number of valid bytes (the buffer may
/// be rounded up larger by the allocator).
struct CoalescedSpan {
    buf: AlignedBuf,
    offset: u64,
    len: u32,
}

/// One LV2 fdatasync chain in flight in the pipelined sync path. Holds the
/// encoded span buffers (and optional checkpoint buffer) alive until the
/// kernel harvests every CQE for this batch's chain — the raw pointers in the
/// submitted SQEs reference these allocations. `inflight_all` is the FULL
/// drained batch (including cancelled entries) used for post-fsync retire /
/// cancel-strip / watermark advance, exactly as the serial path does.
///
/// SQE layout (so a harvested `op_idx` maps to a role): data writes at
/// `0..write_count` (IO_LINK-chained), the terminal `FsyncData` at
/// `write_count`, the optional unlinked checkpoint write at `write_count + 1`.
struct InflightUringBatch {
    batch_id: u64,
    spans: Vec<CoalescedSpan>,
    /// Kept alive (not read) so the checkpoint write SQE's pointer stays valid.
    _ckpt_buf: Option<AlignedBuf>,
    inflight_all: Vec<StagedEntry>,
    max_seq: u64,
    write_count: usize,
    has_ckpt: bool,
    expected_cqes: usize,
    seen_cqes: usize,
    failed: bool,
    write_start: Instant,
}

/// A not-yet-sealed accumulation of staged entries — the ZFS "OPENED lwb"
/// analog. Entries are drained from `staging_rx` and held here across loop
/// iterations while prior batches' writes are in flight, so the accumulation
/// window OVERLAPS the in-flight fdatasync (≈free) and grows the batch under
/// load. Closed (sealed into an `InflightUringBatch`) on full-or-adaptive-
/// timeout. Holds raw entries only; encoding/checkpoint happen at seal time.
struct OpenBatch {
    entries: Vec<StagedEntry>,
    bytes: usize,
    opened_at: Instant,
}

impl WriteBufferPool {
    /// Encode each staged entry directly into a pooled `AlignedBuf`,
    /// coalescing entries whose reserved disk ranges are contiguous into a
    /// single buffer. This replaces the old "encode into a fresh
    /// `vec![0u8; n]` per entry, then memcpy into an AlignedBuf" two-pass:
    /// the per-entry Vec was a jemalloc large-class allocation, so freeing
    /// it drove `madvise(MADV_DONTNEED)` → cross-core TLB-shootdown IPIs on
    /// the LV2 sync thread (perf 2026-05-29: ~10% aggregate on-CPU, smeared
    /// across all cores). Encoding straight into the span buffer's
    /// sub-slice via `encode_full_into_slice` removes that allocation, the
    /// extra memcpy, and the redundant zero-fill. The single AlignedBuf per
    /// span hits the thread-local pool ([`AlignedBuf::new`]) and stays
    /// resident, so no madvise churn remains.
    fn encode_entries_into_spans(entries: &[StagedEntry]) -> OnyxResult<Vec<CoalescedSpan>> {
        let mut spans: Vec<CoalescedSpan> = Vec::new();
        let mut start = 0usize;
        while start < entries.len() {
            let mut end = start + 1;
            let mut next_offset =
                entries[start].pending.disk_offset + entries[start].pending.disk_len as u64;
            while end < entries.len() && entries[end].pending.disk_offset == next_offset {
                next_offset += entries[end].pending.disk_len as u64;
                end += 1;
            }
            let span = &entries[start..end];
            let total_len: usize = span.iter().map(|e| e.pending.disk_len as usize).sum();
            let mut buf = AlignedBuf::new(total_len, false)?;
            {
                let dst = buf.as_mut_slice();
                let mut cursor = 0usize;
                for entry in span {
                    let pending = &entry.pending;
                    let payload = &entry.payload;
                    let disk_len = pending.disk_len as usize;
                    debug_assert_eq!(
                        disk_len,
                        crate::io::aligned::round_up(
                            BufferEntry::raw_size_for(&pending.vol_id, payload.len()),
                            BLOCK_SIZE as usize
                        ),
                        "disk_len must equal the rounded encoded entry size"
                    );
                    BufferEntry::encode_full_into_slice(
                        pending.seq,
                        &pending.vol_id,
                        pending.start_lba,
                        pending.lba_count,
                        pending.payload_crc32,
                        false,
                        pending.vol_created_at,
                        payload,
                        pending.disk_len,
                        &mut dst[cursor..cursor + disk_len],
                    )?;
                    cursor += disk_len;
                }
            }
            spans.push(CoalescedSpan {
                buf,
                offset: span[0].pending.disk_offset,
                len: total_len as u32,
            });
            start = end;
        }
        Ok(spans)
    }

    pub(super) fn sync_device_impl(device: &dyn BlockBackend) -> OnyxResult<()> {
        Self::consume_test_sync_failpoint()?;
        device.flush()
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
        device: &dyn BlockBackend,
        io_lock: &parking_lot::Mutex<()>,
        entries: &[StagedEntry],
        metrics: &Arc<OnceLock<Arc<EngineMetrics>>>,
    ) -> OnyxResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let spans = Self::encode_entries_into_spans(entries)?;

        let write_start = Instant::now();
        let _guard = io_lock.lock();
        // One batched submit for the whole coalesced run. On a chunklet LD this
        // fans the spans across the RAID member PDs in a single submit (the
        // RAID10 LV2 win); on a `RawDevice` it loops pwrite internally — same
        // result as the old per-span `write_at`. Durability still requires the
        // following `flush` (see the sync_loop's syscall branch).
        let ops: Vec<(u64, &[u8])> = spans
            .iter()
            .map(|s| (s.offset, &s.buf.as_slice()[..s.len as usize]))
            .collect();
        device.write_many_at(&ops)?;
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
            for entry in entries {
                let offset = entry.pending.disk_offset;
                if let Err(e) = device.read_at(&mut verify_buf, offset) {
                    tracing::error!(
                        offset,
                        error = %e,
                        "post-write read-back failed"
                    );
                    continue;
                }
                let magic = u32::from_le_bytes(verify_buf[4..8].try_into().unwrap());
                if magic != BUFFER_ENTRY_MAGIC {
                    let disk_first_16: Vec<u8> = verify_buf[..16].to_vec();
                    let write_base = device.uring_target().map(|(_, b)| b).unwrap_or(0);
                    let write_direct_io = device.direct_io();
                    tracing::error!(
                        offset,
                        disk_magic = magic,
                        expected_magic = BUFFER_ENTRY_MAGIC,
                        write_base,
                        write_global = write_base + offset,
                        write_direct_io,
                        disk_first_16 = ?disk_first_16,
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
        device: &dyn BlockBackend,
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

        // This path only runs for a single-fd backend (`RawDevice`); the
        // sync_loop dispatch guarantees a uring target exists here.
        let (data_fd, data_base) = device.uring_target().ok_or_else(|| {
            OnyxError::Io(std::io::Error::other(
                "io_uring LV2 sync path requires a fd-backed device",
            ))
        })?;

        // 1 + 2. Encode each entry directly into a pooled AlignedBuf,
        //    coalescing contiguous reserved ranges into one buffer per span
        //    (one write SQE each). See `encode_entries_into_spans` for why
        //    this avoids the per-entry Vec / madvise-TLB-IPI churn.
        let spans = Self::encode_entries_into_spans(entries)?;

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

        let span_count = spans.len();
        let has_ckpt = ckpt_aligned.is_some() && checkpoint_target.is_some();

        // Shared validation of the data-write CQEs (indices 0..span_count in
        // both the fast and legacy result layouts).
        let validate_span_writes = |results: &[UringOpResult]| -> OnyxResult<()> {
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
            Ok(())
        };

        // Fast path SQE budget: N data writes + 1 fsync + optional checkpoint.
        let fast_path_ops = span_count + 1 + usize::from(has_ckpt);
        let write_start = Instant::now();

        if fast_path_ops as u32 <= ring.sq_entries() {
            // ── Fast path: ONE submit ────────────────────────────────────
            // IO_LINK-chain the data writes into a terminal plain FsyncData,
            // so the fsync waits for exactly this batch's writes (no whole-ring
            // IO_DRAIN). The checkpoint write is appended UNLINKED — it runs
            // concurrently and its durability is best-effort (a recovery hint
            // re-covered by the next batch's device-wide flush). Linking it
            // would let a checkpoint failure -ECANCELED the fsync.
            let mut linked: Vec<LinkedOp> = Vec::with_capacity(fast_path_ops);
            for span in &spans {
                linked.push(LinkedOp {
                    op: UringOp::Write {
                        fd: data_fd,
                        ptr: span.buf.as_ptr(),
                        len: span.len,
                        offset: data_base + span.offset,
                    },
                    link_next: true,
                });
            }
            linked.push(LinkedOp {
                op: UringOp::FsyncData { fd: data_fd },
                link_next: false,
            });
            if let (Some(buf), Some((ckpt_fd, ckpt_base))) =
                (ckpt_aligned.as_ref(), checkpoint_target)
            {
                linked.push(LinkedOp {
                    op: UringOp::Write {
                        fd: ckpt_fd,
                        ptr: buf.as_ptr(),
                        len: BLOCK_SIZE,
                        offset: ckpt_base,
                    },
                    link_next: false,
                });
            }

            let results = {
                let _guard = io_lock.lock();
                unsafe { ring.submit_linked_wait(&linked)? }
            };

            validate_span_writes(&results)?;
            // fsync is at span_count.
            if let Some(errno) = results[span_count].errno() {
                return Err(OnyxError::Io(std::io::Error::other(format!(
                    "io_uring fdatasync failed: errno={errno}"
                ))));
            }
            // checkpoint (best-effort) is at span_count + 1.
            if has_ckpt {
                if let Some(errno) = results[span_count + 1].errno() {
                    tracing::debug!(errno, "io_uring checkpoint write failed (non-fatal)");
                }
            }
        } else {
            // ── Legacy path: chain length exceeds the SQ ring ────────────
            // Submit data writes in sq-sized chunks (each waited), then the
            // checkpoint, then a DRAIN fsync — preserving durability order
            // across multiple submits so group commit can grow past ring depth.
            let mut ops: Vec<UringOp> = Vec::with_capacity(spans.len() + 2);
            for span in &spans {
                ops.push(UringOp::Write {
                    fd: data_fd,
                    ptr: span.buf.as_ptr(),
                    len: span.len,
                    offset: data_base + span.offset,
                });
            }
            if let (Some(buf), Some((ckpt_fd, ckpt_base))) =
                (ckpt_aligned.as_ref(), checkpoint_target)
            {
                ops.push(UringOp::Write {
                    fd: ckpt_fd,
                    ptr: buf.as_ptr(),
                    len: BLOCK_SIZE,
                    offset: ckpt_base,
                });
            }
            ops.push(UringOp::FsyncDataBarrier { fd: data_fd });

            let _guard = io_lock.lock();
            let max_ops = (ring.sq_entries() as usize).max(1);
            let mut results = Vec::with_capacity(ops.len());
            for chunk in ops[..span_count].chunks(max_ops) {
                results.extend(unsafe { ring.submit_batch(chunk)? });
            }
            if has_ckpt {
                results.extend(unsafe { ring.submit_batch(&ops[span_count..span_count + 1])? });
            }
            results.extend(unsafe { ring.submit_batch(&ops[ops.len() - 1..])? });

            validate_span_writes(&results)?;
            let mut next_idx = span_count;
            if has_ckpt {
                if let Some(errno) = results[next_idx].errno() {
                    tracing::debug!(errno, "io_uring checkpoint write failed (non-fatal)");
                }
                next_idx += 1;
            }
            // Final SQE is the fsync barrier.
            if let Some(errno) = results[next_idx].errno() {
                return Err(OnyxError::Io(std::io::Error::other(format!(
                    "io_uring fdatasync failed: errno={errno}"
                ))));
            }
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
            for entry in entries {
                let offset = entry.pending.disk_offset;
                if let Err(e) = device.read_at(&mut verify_buf, offset) {
                    tracing::error!(
                        offset,
                        error = %e,
                        "io_uring post-write read-back failed"
                    );
                    continue;
                }
                let magic = u32::from_le_bytes(verify_buf[4..8].try_into().unwrap());
                if magic != BUFFER_ENTRY_MAGIC {
                    tracing::error!(
                        offset,
                        disk_magic = magic,
                        expected_magic = BUFFER_ENTRY_MAGIC,
                        "POST-WRITE VERIFICATION FAILED (io_uring path): entry not on disk"
                    );
                }
            }
        }

        Ok(())
    }

    /// Build the IO_LINK chain for one in-flight batch: data writes (each
    /// `IOSQE_IO_LINK`) → terminal plain `FsyncData`, plus the unlinked
    /// best-effort checkpoint write. Rebuilt on each (re)submit; cheap. The raw
    /// pointers reference `batch.spans` / `batch._ckpt_buf`, which the batch
    /// keeps alive until its CQEs are harvested.
    fn chain_ops(
        batch: &InflightUringBatch,
        data_fd: RawFd,
        data_base: u64,
        ckpt_target: Option<(RawFd, u64)>,
    ) -> Vec<LinkedOp> {
        let mut ops = Vec::with_capacity(batch.write_count + 2);
        for span in &batch.spans {
            ops.push(LinkedOp {
                op: UringOp::Write {
                    fd: data_fd,
                    ptr: span.buf.as_ptr(),
                    len: span.len,
                    offset: data_base + span.offset,
                },
                link_next: true,
            });
        }
        ops.push(LinkedOp {
            op: UringOp::FsyncData { fd: data_fd },
            link_next: false,
        });
        if let (Some(buf), Some((ckpt_fd, ckpt_base))) = (batch._ckpt_buf.as_ref(), ckpt_target) {
            ops.push(LinkedOp {
                op: UringOp::Write {
                    fd: ckpt_fd,
                    ptr: buf.as_ptr(),
                    len: BLOCK_SIZE,
                    offset: ckpt_base,
                },
                link_next: false,
            });
        }
        ops
    }

    /// Submit a sealed batch's IO_LINK chain (data writes + terminal fdatasync +
    /// optional checkpoint) without waiting for completion. Returns true if the
    /// chain was accepted into the SQ ring, false if the ring was full (caller
    /// holds the batch and retries after harvesting frees space).
    fn submit_batch_nowait(
        ring: &IoUringSession,
        shard: &BufferShard,
        batch: &InflightUringBatch,
        data_fd: RawFd,
        data_base: u64,
        ckpt_target: Option<(RawFd, u64)>,
    ) -> bool {
        let base_ud = batch.batch_id << 32;
        let ops = Self::chain_ops(batch, data_fd, data_base, ckpt_target);
        unsafe {
            let _g = shard.io_lock.lock();
            match ring.submit_linked_nowait(&ops, base_ud) {
                Ok(ok) => ok,
                Err(e) => {
                    tracing::warn!(error = %e, "uring pipeline submit errored; will retry");
                    false
                }
            }
        }
    }

    /// Seal an already-drained set of staged entries into an `InflightUringBatch`
    /// ready to submit. Filters cancelled (rolled-back) appends out of the
    /// written set but keeps the full drained set for post-fsync bookkeeping.
    /// Cancel filtering happens HERE (at seal), so a cancel that lands while an
    /// entry sits in the OPEN batch is still honoured. Caller guarantees
    /// `drained` is non-empty.
    fn seal_uring_batch(
        shard: &BufferShard,
        ckpt_target: Option<(RawFd, u64)>,
        drained: Vec<StagedEntry>,
        next_batch_id: &mut u64,
    ) -> InflightUringBatch {
        let to_persist: Vec<StagedEntry> = {
            let lc = shard.lifecycle.lock();
            let mut persist = Vec::with_capacity(drained.len());
            let mut cancelled = 0usize;
            for entry in &drained {
                if lc.cancelled.contains(&entry.pending.seq) {
                    cancelled += 1;
                } else {
                    persist.push(entry.clone());
                }
            }
            if cancelled > 0 {
                tracing::warn!(
                    cancelled,
                    total = drained.len(),
                    "uring pipeline batch has cancelled entries — not written"
                );
            }
            persist
        };
        let max_seq = drained.iter().map(|e| e.pending.seq).max().unwrap_or(0);

        // Encode retries forever on allocation failure rather than dropping the
        // drained entries (which would strand the parked appenders waiting on
        // their seq). OOM here means the system is already collapsing; the
        // serial path likewise retries its inflight batch indefinitely.
        let spans = loop {
            match Self::encode_entries_into_spans(&to_persist) {
                Ok(s) => break s,
                Err(e) => {
                    tracing::error!(error = %e, "uring pipeline encode failed; retrying");
                    thread::sleep(Duration::from_millis(5));
                }
            }
        };

        let ckpt_payload = shard.encode_checkpoint_for_uring(max_seq);
        let ckpt_buf = match (&ckpt_payload, ckpt_target) {
            (Some(payload), Some(_)) => match AlignedBuf::new(BLOCK_SIZE as usize, false) {
                Ok(mut buf) => {
                    buf.as_mut_slice()[..payload.len()].copy_from_slice(payload);
                    Some(buf)
                }
                Err(_) => None,
            },
            _ => None,
        };
        let has_ckpt = ckpt_buf.is_some();
        let write_count = spans.len();
        let expected_cqes = write_count + 1 + usize::from(has_ckpt);
        let id = *next_batch_id;
        *next_batch_id += 1;
        InflightUringBatch {
            batch_id: id,
            spans,
            _ckpt_buf: ckpt_buf,
            inflight_all: drained,
            max_seq,
            write_count,
            has_ckpt,
            expected_cqes,
            seen_cqes: 0,
            failed: false,
            write_start: Instant::now(),
        }
    }

    /// Fold harvested CQEs into their in-flight batches (matched by the
    /// `batch_id` packed in the high 32 bits of `user_data`). A data-write or
    /// fsync error (or short data write) marks the batch failed; a checkpoint
    /// write error is non-fatal (best-effort recovery hint).
    fn apply_completions(fifo: &mut VecDeque<InflightUringBatch>, comps: Vec<(u64, i32)>) {
        for (ud, res) in comps {
            let bid = ud >> 32;
            let op_idx = (ud & 0xFFFF_FFFF) as usize;
            if let Some(b) = fifo.iter_mut().find(|b| b.batch_id == bid) {
                b.seen_cqes += 1;
                if res < 0 {
                    let is_ckpt = b.has_ckpt && op_idx == b.write_count + 1;
                    if !is_ckpt {
                        b.failed = true;
                    }
                } else if op_idx < b.write_count && res != b.spans[op_idx].len as i32 {
                    // Short data write → treat as failure.
                    b.failed = true;
                }
            }
        }
    }

    /// Post-fsync work for a successfully-durable batch, in FIFO (seq) order:
    /// retire superseded ranges, strip stale cancellation flags, advance the LV2
    /// durability watermark (covers cancelled seqs too), record metrics. Mirrors
    /// the serial path's post-sync block.
    fn finish_uring_batch(
        shard: &BufferShard,
        batch: &InflightUringBatch,
        metrics: &Arc<OnceLock<Arc<EngineMetrics>>>,
    ) {
        let pendings: Vec<Arc<PendingEntry>> = batch
            .inflight_all
            .iter()
            .map(|e| e.pending.clone())
            .collect();
        shard.retire_superseded_by_durable_entries(&pendings);
        {
            let mut lc = shard.lifecycle.lock();
            for e in &batch.inflight_all {
                lc.cancelled.remove(&e.pending.seq);
            }
        }
        shard.lv2_durability.advance(batch.max_seq);
        if let Some(m) = metrics.get() {
            let batch_entries = batch.inflight_all.len() as u64;
            let batch_bytes = batch
                .inflight_all
                .iter()
                .map(|e| e.payload.len() as u64)
                .sum::<u64>();
            m.buffer_sync_batches.fetch_add(1, Ordering::Relaxed);
            m.buffer_sync_entries
                .fetch_add(batch_entries, Ordering::Relaxed);
            m.buffer_sync_bytes
                .fetch_add(batch_bytes, Ordering::Relaxed);
            crate::metrics::record_counter_max(&m.buffer_sync_entries_max, batch_entries);
            crate::metrics::record_counter_max(&m.buffer_sync_bytes_max, batch_bytes);
            m.buffer_sync_epochs_committed
                .fetch_add(batch_entries, Ordering::Relaxed);
            BufferShard::record_metric(&m.buffer_append_log_write_ns, batch.write_start);
            BufferShard::record_metric(&m.buffer_sync_batch_ns, batch.write_start);
        }
    }

    /// Recover after the FIFO front's chain failed. Quiesce the whole pipeline
    /// (harvest every in-flight batch to completion) so no foreign CQEs remain,
    /// then process the FIFO front-to-back: finish successful batches in order,
    /// and re-submit failed ones serially (nothing else in flight, so each
    /// re-submit's CQEs are unambiguous) with backoff until they succeed. This
    /// is the rare error path; the cost of quiescing is irrelevant.
    #[allow(clippy::too_many_arguments)]
    fn recover_failed_front(
        fifo: &mut VecDeque<InflightUringBatch>,
        shard: &BufferShard,
        ring: &Arc<IoUringSession>,
        data_fd: RawFd,
        data_base: u64,
        ckpt_target: Option<(RawFd, u64)>,
        next_batch_id: &mut u64,
        metrics: &Arc<OnceLock<Arc<EngineMetrics>>>,
    ) -> OnyxResult<()> {
        // 1. Quiesce: drive every batch to full completion.
        while fifo.iter().any(|b| b.seen_cqes < b.expected_cqes) {
            let comps = ring.harvest(1)?;
            Self::apply_completions(fifo, comps);
        }
        // 2. Process front-to-back.
        let mut consecutive = 0u32;
        while let Some(front) = fifo.front() {
            if !front.failed {
                let b = fifo.pop_front().unwrap();
                Self::finish_uring_batch(shard, &b, metrics);
                continue;
            }
            consecutive = consecutive.saturating_add(1);
            thread::sleep(Self::sync_retry_backoff(consecutive));
            {
                let front = fifo.front_mut().unwrap();
                front.batch_id = *next_batch_id;
                *next_batch_id += 1;
                front.seen_cqes = 0;
                front.failed = false;
                front.write_start = Instant::now();
            }
            let base_ud = fifo.front().unwrap().batch_id << 32;
            let submitted = {
                let ops = Self::chain_ops(fifo.front().unwrap(), data_fd, data_base, ckpt_target);
                unsafe {
                    let _g = shard.io_lock.lock();
                    ring.submit_linked_nowait(&ops, base_ud)?
                }
            };
            if !submitted {
                // Post-quiesce the SQ is empty so this cannot happen; guard
                // anyway by re-marking failed to retry on the next pass.
                let front = fifo.front_mut().unwrap();
                front.failed = true;
                front.seen_cqes = front.expected_cqes;
                continue;
            }
            let expected = fifo.front().unwrap().expected_cqes;
            while fifo.front().unwrap().seen_cqes < expected {
                let comps = ring.harvest(1)?;
                Self::apply_completions(fifo, comps);
            }
            if Self::consume_test_sync_failpoint().is_err() {
                fifo.front_mut().unwrap().failed = true;
            }
            if !fifo.front().unwrap().failed {
                consecutive = 0;
                let b = fifo.pop_front().unwrap();
                Self::finish_uring_batch(shard, &b, metrics);
            }
        }
        Ok(())
    }

    /// Pipelined LV2 fdatasync loop: keep up to `depth` fsync chains in flight so
    /// batch N+1's writes overlap batch N's flush, removing the per-batch serial
    /// fsync stall. Durability is preserved by advancing `lv2_durability` only
    /// over the contiguous FIFO prefix of fully-fsync'd batches (a later batch's
    /// fsync completing does NOT imply earlier batches' writes reached the
    /// device). One sync thread per shard owns its ring, so the harvest/submit
    /// have no cross-thread contention.
    #[allow(clippy::too_many_arguments)]
    fn uring_sync_pipeline_loop(
        device: Arc<dyn BlockBackend>,
        shard: Arc<BufferShard>,
        group_commit_wait: Duration,
        wake_rx: Receiver<()>,
        shutdown: Arc<AtomicBool>,
        metrics: Arc<OnceLock<Arc<EngineMetrics>>>,
        ring: Arc<IoUringSession>,
        depth: usize,
        commit_timeout_pct: u64,
    ) {
        // The pipeline path replaces the serial path's fixed group-commit sleep
        // with the ZFS self-clocked adaptive window (see `window` below), so the
        // serial `group_commit_wait` knob is intentionally unused here.
        let _ = group_commit_wait;
        // The dispatch in `sync_loop` only routes a fd-backed device here, so
        // the uring target is always present. `device` is kept alive (the Arc)
        // for the loop's lifetime so the fd stays valid behind the SQEs.
        let (data_fd, data_base) = device
            .uring_target()
            .expect("uring pipeline requires a fd-backed device");
        let ckpt_target = shard.checkpoint_target();
        // Reserve 2 SQEs of every chain for the terminal fsync + checkpoint, so
        // a sealed batch's chain always fits the ring in one submit.
        let max_chain_entries = (ring.sq_entries() as usize).saturating_sub(2).max(1);
        // Per-batch "full" caps = SQ-fit ∩ configured batch caps.
        let entry_cap = max_chain_entries.min(shard.sync_batch_max_entries.max(1));
        let byte_cap = shard.sync_batch_max_bytes.max(1);
        let pct = commit_timeout_pct.max(1);
        // ZFS `zl_last_lwb_latency`: EMA of submit→fully-durable latency, sized
        // from real completions; drives the OPEN batch's adaptive close window.
        let window = |ema: Duration| -> Duration {
            let w = Duration::from_nanos((ema.as_nanos() as u64).saturating_mul(pct) / 100);
            w.max(LV2_COMMIT_TIMEOUT_FLOOR)
        };
        let mut fifo: VecDeque<InflightUringBatch> = VecDeque::new();
        let mut pending_submit: Option<InflightUringBatch> = None;
        let mut open: Option<OpenBatch> = None;
        let mut next_batch_id: u64 = 1;
        let mut ema_write = Duration::ZERO;

        loop {
            // 1. Submit a held-back batch (SQ was full) once a FIFO slot frees.
            if let Some(mut batch) = pending_submit.take() {
                if fifo.len() < depth {
                    batch.write_start = Instant::now();
                    if Self::submit_batch_nowait(
                        &ring,
                        &shard,
                        &batch,
                        data_fd,
                        data_base,
                        ckpt_target,
                    ) {
                        fifo.push_back(batch);
                    } else {
                        pending_submit = Some(batch);
                    }
                } else {
                    pending_submit = Some(batch);
                }
            }

            // 2. Accumulate newly-staged entries into the OPEN batch. Runs even
            //    while the FIFO is full, so the batch grows DURING the in-flight
            //    fdatasync — the ZFS "keep the next lwb open" overlap.
            if pending_submit.is_none() {
                let ob = open.get_or_insert_with(|| OpenBatch {
                    entries: Vec::new(),
                    bytes: 0,
                    opened_at: Instant::now(),
                });
                let room = entry_cap.saturating_sub(ob.entries.len());
                if room > 0 && ob.bytes < byte_cap {
                    let more = shard.drain_staged_capped(room);
                    if !more.is_empty() {
                        if ob.entries.is_empty() {
                            ob.opened_at = Instant::now();
                        }
                        ob.bytes = ob
                            .bytes
                            .saturating_add(more.iter().map(|e| e.payload.len()).sum::<usize>());
                        ob.entries.extend(more);
                    }
                }
                if open.as_ref().is_some_and(|o| o.entries.is_empty()) {
                    open = None;
                }
            }

            // 3. Seal+submit the OPEN batch when full OR its adaptive window has
            //    elapsed, if a FIFO slot is free and nothing is held back. Do NOT
            //    issue a partially-full batch early just because staging
            //    momentarily drained (ZFS policy) — the window bounds the wait.
            if pending_submit.is_none() && fifo.len() < depth {
                let should_seal = open.as_ref().is_some_and(|ob| {
                    ob.entries.len() >= entry_cap
                        || ob.bytes >= byte_cap
                        || ob.opened_at.elapsed() >= window(ema_write)
                });
                if should_seal {
                    let ob = open.take().unwrap();
                    let mut batch =
                        Self::seal_uring_batch(&shard, ckpt_target, ob.entries, &mut next_batch_id);
                    batch.write_start = Instant::now();
                    if Self::submit_batch_nowait(
                        &ring,
                        &shard,
                        &batch,
                        data_fd,
                        data_base,
                        ckpt_target,
                    ) {
                        fifo.push_back(batch);
                    } else {
                        pending_submit = Some(batch);
                    }
                }
            }

            // 4. Make progress. With writes in flight, block for ≥1 completion
            //    unless we can still grow the OPEN batch right now; when idle,
            //    park bounded by the OPEN batch's remaining window so a lone
            //    entry still seals on time.
            if fifo.is_empty() {
                let wait = match open.as_ref() {
                    Some(ob) => window(ema_write)
                        .saturating_sub(ob.opened_at.elapsed())
                        .max(Duration::from_micros(1)),
                    None => Duration::from_millis(50),
                };
                let _ = wake_rx.recv_timeout(wait);
                while wake_rx.try_recv().is_ok() {}
            } else {
                let can_accumulate = !shard.staging_rx.is_empty()
                    && pending_submit.is_none()
                    && open
                        .as_ref()
                        .map_or(true, |o| o.entries.len() < entry_cap && o.bytes < byte_cap);
                let min_complete = usize::from(!can_accumulate);
                match ring.harvest(min_complete) {
                    Ok(comps) => Self::apply_completions(&mut fifo, comps),
                    Err(e) => {
                        tracing::warn!(error = %e, "uring pipeline harvest errored");
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            }

            // 5. Advance the contiguous fully-fsync'd prefix from the front.
            loop {
                let front_done = fifo.front().is_some_and(|f| f.seen_cqes >= f.expected_cqes);
                if !front_done {
                    break;
                }
                // Honour the test failpoint once per batch (uring-path injection).
                if !fifo.front().unwrap().failed && Self::consume_test_sync_failpoint().is_err() {
                    fifo.front_mut().unwrap().failed = true;
                }
                if fifo.front().unwrap().failed {
                    if let Err(e) = Self::recover_failed_front(
                        &mut fifo,
                        &shard,
                        &ring,
                        data_fd,
                        data_base,
                        ckpt_target,
                        &mut next_batch_id,
                        &metrics,
                    ) {
                        tracing::error!(error = %e, "uring pipeline failure recovery errored");
                        thread::sleep(Duration::from_millis(5));
                    }
                    break; // recovery drained the FIFO
                } else {
                    let b = fifo.pop_front().unwrap();
                    let measured = b.write_start.elapsed();
                    Self::finish_uring_batch(&shard, &b, &metrics);
                    // Fold the real submit→durable latency into the EMA (ZFS
                    // `zl_last_lwb_latency = (old*7 + new)/8`); normal completions
                    // only, never error-recovered batches.
                    ema_write = if ema_write.is_zero() {
                        measured
                    } else {
                        (ema_write * 7 + measured) / 8
                    };
                }
            }

            // 6. Exit only when fully drained (including the OPEN batch).
            if shutdown.load(Ordering::Relaxed)
                && fifo.is_empty()
                && pending_submit.is_none()
                && open.is_none()
                && shard.staging_rx.is_empty()
            {
                return;
            }
        }
    }

    /// Pool-level sync pipeline for a multi-shard backend whose durability
    /// barrier applies to the whole logical disk (chunklet). One persistent
    /// worker per shard drains and encodes entries in parallel. This thread
    /// collects those prepared batches, writes them through the root LD, and
    /// publishes all covered shard watermarks after one shared flush.
    pub(super) fn global_sync_loop(
        root_device: Arc<dyn BlockBackend>,
        members: Vec<(u64, Arc<BufferShard>, Receiver<()>)>,
        group_commit_wait: Duration,
        shutdown: Arc<AtomicBool>,
        metrics: Arc<OnceLock<Arc<EngineMetrics>>>,
    ) {
        struct PreparedBatch {
            member_idx: usize,
            all: Vec<StagedEntry>,
            spans: Vec<CoalescedSpan>,
            checkpoint: Option<AlignedBuf>,
            started: Instant,
        }

        struct WrittenBatch {
            member_idx: usize,
            all: Vec<StagedEntry>,
            max_seq: u64,
            checkpoint: Option<AlignedBuf>,
            started: Instant,
        }

        let queue_depth = members.len().max(1);
        // Four lanes overlap root writes while the device-wide durability
        // coordinator publishes completed epochs. The group-commit wait lives
        // only here (not in shard preparation or the coordinator), so each
        // request pays one window rather than three.
        let write_lane_count = members.len().min(4).max(1);
        let write_lanes: Vec<_> = (0..write_lane_count)
            .map(|_| bounded::<PreparedBatch>(queue_depth))
            .collect();
        let member_bases = Arc::new(members.iter().map(|member| member.0).collect::<Vec<_>>());
        let (written_tx, written_rx) = bounded::<Vec<WrittenBatch>>(queue_depth);
        thread::scope(|scope| {
            for (member_idx, (_, shard, wake_rx)) in members.iter().enumerate() {
                let tx = write_lanes[member_idx % write_lane_count].0.clone();
                let shard = shard.clone();
                let wake_rx = wake_rx.clone();
                let shutdown = shutdown.clone();
                scope.spawn(move || {
                    crate::affinity::bind_current(
                        crate::affinity::ThreadRole::BufferSync,
                        member_idx,
                    );
                    loop {
                        if shard.staging_rx.is_empty() {
                            match wake_rx.recv_timeout(Duration::from_millis(50)) {
                                Ok(()) => {}
                                Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {
                                    if shutdown.load(Ordering::Relaxed)
                                        && shard.staging_rx.is_empty()
                                    {
                                        return;
                                    }
                                    continue;
                                }
                            }
                        }
                        while wake_rx.try_recv().is_ok() {}
                        // Do not spend the group-commit window here. The four
                        // write lanes below provide the one intentional
                        // coalescing point across shards; waiting at prepare,
                        // write, and flush used to charge the foreground ack
                        // path the same window three times.
                        let started = Instant::now();
                        let all = shard.drain_staged_limited();
                        if all.is_empty() {
                            continue;
                        }
                        let persist = {
                            let lifecycle = shard.lifecycle.lock();
                            all.iter()
                                .filter(|entry| !lifecycle.cancelled.contains(&entry.pending.seq))
                                .cloned()
                                .collect::<Vec<_>>()
                        };
                        let spans = loop {
                            match Self::encode_entries_into_spans(&persist) {
                                Ok(spans) => break spans,
                                Err(error) => {
                                    tracing::error!(
                                        member_idx,
                                        error = %error,
                                        "global sync prepare failed; retrying batch"
                                    );
                                    thread::sleep(Duration::from_millis(5));
                                }
                            }
                        };
                        let max_seq = all.iter().map(|entry| entry.pending.seq).max().unwrap_or(0);
                        let checkpoint =
                            shard
                                .encode_checkpoint_for_uring(max_seq)
                                .and_then(|payload| {
                                    match AlignedBuf::new(SHARD_CHECKPOINT_SIZE as usize, false) {
                                        Ok(mut buf) => {
                                            buf.as_mut_slice()[..payload.len()]
                                                .copy_from_slice(&payload);
                                            Some(buf)
                                        }
                                        Err(error) => {
                                            tracing::warn!(
                                                member_idx,
                                                error = %error,
                                                "global sync checkpoint hint allocation failed"
                                            );
                                            None
                                        }
                                    }
                                });
                        if tx
                            .send(PreparedBatch {
                                member_idx,
                                all,
                                spans,
                                checkpoint,
                                started,
                            })
                            .is_err()
                        {
                            return;
                        }
                        if shutdown.load(Ordering::Relaxed) && shard.staging_rx.is_empty() {
                            return;
                        }
                    }
                });
            }
            for (lane_idx, (lane_tx, lane_rx)) in write_lanes.into_iter().enumerate() {
                drop(lane_tx);
                let root_device = root_device.clone();
                let member_bases = member_bases.clone();
                let metrics = metrics.clone();
                let written_tx = written_tx.clone();
                scope.spawn(move || {
                    crate::affinity::bind_current(
                        crate::affinity::ThreadRole::BufferSync,
                        lane_idx,
                    );
                    while let Ok(first) = lane_rx.recv() {
                        let mut prepared = vec![first];
                        let collect_started = Instant::now();
                        if !group_commit_wait.is_zero() {
                            let deadline = collect_started + group_commit_wait;
                            loop {
                                let now = Instant::now();
                                if now >= deadline {
                                    break;
                                }
                                match lane_rx.recv_timeout(deadline - now) {
                                    Ok(batch) => prepared.push(batch),
                                    Err(RecvTimeoutError::Timeout) => break,
                                    Err(RecvTimeoutError::Disconnected) => break,
                                }
                            }
                        }
                        if let Some(metrics) = metrics.get() {
                            BufferShard::record_metric(
                                &metrics.buffer_sync_sleep_ns,
                                collect_started,
                            );
                        }
                        while let Ok(batch) = lane_rx.try_recv() {
                            prepared.push(batch);
                        }

                        let mut consecutive_failures = 0u32;
                        loop {
                            let mut ops = Vec::new();
                            for batch in &prepared {
                                let shard_base = member_bases[batch.member_idx];
                                ops.extend(batch.spans.iter().map(|span| {
                                    (
                                        shard_base + span.offset,
                                        &span.buf.as_slice()[..span.len as usize],
                                    )
                                }));
                            }
                            let write_started = Instant::now();
                            match root_device.write_many_at(&ops) {
                                Ok(()) => {
                                    let write_elapsed = write_started.elapsed();
                                    if write_elapsed >= Duration::from_millis(10) {
                                        tracing::warn!(
                                            lane_idx,
                                            prepared_batches = prepared.len(),
                                            ops = ops.len(),
                                            elapsed_us = write_elapsed.as_micros() as u64,
                                            "slow LV2 global root write"
                                        );
                                    }
                                    if let Some(metrics) = metrics.get() {
                                        BufferShard::record_metric(
                                            &metrics.buffer_append_log_write_ns,
                                            write_started,
                                        );
                                    }
                                    break;
                                }
                                Err(error) => {
                                    consecutive_failures = consecutive_failures.saturating_add(1);
                                    tracing::warn!(
                                        lane_idx,
                                        error = %error,
                                        consecutive_failures,
                                        "global sync write lane failed; retrying batch"
                                    );
                                    thread::sleep(Self::sync_retry_backoff(consecutive_failures));
                                }
                            }
                        }

                        let written = prepared
                            .into_iter()
                            .map(|batch| {
                                let max_seq = batch
                                    .all
                                    .iter()
                                    .map(|entry| entry.pending.seq)
                                    .max()
                                    .unwrap_or(0);
                                WrittenBatch {
                                    member_idx: batch.member_idx,
                                    all: batch.all,
                                    max_seq,
                                    checkpoint: batch.checkpoint,
                                    started: batch.started,
                                }
                            })
                            .collect();
                        if written_tx.send(written).is_err() {
                            return;
                        }
                    }
                });
            }
            drop(written_tx);

            let mut consecutive_failures = 0u32;
            while let Ok(mut batches) = written_rx.recv() {
                // A lane already formed the complete durability epoch for its
                // root write. Drain any sibling epochs that finished in the
                // meantime, then publish the shared barrier immediately.
                while let Ok(epoch) = written_rx.try_recv() {
                    batches.extend(epoch);
                }

                let epoch_started = batches
                    .iter()
                    .map(|batch| batch.started)
                    .min()
                    .expect("durability epoch has at least one batch");
                let epoch_entries = batches.iter().map(|batch| batch.all.len()).sum::<usize>();

                loop {
                    // Checkpoint pages all live in the first 128 KiB LV2
                    // stripe. Writing one from every payload lane made the
                    // lanes contend on the same chunklet range + stripe locks,
                    // serialising otherwise-disjoint ring writes. Select the
                    // newest hint per shard and write the compact checkpoint
                    // set once, after all payload writes in this epoch landed.
                    let mut latest_checkpoints: Vec<Option<(u64, &AlignedBuf)>> =
                        (0..members.len()).map(|_| None).collect();
                    for batch in &batches {
                        let Some(checkpoint) = batch.checkpoint.as_ref() else {
                            continue;
                        };
                        let slot = &mut latest_checkpoints[batch.member_idx];
                        if slot
                            .as_ref()
                            .is_none_or(|(max_seq, _)| batch.max_seq >= *max_seq)
                        {
                            *slot = Some((batch.max_seq, checkpoint));
                        }
                    }
                    let checkpoint_ops: Vec<(u64, &[u8])> = latest_checkpoints
                        .iter()
                        .enumerate()
                        .filter_map(|(member_idx, checkpoint)| {
                            checkpoint.as_ref().map(|(_, buf)| {
                                (
                                    COMMIT_LOG_SUPERBLOCK_SIZE
                                        + member_idx as u64 * SHARD_CHECKPOINT_SIZE,
                                    &buf.as_slice()[..SHARD_CHECKPOINT_SIZE as usize],
                                )
                            })
                        })
                        .collect();
                    let checkpoint_started = Instant::now();
                    let checkpoint_result = if checkpoint_ops.is_empty() {
                        Ok(())
                    } else {
                        root_device.write_many_at(&checkpoint_ops)
                    };
                    if let Err(err) = checkpoint_result {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        tracing::warn!(
                            error = %err,
                            consecutive_failures,
                            checkpoint_pages = checkpoint_ops.len(),
                            "global persistent slot checkpoint write failed; retrying epoch"
                        );
                        thread::sleep(Self::sync_retry_backoff(consecutive_failures));
                        continue;
                    }
                    if let Some(metrics) = metrics.get() {
                        BufferShard::record_metric(
                            &metrics.buffer_append_log_write_ns,
                            checkpoint_started,
                        );
                    }

                    // All payload and checkpoint writes in `batches` completed.
                    // A device-wide flush now makes that whole prefix durable.
                    // Workers may issue later writes concurrently; those remain
                    // unacknowledged until a subsequent flush.
                    let flush_started = Instant::now();
                    let result = Self::sync_device_impl(root_device.as_ref());
                    let flush_elapsed = flush_started.elapsed();
                    match result {
                        Ok(()) => {
                            consecutive_failures = 0;
                            if let Some(metrics) = metrics.get() {
                                metrics.buffer_sync_flushes.fetch_add(1, Ordering::Relaxed);
                            }
                            let epoch_elapsed = epoch_started.elapsed();
                            if flush_elapsed >= Duration::from_millis(10)
                                || epoch_elapsed >= Duration::from_millis(10)
                            {
                                tracing::warn!(
                                    shard_batches = batches.len(),
                                    entries = epoch_entries,
                                    flush_us = flush_elapsed.as_micros() as u64,
                                    epoch_us = epoch_elapsed.as_micros() as u64,
                                    "slow LV2 global durability epoch"
                                );
                            }
                            break;
                        }
                        Err(err) => {
                            consecutive_failures = consecutive_failures.saturating_add(1);
                            tracing::warn!(
                                error = %err,
                                consecutive_failures,
                                shard_batches = batches.len(),
                                "global persistent slot sync failed; retrying epoch"
                            );
                            thread::sleep(Self::sync_retry_backoff(consecutive_failures));
                        }
                    }
                }

                for batch in batches {
                    let shard = &members[batch.member_idx].1;
                    let pendings: Vec<Arc<PendingEntry>> = batch
                        .all
                        .iter()
                        .map(|entry| entry.pending.clone())
                        .collect();
                    shard.retire_superseded_by_durable_entries(&pendings);
                    {
                        let mut lifecycle = shard.lifecycle.lock();
                        for entry in &batch.all {
                            lifecycle.cancelled.remove(&entry.pending.seq);
                        }
                    }
                    let max_seq = batch
                        .all
                        .iter()
                        .map(|entry| entry.pending.seq)
                        .max()
                        .unwrap_or(0);
                    shard.lv2_durability.advance(max_seq);
                    if let Some(metrics) = metrics.get() {
                        let entries = batch.all.len() as u64;
                        let bytes = batch
                            .all
                            .iter()
                            .map(|entry| entry.payload.len() as u64)
                            .sum::<u64>();
                        metrics.buffer_sync_batches.fetch_add(1, Ordering::Relaxed);
                        metrics
                            .buffer_sync_entries
                            .fetch_add(entries, Ordering::Relaxed);
                        metrics
                            .buffer_sync_bytes
                            .fetch_add(bytes, Ordering::Relaxed);
                        crate::metrics::record_counter_max(
                            &metrics.buffer_sync_entries_max,
                            entries,
                        );
                        crate::metrics::record_counter_max(&metrics.buffer_sync_bytes_max, bytes);
                        metrics
                            .buffer_sync_epochs_committed
                            .fetch_add(entries, Ordering::Relaxed);
                        BufferShard::record_metric(&metrics.buffer_sync_batch_ns, batch.started);
                    }
                }
            }
        });
    }

    pub(super) fn sync_loop(
        device: Arc<dyn BlockBackend>,
        shard: Arc<BufferShard>,
        group_commit_wait: Duration,
        wake_rx: Receiver<()>,
        shutdown: Arc<AtomicBool>,
        metrics: Arc<OnceLock<Arc<EngineMetrics>>>,
        _ready_tx: Sender<u64>,
        _shard_ready_tx: Sender<u64>,
        uring: Option<Arc<IoUringSession>>,
        pipeline_depth: usize,
        commit_timeout_pct: u64,
    ) {
        // A chunklet LD has no single fd — it owns its cross-PD io_uring
        // internally — so it never takes either onyx-side io_uring path. The
        // sync session (`uring`) is only constructed for a fd-backed device, but
        // gate on the device's own discriminator too so the two can never drift.
        let has_uring_target = device.uring_target().is_some();

        // Pipelined LV2 fdatasync path: keep `pipeline_depth` fsync chains in
        // flight so batch N+1's writes overlap batch N's flush, with a ZFS
        // self-clocked adaptive accumulation window. Only the io_uring backend
        // supports it; depth 1 (or syscall/chunklet) falls through to the legacy
        // submit→wait-all→submit-next loop below.
        if pipeline_depth >= 2 && has_uring_target {
            if let Some(ref ring) = uring {
                Self::uring_sync_pipeline_loop(
                    device,
                    shard,
                    group_commit_wait,
                    wake_rx,
                    shutdown,
                    metrics,
                    ring.clone(),
                    pipeline_depth,
                    commit_timeout_pct,
                );
                return;
            }
        }

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

                let result = match (uring.as_ref(), has_uring_target) {
                    (Some(ring), true) => {
                        // Batched io_uring path: N entry writes + 1 checkpoint
                        // write + 1 DRAIN-flagged fdatasync — all in one submit.
                        Self::write_batch_and_sync_uring(
                            device.as_ref(),
                            &shard,
                            ring,
                            &shard.io_lock,
                            &writes_to_persist,
                            batch_max_seq_pre,
                            &metrics,
                        )
                    }
                    // Syscall / chunklet path: one batched `write_many_at`
                    // (chunklet fans across PDs), then checkpoint + `flush`
                    // below provide the ack-after-durable barrier.
                    _ => Self::write_batch(
                        device.as_ref(),
                        &shard.io_lock,
                        &writes_to_persist,
                        &metrics,
                    ),
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
            // write_batch_and_sync_uring. The syscall / chunklet path still
            // needs both: write the checkpoint hint, then one `flush` makes the
            // batch payload AND the checkpoint durable in a single barrier
            // (chunklet's flush fans `sync()` across every member PD).
            let sync_result = if uring.is_some() && has_uring_target {
                Ok(())
            } else {
                shard.write_checkpoint(batch_max_seq);
                let result = Self::sync_device_impl(device.as_ref());
                if result.is_ok() {
                    if let Some(metrics) = metrics.get() {
                        metrics.buffer_sync_flushes.fetch_add(1, Ordering::Relaxed);
                    }
                }
                result
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
