use super::*;
use std::collections::VecDeque;
use std::os::fd::RawFd;

const POST_WRITE_VERIFY: bool = false;

/// Profiling every root write would add two clock syscalls to the LV2 hot path.
/// One sample per 64 calls is frequent enough for one-second metrics deltas while
/// keeping the measurement overhead negligible.
#[cfg(any(test, feature = "diagnostic-metrics"))]
const LV2_PAYLOAD_PROFILE_SAMPLE_MASK: u64 = 63;

#[cfg(any(test, feature = "diagnostic-metrics"))]
fn should_sample_lv2_payload_write(sequence: &mut u64) -> bool {
    *sequence = sequence.wrapping_add(1);
    *sequence & LV2_PAYLOAD_PROFILE_SAMPLE_MASK == 0
}

#[cfg(all(
    target_os = "linux",
    any(test, feature = "diagnostic-metrics")
))]
fn thread_cpu_time() -> Option<Duration> {
    let mut ts = std::mem::MaybeUninit::<libc::timespec>::uninit();
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, ts.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let ts = unsafe { ts.assume_init() };
    Some(Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32))
}

#[cfg(all(
    not(target_os = "linux"),
    any(test, feature = "diagnostic-metrics")
))]
fn thread_cpu_time() -> Option<Duration> {
    None
}

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
    /// Encode each staged entry directly into an `AlignedBuf`, coalescing
    /// entries whose reserved disk ranges are contiguous into a single buffer.
    ///
    /// This replaces the old "encode into a fresh `vec![0u8; n]` per entry,
    /// then memcpy into an AlignedBuf" two-pass: the per-entry Vec was a
    /// jemalloc large-class allocation, so freeing it drove
    /// `madvise(MADV_DONTNEED)` → cross-core TLB-shootdown IPIs on the LV2 sync
    /// thread (perf 2026-05-29: ~10% aggregate on-CPU, smeared across all
    /// cores). Encoding straight into the span buffer's sub-slice via
    /// `encode_full_into_slice` removes that allocation, the extra memcpy, and
    /// the redundant zero-fill.
    ///
    /// ⛔ That change did NOT end the madvise churn, contrary to what this
    /// comment used to claim. It moved it from the per-entry `Vec` to the span
    /// buffer itself: `AlignedBuf::new`'s thread-local pool only reuses a
    /// parked buffer whose capacity already fits, an LV2 span's length is a
    /// wide random variable under a 4k-32k workload, and every miss calls
    /// `alloc_zeroed` — which jemalloc satisfies off a recycled extent by
    /// purging it. A 2026-09-24 in-engine `perf` profile caught this thread
    /// class at ~12.9k madvise/s, **62 % of the box's 20.7k/s**, driving
    /// ~810k TLB-shootdown IPIs/s for ~2.8 CPUs of the machine's 53.4 busy
    /// (memory `perf_inside_engine_first_cpu_ledger`).
    ///
    /// ⭐ So `arena` is the fix, not the pool: a [`crate::mem::SlabArena`] slot
    /// is pre-faulted once and never returned to the OS. The precondition is
    /// the arena's invariant 3, "slots come back dirty, cover every byte you
    /// submit" — which this function already satisfied, because
    /// `encode_full_into_slice` writes the whole `[0..disk_len)` of each entry
    /// (padding included) and `total_len` is exactly the sum of those. The
    /// `alloc_zeroed` was pure waste on this path even before it was measured.
    fn encode_entries_into_spans(
        entries: &[StagedEntry],
        arena: Option<&Arc<SlabArena>>,
    ) -> OnyxResult<Vec<CoalescedSpan>> {
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
            let mut buf = match arena {
                Some(arena) => arena.take(total_len)?,
                None => AlignedBuf::new(total_len, false)?,
            };
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

    #[cfg_attr(
        not(any(test, feature = "diagnostic-metrics")),
        allow(unused_variables)
    )]
    fn write_batch(
        device: &dyn BlockBackend,
        shard: &BufferShard,
        io_lock: &parking_lot::Mutex<()>,
        entries: &[StagedEntry],
        metrics: &Arc<OnceLock<Arc<EngineMetrics>>>,
    ) -> OnyxResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let spans = Self::encode_entries_into_spans(entries, shard.lv2_arena())?;

        #[cfg(any(test, feature = "diagnostic-metrics"))]
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
        crate::diagnostic_metrics! {
            if let Some(metrics) = metrics.get() {
                BufferShard::record_metric(&metrics.buffer_append_log_write_ns, write_start);
            }
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

    #[cfg_attr(
        not(any(test, feature = "diagnostic-metrics")),
        allow(unused_variables)
    )]
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
        let spans = Self::encode_entries_into_spans(entries, shard.lv2_arena())?;

        // 3. Optional checkpoint payload (only when the shard has a checkpoint
        //    device — same condition as `write_checkpoint`).
        let checkpoint_payload = shard.encode_checkpoint_for_uring(batch_max_seq);
        let checkpoint_target = shard.checkpoint_target();
        let mut ckpt_aligned: Option<AlignedBuf> = None;
        if let (Some(payload), Some(_)) = (&checkpoint_payload, checkpoint_target) {
            // `new_zeroed`: the whole 4 KiB block is written but only
            // `[..payload.len()]` is filled, and `AlignedBuf::new` guarantees
            // nothing about the rest. Once per sync cycle, so the memset is
            // not on a hot path.
            let mut buf = AlignedBuf::new_zeroed(BLOCK_SIZE as usize, false)?;
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
        #[cfg(any(test, feature = "diagnostic-metrics"))]
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

        crate::diagnostic_metrics! {
            if let Some(metrics) = metrics.get() {
                BufferShard::record_metric(&metrics.buffer_append_log_write_ns, write_start);
            }
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
    ) -> OnyxResult<bool> {
        let base_ud = batch.batch_id << 32;
        let ops = Self::chain_ops(batch, data_fd, data_base, ckpt_target);
        unsafe {
            let _g = shard.io_lock.lock();
            match ring.submit_linked_nowait(&ops, base_ud)? {
                LinkedSubmitOutcome::Full => Ok(false),
                LinkedSubmitOutcome::Submitted => Ok(true),
                LinkedSubmitOutcome::QueuedAfterSubmitError(error) => {
                    // The complete chain was already published to the shared SQ
                    // before io_uring_enter failed. Track the batch as in-flight
                    // so its buffers remain alive; `harvest` submits it again.
                    tracing::warn!(error = %error, "uring pipeline submit errored; queued chain retained");
                    Ok(true)
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
            match Self::encode_entries_into_spans(&to_persist, shard.lv2_arena()) {
                Ok(s) => break s,
                Err(e) => {
                    tracing::error!(error = %e, "uring pipeline encode failed; retrying");
                    thread::sleep(Duration::from_millis(5));
                }
            }
        };

        let ckpt_payload = shard.encode_checkpoint_for_uring(max_seq);
        let ckpt_buf = match (&ckpt_payload, ckpt_target) {
            // `new_zeroed` for the same reason as the non-pipelined path above:
            // a full block is submitted, only its head is filled.
            (Some(payload), Some(_)) => match AlignedBuf::new_zeroed(BLOCK_SIZE as usize, false) {
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

    /// A stage-order fault freezes the watermark immediately, but submitted SQEs
    /// still borrow the batch buffers through raw pointers. Harvest every CQE
    /// before dropping those buffers; none of these batches are published.
    fn quiesce_uring_fifo_after_stage_fault(
        ring: &IoUringSession,
        fifo: &mut VecDeque<InflightUringBatch>,
    ) {
        while fifo
            .iter()
            .any(|batch| batch.seen_cqes < batch.expected_cqes)
        {
            match ring.harvest(1) {
                Ok(completions) => Self::apply_completions(fifo, completions),
                Err(error) => {
                    tracing::error!(error = %error, "failed to quiesce poisoned LV2 io_uring");
                    thread::sleep(Duration::from_millis(1));
                }
            }
        }
        fifo.clear();
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
        let advanced_at_ns = lv2_metric_timestamp_ns(Instant::now());
        for entry in &batch.inflight_all {
            entry
                .pending
                .durability_advanced_at_ns
                .store(advanced_at_ns, Ordering::Release);
        }
        if !shard.lv2_durability.advance(batch.max_seq) {
            return;
        }
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
        for entry in &batch.inflight_all {
            shard.publish_ready(entry.pending.seq);
        }
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
            crate::diagnostic_metrics! {
                BufferShard::record_metric(&m.buffer_append_log_write_ns, batch.write_start);
                BufferShard::record_metric(&m.buffer_sync_batch_ns, batch.write_start);
            }
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
                    match ring.submit_linked_nowait(&ops, base_ud) {
                        Ok(LinkedSubmitOutcome::Full) => Ok(false),
                        Ok(LinkedSubmitOutcome::Submitted) => Ok(true),
                        Ok(LinkedSubmitOutcome::QueuedAfterSubmitError(error)) => {
                            tracing::warn!(
                                error = %error,
                                "uring recovery submit errored; queued chain retained"
                            );
                            Ok(true)
                        }
                        Err(error) => Err(error),
                    }
                }
            };
            let submitted = match submitted {
                Ok(submitted) => submitted,
                Err(error) => {
                    let front = fifo.front_mut().unwrap();
                    front.failed = true;
                    front.seen_cqes = front.expected_cqes;
                    shard.fence_stage(format!(
                        "LV2 io_uring recovery failed before queueing: {error}"
                    ));
                    return Err(error);
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
        let mut reorder = StageReorder::new(metrics.clone());

        loop {
            if shard.stage_fault_reason().is_some() {
                Self::quiesce_uring_fifo_after_stage_fault(&ring, &mut fifo);
                return;
            }
            // 1. Submit a held-back batch (SQ was full) once a FIFO slot frees.
            if let Some(mut batch) = pending_submit.take() {
                if fifo.len() < depth {
                    batch.write_start = Instant::now();
                    match Self::submit_batch_nowait(
                        &ring,
                        &shard,
                        &batch,
                        data_fd,
                        data_base,
                        ckpt_target,
                    ) {
                        Ok(true) => fifo.push_back(batch),
                        Ok(false) => pending_submit = Some(batch),
                        Err(error) => {
                            shard.fence_stage(format!(
                                "LV2 io_uring batch submission failed before queueing: {error}"
                            ));
                        }
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
                    let more = match shard.drain_staged_capped(&mut reorder, room) {
                        Ok(entries) => entries,
                        Err(error) => {
                            tracing::error!(error = %error, "LV2 staging reorder failed");
                            continue;
                        }
                    };
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
                    match Self::submit_batch_nowait(
                        &ring,
                        &shard,
                        &batch,
                        data_fd,
                        data_base,
                        ckpt_target,
                    ) {
                        Ok(true) => fifo.push_back(batch),
                        Ok(false) => pending_submit = Some(batch),
                        Err(error) => {
                            shard.fence_stage(format!(
                                "LV2 io_uring batch submission failed before queueing: {error}"
                            ));
                        }
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
                let can_accumulate = shard.stage_input_ready(&reorder)
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
                && shard.stage_shutdown_complete(&reorder)
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
    pub(in crate::buffer::commit_log) fn resolve_global_prepared_queue_depth(
        configured: usize,
        member_count: usize,
    ) -> usize {
        if configured == 0 {
            member_count.max(1)
        } else {
            configured
        }
    }

    pub(in crate::buffer::commit_log) fn resolve_global_write_lane_count(
        configured: usize,
        member_count: usize,
    ) -> usize {
        // ONE LANE PER SHARD. The previous default capped this at 8 to "match
        // the eight foreground ublk queues"; that queue model was replaced by a
        // single shared io-worker pool in 086a47a, so the cap only paired two
        // shards onto one lane with no work stealing.
        //
        // Box-measured 2026-08-25, RWMIX=0 QD256 j16d16 on an aged 256 GiB
        // volume, three arms 8/16/8 each with its own 430 s burn (the knob is
        // read at pool open, so arms cannot be interleaved). 16 shards:
        //
        //   metric                  lanes=8   lanes=16   lanes=8
        //   append_total          8570 us    5386 us    9153 us   -39%
        //   wait_durable mean     6556 us    2938 us    7064 us   -57%
        //   wait_durable p99     37749 us   12059 us   39846 us   -68%
        //   entry_write           3304 us    1091 us    3561 us   -67%
        //   prepared_queue        1503 us     448 us    1627 us   -70%
        //   lane epochs            758 k     1792 k      738 k    2.4x
        //
        // The two 8-arms bracket the 16-arm and agree within 7-8%, so a 2.3-3.3x
        // move is the knob, not this box's drift. ⚠ THROUGHPUT IS FLAT
        // (610 / 568 / 579 MB/s, inside the 8-arms' own 5.3% spread) and drive
        // util stayed ~20%: in-flight appends fell 159 -> 93 at fixed QD256, so
        // the appends stopped being the queue and the demand is now held
        // elsewhere. This lands for the latency, not for bandwidth.
        //
        // ⚠ INVARIANT: a shard maps to a lane by `shard_idx % lanes`, so every
        // value keeps a shard's batches on ONE lane. The LV2 durability
        // watermark is a PREFIX marker, so two lanes publishing one shard's
        // batches out of seq order would ack an append whose payload is not on
        // the device yet. Do not replace the per-lane channels with a shared
        // queue without adding per-shard reordering at publish.
        if configured == 0 {
            member_count.max(1)
        } else {
            configured.max(1)
        }
    }

    pub(super) fn global_sync_loop(
        root_device: Arc<dyn BlockBackend>,
        members: Vec<(u64, Arc<BufferShard>, Receiver<()>)>,
        group_commit_wait: Duration,
        lv2_prepared_queue_depth_per_lane: usize,
        lv2_write_lanes: usize,
        checkpoint_epoch_interval: usize,
        shutdown: Arc<AtomicBool>,
        metrics: Arc<OnceLock<Arc<EngineMetrics>>>,
        packed_checkpoint: Option<Arc<parking_lot::Mutex<PackedCheckpointState>>>,
    ) {
        struct PreparedBatch {
            member_idx: usize,
            all: Vec<StagedEntry>,
            spans: Vec<CoalescedSpan>,
            checkpoint: ShardCheckpoint,
            started: Instant,
            #[cfg(any(test, feature = "diagnostic-metrics"))]
            prepared_at: Instant,
        }

        struct WrittenBatch {
            member_idx: usize,
            all: Vec<StagedEntry>,
            max_seq: u64,
            checkpoint: ShardCheckpoint,
            started: Instant,
            /// Stamped when the lane hands the batch to the coordinator. The
            /// lane->coord queue and the coordinator's serial service were the
            /// only segments of `append_wait_durable` with no counter, and they
            /// are where 40% of it hid (box 2026-08-25: 2.80 ms of a 7.02 ms
            /// mean unaccounted, measured with UPPER-BOUND means for every other
            /// stage).
            #[cfg(any(test, feature = "diagnostic-metrics"))]
            written_at: Instant,
        }

        let queue_depth = members.len().max(1);
        let prepared_queue_depth = Self::resolve_global_prepared_queue_depth(
            lv2_prepared_queue_depth_per_lane,
            members.len(),
        );
        // Independent lanes overlap root writes while the device-wide durability
        // coordinator publishes completed epochs. The group-commit wait lives
        // only here (not in shard preparation or the coordinator), so each
        // request pays one window rather than three.
        let write_lane_count =
            Self::resolve_global_write_lane_count(lv2_write_lanes, members.len());
        tracing::info!(
            write_lane_count,
            prepared_queue_depth,
            written_queue_depth = queue_depth,
            "LV2 global sync queue topology"
        );
        let write_lanes: Vec<_> = (0..write_lane_count)
            .map(|_| bounded::<PreparedBatch>(prepared_queue_depth))
            .collect();
        let member_bases = Arc::new(members.iter().map(|member| member.0).collect::<Vec<_>>());
        let (written_tx, written_rx) = bounded::<Vec<WrittenBatch>>(queue_depth);
        thread::scope(|scope| {
            for (member_idx, (_, shard, wake_rx)) in members.iter().enumerate() {
                let tx = write_lanes[member_idx % write_lane_count].0.clone();
                let shard = shard.clone();
                let wake_rx = wake_rx.clone();
                let shutdown = shutdown.clone();
                let metrics = metrics.clone();
                scope.spawn(move || {
                    crate::affinity::bind_current(
                        crate::affinity::ThreadRole::BufferSync,
                        member_idx,
                    );
                    let mut reorder = StageReorder::new(metrics.clone());
                    loop {
                        if shard.stage_fault_reason().is_some() {
                            return;
                        }
                        if !shard.stage_input_ready(&reorder) {
                            #[cfg(any(test, feature = "diagnostic-metrics"))]
                            let idle_started = Instant::now();
                            let woken = wake_rx.recv_timeout(Duration::from_millis(50));
                            crate::diagnostic_metrics! {
                                if let Some(metrics) = metrics.get() {
                                    metrics.buffer_lv2_prepare_idle_ns.fetch_add(
                                        idle_started.elapsed().as_nanos() as u64,
                                        Ordering::Relaxed,
                                    );
                                }
                            }
                            match woken {
                                Ok(()) => {}
                                Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {
                                    if shutdown.load(Ordering::Relaxed)
                                        && shard.stage_shutdown_complete(&reorder)
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
                        let all = match shard.drain_staged_limited(&mut reorder) {
                            Ok(entries) => entries,
                            Err(error) => {
                                tracing::error!(
                                    member_idx,
                                    error = %error,
                                    "global LV2 staging reorder failed"
                                );
                                return;
                            }
                        };
                        #[cfg(any(test, feature = "diagnostic-metrics"))]
                        let drained_at = Instant::now();
                        if all.is_empty() {
                            continue;
                        }
                        crate::diagnostic_metrics! {
                            if let Some(metrics) = metrics.get() {
                                for entry in &all {
                                    metrics.record_buffer_lv2_staging_queue_ns(
                                        drained_at
                                            .saturating_duration_since(entry.staged_at)
                                            .as_nanos() as u64,
                                    );
                                }
                            }
                        }
                        let persist = {
                            let lifecycle = shard.lifecycle.lock();
                            all.iter()
                                .filter(|entry| !lifecycle.cancelled.contains(&entry.pending.seq))
                                .cloned()
                                .collect::<Vec<_>>()
                        };
                        let spans = loop {
                            match Self::encode_entries_into_spans(&persist, shard.lv2_arena()) {
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
                        let mut checkpoint = shard.snapshot_checkpoint();
                        checkpoint.max_seq = max_seq;
                        // `built_at` closes the build segment and opens the send
                        // segment: a full lane queue blocks here, which is the
                        // signal that the write lanes are the constraint.
                        #[cfg(any(test, feature = "diagnostic-metrics"))]
                        let built_at = Instant::now();
                        let sent = tx.send(PreparedBatch {
                            member_idx,
                            all,
                            spans,
                            checkpoint,
                            started,
                            #[cfg(any(test, feature = "diagnostic-metrics"))]
                            prepared_at: built_at,
                        });
                        if let Some(metrics) = metrics.get() {
                            crate::diagnostic_metrics! {
                                metrics.buffer_lv2_prepare_build_ns.fetch_add(
                                    built_at.saturating_duration_since(started).as_nanos() as u64,
                                    Ordering::Relaxed,
                                );
                                metrics.buffer_lv2_prepare_send_block_ns.fetch_add(
                                    built_at.elapsed().as_nanos() as u64,
                                    Ordering::Relaxed,
                                );
                            }
                            metrics
                                .buffer_lv2_prepare_batches
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        if sent.is_err() {
                            return;
                        }
                        if shutdown.load(Ordering::Relaxed)
                            && shard.stage_shutdown_complete(&reorder)
                        {
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
                    #[cfg(any(test, feature = "diagnostic-metrics"))]
                    let mut payload_profile_sequence = 0u64;
                    loop {
                        #[cfg(any(test, feature = "diagnostic-metrics"))]
                        let idle_started = Instant::now();
                        let Ok(first) = lane_rx.recv() else {
                            return;
                        };
                        crate::diagnostic_metrics! {
                            if let Some(metrics) = metrics.get() {
                                metrics.buffer_lv2_lane_idle_ns.fetch_add(
                                    idle_started.elapsed().as_nanos() as u64,
                                    Ordering::Relaxed,
                                );
                                metrics.record_buffer_lv2_prepared_queue_ns(
                                    first.prepared_at.elapsed().as_nanos() as u64,
                                );
                            }
                        }
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
                                    Ok(batch) => {
                                        crate::diagnostic_metrics! {
                                            if let Some(metrics) = metrics.get() {
                                                metrics.record_buffer_lv2_prepared_queue_ns(
                                                    batch.prepared_at.elapsed().as_nanos() as u64,
                                                );
                                            }
                                        }
                                        prepared.push(batch);
                                    }
                                    Err(RecvTimeoutError::Timeout) => break,
                                    Err(RecvTimeoutError::Disconnected) => break,
                                }
                            }
                        }
                        crate::diagnostic_metrics! {
                            if let Some(metrics) = metrics.get() {
                                BufferShard::record_metric(
                                    &metrics.buffer_sync_sleep_ns,
                                    collect_started,
                                );
                            }
                        }
                        while let Ok(batch) = lane_rx.try_recv() {
                            crate::diagnostic_metrics! {
                                if let Some(metrics) = metrics.get() {
                                    metrics.record_buffer_lv2_prepared_queue_ns(
                                        batch.prepared_at.elapsed().as_nanos() as u64,
                                    );
                                }
                            }
                            prepared.push(batch);
                        }
                        crate::diagnostic_metrics! {
                            if let Some(metrics) = metrics.get() {
                                let collect_ns = collect_started.elapsed().as_nanos() as u64;
                                metrics.record_buffer_lv2_group_collect_ns(collect_ns);
                                metrics.buffer_lv2_lane_collect_ns
                                    .fetch_add(collect_ns, Ordering::Relaxed);
                            }
                        }

                        let mut consecutive_failures = 0u32;
                        loop {
                            #[cfg(any(test, feature = "diagnostic-metrics"))]
                            let opsbuild_started = Instant::now();
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
                            crate::diagnostic_metrics! {
                                if let Some(metrics) = metrics.get() {
                                    metrics.buffer_lv2_lane_opsbuild_ns.fetch_add(
                                        write_started
                                            .saturating_duration_since(opsbuild_started)
                                            .as_nanos() as u64,
                                        Ordering::Relaxed,
                                    );
                                }
                            }
                            #[cfg(any(test, feature = "diagnostic-metrics"))]
                            let profile_this_write =
                                should_sample_lv2_payload_write(&mut payload_profile_sequence);
                            #[cfg(any(test, feature = "diagnostic-metrics"))]
                            let cpu_started = profile_this_write.then(thread_cpu_time).flatten();
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
                                    crate::diagnostic_metrics! {
                                        if let Some(metrics) = metrics.get() {
                                            BufferShard::record_metric(
                                                &metrics.buffer_append_log_write_ns,
                                                write_started,
                                            );
                                            metrics.record_buffer_lv2_payload_write_ns(
                                                write_elapsed.as_nanos() as u64,
                                            );
                                        // Failed attempts and their retry backoff
                                        // are excluded; both are zero on a healthy
                                        // device, so the lane ledger still closes.
                                        metrics.buffer_lv2_lane_write_ns.fetch_add(
                                            write_elapsed.as_nanos() as u64,
                                            Ordering::Relaxed,
                                        );
                                        // Same latency, ENTRY-weighted. The
                                        // per-epoch histogram above answers
                                        // "how long is an epoch write"; an
                                        // append waits for the epoch it landed
                                        // in, and big epochs hold more entries
                                        // AND take longer, so the epoch-weighted
                                        // mean understates what an average entry
                                        // pays. That inspection bias is what
                                        // left 37% of `append_wait_durable`
                                        // looking unaccounted.
                                        crate::diagnostic_metrics! {
                                            let epoch_entries: usize =
                                                prepared.iter().map(|b| b.all.len()).sum();
                                            for _ in 0..epoch_entries {
                                                metrics.record_buffer_lv2_entry_write_ns(
                                                    write_elapsed.as_nanos() as u64,
                                                );
                                            }
                                        }
                                            if let Some(cpu_elapsed) = cpu_started.and_then(|started| {
                                                thread_cpu_time().map(|now| now.saturating_sub(started))
                                            }) {
                                                metrics.record_buffer_lv2_payload_profile(
                                                    write_elapsed.as_nanos() as u64,
                                                    cpu_elapsed.as_nanos() as u64,
                                                );
                                            }
                                        }
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

                        #[cfg(any(test, feature = "diagnostic-metrics"))]
                        let written_at = Instant::now();
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
                                    #[cfg(any(test, feature = "diagnostic-metrics"))]
                                    written_at,
                                }
                            })
                            .collect();
                        // A full `written` queue means the single durability
                        // coordinator is the constraint, not this lane.
                        #[cfg(any(test, feature = "diagnostic-metrics"))]
                        let send_started = Instant::now();
                        let sent = written_tx.send(written);
                        if let Some(metrics) = metrics.get() {
                            crate::diagnostic_metrics! {
                                metrics.buffer_lv2_lane_send_block_ns.fetch_add(
                                    send_started.elapsed().as_nanos() as u64,
                                    Ordering::Relaxed,
                                );
                            }
                            metrics
                                .buffer_lv2_lane_epochs
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        if sent.is_err() {
                            return;
                        }
                    }
                });
            }
            drop(written_tx);

            let mut consecutive_failures = 0u32;
            // Epochs since the last packed-checkpoint page write. Coordinator-local:
            // this is the only thread that advances hot-path generations.
            let mut epochs_since_checkpoint = 0usize;
            loop {
                // Everything from here to `publish` is serial and single
                // threaded: every acknowledged append waits behind it, so
                // `1 - idle/interval` is the ceiling this stage can sustain.
                #[cfg(any(test, feature = "diagnostic-metrics"))]
                let idle_started = Instant::now();
                let Ok(mut batches) = written_rx.recv() else {
                    break;
                };
                #[cfg(any(test, feature = "diagnostic-metrics"))]
                let recv_at = Instant::now();
                #[cfg(any(test, feature = "diagnostic-metrics"))]
                let idle_ns = recv_at.saturating_duration_since(idle_started).as_nanos() as u64;
                if let Some(metrics) = metrics.get() {
                    metrics
                        .buffer_lv2_prepare_threads
                        .store(members.len() as u64, Ordering::Relaxed);
                    metrics
                        .buffer_lv2_lane_threads
                        .store(write_lane_count as u64, Ordering::Relaxed);
                }
                // A lane already formed the complete durability epoch for its
                // root write. Drain any sibling epochs that finished in the
                // meantime, then publish the shared barrier immediately.
                crate::diagnostic_metrics! {
                    if let Some(metrics) = metrics.get() {
                        for batch in &batches {
                            metrics.record_buffer_lv2_written_queue_ns(
                                recv_at.saturating_duration_since(batch.written_at).as_nanos() as u64,
                            );
                        }
                    }
                }
                while let Ok(epoch) = written_rx.try_recv() {
                    crate::diagnostic_metrics! {
                        if let Some(metrics) = metrics.get() {
                            let drained_at = Instant::now();
                            for batch in &epoch {
                                metrics.record_buffer_lv2_written_queue_ns(
                                    drained_at.saturating_duration_since(batch.written_at).as_nanos()
                                        as u64,
                                );
                            }
                        }
                    }
                    batches.extend(epoch);
                }

                let epoch_started = batches
                    .iter()
                    .map(|batch| batch.started)
                    .min()
                    .expect("durability epoch has at least one batch");
                let epoch_entries = batches.iter().map(|batch| batch.all.len()).sum::<usize>();

                // Serialize table generations against explicit clean-shutdown
                // persistence. The mutex is otherwise uncontended: only this
                // coordinator advances hot-path generations.
                let mut packed_guard = packed_checkpoint.as_ref().map(|state| state.lock());
                // EVERY epoch folds its shards' positions into `pending`; only
                // every `checkpoint_epoch_interval`-th epoch encodes and writes
                // the page. Box-measured 2026-08-01: that 4 KiB mirrored write
                // cost 101 us and 44 % of this single thread's whole capacity at
                // 4387 epochs/s, with the thread 79 % busy — the front-end
                // ceiling in the healthy regime.
                //
                // Safe because the page is a RECOVERY START POINT, not a
                // durability record: the payloads it describes were written by
                // the lanes above and are made durable by this epoch's own
                // `sync_device_impl` either way, and `read_packed_checkpoint`
                // has exactly one runtime caller — pool open. A generation that
                // lags N epochs costs replay distance, never correctness. Clean
                // shutdown still writes a complete table (`persist_checkpoints`
                // re-snapshots every shard, so it never reads `pending`).
                epochs_since_checkpoint = epochs_since_checkpoint.saturating_add(1);
                let write_checkpoint = epochs_since_checkpoint >= checkpoint_epoch_interval;
                let pending_generation = if let Some(state) = packed_guard.as_mut() {
                    for batch in &batches {
                        state.fold_pending(batch.member_idx, batch.checkpoint);
                    }
                    if !write_checkpoint {
                        None
                    } else {
                        let mut prepare_failures = 0u32;
                        Some(loop {
                            let prepared = state.next_generation().and_then(|generation| {
                                state.encode_pending(generation).map(|()| generation)
                            });
                            match prepared {
                                Ok(generation) => break generation,
                                Err(error) => {
                                    prepare_failures = prepare_failures.saturating_add(1);
                                    tracing::error!(
                                        error = %error,
                                        prepare_failures,
                                        "global packed checkpoint encode failed; retrying epoch"
                                    );
                                    thread::sleep(Self::sync_retry_backoff(prepare_failures));
                                }
                            }
                        })
                    }
                } else {
                    None
                };
                if write_checkpoint {
                    epochs_since_checkpoint = 0;
                }
                if pending_generation.is_none() {
                    if let Some(metrics) = metrics.get() {
                        metrics
                            .buffer_lv2_checkpoint_skipped_epochs
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                // Covers everything between the first written batch arriving and
                // the checkpoint page write: the sibling-epoch drain, the packed
                // checkpoint mutex, `begin_next`, and `encode_pending`.
                #[cfg(any(test, feature = "diagnostic-metrics"))]
                let ckpt_encode_ns = recv_at.elapsed().as_nanos() as u64;
                #[cfg(any(test, feature = "diagnostic-metrics"))]
                let mut ckpt_write_ns = 0u64;
                #[cfg(any(test, feature = "diagnostic-metrics"))]
                let mut flush_ns = 0u64;

                loop {
                    #[cfg(any(test, feature = "diagnostic-metrics"))]
                    let checkpoint_started = Instant::now();
                    let checkpoint_result = match pending_generation {
                        Some(generation) => Self::write_packed_checkpoint_page(
                            root_device.as_ref(),
                            generation,
                            packed_guard
                                .as_ref()
                                .expect("packed generation has state")
                                .scratch
                                .as_slice(),
                        ),
                        None => Ok(()),
                    };
                    #[cfg(any(test, feature = "diagnostic-metrics"))]
                    let checkpoint_elapsed = checkpoint_started.elapsed();
                    crate::diagnostic_metrics! {
                        ckpt_write_ns =
                            ckpt_write_ns.saturating_add(checkpoint_elapsed.as_nanos() as u64);
                    }
                    if let Err(err) = checkpoint_result {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        tracing::warn!(
                            error = %err,
                            consecutive_failures,
                            checkpoint_pages = usize::from(pending_generation.is_some()),
                            "global persistent slot checkpoint write failed; retrying epoch"
                        );
                        thread::sleep(Self::sync_retry_backoff(consecutive_failures));
                        continue;
                    }
                    crate::diagnostic_metrics! {
                        if let Some(metrics) = metrics.get() {
                            BufferShard::record_metric(
                                &metrics.buffer_append_log_write_ns,
                                checkpoint_started,
                            );
                            if pending_generation.is_some() {
                                metrics.record_buffer_lv2_checkpoint_write_ns(
                                    checkpoint_elapsed.as_nanos() as u64,
                                );
                            }
                        }
                    }

                    // All payload and checkpoint writes in `batches` completed.
                    // A device-wide flush now makes that whole prefix durable.
                    // Workers may issue later writes concurrently; those remain
                    // unacknowledged until a subsequent flush.
                    let flush_started = Instant::now();
                    let result = Self::sync_device_impl(root_device.as_ref());
                    let flush_elapsed = flush_started.elapsed();
                    crate::diagnostic_metrics! {
                        flush_ns = flush_ns.saturating_add(flush_elapsed.as_nanos() as u64);
                    }
                    match result {
                        Ok(()) => {
                            consecutive_failures = 0;
                            if let (Some(state), Some(generation)) =
                                (packed_guard.as_mut(), pending_generation)
                            {
                                state.commit_pending(generation);
                            }
                            if let Some(metrics) = metrics.get() {
                                metrics.buffer_sync_flushes.fetch_add(1, Ordering::Relaxed);
                                crate::diagnostic_metrics! {
                                    metrics.record_buffer_lv2_root_flush_ns(
                                        flush_elapsed.as_nanos() as u64
                                    );
                                }
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

                // Per-entry watermark advance and ready-publish, still on the
                // single coordinator thread and still ahead of the next epoch.
                #[cfg(any(test, feature = "diagnostic-metrics"))]
                let publish_started = Instant::now();
                for batch in batches {
                    let shard = &members[batch.member_idx].1;
                    let max_seq = batch
                        .all
                        .iter()
                        .map(|entry| entry.pending.seq)
                        .max()
                        .unwrap_or(0);
                    crate::diagnostic_metrics! {
                        let advance_at = Instant::now();
                        let advanced_at_ns = lv2_metric_timestamp_ns(advance_at);
                        for entry in &batch.all {
                            entry.pending.durability_advanced_at_ns
                                .store(advanced_at_ns, Ordering::Release);
                        }
                        if let Some(metrics) = metrics.get() {
                            // The whole in-pipeline span, per entry: staged until
                            // this entry's watermark advance. `append_wait_durable`
                            // minus `watermark_dispatch` must equal this, so it is
                            // the anchor the per-stage ledger has to add up to.
                            for entry in &batch.all {
                                metrics.record_buffer_lv2_staged_to_durable_ns(
                                    advance_at.saturating_duration_since(entry.staged_at).as_nanos()
                                        as u64,
                                );
                            }
                        }
                    }
                    if !shard.lv2_durability.advance(max_seq) {
                        continue;
                    }
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
                    crate::diagnostic_metrics! {
                        if let Some(metrics) = metrics.get() {
                            // Lane write done -> this batch's watermark advance.
                            // Together with `written_queue` this splits the
                            // coordinator's contribution into "waiting for the
                            // coordinator" and "the coordinator's own serial work".
                            metrics.record_buffer_lv2_written_to_durable_ns(
                                batch.written_at.elapsed().as_nanos() as u64,
                            );
                        }
                    }
                    for entry in &batch.all {
                        shard.publish_ready(entry.pending.seq);
                    }
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
                        crate::diagnostic_metrics! {
                            BufferShard::record_metric(&metrics.buffer_sync_batch_ns, batch.started);
                        }
                    }
                }
                crate::diagnostic_metrics! {
                    if let Some(metrics) = metrics.get() {
                        metrics.record_buffer_lv2_coord_epoch(
                            idle_ns,
                            ckpt_encode_ns,
                            ckpt_write_ns,
                            flush_ns,
                            publish_started.elapsed().as_nanos() as u64,
                        );
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
        let mut reorder = StageReorder::new(metrics.clone());
        let batch_wait = if group_commit_wait.is_zero() {
            Duration::from_millis(1)
        } else {
            group_commit_wait
        };

        loop {
            if shard.stage_fault_reason().is_some() {
                return;
            }
            if inflight.is_empty() {
                if !shard.stage_input_ready(&reorder) {
                    match wake_rx.recv_timeout(Duration::from_millis(50)) {
                        Ok(()) => {}
                        Err(RecvTimeoutError::Timeout) => {
                            if shutdown.load(Ordering::Relaxed)
                                && shard.stage_shutdown_complete(&reorder)
                            {
                                return;
                            }
                            continue;
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            if shutdown.load(Ordering::Relaxed)
                                && shard.stage_shutdown_complete(&reorder)
                            {
                                return;
                            }
                            continue;
                        }
                    }
                    while wake_rx.try_recv().is_ok() {}
                    if !batch_wait.is_zero() {
                        #[cfg(any(test, feature = "diagnostic-metrics"))]
                        let sleep_start = Instant::now();
                        thread::sleep(batch_wait);
                        crate::diagnostic_metrics! {
                            if let Some(metrics) = metrics.get() {
                                BufferShard::record_metric(
                                    &metrics.buffer_sync_sleep_ns,
                                    sleep_start,
                                );
                            }
                        }
                        while wake_rx.try_recv().is_ok() {}
                    }
                }

                inflight = match shard.drain_staged_limited(&mut reorder) {
                    Ok(entries) => entries,
                    Err(error) => {
                        tracing::error!(error = %error, "LV2 staging reorder failed");
                        return;
                    }
                };
                if inflight.is_empty() {
                    if shutdown.load(Ordering::Relaxed) && shard.stage_shutdown_complete(&reorder) {
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

            #[cfg(any(test, feature = "diagnostic-metrics"))]
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
                        &shard,
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
                    // Advance the LV2 fdatasync watermark, then publish every
                    // durable entry. Sync owns this publication so a dropped
                    // deferred ticket cannot strand an entry outside flusher.
                    let batch_max_durable = inflight
                        .iter()
                        .map(|entry| entry.pending.seq)
                        .max()
                        .unwrap_or(0);
                    let advanced_at_ns = lv2_metric_timestamp_ns(Instant::now());
                    for entry in &inflight {
                        entry
                            .pending
                            .durability_advanced_at_ns
                            .store(advanced_at_ns, Ordering::Release);
                    }
                    if !shard.lv2_durability.advance(batch_max_durable) {
                        return;
                    }
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
                    for entry in &inflight {
                        shard.publish_ready(entry.pending.seq);
                    }
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

            crate::diagnostic_metrics! {
                if let Some(metrics) = metrics.get() {
                    BufferShard::record_metric(&metrics.buffer_sync_batch_ns, batch_start);
                }
            }
        }
    }
}

#[cfg(test)]
mod span_encode_tests {
    use super::*;
    use crate::mem::{MemRole, SlabArena};
    use std::sync::atomic::AtomicU64;

    /// Byte the arena slot is poisoned with. Distinct from the payload pattern
    /// so a survivor is unambiguous.
    const POISON: u8 = 0xAA;
    const PAYLOAD_BYTE: u8 = 0x5A;

    fn staged(seq: u64, lba: u64, payload_len: usize, disk_offset: u64) -> StagedEntry {
        let payload: Arc<[u8]> = vec![PAYLOAD_BYTE; payload_len].into();
        let raw = BufferEntry::raw_size_for("span-vol", payload.len());
        let disk_len = round_up(raw, BLOCK_SIZE as usize) as u32;
        let pending = Arc::new(PendingEntry {
            seq,
            vol_id: "span-vol".to_string(),
            start_lba: Lba(lba),
            lba_count: 1,
            payload_crc32: crc32fast::hash(&payload),
            vol_created_at: 7,
            relocation_source: None,
            payload: Some(payload.clone()),
            disk_offset,
            disk_len,
            enqueued_at: Instant::now(),
            durability_advanced_at_ns: AtomicU64::new(0),
            superseded_ranges: Vec::new(),
        });
        StagedEntry {
            pending,
            payload,
            staged_at: Instant::now(),
        }
    }

    /// Two contiguous entries, so they coalesce into one span, plus a
    /// discontiguous third.
    fn three_entries() -> Vec<StagedEntry> {
        let first = staged(1, 0, 4096, 0);
        let first_len = first.pending.disk_len as u64;
        let second = staged(2, 1, 8192, first_len);
        let second_len = second.pending.disk_len as u64;
        // Leave a gap so this one starts its own span.
        let third = staged(3, 2, 4096, first_len + second_len + 4096);
        vec![first, second, third]
    }

    fn arena() -> Arc<SlabArena> {
        SlabArena::new(MemRole::Lv2Sync, 8 * 1024 * 1024, 64, false, None)
    }

    /// ⭐ THE decisive test for dropping `alloc_zeroed` on this path: an arena
    /// slot comes back dirty (arena invariant 3), so if the encoder did not
    /// cover every byte it submits, the previous user's bytes would reach LV2.
    ///
    /// The poison is planted in the very slots the encode then takes: take and
    /// dirty buffers of the same sizes, drop them so they return to their class
    /// free stacks, then encode and assert not one poison byte survives inside
    /// any span's submitted range.
    #[test]
    fn arena_span_encode_covers_every_submitted_byte() {
        let arena = arena();
        let entries = three_entries();

        // Plant the poison in the classes the spans will ask for.
        let sizes: Vec<usize> = vec![
            (entries[0].pending.disk_len + entries[1].pending.disk_len) as usize,
            entries[2].pending.disk_len as usize,
        ];
        for size in &sizes {
            let mut dirty = arena.take(*size).unwrap();
            dirty.as_mut_slice().fill(POISON);
        }
        assert_eq!(arena.live_slots(), 0, "poison buffers must be released");

        let spans =
            WriteBufferPool::encode_entries_into_spans(&entries, Some(&arena)).unwrap();
        assert_eq!(spans.len(), 2, "two contiguous entries must share one span");

        for span in &spans {
            let submitted = &span.buf.as_slice()[..span.len as usize];
            assert!(
                !submitted.contains(&POISON),
                "a recycled slot's byte reached the submitted range at offset {}",
                span.offset
            );
        }
        assert_eq!(
            spans.iter().map(|s| s.len as usize).sum::<usize>(),
            sizes.iter().sum::<usize>(),
            "span lengths must be the sum of the entries' disk_len"
        );
    }

    /// Invariant 2: the class-rounded slot width must never reach the device.
    /// `CoalescedSpan::len` is what every submit path uses, so it must stay the
    /// requested length even when the slot behind it is wider.
    #[test]
    fn span_len_is_the_request_not_the_slot_width() {
        let arena = arena();
        // 4 KiB + 8 KiB + headers rounds to a size the class table serves
        // exactly, so ask for something that must land in a wider class: a
        // 3-block span against the exact 1..16-block classes stays exact, so
        // use a large odd span instead.
        let entry = staged(1, 0, 17 * 4096, 0);
        let expected = entry.pending.disk_len as usize;
        let spans =
            WriteBufferPool::encode_entries_into_spans(&[entry], Some(&arena)).unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].len as usize, expected);
        assert!(
            spans[0].buf.len() >= expected,
            "capacity may be wider than the request, never narrower"
        );
    }

    /// The arena path and the heap path must produce identical bytes — that is
    /// what makes `mem-arena-lv2 off` a valid A/B baseline rather than a
    /// different workload.
    #[test]
    fn arena_and_heap_paths_encode_identical_bytes() {
        let arena = arena();
        let entries = three_entries();
        let with_arena =
            WriteBufferPool::encode_entries_into_spans(&entries, Some(&arena)).unwrap();
        let with_heap = WriteBufferPool::encode_entries_into_spans(&entries, None).unwrap();
        assert_eq!(with_arena.len(), with_heap.len());
        for (a, h) in with_arena.iter().zip(with_heap.iter()) {
            assert_eq!(a.offset, h.offset);
            assert_eq!(a.len, h.len);
            assert_eq!(
                &a.buf.as_slice()[..a.len as usize],
                &h.buf.as_slice()[..h.len as usize],
                "arena and heap encodes must be byte-identical at offset {}",
                a.offset
            );
        }
    }

    /// Span buffers outlive the encoding thread in the pipelined sync path
    /// (`InflightUringBatch` holds them until the kernel harvests the chain),
    /// so releasing one from a foreign thread has to return the slot.
    #[test]
    fn spans_release_their_slots_from_a_foreign_thread() {
        let arena = arena();
        let spans =
            WriteBufferPool::encode_entries_into_spans(&three_entries(), Some(&arena)).unwrap();
        assert_eq!(arena.live_slots(), 2);
        std::thread::spawn(move || drop(spans)).join().unwrap();
        assert_eq!(
            arena.live_slots(),
            0,
            "a foreign-thread drop must still return the slot"
        );
    }
}

#[cfg(test)]
mod payload_profile_tests {
    use super::should_sample_lv2_payload_write;

    #[test]
    fn payload_profile_samples_exactly_one_in_sixty_four_writes() {
        let mut sequence = 0;
        let sampled = (1..=128)
            .filter(|_| should_sample_lv2_payload_write(&mut sequence))
            .collect::<Vec<_>>();
        assert_eq!(sampled, vec![64, 128]);
    }
}
