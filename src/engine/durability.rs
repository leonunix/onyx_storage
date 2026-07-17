use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crate::affinity::{self, ThreadRole};
use crate::buffer::pool::WriteBufferPool;
use crate::meta::store::{DurableCheckpointOutcome, MetaStore};

/// Handle for the durability-watermark background thread.
///
/// Owns the only path that advances `durable_seq` and the only path that
/// releases buffer-ring slots. Engine shutdown joins it before marking the
/// LV3 superblock clean.
pub(super) struct DurabilityWatermarkHandle {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl DurabilityWatermarkHandle {
    /// Spawn the watermark thread.
    ///
    /// Post-WAL invariant: metadb has no journal of its own; the buffer
    /// ring entries are the **only** durable record of commits until a
    /// metadb checkpoint folds their effects into manifest pages. So
    /// `durable_seq` only advances to the exact buffer frontier returned by a
    /// committed metadb manifest, and ring slots are reclaimed via
    /// `pool.release_below(buffer_seq)` at exactly the same moment. The
    /// requested frontier comes from [`WriteBufferPool::applied_frontier`],
    /// never from `max_flushed_seq`: the latter can jump over an older apply
    /// still pending on another shard.
    ///
    /// `checkpoint_interval` is the periodic cadence; if `dirty_pages_threshold`
    /// is non-zero, in-memory L2P/RC pressure can trigger an early checkpoint
    /// between ticks.
    pub(super) fn start(
        meta: Arc<MetaStore>,
        buffer_pool: Arc<WriteBufferPool>,
        durable_seq: Arc<AtomicU64>,
        checkpoint_interval: std::time::Duration,
        dirty_pages_threshold: u64,
        ring_fill_trigger_pct: u8,
    ) -> Self {
        const TICK_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = stop.clone();
        let buffer_pool_thread = buffer_pool.clone();
        let thread = std::thread::Builder::new()
            .name("durability-watermark".into())
            .spawn(move || {
                affinity::bind_current(ThreadRole::Background, 0);
                let mut last_checkpoint = std::time::Instant::now();
                let mut last_checkpoint_request_seq = 0u64;
                let mut pending_checkpoint: Option<(u64, u64)> = None;
                // Consecutive metadb-checkpoint failures. Reset on any success;
                // fences the buffer pool once it reaches FENCE_FAILURE_THRESHOLD
                // (or immediately on a fatal error) — see D6 in the Phase-3 plan.
                let mut consecutive_failures = 0u32;
                while !stop_clone.load(Ordering::Relaxed) {
                    std::thread::sleep(TICK_INTERVAL);

                    if let Some((token, requested_seq)) = pending_checkpoint {
                        match meta.durable_checkpoint_outcome(token) {
                            Ok(Some(DurableCheckpointOutcome::Durable { buffer_seq })) => {
                                consecutive_failures = 0;
                                last_checkpoint_request_seq = buffer_seq;
                                durable_seq.fetch_max(buffer_seq, Ordering::Release);
                                let fill_before =
                                    buffer_pool_thread.physical_fill_percentage();
                                match buffer_pool_thread.release_below_with_stats(buffer_seq) {
                                    Ok((released_entries, released_bytes)) => {
                                        tracing::info!(
                                            token,
                                            requested_seq,
                                            buffer_seq,
                                            released_entries,
                                            released_bytes,
                                            fill_before,
                                            fill_after = buffer_pool_thread
                                                .physical_fill_percentage(),
                                            "durable checkpoint released LV2 ring prefix"
                                        );
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            buffer_seq,
                                            error = %e,
                                            "release_below failed; ring reclaim deferred to next checkpoint"
                                        );
                                    }
                                }
                                pending_checkpoint = None;
                            }
                            Ok(Some(DurableCheckpointOutcome::Skipped)) => {
                                // Non-blocking checkpoint found apply gate busy.
                                // Retry on the next checkpoint interval.
                                last_checkpoint_request_seq = Self::retry_frontier_before(
                                    last_checkpoint_request_seq,
                                    requested_seq,
                                );
                                pending_checkpoint = None;
                            }
                            Ok(None) => {}
                            Err(e) => {
                                let msg = e.to_string();
                                consecutive_failures += 1;
                                let fatal = crate::meta::is_fatal_meta_failure(&msg);
                                tracing::error!(
                                    error = %msg,
                                    consecutive_failures,
                                    fatal,
                                    "durability watermark checkpoint failed; ring reclaim deferred to next cycle"
                                );
                                if Self::should_fence(consecutive_failures, fatal) {
                                    // Stop acking new writes into a ring the dead
                                    // checkpoint path can no longer drain.
                                    buffer_pool_thread.fence_meta(format!(
                                        "metadb checkpoint failed {consecutive_failures}x (last: {msg})"
                                    ));
                                }
                                // The request did not make this frontier
                                // durable. Roll the optimistic request marker
                                // back so the same idle frontier is retried;
                                // otherwise the repeated-failure fence can
                                // never observe a second attempt.
                                last_checkpoint_request_seq = Self::retry_frontier_before(
                                    last_checkpoint_request_seq,
                                    requested_seq,
                                );
                                pending_checkpoint = None;
                            }
                        }
                    }

                    let captured = buffer_pool_thread.applied_frontier();

                    // Threshold-triggered path: when in-memory dirty work
                    // (L2P dirty pages + RC pending deltas) exceeds the
                    // configured cap, kick a checkpoint early instead of
                    // waiting for the periodic tick. Single-outstanding
                    // dispatcher means this only fires effectively while
                    // no checkpoint is active; in steady state the system
                    // self-balances at flush cadence ≈
                    // threshold / dirty-accumulation-rate.
                    let early_trigger = dirty_pages_threshold > 0
                        && pending_checkpoint.is_none()
                        && Self::checkpoint_needed(captured, last_checkpoint_request_seq)
                        && meta.dirty_pages_estimate() as u64 >= dirty_pages_threshold;

                    // LV2 is the sole durable journal between metadb
                    // checkpoints. Use its physical occupancy, rather than
                    // logical pending work, as the capacity-pressure signal.
                    // A large ring can therefore absorb and coalesce writes
                    // until either this watermark or the maximum interval is
                    // reached, without confusing already-applied entries with
                    // free physical slots.
                    let ring_trigger = pending_checkpoint.is_none()
                        && Self::checkpoint_needed(captured, last_checkpoint_request_seq)
                        && Self::ring_checkpoint_needed(
                            buffer_pool_thread.physical_fill_percentage(),
                            ring_fill_trigger_pct,
                        );

                    if !early_trigger
                        && !ring_trigger
                        && last_checkpoint.elapsed() < checkpoint_interval
                    {
                        continue;
                    }
                    last_checkpoint = std::time::Instant::now();
                    if !Self::checkpoint_needed(captured, last_checkpoint_request_seq) {
                        continue;
                    }
                    if pending_checkpoint.is_some() {
                        continue;
                    }
                    let trigger = if early_trigger {
                        "dirty_pages"
                    } else if ring_trigger {
                        "ring_fill"
                    } else {
                        "interval"
                    };
                    meta.set_buffer_applied_watermark(captured);
                    match meta.try_request_durable_checkpoint_token() {
                        Ok(Some(token)) => {
                            last_checkpoint_request_seq = captured;
                            pending_checkpoint = Some((token, captured));
                            tracing::info!(
                                token,
                                trigger,
                                applied_frontier = captured,
                                physical_fill_pct = buffer_pool_thread
                                    .physical_fill_percentage(),
                                dirty_pages = meta.dirty_pages_estimate(),
                                "durability watermark requested metadb checkpoint"
                            );
                        }
                        Ok(None) => {
                            tracing::debug!(
                                applied_frontier = captured,
                                "durability watermark skipped checkpoint; previous one still running"
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                "durability watermark checkpoint request failed; ring reclaim deferred to next cycle"
                            );
                        }
                    }
                }
                // Final checkpoint at shutdown so any committed-but-not-yet-
                // durable seqs get folded into manifest pages before exit.
                let requested_frontier = buffer_pool_thread.applied_frontier();
                meta.set_buffer_applied_watermark(requested_frontier);
                match meta.sync_durable() {
                    Ok(buffer_seq) => {
                        durable_seq.fetch_max(buffer_seq, Ordering::Release);
                        if let Err(e) = buffer_pool_thread.release_below(buffer_seq) {
                            tracing::warn!(
                                buffer_seq,
                                error = %e,
                                "release_below failed during shutdown drain"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            requested_frontier,
                            error = %e,
                            "durability watermark final checkpoint failed at shutdown; ring retained"
                        );
                    }
                }
            })
            .expect("spawn durability-watermark thread");
        Self {
            stop,
            thread: Some(thread),
        }
    }

    /// Consecutive non-fatal checkpoint failures tolerated before the buffer
    /// pool is fenced. Fatal failures (capacity exhausted / persistence
    /// subsystem failed) fence on the first occurrence regardless.
    const FENCE_FAILURE_THRESHOLD: u32 = 3;

    fn checkpoint_needed(captured: u64, last_checkpoint_request_seq: u64) -> bool {
        captured > last_checkpoint_request_seq
    }

    fn retry_frontier_before(last_checkpoint_request_seq: u64, requested_seq: u64) -> u64 {
        last_checkpoint_request_seq.min(requested_seq.saturating_sub(1))
    }

    fn ring_checkpoint_needed(physical_fill_pct: u8, trigger_pct: u8) -> bool {
        trigger_pct > 0 && physical_fill_pct >= trigger_pct.min(100)
    }

    /// Fence decision: a fatal failure fences immediately; otherwise the pool is
    /// fenced only once failures have piled up to the threshold.
    fn should_fence(consecutive_failures: u32, fatal: bool) -> bool {
        fatal || consecutive_failures >= Self::FENCE_FAILURE_THRESHOLD
    }

    pub(super) fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl Drop for DurabilityWatermarkHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::DurabilityWatermarkHandle;

    #[test]
    fn checkpoint_decision_uses_last_request_not_durable_seq() {
        assert!(DurabilityWatermarkHandle::checkpoint_needed(1, 0));
        assert!(DurabilityWatermarkHandle::checkpoint_needed(42, 41));
        assert!(!DurabilityWatermarkHandle::checkpoint_needed(42, 42));
        assert!(!DurabilityWatermarkHandle::checkpoint_needed(41, 42));
    }

    #[test]
    fn failed_or_skipped_checkpoint_retries_same_frontier() {
        let requested = 42;
        let retry_from = DurabilityWatermarkHandle::retry_frontier_before(requested, requested);
        assert_eq!(retry_from, 41);
        assert!(DurabilityWatermarkHandle::checkpoint_needed(
            requested, retry_from
        ));

        // A newer marker must not be allowed to hide the failed frontier.
        assert_eq!(
            DurabilityWatermarkHandle::retry_frontier_before(100, requested),
            41
        );
    }

    #[test]
    fn ring_checkpoint_uses_physical_occupancy_and_can_be_disabled() {
        assert!(!DurabilityWatermarkHandle::ring_checkpoint_needed(100, 0));
        assert!(!DurabilityWatermarkHandle::ring_checkpoint_needed(49, 50));
        assert!(DurabilityWatermarkHandle::ring_checkpoint_needed(50, 50));
        assert!(DurabilityWatermarkHandle::ring_checkpoint_needed(100, 150));
    }

    #[test]
    fn fence_fires_on_fatal_or_repeated_failure() {
        // Fatal fences immediately, even on the first failure.
        assert!(DurabilityWatermarkHandle::should_fence(1, true));
        // Non-fatal only after the threshold is reached.
        assert!(!DurabilityWatermarkHandle::should_fence(1, false));
        assert!(!DurabilityWatermarkHandle::should_fence(2, false));
        assert!(DurabilityWatermarkHandle::should_fence(3, false));
        assert!(DurabilityWatermarkHandle::should_fence(4, false));
    }

    /// End-to-end: a real MetaStore + WriteBufferPool + durability thread. Arm
    /// the checkpoint failpoint so the next checkpoint reports a fatal
    /// `CapacityExhausted`; the durability thread must observe it, fence the
    /// pool, and make `append` fail fast — while reads and reopen stay fine.
    #[test]
    fn durability_thread_fences_pool_on_fatal_checkpoint() {
        use crate::buffer::pool::WriteBufferPool;
        use crate::config::MetaConfig;
        use crate::error::OnyxError;
        use crate::io::device::RawDevice;
        use crate::meta::store::MetaStore;
        use crate::types::Lba;
        use std::sync::Arc;
        use std::time::{Duration, Instant};

        let meta_dir = tempfile::tempdir().unwrap();
        let meta = Arc::new(
            MetaStore::open(&MetaConfig {
                path: Some(meta_dir.path().to_path_buf()),
                block_cache_mb: 16,
                checkpoint_interval_ms: 20,
                dedup_cuckoo_buckets: 100_000,
                dedup_l1_cache_entries: 4096,
                ..MetaConfig::default()
            })
            .unwrap(),
        );

        let size = 4096 + 1024 * 4096;
        let buf = tempfile::NamedTempFile::new().unwrap();
        buf.as_file().set_len(size).unwrap();
        let dev = RawDevice::open_or_create(buf.path(), size).unwrap();
        let pool = Arc::new(
            WriteBufferPool::open_with_options_full(
                dev,
                Duration::from_millis(1),
                1,
                256,
                Duration::ZERO,
                0,
                None,
            )
            .unwrap(),
        );

        // Arm the per-instance failpoint and complete one buffer entry so the
        // contiguous applied frontier makes the watermark thread request a
        // checkpoint.
        meta.arm_checkpoint_capacity_fail();
        let seq = pool.append("vol", Lba(0), 1, &[0u8; 4096], 0).unwrap();
        pool.mark_applied(seq, Lba(0), 1).unwrap();

        let _watermark = DurabilityWatermarkHandle::start(
            meta.clone(),
            pool.clone(),
            pool.durable_seq_handle(),
            Duration::from_millis(20),
            0,
            0,
        );

        // Wait for the fence to trip (should be well under a second).
        let deadline = Instant::now() + Duration::from_secs(5);
        while !pool.is_meta_fenced() {
            assert!(Instant::now() < deadline, "pool was never fenced");
            std::thread::sleep(Duration::from_millis(10));
        }

        // The fence reason carries the fatal error; new appends fail fast.
        assert!(pool
            .meta_fence_reason()
            .unwrap()
            .contains("capacity exhausted"));
        let err = pool.append("vol", Lba(0), 1, &[0u8; 4096], 0).unwrap_err();
        assert!(matches!(err, OnyxError::MetaFenced(_)), "got {err:?}");

        // Reads are never fenced (looking up an unmapped LBA returns Ok(None),
        // not an error).
        assert!(pool.lookup("vol", Lba(0)).unwrap().is_none());
    }
}
