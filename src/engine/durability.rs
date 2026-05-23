use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crate::affinity::{self, ThreadRole};
use crate::buffer::pool::WriteBufferPool;
use crate::meta::store::MetaStore;

/// Handle for the durability-watermark background thread.
///
/// The thread runs until `stop` is signaled, then drains any seqs pending
/// durability by issuing one final sync before joining. Engine shutdown
/// owns the handle and joins it before marking the LV3 superblock clean.
pub(super) struct DurabilityWatermarkHandle {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl DurabilityWatermarkHandle {
    /// Spawn the watermark thread. `checkpoint_interval` is how often the
    /// thread asks metadb to checkpoint dirty metadata pages + commit a
    /// manifest + prune WAL segments. The buffer-ring `durable_seq` is
    /// bumped on a faster cadence ([`RING_BUMP_INTERVAL`]) independent of checkpoint:
    /// every onyx-side `mark_flushed` already implies a metadb commit
    /// returned, which already waited on `wal.submit` → fsync, so the seq
    /// is durable the moment it enters `max_flushed_seq`.
    ///
    /// Decoupling the two cadences fixes a 2026-04-27 soak regression:
    /// the previous design called `meta.sync_durable()` (= `db.flush()`)
    /// every 50 ms, which holds metadb's `apply_gate.write()` exclusively
    /// and blocks every concurrent `commit_ops`. With per-shard apply
    /// lanes already wide open, that exclusive lock had become the new
    /// bottleneck — apply_wait avg 30 ms, ~60% of writer time stalled in
    /// futex_wait on the gate.
    pub(super) fn start(
        meta: Arc<MetaStore>,
        buffer_pool: Arc<WriteBufferPool>,
        max_flushed_seq: Arc<AtomicU64>,
        durable_seq: Arc<AtomicU64>,
        checkpoint_interval: std::time::Duration,
        checkpoint_gates_ring_reclaim: bool,
        dirty_pages_threshold: u64,
    ) -> Self {
        const RING_BUMP_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = stop.clone();
        let shard_count = buffer_pool.shard_count();
        let buffer_pool_thread = buffer_pool.clone();
        let thread = std::thread::Builder::new()
            .name("durability-watermark".into())
            .spawn(move || {
                affinity::bind_current(ThreadRole::Background, 0);
                let mut last_checkpoint = std::time::Instant::now();
                let mut last_checkpoint_request_seq = 0u64;
                let mut pending_checkpoint: Option<(u64, u64)> = None;
                while !stop_clone.load(Ordering::Relaxed) {
                    std::thread::sleep(RING_BUMP_INTERVAL);

                    if let Some((token, seq)) = pending_checkpoint {
                        match meta.durable_checkpoint_outcome(token) {
                            Ok(Some(true)) => {
                                durable_seq.fetch_max(seq, Ordering::Release);
                                for idx in 0..shard_count {
                                    let _ = buffer_pool_thread.advance_tail_for_shard(idx);
                                }
                                pending_checkpoint = None;
                            }
                            Ok(Some(false)) => {
                                // The non-blocking checkpoint found the apply gate busy.
                                // Leave durable_seq behind the unlogged commits and retry
                                // on the next checkpoint interval.
                                last_checkpoint_request_seq =
                                    last_checkpoint_request_seq.min(seq.saturating_sub(1));
                                pending_checkpoint = None;
                            }
                            Ok(None) => {}
                            Err(e) => {
                                tracing::error!(
                                    error = %e,
                                    "durability watermark checkpoint failed; WAL prune deferred to next cycle"
                                );
                                pending_checkpoint = None;
                            }
                        }
                    }

                    let captured = max_flushed_seq.load(Ordering::Relaxed);
                    let bumped = if checkpoint_gates_ring_reclaim {
                        false
                    } else {
                        // Cheap path (every tick): forward `durable_seq` to
                        // wherever `max_flushed_seq` has reached. Pure atomic
                        // load + CAS, no metadb lock acquired.
                        durable_seq
                            .fetch_update(
                                Ordering::Release,
                                Ordering::Relaxed,
                                |cur| if captured > cur { Some(captured) } else { None },
                            )
                            .is_ok()
                    };

                    // After durable_seq advances, kick each shard so
                    // `reclaim_log_prefix` re-runs against the new
                    // watermark. `mark_flushed`'s inline reclaim only
                    // sees the durable_seq value at THAT moment; once
                    // writes go idle (drain complete), nobody else
                    // re-visits the prefix and entries that were
                    // mark_flushed when their seq was still > durable_seq
                    // sit in `log_order` forever — observed as
                    // "Stuck=N, Pending=0" in the dashboard. The advance
                    // is `O(shards)` of mutex-guarded VecDeque pops, so
                    // it stays cheap even when nothing actually drains.
                    if bumped {
                        for idx in 0..shard_count {
                            let _ = buffer_pool_thread.advance_tail_for_shard(idx);
                        }
                    }

                    // Threshold-triggered path: when in-memory dirty
                    // work (L2P dirty pages + RC pending deltas)
                    // exceeds the configured cap, kick a checkpoint
                    // early instead of waiting for the periodic tick.
                    // The single-outstanding-request dispatcher means
                    // this only fires effectively while no flush is
                    // active, but in steady state the system finds a
                    // self-balancing point where flush cadence ≈
                    // threshold / dirty-accumulation-rate, capping
                    // sample_max and meta_io batch_bytes_max.
                    //
                    // Configured via `cfg.meta.flush_dirty_pages_threshold`;
                    // zero disables the early trigger and preserves
                    // the original periodic-only cadence.
                    let early_trigger = if dirty_pages_threshold > 0
                        && pending_checkpoint.is_none()
                        && Self::checkpoint_needed(captured, last_checkpoint_request_seq)
                    {
                        meta.dirty_pages_estimate() as u64 >= dirty_pages_threshold
                    } else {
                        false
                    };

                    // Expensive path (`checkpoint_interval`): metadb
                    // checkpoint. The metadb side keeps the global
                    // apply gate only for the checkpoint-boundary sample,
                    // but the IO still costs real device time, so run it
                    // sparingly.
                    if !early_trigger && last_checkpoint.elapsed() < checkpoint_interval {
                        continue;
                    }
                    last_checkpoint = std::time::Instant::now();
                    if !Self::checkpoint_needed(captured, last_checkpoint_request_seq) {
                        // Nothing new to checkpoint; defer to next round.
                        continue;
                    }
                    if checkpoint_gates_ring_reclaim {
                        if pending_checkpoint.is_some() {
                            continue;
                        }
                        match meta.try_request_durable_checkpoint_token() {
                            Ok(Some(token)) => {
                                last_checkpoint_request_seq = captured;
                                pending_checkpoint = Some((token, captured));
                                tracing::debug!(
                                    max_flushed_seq = captured,
                                    "durability watermark requested gated metadb checkpoint"
                                );
                            }
                            Ok(None) => {
                                tracing::debug!(
                                    max_flushed_seq = captured,
                                    "durability watermark skipped gated metadb checkpoint; previous checkpoint still running"
                                );
                            }
                            Err(e) => {
                                tracing::error!(
                                    error = %e,
                                    "durability watermark checkpoint request failed; WAL prune deferred to next cycle"
                                );
                            }
                        }
                    } else {
                        match meta.try_request_durable_checkpoint() {
                            Ok(true) => {
                                last_checkpoint_request_seq = captured;
                                tracing::debug!(
                                    max_flushed_seq = captured,
                                    "durability watermark requested metadb checkpoint"
                                );
                            }
                            Ok(false) => {
                                tracing::debug!(
                                    max_flushed_seq = captured,
                                    "durability watermark skipped metadb checkpoint; previous checkpoint still running"
                                );
                            }
                            Err(e) => {
                                tracing::error!(
                                    error = %e,
                                    "durability watermark checkpoint request failed; WAL prune deferred to next cycle"
                                );
                            }
                        }
                    }
                }
                // Final checkpoint at shutdown so the WAL segment-prune
                // catches up before the process exits.
                let captured = max_flushed_seq.load(Ordering::Relaxed);
                if let Err(e) = meta.sync_durable() {
                    tracing::error!(
                        error = %e,
                        "durability watermark final checkpoint failed at shutdown"
                    );
                }
                let _ = durable_seq.fetch_update(
                    Ordering::Release,
                    Ordering::Relaxed,
                    |cur| if captured > cur { Some(captured) } else { None },
                );
                for idx in 0..shard_count {
                    let _ = buffer_pool_thread.advance_tail_for_shard(idx);
                }
            })
            .expect("spawn durability-watermark thread");
        Self {
            stop,
            thread: Some(thread),
        }
    }

    fn checkpoint_needed(captured: u64, last_checkpoint_request_seq: u64) -> bool {
        captured > last_checkpoint_request_seq
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
}
