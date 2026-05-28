use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crate::affinity::{self, ThreadRole};
use crate::buffer::pool::WriteBufferPool;
use crate::meta::store::MetaStore;

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
    /// `durable_seq` only advances on a confirmed `Ok(Some(true))`
    /// checkpoint outcome, and ring slots are reclaimed via
    /// `pool.release_below(checkpoint_seq)` at exactly the same moment.
    /// There is no "fast bump" derived from `max_flushed_seq` — that
    /// equated "metadb tx returned" with "durable", which was only true
    /// while metadb had a WAL.
    ///
    /// `checkpoint_interval` is the periodic cadence; if `dirty_pages_threshold`
    /// is non-zero, in-memory L2P/RC pressure can trigger an early checkpoint
    /// between ticks.
    pub(super) fn start(
        meta: Arc<MetaStore>,
        buffer_pool: Arc<WriteBufferPool>,
        max_flushed_seq: Arc<AtomicU64>,
        durable_seq: Arc<AtomicU64>,
        checkpoint_interval: std::time::Duration,
        dirty_pages_threshold: u64,
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
                while !stop_clone.load(Ordering::Relaxed) {
                    std::thread::sleep(TICK_INTERVAL);

                    if let Some((token, seq)) = pending_checkpoint {
                        match meta.durable_checkpoint_outcome(token) {
                            Ok(Some(true)) => {
                                durable_seq.fetch_max(seq, Ordering::Release);
                                if let Err(e) = buffer_pool_thread.release_below(seq) {
                                    tracing::warn!(
                                        seq,
                                        error = %e,
                                        "release_below failed; ring reclaim deferred to next checkpoint"
                                    );
                                }
                                pending_checkpoint = None;
                            }
                            Ok(Some(false)) => {
                                // Non-blocking checkpoint found apply gate busy.
                                // Retry on the next checkpoint interval.
                                last_checkpoint_request_seq =
                                    last_checkpoint_request_seq.min(seq.saturating_sub(1));
                                pending_checkpoint = None;
                            }
                            Ok(None) => {}
                            Err(e) => {
                                tracing::error!(
                                    error = %e,
                                    "durability watermark checkpoint failed; ring reclaim deferred to next cycle"
                                );
                                pending_checkpoint = None;
                            }
                        }
                    }

                    let captured = max_flushed_seq.load(Ordering::Relaxed);

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

                    if !early_trigger && last_checkpoint.elapsed() < checkpoint_interval {
                        continue;
                    }
                    last_checkpoint = std::time::Instant::now();
                    if !Self::checkpoint_needed(captured, last_checkpoint_request_seq) {
                        continue;
                    }
                    if pending_checkpoint.is_some() {
                        continue;
                    }
                    match meta.try_request_durable_checkpoint_token() {
                        Ok(Some(token)) => {
                            last_checkpoint_request_seq = captured;
                            pending_checkpoint = Some((token, captured));
                            tracing::debug!(
                                max_flushed_seq = captured,
                                "durability watermark requested metadb checkpoint"
                            );
                        }
                        Ok(None) => {
                            tracing::debug!(
                                max_flushed_seq = captured,
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
                if let Err(e) = buffer_pool_thread.release_below(captured) {
                    tracing::warn!(
                        captured,
                        error = %e,
                        "release_below failed during shutdown drain"
                    );
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
