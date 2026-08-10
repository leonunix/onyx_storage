//! The resident defragger — the pure allocator-side half of defrag, on its own
//! thread and its own cadence.
//!
//! ## Why this is a thread and not a step in the GC cycle
//!
//! Reclaim (mandatory space safety) and defrag (an optional optimiser) used to
//! share ONE `gc-runner` thread and ONE 5 s cycle. That is a criticality
//! inversion, and it was measured as one: reclaim's throughput is
//! `cycles × MAX_RETIRED_RECLAIM_BLOCKS_PER_CYCLE`, so defrag stretching the
//! cycle 8.6 → 13.7 s cost **23.7 GiB of `retired_depth` that no allocation
//! could use**, 88% of quarantines expired against the stall watchdog, and the
//! flusher's `SpaceExhausted` rate went up 2.7× — defrag starved the very
//! reclaim it depends on to finish its own targets (memory:
//! `gc_reclaim_defrag_criticality_inversion`, `defrag_gc_lever`).
//!
//! Splitting the thread makes that priority order structural: reclaim's cadence
//! is now untouchable by anything defrag does, and defrag can be paced for what
//! it actually is — a slow, bounded, best-effort optimiser.
//!
//! ## What this thread does (and deliberately does NOT do)
//!
//! Zero metadata scanning. One loop iteration is:
//!
//! 1. `maintain` — publish completed quarantines into the stripe reserve, cancel
//!    stalled ones, evaluate the trigger latch;
//! 2. [`SpaceAllocator::retired_stripe_windows`] — enumerate stripe windows
//!    containing a retired block, from the retired set itself (the only reverse
//!    index defrag gets for free);
//! 3. `select_from_allocator` — classify those windows and quarantine the ones
//!    whose whole remainder is free-or-retired, i.e. the ones **reclaim alone
//!    will finish**. The quarantine's entire job is to keep the foreground from
//!    eating the window's free fragments before that happens.
//!
//! Live-pinned windows need a PBA → LBA answer and a rewrite, so they stay with
//! the compactor's scan-driven selector (see [`super::defrag`]).
//!
//! ## The gate is a RATE CAP, and its verdict is a lock-attribution number
//!
//! This thread's whole cost is allocator lock trips:
//! `free_lock.defrag_classify` (classify) and `retired_lock.defrag_windows`
//! (enumerate). What must never happen is those showing up inside a flush
//! writer's `writer_refill` / `writer_unaligned` WAIT — lock COUNT, not
//! per-hold cost, is what the writers pay (memory:
//! `fragmentation_unaligned_alloc_1889_locks`: 1,889 acquisitions for ONE
//! unaligned allocation).
//!
//! So the knobs that matter are `defrag_interval_ms` and
//! `defrag_classify_max_windows_per_cycle`, and they are sized against that
//! number, not against throughput: at the defaults (1000 ms / 4096 windows) and
//! a 128-extent hold bound, this thread takes on the order of **tens of
//! acquisitions per second** across 2048 regions. Raising either knob is only
//! legitimate with the `free_lock` / `retired_lock` tables from two `status`
//! samples showing the writers' wait unchanged.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;

use crate::gc::config::GcConfig;
use crate::gc::defrag::Defragger;
use crate::metrics::EngineMetrics;
use crate::space::allocator::SpaceAllocator;

/// Floor on the resident cadence. A defragger that wakes faster than this is
/// paying acquisition cost for windows reclaim has not had time to touch — the
/// bound the whole design is judged on is lock trips per second.
const MIN_INTERVAL_MS: u64 = 50;

/// Cycles the retired-set walk backs off for after this many consecutive
/// zero-yield laps, capped.
///
/// The walk is O(retired shards) LOCKS per cycle — one per address region, 2048
/// at the default `storage.allocator_regions` — because it has to ask each shard
/// whether it holds anything in range. When the retired set is empty or holds
/// nothing that qualifies, that is pure cost: box 2026-08-10 measured
/// `retired_lock.defrag_windows` at **1,050,624 acquisitions over 903 cycles for
/// ZERO selections**, i.e. the walk lapped 513 times finding nothing (reclaim was
/// keeping up, so there were no retired-pinned windows to find at all).
///
/// So an empty lap backs the walk off by one extra cycle per consecutive empty
/// lap, up to this cap. Any lap that selects something resets it immediately, so
/// the backoff cannot delay real work by more than one cycle.
const MAX_IDLE_BACKOFF_CYCLES: u32 = 30;

/// Resident defrag thread handle.
pub struct DefragRunner {
    running: Arc<AtomicBool>,
    config: Arc<ArcSwap<GcConfig>>,
    handle: Option<JoinHandle<()>>,
}

impl DefragRunner {
    pub(crate) fn start(
        defragger: Arc<Defragger>,
        allocator: Arc<SpaceAllocator>,
        metrics: Arc<EngineMetrics>,
        config: GcConfig,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();
        let config = Arc::new(ArcSwap::from_pointee(config));
        let config_clone = config.clone();

        let handle = thread::Builder::new()
            .name("gc-defrag".into())
            .spawn(move || {
                crate::affinity::bind_current(crate::affinity::ThreadRole::Background, 2);
                // Quarantined free blocks must be back in the allocatable pools
                // before the flusher's shutdown drain, on EVERY exit path
                // including a panic unwind — hence the guard rather than a
                // release at the bottom of the loop.
                let _guard = QuarantineGuard {
                    defragger: &defragger,
                    allocator: &allocator,
                    metrics: &metrics,
                };
                Self::defrag_loop(
                    &defragger,
                    &allocator,
                    &metrics,
                    &config_clone,
                    &running_clone,
                );
            })
            .expect("failed to spawn defrag runner thread");

        Self {
            running,
            config,
            handle: Some(handle),
        }
    }

    /// Hot-reload. Shares [`GcConfig`] with the gc-runner: the defrag knobs live
    /// in one place because the two halves are gated by the same trigger latch.
    pub fn update_config(&self, new_config: GcConfig) {
        self.config.store(Arc::new(new_config));
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }

    fn defrag_loop(
        defragger: &Defragger,
        allocator: &SpaceAllocator,
        metrics: &EngineMetrics,
        config: &ArcSwap<GcConfig>,
        running: &AtomicBool,
    ) {
        // Lap cursor over the retired set's address space. Held across cycles so
        // consecutive wake-ups sweep forward instead of re-classifying the same
        // low-address windows forever.
        let mut cursor = 0u64;
        // Zero-yield backoff state — see `MAX_IDLE_BACKOFF_CYCLES`.
        let mut idle_laps = 0u32;
        let mut skip_cycles = 0u32;

        while running.load(Ordering::Relaxed) {
            let cfg = config.load();
            thread::sleep(Duration::from_millis(
                cfg.defrag_interval_ms.max(MIN_INTERVAL_MS),
            ));
            if !running.load(Ordering::Relaxed) {
                break;
            }
            metrics
                .gc_defrag_cycles
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            let total_blocks = allocator.total_block_count();
            let free_pct = if total_blocks == 0 {
                100
            } else {
                allocator.free_block_count() * 100 / total_blocks
            };

            // `maintain` owns the enable/disable transition, so a hot reload of
            // `defrag_enabled = false` (or a drop below `defrag_min_free_pct`)
            // cancels every active quarantine here.
            let t_maintain = Instant::now();
            let active = defragger.maintain(allocator, &cfg, free_pct, metrics);
            let maintain_ms = t_maintain.elapsed().as_millis();
            if !active {
                continue;
            }

            let Some((stripe, phase)) = allocator.stripe_geometry() else {
                continue;
            };

            // Two cheap preconditions before paying for the walk. `retired_block_count`
            // is a single atomic; a retired set that cannot even cover one whole
            // window has nothing this selector can use.
            if skip_cycles > 0 || allocator.retired_block_count() < u64::from(stripe) {
                skip_cycles = skip_cycles.saturating_sub(1);
                metrics
                    .gc_defrag_walk_skipped
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                continue;
            }

            let t_select = Instant::now();
            let (windows, lapped) = allocator.retired_stripe_windows(
                &mut cursor,
                stripe,
                phase,
                cfg.defrag_classify_max_windows_per_cycle,
            );
            let selected = defragger.select_from_allocator(allocator, &cfg, &windows, metrics);
            if lapped {
                metrics
                    .gc_defrag_retired_laps
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            // Back off only on a completed ZERO-YIELD lap: a capped mid-lap slice
            // that selected nothing still has address space left to look at.
            if selected > 0 {
                idle_laps = 0;
            } else if lapped {
                idle_laps = (idle_laps + 1).min(MAX_IDLE_BACKOFF_CYCLES);
                skip_cycles = idle_laps;
            }
            tracing::debug!(
                maintain_ms,
                select_ms = t_select.elapsed().as_millis(),
                windows = windows.len(),
                selected,
                lapped,
                idle_laps,
                cursor,
                free_pct,
                "defrag cycle"
            );
        }
    }
}

impl Drop for DefragRunner {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Releases every quarantine when the resident loop leaves, however it leaves.
struct QuarantineGuard<'a> {
    defragger: &'a Defragger,
    allocator: &'a SpaceAllocator,
    metrics: &'a EngineMetrics,
}

impl Drop for QuarantineGuard<'_> {
    fn drop(&mut self) {
        self.defragger.deactivate(self.allocator, self.metrics);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BLOCK_SIZE, RESERVED_BLOCKS};

    /// The resident loop may leave through `stop`, a config kill-switch, or a
    /// panic unwind. On every one of those the quarantined free blocks have to be
    /// back in the allocatable pools BEFORE the flusher's shutdown drain runs, or
    /// the drain can hit `SpaceExhausted` against space that is merely parked.
    #[test]
    fn guard_releases_active_quarantine_on_every_exit_path() {
        const STRIPE: u32 = 6;
        let phase = (RESERVED_BLOCKS % u64::from(STRIPE)) as u32;
        let allocator = SpaceAllocator::new(128 * BLOCK_SIZE as u64, 0);
        allocator.set_stripe_geometry(STRIPE, phase);
        let target = allocator
            .allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, phase)
            .unwrap();
        allocator.begin_defrag_quarantine(target).unwrap();
        let metrics = EngineMetrics::default();
        let defragger = Defragger::new();
        defragger.track_target_for_exit_test(target);

        {
            let _guard = QuarantineGuard {
                defragger: &defragger,
                allocator: &allocator,
                metrics: &metrics,
            };
        }

        assert!(!allocator.is_defrag_quarantined(target));
        assert_eq!(metrics.gc_defrag_segments_cancelled.load(Ordering::Relaxed), 1);
        assert!(!defragger.is_active());
    }

    /// The cadence floor is a rate cap, not cosmetics: the thread's whole cost is
    /// allocator lock trips, so a config of 0 must not turn it into a spin.
    #[test]
    fn interval_has_a_floor() {
        let cfg = GcConfig {
            defrag_interval_ms: 0,
            ..GcConfig::default()
        };
        assert_eq!(cfg.defrag_interval_ms.max(MIN_INTERVAL_MS), MIN_INTERVAL_MS);
    }
}
