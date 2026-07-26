use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::types::Pba;

const HAZARD_SHARDS: usize = 256;
const WAIT_LOG_INTERVAL: Duration = Duration::from_secs(1);

/// Hazard-table instrumentation. `wait_for_readers` sits inside the flush
/// writer's timed IO window, where measurement showed 750 s of "io" against
/// only 37 s of actual LV3 batch writes in a 180 s window — so the writer's
/// blocking needs to be attributable. Process-wide because the table is shared
/// and cloned; read via `PbaHazards::stats`.
static WAIT_CALLS: AtomicU64 = AtomicU64::new(0);
static WAIT_BLOCKS: AtomicU64 = AtomicU64::new(0);
static WAIT_NS: AtomicU64 = AtomicU64::new(0);
static WAIT_NS_MAX: AtomicU64 = AtomicU64::new(0);
static PIN_CALLS: AtomicU64 = AtomicU64::new(0);
static PIN_PBAS: AtomicU64 = AtomicU64::new(0);

/// Snapshot of hazard-table activity since process start.
#[derive(Clone, Copy, Debug, Default)]
pub struct HazardStats {
    /// `wait_clear` invocations (one per PBA, so `count` per extent).
    pub wait_calls: u64,
    /// Invocations that actually found a live pin and had to block.
    pub wait_blocks: u64,
    /// Cumulative time spent blocked.
    pub wait_ns: u64,
    /// Longest single block.
    pub wait_ns_max: u64,
    /// `pin_many` invocations and the total PBAs they pinned.
    pub pin_calls: u64,
    pub pin_pbas: u64,
}

#[derive(Default)]
struct HazardState {
    counts: HashMap<Pba, u32>,
}

struct HazardShard {
    state: Mutex<HazardState>,
    cv: Condvar,
}

impl Default for HazardShard {
    fn default() -> Self {
        Self {
            state: Mutex::new(HazardState::default()),
            cv: Condvar::new(),
        }
    }
}

/// In-memory physical block hazard table.
///
/// Metadata refcounts describe durable logical reachability. This table covers
/// transient readers that already copied a `BlockmapValue` out of metadb but
/// have not completed the corresponding LV3 read yet. Cleanup must wait for
/// these pins before returning a physical block to the allocator.
#[derive(Clone)]
pub struct PbaHazards {
    shards: Arc<[HazardShard]>,
}

pub struct PbaHazardGuard {
    hazards: PbaHazards,
    pbas: Vec<Pba>,
}

impl PbaHazards {
    pub fn new() -> Self {
        let shards: Vec<HazardShard> = (0..HAZARD_SHARDS).map(|_| HazardShard::default()).collect();
        Self {
            shards: Arc::from(shards),
        }
    }

    pub fn pin_one(&self, pba: Pba) -> PbaHazardGuard {
        self.pin_many(std::iter::once(pba))
    }

    pub fn pin_many<I>(&self, pbas: I) -> PbaHazardGuard
    where
        I: IntoIterator<Item = Pba>,
    {
        let mut pbas: Vec<Pba> = pbas.into_iter().collect();
        pbas.sort_unstable();
        pbas.dedup();
        PIN_CALLS.fetch_add(1, Ordering::Relaxed);
        PIN_PBAS.fetch_add(pbas.len() as u64, Ordering::Relaxed);

        for &pba in &pbas {
            let shard = self.shard(pba);
            let mut state = shard.state.lock().unwrap();
            let count = state.counts.entry(pba).or_insert(0);
            *count = count.saturating_add(1);
        }

        PbaHazardGuard {
            hazards: self.clone(),
            pbas,
        }
    }

    pub fn wait_clear(&self, pba: Pba) {
        WAIT_CALLS.fetch_add(1, Ordering::Relaxed);
        let shard = self.shard(pba);
        let mut state = shard.state.lock().unwrap();
        if !state.counts.contains_key(&pba) {
            // Uncontended fast path: the mutex acquisition is the whole cost.
            // Kept separate from the blocking path so the two can be told apart
            // in the metrics.
            return;
        }
        WAIT_BLOCKS.fetch_add(1, Ordering::Relaxed);
        let blocked_at = Instant::now();
        let mut last_log = blocked_at;
        while state.counts.contains_key(&pba) {
            let (next_state, _) = shard.cv.wait_timeout(state, WAIT_LOG_INTERVAL).unwrap();
            state = next_state;
            if state.counts.contains_key(&pba) && last_log.elapsed() >= WAIT_LOG_INTERVAL {
                tracing::debug!(pba = pba.0, "waiting for in-flight PBA readers");
                last_log = Instant::now();
            }
        }
        drop(state);
        let elapsed = blocked_at.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        WAIT_NS.fetch_add(elapsed, Ordering::Relaxed);
        WAIT_NS_MAX.fetch_max(elapsed, Ordering::Relaxed);
    }

    /// Cumulative hazard-table activity since process start.
    pub fn stats() -> HazardStats {
        HazardStats {
            wait_calls: WAIT_CALLS.load(Ordering::Relaxed),
            wait_blocks: WAIT_BLOCKS.load(Ordering::Relaxed),
            wait_ns: WAIT_NS.load(Ordering::Relaxed),
            wait_ns_max: WAIT_NS_MAX.load(Ordering::Relaxed),
            pin_calls: PIN_CALLS.load(Ordering::Relaxed),
            pin_pbas: PIN_PBAS.load(Ordering::Relaxed),
        }
    }

    pub fn wait_extent_clear(&self, start: Pba, count: u32) {
        for offset in 0..count {
            self.wait_clear(Pba(start.0 + offset as u64));
        }
    }

    #[cfg(test)]
    pub fn is_pinned(&self, pba: Pba) -> bool {
        self.shard(pba)
            .state
            .lock()
            .unwrap()
            .counts
            .contains_key(&pba)
    }

    fn unpin(&self, pba: Pba) {
        let shard = self.shard(pba);
        let mut state = shard.state.lock().unwrap();
        if let Some(count) = state.counts.get_mut(&pba) {
            *count -= 1;
            if *count == 0 {
                state.counts.remove(&pba);
                shard.cv.notify_all();
            }
        }
    }

    fn shard(&self, pba: Pba) -> &HazardShard {
        &self.shards[pba.0 as usize % self.shards.len()]
    }
}

impl Default for PbaHazards {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for PbaHazardGuard {
    fn drop(&mut self) {
        for &pba in &self.pbas {
            self.hazards.unpin(pba);
        }
    }
}
