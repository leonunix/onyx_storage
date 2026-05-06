use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::types::Pba;

const HAZARD_SHARDS: usize = 256;
const WAIT_LOG_INTERVAL: Duration = Duration::from_secs(1);

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
        let shard = self.shard(pba);
        let mut state = shard.state.lock().unwrap();
        let mut last_log = Instant::now();
        while state.counts.contains_key(&pba) {
            let (next_state, _) = shard.cv.wait_timeout(state, WAIT_LOG_INTERVAL).unwrap();
            state = next_state;
            if state.counts.contains_key(&pba) && last_log.elapsed() >= WAIT_LOG_INTERVAL {
                tracing::debug!(pba = pba.0, "waiting for in-flight PBA readers");
                last_log = Instant::now();
            }
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
