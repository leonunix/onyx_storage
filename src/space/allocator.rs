use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::error::{OnyxError, OnyxResult};
use crate::meta::store::MetaStore;
use crate::space::extent::Extent;
use crate::space::hazard::PbaHazards;
use crate::types::{Pba, BLOCK_SIZE, RESERVED_BLOCKS};

/// Number of blocks to refill a lane cache from the global free list at once.
const LANE_CACHE_REFILL_SIZE: u32 = 256;
/// Number of blocks to reserve for each lane's contiguous extent cache.
/// Raw passthrough flushes commonly allocate 4-8 contiguous blocks per unit;
/// serving those from a lane-local slice avoids hammering the global BTreeSet.
const LANE_EXTENT_CACHE_REFILL_BLOCKS: u32 = 8192;

/// One original retire operation's age, tracked at retire granularity in the
/// `retired_age` log so coalescing the `retired_extents` set can never re-age it.
#[derive(Debug, Clone, Copy)]
struct RetiredRun {
    count: u32,
    retired_at: Instant,
}

pub struct SpaceAllocator {
    total_blocks: u64,
    free_extents: Mutex<BTreeSet<Extent>>,
    /// Coalesced retired set — authority for containment/overlap (`is_retired`,
    /// `overlapping_retired_extent`, `retired_block_count`). NEVER carries age.
    retired_extents: Mutex<BTreeSet<Extent>>,
    /// Advisory young-age log (start pba → run), holds ONLY entries younger than
    /// the reclaim grace (time-windowed → memory bounded to grace × retire-rate).
    /// Gates reclaim eligibility only: a retired sub-range is reclaimable iff it
    /// is NOT covered by a young entry here. Per-entry `retired_at` is fixed at
    /// retire time and never refreshed, so coalescing the set above cannot
    /// re-age it. Lock order: `retired_extents` BEFORE `retired_age`.
    retired_age: Mutex<BTreeMap<u64, RetiredRun>>,
    hazards: PbaHazards,
    allocated_blocks: AtomicU64,
    free_blocks: AtomicU64,
    /// Per-lane single-block caches. Each flush lane pops from its own cache
    /// to avoid contending on `free_extents`. Refilled in bulk from global.
    lane_caches: Vec<Mutex<Vec<Pba>>>,
    /// Per-lane contiguous extent caches for raw multi-block writes.
    lane_extent_caches: Vec<Mutex<Vec<Extent>>>,
    alloc_tracker: Option<Mutex<BTreeSet<Pba>>>,
}

impl SpaceAllocator {
    /// Create a new allocator for a device of the given size.
    /// Blocks 0..RESERVED_BLOCKS are reserved for superblock/heartbeat/HA lock.
    /// Allocatable space starts at PBA RESERVED_BLOCKS.
    pub fn new(device_size_bytes: u64, num_lanes: usize) -> Self {
        Self::new_with_hazards(device_size_bytes, num_lanes)
    }

    pub fn new_with_hazards(device_size_bytes: u64, num_lanes: usize) -> Self {
        let total_blocks = device_size_bytes / BLOCK_SIZE as u64;
        let usable_blocks = total_blocks.saturating_sub(RESERVED_BLOCKS);
        let mut free_extents = BTreeSet::new();
        if usable_blocks > 0 {
            free_extents.insert(Extent::new(
                Pba(RESERVED_BLOCKS),
                usable_blocks.min(u32::MAX as u64) as u32,
            ));
        }
        let lane_caches = (0..num_lanes).map(|_| Mutex::new(Vec::new())).collect();
        let lane_extent_caches = (0..num_lanes).map(|_| Mutex::new(Vec::new())).collect();
        let alloc_tracker = std::env::var("ONYX_ALLOC_TRACK")
            .map(|value| {
                matches!(
                    value.as_str(),
                    "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
                )
            })
            .unwrap_or(false)
            .then(|| Mutex::new(BTreeSet::new()));
        Self {
            total_blocks,
            free_extents: Mutex::new(free_extents),
            retired_extents: Mutex::new(BTreeSet::new()),
            retired_age: Mutex::new(BTreeMap::new()),
            hazards: PbaHazards::new(),
            allocated_blocks: AtomicU64::new(0),
            free_blocks: AtomicU64::new(usable_blocks),
            lane_caches,
            lane_extent_caches,
            alloc_tracker,
        }
    }

    /// Rebuild the free list from MetaStore metadata.
    /// Blockmap is the source of truth so multi-block compression units reserve
    /// all occupied PBAs, not just the starting block.
    /// PBAs below RESERVED_BLOCKS are excluded (reserved for superblock/HA).
    pub fn rebuild_from_metadata(&self, meta: &MetaStore) -> OnyxResult<()> {
        // Collect all allocated PBAs into a sorted vec, filtering out reserved region
        let mut allocated: Vec<u64> = meta
            .iter_allocated_blocks()?
            .into_iter()
            .map(|pba| pba.0)
            .filter(|&pba| pba >= RESERVED_BLOCKS)
            .collect();
        allocated.sort_unstable();

        // Build free extents from gaps (starting at RESERVED_BLOCKS)
        let mut free = BTreeSet::new();
        let mut pos: u64 = RESERVED_BLOCKS;

        for &alloc_pba in &allocated {
            if alloc_pba > pos {
                let gap = alloc_pba - pos;
                // Split into u32-sized extents if needed
                let mut start = pos;
                let mut remaining = gap;
                while remaining > 0 {
                    let count = remaining.min(u32::MAX as u64) as u32;
                    free.insert(Extent::new(Pba(start), count));
                    start += count as u64;
                    remaining -= count as u64;
                }
            }
            pos = alloc_pba + 1;
        }

        // Trailing free space
        if pos < self.total_blocks {
            let gap = self.total_blocks - pos;
            let mut start = pos;
            let mut remaining = gap;
            while remaining > 0 {
                let count = remaining.min(u32::MAX as u64) as u32;
                free.insert(Extent::new(Pba(start), count));
                start += count as u64;
                remaining -= count as u64;
            }
        }

        let usable_blocks = self.total_blocks.saturating_sub(RESERVED_BLOCKS);
        let alloc_count = allocated.len() as u64;
        let free_count = usable_blocks - alloc_count;

        *self.free_extents.lock().unwrap() = free;
        self.retired_extents.lock().unwrap().clear();
        self.retired_age.lock().unwrap().clear();
        self.clear_lane_caches();
        if let Some(tracker) = &self.alloc_tracker {
            let mut tracker = tracker.lock().unwrap();
            tracker.clear();
            for &pba in &allocated {
                tracker.insert(Pba(pba));
            }
        }
        self.allocated_blocks.store(alloc_count, Ordering::Relaxed);
        self.free_blocks.store(free_count, Ordering::Relaxed);

        tracing::info!(
            total = self.total_blocks,
            allocated = alloc_count,
            free = free_count,
            extents = self.free_extents.lock().unwrap().len(),
            "space allocator rebuilt from metadata"
        );

        Ok(())
    }

    pub fn hazards(&self) -> PbaHazards {
        self.hazards.clone()
    }

    /// Wait until no in-flight reader currently pins this physical extent.
    ///
    /// Allocator free waits protect the hand-off back to the free list. Writers
    /// also call this after allocation and before overwriting the physical
    /// blocks, because a reader may have pinned a just-freed PBA after it was
    /// reallocated but before the new payload is written.
    pub fn wait_for_readers(&self, start: Pba, count: u32) {
        self.hazards.wait_extent_clear(start, count);
    }

    fn track_alloc(&self, extent: Extent, context: &'static str) -> OnyxResult<()> {
        let Some(tracker) = &self.alloc_tracker else {
            return Ok(());
        };
        let mut tracker = tracker.lock().unwrap();
        for offset in 0..extent.count {
            let pba = Pba(extent.start.0 + offset as u64);
            if !tracker.insert(pba) {
                tracing::error!(
                    pba = pba.0,
                    start = extent.start.0,
                    blocks = extent.count,
                    context,
                    "allocator live-PBA tracker detected duplicate allocation"
                );
                return Err(OnyxError::Config(format!(
                    "allocator duplicate allocation pba={} context={context}",
                    pba.0
                )));
            }
        }
        Ok(())
    }

    fn track_release(&self, extent: Extent, context: &'static str) {
        let Some(tracker) = &self.alloc_tracker else {
            return;
        };
        let mut tracker = tracker.lock().unwrap();
        for offset in 0..extent.count {
            let pba = Pba(extent.start.0 + offset as u64);
            if !tracker.remove(&pba) {
                tracing::warn!(
                    pba = pba.0,
                    start = extent.start.0,
                    blocks = extent.count,
                    context,
                    "allocator live-PBA tracker released a non-live PBA"
                );
            }
        }
    }

    /// Allocate a single block. Returns PBA.
    pub fn allocate_one(&self) -> OnyxResult<Pba> {
        {
            let mut free = self.free_extents.lock().unwrap();
            if let Some(pba) = Self::alloc_one_from_set(&mut free) {
                self.track_alloc(Extent::single(pba), "allocate_one")?;
                self.allocated_blocks.fetch_add(1, Ordering::Relaxed);
                self.free_blocks.fetch_sub(1, Ordering::Relaxed);
                return Ok(pba);
            }
        }
        // Global pool empty — drain lane caches and retry
        if !self.lane_caches.is_empty() {
            self.drain_lane_caches();
            let mut free = self.free_extents.lock().unwrap();
            if let Some(pba) = Self::alloc_one_from_set(&mut free) {
                self.track_alloc(Extent::single(pba), "allocate_one_retry")?;
                self.allocated_blocks.fetch_add(1, Ordering::Relaxed);
                self.free_blocks.fetch_sub(1, Ordering::Relaxed);
                return Ok(pba);
            }
        }
        Err(OnyxError::SpaceExhausted)
    }

    /// Helper: take one block from the free set (no counter update).
    fn alloc_one_from_set(free: &mut BTreeSet<Extent>) -> Option<Pba> {
        let extent = free.iter().next().copied()?;
        free.remove(&extent);
        let pba = extent.start;
        if extent.count > 1 {
            free.insert(Extent::new(Pba(pba.0 + 1), extent.count - 1));
        }
        Some(pba)
    }

    /// Allocate a single block using the per-lane cache to avoid global lock contention.
    /// Falls back to global allocation with bulk refill when the cache is empty.
    pub fn allocate_one_for_lane(&self, lane: usize) -> OnyxResult<Pba> {
        if lane >= self.lane_caches.len() {
            return self.allocate_one();
        }
        // Fast path: pop from lane cache (no global lock)
        {
            let mut cache = self.lane_caches[lane].lock().unwrap();
            if let Some(pba) = cache.pop() {
                // Count as allocated only when given to caller
                self.track_alloc(Extent::single(pba), "allocate_one_for_lane_cache")?;
                self.allocated_blocks.fetch_add(1, Ordering::Relaxed);
                self.free_blocks.fetch_sub(1, Ordering::Relaxed);
                return Ok(pba);
            }
        }
        // Slow path: refill from global (blocks stay logically "free" in the cache)
        let (first_pba, refill) = match self.take_extent_from_global(LANE_CACHE_REFILL_SIZE) {
            Some(extent) => (extent.start, extent.count),
            None => {
                self.drain_lane_caches();
                let extent = self
                    .take_extent_from_global(LANE_CACHE_REFILL_SIZE)
                    .ok_or(OnyxError::SpaceExhausted)?;
                (extent.start, extent.count)
            }
        };
        // First block goes to caller (counted as allocated), rest into cache
        self.track_alloc(Extent::single(first_pba), "allocate_one_for_lane_refill")?;
        self.allocated_blocks.fetch_add(1, Ordering::Relaxed);
        self.free_blocks.fetch_sub(1, Ordering::Relaxed);
        if refill > 1 {
            let mut cache = self.lane_caches[lane].lock().unwrap();
            for i in 1..refill {
                cache.push(Pba(first_pba.0 + i as u64));
            }
        }
        Ok(first_pba)
    }

    /// Return all cached blocks from lane caches to the global free list.
    /// Called during shutdown to prevent block leaks.
    pub fn drain_lane_caches(&self) {
        let mut free = self.free_extents.lock().unwrap();
        for cache_mutex in &self.lane_caches {
            let mut cache = cache_mutex.lock().unwrap();
            for pba in cache.drain(..) {
                Self::coalesce_and_insert(&mut free, Extent::single(pba));
            }
        }
        for cache_mutex in &self.lane_extent_caches {
            let mut cache = cache_mutex.lock().unwrap();
            for extent in cache.drain(..) {
                Self::coalesce_and_insert(&mut free, extent);
            }
        }
        // No counter adjustment needed: cached blocks were never counted as allocated
    }

    /// Free a single block.
    ///
    /// Returns error if the PBA is out of bounds, already free (in the global
    /// free list **or** a lane cache), or would underflow counters.
    pub fn free_one(&self, pba: Pba) -> OnyxResult<()> {
        self.free_extent_unchecked_ownership(Extent::single(pba))
    }

    /// Return true if the single block is free — either in the global free list
    /// or sitting in a lane cache (allocated from the free list but not yet
    /// handed out to a caller).
    pub fn is_free(&self, pba: Pba) -> bool {
        let free = self.free_extents.lock().unwrap();
        if free
            .range(..=Extent::single(pba))
            .next_back()
            .is_some_and(|extent| extent.contains(pba))
        {
            return true;
        }
        drop(free);
        for cache_mutex in &self.lane_caches {
            let cache = cache_mutex.lock().unwrap();
            if cache.contains(&pba) {
                return true;
            }
        }
        for cache_mutex in &self.lane_extent_caches {
            let cache = cache_mutex.lock().unwrap();
            if cache.iter().any(|extent| extent.contains(pba)) {
                return true;
            }
        }
        false
    }

    /// Allocate a contiguous extent using a lane-local cache before touching
    /// the global free list. This is the hot path for raw 8/16/32 KiB flushes.
    pub fn allocate_extent_for_lane(&self, lane: usize, count: u32) -> OnyxResult<Extent> {
        if count == 0 {
            return Err(OnyxError::Config("cannot allocate 0 blocks".into()));
        }
        if lane >= self.lane_extent_caches.len() {
            return self.allocate_extent(count);
        }

        {
            let mut cache = self.lane_extent_caches[lane].lock().unwrap();
            if let Some(extent) = Self::take_from_extent_cache(&mut cache, count) {
                self.track_alloc(extent, "allocate_extent_for_lane_cache")?;
                self.allocated_blocks
                    .fetch_add(count as u64, Ordering::Relaxed);
                self.free_blocks.fetch_sub(count as u64, Ordering::Relaxed);
                return Ok(extent);
            }
        }

        let target = LANE_EXTENT_CACHE_REFILL_BLOCKS.max(count);
        let refill = self
            .take_extent_from_global_at_least(count, target)
            .or_else(|| {
                self.drain_lane_caches();
                self.take_extent_from_global_at_least(count, target)
            });

        let Some(refill) = refill else {
            return self.allocate_extent(count);
        };

        let result = Extent::new(refill.start, count);
        self.track_alloc(result, "allocate_extent_for_lane_refill")?;
        self.allocated_blocks
            .fetch_add(count as u64, Ordering::Relaxed);
        self.free_blocks.fetch_sub(count as u64, Ordering::Relaxed);

        if refill.count > count {
            let rest = Extent::new(Pba(refill.start.0 + count as u64), refill.count - count);
            self.lane_extent_caches[lane].lock().unwrap().push(rest);
        }
        Ok(result)
    }

    /// Allocate up to `count` contiguous blocks. Returns the extent actually allocated
    /// (may be smaller than requested if no large enough contiguous region exists).
    pub fn allocate_extent(&self, count: u32) -> OnyxResult<Extent> {
        if count == 0 {
            return Err(OnyxError::Config("cannot allocate 0 blocks".into()));
        }

        // Try allocation from global free list. If insufficient, drain lane caches and retry.
        for attempt in 0..2 {
            let mut free = self.free_extents.lock().unwrap();

            // Find first extent that's large enough
            let exact = free.iter().find(|e| e.count >= count).copied();

            if let Some(extent) = exact {
                free.remove(&extent);
                let result = Extent::new(extent.start, count);
                if extent.count > count {
                    free.insert(Extent::new(
                        Pba(extent.start.0 + count as u64),
                        extent.count - count,
                    ));
                }
                self.track_alloc(result, "allocate_extent")?;
                self.allocated_blocks
                    .fetch_add(count as u64, Ordering::Relaxed);
                self.free_blocks.fetch_sub(count as u64, Ordering::Relaxed);
                return Ok(result);
            }

            // No contiguous extent large enough. Cached lane extents may hold
            // enough free contiguous space, so fold them back once before
            // falling back to the largest global fragment.
            if attempt == 0 && self.has_lane_cached_blocks() {
                drop(free);
                self.drain_lane_caches();
                continue;
            }

            // No contiguous extent large enough — return the largest available
            let largest = free.iter().max_by_key(|e| e.count).copied();
            if let Some(extent) = largest {
                free.remove(&extent);
                self.track_alloc(extent, "allocate_extent_largest")?;
                self.allocated_blocks
                    .fetch_add(extent.count as u64, Ordering::Relaxed);
                self.free_blocks
                    .fetch_sub(extent.count as u64, Ordering::Relaxed);
                return Ok(extent);
            }

            // No free extents at all — drain lane caches and retry once
            drop(free);
            if attempt == 0 && self.has_lane_cached_blocks() {
                self.drain_lane_caches();
                continue;
            }
            break;
        }
        Err(OnyxError::SpaceExhausted)
    }

    /// Free an extent.
    ///
    /// Returns error if the extent is out of bounds, overlaps existing free space,
    /// or would underflow counters.
    pub fn free_extent(&self, extent: Extent) -> OnyxResult<()> {
        self.free_extent_unchecked_ownership(extent)
    }

    /// Move a logically dead physical extent into the retired set.
    ///
    /// Retired extents are not allocatable. They become reusable only after
    /// the GC reclaimer re-validates metadata and calls `reclaim_retired_extent`.
    ///
    /// Returns the number of blocks that NEWLY entered the retired set (0 = the
    /// extent was already fully retired — idempotent re-retire). Per-block
    /// idempotency replaces the old caller-side `is_retired(start)` precheck.
    pub fn retire_one(&self, pba: Pba) -> OnyxResult<u32> {
        self.retire_extent(Extent::single(pba))
    }

    pub fn retire_extent(&self, extent: Extent) -> OnyxResult<u32> {
        self.retire_extent_at(extent, Instant::now())
    }

    /// `retire_extent` with an injectable retire timestamp (so age-mechanism
    /// tests can control settle ages deterministically without sleeping).
    pub(crate) fn retire_extent_at(&self, extent: Extent, now: Instant) -> OnyxResult<u32> {
        self.validate_extent_shape(extent, "retire_extent")?;
        self.ensure_not_in_lane_cache(extent, "retire_extent")?;

        let free = self.free_extents.lock().unwrap();
        if let Some(e) = Self::overlapping_extent(&free, extent) {
            return Err(OnyxError::Config(format!(
                "retire_extent: extent {:?} overlaps free extent {:?}",
                extent, e
            )));
        }

        let current_alloc = self.allocated_blocks.load(Ordering::Relaxed);
        if (extent.count as u64) > current_alloc {
            return Err(OnyxError::Config(format!(
                "retire_extent: retiring {} blocks but only {} allocated",
                extent.count, current_alloc
            )));
        }

        // Lock order: retired_extents (set) BEFORE retired_age. Free held across
        // (outermost) so the overlap check can't race a concurrent free.
        let mut retired = self.retired_extents.lock().unwrap();
        // Sub-ranges of `extent` not already covered by the coalesced set = the
        // genuinely-new blocks. Computed BEFORE coalescing so already-retired
        // sub-ranges keep their original age (never refreshed).
        let gaps = Self::uncovered_subranges(&retired, extent);
        let newly: u32 = gaps.iter().map(|g| g.count).sum();
        Self::coalesce_and_insert_any_overlap(&mut retired, extent);
        if newly > 0 {
            let mut age = self.retired_age.lock().unwrap();
            for g in gaps {
                age.insert(
                    g.start.0,
                    RetiredRun {
                        count: g.count,
                        retired_at: now,
                    },
                );
            }
        }
        Ok(newly)
    }

    /// Sub-ranges of `extent` NOT covered by any extent in the coalesced `set`
    /// (the genuinely-new portions of a retire). `set` is non-overlapping and
    /// sorted by start, so this is a single ordered walk.
    fn uncovered_subranges(set: &BTreeSet<Extent>, extent: Extent) -> Vec<Extent> {
        let mut gaps = Vec::new();
        let end = extent.end_pba().0;
        let mut cursor = extent.start.0;
        // Predecessor extent that may cover the start.
        if let Some(before) = set
            .range(..=Extent::single(extent.start))
            .next_back()
            .copied()
        {
            if before.end_pba().0 > cursor {
                cursor = before.end_pba().0.min(end);
            }
        }
        for e in set.range(Extent::single(extent.start)..) {
            if e.start.0 >= end {
                break;
            }
            if e.start.0 > cursor {
                let gap_end = e.start.0.min(end);
                if gap_end > cursor {
                    gaps.push(Extent::new(Pba(cursor), (gap_end - cursor) as u32));
                }
                cursor = gap_end;
            }
            let e_end = e.end_pba().0.min(end);
            if e_end > cursor {
                cursor = e_end;
            }
            if cursor >= end {
                break;
            }
        }
        if cursor < end {
            gaps.push(Extent::new(Pba(cursor), (end - cursor) as u32));
        }
        gaps
    }

    /// Return a snapshot of ALL coalesced retired extents (audit / accounting
    /// invariant `allocated >= live + retired`). NOT grace-filtered — use
    /// [`Self::aged_candidates`] for the reclaim path.
    pub fn retired_candidates(&self, limit: usize) -> Vec<Extent> {
        if limit == 0 {
            return Vec::new();
        }
        self.retired_extents
            .lock()
            .unwrap()
            .iter()
            .take(limit)
            .copied()
            .collect()
    }

    /// Reclaim candidates: retired sub-ranges that have settled ≥ `grace` (i.e.
    /// are NOT covered by a young `retired_age` entry), emitted as coalesced
    /// extents (fat where retires were contiguous → throughput) up to a
    /// `limit_blocks` BLOCK budget (NOT an extent count — a per-extent cap would
    /// collapse throughput under fragmented retires). Prunes aged-out entries
    /// from the age log as it scans (the time-window that bounds its memory).
    /// Every emitted block individually satisfies the grace, so freeing it
    /// honors the settle-window safety invariant.
    /// Returns `(candidates, deferred_blocks)` where `deferred_blocks` is the
    /// total retired-but-still-young block count (held back by the grace) — the
    /// diagnostic that, vs rc-rejected, localized the re-aging bottleneck.
    pub fn aged_candidates(
        &self,
        limit_blocks: usize,
        grace: Duration,
        now: Instant,
    ) -> (Vec<Extent>, u64) {
        if limit_blocks == 0 {
            return (Vec::new(), 0);
        }
        let retired = self.retired_extents.lock().unwrap();
        let mut age = self.retired_age.lock().unwrap();
        // Time-window: drop entries that have aged past the grace — they no
        // longer gate anything (their covering retired extent is fully eligible).
        age.retain(|_, run| now.duration_since(run.retired_at) < grace);
        let deferred_blocks: u64 = age.values().map(|run| run.count as u64).sum();

        let mut out = Vec::new();
        let mut emitted: usize = 0;
        for ext in retired.iter() {
            if emitted >= limit_blocks {
                break;
            }
            for aged in Self::aged_subranges(&age, *ext) {
                if emitted >= limit_blocks {
                    break;
                }
                let take = (aged.count as usize).min(limit_blocks - emitted);
                if take == 0 {
                    continue;
                }
                out.push(Extent::new(aged.start, take as u32));
                emitted += take;
            }
        }
        (out, deferred_blocks)
    }

    /// Sub-ranges of coalesced retired extent `ext` NOT covered by any young
    /// entry in `age` (= the grace-satisfied, reclaimable parts). Same ordered
    /// walk as [`Self::uncovered_subranges`] but over the age log.
    fn aged_subranges(age: &BTreeMap<u64, RetiredRun>, ext: Extent) -> Vec<Extent> {
        let mut aged = Vec::new();
        let end = ext.end_pba().0;
        let mut cursor = ext.start.0;
        if let Some((&ks, run)) = age.range(..=ext.start.0).next_back() {
            let ke = ks + run.count as u64;
            if ke > cursor {
                cursor = ke.min(end);
            }
        }
        for (&ks, run) in age.range(ext.start.0..) {
            if ks >= end {
                break;
            }
            if ks > cursor {
                let gap_end = ks.min(end);
                if gap_end > cursor {
                    aged.push(Extent::new(Pba(cursor), (gap_end - cursor) as u32));
                }
                cursor = gap_end;
            }
            let ke = (ks + run.count as u64).min(end);
            if ke > cursor {
                cursor = ke;
            }
            if cursor >= end {
                break;
            }
        }
        if cursor < end {
            aged.push(Extent::new(Pba(cursor), (end - cursor) as u32));
        }
        aged
    }

    /// Remove young age-log entries whose start lies within `[ext.start,
    /// ext.end)`. Aged candidates are carved between young entries so this is
    /// normally a no-op; kept defensive for the failure/reclaim paths.
    fn purge_age_range(age: &mut BTreeMap<u64, RetiredRun>, ext: Extent) {
        let s = ext.start.0;
        let e = ext.end_pba().0;
        let keys: Vec<u64> = age.range(s..e).map(|(&k, _)| k).collect();
        for k in keys {
            age.remove(&k);
        }
    }

    pub fn is_retired(&self, pba: Pba) -> bool {
        let retired = self.retired_extents.lock().unwrap();
        Self::covering_extent(&retired, pba).is_some()
    }

    pub fn retired_block_count(&self) -> u64 {
        self.retired_extents
            .lock()
            .unwrap()
            .iter()
            .map(|extent| extent.count as u64)
            .sum()
    }

    /// Release a retired extent into the free list after GC has proved it is
    /// no longer referenced by metadata. `extent` may be a SUB-RANGE of a larger
    /// coalesced retired extent (the reclaim path frees aged sub-prefixes); the
    /// covering extent is split and the non-reclaimed remainders kept retired.
    pub fn reclaim_retired_extent(&self, extent: Extent) -> OnyxResult<bool> {
        self.validate_extent_shape(extent, "reclaim_retired_extent")?;

        {
            let mut retired = self.retired_extents.lock().unwrap();
            // The candidate must be fully contained in one coalesced retired
            // extent. Fail closed (Ok(false)) if it is no longer (fully) retired
            // — a raced reclaim / re-alloc — never free a span we didn't verify.
            let cover = match Self::covering_extent(&retired, extent.start) {
                Some(c) if c.end_pba().0 >= extent.end_pba().0 => c,
                _ => return Ok(false),
            };
            retired.remove(&cover);
            if extent.start.0 > cover.start.0 {
                retired.insert(Extent::new(
                    cover.start,
                    (extent.start.0 - cover.start.0) as u32,
                ));
            }
            if cover.end_pba().0 > extent.end_pba().0 {
                retired.insert(Extent::new(
                    extent.end_pba(),
                    (cover.end_pba().0 - extent.end_pba().0) as u32,
                ));
            }
            // Defensive: drop any young age entries inside the reclaimed range
            // (aged candidates are carved between young entries, so normally none).
            let mut age = self.retired_age.lock().unwrap();
            Self::purge_age_range(&mut age, extent);
        }

        let result = (|| -> OnyxResult<()> {
            self.hazards.wait_extent_clear(extent.start, extent.count);
            self.ensure_not_in_lane_cache(extent, "reclaim_retired_extent")?;

            let mut free = self.free_extents.lock().unwrap();
            if let Some(e) = Self::overlapping_extent(&free, extent) {
                return Err(OnyxError::Config(format!(
                    "reclaim_retired_extent: extent {:?} overlaps free extent {:?}",
                    extent, e
                )));
            }
            let current_alloc = self.allocated_blocks.load(Ordering::Relaxed);
            if (extent.count as u64) > current_alloc {
                return Err(OnyxError::Config(format!(
                    "reclaim_retired_extent: freeing {} blocks but only {} allocated",
                    extent.count, current_alloc
                )));
            }

            Self::coalesce_and_insert(&mut free, extent);
            self.track_release(extent, "reclaim_retired_extent");
            self.allocated_blocks
                .fetch_sub(extent.count as u64, Ordering::Relaxed);
            self.free_blocks
                .fetch_add(extent.count as u64, Ordering::Relaxed);
            Ok(())
        })();

        if result.is_err() {
            // Re-insert the extent, COALESCING it back with the split remainders
            // (plain insert would leave adjacent fragments). The age log is NOT
            // touched: `extent` was already aged, so it stays immediately
            // eligible next cycle — no re-aging on the error path.
            let mut retired = self.retired_extents.lock().unwrap();
            Self::coalesce_and_insert_any_overlap(&mut retired, extent);
        }
        result.map(|_| true)
    }

    fn free_extent_unchecked_ownership(&self, extent: Extent) -> OnyxResult<()> {
        self.validate_free_extent(extent)?;

        self.hazards.wait_extent_clear(extent.start, extent.count);

        let mut free = self.free_extents.lock().unwrap();
        self.ensure_not_free_or_retired_after_wait(extent, &free)?;
        Self::coalesce_and_insert(&mut free, extent);
        self.track_release(extent, "free_extent");
        self.allocated_blocks
            .fetch_sub(extent.count as u64, Ordering::Relaxed);
        self.free_blocks
            .fetch_add(extent.count as u64, Ordering::Relaxed);
        Ok(())
    }

    fn validate_free_extent(&self, extent: Extent) -> OnyxResult<()> {
        self.validate_extent_shape(extent, "free_extent")?;
        self.ensure_not_in_lane_cache(extent, "free_extent")?;

        let free = self.free_extents.lock().unwrap();

        // Check no overlap with existing free extents
        if let Some(e) = Self::overlapping_extent(&free, extent) {
            return Err(OnyxError::Config(format!(
                "free_extent: extent {:?} overlaps free extent {:?}",
                extent, e
            )));
        }
        if let Some(e) = self.overlapping_retired_extent(extent) {
            return Err(OnyxError::Config(format!(
                "free_extent: extent {:?} overlaps retired extent {:?}",
                extent, e
            )));
        }

        let current_alloc = self.allocated_blocks.load(Ordering::Relaxed);
        if (extent.count as u64) > current_alloc {
            return Err(OnyxError::Config(format!(
                "free_extent: freeing {} blocks but only {} allocated",
                extent.count, current_alloc
            )));
        }
        Ok(())
    }

    fn validate_extent_shape(&self, extent: Extent, context: &'static str) -> OnyxResult<()> {
        if extent.count == 0 {
            return Err(OnyxError::Config(format!(
                "{context}: cannot cover 0 blocks"
            )));
        }
        if extent.end_pba().0 > self.total_blocks {
            return Err(OnyxError::Config(format!(
                "{context}: extent {:?} exceeds total blocks {}",
                extent, self.total_blocks
            )));
        }
        Ok(())
    }

    fn ensure_not_in_lane_cache(&self, extent: Extent, context: &'static str) -> OnyxResult<()> {
        for (lane_idx, cache_mutex) in self.lane_caches.iter().enumerate() {
            let cache = cache_mutex.lock().unwrap();
            if (0..extent.count).any(|i| cache.contains(&Pba(extent.start.0 + i as u64))) {
                return Err(OnyxError::Config(format!(
                    "{context}: extent {:?} overlaps lane cache {}",
                    extent, lane_idx
                )));
            }
        }
        for (lane_idx, cache_mutex) in self.lane_extent_caches.iter().enumerate() {
            let cache = cache_mutex.lock().unwrap();
            if cache
                .iter()
                .any(|cached| Self::extents_overlap(extent, *cached))
            {
                return Err(OnyxError::Config(format!(
                    "{context}: extent {:?} overlaps lane extent cache {}",
                    extent, lane_idx
                )));
            }
        }
        Ok(())
    }

    /// Return true if the whole extent is already covered by a free extent
    /// or all its blocks are sitting in lane caches.
    pub fn is_extent_free(&self, extent: Extent) -> bool {
        let free = self.free_extents.lock().unwrap();
        if free.range(..=extent).next_back().is_some_and(|existing| {
            extent.start.0 >= existing.start.0 && extent.end_pba().0 <= existing.end_pba().0
        }) {
            return true;
        }
        drop(free);
        // Fallback: check if every block in the extent is in a lane cache.
        (0..extent.count).all(|i| {
            let pba = Pba(extent.start.0 + i as u64);
            self.lane_caches
                .iter()
                .any(|c| c.lock().unwrap().contains(&pba))
                || self
                    .lane_extent_caches
                    .iter()
                    .any(|c| c.lock().unwrap().iter().any(|e| e.contains(pba)))
        })
    }

    pub fn free_block_count(&self) -> u64 {
        self.free_blocks.load(Ordering::Relaxed)
    }

    pub fn allocated_block_count(&self) -> u64 {
        self.allocated_blocks.load(Ordering::Relaxed)
    }

    pub fn total_block_count(&self) -> u64 {
        self.total_blocks
    }

    /// Insert an extent and merge with adjacent free extents.
    fn coalesce_and_insert(free: &mut BTreeSet<Extent>, new: Extent) {
        let mut merged_start = new.start.0;
        let mut merged_end = new.end_pba().0;

        let before = free.range(..=new).next_back().copied();
        if let Some(extent) = before {
            if extent.end_pba().0 == merged_start {
                merged_start = extent.start.0;
                free.remove(&extent);
            }
        }

        let probe = Extent::new(Pba(merged_end), 0);
        let after = free.range(probe..).next().copied();
        if let Some(extent) = after {
            if extent.start.0 == merged_end {
                merged_end = extent.end_pba().0;
                free.remove(&extent);
            }
        }

        let count = (merged_end - merged_start) as u32;
        free.insert(Extent::new(Pba(merged_start), count));
    }

    fn coalesce_and_insert_any_overlap(set: &mut BTreeSet<Extent>, new: Extent) {
        let mut merged_start = new.start.0;
        let mut merged_end = new.end_pba().0;

        loop {
            let probe = Extent::new(Pba(merged_start), 0);
            let before = set.range(..=probe).next_back().copied();
            if let Some(extent) = before {
                if extent.end_pba().0 >= merged_start {
                    merged_start = merged_start.min(extent.start.0);
                    merged_end = merged_end.max(extent.end_pba().0);
                    set.remove(&extent);
                    continue;
                }
            }

            let probe = Extent::new(Pba(merged_start), 0);
            let after = set.range(probe..).next().copied();
            if let Some(extent) = after {
                if extent.start.0 <= merged_end {
                    merged_start = merged_start.min(extent.start.0);
                    merged_end = merged_end.max(extent.end_pba().0);
                    set.remove(&extent);
                    continue;
                }
            }
            break;
        }

        set.insert(Extent::new(
            Pba(merged_start),
            (merged_end - merged_start) as u32,
        ));
    }

    fn clear_lane_caches(&self) {
        for cache in &self.lane_caches {
            cache.lock().unwrap().clear();
        }
        for cache in &self.lane_extent_caches {
            cache.lock().unwrap().clear();
        }
    }

    fn ensure_not_free_or_retired_after_wait(
        &self,
        extent: Extent,
        free: &BTreeSet<Extent>,
    ) -> OnyxResult<()> {
        if let Some(e) = Self::overlapping_extent(free, extent) {
            return Err(OnyxError::Config(format!(
                "free_extent: extent {:?} overlaps free extent {:?} after hazard wait",
                extent, e
            )));
        }
        if let Some(e) = self.overlapping_retired_extent(extent) {
            return Err(OnyxError::Config(format!(
                "free_extent: extent {:?} overlaps retired extent {:?} after hazard wait",
                extent, e
            )));
        }
        Ok(())
    }

    fn has_lane_cached_blocks(&self) -> bool {
        self.lane_caches
            .iter()
            .any(|cache| !cache.lock().unwrap().is_empty())
            || self
                .lane_extent_caches
                .iter()
                .any(|cache| !cache.lock().unwrap().is_empty())
    }

    fn take_extent_from_global(&self, max_count: u32) -> Option<Extent> {
        let mut free = self.free_extents.lock().unwrap();
        let extent = free.iter().next().copied()?;
        free.remove(&extent);
        let take = extent.count.min(max_count);
        if extent.count > take {
            free.insert(Extent::new(
                Pba(extent.start.0 + take as u64),
                extent.count - take,
            ));
        }
        Some(Extent::new(extent.start, take))
    }

    fn take_extent_from_global_at_least(&self, count: u32, max_count: u32) -> Option<Extent> {
        let mut free = self.free_extents.lock().unwrap();
        let extent = free.iter().find(|e| e.count >= count).copied()?;
        free.remove(&extent);
        let take = extent.count.min(max_count);
        if extent.count > take {
            free.insert(Extent::new(
                Pba(extent.start.0 + take as u64),
                extent.count - take,
            ));
        }
        Some(Extent::new(extent.start, take))
    }

    fn take_from_extent_cache(cache: &mut Vec<Extent>, count: u32) -> Option<Extent> {
        let idx = cache.iter().position(|extent| extent.count >= count)?;
        let extent = cache[idx];
        let result = Extent::new(extent.start, count);
        if extent.count == count {
            cache.swap_remove(idx);
        } else {
            cache[idx] = Extent::new(Pba(extent.start.0 + count as u64), extent.count - count);
        }
        Some(result)
    }

    fn covering_extent(free: &BTreeSet<Extent>, pba: Pba) -> Option<Extent> {
        free.range(..=Extent::single(pba))
            .next_back()
            .copied()
            .filter(|extent| extent.contains(pba))
    }

    fn overlapping_extent(free: &BTreeSet<Extent>, extent: Extent) -> Option<Extent> {
        if let Some(before) = free.range(..=extent).next_back().copied() {
            if Self::extents_overlap(before, extent) {
                return Some(before);
            }
        }
        free.range(extent..)
            .next()
            .copied()
            .filter(|candidate| Self::extents_overlap(*candidate, extent))
    }

    fn extents_overlap(a: Extent, b: Extent) -> bool {
        a.start.0 < b.end_pba().0 && a.end_pba().0 > b.start.0
    }

    fn overlapping_retired_extent(&self, extent: Extent) -> Option<Extent> {
        let retired = self.retired_extents.lock().unwrap();
        Self::overlapping_extent(&retired, extent)
    }
}

#[cfg(test)]
mod age_tests {
    //! Reclaim-grace age mechanism: the fix for the re-aging bottleneck where a
    //! contiguous retired region perpetually absorbing fresh neighbors never
    //! satisfied the grace. Per-original-retire `retired_at` (injected here via
    //! `retire_extent_at`) is fixed and never refreshed by coalescing.
    use super::*;

    fn alloc_first(a: &SpaceAllocator, n: usize) -> u64 {
        let first = a.allocate_one().unwrap();
        for _ in 1..n {
            a.allocate_one().unwrap();
        }
        first.0
    }

    fn new_alloc(blocks: u64) -> SpaceAllocator {
        SpaceAllocator::new(blocks * BLOCK_SIZE as u64, 0)
    }

    const GRACE: Duration = Duration::from_secs(10);
    fn secs(s: u64) -> Duration {
        Duration::from_secs(s)
    }

    /// HEADLINE: an aged block reclaims even while an adjacent younger block keeps
    /// arriving — the exact scenario the old coalesced-key grace map starved.
    #[test]
    fn no_reaging_under_adjacent_retire() {
        let a = new_alloc(8192);
        let n = alloc_first(&a, 2);
        let t0 = Instant::now();
        a.retire_extent_at(Extent::single(Pba(n)), t0).unwrap();
        a.retire_extent_at(Extent::single(Pba(n + 1)), t0 + secs(5)).unwrap();
        // t0+11s: N aged (11≥10), N+1 still young (age 6<10).
        let (cands, deferred) = a.aged_candidates(64, GRACE, t0 + secs(11));
        assert_eq!(cands, vec![Extent::new(Pba(n), 1)]);
        assert_eq!(deferred, 1, "young neighbor deferred, NOT re-aging N");
        // Later both age in and (adjacent) merge into one fat candidate.
        let (cands2, deferred2) = a.aged_candidates(64, GRACE, t0 + secs(20));
        assert_eq!(cands2, vec![Extent::new(Pba(n), 2)]);
        assert_eq!(deferred2, 0);
    }

    /// Safety: a just-retired block is never a candidate before its grace.
    #[test]
    fn young_block_not_emitted_before_grace() {
        let a = new_alloc(64);
        let n = alloc_first(&a, 1);
        let t0 = Instant::now();
        a.retire_extent_at(Extent::single(Pba(n)), t0).unwrap();
        let (cands, deferred) = a.aged_candidates(64, GRACE, t0 + secs(5));
        assert!(cands.is_empty());
        assert_eq!(deferred, 1);
    }

    /// Idempotent re-retire does not refresh the original age (no re-aging).
    #[test]
    fn reretire_does_not_refresh_age() {
        let a = new_alloc(64);
        let n = alloc_first(&a, 1);
        let t0 = Instant::now();
        assert_eq!(a.retire_extent_at(Extent::single(Pba(n)), t0).unwrap(), 1);
        // Re-retire much later → newly==0, and the age must STILL be t0.
        assert_eq!(
            a.retire_extent_at(Extent::single(Pba(n)), t0 + secs(8))
                .unwrap(),
            0,
            "already-retired → no new blocks"
        );
        assert_eq!(a.retired_block_count(), 1);
        // At t0+11s it is eligible (age 11≥10). If the re-retire had refreshed to
        // t0+8s it would still be young (age 3<10) and NOT emitted.
        let (cands, _) = a.aged_candidates(64, GRACE, t0 + secs(11));
        assert_eq!(cands, vec![Extent::new(Pba(n), 1)]);
    }

    /// Partial-overlap retire records only the genuinely-new tail with a newer
    /// age; the already-retired prefix keeps its original (older) age.
    #[test]
    fn partial_overlap_ages_only_new_tail() {
        let a = new_alloc(64);
        let n = alloc_first(&a, 6);
        let t0 = Instant::now();
        assert_eq!(a.retire_extent_at(Extent::new(Pba(n), 3), t0).unwrap(), 3);
        // [N+1,3) overlaps N+1,N+2 (already retired) → only N+3 is new.
        assert_eq!(
            a.retire_extent_at(Extent::new(Pba(n + 1), 3), t0 + secs(5))
                .unwrap(),
            1
        );
        assert_eq!(a.retired_block_count(), 4);
        // t0+11s: N..N+2 aged (t0), N+3 young (t5, age 6) → emit [N,3] only.
        let (cands, deferred) = a.aged_candidates(64, GRACE, t0 + secs(11));
        assert_eq!(cands, vec![Extent::new(Pba(n), 3)]);
        assert_eq!(deferred, 1);
    }

    /// Throughput: contiguous aged retires emit as one fat extent, and the budget
    /// is in BLOCKS (a per-extent cap would collapse throughput).
    #[test]
    fn aged_candidates_merge_and_block_budget() {
        let a = new_alloc(4096);
        let n = alloc_first(&a, 1000);
        let t0 = Instant::now();
        a.retire_extent_at(Extent::new(Pba(n), 1000), t0).unwrap();
        // Whole contiguous run as ONE extent.
        let (cands, _) = a.aged_candidates(10_000, GRACE, t0 + secs(11));
        assert_eq!(cands, vec![Extent::new(Pba(n), 1000)]);
        // Block budget truncates to exactly 400 blocks (not 1 extent).
        let (capped, _) = a.aged_candidates(400, GRACE, t0 + secs(11));
        assert_eq!(capped, vec![Extent::new(Pba(n), 400)]);
    }

    /// Sub-extent reclaim splits a coalesced extent: free the aged prefix, keep
    /// the younger suffix retired.
    #[test]
    fn reclaim_splits_coalesced_extent() {
        let a = new_alloc(64);
        let n = alloc_first(&a, 6);
        let t0 = Instant::now();
        a.retire_extent_at(Extent::new(Pba(n), 3), t0).unwrap();
        a.retire_extent_at(Extent::new(Pba(n + 3), 3), t0 + secs(5))
            .unwrap(); // adjacent → set coalesces to [N,6]
        let (cands, _) = a.aged_candidates(64, GRACE, t0 + secs(11));
        assert_eq!(cands, vec![Extent::new(Pba(n), 3)]);
        assert!(a.reclaim_retired_extent(Extent::new(Pba(n), 3)).unwrap());
        for off in 0..3 {
            assert!(a.is_free(Pba(n + off)), "aged prefix freed");
        }
        for off in 3..6 {
            assert!(a.is_retired(Pba(n + off)), "younger suffix stays retired");
            assert!(!a.is_free(Pba(n + off)));
        }
        assert_eq!(a.retired_block_count(), 3);
    }
}
