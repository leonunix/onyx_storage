use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::error::{OnyxError, OnyxResult};
use crate::meta::store::MetaStore;
use crate::space::extent::Extent;
use crate::space::free_set::FreeSet;
use crate::space::hazard::PbaHazards;
use crate::types::{Pba, BLOCK_SIZE, RESERVED_BLOCKS};

/// Number of blocks to refill a lane cache from the global free list at once.
const LANE_CACHE_REFILL_SIZE: u32 = 256;
/// Number of blocks to reserve for each lane's contiguous extent cache.
/// Raw passthrough flushes commonly allocate 4-8 contiguous blocks per unit;
/// serving those from a lane-local slice avoids hammering the global BTreeSet.
const LANE_EXTENT_CACHE_REFILL_BLOCKS: u32 = 8192;
/// Per-chunk extent cap for the batched retire/reclaim paths
/// (`retire_extents_batch`, `reclaim_retired_extents_batch`). Bounds each
/// `retired`/`free`/lane lock hold to a small slice of in-memory BTree work
/// (~sub-millisecond) so the foreground alloc/retire path interleaves between
/// chunks instead of stalling on a single large hold.
const BATCH_LOCK_CHUNK: usize = 4096;

/// One original retire operation's age, tracked at retire granularity in the
/// `retired_age` log so coalescing the `retired_extents` set can never re-age it.
#[derive(Debug, Clone, Copy)]
struct RetiredRun {
    count: u32,
    retired_at: Instant,
}

pub struct SpaceAllocator {
    total_blocks: u64,
    /// Address-ordered free list + (count, start) side index. First-fit
    /// SELECTION is unchanged (lowest-address extent that fits — the metadb
    /// L2P leaf codec's dense-PBA contract); the side index only makes finding
    /// it O(D·log N) instead of an O(N) belt walk under this lock.
    free_extents: Mutex<FreeSet>,
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
    /// O(1) running total of blocks in `retired_extents`. The depth gauge is
    /// read once per GC cycle; summing the (potentially millions of) coalesced
    /// extents under the set lock was ~360 ms/cycle at 60M-deep AND contended
    /// the lock with the foreground retire path. This atomic is advisory (feeds
    /// only the `gc_retired_blocks_depth` metric, never a free decision), kept
    /// in lockstep with the set in `retire_extent_at`/`reclaim_retired_extent`.
    retired_blocks: AtomicU64,
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

    /// `device_size_bytes` is the IO-ADDRESSABLE capacity, NOT the raw device
    /// size: production callers pass `device.size() - RESERVED_BLOCKS *
    /// BLOCK_SIZE` (see `OnyxEngine`), because the `IoEngine` translates
    /// allocator PBA `p` to device block `p + RESERVED_BLOCKS` (its
    /// `pba_offset`). Passing the raw device size here would let the top
    /// RESERVED_BLOCKS PBAs write past the device end (chunklet "IO out of
    /// range: offset == capacity"). The bottom RESERVED_BLOCKS reserved below is
    /// the superblock / heartbeat / HA-lock region in the allocator's own space.
    pub fn new_with_hazards(device_size_bytes: u64, num_lanes: usize) -> Self {
        let total_blocks = device_size_bytes / BLOCK_SIZE as u64;
        let usable_blocks = total_blocks.saturating_sub(RESERVED_BLOCKS);
        let mut free_extents = FreeSet::new();
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
            retired_blocks: AtomicU64::new(0),
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

        {
            let mut fs = self.free_extents.lock().unwrap();
            // Preserve the startup-configured RAID geometry across the
            // wholesale rebuild (set_geometry rebuilds the eff index).
            let geom = fs.geometry();
            *fs = FreeSet::from_addr_set(free);
            if let Some((stripe, phase)) = geom {
                fs.set_geometry(stripe, phase);
            }
        }
        self.retired_extents.lock().unwrap().clear();
        self.retired_age.lock().unwrap().clear();
        self.retired_blocks.store(0, Ordering::Relaxed);
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

    /// Configure the engine's fixed LV3 RAID geometry so stripe-aligned
    /// first-fit queries use the effective-capacity index instead of a
    /// slack-check scan (65 ms/call on a 3M-fragment belt, inside the global
    /// free lock — the 2026-07-03 throughput-oscillation root cause). Call
    /// once at startup before flush traffic; idempotent; `stripe <= 1`
    /// (non-RAID backends) clears it.
    pub fn set_stripe_geometry(&self, stripe_blocks: u32, phase: u32) {
        self.free_extents
            .lock()
            .unwrap()
            .set_geometry(stripe_blocks, phase);
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
        crate::space::free_trace::trace_alloc(extent, context);
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
    /// Lowest-address extent first — `FreeSet::first` iterates by address.
    fn alloc_one_from_set(free: &mut FreeSet) -> Option<Pba> {
        let extent = free.first()?;
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
                free.coalesce_insert(Extent::single(pba));
            }
        }
        for cache_mutex in &self.lane_extent_caches {
            let mut cache = cache_mutex.lock().unwrap();
            for extent in cache.drain(..) {
                free.coalesce_insert(extent);
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
            .by_addr()
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

    /// Allocate a **stripe-aligned** contiguous extent for a flush lane.
    ///
    /// Returns an extent `e` with `(e.start.0 + phase) % stripe_blocks == 0` and
    /// `e.count == round_up(data_blocks, stripe_blocks)` — a whole number of
    /// full stripes whose *device* offset lands on a stripe boundary. The writer
    /// pads the payload to `e.count` blocks, so a chunklet RAID5/6 backend sees a
    /// full-stripe write and skips parity RMW. Only the `data_blocks` prefix is
    /// L2P-referenced; the tail-pad blocks are freed with the unit (via the
    /// caller's `alloc_blocks = e.count`) and never read.
    ///
    /// Alignment `phase` = `pba_offset % stripe_blocks` (device offset is
    /// `(pba + pba_offset) * block_size`, so PBAs must align against the reserved
    /// prefix, not 0 — see [`crate::io::IoEngine::stripe_phase`]).
    ///
    /// Stays lowest-address dense (first-fit). Alignment head/tail remainders go
    /// back to the lane cache / free list — never leaked, never allocated-counted.
    /// `stripe_blocks <= 1` degenerates to [`Self::allocate_extent_for_lane`] so
    /// non-RAID backends (RawDevice, mirror, plain) are byte-for-byte unchanged.
    pub fn allocate_stripe_extent_for_lane(
        &self,
        lane: usize,
        data_blocks: u32,
        stripe_blocks: u32,
        phase: u32,
    ) -> OnyxResult<Extent> {
        if stripe_blocks <= 1 {
            return self.allocate_extent_for_lane(lane, data_blocks);
        }
        if data_blocks == 0 {
            return Err(OnyxError::Config("cannot allocate 0 blocks".into()));
        }
        let need = Self::round_up_blocks(data_blocks, stripe_blocks);
        if lane >= self.lane_extent_caches.len() {
            return self.allocate_stripe_extent_global(need, stripe_blocks, phase);
        }

        // Fast path: carve an aligned `need` out of an already-cached run. Once
        // the cache is seeded with an aligned run, every tail it hands back is
        // itself aligned (tail.start = aligned + need, need % stripe == 0), so
        // steady-state carves have zero head pad.
        {
            let mut cache = self.lane_extent_caches[lane].lock().unwrap();
            if let Some(extent) =
                Self::take_aligned_from_extent_cache(&mut cache, need, stripe_blocks, phase)
            {
                self.track_alloc(extent, "allocate_stripe_extent_for_lane_cache")?;
                self.allocated_blocks.fetch_add(need as u64, Ordering::Relaxed);
                self.free_blocks.fetch_sub(need as u64, Ordering::Relaxed);
                return Ok(extent);
            }
        }

        // Refill: pull one run big enough to host `need` after worst-case head
        // alignment (`need + stripe - 1`), sized to the normal lane refill. Push
        // it into the cache and carve through the same remainder-handling path.
        let floor = need + stripe_blocks - 1;
        let want = LANE_EXTENT_CACHE_REFILL_BLOCKS.max(floor);
        let refill = self
            .take_extent_from_global_at_least(floor, want)
            .or_else(|| {
                self.drain_lane_caches();
                self.take_extent_from_global_at_least(floor, want)
            });
        let Some(refill) = refill else {
            // No run wide enough for a padded stripe; try any run that can host
            // an aligned `need` exactly (a tight but aligned fragment).
            return self.allocate_stripe_extent_global(need, stripe_blocks, phase);
        };
        let extent = {
            let mut cache = self.lane_extent_caches[lane].lock().unwrap();
            cache.push(refill);
            Self::take_aligned_from_extent_cache(&mut cache, need, stripe_blocks, phase)
                .expect("refill run of need+stripe-1 always hosts an aligned need")
        };
        self.track_alloc(extent, "allocate_stripe_extent_for_lane_refill")?;
        self.allocated_blocks.fetch_add(need as u64, Ordering::Relaxed);
        self.free_blocks.fetch_sub(need as u64, Ordering::Relaxed);
        Ok(extent)
    }

    /// Smallest multiple of `stripe` that is `>= data` (`stripe <= 1` → `data`).
    fn round_up_blocks(data: u32, stripe: u32) -> u32 {
        if stripe <= 1 {
            return data;
        }
        data.div_ceil(stripe) * stripe
    }

    /// Smallest PBA `>= from` with `(pba + phase) % stripe == 0`.
    fn align_up_pba(from: u64, stripe: u64, phase: u64) -> u64 {
        if stripe <= 1 {
            return from;
        }
        let r = (from + phase) % stripe;
        if r == 0 {
            from
        } else {
            from + (stripe - r)
        }
    }

    /// Carve a stripe-aligned `need`-block extent out of a contiguous `run`.
    /// `need` MUST already be a multiple of `stripe`. Returns
    /// `(aligned, head_pad, tail)` — head_pad = blocks below the aligned start,
    /// tail = blocks above `aligned + need` — or `None` if `run` can't host an
    /// aligned `need`.
    fn carve_aligned_from_run(
        run: Extent,
        need: u32,
        stripe: u32,
        phase: u32,
    ) -> Option<(Extent, Option<Extent>, Option<Extent>)> {
        let aligned_start = Self::align_up_pba(run.start.0, stripe as u64, phase as u64);
        let run_end = run.start.0 + run.count as u64;
        if aligned_start + need as u64 > run_end {
            return None;
        }
        let aligned = Extent::new(Pba(aligned_start), need);
        let head = aligned_start - run.start.0;
        let head_pad = (head > 0).then(|| Extent::new(run.start, head as u32));
        let tail_start = aligned_start + need as u64;
        let tail = (tail_start < run_end)
            .then(|| Extent::new(Pba(tail_start), (run_end - tail_start) as u32));
        Some((aligned, head_pad, tail))
    }

    /// Carve a stripe-aligned `need` from the first cached run that can host it,
    /// pushing head/tail remainders back into the cache. Head is only non-empty
    /// when a non-aligned run (e.g. a rest pushed by `allocate_extent_for_lane`)
    /// is the only candidate; it stays lane-local for a later non-stripe alloc.
    fn take_aligned_from_extent_cache(
        cache: &mut Vec<Extent>,
        need: u32,
        stripe: u32,
        phase: u32,
    ) -> Option<Extent> {
        for idx in 0..cache.len() {
            if let Some((aligned, head, tail)) =
                Self::carve_aligned_from_run(cache[idx], need, stripe, phase)
            {
                cache.swap_remove(idx);
                if let Some(head) = head {
                    cache.push(head);
                }
                if let Some(tail) = tail {
                    cache.push(tail);
                }
                return Some(aligned);
            }
        }
        None
    }

    /// Last-ditch stripe-aligned allocation straight from the global free list
    /// (no lane cache). Picks the lowest-address run that can host an aligned
    /// `need`, re-inserts head + tail as free. Returns `SpaceExhausted` rather
    /// than a misaligned/short extent — the writer falls back to an unaligned
    /// block-padded write so IO never stalls on alignment fragmentation.
    fn allocate_stripe_extent_global(
        &self,
        need: u32,
        stripe: u32,
        phase: u32,
    ) -> OnyxResult<Extent> {
        for attempt in 0..2 {
            let mut free = self.free_extents.lock().unwrap();
            // First-fit-by-address over the carve predicate, via the size
            // index (O(stripe·log N) instead of walking the fragment belt).
            // Selection is identical to the old
            // `iter().find(|e| carve_aligned_from_run(e, ..).is_some())`.
            let chosen = free.first_fit_aligned(need, stripe, phase);
            if let Some(run) = chosen {
                debug_assert!(
                    Self::carve_aligned_from_run(run, need, stripe, phase).is_some()
                );
                free.remove(&run);
                let (aligned, head, tail) =
                    Self::carve_aligned_from_run(run, need, stripe, phase).unwrap();
                if let Some(head) = head {
                    free.coalesce_insert(head);
                }
                if let Some(tail) = tail {
                    free.coalesce_insert(tail);
                }
                drop(free);
                self.track_alloc(aligned, "allocate_stripe_extent_global")?;
                self.allocated_blocks.fetch_add(need as u64, Ordering::Relaxed);
                self.free_blocks.fetch_sub(need as u64, Ordering::Relaxed);
                return Ok(aligned);
            }
            drop(free);
            if attempt == 0 && self.has_lane_cached_blocks() {
                self.drain_lane_caches();
                continue;
            }
            break;
        }
        Err(OnyxError::SpaceExhausted)
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

            // First (lowest-address) extent that's large enough.
            let exact = free.first_fit(count);

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
            let largest = free.largest();
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

        let newly = {
            let free = self.free_extents.lock().unwrap();
            if let Some(e) = Self::overlapping_extent(free.by_addr(), extent) {
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
                // Keep the O(1) depth gauge in lockstep with the set (only the
                // genuinely-new blocks; idempotent re-retire adds nothing).
                self.retired_blocks
                    .fetch_add(u64::from(newly), Ordering::Relaxed);
            }
            newly
        };
        // Diagnostic trace OUTSIDE the free/retired locks — per-block map
        // inserts inside the global lock section serialise every allocator
        // client (measured collapse on the 2026-07-02 capture run).
        crate::space::free_trace::trace_retire(extent, "retire_extent");
        Ok(newly)
    }

    /// Batch analogue of [`Self::retire_extent_at`] for the foreground cleanup
    /// path (`retire_dead_pbas`). The single-extent retire pays, PER extent,
    /// `ensure_not_in_lane_cache` (~2×num_lanes mutexes) + the `free`,
    /// `retired` and `retired_age` locks — and the cleanup threads run it at
    /// ~the foreground overwrite rate (~11K/s system-wide), hammering the exact
    /// global locks the GC reclaim path needs. That mutual contention is the
    /// residual reclaim-latency floor. This amortizes the lane snapshot + every
    /// lock over a bounded `chunk`, collapsing ~11K per-extent acquisitions/s
    /// into a handful of chunk-holds/s.
    ///
    /// Returns `(total_newly_blocks, failed_extents)`. Lock order matches the
    /// single path exactly — `free` (outermost, held across) → `retired` →
    /// `retired_age` — so a concurrent free cannot race the overlap check, and
    /// there is no inversion with the reclaim batch (which never holds `free`
    /// and `retired` at the same time).
    pub fn retire_extents_batch(&self, extents: &[Extent], now: Instant) -> (u64, Vec<Extent>) {
        let mut total_newly: u64 = 0;
        let mut failed: Vec<Extent> = Vec::new();
        for (chunk_idx, chunk) in extents.chunks(BATCH_LOCK_CHUNK).enumerate() {
            // Same inter-chunk breather as `reclaim_retired_extents_batch`:
            // callers are the background cleanup thread / lineage drain /
            // volume delete, and each chunk holds the free+retired locks the
            // flush writers allocate under.
            if chunk_idx > 0 {
                std::thread::sleep(Duration::from_micros(500));
            }
            let (lane_pbas, lane_exts) = self.snapshot_lane_caches();
            let current_alloc = self.allocated_blocks.load(Ordering::Relaxed);
            let mut chunk_newly: u64 = 0;
            let mut chunk_retired: Vec<Extent> = Vec::new();
            // Lock order: free (outermost) → retired → retired_age, held across
            // the chunk (matches `retire_extent_at`).
            let free = self.free_extents.lock().unwrap();
            let mut retired = self.retired_extents.lock().unwrap();
            let mut age = self.retired_age.lock().unwrap();
            for &extent in chunk {
                if self.validate_extent_shape(extent, "retire_extents_batch").is_err() {
                    failed.push(extent);
                    continue;
                }
                let in_lane = (0..extent.count)
                    .any(|i| lane_pbas.contains(&Pba(extent.start.0 + i as u64)))
                    || Self::sorted_extents_overlap(&lane_exts, extent);
                if in_lane
                    || Self::overlapping_extent(free.by_addr(), extent).is_some()
                    || u64::from(extent.count) > current_alloc
                {
                    failed.push(extent);
                    continue;
                }
                // Genuinely-new sub-ranges (computed before coalescing so
                // already-retired sub-ranges keep their original age).
                let gaps = Self::uncovered_subranges(&retired, extent);
                let newly: u32 = gaps.iter().map(|g| g.count).sum();
                Self::coalesce_and_insert_any_overlap(&mut retired, extent);
                chunk_retired.push(extent);
                if newly > 0 {
                    for g in gaps {
                        age.insert(
                            g.start.0,
                            RetiredRun {
                                count: g.count,
                                retired_at: now,
                            },
                        );
                    }
                    chunk_newly += u64::from(newly);
                }
            }
            drop(age);
            drop(retired);
            drop(free);
            // Diagnostic trace outside the lock section (see retire_extent_at).
            for &extent in &chunk_retired {
                crate::space::free_trace::trace_retire(extent, "retire_batch");
            }
            if chunk_newly > 0 {
                self.retired_blocks
                    .fetch_add(chunk_newly, Ordering::Relaxed);
                total_newly += chunk_newly;
            }
        }
        (total_newly, failed)
    }

    /// Batch analogue of [`Self::free_extent`] (never-committed rollback
    /// frees). The single-extent path pays, PER extent,
    /// `ensure_not_in_lane_cache` (~2×num_lanes mutexes) + the `free` lock
    /// TWICE (validate + insert) + the `retired` lock — and the commit workers
    /// run it per discarded/superseded unit (and per dead raw sub-block) at
    /// the overwrite rate, hammering the exact lock the shard writers need for
    /// allocation. This amortizes the lane snapshot and every lock over a
    /// bounded chunk, mirroring [`Self::retire_extents_batch`].
    ///
    /// Semantics per extent match the single path's authoritative in-lock
    /// re-check (the single path's pre-lock validate is only an early-out):
    /// shape, lane-cache overlap (via the chunk snapshot), free-list overlap,
    /// retired overlap, counter underflow. Failures are returned in `failed`
    /// and leave that extent untouched (callers today `let _ =` single-free
    /// errors; batched callers warn-log the aggregate).
    ///
    /// Lock order matches the single path exactly — `free` (outermost) →
    /// `retired` (inner; the single path takes `retired` inside the held
    /// `free` via `overlapping_retired_extent`) — no inversion with retire
    /// (free→retired→age) or the reclaim batch (never holds free+retired
    /// together).
    pub fn free_extents_batch(&self, extents: &[Extent]) -> (u64, Vec<Extent>) {
        let mut total_freed: u64 = 0;
        let mut failed: Vec<Extent> = Vec::new();
        for chunk in extents.chunks(BATCH_LOCK_CHUNK) {
            let (lane_pbas, lane_exts) = self.snapshot_lane_caches();
            // Hazard barrier outside all locks (matches the single path's
            // wait; cheap when unpinned). Shape-invalid extents are skipped
            // here and rejected below.
            for extent in chunk {
                if extent.count > 0 && extent.end_pba().0 <= self.total_blocks {
                    self.hazards.wait_extent_clear(extent.start, extent.count);
                }
            }

            let mut chunk_freed: u64 = 0;
            let mut chunk_released: Vec<Extent> = Vec::with_capacity(chunk.len());
            {
                let mut free = self.free_extents.lock().unwrap();
                let retired = self.retired_extents.lock().unwrap();
                for &extent in chunk {
                    if self.validate_extent_shape(extent, "free_extents_batch").is_err() {
                        failed.push(extent);
                        continue;
                    }
                    let in_lane = (0..extent.count)
                        .any(|i| lane_pbas.contains(&Pba(extent.start.0 + i as u64)))
                        || Self::sorted_extents_overlap(&lane_exts, extent);
                    if in_lane
                        || Self::overlapping_extent(free.by_addr(), extent).is_some()
                        || Self::overlapping_extent(&retired, extent).is_some()
                    {
                        failed.push(extent);
                        continue;
                    }
                    // Underflow guard with the running debit (counters are
                    // only applied once per chunk, so the raw load is stale
                    // within the chunk).
                    let avail = self
                        .allocated_blocks
                        .load(Ordering::Relaxed)
                        .saturating_sub(chunk_freed);
                    if u64::from(extent.count) > avail {
                        failed.push(extent);
                        continue;
                    }
                    free.coalesce_insert(extent);
                    self.track_release(extent, "free_extents_batch");
                    chunk_released.push(extent);
                    chunk_freed += u64::from(extent.count);
                }
            }
            // Diagnostic trace outside the lock section (see retire_extent_at).
            for &extent in &chunk_released {
                crate::space::free_trace::trace_free(extent, "free_batch");
            }
            if chunk_freed > 0 {
                self.allocated_blocks
                    .fetch_sub(chunk_freed, Ordering::Relaxed);
                self.free_blocks.fetch_add(chunk_freed, Ordering::Relaxed);
                total_freed += chunk_freed;
            }
        }
        (total_freed, failed)
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

    /// O(1) total of retired blocks (advisory gauge). Maintained in lockstep
    /// with `retired_extents` by `retire_extent_at`/`reclaim_retired_extent`;
    /// see [`Self::retired_block_count_exact`] for the audit-grade walk.
    pub fn retired_block_count(&self) -> u64 {
        self.retired_blocks.load(Ordering::Relaxed)
    }

    /// Audit-grade exact retired-block total by walking the coalesced set
    /// (O(#extents)). The cheap [`Self::retired_block_count`] gauge should equal
    /// this; tests assert the two agree.
    #[cfg(test)]
    pub fn retired_block_count_exact(&self) -> u64 {
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
            if let Some(e) = Self::overlapping_extent(free.by_addr(), extent) {
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

            free.coalesce_insert(extent);
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
        if result.is_ok() {
            // `extent` left the retired set for the free list — decrement the
            // O(1) gauge. Failure re-inserted it above, so leave the gauge.
            self.retired_blocks
                .fetch_sub(u64::from(extent.count), Ordering::Relaxed);
            // Diagnostic trace outside the free lock (see retire_extent_at).
            crate::space::free_trace::trace_reclaim(extent, "reclaim");
        }
        result.map(|_| true)
    }

    /// Batch analogue of [`Self::reclaim_retired_extent`] for the GC reclaim
    /// free-loop. The single-extent path paid, PER extent, ~`2 × num_lanes`
    /// lane-cache mutex locks (`ensure_not_in_lane_cache`) + the `retired`,
    /// `retired_age` and `free` locks — all contending the foreground at
    /// 11-13K/s, ~138 µs/extent. Under scattered churn the retired set
    /// fragments into ~single-block extents, so a block-budgeted cycle reclaimed
    /// up to `MAX_RETIRED_RECLAIM_BLOCKS_PER_CYCLE` *extents* → reclaim cost grew
    /// super-linearly with retired depth (the capacity runaway). This amortizes
    /// every per-extent lock and the lane-cache scan over a bounded `chunk`, so
    /// the cost is O(blocks) with a small constant.
    ///
    /// `extents` must already be GC-proven (Gate-1 rc==0 + pre-Gate-2 hazard
    /// barrier + Gate-2 consistent recheck) by the caller. Returns
    /// `(freed_blocks, freed_extents)`. Lock discipline matches the single path:
    /// never holds `free` and `retired` at the same time (Phase A removes under
    /// `retired`, Phase B inserts under `free`), and lane caches are snapshotted
    /// with neither held.
    pub fn reclaim_retired_extents_batch(
        &self,
        extents: &[Extent],
        running: &AtomicBool,
    ) -> OnyxResult<(u64, usize)> {
        let mut freed_blocks: u64 = 0;
        let mut freed_extents: usize = 0;
        for (chunk_idx, chunk) in extents.chunks(BATCH_LOCK_CHUNK).enumerate() {
            if !running.load(Ordering::Relaxed) {
                break;
            }
            // Breathe between chunk lock-holds: this runs on the GC thread
            // (latency-insensitive) but each Phase-B hold does up to 4096
            // coalesce-inserts (~tens of ms on a multi-million-extent free
            // list). Re-acquiring immediately wins the (unfair) mutex over the
            // 16 parked flush writers — box-measured as 22-80 thread-s/s alloc
            // convoy spikes phase-locked to every 262K-block reclaim batch.
            // A short sleep guarantees the foreground a window per chunk.
            if chunk_idx > 0 {
                std::thread::sleep(Duration::from_micros(500));
            }
            // Snapshot the lane caches ONCE per chunk (vs once per extent). Same
            // mutexes/contents the single-extent `ensure_not_in_lane_cache`
            // checks; taken with neither `free` nor `retired` held, as today.
            let (lane_pbas, lane_exts) = self.snapshot_lane_caches();

            // Hazard barrier over the chunk. The caller already drained readers
            // for all survivors before Gate-2; this re-check matches the
            // single-extent path's `wait_extent_clear` (cheap when unpinned).
            for extent in chunk {
                self.hazards.wait_extent_clear(extent.start, extent.count);
            }

            // Phase A — `retired` (+ `retired_age`) lock ONCE: validate
            // containment, split out the covering coalesced extent, keep the
            // remainders retired. Collect the validated extents for Phase B.
            let mut removed: Vec<Extent> = Vec::with_capacity(chunk.len());
            {
                let mut retired = self.retired_extents.lock().unwrap();
                let mut age = self.retired_age.lock().unwrap();
                for &extent in chunk {
                    if self
                        .validate_extent_shape(extent, "reclaim_retired_extents_batch")
                        .is_err()
                    {
                        continue; // defensive: GC candidates are always well-formed
                    }
                    // Fail closed if no longer fully retired (raced reclaim/realloc).
                    let cover = match Self::covering_extent(&retired, extent.start) {
                        Some(c) if c.end_pba().0 >= extent.end_pba().0 => c,
                        _ => continue,
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
                    Self::purge_age_range(&mut age, extent);
                    removed.push(extent);
                }
            }

            // Phase B — `free` lock ONCE: re-validate against the lane snapshot +
            // free-list overlap (double-free guard), free the clean ones, defer
            // conflicts. `chunk_freed` tracks the not-yet-applied allocated debit
            // so the underflow guard stays honest within the chunk.
            let mut conflicts: Vec<Extent> = Vec::new();
            let mut chunk_reclaimed: Vec<Extent> = Vec::new();
            let mut chunk_freed: u64 = 0;
            {
                let mut free = self.free_extents.lock().unwrap();
                for &extent in &removed {
                    let in_lane = (0..extent.count)
                        .any(|i| lane_pbas.contains(&Pba(extent.start.0 + i as u64)))
                        || Self::sorted_extents_overlap(&lane_exts, extent);
                    if in_lane || Self::overlapping_extent(free.by_addr(), extent).is_some() {
                        conflicts.push(extent);
                        continue;
                    }
                    let avail = self
                        .allocated_blocks
                        .load(Ordering::Relaxed)
                        .saturating_sub(chunk_freed);
                    if u64::from(extent.count) > avail {
                        conflicts.push(extent);
                        continue;
                    }
                    free.coalesce_insert(extent);
                    self.track_release(extent, "reclaim_retired_extents_batch");
                    chunk_reclaimed.push(extent);
                    chunk_freed += u64::from(extent.count);
                    freed_extents += 1;
                }
            }
            // Diagnostic trace outside the free lock (see retire_extent_at).
            for &extent in &chunk_reclaimed {
                crate::space::free_trace::trace_reclaim(extent, "reclaim_batch");
            }

            // Counter debits ONCE per chunk (Relaxed gauges; same end state as
            // the single-extent per-op debit).
            if chunk_freed > 0 {
                self.allocated_blocks
                    .fetch_sub(chunk_freed, Ordering::Relaxed);
                self.free_blocks.fetch_add(chunk_freed, Ordering::Relaxed);
                self.retired_blocks
                    .fetch_sub(chunk_freed, Ordering::Relaxed);
                freed_blocks += chunk_freed;
            }

            // Re-insert conflicts: they stay retired (coalescing back with the
            // remainders), age untouched — matches the single path's error path.
            if !conflicts.is_empty() {
                let mut retired = self.retired_extents.lock().unwrap();
                for extent in conflicts {
                    Self::coalesce_and_insert_any_overlap(&mut retired, extent);
                }
            }
        }
        Ok((freed_blocks, freed_extents))
    }

    /// Snapshot the per-lane block + extent caches into owned collections so the
    /// batch reclaim can check membership without re-locking per extent. Each
    /// lane mutex is taken briefly and independently (no `free`/`retired` held),
    /// matching `ensure_not_in_lane_cache`'s lock discipline.
    /// Returned extents are sorted by start (lane-cached extents never overlap
    /// each other — they are disjoint carves off the global pool), so callers
    /// can overlap-test in O(log M) via [`Self::sorted_extents_overlap`]. The
    /// old linear `iter().any(extents_overlap)` per candidate extent was the
    /// stall root cause: 4096-extent reclaim/retire chunks × tens of thousands
    /// of cached fragment rests = 10^8 comparisons per chunk INSIDE the global
    /// free lock (gc-runner pegged at 84% self time in
    /// `reclaim_retired_extents_batch`, all 16 writers parked on the lock —
    /// 2026-07-02 perf capture).
    fn snapshot_lane_caches(&self) -> (HashSet<Pba>, Vec<Extent>) {
        let mut pbas = HashSet::new();
        for cache in &self.lane_caches {
            pbas.extend(cache.lock().unwrap().iter().copied());
        }
        let mut exts = Vec::new();
        for cache in &self.lane_extent_caches {
            exts.extend(cache.lock().unwrap().iter().copied());
        }
        exts.sort_unstable_by_key(|e| e.start.0);
        (pbas, exts)
    }

    /// Binary-search overlap test against a start-sorted, mutually-disjoint
    /// extent list (the [`Self::snapshot_lane_caches`] output). Only two
    /// candidates can overlap `extent`: the last one starting at/before it and
    /// the first one starting after it.
    fn sorted_extents_overlap(sorted: &[Extent], extent: Extent) -> bool {
        let idx = sorted.partition_point(|e| e.start.0 <= extent.start.0);
        if idx > 0 && Self::extents_overlap(sorted[idx - 1], extent) {
            return true;
        }
        idx < sorted.len() && Self::extents_overlap(sorted[idx], extent)
    }

    fn free_extent_unchecked_ownership(&self, extent: Extent) -> OnyxResult<()> {
        self.validate_free_extent(extent)?;

        self.hazards.wait_extent_clear(extent.start, extent.count);

        {
            let mut free = self.free_extents.lock().unwrap();
            self.ensure_not_free_or_retired_after_wait(extent, free.by_addr())?;
            free.coalesce_insert(extent);
            self.track_release(extent, "free_extent");
        }
        // Diagnostic trace outside the free lock (see retire_extent_at).
        crate::space::free_trace::trace_free(extent, "free_extent");
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
        if let Some(e) = Self::overlapping_extent(free.by_addr(), extent) {
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
        if free
            .by_addr()
            .range(..=extent)
            .next_back()
            .is_some_and(|existing| {
                extent.start.0 >= existing.start.0 && extent.end_pba().0 <= existing.end_pba().0
            })
        {
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

    /// Number of distinct runs in the global free set. Test-only: the stripe
    /// density guard asserts this stays O(1) under repeated aligned allocation
    /// (alignment pads must not fragment the free list into per-alloc slivers).
    #[cfg(test)]
    pub(crate) fn free_extent_run_count(&self) -> usize {
        self.free_extents.lock().unwrap().len()
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
        let extent = free.first()?;
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
        // First-fit-by-address via the size index — O(D·log N), and the "no
        // run >= count exists" refill miss is a fail-fast instead of an O(N)
        // walk of the whole fragment belt under this lock.
        let extent = free.first_fit(count)?;
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

    /// PERF microbench (NOT a correctness gate): isolate the two per-GC-cycle
    /// reclaim-SELECTION costs that scale with retired-set depth —
    /// `retired_block_count()` (O(#retired extents), called once/cycle purely
    /// for the depth gauge) and `aged_candidates()` (walks the set + prunes the
    /// age log). Directly populates the private structures to model the prod
    /// steady state (60M-deep, heavily fragmented) without the alloc/free
    /// machinery. Run: `cargo test --release -p onyx-storage --lib
    /// bench_reclaim_selection_scaling -- --ignored --nocapture`.
    #[test]
    #[ignore = "perf microbench"]
    fn bench_reclaim_selection_scaling() {
        let base = Instant::now();
        let now = base + Duration::from_secs(100);
        let grace = Duration::from_secs(30);
        // Young front modelled as one grace-window of retires (the only entries
        // the age log holds in steady state — aged_candidates prunes the rest
        // each cycle). `aged_only`=age log already empty (best case: walk emits
        // budget off the front and stops). `all_in_age`=degenerate worst case
        // where a slow cycle let the age log accumulate to full depth before a
        // prune (bounds the retain()/sum() cost).
        for &n in &[1_000_000u64, 10_000_000, 30_000_000, 60_000_000] {
            for mode in ["aged_only", "front_young", "all_in_age"] {
                let dev = (2 * n + RESERVED_BLOCKS + 16) * BLOCK_SIZE as u64;
                let a = SpaceAllocator::new(dev, 0);
                let young_front = 400_000u64.min(n);
                {
                    let mut retired = a.retired_extents.lock().unwrap();
                    let mut age = a.retired_age.lock().unwrap();
                    for i in 0..n {
                        let pba = RESERVED_BLOCKS + 2 * i; // stride 2 → N separate extents (max frag)
                        retired.insert(Extent::new(Pba(pba), 1));
                        match mode {
                            "aged_only" => {}
                            "front_young" => {
                                if i < young_front {
                                    age.insert(pba, RetiredRun { count: 1, retired_at: now });
                                }
                            }
                            "all_in_age" => {
                                let retired_at = if i < young_front { now } else { base };
                                age.insert(pba, RetiredRun { count: 1, retired_at });
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                a.allocated_blocks.store(2 * n, Ordering::Relaxed);
                a.retired_blocks.store(n, Ordering::Relaxed); // direct-insert bypassed the gauge

                // Times the OLD O(#extents) walk we replaced with the O(1) gauge.
                let t = Instant::now();
                let depth = a.retired_block_count_exact();
                let d_rbc = t.elapsed().as_secs_f64() * 1e3;
                debug_assert_eq!(depth, a.retired_block_count());

                let t = Instant::now();
                let (cands, deferred) = a.aged_candidates(262_144, grace, now);
                let d_aged = t.elapsed().as_secs_f64() * 1e3;
                let emitted: u64 = cands.iter().map(|e| e.count as u64).sum();

                println!(
                    "N={:>10} mode={:<11} depth={:>10} | retired_block_count={:>8.1}ms | \
                     aged_candidates={:>8.1}ms emitted={:>7} deferred={:>9} cands={}",
                    n, mode, depth, d_rbc, d_aged, emitted, deferred, cands.len()
                );
            }
        }
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
        // The O(1) gauge must agree with the exact walk through retire + the
        // sub-extent split reclaim (drift guard for the atomic).
        assert_eq!(a.retired_block_count(), a.retired_block_count_exact());
    }

    fn run_flag() -> AtomicBool {
        AtomicBool::new(true)
    }

    /// The batched reclaim must leave identical allocator state to reclaiming the
    /// same extents one-by-one through the single-extent path.
    #[test]
    fn batch_reclaim_equals_sequence() {
        let mk = || {
            let a = new_alloc(4096);
            let n = alloc_first(&a, 100);
            let t0 = Instant::now();
            for i in 0..100u64 {
                a.retire_extent_at(Extent::single(Pba(n + i)), t0).unwrap();
            }
            (a, n)
        };
        let extents: Vec<Extent> = {
            let (_, n) = mk();
            (0..100u64).map(|i| Extent::single(Pba(n + i))).collect()
        };
        let (a_seq, _) = mk();
        for e in &extents {
            assert!(a_seq.reclaim_retired_extent(*e).unwrap());
        }
        let (a_batch, n) = mk();
        let (blocks, cnt) = a_batch
            .reclaim_retired_extents_batch(&extents, &run_flag())
            .unwrap();
        assert_eq!(blocks, 100);
        assert_eq!(cnt, 100);
        assert_eq!(a_seq.free_block_count(), a_batch.free_block_count());
        assert_eq!(a_seq.retired_block_count(), a_batch.retired_block_count());
        assert_eq!(a_batch.retired_block_count(), 0);
        assert_eq!(a_batch.retired_block_count(), a_batch.retired_block_count_exact());
        for i in 0..100u64 {
            assert!(a_batch.is_free(Pba(n + i)));
        }
    }

    /// A batch entry that is a sub-range of a coalesced retired extent splits it:
    /// free the named sub-range, keep the rest retired.
    #[test]
    fn batch_reclaim_splits_sub_extent() {
        let a = new_alloc(64);
        let n = alloc_first(&a, 6);
        let t0 = Instant::now();
        a.retire_extent_at(Extent::new(Pba(n), 6), t0).unwrap(); // one coalesced [n,6]
        let (blocks, cnt) = a
            .reclaim_retired_extents_batch(&[Extent::new(Pba(n), 3)], &run_flag())
            .unwrap();
        assert_eq!((blocks, cnt), (3, 1));
        for off in 0..3 {
            assert!(a.is_free(Pba(n + off)));
        }
        for off in 3..6 {
            assert!(a.is_retired(Pba(n + off)));
        }
        assert_eq!(a.retired_block_count(), 3);
        assert_eq!(a.retired_block_count(), a.retired_block_count_exact());
    }

    /// An extent that is no longer (fully) retired — e.g. raced realloc — is
    /// skipped (fail closed); the rest of the batch still reclaims.
    #[test]
    fn batch_reclaim_fail_closed_on_non_retired() {
        let a = new_alloc(64);
        let n = alloc_first(&a, 12);
        let t0 = Instant::now();
        a.retire_extent_at(Extent::single(Pba(n)), t0).unwrap();
        // n+10 is allocated but never retired → not reclaimable.
        let batch = [Extent::single(Pba(n)), Extent::single(Pba(n + 10))];
        let (blocks, cnt) = a.reclaim_retired_extents_batch(&batch, &run_flag()).unwrap();
        assert_eq!((blocks, cnt), (1, 1));
        assert!(a.is_free(Pba(n)));
        assert!(!a.is_free(Pba(n + 10)), "non-retired entry untouched");
        assert!(!a.is_retired(Pba(n + 10)));
        assert_eq!(a.retired_block_count(), 0);
        assert_eq!(a.retired_block_count(), a.retired_block_count_exact());
    }

    /// An extent removed from the retired set in Phase A but found to overlap the
    /// free list in Phase B (a should-never-happen inconsistency) is NOT
    /// double-freed: it is re-inserted and stays retired.
    #[test]
    fn batch_reclaim_conflict_reinserts() {
        let a = new_alloc(64);
        let n = alloc_first(&a, 4);
        let p = Pba(n);
        let t0 = Instant::now();
        a.retire_extent_at(Extent::single(p), t0).unwrap();
        // Inject the inconsistency: the same PBA is also in the free list.
        a.free_extents.lock().unwrap().insert_for_test(Extent::single(p));
        let (blocks, cnt) = a
            .reclaim_retired_extents_batch(&[Extent::single(p)], &run_flag())
            .unwrap();
        assert_eq!((blocks, cnt), (0, 0), "free-overlap conflict not freed");
        assert!(a.is_retired(p), "conflict re-inserted, stays retired");
        assert_eq!(a.retired_block_count(), 1);
        assert_eq!(a.retired_block_count(), a.retired_block_count_exact());
    }

    /// A batch larger than `BATCH_LOCK_CHUNK` reclaims every extent across chunks.
    #[test]
    fn batch_reclaim_spans_chunks() {
        let count = BATCH_LOCK_CHUNK + 50;
        let a = new_alloc((4 * count as u64) + 256);
        let base = alloc_first(&a, 2 * count); // 2× so stride-2 retires stay separate
        let t0 = Instant::now();
        let extents: Vec<Extent> = (0..count as u64)
            .map(|i| Extent::single(Pba(base + 2 * i)))
            .collect();
        for e in &extents {
            a.retire_extent_at(*e, t0).unwrap();
        }
        assert_eq!(a.retired_block_count(), count as u64);
        let (blocks, cnt) = a
            .reclaim_retired_extents_batch(&extents, &run_flag())
            .unwrap();
        assert_eq!(blocks, count as u64);
        assert_eq!(cnt, count);
        assert_eq!(a.retired_block_count(), 0);
        assert_eq!(a.retired_block_count(), a.retired_block_count_exact());
        for i in 0..count as u64 {
            assert!(a.is_free(Pba(base + 2 * i)));
        }
    }

    /// Mixed batch (some freed, one non-retired skip, one free-overlap conflict)
    /// keeps the O(1) gauge in lockstep with the exact walk.
    #[test]
    fn batch_reclaim_gauge_stays_consistent() {
        let a = new_alloc(64);
        let n = alloc_first(&a, 12);
        let t0 = Instant::now();
        a.retire_extent_at(Extent::new(Pba(n), 4), t0).unwrap();
        a.retire_extent_at(Extent::single(Pba(n + 8)), t0).unwrap();
        a.free_extents
            .lock()
            .unwrap()
            .insert_for_test(Extent::single(Pba(n + 8))); // conflict on n+8
        let batch = [
            Extent::new(Pba(n), 2),         // freed (sub-extent of [n,4])
            Extent::single(Pba(n + 10)),    // skip (never retired)
            Extent::single(Pba(n + 8)),     // conflict → stays retired
        ];
        let (blocks, _) = a.reclaim_retired_extents_batch(&batch, &run_flag()).unwrap();
        assert_eq!(blocks, 2);
        assert_eq!(a.retired_block_count(), a.retired_block_count_exact());
        assert_eq!(a.retired_block_count(), 3); // [n+2,2] remainder + n+8
    }

    /// The batched retire must leave identical retired state to retiring the
    /// same extents one-by-one through the single-extent path.
    #[test]
    fn batch_retire_equals_sequence() {
        let a_seq = new_alloc(4096);
        let n = alloc_first(&a_seq, 100);
        let t0 = Instant::now();
        for i in 0..100u64 {
            a_seq.retire_extent_at(Extent::single(Pba(n + i)), t0).unwrap();
        }
        let a_batch = new_alloc(4096);
        let nb = alloc_first(&a_batch, 100);
        assert_eq!(n, nb); // fresh allocators start at the same PBA
        let extents: Vec<Extent> = (0..100u64).map(|i| Extent::single(Pba(n + i))).collect();
        let (newly, failed) = a_batch.retire_extents_batch(&extents, t0);
        assert_eq!(newly, 100);
        assert!(failed.is_empty());
        assert_eq!(a_seq.retired_block_count(), a_batch.retired_block_count());
        assert_eq!(a_batch.retired_block_count(), 100);
        assert_eq!(a_batch.retired_block_count(), a_batch.retired_block_count_exact());
        for i in 0..100u64 {
            assert!(a_batch.is_retired(Pba(n + i)));
        }
    }

    /// Idempotent re-retire inside a batch counts only genuinely-new blocks.
    #[test]
    fn batch_retire_idempotent_recounts() {
        let a = new_alloc(64);
        let n = alloc_first(&a, 4);
        let t0 = Instant::now();
        let (newly1, f1) = a.retire_extents_batch(&[Extent::new(Pba(n), 2)], t0);
        assert_eq!((newly1, f1.len()), (2, 0));
        // Re-retire [n,2] (idempotent → 0 new) plus a fresh [n+2,1].
        let (newly2, f2) =
            a.retire_extents_batch(&[Extent::new(Pba(n), 2), Extent::single(Pba(n + 2))], t0);
        assert_eq!((newly2, f2.len()), (1, 0));
        assert_eq!(a.retired_block_count(), 3);
        assert_eq!(a.retired_block_count(), a.retired_block_count_exact());
    }

    /// An extent overlapping the free list is rejected (returned in `failed`),
    /// the rest of the batch still retires.
    #[test]
    fn batch_retire_rejects_free_overlap() {
        let a = new_alloc(64);
        let n = alloc_first(&a, 4); // allocates n..n+3; n+10 is free
        let t0 = Instant::now();
        let (newly, failed) =
            a.retire_extents_batch(&[Extent::single(Pba(n)), Extent::single(Pba(n + 10))], t0);
        assert_eq!(newly, 1);
        assert_eq!(failed, vec![Extent::single(Pba(n + 10))]);
        assert!(a.is_retired(Pba(n)));
        assert!(!a.is_retired(Pba(n + 10)));
        assert_eq!(a.retired_block_count(), a.retired_block_count_exact());
    }

    /// A batch larger than `BATCH_LOCK_CHUNK` retires every extent across chunks.
    #[test]
    fn batch_retire_spans_chunks() {
        let count = BATCH_LOCK_CHUNK + 30;
        let a = new_alloc((4 * count as u64) + 256);
        let base = alloc_first(&a, 2 * count);
        let extents: Vec<Extent> = (0..count as u64)
            .map(|i| Extent::single(Pba(base + 2 * i)))
            .collect();
        let (newly, failed) = a.retire_extents_batch(&extents, Instant::now());
        assert_eq!(newly, count as u64);
        assert!(failed.is_empty());
        assert_eq!(a.retired_block_count(), count as u64);
        assert_eq!(a.retired_block_count(), a.retired_block_count_exact());
    }

    /// The batched free must leave identical allocator state to freeing the
    /// same extents one-by-one through `free_one`/`free_extent`.
    #[test]
    fn batch_free_equals_sequence() {
        let mk = || {
            let a = new_alloc(4096);
            let n = alloc_first(&a, 200);
            (a, n)
        };
        // Stride-2 singles + a couple of multi-block extents.
        let extents: Vec<Extent> = {
            let (_, n) = mk();
            let mut v: Vec<Extent> = (0..50u64).map(|i| Extent::single(Pba(n + 2 * i))).collect();
            v.push(Extent::new(Pba(n + 120), 4));
            v.push(Extent::new(Pba(n + 130), 8));
            v
        };
        let (a_seq, _) = mk();
        for e in &extents {
            a_seq.free_extent(*e).unwrap();
        }
        let (a_batch, _) = mk();
        let (freed, failed) = a_batch.free_extents_batch(&extents);
        assert_eq!(freed, 50 + 4 + 8);
        assert!(failed.is_empty());
        assert_eq!(a_seq.free_block_count(), a_batch.free_block_count());
        assert_eq!(a_seq.allocated_block_count(), a_batch.allocated_block_count());
        assert_eq!(
            *a_seq.free_extents.lock().unwrap().by_addr(),
            *a_batch.free_extents.lock().unwrap().by_addr(),
            "end-state free lists must be identical"
        );
    }

    /// Adjacent extents within one batch coalesce into the same end state the
    /// sequential path produces.
    #[test]
    fn batch_free_coalesces_adjacent_members() {
        let a = new_alloc(4096);
        let n = alloc_first(&a, 12);
        let batch = [
            Extent::new(Pba(n), 3),
            Extent::new(Pba(n + 3), 3),
            Extent::new(Pba(n + 6), 6),
        ];
        let (freed, failed) = a.free_extents_batch(&batch);
        assert_eq!((freed, failed.len()), (12, 0));
        // All 12 blocks free and merged with the trailing free space into one run.
        assert!(a.is_extent_free(Extent::new(Pba(n), 12)));
        assert_eq!(a.allocated_block_count(), 0);
    }

    /// Failure mix: an already-free member and a retired member are rejected
    /// (returned in `failed`), the rest of the batch still frees.
    #[test]
    fn batch_free_failure_mix() {
        let a = new_alloc(4096);
        let n = alloc_first(&a, 12);
        let t0 = Instant::now();
        a.free_one(Pba(n + 4)).unwrap(); // already free
        a.retire_extent_at(Extent::single(Pba(n + 6)), t0).unwrap(); // retired
        let batch = [
            Extent::single(Pba(n)),      // frees
            Extent::single(Pba(n + 4)),  // free-overlap → failed
            Extent::single(Pba(n + 6)),  // retired-overlap → failed
            Extent::single(Pba(n + 8)),  // frees
        ];
        let (freed, failed) = a.free_extents_batch(&batch);
        assert_eq!(freed, 2);
        assert_eq!(failed, vec![Extent::single(Pba(n + 4)), Extent::single(Pba(n + 6))]);
        assert!(a.is_free(Pba(n)));
        assert!(a.is_free(Pba(n + 8)));
        assert!(a.is_retired(Pba(n + 6)), "retired member untouched");
    }

    /// A duplicate entry within one batch is caught by the intra-chunk
    /// free-overlap check (first frees, second fails) — no double free.
    #[test]
    fn batch_free_rejects_intra_batch_duplicate() {
        let a = new_alloc(64);
        let n = alloc_first(&a, 2);
        let batch = [Extent::single(Pba(n)), Extent::single(Pba(n))];
        let (freed, failed) = a.free_extents_batch(&batch);
        assert_eq!(freed, 1);
        assert_eq!(failed, vec![Extent::single(Pba(n))]);
        assert_eq!(a.allocated_block_count(), 1);
    }

    /// A batch larger than `BATCH_LOCK_CHUNK` frees every extent across chunks.
    #[test]
    fn batch_free_spans_chunks() {
        let count = BATCH_LOCK_CHUNK + 50;
        let a = new_alloc((4 * count as u64) + 256);
        let base = alloc_first(&a, 2 * count);
        let extents: Vec<Extent> = (0..count as u64)
            .map(|i| Extent::single(Pba(base + 2 * i)))
            .collect();
        let (freed, failed) = a.free_extents_batch(&extents);
        assert_eq!(freed, count as u64);
        assert!(failed.is_empty());
        for i in 0..count as u64 {
            assert!(a.is_free(Pba(base + 2 * i)));
        }
    }
}

#[cfg(test)]
mod stripe_align_tests {
    //! RAID6 full-stripe-aligned allocation: `(pba + phase) % stripe == 0`,
    //! length a whole number of stripes, lowest-address dense, no free-list
    //! bloat, and `stripe <= 1` identical to the plain path.
    use super::*;

    // 6+2 RAID6 at a 4 KiB strip = 6-block stripe; RESERVED_BLOCKS=8 => phase 2.
    const STRIPE: u32 = 6;
    const PHASE: u32 = (RESERVED_BLOCKS % STRIPE as u64) as u32;

    fn new_alloc_lanes(blocks: u64, lanes: usize) -> SpaceAllocator {
        SpaceAllocator::new(blocks * BLOCK_SIZE as u64, lanes)
    }

    #[test]
    fn align_up_pba_table() {
        // phase 2: aligned pbas satisfy (pba+2)%6==0 => pba in {4,10,16,22,...}
        assert_eq!(SpaceAllocator::align_up_pba(8, 6, 2), 10);
        assert_eq!(SpaceAllocator::align_up_pba(11, 6, 2), 16);
        assert_eq!(SpaceAllocator::align_up_pba(4, 6, 2), 4); // already aligned
        assert_eq!(SpaceAllocator::align_up_pba(10, 6, 2), 10);
        assert_eq!(SpaceAllocator::align_up_pba(5, 6, 2), 10);
        // phase 0: multiples of stripe
        assert_eq!(SpaceAllocator::align_up_pba(7, 6, 0), 12);
        assert_eq!(SpaceAllocator::align_up_pba(12, 6, 0), 12);
        // stripe<=1 is identity
        assert_eq!(SpaceAllocator::align_up_pba(13, 1, 0), 13);
    }

    #[test]
    fn round_up_blocks_table() {
        assert_eq!(SpaceAllocator::round_up_blocks(1, 6), 6);
        assert_eq!(SpaceAllocator::round_up_blocks(6, 6), 6);
        assert_eq!(SpaceAllocator::round_up_blocks(7, 6), 12);
        assert_eq!(SpaceAllocator::round_up_blocks(12, 6), 12);
        assert_eq!(SpaceAllocator::round_up_blocks(4, 1), 4);
    }

    #[test]
    fn io_addressable_capacity_reserves_top_for_offset() {
        // The IoEngine writes allocator PBA `p` at device block `p +
        // RESERVED_BLOCKS`. Production builds the allocator with the
        // io-ADDRESSABLE capacity (`phys - RESERVED_BLOCKS` blocks) so the top
        // RESERVED_BLOCKS physical blocks are never targeted. Draining the whole
        // free list must never yield a PBA whose written device block reaches
        // `phys_blocks`. Regression for the chunklet "Raid6 IO out of range:
        // offset == capacity" flush failure.
        let phys_blocks = 64u64;
        let io_addressable = new_alloc_lanes(phys_blocks - RESERVED_BLOCKS, 1);
        let mut max_pba = 0u64;
        while let Ok(pba) = io_addressable.allocate_one_for_lane(0) {
            assert!(
                pba.0 + RESERVED_BLOCKS < phys_blocks,
                "PBA {} + offset {} must stay below physical capacity {}",
                pba.0,
                RESERVED_BLOCKS,
                phys_blocks
            );
            max_pba = max_pba.max(pba.0);
        }
        assert_eq!(max_pba, phys_blocks - RESERVED_BLOCKS - 1, "top usable PBA");
    }

    #[test]
    fn stripe_extent_at_boundary_stays_in_capacity() {
        // A near-full stripe allocation must never return an extent whose top
        // block (+ offset) exceeds the physical device — the whole 24 KiB
        // full-stripe write must land inside it. Build with the io-addressable
        // capacity like production does.
        let phys_blocks = 6 * 20 + 2 * RESERVED_BLOCKS; // room for ~20 stripes
        let alloc = new_alloc_lanes(phys_blocks - RESERVED_BLOCKS, 1);
        let mut got = 0;
        while let Ok(ext) = alloc.allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE) {
            assert!(
                ext.start.0 + ext.count as u64 + RESERVED_BLOCKS <= phys_blocks,
                "stripe [{}, {}) + offset {} exceeds physical capacity {}",
                ext.start.0,
                ext.start.0 + ext.count as u64,
                RESERVED_BLOCKS,
                phys_blocks
            );
            assert_eq!((ext.start.0 + RESERVED_BLOCKS) % STRIPE as u64, 0, "device-aligned");
            got += 1;
        }
        assert!(got > 0, "should allocate at least one boundary stripe");
    }

    #[test]
    fn carve_aligned_from_run_shapes() {
        // run [8, 8+16) need 6 phase 2 => aligned@10, head [8,2), tail [16, ..)
        let run = Extent::new(Pba(8), 16);
        let (aligned, head, tail) =
            SpaceAllocator::carve_aligned_from_run(run, 6, STRIPE, PHASE).unwrap();
        assert_eq!(aligned, Extent::new(Pba(10), 6));
        assert_eq!(head, Some(Extent::new(Pba(8), 2)));
        assert_eq!(tail, Some(Extent::new(Pba(16), 8)));
        // exact aligned run: no head, no tail
        let run = Extent::new(Pba(10), 6);
        let (aligned, head, tail) =
            SpaceAllocator::carve_aligned_from_run(run, 6, STRIPE, PHASE).unwrap();
        assert_eq!(aligned, Extent::new(Pba(10), 6));
        assert_eq!(head, None);
        assert_eq!(tail, None);
        // run too small to host aligned need
        assert!(SpaceAllocator::carve_aligned_from_run(Extent::new(Pba(11), 6), 6, STRIPE, PHASE)
            .is_none());
    }

    #[test]
    fn lane_alloc_is_aligned_and_sized() {
        let a = new_alloc_lanes(65_536, 4);
        for data in [1u32, 4, 6, 7, 12] {
            let e = a
                .allocate_stripe_extent_for_lane(0, data, STRIPE, PHASE)
                .unwrap();
            assert_eq!(
                (e.start.0 + PHASE as u64) % STRIPE as u64,
                0,
                "start {} not stripe-aligned",
                e.start.0
            );
            let want = SpaceAllocator::round_up_blocks(data, STRIPE);
            assert_eq!(e.count, want, "data={data} count");
        }
    }

    #[test]
    fn lane_allocs_are_dense_and_disjoint() {
        let a = new_alloc_lanes(65_536, 4);
        let mut seen: Vec<Extent> = Vec::new();
        for _ in 0..500 {
            let e = a
                .allocate_stripe_extent_for_lane(1, 4, STRIPE, PHASE)
                .unwrap();
            assert_eq!(e.count, 6);
            assert_eq!((e.start.0 + PHASE as u64) % STRIPE as u64, 0);
            seen.push(e);
        }
        seen.sort_by_key(|e| e.start.0);
        for w in seen.windows(2) {
            assert!(
                w[0].end_pba().0 <= w[1].start.0,
                "overlap {:?} {:?}",
                w[0],
                w[1]
            );
        }
    }

    #[test]
    fn density_guard_no_freelist_bloat() {
        // 10k aligned allocs must NOT explode the free set into per-alloc
        // slivers (that would blow the metadb L2P leaf unit budget). One lane
        // seeded from one contiguous run => free runs stay O(1).
        let a = new_alloc_lanes(1_000_000, 2);
        for _ in 0..10_000 {
            a.allocate_stripe_extent_for_lane(0, 4, STRIPE, PHASE).unwrap();
        }
        assert!(
            a.free_extent_run_count() < 16,
            "free set fragmented to {} runs",
            a.free_extent_run_count()
        );
    }

    #[test]
    fn stripe_one_matches_plain_path() {
        let a = new_alloc_lanes(4096, 2);
        let e = a.allocate_stripe_extent_for_lane(0, 5, 1, 0).unwrap();
        assert_eq!(e.count, 5, "stripe<=1 must not pad");
    }

    #[test]
    fn padded_extent_frees_whole_stripe() {
        let a = new_alloc_lanes(4096, 2);
        let before = a.free_block_count();
        let e = a.allocate_stripe_extent_for_lane(0, 4, STRIPE, PHASE).unwrap();
        assert_eq!(e.count, 6);
        assert_eq!(a.free_block_count(), before - 6);
        a.free_extent(e).unwrap();
        assert_eq!(a.free_block_count(), before, "whole padded stripe returns");
    }

    #[test]
    fn global_path_aligns_without_lane() {
        // lane index out of range forces the global (no-lane) path.
        let a = new_alloc_lanes(4096, 0);
        let e = a.allocate_stripe_extent_for_lane(0, 7, STRIPE, PHASE).unwrap();
        assert_eq!(e.count, 12);
        assert_eq!((e.start.0 + PHASE as u64) % STRIPE as u64, 0);
    }
}
