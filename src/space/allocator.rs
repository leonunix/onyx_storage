use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::error::{OnyxError, OnyxResult};
use crate::meta::store::MetaStore;
use crate::space::extent::Extent;
use crate::space::free_set::FreeSet;
use crate::space::hazard::{PbaHazardGuard, PbaHazards};
use crate::types::{Pba, BLOCK_SIZE, RESERVED_BLOCKS};

/// Number of blocks to refill a lane cache from the global free list at once.
const LANE_CACHE_REFILL_SIZE: u32 = 256;
/// Number of blocks to reserve for each lane's contiguous extent cache.
///
/// Raw passthrough flushes commonly allocate 4-8 contiguous blocks per unit;
/// serving those from a lane-local slice avoids hammering the global BTreeSet.
const LANE_EXTENT_CACHE_REFILL_BLOCKS: u32 = 8192;
/// Maximum number of separate contiguous runs one lane refill may take.
///
/// The block budget above is the *intent*, but an aged pool has no long runs
/// left to satisfy it — the stripe reserve degrades to isolated single-stripe
/// windows. Taking several runs per lock hold decouples the lane cache from
/// contiguity: 64 single-stripe runs still buy 64 allocations per global-lock
/// acquisition. Bounded because the cache is scanned linearly per carve and
/// because cached blocks are parked away from other lanes (64 × one stripe =
/// 1.5 MiB per lane, far under the block budget's 32 MiB).
const LANE_EXTENT_CACHE_REFILL_RUNS: usize = 64;
/// Hard bound on reserve entries EXAMINED by one refill's ascending walk.
/// Without it, a wider-than-one-stripe request against a reserve of
/// single-stripe runs would skip past every entry in a multi-million-entry set
/// while holding the global free lock. Generous relative to the run cap so the
/// common case (every reserve entry qualifies) always fills the batch.
const LANE_EXTENT_CACHE_REFILL_SCAN: usize = 8 * LANE_EXTENT_CACHE_REFILL_RUNS;
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

/// Fragmentation snapshot of the global free set (one lock hold, O(log N)).
/// `stripe_capable_blocks / free_blocks_in_set` = currently allocatable stripe
/// capacity over globally tracked free space. Lane-cached extents are excluded
/// from both; quarantine-free blocks remain in the denominator but are excluded
/// from capability until their target is complete and published to reserve.
#[derive(Debug, Clone, Copy)]
pub struct ContiguityStats {
    pub free_blocks_in_set: u64,
    pub free_extents: u64,
    pub largest_run_blocks: u32,
    /// Whole-stripe aligned capacity (eff floored to stripe multiples).
    /// `None` when no stripe geometry is configured (stripe <= 1).
    pub stripe_capable_blocks: Option<u64>,
    /// Free whole-stripe blocks held exclusively for aligned allocations.
    pub stripe_reserve_blocks: u64,
    /// Total physical span covered by active defrag quarantines.
    pub quarantine_target_blocks: u64,
    /// Already-free blocks held inside active defrag quarantines.
    pub quarantine_free_blocks: u64,
}

/// Lane-cache supply accounting for the aligned (full-stripe) alloc path.
///
/// The aligned fast path is meant to serve most allocations out of a lane-local
/// cache, taking the global `free_pools` lock only to refill. Whether that
/// actually happens depends on how much the refill manages to take: the
/// mechanism was built around ONE contiguous run per refill, so on a fragmented
/// pool it can silently degrade into "global lock per allocation". These
/// counters read that out directly — `blocks_per_refill` / `allocs_per_refill`
/// are the amplification the cache is really buying.
///
/// `drains` counts `drain_lane_caches` calls, which are the expensive shape:
/// one global-lock hold that re-inserts every cached extent from ALL lanes. A
/// nonzero-and-growing `drains` under steady write load means lanes are fighting
/// over an empty stripe reserve, and each fight stalls all 16 writers.
#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct AllocSupplyStats {
    pub aligned_allocs: u64,
    pub refills: u64,
    pub refill_blocks: u64,
    pub refill_runs: u64,
    pub drains: u64,
    pub drain_blocks: u64,
}

impl AllocSupplyStats {
    /// Blocks obtained per global-lock refill (the refill's real yield, as
    /// opposed to `LANE_EXTENT_CACHE_REFILL_BLOCKS`'s intent).
    pub fn blocks_per_refill(&self) -> f64 {
        if self.refills == 0 {
            return 0.0;
        }
        self.refill_blocks as f64 / self.refills as f64
    }

    /// Contiguous runs obtained per refill.
    pub fn runs_per_refill(&self) -> f64 {
        if self.refills == 0 {
            return 0.0;
        }
        self.refill_runs as f64 / self.refills as f64
    }

    /// Aligned allocations served per global-lock refill. 1.0 means the lane
    /// cache is buying nothing and every allocation serializes on `free_pools`.
    pub fn allocs_per_refill(&self) -> f64 {
        if self.refills == 0 {
            return 0.0;
        }
        self.aligned_allocs as f64 / self.refills as f64
    }
}

/// Free-space policy classes protected by one lock. Every free PBA belongs to
/// exactly one of `general`, `stripe_reserve`, an active quarantine's
/// `free_parts`, or a detached lane cache.
struct FreePools {
    general: FreeSet,
    stripe_reserve: FreeSet,
    quarantines: BTreeMap<u64, QuarantineTarget>,
}

struct QuarantineTarget {
    range: Extent,
    free_parts: FreeSet,
}

impl FreePools {
    fn new() -> Self {
        Self {
            general: FreeSet::new(),
            stripe_reserve: FreeSet::new(),
            quarantines: BTreeMap::new(),
        }
    }

    fn geometry(&self) -> Option<(u32, u32)> {
        self.general.geometry()
    }

    fn empty_set_with_geometry(&self) -> FreeSet {
        let mut set = FreeSet::new();
        if let Some((stripe, phase)) = self.geometry() {
            set.set_geometry(stripe, phase);
        }
        set
    }

    fn set_geometry(&mut self, stripe: u32, phase: u32) {
        let requested = (stripe > 1).then_some((stripe, phase));
        if self.geometry() == requested {
            return;
        }
        let mut runs: Vec<Extent> = self.general.by_addr().iter().copied().collect();
        runs.extend(self.stripe_reserve.by_addr().iter().copied());
        runs.extend(
            self.quarantines
                .values()
                .flat_map(|target| target.free_parts.by_addr().iter().copied()),
        );
        self.general = FreeSet::new();
        self.stripe_reserve = FreeSet::new();
        self.general.set_geometry(stripe, phase);
        self.stripe_reserve.set_geometry(stripe, phase);
        self.quarantines.clear();
        runs.sort_unstable_by_key(|extent| extent.start.0);
        for run in runs {
            self.insert_classified(run);
        }
    }

    fn replace_general(&mut self, free: BTreeSet<Extent>) {
        let geometry = self.geometry();
        self.general = FreeSet::new();
        self.stripe_reserve = FreeSet::new();
        if let Some((stripe, phase)) = geometry {
            self.general.set_geometry(stripe, phase);
            self.stripe_reserve.set_geometry(stripe, phase);
        }
        self.quarantines.clear();
        for run in free {
            self.insert_classified(run);
        }
    }

    /// Insert a free run into the canonical policy partition. Adjacent runs in
    /// either pool are first folded into one maximal run; its aligned whole-
    /// stripe middle goes to the reserve and only its head/tail stay general.
    fn insert_classified(&mut self, extent: Extent) {
        let mut start = extent.start.0;
        let mut end = extent.end_pba().0;

        loop {
            let mut changed = false;
            for reserve in [false, true] {
                let set = if reserve {
                    &mut self.stripe_reserve
                } else {
                    &mut self.general
                };
                let probe = Extent::single(Pba(start));
                if let Some(before) = set.by_addr().range(..=probe).next_back().copied() {
                    if before.end_pba().0 == start {
                        set.remove(&before);
                        start = before.start.0;
                        changed = true;
                    }
                }
                let probe = Extent::single(Pba(end));
                if let Some(after) = set.by_addr().range(probe..).next().copied() {
                    if after.start.0 == end {
                        set.remove(&after);
                        end = after.end_pba().0;
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }

        let Some((stripe, phase)) = self.geometry().filter(|(stripe, _)| *stripe > 1) else {
            Self::insert_split(&mut self.general, start, end - start, 1);
            return;
        };
        let aligned_start = SpaceAllocator::align_up_pba(start, stripe as u64, phase as u64);
        if aligned_start >= end {
            // A sub-stripe fragment can end before the next alignment point.
            // Never use that future alignment as the head boundary: doing so
            // would manufacture free PBAs beyond the released range.
            Self::insert_split(&mut self.general, start, end - start, 1);
            return;
        }
        let aligned_blocks = end
            .saturating_sub(aligned_start)
            .checked_div(stripe as u64)
            .unwrap_or(0)
            * stripe as u64;
        let aligned_end = aligned_start + aligned_blocks;
        if aligned_start > start {
            Self::insert_split(&mut self.general, start, aligned_start - start, 1);
        }
        if aligned_blocks > 0 {
            Self::insert_split(
                &mut self.stripe_reserve,
                aligned_start,
                aligned_blocks,
                stripe,
            );
        }
        if aligned_end < end {
            Self::insert_split(&mut self.general, aligned_end, end - aligned_end, 1);
        }
    }

    fn insert_split(set: &mut FreeSet, mut start: u64, mut count: u64, multiple: u32) {
        let multiple = u64::from(multiple.max(1));
        let max_chunk = (u32::MAX as u64 / multiple) * multiple;
        debug_assert!(max_chunk > 0);
        while count > 0 {
            let take = count.min(max_chunk);
            set.insert(Extent::new(Pba(start), take as u32));
            start += take;
            count -= take;
        }
    }

    fn free_blocks_in_pools(&self) -> u64 {
        self.general.blocks_total()
            + self.stripe_reserve.blocks_total()
            + self
                .quarantines
                .values()
                .map(|target| target.free_parts.blocks_total())
                .sum::<u64>()
    }

    fn overlapping_in_set(set: &FreeSet, extent: Extent) -> Option<Extent> {
        SpaceAllocator::overlapping_extent(set.by_addr(), extent)
    }

    fn overlapping_free(&self, extent: Extent) -> Option<Extent> {
        Self::overlapping_in_set(&self.general, extent)
            .or_else(|| Self::overlapping_in_set(&self.stripe_reserve, extent))
            .or_else(|| {
                self.quarantine_starts_overlapping(extent)
                    .into_iter()
                    .find_map(|start| {
                        Self::overlapping_in_set(
                            &self
                                .quarantines
                                .get(&start)
                                .expect("quarantine key remains present")
                                .free_parts,
                            extent,
                        )
                    })
            })
    }

    /// Whether the union of policy pools covers `extent`. Canonical
    /// classification may split one physical run at general/reserve
    /// boundaries, so a single-set covering query is insufficient. Advance by
    /// whole stored runs rather than probing every block.
    fn covers_free(&self, extent: Extent) -> bool {
        let mut cursor = extent.start.0;
        let end = extent.end_pba().0;
        while cursor < end {
            let Some(run) = self.overlapping_free(Extent::single(Pba(cursor))) else {
                return false;
            };
            let next = run.end_pba().0.min(end);
            if next <= cursor {
                return false;
            }
            cursor = next;
        }
        true
    }

    fn overlaps_reserve(&self, extent: Extent) -> bool {
        Self::overlapping_in_set(&self.stripe_reserve, extent).is_some()
    }

    fn overlapping_quarantine(&self, extent: Extent) -> Option<Extent> {
        let mut candidate = self
            .quarantines
            .range(..=extent.start.0)
            .next_back()
            .map(|(_, target)| target.range);
        if candidate.is_none_or(|range| range.end_pba().0 <= extent.start.0) {
            candidate = self
                .quarantines
                .range(extent.start.0..)
                .next()
                .map(|(_, target)| target.range);
        }
        candidate.filter(|range| SpaceAllocator::extents_overlap(*range, extent))
    }

    fn quarantine_starts_overlapping(&self, extent: Extent) -> Vec<u64> {
        let mut starts = Vec::new();
        if let Some((&start, target)) = self.quarantines.range(..extent.start.0).next_back() {
            if target.range.end_pba().0 > extent.start.0 {
                starts.push(start);
            }
        }
        for (&start, target) in self.quarantines.range(extent.start.0..extent.end_pba().0) {
            if target.range.start.0 >= extent.end_pba().0 {
                break;
            }
            starts.push(start);
        }
        starts
    }

    fn extract_from_general(&mut self, range: Extent) -> Vec<Extent> {
        let mut overlaps = Vec::new();
        if let Some(before) = self
            .general
            .by_addr()
            .range(..Extent::single(range.start))
            .next_back()
            .copied()
        {
            if before.end_pba().0 > range.start.0 {
                overlaps.push(before);
            }
        }
        for extent in self.general.by_addr().range(Extent::single(range.start)..) {
            if extent.start.0 >= range.end_pba().0 {
                break;
            }
            overlaps.push(*extent);
        }
        let mut extracted = Vec::with_capacity(overlaps.len());
        for extent in overlaps {
            self.general.remove(&extent);
            let intersection_start = extent.start.0.max(range.start.0);
            let intersection_end = extent.end_pba().0.min(range.end_pba().0);
            if extent.start.0 < intersection_start {
                self.general.insert(Extent::new(
                    extent.start,
                    (intersection_start - extent.start.0) as u32,
                ));
            }
            extracted.push(Extent::new(
                Pba(intersection_start),
                (intersection_end - intersection_start) as u32,
            ));
            if intersection_end < extent.end_pba().0 {
                self.general.insert(Extent::new(
                    Pba(intersection_end),
                    (extent.end_pba().0 - intersection_end) as u32,
                ));
            }
        }
        extracted
    }

    /// Route newly-free blocks around active quarantine boundaries.
    fn release_extent(&mut self, extent: Extent) {
        let target_starts = self.quarantine_starts_overlapping(extent);
        let mut cursor = extent.start.0;
        let end = extent.end_pba().0;
        for target_start in target_starts {
            let target_range = self
                .quarantines
                .get(&target_start)
                .expect("collected quarantine target remains present")
                .range;
            if cursor < target_range.start.0 {
                self.insert_classified(Extent::new(
                    Pba(cursor),
                    (target_range.start.0 - cursor) as u32,
                ));
            }
            let part_start = cursor.max(target_range.start.0);
            let part_end = end.min(target_range.end_pba().0);
            if part_start < part_end {
                let target = self
                    .quarantines
                    .get_mut(&target_start)
                    .expect("collected quarantine target remains present");
                target
                    .free_parts
                    .coalesce_insert(Extent::new(Pba(part_start), (part_end - part_start) as u32));
                cursor = part_end;
            }
        }
        if cursor < end {
            self.insert_classified(Extent::new(Pba(cursor), (end - cursor) as u32));
        }
    }
}

pub struct SpaceAllocator {
    /// IO-addressable capacity in blocks. Atomic so an online `grow_capacity`
    /// (chunklet `extend_ld` on LV3) can publish the larger frontier while
    /// concurrent bounds checks / status reads run lock-free. Only ever grows.
    total_blocks: AtomicU64,
    /// Address-ordered free list + (count, start) side index. First-fit
    /// SELECTION is unchanged (lowest-address extent that fits — the metadb
    /// L2P leaf codec's dense-PBA contract); the side index only makes finding
    /// it O(D·log N) instead of an O(N) belt walk under this lock.
    free_pools: Mutex<FreePools>,
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
    /// Aligned-path lane-cache supply accounting — see [`AllocSupplyStats`].
    /// Relaxed counters, advisory only (never feed an allocation decision).
    aligned_allocs: AtomicU64,
    refill_ops: AtomicU64,
    refill_blocks: AtomicU64,
    refill_runs: AtomicU64,
    drain_ops: AtomicU64,
    drain_blocks: AtomicU64,
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
        let mut free_pools = FreePools::new();
        if usable_blocks > 0 {
            free_pools.general.insert(Extent::new(
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
            total_blocks: AtomicU64::new(total_blocks),
            free_pools: Mutex::new(free_pools),
            retired_extents: Mutex::new(BTreeSet::new()),
            retired_age: Mutex::new(BTreeMap::new()),
            retired_blocks: AtomicU64::new(0),
            hazards: PbaHazards::new(),
            allocated_blocks: AtomicU64::new(0),
            free_blocks: AtomicU64::new(usable_blocks),
            lane_caches,
            lane_extent_caches,
            alloc_tracker,
            aligned_allocs: AtomicU64::new(0),
            refill_ops: AtomicU64::new(0),
            refill_blocks: AtomicU64::new(0),
            refill_runs: AtomicU64::new(0),
            drain_ops: AtomicU64::new(0),
            drain_blocks: AtomicU64::new(0),
        }
    }

    /// Snapshot of the aligned path's lane-cache supply — see
    /// [`AllocSupplyStats`]. Lock-free; monotonic counters, so two reads
    /// difference cleanly.
    pub fn supply_stats(&self) -> AllocSupplyStats {
        AllocSupplyStats {
            aligned_allocs: self.aligned_allocs.load(Ordering::Relaxed),
            refills: self.refill_ops.load(Ordering::Relaxed),
            refill_blocks: self.refill_blocks.load(Ordering::Relaxed),
            refill_runs: self.refill_runs.load(Ordering::Relaxed),
            drains: self.drain_ops.load(Ordering::Relaxed),
            drain_blocks: self.drain_blocks.load(Ordering::Relaxed),
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
        if pos < self.total_blocks.load(Ordering::Relaxed) {
            let gap = self.total_blocks.load(Ordering::Relaxed) - pos;
            let mut start = pos;
            let mut remaining = gap;
            while remaining > 0 {
                let count = remaining.min(u32::MAX as u64) as u32;
                free.insert(Extent::new(Pba(start), count));
                start += count as u64;
                remaining -= count as u64;
            }
        }

        let usable_blocks = self
            .total_blocks
            .load(Ordering::Relaxed)
            .saturating_sub(RESERVED_BLOCKS);
        let alloc_count = allocated.len() as u64;
        let free_count = usable_blocks - alloc_count;

        self.free_pools.lock().unwrap().replace_general(free);
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
            total = self.total_blocks.load(Ordering::Relaxed),
            allocated = alloc_count,
            free = free_count,
            extents = {
                let pools = self.free_pools.lock().unwrap();
                pools.general.len() + pools.stripe_reserve.len()
            },
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
        self.free_pools
            .lock()
            .unwrap()
            .set_geometry(stripe_blocks, phase);
    }

    /// Remove an aligned physical range from ordinary allocation while the
    /// defragger evacuates its live pinners. Existing free pieces are moved into
    /// the target atomically; after publication, wait for pre-existing PBA pins
    /// without holding the allocator lock.
    pub fn begin_defrag_quarantine(&self, target: Extent) -> OnyxResult<()> {
        self.validate_extent_shape(target, "begin_defrag_quarantine")?;
        {
            let mut pools = self.free_pools.lock().unwrap();
            let (stripe, phase) = pools.geometry().ok_or_else(|| {
                OnyxError::Config("begin_defrag_quarantine requires stripe geometry".into())
            })?;
            if stripe <= 1
                || target.count % stripe != 0
                || (target.start.0 + phase as u64) % stripe as u64 != 0
            {
                return Err(OnyxError::Config(format!(
                    "defrag quarantine {:?} is not aligned to stripe={} phase={}",
                    target, stripe, phase
                )));
            }
            if let Some(existing) = pools.overlapping_quarantine(target) {
                return Err(OnyxError::Config(format!(
                    "defrag quarantine {:?} overlaps target {:?}",
                    target, existing
                )));
            }
            if pools.overlaps_reserve(target) {
                return Err(OnyxError::Config(format!(
                    "defrag quarantine {:?} overlaps stripe reserve",
                    target
                )));
            }
            // Lock order is FreePools -> lane caches. Allocation fast paths
            // never hold a lane lock while acquiring FreePools. Cached blocks
            // are logically free, so detach only the target intersections and
            // leave any head/tail pieces in their originating lane.
            let mut free_parts = pools.extract_from_general(target);
            free_parts.extend(self.extract_lane_cache_free_parts(target));
            let mut target_free = pools.empty_set_with_geometry();
            for extent in free_parts {
                target_free.coalesce_insert(extent);
            }
            pools.quarantines.insert(
                target.start.0,
                QuarantineTarget {
                    range: target,
                    free_parts: target_free,
                },
            );
        }

        self.hazards.wait_extent_clear(target.start, target.count);
        Ok(())
    }

    pub fn defrag_quarantine_progress(&self, start: Pba) -> Option<(u64, u64)> {
        let pools = self.free_pools.lock().unwrap();
        let target = pools.quarantines.get(&start.0)?;
        Some((target.free_parts.blocks_total(), target.range.count as u64))
    }

    /// Publish a fully-free quarantine as stripe reserve. A partially-free
    /// target remains active and returns `Ok(false)`.
    pub fn complete_defrag_quarantine(&self, start: Pba) -> OnyxResult<bool> {
        let mut pools = self.free_pools.lock().unwrap();
        let Some(target) = pools.quarantines.get(&start.0) else {
            return Ok(false);
        };
        if target.free_parts.blocks_total() != target.range.count as u64 {
            return Ok(false);
        }
        let target = pools
            .quarantines
            .remove(&start.0)
            .expect("target checked above");
        pools.insert_classified(target.range);
        Ok(true)
    }

    /// Abandon an active quarantine and return only its already-free pieces to
    /// the canonical general/reserve partition. Live/retired pieces were never
    /// removed from their ownership states.
    pub fn cancel_defrag_quarantine(&self, start: Pba) -> bool {
        let mut pools = self.free_pools.lock().unwrap();
        let Some(target) = pools.quarantines.remove(&start.0) else {
            return false;
        };
        let free_parts: Vec<Extent> = target.free_parts.by_addr().iter().copied().collect();
        for extent in free_parts {
            pools.insert_classified(extent);
        }
        true
    }

    pub fn is_defrag_quarantined(&self, extent: Extent) -> bool {
        self.free_pools
            .lock()
            .unwrap()
            .overlapping_quarantine(extent)
            .is_some()
    }

    /// Atomically reject new dedup pins after a quarantine is published. A pin
    /// that wins the race before publication is waited out by
    /// `begin_defrag_quarantine` after it drops the allocator lock.
    pub fn pin_dedup_target_if_allowed(&self, start: Pba, count: u32) -> Option<PbaHazardGuard> {
        let end = start.0.checked_add(count as u64)?;
        if count == 0 || end > self.total_blocks.load(Ordering::Acquire) {
            return None;
        }
        let extent = Extent::new(start, count);
        let pools = self.free_pools.lock().unwrap();
        if pools.overlapping_quarantine(extent).is_some() {
            return None;
        }
        Some(
            self.hazards
                .pin_many((0..count).map(|offset| Pba(start.0 + offset as u64))),
        )
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
            let mut pools = self.free_pools.lock().unwrap();
            if let Some(pba) = Self::alloc_one_from_pools(&mut pools) {
                self.track_alloc(Extent::single(pba), "allocate_one")?;
                self.allocated_blocks.fetch_add(1, Ordering::Relaxed);
                self.free_blocks.fetch_sub(1, Ordering::Relaxed);
                return Ok(pba);
            }
        }
        // Global pool empty — drain lane caches and retry
        if !self.lane_caches.is_empty() {
            self.drain_lane_caches();
            let mut pools = self.free_pools.lock().unwrap();
            if let Some(pba) = Self::alloc_one_from_pools(&mut pools) {
                self.track_alloc(Extent::single(pba), "allocate_one_retry")?;
                self.allocated_blocks.fetch_add(1, Ordering::Relaxed);
                self.free_blocks.fetch_sub(1, Ordering::Relaxed);
                return Ok(pba);
            }
        }
        Err(OnyxError::SpaceExhausted)
    }

    /// Small allocations preserve allocator-wide first-fit-by-address across
    /// both policy pools. This ordering is a MetaDB L2P codec correctness
    /// contract; the reserve controls aligned ownership, never address order.
    fn alloc_one_from_pools(pools: &mut FreePools) -> Option<Pba> {
        Self::take_first_from_pools(pools, 1).map(|extent| extent.start)
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
        // Slow path: refill from global (blocks stay logically "free" in the cache).
        // The global-pool removal and lane-tail publication share the FreePools
        // critical section so defrag quarantine cannot miss an in-flight refill.
        let first_pba = match self.refill_one_lane_from_global(lane, LANE_CACHE_REFILL_SIZE) {
            Some(pba) => pba,
            None => {
                self.drain_lane_caches();
                self.refill_one_lane_from_global(lane, LANE_CACHE_REFILL_SIZE)
                    .ok_or(OnyxError::SpaceExhausted)?
            }
        };
        // First block goes to caller (counted as allocated); the helper has
        // already published the remainder into the lane cache.
        self.track_alloc(Extent::single(first_pba), "allocate_one_for_lane_refill")?;
        self.allocated_blocks.fetch_add(1, Ordering::Relaxed);
        self.free_blocks.fetch_sub(1, Ordering::Relaxed);
        Ok(first_pba)
    }

    /// Return all cached blocks from lane caches to the global free list.
    /// Called during shutdown to prevent block leaks.
    pub fn drain_lane_caches(&self) {
        let mut drained: u64 = 0;
        let mut pools = self.free_pools.lock().unwrap();
        for cache_mutex in &self.lane_caches {
            let mut cache = cache_mutex.lock().unwrap();
            for pba in cache.drain(..) {
                pools.release_extent(Extent::single(pba));
                drained += 1;
            }
        }
        for cache_mutex in &self.lane_extent_caches {
            let mut cache = cache_mutex.lock().unwrap();
            for extent in cache.drain(..) {
                pools.release_extent(extent);
                drained += u64::from(extent.count);
            }
        }
        self.drain_ops.fetch_add(1, Ordering::Relaxed);
        self.drain_blocks.fetch_add(drained, Ordering::Relaxed);
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
        let pools = self.free_pools.lock().unwrap();
        if pools.overlapping_free(Extent::single(pba)).is_some() {
            return true;
        }
        drop(pools);
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
        match self.allocate_exact_extent_for_lane(lane, count) {
            Ok(extent) => Ok(extent),
            Err(OnyxError::SpaceExhausted) => self.allocate_extent(count),
            Err(error) => Err(error),
        }
    }

    /// Exact-width lane allocation. Unlike [`Self::allocate_extent`], this API
    /// never removes and returns the largest short fragment on a miss. Writers
    /// that cannot safely consume a short extent use this to fail without a
    /// compensating rollback/free cycle.
    pub fn allocate_exact_extent_for_lane(&self, lane: usize, count: u32) -> OnyxResult<Extent> {
        if count == 0 {
            return Err(OnyxError::Config("cannot allocate 0 blocks".into()));
        }
        if lane >= self.lane_extent_caches.len() {
            for attempt in 0..2 {
                if let Some(extent) = {
                    let mut pools = self.free_pools.lock().unwrap();
                    Self::take_exact_from_pools(&mut pools, count, count)
                } {
                    self.track_alloc(extent, "allocate_exact_extent_global")?;
                    self.allocated_blocks
                        .fetch_add(count as u64, Ordering::Relaxed);
                    self.free_blocks.fetch_sub(count as u64, Ordering::Relaxed);
                    return Ok(extent);
                }
                if attempt == 0 {
                    self.drain_lane_caches();
                    continue;
                }
                break;
            }
            return Err(OnyxError::SpaceExhausted);
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
        let mut result = self.refill_extent_lane(lane, count, target);
        if result.is_none() {
            // A global fragment can become usable only after it coalesces with
            // short pieces held by one or more lanes. Exact allocation is the
            // ENOSPC boundary, so pay for one bounded drain and retry here.
            self.drain_lane_caches();
            result = self.refill_extent_lane(lane, count, target);
        }

        let Some(result) = result else {
            return Err(OnyxError::SpaceExhausted);
        };

        self.track_alloc(result, "allocate_extent_for_lane_refill")?;
        self.allocated_blocks
            .fetch_add(count as u64, Ordering::Relaxed);
        self.free_blocks.fetch_sub(count as u64, Ordering::Relaxed);
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
        self.aligned_allocs.fetch_add(1, Ordering::Relaxed);
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
                self.allocated_blocks
                    .fetch_add(need as u64, Ordering::Relaxed);
                self.free_blocks.fetch_sub(need as u64, Ordering::Relaxed);
                return Ok(extent);
            }
        }

        // Refill only from the stripe reserve. General/free-fragment runs are
        // deliberately invisible to this path so small allocation and aligned
        // allocation cannot consume each other's working set.
        let want = LANE_EXTENT_CACHE_REFILL_BLOCKS.max(need);
        let mut extent = self.refill_stripe_extent_lane(lane, need, want, stripe_blocks, phase);
        if extent.is_none() {
            // A cold lane may hold the only remaining aligned refill. Reclaim
            // all lane caches once at the reserve-miss boundary, then let the
            // requesting lane seed itself from the reconstituted reserve.
            self.drain_lane_caches();
            extent = self.refill_stripe_extent_lane(lane, need, want, stripe_blocks, phase);
        }
        let Some(extent) = extent else {
            return self.allocate_stripe_extent_global(need, stripe_blocks, phase);
        };
        self.track_alloc(extent, "allocate_stripe_extent_for_lane_refill")?;
        self.allocated_blocks
            .fetch_add(need as u64, Ordering::Relaxed);
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

    /// Insert into a lane extent cache, keeping it ordered by DESCENDING start.
    ///
    /// The cache holds several disjoint runs once a refill takes more than one,
    /// so the order it is scanned in decides the order PBAs are handed out.
    /// Descending-by-start + take-from-the-back means a lane emits aligned
    /// carves in strictly ASCENDING PBA order, which keeps a leaf's PBAs
    /// clustered (`lane_extent_cache_hands_out_ascending` pins it). An unordered
    /// `Vec` with `swap_remove` would scramble them across the whole refill.
    fn push_extent_cache(cache: &mut Vec<Extent>, extent: Extent) {
        let at = cache.partition_point(|held| held.start.0 > extent.start.0);
        cache.insert(at, extent);
    }

    /// Carve a stripe-aligned `need` from the lowest-address cached run that can
    /// host it, pushing head/tail remainders back into the cache. Head is only
    /// non-empty when a non-aligned run (e.g. a rest pushed by
    /// `allocate_extent_for_lane`) is the only candidate; it stays lane-local for
    /// a later non-stripe alloc.
    ///
    /// The cache is descending by start, so scanning from the BACK visits
    /// candidates in ascending address order — first hit is the address-argmin,
    /// mirroring the global pool's first-fit-by-address inside the lane.
    fn take_aligned_from_extent_cache(
        cache: &mut Vec<Extent>,
        need: u32,
        stripe: u32,
        phase: u32,
    ) -> Option<Extent> {
        for idx in (0..cache.len()).rev() {
            if let Some((aligned, head, tail)) =
                Self::carve_aligned_from_run(cache[idx], need, stripe, phase)
            {
                cache.remove(idx);
                if let Some(head) = head {
                    Self::push_extent_cache(cache, head);
                }
                if let Some(tail) = tail {
                    Self::push_extent_cache(cache, tail);
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
        let extent = self
            .take_aligned_extent_from_global(need, stripe, phase)
            .ok_or(OnyxError::SpaceExhausted)?;
        self.track_alloc(extent, "allocate_stripe_extent_global")?;
        self.allocated_blocks
            .fetch_add(need as u64, Ordering::Relaxed);
        self.free_blocks.fetch_sub(need as u64, Ordering::Relaxed);
        Ok(extent)
    }

    /// Allocate up to `count` contiguous blocks. Returns the extent actually allocated
    /// (may be smaller than requested if no large enough contiguous region exists).
    pub fn allocate_extent(&self, count: u32) -> OnyxResult<Extent> {
        if count == 0 {
            return Err(OnyxError::Config("cannot allocate 0 blocks".into()));
        }

        // Try allocation from global free list. If insufficient, drain lane caches and retry.
        for attempt in 0..2 {
            let mut pools = self.free_pools.lock().unwrap();

            if let Some(result) = Self::take_exact_from_pools(&mut pools, count, count) {
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
                drop(pools);
                self.drain_lane_caches();
                continue;
            }

            // No contiguous extent large enough — return the largest available
            if let Some(extent) = Self::take_largest_from_pools(&mut pools) {
                self.track_alloc(extent, "allocate_extent_largest")?;
                self.allocated_blocks
                    .fetch_add(extent.count as u64, Ordering::Relaxed);
                self.free_blocks
                    .fetch_sub(extent.count as u64, Ordering::Relaxed);
                return Ok(extent);
            }

            // No free extents at all — drain lane caches and retry once
            drop(pools);
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
            let pools = self.free_pools.lock().unwrap();
            if let Some(e) = pools.overlapping_free(extent) {
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
            let pools = self.free_pools.lock().unwrap();
            let mut retired = self.retired_extents.lock().unwrap();
            let mut age = self.retired_age.lock().unwrap();
            for &extent in chunk {
                if self
                    .validate_extent_shape(extent, "retire_extents_batch")
                    .is_err()
                {
                    failed.push(extent);
                    continue;
                }
                let in_lane = (0..extent.count)
                    .any(|i| lane_pbas.contains(&Pba(extent.start.0 + i as u64)))
                    || Self::sorted_extents_overlap(&lane_exts, extent);
                if in_lane
                    || pools.overlapping_free(extent).is_some()
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
            drop(pools);
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
                if extent.count > 0
                    && extent.end_pba().0 <= self.total_blocks.load(Ordering::Relaxed)
                {
                    self.hazards.wait_extent_clear(extent.start, extent.count);
                }
            }

            let mut chunk_freed: u64 = 0;
            let mut chunk_released: Vec<Extent> = Vec::with_capacity(chunk.len());
            {
                let mut pools = self.free_pools.lock().unwrap();
                let retired = self.retired_extents.lock().unwrap();
                for &extent in chunk {
                    if self
                        .validate_extent_shape(extent, "free_extents_batch")
                        .is_err()
                    {
                        failed.push(extent);
                        continue;
                    }
                    let in_lane = (0..extent.count)
                        .any(|i| lane_pbas.contains(&Pba(extent.start.0 + i as u64)))
                        || Self::sorted_extents_overlap(&lane_exts, extent);
                    if in_lane
                        || pools.overlapping_free(extent).is_some()
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
                    pools.release_extent(extent);
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

            let mut pools = self.free_pools.lock().unwrap();
            if let Some(e) = pools.overlapping_free(extent) {
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

            pools.release_extent(extent);
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
                let mut pools = self.free_pools.lock().unwrap();
                for &extent in &removed {
                    let in_lane = (0..extent.count)
                        .any(|i| lane_pbas.contains(&Pba(extent.start.0 + i as u64)))
                        || Self::sorted_extents_overlap(&lane_exts, extent);
                    if in_lane || pools.overlapping_free(extent).is_some() {
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
                    pools.release_extent(extent);
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
            let mut pools = self.free_pools.lock().unwrap();
            self.ensure_not_free_or_retired_after_wait(extent, &pools)?;
            pools.release_extent(extent);
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

        let pools = self.free_pools.lock().unwrap();

        // Check no overlap with existing free extents
        if let Some(e) = pools.overlapping_free(extent) {
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
        if extent.end_pba().0 > self.total_blocks.load(Ordering::Relaxed) {
            return Err(OnyxError::Config(format!(
                "{context}: extent {:?} exceeds total blocks {}",
                extent,
                self.total_blocks.load(Ordering::Relaxed)
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

    /// Detach the portions of lane-cached free space covered by `target`.
    ///
    /// The caller must hold `free_pools`; this establishes the allocator-wide
    /// lock order `FreePools -> lane cache`. Allocation paths release a lane
    /// cache before acquiring `FreePools`, so quarantine publication cannot
    /// deadlock with a refill/drain. Counters are unchanged because the blocks
    /// remain logically free while moving from a lane cache to quarantine.
    fn extract_lane_cache_free_parts(&self, target: Extent) -> Vec<Extent> {
        let mut extracted = Vec::new();

        for cache_mutex in &self.lane_caches {
            let mut cache = cache_mutex.lock().unwrap();
            cache.retain(|pba| {
                if target.contains(*pba) {
                    extracted.push(Extent::single(*pba));
                    false
                } else {
                    true
                }
            });
        }

        for cache_mutex in &self.lane_extent_caches {
            let mut cache = cache_mutex.lock().unwrap();
            // Rebuilt through `push_extent_cache` so the descending-by-start
            // invariant survives splitting one cached run into head + tail.
            let mut retained = Vec::with_capacity(cache.len());
            for cached in cache.drain(..) {
                if !Self::extents_overlap(cached, target) {
                    Self::push_extent_cache(&mut retained, cached);
                    continue;
                }

                let intersection_start = cached.start.0.max(target.start.0);
                let intersection_end = cached.end_pba().0.min(target.end_pba().0);
                if cached.start.0 < intersection_start {
                    Self::push_extent_cache(
                        &mut retained,
                        Extent::new(cached.start, (intersection_start - cached.start.0) as u32),
                    );
                }
                extracted.push(Extent::new(
                    Pba(intersection_start),
                    (intersection_end - intersection_start) as u32,
                ));
                if intersection_end < cached.end_pba().0 {
                    Self::push_extent_cache(
                        &mut retained,
                        Extent::new(
                            Pba(intersection_end),
                            (cached.end_pba().0 - intersection_end) as u32,
                        ),
                    );
                }
            }
            *cache = retained;
        }

        extracted
    }

    /// Return true if the whole extent is already covered by a free extent
    /// or all its blocks are sitting in lane caches.
    pub fn is_extent_free(&self, extent: Extent) -> bool {
        let pools = self.free_pools.lock().unwrap();
        if pools.covers_free(extent) {
            return true;
        }
        drop(pools);
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
        self.total_blocks.load(Ordering::Relaxed)
    }

    /// Grow the allocatable frontier online after a chunklet `extend_ld` on the
    /// LV3 LD. `new_device_size_bytes` is the IO-ADDRESSABLE capacity — the SAME
    /// transform the constructor takes (`new_ld.capacity_bytes() -
    /// RESERVED_BLOCKS * BLOCK_SIZE`), NOT the raw LD size.
    ///
    /// The newly-addressable PBAs `[old_total, new_total)` are appended to the
    /// free set as dense extents at the TOP of the space (split into `u32::MAX`
    /// runs like the constructor / rebuild path), preserving the
    /// dense/sequential PBA contract the metadb L2P leaf codec relies on:
    /// first-fit still hands out the lowest free address, so the grown tail is
    /// consumed last. Grow-only — a smaller-or-equal size is a no-op. Returns
    /// the new total block count.
    ///
    /// Ordering: the larger frontier is published (`Release`) BEFORE the new
    /// PBAs enter circulation via the `free_extents` insert, so a concurrent
    /// bounds check (`free_extent` / `retire_*`) can never observe an allocated
    /// PBA from the grown region while `total_blocks` still reads the old value.
    pub fn grow_capacity(&self, new_device_size_bytes: u64) -> OnyxResult<u64> {
        let new_total = new_device_size_bytes / BLOCK_SIZE as u64;
        let old_total = self.total_blocks.load(Ordering::Relaxed);
        if new_total <= old_total {
            return Ok(old_total);
        }
        self.total_blocks.store(new_total, Ordering::Release);
        let added = new_total - old_total;
        {
            let mut pools = self.free_pools.lock().unwrap();
            let mut start = old_total;
            let mut remaining = added;
            while remaining > 0 {
                let count = remaining.min(u32::MAX as u64) as u32;
                pools.insert_classified(Extent::new(Pba(start), count));
                start += count as u64;
                remaining -= count as u64;
            }
        }
        self.free_blocks.fetch_add(added, Ordering::Relaxed);
        tracing::info!(
            old_total,
            new_total,
            added,
            "allocator online grow_capacity"
        );
        Ok(new_total)
    }

    /// O(1)/O(log N) fragmentation snapshot of the global free set — one lock
    /// acquisition. `free_blocks_in_set` deliberately EXCLUDES lane-cached
    /// extents (they are drained out of the set), so
    /// `stripe_capable_blocks / free_blocks_in_set` is the defrag trigger
    /// signal. Active quarantine-free blocks remain in the denominator but are
    /// intentionally unavailable in the numerator until publication.
    pub fn contiguity_stats(&self) -> ContiguityStats {
        let pools = self.free_pools.lock().unwrap();
        let quarantine_free_blocks: u64 = pools
            .quarantines
            .values()
            .map(|target| target.free_parts.blocks_total())
            .sum();
        let quarantine_target_blocks: u64 = pools
            .quarantines
            .values()
            .map(|target| target.range.count as u64)
            .sum();
        let geometry = pools.geometry();
        ContiguityStats {
            free_blocks_in_set: pools.free_blocks_in_pools(),
            free_extents: (pools.general.len()
                + pools.stripe_reserve.len()
                + pools
                    .quarantines
                    .values()
                    .map(|target| target.free_parts.len())
                    .sum::<usize>()) as u64,
            largest_run_blocks: pools
                .general
                .largest()
                .into_iter()
                .chain(pools.stripe_reserve.largest())
                .map(|extent| extent.count)
                .max()
                .unwrap_or(0),
            stripe_capable_blocks: geometry
                .map(|_| pools.general.stripe_capacity() + pools.stripe_reserve.stripe_capacity()),
            stripe_reserve_blocks: pools.stripe_reserve.blocks_total(),
            quarantine_target_blocks,
            quarantine_free_blocks,
        }
    }

    /// The configured RAID geometry `(stripe_blocks, phase)`, if any.
    pub fn stripe_geometry(&self) -> Option<(u32, u32)> {
        self.free_pools.lock().unwrap().geometry()
    }

    /// Blocks of `range` covered by free extents — the defrag target "done"
    /// recheck. One brief free-lock hold, O(log N + overlaps in range).
    pub(crate) fn free_overlap_blocks(&self, range: Extent) -> u64 {
        let s = range.start.0;
        let e = range.end_pba().0;
        let pools = self.free_pools.lock().unwrap();
        let overlap = |set: &FreeSet| {
            let mut covered = 0u64;
            if let Some(prev) = set
                .by_addr()
                .range(..=Extent::single(range.start))
                .next_back()
            {
                covered += prev.end_pba().0.min(e).saturating_sub(s);
            }
            for ext in set.by_addr().range(Extent::single(Pba(s + 1))..) {
                if ext.start.0 >= e {
                    break;
                }
                covered += ext.end_pba().0.min(e) - ext.start.0;
            }
            covered
        };
        overlap(&pools.general)
            + overlap(&pools.stripe_reserve)
            + pools
                .quarantine_starts_overlapping(range)
                .into_iter()
                .map(|start| {
                    overlap(
                        &pools
                            .quarantines
                            .get(&start)
                            .expect("quarantine key remains present")
                            .free_parts,
                    )
                })
                .sum::<u64>()
    }

    /// Snapshot up to `max` free extents strictly below `below`, DESCENDING by
    /// address — the defrag target-selection walk. ONE bounded lock hold
    /// (`max` is chunk-sized by the caller); the snapshot is advisory, so
    /// concurrent mutation between chunks is fine (the scanner/rewriter
    /// re-validate everything downstream).
    pub(crate) fn free_extents_below_desc(&self, below: Pba, max: usize) -> Vec<Extent> {
        if max == 0 {
            return Vec::new();
        }
        let pools = self.free_pools.lock().unwrap();
        pools
            .general
            .by_addr()
            .range(..Extent::single(below))
            .rev()
            .take(max)
            .copied()
            .collect()
    }

    /// Blocks of `range` covered by retired extents. Takes ONLY the retired
    /// lock (callers must NOT hold `free_extents` — keeps the free→retired
    /// lock order one-directional). Retired extents never overlap each other
    /// (coalesced set), so summing clamped intersections is exact.
    pub(crate) fn retired_overlap_blocks(&self, range: Extent) -> u64 {
        let s = range.start.0;
        let e = range.end_pba().0;
        let retired = self.retired_extents.lock().unwrap();
        let mut covered = 0u64;
        // The last extent starting at/before `s` may reach into the range.
        if let Some(prev) = retired.range(..=Extent::single(range.start)).next_back() {
            covered += prev.end_pba().0.min(e).saturating_sub(s);
        }
        for ext in retired.range(Extent::single(Pba(s + 1))..) {
            if ext.start.0 >= e {
                break;
            }
            covered += ext.end_pba().0.min(e) - ext.start.0;
        }
        covered
    }

    /// Number of distinct runs in the global free set. Test-only: the stripe
    /// density guard asserts this stays O(1) under repeated aligned allocation
    /// (alignment pads must not fragment the free list into per-alloc slivers).
    #[cfg(test)]
    pub(crate) fn free_extent_run_count(&self) -> usize {
        let pools = self.free_pools.lock().unwrap();
        pools.general.len() + pools.stripe_reserve.len()
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
        pools: &FreePools,
    ) -> OnyxResult<()> {
        if let Some(e) = pools.overlapping_free(extent) {
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

    /// Transfer a global refill into a single-block lane cache atomically with
    /// respect to defrag quarantine publication. The returned first block is no
    /// longer free; every remaining block is visible in the lane cache before
    /// `FreePools` is unlocked.
    fn refill_one_lane_from_global(&self, lane: usize, max_count: u32) -> Option<Pba> {
        let mut pools = self.free_pools.lock().unwrap();
        let refill = Self::take_first_from_pools(&mut pools, max_count)?;
        if refill.count > 1 {
            let mut cache = self.lane_caches[lane].lock().unwrap();
            for i in 1..refill.count {
                cache.push(Pba(refill.start.0 + i as u64));
            }
        }
        Some(refill.start)
    }

    /// Take exactly `count` blocks for the caller and publish the rest of the
    /// refill into its extent cache before releasing `FreePools`.
    fn refill_extent_lane(&self, lane: usize, count: u32, max_count: u32) -> Option<Extent> {
        let mut pools = self.free_pools.lock().unwrap();
        let refill = Self::take_exact_from_pools(&mut pools, count, max_count)?;
        let result = Extent::new(refill.start, count);
        if refill.count > count {
            Self::push_extent_cache(
                &mut self.lane_extent_caches[lane].lock().unwrap(),
                Extent::new(Pba(refill.start.0 + count as u64), refill.count - count),
            );
        }
        Some(result)
    }

    /// Seed a lane's extent cache from the stripe reserve and hand back one
    /// aligned `min_count` carve.
    ///
    /// Takes up to [`LANE_EXTENT_CACHE_REFILL_RUNS`] runs (bounded by the
    /// `max_count` block budget) in ONE lock hold, because taking a single
    /// contiguous run made the whole lane-cache mechanism **depend on contiguous
    /// free space**: on an aged pool the reserve degrades to isolated
    /// single-stripe windows, `take` is then exactly one stripe, and the cache
    /// serves exactly one allocation before the next allocation retakes the
    /// global lock — with every other writer queued behind it
    /// (`AllocSupplyStats::allocs_per_refill` reads 1.00 in that state; the
    /// `aged_pool_bench` `SingleStripe` shape reproduces it).
    ///
    /// SELECTION IS UNCHANGED. Removing an extent never coalesces, so "the
    /// lowest-address qualifying runs, in ascending order" is exactly the
    /// sequence K successive `first_fit(min_count)` calls would return — this is
    /// batching, not a policy change, and `batched_refill_equals_sequential_refills`
    /// pins it.
    fn refill_stripe_extent_lane(
        &self,
        lane: usize,
        min_count: u32,
        max_count: u32,
        stripe: u32,
        phase: u32,
    ) -> Option<Extent> {
        let mut pools = self.free_pools.lock().unwrap();
        if pools.geometry() != Some((stripe, phase)) {
            return None;
        }
        // PASS 1 — plan the batch. The set stays borrowed while walking it, so
        // the whole plan (run + how much of it to take) is decided up front and
        // pass 2 does the mutation. `take` is floored to a stripe multiple so
        // every cached run keeps the reserve's aligned shape.
        //
        // The first pick is the exact address-argmin over the qualifying runs —
        // the same extent the one-run-at-a-time refill took, found the same way.
        let first = pools.stripe_reserve.first_fit(min_count)?;
        let mut budget = max_count.max(min_count);
        let mut plan: Vec<(Extent, u32)> = Vec::with_capacity(LANE_EXTENT_CACHE_REFILL_RUNS);
        fn plan_push(
            plan: &mut Vec<(Extent, u32)>,
            budget: &mut u32,
            run: Extent,
            stripe: u32,
            min_count: u32,
        ) {
            let take = (run.count.min(*budget) / stripe) * stripe;
            if take < min_count {
                return;
            }
            plan.push((run, take));
            *budget -= take;
        }
        plan_push(&mut plan, &mut budget, first, stripe, min_count);
        // Then an ascending walk for the next K-1. The walk is bounded in ENTRIES
        // EXAMINED, not just entries taken: a request wider than one stripe on a
        // reserve of single-stripe runs would otherwise skip past every entry in
        // a multi-million-entry set while holding the global lock. Stopping early
        // only costs a smaller batch — never correctness, and never worse than
        // the single-run refill this replaced, which is what `first` already is.
        let mut examined = 0usize;
        for run in pools
            .stripe_reserve
            .by_addr()
            .range(Extent::single(Pba(first.start.0 + 1))..)
        {
            if plan.len() >= LANE_EXTENT_CACHE_REFILL_RUNS
                || budget < min_count
                || examined >= LANE_EXTENT_CACHE_REFILL_SCAN
            {
                break;
            }
            examined += 1;
            if run.count >= min_count {
                plan_push(&mut plan, &mut budget, *run, stripe, min_count);
            }
        }

        // PASS 2 — execute it.
        let mut refill_blocks: u64 = 0;
        let mut refill_runs: u64 = 0;
        let mut cache = self.lane_extent_caches[lane].lock().unwrap();
        for (run, take) in plan {
            // A previous iteration's reclassified remainder can, in principle,
            // coalesce with a later pick (adjacent reserve extents exist only
            // where `insert_split` chunked a >16 TiB aligned region). Proceed
            // only with runs this refill actually owns — handing out a run that
            // is still reachable in the pool would be a double allocation.
            if !pools.stripe_reserve.remove(&run) {
                continue;
            }
            if run.count > take {
                pools.insert_classified(Extent::new(
                    Pba(run.start.0 + take as u64),
                    run.count - take,
                ));
            }
            Self::push_extent_cache(&mut cache, Extent::new(run.start, take));
            refill_blocks += u64::from(take);
            refill_runs += 1;
        }
        if refill_blocks == 0 {
            return None;
        }
        self.refill_ops.fetch_add(1, Ordering::Relaxed);
        self.refill_blocks
            .fetch_add(refill_blocks, Ordering::Relaxed);
        self.refill_runs.fetch_add(refill_runs, Ordering::Relaxed);
        Some(
            Self::take_aligned_from_extent_cache(&mut cache, min_count, stripe, phase)
                .expect("stripe-reserve refill is aligned and large enough"),
        )
    }

    /// Take an aligned extent while preserving the legacy API contract that
    /// the geometry supplied to `allocate_stripe_extent_for_lane` is enough on
    /// its own. Production's configured geometry uses the O(log N) reserve
    /// path; tests and hypothetical alternate-geometry callers fall back to
    /// the indexed aligned search across both policy pools.
    fn take_aligned_extent_from_global(
        &self,
        need: u32,
        stripe: u32,
        phase: u32,
    ) -> Option<Extent> {
        let mut pools = self.free_pools.lock().unwrap();
        let reserve_only = pools.geometry() == Some((stripe, phase));
        let (from_reserve, run) = if reserve_only {
            (true, pools.stripe_reserve.first_fit(need)?)
        } else {
            let general = pools.general.first_fit_aligned(need, stripe, phase);
            let reserve = pools.stripe_reserve.first_fit_aligned(need, stripe, phase);
            match (general, reserve) {
                (Some(general), Some(reserve)) => {
                    if reserve.start.0 < general.start.0 {
                        (true, reserve)
                    } else {
                        (false, general)
                    }
                }
                (Some(general), None) => (false, general),
                (None, Some(reserve)) => (true, reserve),
                (None, None) => return None,
            }
        };

        let (aligned, head, tail) = Self::carve_aligned_from_run(run, need, stripe, phase)?;
        if from_reserve {
            pools.stripe_reserve.remove(&run);
        } else {
            pools.general.remove(&run);
        }
        if let Some(head) = head {
            pools.insert_classified(head);
        }
        if let Some(tail) = tail {
            pools.insert_classified(tail);
        }
        Some(aligned)
    }

    fn take_first_from_pools(pools: &mut FreePools, max_count: u32) -> Option<Extent> {
        let (from_reserve, extent) =
            Self::lowest_pool_candidate(pools.general.first(), pools.stripe_reserve.first())?;
        if from_reserve {
            pools.stripe_reserve.remove(&extent);
        } else {
            pools.general.remove(&extent);
        }
        let take = extent.count.min(max_count);
        if extent.count > take {
            let tail = Extent::new(Pba(extent.start.0 + take as u64), extent.count - take);
            if from_reserve {
                pools.insert_classified(tail);
            } else {
                pools.general.insert(Extent::new(
                    Pba(extent.start.0 + take as u64),
                    extent.count - take,
                ));
            }
        }
        Some(Extent::new(extent.start, take))
    }

    fn take_exact_from_pools(
        pools: &mut FreePools,
        min_count: u32,
        max_count: u32,
    ) -> Option<Extent> {
        let (from_reserve, extent) = Self::lowest_pool_candidate(
            pools.general.first_fit(min_count),
            pools.stripe_reserve.first_fit(min_count),
        )?;
        if from_reserve {
            pools.stripe_reserve.remove(&extent);
        } else {
            pools.general.remove(&extent);
        }
        let take = extent.count.min(max_count);
        if extent.count > take {
            let tail = Extent::new(Pba(extent.start.0 + take as u64), extent.count - take);
            if from_reserve {
                pools.insert_classified(tail);
            } else {
                pools.general.insert(Extent::new(
                    Pba(extent.start.0 + take as u64),
                    extent.count - take,
                ));
            }
        }
        Some(Extent::new(extent.start, take))
    }

    fn lowest_pool_candidate(
        general: Option<Extent>,
        reserve: Option<Extent>,
    ) -> Option<(bool, Extent)> {
        match (general, reserve) {
            (Some(general), Some(reserve)) => {
                if reserve.start.0 < general.start.0 {
                    Some((true, reserve))
                } else {
                    Some((false, general))
                }
            }
            (Some(general), None) => Some((false, general)),
            (None, Some(reserve)) => Some((true, reserve)),
            (None, None) => None,
        }
    }

    fn take_largest_from_pools(pools: &mut FreePools) -> Option<Extent> {
        let general = pools.general.largest();
        let reserve = pools.stripe_reserve.largest();
        let take_reserve = match (general, reserve) {
            (None, Some(_)) => true,
            (Some(g), Some(r)) => (r.count, r.start.0) > (g.count, g.start.0),
            _ => false,
        };
        if take_reserve {
            let extent = reserve.expect("selected reserve candidate");
            pools.stripe_reserve.remove(&extent);
            Some(extent)
        } else {
            let extent = general?;
            pools.general.remove(&extent);
            Some(extent)
        }
    }

    /// Front-carve `count` blocks off the lowest-address cached run that fits.
    /// Scans from the back (the cache is descending by start), so selection is
    /// first-fit-by-address within the lane. Front-carving keeps the remainder's
    /// start above the taken run and below the next-higher entry, so replacing
    /// in place preserves the ordering.
    fn take_from_extent_cache(cache: &mut Vec<Extent>, count: u32) -> Option<Extent> {
        let idx = (0..cache.len())
            .rev()
            .find(|&idx| cache[idx].count >= count)?;
        let extent = cache[idx];
        let result = Extent::new(extent.start, count);
        if extent.count == count {
            cache.remove(idx);
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
mod free_pool_policy_tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    use super::*;

    const STRIPE: u32 = 6;
    const PHASE: u32 = 2;

    fn allocator(blocks: u64, lanes: usize) -> SpaceAllocator {
        SpaceAllocator::new(blocks * BLOCK_SIZE as u64, lanes)
    }

    #[test]
    fn sub_stripe_release_before_next_alignment_never_expands() {
        let mut pools = FreePools::new();
        pools.set_geometry(STRIPE, PHASE);
        let released = Extent::new(Pba(12_954), 2);

        pools.insert_classified(released);

        assert_eq!(pools.free_blocks_in_pools(), 2);
        assert_eq!(pools.stripe_reserve.blocks_total(), 0);
        assert_eq!(
            pools.general.by_addr().iter().copied().collect::<Vec<_>>(),
            vec![released]
        );
        assert!(pools
            .overlapping_free(Extent::single(released.end_pba()))
            .is_none());
    }

    #[test]
    fn adjacent_u32_sized_releases_coalesce_without_count_truncation() {
        let mut pools = FreePools::new();
        pools.set_geometry(STRIPE, 0);
        let start = 12u64;
        let first = Extent::new(Pba(start), u32::MAX);
        let second = Extent::new(Pba(start + u32::MAX as u64), 100);

        pools.insert_classified(first);
        pools.insert_classified(second);

        let expected = u32::MAX as u64 + 100;
        assert_eq!(pools.free_blocks_in_pools(), expected);
        assert!(pools
            .stripe_reserve
            .by_addr()
            .iter()
            .all(|extent| extent.count.is_multiple_of(STRIPE)));

        let mut runs: Vec<Extent> = pools.general.by_addr().iter().copied().collect();
        runs.extend(pools.stripe_reserve.by_addr().iter().copied());
        runs.sort_unstable_by_key(|extent| extent.start.0);
        assert_eq!(runs.first().unwrap().start.0, start);
        assert_eq!(runs.last().unwrap().end_pba().0, start + expected);
        assert!(runs
            .windows(2)
            .all(|pair| pair[0].end_pba().0 == pair[1].start.0));
        assert_eq!(
            runs.iter().map(|extent| extent.count as u64).sum::<u64>(),
            expected
        );
    }

    #[test]
    fn geometry_change_preserves_quarantined_free_blocks() {
        let mut pools = FreePools::new();
        pools.set_geometry(STRIPE, PHASE);
        pools.insert_classified(Extent::new(Pba(30), 5));
        let mut free_parts = pools.empty_set_with_geometry();
        free_parts.insert(Extent::new(Pba(100), 3));
        pools.quarantines.insert(
            100,
            QuarantineTarget {
                range: Extent::new(Pba(100), STRIPE),
                free_parts,
            },
        );
        let before = pools.free_blocks_in_pools();

        pools.set_geometry(4, 0);

        assert!(pools.quarantines.is_empty());
        assert_eq!(pools.free_blocks_in_pools(), before);
        assert!(pools.overlapping_free(Extent::new(Pba(100), 3)).is_some());
    }

    #[test]
    fn exact_miss_drains_once_without_consuming_short_space() {
        let allocator = allocator(RESERVED_BLOCKS + 4, 1);
        assert_eq!(
            allocator.allocate_exact_extent_for_lane(0, 1).unwrap(),
            Extent::single(Pba(RESERVED_BLOCKS))
        );
        let cached_before = allocator.lane_extent_caches[0].lock().unwrap().clone();
        let free_before = allocator.free_block_count();
        assert_eq!(
            cached_before,
            vec![Extent::new(Pba(RESERVED_BLOCKS + 1), 3)]
        );

        for _ in 0..2 {
            assert!(matches!(
                allocator.allocate_exact_extent_for_lane(0, 4),
                Err(OnyxError::SpaceExhausted)
            ));
            assert_eq!(allocator.free_block_count(), free_before);
            assert!(allocator.is_extent_free(cached_before[0]));
        }
        assert!(allocator.lane_extent_caches[0].lock().unwrap().is_empty());
    }

    #[test]
    fn exact_miss_coalesces_global_and_lane_boundary_before_enospc() {
        let allocator = allocator(32, 1);
        {
            let mut pools = allocator.free_pools.lock().unwrap();
            *pools = FreePools::new();
            pools.general.insert(Extent::single(Pba(13)));
        }
        allocator.lane_extent_caches[0]
            .lock()
            .unwrap()
            .push(Extent::new(Pba(14), 2));

        assert_eq!(
            allocator.allocate_exact_extent_for_lane(0, 3).unwrap(),
            Extent::new(Pba(13), 3)
        );
    }

    #[test]
    fn cross_pool_selection_is_first_fit_for_single_exact_and_lane_refill() {
        let mut pools = FreePools::new();
        pools.general.insert(Extent::new(Pba(100), 16));
        pools.stripe_reserve.insert(Extent::new(Pba(10), 24));

        assert_eq!(
            SpaceAllocator::alloc_one_from_pools(&mut pools),
            Some(Pba(10))
        );
        assert_eq!(
            SpaceAllocator::take_exact_from_pools(&mut pools, 4, 4),
            Some(Extent::new(Pba(11), 4))
        );

        let allocator = allocator(128, 1);
        {
            let mut allocator_pools = allocator.free_pools.lock().unwrap();
            *allocator_pools = pools;
        }
        assert_eq!(
            allocator.refill_extent_lane(0, 2, 8),
            Some(Extent::new(Pba(15), 2))
        );
    }

    #[test]
    fn quarantine_cannot_miss_pool_to_lane_refill_in_flight() {
        let allocator = Arc::new(allocator(128, 1));
        let target = Extent::new(Pba(10), STRIPE);
        {
            let mut pools = allocator.free_pools.lock().unwrap();
            *pools = FreePools::new();
            pools.general.set_geometry(STRIPE, PHASE);
            pools.stripe_reserve.set_geometry(STRIPE, PHASE);
            pools.stripe_reserve.insert(target);
        }

        let held_lane = allocator.lane_caches[0].lock().unwrap();
        let refill = {
            let allocator = Arc::clone(&allocator);
            thread::spawn(move || {
                allocator
                    .refill_one_lane_from_global(0, LANE_CACHE_REFILL_SIZE)
                    .expect("test reserve contains a refill")
            })
        };
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if matches!(
                allocator.free_pools.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            ) {
                break;
            }
            assert!(Instant::now() < deadline, "refill never acquired FreePools");
            thread::yield_now();
        }
        let quarantine = {
            let allocator = Arc::clone(&allocator);
            thread::spawn(move || allocator.begin_defrag_quarantine(target))
        };
        drop(held_lane);

        let first = refill.join().unwrap();
        quarantine.join().unwrap().unwrap();
        assert_eq!(first, target.start);
        assert_eq!(
            allocator.defrag_quarantine_progress(target.start),
            Some((u64::from(STRIPE - 1), u64::from(STRIPE)))
        );
        assert!(allocator.lane_caches[0]
            .lock()
            .unwrap()
            .iter()
            .all(|pba| !target.contains(*pba)));

        allocator
            .track_alloc(Extent::single(first), "quarantine_refill_test")
            .unwrap();
        allocator.allocated_blocks.fetch_add(1, Ordering::Relaxed);
        allocator.free_blocks.fetch_sub(1, Ordering::Relaxed);
        allocator.free_one(first).unwrap();
        assert!(allocator.complete_defrag_quarantine(target.start).unwrap());
    }

    #[test]
    fn free_coverage_crosses_general_reserve_boundaries_by_run() {
        let allocator = allocator(32, 0);
        allocator.set_stripe_geometry(STRIPE, PHASE);
        let usable = Extent::new(Pba(RESERVED_BLOCKS), 32 - RESERVED_BLOCKS as u32);
        assert!(allocator.is_extent_free(usable));

        let allocated = allocator
            .allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
            .unwrap();
        assert!(!allocator.is_extent_free(usable));
        allocator.free_extent(allocated).unwrap();
        assert!(allocator.is_extent_free(usable));
    }

    #[test]
    fn explicit_alternate_stripe_geometry_keeps_legacy_api_working() {
        let allocator = allocator(64, 1);
        allocator.set_stripe_geometry(STRIPE, PHASE);

        let extent = allocator
            .allocate_stripe_extent_for_lane(0, 3, 4, 0)
            .unwrap();

        assert_eq!(extent.count, 4);
        assert_eq!(extent.start.0 % 4, 0);
    }

    #[test]
    fn quarantine_extracts_and_splits_lane_extent_cache() {
        let allocator = allocator(32, 1);
        assert_eq!(
            allocator.allocate_exact_extent_for_lane(0, 1).unwrap(),
            Extent::single(Pba(RESERVED_BLOCKS))
        );
        allocator.set_stripe_geometry(STRIPE, PHASE);
        let target = Extent::new(Pba(10), STRIPE);

        allocator.begin_defrag_quarantine(target).unwrap();

        assert_eq!(
            allocator.defrag_quarantine_progress(target.start),
            Some((STRIPE as u64, STRIPE as u64))
        );
        // The quarantine split one cached run into a head (9) and a tail (16);
        // both stay lane-local. Lane extent caches are ordered by DESCENDING
        // start (carves are taken from the back so a lane emits ascending PBAs —
        // see `push_extent_cache`), so the tail sorts ahead of the head here.
        let cached = allocator.lane_extent_caches[0].lock().unwrap().clone();
        assert_eq!(
            cached,
            vec![Extent::new(Pba(16), 16), Extent::single(Pba(9))]
        );
        assert!(allocator.complete_defrag_quarantine(target.start).unwrap());
        assert_eq!(
            allocator.contiguity_stats().stripe_reserve_blocks,
            STRIPE as u64
        );
    }

    #[test]
    fn quarantine_extracts_single_block_lane_cache_members() {
        let allocator = allocator(32, 1);
        assert_eq!(
            allocator.allocate_one_for_lane(0).unwrap(),
            Pba(RESERVED_BLOCKS)
        );
        allocator.set_stripe_geometry(STRIPE, PHASE);
        let target = Extent::new(Pba(10), STRIPE);

        allocator.begin_defrag_quarantine(target).unwrap();

        assert_eq!(
            allocator.defrag_quarantine_progress(target.start),
            Some((STRIPE as u64, STRIPE as u64))
        );
        assert!(allocator.lane_caches[0]
            .lock()
            .unwrap()
            .iter()
            .all(|pba| !target.contains(*pba)));
        assert!(allocator.complete_defrag_quarantine(target.start).unwrap());
    }

    #[test]
    fn quarantine_routes_releases_until_target_is_complete() {
        let allocator = allocator(128, 0);
        allocator.set_stripe_geometry(STRIPE, PHASE);
        let target = allocator
            .allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
            .unwrap();
        assert_eq!(target, Extent::new(Pba(10), STRIPE));
        allocator.begin_defrag_quarantine(target).unwrap();

        allocator
            .free_extent(Extent::new(target.start, STRIPE / 2))
            .unwrap();
        assert_eq!(
            allocator.defrag_quarantine_progress(target.start),
            Some(((STRIPE / 2) as u64, STRIPE as u64))
        );
        assert!(!allocator.complete_defrag_quarantine(target.start).unwrap());

        allocator
            .free_extent(Extent::new(
                Pba(target.start.0 + (STRIPE / 2) as u64),
                STRIPE / 2,
            ))
            .unwrap();
        assert!(allocator.complete_defrag_quarantine(target.start).unwrap());
        assert!(!allocator.is_defrag_quarantined(target));
        assert!(allocator.contiguity_stats().stripe_reserve_blocks >= STRIPE as u64);
    }

    #[test]
    fn dedup_pin_and_quarantine_publication_are_atomic() {
        let allocator = Arc::new(allocator(128, 0));
        allocator.set_stripe_geometry(STRIPE, PHASE);
        let target = allocator
            .allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
            .unwrap();
        let old_pin = allocator
            .pin_dedup_target_if_allowed(target.start, target.count)
            .expect("pin before publication must succeed");

        let worker = {
            let allocator = Arc::clone(&allocator);
            thread::spawn(move || allocator.begin_defrag_quarantine(target))
        };
        let deadline = Instant::now() + Duration::from_secs(2);
        while !allocator.is_defrag_quarantined(target) {
            assert!(Instant::now() < deadline, "quarantine was not published");
            thread::yield_now();
        }
        assert!(allocator
            .pin_dedup_target_if_allowed(target.start, target.count)
            .is_none());
        assert!(
            !worker.is_finished(),
            "pre-publication pin must be waited out"
        );

        drop(old_pin);
        worker.join().unwrap().unwrap();
        assert!(allocator.cancel_defrag_quarantine(target.start));
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

    /// contiguity_stats reflects the free set (blocks/extents/largest/eff) and
    /// eff_capacity is None without geometry, Some with it.
    #[test]
    fn contiguity_stats_reflects_free_set() {
        let a = new_alloc(8192);
        let s0 = a.contiguity_stats();
        assert_eq!(s0.free_blocks_in_set, 8192 - RESERVED_BLOCKS);
        assert_eq!(s0.free_extents, 1);
        assert_eq!(s0.largest_run_blocks as u64, 8192 - RESERVED_BLOCKS);
        assert_eq!(s0.stripe_capable_blocks, None, "no geometry configured");

        a.set_stripe_geometry(6, 2);
        let s1 = a.contiguity_stats();
        // Single run starting at RESERVED_BLOCKS=8: head_pad(8,6,2)=((8+2)%6=4→2),
        // eff = total - head, floored to whole stripes.
        let head = {
            let r = (RESERVED_BLOCKS + 2) % 6;
            if r == 0 {
                0
            } else {
                6 - r
            }
        };
        let eff = 8192 - RESERVED_BLOCKS - head;
        assert_eq!(s1.stripe_capable_blocks, Some(eff / 6 * 6));

        // Punch holes: allocate 3 blocks (front carve keeps one run), then
        // free-with-gap via retire is separate — just re-check counts move.
        let _ = a.allocate_one().unwrap();
        let s2 = a.contiguity_stats();
        assert_eq!(s2.free_blocks_in_set, 8192 - RESERVED_BLOCKS - 1);
    }

    /// grow_capacity appends the new top range to the free set (both the free
    /// atomic and the free set grow by the delta) and is a no-op when the new
    /// size is not larger.
    #[test]
    fn grow_capacity_appends_new_free_range() {
        let a = new_alloc(8192);
        let old_free = a.free_block_count();
        assert_eq!(a.total_block_count(), 8192);
        assert_eq!(old_free, 8192 - RESERVED_BLOCKS);

        // Grow to 16384 blocks (io-addressable size = blocks * BLOCK_SIZE).
        let new_total = a.grow_capacity(16384 * BLOCK_SIZE as u64).unwrap();
        assert_eq!(new_total, 16384);
        assert_eq!(a.total_block_count(), 16384);
        assert_eq!(a.free_block_count(), old_free + 8192);
        // The free set gained exactly the [8192, 16384) range worth of blocks
        // (coalesced with the existing top run or not — the total is invariant).
        assert_eq!(
            a.contiguity_stats().free_blocks_in_set,
            16384 - RESERVED_BLOCKS
        );

        // Equal / smaller is a no-op — the frontier never regresses.
        assert_eq!(a.grow_capacity(8192 * BLOCK_SIZE as u64).unwrap(), 16384);
        assert_eq!(a.total_block_count(), 16384);
        assert_eq!(a.free_block_count(), old_free + 8192);
    }

    /// free_extents_below_desc returns strictly-below extents in descending
    /// address order, capped at `max`.
    #[test]
    fn free_extents_below_desc_orders_and_caps() {
        let a = new_alloc(64);
        // Carve the single run into three fragments by allocating separators.
        // Layout after: free runs are rebuilt via direct set manipulation —
        // simpler: allocate everything, then free selected extents back.
        let total = 64 - RESERVED_BLOCKS;
        let first = a.allocate_extent(total as u32).unwrap();
        assert_eq!(first.start.0, RESERVED_BLOCKS);
        for e in [
            Extent::new(Pba(10), 2),
            Extent::new(Pba(20), 3),
            Extent::new(Pba(40), 4),
        ] {
            a.free_extent(e).unwrap();
        }
        let all = a.free_extents_below_desc(Pba(u64::MAX), 16);
        assert_eq!(
            all,
            vec![
                Extent::new(Pba(40), 4),
                Extent::new(Pba(20), 3),
                Extent::new(Pba(10), 2)
            ]
        );
        // Strictly below 40: excludes the extent starting at 40.
        let below = a.free_extents_below_desc(Pba(40), 16);
        assert_eq!(below.len(), 2);
        assert_eq!(below[0].start.0, 20);
        // Cap.
        let capped = a.free_extents_below_desc(Pba(u64::MAX), 1);
        assert_eq!(capped, vec![Extent::new(Pba(40), 4)]);
        assert!(a.free_extents_below_desc(Pba(u64::MAX), 0).is_empty());
    }

    /// retired_overlap_blocks sums clamped intersections, including a retired
    /// extent reaching into the range from below.
    #[test]
    fn retired_overlap_blocks_counts_intersections() {
        let a = new_alloc(128);
        let n = alloc_first(&a, 40); // n..n+40 allocated
        let t0 = Instant::now();
        // Retire [n+2, n+6) and [n+10, n+12).
        a.retire_extent_at(Extent::new(Pba(n + 2), 4), t0).unwrap();
        a.retire_extent_at(Extent::new(Pba(n + 10), 2), t0).unwrap();
        // Range covering both fully.
        assert_eq!(a.retired_overlap_blocks(Extent::new(Pba(n), 20)), 6);
        // Range starting inside the first retired run (reach-from-below).
        assert_eq!(a.retired_overlap_blocks(Extent::new(Pba(n + 4), 4)), 2);
        // Range with no overlap.
        assert_eq!(a.retired_overlap_blocks(Extent::new(Pba(n + 20), 5)), 0);
        // Range clipping the tail of the second run only.
        assert_eq!(a.retired_overlap_blocks(Extent::new(Pba(n + 11), 8)), 1);
    }

    /// HEADLINE: an aged block reclaims even while an adjacent younger block keeps
    /// arriving — the exact scenario the old coalesced-key grace map starved.
    #[test]
    fn no_reaging_under_adjacent_retire() {
        let a = new_alloc(8192);
        let n = alloc_first(&a, 2);
        let t0 = Instant::now();
        a.retire_extent_at(Extent::single(Pba(n)), t0).unwrap();
        a.retire_extent_at(Extent::single(Pba(n + 1)), t0 + secs(5))
            .unwrap();
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
                                    age.insert(
                                        pba,
                                        RetiredRun {
                                            count: 1,
                                            retired_at: now,
                                        },
                                    );
                                }
                            }
                            "all_in_age" => {
                                let retired_at = if i < young_front { now } else { base };
                                age.insert(
                                    pba,
                                    RetiredRun {
                                        count: 1,
                                        retired_at,
                                    },
                                );
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
                    n,
                    mode,
                    depth,
                    d_rbc,
                    d_aged,
                    emitted,
                    deferred,
                    cands.len()
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
        assert_eq!(
            a_batch.retired_block_count(),
            a_batch.retired_block_count_exact()
        );
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
        let (blocks, cnt) = a
            .reclaim_retired_extents_batch(&batch, &run_flag())
            .unwrap();
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
        a.free_pools
            .lock()
            .unwrap()
            .general
            .insert_for_test(Extent::single(p));
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
        a.free_pools
            .lock()
            .unwrap()
            .general
            .insert_for_test(Extent::single(Pba(n + 8))); // conflict on n+8
        let batch = [
            Extent::new(Pba(n), 2),      // freed (sub-extent of [n,4])
            Extent::single(Pba(n + 10)), // skip (never retired)
            Extent::single(Pba(n + 8)),  // conflict → stays retired
        ];
        let (blocks, _) = a
            .reclaim_retired_extents_batch(&batch, &run_flag())
            .unwrap();
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
            a_seq
                .retire_extent_at(Extent::single(Pba(n + i)), t0)
                .unwrap();
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
        assert_eq!(
            a_batch.retired_block_count(),
            a_batch.retired_block_count_exact()
        );
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
        assert_eq!(
            a_seq.allocated_block_count(),
            a_batch.allocated_block_count()
        );
        let seq_pools = a_seq.free_pools.lock().unwrap();
        let batch_pools = a_batch.free_pools.lock().unwrap();
        assert_eq!(
            *seq_pools.general.by_addr(),
            *batch_pools.general.by_addr(),
            "end-state general free lists must be identical"
        );
        assert_eq!(
            *seq_pools.stripe_reserve.by_addr(),
            *batch_pools.stripe_reserve.by_addr(),
            "end-state stripe reserves must be identical"
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
            Extent::single(Pba(n)),     // frees
            Extent::single(Pba(n + 4)), // free-overlap → failed
            Extent::single(Pba(n + 6)), // retired-overlap → failed
            Extent::single(Pba(n + 8)), // frees
        ];
        let (freed, failed) = a.free_extents_batch(&batch);
        assert_eq!(freed, 2);
        assert_eq!(
            failed,
            vec![Extent::single(Pba(n + 4)), Extent::single(Pba(n + 6))]
        );
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
    fn small_lane_allocations_preserve_cross_pool_first_fit() {
        let allocator = new_alloc_lanes(64, 1);
        let usable = 64 - RESERVED_BLOCKS;
        allocator.allocate_extent(usable as u32).unwrap();
        allocator.set_stripe_geometry(STRIPE, PHASE);
        allocator.free_extent(Extent::new(Pba(10), 12)).unwrap();
        allocator.free_extent(Extent::new(Pba(30), 5)).unwrap();

        for expected in 10..22 {
            let extent = allocator.allocate_extent_for_lane(0, 1).unwrap();
            assert_eq!(extent, Extent::single(Pba(expected)));
        }
        assert_eq!(
            allocator.allocate_extent_for_lane(0, 1).unwrap(),
            Extent::single(Pba(30))
        );
    }

    #[test]
    fn stripe_reserve_miss_reclaims_cold_lane_refill_for_hot_lane() {
        let allocator = new_alloc_lanes(128, 2);
        allocator.set_stripe_geometry(STRIPE, PHASE);

        let cold = allocator
            .allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
            .unwrap();
        assert!(!allocator.lane_extent_caches[0].lock().unwrap().is_empty());

        let hot = allocator
            .allocate_stripe_extent_for_lane(1, STRIPE, STRIPE, PHASE)
            .unwrap();
        assert_ne!(hot, cold);
        assert_eq!((hot.start.0 + u64::from(PHASE)) % u64::from(STRIPE), 0);
        assert_eq!(hot.count, STRIPE);
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
            assert_eq!(
                (ext.start.0 + RESERVED_BLOCKS) % STRIPE as u64,
                0,
                "device-aligned"
            );
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
        assert!(
            SpaceAllocator::carve_aligned_from_run(Extent::new(Pba(11), 6), 6, STRIPE, PHASE)
                .is_none()
        );
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
            a.allocate_stripe_extent_for_lane(0, 4, STRIPE, PHASE)
                .unwrap();
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
        let e = a
            .allocate_stripe_extent_for_lane(0, 4, STRIPE, PHASE)
            .unwrap();
        assert_eq!(e.count, 6);
        assert_eq!(a.free_block_count(), before - 6);
        a.free_extent(e).unwrap();
        assert_eq!(a.free_block_count(), before, "whole padded stripe returns");
    }

    #[test]
    fn global_path_aligns_without_lane() {
        // lane index out of range forces the global (no-lane) path.
        let a = new_alloc_lanes(4096, 0);
        let e = a
            .allocate_stripe_extent_for_lane(0, 7, STRIPE, PHASE)
            .unwrap();
        assert_eq!(e.count, 12);
        assert_eq!((e.start.0 + PHASE as u64) % STRIPE as u64, 0);
    }

    /// Build a reserve of ISOLATED single-stripe windows (the aged-pool shape
    /// where a one-run refill degrades to "global lock per allocation"), by
    /// freeing every other aligned window back.
    ///
    /// Returns the aligned window starts in ascending order.
    fn seed_isolated_stripe_windows(a: &SpaceAllocator, windows: usize) -> Vec<u64> {
        let blocks = (windows as u64 + 2) * 2 * STRIPE as u64 + RESERVED_BLOCKS;
        let usable = blocks - RESERVED_BLOCKS;
        a.allocate_extent(usable as u32).unwrap();
        a.set_stripe_geometry(STRIPE, PHASE);
        let first = SpaceAllocator::align_up_pba(RESERVED_BLOCKS, STRIPE as u64, PHASE as u64);
        let mut starts = Vec::with_capacity(windows);
        for i in 0..windows as u64 {
            // Stride of two stripes leaves a live stripe between every free one,
            // so nothing can coalesce: the reserve is `windows` runs of exactly
            // one stripe each.
            let start = first + i * 2 * STRIPE as u64;
            a.free_extent(Extent::new(Pba(start), STRIPE)).unwrap();
            starts.push(start);
        }
        starts
    }

    fn isolated_window_allocator(windows: usize) -> (SpaceAllocator, Vec<u64>) {
        let blocks = (windows as u64 + 2) * 2 * STRIPE as u64 + RESERVED_BLOCKS;
        let a = new_alloc_lanes(blocks, 4);
        let starts = seed_isolated_stripe_windows(&a, windows);
        (a, starts)
    }

    /// Batching a refill must not change SELECTION: taking the K lowest-address
    /// qualifying runs in one lock hold is exactly the sequence K successive
    /// `first_fit(need)` calls return, because removing an extent never
    /// coalesces. Pinned against the reserve's own address order.
    #[test]
    fn batched_refill_equals_sequential_refills() {
        const WINDOWS: usize = LANE_EXTENT_CACHE_REFILL_RUNS * 2 + 5;
        let (a, expected) = isolated_window_allocator(WINDOWS);

        let mut got = Vec::with_capacity(WINDOWS);
        for _ in 0..WINDOWS {
            let e = a
                .allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
                .expect("every seeded window can serve one stripe");
            assert_eq!(e.count, STRIPE);
            got.push(e.start.0);
        }
        assert_eq!(
            got, expected,
            "aligned allocation must consume the reserve in ascending address \
             order, exactly as one-run-at-a-time first-fit did"
        );
        // And the reserve is now empty rather than partially stranded.
        assert_eq!(a.contiguity_stats().stripe_reserve_blocks, 0);
    }

    /// The point of the batch: one global-lock refill must serve many
    /// allocations even when NO two free stripes are adjacent. Before this,
    /// `allocs_per_refill` was exactly 1.00 on this shape.
    #[test]
    fn refill_serves_many_allocs_from_a_discontiguous_reserve() {
        const WINDOWS: usize = LANE_EXTENT_CACHE_REFILL_RUNS * 2;
        let (a, _) = isolated_window_allocator(WINDOWS);

        for _ in 0..WINDOWS {
            a.allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
                .unwrap();
        }
        let supply = a.supply_stats();
        assert_eq!(supply.aligned_allocs, WINDOWS as u64);
        assert_eq!(
            supply.refills, 2,
            "K={LANE_EXTENT_CACHE_REFILL_RUNS} runs per refill ⇒ 2 refills for \
             2K windows, not one per allocation"
        );
        assert_eq!(supply.refill_runs, WINDOWS as u64);
        assert_eq!(supply.refill_blocks, WINDOWS as u64 * STRIPE as u64);
        assert!(
            supply.allocs_per_refill() >= LANE_EXTENT_CACHE_REFILL_RUNS as f64,
            "allocs_per_refill was {}",
            supply.allocs_per_refill()
        );
        assert_eq!(supply.drains, 0, "no lane-cache drain should be needed");
    }

    /// A lane hands out aligned carves in strictly ASCENDING PBA order even when
    /// its cache holds many disjoint runs. This is what keeps one L2P leaf's PBAs
    /// clustered; an unordered cache with `swap_remove` would scramble them
    /// across the whole refill.
    #[test]
    fn lane_extent_cache_hands_out_ascending() {
        let (a, _) = isolated_window_allocator(LANE_EXTENT_CACHE_REFILL_RUNS);
        let mut last = 0u64;
        for i in 0..LANE_EXTENT_CACHE_REFILL_RUNS {
            let e = a
                .allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
                .unwrap();
            assert!(
                e.start.0 > last || i == 0,
                "allocation {i} at {} went backwards from {last}",
                e.start.0
            );
            last = e.start.0;
        }
    }

    /// The cache stays ordered when a carve leaves head/tail remainders, and a
    /// later unaligned take still picks the lowest-address fit.
    #[test]
    fn push_extent_cache_keeps_descending_order_through_splits() {
        let mut cache = Vec::new();
        for start in [40u64, 10, 70, 22] {
            SpaceAllocator::push_extent_cache(&mut cache, Extent::new(Pba(start), 6));
        }
        assert_eq!(
            cache.iter().map(|e| e.start.0).collect::<Vec<_>>(),
            vec![70, 40, 22, 10]
        );
        // Lowest-address fit is taken first, and the front-carve remainder keeps
        // its slot.
        let got = SpaceAllocator::take_from_extent_cache(&mut cache, 2).unwrap();
        assert_eq!(got, Extent::new(Pba(10), 2));
        assert_eq!(
            cache.iter().map(|e| e.start.0).collect::<Vec<_>>(),
            vec![70, 40, 22, 12]
        );
        // A misaligned run that must yield a head pad keeps the cache ordered.
        cache.clear();
        SpaceAllocator::push_extent_cache(&mut cache, Extent::new(Pba(11), 3 * STRIPE));
        let aligned =
            SpaceAllocator::take_aligned_from_extent_cache(&mut cache, STRIPE, STRIPE, PHASE)
                .unwrap();
        assert_eq!((aligned.start.0 + PHASE as u64) % STRIPE as u64, 0);
        let starts: Vec<u64> = cache.iter().map(|e| e.start.0).collect();
        let mut sorted = starts.clone();
        sorted.sort_unstable_by(|a, b| b.cmp(a));
        assert_eq!(starts, sorted, "cache must stay descending by start");
    }

    /// A request wider than every reserve run must still be served from the one
    /// run that does fit, without the ascending walk scanning the whole reserve.
    /// (The seeded reserve is deliberately larger than
    /// `LANE_EXTENT_CACHE_REFILL_SCAN` single-stripe runs.)
    #[test]
    fn wide_request_against_single_stripe_reserve_stays_bounded() {
        const WINDOWS: usize = LANE_EXTENT_CACHE_REFILL_SCAN * 3;
        let (a, starts) = isolated_window_allocator(WINDOWS);
        // Widen exactly one window near the END of the reserve into two stripes,
        // so only it can serve a 2-stripe request and the walk would have to pass
        // every earlier entry to reach it.
        let wide = starts[WINDOWS - 2];
        a.free_extent(Extent::new(Pba(wide + STRIPE as u64), STRIPE))
            .unwrap();

        let e = a
            .allocate_stripe_extent_for_lane(0, 2 * STRIPE, STRIPE, PHASE)
            .expect("the one 2-stripe run must be found");
        assert_eq!(e.start.0, wide);
        assert_eq!(e.count, 2 * STRIPE);
        let supply = a.supply_stats();
        assert_eq!(supply.refills, 1);
        assert_eq!(
            supply.refill_runs, 1,
            "only the one qualifying run is taken; the walk must not keep going"
        );
    }

    /// The block budget still bounds a refill: one huge reserve run cannot park
    /// more than `LANE_EXTENT_CACHE_REFILL_BLOCKS` in a single lane.
    #[test]
    fn refill_respects_the_block_budget() {
        let blocks = 4 * LANE_EXTENT_CACHE_REFILL_BLOCKS as u64;
        let a = new_alloc_lanes(blocks, 2);
        a.set_stripe_geometry(STRIPE, PHASE);
        a.allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
            .unwrap();
        let supply = a.supply_stats();
        assert_eq!(supply.refills, 1);
        assert_eq!(
            supply.refill_runs, 1,
            "one contiguous run satisfies the whole budget"
        );
        assert!(
            supply.refill_blocks <= u64::from(LANE_EXTENT_CACHE_REFILL_BLOCKS),
            "refill took {} blocks, budget is {}",
            supply.refill_blocks,
            LANE_EXTENT_CACHE_REFILL_BLOCKS
        );
    }
}

#[cfg(test)]
pub(crate) mod aged_pool_bench {
    //! Local repro of the box-measured writer wall: 2026-07-28, QD256 j16d16 on
    //! an aged 256 GiB volume, the flush writer spent **55.6% of its time in
    //! aligned PBA allocation at 871 us/op** with `unaligned_ops = 0` and
    //! `reserve_miss_ops = 0`. Pool state then: `free_extents = 24,669,384`,
    //! `free_blocks_in_set = 84,000,718` (mean free extent 3.4 blocks),
    //! `stripe_capable` flat at 27%.
    //!
    //! Two non-exclusive causes were open, and one single-threaded number
    //! separates them:
    //!   (A) the critical section itself is expensive — `first_fit` walks every
    //!       distinct size class >= min_count under the global lock, plus three
    //!       BTreeSet removes on a cache-cold multi-hundred-MB structure;
    //!   (B) pure 16-way convoy on one `Mutex`.
    //! Single-threaded ~100 us/op ⇒ (A) dominates. Single-threaded a few us with
    //! a large multi-thread multiplier ⇒ (B) dominates.
    //!
    //! This also instruments the one link the box run left unmeasured: how many
    //! blocks a `refill_stripe_extent_lane` actually takes.
    //!
    //! Run:
    //! ```text
    //! cargo test --release --lib aged_pool_bench -- --ignored --nocapture
    //! ```
    //! `ONYX_BENCH_SCALE=<n>` overrides the free-extent target (default: the box
    //! figure). The full-scale pool needs ~2.5 GiB RSS and ~1 min to build.
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    const STRIPE: u32 = 6;
    const PHASE: u32 = (RESERVED_BLOCKS % STRIPE as u64) as u32;
    /// Box `free_extents` at the time of the 871 us/op measurement.
    const BOX_FREE_EXTENTS: u64 = 24_669_384;
    /// Box `free_blocks_in_set` — mean free extent 3.4 blocks.
    const BOX_FREE_BLOCKS: u64 = 84_000_718;
    /// Box `stripe_capable` share of `free_blocks_in_set`.
    const BOX_STRIPE_CAPABLE_PCT: f64 = 27.0;

    /// Shape of the long-run tail that carries the stripe reserve. `D` (the
    /// number of distinct size classes `first_fit` has to walk) is an OUTPUT of
    /// this choice, not an input, so both ends are measured: `Spread` gives a
    /// broad size distribution (large D), `Fixed` a narrow one (small D). The
    /// real pool sits somewhere between — background defrag publishes large
    /// compacted runs into the reserve, which widens it.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum TailShape {
        Spread,
        Fixed,
        /// The pessimal shape, and the one the box's own numbers point at: the
        /// reserve is nothing but ISOLATED single-stripe windows. A refill can
        /// then only ever take 6 blocks, so the lane cache serves exactly one
        /// allocation and every single aligned alloc takes the global lock.
        /// `largest_run` collapsing (box: −60% over 40 min) is what produces it.
        SingleStripe,
    }

    /// Synthesize an aged pool whose `contiguity_stats()` match the box:
    /// `target_extents` free runs totalling ~`3.4 * target_extents` blocks with
    /// ~27% of those blocks in whole-stripe aligned middles.
    ///
    /// Mixture solved from the three box numbers: with `N_l` long runs carrying
    /// `reserve + 3*N_l` blocks and the rest short (< one stripe, so they
    /// contribute zero stripe capacity), `N_l = 2%` of runs at mean length ~48
    /// lands all three at once (0.5 M x 48 = 24 M blocks, of which ~22.5 M are
    /// aligned middles = 27% of 84 M; the remaining 24.2 M runs average 2.5).
    pub(crate) fn build_aged_pool(
        target_extents: u64,
        shape: TailShape,
        lanes: usize,
    ) -> (SpaceAllocator, ContiguityStats) {
        const LONG_RUN_MEAN: u64 = 48;
        // Reserve-carrying runs per total runs. `SingleStripe` needs many more of
        // them to reach the same 27% stripe capacity, since each carries only one
        // stripe: 22.7 M / 6 = 3.8 M runs out of 24.7 M ≈ 1 in 7.
        let long_run_in_n: u64 = match shape {
            TailShape::Spread | TailShape::Fixed => 50, // 2%
            TailShape::SingleStripe => 7,               // ~14%
        };
        let mut rng = StdRng::seed_from_u64(0x00a1_10ca_7ed0_u64.wrapping_mul(target_extents | 1));

        // Walk PBA ascending, emitting run/gap pairs. The gap is live data, so
        // total span = free blocks + live blocks; size the device to fit.
        let mut runs: Vec<Extent> = Vec::with_capacity(target_extents as usize);
        let mut cursor = RESERVED_BLOCKS;
        for i in 0..target_extents {
            let len = if i % long_run_in_n == 0 {
                match shape {
                    // Geometric-ish spread around the mean → many distinct sizes.
                    TailShape::Spread => {
                        let mut l = STRIPE as u64;
                        while l < 8 * LONG_RUN_MEAN && rng.gen_bool(0.88) {
                            l += STRIPE as u64;
                        }
                        l
                    }
                    TailShape::Fixed => LONG_RUN_MEAN,
                    TailShape::SingleStripe => {
                        // Start ON an alignment boundary so the whole run is the
                        // aligned middle: reserve gets exactly one stripe, with
                        // no head/tail spilling into general.
                        cursor =
                            SpaceAllocator::align_up_pba(cursor, STRIPE as u64, PHASE as u64);
                        STRIPE as u64
                    }
                }
            } else {
                rng.gen_range(1..=4u64) // mean 2.5, all sub-stripe
            };
            runs.push(Extent::new(Pba(cursor), len as u32));
            // Gap >= 1 keeps runs non-adjacent so classification never coalesces.
            cursor += len + rng.gen_range(1..=5u64);
        }

        let device_blocks = cursor + 1024;
        let allocator = SpaceAllocator::new(device_blocks * BLOCK_SIZE as u64, lanes);
        {
            let mut pools = allocator.free_pools.lock().unwrap();
            pools.replace_general(BTreeSet::new()); // drop the fresh whole-device run
            pools.set_geometry(STRIPE, PHASE);
            for run in &runs {
                pools.insert_classified(*run);
            }
        }
        let free_blocks: u64 = runs.iter().map(|r| r.count as u64).sum();
        let usable = device_blocks - RESERVED_BLOCKS;
        allocator.free_blocks.store(free_blocks, Ordering::Relaxed);
        allocator
            .allocated_blocks
            .store(usable - free_blocks, Ordering::Relaxed);
        let stats = allocator.contiguity_stats();
        (allocator, stats)
    }

    /// Distinct `count` values in the stripe reserve — the `D` in `first_fit`'s
    /// O(D log N) size-class walk, and the direct predictor for hypothesis (A).
    fn reserve_size_classes(allocator: &SpaceAllocator) -> usize {
        let pools = allocator.free_pools.lock().unwrap();
        let mut classes: Vec<u32> = pools
            .stripe_reserve
            .by_addr()
            .iter()
            .map(|e| e.count)
            .collect();
        classes.sort_unstable();
        classes.dedup();
        classes.len()
    }

    fn percentile(sorted_ns: &[u64], p: f64) -> f64 {
        if sorted_ns.is_empty() {
            return 0.0;
        }
        let idx = ((sorted_ns.len() - 1) as f64 * p).round() as usize;
        sorted_ns[idx] as f64 / 1000.0
    }

    #[test]
    #[ignore = "perf microbench"]
    fn bench_aged_pool_stripe_alloc() {
        let scale: u64 = std::env::var("ONYX_BENCH_SCALE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(BOX_FREE_EXTENTS);
        const OPS: usize = 100_000;
        let threads: usize = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8);

        for shape in [
            TailShape::Fixed,
            TailShape::Spread,
            TailShape::SingleStripe,
        ] {
            let build = Instant::now();
            let (allocator, stats) = build_aged_pool(scale, shape, 16);
            let capable = stats.stripe_capable_blocks.unwrap_or(0);
            let classes = reserve_size_classes(&allocator);
            println!(
                "\n=== shape={:?} scale={} built in {:?} ===\n\
                 free_extents={} free_blocks={} mean_extent={:.2} \
                 stripe_capable={} ({:.1}%) reserve={} largest_run={} \
                 reserve_size_classes(D)={}\n\
                 box reference:  free_extents={} free_blocks={} mean_extent=3.40 \
                 stripe_capable={:.1}%",
                shape,
                scale,
                build.elapsed(),
                stats.free_extents,
                stats.free_blocks_in_set,
                stats.free_blocks_in_set as f64 / stats.free_extents.max(1) as f64,
                capable,
                capable as f64 / stats.free_blocks_in_set.max(1) as f64 * 100.0,
                stats.stripe_reserve_blocks,
                stats.largest_run_blocks,
                classes,
                BOX_FREE_EXTENTS,
                BOX_FREE_BLOCKS,
                BOX_STRIPE_CAPABLE_PCT,
            );

            // (1) The raw reserve query, no lock, no allocation: isolates the
            // size-class walk that hypothesis (A) blames.
            let mut ff_ns = Vec::with_capacity(1000);
            {
                let pools = allocator.free_pools.lock().unwrap();
                for _ in 0..1000 {
                    let t = Instant::now();
                    let hit = pools.stripe_reserve.first_fit(STRIPE);
                    ff_ns.push(t.elapsed().as_nanos() as u64);
                    assert!(hit.is_some(), "reserve must be able to serve one stripe");
                }
            }
            ff_ns.sort_unstable();
            println!(
                "  first_fit(6) on reserve alone:      p50 {:8.2} us  p99 {:8.2} us",
                percentile(&ff_ns, 0.5),
                percentile(&ff_ns, 0.99)
            );

            // (2) Single-threaded end-to-end allocation = the critical section.
            let mut ns = Vec::with_capacity(OPS);
            let mut served = 0usize;
            for _ in 0..OPS {
                let t = Instant::now();
                let got = allocator.allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE);
                ns.push(t.elapsed().as_nanos() as u64);
                if got.is_ok() {
                    served += 1;
                }
            }
            let total_us: f64 = ns.iter().sum::<u64>() as f64 / 1000.0;
            ns.sort_unstable();
            println!(
                "  1 thread  x {OPS} ops (served {served}): mean {:8.2} us  p50 {:8.2} us  \
                 p99 {:8.2} us",
                total_us / OPS as f64,
                percentile(&ns, 0.5),
                percentile(&ns, 0.99)
            );

            // (3) Same op under contention. The box ran 16 writers; this host has
            // fewer cores, so the multiplier here is a LOWER bound on the box's.
            let allocator = std::sync::Arc::new(allocator);
            let wall = Instant::now();
            let per_thread: Vec<(u64, usize)> = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..threads)
                    .map(|lane| {
                        let allocator = allocator.clone();
                        scope.spawn(move || {
                            let mut sum_ns = 0u64;
                            let mut ok = 0usize;
                            for _ in 0..(OPS / threads) {
                                let t = Instant::now();
                                let got = allocator
                                    .allocate_stripe_extent_for_lane(lane, STRIPE, STRIPE, PHASE);
                                sum_ns += t.elapsed().as_nanos() as u64;
                                if got.is_ok() {
                                    ok += 1;
                                }
                            }
                            (sum_ns, ok)
                        })
                    })
                    .collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            });
            let wall = wall.elapsed();
            let ops: usize = per_thread.iter().map(|(_, ok)| ok).sum();
            let sum_ns: u64 = per_thread.iter().map(|(ns, _)| ns).sum();
            println!(
                "  {threads} threads x {} ops each (served {ops}): mean {:8.2} us/op  \
                 wall {:?}  => {:.0} allocs/s = {:.1} MiB/s of 24 KiB stripes",
                OPS / threads,
                sum_ns as f64 / 1000.0 / ops.max(1) as f64,
                wall,
                ops as f64 / wall.as_secs_f64(),
                ops as f64 / wall.as_secs_f64() * 24.0 / 1024.0,
            );
            println!(
                "  box baseline for comparison:        871.00 us/op, 9333 allocs/s, \
                 213.7 MiB/s write"
            );
            let supply = allocator.supply_stats();
            println!(
                "  supply: refills={} blocks/refill={:.1} runs/refill={:.1} \
                 allocs/refill={:.2} drains={} drain_blocks={}",
                supply.refills,
                supply.blocks_per_refill(),
                supply.runs_per_refill(),
                supply.allocs_per_refill(),
                supply.drains,
                supply.drain_blocks,
            );
        }
    }

    /// The writers are not the only traffic on `free_pools`. Every overwrite
    /// retires its old PBA and the GC reclaims it, and both go through the
    /// BATCH_LOCK_CHUNK paths, whose Phase-B hold does up to 4096
    /// `release_extent` (coalesce-insert) calls in ONE lock hold. The comment on
    /// `reclaim_retired_extents_batch` already estimates "tens of ms on a
    /// multi-million-extent free list" and records box-measured "22-80
    /// thread-s/s alloc convoy spikes phase-locked to every 262K-block reclaim
    /// batch" — this measures that hold directly, and then measures what it does
    /// to concurrent aligned allocation.
    ///
    /// Run: `cargo test --release --lib bench_batch_hold_vs_alloc -- --ignored --nocapture`
    #[test]
    #[ignore = "perf microbench"]
    fn bench_batch_hold_vs_alloc() {
        let scale: u64 = std::env::var("ONYX_BENCH_SCALE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(BOX_FREE_EXTENTS);
        let threads: usize = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8);
        // Steady state: 6 blocks allocated per stripe write ⇒ 6 blocks retired
        // and reclaimed per stripe write. Model the reclaim side, which is the
        // one that re-inserts into the free pool.
        let (allocator, stats) = build_aged_pool(scale, TailShape::Spread, 16);
        println!(
            "\n=== batch-hold vs alloc: free_extents={} free_blocks={} ===",
            stats.free_extents, stats.free_blocks_in_set
        );

        // Carve allocated (live) extents to feed the retire/reclaim cycle. The
        // synthetic pool's gaps are live, so take a slice of them by allocating
        // fresh — simplest faithful source is the allocator itself.
        let mut owned: Vec<Extent> = Vec::with_capacity(BATCH_LOCK_CHUNK * 4);
        while owned.len() < BATCH_LOCK_CHUNK * 4 {
            match allocator.allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE) {
                Ok(e) => owned.push(e),
                Err(_) => break,
            }
        }
        assert!(
            owned.len() >= BATCH_LOCK_CHUNK,
            "need at least one full chunk of live extents"
        );

        // (1) One chunk's retire hold, then one chunk's reclaim hold.
        let chunk: Vec<Extent> = owned.drain(..BATCH_LOCK_CHUNK).collect();
        let t = Instant::now();
        let (newly, failed) = allocator.retire_extents_batch(&chunk, Instant::now());
        let retire_ms = t.elapsed().as_secs_f64() * 1e3;
        let running = AtomicBool::new(true);
        let t = Instant::now();
        let reclaimed = allocator
            .reclaim_retired_extents_batch(&chunk, &running)
            .unwrap();
        let reclaim_ms = t.elapsed().as_secs_f64() * 1e3;
        println!(
            "  one {BATCH_LOCK_CHUNK}-extent chunk: retire {:8.2} ms (newly={newly} \
             failed={})  reclaim {:8.2} ms (blocks={} extents={})",
            retire_ms,
            failed.len(),
            reclaim_ms,
            reclaimed.0,
            reclaimed.1,
        );
        println!("    (the BATCH_LOCK_CHUNK doc comment claims \"~sub-millisecond\" per hold)");

        // (2) Aligned allocation latency with that batch traffic running
        // concurrently, vs the same measurement with the background idle.
        let allocator = std::sync::Arc::new(allocator);
        for background in [false, true] {
            let stop = std::sync::Arc::new(AtomicBool::new(false));
            let batches = std::sync::Arc::new(AtomicU64::new(0));
            // The real overwrite cycle: allocate → retire → GC reclaim → the
            // blocks come back free. Re-retiring an already-free extent fails
            // fast and does no work, so the batch MUST be freshly allocated each
            // round or the background silently becomes a no-op.
            let held_ns = std::sync::Arc::new(AtomicU64::new(0));
            let bg = background.then(|| {
                let allocator = allocator.clone();
                let stop = stop.clone();
                let batches = batches.clone();
                let held_ns = held_ns.clone();
                std::thread::spawn(move || {
                    let running = AtomicBool::new(true);
                    let bg_lane = 15; // not one of the measured lanes
                    while !stop.load(Ordering::Relaxed) {
                        let mut batch = Vec::with_capacity(BATCH_LOCK_CHUNK);
                        while batch.len() < BATCH_LOCK_CHUNK {
                            match allocator
                                .allocate_stripe_extent_for_lane(bg_lane, STRIPE, STRIPE, PHASE)
                            {
                                Ok(e) => batch.push(e),
                                Err(_) => break,
                            }
                        }
                        if batch.is_empty() {
                            break;
                        }
                        let t = Instant::now();
                        let (_, _) = allocator.retire_extents_batch(&batch, Instant::now());
                        let _ = allocator.reclaim_retired_extents_batch(&batch, &running);
                        held_ns.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        batches.fetch_add(1, Ordering::Relaxed);
                    }
                })
            });

            let ops_per_thread = 20_000;
            let wall = Instant::now();
            let samples: Vec<Vec<u64>> = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..threads)
                    .map(|lane| {
                        let allocator = allocator.clone();
                        scope.spawn(move || {
                            let mut ns = Vec::with_capacity(ops_per_thread);
                            for _ in 0..ops_per_thread {
                                let t = Instant::now();
                                let _ = allocator
                                    .allocate_stripe_extent_for_lane(lane, STRIPE, STRIPE, PHASE);
                                ns.push(t.elapsed().as_nanos() as u64);
                            }
                            ns
                        })
                    })
                    .collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            });
            let wall = wall.elapsed();
            stop.store(true, Ordering::Relaxed);
            if let Some(bg) = bg {
                let _ = bg.join();
            }
            let mut all: Vec<u64> = samples.into_iter().flatten().collect();
            let mean = all.iter().sum::<u64>() as f64 / 1000.0 / all.len() as f64;
            all.sort_unstable();
            let bg_busy = held_ns.load(Ordering::Relaxed) as f64 / wall.as_nanos() as f64 * 100.0;
            println!(
                "  background_batches={:<5} {threads} threads: mean {:8.2} us  p50 {:8.2} us  \
                 p99 {:8.2} us  p999 {:8.2} us  (bg rounds={} busy {:.0}% wall {:?})",
                background,
                mean,
                percentile(&all, 0.5),
                percentile(&all, 0.99),
                percentile(&all, 0.999),
                batches.load(Ordering::Relaxed),
                bg_busy,
                wall,
            );
        }
        let supply = allocator.supply_stats();
        println!(
            "  supply: refills={} blocks/refill={:.1} allocs/refill={:.2} drains={} \
             drain_blocks={}",
            supply.refills,
            supply.blocks_per_refill(),
            supply.allocs_per_refill(),
            supply.drains,
            supply.drain_blocks,
        );
    }
}
