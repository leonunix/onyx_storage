use super::*;

/// RAII guard over ONE region's `FreePools` that charges its hold to a
/// [`FreeLockSite`] and refreshes that region's advisory hints on release.
pub(super) struct FreeLockGuard<'a> {
    pub(super) pools: std::sync::MutexGuard<'a, FreePools>,
    pub(super) shard: &'a LockStatShard<FREE_LOCK_SITES>,
    pub(super) site: usize,
    /// `None` when this acquisition was sampled away — see [`LOCK_STAT_SHARDS`].
    pub(super) acquired: Option<Instant>,
    /// Whether this hold ever handed out `&mut FreePools`, i.e. whether the
    /// advisory hints can possibly have gone stale. Set by `DerefMut`, which is
    /// the ONLY way to reach a mutation: the `MutexGuard` is private to this
    /// guard, and `FreePools` has no interior mutability, so a hold that never
    /// took a `&mut` cannot have changed anything the hints summarise.
    ///
    /// This matters because most acquisitions are read-only probes — audits,
    /// `is_free`, and above all `take_largest_regionwise`'s scan phase, which
    /// takes one lock per region — and each of them used to pay for the full hint
    /// recomputation plus two stores to cache lines that every other region
    /// walker is reading.
    pub(super) dirty: bool,
    /// Advisory aggregates for this region, refreshed on drop — see
    /// [`RegionPools::free_hint`].
    pub(super) free_hint: &'a AtomicU64,
    pub(super) largest_hint: &'a AtomicU64,
    pub(super) stripe_hint: &'a AtomicU64,
}

impl Drop for FreeLockGuard<'_> {
    fn drop(&mut self) {
        // Refresh BEFORE the hold is charged so the hints are always published
        // by a thread that still holds the region lock: a reader can therefore
        // only ever see a value that was true at some point while the lock was
        // held, never a torn or future one. Both reads are O(1) maintained
        // aggregates (plus the normally-empty quarantine map).
        if self.dirty {
            self.free_hint
                .store(self.pools.free_blocks_in_pools(), Ordering::Relaxed);
            self.largest_hint.store(
                self.pools
                    .largest_allocatable()
                    .map_or(0, |(run, _)| u64::from(run.count)),
                Ordering::Relaxed,
            );
            self.stripe_hint.store(
                self.pools
                    .stripe_reserve
                    .largest()
                    .map_or(0, |run| u64::from(run.count)),
                Ordering::Relaxed,
            );
        }
        if let Some(acquired) = self.acquired {
            self.shard
                .charge_hold(self.site, acquired.elapsed().as_nanos() as u64);
        }
    }
}

impl FreeLockGuard<'_> {
    /// Record how many extents this hold covered (batch paths only).
    pub(super) fn charge_items(&self, n: u64) {
        self.shard.charge_items(self.site, n);
    }

    /// Republish the hints on release even though this hold mutated nothing.
    ///
    /// For the one caller that learns a hint was WRONG without changing the pool:
    /// [`SpaceAllocator::take_largest_regionwise`] picks a region by hint and can
    /// find it empty, and without this the `dirty` skip would leave the bad hint
    /// in place for the next argmax to pick again.
    pub(super) fn mark_hints_stale(&mut self) {
        self.dirty = true;
    }
}

impl std::ops::Deref for FreeLockGuard<'_> {
    type Target = FreePools;
    fn deref(&self) -> &FreePools {
        &self.pools
    }
}

impl std::ops::DerefMut for FreeLockGuard<'_> {
    fn deref_mut(&mut self) -> &mut FreePools {
        self.dirty = true;
        &mut self.pools
    }
}

/// One aligned lane refill's request shape — see
/// [`SpaceAllocator::refill_stripe_extent_lane`].
#[derive(Debug, Clone, Copy)]
pub(super) struct StripeRefill {
    /// Blocks the refill must hand back (already a whole number of stripes).
    pub(super) min_count: u32,
    /// Block budget for the whole refill, i.e. how much may be parked in the
    /// lane cache.
    pub(super) max_count: u32,
    /// RAID geometry the reserve is indexed for; a region whose pools carry a
    /// different `(stripe, phase)` is skipped.
    pub(super) stripe: u32,
    pub(super) phase: u32,
    /// Minimum width a reserve run must have to QUALIFY as a candidate. Always
    /// `>= min_count`; equal to it on the legacy path.
    pub(super) floor: u32,
}

/// Free-space policy classes protected by one lock. Every free PBA belongs to
/// exactly one of `general`, `stripe_reserve`, an active quarantine's
/// `free_parts`, or a detached lane cache.
pub(super) struct FreePools {
    pub(super) general: FreeSet,
    pub(super) stripe_reserve: FreeSet,
    pub(super) quarantines: BTreeMap<u64, QuarantineTarget>,
}

pub(super) struct QuarantineTarget {
    pub(super) range: Extent,
    pub(super) free_parts: FreeSet,
}

impl FreePools {
    pub(super) fn new() -> Self {
        Self {
            general: FreeSet::new(),
            stripe_reserve: FreeSet::new(),
            quarantines: BTreeMap::new(),
        }
    }

    pub(super) fn geometry(&self) -> Option<(u32, u32)> {
        self.general.geometry()
    }

    pub(super) fn empty_set_with_geometry(&self) -> FreeSet {
        let mut set = FreeSet::new();
        if let Some((stripe, phase)) = self.geometry() {
            set.set_geometry(stripe, phase);
        }
        set
    }

    /// Empty every policy class and hand back the free runs they held, including
    /// the already-free parts of active quarantines (which are dropped — the
    /// pre-region `set_geometry` did the same, pinned by
    /// `geometry_change_preserves_quarantined_free_blocks`).
    pub(super) fn take_all_runs(&mut self) -> Vec<Extent> {
        let mut runs: Vec<Extent> = self.general.by_addr().iter().copied().collect();
        runs.extend(self.stripe_reserve.by_addr().iter().copied());
        runs.extend(
            self.quarantines
                .values()
                .flat_map(|target| target.free_parts.by_addr().iter().copied()),
        );
        self.general = FreeSet::new();
        self.stripe_reserve = FreeSet::new();
        self.quarantines.clear();
        runs
    }

    /// Install a geometry on an already-emptied pool.
    pub(super) fn reset_geometry(&mut self, stripe: u32, phase: u32) {
        self.general = FreeSet::new();
        self.stripe_reserve = FreeSet::new();
        self.general.set_geometry(stripe, phase);
        self.stripe_reserve.set_geometry(stripe, phase);
        self.quarantines.clear();
    }

    pub(super) fn replace_general(&mut self, free: BTreeSet<Extent>) {
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
    pub(super) fn insert_classified(&mut self, extent: Extent) {
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

    /// The extent [`SpaceAllocator::take_largest_from_pools`] would hand out, and
    /// which pool it came from. Quarantined free parts are excluded because they
    /// are not allocatable, which is exactly what makes this usable as the
    /// `largest_hint` source: the hint and the take can never disagree about what
    /// this region can serve, because they are the same function.
    pub(super) fn largest_allocatable(&self) -> Option<(Extent, bool)> {
        let general = self.general.largest();
        let reserve = self.stripe_reserve.largest();
        match (general, reserve) {
            (None, None) => None,
            (None, Some(r)) => Some((r, true)),
            (Some(g), None) => Some((g, false)),
            (Some(g), Some(r)) => {
                if (r.count, r.start.0) > (g.count, g.start.0) {
                    Some((r, true))
                } else {
                    Some((g, false))
                }
            }
        }
    }

    pub(super) fn free_blocks_in_pools(&self) -> u64 {
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

    pub(super) fn overlapping_free(&self, extent: Extent) -> Option<Extent> {
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
    pub(super) fn covers_free(&self, extent: Extent) -> bool {
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

    pub(super) fn overlaps_reserve(&self, extent: Extent) -> bool {
        Self::overlapping_in_set(&self.stripe_reserve, extent).is_some()
    }

    pub(super) fn overlapping_quarantine(&self, extent: Extent) -> Option<Extent> {
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

    /// Blocks of `range` covered by this region's free space — general +
    /// stripe reserve + the already-free parts of any active quarantine.
    /// Shared by the single-window [`SpaceAllocator::free_overlap_blocks`] and
    /// the batched [`SpaceAllocator::classify_stripe_windows`], so the two can
    /// never disagree.
    pub(super) fn overlap_free_blocks(&self, range: Extent) -> u64 {
        let mut covered = free_set_overlap_blocks(&self.general, range)
            + free_set_overlap_blocks(&self.stripe_reserve, range);
        for start in self.quarantine_starts_overlapping(range) {
            covered += free_set_overlap_blocks(
                &self
                    .quarantines
                    .get(&start)
                    .expect("quarantine key remains present")
                    .free_parts,
                range,
            );
        }
        covered
    }

    pub(super) fn quarantine_starts_overlapping(&self, extent: Extent) -> Vec<u64> {
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

    pub(super) fn extract_from_general(&mut self, range: Extent) -> Vec<Extent> {
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
    pub(super) fn release_extent(&mut self, extent: Extent) {
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

/// Blocks of `range` covered by `set` — O(log N + overlaps in range). Callers
/// hand in the UNCLIPPED range and apply it per region: a region's sets only
/// hold in-region extents, so clamping to the full range is equivalent to
/// clipping first (see [`RegionPools`]'s containment invariant).
fn free_set_overlap_blocks(set: &FreeSet, range: Extent) -> u64 {
    let (s, e) = (range.start.0, range.end_pba().0);
    let mut covered = 0u64;
    // The last extent starting at/before `s` may reach into the range.
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
}

/// The free space, sharded by PBA address into independently-locked regions.
///
/// Each region holds a complete [`FreePools`] (general / stripe reserve /
/// quarantines) restricted to its own address range — **every insert path
/// clips to the region**, so a region's sets never contain an out-of-region
/// extent. That single invariant is what makes every query composable: a
/// containment or overlap question about an extent is answered by asking only
/// the regions it spans.
///
/// Why shard by address rather than shorten the holds: the 2026-07-29 box
/// attribution showed 98% of the holding comes from GC retire/reclaim, whose
/// per-extent cost is dominated by work on the RETIRED structures that must
/// stay atomic with the free-side overlap check (a concurrent `free_extent`
/// and a retire that both pass their checks would produce double ownership —
/// the project's two premature-free P0s were exactly this class of bug). So the
/// holds are kept EXACTLY as they were, atomicity included, and only the lock
/// they serialize on is split: one 68%-busy mutex becomes N at 68/N%.
///
/// Region boundaries are stripe-aligned, so `insert_classified`'s
/// general/reserve classification behaves identically inside a region as it did
/// globally. The only thing lost is coalescing ACROSS a boundary: one seam per
/// region (≤ 2048 extents against the box's 24.6 M) never folds.
pub(super) struct RegionPools {
    pub(super) pools: Vec<Mutex<FreePools>>,
    /// Routing divisor in blocks; 0 = single region (sharding off). Only
    /// rewritten by `set_geometry`, which holds every region lock.
    pub(super) region_blocks: AtomicU64,
    /// First stripe-aligned PBA. Region boundaries sit at
    /// `region_base + i*region_blocks` so none of them splits a stripe window.
    pub(super) region_base: AtomicU64,
    /// Advisory per-region free-block totals, refreshed under the region lock by
    /// [`FreeLockGuard::drop`].
    ///
    /// Read lock-free ONLY to skip a region in an ascending walk or to pick a
    /// lane's region. A stale-zero read is indistinguishable from having taken
    /// that region's lock a moment earlier — the same benign race the single
    /// global lock always had between a free and a concurrent allocation — so it
    /// can never produce a wrong answer, only a slightly older one. The
    /// ENOSPC boundary keeps its `drain_lane_caches` + retry, unchanged.
    pub(super) free_hint: Vec<AtomicU64>,
    /// Advisory per-region largest ALLOCATABLE run, in blocks — i.e. the width of
    /// the extent [`FreePools::largest_allocatable`] would return, across both
    /// policy pools.
    ///
    /// This one is not just a skip filter: it is what makes
    /// [`SpaceAllocator::take_largest_regionwise`] an argmax over `count` relaxed
    /// loads instead of `count` MUTEX acquisitions. Two properties make that
    /// substitution exact rather than advisory:
    ///   - it is derived from the same function the take uses, so hint and take
    ///     cannot disagree about what a region can serve, and
    ///   - it is never stale-LOW while no mutation is in flight: the seeding
    ///     constructor publishes it, and every mutation publishes it on release
    ///     (see [`FreeLockGuard`]'s `dirty` flag).
    /// A mutation that has not yet released can leave it trailing, which is the
    /// same benign "a concurrent free just landed" race `free_hint` already has:
    /// the caller sees the pool as it was a moment earlier, never as something it
    /// never was.
    pub(super) largest_hint: Vec<AtomicU64>,
    /// Advisory per-region LARGEST stripe-reserve run, same discipline.
    ///
    /// Largest-run rather than summed capacity because the question every reader
    /// asks is "can this region serve a `need`-block aligned carve", and for the
    /// reserve that is EXACTLY `largest >= need`: every reserve extent is
    /// stripe-aligned with a stripe-multiple count (the `insert_classified`
    /// invariant), so its effective capacity equals its count. A summed hint
    /// answers a different question and says yes to a region holding a hundred
    /// single stripes when the request needs two contiguous ones.
    pub(super) stripe_hint: Vec<AtomicU64>,
    /// Preferred owner of each region as `lane + 1` (0 = unclaimed).
    ///
    /// ZFS's metaslab insight is that the win comes from EXCLUSIVITY, not from
    /// contiguity: a lane that owns its region neither waits for nor is waited
    /// on by the other lanes. Purely advisory here — when no unclaimed region
    /// can serve a refill, lanes share rather than starve (which also covers
    /// `num_lanes > num_regions` on small devices).
    pub(super) owner: Vec<AtomicUsize>,
    /// A/B gate — see [`set_region_serialize`]. Taken outermost, once per
    /// acquisition group, so arming it can never deadlock.
    pub(super) gate: Mutex<()>,
}

impl RegionPools {
    pub(super) fn new(usable_blocks: u64, regions: usize) -> Self {
        // Geometry is configured after construction (`set_stripe_geometry`), so
        // plan against stripe=1 now; `set_geometry` re-plans and re-routes.
        let (region_blocks, count) =
            RegionLayout::plan(usable_blocks, regions, 1).unwrap_or((0, 1));
        Self {
            pools: (0..count).map(|_| Mutex::new(FreePools::new())).collect(),
            region_blocks: AtomicU64::new(region_blocks),
            region_base: AtomicU64::new(RESERVED_BLOCKS),
            free_hint: (0..count).map(|_| AtomicU64::new(0)).collect(),
            largest_hint: (0..count).map(|_| AtomicU64::new(0)).collect(),
            stripe_hint: (0..count).map(|_| AtomicU64::new(0)).collect(),
            owner: (0..count).map(|_| AtomicUsize::new(0)).collect(),
            gate: Mutex::new(()),
        }
    }

    pub(super) fn layout(&self) -> RegionLayout {
        RegionLayout {
            base: self.region_base.load(Ordering::Relaxed),
            blocks: self.region_blocks.load(Ordering::Relaxed),
            count: self.pools.len(),
        }
    }

    pub(super) fn count(&self) -> usize {
        self.pools.len()
    }

    /// The layout this pool WOULD use for `(stripe, phase)`.
    ///
    /// The region count is fixed at construction (it sizes the mutex vector), so
    /// re-planning only moves the boundaries: `region_base` becomes the first
    /// stripe-aligned PBA and `region_blocks` is rounded up to a stripe
    /// multiple. Both keep every boundary stripe-aligned, which is what stops a
    /// boundary from stranding a partial stripe window in the general pool.
    /// Rounding up can leave the top regions unused (`RegionLayout::of` clamps),
    /// which costs nothing but an idle mutex.
    pub(super) fn planned_layout(&self, stripe: u32, phase: u32) -> RegionLayout {
        let blocks = self.region_blocks.load(Ordering::Relaxed);
        if self.pools.len() <= 1 || blocks == 0 {
            return RegionLayout::single();
        }
        let stripe64 = u64::from(stripe.max(1));
        RegionLayout {
            base: SpaceAllocator::align_up_pba(RESERVED_BLOCKS, stripe64, u64::from(phase)),
            blocks: blocks.div_ceil(stripe64) * stripe64,
            count: self.pools.len(),
        }
    }

    /// Publish a re-planned layout. The caller MUST hold every region lock and
    /// must have emptied the regions first — an extent left behind under the old
    /// boundaries could otherwise end up in a region that does not own it,
    /// breaking the "a region only holds its own addresses" invariant every
    /// query depends on.
    pub(super) fn publish_layout(&self, layout: RegionLayout) {
        self.region_base.store(layout.base, Ordering::Relaxed);
        self.region_blocks.store(layout.blocks, Ordering::Relaxed);
    }

    /// Take the A/B gate when armed. Callers hold the returned guard for the
    /// whole critical section, which is what makes N region locks behave as one.
    pub(super) fn gate(&self) -> Option<std::sync::MutexGuard<'_, ()>> {
        REGION_SERIALIZE
            .load(Ordering::Relaxed)
            .then(|| self.gate.lock().unwrap())
    }
}

/// The regions spanned by one extent, locked in ascending index order.
///
/// Almost always exactly one region: extents on the hot paths are 1-6 blocks
/// against a ~76 K-block region. The multi-region case exists for correctness
/// (a free run released across a boundary, a rebuild, a grow) and is handled by
/// clipping the extent per region — never by widening what a region owns.
pub(super) struct SpanGuard<'a> {
    pub(super) layout: RegionLayout,
    pub(super) lo: usize,
    pub(super) guards: Vec<FreeLockGuard<'a>>,
    /// Declared last so it drops AFTER `guards` (Rust drops fields in
    /// declaration order), keeping the gate outermost.
    pub(super) _gate: Option<std::sync::MutexGuard<'a, ()>>,
}

impl SpanGuard<'_> {
    pub(super) fn region(&self, idx: usize) -> &FreePools {
        &self.guards[idx - self.lo]
    }

    /// Record how many extents this hold covered. Charged once per HOLD (not per
    /// region guard): the counter is per-site, and what the box read needs is
    /// "extents per hold", the divisor that converts a per-acquisition cost into
    /// a per-extent one.
    pub(super) fn charge_items(&self, n: u64) {
        if let Some(first) = self.guards.first() {
            first.charge_items(n);
        }
    }

    pub(super) fn region_mut(&mut self, idx: usize) -> &mut FreePools {
        &mut self.guards[idx - self.lo]
    }

    pub(super) fn spans_one_region(&self) -> bool {
        self.guards.len() == 1
    }

    /// Force every held region to republish its hints on release — see
    /// [`FreeLockGuard::mark_hints_stale`].
    pub(super) fn mark_hints_stale(&mut self) {
        for guard in &mut self.guards {
            guard.mark_hints_stale();
        }
    }

    /// The one region this span covers, or `None` when it straddles a boundary.
    pub(super) fn single_mut(&mut self) -> Option<&mut FreePools> {
        (self.guards.len() == 1).then(|| &mut *self.guards[0])
    }

    pub(super) fn geometry(&self) -> Option<(u32, u32)> {
        self.guards[0].geometry()
    }

    pub(super) fn empty_set_with_geometry(&self) -> FreeSet {
        self.guards[0].empty_set_with_geometry()
    }

    pub(super) fn overlapping_free(&self, extent: Extent) -> Option<Extent> {
        let (lo, hi) = self.layout.span(extent);
        (lo..=hi).find_map(|idx| {
            let part = self.layout.clip(idx, extent)?;
            self.region(idx).overlapping_free(part)
        })
    }

    /// Whether the union of every spanned region's pools covers `extent`. Each
    /// region answers for its own slice; a region that owns none of the extent
    /// cannot withhold coverage.
    pub(super) fn covers_free(&self, extent: Extent) -> bool {
        let (lo, hi) = self.layout.span(extent);
        (lo..=hi).all(|idx| match self.layout.clip(idx, extent) {
            Some(part) => self.region(idx).covers_free(part),
            None => true,
        })
    }

    pub(super) fn overlaps_reserve(&self, extent: Extent) -> bool {
        let (lo, hi) = self.layout.span(extent);
        (lo..=hi).any(|idx| match self.layout.clip(idx, extent) {
            Some(part) => self.region(idx).overlaps_reserve(part),
            None => false,
        })
    }

    pub(super) fn overlapping_quarantine(&self, extent: Extent) -> Option<Extent> {
        let (lo, hi) = self.layout.span(extent);
        (lo..=hi).find_map(|idx| {
            let part = self.layout.clip(idx, extent)?;
            self.region(idx).overlapping_quarantine(part)
        })
    }

    pub(super) fn release_extent(&mut self, extent: Extent) {
        let (lo, hi) = self.layout.span(extent);
        for idx in lo..=hi {
            if let Some(part) = self.layout.clip(idx, extent) {
                self.region_mut(idx).release_extent(part);
            }
        }
    }

    pub(super) fn insert_classified(&mut self, extent: Extent) {
        let (lo, hi) = self.layout.span(extent);
        for idx in lo..=hi {
            if let Some(part) = self.layout.clip(idx, extent) {
                self.region_mut(idx).insert_classified(part);
            }
        }
    }

    pub(super) fn extract_from_general(&mut self, range: Extent) -> Vec<Extent> {
        let (lo, hi) = self.layout.span(range);
        let mut out = Vec::new();
        for idx in lo..=hi {
            if let Some(part) = self.layout.clip(idx, range) {
                out.extend(self.region_mut(idx).extract_from_general(part));
            }
        }
        out
    }
}

/// Split a batch into maximal runs of consecutive extents that share the SAME
/// region span, capped at `cap` entries per run.
///
/// With one region this is exactly `extents.chunks(cap)`, so unsharded behaviour
/// — including where the [`FREE_LOCK_HOLD_EXTENTS`] boundaries fall — is
/// unchanged. Sharded, an address-sorted batch (which is what
/// `buffer/flush/cleanup.rs::retire_dead_pbas` and the GC reclaim loop both
/// produce) yields one hold per region, so the lane-snapshot amortization
/// survives while the hold itself moves off a lock the writers share. An
/// unsorted batch simply gets shorter holds — correct, just more acquisitions.
///
/// Grouping (rather than one hold per extent) is what keeps the documented
/// `free -> retired -> retired_age` order intact: the region lock stays
/// outermost for a whole group, so the free-side overlap check remains atomic
/// with the retired insert. Flipping to a per-extent region lock inside a held
/// `retired` would invert that order against `validate_free_extent`.
pub(super) fn region_holds<'e>(
    layout: RegionLayout,
    extents: &'e [Extent],
    cap: usize,
) -> Vec<(usize, usize, &'e [Extent])> {
    let cap = cap.max(1);
    let mut out = Vec::new();
    let mut i = 0;
    while i < extents.len() {
        let span = layout.span(extents[i]);
        let mut j = i + 1;
        while j < extents.len() && j - i < cap && layout.span(extents[j]) == span {
            j += 1;
        }
        out.push((span.0, span.1, &extents[i..j]));
        i = j;
    }
    out
}
