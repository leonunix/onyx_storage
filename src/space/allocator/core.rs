use super::*;

impl SpaceAllocator {
    /// Create a new allocator for a device of the given size.
    /// Blocks 0..RESERVED_BLOCKS are reserved for superblock/heartbeat/HA lock.
    /// Allocatable space starts at PBA RESERVED_BLOCKS.
    pub fn new(device_size_bytes: u64, num_lanes: usize) -> Self {
        Self::new_with_hazards(device_size_bytes, num_lanes)
    }

    /// Number of address regions the free space is sharded into (1 = off).
    pub fn region_count(&self) -> usize {
        self.regions.count()
    }

    /// Blocks per region (0 when unsharded).
    pub fn region_blocks(&self) -> u64 {
        self.regions.region_blocks.load(Ordering::Relaxed)
    }

    /// Region sharding shape + traffic — see [`AllocRegionStats`].
    pub fn region_stats(&self) -> AllocRegionStats {
        AllocRegionStats {
            regions: self.regions.count(),
            region_blocks: self.regions.region_blocks.load(Ordering::Relaxed),
            switches: self.region_switches.load(Ordering::Relaxed),
            refill_misses: self.region_refill_misses.load(Ordering::Relaxed),
            serialized: region_serialize(),
        }
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
        // Unsharded. NOT the production default (that is
        // `storage.allocator_regions`, which selects 2048 — see `StorageConfig`);
        // this is the direct-construction entry point used by the unit tests, and
        // keeping it single-lock is what lets a test compare the two arms and what
        // makes `ONYX_ALLOCATOR_REGIONS=<n> cargo test` a meaningful sweep.
        Self::new_with_regions(device_size_bytes, num_lanes, 1)
    }

    /// `new_with_hazards` with an explicit region count (see [`RegionPools`]).
    /// `0` selects the compiled default, `1` disables sharding. The device may
    /// still end up unsharded when it is too small (see [`MIN_REGION_BLOCKS`]).
    pub fn new_with_regions(device_size_bytes: u64, num_lanes: usize, regions: usize) -> Self {
        // Diagnostic override, same shape as `ONYX_ALLOC_TRACK`: it exists so the
        // WHOLE suite can be re-run against the sharded paths
        // (`ONYX_ALLOCATOR_REGIONS=8 cargo test`) instead of only the dedicated
        // region tests, which is the only way to find a routing mistake in a
        // consumer nobody thought to region-test. It overrides the config, so
        // production must not set it.
        let regions = std::env::var("ONYX_ALLOCATOR_REGIONS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(regions);
        Self::new_with_exact_regions(device_size_bytes, num_lanes, regions)
    }

    /// [`Self::new_with_regions`] without the `ONYX_ALLOCATOR_REGIONS` override,
    /// so a test that compares a sharded pool against an unsharded one still
    /// gets both arms while the suite is being swept sharded.
    pub(super) fn new_with_exact_regions(
        device_size_bytes: u64,
        num_lanes: usize,
        regions: usize,
    ) -> Self {
        let total_blocks = device_size_bytes / BLOCK_SIZE as u64;
        let usable_blocks = total_blocks.saturating_sub(RESERVED_BLOCKS);
        let regions = if regions == 0 {
            DEFAULT_ALLOCATOR_REGIONS
        } else {
            regions
        };
        let region_pools = RegionPools::new(usable_blocks, regions);
        if usable_blocks > 0 {
            // Same total as the pre-region constructor (which clamped one
            // extent to u32::MAX), just routed to the owning regions.
            let layout = region_pools.layout();
            let seed = Extent::new(
                Pba(RESERVED_BLOCKS),
                usable_blocks.min(u32::MAX as u64) as u32,
            );
            let (lo, hi) = layout.span(seed);
            for idx in lo..=hi {
                if let Some(part) = layout.clip(idx, seed) {
                    let mut pools = region_pools.pools[idx].lock().unwrap();
                    pools.general.insert(part);
                    // Seed the advisory hints too: the ascending region walks
                    // skip regions whose hint reads zero, so a never-published
                    // hint would make a freshly-built allocator look empty.
                    region_pools.free_hint[idx]
                        .store(pools.free_blocks_in_pools(), Ordering::Relaxed);
                    region_pools.largest_hint[idx].store(
                        pools
                            .largest_allocatable()
                            .map_or(0, |(run, _)| u64::from(run.count)),
                        Ordering::Relaxed,
                    );
                }
            }
        }
        let lane_caches = (0..num_lanes).map(|_| Mutex::new(Vec::new())).collect();
        let lane_extent_caches = (0..num_lanes).map(|_| Mutex::new(Vec::new())).collect();
        let lane_cache_depth = (0..num_lanes).map(|_| AtomicU64::new(0)).collect();
        let lane_extent_cache_depth = (0..num_lanes).map(|_| AtomicU64::new(0)).collect();
        let lane_regions = (0..num_lanes)
            .map(|_| AtomicUsize::new(usize::MAX))
            .collect();
        let alloc_tracker = std::env::var("ONYX_ALLOC_TRACK")
            .map(|value| {
                matches!(
                    value.as_str(),
                    "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
                )
            })
            .unwrap_or(false)
            .then(|| Mutex::new(BTreeSet::new()));
        let retired = RetiredRegions::new(region_pools.count());
        Self {
            total_blocks: AtomicU64::new(total_blocks),
            regions: region_pools,
            retired,
            retired_blocks: AtomicU64::new(0),
            hazards: PbaHazards::new(),
            allocated_blocks: AtomicU64::new(0),
            free_blocks: AtomicU64::new(usable_blocks),
            free_lock: FreeLockStats::new(),
            retired_lock: RetiredLockStats::new(),
            lane_caches,
            lane_extent_caches,
            lane_cache_depth,
            lane_extent_cache_depth,
            lane_regions,
            alloc_tracker,
            aligned_allocs: AtomicU64::new(0),
            refill_ops: AtomicU64::new(0),
            refill_blocks: AtomicU64::new(0),
            refill_runs: AtomicU64::new(0),
            drain_ops: AtomicU64::new(0),
            drain_blocks: AtomicU64::new(0),
            drain_skips: AtomicU64::new(0),
            #[cfg(test)]
            drain_guard_off: AtomicBool::new(false),
            region_switches: AtomicU64::new(0),
            region_refill_misses: AtomicU64::new(0),
            stripe_refill_run_stripes: AtomicU64::new(0),
            refill_wide_hits: AtomicU64::new(0),
            refill_wide_misses: AtomicU64::new(0),
            geometry_cache: AtomicU64::new(0),
            stripe_refill_width_bias: AtomicBool::new(false),
            stripe_run_allocs: AtomicU64::new(0),
            stripe_run_stripes: AtomicU64::new(0),
            stripe_run_width_hist: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    /// Prefer wider reserve runs over lower-address ones when refilling a lane
    /// (design D2). See [`crate::config::StorageConfig::stripe_refill_width_bias`];
    /// the engine sets this once at open, next to
    /// [`Self::set_stripe_refill_run_stripes`].
    pub fn set_stripe_refill_width_bias(&self, enabled: bool) {
        self.stripe_refill_width_bias
            .store(enabled, Ordering::Relaxed);
    }

    /// Whether the reserve refill is currently width-biased.
    pub fn stripe_refill_width_bias(&self) -> bool {
        self.stripe_refill_width_bias.load(Ordering::Relaxed)
    }

    /// Set the aligned refill's preferred run width in whole stripes (`0` = off).
    /// See [`crate::config::StorageConfig::stripe_refill_run_stripes`]; the engine
    /// sets this once at open, next to [`Self::set_stripe_geometry`].
    pub fn set_stripe_refill_run_stripes(&self, stripes: u32) {
        self.stripe_refill_run_stripes
            .store(u64::from(stripes), Ordering::Relaxed);
    }

    /// The configured preferred run width in whole stripes (`0` = off).
    pub fn stripe_refill_run_stripes(&self) -> u32 {
        self.stripe_refill_run_stripes.load(Ordering::Relaxed) as u32
    }

    /// Block floor a refill prefers its reserve runs to meet, or `None` when the
    /// knob is off or the floor would not be wider than the request itself.
    ///
    /// Clamped to `max_count` (the refill's block budget): asking for a run wider
    /// than we are willing to take would reject runs that could serve the whole
    /// budget contiguously, which is the entire point.
    pub(super) fn wide_refill_floor(
        &self,
        min_count: u32,
        max_count: u32,
        stripe: u32,
    ) -> Option<u32> {
        let stripes = self.stripe_refill_run_stripes();
        if stripes == 0 {
            return None;
        }
        let floor = stripe.saturating_mul(stripes).min(max_count.max(min_count));
        (floor > min_count).then_some(floor)
    }

    /// Snapshot of the aligned path's lane-cache supply — see
    /// [`AllocSupplyStats`]. Lock-free; monotonic counters, so two reads
    /// difference cleanly.
    /// Acquire `free_pools`, charging the wait to `site` and (on drop) the hold.
    ///
    /// This is THE hot path of the whole allocator — ~10^4 acquisitions per
    /// unaligned allocation in the exhausted regime — so the accounting is
    /// deliberately minimal: one TLS read, one `fetch_add` on a per-thread cache
    /// line, and (on 1 acquisition in [`lock_stat_stride`]) two clock reads.
    pub(super) fn lock_region_raw(&self, site: FreeLockSite, region: usize) -> FreeLockGuard<'_> {
        let idx = site as usize;
        let (shard, queued) = self.free_lock.begin(idx);
        let pools = self.regions.pools[region].lock().unwrap();
        let acquired = queued.map(|queued| {
            let now = Instant::now();
            shard.charge_wait(idx, now.duration_since(queued).as_nanos() as u64);
            now
        });
        FreeLockGuard {
            pools,
            shard,
            site: idx,
            acquired,
            dirty: false,
            free_hint: &self.regions.free_hint[region],
            largest_hint: &self.regions.largest_hint[region],
            stripe_hint: &self.regions.stripe_hint[region],
        }
    }

    /// Lock ONE region (plus the A/B gate when armed). Callers that walk regions
    /// take these one at a time and never hold two, so the walk order is free.
    pub(super) fn lock_region(&self, site: FreeLockSite, region: usize) -> SpanGuard<'_> {
        let gate = self.regions.gate();
        SpanGuard {
            layout: self.regions.layout(),
            lo: region,
            guards: vec![self.lock_region_raw(site, region)],
            _gate: gate,
        }
    }

    /// Lock every region this extent reaches into, ASCENDING by index.
    ///
    /// Ascending order is the allocator-wide rule for holding more than one
    /// region, so multi-region holds can never deadlock against each other.
    /// The overwhelmingly common result is a single guard.
    pub(super) fn lock_span(&self, site: FreeLockSite, extent: Extent) -> SpanGuard<'_> {
        let layout = self.regions.layout();
        let (lo, hi) = layout.span(extent);
        let gate = self.regions.gate();
        SpanGuard {
            layout,
            lo,
            guards: (lo..=hi)
                .map(|idx| self.lock_region_raw(site, idx))
                .collect(),
            _gate: gate,
        }
    }

    /// Lock the inclusive region range `[lo, hi]`, ASCENDING — the batch-path
    /// analogue of [`Self::lock_span`], where the range comes from a group of
    /// extents rather than one.
    pub(super) fn lock_span_range(
        &self,
        site: FreeLockSite,
        lo: usize,
        hi: usize,
    ) -> SpanGuard<'_> {
        let gate = self.regions.gate();
        SpanGuard {
            layout: self.regions.layout(),
            lo,
            guards: (lo..=hi)
                .map(|idx| self.lock_region_raw(site, idx))
                .collect(),
            _gate: gate,
        }
    }

    /// Lock EVERY region ascending. Only for paths that must see the whole space
    /// atomically — today just `set_geometry`, which re-plans the region
    /// boundaries and therefore has to empty and re-route every region at once.
    ///
    /// ⚠ Nothing on an ALLOCATION path may use this. It is `regions` mutex
    /// acquisitions plus a `regions`-entry guard vector in one exclusive hold, and
    /// every version of "the allocator is slow" measured since 2026-08-12 has come
    /// back to some path doing per-region work per allocation. `drain_lane_caches`
    /// used to be here and now folds one region at a time.
    pub(super) fn lock_all_regions(&self, site: FreeLockSite) -> SpanGuard<'_> {
        let layout = self.regions.layout();
        let gate = self.regions.gate();
        SpanGuard {
            layout,
            lo: 0,
            guards: (0..layout.count)
                .map(|idx| self.lock_region_raw(site, idx))
                .collect(),
            _gate: gate,
        }
    }

    /// Regions in ascending address order, optionally skipping those the
    /// advisory hints report empty.
    ///
    /// Selection identity: regions partition the address space in ascending
    /// order, so "the first region that can serve the request" IS the global
    /// lowest-address answer — the same extent the single global pool's
    /// first-fit would have returned. Skipping hint-empty regions cannot change
    /// that (a region with no free blocks has no candidate).
    ///
    /// `min_hint = 0` forces the full walk. Every ENOSPC boundary uses it on its
    /// final attempt, so a hint that went stale exactly while another thread was
    /// freeing can never turn into a spurious `SpaceExhausted`. Against
    /// `stripe_hint` a `min_hint` of the request width is exact (largest-run
    /// semantics); against `free_hint` only `1` is meaningful, because a summed
    /// total says nothing about run widths.
    pub(super) fn walk_regions(
        &self,
        need_stripe: bool,
        min_hint: u64,
    ) -> impl Iterator<Item = usize> + '_ {
        let hints = if need_stripe {
            &self.regions.stripe_hint
        } else {
            &self.regions.free_hint
        };
        let count = self.regions.count();
        (0..count).filter(move |&idx| {
            min_hint == 0 || count == 1 || hints[idx].load(Ordering::Relaxed) >= min_hint
        })
    }

    /// Regions that could hold a CONTIGUOUS run of `min_width` blocks, ascending.
    ///
    /// This is the width-aware counterpart of [`Self::walk_regions`], and the
    /// difference is not cosmetic: `free_hint` is a SUM, so it says yes to a
    /// region holding a thousand single blocks when the caller needs six
    /// contiguous ones. On the box's exhausted pool (`largest_run = 5`) that made
    /// `refill_extent_lane` lock **1,989 regions per unaligned allocation**, every
    /// one of them futile, which is ~82% of that path's cost — the same
    /// lock-COUNT shape as the drain and the largest-pick scans.
    ///
    /// Filtering on `largest_hint` is EXACT rather than advisory (see
    /// [`RegionPools::largest_hint`]): it is derived from the same function the
    /// take uses and is never stale-LOW while no mutation is in flight. A
    /// mutation that has not published yet can still hide a region for a moment,
    /// so `filtered = false` gives the unfiltered walk every ENOSPC boundary ends
    /// with — the same discipline `walk_regions(_, 0)` already has.
    pub(super) fn walk_regions_wide(
        &self,
        min_width: u32,
        filtered: bool,
    ) -> impl Iterator<Item = usize> + '_ {
        let hints = &self.regions.largest_hint;
        let count = self.regions.count();
        let need = u64::from(min_width);
        (0..count).filter(move |&idx| {
            !filtered || count == 1 || hints[idx].load(Ordering::Relaxed) >= need
        })
    }

    /// Test-only direct handle on ONE region's pools, for the tests that inject
    /// a specific free-list shape or a deliberate free/retired inconsistency.
    /// Goes through the real guard so the region's advisory hints are refreshed
    /// on release exactly as a production mutation would refresh them.
    #[cfg(test)]
    pub(super) fn test_region_pools(&self, region: usize) -> FreeLockGuard<'_> {
        self.lock_region_raw(FreeLockSite::Setup, region)
    }

    /// Per-site `free_pools` wait/hold snapshot, one entry per
    /// [`FreeLockSite`] (including never-acquired sites, so the shape is stable
    /// across two reads for differencing).
    pub fn free_lock_stats(&self) -> Vec<LockSiteStats> {
        self.free_lock.snapshot(FreeLockSite::ALL.map(|s| s.name()))
    }

    /// Per-site `retired_extents` wait/hold snapshot — see [`RetiredLockSite`].
    /// Same shape guarantee as [`Self::free_lock_stats`].
    pub fn retired_lock_stats(&self) -> Vec<LockSiteStats> {
        self.retired_lock
            .snapshot(RetiredLockSite::ALL.map(|s| s.name()))
    }

    /// Acquire ONE retired shard, charging the wait to `site` and the hold on
    /// drop. Every acquisition in the file goes through here (or the span
    /// helpers below), so the table is exhaustive by construction.
    pub(super) fn lock_retired_shard(
        &self,
        site: RetiredLockSite,
        idx: usize,
    ) -> TimedGuard<'_, RetiredShard, RETIRED_LOCK_SITES> {
        TimedGuard::new(&self.retired_lock, site as usize, &self.retired.shards[idx])
    }

    /// The retired shards' layout — ALWAYS the free pool's, so that
    /// `{pools[i], retired[i]}` is exactly the atomic unit the retire path needs.
    pub(super) fn retired_layout(&self) -> RegionLayout {
        self.regions.layout()
    }

    /// Lock every retired shard `extent` reaches into, ASCENDING.
    pub(super) fn lock_retired_span(
        &self,
        site: RetiredLockSite,
        extent: Extent,
    ) -> RetiredSpan<'_> {
        let (lo, hi) = self.retired_layout().span(extent);
        self.lock_retired_span_range(site, lo, hi)
    }

    /// Lock EVERY retired shard, ASCENDING. Only for paths that must see the
    /// whole retired space atomically — today just the geometry re-shard.
    pub(super) fn lock_all_retired(
        &self,
        site: RetiredLockSite,
    ) -> Vec<TimedGuard<'_, RetiredShard, RETIRED_LOCK_SITES>> {
        (0..self.retired.count())
            .map(|idx| self.lock_retired_shard(site, idx))
            .collect()
    }

    /// Lock the inclusive shard range `[lo, hi]`, ASCENDING — the batch-path
    /// analogue of [`Self::lock_retired_span`], where the range comes from a
    /// group of extents rather than from one.
    pub(super) fn lock_retired_span_range(
        &self,
        site: RetiredLockSite,
        lo: usize,
        hi: usize,
    ) -> RetiredSpan<'_> {
        RetiredSpan {
            layout: self.retired_layout(),
            lo,
            guards: (lo..=hi)
                .map(|idx| self.lock_retired_shard(site, idx))
                .collect(),
        }
    }

    pub fn supply_stats(&self) -> AllocSupplyStats {
        AllocSupplyStats {
            aligned_allocs: self.aligned_allocs.load(Ordering::Relaxed),
            refills: self.refill_ops.load(Ordering::Relaxed),
            refill_blocks: self.refill_blocks.load(Ordering::Relaxed),
            refill_runs: self.refill_runs.load(Ordering::Relaxed),
            drains: self.drain_ops.load(Ordering::Relaxed),
            drain_blocks: self.drain_blocks.load(Ordering::Relaxed),
            drain_skips: self.drain_skips.load(Ordering::Relaxed),
            wide_hits: self.refill_wide_hits.load(Ordering::Relaxed),
            wide_misses: self.refill_wide_misses.load(Ordering::Relaxed),
            stripe_run_allocs: self.stripe_run_allocs.load(Ordering::Relaxed),
            stripe_run_stripes: self.stripe_run_stripes.load(Ordering::Relaxed),
            stripe_run_width_hist: std::array::from_fn(|i| {
                self.stripe_run_width_hist[i].load(Ordering::Relaxed)
            }),
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

        self.replace_general_regionwise(&free);
        for idx in 0..self.retired.count() {
            let mut shard = self.lock_retired_shard(RetiredLockSite::Setup, idx);
            shard.set.clear();
            shard.age.clear();
        }
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
            extents = self.pool_extent_count(),
            regions = self.regions.count(),
            "space allocator rebuilt from metadata"
        );

        Ok(())
    }

    /// Reset every region to hold exactly the free runs in `free`, clipped to the
    /// region that owns each piece. Regions are visited ascending and locked ONE
    /// AT A TIME: this is the startup/rebuild path, and `free` can hold tens of
    /// millions of extents, so holding all region locks across it would be a
    /// long global stall for no benefit (no allocator client is running yet).
    pub(super) fn replace_general_regionwise(&self, free: &BTreeSet<Extent>) {
        let layout = self.regions.layout();
        let mut runs = free.iter().peekable();
        for idx in 0..layout.count {
            let mut guard = self.lock_region(FreeLockSite::Setup, idx);
            let pools = guard.region_mut(idx);
            pools.replace_general(BTreeSet::new());
            let end = layout.end(idx);
            while let Some(&&run) = runs.peek() {
                if run.start.0 >= end {
                    break;
                }
                if let Some(part) = layout.clip(idx, run) {
                    pools.insert_classified(part);
                }
                if run.end_pba().0 <= end {
                    runs.next();
                } else {
                    // Straddles the boundary — the remainder belongs to the next
                    // region, so leave it in place for the next iteration.
                    break;
                }
            }
        }
    }

    /// Free extents across all regions (general + reserve). One region lock at a
    /// time — advisory aggregate, never a decision input.
    pub(super) fn pool_extent_count(&self) -> usize {
        (0..self.regions.count())
            .map(|idx| {
                let guard = self.lock_region(FreeLockSite::Audit, idx);
                let pools = guard.region(idx);
                pools.general.len() + pools.stripe_reserve.len()
            })
            .sum()
    }

    pub fn hazards(&self) -> PbaHazards {
        self.hazards.clone()
    }
}
