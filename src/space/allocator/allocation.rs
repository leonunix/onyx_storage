use super::*;

impl SpaceAllocator {
    /// Allocate a single block. Returns PBA.
    pub fn allocate_one(&self) -> OnyxResult<Pba> {
        if let Some(pba) = self.take_first_regionwise(FreeLockSite::SmallAlloc, 1, true) {
            self.track_alloc(Extent::single(pba.start), "allocate_one")?;
            self.allocated_blocks.fetch_add(1, Ordering::Relaxed);
            self.free_blocks.fetch_sub(1, Ordering::Relaxed);
            return Ok(pba.start);
        }
        // Global pool empty — drain lane caches and retry. The retry walks EVERY
        // region (no hint skipping) so this ENOSPC verdict never rests on a
        // stale advisory hint.
        self.drain_lane_caches_if_populated();
        if let Some(pba) = self.take_first_regionwise(FreeLockSite::SmallAlloc, 1, false) {
            self.track_alloc(Extent::single(pba.start), "allocate_one_retry")?;
            self.allocated_blocks.fetch_add(1, Ordering::Relaxed);
            self.free_blocks.fetch_sub(1, Ordering::Relaxed);
            return Ok(pba.start);
        }
        Err(OnyxError::SpaceExhausted)
    }

    /// Lowest-address free extent anywhere, capped at `max_count`.
    ///
    /// Small allocations preserve allocator-wide first-fit-by-address across both
    /// policy pools AND across regions — regions are address-ordered and
    /// disjoint, so the first region that has anything holds the global argmin.
    /// This ordering is a MetaDB L2P codec correctness contract; the reserve
    /// controls aligned ownership, never address order.
    pub(super) fn take_first_regionwise(
        &self,
        site: FreeLockSite,
        max_count: u32,
        skip_empty: bool,
    ) -> Option<Extent> {
        for idx in self.walk_regions(false, u64::from(skip_empty)) {
            let mut guard = self.lock_region(site, idx);
            if let Some(extent) = Self::take_first_from_pools(guard.region_mut(idx), max_count) {
                return Some(extent);
            }
        }
        None
    }

    /// Lowest-address free extent that can serve `min_count`, capped at
    /// `max_count`. Same ascending-region argument as
    /// [`Self::take_first_regionwise`].
    ///
    /// `skip_empty` selects the width-aware walk: a region whose largest run is
    /// under `min_count` cannot serve this request, so locking it is pure cost.
    /// `false` is the unfiltered ENOSPC pass — see [`Self::walk_regions_wide`].
    pub(super) fn take_exact_regionwise(
        &self,
        site: FreeLockSite,
        min_count: u32,
        max_count: u32,
        skip_empty: bool,
    ) -> Option<Extent> {
        for idx in self.walk_regions_wide(min_count, skip_empty) {
            let mut guard = self.lock_region(site, idx);
            if let Some(extent) =
                Self::take_exact_from_pools(guard.region_mut(idx), min_count, max_count)
            {
                return Some(extent);
            }
        }
        None
    }

    /// Largest free extent anywhere — the `allocate_extent` short-fragment
    /// fallback.
    ///
    /// Selection is an argmax over [`RegionPools::largest_hint`], which is derived
    /// from the same [`FreePools::largest_allocatable`] the take uses, so the
    /// answer is the same one a full scan would give — for `count` relaxed loads
    /// instead of `count` MUTEX ACQUISITIONS. That matters because this runs on
    /// the unaligned writer path, is reached once per `allocate_extent` miss, and
    /// used to lock EVERY region just to read a `largest()` it then threw away
    /// (and re-scanned from scratch on each retry, filtering through an O(n²)
    /// `exhausted` vector).
    ///
    /// Only the winner is locked. A concurrent allocation can empty it between the
    /// read and the lock; the take then comes back `None`, its release republishes
    /// the truth (`mark_hints_stale` forces that even though nothing was
    /// mutated), so the next argmax cannot pick it again and the retry makes
    /// progress. Bounded by the region count, and an eventual `None` is a
    /// truthful "nothing left".
    ///
    /// Ties now go to the LOWEST region index, where the full scan's
    /// `(count, start, idx)` key gave them to the highest address. Width is this
    /// API's whole contract (address order is
    /// [`Self::take_first_regionwise`]'s), and low-address-first is the density
    /// direction the L2P leaf codec wants anyway.
    pub(super) fn take_largest_regionwise(&self, site: FreeLockSite) -> Option<Extent> {
        for _ in 0..=self.regions.count() {
            let mut best: Option<(u64, usize)> = None;
            for (idx, hint) in self.regions.largest_hint.iter().enumerate() {
                let width = hint.load(Ordering::Relaxed);
                if width > 0 && best.is_none_or(|(current, _)| width > current) {
                    best = Some((width, idx));
                }
            }
            let (_, idx) = best?;
            let mut guard = self.lock_region(site, idx);
            if let Some(extent) = Self::take_largest_from_pools(guard.region_mut(idx)) {
                return Some(extent);
            }
            // Stale-high hint: the region emptied since the load. Nothing was
            // mutated, so force the republish the `dirty` flag would otherwise
            // skip, or the next argmax picks the same region again.
            guard.mark_hints_stale();
            drop(guard);
        }
        None
    }

    /// The pre-2026-08-13 implementation of [`Self::take_largest_regionwise`]:
    /// lock EVERY non-empty region to read a `largest()` it then discards, argmax,
    /// re-take under the winner's lock, and remember the losers in an O(n²)
    /// `exhausted` vector.
    ///
    /// Kept because it is the oracle: `hint_argmax_matches_the_full_scan` asserts
    /// the two pick the same extent over randomised pool shapes, and
    /// `bench_largest_scan_vs_hint` prices them against each other in one process.
    #[cfg(test)]
    pub(super) fn take_largest_regionwise_scanning(&self, site: FreeLockSite) -> Option<Extent> {
        let mut exhausted: Vec<usize> = Vec::new();
        for _ in 0..=self.regions.count() {
            let mut best: Option<(u32, u64, usize)> = None;
            for idx in self.walk_regions(false, 1) {
                if exhausted.contains(&idx) {
                    continue;
                }
                let guard = self.lock_region(site, idx);
                if let Some((candidate, _)) = guard.region(idx).largest_allocatable() {
                    let key = (candidate.count, candidate.start.0, idx);
                    if best.is_none_or(|current| key > current) {
                        best = Some(key);
                    }
                }
            }
            let (_, _, idx) = best?;
            let mut guard = self.lock_region(site, idx);
            if let Some(extent) = Self::take_largest_from_pools(guard.region_mut(idx)) {
                return Some(extent);
            }
            drop(guard);
            exhausted.push(idx);
        }
        None
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
                self.publish_lane_depth(lane, &cache);
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
                self.drain_lane_caches_if_populated();
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

    /// [`Self::drain_lane_caches`], but only when the lanes actually hold
    /// something. Returns whether the drain ran, so a caller can skip the retry
    /// that only makes sense after blocks came back.
    ///
    /// EVERY allocation-path drain goes through this. The drain is the single
    /// most expensive operation in the allocator (every region lock in one hold),
    /// it is reached only at ENOSPC boundaries, and at those boundaries the lanes
    /// are usually empty — which is *why* the allocation is failing. Before this
    /// guard, five of the seven call sites ran it unconditionally and one
    /// unaligned allocation could pay for it three times over: reserve-miss,
    /// refill-miss, then `allocate_extent`'s two retries.
    ///
    /// See [`Self::lane_cached_blocks`] for why the lock-free check is sound.
    pub(super) fn drain_lane_caches_if_populated(&self) -> bool {
        #[cfg(test)]
        if self.drain_guard_off.load(Ordering::Relaxed) {
            // `bench_empty_drain_guard`'s pre-fix arm.
            self.drain_lane_caches();
            return true;
        }
        if !self.has_lane_cached_blocks() {
            self.drain_skips.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        self.drain_lane_caches();
        true
    }

    /// Return all cached blocks from the lane caches to the global free list.
    /// Also called at shutdown, to prevent block leaks.
    ///
    /// Locks ONE REGION AT A TIME, not all of them. The old all-regions hold was
    /// there because "which regions the cached extents land in is only known after
    /// the lane locks are taken, so the region side has to be acquired first and
    /// in full" — true, but a lane refills from one region at a time
    /// (`lane_regions`), so its cache almost always sits in a single region, and
    /// the set can simply be PEEKED first (lane lock only, released before any
    /// region lock is taken, which is the `FreePools -> lane cache` order every
    /// allocation path already follows).
    ///
    /// This matters because the drain became the allocator's biggest region-lock
    /// consumer once the futile walks were gone: on the 2026-08-14 box it ran
    /// 5,768 times in 493 s, and each run took 2048 region locks in one exclusive
    /// hold **to recover 5.1 blocks**.
    ///
    /// ⚠ The move out of a cache and into a pool MUST happen with the owning
    /// region locked and the lane locked, in that order. Popping first and
    /// releasing afterwards would leave the blocks in neither place for a moment,
    /// and a defrag quarantine publishing in that window would miss them —
    /// exactly the "quarantine cannot miss an in-flight refill" invariant the
    /// refill paths are built around, and the class of bug that produced the
    /// stripe-publish CRC P0.
    pub fn drain_lane_caches(&self) {
        let mut drained: u64 = 0;
        for lane in 0..self.lane_caches.len() {
            drained += self.drain_lane_pbas(lane);
            drained += self.drain_lane_extents(lane);
        }
        self.drain_ops.fetch_add(1, Ordering::Relaxed);
        self.drain_blocks.fetch_add(drained, Ordering::Relaxed);
        // No counter adjustment needed: cached blocks were never counted as allocated
    }

    /// The pre-2026-08-14 drain: every region lock in ONE hold, across every lane
    /// lock. Kept as the differential oracle (`per_lane_drain_matches_the_all_regions_drain`)
    /// and the pre-fix arm of `bench_per_lane_drain`.
    #[cfg(test)]
    pub(super) fn drain_lane_caches_all_regions(&self) {
        let mut drained: u64 = 0;
        let mut pools = self.lock_all_regions(FreeLockSite::Drain);
        for (lane, cache_mutex) in self.lane_caches.iter().enumerate() {
            let mut cache = cache_mutex.lock().unwrap();
            for pba in cache.drain(..) {
                pools.release_extent(Extent::single(pba));
                drained += 1;
            }
            self.publish_lane_depth(lane, &cache);
        }
        for (lane, cache_mutex) in self.lane_extent_caches.iter().enumerate() {
            let mut cache = cache_mutex.lock().unwrap();
            for extent in cache.drain(..) {
                pools.release_extent(extent);
                drained += u64::from(extent.count);
            }
            self.publish_lane_extent_depth(lane, &cache);
        }
        drop(pools);
        self.drain_ops.fetch_add(1, Ordering::Relaxed);
        self.drain_blocks.fetch_add(drained, Ordering::Relaxed);
    }

    /// Fold one lane's single-block cache back, one region at a time.
    pub(super) fn drain_lane_pbas(&self, lane: usize) -> u64 {
        let mut drained = 0;
        for _ in 0..LANE_DRAIN_ROUNDS {
            // PEEK: which regions does this lane hold blocks in? The lane lock is
            // released before any region lock is taken.
            let mut regions: Vec<usize> = {
                let cache = self.lane_caches[lane].lock().unwrap();
                if cache.is_empty() {
                    return drained;
                }
                // Both sides of this are read under the lane lock, so it is exact
                // — and it is the property `drain_lane_caches_if_populated` bets
                // on when it skips a drain.
                debug_assert_eq!(
                    self.lane_cache_depth[lane].load(Ordering::Relaxed),
                    cache.len() as u64,
                    "lane {lane} pba depth disagrees with its cache"
                );
                let layout = self.regions.layout();
                cache.iter().map(|pba| layout.of(pba.0)).collect()
            };
            regions.sort_unstable();
            regions.dedup();
            for region in regions {
                let mut pools = self.lock_region(FreeLockSite::Drain, region);
                let mut cache = self.lane_caches[lane].lock().unwrap();
                let layout = pools.layout;
                let mut kept = Vec::with_capacity(cache.len());
                for pba in cache.drain(..) {
                    if layout.of(pba.0) == region {
                        pools.release_extent(Extent::single(pba));
                        drained += 1;
                    } else {
                        kept.push(pba);
                    }
                }
                *cache = kept;
                self.publish_lane_depth(lane, &cache);
            }
        }
        drained
    }

    /// Fold one lane's extent cache back, one region span at a time. An extent
    /// that straddles a boundary needs both regions, which is what
    /// [`Self::lock_span_range`] gives — ascending, so it cannot deadlock against
    /// another multi-region hold.
    pub(super) fn drain_lane_extents(&self, lane: usize) -> u64 {
        let mut drained = 0;
        for _ in 0..LANE_DRAIN_ROUNDS {
            let mut spans: Vec<(usize, usize)> = {
                let cache = self.lane_extent_caches[lane].lock().unwrap();
                if cache.is_empty() {
                    return drained;
                }
                debug_assert_eq!(
                    self.lane_extent_cache_depth[lane].load(Ordering::Relaxed),
                    cache.iter().map(|e| u64::from(e.count)).sum::<u64>(),
                    "lane {lane} extent depth disagrees with its cache"
                );
                let layout = self.regions.layout();
                cache.iter().map(|extent| layout.span(*extent)).collect()
            };
            spans.sort_unstable();
            spans.dedup();
            for (lo, hi) in spans {
                let mut pools = self.lock_span_range(FreeLockSite::Drain, lo, hi);
                let mut cache = self.lane_extent_caches[lane].lock().unwrap();
                let layout = pools.layout;
                // Rebuilt through `push_extent_cache` so the descending-by-start
                // invariant survives, exactly as `extract_lane_cache_free_parts`
                // has to do.
                let mut kept: Vec<Extent> = Vec::with_capacity(cache.len());
                for extent in cache.drain(..) {
                    let (elo, ehi) = layout.span(extent);
                    if elo >= lo && ehi <= hi {
                        pools.release_extent(extent);
                        drained += u64::from(extent.count);
                    } else {
                        Self::push_extent_cache(&mut kept, extent);
                    }
                }
                *cache = kept;
                self.publish_lane_extent_depth(lane, &cache);
            }
        }
        drained
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
        let extent = Extent::single(pba);
        let span = self.lock_span(FreeLockSite::Audit, extent);
        if span.overlapping_free(extent).is_some() {
            return true;
        }
        drop(span);
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
                if let Some(extent) =
                    self.take_exact_regionwise(FreeLockSite::SmallAlloc, count, count, attempt == 0)
                {
                    self.track_alloc(extent, "allocate_exact_extent_global")?;
                    self.allocated_blocks
                        .fetch_add(count as u64, Ordering::Relaxed);
                    self.free_blocks.fetch_sub(count as u64, Ordering::Relaxed);
                    return Ok(extent);
                }
                if attempt == 0 {
                    self.drain_lane_caches_if_populated();
                    continue;
                }
                break;
            }
            return Err(OnyxError::SpaceExhausted);
        }

        {
            let mut cache = self.lane_extent_caches[lane].lock().unwrap();
            if let Some(extent) = Self::take_from_extent_cache(&mut cache, count) {
                self.publish_lane_extent_depth(lane, &cache);
                self.track_alloc(extent, "allocate_extent_for_lane_cache")?;
                self.allocated_blocks
                    .fetch_add(count as u64, Ordering::Relaxed);
                self.free_blocks.fetch_sub(count as u64, Ordering::Relaxed);
                return Ok(extent);
            }
        }

        let target = LANE_EXTENT_CACHE_REFILL_BLOCKS.max(count);
        let mut result = self.refill_extent_lane(lane, count, target, true);
        if result.is_none() {
            // A global fragment can become usable only after it coalesces with
            // short pieces held by one or more lanes. Exact allocation is the
            // ENOSPC boundary, so pay for one bounded drain and retry here —
            // unless the lanes hold nothing, in which case the coalesce this is
            // hoping for cannot exist.
            if self.drain_lane_caches_if_populated() {
                // Unfiltered: a drain republished every region it touched, and
                // drains are rare now, so the one full walk is cheap insurance
                // against an ENOSPC verdict that rests on a hint.
                result = self.refill_extent_lane(lane, count, target, false);
            }
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
                self.publish_lane_extent_depth(lane, &cache);
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
            //
            // This is the hottest of the drain sites — on the box the exhausted
            // regime took this branch 3.37 M times at 5.34 ms each — and also the
            // one where the drain is most often pointless: a reserve miss under a
            // starved pool means the lanes are empty too.
            if self.drain_lane_caches_if_populated() {
                extent = self.refill_stripe_extent_lane(lane, need, want, stripe_blocks, phase);
            }
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
    pub(super) fn round_up_blocks(data: u32, stripe: u32) -> u32 {
        if stripe <= 1 {
            return data;
        }
        data.div_ceil(stripe) * stripe
    }

    /// Smallest PBA `>= from` with `(pba + phase) % stripe == 0`.
    pub(super) fn align_up_pba(from: u64, stripe: u64, phase: u64) -> u64 {
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
    pub(super) fn carve_aligned_from_run(
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
    pub(super) fn push_extent_cache(cache: &mut Vec<Extent>, extent: Extent) {
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
    pub(super) fn take_aligned_from_extent_cache(
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
    pub(super) fn allocate_stripe_extent_global(
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
            if let Some(result) =
                self.take_exact_regionwise(FreeLockSite::SmallAlloc, count, count, attempt == 0)
            {
                self.track_alloc(result, "allocate_extent")?;
                self.allocated_blocks
                    .fetch_add(count as u64, Ordering::Relaxed);
                self.free_blocks.fetch_sub(count as u64, Ordering::Relaxed);
                return Ok(result);
            }

            // No contiguous extent large enough. Cached lane extents may hold
            // enough free contiguous space, so fold them back once before
            // falling back to the largest global fragment.
            if attempt == 0 && self.drain_lane_caches_if_populated() {
                continue;
            }

            // No contiguous extent large enough — return the largest available
            if let Some(extent) = self.take_largest_regionwise(FreeLockSite::SmallAlloc) {
                self.track_alloc(extent, "allocate_extent_largest")?;
                self.allocated_blocks
                    .fetch_add(extent.count as u64, Ordering::Relaxed);
                self.free_blocks
                    .fetch_sub(extent.count as u64, Ordering::Relaxed);
                return Ok(extent);
            }

            // No free extents at all — drain lane caches and retry once
            if attempt == 0 && self.drain_lane_caches_if_populated() {
                continue;
            }
            break;
        }
        Err(OnyxError::SpaceExhausted)
    }

    pub(super) fn clear_lane_caches(&self) {
        for (lane, cache) in self.lane_caches.iter().enumerate() {
            let mut cache = cache.lock().unwrap();
            cache.clear();
            self.publish_lane_depth(lane, &cache);
        }
        for (lane, cache) in self.lane_extent_caches.iter().enumerate() {
            let mut cache = cache.lock().unwrap();
            cache.clear();
            self.publish_lane_extent_depth(lane, &cache);
        }
    }

    pub(super) fn ensure_not_free_or_retired_after_wait(
        &self,
        extent: Extent,
        pools: &SpanGuard<'_>,
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

    /// Publish `lane`'s single-block cache depth. Every entry is one block, so
    /// this is O(1). MUST be called under that lane's cache lock, by every path
    /// that changes the cache — the value is DERIVED from the cache rather than
    /// accumulated as a delta, so a site that forgets to publish can only be
    /// stale for one lane until its next mutation, and can never drift or go
    /// negative.
    pub(super) fn publish_lane_depth(&self, lane: usize, cache: &[Pba]) {
        self.lane_cache_depth[lane].store(cache.len() as u64, Ordering::Relaxed);
    }

    /// Publish `lane`'s extent cache depth in BLOCKS. Same discipline as
    /// [`Self::publish_lane_depth`]; the sum is over the handful of runs one
    /// refill parks (bounded by [`LANE_EXTENT_CACHE_REFILL_RUNS`]).
    pub(super) fn publish_lane_extent_depth(&self, lane: usize, cache: &[Extent]) {
        let blocks = cache.iter().map(|e| u64::from(e.count)).sum();
        self.lane_extent_cache_depth[lane].store(blocks, Ordering::Relaxed);
    }

    /// Park `extent` in a lane's extent cache the way a refill would — including
    /// the depth publish, without which the allocation paths would (correctly)
    /// treat the lane as empty and skip the drain that folds it back.
    #[cfg(test)]
    pub(super) fn seed_lane_extent_cache(&self, lane: usize, extent: Extent) {
        let mut cache = self.lane_extent_caches[lane].lock().unwrap();
        Self::push_extent_cache(&mut cache, extent);
        self.publish_lane_extent_depth(lane, &cache);
    }

    /// Blocks a [`Self::drain_lane_caches`] would hand back, from `2 * lanes`
    /// relaxed loads instead of `2 * lanes` MUTEX acquisitions.
    ///
    /// This is what lets the ENOSPC paths skip a drain that is provably a no-op.
    /// The drain is the most expensive operation in the allocator — it takes
    /// EVERY region lock (2048 by default) in one hold plus a 2048-entry guard
    /// vector — and in the exhausted regime it is also the most useless: the
    /// lanes are empty precisely because allocation is failing, yet five of its
    /// seven call sites used to run it unconditionally, several of them twice
    /// within one allocation.
    ///
    /// **Why a lock-free read is sound.** Blocks only enter a lane cache from a
    /// refill, and every refill publishes into the cache while holding a REGION
    /// lock; `drain_lane_caches` holds EVERY region lock for its whole duration.
    /// So no push can be in flight while a drain runs, and a reader that sees 0
    /// cannot be missing blocks that a drain would have recovered. Blocks that
    /// appear after the read came from a refill that took them out of the same
    /// pool the reader had just found empty — the identical race the
    /// unconditional drain already had, since it too would have run either
    /// before or after that refill's lock hold. Pops need no region lock, but a
    /// pop only lowers the truth, so a stale-high read costs one wasted drain
    /// (i.e. exactly the old behaviour) and never a wrong answer.
    pub(super) fn lane_cached_blocks(&self) -> u64 {
        self.lane_cache_depth
            .iter()
            .chain(self.lane_extent_cache_depth.iter())
            .map(|depth| depth.load(Ordering::Relaxed))
            .sum()
    }

    pub(super) fn has_lane_cached_blocks(&self) -> bool {
        self.lane_cached_blocks() > 0
    }

    /// Transfer a global refill into a single-block lane cache atomically with
    /// respect to defrag quarantine publication. The returned first block is no
    /// longer free; every remaining block is visible in the lane cache before
    /// `FreePools` is unlocked.
    pub(super) fn refill_one_lane_from_global(&self, lane: usize, max_count: u32) -> Option<Pba> {
        // The refill's removal and the lane-tail publication must share ONE
        // critical section so a defrag quarantine cannot miss an in-flight
        // refill. Sharded, that section is the region the refill came from —
        // which is also the only region whose blocks are being published.
        for idx in self.walk_regions(false, 1) {
            let mut guard = self.lock_region(FreeLockSite::SmallAlloc, idx);
            let Some(refill) = Self::take_first_from_pools(guard.region_mut(idx), max_count) else {
                continue;
            };
            if refill.count > 1 {
                let mut cache = self.lane_caches[lane].lock().unwrap();
                for i in 1..refill.count {
                    cache.push(Pba(refill.start.0 + i as u64));
                }
                self.publish_lane_depth(lane, &cache);
            }
            return Some(refill.start);
        }
        None
    }

    /// Take exactly `count` blocks for the caller and publish the rest of the
    /// refill into its extent cache before releasing `FreePools`.
    ///
    /// `filtered` walks only regions whose largest run can host `count` — the
    /// unaligned writer path's hot loop, and on an exhausted pool the difference
    /// between 1,989 futile region locks per allocation and none (see
    /// [`Self::walk_regions_wide`]). The caller's post-drain retry passes `false`
    /// so the ENOSPC verdict never rests on a hint.
    pub(super) fn refill_extent_lane(
        &self,
        lane: usize,
        count: u32,
        max_count: u32,
        filtered: bool,
    ) -> Option<Extent> {
        for idx in self.walk_regions_wide(count, filtered) {
            let mut guard = self.lock_region(FreeLockSite::WriterUnaligned, idx);
            let Some(refill) = Self::take_exact_from_pools(guard.region_mut(idx), count, max_count)
            else {
                continue;
            };
            let result = Extent::new(refill.start, count);
            if refill.count > count {
                let mut cache = self.lane_extent_caches[lane].lock().unwrap();
                Self::push_extent_cache(
                    &mut cache,
                    Extent::new(Pba(refill.start.0 + count as u64), refill.count - count),
                );
                self.publish_lane_extent_depth(lane, &cache);
            }
            return Some(result);
        }
        None
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
    /// Sharded, a lane refills from ONE region at a time — its "active" region —
    /// and only moves when that region cannot serve it.
    ///
    /// ⚠ This is the one DELIBERATE selection change region sharding makes:
    /// aligned allocation is first-fit-by-address **within the lane's region**
    /// instead of globally. It is safe for the metadb L2P leaf codec because the
    /// codec's real requirement is that ONE LEAF's PBAs stay near each other,
    /// and routing already guarantees leaf ⊂ zone ⊂ shard ⊂ lane
    /// (`shard_for_lba` divides by zone before the modulo), so a leaf's blocks
    /// all come from one lane — hence one region — and `push_extent_cache` hands
    /// them out in ascending order. Leaf v5's `MAX_UNITS_PER_LEAF = 128 =
    /// LEAF_ENTRY_COUNT` makes the historical unit-dict overflow structurally
    /// impossible; the only surviving constraint is a 4 G-block (16 TiB) PBA span
    /// per leaf, and region selection prefers LOW addresses precisely to keep the
    /// working set clustered. `region_pools_equal_single_pool` pins that the free
    /// COVERAGE is identical to the unsharded pool; the emission ORDER is what
    /// changes.
    ///
    /// ## Wide-run preference (`storage.stripe_refill_run_stripes`)
    ///
    /// At a one-stripe floor, `first_fit(min_count)` degenerates into "take the
    /// lowest-address run", and on an aged pool the lowest addresses are windows
    /// pinned by a single live block — 6.15 blocks/run box-measured, i.e. 64
    /// isolated 24 KiB windows per refill, so the writer's consecutive stripes
    /// land at unrelated PBAs and chunklet's adjacency merge collapses to 1.02x
    /// ([[submit_io_is_a_563_way_4k_fanout]]). When the knob is on, a first pass
    /// only considers runs at least `floor` blocks wide (and only regions whose
    /// `stripe_hint` says they have one), which routes the lane to intact
    /// material and typically parks ONE budget-sized run in the lane cache, so
    /// every subsequent carve is adjacent to the last.
    ///
    /// The pass is a pure preference: on a miss the second pass is the legacy
    /// one-stripe-floor refill, unchanged, and the caller's drain + global
    /// fallback (hence the ENOSPC boundary) is untouched. Selection stays
    /// first-fit-BY-ADDRESS in both passes — the floor changes the candidate set,
    /// never the ordering, so this is not the best-fit policy that once corrupted
    /// the metadb L2P leaf.
    pub(super) fn refill_stripe_extent_lane(
        &self,
        lane: usize,
        min_count: u32,
        max_count: u32,
        stripe: u32,
        phase: u32,
    ) -> Option<Extent> {
        let legacy = StripeRefill {
            min_count,
            max_count,
            stripe,
            phase,
            floor: min_count,
        };
        if let Some(floor) = self.wide_refill_floor(min_count, max_count, stripe) {
            if let Some(extent) = self.refill_stripe_floored(lane, StripeRefill { floor, ..legacy })
            {
                self.refill_wide_hits.fetch_add(1, Ordering::Relaxed);
                return Some(extent);
            }
            self.refill_wide_misses.fetch_add(1, Ordering::Relaxed);
        }
        self.refill_stripe_floored(lane, legacy)
    }

    /// [`Self::refill_stripe_extent_lane`] restricted to reserve runs of at least
    /// `req.floor` blocks. `floor == min_count` is the legacy behaviour.
    pub(super) fn refill_stripe_floored(&self, lane: usize, req: StripeRefill) -> Option<Extent> {
        let layout = self.regions.layout();
        if !layout.sharded() {
            return self.refill_stripe_from_region(lane, 0, req);
        }
        // A wide pass must not fall back to "try the current region anyway": that
        // fallback exists to reach the caller's ENOSPC boundary, which the second
        // pass reaches on its own. Probing a region the hints say cannot serve
        // the floor would just cost a lock hold per refill on a pool with no wide
        // material left.
        let mut idx = if req.floor > req.min_count {
            self.wide_refill_region(lane, u64::from(req.floor), layout)?
        } else {
            self.lane_region(lane, u64::from(req.min_count), layout)
        };
        for attempt in 0..REGION_REFILL_TRIES {
            if let Some(extent) = self.refill_stripe_from_region(lane, idx, req) {
                return Some(extent);
            }
            // No need to remember which regions were tried: the failed attempt
            // just released that region's lock, and the guard's drop refreshed
            // its `stripe_hint` with the truth, so `switch_lane_region` cannot
            // pick it again for a width it cannot serve. A retry loop is only
            // reachable while another thread is consuming the same regions.
            self.region_refill_misses.fetch_add(1, Ordering::Relaxed);
            if attempt + 1 == REGION_REFILL_TRIES {
                break;
            }
            idx = self.switch_lane_region(lane, idx, u64::from(req.floor), layout)?;
        }
        None
    }

    /// [`Self::lane_region`] without the "nothing qualifies → try the current
    /// region anyway" fallback: `None` means no region's `stripe_hint` claims a
    /// run of `need` blocks, so there is nothing for a wide pass to lock.
    pub(super) fn wide_refill_region(
        &self,
        lane: usize,
        need: u64,
        layout: RegionLayout,
    ) -> Option<usize> {
        let current = self.lane_regions[lane].load(Ordering::Relaxed);
        if current < layout.count
            && self.regions.stripe_hint[current].load(Ordering::Relaxed) >= need.max(1)
        {
            return Some(current);
        }
        self.switch_lane_region(lane, current, need, layout)
    }

    /// The region a lane should refill from, switching if its current one can no
    /// longer serve `need` whole-stripe blocks.
    pub(super) fn lane_region(&self, lane: usize, need: u64, layout: RegionLayout) -> usize {
        let current = self.lane_regions[lane].load(Ordering::Relaxed);
        if current < layout.count
            && self.regions.stripe_hint[current].load(Ordering::Relaxed) >= need.max(1)
        {
            return current;
        }
        match self.switch_lane_region(lane, current, need, layout) {
            Some(next) => next,
            // Nothing anywhere looks servable; try the current region (or region
            // 0 for a lane that never had one) so the caller still reaches its
            // drain + global-fallback boundary rather than short-circuiting.
            None if current < layout.count => current,
            None => 0,
        }
    }

    /// Move `lane` off region `from`.
    ///
    /// Prefers the LOWEST-address region that both looks able to serve `need` and
    /// is unclaimed: low addresses keep the whole working set dense (the leaf
    /// clustering argument above), and exclusivity is where the sharding win
    /// comes from — ZFS's metaslab result is that the benefit is owning a region,
    /// not the region being contiguous. Claims are advisory, so when every
    /// servable region is taken (including `num_lanes > num_regions`) lanes share
    /// rather than starve.
    pub(super) fn switch_lane_region(
        &self,
        lane: usize,
        from: usize,
        need: u64,
        layout: RegionLayout,
    ) -> Option<usize> {
        let mine = lane + 1;
        let mut shared = None;
        let mut exclusive = None;
        for idx in 0..layout.count {
            if idx == from || self.regions.stripe_hint[idx].load(Ordering::Relaxed) < need.max(1) {
                continue;
            }
            let owner = self.regions.owner[idx].load(Ordering::Relaxed);
            if owner == 0 || owner == mine {
                exclusive = Some(idx);
                break;
            }
            if shared.is_none() {
                shared = Some(idx);
            }
        }
        let next = exclusive.or(shared)?;
        self.regions.owner[next].store(mine, Ordering::Relaxed);
        if from < layout.count {
            let _ = self.regions.owner[from].compare_exchange(
                mine,
                0,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
        self.lane_regions[lane].store(next, Ordering::Relaxed);
        self.region_switches.fetch_add(1, Ordering::Relaxed);
        Some(next)
    }

    /// One region's share of the aligned lane refill — the pre-region body,
    /// unchanged except that the pool it walks is one region's reserve.
    ///
    /// `req.floor` is the minimum width a reserve run must have to QUALIFY. It
    /// filters candidates only — the pick is still the address-argmin among them,
    /// and a qualifying run is still drained up to the block budget, which is what
    /// turns one wide hit into a single contiguous cached run.
    pub(super) fn refill_stripe_from_region(
        &self,
        lane: usize,
        region: usize,
        req: StripeRefill,
    ) -> Option<Extent> {
        let StripeRefill {
            min_count,
            max_count,
            stripe,
            phase,
            floor,
        } = req;
        debug_assert!(
            floor >= min_count,
            "a run floor cannot be under the request"
        );
        let mut guard = self.lock_region(FreeLockSite::WriterRefill, region);
        let pools = guard.region_mut(region);
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
        let first = pools.stripe_reserve.first_fit(floor)?;
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
        //
        // A wide pass keeps the SAME entry bound for the same reason, and one wide
        // hit usually consumes the whole budget on its own, so the walk normally
        // exits on `budget` after zero iterations.
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
            if run.count >= floor {
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
            //
            // `take` matches by START, so "owns it" also means the stored span is
            // still the one that was planned. A coalesce that changed the span
            // while keeping the start would otherwise have this loop slice up a
            // run of a different length: re-inserting `[start+take, count-take)`
            // computed from the PLANNED count can manufacture free blocks past
            // the end of the run that actually existed, and those blocks are
            // live. Skipping just yields a smaller batch, which this refill
            // already tolerates everywhere else.
            let Some(stored) = pools.stripe_reserve.take(&run) else {
                continue;
            };
            if stored.count != run.count {
                pools.insert_classified(stored);
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
        let carved = Self::take_aligned_from_extent_cache(&mut cache, min_count, stripe, phase)
            .expect("stripe-reserve refill is aligned and large enough");
        // One publish for the whole refill: the push loop and the carve both ran
        // under this single lane-lock hold.
        self.publish_lane_extent_depth(lane, &cache);
        Some(carved)
    }

    /// Take an aligned extent while preserving the legacy API contract that
    /// the geometry supplied to `allocate_stripe_extent_for_lane` is enough on
    /// its own. Production's configured geometry uses the O(log N) reserve
    /// path; tests and hypothetical alternate-geometry callers fall back to
    /// the indexed aligned search across both policy pools.
    pub(super) fn take_aligned_extent_from_global(
        &self,
        need: u32,
        stripe: u32,
        phase: u32,
    ) -> Option<Extent> {
        // Ascending regions ⇒ still the global lowest-address hosting run.
        // Skipping regions whose reserve capacity reads zero is exact for the
        // production (geometry-matched) branch, and it is what keeps a
        // fully-exhausted reserve from costing one lock per region on every
        // writer allocation.
        let stripe_hinted = self.stripe_geometry() == Some((stripe, phase));
        for idx in self.walk_regions(
            stripe_hinted,
            if stripe_hinted { u64::from(need) } else { 0 },
        ) {
            if let Some(extent) = self.take_aligned_extent_from_region(idx, need, stripe, phase) {
                return Some(extent);
            }
        }
        None
    }

    pub(super) fn take_aligned_extent_from_region(
        &self,
        region: usize,
        need: u32,
        stripe: u32,
        phase: u32,
    ) -> Option<Extent> {
        let mut guard = self.lock_region(FreeLockSite::WriterRefill, region);
        let pools = guard.region_mut(region);
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

    pub(super) fn take_first_from_pools(pools: &mut FreePools, max_count: u32) -> Option<Extent> {
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

    pub(super) fn take_exact_from_pools(
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

    pub(super) fn lowest_pool_candidate(
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

    pub(super) fn take_largest_from_pools(pools: &mut FreePools) -> Option<Extent> {
        let (extent, from_reserve) = pools.largest_allocatable()?;
        if from_reserve {
            pools.stripe_reserve.remove(&extent);
        } else {
            pools.general.remove(&extent);
        }
        Some(extent)
    }

    /// Front-carve `count` blocks off the lowest-address cached run that fits.
    /// Scans from the back (the cache is descending by start), so selection is
    /// first-fit-by-address within the lane. Front-carving keeps the remainder's
    /// start above the taken run and below the next-higher entry, so replacing
    /// in place preserves the ordering.
    pub(super) fn take_from_extent_cache(cache: &mut Vec<Extent>, count: u32) -> Option<Extent> {
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

    pub(super) fn covering_extent(free: &BTreeSet<Extent>, pba: Pba) -> Option<Extent> {
        free.range(..=Extent::single(pba))
            .next_back()
            .copied()
            .filter(|extent| extent.contains(pba))
    }

    pub(super) fn overlapping_extent(free: &BTreeSet<Extent>, extent: Extent) -> Option<Extent> {
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

    pub(super) fn extents_overlap(a: Extent, b: Extent) -> bool {
        a.start.0 < b.end_pba().0 && a.end_pba().0 > b.start.0
    }

    pub(super) fn overlapping_retired_extent(&self, extent: Extent) -> Option<Extent> {
        self.lock_retired_span(RetiredLockSite::FreeOne, extent)
            .overlapping(extent)
    }
}
