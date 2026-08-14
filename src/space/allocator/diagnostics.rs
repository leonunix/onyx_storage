use super::*;

impl SpaceAllocator {
    /// Return true if the whole extent is already covered by a free extent
    /// or all its blocks are sitting in lane caches.
    pub fn is_extent_free(&self, extent: Extent) -> bool {
        let pools = self.lock_span(FreeLockSite::Audit, extent);
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
            // The grown tail routes to whichever regions own it; because the
            // LAST region is unbounded above, growth never needs a re-layout.
            // One `lock_span` per u32-sized chunk rather than one all-regions
            // hold: growth is rare and there is no atomicity requirement across
            // chunks (the frontier was already published above).
            let mut start = old_total;
            let mut remaining = added;
            while remaining > 0 {
                let count = remaining.min(u32::MAX as u64) as u32;
                let extent = Extent::new(Pba(start), count);
                self.lock_span(FreeLockSite::Setup, extent)
                    .insert_classified(extent);
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
    /// Sharded, this sums region by region taking ONE region lock at a time, so
    /// the snapshot is no longer a single instant. Every consumer is advisory
    /// (the defrag trigger, `status`), and holding N locks to freeze the whole
    /// space would stall every writer for the duration.
    pub fn contiguity_stats(&self) -> ContiguityStats {
        let mut out = ContiguityStats {
            free_blocks_in_set: 0,
            free_extents: 0,
            largest_run_blocks: 0,
            stripe_capable_blocks: None,
            stripe_reserve_blocks: 0,
            quarantine_target_blocks: 0,
            quarantine_free_blocks: 0,
        };
        for idx in 0..self.regions.count() {
            let guard = self.lock_region(FreeLockSite::Audit, idx);
            let pools = guard.region(idx);
            out.quarantine_free_blocks += pools
                .quarantines
                .values()
                .map(|target| target.free_parts.blocks_total())
                .sum::<u64>();
            out.quarantine_target_blocks += pools
                .quarantines
                .values()
                .map(|target| target.range.count as u64)
                .sum::<u64>();
            out.free_blocks_in_set += pools.free_blocks_in_pools();
            out.free_extents += (pools.general.len()
                + pools.stripe_reserve.len()
                + pools
                    .quarantines
                    .values()
                    .map(|target| target.free_parts.len())
                    .sum::<usize>()) as u64;
            out.largest_run_blocks = out.largest_run_blocks.max(
                pools
                    .general
                    .largest()
                    .into_iter()
                    .chain(pools.stripe_reserve.largest())
                    .map(|extent| extent.count)
                    .max()
                    .unwrap_or(0),
            );
            if pools.geometry().is_some() {
                let capable =
                    pools.general.stripe_capacity() + pools.stripe_reserve.stripe_capacity();
                out.stripe_capable_blocks = Some(out.stripe_capable_blocks.unwrap_or(0) + capable);
            }
            out.stripe_reserve_blocks += pools.stripe_reserve.blocks_total();
        }
        out
    }

    /// The configured RAID geometry `(stripe_blocks, phase)`, if any. Served from
    /// an atomic written by `set_stripe_geometry`, so the GC defrag scanner's
    /// per-cluster query takes no region lock.
    pub fn stripe_geometry(&self) -> Option<(u32, u32)> {
        let packed = self.geometry_cache.load(Ordering::Relaxed);
        (packed != 0).then(|| ((packed >> 32) as u32, packed as u32))
    }

    /// Blocks of `range` covered by free extents — the defrag target "done"
    /// recheck. One brief free-lock hold, O(log N + overlaps in range).
    pub(crate) fn free_overlap_blocks(&self, range: Extent) -> u64 {
        let span = self.lock_span(FreeLockSite::Audit, range);
        let (lo, hi) = span.layout.span(range);
        (lo..=hi)
            .map(|idx| span.region(idx).overlap_free_blocks(range))
            .sum()
    }

    /// Free/retired occupancy of stripe-aligned windows, BATCHED.
    ///
    /// `starts` must be ascending, deduplicated stripe-aligned window starts
    /// (the caller derives them from [`Self::stripe_geometry`]); the result is
    /// one `(free_blocks, retired_blocks)` per input, in input order.
    ///
    /// This is the scan-driven defrag selector's classify step. The compactor's
    /// L2P window scan streams thousands of candidate windows per cycle, so
    /// calling `free_overlap_blocks` + `retired_overlap_blocks` per window
    /// (two lock acquisitions each) would add ~10^5 region-lock trips per
    /// second to the very locks the flusher-writers contend
    /// (`fragmentation_unaligned_alloc_1889_locks`: lock COUNT, not wait, is
    /// what costs the writer). Grouping by region collapses that to one hold
    /// per region per pass, reusing [`region_holds`] exactly like the retire /
    /// reclaim batch paths.
    ///
    /// Free and retired are two SEPARATE passes so the one-directional
    /// `free -> retired` lock order is never inverted (see
    /// [`Self::retired_overlap_blocks`]). The halves are therefore not one
    /// atomic snapshot, which is fine: selection is advisory —
    /// `begin_defrag_quarantine` re-validates under the free lock and the
    /// rewriter re-validates every LBA against the live blockmap.
    pub(crate) fn classify_stripe_windows(&self, starts: &[u64], stripe: u32) -> Vec<(u32, u32)> {
        if starts.is_empty() || stripe == 0 {
            return Vec::new();
        }
        let windows: Vec<Extent> = starts
            .iter()
            .map(|&start| Extent::new(Pba(start), stripe))
            .collect();
        let layout = self.regions.layout();
        let cap = free_lock_hold_extents();
        let mut out = vec![(0u32, 0u32); windows.len()];

        let mut base = 0usize;
        for (lo, hi, hold) in region_holds(layout, &windows, cap) {
            let span = self.lock_span_range(FreeLockSite::DefragClassify, lo, hi);
            span.charge_items(hold.len() as u64);
            for (i, &window) in hold.iter().enumerate() {
                let (wlo, whi) = layout.span(window);
                let free: u64 = (wlo..=whi)
                    .map(|idx| span.region(idx).overlap_free_blocks(window))
                    .sum();
                out[base + i].0 = free as u32;
            }
            base += hold.len();
        }

        // Second pass, free locks all released: retired occupancy.
        let mut base = 0usize;
        for (lo, hi, hold) in region_holds(self.retired_layout(), &windows, cap) {
            let span = self.lock_retired_span_range(RetiredLockSite::OverlapBlocks, lo, hi);
            for (i, &window) in hold.iter().enumerate() {
                out[base + i].1 = span.overlap_blocks(window) as u32;
            }
            base += hold.len();
        }
        out
    }

    /// Blocks of `range` covered by retired extents. Takes ONLY the retired
    /// lock (callers must NOT hold `free_extents` — keeps the free→retired
    /// lock order one-directional). Retired extents never overlap each other
    /// (coalesced set), so summing clamped intersections is exact.
    pub(crate) fn retired_overlap_blocks(&self, range: Extent) -> u64 {
        self.lock_retired_span(RetiredLockSite::OverlapBlocks, range)
            .overlap_blocks(range)
    }

    /// Number of distinct runs in the global free set. Test-only: the stripe
    /// density guard asserts this stays O(1) under repeated aligned allocation
    /// (alignment pads must not fragment the free list into per-alloc slivers).
    #[cfg(test)]
    pub(crate) fn free_extent_run_count(&self) -> usize {
        self.pool_extent_count()
    }

    pub(super) fn coalesce_and_insert_any_overlap(set: &mut BTreeSet<Extent>, new: Extent) {
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
}
