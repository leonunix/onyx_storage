use super::*;

impl SpaceAllocator {
    /// Configure the engine's fixed LV3 RAID geometry so stripe-aligned
    /// first-fit queries use the effective-capacity index instead of a
    /// slack-check scan (65 ms/call on a 3M-fragment belt, inside the global
    /// free lock — the 2026-07-03 throughput-oscillation root cause). Call
    /// once at startup before flush traffic; idempotent; `stripe <= 1`
    /// (non-RAID backends) clears it.
    /// Also the point where the region boundaries are finalized: they must be
    /// stripe-aligned, and the stripe is not known when the allocator is built.
    /// Every region lock is held across the re-layout, and the regions are
    /// emptied before the new boundaries are published, so no extent can be left
    /// sitting in a region that no longer owns its address.
    pub fn set_stripe_geometry(&self, stripe_blocks: u32, phase: u32) {
        let requested = (stripe_blocks > 1).then_some((stripe_blocks, phase));
        let planned = self.regions.planned_layout(stripe_blocks, phase);
        let mut guard = self.lock_all_regions(FreeLockSite::Setup);
        if guard.geometry() == requested && guard.layout == planned {
            return;
        }
        let mut runs = Vec::new();
        for region in &mut guard.guards {
            runs.extend(region.take_all_runs());
        }
        // The retired shards route on the SAME layout, so moving the boundaries
        // has to re-shard them too — an extent left behind under the old
        // boundaries could otherwise sit in a shard that does not own it, which
        // breaks every containment query. Every shard lock is held from drain to
        // re-insert (lock order stays free -> retired), so no reader can observe
        // the emptied window. Normally a no-op: geometry is configured at startup,
        // before anything has been retired.
        let mut shards = self.lock_all_retired(RetiredLockSite::Setup);
        let mut retired_runs = Vec::new();
        let mut retired_age = Vec::new();
        for shard in &mut shards {
            retired_runs.extend(std::mem::take(&mut shard.set));
            retired_age.extend(std::mem::take(&mut shard.age));
        }
        self.regions.publish_layout(planned);
        guard.layout = planned;
        for region in &mut guard.guards {
            region.reset_geometry(stripe_blocks, phase);
        }
        runs.sort_unstable_by_key(|extent| extent.start.0);
        for run in runs {
            guard.insert_classified(run);
        }
        retired_runs.sort_unstable_by_key(|extent| extent.start.0);
        for run in retired_runs {
            let (lo, hi) = planned.span(run);
            for idx in lo..=hi {
                if let Some(part) = planned.clip(idx, run) {
                    shards[idx].reinsert(part);
                }
            }
        }
        for (start, run) in retired_age {
            shards[planned.of(start)].age.insert(start, run);
        }
        drop(shards);
        drop(guard);
        self.geometry_cache.store(
            requested.map_or(0, |(stripe, phase)| {
                u64::from(stripe) << 32 | u64::from(phase)
            }),
            Ordering::Relaxed,
        );
    }

    /// Remove an aligned physical range from ordinary allocation while the
    /// defragger evacuates its live pinners. Existing free pieces are moved into
    /// the target atomically; after publication, wait for pre-existing PBA pins
    /// without holding the allocator lock.
    pub fn begin_defrag_quarantine(&self, target: Extent) -> OnyxResult<()> {
        self.validate_extent_shape(target, "begin_defrag_quarantine")?;
        {
            let mut span = self.lock_span(FreeLockSite::Quarantine, target);
            let (stripe, phase) = span.geometry().ok_or_else(|| {
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
            // A quarantine is tracked in exactly ONE region so that
            // progress/complete/cancel stay single-lock lookups keyed by the
            // target's start PBA. Region boundaries are stripe-aligned and every
            // real defrag target is exactly one stripe
            // (`GcDefragState::qualify_and_emit`), so this is unreachable in
            // production — rejecting here is still better than silently
            // splitting one target's completion accounting across two locks.
            // Checked BEFORE anything is extracted so there is nothing to undo.
            if !span.spans_one_region() {
                return Err(OnyxError::Config(format!(
                    "defrag quarantine {:?} crosses an allocator region boundary",
                    target
                )));
            }
            if let Some(existing) = span.overlapping_quarantine(target) {
                return Err(OnyxError::Config(format!(
                    "defrag quarantine {:?} overlaps target {:?}",
                    target, existing
                )));
            }
            if span.overlaps_reserve(target) {
                return Err(OnyxError::Config(format!(
                    "defrag quarantine {:?} overlaps stripe reserve",
                    target
                )));
            }
            // Lock order is FreePools -> lane caches. Allocation fast paths
            // never hold a lane lock while acquiring FreePools. Cached blocks
            // are logically free, so detach only the target intersections and
            // leave any head/tail pieces in their originating lane.
            let mut free_parts = span.extract_from_general(target);
            free_parts.extend(self.extract_lane_cache_free_parts(target));
            let mut target_free = span.empty_set_with_geometry();
            for extent in free_parts {
                target_free.coalesce_insert(extent);
            }
            let pools = span
                .single_mut()
                .expect("cross-region target rejected above");
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
        let idx = self.regions.layout().of(start.0);
        let guard = self.lock_region(FreeLockSite::Quarantine, idx);
        let target = guard.region(idx).quarantines.get(&start.0)?;
        Some((target.free_parts.blocks_total(), target.range.count as u64))
    }

    /// Publish a fully-free quarantine as stripe reserve. A partially-free
    /// target remains active and returns `Ok(false)`.
    ///
    /// ⚠ This is the ONLY place that inserts a whole stripe-aligned window into
    /// the allocatable pool in one shot, without having verified each block
    /// individually — every other insert path only ever returns blocks a caller
    /// just proved dead. That makes its gate load-bearing for data integrity:
    /// publishing a window that still holds a LIVE block hands that block to the
    /// next writer, which overwrites it while its L2P mapping is intact, and the
    /// reader gets `CRC mismatch` on an LBA that was never touched. Box forensics
    /// 2026-08-12: 476 double-claimed blocks, and all 126 of their consecutive
    /// runs sat inside ONE stripe-aligned window each (1–5 of 6 blocks, never a
    /// whole stripe) — the fingerprint of exactly this publish.
    ///
    /// So the gate is STRUCTURAL, not a block count. `free_parts` is built only
    /// through `coalesce_insert`, so a genuinely-complete window is one folded
    /// extent equal to `range`; requiring that cannot be satisfied by a drifted
    /// aggregate. A `blocks_total` that claims completeness while the set does
    /// not is an upstream accounting bug: cancel the target (which returns only
    /// the pieces that really are free) instead of publishing, and say so.
    pub fn complete_defrag_quarantine(&self, start: Pba) -> OnyxResult<bool> {
        let idx = self.regions.layout().of(start.0);
        let mut guard = self.lock_region(FreeLockSite::Quarantine, idx);
        let pools = guard.region_mut(idx);
        let Some(target) = pools.quarantines.get(&start.0) else {
            return Ok(false);
        };
        let range = target.range;
        if !target.free_parts.is_exactly(range) {
            if target.free_parts.blocks_total() >= range.count as u64 {
                tracing::error!(
                    start = start.0,
                    blocks = range.count,
                    free_blocks = target.free_parts.blocks_total(),
                    free_extents = target.free_parts.by_addr().len(),
                    "defrag quarantine reports itself complete but its free parts do not \
                     cover the window — refusing to publish, cancelling instead"
                );
                drop(guard);
                self.cancel_defrag_quarantine(start);
            }
            return Ok(false);
        }
        let target = pools
            .quarantines
            .remove(&start.0)
            .expect("target checked above");
        // The target lives inside one region (enforced at begin), so publishing
        // it back needs no cross-region split.
        pools.insert_classified(target.range);
        Ok(true)
    }

    /// Abandon an active quarantine and return only its already-free pieces to
    /// the canonical general/reserve partition. Live/retired pieces were never
    /// removed from their ownership states.
    pub fn cancel_defrag_quarantine(&self, start: Pba) -> bool {
        let idx = self.regions.layout().of(start.0);
        let mut guard = self.lock_region(FreeLockSite::Quarantine, idx);
        let pools = guard.region_mut(idx);
        let Some(target) = pools.quarantines.remove(&start.0) else {
            return false;
        };
        let free_parts: Vec<Extent> = target.free_parts.by_addr().iter().copied().collect();
        for extent in free_parts {
            pools.insert_classified(extent);
        }
        true
    }

    /// Test-only: build an allocator with the live-PBA duplicate-allocation
    /// tracker armed regardless of `ONYX_ALLOC_TRACK`, so a stress test does not
    /// have to mutate process-global env.
    #[cfg(test)]
    pub(crate) fn new_tracked(device_size_bytes: u64, num_lanes: usize, regions: usize) -> Self {
        let mut me = Self::new_with_regions(device_size_bytes, num_lanes, regions);
        me.alloc_tracker = Some(Mutex::new(BTreeSet::new()));
        me
    }

    /// Test-only: every free set the allocator owns, checked against its own
    /// invariants (index agreement, aggregate totals, disjointness).
    #[cfg(test)]
    pub(crate) fn assert_free_sets_consistent(&self) {
        for idx in 0..self.regions.count() {
            let guard = self.lock_region(FreeLockSite::Setup, idx);
            let pools = guard.region(idx);
            pools.general.assert_consistent();
            pools.stripe_reserve.assert_consistent();
            for target in pools.quarantines.values() {
                target.free_parts.assert_consistent();
            }
        }
    }

    /// Test-only: snapshot of the live-PBA tracker.
    #[cfg(test)]
    pub(crate) fn tracked_live_pbas(&self) -> BTreeSet<Pba> {
        self.alloc_tracker
            .as_ref()
            .expect("tracker armed")
            .lock()
            .unwrap()
            .clone()
    }

    /// Test-only: add an extent to an active quarantine's free-parts set without
    /// going through `release_extent`, so a test can model the accounting drift
    /// the structural completion gate exists to catch — a `blocks_total` that
    /// reaches the window size while the set does not actually cover it.
    #[cfg(test)]
    pub(crate) fn inject_quarantine_free_part_for_test(&self, start: Pba, extent: Extent) {
        let idx = self.regions.layout().of(start.0);
        let mut guard = self.lock_region(FreeLockSite::Quarantine, idx);
        let target = guard
            .region_mut(idx)
            .quarantines
            .get_mut(&start.0)
            .expect("quarantine target exists");
        target.free_parts.insert_for_test(extent);
    }

    pub fn is_defrag_quarantined(&self, extent: Extent) -> bool {
        self.lock_span(FreeLockSite::Quarantine, extent)
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
        let span = self.lock_span(FreeLockSite::Quarantine, extent);
        if span.overlapping_quarantine(extent).is_some() {
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

    pub(super) fn track_alloc(&self, extent: Extent, context: &'static str) -> OnyxResult<()> {
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

    pub(super) fn track_release(&self, extent: Extent, context: &'static str) {
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
}
