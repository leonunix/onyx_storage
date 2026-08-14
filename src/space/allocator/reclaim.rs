use super::*;

impl SpaceAllocator {
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
            let pools = self.lock_span(FreeLockSite::RetireOne, extent);
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

            // Lock order: free region span (outermost, held across) -> retired
            // shard span, so the overlap check can't race a concurrent free.
            let mut retired = self.lock_retired_span(RetiredLockSite::RetireOne, extent);
            retired.charge_items(1);
            let newly = retired.retire(extent, now);
            if newly > 0 {
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
            // Lock order: free region span (outermost) → retired shard span,
            // matching `retire_extent_at` — and the two spans are the SAME index
            // range, which is what keeps the free-overlap check atomic with the
            // retired insert without any global lock. Released and retaken every
            // FREE_LOCK_HOLD_EXTENTS *and* at every region boundary, so the hold
            // only ever covers regions this group actually touches; every check
            // below is per-extent independent, so where the hold boundaries fall
            // does not change the outcome (pinned by
            // `batch_retire_equals_sequence`).
            let layout = self.regions.layout();
            for (lo, hi, hold) in region_holds(layout, chunk, free_lock_hold_extents()) {
                let pools = self.lock_span_range(FreeLockSite::RetireBatch, lo, hi);
                pools.charge_items(hold.len() as u64);
                let mut retired =
                    self.lock_retired_span_range(RetiredLockSite::RetireBatch, lo, hi);
                retired.charge_items(hold.len() as u64);
                for &extent in hold {
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
                    chunk_newly += u64::from(retired.retire(extent, now));
                    chunk_retired.push(extent);
                }
            }
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
            // free → retired, released and retaken every FREE_LOCK_HOLD_EXTENTS
            // and at every region boundary (see [`region_holds`]).
            // `chunk_freed` keeps accumulating across holds so the underflow
            // guard stays honest; every other check is per-extent independent
            // (pinned by `batch_free_equals_sequence`).
            let layout = self.regions.layout();
            for (lo, hi, hold) in region_holds(layout, chunk, free_lock_hold_extents()) {
                let mut pools = self.lock_span_range(FreeLockSite::FreeBatch, lo, hi);
                pools.charge_items(hold.len() as u64);
                let retired = self.lock_retired_span_range(RetiredLockSite::FreeBatch, lo, hi);
                retired.charge_items(hold.len() as u64);
                for &extent in hold {
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
                        || retired.overlapping(extent).is_some()
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
    pub(super) fn uncovered_subranges(set: &BTreeSet<Extent>, extent: Extent) -> Vec<Extent> {
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
        // Shard-by-shard ascending, one lock at a time: this is an audit /
        // accounting snapshot, so a torn read across shards is acceptable and
        // strictly preferable to holding every shard at once.
        let mut out = Vec::new();
        for idx in 0..self.retired.count() {
            let shard = self.lock_retired_shard(RetiredLockSite::Candidates, idx);
            out.extend(shard.set.iter().take(limit - out.len()).copied());
            if out.len() >= limit {
                break;
            }
        }
        out
    }

    /// Stripe-aligned window starts containing at least one RETIRED block,
    /// ascending and deduplicated, resuming from `*cursor` and advancing it past
    /// the last window emitted. Returns `(windows, lapped)`, where `lapped` is
    /// true when the walk ran off the end of the address space and reset the
    /// cursor to 0.
    ///
    /// This is the resident defragger's enumeration source, and the reason that
    /// half of defrag needs no L2P scan at all. A window whose non-free
    /// remainder is entirely RETIRED has no live pinner: nothing has to be
    /// rewritten for it to become one whole free stripe — reclaim alone finishes
    /// it, and all the defragger has to do is hold its free fragments out of
    /// allocation until then. Retired blocks are the only place such a window
    /// can be discovered from, and this walk costs no metadata IO.
    ///
    /// Windows with a LIVE pinner are deliberately NOT discoverable here: the
    /// only PBA → LBA map in the system is the compactor's forward L2P scan, so
    /// those stay the scan-driven selector's job
    /// (`DefragState::select_from_scan`). Trying to serve them from a reverse
    /// walk is the mistake the pre-2026-08-06 free-list walk made.
    ///
    /// The shard lock is released every [`RETIRED_WINDOW_SCAN_SLICE`] extents.
    /// A concurrent retire/reclaim can only make the walk skip a window (picked
    /// up next lap) or return a stale one (`classify_stripe_windows` re-reads
    /// occupancy and `begin_defrag_quarantine` re-validates under the free
    /// lock), so slicing needs no new proof.
    pub(crate) fn retired_stripe_windows(
        &self,
        cursor: &mut u64,
        stripe: u32,
        phase: u32,
        max_windows: usize,
    ) -> (Vec<u64>, bool) {
        if max_windows == 0 || stripe <= 1 {
            return (Vec::new(), false);
        }
        let stripe64 = u64::from(stripe);
        let phase64 = u64::from(phase);
        // Start of the stripe window containing `pba`, or None for the grid's
        // partial head window (no whole stripe to clear). Mirrors
        // `gc::defrag::window_start`.
        let window_of = |pba: u64| pba.checked_sub((pba + phase64) % stripe64);

        let layout = self.retired_layout();
        let mut out: Vec<u64> = Vec::new();
        let mut from = *cursor;
        for idx in layout.of(from)..self.retired.count() {
            from = from.max(layout.start(idx));
            let region_end = layout.end(idx);
            loop {
                let shard = self.lock_retired_shard(RetiredLockSite::DefragWindows, idx);
                let mut examined = 0usize;
                let mut advanced = false;
                for extent in shard.set.range(Extent::single(Pba(from))..) {
                    if extent.start.0 >= region_end {
                        break;
                    }
                    examined += 1;
                    // One retired extent can straddle several windows; emit each
                    // one it touches. `out` stays ascending and deduplicated
                    // because the set is address-ordered and window starts are
                    // monotone in the address.
                    let first = window_of(extent.start.0);
                    let last = window_of(extent.end_pba().0 - 1);
                    if let (Some(first), Some(last)) = (first, last) {
                        let mut w = first;
                        while w <= last {
                            if out.last() != Some(&w) {
                                out.push(w);
                            }
                            w += stripe64;
                        }
                    }
                    from = extent.end_pba().0;
                    advanced = true;
                    if out.len() >= max_windows || examined >= RETIRED_WINDOW_SCAN_SLICE {
                        break;
                    }
                }
                drop(shard);
                if out.len() >= max_windows {
                    // Resume at the next WINDOW boundary, not at the next
                    // extent: scattered overwrite retires ~1 block at a time, so
                    // several extents share the last emitted window and a
                    // per-extent cursor would re-emit it forever. Every extent
                    // the skipped remainder of that window holds is already
                    // accounted for by the window we emitted.
                    *cursor = out.last().map_or(from, |&last| (last + stripe64).max(from));
                    return (out, false);
                }
                // Slice exhausted mid-shard: re-lock and resume. Otherwise this
                // shard is done.
                if !(advanced && examined >= RETIRED_WINDOW_SCAN_SLICE) {
                    break;
                }
            }
            from = region_end;
        }
        // Ran off the end: one full lap of the retired set is done.
        *cursor = 0;
        (out, true)
    }

    /// Reclaim candidates: retired sub-ranges that have settled ≥ `grace` (i.e.
    /// are NOT covered by a young age entry), emitted as coalesced extents (fat
    /// where retires were contiguous → throughput) up to a `limit_blocks` BLOCK
    /// budget (NOT an extent count — a per-extent cap would collapse throughput
    /// under fragmented retires). Prunes aged-out entries from the age log as it
    /// scans (the time-window that bounds its memory). Every emitted block
    /// individually satisfies the grace, so freeing it honors the settle-window
    /// safety invariant.
    ///
    /// Returns `(candidates, deferred_blocks)` where `deferred_blocks` is the
    /// total retired-but-still-young block count (held back by the grace) — the
    /// diagnostic that, vs rc-rejected, localized the re-aging bottleneck.
    ///
    /// ## The hold is SLICED (2026-07-30)
    ///
    /// This used to run under ONE acquisition of the global retired+age locks for
    /// its whole duration, box-measured at **1.169 s per GC cycle, 41 cycles in a
    /// 480 s window = 10% of wall with the lock fully closed**. Every cleanup
    /// thread that had already taken a free-pool region lock piled up behind it
    /// still holding that region — which is how a selector ended up as the
    /// `writer_refill wait_max = 1358 ms` the flush writers saw.
    ///
    /// So it now works one shard at a time and releases the shard lock every
    /// [`AGED_SCAN_SLICE`] entries, resuming from a PBA cursor. Two passes per
    /// shard, in this order:
    ///
    /// 1. **prune** — always runs to the end of the shard's age log (so the log
    ///    stays time-windowed and `deferred_blocks` stays a true total), summing
    ///    the still-young blocks;
    /// 2. **emit** — walks the retired set until the block budget is spent.
    ///
    /// Prune-before-emit per shard is REQUIRED: [`Self::aged_subranges`] treats
    /// every present age entry as young without re-reading its timestamp.
    ///
    /// Releasing the lock mid-walk is safe without new proof. A concurrent retire
    /// or reclaim can only make the walk **skip** blocks (picked up next cycle) or
    /// **re-emit** one (`reclaim_retired_extent` / Phase A re-validate containment
    /// under the shard lock and fail closed, so a stale candidate is a no-op).
    /// `deferred_blocks` becomes a slightly torn total, which it already was
    /// relative to the retire path — it feeds a metric, never a free decision.
    ///
    /// ⚠ Sliced, not asymptotically cheaper: the prune is still O(age entries) per
    /// cycle, just never in one hold. If |age| grows enough for that CPU cost to
    /// matter on its own (box: ~1.6 M entries ≈ 152 ms/cycle ≈ 1.3% of wall), the
    /// next step is a time-ordered index so pruning becomes O(expiring).
    pub fn aged_candidates(
        &self,
        limit_blocks: usize,
        grace: Duration,
        now: Instant,
    ) -> (Vec<Extent>, u64) {
        if limit_blocks == 0 {
            return (Vec::new(), 0);
        }
        let layout = self.retired_layout();
        let mut out = Vec::new();
        let mut emitted: usize = 0;
        let mut deferred_blocks: u64 = 0;
        for idx in 0..self.retired.count() {
            // Pass 1 — prune this shard's age log, slice by slice.
            let mut cursor = layout.start(idx);
            loop {
                let mut shard = self.lock_retired_shard(RetiredLockSite::AgedCandidates, idx);
                let mut expired: Vec<u64> = Vec::new();
                let mut last: Option<u64> = None;
                let mut seen = 0usize;
                for (&start, run) in shard.age.range(cursor..) {
                    if now.duration_since(run.retired_at) >= grace {
                        expired.push(start);
                    } else {
                        deferred_blocks += u64::from(run.count);
                    }
                    last = Some(start);
                    seen += 1;
                    if seen >= AGED_SCAN_SLICE {
                        break;
                    }
                }
                shard.charge_items(seen as u64);
                for start in expired {
                    shard.age.remove(&start);
                }
                match last {
                    Some(start) if seen >= AGED_SCAN_SLICE => cursor = start + 1,
                    _ => break,
                }
            }

            // Pass 2 — emit aged sub-ranges until the block budget is spent.
            let mut cursor = layout.start(idx);
            while emitted < limit_blocks {
                let shard = self.lock_retired_shard(RetiredLockSite::AgedCandidates, idx);
                let mut seen = 0usize;
                let mut next: Option<u64> = None;
                for ext in shard.set.range(Extent::single(Pba(cursor))..) {
                    if emitted >= limit_blocks || seen >= AGED_SCAN_SLICE {
                        break;
                    }
                    for aged in Self::aged_subranges(&shard.age, *ext) {
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
                    next = Some(ext.end_pba().0);
                    seen += 1;
                }
                shard.charge_items(seen as u64);
                match next {
                    // Only re-acquire if the slice cap (not the budget) stopped us.
                    Some(end) if seen >= AGED_SCAN_SLICE && emitted < limit_blocks => cursor = end,
                    _ => break,
                }
            }
        }
        (out, deferred_blocks)
    }

    /// Sub-ranges of coalesced retired extent `ext` NOT covered by any young
    /// entry in `age` (= the grace-satisfied, reclaimable parts). Same ordered
    /// walk as [`Self::uncovered_subranges`] but over the age log.
    pub(super) fn aged_subranges(age: &BTreeMap<u64, RetiredRun>, ext: Extent) -> Vec<Extent> {
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
    pub(super) fn purge_age_range(age: &mut BTreeMap<u64, RetiredRun>, ext: Extent) {
        let s = ext.start.0;
        let e = ext.end_pba().0;
        let keys: Vec<u64> = age.range(s..e).map(|(&k, _)| k).collect();
        for k in keys {
            age.remove(&k);
        }
    }

    pub fn is_retired(&self, pba: Pba) -> bool {
        let idx = self.retired_layout().of(pba.0);
        self.lock_retired_shard(RetiredLockSite::IsRetired, idx)
            .covering(pba)
            .is_some()
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
        (0..self.retired.count())
            .map(|idx| {
                self.lock_retired_shard(RetiredLockSite::Audit, idx)
                    .blocks()
            })
            .sum()
    }

    /// Release a retired extent into the free list after GC has proved it is
    /// no longer referenced by metadata. `extent` may be a SUB-RANGE of a larger
    /// coalesced retired extent (the reclaim path frees aged sub-prefixes); the
    /// covering extent is split and the non-reclaimed remainders kept retired.
    pub fn reclaim_retired_extent(&self, extent: Extent) -> OnyxResult<bool> {
        self.validate_extent_shape(extent, "reclaim_retired_extent")?;

        {
            let mut retired = self.lock_retired_span(RetiredLockSite::ReclaimOne, extent);
            retired.charge_items(1);
            // The candidate must be fully contained in one coalesced retired
            // extent per shard it spans. Fail closed (Ok(false)) if it is no
            // longer (fully) retired — a raced reclaim / re-alloc — never free a
            // span we didn't verify. ALL-OR-NOTHING here (the batch path frees
            // per verified part instead): this keeps the single path's contract
            // byte-identical to the pre-sharding one.
            let taken = retired.take_for_reclaim(extent);
            let covered: u32 = taken.iter().map(|t| t.count).sum();
            if covered != extent.count {
                for part in taken {
                    retired.reinsert(part);
                }
                return Ok(false);
            }
        }

        let result = (|| -> OnyxResult<()> {
            self.hazards.wait_extent_clear(extent.start, extent.count);
            self.ensure_not_in_lane_cache(extent, "reclaim_retired_extent")?;

            let mut pools = self.lock_span(FreeLockSite::ReclaimOne, extent);
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
            let mut retired = self.lock_retired_span(RetiredLockSite::ReclaimReinsert, extent);
            retired.charge_items(1);
            retired.reinsert(extent);
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

            // Phase A — retired shards ONCE per group: validate containment, split
            // out the covering coalesced extent, keep the remainders retired.
            // Collect the validated extents for Phase B. Grouped by shard span
            // (`region_holds`) rather than by count, so a sharded pool takes one
            // hold per shard instead of one per `free_lock_hold_extents()`; with
            // one shard it is exactly `chunk.chunks(cap)` as before.
            let layout = self.retired_layout();
            let mut removed: Vec<Extent> = Vec::with_capacity(chunk.len());
            for (lo, hi, hold) in region_holds(layout, chunk, free_lock_hold_extents()) {
                let mut retired =
                    self.lock_retired_span_range(RetiredLockSite::ReclaimPhaseA, lo, hi);
                retired.charge_items(hold.len() as u64);
                for &extent in hold {
                    if self
                        .validate_extent_shape(extent, "reclaim_retired_extents_batch")
                        .is_err()
                    {
                        continue; // defensive: GC candidates are always well-formed
                    }
                    // Fail closed per shard part if no longer fully retired
                    // (raced reclaim/realloc); Phase B then frees exactly the
                    // parts that were verified here.
                    removed.extend(retired.take_for_reclaim(extent));
                }
            }

            // Phase B — `free` lock ONCE: re-validate against the lane snapshot +
            // free-list overlap (double-free guard), free the clean ones, defer
            // conflicts. `chunk_freed` tracks the not-yet-applied allocated debit
            // so the underflow guard stays honest within the chunk.
            let mut conflicts: Vec<Extent> = Vec::new();
            let mut chunk_reclaimed: Vec<Extent> = Vec::new();
            let mut chunk_freed: u64 = 0;
            // The 12.9 ms hold this whole exercise is about: up to 4096
            // `release_extent` (coalesce-insert into 3 indexes) calls used to run
            // under ONE acquisition of ONE global lock. Now bounded to
            // FREE_LOCK_HOLD_EXTENTS per hold AND confined to the regions the
            // group actually touches — same total work, same order.
            for (lo, hi, hold) in region_holds(layout, &removed, free_lock_hold_extents()) {
                let mut pools = self.lock_span_range(FreeLockSite::ReclaimBatch, lo, hi);
                pools.charge_items(hold.len() as u64);
                for &extent in hold {
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
                for (lo, hi, hold) in region_holds(layout, &conflicts, free_lock_hold_extents()) {
                    let mut retired =
                        self.lock_retired_span_range(RetiredLockSite::ReclaimReinsert, lo, hi);
                    retired.charge_items(hold.len() as u64);
                    for &extent in hold {
                        retired.reinsert(extent);
                    }
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
    pub(super) fn snapshot_lane_caches(&self) -> (HashSet<Pba>, Vec<Extent>) {
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
    pub(super) fn sorted_extents_overlap(sorted: &[Extent], extent: Extent) -> bool {
        let idx = sorted.partition_point(|e| e.start.0 <= extent.start.0);
        if idx > 0 && Self::extents_overlap(sorted[idx - 1], extent) {
            return true;
        }
        idx < sorted.len() && Self::extents_overlap(sorted[idx], extent)
    }

    pub(super) fn free_extent_unchecked_ownership(&self, extent: Extent) -> OnyxResult<()> {
        self.validate_free_extent(extent)?;

        self.hazards.wait_extent_clear(extent.start, extent.count);

        {
            let mut pools = self.lock_span(FreeLockSite::FreeOne, extent);
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

    pub(super) fn validate_free_extent(&self, extent: Extent) -> OnyxResult<()> {
        self.validate_extent_shape(extent, "free_extent")?;
        self.ensure_not_in_lane_cache(extent, "free_extent")?;

        let pools = self.lock_span(FreeLockSite::FreeOne, extent);

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

    pub(super) fn validate_extent_shape(
        &self,
        extent: Extent,
        context: &'static str,
    ) -> OnyxResult<()> {
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

    pub(super) fn ensure_not_in_lane_cache(
        &self,
        extent: Extent,
        context: &'static str,
    ) -> OnyxResult<()> {
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
    pub(super) fn extract_lane_cache_free_parts(&self, target: Extent) -> Vec<Extent> {
        let mut extracted = Vec::new();

        for (lane, cache_mutex) in self.lane_caches.iter().enumerate() {
            let mut cache = cache_mutex.lock().unwrap();
            cache.retain(|pba| {
                if target.contains(*pba) {
                    extracted.push(Extent::single(*pba));
                    false
                } else {
                    true
                }
            });
            self.publish_lane_depth(lane, &cache);
        }

        for (lane, cache_mutex) in self.lane_extent_caches.iter().enumerate() {
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
            self.publish_lane_extent_depth(lane, &cache);
        }

        extracted
    }
}
