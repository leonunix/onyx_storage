use super::*;

/// One original retire operation's age, tracked at retire granularity in the
/// `retired_age` log so coalescing the `retired_extents` set can never re-age it.
#[derive(Debug, Clone, Copy)]
pub(super) struct RetiredRun {
    pub(super) count: u32,
    pub(super) retired_at: Instant,
}

/// The coalesced retired extents belonging to ONE PBA region, plus the young-age
/// log for the same address range, under ONE mutex.
///
/// **Sharded on the same [`RegionLayout`] as [`RegionPools`]** — deliberately the
/// same, because the atomicity the retire path needs is exactly "the free-overlap
/// check and the retired insert for extent E happen together". With one shared
/// layout that pair is `{pools[i], retired[i]}` for a single `i`, and different
/// `i` are independent. That is the only way to take this lock off the global
/// path without touching the check↔insert atomicity which prevents double
/// ownership — the property that ruled out the three cheaper alternatives (see
/// the 2026-07-29 note on `reclaim_retired_extents_batch`).
///
/// Merging the age log into the same mutex is a RESULT of the 2026-07-30 box
/// attribution, not a shortcut: `retired_age` was only ever acquired under a
/// `retired_extents` hold **by the same call path**, and its measured wait was
/// 0.13 µs/acq = 0.1% of the retire batch's region hold. A second lock bought
/// nothing and cost an extra 17567 acquisitions/s.
///
/// INVARIANT (load-bearing, mirroring `RegionPools`): every extent in `set` and
/// every entry in `age` lies entirely inside this shard's region range. Every
/// insert clips to the region, so a containment/overlap question about an extent
/// is answered by asking exactly the shards it spans — and coalescing therefore
/// stops at region boundaries (one unfoldable seam per boundary, the same
/// accepted cost the free pool pays).
#[derive(Default)]
pub(super) struct RetiredShard {
    /// Authority for containment/overlap (`is_retired`,
    /// `overlapping_retired_extent`, reclaim validation). NEVER carries age.
    pub(super) set: BTreeSet<Extent>,
    /// Advisory young-age log (start pba → run) holding ONLY entries younger than
    /// the reclaim grace — `aged_candidates` prunes the rest, which is the
    /// time-window that bounds its memory. Gates reclaim eligibility only: a
    /// retired sub-range is reclaimable iff no entry here covers it.
    ///
    /// ⚠ [`SpaceAllocator::aged_subranges`] treats every PRESENT entry as young
    /// without re-reading its timestamp, so the prune for a shard must complete
    /// before that shard is walked for candidates.
    pub(super) age: BTreeMap<u64, RetiredRun>,
}

impl RetiredShard {
    pub(super) fn covering(&self, pba: Pba) -> Option<Extent> {
        SpaceAllocator::covering_extent(&self.set, pba)
    }

    pub(super) fn overlapping(&self, extent: Extent) -> Option<Extent> {
        SpaceAllocator::overlapping_extent(&self.set, extent)
    }

    /// Retire `part` (already clipped to this shard): stamp the genuinely-new
    /// sub-ranges with `now` and coalesce `part` into the set. Returns the newly
    /// retired block count (0 = idempotent re-retire).
    ///
    /// The gaps are computed BEFORE coalescing so already-retired sub-ranges keep
    /// their original age and can never be refreshed.
    pub(super) fn retire(&mut self, part: Extent, now: Instant) -> u32 {
        let gaps = SpaceAllocator::uncovered_subranges(&self.set, part);
        let newly: u32 = gaps.iter().map(|g| g.count).sum();
        SpaceAllocator::coalesce_and_insert_any_overlap(&mut self.set, part);
        for g in gaps {
            self.age.insert(
                g.start.0,
                RetiredRun {
                    count: g.count,
                    retired_at: now,
                },
            );
        }
        newly
    }

    /// Reclaim-side removal of `part`: require it to be FULLY contained in one
    /// coalesced retired extent, split the cover and keep the remainders retired.
    /// `false` = no longer (fully) retired — a raced reclaim/realloc; **fail
    /// closed**, never release a span we did not verify.
    pub(super) fn take_for_reclaim(&mut self, part: Extent) -> bool {
        let cover = match self.covering(part.start) {
            Some(c) if c.end_pba().0 >= part.end_pba().0 => c,
            _ => return false,
        };
        self.set.remove(&cover);
        if part.start.0 > cover.start.0 {
            self.set.insert(Extent::new(
                cover.start,
                (part.start.0 - cover.start.0) as u32,
            ));
        }
        if cover.end_pba().0 > part.end_pba().0 {
            self.set.insert(Extent::new(
                part.end_pba(),
                (cover.end_pba().0 - part.end_pba().0) as u32,
            ));
        }
        // Defensive: aged candidates are carved between young entries, so
        // normally there is nothing to purge.
        SpaceAllocator::purge_age_range(&mut self.age, part);
        true
    }

    /// Re-insert a reclaim that failed downstream, COALESCING it back with the
    /// split remainders. The age log is NOT touched: `part` was already aged, so
    /// it stays immediately eligible next cycle — no re-aging on the error path.
    pub(super) fn reinsert(&mut self, part: Extent) {
        SpaceAllocator::coalesce_and_insert_any_overlap(&mut self.set, part);
    }

    #[cfg(test)]
    pub(super) fn blocks(&self) -> u64 {
        self.set.iter().map(|e| u64::from(e.count)).sum()
    }
}

/// Retired shards, one per PBA region — the `retired_extents` analogue of
/// [`RegionPools`]. The shard count is fixed at construction (it sizes the mutex
/// vector) and equals the region count, so `RegionLayout::of` routes both.
pub(super) struct RetiredRegions {
    pub(super) shards: Vec<Mutex<RetiredShard>>,
}

impl RetiredRegions {
    pub(super) fn new(count: usize) -> Self {
        Self {
            shards: (0..count.max(1))
                .map(|_| Mutex::new(RetiredShard::default()))
                .collect(),
        }
    }

    pub(super) fn count(&self) -> usize {
        self.shards.len()
    }
}

/// The retired shards spanned by one extent, locked in ASCENDING index order —
/// the `retired` analogue of [`SpanGuard`], with the same clipping discipline.
///
/// Lock order across the allocator is uniformly `free region -> retired shard`
/// (no path takes a retired shard and then a free region), and multi-shard holds
/// are always ascending, so neither can deadlock against the other.
pub(super) struct RetiredSpan<'a> {
    pub(super) layout: RegionLayout,
    pub(super) lo: usize,
    pub(super) guards: Vec<TimedGuard<'a, RetiredShard, RETIRED_LOCK_SITES>>,
}

impl RetiredSpan<'_> {
    pub(super) fn shard(&self, idx: usize) -> &RetiredShard {
        &self.guards[idx - self.lo]
    }

    pub(super) fn shard_mut(&mut self, idx: usize) -> &mut RetiredShard {
        &mut self.guards[idx - self.lo]
    }

    /// Charged once per HOLD (not per shard guard) — see
    /// [`SpanGuard::charge_items`].
    pub(super) fn charge_items(&self, n: u64) {
        if let Some(first) = self.guards.first() {
            first.charge_items(n);
        }
    }

    /// First retired extent overlapping `extent`, asking exactly the shards it
    /// spans. The double-free guard on the free path.
    pub(super) fn overlapping(&self, extent: Extent) -> Option<Extent> {
        let (lo, hi) = self.layout.span(extent);
        (lo..=hi).find_map(|idx| {
            let part = self.layout.clip(idx, extent)?;
            self.shard(idx).overlapping(part)
        })
    }

    /// Retire `extent` across the shards it spans; returns newly-retired blocks.
    pub(super) fn retire(&mut self, extent: Extent, now: Instant) -> u32 {
        let (lo, hi) = self.layout.span(extent);
        let mut newly = 0u32;
        for idx in lo..=hi {
            if let Some(part) = self.layout.clip(idx, extent) {
                newly += self.shard_mut(idx).retire(part, now);
            }
        }
        newly
    }

    /// Reclaim-side removal across shards, per clipped part, fail-closed per
    /// part. Returns the parts actually removed (empty = nothing was verifiable).
    ///
    /// Per-part is not a weakening: a candidate is caller-proven rc==0 and
    /// unreferenced for its WHOLE span, so releasing a verified sub-range is
    /// exactly what the single path has always done when a cover only partly
    /// matched; an unverifiable part simply stays retired for the next cycle.
    pub(super) fn take_for_reclaim(&mut self, extent: Extent) -> Vec<Extent> {
        let (lo, hi) = self.layout.span(extent);
        let mut taken = Vec::new();
        for idx in lo..=hi {
            if let Some(part) = self.layout.clip(idx, extent) {
                if self.shard_mut(idx).take_for_reclaim(part) {
                    taken.push(part);
                }
            }
        }
        taken
    }

    pub(super) fn reinsert(&mut self, extent: Extent) {
        let (lo, hi) = self.layout.span(extent);
        for idx in lo..=hi {
            if let Some(part) = self.layout.clip(idx, extent) {
                self.shard_mut(idx).reinsert(part);
            }
        }
    }

    /// Blocks of `range` covered by retired extents. Retired extents never
    /// overlap each other (coalesced set, and shards partition the address
    /// space), so summing clamped intersections is exact.
    pub(super) fn overlap_blocks(&self, range: Extent) -> u64 {
        let (lo, hi) = self.layout.span(range);
        let mut covered = 0u64;
        for idx in lo..=hi {
            let Some(part) = self.layout.clip(idx, range) else {
                continue;
            };
            let (s, e) = (part.start.0, part.end_pba().0);
            let shard = self.shard(idx);
            // The last extent starting at/before `s` may reach into the range.
            if let Some(prev) = shard.set.range(..=Extent::single(part.start)).next_back() {
                covered += prev.end_pba().0.min(e).saturating_sub(s);
            }
            for ext in shard.set.range(Extent::single(Pba(s + 1))..) {
                if ext.start.0 >= e {
                    break;
                }
                covered += ext.end_pba().0.min(e) - ext.start.0;
            }
        }
        covered
    }
}
