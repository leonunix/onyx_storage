//! Dual-index free-extent set: the address-ordered `BTreeSet<Extent>` stays the
//! authority (every overlap/covering/first-by-address query is unchanged), plus
//! a `(count, start)` side index so "lowest-address extent with count >= K"
//! (first-fit-by-address) is O(D·log N) instead of an O(N) walk of the whole
//! fragment belt under the global free lock (D = distinct extent sizes >= K
//! present).
//!
//! ⚠ SELECTION MUST STAY FIRST-FIT-BY-ADDRESS. The side index is used only to
//! *find* the lowest-address candidate faster — never to prefer a
//! smallest-sufficient (best-fit) extent. A size-indexed best-fit
//! deterministically corrupts the metadb L2P leaf codec (it assumes dense,
//! mostly-sequential PBAs; 2026-05-29 A/B: 3/3 best-fit runs broke
//! `compact_in_place`, 0/3 first-fit). The shadow property tests in this file
//! pin query-for-query equivalence with the plain linear scan.

use std::collections::BTreeSet;
use std::ops::Bound;

use crate::space::extent::Extent;
use crate::types::Pba;

pub(crate) struct FreeSet {
    /// Authority. `Extent`'s `Ord` is start-only, so iteration is by address.
    by_addr: BTreeSet<Extent>,
    /// Side index mirroring `by_addr` exactly: one `(count, start)` per extent.
    /// Starts are unique (at most one extent per start in `by_addr`), so the
    /// pairs are unique too.
    by_size: BTreeSet<(u32, u64)>,
    /// Engine RAID geometry `(stripe_blocks, phase)`, fixed at startup. When
    /// set (stripe > 1), `by_eff` is maintained and `first_fit_aligned` for
    /// THIS geometry is a size-class cursor jump instead of a scan.
    geom: Option<(u32, u32)>,
    /// Effective-aligned-capacity index for the configured geometry: one
    /// `(count - head_pad(start), start)` per extent (saturating at 0). An
    /// extent can host an aligned `need`-carve iff `eff >= need`, so
    /// "lowest-address hosting extent" is the same cursor-jump argmin as
    /// [`Self::first_fit`] — this kills the fragmented-belt pathology where
    /// millions of misaligned tight fragments were linearly slack-checked
    /// under the global free lock (65 ms/call at 3M fragments, box-measured
    /// as multi-second front-end stalls).
    by_eff: BTreeSet<(u32, u64)>,
    /// Σ count over `by_addr` — free blocks IN THIS SET (excludes lane-cached
    /// extents, which are drained out; the global `free_blocks` atomic counts
    /// those too, so ratios must use this field for a consistent denominator).
    blocks_total: u64,
    /// Σ floor(eff_count / stripe) * stripe over `by_addr` — WHOLE-stripe
    /// aligned capacity for the configured geometry; 0 when `geom` is None.
    /// The stripe-floor matters: a confetti belt of 3-block fragments has
    /// nonzero raw eff (eff = count − head_pad) but can host zero stripe
    /// writes, so summing raw eff would overstate capability ~33% and mask
    /// the defrag trigger. `stripe_capacity / blocks_total` = fraction of the
    /// free pool usable by stripe-aligned carves.
    stripe_capacity: u64,
}

impl FreeSet {
    pub(crate) fn new() -> Self {
        Self {
            by_addr: BTreeSet::new(),
            by_size: BTreeSet::new(),
            geom: None,
            by_eff: BTreeSet::new(),
            blocks_total: 0,
            stripe_capacity: 0,
        }
    }

    /// eff floored to whole stripes — this extent's usable aligned capacity.
    fn stripe_floor(eff: u32, stripe: u32) -> u64 {
        (eff / stripe) as u64 * stripe as u64
    }

    /// Build from an already-built address set (rebuild_from_metadata path).
    /// Keeps the previously-configured geometry (rebuild happens after
    /// startup wiring).
    pub(crate) fn from_addr_set(by_addr: BTreeSet<Extent>) -> Self {
        let by_size = by_addr.iter().map(|e| (e.count, e.start.0)).collect();
        let blocks_total = by_addr.iter().map(|e| e.count as u64).sum();
        Self {
            by_addr,
            by_size,
            geom: None,
            by_eff: BTreeSet::new(),
            blocks_total,
            stripe_capacity: 0,
        }
    }

    /// Configure the engine's fixed RAID geometry and (re)build the
    /// effective-capacity index. Idempotent; `stripe <= 1` clears it.
    pub(crate) fn set_geometry(&mut self, stripe: u32, phase: u32) {
        if stripe <= 1 {
            self.geom = None;
            self.by_eff.clear();
            self.stripe_capacity = 0;
            return;
        }
        if self.geom == Some((stripe, phase)) && self.by_eff.len() == self.by_addr.len() {
            // by_eff (and therefore stripe_capacity) is maintained whenever
            // geom is set, so both are already correct here.
            debug_assert_eq!(
                self.stripe_capacity,
                self.by_eff
                    .iter()
                    .map(|&(eff, _)| Self::stripe_floor(eff, stripe))
                    .sum::<u64>()
            );
            return;
        }
        self.geom = Some((stripe, phase));
        let mut capacity = 0u64;
        self.by_eff = self
            .by_addr
            .iter()
            .map(|e| {
                let eff = Self::eff_count(*e, stripe, phase);
                capacity += Self::stripe_floor(eff, stripe);
                (eff, e.start.0)
            })
            .collect();
        self.stripe_capacity = capacity;
    }

    pub(crate) fn geometry(&self) -> Option<(u32, u32)> {
        self.geom
    }

    /// Blocks usable for an aligned carve starting at/after `align_up(start)`:
    /// `count - head_pad(start)`, saturating at 0.
    fn eff_count(e: Extent, stripe: u32, phase: u32) -> u32 {
        let head = head_pad(e.start.0, stripe as u64, phase as u64);
        e.count.saturating_sub(head as u32)
    }

    /// Read-only view for the overlap/covering/range logic that predates the
    /// side index and must stay byte-for-byte identical.
    pub(crate) fn by_addr(&self) -> &BTreeSet<Extent> {
        &self.by_addr
    }

    pub(crate) fn len(&self) -> usize {
        self.by_addr.len()
    }

    /// Σ count over the set — O(1) maintained aggregate (excludes lane caches).
    pub(crate) fn blocks_total(&self) -> u64 {
        self.blocks_total
    }

    /// Whole-stripe aligned capacity for the configured geometry — O(1)
    /// maintained aggregate; 0 when no geometry is set.
    pub(crate) fn stripe_capacity(&self) -> u64 {
        self.stripe_capacity
    }

    /// Plain insert (no coalescing) — for split remainders that are never
    /// adjacent to another free extent by construction.
    pub(crate) fn insert(&mut self, extent: Extent) {
        let inserted = self.by_addr.insert(extent);
        debug_assert!(inserted, "FreeSet::insert: duplicate start {}", extent.start.0);
        self.by_size.insert((extent.count, extent.start.0));
        self.blocks_total += extent.count as u64;
        if let Some((stripe, phase)) = self.geom {
            let eff = Self::eff_count(extent, stripe, phase);
            self.by_eff.insert((eff, extent.start.0));
            self.stripe_capacity += Self::stripe_floor(eff, stripe);
        }
        debug_assert_eq!(self.by_addr.len(), self.by_size.len());
    }

    /// Remove an extent. The probe is matched by START (that is `Extent`'s
    /// `Ord`); the by_size entry is removed using the count of the extent
    /// actually stored, so a count-mismatched probe can never leave a stale
    /// `(count, start)` behind (a stale entry would let `first_fit` hand out a
    /// ghost extent → double allocation).
    pub(crate) fn remove(&mut self, extent: &Extent) -> bool {
        match self.by_addr.take(extent) {
            Some(taken) => {
                debug_assert_eq!(
                    taken.count, extent.count,
                    "FreeSet::remove: probe count mismatch at start {}",
                    extent.start.0
                );
                let removed = self.by_size.remove(&(taken.count, taken.start.0));
                debug_assert!(removed, "FreeSet: by_size missing ({}, {})", taken.count, taken.start.0);
                self.blocks_total -= taken.count as u64;
                if let Some((stripe, phase)) = self.geom {
                    let eff = Self::eff_count(taken, stripe, phase);
                    let removed_eff = self.by_eff.remove(&(eff, taken.start.0));
                    debug_assert!(removed_eff, "FreeSet: by_eff missing start {}", taken.start.0);
                    self.stripe_capacity -= Self::stripe_floor(eff, stripe);
                }
                debug_assert_eq!(self.by_addr.len(), self.by_size.len());
                true
            }
            None => false,
        }
    }

    /// Lowest-address extent (any size).
    pub(crate) fn first(&self) -> Option<Extent> {
        self.by_addr.iter().next().copied()
    }

    /// FIRST-FIT-BY-ADDRESS: the lowest-address extent with `count >=
    /// min_count`. Exactly equivalent to
    /// `by_addr.iter().find(|e| e.count >= min_count)` — both compute the
    /// minimum start over the same candidate set — but via a cursor jump over
    /// the distinct size classes in `by_size` (each class's first entry is that
    /// class's lowest address) instead of walking the fragment belt.
    pub(crate) fn first_fit(&self, min_count: u32) -> Option<Extent> {
        let mut best: Option<(u64, u32)> = None;
        let mut lower = Bound::Included((min_count, 0u64));
        while let Some(&(count, start)) = self.by_size.range((lower, Bound::Unbounded)).next() {
            if best.is_none_or(|(bs, _)| start < bs) {
                best = Some((start, count));
            }
            // Skip the rest of this size class. Exclusive bound on (count,
            // u64::MAX) instead of (count + 1, 0): count == u32::MAX is legal
            // (rebuild splits >16 TiB gaps into u32::MAX extents) and must not
            // overflow.
            lower = Bound::Excluded((count, u64::MAX));
        }
        best.map(|(start, count)| Extent::new(Pba(start), count))
    }

    /// FIRST-FIT-BY-ADDRESS over the stripe-carve predicate: the
    /// lowest-address extent that can host an aligned `need`-block carve
    /// (`need` a multiple of `stripe`, alignment `(pba + phase) % stripe == 0`).
    ///
    /// Equivalent to `by_addr.iter().find(|e| carve_aligned_from_run(e, need,
    /// stripe, phase).is_some())`. Carve succeeds iff
    /// `count >= need + head(start)` with `head ∈ [0, stripe-1]`, so the
    /// hosting set is exactly:
    ///   - every extent with `count >= need + stripe - 1` (hosts regardless of
    ///     start) — found via `first_fit`; this branch is correctness-required,
    ///     not an optimization (the no-lane direct path and the lock-release
    ///     window between a failed refill and this fallback can both see big
    ///     runs), and
    ///   - extents with `count ∈ [need, need+stripe-2]` whose start passes the
    ///     slack check — per size class the slack bound is fixed and starts
    ///     ascend, so the first passing entry is that class's argmin.
    ///
    /// The answer is the address-argmin over both.
    ///
    /// When `(stripe, phase)` matches the configured geometry this is a pure
    /// cursor jump over `by_eff` (hosting ⟺ `eff >= need`) — O(D·log N) even
    /// when the belt is millions of misaligned tight fragments. The
    /// slack-check scan below remains as the geometry-mismatch fallback
    /// (tests / hypothetical multi-geometry callers): identical selection,
    /// but O(class size) on a hostless fragmented belt — never let a hot path
    /// take it (the 2026-07-03 oscillation was exactly this scan at 65 ms/call
    /// under the global free lock).
    pub(crate) fn first_fit_aligned(&self, need: u32, stripe: u32, phase: u32) -> Option<Extent> {
        debug_assert!(stripe > 1 && need > 0 && need.is_multiple_of(stripe));
        if self.geom == Some((stripe, phase)) {
            // Cursor jump over distinct eff classes >= need; each class's
            // first entry is that class's lowest address; argmin over classes.
            let mut best: Option<(u64, u32)> = None;
            let mut lower = Bound::Included((need, 0u64));
            while let Some(&(eff, start)) = self.by_eff.range((lower, Bound::Unbounded)).next() {
                if best.is_none_or(|(bs, _)| start < bs) {
                    best = Some((start, eff));
                }
                lower = Bound::Excluded((eff, u64::MAX));
            }
            return best.map(|(start, _)| {
                // Return the stored extent (count from by_addr, not eff).
                *self
                    .by_addr
                    .get(&Extent::single(Pba(start)))
                    .expect("by_eff start must exist in by_addr")
            });
        }

        let mut best: Option<Extent> = self.first_fit(need.saturating_add(stripe - 1));
        // Tight classes: count in [need, need+stripe-2]. Guard the upper bound
        // against u32 overflow (need close to u32::MAX).
        let tight_end = need.saturating_add(stripe - 1); // exclusive
        for count in need..tight_end {
            // Within one class the max hostable head is fixed: head <= count - need.
            let max_head = (count - need) as u64;
            for &(_, start) in self
                .by_size
                .range((count, 0u64)..(count, u64::MAX))
            {
                if best.is_some_and(|b| b.start.0 <= start) {
                    // Every later entry in this class has a larger start; no
                    // improvement possible here.
                    break;
                }
                let head = head_pad(start, stripe as u64, phase as u64);
                if head <= max_head {
                    best = Some(Extent::new(Pba(start), count));
                    break; // first passing entry is this class's argmin
                }
            }
        }
        best
    }

    /// Largest extent. Matches the old `iter().max_by_key(|e| e.count)`
    /// last-wins tie semantics: `by_size.last()` is (max count, max start).
    pub(crate) fn largest(&self) -> Option<Extent> {
        self.by_size
            .last()
            .map(|&(count, start)| Extent::new(Pba(start), count))
    }

    /// Insert an extent and merge it with the adjacent-before/after free runs
    /// (identical semantics to the old `coalesce_and_insert`).
    pub(crate) fn coalesce_insert(&mut self, new: Extent) {
        let mut merged_start = new.start.0;
        let mut merged_end = new.end_pba().0;

        let before = self.by_addr.range(..=new).next_back().copied();
        if let Some(extent) = before {
            if extent.end_pba().0 == merged_start {
                merged_start = extent.start.0;
                self.remove(&extent);
            }
        }

        let probe = Extent::new(Pba(merged_end), 0);
        let after = self.by_addr.range(probe..).next().copied();
        if let Some(extent) = after {
            if extent.start.0 == merged_end {
                merged_end = extent.end_pba().0;
                self.remove(&extent);
            }
        }

        self.insert(Extent::new(Pba(merged_start), (merged_end - merged_start) as u32));
    }

    /// Test-only raw insert that keeps both indexes in sync — for tests that
    /// deliberately inject a free/retired inconsistency.
    #[cfg(test)]
    pub(crate) fn insert_for_test(&mut self, extent: Extent) {
        self.insert(extent);
    }

    /// Test-only full cross-index verification.
    #[cfg(test)]
    pub(crate) fn assert_consistent(&self) {
        assert_eq!(self.by_addr.len(), self.by_size.len());
        for e in &self.by_addr {
            assert!(
                self.by_size.contains(&(e.count, e.start.0)),
                "by_size missing ({}, {})",
                e.count,
                e.start.0
            );
        }
        assert_eq!(
            self.blocks_total,
            self.by_addr.iter().map(|e| e.count as u64).sum::<u64>(),
            "blocks_total aggregate drifted"
        );
        if let Some((stripe, phase)) = self.geom {
            assert_eq!(self.by_addr.len(), self.by_eff.len());
            for e in &self.by_addr {
                assert!(
                    self.by_eff
                        .contains(&(Self::eff_count(*e, stripe, phase), e.start.0)),
                    "by_eff missing start {}",
                    e.start.0
                );
            }
            assert_eq!(
                self.stripe_capacity,
                self.by_addr
                    .iter()
                    .map(|e| Self::stripe_floor(Self::eff_count(*e, stripe, phase), stripe))
                    .sum::<u64>(),
                "stripe_capacity aggregate drifted"
            );
        } else {
            assert!(self.by_eff.is_empty());
            assert_eq!(self.stripe_capacity, 0, "stripe_capacity must be 0 without geometry");
        }
    }
}

/// Blocks below the first aligned PBA at/after `start`
/// (`(pba + phase) % stripe == 0`). Mirrors `SpaceAllocator::align_up_pba`.
fn head_pad(start: u64, stripe: u64, phase: u64) -> u64 {
    let r = (start + phase) % stripe;
    if r == 0 {
        0
    } else {
        stripe - r
    }
}

#[cfg(test)]
mod tests {
    //! Selection-identity pins. The load-bearing invariant is that every query
    //! answers EXACTLY what the old linear scan over the address-ordered set
    //! answered — first-fit-by-address, never best-fit (best-fit corrupts the
    //! metadb L2P leaf codec; see the module doc).
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    /// The literal pre-FreeSet selection code, used as the oracle.
    fn shadow_first_fit(shadow: &BTreeSet<Extent>, k: u32) -> Option<Extent> {
        shadow.iter().find(|e| e.count >= k).copied()
    }

    fn shadow_hosts(e: Extent, need: u32, stripe: u32, phase: u32) -> bool {
        // carve_aligned_from_run's success predicate.
        let aligned = {
            let r = (e.start.0 + phase as u64) % stripe as u64;
            if r == 0 { e.start.0 } else { e.start.0 + (stripe as u64 - r) }
        };
        aligned + need as u64 <= e.end_pba().0
    }

    fn shadow_first_fit_aligned(
        shadow: &BTreeSet<Extent>,
        need: u32,
        stripe: u32,
        phase: u32,
    ) -> Option<Extent> {
        shadow
            .iter()
            .find(|e| shadow_hosts(**e, need, stripe, phase))
            .copied()
    }

    fn shadow_largest(shadow: &BTreeSet<Extent>) -> Option<Extent> {
        shadow.iter().max_by_key(|e| e.count).copied()
    }

    /// FIRST-FIT pin: a lower-address big run must win over a higher-address
    /// tighter fit. This is the single test that fails if anyone ever
    /// "improves" the query to best-fit.
    #[test]
    fn first_fit_is_not_best_fit() {
        let mut fs = FreeSet::new();
        fs.insert(Extent::new(Pba(100), 50));
        fs.insert(Extent::new(Pba(200), 10));
        assert_eq!(fs.first_fit(10), Some(Extent::new(Pba(100), 50)));
    }

    #[test]
    fn first_fit_fail_fast_and_edges() {
        let mut fs = FreeSet::new();
        assert_eq!(fs.first_fit(1), None);
        fs.insert(Extent::new(Pba(10), 4));
        fs.insert(Extent::new(Pba(20), 6));
        assert_eq!(fs.first_fit(7), None, "no run big enough");
        assert_eq!(fs.first_fit(5), Some(Extent::new(Pba(20), 6)));
        assert_eq!(fs.first_fit(1), Some(Extent::new(Pba(10), 4)));
    }

    /// Maintained aggregates (blocks_total / stripe_capacity) must track every
    /// mutation path: insert, remove, coalesce_insert, set_geometry (build,
    /// idempotent early-return, re-geometry, clear), from_addr_set, and the
    /// u32::MAX extent edge. stripe_capacity floors eff to whole stripes —
    /// sub-stripe fragments contribute ZERO (they can host no stripe write).
    #[test]
    fn aggregates_track_all_mutation_paths() {
        const STRIPE: u32 = 6;
        const PHASE: u32 = 2;
        let mut fs = FreeSet::new();
        assert_eq!((fs.blocks_total(), fs.stripe_capacity()), (0, 0));

        // Insert/remove without geometry: stripe_capacity stays 0.
        fs.insert(Extent::new(Pba(10), 8));
        fs.insert(Extent::new(Pba(100), 5));
        assert_eq!((fs.blocks_total(), fs.stripe_capacity()), (13, 0));
        fs.assert_consistent();

        // Geometry build: Pba(10): (10+2)%6=0 → head 0 → eff 8 → floor 6.
        // Pba(100): (100+2)%6=0 → head 0 → eff 5 → floor 0 (sub-stripe!).
        fs.set_geometry(STRIPE, PHASE);
        assert_eq!(fs.stripe_capacity(), 6);
        // Idempotent early-return keeps them intact.
        fs.set_geometry(STRIPE, PHASE);
        assert_eq!((fs.blocks_total(), fs.stripe_capacity()), (13, 6));

        // Misaligned insert: Pba(30): (30+2)%6=2 → head 4 → eff 4 → floor 0.
        fs.insert(Extent::new(Pba(30), 8));
        assert_eq!((fs.blocks_total(), fs.stripe_capacity()), (21, 6));
        fs.assert_consistent();

        // coalesce_insert exercises remove + reinsert internally.
        fs.remove(&Extent::new(Pba(30), 8));
        fs.insert(Extent::new(Pba(18), 5));
        fs.coalesce_insert(Extent::new(Pba(23), 4)); // merges with [18,23)
        // Sets now: [10,18) eff 8→6, [18,27) head 4 → eff 5→0, [100,105) eff 5→0.
        assert_eq!(fs.blocks_total(), 8 + 9 + 5);
        assert_eq!(fs.stripe_capacity(), 6);
        fs.assert_consistent();

        // Re-geometry (different stripe) rebuilds the sum:
        // [10,18): head_pad(10,4,0)=2 → eff 6 → floor 4;
        // [18,27): head 2 → eff 7 → floor 4; [100,105): head 0 → eff 5 → floor 4.
        fs.set_geometry(4, 0);
        assert_eq!(fs.stripe_capacity(), 12);
        fs.assert_consistent();
        // Clearing geometry zeroes stripe_capacity.
        fs.set_geometry(1, 0);
        assert_eq!(fs.stripe_capacity(), 0);
        fs.assert_consistent();

        // u32::MAX extents must not overflow the u64 sums.
        let mut fs = FreeSet::from_addr_set(
            [
                Extent::new(Pba(0), u32::MAX),
                Extent::new(Pba(u32::MAX as u64), u32::MAX),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(fs.blocks_total(), 2 * u32::MAX as u64);
        fs.set_geometry(STRIPE, PHASE);
        fs.assert_consistent();
    }

    /// count == u32::MAX is legal (rebuild splits >16 TiB gaps); the size-class
    /// cursor jump must not overflow.
    #[test]
    fn first_fit_handles_u32_max_counts() {
        let mut fs = FreeSet::new();
        fs.insert(Extent::new(Pba(0), u32::MAX));
        fs.insert(Extent::new(Pba(u32::MAX as u64), u32::MAX));
        fs.insert(Extent::new(Pba(3 * u32::MAX as u64), 5));
        assert_eq!(fs.first_fit(u32::MAX), Some(Extent::new(Pba(0), u32::MAX)));
        assert_eq!(fs.first_fit(6), Some(Extent::new(Pba(0), u32::MAX)));
        assert_eq!(fs.first_fit(1), Some(Extent::new(Pba(0), u32::MAX)));
        fs.assert_consistent();
    }

    /// largest() must keep `max_by_key`'s last-wins tie: highest address among
    /// the maximal counts.
    #[test]
    fn largest_matches_max_by_key_tie_semantics() {
        let mut fs = FreeSet::new();
        let mut shadow = BTreeSet::new();
        for e in [
            Extent::new(Pba(10), 8),
            Extent::new(Pba(30), 8),
            Extent::new(Pba(50), 3),
        ] {
            fs.insert(e);
            shadow.insert(e);
        }
        assert_eq!(fs.largest(), shadow_largest(&shadow));
        assert_eq!(fs.largest(), Some(Extent::new(Pba(30), 8)));
    }

    /// A tight-but-aligned fragment at a lower address must beat a big run at a
    /// higher address, and vice versa — pure address order over the hosting set.
    #[test]
    fn first_fit_aligned_address_order_over_both_branches() {
        const STRIPE: u32 = 6;
        const PHASE: u32 = 2; // aligned starts: (pba+2)%6==0 -> 4,10,16,...
        // Tight aligned fragment low, big run high.
        let mut fs = FreeSet::new();
        fs.insert(Extent::new(Pba(10), 6)); // aligned, exactly need
        fs.insert(Extent::new(Pba(100), 40)); // big
        assert_eq!(
            fs.first_fit_aligned(6, STRIPE, PHASE),
            Some(Extent::new(Pba(10), 6))
        );
        // Big run low, tight aligned fragment high.
        let mut fs = FreeSet::new();
        fs.insert(Extent::new(Pba(11), 40)); // big, misaligned start (hosts anyway)
        fs.insert(Extent::new(Pba(100), 6)); // aligned tight
        assert_eq!(
            fs.first_fit_aligned(6, STRIPE, PHASE),
            Some(Extent::new(Pba(11), 40))
        );
        // Misaligned tight fragment can't host; the aligned one further up wins.
        let mut fs = FreeSet::new();
        fs.insert(Extent::new(Pba(11), 6)); // tight, misaligned -> can't host
        fs.insert(Extent::new(Pba(16), 6)); // tight, aligned
        assert_eq!(
            fs.first_fit_aligned(6, STRIPE, PHASE),
            Some(Extent::new(Pba(16), 6))
        );
        assert_eq!(fs.first_fit_aligned(12, STRIPE, PHASE), None);
    }

    /// Shadow property test: random alloc/free traffic applied to FreeSet and
    /// a plain `BTreeSet<Extent>` in lockstep; every query must select the
    /// SAME extent as the literal old linear-scan code, and the side indexes
    /// must stay perfect mirrors. Even rounds configure the engine geometry
    /// (exercising the `by_eff` fast path); odd rounds leave it unset and
    /// query random geometries (exercising the slack-scan fallback).
    #[test]
    fn shadow_equivalence_random_traffic() {
        let mut rng = StdRng::seed_from_u64(0x000a_110c_a70e);
        for round in 0..8usize {
            let mut fs = FreeSet::new();
            let mut shadow: BTreeSet<Extent> = BTreeSet::new();
            // Seed: one big run, mimicking a fresh allocator.
            let seed = Extent::new(Pba(8), 100_000);
            fs.insert(seed);
            shadow.insert(seed);
            let fixed_geom = if round % 2 == 0 {
                let stripe = [2u32, 6, 384][(round / 2) % 3];
                let phase = (round as u32 * 7) % stripe;
                fs.set_geometry(stripe, phase);
                Some((stripe, phase))
            } else {
                None
            };
            let mut allocated: Vec<Extent> = Vec::new();

            for op in 0..4_000 {
                if op % 512 == 0 {
                    fs.assert_consistent(); // includes aggregate recompute
                }
                // Queries first — every one must agree with the oracle.
                let k = rng.gen_range(1..=64u32);
                assert_eq!(fs.first_fit(k), shadow_first_fit(&shadow, k), "round {round} k {k}");
                let (stripe, phase) = fixed_geom.unwrap_or_else(|| {
                    let s = [2u32, 6, 384][rng.gen_range(0..3)];
                    (s, rng.gen_range(0..s))
                });
                let need = stripe * rng.gen_range(1..=2u32);
                assert_eq!(
                    fs.first_fit_aligned(need, stripe, phase),
                    shadow_first_fit_aligned(&shadow, need, stripe, phase),
                    "round {round} need {need} stripe {stripe} phase {phase}"
                );
                assert_eq!(fs.largest(), shadow_largest(&shadow));
                assert_eq!(fs.first(), shadow.iter().next().copied());

                // Mutation: alloc (front-carve a first-fit hit) or free one back.
                if rng.gen_bool(0.55) {
                    let want = rng.gen_range(1..=32u32);
                    if let Some(e) = fs.first_fit(want) {
                        assert_eq!(Some(e), shadow_first_fit(&shadow, want));
                        fs.remove(&e);
                        shadow.remove(&e);
                        let take = rng.gen_range(1..=e.count.min(want));
                        allocated.push(Extent::new(e.start, take));
                        if e.count > take {
                            let rest = Extent::new(Pba(e.start.0 + take as u64), e.count - take);
                            fs.insert(rest);
                            shadow.insert(rest);
                        }
                    }
                } else if !allocated.is_empty() {
                    let e = allocated.swap_remove(rng.gen_range(0..allocated.len()));
                    fs.coalesce_insert(e);
                    // Oracle coalesce: the literal old coalesce_and_insert.
                    let mut s = e.start.0;
                    let mut end = e.end_pba().0;
                    if let Some(b) = shadow.range(..=e).next_back().copied() {
                        if b.end_pba().0 == s {
                            s = b.start.0;
                            shadow.remove(&b);
                        }
                    }
                    if let Some(a) = shadow.range(Extent::new(Pba(end), 0)..).next().copied() {
                        if a.start.0 == end {
                            end = a.end_pba().0;
                            shadow.remove(&a);
                        }
                    }
                    shadow.insert(Extent::new(Pba(s), (end - s) as u32));
                }
            }
            assert_eq!(fs.by_addr(), &shadow, "round {round} end-state");
            fs.assert_consistent();
        }
    }
}

#[cfg(test)]
mod starve_bench {
    use super::*;

    /// Reproduce the fragmented-belt stall state: millions of MISALIGNED
    /// tight-class fragments (sizes 6..=10 with phase-mismatched starts), NO
    /// run >= need+stripe-1. Measures one first_fit_aligned call.
    /// Run: cargo test --release --lib bench_first_fit_aligned_starved -- --ignored --nocapture
    #[test]
    #[ignore = "perf microbench"]
    fn bench_first_fit_aligned_starved() {
        const STRIPE: u32 = 6;
        const PHASE: u32 = 2; // aligned starts: (s+2)%6==0
        let mut fs = FreeSet::new();
        // 3M fragments, stride 16, sizes cycling 6..=10, starts arranged so
        // (start+PHASE)%STRIPE != 0 AND align_up(start)+6 > end (never hosts).
        let mut n = 0u64;
        let mut start = 8u64;
        while n < 3_000_000 {
            let count = 6 + (n % 5) as u32; // 6..=10
            // choose start with head_pad > count-6 => cannot host aligned 6
            let mut s = start;
            loop {
                let head = { let r = (s + PHASE as u64) % STRIPE as u64; if r == 0 { 0 } else { STRIPE as u64 - r } };
                if head > (count - 6) as u64 { break; }
                s += 1;
            }
            fs.insert(Extent::new(Pba(s), count));
            start = s + count as u64 + 8; // gap so no coalescing/hosting
            n += 1;
        }
        let t = std::time::Instant::now();
        let r = fs.first_fit_aligned(6, STRIPE, PHASE);
        let el = t.elapsed();
        println!("SCAN fallback over 3M misaligned tight fragments: {:?} result={:?}", el, r);
        assert!(r.is_none());
        // and the healthy comparison: one aligned fragment near the front
        fs.insert(Extent::new(Pba(4), 6)); // (4+2)%6==0 aligned
        let t = std::time::Instant::now();
        let r2 = fs.first_fit_aligned(6, STRIPE, PHASE);
        println!("SCAN with an aligned fragment at low address: {:?} result={:?}", t.elapsed(), r2);
        fs.remove(&Extent::new(Pba(4), 6));

        // Now with the engine geometry configured: the by_eff cursor jump.
        let t = std::time::Instant::now();
        fs.set_geometry(STRIPE, PHASE);
        println!("set_geometry index build over 3M extents: {:?}", t.elapsed());
        let t = std::time::Instant::now();
        let r3 = fs.first_fit_aligned(6, STRIPE, PHASE);
        println!("EFF-INDEX starved miss: {:?} result={:?}", t.elapsed(), r3);
        assert!(r3.is_none());
        fs.insert(Extent::new(Pba(4), 6));
        let t = std::time::Instant::now();
        let r4 = fs.first_fit_aligned(6, STRIPE, PHASE);
        println!("EFF-INDEX with aligned fragment: {:?} result={:?}", t.elapsed(), r4);
        assert_eq!(r4, Some(Extent::new(Pba(4), 6)));
    }
}
