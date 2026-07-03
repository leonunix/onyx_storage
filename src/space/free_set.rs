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
}

impl FreeSet {
    pub(crate) fn new() -> Self {
        Self {
            by_addr: BTreeSet::new(),
            by_size: BTreeSet::new(),
        }
    }

    /// Build from an already-built address set (rebuild_from_metadata path).
    pub(crate) fn from_addr_set(by_addr: BTreeSet<Extent>) -> Self {
        let by_size = by_addr.iter().map(|e| (e.count, e.start.0)).collect();
        Self { by_addr, by_size }
    }

    /// Read-only view for the overlap/covering/range logic that predates the
    /// side index and must stay byte-for-byte identical.
    pub(crate) fn by_addr(&self) -> &BTreeSet<Extent> {
        &self.by_addr
    }

    pub(crate) fn len(&self) -> usize {
        self.by_addr.len()
    }

    /// Plain insert (no coalescing) — for split remainders that are never
    /// adjacent to another free extent by construction.
    pub(crate) fn insert(&mut self, extent: Extent) {
        let inserted = self.by_addr.insert(extent);
        debug_assert!(inserted, "FreeSet::insert: duplicate start {}", extent.start.0);
        self.by_size.insert((extent.count, extent.start.0));
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
    pub(crate) fn first_fit_aligned(&self, need: u32, stripe: u32, phase: u32) -> Option<Extent> {
        debug_assert!(stripe > 1 && need > 0 && need.is_multiple_of(stripe));
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
    /// SAME extent as the literal old linear-scan code, and the side index
    /// must stay a perfect mirror.
    #[test]
    fn shadow_equivalence_random_traffic() {
        let mut rng = StdRng::seed_from_u64(0x000a_110c_a70e);
        for round in 0..8 {
            let mut fs = FreeSet::new();
            let mut shadow: BTreeSet<Extent> = BTreeSet::new();
            // Seed: one big run, mimicking a fresh allocator.
            let seed = Extent::new(Pba(8), 100_000);
            fs.insert(seed);
            shadow.insert(seed);
            let mut allocated: Vec<Extent> = Vec::new();

            for _ in 0..4_000 {
                // Queries first — every one must agree with the oracle.
                let k = rng.gen_range(1..=64u32);
                assert_eq!(fs.first_fit(k), shadow_first_fit(&shadow, k), "round {round} k {k}");
                let stripe = [2u32, 6, 384][rng.gen_range(0..3)];
                let phase = rng.gen_range(0..stripe);
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
