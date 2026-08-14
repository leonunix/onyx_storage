//! Address-region sharding.
//!
//! The load-bearing invariant is that **a region only ever holds extents it
//! owns**. Everything else composes from it: an overlap or containment
//! question about an extent is answered by asking exactly the regions it
//! spans, so the sharded pool gives the same answers the single pool did.
//! `region_sharded_traffic_preserves_block_ownership` checks it at the block
//! level after every kind of traffic, because a violation here is a
//! double-ownership bug — the class that produced this project's two
//! premature-free P0s.
use std::collections::HashSet;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::*;

const STRIPE: u32 = 6;
const PHASE: u32 = (RESERVED_BLOCKS % STRIPE as u64) as u32;
/// Small enough to bitmap, big enough that `RegionLayout::plan` shards it.
const DEVICE_BLOCKS: u64 = 16_392;

fn sharded(lanes: usize, regions: usize) -> SpaceAllocator {
    // `new_with_exact_regions`, not `new_with_regions`: the region count must
    // be exactly what the test asks for even while the whole suite is being
    // swept with `ONYX_ALLOCATOR_REGIONS`.
    let allocator =
        SpaceAllocator::new_with_exact_regions(DEVICE_BLOCKS * BLOCK_SIZE as u64, lanes, regions);
    allocator.set_stripe_geometry(STRIPE, PHASE);
    assert!(
        allocator.region_count() > 1,
        "this test needs a sharded pool, got {} region(s)",
        allocator.region_count()
    );
    allocator
}

fn layout_of(allocator: &SpaceAllocator) -> RegionLayout {
    allocator.regions.layout()
}

/// Free runs a region currently holds, across all three policy classes.
fn region_runs(allocator: &SpaceAllocator, idx: usize) -> Vec<Extent> {
    let pools = allocator.test_region_pools(idx);
    let mut runs: Vec<Extent> = pools.general.by_addr().iter().copied().collect();
    runs.extend(pools.stripe_reserve.by_addr().iter().copied());
    runs.extend(
        pools
            .quarantines
            .values()
            .flat_map(|target| target.free_parts.by_addr().iter().copied()),
    );
    runs.sort_unstable_by_key(|extent| extent.start.0);
    runs
}

/// THE invariant: no region holds an address it does not own. A violation
/// silently breaks every overlap query, because `lock_span` would not even
/// take the lock of the region actually holding the extent.
fn assert_region_containment(allocator: &SpaceAllocator) {
    let layout = layout_of(allocator);
    for idx in 0..allocator.region_count() {
        let (lo, hi) = (layout.start(idx), layout.end(idx));
        for run in region_runs(allocator, idx) {
            assert!(
                run.start.0 >= lo && run.end_pba().0 <= hi,
                "region {idx} [{lo},{hi}) holds out-of-range run {run:?}"
            );
        }
    }
}

/// Retired extents and age entries a shard currently holds.
fn shard_contents(allocator: &SpaceAllocator, idx: usize) -> (Vec<Extent>, Vec<(u64, u32)>) {
    let shard = allocator.lock_retired_shard(RetiredLockSite::Audit, idx);
    (
        shard.set.iter().copied().collect(),
        shard.age.iter().map(|(&k, run)| (k, run.count)).collect(),
    )
}

/// THE retired-side invariant, the mirror of [`assert_region_containment`]: no
/// shard holds an address it does not own. A violation silently breaks every
/// containment query, because `lock_retired_span` would not even take the lock
/// of the shard actually holding the extent — the double-ownership class that
/// produced this project's two premature-free P0s.
fn assert_retired_containment(allocator: &SpaceAllocator) {
    let layout = layout_of(allocator);
    for idx in 0..allocator.retired.count() {
        let (lo, hi) = (layout.start(idx), layout.end(idx));
        let (set, age) = shard_contents(allocator, idx);
        for extent in set {
            assert!(
                extent.start.0 >= lo && extent.end_pba().0 <= hi,
                "retired shard {idx} [{lo},{hi}) holds out-of-range {extent:?}"
            );
        }
        for (start, count) in age {
            assert!(
                start >= lo && start + u64::from(count) <= hi,
                "age shard {idx} [{lo},{hi}) holds out-of-range run {start}+{count}"
            );
        }
    }
}

/// The allocator's own retired set must cover exactly the blocks the caller
/// believes it retired — sharding must not lose or duplicate one.
fn assert_retired_matches(allocator: &SpaceAllocator, expected: &[Extent]) {
    let mut want: Vec<u64> = expected
        .iter()
        .flat_map(|e| (0..u64::from(e.count)).map(move |o| e.start.0 + o))
        .collect();
    want.sort_unstable();
    let mut got: Vec<u64> = (0..allocator.retired.count())
        .flat_map(|idx| shard_contents(allocator, idx).0)
        .flat_map(|e| (0..u64::from(e.count)).map(move |o| e.start.0 + o))
        .collect();
    got.sort_unstable();
    assert_eq!(got, want, "retired coverage diverged from the expected set");
}

/// Every usable block is in EXACTLY ONE state: free in a region's pools,
/// parked in a lane cache (still logically free), live, or retired.
fn assert_block_ownership(allocator: &SpaceAllocator, live: &[Extent], retired: &[Extent]) {
    let total = allocator.total_block_count();
    let mut owner: Vec<u8> = vec![0; total as usize];
    let mut claim = |extent: Extent, tag: u8, what: &str| {
        for offset in 0..extent.count {
            let pba = extent.start.0 + offset as u64;
            assert!(
                pba >= RESERVED_BLOCKS && pba < total,
                "{what} {extent:?} escapes the usable range"
            );
            assert_eq!(
                owner[pba as usize], 0,
                "pba {pba} claimed twice: already {} now {what}",
                owner[pba as usize]
            );
            owner[pba as usize] = tag;
        }
    };
    for idx in 0..allocator.region_count() {
        for run in region_runs(allocator, idx) {
            claim(run, 1, "free");
        }
    }
    for cache in &allocator.lane_caches {
        for &pba in cache.lock().unwrap().iter() {
            claim(Extent::single(pba), 2, "lane block cache");
        }
    }
    for cache in &allocator.lane_extent_caches {
        for &extent in cache.lock().unwrap().iter() {
            claim(extent, 2, "lane extent cache");
        }
    }
    for &extent in live {
        claim(extent, 3, "live");
    }
    for &extent in retired {
        claim(extent, 4, "retired");
    }
    let unclaimed = (RESERVED_BLOCKS..total)
        .filter(|&pba| owner[pba as usize] == 0)
        .count();
    assert_eq!(unclaimed, 0, "{unclaimed} usable blocks belong to nobody");
    // Counter closure: retiring does not change the allocated total, so
    // free + allocated must still cover the whole usable range.
    assert_eq!(
        allocator.free_block_count() + allocator.allocated_block_count(),
        total - RESERVED_BLOCKS,
        "free + allocated no longer covers the device"
    );
}

/// Region boundaries must be stripe-aligned and stripe-multiple sized. If
/// they were not, each boundary would strand up to `stripe - 1` blocks in the
/// general pool instead of the reserve, quietly leaking aligned capacity at
/// every one of the (2048 on the box) seams.
#[test]
fn region_boundaries_are_stripe_aligned_after_geometry_is_known() {
    let allocator = sharded(2, 4);
    let layout = layout_of(allocator_ref(&allocator));
    assert_eq!(layout.blocks % u64::from(STRIPE), 0, "blocks per region");
    for idx in 0..allocator.region_count() {
        let start = layout.start(idx);
        if idx == 0 {
            continue; // region 0 starts at 0 and owns the reserved prefix
        }
        assert_eq!(
            (start + u64::from(PHASE)) % u64::from(STRIPE),
            0,
            "region {idx} starts at {start}, which is not stripe-aligned"
        );
    }
    // Routing is total and monotone: consecutive PBAs never move backwards.
    let mut previous = 0usize;
    for pba in (0..DEVICE_BLOCKS).step_by(97) {
        let idx = layout.of(pba);
        assert!(idx >= previous && idx < allocator.region_count());
        previous = idx;
    }
}

fn allocator_ref(allocator: &SpaceAllocator) -> &SpaceAllocator {
    allocator
}

/// Re-planning the layout in `set_stripe_geometry` must not lose or duplicate
/// a single free block, because it moves extents between regions.
#[test]
fn geometry_replan_reroutes_every_block_exactly_once() {
    let allocator = SpaceAllocator::new_with_exact_regions(DEVICE_BLOCKS * BLOCK_SIZE as u64, 2, 4);
    let before = allocator.contiguity_stats().free_blocks_in_set;
    assert_eq!(before, DEVICE_BLOCKS - RESERVED_BLOCKS);
    allocator.set_stripe_geometry(STRIPE, PHASE);
    assert_eq!(
        allocator.contiguity_stats().free_blocks_in_set,
        before,
        "re-layout changed the free total"
    );
    assert_region_containment(&allocator);
    assert_block_ownership(&allocator, &[], &[]);
    // Idempotent: a second call with the same geometry must be a no-op.
    allocator.set_stripe_geometry(STRIPE, PHASE);
    assert_eq!(allocator.contiguity_stats().free_blocks_in_set, before);
    assert_eq!(allocator.stripe_geometry(), Some((STRIPE, PHASE)));
}

/// A free run released across a boundary is split, so each region keeps only
/// its own slice — yet every read-side query must still see one free range.
#[test]
fn a_release_across_a_boundary_splits_but_still_reads_as_free() {
    let allocator = sharded(0, 4);
    let layout = layout_of(&allocator);
    let boundary = layout.end(0);
    // Straddle the boundary by one stripe on each side.
    let straddle = Extent::new(Pba(boundary - u64::from(STRIPE)), 2 * STRIPE);
    // Take it out of the pool first (exact-width, no lanes → global path).
    let mut held = Vec::new();
    while let Ok(extent) = allocator.allocate_extent(STRIPE) {
        held.push(extent);
        if extent.end_pba().0 > straddle.end_pba().0 {
            break;
        }
    }
    assert!(
        held.iter()
            .any(|e| e.start.0 <= straddle.start.0 && e.end_pba().0 >= straddle.end_pba().0)
            || held.len() > 1,
        "the straddling range must have been allocated away"
    );
    // Free everything back and confirm the boundary range reads as free from
    // every angle.
    for extent in held {
        allocator.free_extent(extent).unwrap();
    }
    assert!(allocator.is_extent_free(straddle));
    assert_eq!(
        allocator.free_overlap_blocks(straddle),
        u64::from(straddle.count)
    );
    for offset in 0..straddle.count {
        assert!(allocator.is_free(Pba(straddle.start.0 + offset as u64)));
    }
    // ...and that it is genuinely split: neither region holds the whole thing.
    assert_region_containment(&allocator);
    assert!(
        region_runs(&allocator, 0)
            .iter()
            .all(|run| run.end_pba().0 <= boundary),
        "region 0 must not reach past the boundary"
    );
}

/// The one deliberate selection change: a lane refills from its own region
/// and only moves when that region can no longer serve it.
#[test]
fn a_lane_refills_from_one_region_until_it_starves() {
    let allocator = sharded(2, 4);
    let layout = layout_of(&allocator);
    let mut first_region = None;
    let mut seen = HashSet::new();
    for _ in 0..64 {
        let extent = allocator
            .allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
            .expect("fresh pool serves a stripe");
        let idx = layout.of(extent.start.0);
        seen.insert(idx);
        if first_region.is_none() {
            first_region = Some(idx);
        }
    }
    assert_eq!(
        seen.len(),
        1,
        "one lane's consecutive stripe allocations should stay in one region, saw {seen:?}"
    );
    let (switches_before, _) = {
        let stats = allocator.region_stats();
        (stats.switches, stats.refill_misses)
    };
    // Drain the lane's whole region, then confirm it moves rather than failing.
    let mut extents = Vec::new();
    while let Ok(extent) = allocator.allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE) {
        extents.push(extent);
        if layout.of(extent.start.0) != first_region.unwrap() {
            break;
        }
    }
    let stats = allocator.region_stats();
    assert!(
        stats.switches > switches_before,
        "the lane never switched region"
    );
    assert!(
        extents
            .last()
            .is_some_and(|e| layout.of(e.start.0) != first_region.unwrap()),
        "the lane never left its exhausted region"
    );
    assert_region_containment(&allocator);
}

/// `storage.stripe_refill_run_stripes` on the shape that ships: 2048 regions,
/// the LOW regions aged into pinned single-stripe windows and intact material
/// only higher up. The wide pass has to MIGRATE the lane, because
/// `stripe_hint` (largest reserve run) is what selects a region, and the low
/// regions' hint is one stripe.
///
/// Without the migration the knob would be a no-op in production — the
/// unsharded tests cannot see this.
#[test]
fn a_wide_refill_migrates_the_lane_to_a_region_with_intact_material() {
    const WIDE: u32 = 8;
    let allocator = sharded(2, 4);
    let layout = layout_of(&allocator);
    // Claim everything, then hand back pinned windows in region 0 and one
    // intact run in region 2.
    let mut held = Vec::new();
    while let Ok(extent) = allocator.allocate_extent(u32::MAX) {
        held.push(extent);
    }
    let low_base = SpaceAllocator::align_up_pba(RESERVED_BLOCKS, STRIPE as u64, PHASE as u64);
    for i in 0..16u64 {
        allocator
            .free_extent(Extent::new(
                Pba(low_base + i * 2 * u64::from(STRIPE)),
                STRIPE,
            ))
            .unwrap();
    }
    let intact_start = SpaceAllocator::align_up_pba(
        layout.start(2) + u64::from(STRIPE),
        STRIPE as u64,
        PHASE as u64,
    );
    let intact = Extent::new(Pba(intact_start), WIDE * STRIPE);
    allocator.free_extent(intact).unwrap();
    assert_eq!(layout.of(intact.start.0), 2, "fixture must seed region 2");

    allocator.set_stripe_refill_run_stripes(WIDE);
    // Pin the lane to region 0 first, the way steady-state writing would.
    allocator.lane_regions[0].store(0, Ordering::Relaxed);
    let first = allocator
        .allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
        .expect("intact material exists");
    assert_eq!(
        first.start.0, intact.start.0,
        "the wide pass must leave region 0's pinned windows for region 2's run"
    );
    let supply = allocator.supply_stats();
    assert_eq!(supply.wide_hits, 1);
    assert_eq!(supply.wide_misses, 0);
    // And the rest of the run follows contiguously out of the lane cache.
    for i in 1..WIDE as u64 {
        let next = allocator
            .allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
            .unwrap();
        assert_eq!(next.start.0, intact.start.0 + i * u64::from(STRIPE));
    }
    assert_eq!(allocator.supply_stats().refills, 1);
    assert_region_containment(&allocator);
    drop(held);
}

/// Two lanes should end up in different regions — the exclusivity that makes
/// sharding worth anything (ZFS's metaslab result: the win is ownership, not
/// contiguity).
#[test]
fn distinct_lanes_prefer_distinct_regions() {
    let allocator = sharded(4, 4);
    let layout = layout_of(&allocator);
    let mut regions = HashSet::new();
    for lane in 0..4 {
        let extent = allocator
            .allocate_stripe_extent_for_lane(lane, STRIPE, STRIPE, PHASE)
            .expect("fresh pool serves a stripe");
        regions.insert(layout.of(extent.start.0));
    }
    assert!(
        regions.len() > 1,
        "all four lanes landed in the same region ({regions:?}); \
             claiming is not taking effect"
    );
}

/// Real defrag targets are exactly one stripe and region boundaries are
/// stripe-aligned, so a target can never straddle one. The API still accepts
/// wider aligned extents, and those must be refused rather than silently
/// split across two locks (which would break completion accounting).
#[test]
fn a_cross_region_quarantine_is_refused_and_changes_nothing() {
    let allocator = sharded(0, 4);
    let boundary = layout_of(&allocator).end(0);
    let straddle = Extent::new(Pba(boundary - u64::from(STRIPE)), 2 * STRIPE);
    assert_eq!(
        (straddle.start.0 + u64::from(PHASE)) % u64::from(STRIPE),
        0,
        "the probe must be stripe-aligned or it is rejected for the wrong reason"
    );
    let free_before = allocator.contiguity_stats();
    let error = allocator
        .begin_defrag_quarantine(straddle)
        .expect_err("a cross-region target must be refused");
    assert!(
        format!("{error}").contains("region boundary"),
        "unexpected rejection: {error}"
    );
    let after = allocator.contiguity_stats();
    assert_eq!(after.free_blocks_in_set, free_before.free_blocks_in_set);
    assert_eq!(after.quarantine_target_blocks, 0);
    assert_region_containment(&allocator);

    // A one-stripe target inside a region still works end to end. The target
    // has to be LIVE first: `begin_defrag_quarantine` refuses a range that
    // overlaps the stripe reserve, and on a fresh pool every aligned block is
    // in the reserve.
    let inside = allocator
        .allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
        .unwrap();
    assert_eq!(layout_of(&allocator).span(inside).0, 0);
    allocator.begin_defrag_quarantine(inside).unwrap();
    assert!(allocator.is_defrag_quarantined(inside));
    assert_eq!(
        allocator.defrag_quarantine_progress(inside.start),
        Some((0, u64::from(STRIPE))),
        "a fully-live target has evacuated nothing yet"
    );
    // Freeing inside an active quarantine must route into that quarantine's
    // free parts, not back into the region's reserve — the routing decision
    // and the region lock are the same critical section.
    allocator.free_extent(inside).unwrap();
    assert_eq!(
        allocator.defrag_quarantine_progress(inside.start),
        Some((u64::from(STRIPE), u64::from(STRIPE)))
    );
    assert!(allocator.complete_defrag_quarantine(inside.start).unwrap());
    assert!(!allocator.is_defrag_quarantined(inside));
    assert_eq!(
        allocator.contiguity_stats().free_blocks_in_set,
        free_before.free_blocks_in_set
    );
    assert_region_containment(&allocator);
}

/// An ENOSPC verdict must never rest on an advisory hint. The final attempt
/// walks every region, so even a hint forced to a stale zero cannot make a
/// pool with space look empty.
#[test]
fn enospc_never_rests_on_a_stale_region_hint() {
    let allocator = sharded(0, 4);
    // Drain every region except the last, then lie about the last one.
    let mut held = Vec::new();
    let last = allocator.region_count() - 1;
    let layout = layout_of(&allocator);
    while let Ok(extent) = allocator.allocate_extent(STRIPE) {
        if layout.of(extent.start.0) == last {
            allocator.free_extent(extent).unwrap();
            break;
        }
        held.push(extent);
    }
    allocator.regions.free_hint[last].store(0, Ordering::Relaxed);
    allocator.regions.stripe_hint[last].store(0, Ordering::Relaxed);
    // Exact-width, no lanes: `take_exact_regionwise` only, so nothing but the
    // hint stands between this call and the last region. (`allocate_extent`
    // would mask the point by falling back to the largest short fragment.)
    let extent = allocator
        .allocate_exact_extent_for_lane(0, STRIPE)
        .expect("the forced-stale hint must not cause a spurious ENOSPC");
    assert_eq!(layout.of(extent.start.0), last);
    allocator.free_extent(extent).unwrap();
    for extent in held {
        allocator.free_extent(extent).unwrap();
    }
}

/// Arming the A/B serialization gate must change TIMING only. If it changed
/// results, an A/B run using it would be comparing two different allocators.
#[test]
fn the_serialization_gate_does_not_change_results() {
    for serialize in [false, true] {
        set_region_serialize(serialize);
        let allocator = sharded(2, 4);
        let mut extents = Vec::new();
        for lane in 0..2 {
            for _ in 0..32 {
                extents.push(
                    allocator
                        .allocate_stripe_extent_for_lane(lane, STRIPE, STRIPE, PHASE)
                        .unwrap(),
                );
            }
        }
        assert_eq!(allocator.region_stats().serialized, serialize);
        assert_block_ownership(&allocator, &extents, &[]);
        let now = Instant::now();
        let (newly, failed) = allocator.retire_extents_batch(&extents, now);
        assert!(failed.is_empty(), "batch retire failed: {failed:?}");
        assert_eq!(
            newly,
            extents.iter().map(|e| u64::from(e.count)).sum::<u64>()
        );
        assert_block_ownership(&allocator, &[], &extents);
        let running = AtomicBool::new(true);
        let (freed, count) = allocator
            .reclaim_retired_extents_batch(&extents, &running)
            .unwrap();
        assert_eq!(count, extents.len());
        assert_eq!(freed, newly);
        assert_block_ownership(&allocator, &[], &[]);
        assert_region_containment(&allocator);
    }
    set_region_serialize(false);
}

/// `region_holds` is what keeps the batch paths' `free -> retired -> age`
/// order intact while shortening the hold. With one region it must reproduce
/// `chunks(cap)` exactly, or the unsharded arm of any A/B is not the old
/// behaviour.
#[test]
fn region_holds_degenerates_to_plain_chunks_when_unsharded() {
    let extents: Vec<Extent> = (0..10)
        .map(|i| Extent::new(Pba(RESERVED_BLOCKS + i * 100), 4))
        .collect();
    let grouped = region_holds(RegionLayout::single(), &extents, 4);
    let plain: Vec<&[Extent]> = extents.chunks(4).collect();
    assert_eq!(grouped.len(), plain.len());
    for ((lo, hi, slice), expected) in grouped.iter().zip(plain) {
        assert_eq!((*lo, *hi), (0, 0));
        assert_eq!(*slice, expected);
    }
    // Sharded, a group breaks at every region boundary as well as at `cap`.
    let layout = RegionLayout {
        base: RESERVED_BLOCKS,
        blocks: 128,
        count: 8,
    };
    let grouped = region_holds(layout, &extents, 4);
    assert!(
        grouped.len() > plain_len(&extents, 4),
        "sharded grouping must be at least as fine as chunks(cap)"
    );
    for (lo, hi, slice) in grouped {
        assert!(!slice.is_empty() && slice.len() <= 4);
        for extent in slice {
            assert_eq!(layout.span(*extent), (lo, hi));
        }
    }
}

fn plain_len(extents: &[Extent], cap: usize) -> usize {
    extents.chunks(cap).count()
}

/// Mixed traffic across every ownership-changing path, checked at the block
/// level. This is the test that would catch a routing mistake handing the
/// same block out twice.
#[test]
fn region_sharded_traffic_preserves_block_ownership() {
    const LANES: usize = 4;
    let allocator = sharded(LANES, 4);
    let mut rng = StdRng::seed_from_u64(0x5eed_7e91_a110_c8);
    let mut live: Vec<Extent> = Vec::new();
    let mut retired: Vec<Extent> = Vec::new();

    for round in 0..3_000usize {
        match rng.gen_range(0..100u32) {
            0..=49 => {
                let lane = rng.gen_range(0..LANES);
                let data = rng.gen_range(1..=2 * STRIPE);
                if let Ok(extent) =
                    allocator.allocate_stripe_extent_for_lane(lane, data, STRIPE, PHASE)
                {
                    assert_eq!(
                        (extent.start.0 + u64::from(PHASE)) % u64::from(STRIPE),
                        0,
                        "aligned allocation lost its alignment"
                    );
                    assert!(extent.count.is_multiple_of(STRIPE));
                    live.push(extent);
                }
            }
            50..=59 => {
                let lane = rng.gen_range(0..LANES);
                let count = rng.gen_range(1..=8u32);
                if let Ok(extent) = allocator.allocate_extent_for_lane(lane, count) {
                    live.push(extent);
                }
            }
            60..=64 => {
                let lane = rng.gen_range(0..LANES);
                if let Ok(pba) = allocator.allocate_one_for_lane(lane) {
                    live.push(Extent::single(pba));
                }
            }
            65..=79 => {
                if !live.is_empty() {
                    let extent = live.swap_remove(rng.gen_range(0..live.len()));
                    allocator
                        .free_extent(extent)
                        .unwrap_or_else(|e| panic!("free {extent:?}: {e}"));
                }
            }
            80..=91 => {
                if !live.is_empty() {
                    let extent = live.swap_remove(rng.gen_range(0..live.len()));
                    allocator
                        .retire_extent(extent)
                        .unwrap_or_else(|e| panic!("retire {extent:?}: {e}"));
                    retired.push(extent);
                }
            }
            _ => {
                if !retired.is_empty() {
                    let extent = retired.swap_remove(rng.gen_range(0..retired.len()));
                    assert!(
                        allocator.reclaim_retired_extent(extent).unwrap(),
                        "reclaim of {extent:?} found it not retired"
                    );
                }
            }
        }
        if round % 250 == 0 {
            assert_region_containment(&allocator);
            assert_retired_containment(&allocator);
            assert_retired_matches(&allocator, &retired);
            assert_block_ownership(&allocator, &live, &retired);
        }
    }

    assert_region_containment(&allocator);
    assert_retired_containment(&allocator);
    assert_retired_matches(&allocator, &retired);
    assert_block_ownership(&allocator, &live, &retired);
    // Unwind fully: everything must be reclaimable back to a whole free pool.
    let running = AtomicBool::new(true);
    allocator
        .reclaim_retired_extents_batch(
            &{
                retired.sort_unstable_by_key(|extent| extent.start.0);
                retired.clone()
            },
            &running,
        )
        .unwrap();
    retired.clear();
    live.sort_unstable_by_key(|extent| extent.start.0);
    let (_, failed) = allocator.free_extents_batch(&live);
    assert!(failed.is_empty(), "batch free failed: {failed:?}");
    live.clear();
    allocator.drain_lane_caches();
    assert_block_ownership(&allocator, &[], &[]);
    assert_eq!(
        allocator.contiguity_stats().free_blocks_in_set,
        DEVICE_BLOCKS - RESERVED_BLOCKS,
        "the whole device must be free again"
    );
    assert_eq!(allocator.allocated_block_count(), 0);
}

/// Single-block first-fit must be BYTE-IDENTICAL sharded or not.
///
/// A one-block request can never straddle a region boundary, so there is no
/// escape hatch here: if the ascending-region walk were not exactly global
/// first-fit-by-address, this diverges immediately. That is the property the
/// metadb L2P leaf codec's dense-PBA contract rests on for every non-lane
/// allocation.
#[test]
fn single_block_first_fit_is_identical_sharded_or_not() {
    let single = SpaceAllocator::new_with_exact_regions(DEVICE_BLOCKS * BLOCK_SIZE as u64, 0, 1);
    let many = SpaceAllocator::new_with_exact_regions(DEVICE_BLOCKS * BLOCK_SIZE as u64, 0, 4);
    single.set_stripe_geometry(STRIPE, PHASE);
    many.set_stripe_geometry(STRIPE, PHASE);
    assert_eq!(single.region_count(), 1);
    assert!(many.region_count() > 1);

    let mut rng = StdRng::seed_from_u64(0xf1f5_7f17);
    let mut held: Vec<Pba> = Vec::new();
    for _ in 0..4_000 {
        if rng.gen_bool(0.7) || held.is_empty() {
            let a = single.allocate_one();
            let b = many.allocate_one();
            match (a, b) {
                (Ok(a), Ok(b)) => {
                    assert_eq!(a, b, "sharded walk is not global first-fit");
                    held.push(a);
                }
                (Err(_), Err(_)) => {}
                (a, b) => panic!("divergent outcome: {a:?} vs {b:?}"),
            }
        } else {
            let pba = held.swap_remove(rng.gen_range(0..held.len()));
            single.free_one(pba).unwrap();
            many.free_one(pba).unwrap();
        }
        assert_eq!(single.free_block_count(), many.free_block_count());
    }
    assert_eq!(
        single.contiguity_stats().free_blocks_in_set,
        many.contiguity_stats().free_blocks_in_set
    );
}

/// The retired set + age log sharded must answer EXACTLY what one shard
/// answers, over a mixed retire/reclaim/free/query sequence.
///
/// This is the retired-side `region_pools_equal_single_pool`: the only thing
/// sharding is allowed to change is which mutex an address lives behind, never
/// which blocks are retired, which are reclaimable, or what a query returns.
/// A divergence here is a double-ownership bug, not a performance regression.
#[test]
fn retired_shards_equal_single_shard() {
    const GRACE: Duration = Duration::from_secs(10);
    let single = SpaceAllocator::new_with_exact_regions(DEVICE_BLOCKS * BLOCK_SIZE as u64, 0, 1);
    let many = SpaceAllocator::new_with_exact_regions(DEVICE_BLOCKS * BLOCK_SIZE as u64, 0, 4);
    single.set_stripe_geometry(STRIPE, PHASE);
    many.set_stripe_geometry(STRIPE, PHASE);
    assert_eq!(single.retired.count(), 1);
    assert!(many.retired.count() > 1);

    let t0 = Instant::now();
    let mut rng = StdRng::seed_from_u64(0x4e71_2ed0);
    // Build the SAME live set on both by allocating single blocks — the one
    // request width that is byte-identical sharded or not (multi-block
    // first-fit is deliberately allowed to differ at a region seam, see
    // `region_walk_picks_the_lowest_address_run_that_fits`), then grouping
    // consecutive PBAs into multi-block extents so boundary-straddling
    // extents really do occur.
    let mut pbas: Vec<u64> = Vec::new();
    for _ in 0..3_000 {
        match (single.allocate_one(), many.allocate_one()) {
            (Ok(a), Ok(b)) => {
                assert_eq!(a, b, "single-block allocation diverged");
                pbas.push(a.0);
            }
            (Err(_), Err(_)) => break,
            (a, b) => panic!("divergent allocation: {a:?} vs {b:?}"),
        }
    }
    pbas.sort_unstable();
    let mut live: Vec<Extent> = Vec::new();
    let mut i = 0;
    while i < pbas.len() {
        let want = rng.gen_range(1..=3 * STRIPE) as usize;
        let mut n = 1;
        while n < want && i + n < pbas.len() && pbas[i + n] == pbas[i + n - 1] + 1 {
            n += 1;
        }
        live.push(Extent::new(Pba(pbas[i]), n as u32));
        i += n;
    }
    let mut retired: Vec<Extent> = Vec::new();

    for round in 0..4_000usize {
        // Same op, same extent, on both allocators.
        match rng.gen_range(0..100u32) {
            0..=44 => {
                if !live.is_empty() {
                    let extent = live.swap_remove(rng.gen_range(0..live.len()));
                    let at = t0 + Duration::from_millis(round as u64);
                    assert_eq!(
                        single.retire_extent_at(extent, at).unwrap(),
                        many.retire_extent_at(extent, at).unwrap(),
                        "newly-retired count diverged for {extent:?}"
                    );
                    retired.push(extent);
                }
            }
            45..=59 => {
                if !live.is_empty() {
                    let extent = live.swap_remove(rng.gen_range(0..live.len()));
                    assert_eq!(
                        single.free_extent(extent).is_ok(),
                        many.free_extent(extent).is_ok(),
                        "free outcome diverged for {extent:?}"
                    );
                }
            }
            60..=89 => {
                if !retired.is_empty() {
                    let extent = retired.swap_remove(rng.gen_range(0..retired.len()));
                    assert_eq!(
                        single.reclaim_retired_extent(extent).unwrap(),
                        many.reclaim_retired_extent(extent).unwrap(),
                        "reclaim outcome diverged for {extent:?}"
                    );
                }
            }
            _ => {
                // Selector: the emitted candidate SET (not the extent
                // boundaries — sharding legitimately splits at seams) plus the
                // deferred total must match.
                let now = t0 + Duration::from_millis(round as u64) + GRACE;
                let (ca, da) = single.aged_candidates(64, GRACE, now);
                let (cb, db) = many.aged_candidates(64, GRACE, now);
                assert_eq!(da, db, "deferred_blocks diverged");
                assert_eq!(
                    blocks_of(&ca),
                    blocks_of(&cb),
                    "aged candidate coverage diverged"
                );
            }
        }
        if round % 200 == 0 {
            assert_retired_containment(&many);
            assert_eq!(
                retired_blocks_of(&single),
                retired_blocks_of(&many),
                "retired coverage diverged at round {round}"
            );
            assert_eq!(single.retired_block_count(), many.retired_block_count());
            assert_eq!(single.free_block_count(), many.free_block_count());
            for extent in &live {
                assert_eq!(
                    single.is_retired(extent.start),
                    many.is_retired(extent.start)
                );
            }
        }
    }
    assert_retired_containment(&many);
    assert_eq!(retired_blocks_of(&single), retired_blocks_of(&many));
    assert_eq!(
        single.retired_block_count_exact(),
        many.retired_block_count_exact()
    );
}

fn blocks_of(extents: &[Extent]) -> Vec<u64> {
    let mut out: Vec<u64> = extents
        .iter()
        .flat_map(|e| (0..u64::from(e.count)).map(move |o| e.start.0 + o))
        .collect();
    out.sort_unstable();
    out
}

fn retired_blocks_of(allocator: &SpaceAllocator) -> Vec<u64> {
    let all: Vec<Extent> = (0..allocator.retired.count())
        .flat_map(|idx| shard_contents(allocator, idx).0)
        .collect();
    blocks_of(&all)
}

/// `aged_candidates` releases its shard lock every [`AGED_SCAN_SLICE`] entries
/// and resumes from a PBA cursor. The resume must be exact: a retired set and
/// an age log both several slices deep have to produce the same answer a
/// single-hold walk would, with every expired age entry pruned.
///
/// This is the correctness half of the fix for the box-measured 1.169 s
/// per-cycle monopoly on the retired lock.
#[test]
fn aged_candidates_resumes_across_slices() {
    const GRACE: Duration = Duration::from_secs(10);
    // Several slices' worth of single-block retired extents, plus enough age
    // entries to force the prune pass to slice too.
    let n = (2 * AGED_SCAN_SLICE + 37) as u64;
    let young_from = n - 500;
    let dev = (2 * n + RESERVED_BLOCKS + 64) * BLOCK_SIZE as u64;
    let a = SpaceAllocator::new_with_exact_regions(dev, 0, 4);
    let base = Instant::now();
    let now = base + Duration::from_secs(100);
    {
        let layout = a.retired_layout();
        let mut shards = a.lock_all_retired(RetiredLockSite::Setup);
        for i in 0..n {
            let pba = RESERVED_BLOCKS + 2 * i; // stride 2 → n separate extents
            let shard = &mut shards[layout.of(pba)];
            shard.set.insert(Extent::single(Pba(pba)));
            // Every block gets an age entry; the tail is still young, so it
            // must be withheld and counted as deferred, and the rest must be
            // pruned by the sliced prune pass.
            shard.age.insert(
                pba,
                RetiredRun {
                    count: 1,
                    retired_at: if i >= young_from { now } else { base },
                },
            );
        }
    }
    a.allocated_blocks.store(2 * n, Ordering::Relaxed);
    a.retired_blocks.store(n, Ordering::Relaxed);

    // Budget above the aged population: everything aged must come out, in
    // ascending address order, and nothing young may.
    let (cands, deferred) = a.aged_candidates(4 * AGED_SCAN_SLICE, GRACE, now);
    assert_eq!(deferred, n - young_from, "young blocks must be deferred");
    let got = blocks_of(&cands);
    let want: Vec<u64> = (0..young_from).map(|i| RESERVED_BLOCKS + 2 * i).collect();
    assert_eq!(got, want, "sliced walk lost or reordered candidates");
    // The prune pass must have run to the END of every shard's age log, not
    // just as far as the emit budget reached.
    let left: usize = (0..a.retired.count())
        .map(|idx| shard_contents(&a, idx).1.len())
        .sum();
    assert_eq!(left as u64, n - young_from, "expired age entries survived");

    // A budget that stops mid-walk must emit exactly the lowest addresses.
    let (capped, _) = a.aged_candidates(100, GRACE, now);
    assert_eq!(
        blocks_of(&capped),
        (0..100u64)
            .map(|i| RESERVED_BLOCKS + 2 * i)
            .collect::<Vec<_>>()
    );
}

/// Multi-block first-fit over the sharded set, against an oracle that scans
/// every region's runs in address order.
///
/// ⚠ This also pins the ONE thing sharding costs: a run is never coalesced
/// across a region boundary, so a request wider than a region's tail run
/// cannot be served from the seam and moves to the next region — where the
/// unsharded pool would have served it from the merged run. That is at most
/// one un-mergeable seam per region (≤ 2048 against the box's 24.6 M
/// extents), and it is why this test compares against a region-aware oracle
/// instead of against the single pool.
#[test]
fn region_walk_picks_the_lowest_address_run_that_fits() {
    let allocator = sharded(0, 4);
    let layout = layout_of(&allocator);
    let mut rng = StdRng::seed_from_u64(0x0dd_f17);
    let mut held: Vec<Extent> = Vec::new();

    let oracle = |need: u32| -> Option<Extent> {
        (0..allocator.region_count())
            .flat_map(|idx| region_runs(&allocator, idx))
            .find(|run| run.count >= need)
    };

    for _ in 0..800 {
        if rng.gen_bool(0.65) || held.is_empty() {
            let need = rng.gen_range(1..=24u32);
            let expected = oracle(need);
            match allocator.allocate_exact_extent_for_lane(0, need) {
                Ok(extent) => {
                    let run = expected.expect("allocation succeeded where the oracle saw none");
                    assert_eq!(
                        extent.start, run.start,
                        "picked {extent:?} but the lowest fitting run was {run:?}"
                    );
                    assert_eq!(extent.count, need);
                    assert_eq!(
                        layout.span(extent),
                        (layout.of(extent.start.0), layout.of(extent.start.0)),
                        "an allocation must never straddle a region boundary"
                    );
                    held.push(extent);
                }
                Err(OnyxError::SpaceExhausted) => {
                    assert!(
                        expected.is_none(),
                        "reported ENOSPC while {expected:?} could serve {need}"
                    );
                }
                Err(error) => panic!("unexpected error: {error}"),
            }
        } else {
            let extent = held.swap_remove(rng.gen_range(0..held.len()));
            allocator.free_extent(extent).unwrap();
        }
    }
    assert_region_containment(&allocator);
}

/// Growth lands in the top region without a re-layout, because the last
/// region is unbounded above.
#[test]
fn grow_capacity_lands_in_the_top_region() {
    let allocator = sharded(0, 4);
    let before = allocator.contiguity_stats().free_blocks_in_set;
    let regions_before = allocator.region_count();
    let grown = allocator
        .grow_capacity((DEVICE_BLOCKS + 4_096) * BLOCK_SIZE as u64)
        .unwrap();
    assert_eq!(grown, DEVICE_BLOCKS + 4_096);
    assert_eq!(allocator.region_count(), regions_before, "no re-layout");
    assert_eq!(
        allocator.contiguity_stats().free_blocks_in_set,
        before + 4_096
    );
    assert_region_containment(&allocator);
    let layout = layout_of(&allocator);
    assert_eq!(
        layout.of(DEVICE_BLOCKS + 4_095),
        regions_before - 1,
        "grown tail must route into the last region"
    );
}

/// A device too small to shard usefully must stay single-region rather than
/// producing thousands of tiny pools.
#[test]
fn a_small_device_refuses_to_shard() {
    let tiny = SpaceAllocator::new_with_exact_regions(1_024 * BLOCK_SIZE as u64, 1, 2_048);
    assert_eq!(tiny.region_count(), 1);
    assert_eq!(tiny.region_blocks(), 0);
    assert!(RegionLayout::plan(4_000, 2_048, 6).is_none());
    // Just over two minimum-sized regions does shard.
    let (blocks, count) = RegionLayout::plan(2 * MIN_REGION_BLOCKS + 10, 2_048, 6).unwrap();
    assert_eq!(blocks % 6, 0);
    assert!(count > 1);
}
