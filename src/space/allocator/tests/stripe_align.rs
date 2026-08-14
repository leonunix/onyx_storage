//! RAID6 full-stripe-aligned allocation: `(pba + phase) % stripe == 0`,
//! length a whole number of stripes, lowest-address dense, no free-list
//! bloat, and `stripe <= 1` identical to the plain path.
use super::*;

// 6+2 RAID6 at a 4 KiB strip = 6-block stripe; RESERVED_BLOCKS=8 => phase 2.
const STRIPE: u32 = 6;
const PHASE: u32 = (RESERVED_BLOCKS % STRIPE as u64) as u32;

fn new_alloc_lanes(blocks: u64, lanes: usize) -> SpaceAllocator {
    SpaceAllocator::new(blocks * BLOCK_SIZE as u64, lanes)
}

#[test]
fn align_up_pba_table() {
    // phase 2: aligned pbas satisfy (pba+2)%6==0 => pba in {4,10,16,22,...}
    assert_eq!(SpaceAllocator::align_up_pba(8, 6, 2), 10);
    assert_eq!(SpaceAllocator::align_up_pba(11, 6, 2), 16);
    assert_eq!(SpaceAllocator::align_up_pba(4, 6, 2), 4); // already aligned
    assert_eq!(SpaceAllocator::align_up_pba(10, 6, 2), 10);
    assert_eq!(SpaceAllocator::align_up_pba(5, 6, 2), 10);
    // phase 0: multiples of stripe
    assert_eq!(SpaceAllocator::align_up_pba(7, 6, 0), 12);
    assert_eq!(SpaceAllocator::align_up_pba(12, 6, 0), 12);
    // stripe<=1 is identity
    assert_eq!(SpaceAllocator::align_up_pba(13, 1, 0), 13);
}

#[test]
fn round_up_blocks_table() {
    assert_eq!(SpaceAllocator::round_up_blocks(1, 6), 6);
    assert_eq!(SpaceAllocator::round_up_blocks(6, 6), 6);
    assert_eq!(SpaceAllocator::round_up_blocks(7, 6), 12);
    assert_eq!(SpaceAllocator::round_up_blocks(12, 6), 12);
    assert_eq!(SpaceAllocator::round_up_blocks(4, 1), 4);
}

#[test]
fn small_lane_allocations_preserve_cross_pool_first_fit() {
    let allocator = new_alloc_lanes(64, 1);
    let usable = 64 - RESERVED_BLOCKS;
    allocator.allocate_extent(usable as u32).unwrap();
    allocator.set_stripe_geometry(STRIPE, PHASE);
    allocator.free_extent(Extent::new(Pba(10), 12)).unwrap();
    allocator.free_extent(Extent::new(Pba(30), 5)).unwrap();

    for expected in 10..22 {
        let extent = allocator.allocate_extent_for_lane(0, 1).unwrap();
        assert_eq!(extent, Extent::single(Pba(expected)));
    }
    assert_eq!(
        allocator.allocate_extent_for_lane(0, 1).unwrap(),
        Extent::single(Pba(30))
    );
}

#[test]
fn stripe_reserve_miss_reclaims_cold_lane_refill_for_hot_lane() {
    let allocator = new_alloc_lanes(128, 2);
    allocator.set_stripe_geometry(STRIPE, PHASE);

    let cold = allocator
        .allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
        .unwrap();
    assert!(!allocator.lane_extent_caches[0].lock().unwrap().is_empty());

    let hot = allocator
        .allocate_stripe_extent_for_lane(1, STRIPE, STRIPE, PHASE)
        .unwrap();
    assert_ne!(hot, cold);
    assert_eq!((hot.start.0 + u64::from(PHASE)) % u64::from(STRIPE), 0);
    assert_eq!(hot.count, STRIPE);
}

#[test]
fn io_addressable_capacity_reserves_top_for_offset() {
    // The IoEngine writes allocator PBA `p` at device block `p +
    // RESERVED_BLOCKS`. Production builds the allocator with the
    // io-ADDRESSABLE capacity (`phys - RESERVED_BLOCKS` blocks) so the top
    // RESERVED_BLOCKS physical blocks are never targeted. Draining the whole
    // free list must never yield a PBA whose written device block reaches
    // `phys_blocks`. Regression for the chunklet "Raid6 IO out of range:
    // offset == capacity" flush failure.
    let phys_blocks = 64u64;
    let io_addressable = new_alloc_lanes(phys_blocks - RESERVED_BLOCKS, 1);
    let mut max_pba = 0u64;
    while let Ok(pba) = io_addressable.allocate_one_for_lane(0) {
        assert!(
            pba.0 + RESERVED_BLOCKS < phys_blocks,
            "PBA {} + offset {} must stay below physical capacity {}",
            pba.0,
            RESERVED_BLOCKS,
            phys_blocks
        );
        max_pba = max_pba.max(pba.0);
    }
    assert_eq!(max_pba, phys_blocks - RESERVED_BLOCKS - 1, "top usable PBA");
}

#[test]
fn stripe_extent_at_boundary_stays_in_capacity() {
    // A near-full stripe allocation must never return an extent whose top
    // block (+ offset) exceeds the physical device — the whole 24 KiB
    // full-stripe write must land inside it. Build with the io-addressable
    // capacity like production does.
    let phys_blocks = 6 * 20 + 2 * RESERVED_BLOCKS; // room for ~20 stripes
    let alloc = new_alloc_lanes(phys_blocks - RESERVED_BLOCKS, 1);
    let mut got = 0;
    while let Ok(ext) = alloc.allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE) {
        assert!(
            ext.start.0 + ext.count as u64 + RESERVED_BLOCKS <= phys_blocks,
            "stripe [{}, {}) + offset {} exceeds physical capacity {}",
            ext.start.0,
            ext.start.0 + ext.count as u64,
            RESERVED_BLOCKS,
            phys_blocks
        );
        assert_eq!(
            (ext.start.0 + RESERVED_BLOCKS) % STRIPE as u64,
            0,
            "device-aligned"
        );
        got += 1;
    }
    assert!(got > 0, "should allocate at least one boundary stripe");
}

#[test]
fn carve_aligned_from_run_shapes() {
    // run [8, 8+16) need 6 phase 2 => aligned@10, head [8,2), tail [16, ..)
    let run = Extent::new(Pba(8), 16);
    let (aligned, head, tail) =
        SpaceAllocator::carve_aligned_from_run(run, 6, STRIPE, PHASE).unwrap();
    assert_eq!(aligned, Extent::new(Pba(10), 6));
    assert_eq!(head, Some(Extent::new(Pba(8), 2)));
    assert_eq!(tail, Some(Extent::new(Pba(16), 8)));
    // exact aligned run: no head, no tail
    let run = Extent::new(Pba(10), 6);
    let (aligned, head, tail) =
        SpaceAllocator::carve_aligned_from_run(run, 6, STRIPE, PHASE).unwrap();
    assert_eq!(aligned, Extent::new(Pba(10), 6));
    assert_eq!(head, None);
    assert_eq!(tail, None);
    // run too small to host aligned need
    assert!(
        SpaceAllocator::carve_aligned_from_run(Extent::new(Pba(11), 6), 6, STRIPE, PHASE).is_none()
    );
}

#[test]
fn lane_alloc_is_aligned_and_sized() {
    let a = new_alloc_lanes(65_536, 4);
    for data in [1u32, 4, 6, 7, 12] {
        let e = a
            .allocate_stripe_extent_for_lane(0, data, STRIPE, PHASE)
            .unwrap();
        assert_eq!(
            (e.start.0 + PHASE as u64) % STRIPE as u64,
            0,
            "start {} not stripe-aligned",
            e.start.0
        );
        let want = SpaceAllocator::round_up_blocks(data, STRIPE);
        assert_eq!(e.count, want, "data={data} count");
    }
}

#[test]
fn lane_allocs_are_dense_and_disjoint() {
    let a = new_alloc_lanes(65_536, 4);
    let mut seen: Vec<Extent> = Vec::new();
    for _ in 0..500 {
        let e = a
            .allocate_stripe_extent_for_lane(1, 4, STRIPE, PHASE)
            .unwrap();
        assert_eq!(e.count, 6);
        assert_eq!((e.start.0 + PHASE as u64) % STRIPE as u64, 0);
        seen.push(e);
    }
    seen.sort_by_key(|e| e.start.0);
    for w in seen.windows(2) {
        assert!(
            w[0].end_pba().0 <= w[1].start.0,
            "overlap {:?} {:?}",
            w[0],
            w[1]
        );
    }
}

#[test]
fn density_guard_no_freelist_bloat() {
    // 10k aligned allocs must NOT explode the free set into per-alloc
    // slivers (that would blow the metadb L2P leaf unit budget). One lane
    // seeded from one contiguous run => free runs stay O(1).
    let a = new_alloc_lanes(1_000_000, 2);
    for _ in 0..10_000 {
        a.allocate_stripe_extent_for_lane(0, 4, STRIPE, PHASE)
            .unwrap();
    }
    assert!(
        a.free_extent_run_count() < 16,
        "free set fragmented to {} runs",
        a.free_extent_run_count()
    );
}

#[test]
fn stripe_one_matches_plain_path() {
    let a = new_alloc_lanes(4096, 2);
    let e = a.allocate_stripe_extent_for_lane(0, 5, 1, 0).unwrap();
    assert_eq!(e.count, 5, "stripe<=1 must not pad");
}

#[test]
fn padded_extent_frees_whole_stripe() {
    let a = new_alloc_lanes(4096, 2);
    let before = a.free_block_count();
    let e = a
        .allocate_stripe_extent_for_lane(0, 4, STRIPE, PHASE)
        .unwrap();
    assert_eq!(e.count, 6);
    assert_eq!(a.free_block_count(), before - 6);
    a.free_extent(e).unwrap();
    assert_eq!(a.free_block_count(), before, "whole padded stripe returns");
}

#[test]
fn global_path_aligns_without_lane() {
    // lane index out of range forces the global (no-lane) path.
    let a = new_alloc_lanes(4096, 0);
    let e = a
        .allocate_stripe_extent_for_lane(0, 7, STRIPE, PHASE)
        .unwrap();
    assert_eq!(e.count, 12);
    assert_eq!((e.start.0 + PHASE as u64) % STRIPE as u64, 0);
}

/// Build a reserve of ISOLATED single-stripe windows (the aged-pool shape
/// where a one-run refill degrades to "global lock per allocation"), by
/// freeing every other aligned window back.
///
/// Returns the aligned window starts in ascending order.
fn seed_isolated_stripe_windows(a: &SpaceAllocator, windows: usize) -> Vec<u64> {
    let blocks = (windows as u64 + 2) * 2 * STRIPE as u64 + RESERVED_BLOCKS;
    let usable = blocks - RESERVED_BLOCKS;
    // No single extent can be wider than one address region, so claim by
    // repeated request rather than in one call.
    claim_whole_pool(a, usable);
    a.set_stripe_geometry(STRIPE, PHASE);
    let first = SpaceAllocator::align_up_pba(RESERVED_BLOCKS, STRIPE as u64, PHASE as u64);
    let mut starts = Vec::with_capacity(windows);
    for i in 0..windows as u64 {
        // Stride of two stripes leaves a live stripe between every free one,
        // so nothing can coalesce: the reserve is `windows` runs of exactly
        // one stripe each.
        let start = first + i * 2 * STRIPE as u64;
        a.free_extent(Extent::new(Pba(start), STRIPE)).unwrap();
        starts.push(start);
    }
    starts
}

/// Allocate until nothing is free. `allocate_extent` returns the largest
/// available fragment when the exact width is unavailable, which is exactly
/// what a region-sharded pool offers for a device-wide request.
fn claim_whole_pool(a: &SpaceAllocator, usable: u64) {
    let mut claimed = 0u64;
    while claimed < usable {
        let extent = a
            .allocate_extent((usable - claimed).min(u32::MAX as u64) as u32)
            .expect("the pool still had free blocks");
        claimed += u64::from(extent.count);
    }
    assert_eq!(a.free_block_count(), 0, "pool not fully claimed");
}

fn isolated_window_allocator(windows: usize) -> (SpaceAllocator, Vec<u64>) {
    let blocks = (windows as u64 + 2) * 2 * STRIPE as u64 + RESERVED_BLOCKS;
    let a = new_alloc_lanes(blocks, 4);
    let starts = seed_isolated_stripe_windows(&a, windows);
    (a, starts)
}

/// Batching a refill must not change SELECTION: taking the K lowest-address
/// qualifying runs in one lock hold is exactly the sequence K successive
/// `first_fit(need)` calls return, because removing an extent never
/// coalesces. Pinned against the reserve's own address order.
#[test]
fn batched_refill_equals_sequential_refills() {
    const WINDOWS: usize = LANE_EXTENT_CACHE_REFILL_RUNS * 2 + 5;
    let (a, expected) = isolated_window_allocator(WINDOWS);

    let mut got = Vec::with_capacity(WINDOWS);
    for _ in 0..WINDOWS {
        let e = a
            .allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
            .expect("every seeded window can serve one stripe");
        assert_eq!(e.count, STRIPE);
        got.push(e.start.0);
    }
    assert_eq!(
        got, expected,
        "aligned allocation must consume the reserve in ascending address \
             order, exactly as one-run-at-a-time first-fit did"
    );
    // And the reserve is now empty rather than partially stranded.
    assert_eq!(a.contiguity_stats().stripe_reserve_blocks, 0);
}

/// The point of the batch: one global-lock refill must serve many
/// allocations even when NO two free stripes are adjacent. Before this,
/// `allocs_per_refill` was exactly 1.00 on this shape.
#[test]
fn refill_serves_many_allocs_from_a_discontiguous_reserve() {
    const WINDOWS: usize = LANE_EXTENT_CACHE_REFILL_RUNS * 2;
    let (a, _) = isolated_window_allocator(WINDOWS);

    for _ in 0..WINDOWS {
        a.allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
            .unwrap();
    }
    let supply = a.supply_stats();
    assert_eq!(supply.aligned_allocs, WINDOWS as u64);
    assert_eq!(
        supply.refills, 2,
        "K={LANE_EXTENT_CACHE_REFILL_RUNS} runs per refill ⇒ 2 refills for \
             2K windows, not one per allocation"
    );
    assert_eq!(supply.refill_runs, WINDOWS as u64);
    assert_eq!(supply.refill_blocks, WINDOWS as u64 * STRIPE as u64);
    assert!(
        supply.allocs_per_refill() >= LANE_EXTENT_CACHE_REFILL_RUNS as f64,
        "allocs_per_refill was {}",
        supply.allocs_per_refill()
    );
    assert_eq!(supply.drains, 0, "no lane-cache drain should be needed");
}

/// A lane hands out aligned carves in strictly ASCENDING PBA order even when
/// its cache holds many disjoint runs. This is what keeps one L2P leaf's PBAs
/// clustered; an unordered cache with `swap_remove` would scramble them
/// across the whole refill.
#[test]
fn lane_extent_cache_hands_out_ascending() {
    let (a, _) = isolated_window_allocator(LANE_EXTENT_CACHE_REFILL_RUNS);
    let mut last = 0u64;
    for i in 0..LANE_EXTENT_CACHE_REFILL_RUNS {
        let e = a
            .allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
            .unwrap();
        assert!(
            e.start.0 > last || i == 0,
            "allocation {i} at {} went backwards from {last}",
            e.start.0
        );
        last = e.start.0;
    }
}

/// The cache stays ordered when a carve leaves head/tail remainders, and a
/// later unaligned take still picks the lowest-address fit.
#[test]
fn push_extent_cache_keeps_descending_order_through_splits() {
    let mut cache = Vec::new();
    for start in [40u64, 10, 70, 22] {
        SpaceAllocator::push_extent_cache(&mut cache, Extent::new(Pba(start), 6));
    }
    assert_eq!(
        cache.iter().map(|e| e.start.0).collect::<Vec<_>>(),
        vec![70, 40, 22, 10]
    );
    // Lowest-address fit is taken first, and the front-carve remainder keeps
    // its slot.
    let got = SpaceAllocator::take_from_extent_cache(&mut cache, 2).unwrap();
    assert_eq!(got, Extent::new(Pba(10), 2));
    assert_eq!(
        cache.iter().map(|e| e.start.0).collect::<Vec<_>>(),
        vec![70, 40, 22, 12]
    );
    // A misaligned run that must yield a head pad keeps the cache ordered.
    cache.clear();
    SpaceAllocator::push_extent_cache(&mut cache, Extent::new(Pba(11), 3 * STRIPE));
    let aligned =
        SpaceAllocator::take_aligned_from_extent_cache(&mut cache, STRIPE, STRIPE, PHASE).unwrap();
    assert_eq!((aligned.start.0 + PHASE as u64) % STRIPE as u64, 0);
    let starts: Vec<u64> = cache.iter().map(|e| e.start.0).collect();
    let mut sorted = starts.clone();
    sorted.sort_unstable_by(|a, b| b.cmp(a));
    assert_eq!(starts, sorted, "cache must stay descending by start");
}

/// A request wider than every reserve run must still be served from the one
/// run that does fit, without the ascending walk scanning the whole reserve.
/// (The seeded reserve is deliberately larger than
/// `LANE_EXTENT_CACHE_REFILL_SCAN` single-stripe runs.)
#[test]
fn wide_request_against_single_stripe_reserve_stays_bounded() {
    const WINDOWS: usize = LANE_EXTENT_CACHE_REFILL_SCAN * 3;
    let (a, starts) = isolated_window_allocator(WINDOWS);
    // Widen exactly one window near the END of the reserve into two stripes,
    // so only it can serve a 2-stripe request and the walk would have to pass
    // every earlier entry to reach it.
    let wide = starts[WINDOWS - 2];
    a.free_extent(Extent::new(Pba(wide + STRIPE as u64), STRIPE))
        .unwrap();

    let e = a
        .allocate_stripe_extent_for_lane(0, 2 * STRIPE, STRIPE, PHASE)
        .expect("the one 2-stripe run must be found");
    assert_eq!(e.start.0, wide);
    assert_eq!(e.count, 2 * STRIPE);
    let supply = a.supply_stats();
    assert_eq!(supply.refills, 1);
    assert_eq!(
        supply.refill_runs, 1,
        "only the one qualifying run is taken; the walk must not keep going"
    );
}

/// The block budget still bounds a refill: one huge reserve run cannot park
/// more than `LANE_EXTENT_CACHE_REFILL_BLOCKS` in a single lane.
#[test]
fn refill_respects_the_block_budget() {
    let blocks = 4 * LANE_EXTENT_CACHE_REFILL_BLOCKS as u64;
    let a = new_alloc_lanes(blocks, 2);
    a.set_stripe_geometry(STRIPE, PHASE);
    a.allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
        .unwrap();
    let supply = a.supply_stats();
    assert_eq!(supply.refills, 1);
    assert_eq!(
        supply.refill_runs, 1,
        "one contiguous run satisfies the whole budget"
    );
    assert!(
        supply.refill_blocks <= u64::from(LANE_EXTENT_CACHE_REFILL_BLOCKS),
        "refill took {} blocks, budget is {}",
        supply.refill_blocks,
        LANE_EXTENT_CACHE_REFILL_BLOCKS
    );
}

// ---------------------------------------------------------------------
// `storage.stripe_refill_run_stripes` — prefer intact reserve runs.
// ---------------------------------------------------------------------

/// Whole stripes the wide pass asks for in these tests (the shipped default
/// when the knob is on, and the width that makes chunklet's merge ~8x).
const WIDE: u32 = 8;

/// The 2026-08-01 box shape: a fragmented LOW area of isolated single-stripe
/// windows (pinned 24 KiB windows) plus INTACT material higher up, which
/// address-first-fit at a one-stripe floor never reaches.
///
/// Returns the confetti window starts (ascending) and the intact run. Sized
/// to stay under [`MIN_REGION_BLOCKS`] so the pool is single-region under an
/// `ONYX_ALLOCATOR_REGIONS` sweep too — region routing is not what these
/// tests are about, and a run cannot straddle a region.
fn pinned_windows_plus_intact_run(
    windows: usize,
    wide_stripes: u32,
) -> (SpaceAllocator, Vec<u64>, Extent) {
    let confetti_blocks = (windows as u64 + 1) * 2 * STRIPE as u64;
    let wide_blocks = u64::from(wide_stripes) * STRIPE as u64;
    let blocks = RESERVED_BLOCKS + confetti_blocks + 4 * STRIPE as u64 + wide_blocks;
    assert!(
        blocks <= MIN_REGION_BLOCKS,
        "keep the fixture single-region: {blocks} blocks"
    );
    let a = new_alloc_lanes(blocks, 2);
    claim_whole_pool(&a, blocks - RESERVED_BLOCKS);
    a.set_stripe_geometry(STRIPE, PHASE);

    let first = SpaceAllocator::align_up_pba(RESERVED_BLOCKS, STRIPE as u64, PHASE as u64);
    let mut starts = Vec::with_capacity(windows);
    for i in 0..windows as u64 {
        let start = first + i * 2 * STRIPE as u64;
        a.free_extent(Extent::new(Pba(start), STRIPE)).unwrap();
        starts.push(start);
    }
    // Two live stripes of separation so the intact run cannot coalesce with
    // the last confetti window.
    let wide_start = first + (windows as u64 + 2) * 2 * STRIPE as u64;
    let wide = Extent::new(Pba(wide_start), (wide_blocks) as u32);
    a.free_extent(wide).unwrap();
    (a, starts, wide)
}

/// chunklet's per-PD adjacency merge, computed over the PBAs one lane was
/// handed: `ops / maximal contiguous groups`. This is the local stand-in for
/// the box's `chunklet_submit_drain_data merge` — the device-side merge is a
/// pure function of whether consecutive stripes are adjacent, so a lane whose
/// carves are adjacent merges and one whose carves scatter does not.
fn adjacency_merge_factor(starts: &[u64], width: u32) -> f64 {
    assert!(!starts.is_empty());
    let mut sorted = starts.to_vec();
    sorted.sort_unstable();
    let groups = 1 + sorted
        .windows(2)
        .filter(|pair| pair[0] + u64::from(width) != pair[1])
        .count();
    starts.len() as f64 / groups as f64
}

fn drain_stripes(a: &SpaceAllocator, lane: usize, count: usize) -> Vec<u64> {
    (0..count)
        .map(|i| {
            a.allocate_stripe_extent_for_lane(lane, STRIPE, STRIPE, PHASE)
                .unwrap_or_else(|e| panic!("allocation {i} failed: {e}"))
                .start
                .0
        })
        .collect()
}

/// The knob's whole purpose, stated as the box gate: with it off, consecutive
/// stripes scatter across pinned windows and the merge is ~1x; with it on the
/// lane is routed to intact material and the same allocations are adjacent.
#[test]
fn wide_refill_turns_scattered_carves_into_one_contiguous_run() {
    const OPS: usize = 72; // one box-sized flusher batch
    let (off, _, _) = pinned_windows_plus_intact_run(OPS, OPS as u32);
    let legacy = drain_stripes(&off, 0, OPS);
    let legacy_merge = adjacency_merge_factor(&legacy, STRIPE);
    assert!(
        legacy_merge < 1.05,
        "fixture is not the box shape: legacy merge was {legacy_merge}"
    );

    let (on, _, wide) = pinned_windows_plus_intact_run(OPS, OPS as u32);
    on.set_stripe_refill_run_stripes(WIDE);
    let wide_arm = drain_stripes(&on, 0, OPS);
    assert_eq!(
        adjacency_merge_factor(&wide_arm, STRIPE),
        OPS as f64,
        "all {OPS} carves should come from the one intact run"
    );
    assert_eq!(wide_arm[0], wide.start.0);
    let supply = on.supply_stats();
    assert_eq!(supply.refills, 1, "one refill parks the whole intact run");
    assert_eq!(supply.refill_runs, 1);
    assert_eq!(supply.wide_hits, 1);
    assert_eq!(supply.wide_misses, 0);
    assert!(
        supply.blocks_per_run() >= f64::from(WIDE * STRIPE),
        "blocks_per_run was {} (gate: >= {})",
        supply.blocks_per_run(),
        WIDE * STRIPE
    );
}

/// The floor filters CANDIDATES, it does not reorder them: among runs that
/// qualify the pick is still the lowest address, so the wide pass consumes
/// intact material in ascending order exactly as first-fit always did. (This
/// is the property that keeps the change out of the best-fit family that once
/// corrupted the metadb L2P leaf codec.)
#[test]
fn wide_refill_is_still_first_fit_by_address() {
    let a = new_alloc_lanes(1024, 2);
    claim_whole_pool(&a, 1024 - RESERVED_BLOCKS);
    a.set_stripe_geometry(STRIPE, PHASE);
    let base = SpaceAllocator::align_up_pba(RESERVED_BLOCKS, STRIPE as u64, PHASE as u64);
    // Two qualifying runs, separated by live blocks; the HIGHER one is freed
    // first so insertion order cannot be what decides the pick.
    let high = Extent::new(Pba(base + 40 * STRIPE as u64), WIDE * STRIPE);
    let low = Extent::new(Pba(base + 20 * STRIPE as u64), WIDE * STRIPE);
    a.free_extent(high).unwrap();
    a.free_extent(low).unwrap();

    a.set_stripe_refill_run_stripes(WIDE);
    let got = drain_stripes(&a, 0, WIDE as usize);
    assert_eq!(got[0], low.start.0, "lowest qualifying address wins");
    assert!(
        got.iter().all(|&s| s < high.start.0),
        "the lower intact run must be fully consumed first: {got:?}"
    );
}

/// A pool with no intact run left must behave EXACTLY like the legacy path:
/// same PBAs, same supply accounting. The wide pass is a preference, and a
/// preference that cannot be met has to cost nothing but a counter.
#[test]
fn wide_refill_falls_back_to_the_legacy_selection_verbatim() {
    const WINDOWS: usize = LANE_EXTENT_CACHE_REFILL_RUNS + 7;
    let (off, _) = isolated_window_allocator(WINDOWS);
    let legacy = drain_stripes(&off, 0, WINDOWS);

    let (on, _) = isolated_window_allocator(WINDOWS);
    on.set_stripe_refill_run_stripes(WIDE);
    let got = drain_stripes(&on, 0, WINDOWS);

    assert_eq!(got, legacy, "fallback must not change selection");
    let (a, b) = (off.supply_stats(), on.supply_stats());
    assert_eq!(
        (a.refills, a.refill_runs, a.refill_blocks, a.drains),
        (b.refills, b.refill_runs, b.refill_blocks, b.drains)
    );
    assert_eq!(b.wide_hits, 0);
    assert_eq!(
        b.wide_misses, b.refills,
        "every refill on a pinned-window pool is a wide miss"
    );
}

/// `0` is the rollback: on a pool where the knob WOULD change the answer, an
/// allocator left at the default emits the legacy sequence.
#[test]
fn wide_refill_knob_off_is_the_legacy_path() {
    const OPS: usize = 24;
    let (a, windows, _) = pinned_windows_plus_intact_run(OPS, WIDE);
    assert_eq!(a.stripe_refill_run_stripes(), 0, "default must be off");
    let got = drain_stripes(&a, 0, OPS);
    assert_eq!(
        got,
        windows[..OPS].to_vec(),
        "knob off must consume the pinned windows lowest-address-first"
    );
    assert_eq!(a.supply_stats().wide_hits + a.supply_stats().wide_misses, 0);
}

/// The wide pass must never turn a servable request into `SpaceExhausted`:
/// the same pool drains to the same last block with the knob on.
#[test]
fn wide_refill_never_costs_capacity() {
    const WINDOWS: usize = 40;
    let (off, _, _) = pinned_windows_plus_intact_run(WINDOWS, WIDE);
    let (on, _, _) = pinned_windows_plus_intact_run(WINDOWS, WIDE);
    on.set_stripe_refill_run_stripes(WIDE);

    let drain_all = |a: &SpaceAllocator| {
        let mut got = Vec::new();
        while let Ok(e) = a.allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE) {
            got.push(e.start.0);
        }
        got.sort_unstable();
        got
    };
    let legacy = drain_all(&off);
    let wide = drain_all(&on);
    assert_eq!(
        legacy, wide,
        "the wide pass reordered emission, not the reachable set"
    );
    assert_eq!(off.free_block_count(), on.free_block_count());
}

/// A wide floor on a reserve that holds only single-stripe runs must not walk
/// the set: the size index has no class at or above the floor, so the probe is
/// two descents. Pinned by the entry bound the legacy walk already carries —
/// a reserve deliberately larger than [`LANE_EXTENT_CACHE_REFILL_SCAN`].
#[test]
fn wide_refill_probe_is_bounded_on_a_huge_pinned_reserve() {
    const WINDOWS: usize = LANE_EXTENT_CACHE_REFILL_SCAN * 3;
    let (a, starts) = isolated_window_allocator(WINDOWS);
    a.set_stripe_refill_run_stripes(WIDE);
    let e = a
        .allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
        .expect("a one-stripe request is still servable");
    assert_eq!(e.start.0, starts[0], "fallback keeps first-fit-by-address");
    let supply = a.supply_stats();
    assert_eq!(supply.wide_misses, 1);
    assert_eq!(supply.refill_runs, LANE_EXTENT_CACHE_REFILL_RUNS as u64);
}

/// The floor is clamped to the refill budget and never below the request:
/// asking for a run wider than we would take would reject runs that can serve
/// the whole budget contiguously.
#[test]
fn wide_refill_floor_table() {
    let a = new_alloc_lanes(64, 1);
    assert_eq!(a.wide_refill_floor(STRIPE, 8192, STRIPE), None, "knob off");

    a.set_stripe_refill_run_stripes(WIDE);
    assert_eq!(
        a.wide_refill_floor(STRIPE, 8192, STRIPE),
        Some(WIDE * STRIPE)
    );
    // Clamped to the budget.
    assert_eq!(a.wide_refill_floor(STRIPE, 12, STRIPE), Some(12));
    // A request already wider than the floor gets no wide pass.
    assert_eq!(a.wide_refill_floor(WIDE * STRIPE, 8192, STRIPE), None);
    assert_eq!(a.wide_refill_floor(100 * STRIPE, 8192, STRIPE), None);
    // Never under the request, even with an absurd stripe width.
    a.set_stripe_refill_run_stripes(u32::MAX);
    let floor = a.wide_refill_floor(STRIPE, 8192, STRIPE).unwrap();
    assert!((STRIPE..=8192).contains(&floor));
}
