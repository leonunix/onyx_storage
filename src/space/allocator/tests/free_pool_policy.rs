use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use super::*;

const STRIPE: u32 = 6;
const PHASE: u32 = 2;

fn allocator(blocks: u64, lanes: usize) -> SpaceAllocator {
    SpaceAllocator::new(blocks * BLOCK_SIZE as u64, lanes)
}

/// The per-region geometry change `SpaceAllocator::set_stripe_geometry`
/// performs: drain every policy class, install the new geometry, re-insert.
/// Kept as a test helper rather than a `FreePools` method so the production
/// sequence has exactly one implementation.
fn regeometry(pools: &mut FreePools, stripe: u32, phase: u32) {
    let mut runs = pools.take_all_runs();
    pools.reset_geometry(stripe, phase);
    runs.sort_unstable_by_key(|extent| extent.start.0);
    for run in runs {
        pools.insert_classified(run);
    }
}

/// The wait/hold attribution must charge the acquiring PATH, not a default
/// bucket, and every acquisition must record a hold — otherwise the box read
/// silently attributes everything to one site.
#[test]
fn free_lock_attribution_charges_the_acquiring_site() {
    let a = allocator(4096, 2);
    let acq = |sites: &[LockSiteStats], name: &str| {
        sites
            .iter()
            .find(|s| s.site == name)
            .expect("every site is always reported")
            .acquisitions
    };
    // Shape is stable across reads (all sites always present) so two status
    // samples can be differenced field-by-field.
    let s0 = a.free_lock_stats();
    assert_eq!(s0.len(), FREE_LOCK_SITES);

    a.allocate_one().unwrap();
    let s1 = a.free_lock_stats();
    assert!(acq(&s1, "small_alloc") > acq(&s0, "small_alloc"));
    assert_eq!(
        acq(&s1, "audit"),
        acq(&s0, "audit"),
        "alloc is not an audit"
    );

    a.contiguity_stats();
    let s2 = a.free_lock_stats();
    assert!(acq(&s2, "audit") > acq(&s1, "audit"));
    assert_eq!(
        acq(&s2, "small_alloc"),
        acq(&s1, "small_alloc"),
        "a status read must not be charged to allocation"
    );

    a.set_stripe_geometry(STRIPE, PHASE);
    a.allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
        .unwrap();
    let s3 = a.free_lock_stats();
    assert!(
        acq(&s3, "writer_refill") > 0,
        "the aligned writer path must be attributed to writer_refill"
    );

    for s in s3.iter().filter(|s| s.acquisitions > 0) {
        assert!(s.hold_ns > 0, "site {} recorded no hold", s.site);
        assert!(s.hold_ns_max > 0, "site {} recorded no max hold", s.site);
    }
}

/// The retired-shard attribution has to charge the acquiring path too, and —
/// the whole point — the retire BATCH path must record its `retired`
/// acquisition as nested INSIDE its region hold, with an `items` count, so a
/// box read can split "region hold" into "waited for the retired set" vs "did
/// per-extent work".
#[test]
fn retired_lock_attribution_charges_the_acquiring_site() {
    let a = allocator(4096, 2);
    let find = |sites: &[LockSiteStats], name: &str| {
        *sites
            .iter()
            .find(|s| s.site == name)
            .expect("every site is always reported")
    };
    let r0 = a.retired_lock_stats();
    assert_eq!(r0.len(), RETIRED_LOCK_SITES);

    // Single retire → retire_one.
    let one = a.allocate_extent(1).unwrap();
    a.retire_extent(one).unwrap();
    let r1 = a.retired_lock_stats();
    assert!(find(&r1, "retire_one").acquisitions > 0);
    assert_eq!(find(&r1, "retire_one").items, 1);
    assert_eq!(find(&r1, "retire_batch").acquisitions, 0);

    // Batch retire → retire_batch, and its `items` must equal the extents
    // processed, NOT the acquisition count (they differ once the hold is cut
    // at region boundaries, which is exactly the effect being measured).
    let batch: Vec<Extent> = (0..8).map(|_| a.allocate_extent(1).unwrap()).collect();
    a.retire_extents_batch(&batch, Instant::now());
    let r2 = a.retired_lock_stats();
    let rb = find(&r2, "retire_batch");
    assert_eq!(rb.items, batch.len() as u64, "items must count extents");
    assert!(rb.acquisitions > 0 && rb.acquisitions <= batch.len() as u64);
    assert!(rb.hold_ns > 0, "retire_batch recorded no retired hold");
    // The nesting that makes the ledger readable: the region hold has to
    // cover the retired acquisition it performs inside itself.
    let fb = find(&a.free_lock_stats(), "retire_batch");
    assert_eq!(fb.items, batch.len() as u64);
    assert!(
        fb.hold_ns >= rb.hold_ns,
        "region hold {} must contain the retired hold {} taken inside it",
        fb.hold_ns,
        rb.hold_ns
    );

    // A read-only query is charged to its own site, never to a mutator.
    let before = find(&a.retired_lock_stats(), "retire_batch").acquisitions;
    a.is_retired(one.start);
    let r3 = a.retired_lock_stats();
    assert!(find(&r3, "is_retired").acquisitions > 0);
    assert_eq!(find(&r3, "retire_batch").acquisitions, before);

    for s in r3.iter().filter(|s| s.acquisitions > 0) {
        assert!(s.hold_ns > 0, "site {} recorded no hold", s.site);
    }
}

/// Sampling must never cost a COUNT: `acquisitions` and `items` stay exact at
/// any stride (they are what every rate and per-extent read divides by), only
/// the time is sampled — and the reported time is scaled back up so the table
/// still means "total ns at this site" and stays comparable with the
/// pre-sampling history. The first sample of a (shard, site) is always timed,
/// so a site that was acquired at all always reports a hold.
#[test]
fn lock_stats_sampling_keeps_counts_exact_and_scales_the_time() {
    let stats = SiteLockStats::<2>::new();
    stats.stride_override.store(4, Ordering::Relaxed);

    for _ in 0..9 {
        let (shard, queued) = stats.begin(0);
        if queued.is_some() {
            shard.charge_wait(0, 100);
            shard.charge_hold(0, 1000);
        }
        shard.charge_items(0, 3);
    }
    // Site 1 is acquired exactly once: the "always time the first" rule is
    // what keeps a rarely-taken site from reporting a hold of zero.
    let (shard, queued) = stats.begin(1);
    assert!(queued.is_some(), "the first acquisition is always timed");
    shard.charge_wait(1, 7);
    shard.charge_hold(1, 11);

    let snap = stats.snapshot(["hot", "rare"]);
    let hot = snap[0];
    assert_eq!(hot.acquisitions, 9, "counts are never sampled");
    assert_eq!(hot.items, 27, "items are never sampled");
    // Timed acquisitions are 1, 5, 9 — ceil(9/4).
    assert_eq!(hot.timed, 3);
    assert_eq!(
        hot.wait_ns,
        300 * 9 / 3,
        "wait scaled by acquisitions/timed"
    );
    assert_eq!(hot.hold_ns, 3000 * 9 / 3);
    assert_eq!(hot.wait_ns_max, 100, "maxima stay raw (a lower bound)");
    assert_eq!(hot.hold_ns_max, 1000);
    assert!(
        (hot.hold_us() - 1.0).abs() < 1e-9,
        "mean per acquisition holds"
    );

    let rare = snap[1];
    assert_eq!((rare.acquisitions, rare.timed), (1, 1));
    assert_eq!((rare.wait_ns, rare.hold_ns), (7, 11), "nothing to scale");
}

/// Every thread accounts to its own shard, so the snapshot has to sum them —
/// a per-thread counter that only ever reported one shard would silently
/// under-count everything the box reads.
#[test]
fn lock_stats_shards_are_summed_across_threads() {
    let stats = std::sync::Arc::new(SiteLockStats::<1>::new());
    // Write directly to two distinct slots: slot assignment is process-global
    // round-robin, so this is the only way to pin the merge deterministically.
    stats.shards[0].charge_items(0, 5);
    stats.shards[LOCK_STAT_SHARDS - 1].charge_items(0, 7);
    assert_eq!(stats.snapshot(["x"])[0].items, 12);

    let threads: Vec<_> = (0..4)
        .map(|_| {
            let stats = std::sync::Arc::clone(&stats);
            std::thread::spawn(move || {
                for _ in 0..1000 {
                    let (shard, queued) = stats.begin(0);
                    if let Some(queued) = queued {
                        shard.charge_wait(0, queued.elapsed().as_nanos() as u64);
                    }
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let snap = stats.snapshot(["x"])[0];
    assert_eq!(snap.acquisitions, 4000, "no thread's count may be lost");
    assert!(snap.timed > 0 && snap.timed <= snap.acquisitions);
}

#[test]
fn sub_stripe_release_before_next_alignment_never_expands() {
    let mut pools = FreePools::new();
    pools.reset_geometry(STRIPE, PHASE);
    let released = Extent::new(Pba(12_954), 2);

    pools.insert_classified(released);

    assert_eq!(pools.free_blocks_in_pools(), 2);
    assert_eq!(pools.stripe_reserve.blocks_total(), 0);
    assert_eq!(
        pools.general.by_addr().iter().copied().collect::<Vec<_>>(),
        vec![released]
    );
    assert!(pools
        .overlapping_free(Extent::single(released.end_pba()))
        .is_none());
}

#[test]
fn adjacent_u32_sized_releases_coalesce_without_count_truncation() {
    let mut pools = FreePools::new();
    pools.reset_geometry(STRIPE, 0);
    let start = 12u64;
    let first = Extent::new(Pba(start), u32::MAX);
    let second = Extent::new(Pba(start + u32::MAX as u64), 100);

    pools.insert_classified(first);
    pools.insert_classified(second);

    let expected = u32::MAX as u64 + 100;
    assert_eq!(pools.free_blocks_in_pools(), expected);
    assert!(pools
        .stripe_reserve
        .by_addr()
        .iter()
        .all(|extent| extent.count.is_multiple_of(STRIPE)));

    let mut runs: Vec<Extent> = pools.general.by_addr().iter().copied().collect();
    runs.extend(pools.stripe_reserve.by_addr().iter().copied());
    runs.sort_unstable_by_key(|extent| extent.start.0);
    assert_eq!(runs.first().unwrap().start.0, start);
    assert_eq!(runs.last().unwrap().end_pba().0, start + expected);
    assert!(runs
        .windows(2)
        .all(|pair| pair[0].end_pba().0 == pair[1].start.0));
    assert_eq!(
        runs.iter().map(|extent| extent.count as u64).sum::<u64>(),
        expected
    );
}

#[test]
fn geometry_change_preserves_quarantined_free_blocks() {
    let mut pools = FreePools::new();
    pools.reset_geometry(STRIPE, PHASE);
    pools.insert_classified(Extent::new(Pba(30), 5));
    let mut free_parts = pools.empty_set_with_geometry();
    free_parts.insert(Extent::new(Pba(100), 3));
    pools.quarantines.insert(
        100,
        QuarantineTarget {
            range: Extent::new(Pba(100), STRIPE),
            free_parts,
        },
    );
    let before = pools.free_blocks_in_pools();

    regeometry(&mut pools, 4, 0);

    assert!(pools.quarantines.is_empty());
    assert_eq!(pools.free_blocks_in_pools(), before);
    assert!(pools.overlapping_free(Extent::new(Pba(100), 3)).is_some());
}

#[test]
fn exact_miss_drains_once_without_consuming_short_space() {
    let allocator = allocator(RESERVED_BLOCKS + 4, 1);
    assert_eq!(
        allocator.allocate_exact_extent_for_lane(0, 1).unwrap(),
        Extent::single(Pba(RESERVED_BLOCKS))
    );
    let cached_before = allocator.lane_extent_caches[0].lock().unwrap().clone();
    let free_before = allocator.free_block_count();
    assert_eq!(
        cached_before,
        vec![Extent::new(Pba(RESERVED_BLOCKS + 1), 3)]
    );

    for _ in 0..2 {
        assert!(matches!(
            allocator.allocate_exact_extent_for_lane(0, 4),
            Err(OnyxError::SpaceExhausted)
        ));
        assert_eq!(allocator.free_block_count(), free_before);
        assert!(allocator.is_extent_free(cached_before[0]));
    }
    assert!(allocator.lane_extent_caches[0].lock().unwrap().is_empty());
}

/// A pool that really does shard into 16 regions: `RegionLayout::plan` floors
/// a region at [`MIN_REGION_BLOCKS`], so a device under `16 * 4096` usable
/// blocks silently collapses to ONE region and any region test on it is
/// vacuous.
fn sharded_16_regions_lanes(lanes: usize) -> SpaceAllocator {
    const PER_REGION: u64 = 4200;
    let allocator = SpaceAllocator::new_with_exact_regions(
        (16 * PER_REGION + RESERVED_BLOCKS) * BLOCK_SIZE as u64,
        lanes,
        16,
    );
    assert_eq!(
        allocator.region_count(),
        16,
        "region planning changed; re-pick PER_REGION"
    );
    allocator
}

fn sharded_16_regions() -> SpaceAllocator {
    sharded_16_regions_lanes(1)
}

/// True blocks parked in every lane cache, read the expensive way.
fn lane_cached_truth(allocator: &SpaceAllocator) -> Vec<u64> {
    allocator
        .lane_caches
        .iter()
        .map(|c| c.lock().unwrap().len() as u64)
        .chain(allocator.lane_extent_caches.iter().map(|c| {
            c.lock()
                .unwrap()
                .iter()
                .map(|e| u64::from(e.count))
                .sum::<u64>()
        }))
        .collect()
}

fn assert_depths_exact(allocator: &SpaceAllocator, context: &str) {
    let published: Vec<u64> = allocator
        .lane_cache_depth
        .iter()
        .chain(allocator.lane_extent_cache_depth.iter())
        .map(|d| d.load(Ordering::Relaxed))
        .collect();
    assert_eq!(
        published,
        lane_cached_truth(allocator),
        "lane depth counters disagree with the caches after {context}"
    );
}

/// Every ENOSPC path now asks the depth counters — instead of `2 * lanes`
/// mutexes — whether an all-regions drain is worth doing, so a publish that
/// goes missing at any mutation site would make the allocator skip a drain
/// that had blocks to give (a spurious `SpaceExhausted`). Single-threaded, so
/// the counters must match the caches EXACTLY after every operation, and the
/// traffic below is chosen to touch all of them: both refill kinds, both pop
/// kinds, the drain, the quarantine extraction, and the rebuild's clear.
#[test]
fn lane_depth_counters_track_the_caches_through_mixed_traffic() {
    const LANES: usize = 4;
    let allocator = allocator(RESERVED_BLOCKS + 1024, LANES);
    allocator.set_stripe_geometry(STRIPE, PHASE);
    assert_depths_exact(&allocator, "geometry install");

    let mut rng = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let mut held: Vec<Extent> = Vec::new();
    for step in 0..600usize {
        let lane = step % LANES;
        let roll = next() % 100;
        let got = if roll < 40 {
            allocator.allocate_stripe_extent_for_lane(lane, STRIPE, STRIPE, PHASE)
        } else if roll < 70 {
            allocator.allocate_extent_for_lane(lane, 1 + (next() % 4) as u32)
        } else {
            allocator.allocate_one_for_lane(lane).map(Extent::single)
        };
        if let Ok(extent) = got {
            held.push(extent);
        }
        assert_depths_exact(&allocator, "an allocation");
        if held.len() > 8 && next() % 2 == 0 {
            let extent = held.swap_remove(next() as usize % held.len());
            allocator.free_extent(extent).unwrap();
            assert_depths_exact(&allocator, "a free");
        }
    }

    // Quarantine detaches the parts of the lane caches it covers.
    let target = Extent::new(
        Pba(SpaceAllocator::align_up_pba(
            RESERVED_BLOCKS + 256,
            u64::from(STRIPE),
            u64::from(PHASE),
        )),
        STRIPE,
    );
    if allocator.begin_defrag_quarantine(target).is_ok() {
        assert_depths_exact(&allocator, "a quarantine open");
        allocator.cancel_defrag_quarantine(target.start);
    }

    // The drain zeroes every counter, and does so having folded back exactly
    // as many blocks as they claimed.
    let claimed: u64 = lane_cached_truth(&allocator).iter().sum();
    let drained_before = allocator.supply_stats().drain_blocks;
    allocator.drain_lane_caches();
    assert_eq!(
        allocator.supply_stats().drain_blocks - drained_before,
        claimed,
        "the drain folded back a different number of blocks than the caches held"
    );
    assert_depths_exact(&allocator, "a drain");
    assert_eq!(allocator.lane_cached_blocks(), 0);
}

/// `largest_hint` is not a skip filter — `take_largest_regionwise` picks the
/// winner from it WITHOUT locking anyone else, so a hint that reads LOW while
/// the region holds a wide run would hide that run from the unaligned path.
/// Pin the exactness after every mutating shape: seeding, allocation, free,
/// retire/reclaim, geometry change, quarantine, and drain.
#[test]
fn largest_hint_equals_the_truth_after_every_mutation() {
    let allocator = allocator(RESERVED_BLOCKS + 512, 2);
    let check = |context: &str| {
        for idx in 0..allocator.region_count() {
            let guard = allocator.lock_region(FreeLockSite::Audit, idx);
            let truth = guard
                .region(idx)
                .largest_allocatable()
                .map_or(0, |(run, _)| u64::from(run.count));
            drop(guard);
            assert_eq!(
                allocator.regions.largest_hint[idx].load(Ordering::Relaxed),
                truth,
                "region {idx} largest_hint is wrong after {context}"
            );
        }
    };
    check("seeding");
    allocator.set_stripe_geometry(STRIPE, PHASE);
    check("a geometry install");

    let mut held = Vec::new();
    for _ in 0..40 {
        if let Ok(extent) = allocator.allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE) {
            held.push(extent);
        }
    }
    check("aligned allocations");
    allocator.drain_lane_caches();
    check("a drain");

    let mid = held.len() / 2;
    for extent in held.drain(..mid) {
        allocator.free_extent(extent).unwrap();
    }
    check("frees");

    let running = AtomicBool::new(true);
    for extent in held.drain(..) {
        allocator.retire_extent(extent).unwrap();
        let _ = allocator.reclaim_retired_extents_batch(&[extent], &running);
    }
    check("retire + reclaim");

    let target = Extent::new(
        Pba(SpaceAllocator::align_up_pba(
            RESERVED_BLOCKS + 64,
            u64::from(STRIPE),
            u64::from(PHASE),
        )),
        STRIPE,
    );
    if allocator.begin_defrag_quarantine(target).is_ok() {
        check("a quarantine open");
        allocator.cancel_defrag_quarantine(target.start);
        check("a quarantine cancel");
    }
}

/// The hint argmax must return the extent the full scan would have returned.
/// Randomised region shapes, both implementations run against byte-identical
/// pools, until every region is empty — so the whole drain sequence is
/// compared, not just the first pick.
#[test]
fn hint_argmax_matches_the_full_scan() {
    let build = |seed: u64| {
        let allocator = sharded_16_regions();
        let mut rng = seed | 1;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for idx in 0..allocator.region_count() {
            let mut guard = allocator.lock_region(FreeLockSite::Setup, idx);
            let layout = allocator.regions.layout();
            let pools = guard.region_mut(idx);
            *pools = FreePools::new();
            // 0-3 runs per region, widths 1-9, inside the region's own span.
            let base = layout.start(idx).max(RESERVED_BLOCKS);
            let mut at = base;
            for _ in 0..(next() % 4) {
                let width = 1 + (next() % 9) as u32;
                let gap = 1 + (next() % 5);
                if at + u64::from(width) + gap >= layout.end(idx) {
                    break;
                }
                if next() % 2 == 0 {
                    pools.general.insert(Extent::new(Pba(at), width));
                } else {
                    pools.stripe_reserve.insert(Extent::new(Pba(at), width));
                }
                at += u64::from(width) + gap;
            }
            drop(guard);
        }
        allocator
    };

    for seed in 1..25u64 {
        let hinted = build(seed);
        let scanned = build(seed);
        loop {
            let a = hinted.take_largest_regionwise(FreeLockSite::SmallAlloc);
            let b = scanned.take_largest_regionwise_scanning(FreeLockSite::SmallAlloc);
            match (a, b) {
                (None, None) => break,
                (Some(a), Some(b)) => assert_eq!(
                    a.count, b.count,
                    "seed {seed}: hint argmax took {a:?}, full scan took {b:?}"
                ),
                (a, b) => panic!("seed {seed}: hint {a:?} vs scan {b:?}"),
            }
        }
    }
}

/// The two halves of this fix, as behaviour rather than timing: picking the
/// largest fragment must cost ONE region lock instead of one per region, and a
/// read-only hold must not republish hints (which is what made the old scan
/// phase expensive on both counts).
#[test]
fn largest_pick_locks_one_region_and_read_only_holds_publish_nothing() {
    let allocator = sharded_16_regions();
    let regions = allocator.region_count();
    assert!(
        regions >= 8,
        "this test needs a sharded pool, got {regions}"
    );
    let acqs = |a: &SpaceAllocator| {
        a.free_lock_stats()
            .iter()
            .find(|s| s.site == "small_alloc")
            .expect("every site is always reported")
            .acquisitions
    };

    let before = acqs(&allocator);
    assert!(allocator
        .take_largest_regionwise(FreeLockSite::SmallAlloc)
        .is_some());
    let hinted_locks = acqs(&allocator) - before;
    assert!(
        hinted_locks <= 2,
        "the hint argmax took {hinted_locks} region locks (want 1, the winner)"
    );

    let before = acqs(&allocator);
    assert!(allocator
        .take_largest_regionwise_scanning(FreeLockSite::SmallAlloc)
        .is_some());
    let scan_locks = acqs(&allocator) - before;
    assert!(
        scan_locks > hinted_locks * 2,
        "the full scan should cost one lock per non-empty region, took {scan_locks}"
    );

    // A read-only hold leaves a deliberately wrong hint alone; a mutating one
    // corrects it. This is the `dirty` flag, and the reason
    // `take_largest_regionwise` has to ask for a republish explicitly.
    allocator.regions.free_hint[0].store(123_456, Ordering::Relaxed);
    allocator.contiguity_stats();
    allocator.is_free(Pba(RESERVED_BLOCKS));
    assert_eq!(
        allocator.regions.free_hint[0].load(Ordering::Relaxed),
        123_456,
        "a read-only hold recomputed the hints"
    );
    allocator.allocate_one().unwrap();
    assert_ne!(
        allocator.regions.free_hint[0].load(Ordering::Relaxed),
        123_456,
        "a mutating hold must republish the hints"
    );
}

/// The per-lane drain must be indistinguishable from the all-regions one in
/// WHAT it folds back — only in how much it locks. Randomised lane contents,
/// both implementations on byte-identical pools, compared on the resulting
/// free-space shape.
#[test]
fn per_lane_drain_matches_the_all_regions_drain() {
    let build = |seed: u64| {
        let allocator = sharded_16_regions_lanes(4);
        allocator.set_stripe_geometry(STRIPE, PHASE);
        let mut rng = seed | 1;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        // Mixed traffic so the lanes hold both single blocks and extents, in
        // several regions, with some of it handed out and freed again.
        let mut held = Vec::new();
        for step in 0..120 {
            let lane = step % 4;
            match next() % 3 {
                0 => {
                    if let Ok(pba) = allocator.allocate_one_for_lane(lane) {
                        held.push(Extent::single(pba));
                    }
                }
                1 => {
                    if let Ok(e) = allocator.allocate_extent_for_lane(lane, 1 + (next() % 5) as u32)
                    {
                        held.push(e);
                    }
                }
                _ => {
                    if let Ok(e) =
                        allocator.allocate_stripe_extent_for_lane(lane, STRIPE, STRIPE, PHASE)
                    {
                        held.push(e);
                    }
                }
            }
            if held.len() > 6 && next() % 2 == 0 {
                let e = held.swap_remove(next() as usize % held.len());
                allocator.free_extent(e).unwrap();
            }
        }
        allocator
    };

    for seed in 1..14u64 {
        let per_lane = build(seed);
        let all_regions = build(seed);
        assert_eq!(
            per_lane.lane_cached_blocks(),
            all_regions.lane_cached_blocks(),
            "seed {seed}: the two pools diverged before the drain"
        );
        per_lane.drain_lane_caches();
        all_regions.drain_lane_caches_all_regions();
        assert_eq!(per_lane.lane_cached_blocks(), 0);
        assert_eq!(all_regions.lane_cached_blocks(), 0);
        assert_eq!(
            per_lane.free_block_count(),
            all_regions.free_block_count(),
            "seed {seed}: different free totals after the drain"
        );
        let (a, b) = (per_lane.contiguity_stats(), all_regions.contiguity_stats());
        assert_eq!(
            (
                a.free_blocks_in_set,
                a.free_extents,
                a.largest_run_blocks,
                a.stripe_reserve_blocks
            ),
            (
                b.free_blocks_in_set,
                b.free_extents,
                b.largest_run_blocks,
                b.stripe_reserve_blocks
            ),
            "seed {seed}: the folded-back free space has a different SHAPE (coalescing differs)"
        );
    }
}

/// The drain must fold everything back while locking only the regions the
/// lanes actually hold blocks in — it used to take every region lock in the
/// pool. Both halves matter: the block count is a leak check (shutdown calls
/// this), the lock count is the fix.
#[test]
fn drain_folds_every_block_back_but_locks_only_the_regions_involved() {
    let allocator = sharded_16_regions_lanes(2);
    allocator.set_stripe_geometry(STRIPE, PHASE);
    let free_before = allocator.free_block_count();

    // Seed two lanes from the pool: one single-block cache, one extent cache.
    let mut handed_out: u64 = 0;
    for _ in 0..3 {
        allocator.allocate_one_for_lane(0).unwrap();
        handed_out += 1;
    }
    handed_out += u64::from(
        allocator
            .allocate_stripe_extent_for_lane(1, STRIPE, STRIPE, PHASE)
            .unwrap()
            .count,
    );
    let cached = allocator.lane_cached_blocks();
    assert!(cached > 0, "the lanes must hold something to drain");
    let touched: usize = {
        let layout = allocator.regions.layout();
        let mut regions: Vec<usize> = allocator.lane_caches[0]
            .lock()
            .unwrap()
            .iter()
            .map(|pba| layout.of(pba.0))
            .collect();
        regions.extend(
            allocator.lane_extent_caches[1]
                .lock()
                .unwrap()
                .iter()
                .map(|e| layout.of(e.start.0)),
        );
        regions.sort_unstable();
        regions.dedup();
        regions.len()
    };

    let drain_acqs = |a: &SpaceAllocator| {
        a.free_lock_stats()
            .iter()
            .find(|s| s.site == "drain")
            .expect("every site is always reported")
            .acquisitions
    };
    let before = drain_acqs(&allocator);
    let blocks_before = allocator.supply_stats().drain_blocks;
    allocator.drain_lane_caches();
    let locks = drain_acqs(&allocator) - before;

    assert_eq!(
        allocator.supply_stats().drain_blocks - blocks_before,
        cached,
        "the drain must fold back exactly what the lanes held"
    );
    assert_eq!(allocator.lane_cached_blocks(), 0);
    // Every handed-out block is still allocated, and the cached remainder is
    // free again: no block was lost or double-counted.
    assert_eq!(allocator.free_block_count(), free_before - handed_out);
    // The whole point: one lock per region involved, not one per region.
    assert!(
        locks <= (touched as u64) * LANE_DRAIN_ROUNDS as u64,
        "drain took {locks} region locks for {touched} involved regions \
             (pool has {})",
        allocator.region_count()
    );
    assert!(
        locks < allocator.region_count() as u64,
        "drain still locks the whole pool: {locks} locks"
    );
}

/// The width filter must never HIDE a region that could have served the
/// request — that would be a spurious `SpaceExhausted` (or a needlessly short
/// fragment). Randomised shapes, every width, checked against the truth.
#[test]
fn width_filter_never_hides_a_region_that_could_serve_the_request() {
    let allocator = sharded_16_regions();
    let mut rng = 0xDEAD_BEEF_CAFE_F00Du64;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    for round in 0..12 {
        let layout = allocator.regions.layout();
        for idx in 0..allocator.region_count() {
            let mut guard = allocator.lock_region(FreeLockSite::Setup, idx);
            let pools = guard.region_mut(idx);
            *pools = FreePools::new();
            let mut at = layout.start(idx).max(RESERVED_BLOCKS);
            for _ in 0..(next() % 4) {
                let width = 1 + (next() % 11) as u32;
                if at + u64::from(width) + 2 >= layout.end(idx) {
                    break;
                }
                pools.general.insert(Extent::new(Pba(at), width));
                at += u64::from(width) + 1 + (next() % 3);
            }
        }
        for width in 1..=12u32 {
            let visible: Vec<usize> = allocator.walk_regions_wide(width, true).collect();
            for idx in 0..allocator.region_count() {
                let guard = allocator.lock_region(FreeLockSite::Audit, idx);
                let serves = guard
                    .region(idx)
                    .largest_allocatable()
                    .is_some_and(|(run, _)| run.count >= width);
                drop(guard);
                assert!(
                    !serves || visible.contains(&idx),
                    "round {round} width {width}: region {idx} can serve it but the \
                         filtered walk skipped it"
                );
            }
            // And the unfiltered walk always offers everyone.
            assert_eq!(
                allocator.walk_regions_wide(width, false).count(),
                allocator.region_count()
            );
        }
    }
}

/// The point of the filter, as lock COUNT: a request wider than any region's
/// largest run must cost ZERO region locks instead of one per region.
#[test]
fn a_too_wide_refill_takes_no_region_locks() {
    let allocator = sharded_16_regions();
    let layout = allocator.regions.layout();
    for idx in 0..allocator.region_count() {
        let mut guard = allocator.lock_region(FreeLockSite::Setup, idx);
        let pools = guard.region_mut(idx);
        *pools = FreePools::new();
        // Every region holds free space, but no run wider than 5 blocks —
        // the box's exhausted shape (`largest_run = 5`).
        let base = layout.start(idx).max(RESERVED_BLOCKS);
        for k in 0..4u64 {
            pools.general.insert(Extent::new(Pba(base + k * 8), 5));
        }
    }
    let acqs = |a: &SpaceAllocator| {
        a.free_lock_stats()
            .iter()
            .find(|s| s.site == "writer_unaligned")
            .expect("every site is always reported")
            .acquisitions
    };

    let before = acqs(&allocator);
    assert!(allocator.refill_extent_lane(0, 6, 8, true).is_none());
    assert_eq!(
        acqs(&allocator),
        before,
        "a 6-block request against a 5-block pool must not lock a single region"
    );

    // The unfiltered ENOSPC pass still walks everyone...
    assert!(allocator.refill_extent_lane(0, 6, 8, false).is_none());
    assert_eq!(acqs(&allocator) - before, allocator.region_count() as u64);

    // ...and a width the pool CAN serve is found under exactly one lock.
    let before = acqs(&allocator);
    assert!(allocator.refill_extent_lane(0, 5, 8, true).is_some());
    assert_eq!(acqs(&allocator) - before, 1);
}

/// A stale-HIGH hint (the region emptied after the load) must not livelock the
/// argmax: the failed probe republishes the truth, so the retry picks someone
/// else and the caller still gets the real largest run.
#[test]
fn stale_high_largest_hint_self_corrects() {
    let allocator = sharded_16_regions();
    // Empty region 1, then lie about it being the widest in the pool.
    {
        let mut guard = allocator.lock_region(FreeLockSite::Setup, 1);
        *guard.region_mut(1) = FreePools::new();
    }
    allocator.regions.largest_hint[1].store(u64::MAX, Ordering::Relaxed);

    let picked = allocator
        .take_largest_regionwise(FreeLockSite::SmallAlloc)
        .expect("the rest of the pool still has runs");
    assert!(!Extent::new(Pba(0), 1).contains(picked.start));
    assert_eq!(
        allocator.regions.largest_hint[1].load(Ordering::Relaxed),
        0,
        "the failed probe must have republished region 1's truth"
    );
}

/// The point of the guard: at the exhaustion boundary with empty lanes, the
/// allocator must stop paying for an all-regions drain that can only return
/// nothing. `free_lock.drain` acquisitions are the proof — one drain costs one
/// per region.
#[test]
fn enospc_with_empty_lanes_takes_no_region_locks_for_the_drain() {
    let allocator = allocator(RESERVED_BLOCKS + 8, 2);
    let drain_acqs = |a: &SpaceAllocator| {
        a.free_lock_stats()
            .iter()
            .find(|s| s.site == "drain")
            .expect("every site is always reported")
            .acquisitions
    };
    // Consume the pool through the non-lane path so nothing is ever cached.
    while allocator.allocate_extent(1).is_ok() {}
    assert_eq!(allocator.lane_cached_blocks(), 0);
    let before = drain_acqs(&allocator);

    for _ in 0..4 {
        assert!(matches!(
            allocator.allocate_one(),
            Err(OnyxError::SpaceExhausted)
        ));
        assert!(matches!(
            allocator.allocate_extent(4),
            Err(OnyxError::SpaceExhausted)
        ));
        assert!(matches!(
            allocator.allocate_exact_extent_for_lane(0, 4),
            Err(OnyxError::SpaceExhausted)
        ));
    }
    assert_eq!(
        drain_acqs(&allocator),
        before,
        "a provably empty drain still took region locks"
    );
    let supply = allocator.supply_stats();
    assert_eq!(supply.drains, 0, "no drain should have run");
    assert!(supply.drain_skips >= 12, "skips: {}", supply.drain_skips);

    // Positive control: with a lane holding space, the drain must still run.
    allocator.seed_lane_extent_cache(1, Extent::new(Pba(RESERVED_BLOCKS + 1), 2));
    assert!(allocator.allocate_extent(2).is_ok());
    assert_eq!(allocator.supply_stats().drains, 1);
    assert!(drain_acqs(&allocator) > before);
}

#[test]
fn exact_miss_coalesces_global_and_lane_boundary_before_enospc() {
    let allocator = allocator(32, 1);
    {
        let mut pools = allocator.test_region_pools(0);
        *pools = FreePools::new();
        pools.general.insert(Extent::single(Pba(13)));
    }
    allocator.seed_lane_extent_cache(0, Extent::new(Pba(14), 2));

    assert_eq!(
        allocator.allocate_exact_extent_for_lane(0, 3).unwrap(),
        Extent::new(Pba(13), 3)
    );
}

#[test]
fn cross_pool_selection_is_first_fit_for_single_exact_and_lane_refill() {
    let mut pools = FreePools::new();
    pools.general.insert(Extent::new(Pba(100), 16));
    pools.stripe_reserve.insert(Extent::new(Pba(10), 24));

    assert_eq!(
        SpaceAllocator::take_first_from_pools(&mut pools, 1).map(|e| e.start),
        Some(Pba(10))
    );
    assert_eq!(
        SpaceAllocator::take_exact_from_pools(&mut pools, 4, 4),
        Some(Extent::new(Pba(11), 4))
    );

    let allocator = allocator(128, 1);
    *allocator.test_region_pools(0) = pools;
    assert_eq!(
        allocator.refill_extent_lane(0, 2, 8, true),
        Some(Extent::new(Pba(15), 2))
    );
}

#[test]
fn quarantine_cannot_miss_pool_to_lane_refill_in_flight() {
    let allocator = Arc::new(allocator(128, 1));
    let target = Extent::new(Pba(10), STRIPE);
    {
        let mut pools = allocator.test_region_pools(0);
        *pools = FreePools::new();
        pools.reset_geometry(STRIPE, PHASE);
        pools.stripe_reserve.insert(target);
    }

    let held_lane = allocator.lane_caches[0].lock().unwrap();
    let refill = {
        let allocator = Arc::clone(&allocator);
        thread::spawn(move || {
            allocator
                .refill_one_lane_from_global(0, LANE_CACHE_REFILL_SIZE)
                .expect("test reserve contains a refill")
        })
    };
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if matches!(
            allocator.regions.pools[0].try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ) {
            break;
        }
        assert!(Instant::now() < deadline, "refill never acquired FreePools");
        thread::yield_now();
    }
    let quarantine = {
        let allocator = Arc::clone(&allocator);
        thread::spawn(move || allocator.begin_defrag_quarantine(target))
    };
    drop(held_lane);

    let first = refill.join().unwrap();
    quarantine.join().unwrap().unwrap();
    assert_eq!(first, target.start);
    assert_eq!(
        allocator.defrag_quarantine_progress(target.start),
        Some((u64::from(STRIPE - 1), u64::from(STRIPE)))
    );
    assert!(allocator.lane_caches[0]
        .lock()
        .unwrap()
        .iter()
        .all(|pba| !target.contains(*pba)));

    allocator
        .track_alloc(Extent::single(first), "quarantine_refill_test")
        .unwrap();
    allocator.allocated_blocks.fetch_add(1, Ordering::Relaxed);
    allocator.free_blocks.fetch_sub(1, Ordering::Relaxed);
    allocator.free_one(first).unwrap();
    assert!(allocator.complete_defrag_quarantine(target.start).unwrap());
}

#[test]
fn free_coverage_crosses_general_reserve_boundaries_by_run() {
    let allocator = allocator(32, 0);
    allocator.set_stripe_geometry(STRIPE, PHASE);
    let usable = Extent::new(Pba(RESERVED_BLOCKS), 32 - RESERVED_BLOCKS as u32);
    assert!(allocator.is_extent_free(usable));

    let allocated = allocator
        .allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
        .unwrap();
    assert!(!allocator.is_extent_free(usable));
    allocator.free_extent(allocated).unwrap();
    assert!(allocator.is_extent_free(usable));
}

#[test]
fn explicit_alternate_stripe_geometry_keeps_legacy_api_working() {
    let allocator = allocator(64, 1);
    allocator.set_stripe_geometry(STRIPE, PHASE);

    let extent = allocator
        .allocate_stripe_extent_for_lane(0, 3, 4, 0)
        .unwrap();

    assert_eq!(extent.count, 4);
    assert_eq!(extent.start.0 % 4, 0);
}

/// The resident defragger's enumeration source. Its contract is narrow: one
/// window start per stripe window that holds a retired block, ascending,
/// deduplicated, resumable, and capped.
#[test]
fn retired_stripe_windows_enumerates_deduped_and_resumes() {
    let allocator = allocator(4096, 0);
    allocator.set_stripe_geometry(STRIPE, PHASE);
    let grid = |w: u64| {
        let first = RESERVED_BLOCKS
            + (u64::from(STRIPE) - (RESERVED_BLOCKS + u64::from(PHASE)) % u64::from(STRIPE))
                % u64::from(STRIPE);
        first + w * u64::from(STRIPE)
    };
    let claimed = allocator.allocate_extent(2048).unwrap();
    assert!(claimed.start.0 <= grid(0));

    // Two retired blocks inside ONE window must collapse to one start; a
    // window with none must not appear at all.
    allocator.retire_one(Pba(grid(0))).unwrap();
    allocator.retire_one(Pba(grid(0) + 2)).unwrap();
    allocator.retire_one(Pba(grid(3) + 1)).unwrap();

    let mut cursor = 0u64;
    let (windows, lapped) = allocator.retired_stripe_windows(&mut cursor, STRIPE, PHASE, 64);
    assert_eq!(windows, vec![grid(0), grid(3)], "deduped, ascending");
    assert!(lapped, "the walk ran to the end of the address space");
    assert_eq!(cursor, 0, "a completed lap resets the cursor");

    // Capped: one window per call, resuming where it stopped.
    let mut cursor = 0u64;
    let (first, lapped) = allocator.retired_stripe_windows(&mut cursor, STRIPE, PHASE, 1);
    assert_eq!(first, vec![grid(0)]);
    assert!(!lapped);
    let (second, _) = allocator.retired_stripe_windows(&mut cursor, STRIPE, PHASE, 1);
    assert_eq!(second, vec![grid(3)], "resumed past the first window");

    // A retired extent straddling two windows yields both.
    allocator
        .retire_extent(Extent::new(Pba(grid(6) + 5), 2))
        .unwrap();
    let mut cursor = grid(6);
    let (straddle, _) = allocator.retired_stripe_windows(&mut cursor, STRIPE, PHASE, 64);
    assert_eq!(straddle, vec![grid(6), grid(7)]);

    // No geometry / degenerate stripe: nothing to clear, no lock trips.
    let mut cursor = 0u64;
    assert_eq!(
        allocator.retired_stripe_windows(&mut cursor, 1, 0, 64),
        (Vec::new(), false)
    );
    let mut cursor = 0u64;
    assert_eq!(
        allocator.retired_stripe_windows(&mut cursor, STRIPE, PHASE, 0),
        (Vec::new(), false)
    );
}

#[test]
fn quarantine_extracts_and_splits_lane_extent_cache() {
    let allocator = allocator(32, 1);
    assert_eq!(
        allocator.allocate_exact_extent_for_lane(0, 1).unwrap(),
        Extent::single(Pba(RESERVED_BLOCKS))
    );
    allocator.set_stripe_geometry(STRIPE, PHASE);
    let target = Extent::new(Pba(10), STRIPE);

    allocator.begin_defrag_quarantine(target).unwrap();

    assert_eq!(
        allocator.defrag_quarantine_progress(target.start),
        Some((STRIPE as u64, STRIPE as u64))
    );
    // The quarantine split one cached run into a head (9) and a tail (16);
    // both stay lane-local. Lane extent caches are ordered by DESCENDING
    // start (carves are taken from the back so a lane emits ascending PBAs —
    // see `push_extent_cache`), so the tail sorts ahead of the head here.
    let cached = allocator.lane_extent_caches[0].lock().unwrap().clone();
    assert_eq!(
        cached,
        vec![Extent::new(Pba(16), 16), Extent::single(Pba(9))]
    );
    assert!(allocator.complete_defrag_quarantine(target.start).unwrap());
    assert_eq!(
        allocator.contiguity_stats().stripe_reserve_blocks,
        STRIPE as u64
    );
}

#[test]
fn quarantine_extracts_single_block_lane_cache_members() {
    let allocator = allocator(32, 1);
    assert_eq!(
        allocator.allocate_one_for_lane(0).unwrap(),
        Pba(RESERVED_BLOCKS)
    );
    allocator.set_stripe_geometry(STRIPE, PHASE);
    let target = Extent::new(Pba(10), STRIPE);

    allocator.begin_defrag_quarantine(target).unwrap();

    assert_eq!(
        allocator.defrag_quarantine_progress(target.start),
        Some((STRIPE as u64, STRIPE as u64))
    );
    assert!(allocator.lane_caches[0]
        .lock()
        .unwrap()
        .iter()
        .all(|pba| !target.contains(*pba)));
    assert!(allocator.complete_defrag_quarantine(target.start).unwrap());
}

#[test]
fn quarantine_routes_releases_until_target_is_complete() {
    let allocator = allocator(128, 0);
    allocator.set_stripe_geometry(STRIPE, PHASE);
    let target = allocator
        .allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
        .unwrap();
    assert_eq!(target, Extent::new(Pba(10), STRIPE));
    allocator.begin_defrag_quarantine(target).unwrap();

    allocator
        .free_extent(Extent::new(target.start, STRIPE / 2))
        .unwrap();
    assert_eq!(
        allocator.defrag_quarantine_progress(target.start),
        Some(((STRIPE / 2) as u64, STRIPE as u64))
    );
    assert!(!allocator.complete_defrag_quarantine(target.start).unwrap());

    allocator
        .free_extent(Extent::new(
            Pba(target.start.0 + (STRIPE / 2) as u64),
            STRIPE / 2,
        ))
        .unwrap();
    assert!(allocator.complete_defrag_quarantine(target.start).unwrap());
    assert!(!allocator.is_defrag_quarantined(target));
    assert!(allocator.contiguity_stats().stripe_reserve_blocks >= STRIPE as u64);
}

/// Concurrent allocate / free / retire / reclaim / defrag-quarantine traffic
/// on a pool small enough to run out of space, with the allocator's live-PBA
/// tracker armed.
///
/// This is the shape that produced the box corruption: a fully-fragmented
/// pool under sustained `SpaceExhausted` (millions of `passthrough alloc
/// failed` in the hour the first CRC error appeared), 16 flush lanes
/// allocating through their per-lane caches, reclaim returning retired
/// extents, and the resident defrag thread quarantining and publishing
/// stripe windows. The exhaustion boundary is where the lane caches get
/// drained back into the pools, so it is the one place where "logically
/// free" blocks change owner without a per-block proof.
///
/// `track_alloc` fails the allocation the moment a block is handed out
/// twice, so any duplicate surfaces as an error containing "duplicate
/// allocation" rather than as silent data loss.
#[test]
fn concurrent_exhaustion_and_quarantine_never_double_allocate() {
    const LANES: usize = 8;
    const BLOCKS: u64 = 4096;
    const ITERS: usize = 3000;

    let allocator = Arc::new(SpaceAllocator::new_tracked(
        BLOCKS * BLOCK_SIZE as u64,
        LANES,
        8,
    ));
    allocator.set_stripe_geometry(STRIPE, PHASE);
    let stop = Arc::new(AtomicBool::new(false));
    let dupes = Arc::new(Mutex::new(Vec::<String>::new()));

    let mut workers = Vec::new();
    for lane in 0..LANES {
        let allocator = allocator.clone();
        let dupes = dupes.clone();
        workers.push(thread::spawn(move || {
            // Per-lane LCG: deterministic mix, different stream per lane.
            let mut rng = 0x2545_F491_4F6C_DD1Du64 ^ ((lane as u64 + 1) << 32);
            let mut next = move || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                rng
            };
            let mut held: Vec<Extent> = Vec::new();
            let mut record = |err: &OnyxError| {
                let text = err.to_string();
                if text.contains("duplicate allocation") {
                    dupes.lock().unwrap().push(text);
                }
            };
            for _ in 0..ITERS {
                let roll = next() % 100;
                let got = if roll < 40 {
                    allocator.allocate_stripe_extent_for_lane(lane, STRIPE, STRIPE, PHASE)
                } else if roll < 70 {
                    allocator.allocate_extent_for_lane(lane, 1 + (next() % STRIPE as u64) as u32)
                } else {
                    allocator.allocate_one_for_lane(lane).map(Extent::single)
                };
                match got {
                    Ok(extent) => held.push(extent),
                    Err(OnyxError::SpaceExhausted) => {}
                    Err(error) => record(&error),
                }
                // Give space back so the pool keeps churning at the
                // exhaustion boundary instead of just filling up once.
                if held.len() > 4 && next() % 100 < 60 {
                    let extent = held.swap_remove((next() as usize) % held.len());
                    if next() % 2 == 0 {
                        if let Err(error) = allocator.free_extent(extent) {
                            record(&error);
                            held.push(extent);
                        }
                    } else {
                        match allocator.retire_extent(extent) {
                            Ok(_) => {
                                // Reclaim is the GC gate's job; model both the
                                // single and the batch entry point.
                                let running = AtomicBool::new(true);
                                let reclaimed = if next() % 2 == 0 {
                                    allocator
                                        .reclaim_retired_extent(extent)
                                        .map(|freed| u64::from(freed) * u64::from(extent.count))
                                } else {
                                    allocator
                                        .reclaim_retired_extents_batch(&[extent], &running)
                                        .map(|(blocks, _)| blocks)
                                };
                                match reclaimed {
                                    Ok(0) => held.push(extent),
                                    Ok(_) => {}
                                    Err(error) => {
                                        record(&error);
                                        held.push(extent);
                                    }
                                }
                            }
                            Err(error) => {
                                record(&error);
                                held.push(extent);
                            }
                        }
                    }
                }
            }
            held
        }));
    }

    // Defrag: quarantine stripe-aligned windows, publish or cancel them.
    let quarantiner = {
        let allocator = allocator.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            let mut start = RESERVED_BLOCKS;
            let mut published = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let aligned = SpaceAllocator::align_up_pba(start, STRIPE as u64, PHASE as u64);
                if aligned + STRIPE as u64 >= BLOCKS - RESERVED_BLOCKS {
                    start = RESERVED_BLOCKS;
                    continue;
                }
                let target = Extent::new(Pba(aligned), STRIPE);
                start = aligned + STRIPE as u64;
                if allocator.begin_defrag_quarantine(target).is_err() {
                    continue;
                }
                for _ in 0..4 {
                    match allocator.complete_defrag_quarantine(target.start) {
                        Ok(true) => {
                            published += 1;
                            break;
                        }
                        Ok(false) => {}
                        Err(_) => break,
                    }
                    std::thread::yield_now();
                }
                allocator.cancel_defrag_quarantine(target.start);
            }
            published
        })
    };

    let mut still_held: Vec<Extent> = Vec::new();
    for worker in workers {
        still_held.extend(worker.join().expect("worker panicked"));
    }
    stop.store(true, Ordering::Relaxed);
    quarantiner.join().expect("quarantiner panicked");

    let dupes = dupes.lock().unwrap();
    assert!(
        dupes.is_empty(),
        "allocator handed the same block to two callers: {:?}",
        &dupes[..dupes.len().min(8)]
    );
    allocator.assert_free_sets_consistent();

    // End state: the tracker must hold exactly the blocks the workers still
    // own. Anything else means an allocation or a release went unaccounted,
    // which is the same drift that lets a quarantine publish a live block.
    let expected: BTreeSet<Pba> = still_held
        .iter()
        .flat_map(|extent| (0..extent.count).map(|i| Pba(extent.start.0 + i as u64)))
        .collect();
    let tracked = allocator.tracked_live_pbas();
    assert_eq!(
        tracked, expected,
        "live-PBA tracker disagrees with what the workers hold"
    );
}

/// Publishing a quarantine is the only path that returns a whole stripe
/// window to the allocatable pool without having verified each block, so its
/// gate must be structural. It used to be `free_parts.blocks_total() ==
/// range.count`; when that aggregate drifted upward, a window with LIVE
/// blocks in it got published, the next writer overwrote them, and reads of
/// the untouched LBAs failed onyx's own CRC check (box, 2026-08-12: 476
/// double-claimed blocks, every consecutive run of them inside one
/// stripe-aligned window, 1-5 of 6 blocks each).
#[test]
fn quarantine_never_publishes_a_window_that_still_holds_a_live_block() {
    let allocator = allocator(128, 0);
    allocator.set_stripe_geometry(STRIPE, PHASE);
    let target = allocator
        .allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
        .unwrap();
    // Keep the last block of the window LIVE; free the rest.
    let live = Pba(target.start.0 + (STRIPE - 1) as u64);
    allocator.begin_defrag_quarantine(target).unwrap();
    allocator
        .free_extent(Extent::new(target.start, STRIPE - 1))
        .unwrap();
    assert!(!allocator.complete_defrag_quarantine(target.start).unwrap());

    // Drift the block counter up to the window size without making the set
    // cover it. The old counter gate would publish here.
    let outside = Extent::single(Pba(target.end_pba().0 + 1));
    allocator.inject_quarantine_free_part_for_test(target.start, outside);
    assert_eq!(
        allocator.defrag_quarantine_progress(target.start),
        Some((STRIPE as u64, STRIPE as u64)),
        "counter now claims the window is complete"
    );

    assert!(
        !allocator.complete_defrag_quarantine(target.start).unwrap(),
        "must refuse to publish a window it cannot prove is fully free"
    );
    // Refusing is not enough — the target must not stay parked forever
    // either, so the refusal cancels it and hands back the real free parts.
    assert!(!allocator.is_defrag_quarantined(target));
    assert!(
        allocator.free_overlap_blocks(Extent::single(live)) == 0,
        "the live block must never become allocatable"
    );
    assert_eq!(
        allocator.free_overlap_blocks(Extent::new(target.start, STRIPE - 1)),
        (STRIPE - 1) as u64,
        "the genuinely-free part of the window comes back"
    );
}

#[test]
fn dedup_pin_and_quarantine_publication_are_atomic() {
    let allocator = Arc::new(allocator(128, 0));
    allocator.set_stripe_geometry(STRIPE, PHASE);
    let target = allocator
        .allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
        .unwrap();
    let old_pin = allocator
        .pin_dedup_target_if_allowed(target.start, target.count)
        .expect("pin before publication must succeed");

    let worker = {
        let allocator = Arc::clone(&allocator);
        thread::spawn(move || allocator.begin_defrag_quarantine(target))
    };
    let deadline = Instant::now() + Duration::from_secs(2);
    while !allocator.is_defrag_quarantined(target) {
        assert!(Instant::now() < deadline, "quarantine was not published");
        thread::yield_now();
    }
    assert!(allocator
        .pin_dedup_target_if_allowed(target.start, target.count)
        .is_none());
    assert!(
        !worker.is_finished(),
        "pre-publication pin must be waited out"
    );

    drop(old_pin);
    worker.join().unwrap().unwrap();
    assert!(allocator.cancel_defrag_quarantine(target.start));
}
