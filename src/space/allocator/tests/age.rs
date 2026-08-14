//! Reclaim-grace age mechanism: the fix for the re-aging bottleneck where a
//! contiguous retired region perpetually absorbing fresh neighbors never
//! satisfied the grace. Per-original-retire `retired_at` (injected here via
//! `retire_extent_at`) is fixed and never refreshed by coalescing.
use super::*;

fn alloc_first(a: &SpaceAllocator, n: usize) -> u64 {
    let first = a.allocate_one().unwrap();
    for _ in 1..n {
        a.allocate_one().unwrap();
    }
    first.0
}

fn new_alloc(blocks: u64) -> SpaceAllocator {
    SpaceAllocator::new(blocks * BLOCK_SIZE as u64, 0)
}

const GRACE: Duration = Duration::from_secs(10);
fn secs(s: u64) -> Duration {
    Duration::from_secs(s)
}

/// contiguity_stats reflects the free set (blocks/extents/largest/eff) and
/// eff_capacity is None without geometry, Some with it.
#[test]
fn contiguity_stats_reflects_free_set() {
    let a = new_alloc(8192);
    let s0 = a.contiguity_stats();
    assert_eq!(s0.free_blocks_in_set, 8192 - RESERVED_BLOCKS);
    // A fresh pool is one run per address region (regions never coalesce
    // across their boundary), so both of these are region-relative.
    assert_eq!(s0.free_extents, a.region_count() as u64);
    if a.region_count() == 1 {
        assert_eq!(s0.largest_run_blocks as u64, 8192 - RESERVED_BLOCKS);
    }
    assert_eq!(s0.stripe_capable_blocks, None, "no geometry configured");

    a.set_stripe_geometry(6, 2);
    let s1 = a.contiguity_stats();
    // Single run starting at RESERVED_BLOCKS=8: head_pad(8,6,2)=((8+2)%6=4→2),
    // eff = total - head, floored to whole stripes.
    let head = {
        let r = (RESERVED_BLOCKS + 2) % 6;
        if r == 0 {
            0
        } else {
            6 - r
        }
    };
    let eff = 8192 - RESERVED_BLOCKS - head;
    if a.region_count() == 1 {
        assert_eq!(s1.stripe_capable_blocks, Some(eff / 6 * 6));
    } else {
        // Sharded, each boundary is stripe-aligned, so no whole stripe is
        // lost — only the per-region head pads differ from one big run.
        let capable = s1.stripe_capable_blocks.unwrap();
        assert!(capable <= eff / 6 * 6 && capable + 6 * a.region_count() as u64 >= eff);
    }

    // Punch holes: allocate 3 blocks (front carve keeps one run), then
    // free-with-gap via retire is separate — just re-check counts move.
    let _ = a.allocate_one().unwrap();
    let s2 = a.contiguity_stats();
    assert_eq!(s2.free_blocks_in_set, 8192 - RESERVED_BLOCKS - 1);
}

/// grow_capacity appends the new top range to the free set (both the free
/// atomic and the free set grow by the delta) and is a no-op when the new
/// size is not larger.
#[test]
fn grow_capacity_appends_new_free_range() {
    let a = new_alloc(8192);
    let old_free = a.free_block_count();
    assert_eq!(a.total_block_count(), 8192);
    assert_eq!(old_free, 8192 - RESERVED_BLOCKS);

    // Grow to 16384 blocks (io-addressable size = blocks * BLOCK_SIZE).
    let new_total = a.grow_capacity(16384 * BLOCK_SIZE as u64).unwrap();
    assert_eq!(new_total, 16384);
    assert_eq!(a.total_block_count(), 16384);
    assert_eq!(a.free_block_count(), old_free + 8192);
    // The free set gained exactly the [8192, 16384) range worth of blocks
    // (coalesced with the existing top run or not — the total is invariant).
    assert_eq!(
        a.contiguity_stats().free_blocks_in_set,
        16384 - RESERVED_BLOCKS
    );

    // Equal / smaller is a no-op — the frontier never regresses.
    assert_eq!(a.grow_capacity(8192 * BLOCK_SIZE as u64).unwrap(), 16384);
    assert_eq!(a.total_block_count(), 16384);
    assert_eq!(a.free_block_count(), old_free + 8192);
}

/// The batched window classifier must agree, window for window, with the
/// per-window `free_overlap_blocks` / `retired_overlap_blocks` pair it
/// replaces — including free blocks parked inside an active quarantine's
/// `free_parts` (which are still physically free, just unallocatable) and
/// windows spread across several allocator regions.
#[test]
fn classify_stripe_windows_matches_per_window_ground_truth() {
    const STRIPE: u32 = 6;
    let a = new_alloc(8192);
    a.set_stripe_geometry(STRIPE, 0);
    // Claim everything, then carve a varied free/retired/live pattern.
    let total = 8192 - RESERVED_BLOCKS;
    let mut claimed = 0u64;
    while claimed < total {
        claimed += u64::from(a.allocate_extent((total - claimed) as u32).unwrap().count);
    }
    let t0 = Instant::now();
    let base = (RESERVED_BLOCKS / u64::from(STRIPE) + 1) * u64::from(STRIPE);
    for w in 0..64u64 {
        let start = base + w * u64::from(STRIPE);
        match w % 4 {
            // Fully free window.
            0 => a.free_extent(Extent::new(Pba(start), STRIPE)).unwrap(),
            // Mostly free, one live pinner in the middle.
            1 => {
                a.free_extent(Extent::new(Pba(start), 3)).unwrap();
                a.free_extent(Extent::new(Pba(start + 4), 2)).unwrap();
            }
            // Free head + retired tail (reclaimable, no live pinner).
            2 => {
                a.free_extent(Extent::new(Pba(start), 4)).unwrap();
                a.retire_extent_at(Extent::new(Pba(start + 4), 2), t0)
                    .unwrap();
            }
            // Fully live.
            _ => {}
        }
    }
    // Quarantine one of the mostly-free windows: its free blocks move into
    // `free_parts` and must still be counted as free.
    let quarantined = Extent::new(Pba(base + u64::from(STRIPE)), STRIPE);
    a.begin_defrag_quarantine(quarantined).unwrap();

    let starts: Vec<u64> = (0..64).map(|w| base + w * u64::from(STRIPE)).collect();
    let batched = a.classify_stripe_windows(&starts, STRIPE);
    assert_eq!(batched.len(), starts.len());
    for (i, &start) in starts.iter().enumerate() {
        let window = Extent::new(Pba(start), STRIPE);
        assert_eq!(
            (u64::from(batched[i].0), u64::from(batched[i].1)),
            (
                a.free_overlap_blocks(window),
                a.retired_overlap_blocks(window),
            ),
            "window {start} disagrees with the per-window query"
        );
        assert!(
            batched[i].0 + batched[i].1 <= STRIPE,
            "window {start} over-counts its own span"
        );
    }
    // Non-vacuity: the pattern really does produce all three shapes.
    assert!(batched.iter().any(|&(free, _)| free == STRIPE));
    assert!(batched
        .iter()
        .any(|&(free, retired)| free > 0 && free + retired < STRIPE));
    assert!(batched.iter().any(|&(_, retired)| retired > 0));
    assert!(batched
        .iter()
        .any(|&(free, retired)| free == 0 && retired == 0));
    // The quarantined window's free blocks were NOT lost by the classifier.
    let qi = starts
        .iter()
        .position(|&s| s == quarantined.start.0)
        .unwrap();
    assert_eq!(batched[qi].0, 5, "quarantined free_parts must still count");

    assert!(a.classify_stripe_windows(&[], STRIPE).is_empty());
    assert!(a.classify_stripe_windows(&starts, 0).is_empty());
}

/// retired_overlap_blocks sums clamped intersections, including a retired
/// extent reaching into the range from below.
#[test]
fn retired_overlap_blocks_counts_intersections() {
    let a = new_alloc(128);
    let n = alloc_first(&a, 40); // n..n+40 allocated
    let t0 = Instant::now();
    // Retire [n+2, n+6) and [n+10, n+12).
    a.retire_extent_at(Extent::new(Pba(n + 2), 4), t0).unwrap();
    a.retire_extent_at(Extent::new(Pba(n + 10), 2), t0).unwrap();
    // Range covering both fully.
    assert_eq!(a.retired_overlap_blocks(Extent::new(Pba(n), 20)), 6);
    // Range starting inside the first retired run (reach-from-below).
    assert_eq!(a.retired_overlap_blocks(Extent::new(Pba(n + 4), 4)), 2);
    // Range with no overlap.
    assert_eq!(a.retired_overlap_blocks(Extent::new(Pba(n + 20), 5)), 0);
    // Range clipping the tail of the second run only.
    assert_eq!(a.retired_overlap_blocks(Extent::new(Pba(n + 11), 8)), 1);
}

/// HEADLINE: an aged block reclaims even while an adjacent younger block keeps
/// arriving — the exact scenario the old coalesced-key grace map starved.
#[test]
fn no_reaging_under_adjacent_retire() {
    let a = new_alloc(8192);
    let n = alloc_first(&a, 2);
    let t0 = Instant::now();
    a.retire_extent_at(Extent::single(Pba(n)), t0).unwrap();
    a.retire_extent_at(Extent::single(Pba(n + 1)), t0 + secs(5))
        .unwrap();
    // t0+11s: N aged (11≥10), N+1 still young (age 6<10).
    let (cands, deferred) = a.aged_candidates(64, GRACE, t0 + secs(11));
    assert_eq!(cands, vec![Extent::new(Pba(n), 1)]);
    assert_eq!(deferred, 1, "young neighbor deferred, NOT re-aging N");
    // Later both age in and (adjacent) merge into one fat candidate.
    let (cands2, deferred2) = a.aged_candidates(64, GRACE, t0 + secs(20));
    assert_eq!(cands2, vec![Extent::new(Pba(n), 2)]);
    assert_eq!(deferred2, 0);
}

/// Safety: a just-retired block is never a candidate before its grace.
#[test]
fn young_block_not_emitted_before_grace() {
    let a = new_alloc(64);
    let n = alloc_first(&a, 1);
    let t0 = Instant::now();
    a.retire_extent_at(Extent::single(Pba(n)), t0).unwrap();
    let (cands, deferred) = a.aged_candidates(64, GRACE, t0 + secs(5));
    assert!(cands.is_empty());
    assert_eq!(deferred, 1);
}

/// Idempotent re-retire does not refresh the original age (no re-aging).
#[test]
fn reretire_does_not_refresh_age() {
    let a = new_alloc(64);
    let n = alloc_first(&a, 1);
    let t0 = Instant::now();
    assert_eq!(a.retire_extent_at(Extent::single(Pba(n)), t0).unwrap(), 1);
    // Re-retire much later → newly==0, and the age must STILL be t0.
    assert_eq!(
        a.retire_extent_at(Extent::single(Pba(n)), t0 + secs(8))
            .unwrap(),
        0,
        "already-retired → no new blocks"
    );
    assert_eq!(a.retired_block_count(), 1);
    // At t0+11s it is eligible (age 11≥10). If the re-retire had refreshed to
    // t0+8s it would still be young (age 3<10) and NOT emitted.
    let (cands, _) = a.aged_candidates(64, GRACE, t0 + secs(11));
    assert_eq!(cands, vec![Extent::new(Pba(n), 1)]);
}

/// Partial-overlap retire records only the genuinely-new tail with a newer
/// age; the already-retired prefix keeps its original (older) age.
#[test]
fn partial_overlap_ages_only_new_tail() {
    let a = new_alloc(64);
    let n = alloc_first(&a, 6);
    let t0 = Instant::now();
    assert_eq!(a.retire_extent_at(Extent::new(Pba(n), 3), t0).unwrap(), 3);
    // [N+1,3) overlaps N+1,N+2 (already retired) → only N+3 is new.
    assert_eq!(
        a.retire_extent_at(Extent::new(Pba(n + 1), 3), t0 + secs(5))
            .unwrap(),
        1
    );
    assert_eq!(a.retired_block_count(), 4);
    // t0+11s: N..N+2 aged (t0), N+3 young (t5, age 6) → emit [N,3] only.
    let (cands, deferred) = a.aged_candidates(64, GRACE, t0 + secs(11));
    assert_eq!(cands, vec![Extent::new(Pba(n), 3)]);
    assert_eq!(deferred, 1);
}

/// Throughput: contiguous aged retires emit as one fat extent, and the budget
/// is in BLOCKS (a per-extent cap would collapse throughput).
#[test]
fn aged_candidates_merge_and_block_budget() {
    let a = new_alloc(4096);
    let n = alloc_first(&a, 1000);
    let t0 = Instant::now();
    a.retire_extent_at(Extent::new(Pba(n), 1000), t0).unwrap();
    // Whole contiguous run as ONE extent.
    let (cands, _) = a.aged_candidates(10_000, GRACE, t0 + secs(11));
    assert_eq!(cands, vec![Extent::new(Pba(n), 1000)]);
    // Block budget truncates to exactly 400 blocks (not 1 extent).
    let (capped, _) = a.aged_candidates(400, GRACE, t0 + secs(11));
    assert_eq!(capped, vec![Extent::new(Pba(n), 400)]);
}

/// PERF microbench (NOT a correctness gate): isolate the two per-GC-cycle
/// reclaim-SELECTION costs that scale with retired-set depth —
/// `retired_block_count()` (O(#retired extents), called once/cycle purely
/// for the depth gauge) and `aged_candidates()` (walks the set + prunes the
/// age log). Directly populates the private structures to model the prod
/// steady state (60M-deep, heavily fragmented) without the alloc/free
/// machinery. Run: `cargo test --release -p onyx-storage --lib
/// bench_reclaim_selection_scaling -- --ignored --nocapture`.
#[test]
#[ignore = "perf microbench"]
fn bench_reclaim_selection_scaling() {
    let base = Instant::now();
    let now = base + Duration::from_secs(100);
    let grace = Duration::from_secs(30);
    // Young front modelled as one grace-window of retires (the only entries
    // the age log holds in steady state — aged_candidates prunes the rest
    // each cycle). `aged_only`=age log already empty (best case: walk emits
    // budget off the front and stops). `all_in_age`=degenerate worst case
    // where a slow cycle let the age log accumulate to full depth before a
    // prune (bounds the retain()/sum() cost).
    for &n in &[1_000_000u64, 10_000_000, 30_000_000, 60_000_000] {
        for mode in ["aged_only", "front_young", "all_in_age"] {
            let dev = (2 * n + RESERVED_BLOCKS + 16) * BLOCK_SIZE as u64;
            let a = SpaceAllocator::new(dev, 0);
            let young_front = 400_000u64.min(n);
            {
                // Direct-insert into the owning shard (the invariant every
                // containment query depends on), bypassing the retire path.
                let layout = a.retired_layout();
                let mut shards = a.lock_all_retired(RetiredLockSite::Setup);
                for i in 0..n {
                    let pba = RESERVED_BLOCKS + 2 * i; // stride 2 → N separate extents (max frag)
                    let shard = &mut shards[layout.of(pba)];
                    shard.set.insert(Extent::new(Pba(pba), 1));
                    match mode {
                        "aged_only" => {}
                        "front_young" => {
                            if i < young_front {
                                shard.age.insert(
                                    pba,
                                    RetiredRun {
                                        count: 1,
                                        retired_at: now,
                                    },
                                );
                            }
                        }
                        "all_in_age" => {
                            let retired_at = if i < young_front { now } else { base };
                            shard.age.insert(
                                pba,
                                RetiredRun {
                                    count: 1,
                                    retired_at,
                                },
                            );
                        }
                        _ => unreachable!(),
                    }
                }
            }
            a.allocated_blocks.store(2 * n, Ordering::Relaxed);
            a.retired_blocks.store(n, Ordering::Relaxed); // direct-insert bypassed the gauge

            // Times the OLD O(#extents) walk we replaced with the O(1) gauge.
            let t = Instant::now();
            let depth = a.retired_block_count_exact();
            let d_rbc = t.elapsed().as_secs_f64() * 1e3;
            debug_assert_eq!(depth, a.retired_block_count());

            let t = Instant::now();
            let (cands, deferred) = a.aged_candidates(262_144, grace, now);
            let d_aged = t.elapsed().as_secs_f64() * 1e3;
            let emitted: u64 = cands.iter().map(|e| e.count as u64).sum();

            println!(
                "N={:>10} mode={:<11} depth={:>10} | retired_block_count={:>8.1}ms | \
                     aged_candidates={:>8.1}ms emitted={:>7} deferred={:>9} cands={}",
                n,
                mode,
                depth,
                d_rbc,
                d_aged,
                emitted,
                deferred,
                cands.len()
            );
        }
    }
}

/// Sub-extent reclaim splits a coalesced extent: free the aged prefix, keep
/// the younger suffix retired.
#[test]
fn reclaim_splits_coalesced_extent() {
    let a = new_alloc(64);
    let n = alloc_first(&a, 6);
    let t0 = Instant::now();
    a.retire_extent_at(Extent::new(Pba(n), 3), t0).unwrap();
    a.retire_extent_at(Extent::new(Pba(n + 3), 3), t0 + secs(5))
        .unwrap(); // adjacent → set coalesces to [N,6]
    let (cands, _) = a.aged_candidates(64, GRACE, t0 + secs(11));
    assert_eq!(cands, vec![Extent::new(Pba(n), 3)]);
    assert!(a.reclaim_retired_extent(Extent::new(Pba(n), 3)).unwrap());
    for off in 0..3 {
        assert!(a.is_free(Pba(n + off)), "aged prefix freed");
    }
    for off in 3..6 {
        assert!(a.is_retired(Pba(n + off)), "younger suffix stays retired");
        assert!(!a.is_free(Pba(n + off)));
    }
    assert_eq!(a.retired_block_count(), 3);
    // The O(1) gauge must agree with the exact walk through retire + the
    // sub-extent split reclaim (drift guard for the atomic).
    assert_eq!(a.retired_block_count(), a.retired_block_count_exact());
}

fn run_flag() -> AtomicBool {
    AtomicBool::new(true)
}

/// The batched reclaim must leave identical allocator state to reclaiming the
/// same extents one-by-one through the single-extent path.
#[test]
fn batch_reclaim_equals_sequence() {
    let mk = || {
        let a = new_alloc(4096);
        let n = alloc_first(&a, 100);
        let t0 = Instant::now();
        for i in 0..100u64 {
            a.retire_extent_at(Extent::single(Pba(n + i)), t0).unwrap();
        }
        (a, n)
    };
    let extents: Vec<Extent> = {
        let (_, n) = mk();
        (0..100u64).map(|i| Extent::single(Pba(n + i))).collect()
    };
    let (a_seq, _) = mk();
    for e in &extents {
        assert!(a_seq.reclaim_retired_extent(*e).unwrap());
    }
    let (a_batch, n) = mk();
    let (blocks, cnt) = a_batch
        .reclaim_retired_extents_batch(&extents, &run_flag())
        .unwrap();
    assert_eq!(blocks, 100);
    assert_eq!(cnt, 100);
    assert_eq!(a_seq.free_block_count(), a_batch.free_block_count());
    assert_eq!(a_seq.retired_block_count(), a_batch.retired_block_count());
    assert_eq!(a_batch.retired_block_count(), 0);
    assert_eq!(
        a_batch.retired_block_count(),
        a_batch.retired_block_count_exact()
    );
    for i in 0..100u64 {
        assert!(a_batch.is_free(Pba(n + i)));
    }
}

/// A batch entry that is a sub-range of a coalesced retired extent splits it:
/// free the named sub-range, keep the rest retired.
#[test]
fn batch_reclaim_splits_sub_extent() {
    let a = new_alloc(64);
    let n = alloc_first(&a, 6);
    let t0 = Instant::now();
    a.retire_extent_at(Extent::new(Pba(n), 6), t0).unwrap(); // one coalesced [n,6]
    let (blocks, cnt) = a
        .reclaim_retired_extents_batch(&[Extent::new(Pba(n), 3)], &run_flag())
        .unwrap();
    assert_eq!((blocks, cnt), (3, 1));
    for off in 0..3 {
        assert!(a.is_free(Pba(n + off)));
    }
    for off in 3..6 {
        assert!(a.is_retired(Pba(n + off)));
    }
    assert_eq!(a.retired_block_count(), 3);
    assert_eq!(a.retired_block_count(), a.retired_block_count_exact());
}

/// An extent that is no longer (fully) retired — e.g. raced realloc — is
/// skipped (fail closed); the rest of the batch still reclaims.
#[test]
fn batch_reclaim_fail_closed_on_non_retired() {
    let a = new_alloc(64);
    let n = alloc_first(&a, 12);
    let t0 = Instant::now();
    a.retire_extent_at(Extent::single(Pba(n)), t0).unwrap();
    // n+10 is allocated but never retired → not reclaimable.
    let batch = [Extent::single(Pba(n)), Extent::single(Pba(n + 10))];
    let (blocks, cnt) = a
        .reclaim_retired_extents_batch(&batch, &run_flag())
        .unwrap();
    assert_eq!((blocks, cnt), (1, 1));
    assert!(a.is_free(Pba(n)));
    assert!(!a.is_free(Pba(n + 10)), "non-retired entry untouched");
    assert!(!a.is_retired(Pba(n + 10)));
    assert_eq!(a.retired_block_count(), 0);
    assert_eq!(a.retired_block_count(), a.retired_block_count_exact());
}

/// An extent removed from the retired set in Phase A but found to overlap the
/// free list in Phase B (a should-never-happen inconsistency) is NOT
/// double-freed: it is re-inserted and stays retired.
#[test]
fn batch_reclaim_conflict_reinserts() {
    let a = new_alloc(64);
    let n = alloc_first(&a, 4);
    let p = Pba(n);
    let t0 = Instant::now();
    a.retire_extent_at(Extent::single(p), t0).unwrap();
    // Inject the inconsistency: the same PBA is also in the free list.
    a.test_region_pools(0)
        .general
        .insert_for_test(Extent::single(p));
    let (blocks, cnt) = a
        .reclaim_retired_extents_batch(&[Extent::single(p)], &run_flag())
        .unwrap();
    assert_eq!((blocks, cnt), (0, 0), "free-overlap conflict not freed");
    assert!(a.is_retired(p), "conflict re-inserted, stays retired");
    assert_eq!(a.retired_block_count(), 1);
    assert_eq!(a.retired_block_count(), a.retired_block_count_exact());
}

/// A batch larger than `BATCH_LOCK_CHUNK` reclaims every extent across chunks.
#[test]
fn batch_reclaim_spans_chunks() {
    let count = BATCH_LOCK_CHUNK + 50;
    let a = new_alloc((4 * count as u64) + 256);
    let base = alloc_first(&a, 2 * count); // 2× so stride-2 retires stay separate
    let t0 = Instant::now();
    let extents: Vec<Extent> = (0..count as u64)
        .map(|i| Extent::single(Pba(base + 2 * i)))
        .collect();
    for e in &extents {
        a.retire_extent_at(*e, t0).unwrap();
    }
    assert_eq!(a.retired_block_count(), count as u64);
    let (blocks, cnt) = a
        .reclaim_retired_extents_batch(&extents, &run_flag())
        .unwrap();
    assert_eq!(blocks, count as u64);
    assert_eq!(cnt, count);
    assert_eq!(a.retired_block_count(), 0);
    assert_eq!(a.retired_block_count(), a.retired_block_count_exact());
    for i in 0..count as u64 {
        assert!(a.is_free(Pba(base + 2 * i)));
    }
}

/// Mixed batch (some freed, one non-retired skip, one free-overlap conflict)
/// keeps the O(1) gauge in lockstep with the exact walk.
#[test]
fn batch_reclaim_gauge_stays_consistent() {
    let a = new_alloc(64);
    let n = alloc_first(&a, 12);
    let t0 = Instant::now();
    a.retire_extent_at(Extent::new(Pba(n), 4), t0).unwrap();
    a.retire_extent_at(Extent::single(Pba(n + 8)), t0).unwrap();
    a.test_region_pools(0)
        .general
        .insert_for_test(Extent::single(Pba(n + 8))); // conflict on n+8
    let batch = [
        Extent::new(Pba(n), 2),      // freed (sub-extent of [n,4])
        Extent::single(Pba(n + 10)), // skip (never retired)
        Extent::single(Pba(n + 8)),  // conflict → stays retired
    ];
    let (blocks, _) = a
        .reclaim_retired_extents_batch(&batch, &run_flag())
        .unwrap();
    assert_eq!(blocks, 2);
    assert_eq!(a.retired_block_count(), a.retired_block_count_exact());
    assert_eq!(a.retired_block_count(), 3); // [n+2,2] remainder + n+8
}

/// The batched retire must leave identical retired state to retiring the
/// same extents one-by-one through the single-extent path.
#[test]
fn batch_retire_equals_sequence() {
    let a_seq = new_alloc(4096);
    let n = alloc_first(&a_seq, 100);
    let t0 = Instant::now();
    for i in 0..100u64 {
        a_seq
            .retire_extent_at(Extent::single(Pba(n + i)), t0)
            .unwrap();
    }
    let a_batch = new_alloc(4096);
    let nb = alloc_first(&a_batch, 100);
    assert_eq!(n, nb); // fresh allocators start at the same PBA
    let extents: Vec<Extent> = (0..100u64).map(|i| Extent::single(Pba(n + i))).collect();
    let (newly, failed) = a_batch.retire_extents_batch(&extents, t0);
    assert_eq!(newly, 100);
    assert!(failed.is_empty());
    assert_eq!(a_seq.retired_block_count(), a_batch.retired_block_count());
    assert_eq!(a_batch.retired_block_count(), 100);
    assert_eq!(
        a_batch.retired_block_count(),
        a_batch.retired_block_count_exact()
    );
    for i in 0..100u64 {
        assert!(a_batch.is_retired(Pba(n + i)));
    }
}

/// Idempotent re-retire inside a batch counts only genuinely-new blocks.
#[test]
fn batch_retire_idempotent_recounts() {
    let a = new_alloc(64);
    let n = alloc_first(&a, 4);
    let t0 = Instant::now();
    let (newly1, f1) = a.retire_extents_batch(&[Extent::new(Pba(n), 2)], t0);
    assert_eq!((newly1, f1.len()), (2, 0));
    // Re-retire [n,2] (idempotent → 0 new) plus a fresh [n+2,1].
    let (newly2, f2) =
        a.retire_extents_batch(&[Extent::new(Pba(n), 2), Extent::single(Pba(n + 2))], t0);
    assert_eq!((newly2, f2.len()), (1, 0));
    assert_eq!(a.retired_block_count(), 3);
    assert_eq!(a.retired_block_count(), a.retired_block_count_exact());
}

/// An extent overlapping the free list is rejected (returned in `failed`),
/// the rest of the batch still retires.
#[test]
fn batch_retire_rejects_free_overlap() {
    let a = new_alloc(64);
    let n = alloc_first(&a, 4); // allocates n..n+3; n+10 is free
    let t0 = Instant::now();
    let (newly, failed) =
        a.retire_extents_batch(&[Extent::single(Pba(n)), Extent::single(Pba(n + 10))], t0);
    assert_eq!(newly, 1);
    assert_eq!(failed, vec![Extent::single(Pba(n + 10))]);
    assert!(a.is_retired(Pba(n)));
    assert!(!a.is_retired(Pba(n + 10)));
    assert_eq!(a.retired_block_count(), a.retired_block_count_exact());
}

/// A batch larger than `BATCH_LOCK_CHUNK` retires every extent across chunks.
#[test]
fn batch_retire_spans_chunks() {
    let count = BATCH_LOCK_CHUNK + 30;
    let a = new_alloc((4 * count as u64) + 256);
    let base = alloc_first(&a, 2 * count);
    let extents: Vec<Extent> = (0..count as u64)
        .map(|i| Extent::single(Pba(base + 2 * i)))
        .collect();
    let (newly, failed) = a.retire_extents_batch(&extents, Instant::now());
    assert_eq!(newly, count as u64);
    assert!(failed.is_empty());
    assert_eq!(a.retired_block_count(), count as u64);
    assert_eq!(a.retired_block_count(), a.retired_block_count_exact());
}

/// The batched free must leave identical allocator state to freeing the
/// same extents one-by-one through `free_one`/`free_extent`.
#[test]
fn batch_free_equals_sequence() {
    let mk = || {
        let a = new_alloc(4096);
        let n = alloc_first(&a, 200);
        (a, n)
    };
    // Stride-2 singles + a couple of multi-block extents.
    let extents: Vec<Extent> = {
        let (_, n) = mk();
        let mut v: Vec<Extent> = (0..50u64).map(|i| Extent::single(Pba(n + 2 * i))).collect();
        v.push(Extent::new(Pba(n + 120), 4));
        v.push(Extent::new(Pba(n + 130), 8));
        v
    };
    let (a_seq, _) = mk();
    for e in &extents {
        a_seq.free_extent(*e).unwrap();
    }
    let (a_batch, _) = mk();
    let (freed, failed) = a_batch.free_extents_batch(&extents);
    assert_eq!(freed, 50 + 4 + 8);
    assert!(failed.is_empty());
    assert_eq!(a_seq.free_block_count(), a_batch.free_block_count());
    assert_eq!(
        a_seq.allocated_block_count(),
        a_batch.allocated_block_count()
    );
    let seq_pools = a_seq.test_region_pools(0);
    let batch_pools = a_batch.test_region_pools(0);
    assert_eq!(
        *seq_pools.general.by_addr(),
        *batch_pools.general.by_addr(),
        "end-state general free lists must be identical"
    );
    assert_eq!(
        *seq_pools.stripe_reserve.by_addr(),
        *batch_pools.stripe_reserve.by_addr(),
        "end-state stripe reserves must be identical"
    );
}

/// Adjacent extents within one batch coalesce into the same end state the
/// sequential path produces.
#[test]
fn batch_free_coalesces_adjacent_members() {
    let a = new_alloc(4096);
    let n = alloc_first(&a, 12);
    let batch = [
        Extent::new(Pba(n), 3),
        Extent::new(Pba(n + 3), 3),
        Extent::new(Pba(n + 6), 6),
    ];
    let (freed, failed) = a.free_extents_batch(&batch);
    assert_eq!((freed, failed.len()), (12, 0));
    // All 12 blocks free and merged with the trailing free space into one run.
    assert!(a.is_extent_free(Extent::new(Pba(n), 12)));
    assert_eq!(a.allocated_block_count(), 0);
}

/// Failure mix: an already-free member and a retired member are rejected
/// (returned in `failed`), the rest of the batch still frees.
#[test]
fn batch_free_failure_mix() {
    let a = new_alloc(4096);
    let n = alloc_first(&a, 12);
    let t0 = Instant::now();
    a.free_one(Pba(n + 4)).unwrap(); // already free
    a.retire_extent_at(Extent::single(Pba(n + 6)), t0).unwrap(); // retired
    let batch = [
        Extent::single(Pba(n)),     // frees
        Extent::single(Pba(n + 4)), // free-overlap → failed
        Extent::single(Pba(n + 6)), // retired-overlap → failed
        Extent::single(Pba(n + 8)), // frees
    ];
    let (freed, failed) = a.free_extents_batch(&batch);
    assert_eq!(freed, 2);
    assert_eq!(
        failed,
        vec![Extent::single(Pba(n + 4)), Extent::single(Pba(n + 6))]
    );
    assert!(a.is_free(Pba(n)));
    assert!(a.is_free(Pba(n + 8)));
    assert!(a.is_retired(Pba(n + 6)), "retired member untouched");
}

/// A duplicate entry within one batch is caught by the intra-chunk
/// free-overlap check (first frees, second fails) — no double free.
#[test]
fn batch_free_rejects_intra_batch_duplicate() {
    let a = new_alloc(64);
    let n = alloc_first(&a, 2);
    let batch = [Extent::single(Pba(n)), Extent::single(Pba(n))];
    let (freed, failed) = a.free_extents_batch(&batch);
    assert_eq!(freed, 1);
    assert_eq!(failed, vec![Extent::single(Pba(n))]);
    assert_eq!(a.allocated_block_count(), 1);
}

/// A batch larger than `BATCH_LOCK_CHUNK` frees every extent across chunks.
#[test]
fn batch_free_spans_chunks() {
    let count = BATCH_LOCK_CHUNK + 50;
    let a = new_alloc((4 * count as u64) + 256);
    let base = alloc_first(&a, 2 * count);
    let extents: Vec<Extent> = (0..count as u64)
        .map(|i| Extent::single(Pba(base + 2 * i)))
        .collect();
    let (freed, failed) = a.free_extents_batch(&extents);
    assert_eq!(freed, count as u64);
    assert!(failed.is_empty());
    for i in 0..count as u64 {
        assert!(a.is_free(Pba(base + 2 * i)));
    }
}
