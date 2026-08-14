//! Local repro of the box-measured writer wall: 2026-07-28, QD256 j16d16 on
//! an aged 256 GiB volume, the flush writer spent **55.6% of its time in
//! aligned PBA allocation at 871 us/op** with `unaligned_ops = 0` and
//! `reserve_miss_ops = 0`. Pool state then: `free_extents = 24,669,384`,
//! `free_blocks_in_set = 84,000,718` (mean free extent 3.4 blocks),
//! `stripe_capable` flat at 27%.
//!
//! Two non-exclusive causes were open, and one single-threaded number
//! separates them:
//!   (A) the critical section itself is expensive — `first_fit` walks every
//!       distinct size class >= min_count under the global lock, plus three
//!       BTreeSet removes on a cache-cold multi-hundred-MB structure;
//!   (B) pure 16-way convoy on one `Mutex`.
//! Single-threaded ~100 us/op ⇒ (A) dominates. Single-threaded a few us with
//! a large multi-thread multiplier ⇒ (B) dominates.
//!
//! This also instruments the one link the box run left unmeasured: how many
//! blocks a `refill_stripe_extent_lane` actually takes.
//!
//! Run:
//! ```text
//! cargo test --release --lib aged_pool_bench -- --ignored --nocapture
//! ```
//! `ONYX_BENCH_SCALE=<n>` overrides the free-extent target (default: the box
//! figure). The full-scale pool needs ~2.5 GiB RSS and ~1 min to build.
use super::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const STRIPE: u32 = 6;
const PHASE: u32 = (RESERVED_BLOCKS % STRIPE as u64) as u32;
/// Box `free_extents` at the time of the 871 us/op measurement.
const BOX_FREE_EXTENTS: u64 = 24_669_384;
/// Box `free_blocks_in_set` — mean free extent 3.4 blocks.
const BOX_FREE_BLOCKS: u64 = 84_000_718;
/// Box `stripe_capable` share of `free_blocks_in_set`.
const BOX_STRIPE_CAPABLE_PCT: f64 = 27.0;

/// Shape of the long-run tail that carries the stripe reserve. `D` (the
/// number of distinct size classes `first_fit` has to walk) is an OUTPUT of
/// this choice, not an input, so both ends are measured: `Spread` gives a
/// broad size distribution (large D), `Fixed` a narrow one (small D). The
/// real pool sits somewhere between — background defrag publishes large
/// compacted runs into the reserve, which widens it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TailShape {
    Spread,
    Fixed,
    /// The pessimal shape, and the one the box's own numbers point at: the
    /// reserve is nothing but ISOLATED single-stripe windows. A refill can
    /// then only ever take 6 blocks, so the lane cache serves exactly one
    /// allocation and every single aligned alloc takes the global lock.
    /// `largest_run` collapsing (box: −60% over 40 min) is what produces it.
    SingleStripe,
}

/// Synthesize an aged pool whose `contiguity_stats()` match the box:
/// `target_extents` free runs totalling ~`3.4 * target_extents` blocks with
/// ~27% of those blocks in whole-stripe aligned middles.
///
/// Mixture solved from the three box numbers: with `N_l` long runs carrying
/// `reserve + 3*N_l` blocks and the rest short (< one stripe, so they
/// contribute zero stripe capacity), `N_l = 2%` of runs at mean length ~48
/// lands all three at once (0.5 M x 48 = 24 M blocks, of which ~22.5 M are
/// aligned middles = 27% of 84 M; the remaining 24.2 M runs average 2.5).
pub(crate) fn build_aged_pool(
    target_extents: u64,
    shape: TailShape,
    lanes: usize,
) -> (SpaceAllocator, ContiguityStats) {
    let (allocator, stats, _live) = build_aged_pool_parts(target_extents, shape, lanes, None);
    (allocator, stats)
}

/// [`build_aged_pool`] with an explicit region count (so one process can run
/// both a sharded and an unsharded arm on byte-identical pool shapes — the
/// only A/B form this project trusts) and the LIVE extents returned.
///
/// The live set is the synthetic pool's gaps: `build_aged_pool` emits
/// run/gap pairs and accounts every gap block as allocated, so the gaps are
/// exactly the live blocks — scattered over the whole address space, which is
/// what makes them a faithful stand-in for the box's retire candidates (old
/// PBAs of overwritten LBAs, written long ago and therefore everywhere).
pub(crate) fn build_aged_pool_parts(
    target_extents: u64,
    shape: TailShape,
    lanes: usize,
    regions: Option<usize>,
) -> (SpaceAllocator, ContiguityStats, Vec<Extent>) {
    const LONG_RUN_MEAN: u64 = 48;
    // Reserve-carrying runs per total runs. `SingleStripe` needs many more of
    // them to reach the same 27% stripe capacity, since each carries only one
    // stripe: 22.7 M / 6 = 3.8 M runs out of 24.7 M ≈ 1 in 7.
    let long_run_in_n: u64 = match shape {
        TailShape::Spread | TailShape::Fixed => 50, // 2%
        TailShape::SingleStripe => 7,               // ~14%
    };
    let mut rng = StdRng::seed_from_u64(0x00a1_10ca_7ed0_u64.wrapping_mul(target_extents | 1));

    // Walk PBA ascending, emitting run/gap pairs. The gap is live data, so
    // total span = free blocks + live blocks; size the device to fit.
    let mut runs: Vec<Extent> = Vec::with_capacity(target_extents as usize);
    let mut cursor = RESERVED_BLOCKS;
    for i in 0..target_extents {
        let len = if i % long_run_in_n == 0 {
            match shape {
                // Geometric-ish spread around the mean → many distinct sizes.
                TailShape::Spread => {
                    let mut l = STRIPE as u64;
                    while l < 8 * LONG_RUN_MEAN && rng.gen_bool(0.88) {
                        l += STRIPE as u64;
                    }
                    l
                }
                TailShape::Fixed => LONG_RUN_MEAN,
                TailShape::SingleStripe => {
                    // Start ON an alignment boundary so the whole run is the
                    // aligned middle: reserve gets exactly one stripe, with
                    // no head/tail spilling into general.
                    cursor = SpaceAllocator::align_up_pba(cursor, STRIPE as u64, PHASE as u64);
                    STRIPE as u64
                }
            }
        } else {
            rng.gen_range(1..=4u64) // mean 2.5, all sub-stripe
        };
        runs.push(Extent::new(Pba(cursor), len as u32));
        // Gap >= 1 keeps runs non-adjacent so classification never coalesces.
        cursor += len + rng.gen_range(1..=5u64);
    }

    let device_blocks = cursor + 1024;
    let allocator = match regions {
        Some(n) => {
            SpaceAllocator::new_with_exact_regions(device_blocks * BLOCK_SIZE as u64, lanes, n)
        }
        None => SpaceAllocator::new(device_blocks * BLOCK_SIZE as u64, lanes),
    };
    allocator.set_stripe_geometry(STRIPE, PHASE);
    allocator.replace_general_regionwise(&runs.iter().copied().collect());
    let free_blocks: u64 = runs.iter().map(|r| r.count as u64).sum();
    let usable = device_blocks - RESERVED_BLOCKS;
    allocator.free_blocks.store(free_blocks, Ordering::Relaxed);
    allocator
        .allocated_blocks
        .store(usable - free_blocks, Ordering::Relaxed);
    let stats = allocator.contiguity_stats();
    // The gaps between consecutive free runs are the live blocks.
    let live: Vec<Extent> = runs
        .windows(2)
        .map(|pair| {
            let end = pair[0].end_pba().0;
            Extent::new(Pba(end), (pair[1].start.0 - end) as u32)
        })
        .collect();
    (allocator, stats, live)
}

/// Distinct `count` values in the stripe reserve — the `D` in `first_fit`'s
/// O(D log N) size-class walk, and the direct predictor for hypothesis (A).
fn reserve_size_classes(allocator: &SpaceAllocator) -> usize {
    let mut classes: Vec<u32> = (0..allocator.region_count())
        .flat_map(|idx| {
            let pools = allocator.test_region_pools(idx);
            let counts: Vec<u32> = pools
                .stripe_reserve
                .by_addr()
                .iter()
                .map(|e| e.count)
                .collect();
            counts
        })
        .collect();
    classes.sort_unstable();
    classes.dedup();
    classes.len()
}

fn percentile(sorted_ns: &[u64], p: f64) -> f64 {
    if sorted_ns.is_empty() {
        return 0.0;
    }
    let idx = ((sorted_ns.len() - 1) as f64 * p).round() as usize;
    sorted_ns[idx] as f64 / 1000.0
}

#[test]
#[ignore = "perf microbench"]
fn bench_aged_pool_stripe_alloc() {
    let scale: u64 = std::env::var("ONYX_BENCH_SCALE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(BOX_FREE_EXTENTS);
    const OPS: usize = 100_000;
    let threads: usize = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);

    for shape in [TailShape::Fixed, TailShape::Spread, TailShape::SingleStripe] {
        let build = Instant::now();
        let (allocator, stats) = build_aged_pool(scale, shape, 16);
        let capable = stats.stripe_capable_blocks.unwrap_or(0);
        let classes = reserve_size_classes(&allocator);
        println!(
            "\n=== shape={:?} scale={} built in {:?} ===\n\
                 free_extents={} free_blocks={} mean_extent={:.2} \
                 stripe_capable={} ({:.1}%) reserve={} largest_run={} \
                 reserve_size_classes(D)={}\n\
                 box reference:  free_extents={} free_blocks={} mean_extent=3.40 \
                 stripe_capable={:.1}%",
            shape,
            scale,
            build.elapsed(),
            stats.free_extents,
            stats.free_blocks_in_set,
            stats.free_blocks_in_set as f64 / stats.free_extents.max(1) as f64,
            capable,
            capable as f64 / stats.free_blocks_in_set.max(1) as f64 * 100.0,
            stats.stripe_reserve_blocks,
            stats.largest_run_blocks,
            classes,
            BOX_FREE_EXTENTS,
            BOX_FREE_BLOCKS,
            BOX_STRIPE_CAPABLE_PCT,
        );

        // (1) The raw reserve query, no lock, no allocation: isolates the
        // size-class walk that hypothesis (A) blames.
        let mut ff_ns = Vec::with_capacity(1000);
        {
            let pools = allocator.test_region_pools(0);
            for _ in 0..1000 {
                let t = Instant::now();
                let hit = pools.stripe_reserve.first_fit(STRIPE);
                ff_ns.push(t.elapsed().as_nanos() as u64);
                assert!(hit.is_some(), "reserve must be able to serve one stripe");
            }
        }
        ff_ns.sort_unstable();
        println!(
            "  first_fit(6) on reserve alone:      p50 {:8.2} us  p99 {:8.2} us",
            percentile(&ff_ns, 0.5),
            percentile(&ff_ns, 0.99)
        );

        // (2) Single-threaded end-to-end allocation = the critical section.
        let mut ns = Vec::with_capacity(OPS);
        let mut served = 0usize;
        for _ in 0..OPS {
            let t = Instant::now();
            let got = allocator.allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE);
            ns.push(t.elapsed().as_nanos() as u64);
            if got.is_ok() {
                served += 1;
            }
        }
        let total_us: f64 = ns.iter().sum::<u64>() as f64 / 1000.0;
        ns.sort_unstable();
        println!(
            "  1 thread  x {OPS} ops (served {served}): mean {:8.2} us  p50 {:8.2} us  \
                 p99 {:8.2} us",
            total_us / OPS as f64,
            percentile(&ns, 0.5),
            percentile(&ns, 0.99)
        );

        // (3) Same op under contention. The box ran 16 writers; this host has
        // fewer cores, so the multiplier here is a LOWER bound on the box's.
        let allocator = std::sync::Arc::new(allocator);
        let wall = Instant::now();
        let per_thread: Vec<(u64, usize)> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..threads)
                .map(|lane| {
                    let allocator = allocator.clone();
                    scope.spawn(move || {
                        let mut sum_ns = 0u64;
                        let mut ok = 0usize;
                        for _ in 0..(OPS / threads) {
                            let t = Instant::now();
                            let got = allocator
                                .allocate_stripe_extent_for_lane(lane, STRIPE, STRIPE, PHASE);
                            sum_ns += t.elapsed().as_nanos() as u64;
                            if got.is_ok() {
                                ok += 1;
                            }
                        }
                        (sum_ns, ok)
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let wall = wall.elapsed();
        let ops: usize = per_thread.iter().map(|(_, ok)| ok).sum();
        let sum_ns: u64 = per_thread.iter().map(|(ns, _)| ns).sum();
        println!(
            "  {threads} threads x {} ops each (served {ops}): mean {:8.2} us/op  \
                 wall {:?}  => {:.0} allocs/s = {:.1} MiB/s of 24 KiB stripes",
            OPS / threads,
            sum_ns as f64 / 1000.0 / ops.max(1) as f64,
            wall,
            ops as f64 / wall.as_secs_f64(),
            ops as f64 / wall.as_secs_f64() * 24.0 / 1024.0,
        );
        println!(
            "  box baseline for comparison:        871.00 us/op, 9333 allocs/s, \
                 213.7 MiB/s write"
        );
        let supply = allocator.supply_stats();
        println!(
            "  supply: refills={} blocks/refill={:.1} runs/refill={:.1} \
                 allocs/refill={:.2} drains={} drain_blocks={}",
            supply.refills,
            supply.blocks_per_refill(),
            supply.runs_per_refill(),
            supply.allocs_per_refill(),
            supply.drains,
            supply.drain_blocks,
        );
    }
}

/// The writers are not the only traffic on `free_pools`. Every overwrite
/// retires its old PBA and the GC reclaims it, and both go through the
/// BATCH_LOCK_CHUNK paths, whose Phase-B hold does up to 4096
/// `release_extent` (coalesce-insert) calls in ONE lock hold. The comment on
/// `reclaim_retired_extents_batch` already estimates "tens of ms on a
/// multi-million-extent free list" and records box-measured "22-80
/// thread-s/s alloc convoy spikes phase-locked to every 262K-block reclaim
/// batch" — this measures that hold directly, and then measures what it does
/// to concurrent aligned allocation.
///
/// Run: `cargo test --release --lib bench_batch_hold_vs_alloc -- --ignored --nocapture`
#[test]
#[ignore = "perf microbench"]
fn bench_batch_hold_vs_alloc() {
    // Sleep multiplier that pins the background batch thread to roughly the
    // box's measured lock duty cycle: sleep = busy × N ⇒ occupancy ≈ 1/(1+N).
    // N = 32 ⇒ ~3%. Override with ONYX_BENCH_BG_DUTY_DIVISOR to sweep.
    const BG_DUTY_DIVISOR_DEFAULT: u32 = 32;
    let bg_duty_divisor: u32 = std::env::var("ONYX_BENCH_BG_DUTY_DIVISOR")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(BG_DUTY_DIVISOR_DEFAULT);
    let scale: u64 = std::env::var("ONYX_BENCH_SCALE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(BOX_FREE_EXTENTS);
    let threads: usize = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    // Steady state: 6 blocks allocated per stripe write ⇒ 6 blocks retired
    // and reclaimed per stripe write. Model the reclaim side, which is the
    // one that re-inserts into the free pool.
    let (allocator, stats) = build_aged_pool(scale, TailShape::Spread, 16);
    println!(
        "\n=== batch-hold vs alloc: free_extents={} free_blocks={} ===",
        stats.free_extents, stats.free_blocks_in_set
    );

    // Carve allocated (live) extents to feed the retire/reclaim cycle. The
    // synthetic pool's gaps are live, so take a slice of them by allocating
    // fresh — simplest faithful source is the allocator itself.
    let mut owned: Vec<Extent> = Vec::with_capacity(BATCH_LOCK_CHUNK * 4);
    while owned.len() < BATCH_LOCK_CHUNK * 4 {
        match allocator.allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE) {
            Ok(e) => owned.push(e),
            Err(_) => break,
        }
    }
    assert!(
        owned.len() >= BATCH_LOCK_CHUNK,
        "need at least one full chunk of live extents"
    );

    // (1) One chunk's retire hold, then one chunk's reclaim hold.
    let chunk: Vec<Extent> = owned.drain(..BATCH_LOCK_CHUNK).collect();
    let t = Instant::now();
    let (newly, failed) = allocator.retire_extents_batch(&chunk, Instant::now());
    let retire_ms = t.elapsed().as_secs_f64() * 1e3;
    let running = AtomicBool::new(true);
    let t = Instant::now();
    let reclaimed = allocator
        .reclaim_retired_extents_batch(&chunk, &running)
        .unwrap();
    let reclaim_ms = t.elapsed().as_secs_f64() * 1e3;
    println!(
        "  one {BATCH_LOCK_CHUNK}-extent chunk: retire {:8.2} ms (newly={newly} \
             failed={})  reclaim {:8.2} ms (blocks={} extents={})",
        retire_ms,
        failed.len(),
        reclaim_ms,
        reclaimed.0,
        reclaimed.1,
    );
    println!("    (the BATCH_LOCK_CHUNK doc comment claims \"~sub-millisecond\" per hold)");

    // (2) Aligned allocation latency with that batch traffic running
    // concurrently, vs the same measurement with the background idle.
    let allocator = std::sync::Arc::new(allocator);
    // Interleave the two hold sizes several times. Foreground ops here leak
    // blocks (nothing frees them), so the pool reshapes as the bench runs and
    // a single A-then-B comparison would carry exactly the run-order confound
    // that invalidated the 2026-07-28 box A/B. Paired rounds make any drift
    // visible instead of silent.
    //
    // hold = BATCH_LOCK_CHUNK reproduces the pre-fix behaviour (one hold per
    // chunk); hold = 128 is the shipped default.
    let rounds: [(usize, bool); 7] = [
        (128, false),
        (BATCH_LOCK_CHUNK, true),
        (128, true),
        (BATCH_LOCK_CHUNK, true),
        (128, true),
        (BATCH_LOCK_CHUNK, true),
        (128, true),
    ];
    for (hold, background) in rounds {
        set_free_lock_hold_extents(hold);
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let batches = std::sync::Arc::new(AtomicU64::new(0));
        // The real overwrite cycle: allocate → retire → GC reclaim → the
        // blocks come back free. Re-retiring an already-free extent fails
        // fast and does no work, so the batch MUST be freshly allocated each
        // round or the background silently becomes a no-op.
        let held_ns = std::sync::Arc::new(AtomicU64::new(0));
        let bg = background.then(|| {
            let allocator = allocator.clone();
            let stop = stop.clone();
            let batches = batches.clone();
            let held_ns = held_ns.clone();
            let bg_duty_divisor = bg_duty_divisor;
            std::thread::spawn(move || {
                let running = AtomicBool::new(true);
                let bg_lane = 15; // not one of the measured lanes
                while !stop.load(Ordering::Relaxed) {
                    let mut batch = Vec::with_capacity(BATCH_LOCK_CHUNK);
                    while batch.len() < BATCH_LOCK_CHUNK {
                        match allocator
                            .allocate_stripe_extent_for_lane(bg_lane, STRIPE, STRIPE, PHASE)
                        {
                            Ok(e) => batch.push(e),
                            Err(_) => break,
                        }
                    }
                    if batch.is_empty() {
                        break;
                    }
                    let t = Instant::now();
                    let (_, _) = allocator.retire_extents_batch(&batch, Instant::now());
                    let _ = allocator.reclaim_retired_extents_batch(&batch, &running);
                    let busy = t.elapsed();
                    held_ns.fetch_add(busy.as_nanos() as u64, Ordering::Relaxed);
                    batches.fetch_add(1, Ordering::Relaxed);
                    // Pace to the BOX's duty cycle, not back-to-back. At the
                    // box's 56 K blocks/s reclaim rate the GC occupies this
                    // lock only ~3% of wall time; an unpaced loop sits at
                    // 84-94% and starves the foreground regardless of hold
                    // size, which measures the wrong regime entirely.
                    std::thread::sleep(busy * bg_duty_divisor);
                }
            })
        });

        let ops_per_thread = 40_000;
        let wall = Instant::now();
        let samples: Vec<Vec<u64>> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..threads)
                .map(|lane| {
                    let allocator = allocator.clone();
                    scope.spawn(move || {
                        let mut ns = Vec::with_capacity(ops_per_thread);
                        for _ in 0..ops_per_thread {
                            let t = Instant::now();
                            let _ = allocator
                                .allocate_stripe_extent_for_lane(lane, STRIPE, STRIPE, PHASE);
                            ns.push(t.elapsed().as_nanos() as u64);
                        }
                        ns
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let wall = wall.elapsed();
        stop.store(true, Ordering::Relaxed);
        if let Some(bg) = bg {
            let _ = bg.join();
        }
        let mut all: Vec<u64> = samples.into_iter().flatten().collect();
        let mean = all.iter().sum::<u64>() as f64 / 1000.0 / all.len() as f64;
        all.sort_unstable();
        let bg_busy = held_ns.load(Ordering::Relaxed) as f64 / wall.as_nanos() as f64 * 100.0;
        // max / p9999 are the statistics that matter here: the question is
        // how long ONE writer can be shut out by ONE hold, not the average.
        println!(
            "  bg={:<5} hold={:<5} mean {:7.2} us  p99 {:7.2} us  p999 {:8.2} us  \
                 p9999 {:8.2} us  max {:8.2} us  (bg batches={} busy {:.0}% wall {:?})",
            background,
            hold,
            mean,
            percentile(&all, 0.99),
            percentile(&all, 0.999),
            percentile(&all, 0.9999),
            all.last().copied().unwrap_or(0) as f64 / 1000.0,
            batches.load(Ordering::Relaxed),
            bg_busy,
            wall,
        );
    }
    // The attribution this whole exercise was missing: of the foreground's
    // wait, whose hold was it? Sorted by total hold so the monopolist is top.
    let mut sites = allocator.free_lock_stats();
    sites.retain(|s| s.acquisitions > 0);
    sites.sort_by(|x, y| y.hold_ns.cmp(&x.hold_ns));
    let total_hold: u64 = sites.iter().map(|s| s.hold_ns).sum();
    println!("  -- free_pools attribution (who held it) --");
    for s in &sites {
        println!(
            "  {:<17} acq {:9}  wait {:9.2} ms ({:8.2} us/acq)  hold {:9.2} ms \
                 ({:8.2} us/acq) {:5.1}% of holds  hold_max {:8.2} ms",
            s.site,
            s.acquisitions,
            s.wait_ns as f64 / 1e6,
            s.wait_us(),
            s.hold_ns as f64 / 1e6,
            s.hold_us(),
            if total_hold > 0 {
                s.hold_ns as f64 / total_hold as f64 * 100.0
            } else {
                0.0
            },
            s.hold_ns_max as f64 / 1e6,
        );
    }
    let supply = allocator.supply_stats();
    println!(
        "  supply: refills={} blocks/refill={:.1} allocs/refill={:.2} drains={} \
             drain_blocks={}",
        supply.refills,
        supply.blocks_per_refill(),
        supply.allocs_per_refill(),
        supply.drains,
        supply.drain_blocks,
    );
}

fn site_of(sites: &[LockSiteStats], name: &str) -> LockSiteStats {
    *sites.iter().find(|s| s.site == name).expect("known site")
}

/// Local repro of the 2026-07-29 post-sharding anomaly.
///
/// Region sharding cut the writer's free-lock wait 169× (22936 → 136 µs/acq),
/// but `retire_batch`'s SUMMED region hold went **218 s → 1670 s** and its
/// per-acquisition hold 5.4 → 185 µs. The acquisition COUNT rise is explained
/// (`region_holds` breaks at every region boundary, and GC's ~28-extent retire
/// batches are scattered over the whole address space, so a 28-extent hold
/// becomes ~28 one-extent holds). The per-acquisition COST rise was not:
///
///   (1) `retire_extents_batch` acquires the ONE global `retired_extents`
///       lock INSIDE its region hold, so waiting for it is REPORTED as region
///       hold — and it is now acquired ~28× more often.
///   (2) 2048 independent BTreeSets are colder than one, i.e. the per-extent
///       work itself got dearer.
///
/// The `retired_lock` / `age_lock` attribution splits the two. This bench runs
/// both arms in ONE process on byte-identical synthetic pools — the box cannot
/// do that (restart-per-arm A/B measured 2.13× between identical arms).
///
/// Traffic: `retire_threads` cleanup threads retiring scattered live extents
/// in ~28-extent batches (the box shape) plus one GC thread running
/// `aged_candidates` → `reclaim_retired_extents_batch`.
///
/// Run: `cargo test --release --lib bench_retired_lock_convoy -- --ignored --nocapture`
#[test]
#[ignore = "perf microbench"]
fn bench_retired_lock_convoy() {
    /// Box `retire_dead_pbas` batch shape: 18846 acq/s ÷ 658 holds/s ≈ 28.6.
    const BATCH: usize = 28;
    let scale: u64 = std::env::var("ONYX_BENCH_SCALE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4_000_000);
    let secs: u64 = std::env::var("ONYX_BENCH_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    // The box runs 16 buffer shards' cleanup threads against this lock.
    let retire_threads: usize = std::env::var("ONYX_BENCH_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);

    for regions in [1usize, 64, 2048] {
        let (allocator, stats, mut live) =
            build_aged_pool_parts(scale, TailShape::Spread, 16, Some(regions));
        // Scatter: the box's retire candidates are old PBAs of overwritten
        // LBAs, i.e. uniform over the address space. Shuffle so consecutive
        // batches are not address-adjacent, then sort WITHIN each batch —
        // exactly what `retire_dead_pbas` does before calling the allocator.
        let mut rng = StdRng::seed_from_u64(0x5EED_9C0F);
        for i in (1..live.len()).rev() {
            live.swap(i, rng.gen_range(0..=i));
        }
        let allocator = std::sync::Arc::new(allocator);
        let live = std::sync::Arc::new(live);
        let cursor = std::sync::Arc::new(AtomicUsize::new(0));
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let retired_extents = std::sync::Arc::new(AtomicU64::new(0));

        let wall = Instant::now();
        let mut workers = Vec::new();
        for _ in 0..retire_threads {
            let (allocator, live, cursor, stop, retired_extents) = (
                allocator.clone(),
                live.clone(),
                cursor.clone(),
                stop.clone(),
                retired_extents.clone(),
            );
            workers.push(std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let lo = cursor.fetch_add(BATCH, Ordering::Relaxed);
                    if lo + BATCH >= live.len() {
                        break; // ran out of live extents to retire
                    }
                    let mut batch: Vec<Extent> = live[lo..lo + BATCH].to_vec();
                    batch.sort_unstable_by_key(|e| e.start.0);
                    allocator.retire_extents_batch(&batch, Instant::now());
                    retired_extents.fetch_add(BATCH as u64, Ordering::Relaxed);
                }
            }));
        }
        // GC: select aged candidates and reclaim them, like `GcRunner`.
        let gc = {
            let (allocator, stop) = (allocator.clone(), stop.clone());
            std::thread::spawn(move || {
                let running = AtomicBool::new(true);
                let mut reclaimed = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    // grace = 0: every retired block is immediately eligible,
                    // so the selector keeps up with the retire threads (the
                    // box's steady state, where retire_in ≈ reclaimed).
                    // `gc::runner::MAX_RETIRED_RECLAIM_BLOCKS_PER_CYCLE`
                    // (private to that module).
                    let (cands, _) =
                        allocator.aged_candidates(1_048_576, Duration::ZERO, Instant::now());
                    if cands.is_empty() {
                        std::thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    if let Ok((blocks, _)) =
                        allocator.reclaim_retired_extents_batch(&cands, &running)
                    {
                        reclaimed += blocks;
                    }
                }
                reclaimed
            })
        };
        std::thread::sleep(Duration::from_secs(secs));
        stop.store(true, Ordering::Relaxed);
        for w in workers {
            let _ = w.join();
        }
        let reclaimed = gc.join().unwrap_or(0);
        let wall_ns = wall.elapsed().as_nanos() as f64;

        let free = allocator.free_lock_stats();
        let ret = allocator.retired_lock_stats();
        let (fr, rr) = (
            site_of(&free, "retire_batch"),
            site_of(&ret, "retire_batch"),
        );
        let ret_busy: u64 = ret.iter().map(|s| s.hold_ns).sum();
        println!(
            "\n=== regions={regions:<5} free_extents={} retired {} extents, reclaimed {} \
                 blocks in {:.1}s ===",
            stats.free_extents,
            retired_extents.load(Ordering::Relaxed),
            reclaimed,
            wall_ns / 1e9,
        );
        println!(
            "  retire_batch  region: acq {:8} ({:7.0}/s) items/acq {:5.1}  hold {:8.2} s \
                 ({:8.2} µs/acq, {:6.2} µs/item)",
            fr.acquisitions,
            fr.acquisitions as f64 / wall_ns * 1e9,
            if fr.acquisitions > 0 {
                fr.items as f64 / fr.acquisitions as f64
            } else {
                0.0
            },
            fr.hold_ns as f64 / 1e9,
            fr.hold_us(),
            if fr.items > 0 {
                fr.hold_ns as f64 / 1e3 / fr.items as f64
            } else {
                0.0
            },
        );
        // THE ledger: the region hold contains the retired acquisition, so it
        // splits into wait + hold + residual. `residual` is the work that runs
        // under the region lock ONLY — the quantity hypothesis (2) predicts
        // must grow, and hypothesis (1) predicts must not.
        let residual = fr.hold_ns as f64 - rr.wait_ns as f64 - rr.hold_ns as f64;
        for (label, v) in [
            ("region hold", fr.hold_ns as f64),
            ("  retired wait", rr.wait_ns as f64),
            ("  retired hold", rr.hold_ns as f64),
            ("  residual (region-lock-only work)", residual),
        ] {
            println!(
                "  {:<36} {:8.2} s  {:5.1}% of region hold",
                label,
                v / 1e9,
                if fr.hold_ns > 0 {
                    v / fr.hold_ns as f64 * 100.0
                } else {
                    0.0
                }
            );
        }
        println!(
            "  retired lock busy {:5.1}% of wall   (per-site holds: {})",
            ret_busy as f64 / wall_ns * 100.0,
            ret.iter()
                .filter(|s| s.hold_ns > 0)
                .map(|s| format!("{}={:.2}s", s.site, s.hold_ns as f64 / 1e9))
                .collect::<Vec<_>>()
                .join(" "),
        );
    }
}

/// What the unconditional all-regions drain cost at the exhaustion boundary,
/// as two arms in ONE process.
///
/// The exhausted regime is where the box spends its time: the reserve is gone,
/// so `allocate_stripe_extent_for_lane` misses, and before 2026-08-13 that
/// miss paid for `drain_lane_caches()` — every region lock in one hold plus a
/// `regions`-entry guard vector — to fold back lane caches that are empty
/// precisely BECAUSE allocation is failing.
///
/// Run:
/// ```text
/// cargo test --release --lib bench_empty_drain_guard -- --ignored --nocapture
/// ```
#[test]
#[ignore = "perf microbench"]
fn bench_empty_drain_guard() {
    let per_thread: u64 = std::env::var("ONYX_BENCH_ALLOCS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2_000);
    let scale: u64 = std::env::var("ONYX_BENCH_SCALE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let threads: usize = std::thread::available_parallelism()
        .map(|n| n.get().min(16))
        .unwrap_or(8);

    // Production region count, because the whole cost being priced is "one
    // lock per region".
    let (allocator, _stats, _live) = build_aged_pool_parts(
        scale,
        TailShape::Spread,
        16,
        Some(DEFAULT_ALLOCATOR_REGIONS),
    );
    let regions = allocator.region_count();
    // Drain the reserve so every aligned allocation takes the miss branch,
    // then empty the lane caches — the exhausted regime's shape.
    while allocator
        .allocate_stripe_extent_for_lane(0, STRIPE, STRIPE, PHASE)
        .is_ok()
    {}
    allocator.drain_lane_caches();
    let allocator = std::sync::Arc::new(allocator);
    println!(
        "\n=== empty-drain guard: {threads} threads x {per_thread} failing aligned allocs, \
             {regions} regions ==="
    );

    let arm = |guard_off: bool| -> (f64, u64) {
        allocator.free_lock.shards.iter().for_each(|shard| {
            shard.acquisitions[FreeLockSite::Drain as usize].store(0, Ordering::Relaxed);
        });
        allocator
            .drain_guard_off
            .store(guard_off, Ordering::Relaxed);
        let start = Instant::now();
        let workers: Vec<_> = (0..threads)
            .map(|tid| {
                let allocator = allocator.clone();
                std::thread::spawn(move || {
                    for _ in 0..per_thread {
                        let _ = std::hint::black_box(
                            allocator.allocate_stripe_extent_for_lane(tid, STRIPE, STRIPE, PHASE),
                        );
                    }
                })
            })
            .collect();
        for w in workers {
            w.join().unwrap();
        }
        let ns = start.elapsed().as_nanos() as f64 / (threads as u64 * per_thread) as f64;
        let locks: u64 = allocator
            .free_lock_stats()
            .iter()
            .find(|s| s.site == "drain")
            .map_or(0, |s| s.acquisitions);
        (ns, locks)
    };

    // Alternate so drift shows up instead of hiding in an A-then-B order.
    let (off1, off1_locks) = arm(true);
    let (on1, on1_locks) = arm(false);
    let (on2, on2_locks) = arm(false);
    let (off2, off2_locks) = arm(true);
    allocator.drain_guard_off.store(false, Ordering::Relaxed);
    for (label, a, b, la, lb) in [
        (
            "unconditional (pre-fix)",
            off1,
            off2,
            off1_locks,
            off2_locks,
        ),
        ("guarded (shipped)", on1, on2, on1_locks, on2_locks),
    ] {
        println!(
            "  {label:<26} {:10.1} ns/alloc  (pass1 {:10.1} / pass2 {:10.1})  \
                 drain region locks {la} / {lb}",
            (a + b) / 2.0,
            a,
            b,
        );
    }
}

/// What the lane-cache drain costs per-lane vs the old all-regions hold, both
/// arms in ONE process on the same pool.
///
/// The box turned this into the allocator's biggest region-lock consumer once
/// the futile walks were gone: 5,768 drains in 493 s, 2048 region locks each,
/// recovering 5.1 blocks per drain.
///
/// Run:
/// ```text
/// cargo test --release --lib bench_per_lane_drain -- --ignored --nocapture
/// ```
#[test]
#[ignore = "perf microbench"]
fn bench_per_lane_drain() {
    let rounds: u64 = std::env::var("ONYX_BENCH_DRAINS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2_000);
    let scale: u64 = std::env::var("ONYX_BENCH_SCALE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_300_000);
    let lanes = 16usize;

    let (allocator, _stats, _live) = build_aged_pool_parts(
        scale,
        TailShape::Spread,
        lanes,
        Some(DEFAULT_ALLOCATOR_REGIONS),
    );
    let regions = allocator.region_count();
    println!("\n=== lane-cache drain: {rounds} drains, {lanes} lanes, {regions} regions ===");

    // Each round seeds the lanes the way the writers do, then drains. Seeding
    // is charged to neither arm: it is measured once and subtracted.
    let seed = |a: &SpaceAllocator| {
        for lane in 0..lanes {
            let _ = std::hint::black_box(a.allocate_one_for_lane(lane));
        }
    };
    let arm = |per_lane: bool| -> (f64, u64, u64) {
        let acqs = || -> u64 {
            allocator
                .free_lock_stats()
                .iter()
                .find(|s| s.site == "drain")
                .map_or(0, |s| s.acquisitions)
        };
        let blocks = || allocator.supply_stats().drain_blocks;
        let (locks_before, blocks_before) = (acqs(), blocks());
        let start = Instant::now();
        for _ in 0..rounds {
            seed(&allocator);
            if per_lane {
                allocator.drain_lane_caches();
            } else {
                allocator.drain_lane_caches_all_regions();
            }
        }
        let ns = start.elapsed().as_nanos() as f64 / rounds as f64;
        (ns, acqs() - locks_before, blocks() - blocks_before)
    };

    let (old1, old1_locks, old1_blocks) = arm(false);
    let (new1, new1_locks, new1_blocks) = arm(true);
    let (new2, new2_locks, _) = arm(true);
    let (old2, old2_locks, _) = arm(false);
    for (label, a, b, la, lb, blocks) in [
        (
            "all regions (pre-fix)",
            old1,
            old2,
            old1_locks,
            old2_locks,
            old1_blocks,
        ),
        (
            "per lane (shipped)",
            new1,
            new2,
            new1_locks,
            new2_locks,
            new1_blocks,
        ),
    ] {
        println!(
            "  {label:<24} {:10.1} ns/drain  (pass1 {:10.1} / pass2 {:10.1})  \
                 region locks/drain {:7.1} / {:7.1}  blocks recovered/drain {:.1}",
            (a + b) / 2.0,
            a,
            b,
            la as f64 / rounds as f64,
            lb as f64 / rounds as f64,
            blocks as f64 / rounds as f64,
        );
    }
}

/// What the unaligned refill's region walk costs with and without the width
/// filter, alternated in ONE process on the SAME pool.
///
/// The box measured `refill_extent_lane` at **1,989 region locks per unaligned
/// allocation** in the exhausted regime — every lock futile, because
/// `free_hint` is a SUM and says yes to a region holding only runs narrower
/// than the request. `ONYX_BENCH_WIDTH` picks the request width; the default
/// is wider than most of the aged pool's runs, which is the box's condition.
///
/// Run:
/// ```text
/// cargo test --release --lib bench_unaligned_refill_width_filter -- --ignored --nocapture
/// ```
#[test]
#[ignore = "perf microbench"]
fn bench_unaligned_refill_width_filter() {
    let per_thread: u64 = std::env::var("ONYX_BENCH_REFILLS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000);
    let scale: u64 = std::env::var("ONYX_BENCH_SCALE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_300_000);
    let width: u32 = std::env::var("ONYX_BENCH_WIDTH")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64);
    let threads: usize = std::thread::available_parallelism()
        .map(|n| n.get().min(16))
        .unwrap_or(8);

    let (allocator, stats, _live) = build_aged_pool_parts(
        scale,
        TailShape::Spread,
        16,
        Some(DEFAULT_ALLOCATOR_REGIONS),
    );
    let regions = allocator.region_count();
    let allocator = std::sync::Arc::new(allocator);
    println!(
        "\n=== unaligned refill, {width}-block request: {threads} threads x {per_thread}, \
             {regions} regions, free_extents={} ===",
        stats.free_extents
    );

    let arm = |filtered: bool| -> (f64, u64) {
        let site_acqs = || -> u64 {
            allocator
                .free_lock_stats()
                .iter()
                .find(|s| s.site == "writer_unaligned")
                .map_or(0, |s| s.acquisitions)
        };
        let before = site_acqs();
        let start = Instant::now();
        let workers: Vec<_> = (0..threads)
            .map(|tid| {
                let allocator = allocator.clone();
                std::thread::spawn(move || {
                    for _ in 0..per_thread {
                        let got = allocator.refill_extent_lane(
                            tid,
                            width,
                            width.max(LANE_EXTENT_CACHE_REFILL_BLOCKS),
                            filtered,
                        );
                        // Give it straight back so both arms see one shape.
                        if let Some(extent) = std::hint::black_box(got) {
                            let mut guard = allocator.lock_span(FreeLockSite::FreeOne, extent);
                            guard.release_extent(extent);
                        }
                    }
                })
            })
            .collect();
        for w in workers {
            w.join().unwrap();
        }
        let ns = start.elapsed().as_nanos() as f64 / (threads as u64 * per_thread) as f64;
        (ns, site_acqs() - before)
    };

    let (unf1, unf1_locks) = arm(false);
    let (fil1, fil1_locks) = arm(true);
    let (fil2, fil2_locks) = arm(true);
    let (unf2, unf2_locks) = arm(false);
    allocator.drain_lane_caches();
    let picks = (threads as u64 * per_thread) as f64;
    for (label, a, b, la, lb) in [
        (
            "free_hint walk (pre-fix)",
            unf1,
            unf2,
            unf1_locks,
            unf2_locks,
        ),
        ("width filter (shipped)", fil1, fil2, fil1_locks, fil2_locks),
    ] {
        println!(
            "  {label:<26} {:9.1} ns/refill  (pass1 {:9.1} / pass2 {:9.1})  \
                 region locks/refill {:7.1} / {:7.1}",
            (a + b) / 2.0,
            a,
            b,
            la as f64 / picks,
            lb as f64 / picks,
        );
    }
}

/// What the unaligned path's "give me the largest fragment" scan cost, hint
/// argmax vs the pre-2026-08-13 lock-every-region scan, alternated in ONE
/// process on the SAME pool.
///
/// This is `allocate_extent`'s short-fragment fallback, i.e. where an
/// unaligned writer allocation lands once the reserve is gone. The scan locked
/// every non-empty region to read a `largest()` it then discarded.
///
/// Run:
/// ```text
/// cargo test --release --lib bench_largest_scan_vs_hint -- --ignored --nocapture
/// ```
#[test]
#[ignore = "perf microbench"]
fn bench_largest_scan_vs_hint() {
    let per_thread: u64 = std::env::var("ONYX_BENCH_PICKS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000);
    let scale: u64 = std::env::var("ONYX_BENCH_SCALE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let threads: usize = std::thread::available_parallelism()
        .map(|n| n.get().min(16))
        .unwrap_or(8);

    let (allocator, stats, _live) = build_aged_pool_parts(
        scale,
        TailShape::Spread,
        16,
        Some(DEFAULT_ALLOCATOR_REGIONS),
    );
    let regions = allocator.region_count();
    let allocator = std::sync::Arc::new(allocator);
    println!(
        "\n=== largest-fragment pick: {threads} threads x {per_thread} picks, {regions} \
             regions, free_extents={} ===",
        stats.free_extents
    );

    // Take and immediately give back, so both arms see the same pool shape and
    // neither drains it — the cost being priced is the SEARCH, not the churn.
    let arm = |hinted: bool| -> (f64, u64) {
        let before: u64 = allocator
            .free_lock_stats()
            .iter()
            .find(|s| s.site == "small_alloc")
            .map_or(0, |s| s.acquisitions);
        let start = Instant::now();
        let workers: Vec<_> = (0..threads)
            .map(|_| {
                let allocator = allocator.clone();
                std::thread::spawn(move || {
                    for _ in 0..per_thread {
                        let picked = if hinted {
                            allocator.take_largest_regionwise(FreeLockSite::SmallAlloc)
                        } else {
                            allocator.take_largest_regionwise_scanning(FreeLockSite::SmallAlloc)
                        };
                        if let Some(extent) = std::hint::black_box(picked) {
                            let mut guard = allocator.lock_span(FreeLockSite::FreeOne, extent);
                            guard.release_extent(extent);
                        }
                    }
                })
            })
            .collect();
        for w in workers {
            w.join().unwrap();
        }
        let ns = start.elapsed().as_nanos() as f64 / (threads as u64 * per_thread) as f64;
        let locks: u64 = allocator
            .free_lock_stats()
            .iter()
            .find(|s| s.site == "small_alloc")
            .map_or(0, |s| s.acquisitions)
            - before;
        (ns, locks)
    };

    let (scan1, scan1_locks) = arm(false);
    let (hint1, hint1_locks) = arm(true);
    let (hint2, hint2_locks) = arm(true);
    let (scan2, scan2_locks) = arm(false);
    for (label, a, b, la, lb) in [
        (
            "full scan (pre-fix)",
            scan1,
            scan2,
            scan1_locks,
            scan2_locks,
        ),
        (
            "hint argmax (shipped)",
            hint1,
            hint2,
            hint1_locks,
            hint2_locks,
        ),
    ] {
        let picks = (threads as u64 * per_thread) as f64;
        println!(
            "  {label:<24} {:9.1} ns/pick  (pass1 {:9.1} / pass2 {:9.1})  \
                 region locks/pick {:6.1} / {:6.1}",
            (a + b) / 2.0,
            a,
            b,
            la as f64 / picks,
            lb as f64 / picks,
        );
    }
}

/// What the lock ACCOUNTING costs per region acquire+release — four arms in
/// ONE process, alternated and repeated so run-order drift is visible rather
/// than silent.
///
/// The 2026-08-12 box profile put ~66% of the flush writers' CPU in region
/// acquire/release machinery with contention at only 10.2%, so this bench runs
/// each thread on its OWN region: what is left is exactly the uncontended
/// acquire + guard-drop path the profile blamed.
///
/// | arm | shape |
/// |---|---|
/// | `shared+every` | every thread on one counter set, clock on every acquisition — the pre-fix shape |
/// | `shared+sampled` | isolates the clock reads alone |
/// | `sharded+every` | isolates the shared-cache-line RMWs alone |
/// | `sharded+sampled` | shipped default |
///
/// Run:
/// ```text
/// cargo test --release --lib bench_lock_stats_overhead -- --ignored --nocapture
/// ```
#[test]
#[ignore = "perf microbench"]
fn bench_lock_stats_overhead() {
    /// The production stride. Named here because `DEFAULT_LOCK_STAT_STRIDE`
    /// is 1 under `cfg(test)` (the accounting tests need exact timing).
    const BENCH_STRIDE: u64 = 16;
    let per_thread: u64 = std::env::var("ONYX_BENCH_ACQUISITIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500_000);
    // Small enough to build in seconds; the arms differ only in accounting, so
    // the pool shape is a shared constant, not a variable.
    let scale: u64 = std::env::var("ONYX_BENCH_SCALE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let threads: usize = std::thread::available_parallelism()
        .map(|n| n.get().min(16))
        .unwrap_or(8);

    // Explicit region count: `build_aged_pool`'s default constructor is
    // single-region, and this bench needs one region per thread so the mutex
    // itself never contends.
    let (allocator, stats, _live) = build_aged_pool_parts(scale, TailShape::Spread, 16, Some(64));
    let regions = allocator.region_count();
    let allocator = std::sync::Arc::new(allocator);
    println!(
        "\n=== lock-stats overhead: {threads} threads x {per_thread} acquisitions, \
             {regions} regions, free_extents={} ===",
        stats.free_extents
    );
    assert!(
        regions >= threads,
        "each thread needs its own region to isolate the uncontended path"
    );

    // ns per acquire+release, with the accounting configured as asked.
    let arm = |pinned: bool, stride: u64| -> f64 {
        allocator
            .free_lock
            .stride_override
            .store(stride, Ordering::Relaxed);
        allocator
            .free_lock
            .shard_pin
            .store(if pinned { 0 } else { usize::MAX }, Ordering::Relaxed);
        let start = Instant::now();
        let workers: Vec<_> = (0..threads)
            .map(|tid| {
                let allocator = allocator.clone();
                std::thread::spawn(move || {
                    for _ in 0..per_thread {
                        let guard = allocator.lock_region_raw(FreeLockSite::Audit, tid % regions);
                        std::hint::black_box(guard.free_blocks_in_pools());
                    }
                })
            })
            .collect();
        for w in workers {
            w.join().unwrap();
        }
        let elapsed = start.elapsed().as_nanos() as f64;
        elapsed / (threads as u64 * per_thread) as f64
    };

    let arms: [(&str, bool, u64); 4] = [
        ("shared  + every  (pre-fix)", true, 1),
        ("shared  + sampled", true, BENCH_STRIDE),
        ("sharded + every", false, 1),
        ("sharded + sampled (shipped)", false, BENCH_STRIDE),
    ];
    // Forward then reverse: if the two passes of one arm disagree by more than
    // the arm-to-arm spread, the result is drift and not the knob.
    let mut fwd = [0.0; 4];
    let mut rev = [0.0; 4];
    for (i, (_, pinned, stride)) in arms.iter().enumerate() {
        fwd[i] = arm(*pinned, *stride);
    }
    for (i, (_, pinned, stride)) in arms.iter().enumerate().rev() {
        rev[i] = arm(*pinned, *stride);
    }
    let base = (fwd[0] + rev[0]) / 2.0;
    for (i, (label, _, _)) in arms.iter().enumerate() {
        let mean = (fwd[i] + rev[i]) / 2.0;
        println!(
            "  {label:<28} {mean:7.1} ns/acq  (pass1 {:7.1} / pass2 {:7.1})  {:+6.1}% vs pre-fix",
            fwd[i],
            rev[i],
            (mean - base) / base * 100.0,
        );
    }
    // Restore production behaviour for anything that shares this process.
    allocator
        .free_lock
        .stride_override
        .store(0, Ordering::Relaxed);
    allocator
        .free_lock
        .shard_pin
        .store(usize::MAX, Ordering::Relaxed);
}
