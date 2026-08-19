use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::error::{OnyxError, OnyxResult};
use crate::meta::store::MetaStore;
use crate::space::extent::Extent;
use crate::space::free_set::FreeSet;
use crate::space::hazard::{PbaHazardGuard, PbaHazards};
use crate::types::{Pba, BLOCK_SIZE, RESERVED_BLOCKS};

/// Number of blocks to refill a lane cache from the global free list at once.
const LANE_CACHE_REFILL_SIZE: u32 = 256;
/// Number of blocks to reserve for each lane's contiguous extent cache.
///
/// Raw passthrough flushes commonly allocate 4-8 contiguous blocks per unit;
/// serving those from a lane-local slice avoids hammering the global BTreeSet.
const LANE_EXTENT_CACHE_REFILL_BLOCKS: u32 = 8192;
/// Maximum number of separate contiguous runs one lane refill may take.
///
/// The block budget above is the *intent*, but an aged pool has no long runs
/// left to satisfy it — the stripe reserve degrades to isolated single-stripe
/// windows. Taking several runs per lock hold decouples the lane cache from
/// contiguity: 64 single-stripe runs still buy 64 allocations per global-lock
/// acquisition. Bounded because the cache is scanned linearly per carve and
/// because cached blocks are parked away from other lanes (64 × one stripe =
/// 1.5 MiB per lane, far under the block budget's 32 MiB).
const LANE_EXTENT_CACHE_REFILL_RUNS: usize = 64;
/// Hard bound on reserve entries EXAMINED by one refill's ascending walk.
/// Without it, a wider-than-one-stripe request against a reserve of
/// single-stripe runs would skip past every entry in a multi-million-entry set
/// while holding the global free lock. Generous relative to the run cap so the
/// common case (every reserve entry qualifies) always fills the batch.
const LANE_EXTENT_CACHE_REFILL_SCAN: usize = 8 * LANE_EXTENT_CACHE_REFILL_RUNS;
/// Per-chunk extent cap for the batched retire/reclaim paths
/// (`retire_extents_batch`, `reclaim_retired_extents_batch`). Bounds how much
/// work ONE lane-cache snapshot (`2 × num_lanes` mutexes) and hazard barrier is
/// amortized over, and how often the inter-chunk breather runs.
///
/// ⚠ This does NOT bound the lock hold — see [`FREE_LOCK_HOLD_EXTENTS`]. The
/// comment here used to claim "~sub-millisecond per hold", which was wrong by
/// 12-25×: one 4096-extent Phase-B hold measures **retire 3.3 ms / reclaim
/// 12.9 ms** on a box-scale free list (`bench_batch_hold_vs_alloc`).
const BATCH_LOCK_CHUNK: usize = 4096;
/// Max extents processed per SINGLE acquisition of `free_pools` /
/// `retired_extents` inside the batched paths. This is what bounds how long the
/// foreground can be shut out of the free lock, and it is a different concern
/// from [`BATCH_LOCK_CHUNK`] (which amortizes the lane snapshot).
///
/// Splitting the hold costs no reclaim throughput because GC's **total** demand
/// on this lock is tiny — it is the burst shape that hurts. At the box's
/// 56 K blocks/s reclaim rate with ~6-block extents that is ~9.3 K extents/s,
/// i.e. ~2.3 chunks/s × 12.9 ms ≈ **3% lock occupancy**, yet any writer landing
/// inside a hold waits up to the full 12.9 ms. Mean wait for a random arrival is
/// `occupancy × hold/2`, so it falls linearly with the hold: ~190 µs at 4096
/// extents/hold, ~6 µs at 128. Capacity stays far above demand — 128 extents per
/// (0.4 ms hold + 0.5 ms breather) is ~142 K extents/s vs the ~9.3 K/s needed.
///
/// Runtime-settable (not a `const`) for one reason: A/B'ing it by restarting the
/// process is worthless. The 2026-07-28 box A/B of the refill change measured two
/// IDENTICAL arms 2.13× apart on run-order drift alone, so the only way to get a
/// signal is to alternate the setting INSIDE one process against one pool state.
static FREE_LOCK_HOLD_EXTENTS: AtomicUsize = AtomicUsize::new(128);

/// Read the current free-lock hold bound (see [`FREE_LOCK_HOLD_EXTENTS`]). One
/// relaxed load per hold-chunk, i.e. per ~128 extents — not per extent.
fn free_lock_hold_extents() -> usize {
    FREE_LOCK_HOLD_EXTENTS.load(Ordering::Relaxed).max(1)
}

/// Override the free-lock hold bound. Benches use this to compare hold sizes
/// within a single process; production leaves the default.
pub fn set_free_lock_hold_extents(extents: usize) {
    FREE_LOCK_HOLD_EXTENTS.store(extents.max(1), Ordering::Relaxed);
}

/// Entries (age log) or extents (retired set) one [`SpaceAllocator::aged_candidates`]
/// slice examines before releasing the shard lock.
///
/// The box-measured cost of that selector was **1.169 s per GC cycle under one
/// acquisition** — 10% of wall with the retired lock fully closed, and the source
/// of the 1.4-1.65 s tails every other site saw. The work itself is necessary
/// (~1 M candidates per cycle, because retire extents average ~1 block), so what
/// gets fixed is the monopoly, not the total: 4096 entries is ~0.2-0.5 ms of walk
/// per hold, three orders of magnitude below the whole-cycle hold, while still
/// amortizing the acquisition over enough entries to be free.
const AGED_SCAN_SLICE: usize = 4096;

/// Retired extents [`SpaceAllocator::retired_stripe_windows`] examines before
/// releasing the shard lock.
///
/// The window walk needs its own slice bound because its output is deduplicated
/// by WINDOW while its cost is per EXTENT: under the ~1-block retires that
/// scattered overwrite produces, up to `stripe` extents collapse into one
/// window, so a windows-only budget would let one hold walk `windows × stripe`
/// entries. Same reasoning and same order of magnitude as [`AGED_SCAN_SLICE`].
const RETIRED_WINDOW_SCAN_SLICE: usize = 4096;

/// Target number of address regions when `storage.allocator_regions` is 0 — which
/// is the production default since 2026-08-01.
///
/// Over the box's 600 GiB LV3 (157 M blocks) that is ~76 K blocks / 300 MiB per
/// region: large enough that one lane refill (64 stripe windows) is served from
/// a single region, small enough that the GC's address-scattered retire/reclaim
/// holds land on the region a writer is refilling from only ~1/N of the time.
/// The 2026-07-29 box attribution measured the single lock **68.4% busy with 98%
/// of the holding coming from GC**, while the writer's own hold was 1.9% and
/// 98.8% of its allocation time was WAIT — so the fix is not to make the writer
/// faster but to stop it queueing behind GC.
const DEFAULT_ALLOCATOR_REGIONS: usize = 2048;
/// Never shard below this many blocks per region: a region has to be able to
/// hold a useful number of whole stripes, and each one costs a mutex plus two
/// hint atomics. Small test allocators therefore stay single-region.
const MIN_REGION_BLOCKS: u64 = 4096;
/// How many alternative regions a lane refill tries before giving up to the
/// lane-cache drain / global aligned search.
const REGION_REFILL_TRIES: usize = 4;

/// How many peek-then-fold rounds a per-lane drain will do.
///
/// One round folds back everything the lane held when it peeked. A second is only
/// needed when a concurrent refill parked something in a region the round had
/// already passed — and a refill can only do that by taking blocks OUT of the
/// pool, so leaving them for the next drain costs nothing. At shutdown, where the
/// drain must not leak blocks, there are no concurrent refills and the first
/// round takes everything.
const LANE_DRAIN_ROUNDS: usize = 2;

/// Serialize every region acquisition behind one gate, reproducing the
/// pre-region single-global-lock contention shape at runtime.
static REGION_SERIALIZE: AtomicBool = AtomicBool::new(false);

/// Arm/disarm the region serialization gate — the ONLY way to A/B region
/// sharding against the old single lock **inside one process**, which is the
/// only A/B this box supports: on 2026-07-28 two byte-identical arms measured
/// 119.2 vs 253.3 MB/s (2.13x) purely on run-order drift, so restart-per-arm
/// comparisons resolve nothing here.
///
/// When armed, every region acquisition first takes a single process-wide gate
/// held for the whole critical section, so N region locks behave as one lock
/// with the same hold durations. Region *routing* is unchanged, so arming and
/// disarming is safe at any time and needs no pool state change.
pub fn set_region_serialize(on: bool) {
    REGION_SERIALIZE.store(on, Ordering::Relaxed);
}

/// Whether the serialization gate is currently armed.
pub fn region_serialize() -> bool {
    REGION_SERIALIZE.load(Ordering::Relaxed)
}

mod allocation;
mod core;
mod diagnostics;
mod layout;
mod pools;
mod quarantine;
mod reclaim;
mod retired;
mod stats;

use layout::*;
use pools::*;
use retired::*;
pub use stats::*;

pub struct SpaceAllocator {
    /// IO-addressable capacity in blocks. Atomic so an online `grow_capacity`
    /// (chunklet `extend_ld` on LV3) can publish the larger frontier while
    /// concurrent bounds checks / status reads run lock-free. Only ever grows.
    total_blocks: AtomicU64,
    /// Address-ordered free list + (count, start) side index, sharded by PBA
    /// address into independently-locked regions (see [`RegionPools`]).
    /// First-fit SELECTION is unchanged for every path that spans regions (they
    /// walk regions in ascending address order, so "first region that can serve"
    /// IS the global address-argmin); the one deliberate exception is the flush
    /// writer's lane refill, which is first-fit WITHIN the lane's active region.
    regions: RegionPools,
    /// Coalesced retired set + young-age log, sharded by PBA on the SAME
    /// [`RegionLayout`] as `regions` — see [`RetiredShard`]. One shard when
    /// sharding is turned off (`storage.allocator_regions = 1`, the rollback
    /// path), i.e. byte-for-byte the pre-sharding structure behind one mutex.
    retired: RetiredRegions,
    /// O(1) running total of blocks in `retired_extents`. The depth gauge is
    /// read once per GC cycle; summing the (potentially millions of) coalesced
    /// extents under the set lock was ~360 ms/cycle at 60M-deep AND contended
    /// the lock with the foreground retire path. This atomic is advisory (feeds
    /// only the `gc_retired_blocks_depth` metric, never a free decision), kept
    /// in lockstep with the set in `retire_extent_at`/`reclaim_retired_extent`.
    retired_blocks: AtomicU64,
    hazards: PbaHazards,
    allocated_blocks: AtomicU64,
    free_blocks: AtomicU64,
    /// Per-site wait/hold accounting for `free_pools` — see [`FreeLockSite`].
    free_lock: FreeLockStats,
    /// Per-site wait/hold accounting for the retired shards — see
    /// [`RetiredLockSite`]. The ledger nests: `free_lock.<site>.hold` ⊇
    /// `retired_lock.<site>.{wait,hold}` for the paths that take the free lock
    /// outermost (retire, free), which is what makes the residual readable as
    /// "real work". (There is no separate `age_lock` table any more: the age log
    /// now lives under the same shard mutex, because its measured wait was 0.1%
    /// of the retire batch's region hold.)
    retired_lock: RetiredLockStats,
    /// Per-lane single-block caches. Each flush lane pops from its own cache
    /// to avoid contending on `free_extents`. Refilled in bulk from global.
    lane_caches: Vec<Mutex<Vec<Pba>>>,
    /// Per-lane contiguous extent caches for raw multi-block writes.
    lane_extent_caches: Vec<Mutex<Vec<Extent>>>,
    /// Free blocks parked in each lane cache — i.e. exactly what a
    /// [`Self::drain_lane_caches`] would hand back. See
    /// [`Self::lane_cached_blocks`] for why these exist and why reading them
    /// lock-free is sound.
    lane_cache_depth: Vec<AtomicU64>,
    lane_extent_cache_depth: Vec<AtomicU64>,
    /// The region each lane currently refills its aligned extent cache from
    /// (`usize::MAX` = not yet chosen). Advisory: a lane that cannot be served
    /// switches, and lanes may share a region rather than starve.
    lane_regions: Vec<AtomicUsize>,
    alloc_tracker: Option<Mutex<BTreeSet<Pba>>>,
    /// Aligned-path lane-cache supply accounting — see [`AllocSupplyStats`].
    /// Relaxed counters, advisory only (never feed an allocation decision).
    aligned_allocs: AtomicU64,
    refill_ops: AtomicU64,
    refill_blocks: AtomicU64,
    refill_runs: AtomicU64,
    drain_ops: AtomicU64,
    drain_blocks: AtomicU64,
    /// Drains skipped because every lane cache was empty — see
    /// [`Self::drain_lane_caches_if_populated`]. Read against `drain_ops`: a
    /// large ratio is the guard earning its keep, and `drain_ops` staying high
    /// while allocations fail means the lanes really do hold the space.
    drain_skips: AtomicU64,
    /// Test-only: restore the pre-2026-08-13 unconditional drain, so
    /// `bench_empty_drain_guard` can price the guard inside ONE process.
    #[cfg(test)]
    drain_guard_off: AtomicBool,
    /// Times a lane moved its aligned refill to a different region.
    region_switches: AtomicU64,
    /// Refill attempts that found nothing usable in the region they tried.
    region_refill_misses: AtomicU64,
    /// Whole stripes an aligned lane refill prefers a reserve run to be long
    /// enough for before it will settle for a one-stripe run. `0` = off, which
    /// is byte-for-byte the pre-2026-08-02 refill. See
    /// [`crate::config::StorageConfig::stripe_refill_run_stripes`] and
    /// [`Self::refill_stripe_extent_lane`].
    ///
    /// Runtime-settable for the same reason as [`FREE_LOCK_HOLD_EXTENTS`]: this
    /// box measured two byte-identical arms 2.13x apart on run-order drift, so
    /// the only trustworthy A/B alternates the setting inside ONE process
    /// against ONE pool state.
    stripe_refill_run_stripes: AtomicU64,
    /// Refills that were served by a run meeting the wide floor above.
    refill_wide_hits: AtomicU64,
    /// Refills that fell back to the one-stripe floor (no wide run anywhere the
    /// lane looked). `hits + misses` counts only refills attempted while the
    /// knob is on, so a zero/zero pair means "knob off", not "no traffic".
    refill_wide_misses: AtomicU64,
    /// `(stripe, phase)` packed as `stripe << 32 | phase`, 0 = unset. Lets the
    /// public `stripe_geometry()` answer without taking a region lock (the GC
    /// defrag scanner asks once per candidate cluster).
    geometry_cache: AtomicU64,
    /// Prefer WIDER reserve runs over lower-address ones when refilling a lane
    /// (`storage.stripe_refill_width_bias`, design D2). `false` = the shipped
    /// first-fit-by-address refill, byte for byte.
    ///
    /// Only the stripe-reserve refill consults this; small/unaligned allocation and
    /// every ENOSPC fallback stay address-ordered. Runtime-settable for the same
    /// reason as `stripe_refill_run_stripes`.
    stripe_refill_width_bias: AtomicBool,
    /// [`Self::allocate_stripe_run_for_lane`] calls that returned an extent.
    stripe_run_allocs: AtomicU64,
    /// Whole stripes those calls returned. `stripes / allocs` is the mean bundle
    /// width — the mechanism read for design D1, and the local stand-in for
    /// chunklet's per-PD adjacency merge (1.0 = the pre-D1 behaviour).
    stripe_run_stripes: AtomicU64,
    /// Bundle widths bucketed by `floor(log2(stripes))`: 1, 2-3, 4-7, 8-15, 16-31,
    /// 32-63, 64+. The mean alone cannot tell "every bundle is 2 stripes" from
    /// "most are 1 and a few are 30", and those imply different next steps.
    stripe_run_width_hist: [AtomicU64; STRIPE_RUN_WIDTH_BUCKETS],
}

/// Buckets in [`SpaceAllocator::stripe_run_width_hist`].
pub(crate) const STRIPE_RUN_WIDTH_BUCKETS: usize = 7;

#[cfg(test)]
#[path = "allocator/tests/free_pool_policy.rs"]
mod free_pool_policy_tests;

#[cfg(test)]
#[path = "allocator/tests/age.rs"]
mod age_tests;

#[cfg(test)]
#[path = "allocator/tests/stripe_align.rs"]
mod stripe_align_tests;

#[cfg(test)]
#[path = "allocator/tests/aged_pool_bench.rs"]
pub(crate) mod aged_pool_bench;

#[cfg(test)]
#[path = "allocator/tests/region.rs"]
mod region_tests;
