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

/// Address-region layout snapshot. Immutable for the duration of one allocator
/// operation: the layout is only ever rewritten by `set_geometry`, which holds
/// every region lock while it re-routes the whole free set.
///
/// Region `i` owns `[base + i*blocks, base + (i+1)*blocks)`, with two
/// deliberate asymmetries:
///   - region 0 also owns everything BELOW `base` (the reserved prefix plus the
///     ≤ stripe-1 blocks between `RESERVED_BLOCKS` and the first aligned PBA),
///   - the LAST region owns everything above its start, so an online
///     `grow_capacity` needs no re-layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RegionLayout {
    base: u64,
    blocks: u64,
    count: usize,
}

impl RegionLayout {
    /// Single-region (sharding off) — byte-for-byte the pre-region behaviour.
    fn single() -> Self {
        Self {
            base: 0,
            blocks: 0,
            count: 1,
        }
    }

    fn sharded(&self) -> bool {
        self.count > 1 && self.blocks > 0
    }

    /// Owning region of a PBA. Total over all u64 (clamped both ends), so no
    /// caller has to bounds-check before routing.
    fn of(&self, pba: u64) -> usize {
        if !self.sharded() {
            return 0;
        }
        ((pba.saturating_sub(self.base)) / self.blocks).min(self.count as u64 - 1) as usize
    }

    fn start(&self, idx: usize) -> u64 {
        if idx == 0 || !self.sharded() {
            0
        } else {
            self.base + idx as u64 * self.blocks
        }
    }

    fn end(&self, idx: usize) -> u64 {
        if !self.sharded() || idx + 1 >= self.count {
            u64::MAX
        } else {
            self.base + (idx + 1) as u64 * self.blocks
        }
    }

    /// Inclusive region index range spanned by `extent`. Zero-count extents
    /// (rejected downstream by `validate_extent_shape`) route to their start's
    /// region so the caller can still take a lock and report the failure.
    fn span(&self, extent: Extent) -> (usize, usize) {
        let lo = self.of(extent.start.0);
        let last = extent.end_pba().0.max(extent.start.0 + 1) - 1;
        (lo, self.of(last).max(lo))
    }

    /// `extent` clipped to region `idx`, or `None` if it does not reach into it.
    fn clip(&self, idx: usize, extent: Extent) -> Option<Extent> {
        let s = extent.start.0.max(self.start(idx));
        let e = extent.end_pba().0.min(self.end(idx));
        (s < e).then(|| Extent::new(Pba(s), (e - s) as u32))
    }

    /// Blocks per region for a device of `usable_blocks`, aiming at `regions`
    /// shards. Returns `None` when the device is too small to shard usefully.
    /// The result is a multiple of `stripe` so no region boundary can split a
    /// stripe window — without that, every boundary would strand up to
    /// `stripe - 1` blocks in the general pool instead of the reserve.
    fn plan(usable_blocks: u64, regions: usize, stripe: u32) -> Option<(u64, usize)> {
        if regions <= 1 || usable_blocks == 0 {
            return None;
        }
        let stripe = u64::from(stripe.max(1));
        let want = usable_blocks.div_ceil(regions as u64).max(MIN_REGION_BLOCKS);
        let blocks = want.div_ceil(stripe) * stripe;
        let count = usable_blocks.div_ceil(blocks) as usize;
        (count > 1).then_some((blocks, count))
    }
}

/// One original retire operation's age, tracked at retire granularity in the
/// `retired_age` log so coalescing the `retired_extents` set can never re-age it.
#[derive(Debug, Clone, Copy)]
struct RetiredRun {
    count: u32,
    retired_at: Instant,
}

/// Fragmentation snapshot of the global free set (one lock hold, O(log N)).
/// `stripe_capable_blocks / free_blocks_in_set` = currently allocatable stripe
/// capacity over globally tracked free space. Lane-cached extents are excluded
/// from both; quarantine-free blocks remain in the denominator but are excluded
/// from capability until their target is complete and published to reserve.
#[derive(Debug, Clone, Copy)]
pub struct ContiguityStats {
    pub free_blocks_in_set: u64,
    pub free_extents: u64,
    pub largest_run_blocks: u32,
    /// Whole-stripe aligned capacity (eff floored to stripe multiples).
    /// `None` when no stripe geometry is configured (stripe <= 1).
    pub stripe_capable_blocks: Option<u64>,
    /// Free whole-stripe blocks held exclusively for aligned allocations.
    pub stripe_reserve_blocks: u64,
    /// Total physical span covered by active defrag quarantines.
    pub quarantine_target_blocks: u64,
    /// Already-free blocks held inside active defrag quarantines.
    pub quarantine_free_blocks: u64,
}

/// Lane-cache supply accounting for the aligned (full-stripe) alloc path.
///
/// The aligned fast path is meant to serve most allocations out of a lane-local
/// cache, taking the global `free_pools` lock only to refill. Whether that
/// actually happens depends on how much the refill manages to take: the
/// mechanism was built around ONE contiguous run per refill, so on a fragmented
/// pool it can silently degrade into "global lock per allocation". These
/// counters read that out directly — `blocks_per_refill` / `allocs_per_refill`
/// are the amplification the cache is really buying.
///
/// `drains` counts `drain_lane_caches` calls, which are the expensive shape:
/// one hold that re-inserts every cached extent from ALL lanes — and with region
/// sharding it holds EVERY region lock for the duration, so this is the one path
/// sharding makes *more* expensive, not less. A nonzero-and-growing `drains`
/// under steady write load means lanes are fighting over an empty stripe reserve,
/// and each fight stalls all 16 writers. Sharded, a lane tries
/// [`REGION_REFILL_TRIES`] other regions before it resorts to a drain, so this
/// should sit at 0 even more firmly than it already did.
///
/// `wide_hits` / `wide_misses` are the direct read of
/// `storage.stripe_refill_run_stripes`: a miss means no region the lane could see
/// held a run of the preferred width, so the refill fell back to the legacy
/// one-stripe floor. Both zero = knob off. A miss-dominated pool is the signal
/// that the reserve has no intact material left and the lever moves to defrag,
/// not to the refill.
#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct AllocSupplyStats {
    pub aligned_allocs: u64,
    pub refills: u64,
    pub refill_blocks: u64,
    pub refill_runs: u64,
    pub drains: u64,
    pub drain_blocks: u64,
    /// All-region drains the empty-lane guard skipped — see
    /// [`SpaceAllocator::drain_lane_caches_if_populated`]. `drain_skips` climbing
    /// while `drains` stays flat is the exhausted regime: every allocation was
    /// asking for a fold of caches that hold nothing.
    pub drain_skips: u64,
    pub wide_hits: u64,
    pub wide_misses: u64,
}

/// Address-region sharding shape and traffic — see [`RegionPools`].
///
/// `regions` is the divisor `tools/flush_delta.py` needs to turn the summed
/// `free_lock.*.hold_ns` into a PER-LOCK occupancy: with one lock, "sum of holds
/// / wall" was the busy fraction of that lock (68.4% on the box); with N locks it
/// is the busy fraction of the average lock only after dividing by N.
///
/// `switches` and `refill_misses` are the health signal: a lane that keeps
/// changing region, or refills that keep coming up empty, means the regions are
/// too small (or the reserve too starved) for the lanes to own one each — the
/// wrong shape, not the wrong idea. Compare against `allocator_supply.refills`.
#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct AllocRegionStats {
    pub regions: usize,
    pub region_blocks: u64,
    pub switches: u64,
    pub refill_misses: u64,
    pub serialized: bool,
}

impl AllocSupplyStats {
    /// Blocks obtained per global-lock refill (the refill's real yield, as
    /// opposed to `LANE_EXTENT_CACHE_REFILL_BLOCKS`'s intent).
    pub fn blocks_per_refill(&self) -> f64 {
        if self.refills == 0 {
            return 0.0;
        }
        self.refill_blocks as f64 / self.refills as f64
    }

    /// Contiguous runs obtained per refill.
    pub fn runs_per_refill(&self) -> f64 {
        if self.refills == 0 {
            return 0.0;
        }
        self.refill_runs as f64 / self.refills as f64
    }

    /// Blocks per contiguous run the refill took — the write path's contiguity in
    /// one number, and the local stand-in for chunklet's per-PD adjacency merge.
    /// One stripe's worth (6.15 blocks on the 2026-08-01 box, stripe = 6) means
    /// every LV3 op lands at an unrelated PBA and the merge is ~1x.
    pub fn blocks_per_run(&self) -> f64 {
        if self.refill_runs == 0 {
            return 0.0;
        }
        self.refill_blocks as f64 / self.refill_runs as f64
    }

    /// Aligned allocations served per global-lock refill. 1.0 means the lane
    /// cache is buying nothing and every allocation serializes on `free_pools`.
    pub fn allocs_per_refill(&self) -> f64 {
        if self.refills == 0 {
            return 0.0;
        }
        self.aligned_allocs as f64 / self.refills as f64
    }
}

/// Which call path acquired `free_pools`.
///
/// This split exists to answer one question no other metric can: **when a flush
/// writer waits on the free lock, who is holding it?** `flush_writer_alloc_split`
/// only says the writer spent 181-760 µs inside the allocator, and the local
/// `aged_pool_bench` says the allocator's own work is 0.1-8.7 µs — so nearly all
/// of it is wait, attributable to someone else's hold. The 2026-07-29 attempt to
/// shorten the batch hold could not be validated precisely because this
/// attribution did not exist (the "~3% GC lock occupancy" that motivated it was
/// estimated from GC block rates, never measured).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FreeLockSite {
    /// `refill_stripe_extent_lane` + the aligned global fallback — the hot
    /// full-stripe writer path (100% of box `aligned_ops`).
    WriterRefill = 0,
    /// `refill_extent_lane` — the unaligned writer path (0 ops on the box).
    WriterUnaligned,
    /// Single-block / non-lane allocation, incl. the packer's lane refill.
    SmallAlloc,
    RetireBatch,
    RetireOne,
    ReclaimBatch,
    ReclaimOne,
    FreeBatch,
    FreeOne,
    /// `drain_lane_caches` — one hold that re-inserts every lane's cache.
    Drain,
    Quarantine,
    /// `classify_stripe_windows` — the scan-driven defrag selector's per-cycle
    /// window classification. Kept separate from `Audit` because it is the one
    /// GC query whose trip count scales with the compactor's scan budget, so it
    /// is the site to watch if defrag starts showing up in the writer's wait.
    DefragClassify,
    /// Read-only status / GC queries (`contiguity_stats`, `is_free`, …).
    Audit,
    /// Startup / rebuild / geometry / grow.
    Setup,
}

/// Number of variants in [`FreeLockSite`].
const FREE_LOCK_SITES: usize = 14;

impl FreeLockSite {
    pub const ALL: [FreeLockSite; FREE_LOCK_SITES] = [
        Self::WriterRefill,
        Self::WriterUnaligned,
        Self::SmallAlloc,
        Self::RetireBatch,
        Self::RetireOne,
        Self::ReclaimBatch,
        Self::ReclaimOne,
        Self::FreeBatch,
        Self::FreeOne,
        Self::Drain,
        Self::Quarantine,
        Self::DefragClassify,
        Self::Audit,
        Self::Setup,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::WriterRefill => "writer_refill",
            Self::WriterUnaligned => "writer_unaligned",
            Self::SmallAlloc => "small_alloc",
            Self::RetireBatch => "retire_batch",
            Self::RetireOne => "retire_one",
            Self::ReclaimBatch => "reclaim_batch",
            Self::ReclaimOne => "reclaim_one",
            Self::FreeBatch => "free_batch",
            Self::FreeOne => "free_one",
            Self::Drain => "drain",
            Self::Quarantine => "quarantine",
            Self::DefragClassify => "defrag_classify",
            Self::Audit => "audit",
            Self::Setup => "setup",
        }
    }
}

/// Which call path acquired `retired_extents` (and, nested inside it,
/// `retired_age`).
///
/// This exists to settle one question the `free_pools` attribution raised but
/// could not answer: after region sharding, `retire_batch`'s summed region hold
/// went 218 s -> 1670 s while its per-acquisition cost went 5.4 -> 185 µs.
/// `retire_extents_batch` takes `retired` INSIDE the region hold, so a wait on
/// `retired` is reported as region hold time. Either that wait is most of the
/// 1670 s (fix: stop waiting for `retired` under a region lock) or the per-extent
/// work itself got more expensive (fix: fewer/warmer regions) — opposite
/// directions, and the `free_pools` table alone cannot tell them apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetiredLockSite {
    /// `retire_extents_batch` — one acquisition per region hold.
    RetireBatch = 0,
    RetireOne,
    /// `reclaim_retired_extents_batch` Phase A (validate + split out the cover).
    ReclaimPhaseA,
    /// `reclaim_retired_extents_batch`'s conflict re-insert.
    ReclaimReinsert,
    ReclaimOne,
    /// `free_extents_batch` — taken inside the region hold, like retire.
    FreeBatch,
    /// The single free path's `overlapping_retired_extent` double-free guard.
    FreeOne,
    /// `aged_candidates` — the GC reclaim selector. Walks the retired set and
    /// prunes the age log under both locks, so a long hold here blocks every
    /// retire that already owns a region lock.
    AgedCandidates,
    IsRetired,
    /// `retired_overlap_blocks` — GC defrag's per-cluster query.
    OverlapBlocks,
    /// `retired_candidates` — audit / accounting snapshot.
    Candidates,
    /// `retired_stripe_windows` — the resident defragger's window enumeration.
    /// Separate from `Candidates` because it is a STANDING background walk on
    /// its own cadence, so it is the retired-side counterpart of
    /// [`FreeLockSite::DefragClassify`]: if defrag ever shows up in a writer's
    /// wait, these two sites are where to look first.
    DefragWindows,
    Audit,
    /// Startup / rebuild.
    Setup,
}

/// Number of variants in [`RetiredLockSite`].
const RETIRED_LOCK_SITES: usize = 14;

impl RetiredLockSite {
    pub const ALL: [RetiredLockSite; RETIRED_LOCK_SITES] = [
        Self::RetireBatch,
        Self::RetireOne,
        Self::ReclaimPhaseA,
        Self::ReclaimReinsert,
        Self::ReclaimOne,
        Self::FreeBatch,
        Self::FreeOne,
        Self::AgedCandidates,
        Self::IsRetired,
        Self::OverlapBlocks,
        Self::Candidates,
        Self::DefragWindows,
        Self::Audit,
        Self::Setup,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::RetireBatch => "retire_batch",
            Self::RetireOne => "retire_one",
            Self::ReclaimPhaseA => "reclaim_phase_a",
            Self::ReclaimReinsert => "reclaim_reinsert",
            Self::ReclaimOne => "reclaim_one",
            Self::FreeBatch => "free_batch",
            Self::FreeOne => "free_one",
            Self::AgedCandidates => "aged_candidates",
            Self::IsRetired => "is_retired",
            Self::OverlapBlocks => "overlap_blocks",
            Self::Candidates => "candidates",
            Self::DefragWindows => "defrag_windows",
            Self::Audit => "audit",
            Self::Setup => "setup",
        }
    }
}

/// Per-thread accounting shards, and how many acquisitions share one timing
/// sample — the two constants that keep this instrumentation off the wall it
/// measures.
///
/// The 2026-08-12 profile of the exhausted regime found ~66% of the flush
/// writers' CPU in region-lock acquire/release machinery against 15% in the
/// actual free-space search, and the accounting was a load-bearing part of it:
/// at ~10^4 acquisitions per unaligned allocation, 16 writers were executing
/// five read-modify-writes on ONE set of shared cache lines (`charge_wait` alone
/// = 10.35% of `lock_region_raw`'s own time, half of that the shared `fetch_max`)
/// plus four `Instant::now()` calls (vdso `clock_gettime` = 6.57% of the whole
/// cycle). Both charges land INSIDE the critical section, so they also inflated
/// the `wait_ns`/`hold_ns` this table reports — every decision ever made from
/// `free_lock.*` was made on numbers the measurement itself had padded.
///
/// Threads take a slot round-robin at first use; more threads than slots share
/// one (still correct, just contended again). 64 covers the box's 16 flush
/// writers + coalescers + dedup + GC + defrag with room to spare.
const LOCK_STAT_SHARDS: usize = 64;

/// Default sampling stride: time 1 acquisition in this many, per (shard, site).
///
/// Tests default to 1 (time everything) so the accounting tests stay exact; a
/// dedicated test covers the sampled path.
const DEFAULT_LOCK_STAT_STRIDE: u64 = if cfg!(test) { 1 } else { 16 };

/// Resolved sampling stride; `0` = not yet read from the environment.
static LOCK_STAT_STRIDE: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// This thread's accounting shard, assigned once on first use.
    static LOCK_STAT_SLOT: usize = {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        NEXT.fetch_add(1, Ordering::Relaxed) % LOCK_STAT_SHARDS
    };
}

/// The stride in force, resolving `ONYX_LOCK_STATS_STRIDE` on first use.
fn lock_stat_stride() -> u64 {
    let cached = LOCK_STAT_STRIDE.load(Ordering::Relaxed);
    if cached != 0 {
        return cached;
    }
    let resolved = std::env::var("ONYX_LOCK_STATS_STRIDE")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_LOCK_STAT_STRIDE);
    LOCK_STAT_STRIDE.store(resolved, Ordering::Relaxed);
    resolved
}

/// Set the lock-accounting sampling stride at runtime (`1` = time every
/// acquisition, the pre-sampling behaviour).
///
/// Exists so the cost of the instrumentation can be A/B'd **inside one process**:
/// per [`set_region_serialize`], restart-per-arm comparisons resolve nothing on
/// this box. Safe at any time — it only changes how often the clock is read, and
/// `snapshot` scales the sums by the sample rate either way, so a stride change
/// mid-run costs accuracy on the straddling interval and nothing else.
pub fn set_lock_stats_stride(stride: u64) {
    LOCK_STAT_STRIDE.store(stride.max(1), Ordering::Relaxed);
}

/// The sampling stride currently in force — see [`set_lock_stats_stride`].
pub fn lock_stats_stride() -> u64 {
    lock_stat_stride()
}

/// One thread-slot's copy of the per-site counters. Cache-line aligned so two
/// slots never share a line; sites WITHIN a slot may share one, which costs
/// nothing because only that slot's thread writes them.
#[repr(align(64))]
struct LockStatShard<const N: usize> {
    acquisitions: [AtomicU64; N],
    /// Extents processed under the hold, charged by the batch paths only. This
    /// is what turns a per-hold cost into a per-EXTENT cost: sharding cuts one
    /// hold into many, so per-acquisition numbers move even when the work per
    /// extent is unchanged.
    items: [AtomicU64; N],
    /// Acquisitions that actually carried a timing sample — the divisor
    /// `snapshot` scales `wait_ns` / `hold_ns` by.
    timed: [AtomicU64; N],
    wait_ns: [AtomicU64; N],
    wait_ns_max: [AtomicU64; N],
    hold_ns: [AtomicU64; N],
    hold_ns_max: [AtomicU64; N],
}

impl<const N: usize> LockStatShard<N> {
    fn new() -> Self {
        Self {
            acquisitions: std::array::from_fn(|_| AtomicU64::new(0)),
            items: std::array::from_fn(|_| AtomicU64::new(0)),
            timed: std::array::from_fn(|_| AtomicU64::new(0)),
            wait_ns: std::array::from_fn(|_| AtomicU64::new(0)),
            wait_ns_max: std::array::from_fn(|_| AtomicU64::new(0)),
            hold_ns: std::array::from_fn(|_| AtomicU64::new(0)),
            hold_ns_max: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    /// Count one acquisition of `site` and decide whether to time it.
    ///
    /// The stride is applied to this shard's own acquisition count, so sample
    /// number 1 is ALWAYS timed: a site acquired once still reports a hold, which
    /// is what the accounting tests (and any "did this path run at all" read)
    /// depend on.
    fn count(&self, site: usize, stride: u64) -> bool {
        let n = self.acquisitions[site].fetch_add(1, Ordering::Relaxed) + 1;
        stride <= 1 || n % stride == 1
    }

    /// Charge the wait for one TIMED acquisition of `site`.
    fn charge_wait(&self, site: usize, waited: u64) {
        self.timed[site].fetch_add(1, Ordering::Relaxed);
        self.wait_ns[site].fetch_add(waited, Ordering::Relaxed);
        self.wait_ns_max[site].fetch_max(waited, Ordering::Relaxed);
    }

    /// Charge the hold for one TIMED release of `site`.
    fn charge_hold(&self, site: usize, held: u64) {
        self.hold_ns[site].fetch_add(held, Ordering::Relaxed);
        self.hold_ns_max[site].fetch_max(held, Ordering::Relaxed);
    }

    fn charge_items(&self, site: usize, n: u64) {
        self.items[site].fetch_add(n, Ordering::Relaxed);
    }
}

/// Per-site wait/hold accounting for ONE mutex, sharded per thread. Monotonic
/// counters, so two reads difference cleanly; the `_max` fields are high-water
/// marks and do NOT difference (`hold_ns_max` answers "what is the worst
/// shut-out window this site ever caused"; `wait_ns_max` separates steady
/// queueing from a single blackout by a long holder).
struct SiteLockStats<const N: usize> {
    shards: Vec<LockStatShard<N>>,
    /// Per-instance stride, `0` = follow the process-wide one. Test-only: the
    /// suite runs in parallel in one process, so a test must not perturb another
    /// test's accounting through [`set_lock_stats_stride`].
    #[cfg(test)]
    stride_override: AtomicU64,
    /// Test-only: force every thread onto ONE shard, reproducing the pre-sharding
    /// single-cache-line shape so `bench_lock_stats_overhead` can A/B it inside
    /// one process — the same in-process A/B discipline as
    /// [`set_region_serialize`]. `usize::MAX` = per-thread (production).
    #[cfg(test)]
    shard_pin: AtomicUsize,
}

impl<const N: usize> SiteLockStats<N> {
    fn new() -> Self {
        Self {
            shards: (0..LOCK_STAT_SHARDS).map(|_| LockStatShard::new()).collect(),
            #[cfg(test)]
            stride_override: AtomicU64::new(0),
            #[cfg(test)]
            shard_pin: AtomicUsize::new(usize::MAX),
        }
    }

    #[cfg(not(test))]
    fn stride(&self) -> u64 {
        lock_stat_stride()
    }

    #[cfg(test)]
    fn stride(&self) -> u64 {
        match self.stride_override.load(Ordering::Relaxed) {
            0 => lock_stat_stride(),
            n => n,
        }
    }

    /// This thread's shard.
    #[cfg(not(test))]
    fn shard(&self) -> &LockStatShard<N> {
        &self.shards[LOCK_STAT_SLOT.with(|slot| *slot)]
    }

    #[cfg(test)]
    fn shard(&self) -> &LockStatShard<N> {
        match self.shard_pin.load(Ordering::Relaxed) {
            usize::MAX => &self.shards[LOCK_STAT_SLOT.with(|slot| *slot)],
            pinned => &self.shards[pinned],
        }
    }

    /// Take this thread's shard and count one acquisition of `site`, returning
    /// the wait's start instant only when this acquisition is being timed.
    ///
    /// One TLS read and one private-line `fetch_add` on the untimed path; the
    /// caller keeps the shard reference so the release side needs neither.
    ///
    /// The count is taken BEFORE the lock (it has to be, to decide whether to
    /// read the clock), so `acquisitions` counts attempts rather than completed
    /// acquisitions. Every attempt here does complete — these are plain blocking
    /// `lock()` calls, no `try_lock` and no timeout — so the two differ only by
    /// the handful currently in flight.
    fn begin(&self, site: usize) -> (&LockStatShard<N>, Option<Instant>) {
        let shard = self.shard();
        let queued = shard.count(site, self.stride()).then(Instant::now);
        (shard, queued)
    }

    fn snapshot(&self, names: [&'static str; N]) -> Vec<LockSiteStats> {
        (0..N)
            .map(|i| {
                let mut out = LockSiteStats {
                    site: names[i],
                    acquisitions: 0,
                    items: 0,
                    timed: 0,
                    wait_ns: 0,
                    wait_ns_max: 0,
                    hold_ns: 0,
                    hold_ns_max: 0,
                };
                for shard in &self.shards {
                    let load = |a: &AtomicU64| a.load(Ordering::Relaxed);
                    out.acquisitions += load(&shard.acquisitions[i]);
                    out.items += load(&shard.items[i]);
                    out.timed += load(&shard.timed[i]);
                    out.wait_ns += load(&shard.wait_ns[i]);
                    out.hold_ns += load(&shard.hold_ns[i]);
                    out.wait_ns_max = out.wait_ns_max.max(load(&shard.wait_ns_max[i]));
                    out.hold_ns_max = out.hold_ns_max.max(load(&shard.hold_ns_max[i]));
                }
                // Sampling measures `timed` of `acquisitions` acquisitions, so
                // scale the sums back up: the table keeps meaning "total ns at
                // this site" and stays comparable with the pre-sampling history
                // (and with `hold_ns / wall` occupancy reads). Means are
                // unbiased — the sample is chosen by acquisition COUNT, which is
                // independent of how long any one of them waits. The `_max`
                // fields stay raw: a high-water mark cannot be extrapolated, so
                // under sampling they are a lower bound.
                let scale = |sum: u64| {
                    if out.timed == 0 || out.acquisitions <= out.timed {
                        sum
                    } else {
                        ((u128::from(sum) * u128::from(out.acquisitions))
                            / u128::from(out.timed)) as u64
                    }
                };
                out.wait_ns = scale(out.wait_ns);
                out.hold_ns = scale(out.hold_ns);
                out
            })
            .collect()
    }
}

type FreeLockStats = SiteLockStats<FREE_LOCK_SITES>;
type RetiredLockStats = SiteLockStats<RETIRED_LOCK_SITES>;

/// One site's wait/hold snapshot for one lock.
///
/// `wait_ns` / `hold_ns` are totals extrapolated from `timed` samples out of
/// `acquisitions` acquisitions (see [`SiteLockStats::snapshot`]); `timed ==
/// acquisitions` means nothing was sampled away. The `_max` fields are raw
/// observed maxima, i.e. a lower bound when sampling is on.
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct LockSiteStats {
    pub site: &'static str,
    pub acquisitions: u64,
    pub items: u64,
    pub timed: u64,
    pub wait_ns: u64,
    pub wait_ns_max: u64,
    pub hold_ns: u64,
    pub hold_ns_max: u64,
}

impl LockSiteStats {
    /// Mean wait per acquisition, µs.
    pub fn wait_us(&self) -> f64 {
        if self.acquisitions == 0 {
            return 0.0;
        }
        self.wait_ns as f64 / 1000.0 / self.acquisitions as f64
    }

    /// Mean hold per acquisition, µs.
    pub fn hold_us(&self) -> f64 {
        if self.acquisitions == 0 {
            return 0.0;
        }
        self.hold_ns as f64 / 1000.0 / self.acquisitions as f64
    }
}

/// RAII guard over one plain `Mutex` that charges its hold to a site — the
/// `retired_extents` / `retired_age` analogue of [`FreeLockGuard`] (which
/// additionally refreshes per-region hints on release).
struct TimedGuard<'a, T, const N: usize> {
    inner: std::sync::MutexGuard<'a, T>,
    shard: &'a LockStatShard<N>,
    site: usize,
    /// `None` when this acquisition was sampled away — see
    /// [`LOCK_STAT_SHARDS`].
    acquired: Option<Instant>,
}

impl<'a, T, const N: usize> TimedGuard<'a, T, N> {
    /// Acquire `lock`, charging the wait to `site` and (on drop) the hold. Two
    /// clock reads on a timed acquisition, none otherwise; the post-lock read
    /// serves as both the wait's end and the hold's start.
    fn new(stats: &'a SiteLockStats<N>, site: usize, lock: &'a Mutex<T>) -> Self {
        let (shard, queued) = stats.begin(site);
        let inner = lock.lock().unwrap();
        let acquired = queued.map(|queued| {
            let now = Instant::now();
            shard.charge_wait(site, now.duration_since(queued).as_nanos() as u64);
            now
        });
        Self {
            inner,
            shard,
            site,
            acquired,
        }
    }

    /// Record how many extents this hold covered (batch paths only).
    fn charge_items(&self, n: u64) {
        self.shard.charge_items(self.site, n);
    }
}

impl<T, const N: usize> Drop for TimedGuard<'_, T, N> {
    fn drop(&mut self) {
        if let Some(acquired) = self.acquired {
            self.shard
                .charge_hold(self.site, acquired.elapsed().as_nanos() as u64);
        }
    }
}

impl<T, const N: usize> std::ops::Deref for TimedGuard<'_, T, N> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T, const N: usize> std::ops::DerefMut for TimedGuard<'_, T, N> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

/// RAII guard over ONE region's `FreePools` that charges its hold to a
/// [`FreeLockSite`] and refreshes that region's advisory hints on release.
struct FreeLockGuard<'a> {
    pools: std::sync::MutexGuard<'a, FreePools>,
    shard: &'a LockStatShard<FREE_LOCK_SITES>,
    site: usize,
    /// `None` when this acquisition was sampled away — see [`LOCK_STAT_SHARDS`].
    acquired: Option<Instant>,
    /// Advisory aggregates for this region, refreshed on drop — see
    /// [`RegionPools::free_hint`].
    free_hint: &'a AtomicU64,
    stripe_hint: &'a AtomicU64,
}

impl Drop for FreeLockGuard<'_> {
    fn drop(&mut self) {
        // Refresh BEFORE the hold is charged so the hints are always published
        // by a thread that still holds the region lock: a reader can therefore
        // only ever see a value that was true at some point while the lock was
        // held, never a torn or future one. Both reads are O(1) maintained
        // aggregates (plus the normally-empty quarantine map).
        self.free_hint
            .store(self.pools.free_blocks_in_pools(), Ordering::Relaxed);
        self.stripe_hint.store(
            self.pools
                .stripe_reserve
                .largest()
                .map_or(0, |run| u64::from(run.count)),
            Ordering::Relaxed,
        );
        if let Some(acquired) = self.acquired {
            self.shard
                .charge_hold(self.site, acquired.elapsed().as_nanos() as u64);
        }
    }
}

impl FreeLockGuard<'_> {
    /// Record how many extents this hold covered (batch paths only).
    fn charge_items(&self, n: u64) {
        self.shard.charge_items(self.site, n);
    }
}

impl std::ops::Deref for FreeLockGuard<'_> {
    type Target = FreePools;
    fn deref(&self) -> &FreePools {
        &self.pools
    }
}

impl std::ops::DerefMut for FreeLockGuard<'_> {
    fn deref_mut(&mut self) -> &mut FreePools {
        &mut self.pools
    }
}

/// One aligned lane refill's request shape — see
/// [`SpaceAllocator::refill_stripe_extent_lane`].
#[derive(Debug, Clone, Copy)]
struct StripeRefill {
    /// Blocks the refill must hand back (already a whole number of stripes).
    min_count: u32,
    /// Block budget for the whole refill, i.e. how much may be parked in the
    /// lane cache.
    max_count: u32,
    /// RAID geometry the reserve is indexed for; a region whose pools carry a
    /// different `(stripe, phase)` is skipped.
    stripe: u32,
    phase: u32,
    /// Minimum width a reserve run must have to QUALIFY as a candidate. Always
    /// `>= min_count`; equal to it on the legacy path.
    floor: u32,
}

/// Free-space policy classes protected by one lock. Every free PBA belongs to
/// exactly one of `general`, `stripe_reserve`, an active quarantine's
/// `free_parts`, or a detached lane cache.
struct FreePools {
    general: FreeSet,
    stripe_reserve: FreeSet,
    quarantines: BTreeMap<u64, QuarantineTarget>,
}

struct QuarantineTarget {
    range: Extent,
    free_parts: FreeSet,
}

impl FreePools {
    fn new() -> Self {
        Self {
            general: FreeSet::new(),
            stripe_reserve: FreeSet::new(),
            quarantines: BTreeMap::new(),
        }
    }

    fn geometry(&self) -> Option<(u32, u32)> {
        self.general.geometry()
    }

    fn empty_set_with_geometry(&self) -> FreeSet {
        let mut set = FreeSet::new();
        if let Some((stripe, phase)) = self.geometry() {
            set.set_geometry(stripe, phase);
        }
        set
    }

    /// Empty every policy class and hand back the free runs they held, including
    /// the already-free parts of active quarantines (which are dropped — the
    /// pre-region `set_geometry` did the same, pinned by
    /// `geometry_change_preserves_quarantined_free_blocks`).
    fn take_all_runs(&mut self) -> Vec<Extent> {
        let mut runs: Vec<Extent> = self.general.by_addr().iter().copied().collect();
        runs.extend(self.stripe_reserve.by_addr().iter().copied());
        runs.extend(
            self.quarantines
                .values()
                .flat_map(|target| target.free_parts.by_addr().iter().copied()),
        );
        self.general = FreeSet::new();
        self.stripe_reserve = FreeSet::new();
        self.quarantines.clear();
        runs
    }

    /// Install a geometry on an already-emptied pool.
    fn reset_geometry(&mut self, stripe: u32, phase: u32) {
        self.general = FreeSet::new();
        self.stripe_reserve = FreeSet::new();
        self.general.set_geometry(stripe, phase);
        self.stripe_reserve.set_geometry(stripe, phase);
        self.quarantines.clear();
    }

    fn replace_general(&mut self, free: BTreeSet<Extent>) {
        let geometry = self.geometry();
        self.general = FreeSet::new();
        self.stripe_reserve = FreeSet::new();
        if let Some((stripe, phase)) = geometry {
            self.general.set_geometry(stripe, phase);
            self.stripe_reserve.set_geometry(stripe, phase);
        }
        self.quarantines.clear();
        for run in free {
            self.insert_classified(run);
        }
    }

    /// Insert a free run into the canonical policy partition. Adjacent runs in
    /// either pool are first folded into one maximal run; its aligned whole-
    /// stripe middle goes to the reserve and only its head/tail stay general.
    fn insert_classified(&mut self, extent: Extent) {
        let mut start = extent.start.0;
        let mut end = extent.end_pba().0;

        loop {
            let mut changed = false;
            for reserve in [false, true] {
                let set = if reserve {
                    &mut self.stripe_reserve
                } else {
                    &mut self.general
                };
                let probe = Extent::single(Pba(start));
                if let Some(before) = set.by_addr().range(..=probe).next_back().copied() {
                    if before.end_pba().0 == start {
                        set.remove(&before);
                        start = before.start.0;
                        changed = true;
                    }
                }
                let probe = Extent::single(Pba(end));
                if let Some(after) = set.by_addr().range(probe..).next().copied() {
                    if after.start.0 == end {
                        set.remove(&after);
                        end = after.end_pba().0;
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }

        let Some((stripe, phase)) = self.geometry().filter(|(stripe, _)| *stripe > 1) else {
            Self::insert_split(&mut self.general, start, end - start, 1);
            return;
        };
        let aligned_start = SpaceAllocator::align_up_pba(start, stripe as u64, phase as u64);
        if aligned_start >= end {
            // A sub-stripe fragment can end before the next alignment point.
            // Never use that future alignment as the head boundary: doing so
            // would manufacture free PBAs beyond the released range.
            Self::insert_split(&mut self.general, start, end - start, 1);
            return;
        }
        let aligned_blocks = end
            .saturating_sub(aligned_start)
            .checked_div(stripe as u64)
            .unwrap_or(0)
            * stripe as u64;
        let aligned_end = aligned_start + aligned_blocks;
        if aligned_start > start {
            Self::insert_split(&mut self.general, start, aligned_start - start, 1);
        }
        if aligned_blocks > 0 {
            Self::insert_split(
                &mut self.stripe_reserve,
                aligned_start,
                aligned_blocks,
                stripe,
            );
        }
        if aligned_end < end {
            Self::insert_split(&mut self.general, aligned_end, end - aligned_end, 1);
        }
    }

    fn insert_split(set: &mut FreeSet, mut start: u64, mut count: u64, multiple: u32) {
        let multiple = u64::from(multiple.max(1));
        let max_chunk = (u32::MAX as u64 / multiple) * multiple;
        debug_assert!(max_chunk > 0);
        while count > 0 {
            let take = count.min(max_chunk);
            set.insert(Extent::new(Pba(start), take as u32));
            start += take;
            count -= take;
        }
    }

    fn free_blocks_in_pools(&self) -> u64 {
        self.general.blocks_total()
            + self.stripe_reserve.blocks_total()
            + self
                .quarantines
                .values()
                .map(|target| target.free_parts.blocks_total())
                .sum::<u64>()
    }

    fn overlapping_in_set(set: &FreeSet, extent: Extent) -> Option<Extent> {
        SpaceAllocator::overlapping_extent(set.by_addr(), extent)
    }

    fn overlapping_free(&self, extent: Extent) -> Option<Extent> {
        Self::overlapping_in_set(&self.general, extent)
            .or_else(|| Self::overlapping_in_set(&self.stripe_reserve, extent))
            .or_else(|| {
                self.quarantine_starts_overlapping(extent)
                    .into_iter()
                    .find_map(|start| {
                        Self::overlapping_in_set(
                            &self
                                .quarantines
                                .get(&start)
                                .expect("quarantine key remains present")
                                .free_parts,
                            extent,
                        )
                    })
            })
    }

    /// Whether the union of policy pools covers `extent`. Canonical
    /// classification may split one physical run at general/reserve
    /// boundaries, so a single-set covering query is insufficient. Advance by
    /// whole stored runs rather than probing every block.
    fn covers_free(&self, extent: Extent) -> bool {
        let mut cursor = extent.start.0;
        let end = extent.end_pba().0;
        while cursor < end {
            let Some(run) = self.overlapping_free(Extent::single(Pba(cursor))) else {
                return false;
            };
            let next = run.end_pba().0.min(end);
            if next <= cursor {
                return false;
            }
            cursor = next;
        }
        true
    }

    fn overlaps_reserve(&self, extent: Extent) -> bool {
        Self::overlapping_in_set(&self.stripe_reserve, extent).is_some()
    }

    fn overlapping_quarantine(&self, extent: Extent) -> Option<Extent> {
        let mut candidate = self
            .quarantines
            .range(..=extent.start.0)
            .next_back()
            .map(|(_, target)| target.range);
        if candidate.is_none_or(|range| range.end_pba().0 <= extent.start.0) {
            candidate = self
                .quarantines
                .range(extent.start.0..)
                .next()
                .map(|(_, target)| target.range);
        }
        candidate.filter(|range| SpaceAllocator::extents_overlap(*range, extent))
    }

    /// Blocks of `range` covered by this region's free space — general +
    /// stripe reserve + the already-free parts of any active quarantine.
    /// Shared by the single-window [`SpaceAllocator::free_overlap_blocks`] and
    /// the batched [`SpaceAllocator::classify_stripe_windows`], so the two can
    /// never disagree.
    fn overlap_free_blocks(&self, range: Extent) -> u64 {
        let mut covered = free_set_overlap_blocks(&self.general, range)
            + free_set_overlap_blocks(&self.stripe_reserve, range);
        for start in self.quarantine_starts_overlapping(range) {
            covered += free_set_overlap_blocks(
                &self
                    .quarantines
                    .get(&start)
                    .expect("quarantine key remains present")
                    .free_parts,
                range,
            );
        }
        covered
    }

    fn quarantine_starts_overlapping(&self, extent: Extent) -> Vec<u64> {
        let mut starts = Vec::new();
        if let Some((&start, target)) = self.quarantines.range(..extent.start.0).next_back() {
            if target.range.end_pba().0 > extent.start.0 {
                starts.push(start);
            }
        }
        for (&start, target) in self.quarantines.range(extent.start.0..extent.end_pba().0) {
            if target.range.start.0 >= extent.end_pba().0 {
                break;
            }
            starts.push(start);
        }
        starts
    }

    fn extract_from_general(&mut self, range: Extent) -> Vec<Extent> {
        let mut overlaps = Vec::new();
        if let Some(before) = self
            .general
            .by_addr()
            .range(..Extent::single(range.start))
            .next_back()
            .copied()
        {
            if before.end_pba().0 > range.start.0 {
                overlaps.push(before);
            }
        }
        for extent in self.general.by_addr().range(Extent::single(range.start)..) {
            if extent.start.0 >= range.end_pba().0 {
                break;
            }
            overlaps.push(*extent);
        }
        let mut extracted = Vec::with_capacity(overlaps.len());
        for extent in overlaps {
            self.general.remove(&extent);
            let intersection_start = extent.start.0.max(range.start.0);
            let intersection_end = extent.end_pba().0.min(range.end_pba().0);
            if extent.start.0 < intersection_start {
                self.general.insert(Extent::new(
                    extent.start,
                    (intersection_start - extent.start.0) as u32,
                ));
            }
            extracted.push(Extent::new(
                Pba(intersection_start),
                (intersection_end - intersection_start) as u32,
            ));
            if intersection_end < extent.end_pba().0 {
                self.general.insert(Extent::new(
                    Pba(intersection_end),
                    (extent.end_pba().0 - intersection_end) as u32,
                ));
            }
        }
        extracted
    }

    /// Route newly-free blocks around active quarantine boundaries.
    fn release_extent(&mut self, extent: Extent) {
        let target_starts = self.quarantine_starts_overlapping(extent);
        let mut cursor = extent.start.0;
        let end = extent.end_pba().0;
        for target_start in target_starts {
            let target_range = self
                .quarantines
                .get(&target_start)
                .expect("collected quarantine target remains present")
                .range;
            if cursor < target_range.start.0 {
                self.insert_classified(Extent::new(
                    Pba(cursor),
                    (target_range.start.0 - cursor) as u32,
                ));
            }
            let part_start = cursor.max(target_range.start.0);
            let part_end = end.min(target_range.end_pba().0);
            if part_start < part_end {
                let target = self
                    .quarantines
                    .get_mut(&target_start)
                    .expect("collected quarantine target remains present");
                target
                    .free_parts
                    .coalesce_insert(Extent::new(Pba(part_start), (part_end - part_start) as u32));
                cursor = part_end;
            }
        }
        if cursor < end {
            self.insert_classified(Extent::new(Pba(cursor), (end - cursor) as u32));
        }
    }
}

/// Blocks of `range` covered by `set` — O(log N + overlaps in range). Callers
/// hand in the UNCLIPPED range and apply it per region: a region's sets only
/// hold in-region extents, so clamping to the full range is equivalent to
/// clipping first (see [`RegionPools`]'s containment invariant).
fn free_set_overlap_blocks(set: &FreeSet, range: Extent) -> u64 {
    let (s, e) = (range.start.0, range.end_pba().0);
    let mut covered = 0u64;
    // The last extent starting at/before `s` may reach into the range.
    if let Some(prev) = set
        .by_addr()
        .range(..=Extent::single(range.start))
        .next_back()
    {
        covered += prev.end_pba().0.min(e).saturating_sub(s);
    }
    for ext in set.by_addr().range(Extent::single(Pba(s + 1))..) {
        if ext.start.0 >= e {
            break;
        }
        covered += ext.end_pba().0.min(e) - ext.start.0;
    }
    covered
}

/// The free space, sharded by PBA address into independently-locked regions.
///
/// Each region holds a complete [`FreePools`] (general / stripe reserve /
/// quarantines) restricted to its own address range — **every insert path
/// clips to the region**, so a region's sets never contain an out-of-region
/// extent. That single invariant is what makes every query composable: a
/// containment or overlap question about an extent is answered by asking only
/// the regions it spans.
///
/// Why shard by address rather than shorten the holds: the 2026-07-29 box
/// attribution showed 98% of the holding comes from GC retire/reclaim, whose
/// per-extent cost is dominated by work on the RETIRED structures that must
/// stay atomic with the free-side overlap check (a concurrent `free_extent`
/// and a retire that both pass their checks would produce double ownership —
/// the project's two premature-free P0s were exactly this class of bug). So the
/// holds are kept EXACTLY as they were, atomicity included, and only the lock
/// they serialize on is split: one 68%-busy mutex becomes N at 68/N%.
///
/// Region boundaries are stripe-aligned, so `insert_classified`'s
/// general/reserve classification behaves identically inside a region as it did
/// globally. The only thing lost is coalescing ACROSS a boundary: one seam per
/// region (≤ 2048 extents against the box's 24.6 M) never folds.
struct RegionPools {
    pools: Vec<Mutex<FreePools>>,
    /// Routing divisor in blocks; 0 = single region (sharding off). Only
    /// rewritten by `set_geometry`, which holds every region lock.
    region_blocks: AtomicU64,
    /// First stripe-aligned PBA. Region boundaries sit at
    /// `region_base + i*region_blocks` so none of them splits a stripe window.
    region_base: AtomicU64,
    /// Advisory per-region free-block totals, refreshed under the region lock by
    /// [`FreeLockGuard::drop`].
    ///
    /// Read lock-free ONLY to skip a region in an ascending walk or to pick a
    /// lane's region. A stale-zero read is indistinguishable from having taken
    /// that region's lock a moment earlier — the same benign race the single
    /// global lock always had between a free and a concurrent allocation — so it
    /// can never produce a wrong answer, only a slightly older one. The
    /// ENOSPC boundary keeps its `drain_lane_caches` + retry, unchanged.
    free_hint: Vec<AtomicU64>,
    /// Advisory per-region LARGEST stripe-reserve run, same discipline.
    ///
    /// Largest-run rather than summed capacity because the question every reader
    /// asks is "can this region serve a `need`-block aligned carve", and for the
    /// reserve that is EXACTLY `largest >= need`: every reserve extent is
    /// stripe-aligned with a stripe-multiple count (the `insert_classified`
    /// invariant), so its effective capacity equals its count. A summed hint
    /// answers a different question and says yes to a region holding a hundred
    /// single stripes when the request needs two contiguous ones.
    stripe_hint: Vec<AtomicU64>,
    /// Preferred owner of each region as `lane + 1` (0 = unclaimed).
    ///
    /// ZFS's metaslab insight is that the win comes from EXCLUSIVITY, not from
    /// contiguity: a lane that owns its region neither waits for nor is waited
    /// on by the other lanes. Purely advisory here — when no unclaimed region
    /// can serve a refill, lanes share rather than starve (which also covers
    /// `num_lanes > num_regions` on small devices).
    owner: Vec<AtomicUsize>,
    /// A/B gate — see [`set_region_serialize`]. Taken outermost, once per
    /// acquisition group, so arming it can never deadlock.
    gate: Mutex<()>,
}

impl RegionPools {
    fn new(usable_blocks: u64, regions: usize) -> Self {
        // Geometry is configured after construction (`set_stripe_geometry`), so
        // plan against stripe=1 now; `set_geometry` re-plans and re-routes.
        let (region_blocks, count) =
            RegionLayout::plan(usable_blocks, regions, 1).unwrap_or((0, 1));
        Self {
            pools: (0..count).map(|_| Mutex::new(FreePools::new())).collect(),
            region_blocks: AtomicU64::new(region_blocks),
            region_base: AtomicU64::new(RESERVED_BLOCKS),
            free_hint: (0..count).map(|_| AtomicU64::new(0)).collect(),
            stripe_hint: (0..count).map(|_| AtomicU64::new(0)).collect(),
            owner: (0..count).map(|_| AtomicUsize::new(0)).collect(),
            gate: Mutex::new(()),
        }
    }

    fn layout(&self) -> RegionLayout {
        RegionLayout {
            base: self.region_base.load(Ordering::Relaxed),
            blocks: self.region_blocks.load(Ordering::Relaxed),
            count: self.pools.len(),
        }
    }

    fn count(&self) -> usize {
        self.pools.len()
    }

    /// The layout this pool WOULD use for `(stripe, phase)`.
    ///
    /// The region count is fixed at construction (it sizes the mutex vector), so
    /// re-planning only moves the boundaries: `region_base` becomes the first
    /// stripe-aligned PBA and `region_blocks` is rounded up to a stripe
    /// multiple. Both keep every boundary stripe-aligned, which is what stops a
    /// boundary from stranding a partial stripe window in the general pool.
    /// Rounding up can leave the top regions unused (`RegionLayout::of` clamps),
    /// which costs nothing but an idle mutex.
    fn planned_layout(&self, stripe: u32, phase: u32) -> RegionLayout {
        let blocks = self.region_blocks.load(Ordering::Relaxed);
        if self.pools.len() <= 1 || blocks == 0 {
            return RegionLayout::single();
        }
        let stripe64 = u64::from(stripe.max(1));
        RegionLayout {
            base: SpaceAllocator::align_up_pba(RESERVED_BLOCKS, stripe64, u64::from(phase)),
            blocks: blocks.div_ceil(stripe64) * stripe64,
            count: self.pools.len(),
        }
    }

    /// Publish a re-planned layout. The caller MUST hold every region lock and
    /// must have emptied the regions first — an extent left behind under the old
    /// boundaries could otherwise end up in a region that does not own it,
    /// breaking the "a region only holds its own addresses" invariant every
    /// query depends on.
    fn publish_layout(&self, layout: RegionLayout) {
        self.region_base.store(layout.base, Ordering::Relaxed);
        self.region_blocks.store(layout.blocks, Ordering::Relaxed);
    }

    /// Take the A/B gate when armed. Callers hold the returned guard for the
    /// whole critical section, which is what makes N region locks behave as one.
    fn gate(&self) -> Option<std::sync::MutexGuard<'_, ()>> {
        REGION_SERIALIZE
            .load(Ordering::Relaxed)
            .then(|| self.gate.lock().unwrap())
    }
}

/// The regions spanned by one extent, locked in ascending index order.
///
/// Almost always exactly one region: extents on the hot paths are 1-6 blocks
/// against a ~76 K-block region. The multi-region case exists for correctness
/// (a free run released across a boundary, a rebuild, a grow) and is handled by
/// clipping the extent per region — never by widening what a region owns.
struct SpanGuard<'a> {
    layout: RegionLayout,
    lo: usize,
    guards: Vec<FreeLockGuard<'a>>,
    /// Declared last so it drops AFTER `guards` (Rust drops fields in
    /// declaration order), keeping the gate outermost.
    _gate: Option<std::sync::MutexGuard<'a, ()>>,
}

impl SpanGuard<'_> {
    fn region(&self, idx: usize) -> &FreePools {
        &self.guards[idx - self.lo]
    }

    /// Record how many extents this hold covered. Charged once per HOLD (not per
    /// region guard): the counter is per-site, and what the box read needs is
    /// "extents per hold", the divisor that converts a per-acquisition cost into
    /// a per-extent one.
    fn charge_items(&self, n: u64) {
        if let Some(first) = self.guards.first() {
            first.charge_items(n);
        }
    }

    fn region_mut(&mut self, idx: usize) -> &mut FreePools {
        &mut self.guards[idx - self.lo]
    }

    fn spans_one_region(&self) -> bool {
        self.guards.len() == 1
    }

    /// The one region this span covers, or `None` when it straddles a boundary.
    fn single_mut(&mut self) -> Option<&mut FreePools> {
        (self.guards.len() == 1).then(|| &mut *self.guards[0])
    }

    fn geometry(&self) -> Option<(u32, u32)> {
        self.guards[0].geometry()
    }

    fn empty_set_with_geometry(&self) -> FreeSet {
        self.guards[0].empty_set_with_geometry()
    }

    fn overlapping_free(&self, extent: Extent) -> Option<Extent> {
        let (lo, hi) = self.layout.span(extent);
        (lo..=hi).find_map(|idx| {
            let part = self.layout.clip(idx, extent)?;
            self.region(idx).overlapping_free(part)
        })
    }

    /// Whether the union of every spanned region's pools covers `extent`. Each
    /// region answers for its own slice; a region that owns none of the extent
    /// cannot withhold coverage.
    fn covers_free(&self, extent: Extent) -> bool {
        let (lo, hi) = self.layout.span(extent);
        (lo..=hi).all(|idx| match self.layout.clip(idx, extent) {
            Some(part) => self.region(idx).covers_free(part),
            None => true,
        })
    }

    fn overlaps_reserve(&self, extent: Extent) -> bool {
        let (lo, hi) = self.layout.span(extent);
        (lo..=hi).any(|idx| match self.layout.clip(idx, extent) {
            Some(part) => self.region(idx).overlaps_reserve(part),
            None => false,
        })
    }

    fn overlapping_quarantine(&self, extent: Extent) -> Option<Extent> {
        let (lo, hi) = self.layout.span(extent);
        (lo..=hi).find_map(|idx| {
            let part = self.layout.clip(idx, extent)?;
            self.region(idx).overlapping_quarantine(part)
        })
    }

    fn release_extent(&mut self, extent: Extent) {
        let (lo, hi) = self.layout.span(extent);
        for idx in lo..=hi {
            if let Some(part) = self.layout.clip(idx, extent) {
                self.region_mut(idx).release_extent(part);
            }
        }
    }

    fn insert_classified(&mut self, extent: Extent) {
        let (lo, hi) = self.layout.span(extent);
        for idx in lo..=hi {
            if let Some(part) = self.layout.clip(idx, extent) {
                self.region_mut(idx).insert_classified(part);
            }
        }
    }

    fn extract_from_general(&mut self, range: Extent) -> Vec<Extent> {
        let (lo, hi) = self.layout.span(range);
        let mut out = Vec::new();
        for idx in lo..=hi {
            if let Some(part) = self.layout.clip(idx, range) {
                out.extend(self.region_mut(idx).extract_from_general(part));
            }
        }
        out
    }
}

/// Split a batch into maximal runs of consecutive extents that share the SAME
/// region span, capped at `cap` entries per run.
///
/// With one region this is exactly `extents.chunks(cap)`, so unsharded behaviour
/// — including where the [`FREE_LOCK_HOLD_EXTENTS`] boundaries fall — is
/// unchanged. Sharded, an address-sorted batch (which is what
/// `buffer/flush/cleanup.rs::retire_dead_pbas` and the GC reclaim loop both
/// produce) yields one hold per region, so the lane-snapshot amortization
/// survives while the hold itself moves off a lock the writers share. An
/// unsorted batch simply gets shorter holds — correct, just more acquisitions.
///
/// Grouping (rather than one hold per extent) is what keeps the documented
/// `free -> retired -> retired_age` order intact: the region lock stays
/// outermost for a whole group, so the free-side overlap check remains atomic
/// with the retired insert. Flipping to a per-extent region lock inside a held
/// `retired` would invert that order against `validate_free_extent`.
fn region_holds<'e>(
    layout: RegionLayout,
    extents: &'e [Extent],
    cap: usize,
) -> Vec<(usize, usize, &'e [Extent])> {
    let cap = cap.max(1);
    let mut out = Vec::new();
    let mut i = 0;
    while i < extents.len() {
        let span = layout.span(extents[i]);
        let mut j = i + 1;
        while j < extents.len() && j - i < cap && layout.span(extents[j]) == span {
            j += 1;
        }
        out.push((span.0, span.1, &extents[i..j]));
        i = j;
    }
    out
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
struct RetiredShard {
    /// Authority for containment/overlap (`is_retired`,
    /// `overlapping_retired_extent`, reclaim validation). NEVER carries age.
    set: BTreeSet<Extent>,
    /// Advisory young-age log (start pba → run) holding ONLY entries younger than
    /// the reclaim grace — `aged_candidates` prunes the rest, which is the
    /// time-window that bounds its memory. Gates reclaim eligibility only: a
    /// retired sub-range is reclaimable iff no entry here covers it.
    ///
    /// ⚠ [`SpaceAllocator::aged_subranges`] treats every PRESENT entry as young
    /// without re-reading its timestamp, so the prune for a shard must complete
    /// before that shard is walked for candidates.
    age: BTreeMap<u64, RetiredRun>,
}

impl RetiredShard {
    fn covering(&self, pba: Pba) -> Option<Extent> {
        SpaceAllocator::covering_extent(&self.set, pba)
    }

    fn overlapping(&self, extent: Extent) -> Option<Extent> {
        SpaceAllocator::overlapping_extent(&self.set, extent)
    }

    /// Retire `part` (already clipped to this shard): stamp the genuinely-new
    /// sub-ranges with `now` and coalesce `part` into the set. Returns the newly
    /// retired block count (0 = idempotent re-retire).
    ///
    /// The gaps are computed BEFORE coalescing so already-retired sub-ranges keep
    /// their original age and can never be refreshed.
    fn retire(&mut self, part: Extent, now: Instant) -> u32 {
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
    fn take_for_reclaim(&mut self, part: Extent) -> bool {
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
    fn reinsert(&mut self, part: Extent) {
        SpaceAllocator::coalesce_and_insert_any_overlap(&mut self.set, part);
    }

    #[cfg(test)]
    fn blocks(&self) -> u64 {
        self.set.iter().map(|e| u64::from(e.count)).sum()
    }
}

/// Retired shards, one per PBA region — the `retired_extents` analogue of
/// [`RegionPools`]. The shard count is fixed at construction (it sizes the mutex
/// vector) and equals the region count, so `RegionLayout::of` routes both.
struct RetiredRegions {
    shards: Vec<Mutex<RetiredShard>>,
}

impl RetiredRegions {
    fn new(count: usize) -> Self {
        Self {
            shards: (0..count.max(1))
                .map(|_| Mutex::new(RetiredShard::default()))
                .collect(),
        }
    }

    fn count(&self) -> usize {
        self.shards.len()
    }
}

/// The retired shards spanned by one extent, locked in ASCENDING index order —
/// the `retired` analogue of [`SpanGuard`], with the same clipping discipline.
///
/// Lock order across the allocator is uniformly `free region -> retired shard`
/// (no path takes a retired shard and then a free region), and multi-shard holds
/// are always ascending, so neither can deadlock against the other.
struct RetiredSpan<'a> {
    layout: RegionLayout,
    lo: usize,
    guards: Vec<TimedGuard<'a, RetiredShard, RETIRED_LOCK_SITES>>,
}

impl RetiredSpan<'_> {
    fn shard(&self, idx: usize) -> &RetiredShard {
        &self.guards[idx - self.lo]
    }

    fn shard_mut(&mut self, idx: usize) -> &mut RetiredShard {
        &mut self.guards[idx - self.lo]
    }

    /// Charged once per HOLD (not per shard guard) — see
    /// [`SpanGuard::charge_items`].
    fn charge_items(&self, n: u64) {
        if let Some(first) = self.guards.first() {
            first.charge_items(n);
        }
    }

    /// First retired extent overlapping `extent`, asking exactly the shards it
    /// spans. The double-free guard on the free path.
    fn overlapping(&self, extent: Extent) -> Option<Extent> {
        let (lo, hi) = self.layout.span(extent);
        (lo..=hi).find_map(|idx| {
            let part = self.layout.clip(idx, extent)?;
            self.shard(idx).overlapping(part)
        })
    }

    /// Retire `extent` across the shards it spans; returns newly-retired blocks.
    fn retire(&mut self, extent: Extent, now: Instant) -> u32 {
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
    fn take_for_reclaim(&mut self, extent: Extent) -> Vec<Extent> {
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

    fn reinsert(&mut self, extent: Extent) {
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
    fn overlap_blocks(&self, range: Extent) -> u64 {
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
}

impl SpaceAllocator {
    /// Create a new allocator for a device of the given size.
    /// Blocks 0..RESERVED_BLOCKS are reserved for superblock/heartbeat/HA lock.
    /// Allocatable space starts at PBA RESERVED_BLOCKS.
    pub fn new(device_size_bytes: u64, num_lanes: usize) -> Self {
        Self::new_with_hazards(device_size_bytes, num_lanes)
    }

    /// Number of address regions the free space is sharded into (1 = off).
    pub fn region_count(&self) -> usize {
        self.regions.count()
    }

    /// Blocks per region (0 when unsharded).
    pub fn region_blocks(&self) -> u64 {
        self.regions.region_blocks.load(Ordering::Relaxed)
    }

    /// Region sharding shape + traffic — see [`AllocRegionStats`].
    pub fn region_stats(&self) -> AllocRegionStats {
        AllocRegionStats {
            regions: self.regions.count(),
            region_blocks: self.regions.region_blocks.load(Ordering::Relaxed),
            switches: self.region_switches.load(Ordering::Relaxed),
            refill_misses: self.region_refill_misses.load(Ordering::Relaxed),
            serialized: region_serialize(),
        }
    }

    /// `device_size_bytes` is the IO-ADDRESSABLE capacity, NOT the raw device
    /// size: production callers pass `device.size() - RESERVED_BLOCKS *
    /// BLOCK_SIZE` (see `OnyxEngine`), because the `IoEngine` translates
    /// allocator PBA `p` to device block `p + RESERVED_BLOCKS` (its
    /// `pba_offset`). Passing the raw device size here would let the top
    /// RESERVED_BLOCKS PBAs write past the device end (chunklet "IO out of
    /// range: offset == capacity"). The bottom RESERVED_BLOCKS reserved below is
    /// the superblock / heartbeat / HA-lock region in the allocator's own space.
    pub fn new_with_hazards(device_size_bytes: u64, num_lanes: usize) -> Self {
        // Unsharded. NOT the production default (that is
        // `storage.allocator_regions`, which selects 2048 — see `StorageConfig`);
        // this is the direct-construction entry point used by the unit tests, and
        // keeping it single-lock is what lets a test compare the two arms and what
        // makes `ONYX_ALLOCATOR_REGIONS=<n> cargo test` a meaningful sweep.
        Self::new_with_regions(device_size_bytes, num_lanes, 1)
    }

    /// `new_with_hazards` with an explicit region count (see [`RegionPools`]).
    /// `0` selects the compiled default, `1` disables sharding. The device may
    /// still end up unsharded when it is too small (see [`MIN_REGION_BLOCKS`]).
    pub fn new_with_regions(device_size_bytes: u64, num_lanes: usize, regions: usize) -> Self {
        // Diagnostic override, same shape as `ONYX_ALLOC_TRACK`: it exists so the
        // WHOLE suite can be re-run against the sharded paths
        // (`ONYX_ALLOCATOR_REGIONS=8 cargo test`) instead of only the dedicated
        // region tests, which is the only way to find a routing mistake in a
        // consumer nobody thought to region-test. It overrides the config, so
        // production must not set it.
        let regions = std::env::var("ONYX_ALLOCATOR_REGIONS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(regions);
        Self::new_with_exact_regions(device_size_bytes, num_lanes, regions)
    }

    /// [`Self::new_with_regions`] without the `ONYX_ALLOCATOR_REGIONS` override,
    /// so a test that compares a sharded pool against an unsharded one still
    /// gets both arms while the suite is being swept sharded.
    fn new_with_exact_regions(device_size_bytes: u64, num_lanes: usize, regions: usize) -> Self {
        let total_blocks = device_size_bytes / BLOCK_SIZE as u64;
        let usable_blocks = total_blocks.saturating_sub(RESERVED_BLOCKS);
        let regions = if regions == 0 {
            DEFAULT_ALLOCATOR_REGIONS
        } else {
            regions
        };
        let region_pools = RegionPools::new(usable_blocks, regions);
        if usable_blocks > 0 {
            // Same total as the pre-region constructor (which clamped one
            // extent to u32::MAX), just routed to the owning regions.
            let layout = region_pools.layout();
            let seed = Extent::new(
                Pba(RESERVED_BLOCKS),
                usable_blocks.min(u32::MAX as u64) as u32,
            );
            let (lo, hi) = layout.span(seed);
            for idx in lo..=hi {
                if let Some(part) = layout.clip(idx, seed) {
                    let mut pools = region_pools.pools[idx].lock().unwrap();
                    pools.general.insert(part);
                    // Seed the advisory hints too: the ascending region walks
                    // skip regions whose hint reads zero, so a never-published
                    // hint would make a freshly-built allocator look empty.
                    region_pools.free_hint[idx]
                        .store(pools.free_blocks_in_pools(), Ordering::Relaxed);
                }
            }
        }
        let lane_caches = (0..num_lanes).map(|_| Mutex::new(Vec::new())).collect();
        let lane_extent_caches = (0..num_lanes).map(|_| Mutex::new(Vec::new())).collect();
        let lane_cache_depth = (0..num_lanes).map(|_| AtomicU64::new(0)).collect();
        let lane_extent_cache_depth = (0..num_lanes).map(|_| AtomicU64::new(0)).collect();
        let lane_regions = (0..num_lanes).map(|_| AtomicUsize::new(usize::MAX)).collect();
        let alloc_tracker = std::env::var("ONYX_ALLOC_TRACK")
            .map(|value| {
                matches!(
                    value.as_str(),
                    "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
                )
            })
            .unwrap_or(false)
            .then(|| Mutex::new(BTreeSet::new()));
        let retired = RetiredRegions::new(region_pools.count());
        Self {
            total_blocks: AtomicU64::new(total_blocks),
            regions: region_pools,
            retired,
            retired_blocks: AtomicU64::new(0),
            hazards: PbaHazards::new(),
            allocated_blocks: AtomicU64::new(0),
            free_blocks: AtomicU64::new(usable_blocks),
            free_lock: FreeLockStats::new(),
            retired_lock: RetiredLockStats::new(),
            lane_caches,
            lane_extent_caches,
            lane_cache_depth,
            lane_extent_cache_depth,
            lane_regions,
            alloc_tracker,
            aligned_allocs: AtomicU64::new(0),
            refill_ops: AtomicU64::new(0),
            refill_blocks: AtomicU64::new(0),
            refill_runs: AtomicU64::new(0),
            drain_ops: AtomicU64::new(0),
            drain_blocks: AtomicU64::new(0),
            drain_skips: AtomicU64::new(0),
            #[cfg(test)]
            drain_guard_off: AtomicBool::new(false),
            region_switches: AtomicU64::new(0),
            region_refill_misses: AtomicU64::new(0),
            stripe_refill_run_stripes: AtomicU64::new(0),
            refill_wide_hits: AtomicU64::new(0),
            refill_wide_misses: AtomicU64::new(0),
            geometry_cache: AtomicU64::new(0),
        }
    }

    /// Set the aligned refill's preferred run width in whole stripes (`0` = off).
    /// See [`crate::config::StorageConfig::stripe_refill_run_stripes`]; the engine
    /// sets this once at open, next to [`Self::set_stripe_geometry`].
    pub fn set_stripe_refill_run_stripes(&self, stripes: u32) {
        self.stripe_refill_run_stripes
            .store(u64::from(stripes), Ordering::Relaxed);
    }

    /// The configured preferred run width in whole stripes (`0` = off).
    pub fn stripe_refill_run_stripes(&self) -> u32 {
        self.stripe_refill_run_stripes.load(Ordering::Relaxed) as u32
    }

    /// Block floor a refill prefers its reserve runs to meet, or `None` when the
    /// knob is off or the floor would not be wider than the request itself.
    ///
    /// Clamped to `max_count` (the refill's block budget): asking for a run wider
    /// than we are willing to take would reject runs that could serve the whole
    /// budget contiguously, which is the entire point.
    fn wide_refill_floor(&self, min_count: u32, max_count: u32, stripe: u32) -> Option<u32> {
        let stripes = self.stripe_refill_run_stripes();
        if stripes == 0 {
            return None;
        }
        let floor = stripe
            .saturating_mul(stripes)
            .min(max_count.max(min_count));
        (floor > min_count).then_some(floor)
    }

    /// Snapshot of the aligned path's lane-cache supply — see
    /// [`AllocSupplyStats`]. Lock-free; monotonic counters, so two reads
    /// difference cleanly.
    /// Acquire `free_pools`, charging the wait to `site` and (on drop) the hold.
    ///
    /// This is THE hot path of the whole allocator — ~10^4 acquisitions per
    /// unaligned allocation in the exhausted regime — so the accounting is
    /// deliberately minimal: one TLS read, one `fetch_add` on a per-thread cache
    /// line, and (on 1 acquisition in [`lock_stat_stride`]) two clock reads.
    fn lock_region_raw(&self, site: FreeLockSite, region: usize) -> FreeLockGuard<'_> {
        let idx = site as usize;
        let (shard, queued) = self.free_lock.begin(idx);
        let pools = self.regions.pools[region].lock().unwrap();
        let acquired = queued.map(|queued| {
            let now = Instant::now();
            shard.charge_wait(idx, now.duration_since(queued).as_nanos() as u64);
            now
        });
        FreeLockGuard {
            pools,
            shard,
            site: idx,
            acquired,
            free_hint: &self.regions.free_hint[region],
            stripe_hint: &self.regions.stripe_hint[region],
        }
    }

    /// Lock ONE region (plus the A/B gate when armed). Callers that walk regions
    /// take these one at a time and never hold two, so the walk order is free.
    fn lock_region(&self, site: FreeLockSite, region: usize) -> SpanGuard<'_> {
        let gate = self.regions.gate();
        SpanGuard {
            layout: self.regions.layout(),
            lo: region,
            guards: vec![self.lock_region_raw(site, region)],
            _gate: gate,
        }
    }

    /// Lock every region this extent reaches into, ASCENDING by index.
    ///
    /// Ascending order is the allocator-wide rule for holding more than one
    /// region, so multi-region holds can never deadlock against each other.
    /// The overwhelmingly common result is a single guard.
    fn lock_span(&self, site: FreeLockSite, extent: Extent) -> SpanGuard<'_> {
        let layout = self.regions.layout();
        let (lo, hi) = layout.span(extent);
        let gate = self.regions.gate();
        SpanGuard {
            layout,
            lo,
            guards: (lo..=hi)
                .map(|idx| self.lock_region_raw(site, idx))
                .collect(),
            _gate: gate,
        }
    }

    /// Lock the inclusive region range `[lo, hi]`, ASCENDING — the batch-path
    /// analogue of [`Self::lock_span`], where the range comes from a group of
    /// extents rather than one.
    fn lock_span_range(&self, site: FreeLockSite, lo: usize, hi: usize) -> SpanGuard<'_> {
        let gate = self.regions.gate();
        SpanGuard {
            layout: self.regions.layout(),
            lo,
            guards: (lo..=hi)
                .map(|idx| self.lock_region_raw(site, idx))
                .collect(),
            _gate: gate,
        }
    }

    /// Lock EVERY region ascending. Only for paths that must see the whole
    /// space atomically — today just `drain_lane_caches`, which folds lane
    /// caches back and therefore has to hold the region locks across the lane
    /// locks to keep the `FreePools -> lane cache` order.
    fn lock_all_regions(&self, site: FreeLockSite) -> SpanGuard<'_> {
        let layout = self.regions.layout();
        let gate = self.regions.gate();
        SpanGuard {
            layout,
            lo: 0,
            guards: (0..layout.count)
                .map(|idx| self.lock_region_raw(site, idx))
                .collect(),
            _gate: gate,
        }
    }

    /// Regions in ascending address order, optionally skipping those the
    /// advisory hints report empty.
    ///
    /// Selection identity: regions partition the address space in ascending
    /// order, so "the first region that can serve the request" IS the global
    /// lowest-address answer — the same extent the single global pool's
    /// first-fit would have returned. Skipping hint-empty regions cannot change
    /// that (a region with no free blocks has no candidate).
    ///
    /// `min_hint = 0` forces the full walk. Every ENOSPC boundary uses it on its
    /// final attempt, so a hint that went stale exactly while another thread was
    /// freeing can never turn into a spurious `SpaceExhausted`. Against
    /// `stripe_hint` a `min_hint` of the request width is exact (largest-run
    /// semantics); against `free_hint` only `1` is meaningful, because a summed
    /// total says nothing about run widths.
    fn walk_regions(
        &self,
        need_stripe: bool,
        min_hint: u64,
    ) -> impl Iterator<Item = usize> + '_ {
        let hints = if need_stripe {
            &self.regions.stripe_hint
        } else {
            &self.regions.free_hint
        };
        let count = self.regions.count();
        (0..count).filter(move |&idx| {
            min_hint == 0 || count == 1 || hints[idx].load(Ordering::Relaxed) >= min_hint
        })
    }

    /// Test-only direct handle on ONE region's pools, for the tests that inject
    /// a specific free-list shape or a deliberate free/retired inconsistency.
    /// Goes through the real guard so the region's advisory hints are refreshed
    /// on release exactly as a production mutation would refresh them.
    #[cfg(test)]
    fn test_region_pools(&self, region: usize) -> FreeLockGuard<'_> {
        self.lock_region_raw(FreeLockSite::Setup, region)
    }

    /// Per-site `free_pools` wait/hold snapshot, one entry per
    /// [`FreeLockSite`] (including never-acquired sites, so the shape is stable
    /// across two reads for differencing).
    pub fn free_lock_stats(&self) -> Vec<LockSiteStats> {
        self.free_lock.snapshot(FreeLockSite::ALL.map(|s| s.name()))
    }

    /// Per-site `retired_extents` wait/hold snapshot — see [`RetiredLockSite`].
    /// Same shape guarantee as [`Self::free_lock_stats`].
    pub fn retired_lock_stats(&self) -> Vec<LockSiteStats> {
        self.retired_lock
            .snapshot(RetiredLockSite::ALL.map(|s| s.name()))
    }

    /// Acquire ONE retired shard, charging the wait to `site` and the hold on
    /// drop. Every acquisition in the file goes through here (or the span
    /// helpers below), so the table is exhaustive by construction.
    fn lock_retired_shard(
        &self,
        site: RetiredLockSite,
        idx: usize,
    ) -> TimedGuard<'_, RetiredShard, RETIRED_LOCK_SITES> {
        TimedGuard::new(&self.retired_lock, site as usize, &self.retired.shards[idx])
    }

    /// The retired shards' layout — ALWAYS the free pool's, so that
    /// `{pools[i], retired[i]}` is exactly the atomic unit the retire path needs.
    fn retired_layout(&self) -> RegionLayout {
        self.regions.layout()
    }

    /// Lock every retired shard `extent` reaches into, ASCENDING.
    fn lock_retired_span(&self, site: RetiredLockSite, extent: Extent) -> RetiredSpan<'_> {
        let (lo, hi) = self.retired_layout().span(extent);
        self.lock_retired_span_range(site, lo, hi)
    }

    /// Lock EVERY retired shard, ASCENDING. Only for paths that must see the
    /// whole retired space atomically — today just the geometry re-shard.
    fn lock_all_retired(
        &self,
        site: RetiredLockSite,
    ) -> Vec<TimedGuard<'_, RetiredShard, RETIRED_LOCK_SITES>> {
        (0..self.retired.count())
            .map(|idx| self.lock_retired_shard(site, idx))
            .collect()
    }

    /// Lock the inclusive shard range `[lo, hi]`, ASCENDING — the batch-path
    /// analogue of [`Self::lock_retired_span`], where the range comes from a
    /// group of extents rather than from one.
    fn lock_retired_span_range(
        &self,
        site: RetiredLockSite,
        lo: usize,
        hi: usize,
    ) -> RetiredSpan<'_> {
        RetiredSpan {
            layout: self.retired_layout(),
            lo,
            guards: (lo..=hi)
                .map(|idx| self.lock_retired_shard(site, idx))
                .collect(),
        }
    }

    pub fn supply_stats(&self) -> AllocSupplyStats {
        AllocSupplyStats {
            aligned_allocs: self.aligned_allocs.load(Ordering::Relaxed),
            refills: self.refill_ops.load(Ordering::Relaxed),
            refill_blocks: self.refill_blocks.load(Ordering::Relaxed),
            refill_runs: self.refill_runs.load(Ordering::Relaxed),
            drains: self.drain_ops.load(Ordering::Relaxed),
            drain_blocks: self.drain_blocks.load(Ordering::Relaxed),
            drain_skips: self.drain_skips.load(Ordering::Relaxed),
            wide_hits: self.refill_wide_hits.load(Ordering::Relaxed),
            wide_misses: self.refill_wide_misses.load(Ordering::Relaxed),
        }
    }

    /// Rebuild the free list from MetaStore metadata.
    /// Blockmap is the source of truth so multi-block compression units reserve
    /// all occupied PBAs, not just the starting block.
    /// PBAs below RESERVED_BLOCKS are excluded (reserved for superblock/HA).
    pub fn rebuild_from_metadata(&self, meta: &MetaStore) -> OnyxResult<()> {
        // Collect all allocated PBAs into a sorted vec, filtering out reserved region
        let mut allocated: Vec<u64> = meta
            .iter_allocated_blocks()?
            .into_iter()
            .map(|pba| pba.0)
            .filter(|&pba| pba >= RESERVED_BLOCKS)
            .collect();
        allocated.sort_unstable();

        // Build free extents from gaps (starting at RESERVED_BLOCKS)
        let mut free = BTreeSet::new();
        let mut pos: u64 = RESERVED_BLOCKS;

        for &alloc_pba in &allocated {
            if alloc_pba > pos {
                let gap = alloc_pba - pos;
                // Split into u32-sized extents if needed
                let mut start = pos;
                let mut remaining = gap;
                while remaining > 0 {
                    let count = remaining.min(u32::MAX as u64) as u32;
                    free.insert(Extent::new(Pba(start), count));
                    start += count as u64;
                    remaining -= count as u64;
                }
            }
            pos = alloc_pba + 1;
        }

        // Trailing free space
        if pos < self.total_blocks.load(Ordering::Relaxed) {
            let gap = self.total_blocks.load(Ordering::Relaxed) - pos;
            let mut start = pos;
            let mut remaining = gap;
            while remaining > 0 {
                let count = remaining.min(u32::MAX as u64) as u32;
                free.insert(Extent::new(Pba(start), count));
                start += count as u64;
                remaining -= count as u64;
            }
        }

        let usable_blocks = self
            .total_blocks
            .load(Ordering::Relaxed)
            .saturating_sub(RESERVED_BLOCKS);
        let alloc_count = allocated.len() as u64;
        let free_count = usable_blocks - alloc_count;

        self.replace_general_regionwise(&free);
        for idx in 0..self.retired.count() {
            let mut shard = self.lock_retired_shard(RetiredLockSite::Setup, idx);
            shard.set.clear();
            shard.age.clear();
        }
        self.retired_blocks.store(0, Ordering::Relaxed);
        self.clear_lane_caches();
        if let Some(tracker) = &self.alloc_tracker {
            let mut tracker = tracker.lock().unwrap();
            tracker.clear();
            for &pba in &allocated {
                tracker.insert(Pba(pba));
            }
        }
        self.allocated_blocks.store(alloc_count, Ordering::Relaxed);
        self.free_blocks.store(free_count, Ordering::Relaxed);

        tracing::info!(
            total = self.total_blocks.load(Ordering::Relaxed),
            allocated = alloc_count,
            free = free_count,
            extents = self.pool_extent_count(),
            regions = self.regions.count(),
            "space allocator rebuilt from metadata"
        );

        Ok(())
    }

    /// Reset every region to hold exactly the free runs in `free`, clipped to the
    /// region that owns each piece. Regions are visited ascending and locked ONE
    /// AT A TIME: this is the startup/rebuild path, and `free` can hold tens of
    /// millions of extents, so holding all region locks across it would be a
    /// long global stall for no benefit (no allocator client is running yet).
    fn replace_general_regionwise(&self, free: &BTreeSet<Extent>) {
        let layout = self.regions.layout();
        let mut runs = free.iter().peekable();
        for idx in 0..layout.count {
            let mut guard = self.lock_region(FreeLockSite::Setup, idx);
            let pools = guard.region_mut(idx);
            pools.replace_general(BTreeSet::new());
            let end = layout.end(idx);
            while let Some(&&run) = runs.peek() {
                if run.start.0 >= end {
                    break;
                }
                if let Some(part) = layout.clip(idx, run) {
                    pools.insert_classified(part);
                }
                if run.end_pba().0 <= end {
                    runs.next();
                } else {
                    // Straddles the boundary — the remainder belongs to the next
                    // region, so leave it in place for the next iteration.
                    break;
                }
            }
        }
    }

    /// Free extents across all regions (general + reserve). One region lock at a
    /// time — advisory aggregate, never a decision input.
    fn pool_extent_count(&self) -> usize {
        (0..self.regions.count())
            .map(|idx| {
                let guard = self.lock_region(FreeLockSite::Audit, idx);
                let pools = guard.region(idx);
                pools.general.len() + pools.stripe_reserve.len()
            })
            .sum()
    }

    pub fn hazards(&self) -> PbaHazards {
        self.hazards.clone()
    }

    /// Configure the engine's fixed LV3 RAID geometry so stripe-aligned
    /// first-fit queries use the effective-capacity index instead of a
    /// slack-check scan (65 ms/call on a 3M-fragment belt, inside the global
    /// free lock — the 2026-07-03 throughput-oscillation root cause). Call
    /// once at startup before flush traffic; idempotent; `stripe <= 1`
    /// (non-RAID backends) clears it.
    /// Also the point where the region boundaries are finalized: they must be
    /// stripe-aligned, and the stripe is not known when the allocator is built.
    /// Every region lock is held across the re-layout, and the regions are
    /// emptied before the new boundaries are published, so no extent can be left
    /// sitting in a region that no longer owns its address.
    pub fn set_stripe_geometry(&self, stripe_blocks: u32, phase: u32) {
        let requested = (stripe_blocks > 1).then_some((stripe_blocks, phase));
        let planned = self.regions.planned_layout(stripe_blocks, phase);
        let mut guard = self.lock_all_regions(FreeLockSite::Setup);
        if guard.geometry() == requested && guard.layout == planned {
            return;
        }
        let mut runs = Vec::new();
        for region in &mut guard.guards {
            runs.extend(region.take_all_runs());
        }
        // The retired shards route on the SAME layout, so moving the boundaries
        // has to re-shard them too — an extent left behind under the old
        // boundaries could otherwise sit in a shard that does not own it, which
        // breaks every containment query. Every shard lock is held from drain to
        // re-insert (lock order stays free -> retired), so no reader can observe
        // the emptied window. Normally a no-op: geometry is configured at startup,
        // before anything has been retired.
        let mut shards = self.lock_all_retired(RetiredLockSite::Setup);
        let mut retired_runs = Vec::new();
        let mut retired_age = Vec::new();
        for shard in &mut shards {
            retired_runs.extend(std::mem::take(&mut shard.set));
            retired_age.extend(std::mem::take(&mut shard.age));
        }
        self.regions.publish_layout(planned);
        guard.layout = planned;
        for region in &mut guard.guards {
            region.reset_geometry(stripe_blocks, phase);
        }
        runs.sort_unstable_by_key(|extent| extent.start.0);
        for run in runs {
            guard.insert_classified(run);
        }
        retired_runs.sort_unstable_by_key(|extent| extent.start.0);
        for run in retired_runs {
            let (lo, hi) = planned.span(run);
            for idx in lo..=hi {
                if let Some(part) = planned.clip(idx, run) {
                    shards[idx].reinsert(part);
                }
            }
        }
        for (start, run) in retired_age {
            shards[planned.of(start)].age.insert(start, run);
        }
        drop(shards);
        drop(guard);
        self.geometry_cache.store(
            requested.map_or(0, |(stripe, phase)| {
                u64::from(stripe) << 32 | u64::from(phase)
            }),
            Ordering::Relaxed,
        );
    }

    /// Remove an aligned physical range from ordinary allocation while the
    /// defragger evacuates its live pinners. Existing free pieces are moved into
    /// the target atomically; after publication, wait for pre-existing PBA pins
    /// without holding the allocator lock.
    pub fn begin_defrag_quarantine(&self, target: Extent) -> OnyxResult<()> {
        self.validate_extent_shape(target, "begin_defrag_quarantine")?;
        {
            let mut span = self.lock_span(FreeLockSite::Quarantine, target);
            let (stripe, phase) = span.geometry().ok_or_else(|| {
                OnyxError::Config("begin_defrag_quarantine requires stripe geometry".into())
            })?;
            if stripe <= 1
                || target.count % stripe != 0
                || (target.start.0 + phase as u64) % stripe as u64 != 0
            {
                return Err(OnyxError::Config(format!(
                    "defrag quarantine {:?} is not aligned to stripe={} phase={}",
                    target, stripe, phase
                )));
            }
            // A quarantine is tracked in exactly ONE region so that
            // progress/complete/cancel stay single-lock lookups keyed by the
            // target's start PBA. Region boundaries are stripe-aligned and every
            // real defrag target is exactly one stripe
            // (`GcDefragState::qualify_and_emit`), so this is unreachable in
            // production — rejecting here is still better than silently
            // splitting one target's completion accounting across two locks.
            // Checked BEFORE anything is extracted so there is nothing to undo.
            if !span.spans_one_region() {
                return Err(OnyxError::Config(format!(
                    "defrag quarantine {:?} crosses an allocator region boundary",
                    target
                )));
            }
            if let Some(existing) = span.overlapping_quarantine(target) {
                return Err(OnyxError::Config(format!(
                    "defrag quarantine {:?} overlaps target {:?}",
                    target, existing
                )));
            }
            if span.overlaps_reserve(target) {
                return Err(OnyxError::Config(format!(
                    "defrag quarantine {:?} overlaps stripe reserve",
                    target
                )));
            }
            // Lock order is FreePools -> lane caches. Allocation fast paths
            // never hold a lane lock while acquiring FreePools. Cached blocks
            // are logically free, so detach only the target intersections and
            // leave any head/tail pieces in their originating lane.
            let mut free_parts = span.extract_from_general(target);
            free_parts.extend(self.extract_lane_cache_free_parts(target));
            let mut target_free = span.empty_set_with_geometry();
            for extent in free_parts {
                target_free.coalesce_insert(extent);
            }
            let pools = span
                .single_mut()
                .expect("cross-region target rejected above");
            pools.quarantines.insert(
                target.start.0,
                QuarantineTarget {
                    range: target,
                    free_parts: target_free,
                },
            );
        }

        self.hazards.wait_extent_clear(target.start, target.count);
        Ok(())
    }

    pub fn defrag_quarantine_progress(&self, start: Pba) -> Option<(u64, u64)> {
        let idx = self.regions.layout().of(start.0);
        let guard = self.lock_region(FreeLockSite::Quarantine, idx);
        let target = guard.region(idx).quarantines.get(&start.0)?;
        Some((target.free_parts.blocks_total(), target.range.count as u64))
    }

    /// Publish a fully-free quarantine as stripe reserve. A partially-free
    /// target remains active and returns `Ok(false)`.
    ///
    /// ⚠ This is the ONLY place that inserts a whole stripe-aligned window into
    /// the allocatable pool in one shot, without having verified each block
    /// individually — every other insert path only ever returns blocks a caller
    /// just proved dead. That makes its gate load-bearing for data integrity:
    /// publishing a window that still holds a LIVE block hands that block to the
    /// next writer, which overwrites it while its L2P mapping is intact, and the
    /// reader gets `CRC mismatch` on an LBA that was never touched. Box forensics
    /// 2026-08-12: 476 double-claimed blocks, and all 126 of their consecutive
    /// runs sat inside ONE stripe-aligned window each (1–5 of 6 blocks, never a
    /// whole stripe) — the fingerprint of exactly this publish.
    ///
    /// So the gate is STRUCTURAL, not a block count. `free_parts` is built only
    /// through `coalesce_insert`, so a genuinely-complete window is one folded
    /// extent equal to `range`; requiring that cannot be satisfied by a drifted
    /// aggregate. A `blocks_total` that claims completeness while the set does
    /// not is an upstream accounting bug: cancel the target (which returns only
    /// the pieces that really are free) instead of publishing, and say so.
    pub fn complete_defrag_quarantine(&self, start: Pba) -> OnyxResult<bool> {
        let idx = self.regions.layout().of(start.0);
        let mut guard = self.lock_region(FreeLockSite::Quarantine, idx);
        let pools = guard.region_mut(idx);
        let Some(target) = pools.quarantines.get(&start.0) else {
            return Ok(false);
        };
        let range = target.range;
        if !target.free_parts.is_exactly(range) {
            if target.free_parts.blocks_total() >= range.count as u64 {
                tracing::error!(
                    start = start.0,
                    blocks = range.count,
                    free_blocks = target.free_parts.blocks_total(),
                    free_extents = target.free_parts.by_addr().len(),
                    "defrag quarantine reports itself complete but its free parts do not \
                     cover the window — refusing to publish, cancelling instead"
                );
                drop(guard);
                self.cancel_defrag_quarantine(start);
            }
            return Ok(false);
        }
        let target = pools
            .quarantines
            .remove(&start.0)
            .expect("target checked above");
        // The target lives inside one region (enforced at begin), so publishing
        // it back needs no cross-region split.
        pools.insert_classified(target.range);
        Ok(true)
    }

    /// Abandon an active quarantine and return only its already-free pieces to
    /// the canonical general/reserve partition. Live/retired pieces were never
    /// removed from their ownership states.
    pub fn cancel_defrag_quarantine(&self, start: Pba) -> bool {
        let idx = self.regions.layout().of(start.0);
        let mut guard = self.lock_region(FreeLockSite::Quarantine, idx);
        let pools = guard.region_mut(idx);
        let Some(target) = pools.quarantines.remove(&start.0) else {
            return false;
        };
        let free_parts: Vec<Extent> = target.free_parts.by_addr().iter().copied().collect();
        for extent in free_parts {
            pools.insert_classified(extent);
        }
        true
    }

    /// Test-only: build an allocator with the live-PBA duplicate-allocation
    /// tracker armed regardless of `ONYX_ALLOC_TRACK`, so a stress test does not
    /// have to mutate process-global env.
    #[cfg(test)]
    pub(crate) fn new_tracked(device_size_bytes: u64, num_lanes: usize, regions: usize) -> Self {
        let mut me = Self::new_with_regions(device_size_bytes, num_lanes, regions);
        me.alloc_tracker = Some(Mutex::new(BTreeSet::new()));
        me
    }

    /// Test-only: every free set the allocator owns, checked against its own
    /// invariants (index agreement, aggregate totals, disjointness).
    #[cfg(test)]
    pub(crate) fn assert_free_sets_consistent(&self) {
        for idx in 0..self.regions.count() {
            let guard = self.lock_region(FreeLockSite::Setup, idx);
            let pools = guard.region(idx);
            pools.general.assert_consistent();
            pools.stripe_reserve.assert_consistent();
            for target in pools.quarantines.values() {
                target.free_parts.assert_consistent();
            }
        }
    }

    /// Test-only: snapshot of the live-PBA tracker.
    #[cfg(test)]
    pub(crate) fn tracked_live_pbas(&self) -> BTreeSet<Pba> {
        self.alloc_tracker
            .as_ref()
            .expect("tracker armed")
            .lock()
            .unwrap()
            .clone()
    }

    /// Test-only: add an extent to an active quarantine's free-parts set without
    /// going through `release_extent`, so a test can model the accounting drift
    /// the structural completion gate exists to catch — a `blocks_total` that
    /// reaches the window size while the set does not actually cover it.
    #[cfg(test)]
    pub(crate) fn inject_quarantine_free_part_for_test(&self, start: Pba, extent: Extent) {
        let idx = self.regions.layout().of(start.0);
        let mut guard = self.lock_region(FreeLockSite::Quarantine, idx);
        let target = guard
            .region_mut(idx)
            .quarantines
            .get_mut(&start.0)
            .expect("quarantine target exists");
        target.free_parts.insert_for_test(extent);
    }

    pub fn is_defrag_quarantined(&self, extent: Extent) -> bool {
        self.lock_span(FreeLockSite::Quarantine, extent)
            .overlapping_quarantine(extent)
            .is_some()
    }

    /// Atomically reject new dedup pins after a quarantine is published. A pin
    /// that wins the race before publication is waited out by
    /// `begin_defrag_quarantine` after it drops the allocator lock.
    pub fn pin_dedup_target_if_allowed(&self, start: Pba, count: u32) -> Option<PbaHazardGuard> {
        let end = start.0.checked_add(count as u64)?;
        if count == 0 || end > self.total_blocks.load(Ordering::Acquire) {
            return None;
        }
        let extent = Extent::new(start, count);
        let span = self.lock_span(FreeLockSite::Quarantine, extent);
        if span.overlapping_quarantine(extent).is_some() {
            return None;
        }
        Some(
            self.hazards
                .pin_many((0..count).map(|offset| Pba(start.0 + offset as u64))),
        )
    }

    /// Wait until no in-flight reader currently pins this physical extent.
    ///
    /// Allocator free waits protect the hand-off back to the free list. Writers
    /// also call this after allocation and before overwriting the physical
    /// blocks, because a reader may have pinned a just-freed PBA after it was
    /// reallocated but before the new payload is written.
    pub fn wait_for_readers(&self, start: Pba, count: u32) {
        self.hazards.wait_extent_clear(start, count);
    }

    fn track_alloc(&self, extent: Extent, context: &'static str) -> OnyxResult<()> {
        crate::space::free_trace::trace_alloc(extent, context);
        let Some(tracker) = &self.alloc_tracker else {
            return Ok(());
        };
        let mut tracker = tracker.lock().unwrap();
        for offset in 0..extent.count {
            let pba = Pba(extent.start.0 + offset as u64);
            if !tracker.insert(pba) {
                tracing::error!(
                    pba = pba.0,
                    start = extent.start.0,
                    blocks = extent.count,
                    context,
                    "allocator live-PBA tracker detected duplicate allocation"
                );
                return Err(OnyxError::Config(format!(
                    "allocator duplicate allocation pba={} context={context}",
                    pba.0
                )));
            }
        }
        Ok(())
    }

    fn track_release(&self, extent: Extent, context: &'static str) {
        let Some(tracker) = &self.alloc_tracker else {
            return;
        };
        let mut tracker = tracker.lock().unwrap();
        for offset in 0..extent.count {
            let pba = Pba(extent.start.0 + offset as u64);
            if !tracker.remove(&pba) {
                tracing::warn!(
                    pba = pba.0,
                    start = extent.start.0,
                    blocks = extent.count,
                    context,
                    "allocator live-PBA tracker released a non-live PBA"
                );
            }
        }
    }

    /// Allocate a single block. Returns PBA.
    pub fn allocate_one(&self) -> OnyxResult<Pba> {
        if let Some(pba) = self.take_first_regionwise(FreeLockSite::SmallAlloc, 1, true) {
            self.track_alloc(Extent::single(pba.start), "allocate_one")?;
            self.allocated_blocks.fetch_add(1, Ordering::Relaxed);
            self.free_blocks.fetch_sub(1, Ordering::Relaxed);
            return Ok(pba.start);
        }
        // Global pool empty — drain lane caches and retry. The retry walks EVERY
        // region (no hint skipping) so this ENOSPC verdict never rests on a
        // stale advisory hint.
        self.drain_lane_caches_if_populated();
        if let Some(pba) = self.take_first_regionwise(FreeLockSite::SmallAlloc, 1, false) {
            self.track_alloc(Extent::single(pba.start), "allocate_one_retry")?;
            self.allocated_blocks.fetch_add(1, Ordering::Relaxed);
            self.free_blocks.fetch_sub(1, Ordering::Relaxed);
            return Ok(pba.start);
        }
        Err(OnyxError::SpaceExhausted)
    }

    /// Lowest-address free extent anywhere, capped at `max_count`.
    ///
    /// Small allocations preserve allocator-wide first-fit-by-address across both
    /// policy pools AND across regions — regions are address-ordered and
    /// disjoint, so the first region that has anything holds the global argmin.
    /// This ordering is a MetaDB L2P codec correctness contract; the reserve
    /// controls aligned ownership, never address order.
    fn take_first_regionwise(
        &self,
        site: FreeLockSite,
        max_count: u32,
        skip_empty: bool,
    ) -> Option<Extent> {
        for idx in self.walk_regions(false, u64::from(skip_empty)) {
            let mut guard = self.lock_region(site, idx);
            if let Some(extent) = Self::take_first_from_pools(guard.region_mut(idx), max_count) {
                return Some(extent);
            }
        }
        None
    }

    /// Lowest-address free extent that can serve `min_count`, capped at
    /// `max_count`. Same ascending-region argument as
    /// [`Self::take_first_regionwise`].
    fn take_exact_regionwise(
        &self,
        site: FreeLockSite,
        min_count: u32,
        max_count: u32,
        skip_empty: bool,
    ) -> Option<Extent> {
        for idx in self.walk_regions(false, u64::from(skip_empty)) {
            let mut guard = self.lock_region(site, idx);
            if let Some(extent) =
                Self::take_exact_from_pools(guard.region_mut(idx), min_count, max_count)
            {
                return Some(extent);
            }
        }
        None
    }

    /// Largest free extent anywhere — the `allocate_extent` short-fragment
    /// fallback.
    ///
    /// Two phases so no more than one region lock is ever held: read each
    /// region's `largest()`, then re-take the winner under its own lock. A
    /// concurrent allocation can empty the winner between the phases, so a region
    /// that comes back empty is dropped and the scan repeats — bounded by the
    /// region count, and only reachable while another thread is allocating (in
    /// which case an eventual `None` is a truthful "nothing left").
    fn take_largest_regionwise(&self, site: FreeLockSite) -> Option<Extent> {
        let mut exhausted: Vec<usize> = Vec::new();
        for _ in 0..=self.regions.count() {
            let mut best: Option<(u32, u64, usize)> = None;
            for idx in self.walk_regions(false, 1) {
                if exhausted.contains(&idx) {
                    continue;
                }
                let guard = self.lock_region(site, idx);
                let pools = guard.region(idx);
                let candidate = match (pools.general.largest(), pools.stripe_reserve.largest()) {
                    (Some(g), Some(r)) if (r.count, r.start.0) > (g.count, g.start.0) => Some(r),
                    (Some(g), Some(_)) => Some(g),
                    (Some(g), None) => Some(g),
                    (None, r) => r,
                };
                if let Some(candidate) = candidate {
                    let key = (candidate.count, candidate.start.0, idx);
                    if best.is_none_or(|current| key > current) {
                        best = Some(key);
                    }
                }
            }
            let (_, _, idx) = best?;
            let mut guard = self.lock_region(site, idx);
            if let Some(extent) = Self::take_largest_from_pools(guard.region_mut(idx)) {
                return Some(extent);
            }
            drop(guard);
            exhausted.push(idx);
        }
        None
    }

    /// Allocate a single block using the per-lane cache to avoid global lock contention.
    /// Falls back to global allocation with bulk refill when the cache is empty.
    pub fn allocate_one_for_lane(&self, lane: usize) -> OnyxResult<Pba> {
        if lane >= self.lane_caches.len() {
            return self.allocate_one();
        }
        // Fast path: pop from lane cache (no global lock)
        {
            let mut cache = self.lane_caches[lane].lock().unwrap();
            if let Some(pba) = cache.pop() {
                self.publish_lane_depth(lane, &cache);
                // Count as allocated only when given to caller
                self.track_alloc(Extent::single(pba), "allocate_one_for_lane_cache")?;
                self.allocated_blocks.fetch_add(1, Ordering::Relaxed);
                self.free_blocks.fetch_sub(1, Ordering::Relaxed);
                return Ok(pba);
            }
        }
        // Slow path: refill from global (blocks stay logically "free" in the cache).
        // The global-pool removal and lane-tail publication share the FreePools
        // critical section so defrag quarantine cannot miss an in-flight refill.
        let first_pba = match self.refill_one_lane_from_global(lane, LANE_CACHE_REFILL_SIZE) {
            Some(pba) => pba,
            None => {
                self.drain_lane_caches_if_populated();
                self.refill_one_lane_from_global(lane, LANE_CACHE_REFILL_SIZE)
                    .ok_or(OnyxError::SpaceExhausted)?
            }
        };
        // First block goes to caller (counted as allocated); the helper has
        // already published the remainder into the lane cache.
        self.track_alloc(Extent::single(first_pba), "allocate_one_for_lane_refill")?;
        self.allocated_blocks.fetch_add(1, Ordering::Relaxed);
        self.free_blocks.fetch_sub(1, Ordering::Relaxed);
        Ok(first_pba)
    }

    /// Return all cached blocks from lane caches to the global free list.
    /// Called during shutdown to prevent block leaks.
    /// Holds EVERY region lock across the lane locks. That is the one place the
    /// all-regions guard is needed: the allocator-wide order is
    /// `FreePools -> lane cache`, and which regions the cached extents land in is
    /// only known after the lane locks are taken, so the region side has to be
    /// acquired first and in full.
    /// [`Self::drain_lane_caches`], but only when the lanes actually hold
    /// something. Returns whether the drain ran, so a caller can skip the retry
    /// that only makes sense after blocks came back.
    ///
    /// EVERY allocation-path drain goes through this. The drain is the single
    /// most expensive operation in the allocator (every region lock in one hold),
    /// it is reached only at ENOSPC boundaries, and at those boundaries the lanes
    /// are usually empty — which is *why* the allocation is failing. Before this
    /// guard, five of the seven call sites ran it unconditionally and one
    /// unaligned allocation could pay for it three times over: reserve-miss,
    /// refill-miss, then `allocate_extent`'s two retries.
    ///
    /// See [`Self::lane_cached_blocks`] for why the lock-free check is sound.
    fn drain_lane_caches_if_populated(&self) -> bool {
        #[cfg(test)]
        if self.drain_guard_off.load(Ordering::Relaxed) {
            // `bench_empty_drain_guard`'s pre-fix arm.
            self.drain_lane_caches();
            return true;
        }
        if !self.has_lane_cached_blocks() {
            self.drain_skips.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        self.drain_lane_caches();
        true
    }

    pub fn drain_lane_caches(&self) {
        let mut drained: u64 = 0;
        let mut pools = self.lock_all_regions(FreeLockSite::Drain);
        // Sampled INSIDE the all-regions hold, which is what makes the invariant
        // below checkable: outside it a refill may legally push (see
        // `lane_cached_blocks`), so a sample taken before the locks would trail
        // the truth for a perfectly correct reason.
        let claimed = self.lane_cached_blocks();
        for (lane, cache_mutex) in self.lane_caches.iter().enumerate() {
            let mut cache = cache_mutex.lock().unwrap();
            for pba in cache.drain(..) {
                pools.release_extent(Extent::single(pba));
                drained += 1;
            }
            self.publish_lane_depth(lane, &cache);
        }
        for (lane, cache_mutex) in self.lane_extent_caches.iter().enumerate() {
            let mut cache = cache_mutex.lock().unwrap();
            for extent in cache.drain(..) {
                pools.release_extent(extent);
                drained += u64::from(extent.count);
            }
            self.publish_lane_extent_depth(lane, &cache);
        }
        drop(pools);
        // The depth counters must never UNDER-report: the whole point is that a
        // caller may skip this drain when they read zero, so a missing publish
        // would turn into a spurious `SpaceExhausted`. `claimed >= drained` is
        // the race-tolerant form of that invariant — a pop that landed while the
        // drain was running (pops need no region lock) can only make `claimed`
        // the larger of the two, while a forgotten publish on a PUSH is exactly
        // what makes it smaller. Pushes cannot race the drain itself: they
        // publish under a region lock and this holds every one of them.
        debug_assert!(
            claimed >= drained,
            "lane depth counters under-reported: claimed {claimed} < drained {drained}"
        );
        self.drain_ops.fetch_add(1, Ordering::Relaxed);
        self.drain_blocks.fetch_add(drained, Ordering::Relaxed);
        // No counter adjustment needed: cached blocks were never counted as allocated
    }

    /// Free a single block.
    ///
    /// Returns error if the PBA is out of bounds, already free (in the global
    /// free list **or** a lane cache), or would underflow counters.
    pub fn free_one(&self, pba: Pba) -> OnyxResult<()> {
        self.free_extent_unchecked_ownership(Extent::single(pba))
    }

    /// Return true if the single block is free — either in the global free list
    /// or sitting in a lane cache (allocated from the free list but not yet
    /// handed out to a caller).
    pub fn is_free(&self, pba: Pba) -> bool {
        let extent = Extent::single(pba);
        let span = self.lock_span(FreeLockSite::Audit, extent);
        if span.overlapping_free(extent).is_some() {
            return true;
        }
        drop(span);
        for cache_mutex in &self.lane_caches {
            let cache = cache_mutex.lock().unwrap();
            if cache.contains(&pba) {
                return true;
            }
        }
        for cache_mutex in &self.lane_extent_caches {
            let cache = cache_mutex.lock().unwrap();
            if cache.iter().any(|extent| extent.contains(pba)) {
                return true;
            }
        }
        false
    }

    /// Allocate a contiguous extent using a lane-local cache before touching
    /// the global free list. This is the hot path for raw 8/16/32 KiB flushes.
    pub fn allocate_extent_for_lane(&self, lane: usize, count: u32) -> OnyxResult<Extent> {
        match self.allocate_exact_extent_for_lane(lane, count) {
            Ok(extent) => Ok(extent),
            Err(OnyxError::SpaceExhausted) => self.allocate_extent(count),
            Err(error) => Err(error),
        }
    }

    /// Exact-width lane allocation. Unlike [`Self::allocate_extent`], this API
    /// never removes and returns the largest short fragment on a miss. Writers
    /// that cannot safely consume a short extent use this to fail without a
    /// compensating rollback/free cycle.
    pub fn allocate_exact_extent_for_lane(&self, lane: usize, count: u32) -> OnyxResult<Extent> {
        if count == 0 {
            return Err(OnyxError::Config("cannot allocate 0 blocks".into()));
        }
        if lane >= self.lane_extent_caches.len() {
            for attempt in 0..2 {
                if let Some(extent) = self.take_exact_regionwise(
                    FreeLockSite::SmallAlloc,
                    count,
                    count,
                    attempt == 0,
                ) {
                    self.track_alloc(extent, "allocate_exact_extent_global")?;
                    self.allocated_blocks
                        .fetch_add(count as u64, Ordering::Relaxed);
                    self.free_blocks.fetch_sub(count as u64, Ordering::Relaxed);
                    return Ok(extent);
                }
                if attempt == 0 {
                    self.drain_lane_caches_if_populated();
                    continue;
                }
                break;
            }
            return Err(OnyxError::SpaceExhausted);
        }

        {
            let mut cache = self.lane_extent_caches[lane].lock().unwrap();
            if let Some(extent) = Self::take_from_extent_cache(&mut cache, count) {
                self.publish_lane_extent_depth(lane, &cache);
                self.track_alloc(extent, "allocate_extent_for_lane_cache")?;
                self.allocated_blocks
                    .fetch_add(count as u64, Ordering::Relaxed);
                self.free_blocks.fetch_sub(count as u64, Ordering::Relaxed);
                return Ok(extent);
            }
        }

        let target = LANE_EXTENT_CACHE_REFILL_BLOCKS.max(count);
        let mut result = self.refill_extent_lane(lane, count, target);
        if result.is_none() {
            // A global fragment can become usable only after it coalesces with
            // short pieces held by one or more lanes. Exact allocation is the
            // ENOSPC boundary, so pay for one bounded drain and retry here —
            // unless the lanes hold nothing, in which case the coalesce this is
            // hoping for cannot exist.
            if self.drain_lane_caches_if_populated() {
                result = self.refill_extent_lane(lane, count, target);
            }
        }

        let Some(result) = result else {
            return Err(OnyxError::SpaceExhausted);
        };

        self.track_alloc(result, "allocate_extent_for_lane_refill")?;
        self.allocated_blocks
            .fetch_add(count as u64, Ordering::Relaxed);
        self.free_blocks.fetch_sub(count as u64, Ordering::Relaxed);
        Ok(result)
    }

    /// Allocate a **stripe-aligned** contiguous extent for a flush lane.
    ///
    /// Returns an extent `e` with `(e.start.0 + phase) % stripe_blocks == 0` and
    /// `e.count == round_up(data_blocks, stripe_blocks)` — a whole number of
    /// full stripes whose *device* offset lands on a stripe boundary. The writer
    /// pads the payload to `e.count` blocks, so a chunklet RAID5/6 backend sees a
    /// full-stripe write and skips parity RMW. Only the `data_blocks` prefix is
    /// L2P-referenced; the tail-pad blocks are freed with the unit (via the
    /// caller's `alloc_blocks = e.count`) and never read.
    ///
    /// Alignment `phase` = `pba_offset % stripe_blocks` (device offset is
    /// `(pba + pba_offset) * block_size`, so PBAs must align against the reserved
    /// prefix, not 0 — see [`crate::io::IoEngine::stripe_phase`]).
    ///
    /// Stays lowest-address dense (first-fit). Alignment head/tail remainders go
    /// back to the lane cache / free list — never leaked, never allocated-counted.
    /// `stripe_blocks <= 1` degenerates to [`Self::allocate_extent_for_lane`] so
    /// non-RAID backends (RawDevice, mirror, plain) are byte-for-byte unchanged.
    pub fn allocate_stripe_extent_for_lane(
        &self,
        lane: usize,
        data_blocks: u32,
        stripe_blocks: u32,
        phase: u32,
    ) -> OnyxResult<Extent> {
        if stripe_blocks <= 1 {
            return self.allocate_extent_for_lane(lane, data_blocks);
        }
        if data_blocks == 0 {
            return Err(OnyxError::Config("cannot allocate 0 blocks".into()));
        }
        self.aligned_allocs.fetch_add(1, Ordering::Relaxed);
        let need = Self::round_up_blocks(data_blocks, stripe_blocks);
        if lane >= self.lane_extent_caches.len() {
            return self.allocate_stripe_extent_global(need, stripe_blocks, phase);
        }

        // Fast path: carve an aligned `need` out of an already-cached run. Once
        // the cache is seeded with an aligned run, every tail it hands back is
        // itself aligned (tail.start = aligned + need, need % stripe == 0), so
        // steady-state carves have zero head pad.
        {
            let mut cache = self.lane_extent_caches[lane].lock().unwrap();
            if let Some(extent) =
                Self::take_aligned_from_extent_cache(&mut cache, need, stripe_blocks, phase)
            {
                self.publish_lane_extent_depth(lane, &cache);
                self.track_alloc(extent, "allocate_stripe_extent_for_lane_cache")?;
                self.allocated_blocks
                    .fetch_add(need as u64, Ordering::Relaxed);
                self.free_blocks.fetch_sub(need as u64, Ordering::Relaxed);
                return Ok(extent);
            }
        }

        // Refill only from the stripe reserve. General/free-fragment runs are
        // deliberately invisible to this path so small allocation and aligned
        // allocation cannot consume each other's working set.
        let want = LANE_EXTENT_CACHE_REFILL_BLOCKS.max(need);
        let mut extent = self.refill_stripe_extent_lane(lane, need, want, stripe_blocks, phase);
        if extent.is_none() {
            // A cold lane may hold the only remaining aligned refill. Reclaim
            // all lane caches once at the reserve-miss boundary, then let the
            // requesting lane seed itself from the reconstituted reserve.
            //
            // This is the hottest of the drain sites — on the box the exhausted
            // regime took this branch 3.37 M times at 5.34 ms each — and also the
            // one where the drain is most often pointless: a reserve miss under a
            // starved pool means the lanes are empty too.
            if self.drain_lane_caches_if_populated() {
                extent = self.refill_stripe_extent_lane(lane, need, want, stripe_blocks, phase);
            }
        }
        let Some(extent) = extent else {
            return self.allocate_stripe_extent_global(need, stripe_blocks, phase);
        };
        self.track_alloc(extent, "allocate_stripe_extent_for_lane_refill")?;
        self.allocated_blocks
            .fetch_add(need as u64, Ordering::Relaxed);
        self.free_blocks.fetch_sub(need as u64, Ordering::Relaxed);
        Ok(extent)
    }

    /// Smallest multiple of `stripe` that is `>= data` (`stripe <= 1` → `data`).
    fn round_up_blocks(data: u32, stripe: u32) -> u32 {
        if stripe <= 1 {
            return data;
        }
        data.div_ceil(stripe) * stripe
    }

    /// Smallest PBA `>= from` with `(pba + phase) % stripe == 0`.
    fn align_up_pba(from: u64, stripe: u64, phase: u64) -> u64 {
        if stripe <= 1 {
            return from;
        }
        let r = (from + phase) % stripe;
        if r == 0 {
            from
        } else {
            from + (stripe - r)
        }
    }

    /// Carve a stripe-aligned `need`-block extent out of a contiguous `run`.
    /// `need` MUST already be a multiple of `stripe`. Returns
    /// `(aligned, head_pad, tail)` — head_pad = blocks below the aligned start,
    /// tail = blocks above `aligned + need` — or `None` if `run` can't host an
    /// aligned `need`.
    fn carve_aligned_from_run(
        run: Extent,
        need: u32,
        stripe: u32,
        phase: u32,
    ) -> Option<(Extent, Option<Extent>, Option<Extent>)> {
        let aligned_start = Self::align_up_pba(run.start.0, stripe as u64, phase as u64);
        let run_end = run.start.0 + run.count as u64;
        if aligned_start + need as u64 > run_end {
            return None;
        }
        let aligned = Extent::new(Pba(aligned_start), need);
        let head = aligned_start - run.start.0;
        let head_pad = (head > 0).then(|| Extent::new(run.start, head as u32));
        let tail_start = aligned_start + need as u64;
        let tail = (tail_start < run_end)
            .then(|| Extent::new(Pba(tail_start), (run_end - tail_start) as u32));
        Some((aligned, head_pad, tail))
    }

    /// Insert into a lane extent cache, keeping it ordered by DESCENDING start.
    ///
    /// The cache holds several disjoint runs once a refill takes more than one,
    /// so the order it is scanned in decides the order PBAs are handed out.
    /// Descending-by-start + take-from-the-back means a lane emits aligned
    /// carves in strictly ASCENDING PBA order, which keeps a leaf's PBAs
    /// clustered (`lane_extent_cache_hands_out_ascending` pins it). An unordered
    /// `Vec` with `swap_remove` would scramble them across the whole refill.
    fn push_extent_cache(cache: &mut Vec<Extent>, extent: Extent) {
        let at = cache.partition_point(|held| held.start.0 > extent.start.0);
        cache.insert(at, extent);
    }

    /// Carve a stripe-aligned `need` from the lowest-address cached run that can
    /// host it, pushing head/tail remainders back into the cache. Head is only
    /// non-empty when a non-aligned run (e.g. a rest pushed by
    /// `allocate_extent_for_lane`) is the only candidate; it stays lane-local for
    /// a later non-stripe alloc.
    ///
    /// The cache is descending by start, so scanning from the BACK visits
    /// candidates in ascending address order — first hit is the address-argmin,
    /// mirroring the global pool's first-fit-by-address inside the lane.
    fn take_aligned_from_extent_cache(
        cache: &mut Vec<Extent>,
        need: u32,
        stripe: u32,
        phase: u32,
    ) -> Option<Extent> {
        for idx in (0..cache.len()).rev() {
            if let Some((aligned, head, tail)) =
                Self::carve_aligned_from_run(cache[idx], need, stripe, phase)
            {
                cache.remove(idx);
                if let Some(head) = head {
                    Self::push_extent_cache(cache, head);
                }
                if let Some(tail) = tail {
                    Self::push_extent_cache(cache, tail);
                }
                return Some(aligned);
            }
        }
        None
    }

    /// Last-ditch stripe-aligned allocation straight from the global free list
    /// (no lane cache). Picks the lowest-address run that can host an aligned
    /// `need`, re-inserts head + tail as free. Returns `SpaceExhausted` rather
    /// than a misaligned/short extent — the writer falls back to an unaligned
    /// block-padded write so IO never stalls on alignment fragmentation.
    fn allocate_stripe_extent_global(
        &self,
        need: u32,
        stripe: u32,
        phase: u32,
    ) -> OnyxResult<Extent> {
        let extent = self
            .take_aligned_extent_from_global(need, stripe, phase)
            .ok_or(OnyxError::SpaceExhausted)?;
        self.track_alloc(extent, "allocate_stripe_extent_global")?;
        self.allocated_blocks
            .fetch_add(need as u64, Ordering::Relaxed);
        self.free_blocks.fetch_sub(need as u64, Ordering::Relaxed);
        Ok(extent)
    }

    /// Allocate up to `count` contiguous blocks. Returns the extent actually allocated
    /// (may be smaller than requested if no large enough contiguous region exists).
    pub fn allocate_extent(&self, count: u32) -> OnyxResult<Extent> {
        if count == 0 {
            return Err(OnyxError::Config("cannot allocate 0 blocks".into()));
        }

        // Try allocation from global free list. If insufficient, drain lane caches and retry.
        for attempt in 0..2 {
            if let Some(result) =
                self.take_exact_regionwise(FreeLockSite::SmallAlloc, count, count, attempt == 0)
            {
                self.track_alloc(result, "allocate_extent")?;
                self.allocated_blocks
                    .fetch_add(count as u64, Ordering::Relaxed);
                self.free_blocks.fetch_sub(count as u64, Ordering::Relaxed);
                return Ok(result);
            }

            // No contiguous extent large enough. Cached lane extents may hold
            // enough free contiguous space, so fold them back once before
            // falling back to the largest global fragment.
            if attempt == 0 && self.drain_lane_caches_if_populated() {
                continue;
            }

            // No contiguous extent large enough — return the largest available
            if let Some(extent) = self.take_largest_regionwise(FreeLockSite::SmallAlloc) {
                self.track_alloc(extent, "allocate_extent_largest")?;
                self.allocated_blocks
                    .fetch_add(extent.count as u64, Ordering::Relaxed);
                self.free_blocks
                    .fetch_sub(extent.count as u64, Ordering::Relaxed);
                return Ok(extent);
            }

            // No free extents at all — drain lane caches and retry once
            if attempt == 0 && self.drain_lane_caches_if_populated() {
                continue;
            }
            break;
        }
        Err(OnyxError::SpaceExhausted)
    }

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
                let mut retired = self.lock_retired_span_range(RetiredLockSite::RetireBatch, lo, hi);
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
    fn uncovered_subranges(set: &BTreeSet<Extent>, extent: Extent) -> Vec<Extent> {
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
                    *cursor = out
                        .last()
                        .map_or(from, |&last| (last + stripe64).max(from));
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
    fn aged_subranges(age: &BTreeMap<u64, RetiredRun>, ext: Extent) -> Vec<Extent> {
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
    fn purge_age_range(age: &mut BTreeMap<u64, RetiredRun>, ext: Extent) {
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
                let mut retired = self.lock_retired_span_range(RetiredLockSite::ReclaimPhaseA, lo, hi);
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
    fn snapshot_lane_caches(&self) -> (HashSet<Pba>, Vec<Extent>) {
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
    fn sorted_extents_overlap(sorted: &[Extent], extent: Extent) -> bool {
        let idx = sorted.partition_point(|e| e.start.0 <= extent.start.0);
        if idx > 0 && Self::extents_overlap(sorted[idx - 1], extent) {
            return true;
        }
        idx < sorted.len() && Self::extents_overlap(sorted[idx], extent)
    }

    fn free_extent_unchecked_ownership(&self, extent: Extent) -> OnyxResult<()> {
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

    fn validate_free_extent(&self, extent: Extent) -> OnyxResult<()> {
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

    fn validate_extent_shape(&self, extent: Extent, context: &'static str) -> OnyxResult<()> {
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

    fn ensure_not_in_lane_cache(&self, extent: Extent, context: &'static str) -> OnyxResult<()> {
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
    fn extract_lane_cache_free_parts(&self, target: Extent) -> Vec<Extent> {
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

    /// Return true if the whole extent is already covered by a free extent
    /// or all its blocks are sitting in lane caches.
    pub fn is_extent_free(&self, extent: Extent) -> bool {
        let pools = self.lock_span(FreeLockSite::Audit, extent);
        if pools.covers_free(extent) {
            return true;
        }
        drop(pools);
        // Fallback: check if every block in the extent is in a lane cache.
        (0..extent.count).all(|i| {
            let pba = Pba(extent.start.0 + i as u64);
            self.lane_caches
                .iter()
                .any(|c| c.lock().unwrap().contains(&pba))
                || self
                    .lane_extent_caches
                    .iter()
                    .any(|c| c.lock().unwrap().iter().any(|e| e.contains(pba)))
        })
    }

    pub fn free_block_count(&self) -> u64 {
        self.free_blocks.load(Ordering::Relaxed)
    }

    pub fn allocated_block_count(&self) -> u64 {
        self.allocated_blocks.load(Ordering::Relaxed)
    }

    pub fn total_block_count(&self) -> u64 {
        self.total_blocks.load(Ordering::Relaxed)
    }

    /// Grow the allocatable frontier online after a chunklet `extend_ld` on the
    /// LV3 LD. `new_device_size_bytes` is the IO-ADDRESSABLE capacity — the SAME
    /// transform the constructor takes (`new_ld.capacity_bytes() -
    /// RESERVED_BLOCKS * BLOCK_SIZE`), NOT the raw LD size.
    ///
    /// The newly-addressable PBAs `[old_total, new_total)` are appended to the
    /// free set as dense extents at the TOP of the space (split into `u32::MAX`
    /// runs like the constructor / rebuild path), preserving the
    /// dense/sequential PBA contract the metadb L2P leaf codec relies on:
    /// first-fit still hands out the lowest free address, so the grown tail is
    /// consumed last. Grow-only — a smaller-or-equal size is a no-op. Returns
    /// the new total block count.
    ///
    /// Ordering: the larger frontier is published (`Release`) BEFORE the new
    /// PBAs enter circulation via the `free_extents` insert, so a concurrent
    /// bounds check (`free_extent` / `retire_*`) can never observe an allocated
    /// PBA from the grown region while `total_blocks` still reads the old value.
    pub fn grow_capacity(&self, new_device_size_bytes: u64) -> OnyxResult<u64> {
        let new_total = new_device_size_bytes / BLOCK_SIZE as u64;
        let old_total = self.total_blocks.load(Ordering::Relaxed);
        if new_total <= old_total {
            return Ok(old_total);
        }
        self.total_blocks.store(new_total, Ordering::Release);
        let added = new_total - old_total;
        {
            // The grown tail routes to whichever regions own it; because the
            // LAST region is unbounded above, growth never needs a re-layout.
            // One `lock_span` per u32-sized chunk rather than one all-regions
            // hold: growth is rare and there is no atomicity requirement across
            // chunks (the frontier was already published above).
            let mut start = old_total;
            let mut remaining = added;
            while remaining > 0 {
                let count = remaining.min(u32::MAX as u64) as u32;
                let extent = Extent::new(Pba(start), count);
                self.lock_span(FreeLockSite::Setup, extent)
                    .insert_classified(extent);
                start += count as u64;
                remaining -= count as u64;
            }
        }
        self.free_blocks.fetch_add(added, Ordering::Relaxed);
        tracing::info!(
            old_total,
            new_total,
            added,
            "allocator online grow_capacity"
        );
        Ok(new_total)
    }

    /// O(1)/O(log N) fragmentation snapshot of the global free set — one lock
    /// acquisition. `free_blocks_in_set` deliberately EXCLUDES lane-cached
    /// extents (they are drained out of the set), so
    /// `stripe_capable_blocks / free_blocks_in_set` is the defrag trigger
    /// signal. Active quarantine-free blocks remain in the denominator but are
    /// intentionally unavailable in the numerator until publication.
    /// Sharded, this sums region by region taking ONE region lock at a time, so
    /// the snapshot is no longer a single instant. Every consumer is advisory
    /// (the defrag trigger, `status`), and holding N locks to freeze the whole
    /// space would stall every writer for the duration.
    pub fn contiguity_stats(&self) -> ContiguityStats {
        let mut out = ContiguityStats {
            free_blocks_in_set: 0,
            free_extents: 0,
            largest_run_blocks: 0,
            stripe_capable_blocks: None,
            stripe_reserve_blocks: 0,
            quarantine_target_blocks: 0,
            quarantine_free_blocks: 0,
        };
        for idx in 0..self.regions.count() {
            let guard = self.lock_region(FreeLockSite::Audit, idx);
            let pools = guard.region(idx);
            out.quarantine_free_blocks += pools
                .quarantines
                .values()
                .map(|target| target.free_parts.blocks_total())
                .sum::<u64>();
            out.quarantine_target_blocks += pools
                .quarantines
                .values()
                .map(|target| target.range.count as u64)
                .sum::<u64>();
            out.free_blocks_in_set += pools.free_blocks_in_pools();
            out.free_extents += (pools.general.len()
                + pools.stripe_reserve.len()
                + pools
                    .quarantines
                    .values()
                    .map(|target| target.free_parts.len())
                    .sum::<usize>()) as u64;
            out.largest_run_blocks = out.largest_run_blocks.max(
                pools
                    .general
                    .largest()
                    .into_iter()
                    .chain(pools.stripe_reserve.largest())
                    .map(|extent| extent.count)
                    .max()
                    .unwrap_or(0),
            );
            if pools.geometry().is_some() {
                let capable = pools.general.stripe_capacity() + pools.stripe_reserve.stripe_capacity();
                out.stripe_capable_blocks =
                    Some(out.stripe_capable_blocks.unwrap_or(0) + capable);
            }
            out.stripe_reserve_blocks += pools.stripe_reserve.blocks_total();
        }
        out
    }

    /// The configured RAID geometry `(stripe_blocks, phase)`, if any. Served from
    /// an atomic written by `set_stripe_geometry`, so the GC defrag scanner's
    /// per-cluster query takes no region lock.
    pub fn stripe_geometry(&self) -> Option<(u32, u32)> {
        let packed = self.geometry_cache.load(Ordering::Relaxed);
        (packed != 0).then(|| ((packed >> 32) as u32, packed as u32))
    }

    /// Blocks of `range` covered by free extents — the defrag target "done"
    /// recheck. One brief free-lock hold, O(log N + overlaps in range).
    pub(crate) fn free_overlap_blocks(&self, range: Extent) -> u64 {
        let span = self.lock_span(FreeLockSite::Audit, range);
        let (lo, hi) = span.layout.span(range);
        (lo..=hi)
            .map(|idx| span.region(idx).overlap_free_blocks(range))
            .sum()
    }

    /// Free/retired occupancy of stripe-aligned windows, BATCHED.
    ///
    /// `starts` must be ascending, deduplicated stripe-aligned window starts
    /// (the caller derives them from [`Self::stripe_geometry`]); the result is
    /// one `(free_blocks, retired_blocks)` per input, in input order.
    ///
    /// This is the scan-driven defrag selector's classify step. The compactor's
    /// L2P window scan streams thousands of candidate windows per cycle, so
    /// calling `free_overlap_blocks` + `retired_overlap_blocks` per window
    /// (two lock acquisitions each) would add ~10^5 region-lock trips per
    /// second to the very locks the flusher-writers contend
    /// (`fragmentation_unaligned_alloc_1889_locks`: lock COUNT, not wait, is
    /// what costs the writer). Grouping by region collapses that to one hold
    /// per region per pass, reusing [`region_holds`] exactly like the retire /
    /// reclaim batch paths.
    ///
    /// Free and retired are two SEPARATE passes so the one-directional
    /// `free -> retired` lock order is never inverted (see
    /// [`Self::retired_overlap_blocks`]). The halves are therefore not one
    /// atomic snapshot, which is fine: selection is advisory —
    /// `begin_defrag_quarantine` re-validates under the free lock and the
    /// rewriter re-validates every LBA against the live blockmap.
    pub(crate) fn classify_stripe_windows(&self, starts: &[u64], stripe: u32) -> Vec<(u32, u32)> {
        if starts.is_empty() || stripe == 0 {
            return Vec::new();
        }
        let windows: Vec<Extent> = starts
            .iter()
            .map(|&start| Extent::new(Pba(start), stripe))
            .collect();
        let layout = self.regions.layout();
        let cap = free_lock_hold_extents();
        let mut out = vec![(0u32, 0u32); windows.len()];

        let mut base = 0usize;
        for (lo, hi, hold) in region_holds(layout, &windows, cap) {
            let span = self.lock_span_range(FreeLockSite::DefragClassify, lo, hi);
            span.charge_items(hold.len() as u64);
            for (i, &window) in hold.iter().enumerate() {
                let (wlo, whi) = layout.span(window);
                let free: u64 = (wlo..=whi)
                    .map(|idx| span.region(idx).overlap_free_blocks(window))
                    .sum();
                out[base + i].0 = free as u32;
            }
            base += hold.len();
        }

        // Second pass, free locks all released: retired occupancy.
        let mut base = 0usize;
        for (lo, hi, hold) in region_holds(self.retired_layout(), &windows, cap) {
            let span = self.lock_retired_span_range(RetiredLockSite::OverlapBlocks, lo, hi);
            for (i, &window) in hold.iter().enumerate() {
                out[base + i].1 = span.overlap_blocks(window) as u32;
            }
            base += hold.len();
        }
        out
    }

    /// Blocks of `range` covered by retired extents. Takes ONLY the retired
    /// lock (callers must NOT hold `free_extents` — keeps the free→retired
    /// lock order one-directional). Retired extents never overlap each other
    /// (coalesced set), so summing clamped intersections is exact.
    pub(crate) fn retired_overlap_blocks(&self, range: Extent) -> u64 {
        self.lock_retired_span(RetiredLockSite::OverlapBlocks, range)
            .overlap_blocks(range)
    }

    /// Number of distinct runs in the global free set. Test-only: the stripe
    /// density guard asserts this stays O(1) under repeated aligned allocation
    /// (alignment pads must not fragment the free list into per-alloc slivers).
    #[cfg(test)]
    pub(crate) fn free_extent_run_count(&self) -> usize {
        self.pool_extent_count()
    }

    fn coalesce_and_insert_any_overlap(set: &mut BTreeSet<Extent>, new: Extent) {
        let mut merged_start = new.start.0;
        let mut merged_end = new.end_pba().0;

        loop {
            let probe = Extent::new(Pba(merged_start), 0);
            let before = set.range(..=probe).next_back().copied();
            if let Some(extent) = before {
                if extent.end_pba().0 >= merged_start {
                    merged_start = merged_start.min(extent.start.0);
                    merged_end = merged_end.max(extent.end_pba().0);
                    set.remove(&extent);
                    continue;
                }
            }

            let probe = Extent::new(Pba(merged_start), 0);
            let after = set.range(probe..).next().copied();
            if let Some(extent) = after {
                if extent.start.0 <= merged_end {
                    merged_start = merged_start.min(extent.start.0);
                    merged_end = merged_end.max(extent.end_pba().0);
                    set.remove(&extent);
                    continue;
                }
            }
            break;
        }

        set.insert(Extent::new(
            Pba(merged_start),
            (merged_end - merged_start) as u32,
        ));
    }

    fn clear_lane_caches(&self) {
        for (lane, cache) in self.lane_caches.iter().enumerate() {
            let mut cache = cache.lock().unwrap();
            cache.clear();
            self.publish_lane_depth(lane, &cache);
        }
        for (lane, cache) in self.lane_extent_caches.iter().enumerate() {
            let mut cache = cache.lock().unwrap();
            cache.clear();
            self.publish_lane_extent_depth(lane, &cache);
        }
    }

    fn ensure_not_free_or_retired_after_wait(
        &self,
        extent: Extent,
        pools: &SpanGuard<'_>,
    ) -> OnyxResult<()> {
        if let Some(e) = pools.overlapping_free(extent) {
            return Err(OnyxError::Config(format!(
                "free_extent: extent {:?} overlaps free extent {:?} after hazard wait",
                extent, e
            )));
        }
        if let Some(e) = self.overlapping_retired_extent(extent) {
            return Err(OnyxError::Config(format!(
                "free_extent: extent {:?} overlaps retired extent {:?} after hazard wait",
                extent, e
            )));
        }
        Ok(())
    }

    /// Publish `lane`'s single-block cache depth. Every entry is one block, so
    /// this is O(1). MUST be called under that lane's cache lock, by every path
    /// that changes the cache — the value is DERIVED from the cache rather than
    /// accumulated as a delta, so a site that forgets to publish can only be
    /// stale for one lane until its next mutation, and can never drift or go
    /// negative.
    fn publish_lane_depth(&self, lane: usize, cache: &[Pba]) {
        self.lane_cache_depth[lane].store(cache.len() as u64, Ordering::Relaxed);
    }

    /// Publish `lane`'s extent cache depth in BLOCKS. Same discipline as
    /// [`Self::publish_lane_depth`]; the sum is over the handful of runs one
    /// refill parks (bounded by [`LANE_EXTENT_CACHE_REFILL_RUNS`]).
    fn publish_lane_extent_depth(&self, lane: usize, cache: &[Extent]) {
        let blocks = cache.iter().map(|e| u64::from(e.count)).sum();
        self.lane_extent_cache_depth[lane].store(blocks, Ordering::Relaxed);
    }

    /// Park `extent` in a lane's extent cache the way a refill would — including
    /// the depth publish, without which the allocation paths would (correctly)
    /// treat the lane as empty and skip the drain that folds it back.
    #[cfg(test)]
    fn seed_lane_extent_cache(&self, lane: usize, extent: Extent) {
        let mut cache = self.lane_extent_caches[lane].lock().unwrap();
        Self::push_extent_cache(&mut cache, extent);
        self.publish_lane_extent_depth(lane, &cache);
    }

    /// Blocks a [`Self::drain_lane_caches`] would hand back, from `2 * lanes`
    /// relaxed loads instead of `2 * lanes` MUTEX acquisitions.
    ///
    /// This is what lets the ENOSPC paths skip a drain that is provably a no-op.
    /// The drain is the most expensive operation in the allocator — it takes
    /// EVERY region lock (2048 by default) in one hold plus a 2048-entry guard
    /// vector — and in the exhausted regime it is also the most useless: the
    /// lanes are empty precisely because allocation is failing, yet five of its
    /// seven call sites used to run it unconditionally, several of them twice
    /// within one allocation.
    ///
    /// **Why a lock-free read is sound.** Blocks only enter a lane cache from a
    /// refill, and every refill publishes into the cache while holding a REGION
    /// lock; `drain_lane_caches` holds EVERY region lock for its whole duration.
    /// So no push can be in flight while a drain runs, and a reader that sees 0
    /// cannot be missing blocks that a drain would have recovered. Blocks that
    /// appear after the read came from a refill that took them out of the same
    /// pool the reader had just found empty — the identical race the
    /// unconditional drain already had, since it too would have run either
    /// before or after that refill's lock hold. Pops need no region lock, but a
    /// pop only lowers the truth, so a stale-high read costs one wasted drain
    /// (i.e. exactly the old behaviour) and never a wrong answer.
    fn lane_cached_blocks(&self) -> u64 {
        self.lane_cache_depth
            .iter()
            .chain(self.lane_extent_cache_depth.iter())
            .map(|depth| depth.load(Ordering::Relaxed))
            .sum()
    }

    fn has_lane_cached_blocks(&self) -> bool {
        self.lane_cached_blocks() > 0
    }

    /// Transfer a global refill into a single-block lane cache atomically with
    /// respect to defrag quarantine publication. The returned first block is no
    /// longer free; every remaining block is visible in the lane cache before
    /// `FreePools` is unlocked.
    fn refill_one_lane_from_global(&self, lane: usize, max_count: u32) -> Option<Pba> {
        // The refill's removal and the lane-tail publication must share ONE
        // critical section so a defrag quarantine cannot miss an in-flight
        // refill. Sharded, that section is the region the refill came from —
        // which is also the only region whose blocks are being published.
        for idx in self.walk_regions(false, 1) {
            let mut guard = self.lock_region(FreeLockSite::SmallAlloc, idx);
            let Some(refill) = Self::take_first_from_pools(guard.region_mut(idx), max_count) else {
                continue;
            };
            if refill.count > 1 {
                let mut cache = self.lane_caches[lane].lock().unwrap();
                for i in 1..refill.count {
                    cache.push(Pba(refill.start.0 + i as u64));
                }
                self.publish_lane_depth(lane, &cache);
            }
            return Some(refill.start);
        }
        None
    }

    /// Take exactly `count` blocks for the caller and publish the rest of the
    /// refill into its extent cache before releasing `FreePools`.
    fn refill_extent_lane(&self, lane: usize, count: u32, max_count: u32) -> Option<Extent> {
        for idx in self.walk_regions(false, 1) {
            let mut guard = self.lock_region(FreeLockSite::WriterUnaligned, idx);
            let Some(refill) = Self::take_exact_from_pools(guard.region_mut(idx), count, max_count)
            else {
                continue;
            };
            let result = Extent::new(refill.start, count);
            if refill.count > count {
                let mut cache = self.lane_extent_caches[lane].lock().unwrap();
                Self::push_extent_cache(
                    &mut cache,
                    Extent::new(Pba(refill.start.0 + count as u64), refill.count - count),
                );
                self.publish_lane_extent_depth(lane, &cache);
            }
            return Some(result);
        }
        None
    }

    /// Seed a lane's extent cache from the stripe reserve and hand back one
    /// aligned `min_count` carve.
    ///
    /// Takes up to [`LANE_EXTENT_CACHE_REFILL_RUNS`] runs (bounded by the
    /// `max_count` block budget) in ONE lock hold, because taking a single
    /// contiguous run made the whole lane-cache mechanism **depend on contiguous
    /// free space**: on an aged pool the reserve degrades to isolated
    /// single-stripe windows, `take` is then exactly one stripe, and the cache
    /// serves exactly one allocation before the next allocation retakes the
    /// global lock — with every other writer queued behind it
    /// (`AllocSupplyStats::allocs_per_refill` reads 1.00 in that state; the
    /// `aged_pool_bench` `SingleStripe` shape reproduces it).
    ///
    /// SELECTION IS UNCHANGED. Removing an extent never coalesces, so "the
    /// lowest-address qualifying runs, in ascending order" is exactly the
    /// sequence K successive `first_fit(min_count)` calls would return — this is
    /// batching, not a policy change, and `batched_refill_equals_sequential_refills`
    /// pins it.
    /// Sharded, a lane refills from ONE region at a time — its "active" region —
    /// and only moves when that region cannot serve it.
    ///
    /// ⚠ This is the one DELIBERATE selection change region sharding makes:
    /// aligned allocation is first-fit-by-address **within the lane's region**
    /// instead of globally. It is safe for the metadb L2P leaf codec because the
    /// codec's real requirement is that ONE LEAF's PBAs stay near each other,
    /// and routing already guarantees leaf ⊂ zone ⊂ shard ⊂ lane
    /// (`shard_for_lba` divides by zone before the modulo), so a leaf's blocks
    /// all come from one lane — hence one region — and `push_extent_cache` hands
    /// them out in ascending order. Leaf v5's `MAX_UNITS_PER_LEAF = 128 =
    /// LEAF_ENTRY_COUNT` makes the historical unit-dict overflow structurally
    /// impossible; the only surviving constraint is a 4 G-block (16 TiB) PBA span
    /// per leaf, and region selection prefers LOW addresses precisely to keep the
    /// working set clustered. `region_pools_equal_single_pool` pins that the free
    /// COVERAGE is identical to the unsharded pool; the emission ORDER is what
    /// changes.
    ///
    /// ## Wide-run preference (`storage.stripe_refill_run_stripes`)
    ///
    /// At a one-stripe floor, `first_fit(min_count)` degenerates into "take the
    /// lowest-address run", and on an aged pool the lowest addresses are windows
    /// pinned by a single live block — 6.15 blocks/run box-measured, i.e. 64
    /// isolated 24 KiB windows per refill, so the writer's consecutive stripes
    /// land at unrelated PBAs and chunklet's adjacency merge collapses to 1.02x
    /// ([[submit_io_is_a_563_way_4k_fanout]]). When the knob is on, a first pass
    /// only considers runs at least `floor` blocks wide (and only regions whose
    /// `stripe_hint` says they have one), which routes the lane to intact
    /// material and typically parks ONE budget-sized run in the lane cache, so
    /// every subsequent carve is adjacent to the last.
    ///
    /// The pass is a pure preference: on a miss the second pass is the legacy
    /// one-stripe-floor refill, unchanged, and the caller's drain + global
    /// fallback (hence the ENOSPC boundary) is untouched. Selection stays
    /// first-fit-BY-ADDRESS in both passes — the floor changes the candidate set,
    /// never the ordering, so this is not the best-fit policy that once corrupted
    /// the metadb L2P leaf.
    fn refill_stripe_extent_lane(
        &self,
        lane: usize,
        min_count: u32,
        max_count: u32,
        stripe: u32,
        phase: u32,
    ) -> Option<Extent> {
        let legacy = StripeRefill {
            min_count,
            max_count,
            stripe,
            phase,
            floor: min_count,
        };
        if let Some(floor) = self.wide_refill_floor(min_count, max_count, stripe) {
            if let Some(extent) = self.refill_stripe_floored(lane, StripeRefill { floor, ..legacy })
            {
                self.refill_wide_hits.fetch_add(1, Ordering::Relaxed);
                return Some(extent);
            }
            self.refill_wide_misses.fetch_add(1, Ordering::Relaxed);
        }
        self.refill_stripe_floored(lane, legacy)
    }

    /// [`Self::refill_stripe_extent_lane`] restricted to reserve runs of at least
    /// `req.floor` blocks. `floor == min_count` is the legacy behaviour.
    fn refill_stripe_floored(&self, lane: usize, req: StripeRefill) -> Option<Extent> {
        let layout = self.regions.layout();
        if !layout.sharded() {
            return self.refill_stripe_from_region(lane, 0, req);
        }
        // A wide pass must not fall back to "try the current region anyway": that
        // fallback exists to reach the caller's ENOSPC boundary, which the second
        // pass reaches on its own. Probing a region the hints say cannot serve
        // the floor would just cost a lock hold per refill on a pool with no wide
        // material left.
        let mut idx = if req.floor > req.min_count {
            self.wide_refill_region(lane, u64::from(req.floor), layout)?
        } else {
            self.lane_region(lane, u64::from(req.min_count), layout)
        };
        for attempt in 0..REGION_REFILL_TRIES {
            if let Some(extent) = self.refill_stripe_from_region(lane, idx, req) {
                return Some(extent);
            }
            // No need to remember which regions were tried: the failed attempt
            // just released that region's lock, and the guard's drop refreshed
            // its `stripe_hint` with the truth, so `switch_lane_region` cannot
            // pick it again for a width it cannot serve. A retry loop is only
            // reachable while another thread is consuming the same regions.
            self.region_refill_misses.fetch_add(1, Ordering::Relaxed);
            if attempt + 1 == REGION_REFILL_TRIES {
                break;
            }
            idx = self.switch_lane_region(lane, idx, u64::from(req.floor), layout)?;
        }
        None
    }

    /// [`Self::lane_region`] without the "nothing qualifies → try the current
    /// region anyway" fallback: `None` means no region's `stripe_hint` claims a
    /// run of `need` blocks, so there is nothing for a wide pass to lock.
    fn wide_refill_region(&self, lane: usize, need: u64, layout: RegionLayout) -> Option<usize> {
        let current = self.lane_regions[lane].load(Ordering::Relaxed);
        if current < layout.count
            && self.regions.stripe_hint[current].load(Ordering::Relaxed) >= need.max(1)
        {
            return Some(current);
        }
        self.switch_lane_region(lane, current, need, layout)
    }

    /// The region a lane should refill from, switching if its current one can no
    /// longer serve `need` whole-stripe blocks.
    fn lane_region(&self, lane: usize, need: u64, layout: RegionLayout) -> usize {
        let current = self.lane_regions[lane].load(Ordering::Relaxed);
        if current < layout.count
            && self.regions.stripe_hint[current].load(Ordering::Relaxed) >= need.max(1)
        {
            return current;
        }
        match self.switch_lane_region(lane, current, need, layout) {
            Some(next) => next,
            // Nothing anywhere looks servable; try the current region (or region
            // 0 for a lane that never had one) so the caller still reaches its
            // drain + global-fallback boundary rather than short-circuiting.
            None if current < layout.count => current,
            None => 0,
        }
    }

    /// Move `lane` off region `from`.
    ///
    /// Prefers the LOWEST-address region that both looks able to serve `need` and
    /// is unclaimed: low addresses keep the whole working set dense (the leaf
    /// clustering argument above), and exclusivity is where the sharding win
    /// comes from — ZFS's metaslab result is that the benefit is owning a region,
    /// not the region being contiguous. Claims are advisory, so when every
    /// servable region is taken (including `num_lanes > num_regions`) lanes share
    /// rather than starve.
    fn switch_lane_region(
        &self,
        lane: usize,
        from: usize,
        need: u64,
        layout: RegionLayout,
    ) -> Option<usize> {
        let mine = lane + 1;
        let mut shared = None;
        let mut exclusive = None;
        for idx in 0..layout.count {
            if idx == from
                || self.regions.stripe_hint[idx].load(Ordering::Relaxed) < need.max(1)
            {
                continue;
            }
            let owner = self.regions.owner[idx].load(Ordering::Relaxed);
            if owner == 0 || owner == mine {
                exclusive = Some(idx);
                break;
            }
            if shared.is_none() {
                shared = Some(idx);
            }
        }
        let next = exclusive.or(shared)?;
        self.regions.owner[next].store(mine, Ordering::Relaxed);
        if from < layout.count {
            let _ = self.regions.owner[from].compare_exchange(
                mine,
                0,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
        self.lane_regions[lane].store(next, Ordering::Relaxed);
        self.region_switches.fetch_add(1, Ordering::Relaxed);
        Some(next)
    }

    /// One region's share of the aligned lane refill — the pre-region body,
    /// unchanged except that the pool it walks is one region's reserve.
    ///
    /// `req.floor` is the minimum width a reserve run must have to QUALIFY. It
    /// filters candidates only — the pick is still the address-argmin among them,
    /// and a qualifying run is still drained up to the block budget, which is what
    /// turns one wide hit into a single contiguous cached run.
    fn refill_stripe_from_region(
        &self,
        lane: usize,
        region: usize,
        req: StripeRefill,
    ) -> Option<Extent> {
        let StripeRefill {
            min_count,
            max_count,
            stripe,
            phase,
            floor,
        } = req;
        debug_assert!(floor >= min_count, "a run floor cannot be under the request");
        let mut guard = self.lock_region(FreeLockSite::WriterRefill, region);
        let pools = guard.region_mut(region);
        if pools.geometry() != Some((stripe, phase)) {
            return None;
        }
        // PASS 1 — plan the batch. The set stays borrowed while walking it, so
        // the whole plan (run + how much of it to take) is decided up front and
        // pass 2 does the mutation. `take` is floored to a stripe multiple so
        // every cached run keeps the reserve's aligned shape.
        //
        // The first pick is the exact address-argmin over the qualifying runs —
        // the same extent the one-run-at-a-time refill took, found the same way.
        let first = pools.stripe_reserve.first_fit(floor)?;
        let mut budget = max_count.max(min_count);
        let mut plan: Vec<(Extent, u32)> = Vec::with_capacity(LANE_EXTENT_CACHE_REFILL_RUNS);
        fn plan_push(
            plan: &mut Vec<(Extent, u32)>,
            budget: &mut u32,
            run: Extent,
            stripe: u32,
            min_count: u32,
        ) {
            let take = (run.count.min(*budget) / stripe) * stripe;
            if take < min_count {
                return;
            }
            plan.push((run, take));
            *budget -= take;
        }
        plan_push(&mut plan, &mut budget, first, stripe, min_count);
        // Then an ascending walk for the next K-1. The walk is bounded in ENTRIES
        // EXAMINED, not just entries taken: a request wider than one stripe on a
        // reserve of single-stripe runs would otherwise skip past every entry in
        // a multi-million-entry set while holding the global lock. Stopping early
        // only costs a smaller batch — never correctness, and never worse than
        // the single-run refill this replaced, which is what `first` already is.
        //
        // A wide pass keeps the SAME entry bound for the same reason, and one wide
        // hit usually consumes the whole budget on its own, so the walk normally
        // exits on `budget` after zero iterations.
        let mut examined = 0usize;
        for run in pools
            .stripe_reserve
            .by_addr()
            .range(Extent::single(Pba(first.start.0 + 1))..)
        {
            if plan.len() >= LANE_EXTENT_CACHE_REFILL_RUNS
                || budget < min_count
                || examined >= LANE_EXTENT_CACHE_REFILL_SCAN
            {
                break;
            }
            examined += 1;
            if run.count >= floor {
                plan_push(&mut plan, &mut budget, *run, stripe, min_count);
            }
        }

        // PASS 2 — execute it.
        let mut refill_blocks: u64 = 0;
        let mut refill_runs: u64 = 0;
        let mut cache = self.lane_extent_caches[lane].lock().unwrap();
        for (run, take) in plan {
            // A previous iteration's reclassified remainder can, in principle,
            // coalesce with a later pick (adjacent reserve extents exist only
            // where `insert_split` chunked a >16 TiB aligned region). Proceed
            // only with runs this refill actually owns — handing out a run that
            // is still reachable in the pool would be a double allocation.
            //
            // `take` matches by START, so "owns it" also means the stored span is
            // still the one that was planned. A coalesce that changed the span
            // while keeping the start would otherwise have this loop slice up a
            // run of a different length: re-inserting `[start+take, count-take)`
            // computed from the PLANNED count can manufacture free blocks past
            // the end of the run that actually existed, and those blocks are
            // live. Skipping just yields a smaller batch, which this refill
            // already tolerates everywhere else.
            let Some(stored) = pools.stripe_reserve.take(&run) else {
                continue;
            };
            if stored.count != run.count {
                pools.insert_classified(stored);
                continue;
            }
            if run.count > take {
                pools.insert_classified(Extent::new(
                    Pba(run.start.0 + take as u64),
                    run.count - take,
                ));
            }
            Self::push_extent_cache(&mut cache, Extent::new(run.start, take));
            refill_blocks += u64::from(take);
            refill_runs += 1;
        }
        if refill_blocks == 0 {
            return None;
        }
        self.refill_ops.fetch_add(1, Ordering::Relaxed);
        self.refill_blocks
            .fetch_add(refill_blocks, Ordering::Relaxed);
        self.refill_runs.fetch_add(refill_runs, Ordering::Relaxed);
        let carved = Self::take_aligned_from_extent_cache(&mut cache, min_count, stripe, phase)
            .expect("stripe-reserve refill is aligned and large enough");
        // One publish for the whole refill: the push loop and the carve both ran
        // under this single lane-lock hold.
        self.publish_lane_extent_depth(lane, &cache);
        Some(carved)
    }

    /// Take an aligned extent while preserving the legacy API contract that
    /// the geometry supplied to `allocate_stripe_extent_for_lane` is enough on
    /// its own. Production's configured geometry uses the O(log N) reserve
    /// path; tests and hypothetical alternate-geometry callers fall back to
    /// the indexed aligned search across both policy pools.
    fn take_aligned_extent_from_global(
        &self,
        need: u32,
        stripe: u32,
        phase: u32,
    ) -> Option<Extent> {
        // Ascending regions ⇒ still the global lowest-address hosting run.
        // Skipping regions whose reserve capacity reads zero is exact for the
        // production (geometry-matched) branch, and it is what keeps a
        // fully-exhausted reserve from costing one lock per region on every
        // writer allocation.
        let stripe_hinted = self.stripe_geometry() == Some((stripe, phase));
        for idx in self
            .walk_regions(stripe_hinted, if stripe_hinted { u64::from(need) } else { 0 })
        {
            if let Some(extent) = self.take_aligned_extent_from_region(idx, need, stripe, phase) {
                return Some(extent);
            }
        }
        None
    }

    fn take_aligned_extent_from_region(
        &self,
        region: usize,
        need: u32,
        stripe: u32,
        phase: u32,
    ) -> Option<Extent> {
        let mut guard = self.lock_region(FreeLockSite::WriterRefill, region);
        let pools = guard.region_mut(region);
        let reserve_only = pools.geometry() == Some((stripe, phase));
        let (from_reserve, run) = if reserve_only {
            (true, pools.stripe_reserve.first_fit(need)?)
        } else {
            let general = pools.general.first_fit_aligned(need, stripe, phase);
            let reserve = pools.stripe_reserve.first_fit_aligned(need, stripe, phase);
            match (general, reserve) {
                (Some(general), Some(reserve)) => {
                    if reserve.start.0 < general.start.0 {
                        (true, reserve)
                    } else {
                        (false, general)
                    }
                }
                (Some(general), None) => (false, general),
                (None, Some(reserve)) => (true, reserve),
                (None, None) => return None,
            }
        };

        let (aligned, head, tail) = Self::carve_aligned_from_run(run, need, stripe, phase)?;
        if from_reserve {
            pools.stripe_reserve.remove(&run);
        } else {
            pools.general.remove(&run);
        }
        if let Some(head) = head {
            pools.insert_classified(head);
        }
        if let Some(tail) = tail {
            pools.insert_classified(tail);
        }
        Some(aligned)
    }

    fn take_first_from_pools(pools: &mut FreePools, max_count: u32) -> Option<Extent> {
        let (from_reserve, extent) =
            Self::lowest_pool_candidate(pools.general.first(), pools.stripe_reserve.first())?;
        if from_reserve {
            pools.stripe_reserve.remove(&extent);
        } else {
            pools.general.remove(&extent);
        }
        let take = extent.count.min(max_count);
        if extent.count > take {
            let tail = Extent::new(Pba(extent.start.0 + take as u64), extent.count - take);
            if from_reserve {
                pools.insert_classified(tail);
            } else {
                pools.general.insert(Extent::new(
                    Pba(extent.start.0 + take as u64),
                    extent.count - take,
                ));
            }
        }
        Some(Extent::new(extent.start, take))
    }

    fn take_exact_from_pools(
        pools: &mut FreePools,
        min_count: u32,
        max_count: u32,
    ) -> Option<Extent> {
        let (from_reserve, extent) = Self::lowest_pool_candidate(
            pools.general.first_fit(min_count),
            pools.stripe_reserve.first_fit(min_count),
        )?;
        if from_reserve {
            pools.stripe_reserve.remove(&extent);
        } else {
            pools.general.remove(&extent);
        }
        let take = extent.count.min(max_count);
        if extent.count > take {
            let tail = Extent::new(Pba(extent.start.0 + take as u64), extent.count - take);
            if from_reserve {
                pools.insert_classified(tail);
            } else {
                pools.general.insert(Extent::new(
                    Pba(extent.start.0 + take as u64),
                    extent.count - take,
                ));
            }
        }
        Some(Extent::new(extent.start, take))
    }

    fn lowest_pool_candidate(
        general: Option<Extent>,
        reserve: Option<Extent>,
    ) -> Option<(bool, Extent)> {
        match (general, reserve) {
            (Some(general), Some(reserve)) => {
                if reserve.start.0 < general.start.0 {
                    Some((true, reserve))
                } else {
                    Some((false, general))
                }
            }
            (Some(general), None) => Some((false, general)),
            (None, Some(reserve)) => Some((true, reserve)),
            (None, None) => None,
        }
    }

    fn take_largest_from_pools(pools: &mut FreePools) -> Option<Extent> {
        let general = pools.general.largest();
        let reserve = pools.stripe_reserve.largest();
        let take_reserve = match (general, reserve) {
            (None, Some(_)) => true,
            (Some(g), Some(r)) => (r.count, r.start.0) > (g.count, g.start.0),
            _ => false,
        };
        if take_reserve {
            let extent = reserve.expect("selected reserve candidate");
            pools.stripe_reserve.remove(&extent);
            Some(extent)
        } else {
            let extent = general?;
            pools.general.remove(&extent);
            Some(extent)
        }
    }

    /// Front-carve `count` blocks off the lowest-address cached run that fits.
    /// Scans from the back (the cache is descending by start), so selection is
    /// first-fit-by-address within the lane. Front-carving keeps the remainder's
    /// start above the taken run and below the next-higher entry, so replacing
    /// in place preserves the ordering.
    fn take_from_extent_cache(cache: &mut Vec<Extent>, count: u32) -> Option<Extent> {
        let idx = (0..cache.len())
            .rev()
            .find(|&idx| cache[idx].count >= count)?;
        let extent = cache[idx];
        let result = Extent::new(extent.start, count);
        if extent.count == count {
            cache.remove(idx);
        } else {
            cache[idx] = Extent::new(Pba(extent.start.0 + count as u64), extent.count - count);
        }
        Some(result)
    }

    fn covering_extent(free: &BTreeSet<Extent>, pba: Pba) -> Option<Extent> {
        free.range(..=Extent::single(pba))
            .next_back()
            .copied()
            .filter(|extent| extent.contains(pba))
    }

    fn overlapping_extent(free: &BTreeSet<Extent>, extent: Extent) -> Option<Extent> {
        if let Some(before) = free.range(..=extent).next_back().copied() {
            if Self::extents_overlap(before, extent) {
                return Some(before);
            }
        }
        free.range(extent..)
            .next()
            .copied()
            .filter(|candidate| Self::extents_overlap(*candidate, extent))
    }

    fn extents_overlap(a: Extent, b: Extent) -> bool {
        a.start.0 < b.end_pba().0 && a.end_pba().0 > b.start.0
    }

    fn overlapping_retired_extent(&self, extent: Extent) -> Option<Extent> {
        self.lock_retired_span(RetiredLockSite::FreeOne, extent)
            .overlapping(extent)
    }
}

#[cfg(test)]
mod free_pool_policy_tests {
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
        assert_eq!(acq(&s1, "audit"), acq(&s0, "audit"), "alloc is not an audit");

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
        assert_eq!(hot.wait_ns, 300 * 9 / 3, "wait scaled by acquisitions/timed");
        assert_eq!(hot.hold_ns, 3000 * 9 / 3);
        assert_eq!(hot.wait_ns_max, 100, "maxima stay raw (a lower bound)");
        assert_eq!(hot.hold_ns_max, 1000);
        assert!((hot.hold_us() - 1.0).abs() < 1e-9, "mean per acquisition holds");

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
            allocator.refill_extent_lane(0, 2, 8),
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
            let first = RESERVED_BLOCKS + (u64::from(STRIPE) - (RESERVED_BLOCKS + u64::from(PHASE)) % u64::from(STRIPE)) % u64::from(STRIPE);
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
                        allocator
                            .allocate_extent_for_lane(lane, 1 + (next() % STRIPE as u64) as u32)
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
                    let aligned =
                        SpaceAllocator::align_up_pba(start, STRIPE as u64, PHASE as u64);
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
}

#[cfg(test)]
mod age_tests {
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
}

#[cfg(test)]
mod stripe_align_tests {
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
            SpaceAllocator::carve_aligned_from_run(Extent::new(Pba(11), 6), 6, STRIPE, PHASE)
                .is_none()
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
            SpaceAllocator::take_aligned_from_extent_cache(&mut cache, STRIPE, STRIPE, PHASE)
                .unwrap();
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
        assert_eq!((a.refills, a.refill_runs, a.refill_blocks, a.drains),
                   (b.refills, b.refill_runs, b.refill_blocks, b.drains));
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
}

#[cfg(test)]
pub(crate) mod aged_pool_bench {
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
                        cursor =
                            SpaceAllocator::align_up_pba(cursor, STRIPE as u64, PHASE as u64);
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
            Some(n) => SpaceAllocator::new_with_exact_regions(
                device_blocks * BLOCK_SIZE as u64,
                lanes,
                n,
            ),
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
                let counts: Vec<u32> =
                    pools.stripe_reserve.by_addr().iter().map(|e| e.count).collect();
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

        for shape in [
            TailShape::Fixed,
            TailShape::Spread,
            TailShape::SingleStripe,
        ] {
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
        let (allocator, _stats, _live) =
            build_aged_pool_parts(scale, TailShape::Spread, 16, Some(DEFAULT_ALLOCATOR_REGIONS));
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
                                allocator
                                    .allocate_stripe_extent_for_lane(tid, STRIPE, STRIPE, PHASE),
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
            ("unconditional (pre-fix)", off1, off2, off1_locks, off2_locks),
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
        let (allocator, stats, _live) =
            build_aged_pool_parts(scale, TailShape::Spread, 16, Some(64));
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
            allocator.free_lock.stride_override.store(stride, Ordering::Relaxed);
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
                            let guard =
                                allocator.lock_region_raw(FreeLockSite::Audit, tid % regions);
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
        allocator.free_lock.stride_override.store(0, Ordering::Relaxed);
        allocator
            .free_lock
            .shard_pin
            .store(usize::MAX, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod region_tests {
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
        let allocator = SpaceAllocator::new_with_exact_regions(
            DEVICE_BLOCKS * BLOCK_SIZE as u64,
            lanes,
            regions,
        );
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
    fn shard_contents(
        allocator: &SpaceAllocator,
        idx: usize,
    ) -> (Vec<Extent>, Vec<(u64, u32)>) {
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
        let allocator =
            SpaceAllocator::new_with_exact_regions(DEVICE_BLOCKS * BLOCK_SIZE as u64, 2, 4);
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
            held.iter().any(|e| e.start.0 <= straddle.start.0
                && e.end_pba().0 >= straddle.end_pba().0)
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
                .free_extent(Extent::new(Pba(low_base + i * 2 * u64::from(STRIPE)), STRIPE))
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
            assert_eq!(newly, extents.iter().map(|e| u64::from(e.count)).sum::<u64>());
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
            .reclaim_retired_extents_batch(&{
                retired.sort_unstable_by_key(|extent| extent.start.0);
                retired.clone()
            }, &running)
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
            (0..100u64).map(|i| RESERVED_BLOCKS + 2 * i).collect::<Vec<_>>()
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
}
