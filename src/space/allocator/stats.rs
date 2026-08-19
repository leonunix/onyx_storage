use super::*;

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
    /// `allocate_stripe_run_for_lane` calls and the whole stripes they served —
    /// the direct read of `flush.stripe_run_max_stripes` (design D1). `stripes /
    /// allocs` is the mean bundle width, i.e. how many consecutive stripes one LV3
    /// op covers; 1.0 means the knob is off or the pool has no contiguity left.
    pub stripe_run_allocs: u64,
    pub stripe_run_stripes: u64,
    /// Bundle widths bucketed by `floor(log2(stripes))`: 1, 2-3, 4-7, 8-15, 16-31,
    /// 32-63, 64+. The mean hides the shape, and the shape is what says whether the
    /// SUPPLY or the CAP is binding.
    pub stripe_run_width_hist: [u64; super::STRIPE_RUN_WIDTH_BUCKETS],
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

    /// Mean whole stripes per LV3 write bundle. `1.0` = one stripe per op, the
    /// pre-D1 shape whose per-PD adjacency merge measured 1.9x.
    pub fn stripes_per_run(&self) -> f64 {
        if self.stripe_run_allocs == 0 {
            return 0.0;
        }
        self.stripe_run_stripes as f64 / self.stripe_run_allocs as f64
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
pub(super) const FREE_LOCK_SITES: usize = 14;

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
pub(super) const RETIRED_LOCK_SITES: usize = 14;

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
pub(super) const LOCK_STAT_SHARDS: usize = 64;

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
pub(super) struct LockStatShard<const N: usize> {
    pub(super) acquisitions: [AtomicU64; N],
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
    pub(super) fn new() -> Self {
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
    pub(super) fn count(&self, site: usize, stride: u64) -> bool {
        let n = self.acquisitions[site].fetch_add(1, Ordering::Relaxed) + 1;
        stride <= 1 || n % stride == 1
    }

    /// Charge the wait for one TIMED acquisition of `site`.
    pub(super) fn charge_wait(&self, site: usize, waited: u64) {
        self.timed[site].fetch_add(1, Ordering::Relaxed);
        self.wait_ns[site].fetch_add(waited, Ordering::Relaxed);
        self.wait_ns_max[site].fetch_max(waited, Ordering::Relaxed);
    }

    /// Charge the hold for one TIMED release of `site`.
    pub(super) fn charge_hold(&self, site: usize, held: u64) {
        self.hold_ns[site].fetch_add(held, Ordering::Relaxed);
        self.hold_ns_max[site].fetch_max(held, Ordering::Relaxed);
    }

    pub(super) fn charge_items(&self, site: usize, n: u64) {
        self.items[site].fetch_add(n, Ordering::Relaxed);
    }
}

/// Per-site wait/hold accounting for ONE mutex, sharded per thread. Monotonic
/// counters, so two reads difference cleanly; the `_max` fields are high-water
/// marks and do NOT difference (`hold_ns_max` answers "what is the worst
/// shut-out window this site ever caused"; `wait_ns_max` separates steady
/// queueing from a single blackout by a long holder).
pub(super) struct SiteLockStats<const N: usize> {
    pub(super) shards: Vec<LockStatShard<N>>,
    /// Per-instance stride, `0` = follow the process-wide one. Test-only: the
    /// suite runs in parallel in one process, so a test must not perturb another
    /// test's accounting through [`set_lock_stats_stride`].
    #[cfg(test)]
    pub(super) stride_override: AtomicU64,
    /// Test-only: force every thread onto ONE shard, reproducing the pre-sharding
    /// single-cache-line shape so `bench_lock_stats_overhead` can A/B it inside
    /// one process — the same in-process A/B discipline as
    /// [`set_region_serialize`]. `usize::MAX` = per-thread (production).
    #[cfg(test)]
    pub(super) shard_pin: AtomicUsize,
}

impl<const N: usize> SiteLockStats<N> {
    pub(super) fn new() -> Self {
        Self {
            shards: (0..LOCK_STAT_SHARDS)
                .map(|_| LockStatShard::new())
                .collect(),
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
    pub(super) fn begin(&self, site: usize) -> (&LockStatShard<N>, Option<Instant>) {
        let shard = self.shard();
        let queued = shard.count(site, self.stride()).then(Instant::now);
        (shard, queued)
    }

    pub(super) fn snapshot(&self, names: [&'static str; N]) -> Vec<LockSiteStats> {
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
                        ((u128::from(sum) * u128::from(out.acquisitions)) / u128::from(out.timed))
                            as u64
                    }
                };
                out.wait_ns = scale(out.wait_ns);
                out.hold_ns = scale(out.hold_ns);
                out
            })
            .collect()
    }
}

pub(super) type FreeLockStats = SiteLockStats<FREE_LOCK_SITES>;
pub(super) type RetiredLockStats = SiteLockStats<RETIRED_LOCK_SITES>;

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
pub(super) struct TimedGuard<'a, T, const N: usize> {
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
    pub(super) fn new(stats: &'a SiteLockStats<N>, site: usize, lock: &'a Mutex<T>) -> Self {
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
    pub(super) fn charge_items(&self, n: u64) {
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
