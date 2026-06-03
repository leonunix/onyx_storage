//! Coarse, in-RAM PBA "heat map" for adaptive reclaim (observe-only, Stage A).
//!
//! Physical-block reclaim must answer "is PBA P still referenced?" before
//! returning it to the allocator. Onyx keeps no persistent `PBA → referrers`
//! reverse index (it was retired to keep the write path's WAL/apply cheap), so
//! that question degrades to a full all-volume L2P forward scan. A batched
//! version of that scan already landed (`MetaStore::referenced_extents`, one
//! scan per GC cycle).
//!
//! This structure is the next step: a standing background "slow scan" (driven
//! by the GC runner — see [`crate::gc::runner`]) walks live L2P incrementally
//! and accumulates a coarse per-region **live-mapping count** here. Reclaim can
//! later read those counts as a *prior* (hot regions → defer the confirm scan;
//! cold regions → chase them) instead of always paying a full scan.
//!
//! # North-star invariants
//!
//! - **100 % off the write hot path.** Only the background refresh bumps the
//!   map; the front-end write path never touches it.
//! - **Snapshot-free.** Only live forward mappings are counted, so
//!   `take_snapshot` (COW page share, no new mappings) stays O(1) and never
//!   moves the heat.
//! - **A prior, never proof.** In Stage A the map is observe-only and changes
//!   no behaviour. When reclaim later consumes it, the count only decides
//!   *whether* to run the confirm scan, never the correctness of a free —
//!   staleness can only ever *delay* reclaim.
//!
//! # Representation: single self-resetting epoch-packed array
//!
//! One [`AtomicU64`] per bucket, packing `[epoch:32 | count:32]`. A global
//! `current_epoch` advances each time a full sweep laps the whole volume set.
//! On the first bump of a bucket in epoch `E`, its packed epoch field differs
//! from `E`, so it is **reset** to `(E, 1)`; subsequent bumps in `E` only
//! increment the count. So every sweep recomputes counts from scratch with **no
//! double-counting**, even though the walk is partial across many cycles, and a
//! bucket not yet revisited this sweep still carries its previous sweep's count
//! with a self-describing staleness stamp — all in one array, no zeroing pass.
//!
//! Bucket epoch `0` is a reserved **never-scanned sentinel**: `current_epoch`
//! starts at `1`, so a bucket that has never seen a live mapping reads as
//! maximally stale (`age == u32::MAX`) rather than masquerading as a fresh
//! count-0 region.
//!
//! # Concurrency
//!
//! Single writer: all `bump`s and the `advance_epoch` tick happen on the GC
//! runner thread, so the epoch never advances mid-bump and the count never
//! races writer-vs-writer. Readers (reclaim, status) do a single relaxed load
//! of one `AtomicU64`; epoch and count live in the same word, so a reader never
//! sees a torn (epoch, count) pair. The CAS in `bump` is therefore uncontended
//! today but keeps a future *sharded* refresh correct without a redesign.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::types::{Pba, RESERVED_BLOCKS};

/// Reserved bucket-epoch value meaning "this region has never been scanned".
/// `current_epoch` starts at `FIRST_EPOCH` so a freshly-allocated bucket
/// (packed value `0` → epoch field `0`) is distinguishable from one counted in
/// the current sweep.
const NEVER_SCANNED_EPOCH: u32 = 0;
const FIRST_EPOCH: u32 = 1;

#[inline]
const fn pack(epoch: u32, count: u32) -> u64 {
    ((epoch as u64) << 32) | (count as u64)
}

#[inline]
const fn unpack(v: u64) -> (u32, u32) {
    ((v >> 32) as u32, v as u32)
}

/// Shared handle to the heat map; cheap to clone (mirrors `CandidateCache`).
#[derive(Clone)]
pub struct HeatMap {
    inner: Arc<Inner>,
}

struct Inner {
    /// One packed `[epoch:32 | count:32]` slot per PBA region.
    buckets: Box<[AtomicU64]>,
    /// `log2(bucket_size_blocks)` — bucket index is a shift, not a divide.
    bucket_shift: u32,
    /// Blocks per bucket (power of two; the value `bucket_shift` shifts by).
    bucket_size_blocks: u64,
    /// Reserved blocks at the bottom of LV3 (fold into bucket 0).
    reserved: u64,
    /// Advances once per completed full sweep over the volume set.
    current_epoch: AtomicU32,
    /// Total PBA count the map was sized for (`allocator.total_block_count()`).
    total_pbas: u64,
    /// Last summary published by the GC refresh thread. Status reads this
    /// (`cached_summary`, O(1)) instead of paying the O(n_buckets) scan on
    /// every poll — that scan touches the whole ~176 MiB bucket array, evicting
    /// the IO threads' working set from cache and burning memory bandwidth, so
    /// a frequently-polled status endpoint shows up as periodic latency hitches.
    /// Refreshed once per GC refresh cycle by the single writer (no extra lock);
    /// at most one cycle stale, which is fine for an observe-only convergence
    /// signal. Seeded with the true initial all-never-scanned state.
    summary_cache: ArcSwap<HeatSummary>,
    /// EMA of recent confirm-scan reclaim yield (reclaimed extents / scanned
    /// extents), fixed-point ×1000. Single-writer (GC thread). Drives the
    /// yield gate: high yield ⇒ the "hot ⇒ defer" premise is false for this
    /// workload ⇒ stop deferring. `confirm_yield_samples == 0` means "no scan
    /// has been measured yet" (cold start → trust the heat prior, keep deferring).
    confirm_yield_ema_milli: AtomicU32,
    confirm_yield_samples: AtomicU64,
}

/// Cheap snapshot of the heat map for status/metrics. Computing it scans every
/// bucket once (`O(n_buckets)` relaxed loads), so call it off the hot path.
#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct HeatSummary {
    pub n_buckets: u64,
    /// Buckets whose last-scanned count is `> 0` (regions with live mappings).
    pub nonzero_buckets: u64,
    /// Buckets that have never been scanned (epoch-0 sentinel).
    pub never_scanned_buckets: u64,
    pub current_epoch: u32,
    pub max_count: u32,
    pub bucket_size_blocks: u64,
    /// Confirm-scan reclaim-yield EMA in ‰ (0..=1000), or `-1` if no scan has
    /// been measured yet. Drives the reclaim yield gate; surfaced for tuning.
    pub confirm_yield_milli: i32,
}

impl HeatMap {
    /// Build a heat map covering `0..total_pbas`, one bucket per
    /// `bucket_size_blocks` (rounded up to the next power of two so the index
    /// is a shift). Allocates `n_buckets * 8` bytes up front.
    pub fn new(total_pbas: u64, bucket_size_blocks: u64) -> Self {
        let bucket_size_blocks = bucket_size_blocks.max(1).next_power_of_two();
        let bucket_shift = bucket_size_blocks.trailing_zeros();
        let usable = total_pbas.saturating_sub(RESERVED_BLOCKS);
        let n_buckets = usable.div_ceil(bucket_size_blocks).max(1) as usize;
        let buckets = (0..n_buckets)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        // Seed the cache with the true initial state (every bucket never-scanned)
        // so status is correct before the first GC refresh publishes a summary —
        // no startup scan needed.
        let init_summary = HeatSummary {
            n_buckets: n_buckets as u64,
            nonzero_buckets: 0,
            never_scanned_buckets: n_buckets as u64,
            current_epoch: FIRST_EPOCH,
            max_count: 0,
            bucket_size_blocks,
            confirm_yield_milli: -1,
        };
        Self {
            inner: Arc::new(Inner {
                buckets,
                bucket_shift,
                bucket_size_blocks,
                reserved: RESERVED_BLOCKS,
                current_epoch: AtomicU32::new(FIRST_EPOCH),
                total_pbas,
                summary_cache: ArcSwap::from_pointee(init_summary),
                confirm_yield_ema_milli: AtomicU32::new(0),
                confirm_yield_samples: AtomicU64::new(0),
            }),
        }
    }

    /// Number of buckets in the map.
    pub fn n_buckets(&self) -> usize {
        self.inner.buckets.len()
    }

    /// Heap footprint of the bucket array in bytes.
    pub fn memory_bytes(&self) -> usize {
        self.inner.buckets.len() * std::mem::size_of::<AtomicU64>()
    }

    /// Blocks per bucket (after power-of-two rounding).
    pub fn bucket_size_blocks(&self) -> u64 {
        self.inner.bucket_size_blocks
    }

    /// Total PBA range the map was sized for.
    pub fn total_pbas(&self) -> u64 {
        self.inner.total_pbas
    }

    #[inline]
    fn bucket_index(&self, pba: Pba) -> usize {
        let off = pba.0.saturating_sub(self.inner.reserved);
        ((off >> self.inner.bucket_shift) as usize).min(self.inner.buckets.len() - 1)
    }

    /// Record one live-mapping hit at `pba`. Single-writer (GC thread).
    ///
    /// First hit of a bucket in the current epoch resets it to count `1`;
    /// later hits in the same epoch increment (saturating at `u32::MAX`).
    pub fn bump(&self, pba: Pba) {
        let epoch = self.inner.current_epoch.load(Ordering::Relaxed);
        let slot = &self.inner.buckets[self.bucket_index(pba)];
        let mut cur = slot.load(Ordering::Relaxed);
        loop {
            let (bucket_epoch, count) = unpack(cur);
            let next = if bucket_epoch != epoch {
                pack(epoch, 1)
            } else {
                pack(epoch, count.saturating_add(1))
            };
            match slot.compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }

    /// Read the region covering `pba` as `(age, count)`.
    ///
    /// `age` is the number of completed sweeps since this bucket was last
    /// counted: `0` = counted in the in-progress sweep (freshest), `1` = the
    /// prior completed sweep (steady state for most buckets), `>= 2` = stale.
    /// A never-scanned bucket reads `(u32::MAX, 0)`.
    pub fn region(&self, pba: Pba) -> (u32, u32) {
        let epoch = self.inner.current_epoch.load(Ordering::Relaxed);
        let (bucket_epoch, count) =
            unpack(self.inner.buckets[self.bucket_index(pba)].load(Ordering::Relaxed));
        if bucket_epoch == NEVER_SCANNED_EPOCH {
            return (u32::MAX, count);
        }
        // wrapping_sub keeps `age` correct across the u32 epoch wraparound; the
        // staleness floor guarantees every region is revisited far inside
        // 2^31 sweeps, so a wrapped epoch can never alias as fresh.
        (epoch.wrapping_sub(bucket_epoch), count)
    }

    /// Current sweep epoch.
    pub fn current_epoch(&self) -> u32 {
        self.inner.current_epoch.load(Ordering::Relaxed)
    }

    /// Stage-B reclaim prior: is EVERY bucket covering the physical extent
    /// `[start, start+count)` both hot (count > 0) and fresh (age ≤
    /// `fresh_max_age`)? Used to *defer* the confirm scan of a retired extent
    /// whose whole region still looks live. A never-scanned, cold, or stale
    /// covering bucket returns `false` → the caller confirms-by-scan (the
    /// conservative direction). This is only a prior: the actual free still
    /// goes through the confirm scan + rc/retire gate, so a false "hot" only
    /// ever *delays* reclaim, never frees a live block.
    pub fn extent_hot_and_fresh(&self, start: Pba, count: u32, fresh_max_age: u32) -> bool {
        if count == 0 {
            return false;
        }
        let epoch = self.inner.current_epoch.load(Ordering::Relaxed);
        let first = self.bucket_index(start);
        let last = self.bucket_index(Pba(start.0 + u64::from(count) - 1));
        for b in first..=last {
            let (bucket_epoch, c) = unpack(self.inner.buckets[b].load(Ordering::Relaxed));
            if bucket_epoch == NEVER_SCANNED_EPOCH {
                return false; // never counted → cannot trust as hot
            }
            let age = epoch.wrapping_sub(bucket_epoch);
            if c == 0 || age > fresh_max_age {
                return false; // cold or stale → confirm
            }
        }
        true
    }

    /// Advance to the next epoch (a full sweep completed). Single-writer.
    /// Returns the new epoch. Skips the `NEVER_SCANNED_EPOCH` sentinel on
    /// wraparound so `0` always means "never scanned".
    pub fn advance_epoch(&self) -> u32 {
        let prev = self.inner.current_epoch.fetch_add(1, Ordering::Relaxed);
        let next = prev.wrapping_add(1);
        if next == NEVER_SCANNED_EPOCH {
            // wrapped past u32::MAX onto the sentinel — bump once more.
            self.inner.current_epoch.fetch_add(1, Ordering::Relaxed);
            return NEVER_SCANNED_EPOCH.wrapping_add(1);
        }
        next
    }

    /// Read the cached convergence summary (`O(1)`). This is what status/metrics
    /// should call: it returns the last summary the GC refresh thread published
    /// via [`Self::refresh_summary_cache`], avoiding the `O(n_buckets)` scan
    /// (and the ~176 MiB cache-thrash) on every poll. At most one GC refresh
    /// cycle stale.
    pub fn cached_summary(&self) -> HeatSummary {
        *self.inner.summary_cache.load_full()
    }

    /// Recompute the summary (one `O(n_buckets)` scan) and publish it to the
    /// cache. Called by the single GC refresh thread once per cycle, so the
    /// scan stays on a background thread at a bounded cadence instead of firing
    /// on every (unbounded-frequency, foreground) status poll.
    pub fn refresh_summary_cache(&self) {
        let s = self.summary();
        self.inner.summary_cache.store(Arc::new(s));
    }

    /// Record a confirm-scan outcome for the yield gate: `confirmed` extents
    /// were scanned, `reclaimed` of them freed. Updates the yield EMA (×1000),
    /// single-writer (GC thread). Only call on cycles that scanned the FULL
    /// survivor set (incl. hot regions) — a pure-defer cycle only confirms cold
    /// extents and would bias the EMA high. No-op when `confirmed == 0`.
    pub fn record_confirm_yield(&self, confirmed: usize, reclaimed: usize) {
        if confirmed == 0 {
            return;
        }
        let sample = ((reclaimed as u64 * 1000) / confirmed as u64).min(1000) as u32;
        let n = self
            .inner
            .confirm_yield_samples
            .fetch_add(1, Ordering::Relaxed);
        // Seed with the first sample, then EMA with alpha = 1/4 (responsive to
        // workload changes while smoothing per-cycle noise).
        let next = if n == 0 {
            sample
        } else {
            let prev = self.inner.confirm_yield_ema_milli.load(Ordering::Relaxed);
            (prev * 3 + sample) / 4
        };
        self.inner
            .confirm_yield_ema_milli
            .store(next, Ordering::Relaxed);
    }

    /// Current confirm-scan reclaim-yield EMA in ‰ (0..=1000), or `None` if no
    /// scan has been measured yet (cold start). Read by the reclaim yield gate
    /// and surfaced in status.
    pub fn confirm_yield_milli(&self) -> Option<u32> {
        if self.inner.confirm_yield_samples.load(Ordering::Relaxed) == 0 {
            None
        } else {
            Some(self.inner.confirm_yield_ema_milli.load(Ordering::Relaxed))
        }
    }

    /// Scan every bucket for a status/metrics snapshot. `O(n_buckets)` — do NOT
    /// call on the status hot path; use [`Self::cached_summary`]. Kept public
    /// for the GC refresh (`refresh_summary_cache`) and tests.
    pub fn summary(&self) -> HeatSummary {
        let mut nonzero = 0u64;
        let mut never = 0u64;
        let mut max_count = 0u32;
        for slot in self.inner.buckets.iter() {
            let (bucket_epoch, count) = unpack(slot.load(Ordering::Relaxed));
            if bucket_epoch == NEVER_SCANNED_EPOCH {
                never += 1;
            }
            if count > 0 {
                nonzero += 1;
                max_count = max_count.max(count);
            }
        }
        HeatSummary {
            n_buckets: self.inner.buckets.len() as u64,
            nonzero_buckets: nonzero,
            never_scanned_buckets: never,
            current_epoch: self.current_epoch(),
            max_count,
            bucket_size_blocks: self.inner.bucket_size_blocks,
            confirm_yield_milli: self.confirm_yield_milli().map_or(-1, |y| y as i32),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(total_pbas: u64, bucket_size: u64) -> HeatMap {
        HeatMap::new(total_pbas, bucket_size)
    }

    #[test]
    fn pack_unpack_roundtrip() {
        for &(e, c) in &[
            (0u32, 0u32),
            (1, 1),
            (7, 4096),
            (u32::MAX, u32::MAX),
            (3, 0),
        ] {
            assert_eq!(unpack(pack(e, c)), (e, c));
        }
    }

    #[test]
    fn sizing_and_bucketing() {
        // 1 MiB buckets = 256 blocks; 1024 usable blocks (+8 reserved) → 4 buckets.
        let hm = map(8 + 1024, 256);
        assert_eq!(hm.bucket_size_blocks(), 256);
        assert_eq!(hm.n_buckets(), 4);
        // reserved PBAs fold into bucket 0; first usable PBA also bucket 0.
        assert_eq!(hm.bucket_index(Pba(0)), 0);
        assert_eq!(hm.bucket_index(Pba(RESERVED_BLOCKS)), 0);
        assert_eq!(hm.bucket_index(Pba(RESERVED_BLOCKS + 255)), 0);
        assert_eq!(hm.bucket_index(Pba(RESERVED_BLOCKS + 256)), 1);
        // top PBA stays in range; an out-of-range PBA clamps to the last bucket.
        assert_eq!(hm.bucket_index(Pba(8 + 1023)), 3);
        assert_eq!(hm.bucket_index(Pba(u64::MAX)), hm.n_buckets() - 1);
    }

    #[test]
    fn non_power_of_two_bucket_size_rounds_up() {
        let hm = map(8 + 10_000, 100);
        assert_eq!(hm.bucket_size_blocks(), 128); // 100 → next pow2
    }

    #[test]
    fn never_scanned_reads_max_age() {
        let hm = map(8 + 1024, 256);
        let (age, count) = hm.region(Pba(8));
        assert_eq!(age, u32::MAX);
        assert_eq!(count, 0);
        assert_eq!(hm.current_epoch(), FIRST_EPOCH);
    }

    #[test]
    fn bump_counts_within_a_sweep() {
        let hm = map(8 + 1024, 256);
        let pba = Pba(8);
        hm.bump(pba);
        hm.bump(pba);
        hm.bump(pba);
        let (age, count) = hm.region(pba);
        assert_eq!(count, 3);
        assert_eq!(age, 0); // counted in the current epoch
    }

    #[test]
    fn epoch_advance_resets_count_no_double_count() {
        let hm = map(8 + 1024, 256);
        let pba = Pba(8);
        // Sweep 1 (epoch 1): two hits.
        hm.bump(pba);
        hm.bump(pba);
        assert_eq!(hm.region(pba).1, 2);
        // Sweep 1 completes.
        let e2 = hm.advance_epoch();
        assert_eq!(e2, FIRST_EPOCH + 1);
        // Not yet revisited: count survives, age becomes 1 (steady state).
        let (age, count) = hm.region(pba);
        assert_eq!(count, 2);
        assert_eq!(age, 1);
        // Sweep 2 hits once: count RESETS to 1 (not 3), proving no carry-over.
        hm.bump(pba);
        let (age, count) = hm.region(pba);
        assert_eq!(count, 1);
        assert_eq!(age, 0);
    }

    #[test]
    fn age_grows_with_each_unrevisited_sweep() {
        let hm = map(8 + 1024, 256);
        let pba = Pba(8);
        hm.bump(pba);
        assert_eq!(hm.region(pba).0, 0);
        hm.advance_epoch();
        assert_eq!(hm.region(pba).0, 1);
        hm.advance_epoch();
        assert_eq!(hm.region(pba).0, 2);
    }

    #[test]
    fn distinct_buckets_are_independent() {
        let hm = map(8 + 1024, 256);
        let a = Pba(RESERVED_BLOCKS); // bucket 0
        let b = Pba(RESERVED_BLOCKS + 300); // bucket 1
        hm.bump(a);
        hm.bump(a);
        hm.bump(b);
        assert_eq!(hm.region(a).1, 2);
        assert_eq!(hm.region(b).1, 1);
        // an untouched bucket is still never-scanned.
        let c = Pba(RESERVED_BLOCKS + 600); // bucket 2
        assert_eq!(hm.region(c).0, u32::MAX);
    }

    #[test]
    fn summary_reports_convergence_signal() {
        let hm = map(8 + 1024, 256); // 4 buckets
        assert_eq!(hm.summary().never_scanned_buckets, 4);
        hm.bump(Pba(RESERVED_BLOCKS)); // bucket 0
        hm.bump(Pba(RESERVED_BLOCKS)); // bucket 0 again
        hm.bump(Pba(RESERVED_BLOCKS + 256)); // bucket 1
        let s = hm.summary();
        assert_eq!(s.n_buckets, 4);
        assert_eq!(s.nonzero_buckets, 2);
        assert_eq!(s.never_scanned_buckets, 2);
        assert_eq!(s.max_count, 2);
        assert_eq!(s.bucket_size_blocks, 256);
    }

    #[test]
    fn cached_summary_seeds_then_tracks_refresh() {
        let hm = map(8 + 1024, 256); // 4 buckets
                                     // Before any refresh, the cache reflects the true initial state
                                     // (all never-scanned) without scanning.
        let c = hm.cached_summary();
        assert_eq!(c.n_buckets, 4);
        assert_eq!(c.never_scanned_buckets, 4);
        assert_eq!(c.nonzero_buckets, 0);
        // Bumps alone do NOT update the cache (status must not see live scans).
        hm.bump(Pba(RESERVED_BLOCKS)); // bucket 0
        hm.bump(Pba(RESERVED_BLOCKS + 256)); // bucket 1
        assert_eq!(
            hm.cached_summary().nonzero_buckets,
            0,
            "cache stale until refresh"
        );
        // The fresh scan still reflects reality on demand.
        assert_eq!(hm.summary().nonzero_buckets, 2);
        // A refresh (what the GC thread does each cycle) publishes it.
        hm.refresh_summary_cache();
        let c = hm.cached_summary();
        assert_eq!(c.nonzero_buckets, 2);
        assert_eq!(c.never_scanned_buckets, 2);
    }

    #[test]
    fn extent_hot_and_fresh_predicate() {
        let hm = map(8 + 1024, 256); // 4 buckets, 256 blocks each
        let base = RESERVED_BLOCKS; // bucket 0 starts here
                                    // Empty extent never qualifies.
        assert!(!hm.extent_hot_and_fresh(Pba(base), 0, 1));
        // Never-scanned region → not hot.
        assert!(!hm.extent_hot_and_fresh(Pba(base), 4, 1));
        // Make bucket 0 hot+fresh.
        hm.bump(Pba(base));
        hm.bump(Pba(base + 5));
        assert!(hm.extent_hot_and_fresh(Pba(base), 4, 1)); // within bucket 0, fresh
                                                           // An extent spanning bucket 0 (hot) into bucket 1 (cold) → not all hot.
        assert!(!hm.extent_hot_and_fresh(Pba(base + 254), 4, 1));
        // Age it out: two epoch advances without revisiting → age 2 > fresh 1.
        hm.advance_epoch();
        assert!(hm.extent_hot_and_fresh(Pba(base), 4, 1)); // age 1 still ≤ 1
        hm.advance_epoch();
        assert!(!hm.extent_hot_and_fresh(Pba(base), 4, 1)); // age 2 > 1 → stale
                                                            // A larger fresh_max_age tolerates the staleness.
        assert!(hm.extent_hot_and_fresh(Pba(base), 4, 2));
    }

    #[test]
    fn multi_block_unit_bumps_each_spanned_bucket() {
        // A multi-block compression unit spans several PBAs; physical_pbas
        // yields one per spanned block, and each lands in its own bucket here.
        let hm = map(8 + 1024, 256);
        // Span PBAs at the boundary of bucket 0 and 1.
        for pba in [
            Pba(RESERVED_BLOCKS + 254),
            Pba(RESERVED_BLOCKS + 255),
            Pba(RESERVED_BLOCKS + 256),
        ] {
            hm.bump(pba);
        }
        assert_eq!(hm.region(Pba(RESERVED_BLOCKS)).1, 2); // two in bucket 0
        assert_eq!(hm.region(Pba(RESERVED_BLOCKS + 256)).1, 1); // one in bucket 1
    }
}
