//! Unified PBA lifecycle entry points.
//!
//! A physical block leaves active use in exactly four ways. Historically each
//! call site reached straight into [`SpaceAllocator`] and had to re-derive the
//! "drop the RAM [`CandidateCache`] entry *before* the allocator can hand this
//! PBA to a new owner" invariant by hand — a freed/retired PBA left in the
//! candidate cache can be served to a later dedup verify/promote, which then
//! reads a sector the allocator already gave away (the class of bug the
//! lineage-drain audit flagged). This layer is the single home for that
//! invariant.
//!
//! | entry point             | metadata state of the PBA               | candidate cache | allocator op       |
//! |-------------------------|-----------------------------------------|-----------------|--------------------|
//! | [`rollback_uncommitted`]| never committed (alloc → IO/seq reject)  | untouched       | `free_*`           |
//! | [`PbaLifecycle::retire_committed`] | committed dead (remap/delete/demote) | remove first    | `retire_*`        |
//! | [`PbaLifecycle::free_lineage_gc_proven`] | Lineage GC `FreePbas` proof (rc==0) | remove first | `free_*` (idempotent) |
//! | [`PbaLifecycle::confirm_and_reclaim`] | retired → proven unreferenced (GC) | (already gone)  | `reclaim_*`        |
//!
//! Business code SHOULD route through these instead of calling
//! `SpaceAllocator::{free_*,retire_*,reclaim_*}` directly. The one deliberate
//! exception is the allocator's own internals and unit tests that exercise the
//! allocator primitives in isolation.

use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::dedup::CandidateCache;
use crate::error::OnyxResult;
use crate::metrics::EngineMetrics;
use crate::space::allocator::SpaceAllocator;
use crate::space::extent::Extent;
use crate::types::Pba;

/// Backoff before re-attempting a committed-PBA retire that failed *after* its
/// metadata commit. Matches the buffer writer's `RETRY_BACKOFF` order of
/// magnitude so the two deferred-work paths behave alike.
pub const RETIRE_RETRY_BACKOFF: Duration = Duration::from_secs(1);

/// Return a never-committed PBA extent to the free list.
///
/// For rollback of an allocation whose L2P / dedup / lineage state was *never*
/// published: IO failure, `seq_guard` rejection, over-allocation shrink, a
/// discarded passthrough job. Such a PBA has no [`CandidateCache`] entry (the
/// candidate insert only happens on a *successful* commit), so this path
/// deliberately does NOT touch the candidate cache — a spurious
/// `remove_by_pba` could evict a fingerprint a fast realloc just stamped onto
/// the same PBA.
///
/// Synchronous and safe to call inside the allocate lock scope: callers in the
/// allocate path rely on the free completing before they return
/// `SpaceExhausted`.
pub fn rollback_uncommitted(allocator: &SpaceAllocator, extent: Extent) -> OnyxResult<()> {
    allocator.free_extent(extent)
}

/// [`rollback_uncommitted`] for a single block.
pub fn rollback_uncommitted_one(allocator: &SpaceAllocator, pba: Pba) -> OnyxResult<()> {
    rollback_uncommitted(allocator, Extent::single(pba))
}

struct RetireRetryItem {
    extent: Extent,
    reason: &'static str,
    deadline: Instant,
    attempts: u32,
}

#[derive(Default)]
struct RetireRetryQueue {
    items: VecDeque<RetireRetryItem>,
}

/// Shared handle to the PBA lifecycle layer; cheap to clone (three `Arc`s plus
/// the candidate-cache `Arc`).
#[derive(Clone)]
pub struct PbaLifecycle {
    allocator: Arc<SpaceAllocator>,
    candidate: CandidateCache,
    metrics: Arc<EngineMetrics>,
    retire_retry: Arc<Mutex<RetireRetryQueue>>,
}

impl PbaLifecycle {
    pub fn new(
        allocator: Arc<SpaceAllocator>,
        candidate: CandidateCache,
        metrics: Arc<EngineMetrics>,
    ) -> Self {
        Self {
            allocator,
            candidate,
            metrics,
            retire_retry: Arc::new(Mutex::new(RetireRetryQueue::default())),
        }
    }

    /// Borrow the backing allocator for read-only / allocation operations that
    /// are not part of the free/retire lifecycle.
    pub fn allocator(&self) -> &Arc<SpaceAllocator> {
        &self.allocator
    }

    /// Borrow the candidate cache.
    pub fn candidate(&self) -> &CandidateCache {
        &self.candidate
    }

    /// Roll back a never-committed PBA extent (see free fn
    /// [`rollback_uncommitted`]).
    pub fn rollback_uncommitted(&self, extent: Extent) -> OnyxResult<()> {
        rollback_uncommitted(&self.allocator, extent)
    }

    /// Drop every candidate-cache slot pointing at any PBA in `extent`.
    fn evict_candidates(&self, extent: Extent) {
        for off in 0..extent.count {
            self.candidate
                .remove_by_pba(Pba(extent.start.0 + off as u64));
        }
    }

    /// Retire one committed-dead PBA extent: evict candidate-cache slots first,
    /// then move the extent into the allocator's retired set (not yet
    /// reusable — GC's `confirm_and_reclaim` releases it once proven
    /// unreferenced).
    ///
    /// Failures after the metadata commit are not fatal: the extent is deferred
    /// onto an internal retry queue (re-driven on the next call / by
    /// [`Self::drive_retire_retries`]) and surfaced via the `pba_reclaim_stuck`
    /// gauge, instead of being dropped with only a warn (the old behaviour,
    /// which leaked space until a restart rebuilt the free list).
    pub fn retire_committed(&self, reason: &'static str, extent: Extent) {
        self.drive_retire_retries();
        self.retire_committed_inner(reason, extent, 0);
    }

    fn retire_committed_inner(&self, reason: &'static str, extent: Extent, prior_attempts: u32) {
        // No `is_retired(start)` precheck: that `.start`-only proxy would skip a
        // partially-new extent's tail (→ tail freed before its own grace, and no
        // age-log entry). `retire_extent` is now idempotent per-block and reports
        // the newly-retired count, so re-retires are absorbed there.
        //
        // Candidate eviction MUST precede the allocator handoff so a verifier /
        // promote can never pick up a PBA that is on its way out.
        self.evict_candidates(extent);

        let retire_result = if extent.count <= 1 {
            self.allocator.retire_one(extent.start)
        } else {
            self.allocator.retire_extent(extent)
        };
        match retire_result {
            Ok(newly) => {
                if newly > 0 {
                    // Retire INPUT counter: blocks NEWLY entering the retired set
                    // (the GC reclaim Gate's input). Excludes idempotent re-entries
                    // (already-retired sub-ranges → newly==0) and the lineage
                    // direct-free path. Paired with `gc_retired_blocks_reclaimed`.
                    self.metrics
                        .pba_blocks_retired
                        .fetch_add(u64::from(newly), Ordering::Relaxed);
                }
            }
            Err(e) => {
                self.defer_retire(extent, reason, prior_attempts + 1);
                tracing::warn!(
                    pba = extent.start.0,
                    blocks = extent.count,
                    error = %e,
                    reason,
                    attempts = prior_attempts + 1,
                    "pba_lifecycle: retire failed after metadata commit; deferred for retry"
                );
            }
        }
    }

    fn defer_retire(&self, extent: Extent, reason: &'static str, attempts: u32) {
        let mut q = self.retire_retry.lock().unwrap();
        q.items.push_back(RetireRetryItem {
            extent,
            reason,
            deadline: Instant::now() + RETIRE_RETRY_BACKOFF,
            attempts,
        });
        self.metrics
            .pba_reclaim_stuck
            .store(q.items.len() as u64, Ordering::Relaxed);
    }

    /// Re-attempt any deferred retires whose backoff has elapsed. Safe to call
    /// frequently (cleanup batches, lineage/GC ticks); a no-op when the queue
    /// is empty or nothing is ready.
    pub fn drive_retire_retries(&self) {
        let ready: Vec<RetireRetryItem> = {
            let mut q = self.retire_retry.lock().unwrap();
            if q.items.is_empty() {
                return;
            }
            let now = Instant::now();
            let mut ready = Vec::new();
            let mut keep = VecDeque::with_capacity(q.items.len());
            while let Some(item) = q.items.pop_front() {
                if item.deadline <= now {
                    ready.push(item);
                } else {
                    keep.push_back(item);
                }
            }
            q.items = keep;
            self.metrics
                .pba_reclaim_stuck
                .store(q.items.len() as u64, Ordering::Relaxed);
            ready
        };
        // Candidate cache was already evicted on the first attempt; retries
        // only re-issue the allocator retire (idempotent via the is_retired
        // short-circuit). Failures re-defer with a fresh backoff.
        for item in ready {
            self.retire_committed_inner(item.reason, item.extent, item.attempts);
        }
    }

    /// Free a PBA extent surfaced by metadb Lineage GC's `WalOp::FreePbas`.
    ///
    /// This is the proof-carrying direct-free fast path: metadb only surfaces a
    /// PBA here once its dead-list segment has cleared every snapshot /
    /// descendant-branch pin and the refcount ledger is 0, so the durable
    /// metadata genuinely no longer references it. The layer adds the two
    /// consumer-side obligations the bare proof does NOT cover:
    ///
    /// 1. **Candidate-cache eviction** — same invariant as
    ///    [`Self::retire_committed`]: a RAM candidate slot pointing at this PBA
    ///    must go before the allocator can reissue it.
    /// 2. **Idempotent duplicate surface** — metadb can re-emit the same
    ///    dead-list segment after a crash between the `FreePbas` commit and the
    ///    chain truncate. Within one drain batch duplicates are already folded
    ///    by `coalesce_free_pbas_to_extents`; across batches a re-surfaced PBA
    ///    that is *already free/retired* is absorbed here (counter + debug, not
    ///    a warn) via the `is_extent_free` / `is_retired` precheck.
    ///
    /// Assumption (honest-minimal scope): metadb truncates the dead-list chain
    /// *after* the `FreePbas` commit, so a re-surface cannot name a PBA that was
    /// freed, reallocated, and is now live again — there is no always-on
    /// allocator owned-set guard against that narrow window, only the
    /// precheck (which carries a benign TOCTOU race). A persistently rising
    /// `gc_lineage_idempotent_frees` means duplicate surfacing is happening and
    /// the assumption deserves a fresh look.
    pub fn free_lineage_gc_proven(&self, extent: Extent) {
        // Evict candidate slots first (consumer obligation #1).
        self.evict_candidates(extent);

        // Idempotent duplicate-surface absorption (consumer obligation #2).
        if self.allocator.is_extent_free(extent) || self.allocator.is_retired(extent.start) {
            self.metrics
                .gc_lineage_idempotent_frees
                .fetch_add(extent.count as u64, Ordering::Relaxed);
            tracing::debug!(
                pba = extent.start.0,
                blocks = extent.count,
                "pba_lifecycle: lineage FreePbas extent already free/retired — duplicate surface absorbed"
            );
            return;
        }

        match self.allocator.free_extent(extent) {
            Ok(()) => {
                self.metrics
                    .gc_lineage_freed_blocks
                    .fetch_add(extent.count as u64, Ordering::Relaxed);
            }
            Err(_) => {
                // The whole-extent precheck passed (extent not entirely
                // free/retired) but `free_extent` still failed — almost always
                // a PARTIAL overlap with an already-free/retired region: the
                // same shared base PBA can surface across more than one drain
                // batch as overlapping extents, so a later coalesced extent
                // straddles PBAs a previous surface already freed. Fall back to
                // per-PBA: free the still-allocated PBAs, absorb the
                // already-free/retired ones idempotently. (Caller has already
                // confirmed the whole extent is unreferenced via
                // `referenced_extents`, so every still-allocated PBA here is
                // genuinely dead.)
                let mut freed = 0u64;
                let mut absorbed = 0u64;
                for off in 0..extent.count {
                    let pba = Pba(extent.start.0 + off as u64);
                    if self.allocator.is_free(pba) || self.allocator.is_retired(pba) {
                        absorbed += 1;
                        continue;
                    }
                    match self.allocator.free_one(pba) {
                        Ok(()) => freed += 1,
                        Err(e) => {
                            tracing::warn!(
                                pba = pba.0,
                                error = %e,
                                "pba_lifecycle: lineage FreePbas per-PBA free failed unexpectedly; PBA leaked until restart"
                            );
                        }
                    }
                }
                if freed > 0 {
                    self.metrics
                        .gc_lineage_freed_blocks
                        .fetch_add(freed, Ordering::Relaxed);
                }
                if absorbed > 0 {
                    self.metrics
                        .gc_lineage_idempotent_frees
                        .fetch_add(absorbed, Ordering::Relaxed);
                }
            }
        }
    }

    /// Test-only: re-drive deferred retire retries ignoring their backoff
    /// deadline, so retry behaviour can be exercised without sleeping.
    #[cfg(test)]
    fn drive_retire_retries_force(&self) {
        {
            let mut q = self.retire_retry.lock().unwrap();
            for item in q.items.iter_mut() {
                item.deadline = Instant::now();
            }
        }
        self.drive_retire_retries();
    }

    /// Release a retired extent the GC has proven unreferenced (Gate 1 rc==0 +
    /// hazard barrier + Gate 2 `referenced_extents` live in
    /// `gc::runner::reclaim_retired_extents`; this owns only the final free-list
    /// handoff). Returns `Ok(true)` if the extent was reclaimed, `Ok(false)` if
    /// it was no longer retired.
    pub fn confirm_and_reclaim(&self, extent: Extent) -> OnyxResult<bool> {
        self.allocator.reclaim_retired_extent(extent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::schema::{BlockmapValue, ContentHash};
    use crate::metrics::EngineMetrics;
    use std::sync::atomic::Ordering;

    fn bv(pba: Pba) -> BlockmapValue {
        BlockmapValue {
            pba,
            compression: 0,
            unit_compressed_size: 4096,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 0,
            slot_offset: 0,
            flags: 0,
        }
    }

    fn lifecycle() -> (Arc<SpaceAllocator>, CandidateCache, Arc<EngineMetrics>, PbaLifecycle) {
        let allocator = Arc::new(SpaceAllocator::new(4096 * 1000, 0));
        let candidate = CandidateCache::new(8, 64);
        let metrics = Arc::new(EngineMetrics::default());
        let lc = PbaLifecycle::new(allocator.clone(), candidate.clone(), metrics.clone());
        (allocator, candidate, metrics, lc)
    }

    #[test]
    fn lineage_free_evicts_candidate_then_frees_and_is_idempotent() {
        let (allocator, candidate, metrics, lc) = lifecycle();
        let pba = allocator.allocate_one().unwrap();
        let fp: ContentHash = [1, 2, 3, 4, 5, 6, 7, 8];
        candidate.insert(fp, bv(pba));
        assert!(candidate.has_pba(pba));

        lc.free_lineage_gc_proven(Extent::single(pba));
        assert!(
            !candidate.has_pba(pba),
            "candidate slot must be evicted before the PBA returns to the allocator"
        );
        assert!(allocator.is_free(pba), "PBA must be freed");
        assert_eq!(metrics.gc_lineage_freed_blocks.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.gc_lineage_idempotent_frees.load(Ordering::Relaxed), 0);

        // Duplicate surface of an already-free extent → absorbed idempotently,
        // no double free.
        lc.free_lineage_gc_proven(Extent::single(pba));
        assert_eq!(
            metrics.gc_lineage_freed_blocks.load(Ordering::Relaxed),
            1,
            "duplicate surface must not double-free"
        );
        assert_eq!(metrics.gc_lineage_idempotent_frees.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn retire_committed_evicts_candidate_and_retires_live_pba() {
        let (allocator, candidate, metrics, lc) = lifecycle();
        let pba = allocator.allocate_one().unwrap();
        let fp: ContentHash = [9, 9, 9, 9, 9, 9, 9, 9];
        candidate.insert(fp, bv(pba));

        lc.retire_committed("test", Extent::single(pba));
        assert!(!candidate.has_pba(pba), "candidate evicted before retire");
        assert!(allocator.is_retired(pba), "live PBA retired");
        assert_eq!(
            metrics.pba_blocks_retired.load(Ordering::Relaxed),
            1,
            "retire INPUT counter counts the newly-retired block"
        );

        // Idempotent re-entry must NOT double-count the retire input.
        lc.retire_committed("test", Extent::single(pba));
        assert_eq!(
            metrics.pba_blocks_retired.load(Ordering::Relaxed),
            1,
            "already-retired re-entry must not re-count"
        );
    }

    #[test]
    fn retire_failure_defers_then_retry_succeeds_after_condition_clears() {
        let (allocator, _candidate, metrics, lc) = lifecycle();
        // Retiring an already-FREE PBA fails (overlaps the free list). It must be
        // deferred onto the retry queue, not dropped.
        let pba = allocator.allocate_one().unwrap();
        allocator.free_one(pba).unwrap();
        lc.retire_committed("test", Extent::single(pba));
        assert_eq!(
            metrics.pba_reclaim_stuck.load(Ordering::Relaxed),
            1,
            "failed retire must be deferred and surfaced on the stuck gauge"
        );
        assert!(!allocator.is_retired(pba));
        assert_eq!(
            metrics.pba_blocks_retired.load(Ordering::Relaxed),
            0,
            "a deferred (failed) retire must not count as retire input yet"
        );

        // Re-allocate the same lowest PBA so it is live again, then force a
        // retry: the deferred retire now succeeds and the gauge drains.
        let pba2 = allocator.allocate_one().unwrap();
        assert_eq!(pba2, pba, "allocator hands back the lowest free PBA");
        lc.drive_retire_retries_force();
        assert!(allocator.is_retired(pba), "deferred retire retried to success");
        assert_eq!(metrics.pba_reclaim_stuck.load(Ordering::Relaxed), 0);
        assert_eq!(
            metrics.pba_blocks_retired.load(Ordering::Relaxed),
            1,
            "retire input is counted once the deferred retire finally succeeds"
        );
    }

    #[test]
    fn rollback_uncommitted_frees_without_touching_candidate() {
        let (allocator, candidate, _metrics, lc) = lifecycle();
        let pba = allocator.allocate_one().unwrap();
        // A candidate entry that happens to point at this PBA must be left
        // intact: rollback is for never-committed PBAs that own no candidate.
        let fp: ContentHash = [7; 8];
        candidate.insert(fp, bv(pba));
        lc.rollback_uncommitted(Extent::single(pba)).unwrap();
        assert!(allocator.is_free(pba));
        assert!(
            candidate.has_pba(pba),
            "rollback_uncommitted must not evict candidate slots"
        );
    }

    #[test]
    fn lineage_free_tolerates_partial_overlap_with_already_free() {
        // A multi-block lineage extent can partially overlap PBAs a prior
        // surface already freed (the same shared base re-coalesced across drain
        // batches). `free_extent` rejects the whole extent on overlap; the
        // per-PBA fallback must free the still-allocated PBAs and absorb the
        // already-free one without leaking.
        let (allocator, _candidate, metrics, lc) = lifecycle();
        let p0 = allocator.allocate_one().unwrap();
        let p1 = allocator.allocate_one().unwrap();
        let p2 = allocator.allocate_one().unwrap();
        assert_eq!(p1.0, p0.0 + 1);
        assert_eq!(p2.0, p0.0 + 2);
        // Pre-free the middle PBA so the extent [p0, 3] partially overlaps.
        allocator.free_one(p1).unwrap();

        lc.free_lineage_gc_proven(Extent::new(p0, 3));

        assert!(allocator.is_free(p0), "p0 freed via fallback");
        assert!(allocator.is_free(p1), "p1 stays free");
        assert!(allocator.is_free(p2), "p2 freed via fallback");
        assert_eq!(
            metrics.gc_lineage_freed_blocks.load(Ordering::Relaxed),
            2,
            "two still-allocated PBAs freed by the per-PBA fallback"
        );
        assert_eq!(
            metrics.gc_lineage_idempotent_frees.load(Ordering::Relaxed),
            1,
            "the already-free PBA is absorbed idempotently, not leaked"
        );
    }
}
