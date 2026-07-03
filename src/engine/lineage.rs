use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::meta::store::MetaStore;
use crate::space::pba_lifecycle::PbaLifecycle;

/// Handle for the lineage-freed PBA drain thread.
///
/// Phase 5 ([[no-refcount-hot-path-design]]) moves PBA retirement off the
/// hot write path: the engine's commit path no longer maintains rc, so
/// retiring an exclusive PBA happens later, when Lineage GC clears its
/// dead-list segment past every snap_pin and emits a `WalOp::FreePbas`.
/// The metadb backend converts those WAL outcomes into a crossbeam
/// channel; this thread drains the channel and returns each PBA to the
/// allocator's free list. Because the metadb rc==0 proof does NOT cover
/// rc-untracked L2P sharing (compressed/packed units whose member LBAs share
/// one base PBA), the drain re-verifies every surfaced extent against the
/// live L2P (hazard barrier + all-volume `referenced_extents`) before acting:
/// truly-unreferenced extents go to [`PbaLifecycle::free_lineage_gc_proven`]
/// (candidate-cache evicted, duplicate surfaces absorbed idempotently); any
/// still-referenced extent is retired for `GcRunner` to reclaim once its last
/// reference dies.
///
/// The thread parks for `interval` between drains; channel sends from the
/// FreedPbasSink wake it implicitly when production resumes, since the
/// next park interval expires and we re-check.
pub(super) struct LineageFreedPbaDrainHandle {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl LineageFreedPbaDrainHandle {
    pub(super) fn start(
        meta: Arc<MetaStore>,
        pba_lifecycle: PbaLifecycle,
        interval: std::time::Duration,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = stop.clone();
        let thread = std::thread::Builder::new()
            .name("onyx-lineage-drain".into())
            .spawn(move || {
                while !stop_clone.load(Ordering::Relaxed) {
                    Self::drain_once(&meta, &pba_lifecycle);
                    std::thread::sleep(interval);
                }
                // Final drain so PBAs the GC emitted right before
                // shutdown still make it back to the allocator.
                Self::drain_once(&meta, &pba_lifecycle);
            })
            .expect("spawn lineage drain thread");
        Self {
            stop,
            thread: Some(thread),
        }
    }

    fn drain_once(meta: &Arc<MetaStore>, pba_lifecycle: &PbaLifecycle) {
        let pbas = meta.drain_lineage_freed_pbas();
        if pbas.is_empty() {
            return;
        }
        // Within-batch duplicate PBAs are folded here; cross-batch / cross-crash
        // duplicates are absorbed idempotently by `retire_committed`.
        let extents = crate::meta::backend::coalesce_free_pbas_to_extents(&pbas);
        if extents.is_empty() {
            return;
        }

        // Phase 5 lineage surfaces rc==0 dead PBAs as free CANDIDATES. We do NOT
        // direct-free them here. The metadb rc==0 proof does NOT cover
        // rc-untracked L2P sharing (compressed / packed multi-LBA units share
        // one base PBA), so freeing requires a live-L2P reverify — and a reverify
        // done HERE, immediately on surfacing, raced the metadata pipeline: a
        // sibling reference already committed to LV2 but whose metadb L2P apply
        // was still in flight was transiently invisible to the scan, so a direct
        // free corrupted the sibling's reads (CRC; see
        // fixb_soak_exposed_referenced_extents_race).
        //
        // Instead RETIRE every surfaced extent and let the unified
        // `GcRunner::reclaim_retired_extents` (Gate-1 rc==0 + hazard barrier +
        // buffer-aware `referenced_extents` + reclaim-age grace) be the SOLE
        // committed→free path — the same retire→reclaim path the writer's
        // post-commit cleanup uses, which has soaked clean for hours. A retired
        // PBA is not in the allocator free list (so it cannot be reused under
        // us), and the reclaim path's delay + age grace lets any in-flight
        // sibling reference settle into the L2P before the reverify decides.
        // `retire_committed_batch` evicts the candidate cache. Survivors of the
        // precheck retire in ONE lock-amortized batch — the old per-extent
        // `retire_committed` paid the full lane-scan + free/retired/age lock
        // cost per surfaced extent, contending the allocator at the overwrite
        // rate from this background thread.
        let allocator = pba_lifecycle.allocator();
        let survivors: Vec<_> = extents
            .into_iter()
            .filter(|extent| {
                // Idempotent duplicate-surface absorb (mirrors the old
                // free_lineage_gc_proven precheck): metadb legitimately
                // re-surfaces the same PBA across GC cycles (documented
                // duplicate FreePbas). If a prior surface already
                // retired+reclaimed it, the PBA is now free / still retired —
                // retiring again would hit "retire_extent overlaps free
                // extent" and churn the retry queue. Skip it. (Harmless even
                // without this — grace + GcRunner Gate-2 prevent any bad free
                // — but this keeps the log + retry queue clean.)
                !allocator.is_extent_free(*extent) && !allocator.is_retired(extent.start)
            })
            .collect();
        pba_lifecycle.retire_committed_batch("lineage_gc_surfaced", &survivors);
    }

    pub(super) fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl Drop for LineageFreedPbaDrainHandle {
    fn drop(&mut self) {
        self.stop();
    }
}
