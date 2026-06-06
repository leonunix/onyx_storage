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
        // duplicates are absorbed idempotently inside free_lineage_gc_proven.
        let extents = crate::meta::backend::coalesce_free_pbas_to_extents(&pbas);
        if extents.is_empty() {
            return;
        }

        // The metadb lineage proof (rc==0 + no snap/descendant pin) is NOT
        // sufficient on its own. Under Phase 5 rc-neutral writes a PBA can be
        // referenced by multiple live L2P entries WITHOUT bumping rc: the member
        // LBAs of a compressed / packed unit share one base PBA, so overwriting
        // one member records that base dead at rc==0 while siblings still point
        // at it. Direct-freeing it then corrupts the siblings' reads (observed
        // under compression=none; masked but still racy with compression). So
        // mirror the GC retired-extent Gate 2 here before freeing:
        //   1. hazard barrier — drain in-flight dedup-promote readers so a
        //      committed `L→P` is observable by the scan below;
        //   2. ONE all-volume, buffer-aware `referenced_extents` scan.
        // Truly-unreferenced extents take the fast direct-free; any extent still
        // referenced is retired and left to `GcRunner::reclaim_retired_extents`
        // to re-confirm and reclaim once its last reference dies.
        for extent in &extents {
            pba_lifecycle
                .allocator()
                .wait_for_readers(extent.start, extent.count);
        }
        let pairs: Vec<(crate::types::Pba, u32)> =
            extents.iter().map(|e| (e.start, e.count)).collect();
        let referenced = match meta.referenced_extents(&pairs) {
            Ok(referenced) => referenced,
            Err(e) => {
                // Conservative on scan failure: retire (never free unverified);
                // GcRunner re-confirms on its next cycle.
                tracing::warn!(
                    error = %e,
                    extents = extents.len(),
                    "lineage drain: referenced_extents scan failed; retiring surfaced extents"
                );
                for extent in extents {
                    pba_lifecycle.retire_committed("lineage_gc_scan_err", extent);
                }
                return;
            }
        };
        for (extent, is_referenced) in extents.into_iter().zip(referenced) {
            if is_referenced {
                pba_lifecycle.retire_committed("lineage_gc_referenced", extent);
            } else {
                pba_lifecycle.free_lineage_gc_proven(extent);
            }
        }
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
