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
/// allocator's free list — via [`PbaLifecycle::free_lineage_gc_proven`], so
/// the RAM candidate cache is evicted before the allocator can reissue the
/// PBA and duplicate `FreePbas` surfaces are absorbed idempotently.
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
        for extent in extents {
            pba_lifecycle.free_lineage_gc_proven(extent);
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
