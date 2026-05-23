use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::meta::store::MetaStore;
use crate::metrics::EngineMetrics;
use crate::space::allocator::SpaceAllocator;

/// Handle for the lineage-freed PBA drain thread.
///
/// Phase 5 ([[no-refcount-hot-path-design]]) moves PBA retirement off the
/// hot write path: the engine's commit path no longer maintains rc, so
/// retiring an exclusive PBA happens later, when Lineage GC clears its
/// dead-list segment past every snap_pin and emits a `WalOp::FreePbas`.
/// The metadb backend converts those WAL outcomes into a crossbeam
/// channel; this thread drains the channel and returns each PBA to the
/// allocator's free list.
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
        allocator: Arc<SpaceAllocator>,
        metrics: Arc<EngineMetrics>,
        interval: std::time::Duration,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = stop.clone();
        let thread = std::thread::Builder::new()
            .name("onyx-lineage-drain".into())
            .spawn(move || {
                while !stop_clone.load(Ordering::Relaxed) {
                    Self::drain_once(&meta, &allocator, &metrics);
                    std::thread::sleep(interval);
                }
                // Final drain so PBAs the GC emitted right before
                // shutdown still make it back to the allocator.
                Self::drain_once(&meta, &allocator, &metrics);
            })
            .expect("spawn lineage drain thread");
        Self {
            stop,
            thread: Some(thread),
        }
    }

    fn drain_once(
        meta: &Arc<MetaStore>,
        allocator: &Arc<SpaceAllocator>,
        metrics: &Arc<EngineMetrics>,
    ) {
        let pbas = meta.drain_lineage_freed_pbas();
        if pbas.is_empty() {
            return;
        }
        let extents = crate::meta::backend::coalesce_free_pbas_to_extents(&pbas);
        let mut freed_blocks: u64 = 0;
        for extent in extents {
            let result = if extent.count <= 1 {
                allocator.free_one(extent.start)
            } else {
                allocator.free_extent(extent)
            };
            if let Err(e) = result {
                tracing::warn!(
                    pba = extent.start.0,
                    blocks = extent.count,
                    error = %e,
                    "lineage drain: allocator free failed; PBA leaked until restart",
                );
            } else {
                freed_blocks += extent.count as u64;
            }
        }
        if freed_blocks > 0 {
            metrics
                .gc_lineage_freed_blocks
                .fetch_add(freed_blocks, Ordering::Relaxed);
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
