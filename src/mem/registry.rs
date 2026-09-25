//! Owner of the process's [`SlabArena`]s.
//!
//! One registry per engine. It exists so that
//!
//! - an arena is created **once per (role, lane)** and outlives every buffer it
//!   handed out, even one dropped on a foreign thread;
//! - a new consumer (compress payloads, per-cycle scratch, ...) is one
//!   [`MemRole`] variant plus one `arena()` call, not another bespoke pool;
//! - resident memory is queryable in one place instead of being spread across
//!   thread stacks.
//!
//! Per-buffer counters go to [`EngineMetrics`] rather than through the registry,
//! because that is what `onyx status` and `tools/flush_delta.py` already read.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use super::arena::SlabArena;
use crate::metrics::EngineMetrics;

/// Which pipeline stage an arena belongs to. Add a variant when a stage is
/// actually measured to need one — an unused role is an unused arena.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemRole {
    /// LV3 flush writer, one arena per buffer shard (`shard_idx` = lane).
    Lv3Writer,
    /// LV2 commit-log sync, one arena per buffer shard, owned by that shard
    /// rather than by this registry — see [`crate::mem::lv2_sync_arena`] for
    /// why. Present here because a role also selects a counter group.
    ///
    /// Measured need: that thread class issued **62 % of the box's 20.7k
    /// madvise/s**, because `encode_entries_into_spans` asks `AlignedBuf::new`
    /// for a span buffer per ~1.4 write ops and jemalloc satisfies the
    /// `alloc_zeroed` on a recycled extent by purging it — one
    /// `MADV_DONTNEED` and a TLB-shootdown IPI to all ~39 CPUs sharing the
    /// `mm`. See memory `perf_inside_engine_first_cpu_ledger`.
    Lv2Sync,
}

/// Resident totals across every arena this registry owns.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemTotals {
    pub arenas: usize,
    pub resident_bytes: usize,
    pub live_slots: u64,
}

pub struct MemRegistry {
    arenas: Mutex<HashMap<(MemRole, usize), Arc<SlabArena>>>,
    cap_bytes_per_lane: usize,
    max_class_blocks: u32,
    hugepage: bool,
    metrics: Option<Arc<EngineMetrics>>,
}

impl MemRegistry {
    /// Snapshot the tuning knobs once, so every arena in one engine has the same
    /// shape even if a knob is changed underneath at runtime — two arms have to
    /// stay comparable.
    pub fn new(metrics: Option<Arc<EngineMetrics>>) -> Arc<Self> {
        Self::with_config(
            super::arena_max_bytes_per_lane(),
            super::arena_max_class_blocks(),
            super::arena_hugepage(),
            metrics,
        )
    }

    pub fn with_config(
        cap_bytes_per_lane: usize,
        max_class_blocks: u32,
        hugepage: bool,
        metrics: Option<Arc<EngineMetrics>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            arenas: Mutex::new(HashMap::new()),
            cap_bytes_per_lane,
            max_class_blocks,
            hugepage,
            metrics,
        })
    }

    /// Get (or create) the arena for `(role, lane)`.
    ///
    /// ⚠ Call this **on the owning thread and after `affinity::bind_current`**:
    /// the first `take` maps and pre-faults memory, and pre-faulting is what puts
    /// the pages on the caller's NUMA node.
    ///
    /// This does **not** consult `mem.arena_enabled`. Handing the arena out
    /// unconditionally is what lets the A/B alternate inside one process: the
    /// enable check lives at the allocation site
    /// ([`crate::mem::arena_enabled`]), and an arena that is never taken from
    /// maps nothing, so a disabled engine still costs zero resident bytes.
    pub fn arena(&self, role: MemRole, lane: usize) -> Arc<SlabArena> {
        let mut arenas = self.arenas.lock();
        arenas
            .entry((role, lane))
            .or_insert_with(|| {
                SlabArena::new(
                    role,
                    self.cap_bytes_per_lane,
                    self.max_class_blocks,
                    self.hugepage,
                    self.metrics.clone(),
                )
            })
            .clone()
    }

    pub fn totals(&self) -> MemTotals {
        let arenas = self.arenas.lock();
        MemTotals {
            arenas: arenas.len(),
            resident_bytes: arenas.values().map(|a| a.resident_bytes()).sum(),
            live_slots: arenas.values().map(|a| a.live_slots()).sum(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_lane_shares_one_arena() {
        let registry = MemRegistry::new(None);
        let a = registry.arena(MemRole::Lv3Writer, 3);
        let b = registry.arena(MemRole::Lv3Writer, 3);
        assert!(Arc::ptr_eq(&a, &b));
        let c = registry.arena(MemRole::Lv3Writer, 4);
        assert!(!Arc::ptr_eq(&a, &c));
        assert_eq!(registry.totals().arenas, 2);
    }

    #[test]
    fn totals_track_resident_and_live_slots() {
        let registry = MemRegistry::new(None);
        let arena = registry.arena(MemRole::Lv3Writer, 0);
        assert_eq!(registry.totals().resident_bytes, 0);
        let buf = arena.take(24 * 1024).unwrap();
        let totals = registry.totals();
        assert!(totals.resident_bytes > 0);
        assert_eq!(totals.live_slots, 1);
        drop(buf);
        assert_eq!(registry.totals().live_slots, 0);
    }

    /// An arena that is never taken from maps nothing — that is what makes it
    /// safe to hand one out unconditionally and gate at the allocation site.
    #[test]
    fn an_untaken_arena_costs_nothing() {
        let registry = MemRegistry::new(None);
        for lane in 0..16 {
            registry.arena(MemRole::Lv3Writer, lane);
        }
        let totals = registry.totals();
        assert_eq!(totals.arenas, 16);
        assert_eq!(totals.resident_bytes, 0);
        assert_eq!(totals.live_slots, 0);
    }
}
