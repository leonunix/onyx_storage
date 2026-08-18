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
    enabled: bool,
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
            super::arena_enabled(),
            super::arena_max_bytes_per_lane(),
            super::arena_max_class_blocks(),
            super::arena_hugepage(),
            metrics,
        )
    }

    pub fn with_config(
        enabled: bool,
        cap_bytes_per_lane: usize,
        max_class_blocks: u32,
        hugepage: bool,
        metrics: Option<Arc<EngineMetrics>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            arenas: Mutex::new(HashMap::new()),
            enabled,
            cap_bytes_per_lane,
            max_class_blocks,
            hugepage,
            metrics,
        })
    }

    /// Get (or create) the arena for `(role, lane)`.
    ///
    /// ⚠ Call this **on the owning thread and after `affinity::bind_current`**:
    /// the first call maps and pre-faults memory, and pre-faulting is what puts
    /// the pages on the caller's NUMA node.
    ///
    /// Returns `None` when arenas are disabled, so the caller keeps its previous
    /// heap path — that is the A/B baseline and it needs no rebuild.
    pub fn arena(&self, role: MemRole, lane: usize) -> Option<Arc<SlabArena>> {
        if !self.enabled {
            return None;
        }
        let mut arenas = self.arenas.lock();
        Some(
            arenas
                .entry((role, lane))
                .or_insert_with(|| {
                    SlabArena::new(
                        self.cap_bytes_per_lane,
                        self.max_class_blocks,
                        self.hugepage,
                        self.metrics.clone(),
                    )
                })
                .clone(),
        )
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
        let a = registry.arena(MemRole::Lv3Writer, 3).unwrap();
        let b = registry.arena(MemRole::Lv3Writer, 3).unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        let c = registry.arena(MemRole::Lv3Writer, 4).unwrap();
        assert!(!Arc::ptr_eq(&a, &c));
        assert_eq!(registry.totals().arenas, 2);
    }

    #[test]
    fn totals_track_resident_and_live_slots() {
        let registry = MemRegistry::new(None);
        let arena = registry.arena(MemRole::Lv3Writer, 0).unwrap();
        assert_eq!(registry.totals().resident_bytes, 0);
        let buf = arena.take(24 * 1024).unwrap();
        let totals = registry.totals();
        assert!(totals.resident_bytes > 0);
        assert_eq!(totals.live_slots, 1);
        drop(buf);
        assert_eq!(registry.totals().live_slots, 0);
    }

    /// The A/B baseline arm: `mem.arena_enabled = false` gives every caller its
    /// old heap path back without a rebuild. Configured per registry, never read
    /// from a global here, so this test cannot race the rest of the suite.
    #[test]
    fn disabled_registry_hands_out_no_arena() {
        let disabled = MemRegistry::with_config(false, 64 << 20, 64, false, None);
        assert!(disabled.arena(MemRole::Lv3Writer, 0).is_none());
        assert_eq!(disabled.totals(), MemTotals::default());
        let enabled = MemRegistry::with_config(true, 64 << 20, 64, false, None);
        assert!(enabled.arena(MemRole::Lv3Writer, 0).is_some());
    }
}
