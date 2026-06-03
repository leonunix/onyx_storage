use crate::meta::schema::BlockmapValue;
use crate::types::{Lba, VolumeId};

/// A single live L2P entry handed from the GC heat-refresh walk (producer)
/// to the dedup-scanner cold-tail consumer over a bounded channel, when
/// `heat_fold_cold_tail_enabled` folds both live-L2P walks into one pass
/// (`docs/adaptive-reclaim-heatmap.md` Stage 4).
///
/// The GC walk already decodes every non-zero `BlockmapValue` to bump the
/// heat map; with the fold enabled it also emits cold candidates here so the
/// dedup scanner no longer needs its own independent `scan_blockmap_range`
/// traversal — the expensive LV3 read + hash + remap/warm stays on the dedup
/// thread, only the *target discovery* is shared.
///
/// Best-effort: the producer `try_send`s and drops on a full/disconnected
/// channel, and the consumer re-validates the mapping
/// (`same_physical_mapping`) before any remap/warm. So a dropped or stale
/// target only costs dedup ratio, never correctness.
#[derive(Debug, Clone)]
pub struct ColdTailTarget {
    pub vol_id: VolumeId,
    pub lba: Lba,
    pub bv: BlockmapValue,
}
