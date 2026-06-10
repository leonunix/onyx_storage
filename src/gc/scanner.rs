use std::collections::HashMap;

use crate::error::OnyxResult;
use crate::meta::schema::{decode_blockmap_key, decode_blockmap_value, BlockmapValue}; // decode_blockmap_key now returns Option<Lba>
use crate::meta::store::MetaStore;
use crate::types::{Lba, Pba, VolumeId};

/// A compression unit identified as having dead blocks worth reclaiming.
#[derive(Debug, Clone)]
pub struct GcCandidate {
    pub pba: Pba,
    pub vol_id: VolumeId,
    pub compression: u8,
    pub unit_compressed_size: u32,
    pub unit_original_size: u32,
    pub unit_lba_count: u16,
    pub crc32: u32,
    pub slot_offset: u16,
    /// LBAs still live (still point to this PBA).
    pub live_lbas: Vec<(Lba, u16)>,
    /// Ratio of dead blocks: 1.0 - (live_count / unit_lba_count).
    pub dead_ratio: f64,
}

/// Composite key to distinguish fragments within a packed slot.
/// Two fragments sharing the same PBA must be differentiated by their full
/// fragment identity, not just byte range size.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct FragmentKey {
    pba: Pba,
    slot_offset: u16,
    compression: u8,
    unit_compressed_size: u32,
    unit_original_size: u32,
    unit_lba_count: u16,
    crc32: u32,
}

/// Info collected per fragment during scanning.
struct FragmentInfo {
    vol_id: String,
    unit_lba_count: u16,
    compression: u8,
    unit_compressed_size: u32,
    unit_original_size: u32,
    crc32: u32,
    slot_offset: u16,
    pba: Pba,
    /// LBAs currently pointing to this fragment, with their offset_in_unit.
    live_lbas: Vec<(Lba, u16)>,
}

/// Aggregates blockmap entries by fragment so dead-block ratios can be computed
/// per compression unit. Shared by the full-volume scan ([`scan_gc_candidates`])
/// and the bounded windowed scan ([`scan_gc_candidates_window`]) used by the
/// resident compactor — `observe` per entry, then `finalize` once.
#[derive(Default)]
struct FragmentAccumulator {
    frags: HashMap<FragmentKey, FragmentInfo>,
}

impl FragmentAccumulator {
    /// Record one live blockmap entry. Single-LBA units are skipped (they can
    /// never have dead blocks). Order-independent, so a shard-internal-ordered
    /// range walk is fine.
    fn observe(&mut self, vol_id: &str, lba: Lba, bv: &BlockmapValue) {
        if bv.unit_lba_count <= 1 {
            return;
        }
        let fkey = FragmentKey {
            pba: bv.pba,
            slot_offset: bv.slot_offset,
            compression: bv.compression,
            unit_compressed_size: bv.unit_compressed_size,
            unit_original_size: bv.unit_original_size,
            unit_lba_count: bv.unit_lba_count,
            crc32: bv.crc32,
        };
        let info = self.frags.entry(fkey).or_insert_with(|| FragmentInfo {
            vol_id: vol_id.to_string(),
            unit_lba_count: bv.unit_lba_count,
            compression: bv.compression,
            unit_compressed_size: bv.unit_compressed_size,
            unit_original_size: bv.unit_original_size,
            crc32: bv.crc32,
            slot_offset: bv.slot_offset,
            pba: bv.pba,
            live_lbas: Vec::new(),
        });
        info.live_lbas.push((lba, bv.offset_in_unit));
    }

    /// Estimated compactable dead blocks across every observed multi-LBA
    /// fragment: `sum(unit_lba_count - live_count)`, computed BEFORE the
    /// threshold filter so it reflects total slack (debt), not just the units
    /// picked this cycle. Best-effort: a unit straddling two scan windows is
    /// counted partially in each (negligible at ~1M-LBA windows vs ≤32-LBA units).
    fn dead_estimate(&self) -> u64 {
        self.frags
            .values()
            .map(|info| (info.unit_lba_count as u64).saturating_sub(info.live_lbas.len() as u64))
            .sum()
    }

    /// Turn the accumulated fragments into candidates with `dead_ratio >=
    /// threshold`, sorted worst-first, truncated to `max_results`.
    fn finalize(self, threshold: f64, max_results: usize) -> Vec<GcCandidate> {
        let mut candidates: Vec<GcCandidate> = self
            .frags
            .into_iter()
            .filter_map(|(_, info)| {
                let live_count = info.live_lbas.len() as f64;
                let total = info.unit_lba_count as f64;
                let dead_ratio = 1.0 - (live_count / total);
                if dead_ratio >= threshold {
                    Some(GcCandidate {
                        pba: info.pba,
                        vol_id: VolumeId(info.vol_id),
                        compression: info.compression,
                        unit_compressed_size: info.unit_compressed_size,
                        unit_original_size: info.unit_original_size,
                        unit_lba_count: info.unit_lba_count,
                        crc32: info.crc32,
                        slot_offset: info.slot_offset,
                        live_lbas: info.live_lbas,
                        dead_ratio,
                    })
                } else {
                    None
                }
            })
            .collect();

        // Sort by dead ratio descending (worst fragmentation first)
        candidates.sort_by(|a, b| b.dead_ratio.partial_cmp(&a.dead_ratio).unwrap());
        candidates.truncate(max_results);
        candidates
    }
}

/// Scan all blockmap entries to find compression units with high dead ratios.
///
/// Aggregates by (pba, slot_offset, unit_compressed_size) to correctly
/// distinguish multiple fragments packed into the same 4KB physical slot.
///
/// Returns candidates sorted by dead_ratio descending, up to `max_results`.
///
/// This is the unbounded full-volume-set scan (~all L2P). The resident
/// compactor uses [`scan_gc_candidates_window`] instead; this entry point is
/// retained for tests and one-shot tooling.
pub fn scan_gc_candidates(
    meta: &MetaStore,
    threshold: f64,
    max_results: usize,
) -> OnyxResult<Vec<GcCandidate>> {
    let mut acc = FragmentAccumulator::default();

    // Iterate all blockmap entries across all volume CFs
    meta.scan_all_blockmap_entries(&mut |vol_id_str: &str, key: &[u8], val: &[u8]| {
        let lba = match decode_blockmap_key(key) {
            Some(v) => v,
            None => return,
        };
        let bv = match decode_blockmap_value(val) {
            Some(v) => v,
            None => return,
        };
        acc.observe(vol_id_str, lba, &bv);
    })?;

    Ok(acc.finalize(threshold, max_results))
}

/// Bounded, resumable candidate scan over one volume's LBA window
/// `[start_lba, start_lba + count)`. Drives the resident background compactor:
/// each GC cycle scans ~1M LBAs instead of the full ~80M-entry set, advancing a
/// per-volume lap cursor across cycles.
///
/// Returns `(candidates, dead_estimate)` where `dead_estimate` is the
/// compactable-debt estimate for this window (see [`FragmentAccumulator::dead_estimate`]).
///
/// A compression unit's member LBAs are confined to a contiguous range
/// (≤ `coalesce_max_lbas`), so a unit fully inside the window is evaluated
/// exactly; a unit split by the window boundary undercounts its live members →
/// OVERSTATES dead_ratio → it may be picked one lap early, never wrongly
/// dropped, and `rewrite_candidate` re-validates every LBA against the live
/// blockmap before acting.
pub fn scan_gc_candidates_window(
    meta: &MetaStore,
    vol_id: &VolumeId,
    start_lba: Lba,
    count: u64,
    threshold: f64,
    max_results: usize,
) -> OnyxResult<(Vec<GcCandidate>, u64)> {
    let mut acc = FragmentAccumulator::default();
    meta.scan_blockmap_range(vol_id, start_lba, count, &mut |lba, bv| {
        acc.observe(&vol_id.0, lba, &bv);
    })?;
    let dead_estimate = acc.dead_estimate();
    Ok((acc.finalize(threshold, max_results), dead_estimate))
}
