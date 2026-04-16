use std::collections::HashMap;

use crate::error::OnyxResult;
use crate::meta::schema::{decode_blockmap_key, decode_blockmap_value}; // decode_blockmap_key now returns Option<Lba>
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

/// Scan all blockmap entries to find compression units with high dead ratios.
///
/// Aggregates by (pba, slot_offset, unit_compressed_size) to correctly
/// distinguish multiple fragments packed into the same 4KB physical slot.
///
/// Returns candidates sorted by dead_ratio descending, up to `max_results`.
pub fn scan_gc_candidates(
    meta: &MetaStore,
    threshold: f64,
    max_results: usize,
) -> OnyxResult<Vec<GcCandidate>> {
    let mut frag_map: HashMap<FragmentKey, FragmentInfo> = HashMap::new();

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

        // Only consider multi-LBA compression units (single-LBA units have no dead blocks)
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

        let info = frag_map.entry(fkey).or_insert_with(|| FragmentInfo {
            vol_id: vol_id_str.to_string(),
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
    })?;

    // Filter by dead ratio
    let mut candidates: Vec<GcCandidate> = frag_map
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

    Ok(candidates)
}
