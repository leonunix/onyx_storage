use std::collections::{HashMap, HashSet};

use crate::error::OnyxResult;
use crate::meta::schema::{decode_blockmap_key, decode_blockmap_value, BlockmapValue}; // decode_blockmap_key now returns Option<Lba>
use crate::meta::store::MetaStore;
use crate::types::{Lba, Pba, VolumeId, BLOCK_SIZE};

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

/// Runtime parameters for slot-aware compaction (`compactor_slot_evac_enabled`).
///
/// When `enabled`, the candidate scan additionally targets *whole packed 4 KiB
/// slots that are mostly dead by bytes*, promoting their few live fragments so
/// the slot can be evacuated and freed. The per-fragment dead-ratio path selects
/// the *dead* fragment (which frees nothing — the slot is pinned by a live
/// sibling); this selects the *live* fragment that pins the slot. Inert unless
/// rc is authoritative (the caller folds `rc_authoritative_reclaim` into
/// `enabled`), because the completeness check relies on `rc(P)` == the slot's
/// live-LBA count.
#[derive(Debug, Clone, Copy)]
pub struct SlotEvacParams {
    pub enabled: bool,
    /// Max live blocks a packed slot may hold and still be evacuated (the caller
    /// has already clamped this to the per-cycle rewrite budget).
    pub max_live: u16,
    pub block_size: u32,
}

impl SlotEvacParams {
    /// Slot-evac off (legacy per-fragment selection only).
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            max_live: 0,
            block_size: BLOCK_SIZE,
        }
    }
}

/// Per-cycle slot-evac selection stats (folded into engine metrics by the
/// compactor). `candidates` = fragments promoted for slot evacuation; `blocks` =
/// live blocks selected for evacuation (the rewrite cost). The two skip counters
/// are kept distinct because they have opposite implications:
/// - `incomplete_skips` = byte-dead slot whose window did NOT see all of its
///   live references (`rc != visible_live`: a sibling fragment in another scan
///   window, or transient rc-apply lag). The view is incomplete → the Phase-2
///   sweep accumulator (complete-by-construction) WOULD reclaim these.
/// - `cost_cap_skips` = byte-dead slot seen completely (`rc == visible_live`)
///   but pinned by more than `max_live` live blocks → too expensive to relocate.
///   Phase 2 does NOT help these; they need a different policy (raise the cap, or
///   accept the slack).
#[derive(Debug, Default, Clone, Copy)]
pub struct SlotEvacStats {
    pub candidates: u64,
    pub blocks: u64,
    pub incomplete_skips: u64,
    pub cost_cap_skips: u64,
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

impl FragmentInfo {
    /// Does this fragment occupy exactly one physical 4 KiB slot (packed
    /// fragment), as opposed to a multi-PBA passthrough unit? Mirrors
    /// `BlockmapValue::physical_blocks(block_size) == 1`.
    fn is_packed(&self, block_size: u32) -> bool {
        self.slot_offset > 0 || self.unit_compressed_size < block_size
    }

    fn dead_ratio(&self) -> f64 {
        let total = self.unit_lba_count.max(1) as f64;
        1.0 - (self.live_lbas.len() as f64 / total)
    }

    fn into_candidate(self) -> GcCandidate {
        let dead_ratio = self.dead_ratio();
        GcCandidate {
            pba: self.pba,
            vol_id: VolumeId(self.vol_id),
            compression: self.compression,
            unit_compressed_size: self.unit_compressed_size,
            unit_original_size: self.unit_original_size,
            unit_lba_count: self.unit_lba_count,
            crc32: self.crc32,
            slot_offset: self.slot_offset,
            live_lbas: self.live_lbas,
            dead_ratio,
        }
    }
}

/// Aggregates blockmap entries by fragment so dead-block ratios can be computed
/// per compression unit. Shared by the full-volume scan ([`scan_gc_candidates`])
/// and the bounded windowed scan ([`scan_gc_candidates_window`]) used by the
/// resident compactor — `observe` per entry, then `finalize` once.
struct FragmentAccumulator {
    frags: HashMap<FragmentKey, FragmentInfo>,
    /// When true, single-LBA fragments are also observed (the slot-evac pass
    /// needs them — a live single-LBA compressed block can pin a packed slot).
    /// When false, the legacy per-fragment path skips them (they can never have
    /// a partial dead ratio, so they were dropped as an optimization).
    slot_evac: bool,
}

impl FragmentAccumulator {
    fn new(slot_evac: bool) -> Self {
        Self {
            frags: HashMap::new(),
            slot_evac,
        }
    }

    /// Record one live blockmap entry. Zero mappings are always skipped.
    /// Single-LBA units are skipped unless slot-evac is on (they can never have
    /// dead blocks for the per-fragment path). Order-independent, so a
    /// shard-internal-ordered range walk is fine.
    fn observe(&mut self, vol_id: &str, lba: Lba, bv: &BlockmapValue) {
        if bv.is_zero() {
            return;
        }
        if !self.slot_evac && bv.unit_lba_count <= 1 {
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

    /// Turn the accumulated fragments into rewrite candidates.
    ///
    /// Without slot-evac: keep fragments with `dead_ratio >= threshold`, sorted
    /// worst-first, truncated to `max_results` (legacy behavior).
    ///
    /// With slot-evac (`slot_evac.enabled`): FIRST select whole packed slots
    /// that are mostly dead by bytes and whose window saw all their live
    /// references (`rc == visible_live`), promoting every fragment of such a slot
    /// (atomic w.r.t. the `max_results` budget). THEN fill any remaining budget
    /// with the legacy per-fragment candidates (dead-ratio path), de-duplicated
    /// against the slot-evac fragments by identity.
    fn finalize(
        self,
        threshold: f64,
        max_results: usize,
        slot_evac: SlotEvacParams,
        meta: &MetaStore,
    ) -> OnyxResult<(Vec<GcCandidate>, SlotEvacStats)> {
        if !slot_evac.enabled {
            return Ok((
                self.finalize_per_fragment(threshold, max_results),
                SlotEvacStats::default(),
            ));
        }
        self.finalize_with_slot_evac(threshold, max_results, slot_evac, meta)
    }

    /// Legacy per-fragment selection (also used when slot-evac is off).
    fn finalize_per_fragment(self, threshold: f64, max_results: usize) -> Vec<GcCandidate> {
        let mut candidates: Vec<GcCandidate> = self
            .frags
            .into_values()
            .filter_map(|info| {
                if info.dead_ratio() >= threshold {
                    Some(info.into_candidate())
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

    fn finalize_with_slot_evac(
        self,
        threshold: f64,
        max_results: usize,
        slot_evac: SlotEvacParams,
        meta: &MetaStore,
    ) -> OnyxResult<(Vec<GcCandidate>, SlotEvacStats)> {
        let block_size = slot_evac.block_size.max(1);
        let max_live = slot_evac.max_live as usize;
        let mut stats = SlotEvacStats::default();

        // Group packed fragments (physical_blocks == 1) by their shared PBA, and
        // aggregate per-slot occupancy: live LBA count and live compressed bytes
        // (summed over DISTINCT fragments — N live LBAs of one fragment share one
        // byte range). Multi-PBA passthrough units and zero mappings never enter.
        let mut keys_by_pba: HashMap<Pba, Vec<FragmentKey>> = HashMap::new();
        let mut occ_by_pba: HashMap<Pba, (usize, u64)> = HashMap::new(); // (live_lbas, live_bytes)
        for (key, info) in self.frags.iter() {
            if !info.is_packed(block_size) {
                continue;
            }
            keys_by_pba.entry(info.pba).or_default().push(*key);
            let entry = occ_by_pba.entry(info.pba).or_insert((0, 0));
            entry.0 += info.live_lbas.len();
            entry.1 += info.unit_compressed_size as u64;
        }

        // Byte-deadness ROI gate: a slot is worth evacuating only when its live
        // bytes are a small fraction of the physical 4 KiB (so re-packing the
        // live data costs little vs the slot freed). Gate first, then read rc
        // only for the survivors.
        let gated_pbas: Vec<Pba> = occ_by_pba
            .iter()
            .filter(|(_, (_, live_bytes))| {
                1.0 - (*live_bytes as f64) / (block_size as f64) >= threshold
            })
            .map(|(pba, _)| *pba)
            .collect();

        // Completeness gate: `rc(P) == visible_live_lbas` proves this window saw
        // ALL live references to the slot (under rc-authoritative rc(P) counts
        // live L2P refs to P). If a sibling fragment lives in another window /
        // straddles the boundary, rc > visible → defer (counted), caught later.
        let mut promote_pbas: Vec<Pba> = Vec::new();
        if !gated_pbas.is_empty() {
            let refcounts = meta.multi_get_refcounts(&gated_pbas)?;
            for (pba, rc) in gated_pbas.iter().zip(refcounts) {
                let (live_lbas, _) = occ_by_pba[pba];
                if live_lbas == 0 {
                    continue; // degenerate (no live fragment) — not a real skip
                } else if rc as usize != live_lbas {
                    // Incomplete view: this window did not see all of the slot's
                    // live refs (cross-window sibling, or rc-apply lag). Phase-2
                    // sweep accumulation would close this.
                    stats.incomplete_skips += 1;
                } else if live_lbas > max_live {
                    // Complete view, but too many live blocks pin the slot to be
                    // worth relocating. Phase 2 does NOT help these.
                    stats.cost_cap_skips += 1;
                } else {
                    promote_pbas.push(*pba);
                }
            }
        }

        // Fill the budget with whole slots, highest byte-deadness first. A slot
        // is all-or-nothing: a partially-evacuated slot never reaches rc→0, so a
        // truncation that split it would be pure wasted IO.
        promote_pbas.sort_by(|a, b| {
            let da = 1.0 - occ_by_pba[a].1 as f64 / block_size as f64;
            let db = 1.0 - occ_by_pba[b].1 as f64 / block_size as f64;
            db.partial_cmp(&da).unwrap()
        });
        let mut promoted_keys: HashSet<FragmentKey> = HashSet::new();
        let mut promoted_count = 0usize;
        for pba in &promote_pbas {
            let keys = &keys_by_pba[pba];
            if promoted_count + keys.len() > max_results {
                continue; // whole slot would not fit this cycle's budget
            }
            for key in keys {
                promoted_keys.insert(*key);
            }
            promoted_count += keys.len();
            stats.candidates += keys.len() as u64;
            stats.blocks += occ_by_pba[pba].0 as u64;
        }

        // Materialize: promoted fragments become candidates; the rest feed the
        // legacy per-fragment pass (so a fragment is never emitted twice).
        let mut candidates: Vec<GcCandidate> = Vec::with_capacity(promoted_count);
        let mut remaining: Vec<FragmentInfo> = Vec::new();
        for (key, info) in self.frags {
            if promoted_keys.contains(&key) {
                candidates.push(info.into_candidate());
            } else {
                remaining.push(info);
            }
        }

        let room = max_results.saturating_sub(candidates.len());
        if room > 0 {
            let mut frag_cands: Vec<GcCandidate> = remaining
                .into_iter()
                .filter_map(|info| {
                    if info.dead_ratio() >= threshold {
                        Some(info.into_candidate())
                    } else {
                        None
                    }
                })
                .collect();
            frag_cands.sort_by(|a, b| b.dead_ratio.partial_cmp(&a.dead_ratio).unwrap());
            frag_cands.truncate(room);
            candidates.extend(frag_cands);
        }

        Ok((candidates, stats))
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
/// retained for tests and one-shot tooling. Slot-evac is OFF here (the legacy
/// per-fragment path); the resident compactor opts in via the window scan.
pub fn scan_gc_candidates(
    meta: &MetaStore,
    threshold: f64,
    max_results: usize,
) -> OnyxResult<Vec<GcCandidate>> {
    let mut acc = FragmentAccumulator::new(false);

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

    let (candidates, _stats) =
        acc.finalize(threshold, max_results, SlotEvacParams::disabled(), meta)?;
    Ok(candidates)
}

/// Bounded, resumable candidate scan over one volume's LBA window
/// `[start_lba, start_lba + count)`. Drives the resident background compactor:
/// each GC cycle scans ~1M LBAs instead of the full ~80M-entry set, advancing a
/// per-volume lap cursor across cycles.
///
/// Returns `(candidates, dead_estimate, slot_evac_stats)` where `dead_estimate`
/// is the compactable-debt estimate for this window (see
/// [`FragmentAccumulator::dead_estimate`]) and `slot_evac_stats` reports the
/// slot-aware selection (empty when `slot_evac.enabled == false`).
///
/// A compression unit's member LBAs are confined to a contiguous range
/// (≤ `coalesce_max_lbas`), so a unit fully inside the window is evaluated
/// exactly; a unit split by the window boundary undercounts its live members →
/// OVERSTATES dead_ratio → it may be picked one lap early, never wrongly
/// dropped, and `rewrite_candidate` re-validates every LBA against the live
/// blockmap before acting. For slot-evac, a boundary split makes `rc > visible`
/// → the slot is deferred (counted in `incomplete_skips`), never mis-evacuated.
pub fn scan_gc_candidates_window(
    meta: &MetaStore,
    vol_id: &VolumeId,
    start_lba: Lba,
    count: u64,
    threshold: f64,
    max_results: usize,
    slot_evac: SlotEvacParams,
) -> OnyxResult<(Vec<GcCandidate>, u64, SlotEvacStats)> {
    let mut acc = FragmentAccumulator::new(slot_evac.enabled);
    meta.scan_blockmap_range(vol_id, start_lba, count, &mut |lba, bv| {
        acc.observe(&vol_id.0, lba, &bv);
    })?;
    let dead_estimate = acc.dead_estimate();
    let (candidates, stats) = acc.finalize(threshold, max_results, slot_evac, meta)?;
    Ok((candidates, dead_estimate, stats))
}
