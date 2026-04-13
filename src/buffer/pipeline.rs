use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::buffer::commit_log::PendingEntry;
use crate::buffer::entry::BufferEntry;
use crate::meta::schema::{BlockmapValue, ContentHash};
use crate::types::{CompressionAlgo, Lba, BLOCK_SIZE};

/// Tracks completion of sub-units produced by dedup splitting.
///
/// When dedup splits a coalesced unit into N miss sub-units, it creates a
/// DedupCompletion with `remaining = N` and the original unit's full seqs.
/// Each sub-unit carries an `Arc<DedupCompletion>`. When the writer finishes
/// a sub-unit, it calls `decrement()`. The last one to reach 0 gets back
/// the seqs to send via done_tx.
#[derive(Debug)]
pub struct DedupCompletion {
    remaining: AtomicU32,
    /// All seqs from the original coalesced unit (for done_tx).
    seqs: Vec<u64>,
}

impl DedupCompletion {
    pub fn new(count: u32, seqs: Vec<u64>) -> Arc<Self> {
        Arc::new(Self {
            remaining: AtomicU32::new(count),
            seqs,
        })
    }

    /// Decrement the counter. If this was the last sub-unit (counter reaches 0),
    /// returns the seqs that should be sent to done_tx.
    pub fn decrement(&self) -> Option<Vec<u64>> {
        let prev = self.remaining.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            // We were the last one
            Some(self.seqs.clone())
        } else {
            None
        }
    }

    pub fn seqs(&self) -> &[u64] {
        &self.seqs
    }
}

/// A group of contiguous LBAs from the same volume, merged from buffer entries.
/// Ready to be compressed as a single unit.
#[derive(Debug, Clone)]
pub struct CoalesceUnit {
    pub vol_id: String,
    pub start_lba: Lba,
    pub lba_count: u32,
    pub raw_data: Vec<u8>,
    /// Per-volume compression algorithm (from VolumeConfig metadata).
    pub compression: CompressionAlgo,
    /// Volume generation epoch from the buffer entries.
    pub vol_created_at: u64,
    /// (seq, first_lba_from_this_entry_in_unit, count) — tracks exactly which
    /// LBAs from each original entry are in this unit. Needed for idempotent
    /// partial mark_flushed when a multi-LBA entry is split across units.
    pub seq_lba_ranges: Vec<(u64, Lba, u32)>,
    /// True if dedup was skipped under backpressure (buffer > threshold).
    pub dedup_skipped: bool,
    /// SHA-256 hashes per 4KB block, populated by dedup stage for miss blocks.
    /// None if dedup not yet run or skipped.
    pub block_hashes: Option<Vec<ContentHash>>,
    /// When set, this sub-unit is part of a dedup split. The writer calls
    /// `decrement()` on completion; the last sub-unit to finish triggers done_tx
    /// with the original unit's full seqs. When None, the writer sends done_tx
    /// directly using this unit's own seq_lba_ranges (normal non-dedup path).
    pub dedup_completion: Option<Arc<DedupCompletion>>,
}

/// A dedup hit: this 4KB block matches an existing entry in the dedup index.
/// Sent from dedup workers to the writer thread for metadata-only update.
#[derive(Debug, Clone)]
pub struct DedupHit {
    pub vol_id: String,
    pub lba: Lba,
    pub content_hash: ContentHash,
    pub existing_value: BlockmapValue,
    pub vol_created_at: u64,
    pub seq_lba_ranges: Vec<(u64, Lba, u32)>,
}

/// A compressed coalesce unit, ready to be written to LV3.
#[derive(Debug, Clone)]
pub struct CompressedUnit {
    pub vol_id: String,
    pub start_lba: Lba,
    pub lba_count: u32,
    pub original_size: u32,
    pub compressed_data: Vec<u8>,
    pub compression: u8,
    pub crc32: u32,
    /// Volume generation epoch from the buffer entries.
    pub vol_created_at: u64,
    pub seq_lba_ranges: Vec<(u64, Lba, u32)>,
    /// SHA-256 hashes per 4KB block (one per LBA), populated by dedup stage.
    /// None if dedup was skipped under backpressure.
    pub block_hashes: Option<Vec<ContentHash>>,
    /// True if dedup was skipped under backpressure.
    pub dedup_skipped: bool,
    /// See CoalesceUnit::dedup_completion.
    pub dedup_completion: Option<Arc<DedupCompletion>>,
}

/// A single-LBA slice extracted from a (possibly multi-LBA) buffer entry.
struct LbaSlice<'a> {
    vol_id: &'a str,
    lba: Lba,
    data: &'a [u8], // exactly BLOCK_SIZE bytes
    entry_seq: u64,
    vol_created_at: u64,
}

/// Record that `lba` from entry `seq` is in this unit.
/// Tracks contiguous ranges per seq: if seq already has a range ending at lba-1,
/// extend it; otherwise start a new range.
fn add_seq_lba(acc: &mut Vec<(u64, Lba, u32)>, seq: u64, lba: Lba) {
    if let Some(existing) = acc
        .iter_mut()
        .find(|(s, start, count)| *s == seq && start.0 + *count as u64 == lba.0)
    {
        existing.2 += 1;
    } else {
        acc.push((seq, lba, 1));
    }
}

/// Coalesce buffer entries into compression units.
///
/// Multi-LBA entries are expanded into per-LBA slices for dedup, then
/// contiguous same-volume LBAs are merged up to the configured limits.
///
/// `vol_compression` returns the compression algorithm for a given volume ID.
/// If the volume is not found (e.g., deleted while entries were pending),
/// `CompressionAlgo::None` is used as fallback.
pub fn coalesce(
    entries: &[BufferEntry],
    max_raw_bytes: usize,
    max_lbas: u32,
    vol_compression: &dyn Fn(&str) -> CompressionAlgo,
) -> Vec<CoalesceUnit> {
    let bs = BLOCK_SIZE as usize;

    let mut all_slices: Vec<LbaSlice> = Vec::new();
    for entry in entries {
        for i in 0..entry.lba_count {
            let offset = i as usize * bs;
            let end = offset + bs;
            if end <= entry.payload.len() {
                all_slices.push(LbaSlice {
                    vol_id: &entry.vol_id,
                    lba: Lba(entry.start_lba.0 + i as u64),
                    data: &entry.payload[offset..end],
                    entry_seq: entry.seq,
                    vol_created_at: entry.vol_created_at,
                });
            }
        }
    }
    coalesce_slices(all_slices, max_raw_bytes, max_lbas, vol_compression)
}

pub fn coalesce_pending(
    entries: &[Arc<PendingEntry>],
    max_raw_bytes: usize,
    max_lbas: u32,
    vol_compression: &dyn Fn(&str) -> CompressionAlgo,
    skip_offsets: &HashMap<u64, HashSet<u16>>,
) -> Vec<CoalesceUnit> {
    let bs = BLOCK_SIZE as usize;

    let mut all_slices: Vec<LbaSlice> = Vec::new();
    for entry in entries {
        let Some(ref payload) = entry.payload else {
            continue;
        };
        let skip = skip_offsets.get(&entry.seq);
        for i in 0..entry.lba_count {
            if let Some(flushed) = skip {
                if flushed.contains(&(i as u16)) {
                    continue;
                }
            }
            let offset = i as usize * bs;
            let end = offset + bs;
            if end <= payload.len() {
                all_slices.push(LbaSlice {
                    vol_id: &entry.vol_id,
                    lba: Lba(entry.start_lba.0 + i as u64),
                    data: &payload[offset..end],
                    entry_seq: entry.seq,
                    vol_created_at: entry.vol_created_at,
                });
            }
        }
    }
    coalesce_slices(all_slices, max_raw_bytes, max_lbas, vol_compression)
}

fn coalesce_slices(
    all_slices: Vec<LbaSlice<'_>>,
    max_raw_bytes: usize,
    max_lbas: u32,
    vol_compression: &dyn Fn(&str) -> CompressionAlgo,
) -> Vec<CoalesceUnit> {
    if all_slices.is_empty() {
        return Vec::new();
    }
    let bs = BLOCK_SIZE as usize;

    // Phase 2: Dedup — keep latest (highest seq) per (vol_id, lba).
    // Collect all seqs per key for flushing.
    struct DedupSlice<'a> {
        latest: LbaSlice<'a>,
        all_seqs: Vec<u64>,
    }

    let mut deduped: std::collections::HashMap<(String, u64), DedupSlice> =
        std::collections::HashMap::new();
    for slice in all_slices {
        let key = (slice.vol_id.to_string(), slice.lba.0);
        match deduped.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                let existing = e.get_mut();
                existing.all_seqs.push(slice.entry_seq);
                if slice.entry_seq > existing.latest.entry_seq {
                    existing.latest = slice;
                }
            }
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(DedupSlice {
                    all_seqs: vec![slice.entry_seq],
                    latest: slice,
                });
            }
        }
    }

    // Phase 3: Sort by (vol_id, lba)
    let mut sorted: Vec<&DedupSlice> = deduped.values().collect();
    sorted.sort_by(|a, b| {
        (&a.latest.vol_id, a.latest.lba.0).cmp(&(&b.latest.vol_id, b.latest.lba.0))
    });

    // Phase 4: Merge contiguous LBAs
    let mut units = Vec::new();
    let mut current: Option<CoalesceUnit> = None;

    for ds in sorted {
        let should_extend = if let Some(ref cur) = current {
            cur.vol_id == ds.latest.vol_id
                && ds.latest.lba.0 == cur.start_lba.0 + cur.lba_count as u64
                && cur.raw_data.len() + bs <= max_raw_bytes
                && cur.lba_count + 1 <= max_lbas
        } else {
            false
        };

        if should_extend {
            let cur = current.as_mut().unwrap();
            cur.lba_count += 1;
            cur.raw_data.extend_from_slice(ds.latest.data);
            for &seq in &ds.all_seqs {
                add_seq_lba(&mut cur.seq_lba_ranges, seq, ds.latest.lba);
            }
        } else {
            if let Some(unit) = current.take() {
                units.push(unit);
            }
            let mut seq_lba_ranges = Vec::new();
            for &seq in &ds.all_seqs {
                add_seq_lba(&mut seq_lba_ranges, seq, ds.latest.lba);
            }
            current = Some(CoalesceUnit {
                vol_id: ds.latest.vol_id.to_string(),
                start_lba: ds.latest.lba,
                lba_count: 1,
                raw_data: ds.latest.data.to_vec(),
                compression: vol_compression(ds.latest.vol_id),
                vol_created_at: ds.latest.vol_created_at,
                seq_lba_ranges,
                dedup_skipped: false,
                block_hashes: None,
                dedup_completion: None,
            });
        }
    }

    if let Some(unit) = current {
        units.push(unit);
    }

    units
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(vol_id: &str, start_lba: u64, lba_count: u32, seq: u64, fill: u8) -> BufferEntry {
        let payload = vec![fill; lba_count as usize * BLOCK_SIZE as usize];
        let payload_crc32 = crc32fast::hash(&payload);
        BufferEntry {
            seq,
            vol_id: vol_id.to_string(),
            start_lba: Lba(start_lba),
            lba_count,
            payload_crc32,
            flushed: false,
            vol_created_at: 0,
            payload: Arc::from(payload),
        }
    }

    #[test]
    fn coalesce_single_lba_entries() {
        let entries: Vec<_> = (0..8).map(|i| make_entry("vol-a", i, 1, i, 0xAA)).collect();
        let units = coalesce(&entries, 131072, 32, &|_| CompressionAlgo::None);
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].lba_count, 8);
        assert_eq!(units[0].raw_data.len(), 8 * BLOCK_SIZE as usize);
    }

    #[test]
    fn coalesce_multi_lba_entry() {
        // One entry covering 4 LBAs
        let entries = vec![make_entry("vol-a", 0, 4, 0, 0xBB)];
        let units = coalesce(&entries, 131072, 32, &|_| CompressionAlgo::None);
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].lba_count, 4);
    }

    #[test]
    fn coalesce_adjacent_multi_lba_entries() {
        // Two entries: LBAs 0-3 and LBAs 4-7 → should merge into one 8-LBA unit
        let entries = vec![
            make_entry("vol-a", 0, 4, 0, 0xAA),
            make_entry("vol-a", 4, 4, 1, 0xBB),
        ];
        let units = coalesce(&entries, 131072, 32, &|_| CompressionAlgo::None);
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].lba_count, 8);
        // First 4 LBAs filled with 0xAA, next 4 with 0xBB
        assert_eq!(units[0].raw_data[0], 0xAA);
        assert_eq!(units[0].raw_data[4 * BLOCK_SIZE as usize], 0xBB);
    }

    #[test]
    fn coalesce_gap_splits() {
        let entries = vec![
            make_entry("vol-a", 0, 2, 0, 0xAA),
            make_entry("vol-a", 5, 2, 1, 0xBB), // gap at LBA 2-4
        ];
        let units = coalesce(&entries, 131072, 32, &|_| CompressionAlgo::None);
        assert_eq!(units.len(), 2);
        assert_eq!(units[0].lba_count, 2);
        assert_eq!(units[1].lba_count, 2);
    }

    #[test]
    fn coalesce_cross_volume_splits() {
        let entries = vec![
            make_entry("vol-a", 0, 2, 0, 0xAA),
            make_entry("vol-b", 2, 2, 1, 0xBB),
        ];
        let units = coalesce(&entries, 131072, 32, &|_| CompressionAlgo::None);
        assert_eq!(units.len(), 2);
    }

    #[test]
    fn coalesce_max_lbas_limit() {
        let entries: Vec<_> = (0..10)
            .map(|i| make_entry("vol-a", i, 1, i, 0xAA))
            .collect();
        let units = coalesce(&entries, 131072, 4, &|_| CompressionAlgo::None);
        assert_eq!(units.len(), 3); // 4 + 4 + 2
    }

    #[test]
    fn coalesce_dedup_overwrites() {
        let entries = vec![
            make_entry("vol-a", 0, 1, 0, 0x11), // old
            make_entry("vol-a", 0, 1, 5, 0x22), // new (higher seq)
            make_entry("vol-a", 1, 1, 1, 0x33),
        ];
        let units = coalesce(&entries, 131072, 32, &|_| CompressionAlgo::None);
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].lba_count, 2);
        assert_eq!(units[0].raw_data[0], 0x22); // latest data
    }

    #[test]
    fn coalesce_multi_lba_partial_overwrite() {
        // Entry covers LBAs 0-3, then a single-LBA overwrite at LBA 1
        let entries = vec![
            make_entry("vol-a", 0, 4, 0, 0xAA),
            make_entry("vol-a", 1, 1, 5, 0xFF), // overwrite LBA 1
        ];
        let units = coalesce(&entries, 131072, 32, &|_| CompressionAlgo::None);
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].lba_count, 4);
        // LBA 0: 0xAA, LBA 1: 0xFF (overwritten), LBA 2-3: 0xAA
        assert_eq!(units[0].raw_data[0], 0xAA);
        assert_eq!(units[0].raw_data[BLOCK_SIZE as usize], 0xFF);
        assert_eq!(units[0].raw_data[2 * BLOCK_SIZE as usize], 0xAA);
    }

    #[test]
    fn coalesce_empty() {
        assert!(coalesce(&[], 131072, 32, &|_| CompressionAlgo::None).is_empty());
    }
}
