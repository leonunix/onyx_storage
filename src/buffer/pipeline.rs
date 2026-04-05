use crate::buffer::entry::BufferEntry;
use crate::types::{Lba, BLOCK_SIZE};

/// A group of contiguous LBAs from the same volume, merged from buffer entries.
/// Ready to be compressed as a single unit.
#[derive(Debug, Clone)]
pub struct CoalesceUnit {
    pub vol_id: String,
    pub start_lba: Lba,
    pub lba_count: u32,
    pub raw_data: Vec<u8>,
    pub entry_seqs: Vec<u64>,
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
    pub entry_seqs: Vec<u64>,
}

/// Coalesce buffer entries into compression units.
///
/// Sorts entries by (vol_id, lba), then merges runs of contiguous LBAs from
/// the same volume. A run is cut when:
/// - vol_id changes
/// - LBA is not contiguous (gap)
/// - accumulated raw bytes exceed `max_raw_bytes`
/// - LBA count exceeds `max_lbas`
///
/// Each entry is assumed to carry exactly one 4KB LBA of raw data.
/// If an LBA appears multiple times (overwrite), only the latest (highest seq) is kept.
pub fn coalesce(entries: &[BufferEntry], max_raw_bytes: usize, max_lbas: u32) -> Vec<CoalesceUnit> {
    if entries.is_empty() {
        return Vec::new();
    }

    struct DedupedEntry<'a> {
        latest: &'a BufferEntry,
        seqs_to_flush: Vec<u64>,
    }

    // Deduplicate: keep only the latest entry per (vol_id, lba) by highest seq.
    // Older overwritten entries must still be retired once the newest value is
    // durable on LV3, otherwise stale log entries can stay pending forever.
    let mut deduped: std::collections::HashMap<(String, u64), DedupedEntry<'_>> =
        std::collections::HashMap::new();
    for entry in entries {
        let key = (entry.vol_id.clone(), entry.lba.0);
        match deduped.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut existing) => {
                let existing = existing.get_mut();
                existing.seqs_to_flush.push(entry.seq);
                if entry.seq > existing.latest.seq {
                    existing.latest = entry;
                }
            }
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(DedupedEntry {
                    latest: entry,
                    seqs_to_flush: vec![entry.seq],
                });
            }
        }
    }

    // Sort by (vol_id, lba)
    let mut sorted: Vec<&DedupedEntry<'_>> = deduped.values().collect();
    sorted.sort_by(|a, b| {
        (&a.latest.vol_id, a.latest.lba.0).cmp(&(&b.latest.vol_id, b.latest.lba.0))
    });

    let bs = BLOCK_SIZE as usize;
    let mut units = Vec::new();
    let mut current: Option<CoalesceUnit> = None;

    for entry in sorted {
        let latest = entry.latest;
        let should_extend = if let Some(ref cur) = current {
            cur.vol_id == latest.vol_id
                && latest.lba.0 == cur.start_lba.0 + cur.lba_count as u64
                && cur.raw_data.len() + bs <= max_raw_bytes
                && cur.lba_count + 1 <= max_lbas
        } else {
            false
        };

        if should_extend {
            let cur = current.as_mut().unwrap();
            cur.lba_count += 1;
            // Pad payload to exactly 4KB if shorter
            let mut block = vec![0u8; bs];
            let copy_len = latest.payload.len().min(bs);
            block[..copy_len].copy_from_slice(&latest.payload[..copy_len]);
            cur.raw_data.extend_from_slice(&block);
            cur.entry_seqs.extend_from_slice(&entry.seqs_to_flush);
        } else {
            // Flush current unit if any
            if let Some(unit) = current.take() {
                units.push(unit);
            }
            // Start new unit
            let mut block = vec![0u8; bs];
            let copy_len = latest.payload.len().min(bs);
            block[..copy_len].copy_from_slice(&latest.payload[..copy_len]);
            current = Some(CoalesceUnit {
                vol_id: latest.vol_id.clone(),
                start_lba: latest.lba,
                lba_count: 1,
                raw_data: block,
                entry_seqs: entry.seqs_to_flush.clone(),
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

    fn make_entry(vol_id: &str, lba: u64, seq: u64, data: u8) -> BufferEntry {
        let payload = vec![data; BLOCK_SIZE as usize];
        BufferEntry {
            seq,
            vol_id: vol_id.to_string(),
            lba: Lba(lba),
            compression: 0,
            original_size: BLOCK_SIZE,
            compressed_size: BLOCK_SIZE,
            payload_crc32: crc32fast::hash(&payload),
            flushed: false,
            payload,
        }
    }

    #[test]
    fn coalesce_contiguous() {
        let entries: Vec<_> = (0..8).map(|i| make_entry("vol-a", i, i, 0xAA)).collect();
        let units = coalesce(&entries, 131072, 32);
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].lba_count, 8);
        assert_eq!(units[0].raw_data.len(), 8 * BLOCK_SIZE as usize);
    }

    #[test]
    fn coalesce_gap_splits() {
        let mut entries = vec![
            make_entry("vol-a", 0, 0, 0xAA),
            make_entry("vol-a", 1, 1, 0xAA),
            // gap: LBA 2 missing
            make_entry("vol-a", 3, 2, 0xBB),
            make_entry("vol-a", 4, 3, 0xBB),
        ];
        let units = coalesce(&entries, 131072, 32);
        assert_eq!(units.len(), 2);
        assert_eq!(units[0].lba_count, 2);
        assert_eq!(units[0].start_lba, Lba(0));
        assert_eq!(units[1].lba_count, 2);
        assert_eq!(units[1].start_lba, Lba(3));
    }

    #[test]
    fn coalesce_cross_volume_splits() {
        let entries = vec![
            make_entry("vol-a", 0, 0, 0xAA),
            make_entry("vol-a", 1, 1, 0xAA),
            make_entry("vol-b", 2, 2, 0xBB),
            make_entry("vol-b", 3, 3, 0xBB),
        ];
        let units = coalesce(&entries, 131072, 32);
        assert_eq!(units.len(), 2);
        assert_eq!(units[0].vol_id, "vol-a");
        assert_eq!(units[1].vol_id, "vol-b");
    }

    #[test]
    fn coalesce_max_lbas_limit() {
        let entries: Vec<_> = (0..10).map(|i| make_entry("vol-a", i, i, 0xAA)).collect();
        let units = coalesce(&entries, 131072, 4); // max 4 LBAs per unit
        assert_eq!(units.len(), 3); // 4 + 4 + 2
        assert_eq!(units[0].lba_count, 4);
        assert_eq!(units[1].lba_count, 4);
        assert_eq!(units[2].lba_count, 2);
    }

    #[test]
    fn coalesce_max_bytes_limit() {
        let entries: Vec<_> = (0..10).map(|i| make_entry("vol-a", i, i, 0xAA)).collect();
        // 2 * 4096 = 8192 max
        let units = coalesce(&entries, 8192, 100);
        assert_eq!(units.len(), 5); // 2 + 2 + 2 + 2 + 2
        for u in &units {
            assert_eq!(u.lba_count, 2);
        }
    }

    #[test]
    fn coalesce_empty() {
        let units = coalesce(&[], 131072, 32);
        assert!(units.is_empty());
    }

    #[test]
    fn coalesce_single_entry() {
        let entries = vec![make_entry("vol-a", 5, 0, 0xCC)];
        let units = coalesce(&entries, 131072, 32);
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].lba_count, 1);
        assert_eq!(units[0].start_lba, Lba(5));
    }

    #[test]
    fn coalesce_dedup_keeps_latest() {
        let entries = vec![
            make_entry("vol-a", 0, 0, 0x11), // old
            make_entry("vol-a", 0, 5, 0x22), // new (higher seq)
            make_entry("vol-a", 1, 1, 0x33),
        ];
        let units = coalesce(&entries, 131072, 32);
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].lba_count, 2);
        // First 4KB should be from seq 5 (0x22), not seq 0 (0x11)
        assert_eq!(units[0].raw_data[0], 0x22);
        assert_eq!(units[0].raw_data[BLOCK_SIZE as usize], 0x33);
        assert_eq!(units[0].entry_seqs, vec![0, 5, 1]);
    }

    #[test]
    fn coalesce_marks_superseded_entries_flushed() {
        let entries = vec![
            make_entry("vol-a", 10, 1, 0x10),
            make_entry("vol-a", 10, 4, 0x40),
            make_entry("vol-a", 11, 2, 0x20),
            make_entry("vol-a", 11, 6, 0x60),
        ];
        let units = coalesce(&entries, 131072, 32);
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].lba_count, 2);
        assert_eq!(units[0].raw_data[0], 0x40);
        assert_eq!(units[0].raw_data[BLOCK_SIZE as usize], 0x60);
        assert_eq!(units[0].entry_seqs, vec![1, 4, 2, 6]);
    }
}
