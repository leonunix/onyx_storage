//! L2 page layout and codec.
//!
//! A single L2 page covers 256 LBAs (= 1 MiB of user data at 4 KiB blocks).
//! Wire format:
//!
//! ```text
//! [0..32]   bitmap: 256 bits little-endian, bit i set ⇔ slot i present
//! [32..]    packed BlockmapValue[popcount(bitmap)], 28 B each, in slot-index order
//! ```
//!
//! Entries are stored in ascending slot-index order so lookups can skip the
//! decode step and binary-search on slot index if they choose to.

use crate::meta::schema::{decode_blockmap_value, encode_blockmap_value, BlockmapValue};

/// LBAs per L2 page. Must be a power of two.
pub const LBAS_PER_PAGE: usize = 256;

const BITMAP_BYTES: usize = LBAS_PER_PAGE / 8;
const ENTRY_SIZE: usize = 28;

/// Maximum raw size of a fully-populated L2 page (before compression).
pub const L2_PAGE_RAW_MAX: usize = BITMAP_BYTES + LBAS_PER_PAGE * ENTRY_SIZE;

/// In-memory representation of an L2 page.
///
/// Stored as a dense `[Option<BlockmapValue>; 256]` for O(1) get/set; serialized
/// as a sparse (bitmap + packed entries) blob via [`L2Page::encode`].
#[derive(Debug, Clone)]
pub struct L2Page {
    slots: [Option<BlockmapValue>; LBAS_PER_PAGE],
}

impl Default for L2Page {
    fn default() -> Self {
        Self::empty()
    }
}

impl L2Page {
    /// Construct an empty page.
    pub fn empty() -> Self {
        Self {
            slots: [None; LBAS_PER_PAGE],
        }
    }

    /// Read the entry at `slot_idx` (= `lba % LBAS_PER_PAGE`).
    pub fn get(&self, slot_idx: usize) -> Option<BlockmapValue> {
        self.slots.get(slot_idx).copied().flatten()
    }

    /// Overwrite the entry at `slot_idx`.
    pub fn set(&mut self, slot_idx: usize, value: BlockmapValue) {
        assert!(slot_idx < LBAS_PER_PAGE, "slot_idx out of range");
        self.slots[slot_idx] = Some(value);
    }

    /// Clear the entry at `slot_idx`.
    pub fn clear(&mut self, slot_idx: usize) {
        assert!(slot_idx < LBAS_PER_PAGE, "slot_idx out of range");
        self.slots[slot_idx] = None;
    }

    /// Count populated entries in the page.
    pub fn popcount(&self) -> usize {
        self.slots.iter().filter(|s| s.is_some()).count()
    }

    /// Iterate `(slot_idx, BlockmapValue)` in ascending slot order.
    pub fn iter(&self) -> impl Iterator<Item = (usize, BlockmapValue)> + '_ {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(i, s)| s.map(|v| (i, v)))
    }

    /// Encode to wire format: bitmap || packed entries.
    pub fn encode(&self) -> Vec<u8> {
        let popcount = self.popcount();
        let mut out = Vec::with_capacity(BITMAP_BYTES + popcount * ENTRY_SIZE);
        out.extend_from_slice(&[0u8; BITMAP_BYTES]);
        for (slot_idx, value) in self.iter() {
            out[slot_idx / 8] |= 1u8 << (slot_idx % 8);
            out.extend_from_slice(&encode_blockmap_value(&value));
        }
        out
    }

    /// Decode from wire format. Returns `None` on any structural mismatch.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < BITMAP_BYTES {
            return None;
        }
        let bitmap = &bytes[..BITMAP_BYTES];
        let popcount: u32 = bitmap.iter().map(|b| b.count_ones()).sum();
        let expected_len = BITMAP_BYTES + (popcount as usize) * ENTRY_SIZE;
        if bytes.len() != expected_len {
            return None;
        }
        let mut page = Self::empty();
        let mut cursor = BITMAP_BYTES;
        for slot_idx in 0..LBAS_PER_PAGE {
            if bitmap[slot_idx / 8] & (1u8 << (slot_idx % 8)) == 0 {
                continue;
            }
            let value = decode_blockmap_value(&bytes[cursor..cursor + ENTRY_SIZE])?;
            page.slots[slot_idx] = Some(value);
            cursor += ENTRY_SIZE;
        }
        Some(page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Pba;

    fn sample_value(pba: u64) -> BlockmapValue {
        BlockmapValue {
            pba: Pba(pba),
            compression: 1,
            unit_compressed_size: 2048,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 0xdead_beef,
            slot_offset: 0,
            flags: 0,
        }
    }

    #[test]
    fn empty_page_encodes_to_bitmap_only() {
        let page = L2Page::empty();
        let bytes = page.encode();
        assert_eq!(bytes.len(), BITMAP_BYTES);
        assert!(bytes.iter().all(|&b| b == 0));
    }

    #[test]
    fn single_entry_roundtrip() {
        let mut page = L2Page::empty();
        page.set(42, sample_value(7));
        let bytes = page.encode();
        assert_eq!(bytes.len(), BITMAP_BYTES + ENTRY_SIZE);
        let decoded = L2Page::decode(&bytes).unwrap();
        assert_eq!(decoded.get(42), Some(sample_value(7)));
        assert!(decoded.get(41).is_none());
        assert!(decoded.get(43).is_none());
    }

    #[test]
    fn full_page_roundtrip() {
        let mut page = L2Page::empty();
        for i in 0..LBAS_PER_PAGE {
            page.set(i, sample_value(1000 + i as u64));
        }
        let bytes = page.encode();
        assert_eq!(bytes.len(), L2_PAGE_RAW_MAX);
        let decoded = L2Page::decode(&bytes).unwrap();
        for i in 0..LBAS_PER_PAGE {
            assert_eq!(decoded.get(i), Some(sample_value(1000 + i as u64)));
        }
        assert_eq!(decoded.popcount(), LBAS_PER_PAGE);
    }

    #[test]
    fn sparse_page_roundtrip() {
        let mut page = L2Page::empty();
        for i in (0..LBAS_PER_PAGE).step_by(7) {
            page.set(i, sample_value(i as u64));
        }
        let bytes = page.encode();
        let decoded = L2Page::decode(&bytes).unwrap();
        for i in 0..LBAS_PER_PAGE {
            if i % 7 == 0 {
                assert_eq!(decoded.get(i), Some(sample_value(i as u64)));
            } else {
                assert!(decoded.get(i).is_none(), "slot {i} should be empty");
            }
        }
    }

    #[test]
    fn clear_removes_entry() {
        let mut page = L2Page::empty();
        page.set(3, sample_value(3));
        page.set(5, sample_value(5));
        page.clear(3);
        assert!(page.get(3).is_none());
        assert_eq!(page.get(5), Some(sample_value(5)));
        assert_eq!(page.popcount(), 1);
    }

    #[test]
    fn decode_rejects_truncated_bitmap() {
        assert!(L2Page::decode(&[0u8; BITMAP_BYTES - 1]).is_none());
    }

    #[test]
    fn decode_rejects_length_mismatch() {
        let mut page = L2Page::empty();
        page.set(0, sample_value(1));
        let mut bytes = page.encode();
        bytes.push(0); // extra trailing byte
        assert!(L2Page::decode(&bytes).is_none());
    }

    #[test]
    fn iter_yields_in_slot_order() {
        let mut page = L2Page::empty();
        page.set(100, sample_value(100));
        page.set(10, sample_value(10));
        page.set(200, sample_value(200));
        let order: Vec<usize> = page.iter().map(|(i, _)| i).collect();
        assert_eq!(order, vec![10, 100, 200]);
    }
}
