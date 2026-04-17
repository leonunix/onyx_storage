//! redb table definitions and value types for paged blockmap.

use redb::TableDefinition;

/// `T_VOLUMES`: `vol_id → VolumeRoot` (bincode encoded).
pub const T_VOLUMES: TableDefinition<&str, &[u8]> = TableDefinition::new("volumes");

/// `T_L1`: `(vol_id, l1_idx) → L1Entry` encoded as 12 bytes.
pub const T_L1: TableDefinition<(&str, u64), [u8; L1_ENTRY_ENCODED_LEN]> =
    TableDefinition::new("l1");

/// `T_L2_PAGES`: `page_id → encoded L2Page` (variable length, compressed-friendly).
pub const T_L2_PAGES: TableDefinition<u64, &[u8]> = TableDefinition::new("l2_pages");

/// `T_PAGE_REFS`: `page_id → u32 refcount` (COW; v1 always 1).
pub const T_PAGE_REFS: TableDefinition<u64, u32> = TableDefinition::new("page_refs");

/// `T_PAGE_FREE`: `page_id → ()` freelist, used LIFO for locality.
pub const T_PAGE_FREE: TableDefinition<u64, ()> = TableDefinition::new("page_free");

/// `T_PAGE_NEXT_ID`: single-row table, key `""`, value = next page id.
pub const T_PAGE_NEXT_ID: TableDefinition<&str, u64> = TableDefinition::new("page_next_id");

/// VolumeRoot: paged blockmap metadata for one volume.
///
/// Encoded with bincode as the `T_VOLUMES` value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VolumeRoot {
    /// Number of L1 slots allocated for this volume. Equals `ceil(volume_size_lbas / LBAS_PER_PAGE)`.
    pub l1_size: u64,
    /// Monotonic generation bumped on every L1 root operation (v1 unused, reserved for snapshot).
    pub gen: u64,
}

/// L1Entry: one slot in the L1 array of a volume.
///
/// Encoded as 12 bytes (BE), stored as the `T_L1` value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L1Entry {
    /// Pointer into `T_L2_PAGES`.
    pub page_id: u64,
    /// Monotonic generation bumped on every COW of this entry (v1 unused, reserved for snapshot).
    pub gen: u32,
}

pub const L1_ENTRY_ENCODED_LEN: usize = 12;

impl L1Entry {
    pub fn encode(&self) -> [u8; L1_ENTRY_ENCODED_LEN] {
        let mut buf = [0u8; L1_ENTRY_ENCODED_LEN];
        buf[0..8].copy_from_slice(&self.page_id.to_be_bytes());
        buf[8..12].copy_from_slice(&self.gen.to_be_bytes());
        buf
    }

    pub fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() != L1_ENTRY_ENCODED_LEN {
            return None;
        }
        Some(Self {
            page_id: u64::from_be_bytes(buf[0..8].try_into().unwrap()),
            gen: u32::from_be_bytes(buf[8..12].try_into().unwrap()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn l1_entry_roundtrip() {
        let e = L1Entry {
            page_id: 0xdead_beef_1234_5678,
            gen: 0x9abc_def0,
        };
        let buf = e.encode();
        assert_eq!(buf.len(), L1_ENTRY_ENCODED_LEN);
        let decoded = L1Entry::decode(&buf).unwrap();
        assert_eq!(decoded, e);
    }

    #[test]
    fn l1_entry_decode_wrong_len() {
        assert!(L1Entry::decode(&[0u8; 11]).is_none());
        assert!(L1Entry::decode(&[0u8; 13]).is_none());
    }

    #[test]
    fn l1_entry_big_endian_order() {
        let e = L1Entry {
            page_id: 1,
            gen: 0,
        };
        let buf = e.encode();
        assert_eq!(&buf[0..8], &[0, 0, 0, 0, 0, 0, 0, 1]);
    }
}
