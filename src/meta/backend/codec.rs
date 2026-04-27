use crate::meta::schema::{
    decode_blockmap_value, decode_dedup_entry, encode_blockmap_value, encode_dedup_entry,
    BlockmapValue, DedupEntry,
};
use crate::types::BLOCK_SIZE;

pub(crate) const L2P_VALUE_BYTES: usize = 28;
pub(crate) const DEDUP_VALUE_BYTES: usize = 27;

pub(crate) fn blockmap_to_l2p_bytes(value: &BlockmapValue) -> [u8; L2P_VALUE_BYTES] {
    encode_blockmap_value(value)
}

pub(crate) fn blockmap_from_l2p_bytes(bytes: &[u8; L2P_VALUE_BYTES]) -> Option<BlockmapValue> {
    decode_blockmap_value(bytes)
}

pub(crate) fn dedup_to_value_bytes(entry: &DedupEntry) -> [u8; DEDUP_VALUE_BYTES] {
    encode_dedup_entry(entry)
}

pub(crate) fn dedup_from_value_bytes(bytes: &[u8; DEDUP_VALUE_BYTES]) -> Option<DedupEntry> {
    decode_dedup_entry(bytes)
}

/// Number of physical 4 KiB blocks Onyx should return to `SpaceAllocator`
/// when metadb reports the head PBA of this value transitioned to refcount 0.
pub(crate) fn freed_blocks_for_l2p_value(value: &BlockmapValue) -> u32 {
    if value.slot_offset > 0 || value.unit_compressed_size < BLOCK_SIZE {
        1
    } else {
        value.unit_compressed_size.div_ceil(BLOCK_SIZE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::schema::FLAG_DEDUP_SKIPPED;
    use crate::types::Pba;

    fn sample_blockmap_value() -> BlockmapValue {
        BlockmapValue {
            pba: Pba(0x0102_0304_0506_0708),
            compression: 1,
            unit_compressed_size: 8193,
            unit_original_size: 131_072,
            unit_lba_count: 32,
            offset_in_unit: 7,
            crc32: 0xAABB_CCDD,
            slot_offset: 0,
            flags: FLAG_DEDUP_SKIPPED,
        }
    }

    #[test]
    fn blockmap_l2p_bytes_round_trip() {
        let value = sample_blockmap_value();
        let bytes = blockmap_to_l2p_bytes(&value);

        assert_eq!(bytes.len(), L2P_VALUE_BYTES);
        assert_eq!(blockmap_from_l2p_bytes(&bytes), Some(value));
    }

    #[test]
    fn dedup_value_bytes_round_trip() {
        let entry = DedupEntry {
            pba: Pba(42),
            slot_offset: 12,
            compression: 2,
            unit_compressed_size: 2048,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 0x1234_5678,
        };
        let bytes = dedup_to_value_bytes(&entry);

        assert_eq!(bytes.len(), DEDUP_VALUE_BYTES);
        assert_eq!(dedup_from_value_bytes(&bytes), Some(entry));
    }

    #[test]
    fn freed_blocks_for_l2p_value_matches_packer_rules() {
        let mut value = sample_blockmap_value();

        value.slot_offset = 9;
        value.unit_compressed_size = 8193;
        assert_eq!(freed_blocks_for_l2p_value(&value), 1);

        value.slot_offset = 0;
        value.unit_compressed_size = BLOCK_SIZE - 1;
        assert_eq!(freed_blocks_for_l2p_value(&value), 1);

        value.slot_offset = 0;
        value.unit_compressed_size = BLOCK_SIZE * 2 + 1;
        assert_eq!(freed_blocks_for_l2p_value(&value), 3);
    }
}
