use crate::meta::schema::{
    decode_blockmap_value, decode_dedup_entry, encode_blockmap_value, encode_dedup_entry,
    BlockmapValue, DedupEntry,
};
use crate::types::BLOCK_SIZE;

/// Re-exports of metadb's authoritative `L2pValue` byte layout so this
/// crate doesn't drift if metadb bumps the schema again. Bytes
/// [0..L2P_SEQ_OFFSET] embed Onyx's `BlockmapValue`; the trailing 8 B
/// is a big-endian u64 commit seq metadb uses for its apply-time CAS.
pub(crate) const L2P_VALUE_BYTES: usize = onyx_metadb::paged::format::LEAF_VALUE_SIZE;
pub(crate) const L2P_SEQ_OFFSET: usize = onyx_metadb::paged::format::L2P_SEQ_OFFSET;
pub(crate) const BLOCKMAP_BYTES: usize = L2P_SEQ_OFFSET;
pub(crate) const DEDUP_VALUE_BYTES: usize = 27;

pub(crate) fn blockmap_to_l2p_bytes(value: &BlockmapValue) -> [u8; L2P_VALUE_BYTES] {
    blockmap_to_l2p_bytes_with_seq(value, 0)
}

pub(crate) fn blockmap_to_l2p_bytes_with_seq(
    value: &BlockmapValue,
    seq: u64,
) -> [u8; L2P_VALUE_BYTES] {
    let mut out = [0u8; L2P_VALUE_BYTES];
    out[..BLOCKMAP_BYTES].copy_from_slice(&encode_blockmap_value(value));
    out[L2P_SEQ_OFFSET..L2P_SEQ_OFFSET + 8].copy_from_slice(&seq.to_be_bytes());
    out
}

pub(crate) fn blockmap_from_l2p_bytes(bytes: &[u8; L2P_VALUE_BYTES]) -> Option<BlockmapValue> {
    let head: &[u8; BLOCKMAP_BYTES] = bytes[..BLOCKMAP_BYTES]
        .try_into()
        .expect("slice has known length");
    decode_blockmap_value(head)
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
    value.physical_blocks(BLOCK_SIZE)
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
        // Trailing seq bytes default to zero (no-guard sentinel).
        assert_eq!(&bytes[L2P_SEQ_OFFSET..], &[0u8; 8]);
        assert_eq!(blockmap_from_l2p_bytes(&bytes), Some(value));
    }

    #[test]
    fn blockmap_l2p_bytes_with_seq_round_trip() {
        let value = sample_blockmap_value();
        let seq = 0xDEAD_BEEF_CAFE_F00Du64;
        let bytes = blockmap_to_l2p_bytes_with_seq(&value, seq);

        assert_eq!(bytes.len(), L2P_VALUE_BYTES);
        let seq_bytes: [u8; 8] = bytes[L2P_SEQ_OFFSET..L2P_SEQ_OFFSET + 8]
            .try_into()
            .unwrap();
        assert_eq!(u64::from_be_bytes(seq_bytes), seq);
        // BlockmapValue round-trips independently of seq.
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
        value.compression = 1;
        value.unit_compressed_size = 8193;
        assert_eq!(freed_blocks_for_l2p_value(&value), 1);

        value.slot_offset = 0;
        value.unit_compressed_size = BLOCK_SIZE - 1;
        assert_eq!(freed_blocks_for_l2p_value(&value), 1);

        value.slot_offset = 0;
        value.unit_compressed_size = BLOCK_SIZE * 2 + 1;
        assert_eq!(freed_blocks_for_l2p_value(&value), 3);

        value.compression = 0;
        value.unit_compressed_size = BLOCK_SIZE * 8;
        value.unit_original_size = BLOCK_SIZE * 8;
        value.unit_lba_count = 8;
        value.offset_in_unit = 7;
        assert_eq!(freed_blocks_for_l2p_value(&value), 8);
    }
}
