use onyx_metadb::{DedupValue, L2pValue};

use crate::meta::schema::{BlockmapValue, DedupEntry};
use crate::types::Pba;

use super::codec::{
    blockmap_from_l2p_bytes, blockmap_to_l2p_bytes, dedup_from_value_bytes, dedup_to_value_bytes,
    DEDUP_VALUE_BYTES,
};

const METADB_DEDUP_VALUE_BYTES: usize = 28;

pub(crate) fn to_metadb_pba(pba: Pba) -> onyx_metadb::Pba {
    pba.0
}

pub(crate) fn from_metadb_pba(pba: onyx_metadb::Pba) -> Pba {
    Pba(pba)
}

pub(crate) fn to_l2p_value(value: &BlockmapValue) -> L2pValue {
    L2pValue(blockmap_to_l2p_bytes(value))
}

pub(crate) fn from_l2p_value(value: L2pValue) -> Option<BlockmapValue> {
    blockmap_from_l2p_bytes(&value.0)
}

pub(crate) fn to_dedup_value(entry: &DedupEntry) -> DedupValue {
    let mut bytes = [0u8; METADB_DEDUP_VALUE_BYTES];
    bytes[..DEDUP_VALUE_BYTES].copy_from_slice(&dedup_to_value_bytes(entry));
    DedupValue::new(bytes)
}

pub(crate) fn from_dedup_value(value: DedupValue) -> Option<DedupEntry> {
    let mut bytes = [0u8; DEDUP_VALUE_BYTES];
    bytes.copy_from_slice(&value.as_bytes()[..DEDUP_VALUE_BYTES]);
    dedup_from_value_bytes(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pba_newtypes_cross_backend_losslessly() {
        let pba = Pba(1234);
        assert_eq!(from_metadb_pba(to_metadb_pba(pba)), pba);
    }

    #[test]
    fn dedup_value_uses_zero_padded_metadb_slot() {
        let entry = DedupEntry {
            pba: Pba(7),
            slot_offset: 5,
            compression: 1,
            unit_compressed_size: 1024,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 0xDEAD_BEEF,
        };

        let value = to_dedup_value(&entry);
        assert_eq!(value.as_bytes()[27], 0);
        assert_eq!(from_dedup_value(value), Some(entry));
    }
}
