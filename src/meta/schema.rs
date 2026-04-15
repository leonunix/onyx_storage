use crate::error::OnyxError;
use crate::types::{Lba, Pba, VolumeId};

/// Column family names
pub const CF_VOLUMES: &str = "volumes";
pub const CF_BLOCKMAP: &str = "blockmap";
pub const CF_REFCOUNT: &str = "refcount";
pub const CF_DEDUP_INDEX: &str = "dedup_index";
pub const CF_DEDUP_REVERSE: &str = "dedup_reverse";

/// BlockmapValue flags
pub const FLAG_DEDUP_SKIPPED: u8 = 0x01;

/// Maximum volume ID length in bytes. Enforced at volume creation time.
/// The length byte in blockmap keys is u8, so the hard ceiling is 255.
pub const MAX_VOLUME_ID_BYTES: usize = 255;

/// Blockmap value: full metadata for a stored block or compression unit.
///
/// When flusher coalesces multiple LBAs into one compression unit, all LBAs
/// in the unit share the same (pba, unit_compressed_size, unit_original_size,
/// unit_lba_count, crc32) but differ in offset_in_unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockmapValue {
    pub pba: Pba,
    pub compression: u8,
    pub unit_compressed_size: u32,
    pub unit_original_size: u32,
    pub unit_lba_count: u16,
    pub offset_in_unit: u16,
    pub crc32: u32,
    /// Byte offset of this fragment within a shared 4KB physical slot (packer).
    /// 0 = fragment starts at beginning of slot (no packing, or first fragment).
    pub slot_offset: u16,
    /// Flags: bit 0 = DEDUP_SKIPPED (block bypassed dedup under pressure).
    pub flags: u8,
}

// --- Blockmap key: volume_id (length-prefixed) + lba ---
// Format: 1-byte vol_id_len + vol_id bytes + 8-byte lba (BE)
// This eliminates hash collision risk entirely.

/// Encode blockmap key: vol_id_len(1B) + vol_id + lba(8B BE)
///
/// Returns error if vol_id exceeds MAX_VOLUME_ID_BYTES or is empty.
pub fn encode_blockmap_key(vol_id: &VolumeId, lba: Lba) -> Result<Vec<u8>, OnyxError> {
    let id_bytes = vol_id.0.as_bytes();
    validate_vol_id_len(id_bytes.len())?;
    let id_len = id_bytes.len() as u8;
    let mut key = Vec::with_capacity(1 + id_len as usize + 8);
    key.push(id_len);
    key.extend_from_slice(id_bytes);
    key.extend_from_slice(&lba.0.to_be_bytes());
    Ok(key)
}

/// Decode blockmap key back to (vol_id_str, lba)
pub fn decode_blockmap_key(key: &[u8]) -> Option<(String, Lba)> {
    if key.len() < 1 + 8 {
        return None;
    }
    let id_len = key[0] as usize;
    if key.len() != 1 + id_len + 8 {
        return None;
    }
    let vol_id = std::str::from_utf8(&key[1..1 + id_len]).ok()?.to_string();
    let lba_bytes: [u8; 8] = key[1 + id_len..].try_into().ok()?;
    let lba = u64::from_be_bytes(lba_bytes);
    Some((vol_id, Lba(lba)))
}

/// Build the key prefix for a volume (for iteration / delete_volume scans)
pub fn blockmap_key_prefix(vol_id: &VolumeId) -> Result<Vec<u8>, OnyxError> {
    let id_bytes = vol_id.0.as_bytes();
    validate_vol_id_len(id_bytes.len())?;
    let id_len = id_bytes.len() as u8;
    let mut prefix = Vec::with_capacity(1 + id_len as usize);
    prefix.push(id_len);
    prefix.extend_from_slice(id_bytes);
    Ok(prefix)
}

fn validate_vol_id_len(len: usize) -> Result<(), OnyxError> {
    if len == 0 || len > MAX_VOLUME_ID_BYTES {
        Err(OnyxError::Config(format!(
            "volume ID must be 1..{} bytes, got {}",
            MAX_VOLUME_ID_BYTES, len
        )))
    } else {
        Ok(())
    }
}

/// Encode blockmap value (28 bytes):
/// pba(8B) + compression(1B) + unit_compressed_size(4B) + unit_original_size(4B)
/// + unit_lba_count(2B) + offset_in_unit(2B) + crc32(4B) + slot_offset(2B) + flags(1B)
pub fn encode_blockmap_value(v: &BlockmapValue) -> [u8; 28] {
    let mut val = [0u8; 28];
    val[0..8].copy_from_slice(&v.pba.0.to_be_bytes());
    val[8] = v.compression;
    val[9..13].copy_from_slice(&v.unit_compressed_size.to_be_bytes());
    val[13..17].copy_from_slice(&v.unit_original_size.to_be_bytes());
    val[17..19].copy_from_slice(&v.unit_lba_count.to_be_bytes());
    val[19..21].copy_from_slice(&v.offset_in_unit.to_be_bytes());
    val[21..25].copy_from_slice(&v.crc32.to_be_bytes());
    val[25..27].copy_from_slice(&v.slot_offset.to_be_bytes());
    val[27] = v.flags;
    val
}

/// Decode blockmap value (28 bytes).
pub fn decode_blockmap_value(val: &[u8]) -> Option<BlockmapValue> {
    if val.len() != 28 {
        return None;
    }
    Some(BlockmapValue {
        pba: Pba(u64::from_be_bytes(val[0..8].try_into().unwrap())),
        compression: val[8],
        unit_compressed_size: u32::from_be_bytes(val[9..13].try_into().unwrap()),
        unit_original_size: u32::from_be_bytes(val[13..17].try_into().unwrap()),
        unit_lba_count: u16::from_be_bytes(val[17..19].try_into().unwrap()),
        offset_in_unit: u16::from_be_bytes(val[19..21].try_into().unwrap()),
        crc32: u32::from_be_bytes(val[21..25].try_into().unwrap()),
        slot_offset: u16::from_be_bytes(val[25..27].try_into().unwrap()),
        flags: val[27],
    })
}

/// Content hash type for dedup (SHA-256, 32 bytes)
pub type ContentHash = [u8; 32];

/// Dedup index value: physical location info for a deduplicated block.
/// Stored in CF_DEDUP_INDEX with key = ContentHash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DedupEntry {
    pub pba: Pba,
    pub slot_offset: u16,
    pub compression: u8,
    pub unit_compressed_size: u32,
    pub unit_original_size: u32,
    pub unit_lba_count: u16,
    pub offset_in_unit: u16,
    pub crc32: u32,
}

impl DedupEntry {
    /// Convert to a BlockmapValue (flags=0).
    pub fn to_blockmap_value(&self) -> BlockmapValue {
        BlockmapValue {
            pba: self.pba,
            compression: self.compression,
            unit_compressed_size: self.unit_compressed_size,
            unit_original_size: self.unit_original_size,
            unit_lba_count: self.unit_lba_count,
            offset_in_unit: self.offset_in_unit,
            crc32: self.crc32,
            slot_offset: self.slot_offset,
            flags: 0,
        }
    }
}

/// Encode dedup index value (27 bytes):
/// pba(8B) + slot_offset(2B) + compression(1B) + unit_compressed_size(4B)
/// + unit_original_size(4B) + unit_lba_count(2B) + offset_in_unit(2B) + crc32(4B)
pub fn encode_dedup_entry(e: &DedupEntry) -> [u8; 27] {
    let mut val = [0u8; 27];
    val[0..8].copy_from_slice(&e.pba.0.to_be_bytes());
    val[8..10].copy_from_slice(&e.slot_offset.to_be_bytes());
    val[10] = e.compression;
    val[11..15].copy_from_slice(&e.unit_compressed_size.to_be_bytes());
    val[15..19].copy_from_slice(&e.unit_original_size.to_be_bytes());
    val[19..21].copy_from_slice(&e.unit_lba_count.to_be_bytes());
    val[21..23].copy_from_slice(&e.offset_in_unit.to_be_bytes());
    val[23..27].copy_from_slice(&e.crc32.to_be_bytes());
    val
}

/// Decode dedup index value (27 bytes)
pub fn decode_dedup_entry(val: &[u8]) -> Option<DedupEntry> {
    if val.len() != 27 {
        return None;
    }
    Some(DedupEntry {
        pba: Pba(u64::from_be_bytes(val[0..8].try_into().unwrap())),
        slot_offset: u16::from_be_bytes(val[8..10].try_into().unwrap()),
        compression: val[10],
        unit_compressed_size: u32::from_be_bytes(val[11..15].try_into().unwrap()),
        unit_original_size: u32::from_be_bytes(val[15..19].try_into().unwrap()),
        unit_lba_count: u16::from_be_bytes(val[19..21].try_into().unwrap()),
        offset_in_unit: u16::from_be_bytes(val[21..23].try_into().unwrap()),
        crc32: u32::from_be_bytes(val[23..27].try_into().unwrap()),
    })
}

/// Encode dedup reverse key: pba(8B) + content_hash(32B) = 40B
pub fn encode_dedup_reverse_key(pba: Pba, hash: &ContentHash) -> [u8; 40] {
    let mut key = [0u8; 40];
    key[0..8].copy_from_slice(&pba.0.to_be_bytes());
    key[8..40].copy_from_slice(hash);
    key
}

/// Decode dedup reverse key (40B) → (pba, hash)
pub fn decode_dedup_reverse_key(key: &[u8]) -> Option<(Pba, ContentHash)> {
    if key.len() != 40 {
        return None;
    }
    let pba = Pba(u64::from_be_bytes(key[0..8].try_into().unwrap()));
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&key[8..40]);
    Some((pba, hash))
}

/// Encode refcount key: pba (8B BE)
pub fn encode_refcount_key(pba: Pba) -> [u8; 8] {
    pba.0.to_be_bytes()
}

/// Decode refcount key
pub fn decode_refcount_key(key: &[u8]) -> Option<Pba> {
    if key.len() != 8 {
        return None;
    }
    Some(Pba(u64::from_be_bytes(key[0..8].try_into().unwrap())))
}

/// Encode refcount value: count (4B BE)
pub fn encode_refcount_value(count: u32) -> [u8; 4] {
    count.to_be_bytes()
}

/// Decode refcount value
pub fn decode_refcount_value(val: &[u8]) -> Option<u32> {
    if val.len() != 4 {
        return None;
    }
    Some(u32::from_be_bytes(val[0..4].try_into().unwrap()))
}

/// Encode a refcount delta for the MergeOperator (4B BE i32).
/// Positive = increment, negative = decrement.
pub fn encode_refcount_delta(delta: i32) -> [u8; 4] {
    delta.to_be_bytes()
}

/// Encode volumes CF key: "vol-{id}"
pub fn encode_volume_key(vol_id: &str) -> Vec<u8> {
    format!("vol-{}", vol_id).into_bytes()
}
