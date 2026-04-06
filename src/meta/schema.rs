use crate::error::OnyxError;
use crate::types::{Lba, Pba, VolumeId};

/// Column family names
pub const CF_VOLUMES: &str = "volumes";
pub const CF_BLOCKMAP: &str = "blockmap";
pub const CF_REFCOUNT: &str = "refcount";

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

/// Encode blockmap value (27 bytes):
/// pba(8B) + compression(1B) + unit_compressed_size(4B) + unit_original_size(4B)
/// + unit_lba_count(2B) + offset_in_unit(2B) + crc32(4B) + slot_offset(2B)
pub fn encode_blockmap_value(v: &BlockmapValue) -> [u8; 27] {
    let mut val = [0u8; 27];
    val[0..8].copy_from_slice(&v.pba.0.to_be_bytes());
    val[8] = v.compression;
    val[9..13].copy_from_slice(&v.unit_compressed_size.to_be_bytes());
    val[13..17].copy_from_slice(&v.unit_original_size.to_be_bytes());
    val[17..19].copy_from_slice(&v.unit_lba_count.to_be_bytes());
    val[19..21].copy_from_slice(&v.offset_in_unit.to_be_bytes());
    val[21..25].copy_from_slice(&v.crc32.to_be_bytes());
    val[25..27].copy_from_slice(&v.slot_offset.to_be_bytes());
    val
}

/// Decode blockmap value. Supports:
/// - 27-byte format (with slot_offset for packer)
/// - 25-byte format (compression units, slot_offset=0)
/// - 17-byte legacy format (single-LBA, migrated to unit_lba_count=1, slot_offset=0)
pub fn decode_blockmap_value(val: &[u8]) -> Option<BlockmapValue> {
    if val.len() == 27 {
        return Some(BlockmapValue {
            pba: Pba(u64::from_be_bytes(val[0..8].try_into().unwrap())),
            compression: val[8],
            unit_compressed_size: u32::from_be_bytes(val[9..13].try_into().unwrap()),
            unit_original_size: u32::from_be_bytes(val[13..17].try_into().unwrap()),
            unit_lba_count: u16::from_be_bytes(val[17..19].try_into().unwrap()),
            offset_in_unit: u16::from_be_bytes(val[19..21].try_into().unwrap()),
            crc32: u32::from_be_bytes(val[21..25].try_into().unwrap()),
            slot_offset: u16::from_be_bytes(val[25..27].try_into().unwrap()),
        });
    }
    if val.len() == 25 {
        return Some(BlockmapValue {
            pba: Pba(u64::from_be_bytes(val[0..8].try_into().unwrap())),
            compression: val[8],
            unit_compressed_size: u32::from_be_bytes(val[9..13].try_into().unwrap()),
            unit_original_size: u32::from_be_bytes(val[13..17].try_into().unwrap()),
            unit_lba_count: u16::from_be_bytes(val[17..19].try_into().unwrap()),
            offset_in_unit: u16::from_be_bytes(val[19..21].try_into().unwrap()),
            crc32: u32::from_be_bytes(val[21..25].try_into().unwrap()),
            slot_offset: 0,
        });
    }
    // Legacy 17-byte format: single-LBA block
    if val.len() == 17 {
        let compressed_size = u16::from_be_bytes(val[9..11].try_into().unwrap());
        let original_size = u16::from_be_bytes(val[11..13].try_into().unwrap());
        return Some(BlockmapValue {
            pba: Pba(u64::from_be_bytes(val[0..8].try_into().unwrap())),
            compression: val[8],
            unit_compressed_size: compressed_size as u32,
            unit_original_size: original_size as u32,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: u32::from_be_bytes(val[13..17].try_into().unwrap()),
            slot_offset: 0,
        });
    }
    None
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

/// Encode volumes CF key: "vol-{id}"
pub fn encode_volume_key(vol_id: &str) -> Vec<u8> {
    format!("vol-{}", vol_id).into_bytes()
}
