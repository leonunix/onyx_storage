use crate::error::{OnyxError, OnyxResult};
use crate::meta::schema::MAX_VOLUME_ID_BYTES;
use crate::types::Lba;

pub const BUFFER_SUPERBLOCK_MAGIC: u32 = 0x4F42_5546; // "OBUF"
pub const BUFFER_ENTRY_MAGIC: u32 = 0x4245_4E54; // "BENT"
pub const BUFFER_SUPERBLOCK_SIZE: u64 = 4096;
pub const BUFFER_ENTRY_SIZE: u64 = 8192; // 8KB per slot

/// Write buffer superblock at offset 0 of the buffer device.
#[derive(Debug, Clone, Copy)]
pub struct BufferSuperblock {
    pub magic: u32,
    pub version: u32,
    pub head_seq: u64,
    pub tail_seq: u64,
    pub capacity_entries: u64,
    pub crc32: u32,
}

impl BufferSuperblock {
    pub fn new(capacity_entries: u64) -> Self {
        let mut sb = Self {
            magic: BUFFER_SUPERBLOCK_MAGIC,
            version: 1,
            head_seq: 0,
            tail_seq: 0,
            capacity_entries,
            crc32: 0,
        };
        sb.update_crc();
        sb
    }

    pub fn to_bytes(&self) -> [u8; 4096] {
        let mut buf = [0u8; 4096];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..16].copy_from_slice(&self.head_seq.to_le_bytes());
        buf[16..24].copy_from_slice(&self.tail_seq.to_le_bytes());
        buf[24..32].copy_from_slice(&self.capacity_entries.to_le_bytes());
        buf[32..36].copy_from_slice(&self.crc32.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8; 4096]) -> Option<Self> {
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != BUFFER_SUPERBLOCK_MAGIC {
            return None;
        }
        let sb = Self {
            magic,
            version: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            head_seq: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            tail_seq: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            capacity_entries: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            crc32: u32::from_le_bytes(buf[32..36].try_into().unwrap()),
        };
        let expected_crc = crc32fast::hash(&buf[0..32]);
        if sb.crc32 != expected_crc {
            return None;
        }
        Some(sb)
    }

    pub fn update_crc(&mut self) {
        let mut tmp = [0u8; 32];
        tmp[0..4].copy_from_slice(&self.magic.to_le_bytes());
        tmp[4..8].copy_from_slice(&self.version.to_le_bytes());
        tmp[8..16].copy_from_slice(&self.head_seq.to_le_bytes());
        tmp[16..24].copy_from_slice(&self.tail_seq.to_le_bytes());
        tmp[24..32].copy_from_slice(&self.capacity_entries.to_le_bytes());
        self.crc32 = crc32fast::hash(&tmp);
    }

    pub fn is_valid(&self) -> bool {
        self.magic == BUFFER_SUPERBLOCK_MAGIC
    }

    pub fn offset_for_seq(&self, seq: u64) -> u64 {
        let slot = seq % self.capacity_entries;
        BUFFER_SUPERBLOCK_SIZE + slot * BUFFER_ENTRY_SIZE
    }

    pub fn pending_count(&self) -> u64 {
        self.head_seq - self.tail_seq
    }

    pub fn is_full(&self) -> bool {
        self.pending_count() >= self.capacity_entries
    }
}

/// Entry fixed-field layout (40 bytes):
///   0.. 4: magic
///   4..12: seq
///  12..20: lba
///  20    : compression
///  21    : flushed flag
///  22..24: vol_id_len (u16 LE, supports up to 255 consistently with schema)
///  24..28: original_size
///  28..32: compressed_size
///  32..36: payload_crc32
///  36..40: entry_crc32 (covers fixed fields + vol_id + payload)
///
/// Variable region (offset 40..):
///  40 .. 40+vol_id_len: vol_id bytes
///  40+vol_id_len .. 40+vol_id_len+compressed_size: payload
///
/// Max capacity check:
///  40 + 255 + 4096 = 4391 < 8192, so even worst case fits.
const FIXED_HEADER_SIZE: usize = 40;

/// Write buffer entry (serialized into an 8KB slot).
///
/// CRC covers the entire entry: fixed fields (0..36) + vol_id + payload.
/// On deserialization, any corruption in any part → returns None.
#[derive(Debug, Clone)]
pub struct BufferEntry {
    pub seq: u64,
    pub vol_id: String,
    pub lba: Lba,
    pub compression: u8,
    pub original_size: u32,
    pub compressed_size: u32,
    pub payload_crc32: u32,
    pub flushed: bool,
    pub payload: Vec<u8>,
}

impl BufferEntry {
    /// Serialize entry to an 8KB buffer.
    ///
    /// Returns error if vol_id exceeds MAX_VOLUME_ID_BYTES.
    pub fn to_bytes(&self) -> OnyxResult<[u8; BUFFER_ENTRY_SIZE as usize]> {
        let id_bytes = self.vol_id.as_bytes();
        if id_bytes.len() > MAX_VOLUME_ID_BYTES {
            return Err(OnyxError::Config(format!(
                "buffer entry vol_id too long: {} bytes (max {})",
                id_bytes.len(),
                MAX_VOLUME_ID_BYTES,
            )));
        }
        let id_len = id_bytes.len();

        let mut buf = [0u8; BUFFER_ENTRY_SIZE as usize];

        // Fixed fields
        buf[0..4].copy_from_slice(&BUFFER_ENTRY_MAGIC.to_le_bytes());
        buf[4..12].copy_from_slice(&self.seq.to_le_bytes());
        buf[12..20].copy_from_slice(&self.lba.0.to_le_bytes());
        buf[20] = self.compression;
        buf[21] = if self.flushed { 1 } else { 0 };
        buf[22..24].copy_from_slice(&(id_len as u16).to_le_bytes());
        buf[24..28].copy_from_slice(&self.original_size.to_le_bytes());
        buf[28..32].copy_from_slice(&self.compressed_size.to_le_bytes());
        buf[32..36].copy_from_slice(&self.payload_crc32.to_le_bytes());
        // 36..40 reserved for entry_crc32, filled below

        // Variable: vol_id
        let vol_id_start = FIXED_HEADER_SIZE;
        buf[vol_id_start..vol_id_start + id_len].copy_from_slice(id_bytes);

        // Variable: payload
        let payload_start = vol_id_start + id_len;
        let payload_len = self
            .payload
            .len()
            .min(BUFFER_ENTRY_SIZE as usize - payload_start);
        buf[payload_start..payload_start + payload_len]
            .copy_from_slice(&self.payload[..payload_len]);

        // Entry CRC covers: fixed fields (0..36) + variable data (40..payload_end)
        // Excludes bytes 36..40 (the CRC field itself)
        let crc_end = payload_start + payload_len;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf[0..36]);
        hasher.update(&buf[40..crc_end]);
        let entry_crc = hasher.finalize();
        buf[36..40].copy_from_slice(&entry_crc.to_le_bytes());

        Ok(buf)
    }

    /// Deserialize entry from an 8KB buffer.
    ///
    /// Returns None if magic is wrong, CRC doesn't match, vol_id is invalid UTF-8,
    /// or payload CRC doesn't match. No silent degradation — corrupt entry = None.
    pub fn from_bytes(buf: &[u8; BUFFER_ENTRY_SIZE as usize]) -> Option<Self> {
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != BUFFER_ENTRY_MAGIC {
            return None;
        }

        // Parse fixed fields needed to locate variable data
        let id_len = u16::from_le_bytes(buf[22..24].try_into().unwrap()) as usize;
        if id_len > MAX_VOLUME_ID_BYTES {
            return None;
        }

        let compressed_size = u32::from_le_bytes(buf[28..32].try_into().unwrap()) as usize;

        let vol_id_start = FIXED_HEADER_SIZE;
        let payload_start = vol_id_start + id_len;
        let payload_end = payload_start + compressed_size;

        if payload_end > BUFFER_ENTRY_SIZE as usize {
            return None;
        }

        // Verify entry CRC: covers fixed fields (0..36) + variable data (40..payload_end)
        let stored_crc = u32::from_le_bytes(buf[36..40].try_into().unwrap());
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf[0..36]);
        hasher.update(&buf[40..payload_end]);
        let computed_crc = hasher.finalize();
        if stored_crc != computed_crc {
            return None;
        }

        // Vol_id must be valid UTF-8 — no silent fallback to empty string
        let vol_id = std::str::from_utf8(&buf[vol_id_start..vol_id_start + id_len])
            .ok()?
            .to_string();

        let payload = buf[payload_start..payload_end].to_vec();

        // Verify payload CRC
        let payload_crc32 = u32::from_le_bytes(buf[32..36].try_into().unwrap());
        if crc32fast::hash(&payload) != payload_crc32 {
            return None;
        }

        Some(Self {
            seq: u64::from_le_bytes(buf[4..12].try_into().unwrap()),
            vol_id,
            lba: Lba(u64::from_le_bytes(buf[12..20].try_into().unwrap())),
            compression: buf[20],
            flushed: buf[21] != 0,
            original_size: u32::from_le_bytes(buf[24..28].try_into().unwrap()),
            compressed_size: compressed_size as u32,
            payload_crc32,
            payload,
        })
    }
}
