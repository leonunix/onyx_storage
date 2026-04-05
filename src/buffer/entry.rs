use crate::error::{OnyxError, OnyxResult};
use crate::io::aligned::round_up;
use crate::meta::schema::MAX_VOLUME_ID_BYTES;
use crate::types::{Lba, BLOCK_SIZE};

pub const BUFFER_SUPERBLOCK_MAGIC: u32 = 0x4F42_5546; // "OBUF"
pub const BUFFER_ENTRY_MAGIC: u32 = 0x4245_4E54; // "BENT"
pub const BUFFER_SUPERBLOCK_SIZE: u64 = 4096;

/// Minimum entry size: fixed header (40 bytes) + at least 1 byte vol_id, 4KB-aligned = 4096
pub const MIN_ENTRY_SIZE: u32 = 4096;
/// Maximum entry size: 40 + 255 + 256*4096 ~= 1MB+. Cap at 2MB for sanity.
pub const MAX_ENTRY_SIZE: u32 = 2 * 1024 * 1024;

/// Fixed header size in bytes.
const FIXED_HEADER_SIZE: usize = 40;

// ──────────────────────────── Superblock ────────────────────────────

/// Write buffer superblock (4096 bytes). Tracks byte offsets, not slot indices.
///
/// Layout:
///   0.. 4: magic
///   4.. 8: version
///   8..16: head_offset (next write position, bytes from device start)
///  16..24: tail_offset (oldest unflushed entry, bytes from device start)
///  24..32: capacity_bytes (total data area size in bytes)
///  32..40: used_bytes (current bytes occupied, disambiguates full vs empty when head==tail)
///  40..44: crc32
#[derive(Debug, Clone, Copy)]
pub struct BufferSuperblock {
    pub magic: u32,
    pub version: u32,
    pub head_offset: u64,
    pub tail_offset: u64,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub crc32: u32,
}

impl BufferSuperblock {
    pub fn new(capacity_bytes: u64) -> Self {
        let mut sb = Self {
            magic: BUFFER_SUPERBLOCK_MAGIC,
            version: 2,
            head_offset: BUFFER_SUPERBLOCK_SIZE,
            tail_offset: BUFFER_SUPERBLOCK_SIZE,
            capacity_bytes,
            used_bytes: 0,
            crc32: 0,
        };
        sb.update_crc();
        sb
    }

    pub fn to_bytes(&self) -> [u8; 4096] {
        let mut buf = [0u8; 4096];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..16].copy_from_slice(&self.head_offset.to_le_bytes());
        buf[16..24].copy_from_slice(&self.tail_offset.to_le_bytes());
        buf[24..32].copy_from_slice(&self.capacity_bytes.to_le_bytes());
        buf[32..40].copy_from_slice(&self.used_bytes.to_le_bytes());
        buf[40..44].copy_from_slice(&self.crc32.to_le_bytes());
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
            head_offset: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            tail_offset: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            capacity_bytes: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            used_bytes: u64::from_le_bytes(buf[32..40].try_into().unwrap()),
            crc32: u32::from_le_bytes(buf[40..44].try_into().unwrap()),
        };
        let expected_crc = crc32fast::hash(&buf[0..40]);
        if sb.crc32 != expected_crc {
            return None;
        }
        Some(sb)
    }

    pub fn update_crc(&mut self) {
        let mut tmp = [0u8; 40];
        tmp[0..4].copy_from_slice(&self.magic.to_le_bytes());
        tmp[4..8].copy_from_slice(&self.version.to_le_bytes());
        tmp[8..16].copy_from_slice(&self.head_offset.to_le_bytes());
        tmp[16..24].copy_from_slice(&self.tail_offset.to_le_bytes());
        tmp[24..32].copy_from_slice(&self.capacity_bytes.to_le_bytes());
        tmp[32..40].copy_from_slice(&self.used_bytes.to_le_bytes());
        self.crc32 = crc32fast::hash(&tmp);
    }

    /// End of the data area (device end).
    pub fn data_end(&self) -> u64 {
        BUFFER_SUPERBLOCK_SIZE + self.capacity_bytes
    }

    /// Check if there's room for `entry_len` more bytes.
    /// Uses the explicit `used_bytes` counter to avoid head==tail ambiguity.
    pub fn has_room(&self, entry_len: u64) -> bool {
        let free = self.capacity_bytes.saturating_sub(self.used_bytes);
        entry_len <= free
    }
}

// ──────────────────────────── BufferEntry ────────────────────────────

/// Variable-length buffer entry. Stores raw (uncompressed) data for one or more
/// contiguous LBAs from the same volume.
///
/// On-disk layout (4KB-aligned total size):
///   0.. 4: total_len (u32 LE, 4KB-aligned)
///   4.. 8: magic
///   8..16: seq (u64 LE)
///  16..24: start_lba (u64 LE)
///  24..28: lba_count (u32 LE)
///  28..30: vol_id_len (u16 LE)
///  30    : flushed flag
///  31    : reserved
///  32..36: payload_crc32
///  36..40: entry_crc32 (covers 0..36 + 40..data_end, excludes 36..40)
///  40 .. 40+vol_id_len              : vol_id
///  40+vol_id_len .. +payload_len    : payload (lba_count * BLOCK_SIZE raw)
///  ... zero padding to 4KB alignment
#[derive(Debug, Clone)]
pub struct BufferEntry {
    pub seq: u64,
    pub vol_id: String,
    pub start_lba: Lba,
    pub lba_count: u32,
    pub payload_crc32: u32,
    pub flushed: bool,
    pub payload: Vec<u8>,
}

impl BufferEntry {
    /// Compute the 4KB-aligned disk size for this entry.
    pub fn disk_size(&self) -> u32 {
        let raw = FIXED_HEADER_SIZE + self.vol_id.len() + self.payload.len();
        round_up(raw, BLOCK_SIZE as usize) as u32
    }

    /// Serialize to a 4KB-aligned byte vector.
    pub fn to_bytes(&self) -> OnyxResult<Vec<u8>> {
        let id_bytes = self.vol_id.as_bytes();
        if id_bytes.len() > MAX_VOLUME_ID_BYTES {
            return Err(OnyxError::Config(format!(
                "vol_id too long: {} bytes (max {})",
                id_bytes.len(),
                MAX_VOLUME_ID_BYTES,
            )));
        }

        let total_len = self.disk_size();
        let mut buf = vec![0u8; total_len as usize];

        // Fixed header
        buf[0..4].copy_from_slice(&total_len.to_le_bytes());
        buf[4..8].copy_from_slice(&BUFFER_ENTRY_MAGIC.to_le_bytes());
        buf[8..16].copy_from_slice(&self.seq.to_le_bytes());
        buf[16..24].copy_from_slice(&self.start_lba.0.to_le_bytes());
        buf[24..28].copy_from_slice(&self.lba_count.to_le_bytes());
        buf[28..30].copy_from_slice(&(id_bytes.len() as u16).to_le_bytes());
        buf[30] = if self.flushed { 1 } else { 0 };
        // 31: reserved
        buf[32..36].copy_from_slice(&self.payload_crc32.to_le_bytes());
        // 36..40: entry_crc32, filled below

        // Variable: vol_id
        let vid_start = FIXED_HEADER_SIZE;
        buf[vid_start..vid_start + id_bytes.len()].copy_from_slice(id_bytes);

        // Variable: payload
        let payload_start = vid_start + id_bytes.len();
        buf[payload_start..payload_start + self.payload.len()].copy_from_slice(&self.payload);

        // Entry CRC: covers 0..36 + 40..data_end (excludes CRC field at 36..40)
        let data_end = payload_start + self.payload.len();
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf[0..36]);
        hasher.update(&buf[40..data_end]);
        let entry_crc = hasher.finalize();
        buf[36..40].copy_from_slice(&entry_crc.to_le_bytes());

        Ok(buf)
    }

    /// Deserialize from a byte slice. The slice must be at least `total_len` bytes.
    /// Returns None if magic, CRC, or UTF-8 checks fail.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < FIXED_HEADER_SIZE {
            return None;
        }

        let total_len = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if (total_len as usize) < FIXED_HEADER_SIZE || total_len > MAX_ENTRY_SIZE {
            return None;
        }
        if buf.len() < total_len as usize {
            return None;
        }

        let magic = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if magic != BUFFER_ENTRY_MAGIC {
            return None;
        }

        let vol_id_len = u16::from_le_bytes(buf[28..30].try_into().unwrap()) as usize;
        if vol_id_len > MAX_VOLUME_ID_BYTES {
            return None;
        }

        let lba_count = u32::from_le_bytes(buf[24..28].try_into().unwrap());
        let payload_len = lba_count as usize * BLOCK_SIZE as usize;
        let data_end = FIXED_HEADER_SIZE + vol_id_len + payload_len;
        if data_end > total_len as usize {
            return None;
        }

        // Verify entry CRC
        let stored_crc = u32::from_le_bytes(buf[36..40].try_into().unwrap());
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf[0..36]);
        hasher.update(&buf[40..data_end]);
        let computed_crc = hasher.finalize();
        if stored_crc != computed_crc {
            return None;
        }

        // vol_id must be valid UTF-8
        let vid_start = FIXED_HEADER_SIZE;
        let vol_id = std::str::from_utf8(&buf[vid_start..vid_start + vol_id_len])
            .ok()?
            .to_string();

        let payload_start = vid_start + vol_id_len;
        let payload = buf[payload_start..payload_start + payload_len].to_vec();

        // Verify payload CRC
        let payload_crc32 = u32::from_le_bytes(buf[32..36].try_into().unwrap());
        if crc32fast::hash(&payload) != payload_crc32 {
            return None;
        }

        Some(Self {
            seq: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            vol_id,
            start_lba: Lba(u64::from_le_bytes(buf[16..24].try_into().unwrap())),
            lba_count,
            payload_crc32,
            flushed: buf[30] != 0,
            payload,
        })
    }
}
