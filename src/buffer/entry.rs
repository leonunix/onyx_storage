use std::sync::Arc;

use crate::error::{OnyxError, OnyxResult};
use crate::io::aligned::{round_up, AlignedBuf};
use crate::meta::schema::MAX_VOLUME_ID_BYTES;
use crate::types::{Lba, BLOCK_SIZE};

pub const BUFFER_SUPERBLOCK_MAGIC: u32 = 0x4F42_5546; // "OBUF"
pub const BUFFER_ENTRY_MAGIC: u32 = 0x4245_4E54; // "BENT"
pub const BUFFER_SUPERBLOCK_SIZE: u64 = 4096;

/// Minimum entry size: fixed header (48 bytes) + at least 1 byte vol_id, 4KB-aligned = 4096
pub const MIN_ENTRY_SIZE: u32 = 4096;
/// Maximum entry size: 40 + 255 + 256*4096 ~= 1MB+. Cap at 2MB for sanity.
pub const MAX_ENTRY_SIZE: u32 = 2 * 1024 * 1024;

const CURRENT_SUPERBLOCK_VERSION: u32 = 3;
/// Fixed header size in bytes for all supported entry formats.
const FIXED_HEADER_SIZE: usize = 48;
/// Full-entry CRC format used by the direct-IO path.
const FULL_CRC_HEADER_VERSION: u8 = 1;
/// Compact buffered append format: payload is written first and the header is
/// written last, so the header alone acts as the durable commit record.
const COMPACT_HEADER_VERSION: u8 = 2;
/// Direct-IO compact format: payload blocks are written first starting at the
/// next 4KB slot, then a single 4KB header slot is written as the commit record.
const DIRECT_COMPACT_HEADER_VERSION: u8 = 3;

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
///  44..48: shard_count (current format)
#[derive(Debug, Clone, Copy)]
pub struct BufferSuperblock {
    pub magic: u32,
    pub version: u32,
    pub head_offset: u64,
    pub tail_offset: u64,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub crc32: u32,
    pub shard_count: u32,
}

impl BufferSuperblock {
    pub fn new(capacity_bytes: u64) -> Self {
        Self::new_with_shards(capacity_bytes, 1)
    }

    pub fn new_with_shards(capacity_bytes: u64, shard_count: u32) -> Self {
        let mut sb = Self {
            magic: BUFFER_SUPERBLOCK_MAGIC,
            version: CURRENT_SUPERBLOCK_VERSION,
            head_offset: BUFFER_SUPERBLOCK_SIZE,
            tail_offset: BUFFER_SUPERBLOCK_SIZE,
            capacity_bytes,
            used_bytes: 0,
            crc32: 0,
            shard_count: shard_count.max(1),
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
        buf[44..48].copy_from_slice(&self.shard_count.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8; 4096]) -> Option<Self> {
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != BUFFER_SUPERBLOCK_MAGIC {
            return None;
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if version != CURRENT_SUPERBLOCK_VERSION {
            return None;
        }

        let mut crc_buf = [0u8; 48];
        crc_buf.copy_from_slice(&buf[0..48]);
        crc_buf[40..44].fill(0);
        let expected_crc = crc32fast::hash(&crc_buf);

        let sb = Self {
            magic,
            version,
            head_offset: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            tail_offset: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            capacity_bytes: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            used_bytes: u64::from_le_bytes(buf[32..40].try_into().unwrap()),
            crc32: u32::from_le_bytes(buf[40..44].try_into().unwrap()),
            shard_count: u32::from_le_bytes(buf[44..48].try_into().unwrap()).max(1),
        };
        if sb.crc32 != expected_crc {
            return None;
        }
        Some(sb)
    }

    pub fn update_crc(&mut self) {
        let mut tmp = [0u8; 48];
        tmp[0..4].copy_from_slice(&self.magic.to_le_bytes());
        tmp[4..8].copy_from_slice(&self.version.to_le_bytes());
        tmp[8..16].copy_from_slice(&self.head_offset.to_le_bytes());
        tmp[16..24].copy_from_slice(&self.tail_offset.to_le_bytes());
        tmp[24..32].copy_from_slice(&self.capacity_bytes.to_le_bytes());
        tmp[32..40].copy_from_slice(&self.used_bytes.to_le_bytes());
        tmp[44..48].copy_from_slice(&self.shard_count.to_le_bytes());
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
///  31    : header_version (1 = 48-byte full CRC, 2 = 48-byte header-only CRC)
///  32..36: payload_crc32
///  36..40: entry_crc32
///  40..48: vol_created_at (u64 LE) — volume generation epoch
///  48 .. 48+vol_id_len              : vol_id
///  48+vol_id_len .. +payload_len    : payload (lba_count * BLOCK_SIZE raw)
///  ... zero padding to 4KB alignment
///
/// Header version 2 entries are used by the buffered fast path. Their entry CRC
/// covers only header bytes (including vol_id); payload integrity relies on the
/// write ordering: payload first, header last.
#[derive(Debug, Clone)]
pub struct BufferEntry {
    pub seq: u64,
    pub vol_id: String,
    pub start_lba: Lba,
    pub lba_count: u32,
    pub payload_crc32: u32,
    pub flushed: bool,
    /// Volume generation epoch (VolumeConfig.created_at at write time).
    pub vol_created_at: u64,
    pub payload: Arc<[u8]>,
}

impl BufferEntry {
    pub fn encode_compact_parts(
        seq: u64,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        flushed: bool,
        vol_created_at: u64,
        payload: &[u8],
    ) -> OnyxResult<(Vec<u8>, u32)> {
        let id_bytes = vol_id.as_bytes();
        if id_bytes.len() > MAX_VOLUME_ID_BYTES {
            return Err(OnyxError::Config(format!(
                "vol_id too long: {} bytes (max {})",
                id_bytes.len(),
                MAX_VOLUME_ID_BYTES,
            )));
        }

        let total_len = Self::raw_size_for(vol_id, payload.len()) as u32;
        let mut header = vec![0u8; FIXED_HEADER_SIZE + id_bytes.len()];
        header[0..4].copy_from_slice(&total_len.to_le_bytes());
        header[4..8].copy_from_slice(&BUFFER_ENTRY_MAGIC.to_le_bytes());
        header[8..16].copy_from_slice(&seq.to_le_bytes());
        header[16..24].copy_from_slice(&start_lba.0.to_le_bytes());
        header[24..28].copy_from_slice(&lba_count.to_le_bytes());
        header[28..30].copy_from_slice(&(id_bytes.len() as u16).to_le_bytes());
        header[30] = if flushed { 1 } else { 0 };
        header[31] = COMPACT_HEADER_VERSION;
        header[40..48].copy_from_slice(&vol_created_at.to_le_bytes());
        header[FIXED_HEADER_SIZE..FIXED_HEADER_SIZE + id_bytes.len()].copy_from_slice(id_bytes);

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header[0..36]);
        hasher.update(&header[40..]);
        let entry_crc = hasher.finalize();
        header[36..40].copy_from_slice(&entry_crc.to_le_bytes());

        Ok((header, total_len))
    }

    fn encode_full_into_slice(
        seq: u64,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        payload_crc32: u32,
        flushed: bool,
        vol_created_at: u64,
        payload: &[u8],
        total_len: u32,
        buf: &mut [u8],
    ) -> OnyxResult<()> {
        let id_bytes = vol_id.as_bytes();
        if id_bytes.len() > MAX_VOLUME_ID_BYTES {
            return Err(OnyxError::Config(format!(
                "vol_id too long: {} bytes (max {})",
                id_bytes.len(),
                MAX_VOLUME_ID_BYTES,
            )));
        }

        let raw = FIXED_HEADER_SIZE + id_bytes.len() + payload.len();
        if total_len < raw as u32 {
            return Err(OnyxError::Config(format!(
                "total_len {} smaller than raw entry size {}",
                total_len, raw
            )));
        }
        if buf.len() < total_len as usize {
            return Err(OnyxError::Config(format!(
                "destination buffer too small: need {}, got {}",
                total_len,
                buf.len()
            )));
        }
        buf[..total_len as usize].fill(0);

        buf[0..4].copy_from_slice(&total_len.to_le_bytes());
        buf[4..8].copy_from_slice(&BUFFER_ENTRY_MAGIC.to_le_bytes());
        buf[8..16].copy_from_slice(&seq.to_le_bytes());
        buf[16..24].copy_from_slice(&start_lba.0.to_le_bytes());
        buf[24..28].copy_from_slice(&lba_count.to_le_bytes());
        buf[28..30].copy_from_slice(&(id_bytes.len() as u16).to_le_bytes());
        buf[30] = if flushed { 1 } else { 0 };
        buf[31] = FULL_CRC_HEADER_VERSION;
        buf[32..36].copy_from_slice(&payload_crc32.to_le_bytes());
        buf[40..48].copy_from_slice(&vol_created_at.to_le_bytes());

        let vid_start = FIXED_HEADER_SIZE;
        buf[vid_start..vid_start + id_bytes.len()].copy_from_slice(id_bytes);

        let payload_start = vid_start + id_bytes.len();
        buf[payload_start..payload_start + payload.len()].copy_from_slice(payload);

        let data_end = payload_start + payload.len();
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf[0..36]);
        hasher.update(&buf[40..data_end]);
        let entry_crc = hasher.finalize();
        buf[36..40].copy_from_slice(&entry_crc.to_le_bytes());

        Ok(())
    }

    fn encode_with_total_len(
        seq: u64,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        payload_crc32: u32,
        flushed: bool,
        vol_created_at: u64,
        payload: &[u8],
        total_len: u32,
    ) -> OnyxResult<Vec<u8>> {
        let mut buf = vec![0u8; total_len as usize];
        Self::encode_full_into_slice(
            seq,
            vol_id,
            start_lba,
            lba_count,
            payload_crc32,
            flushed,
            vol_created_at,
            payload,
            total_len,
            &mut buf,
        )?;
        Ok(buf)
    }

    pub fn raw_size_for(vol_id: &str, payload_len: usize) -> usize {
        FIXED_HEADER_SIZE + vol_id.len() + payload_len
    }

    pub fn encode(
        seq: u64,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        payload_crc32: u32,
        flushed: bool,
        vol_created_at: u64,
        payload: &[u8],
    ) -> OnyxResult<Vec<u8>> {
        let raw = Self::raw_size_for(vol_id, payload.len());
        let total_len = round_up(raw, BLOCK_SIZE as usize) as u32;
        Self::encode_with_total_len(
            seq,
            vol_id,
            start_lba,
            lba_count,
            payload_crc32,
            flushed,
            vol_created_at,
            payload,
            total_len,
        )
    }

    /// Serialize directly into a 4KB-aligned buffer for O_DIRECT writes.
    pub fn encode_aligned(
        seq: u64,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        payload_crc32: u32,
        flushed: bool,
        vol_created_at: u64,
        payload: &[u8],
    ) -> OnyxResult<AlignedBuf> {
        let raw = Self::raw_size_for(vol_id, payload.len());
        let total_len = round_up(raw, BLOCK_SIZE as usize) as u32;
        let mut buf = AlignedBuf::new(total_len as usize, false)?;
        Self::encode_into_aligned(
            seq,
            vol_id,
            start_lba,
            lba_count,
            payload_crc32,
            flushed,
            vol_created_at,
            payload,
            &mut buf,
        )?;
        Ok(buf)
    }

    /// Serialize into an existing 4KB-aligned buffer. Returns the encoded length.
    pub fn encode_into_aligned(
        seq: u64,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        payload_crc32: u32,
        flushed: bool,
        vol_created_at: u64,
        payload: &[u8],
        buf: &mut AlignedBuf,
    ) -> OnyxResult<u32> {
        let raw = Self::raw_size_for(vol_id, payload.len());
        let total_len = round_up(raw, BLOCK_SIZE as usize) as u32;
        if buf.len() < total_len as usize {
            return Err(OnyxError::Config(format!(
                "aligned buffer too small: need {}, got {}",
                total_len,
                buf.len()
            )));
        }
        Self::encode_full_into_slice(
            seq,
            vol_id,
            start_lba,
            lba_count,
            payload_crc32,
            flushed,
            vol_created_at,
            payload,
            total_len,
            buf.as_mut_slice(),
        )?;
        Ok(total_len)
    }

    pub fn direct_compact_total_len(payload_len: usize) -> u32 {
        (BLOCK_SIZE as usize + payload_len) as u32
    }

    pub fn encode_direct_compact_header(
        seq: u64,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        flushed: bool,
        vol_created_at: u64,
        payload_len: usize,
    ) -> OnyxResult<AlignedBuf> {
        let mut buf = AlignedBuf::new(BLOCK_SIZE as usize, false)?;
        Self::encode_direct_compact_header_into(
            seq,
            vol_id,
            start_lba,
            lba_count,
            flushed,
            vol_created_at,
            payload_len,
            &mut buf,
        )?;
        Ok(buf)
    }

    pub fn encode_direct_compact_header_into(
        seq: u64,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        flushed: bool,
        vol_created_at: u64,
        payload_len: usize,
        buf: &mut AlignedBuf,
    ) -> OnyxResult<u32> {
        let id_bytes = vol_id.as_bytes();
        if id_bytes.len() > MAX_VOLUME_ID_BYTES {
            return Err(OnyxError::Config(format!(
                "vol_id too long: {} bytes (max {})",
                id_bytes.len(),
                MAX_VOLUME_ID_BYTES,
            )));
        }

        let header_end = FIXED_HEADER_SIZE + id_bytes.len();
        if header_end > BLOCK_SIZE as usize {
            return Err(OnyxError::Config(format!(
                "direct compact header exceeds one slot: {} bytes",
                header_end
            )));
        }
        if buf.len() < BLOCK_SIZE as usize {
            return Err(OnyxError::Config(format!(
                "aligned buffer too small: need {}, got {}",
                BLOCK_SIZE,
                buf.len()
            )));
        }

        let total_len = Self::direct_compact_total_len(payload_len);
        let slot = &mut buf.as_mut_slice()[..BLOCK_SIZE as usize];
        slot.fill(0);
        slot[0..4].copy_from_slice(&total_len.to_le_bytes());
        slot[4..8].copy_from_slice(&BUFFER_ENTRY_MAGIC.to_le_bytes());
        slot[8..16].copy_from_slice(&seq.to_le_bytes());
        slot[16..24].copy_from_slice(&start_lba.0.to_le_bytes());
        slot[24..28].copy_from_slice(&lba_count.to_le_bytes());
        slot[28..30].copy_from_slice(&(id_bytes.len() as u16).to_le_bytes());
        slot[30] = if flushed { 1 } else { 0 };
        slot[31] = DIRECT_COMPACT_HEADER_VERSION;
        slot[32..36].fill(0);
        slot[40..48].copy_from_slice(&vol_created_at.to_le_bytes());
        slot[FIXED_HEADER_SIZE..header_end].copy_from_slice(id_bytes);

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&slot[0..36]);
        hasher.update(&slot[40..header_end]);
        let entry_crc = hasher.finalize();
        slot[36..40].copy_from_slice(&entry_crc.to_le_bytes());
        Ok(total_len)
    }

    pub fn encode_compact(
        seq: u64,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        payload_crc32: u32,
        flushed: bool,
        vol_created_at: u64,
        payload: &[u8],
    ) -> OnyxResult<Vec<u8>> {
        let raw = Self::raw_size_for(vol_id, payload.len()) as u32;
        Self::encode_with_total_len(
            seq,
            vol_id,
            start_lba,
            lba_count,
            payload_crc32,
            flushed,
            vol_created_at,
            payload,
            raw,
        )
    }

    /// Compute the 4KB-aligned disk size for this entry.
    pub fn disk_size(&self) -> u32 {
        let raw = FIXED_HEADER_SIZE + self.vol_id.len() + self.payload.len();
        round_up(raw, BLOCK_SIZE as usize) as u32
    }

    /// Serialize to a 4KB-aligned byte vector.
    pub fn to_bytes(&self) -> OnyxResult<Vec<u8>> {
        Self::encode(
            self.seq,
            &self.vol_id,
            self.start_lba,
            self.lba_count,
            self.payload_crc32,
            self.flushed,
            self.vol_created_at,
            &self.payload,
        )
    }

    /// Deserialize from a byte slice. The slice must be at least `total_len` bytes.
    /// Returns None if magic, CRC, or UTF-8 checks fail.
    /// Supports only the current 48-byte entry formats:
    /// version 1 (full CRC), version 2 (header-only CRC), and
    /// version 3 (direct compact header slot).
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

        let header_version = buf[31];
        if header_version != FULL_CRC_HEADER_VERSION
            && header_version != COMPACT_HEADER_VERSION
            && header_version != DIRECT_COMPACT_HEADER_VERSION
        {
            return None;
        }

        let vol_id_len = u16::from_le_bytes(buf[28..30].try_into().unwrap()) as usize;
        if vol_id_len > MAX_VOLUME_ID_BYTES {
            return None;
        }

        let lba_count = u32::from_le_bytes(buf[24..28].try_into().unwrap());
        let payload_len = lba_count as usize * BLOCK_SIZE as usize;
        let payload_start = if header_version == DIRECT_COMPACT_HEADER_VERSION {
            BLOCK_SIZE as usize
        } else {
            FIXED_HEADER_SIZE + vol_id_len
        };
        let data_end = payload_start + payload_len;
        if data_end > total_len as usize {
            return None;
        }

        let header_end = FIXED_HEADER_SIZE + vol_id_len;

        // Verify entry CRC. Compact v4 entries only cover header bytes because
        // payload is written before the header commit record.
        let stored_crc = u32::from_le_bytes(buf[36..40].try_into().unwrap());
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf[0..36]);
        if header_version >= COMPACT_HEADER_VERSION {
            hasher.update(&buf[40..header_end]);
        } else {
            hasher.update(&buf[40..data_end]);
        }
        let computed_crc = hasher.finalize();
        if stored_crc != computed_crc {
            return None;
        }

        let vol_created_at = u64::from_le_bytes(buf[40..48].try_into().unwrap());

        // vol_id must be valid UTF-8
        let vid_start = FIXED_HEADER_SIZE;
        let vol_id = std::str::from_utf8(&buf[vid_start..vid_start + vol_id_len])
            .ok()?
            .to_string();

        let payload: Arc<[u8]> = Arc::from(&buf[payload_start..payload_start + payload_len]);

        let payload_crc32 = u32::from_le_bytes(buf[32..36].try_into().unwrap());
        if header_version == FULL_CRC_HEADER_VERSION && crc32fast::hash(&payload) != payload_crc32 {
            return None;
        }

        Some(Self {
            seq: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            vol_id,
            start_lba: Lba(u64::from_le_bytes(buf[16..24].try_into().unwrap())),
            lba_count,
            payload_crc32,
            flushed: buf[30] != 0,
            vol_created_at,
            payload,
        })
    }

    /// Parse only metadata from the first 4KB header block, without reading
    /// the payload. Returns `(entry_without_payload, slot_count)`.
    /// CRC is validated for compact/direct-compact formats (header-only CRC).
    /// For full-CRC v1 entries, payload CRC is deferred to hydration time.
    pub fn from_header_block(buf: &[u8]) -> Option<(Self, u32)> {
        if buf.len() < FIXED_HEADER_SIZE {
            return None;
        }

        let total_len = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if (total_len as usize) < FIXED_HEADER_SIZE || total_len > MAX_ENTRY_SIZE {
            return None;
        }

        let magic = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if magic != BUFFER_ENTRY_MAGIC {
            return None;
        }

        let header_version = buf[31];
        if header_version != FULL_CRC_HEADER_VERSION
            && header_version != COMPACT_HEADER_VERSION
            && header_version != DIRECT_COMPACT_HEADER_VERSION
        {
            return None;
        }

        let vol_id_len = u16::from_le_bytes(buf[28..30].try_into().unwrap()) as usize;
        if vol_id_len > MAX_VOLUME_ID_BYTES {
            return None;
        }

        let header_end = FIXED_HEADER_SIZE + vol_id_len;
        if header_end > buf.len() {
            return None;
        }

        // Verify header CRC. For compact formats, CRC covers only header bytes
        // which fit in the first 4KB block. For full-CRC v1, we can only validate
        // the header portion — payload CRC is deferred.
        let stored_crc = u32::from_le_bytes(buf[36..40].try_into().unwrap());
        if header_version >= COMPACT_HEADER_VERSION {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&buf[0..36]);
            hasher.update(&buf[40..header_end]);
            if stored_crc != hasher.finalize() {
                return None;
            }
        }
        // For FULL_CRC_HEADER_VERSION: CRC covers header + payload.
        // We can't validate it without the payload, so we skip CRC here and
        // rely on payload_crc32 validation at hydration time.

        let lba_count = u32::from_le_bytes(buf[24..28].try_into().unwrap());
        let slot_count = round_up(total_len as usize, BLOCK_SIZE as usize) as u32 / BLOCK_SIZE;

        let vol_id = std::str::from_utf8(&buf[FIXED_HEADER_SIZE..header_end])
            .ok()?
            .to_string();

        Some((
            Self {
                seq: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
                vol_id,
                start_lba: Lba(u64::from_le_bytes(buf[16..24].try_into().unwrap())),
                lba_count,
                payload_crc32: u32::from_le_bytes(buf[32..36].try_into().unwrap()),
                flushed: buf[30] != 0,
                vol_created_at: u64::from_le_bytes(buf[40..48].try_into().unwrap()),
                payload: Arc::from(Vec::new()),
            },
            slot_count,
        ))
    }
}
