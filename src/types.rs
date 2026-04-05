use serde::{Deserialize, Serialize};

/// Volume identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VolumeId(pub String);

impl VolumeId {
    /// FNV-1a hash of the volume ID string, used as blockmap key prefix
    pub fn hash32(&self) -> u32 {
        let mut h: u32 = 0x811c_9dc5;
        for b in self.0.as_bytes() {
            h ^= *b as u32;
            h = h.wrapping_mul(0x0100_0193);
        }
        h
    }
}

impl std::fmt::Display for VolumeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Logical Block Address — offset within a volume, in units of BLOCK_SIZE
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Lba(pub u64);

/// Physical Block Address — offset on data device, in units of BLOCK_SIZE
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Pba(pub u64);

/// Zone identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ZoneId(pub u32);

pub const BLOCK_SIZE: u32 = 4096;
pub const SECTOR_SIZE: u32 = 512;
pub const SECTORS_PER_BLOCK: u32 = BLOCK_SIZE / SECTOR_SIZE;

pub const BLOCK_HEADER_MAGIC: u32 = 0x4F4E_5958; // "ONYX"
pub const BLOCK_HEADER_VERSION: u8 = 1;

/// On-disk block header prepended to every data block on LV3
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct BlockHeader {
    /// Magic number: 0x4F4E5958 ("ONYX")
    pub magic: u32,
    /// Format version
    pub version: u8,
    /// Compression algorithm: 0=none, 1=lz4, 2=zstd
    pub compression: u8,
    pub _pad: [u8; 2],
    /// Uncompressed data size in bytes
    pub original_size: u32,
    /// Compressed payload size in bytes
    pub compressed_size: u32,
    /// CRC32 of compressed payload
    pub crc32: u32,
    /// FNV-1a hash of volume ID (for debugging/recovery)
    pub volume_id_hash: u32,
    /// Source LBA (for recovery cross-check)
    pub lba: u64,
}

const _: () = assert!(std::mem::size_of::<BlockHeader>() == 32);

impl BlockHeader {
    pub fn new(
        compression: u8,
        original_size: u32,
        compressed_size: u32,
        crc32: u32,
        volume_id_hash: u32,
        lba: Lba,
    ) -> Self {
        Self {
            magic: BLOCK_HEADER_MAGIC,
            version: BLOCK_HEADER_VERSION,
            compression,
            _pad: [0; 2],
            original_size,
            compressed_size,
            crc32,
            volume_id_hash,
            lba: lba.0,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                std::mem::size_of::<Self>(),
            )
        }
    }

    pub fn from_bytes(bytes: &[u8; 32]) -> Self {
        unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const Self) }
    }

    pub fn is_valid(&self) -> bool {
        self.magic == BLOCK_HEADER_MAGIC && self.version == BLOCK_HEADER_VERSION
    }
}

/// Compression algorithm selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionAlgo {
    None,
    Lz4,
    Zstd { level: i32 },
}

impl CompressionAlgo {
    pub fn to_u8(self) -> u8 {
        match self {
            CompressionAlgo::None => 0,
            CompressionAlgo::Lz4 => 1,
            CompressionAlgo::Zstd { .. } => 2,
        }
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(CompressionAlgo::None),
            1 => Some(CompressionAlgo::Lz4),
            2 => Some(CompressionAlgo::Zstd { level: 3 }),
            _ => None,
        }
    }
}

/// Volume configuration stored in RocksDB "volumes" CF
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeConfig {
    pub id: VolumeId,
    pub size_bytes: u64,
    pub block_size: u32,
    pub compression: CompressionAlgo,
    pub created_at: u64,
    pub zone_count: u32,
}
