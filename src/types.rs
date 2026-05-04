use serde::{Deserialize, Serialize};

/// Volume identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VolumeId(pub String);

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

/// Number of blocks reserved at the start of the LV3 data device
/// for superblock, heartbeat, HA lock, and future expansion.
pub const RESERVED_BLOCKS: u64 = 8;

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

/// Volume configuration stored in metadb "volumes" CF
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeConfig {
    pub id: VolumeId,
    pub size_bytes: u64,
    pub block_size: u32,
    pub compression: CompressionAlgo,
    pub created_at: u64,
    pub zone_count: u32,
}
