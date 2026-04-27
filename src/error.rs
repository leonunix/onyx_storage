use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum OnyxError {
    #[error("metadb error: {0}")]
    MetaDb(#[from] onyx_metadb::MetaDbError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Compression error: {0}")]
    Compress(String),

    #[error("Space exhausted: no free PBAs")]
    SpaceExhausted,

    #[error("Volume {0} not found")]
    VolumeNotFound(String),

    #[error("Invalid LBA {lba} for volume {vol_id} (max {max_lba})")]
    InvalidLba {
        vol_id: String,
        lba: u64,
        max_lba: u64,
    },

    #[error("Buffer pool full: {0} bytes in use")]
    BufferPoolFull(usize),

    #[error("CRC mismatch: expected {expected:#x}, got {actual:#x}")]
    CrcMismatch { expected: u32, actual: u32 },

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Device error: {path}: {reason}")]
    Device { path: PathBuf, reason: String },

    #[error("Ublk error: {0}")]
    Ublk(String),

    #[error("IO out of bounds: offset={offset} + len={len} exceeds volume size {size}")]
    OutOfBounds { offset: u64, len: u64, size: u64 },

    #[error("Volume '{0}' has been deleted")]
    VolumeDeleted(String),
}

pub type OnyxResult<T> = Result<T, OnyxError>;
