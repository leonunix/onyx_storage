use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum OnyxError {
    #[error("metadb error: {0}")]
    MetaDb(#[from] onyx_metadb::MetaDbError),

    #[error("chunklet error: {0}")]
    Chunklet(#[from] onyx_chunklet::ChunkletError),

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

    #[error("metadb persistence fenced: writes rejected until restart ({0})")]
    MetaFenced(String),

    /// A GC relocation append was cancelled instead of parking on ring space.
    /// Shutdown latches this (`WriteBufferPool::cancel_relocation_appends`)
    /// because a parked rewrite append pins the gc-runner, `GcRunner::stop()`
    /// cannot interrupt its own join, and the drain that would free the ring runs
    /// after that join. Never an error condition: the candidate is simply
    /// re-selected on the next compactor lap.
    #[error("GC relocation append cancelled (shutting down)")]
    RelocationCancelled,

    /// Graceful shutdown could not drain the LV2 ring. The entries are still
    /// durable (that is what the ring is), the LV3 superblock is deliberately
    /// left dirty, and the next open will replay them — but `stop` must NOT
    /// report success, because the operator's next action (reboot, cable pull,
    /// pool re-init) depends on knowing the ring is not clean.
    #[error("shutdown incomplete: {0}")]
    ShutdownIncomplete(String),
}

pub type OnyxResult<T> = Result<T, OnyxError>;
