use std::path::PathBuf;

use serde::Deserialize;

use crate::error::{OnyxError, OnyxResult};
use crate::types::CompressionAlgo;

#[derive(Debug, Clone, Deserialize)]
pub struct OnyxConfig {
    pub meta: MetaConfig,
    pub storage: StorageConfig,
    pub buffer: BufferConfig,
    #[serde(default)]
    pub ublk: UblkConfig,
    #[serde(default)]
    pub flush: FlushConfig,
    #[serde(default)]
    pub engine: EngineConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetaConfig {
    /// Path to RocksDB data directory (on LV1 / XFS)
    pub rocksdb_path: PathBuf,
    /// Block cache size in MB (default 256)
    #[serde(default = "default_block_cache_mb")]
    pub block_cache_mb: usize,
    /// Optional separate WAL directory
    pub wal_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    /// Path to data device (LV3)
    pub data_device: PathBuf,
    /// Block size in bytes (default 4096)
    #[serde(default = "default_block_size")]
    pub block_size: u32,
    /// Enable hugepage memory allocation
    #[serde(default)]
    pub use_hugepages: bool,
    /// Default compression algorithm for new volumes
    #[serde(default = "default_compression")]
    pub default_compression: CompressionAlgo,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BufferConfig {
    /// Path to write buffer device (LV2)
    pub device: PathBuf,
    /// Buffer capacity in MB (default 16384 = 16GB)
    #[serde(default = "default_buffer_capacity_mb")]
    pub capacity_mb: usize,
    /// Flush watermark percentage (default 80)
    #[serde(default = "default_flush_watermark_pct")]
    pub flush_watermark_pct: u8,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UblkConfig {
    /// Number of IO queues (default 4)
    #[serde(default = "default_nr_queues")]
    pub nr_queues: u16,
    /// Queue depth (default 128)
    #[serde(default = "default_queue_depth")]
    pub queue_depth: u16,
    /// IO buffer size in bytes (default 1MB)
    #[serde(default = "default_io_buf_bytes")]
    pub io_buf_bytes: u32,
}

impl Default for UblkConfig {
    fn default() -> Self {
        Self {
            nr_queues: default_nr_queues(),
            queue_depth: default_queue_depth(),
            io_buf_bytes: default_io_buf_bytes(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct FlushConfig {
    /// Number of parallel compression worker threads (default 2)
    #[serde(default = "default_compress_workers")]
    pub compress_workers: usize,
    /// Max raw bytes to coalesce before compressing (default 128KB)
    #[serde(default = "default_coalesce_max_raw_bytes")]
    pub coalesce_max_raw_bytes: usize,
    /// Max number of LBAs to coalesce into one compression unit (default 32)
    #[serde(default = "default_coalesce_max_lbas")]
    pub coalesce_max_lbas: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EngineConfig {
    /// Number of zones (default 4)
    #[serde(default = "default_zone_count")]
    pub zone_count: u32,
    /// Blocks per zone (default 256)
    #[serde(default = "default_zone_size_blocks")]
    pub zone_size_blocks: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            zone_count: default_zone_count(),
            zone_size_blocks: default_zone_size_blocks(),
        }
    }
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self {
            compress_workers: default_compress_workers(),
            coalesce_max_raw_bytes: default_coalesce_max_raw_bytes(),
            coalesce_max_lbas: default_coalesce_max_lbas(),
        }
    }
}

fn default_compress_workers() -> usize {
    2
}
fn default_coalesce_max_raw_bytes() -> usize {
    131072 // 128KB
}
fn default_coalesce_max_lbas() -> u32 {
    32
}
fn default_zone_count() -> u32 {
    4
}
fn default_zone_size_blocks() -> u64 {
    256
}
fn default_block_cache_mb() -> usize {
    256
}
fn default_block_size() -> u32 {
    4096
}
fn default_compression() -> CompressionAlgo {
    CompressionAlgo::Lz4
}
fn default_buffer_capacity_mb() -> usize {
    16384
}
fn default_flush_watermark_pct() -> u8 {
    80
}
fn default_nr_queues() -> u16 {
    4
}
fn default_queue_depth() -> u16 {
    128
}
fn default_io_buf_bytes() -> u32 {
    1024 * 1024
}

impl OnyxConfig {
    pub fn load(path: &std::path::Path) -> OnyxResult<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            OnyxError::Config(format!("failed to read config file {:?}: {}", path, e))
        })?;
        toml::from_str(&content)
            .map_err(|e| OnyxError::Config(format!("failed to parse config: {}", e)))
    }
}
