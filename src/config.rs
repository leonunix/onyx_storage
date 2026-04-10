use std::path::PathBuf;

use serde::Deserialize;

use crate::dedup::config::DedupConfig;
use crate::error::{OnyxError, OnyxResult};
use crate::gc::config::GcConfig;
use crate::types::CompressionAlgo;

#[derive(Debug, Clone, Deserialize)]
pub struct OnyxConfig {
    #[serde(default)]
    pub meta: MetaConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub buffer: BufferConfig,
    #[serde(default)]
    pub ublk: UblkConfig,
    #[serde(default)]
    pub flush: FlushConfig,
    #[serde(default)]
    pub engine: EngineConfig,
    #[serde(default)]
    pub gc: GcConfig,
    #[serde(default)]
    pub dedup: DedupConfig,
    #[serde(default)]
    pub service: ServiceConfig,
    #[serde(default)]
    pub ha: HaConfig,
}

/// What the engine can do given the current configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfiguredMode {
    /// Nothing configured — only IPC socket, no engine at all.
    Bare,
    /// RocksDB available but storage devices missing — metadata-only operations.
    Standby,
    /// Everything configured — full IO.
    Active,
}

impl std::fmt::Display for ConfiguredMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfiguredMode::Bare => write!(f, "bare"),
            ConfiguredMode::Standby => write!(f, "standby"),
            ConfiguredMode::Active => write!(f, "active"),
        }
    }
}

impl OnyxConfig {
    /// Detect what mode the engine should operate in, based on which
    /// paths are configured and actually exist on disk.
    pub fn detect_mode(&self) -> ConfiguredMode {
        // Check RocksDB path
        let meta_ok = self
            .meta
            .rocksdb_path
            .as_ref()
            .map(|p| !p.as_os_str().is_empty())
            .unwrap_or(false);
        if !meta_ok {
            return ConfiguredMode::Bare;
        }

        // Check data + buffer devices
        let data_ok = self
            .storage
            .data_device
            .as_ref()
            .map(|p| !p.as_os_str().is_empty() && p.exists())
            .unwrap_or(false);
        let buffer_ok = self
            .buffer
            .device
            .as_ref()
            .map(|p| !p.as_os_str().is_empty() && p.exists())
            .unwrap_or(false);

        if data_ok && buffer_ok {
            ConfiguredMode::Active
        } else {
            ConfiguredMode::Standby
        }
    }

    pub fn load(path: &std::path::Path) -> OnyxResult<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            OnyxError::Config(format!("failed to read config file {:?}: {}", path, e))
        })?;
        toml::from_str(&content)
            .map_err(|e| OnyxError::Config(format!("failed to parse config: {}", e)))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetaConfig {
    /// Path to RocksDB data directory (on LV1 / XFS).
    /// None = bare mode (no metadata store, only IPC).
    #[serde(default)]
    pub rocksdb_path: Option<PathBuf>,
    /// Block cache size in MB (default 256)
    #[serde(default = "default_block_cache_mb")]
    pub block_cache_mb: usize,
    /// Optional separate WAL directory
    pub wal_dir: Option<PathBuf>,
}

impl Default for MetaConfig {
    fn default() -> Self {
        Self {
            rocksdb_path: None,
            block_cache_mb: default_block_cache_mb(),
            wal_dir: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    /// Path to data device (LV3). None = standby mode (no IO).
    #[serde(default)]
    pub data_device: Option<PathBuf>,
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

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_device: None,
            block_size: default_block_size(),
            use_hugepages: false,
            default_compression: default_compression(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BufferConfig {
    /// Path to write buffer device (LV2). None = standby mode (no IO).
    #[serde(default)]
    pub device: Option<PathBuf>,
    /// Buffer capacity in MB (default 16384 = 16GB)
    #[serde(default = "default_buffer_capacity_mb")]
    pub capacity_mb: usize,
    /// Flush watermark percentage (default 80)
    #[serde(default = "default_flush_watermark_pct")]
    pub flush_watermark_pct: u8,
    /// Max time to wait for a batched durable sync before forcing a commit (default 250us)
    #[serde(default = "default_group_commit_wait_us")]
    pub group_commit_wait_us: u64,
    /// Number of internal journal shards inside the buffer device (default 1)
    #[serde(default = "default_buffer_shards")]
    pub shards: usize,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            device: None,
            capacity_mb: default_buffer_capacity_mb(),
            flush_watermark_pct: default_flush_watermark_pct(),
            group_commit_wait_us: default_group_commit_wait_us(),
            shards: default_buffer_shards(),
        }
    }
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
    /// Number of compression worker threads **per flush lane** (default 2).
    /// Each buffer shard has its own flush lane. Total compress threads = shards × compress_workers.
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
    2 // per flush lane (1 lane per buffer shard)
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
fn default_group_commit_wait_us() -> u64 {
    500 // 500µs batching window — good balance for group commit
}
fn default_buffer_shards() -> usize {
    4
}
#[derive(Debug, Clone, Deserialize)]
pub struct ServiceConfig {
    /// Unix socket path for IPC (stop command, status queries)
    #[serde(default = "default_socket_path")]
    pub socket_path: PathBuf,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            socket_path: default_socket_path(),
        }
    }
}

fn default_socket_path() -> PathBuf {
    PathBuf::from("/var/run/onyx-storage.sock")
}

#[derive(Debug, Clone, Deserialize)]
pub struct HaConfig {
    /// Whether HA heartbeat is enabled (default false).
    #[serde(default)]
    pub enabled: bool,
    /// Node identifier for this engine instance (default 0).
    #[serde(default)]
    pub node_id: u64,
    /// Heartbeat write interval in milliseconds (default 3000).
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
    /// Lease duration in seconds for HA lock (default 30).
    #[serde(default = "default_lease_duration_secs")]
    pub lease_duration_secs: u64,
}

impl Default for HaConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: 0,
            heartbeat_interval_ms: default_heartbeat_interval_ms(),
            lease_duration_secs: default_lease_duration_secs(),
        }
    }
}

fn default_heartbeat_interval_ms() -> u64 {
    3000
}
fn default_lease_duration_secs() -> u64 {
    30
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

// OnyxConfig::load and should_standby are defined in the impl block above.
