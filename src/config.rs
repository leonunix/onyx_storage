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
    /// Metadata store available but storage devices missing — metadata-only operations.
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
        let meta_ok = self
            .meta
            .path()
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
    /// Path to metadata directory (on LV1 / XFS). Holds blockmap, refcount,
    /// dedup index, volume metadata. None = bare mode (no metadata store).
    #[serde(default)]
    pub path: Option<PathBuf>,
    /// Shared block cache size in MB. One LRU cache is created at startup and
    /// shared across every CF (blockmap + refcount + dedup_index +
    /// dedup_reverse). Index + filter blocks are accounted against this cache
    /// (`cache_index_and_filter_blocks=true`), so this is the authoritative
    /// upper bound on metadb read-side memory. Scale roughly proportional to
    /// working set; on a 256 GiB host, 16–32 GiB is a reasonable starting
    /// point.
    #[serde(default = "default_block_cache_mb")]
    pub block_cache_mb: usize,
    /// Total memtable memory budget in MB across all CFs. Enforced via
    /// metadb's `WriteBufferManager`; when this is exceeded, writes stall
    /// (`allow_stall=true`) rather than blow up RSS. Default 0 = auto = half
    /// of `block_cache_mb`. Override when you want a different write-buffer
    /// vs read-cache ratio (e.g. write-heavy: raise; read-heavy: lower).
    #[serde(default)]
    pub memtable_budget_mb: usize,
    /// Per-`Db` upper bound on bytes used to pin L2P index pages in metadb's
    /// page cache. Pinned pages live outside the LRU and never compete with
    /// leaf capacity, so random L2P gets never miss on inner nodes. Index
    /// pages are ~1/256 of leaf bytes, so 1 GiB covers on the order of
    /// hundreds of GiB of leaf data. Default 1024 = 1 GiB; raise on
    /// large-memory deployments. Set to 0 to disable.
    #[serde(default = "default_index_pin_mb")]
    pub index_pin_mb: usize,
    /// Optional separate WAL directory for the metadata store.
    pub wal_dir: Option<PathBuf>,
}

impl MetaConfig {
    pub fn path(&self) -> Option<&PathBuf> {
        self.path.as_ref()
    }

    /// Resolved memtable budget in bytes. `memtable_budget_mb = 0` → half of
    /// `block_cache_mb`, clamped to at least 64 MiB so a tiny config doesn't
    /// starve memtables entirely.
    pub fn memtable_budget_bytes(&self) -> usize {
        let explicit = self.memtable_budget_mb.saturating_mul(1024 * 1024);
        if explicit > 0 {
            return explicit.max(64 * 1024 * 1024);
        }
        let cache_bytes = self.block_cache_mb.saturating_mul(1024 * 1024);
        (cache_bytes / 2).max(64 * 1024 * 1024)
    }

    /// Resolved block cache capacity in bytes, with a minimum floor so an
    /// empty/zero config still yields a usable cache.
    pub fn block_cache_bytes(&self) -> usize {
        self.block_cache_mb
            .saturating_mul(1024 * 1024)
            .max(8 * 1024 * 1024)
    }

    /// Resolved index-pin budget in bytes. `index_pin_mb = 0` disables
    /// pinning and lets index pages compete with leaves for LRU space.
    pub fn index_pin_bytes(&self) -> usize {
        self.index_pin_mb.saturating_mul(1024 * 1024)
    }
}

impl Default for MetaConfig {
    fn default() -> Self {
        Self {
            path: None,
            block_cache_mb: default_block_cache_mb(),
            memtable_budget_mb: 0,
            index_pin_mb: default_index_pin_mb(),
            wal_dir: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IoBackend {
    /// Classic pread/pwrite + fsync via libc.
    Syscall,
    /// io_uring batched submission (Linux only). Default backend.
    Uring,
}

impl Default for IoBackend {
    fn default() -> Self {
        // Set `[storage] io_backend = "syscall"` to fall back to pread/pwrite.
        IoBackend::Uring
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
    /// IO backend for LV3 + buffer commit log + heartbeat (default syscall).
    #[serde(default)]
    pub io_backend: IoBackend,
    /// io_uring submission-queue depth per ring (default 128).
    #[serde(default = "default_uring_sq_entries")]
    pub uring_sq_entries: u32,
    /// Number of LV3 read-pool worker threads (default 4). Each owns its own
    /// io_uring ring; reads are sharded across workers by hash(PBA). 0 = disable
    /// the pool and execute reads inline on the caller thread.
    #[serde(default = "default_read_pool_workers")]
    pub read_pool_workers: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_device: None,
            block_size: default_block_size(),
            use_hugepages: false,
            default_compression: default_compression(),
            io_backend: IoBackend::default(),
            uring_sq_entries: default_uring_sq_entries(),
            read_pool_workers: default_read_pool_workers(),
        }
    }
}

fn default_uring_sq_entries() -> u32 {
    128
}

fn default_read_pool_workers() -> usize {
    4
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
    /// Maximum in-memory payload bytes before append backpressure kicks in.
    /// Default 0 = auto (50% of system memory, capped at 8 GiB).
    #[serde(default)]
    pub max_memory_mb: usize,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            device: None,
            capacity_mb: default_buffer_capacity_mb(),
            flush_watermark_pct: default_flush_watermark_pct(),
            group_commit_wait_us: default_group_commit_wait_us(),
            shards: default_buffer_shards(),
            max_memory_mb: 0,
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
    /// Skip coalesce/compress/dedup work for pending entries whose LBAs have
    /// all been superseded by a later seq still in the ring. Entries are
    /// mark_flushed immediately so the ring tail can advance without paying
    /// SHA-256 + compress + dedup_index insert/delete for soon-to-be-dead
    /// data. Default `true`; set `false` to regression-test the full path.
    #[serde(default = "default_skip_fully_superseded")]
    pub skip_fully_superseded: bool,
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
            skip_fully_superseded: default_skip_fully_superseded(),
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
fn default_skip_fully_superseded() -> bool {
    true
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
fn default_index_pin_mb() -> usize {
    1024
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
