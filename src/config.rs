use std::path::PathBuf;

use serde::Deserialize;

use crate::buffer::flush::{DEFAULT_PACKED_META_BATCH_LBA_LIMIT, TARGET_OPS_PER_COMMIT};
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
    #[serde(default)]
    pub threading: ThreadingConfig,
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
    /// shared across the metadata indexes (blockmap + refcount + dedup_index).
    /// Index + filter blocks are accounted against this cache
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
    /// Bloom-filter budget for metadb's dedup LSM SSTs. Higher values
    /// reduce false positives on all-miss foreground dedup lookups at the
    /// cost of more metadata pages. Large-memory NVMe deployments should
    /// prefer 16-20 bits/entry; the default stays RocksDB-like.
    #[serde(default = "default_lsm_bloom_bits_per_entry")]
    pub lsm_bloom_bits_per_entry: u32,
    /// Minimum interval between background metadb checkpoints, in
    /// milliseconds. Metadb WAL fsync makes user writes durable; full
    /// checkpoints are for WAL pruning and recovery-time control, so they
    /// should not run at buffer-ring watermark cadence.
    #[serde(default = "default_metadb_checkpoint_interval_ms")]
    pub checkpoint_interval_ms: u64,
    /// How long metadb's WAL writer waits for new sibling committers before
    /// fsyncing a partial group-commit batch. The writer still drains already
    /// queued commits first, so this should stay tiny on low-latency NVMe.
    #[serde(default = "default_metadb_group_commit_timeout_us")]
    pub group_commit_timeout_us: u64,
    /// Trigger an early metadb checkpoint when the in-memory dirty
    /// work (L2P dirty pages + RC pending deltas) exceeds this
    /// count, instead of waiting only for the periodic checkpoint
    /// interval. Caps single-flush sample size, smoothing the
    /// otherwise-bursty NVMe IO + apply_gate hold pattern.
    ///
    /// 2026-05-15 nvme-box sweep: 100k is the sweet spot on the
    /// configured profile (READ +2.7 %, p99 -16 %,
    /// buffer_volatile_payload 12.3 GB → 0.59 GB = -95 %).
    /// 500k / 2M trailed the parallel-rc-drain baseline. Set to 0
    /// to disable the early trigger and preserve the prior
    /// periodic-only behaviour.
    #[serde(default = "default_metadb_flush_dirty_pages_threshold")]
    pub flush_dirty_pages_threshold: u64,
    /// Background streaming-writeback target: when total dirty L2P
    /// pages exceeds this value the writeback worker actively seals
    /// and writes pages outside `apply_gate.write()`. Below the
    /// target the worker idles. Pairs with
    /// `flush_dirty_pages_threshold` as the target/trigger duality
    /// (writeback drains the steady backlog; the threshold remains
    /// the apply-gate-holding fallback). 0 disables the gate (worker
    /// runs whenever per-shard `min_dirty_pages` is met). Only takes
    /// effect when `l2p_writeback_enabled = true`.
    #[serde(default = "default_metadb_flush_dirty_pages_target")]
    pub flush_dirty_pages_target: u64,
    /// Cap on background-priority ops in flight at metadb's centralised
    /// IoSubmitter. Sync-priority ops (commit-path writes / fsync) always
    /// admit up to SQ capacity; background ops (L2P streaming writeback)
    /// wait in a deferred queue once `inflight_bg` reaches this cap.
    /// Keeps writeback from displacing commits from the SQ — required
    /// for `l2p_writeback_enabled=true` to not regress commit p99.
    /// 0 disables the cap.
    #[serde(default = "default_metadb_io_submitter_bg_inflight_cap")]
    pub io_submitter_bg_inflight_cap: u64,
    /// Per-flush budget on the sum of `(dirty_l2p_pages +
    /// pending_rc_deltas)` the sample phase will process. When the
    /// running total crosses this cap during shard selection,
    /// remaining shards stay unselected and their roots /
    /// `last_flushed_lsn` carry over to the next flush. Combined
    /// with the metadb-side round-robin cursor, partial sampling
    /// keeps a single flush short enough to interleave with commit
    /// apply. `manifest.checkpoint_lsn` becomes
    /// `min(per-shard last_flushed_lsn)` so WAL prune / recovery
    /// remain correct even when most flushes are partial.
    /// Set to 0 to disable partial sampling and force every flush
    /// to sample every shard.
    #[serde(default = "default_metadb_flush_select_budget")]
    pub flush_select_budget: u64,
    /// Move `page_store.deferred_free` draining off the
    /// `flush_with_gate` critical path into a background worker.
    /// 2026-05-15 nvme-box: reclaim was ~35 % of `flush_total_max`
    /// (23 s of 67 s); detaching it lets the single-outstanding
    /// dispatcher fire the next flush as soon as the manifest
    /// commits. Disable to fall back to inline reclaim (debug / A/B).
    #[serde(default = "default_metadb_async_reclaim_enabled")]
    pub async_reclaim_enabled: bool,
    /// Pages reclaimed per worker cycle. Caps single-cycle NVMe
    /// burst (zero-write + punch_hole) while amortising per-page
    /// overhead. Default 65 536 pages = 256 MiB.
    #[serde(default = "default_metadb_async_reclaim_max_pages_per_cycle")]
    pub async_reclaim_max_pages_per_cycle: u64,
    /// Worker idle parking time in milliseconds. Flush
    /// notifications cut this short via a condvar, so this only
    /// matters on quiet systems. Default 50 ms.
    #[serde(default = "default_metadb_async_reclaim_idle_interval_ms")]
    pub async_reclaim_idle_interval_ms: u64,
    /// Experimental NVMe mode: ordinary flusher metadata commits bypass the
    /// metadb WAL and rely on LV2's durable write log until the next metadb
    /// checkpoint. Leave off unless the buffer device has enough headroom to
    /// retain flushed entries until checkpoint durability advances.
    #[serde(default)]
    pub unlogged_flush_commits: bool,
    /// Optional separate WAL directory for the metadata store.
    pub wal_dir: Option<PathBuf>,
    /// Number of metadb dedup LSM shards. Must be a power of two in
    /// `[1, 64]`. Recorded in the metadb manifest at create time;
    /// changing this value on an existing database will be rejected
    /// at open with a "recreate the database" error. Default 8 matches
    /// the 2026-05 NVMe phase-4 perf result: it removes the single dedup
    /// apply-lane ceiling without the unstable N=4 balance point.
    #[serde(default = "default_metadb_dedup_shards")]
    pub dedup_shards: u32,
    /// Number of metadb L2P / refcount partition shards. Recorded in
    /// the manifest at create time. Default 16 matches metadb's own
    /// default; raise on hosts where per-shard apply-lane queue_wait
    /// has become the bottleneck (a doubled shard count halves the
    /// per-shard task arrival rate at the cost of more metadb apply
    /// threads + per-shard memory). Changing this value on an existing
    /// database is rejected at open.
    #[serde(default = "default_metadb_shards_per_partition")]
    pub shards_per_partition: u32,
    /// Number of buckets in metadb's cuckoo dedup index. Each data page
    /// holds 64 slots; choose enough buckets that the unique-hash working
    /// set stays well below the physical slot count. Recorded at create time.
    #[serde(default = "default_metadb_dedup_cuckoo_buckets")]
    pub dedup_cuckoo_buckets: u64,
    /// Entries in metadb's in-memory dedup L1 hot cache.
    #[serde(default = "default_metadb_dedup_l1_cache_entries")]
    pub dedup_l1_cache_entries: usize,
    /// Enable metadb's background refcount drainer. Default **on**
    /// (Tier 1.A, `/root/.claude/plans/ticklish-sparking-barto.md`):
    /// the drainer absorbs `RcShard.delta_active` into a sealed-page
    /// overlay outside `apply_gate.write()`, shrinking sample-phase
    /// `rc_drain` from seconds to sub-100 ms. Flip back to `false` to
    /// recover the priority-1 path for bisection.
    #[serde(default = "default_refcount_drainer_enabled")]
    pub refcount_drainer_enabled: bool,
    #[serde(default = "default_refcount_drainer_interval_ms")]
    pub refcount_drainer_interval_ms: u64,
    #[serde(default = "default_refcount_drainer_threshold_entries")]
    pub refcount_drainer_threshold_entries: usize,
    #[serde(default = "default_refcount_drainer_max_entries_per_cycle")]
    pub refcount_drainer_max_entries_per_cycle: usize,
    #[serde(default = "default_refcount_drainer_alloc_run_size")]
    pub refcount_drainer_alloc_run_size: usize,
    #[serde(default = "default_refcount_drainer_backpressure_pages")]
    pub refcount_drainer_backpressure_pages: usize,

    /// Enable metadb's L2P streaming writeback worker. When `true`, a
    /// background thread continuously seals dirty L2P pages and writes
    /// them through the centralised `IoSubmitter` outside
    /// `apply_gate.write()`. The next `Db::flush()` then samples a
    /// small dirty set so its gate-hold time stays in the
    /// low-millisecond range under sustained mixed write load.
    /// Production-on by default for Onyx; tests and bisects can flip
    /// to `false` to recover the pre-writeback checkpoint shape.
    #[serde(default = "default_l2p_writeback_enabled")]
    pub l2p_writeback_enabled: bool,

    /// Microseconds the writeback worker parks between idle cycles.
    /// Active cycles run back-to-back without sleeping; this only
    /// applies when every shard is below
    /// `l2p_writeback_min_dirty_pages`.
    #[serde(default = "default_l2p_writeback_idle_sleep_us")]
    pub l2p_writeback_idle_sleep_us: u64,

    /// Minimum per-shard dirty page count to trigger a writeback cycle.
    /// Smaller values keep dirty backlog tighter at the cost of more
    /// install-lock acquisitions.
    #[serde(default = "default_l2p_writeback_min_dirty_pages")]
    pub l2p_writeback_min_dirty_pages: usize,

    /// Hard cap on pages written by a single writeback cycle per
    /// shard. Caps install-lock hold time (commits on the same shard
    /// queue on this lock for the duration of `install_writeback`).
    #[serde(default = "default_l2p_writeback_max_pages_per_cycle")]
    pub l2p_writeback_max_pages_per_cycle: usize,

    /// Enable the B2 in-memory L2P buffer + periodic compaction path.
    /// When `false` (default), commits mutate the paged radix tree
    /// in-line. When `true`, commits insert into the per-shard buffer
    /// and a background compactor folds them into the tree. See
    /// `metadb/src/db/l2p_buffer.rs`.
    #[serde(default = "default_l2p_buffer_enabled")]
    pub l2p_buffer_enabled: bool,

    /// Per-shard soft trigger for the compactor (in entries). When
    /// `active.len()` crosses this the compactor wakes.
    #[serde(default = "default_l2p_buffer_soft_entries")]
    pub l2p_buffer_soft_entries: usize,

    /// Per-shard hard trigger. Commits to a shard block on a Condvar
    /// when its `active.len()` reaches this until the compactor swaps
    /// the active map out.
    #[serde(default = "default_l2p_buffer_hard_entries")]
    pub l2p_buffer_hard_entries: usize,

    /// Maximum wall time the compactor may wait between cycles when
    /// the entry-count triggers do not fire.
    #[serde(default = "default_l2p_buffer_max_interval_ms")]
    pub l2p_buffer_max_interval_ms: u64,

    /// Enable the ZFS-TXG-clone Phase 1 direct L2P apply fast path.
    /// When `true` and every target L2P shard runs in `use_buffer`
    /// mode (requires `l2p_buffer_enabled = true`), L2P-only commits
    /// apply on the caller thread instead of enqueuing closures onto
    /// per-shard apply-lane workers. Falls back to the lane path
    /// automatically for commits that don't match the eligibility
    /// check. See `metadb/src/config.rs::commit_direct_apply_enabled`.
    #[serde(default = "default_commit_direct_apply_enabled")]
    pub commit_direct_apply_enabled: bool,

    /// Enable the ZFS-TXG-clone Phase 2 deferred-outcome commit path
    /// on the onyx side. When `true`, the commit_worker calls metadb's
    /// `Db::commit_ops_deferred` and the returned outcomes are
    /// delivered at the next L2P compactor pass (the TXG-sync
    /// boundary) rather than synchronously on the commit thread.
    /// This config also requires the metadb-side flag
    /// `commit_deferred_outcomes_enabled = true`; with either flag
    /// off the call resolves to the existing synchronous path.
    /// Default off until the 8h `deferred_outcomes_proptest` soak
    /// gate on nvme-box passes. See
    /// `/root/.claude/plans/soft-doodling-snail.md`.
    #[serde(default = "default_commit_deferred_outcomes_enabled")]
    pub commit_deferred_outcomes_enabled: bool,
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

    pub fn checkpoint_interval(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.checkpoint_interval_ms.max(1))
    }

    pub fn group_commit_timeout_us(&self) -> u64 {
        self.group_commit_timeout_us.max(1)
    }

    pub fn lsm_bloom_bits_per_entry(&self) -> u32 {
        self.lsm_bloom_bits_per_entry.clamp(1, 32)
    }
}

impl Default for MetaConfig {
    fn default() -> Self {
        Self {
            path: None,
            block_cache_mb: default_block_cache_mb(),
            memtable_budget_mb: 0,
            index_pin_mb: default_index_pin_mb(),
            lsm_bloom_bits_per_entry: default_lsm_bloom_bits_per_entry(),
            checkpoint_interval_ms: default_metadb_checkpoint_interval_ms(),
            group_commit_timeout_us: default_metadb_group_commit_timeout_us(),
            flush_dirty_pages_threshold: default_metadb_flush_dirty_pages_threshold(),
            flush_dirty_pages_target: default_metadb_flush_dirty_pages_target(),
            io_submitter_bg_inflight_cap: default_metadb_io_submitter_bg_inflight_cap(),
            flush_select_budget: default_metadb_flush_select_budget(),
            async_reclaim_enabled: default_metadb_async_reclaim_enabled(),
            async_reclaim_max_pages_per_cycle: default_metadb_async_reclaim_max_pages_per_cycle(),
            async_reclaim_idle_interval_ms: default_metadb_async_reclaim_idle_interval_ms(),
            unlogged_flush_commits: false,
            wal_dir: None,
            dedup_shards: default_metadb_dedup_shards(),
            shards_per_partition: default_metadb_shards_per_partition(),
            dedup_cuckoo_buckets: default_metadb_dedup_cuckoo_buckets(),
            dedup_l1_cache_entries: default_metadb_dedup_l1_cache_entries(),
            refcount_drainer_enabled: default_refcount_drainer_enabled(),
            refcount_drainer_interval_ms: default_refcount_drainer_interval_ms(),
            refcount_drainer_threshold_entries: default_refcount_drainer_threshold_entries(),
            refcount_drainer_max_entries_per_cycle: default_refcount_drainer_max_entries_per_cycle(
            ),
            refcount_drainer_alloc_run_size: default_refcount_drainer_alloc_run_size(),
            refcount_drainer_backpressure_pages: default_refcount_drainer_backpressure_pages(),
            l2p_writeback_enabled: default_l2p_writeback_enabled(),
            l2p_writeback_idle_sleep_us: default_l2p_writeback_idle_sleep_us(),
            l2p_writeback_min_dirty_pages: default_l2p_writeback_min_dirty_pages(),
            l2p_writeback_max_pages_per_cycle: default_l2p_writeback_max_pages_per_cycle(),
            l2p_buffer_enabled: default_l2p_buffer_enabled(),
            l2p_buffer_soft_entries: default_l2p_buffer_soft_entries(),
            l2p_buffer_hard_entries: default_l2p_buffer_hard_entries(),
            l2p_buffer_max_interval_ms: default_l2p_buffer_max_interval_ms(),
            commit_direct_apply_enabled: default_commit_direct_apply_enabled(),
            commit_deferred_outcomes_enabled: default_commit_deferred_outcomes_enabled(),
        }
    }
}

fn default_metadb_dedup_shards() -> u32 {
    8
}

fn default_metadb_shards_per_partition() -> u32 {
    16
}

fn default_metadb_dedup_cuckoo_buckets() -> u64 {
    1_000_000
}

fn default_metadb_dedup_l1_cache_entries() -> usize {
    256_000
}
fn default_refcount_drainer_enabled() -> bool {
    true
}
fn default_refcount_drainer_interval_ms() -> u64 {
    50
}
fn default_refcount_drainer_threshold_entries() -> usize {
    4_096
}
fn default_refcount_drainer_max_entries_per_cycle() -> usize {
    65_536
}
fn default_refcount_drainer_alloc_run_size() -> usize {
    64
}
fn default_refcount_drainer_backpressure_pages() -> usize {
    8_192
}
fn default_l2p_writeback_enabled() -> bool {
    // Default off. The streaming writeback worker correctly shrinks
    // the checkpoint sample dirty set (validated: 558k → 1.3k pages,
    // gate-hold 728 → 45 ms), but its writes share the metadb page
    // store's `IoSubmitter` queue (SQ=1024) with commit-apply's own
    // RC-delta and dirty-page writes. Under sustained writeback the
    // submitter saturates, raising commit-apply latency and net
    // costing more foreground IOPS than the sample-hold reduction
    // saves on this `j8 d4` mixed workload. Once the submitter has a
    // dedicated writeback channel (or adaptive backpressure that
    // yields to commits), flip back to `true`.
    false
}
fn default_l2p_writeback_idle_sleep_us() -> u64 {
    500
}
fn default_l2p_writeback_min_dirty_pages() -> usize {
    64
}
fn default_l2p_writeback_max_pages_per_cycle() -> usize {
    512
}
fn default_l2p_buffer_enabled() -> bool {
    // Phase 1 lands infrastructure only. Phase 3 flips behaviour
    // (commit writes buffer instead of tree). Phase 5 may flip
    // default to true after nvme-box A/B validation. See
    // /root/.claude/plans/ticklish-sparking-barto.md.
    false
}
fn default_l2p_buffer_soft_entries() -> usize {
    64_000
}
fn default_l2p_buffer_hard_entries() -> usize {
    512_000
}
fn default_l2p_buffer_max_interval_ms() -> u64 {
    30_000
}
fn default_commit_direct_apply_enabled() -> bool {
    // metadb defaults to true; safe fallback to lane path when the
    // commit isn't eligible (touches dedup, guarded remap, lifecycle).
    // Engages only when `l2p_buffer_enabled` is also set, since every
    // target shard must have `use_buffer = true`.
    true
}
fn default_commit_deferred_outcomes_enabled() -> bool {
    // ZFS-TXG-clone Phase 2 — default off during first-month soak.
    // Flip on after the 8h deferred_outcomes_proptest passes on
    // nvme-box. The metadb-side flag also defaults off; both must be
    // true for the deferred path to engage.
    false
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
    /// Maximum pre-sync payload bytes. 0 = derive from max_memory_mb.
    #[serde(default)]
    pub volatile_memory_mb: usize,
    /// Per-shard staging queue length between appenders and sync thread.
    /// 0 = engine default.
    #[serde(default)]
    pub staging_queue_entries: usize,
    /// Max entries one sync thread drains into a single fdatasync epoch.
    /// 0 = engine default.
    #[serde(default)]
    pub sync_batch_max_entries: usize,
    /// Max payload bytes one sync thread drains into a single fdatasync epoch.
    /// 0 = engine default.
    #[serde(default)]
    pub sync_batch_max_bytes_mb: usize,
    /// Tier 1.B (ZFS-inspired) hyperbolic write throttle: LV2 fill percentage
    /// at which the throttle starts paying per-append delay. 0 disables the
    /// throttle entirely (the existing condvar-based hard backpressure at
    /// 100% fill still applies).
    #[serde(default)]
    pub throttle_min_pct: u8,
    /// LV2 fill percentage at which the hyperbolic divisor approaches zero
    /// and per-append delay saturates to `throttle_cap_us`. 0 = use 100.
    #[serde(default)]
    pub throttle_max_pct: u8,
    /// Throttle curve scale (microseconds). 0 disables the throttle.
    /// `delay_us = scale * (fill - min_pct) / (max_pct - fill)`.
    #[serde(default)]
    pub throttle_scale_us: u64,
    /// Per-append throttle delay cap (microseconds). 0 = 100_000 = 100 ms.
    #[serde(default)]
    pub throttle_cap_us: u64,
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
            volatile_memory_mb: 0,
            staging_queue_entries: 0,
            sync_batch_max_entries: 0,
            sync_batch_max_bytes_mb: 0,
            throttle_min_pct: 0,
            throttle_max_pct: 0,
            throttle_scale_us: 0,
            throttle_cap_us: 0,
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
    /// Worker threads per ublk queue used to offload backend IO before
    /// completing commands on the queue thread.
    #[serde(default = "default_queue_workers")]
    pub queue_workers: usize,
}

impl Default for UblkConfig {
    fn default() -> Self {
        Self {
            nr_queues: default_nr_queues(),
            queue_depth: default_queue_depth(),
            io_buf_bytes: default_io_buf_bytes(),
            queue_workers: default_queue_workers(),
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
    /// Minimum space saving required to keep a compressed unit after the
    /// compression attempt. If compression saves less than this percentage,
    /// the flusher stores the unit raw to avoid read-time decompression and
    /// reduce compressed-byte read amplification.
    #[serde(default = "default_min_compression_savings_pct")]
    pub min_compression_savings_pct: u8,
    /// Skip coalesce/compress/dedup work for pending entries whose LBAs have
    /// all been superseded by a later seq still in the ring. Entries are
    /// mark_flushed immediately so the ring tail can advance without paying
    /// SHA-256 + compress + dedup_index insert/delete for soon-to-be-dead
    /// data. Default `true`; set `false` to regression-test the full path.
    #[serde(default = "default_skip_fully_superseded")]
    pub skip_fully_superseded: bool,
    /// Maximum mapped LBAs folded into one packed-slot metadata commit.
    /// Lower values reduce metadb apply head-of-line tail under heavy mixed
    /// read/write load; higher values amortise WAL/apply overhead and improve
    /// drain throughput. Set 0 to use the built-in default.
    #[serde(default = "default_packed_meta_batch_max_lbas")]
    pub packed_meta_batch_max_lbas: usize,
    /// Number of commit workers a single volume may fan out to.
    /// Default 8 — with the L2P stripe Mutex removed in B-语义路
    /// Batch D, same-LBA serialization is delegated to metadb's
    /// seq_guard CAS, so one volume can use up to 8 consecutive
    /// commit workers in parallel without pinning each other on
    /// the onyx-side lock. Selection is `shard_idx % per_vol`.
    /// Total commit workers stays at NUM_COMMIT_WORKERS=16; only
    /// the per-vol fanout changes.
    #[serde(default = "default_commit_workers_per_volume")]
    pub commit_workers_per_volume: usize,
    /// Maximum live LBAs placed in one passthrough metadb transaction.
    /// This replaces the built-in target in the commit worker so NVMe
    /// experiments can sweep transaction shape without rebuilding.
    #[serde(default = "default_commit_target_lbas_per_tx")]
    pub commit_target_lbas_per_tx: usize,
    /// Soft LBA budget a commit worker drains from its bounded queue
    /// before dispatching. Adjacent same-volume passthrough jobs are
    /// merged, then split into transactions by `commit_target_lbas_per_tx`.
    /// Set 0 to disable cross-job coalescing.
    #[serde(default = "default_commit_coalesce_lba_budget")]
    pub commit_coalesce_lba_budget: usize,
    /// Optional wait window after the first queued commit job arrives.
    /// This lets nearby shard-writer jobs join the same coalesced batch
    /// without making the queue unbounded. Set 0 for try_recv-only drain.
    #[serde(default = "default_commit_coalesce_timeout_us")]
    pub commit_coalesce_timeout_us: u64,
    /// No-wait LBA budget for batching consecutive packed-slot metadata jobs.
    /// Set 0 to keep one packed slot per metadb commit. Higher values reduce
    /// commit count but can evict hot write-buffer entries sooner, hurting
    /// mixed read/write foreground IOPS.
    #[serde(default = "default_packed_commit_try_drain_lba_budget")]
    pub packed_commit_try_drain_lba_budget: usize,
    /// Maximum units a writer lane drains per cycle while foreground reads
    /// are active. Lower values protect read tail latency; higher values
    /// improve backend drain when the write queue is saturated.
    #[serde(default = "default_writer_read_active_batch_size")]
    pub writer_read_active_batch_size: usize,
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
            min_compression_savings_pct: default_min_compression_savings_pct(),
            skip_fully_superseded: default_skip_fully_superseded(),
            packed_meta_batch_max_lbas: default_packed_meta_batch_max_lbas(),
            commit_workers_per_volume: default_commit_workers_per_volume(),
            commit_target_lbas_per_tx: default_commit_target_lbas_per_tx(),
            commit_coalesce_lba_budget: default_commit_coalesce_lba_budget(),
            commit_coalesce_timeout_us: default_commit_coalesce_timeout_us(),
            packed_commit_try_drain_lba_budget: default_packed_commit_try_drain_lba_budget(),
            writer_read_active_batch_size: default_writer_read_active_batch_size(),
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
fn default_min_compression_savings_pct() -> u8 {
    12
}
fn default_skip_fully_superseded() -> bool {
    true
}
fn default_packed_meta_batch_max_lbas() -> usize {
    DEFAULT_PACKED_META_BATCH_LBA_LIMIT
}
fn default_commit_workers_per_volume() -> usize {
    8
}
fn default_commit_target_lbas_per_tx() -> usize {
    TARGET_OPS_PER_COMMIT
}
fn default_commit_coalesce_lba_budget() -> usize {
    0
}
fn default_commit_coalesce_timeout_us() -> u64 {
    0
}
fn default_packed_commit_try_drain_lba_budget() -> usize {
    0
}
fn default_writer_read_active_batch_size() -> usize {
    crate::buffer::flush::BufferFlusher::WRITER_BATCH_SIZE_READ_ACTIVE
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
fn default_lsm_bloom_bits_per_entry() -> u32 {
    10
}
fn default_metadb_checkpoint_interval_ms() -> u64 {
    5_000
}
fn default_metadb_group_commit_timeout_us() -> u64 {
    1
}
fn default_metadb_flush_dirty_pages_threshold() -> u64 {
    // 2026-05-15 nvme-box sweep: 100k caps single-flush sample size
    // so dirty work is drained continuously rather than in
    // multi-second bursts. buffer_volatile_payload dropped 12.3 GB →
    // 0.59 GB at this threshold. Set to 0 to disable.
    100_000
}
fn default_metadb_flush_dirty_pages_target() -> u64 {
    // Default 0 = streaming writeback target gate disabled, preserves
    // status quo (writeback worker, when enabled, runs whenever
    // per-shard `l2p_writeback_min_dirty_pages` is met). Operators
    // pairing this with `l2p_writeback_enabled = true` typically set
    // target ≈ threshold / 3 (e.g. 100_000 with threshold 300_000)
    // so the writeback worker drains the steady backlog before the
    // apply-gate-holding checkpoint trigger fires.
    0
}
fn default_metadb_io_submitter_bg_inflight_cap() -> u64 {
    // 1024 ≈ 6 % of SQ_ENTRIES=16384 in metadb's IoSubmitter. Below
    // this the bg admission gate is invisible (sync ops always have
    // headroom); above it bg ops park in the submitter's deferred
    // queue. 0 disables the cap.
    1024
}
fn default_metadb_async_reclaim_enabled() -> bool {
    // OFF after 2026-05-15 nvme-box A/B. v1 tight-loop = correct
    // but READ IOPS -26 % (NVMe contention); v2 one-cycle-per-notify =
    // 72 967 refcount underflow errors. Infra retained but disabled
    // until underflow root cause is identified. See memory
    // `async_reclaim_default_off`.
    false
}
fn default_metadb_async_reclaim_max_pages_per_cycle() -> u64 {
    65_536
}
fn default_metadb_async_reclaim_idle_interval_ms() -> u64 {
    50
}
fn default_metadb_flush_select_budget() -> u64 {
    // Default OFF — every flush samples every shard.
    //
    // 2026-05-15 nvme-box A/B with budget=100_000 (matching the
    // threshold trigger): partial sample correctly capped sample /
    // IO / reclaim max (sample_max 13.1 s → 0.6 s; meta_io
    // batch_bytes 4.9 GB → 390 MB), but backend throughput stayed
    // single-outstanding-dispatcher-bound at ~1.5 flushes/s and the
    // unlogged-commit buffer reclaim path got pinned by
    // `min(per-shard last_flushed_lsn)` not advancing (cold shards
    // never get re-flushed, so `manifest.checkpoint_lsn` lags).
    // Result: READ IOPS -8.5 %, buffer_volatile 0.6 GB → 32 GB.
    // Disabling `unlogged_flush_commits` didn't help — WAL fsync
    // became the new bottleneck (buffer 42 GB, IOPS -14 %).
    //
    // The infrastructure is sound (495 tests pass, 0 CRC /
    // corruption, recovery via per-shard idempotent apply) and the
    // knob remains exposed so configurations that DO benefit from
    // partial sampling (e.g. workloads where most shards are cold,
    // or where the embedder doesn't gate buffer reclaim on
    // checkpoint LSN) can enable it. Set to a positive value to
    // activate.
    0
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

/// Optional CPU affinity layout. All fields accept Linux CPU-list syntax such
/// as `"0-7,16-23"`. Empty fields leave that role unbound.
#[derive(Debug, Clone, Deserialize)]
pub struct ThreadingConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub ublk_cpus: String,
    #[serde(default)]
    pub read_pool_cpus: String,
    #[serde(default)]
    pub buffer_sync_cpus: String,
    #[serde(default)]
    pub flusher_coalesce_cpus: String,
    #[serde(default)]
    pub flusher_dedup_cpus: String,
    #[serde(default)]
    pub flusher_compress_cpus: String,
    #[serde(default)]
    pub flusher_writer_cpus: String,
    #[serde(default)]
    pub flusher_cleanup_cpus: String,
    #[serde(default)]
    pub metadb_wal_cpus: String,
    #[serde(default)]
    pub metadb_l2p_apply_cpus: String,
    #[serde(default)]
    pub metadb_refcount_apply_cpus: String,
    #[serde(default)]
    pub metadb_dedup_apply_cpus: String,
    #[serde(default)]
    pub metadb_checkpoint_cpus: String,
    /// CPU set for the metadb refcount drainer threads (one per
    /// refcount shard). The drainer absorbs `RcShard.delta_active`
    /// into a sealed-page overlay outside `apply_gate.write()`; pin
    /// it to a small same-NUMA CPU set so its working set stays in
    /// L2/L3 next to the apply lanes. Leave empty to inherit the OS
    /// default — correctness unaffected, but the drainer can be
    /// starved on a busy box and the in-gate fallback rises.
    #[serde(default)]
    pub metadb_refcount_drainer_cpus: String,
    /// CPU set for the metadb L2P compactor (single serial thread).
    /// Pin to 1–2 CPUs on the same NUMA node as `metadb_l2p_apply`
    /// to keep its working set local; without pinning the kernel
    /// scheduler can co-locate the compactor on an apply-lane CPU
    /// during a flush window and push apply-lane exec tails up.
    /// Leave empty to inherit the OS default.
    #[serde(default)]
    pub metadb_l2p_compactor_cpus: String,
    /// CPU set for the metadb io_uring submitter threads. With
    /// `io_submitter_pool_size > 1`, each submitter pins to
    /// `cpus[ordinal % len]` so the kernel mq-block layer routes
    /// each submitter's IO to a distinct NVMe hardware queue. Leave
    /// empty to inherit the OS default (only safe when pool=1).
    #[serde(default)]
    pub metadb_io_submitter_cpus: String,
    /// CPU set for the onyx per-volume commit workers (one channel
    /// per `hash(vol_id) % NUM_COMMIT_WORKERS`). Each commit_worker
    /// calls `tx.commit_with_outcomes` so it benefits from sharing
    /// a NUMA node with `metadb_l2p_apply` / `metadb_refcount_apply`
    /// — the previous default of borrowing `flusher_writer`'s CPUs
    /// crossed sockets on the v4 layout and added ~0.5–1 ms /
    /// commit of cache-line bounce. Leave empty to inherit the
    /// previous behaviour (commit workers fall back to
    /// `flusher_writer_cpus`).
    #[serde(default)]
    pub commit_worker_cpus: String,
    #[serde(default)]
    pub background_cpus: String,
}

impl Default for ThreadingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ublk_cpus: String::new(),
            read_pool_cpus: String::new(),
            buffer_sync_cpus: String::new(),
            flusher_coalesce_cpus: String::new(),
            flusher_dedup_cpus: String::new(),
            flusher_compress_cpus: String::new(),
            flusher_writer_cpus: String::new(),
            flusher_cleanup_cpus: String::new(),
            metadb_wal_cpus: String::new(),
            metadb_l2p_apply_cpus: String::new(),
            metadb_refcount_apply_cpus: String::new(),
            metadb_dedup_apply_cpus: String::new(),
            metadb_checkpoint_cpus: String::new(),
            metadb_refcount_drainer_cpus: String::new(),
            metadb_l2p_compactor_cpus: String::new(),
            metadb_io_submitter_cpus: String::new(),
            commit_worker_cpus: String::new(),
            background_cpus: String::new(),
        }
    }
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
fn default_queue_workers() -> usize {
    1
}

// OnyxConfig::load and should_standby are defined in the impl block above.
