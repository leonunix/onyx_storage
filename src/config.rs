use std::path::PathBuf;

use serde::Deserialize;

use crate::buffer::flush::{DEFAULT_PACKED_META_BATCH_LBA_LIMIT, TARGET_OPS_PER_COMMIT};
use crate::dedup::config::DedupConfig;
use crate::error::{OnyxError, OnyxResult};
use crate::gc::config::GcConfig;
use crate::types::CompressionAlgo;

#[derive(Debug, Clone, Default, Deserialize)]
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
    #[serde(default)]
    pub numa: NumaConfig,
    #[serde(default)]
    pub chunklet: ChunkletConfig,
}

/// NUMA awareness mode (docs/numa-aware-design.md §3.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NumaMode {
    /// Legacy behaviour: `[threading]` governs affinity, memory placement is
    /// the OS default (use external numactl if desired).
    #[default]
    Off,
    /// Single-node confinement implemented in-engine: every role binds to
    /// `home_node` (minus reserved cores, ublk queue threads included) and
    /// the process memory policy prefers `home_node`, with Tier B caches
    /// allowed to spill per `cold_cache_policy`. Replaces the
    /// "numactl --cpunodebind --membind + threading.enabled=false" recipe.
    Confine,
    /// Dual-socket pod partition mode. Parsing is accepted so
    /// configs can be staged, but startup refuses until implemented.
    Partition,
}

/// Tier B (capacity cache) placement policy under `mode = "confine"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ColdCachePolicy {
    /// Budget-driven: keep Tier B on the home node when the memory plan
    /// fits, otherwise let it spill across nodes.
    #[default]
    Auto,
    Home,
    Interleave,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NumaConfig {
    #[serde(default)]
    pub mode: NumaMode,
    /// Node hosting the control-plane singletons; the confinement target in
    /// `confine` mode.
    #[serde(default)]
    pub home_node: usize,
    /// Nodes participating in `partition` mode. Empty = all detected nodes.
    #[serde(default)]
    pub data_nodes: Vec<usize>,
    /// Physical cores (both HT siblings) left out of every engine CPU set,
    /// per node, for the OS / IRQs / interactive shells. The highest-numbered
    /// cores are reserved; capped so at least one core remains.
    #[serde(default = "default_numa_reserve_cores")]
    pub reserve_cores_per_node: usize,
    /// Physical cores carved out of the confine engine set for foreground
    /// ublk and LV2 sync threads. Zero preserves the shared-set behaviour.
    #[serde(default)]
    pub foreground_cores_per_node: usize,
    #[serde(default)]
    pub cold_cache_policy: ColdCachePolicy,
    /// Permit startup even when the memory plan does not fit the home node
    /// (the kernel will pick what spills — see design §3.5 for why that is
    /// normally refused).
    #[serde(default)]
    pub allow_overcommit: bool,
}

fn default_numa_reserve_cores() -> usize {
    2
}

impl Default for NumaConfig {
    fn default() -> Self {
        Self {
            mode: NumaMode::Off,
            home_node: 0,
            data_nodes: Vec::new(),
            reserve_cores_per_node: default_numa_reserve_cores(),
            foreground_cores_per_node: 0,
            cold_cache_policy: ColdCachePolicy::Auto,
            allow_overcommit: false,
        }
    }
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

        // Chunklet provides BOTH LV3 (data) and LV2 (buffer) from its pool, so
        // when it is enabled with a PD list the engine is fully configured —
        // storage.data_device / buffer.device are intentionally absent.
        if self.chunklet.enabled && !self.chunklet.devices.is_empty() {
            return ConfiguredMode::Active;
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
        let config: Self = toml::from_str(&content)
            .map_err(|e| OnyxError::Config(format!("failed to parse config: {}", e)))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> OnyxResult<()> {
        self.chunklet.validate_write_scheduler()
    }
}

/// Buffer-as-sole-journal Phase B / C selector. See
/// `MetaConfig::journal_mode` for the full contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetadbJournalMode {
    /// Legacy: WAL is the authoritative metadata journal. Default.
    Wal,
    /// Phase B observability: WAL still authoritative, but checkpoint
    /// commits also persist `manifest.last_processed_buffer_seq` so a
    /// parallel buffer-replay can be diffed against the WAL state.
    Shadow,
    /// Phase C cutover: commit_ops skips the WAL submit; LV2 buffer +
    /// lifecycle-log are the only journals.
    Buffer,
}

// MetadbJournalMode helper methods removed post-Phase D: metadb has no
// WAL anymore, and the metadb backend force-overrides this enum to
// `Buffer` regardless of what the TOML file says. The enum is kept only
// so old configuration files continue to parse.

/// Which physical store backs metadb.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum MetaBackendKind {
    /// Host-filesystem directory at `meta.path` (default; the `pages.onyx_meta`
    /// flat file + a `lifecycle_log/` segment dir + `onyx-volume-catalog.bin`).
    #[default]
    File,
    /// A chunklet meta LogicalDisk (RAID10). Requires `[chunklet].enabled` +
    /// `meta_ld_id`; `meta.path` is ignored for persistence. Removes the
    /// single-disk metadata SPOF.
    Chunklet,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetaConfig {
    /// Path to metadata directory (on LV1 / XFS). Holds blockmap, refcount,
    /// dedup index, volume metadata. None = bare mode (no metadata store).
    /// Ignored for persistence when `backend = "chunklet"` (kept as a label).
    #[serde(default)]
    pub path: Option<PathBuf>,
    /// Physical store backing metadb: `"file"` (default) or `"chunklet"` (meta
    /// LD). See [`MetaBackendKind`].
    #[serde(default)]
    pub backend: MetaBackendKind,
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
    /// Trigger a metadb checkpoint early when the physical LV2 ring occupancy
    /// reaches this percentage. The ring is the durable write journal in
    /// buffer mode, so this lets deployments use it as the cheap aggregation
    /// window while retaining headroom for a checkpoint to finish before
    /// write throttling starts. Set to 0 to disable the ring trigger.
    #[serde(default)]
    pub checkpoint_ring_fill_pct: u8,
    /// How long metadb's WAL writer waits for new sibling committers before
    /// fsyncing a partial group-commit batch. The writer still drains already
    /// queued commits first, so this should stay tiny on low-latency NVMe.
    #[serde(default = "default_metadb_group_commit_timeout_us")]
    pub group_commit_timeout_us: u64,
    /// Async-WAL group-commit window: how long the writer holds the
    /// batch open before acking when every submit in the batch has
    /// `synchronous=false`. Metadb default is 1000 µs (amortises fsync
    /// cost). With `wal_async_commits_enabled=true` plus
    /// `commit_deferred_outcomes_enabled=true` (both default on),
    /// fsync runs only at the BFG-sync barrier, so the window is pure
    /// per-submit latency. 2026-05-27 instrumentation pinned this as
    /// the dominant chunk of `wal_submit_us`.
    #[serde(default = "default_metadb_wal_async_group_commit_window_us")]
    pub wal_async_group_commit_window_us: u64,
    /// Buffer-as-sole-journal Phase B / C selector. `"wal"` (default)
    /// keeps the legacy hot-path WAL submit + fsync_all_lanes barrier.
    /// `"buffer"` skips the WAL submit, treating the LV2 buffer as the
    /// single intent log: metadb mutations live in memory until a
    /// checkpoint persists them, and onyx replays buffer entries with
    /// `seq > manifest.last_processed_buffer_seq` on open.
    ///
    /// `"shadow"` (Phase B observability) keeps the WAL authoritative
    /// but additionally maintains `manifest.last_processed_buffer_seq`
    /// on every checkpoint so a parallel buffer-replay can be compared
    /// against the WAL-derived state. No commit-path latency cost; the
    /// only added work is one extra atomic load + manifest field write
    /// per checkpoint.
    ///
    /// See `.claude/plans/ethereal-exploring-pretzel.md` for the
    /// migration plan.
    #[serde(default = "default_metadb_journal_mode")]
    pub journal_mode: MetadbJournalMode,
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
    /// Run metadb's background Lineage GC driver — the **only** trigger
    /// that surfaces dead LV3 PBAs (`FreePbas`) so onyx can free them.
    /// Without it dead-list chains grow without bound and no reclaim ever
    /// happens (`gc_lineage_freed_blocks` stays 0). Defaults ON; disable
    /// only for A/B or debugging. (metadb's own `Config::new` defaults
    /// this OFF — onyx overrides it here.)
    #[serde(default = "default_metadb_lineage_gc_enabled")]
    pub lineage_gc_enabled: bool,
    /// Idle park (ms) between Lineage GC wakes once nothing more can
    /// advance. Default 1000 ms.
    #[serde(default = "default_metadb_lineage_gc_interval_ms")]
    pub lineage_gc_interval_ms: u64,
    /// Per-wake budget: GC cycles (each advances ≤1 dead-list segment per
    /// volume) driven before parking. Bounds `apply_gate.write()` pressure
    /// on the commit path while letting a backlog drain. Default 256.
    #[serde(default = "default_metadb_lineage_gc_max_cycles_per_wake")]
    pub lineage_gc_max_cycles_per_wake: usize,
    /// Lineage GC head-advance: DROP `rc > 0` (dedup-membership) dead-list
    /// records and advance the head past them, surfacing only the `rc == 0`
    /// exclusive records — instead of bailing the whole segment on the first
    /// `rc > 0`. This is the reclaim-lag fix: under sustained dedup overwrite
    /// the old whole-segment bail left every rc==0 exclusive PBA stranded
    /// behind a dedup-shared sibling, so `gc_lineage_freed_blocks` stayed ~0
    /// and a 320 GiB volume grew to multiples of its logical size.
    ///
    /// Safe for onyx because onyx exposes NO snapshot/clone (CLI + meta layer
    /// both lack it), so every `rc > 0` PBA is a dedup target whose reclaim is
    /// owned by the dedup orphan-reclaim path — dropping the redundant dead-list
    /// record cannot leak. Defaults ON. **Must stay OFF if onyx ever gains
    /// snapshot/clone** (a clone-promotion-shared PBA has no dedup entry and
    /// would then leak). metadb standalone defaults this OFF.
    #[serde(default = "default_metadb_lineage_gc_drop_dedup_shared")]
    pub lineage_gc_drop_dedup_shared: bool,
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

    /// Move the dedup-index cuckoo insert off the commit/apply critical
    /// path: a per-dedup-shard background drainer absorbs staged
    /// `(hash → value)` mutations into the on-disk cuckoo table. Ships
    /// default-off (behind a flag, like `parallel_l2p_drain_enabled`)
    /// until the soak gate passes — flag off is byte-identical to the
    /// eager path. The paired `rc.stage(±1)` stays inline regardless.
    #[serde(default = "default_dedup_drainer_enabled")]
    pub dedup_drainer_enabled: bool,
    #[serde(default = "default_dedup_drainer_interval_ms")]
    pub dedup_drainer_interval_ms: u64,
    #[serde(default = "default_dedup_drainer_threshold_entries")]
    pub dedup_drainer_threshold_entries: usize,
    #[serde(default = "default_dedup_drainer_max_entries_per_cycle")]
    pub dedup_drainer_max_entries_per_cycle: usize,
    #[serde(default = "default_dedup_drainer_backpressure_entries")]
    pub dedup_drainer_backpressure_entries: usize,

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

    /// Global L2P mutation budget per Open BFG. The crossing batch is admitted
    /// and closes that generation to later commits until it rolls, bounding
    /// submitted work by `limit + max_single_batch - 1`. The checkpoint timer
    /// remains a low-rate fallback. Attempted mutations are counted
    /// conservatively; the trigger is suspended while snapshots are live,
    /// whose lifecycle uses its own forced checkpoint boundaries.
    #[serde(default = "default_l2p_buffer_soft_entries")]
    pub l2p_buffer_soft_entries: usize,

    /// Reserved compatibility setting for a future per-shard hard trigger.
    /// It is currently parsed and forwarded but not enforced; use
    /// `l2p_buffer_soft_entries` for the active global BFG admission bound.
    #[serde(default = "default_l2p_buffer_hard_entries")]
    pub l2p_buffer_hard_entries: usize,

    /// Pipeline-mode hard ceiling on admitted-but-not-yet-folded L2P entries
    /// across all active BFG generations (forwarded to metadb
    /// `l2p_buffer_total_hard_entries`). Bounds RAM/WAL when
    /// `bfg_admission_pipeline_enabled` decouples admission from the fold.
    #[serde(default = "default_l2p_buffer_total_hard_entries")]
    pub l2p_buffer_total_hard_entries: usize,

    /// Maximum wall time the compactor may wait between cycles when
    /// the entry-count triggers do not fire.
    #[serde(default = "default_l2p_buffer_max_interval_ms")]
    pub l2p_buffer_max_interval_ms: u64,

    /// Enable the direct L2P apply fast path.
    /// When `true` and every target L2P shard runs in `use_buffer`
    /// mode (requires `l2p_buffer_enabled = true`), L2P-only commits
    /// apply on the caller thread instead of enqueuing closures onto
    /// per-shard apply-lane workers. Falls back to the lane path
    /// automatically for commits that don't match the eligibility
    /// check. See `metadb/src/config.rs::commit_direct_apply_enabled`.
    #[serde(default = "default_commit_direct_apply_enabled")]
    pub commit_direct_apply_enabled: bool,

    /// Enable deferred commit outcomes on the onyx side. When `true`,
    /// the commit_worker calls metadb's
    /// `Db::commit_ops_deferred` and the returned outcomes are
    /// delivered at the next BFG sync boundary rather than
    /// synchronously on the commit thread.
    /// This config also requires the metadb-side flag
    /// `commit_deferred_outcomes_enabled = true`; with either flag
    /// off the call resolves to the existing synchronous path.
    /// This lets each per-volume commit worker pipeline multiple
    /// metadata commits instead of waiting for each one inline.
    #[serde(default = "default_commit_deferred_outcomes_enabled")]
    pub commit_deferred_outcomes_enabled: bool,

    /// Enable the BFG background workers in metadb. When `true`,
    /// metadb spawns the `BfgQuiesceThread` + `BfgSyncThread`: the
    /// open BFG rolls on a timer and a
    /// background sync drains only the frozen syncing slot per cycle,
    /// so buffer-ring reclaim (`release_below`) runs continuously
    /// instead of being gated behind one giant inline checkpoint.
    /// Requires `l2p_buffer_enabled = true` (buffer mode).
    #[serde(default = "default_bfg_threads_enabled")]
    pub bfg_threads_enabled: bool,

    /// Decouple BFG commit admission from L2P fold completion (forwarded to
    /// metadb `bfg_admission_pipeline_enabled`). The quiesce worker rolls the
    /// next generation without blocking on the prior fold, so a soft-limit
    /// crossing during a fold no longer parks commits for the fold's duration.
    /// Requires `bfg_threads_enabled` + `l2p_buffer_enabled`. Default off.
    #[serde(default = "default_bfg_admission_pipeline_enabled")]
    pub bfg_admission_pipeline_enabled: bool,

    /// Stream threads-on refcount checkpoint page writeback in bounded chunks.
    /// Disable only for a controlled A/B against the legacy one-shot path;
    /// commit queues, BFG cadence, and shard selection remain unchanged.
    #[serde(default = "default_rc_checkpoint_streaming_enabled")]
    pub rc_checkpoint_streaming_enabled: bool,

    /// Encode candidate RC delta-run pages during checkpoint sampling and
    /// discard them after exporting size/CPU metrics. This never changes
    /// durable metadata and is intended only for controlled L3 measurements.
    #[serde(default = "default_rc_delta_run_shadow_enabled")]
    pub rc_delta_run_shadow_enabled: bool,

    /// L3: make delta-run segments the DURABLE refcount representation between
    /// condenses — the streaming BFG checkpoint appends a compact segment
    /// instead of rewriting scattered base pages, and the base array is
    /// condensed only every [`Self::rc_condense_interval_cycles`] cycles.
    /// Requires `rc_checkpoint_streaming_enabled` + `bfg_threads_enabled`.
    /// Default off (byte/behaviour-identical to the eager fold when off). ⚠ Do
    /// not enable across restarts on the box until the metadb soak gate passes.
    #[serde(default = "default_rc_delta_run_persist_enabled")]
    pub rc_delta_run_persist_enabled: bool,

    /// Condense interval K: fold a shard's accumulated segments back into the
    /// base array once it has appended this many since the last condense. Only
    /// consulted when [`Self::rc_delta_run_persist_enabled`] is on. Default 8.
    #[serde(default = "default_rc_condense_interval_cycles")]
    pub rc_condense_interval_cycles: u64,

    /// Global cap on unique PBAs across every shard's segment overlay; a shard
    /// force-condenses when its slice is exceeded. Only consulted when
    /// [`Self::rc_delta_run_persist_enabled`] is on. Default 4_000_000.
    #[serde(default = "default_rc_segment_overlay_max_entries")]
    pub rc_segment_overlay_max_entries: usize,

    /// Fan the per-BFG L2P syncing-slot drain out across shards instead of
    /// folding them serially on the single `metadb-bfg-sync` thread (which
    /// was the drain bottleneck capping single-volume write throughput).
    /// Default off because the historical implementation regressed on
    /// nvme-box; enable only for a current A/B.
    #[serde(default = "default_parallel_l2p_drain_enabled")]
    pub parallel_l2p_drain_enabled: bool,

    /// Maximum concurrently executing L2P shard folds when parallel drain is
    /// enabled. `0` preserves the legacy unbounded fan-out; positive values are
    /// a hard cap. Set this explicitly for production A/B runs.
    #[serde(default = "default_parallel_l2p_drain_workers")]
    pub parallel_l2p_drain_workers: usize,

    /// Bound on buffered entries the BFG syncing-slot drain folds per
    /// `tree.write()` hold. The one-shot fold parked every commit worker
    /// (`apply_l2p_remap` takes the same write lock) and dedup/read
    /// lookup (read lock) on the shard for the fold's full multi-second
    /// duration. `0` = legacy unbounded hold (A/B fallback). Default 4096.
    #[serde(default = "default_l2p_drain_chunk_entries")]
    pub l2p_drain_chunk_entries: usize,

    /// Overlap the next frozen BFG's serial L2P fold with current checkpoint
    /// page IO. This is a cross-stage pipeline, not the disproven per-shard
    /// parallel drain. Snapshot/clone lifecycles automatically fall back to
    /// strict serial checkpoint boundaries.
    #[serde(default = "default_l2p_checkpoint_pipeline_enabled")]
    pub l2p_checkpoint_pipeline_enabled: bool,

    /// Make PBA refcount authoritative for ALL live L2P references so GC
    /// reclaim is a pure `rc==0` check and the full-volume `referenced_extents`
    /// reverify scan (which stalled the BFG fold/checkpoint → multi-second
    /// commit spikes) is eliminated. Default `false`. ⚠ Requires a FRESH
    /// metadb — turning this on against an existing store is refused at open
    /// (existing `rc==0` exclusive PBAs would be mass-premature-freed).
    #[serde(default = "default_rc_authoritative_reclaim")]
    pub rc_authoritative_reclaim: bool,
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

    pub fn wal_async_group_commit_window_us(&self) -> u64 {
        self.wal_async_group_commit_window_us
    }

    pub fn lsm_bloom_bits_per_entry(&self) -> u32 {
        self.lsm_bloom_bits_per_entry.clamp(1, 32)
    }
}

impl Default for MetaConfig {
    fn default() -> Self {
        Self {
            path: None,
            backend: MetaBackendKind::File,
            block_cache_mb: default_block_cache_mb(),
            memtable_budget_mb: 0,
            index_pin_mb: default_index_pin_mb(),
            lsm_bloom_bits_per_entry: default_lsm_bloom_bits_per_entry(),
            checkpoint_interval_ms: default_metadb_checkpoint_interval_ms(),
            checkpoint_ring_fill_pct: 0,
            group_commit_timeout_us: default_metadb_group_commit_timeout_us(),
            wal_async_group_commit_window_us: default_metadb_wal_async_group_commit_window_us(),
            journal_mode: default_metadb_journal_mode(),
            flush_dirty_pages_threshold: default_metadb_flush_dirty_pages_threshold(),
            flush_dirty_pages_target: default_metadb_flush_dirty_pages_target(),
            io_submitter_bg_inflight_cap: default_metadb_io_submitter_bg_inflight_cap(),
            flush_select_budget: default_metadb_flush_select_budget(),
            async_reclaim_enabled: default_metadb_async_reclaim_enabled(),
            async_reclaim_max_pages_per_cycle: default_metadb_async_reclaim_max_pages_per_cycle(),
            async_reclaim_idle_interval_ms: default_metadb_async_reclaim_idle_interval_ms(),
            lineage_gc_enabled: default_metadb_lineage_gc_enabled(),
            lineage_gc_interval_ms: default_metadb_lineage_gc_interval_ms(),
            lineage_gc_max_cycles_per_wake: default_metadb_lineage_gc_max_cycles_per_wake(),
            lineage_gc_drop_dedup_shared: default_metadb_lineage_gc_drop_dedup_shared(),
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
            dedup_drainer_enabled: default_dedup_drainer_enabled(),
            dedup_drainer_interval_ms: default_dedup_drainer_interval_ms(),
            dedup_drainer_threshold_entries: default_dedup_drainer_threshold_entries(),
            dedup_drainer_max_entries_per_cycle: default_dedup_drainer_max_entries_per_cycle(),
            dedup_drainer_backpressure_entries: default_dedup_drainer_backpressure_entries(),
            l2p_writeback_enabled: default_l2p_writeback_enabled(),
            l2p_writeback_idle_sleep_us: default_l2p_writeback_idle_sleep_us(),
            l2p_writeback_min_dirty_pages: default_l2p_writeback_min_dirty_pages(),
            l2p_writeback_max_pages_per_cycle: default_l2p_writeback_max_pages_per_cycle(),
            l2p_buffer_enabled: default_l2p_buffer_enabled(),
            l2p_buffer_soft_entries: default_l2p_buffer_soft_entries(),
            l2p_buffer_hard_entries: default_l2p_buffer_hard_entries(),
            l2p_buffer_total_hard_entries: default_l2p_buffer_total_hard_entries(),
            l2p_buffer_max_interval_ms: default_l2p_buffer_max_interval_ms(),
            commit_direct_apply_enabled: default_commit_direct_apply_enabled(),
            commit_deferred_outcomes_enabled: default_commit_deferred_outcomes_enabled(),
            bfg_threads_enabled: default_bfg_threads_enabled(),
            bfg_admission_pipeline_enabled: default_bfg_admission_pipeline_enabled(),
            rc_checkpoint_streaming_enabled: default_rc_checkpoint_streaming_enabled(),
            rc_delta_run_shadow_enabled: default_rc_delta_run_shadow_enabled(),
            rc_delta_run_persist_enabled: default_rc_delta_run_persist_enabled(),
            rc_condense_interval_cycles: default_rc_condense_interval_cycles(),
            rc_segment_overlay_max_entries: default_rc_segment_overlay_max_entries(),
            parallel_l2p_drain_enabled: default_parallel_l2p_drain_enabled(),
            parallel_l2p_drain_workers: default_parallel_l2p_drain_workers(),
            l2p_drain_chunk_entries: default_l2p_drain_chunk_entries(),
            l2p_checkpoint_pipeline_enabled: default_l2p_checkpoint_pipeline_enabled(),
            rc_authoritative_reclaim: default_rc_authoritative_reclaim(),
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
fn default_dedup_drainer_enabled() -> bool {
    // Default-off behind a flag (see field doc); flip on after soak.
    false
}
fn default_dedup_drainer_interval_ms() -> u64 {
    50
}
fn default_dedup_drainer_threshold_entries() -> usize {
    4_096
}
fn default_dedup_drainer_max_entries_per_cycle() -> usize {
    65_536
}
fn default_dedup_drainer_backpressure_entries() -> usize {
    16_384
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
    // Default off until every production profile is ready for buffered L2P
    // commits. Enabling this makes commits write the per-shard buffer first;
    // the compactor folds entries into the tree later.
    false
}
fn default_l2p_buffer_soft_entries() -> usize {
    // This is a global per-BFG admission budget, not the old per-shard
    // compactor wake threshold. The nvme-box A/B validated 4 M mutations;
    // retaining the historical 64 K default causes checkpoint storms in any
    // profile that enables buffered L2P without an explicit override.
    4_000_000
}
fn default_l2p_buffer_hard_entries() -> usize {
    512_000
}
fn default_l2p_buffer_total_hard_entries() -> usize {
    // 12 M entries ~= +0.9 GB worst-case RAM over the 4 M soft budget; caps
    // pipeline-mode outstanding work when the fold falls behind.
    12_000_000
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
    // Production default. Paired with the onyx
    // `commit_worker_deferred_outcomes` default so both sides engage
    // together.
    true
}
fn default_bfg_threads_enabled() -> bool {
    // Default ON: the BFG quiesce/sync workers keep metadata draining in
    // the background, which keeps buffer reclaim moving during sustained
    // write load. Only engages when `l2p_buffer_enabled = true`.
    true
}
fn default_bfg_admission_pipeline_enabled() -> bool {
    // Default OFF: the legacy at-most-one-Quiescing blocking promote is the
    // validated path. Flip on (with bfg_threads + l2p_buffer) to decouple
    // commit admission from L2P fold completion.
    false
}
fn default_rc_checkpoint_streaming_enabled() -> bool {
    true
}
fn default_rc_delta_run_shadow_enabled() -> bool {
    false
}
fn default_rc_delta_run_persist_enabled() -> bool {
    false
}
fn default_rc_condense_interval_cycles() -> u64 {
    8
}
fn default_rc_segment_overlay_max_entries() -> usize {
    4_000_000
}
fn default_parallel_l2p_drain_enabled() -> bool {
    // The bounded worker pool and per-shard NUMA binding remove the old
    // spawn-per-cycle affinity regression. Aged A/B/A' validation retained
    // foreground performance while substantially reducing L2P fold time.
    true
}
fn default_parallel_l2p_drain_workers() -> usize {
    // Leave room for RC apply while parallel L2P folds are active. Zero
    // remains available for an explicit legacy unbounded-fan-out A/B.
    4
}
fn default_l2p_drain_chunk_entries() -> usize {
    // Bounded fold lock-holds default-ON (semantics-preserving: same lock,
    // same op order, same publish-before-clear point). 0 restores the
    // one-shot hold for A/B. Root-caused 2026-06-12: the unbounded hold
    // parked all commit workers + dedup lookups for multi-second folds.
    4096
}
fn default_l2p_checkpoint_pipeline_enabled() -> bool {
    // Default ON. The serial successor-fold checkpoint worker is a box-measured
    // FREE WIN on the aged/sustained-drain wall: median drain +136% (92->218
    // MB/s), hard-stall seconds 42%->22%, write p999 4731ms->261ms (18x). It is
    // a pure runtime worker toggle (no on-disk format gate, no fresh-pool
    // requirement). See memory write_drain_22mbps_lv2_durability_roundtrip.
    true
}
fn default_rc_authoritative_reclaim() -> bool {
    // Default OFF. Turning it on eliminates the reclaim
    // reverify scan + the commit-latency spike, but requires a FRESH metadb
    // (refused at open against an existing store). See memory
    // commit_spike_rc_authoritative_reclaim.
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
    /// Give each flusher writer shard its own io_uring ring for LV3 writes
    /// (default true). Removes the engine-wide serialization where all writers
    /// contend on one shared `Mutex<IoUring>` held across `submit_and_wait`.
    /// Set false to reproduce the legacy single-shared-ring behavior (A/B
    /// baseline). No effect on the syscall backend.
    #[serde(default = "default_lv3_per_shard_write_rings")]
    pub lv3_per_shard_write_rings: bool,
    /// LV3 cross-lane aggregation window, microseconds. Writer lanes block
    /// synchronously until their aggregated batch completes, so this delay is on
    /// the drain path. `0` keeps the compiled default (2000 us). 2026-07-25
    /// nvme-box: the aggregator emitted 259 batches/s of ~300 KiB — 13x short of
    /// `lv3_batch_target_bytes` — so every batch left on this timeout while the
    /// six executors ran at 3.3 % utilisation.
    #[serde(default)]
    pub lv3_batch_coalesce_us: u64,
    /// Byte target that ends an LV3 aggregation window early. `0` keeps the
    /// compiled default (4 MiB = two RAID6 full stripes).
    #[serde(default)]
    pub lv3_batch_target_bytes: usize,
    /// Number of LV3 batch executor threads (each owns one chunklet io_uring).
    /// `0` keeps the compiled default (6).
    ///
    /// Sweep this together with `lv3_batch_coalesce_us`: shortening the window
    /// raised batch frequency 5x on nvme-box, at which point `exec_queue` --
    /// a producer waiting for a free executor -- became the largest non-device
    /// term in its blocked time (1006-1934 us per request vs 347-1016 us at the
    /// 2 ms default). Either knob alone is capped by the other.
    #[serde(default)]
    pub lv3_batch_executors: usize,
    /// RAID-aware full-stripe writes (roadmap ③). When true, the flush writer
    /// allocates + zero-pads each LV3 passthrough write to a whole RAID stripe
    /// (`full_stripe_bytes` from the chunklet LD geometry) so a RAID5/6 backend
    /// takes its zero-RMW full-stripe path instead of read-modify-write parity.
    /// Default false (legacy unaligned writes). Only affects a parity chunklet
    /// backend; non-parity backends report a 1-block stripe and no-op. Pair with
    /// `flush.coalesce_max_raw_bytes` tuned to a stripe multiple to keep pad
    /// waste low. Flag exists for A/B baselining + instant rollback.
    #[serde(default = "default_raid_full_stripe_writes")]
    pub raid_full_stripe_writes: bool,
    /// Pack each RAID stripe from units of the same volume with adjacent LBAs
    /// ("lifetime affinity") instead of purely by block count.
    ///
    /// The stripe reserve only regains a 24 KiB window when *every* block in it
    /// is free at the same time, and a window is never re-folded while one live
    /// block pins it. Size-only bin-packing mixes six unrelated LBAs into one
    /// stripe, so their overwrite times are independent and the window stays
    /// part-pinned indefinitely — that is what drives `stripe_capable` down
    /// monotonically on an aged pool and pushes the writer onto the general
    /// pool. Co-locating same-volume neighbours makes a whole stripe far more
    /// likely to die at once and return to the reserve without any defrag IO.
    ///
    /// Affinity alone can cost space: it hands a subset back to the size-first
    /// packer, and a packer with fewer options can pack worse (measured
    /// counterexample: `[8,2,2,2,6,5,6,4,4,3,1,8,1]`, pad 6 vs 0). So both plans
    /// are computed and affinity is kept only when its stripe padding ties or
    /// beats size-only — the knob can never trade space for co-location.
    /// Default false for A/B baselining and instant rollback.
    #[serde(default)]
    pub stripe_group_lifetime_affinity: bool,
    /// Shard the PBA allocator's free space AND its retired set into this many
    /// address regions, each with its own lock. `0` (default) = compiled default
    /// (2048). `1` = one global lock per structure, i.e. exactly the pre-region
    /// behaviour, kept as the rollback path.
    ///
    /// **On by default since 2026-08-01**, on two nvme-box reads of the same aged
    /// 256 GiB volume (QD256 j16d16, 480 s window deltas, `free_lock.<site>` /
    /// `retired_lock.<site>` in `status`):
    ///
    /// - the single free lock was **68.4% busy with 98% of all holding coming from
    ///   GC retire/reclaim**, and **98.8% of the flush writer's whole allocation
    ///   time was WAIT** while its own holding was 1.9%. Sharding makes no hold
    ///   shorter — it decouples GC's holding from the writer's acquisition:
    ///   `writer_refill` wait **22936 → 136 µs/acq**, `aligned` **871 → 2.13
    ///   µs/op**, writer `alloc` **21.0% → 0.2%** of its time.
    /// - the retired set was a second global mutex behind it (**49.1% busy at
    ///   17567 acq/s**), which sharding on the same layout removed:
    ///   `retire_batch`'s summed region hold **1588 → 153 s** and its wait share
    ///   **87.3% → 2.2%**, `writer_refill wait_max` **1359 → 1.33 ms**.
    ///
    /// ⚠ THROUGHPUT IS FLAT (253.4 → 252.2 MB/s) and always was: the writer spends
    /// 85% in `io`, of which 99.7% is `submit.io` and only 5.7% the real device
    /// write, so the queue lives downstream in LV3 aggregation. What this buys is
    /// ~3 cores of blocked cleanup-thread time and the removal of 1.4-1.65 s tail
    /// events that did reach the writer. Do not expect (or gate on) bandwidth.
    ///
    /// The one deliberate semantic change: aligned allocation becomes first-fit
    /// WITHIN a lane's active region rather than globally. Every other path still
    /// walks regions in ascending address order, which is identical to global
    /// first-fit, and the metadb L2P leaf codec only needs PBAs of one leaf to
    /// stay near each other (leaf ⊂ zone ⊂ lane ⊂ region).
    #[serde(default = "default_allocator_regions")]
    pub allocator_regions: usize,
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
            lv3_per_shard_write_rings: default_lv3_per_shard_write_rings(),
            lv3_batch_coalesce_us: 0,
            lv3_batch_target_bytes: 0,
            lv3_batch_executors: 0,
            raid_full_stripe_writes: default_raid_full_stripe_writes(),
            stripe_group_lifetime_affinity: false,
            allocator_regions: default_allocator_regions(),
        }
    }
}

/// `0` = the allocator's compiled region target (2048). See
/// [`StorageConfig::allocator_regions`] for the box evidence behind turning this
/// on by default; `1` is the single-lock rollback.
fn default_allocator_regions() -> usize {
    0
}

fn default_uring_sq_entries() -> u32 {
    128
}

fn default_lv3_per_shard_write_rings() -> bool {
    true
}

fn default_raid_full_stripe_writes() -> bool {
    false
}

fn default_read_pool_workers() -> usize {
    4
}

/// chunklet RAID backend wiring (roadmap ③ RAID-aware / Phase 8 integration).
/// When `enabled`, LV3 (and later LV2 / metadb) sit on chunklet `LogicalDisk`s
/// carved from one Pool over `devices`, replacing the single-device paths.
#[derive(Debug, Clone, Deserialize)]
pub struct ChunkletConfig {
    /// Master switch. `false` (default) keeps the legacy single-device paths.
    #[serde(default)]
    pub enabled: bool,
    /// Raw PD block devices forming the pool (count ≥ the widest LD set size).
    #[serde(default)]
    pub devices: Vec<PathBuf>,
    /// Cross-PD IO backend inside chunklet: `uring` (default) or `sync`.
    #[serde(default)]
    pub io_backend: ChunkletIoBackend,
    /// Wait for a whole chunklet write batch in ONE `io_uring_enter` instead of
    /// waking once per completion. A logical flush write fans out to hundreds of
    /// per-strip writes whose NVMe completions arrive staggered, so the legacy
    /// per-CQE wake costs one syscall + wakeup round-trip each (box-measured:
    /// ~16 us per 4 KiB strip, submit = 75.6% of chunklet write time). Only the
    /// no-observer drain path changes; streaming/scheduler observers keep their
    /// per-arrival wake. Default off pending the box A/B.
    #[serde(default)]
    pub uring_coalesced_wait: bool,
    /// SQEs per stop-and-wait wave inside chunklet's batched submit paths. A
    /// many-strip write must fully drain each wave before the next is pushed, so
    /// a small wave leaves the disks idle at every barrier (box-measured: an
    /// 847 KB write fans out to ~414 strips = 7 waves at the historical 64).
    /// `0` keeps that legacy 64; the value is capped by chunklet's ring depth.
    #[serde(default)]
    pub uring_write_chunk_ops: usize,
    /// Persistent chunklet io_uring workers reserved for foreground writes.
    /// `0` together with `pd_write_background_workers = 0` preserves the
    /// caller-thread execution path.
    #[serde(default)]
    pub pd_write_foreground_workers: usize,
    /// Persistent chunklet io_uring workers shared by LV3, MetaDB, rebuild,
    /// and rebalance writes. Must be enabled together with the foreground pool.
    #[serde(default)]
    pub pd_write_background_workers: usize,
    /// Pool-wide cap on concurrent chunklet write batches across LV2, LV3,
    /// and MetaDB. `0` disables Onyx-side class admission. When enabled, the
    /// three class reservations below must be non-zero and their sum must not
    /// exceed this value. Remaining slots are shared headroom.
    #[serde(default)]
    pub write_max_active: u32,
    /// LV2 durability share while LV3 or MetaDB writes are waiting.
    #[serde(default)]
    pub write_foreground_active: u32,
    /// LV3 drain share while LV2 or MetaDB writes are waiting.
    #[serde(default)]
    pub write_lv3_active: u32,
    /// MetaDB apply/checkpoint share while LV2 or LV3 writes are waiting.
    #[serde(default)]
    pub write_meta_active: u32,
    /// Per-physical-disk cap measured in actual 4 KiB write blocks after the
    /// chunklet LD expands RAID fanout. `0` disables nested block admission.
    #[serde(default)]
    pub pd_write_max_active_blocks: u64,
    /// Guaranteed LV2 foreground floor per PD while that class is queued.
    #[serde(default)]
    pub pd_write_foreground_min_blocks: u64,
    /// Guaranteed LV3 drain floor per PD while that class is queued.
    #[serde(default)]
    pub pd_write_lv3_min_blocks: u64,
    /// Guaranteed MetaDB drain floor per PD while that class is queued.
    #[serde(default)]
    pub pd_write_meta_min_blocks: u64,
    /// Guaranteed rebuild/rebalance floor per PD while maintenance is queued.
    #[serde(default)]
    pub pd_write_maintenance_min_blocks: u64,
    /// Per-PD free chunklets reserved for rebuild (percent, default 5).
    #[serde(default = "default_chunklet_spare_pct")]
    pub spare_pct: u8,
    /// LV3 data LD geometry (RAID6). Used by `chunklet-init`; id resolved after.
    #[serde(default = "ChunkletLdGeom::lv3_default")]
    pub lv3: ChunkletLdGeom,
    /// LV2 write-buffer LD geometry (RAID10). Phase 2.
    #[serde(default = "ChunkletLdGeom::lv2_default")]
    pub lv2: ChunkletLdGeom,
    /// metadb LD geometry (RAID10). Phase 3.
    #[serde(default = "ChunkletLdGeom::meta_default")]
    pub meta: ChunkletLdGeom,
    /// LD ids (UUID strings) resolved by `chunklet-init` and written back here.
    /// When a role's id is unset but exactly one LD exists in the pool, onyx
    /// falls back to that single LD.
    #[serde(default)]
    pub lv3_ld_id: Option<String>,
    #[serde(default)]
    pub lv2_ld_id: Option<String>,
    #[serde(default)]
    pub meta_ld_id: Option<String>,
    /// Runtime PD health watchdog (Phase 4d): a background thread periodically
    /// probes each live PD (`Pool::probe_pd_liveness`) and auto-marks
    /// unresponsive ones Failed after `watchdog_fail_threshold` consecutive
    /// misses. Default `false` (opt-in) — a false positive marks a healthy PD
    /// Failed and forces degraded reads until it is cleared/rebuilt.
    #[serde(default)]
    pub watchdog_enabled: bool,
    /// Seconds between watchdog probe sweeps (default 10). Kept coarse: a probe
    /// is a single O_DIRECT read per PD, and a pulled disk is not a sub-second
    /// event.
    #[serde(default = "default_watchdog_interval_secs")]
    pub watchdog_interval_secs: u64,
    /// Consecutive failed probes before a PD is marked Failed (default 3).
    /// Debounces a single transient IO hiccup from flapping a PD.
    #[serde(default = "default_watchdog_fail_threshold")]
    pub watchdog_fail_threshold: u32,
    /// After the watchdog auto-marks a PD Failed, automatically rebuild every
    /// affected redundant LD onto spares (`Pool::auto_recover`). Default `true`
    /// — inert unless `watchdog_enabled` is also on, in which case auto-heal is
    /// the sensible posture.
    ///
    /// chunklet's `rebuild_ld` is now ONLINE (non-blocking): it holds only
    /// `io_lock.read()` during the backfill and swaps the descriptor under a
    /// brief write lock, so foreground IO keeps flowing and a live ublk is not
    /// starved. (The old whole-op write lock that made this unsafe is gone.)
    #[serde(default = "default_true")]
    pub auto_failover: bool,
    /// Content-addressed device discovery: scan `device_glob` and select the
    /// pool's PDs by the on-disk pool_id in their superblocks, not by their
    /// `/dev/nvmeXnY` path. Robust to NVMe re-enumeration across reboots /
    /// hot-plug (a returned disk reappears under a new name). Default `true`;
    /// `devices` is always the seed/fallback set. When off, `devices` is used
    /// verbatim (legacy behavior).
    #[serde(default = "default_true")]
    pub device_discovery: bool,
    /// Glob of candidate raw devices to probe for this pool (e.g.
    /// `/dev/nvme*n*`). Only used when `device_discovery` is on: a single
    /// directory + a `*`-wildcard filename pattern. Matched paths are probed
    /// and the majority-pool_id set is kept.
    #[serde(default)]
    pub device_glob: Option<String>,
    /// Tolerant open: if a strict open fails because a PD is missing, retry with
    /// `Pool::open_with_missing` so the engine starts degraded (reads reconstruct
    /// from redundancy; the missing member is rebuilt/reintegrated online).
    /// Default `true` — full-auto operation must survive a single absent disk at
    /// boot. A complete pool always opens strict (which also reverse-reconciles
    /// stale capacity); only a genuinely-incomplete set falls to the degraded
    /// path.
    #[serde(default = "default_true")]
    pub tolerant_open: bool,
    /// Full-auto returned-disk reintegration: the watchdog scans `device_glob`
    /// each sweep for a disk whose superblock carries this pool_id and a Failed
    /// tombstone's pd_id (a returned disk), and starts a `reintegrate_wipe` job
    /// automatically (safety-gated in chunklet). Default `true`; inert unless
    /// `watchdog_enabled` + a `device_glob` are set. Idempotent — a disk already
    /// being reintegrated is not re-kicked.
    #[serde(default = "default_true")]
    pub auto_reintegrate: bool,
    /// Full-auto data rebalance: after a reintegrate/failover leaves per-PD
    /// used-skew above `rebalance_target_skew_pct`, the watchdog kicks a bounded
    /// online `rebalance` job (write-forward keeps foreground IO flowing).
    /// Default `true`; inert unless `watchdog_enabled`. Event-driven (fires once
    /// per skew-raising event, never thrashes on a stuck pool).
    #[serde(default = "default_true")]
    pub auto_rebalance: bool,
    /// Target worst-case per-PD used-skew percent for auto/manual rebalance
    /// (default 20).
    #[serde(default = "default_rebalance_target_skew_pct")]
    pub rebalance_target_skew_pct: f64,
    /// Hard cap on committed moves per auto-rebalance cycle (bounded work;
    /// default 256). A cycle that hits the cap simply leaves the rest for the
    /// next skew-raising event.
    #[serde(default = "default_rebalance_max_moves")]
    pub rebalance_max_moves_per_cycle: usize,
}

fn default_true() -> bool {
    true
}

impl Default for ChunkletConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            devices: Vec::new(),
            io_backend: ChunkletIoBackend::default(),
            uring_coalesced_wait: false,
            uring_write_chunk_ops: 0,
            pd_write_foreground_workers: 0,
            pd_write_background_workers: 0,
            write_max_active: 0,
            write_foreground_active: 0,
            write_lv3_active: 0,
            write_meta_active: 0,
            pd_write_max_active_blocks: 0,
            pd_write_foreground_min_blocks: 0,
            pd_write_lv3_min_blocks: 0,
            pd_write_meta_min_blocks: 0,
            pd_write_maintenance_min_blocks: 0,
            spare_pct: default_chunklet_spare_pct(),
            lv3: ChunkletLdGeom::lv3_default(),
            lv2: ChunkletLdGeom::lv2_default(),
            meta: ChunkletLdGeom::meta_default(),
            lv3_ld_id: None,
            lv2_ld_id: None,
            meta_ld_id: None,
            watchdog_enabled: false,
            watchdog_interval_secs: default_watchdog_interval_secs(),
            watchdog_fail_threshold: default_watchdog_fail_threshold(),
            auto_failover: true,
            device_discovery: true,
            device_glob: None,
            tolerant_open: true,
            auto_reintegrate: true,
            auto_rebalance: true,
            rebalance_target_skew_pct: default_rebalance_target_skew_pct(),
            rebalance_max_moves_per_cycle: default_rebalance_max_moves(),
        }
    }
}

impl ChunkletConfig {
    pub fn validate_write_scheduler(&self) -> OnyxResult<()> {
        let shares = [
            self.write_foreground_active,
            self.write_lv3_active,
            self.write_meta_active,
        ];
        if self.write_max_active == 0 {
            if shares.into_iter().any(|share| share != 0) {
                return Err(OnyxError::Config(
                    "chunklet write class shares require write_max_active > 0".into(),
                ));
            }
        } else {
            if !self.enabled {
                return Err(OnyxError::Config(
                    "chunklet write scheduler requires [chunklet].enabled = true".into(),
                ));
            }
            let sum = shares
                .into_iter()
                .try_fold(0u32, |total, share| total.checked_add(share));
            if shares.into_iter().any(|share| share == 0)
                || !sum.is_some_and(|total| total <= self.write_max_active)
            {
                return Err(OnyxError::Config(
                    "chunklet write scheduler requires three non-zero reservations whose checked sum does not exceed write_max_active"
                        .into(),
                ));
            }
        }

        self.validate_pd_write_workers()?;
        self.validate_pd_write_scheduler()
    }

    fn validate_pd_write_workers(&self) -> OnyxResult<()> {
        let foreground = self.pd_write_foreground_workers;
        let background = self.pd_write_background_workers;
        if (foreground == 0) != (background == 0) {
            return Err(OnyxError::Config(
                "chunklet persistent write execution requires both pd_write_foreground_workers and pd_write_background_workers to be zero or both to be non-zero"
                    .into(),
            ));
        }
        if foreground == 0 {
            return Ok(());
        }
        if !self.enabled {
            return Err(OnyxError::Config(
                "chunklet persistent write execution requires [chunklet].enabled = true".into(),
            ));
        }
        if self.io_backend != ChunkletIoBackend::Uring {
            return Err(OnyxError::Config(
                "chunklet persistent write execution requires io_backend = \"uring\"".into(),
            ));
        }
        Ok(())
    }

    fn validate_pd_write_scheduler(&self) -> OnyxResult<()> {
        let minimums = [
            self.pd_write_foreground_min_blocks,
            self.pd_write_lv3_min_blocks,
            self.pd_write_meta_min_blocks,
            self.pd_write_maintenance_min_blocks,
        ];
        if self.pd_write_max_active_blocks == 0 {
            if minimums.into_iter().any(|minimum| minimum != 0) {
                return Err(OnyxError::Config(
                    "chunklet per-PD write minimums require pd_write_max_active_blocks > 0".into(),
                ));
            }
            return Ok(());
        }
        if !self.enabled {
            return Err(OnyxError::Config(
                "chunklet per-PD write scheduler requires [chunklet].enabled = true".into(),
            ));
        }
        let sum = minimums
            .into_iter()
            .try_fold(0u64, |total, minimum| total.checked_add(minimum));
        if minimums.into_iter().any(|minimum| minimum == 0)
            || !sum.is_some_and(|total| total <= self.pd_write_max_active_blocks)
        {
            return Err(OnyxError::Config(
                "chunklet per-PD write scheduler requires four non-zero class minimums whose checked sum does not exceed pd_write_max_active_blocks"
                    .into(),
            ));
        }
        Ok(())
    }
}

fn default_rebalance_target_skew_pct() -> f64 {
    20.0
}

fn default_rebalance_max_moves() -> usize {
    256
}

fn default_chunklet_spare_pct() -> u8 {
    5
}

fn default_watchdog_interval_secs() -> u64 {
    10
}

fn default_watchdog_fail_threshold() -> u32 {
    3
}

/// Cross-PD IO submission backend selector inside chunklet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChunkletIoBackend {
    #[default]
    Uring,
    Sync,
}

/// LD geometry for `chunklet-init`, mirroring chunklet's `LdSpec` constructors.
/// `raid` selects the level; the remaining fields map onto that constructor's
/// parameters (see chunklet `pool::LdSpec`).
#[derive(Debug, Clone, Deserialize)]
pub struct ChunkletLdGeom {
    /// `raid6` | `raid10` | `raid5` | `raid0` | `plain`.
    pub raid: String,
    /// raid5/6: data chunklets per set (K). raid10/mirror: mirror copies.
    /// raid0: stripe width. plain: chunklet count.
    #[serde(default = "default_geom_set")]
    pub set_size: u8,
    /// Rows of sets striped together (1 = no striping above the set).
    #[serde(default = "default_geom_one")]
    pub row_size: u16,
    /// Rows chained for capacity.
    #[serde(default = "default_geom_one")]
    pub num_rows: u16,
    /// Strip size in KiB; 0 = one 4 KiB block. Must be a power of two.
    #[serde(default)]
    pub strip_kib: u32,
}

impl ChunkletLdGeom {
    fn lv3_default() -> Self {
        // RAID6 over 6 data + 2 parity = 8 PDs; 256 KiB strip for big sequential.
        Self {
            raid: "raid6".to_string(),
            set_size: 6,
            row_size: 1,
            num_rows: 1,
            strip_kib: 256,
        }
    }
    fn lv2_default() -> Self {
        // RAID10: 2-way mirror striped 2 wide.
        Self {
            raid: "raid10".to_string(),
            set_size: 2,
            row_size: 2,
            num_rows: 1,
            strip_kib: 0,
        }
    }
    fn meta_default() -> Self {
        Self {
            raid: "raid10".to_string(),
            set_size: 2,
            row_size: 2,
            num_rows: 1,
            strip_kib: 0,
        }
    }
}

fn default_geom_set() -> u8 {
    6
}

fn default_geom_one() -> u16 {
    1
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
    /// Legacy: pre-sync (volatile) payload budget. Ignored — append now
    /// blocks until the seq is fdatasync'd on LV2, so there is no
    /// pre-sync window to budget. Retained as a `#[serde(default)]` field
    /// for backward compatibility with existing TOML configs.
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
    /// Prepared-batch channel depth for each LV2 global root-write lane.
    /// Smaller values propagate a slow root write back to shard staging sooner,
    /// allowing later entries to coalesce before encoding. 0 preserves the
    /// existing depth of one slot per buffer shard.
    #[serde(default)]
    pub lv2_prepared_queue_depth_per_lane: usize,
    /// Drain LV2 ring backpressure BEFORE taking the append-order stripe
    /// locks, so a full ring blocks one appender instead of convoying everyone
    /// who collides with its LBAs.
    ///
    /// Measured 2026-07-26, randrw 70/30 on an aged full volume: `order_hold`
    /// 30.0 ms was within 0.1 ms of `backpressure_wait` 29.9 ms — the stripe
    /// locks were held for the entire space wait — and the resulting
    /// `order_wait` was 20.8 ms of a 67 ms append. Default off pending an A/B.
    #[serde(default)]
    pub prewait_ring_space_outside_order: bool,
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
    /// Also drive the foreground throttle from sustained MetaDB commit
    /// executor debt. Disabled by default so existing physical-fill profiles
    /// keep identical pacing until this controller is explicitly enabled.
    #[serde(default)]
    pub throttle_backend_debt: bool,
    /// Number of LV2 fdatasync chains a sync thread keeps in flight at once.
    /// 1 = legacy serial behaviour (submit → wait-all → submit-next). >1
    /// pipelines: batch N+1's writes overlap batch N's fdatasync flush, so the
    /// per-batch fsync stall no longer gates the front→buffer→flush pipeline.
    /// Durability is preserved by advancing the LV2 watermark only over the
    /// contiguous FIFO prefix of fully-fsync'd batches. 0 = engine default (2).
    #[serde(default)]
    pub lv2_sync_pipeline_depth: usize,
    /// ZFS `zfs_commit_timeout_pct` analog for the LV2 sync pipeline. The OPEN
    /// (accumulating) batch is sealed when full OR after
    /// `ema_lv2_write_latency * pct/100` (floored) of accumulation, so the
    /// window self-clocks to device latency and overlaps in-flight fdatasyncs.
    /// Larger = bigger batches / higher latency; smaller = lower latency.
    /// 0 = engine default (10).
    #[serde(default)]
    pub lv2_commit_timeout_pct: u64,
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
            lv2_prepared_queue_depth_per_lane: 0,
            prewait_ring_space_outside_order: false,
            throttle_min_pct: 0,
            throttle_max_pct: 0,
            throttle_scale_us: 0,
            throttle_cap_us: 0,
            throttle_backend_debt: false,
            lv2_sync_pipeline_depth: 0,
            lv2_commit_timeout_pct: 0,
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
    /// Keep newly durable LV2 entries resident for at least this long before
    /// admitting them to the LV3 flush pipeline. This gives later writes to
    /// the same LBA a chance to supersede the old version in the cheap buffer.
    /// `0` preserves immediate draining.
    #[serde(default)]
    pub buffer_write_window_ms: u64,
    /// Deprecated combined-pressure knob retained for config compatibility.
    /// When both split thresholds are unset, this value is used for physical
    /// and payload pressure, preserving old configuration behavior.
    #[serde(default)]
    pub buffer_write_window_pressure_pct: u8,
    /// Bypass the write window when this shard's physical LV2 ring fill reaches
    /// the threshold. Admission remains paced until the separate QoS emergency
    /// watermark is reached. `0` falls back to the legacy field, then 80%.
    #[serde(default)]
    pub buffer_write_window_physical_pressure_pct: u8,
    /// Bypass the write window when the global resident-payload cache reaches
    /// this threshold. Payload pressure is deliberately separate from physical
    /// LV2 occupancy: it should admit old work, but must not unleash an
    /// unpaced, device-wide catch-up burst while foreground IO is active. `0`
    /// means 80 percent.
    #[serde(default)]
    pub buffer_write_window_payload_pressure_pct: u8,
    /// Foreground durable-write p99 target used by the global flush-admission
    /// controller. The controller AIMD-adjusts aggregate raw bytes admitted to
    /// coalesce/dedup/compress while ublk writes are active. `0` disables QoS.
    #[serde(default = "default_foreground_flush_target_p99_ms")]
    pub foreground_flush_target_p99_ms: u64,
    /// Normal aggregate admission rate while foreground writes are active and
    /// LV2 occupancy is below the recovery watermark. `0` means 128 MiB/s.
    #[serde(default)]
    pub foreground_flush_active_mib_s: u64,
    /// Lower bound after latency-driven multiplicative backoff. `0` means 32
    /// MiB/s; space-recovery mode can override it.
    #[serde(default)]
    pub foreground_flush_min_mib_s: u64,
    /// Admission rate reached as LV2 approaches its emergency watermark. `0`
    /// means 384 MiB/s.
    #[serde(default)]
    pub foreground_flush_max_mib_s: u64,
    /// Global token-bucket burst allowance. It is clamped to at least one
    /// coalesced unit; `0` means 8 MiB.
    #[serde(default)]
    pub foreground_flush_burst_mib: u64,
    /// Logical/physical LV2 fill where occupancy recovery starts ramping above
    /// the normal foreground-active rate. `0` means 40 percent.
    #[serde(default)]
    pub foreground_flush_recovery_pct: u8,
    /// Physical LV2 fill where admission becomes unlimited so cache QoS cannot
    /// consume durability capacity. `0` means 65 percent.
    #[serde(default)]
    pub foreground_flush_emergency_pct: u8,
    /// Maximum mapped LBAs folded into one packed-slot metadata commit.
    /// Lower values reduce metadb apply head-of-line tail under heavy mixed
    /// read/write load; higher values amortise WAL/apply overhead and improve
    /// drain throughput. Set 0 to use the built-in default.
    #[serde(default = "default_packed_meta_batch_max_lbas")]
    pub packed_meta_batch_max_lbas: usize,
    /// Number of commit executor threads. All executors consume one shared,
    /// bounded MPMC queue, so queued jobs from every writer shard can be
    /// coalesced before MetaDB apply. Capped at NUM_COMMIT_WORKERS.
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
    /// Retain a sub-target commit tail across receive cycles so sustained
    /// traffic can form transactions closer to `commit_target_lbas_per_tx`.
    /// Disabled by default to preserve the legacy fixed-window drain behavior.
    #[serde(default)]
    pub commit_retain_tail: bool,
    /// Maximum aggregation residence measured from the oldest queued commit
    /// job. Retain-tail may seal earlier at the transaction target, under LV2
    /// physical pressure, or after sustained underfill; arrivals never extend
    /// this hard ceiling. Set 0 for try_recv-only drain.
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
    /// Enable the per-volume commit_worker pipeline. When enabled, a
    /// worker can issue up to `commit_worker_pipeline_depth` metadb
    /// `atomic_batch_write_multi_with_dedup_deferred` calls before
    /// blocking on the oldest handle. When disabled, the worker drains
    /// every issued handle immediately, preserving synchronous pacing.
    /// This flag and `metadb.commit_deferred_outcomes_enabled` must
    /// both be true before pipelining changes runtime behavior.
    #[serde(default = "default_commit_worker_deferred_outcomes")]
    pub commit_worker_deferred_outcomes: bool,
    /// Maximum number of in-flight deferred commits a passthrough
    /// commit worker keeps queued before blocking on the oldest. Has
    /// no effect when `commit_worker_deferred_outcomes = false`
    /// (worker still issues + drains one at a time). Default 4
    /// matches the plan's per-volume FIFO budget.
    #[serde(default = "default_commit_worker_pipeline_depth")]
    pub commit_worker_pipeline_depth: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EngineConfig {
    /// Number of zones (default 4)
    #[serde(default = "default_zone_count")]
    pub zone_count: u32,
    /// Blocks per zone (default 256)
    #[serde(default = "default_zone_size_blocks")]
    pub zone_size_blocks: u64,
    /// Max wall time `Engine::open` will spend draining any buffer
    /// entries that exist past the last checkpoint before accepting
    /// client IO. Post-WAL, the buffer ring is the only durable record
    /// of commits between checkpoints, so a graceful start MUST replay
    /// them through the flush pipeline. Default 60000 ms; bump if
    /// you've configured a very deep ring.
    #[serde(default = "default_recovery_timeout_ms")]
    pub recovery_timeout_ms: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            zone_count: default_zone_count(),
            zone_size_blocks: default_zone_size_blocks(),
            recovery_timeout_ms: default_recovery_timeout_ms(),
        }
    }
}

fn default_recovery_timeout_ms() -> u64 {
    60_000
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self {
            compress_workers: default_compress_workers(),
            coalesce_max_raw_bytes: default_coalesce_max_raw_bytes(),
            coalesce_max_lbas: default_coalesce_max_lbas(),
            min_compression_savings_pct: default_min_compression_savings_pct(),
            skip_fully_superseded: default_skip_fully_superseded(),
            buffer_write_window_ms: 0,
            buffer_write_window_pressure_pct: 0,
            buffer_write_window_physical_pressure_pct: 0,
            buffer_write_window_payload_pressure_pct: 0,
            foreground_flush_target_p99_ms: default_foreground_flush_target_p99_ms(),
            foreground_flush_active_mib_s: 0,
            foreground_flush_min_mib_s: 0,
            foreground_flush_max_mib_s: 0,
            foreground_flush_burst_mib: 0,
            foreground_flush_recovery_pct: 0,
            foreground_flush_emergency_pct: 0,
            packed_meta_batch_max_lbas: default_packed_meta_batch_max_lbas(),
            commit_workers_per_volume: default_commit_workers_per_volume(),
            commit_target_lbas_per_tx: default_commit_target_lbas_per_tx(),
            commit_coalesce_lba_budget: default_commit_coalesce_lba_budget(),
            commit_retain_tail: false,
            commit_coalesce_timeout_us: default_commit_coalesce_timeout_us(),
            packed_commit_try_drain_lba_budget: default_packed_commit_try_drain_lba_budget(),
            writer_read_active_batch_size: default_writer_read_active_batch_size(),
            commit_worker_deferred_outcomes: default_commit_worker_deferred_outcomes(),
            commit_worker_pipeline_depth: default_commit_worker_pipeline_depth(),
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
fn default_foreground_flush_target_p99_ms() -> u64 {
    100
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
    16_384
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
fn default_commit_worker_deferred_outcomes() -> bool {
    // Production default. Paired with the metadb-side
    // `commit_deferred_outcomes_enabled`; flipping either alone leaves
    // the pipeline draining each handle inline.
    true
}
fn default_commit_worker_pipeline_depth() -> usize {
    4
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
fn default_metadb_wal_async_group_commit_window_us() -> u64 {
    // Match metadb's own default; opt-in tuning lives in nvme-detailed.toml.
    1000
}
fn default_metadb_journal_mode() -> MetadbJournalMode {
    // Phase D cutover complete: metadb has no WAL of its own anymore;
    // the LV2 buffer ring + lifecycle journal are the sole durability
    // records. `Wal` / `Shadow` are retained in the enum so existing
    // TOML files parse, but they are force-overridden to `Buffer` by
    // `meta/backend/metadb.rs` since the metadb crate only supports
    // Buffer mode.
    MetadbJournalMode::Buffer
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
    // Reclaim is post-manifest maintenance and must not delay BFG durability.
    // The worker is page-store-only, retryable, and background-priority;
    // lineage GC runs on its separate Db-aware path.
    true
}
fn default_metadb_async_reclaim_max_pages_per_cycle() -> u64 {
    4_096
}
fn default_metadb_async_reclaim_idle_interval_ms() -> u64 {
    50
}
fn default_metadb_lineage_gc_enabled() -> bool {
    // ON. The FreePbas-emitting Lineage GC driver is the sole production
    // trigger for PBA reclaim under rc-neutral writes; without it
    // dead-list chains grow without bound (gc_lineage_freed_blocks=0,
    // allocator slowly exhausts). Independent of async_reclaim_enabled,
    // which gates a different worker (page_store deferred_free) that is OFF
    // for an unrelated refcount-underflow reason.
    true
}
fn default_metadb_lineage_gc_interval_ms() -> u64 {
    1000
}
fn default_metadb_lineage_gc_max_cycles_per_wake() -> usize {
    256
}
fn default_metadb_lineage_gc_drop_dedup_shared() -> bool {
    // ON for onyx. onyx exposes no snapshot/clone, so every rc>0 dead-list
    // record is a dedup-membership PBA reclaimed by the dedup orphan-reclaim
    // path; dropping the redundant record lets lineage GC advance the head and
    // surface the rc==0 exclusive siblings (the reclaim-lag fix). Set to false
    // only if onyx ever gains snapshot/clone.
    true
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
    /// Startup-only CPU set for the direct-IO data-plane API. Accepts Linux
    /// CPU-list syntax such as `"2"` or `"2,4-5"`. In NUMA confine mode,
    /// background roles are kept off this set while normal foreground roles
    /// retain their full CPU pool. Empty uses the normal foreground
    /// (`ThreadRole::Ublk`) placement policy. Changing this value requires a
    /// service restart.
    #[serde(default)]
    pub direct_io_cpus: String,
}

impl ServiceConfig {
    pub fn direct_io_cpu_set(&self) -> OnyxResult<Vec<usize>> {
        crate::numa::parse_cpu_list_checked(&self.direct_io_cpus).map_err(|error| {
            OnyxError::Config(format!(
                "invalid service.direct_io_cpus {:?}: {error}",
                self.direct_io_cpus
            ))
        })
    }
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            socket_path: default_socket_path(),
            direct_io_cpus: String::new(),
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

#[cfg(test)]
mod service_config_tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn service_direct_io_cpus_defaults_to_unset() {
        let config: OnyxConfig = toml::from_str("").unwrap();
        assert!(config.service.direct_io_cpus.is_empty());
    }

    #[test]
    fn parallel_l2p_drain_workers_default_bounded_and_accept_legacy_zero() {
        let default_config: OnyxConfig = toml::from_str("").unwrap();
        assert!(default_config.meta.parallel_l2p_drain_enabled);
        assert_eq!(default_config.meta.parallel_l2p_drain_workers, 4);

        let legacy: OnyxConfig = toml::from_str(
            r#"
                [meta]
                parallel_l2p_drain_workers = 0
            "#,
        )
        .unwrap();
        assert_eq!(legacy.meta.parallel_l2p_drain_workers, 0);
    }

    #[test]
    fn l2p_buffer_default_uses_validated_global_bfg_budget() {
        let config: OnyxConfig = toml::from_str("").unwrap();
        assert_eq!(config.meta.l2p_buffer_soft_entries, 4_000_000);
    }

    #[test]
    fn service_direct_io_cpus_deserializes_linux_cpu_list() {
        let config: OnyxConfig = toml::from_str(
            r#"
                [service]
                direct_io_cpus = "2,4-5"
            "#,
        )
        .unwrap();
        assert_eq!(config.service.direct_io_cpus, "2,4-5");
        assert_eq!(config.service.direct_io_cpu_set().unwrap(), vec![2, 4, 5]);
    }

    #[test]
    fn service_direct_io_cpus_rejects_invalid_cpu_list() {
        let config: OnyxConfig = toml::from_str(
            r#"
                [service]
                direct_io_cpus = "2,bad"
            "#,
        )
        .unwrap();
        assert!(matches!(
            config.service.direct_io_cpu_set(),
            Err(OnyxError::Config(_))
        ));
    }

    #[test]
    fn lv2_prepared_queue_depth_defaults_to_auto_and_accepts_explicit_value() {
        let default_config: OnyxConfig = toml::from_str("").unwrap();
        assert_eq!(default_config.buffer.lv2_prepared_queue_depth_per_lane, 0);

        let configured: OnyxConfig = toml::from_str(
            r#"
                [buffer]
                lv2_prepared_queue_depth_per_lane = 4
            "#,
        )
        .unwrap();
        assert_eq!(configured.buffer.lv2_prepared_queue_depth_per_lane, 4);
    }

    /// The three LV3 batcher knobs have to be sweepable together: the window and
    /// the executor count cap each other (a short window multiplies batch
    /// frequency until producers queue on the fixed executors).
    #[test]
    fn lv3_batch_knobs_default_to_compiled_values_and_parse_together() {
        let default_config: OnyxConfig = toml::from_str("").unwrap();
        assert_eq!(default_config.storage.lv3_batch_coalesce_us, 0);
        assert_eq!(default_config.storage.lv3_batch_target_bytes, 0);
        assert_eq!(default_config.storage.lv3_batch_executors, 0);

        let configured: OnyxConfig = toml::from_str(
            r#"
                [storage]
                lv3_batch_coalesce_us = 200
                lv3_batch_executors = 12
            "#,
        )
        .unwrap();
        assert_eq!(configured.storage.lv3_batch_coalesce_us, 200);
        assert_eq!(configured.storage.lv3_batch_executors, 12);
        // Unset knobs must stay on the compiled default rather than zeroing the
        // byte target, which would make every batch dispatch immediately.
        assert_eq!(configured.storage.lv3_batch_target_bytes, 0);
    }

    #[test]
    fn chunklet_write_scheduler_validation_rejects_partial_or_invalid_shares() {
        let mut config = OnyxConfig::default();
        config.chunklet.write_foreground_active = 1;
        assert!(config.validate().is_err());

        config.chunklet.enabled = true;
        config.chunklet.write_max_active = 4;
        config.chunklet.write_lv3_active = 1;
        config.chunklet.write_meta_active = 0;
        assert!(config.validate().is_err());

        config.chunklet.write_meta_active = 1;
        assert!(config.validate().is_ok());

        config.chunklet.write_foreground_active = 2;
        assert!(config.validate().is_ok());

        config.chunklet.write_foreground_active = 3;
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_load_rejects_invalid_chunklet_write_scheduler() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
                [chunklet]
                enabled = true
                write_max_active = 24
                write_foreground_active = 9
                write_lv3_active = 6
                write_meta_active = 10
            "#
        )
        .unwrap();
        assert!(OnyxConfig::load(file.path()).is_err());
    }

    #[test]
    fn chunklet_pd_write_scheduler_defaults_parse_and_validate_independently() {
        let default_config: OnyxConfig = toml::from_str("").unwrap();
        assert_eq!(default_config.chunklet.pd_write_foreground_workers, 0);
        assert_eq!(default_config.chunklet.pd_write_background_workers, 0);
        assert_eq!(default_config.chunklet.pd_write_max_active_blocks, 0);
        assert_eq!(default_config.chunklet.pd_write_foreground_min_blocks, 0);
        assert_eq!(default_config.chunklet.pd_write_lv3_min_blocks, 0);
        assert_eq!(default_config.chunklet.pd_write_meta_min_blocks, 0);
        assert_eq!(default_config.chunklet.pd_write_maintenance_min_blocks, 0);

        let configured: OnyxConfig = toml::from_str(
            r#"
                [chunklet]
                enabled = true
                pd_write_max_active_blocks = 512
                pd_write_foreground_min_blocks = 32
                pd_write_lv3_min_blocks = 128
                pd_write_meta_min_blocks = 64
                pd_write_maintenance_min_blocks = 8
            "#,
        )
        .unwrap();
        assert_eq!(configured.chunklet.write_max_active, 0);
        assert_eq!(configured.chunklet.pd_write_max_active_blocks, 512);
        assert!(configured.validate().is_ok());
    }

    #[test]
    fn chunklet_pd_write_scheduler_rejects_orphaned_or_excess_minimums() {
        let mut config = OnyxConfig::default();
        config.chunklet.pd_write_lv3_min_blocks = 1;
        assert!(config.validate().is_err());

        config.chunklet.enabled = true;
        config.chunklet.pd_write_max_active_blocks = 8;
        config.chunklet.pd_write_foreground_min_blocks = 1;
        config.chunklet.pd_write_lv3_min_blocks = 3;
        config.chunklet.pd_write_meta_min_blocks = 3;
        config.chunklet.pd_write_maintenance_min_blocks = 1;
        assert!(config.validate().is_ok());

        config.chunklet.pd_write_maintenance_min_blocks = 2;
        assert!(config.validate().is_err());
    }

    #[test]
    fn chunklet_pd_write_scheduler_rejects_any_zero_class_minimum() {
        let mut config = OnyxConfig::default();
        config.chunklet.enabled = true;
        config.chunklet.pd_write_max_active_blocks = 8;
        config.chunklet.pd_write_foreground_min_blocks = 1;
        config.chunklet.pd_write_lv3_min_blocks = 3;
        config.chunklet.pd_write_meta_min_blocks = 3;
        config.chunklet.pd_write_maintenance_min_blocks = 1;
        assert!(config.validate().is_ok());

        config.chunklet.pd_write_foreground_min_blocks = 0;
        assert!(config.validate().is_err());
        config.chunklet.pd_write_foreground_min_blocks = 1;

        config.chunklet.pd_write_lv3_min_blocks = 0;
        assert!(config.validate().is_err());
        config.chunklet.pd_write_lv3_min_blocks = 3;

        config.chunklet.pd_write_meta_min_blocks = 0;
        assert!(config.validate().is_err());
        config.chunklet.pd_write_meta_min_blocks = 3;

        config.chunklet.pd_write_maintenance_min_blocks = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn chunklet_persistent_write_workers_parse_without_pd_admission() {
        let configured: OnyxConfig = toml::from_str(
            r#"
                [chunklet]
                enabled = true
                io_backend = "uring"
                pd_write_foreground_workers = 8
                pd_write_background_workers = 12
            "#,
        )
        .unwrap();

        assert_eq!(configured.chunklet.pd_write_foreground_workers, 8);
        assert_eq!(configured.chunklet.pd_write_background_workers, 12);
        assert_eq!(configured.chunklet.pd_write_max_active_blocks, 0);
        assert!(configured.validate().is_ok());
    }

    #[test]
    fn chunklet_persistent_write_workers_require_a_pair_and_uring() {
        let mut config = OnyxConfig::default();
        config.chunklet.enabled = true;
        config.chunklet.pd_write_foreground_workers = 1;
        assert!(config.validate().is_err());

        config.chunklet.pd_write_background_workers = 1;
        assert!(config.validate().is_ok());

        config.chunklet.io_backend = ChunkletIoBackend::Sync;
        assert!(config.validate().is_err());

        config.chunklet.pd_write_foreground_workers = 0;
        config.chunklet.pd_write_background_workers = 0;
        assert!(config.validate().is_ok());
    }
}
