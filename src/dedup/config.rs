use serde::Deserialize;

pub const DEFAULT_REGISTER_BATCH_MAX_ENTRIES: usize = 1024;
pub const DEFAULT_REGISTER_BATCH_WAIT_US: u64 = 500;
pub const REGISTER_BATCH_HARD_MAX_ENTRIES: usize = 8192;

/// Dedup configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct DedupConfig {
    /// Whether dedup is enabled (default true).
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Number of dedup worker threads (default 2).
    #[serde(default = "default_workers")]
    pub workers: usize,
    /// Skip dedup when buffer usage exceeds this percentage (default 90).
    #[serde(default = "default_buffer_skip_threshold_pct")]
    pub buffer_skip_threshold_pct: u8,
    /// Skip foreground dedup when a buffer shard has more than this many
    /// pending entries. 0 disables the pending-depth gate and keeps dedup
    /// admission controlled only by buffer fill percentage.
    #[serde(default)]
    pub pending_skip_threshold_entries: u64,
    /// Max dedup-index rows a background registration thread commits in one
    /// metadb transaction. Bigger batches reduce fixed WAL/apply overhead but
    /// can create long metadata apply bursts that hurt foreground reads.
    #[serde(default = "default_register_batch_max_entries")]
    pub register_batch_max_entries: usize,
    /// How long a dedup registration thread waits for sibling batches before
    /// committing a partial batch. 0 means commit the first received batch
    /// immediately.
    #[serde(default = "default_register_batch_wait_us")]
    pub register_batch_wait_us: u64,
    /// Background re-dedup scan interval in milliseconds (default 30000).
    #[serde(default = "default_rescan_interval_ms")]
    pub rescan_interval_ms: u64,
    /// Max blocks to re-dedup per scan cycle (default 256).
    #[serde(default = "default_max_rescan_per_cycle")]
    pub max_rescan_per_cycle: usize,
    /// Number of shards in the in-memory candidate cache. Defaults to 8 when
    /// unset (matching the metadb dedup_shards default). Must be a power of
    /// two; the constructor rounds up if not.
    #[serde(default)]
    pub candidate_shards: Option<usize>,
    /// Per-shard capacity of the candidate cache. Defaults to
    /// `CandidateCache::DEFAULT_PER_SHARD_CAPACITY` (1 M / shard) when unset.
    /// Total memory ≈ shards × per_shard × ~32 B.
    #[serde(default)]
    pub candidate_per_shard_capacity: Option<usize>,
    /// Cold-tail rescan: walk live blockmap entries that are not in the
    /// candidate cache yet, hash their content, and warm the cache so the
    /// next duplicate write can verify-and-promote against an existing
    /// fingerprint instead of fresh-writing. Set 0 to disable.
    #[serde(default = "default_cold_tail_max_per_cycle")]
    pub cold_tail_max_per_cycle: usize,
}

impl Default for DedupConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            workers: default_workers(),
            buffer_skip_threshold_pct: default_buffer_skip_threshold_pct(),
            pending_skip_threshold_entries: 0,
            register_batch_max_entries: default_register_batch_max_entries(),
            register_batch_wait_us: default_register_batch_wait_us(),
            rescan_interval_ms: default_rescan_interval_ms(),
            max_rescan_per_cycle: default_max_rescan_per_cycle(),
            candidate_shards: None,
            candidate_per_shard_capacity: None,
            cold_tail_max_per_cycle: default_cold_tail_max_per_cycle(),
        }
    }
}

fn default_enabled() -> bool {
    true
}
fn default_workers() -> usize {
    2
}
fn default_buffer_skip_threshold_pct() -> u8 {
    90
}
fn default_register_batch_max_entries() -> usize {
    DEFAULT_REGISTER_BATCH_MAX_ENTRIES
}
fn default_register_batch_wait_us() -> u64 {
    DEFAULT_REGISTER_BATCH_WAIT_US
}
fn default_rescan_interval_ms() -> u64 {
    30000
}
fn default_max_rescan_per_cycle() -> usize {
    256
}
fn default_cold_tail_max_per_cycle() -> usize {
    256
}
