use serde::Deserialize;

/// Hard cap on the per-tx batch size used by `put_dedup_entries` when
/// chunking a large input through metadb. Not user-tunable; the metadb
/// backend chunks any larger input into pieces of this size to keep
/// individual WAL records bounded.
pub const DEDUP_PUT_BATCH_HARD_MAX_ENTRIES: usize = 8192;

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
    /// Background dedup_index scrub: verify cached hash -> PBA hints
    /// and conditionally delete stale entries. Set 0 to disable.
    #[serde(default = "default_index_scrub_max_per_cycle")]
    pub index_scrub_max_per_cycle: usize,
    /// §6 orphan dedup-PBA reclaim (default FALSE — behavior change, A/B before
    /// flip). When on, the scanner walks the dedup_index and DEMOTES entries
    /// whose PBA region the heat map reports cold (no live L2P references):
    /// delete the index entry (decref the membership rc to 0) and retire the
    /// PBA so the GC confirm scan frees it. Safety rests on the exact
    /// `referenced_extents` Gate-2 scan, not the heat map, so a wrong heat read
    /// only costs a re-promote, never data. Requires a refreshed heat map
    /// (`gc.heat_enabled`).
    #[serde(default = "default_orphan_reclaim_enabled")]
    pub orphan_reclaim_enabled: bool,
    /// Max dedup_index entries the orphan-reclaim pass examines per cycle
    /// (default 256). `0` disables the pass.
    #[serde(default = "default_orphan_reclaim_max_per_cycle")]
    pub orphan_reclaim_max_per_cycle: usize,
    /// A dedup entry is demoted only when its PBA region has NOT been bumped by
    /// the heat refresh for more than this many completed sweeps (i.e. it is no
    /// longer hot+fresh). Larger = more conservative (wait longer before
    /// reclaiming a freshly-orphaned region). Default 2.
    #[serde(default = "default_orphan_reclaim_fresh_max_age")]
    pub orphan_reclaim_fresh_max_age: u32,
}

impl Default for DedupConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            workers: default_workers(),
            buffer_skip_threshold_pct: default_buffer_skip_threshold_pct(),
            pending_skip_threshold_entries: 0,
            rescan_interval_ms: default_rescan_interval_ms(),
            max_rescan_per_cycle: default_max_rescan_per_cycle(),
            candidate_shards: None,
            candidate_per_shard_capacity: None,
            cold_tail_max_per_cycle: default_cold_tail_max_per_cycle(),
            index_scrub_max_per_cycle: default_index_scrub_max_per_cycle(),
            orphan_reclaim_enabled: default_orphan_reclaim_enabled(),
            orphan_reclaim_max_per_cycle: default_orphan_reclaim_max_per_cycle(),
            orphan_reclaim_fresh_max_age: default_orphan_reclaim_fresh_max_age(),
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
fn default_rescan_interval_ms() -> u64 {
    30000
}
fn default_max_rescan_per_cycle() -> usize {
    256
}
fn default_cold_tail_max_per_cycle() -> usize {
    256
}
fn default_index_scrub_max_per_cycle() -> usize {
    64
}
fn default_orphan_reclaim_enabled() -> bool {
    false
}
fn default_orphan_reclaim_max_per_cycle() -> usize {
    256
}
fn default_orphan_reclaim_fresh_max_age() -> u32 {
    2
}
