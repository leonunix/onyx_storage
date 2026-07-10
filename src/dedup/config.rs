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
    /// Orphan dedup-PBA reclaim master switch (default TRUE since 2026-06-04,
    /// after the Stage-5 per-PBA selector was box-validated). When on, the
    /// scanner walks the dedup_index and DEMOTES orphaned entries (no live L2P
    /// reference): delete the index entry (decref the membership rc to 0) and
    /// retire the PBA so the GC confirm scan frees it. Safety rests on the exact
    /// `referenced_extents` Gate-2 scan, not the selector, so a wrong selector
    /// read only costs a re-promote, never data. Requires a refreshed heat map
    /// (`gc.heat_enabled`, also default true). The selector is per-PBA when
    /// `orphan_reclaim_per_pba` (default), else the 1 MiB heat region.
    #[serde(default = "default_orphan_reclaim_enabled")]
    pub orphan_reclaim_enabled: bool,
    /// Max dedup_index entries the orphan-reclaim pass examines per cycle
    /// (default 256). `0` disables the pass.
    #[serde(default = "default_orphan_reclaim_max_per_cycle")]
    pub orphan_reclaim_max_per_cycle: usize,
    /// A dedup entry is demoted only when its PBA region has NOT been bumped by
    /// the heat refresh for more than this many completed sweeps (i.e. it is no
    /// longer hot+fresh). Larger = more conservative (wait longer before
    /// reclaiming a freshly-orphaned region). Default 2. (Region-selector mode
    /// only; ignored when `orphan_reclaim_per_pba` is on.)
    #[serde(default = "default_orphan_reclaim_fresh_max_age")]
    pub orphan_reclaim_fresh_max_age: u32,
    /// Stage-5 per-PBA-precision orphan reclaim (default TRUE since 2026-06-04,
    /// box-validated: reclaimed +46% more orphan PBAs than the region selector
    /// with no churn). When on (and `orphan_reclaim_enabled` + `gc.heat_enabled`,
    /// all default true), the orphan-reclaim pass selects entries with a per-PBA
    /// "referenced" bitmap filled by the heat sweep instead of the coarse 1 MiB
    /// heat region — so orphans *interleaved* with live data (which the region
    /// selector skips because `region_count>0`) get reclaimed too. Safety is
    /// unchanged: the bitmap is only a selector; the GC Gate-2
    /// `referenced_extents` scan still authorizes every free. Allocates a per-PBA
    /// bitmap (~`(clean_sweeps+1) × device_blocks/8` bytes, see startup log) only
    /// when this is on; set false to fall back to the region selector (zero
    /// bitmap memory).
    #[serde(default = "default_orphan_reclaim_per_pba")]
    pub orphan_reclaim_per_pba: bool,
    /// Stage-5: number of consecutive *completed lap-barriers* a PBA must read
    /// unreferenced before it is demoted (the per-PBA analog of
    /// `orphan_reclaim_fresh_max_age`). ≥2 absorbs the in-flight-write race (a
    /// write landing after a referrer-LBA was walked but before the PBA was
    /// orphaned). Clamped to 1..=4 (each barrier retains one bitmap snapshot).
    /// Default 2.
    #[serde(default = "default_orphan_reclaim_clean_sweeps")]
    pub orphan_reclaim_clean_sweeps: u32,
    /// Online cuckoo dedup-index modulus resize: master switch for the
    /// scanner's grow TRIGGER (default false). When on, the scanner asks metadb
    /// to grow the modulus once the live cuckoo load factor crosses
    /// `cuckoo_grow_watermark`. An already-in-progress migration (e.g. resumed
    /// after a crash) is ALWAYS driven to completion regardless of this flag, so
    /// a Growing database never stalls. Kept off by default until box/soak
    /// validation; the customer dedup ratio is unpredictable, so this is what
    /// makes the modulus adapt on-demand instead of being frozen at init.
    #[serde(default)]
    pub cuckoo_auto_resize: bool,
    /// Load factor (`live_entries / (bucket_count × 4)`) at which the scanner
    /// triggers a grow when `cuckoo_auto_resize` is on. 0.5–0.6 is the cuckoo
    /// sweet spot. Default 0.55.
    #[serde(default = "default_cuckoo_grow_watermark")]
    pub cuckoo_grow_watermark: f64,
    /// Multiplier applied to the current bucket count on each grow (default 2).
    #[serde(default = "default_cuckoo_grow_factor")]
    pub cuckoo_grow_factor: u64,
    /// Hard cap on the cuckoo bucket count the auto-grow will target. `0` = no
    /// cap (bounded only by RAM: dense L0 filter + page-table). The unique-4K
    /// hash working set is itself bounded by LV3 capacity / 4K. Default 0.
    #[serde(default)]
    pub cuckoo_max_buckets: u64,
    /// OLD data pages the migration walker copies into NEW per scan cycle while
    /// a resize is in progress. Bounds the `apply_gate.read()` the step holds
    /// (it briefly blocks flush/snapshot writers). `0` disables migration
    /// progress — do NOT set 0 if a resize can be in flight, or it stalls.
    /// Default 256.
    #[serde(default = "default_cuckoo_migrate_max_per_cycle")]
    pub cuckoo_migrate_max_per_cycle: usize,
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
            orphan_reclaim_per_pba: default_orphan_reclaim_per_pba(),
            orphan_reclaim_clean_sweeps: default_orphan_reclaim_clean_sweeps(),
            cuckoo_auto_resize: false,
            cuckoo_grow_watermark: default_cuckoo_grow_watermark(),
            cuckoo_grow_factor: default_cuckoo_grow_factor(),
            cuckoo_max_buckets: 0,
            cuckoo_migrate_max_per_cycle: default_cuckoo_migrate_max_per_cycle(),
        }
    }
}

fn default_cuckoo_grow_watermark() -> f64 {
    0.55
}
fn default_cuckoo_grow_factor() -> u64 {
    2
}
fn default_cuckoo_migrate_max_per_cycle() -> usize {
    256
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
    true
}
fn default_orphan_reclaim_max_per_cycle() -> usize {
    256
}
fn default_orphan_reclaim_fresh_max_age() -> u32 {
    2
}
fn default_orphan_reclaim_per_pba() -> bool {
    true
}
fn default_orphan_reclaim_clean_sweeps() -> u32 {
    2
}
