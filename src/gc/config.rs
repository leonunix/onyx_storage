use serde::Deserialize;

/// GC configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct GcConfig {
    /// Whether GC is enabled (default true).
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Scan interval in milliseconds (default 5000).
    #[serde(default = "default_scan_interval_ms")]
    pub scan_interval_ms: u64,
    /// Dead ratio threshold to trigger repack (default 0.25).
    #[serde(default = "default_dead_ratio_threshold")]
    pub dead_ratio_threshold: f64,
    /// Skip GC if buffer usage exceeds this percentage (default 80).
    #[serde(default = "default_buffer_usage_max_pct")]
    pub buffer_usage_max_pct: u8,
    /// Resume GC when buffer usage drops below this percentage (default 50).
    #[serde(default = "default_buffer_usage_resume_pct")]
    pub buffer_usage_resume_pct: u8,
    /// Max candidates to rewrite per scan cycle (default 64).
    #[serde(default = "default_max_rewrite_per_cycle")]
    pub max_rewrite_per_cycle: usize,
    /// Reclaim grace: a retired extent is not freed until it has been seen in
    /// the retired set for at least this many seconds (default 300 = 5 min).
    /// This guarantees the settle window the retire→reclaim path otherwise gets
    /// only incidentally from the GC cycle cadence: a reference committed to the
    /// LV2 log but whose metadb L2P apply is still in flight (or mid-TXG-fold)
    /// is transiently invisible to the reclaim reverify; waiting the grace lets
    /// it land before the free decision, closing the premature-free race that
    /// corrupted reads. `0` disables the grace (incidental cycle delay only).
    #[serde(default = "default_reclaim_grace_secs")]
    pub reclaim_grace_secs: u64,

    // --- Adaptive reclaim heat map (Stage A: observe-only) ---
    /// Master switch for the background PBA heat-map refresh (default true).
    /// Observe-only in Stage A: builds the map but changes no reclaim
    /// behaviour. Set false to skip the refresh and allocate no bucket array.
    #[serde(default = "default_heat_enabled")]
    pub heat_enabled: bool,
    /// Blocks per heat bucket (default 256 = 1 MiB at 4 KiB blocks). Rounded
    /// up to the next power of two. Sizes the array: `total_blocks / this * 8 B`
    /// (~176 MiB for a 21 TiB device at 256). Read once when the map is built.
    #[serde(default = "default_heat_bucket_size_blocks")]
    pub heat_bucket_size_blocks: u64,
    /// Live blockmap entries the background refresh walks per GC cycle
    /// (default 1_000_000). `0` disables the refresh. Bounds the per-cycle
    /// cost and, with the volume set's total LBA count, the worst-case
    /// region-revisit interval (the staleness floor).
    #[serde(default = "default_heat_refresh_max_lbas_per_cycle")]
    pub heat_refresh_max_lbas_per_cycle: u64,

    // --- Stage-B: reclaim consumes the heat map (behavior change) ---
    /// Master switch for reclaim *consuming* the heat map as a prior to defer
    /// the confirm scan of retired extents whose region still looks hot
    /// (default FALSE — Stage B is a behavior change, A/B-prove before flip).
    /// Requires `heat_enabled` (a real, refreshed map) to have any effect.
    #[serde(default = "default_heat_reclaim_enabled")]
    pub heat_reclaim_enabled: bool,
    /// Max region age (completed sweeps since last counted) still considered
    /// "fresh" enough to defer a retired extent on (default 1 = only this/last
    /// sweep). A region older than this is confirmed-by-scan, so a mass-discard
    /// that stops bumping a region drains it within a sweep or two.
    #[serde(default = "default_heat_fresh_max_age")]
    pub heat_fresh_max_age: u32,
    /// Every Nth GC cycle, reclaim force-confirms ALL retired survivors
    /// regardless of heat — a belt-and-suspenders periodic full check so no
    /// deferred extent is starved (default 64; 0 disables the periodic pass).
    #[serde(default = "default_heat_force_confirm_interval_cycles")]
    pub heat_force_confirm_interval_cycles: u64,
    /// Reserved for Stage-B2 adaptive refresh cadence (refresh scans hot
    /// regions less often): the hard revisit floor that forces a rescan of a
    /// region not counted in this many sweeps. Unused while the refresh is
    /// uniform (every region is counted every sweep, so age never grows for a
    /// live region) — kept so the knob is stable across the B→B2 step.
    #[serde(default = "default_heat_staleness_floor_sweeps")]
    pub heat_staleness_floor_sweeps: u32,

    // --- Stage-B yield gate (self-correct the "hot ⇒ defer" heuristic) ---
    /// If the recent confirm-scan reclaim YIELD (reclaimed extents / scanned
    /// extents, EMA) is ≥ this percent, STOP deferring hot regions this window:
    /// a productive scan means the "hot region ⇒ extent still referenced ⇒ scan
    /// wasted" premise is FALSE for this workload (e.g. unique/discard churn,
    /// where retired extents are genuinely dead), so deferring would only lose
    /// real reclaim. Low yield (e.g. dedup-heavy, where retired rc==0 extents
    /// are often still referenced via an un-drained re-share) keeps deferring —
    /// that is where skipping the scan actually pays. Default 25.
    #[serde(default = "default_heat_defer_yield_suppress_pct")]
    pub heat_defer_yield_suppress_pct: u8,
    /// Every Nth cycle, confirm-all to (re)measure the confirm-scan yield even
    /// while deferring — bounds the cold-start / workload-change blind-defer
    /// window to this many cycles (default 8; 0 disables periodic recalibration,
    /// leaving only the force-confirm pass to sample yield). Distinct from
    /// `heat_force_confirm_interval_cycles` (anti-starvation, coarser).
    #[serde(default = "default_heat_defer_recalibrate_interval_cycles")]
    pub heat_defer_recalibrate_interval_cycles: u64,
    /// If allocator free-space ≤ this percent, STOP deferring (confirm-all) so
    /// reclaim is not delayed under space pressure — the retired-reclaim analog
    /// of the rewrite-GC `dynamic_threshold`. Default 10; 0 disables the gate.
    #[serde(default = "default_heat_defer_min_free_pct")]
    pub heat_defer_min_free_pct: u8,

    // --- Stage-B2: adaptive (per-volume) refresh budget ---
    /// Master switch for biasing the heat-refresh budget toward high-churn
    /// volumes (scan changing volumes more, stable ones less), with
    /// `heat_staleness_floor_sweeps` guaranteeing every volume is fully covered
    /// at least that often regardless of churn. Default FALSE — behavior change,
    /// A/B before flip; no effect with a single volume (degrades to uniform).
    #[serde(default = "default_heat_adaptive_refresh_enabled")]
    pub heat_adaptive_refresh_enabled: bool,

    // --- Stage-4: fold dedup cold-tail warming into the heat-refresh walk ---
    /// Master switch for folding the dedup cold-tail scan into the GC
    /// heat-refresh walk (`docs/adaptive-reclaim-heatmap.md` Stage 4). When ON,
    /// the heat walk (already decoding every non-zero `BlockmapValue`) also
    /// emits cold candidates over a bounded channel; the dedup scanner drains
    /// that channel instead of running its own independent
    /// `scan_blockmap_range` traversal. Eliminates the duplicate live-L2P walk
    /// and lets cold-tail warming ride the heat walk's fast, churn-weighted
    /// coverage at unchanged LV3 read IO. Default FALSE — behavior change,
    /// A/B before flip. Requires `heat_enabled` (the walk must exist) AND a
    /// configured `ReadPool` AND `dedup.enabled`; otherwise the fold is inert
    /// and both subsystems run their legacy paths. Read once at engine open
    /// (toggling needs a restart).
    #[serde(default = "default_heat_fold_cold_tail_enabled")]
    pub heat_fold_cold_tail_enabled: bool,
    /// Bounded capacity of the cold-tail fold channel (default 2048). Producer
    /// `try_send`s and drops on full, so this only bounds how many recent cold
    /// candidates the consumer can choose from; dropping costs dedup ratio,
    /// never correctness.
    #[serde(default = "default_heat_fold_channel_capacity")]
    pub heat_fold_channel_capacity: usize,
    /// Max cold candidates the heat walk pushes per refresh cycle (default
    /// 256). Bounds producer work + channel refill rate. The consumer's own
    /// `dedup.cold_tail_max_per_cycle` still bounds the (unchanged) LV3 read
    /// IO independently. `0` disables pushing (heat walk runs, no fold).
    #[serde(default = "default_heat_fold_push_max_per_cycle")]
    pub heat_fold_push_max_per_cycle: usize,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            scan_interval_ms: default_scan_interval_ms(),
            dead_ratio_threshold: default_dead_ratio_threshold(),
            buffer_usage_max_pct: default_buffer_usage_max_pct(),
            buffer_usage_resume_pct: default_buffer_usage_resume_pct(),
            max_rewrite_per_cycle: default_max_rewrite_per_cycle(),
            reclaim_grace_secs: default_reclaim_grace_secs(),
            heat_enabled: default_heat_enabled(),
            heat_bucket_size_blocks: default_heat_bucket_size_blocks(),
            heat_refresh_max_lbas_per_cycle: default_heat_refresh_max_lbas_per_cycle(),
            heat_reclaim_enabled: default_heat_reclaim_enabled(),
            heat_fresh_max_age: default_heat_fresh_max_age(),
            heat_force_confirm_interval_cycles: default_heat_force_confirm_interval_cycles(),
            heat_staleness_floor_sweeps: default_heat_staleness_floor_sweeps(),
            heat_defer_yield_suppress_pct: default_heat_defer_yield_suppress_pct(),
            heat_defer_recalibrate_interval_cycles: default_heat_defer_recalibrate_interval_cycles(
            ),
            heat_defer_min_free_pct: default_heat_defer_min_free_pct(),
            heat_adaptive_refresh_enabled: default_heat_adaptive_refresh_enabled(),
            heat_fold_cold_tail_enabled: default_heat_fold_cold_tail_enabled(),
            heat_fold_channel_capacity: default_heat_fold_channel_capacity(),
            heat_fold_push_max_per_cycle: default_heat_fold_push_max_per_cycle(),
        }
    }
}

fn default_enabled() -> bool {
    true
}
fn default_scan_interval_ms() -> u64 {
    5000
}
fn default_dead_ratio_threshold() -> f64 {
    0.25
}
fn default_buffer_usage_max_pct() -> u8 {
    80
}
fn default_buffer_usage_resume_pct() -> u8 {
    50
}
fn default_max_rewrite_per_cycle() -> usize {
    64
}
fn default_reclaim_grace_secs() -> u64 {
    300
}
fn default_heat_enabled() -> bool {
    true
}
fn default_heat_bucket_size_blocks() -> u64 {
    256 // 1 MiB at 4 KiB blocks
}
fn default_heat_refresh_max_lbas_per_cycle() -> u64 {
    1_000_000
}
fn default_heat_reclaim_enabled() -> bool {
    false
}
fn default_heat_staleness_floor_sweeps() -> u32 {
    4
}
fn default_heat_fresh_max_age() -> u32 {
    1
}
fn default_heat_force_confirm_interval_cycles() -> u64 {
    64
}
fn default_heat_defer_yield_suppress_pct() -> u8 {
    25
}
fn default_heat_defer_recalibrate_interval_cycles() -> u64 {
    8
}
fn default_heat_defer_min_free_pct() -> u8 {
    10
}
fn default_heat_adaptive_refresh_enabled() -> bool {
    false
}
fn default_heat_fold_cold_tail_enabled() -> bool {
    false
}
fn default_heat_fold_channel_capacity() -> usize {
    2048
}
fn default_heat_fold_push_max_per_cycle() -> usize {
    256
}
