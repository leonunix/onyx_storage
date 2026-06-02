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
            heat_enabled: default_heat_enabled(),
            heat_bucket_size_blocks: default_heat_bucket_size_blocks(),
            heat_refresh_max_lbas_per_cycle: default_heat_refresh_max_lbas_per_cycle(),
            heat_reclaim_enabled: default_heat_reclaim_enabled(),
            heat_fresh_max_age: default_heat_fresh_max_age(),
            heat_force_confirm_interval_cycles: default_heat_force_confirm_interval_cycles(),
            heat_staleness_floor_sweeps: default_heat_staleness_floor_sweeps(),
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
