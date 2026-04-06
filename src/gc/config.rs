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
