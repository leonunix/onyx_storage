use serde::Deserialize;

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
    /// Background re-dedup scan interval in milliseconds (default 30000).
    #[serde(default = "default_rescan_interval_ms")]
    pub rescan_interval_ms: u64,
    /// Max blocks to re-dedup per scan cycle (default 256).
    #[serde(default = "default_max_rescan_per_cycle")]
    pub max_rescan_per_cycle: usize,
}

impl Default for DedupConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            workers: default_workers(),
            buffer_skip_threshold_pct: default_buffer_skip_threshold_pct(),
            rescan_interval_ms: default_rescan_interval_ms(),
            max_rescan_per_cycle: default_max_rescan_per_cycle(),
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
