use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arc_swap::ArcSwap;

use crate::buffer::pool::WriteBufferPool;
use crate::gc::config::GcConfig;
use crate::gc::rewriter::rewrite_candidate;
use crate::gc::scanner::scan_gc_candidates;
use crate::io::engine::IoEngine;
use crate::lifecycle::VolumeLifecycleManager;
use crate::meta::store::MetaStore;
use crate::metrics::EngineMetrics;
use crate::space::allocator::SpaceAllocator;

/// Background GC runner thread.
pub struct GcRunner {
    running: Arc<AtomicBool>,
    config: Arc<ArcSwap<GcConfig>>,
    handle: Option<JoinHandle<()>>,
}

impl GcRunner {
    pub fn start(
        meta: Arc<MetaStore>,
        io_engine: Arc<IoEngine>,
        buffer_pool: Arc<WriteBufferPool>,
        lifecycle: Arc<VolumeLifecycleManager>,
        allocator: Arc<SpaceAllocator>,
        config: GcConfig,
    ) -> Self {
        Self::start_with_metrics(
            Arc::new(EngineMetrics::default()),
            meta,
            io_engine,
            buffer_pool,
            lifecycle,
            allocator,
            config,
        )
    }

    pub fn start_with_metrics(
        metrics: Arc<EngineMetrics>,
        meta: Arc<MetaStore>,
        io_engine: Arc<IoEngine>,
        buffer_pool: Arc<WriteBufferPool>,
        lifecycle: Arc<VolumeLifecycleManager>,
        allocator: Arc<SpaceAllocator>,
        config: GcConfig,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();
        let config = Arc::new(ArcSwap::from_pointee(config));
        let config_clone = config.clone();

        let handle = thread::Builder::new()
            .name("gc-runner".into())
            .spawn(move || {
                Self::gc_loop(
                    &metrics,
                    &meta,
                    &io_engine,
                    &buffer_pool,
                    &lifecycle,
                    &allocator,
                    &config_clone,
                    &running_clone,
                );
            })
            .expect("failed to spawn gc runner thread");

        Self {
            running,
            config,
            handle: Some(handle),
        }
    }

    /// Hot-reload GC configuration.
    pub fn update_config(&self, new_config: GcConfig) {
        tracing::info!("gc: config updated");
        self.config.store(Arc::new(new_config));
    }

    /// Compute dynamic dead_ratio_threshold based on space pressure.
    ///
    /// When space is plentiful, only reclaim heavily fragmented slots.
    /// As space gets tighter, lower the threshold to reclaim more aggressively.
    fn dynamic_threshold(cfg: &GcConfig, allocator: &SpaceAllocator) -> f64 {
        let total = allocator.total_block_count();
        if total == 0 {
            return cfg.dead_ratio_threshold;
        }
        let free_pct = (allocator.free_block_count() * 100) / total;

        if free_pct > 50 {
            0.70  // Plentiful — only GC very dead slots
        } else if free_pct > 30 {
            0.50  // Moderate pressure
        } else if free_pct > 10 {
            0.30  // Getting tight
        } else {
            cfg.dead_ratio_threshold // Critical — use configured minimum (default 0.25)
        }
    }

    fn gc_loop(
        metrics: &EngineMetrics,
        meta: &MetaStore,
        io_engine: &IoEngine,
        buffer_pool: &WriteBufferPool,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        config: &ArcSwap<GcConfig>,
        running: &AtomicBool,
    ) {
        let mut paused = false;

        while running.load(Ordering::Relaxed) {
            let cfg = config.load();
            thread::sleep(Duration::from_millis(cfg.scan_interval_ms));

            if !running.load(Ordering::Relaxed) {
                break;
            }

            metrics.gc_cycles.fetch_add(1, Ordering::Relaxed);

            // Back-pressure: check buffer usage
            let fill_pct = buffer_pool.fill_percentage();
            if fill_pct > cfg.buffer_usage_max_pct {
                metrics.gc_paused_cycles.fetch_add(1, Ordering::Relaxed);
                if !paused {
                    tracing::debug!(
                        fill_pct,
                        max = cfg.buffer_usage_max_pct,
                        "gc: pausing due to high buffer usage"
                    );
                    paused = true;
                }
                continue;
            }
            if paused && fill_pct <= cfg.buffer_usage_resume_pct {
                tracing::debug!(
                    fill_pct,
                    resume = cfg.buffer_usage_resume_pct,
                    "gc: resuming"
                );
                paused = false;
            }
            if paused {
                continue;
            }

            // Smart GC: dynamic dead_ratio_threshold based on space pressure.
            // More aggressive reclamation when space is tight.
            let threshold = Self::dynamic_threshold(&cfg, allocator);

            // Scan for GC rewrite candidates
            let candidates =
                match scan_gc_candidates(meta, threshold, cfg.max_rewrite_per_cycle)
                {
                    Ok(c) => c,
                    Err(e) => {
                        metrics.gc_errors.fetch_add(1, Ordering::Relaxed);
                        tracing::error!(error = %e, "gc: scan failed");
                        continue;
                    }
                };

            metrics
                .gc_candidates_found
                .fetch_add(candidates.len() as u64, Ordering::Relaxed);

            if candidates.is_empty() {
                continue;
            }

            tracing::debug!(
                candidates = candidates.len(),
                "gc: found candidates for reclamation"
            );

            for candidate in &candidates {
                if !running.load(Ordering::Relaxed) {
                    break;
                }

                // Re-check back-pressure before each candidate (re-load config for latest thresholds)
                let cfg = config.load();
                if buffer_pool.fill_percentage() > cfg.buffer_usage_max_pct {
                    metrics.gc_paused_cycles.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!("gc: pausing mid-cycle due to buffer pressure");
                    paused = true;
                    break;
                }

                metrics.gc_rewrite_attempts.fetch_add(1, Ordering::Relaxed);

                match rewrite_candidate(candidate, io_engine, buffer_pool, meta, lifecycle) {
                    Ok(rewritten) => {
                        metrics
                            .gc_blocks_rewritten
                            .fetch_add(rewritten as u64, Ordering::Relaxed);
                    }
                    Err(e) => {
                        metrics.gc_errors.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            pba = candidate.pba.0,
                            vol = %candidate.vol_id,
                            error = %e,
                            "gc: failed to rewrite candidate"
                        );
                    }
                }
            }
        }
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for GcRunner {
    fn drop(&mut self) {
        self.stop();
    }
}
