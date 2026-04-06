use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::buffer::pool::WriteBufferPool;
use crate::gc::config::GcConfig;
use crate::gc::rewriter::rewrite_candidate;
use crate::gc::scanner::scan_gc_candidates;
use crate::io::engine::IoEngine;
use crate::lifecycle::VolumeLifecycleManager;
use crate::meta::store::MetaStore;
use crate::metrics::EngineMetrics;

/// Background GC runner thread.
pub struct GcRunner {
    running: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl GcRunner {
    pub fn start(
        meta: Arc<MetaStore>,
        io_engine: Arc<IoEngine>,
        buffer_pool: Arc<WriteBufferPool>,
        lifecycle: Arc<VolumeLifecycleManager>,
        config: GcConfig,
    ) -> Self {
        Self::start_with_metrics(
            Arc::new(EngineMetrics::default()),
            meta,
            io_engine,
            buffer_pool,
            lifecycle,
            config,
        )
    }

    pub fn start_with_metrics(
        metrics: Arc<EngineMetrics>,
        meta: Arc<MetaStore>,
        io_engine: Arc<IoEngine>,
        buffer_pool: Arc<WriteBufferPool>,
        lifecycle: Arc<VolumeLifecycleManager>,
        config: GcConfig,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();

        let handle = thread::Builder::new()
            .name("gc-runner".into())
            .spawn(move || {
                Self::gc_loop(
                    &metrics,
                    &meta,
                    &io_engine,
                    &buffer_pool,
                    &lifecycle,
                    &config,
                    &running_clone,
                );
            })
            .expect("failed to spawn gc runner thread");

        Self {
            running,
            handle: Some(handle),
        }
    }

    fn gc_loop(
        metrics: &EngineMetrics,
        meta: &MetaStore,
        io_engine: &IoEngine,
        buffer_pool: &WriteBufferPool,
        lifecycle: &VolumeLifecycleManager,
        config: &GcConfig,
        running: &AtomicBool,
    ) {
        let mut paused = false;

        while running.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(config.scan_interval_ms));

            if !running.load(Ordering::Relaxed) {
                break;
            }

            metrics.gc_cycles.fetch_add(1, Ordering::Relaxed);

            // Back-pressure: check buffer usage
            let fill_pct = buffer_pool.fill_percentage();
            if fill_pct > config.buffer_usage_max_pct {
                metrics.gc_paused_cycles.fetch_add(1, Ordering::Relaxed);
                if !paused {
                    tracing::debug!(
                        fill_pct,
                        max = config.buffer_usage_max_pct,
                        "gc: pausing due to high buffer usage"
                    );
                    paused = true;
                }
                continue;
            }
            if paused && fill_pct <= config.buffer_usage_resume_pct {
                tracing::debug!(
                    fill_pct,
                    resume = config.buffer_usage_resume_pct,
                    "gc: resuming"
                );
                paused = false;
            }
            if paused {
                continue;
            }

            // Scan for GC rewrite candidates
            let candidates = match scan_gc_candidates(
                meta,
                config.dead_ratio_threshold,
                config.max_rewrite_per_cycle,
            ) {
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

                // Re-check back-pressure before each candidate
                if buffer_pool.fill_percentage() > config.buffer_usage_max_pct {
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
