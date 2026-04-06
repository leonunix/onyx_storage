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
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();

        let handle = thread::Builder::new()
            .name("gc-runner".into())
            .spawn(move || {
                Self::gc_loop(
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

            // Back-pressure: check buffer usage
            let fill_pct = buffer_pool.fill_percentage();
            if fill_pct > config.buffer_usage_max_pct {
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

            // Scan for candidates
            let candidates = match scan_gc_candidates(
                meta,
                config.dead_ratio_threshold,
                config.max_rewrite_per_cycle,
            ) {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(error = %e, "gc: scan failed");
                    continue;
                }
            };

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
                    tracing::debug!("gc: pausing mid-cycle due to buffer pressure");
                    paused = true;
                    break;
                }

                if let Err(e) =
                    rewrite_candidate(candidate, io_engine, buffer_pool, meta, lifecycle)
                {
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
