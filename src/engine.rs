use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::buffer::flush::BufferFlusher;
use crate::buffer::pool::{BufferRuntimeLimits, ThrottleSettings, WriteBufferPool};
use crate::chunklet_watchdog::{ChunkletWatchdog, WatchdogConfig};
use crate::config::{IoBackend as IoBackendConfig, OnyxConfig, StorageConfig};
use crate::dedup::scanner::DedupScanner;
use crate::error::{OnyxError, OnyxResult};
use crate::gc::runner::GcRunner;
use crate::gc::{HeatMap, RefBitmap};
use crate::io::block_backend::BlockBackend;
use crate::io::device::RawDevice;
use crate::io::engine::IoEngine;
use crate::io::read_pool::ReadPool;
use crate::io::superblock::{self, HeartbeatWriter};
use crate::io::uring::IoUringSession;
use crate::lifecycle::VolumeLifecycleManager;
use crate::meta::store::{MetaStore, SnapshotInfo};
use crate::metrics::{EngineMetrics, EngineMetricsSnapshot, EngineStatusSnapshot};
use crate::space::allocator::SpaceAllocator;
use crate::types::{CompressionAlgo, Pba, VolumeConfig, VolumeId};
use crate::volume::OnyxVolume;
use crate::zone::manager::ZoneManager;

mod durability;
mod lineage;

use durability::DurabilityWatermarkHandle;
use lineage::LineageFreedPbaDrainHandle;

/// TTL for the per-volume usage cache. Usage is cold capacity data derived from
/// an O(live-entries) L2P scan, so it is recomputed at most once per TTL per
/// volume rather than on every request.
const USAGE_CACHE_TTL_SECS: u64 = 60;

/// A per-handle "alive" flag. Set to false when the volume is deleted.
/// Each OnyxVolume holds its own Arc to this flag. The engine keeps Weak
/// references so it can invalidate all outstanding handles on delete.
pub type VolumeAliveFlag = Arc<AtomicBool>;

type LiveHandleRegistry = Vec<(String, Weak<AtomicBool>)>;

fn register_live_handle(handles: &mut LiveHandleRegistry, name: &str, alive: &VolumeAliveFlag) {
    handles.retain(|(_, flag)| flag.strong_count() > 0);
    handles.push((name.to_string(), Arc::downgrade(alive)));
}

fn invalidate_live_handles_in(handles: &mut LiveHandleRegistry, name: &str) {
    handles.retain(|(vol_name, weak)| {
        let Some(flag) = weak.upgrade() else {
            return false;
        };
        if vol_name == name {
            flag.store(false, Ordering::Release);
        }
        true
    });
}

/// Top-level storage engine handle (librbd-style).
///
/// Owns all shared components. Use `open_volume()` to get per-volume IO handles.
/// Thread-safe: multiple threads can call methods concurrently.
pub struct OnyxEngine {
    meta: Arc<MetaStore>,
    #[allow(dead_code)]
    io_engine: Option<Arc<IoEngine>>,
    #[allow(dead_code)]
    allocator: Option<Arc<SpaceAllocator>>,
    #[allow(dead_code)]
    buffer_pool: Option<Arc<WriteBufferPool>>,
    flusher: Mutex<Option<BufferFlusher>>,
    gc_runner: Mutex<Option<GcRunner>>,
    dedup_scanner: Mutex<Option<DedupScanner>>,
    /// Phase 4d: background PD-health watchdog. `Some` only in full mode with a
    /// chunklet backend and `[chunklet].watchdog_enabled`. Probes each live PD
    /// and auto-marks unresponsive ones Failed (and optionally auto-rebuilds).
    chunklet_watchdog: Mutex<Option<ChunkletWatchdog>>,
    /// Inline-degrade fast-isolation reactor. `Some` whenever a chunklet backend
    /// is live (independent of `watchdog_enabled`), because inline-degrade
    /// correctness depends on absorbed-write suspects being isolated promptly.
    chunklet_isolation: Mutex<Option<crate::chunklet_isolation::ChunkletIsolationReactor>>,
    heartbeat_writer: Mutex<Option<HeartbeatWriter>>,
    /// Background thread that periodically syncs metadata, then advances
    /// the buffer pool's `durable_seq` watermark so ring reclaim can safely
    /// proceed. Keeps the hot path at `WriteOptions::sync = false`.
    durability_watermark: Mutex<Option<DurabilityWatermarkHandle>>,
    /// [[no-refcount-hot-path-design]] Rc-neutral path: drains
    /// `WalOp::FreePbas` outcomes from metadb's Lineage GC into the
    /// allocator's free list. Started in full mode, `None` in
    /// meta-only mode (no allocator to feed).
    lineage_drain: Mutex<Option<LineageFreedPbaDrainHandle>>,
    #[allow(dead_code)]
    read_pool: Option<Arc<ReadPool>>,
    zone_manager: Option<Arc<ZoneManager>>,
    /// Live volume handles: (vol_name, alive_flag).
    /// delete_volume sets all matching flags to false.
    /// Entries with dropped handles are cleaned up lazily on subsequent
    /// open_volume/delete_volume calls.
    live_handles: Mutex<LiveHandleRegistry>,
    lifecycle: Arc<VolumeLifecycleManager>,
    metrics: Arc<EngineMetrics>,
    /// Cold-data cache of per-volume capacity usage, keyed by volume name.
    /// Recomputed lazily on read when older than [`USAGE_CACHE_TTL_SECS`].
    usage_cache: dashmap::DashMap<String, crate::meta::store::VolumeUsage>,
    /// Adaptive reclaim heat map (observe-only, Stage A). Shared with the GC
    /// runner (writer) and read by the status path. `None` in standby /
    /// meta-only mode or when the heat refresh is disabled.
    heat: Option<HeatMap>,
    generation_clock: AtomicU64,
    /// Concrete LV3 chunklet backend (full mode + chunklet backend only), the
    /// same `Arc` the IoEngine / ReadPool use. Held so an online `extend_ld` on
    /// LV3 can `swap_ld` a re-opened, larger handle into the shared cell. `None`
    /// on the RawDevice path or in meta-only mode. The meta LD's equivalent
    /// lives inside `MetaStore` (it self-contains its grow via `grow_meta_capacity`).
    lv3_ck_backend: Option<Arc<crate::io::block_backend::ChunkletBackend>>,
    config: OnyxConfig,
    shutdown_done: Mutex<bool>,
}

impl OnyxEngine {
    fn buffer_backpressure_timeout() -> std::time::Duration {
        std::time::Duration::MAX
    }

    /// Auto-detect maximum buffer payload memory: 20% of system memory, capped at 8 GiB.
    fn auto_detect_max_payload_memory() -> u64 {
        let sys_mem = std::fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("MemTotal:"))
                    .and_then(|l| l.split_whitespace().nth(1))
                    .and_then(|kb| kb.parse::<u64>().ok())
                    .map(|kb| kb * 1024) // kB → bytes
            })
            .unwrap_or(8 * 1024 * 1024 * 1024); // fallback 8 GiB
        let twenty_pct = sys_mem / 5;
        let cap = 8u64 * 1024 * 1024 * 1024; // 8 GiB
        let limit = twenty_pct.min(cap);
        tracing::info!(
            system_memory_mb = sys_mem / (1024 * 1024),
            buffer_memory_limit_mb = limit / (1024 * 1024),
            "buffer payload memory limit (20% of system memory, max 8 GiB)"
        );
        limit
    }

    fn current_time_nanos() -> u64 {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        u64::try_from(nanos).unwrap_or(u64::MAX)
    }

    fn build_read_pool(
        config: &OnyxConfig,
        lv3: &Arc<dyn crate::io::block_backend::BlockBackend>,
        metrics: Arc<EngineMetrics>,
    ) -> OnyxResult<Option<Arc<ReadPool>>> {
        let workers = config.storage.read_pool_workers;
        if workers == 0 {
            tracing::info!("read pool disabled (read_pool_workers=0) — LV3 reads run inline");
            return Ok(None);
        }
        // Chunklet: workers share the one LD backend (chunklet owns the cross-PD
        // io_uring); no per-worker fd.
        if config.chunklet.enabled {
            let pool = ReadPool::start_backend(
                workers,
                lv3.clone(),
                crate::types::RESERVED_BLOCKS,
                config.storage.block_size,
                config.storage.use_hugepages,
                metrics,
            )?;
            return Ok(Some(Arc::new(pool)));
        }
        // RawDevice: each worker opens its own fd + io_uring from the device
        // path, so the template handle is only needed for path + base_offset.
        let data_path = config.storage.data_device.as_ref().ok_or_else(|| {
            OnyxError::Config("storage.data_device is required to build the read pool".into())
        })?;
        let device = RawDevice::open(data_path)?;
        let pool = ReadPool::start(
            workers,
            config.storage.uring_sq_entries,
            &device,
            crate::types::RESERVED_BLOCKS,
            config.storage.block_size,
            config.storage.use_hugepages,
            metrics,
        )?;
        drop(device);
        Ok(Some(Arc::new(pool)))
    }

    fn build_heartbeat_writer(
        device: Arc<dyn crate::io::block_backend::BlockBackend>,
        node_id: u64,
        interval: std::time::Duration,
        storage: &StorageConfig,
    ) -> OnyxResult<HeartbeatWriter> {
        // io_uring heartbeat only when the device exposes a fd; a chunklet LD
        // (uring_target == None) falls back to the syscall write+flush path.
        match storage.io_backend {
            IoBackendConfig::Uring if device.uring_target().is_some() => {
                let session = Arc::new(IoUringSession::new(8)?);
                Ok(HeartbeatWriter::start_uring(
                    device, node_id, interval, session,
                ))
            }
            _ => Ok(HeartbeatWriter::start(device, node_id, interval)),
        }
    }

    fn build_io_engine(
        device: Arc<dyn crate::io::block_backend::BlockBackend>,
        storage: &StorageConfig,
        metrics: Arc<EngineMetrics>,
    ) -> OnyxResult<Arc<IoEngine>> {
        use crate::io::engine::IoBackend;
        // A chunklet backend exposes no single fd — it owns io_uring internally,
        // so we always drive it via the Syscall submission mode regardless of
        // storage.io_backend.
        if device.uring_target().is_none() {
            return Ok(Arc::new(
                IoEngine::new_chunklet(device, storage.use_hugepages, metrics)
                    .with_full_stripe_writes(storage.raid_full_stripe_writes),
            ));
        }
        match storage.io_backend {
            IoBackendConfig::Syscall => Ok(Arc::new(IoEngine::new_block(
                device,
                storage.use_hugepages,
                metrics,
                IoBackend::Syscall,
            ))),
            IoBackendConfig::Uring => {
                let session = Arc::new(IoUringSession::new(storage.uring_sq_entries)?);
                tracing::info!(
                    sq_entries = storage.uring_sq_entries,
                    "LV3 IoEngine using io_uring backend"
                );
                Ok(Arc::new(
                    IoEngine::new_block(
                        device,
                        storage.use_hugepages,
                        metrics,
                        IoBackend::Uring(session),
                    )
                    .with_per_shard_write_sessions(storage.lv3_per_shard_write_rings),
                ))
            }
        }
    }

    /// Open the chunklet RAID Pool ONCE per engine startup when
    /// `[chunklet].enabled`. Both LV3 and LV2 derive their LDs from this single
    /// pool (a second in-process `Pool::open` over the same PDs would be a
    /// distinct in-memory pool — two writers to one superblock). Returns `None`
    /// in the non-chunklet deployment so callers keep the `RawDevice` paths.
    fn acquire_chunklet_pool(config: &OnyxConfig) -> OnyxResult<Option<Arc<onyx_chunklet::Pool>>> {
        if config.chunklet.enabled {
            Ok(Some(crate::chunklet_pool::open_pool(&config.chunklet)?))
        } else {
            Ok(None)
        }
    }

    /// Start the Phase-4d PD-health watchdog when a chunklet pool is live and
    /// `[chunklet].watchdog_enabled`. `None` otherwise (RawDevice path,
    /// meta-only mode, or watchdog off). Shared by `open` and
    /// `upgrade_from_meta_only`.
    fn build_chunklet_watchdog(
        chunklet_pool: &Option<Arc<onyx_chunklet::Pool>>,
        config: &OnyxConfig,
    ) -> Option<ChunkletWatchdog> {
        if !config.chunklet.watchdog_enabled {
            return None;
        }
        let pool = chunklet_pool.as_ref()?.clone();
        Some(ChunkletWatchdog::start(
            pool,
            WatchdogConfig {
                interval: std::time::Duration::from_secs(
                    config.chunklet.watchdog_interval_secs.max(1),
                ),
                fail_threshold: config.chunklet.watchdog_fail_threshold.max(1),
                auto_failover: config.chunklet.auto_failover,
                auto_reintegrate: config.chunklet.auto_reintegrate,
                auto_rebalance: config.chunklet.auto_rebalance,
                rebalance_target_skew_pct: config.chunklet.rebalance_target_skew_pct,
                rebalance_max_moves: config.chunklet.rebalance_max_moves_per_cycle,
                device_glob: config.chunklet.device_glob.clone(),
            },
        ))
    }

    /// Start the inline-degrade fast-isolation reactor whenever a chunklet pool
    /// is live. Unlike the watchdog this is NOT gated on `watchdog_enabled`:
    /// chunklet's RAID writes now ride through a member EIO on surviving
    /// redundancy, and that ride-through is only safe if the failed member is
    /// isolated promptly (epoch bump → degraded reopen). The reactor is the
    /// consumer of `Pool::suspect_events` that performs that isolation.
    /// `None` on the RawDevice / meta-only path (no pool to react to).
    fn build_chunklet_isolation(
        chunklet_pool: &Option<Arc<onyx_chunklet::Pool>>,
        config: &OnyxConfig,
    ) -> Option<crate::chunklet_isolation::ChunkletIsolationReactor> {
        let pool = chunklet_pool.as_ref()?.clone();
        Some(crate::chunklet_isolation::ChunkletIsolationReactor::start(
            pool,
            config.chunklet.auto_failover,
        ))
    }

    /// `meta.backend = "chunklet"` requires the chunklet pool to exist.
    fn validate_meta_backend(config: &OnyxConfig) -> OnyxResult<()> {
        if config.meta.backend == crate::config::MetaBackendKind::Chunklet
            && !config.chunklet.enabled
        {
            return Err(OnyxError::Config(
                "meta.backend = \"chunklet\" requires [chunklet].enabled = true".into(),
            ));
        }
        Ok(())
    }

    /// Open the metadb store on whichever backend `meta.backend` selects. The
    /// chunklet path resolves the meta-role LD from the shared `chunklet_pool`
    /// (so the pool is opened exactly once for meta + LV3 + LV2) and keeps that
    /// pool alive inside the backend.
    fn acquire_meta_store(
        config: &OnyxConfig,
        chunklet_pool: &Option<Arc<onyx_chunklet::Pool>>,
    ) -> OnyxResult<MetaStore> {
        match config.meta.backend {
            crate::config::MetaBackendKind::Chunklet => {
                let pool = chunklet_pool.as_ref().ok_or_else(|| {
                    OnyxError::Config("meta.backend=chunklet but pool not opened (internal)".into())
                })?;
                // Concrete `Arc<ChunkletBackend>` (not upcast): `MetaStore` keeps
                // it in its grow handle so an online meta-LD `extend_ld` can
                // `swap_ld` it in place.
                let meta_backend = crate::chunklet_pool::role_backend_from_pool(
                    pool,
                    &config.chunklet,
                    crate::chunklet_pool::LdRoleSel::Meta,
                )?;
                tracing::info!(
                    capacity_bytes = meta_backend.size(),
                    "metadb on chunklet meta RAID LD"
                );
                MetaStore::open_on_meta_ld(&config.meta, meta_backend, pool.clone())
            }
            crate::config::MetaBackendKind::File => MetaStore::open(&config.meta),
        }
    }

    /// Acquire the LV3 device backend for both full-mode startup paths. With
    /// `[chunklet].enabled`, this resolves the LV3 LD from the shared pool (the
    /// returned `ChunkletBackend` keeps the Pool alive); otherwise it opens the
    /// single `storage.data_device` as a `RawDevice`. Either way the result is a
    /// `BlockBackend` the rest of startup treats uniformly.
    /// Returns the LV3 device backend plus, on the chunklet path, the concrete
    /// `Arc<ChunkletBackend>` (a clone of the same instance the IoEngine/ReadPool
    /// use) so an online `extend_ld` can `swap_ld` it. `None` concrete on the
    /// RawDevice path.
    fn acquire_lv3_device(
        config: &OnyxConfig,
        chunklet_pool: &Option<Arc<onyx_chunklet::Pool>>,
    ) -> OnyxResult<(
        Arc<dyn crate::io::block_backend::BlockBackend>,
        Option<Arc<crate::io::block_backend::ChunkletBackend>>,
    )> {
        if config.chunklet.enabled {
            let pool = chunklet_pool.as_ref().ok_or_else(|| {
                OnyxError::Config("chunklet.enabled but pool not opened (internal)".into())
            })?;
            let backend = crate::chunklet_pool::role_backend_from_pool(
                pool,
                &config.chunklet,
                crate::chunklet_pool::LdRoleSel::Lv3,
            )?;
            let device: Arc<dyn crate::io::block_backend::BlockBackend> = backend.clone();
            tracing::info!(capacity_bytes = device.size(), "LV3 on chunklet RAID LD");
            Ok((device, Some(backend)))
        } else {
            let data_path = config.storage.data_device.as_ref().ok_or_else(|| {
                OnyxError::Config("storage.data_device is required for full mode".into())
            })?;
            Ok((Arc::new(RawDevice::open(data_path)?), None))
        }
    }

    fn seed_generation_clock(meta: &MetaStore) -> OnyxResult<u64> {
        let max_existing = meta
            .list_volumes()?
            .into_iter()
            .map(|vol| vol.created_at)
            .max()
            .unwrap_or(0);
        Ok(Self::current_time_nanos().max(max_existing))
    }

    fn next_volume_generation(&self) -> u64 {
        let mut candidate = Self::current_time_nanos();
        loop {
            let observed = self.generation_clock.load(Ordering::Relaxed);
            if candidate <= observed {
                candidate = observed.saturating_add(1);
            }
            match self.generation_clock.compare_exchange(
                observed,
                candidate,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return candidate,
                Err(new_observed) => {
                    if candidate <= new_observed {
                        candidate = new_observed.saturating_add(1);
                    }
                }
            }
        }
    }

    /// Open buffer pool, handling shard count migration at startup if needed.
    fn open_buffer_pool(
        config: &OnyxConfig,
        chunklet_pool: &Option<Arc<onyx_chunklet::Pool>>,
        meta: &Arc<MetaStore>,
        lifecycle: &Arc<VolumeLifecycleManager>,
        allocator: &Arc<SpaceAllocator>,
        io_engine: &Arc<IoEngine>,
        metrics: &Arc<EngineMetrics>,
    ) -> OnyxResult<Arc<WriteBufferPool>> {
        let max_payload_memory = if config.buffer.max_memory_mb > 0 {
            config.buffer.max_memory_mb as u64 * 1024 * 1024
        } else {
            Self::auto_detect_max_payload_memory()
        };
        let buffer_runtime_limits = BufferRuntimeLimits::from_config(
            max_payload_memory,
            config.buffer.staging_queue_entries,
            config.buffer.sync_batch_max_entries,
            config.buffer.sync_batch_max_bytes_mb as usize * 1024 * 1024,
            config.buffer.lv2_sync_pipeline_depth,
            config.buffer.lv2_commit_timeout_pct,
        )
        .with_throttle(ThrottleSettings {
            min_pct: config.buffer.throttle_min_pct,
            max_pct: config.buffer.throttle_max_pct,
            scale_us: config.buffer.throttle_scale_us,
            cap_us: config.buffer.throttle_cap_us,
        });

        // Resolve the LV2 backend + whether onyx drives its own device-level
        // io_uring. A chunklet LD owns its cross-PD io_uring internally, so the
        // sync thread takes the write_many_at + flush path (uring_entries=None);
        // a `RawDevice` keeps the existing io_uring hot path.
        let (device, buffer_uring_entries): (
            Arc<dyn crate::io::block_backend::BlockBackend>,
            Option<u32>,
        ) = if config.chunklet.enabled {
            // LV2 = a chunklet RAID10 LD from the shared pool. It is a fresh
            // fixed-size region, so there is no RawDevice-style shard-count
            // migration probe (a dirty shard-count change surfaces as a
            // buffer-open error — online LV2 reshape is a Phase-4 concern).
            let pool = chunklet_pool.as_ref().ok_or_else(|| {
                OnyxError::Config("chunklet.enabled but pool not opened (internal)".into())
            })?;
            let backend: Arc<dyn crate::io::block_backend::BlockBackend> =
                crate::chunklet_pool::role_backend_from_pool(
                    pool,
                    &config.chunklet,
                    crate::chunklet_pool::LdRoleSel::Lv2,
                )?;
            tracing::info!(capacity_bytes = backend.size(), "LV2 on chunklet RAID LD");
            (backend, None)
        } else {
            let buf_path = config.buffer.device.as_ref().ok_or_else(|| {
                OnyxError::Config("buffer.device is required for full mode".into())
            })?;

            // Detect shard count change and migrate if needed
            let probe_dev = RawDevice::open(buf_path)?;
            let disk_shards = WriteBufferPool::read_disk_shard_count(&probe_dev)?;
            drop(probe_dev);

            if let Some(old_count) = disk_shards {
                if old_count != config.buffer.shards {
                    tracing::info!(
                        old_shards = old_count,
                        new_shards = config.buffer.shards,
                        "shard count changed — attempting online migration"
                    );
                    // Try direct open (auto-reinit if buffer already clean)
                    let try_dev = RawDevice::open(buf_path)?;
                    let direct = WriteBufferPool::open_with_options(
                        try_dev,
                        std::time::Duration::from_micros(config.buffer.group_commit_wait_us),
                        config.buffer.shards,
                        config.engine.zone_size_blocks,
                        Self::buffer_backpressure_timeout(),
                    );
                    match direct {
                        Ok(pool) => drop(pool), // clean, will reopen below
                        Err(_) => {
                            // Drain unflushed entries with old shard layout
                            tracing::info!(
                                old_shards = old_count,
                                "opening buffer with old shard count to drain"
                            );
                            let old_dev = RawDevice::open(buf_path)?;
                            let old_pool = Arc::new(WriteBufferPool::open_with_options(
                                old_dev,
                                std::time::Duration::from_micros(
                                    config.buffer.group_commit_wait_us,
                                ),
                                old_count,
                                config.engine.zone_size_blocks,
                                Self::buffer_backpressure_timeout(),
                            )?);
                            old_pool.attach_metrics(metrics.clone());
                            let old_pending = old_pool.pending_count();
                            if old_pending > 0 {
                                tracing::info!(
                                    count = old_pending,
                                    "draining unflushed entries before shard migration"
                                );
                            }
                            let mut temp_flusher = BufferFlusher::start_with_metrics(
                                old_pool.clone(),
                                meta.clone(),
                                lifecycle.clone(),
                                allocator.clone(),
                                io_engine.clone(),
                                // Drain-and-stop helper: no read pool
                                // available here, drop verify (the flusher
                                // is short-lived and writes through the
                                // existing trust-hash path).
                                None,
                                &config.flush,
                                &config.dedup,
                                metrics.clone(),
                            );
                            temp_flusher.drain_and_stop(&old_pool);
                            drop(old_pool);
                            tracing::info!(
                                new_shards = config.buffer.shards,
                                "buffer drained — reinitializing with new shard layout"
                            );
                        }
                    }
                }
            }

            // Open buffer pool (auto-reinit if drained above, or normal open)
            let buf_dev = RawDevice::open(buf_path)?;
            let uring = match config.storage.io_backend {
                IoBackendConfig::Uring => Some(config.storage.uring_sq_entries),
                IoBackendConfig::Syscall => None,
            };
            (Arc::new(buf_dev), uring)
        };

        let pool = Arc::new(WriteBufferPool::open_with_options_full_and_limits(
            device,
            std::time::Duration::from_micros(config.buffer.group_commit_wait_us),
            config.buffer.shards,
            config.engine.zone_size_blocks,
            Self::buffer_backpressure_timeout(),
            max_payload_memory,
            buffer_uring_entries,
            buffer_runtime_limits,
        )?);
        pool.attach_metrics(metrics.clone());

        // --- Fast recovery: zero per-block IO ---
        // Pending entries are already in pending_entries + lba_index + ready_tx
        // after open(). The flusher will pick them up automatically and handles:
        //   - Same-LBA dedup via coalescer
        //   - vol_created_at generation checks (discards stale entries)
        //   - Idempotent re-flush of already-committed blocks
        // No per-block LV3 reads, no payload clones, no re-append — instant startup.
        let unflushed_count = pool.pending_count();
        if unflushed_count > 0 {
            tracing::info!(
                count = unflushed_count,
                "pending buffer entries will be flushed in background"
            );
        }

        Ok(pool)
    }

    fn invalidate_live_handles(&self, name: &str) {
        let mut handles = self.live_handles.lock().unwrap();
        invalidate_live_handles_in(&mut handles, name);
    }

    #[cfg(target_os = "linux")]
    fn validate_data_buffer_devices_disjoint(config: &OnyxConfig) -> OnyxResult<()> {
        // With chunklet, LV3 is a RAID LD (not storage.data_device) carved from
        // the chunklet pool; LV2 still lives on buffer.device. The data/buffer
        // single-device disjointness check does not apply.
        if config.chunklet.enabled {
            return Ok(());
        }
        use std::collections::HashSet;
        use std::os::unix::fs::{FileTypeExt, MetadataExt};
        use std::path::{Path, PathBuf};

        #[derive(Debug)]
        struct BlockIdentity {
            dev: (u64, u64),
            sysfs: PathBuf,
        }

        fn linux_major(dev: u64) -> u64 {
            ((dev >> 8) & 0xfff) | ((dev >> 32) & !0xfff)
        }

        fn linux_minor(dev: u64) -> u64 {
            (dev & 0xff) | ((dev >> 12) & !0xff)
        }

        fn block_identity(path: &Path) -> OnyxResult<Option<BlockIdentity>> {
            let meta = std::fs::metadata(path).map_err(|e| OnyxError::Device {
                path: path.to_path_buf(),
                reason: e.to_string(),
            })?;
            if !meta.file_type().is_block_device() {
                return Ok(None);
            }
            let major = linux_major(meta.rdev());
            let minor = linux_minor(meta.rdev());
            Ok(Some(BlockIdentity {
                dev: (major, minor),
                sysfs: PathBuf::from(format!("/sys/dev/block/{major}:{minor}")),
            }))
        }

        fn read_sysfs_dev(path: &Path) -> Option<(u64, u64)> {
            let text = std::fs::read_to_string(path.join("dev")).ok()?;
            let (major, minor) = text.trim().split_once(':')?;
            Some((major.parse().ok()?, minor.parse().ok()?))
        }

        fn collect_related(
            sysfs: &Path,
            dir_name: &str,
            out: &mut HashSet<(u64, u64)>,
        ) -> OnyxResult<()> {
            let dir = sysfs.join(dir_name);
            let Ok(entries) = std::fs::read_dir(&dir) else {
                return Ok(());
            };
            for entry in entries {
                let entry = entry.map_err(|e| OnyxError::Config(e.to_string()))?;
                let target = std::fs::canonicalize(entry.path()).unwrap_or_else(|_| entry.path());
                if let Some(dev) = read_sysfs_dev(&target) {
                    if out.insert(dev) {
                        collect_related(&target, "slaves", out)?;
                        collect_related(&target, "holders", out)?;
                    }
                }
            }
            Ok(())
        }

        let Some(data_path) = config.storage.data_device.as_ref() else {
            return Ok(());
        };
        let Some(buffer_path) = config.buffer.device.as_ref() else {
            return Ok(());
        };
        let Some(data) = block_identity(data_path)? else {
            return Ok(());
        };
        let Some(buffer) = block_identity(buffer_path)? else {
            return Ok(());
        };

        if data.dev == buffer.dev {
            return Err(OnyxError::Config(format!(
                "storage.data_device ({}) and buffer.device ({}) resolve to the same block device",
                data_path.display(),
                buffer_path.display()
            )));
        }

        let mut data_related = HashSet::new();
        collect_related(&data.sysfs, "slaves", &mut data_related)?;
        collect_related(&data.sysfs, "holders", &mut data_related)?;

        let mut buffer_related = HashSet::new();
        collect_related(&buffer.sysfs, "slaves", &mut buffer_related)?;
        collect_related(&buffer.sysfs, "holders", &mut buffer_related)?;

        if buffer_related.contains(&data.dev) || data_related.contains(&buffer.dev) {
            return Err(OnyxError::Config(format!(
                "storage.data_device ({}) overlaps buffer.device ({}) through block-device holder/slave topology; LV2 and LV3 must not share physical devices",
                data_path.display(),
                buffer_path.display()
            )));
        }

        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn validate_data_buffer_devices_disjoint(_config: &OnyxConfig) -> OnyxResult<()> {
        Ok(())
    }

    /// Reject configs that enable dedup with no LV3 read pool.
    ///
    /// With `storage.read_pool_workers == 0` the dedup pipeline runs in
    /// trust-hash mode — it skips the LV3 byte-compare that turns an
    /// `xxh3_64` fingerprint match into a *proven* duplicate (see
    /// `BufferFlusher::start_with_metrics` / `stages::dedup`). `xxh3_64`
    /// is not collision-resistant, so a single 64-bit collision would
    /// silently share two unrelated 4 KiB blocks → data corruption. Verify
    /// is correctness, not optimisation, so this combination is refused at
    /// startup rather than silently degraded.
    fn validate_dedup_read_pool(config: &OnyxConfig) -> OnyxResult<()> {
        if config.dedup.enabled && config.storage.read_pool_workers == 0 {
            return Err(OnyxError::Config(
                "dedup.enabled=true requires storage.read_pool_workers > 0: with no read pool the \
                 dedup verify step is skipped (trust-hash mode), and xxh3_64 is not \
                 collision-resistant, so a hash collision would silently share unrelated blocks. \
                 Set storage.read_pool_workers >= 1, or set dedup.enabled = false."
                    .into(),
            ));
        }
        Ok(())
    }

    /// Open the engine with full IO capability (data device + buffer + flusher + zones).
    ///
    /// Compression is per-volume (stored in VolumeConfig metadata), not engine-wide.
    pub fn open(config: &OnyxConfig) -> OnyxResult<Self> {
        Self::validate_data_buffer_devices_disjoint(config)?;
        Self::validate_dedup_read_pool(config)?;
        Self::validate_meta_backend(config)?;

        // 1. Chunklet RAID Pool (opened once; meta + LV3 + LV2 all share it) —
        //    None when [chunklet] is disabled. Opened BEFORE metadb because the
        //    meta LD lives inside this pool when `meta.backend = "chunklet"`.
        let chunklet_pool = Self::acquire_chunklet_pool(config)?;

        // 2. MetaStore — on the meta LD (chunklet backend, from the shared pool)
        //    or the host FS (file backend).
        let meta = Arc::new(Self::acquire_meta_store(config, &chunklet_pool)?);
        let lifecycle = Arc::new(VolumeLifecycleManager::default());
        let metrics = Arc::new(EngineMetrics::default());
        let generation_clock = Self::seed_generation_clock(&meta)?;

        // (no shared deletion state needed — per-handle alive flags are used)

        // 3. LV3 device (RawDevice or chunklet RAID LD) + IO engine
        let (device, lv3_ck_backend) = Self::acquire_lv3_device(config, &chunklet_pool)?;
        let device_size = device.size();

        // 2a. Validate / format LV3 superblock
        let mut superblock = match superblock::read_superblock(device.as_ref())? {
            Some(sb) => {
                if sb.device_size_bytes != device_size {
                    return Err(OnyxError::Config(format!(
                        "LV3 superblock device_size {} != actual {}",
                        sb.device_size_bytes, device_size
                    )));
                }
                tracing::info!(
                    uuid = sb.uuid_string(),
                    version = sb.version,
                    clean_shutdown = sb.is_clean_shutdown(),
                    "LV3 superblock validated"
                );
                sb
            }
            None => {
                // Check if the device is fresh (all zeros in block 0)
                let mut block0 = [0u8; 4096];
                device.read_at(&mut block0, 0)?;
                if block0.iter().all(|&b| b == 0) {
                    tracing::info!("fresh LV3 device — formatting");
                    superblock::format_device(device.as_ref())?
                } else {
                    return Err(OnyxError::Config(
                        "LV3 block 0 has data but invalid superblock (magic/CRC/version failed)"
                            .into(),
                    ));
                }
            }
        };

        // 2b. Recovery branch based on clean/dirty shutdown marker.
        //
        // Refcount durability is owned by metadb's WAL/BFG recovery (the
        // refcount paged-array + per-shard deltas are replayed when
        // `MetaStore::open` brings metadb up). There is no separate
        // blockmap-walking rebuild on the onyx side — historically this
        // branch logged "rebuilding refcount" and called a no-op stub,
        // which was misleading. We now just record that recovery already
        // happened and (cheaply) validate it.
        if superblock.is_clean_shutdown() {
            tracing::info!("clean shutdown marker present");
        } else {
            tracing::info!(
                "dirty startup detected — refcount was restored by metadb WAL/BFG recovery \
                 (no separate onyx-side rebuild)"
            );
            meta.recover_or_validate_refcount()?;
        }

        // 2c. Mark dirty before serving IO. The bit is cleared again only on
        //     graceful shutdown, so an unexpected exit automatically forces a
        //     dirty recovery on the next boot.
        superblock.set_clean_shutdown(false);
        superblock.update_crc();
        superblock::write_superblock(device.as_ref(), &superblock)?;

        let io_engine = Self::build_io_engine(device.clone(), &config.storage, metrics.clone())?;

        // 3. Space allocator
        // The allocator addresses PBAs through the IoEngine's `pba_offset`
        // (= RESERVED_BLOCKS): device block = pba + pba_offset. So it must be
        // built with the io-ADDRESSABLE capacity (raw device minus the reserved
        // offset), not the raw device size — otherwise the top RESERVED_BLOCKS
        // PBAs would translate past the device end and a boundary write (esp. a
        // multi-block full-stripe write) fails with chunklet "IO out of range".
        let allocator = Arc::new(SpaceAllocator::new_with_hazards(
            device_size
                .saturating_sub(crate::types::RESERVED_BLOCKS * crate::types::BLOCK_SIZE as u64),
            config.buffer.shards,
        ));
        // Fixed LV3 RAID geometry → effective-capacity index for stripe-aligned
        // first-fit (set BEFORE rebuild so the rebuilt free list is indexed too;
        // rebuild preserves it).
        allocator.set_stripe_geometry(io_engine.stripe_blocks(), io_engine.stripe_phase());
        allocator.rebuild_from_metadata(&meta)?;

        // 4. Write buffer pool (with shard migration if needed)
        let buffer_pool = Self::open_buffer_pool(
            config,
            &chunklet_pool,
            &meta,
            &lifecycle,
            &allocator,
            &io_engine,
            &metrics,
        )?;

        // LV3 read pool — built BEFORE the flusher so the dedup
        // workers can route candidate-hit / dedup_index-hit verifies
        // through it (`dedup::verify::batched_verify`). Also still
        // shared with ZoneManager for the foreground read path.
        let read_pool = Self::build_read_pool(config, &device, metrics.clone())?;

        // 5. Buffer-as-sole-journal recovery: any ring entries past the
        //    last metadb-applied seq are the only durable record of
        //    those commits. Drive them through a one-shot flusher
        //    before standing up the long-lived one so clients never see
        //    a state that contradicts the post-checkpoint buffer tail.
        //    On a clean shutdown this is a no-op (pending == 0).
        let recovery_stats = crate::buffer::flush::replay_buffer_pending(
            buffer_pool.clone(),
            meta.clone(),
            lifecycle.clone(),
            allocator.clone(),
            io_engine.clone(),
            read_pool.clone(),
            &config.flush,
            &config.dedup,
            metrics.clone(),
            std::time::Duration::from_millis(config.engine.recovery_timeout_ms),
        );
        if recovery_stats.timed_out {
            return Err(OnyxError::Config(format!(
                "buffer replay did not reach quiescence in {} ms (pending {} of {}); raise engine.recovery_timeout_ms or investigate stuck entries",
                config.engine.recovery_timeout_ms,
                recovery_stats.pending_at_exit,
                recovery_stats.pending_at_start,
            )));
        }
        if recovery_stats.pending_at_start > 0 {
            tracing::info!(
                pending_at_start = recovery_stats.pending_at_start,
                elapsed_ms = recovery_stats.elapsed.as_millis() as u64,
                "buffer recovery drained cleanly"
            );
        }

        // 6. Background flusher
        let flusher = BufferFlusher::start_with_metrics(
            buffer_pool.clone(),
            meta.clone(),
            lifecycle.clone(),
            allocator.clone(),
            io_engine.clone(),
            read_pool.clone(),
            &config.flush,
            &config.dedup,
            metrics.clone(),
        );

        // Single unified PBA lifecycle layer (candidate-evict-before-free +
        // retire retry queue + lineage proof free), owned by the flusher and
        // shared with the lineage drain / GC reclaim / dedup scanner / zone
        // discard so they all use one retry queue + `pba_reclaim_stuck` gauge.
        let pba_lifecycle = flusher.pba_lifecycle();

        // 9. Zone manager
        let zone_manager = Arc::new(ZoneManager::new_full(
            config.engine.zone_count,
            config.engine.zone_size_blocks,
            meta.clone(),
            buffer_pool.clone(),
            io_engine.clone(),
            metrics.clone(),
            Some(allocator.clone()),
            Some(pba_lifecycle.clone()),
            read_pool.clone(),
        )?);

        // Stage-4 fold: when enabled, the GC heat walk feeds cold candidates
        // to the dedup scanner over this channel instead of the scanner
        // running its own live-L2P traversal. (None, None) ⇒ legacy paths.
        let (cold_tail_tx, cold_tail_rx) = build_cold_tail_fold_channel(&config, &read_pool);

        // Build the adaptive-reclaim heat map first so BOTH the GC runner (which
        // refreshes it) and the dedup scanner (§6 orphan reclaim reads it as a
        // cold-region selector) share the same map. When disabled, this is a
        // minimal 1-bucket map (allocates nothing; refresh + orphan reclaim gated
        // off). The scanner only gets a usable map when `heat_enabled`.
        let heat = build_heat_map(&config.gc, &allocator);
        let scanner_heat = config.gc.heat_enabled.then(|| heat.clone());
        // Stage-5 per-PBA orphan-reclaim bitmap (None unless per-PBA mode on):
        // GC fills it on the heat sweep, the dedup scanner reads it.
        let ref_bitmap = build_ref_bitmap(&config.gc, &config.dedup, &allocator);

        // 9. Dedup scanner (after flusher; re-processes skipped blocks)
        let dedup_scanner = if config.dedup.enabled {
            Some(DedupScanner::start_with_metrics(
                metrics.clone(),
                meta.clone(),
                io_engine.clone(),
                allocator.clone(),
                lifecycle.clone(),
                buffer_pool.clone(),
                flusher.candidate_cache(),
                read_pool.clone(),
                cold_tail_rx,
                scanner_heat,
                ref_bitmap.clone(),
                pba_lifecycle.clone(),
                config.dedup.clone(),
            ))
        } else {
            None
        };

        // 10. GC runner (after flusher). It always runs the physical
        // retired-PBA reclaimer; `gc.enabled` only gates rewrite scanning.
        let gc_runner = Some(GcRunner::start_with_metrics(
            metrics.clone(),
            meta.clone(),
            io_engine.clone(),
            buffer_pool.clone(),
            lifecycle.clone(),
            allocator.clone(),
            heat.clone(),
            ref_bitmap,
            cold_tail_tx,
            pba_lifecycle.clone(),
            config.gc.clone(),
        ));
        let heat = config.gc.heat_enabled.then_some(heat);

        // 11. Heartbeat writer (after all other subsystems)
        let heartbeat_writer = if config.ha.enabled {
            Some(Self::build_heartbeat_writer(
                device.clone(),
                config.ha.node_id,
                std::time::Duration::from_millis(config.ha.heartbeat_interval_ms),
                &config.storage,
            )?)
        } else {
            None
        };

        tracing::info!("onyx engine opened (full mode)");

        // Start the durability watermark thread now that both meta and
        // buffer_pool are live. It is the sole driver of `durable_seq`
        // and the sole caller of `pool.release_below` — both advance
        // only on a confirmed metadb checkpoint outcome, since the
        // post-WAL buffer ring is the only durable record of commits
        // until then.
        let watermark = DurabilityWatermarkHandle::start(
            meta.clone(),
            buffer_pool.clone(),
            buffer_pool.durable_seq_handle(),
            config.meta.checkpoint_interval(),
            config.meta.flush_dirty_pages_threshold,
            config.meta.checkpoint_ring_fill_pct,
        );
        let lineage_drain = LineageFreedPbaDrainHandle::start(
            meta.clone(),
            pba_lifecycle.clone(),
            std::time::Duration::from_millis(100),
        );

        let chunklet_watchdog = Self::build_chunklet_watchdog(&chunklet_pool, config);
        let chunklet_isolation = Self::build_chunklet_isolation(&chunklet_pool, config);

        Ok(Self {
            meta,
            io_engine: Some(io_engine),
            allocator: Some(allocator),
            buffer_pool: Some(buffer_pool),
            flusher: Mutex::new(Some(flusher)),
            gc_runner: Mutex::new(gc_runner),
            dedup_scanner: Mutex::new(dedup_scanner),
            chunklet_watchdog: Mutex::new(chunklet_watchdog),
            chunklet_isolation: Mutex::new(chunklet_isolation),
            heartbeat_writer: Mutex::new(heartbeat_writer),
            durability_watermark: Mutex::new(Some(watermark)),
            lineage_drain: Mutex::new(Some(lineage_drain)),
            read_pool,
            zone_manager: Some(zone_manager),
            live_handles: Mutex::new(Vec::new()),
            lifecycle,
            metrics,
            heat,
            usage_cache: dashmap::DashMap::new(),
            generation_clock: AtomicU64::new(generation_clock),
            lv3_ck_backend,
            config: config.clone(),
            shutdown_done: Mutex::new(false),
        })
    }

    /// Open engine in metadata-only mode (no data device, no IO).
    ///
    /// Only volume management operations (create/delete/list) are available.
    /// Attempting to open_volume() will fail.
    pub fn open_meta_only(config: &OnyxConfig) -> OnyxResult<Self> {
        Self::validate_meta_backend(config)?;
        // Meta-only mode has no LV3/LV2, so a file-backed metadb needs no pool.
        // A meta-LD-backed metadb must still open the pool (it holds the meta LD);
        // it stays alive inside `meta` and is reused by a later upgrade to active.
        let chunklet_pool = if config.meta.backend == crate::config::MetaBackendKind::Chunklet {
            Self::acquire_chunklet_pool(config)?
        } else {
            None
        };
        let meta = Arc::new(Self::acquire_meta_store(config, &chunklet_pool)?);
        let lifecycle = Arc::new(VolumeLifecycleManager::default());
        let metrics = Arc::new(EngineMetrics::default());
        let generation_clock = Self::seed_generation_clock(&meta)?;

        tracing::info!("onyx engine opened (meta-only mode)");

        Ok(Self {
            meta,
            io_engine: None,
            allocator: None,
            buffer_pool: None,
            flusher: Mutex::new(None),
            gc_runner: Mutex::new(None),
            dedup_scanner: Mutex::new(None),
            chunklet_watchdog: Mutex::new(None),
            chunklet_isolation: Mutex::new(None),
            heartbeat_writer: Mutex::new(None),
            durability_watermark: Mutex::new(None),
            lineage_drain: Mutex::new(None),
            read_pool: None,
            zone_manager: None,
            live_handles: Mutex::new(Vec::new()),
            lifecycle,
            metrics,
            heat: None,
            usage_cache: dashmap::DashMap::new(),
            generation_clock: AtomicU64::new(generation_clock),
            lv3_ck_backend: None,
            config: config.clone(),
            shutdown_done: Mutex::new(false),
        })
    }

    /// Create a new volume.
    pub fn create_volume(
        &self,
        name: &str,
        size_bytes: u64,
        compression: CompressionAlgo,
    ) -> OnyxResult<()> {
        self.lifecycle.with_write_lock(name, || {
            let vol = VolumeConfig {
                id: VolumeId(name.to_string()),
                size_bytes,
                block_size: 4096,
                compression,
                created_at: self.next_volume_generation(),
                zone_count: self.config.engine.zone_count,
            };
            self.meta.put_volume(&vol)?;
            self.metrics
                .volume_create_ops
                .fetch_add(1, Ordering::Relaxed);
            tracing::info!(
                name,
                size_bytes,
                generation = vol.created_at,
                "volume created"
            );
            Ok(())
        })
    }

    /// Delete a volume, purge its buffer entries, and free its physical blocks.
    ///
    /// Steps:
    /// 1. Take the per-volume lifecycle write lock.
    /// 2. Purge pending buffer entries.
    /// 3. Delete metadata (volume config + blockmap + refcounts) atomically.
    /// 4. Return freed PBAs to the in-memory SpaceAllocator.
    /// 5. Wait for old-generation flusher work to retire, then clean orphaned refcounts.
    /// 6. Invalidate existing handles after delete succeeds.
    pub fn delete_volume(&self, name: &str) -> OnyxResult<usize> {
        let deleted = self
            .lifecycle
            .with_write_lock(name, || -> OnyxResult<Option<(usize, u64)>> {
            let vol_id = VolumeId(name.to_string());
            let Some(volume) = self.meta.get_volume(&vol_id)? else {
                tracing::info!(name, "delete_volume: volume not found, nothing to do");
                return Ok(None);
            };
            let deleted_generation = volume.created_at;

            if let Some(pool) = &self.buffer_pool {
                pool.purge_volume(name)?;
            }

            let cleanups = self.meta.delete_volume(&vol_id)?;
            let freed_blocks: usize = cleanups
                .iter()
                .filter(|cleanup| cleanup.pba_freed)
                .map(|cleanup| cleanup.blocks as usize)
                .sum();

            let pba_lifecycle = self
                .flusher
                .lock()
                .unwrap()
                .as_ref()
                .map(|flusher| flusher.pba_lifecycle());
            if let Some(pba_lifecycle) = pba_lifecycle {
                BufferFlusher::cleanup_dead_pbas_batch(&pba_lifecycle, &cleanups, "volume_delete");
            } else if cleanups.iter().any(|cleanup| cleanup.pba_freed) {
                tracing::warn!(
                    name,
                    cleanup_count = cleanups.len(),
                    "delete_volume: meta-only mode cannot return freed PBAs to allocator; dedup_index is left to verified repair/scrub"
                );
            }

            self.invalidate_live_handles(name);
            self.metrics.remove_volume_metrics(name);
            self.usage_cache.remove(name);
            self.metrics
                .volume_delete_ops
                .fetch_add(1, Ordering::Relaxed);

            tracing::info!(
                name,
                generation = deleted_generation,
                freed_extents = cleanups.iter().filter(|cleanup| cleanup.pba_freed).count(),
                freed_blocks,
                "volume deleted"
            );
            Ok(Some((freed_blocks, deleted_generation)))
        })?;

        let Some((freed_blocks, deleted_generation)) = deleted else {
            return Ok(0);
        };

        if let Some(flusher) = self.flusher.lock().unwrap().as_ref() {
            let timeout = std::time::Duration::from_secs(60);
            if !flusher.wait_volume_generation_idle(name, deleted_generation, timeout) {
                tracing::warn!(
                    name,
                    generation = deleted_generation,
                    timeout_secs = timeout.as_secs(),
                    "timed out waiting for old-generation flusher work to retire"
                );
            }
        }

        // Rc-neutral path: lineage GC + FreePbas drive PBA retirement; the
        // per-write refcount cleanup loop that used to fire here is
        // gone. Any rc that survived `delete_volume` will be retired by
        // the next Lineage GC pass.
        Ok(freed_blocks)
    }

    /// List all volumes.
    pub fn list_volumes(&self) -> OnyxResult<Vec<VolumeConfig>> {
        self.meta.list_volumes()
    }

    // ── Snapshot lifecycle (see docs/onyx-phase2-snapshots.md) ──────────────

    /// Take a named point-in-time snapshot of `volume`. O(1) in metadb: the COW
    /// L2P roots are ref-counted, no data is copied.
    pub fn create_snapshot(&self, volume: &str, snap_name: &str) -> OnyxResult<SnapshotInfo> {
        self.lifecycle.with_write_lock(volume, || {
            let created_at = self.next_volume_generation();
            let info =
                self.meta
                    .create_snapshot(&VolumeId(volume.to_string()), snap_name, created_at)?;
            self.metrics
                .snapshot_create_ops
                .fetch_add(1, Ordering::Relaxed);
            tracing::info!(
                volume,
                snapshot = snap_name,
                snapshot_id = info.snapshot_id,
                "snapshot created"
            );
            Ok(info)
        })
    }

    /// List snapshots, optionally filtered to one volume.
    pub fn list_snapshots(&self, volume: Option<&str>) -> OnyxResult<Vec<SnapshotInfo>> {
        self.meta.list_snapshots(volume)
    }

    /// Capacity accounting for one volume. Usage is **cold data**: an L2P scan is
    /// O(live entries) (seconds on a large volume), so results are served from a
    /// per-volume TTL cache ([`USAGE_CACHE_TTL_SECS`]) and only recomputed when
    /// stale or missing. The returned `computed_at` tells callers how fresh it is.
    pub fn volume_usage(&self, volume: &str) -> OnyxResult<crate::meta::store::VolumeUsage> {
        let now = Self::current_time_nanos() / 1_000_000_000;
        if let Some(cached) = self.usage_cache.get(volume) {
            if now.saturating_sub(cached.computed_at) < USAGE_CACHE_TTL_SECS {
                return Ok(cached.clone());
            }
        }
        let usage = self.compute_volume_usage(volume, now)?;
        self.usage_cache.insert(volume.to_string(), usage.clone());
        Ok(usage)
    }

    /// Uncached L2P scan that produces a fresh [`VolumeUsage`]. See
    /// [`Self::volume_usage`] for the caching wrapper.
    fn compute_volume_usage(
        &self,
        volume: &str,
        now_secs: u64,
    ) -> OnyxResult<crate::meta::store::VolumeUsage> {
        use std::collections::HashSet;
        let vol_id = VolumeId(volume.to_string());
        let cfg = self
            .meta
            .get_volume(&vol_id)?
            .ok_or_else(|| OnyxError::VolumeNotFound(volume.to_string()))?;
        let block_size = u64::from(cfg.block_size);
        let lba_count = if block_size == 0 {
            0
        } else {
            cfg.size_bytes / block_size
        };

        let mut mapped_lbas: u64 = 0;
        // Each compressed unit (identified by base PBA + packer slot offset) is
        // counted once for the physical/original tallies even though every member
        // LBA carries the same unit fields.
        let mut seen_units: HashSet<(u64, u16)> = HashSet::new();
        let mut orig_bytes: u64 = 0;
        let mut phys_bytes: u64 = 0;
        self.meta.scan_blockmap_range(
            &vol_id,
            crate::types::Lba(0),
            lba_count,
            &mut |_lba, v| {
                if v.is_zero() {
                    return;
                }
                mapped_lbas += 1;
                if seen_units.insert((v.pba.0, v.slot_offset)) {
                    orig_bytes += u64::from(v.unit_original_size);
                    phys_bytes += u64::from(v.unit_compressed_size);
                }
            },
        )?;

        let mapped_bytes = mapped_lbas.saturating_mul(block_size);
        let ratio = |num: u64, den: u64| -> f64 {
            if den == 0 {
                0.0
            } else {
                num as f64 / den as f64
            }
        };
        Ok(crate::meta::store::VolumeUsage {
            volume: volume.to_string(),
            logical_size_bytes: cfg.size_bytes,
            mapped_lbas,
            mapped_bytes,
            physical_bytes: phys_bytes,
            unique_blocks: seen_units.len() as u64,
            dedup_ratio: ratio(mapped_bytes, orig_bytes),
            compress_ratio: ratio(orig_bytes, phys_bytes),
            data_reduction_ratio: ratio(mapped_bytes, phys_bytes),
            computed_at: now_secs,
        })
    }

    /// Drop a named snapshot and retire the PBAs its drop freed (rc hit zero).
    /// Returns the number of freed physical blocks. The freed PBAs are RETIRED
    /// (not direct-freed) so `GcRunner` Gate 1/2 reclaims them — same contract
    /// as lineage-GC-surfaced PBAs (avoids the premature-free CRC).
    pub fn delete_snapshot(&self, volume: &str, snap_name: &str) -> OnyxResult<usize> {
        let freed = self.lifecycle.with_write_lock(volume, || {
            self.meta
                .delete_snapshot(&VolumeId(volume.to_string()), snap_name)
        })?;
        let freed_blocks = freed.len();
        self.retire_freed_pbas(freed, "snapshot_drop_surfaced");
        self.metrics
            .snapshot_delete_ops
            .fetch_add(1, Ordering::Relaxed);
        tracing::info!(
            volume,
            snapshot = snap_name,
            freed_blocks,
            "snapshot deleted"
        );
        Ok(freed_blocks)
    }

    /// Clone a snapshot into a new writable volume `new_name`. The clone shares
    /// the snapshot's L2P pages copy-on-write; the source volume is untouched.
    /// The new volume is created but not opened — `start -v <new_name>` to serve.
    pub fn clone_snapshot(
        &self,
        volume: &str,
        snap_name: &str,
        new_name: &str,
    ) -> OnyxResult<VolumeConfig> {
        self.lifecycle.with_write_lock(new_name, || {
            let created_at = self.next_volume_generation();
            let cfg = self.meta.clone_snapshot(
                &VolumeId(volume.to_string()),
                snap_name,
                new_name,
                created_at,
            )?;
            self.metrics
                .snapshot_clone_ops
                .fetch_add(1, Ordering::Relaxed);
            tracing::info!(
                source = volume,
                snapshot = snap_name,
                clone = new_name,
                "snapshot cloned into new volume"
            );
            Ok(cfg)
        })
    }

    /// Restore a volume in place to a named snapshot (destructive rollback):
    /// every write made since the snapshot is discarded. Backed by metadb's
    /// atomic `restore_volume_to_snapshot` (snapshot→current diff replayed
    /// through the remap path); the diverged PBAs are reclaimed by the usual
    /// lineage path.
    ///
    /// The volume must be **stopped** (no live IO handle) — restore rewrites the
    /// L2P out from under any reader. The engine quiesces residual buffered
    /// writes and folds the metadb L2P buffer before diffing.
    pub fn restore_snapshot(&self, volume: &str, snap_name: &str) -> OnyxResult<()> {
        if self.has_active_handle(volume) {
            return Err(OnyxError::Config(format!(
                "cannot restore volume '{}' while it is open/served — stop it first",
                volume
            )));
        }

        // Quiesce so the snapshot→current diff sees fully-settled state: drop any
        // residual pending buffer entries and drain in-flight flush work for this
        // volume. metadb's `restore_volume_to_snapshot` does a forced BFG sync
        // itself (to fold staged commits + the L2P buffer for the diff), so no
        // onyx-side `sync_durable` is needed here.
        if let Some(pool) = &self.buffer_pool {
            pool.purge_volume(volume)?;
        }
        let vol_id = VolumeId(volume.to_string());
        if let Some(cfg) = self.meta.get_volume(&vol_id)? {
            if let Some(flusher) = self.flusher.lock().unwrap().as_ref() {
                flusher.wait_volume_generation_idle(
                    volume,
                    cfg.created_at,
                    std::time::Duration::from_secs(60),
                );
            }
        }

        let stats = self
            .lifecycle
            .with_write_lock(volume, || self.meta.restore_snapshot(&vol_id, snap_name))?;

        // Restore rewrote the L2P wholesale; drop any cached usage so a read
        // within the TTL recomputes against the rolled-back state.
        self.usage_cache.remove(volume);
        self.metrics
            .snapshot_restore_ops
            .fetch_add(1, Ordering::Relaxed);
        tracing::info!(
            volume,
            snapshot = snap_name,
            lbas_remapped = stats.lbas_remapped,
            lbas_deleted = stats.lbas_deleted,
            "volume restored to snapshot"
        );
        // The restore remaps dead-listed the diverged PBAs; nudge a checkpoint so
        // lineage GC surfaces them for the drain thread to retire.
        let _ = self.meta.request_durable_checkpoint();
        Ok(())
    }

    /// Whether `name` currently has a live, served IO handle (an `OnyxVolume`
    /// is open). Restore and other L2P-rewriting ops must refuse while true.
    fn has_active_handle(&self, name: &str) -> bool {
        let handles = self.live_handles.lock().unwrap();
        handles.iter().any(|(vol_name, weak)| {
            vol_name == name
                && weak
                    .upgrade()
                    .is_some_and(|flag| flag.load(Ordering::Acquire))
        })
    }

    /// Retire PBAs surfaced as freed by a snapshot drop. These are one-shot
    /// (a snapshot drops once, unlike lineage GC's re-surfacing), so they go
    /// straight through the lock-amortized batch retire — the same path
    /// `delete_volume`'s freed PBAs take via `cleanup_dead_pbas_batch`.
    fn retire_freed_pbas(&self, pbas: Vec<Pba>, reason: &'static str) {
        if pbas.is_empty() {
            return;
        }
        let pba_lifecycle = self
            .flusher
            .lock()
            .unwrap()
            .as_ref()
            .map(|flusher| flusher.pba_lifecycle());
        let Some(pba_lifecycle) = pba_lifecycle else {
            tracing::warn!(
                count = pbas.len(),
                reason,
                "cannot retire snapshot-freed PBAs in meta-only mode; left for next GC pass"
            );
            return;
        };
        let extents = crate::meta::backend::coalesce_free_pbas_to_extents(&pbas);
        pba_lifecycle.retire_committed_batch(reason, &extents);
    }

    /// Open a volume for IO. Requires full engine mode.
    pub fn open_volume(&self, name: &str) -> OnyxResult<OnyxVolume> {
        self.lifecycle.with_read_lock(name, || {
            let zm = self
                .zone_manager
                .as_ref()
                .ok_or_else(|| OnyxError::Config("cannot open volume in meta-only mode".into()))?;

            let vol_id = VolumeId(name.to_string());
            let vol_config = self
                .meta
                .get_volume(&vol_id)?
                .ok_or_else(|| OnyxError::VolumeNotFound(name.to_string()))?;
            let vol_ord = self.meta.volume_ordinal_str(name)?;

            let alive = Arc::new(AtomicBool::new(true));
            register_live_handle(&mut self.live_handles.lock().unwrap(), name, &alive);
            self.metrics.volume_open_ops.fetch_add(1, Ordering::Relaxed);

            let vol_lock = self.lifecycle.get_lock(name);
            Ok(OnyxVolume::new(
                name.to_string(),
                vol_ord,
                vol_config.size_bytes,
                vol_config.created_at,
                zm.clone(),
                alive,
                vol_lock,
                self.metrics.clone(),
            ))
        })
    }

    /// Graceful shutdown: stop flusher, then zone manager.
    pub fn shutdown(&self) -> OnyxResult<()> {
        let mut done = self.shutdown_done.lock().unwrap();
        if *done {
            return Ok(());
        }
        *done = true;

        // Stop the PD-health watchdog first: it is independent of the data
        // path (only reads PD liveness + spawns rebuild jobs), and stopping it
        // early keeps it from kicking a fresh auto-failover mid-shutdown.
        if let Some(mut wd) = self.chunklet_watchdog.lock().unwrap().take() {
            wd.stop();
        }
        // Stop the inline-degrade isolation reactor alongside the watchdog — it
        // likewise only marks PDs failed + spawns rebuild jobs, so quiescing it
        // early avoids a fresh isolation/failover racing shutdown.
        if let Some(mut r) = self.chunklet_isolation.lock().unwrap().take() {
            r.stop();
        }

        // Stop heartbeat writer first
        if let Some(mut hb) = self.heartbeat_writer.lock().unwrap().take() {
            hb.stop();
        }

        // Stop dedup scanner
        if let Some(mut scanner) = self.dedup_scanner.lock().unwrap().take() {
            scanner.stop();
        }

        // Stop GC (it injects into buffer pool)
        if let Some(mut gc) = self.gc_runner.lock().unwrap().take() {
            gc.stop();
        }

        // Then stop flusher and give graceful shutdown a chance to drain the
        // buffer so recovered pending entries do not accumulate across restarts.
        if let Some(mut flusher) = self.flusher.lock().unwrap().take() {
            if let Some(pool) = self.buffer_pool.as_ref() {
                flusher.drain_and_stop(pool);
            } else {
                flusher.stop();
            }
        }

        // Drain per-lane allocator caches back to the global free list
        if let Some(ref allocator) = self.allocator {
            allocator.drain_lane_caches();
        }

        // Zone manager shutdown is handled by Drop (it sends Shutdown to all workers)
        // We can't call shutdown(&mut self) through Arc, but Drop handles it.

        // Stop the durability watermark thread. Its final sync covers
        // everything the flusher has mark_flushed'd right up to now, so by the
        // time the thread has joined, metadb is durable for all acked writes.
        if let Some(mut watermark) = self.durability_watermark.lock().unwrap().take() {
            watermark.stop();
        }

        // Stop the lineage-freed PBA drain. It runs a final drain pass
        // before joining so any PBAs the GC surfaced between the last
        // tick and shutdown are returned to the allocator before we
        // checkpoint metadb durably.
        if let Some(mut drain) = self.lineage_drain.lock().unwrap().take() {
            drain.stop();
        }

        // Belt-and-suspenders: one more explicit sync in case new writes
        // arrived between the watermark thread's final tick and now. Stamp
        // only the contiguous applied prefix, then use the frontier returned
        // from the committed manifest for ring release below.
        let requested_frontier = self
            .buffer_pool
            .as_ref()
            .map(|pool| pool.applied_frontier())
            .unwrap_or(0);
        self.meta.set_buffer_applied_watermark(requested_frontier);
        let durable_buffer_seq = match self.meta.sync_durable() {
            Ok(seq) => seq,
            Err(e) => {
                tracing::error!(
                    requested_frontier,
                    error = %e,
                    "failed to sync_durable at shutdown — forcing dirty recovery on next boot"
                );
                return Ok(());
            }
        };

        // Drive one final reclaim pass. The durability watermark thread has
        // Re-run release with the exact manifest frontier returned above so
        // applied entries can leave the ring without crossing a lower seq
        // that was still pending when the checkpoint was built.
        if let Some(pool) = self.buffer_pool.as_ref() {
            pool.durable_seq_handle()
                .fetch_max(durable_buffer_seq, std::sync::atomic::Ordering::Release);
            if let Err(e) = pool.release_below(durable_buffer_seq) {
                tracing::warn!(
                    durable_buffer_seq,
                    error = %e,
                    "final release_below at shutdown failed"
                );
            }
            if let Err(e) = pool.advance_tail() {
                tracing::warn!(
                    error = %e,
                    "final advance_tail at shutdown failed — pending entries may persist"
                );
            }
            if let Err(e) = pool.persist_checkpoints() {
                tracing::error!(
                    error = %e,
                    "failed to persist final LV2 shard checkpoints — forcing dirty recovery on next boot"
                );
                return Ok(());
            }
        }

        // Stamp the LV3 superblock with FLAG_CLEAN_SHUTDOWN so the next boot
        // can skip dirty recovery. This is the last persistent act of the
        // engine — by this point flusher has drained, cleanup_tx is idle, and
        // the refcount CF is consistent with the per-volume blockmap CFs.
        if let Some(ref io_engine) = self.io_engine {
            match superblock::read_superblock(io_engine.device().as_ref()) {
                Ok(Some(mut sb)) => {
                    sb.set_clean_shutdown(true);
                    sb.update_crc();
                    if let Err(e) = superblock::write_superblock(io_engine.device().as_ref(), &sb) {
                        tracing::error!(
                            error = %e,
                            "failed to write clean-shutdown superblock — next boot will do dirty recovery"
                        );
                    } else {
                        tracing::info!("LV3 superblock marked clean");
                    }
                }
                Ok(None) => {
                    tracing::warn!("LV3 superblock missing at shutdown — cannot mark clean");
                }
                Err(e) => {
                    tracing::error!(error = %e, "reading LV3 superblock failed at shutdown");
                }
            }
        }

        tracing::info!("onyx engine shut down");
        Ok(())
    }

    /// Upgrade from a meta-only engine to full mode, reusing the existing MetaStore.
    ///
    /// The old engine's MetaStore Arc (and, on chunklet, the Pool it opened) is
    /// shared with the new engine rather than opened a second time. For a
    /// chunklet-backed metadb this is load-bearing: the pool now takes an
    /// exclusive `flock` on every member PD (see `Pool::open`), so a second
    /// `Pool::open` on the same devices would be rejected outright — and even if
    /// it weren't, two in-memory pools would tear the shared superblock.
    pub fn upgrade_from_meta_only(meta: Arc<MetaStore>, config: &OnyxConfig) -> OnyxResult<Self> {
        let lifecycle = Arc::new(VolumeLifecycleManager::default());
        let metrics = Arc::new(EngineMetrics::default());
        let generation_clock = Self::seed_generation_clock(&meta)?;

        // Chunklet RAID Pool (opened once; meta + LV3 + LV2 share it) + LV3
        // device. If metadb already opened the pool (meta lives on the meta LD),
        // REUSE it — a second `Pool::open` on the same PDs would tear the pool
        // superblock. Only open a fresh pool when metadb is file-backed but the
        // data plane is on chunklet.
        Self::validate_data_buffer_devices_disjoint(config)?;
        let chunklet_pool = match meta.chunklet_pool() {
            Some(pool) => Some(pool),
            None => Self::acquire_chunklet_pool(config)?,
        };
        let (device, lv3_ck_backend) = Self::acquire_lv3_device(config, &chunklet_pool)?;
        let device_size = device.size();

        // Validate / format LV3 superblock
        match superblock::read_superblock(device.as_ref())? {
            Some(sb) => {
                if sb.device_size_bytes != device_size {
                    return Err(OnyxError::Config(format!(
                        "LV3 superblock device_size {} != actual {}",
                        sb.device_size_bytes, device_size
                    )));
                }
                tracing::info!(
                    uuid = sb.uuid_string(),
                    version = sb.version,
                    "LV3 superblock validated (upgrade)"
                );
            }
            None => {
                let mut block0 = [0u8; 4096];
                device.read_at(&mut block0, 0)?;
                if block0.iter().all(|&b| b == 0) {
                    tracing::info!("fresh LV3 device — formatting (upgrade)");
                    superblock::format_device(device.as_ref())?;
                } else {
                    return Err(OnyxError::Config(
                        "LV3 block 0 has data but invalid superblock (magic/CRC/version failed)"
                            .into(),
                    ));
                }
            }
        }

        let io_engine = Self::build_io_engine(device.clone(), &config.storage, metrics.clone())?;

        // Space allocator
        // The allocator addresses PBAs through the IoEngine's `pba_offset`
        // (= RESERVED_BLOCKS): device block = pba + pba_offset. So it must be
        // built with the io-ADDRESSABLE capacity (raw device minus the reserved
        // offset), not the raw device size — otherwise the top RESERVED_BLOCKS
        // PBAs would translate past the device end and a boundary write (esp. a
        // multi-block full-stripe write) fails with chunklet "IO out of range".
        let allocator = Arc::new(SpaceAllocator::new_with_hazards(
            device_size
                .saturating_sub(crate::types::RESERVED_BLOCKS * crate::types::BLOCK_SIZE as u64),
            config.buffer.shards,
        ));
        allocator.set_stripe_geometry(io_engine.stripe_blocks(), io_engine.stripe_phase());
        allocator.rebuild_from_metadata(&meta)?;

        // Write buffer pool (with shard migration if needed)
        let buffer_pool = Self::open_buffer_pool(
            config,
            &chunklet_pool,
            &meta,
            &lifecycle,
            &allocator,
            &io_engine,
            &metrics,
        )?;

        // LV3 read pool — needed by the flusher's dedup verify-on-hit
        // path. Built before the flusher so the read pool is wired up
        // by the time the dedup workers spawn.
        let read_pool = Self::build_read_pool(config, &device, metrics.clone())?;

        // Background flusher
        let flusher = BufferFlusher::start_with_metrics(
            buffer_pool.clone(),
            meta.clone(),
            lifecycle.clone(),
            allocator.clone(),
            io_engine.clone(),
            read_pool.clone(),
            &config.flush,
            &config.dedup,
            metrics.clone(),
        );

        let pba_lifecycle = flusher.pba_lifecycle();

        // Zone manager — reuses the read_pool built above for the
        // flusher's dedup verify path.
        let zone_manager = Arc::new(ZoneManager::new_full(
            config.engine.zone_count,
            config.engine.zone_size_blocks,
            meta.clone(),
            buffer_pool.clone(),
            io_engine.clone(),
            metrics.clone(),
            Some(allocator.clone()),
            Some(pba_lifecycle.clone()),
            read_pool.clone(),
        )?);

        // Stage-4 fold: cold-tail channel (see the full-open path). (None,
        // None) ⇒ legacy paths.
        let (cold_tail_tx, cold_tail_rx) = build_cold_tail_fold_channel(&config, &read_pool);

        // Shared heat map (GC refreshes it; dedup scanner reads it for §6 orphan
        // reclaim). See the full-open path.
        let heat = build_heat_map(&config.gc, &allocator);
        let scanner_heat = config.gc.heat_enabled.then(|| heat.clone());
        // Stage-5 per-PBA orphan-reclaim bitmap (None unless per-PBA mode on).
        let ref_bitmap = build_ref_bitmap(&config.gc, &config.dedup, &allocator);

        // Dedup scanner
        let dedup_scanner = if config.dedup.enabled {
            Some(DedupScanner::start_with_metrics(
                metrics.clone(),
                meta.clone(),
                io_engine.clone(),
                allocator.clone(),
                lifecycle.clone(),
                buffer_pool.clone(),
                flusher.candidate_cache(),
                read_pool.clone(),
                cold_tail_rx,
                scanner_heat,
                ref_bitmap.clone(),
                pba_lifecycle.clone(),
                config.dedup.clone(),
            ))
        } else {
            None
        };

        // GC runner. It always runs the physical retired-PBA reclaimer;
        // `gc.enabled` only gates rewrite scanning.
        let gc_runner = Some(GcRunner::start_with_metrics(
            metrics.clone(),
            meta.clone(),
            io_engine.clone(),
            buffer_pool.clone(),
            lifecycle.clone(),
            allocator.clone(),
            heat.clone(),
            ref_bitmap,
            cold_tail_tx,
            pba_lifecycle.clone(),
            config.gc.clone(),
        ));
        let heat = config.gc.heat_enabled.then_some(heat);

        // Heartbeat writer
        let heartbeat_writer = if config.ha.enabled {
            Some(Self::build_heartbeat_writer(
                device.clone(),
                config.ha.node_id,
                std::time::Duration::from_millis(config.ha.heartbeat_interval_ms),
                &config.storage,
            )?)
        } else {
            None
        };

        tracing::info!("onyx engine upgraded to full mode");

        let watermark = DurabilityWatermarkHandle::start(
            meta.clone(),
            buffer_pool.clone(),
            buffer_pool.durable_seq_handle(),
            std::time::Duration::from_millis(50),
            config.meta.flush_dirty_pages_threshold,
            config.meta.checkpoint_ring_fill_pct,
        );
        let lineage_drain = LineageFreedPbaDrainHandle::start(
            meta.clone(),
            pba_lifecycle.clone(),
            std::time::Duration::from_millis(100),
        );

        let chunklet_watchdog = Self::build_chunklet_watchdog(&chunklet_pool, config);
        let chunklet_isolation = Self::build_chunklet_isolation(&chunklet_pool, config);

        Ok(Self {
            meta,
            io_engine: Some(io_engine),
            allocator: Some(allocator),
            buffer_pool: Some(buffer_pool),
            flusher: Mutex::new(Some(flusher)),
            gc_runner: Mutex::new(gc_runner),
            dedup_scanner: Mutex::new(dedup_scanner),
            chunklet_watchdog: Mutex::new(chunklet_watchdog),
            chunklet_isolation: Mutex::new(chunklet_isolation),
            heartbeat_writer: Mutex::new(heartbeat_writer),
            durability_watermark: Mutex::new(Some(watermark)),
            lineage_drain: Mutex::new(Some(lineage_drain)),
            read_pool,
            zone_manager: Some(zone_manager),
            live_handles: Mutex::new(Vec::new()),
            lifecycle,
            metrics,
            heat,
            usage_cache: dashmap::DashMap::new(),
            generation_clock: AtomicU64::new(generation_clock),
            lv3_ck_backend,
            config: config.clone(),
            shutdown_done: Mutex::new(false),
        })
    }

    /// Update GC config on a running engine (hot-reload).
    pub fn update_gc_config(&self, config: crate::gc::config::GcConfig) {
        if let Some(gc) = self.gc_runner.lock().unwrap().as_ref() {
            gc.update_config(config);
        }
    }

    /// Update dedup scanner config on a running engine (hot-reload).
    pub fn update_dedup_config(&self, config: crate::dedup::config::DedupConfig) {
        if let Some(scanner) = self.dedup_scanner.lock().unwrap().as_ref() {
            scanner.update_config(config);
        }
    }

    /// Whether the engine is in full IO mode (not meta-only / standby).
    pub fn is_full_mode(&self) -> bool {
        self.zone_manager.is_some()
    }

    /// Access the MetaStore (for advanced use / testing).
    pub fn meta(&self) -> &Arc<MetaStore> {
        &self.meta
    }

    /// Access the ZoneManager (for frontends like ublk).
    pub fn zone_manager(&self) -> Option<&Arc<ZoneManager>> {
        self.zone_manager.as_ref()
    }

    /// Access the WriteBufferPool (for testing / inspection).
    pub fn buffer_pool(&self) -> Option<&Arc<WriteBufferPool>> {
        self.buffer_pool.as_ref()
    }

    /// Access the IoEngine (for testing / inspection).
    pub fn io_engine(&self) -> Option<&Arc<IoEngine>> {
        self.io_engine.as_ref()
    }

    /// Access the SpaceAllocator (for testing / inspection).
    pub fn allocator(&self) -> Option<&Arc<SpaceAllocator>> {
        self.allocator.as_ref()
    }

    pub fn metrics_snapshot(&self) -> EngineMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn volume_metrics_snapshot(&self) -> Vec<(String, crate::metrics::VolumeMetricsSnapshot)> {
        self.metrics.volume_metrics_snapshot()
    }

    /// The shared chunklet pool handle when running on the chunklet backend,
    /// `None` on the file backend. Reaches it through `MetaStore`, which owns
    /// the single `Pool::open` (all three role LDs derive from it), so online
    /// operator ops (`chunklet_ops`) act on the same pool the engine serves IO
    /// from rather than a second, superblock-tearing open.
    pub fn chunklet_pool(&self) -> Option<Arc<onyx_chunklet::Pool>> {
        self.meta.chunklet_pool()
    }

    /// Online-extend a chunklet role LD by `additional_rows` and propagate the
    /// new capacity into the live engine — no restart. `role` is `"lv3"` or
    /// `"meta"`:
    /// - **lv3**: `extend_ld` → `open_ld` → `swap_ld` the shared backend →
    ///   `SpaceAllocator::grow_capacity` (new dense PBAs at the top) → rewrite
    ///   the LV3 superblock's `device_size_bytes` so the next boot's size check
    ///   matches the grown LD.
    /// - **meta**: `extend_ld` → `open_ld` → `MetaStore::grow_meta_capacity`
    ///   (swap + OMET superblock rewrite + metadb page-device widen, clearing any
    ///   `CapacityExhausted`).
    ///
    /// Returns the new LD capacity in bytes. `extend_ld` holds only the LD read
    /// lock, so live IO continues; the extra rows are additive (old rows/parity
    /// untouched).
    pub fn chunklet_extend(&self, role: &str, additional_rows: u16) -> OnyxResult<u64> {
        let pool = self
            .chunklet_pool()
            .ok_or_else(|| OnyxError::Config("chunklet backend not enabled".into()))?;
        match role {
            "lv3" => {
                let id_str = self.config.chunklet.lv3_ld_id.as_deref().ok_or_else(|| {
                    OnyxError::Config("[chunklet] lv3_ld_id is not configured".into())
                })?;
                let ld_id = onyx_chunklet::ops::parse_ld_id(id_str)?;
                let new_cap = pool.extend_ld(ld_id, additional_rows)?;
                let new_ld = pool.open_ld(ld_id)?;
                let backend = self.lv3_ck_backend.as_ref().ok_or_else(|| {
                    OnyxError::Config("LV3 is not on a chunklet LD (cannot online-extend)".into())
                })?;
                backend.swap_ld(new_ld);
                if let Some(alloc) = &self.allocator {
                    // Same io-addressable transform the allocator was built with.
                    let io_addressable = new_cap.saturating_sub(
                        crate::types::RESERVED_BLOCKS * crate::types::BLOCK_SIZE as u64,
                    );
                    alloc.grow_capacity(io_addressable)?;
                }
                if let Some(mut sb) = superblock::read_superblock(backend.as_ref())? {
                    sb.device_size_bytes = new_cap;
                    sb.update_crc();
                    superblock::write_superblock(backend.as_ref(), &sb)?;
                }
                tracing::info!(
                    role = "lv3",
                    additional_rows,
                    new_cap,
                    "chunklet online extend complete"
                );
                Ok(new_cap)
            }
            "meta" => {
                let id_str = self.config.chunklet.meta_ld_id.as_deref().ok_or_else(|| {
                    OnyxError::Config("[chunklet] meta_ld_id is not configured".into())
                })?;
                let ld_id = onyx_chunklet::ops::parse_ld_id(id_str)?;
                let new_cap = pool.extend_ld(ld_id, additional_rows)?;
                let new_ld = pool.open_ld(ld_id)?;
                self.meta.grow_meta_capacity(new_ld)?;
                tracing::info!(
                    role = "meta",
                    additional_rows,
                    new_cap,
                    "chunklet online extend complete"
                );
                Ok(new_cap)
            }
            other => Err(OnyxError::Config(format!(
                "unknown chunklet extend role '{other}' (use 'lv3' or 'meta'; \
                 lv2/buffer online-extend is not supported)"
            ))),
        }
    }

    pub fn status_snapshot(&self) -> OnyxResult<EngineStatusSnapshot> {
        let contiguity = self
            .allocator
            .as_ref()
            .map(|alloc| alloc.contiguity_stats());
        let dedup_migration = self.meta.dedup_migration_status();
        Ok(EngineStatusSnapshot {
            mode: if self.zone_manager.is_some() {
                "active".to_string()
            } else {
                "standby".to_string()
            },
            volume_count: self.meta.list_volumes()?.len(),
            live_handle_count: self
                .live_handles
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, flag)| flag.strong_count() > 0)
                .count(),
            zone_count: self.zone_manager.as_ref().map(|zm| zm.zone_count()),
            buffer_pending_entries: self.buffer_pool.as_ref().map(|pool| pool.pending_count()),
            buffer_fill_pct: self.buffer_pool.as_ref().map(|pool| pool.fill_percentage()),
            buffer_physical_fill_pct: self
                .buffer_pool
                .as_ref()
                .map(|pool| pool.physical_fill_percentage()),
            buffer_payload_memory_bytes: self
                .buffer_pool
                .as_ref()
                .map(|pool| pool.payload_memory_bytes()),
            buffer_payload_memory_limit_bytes: self
                .buffer_pool
                .as_ref()
                .map(|pool| pool.payload_memory_limit_bytes()),
            metadb_memory: self.meta.memory_stats().ok(),
            buffer_shards: self
                .buffer_pool
                .as_ref()
                .map(|pool| pool.shard_snapshots())
                .unwrap_or_default(),
            allocator_free_blocks: self
                .allocator
                .as_ref()
                .map(|alloc| alloc.free_block_count()),
            allocator_total_blocks: self
                .allocator
                .as_ref()
                .map(|alloc| alloc.total_block_count()),
            allocator_free_extents: contiguity.map(|c| c.free_extents),
            allocator_largest_run_blocks: contiguity.map(|c| c.largest_run_blocks as u64),
            allocator_stripe_capable_blocks: contiguity.and_then(|c| c.stripe_capable_blocks),
            allocator_free_blocks_in_set: contiguity.map(|c| c.free_blocks_in_set),
            heat: self.heat.as_ref().map(|h| h.cached_summary()),
            meta_fenced: self
                .buffer_pool
                .as_ref()
                .and_then(|pool| pool.meta_fence_reason().map(str::to_string)),
            // chunklet topology/health: read-only `pool.metrics()` (holds only
            // `state.read()`, safe against live IO). None on the file backend or
            // if the metrics read errors — status must never fail on it.
            chunklet: self
                .meta
                .chunklet_pool()
                .and_then(|pool| pool.metrics().ok())
                .map(|m| onyx_chunklet::ops::PoolSnapshot::from_metrics(&m)),
            dedup_cuckoo_buckets: dedup_migration.new_bucket_count,
            dedup_resize_growing: dedup_migration.growing,
            dedup_resize_old_buckets: dedup_migration.old_bucket_count,
            metrics: self.metrics.snapshot(),
        })
    }

    pub fn status_report(&self) -> OnyxResult<String> {
        Ok(self.status_snapshot()?.render_text())
    }
}

impl Drop for OnyxEngine {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

/// Build the adaptive-reclaim heat map for the GC runner. When the refresh is
/// disabled we still hand the runner a valid handle, but size it to a single
/// bucket so it allocates essentially nothing (the refresh step is gated off at
/// runtime, and the engine stores `None` so status reports no heat section).
fn build_heat_map(gc: &crate::gc::config::GcConfig, allocator: &Arc<SpaceAllocator>) -> HeatMap {
    if !gc.heat_enabled {
        return HeatMap::new(0, gc.heat_bucket_size_blocks);
    }
    let hm = HeatMap::new(allocator.total_block_count(), gc.heat_bucket_size_blocks);
    tracing::info!(
        buckets = hm.n_buckets(),
        memory_mib = hm.memory_bytes() / (1024 * 1024),
        bucket_blocks = hm.bucket_size_blocks(),
        total_pbas = hm.total_pbas(),
        "heat map: adaptive-reclaim refresh enabled (observe-only)"
    );
    hm
}

/// Build the Stage-5 per-PBA referenced bitmap, or `None` when per-PBA orphan
/// reclaim is off. Returning `None` allocates nothing — the GC writer and dedup
/// reader stay on the §6 region-selector path. Only built when the heat refresh
/// is on (the bitmap rides the heat sweep's live-L2P walk), orphan reclaim is
/// enabled, and the per-PBA selector flag is set. Sized from device capacity ×
/// (K+1) snapshots; logs the projected resident footprint like `build_heat_map`.
fn build_ref_bitmap(
    gc: &crate::gc::config::GcConfig,
    dedup: &crate::dedup::config::DedupConfig,
    allocator: &Arc<SpaceAllocator>,
) -> Option<RefBitmap> {
    if !(gc.heat_enabled && dedup.orphan_reclaim_enabled && dedup.orphan_reclaim_per_pba) {
        return None;
    }
    let k = dedup.orphan_reclaim_clean_sweeps.clamp(1, 4) as usize;
    let rb = RefBitmap::new(allocator.total_block_count(), k);
    tracing::info!(
        total_pbas = rb.total_pbas(),
        clean_sweeps = rb.k(),
        snapshot_mib = (rb.n_words() * 8) / (1024 * 1024),
        projected_resident_mib = rb.projected_resident_bytes() / (1024 * 1024),
        "dedup: Stage-5 per-PBA orphan-reclaim bitmap enabled"
    );
    Some(rb)
}

/// Stage-4 fold: build the cold-tail channel that lets the GC heat-refresh
/// walk feed cold candidates to the dedup scanner, eliminating the scanner's
/// own duplicate live-L2P traversal (`docs/adaptive-reclaim-heatmap.md`
/// Stage 4). Returns `(producer, consumer)` endpoints when the fold is
/// enabled, else `(None, None)` — which leaves both subsystems on their
/// legacy paths (zero behavior change). The fold requires:
///   - `gc.heat_fold_cold_tail_enabled` (the flag),
///   - `gc.heat_enabled` (there must be a heat walk to ride),
///   - a configured `ReadPool` (cold-tail's LV3 reads are batched through it),
///   - `dedup.enabled` (there is a scanner to consume the channel).
fn build_cold_tail_fold_channel(
    config: &OnyxConfig,
    read_pool: &Option<Arc<ReadPool>>,
) -> (
    Option<crossbeam_channel::Sender<crate::dedup::ColdTailTarget>>,
    Option<crossbeam_channel::Receiver<crate::dedup::ColdTailTarget>>,
) {
    let enabled = config.gc.heat_fold_cold_tail_enabled
        && config.gc.heat_enabled
        && config.dedup.enabled
        && read_pool.is_some();
    if !enabled {
        if config.gc.heat_fold_cold_tail_enabled {
            tracing::info!(
                heat_enabled = config.gc.heat_enabled,
                dedup_enabled = config.dedup.enabled,
                read_pool = read_pool.is_some(),
                "cold-tail fold requested but a prerequisite is off; running legacy paths"
            );
        }
        return (None, None);
    }
    let cap = config.gc.heat_fold_channel_capacity.max(1);
    let (tx, rx) = crossbeam_channel::bounded(cap);
    tracing::info!(
        capacity = cap,
        push_max_per_cycle = config.gc.heat_fold_push_max_per_cycle,
        "cold-tail fold enabled: dedup scanner drains the GC heat-refresh walk"
    );
    (Some(tx), Some(rx))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> OnyxConfig {
        // Every OnyxConfig field is `#[serde(default)]`, so an empty document
        // yields the all-defaults config.
        toml::from_str("").expect("empty toml -> all serde defaults")
    }

    #[test]
    fn dedup_enabled_without_read_pool_is_rejected() {
        let mut cfg = default_config();
        cfg.dedup.enabled = true;
        cfg.storage.read_pool_workers = 0;
        assert!(
            OnyxEngine::validate_dedup_read_pool(&cfg).is_err(),
            "dedup + no read pool must be refused (trust-hash mode is unsafe)"
        );
    }

    #[test]
    fn dedup_enabled_with_read_pool_is_ok() {
        let mut cfg = default_config();
        cfg.dedup.enabled = true;
        cfg.storage.read_pool_workers = 4;
        assert!(OnyxEngine::validate_dedup_read_pool(&cfg).is_ok());
    }

    #[test]
    fn dedup_disabled_without_read_pool_is_ok() {
        let mut cfg = default_config();
        cfg.dedup.enabled = false;
        cfg.storage.read_pool_workers = 0;
        assert!(OnyxEngine::validate_dedup_read_pool(&cfg).is_ok());
    }

    #[test]
    fn live_handle_registry_does_not_retain_disconnected_sessions() {
        let mut handles = LiveHandleRegistry::new();

        for _ in 0..128 {
            let alive = Arc::new(AtomicBool::new(true));
            let observer = Arc::downgrade(&alive);
            register_live_handle(&mut handles, "bench", &alive);
            assert_eq!(handles.len(), 1);
            drop(alive);
            assert!(observer.upgrade().is_none());
        }

        let active = Arc::new(AtomicBool::new(true));
        register_live_handle(&mut handles, "bench", &active);
        assert_eq!(handles.len(), 1, "expired session entries must be pruned");

        let other = Arc::new(AtomicBool::new(true));
        register_live_handle(&mut handles, "other", &other);
        invalidate_live_handles_in(&mut handles, "bench");
        assert!(!active.load(Ordering::Acquire));
        assert!(other.load(Ordering::Acquire));
    }
}
