use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::buffer::flush::BufferFlusher;
use crate::buffer::pool::{BufferRuntimeLimits, ThrottleSettings, WriteBufferPool};
use crate::config::{IoBackend as IoBackendConfig, OnyxConfig, StorageConfig};
use crate::dedup::scanner::DedupScanner;
use crate::error::{OnyxError, OnyxResult};
use crate::gc::runner::GcRunner;
use crate::io::device::RawDevice;
use crate::io::engine::IoEngine;
use crate::io::read_pool::ReadPool;
use crate::io::superblock::{self, HeartbeatWriter};
use crate::io::uring::IoUringSession;
use crate::lifecycle::VolumeLifecycleManager;
use crate::meta::store::MetaStore;
use crate::metrics::{EngineMetrics, EngineMetricsSnapshot, EngineStatusSnapshot};
use crate::space::allocator::SpaceAllocator;
use crate::types::{CompressionAlgo, VolumeConfig, VolumeId};
use crate::volume::OnyxVolume;
use crate::zone::manager::ZoneManager;

mod durability;
mod lineage;

use durability::DurabilityWatermarkHandle;
use lineage::LineageFreedPbaDrainHandle;

/// A per-handle "alive" flag. Set to false when the volume is deleted.
/// Each OnyxVolume holds its own Arc to this flag. The engine keeps Weak
/// references so it can invalidate all outstanding handles on delete.
pub type VolumeAliveFlag = Arc<AtomicBool>;

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
    heartbeat_writer: Mutex<Option<HeartbeatWriter>>,
    /// Background thread that periodically syncs metadata, then advances
    /// the buffer pool's `durable_seq` watermark so ring reclaim can safely
    /// proceed. Keeps the hot path at `WriteOptions::sync = false`.
    durability_watermark: Mutex<Option<DurabilityWatermarkHandle>>,
    /// [[no-refcount-hot-path-design]] Phase 5: drains
    /// `WalOp::FreePbas` outcomes from metadb's Lineage GC into the
    /// allocator's free list. Started in full mode, `None` in
    /// meta-only mode (no allocator to feed).
    lineage_drain: Mutex<Option<LineageFreedPbaDrainHandle>>,
    #[allow(dead_code)]
    read_pool: Option<Arc<ReadPool>>,
    zone_manager: Option<Arc<ZoneManager>>,
    /// Live volume handles: (vol_name, alive_flag).
    /// delete_volume sets all matching flags to false.
    /// Entries with dropped handles (strong_count==1, only engine's copy) are
    /// cleaned up lazily on subsequent open_volume/delete_volume calls.
    live_handles: Mutex<Vec<(String, VolumeAliveFlag)>>,
    lifecycle: Arc<VolumeLifecycleManager>,
    metrics: Arc<EngineMetrics>,
    generation_clock: AtomicU64,
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
        metrics: Arc<EngineMetrics>,
    ) -> OnyxResult<Option<Arc<ReadPool>>> {
        let workers = config.storage.read_pool_workers;
        if workers == 0 {
            tracing::info!("read pool disabled (read_pool_workers=0) — LV3 reads run inline");
            return Ok(None);
        }
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
        // Each worker opens its own io_uring; the `device` handle itself is
        // only needed for fd + base_offset, so drop it once the pool has
        // captured what it needs.
        drop(device);
        Ok(Some(Arc::new(pool)))
    }

    fn build_heartbeat_writer(
        device: RawDevice,
        node_id: u64,
        interval: std::time::Duration,
        storage: &StorageConfig,
    ) -> OnyxResult<HeartbeatWriter> {
        match storage.io_backend {
            IoBackendConfig::Syscall => Ok(HeartbeatWriter::start(device, node_id, interval)),
            IoBackendConfig::Uring => {
                let session = Arc::new(IoUringSession::new(8)?);
                Ok(HeartbeatWriter::start_uring(
                    device, node_id, interval, session,
                ))
            }
        }
    }

    fn build_io_engine(
        data_dev: RawDevice,
        storage: &StorageConfig,
        metrics: Arc<EngineMetrics>,
    ) -> OnyxResult<Arc<IoEngine>> {
        match storage.io_backend {
            IoBackendConfig::Syscall => Ok(Arc::new(IoEngine::new_with_metrics(
                data_dev,
                storage.use_hugepages,
                metrics,
            ))),
            IoBackendConfig::Uring => {
                let session = Arc::new(IoUringSession::new(storage.uring_sq_entries)?);
                tracing::info!(
                    sq_entries = storage.uring_sq_entries,
                    "LV3 IoEngine using io_uring backend"
                );
                Ok(Arc::new(IoEngine::new_with_metrics_uring(
                    data_dev,
                    storage.use_hugepages,
                    metrics,
                    session,
                )))
            }
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
        meta: &Arc<MetaStore>,
        lifecycle: &Arc<VolumeLifecycleManager>,
        allocator: &Arc<SpaceAllocator>,
        io_engine: &Arc<IoEngine>,
        metrics: &Arc<EngineMetrics>,
    ) -> OnyxResult<Arc<WriteBufferPool>> {
        let buf_path =
            config.buffer.device.as_ref().ok_or_else(|| {
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
                            std::time::Duration::from_micros(config.buffer.group_commit_wait_us),
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
        let max_payload_memory = if config.buffer.max_memory_mb > 0 {
            config.buffer.max_memory_mb as u64 * 1024 * 1024
        } else {
            Self::auto_detect_max_payload_memory()
        };
        let buffer_uring_entries = match config.storage.io_backend {
            IoBackendConfig::Uring => Some(config.storage.uring_sq_entries),
            IoBackendConfig::Syscall => None,
        };
        let buffer_runtime_limits = BufferRuntimeLimits::from_config(
            max_payload_memory,
            config.buffer.staging_queue_entries,
            config.buffer.sync_batch_max_entries,
            config.buffer.sync_batch_max_bytes_mb as usize * 1024 * 1024,
            config.buffer.volatile_memory_mb as u64 * 1024 * 1024,
        )
        .with_throttle(ThrottleSettings {
            min_pct: config.buffer.throttle_min_pct,
            max_pct: config.buffer.throttle_max_pct,
            scale_us: config.buffer.throttle_scale_us,
            cap_us: config.buffer.throttle_cap_us,
        });
        let pool = Arc::new(WriteBufferPool::open_with_options_full_and_limits(
            buf_dev,
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
        for (vol_name, flag) in handles.iter() {
            if vol_name == name {
                flag.store(false, Ordering::Release);
            }
        }
        handles.retain(|(_, flag)| Arc::strong_count(flag) > 1);
    }

    #[cfg(target_os = "linux")]
    fn validate_data_buffer_devices_disjoint(config: &OnyxConfig) -> OnyxResult<()> {
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

    /// Open the engine with full IO capability (data device + buffer + flusher + zones).
    ///
    /// Compression is per-volume (stored in VolumeConfig metadata), not engine-wide.
    pub fn open(config: &OnyxConfig) -> OnyxResult<Self> {
        Self::validate_data_buffer_devices_disjoint(config)?;

        // 1. MetaStore
        let meta = Arc::new(MetaStore::open(&config.meta)?);
        let lifecycle = Arc::new(VolumeLifecycleManager::default());
        let metrics = Arc::new(EngineMetrics::default());
        let generation_clock = Self::seed_generation_clock(&meta)?;

        // (no shared deletion state needed — per-handle alive flags are used)

        // 2. Data device + IO engine
        let data_path = config.storage.data_device.as_ref().ok_or_else(|| {
            OnyxError::Config("storage.data_device is required for full mode".into())
        })?;
        let data_dev = RawDevice::open(data_path)?;
        let device_size = data_dev.size();

        // 2a. Validate / format LV3 superblock
        let mut superblock = match superblock::read_superblock(&data_dev)? {
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
                data_dev.read_at(&mut block0, 0)?;
                if block0.iter().all(|&b| b == 0) {
                    tracing::info!("fresh LV3 device — formatting");
                    superblock::format_device(&data_dev)?
                } else {
                    return Err(OnyxError::Config(
                        "LV3 block 0 has data but invalid superblock (magic/CRC/version failed)"
                            .into(),
                    ));
                }
            }
        };

        // 2b. Recovery branch based on clean/dirty shutdown marker.
        if superblock.is_clean_shutdown() {
            tracing::info!("clean shutdown marker present — skipping refcount rebuild");
        } else {
            tracing::warn!(
                "dirty startup detected — rebuilding refcount CF from per-volume blockmap CFs"
            );
            let summary = meta.rebuild_refcount_from_blockmap()?;
            tracing::info!(
                referenced_pbas = summary.referenced_pbas,
                fixed_entries = summary.fixed_entries,
                orphan_entries_removed = summary.orphan_entries_removed,
                total_set = summary.total_set,
                "refcount CF rebuilt"
            );
        }

        // 2c. Mark dirty before serving IO. The bit is cleared again only on
        //     graceful shutdown, so an unexpected exit automatically forces a
        //     dirty recovery on the next boot.
        superblock.set_clean_shutdown(false);
        superblock.update_crc();
        superblock::write_superblock(&data_dev, &superblock)?;

        let io_engine = Self::build_io_engine(data_dev, &config.storage, metrics.clone())?;

        // 3. Space allocator
        let allocator = Arc::new(SpaceAllocator::new_with_hazards(
            device_size,
            config.buffer.shards,
        ));
        allocator.rebuild_from_metadata(&meta)?;

        // 4. Write buffer pool (with shard migration if needed)
        let buffer_pool =
            Self::open_buffer_pool(config, &meta, &lifecycle, &allocator, &io_engine, &metrics)?;

        // LV3 read pool — built BEFORE the flusher so the dedup
        // workers can route candidate-hit / dedup_index-hit verifies
        // through it (`dedup::verify::batched_verify`). Also still
        // shared with ZoneManager for the foreground read path.
        let read_pool = Self::build_read_pool(config, metrics.clone())?;

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

        // 9. Zone manager
        let zone_manager = Arc::new(ZoneManager::new_full(
            config.engine.zone_count,
            config.engine.zone_size_blocks,
            meta.clone(),
            buffer_pool.clone(),
            io_engine.clone(),
            metrics.clone(),
            Some(allocator.clone()),
            flusher.candidate_cache(),
            read_pool.clone(),
        )?);

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
            config.gc.clone(),
        ));

        // 11. Heartbeat writer (after all other subsystems)
        let heartbeat_writer = if config.ha.enabled {
            let hb_dev = RawDevice::open(data_path)?;
            Some(Self::build_heartbeat_writer(
                hb_dev,
                config.ha.node_id,
                std::time::Duration::from_millis(config.ha.heartbeat_interval_ms),
                &config.storage,
            )?)
        } else {
            None
        };

        tracing::info!("onyx engine opened (full mode)");

        // Start the durability watermark thread now that both meta and
        // buffer_pool are live. It advances `durable_seq` cheaply and asks
        // metadb for low-frequency checkpoints, unblocking the buffer ring
        // reclaim path without forcing every tick through metadata IO.
        let watermark = DurabilityWatermarkHandle::start(
            meta.clone(),
            buffer_pool.clone(),
            buffer_pool.max_flushed_seq_handle(),
            buffer_pool.durable_seq_handle(),
            config.meta.checkpoint_interval(),
            config.meta.unlogged_flush_commits,
            config.meta.flush_dirty_pages_threshold,
        );
        let lineage_drain = LineageFreedPbaDrainHandle::start(
            meta.clone(),
            allocator.clone(),
            metrics.clone(),
            std::time::Duration::from_millis(100),
        );

        Ok(Self {
            meta,
            io_engine: Some(io_engine),
            allocator: Some(allocator),
            buffer_pool: Some(buffer_pool),
            flusher: Mutex::new(Some(flusher)),
            gc_runner: Mutex::new(gc_runner),
            dedup_scanner: Mutex::new(dedup_scanner),
            heartbeat_writer: Mutex::new(heartbeat_writer),
            durability_watermark: Mutex::new(Some(watermark)),
            lineage_drain: Mutex::new(Some(lineage_drain)),
            read_pool,
            zone_manager: Some(zone_manager),
            live_handles: Mutex::new(Vec::new()),
            lifecycle,
            metrics,
            generation_clock: AtomicU64::new(generation_clock),
            config: config.clone(),
            shutdown_done: Mutex::new(false),
        })
    }

    /// Open engine in metadata-only mode (no data device, no IO).
    ///
    /// Only volume management operations (create/delete/list) are available.
    /// Attempting to open_volume() will fail.
    pub fn open_meta_only(config: &OnyxConfig) -> OnyxResult<Self> {
        let meta = Arc::new(MetaStore::open(&config.meta)?);
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
            heartbeat_writer: Mutex::new(None),
            durability_watermark: Mutex::new(None),
            lineage_drain: Mutex::new(None),
            read_pool: None,
            zone_manager: None,
            live_handles: Mutex::new(Vec::new()),
            lifecycle,
            metrics,
            generation_clock: AtomicU64::new(generation_clock),
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

            if let Some(allocator) = &self.allocator {
                let candidate = self
                    .flusher
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|flusher| flusher.candidate_cache());
                if let Some(candidate) = candidate {
                    BufferFlusher::cleanup_dead_pbas_batch(
                        allocator,
                        &candidate,
                        &cleanups,
                        "volume_delete",
                    );
                }
            } else if cleanups.iter().any(|cleanup| cleanup.pba_freed) {
                tracing::warn!(
                    name,
                    cleanup_count = cleanups.len(),
                    "delete_volume: meta-only mode cannot return freed PBAs to allocator; dedup_index is left to verified repair/scrub"
                );
            }

            self.invalidate_live_handles(name);
            self.metrics.remove_volume_metrics(name);
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

        // Phase 5: lineage GC + FreePbas drive PBA retirement; the
        // per-write refcount cleanup loop that used to fire here is
        // gone. Any rc that survived `delete_volume` will be retired by
        // the next Lineage GC pass.
        Ok(freed_blocks)
    }

    /// List all volumes.
    pub fn list_volumes(&self) -> OnyxResult<Vec<VolumeConfig>> {
        self.meta.list_volumes()
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
            self.live_handles
                .lock()
                .unwrap()
                .push((name.to_string(), alive.clone()));
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
        // arrived between the watermark thread's final tick and now.
        if let Err(e) = self.meta.sync_durable() {
            tracing::error!(
                error = %e,
                "failed to sync_durable at shutdown — forcing dirty recovery on next boot"
            );
            return Ok(());
        }

        // Drive one final reclaim pass. The durability watermark thread has
        // already advanced `durable_seq` to cover every mark_flushed'd seq,
        // but `free_seq_allocation`'s inline reclaim used a stale watermark
        // for the last few seqs in the drain. Re-running `advance_tail` with
        // the now-up-to-date durable_seq lets those seqs release their ring
        // slots so the next boot starts with a clean buffer log.
        if let Some(pool) = self.buffer_pool.as_ref() {
            pool.durable_seq_handle().store(
                pool.max_flushed_seq_handle()
                    .load(std::sync::atomic::Ordering::Acquire),
                std::sync::atomic::Ordering::Release,
            );
            if let Err(e) = pool.advance_tail() {
                tracing::warn!(
                    error = %e,
                    "final advance_tail at shutdown failed — pending entries may persist"
                );
            }
        }

        // Stamp the LV3 superblock with FLAG_CLEAN_SHUTDOWN so the next boot
        // can skip dirty recovery. This is the last persistent act of the
        // engine — by this point flusher has drained, cleanup_tx is idle, and
        // the refcount CF is consistent with the per-volume blockmap CFs.
        if let Some(ref io_engine) = self.io_engine {
            match superblock::read_superblock(io_engine.data_device()) {
                Ok(Some(mut sb)) => {
                    sb.set_clean_shutdown(true);
                    sb.update_crc();
                    if let Err(e) = superblock::write_superblock(io_engine.data_device(), &sb) {
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
    /// This avoids the metadb exclusive directory lock problem: the old engine's
    /// MetaStore Arc is shared with the new engine rather than opening a second one.
    pub fn upgrade_from_meta_only(meta: Arc<MetaStore>, config: &OnyxConfig) -> OnyxResult<Self> {
        let lifecycle = Arc::new(VolumeLifecycleManager::default());
        let metrics = Arc::new(EngineMetrics::default());
        let generation_clock = Self::seed_generation_clock(&meta)?;

        // Data device + IO engine
        let data_path = config.storage.data_device.as_ref().ok_or_else(|| {
            OnyxError::Config("storage.data_device is required for full mode".into())
        })?;
        Self::validate_data_buffer_devices_disjoint(config)?;
        let data_dev = RawDevice::open(data_path)?;
        let device_size = data_dev.size();

        // Validate / format LV3 superblock
        match superblock::read_superblock(&data_dev)? {
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
                data_dev.read_at(&mut block0, 0)?;
                if block0.iter().all(|&b| b == 0) {
                    tracing::info!("fresh LV3 device — formatting (upgrade)");
                    superblock::format_device(&data_dev)?;
                } else {
                    return Err(OnyxError::Config(
                        "LV3 block 0 has data but invalid superblock (magic/CRC/version failed)"
                            .into(),
                    ));
                }
            }
        }

        let io_engine = Self::build_io_engine(data_dev, &config.storage, metrics.clone())?;

        // Space allocator
        let allocator = Arc::new(SpaceAllocator::new_with_hazards(
            device_size,
            config.buffer.shards,
        ));
        allocator.rebuild_from_metadata(&meta)?;

        // Write buffer pool (with shard migration if needed)
        let buffer_pool =
            Self::open_buffer_pool(config, &meta, &lifecycle, &allocator, &io_engine, &metrics)?;

        // LV3 read pool — needed by the flusher's dedup verify-on-hit
        // path. Built before the flusher so the read pool is wired up
        // by the time the dedup workers spawn.
        let read_pool = Self::build_read_pool(config, metrics.clone())?;

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
            flusher.candidate_cache(),
            read_pool.clone(),
        )?);

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
            config.gc.clone(),
        ));

        // Heartbeat writer
        let heartbeat_writer = if config.ha.enabled {
            let hb_dev = RawDevice::open(data_path)?;
            Some(HeartbeatWriter::start(
                hb_dev,
                config.ha.node_id,
                std::time::Duration::from_millis(config.ha.heartbeat_interval_ms),
            ))
        } else {
            None
        };

        tracing::info!("onyx engine upgraded to full mode");

        let watermark = DurabilityWatermarkHandle::start(
            meta.clone(),
            buffer_pool.clone(),
            buffer_pool.max_flushed_seq_handle(),
            buffer_pool.durable_seq_handle(),
            std::time::Duration::from_millis(50),
            config.meta.unlogged_flush_commits,
            config.meta.flush_dirty_pages_threshold,
        );
        let lineage_drain = LineageFreedPbaDrainHandle::start(
            meta.clone(),
            allocator.clone(),
            metrics.clone(),
            std::time::Duration::from_millis(100),
        );

        Ok(Self {
            meta,
            io_engine: Some(io_engine),
            allocator: Some(allocator),
            buffer_pool: Some(buffer_pool),
            flusher: Mutex::new(Some(flusher)),
            gc_runner: Mutex::new(gc_runner),
            dedup_scanner: Mutex::new(dedup_scanner),
            heartbeat_writer: Mutex::new(heartbeat_writer),
            durability_watermark: Mutex::new(Some(watermark)),
            lineage_drain: Mutex::new(Some(lineage_drain)),
            read_pool,
            zone_manager: Some(zone_manager),
            live_handles: Mutex::new(Vec::new()),
            lifecycle,
            metrics,
            generation_clock: AtomicU64::new(generation_clock),
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

    pub fn status_snapshot(&self) -> OnyxResult<EngineStatusSnapshot> {
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
                .filter(|(_, flag)| Arc::strong_count(flag) > 1)
                .count(),
            zone_count: self.zone_manager.as_ref().map(|zm| zm.zone_count()),
            buffer_pending_entries: self.buffer_pool.as_ref().map(|pool| pool.pending_count()),
            buffer_fill_pct: self.buffer_pool.as_ref().map(|pool| pool.fill_percentage()),
            buffer_payload_memory_bytes: self
                .buffer_pool
                .as_ref()
                .map(|pool| pool.payload_memory_bytes()),
            buffer_payload_memory_limit_bytes: self
                .buffer_pool
                .as_ref()
                .map(|pool| pool.payload_memory_limit_bytes()),
            buffer_volatile_payload_memory_bytes: self
                .buffer_pool
                .as_ref()
                .map(|pool| pool.volatile_payload_memory_bytes()),
            buffer_volatile_payload_memory_limit_bytes: self
                .buffer_pool
                .as_ref()
                .map(|pool| pool.volatile_payload_memory_limit_bytes()),
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
