use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::buffer::flush::BufferFlusher;
use crate::buffer::pool::WriteBufferPool;
use crate::config::OnyxConfig;
use crate::dedup::scanner::DedupScanner;
use crate::error::{OnyxError, OnyxResult};
use crate::gc::runner::GcRunner;
use crate::io::device::RawDevice;
use crate::io::engine::IoEngine;
use crate::io::superblock::{self, HeartbeatWriter};
use crate::lifecycle::VolumeLifecycleManager;
use crate::meta::store::MetaStore;
use crate::metrics::{EngineMetrics, EngineMetricsSnapshot, EngineStatusSnapshot};
use crate::space::allocator::SpaceAllocator;
use crate::space::extent::Extent;
use crate::types::{CompressionAlgo, VolumeConfig, VolumeId};
use crate::volume::OnyxVolume;
use crate::zone::manager::ZoneManager;

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
    fn current_time_nanos() -> u64 {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        u64::try_from(nanos).unwrap_or(u64::MAX)
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
                    std::time::Duration::from_secs(30),
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
                            std::time::Duration::from_secs(30),
                        )?);
                        old_pool.attach_metrics(metrics.clone());
                        let old_pending = old_pool.pending_count();
                        if old_pending > 0 {
                            tracing::info!(
                                count = old_pending,
                                "draining unflushed entries before shard migration"
                            );
                        }
                        let temp_hole_map = crate::packer::packer::new_hole_map();
                        let mut temp_flusher = BufferFlusher::start_with_metrics(
                            old_pool.clone(),
                            meta.clone(),
                            lifecycle.clone(),
                            allocator.clone(),
                            io_engine.clone(),
                            &config.flush,
                            temp_hole_map,
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
        let pool = Arc::new(WriteBufferPool::open_with_options(
            buf_dev,
            std::time::Duration::from_micros(config.buffer.group_commit_wait_us),
            config.buffer.shards,
            config.engine.zone_size_blocks,
            std::time::Duration::from_secs(30),
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

    /// Open the engine with full IO capability (data device + buffer + flusher + zones).
    ///
    /// Compression is per-volume (stored in VolumeConfig metadata), not engine-wide.
    pub fn open(config: &OnyxConfig) -> OnyxResult<Self> {
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
                    "LV3 superblock validated"
                );
            }
            None => {
                // Check if the device is fresh (all zeros in block 0)
                let mut block0 = [0u8; 4096];
                data_dev.read_at(&mut block0, 0)?;
                if block0.iter().all(|&b| b == 0) {
                    tracing::info!("fresh LV3 device — formatting");
                    superblock::format_device(&data_dev)?;
                } else {
                    return Err(OnyxError::Config(
                        "LV3 block 0 has data but invalid superblock (magic/CRC/version failed)"
                            .into(),
                    ));
                }
            }
        }

        let io_engine = Arc::new(IoEngine::new(data_dev, config.storage.use_hugepages));

        // 3. Space allocator
        let allocator = Arc::new(SpaceAllocator::new(device_size, config.buffer.shards));
        allocator.rebuild_from_metadata(&meta)?;

        // 4. Write buffer pool (with shard migration if needed)
        let buffer_pool =
            Self::open_buffer_pool(config, &meta, &lifecycle, &allocator, &io_engine, &metrics)?;

        // 6. Shared hole map (GC → Packer)
        let hole_map = crate::packer::packer::new_hole_map();

        // 7. Background flusher (owns the Packer, which reads from hole_map)
        let flusher = BufferFlusher::start_with_metrics(
            buffer_pool.clone(),
            meta.clone(),
            lifecycle.clone(),
            allocator.clone(),
            io_engine.clone(),
            &config.flush,
            hole_map.clone(),
            &config.dedup,
            metrics.clone(),
        );

        // 8. Zone manager
        let zone_manager = Arc::new(ZoneManager::new_with_metrics(
            config.engine.zone_count,
            config.engine.zone_size_blocks,
            meta.clone(),
            buffer_pool.clone(),
            io_engine.clone(),
            metrics.clone(),
            Some(allocator.clone()),
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
                config.dedup.clone(),
            ))
        } else {
            None
        };

        // 10. GC runner (after flusher; rewrites dead blocks back to buffer)
        let gc_runner = if config.gc.enabled {
            Some(GcRunner::start_with_metrics(
                metrics.clone(),
                meta.clone(),
                io_engine.clone(),
                buffer_pool.clone(),
                lifecycle.clone(),
                config.gc.clone(),
            ))
        } else {
            None
        };

        // 11. Heartbeat writer (after all other subsystems)
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

        tracing::info!("onyx engine opened (full mode)");

        Ok(Self {
            meta,
            io_engine: Some(io_engine),
            allocator: Some(allocator),
            buffer_pool: Some(buffer_pool),
            flusher: Mutex::new(Some(flusher)),
            gc_runner: Mutex::new(gc_runner),
            dedup_scanner: Mutex::new(dedup_scanner),
            heartbeat_writer: Mutex::new(heartbeat_writer),
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

            let freed = self.meta.delete_volume(&vol_id)?;
            let freed_blocks: usize = freed.iter().map(|(_, blocks)| *blocks as usize).sum();

            if let Some(allocator) = &self.allocator {
                for (pba, block_count) in &freed {
                    let result = if *block_count <= 1 {
                        allocator.free_one(*pba)
                    } else {
                        allocator.free_extent(Extent::new(*pba, *block_count))
                    };
                    if let Err(e) = result {
                        tracing::warn!(
                            pba = pba.0, blocks = block_count,
                            error = %e, "failed to free extent to allocator during volume delete"
                        );
                    }
                }
            }

            self.invalidate_live_handles(name);
            self.metrics.remove_volume_metrics(name);
            self.metrics
                .volume_delete_ops
                .fetch_add(1, Ordering::Relaxed);

            tracing::info!(
                name,
                generation = deleted_generation,
                freed_extents = freed.len(),
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

        let orphaned = self.meta.cleanup_orphaned_refcounts()?;
        if let Some(allocator) = &self.allocator {
            for (pba, _refcount) in &orphaned {
                if let Err(e) = allocator.free_one(*pba) {
                    tracing::warn!(
                        name,
                        generation = deleted_generation,
                        pba = pba.0,
                        error = %e,
                        "failed to free orphaned packed-slot PBA after volume delete"
                    );
                }
            }
        }
        if !orphaned.is_empty() {
            tracing::warn!(
                name,
                generation = deleted_generation,
                orphaned_pbas = orphaned.len(),
                "cleaned orphaned refcounts after volume delete"
            );
        }

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

            let alive = Arc::new(AtomicBool::new(true));
            self.live_handles
                .lock()
                .unwrap()
                .push((name.to_string(), alive.clone()));
            self.metrics.volume_open_ops.fetch_add(1, Ordering::Relaxed);

            let vol_lock = self.lifecycle.get_lock(name);
            Ok(OnyxVolume::new(
                name.to_string(),
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

        // Then stop flusher (drains pending flushes)
        if let Some(mut flusher) = self.flusher.lock().unwrap().take() {
            flusher.stop();
        }

        // Drain per-lane allocator caches back to the global free list
        if let Some(ref allocator) = self.allocator {
            allocator.drain_lane_caches();
        }

        // Zone manager shutdown is handled by Drop (it sends Shutdown to all workers)
        // We can't call shutdown(&mut self) through Arc, but Drop handles it.

        tracing::info!("onyx engine shut down");
        Ok(())
    }

    /// Upgrade from a meta-only engine to full mode, reusing the existing MetaStore.
    ///
    /// This avoids the RocksDB exclusive directory lock problem: the old engine's
    /// MetaStore Arc is shared with the new engine rather than opening a second one.
    pub fn upgrade_from_meta_only(meta: Arc<MetaStore>, config: &OnyxConfig) -> OnyxResult<Self> {
        let lifecycle = Arc::new(VolumeLifecycleManager::default());
        let metrics = Arc::new(EngineMetrics::default());
        let generation_clock = Self::seed_generation_clock(&meta)?;

        // Data device + IO engine
        let data_path = config.storage.data_device.as_ref().ok_or_else(|| {
            OnyxError::Config("storage.data_device is required for full mode".into())
        })?;
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

        let io_engine = Arc::new(IoEngine::new(data_dev, config.storage.use_hugepages));

        // Space allocator
        let allocator = Arc::new(SpaceAllocator::new(device_size, config.buffer.shards));
        allocator.rebuild_from_metadata(&meta)?;

        // Write buffer pool (with shard migration if needed)
        let buffer_pool =
            Self::open_buffer_pool(config, &meta, &lifecycle, &allocator, &io_engine, &metrics)?;

        // Shared hole map
        let hole_map = crate::packer::packer::new_hole_map();

        // Background flusher
        let flusher = BufferFlusher::start_with_metrics(
            buffer_pool.clone(),
            meta.clone(),
            lifecycle.clone(),
            allocator.clone(),
            io_engine.clone(),
            &config.flush,
            hole_map.clone(),
            &config.dedup,
            metrics.clone(),
        );

        // Zone manager
        let zone_manager = Arc::new(ZoneManager::new_with_metrics(
            config.engine.zone_count,
            config.engine.zone_size_blocks,
            meta.clone(),
            buffer_pool.clone(),
            io_engine.clone(),
            metrics.clone(),
            Some(allocator.clone()),
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
                config.dedup.clone(),
            ))
        } else {
            None
        };

        // GC runner
        let gc_runner = if config.gc.enabled {
            Some(GcRunner::start_with_metrics(
                metrics.clone(),
                meta.clone(),
                io_engine.clone(),
                buffer_pool.clone(),
                lifecycle.clone(),
                config.gc.clone(),
            ))
        } else {
            None
        };

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

        Ok(Self {
            meta,
            io_engine: Some(io_engine),
            allocator: Some(allocator),
            buffer_pool: Some(buffer_pool),
            flusher: Mutex::new(Some(flusher)),
            gc_runner: Mutex::new(gc_runner),
            dedup_scanner: Mutex::new(dedup_scanner),
            heartbeat_writer: Mutex::new(heartbeat_writer),
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
