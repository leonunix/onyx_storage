use std::sync::{Arc, Mutex};

use crate::buffer::flush::BufferFlusher;
use crate::buffer::pool::WriteBufferPool;
use crate::config::OnyxConfig;
use crate::error::{OnyxError, OnyxResult};
use crate::io::device::RawDevice;
use crate::io::engine::IoEngine;
use crate::meta::store::MetaStore;
use crate::space::allocator::SpaceAllocator;
use crate::types::{CompressionAlgo, VolumeConfig, VolumeId};
use crate::volume::OnyxVolume;
use crate::zone::manager::ZoneManager;

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
    zone_manager: Option<Arc<ZoneManager>>,
    config: OnyxConfig,
    shutdown_done: Mutex<bool>,
}

impl OnyxEngine {
    /// Open the engine with full IO capability (data device + buffer + flusher + zones).
    ///
    /// The `compression` parameter sets the engine-wide compression algorithm used
    /// by the flusher pipeline and zone workers.
    pub fn open(config: &OnyxConfig, compression: CompressionAlgo) -> OnyxResult<Self> {
        // 1. MetaStore
        let meta = Arc::new(MetaStore::open(&config.meta)?);

        // 2. Data device + IO engine
        let data_dev = RawDevice::open(&config.storage.data_device)?;
        let device_size = data_dev.size();
        let io_engine = Arc::new(IoEngine::new(data_dev, config.storage.use_hugepages));

        // 3. Space allocator
        let allocator = Arc::new(SpaceAllocator::new(device_size));
        allocator.rebuild_from_metadata(&meta)?;

        // 4. Write buffer pool
        let buf_dev = RawDevice::open(&config.buffer.device)?;
        let buffer_pool = Arc::new(WriteBufferPool::open(buf_dev)?);

        // 5. Recover unflushed entries
        let unflushed = buffer_pool.recover()?;
        if !unflushed.is_empty() {
            tracing::info!(count = unflushed.len(), "replaying unflushed buffer entries");
        }

        // 6. Background flusher
        let flusher = BufferFlusher::start(
            buffer_pool.clone(),
            meta.clone(),
            allocator.clone(),
            io_engine.clone(),
            compression,
            &config.flush,
        );

        // 7. Zone manager
        let zone_manager = Arc::new(ZoneManager::new(
            config.engine.zone_count,
            config.engine.zone_size_blocks,
            meta.clone(),
            buffer_pool.clone(),
            io_engine.clone(),
            compression,
        )?);

        tracing::info!("onyx engine opened (full mode)");

        Ok(Self {
            meta,
            io_engine: Some(io_engine),
            allocator: Some(allocator),
            buffer_pool: Some(buffer_pool),
            flusher: Mutex::new(Some(flusher)),
            zone_manager: Some(zone_manager),
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

        tracing::info!("onyx engine opened (meta-only mode)");

        Ok(Self {
            meta,
            io_engine: None,
            allocator: None,
            buffer_pool: None,
            flusher: Mutex::new(None),
            zone_manager: None,
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
        let vol = VolumeConfig {
            id: VolumeId(name.to_string()),
            size_bytes,
            block_size: 4096,
            compression,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            zone_count: self.config.engine.zone_count,
        };
        self.meta.put_volume(&vol)?;
        tracing::info!(name, size_bytes, "volume created");
        Ok(())
    }

    /// Delete a volume and free its physical blocks.
    pub fn delete_volume(&self, name: &str) -> OnyxResult<usize> {
        let freed = self.meta.delete_volume(&VolumeId(name.to_string()))?;
        let count = freed.len();
        tracing::info!(name, freed_pbas = count, "volume deleted");
        Ok(count)
    }

    /// List all volumes.
    pub fn list_volumes(&self) -> OnyxResult<Vec<VolumeConfig>> {
        self.meta.list_volumes()
    }

    /// Open a volume for IO. Requires full engine mode.
    pub fn open_volume(&self, name: &str) -> OnyxResult<OnyxVolume> {
        let zm = self.zone_manager.as_ref().ok_or_else(|| {
            OnyxError::Config("cannot open volume in meta-only mode".into())
        })?;

        let vol_id = VolumeId(name.to_string());
        let vol_config = self
            .meta
            .get_volume(&vol_id)?
            .ok_or_else(|| OnyxError::VolumeNotFound(name.to_string()))?;

        Ok(OnyxVolume::new(
            name.to_string(),
            vol_config.size_bytes,
            zm.clone(),
        ))
    }

    /// Graceful shutdown: stop flusher, then zone manager.
    pub fn shutdown(&self) -> OnyxResult<()> {
        let mut done = self.shutdown_done.lock().unwrap();
        if *done {
            return Ok(());
        }
        *done = true;

        // Stop flusher first (drains pending flushes)
        if let Some(mut flusher) = self.flusher.lock().unwrap().take() {
            flusher.stop();
        }

        // Zone manager shutdown is handled by Drop (it sends Shutdown to all workers)
        // We can't call shutdown(&mut self) through Arc, but Drop handles it.

        tracing::info!("onyx engine shut down");
        Ok(())
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
}

impl Drop for OnyxEngine {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
