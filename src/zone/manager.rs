use std::sync::Arc;
use std::time::Instant;

use crate::buffer::pool::WriteBufferPool;
use crate::error::OnyxResult;
use crate::io::engine::IoEngine;
use crate::io::read_pool::ReadPool;
use crate::meta::store::MetaStore;
use crate::metrics::EngineMetrics;
use crate::space::allocator::SpaceAllocator;
use crate::space::extent::Extent;
use crate::types::{Lba, VolumeId, ZoneId, BLOCK_SIZE};
use crate::zone::read;

/// Routes IO across LBAs.
///
/// Both reads and writes execute inline on the caller thread:
/// * Writes go straight into `WriteBufferPool::append` — same-zone ordering is
///   preserved by the per-shard append lock inside the pool.
/// * Reads run via `crate::zone::read::execute_read` — the buffer DashMap is
///   lock-free and `MetaStore::get_mapping` is a RocksDB point-get, so no
///   external serialization is needed.
///
/// `zone_count` / `zone_size_blocks` are kept for write-splitting at zone
/// boundaries (so a wide write doesn't span shards on disk) and for metric
/// labelling, but no per-zone worker threads are spawned anymore.
pub struct ZoneManager {
    zone_size_blocks: u64,
    zone_count: u32,
    io_engine: Arc<IoEngine>,
    buffer_pool: Arc<WriteBufferPool>,
    meta: Arc<MetaStore>,
    allocator: Option<Arc<SpaceAllocator>>,
    metrics: Arc<EngineMetrics>,
    /// Optional LV3 read pool. When present, mapped reads dispatch here for
    /// batched io_uring submission + parallel decompression. When absent,
    /// reads fall back to inline `IoEngine::read_blocks` on the caller thread.
    read_pool: Option<Arc<ReadPool>>,
}

impl ZoneManager {
    pub fn new(
        zone_count: u32,
        zone_size_blocks: u64,
        meta: Arc<MetaStore>,
        buffer_pool: Arc<WriteBufferPool>,
        io_engine: Arc<IoEngine>,
    ) -> OnyxResult<Self> {
        Self::new_with_metrics(
            zone_count,
            zone_size_blocks,
            meta,
            buffer_pool,
            io_engine,
            Arc::new(EngineMetrics::default()),
            None,
        )
    }

    pub fn new_with_metrics(
        zone_count: u32,
        zone_size_blocks: u64,
        meta: Arc<MetaStore>,
        buffer_pool: Arc<WriteBufferPool>,
        io_engine: Arc<IoEngine>,
        metrics: Arc<EngineMetrics>,
        allocator: Option<Arc<SpaceAllocator>>,
    ) -> OnyxResult<Self> {
        Self::new_full(
            zone_count,
            zone_size_blocks,
            meta,
            buffer_pool,
            io_engine,
            metrics,
            allocator,
            None,
        )
    }

    /// Full constructor that additionally takes a shared LV3 `ReadPool`. Pass
    /// `Some(pool)` in production (built from `config.storage.read_pool_workers`)
    /// so mapped reads enjoy batched io_uring + parallel decompress; pass
    /// `None` to keep the legacy inline LV3 path.
    pub fn new_full(
        zone_count: u32,
        zone_size_blocks: u64,
        meta: Arc<MetaStore>,
        buffer_pool: Arc<WriteBufferPool>,
        io_engine: Arc<IoEngine>,
        metrics: Arc<EngineMetrics>,
        allocator: Option<Arc<SpaceAllocator>>,
        read_pool: Option<Arc<ReadPool>>,
    ) -> OnyxResult<Self> {
        tracing::info!(
            zone_count,
            zone_size_blocks,
            read_pool_workers = read_pool.as_ref().map(|p| p.worker_count()).unwrap_or(0),
            "zone manager initialised (inline read/write — no zone worker threads)"
        );

        Ok(Self {
            zone_size_blocks,
            zone_count,
            io_engine,
            buffer_pool,
            meta,
            allocator,
            metrics,
            read_pool,
        })
    }

    /// Access the shared engine metrics.
    pub fn metrics(&self) -> &Arc<EngineMetrics> {
        &self.metrics
    }

    /// Determine which zone handles a given LBA
    pub fn zone_for_lba(&self, lba: Lba) -> ZoneId {
        let zone_idx = (lba.0 / self.zone_size_blocks) % self.zone_count as u64;
        ZoneId(zone_idx as u32)
    }

    /// Submit a write IO covering one or more contiguous LBAs.
    /// Automatically splits at zone boundaries to preserve the "same zone = serial" invariant.
    pub fn submit_write(
        &self,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        data: &[u8],
        vol_created_at: u64,
    ) -> OnyxResult<()> {
        let total_start = Instant::now();
        let result = (|| {
            if lba_count == 0 {
                return Ok(());
            }

            let chunks = if lba_count == 0 {
                0
            } else {
                let first_zone = start_lba.0 / self.zone_size_blocks;
                let last_lba = start_lba.0 + lba_count as u64 - 1;
                let last_zone = last_lba / self.zone_size_blocks;
                last_zone.saturating_sub(first_zone) + 1
            };

            let block_size = BLOCK_SIZE as usize;
            let mut remaining_lbas = lba_count as u64;
            let mut current_lba = start_lba.0;
            let mut data_offset = 0usize;

            while remaining_lbas > 0 {
                let zone_end_lba =
                    ((current_lba / self.zone_size_blocks) + 1) * self.zone_size_blocks;
                let lbas_in_this_zone =
                    remaining_lbas.min(zone_end_lba.saturating_sub(current_lba));
                let byte_len = lbas_in_this_zone as usize * block_size;
                let end = data_offset + byte_len;
                self.buffer_pool.append(
                    vol_id,
                    Lba(current_lba),
                    lbas_in_this_zone as u32,
                    &data[data_offset..end],
                    vol_created_at,
                )?;
                current_lba += lbas_in_this_zone;
                remaining_lbas -= lbas_in_this_zone;
                data_offset = end;
            }

            self.metrics
                .zone_write_dispatches
                .fetch_add(chunks, std::sync::atomic::Ordering::Relaxed);
            self.metrics
                .zone_write_lbas
                .fetch_add(lba_count as u64, std::sync::atomic::Ordering::Relaxed);
            if chunks > 1 {
                self.metrics
                    .zone_write_split_ops
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }

            Ok(())
        })();

        self.metrics.zone_submit_write_ns.fetch_add(
            total_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        result
    }

    /// Submit a read IO. Executes inline on the caller thread — no zone-worker
    /// channel hop, no per-zone serialization. Returns `Ok(Some(data))` if
    /// mapped, `Ok(None)` if unmapped, `Err` on failure.
    pub fn submit_read(&self, vol_id: &str, lba: Lba) -> OnyxResult<Option<Vec<u8>>> {
        self.submit_read_with_generation(vol_id, lba, 0)
    }

    pub fn submit_read_with_generation(
        &self,
        vol_id: &str,
        lba: Lba,
        vol_created_at: u64,
    ) -> OnyxResult<Option<Vec<u8>>> {
        self.metrics
            .zone_read_dispatches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        read::execute_read(
            &self.meta,
            &self.buffer_pool,
            &self.io_engine,
            &self.metrics,
            self.read_pool.as_deref(),
            vol_id,
            lba,
            vol_created_at,
        )
    }

    /// Submit a DISCARD (TRIM) request for a range of LBAs.
    ///
    /// Invalidates buffer index entries, deletes blockmap mappings,
    /// decrements refcounts, and frees PBAs with zero refcount.
    /// No LV3 IO is performed — this is purely a metadata operation.
    pub fn submit_discard(&self, vol_id: &str, start_lba: Lba, lba_count: u32) -> OnyxResult<()> {
        if lba_count == 0 {
            return Ok(());
        }

        // Step 1: invalidate buffer index so reads see unmapped immediately
        self.buffer_pool
            .invalidate_lba_range(vol_id, start_lba, lba_count);

        // Step 2: delete blockmap entries + decrement refcounts atomically
        let vol_id_obj = VolumeId(vol_id.to_string());
        let end_lba = Lba(start_lba.0 + lba_count as u64);
        let freed = self
            .meta
            .delete_blockmap_range(&vol_id_obj, start_lba, end_lba)?;

        // Step 3: return freed PBAs to allocator
        if let Some(allocator) = &self.allocator {
            let mut blocks_freed = 0u64;
            for (pba, block_count) in &freed {
                let result = if *block_count <= 1 {
                    allocator.free_one(*pba)
                } else {
                    allocator.free_extent(Extent::new(*pba, *block_count))
                };
                match result {
                    Ok(()) => blocks_freed += *block_count as u64,
                    Err(e) => {
                        tracing::warn!(pba = pba.0, error = %e, "discard: failed to free extent");
                    }
                }
            }
            if blocks_freed > 0 {
                self.metrics
                    .discard_blocks_freed
                    .fetch_add(blocks_freed, std::sync::atomic::Ordering::Relaxed);
            }
        }

        Ok(())
    }

    /// Graceful shutdown — currently a no-op since reads/writes run inline.
    /// Kept on the API surface for callers (engine.rs) that expect to be able
    /// to call it.
    pub fn shutdown(&mut self) -> OnyxResult<()> {
        tracing::info!("zone manager stopped");
        Ok(())
    }

    pub fn zone_count(&self) -> u32 {
        self.zone_count
    }
}

impl Drop for ZoneManager {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
