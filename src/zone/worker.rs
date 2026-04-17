use std::sync::Arc;
use std::time::Instant;

use crate::buffer::pool::WriteBufferPool;
use crate::error::OnyxResult;
use crate::io::engine::IoEngine;
use crate::meta::store::MetaStore;
use crate::metrics::EngineMetrics;
use crate::types::{Lba, ZoneId};
use crate::zone::read;

/// Per-zone worker.
///
/// Historically routed both reads and writes; today writes go directly through
/// `ZoneManager::submit_write` → `WriteBufferPool::append`, and the read path
/// is shared via `crate::zone::read::execute_read`. This type is kept primarily
/// for tests that exercise the read pipeline directly without spinning up a
/// full `ZoneManager`.
pub struct ZoneWorker {
    pub zone_id: ZoneId,
    meta: Arc<MetaStore>,
    pub buffer_pool: Arc<WriteBufferPool>,
    io_engine: Arc<IoEngine>,
    metrics: Arc<EngineMetrics>,
}

impl ZoneWorker {
    fn elapsed_ns(start: Instant) -> u64 {
        start.elapsed().as_nanos().min(u64::MAX as u128) as u64
    }

    pub fn new(
        zone_id: ZoneId,
        meta: Arc<MetaStore>,
        buffer_pool: Arc<WriteBufferPool>,
        io_engine: Arc<IoEngine>,
    ) -> Self {
        Self::new_with_metrics(
            zone_id,
            meta,
            buffer_pool,
            io_engine,
            Arc::new(EngineMetrics::default()),
        )
    }

    pub fn new_with_metrics(
        zone_id: ZoneId,
        meta: Arc<MetaStore>,
        buffer_pool: Arc<WriteBufferPool>,
        io_engine: Arc<IoEngine>,
        metrics: Arc<EngineMetrics>,
    ) -> Self {
        Self {
            zone_id,
            meta,
            buffer_pool,
            io_engine,
            metrics,
        }
    }

    pub(crate) fn record_write_ns(&self, start: Instant) {
        self.metrics.zone_worker_write_ns.fetch_add(
            Self::elapsed_ns(start),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// Write raw data covering one or more contiguous LBAs.
    /// `data.len()` must equal `lba_count * BLOCK_SIZE`.
    pub fn handle_write(
        &self,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        data: &[u8],
        vol_created_at: u64,
    ) -> OnyxResult<()> {
        self.buffer_pool
            .append(vol_id, start_lba, lba_count, data, vol_created_at)?;
        Ok(())
    }

    pub fn handle_read_with_generation(
        &self,
        vol_id: &str,
        lba: Lba,
        vol_created_at: u64,
    ) -> OnyxResult<Option<Vec<u8>>> {
        // Inline LV3 path for tests / direct callers. Production reads should
        // go through `ZoneManager::submit_read`, which routes mapped reads to
        // the ReadPool when one is attached.
        read::execute_read(
            &self.meta,
            &self.buffer_pool,
            &self.io_engine,
            &self.metrics,
            None,
            vol_id,
            lba,
            vol_created_at,
        )
    }

    /// Read a single 4KB LBA.
    pub fn handle_read(&self, vol_id: &str, lba: Lba) -> OnyxResult<Option<Vec<u8>>> {
        self.handle_read_with_generation(vol_id, lba, 0)
    }
}
