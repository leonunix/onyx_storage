use std::sync::Arc;
use std::time::Instant;

use crate::buffer::pool::WriteBufferPool;
use crate::compress::codec::create_compressor;
use crate::error::OnyxResult;
use crate::io::engine::IoEngine;
use crate::meta::store::MetaStore;
use crate::metrics::EngineMetrics;
use crate::types::{CompressionAlgo, Lba, VolumeId, ZoneId, BLOCK_SIZE};

/// Single-threaded zone worker.
///
/// Write path: stores raw data in variable-length buffer entries (no compression).
/// Read path: checks buffer first (O(1)), then reads compression units from LV3.
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
        // 1. Check buffer — may be part of a multi-LBA entry
        if let Some(pending) = self.buffer_pool.lookup(vol_id, lba)? {
            if vol_created_at == 0 || pending.vol_created_at == vol_created_at {
                if let Some(ref payload) = pending.payload {
                    let offset = (lba.0 - pending.start_lba.0) as usize * BLOCK_SIZE as usize;
                    let end = offset + BLOCK_SIZE as usize;
                    if end <= payload.len() {
                        self.metrics
                            .read_buffer_hits
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        self.metrics
                            .buffer_read_ops
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        self.metrics
                            .buffer_read_bytes
                            .fetch_add(BLOCK_SIZE as u64, std::sync::atomic::Ordering::Relaxed);
                        return Ok(Some(payload[offset..end].to_vec()));
                    }
                }
            }
        }

        // 2. Check blockmap
        let vid = VolumeId(vol_id.to_string());
        let mapping = match self.meta.get_mapping(&vid, lba)? {
            Some(m) => m,
            None => {
                self.metrics
                    .read_unmapped
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Ok(None);
            }
        };

        // 3. Read compression unit from LV3
        let compressed_data = if mapping.slot_offset > 0 {
            let slot_data = self
                .io_engine
                .read_blocks(mapping.pba, BLOCK_SIZE as usize)?;
            let start = mapping.slot_offset as usize;
            let end = start + mapping.unit_compressed_size as usize;
            if end > slot_data.len() {
                return Err(crate::error::OnyxError::Compress(format!(
                    "packed fragment out of bounds: slot_offset={} + size={} > slot_len={}",
                    start,
                    mapping.unit_compressed_size,
                    slot_data.len()
                )));
            }
            slot_data[start..end].to_vec()
        } else {
            self.io_engine
                .read_blocks(mapping.pba, mapping.unit_compressed_size as usize)?
        };

        // 4. Verify CRC
        let actual_crc = crc32fast::hash(&compressed_data);
        if actual_crc != mapping.crc32 {
            self.metrics
                .read_crc_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Err(crate::error::OnyxError::CrcMismatch {
                expected: mapping.crc32,
                actual: actual_crc,
            });
        }

        // 5. Decompress
        let decompressed = if mapping.compression == 0 {
            compressed_data
        } else {
            let algo =
                CompressionAlgo::from_u8(mapping.compression).unwrap_or(CompressionAlgo::None);
            let decompressor = create_compressor(algo);
            let mut buf = vec![0u8; mapping.unit_original_size as usize];
            if let Err(err) = decompressor.decompress(
                &compressed_data,
                &mut buf,
                mapping.unit_original_size as usize,
            ) {
                self.metrics
                    .read_decompress_errors
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err(err);
            }
            buf
        };

        // 6. Extract the requested 4KB
        let start = mapping.offset_in_unit as usize * BLOCK_SIZE as usize;
        let end = start + BLOCK_SIZE as usize;
        if end > decompressed.len() {
            return Err(crate::error::OnyxError::Compress(format!(
                "decompressed unit too small: {} bytes, need {}..{}",
                decompressed.len(),
                start,
                end
            )));
        }

        self.metrics
            .read_lv3_hits
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(Some(decompressed[start..end].to_vec()))
    }

    /// Read a single 4KB LBA.
    pub fn handle_read(&self, vol_id: &str, lba: Lba) -> OnyxResult<Option<Vec<u8>>> {
        self.handle_read_with_generation(vol_id, lba, 0)
    }
}
