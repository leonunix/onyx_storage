use std::sync::Arc;

use crate::buffer::pool::WriteBufferPool;
use crate::compress::codec::{create_compressor, Compressor};
use crate::error::OnyxResult;
use crate::io::engine::IoEngine;
use crate::meta::store::MetaStore;
use crate::types::{CompressionAlgo, Lba, VolumeId, ZoneId, BLOCK_SIZE};

/// Single-threaded zone worker. Processes all IO for a specific LBA range.
///
/// Write path: stores raw (uncompressed) data in the buffer pool.
/// Compression is done by the background flusher pipeline.
///
/// Read path: checks buffer (raw) first, then reads compression units from LV3.
pub struct ZoneWorker {
    pub zone_id: ZoneId,
    meta: Arc<MetaStore>,
    pub buffer_pool: Arc<WriteBufferPool>,
    io_engine: Arc<IoEngine>,
    compression_algo: CompressionAlgo,
}

impl ZoneWorker {
    pub fn new(
        zone_id: ZoneId,
        meta: Arc<MetaStore>,
        buffer_pool: Arc<WriteBufferPool>,
        io_engine: Arc<IoEngine>,
        compression_algo: CompressionAlgo,
    ) -> Self {
        Self {
            zone_id,
            meta,
            buffer_pool,
            io_engine,
            compression_algo,
        }
    }

    /// Write raw data to buffer. No compression — flusher handles that.
    pub fn handle_write(&self, vol_id: &str, lba: Lba, data: &[u8]) -> OnyxResult<()> {
        self.buffer_pool.append(vol_id, lba, data)?;
        Ok(())
    }

    /// Read a single 4KB LBA.
    ///
    /// Returns `Ok(Some(data))` if mapped, `Ok(None)` if unmapped, `Err` on failure.
    pub fn handle_read(&self, vol_id: &str, lba: Lba) -> OnyxResult<Option<Vec<u8>>> {
        // 1. Check buffer — raw data, return directly
        if let Some(entry) = self.buffer_pool.lookup(vol_id, lba)? {
            return Ok(Some(entry.payload));
        }

        // 2. Check blockmap
        let vid = VolumeId(vol_id.to_string());
        let mapping = match self.meta.get_mapping(&vid, lba)? {
            Some(m) => m,
            None => return Ok(None),
        };

        // 3. Read compression unit from LV3
        let compressed_data = self
            .io_engine
            .read_blocks(mapping.pba, mapping.unit_compressed_size as usize)?;

        // 4. Verify CRC
        let actual_crc = crc32fast::hash(&compressed_data);
        if actual_crc != mapping.crc32 {
            return Err(crate::error::OnyxError::CrcMismatch {
                expected: mapping.crc32,
                actual: actual_crc,
            });
        }

        // 5. Decompress (or just use raw if uncompressed)
        let decompressed = if mapping.compression == 0 {
            compressed_data
        } else {
            let algo =
                CompressionAlgo::from_u8(mapping.compression).unwrap_or(CompressionAlgo::None);
            let decompressor = create_compressor(algo);
            let mut buf = vec![0u8; mapping.unit_original_size as usize];
            decompressor.decompress(
                &compressed_data,
                &mut buf,
                mapping.unit_original_size as usize,
            )?;
            buf
        };

        // 6. Extract the requested 4KB from the decompressed unit
        let start = mapping.offset_in_unit as usize * BLOCK_SIZE as usize;
        let end = start + BLOCK_SIZE as usize;
        if end > decompressed.len() {
            return Err(crate::error::OnyxError::Compress(format!(
                "decompressed unit too small: {} bytes, need offset {}..{}",
                decompressed.len(),
                start,
                end
            )));
        }

        Ok(Some(decompressed[start..end].to_vec()))
    }
}
