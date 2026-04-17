use std::sync::Arc;

use crate::buffer::pool::WriteBufferPool;
use crate::compress::codec::create_compressor;
use crate::error::{OnyxError, OnyxResult};
use crate::io::engine::IoEngine;
use crate::io::read_pool::ReadPool;
use crate::meta::store::MetaStore;
use crate::metrics::EngineMetrics;
use crate::types::{CompressionAlgo, Lba, VolumeId, BLOCK_SIZE};

/// Execute a read for one 4KB LBA.
///
/// Returns `Ok(Some(data))` if the LBA is mapped (either in the buffer or in
/// the LV3 blockmap), `Ok(None)` if unmapped, `Err(_)` on real IO failure.
///
/// Buffer hits and unmapped reads are zero-IO and stay inline on the caller
/// thread for minimum latency. Mapped LV3 reads dispatch to `read_pool` when
/// supplied so their disk IO is batched into io_uring and decompression runs
/// in parallel across worker threads. Without a pool, the LV3 path falls back
/// to inline `IoEngine::read_blocks` on the caller thread.
pub fn execute_read(
    meta: &MetaStore,
    buffer_pool: &WriteBufferPool,
    io_engine: &IoEngine,
    metrics: &EngineMetrics,
    read_pool: Option<&ReadPool>,
    vol_id: &str,
    lba: Lba,
    vol_created_at: u64,
) -> OnyxResult<Option<Vec<u8>>> {
    use std::sync::atomic::Ordering;

    // 1. Buffer index — lock-free DashMap lookup, zero IO on hit.
    if let Some(pending) = buffer_pool.lookup(vol_id, lba)? {
        if vol_created_at == 0 || pending.vol_created_at == vol_created_at {
            if let Some(ref payload) = pending.payload {
                let offset = (lba.0 - pending.start_lba.0) as usize * BLOCK_SIZE as usize;
                let end = offset + BLOCK_SIZE as usize;
                if end <= payload.len() {
                    metrics.read_buffer_hits.fetch_add(1, Ordering::Relaxed);
                    metrics.buffer_read_ops.fetch_add(1, Ordering::Relaxed);
                    metrics
                        .buffer_read_bytes
                        .fetch_add(BLOCK_SIZE as u64, Ordering::Relaxed);
                    return Ok(Some(payload[offset..end].to_vec()));
                }
            }
        }
    }

    // 2. Persistent blockmap — RocksDB point read, no IO yet.
    let vid = VolumeId(vol_id.to_string());
    let mapping = match meta.get_mapping(&vid, lba)? {
        Some(m) => m,
        None => {
            metrics.read_unmapped.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
    };

    // 3. LV3 read + CRC + decompress. Goes through the read pool when one is
    //    attached so the disk IO joins a batched io_uring submit and the
    //    decompression runs on the worker, freeing the caller (e.g. ublk
    //    queue thread) to dispatch the next request.
    if let Some(pool) = read_pool {
        return pool.submit_read(mapping).map(Some);
    }

    // Fallback: inline LV3 read on the caller thread (used when the pool is
    // disabled in config or in tests that wire `IoEngine` directly).
    inline_lv3_read(io_engine, metrics, mapping)
}

fn inline_lv3_read(
    io_engine: &IoEngine,
    metrics: &EngineMetrics,
    mapping: crate::meta::schema::BlockmapValue,
) -> OnyxResult<Option<Vec<u8>>> {
    let read_size = mapping.compressed_read_size(BLOCK_SIZE as usize);
    let raw = io_engine.read_blocks(mapping.pba, read_size)?;
    extract_lba_from_compressed(&raw, &mapping, metrics).map(Some)
}

/// Shared post-IO pipeline: slice out the compressed unit, verify CRC,
/// decompress, then carve out the requested 4 KB LBA. Used by both the inline
/// LV3 read path and the `ReadPool` worker so the two paths can never drift.
pub(crate) fn extract_lba_from_compressed(
    raw: &[u8],
    mapping: &crate::meta::schema::BlockmapValue,
    metrics: &EngineMetrics,
) -> OnyxResult<Vec<u8>> {
    use std::sync::atomic::Ordering;

    let (start, end) = mapping.compressed_slice_range();
    if end > raw.len() {
        return Err(OnyxError::Compress(format!(
            "fragment out of bounds: range={start}..{end} > buffer_len={}",
            raw.len()
        )));
    }
    let compressed = &raw[start..end];

    let actual_crc = crc32fast::hash(compressed);
    if actual_crc != mapping.crc32 {
        metrics.read_crc_errors.fetch_add(1, Ordering::Relaxed);
        return Err(OnyxError::CrcMismatch {
            expected: mapping.crc32,
            actual: actual_crc,
        });
    }

    let lba_off = mapping.offset_in_unit as usize * BLOCK_SIZE as usize;
    let lba_end = lba_off + BLOCK_SIZE as usize;

    let result = if mapping.is_uncompressed() {
        if lba_end > compressed.len() {
            return Err(OnyxError::Compress(format!(
                "uncompressed unit too small: {} bytes, need {lba_off}..{lba_end}",
                compressed.len()
            )));
        }
        compressed[lba_off..lba_end].to_vec()
    } else {
        let algo = CompressionAlgo::from_u8(mapping.compression).unwrap_or(CompressionAlgo::None);
        let codec = create_compressor(algo);
        let mut decompressed = vec![0u8; mapping.unit_original_size as usize];
        if let Err(err) =
            codec.decompress(compressed, &mut decompressed, mapping.unit_original_size as usize)
        {
            metrics.read_decompress_errors.fetch_add(1, Ordering::Relaxed);
            return Err(err);
        }
        if lba_end > decompressed.len() {
            return Err(OnyxError::Compress(format!(
                "decompressed unit too small: {} bytes, need {lba_off}..{lba_end}",
                decompressed.len()
            )));
        }
        decompressed[lba_off..lba_end].to_vec()
    };

    metrics.read_lv3_hits.fetch_add(1, Ordering::Relaxed);
    Ok(result)
}

/// Convenience wrapper that takes `Arc`s and an optional pool — used by
/// `ZoneManager` and friends that already hold them in their own state.
pub fn execute_read_arc(
    meta: &Arc<MetaStore>,
    buffer_pool: &Arc<WriteBufferPool>,
    io_engine: &Arc<IoEngine>,
    metrics: &Arc<EngineMetrics>,
    read_pool: Option<&Arc<ReadPool>>,
    vol_id: &str,
    lba: Lba,
    vol_created_at: u64,
) -> OnyxResult<Option<Vec<u8>>> {
    execute_read(
        meta,
        buffer_pool,
        io_engine,
        metrics,
        read_pool.map(|p| p.as_ref()),
        vol_id,
        lba,
        vol_created_at,
    )
}
