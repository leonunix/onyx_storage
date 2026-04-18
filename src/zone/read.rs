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

/// Decoded compression-unit payload. Either a borrowed slice of the raw LV3
/// read buffer (uncompressed case) or an owned buffer produced by the codec
/// (compressed case). Callers slice out individual LBAs by
/// `offset_in_unit * BLOCK_SIZE`.
#[derive(Debug)]
pub(crate) enum UnitPayload<'a> {
    Borrowed(&'a [u8]),
    Owned(Vec<u8>),
}

impl<'a> UnitPayload<'a> {
    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            UnitPayload::Borrowed(s) => s,
            UnitPayload::Owned(v) => v.as_slice(),
        }
    }

    pub(crate) fn into_owned(self) -> Vec<u8> {
        match self {
            UnitPayload::Borrowed(s) => s.to_vec(),
            UnitPayload::Owned(v) => v,
        }
    }
}

/// Verify CRC and decompress a compression unit. Returns the full unit payload
/// (`unit_original_size` bytes). Shared by the inline LV3 path, the `ReadPool`
/// worker, and the vectorized batch read path so they cannot drift.
///
/// Per-unit metrics (`read_crc_errors`, `read_decompress_errors`) live here.
/// Per-LBA metrics stay with the caller so that callers serving multiple LBAs
/// from one unit bump counters the right number of times.
pub(crate) fn decode_unit<'a>(
    raw: &'a [u8],
    mapping: &crate::meta::schema::BlockmapValue,
    metrics: &EngineMetrics,
) -> OnyxResult<UnitPayload<'a>> {
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

    if mapping.is_uncompressed() {
        let needed = mapping.unit_original_size as usize;
        if needed > compressed.len() {
            return Err(OnyxError::Compress(format!(
                "uncompressed unit too small: {} bytes, need {needed}",
                compressed.len()
            )));
        }
        Ok(UnitPayload::Borrowed(&compressed[..needed]))
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
        Ok(UnitPayload::Owned(decompressed))
    }
}

/// Pure slice helper — extract the 4 KB LBA at `offset_in_unit` from a decoded
/// unit payload. Returns an error if the offset runs past the payload.
pub(crate) fn slice_lba<'a>(
    payload: &'a UnitPayload<'a>,
    offset_in_unit: u16,
) -> OnyxResult<&'a [u8]> {
    let bs = BLOCK_SIZE as usize;
    let lba_off = offset_in_unit as usize * bs;
    let lba_end = lba_off + bs;
    let buf = payload.as_slice();
    if lba_end > buf.len() {
        return Err(OnyxError::Compress(format!(
            "unit payload too small: {} bytes, need {lba_off}..{lba_end}",
            buf.len()
        )));
    }
    Ok(&buf[lba_off..lba_end])
}

/// Shared post-IO pipeline: decode the unit then carve out one 4 KB LBA.
/// Thin wrapper over `decode_unit` + `slice_lba` used by the inline LV3 path
/// and the per-LBA `ReadPool` entry point.
pub(crate) fn extract_lba_from_compressed(
    raw: &[u8],
    mapping: &crate::meta::schema::BlockmapValue,
    metrics: &EngineMetrics,
) -> OnyxResult<Vec<u8>> {
    use std::sync::atomic::Ordering;

    let payload = decode_unit(raw, mapping, metrics)?;
    let lba = slice_lba(&payload, mapping.offset_in_unit)?.to_vec();

    metrics.read_lv3_hits.fetch_add(1, Ordering::Relaxed);
    metrics
        .lv3_read_decompressed_bytes
        .fetch_add(BLOCK_SIZE as u64, Ordering::Relaxed);
    Ok(lba)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compress::codec::create_compressor;
    use crate::meta::schema::BlockmapValue;
    use crate::types::{CompressionAlgo, Pba};

    fn make_mapping(
        pba: u64,
        compression: u8,
        unit_compressed_size: u32,
        unit_original_size: u32,
        unit_lba_count: u16,
        offset_in_unit: u16,
        crc32: u32,
        slot_offset: u16,
    ) -> BlockmapValue {
        BlockmapValue {
            pba: Pba(pba),
            compression,
            unit_compressed_size,
            unit_original_size,
            unit_lba_count,
            offset_in_unit,
            crc32,
            slot_offset,
            flags: 0,
        }
    }

    /// Uncompressed unit: `decode_unit` returns a borrowed slice into `raw`,
    /// and `slice_lba` picks out each LBA by offset.
    #[test]
    fn decode_unit_uncompressed_unpacked() {
        let mut raw = vec![0u8; BLOCK_SIZE as usize * 4];
        for (i, chunk) in raw.chunks_mut(BLOCK_SIZE as usize).enumerate() {
            chunk.fill((i + 1) as u8);
        }
        let crc = crc32fast::hash(&raw);
        let mapping = make_mapping(
            0,
            CompressionAlgo::None.to_u8(),
            raw.len() as u32,
            raw.len() as u32,
            4,
            0,
            crc,
            0,
        );
        let metrics = EngineMetrics::default();
        let payload = decode_unit(&raw, &mapping, &metrics).unwrap();
        assert!(
            matches!(payload, UnitPayload::Borrowed(_)),
            "uncompressed path must borrow from raw buffer"
        );
        for i in 0..4u16 {
            let lba = slice_lba(&payload, i).unwrap();
            assert_eq!(lba, &[(i + 1) as u8; BLOCK_SIZE as usize]);
        }
    }

    /// LZ4-compressed unit: every LBA in the unit must decode back to the
    /// original block. This exercises the coalescing path's invariant — one
    /// decode fans out to N LBAs.
    #[test]
    fn decode_unit_lz4_fans_out_all_lbas() {
        let lba_count = 8u16;
        let unit_original_size = (lba_count as usize) * BLOCK_SIZE as usize;
        let mut original = vec![0u8; unit_original_size];
        for (i, chunk) in original.chunks_mut(BLOCK_SIZE as usize).enumerate() {
            chunk.fill(0xA0 | i as u8);
        }
        let codec = create_compressor(CompressionAlgo::Lz4);
        let mut compressed = vec![0u8; unit_original_size + 64];
        let csize = codec.compress(&original, &mut compressed).unwrap();
        compressed.truncate(csize);
        let crc = crc32fast::hash(&compressed);

        // Raw buffer is compressed bytes rounded up to block_size for O_DIRECT.
        let read_size = ((csize + BLOCK_SIZE as usize - 1) / BLOCK_SIZE as usize)
            * BLOCK_SIZE as usize;
        let mut raw = vec![0u8; read_size];
        raw[..csize].copy_from_slice(&compressed);

        let mapping = make_mapping(
            1,
            CompressionAlgo::Lz4.to_u8(),
            csize as u32,
            unit_original_size as u32,
            lba_count,
            0,
            crc,
            0,
        );
        let metrics = EngineMetrics::default();
        let payload = decode_unit(&raw, &mapping, &metrics).unwrap();
        assert!(matches!(payload, UnitPayload::Owned(_)));
        for i in 0..lba_count {
            let lba = slice_lba(&payload, i).unwrap();
            assert_eq!(lba, &[0xA0 | i as u8; BLOCK_SIZE as usize]);
        }
    }

    /// Packed fragment: `slot_offset > 0` means `decode_unit` must honor
    /// `compressed_slice_range` — it slices compressed bytes out of the
    /// middle of a shared 4 KB physical slot.
    #[test]
    fn decode_unit_packed_fragment_uses_slot_offset() {
        let payload_a = vec![0x11u8; 1024];
        let payload_b = vec![0x22u8; 1024];
        let mut slot = vec![0u8; BLOCK_SIZE as usize];
        slot[..1024].copy_from_slice(&payload_a);
        slot[1024..2048].copy_from_slice(&payload_b);
        let crc_b = crc32fast::hash(&payload_b);

        let mapping = make_mapping(
            7,
            CompressionAlgo::None.to_u8(),
            1024,
            1024,
            1,
            0,
            crc_b,
            1024, // slot_offset — second fragment
        );
        let metrics = EngineMetrics::default();
        let payload = decode_unit(&slot, &mapping, &metrics).unwrap();
        assert_eq!(payload.as_slice(), payload_b.as_slice());
    }

    #[test]
    fn decode_unit_crc_mismatch_returns_error_and_bumps_metric() {
        let raw = vec![0u8; BLOCK_SIZE as usize];
        let bad_crc = crc32fast::hash(&raw).wrapping_add(1);
        let mapping = make_mapping(
            0,
            CompressionAlgo::None.to_u8(),
            BLOCK_SIZE,
            BLOCK_SIZE,
            1,
            0,
            bad_crc,
            0,
        );
        let metrics = EngineMetrics::default();
        let err = decode_unit(&raw, &mapping, &metrics).unwrap_err();
        assert!(matches!(err, OnyxError::CrcMismatch { .. }));
        assert_eq!(
            metrics
                .read_crc_errors
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn slice_lba_rejects_out_of_range_offset() {
        let payload = UnitPayload::Owned(vec![0u8; BLOCK_SIZE as usize]);
        assert!(slice_lba(&payload, 0).is_ok());
        // offset_in_unit = 1 requires 2 × BLOCK_SIZE of payload.
        assert!(slice_lba(&payload, 1).is_err());
    }
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
