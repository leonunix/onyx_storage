use crate::buffer::pool::WriteBufferPool;
use crate::compress::codec::create_compressor;
use crate::error::OnyxResult;
use crate::gc::scanner::GcCandidate;
use crate::io::engine::IoEngine;
use crate::lifecycle::VolumeLifecycleManager;
use crate::meta::store::MetaStore;
use crate::types::{CompressionAlgo, BLOCK_SIZE};

/// Rewrite live blocks from a GC candidate back into the buffer pool.
///
/// The flusher pipeline will re-compress and write them to new PBAs,
/// which naturally decrements the old PBA's refcount to 0 and frees it.
///
/// Returns the number of blocks successfully rewritten.
pub fn rewrite_candidate(
    candidate: &GcCandidate,
    io_engine: &IoEngine,
    buffer_pool: &WriteBufferPool,
    meta: &MetaStore,
    lifecycle: &VolumeLifecycleManager,
) -> OnyxResult<u32> {
    // Get volume config for created_at epoch
    let vol_created_at = lifecycle.with_read_lock(&candidate.vol_id.0, || {
        match meta.get_volume(&candidate.vol_id)? {
            Some(vc) => Ok::<_, crate::error::OnyxError>(Some(vc.created_at)),
            None => Ok(None),
        }
    })?;

    let vol_created_at = match vol_created_at {
        Some(ts) => ts,
        None => {
            tracing::debug!(
                vol = %candidate.vol_id,
                "gc rewriter: volume no longer exists, skipping"
            );
            return Ok(0);
        }
    };

    // Read the compressed unit from LV3
    let compressed_data = if candidate.slot_offset > 0 {
        let slot_data = io_engine.read_blocks(candidate.pba, BLOCK_SIZE as usize)?;
        let start = candidate.slot_offset as usize;
        let end = start + candidate.unit_compressed_size as usize;
        if end > slot_data.len() {
            return Err(crate::error::OnyxError::Compress(format!(
                "gc: packed fragment out of bounds: offset={} + size={} > {}",
                start, candidate.unit_compressed_size, slot_data.len()
            )));
        }
        slot_data[start..end].to_vec()
    } else {
        io_engine.read_blocks(candidate.pba, candidate.unit_compressed_size as usize)?
    };

    // Verify CRC
    let actual_crc = crc32fast::hash(&compressed_data);
    if actual_crc != candidate.crc32 {
        return Err(crate::error::OnyxError::CrcMismatch {
            expected: candidate.crc32,
            actual: actual_crc,
        });
    }

    // Decompress
    let decompressed = if candidate.compression == 0 {
        compressed_data
    } else {
        let algo =
            CompressionAlgo::from_u8(candidate.compression).unwrap_or(CompressionAlgo::None);
        let compressor = create_compressor(algo);
        let mut buf = vec![0u8; candidate.unit_original_size as usize];
        compressor.decompress(
            &compressed_data,
            &mut buf,
            candidate.unit_original_size as usize,
        )?;
        buf
    };

    let mut rewritten = 0u32;

    for (lba, offset_in_unit) in &candidate.live_lbas {
        // Re-verify that this LBA still points to the candidate PBA
        // (it may have been overwritten since the scan)
        let current = meta.get_mapping(&candidate.vol_id, *lba)?;
        match current {
            Some(bv) if bv.pba == candidate.pba => {}
            _ => continue, // LBA was overwritten or deleted since scan, skip
        }

        // Extract the 4KB block from the decompressed unit
        let start = *offset_in_unit as usize * BLOCK_SIZE as usize;
        let end = start + BLOCK_SIZE as usize;
        if end > decompressed.len() {
            tracing::warn!(
                lba = lba.0,
                offset_in_unit,
                decompressed_len = decompressed.len(),
                "gc rewriter: block out of bounds in decompressed unit, skipping"
            );
            continue;
        }

        let block_data = &decompressed[start..end];

        // Write back to buffer pool — reuses the normal write path
        buffer_pool.append(
            &candidate.vol_id.0,
            *lba,
            1,
            block_data,
            vol_created_at,
        )?;

        rewritten += 1;
    }

    tracing::debug!(
        pba = candidate.pba.0,
        vol = %candidate.vol_id,
        live = candidate.live_lbas.len(),
        rewritten,
        dead_ratio = candidate.dead_ratio,
        "gc: rewrote candidate"
    );

    Ok(rewritten)
}
