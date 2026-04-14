use crate::buffer::pool::WriteBufferPool;
use crate::compress::codec::create_compressor;
use crate::error::OnyxResult;
use crate::gc::scanner::GcCandidate;
use crate::io::engine::IoEngine;
use crate::lifecycle::VolumeLifecycleManager;
use crate::meta::schema::BlockmapValue;
use crate::meta::store::MetaStore;
use crate::types::{CompressionAlgo, BLOCK_SIZE};

fn candidate_matches_mapping(
    candidate: &GcCandidate,
    offset_in_unit: u16,
    bv: &BlockmapValue,
) -> bool {
    bv.pba == candidate.pba
        && bv.slot_offset == candidate.slot_offset
        && bv.unit_compressed_size == candidate.unit_compressed_size
        && bv.unit_original_size == candidate.unit_original_size
        && bv.unit_lba_count == candidate.unit_lba_count
        && bv.compression == candidate.compression
        && bv.crc32 == candidate.crc32
        && bv.offset_in_unit == offset_in_unit
}

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

    // Pre-validate: check if ANY LBA still maps to this candidate's PBA before
    // doing disk IO.  Between scan and rewrite the flusher may have remapped
    // every LBA, freed the PBA, and the allocator may have recycled it for new
    // data.  Reading from a recycled PBA would yield a CRC mismatch against the
    // stale candidate metadata — an expected race, not a data-integrity issue.
    // By validating first we avoid the unnecessary IO and the spurious error.
    //
    // Safety: if at least one LBA still maps here, refcount > 0, so the PBA has
    // NOT been freed and the on-disk data is intact.
    let mut valid_lbas: Vec<(crate::types::Lba, u16)> = Vec::new();
    for (lba, offset_in_unit) in &candidate.live_lbas {
        let current = meta.get_mapping(&candidate.vol_id, *lba)?;
        match current {
            Some(bv) if candidate_matches_mapping(candidate, *offset_in_unit, &bv) => {
                valid_lbas.push((*lba, *offset_in_unit));
            }
            _ => {} // LBA remapped since scan
        }
    }

    if valid_lbas.is_empty() {
        tracing::debug!(
            pba = candidate.pba.0,
            vol = %candidate.vol_id,
            "gc: all LBAs remapped since scan, skipping candidate"
        );
        return Ok(0);
    }

    // Read the compressed unit from LV3
    let compressed_data = if candidate.slot_offset > 0 {
        let slot_data = io_engine.read_blocks(candidate.pba, BLOCK_SIZE as usize)?;
        let start = candidate.slot_offset as usize;
        let end = start + candidate.unit_compressed_size as usize;
        if end > slot_data.len() {
            return Err(crate::error::OnyxError::Compress(format!(
                "gc: packed fragment out of bounds: offset={} + size={} > {}",
                start,
                candidate.unit_compressed_size,
                slot_data.len()
            )));
        }
        slot_data[start..end].to_vec()
    } else {
        io_engine.read_blocks(candidate.pba, candidate.unit_compressed_size as usize)?
    };

    // Verify CRC
    let actual_crc = crc32fast::hash(&compressed_data);
    if actual_crc != candidate.crc32 {
        // CRC mismatch after pre-validation found valid LBAs.  This means the
        // PBA was freed and reallocated in the tiny window between our mapping
        // check and the disk read.  Re-validate: if all LBAs have now been
        // remapped, this is a benign race — the flusher committed new mappings
        // and recycled the PBA concurrently.  If some LBAs still point here,
        // this is a genuine data-integrity concern.
        let still_valid = valid_lbas.iter().any(|(lba, off)| {
            meta.get_mapping(&candidate.vol_id, *lba)
                .ok()
                .flatten()
                .is_some_and(|bv| candidate_matches_mapping(candidate, *off, &bv))
        });
        if !still_valid {
            tracing::debug!(
                pba = candidate.pba.0,
                vol = %candidate.vol_id,
                expected = format!("0x{:08x}", candidate.crc32),
                actual = format!("0x{actual_crc:08x}"),
                "gc: PBA recycled between pre-check and disk read, skipping"
            );
            return Ok(0);
        }
        return Err(crate::error::OnyxError::CrcMismatch {
            expected: candidate.crc32,
            actual: actual_crc,
        });
    }

    // Decompress
    let decompressed = if candidate.compression == 0 {
        compressed_data
    } else {
        let algo = CompressionAlgo::from_u8(candidate.compression).unwrap_or(CompressionAlgo::None);
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

    // Use the pre-validated set, but re-verify each LBA one more time right
    // before writing — mappings could have shifted during the disk read.
    for (lba, offset_in_unit) in &valid_lbas {
        let current = meta.get_mapping(&candidate.vol_id, *lba)?;
        match current {
            Some(bv) if candidate_matches_mapping(candidate, *offset_in_unit, &bv) => {}
            _ => continue,
        }

        // If this LBA already has a pending newer value in the write buffer,
        // let the normal flusher path drain it instead of re-injecting the
        // same block every GC cycle and starving the commit from reaching LV3.
        if let Some(pending) = buffer_pool.lookup(&candidate.vol_id.0, *lba)? {
            if pending.vol_created_at == 0
                || vol_created_at == 0
                || pending.vol_created_at == vol_created_at
            {
                continue;
            }
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
        buffer_pool.append(&candidate.vol_id.0, *lba, 1, block_data, vol_created_at)?;

        rewritten += 1;
    }

    tracing::debug!(
        pba = candidate.pba.0,
        vol = %candidate.vol_id,
        live = valid_lbas.len(),
        rewritten,
        dead_ratio = candidate.dead_ratio,
        "gc: rewrote candidate"
    );

    Ok(rewritten)
}
