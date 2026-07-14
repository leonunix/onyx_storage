use crate::buffer::pool::WriteBufferPool;
use crate::compress::codec::create_compressor;
use crate::error::OnyxResult;
use crate::gc::scanner::GcCandidate;
use crate::io::engine::IoEngine;
use crate::lifecycle::VolumeLifecycleManager;
use crate::meta::schema::BlockmapValue;
use crate::meta::store::MetaStore;
use crate::space::extent::Extent;
use crate::space::hazard::PbaHazards;
use crate::types::{CompressionAlgo, BLOCK_SIZE};

fn relocation_run_end(ready_lbas: &[(crate::types::Lba, usize)], run_start: usize) -> usize {
    let mut run_end = run_start + 1;
    while run_end < ready_lbas.len() {
        let (previous_lba, previous_offset) = ready_lbas[run_end - 1];
        let (next_lba, next_offset) = ready_lbas[run_end];
        if previous_lba.0.checked_add(1) != Some(next_lba.0)
            || previous_offset.checked_add(BLOCK_SIZE as usize) != Some(next_offset)
        {
            break;
        }
        run_end += 1;
    }
    run_end
}

fn finish_relocation_submissions<T, E>(
    submissions: Vec<(T, u32)>,
    append_error: Option<E>,
    mut wait: impl FnMut(T),
) -> Result<u32, E> {
    let mut rewritten = 0u32;
    for (ticket, lba_count) in submissions {
        wait(ticket);
        rewritten = rewritten.saturating_add(lba_count);
    }
    match append_error {
        Some(error) => Err(error),
        None => Ok(rewritten),
    }
}

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
    hazards: Option<&PbaHazards>,
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

    let candidate_mapping = BlockmapValue {
        pba: candidate.pba,
        compression: candidate.compression,
        unit_compressed_size: candidate.unit_compressed_size,
        unit_original_size: candidate.unit_original_size,
        unit_lba_count: candidate.unit_lba_count,
        offset_in_unit: 0,
        crc32: candidate.crc32,
        slot_offset: candidate.slot_offset,
        flags: 0,
    };
    let source_extent = Extent::new(candidate.pba, candidate_mapping.physical_blocks(BLOCK_SIZE));
    let _hazard_guard = if let Some(hazards) = hazards {
        let guard = hazards.pin_many(candidate_mapping.physical_pbas(BLOCK_SIZE));
        let still_valid = valid_lbas.iter().any(|(lba, off)| {
            meta.get_mapping(&candidate.vol_id, *lba)
                .ok()
                .flatten()
                .is_some_and(|bv| candidate_matches_mapping(candidate, *off, &bv))
        });
        if !still_valid {
            return Ok(0);
        }
        Some(guard)
    } else {
        None
    };

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

    let mut ready_lbas = Vec::with_capacity(valid_lbas.len());
    let valid_lba_count = valid_lbas.len();

    // Use the pre-validated set, but re-verify each LBA one more time before
    // staging it — mappings could have shifted during the disk read.
    for (lba, offset_in_unit) in valid_lbas {
        let current = meta.get_mapping(&candidate.vol_id, lba)?;
        match current {
            Some(bv) if candidate_matches_mapping(candidate, offset_in_unit, &bv) => {}
            _ => continue,
        }

        // If this LBA already has a pending newer value in the write buffer,
        // let the normal flusher path drain it instead of re-injecting the
        // same block every GC cycle and starving the commit from reaching LV3.
        if let Some(pending) = buffer_pool.lookup(&candidate.vol_id.0, lba)? {
            if pending.vol_created_at == 0
                || vol_created_at == 0
                || pending.vol_created_at == vol_created_at
            {
                continue;
            }
        }

        // Extract the 4KB block from the decompressed unit
        let start = offset_in_unit as usize * BLOCK_SIZE as usize;
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

        ready_lbas.push((lba, start));
    }

    // Submit every contiguous logical run before waiting for LV2 durability.
    // The sync thread can then fold the writes into one durability epoch instead
    // of the old append/wait loop serialising one fdatasync boundary per block.
    ready_lbas.sort_unstable_by_key(|(lba, _)| lba.0);
    let mut tickets = Vec::new();
    let mut append_error = None;
    let mut run_start = 0usize;
    while run_start < ready_lbas.len() {
        let contiguous_end = relocation_run_end(&ready_lbas, run_start);
        let routing_count = buffer_pool.lbas_until_routing_boundary(
            ready_lbas[run_start].0,
            (contiguous_end - run_start) as u32,
        ) as usize;
        let run_end = run_start + routing_count;
        let mut payload = Vec::with_capacity((run_end - run_start) * BLOCK_SIZE as usize);
        for (_, source_offset) in &ready_lbas[run_start..run_end] {
            payload.extend_from_slice(
                &decompressed[*source_offset..*source_offset + BLOCK_SIZE as usize],
            );
        }
        let lba_count = (run_end - run_start) as u32;
        let run_lbas = &ready_lbas[run_start..run_end];
        let append_result = lifecycle.with_read_lock(&candidate.vol_id.0, || {
            buffer_pool.append_relocation_deferred_checked(
                &candidate.vol_id.0,
                ready_lbas[run_start].0,
                lba_count,
                &payload,
                vol_created_at,
                source_extent,
                || {
                    // This closure runs while foreground appends for the same
                    // routing shard are serialized. Recheck both pending LV2
                    // state and the source mapping immediately before the GC
                    // seq is allocated; otherwise stale relocation data could
                    // receive a newer seq and supersede a racing user write.
                    for &(lba, source_offset) in run_lbas {
                        if buffer_pool.lookup(&candidate.vol_id.0, lba)?.is_some() {
                            return Ok(false);
                        }
                        let offset_in_unit = u16::try_from(source_offset / BLOCK_SIZE as usize)
                            .map_err(|_| {
                                crate::error::OnyxError::Config(
                                    "GC relocation offset exceeds u16".into(),
                                )
                            })?;
                        let Some(current) = meta.get_mapping(&candidate.vol_id, lba)? else {
                            return Ok(false);
                        };
                        if !candidate_matches_mapping(candidate, offset_in_unit, &current) {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                },
            )
        });
        match append_result {
            Ok(Some(ticket)) => tickets.push((ticket, lba_count)),
            Ok(None) => {}
            Err(error) => {
                append_error = Some(error);
                break;
            }
        }
        run_start = run_end;
    }

    // A later run can fail after earlier deferred appends were accepted. Drain
    // those durability tickets before surfacing the error so the caller never
    // drops accepted relocation work with ambiguous LV2 durability.
    let rewritten = finish_relocation_submissions(tickets, append_error, |ticket| {
        ticket.wait();
    })?;

    tracing::debug!(
        pba = candidate.pba.0,
        vol = %candidate.vol_id,
        live = valid_lba_count,
        rewritten,
        dead_ratio = candidate.dead_ratio,
        "gc: rewrote candidate"
    );

    Ok(rewritten)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Lba;

    #[test]
    fn relocation_runs_require_lba_and_source_offset_continuity() {
        let block = BLOCK_SIZE as usize;
        let ready = vec![
            (Lba(10), 0),
            (Lba(11), block),
            // LBA is contiguous, but this source offset skips a block.
            (Lba(12), block * 3),
            (Lba(13), block * 4),
            // Source is contiguous, but the logical LBA skips a block.
            (Lba(15), block * 5),
        ];

        assert_eq!(relocation_run_end(&ready, 0), 2);
        assert_eq!(relocation_run_end(&ready, 2), 4);
        assert_eq!(relocation_run_end(&ready, 4), 5);
    }

    #[test]
    fn partial_submission_drains_accepted_tickets_before_error() {
        let mut waited = Vec::new();
        let result = finish_relocation_submissions(
            vec![(11u64, 2), (22u64, 3)],
            Some("later append failed"),
            |ticket| waited.push(ticket),
        );

        assert_eq!(waited, vec![11, 22]);
        assert_eq!(result, Err("later append failed"));
    }
}
