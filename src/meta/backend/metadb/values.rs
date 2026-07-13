use std::collections::HashMap;

use onyx_metadb::{ApplyOutcome, DedupValue, L2pValue, Transaction, VolumeOrdinal};

use crate::error::{OnyxError, OnyxResult};
use crate::meta::schema::{BlockmapValue, ContentHash, DedupEntry};
use crate::meta::store::{DedupHitResult, RemapCleanup};
use crate::space::extent::Extent;
use crate::types::{Lba, Pba};

use crate::meta::backend::codec::{
    blockmap_from_l2p_bytes, blockmap_to_l2p_bytes, blockmap_to_l2p_bytes_with_seq,
    dedup_from_value_bytes, dedup_to_value_bytes, freed_blocks_for_l2p_value, DEDUP_VALUE_BYTES,
};

use super::METADB_DEDUP_VALUE_BYTES;

pub(super) fn decode_l2p_value(value: L2pValue) -> OnyxResult<BlockmapValue> {
    from_l2p_value(value)
        .ok_or_else(|| OnyxError::Config("metadb L2P value has invalid Onyx layout".into()))
}

pub(super) fn decode_dedup_value(value: DedupValue) -> OnyxResult<DedupEntry> {
    from_dedup_value(value)
        .ok_or_else(|| OnyxError::Config("metadb dedup value has invalid Onyx layout".into()))
}

pub(super) fn newly_zeroed_from_remaps<I>(
    new_values: I,
    outcomes: Vec<ApplyOutcome>,
) -> OnyxResult<(HashMap<Pba, RemapCleanup>, Vec<bool>)>
where
    I: IntoIterator<Item = BlockmapValue>,
{
    let new_values: Vec<BlockmapValue> = new_values.into_iter().collect();
    let mut new_iter = new_values.into_iter();
    let mut decrements: HashMap<Pba, RemapCleanup> = HashMap::new();
    let mut freed = std::collections::HashSet::new();
    let mut accepted: Vec<bool> = Vec::new();

    // Per-LBA handler shared by L2pRemap and L2pRemapRange outcomes —
    // semantics are identical at the LBA level (range op is just a
    // compact transport for the same decision table).
    let mut handle_lba = |new_value: BlockmapValue,
                          applied: bool,
                          prev: Option<L2pValue>,
                          decrements: &mut HashMap<Pba, RemapCleanup>|
     -> OnyxResult<()> {
        accepted.push(applied);
        if !applied {
            return Ok(());
        }
        let Some(prev) = prev else {
            return Ok(());
        };
        let old = decode_l2p_value(prev)?;
        if old.is_zero() || old.pba == new_value.pba {
            return Ok(());
        }
        let blocks = freed_blocks_for_l2p_value(&old);
        decrements
            .entry(old.pba)
            .and_modify(|entry| entry.merge(RemapCleanup::new(old, blocks)))
            .or_insert_with(|| RemapCleanup::new(old, blocks));
        Ok(())
    };

    for outcome in outcomes {
        match outcome {
            ApplyOutcome::L2pRemap {
                applied,
                prev,
                freed_pba,
            } => {
                let new_value = new_iter.next().ok_or_else(|| {
                    OnyxError::Config(
                        "metadb outcomes consumed more values than the batch produced".into(),
                    )
                })?;
                if let Some(pba) = freed_pba {
                    freed.insert(from_metadb_pba(pba));
                }
                handle_lba(new_value, applied, prev, &mut decrements)?;
            }
            ApplyOutcome::L2pRemapRange {
                applied,
                prevs,
                freed_pbas,
            } => {
                if applied.len() != prevs.len() {
                    return Err(OnyxError::Config(format!(
                        "metadb L2pRemapRange applied/prevs length mismatch: {} vs {}",
                        applied.len(),
                        prevs.len(),
                    )));
                }
                for pba in &freed_pbas {
                    freed.insert(from_metadb_pba(*pba));
                }
                for (i, app) in applied.iter().enumerate() {
                    let new_value = new_iter.next().ok_or_else(|| {
                        OnyxError::Config(
                            "metadb outcomes consumed more values than the batch produced".into(),
                        )
                    })?;
                    handle_lba(new_value, *app, prevs[i], &mut decrements)?;
                }
            }
            // The onyx adapter may append explicit refcount ops after the
            // remap ops to cover multi-PBA raw extents. Those outcomes do not
            // consume a batch value; freed old mappings are already reported
            // through the remap outcome's `freed_pba(s)` fields.
            ApplyOutcome::RefcountNew(_) => {}
            _ => {
                return Err(OnyxError::Config(
                    "metadb returned non-remap outcome for remap batch".into(),
                ));
            }
        }
    }

    if new_iter.next().is_some() {
        return Err(OnyxError::Config(
            "metadb outcomes consumed fewer values than the batch produced".into(),
        ));
    }

    for (pba, cleanup) in decrements.iter_mut() {
        cleanup.pba_freed = freed.contains(pba);
    }
    decrements.retain(|_, cleanup| cleanup.pba_freed);
    Ok((decrements, accepted))
}

/// Emit `tx.l2p_remap_range` for each maximal contiguous LBA run in
/// `batch_values`, mapping each LBA's `BlockmapValue` to its metadb
/// `L2pValue` (with the corresponding seq from `seqs`).
///
/// `seq_base` is the offset of `batch_values[0]` inside the caller's
/// flat seqs vec — used by `atomic_batch_write_multi_with_dedup` which
/// passes a shared seqs vec spanning multiple units.
///
/// In the common passthrough case (`batch_values` is one CompressedUnit
/// with strictly contiguous LBAs) this emits exactly one range op. A
/// defensive gap or sub-range cap split produces multiple range ops.
/// Returns the number of emitted range ops so callers can separate their
/// outcomes from any following non-remap operations.
pub(super) fn emit_l2p_remap_runs(
    tx: &mut Transaction<'_>,
    ord: VolumeOrdinal,
    batch_values: &[(Lba, BlockmapValue)],
    seqs: &[u64],
    seq_base: usize,
) -> usize {
    if batch_values.is_empty() {
        return 0;
    }
    let cap = onyx_metadb::op::MAX_REMAP_RANGE_LBAS;
    let mut i = 0;
    let mut emitted_ops = 0;
    while i < batch_values.len() {
        let run_start = i;
        let start_lba = batch_values[i].0;
        // Extend while next LBA is contiguous and run length is below the
        // metadb-side cap. The cap is a defensive bound (4096) far above
        // passthrough's natural ceiling (`coalesce_max_lbas`), but enforce
        // it here so a future caller can't trip the decoder's reject.
        while i + 1 < batch_values.len()
            && batch_values[i + 1].0 .0 == batch_values[i].0 .0 + 1
            && (i + 1 - run_start) < cap
        {
            i += 1;
        }
        let run_end_inclusive = i;
        let count = run_end_inclusive - run_start + 1;
        let mut values: Vec<L2pValue> = Vec::with_capacity(count);
        for (off, (_lba, value)) in batch_values[run_start..=run_end_inclusive]
            .iter()
            .enumerate()
        {
            values.push(to_l2p_value_with_seq(
                value,
                seq_for(seqs, seq_base + run_start + off),
            ));
        }
        tx.l2p_remap_range(ord, start_lba.0, values.into_boxed_slice());
        emitted_ops += 1;
        i += 1;
    }
    emitted_ops
}

pub(super) fn dedup_hit_results_from_remaps(
    hits: &[(Lba, BlockmapValue, ContentHash)],
    outcomes: Vec<ApplyOutcome>,
) -> OnyxResult<(Vec<DedupHitResult>, HashMap<Pba, RemapCleanup>)> {
    // The tx queues `hits.len()` L2pRemap ops followed by promote-side
    // DedupPut ops. commit_with_outcomes returns one outcome per WAL op in
    // submission order, so the L2pRemap outcomes are exactly the first
    // `hits.len()` entries.
    if outcomes.len() < hits.len() {
        return Err(OnyxError::Config(format!(
            "metadb dedup outcome length mismatch: {} hits, {} outcomes",
            hits.len(),
            outcomes.len()
        )));
    }

    let mut results = Vec::with_capacity(hits.len());
    let mut old_mappings: HashMap<Pba, RemapCleanup> = HashMap::new();
    let mut freed = std::collections::HashSet::new();
    for ((_, new_value, _), outcome) in hits.iter().zip(outcomes.into_iter().take(hits.len())) {
        let ApplyOutcome::L2pRemap {
            applied,
            prev,
            freed_pba,
        } = outcome
        else {
            return Err(OnyxError::Config(
                "metadb returned non-remap outcome for dedup hit batch".into(),
            ));
        };

        if !applied {
            results.push(DedupHitResult::Rejected);
            continue;
        }

        if let Some(pba) = freed_pba {
            freed.insert(from_metadb_pba(pba));
        }

        if let Some(prev) = prev {
            let old = decode_l2p_value(prev)?;
            if !old.is_zero() && old.pba != new_value.pba {
                let blocks = freed_blocks_for_l2p_value(&old);
                let cleanup = RemapCleanup::new(old, blocks);
                old_mappings
                    .entry(old.pba)
                    .and_modify(|entry| entry.merge(cleanup.clone()))
                    .or_insert(cleanup);
            }
        }
        results.push(DedupHitResult::Accepted(None));
    }
    for (pba, cleanup) in old_mappings.iter_mut() {
        cleanup.pba_freed = freed.contains(pba);
    }
    Ok((results, old_mappings))
}

pub(crate) fn to_metadb_pba(pba: Pba) -> onyx_metadb::Pba {
    pba.0
}

pub(crate) fn from_metadb_pba(pba: onyx_metadb::Pba) -> Pba {
    Pba(pba)
}

/// Sort + dedup + run-length compress a list of freed PBAs into the
/// minimum set of contiguous extents the allocator's retire path
/// expects. Input order is unconstrained; duplicates are collapsed.
pub(crate) fn coalesce_free_pbas_to_extents(pbas: &[Pba]) -> Vec<Extent> {
    if pbas.is_empty() {
        return Vec::new();
    }
    let mut sorted: Vec<u64> = pbas.iter().map(|p| p.0).collect();
    sorted.sort_unstable();
    sorted.dedup();

    let mut out = Vec::new();
    let mut run_start = sorted[0];
    let mut run_len: u64 = 1;
    for &pba in &sorted[1..] {
        if pba == run_start + run_len {
            run_len += 1;
        } else {
            out.push(Extent::new(Pba(run_start), run_len as u32));
            run_start = pba;
            run_len = 1;
        }
    }
    out.push(Extent::new(Pba(run_start), run_len as u32));
    out
}

pub(crate) fn to_l2p_value(value: &BlockmapValue) -> L2pValue {
    L2pValue(blockmap_to_l2p_bytes(value))
}

pub(crate) fn to_l2p_value_with_seq(value: &BlockmapValue, seq: u64) -> L2pValue {
    L2pValue(blockmap_to_l2p_bytes_with_seq(value, seq))
}

#[inline]
pub(super) fn seq_for(seqs: &[u64], i: usize) -> u64 {
    if seqs.is_empty() {
        0
    } else {
        seqs[i]
    }
}

pub(crate) fn from_l2p_value(value: L2pValue) -> Option<BlockmapValue> {
    blockmap_from_l2p_bytes(&value.0)
}

pub(crate) fn to_dedup_value(entry: &DedupEntry) -> DedupValue {
    let mut bytes = [0u8; METADB_DEDUP_VALUE_BYTES];
    bytes[..DEDUP_VALUE_BYTES].copy_from_slice(&dedup_to_value_bytes(entry));
    DedupValue::new(bytes)
}

pub(crate) fn from_dedup_value(value: DedupValue) -> Option<DedupEntry> {
    let mut bytes = [0u8; DEDUP_VALUE_BYTES];
    bytes.copy_from_slice(&value.as_bytes()[..DEDUP_VALUE_BYTES]);
    dedup_from_value_bytes(&bytes)
}
