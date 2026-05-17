use std::collections::HashMap;

use crate::config::MetaConfig;
use crate::error::{OnyxError, OnyxResult};
use crate::meta::backend::codec::freed_blocks_for_l2p_value;
use crate::meta::backend::metadb::MetadbBackend;
use crate::meta::schema::{
    encode_blockmap_key, encode_blockmap_value, BlockmapValue, ContentHash, DedupEntry,
    MAX_VOLUME_ID_BYTES,
};
use crate::metrics::MetaMemorySnapshot;
use crate::types::{CompressionAlgo, Lba, Pba, VolumeConfig, VolumeId, BLOCK_SIZE};
use onyx_metadb::VolumeOrdinal;

/// Old physical metadata observed while replacing blockmap entries.
///
/// The cleanup thread uses the full old mapping to reconstruct the original
/// 4 KiB payload, recompute its short dedup hash, and conditionally remove the
/// matching forward dedup_index entry without maintaining a persistent reverse
/// table.
#[derive(Debug, Clone)]
pub struct RemapCleanup {
    pub pba: Pba,
    pub decrements: u32,
    pub blocks: u32,
    pub pba_freed: bool,
    pub mappings: Vec<BlockmapValue>,
}

impl RemapCleanup {
    pub fn new(mapping: BlockmapValue, blocks: u32) -> Self {
        Self {
            pba: mapping.pba,
            decrements: 1,
            blocks,
            pba_freed: false,
            mappings: vec![mapping],
        }
    }

    pub fn merge(&mut self, other: RemapCleanup) {
        debug_assert_eq!(self.pba, other.pba);
        self.decrements = self.decrements.saturating_add(other.decrements);
        self.blocks = self.blocks.max(other.blocks);
        self.pba_freed |= other.pba_freed;
        self.mappings.extend(other.mappings);
    }
}

/// Result for each dedup hit in a batched `atomic_batch_dedup_hits` call.
#[derive(Debug, Clone, Copy)]
pub enum DedupHitResult {
    /// Hit accepted. Contains `Some((old_pba, old_blocks))` if an old PBA was
    /// decremented, or `None` if the LBA already pointed to the target PBA.
    Accepted(Option<(Pba, u32)>),
    /// Hit rejected because the target PBA's refcount was 0 (freed).
    Rejected,
}

/// Summary of a [`MetaStore::rebuild_refcount_from_blockmap`] run.
#[derive(Debug, Clone, Copy, Default)]
pub struct RebuildSummary {
    pub referenced_pbas: u64,
    pub fixed_entries: u64,
    pub orphan_entries_removed: u64,
    pub total_set: u64,
}

pub struct MetaStore {
    backend: MetadbBackend,
}

impl MetaStore {
    pub fn open(config: &MetaConfig) -> OnyxResult<Self> {
        Ok(Self {
            backend: MetadbBackend::open(config)?,
        })
    }

    pub fn sync_durable(&self) -> OnyxResult<()> {
        self.backend.sync_durable()
    }

    pub fn request_durable_checkpoint(&self) -> OnyxResult<()> {
        self.backend.request_durable_checkpoint()
    }

    pub fn try_request_durable_checkpoint(&self) -> OnyxResult<bool> {
        self.backend.try_request_durable_checkpoint()
    }

    pub(crate) fn try_request_durable_checkpoint_token(&self) -> OnyxResult<Option<u64>> {
        self.backend.try_request_durable_checkpoint_token()
    }

    /// Best-effort count of in-memory dirty work (L2P dirty page
    /// buffer + RC pending deltas). The watermark thread uses this
    /// to threshold-trigger an early checkpoint when configured.
    pub fn dirty_pages_estimate(&self) -> usize {
        self.backend.dirty_pages_estimate()
    }

    pub(crate) fn durable_checkpoint_outcome(&self, token: u64) -> OnyxResult<Option<bool>> {
        self.backend.durable_checkpoint_outcome(token)
    }

    pub fn memory_stats(&self) -> OnyxResult<MetaMemorySnapshot> {
        self.backend.memory_stats()
    }

    pub fn create_blockmap_cf(&self, vol_id: &str) -> OnyxResult<()> {
        if self.get_volume(&VolumeId(vol_id.to_string()))?.is_some() {
            return Ok(());
        }
        self.put_volume(&VolumeConfig {
            id: VolumeId(vol_id.to_string()),
            size_bytes: u64::from(BLOCK_SIZE) * 1024,
            block_size: BLOCK_SIZE,
            compression: CompressionAlgo::None,
            created_at: 0,
            zone_count: 1,
        })
    }

    pub fn drop_blockmap_cf(&self, vol_id: &str) -> OnyxResult<()> {
        let _ = self.delete_volume(&VolumeId(vol_id.to_string()))?;
        Ok(())
    }

    pub fn put_volume(&self, config: &VolumeConfig) -> OnyxResult<()> {
        let id_len = config.id.0.len();
        if id_len == 0 || id_len > MAX_VOLUME_ID_BYTES {
            return Err(OnyxError::Config(format!(
                "volume ID must be 1..{} bytes, got {}",
                MAX_VOLUME_ID_BYTES, id_len
            )));
        }
        self.backend.put_volume(config)
    }

    pub fn get_volume(&self, id: &VolumeId) -> OnyxResult<Option<VolumeConfig>> {
        self.backend.get_volume(id)
    }

    pub fn volume_ordinal_str(&self, id: &str) -> OnyxResult<VolumeOrdinal> {
        self.backend.volume_ordinal_str(id)
    }

    pub fn list_volumes(&self) -> OnyxResult<Vec<VolumeConfig>> {
        self.backend.list_volumes()
    }

    pub fn delete_volume(&self, id: &VolumeId) -> OnyxResult<Vec<RemapCleanup>> {
        self.backend.delete_volume(id)
    }

    pub fn put_mapping(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        value: &BlockmapValue,
    ) -> OnyxResult<()> {
        self.backend.put_mapping(vol_id, lba, value)
    }

    pub fn get_mapping(&self, vol_id: &VolumeId, lba: Lba) -> OnyxResult<Option<BlockmapValue>> {
        self.backend.get_mapping(vol_id, lba)
    }

    /// Read the L2P entry along with its committed seq. Callers that
    /// submit a derived update (DedupScanner's flag-clear and
    /// dedup-hit promotion paths) MUST forward this seq to the
    /// subsequent `l2p_remap` so apply's `seq_guard_rejects` can
    /// distinguish "I read this and nobody else has touched it" from
    /// "a newer flusher commit landed while I was working" — emitting
    /// seq=0 silently bypasses the guard and clobbers the newer write.
    pub fn get_mapping_with_seq(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
    ) -> OnyxResult<Option<(BlockmapValue, u64)>> {
        self.backend.get_mapping_with_seq(vol_id, lba)
    }

    pub fn get_mapping_str(&self, vol_id: &str, lba: Lba) -> OnyxResult<Option<BlockmapValue>> {
        self.backend.get_mapping_str(vol_id, lba)
    }

    pub fn get_mapping_ord(
        &self,
        ord: VolumeOrdinal,
        lba: Lba,
    ) -> OnyxResult<Option<BlockmapValue>> {
        self.backend.get_mapping_ord(ord, lba)
    }

    pub fn multi_get_mappings(
        &self,
        vol_id: &VolumeId,
        lbas: &[Lba],
    ) -> OnyxResult<Vec<Option<BlockmapValue>>> {
        self.backend.multi_get_mappings(vol_id, lbas)
    }

    pub fn multi_get_mappings_str(
        &self,
        vol_id: &str,
        lbas: &[Lba],
    ) -> OnyxResult<Vec<Option<BlockmapValue>>> {
        self.backend.multi_get_mappings_str(vol_id, lbas)
    }

    pub fn multi_get_mappings_ord(
        &self,
        ord: VolumeOrdinal,
        lbas: &[Lba],
    ) -> OnyxResult<Vec<Option<BlockmapValue>>> {
        self.backend.multi_get_mappings_ord(ord, lbas)
    }

    pub fn multi_get_mappings_raw_ord(
        &self,
        ord: VolumeOrdinal,
        lbas: &[onyx_metadb::Lba],
    ) -> OnyxResult<Vec<Option<BlockmapValue>>> {
        self.backend.multi_get_mappings_raw_ord(ord, lbas)
    }

    pub fn delete_mapping(&self, vol_id: &VolumeId, lba: Lba) -> OnyxResult<()> {
        self.backend.delete_mapping(vol_id, lba)
    }

    pub fn get_mappings_range(
        &self,
        vol_id: &VolumeId,
        start: Lba,
        end: Lba,
    ) -> OnyxResult<Vec<(Lba, BlockmapValue)>> {
        self.backend.get_mappings_range(vol_id, start, end)
    }

    pub fn get_mappings_range_unordered(
        &self,
        vol_id: &VolumeId,
        start: Lba,
        end: Lba,
    ) -> OnyxResult<Vec<(Lba, BlockmapValue)>> {
        self.backend
            .get_mappings_range_unordered(vol_id, start, end)
    }

    pub fn get_mappings_range_unordered_str(
        &self,
        vol_id: &str,
        start: Lba,
        end: Lba,
    ) -> OnyxResult<Vec<(Lba, BlockmapValue)>> {
        self.backend
            .get_mappings_range_unordered_str(vol_id, start, end)
    }

    pub fn get_mappings_range_unordered_ord(
        &self,
        ord: VolumeOrdinal,
        start: Lba,
        end: Lba,
    ) -> OnyxResult<Vec<(Lba, BlockmapValue)>> {
        self.backend
            .get_mappings_range_unordered_ord(ord, start, end)
    }

    pub fn delete_blockmap_range(
        &self,
        vol_id: &VolumeId,
        start_lba: Lba,
        end_lba: Lba,
    ) -> OnyxResult<Vec<RemapCleanup>> {
        self.backend
            .delete_blockmap_range(vol_id, start_lba, end_lba)
    }

    pub fn atomic_write_mapping(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        value: &BlockmapValue,
    ) -> OnyxResult<()> {
        self.backend.atomic_write_mapping(vol_id, lba, value)
    }

    pub fn atomic_remap(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        old_pba: Option<Pba>,
        new_value: &BlockmapValue,
    ) -> OnyxResult<()> {
        self.backend.atomic_remap(vol_id, lba, old_pba, new_value)
    }

    pub fn atomic_batch_write(
        &self,
        vol_id: &VolumeId,
        batch_values: &[(Lba, BlockmapValue)],
        new_refcount: u32,
    ) -> OnyxResult<HashMap<Pba, RemapCleanup>> {
        self.backend
            .atomic_batch_write(vol_id, batch_values, new_refcount)
    }

    pub fn atomic_batch_write_with_dedup(
        &self,
        vol_id: &VolumeId,
        batch_values: &[(Lba, BlockmapValue)],
        new_refcount: u32,
        dedup_entries: &[(ContentHash, DedupEntry)],
        seqs: &[u64],
    ) -> OnyxResult<(HashMap<Pba, RemapCleanup>, Vec<bool>)> {
        self.backend.atomic_batch_write_with_dedup(
            vol_id,
            batch_values,
            new_refcount,
            dedup_entries,
            seqs,
        )
    }

    pub fn atomic_batch_write_packed(
        &self,
        batch_values: &[(VolumeId, Lba, BlockmapValue)],
        new_pba: Pba,
        new_refcount: u32,
    ) -> OnyxResult<HashMap<Pba, RemapCleanup>> {
        self.backend
            .atomic_batch_write_packed(batch_values, new_pba, new_refcount)
    }

    pub fn atomic_batch_write_packed_with_dedup(
        &self,
        batch_values: &[(VolumeId, Lba, BlockmapValue)],
        new_pba: Pba,
        new_refcount: u32,
        dedup_entries: &[(ContentHash, DedupEntry)],
        seqs: &[u64],
    ) -> OnyxResult<(HashMap<Pba, RemapCleanup>, Vec<bool>)> {
        self.backend.atomic_batch_write_packed_with_dedup(
            batch_values,
            new_pba,
            new_refcount,
            dedup_entries,
            seqs,
        )
    }

    pub fn atomic_batch_write_multi(
        &self,
        units: &[(&VolumeId, &[(Lba, BlockmapValue)], u32)],
    ) -> OnyxResult<HashMap<Pba, RemapCleanup>> {
        self.backend.atomic_batch_write_multi(units)
    }

    pub fn atomic_batch_write_multi_with_dedup(
        &self,
        units: &[(&VolumeId, &[(Lba, BlockmapValue)], u32)],
        dedup_entries: &[(ContentHash, DedupEntry)],
        seqs: &[u64],
    ) -> OnyxResult<(HashMap<Pba, RemapCleanup>, Vec<bool>)> {
        self.backend
            .atomic_batch_write_multi_with_dedup(units, dedup_entries, seqs)
    }

    pub fn get_refcount(&self, pba: Pba) -> OnyxResult<u32> {
        self.backend.get_refcount(pba)
    }

    pub fn multi_get_refcounts(&self, pbas: &[Pba]) -> OnyxResult<Vec<u32>> {
        self.backend.multi_get_refcounts(pbas)
    }

    pub fn set_refcount(&self, pba: Pba, count: u32) -> OnyxResult<()> {
        self.backend.set_refcount(pba, count)
    }

    pub fn increment_refcount(&self, pba: Pba) -> OnyxResult<u32> {
        self.backend.increment_refcount(pba)
    }

    pub fn decrement_refcount(&self, pba: Pba) -> OnyxResult<u32> {
        self.backend.decrement_refcount(pba)
    }

    pub fn atomic_dedup_hit(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        new_value: &BlockmapValue,
        hash: &ContentHash,
        seq: u64,
    ) -> OnyxResult<Option<RemapCleanup>> {
        self.backend
            .atomic_dedup_hit(vol_id, lba, new_value, hash, seq)
    }

    pub fn atomic_batch_dedup_hits(
        &self,
        vol_id: &VolumeId,
        hits: &[(Lba, BlockmapValue, ContentHash)],
    ) -> OnyxResult<(Vec<DedupHitResult>, HashMap<Pba, RemapCleanup>)> {
        self.backend.atomic_batch_dedup_hits(vol_id, hits)
    }

    /// Same as [`atomic_batch_dedup_hits`] but also writes
    /// `promote_entries` into `dedup_index` in the same metadb
    /// transaction. Used by the promote-on-verified-hit path so the
    /// dedup_index registration and the LBA remap land atomically — a
    /// crash between the two would leave the cache promotion half-applied.
    pub fn atomic_batch_dedup_hits_with_promote(
        &self,
        vol_id: &VolumeId,
        hits: &[(Lba, BlockmapValue, ContentHash)],
        promote_entries: &[(ContentHash, DedupEntry)],
        seqs: &[u64],
    ) -> OnyxResult<(Vec<DedupHitResult>, HashMap<Pba, RemapCleanup>)> {
        self.backend
            .atomic_batch_dedup_hits_with_promote(vol_id, hits, promote_entries, seqs)
    }

    pub fn get_dedup_entry(&self, hash: &ContentHash) -> OnyxResult<Option<DedupEntry>> {
        self.backend.get_dedup(hash)
    }

    pub fn multi_get_dedup_entries(
        &self,
        hashes: &[ContentHash],
    ) -> OnyxResult<Vec<Option<DedupEntry>>> {
        self.backend.multi_get_dedup(hashes)
    }

    pub fn put_dedup_entries(&self, entries: &[(ContentHash, DedupEntry)]) -> OnyxResult<()> {
        self.backend.put_dedup_entries(entries)
    }

    pub fn delete_dedup_index(&self, hash: &ContentHash) -> OnyxResult<()> {
        self.backend.delete_dedup_index(hash)
    }

    pub fn delete_dedup_index_if_matches(
        &self,
        hash: &ContentHash,
        mapping: &BlockmapValue,
    ) -> OnyxResult<bool> {
        self.backend.delete_dedup_index_if_matches(hash, mapping)
    }

    pub fn compare_put_dedup_index(
        &self,
        hash: &ContentHash,
        old_entry: &DedupEntry,
        new_entry: &DedupEntry,
    ) -> OnyxResult<bool> {
        self.backend
            .compare_put_dedup_index(hash, old_entry, new_entry)
    }

    pub fn dedup_entry_is_live(&self, hash: &ContentHash, entry: &DedupEntry) -> OnyxResult<bool> {
        self.backend.dedup_entry_is_live(hash, entry)
    }

    pub fn multi_dedup_entries_are_live(
        &self,
        entries: &[(ContentHash, DedupEntry)],
    ) -> OnyxResult<Vec<bool>> {
        self.backend.multi_dedup_entries_are_live(entries)
    }

    pub fn scan_dedup_skipped(
        &self,
        limit: usize,
    ) -> OnyxResult<Vec<(String, Lba, BlockmapValue)>> {
        self.backend.scan_dedup_skipped(limit)
    }

    pub fn update_blockmap_flags(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        new_flags: u8,
    ) -> OnyxResult<()> {
        self.backend.update_blockmap_flags(vol_id, lba, new_flags)
    }

    pub fn has_any_blockmap_ref(&self, target_pba: Pba) -> OnyxResult<bool> {
        self.backend.has_any_blockmap_ref(target_pba)
    }

    pub fn has_any_blockmap_ref_in_extent(&self, start: Pba, blocks: u32) -> OnyxResult<bool> {
        self.backend.has_any_blockmap_ref_in_extent(start, blocks)
    }

    pub fn count_blockmap_refs_for_pba(&self, target_pba: Pba) -> OnyxResult<u32> {
        self.backend.count_blockmap_refs_for_pba(target_pba)
    }

    pub fn cleanup_orphaned_refcounts(&self) -> OnyxResult<Vec<(Pba, u32)>> {
        self.backend.cleanup_orphaned_refcounts()
    }

    pub fn scan_all_blockmap_entries(
        &self,
        callback: &mut dyn FnMut(&str, &[u8], &[u8]),
    ) -> OnyxResult<()> {
        self.backend
            .scan_all_blockmap_entries_with(&mut |vol_id, lba, value| {
                let key = encode_blockmap_key(lba);
                let val = encode_blockmap_value(&value);
                callback(&vol_id.0, &key, &val);
            })?;
        Ok(())
    }

    /// Visit live blockmap entries for one volume in `[start_lba, start_lba + count)`.
    /// Order within the range is shard-internal (not LBA-sorted), so callers that
    /// only need to bound work per cycle can checkpoint by `start + count` and
    /// resume on the next cycle. Used by the dedup scanner's cold-tail warming
    /// pass to walk live mappings whose hashes are not yet in the candidate cache.
    pub fn scan_blockmap_range(
        &self,
        vol_id: &VolumeId,
        start_lba: Lba,
        count: u64,
        callback: &mut dyn FnMut(Lba, BlockmapValue),
    ) -> OnyxResult<()> {
        self.backend
            .scan_blockmap_range(vol_id, start_lba, count, callback)
    }

    pub fn rebuild_refcount_from_blockmap(&self) -> OnyxResult<RebuildSummary> {
        Ok(RebuildSummary::default())
    }

    pub fn iter_refcounts(&self) -> OnyxResult<Vec<(Pba, u32)>> {
        self.backend.iter_refcounts()
    }

    pub fn iter_dedup_entries(&self) -> OnyxResult<Vec<(ContentHash, DedupEntry)>> {
        self.backend.iter_dedup_entries()
    }

    pub fn iter_allocated_blocks(&self) -> OnyxResult<Vec<Pba>> {
        let mut allocated = std::collections::BTreeSet::new();
        for (_, _, value) in self.backend.scan_all_blockmap_entries()? {
            if value.is_zero() {
                continue;
            }
            let blocks = freed_blocks_for_l2p_value(&value);
            for block in 0..blocks {
                allocated.insert(Pba(value.pba.0 + u64::from(block)));
            }
        }
        Ok(allocated.into_iter().collect())
    }
}
