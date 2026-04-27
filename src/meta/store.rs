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

    pub fn memory_stats(&self) -> OnyxResult<MetaMemorySnapshot> {
        self.backend.memory_stats()
    }

    pub fn create_blockmap_cf(&self, vol_id: &str) -> OnyxResult<()> {
        if self.get_volume(&VolumeId(vol_id.to_string()))?.is_some() {
            return Ok(());
        }
        self.put_volume(&VolumeConfig {
            id: VolumeId(vol_id.to_string()),
            size_bytes: u64::MAX / u64::from(BLOCK_SIZE) * u64::from(BLOCK_SIZE),
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

    pub fn list_volumes(&self) -> OnyxResult<Vec<VolumeConfig>> {
        self.backend.list_volumes()
    }

    pub fn delete_volume(&self, id: &VolumeId) -> OnyxResult<Vec<(Pba, u32)>> {
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

    pub fn multi_get_mappings(
        &self,
        vol_id: &VolumeId,
        lbas: &[Lba],
    ) -> OnyxResult<Vec<Option<BlockmapValue>>> {
        self.backend.multi_get_mappings(vol_id, lbas)
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

    pub fn delete_blockmap_range(
        &self,
        vol_id: &VolumeId,
        start_lba: Lba,
        end_lba: Lba,
    ) -> OnyxResult<Vec<(Pba, u32)>> {
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
    ) -> OnyxResult<HashMap<Pba, (u32, u32)>> {
        self.backend
            .atomic_batch_write(vol_id, batch_values, new_refcount)
    }

    pub fn atomic_batch_write_packed(
        &self,
        batch_values: &[(VolumeId, Lba, BlockmapValue)],
        new_pba: Pba,
        new_refcount: u32,
    ) -> OnyxResult<HashMap<Pba, (u32, u32)>> {
        self.backend
            .atomic_batch_write_packed(batch_values, new_pba, new_refcount)
    }

    pub fn atomic_batch_write_multi(
        &self,
        units: &[(&VolumeId, &[(Lba, BlockmapValue)], u32)],
    ) -> OnyxResult<HashMap<Pba, (u32, u32)>> {
        self.backend.atomic_batch_write_multi(units)
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
    ) -> OnyxResult<Option<(Pba, u32)>> {
        self.backend.atomic_dedup_hit(vol_id, lba, new_value, hash)
    }

    pub fn atomic_batch_dedup_hits(
        &self,
        vol_id: &VolumeId,
        hits: &[(Lba, BlockmapValue, ContentHash)],
    ) -> OnyxResult<(Vec<DedupHitResult>, HashMap<Pba, u32>)> {
        self.backend.atomic_batch_dedup_hits(vol_id, hits)
    }

    pub fn get_dedup_entry(&self, hash: &ContentHash) -> OnyxResult<Option<DedupEntry>> {
        self.backend.get_dedup(hash)
    }

    pub fn put_dedup_entries(&self, entries: &[(ContentHash, DedupEntry)]) -> OnyxResult<()> {
        self.backend.put_dedup_entries(entries)
    }

    pub fn delete_dedup_index(&self, hash: &ContentHash) -> OnyxResult<()> {
        self.backend.delete_dedup_index(hash)
    }

    pub fn dedup_entry_is_live(&self, hash: &ContentHash, entry: &DedupEntry) -> OnyxResult<bool> {
        self.backend.dedup_entry_is_live(hash, entry)
    }

    pub fn cleanup_dedup_for_pba_standalone(&self, pba: Pba) -> OnyxResult<()> {
        self.backend.cleanup_dedup_for_pbas_batch(&[pba])
    }

    pub fn cleanup_dedup_for_pbas_batch(&self, pbas: &[Pba]) -> OnyxResult<()> {
        self.backend.cleanup_dedup_for_pbas_batch(pbas)
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
        for (vol_id, lba, value) in self.backend.scan_all_blockmap_entries()? {
            let key = encode_blockmap_key(lba);
            let val = encode_blockmap_value(&value);
            callback(&vol_id.0, &key, &val);
        }
        Ok(())
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

    pub fn iter_dedup_reverse_entries(&self) -> OnyxResult<Vec<(Pba, ContentHash)>> {
        self.backend.iter_dedup_reverse_entries()
    }

    pub fn iter_allocated_blocks(&self) -> OnyxResult<Vec<Pba>> {
        let mut allocated = std::collections::BTreeSet::new();
        for (_, _, value) in self.backend.scan_all_blockmap_entries()? {
            let blocks = freed_blocks_for_l2p_value(&value);
            for block in 0..blocks {
                allocated.insert(Pba(value.pba.0 + u64::from(block)));
            }
        }
        for (pba, _) in self.iter_refcounts()? {
            allocated.insert(pba);
        }
        Ok(allocated.into_iter().collect())
    }
}
