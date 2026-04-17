use rocksdb::WriteBatch;

use crate::error::OnyxResult;
use crate::meta::schema::*;
use crate::types::{Pba, BLOCK_SIZE};

use super::MetaStore;

impl MetaStore {
    /// Check if any blockmap entry across all volumes references the given PBA.
    pub fn has_any_blockmap_ref(&self, target_pba: Pba) -> OnyxResult<bool> {
        let mut found = false;
        self.redb.scan_all_mappings(|_, _, bv| {
            if bv.pba == target_pba {
                found = true;
            }
        })?;
        Ok(found)
    }

    /// Count live blockmap references that currently point at `target_pba`.
    /// Full redb scan — test helper / diagnostic only, not for hot paths.
    pub fn count_blockmap_refs_for_pba(&self, target_pba: Pba) -> OnyxResult<u32> {
        let mut refs = 0u32;
        self.redb.scan_all_mappings(|_, _, bv| {
            if bv.pba == target_pba {
                refs = refs.saturating_add(1);
            }
        })?;
        Ok(refs)
    }

    /// Scan for and remove orphaned refcount entries — PBAs with refcount > 0
    /// but no blockmap entries referencing them. Returns the PBAs that were
    /// cleaned up. Uses the redb paged scan as the blockmap source of truth.
    pub fn cleanup_orphaned_refcounts(&self) -> OnyxResult<Vec<(Pba, u32)>> {
        let _blockmap_guards = self.lock_all_blockmap_stripes();
        let _refcount_guards = self.lock_all_refcount_stripes();
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();

        let mut referenced_pbas = std::collections::HashSet::new();
        self.redb.scan_all_mappings(|_, _, bv| {
            referenced_pbas.insert(bv.pba);
        })?;

        let mut orphans = Vec::new();
        let mut batch = WriteBatch::default();
        let rc_iter = self
            .db
            .iterator_cf(&cf_refcount, rocksdb::IteratorMode::Start);
        for item in rc_iter {
            let (key, value) = item?;
            if key.len() == 8 && value.len() == 4 {
                let pba = Pba(u64::from_be_bytes(key[..8].try_into().unwrap()));
                let rc = u32::from_be_bytes(value[..4].try_into().unwrap());
                if !referenced_pbas.contains(&pba) {
                    orphans.push((pba, rc));
                    batch.delete_cf(&cf_refcount, &key);
                }
            }
        }

        if !orphans.is_empty() {
            self.db.write(batch)?;
            tracing::warn!(
                count = orphans.len(),
                "cleaned up orphaned refcount entries"
            );
        }

        Ok(orphans)
    }

    /// Scan all blockmap entries across all volumes via the redb paged layout.
    /// Callback receives (vol_id, encoded_key, encoded_value) for each entry,
    /// matching the legacy byte-level interface so existing consumers (GC
    /// scanner, probe tools) work without change.
    pub fn scan_all_blockmap_entries(
        &self,
        callback: &mut dyn FnMut(&str, &[u8], &[u8]),
    ) -> OnyxResult<()> {
        self.redb.scan_all_mappings(|vol_id, lba, bv| {
            let key = encode_blockmap_key(lba);
            let value = encode_blockmap_value(&bv);
            callback(vol_id, &key, &value);
        })?;
        Ok(())
    }

    /// Iterate all refcount entries (for space allocator rebuild).
    pub fn iter_refcounts(&self) -> OnyxResult<Vec<(Pba, u32)>> {
        let cf = self.db.cf_handle(CF_REFCOUNT).unwrap();
        let mut results = Vec::new();
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, value) = item?;
            if let (Some(pba), Some(count)) =
                (decode_refcount_key(&key), decode_refcount_value(&value))
            {
                if count > 0 {
                    results.push((pba, count));
                }
            }
        }
        Ok(results)
    }

    /// Iterate all allocated physical blocks across all volumes, expanding
    /// compression-unit spans into individual PBAs. Combines redb blockmap +
    /// RocksDB refcount so allocator rebuild never misses a PBA that is tracked
    /// by refcount but not yet mapped (e.g., crash window between RocksDB
    /// refcount bump and redb blockmap commit).
    pub fn iter_allocated_blocks(&self) -> OnyxResult<Vec<Pba>> {
        let mut allocated = std::collections::BTreeSet::new();

        self.redb.scan_all_mappings(|_, _, bv| {
            let blocks = bv.unit_compressed_size.div_ceil(BLOCK_SIZE);
            for block in 0..blocks {
                allocated.insert(Pba(bv.pba.0 + block as u64));
            }
        })?;

        for (pba, _) in self.iter_refcounts()? {
            allocated.insert(pba);
        }

        Ok(allocated.into_iter().collect())
    }
}
