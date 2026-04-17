use std::collections::HashMap;

use rocksdb::WriteBatch;

use crate::error::OnyxResult;
use crate::meta::schema::*;
use crate::types::{Lba, Pba, VolumeId, BLOCK_SIZE};

use super::MetaStore;

impl MetaStore {
    pub fn put_mapping(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        value: &BlockmapValue,
    ) -> OnyxResult<()> {
        let key = encode_blockmap_key(lba);
        let _blockmap_guards = self.lock_blockmap_keys([key.as_slice()]);
        self.redb.put_mapping(&vol_id.0, lba, *value)?;
        Ok(())
    }

    pub fn get_mapping(&self, vol_id: &VolumeId, lba: Lba) -> OnyxResult<Option<BlockmapValue>> {
        self.redb.get_mapping(&vol_id.0, lba)
    }

    /// Batch-read multiple LBA mappings in a single redb read transaction.
    /// Results are returned in the same order as the input `lbas` slice.
    pub fn multi_get_mappings(
        &self,
        vol_id: &VolumeId,
        lbas: &[Lba],
    ) -> OnyxResult<Vec<Option<BlockmapValue>>> {
        self.redb.batch_get_mappings(&vol_id.0, lbas)
    }

    pub fn delete_mapping(&self, vol_id: &VolumeId, lba: Lba) -> OnyxResult<()> {
        let key = encode_blockmap_key(lba);
        let _blockmap_guards = self.lock_blockmap_keys([key.as_slice()]);
        self.redb.delete_mapping(&vol_id.0, lba)?;
        Ok(())
    }

    pub fn get_mappings_range(
        &self,
        vol_id: &VolumeId,
        start: Lba,
        end: Lba,
    ) -> OnyxResult<Vec<(Lba, BlockmapValue)>> {
        self.redb.get_mappings_range(&vol_id.0, start, end)
    }

    /// Atomically write a blockmap entry + set refcount = 1.
    ///
    /// Cross-DB ordering (per redb_paged_blockmap plan §5.2): RocksDB refcount
    /// first, redb blockmap second. A crash between the two leaks the PBA
    /// refcount but never corrupts data; startup recovery reconciles.
    pub fn atomic_write_mapping(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        value: &BlockmapValue,
    ) -> OnyxResult<()> {
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();

        let bm_key = encode_blockmap_key(lba);
        let _blockmap_guards = self.lock_blockmap_keys([bm_key.as_slice()]);
        let _refcount_guards = self.lock_refcount_pbas([value.pba]);

        let mut batch = WriteBatch::default();
        let rc_key = encode_refcount_key(value.pba);
        batch.put_cf(&cf_refcount, &rc_key, &encode_refcount_value(1));
        self.db.write_opt(batch, &self.hot_write_opts)?;

        self.redb.put_mapping(&vol_id.0, lba, *value)?;
        Ok(())
    }

    /// Atomically update mapping: decrement old PBA refcount, write new mapping + new refcount.
    ///
    /// The authoritative old PBA is read from redb inside the blockmap stripe
    /// lock (prevents same-LBA races from decrementing the wrong PBA). The
    /// caller's `old_pba` hint is only used as a fallback when redb has no
    /// prior mapping.
    pub fn atomic_remap(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        old_pba: Option<Pba>,
        new_value: &BlockmapValue,
    ) -> OnyxResult<()> {
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();

        let bm_key = encode_blockmap_key(lba);
        let _blockmap_guards = self.lock_blockmap_keys([bm_key.as_slice()]);
        let current_old_mapping = self.redb.get_mapping(&vol_id.0, lba)?;
        let current_old_pba = current_old_mapping.as_ref().map(|mapping| mapping.pba);
        let old_pba = current_old_pba.or(old_pba);

        let mut touched_pbas = vec![new_value.pba];
        if let Some(old) = old_pba {
            touched_pbas.push(old);
        }
        let _refcount_guards = self.lock_refcount_pbas(touched_pbas);

        let mut batch = WriteBatch::default();
        let new_rc_key = encode_refcount_key(new_value.pba);
        batch.put_cf(&cf_refcount, &new_rc_key, &encode_refcount_value(1));

        if let Some(old) = old_pba {
            let old_count = self.get_refcount(old)?;
            let new_count = old_count.saturating_sub(1);
            let old_rc_key = encode_refcount_key(old);
            if new_count == 0 {
                batch.delete_cf(&cf_refcount, &old_rc_key);
            } else {
                batch.put_cf(&cf_refcount, &old_rc_key, &encode_refcount_value(new_count));
            }
        }

        self.db.write_opt(batch, &self.hot_write_opts)?;
        self.redb.put_mapping(&vol_id.0, lba, *new_value)?;
        Ok(())
    }

    /// Atomically write multiple blockmap entries sharing the same PBA (compression unit),
    /// set new PBA refcount, and decrement old PBA refcounts.
    ///
    /// Old mappings are re-read **inside** the blockmap stripe lock to prevent
    /// stale-read races (another thread remapping an LBA between the caller's
    /// read and this write would cause the wrong old PBA to be decremented,
    /// leading to refcount drift).
    ///
    /// Returns `HashMap<Pba, (decrement, block_count)>` for old PBAs that were
    /// decremented, so the caller can check refcounts and free dead PBAs.
    pub fn atomic_batch_write(
        &self,
        vol_id: &VolumeId,
        batch_values: &[(Lba, BlockmapValue)],
        new_refcount: u32,
    ) -> OnyxResult<HashMap<Pba, (u32, u32)>> {
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();

        let lbas: Vec<Lba> = batch_values.iter().map(|(lba, _)| *lba).collect();
        let bm_keys: Vec<[u8; 8]> = lbas.iter().map(|lba| encode_blockmap_key(*lba)).collect();
        let _blockmap_guards = self.lock_blockmap_keys(&bm_keys);

        let new_pba = batch_values.first().map(|(_, v)| v.pba);
        let old_mappings = self.redb.batch_get_mappings(&vol_id.0, &lbas)?;

        let mut old_pba_meta: HashMap<Pba, (u32, u32)> = HashMap::new();
        for old in old_mappings.iter().flatten() {
            if Some(old.pba) != new_pba {
                let old_blocks = old.unit_compressed_size.div_ceil(BLOCK_SIZE);
                let entry = old_pba_meta.entry(old.pba).or_insert((0, old_blocks));
                entry.0 += 1;
                entry.1 = entry.1.max(old_blocks);
            }
        }

        let mut touched_pbas: Vec<Pba> = old_pba_meta.keys().copied().collect();
        if let Some(pba) = new_pba {
            touched_pbas.push(pba);
        }
        let _refcount_guards = self.lock_refcount_pbas(touched_pbas);
        let old_pbas: Vec<Pba> = old_pba_meta.keys().copied().collect();
        let old_refcounts = self.refcounts_by_pba(&old_pbas)?;

        let mut batch = WriteBatch::default();

        if let Some((_, first_val)) = batch_values.first() {
            let rc_key = encode_refcount_key(first_val.pba);
            batch.put_cf(&cf_refcount, &rc_key, &encode_refcount_value(new_refcount));
        }

        let mut newly_zeroed: HashMap<Pba, (u32, u32)> = HashMap::new();
        for (old_pba, (decrement, blocks)) in &old_pba_meta {
            let current_rc = old_refcounts.get(old_pba).copied().unwrap_or(0);
            let new_rc = current_rc.saturating_sub(*decrement);
            let rc_key = encode_refcount_key(*old_pba);
            if new_rc == 0 {
                batch.delete_cf(&cf_refcount, &rc_key);
                if current_rc > 0 {
                    newly_zeroed.insert(*old_pba, (*decrement, *blocks));
                }
            } else {
                batch.put_cf(&cf_refcount, &rc_key, &encode_refcount_value(new_rc));
            }
        }

        self.db.write_opt(batch, &self.hot_write_opts)?;

        // Commit blockmap updates to redb after refcount bumps are durable.
        let redb_entries: Vec<(&str, Lba, BlockmapValue)> = batch_values
            .iter()
            .map(|(lba, v)| (vol_id.0.as_str(), *lba, *v))
            .collect();
        self.redb.batch_put_mappings(&redb_entries)?;

        Ok(newly_zeroed)
    }

    /// Atomically write blockmap entries from multiple volumes sharing the same PBA
    /// (packed slot). Sets the total refcount for the PBA and decrements old PBAs.
    ///
    /// Old mappings are re-read inside the lock (same rationale as `atomic_batch_write`).
    ///
    /// Returns `HashMap<Pba, (decrement, block_count)>` for old PBAs that were decremented.
    pub fn atomic_batch_write_packed(
        &self,
        batch_values: &[(VolumeId, Lba, BlockmapValue)],
        new_pba: Pba,
        new_refcount: u32,
    ) -> OnyxResult<HashMap<Pba, (u32, u32)>> {
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();

        let bm_keys: Vec<[u8; 8]> = batch_values
            .iter()
            .map(|(_, lba, _)| encode_blockmap_key(*lba))
            .collect();
        let _blockmap_guards = self.lock_blockmap_keys(&bm_keys);

        let mut old_pba_meta: HashMap<Pba, (u32, u32)> = HashMap::new();
        let mut self_decrement: u32 = 0;
        for (vol_id, lba, _new_value) in batch_values {
            let old_mapping = self.redb.get_mapping(&vol_id.0, *lba)?;
            if let Some(old) = old_mapping {
                if old.pba == new_pba {
                    self_decrement += 1;
                } else {
                    let old_blocks = old.unit_compressed_size.div_ceil(BLOCK_SIZE);
                    let entry = old_pba_meta.entry(old.pba).or_insert((0, old_blocks));
                    entry.0 += 1;
                    entry.1 = entry.1.max(old_blocks);
                }
            }
        }

        let mut touched_pbas: Vec<Pba> = old_pba_meta.keys().copied().collect();
        touched_pbas.push(new_pba);
        let _refcount_guards = self.lock_refcount_pbas(touched_pbas);
        let old_pbas: Vec<Pba> = old_pba_meta.keys().copied().collect();
        let old_refcounts = self.refcounts_by_pba(&old_pbas)?;

        let mut batch = WriteBatch::default();

        // Read current refcount and compute net increment, instead of blind PUT.
        // A blind PUT would overwrite any refs added by concurrent dedup hits or
        // other paths, causing refcount drift -> premature PBA free -> CRC mismatch.
        let current_rc = self.get_refcount(new_pba)?;
        let net_increment = new_refcount - self_decrement;
        let new_rc = current_rc + net_increment;
        let rc_key = encode_refcount_key(new_pba);
        batch.put_cf(&cf_refcount, &rc_key, &encode_refcount_value(new_rc));

        let mut newly_zeroed: HashMap<Pba, (u32, u32)> = HashMap::new();
        for (old_pba, (decrement, blocks)) in &old_pba_meta {
            let current_rc = old_refcounts.get(old_pba).copied().unwrap_or(0);
            let new_rc = current_rc.saturating_sub(*decrement);
            let rc_key = encode_refcount_key(*old_pba);
            if new_rc == 0 {
                batch.delete_cf(&cf_refcount, &rc_key);
                if current_rc > 0 {
                    newly_zeroed.insert(*old_pba, (*decrement, *blocks));
                }
            } else {
                batch.put_cf(&cf_refcount, &rc_key, &encode_refcount_value(new_rc));
            }
        }

        self.db.write_opt(batch, &self.hot_write_opts)?;

        let redb_entries: Vec<(&str, Lba, BlockmapValue)> = batch_values
            .iter()
            .map(|(vol_id, lba, v)| (vol_id.0.as_str(), *lba, *v))
            .collect();
        self.redb.batch_put_mappings(&redb_entries)?;

        Ok(newly_zeroed)
    }

    /// Atomically write blockmap entries for multiple units, each with its own PBA.
    /// Combines all units into a single RocksDB WriteBatch for one WAL sync and a
    /// single redb write transaction for the blockmap updates.
    ///
    /// Old mappings are re-read inside the lock (same rationale as `atomic_batch_write`).
    ///
    /// Each item in `units`: (vol_id, blockmap entries, new_pba refcount).
    /// Returns `HashMap<Pba, (decrement, block_count)>` for old PBAs that were decremented.
    pub fn atomic_batch_write_multi(
        &self,
        units: &[(&VolumeId, &[(Lba, BlockmapValue)], u32)],
    ) -> OnyxResult<HashMap<Pba, (u32, u32)>> {
        if units.is_empty() {
            return Ok(HashMap::new());
        }
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();

        let mut unit_lbas: Vec<Vec<Lba>> = Vec::with_capacity(units.len());
        let mut unit_keys: Vec<Vec<[u8; 8]>> = Vec::with_capacity(units.len());
        for (_vol_id, batch_values, _) in units {
            let lbas: Vec<Lba> = batch_values.iter().map(|(lba, _)| *lba).collect();
            let keys: Vec<[u8; 8]> = lbas.iter().map(|lba| encode_blockmap_key(*lba)).collect();
            unit_lbas.push(lbas);
            unit_keys.push(keys);
        }
        let _blockmap_guards =
            self.lock_blockmap_keys(unit_keys.iter().flat_map(|keys| keys.iter()));

        let mut new_refcounts: HashMap<Pba, u32> = HashMap::new();
        let mut aggregated_decrements: HashMap<Pba, (u32, u32)> = HashMap::new();

        for (unit_idx, (vol_id, batch_values, new_refcount)) in units.iter().enumerate() {
            let new_pba = batch_values.first().map(|(_, v)| v.pba);

            let old_mappings = self.redb.batch_get_mappings(&vol_id.0, &unit_lbas[unit_idx])?;
            for old in old_mappings.iter().flatten() {
                if Some(old.pba) != new_pba {
                    let old_blocks = old.unit_compressed_size.div_ceil(BLOCK_SIZE);
                    let entry = aggregated_decrements
                        .entry(old.pba)
                        .or_insert((0, old_blocks));
                    entry.0 += 1;
                    entry.1 = entry.1.max(old_blocks);
                }
            }

            if let Some((_, first_val)) = batch_values.first() {
                *new_refcounts.entry(first_val.pba).or_insert(0) += *new_refcount;
            }
        }

        let mut touched_pbas: Vec<Pba> = new_refcounts.keys().copied().collect();
        touched_pbas.extend(aggregated_decrements.keys().copied());
        touched_pbas.sort_unstable();
        touched_pbas.dedup();
        let _refcount_guards = self.lock_refcount_pbas(touched_pbas.iter().copied());
        let current_refcounts = self.refcounts_by_pba(&touched_pbas)?;

        let mut batch = WriteBatch::default();
        let mut newly_zeroed: HashMap<Pba, (u32, u32)> = HashMap::new();

        for pba in touched_pbas {
            let current_rc = current_refcounts.get(&pba).copied().unwrap_or(0);
            let increments = new_refcounts.get(&pba).copied().unwrap_or(0);
            let decrements = aggregated_decrements
                .get(&pba)
                .map(|(d, _)| *d)
                .unwrap_or(0);
            let final_rc = current_rc
                .saturating_add(increments)
                .saturating_sub(decrements);
            let rc_key = encode_refcount_key(pba);
            if final_rc == 0 {
                batch.delete_cf(&cf_refcount, &rc_key);
                if current_rc > 0 {
                    if let Some((dec, blocks)) = aggregated_decrements.get(&pba) {
                        newly_zeroed.insert(pba, (*dec, *blocks));
                    }
                }
            } else {
                batch.put_cf(&cf_refcount, &rc_key, &encode_refcount_value(final_rc));
            }
        }

        self.db.write_opt(batch, &self.hot_write_opts)?;

        // Commit all blockmap updates in a single redb transaction.
        let mut redb_entries: Vec<(&str, Lba, BlockmapValue)> = Vec::new();
        for (vol_id, batch_values, _) in units {
            for (lba, value) in batch_values.iter() {
                redb_entries.push((vol_id.0.as_str(), *lba, *value));
            }
        }
        self.redb.batch_put_mappings(&redb_entries)?;

        Ok(newly_zeroed)
    }
}
