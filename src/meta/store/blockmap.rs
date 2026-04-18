use std::collections::HashMap;

use rocksdb::{ReadOptions, WriteBatch};

use crate::error::{OnyxError, OnyxResult};
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
        let cf_blockmap = self.require_blockmap_cf(&vol_id.0)?;
        let key = encode_blockmap_key(lba);
        let _blockmap_guards = self.lock_blockmap_keys([key.as_slice()]);
        let val = encode_blockmap_value(value);
        let mut batch = WriteBatch::default();
        batch.put_cf(&cf_blockmap, &key, &val);
        self.db.write_opt(batch, &self.hot_write_opts)?;
        Ok(())
    }

    pub fn get_mapping(&self, vol_id: &VolumeId, lba: Lba) -> OnyxResult<Option<BlockmapValue>> {
        let cf = match self.blockmap_cf(&vol_id.0) {
            Some(cf) => cf,
            None => return Ok(None),
        };
        let key = encode_blockmap_key(lba);
        match self.db.get_cf(&cf, &key)? {
            Some(data) => Ok(decode_blockmap_value(&data)),
            None => Ok(None),
        }
    }

    /// Batch-read multiple LBA mappings in one RocksDB multi_get_cf call.
    /// Returns results in the same order as the input `lbas` slice.
    pub fn multi_get_mappings(
        &self,
        vol_id: &VolumeId,
        lbas: &[Lba],
    ) -> OnyxResult<Vec<Option<BlockmapValue>>> {
        if lbas.is_empty() {
            return Ok(Vec::new());
        }
        let cf = match self.blockmap_cf(&vol_id.0) {
            Some(cf) => cf,
            None => return Ok(vec![None; lbas.len()]),
        };
        let keys: Vec<[u8; 8]> = lbas.iter().map(|lba| encode_blockmap_key(*lba)).collect();
        let results = self
            .db
            .multi_get_cf(keys.iter().map(|k| (&cf, k.as_slice())));
        let mut out = Vec::with_capacity(lbas.len());
        for result in results {
            match result {
                Ok(Some(data)) => out.push(decode_blockmap_value(&data)),
                Ok(None) => out.push(None),
                Err(e) => return Err(OnyxError::Meta(e)),
            }
        }
        Ok(out)
    }

    pub fn delete_mapping(&self, vol_id: &VolumeId, lba: Lba) -> OnyxResult<()> {
        let cf_blockmap = self.require_blockmap_cf(&vol_id.0)?;
        let key = encode_blockmap_key(lba);
        let _blockmap_guards = self.lock_blockmap_keys([key.as_slice()]);
        let mut batch = WriteBatch::default();
        batch.delete_cf(&cf_blockmap, &key);
        self.db.write_opt(batch, &self.hot_write_opts)?;
        Ok(())
    }

    pub fn get_mappings_range(
        &self,
        vol_id: &VolumeId,
        start: Lba,
        end: Lba,
    ) -> OnyxResult<Vec<(Lba, BlockmapValue)>> {
        let cf = match self.blockmap_cf(&vol_id.0) {
            Some(cf) => cf,
            None => return Ok(Vec::new()),
        };
        let start_key = encode_blockmap_key(start);
        let end_key = encode_blockmap_key(end);

        let mut read_opts = ReadOptions::default();
        read_opts.set_iterate_upper_bound(end_key.to_vec());

        let mut results = Vec::new();
        let mut iter = self.db.raw_iterator_cf_opt(&cf, read_opts);
        iter.seek(&start_key);

        while iter.valid() {
            if let (Some(key), Some(val)) = (iter.key(), iter.value()) {
                if let Some(lba) = decode_blockmap_key(key) {
                    if let Some(bv) = decode_blockmap_value(val) {
                        results.push((lba, bv));
                    }
                }
            }
            iter.next();
        }
        iter.status()?;
        Ok(results)
    }

    /// Atomically write a blockmap entry + set refcount=1
    pub fn atomic_write_mapping(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        value: &BlockmapValue,
    ) -> OnyxResult<()> {
        let cf_blockmap = self.require_blockmap_cf(&vol_id.0)?;
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();

        let bm_key = encode_blockmap_key(lba);
        let _blockmap_guards = self.lock_blockmap_keys([bm_key.as_slice()]);
        let _refcount_guards = self.lock_refcount_pbas([value.pba]);
        let bm_val = encode_blockmap_value(value);
        let rc_key = encode_refcount_key(value.pba);
        let rc_val = encode_refcount_value(1);

        let mut batch = WriteBatch::default();
        batch.put_cf(&cf_blockmap, &bm_key, &bm_val);
        batch.put_cf(&cf_refcount, &rc_key, &rc_val);
        self.db.write_opt(batch, &self.hot_write_opts)?;
        Ok(())
    }

    /// Atomically update mapping: decrement old PBA refcount, write new mapping + new refcount
    pub fn atomic_remap(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        old_pba: Option<Pba>,
        new_value: &BlockmapValue,
    ) -> OnyxResult<()> {
        let cf_blockmap = self.require_blockmap_cf(&vol_id.0)?;
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();

        let bm_key = encode_blockmap_key(lba);
        let _blockmap_guards = self.lock_blockmap_keys([bm_key.as_slice()]);
        let current_old_mapping = self.get_mapping(vol_id, lba)?;
        let current_old_pba = current_old_mapping.as_ref().map(|mapping| mapping.pba);
        let old_pba = current_old_pba.or(old_pba);
        let mut touched_pbas = vec![new_value.pba];
        if let Some(old) = old_pba {
            touched_pbas.push(old);
        }
        let _refcount_guards = self.lock_refcount_pbas(touched_pbas);

        let mut batch = WriteBatch::default();

        let bm_val = encode_blockmap_value(new_value);
        batch.put_cf(&cf_blockmap, &bm_key, &bm_val);

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
        Ok(())
    }

    /// Atomically write multiple blockmap entries sharing the same PBA (compression unit),
    /// set new PBA refcount, and decrement old PBA refcounts.
    ///
    /// Old mappings are re-read **inside** the refcount lock to prevent stale-read races
    /// (another thread remapping an LBA between the caller's read and this write would
    /// cause the wrong old PBA to be decremented, leading to refcount drift).
    ///
    /// Returns `HashMap<Pba, (decrement, block_count)>` for old PBAs that were decremented,
    /// so the caller can check refcounts and free dead PBAs.
    pub fn atomic_batch_write(
        &self,
        vol_id: &VolumeId,
        batch_values: &[(Lba, BlockmapValue)],
        new_refcount: u32,
    ) -> OnyxResult<HashMap<Pba, (u32, u32)>> {
        let cf_blockmap = self.require_blockmap_cf(&vol_id.0)?;
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();

        let lbas: Vec<Lba> = batch_values.iter().map(|(lba, _)| *lba).collect();
        let bm_keys: Vec<[u8; 8]> = lbas.iter().map(|lba| encode_blockmap_key(*lba)).collect();
        let _blockmap_guards = self.lock_blockmap_keys(&bm_keys);

        let new_pba = batch_values.first().map(|(_, v)| v.pba);
        let old_mappings = self.multi_get_mappings(vol_id, &lbas)?;

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

        for ((_, value), bm_key) in batch_values.iter().zip(bm_keys.iter()) {
            let bm_val = encode_blockmap_value(value);
            batch.put_cf(&cf_blockmap, &bm_key, &bm_val);
        }

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
            let old_mapping = self.get_mapping(vol_id, *lba)?;
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

        for ((vol_id, _, value), bm_key) in batch_values.iter().zip(bm_keys.iter()) {
            let vol_cf = self.require_blockmap_cf(&vol_id.0)?;
            let bm_val = encode_blockmap_value(value);
            batch.put_cf(&vol_cf, bm_key, &bm_val);
        }
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
        Ok(newly_zeroed)
    }

    /// Atomically write blockmap entries for multiple units, each with its own PBA.
    /// Combines all units into a single RocksDB WriteBatch for one WAL sync.
    ///
    /// Old mappings are re-read inside the lock (same rationale as `atomic_batch_write`).
    ///
    /// Each item in `units`: (vol_id, blockmap entries, new_pba refcount)
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

        let mut batch = WriteBatch::default();

        let mut new_refcounts: HashMap<Pba, u32> = HashMap::new();
        let mut aggregated_decrements: HashMap<Pba, (u32, u32)> = HashMap::new();

        for (unit_idx, (vol_id, batch_values, new_refcount)) in units.iter().enumerate() {
            let new_pba = batch_values.first().map(|(_, v)| v.pba);

            let old_mappings = self.multi_get_mappings(vol_id, &unit_lbas[unit_idx])?;
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

            let vol_cf = self.require_blockmap_cf(&vol_id.0)?;
            for ((_, value), bm_key) in batch_values.iter().zip(unit_keys[unit_idx].iter()) {
                let bm_val = encode_blockmap_value(value);
                batch.put_cf(&vol_cf, bm_key, &bm_val);
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
        Ok(newly_zeroed)
    }
}
