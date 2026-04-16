use std::collections::HashMap;

use rocksdb::{ReadOptions, WriteBatch};

use crate::error::{OnyxError, OnyxResult};
use crate::meta::schema::*;
use crate::types::{Lba, Pba, VolumeConfig, VolumeId, BLOCK_SIZE};

use super::MetaStore;

impl MetaStore {
    pub fn put_volume(&self, config: &VolumeConfig) -> OnyxResult<()> {
        let id_len = config.id.0.as_bytes().len();
        if id_len == 0 || id_len > MAX_VOLUME_ID_BYTES {
            return Err(OnyxError::Config(format!(
                "volume ID must be 1..{} bytes, got {}",
                MAX_VOLUME_ID_BYTES, id_len
            )));
        }

        self.create_blockmap_cf(&config.id.0)?;

        let cf = self.db.cf_handle(CF_VOLUMES).unwrap();
        let key = encode_volume_key(&config.id.0);
        let value = bincode::serialize(config).map_err(|e| OnyxError::Config(e.to_string()))?;
        self.db.put_cf(&cf, &key, &value)?;
        Ok(())
    }

    pub fn get_volume(&self, id: &VolumeId) -> OnyxResult<Option<VolumeConfig>> {
        let cf = self.db.cf_handle(CF_VOLUMES).unwrap();
        let key = encode_volume_key(&id.0);
        match self.db.get_cf(&cf, &key)? {
            Some(data) => {
                let config: VolumeConfig =
                    bincode::deserialize(&data).map_err(|e| OnyxError::Config(e.to_string()))?;
                Ok(Some(config))
            }
            None => Ok(None),
        }
    }

    /// Delete a volume and all its associated blockmap/refcount entries.
    ///
    /// Aggregates per-PBA decrement counts first, then reads current refcounts once
    /// and applies the total delta in a single WriteBatch. This is correct even when
    /// multiple LBAs in this volume map to the same PBA (dedup scenario).
    ///
    /// Returns `Vec<(Pba, block_count)>` for freed extents. The block_count
    /// is derived from `unit_compressed_size.div_ceil(BLOCK_SIZE)` so the
    /// caller can free the correct number of physical blocks.
    pub fn delete_volume(&self, id: &VolumeId) -> OnyxResult<Vec<(Pba, u32)>> {
        let cf_volumes = self.db.cf_handle(CF_VOLUMES).unwrap();
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();

        let bm_cf = match self.blockmap_cf(&id.0) {
            Some(cf) => cf,
            None => {
                let vol_key = encode_volume_key(&id.0);
                self.db.delete_cf(&cf_volumes, &vol_key)?;
                return Ok(Vec::new());
            }
        };

        let mut pba_decrements: HashMap<Pba, (u32, u32)> = HashMap::new();
        let mut deleted_entries = 0usize;

        let iter = self.db.iterator_cf(&bm_cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (_, value) = item?;
            if let Some(bv) = decode_blockmap_value(&value) {
                let blocks = bv.unit_compressed_size.div_ceil(BLOCK_SIZE);
                let entry = pba_decrements.entry(bv.pba).or_insert((0, blocks));
                entry.0 += 1;
                entry.1 = entry.1.max(blocks);
            }
            deleted_entries += 1;
        }

        let _refcount_guards = self.lock_refcount_pbas(pba_decrements.keys().copied());

        let mut batch = WriteBatch::default();

        for (pba, (decrement, _)) in &pba_decrements {
            let rc_key = encode_refcount_key(*pba);
            batch.merge_cf(
                &cf_refcount,
                &rc_key,
                encode_refcount_delta(-(*decrement as i32)),
            );
        }

        let vol_key = encode_volume_key(&id.0);
        batch.delete_cf(&cf_volumes, &vol_key);

        self.db.write(batch)?;

        self.drop_blockmap_cf(&id.0)?;

        let mut freed_extents = Vec::new();
        let pba_keys: Vec<Pba> = pba_decrements.keys().copied().collect();
        match self.multi_get_refcounts(&pba_keys) {
            Ok(refcounts) => {
                let mut cleanup_batch = WriteBatch::default();
                let mut need_cleanup = false;
                for (i, pba) in pba_keys.iter().enumerate() {
                    let rc = refcounts.get(i).copied().unwrap_or(0);
                    if rc == 0 {
                        let rc_key = encode_refcount_key(*pba);
                        cleanup_batch.delete_cf(&cf_refcount, &rc_key);
                        let _ = self.cleanup_dedup_for_pba(*pba, &mut cleanup_batch);
                        let block_count = pba_decrements
                            .get(pba)
                            .map(|(_, blocks)| *blocks)
                            .unwrap_or(1);
                        freed_extents.push((*pba, block_count));
                        need_cleanup = true;
                    }
                }
                if need_cleanup {
                    if let Err(e) = self.db.write(cleanup_batch) {
                        tracing::warn!(error = %e, "delete_volume: cleanup batch failed (non-fatal)");
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "delete_volume: refcount readback failed (non-fatal)");
            }
        }

        tracing::info!(
            volume = %id,
            deleted_entries,
            freed_extents = freed_extents.len(),
            "volume deleted with blockmap/refcount cleanup"
        );

        Ok(freed_extents)
    }

    /// Delete blockmap entries for an LBA range and decrement refcounts.
    ///
    /// Follows the same three-phase pattern as `delete_volume`:
    /// 1. Scan blockmap range, collect PBA decrements
    /// 2. Atomic WriteBatch: delete blockmap + merge-decrement refcounts
    /// 3. Read back refcounts, cleanup dedup for zeroed PBAs
    ///
    /// Returns `Vec<(Pba, block_count)>` for freed extents.
    pub fn delete_blockmap_range(
        &self,
        vol_id: &VolumeId,
        start_lba: Lba,
        end_lba: Lba,
    ) -> OnyxResult<Vec<(Pba, u32)>> {
        let _blockmap_guards = self.lock_all_blockmap_stripes();
        let cf_blockmap = self.require_blockmap_cf(&vol_id.0)?;
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();

        let start_key = encode_blockmap_key(start_lba);
        let end_key = encode_blockmap_key(end_lba);

        let mut read_opts = ReadOptions::default();
        read_opts.set_iterate_upper_bound(end_key.to_vec());

        let mut pba_decrements: HashMap<Pba, (u32, u32)> = HashMap::new();
        let mut deleted_entries = 0usize;

        let mut iter = self.db.raw_iterator_cf_opt(&cf_blockmap, read_opts);
        iter.seek(&start_key);
        while iter.valid() {
            if let (Some(_key), Some(val)) = (iter.key(), iter.value()) {
                if let Some(bv) = decode_blockmap_value(val) {
                    let blocks = bv.unit_compressed_size.div_ceil(BLOCK_SIZE);
                    let entry = pba_decrements.entry(bv.pba).or_insert((0, blocks));
                    entry.0 += 1;
                    entry.1 = entry.1.max(blocks);
                }
                deleted_entries += 1;
            }
            iter.next();
        }
        iter.status()?;

        if deleted_entries == 0 {
            return Ok(Vec::new());
        }

        let _refcount_guards = self.lock_refcount_pbas(pba_decrements.keys().copied());

        let mut batch = WriteBatch::default();

        for (pba, (decrement, _)) in &pba_decrements {
            let rc_key = encode_refcount_key(*pba);
            batch.merge_cf(
                &cf_refcount,
                &rc_key,
                encode_refcount_delta(-(*decrement as i32)),
            );
        }

        batch.delete_range_cf(&cf_blockmap, &start_key, &end_key);

        self.db.write(batch)?;

        let mut freed_extents = Vec::new();
        let pba_keys: Vec<Pba> = pba_decrements.keys().copied().collect();
        match self.multi_get_refcounts(&pba_keys) {
            Ok(refcounts) => {
                let mut cleanup_batch = WriteBatch::default();
                let mut need_cleanup = false;
                for (i, pba) in pba_keys.iter().enumerate() {
                    let rc = refcounts.get(i).copied().unwrap_or(0);
                    if rc == 0 {
                        let rc_key = encode_refcount_key(*pba);
                        cleanup_batch.delete_cf(&cf_refcount, &rc_key);
                        let _ = self.cleanup_dedup_for_pba(*pba, &mut cleanup_batch);
                        let block_count = pba_decrements
                            .get(pba)
                            .map(|(_, blocks)| *blocks)
                            .unwrap_or(1);
                        freed_extents.push((*pba, block_count));
                        need_cleanup = true;
                    }
                }
                if need_cleanup {
                    if let Err(e) = self.db.write(cleanup_batch) {
                        tracing::warn!(error = %e, "delete_blockmap_range: cleanup batch failed (non-fatal)");
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "delete_blockmap_range: refcount readback failed (non-fatal)");
            }
        }

        tracing::debug!(
            volume = %vol_id,
            start_lba = start_lba.0,
            end_lba = end_lba.0,
            deleted_keys = deleted_entries,
            freed_extents = freed_extents.len(),
            "blockmap range deleted"
        );

        Ok(freed_extents)
    }

    pub fn list_volumes(&self) -> OnyxResult<Vec<VolumeConfig>> {
        let cf = self.db.cf_handle(CF_VOLUMES).unwrap();
        let mut volumes = Vec::new();
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (_, value) = item?;
            let config: VolumeConfig =
                bincode::deserialize(&value).map_err(|e| OnyxError::Config(e.to_string()))?;
            volumes.push(config);
        }
        Ok(volumes)
    }
}
