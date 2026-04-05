use std::collections::HashMap;

use rocksdb::{BlockBasedOptions, ColumnFamilyDescriptor, Options, ReadOptions, WriteBatch, DB};

use crate::config::MetaConfig;
use crate::error::{OnyxError, OnyxResult};
use crate::meta::schema::*;
use crate::types::{Lba, Pba, VolumeConfig, VolumeId};

pub struct MetaStore {
    db: DB,
}

impl MetaStore {
    pub fn open(config: &MetaConfig) -> OnyxResult<Self> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_keep_log_file_num(5);

        if let Some(ref wal_dir) = config.wal_dir {
            db_opts.set_wal_dir(wal_dir);
        }

        // Volumes CF: small, default options
        let cf_volumes = ColumnFamilyDescriptor::new(CF_VOLUMES, Options::default());

        // Blockmap CF: hot path, bloom filter + LZ4
        // Keys are length-prefixed volume ID + LBA, no fixed prefix size.
        let mut blockmap_opts = Options::default();
        let mut blockmap_block_opts = BlockBasedOptions::default();
        blockmap_block_opts.set_bloom_filter(10.0, false);
        blockmap_block_opts.set_block_size(4096);
        if config.block_cache_mb > 0 {
            let cache = rocksdb::Cache::new_lru_cache(config.block_cache_mb * 1024 * 1024);
            blockmap_block_opts.set_block_cache(&cache);
        }
        blockmap_opts.set_block_based_table_factory(&blockmap_block_opts);
        blockmap_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        let cf_blockmap = ColumnFamilyDescriptor::new(CF_BLOCKMAP, blockmap_opts);

        // Refcount CF: bloom filter
        let mut refcount_opts = Options::default();
        let mut refcount_block_opts = BlockBasedOptions::default();
        refcount_block_opts.set_bloom_filter(10.0, false);
        refcount_opts.set_block_based_table_factory(&refcount_block_opts);
        let cf_refcount = ColumnFamilyDescriptor::new(CF_REFCOUNT, refcount_opts);

        let db = DB::open_cf_descriptors(
            &db_opts,
            &config.rocksdb_path,
            vec![cf_volumes, cf_blockmap, cf_refcount],
        )?;

        Ok(Self { db })
    }

    // --- Volume operations ---

    pub fn put_volume(&self, config: &VolumeConfig) -> OnyxResult<()> {
        let id_len = config.id.0.as_bytes().len();
        if id_len == 0 || id_len > MAX_VOLUME_ID_BYTES {
            return Err(OnyxError::Config(format!(
                "volume ID must be 1..{} bytes, got {}",
                MAX_VOLUME_ID_BYTES, id_len
            )));
        }

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
    /// Returns the list of freed PBAs (for space allocator reclamation).
    /// Delete a volume and all its associated blockmap/refcount entries.
    ///
    /// Aggregates per-PBA decrement counts first, then reads current refcounts once
    /// and applies the total delta in a single WriteBatch. This is correct even when
    /// multiple LBAs in this volume map to the same PBA (dedup scenario).
    pub fn delete_volume(&self, id: &VolumeId) -> OnyxResult<Vec<Pba>> {
        let cf_volumes = self.db.cf_handle(CF_VOLUMES).unwrap();
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();

        let prefix = blockmap_key_prefix(id)?;

        // Phase 1: scan all blockmap entries, collect PBA decrement counts
        // and blockmap keys to delete.
        let mut pba_decrements: HashMap<Pba, u32> = HashMap::new();
        let mut blockmap_keys_to_delete: Vec<Vec<u8>> = Vec::new();

        let mut iter = self.db.raw_iterator_cf(&cf_blockmap);
        iter.seek(&prefix);
        while iter.valid() {
            if let Some(key) = iter.key() {
                if !key.starts_with(&prefix) {
                    break;
                }
                if let Some(val) = iter.value() {
                    if let Some(bv) = decode_blockmap_value(val) {
                        *pba_decrements.entry(bv.pba).or_insert(0) += 1;
                    }
                }
                blockmap_keys_to_delete.push(key.to_vec());
            }
            iter.next();
        }
        iter.status()?;

        // Phase 2: read current refcounts once per PBA, compute final values
        let mut batch = WriteBatch::default();
        let mut freed_pbas = Vec::new();

        for (pba, decrement) in &pba_decrements {
            let current_rc = self.get_refcount(*pba)?;
            let new_rc = current_rc.saturating_sub(*decrement);
            let rc_key = encode_refcount_key(*pba);
            if new_rc == 0 {
                batch.delete_cf(&cf_refcount, &rc_key);
                freed_pbas.push(*pba);
            } else {
                batch.put_cf(&cf_refcount, &rc_key, &encode_refcount_value(new_rc));
            }
        }

        // Phase 3: delete all blockmap entries + volume record in one batch
        for key in &blockmap_keys_to_delete {
            batch.delete_cf(&cf_blockmap, key);
        }

        let vol_key = encode_volume_key(&id.0);
        batch.delete_cf(&cf_volumes, &vol_key);

        self.db.write(batch)?;

        tracing::info!(
            volume = %id,
            freed_pbas = freed_pbas.len(),
            "volume deleted with blockmap/refcount cleanup"
        );

        Ok(freed_pbas)
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

    // --- Blockmap operations ---

    pub fn put_mapping(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        value: &BlockmapValue,
    ) -> OnyxResult<()> {
        let cf = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let key = encode_blockmap_key(vol_id, lba)?;
        let val = encode_blockmap_value(value);
        self.db.put_cf(&cf, &key, &val)?;
        Ok(())
    }

    pub fn get_mapping(&self, vol_id: &VolumeId, lba: Lba) -> OnyxResult<Option<BlockmapValue>> {
        let cf = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let key = encode_blockmap_key(vol_id, lba)?;
        match self.db.get_cf(&cf, &key)? {
            Some(data) => Ok(decode_blockmap_value(&data)),
            None => Ok(None),
        }
    }

    pub fn delete_mapping(&self, vol_id: &VolumeId, lba: Lba) -> OnyxResult<()> {
        let cf = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let key = encode_blockmap_key(vol_id, lba)?;
        self.db.delete_cf(&cf, &key)?;
        Ok(())
    }

    pub fn get_mappings_range(
        &self,
        vol_id: &VolumeId,
        start: Lba,
        end: Lba,
    ) -> OnyxResult<Vec<(Lba, BlockmapValue)>> {
        let cf = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let start_key = encode_blockmap_key(vol_id, start)?;
        let end_key = encode_blockmap_key(vol_id, end)?;
        let prefix = blockmap_key_prefix(vol_id)?;

        let mut read_opts = ReadOptions::default();
        read_opts.set_iterate_upper_bound(end_key);

        let mut results = Vec::new();
        let mut iter = self.db.raw_iterator_cf_opt(&cf, read_opts);
        iter.seek(&start_key);

        while iter.valid() {
            if let (Some(key), Some(val)) = (iter.key(), iter.value()) {
                if !key.starts_with(&prefix) {
                    break;
                }
                if let Some((_, lba)) = decode_blockmap_key(key) {
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

    // --- Refcount operations ---

    pub fn get_refcount(&self, pba: Pba) -> OnyxResult<u32> {
        let cf = self.db.cf_handle(CF_REFCOUNT).unwrap();
        let key = encode_refcount_key(pba);
        match self.db.get_cf(&cf, &key)? {
            Some(data) => Ok(decode_refcount_value(&data).unwrap_or(0)),
            None => Ok(0),
        }
    }

    pub fn set_refcount(&self, pba: Pba, count: u32) -> OnyxResult<()> {
        let cf = self.db.cf_handle(CF_REFCOUNT).unwrap();
        let key = encode_refcount_key(pba);
        if count == 0 {
            self.db.delete_cf(&cf, &key)?;
        } else {
            self.db.put_cf(&cf, &key, &encode_refcount_value(count))?;
        }
        Ok(())
    }

    pub fn increment_refcount(&self, pba: Pba) -> OnyxResult<u32> {
        let current = self.get_refcount(pba)?;
        let new_count = current + 1;
        self.set_refcount(pba, new_count)?;
        Ok(new_count)
    }

    pub fn decrement_refcount(&self, pba: Pba) -> OnyxResult<u32> {
        let current = self.get_refcount(pba)?;
        let new_count = current.saturating_sub(1);
        self.set_refcount(pba, new_count)?;
        Ok(new_count)
    }

    // --- Atomic batch operations ---

    /// Atomically write a blockmap entry + set refcount=1
    pub fn atomic_write_mapping(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        value: &BlockmapValue,
    ) -> OnyxResult<()> {
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();

        let bm_key = encode_blockmap_key(vol_id, lba)?;
        let bm_val = encode_blockmap_value(value);
        let rc_key = encode_refcount_key(value.pba);
        let rc_val = encode_refcount_value(1);

        let mut batch = WriteBatch::default();
        batch.put_cf(&cf_blockmap, &bm_key, &bm_val);
        batch.put_cf(&cf_refcount, &rc_key, &rc_val);
        self.db.write(batch)?;
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
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();

        let mut batch = WriteBatch::default();

        let bm_key = encode_blockmap_key(vol_id, lba)?;
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

        self.db.write(batch)?;
        Ok(())
    }

    /// Atomically write multiple blockmap entries sharing the same PBA (compression unit),
    /// set new PBA refcount, and decrement old PBA refcounts.
    ///
    /// `batch_values`: vec of (lba, BlockmapValue) — each LBA gets its own entry
    /// `new_refcount`: refcount to set for the new PBA (typically = lba_count)
    /// `old_pba_decrements`: aggregated decrements per old PBA (from HashMap)
    pub fn atomic_batch_write(
        &self,
        vol_id: &VolumeId,
        batch_values: &[(Lba, BlockmapValue)],
        new_refcount: u32,
        old_pba_decrements: &std::collections::HashMap<Pba, u32>,
    ) -> OnyxResult<()> {
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();

        let mut batch = WriteBatch::default();

        // Write all blockmap entries
        for (lba, value) in batch_values {
            let bm_key = encode_blockmap_key(vol_id, *lba)?;
            let bm_val = encode_blockmap_value(value);
            batch.put_cf(&cf_blockmap, &bm_key, &bm_val);
        }

        // Set new PBA refcount (all entries share the same PBA)
        if let Some((_, first_val)) = batch_values.first() {
            let rc_key = encode_refcount_key(first_val.pba);
            batch.put_cf(&cf_refcount, &rc_key, &encode_refcount_value(new_refcount));
        }

        // Decrement old PBA refcounts
        for (old_pba, decrement) in old_pba_decrements {
            let current_rc = self.get_refcount(*old_pba)?;
            let new_rc = current_rc.saturating_sub(*decrement);
            let rc_key = encode_refcount_key(*old_pba);
            if new_rc == 0 {
                batch.delete_cf(&cf_refcount, &rc_key);
            } else {
                batch.put_cf(&cf_refcount, &rc_key, &encode_refcount_value(new_rc));
            }
        }

        self.db.write(batch)?;
        Ok(())
    }

    /// Iterate all refcount entries (for space allocator rebuild)
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

    /// Iterate all allocated physical blocks. Compression units can span multiple
    /// 4KB slots, so blockmap is the source of truth for allocator rebuild.
    /// Refcount keys are unioned in as a fallback for older single-slot metadata.
    pub fn iter_allocated_blocks(&self) -> OnyxResult<Vec<Pba>> {
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let mut allocated = std::collections::BTreeSet::new();

        let iter = self
            .db
            .iterator_cf(&cf_blockmap, rocksdb::IteratorMode::Start);
        for item in iter {
            let (_, value) = item?;
            if let Some(bv) = decode_blockmap_value(&value) {
                let blocks = bv.unit_compressed_size.div_ceil(crate::types::BLOCK_SIZE);
                for block in 0..blocks {
                    allocated.insert(Pba(bv.pba.0 + block as u64));
                }
            }
        }

        for (pba, _) in self.iter_refcounts()? {
            allocated.insert(pba);
        }

        Ok(allocated.into_iter().collect())
    }
}
