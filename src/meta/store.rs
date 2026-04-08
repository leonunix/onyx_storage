use std::collections::HashMap;
use std::sync::Mutex;

use rocksdb::{BlockBasedOptions, ColumnFamilyDescriptor, Options, ReadOptions, WriteBatch, DB};

use crate::config::MetaConfig;
use crate::error::{OnyxError, OnyxResult};
use crate::meta::schema::*;
use crate::types::{Lba, Pba, VolumeConfig, VolumeId};

pub struct MetaStore {
    db: DB,
    /// Mutex protecting ALL refcount read-modify-write operations.
    /// Covers: atomic_dedup_hit(), atomic_batch_write(), atomic_batch_write_packed(),
    /// atomic_remap(), delete_volume(). Without serialization, concurrent operations
    /// on the same PBA's refcount lose updates (e.g., dedup hit +1 racing with
    /// overwrite -1 on the same PBA).
    refcount_lock: Mutex<()>,
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

        // Dedup index CF: content_hash(32B) → DedupEntry(27B), high bloom FPR for mostly-miss workloads
        let mut dedup_index_opts = Options::default();
        let mut dedup_index_block_opts = BlockBasedOptions::default();
        dedup_index_block_opts.set_bloom_filter(15.0, false);
        dedup_index_opts.set_block_based_table_factory(&dedup_index_block_opts);
        let cf_dedup_index = ColumnFamilyDescriptor::new(CF_DEDUP_INDEX, dedup_index_opts);

        // Dedup reverse CF: pba(8B)+hash(32B) → empty, for eager cleanup on PBA free
        let mut dedup_reverse_opts = Options::default();
        let mut dedup_reverse_block_opts = BlockBasedOptions::default();
        dedup_reverse_block_opts.set_bloom_filter(10.0, false);
        dedup_reverse_opts.set_block_based_table_factory(&dedup_reverse_block_opts);
        let cf_dedup_reverse = ColumnFamilyDescriptor::new(CF_DEDUP_REVERSE, dedup_reverse_opts);

        let db = DB::open_cf_descriptors(
            &db_opts,
            &config.rocksdb_path,
            vec![
                cf_volumes,
                cf_blockmap,
                cf_refcount,
                cf_dedup_index,
                cf_dedup_reverse,
            ],
        )?;

        Ok(Self {
            db,
            refcount_lock: Mutex::new(()),
        })
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
    /// Delete a volume and all its associated blockmap/refcount entries.
    ///
    /// Returns `Vec<(Pba, block_count)>` for freed extents. The block_count
    /// is derived from `unit_compressed_size.div_ceil(BLOCK_SIZE)` so the
    /// caller can free the correct number of physical blocks.
    pub fn delete_volume(&self, id: &VolumeId) -> OnyxResult<Vec<(Pba, u32)>> {
        let _guard = self.refcount_lock.lock().unwrap();
        let cf_volumes = self.db.cf_handle(CF_VOLUMES).unwrap();
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();

        let prefix = blockmap_key_prefix(id)?;

        // Phase 1: scan all blockmap entries, collect PBA decrement counts,
        // block counts per PBA, and blockmap keys to delete.
        let mut pba_decrements: HashMap<Pba, u32> = HashMap::new();
        let mut pba_block_counts: HashMap<Pba, u32> = HashMap::new();
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
                        let blocks = bv.unit_compressed_size.div_ceil(crate::types::BLOCK_SIZE);
                        // All LBAs in the same compression unit share the same
                        // pba and unit_compressed_size, so max() is correct.
                        pba_block_counts
                            .entry(bv.pba)
                            .and_modify(|b| *b = (*b).max(blocks))
                            .or_insert(blocks);
                    }
                }
                blockmap_keys_to_delete.push(key.to_vec());
            }
            iter.next();
        }
        iter.status()?;

        // Phase 2: read current refcounts once per PBA, compute final values
        let mut batch = WriteBatch::default();
        let mut freed_extents = Vec::new();

        for (pba, decrement) in &pba_decrements {
            let current_rc = self.get_refcount(*pba)?;
            let new_rc = current_rc.saturating_sub(*decrement);
            let rc_key = encode_refcount_key(*pba);
            if new_rc == 0 {
                batch.delete_cf(&cf_refcount, &rc_key);
                // Clean up dedup entries for freed PBA
                self.cleanup_dedup_for_pba(*pba, &mut batch)?;
                let block_count = pba_block_counts.get(pba).copied().unwrap_or(1);
                freed_extents.push((*pba, block_count));
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
            freed_extents = freed_extents.len(),
            "volume deleted with blockmap/refcount cleanup"
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
        let cf = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let keys: Vec<Vec<u8>> = lbas
            .iter()
            .map(|lba| encode_blockmap_key(vol_id, *lba))
            .collect::<OnyxResult<Vec<_>>>()?;
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

    /// Batch-read refcounts for multiple PBAs in one RocksDB multi_get_cf call.
    pub fn multi_get_refcounts(&self, pbas: &[Pba]) -> OnyxResult<Vec<u32>> {
        if pbas.is_empty() {
            return Ok(Vec::new());
        }
        let cf = self.db.cf_handle(CF_REFCOUNT).unwrap();
        let keys: Vec<[u8; 8]> = pbas.iter().map(|pba| encode_refcount_key(*pba)).collect();
        let results = self
            .db
            .multi_get_cf(keys.iter().map(|k| (&cf, k.as_slice())));
        let mut out = Vec::with_capacity(pbas.len());
        for result in results {
            match result {
                Ok(Some(data)) => out.push(decode_refcount_value(&data).unwrap_or(0)),
                Ok(None) => out.push(0),
                Err(e) => return Err(OnyxError::Meta(e)),
            }
        }
        Ok(out)
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
        let _guard = self.refcount_lock.lock().unwrap();
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
        let _guard = self.refcount_lock.lock().unwrap();
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
        let _guard = self.refcount_lock.lock().unwrap();
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

    /// Atomically write blockmap entries from multiple volumes sharing the same PBA
    /// (packed slot). Sets the total refcount for the PBA and decrements old PBAs.
    ///
    /// `batch_values`: vec of (vol_id, lba, BlockmapValue) — each entry may belong to a different volume
    /// `new_pba`: the shared PBA for the packed slot
    /// `new_refcount`: total refcount for new_pba (sum of all lba_counts across fragments)
    /// `old_pba_decrements`: aggregated decrements per old PBA
    pub fn atomic_batch_write_packed(
        &self,
        batch_values: &[(VolumeId, Lba, BlockmapValue)],
        new_pba: Pba,
        new_refcount: u32,
        old_pba_decrements: &std::collections::HashMap<Pba, u32>,
    ) -> OnyxResult<()> {
        let _guard = self.refcount_lock.lock().unwrap();
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();

        let mut batch = WriteBatch::default();

        for (vol_id, lba, value) in batch_values {
            let bm_key = encode_blockmap_key(vol_id, *lba)?;
            let bm_val = encode_blockmap_value(value);
            batch.put_cf(&cf_blockmap, &bm_key, &bm_val);
        }

        let rc_key = encode_refcount_key(new_pba);
        batch.put_cf(&cf_refcount, &rc_key, &encode_refcount_value(new_refcount));

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

    /// Atomically write blockmap entries for multiple units, each with its own PBA.
    /// Combines all units into a single RocksDB WriteBatch for one WAL sync.
    ///
    /// Each item in `units`: (vol_id, blockmap entries, new_pba refcount, old PBA decrements)
    pub fn atomic_batch_write_multi(
        &self,
        units: &[(
            &VolumeId,
            &[(Lba, BlockmapValue)],
            u32, // new_refcount
            &HashMap<Pba, u32>, // old_pba_decrements
        )],
    ) -> OnyxResult<()> {
        if units.is_empty() {
            return Ok(());
        }
        let _guard = self.refcount_lock.lock().unwrap();
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();

        let mut batch = WriteBatch::default();

        // Aggregate all old_pba decrements across units to handle correctly
        // when multiple units reference the same old PBA.
        let mut aggregated_decrements: HashMap<Pba, u32> = HashMap::new();

        for (vol_id, batch_values, new_refcount, old_pba_decrements) in units {
            // Write blockmap entries
            for (lba, value) in *batch_values {
                let bm_key = encode_blockmap_key(vol_id, *lba)?;
                let bm_val = encode_blockmap_value(value);
                batch.put_cf(&cf_blockmap, &bm_key, &bm_val);
            }

            // Set new PBA refcount
            if let Some((_, first_val)) = batch_values.first() {
                let rc_key = encode_refcount_key(first_val.pba);
                batch.put_cf(&cf_refcount, &rc_key, &encode_refcount_value(*new_refcount));
            }

            // Aggregate old PBA decrements
            for (old_pba, decrement) in *old_pba_decrements {
                *aggregated_decrements.entry(*old_pba).or_insert(0) += decrement;
            }
        }

        // Apply aggregated decrements
        for (old_pba, decrement) in &aggregated_decrements {
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

    /// Find all unique volume IDs that have blockmap entries pointing to a given PBA.
    /// Used by write_hole_fill to acquire lifecycle locks on all volumes in a packed slot.
    pub fn find_volume_ids_by_pba(&self, target_pba: Pba) -> OnyxResult<Vec<String>> {
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let mut vol_ids = std::collections::HashSet::new();
        let iter = self
            .db
            .iterator_cf(&cf_blockmap, rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, value) = item?;
            if let Some(bv) = decode_blockmap_value(&value) {
                if bv.pba == target_pba {
                    if let Some((vol_id_str, _)) = decode_blockmap_key(&key) {
                        vol_ids.insert(vol_id_str);
                    }
                }
            }
        }
        let mut result: Vec<String> = vol_ids.into_iter().collect();
        result.sort();
        Ok(result)
    }

    /// Check if any blockmap entry at `target_pba` has a fragment whose byte
    /// range `[bv.slot_offset, bv.slot_offset + bv.unit_compressed_size)` overlaps
    /// with `[fill_offset, fill_offset + fill_size)`.
    ///
    /// This prevents filling a hole that has been (partially) reclaimed by
    /// another write or that overlaps with a live fragment starting at a
    /// different offset.
    pub fn has_overlap_at_pba(
        &self,
        target_pba: Pba,
        fill_offset: u16,
        fill_size: u16,
    ) -> OnyxResult<bool> {
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let fill_start = fill_offset as u32;
        let fill_end = fill_start + fill_size as u32;
        let iter = self
            .db
            .iterator_cf(&cf_blockmap, rocksdb::IteratorMode::Start);
        for item in iter {
            let (_, value) = item?;
            if let Some(bv) = decode_blockmap_value(&value) {
                if bv.pba == target_pba {
                    let frag_start = bv.slot_offset as u32;
                    let frag_end = frag_start + bv.unit_compressed_size;
                    // Interval overlap: [a, b) ∩ [c, d) ≠ ∅  iff  a < d && c < b
                    if fill_start < frag_end && frag_start < fill_end {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }

    /// Scan all blockmap entries, calling the provided callback for each (key, value) pair.
    /// Used by GC scanner to identify compression units with dead blocks.
    pub fn scan_all_blockmap_entries(
        &self,
        callback: &mut dyn FnMut(&[u8], &[u8]),
    ) -> OnyxResult<()> {
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let iter = self
            .db
            .iterator_cf(&cf_blockmap, rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, value) = item?;
            callback(&key, &value);
        }
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

    /// Atomically handle a dedup hit: write blockmap entry, increment existing PBA refcount,
    /// and optionally decrement old PBA refcount.
    ///
    /// Protected by `refcount_lock` to serialize concurrent read-modify-write
    /// on refcounts from multiple dedup workers / scanner.
    pub fn atomic_dedup_hit(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        new_value: &BlockmapValue,
        old_pba: Option<Pba>,
    ) -> OnyxResult<()> {
        let _guard = self.refcount_lock.lock().unwrap();

        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();

        let mut batch = WriteBatch::default();

        // Write blockmap entry
        let bm_key = encode_blockmap_key(vol_id, lba)?;
        let bm_val = encode_blockmap_value(new_value);
        batch.put_cf(&cf_blockmap, &bm_key, &bm_val);

        let same_pba = old_pba.map_or(false, |old| old == new_value.pba);

        if !same_pba {
            // Increment refcount for existing PBA (not set to 1!)
            let current_rc = self.get_refcount(new_value.pba)?;
            let new_rc = current_rc + 1;
            let rc_key = encode_refcount_key(new_value.pba);
            batch.put_cf(&cf_refcount, &rc_key, &encode_refcount_value(new_rc));

            // Decrement old PBA refcount
            if let Some(old) = old_pba {
                let old_count = self.get_refcount(old)?;
                let old_new = old_count.saturating_sub(1);
                let old_rc_key = encode_refcount_key(old);
                if old_new == 0 {
                    batch.delete_cf(&cf_refcount, &old_rc_key);
                } else {
                    batch.put_cf(&cf_refcount, &old_rc_key, &encode_refcount_value(old_new));
                }
            }
        }
        // If same PBA: blockmap is refreshed but refcount stays unchanged

        self.db.write(batch)?;
        Ok(())
    }

    // --- Dedup operations ---

    /// Look up a content hash in the dedup index.
    pub fn get_dedup_entry(&self, hash: &ContentHash) -> OnyxResult<Option<DedupEntry>> {
        let cf = self.db.cf_handle(CF_DEDUP_INDEX).unwrap();
        match self.db.get_cf(&cf, hash)? {
            Some(data) => Ok(decode_dedup_entry(&data)),
            None => Ok(None),
        }
    }

    /// Insert dedup index + reverse entries into an existing WriteBatch.
    /// Called from the writer thread after a successful write to populate the dedup index.
    pub fn put_dedup_entries_in_batch(
        &self,
        batch: &mut WriteBatch,
        entries: &[(ContentHash, DedupEntry)],
    ) {
        let cf_index = self.db.cf_handle(CF_DEDUP_INDEX).unwrap();
        let cf_reverse = self.db.cf_handle(CF_DEDUP_REVERSE).unwrap();
        for (hash, entry) in entries {
            let val = encode_dedup_entry(entry);
            batch.put_cf(&cf_index, hash, &val);
            let rev_key = encode_dedup_reverse_key(entry.pba, hash);
            batch.put_cf(&cf_reverse, &rev_key, &[]);
        }
    }

    /// Write dedup index + reverse entries atomically.
    pub fn put_dedup_entries(&self, entries: &[(ContentHash, DedupEntry)]) -> OnyxResult<()> {
        let mut batch = WriteBatch::default();
        self.put_dedup_entries_in_batch(&mut batch, entries);
        self.db.write(batch)?;
        Ok(())
    }

    /// Iterate all dedup index entries.
    pub fn iter_dedup_entries(&self) -> OnyxResult<Vec<(ContentHash, DedupEntry)>> {
        let cf = self.db.cf_handle(CF_DEDUP_INDEX).unwrap();
        let mut results = Vec::new();
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, value) = item?;
            if key.len() != 32 {
                continue;
            }
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&key);
            if let Some(entry) = decode_dedup_entry(&value) {
                results.push((hash, entry));
            }
        }
        Ok(results)
    }

    /// Iterate all dedup reverse entries.
    pub fn iter_dedup_reverse_entries(&self) -> OnyxResult<Vec<(Pba, ContentHash)>> {
        let cf = self.db.cf_handle(CF_DEDUP_REVERSE).unwrap();
        let mut results = Vec::new();
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, _) = item?;
            if let Some((pba, hash)) = decode_dedup_reverse_key(&key) {
                results.push((pba, hash));
            }
        }
        Ok(results)
    }

    /// Delete a single dedup index entry by hash (best-effort stale cleanup).
    pub fn delete_dedup_index(&self, hash: &ContentHash) -> OnyxResult<()> {
        let cf = self.db.cf_handle(CF_DEDUP_INDEX).unwrap();
        self.db.delete_cf(&cf, hash)?;
        Ok(())
    }

    /// Clean up dedup index + reverse entries for a given PBA.
    /// Called when refcount → 0 (PBA is freed).
    /// Only deletes the forward index entry if it still points to this PBA —
    /// the same hash may have been re-registered to a different PBA by a
    /// concurrent write, and we must not delete that newer mapping.
    /// Adds deletions to the provided WriteBatch for atomicity.
    pub fn cleanup_dedup_for_pba(&self, pba: Pba, batch: &mut WriteBatch) -> OnyxResult<()> {
        let cf_index = self.db.cf_handle(CF_DEDUP_INDEX).unwrap();
        let cf_reverse = self.db.cf_handle(CF_DEDUP_REVERSE).unwrap();

        // Prefix scan dedup_reverse for this PBA
        let prefix = pba.0.to_be_bytes();
        let mut iter = self.db.raw_iterator_cf(&cf_reverse);
        iter.seek(&prefix);
        while iter.valid() {
            if let Some(key) = iter.key() {
                if !key.starts_with(&prefix) {
                    break;
                }
                if let Some((_pba, hash)) = decode_dedup_reverse_key(key) {
                    // Only delete forward index if it still points to THIS PBA.
                    // A concurrent miss-write may have re-registered the same
                    // hash → different PBA; deleting that would be wrong.
                    if let Some(current_entry) = self.get_dedup_entry(&hash)? {
                        if current_entry.pba == pba {
                            batch.delete_cf(&cf_index, &hash);
                        }
                    }
                    // Always delete the reverse entry for this PBA
                    batch.delete_cf(&cf_reverse, key);
                }
            }
            iter.next();
        }
        iter.status()?;
        Ok(())
    }

    /// Standalone cleanup_dedup_for_pba that writes its own batch.
    pub fn cleanup_dedup_for_pba_standalone(&self, pba: Pba) -> OnyxResult<()> {
        let mut batch = WriteBatch::default();
        self.cleanup_dedup_for_pba(pba, &mut batch)?;
        if !batch.is_empty() {
            self.db.write(batch)?;
        }
        Ok(())
    }

    /// Batch cleanup dedup index for multiple PBAs in one WriteBatch.
    /// Much faster than calling cleanup_dedup_for_pba_standalone per PBA.
    pub fn cleanup_dedup_for_pbas_batch(&self, pbas: &[Pba]) -> OnyxResult<()> {
        if pbas.is_empty() {
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        for pba in pbas {
            self.cleanup_dedup_for_pba(*pba, &mut batch)?;
        }
        if !batch.is_empty() {
            self.db.write(batch)?;
        }
        Ok(())
    }

    /// Scan blockmap for entries with DEDUP_SKIPPED flag set.
    /// Returns (vol_id, lba, BlockmapValue) tuples, up to `limit`.
    pub fn scan_dedup_skipped(
        &self,
        limit: usize,
    ) -> OnyxResult<Vec<(String, Lba, BlockmapValue)>> {
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let mut results = Vec::new();
        let iter = self
            .db
            .iterator_cf(&cf_blockmap, rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, value) = item?;
            if let Some(bv) = decode_blockmap_value(&value) {
                if bv.flags & FLAG_DEDUP_SKIPPED != 0 {
                    if let Some((vol_id, lba)) = decode_blockmap_key(&key) {
                        results.push((vol_id, lba, bv));
                        if results.len() >= limit {
                            break;
                        }
                    }
                }
            }
        }
        Ok(results)
    }

    /// Update a single blockmap entry's flags (e.g., clear DEDUP_SKIPPED).
    pub fn update_blockmap_flags(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        new_flags: u8,
    ) -> OnyxResult<()> {
        let cf = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let key = encode_blockmap_key(vol_id, lba)?;
        match self.db.get_cf(&cf, &key)? {
            Some(data) => {
                if let Some(mut bv) = decode_blockmap_value(&data) {
                    bv.flags = new_flags;
                    let val = encode_blockmap_value(&bv);
                    self.db.put_cf(&cf, &key, &val)?;
                }
                Ok(())
            }
            None => Ok(()),
        }
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
