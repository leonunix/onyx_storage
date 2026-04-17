use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rocksdb::{
    properties, BlockBasedOptions, BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode,
    MultiThreaded, Options, WriteBatch, WriteOptions,
};

use crate::config::MetaConfig;
use crate::error::{OnyxError, OnyxResult};
use crate::meta::redb::RedbStore;
use crate::meta::schema::*;
use crate::metrics::RocksDbMemorySnapshot;
use crate::types::Lba;

use super::{
    refcount_full_merge, refcount_partial_merge, MetaStore, BLOCKMAP_LOCK_STRIPES,
    REFCOUNT_LOCK_STRIPES,
};

impl MetaStore {
    /// Build the Options for a per-volume blockmap CF (bloom + LZ4 + cache).
    fn blockmap_cf_opts(block_cache_mb: usize) -> Options {
        let mut opts = Options::default();
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_bloom_filter(10.0, false);
        block_opts.set_block_size(4096);
        if block_cache_mb > 0 {
            let cache = rocksdb::Cache::new_lru_cache(block_cache_mb * 1024 * 1024);
            block_opts.set_block_cache(&cache);
        }
        opts.set_block_based_table_factory(&block_opts);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts
    }

    pub fn open(config: &MetaConfig) -> OnyxResult<Self> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_keep_log_file_num(5);
        db_opts.set_enable_pipelined_write(true);

        if let Some(ref wal_dir) = config.wal_dir {
            db_opts.set_wal_dir(wal_dir);
        }

        let rocksdb_path = config.rocksdb_path.as_ref().ok_or_else(|| {
            OnyxError::Config("meta.rocksdb_path is required to open MetaStore".into())
        })?;

        // Discover existing CFs so we don't lose per-volume blockmap CFs on reopen.
        let existing_cfs: Vec<String> = if rocksdb_path.exists() {
            DBWithThreadMode::<MultiThreaded>::list_cf(&db_opts, rocksdb_path).unwrap_or_default()
        } else {
            Vec::new()
        };

        let mut descriptors: Vec<ColumnFamilyDescriptor> = Vec::new();

        descriptors.push(ColumnFamilyDescriptor::new(CF_VOLUMES, Options::default()));

        let mut refcount_opts = Options::default();
        let mut refcount_block_opts = BlockBasedOptions::default();
        refcount_block_opts.set_bloom_filter(10.0, false);
        refcount_opts.set_block_based_table_factory(&refcount_block_opts);
        refcount_opts.set_merge_operator(
            "refcount_sum",
            refcount_full_merge,
            refcount_partial_merge,
        );
        descriptors.push(ColumnFamilyDescriptor::new(CF_REFCOUNT, refcount_opts));

        let mut dedup_index_opts = Options::default();
        let mut dedup_index_block_opts = BlockBasedOptions::default();
        dedup_index_block_opts.set_bloom_filter(15.0, false);
        dedup_index_opts.set_block_based_table_factory(&dedup_index_block_opts);
        descriptors.push(ColumnFamilyDescriptor::new(
            CF_DEDUP_INDEX,
            dedup_index_opts,
        ));

        let mut dedup_reverse_opts = Options::default();
        let mut dedup_reverse_block_opts = BlockBasedOptions::default();
        dedup_reverse_block_opts.set_bloom_filter(10.0, false);
        dedup_reverse_opts.set_block_based_table_factory(&dedup_reverse_block_opts);
        descriptors.push(ColumnFamilyDescriptor::new(
            CF_DEDUP_REVERSE,
            dedup_reverse_opts,
        ));

        let has_legacy_blockmap = existing_cfs.iter().any(|n| n == CF_BLOCKMAP_LEGACY);
        for cf_name in &existing_cfs {
            if vol_id_from_blockmap_cf(cf_name).is_some() {
                descriptors.push(ColumnFamilyDescriptor::new(
                    cf_name.as_str(),
                    Self::blockmap_cf_opts(config.block_cache_mb),
                ));
            }
        }

        if has_legacy_blockmap {
            descriptors.push(ColumnFamilyDescriptor::new(
                CF_BLOCKMAP_LEGACY,
                Self::blockmap_cf_opts(config.block_cache_mb),
            ));
        }

        let db = DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(
            &db_opts,
            rocksdb_path,
            descriptors,
        )?;

        let mut hot_write_opts = WriteOptions::default();
        hot_write_opts.set_sync(false);

        // Open the paged blockmap backend (redb). Path defaults to
        // `{rocksdb_path}/blockmap.redb` if the config did not set one.
        let redb_path = config.resolved_redb_path().ok_or_else(|| {
            OnyxError::Config("meta.redb_path or meta.rocksdb_path must be set".into())
        })?;
        if let Some(parent) = redb_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(OnyxError::Io)?;
            }
        }
        let redb = Arc::new(RedbStore::open(&redb_path)?);

        let store = Self {
            db,
            blockmap_locks: (0..BLOCKMAP_LOCK_STRIPES).map(|_| Mutex::new(())).collect(),
            refcount_locks: (0..REFCOUNT_LOCK_STRIPES).map(|_| Mutex::new(())).collect(),
            hot_write_opts,
            block_cache_mb: config.block_cache_mb,
            redb,
        };

        if has_legacy_blockmap {
            store.migrate_legacy_blockmap()?;
        }

        Ok(store)
    }

    /// Create a per-volume blockmap CF. Called when a new volume is created.
    pub fn create_blockmap_cf(&self, vol_id: &str) -> OnyxResult<()> {
        let cf_name = blockmap_cf_name(vol_id);
        if self.db.cf_handle(&cf_name).is_some() {
            return Ok(());
        }
        let opts = Self::blockmap_cf_opts(self.block_cache_mb);
        self.db.create_cf(&cf_name, &opts)?;
        tracing::info!(volume = vol_id, cf = %cf_name, "created per-volume blockmap CF");
        Ok(())
    }

    /// Drop a per-volume blockmap CF. Called when a volume is deleted.
    pub fn drop_blockmap_cf(&self, vol_id: &str) -> OnyxResult<()> {
        let cf_name = blockmap_cf_name(vol_id);
        if self.db.cf_handle(&cf_name).is_none() {
            return Ok(());
        }
        self.db.drop_cf(&cf_name)?;
        tracing::info!(volume = vol_id, cf = %cf_name, "dropped per-volume blockmap CF");
        Ok(())
    }

    /// Get the CF handle for a volume's blockmap. Returns None if the CF doesn't exist.
    pub(super) fn blockmap_cf(&self, vol_id: &str) -> Option<Arc<BoundColumnFamily<'_>>> {
        let cf_name = blockmap_cf_name(vol_id);
        self.db.cf_handle(&cf_name)
    }

    /// Get the CF handle for a volume's blockmap, returning an error if missing.
    pub(super) fn require_blockmap_cf(
        &self,
        vol_id: &str,
    ) -> OnyxResult<Arc<BoundColumnFamily<'_>>> {
        self.blockmap_cf(vol_id).ok_or_else(|| {
            OnyxError::Config(format!(
                "blockmap CF not found for volume '{}' — was it created?",
                vol_id
            ))
        })
    }

    /// Collect all per-volume blockmap CF names currently in the DB.
    pub(super) fn all_blockmap_cf_names(&self) -> Vec<String> {
        let path = self.db.path();
        let opts = Options::default();
        DBWithThreadMode::<MultiThreaded>::list_cf(&opts, path)
            .unwrap_or_default()
            .into_iter()
            .filter(|n| vol_id_from_blockmap_cf(n).is_some())
            .collect()
    }

    /// Migrate legacy single CF_BLOCKMAP to per-volume CFs.
    fn migrate_legacy_blockmap(&self) -> OnyxResult<()> {
        let legacy_cf = match self.db.cf_handle(CF_BLOCKMAP_LEGACY) {
            Some(cf) => cf,
            None => return Ok(()),
        };

        tracing::info!("starting migration from legacy CF_BLOCKMAP to per-volume CFs");

        let mut entries_by_vol: HashMap<String, Vec<(Lba, Vec<u8>)>> = HashMap::new();
        let iter = self
            .db
            .iterator_cf(&legacy_cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, value) = item?;
            if let Some((vol_id, lba)) = decode_blockmap_key_legacy(&key) {
                entries_by_vol
                    .entry(vol_id)
                    .or_default()
                    .push((lba, value.to_vec()));
            }
        }

        let total_entries: usize = entries_by_vol.values().map(|v| v.len()).sum();
        tracing::info!(
            volumes = entries_by_vol.len(),
            total_entries,
            "scanned legacy blockmap for migration"
        );

        for (vol_id, entries) in &entries_by_vol {
            self.create_blockmap_cf(vol_id)?;
            let cf = self.require_blockmap_cf(vol_id)?;
            let mut batch = WriteBatch::default();
            for (lba, value) in entries {
                let key = encode_blockmap_key(*lba);
                batch.put_cf(&cf, &key, value);
            }
            self.db.write(batch)?;
            tracing::info!(
                volume = vol_id,
                entries = entries.len(),
                "migrated blockmap entries to per-volume CF"
            );
        }

        self.db.drop_cf(CF_BLOCKMAP_LEGACY)?;
        tracing::info!("dropped legacy CF_BLOCKMAP after migration");

        Ok(())
    }

    pub fn memory_stats(&self) -> OnyxResult<RocksDbMemorySnapshot> {
        let mut all_cf_names: Vec<String> =
            Self::GLOBAL_CFS.iter().map(|s| s.to_string()).collect();
        all_cf_names.extend(self.all_blockmap_cf_names());

        let sum_cf_property = |prop| -> OnyxResult<u64> {
            let mut total = 0u64;
            for cf_name in &all_cf_names {
                if let Some(cf) = self.db.cf_handle(cf_name) {
                    total = total
                        .saturating_add(self.db.property_int_value_cf(&cf, prop)?.unwrap_or(0));
                }
            }
            Ok(total)
        };

        let cache_cf_name = self
            .all_blockmap_cf_names()
            .into_iter()
            .next()
            .unwrap_or_else(|| CF_REFCOUNT.to_string());
        let cache_cf = self.db.cf_handle(&cache_cf_name).unwrap();

        Ok(RocksDbMemorySnapshot {
            block_cache_capacity_bytes: self
                .db
                .property_int_value_cf(&cache_cf, properties::BLOCK_CACHE_CAPACITY)?,
            block_cache_usage_bytes: self
                .db
                .property_int_value_cf(&cache_cf, properties::BLOCK_CACHE_USAGE)?,
            block_cache_pinned_usage_bytes: self
                .db
                .property_int_value_cf(&cache_cf, properties::BLOCK_CACHE_PINNED_USAGE)?,
            cur_size_all_mem_tables_bytes: sum_cf_property(properties::CUR_SIZE_ALL_MEM_TABLES)?,
            size_all_mem_tables_bytes: sum_cf_property(properties::SIZE_ALL_MEM_TABLES)?,
            estimate_table_readers_mem_bytes: sum_cf_property(
                properties::ESTIMATE_TABLE_READERS_MEM,
            )?,
        })
    }
}
