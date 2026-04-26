use std::sync::{Arc, Mutex};

use rocksdb::{
    properties, BlockBasedOptions, BoundColumnFamily, Cache, ColumnFamilyDescriptor,
    DBWithThreadMode, MultiThreaded, Options, WriteBufferManager, WriteOptions,
};

use crate::config::MetaConfig;
use crate::error::{OnyxError, OnyxResult};
use crate::meta::schema::*;
use crate::metrics::RocksDbMemorySnapshot;

use super::{
    refcount_full_merge, refcount_partial_merge, MetaStore, BLOCKMAP_LOCK_STRIPES,
    REFCOUNT_LOCK_STRIPES,
};

impl MetaStore {
    /// Build `BlockBasedOptions` shared by every CF.
    ///
    /// Key invariants, all critical for bounded memory:
    /// - `set_block_cache(cache)`: every CF points at the ONE shared cache so
    ///   total RocksDB read memory is capped by the cache's capacity.
    /// - `set_cache_index_and_filter_blocks(true)`: filter + index blocks are
    ///   accounted against the cache. Without this, they sit outside the cache
    ///   in `estimate_table_readers_mem_bytes` and grow unbounded with the
    ///   dataset.
    /// - `set_pin_l0_filter_and_index_blocks_in_cache(true)`: L0 filter/index
    ///   stay pinned so the hot path never eats a cache miss on them. Higher
    ///   levels are still evictable.
    fn shared_block_opts(cache: &Cache, bloom_bits_per_key: f64) -> BlockBasedOptions {
        let mut bbto = BlockBasedOptions::default();
        bbto.set_bloom_filter(bloom_bits_per_key, false);
        bbto.set_block_size(4096);
        bbto.set_block_cache(cache);
        bbto.set_cache_index_and_filter_blocks(true);
        bbto.set_pin_l0_filter_and_index_blocks_in_cache(true);
        bbto
    }

    /// Build the Options for a per-volume blockmap CF (bloom + LZ4).
    /// All CFs share the same `Cache` instance passed in — creating a new
    /// `Cache` per CF used to multiply memory by the number of volumes.
    fn blockmap_cf_opts(cache: &Cache) -> Options {
        let mut opts = Options::default();
        opts.set_block_based_table_factory(&Self::shared_block_opts(cache, 10.0));
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

        // Single shared block cache for every CF. Capacity from config.
        let block_cache = Arc::new(Cache::new_lru_cache(config.block_cache_bytes()));

        // Cap total memtable memory across all CFs (default = half of cache,
        // overridable via `meta.memtable_budget_mb`). `allow_stall = true`
        // lets writes block briefly rather than balloon memory when many CFs
        // flush at once.
        let write_buffer_manager = Arc::new(WriteBufferManager::new_write_buffer_manager(
            config.memtable_budget_bytes(),
            true,
        ));
        db_opts.set_write_buffer_manager(&write_buffer_manager);

        let rocksdb_path = config.path().ok_or_else(|| {
            OnyxError::Config(
                "meta.path (or legacy meta.rocksdb_path) is required to open MetaStore".into(),
            )
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
        refcount_opts.set_block_based_table_factory(&Self::shared_block_opts(&block_cache, 10.0));
        refcount_opts.set_merge_operator(
            "refcount_sum",
            refcount_full_merge,
            refcount_partial_merge,
        );
        descriptors.push(ColumnFamilyDescriptor::new(CF_REFCOUNT, refcount_opts));

        let mut dedup_index_opts = Options::default();
        dedup_index_opts
            .set_block_based_table_factory(&Self::shared_block_opts(&block_cache, 15.0));
        descriptors.push(ColumnFamilyDescriptor::new(
            CF_DEDUP_INDEX,
            dedup_index_opts,
        ));

        let mut dedup_reverse_opts = Options::default();
        dedup_reverse_opts
            .set_block_based_table_factory(&Self::shared_block_opts(&block_cache, 10.0));
        descriptors.push(ColumnFamilyDescriptor::new(
            CF_DEDUP_REVERSE,
            dedup_reverse_opts,
        ));

        for cf_name in &existing_cfs {
            if vol_id_from_blockmap_cf(cf_name).is_some() {
                descriptors.push(ColumnFamilyDescriptor::new(
                    cf_name.as_str(),
                    Self::blockmap_cf_opts(&block_cache),
                ));
            }
        }

        let db = DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(
            &db_opts,
            rocksdb_path,
            descriptors,
        )?;

        let mut hot_write_opts = WriteOptions::default();
        hot_write_opts.set_sync(false);

        let store = Self {
            db,
            blockmap_locks: (0..BLOCKMAP_LOCK_STRIPES).map(|_| Mutex::new(())).collect(),
            refcount_locks: (0..REFCOUNT_LOCK_STRIPES).map(|_| Mutex::new(())).collect(),
            hot_write_opts,
            block_cache,
            write_buffer_manager,
        };

        Ok(store)
    }

    /// Create a per-volume blockmap CF. Called when a new volume is created.
    pub fn create_blockmap_cf(&self, vol_id: &str) -> OnyxResult<()> {
        let cf_name = blockmap_cf_name(vol_id);
        if self.db.cf_handle(&cf_name).is_some() {
            return Ok(());
        }
        let opts = Self::blockmap_cf_opts(&self.block_cache);
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

    /// Drive RocksDB to physical durability.
    ///
    /// Hot-path writes use `WriteOptions::sync = false` (only the buffer commit
    /// log fsyncs on user IO). This method flushes the WAL + fsyncs so the
    /// deferred commits become durable.
    ///
    /// Intended caller: the durability-watermark background thread, which
    /// invokes this on a periodic cadence and then advances the watermark
    /// that the buffer pool reclaim path observes.
    pub fn sync_durable(&self) -> OnyxResult<()> {
        self.db.flush_wal(true)?;
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

        // All CFs share one Cache instance now, so any CF reports the same
        // capacity/usage figures. Use CF_REFCOUNT (always present) so the
        // reported numbers stay correct even when no volume exists.
        let cache_cf = self.db.cf_handle(CF_REFCOUNT).ok_or_else(|| {
            OnyxError::Config("refcount CF missing — cannot query block cache stats".into())
        })?;

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
