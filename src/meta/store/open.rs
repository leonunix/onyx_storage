use std::sync::{Arc, Mutex};

use rocksdb::{
    properties, BlockBasedOptions, ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded,
    Options, WriteOptions,
};

use crate::config::MetaConfig;
use crate::error::{OnyxError, OnyxResult};
use crate::meta::redb::RedbStore;
use crate::meta::schema::*;
use crate::metrics::RocksDbMemorySnapshot;

use super::{
    refcount_full_merge, refcount_partial_merge, MetaStore, BLOCKMAP_LOCK_STRIPES,
    REFCOUNT_LOCK_STRIPES,
};

impl MetaStore {
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

        // RocksDB holds metadata, refcount, and dedup only. Blockmap lives in
        // redb. If an older deployment left per-volume blockmap CFs or a
        // legacy `blockmap` CF behind, we still register descriptors for them
        // so the database opens cleanly — they are ignored afterwards.
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

        // Legacy per-volume blockmap CFs are recognized by their prefix. They
        // predate the redb migration; register descriptors so RocksDB opens
        // them but never touch them again. The legacy single-CF `blockmap` is
        // treated the same way.
        for cf_name in &existing_cfs {
            if vol_id_from_blockmap_cf(cf_name).is_some() {
                descriptors.push(ColumnFamilyDescriptor::new(
                    cf_name.as_str(),
                    Self::legacy_blockmap_cf_opts(),
                ));
            }
        }
        if existing_cfs.iter().any(|n| n == CF_BLOCKMAP_LEGACY) {
            descriptors.push(ColumnFamilyDescriptor::new(
                CF_BLOCKMAP_LEGACY,
                Self::legacy_blockmap_cf_opts(),
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

        Ok(store)
    }

    /// Minimal Options for legacy per-volume blockmap CFs we still have to
    /// register on open. We do not use them; values here just make the DB open.
    fn legacy_blockmap_cf_opts() -> Options {
        let mut opts = Options::default();
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts
    }

    /// Force any in-memory redb commits to disk. Called at engine shutdown
    /// before the clean-shutdown superblock marker is written.
    pub fn sync_redb(&self) -> OnyxResult<()> {
        self.redb.sync()
    }

    /// Drive both RocksDB and redb to physical durability.
    ///
    /// RocksDB hot-path writes use `WriteOptions::sync = false` (only the
    /// buffer commit log fsyncs on user IO). redb hot-path writes use
    /// `Durability::None`. Both defer their fsyncs so we can amortize the
    /// cost across many IOs; this method performs the amortized fsync.
    ///
    /// Intended caller: the durability-watermark background thread, which
    /// invokes this on a periodic cadence and then advances the watermark
    /// the buffer pool reclaim path observes.
    pub fn sync_durable(&self) -> OnyxResult<()> {
        self.db.flush_wal(true)?;
        self.redb.sync()?;
        Ok(())
    }

    pub fn memory_stats(&self) -> OnyxResult<RocksDbMemorySnapshot> {
        let cf_names: Vec<&str> = Self::GLOBAL_CFS.to_vec();

        let sum_cf_property = |prop| -> OnyxResult<u64> {
            let mut total = 0u64;
            for cf_name in &cf_names {
                if let Some(cf) = self.db.cf_handle(cf_name) {
                    total = total
                        .saturating_add(self.db.property_int_value_cf(&cf, prop)?.unwrap_or(0));
                }
            }
            Ok(total)
        };

        // Use the refcount CF as the handle for block-cache-wide properties.
        // Block cache is shared across CFs in practice, so any CF works.
        let cache_cf = self.db.cf_handle(CF_REFCOUNT).unwrap();

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
            estimate_table_readers_mem_bytes: sum_cf_property(properties::ESTIMATE_TABLE_READERS_MEM)?,
        })
    }
}
