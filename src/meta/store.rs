use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use rocksdb::{
    BlockBasedOptions, ColumnFamilyDescriptor, MergeOperands, Options, ReadOptions, WriteBatch,
    WriteOptions, DB,
};

use crate::config::MetaConfig;
use crate::error::{OnyxError, OnyxResult};
use crate::meta::schema::*;
use crate::types::{Lba, Pba, VolumeConfig, VolumeId};

const BLOCKMAP_LOCK_STRIPES: usize = 1024;
const REFCOUNT_LOCK_STRIPES: usize = 1024;

fn mix_u64(mut value: u64) -> u64 {
    value ^= value >> 33;
    value = value.wrapping_mul(0xff51afd7ed558ccd);
    value ^= value >> 33;
    value = value.wrapping_mul(0xc4ceb9fe1a85ec53);
    value ^ (value >> 33)
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    // FNV-1a followed by a final avalanche to spread adjacent LBA keys well.
    let mut hash = 0xcbf29ce484222325u64;
    for &byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    mix_u64(hash)
}

/// Full merge for CF_REFCOUNT: applies all pending i32 deltas to the base u32 value.
///
/// Base value: 4-byte BE u32 (existing refcount, or absent = 0).
/// Operand: 4-byte BE i32 (delta: positive = increment, negative = decrement).
/// Result: max(base + sum(deltas), 0) as u32, encoded as 4-byte BE u32.
fn refcount_full_merge(
    _key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let base: i64 = match existing_val {
        Some(v) if v.len() == 4 => u32::from_be_bytes(v[0..4].try_into().unwrap()) as i64,
        Some(_) => return None,
        None => 0,
    };
    let mut total_delta: i64 = 0;
    for op in operands {
        if op.len() == 4 {
            total_delta += i32::from_be_bytes(op[0..4].try_into().unwrap()) as i64;
        } else {
            return None;
        }
    }
    let result = (base + total_delta).max(0) as u32;
    Some(result.to_be_bytes().to_vec())
}

/// Partial merge for CF_REFCOUNT: combines multiple i32 deltas into one.
/// Returns i32 (NOT clamped to 0) so negative deltas are preserved for full_merge.
fn refcount_partial_merge(
    _key: &[u8],
    _existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut total: i32 = 0;
    for op in operands {
        if op.len() == 4 {
            total = total.saturating_add(i32::from_be_bytes(op[0..4].try_into().unwrap()));
        } else {
            return None;
        }
    }
    Some(total.to_be_bytes().to_vec())
}

/// Result for each dedup hit in a batched `atomic_batch_dedup_hits` call.
#[derive(Debug, Clone, Copy)]
pub enum DedupHitResult {
    /// Hit accepted. Contains `Some((old_pba, old_blocks))` if an old PBA was
    /// decremented, or `None` if the LBA already pointed to the target PBA.
    Accepted(Option<(Pba, u32)>),
    /// Hit rejected because the target PBA's refcount was 0 (freed).
    Rejected,
}

pub struct MetaStore {
    db: DB,
    /// Striped locks for blockmap key updates.
    ///
    /// Any operation that re-reads + rewrites a blockmap entry must hold the
    /// corresponding stripe so same-LBA races do not observe stale mappings.
    blockmap_locks: Vec<Mutex<()>>,
    /// Striped locks for refcount read-modify-write operations.
    ///
    /// Any operation that changes the live-reference set for a PBA must hold
    /// the corresponding stripe so overlapping refcount updates do not lose
    /// increments/decrements.
    refcount_locks: Vec<Mutex<()>>,
    /// Non-sync write options for hot-path metadata commits (blockmap + refcount
    /// in flush/dedup/GC paths).  WAL is still written to the OS buffer, but
    /// we skip the fsync that dominates lock-hold time (~20 ms → ~0.5 ms).
    /// Crash durability is provided by the buffer ring: LV2 write thread does
    /// fdatasync before ack; on crash, unflushed buffer entries are replayed
    /// idempotently by the flusher, re-deriving all hot-path metadata.
    /// Cold-path operations (create/delete volume, reconciliation) keep
    /// sync = true via the default `db.write(batch)`.
    hot_write_opts: WriteOptions,
}

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

        // Refcount CF: bloom filter + merge operator for lock-free increment/decrement
        let mut refcount_opts = Options::default();
        let mut refcount_block_opts = BlockBasedOptions::default();
        refcount_block_opts.set_bloom_filter(10.0, false);
        refcount_opts.set_block_based_table_factory(&refcount_block_opts);
        refcount_opts.set_merge_operator(
            "refcount_sum",
            refcount_full_merge,
            refcount_partial_merge,
        );
        let cf_refcount = ColumnFamilyDescriptor::new(CF_REFCOUNT, refcount_opts);

        // Fragment refs CF: exact compressed-fragment identity -> live LBA refcount.
        // Uses the same merge semantics as refcount to support batched +/- deltas.
        let mut fragment_ref_opts = Options::default();
        let mut fragment_ref_block_opts = BlockBasedOptions::default();
        fragment_ref_block_opts.set_bloom_filter(10.0, false);
        fragment_ref_opts.set_block_based_table_factory(&fragment_ref_block_opts);
        fragment_ref_opts.set_merge_operator(
            "fragment_ref_sum",
            refcount_full_merge,
            refcount_partial_merge,
        );
        let cf_fragment_refs = ColumnFamilyDescriptor::new(CF_FRAGMENT_REFS, fragment_ref_opts);

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

        let rocksdb_path = config.rocksdb_path.as_ref().ok_or_else(|| {
            OnyxError::Config("meta.rocksdb_path is required to open MetaStore".into())
        })?;

        let db = DB::open_cf_descriptors(
            &db_opts,
            rocksdb_path,
            vec![
                cf_volumes,
                cf_blockmap,
                cf_refcount,
                cf_fragment_refs,
                cf_dedup_index,
                cf_dedup_reverse,
            ],
        )?;

        let mut hot_write_opts = WriteOptions::default();
        hot_write_opts.set_sync(false);

        Self::rebuild_fragment_refs_if_needed(&db, &hot_write_opts)?;

        Ok(Self {
            db,
            blockmap_locks: (0..BLOCKMAP_LOCK_STRIPES).map(|_| Mutex::new(())).collect(),
            refcount_locks: (0..REFCOUNT_LOCK_STRIPES).map(|_| Mutex::new(())).collect(),
            hot_write_opts,
        })
    }

    fn blockmap_lock_index(key: &[u8]) -> usize {
        (hash_bytes(key) as usize) % BLOCKMAP_LOCK_STRIPES
    }

    fn refcount_lock_index(pba: Pba) -> usize {
        (mix_u64(pba.0) as usize) % REFCOUNT_LOCK_STRIPES
    }

    fn lock_indices<'a>(
        locks: &'a [Mutex<()>],
        mut indices: Vec<usize>,
    ) -> Vec<MutexGuard<'a, ()>> {
        indices.sort_unstable();
        indices.dedup();
        indices
            .into_iter()
            .map(|idx| locks[idx].lock().unwrap())
            .collect()
    }

    fn lock_blockmap_keys<'a, I, K>(&'a self, keys: I) -> Vec<MutexGuard<'a, ()>>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<[u8]>,
    {
        let indices = keys
            .into_iter()
            .map(|key| Self::blockmap_lock_index(key.as_ref()))
            .collect();
        Self::lock_indices(&self.blockmap_locks, indices)
    }

    fn lock_refcount_pbas<'a, I>(&'a self, pbas: I) -> Vec<MutexGuard<'a, ()>>
    where
        I: IntoIterator<Item = Pba>,
    {
        let indices = pbas.into_iter().map(Self::refcount_lock_index).collect();
        Self::lock_indices(&self.refcount_locks, indices)
    }

    fn lock_all_blockmap_stripes(&self) -> Vec<MutexGuard<'_, ()>> {
        Self::lock_indices(
            &self.blockmap_locks,
            (0..self.blockmap_locks.len()).collect(),
        )
    }

    fn lock_all_refcount_stripes(&self) -> Vec<MutexGuard<'_, ()>> {
        Self::lock_indices(
            &self.refcount_locks,
            (0..self.refcount_locks.len()).collect(),
        )
    }

    fn refcounts_by_pba(&self, pbas: &[Pba]) -> OnyxResult<HashMap<Pba, u32>> {
        let counts = self.multi_get_refcounts(pbas)?;
        Ok(pbas.iter().copied().zip(counts).collect())
    }

    fn rebuild_fragment_refs_if_needed(db: &DB, hot_write_opts: &WriteOptions) -> OnyxResult<()> {
        let cf_blockmap = db.cf_handle(CF_BLOCKMAP).unwrap();
        let cf_fragment_refs = db.cf_handle(CF_FRAGMENT_REFS).unwrap();

        let mut fragment_iter = db.raw_iterator_cf(&cf_fragment_refs);
        fragment_iter.seek_to_first();
        if fragment_iter.valid() {
            fragment_iter.status()?;
            return Ok(());
        }
        fragment_iter.status()?;

        let mut blockmap_iter = db.raw_iterator_cf(&cf_blockmap);
        blockmap_iter.seek_to_first();
        if !blockmap_iter.valid() {
            blockmap_iter.status()?;
            return Ok(());
        }

        let mut batch = WriteBatch::default();
        let mut seen = 0usize;
        while blockmap_iter.valid() {
            if let Some(value) = blockmap_iter.value() {
                if let Some(bv) = decode_blockmap_value(value) {
                    let key = encode_fragment_ref_key(&FragmentRefKey::from(&bv));
                    batch.merge_cf(&cf_fragment_refs, key, encode_refcount_delta(1));
                    seen += 1;
                }
            }
            if seen > 0 && seen % 10_000 == 0 {
                db.write_opt(batch, hot_write_opts)?;
                batch = WriteBatch::default();
            }
            blockmap_iter.next();
        }
        blockmap_iter.status()?;
        if seen % 10_000 != 0 {
            db.write_opt(batch, hot_write_opts)?;
        }

        tracing::info!(entries = seen, "rebuilt fragment ref index from blockmap");
        Ok(())
    }

    fn same_fragment_identity(lhs: &BlockmapValue, rhs: &BlockmapValue) -> bool {
        lhs.pba == rhs.pba
            && lhs.slot_offset == rhs.slot_offset
            && lhs.compression == rhs.compression
            && lhs.unit_compressed_size == rhs.unit_compressed_size
            && lhs.unit_original_size == rhs.unit_original_size
            && lhs.unit_lba_count == rhs.unit_lba_count
            && lhs.crc32 == rhs.crc32
    }

    fn apply_fragment_ref_delta(
        batch: &mut WriteBatch,
        cf_fragment_refs: &impl rocksdb::AsColumnFamilyRef,
        value: &BlockmapValue,
        delta: i32,
    ) {
        let key = encode_fragment_ref_key(&FragmentRefKey::from(value));
        batch.merge_cf(cf_fragment_refs, key, encode_refcount_delta(delta));
    }

    fn apply_fragment_ref_replacement(
        batch: &mut WriteBatch,
        cf_fragment_refs: &impl rocksdb::AsColumnFamilyRef,
        old: Option<&BlockmapValue>,
        new: Option<&BlockmapValue>,
    ) {
        if let (Some(old), Some(new)) = (old, new) {
            if Self::same_fragment_identity(old, new) {
                return;
            }
        }
        if let Some(old) = old {
            Self::apply_fragment_ref_delta(batch, cf_fragment_refs, old, -1);
        }
        if let Some(new) = new {
            Self::apply_fragment_ref_delta(batch, cf_fragment_refs, new, 1);
        }
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
        let cf_volumes = self.db.cf_handle(CF_VOLUMES).unwrap();
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();
        let cf_fragment_refs = self.db.cf_handle(CF_FRAGMENT_REFS).unwrap();

        let prefix = blockmap_key_prefix(id)?;

        // Phase 1: scan all blockmap entries, collect PBA decrement counts,
        // block counts per PBA, and blockmap keys to delete.
        let mut pba_decrements: HashMap<Pba, u32> = HashMap::new();
        let mut pba_block_counts: HashMap<Pba, u32> = HashMap::new();
        let mut fragment_decrements: HashMap<FragmentRefKey, u32> = HashMap::new();
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
                        *fragment_decrements
                            .entry(FragmentRefKey::from(&bv))
                            .or_insert(0) += 1;
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

        let _refcount_guards = self.lock_refcount_pbas(pba_decrements.keys().copied());

        // Phase 2: merge-decrement refcounts + delete blockmap + volume in one WriteBatch
        let mut batch = WriteBatch::default();

        for (pba, decrement) in &pba_decrements {
            let rc_key = encode_refcount_key(*pba);
            batch.merge_cf(
                &cf_refcount,
                &rc_key,
                encode_refcount_delta(-(*decrement as i32)),
            );
        }

        for (fragment, decrement) in &fragment_decrements {
            let key = encode_fragment_ref_key(fragment);
            batch.merge_cf(
                &cf_fragment_refs,
                key,
                encode_refcount_delta(-(*decrement as i32)),
            );
        }

        for key in &blockmap_keys_to_delete {
            batch.delete_cf(&cf_blockmap, key);
        }

        let vol_key = encode_volume_key(&id.0);
        batch.delete_cf(&cf_volumes, &vol_key);

        self.db.write(batch)?;

        // Phase 3: read back refcounts to find zeroed PBAs, cleanup dedup + collect freed extents.
        // This is best-effort: the volume is already deleted after Phase 2 committed.
        // If cleanup fails, orphaned refcount/dedup entries are harmless and will be
        // cleaned up by cleanup_orphaned_refcounts on next startup.
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
                        let block_count = pba_block_counts.get(pba).copied().unwrap_or(1);
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
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();
        let cf_fragment_refs = self.db.cf_handle(CF_FRAGMENT_REFS).unwrap();

        // Phase 1: scan blockmap range with raw iterator, collect raw keys
        // (avoids re-encoding keys that the iterator already yields)
        let start_key = encode_blockmap_key(vol_id, start_lba)?;
        let end_key = encode_blockmap_key(vol_id, end_lba)?;
        let prefix = blockmap_key_prefix(vol_id)?;

        let mut read_opts = ReadOptions::default();
        read_opts.set_iterate_upper_bound(end_key);

        let mut pba_decrements: HashMap<Pba, u32> = HashMap::new();
        let mut pba_block_counts: HashMap<Pba, u32> = HashMap::new();
        let mut fragment_decrements: HashMap<FragmentRefKey, u32> = HashMap::new();
        let mut blockmap_keys: Vec<Vec<u8>> = Vec::new();

        let mut iter = self.db.raw_iterator_cf_opt(&cf_blockmap, read_opts);
        iter.seek(&start_key);
        while iter.valid() {
            if let (Some(key), Some(val)) = (iter.key(), iter.value()) {
                if !key.starts_with(&prefix) {
                    break;
                }
                if let Some(bv) = decode_blockmap_value(val) {
                    *pba_decrements.entry(bv.pba).or_insert(0) += 1;
                    *fragment_decrements
                        .entry(FragmentRefKey::from(&bv))
                        .or_insert(0) += 1;
                    let blocks = bv.unit_compressed_size.div_ceil(crate::types::BLOCK_SIZE);
                    pba_block_counts
                        .entry(bv.pba)
                        .and_modify(|b| *b = (*b).max(blocks))
                        .or_insert(blocks);
                }
                blockmap_keys.push(key.to_vec());
            }
            iter.next();
        }
        iter.status()?;

        if blockmap_keys.is_empty() {
            return Ok(Vec::new());
        }

        let _refcount_guards = self.lock_refcount_pbas(pba_decrements.keys().copied());

        // Phase 2: atomic WriteBatch — delete blockmap entries + merge-decrement refcounts
        let mut batch = WriteBatch::default();

        for (pba, decrement) in &pba_decrements {
            let rc_key = encode_refcount_key(*pba);
            batch.merge_cf(
                &cf_refcount,
                &rc_key,
                encode_refcount_delta(-(*decrement as i32)),
            );
        }

        for (fragment, decrement) in &fragment_decrements {
            let key = encode_fragment_ref_key(fragment);
            batch.merge_cf(
                &cf_fragment_refs,
                key,
                encode_refcount_delta(-(*decrement as i32)),
            );
        }

        for key in &blockmap_keys {
            batch.delete_cf(&cf_blockmap, key);
        }

        self.db.write(batch)?;

        // Phase 3: read back refcounts, cleanup dedup for zeroed PBAs, collect freed extents
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
                        let block_count = pba_block_counts.get(pba).copied().unwrap_or(1);
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
            deleted_keys = blockmap_keys.len(),
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

    // --- Blockmap operations ---

    pub fn put_mapping(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        value: &BlockmapValue,
    ) -> OnyxResult<()> {
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let cf_fragment_refs = self.db.cf_handle(CF_FRAGMENT_REFS).unwrap();
        let key = encode_blockmap_key(vol_id, lba)?;
        let _blockmap_guards = self.lock_blockmap_keys([key.as_slice()]);
        let old = self.get_mapping(vol_id, lba)?;
        let val = encode_blockmap_value(value);
        let mut batch = WriteBatch::default();
        batch.put_cf(&cf_blockmap, &key, &val);
        Self::apply_fragment_ref_replacement(
            &mut batch,
            &cf_fragment_refs,
            old.as_ref(),
            Some(value),
        );
        self.db.write_opt(batch, &self.hot_write_opts)?;
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
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let cf_fragment_refs = self.db.cf_handle(CF_FRAGMENT_REFS).unwrap();
        let key = encode_blockmap_key(vol_id, lba)?;
        let _blockmap_guards = self.lock_blockmap_keys([key.as_slice()]);
        let old = self.get_mapping(vol_id, lba)?;
        let mut batch = WriteBatch::default();
        batch.delete_cf(&cf_blockmap, &key);
        Self::apply_fragment_ref_replacement(&mut batch, &cf_fragment_refs, old.as_ref(), None);
        self.db.write_opt(batch, &self.hot_write_opts)?;
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

    fn set_refcount_locked(&self, pba: Pba, count: u32) -> OnyxResult<()> {
        let cf = self.db.cf_handle(CF_REFCOUNT).unwrap();
        let key = encode_refcount_key(pba);
        if count == 0 {
            self.db.delete_cf(&cf, &key)?;
        } else {
            self.db.put_cf(&cf, &key, &encode_refcount_value(count))?;
        }
        Ok(())
    }

    pub fn set_refcount(&self, pba: Pba, count: u32) -> OnyxResult<()> {
        let _refcount_guards = self.lock_refcount_pbas([pba]);
        self.set_refcount_locked(pba, count)
    }

    pub fn increment_refcount(&self, pba: Pba) -> OnyxResult<u32> {
        let _refcount_guards = self.lock_refcount_pbas([pba]);
        let current = self.get_refcount(pba)?;
        let new_count = current + 1;
        self.set_refcount_locked(pba, new_count)?;
        Ok(new_count)
    }

    pub fn decrement_refcount(&self, pba: Pba) -> OnyxResult<u32> {
        let _refcount_guards = self.lock_refcount_pbas([pba]);
        let current = self.get_refcount(pba)?;
        let new_count = current.saturating_sub(1);
        self.set_refcount_locked(pba, new_count)?;
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
        let cf_fragment_refs = self.db.cf_handle(CF_FRAGMENT_REFS).unwrap();

        let bm_key = encode_blockmap_key(vol_id, lba)?;
        let _blockmap_guards = self.lock_blockmap_keys([bm_key.as_slice()]);
        let old = self.get_mapping(vol_id, lba)?;
        let _refcount_guards = self.lock_refcount_pbas([value.pba]);
        let bm_val = encode_blockmap_value(value);
        let rc_key = encode_refcount_key(value.pba);
        let rc_val = encode_refcount_value(1);

        let mut batch = WriteBatch::default();
        batch.put_cf(&cf_blockmap, &bm_key, &bm_val);
        batch.put_cf(&cf_refcount, &rc_key, &rc_val);
        Self::apply_fragment_ref_replacement(
            &mut batch,
            &cf_fragment_refs,
            old.as_ref(),
            Some(value),
        );
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
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();
        let cf_fragment_refs = self.db.cf_handle(CF_FRAGMENT_REFS).unwrap();

        let bm_key = encode_blockmap_key(vol_id, lba)?;
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
        Self::apply_fragment_ref_replacement(
            &mut batch,
            &cf_fragment_refs,
            current_old_mapping.as_ref(),
            Some(new_value),
        );

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
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();
        let cf_fragment_refs = self.db.cf_handle(CF_FRAGMENT_REFS).unwrap();

        let lbas: Vec<Lba> = batch_values.iter().map(|(lba, _)| *lba).collect();
        let bm_keys: Vec<Vec<u8>> = lbas
            .iter()
            .map(|lba| encode_blockmap_key(vol_id, *lba))
            .collect::<OnyxResult<Vec<_>>>()?;
        let _blockmap_guards = self.lock_blockmap_keys(&bm_keys);

        let new_pba = batch_values.first().map(|(_, v)| v.pba);
        let old_mappings = self.multi_get_mappings(vol_id, &lbas)?;

        let mut old_pba_meta: HashMap<Pba, (u32, u32)> = HashMap::new();
        for old in old_mappings.iter().flatten() {
            if Some(old.pba) != new_pba {
                let old_blocks = old.unit_compressed_size.div_ceil(crate::types::BLOCK_SIZE);
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
        for (old, (_, value)) in old_mappings.iter().zip(batch_values.iter()) {
            Self::apply_fragment_ref_replacement(
                &mut batch,
                &cf_fragment_refs,
                old.as_ref(),
                Some(value),
            );
        }

        // Set new PBA refcount (all entries share the same PBA)
        if let Some((_, first_val)) = batch_values.first() {
            let rc_key = encode_refcount_key(first_val.pba);
            batch.put_cf(&cf_refcount, &rc_key, &encode_refcount_value(new_refcount));
        }

        for (old_pba, (decrement, _)) in &old_pba_meta {
            let current_rc = old_refcounts.get(old_pba).copied().unwrap_or(0);
            let new_rc = current_rc.saturating_sub(*decrement);
            let rc_key = encode_refcount_key(*old_pba);
            if new_rc == 0 {
                batch.delete_cf(&cf_refcount, &rc_key);
            } else {
                batch.put_cf(&cf_refcount, &rc_key, &encode_refcount_value(new_rc));
            }
        }

        self.db.write_opt(batch, &self.hot_write_opts)?;
        Ok(old_pba_meta)
    }

    /// Atomically write blockmap entries for a hole fill, incrementing the existing
    /// PBA refcount and decrementing old PBA refcounts.
    ///
    /// Old mappings are re-read inside the lock (same rationale as `atomic_batch_write`).
    /// `total_new_refs` is the number of LBAs being written (used to compute net increment).
    ///
    /// Returns `HashMap<Pba, (decrement, block_count)>` for old PBAs that were decremented
    /// (excludes self-referencing decrements where old PBA == fill PBA).
    pub fn atomic_batch_write_hole_fill(
        &self,
        vol_id: &VolumeId,
        batch_values: &[(Lba, BlockmapValue)],
        pba: Pba,
        total_new_refs: u32,
    ) -> OnyxResult<HashMap<Pba, (u32, u32)>> {
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();
        let cf_fragment_refs = self.db.cf_handle(CF_FRAGMENT_REFS).unwrap();

        let lbas: Vec<Lba> = batch_values.iter().map(|(lba, _)| *lba).collect();
        let bm_keys: Vec<Vec<u8>> = lbas
            .iter()
            .map(|lba| encode_blockmap_key(vol_id, *lba))
            .collect::<OnyxResult<Vec<_>>>()?;
        let _blockmap_guards = self.lock_blockmap_keys(&bm_keys);
        let old_mappings = self.multi_get_mappings(vol_id, &lbas)?;

        let mut self_decrement: u32 = 0;
        let mut old_pba_meta: HashMap<Pba, (u32, u32)> = HashMap::new();
        for old in old_mappings.iter().flatten() {
            if old.pba == pba {
                self_decrement += 1;
            } else {
                let old_blocks = old.unit_compressed_size.div_ceil(crate::types::BLOCK_SIZE);
                let entry = old_pba_meta.entry(old.pba).or_insert((0, old_blocks));
                entry.0 += 1;
                entry.1 = entry.1.max(old_blocks);
            }
        }

        let mut touched_pbas: Vec<Pba> = old_pba_meta.keys().copied().collect();
        touched_pbas.push(pba);
        let _refcount_guards = self.lock_refcount_pbas(touched_pbas);

        // Guard: reject if target PBA refcount is 0 (slot was freed).
        // Between the caller's hole detection and this point, another thread may
        // have decremented the slot's refcount to 0 and freed it to the allocator.
        // Writing new entries to a freed PBA would create live blockmap references
        // for a PBA the allocator considers free.
        let current_rc = self.get_refcount(pba)?;
        if current_rc == 0 {
            return Err(OnyxError::Io(std::io::Error::other(format!(
                "hole fill rejected: target PBA {:?} refcount is 0 (slot freed between hole detection and fill)",
                pba,
            ))));
        }

        let net_increment = total_new_refs - self_decrement;
        let old_pbas: Vec<Pba> = old_pba_meta.keys().copied().collect();
        let old_refcounts = self.refcounts_by_pba(&old_pbas)?;

        let mut batch = WriteBatch::default();

        for ((_, value), bm_key) in batch_values.iter().zip(bm_keys.iter()) {
            let bm_val = encode_blockmap_value(value);
            batch.put_cf(&cf_blockmap, &bm_key, &bm_val);
        }
        for (old, (_, value)) in old_mappings.iter().zip(batch_values.iter()) {
            Self::apply_fragment_ref_replacement(
                &mut batch,
                &cf_fragment_refs,
                old.as_ref(),
                Some(value),
            );
        }

        // Increment existing PBA refcount by net amount
        let new_rc = current_rc + net_increment;
        let rc_key = encode_refcount_key(pba);
        batch.put_cf(&cf_refcount, &rc_key, &encode_refcount_value(new_rc));

        for (old_pba, (decrement, _)) in &old_pba_meta {
            let old_rc = old_refcounts.get(old_pba).copied().unwrap_or(0);
            let old_new = old_rc.saturating_sub(*decrement);
            let old_rc_key = encode_refcount_key(*old_pba);
            if old_new == 0 {
                batch.delete_cf(&cf_refcount, &old_rc_key);
            } else {
                batch.put_cf(&cf_refcount, &old_rc_key, &encode_refcount_value(old_new));
            }
        }

        self.db.write_opt(batch, &self.hot_write_opts)?;
        Ok(old_pba_meta)
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
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();
        let cf_fragment_refs = self.db.cf_handle(CF_FRAGMENT_REFS).unwrap();

        let bm_keys: Vec<Vec<u8>> = batch_values
            .iter()
            .map(|(vol_id, lba, _)| encode_blockmap_key(vol_id, *lba))
            .collect::<OnyxResult<Vec<_>>>()?;
        let _blockmap_guards = self.lock_blockmap_keys(&bm_keys);

        let mut old_mappings = Vec::with_capacity(batch_values.len());
        let mut old_pba_meta: HashMap<Pba, (u32, u32)> = HashMap::new();
        for (vol_id, lba, _new_value) in batch_values {
            let old_mapping = self.get_mapping(vol_id, *lba)?;
            old_mappings.push(old_mapping);
            if let Some(old) = old_mapping {
                if old.pba != new_pba {
                    let old_blocks = old.unit_compressed_size.div_ceil(crate::types::BLOCK_SIZE);
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

        for ((_, _, value), bm_key) in batch_values.iter().zip(bm_keys.iter()) {
            let bm_val = encode_blockmap_value(value);
            batch.put_cf(&cf_blockmap, &bm_key, &bm_val);
        }
        for (old, (_, _, value)) in old_mappings.iter().zip(batch_values.iter()) {
            Self::apply_fragment_ref_replacement(
                &mut batch,
                &cf_fragment_refs,
                old.as_ref(),
                Some(value),
            );
        }

        let rc_key = encode_refcount_key(new_pba);
        batch.put_cf(&cf_refcount, &rc_key, &encode_refcount_value(new_refcount));

        for (old_pba, (decrement, _)) in &old_pba_meta {
            let current_rc = old_refcounts.get(old_pba).copied().unwrap_or(0);
            let new_rc = current_rc.saturating_sub(*decrement);
            let rc_key = encode_refcount_key(*old_pba);
            if new_rc == 0 {
                batch.delete_cf(&cf_refcount, &rc_key);
            } else {
                batch.put_cf(&cf_refcount, &rc_key, &encode_refcount_value(new_rc));
            }
        }

        self.db.write_opt(batch, &self.hot_write_opts)?;
        Ok(old_pba_meta)
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
        units: &[(
            &VolumeId,
            &[(Lba, BlockmapValue)],
            u32, // new_refcount
        )],
    ) -> OnyxResult<HashMap<Pba, (u32, u32)>> {
        if units.is_empty() {
            return Ok(HashMap::new());
        }
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();
        let cf_fragment_refs = self.db.cf_handle(CF_FRAGMENT_REFS).unwrap();

        let mut unit_lbas: Vec<Vec<Lba>> = Vec::with_capacity(units.len());
        let mut unit_keys: Vec<Vec<Vec<u8>>> = Vec::with_capacity(units.len());
        for (vol_id, batch_values, _) in units {
            let lbas: Vec<Lba> = batch_values.iter().map(|(lba, _)| *lba).collect();
            let keys: Vec<Vec<u8>> = lbas
                .iter()
                .map(|lba| encode_blockmap_key(vol_id, *lba))
                .collect::<OnyxResult<Vec<_>>>()?;
            unit_lbas.push(lbas);
            unit_keys.push(keys);
        }
        let _blockmap_guards =
            self.lock_blockmap_keys(unit_keys.iter().flat_map(|keys| keys.iter()));

        let mut batch = WriteBatch::default();

        // Aggregate all refcount deltas so a PBA that appears as both a newly
        // written target and an old overwritten source still gets one final,
        // correct value in the batch.
        let mut new_refcounts: HashMap<Pba, u32> = HashMap::new();
        let mut aggregated_decrements: HashMap<Pba, (u32, u32)> = HashMap::new();

        for (unit_idx, (vol_id, batch_values, new_refcount)) in units.iter().enumerate() {
            // Collect new PBA for this unit to exclude self-references
            let new_pba = batch_values.first().map(|(_, v)| v.pba);

            let old_mappings = self.multi_get_mappings(vol_id, &unit_lbas[unit_idx])?;
            for old in old_mappings.iter().flatten() {
                if Some(old.pba) != new_pba {
                    let old_blocks = old.unit_compressed_size.div_ceil(crate::types::BLOCK_SIZE);
                    let entry = aggregated_decrements
                        .entry(old.pba)
                        .or_insert((0, old_blocks));
                    entry.0 += 1;
                    entry.1 = entry.1.max(old_blocks);
                }
            }

            for ((_, value), bm_key) in batch_values.iter().zip(unit_keys[unit_idx].iter()) {
                let bm_val = encode_blockmap_value(value);
                batch.put_cf(&cf_blockmap, &bm_key, &bm_val);
            }
            for (old, (_, value)) in old_mappings.iter().zip(batch_values.iter()) {
                Self::apply_fragment_ref_replacement(
                    &mut batch,
                    &cf_fragment_refs,
                    old.as_ref(),
                    Some(value),
                );
            }

            // Set new PBA refcount
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
            } else {
                batch.put_cf(&cf_refcount, &rc_key, &encode_refcount_value(final_rc));
            }
        }

        self.db.write_opt(batch, &self.hot_write_opts)?;
        Ok(aggregated_decrements)
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

    /// Check if any blockmap entry across all volumes references the given PBA.
    pub fn has_any_blockmap_ref(&self, target_pba: Pba) -> OnyxResult<bool> {
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let iter = self
            .db
            .iterator_cf(&cf_blockmap, rocksdb::IteratorMode::Start);
        for item in iter {
            let (_, value) = item?;
            if let Some(bv) = decode_blockmap_value(&value) {
                if bv.pba == target_pba {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn count_blockmap_refs_for_pba_inner(&self, target_pba: Pba) -> OnyxResult<u32> {
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let iter = self
            .db
            .iterator_cf(&cf_blockmap, rocksdb::IteratorMode::Start);
        let mut refs = 0u32;
        for item in iter {
            let (_, value) = item?;
            if let Some(bv) = decode_blockmap_value(&value) {
                if bv.pba == target_pba {
                    refs = refs.saturating_add(1);
                }
            }
        }
        Ok(refs)
    }

    // TEMP(soak-debug): full blockmap scan used to diagnose allocator/PBA reuse
    // corruption. Remove or gate after the root cause is fixed and soak proves
    // stable without the extra protection.
    /// Count live blockmap references that currently point at `target_pba`.
    pub fn count_blockmap_refs_for_pba(&self, target_pba: Pba) -> OnyxResult<u32> {
        self.count_blockmap_refs_for_pba_inner(target_pba)
    }

    fn unique_fragments_for_pba_inner(
        &self,
        target_pba: Pba,
        limit: Option<usize>,
    ) -> OnyxResult<Vec<(String, Lba, BlockmapValue)>> {
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let iter = self
            .db
            .iterator_cf(&cf_blockmap, rocksdb::IteratorMode::Start);
        let mut seen = std::collections::HashSet::new();
        let mut fragments = Vec::new();
        for item in iter {
            let (key, value) = item?;
            let Some(bv) = decode_blockmap_value(&value) else {
                continue;
            };
            if bv.pba != target_pba {
                continue;
            }
            let frag_key = (
                bv.slot_offset,
                bv.unit_compressed_size,
                bv.unit_original_size,
                bv.unit_lba_count,
                bv.compression,
                bv.crc32,
                bv.flags,
            );
            if !seen.insert(frag_key) {
                continue;
            }
            let Some((vol_id, lba)) = decode_blockmap_key(&key) else {
                continue;
            };
            fragments.push((vol_id, lba, bv));
            if limit.is_some_and(|max| fragments.len() >= max) {
                break;
            }
        }
        Ok(fragments)
    }

    // TEMP(soak-debug): exemplar collection for corruption logs. Full scan;
    // remove or gate once the root cause is fixed.
    /// Return exemplar live fragment mappings for a PBA.
    pub fn unique_fragments_for_pba(
        &self,
        target_pba: Pba,
        limit: usize,
    ) -> OnyxResult<Vec<(String, Lba, BlockmapValue)>> {
        self.unique_fragments_for_pba_inner(target_pba, Some(limit))
    }

    // TEMP(soak-debug): shared-slot overlap detector for early corruption
    // fail-fast. Remove or gate after root cause is fixed.
    /// Return the first pair of distinct live fragments at a PBA whose byte
    /// ranges overlap inside the shared 4KB slot.
    pub fn first_fragment_overlap_at_pba(
        &self,
        target_pba: Pba,
    ) -> OnyxResult<Option<((String, Lba, BlockmapValue), (String, Lba, BlockmapValue))>> {
        let fragments = self.unique_fragments_for_pba_inner(target_pba, None)?;
        for i in 0..fragments.len() {
            let lhs = &fragments[i];
            let lhs_start = lhs.2.slot_offset as u32;
            let lhs_end = lhs_start + lhs.2.unit_compressed_size;
            for rhs in &fragments[i + 1..] {
                let rhs_start = rhs.2.slot_offset as u32;
                let rhs_end = rhs_start + rhs.2.unit_compressed_size;
                if lhs_start < rhs_end && rhs_start < lhs_end {
                    return Ok(Some((lhs.clone(), rhs.clone())));
                }
            }
        }
        Ok(None)
    }

    // TEMP(soak-debug): heavy refcount reconciliation used to keep soak runs
    // alive while we locate the real bug. Remove or gate after the allocator /
    // hole-map issue is fixed and verified.
    /// Repair a single PBA's refcount from the live blockmap if it drifted.
    ///
    /// This is intentionally slow and only used on suspicious paths such as
    /// "refcount says zero, but freeing this PBA would be dangerous". The
    /// returned value is the exact live refcount observed in blockmap.
    pub fn reconcile_refcount_for_pba(&self, pba: Pba) -> OnyxResult<u32> {
        let _refcount_guards = self.lock_refcount_pbas([pba]);
        let current = self.get_refcount(pba)?;
        let actual = self.count_blockmap_refs_for_pba_inner(pba)?;
        if current == actual {
            return Ok(actual);
        }

        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();
        let key = encode_refcount_key(pba);
        if actual == 0 {
            self.db.delete_cf(&cf_refcount, &key)?;
        } else {
            self.db
                .put_cf(&cf_refcount, &key, &encode_refcount_value(actual))?;
        }

        tracing::error!(
            pba = pba.0,
            recorded_refcount = current,
            actual_refcount = actual,
            "reconciled refcount from live blockmap references"
        );

        Ok(actual)
    }

    /// Check whether the exact packed fragment described by `target` still has
    /// any live blockmap reference.
    ///
    /// Ignores `offset_in_unit` because any surviving block in the compression
    /// unit keeps the fragment's byte range live.
    fn get_fragment_refcount(&self, target: &BlockmapValue) -> OnyxResult<u32> {
        let cf = self.db.cf_handle(CF_FRAGMENT_REFS).unwrap();
        let key = encode_fragment_ref_key(&FragmentRefKey::from(target));
        match self.db.get_cf(&cf, key)? {
            Some(data) => Ok(decode_refcount_value(&data).unwrap_or(0)),
            None => Ok(0),
        }
    }

    pub fn has_live_fragment_ref(&self, target: &BlockmapValue) -> OnyxResult<bool> {
        Ok(self.get_fragment_refcount(target)? > 0)
    }

    /// Scan for and remove orphaned refcount entries — PBAs with refcount > 0
    /// but no blockmap entries referencing them. Returns the PBAs that were cleaned up.
    pub fn cleanup_orphaned_refcounts(&self) -> OnyxResult<Vec<(Pba, u32)>> {
        let _blockmap_guards = self.lock_all_blockmap_stripes();
        let _refcount_guards = self.lock_all_refcount_stripes();
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();

        // Collect all PBAs referenced by blockmap entries
        let mut referenced_pbas = std::collections::HashSet::new();
        let bm_iter = self
            .db
            .iterator_cf(&cf_blockmap, rocksdb::IteratorMode::Start);
        for item in bm_iter {
            let (_, value) = item?;
            if let Some(bv) = decode_blockmap_value(&value) {
                referenced_pbas.insert(bv.pba);
            }
        }

        // Scan refcount CF, find entries not in referenced set
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
        let cf_fragment_refs = self.db.cf_handle(CF_FRAGMENT_REFS).unwrap();
        let fill_start = fill_offset as u32;
        let fill_end = fill_start + fill_size as u32;
        let prefix = fragment_ref_prefix(target_pba);
        let mut iter = self.db.raw_iterator_cf(&cf_fragment_refs);
        iter.seek(&prefix);
        while iter.valid() {
            let Some(key) = iter.key() else {
                break;
            };
            if !key.starts_with(&prefix) {
                break;
            }
            let Some(value) = iter.value() else {
                iter.next();
                continue;
            };
            if decode_refcount_value(value).unwrap_or(0) == 0 {
                iter.next();
                continue;
            }
            if let Some(fragment) = decode_fragment_ref_key(key) {
                let frag_start = fragment.slot_offset as u32;
                let frag_end = frag_start + fragment.unit_compressed_size;
                if fill_start < frag_end && frag_start < fill_end {
                    iter.status()?;
                    return Ok(true);
                }
            }
            iter.next();
        }
        iter.status()?;
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
    /// and decrement old PBA refcount if applicable.
    ///
    /// The old mapping is re-read **inside** the lock to prevent stale-read races
    /// (the caller's pre-read old_pba may have been changed by another thread).
    ///
    /// Returns `Some((old_pba, block_count))` if an old PBA was decremented, for cleanup.
    pub fn atomic_dedup_hit(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        new_value: &BlockmapValue,
        hash: &ContentHash,
    ) -> OnyxResult<Option<(Pba, u32)>> {
        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();
        let cf_fragment_refs = self.db.cf_handle(CF_FRAGMENT_REFS).unwrap();
        let cf_dedup_reverse = self.db.cf_handle(CF_DEDUP_REVERSE).unwrap();

        let bm_key = encode_blockmap_key(vol_id, lba)?;
        let _blockmap_guards = self.lock_blockmap_keys([bm_key.as_slice()]);

        let old_mapping = self.get_mapping(vol_id, lba)?;
        let old_pba = old_mapping.as_ref().map(|m| m.pba);
        let same_pba = old_pba.map_or(false, |old| old == new_value.pba);
        let mut touched_pbas = vec![new_value.pba];
        if let Some(old) = old_pba {
            touched_pbas.push(old);
        }
        let _refcount_guards = self.lock_refcount_pbas(touched_pbas);

        // Guard: re-verify the dedup_reverse entry still exists.
        // Between the caller's dedup_entry_is_live() check and this point,
        // cleanup_dead_pba_post_commit may have deleted the dedup_reverse
        // entry, freed the PBA, and another write may have reused it.
        let reverse_key = encode_dedup_reverse_key(new_value.pba, hash);
        if self.db.get_cf(&cf_dedup_reverse, &reverse_key)?.is_none() {
            return Err(OnyxError::Io(std::io::Error::other(format!(
                "dedup hit rejected: dedup_reverse entry for PBA {:?} was cleaned up (PBA freed between liveness check and hit)",
                new_value.pba,
            ))));
        }

        let mut batch = WriteBatch::default();

        let bm_val = encode_blockmap_value(new_value);
        batch.put_cf(&cf_blockmap, &bm_key, &bm_val);
        Self::apply_fragment_ref_replacement(
            &mut batch,
            &cf_fragment_refs,
            old_mapping.as_ref(),
            Some(new_value),
        );

        let mut decremented_old: Option<(Pba, u32)> = None;

        if !same_pba {
            // Guard: reject the hit if the dedup target PBA was freed.
            let current_rc = self.get_refcount(new_value.pba)?;
            if current_rc == 0 {
                return Err(OnyxError::Io(std::io::Error::other(format!(
                    "dedup hit rejected: target PBA {:?} refcount is 0 (freed between liveness check and hit)",
                    new_value.pba,
                ))));
            }

            // Increment refcount for existing PBA (reuse current_rc, no second read)
            let rc_key = encode_refcount_key(new_value.pba);
            batch.put_cf(
                &cf_refcount,
                &rc_key,
                &encode_refcount_value(current_rc + 1),
            );

            // Decrement old PBA refcount (read + put/delete inside lock)
            if let Some(old) = &old_mapping {
                let old_count = self.get_refcount(old.pba)?;
                let old_new = old_count.saturating_sub(1);
                let old_rc_key = encode_refcount_key(old.pba);
                if old_new == 0 {
                    batch.delete_cf(&cf_refcount, &old_rc_key);
                } else {
                    batch.put_cf(&cf_refcount, &old_rc_key, &encode_refcount_value(old_new));
                }
                let old_blocks = old.unit_compressed_size.div_ceil(crate::types::BLOCK_SIZE);
                decremented_old = Some((old.pba, old_blocks));
            }
        }
        // If same PBA: blockmap is refreshed but refcount stays unchanged

        self.db.write_opt(batch, &self.hot_write_opts)?;
        Ok(decremented_old)
    }

    /// Atomically commit a batch of dedup hits in one lock acquisition + one
    /// WriteBatch.  All hits must belong to the same volume (CoalesceUnit
    /// invariant).  Returns a result per input hit.
    ///
    /// For each hit the method:
    ///   1. Re-reads the current blockmap mapping inside the lock.
    ///   2. If the LBA already points to the target PBA → refresh blockmap only
    ///      (no refcount change), result = `Ok(None)`.
    ///   3. Guard: if the target PBA's refcount is 0 → `Rejected`.
    ///   4. Otherwise: write new blockmap, increment target PBA refcount, and
    ///      merge-decrement the old PBA refcount.
    pub fn atomic_batch_dedup_hits(
        &self,
        vol_id: &VolumeId,
        hits: &[(Lba, BlockmapValue, ContentHash)],
    ) -> OnyxResult<Vec<DedupHitResult>> {
        if hits.is_empty() {
            return Ok(Vec::new());
        }

        let cf_blockmap = self.db.cf_handle(CF_BLOCKMAP).unwrap();
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();
        let cf_fragment_refs = self.db.cf_handle(CF_FRAGMENT_REFS).unwrap();
        let cf_dedup_reverse = self.db.cf_handle(CF_DEDUP_REVERSE).unwrap();

        let lbas: Vec<Lba> = hits.iter().map(|(lba, _, _)| *lba).collect();
        let bm_keys: Vec<Vec<u8>> = lbas
            .iter()
            .map(|lba| encode_blockmap_key(vol_id, *lba))
            .collect::<OnyxResult<Vec<_>>>()?;
        let _blockmap_guards = self.lock_blockmap_keys(&bm_keys);
        let old_mappings = self.multi_get_mappings(vol_id, &lbas)?;

        let mut touched_pbas = Vec::with_capacity(hits.len() * 2);
        for (idx, (_, new_val, _)) in hits.iter().enumerate() {
            touched_pbas.push(new_val.pba);
            if let Some(old) = old_mappings[idx] {
                touched_pbas.push(old.pba);
            }
        }
        let _refcount_guards = self.lock_refcount_pbas(touched_pbas);

        // Collect unique target PBAs that need refcount reads for the guard
        // check.  Old PBA refcounts are not read — we use merge for decrements.
        let mut pba_index: HashMap<Pba, usize> = HashMap::new();
        let mut pbas_to_read: Vec<Pba> = Vec::new();

        for (idx, (_, new_val, _)) in hits.iter().enumerate() {
            let same_pba = old_mappings[idx]
                .as_ref()
                .map_or(false, |old| old.pba == new_val.pba);
            if !same_pba && !pba_index.contains_key(&new_val.pba) {
                pba_index.insert(new_val.pba, pbas_to_read.len());
                pbas_to_read.push(new_val.pba);
            }
        }

        let refcounts = if pbas_to_read.is_empty() {
            Vec::new()
        } else {
            self.multi_get_refcounts(&pbas_to_read)?
        };

        let mut batch = WriteBatch::default();
        let mut results: Vec<DedupHitResult> = Vec::with_capacity(hits.len());
        let mut target_increments: HashMap<Pba, u32> = HashMap::new();
        let mut old_decrements: HashMap<Pba, u32> = HashMap::new();

        for (idx, (_lba, new_val, hash)) in hits.iter().enumerate() {
            let old_mapping = &old_mappings[idx];
            let same_pba = old_mapping
                .as_ref()
                .map_or(false, |old| old.pba == new_val.pba);

            // Guard: re-verify the dedup_reverse entry still exists.
            // Between the caller's dedup_entry_is_live() check and this point,
            // cleanup_dead_pba_post_commit may have deleted the dedup_reverse
            // entry, freed the PBA, and another write may have reused it.
            // The refcount-only guard below would pass (reused PBA has rc > 0),
            // but the data no longer matches this dedup entry.
            let reverse_key = encode_dedup_reverse_key(new_val.pba, hash);
            if self.db.get_cf(&cf_dedup_reverse, &reverse_key)?.is_none() {
                results.push(DedupHitResult::Rejected);
                continue;
            }

            if same_pba {
                // Refresh blockmap only — no refcount change.
                batch.put_cf(&cf_blockmap, &bm_keys[idx], &encode_blockmap_value(new_val));
                Self::apply_fragment_ref_replacement(
                    &mut batch,
                    &cf_fragment_refs,
                    old_mapping.as_ref(),
                    Some(new_val),
                );
                results.push(DedupHitResult::Accepted(None));
                continue;
            }

            // Guard: effective refcount must be > 0, accounting for
            // increments/decrements already accumulated in this batch.
            let rc_idx = pba_index[&new_val.pba];
            let base_rc = refcounts[rc_idx];
            let accumulated = target_increments.get(&new_val.pba).copied().unwrap_or(0);
            let dec_so_far = old_decrements.get(&new_val.pba).copied().unwrap_or(0);
            let effective_rc = (base_rc as i64) + (accumulated as i64) - (dec_so_far as i64);
            if effective_rc <= 0 {
                results.push(DedupHitResult::Rejected);
                continue;
            }

            batch.put_cf(&cf_blockmap, &bm_keys[idx], &encode_blockmap_value(new_val));
            Self::apply_fragment_ref_replacement(
                &mut batch,
                &cf_fragment_refs,
                old_mapping.as_ref(),
                Some(new_val),
            );
            *target_increments.entry(new_val.pba).or_insert(0) += 1;

            let mut decremented_old: Option<(Pba, u32)> = None;
            if let Some(old) = old_mapping {
                *old_decrements.entry(old.pba).or_insert(0) += 1;
                let old_blocks = old.unit_compressed_size.div_ceil(crate::types::BLOCK_SIZE);
                decremented_old = Some((old.pba, old_blocks));
            }
            results.push(DedupHitResult::Accepted(decremented_old));
        }

        // Target PBAs: base + total_increment − overlapping decrements → put
        for (pba, increment) in &target_increments {
            let rc_idx = pba_index[pba];
            let base = refcounts[rc_idx];
            let dec = old_decrements.get(pba).copied().unwrap_or(0);
            let final_rc = (base as i64) + (*increment as i64) - (dec as i64);
            let rc_key = encode_refcount_key(*pba);
            if final_rc <= 0 {
                batch.delete_cf(&cf_refcount, &rc_key);
            } else {
                batch.put_cf(
                    &cf_refcount,
                    &rc_key,
                    &encode_refcount_value(final_rc as u32),
                );
            }
        }

        let decrement_only_pbas: Vec<Pba> = old_decrements
            .keys()
            .filter(|pba| !target_increments.contains_key(pba))
            .copied()
            .collect();
        let decrement_only_refcounts = self.refcounts_by_pba(&decrement_only_pbas)?;

        for (pba, decrement) in &old_decrements {
            if target_increments.contains_key(pba) {
                continue;
            }
            let current_rc = decrement_only_refcounts.get(pba).copied().unwrap_or(0);
            let new_rc = current_rc.saturating_sub(*decrement);
            let rc_key = encode_refcount_key(*pba);
            if new_rc == 0 {
                batch.delete_cf(&cf_refcount, &rc_key);
            } else {
                batch.put_cf(&cf_refcount, &rc_key, &encode_refcount_value(new_rc));
            }
        }

        self.db.write_opt(batch, &self.hot_write_opts)?;
        Ok(results)
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
        self.db.write_opt(batch, &self.hot_write_opts)?;
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

    /// Delete a single dedup index entry by hash, including its reverse entry.
    /// Best-effort stale cleanup for hit paths that discover an index entry no
    /// longer points at a live physical fragment.
    pub fn delete_dedup_index(&self, hash: &ContentHash) -> OnyxResult<()> {
        let cf_index = self.db.cf_handle(CF_DEDUP_INDEX).unwrap();
        let cf_reverse = self.db.cf_handle(CF_DEDUP_REVERSE).unwrap();
        let mut batch = WriteBatch::default();
        if let Some(raw) = self.db.get_cf(&cf_index, hash)? {
            if let Some(entry) = decode_dedup_entry(&raw) {
                let reverse_key = encode_dedup_reverse_key(entry.pba, hash);
                batch.delete_cf(&cf_reverse, &reverse_key);
            }
        }
        batch.delete_cf(&cf_index, hash);
        self.db.write_opt(batch, &self.hot_write_opts)?;
        Ok(())
    }

    /// A dedup entry is safe to use only if the owning PBA is still referenced,
    /// the hash is still registered to that PBA in dedup_reverse, and the
    /// exact fragment metadata still has a live blockmap reference.
    ///
    /// The exact-fragment check stays always-on. A stale dedup entry can
    /// survive long enough for a recycled PBA to have `refcount > 0` again,
    /// and the old reverse entry may still exist until cleanup catches up.
    /// Trusting only `(pba, refcount, reverse-key)` in that window can remap a
    /// block onto unrelated bytes and surface later as a CRC mismatch.
    pub fn dedup_entry_is_live(&self, hash: &ContentHash, entry: &DedupEntry) -> OnyxResult<bool> {
        if self.get_refcount(entry.pba)? == 0 {
            return Ok(false);
        }
        let cf_reverse = self.db.cf_handle(CF_DEDUP_REVERSE).unwrap();
        let reverse_key = encode_dedup_reverse_key(entry.pba, hash);
        if self.db.get_cf(&cf_reverse, &reverse_key)?.is_none() {
            return Ok(false);
        }
        self.has_live_fragment_ref(&entry.to_blockmap_value())
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
