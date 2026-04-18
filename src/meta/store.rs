mod blockmap;
mod dedup;
mod open;
mod refcount;
mod scan;
mod volume;

#[cfg(test)]
mod tests;

use std::sync::{Mutex, MutexGuard};

use rocksdb::{DBWithThreadMode, MergeOperands, MultiThreaded, WriteOptions};

use crate::types::Pba;

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
    db: DBWithThreadMode<MultiThreaded>,
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
    /// in flush/dedup/GC paths). WAL is still written to the OS buffer, but
    /// we skip the fsync that dominates lock-hold time (~20 ms -> ~0.5 ms).
    /// Crash durability is provided by the buffer ring: LV2 write thread does
    /// fdatasync before ack; on crash, unflushed buffer entries are replayed
    /// idempotently by the flusher, re-deriving all hot-path metadata.
    /// Cold-path operations (create/delete volume, reconciliation) keep
    /// sync = true via the default `db.write(batch)`.
    hot_write_opts: WriteOptions,
    /// Block cache size from config — reused when creating new per-volume CFs.
    block_cache_mb: usize,
}

impl MetaStore {
    /// Global (non-blockmap) column families that always exist.
    const GLOBAL_CFS: [&'static str; 4] = [
        crate::meta::schema::CF_VOLUMES,
        crate::meta::schema::CF_REFCOUNT,
        crate::meta::schema::CF_DEDUP_INDEX,
        crate::meta::schema::CF_DEDUP_REVERSE,
    ];

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

    fn refcounts_by_pba(
        &self,
        pbas: &[Pba],
    ) -> crate::error::OnyxResult<std::collections::HashMap<Pba, u32>> {
        let counts = self.multi_get_refcounts(pbas)?;
        Ok(pbas.iter().copied().zip(counts).collect())
    }
}
