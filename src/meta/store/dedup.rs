use std::collections::HashMap;

use rocksdb::WriteBatch;

use crate::error::{OnyxError, OnyxResult};
use crate::meta::schema::*;
use crate::types::{Lba, Pba, VolumeId, BLOCK_SIZE};

use super::{DedupHitResult, MetaStore};

impl MetaStore {
    /// Atomically handle a dedup hit: write blockmap entry, increment existing PBA refcount,
    /// and decrement old PBA refcount if applicable.
    ///
    /// The old mapping is re-read **inside** the lock to prevent stale-read races
    /// (the caller's pre-read old_pba may have been changed by another thread).
    ///
    /// Returns `Some((old_pba, block_count))` if an old PBA was decremented, for cleanup.
    #[allow(unused_variables)]
    pub fn atomic_dedup_hit(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        new_value: &BlockmapValue,
        hash: &ContentHash,
    ) -> OnyxResult<Option<(Pba, u32)>> {
        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();
        let cf_dedup_reverse = self.db.cf_handle(CF_DEDUP_REVERSE).unwrap();

        let bm_key = encode_blockmap_key(lba);
        let _blockmap_guards = self.lock_blockmap_keys([bm_key.as_slice()]);

        let old_mapping = self.redb.get_mapping(&vol_id.0, lba)?;
        let old_pba = old_mapping.as_ref().map(|m| m.pba);
        let same_pba = old_pba.is_some_and(|old| old == new_value.pba);
        let mut touched_pbas = vec![new_value.pba];
        if let Some(old) = old_pba {
            touched_pbas.push(old);
        }
        let _refcount_guards = self.lock_refcount_pbas(touched_pbas);

        #[cfg(debug_assertions)]
        {
            let reverse_key = encode_dedup_reverse_key(new_value.pba, hash);
            if self.db.get_cf(&cf_dedup_reverse, &reverse_key)?.is_none() {
                return Err(OnyxError::Io(std::io::Error::other(format!(
                    "dedup hit rejected: dedup_reverse entry for PBA {:?} was cleaned up (PBA freed between liveness check and hit)",
                    new_value.pba,
                ))));
            }
        }

        let mut batch = WriteBatch::default();
        let mut decremented_old: Option<(Pba, u32)> = None;

        if !same_pba {
            let current_rc = self.get_refcount(new_value.pba)?;
            if current_rc == 0 {
                return Err(OnyxError::Io(std::io::Error::other(format!(
                    "dedup hit rejected: target PBA {:?} refcount is 0 (freed between liveness check and hit)",
                    new_value.pba,
                ))));
            }

            let rc_key = encode_refcount_key(new_value.pba);
            batch.put_cf(
                &cf_refcount,
                &rc_key,
                &encode_refcount_value(current_rc + 1),
            );

            if let Some(old) = &old_mapping {
                let old_count = self.get_refcount(old.pba)?;
                let old_new = old_count.saturating_sub(1);
                let old_rc_key = encode_refcount_key(old.pba);
                if old_new == 0 {
                    batch.delete_cf(&cf_refcount, &old_rc_key);
                    if old_count > 0 {
                        let old_blocks = old.unit_compressed_size.div_ceil(BLOCK_SIZE);
                        decremented_old = Some((old.pba, old_blocks));
                    }
                } else {
                    batch.put_cf(&cf_refcount, &old_rc_key, &encode_refcount_value(old_new));
                }
            }
        }

        self.db.write_opt(batch, &self.hot_write_opts)?;

        // Commit blockmap change to redb. If same_pba and no refcount change,
        // still refresh the blockmap entry (e.g., LBA may need updated flags).
        self.redb.put_mapping(&vol_id.0, lba, *new_value)?;

        Ok(decremented_old)
    }

    /// Atomically commit a batch of dedup hits in one lock acquisition + one
    /// WriteBatch. All hits must belong to the same volume (CoalesceUnit
    /// invariant). Returns a result per input hit.
    ///
    /// For each hit the method:
    ///   1. Re-reads the current blockmap mapping inside the lock.
    ///   2. If the LBA already points to the target PBA -> refresh blockmap only
    ///      (no refcount change), result = `Ok(None)`.
    ///   3. Guard: if the target PBA's refcount is 0 -> `Rejected`.
    ///   4. Otherwise: write new blockmap, increment target PBA refcount, and
    ///      merge-decrement the old PBA refcount.
    /// Returns `(per-hit results, newly_zeroed old PBAs)`.
    ///
    /// `newly_zeroed` contains only PBAs that THIS batch actually drove from
    /// refcount > 0 to 0. The caller should only call cleanup/free for PBAs
    /// in this map — blindly cleaning up every decremented PBA risks
    /// double-free when two concurrent batches both decrement the same PBA.
    #[allow(unused_variables)]
    pub fn atomic_batch_dedup_hits(
        &self,
        vol_id: &VolumeId,
        hits: &[(Lba, BlockmapValue, ContentHash)],
    ) -> OnyxResult<(Vec<DedupHitResult>, HashMap<Pba, u32>)> {
        if hits.is_empty() {
            return Ok((Vec::new(), HashMap::new()));
        }

        let cf_refcount = self.db.cf_handle(CF_REFCOUNT).unwrap();
        let cf_dedup_reverse = self.db.cf_handle(CF_DEDUP_REVERSE).unwrap();

        let lbas: Vec<Lba> = hits.iter().map(|(lba, _, _)| *lba).collect();
        let bm_keys: Vec<[u8; 8]> = lbas.iter().map(|lba| encode_blockmap_key(*lba)).collect();
        let _blockmap_guards = self.lock_blockmap_keys(&bm_keys);
        let old_mappings = self.redb.batch_get_mappings(&vol_id.0, &lbas)?;

        let mut touched_pbas = Vec::with_capacity(hits.len() * 2);
        for (idx, (_, new_val, _)) in hits.iter().enumerate() {
            touched_pbas.push(new_val.pba);
            if let Some(old) = old_mappings[idx] {
                touched_pbas.push(old.pba);
            }
        }
        let _refcount_guards = self.lock_refcount_pbas(touched_pbas);

        let mut pba_index: HashMap<Pba, usize> = HashMap::new();
        let mut pbas_to_read: Vec<Pba> = Vec::new();

        for (idx, (_, new_val, _)) in hits.iter().enumerate() {
            let same_pba = old_mappings[idx]
                .as_ref()
                .is_some_and(|old| old.pba == new_val.pba);
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
        let mut old_decrements: HashMap<Pba, (u32, u32)> = HashMap::new();
        // Records which hit indices actually need a blockmap write in redb.
        let mut accepted_idxs: Vec<usize> = Vec::new();

        for (idx, (_lba, new_val, hash)) in hits.iter().enumerate() {
            let old_mapping = &old_mappings[idx];
            let same_pba = old_mapping
                .as_ref()
                .is_some_and(|old| old.pba == new_val.pba);

            #[cfg(debug_assertions)]
            {
                let reverse_key = encode_dedup_reverse_key(new_val.pba, hash);
                if self.db.get_cf(&cf_dedup_reverse, &reverse_key)?.is_none() {
                    results.push(DedupHitResult::Rejected);
                    continue;
                }
            }

            if same_pba {
                // LBA already points to the target PBA. Still refresh the
                // blockmap entry in redb (flags may have changed).
                accepted_idxs.push(idx);
                results.push(DedupHitResult::Accepted(None));
                continue;
            }

            let rc_idx = pba_index[&new_val.pba];
            let base_rc = refcounts[rc_idx];
            let accumulated = target_increments.get(&new_val.pba).copied().unwrap_or(0);
            let (dec_so_far, _) = old_decrements.get(&new_val.pba).copied().unwrap_or((0, 1));
            let effective_rc = (base_rc as i64) + (accumulated as i64) - (dec_so_far as i64);
            if effective_rc <= 0 {
                results.push(DedupHitResult::Rejected);
                continue;
            }

            accepted_idxs.push(idx);
            *target_increments.entry(new_val.pba).or_insert(0) += 1;

            if let Some(old) = old_mapping {
                let old_blocks = old.unit_compressed_size.div_ceil(BLOCK_SIZE);
                let entry = old_decrements.entry(old.pba).or_insert((0, old_blocks));
                entry.0 += 1;
                entry.1 = entry.1.max(old_blocks);
            }
            results.push(DedupHitResult::Accepted(None));
        }

        for (pba, increment) in &target_increments {
            let rc_idx = pba_index[pba];
            let base = refcounts[rc_idx];
            let (dec, _) = old_decrements.get(pba).copied().unwrap_or((0, 1));
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

        let mut newly_zeroed: HashMap<Pba, u32> = HashMap::new();

        for (pba, increment) in &target_increments {
            let rc_idx = pba_index[pba];
            let base = refcounts[rc_idx];
            let (dec, blocks) = old_decrements.get(pba).copied().unwrap_or((0, 1));
            let final_rc = (base as i64) + (*increment as i64) - (dec as i64);
            if final_rc <= 0 && base > 0 && dec > 0 {
                newly_zeroed.insert(*pba, blocks);
            }
        }

        let decrement_only_pbas: Vec<Pba> = old_decrements
            .keys()
            .filter(|pba| !target_increments.contains_key(pba))
            .copied()
            .collect();
        let decrement_only_refcounts = self.refcounts_by_pba(&decrement_only_pbas)?;

        for (pba, (decrement, blocks)) in &old_decrements {
            if target_increments.contains_key(pba) {
                continue;
            }
            let current_rc = decrement_only_refcounts.get(pba).copied().unwrap_or(0);
            let new_rc = current_rc.saturating_sub(*decrement);
            let rc_key = encode_refcount_key(*pba);
            if new_rc == 0 {
                batch.delete_cf(&cf_refcount, &rc_key);
                if current_rc > 0 {
                    newly_zeroed.insert(*pba, *blocks);
                }
            } else {
                batch.put_cf(&cf_refcount, &rc_key, &encode_refcount_value(new_rc));
            }
        }

        self.db.write_opt(batch, &self.hot_write_opts)?;

        // Commit blockmap updates for every accepted hit.
        if !accepted_idxs.is_empty() {
            let redb_entries: Vec<(&str, Lba, BlockmapValue)> = accepted_idxs
                .iter()
                .map(|&idx| (vol_id.0.as_str(), hits[idx].0, hits[idx].1))
                .collect();
            self.redb.batch_put_mappings(&redb_entries)?;
        }

        Ok((results, newly_zeroed))
    }

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

    /// A dedup entry is safe to use only if the owning PBA is still referenced
    /// and the hash is still registered to that PBA in dedup_reverse.
    pub fn dedup_entry_is_live(&self, hash: &ContentHash, entry: &DedupEntry) -> OnyxResult<bool> {
        if self.get_refcount(entry.pba)? == 0 {
            return Ok(false);
        }
        let cf_reverse = self.db.cf_handle(CF_DEDUP_REVERSE).unwrap();
        let reverse_key = encode_dedup_reverse_key(entry.pba, hash);
        if self.db.get_cf(&cf_reverse, &reverse_key)?.is_none() {
            return Ok(false);
        }
        Ok(true)
    }

    /// Clean up dedup index + reverse entries for a given PBA.
    /// Called when refcount -> 0 (PBA is freed).
    /// Only deletes the forward index entry if it still points to this PBA —
    /// the same hash may have been re-registered to a different PBA by a
    /// concurrent write, and we must not delete that newer mapping.
    /// Adds deletions to the provided WriteBatch for atomicity.
    pub fn cleanup_dedup_for_pba(&self, pba: Pba, batch: &mut WriteBatch) -> OnyxResult<()> {
        let cf_index = self.db.cf_handle(CF_DEDUP_INDEX).unwrap();
        let cf_reverse = self.db.cf_handle(CF_DEDUP_REVERSE).unwrap();

        let prefix = pba.0.to_be_bytes();
        let mut iter = self.db.raw_iterator_cf(&cf_reverse);
        iter.seek(&prefix);
        while iter.valid() {
            if let Some(key) = iter.key() {
                if !key.starts_with(&prefix) {
                    break;
                }
                if let Some((_pba, hash)) = decode_dedup_reverse_key(key) {
                    if let Some(current_entry) = self.get_dedup_entry(&hash)? {
                        if current_entry.pba == pba {
                            batch.delete_cf(&cf_index, &hash);
                        }
                    }
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

    /// Scan all blockmap entries for DEDUP_SKIPPED flag. Uses redb paged scan.
    /// Returns (vol_id, lba, BlockmapValue) tuples, up to `limit`.
    pub fn scan_dedup_skipped(
        &self,
        limit: usize,
    ) -> OnyxResult<Vec<(String, Lba, BlockmapValue)>> {
        let mut results: Vec<(String, Lba, BlockmapValue)> = Vec::new();
        // Use a manual iteration so we can stop early when `limit` is reached.
        // `scan_all_mappings` does not support early termination, so we run it
        // to completion but short-circuit the callback.
        self.redb.scan_all_mappings(|vol_id, lba, bv| {
            if results.len() >= limit {
                return;
            }
            if bv.flags & FLAG_DEDUP_SKIPPED != 0 {
                results.push((vol_id.to_string(), lba, bv));
            }
        })?;
        Ok(results)
    }

    /// Update a single blockmap entry's flags (e.g., clear DEDUP_SKIPPED).
    /// Read-modify-write on the redb blockmap; no refcount involvement.
    pub fn update_blockmap_flags(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        new_flags: u8,
    ) -> OnyxResult<()> {
        let key = encode_blockmap_key(lba);
        let _blockmap_guards = self.lock_blockmap_keys([key.as_slice()]);
        let Some(mut bv) = self.redb.get_mapping(&vol_id.0, lba)? else {
            return Ok(());
        };
        bv.flags = new_flags;
        self.redb.put_mapping(&vol_id.0, lba, bv)?;
        Ok(())
    }
}
