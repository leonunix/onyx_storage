//! `RedbStore`: paged blockmap backed by a single redb file.
//!
//! Public entry points:
//! - [`RedbStore::open`] — create or open the redb file, ensure all tables exist.
//! - volume CRUD: [`put_volume`], [`get_volume`], [`delete_volume`].
//! - mapping CRUD: [`get_mapping`], [`put_mapping`], [`delete_mapping`].
//!
//! Each mapping op runs in its own write transaction for v1. Batch helpers for
//! flusher integration land in the next patch.

use std::path::Path;
use std::sync::Arc;

use redb::{Database, Durability, ReadableDatabase, ReadableTable};

use crate::error::{OnyxError, OnyxResult};
use crate::meta::redb::page::{L2Page, LBAS_PER_PAGE};
use crate::meta::redb::schema::{
    L1Entry, VolumeRoot, T_L1, T_L2_PAGES, T_PAGE_FREE, T_PAGE_NEXT_ID, T_PAGE_REFS, T_VOLUMES,
};
use crate::meta::schema::BlockmapValue;
use crate::types::Lba;

const PAGE_NEXT_ID_KEY: &str = "";

/// Paged blockmap store on redb.
#[derive(Clone)]
pub struct RedbStore {
    db: Arc<Database>,
}

/// Begin a write transaction configured with the hot-path durability. redb's
/// `Durability::None` skips fsync on commit; the page changes stay queryable
/// by subsequent read transactions (logical commit) but are not persistent
/// until a later [`RedbStore::sync`] call. The buffer commit log is the
/// persistence anchor for any unsynced blockmap updates — on crash, recovery
/// replays from buffer and the partial redb state is discarded in the
/// reconcile step.
fn begin_hot_write(db: &Database) -> OnyxResult<redb::WriteTransaction> {
    let mut write = db.begin_write().map_err(to_onyx)?;
    write.set_durability(Durability::None).map_err(to_onyx)?;
    Ok(write)
}

impl RedbStore {
    /// Open (or create) the redb file at `path` and ensure every required table exists.
    pub fn open(path: &Path) -> OnyxResult<Self> {
        let db = Database::create(path).map_err(to_onyx)?;
        // Materialize tables so later read-only txns find them.
        let write = db.begin_write().map_err(to_onyx)?;
        {
            let _ = write.open_table(T_VOLUMES).map_err(to_onyx)?;
            let _ = write.open_table(T_L1).map_err(to_onyx)?;
            let _ = write.open_table(T_L2_PAGES).map_err(to_onyx)?;
            let _ = write.open_table(T_PAGE_REFS).map_err(to_onyx)?;
            let _ = write.open_table(T_PAGE_FREE).map_err(to_onyx)?;
            let _ = write.open_table(T_PAGE_NEXT_ID).map_err(to_onyx)?;
        }
        write.commit().map_err(to_onyx)?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Force all prior `Durability::None` writes to disk by issuing an empty
    /// `Durability::Immediate` commit. Called from engine shutdown so the
    /// clean-shutdown superblock marker is only written after all blockmap
    /// state is physically persistent.
    pub fn sync(&self) -> OnyxResult<()> {
        let write = self.db.begin_write().map_err(to_onyx)?;
        write.commit().map_err(to_onyx)?;
        Ok(())
    }

    // ---- volume CRUD --------------------------------------------------------

    pub fn put_volume(&self, vol_id: &str, root: VolumeRoot) -> OnyxResult<()> {
        let encoded = bincode::serialize(&root)
            .map_err(|e| OnyxError::Config(format!("volume root serialize: {e}")))?;
        let write = begin_hot_write(&self.db)?;
        {
            let mut table = write.open_table(T_VOLUMES).map_err(to_onyx)?;
            table
                .insert(vol_id, encoded.as_slice())
                .map_err(to_onyx)?;
        }
        write.commit().map_err(to_onyx)?;
        Ok(())
    }

    pub fn get_volume(&self, vol_id: &str) -> OnyxResult<Option<VolumeRoot>> {
        let read = self.db.begin_read().map_err(to_onyx)?;
        let table = read.open_table(T_VOLUMES).map_err(to_onyx)?;
        let Some(raw) = table.get(vol_id).map_err(to_onyx)? else {
            return Ok(None);
        };
        let root: VolumeRoot = bincode::deserialize(raw.value())
            .map_err(|e| OnyxError::Config(format!("volume root deserialize: {e}")))?;
        Ok(Some(root))
    }

    /// Drop every L1 entry, every referenced L2 page, and the volume row.
    ///
    /// Pages shared with snapshots (refcount > 1) just have their refcount
    /// decremented; orphaned pages go to the freelist.
    pub fn delete_volume(&self, vol_id: &str) -> OnyxResult<()> {
        let write = begin_hot_write(&self.db)?;
        {
            // Collect all page_ids to decrement before mutating tables.
            let l1_entries = {
                let t_l1 = write.open_table(T_L1).map_err(to_onyx)?;
                let start = (vol_id, 0u64);
                let end = (vol_id, u64::MAX);
                let mut collected: Vec<(u64, L1Entry)> = Vec::new();
                let iter = t_l1.range(start..=end).map_err(to_onyx)?;
                for item in iter {
                    let (key, val) = item.map_err(to_onyx)?;
                    let (_, l1_idx) = key.value();
                    let entry = L1Entry::decode(&val.value())
                        .expect("L1 entry encoded by this module must roundtrip");
                    collected.push((l1_idx, entry));
                }
                collected
            };

            let mut t_l1 = write.open_table(T_L1).map_err(to_onyx)?;
            let mut t_refs = write.open_table(T_PAGE_REFS).map_err(to_onyx)?;
            let mut t_pages = write.open_table(T_L2_PAGES).map_err(to_onyx)?;
            let mut t_free = write.open_table(T_PAGE_FREE).map_err(to_onyx)?;

            for (l1_idx, entry) in l1_entries {
                t_l1.remove((vol_id, l1_idx)).map_err(to_onyx)?;
                let old_refs = t_refs
                    .get(entry.page_id)
                    .map_err(to_onyx)?
                    .map(|g| g.value())
                    .unwrap_or_else(|| panic!("page {} missing refs row", entry.page_id));
                if old_refs <= 1 {
                    t_refs.remove(entry.page_id).map_err(to_onyx)?;
                    t_pages.remove(entry.page_id).map_err(to_onyx)?;
                    t_free.insert(entry.page_id, ()).map_err(to_onyx)?;
                } else {
                    t_refs
                        .insert(entry.page_id, old_refs - 1)
                        .map_err(to_onyx)?;
                }
            }

            let mut t_vol = write.open_table(T_VOLUMES).map_err(to_onyx)?;
            t_vol.remove(vol_id).map_err(to_onyx)?;
        }
        write.commit().map_err(to_onyx)?;
        Ok(())
    }

    // ---- mapping CRUD -------------------------------------------------------

    pub fn get_mapping(&self, vol_id: &str, lba: Lba) -> OnyxResult<Option<BlockmapValue>> {
        let l1_idx = lba.0 / LBAS_PER_PAGE as u64;
        let l2_off = (lba.0 % LBAS_PER_PAGE as u64) as usize;

        let read = self.db.begin_read().map_err(to_onyx)?;
        let t_l1 = read.open_table(T_L1).map_err(to_onyx)?;
        let Some(l1_raw) = t_l1.get((vol_id, l1_idx)).map_err(to_onyx)? else {
            return Ok(None);
        };
        let l1_entry = L1Entry::decode(&l1_raw.value())
            .expect("L1 entry encoded by this module must roundtrip");

        let t_pages = read.open_table(T_L2_PAGES).map_err(to_onyx)?;
        let page_raw = t_pages
            .get(l1_entry.page_id)
            .map_err(to_onyx)?
            .unwrap_or_else(|| panic!("L1 references missing page {}", l1_entry.page_id));
        let page = L2Page::decode(page_raw.value())
            .unwrap_or_else(|| panic!("L2 page {} failed to decode", l1_entry.page_id));
        Ok(page.get(l2_off))
    }

    /// Insert or overwrite the mapping for `(vol_id, lba)`. Returns the previous
    /// value if one was present.
    pub fn put_mapping(
        &self,
        vol_id: &str,
        lba: Lba,
        value: BlockmapValue,
    ) -> OnyxResult<Option<BlockmapValue>> {
        let l1_idx = lba.0 / LBAS_PER_PAGE as u64;
        let l2_off = (lba.0 % LBAS_PER_PAGE as u64) as usize;
        let write = begin_hot_write(&self.db)?;
        let old = Self::update_lba_in_txn(&write, vol_id, l1_idx, l2_off, Some(value))?;
        write.commit().map_err(to_onyx)?;
        Ok(old)
    }

    /// Clear the mapping at `(vol_id, lba)`. Returns the previous value if any.
    ///
    /// If removing the last entry from an L2 page, the page is dropped and its
    /// `page_id` returned to the freelist.
    pub fn delete_mapping(
        &self,
        vol_id: &str,
        lba: Lba,
    ) -> OnyxResult<Option<BlockmapValue>> {
        let l1_idx = lba.0 / LBAS_PER_PAGE as u64;
        let l2_off = (lba.0 % LBAS_PER_PAGE as u64) as usize;
        let write = begin_hot_write(&self.db)?;
        let old = Self::update_lba_in_txn(&write, vol_id, l1_idx, l2_off, None)?;
        write.commit().map_err(to_onyx)?;
        Ok(old)
    }

    /// Batch-read mappings for multiple LBAs in a single redb read transaction.
    /// Results preserve the order of `lbas`. Caches decoded L2 pages so queries
    /// hitting the same page only pay decode cost once.
    pub fn batch_get_mappings(
        &self,
        vol_id: &str,
        lbas: &[Lba],
    ) -> OnyxResult<Vec<Option<BlockmapValue>>> {
        if lbas.is_empty() {
            return Ok(Vec::new());
        }
        let read = self.db.begin_read().map_err(to_onyx)?;
        let t_l1 = read.open_table(T_L1).map_err(to_onyx)?;
        let t_pages = read.open_table(T_L2_PAGES).map_err(to_onyx)?;

        let mut out = Vec::with_capacity(lbas.len());
        let mut page_cache: std::collections::HashMap<u64, L2Page> =
            std::collections::HashMap::new();
        for lba in lbas {
            let l1_idx = lba.0 / LBAS_PER_PAGE as u64;
            let l2_off = (lba.0 % LBAS_PER_PAGE as u64) as usize;
            let l1_guard = t_l1.get((vol_id, l1_idx)).map_err(to_onyx)?;
            let Some(l1_raw) = l1_guard else {
                out.push(None);
                continue;
            };
            let l1_entry = L1Entry::decode(&l1_raw.value())
                .expect("L1 entry encoded by this module must roundtrip");
            drop(l1_raw);

            if !page_cache.contains_key(&l1_entry.page_id) {
                let page_guard = t_pages
                    .get(l1_entry.page_id)
                    .map_err(to_onyx)?
                    .unwrap_or_else(|| panic!("L1 references missing page {}", l1_entry.page_id));
                let page = L2Page::decode(page_guard.value())
                    .unwrap_or_else(|| panic!("L2 page {} failed to decode", l1_entry.page_id));
                page_cache.insert(l1_entry.page_id, page);
            }
            out.push(page_cache.get(&l1_entry.page_id).unwrap().get(l2_off));
        }
        Ok(out)
    }

    /// Batch-write mappings from possibly multiple volumes in a single redb
    /// write transaction. Each entry fully overwrites any existing value at
    /// `(vol_id, lba)`.
    ///
    /// Entries are grouped by their L1 page before applying updates so each
    /// L2 page is decoded/encoded once per batch rather than once per LBA.
    /// This is the dominant CPU cost when many LBAs in a batch share the same
    /// compression-unit locality (the common case from the flusher).
    ///
    /// Returns the old mappings that were replaced, in the same order as the
    /// input `entries`. `None` means no prior mapping existed.
    pub fn batch_put_mappings(
        &self,
        entries: &[(&str, Lba, BlockmapValue)],
    ) -> OnyxResult<Vec<Option<BlockmapValue>>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        type GroupKey = (String, u64);
        let mut groups: std::collections::HashMap<GroupKey, Vec<(usize, usize, BlockmapValue)>> =
            std::collections::HashMap::new();
        for (idx, (vol_id, lba, value)) in entries.iter().enumerate() {
            let l1_idx = lba.0 / LBAS_PER_PAGE as u64;
            let l2_off = (lba.0 % LBAS_PER_PAGE as u64) as usize;
            groups
                .entry(((*vol_id).to_string(), l1_idx))
                .or_default()
                .push((idx, l2_off, *value));
        }

        let mut olds: Vec<Option<BlockmapValue>> = vec![None; entries.len()];
        let write = begin_hot_write(&self.db)?;
        for ((vol_id, l1_idx), updates) in &groups {
            Self::apply_page_updates_in_txn(&write, vol_id, *l1_idx, updates, &mut olds)?;
        }
        write.commit().map_err(to_onyx)?;
        Ok(olds)
    }

    /// Apply several slot updates to the L2 page that backs `(vol_id, l1_idx)`
    /// in a single decode/encode pair. Writes the resulting old values back
    /// into the caller-supplied `olds` slice, indexed by each update's
    /// original batch position.
    fn apply_page_updates_in_txn(
        write: &redb::WriteTransaction,
        vol_id: &str,
        l1_idx: u64,
        updates: &[(usize, usize, BlockmapValue)],
        olds: &mut [Option<BlockmapValue>],
    ) -> OnyxResult<()> {
        let current = {
            let t_l1 = write.open_table(T_L1).map_err(to_onyx)?;
            let guard = t_l1.get((vol_id, l1_idx)).map_err(to_onyx)?;
            guard.map(|g| {
                L1Entry::decode(&g.value())
                    .expect("L1 entry encoded by this module must roundtrip")
            })
        };

        match current {
            None => {
                let mut page = L2Page::empty();
                for &(batch_idx, l2_off, value) in updates {
                    olds[batch_idx] = None;
                    page.set(l2_off, value);
                }
                let page_id = Self::alloc_page_id(write)?;
                {
                    let mut t_pages = write.open_table(T_L2_PAGES).map_err(to_onyx)?;
                    t_pages
                        .insert(page_id, page.encode().as_slice())
                        .map_err(to_onyx)?;
                }
                {
                    let mut t_refs = write.open_table(T_PAGE_REFS).map_err(to_onyx)?;
                    t_refs.insert(page_id, 1u32).map_err(to_onyx)?;
                }
                {
                    let mut t_l1 = write.open_table(T_L1).map_err(to_onyx)?;
                    let entry = L1Entry { page_id, gen: 0 };
                    t_l1.insert((vol_id, l1_idx), entry.encode())
                        .map_err(to_onyx)?;
                }
            }
            Some(current) => {
                let mut page = {
                    let t_pages = write.open_table(T_L2_PAGES).map_err(to_onyx)?;
                    let raw = t_pages
                        .get(current.page_id)
                        .map_err(to_onyx)?
                        .unwrap_or_else(|| {
                            panic!("L1 references missing page {}", current.page_id)
                        });
                    L2Page::decode(raw.value())
                        .unwrap_or_else(|| panic!("L2 page {} failed to decode", current.page_id))
                };
                let refs = {
                    let t_refs = write.open_table(T_PAGE_REFS).map_err(to_onyx)?;
                    let guard = t_refs.get(current.page_id).map_err(to_onyx)?;
                    guard
                        .map(|g| g.value())
                        .unwrap_or_else(|| panic!("page {} missing refs row", current.page_id))
                };

                for &(batch_idx, l2_off, value) in updates {
                    olds[batch_idx] = page.get(l2_off);
                    page.set(l2_off, value);
                }

                if refs == 1 {
                    let mut t_pages = write.open_table(T_L2_PAGES).map_err(to_onyx)?;
                    t_pages
                        .insert(current.page_id, page.encode().as_slice())
                        .map_err(to_onyx)?;
                } else {
                    let new_page_id = Self::alloc_page_id(write)?;
                    {
                        let mut t_pages = write.open_table(T_L2_PAGES).map_err(to_onyx)?;
                        t_pages
                            .insert(new_page_id, page.encode().as_slice())
                            .map_err(to_onyx)?;
                    }
                    {
                        let mut t_refs = write.open_table(T_PAGE_REFS).map_err(to_onyx)?;
                        t_refs.insert(new_page_id, 1u32).map_err(to_onyx)?;
                        t_refs
                            .insert(current.page_id, refs - 1)
                            .map_err(to_onyx)?;
                    }
                    {
                        let mut t_l1 = write.open_table(T_L1).map_err(to_onyx)?;
                        let entry = L1Entry {
                            page_id: new_page_id,
                            gen: current.gen.wrapping_add(1),
                        };
                        t_l1.insert((vol_id, l1_idx), entry.encode())
                            .map_err(to_onyx)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Range scan over `[start, end)` for a single volume. Returns mappings in
    /// ascending LBA order.
    pub fn get_mappings_range(
        &self,
        vol_id: &str,
        start: Lba,
        end: Lba,
    ) -> OnyxResult<Vec<(Lba, BlockmapValue)>> {
        if start.0 >= end.0 {
            return Ok(Vec::new());
        }
        let start_l1 = start.0 / LBAS_PER_PAGE as u64;
        let end_l1 = (end.0 - 1) / LBAS_PER_PAGE as u64;

        let read = self.db.begin_read().map_err(to_onyx)?;
        let t_l1 = read.open_table(T_L1).map_err(to_onyx)?;
        let t_pages = read.open_table(T_L2_PAGES).map_err(to_onyx)?;

        let mut out = Vec::new();
        let range = t_l1
            .range((vol_id, start_l1)..=(vol_id, end_l1))
            .map_err(to_onyx)?;
        for item in range {
            let (key, val) = item.map_err(to_onyx)?;
            let (_, l1_idx) = key.value();
            let l1_entry = L1Entry::decode(&val.value())
                .expect("L1 entry encoded by this module must roundtrip");
            let page_guard = t_pages
                .get(l1_entry.page_id)
                .map_err(to_onyx)?
                .unwrap_or_else(|| panic!("L1 references missing page {}", l1_entry.page_id));
            let page = L2Page::decode(page_guard.value())
                .unwrap_or_else(|| panic!("L2 page {} failed to decode", l1_entry.page_id));
            for (slot, bv) in page.iter() {
                let lba = Lba(l1_idx * LBAS_PER_PAGE as u64 + slot as u64);
                if lba.0 >= start.0 && lba.0 < end.0 {
                    out.push((lba, bv));
                }
            }
        }
        Ok(out)
    }

    /// Enumerate every mapping for a single volume. Used by delete_volume /
    /// delete_blockmap_range to aggregate PBA decrements before dropping the
    /// volume's L1 entries.
    pub fn list_volume_mappings(
        &self,
        vol_id: &str,
    ) -> OnyxResult<Vec<(Lba, BlockmapValue)>> {
        let read = self.db.begin_read().map_err(to_onyx)?;
        let t_l1 = read.open_table(T_L1).map_err(to_onyx)?;
        let t_pages = read.open_table(T_L2_PAGES).map_err(to_onyx)?;

        let start = (vol_id, 0u64);
        let end = (vol_id, u64::MAX);
        let mut out = Vec::new();
        let range = t_l1.range(start..=end).map_err(to_onyx)?;
        for item in range {
            let (key, val) = item.map_err(to_onyx)?;
            let (_, l1_idx) = key.value();
            let l1_entry = L1Entry::decode(&val.value())
                .expect("L1 entry encoded by this module must roundtrip");
            let page_guard = t_pages
                .get(l1_entry.page_id)
                .map_err(to_onyx)?
                .unwrap_or_else(|| panic!("L1 references missing page {}", l1_entry.page_id));
            let page = L2Page::decode(page_guard.value())
                .unwrap_or_else(|| panic!("L2 page {} failed to decode", l1_entry.page_id));
            for (slot, bv) in page.iter() {
                let lba = Lba(l1_idx * LBAS_PER_PAGE as u64 + slot as u64);
                out.push((lba, bv));
            }
        }
        Ok(out)
    }

    /// Delete all mappings in `[start, end)` for a volume. Caller is expected
    /// to have enumerated the entries first via `list_volume_mappings` or
    /// `get_mappings_range` for refcount accounting.
    pub fn delete_mappings_range(
        &self,
        vol_id: &str,
        start: Lba,
        end: Lba,
    ) -> OnyxResult<()> {
        if start.0 >= end.0 {
            return Ok(());
        }
        let start_l1 = start.0 / LBAS_PER_PAGE as u64;
        let end_l1 = (end.0 - 1) / LBAS_PER_PAGE as u64;

        let write = begin_hot_write(&self.db)?;
        for l1_idx in start_l1..=end_l1 {
            let page_start = l1_idx * LBAS_PER_PAGE as u64;
            let page_end = page_start + LBAS_PER_PAGE as u64;
            let lo = start.0.max(page_start);
            let hi = end.0.min(page_end);
            for lba_val in lo..hi {
                let l2_off = (lba_val % LBAS_PER_PAGE as u64) as usize;
                Self::update_lba_in_txn(&write, vol_id, l1_idx, l2_off, None)?;
            }
        }
        write.commit().map_err(to_onyx)?;
        Ok(())
    }

    /// Iterate all live (vol_id, lba, BlockmapValue) triples across every
    /// volume. Used by `scan_all_blockmap_entries` in GC / dedup rescan.
    pub fn scan_all_mappings<F>(&self, mut callback: F) -> OnyxResult<()>
    where
        F: FnMut(&str, Lba, BlockmapValue),
    {
        let read = self.db.begin_read().map_err(to_onyx)?;
        let t_l1 = read.open_table(T_L1).map_err(to_onyx)?;
        let t_pages = read.open_table(T_L2_PAGES).map_err(to_onyx)?;

        let iter = t_l1.iter().map_err(to_onyx)?;
        for item in iter {
            let (key, val) = item.map_err(to_onyx)?;
            let (vol_id, l1_idx) = key.value();
            let l1_entry = L1Entry::decode(&val.value())
                .expect("L1 entry encoded by this module must roundtrip");
            let page_guard = t_pages
                .get(l1_entry.page_id)
                .map_err(to_onyx)?
                .unwrap_or_else(|| panic!("L1 references missing page {}", l1_entry.page_id));
            let page = L2Page::decode(page_guard.value())
                .unwrap_or_else(|| panic!("L2 page {} failed to decode", l1_entry.page_id));
            for (slot, bv) in page.iter() {
                let lba = Lba(l1_idx * LBAS_PER_PAGE as u64 + slot as u64);
                callback(vol_id, lba, bv);
            }
        }
        Ok(())
    }

    // ---- internal helpers ---------------------------------------------------

    /// Core page-COW logic. Shared by put / delete / future batch APIs.
    ///
    /// - `Some(v)` writes `v` at `l2_off`
    /// - `None` clears `l2_off`
    /// - If the resulting page is empty, the page_id is freed.
    /// - If the L2 page is shared (refcount > 1), copy-on-write: allocate a new
    ///   page_id, copy+mutate, and rewrite the L1 entry. Old page refcount
    ///   decrements.
    fn update_lba_in_txn(
        write: &redb::WriteTransaction,
        vol_id: &str,
        l1_idx: u64,
        l2_off: usize,
        new_value: Option<BlockmapValue>,
    ) -> OnyxResult<Option<BlockmapValue>> {
        // Step 1: resolve current L1 entry.
        let current = {
            let t_l1 = write.open_table(T_L1).map_err(to_onyx)?;
            let guard = t_l1.get((vol_id, l1_idx)).map_err(to_onyx)?;
            guard.map(|g| {
                L1Entry::decode(&g.value())
                    .expect("L1 entry encoded by this module must roundtrip")
            })
        };

        let current = match current {
            None => {
                // No page yet. Delete is a no-op; insert allocates a fresh page.
                let Some(value) = new_value else {
                    return Ok(None);
                };
                let mut page = L2Page::empty();
                page.set(l2_off, value);
                let page_id = Self::alloc_page_id(write)?;
                {
                    let mut t_pages = write.open_table(T_L2_PAGES).map_err(to_onyx)?;
                    t_pages
                        .insert(page_id, page.encode().as_slice())
                        .map_err(to_onyx)?;
                    let mut t_refs = write.open_table(T_PAGE_REFS).map_err(to_onyx)?;
                    t_refs.insert(page_id, 1u32).map_err(to_onyx)?;
                    let mut t_l1 = write.open_table(T_L1).map_err(to_onyx)?;
                    let entry = L1Entry { page_id, gen: 0 };
                    t_l1.insert((vol_id, l1_idx), entry.encode())
                        .map_err(to_onyx)?;
                }
                return Ok(None);
            }
            Some(entry) => entry,
        };

        // Step 2: load the page + its refcount.
        let mut page = {
            let t_pages = write.open_table(T_L2_PAGES).map_err(to_onyx)?;
            let raw = t_pages
                .get(current.page_id)
                .map_err(to_onyx)?
                .unwrap_or_else(|| panic!("L1 references missing page {}", current.page_id));
            L2Page::decode(raw.value())
                .unwrap_or_else(|| panic!("L2 page {} failed to decode", current.page_id))
        };
        let refs = {
            let t_refs = write.open_table(T_PAGE_REFS).map_err(to_onyx)?;
            let guard = t_refs.get(current.page_id).map_err(to_onyx)?;
            let refs = guard
                .map(|g| g.value())
                .unwrap_or_else(|| panic!("page {} missing refs row", current.page_id));
            refs
        };

        let old_value = page.get(l2_off);
        match new_value {
            Some(v) => page.set(l2_off, v),
            None => page.clear(l2_off),
        }

        if page.popcount() == 0 {
            // Page became empty — drop it and clear L1.
            if refs <= 1 {
                let mut t_pages = write.open_table(T_L2_PAGES).map_err(to_onyx)?;
                t_pages.remove(current.page_id).map_err(to_onyx)?;
                let mut t_refs = write.open_table(T_PAGE_REFS).map_err(to_onyx)?;
                t_refs.remove(current.page_id).map_err(to_onyx)?;
                let mut t_free = write.open_table(T_PAGE_FREE).map_err(to_onyx)?;
                t_free.insert(current.page_id, ()).map_err(to_onyx)?;
            } else {
                // Shared page whose view just became empty from our perspective.
                // Decrement refcount; other holders keep their copy.
                let mut t_refs = write.open_table(T_PAGE_REFS).map_err(to_onyx)?;
                t_refs
                    .insert(current.page_id, refs - 1)
                    .map_err(to_onyx)?;
            }
            let mut t_l1 = write.open_table(T_L1).map_err(to_onyx)?;
            t_l1.remove((vol_id, l1_idx)).map_err(to_onyx)?;
            return Ok(old_value);
        }

        if refs == 1 {
            // Exclusive — in-place update.
            let mut t_pages = write.open_table(T_L2_PAGES).map_err(to_onyx)?;
            t_pages
                .insert(current.page_id, page.encode().as_slice())
                .map_err(to_onyx)?;
        } else {
            // COW: allocate new page, rewrite L1, decrement old.
            let new_page_id = Self::alloc_page_id(write)?;
            {
                let mut t_pages = write.open_table(T_L2_PAGES).map_err(to_onyx)?;
                t_pages
                    .insert(new_page_id, page.encode().as_slice())
                    .map_err(to_onyx)?;
                let mut t_refs = write.open_table(T_PAGE_REFS).map_err(to_onyx)?;
                t_refs.insert(new_page_id, 1u32).map_err(to_onyx)?;
                t_refs
                    .insert(current.page_id, refs - 1)
                    .map_err(to_onyx)?;
            }
            let mut t_l1 = write.open_table(T_L1).map_err(to_onyx)?;
            let entry = L1Entry {
                page_id: new_page_id,
                gen: current.gen.wrapping_add(1),
            };
            t_l1.insert((vol_id, l1_idx), entry.encode())
                .map_err(to_onyx)?;
        }
        Ok(old_value)
    }

    /// Allocate a page id: pop from freelist if available, else bump counter.
    fn alloc_page_id(write: &redb::WriteTransaction) -> OnyxResult<u64> {
        // Try freelist first (pop lowest-numbered entry for determinism).
        let from_free = {
            let t_free = write.open_table(T_PAGE_FREE).map_err(to_onyx)?;
            let mut iter = t_free.iter().map_err(to_onyx)?;
            let first = iter.next().transpose().map_err(to_onyx)?;
            first.map(|(k, _)| k.value())
        };
        if let Some(page_id) = from_free {
            let mut t_free = write.open_table(T_PAGE_FREE).map_err(to_onyx)?;
            t_free.remove(page_id).map_err(to_onyx)?;
            return Ok(page_id);
        }

        // Fresh id from the monotonic counter.
        let mut t_next = write.open_table(T_PAGE_NEXT_ID).map_err(to_onyx)?;
        let next = t_next
            .get(PAGE_NEXT_ID_KEY)
            .map_err(to_onyx)?
            .map(|g| g.value())
            .unwrap_or(0);
        // page_id 0 is reserved as a sentinel for "no page" in future code paths.
        let page_id = next.max(1);
        t_next
            .insert(PAGE_NEXT_ID_KEY, page_id + 1)
            .map_err(to_onyx)?;
        Ok(page_id)
    }

    // ---- test-only hooks ----------------------------------------------------

    /// Bump the refcount of the L2 page holding `lba` so the next `put_mapping`
    /// exercises the COW branch. Used only to reach COW coverage in v1 where
    /// snapshots do not exist yet.
    #[cfg(test)]
    pub fn _test_bump_page_refcount(&self, vol_id: &str, lba: Lba) -> OnyxResult<u64> {
        let l1_idx = lba.0 / LBAS_PER_PAGE as u64;
        let write = begin_hot_write(&self.db)?;
        let page_id = {
            let t_l1 = write.open_table(T_L1).map_err(to_onyx)?;
            let entry_raw = t_l1
                .get((vol_id, l1_idx))
                .map_err(to_onyx)?
                .ok_or_else(|| OnyxError::Config(format!("no L1 entry for lba {}", lba.0)))?;
            L1Entry::decode(&entry_raw.value())
                .expect("L1 entry encoded by this module must roundtrip")
                .page_id
        };
        {
            let mut t_refs = write.open_table(T_PAGE_REFS).map_err(to_onyx)?;
            let current = t_refs
                .get(page_id)
                .map_err(to_onyx)?
                .map(|g| g.value())
                .unwrap_or_else(|| panic!("page {} missing refs row", page_id));
            t_refs.insert(page_id, current + 1).map_err(to_onyx)?;
        }
        write.commit().map_err(to_onyx)?;
        Ok(page_id)
    }
}

fn to_onyx<E: std::fmt::Display>(e: E) -> OnyxError {
    OnyxError::Redb(e.to_string())
}
