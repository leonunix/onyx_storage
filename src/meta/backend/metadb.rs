use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use onyx_metadb::{ApplyOutcome, Config as MetaDbConfig, Db, DedupValue, L2pValue, VolumeOrdinal};
use serde::{Deserialize, Serialize};

use crate::config::MetaConfig;
use crate::error::{OnyxError, OnyxResult};
use crate::meta::schema::{BlockmapValue, ContentHash, DedupEntry, FLAG_DEDUP_SKIPPED};
use crate::meta::store::DedupHitResult;
use crate::types::{Lba, Pba, VolumeConfig, VolumeId};

use super::codec::{
    blockmap_from_l2p_bytes, blockmap_to_l2p_bytes, dedup_from_value_bytes, dedup_to_value_bytes,
    freed_blocks_for_l2p_value, DEDUP_VALUE_BYTES,
};

const METADB_DEDUP_VALUE_BYTES: usize = 28;
const CATALOG_VERSION: u32 = 1;
const CATALOG_FILE: &str = "onyx-volume-catalog.bin";
const METADB_PAGE_FILE: &str = "pages.onyx_meta";

pub(crate) struct MetadbBackend {
    db: Db,
    catalog: Mutex<VolumeCatalog>,
    catalog_path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct VolumeCatalogFile {
    version: u32,
    volumes: Vec<VolumeCatalogEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct VolumeCatalogEntry {
    ordinal: VolumeOrdinal,
    config: VolumeConfig,
}

#[derive(Clone, Debug, Default)]
struct VolumeCatalog {
    by_id: HashMap<String, VolumeCatalogEntry>,
}

impl MetadbBackend {
    pub(crate) fn open(config: &MetaConfig) -> OnyxResult<Self> {
        let path = config.path().ok_or_else(|| {
            OnyxError::Config("meta.path is required to open metadb backend".into())
        })?;
        fs::create_dir_all(path)?;

        let db_config = metadb_config_from_onyx(path, config);
        let db = if path.join(METADB_PAGE_FILE).exists() {
            Db::open_with_config(db_config)?
        } else {
            Db::create_with_config(db_config)?
        };

        let catalog_path = path.join(CATALOG_FILE);
        let catalog = VolumeCatalog::load(&catalog_path)?;
        catalog.validate_against_db(&db)?;

        Ok(Self {
            db,
            catalog: Mutex::new(catalog),
            catalog_path,
        })
    }

    pub(crate) fn put_volume(&self, config: &VolumeConfig) -> OnyxResult<()> {
        let mut catalog = self.catalog.lock().unwrap();
        if let Some(entry) = catalog.by_id.get_mut(&config.id.0) {
            entry.config = config.clone();
            catalog.persist(&self.catalog_path)?;
            return Ok(());
        }

        let ordinal = self.db.create_volume()?;
        catalog.by_id.insert(
            config.id.0.clone(),
            VolumeCatalogEntry {
                ordinal,
                config: config.clone(),
            },
        );
        catalog.persist(&self.catalog_path)?;
        Ok(())
    }

    pub(crate) fn get_volume(&self, id: &VolumeId) -> OnyxResult<Option<VolumeConfig>> {
        let catalog = self.catalog.lock().unwrap();
        Ok(catalog.by_id.get(&id.0).map(|entry| entry.config.clone()))
    }

    pub(crate) fn list_volumes(&self) -> OnyxResult<Vec<VolumeConfig>> {
        let catalog = self.catalog.lock().unwrap();
        let mut volumes: Vec<VolumeConfig> = catalog
            .by_id
            .values()
            .map(|entry| entry.config.clone())
            .collect();
        volumes.sort_by(|a, b| a.id.0.cmp(&b.id.0));
        Ok(volumes)
    }

    pub(crate) fn volume_ordinal(&self, id: &VolumeId) -> OnyxResult<VolumeOrdinal> {
        let catalog = self.catalog.lock().unwrap();
        catalog
            .by_id
            .get(&id.0)
            .map(|entry| entry.ordinal)
            .ok_or_else(|| OnyxError::VolumeNotFound(id.0.clone()))
    }

    fn volume_ordinal_optional(&self, id: &VolumeId) -> Option<VolumeOrdinal> {
        let catalog = self.catalog.lock().unwrap();
        catalog.by_id.get(&id.0).map(|entry| entry.ordinal)
    }

    pub(crate) fn delete_volume(&self, id: &VolumeId) -> OnyxResult<Vec<(Pba, u32)>> {
        let (ordinal, config) = {
            let catalog = self.catalog.lock().unwrap();
            let Some(entry) = catalog.by_id.get(&id.0) else {
                return Ok(Vec::new());
            };
            (entry.ordinal, entry.config.clone())
        };

        let end = Lba(config.size_bytes / u64::from(config.block_size));
        let freed = self.delete_blockmap_range(id, Lba(0), end)?;
        self.db.drop_volume(ordinal)?;

        let mut catalog = self.catalog.lock().unwrap();
        catalog.by_id.remove(&id.0);
        catalog.persist(&self.catalog_path)?;

        Ok(freed)
    }

    pub(crate) fn get_mapping(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
    ) -> OnyxResult<Option<BlockmapValue>> {
        let Some(ord) = self.volume_ordinal_optional(vol_id) else {
            return Ok(None);
        };
        self.db.get(ord, lba.0)?.map(decode_l2p_value).transpose()
    }

    pub(crate) fn put_mapping(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        value: &BlockmapValue,
    ) -> OnyxResult<()> {
        let ord = self.volume_ordinal(vol_id)?;
        self.db.insert(ord, lba.0, to_l2p_value(value))?;
        Ok(())
    }

    pub(crate) fn delete_mapping(&self, vol_id: &VolumeId, lba: Lba) -> OnyxResult<()> {
        let ord = self.volume_ordinal(vol_id)?;
        self.db.delete(ord, lba.0)?;
        Ok(())
    }

    pub(crate) fn multi_get_mappings(
        &self,
        vol_id: &VolumeId,
        lbas: &[Lba],
    ) -> OnyxResult<Vec<Option<BlockmapValue>>> {
        if lbas.is_empty() {
            return Ok(Vec::new());
        }
        let Some(ord) = self.volume_ordinal_optional(vol_id) else {
            return Ok(vec![None; lbas.len()]);
        };
        let raw_lbas: Vec<onyx_metadb::Lba> = lbas.iter().map(|lba| lba.0).collect();
        self.db
            .multi_get(ord, &raw_lbas)?
            .into_iter()
            .map(|value| value.map(decode_l2p_value).transpose())
            .collect()
    }

    pub(crate) fn get_mappings_range(
        &self,
        vol_id: &VolumeId,
        start: Lba,
        end: Lba,
    ) -> OnyxResult<Vec<(Lba, BlockmapValue)>> {
        let Some(ord) = self.volume_ordinal_optional(vol_id) else {
            return Ok(Vec::new());
        };
        self.db
            .range(ord, start.0..end.0)?
            .map(|item| {
                let (lba, value) = item?;
                Ok((Lba(lba), decode_l2p_value(value)?))
            })
            .collect()
    }

    pub(crate) fn delete_blockmap_range(
        &self,
        vol_id: &VolumeId,
        start: Lba,
        end: Lba,
    ) -> OnyxResult<Vec<(Pba, u32)>> {
        if start >= end {
            return Ok(Vec::new());
        }

        let Some(ord) = self.volume_ordinal_optional(vol_id) else {
            return Ok(Vec::new());
        };
        let mut pba_meta: HashMap<Pba, u32> = HashMap::new();
        for item in self.db.range(ord, start.0..end.0)? {
            let (_, value) = item?;
            let value = decode_l2p_value(value)?;
            let blocks = freed_blocks_for_l2p_value(&value);
            pba_meta
                .entry(value.pba)
                .and_modify(|existing| *existing = (*existing).max(blocks))
                .or_insert(blocks);
        }
        if pba_meta.is_empty() {
            return Ok(Vec::new());
        }

        self.db.range_delete(ord, start.0, end.0)?;

        let mut freed = Vec::new();
        let pbas: Vec<Pba> = pba_meta.keys().copied().collect();
        let refcounts = self.multi_get_refcounts(&pbas)?;
        for (pba, refcount) in pbas.into_iter().zip(refcounts.into_iter()) {
            if refcount == 0 {
                let blocks = pba_meta.get(&pba).copied().unwrap_or(1);
                freed.push((pba, blocks));
            }
        }
        freed.sort_unstable_by_key(|(pba, _)| *pba);
        let freed_pbas: Vec<Pba> = freed.iter().map(|(pba, _)| *pba).collect();
        self.cleanup_dedup_for_pbas_batch(&freed_pbas)?;
        Ok(freed)
    }

    pub(crate) fn get_refcount(&self, pba: Pba) -> OnyxResult<u32> {
        Ok(self.db.get_refcount(to_metadb_pba(pba))?)
    }

    pub(crate) fn multi_get_refcounts(&self, pbas: &[Pba]) -> OnyxResult<Vec<u32>> {
        let pbas: Vec<onyx_metadb::Pba> = pbas.iter().map(|pba| to_metadb_pba(*pba)).collect();
        Ok(self.db.multi_get_refcount(&pbas)?)
    }

    pub(crate) fn set_refcount(&self, pba: Pba, count: u32) -> OnyxResult<()> {
        let current = self.get_refcount(pba)?;
        if count > current {
            self.db.incref_pba(to_metadb_pba(pba), count - current)?;
        } else if current > count {
            self.db.decref_pba(to_metadb_pba(pba), current - count)?;
        }
        Ok(())
    }

    pub(crate) fn increment_refcount(&self, pba: Pba) -> OnyxResult<u32> {
        Ok(self.db.incref_pba(to_metadb_pba(pba), 1)?)
    }

    pub(crate) fn decrement_refcount(&self, pba: Pba) -> OnyxResult<u32> {
        Ok(self.db.decref_pba(to_metadb_pba(pba), 1)?)
    }

    pub(crate) fn atomic_write_mapping(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        value: &BlockmapValue,
    ) -> OnyxResult<()> {
        self.atomic_batch_write(vol_id, &[(lba, *value)], 1)?;
        Ok(())
    }

    pub(crate) fn atomic_remap(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        _old_pba: Option<Pba>,
        new_value: &BlockmapValue,
    ) -> OnyxResult<()> {
        self.atomic_batch_write(vol_id, &[(lba, *new_value)], 1)?;
        Ok(())
    }

    pub(crate) fn get_dedup(&self, hash: &ContentHash) -> OnyxResult<Option<DedupEntry>> {
        self.db.get_dedup(hash)?.map(decode_dedup_value).transpose()
    }

    pub(crate) fn multi_get_dedup(
        &self,
        hashes: &[ContentHash],
    ) -> OnyxResult<Vec<Option<DedupEntry>>> {
        self.db
            .multi_get_dedup(hashes)?
            .into_iter()
            .map(|value| value.map(decode_dedup_value).transpose())
            .collect()
    }

    pub(crate) fn atomic_batch_write(
        &self,
        vol_id: &VolumeId,
        batch_values: &[(Lba, BlockmapValue)],
        new_refcount: u32,
    ) -> OnyxResult<HashMap<Pba, (u32, u32)>> {
        if batch_values.is_empty() {
            return Ok(HashMap::new());
        }
        let ord = self.volume_ordinal(vol_id)?;
        let mut tx = self.db.begin();
        for (lba, value) in batch_values {
            tx.l2p_remap(ord, lba.0, to_l2p_value(value), None);
        }
        for (pba, delta) in
            extra_new_refcount_increfs(batch_values.iter().map(|(_, value)| *value), new_refcount)
        {
            tx.incref_pba(to_metadb_pba(pba), delta);
        }
        let (_, outcomes) = tx.commit_with_outcomes()?;
        newly_zeroed_from_remaps(
            batch_values.iter().map(|(_, value)| *value),
            outcomes.into_iter().take(batch_values.len()).collect(),
        )
    }

    pub(crate) fn atomic_batch_write_packed(
        &self,
        batch_values: &[(VolumeId, Lba, BlockmapValue)],
        _new_pba: Pba,
        _new_refcount: u32,
    ) -> OnyxResult<HashMap<Pba, (u32, u32)>> {
        if batch_values.is_empty() {
            return Ok(HashMap::new());
        }
        let mut tx = self.db.begin();
        let mut new_values = Vec::with_capacity(batch_values.len());
        for (vol_id, lba, value) in batch_values {
            let ord = self.volume_ordinal(vol_id)?;
            tx.l2p_remap(ord, lba.0, to_l2p_value(value), None);
            new_values.push(*value);
        }
        let (_, outcomes) = tx.commit_with_outcomes()?;
        newly_zeroed_from_remaps(new_values, outcomes)
    }

    pub(crate) fn atomic_batch_write_multi(
        &self,
        units: &[(&VolumeId, &[(Lba, BlockmapValue)], u32)],
    ) -> OnyxResult<HashMap<Pba, (u32, u32)>> {
        if units.is_empty() {
            return Ok(HashMap::new());
        }
        let mut tx = self.db.begin();
        let mut new_values = Vec::new();
        let mut extra_increfs: HashMap<Pba, u32> = HashMap::new();
        for (vol_id, batch_values, new_refcount) in units {
            let ord = self.volume_ordinal(vol_id)?;
            for (lba, value) in *batch_values {
                tx.l2p_remap(ord, lba.0, to_l2p_value(value), None);
                new_values.push(*value);
            }
            for (pba, delta) in extra_new_refcount_increfs(
                batch_values.iter().map(|(_, value)| *value),
                *new_refcount,
            ) {
                *extra_increfs.entry(pba).or_insert(0) += delta;
            }
        }
        for (pba, delta) in extra_increfs {
            tx.incref_pba(to_metadb_pba(pba), delta);
        }
        let (_, outcomes) = tx.commit_with_outcomes()?;
        let remap_count = new_values.len();
        newly_zeroed_from_remaps(new_values, outcomes.into_iter().take(remap_count).collect())
    }

    pub(crate) fn put_dedup_entries(
        &self,
        entries: &[(ContentHash, DedupEntry)],
    ) -> OnyxResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut tx = self.db.begin();
        for (hash, entry) in entries {
            tx.put_dedup(*hash, to_dedup_value(entry));
            tx.register_dedup_reverse(to_metadb_pba(entry.pba), *hash);
        }
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn delete_dedup_index(&self, hash: &ContentHash) -> OnyxResult<()> {
        let mut tx = self.db.begin();
        if let Some(entry) = self.db.get_dedup(hash)? {
            tx.unregister_dedup_reverse(entry.head_pba(), *hash);
        }
        tx.delete_dedup(*hash);
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn dedup_entry_is_live(
        &self,
        hash: &ContentHash,
        entry: &DedupEntry,
    ) -> OnyxResult<bool> {
        if self.get_refcount(entry.pba)? == 0 {
            return Ok(false);
        }
        let hashes = self
            .db
            .scan_dedup_reverse_for_pba(to_metadb_pba(entry.pba))?;
        Ok(hashes.iter().any(|candidate| candidate == hash))
    }

    pub(crate) fn cleanup_dedup_for_pbas_batch(&self, pbas: &[Pba]) -> OnyxResult<()> {
        let pbas: Vec<onyx_metadb::Pba> = pbas.iter().map(|pba| to_metadb_pba(*pba)).collect();
        self.db.cleanup_dedup_for_dead_pbas(&pbas)?;
        Ok(())
    }

    pub(crate) fn scan_all_blockmap_entries(
        &self,
    ) -> OnyxResult<Vec<(VolumeId, Lba, BlockmapValue)>> {
        let volumes = self.list_volumes()?;
        let mut entries = Vec::new();
        for volume in volumes {
            let ord = self.volume_ordinal(&volume.id)?;
            for item in self.db.range(ord, 0..u64::MAX)? {
                let (lba, value) = item?;
                entries.push((volume.id.clone(), Lba(lba), decode_l2p_value(value)?));
            }
        }
        Ok(entries)
    }

    pub(crate) fn count_blockmap_refs_for_pba(&self, target: Pba) -> OnyxResult<u32> {
        let count = self
            .scan_all_blockmap_entries()?
            .into_iter()
            .filter(|(_, _, value)| value.pba == target)
            .count();
        Ok(count as u32)
    }

    pub(crate) fn has_any_blockmap_ref(&self, target: Pba) -> OnyxResult<bool> {
        Ok(self
            .scan_all_blockmap_entries()?
            .into_iter()
            .any(|(_, _, value)| value.pba == target))
    }

    pub(crate) fn iter_refcounts(&self) -> OnyxResult<Vec<(Pba, u32)>> {
        self.db
            .iter_refcounts()?
            .map(|item| {
                let (pba, rc) = item?;
                Ok((from_metadb_pba(pba), rc))
            })
            .collect()
    }

    pub(crate) fn iter_dedup_entries(&self) -> OnyxResult<Vec<(ContentHash, DedupEntry)>> {
        self.db
            .iter_dedup()?
            .map(|item| {
                let (hash, value) = item?;
                Ok((hash, decode_dedup_value(value)?))
            })
            .collect()
    }

    pub(crate) fn iter_dedup_reverse_entries(&self) -> OnyxResult<Vec<(Pba, ContentHash)>> {
        let mut entries = Vec::new();
        for (hash, entry) in self.iter_dedup_entries()? {
            entries.push((entry.pba, hash));
        }
        Ok(entries)
    }

    pub(crate) fn iter_allocated_blocks(&self) -> OnyxResult<Vec<(Pba, u32)>> {
        let mut blocks: Vec<(Pba, u32)> = self
            .scan_all_blockmap_entries()?
            .into_iter()
            .map(|(_, _, value)| (value.pba, freed_blocks_for_l2p_value(&value)))
            .collect();
        blocks.sort_unstable_by_key(|(pba, _)| *pba);
        blocks.dedup_by_key(|(pba, _)| *pba);
        Ok(blocks)
    }

    pub(crate) fn cleanup_orphaned_refcounts(&self) -> OnyxResult<Vec<(Pba, u32)>> {
        let refs = self.iter_refcounts()?;
        let mut orphaned = Vec::new();
        for (pba, rc) in refs {
            if rc > 0 && !self.has_any_blockmap_ref(pba)? {
                self.set_refcount(pba, 0)?;
                orphaned.push((pba, rc));
            }
        }
        Ok(orphaned)
    }

    pub(crate) fn rebuild_refcount_from_blockmap(&self) -> OnyxResult<()> {
        Err(OnyxError::Config(
            "refcount rebuild is not supported by metadb backend; WAL recovery owns refcount"
                .into(),
        ))
    }

    pub(crate) fn sync_durable(&self) -> OnyxResult<()> {
        self.db.flush()?;
        Ok(())
    }

    pub(crate) fn scan_dedup_skipped(
        &self,
        limit: usize,
    ) -> OnyxResult<Vec<(String, Lba, BlockmapValue)>> {
        let volumes = self.list_volumes()?;
        let mut results = Vec::new();
        for volume in volumes {
            let ord = self.volume_ordinal(&volume.id)?;
            let lba_count = volume.size_bytes / u64::from(volume.block_size);
            for item in self.db.range(ord, 0..lba_count)? {
                let (lba, value) = item?;
                let value = decode_l2p_value(value)?;
                if value.flags & FLAG_DEDUP_SKIPPED == 0 {
                    continue;
                }
                results.push((volume.id.0.clone(), Lba(lba), value));
                if results.len() >= limit {
                    return Ok(results);
                }
            }
        }
        Ok(results)
    }

    pub(crate) fn update_blockmap_flags(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        new_flags: u8,
    ) -> OnyxResult<()> {
        let Some(mut value) = self.get_mapping(vol_id, lba)? else {
            return Ok(());
        };
        if value.flags == new_flags {
            return Ok(());
        }
        value.flags = new_flags;
        let ord = self.volume_ordinal(vol_id)?;
        let mut tx = self.db.begin();
        tx.l2p_remap(ord, lba.0, to_l2p_value(&value), None);
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn atomic_dedup_hit(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        new_value: &BlockmapValue,
        _hash: &ContentHash,
    ) -> OnyxResult<Option<(Pba, u32)>> {
        let ord = self.volume_ordinal(vol_id)?;
        let mut tx = self.db.begin();
        tx.l2p_remap(
            ord,
            lba.0,
            to_l2p_value(new_value),
            Some((to_metadb_pba(new_value.pba), 1)),
        );
        let (_, outcomes) = tx.commit_with_outcomes()?;
        let newly_zeroed = newly_zeroed_from_remaps([*new_value], outcomes)?;
        Ok(newly_zeroed
            .into_iter()
            .next()
            .map(|(pba, (_, blocks))| (pba, blocks)))
    }

    pub(crate) fn atomic_batch_dedup_hits(
        &self,
        vol_id: &VolumeId,
        hits: &[(Lba, BlockmapValue, ContentHash)],
    ) -> OnyxResult<(Vec<DedupHitResult>, HashMap<Pba, u32>)> {
        if hits.is_empty() {
            return Ok((Vec::new(), HashMap::new()));
        }
        let ord = self.volume_ordinal(vol_id)?;
        let mut tx = self.db.begin();
        for (lba, value, _) in hits {
            tx.l2p_remap(
                ord,
                lba.0,
                to_l2p_value(value),
                Some((to_metadb_pba(value.pba), 1)),
            );
        }
        let (_, outcomes) = tx.commit_with_outcomes()?;
        dedup_hit_results_from_remaps(hits, outcomes)
    }
}

impl VolumeCatalog {
    fn load(path: &Path) -> OnyxResult<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }

        let bytes = fs::read(path)?;
        let file: VolumeCatalogFile =
            bincode::deserialize(&bytes).map_err(|e| OnyxError::Config(e.to_string()))?;
        if file.version != CATALOG_VERSION {
            return Err(OnyxError::Config(format!(
                "unsupported metadb volume catalog version {}, expected {}",
                file.version, CATALOG_VERSION
            )));
        }

        let mut by_id = HashMap::with_capacity(file.volumes.len());
        for entry in file.volumes {
            let id = entry.config.id.0.clone();
            if by_id.insert(id.clone(), entry).is_some() {
                return Err(OnyxError::Config(format!(
                    "duplicate volume id '{id}' in metadb volume catalog"
                )));
            }
        }
        Ok(Self { by_id })
    }

    fn persist(&self, path: &Path) -> OnyxResult<()> {
        let mut volumes: Vec<VolumeCatalogEntry> = self.by_id.values().cloned().collect();
        volumes.sort_by_key(|entry| entry.ordinal);
        let file = VolumeCatalogFile {
            version: CATALOG_VERSION,
            volumes,
        };
        let bytes = bincode::serialize(&file).map_err(|e| OnyxError::Config(e.to_string()))?;
        atomic_write(path, &bytes)
    }

    fn validate_against_db(&self, db: &Db) -> OnyxResult<()> {
        let live_ordinals: std::collections::HashSet<VolumeOrdinal> =
            db.volumes().into_iter().collect();
        for entry in self.by_id.values() {
            if !live_ordinals.contains(&entry.ordinal) {
                return Err(OnyxError::Config(format!(
                    "volume '{}' maps to missing metadb ordinal {}",
                    entry.config.id, entry.ordinal
                )));
            }
        }
        Ok(())
    }
}

fn metadb_config_from_onyx(path: &Path, config: &MetaConfig) -> MetaDbConfig {
    let mut cfg = MetaDbConfig::new(path);
    cfg.page_cache_bytes = config.block_cache_bytes() as u64;
    cfg.lsm_memtable_bytes = config.memtable_budget_bytes() as u64;
    cfg
}

fn atomic_write(path: &Path, bytes: &[u8]) -> OnyxResult<()> {
    let parent = path.parent().ok_or_else(|| {
        OnyxError::Config(format!(
            "cannot persist metadb catalog at path without parent: {}",
            path.display()
        ))
    })?;
    fs::create_dir_all(parent)?;

    let tmp = path.with_extension("tmp");
    {
        let mut file = File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn decode_l2p_value(value: L2pValue) -> OnyxResult<BlockmapValue> {
    from_l2p_value(value)
        .ok_or_else(|| OnyxError::Config("metadb L2P value has invalid Onyx layout".into()))
}

fn decode_dedup_value(value: DedupValue) -> OnyxResult<DedupEntry> {
    from_dedup_value(value)
        .ok_or_else(|| OnyxError::Config("metadb dedup value has invalid Onyx layout".into()))
}

fn newly_zeroed_from_remaps<I>(
    new_values: I,
    outcomes: Vec<ApplyOutcome>,
) -> OnyxResult<HashMap<Pba, (u32, u32)>>
where
    I: IntoIterator<Item = BlockmapValue>,
{
    let new_values: Vec<BlockmapValue> = new_values.into_iter().collect();
    if new_values.len() != outcomes.len() {
        return Err(OnyxError::Config(format!(
            "metadb outcome length mismatch: {} new values, {} outcomes",
            new_values.len(),
            outcomes.len()
        )));
    }

    let mut decrements: HashMap<Pba, (u32, u32)> = HashMap::new();
    let mut freed = std::collections::HashSet::new();

    for (new_value, outcome) in new_values.into_iter().zip(outcomes.into_iter()) {
        let ApplyOutcome::L2pRemap {
            applied,
            prev,
            freed_pba,
        } = outcome
        else {
            return Err(OnyxError::Config(
                "metadb returned non-remap outcome for remap batch".into(),
            ));
        };

        if let Some(pba) = freed_pba {
            freed.insert(from_metadb_pba(pba));
        }

        if !applied {
            continue;
        }
        let Some(prev) = prev else {
            continue;
        };
        let old = decode_l2p_value(prev)?;
        if old.pba == new_value.pba {
            continue;
        }
        let blocks = freed_blocks_for_l2p_value(&old);
        let entry = decrements.entry(old.pba).or_insert((0, blocks));
        entry.0 += 1;
        entry.1 = entry.1.max(blocks);
    }

    decrements.retain(|pba, _| freed.contains(pba));
    Ok(decrements)
}

fn extra_new_refcount_increfs<I>(new_values: I, new_refcount: u32) -> HashMap<Pba, u32>
where
    I: IntoIterator<Item = BlockmapValue>,
{
    let mut occurrences: HashMap<Pba, u32> = HashMap::new();
    for value in new_values {
        *occurrences.entry(value.pba).or_insert(0) += 1;
    }
    if occurrences.len() != 1 {
        return HashMap::new();
    }
    occurrences
        .into_iter()
        .filter_map(|(pba, count)| {
            new_refcount
                .checked_sub(count)
                .filter(|delta| *delta > 0)
                .map(|delta| (pba, delta))
        })
        .collect()
}

fn dedup_hit_results_from_remaps(
    hits: &[(Lba, BlockmapValue, ContentHash)],
    outcomes: Vec<ApplyOutcome>,
) -> OnyxResult<(Vec<DedupHitResult>, HashMap<Pba, u32>)> {
    if hits.len() != outcomes.len() {
        return Err(OnyxError::Config(format!(
            "metadb dedup outcome length mismatch: {} hits, {} outcomes",
            hits.len(),
            outcomes.len()
        )));
    }

    let mut results = Vec::with_capacity(hits.len());
    let mut newly_zeroed = HashMap::new();
    for ((_, new_value, _), outcome) in hits.iter().zip(outcomes.into_iter()) {
        let ApplyOutcome::L2pRemap {
            applied,
            prev,
            freed_pba,
        } = outcome
        else {
            return Err(OnyxError::Config(
                "metadb returned non-remap outcome for dedup hit batch".into(),
            ));
        };

        if !applied {
            results.push(DedupHitResult::Rejected);
            continue;
        }

        if let Some(prev) = prev {
            let old = decode_l2p_value(prev)?;
            if old.pba != new_value.pba {
                let blocks = freed_blocks_for_l2p_value(&old);
                if freed_pba.is_some_and(|pba| from_metadb_pba(pba) == old.pba) {
                    newly_zeroed.insert(old.pba, blocks);
                }
            }
        }
        results.push(DedupHitResult::Accepted(None));
    }
    Ok((results, newly_zeroed))
}

pub(crate) fn to_metadb_pba(pba: Pba) -> onyx_metadb::Pba {
    pba.0
}

pub(crate) fn from_metadb_pba(pba: onyx_metadb::Pba) -> Pba {
    Pba(pba)
}

pub(crate) fn to_l2p_value(value: &BlockmapValue) -> L2pValue {
    L2pValue(blockmap_to_l2p_bytes(value))
}

pub(crate) fn from_l2p_value(value: L2pValue) -> Option<BlockmapValue> {
    blockmap_from_l2p_bytes(&value.0)
}

pub(crate) fn to_dedup_value(entry: &DedupEntry) -> DedupValue {
    let mut bytes = [0u8; METADB_DEDUP_VALUE_BYTES];
    bytes[..DEDUP_VALUE_BYTES].copy_from_slice(&dedup_to_value_bytes(entry));
    DedupValue::new(bytes)
}

pub(crate) fn from_dedup_value(value: DedupValue) -> Option<DedupEntry> {
    let mut bytes = [0u8; DEDUP_VALUE_BYTES];
    bytes.copy_from_slice(&value.as_bytes()[..DEDUP_VALUE_BYTES]);
    dedup_from_value_bytes(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CompressionAlgo, VolumeId};

    #[test]
    fn pba_newtypes_cross_backend_losslessly() {
        let pba = Pba(1234);
        assert_eq!(from_metadb_pba(to_metadb_pba(pba)), pba);
    }

    #[test]
    fn dedup_value_uses_zero_padded_metadb_slot() {
        let entry = DedupEntry {
            pba: Pba(7),
            slot_offset: 5,
            compression: 1,
            unit_compressed_size: 1024,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 0xDEAD_BEEF,
        };

        let value = to_dedup_value(&entry);
        assert_eq!(value.as_bytes()[27], 0);
        assert_eq!(from_dedup_value(value), Some(entry));
    }

    #[test]
    fn volume_catalog_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(CATALOG_FILE);
        let mut catalog = VolumeCatalog::default();
        catalog.by_id.insert(
            "vol-a".to_string(),
            VolumeCatalogEntry {
                ordinal: 3,
                config: VolumeConfig {
                    id: VolumeId("vol-a".to_string()),
                    size_bytes: 4096,
                    block_size: 4096,
                    compression: CompressionAlgo::Lz4,
                    created_at: 10,
                    zone_count: 4,
                },
            },
        );

        catalog.persist(&path).unwrap();
        let loaded = VolumeCatalog::load(&path).unwrap();

        let entry = loaded.by_id.get("vol-a").unwrap();
        assert_eq!(entry.ordinal, 3);
        assert_eq!(entry.config.size_bytes, 4096);
        assert_eq!(entry.config.compression, CompressionAlgo::Lz4);
    }

    #[test]
    fn backend_volume_catalog_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let meta = MetaConfig {
            path: Some(dir.path().to_path_buf()),
            block_cache_mb: 8,
            memtable_budget_mb: 64,
            wal_dir: None,
        };
        let vol = VolumeConfig {
            id: VolumeId("vol-a".to_string()),
            size_bytes: 4096,
            block_size: 4096,
            compression: CompressionAlgo::Lz4,
            created_at: 10,
            zone_count: 4,
        };

        {
            let backend = MetadbBackend::open(&meta).unwrap();
            backend.put_volume(&vol).unwrap();
        }

        let backend = MetadbBackend::open(&meta).unwrap();
        let loaded = backend.get_volume(&vol.id).unwrap().unwrap();
        assert_eq!(loaded.id, vol.id);
        assert_eq!(loaded.size_bytes, vol.size_bytes);
        assert_eq!(loaded.compression, vol.compression);
        let listed = backend.list_volumes().unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, vol.id);
    }

    #[test]
    fn backend_reads_l2p_values_by_volume_id() {
        let dir = tempfile::tempdir().unwrap();
        let meta = MetaConfig {
            path: Some(dir.path().to_path_buf()),
            block_cache_mb: 8,
            memtable_budget_mb: 64,
            wal_dir: None,
        };
        let vol = VolumeConfig {
            id: VolumeId("vol-a".to_string()),
            size_bytes: 4096 * 8,
            block_size: 4096,
            compression: CompressionAlgo::Lz4,
            created_at: 10,
            zone_count: 4,
        };
        let value = BlockmapValue {
            pba: Pba(77),
            compression: 1,
            unit_compressed_size: 1234,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 0xCAFE_BABE,
            slot_offset: 0,
            flags: 0,
        };

        let backend = MetadbBackend::open(&meta).unwrap();
        backend.put_volume(&vol).unwrap();
        let ord = backend.volume_ordinal(&vol.id).unwrap();
        backend
            .db
            .insert(ord, 3, to_l2p_value(&value))
            .expect("insert test mapping");

        assert_eq!(backend.get_mapping(&vol.id, Lba(3)).unwrap(), Some(value));
        assert_eq!(
            backend
                .multi_get_mappings(&vol.id, &[Lba(2), Lba(3)])
                .unwrap(),
            vec![None, Some(value)]
        );
        assert_eq!(
            backend.get_mappings_range(&vol.id, Lba(0), Lba(8)).unwrap(),
            vec![(Lba(3), value)]
        );
    }

    #[test]
    fn atomic_batch_write_updates_refcounts_and_reports_freed_pba() {
        let dir = tempfile::tempdir().unwrap();
        let meta = MetaConfig {
            path: Some(dir.path().to_path_buf()),
            block_cache_mb: 8,
            memtable_budget_mb: 64,
            wal_dir: None,
        };
        let vol = VolumeConfig {
            id: VolumeId("vol-a".to_string()),
            size_bytes: 4096 * 8,
            block_size: 4096,
            compression: CompressionAlgo::Lz4,
            created_at: 10,
            zone_count: 4,
        };
        let backend = MetadbBackend::open(&meta).unwrap();
        backend.put_volume(&vol).unwrap();

        let old = BlockmapValue {
            pba: Pba(10),
            compression: 1,
            unit_compressed_size: 2048,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 1,
            slot_offset: 0,
            flags: 0,
        };
        let new = BlockmapValue {
            pba: Pba(20),
            crc32: 2,
            ..old
        };

        backend
            .atomic_batch_write(&vol.id, &[(Lba(0), old), (Lba(1), old)], 2)
            .unwrap();
        assert_eq!(backend.get_refcount(Pba(10)).unwrap(), 2);

        let freed = backend
            .atomic_batch_write(&vol.id, &[(Lba(0), new), (Lba(1), new)], 2)
            .unwrap();

        assert_eq!(backend.get_refcount(Pba(10)).unwrap(), 0);
        assert_eq!(backend.get_refcount(Pba(20)).unwrap(), 2);
        assert_eq!(freed.get(&Pba(10)), Some(&(2, 1)));
    }

    #[test]
    fn dedup_entries_and_flag_scan_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let meta = MetaConfig {
            path: Some(dir.path().to_path_buf()),
            block_cache_mb: 8,
            memtable_budget_mb: 64,
            wal_dir: None,
        };
        let vol = VolumeConfig {
            id: VolumeId("vol-a".to_string()),
            size_bytes: 4096 * 8,
            block_size: 4096,
            compression: CompressionAlgo::Lz4,
            created_at: 10,
            zone_count: 4,
        };
        let backend = MetadbBackend::open(&meta).unwrap();
        backend.put_volume(&vol).unwrap();

        let value = BlockmapValue {
            pba: Pba(30),
            compression: 1,
            unit_compressed_size: 2048,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 1,
            slot_offset: 0,
            flags: FLAG_DEDUP_SKIPPED,
        };
        backend
            .atomic_batch_write(&vol.id, &[(Lba(0), value)], 1)
            .unwrap();

        let hash = [9u8; 32];
        let dedup = DedupEntry {
            pba: value.pba,
            slot_offset: value.slot_offset,
            compression: value.compression,
            unit_compressed_size: value.unit_compressed_size,
            unit_original_size: value.unit_original_size,
            unit_lba_count: value.unit_lba_count,
            offset_in_unit: value.offset_in_unit,
            crc32: value.crc32,
        };
        backend.put_dedup_entries(&[(hash, dedup)]).unwrap();

        assert_eq!(backend.get_dedup(&hash).unwrap(), Some(dedup));
        assert!(backend.dedup_entry_is_live(&hash, &dedup).unwrap());
        assert_eq!(backend.scan_dedup_skipped(8).unwrap().len(), 1);

        backend.update_blockmap_flags(&vol.id, Lba(0), 0).unwrap();
        assert!(backend.scan_dedup_skipped(8).unwrap().is_empty());

        let replacement = BlockmapValue {
            pba: Pba(40),
            flags: 0,
            ..value
        };
        backend
            .atomic_batch_write(&vol.id, &[(Lba(0), replacement)], 1)
            .unwrap();
        assert_eq!(backend.get_refcount(value.pba).unwrap(), 0);
        backend.cleanup_dedup_for_pbas_batch(&[value.pba]).unwrap();
        assert_eq!(backend.get_dedup(&hash).unwrap(), None);
    }

    #[test]
    fn delete_range_and_volume_report_freed_extents() {
        let dir = tempfile::tempdir().unwrap();
        let meta = MetaConfig {
            path: Some(dir.path().to_path_buf()),
            block_cache_mb: 8,
            memtable_budget_mb: 64,
            wal_dir: None,
        };
        let vol = VolumeConfig {
            id: VolumeId("vol-a".to_string()),
            size_bytes: 4096 * 8,
            block_size: 4096,
            compression: CompressionAlgo::Lz4,
            created_at: 10,
            zone_count: 4,
        };
        let backend = MetadbBackend::open(&meta).unwrap();
        backend.put_volume(&vol).unwrap();

        let a = BlockmapValue {
            pba: Pba(100),
            compression: 1,
            unit_compressed_size: 2048,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 1,
            slot_offset: 0,
            flags: 0,
        };
        let b = BlockmapValue {
            pba: Pba(200),
            crc32: 2,
            ..a
        };
        backend
            .atomic_batch_write(&vol.id, &[(Lba(0), a), (Lba(1), b), (Lba(2), b)], 3)
            .unwrap();

        let freed = backend
            .delete_blockmap_range(&vol.id, Lba(1), Lba(3))
            .unwrap();
        assert_eq!(freed, vec![(Pba(200), 1)]);
        assert_eq!(backend.count_blockmap_refs_for_pba(Pba(100)).unwrap(), 1);

        let freed = backend.delete_volume(&vol.id).unwrap();
        assert_eq!(freed, vec![(Pba(100), 1)]);
        assert!(backend.get_volume(&vol.id).unwrap().is_none());
    }

    #[test]
    fn diagnostic_helpers_track_refs_and_allocated_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let meta = MetaConfig {
            path: Some(dir.path().to_path_buf()),
            block_cache_mb: 8,
            memtable_budget_mb: 64,
            wal_dir: None,
        };
        let vol = VolumeConfig {
            id: VolumeId("vol-a".to_string()),
            size_bytes: 4096 * 8,
            block_size: 4096,
            compression: CompressionAlgo::Lz4,
            created_at: 10,
            zone_count: 4,
        };
        let backend = MetadbBackend::open(&meta).unwrap();
        backend.put_volume(&vol).unwrap();
        let value = BlockmapValue {
            pba: Pba(55),
            compression: 1,
            unit_compressed_size: 2048,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 1,
            slot_offset: 0,
            flags: 0,
        };

        backend
            .atomic_write_mapping(&vol.id, Lba(0), &value)
            .unwrap();
        assert_eq!(backend.iter_refcounts().unwrap(), vec![(Pba(55), 1)]);
        assert_eq!(backend.iter_allocated_blocks().unwrap(), vec![(Pba(55), 1)]);
        assert!(backend.has_any_blockmap_ref(Pba(55)).unwrap());

        backend.delete_mapping(&vol.id, Lba(0)).unwrap();
        assert_eq!(
            backend.cleanup_orphaned_refcounts().unwrap(),
            vec![(Pba(55), 1)]
        );
        assert_eq!(backend.get_refcount(Pba(55)).unwrap(), 0);
    }
}
