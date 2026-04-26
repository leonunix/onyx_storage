use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use onyx_metadb::{Config as MetaDbConfig, Db, DedupValue, L2pValue, VolumeOrdinal};
use serde::{Deserialize, Serialize};

use crate::config::MetaConfig;
use crate::error::{OnyxError, OnyxResult};
use crate::meta::schema::{BlockmapValue, ContentHash, DedupEntry};
use crate::types::{Lba, Pba, VolumeConfig, VolumeId};

use super::codec::{
    blockmap_from_l2p_bytes, blockmap_to_l2p_bytes, dedup_from_value_bytes, dedup_to_value_bytes,
    DEDUP_VALUE_BYTES,
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
            OnyxError::Config(
                "meta.path (or legacy meta.rocksdb_path) is required to open metadb backend".into(),
            )
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

    pub(crate) fn get_mapping(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
    ) -> OnyxResult<Option<BlockmapValue>> {
        let ord = self.volume_ordinal(vol_id)?;
        self.db.get(ord, lba.0)?.map(decode_l2p_value).transpose()
    }

    pub(crate) fn multi_get_mappings(
        &self,
        vol_id: &VolumeId,
        lbas: &[Lba],
    ) -> OnyxResult<Vec<Option<BlockmapValue>>> {
        if lbas.is_empty() {
            return Ok(Vec::new());
        }
        let ord = self.volume_ordinal(vol_id)?;
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
        let ord = self.volume_ordinal(vol_id)?;
        self.db
            .range(ord, start.0..end.0)?
            .map(|item| {
                let (lba, value) = item?;
                Ok((Lba(lba), decode_l2p_value(value)?))
            })
            .collect()
    }

    pub(crate) fn get_refcount(&self, pba: Pba) -> OnyxResult<u32> {
        Ok(self.db.get_refcount(to_metadb_pba(pba))?)
    }

    pub(crate) fn multi_get_refcounts(&self, pbas: &[Pba]) -> OnyxResult<Vec<u32>> {
        let pbas: Vec<onyx_metadb::Pba> = pbas.iter().map(|pba| to_metadb_pba(*pba)).collect();
        Ok(self.db.multi_get_refcount(&pbas)?)
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
            rocksdb_path: Some(dir.path().to_path_buf()),
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
            rocksdb_path: Some(dir.path().to_path_buf()),
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
}
