use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

use crossbeam_channel::{unbounded, Receiver, Sender};
use dashmap::DashMap;
use onyx_metadb::{
    ApplyOutcome, Config as MetaDbConfig, Db, DedupValue, L2pValue, Transaction, VolumeOrdinal,
};
use serde::{Deserialize, Serialize};

use crate::config::MetaConfig;
use crate::error::{OnyxError, OnyxResult};
use crate::meta::schema::{BlockmapValue, ContentHash, DedupEntry, FLAG_DEDUP_SKIPPED};
use crate::meta::store::{DedupHitResult, RemapCleanup};
use crate::metrics::MetaMemorySnapshot;
use crate::space::extent::Extent;
use crate::types::{Lba, Pba, VolumeConfig, VolumeId};

use super::codec::{
    blockmap_from_l2p_bytes, blockmap_to_l2p_bytes, blockmap_to_l2p_bytes_with_seq,
    dedup_from_value_bytes, dedup_to_value_bytes, freed_blocks_for_l2p_value, DEDUP_VALUE_BYTES,
};

const METADB_DEDUP_VALUE_BYTES: usize = 28;
const CATALOG_VERSION: u32 = 1;
const CATALOG_FILE: &str = "onyx-volume-catalog.bin";
const METADB_PAGE_FILE: &str = "pages.onyx_meta";
const BLOCKMAP_SCAN_CHUNK_LBAS: u64 = 262_144; // 1 GiB of 4 KiB LBAs.
const DEDUP_PERSIST_BATCH_LIMIT: usize = crate::dedup::config::DEDUP_PUT_BATCH_HARD_MAX_ENTRIES;

pub(crate) struct MetadbBackend {
    db: Arc<Db>,
    checkpoint: AsyncCheckpoint,
    unlogged_flush_commits: bool,
    catalog: Mutex<VolumeCatalog>,
    volume_ordinals: DashMap<String, VolumeOrdinal>,
    catalog_path: PathBuf,
    // Lineage GC freed-PBA signal channel.
    //
    // `metadb` invokes `freed_pbas_sink` synchronously on its GC driver
    // thread. The sink does a non-blocking enqueue here so the GC thread
    // never blocks on onyx's retire pipeline. The engine drains this
    // channel and feeds the PBAs through the existing allocator retire
    // path (coalesced into extents).
    //
    // Phase 4 keeps `lineage_gc_emit_freepbas` default OFF, so the channel
    // stays empty unless tests or Phase 5 explicitly enable the flag.
    lineage_freed_pbas_tx: Sender<Pba>,
    lineage_freed_pbas_rx: Receiver<Pba>,
}

struct AsyncCheckpoint {
    state: Arc<(Mutex<CheckpointState>, Condvar)>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Default)]
struct CheckpointState {
    requested: u64,
    completed: u64,
    checkpointed: u64,
    force_requested: u64,
    failures: Vec<CheckpointFailure>,
    shutdown: bool,
}

struct CheckpointFailure {
    start: u64,
    end: u64,
    message: String,
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
        let db = Arc::new(db);

        let catalog_path = path.join(CATALOG_FILE);
        let catalog = VolumeCatalog::load(&catalog_path)?;
        catalog.validate_against_db(&db)?;
        let volume_ordinals = catalog
            .by_id
            .iter()
            .map(|(id, entry)| (id.clone(), entry.ordinal))
            .collect();
        let checkpoint = AsyncCheckpoint::start(db.clone())?;

        let (lineage_freed_pbas_tx, lineage_freed_pbas_rx) = unbounded::<Pba>();
        let sink_tx = lineage_freed_pbas_tx.clone();
        db.set_freed_pbas_sink(Arc::new(move |_vol_ord, pbas| {
            for pba in pbas {
                // Unbounded channel: send only fails when all receivers are
                // dropped, which means `MetadbBackend` itself is gone.
                let _ = sink_tx.send(from_metadb_pba(pba));
            }
        }));

        Ok(Self {
            db,
            checkpoint,
            unlogged_flush_commits: config.unlogged_flush_commits,
            catalog: Mutex::new(catalog),
            volume_ordinals,
            catalog_path,
            lineage_freed_pbas_tx,
            lineage_freed_pbas_rx,
        })
    }

    /// Non-blocking drain of every PBA that lineage GC has signalled as
    /// freed since the last call. Returns owned `Vec<Pba>`; the engine
    /// passes this through `coalesce_free_pbas_to_extents` and retires the
    /// resulting extents via the allocator.
    pub(crate) fn drain_lineage_freed_pbas(&self) -> Vec<Pba> {
        let mut out = Vec::new();
        while let Ok(pba) = self.lineage_freed_pbas_rx.try_recv() {
            out.push(pba);
        }
        out
    }

    #[cfg(test)]
    pub(crate) fn lineage_freed_pbas_sender(&self) -> Sender<Pba> {
        self.lineage_freed_pbas_tx.clone()
    }

    pub(crate) fn put_volume(&self, config: &VolumeConfig) -> OnyxResult<()> {
        let mut catalog = self.catalog.lock().unwrap();
        if let Some(entry) = catalog.by_id.get_mut(&config.id.0) {
            entry.config = config.clone();
            self.volume_ordinals
                .insert(config.id.0.clone(), entry.ordinal);
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
        self.volume_ordinals.insert(config.id.0.clone(), ordinal);
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
        self.volume_ordinal_str(&id.0)
    }

    fn volume_ordinal_optional(&self, id: &VolumeId) -> Option<VolumeOrdinal> {
        self.volume_ordinal_optional_str(&id.0)
    }

    pub(crate) fn volume_ordinal_str(&self, id: &str) -> OnyxResult<VolumeOrdinal> {
        self.volume_ordinal_optional_str(id)
            .ok_or_else(|| OnyxError::VolumeNotFound(id.to_string()))
    }

    fn volume_ordinal_optional_str(&self, id: &str) -> Option<VolumeOrdinal> {
        if let Some(ord) = self.volume_ordinals.get(id) {
            return Some(*ord);
        }
        let catalog = self.catalog.lock().unwrap();
        let ord = catalog.by_id.get(id).map(|entry| entry.ordinal)?;
        self.volume_ordinals.insert(id.to_string(), ord);
        Some(ord)
    }

    pub(crate) fn delete_volume(&self, id: &VolumeId) -> OnyxResult<Vec<RemapCleanup>> {
        let (ordinal, config) = {
            let catalog = self.catalog.lock().unwrap();
            let Some(entry) = catalog.by_id.get(&id.0) else {
                return Ok(Vec::new());
            };
            (entry.ordinal, entry.config.clone())
        };

        let end = Lba(config.size_bytes / u64::from(config.block_size));
        let cleanups = self.delete_blockmap_range(id, Lba(0), end)?;
        self.db.drop_volume(ordinal)?;

        let mut catalog = self.catalog.lock().unwrap();
        catalog.by_id.remove(&id.0);
        self.volume_ordinals.remove(&id.0);
        catalog.persist(&self.catalog_path)?;

        Ok(cleanups)
    }

    pub(crate) fn get_mapping(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
    ) -> OnyxResult<Option<BlockmapValue>> {
        self.get_mapping_str(&vol_id.0, lba)
    }

    pub(crate) fn get_mapping_str(
        &self,
        vol_id: &str,
        lba: Lba,
    ) -> OnyxResult<Option<BlockmapValue>> {
        let Some(ord) = self.volume_ordinal_optional_str(vol_id) else {
            return Ok(None);
        };
        self.get_mapping_ord(ord, lba)
    }

    pub(crate) fn get_mapping_ord(
        &self,
        ord: VolumeOrdinal,
        lba: Lba,
    ) -> OnyxResult<Option<BlockmapValue>> {
        self.db.get(ord, lba.0)?.map(decode_l2p_value).transpose()
    }

    /// Like [`get_mapping`] but also returns the committed L2pValue
    /// seq so the caller can forward it as the seq_guard on a
    /// subsequent `l2p_remap`. Background scanners that re-check an
    /// LBA's mapping and then submit a derived update must use this
    /// (not plain `get_mapping`) — emitting seq=0 silently bypasses
    /// `seq_guard_rejects` and clobbers any newer write that landed in
    /// the meantime (see `update_blockmap_flags` and the
    /// `metadb_seq0_in_l2p_remap_bypasses_guard_and_clobbers_newer_write`
    /// regression test).
    pub(crate) fn get_mapping_with_seq(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
    ) -> OnyxResult<Option<(BlockmapValue, u64)>> {
        let ord = self.volume_ordinal(vol_id)?;
        let Some(raw) = self.db.get(ord, lba.0)? else {
            return Ok(None);
        };
        let seq = raw.seq();
        let Some(bv) = blockmap_from_l2p_bytes(&raw.0) else {
            return Ok(None);
        };
        Ok(Some((bv, seq)))
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
        self.multi_get_mappings_str(&vol_id.0, lbas)
    }

    pub(crate) fn multi_get_mappings_str(
        &self,
        vol_id: &str,
        lbas: &[Lba],
    ) -> OnyxResult<Vec<Option<BlockmapValue>>> {
        if lbas.is_empty() {
            return Ok(Vec::new());
        }
        let Some(ord) = self.volume_ordinal_optional_str(vol_id) else {
            return Ok(vec![None; lbas.len()]);
        };
        self.multi_get_mappings_ord(ord, lbas)
    }

    pub(crate) fn multi_get_mappings_ord(
        &self,
        ord: VolumeOrdinal,
        lbas: &[Lba],
    ) -> OnyxResult<Vec<Option<BlockmapValue>>> {
        let raw_lbas: Vec<onyx_metadb::Lba> = lbas.iter().map(|lba| lba.0).collect();
        self.multi_get_mappings_raw_ord(ord, &raw_lbas)
    }

    pub(crate) fn multi_get_mappings_raw_ord(
        &self,
        ord: VolumeOrdinal,
        lbas: &[onyx_metadb::Lba],
    ) -> OnyxResult<Vec<Option<BlockmapValue>>> {
        if lbas.is_empty() {
            return Ok(Vec::new());
        }
        self.db
            .multi_get(ord, lbas)?
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

    pub(crate) fn get_mappings_range_unordered(
        &self,
        vol_id: &VolumeId,
        start: Lba,
        end: Lba,
    ) -> OnyxResult<Vec<(Lba, BlockmapValue)>> {
        self.get_mappings_range_unordered_str(&vol_id.0, start, end)
    }

    pub(crate) fn get_mappings_range_unordered_str(
        &self,
        vol_id: &str,
        start: Lba,
        end: Lba,
    ) -> OnyxResult<Vec<(Lba, BlockmapValue)>> {
        let Some(ord) = self.volume_ordinal_optional_str(vol_id) else {
            return Ok(Vec::new());
        };
        self.get_mappings_range_unordered_ord(ord, start, end)
    }

    pub(crate) fn get_mappings_range_unordered_ord(
        &self,
        ord: VolumeOrdinal,
        start: Lba,
        end: Lba,
    ) -> OnyxResult<Vec<(Lba, BlockmapValue)>> {
        let mut out = Vec::new();
        self.db
            .scan_range_unordered(ord, start.0..end.0, |lba, value| {
                let decoded = from_l2p_value(value).ok_or_else(|| {
                    onyx_metadb::MetaDbError::Corruption("invalid onyx blockmap value".into())
                })?;
                out.push((Lba(lba), decoded));
                Ok(())
            })?;
        Ok(out)
    }

    pub(crate) fn delete_blockmap_range(
        &self,
        vol_id: &VolumeId,
        start: Lba,
        end: Lba,
    ) -> OnyxResult<Vec<RemapCleanup>> {
        if start >= end {
            return Ok(Vec::new());
        }

        let Some(ord) = self.volume_ordinal_optional(vol_id) else {
            return Ok(Vec::new());
        };
        let mut pba_meta: HashMap<Pba, RemapCleanup> = HashMap::new();
        for item in self.db.range(ord, start.0..end.0)? {
            let (_, value) = item?;
            let value = decode_l2p_value(value)?;
            if value.is_zero() {
                continue;
            }
            let blocks = freed_blocks_for_l2p_value(&value);
            pba_meta
                .entry(value.pba)
                .and_modify(|existing| existing.merge(RemapCleanup::new(value, blocks)))
                .or_insert_with(|| RemapCleanup::new(value, blocks));
        }
        if pba_meta.is_empty() {
            return Ok(Vec::new());
        }

        self.db.range_delete(ord, start.0, end.0)?;

        let mut cleanups = Vec::new();
        let pbas: Vec<Pba> = pba_meta.keys().copied().collect();
        let refcounts = self.multi_get_refcounts(&pbas)?;
        for (pba, refcount) in pbas.into_iter().zip(refcounts.into_iter()) {
            if let Some(mut cleanup) = pba_meta.remove(&pba) {
                if refcount == 0 {
                    cleanup.pba_freed = true;
                    cleanups.push(cleanup);
                }
            }
        }
        cleanups.sort_unstable_by_key(|cleanup| cleanup.pba);
        Ok(cleanups)
    }

    pub(crate) fn get_refcount(&self, pba: Pba) -> OnyxResult<u32> {
        Ok(self.db.get_refcount(to_metadb_pba(pba))?)
    }

    pub(crate) fn multi_get_refcounts(&self, pbas: &[Pba]) -> OnyxResult<Vec<u32>> {
        let pbas: Vec<onyx_metadb::Pba> = pbas.iter().map(|pba| to_metadb_pba(*pba)).collect();
        Ok(self.db.multi_get_refcount(&pbas)?)
    }

    /// Test-only refcount seed. Phase 5 removed the per-write refcount
    /// path; production code mutates rc only via lineage events
    /// (PromotionChunk / FreePbas / drop_volume). This helper exists
    /// so existing tests can prep a non-zero rc on a PBA when
    /// exercising dedup hit / packed-slot scenarios. Routes through the
    /// metadb test helper which itself drives a PromotionChunk.
    #[doc(hidden)]
    pub(crate) fn set_refcount(&self, pba: Pba, count: u32) -> OnyxResult<()> {
        let current = self.get_refcount(pba)?;
        if count > current {
            self.db.incref_pba(to_metadb_pba(pba), count - current)?;
        } else if current > count {
            self.db.decref_pba(to_metadb_pba(pba), current - count)?;
        }
        Ok(())
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

    pub(crate) fn multi_dedup_entries_are_live(
        &self,
        entries: &[(ContentHash, DedupEntry)],
    ) -> OnyxResult<Vec<bool>> {
        let metadb_entries: Vec<(ContentHash, DedupValue)> = entries
            .iter()
            .map(|(hash, entry)| (*hash, to_dedup_value(entry)))
            .collect();
        Ok(self.db.multi_dedup_entries_are_live(&metadb_entries)?)
    }

    pub(crate) fn atomic_batch_write(
        &self,
        vol_id: &VolumeId,
        batch_values: &[(Lba, BlockmapValue)],
        new_refcount: u32,
    ) -> OnyxResult<HashMap<Pba, RemapCleanup>> {
        let (cleanups, _accepted) =
            self.atomic_batch_write_with_dedup(vol_id, batch_values, new_refcount, &[], &[])?;
        Ok(cleanups)
    }

    pub(crate) fn atomic_batch_write_with_dedup(
        &self,
        vol_id: &VolumeId,
        batch_values: &[(Lba, BlockmapValue)],
        new_refcount: u32,
        dedup_entries: &[(ContentHash, DedupEntry)],
        seqs: &[u64],
    ) -> OnyxResult<(HashMap<Pba, RemapCleanup>, Vec<bool>)> {
        debug_assert!(seqs.is_empty() || seqs.len() == batch_values.len());
        if batch_values.is_empty() {
            self.put_dedup_entries(dedup_entries)?;
            return Ok((HashMap::new(), Vec::new()));
        }
        let ord = self.volume_ordinal(vol_id)?;
        let _ = new_refcount;
        let mut tx = self.db.begin();
        emit_l2p_remap_runs(&mut tx, ord, batch_values, seqs, 0);
        for (hash, entry) in dedup_entries {
            tx.put_dedup(*hash, to_dedup_value(entry));
        }
        let (_, outcomes) = if self.unlogged_flush_commits {
            tx.commit_unlogged_with_outcomes()?
        } else {
            tx.commit_with_outcomes()?
        };
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
    ) -> OnyxResult<HashMap<Pba, RemapCleanup>> {
        let (cleanups, _accepted) = self.atomic_batch_write_packed_with_dedup(
            batch_values,
            _new_pba,
            _new_refcount,
            &[],
            &[],
        )?;
        Ok(cleanups)
    }

    pub(crate) fn atomic_batch_write_packed_with_dedup(
        &self,
        batch_values: &[(VolumeId, Lba, BlockmapValue)],
        _new_pba: Pba,
        _new_refcount: u32,
        dedup_entries: &[(ContentHash, DedupEntry)],
        seqs: &[u64],
    ) -> OnyxResult<(HashMap<Pba, RemapCleanup>, Vec<bool>)> {
        debug_assert!(seqs.is_empty() || seqs.len() == batch_values.len());
        if batch_values.is_empty() {
            self.put_dedup_entries(dedup_entries)?;
            return Ok((HashMap::new(), Vec::new()));
        }
        let mut tx = self.db.begin();
        let mut new_values = Vec::with_capacity(batch_values.len());
        let mut ordinal_cache: HashMap<&str, VolumeOrdinal> = HashMap::new();
        for (i, (vol_id, lba, value)) in batch_values.iter().enumerate() {
            let ord = match ordinal_cache.get(vol_id.0.as_str()) {
                Some(ord) => *ord,
                None => {
                    let ord = self.volume_ordinal(vol_id)?;
                    ordinal_cache.insert(vol_id.0.as_str(), ord);
                    ord
                }
            };
            tx.l2p_remap(
                ord,
                lba.0,
                to_l2p_value_with_seq(value, seq_for(seqs, i)),
                None,
            );
            new_values.push(*value);
        }
        for (hash, entry) in dedup_entries {
            tx.put_dedup(*hash, to_dedup_value(entry));
        }
        let (_, outcomes) = if self.unlogged_flush_commits {
            tx.commit_unlogged_with_outcomes()?
        } else {
            tx.commit_with_outcomes()?
        };
        let remap_count = new_values.len();
        newly_zeroed_from_remaps(new_values, outcomes.into_iter().take(remap_count).collect())
    }

    pub(crate) fn atomic_batch_write_multi(
        &self,
        units: &[(&VolumeId, &[(Lba, BlockmapValue)], u32)],
    ) -> OnyxResult<HashMap<Pba, RemapCleanup>> {
        let (cleanups, _accepted) = self.atomic_batch_write_multi_with_dedup(units, &[], &[])?;
        Ok(cleanups)
    }

    pub(crate) fn atomic_batch_write_multi_with_dedup(
        &self,
        units: &[(&VolumeId, &[(Lba, BlockmapValue)], u32)],
        dedup_entries: &[(ContentHash, DedupEntry)],
        seqs: &[u64],
    ) -> OnyxResult<(HashMap<Pba, RemapCleanup>, Vec<bool>)> {
        let total_lbas: usize = units.iter().map(|(_, b, _)| b.len()).sum();
        debug_assert!(seqs.is_empty() || seqs.len() == total_lbas);
        if units.is_empty() {
            self.put_dedup_entries(dedup_entries)?;
            return Ok((HashMap::new(), Vec::new()));
        }
        let mut tx = self.db.begin();
        let mut new_values = Vec::new();
        let mut flat_idx: usize = 0;
        for (vol_id, batch_values, new_refcount) in units {
            let _ = new_refcount;
            let ord = self.volume_ordinal(vol_id)?;
            // Each unit's batch_values is contiguous LBAs by construction
            // (one CompressedUnit / one passthrough sub-batch); emit one
            // range op per maximal contiguous LBA run within the unit.
            emit_l2p_remap_runs(&mut tx, ord, batch_values, seqs, flat_idx);
            for (_, value) in *batch_values {
                new_values.push(*value);
            }
            flat_idx += batch_values.len();
        }
        for (hash, entry) in dedup_entries {
            tx.put_dedup(*hash, to_dedup_value(entry));
        }
        let (_, outcomes) = if self.unlogged_flush_commits {
            tx.commit_unlogged_with_outcomes()?
        } else {
            tx.commit_with_outcomes()?
        };
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
        if entries.len() > DEDUP_PERSIST_BATCH_LIMIT {
            for chunk in entries.chunks(DEDUP_PERSIST_BATCH_LIMIT) {
                self.put_dedup_entries(chunk)?;
            }
            return Ok(());
        }

        let mut tx = self.db.begin();
        for (hash, entry) in entries {
            tx.put_dedup(*hash, to_dedup_value(entry));
        }
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn delete_dedup_index(&self, hash: &ContentHash) -> OnyxResult<()> {
        let mut tx = self.db.begin();
        tx.delete_dedup(*hash);
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn delete_dedup_index_if_matches(
        &self,
        hash: &ContentHash,
        mapping: &BlockmapValue,
    ) -> OnyxResult<bool> {
        let entry = BlockmapValue {
            flags: 0,
            ..*mapping
        }
        .to_dedup_entry();
        Ok(self
            .db
            .compare_delete_dedup(*hash, to_dedup_value(&entry))?)
    }

    pub(crate) fn compare_put_dedup_index(
        &self,
        hash: &ContentHash,
        old_entry: &DedupEntry,
        new_entry: &DedupEntry,
    ) -> OnyxResult<bool> {
        Ok(self.db.compare_put_dedup(
            *hash,
            to_dedup_value(old_entry),
            to_dedup_value(new_entry),
        )?)
    }

    pub(crate) fn dedup_entry_is_live(
        &self,
        hash: &ContentHash,
        entry: &DedupEntry,
    ) -> OnyxResult<bool> {
        // Hot-path liveness check (one per dedup hit). Two semantics:
        //   1. Re-lookup the forward index for `hash`. If it still
        //      points at exactly `entry` (same pba + payload), the
        //      original lookup hasn't been superseded by a concurrent
        //      re-registration or tombstoned by old-mapping cleanup.
        //   2. Short-circuit if the entry's pba already nets to refcount 0.
        //      Cleanup runs lazily; the forward entry can momentarily point
        //      at a doomed pba whose final decref already landed. Treating
        //      it as live here would let us bump a refcount that's about
        //      to be reclaimed.
        Ok(self.multi_dedup_entries_are_live(&[(*hash, *entry)])?[0])
    }

    pub(crate) fn scan_all_blockmap_entries(
        &self,
    ) -> OnyxResult<Vec<(VolumeId, Lba, BlockmapValue)>> {
        let mut entries = Vec::new();
        self.scan_all_blockmap_entries_with(&mut |volume, lba, value| {
            entries.push((volume.clone(), lba, value));
        })?;
        Ok(entries)
    }

    pub(crate) fn scan_all_blockmap_entries_with(
        &self,
        callback: &mut dyn FnMut(&VolumeId, Lba, BlockmapValue),
    ) -> OnyxResult<()> {
        let volumes = self.list_volumes()?;
        for volume in volumes {
            let ord = self.volume_ordinal(&volume.id)?;
            let lba_count = volume.size_bytes / u64::from(volume.block_size);
            let mut decode_error = None;
            let scan_result = self.db.scan_range_unordered_chunked(
                ord,
                0,
                lba_count,
                BLOCKMAP_SCAN_CHUNK_LBAS,
                |lba, value| {
                    match decode_l2p_value(value) {
                        Ok(decoded) => callback(&volume.id, Lba(lba), decoded),
                        Err(err) => {
                            decode_error = Some(err);
                            return Err(onyx_metadb::MetaDbError::Corruption(
                                "onyx blockmap decode failed".into(),
                            ));
                        }
                    }
                    Ok(())
                },
            );
            if let Some(err) = decode_error {
                return Err(err);
            }
            scan_result?;
        }
        Ok(())
    }

    pub(crate) fn scan_blockmap_range(
        &self,
        vol_id: &VolumeId,
        start_lba: Lba,
        count: u64,
        callback: &mut dyn FnMut(Lba, BlockmapValue),
    ) -> OnyxResult<()> {
        if count == 0 {
            return Ok(());
        }
        let ord = self.volume_ordinal(vol_id)?;
        let end = start_lba.0.saturating_add(count);
        let mut decode_error = None;
        let scan_result =
            self.db
                .scan_range_unordered_chunked(ord, start_lba.0, end, count, |lba, value| {
                    match decode_l2p_value(value) {
                        Ok(decoded) => callback(Lba(lba), decoded),
                        Err(err) => {
                            decode_error = Some(err);
                            return Err(onyx_metadb::MetaDbError::Corruption(
                                "onyx blockmap decode failed".into(),
                            ));
                        }
                    }
                    Ok(())
                });
        if let Some(err) = decode_error {
            return Err(err);
        }
        scan_result?;
        Ok(())
    }

    pub(crate) fn count_blockmap_refs_for_pba(&self, target: Pba) -> OnyxResult<u32> {
        let mut count = 0u32;
        self.scan_all_blockmap_entries_with(&mut |_, _, value| {
            if value.pba == target {
                count = count.saturating_add(1);
            }
        })?;
        Ok(count)
    }

    pub(crate) fn has_any_blockmap_ref(&self, target: Pba) -> OnyxResult<bool> {
        let mut found = false;
        self.scan_all_blockmap_entries_with(&mut |_, _, value| {
            if !value.is_zero()
                && value
                    .physical_pbas(crate::types::BLOCK_SIZE)
                    .any(|pba| pba == target)
            {
                found = true;
            }
        })?;
        Ok(found)
    }

    pub(crate) fn has_any_blockmap_ref_in_extent(
        &self,
        start: Pba,
        blocks: u32,
    ) -> OnyxResult<bool> {
        if blocks == 0 {
            return Ok(false);
        }
        let end = start.0.saturating_add(u64::from(blocks));
        let mut found = false;
        self.scan_all_blockmap_entries_with(&mut |_, _, value| {
            if value.is_zero() {
                return;
            }
            if value
                .physical_pbas(crate::types::BLOCK_SIZE)
                .any(|pba| pba.0 >= start.0 && pba.0 < end)
            {
                found = true;
            }
        })?;
        Ok(found)
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

    pub(crate) fn iter_allocated_blocks(&self) -> OnyxResult<Vec<(Pba, u32)>> {
        let mut blocks: Vec<(Pba, u32)> = Vec::new();
        for (_, _, value) in self.scan_all_blockmap_entries()? {
            if value.is_zero() {
                continue;
            }
            let physical_blocks = freed_blocks_for_l2p_value(&value);
            for pba in value.physical_pbas(crate::types::BLOCK_SIZE) {
                blocks.push((pba, 1));
            }
            debug_assert_eq!(
                physical_blocks as usize,
                value.physical_pbas(crate::types::BLOCK_SIZE).count(),
                "freed block count must match expanded physical footprint"
            );
        }
        blocks.sort_unstable_by_key(|(pba, _)| *pba);
        blocks.dedup_by_key(|(pba, _)| *pba);
        Ok(blocks)
    }

    pub(crate) fn rebuild_refcount_from_blockmap(&self) -> OnyxResult<()> {
        Err(OnyxError::Config(
            "refcount rebuild is not supported by metadb backend; WAL recovery owns refcount"
                .into(),
        ))
    }

    pub(crate) fn sync_durable(&self) -> OnyxResult<()> {
        self.checkpoint.sync()
    }

    pub(crate) fn request_durable_checkpoint(&self) -> OnyxResult<()> {
        self.checkpoint.request_async()
    }

    pub(crate) fn try_request_durable_checkpoint(&self) -> OnyxResult<bool> {
        self.checkpoint.try_request_async()
    }

    pub(crate) fn dirty_pages_estimate(&self) -> usize {
        self.db.dirty_pages_estimate()
    }

    pub(crate) fn try_request_durable_checkpoint_token(&self) -> OnyxResult<Option<u64>> {
        self.checkpoint.try_request_async_token()
    }

    pub(crate) fn durable_checkpoint_outcome(&self, token: u64) -> OnyxResult<Option<bool>> {
        self.checkpoint.checkpoint_outcome(token)
    }

    pub(crate) fn memory_stats(&self) -> OnyxResult<MetaMemorySnapshot> {
        Ok(MetaMemorySnapshot::from_metadb(
            self.db.last_applied_lsn(),
            self.db.high_water(),
            self.db.free_list_len() as u64,
            self.db.dedup_tier_sizes(),
            self.db.cache_stats(),
            self.db.metrics_snapshot(),
            self.db.pending_state(),
        ))
    }

    pub(crate) fn scan_dedup_skipped(
        &self,
        limit: usize,
    ) -> OnyxResult<Vec<(String, Lba, BlockmapValue)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
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
        let ord = self.volume_ordinal(vol_id)?;
        // Read the raw L2pValue so we can carry its commit seq through
        // and use it as the seq_guard at apply time. Plain
        // `get_mapping` decodes only the BlockmapValue payload and
        // drops the trailing 8 B seq, which is the field metadb's
        // `apply_l2p_remap` checks. Without this, the tx below would
        // emit seq=0 and `seq_guard_rejects` short-circuits — a
        // concurrent buffer-flusher commit landing between our get and
        // the apply would be silently clobbered. See the
        // `update_blockmap_flags_seq0_must_not_clobber_newer_write`
        // regression test for the exact race.
        let Some(raw) = self.db.get(ord, lba.0)? else {
            return Ok(());
        };
        let observed_seq = raw.seq();
        let Some(mut value) = blockmap_from_l2p_bytes(&raw.0) else {
            return Ok(());
        };
        if value.flags == new_flags {
            return Ok(());
        }
        value.flags = new_flags;
        let mut tx = self.db.begin();
        // Re-emit with the same seq we observed. The apply path's
        // `seq_guard_rejects` will accept the equal-seq update (no
        // concurrent commit) and reject the strictly-less-than case
        // (a newer flusher commit raced us), preserving the invariant
        // that newer seqs win.
        tx.l2p_remap(
            ord,
            lba.0,
            to_l2p_value_with_seq(&value, observed_seq),
            None,
        );
        // Match `atomic_dedup_hit` / `atomic_batch_dedup_hits` semantics:
        // this is a flag-bit edit on an existing mapping with no LV3
        // write. The logged path forces `unlogged_commit_gate.write()` +
        // `flush()` per call, which (per `phaseA-diag` 2026-05-14)
        // serialises every concurrent unlogged writer behind a single
        // `checkpoint_unlogged_before_wal_commit` that can take up to
        // 17.6s. The DedupScanner calls this once per re-scanned LBA at
        // ~3 Hz, so a logged path here stalls the entire writer
        // pipeline. Crash safety: if this flag-clear is lost, the next
        // scanner cycle re-processes the LBA — idempotent.
        if self.unlogged_flush_commits {
            tx.commit_unlogged_with_outcomes()?;
        } else {
            tx.commit_with_outcomes()?;
        }
        Ok(())
    }

    pub(crate) fn atomic_dedup_hit(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        new_value: &BlockmapValue,
        _hash: &ContentHash,
        seq: u64,
    ) -> OnyxResult<Option<RemapCleanup>> {
        let ord = self.volume_ordinal(vol_id)?;
        let mut tx = self.db.begin();
        tx.l2p_remap(
            ord,
            lba.0,
            to_l2p_value_with_seq(new_value, seq),
            Some((to_metadb_pba(new_value.pba), 1)),
        );
        // Dedup-hit has no LV3 write — it just remaps an LBA onto an
        // existing PBA. Crash before the commit checkpoints rolls the
        // state back to "pre-hit": the LBA still points to its previous
        // PBA, refcounts unchanged. The buffer's pending entry stays
        // visible to the flusher and will be re-hashed on the next
        // cycle, so the worst case is a single missed dedup hit, not
        // data loss. Logged path would force `unlogged_commit_gate.write`
        // + `flush()` on every hit and serialise the entire writer
        // pipeline behind a metadb checkpoint.
        let (_, outcomes) = if self.unlogged_flush_commits {
            tx.commit_unlogged_with_outcomes()?
        } else {
            tx.commit_with_outcomes()?
        };
        let (newly_zeroed, _accepted) = newly_zeroed_from_remaps([*new_value], outcomes)?;
        Ok(newly_zeroed.into_iter().next().map(|(_, cleanup)| cleanup))
    }

    pub(crate) fn atomic_batch_dedup_hits(
        &self,
        vol_id: &VolumeId,
        hits: &[(Lba, BlockmapValue, ContentHash)],
    ) -> OnyxResult<(Vec<DedupHitResult>, HashMap<Pba, RemapCleanup>)> {
        self.atomic_batch_dedup_hits_with_promote(vol_id, hits, &[], &[])
    }

    pub(crate) fn atomic_batch_dedup_hits_with_promote(
        &self,
        vol_id: &VolumeId,
        hits: &[(Lba, BlockmapValue, ContentHash)],
        promote_entries: &[(ContentHash, DedupEntry)],
        seqs: &[u64],
    ) -> OnyxResult<(Vec<DedupHitResult>, HashMap<Pba, RemapCleanup>)> {
        debug_assert!(seqs.is_empty() || seqs.len() == hits.len());
        if hits.is_empty() && promote_entries.is_empty() {
            return Ok((Vec::new(), HashMap::new()));
        }
        let ord = self.volume_ordinal(vol_id)?;
        // Phase 5: per-WalOp apply runs on per-shard lanes
        // (`apply_l2p_bucket` for L2P, `apply_dedup_indices_to` for
        // dedup), so even within a single tx the L2pRemap may execute
        // before the paired DedupPut bumps rc. Hits whose target PBA
        // is being promoted **must not** carry an `rc >= 1` guard:
        // the target's rc is 0 before this tx, the lane racing the
        // dedup lane would observe rc=0 and reject the remap, and
        // pre-Phase-5 the hot-path L2pRemap itself drove rc so the
        // race didn't exist.
        //
        // Race-safety for the unguarded promote: the dedup pipeline
        // calls `candidate.remove_by_pba(pba)` before
        // `allocator.free(pba)` (see [`buffer::flush::cleanup`]), so
        // any candidate-cache hit on PBA `P` proves `P` has not yet
        // been retired — there is no "promote to a PBA already
        // freed" path to protect against.
        //
        // Non-promote hits (target already in `dedup_index`,
        // `rc >= 1` from the prior DedupPut) keep the guard as a
        // belt-and-suspenders defense against a concurrent
        // FreePbas-driven decref-to-zero.
        let promote_hashes: std::collections::HashSet<ContentHash> =
            promote_entries.iter().map(|(h, _)| *h).collect();
        let mut tx = self.db.begin();
        for (i, (lba, value, hash)) in hits.iter().enumerate() {
            let guard = if promote_hashes.contains(hash) {
                None
            } else {
                Some((to_metadb_pba(value.pba), 1))
            };
            tx.l2p_remap(
                ord,
                lba.0,
                to_l2p_value_with_seq(value, seq_for(seqs, i)),
                guard,
            );
        }
        // Promote candidate-cache hits into the persistent dedup
        // tables. Atomic with the LBA remaps: if the commit succeeds,
        // both happen; if it fails, neither. Use unguarded `put_dedup`
        // for the same reason the L2pRemap is unguarded above — the
        // candidate cache eviction contract closes the race window.
        for (hash, entry) in promote_entries {
            tx.put_dedup(*hash, to_dedup_value(entry));
        }
        // Same reasoning as `atomic_dedup_hit`: no LV3 write, refcount
        // guard inside metadb handles the race against a concurrent
        // decref-to-zero, and crash rolls back atomically. Routing
        // through the logged path forces a metadb `flush()` per call
        // (the `checkpoint_unlogged_before_wal_commit` step under
        // `unlogged_commit_gate.write`), pinning `commit_total_us` at
        // ~300ms per hit batch under skip=0 mixed workloads — see
        // docs/metadb-nvme-drain-plan.md for the bisect data.
        let (_, outcomes) = if self.unlogged_flush_commits {
            tx.commit_unlogged_with_outcomes()?
        } else {
            tx.commit_with_outcomes()?
        };
        dedup_hit_results_from_remaps(hits, outcomes)
    }
}

impl AsyncCheckpoint {
    fn start(db: Arc<Db>) -> OnyxResult<Self> {
        let state = Arc::new((Mutex::new(CheckpointState::default()), Condvar::new()));
        let worker_state = state.clone();
        let thread = std::thread::Builder::new()
            .name("metadb-checkpoint".into())
            .spawn(move || {
                crate::affinity::bind_current(crate::affinity::ThreadRole::MetadbCheckpoint, 0);
                loop {
                    let (start, target, force) = {
                        let (lock, cvar) = &*worker_state;
                        let mut state = lock.lock().unwrap();
                        while state.requested == state.completed && !state.shutdown {
                            state = cvar.wait(state).unwrap();
                        }
                        if state.shutdown && state.requested == state.completed {
                            return;
                        }
                        (
                            state.completed + 1,
                            state.requested,
                            state.force_requested > state.completed,
                        )
                    };

                    let result = if force {
                        db.flush().map(|_| true)
                    } else {
                        db.try_flush()
                    };
                    let (lock, cvar) = &*worker_state;
                    let mut state = lock.lock().unwrap();
                    match result {
                        Ok(true) => {
                            state.checkpointed = state.checkpointed.max(target);
                        }
                        Ok(false) => {
                            tracing::debug!(
                                start,
                                target,
                                "metadb checkpoint skipped; apply gate busy"
                            );
                        }
                        Err(err) => {
                            state.failures.push(CheckpointFailure {
                                start,
                                end: target,
                                message: err.to_string(),
                            });
                        }
                    }
                    state.completed = state.completed.max(target);
                    cvar.notify_all();
                }
            })
            .map_err(OnyxError::Io)?;
        Ok(Self {
            state,
            thread: Mutex::new(Some(thread)),
        })
    }

    fn request_async(&self) -> OnyxResult<()> {
        self.request().map(|_| ())
    }

    fn try_request_async(&self) -> OnyxResult<bool> {
        self.try_request_async_token().map(|token| token.is_some())
    }

    fn try_request_async_token(&self) -> OnyxResult<Option<u64>> {
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock().unwrap();
        if state.shutdown {
            return Err(OnyxError::Config(
                "metadb checkpoint worker is shutting down".into(),
            ));
        }
        if state.requested != state.completed {
            return Ok(None);
        }
        state.requested = state
            .requested
            .checked_add(1)
            .ok_or_else(|| OnyxError::Config("metadb checkpoint token overflow".into()))?;
        let token = state.requested;
        cvar.notify_one();
        Ok(Some(token))
    }

    fn checkpoint_outcome(&self, token: u64) -> OnyxResult<Option<bool>> {
        let (lock, _) = &*self.state;
        let state = lock.lock().unwrap();
        if let Some(failure) = state
            .failures
            .iter()
            .find(|failure| failure.start <= token && token <= failure.end)
        {
            return Err(OnyxError::Config(format!(
                "metadb checkpoint failed: {}",
                failure.message
            )));
        }
        if state.checkpointed >= token {
            return Ok(Some(true));
        }
        if state.completed >= token {
            return Ok(Some(false));
        }
        Ok(None)
    }

    fn sync(&self) -> OnyxResult<()> {
        let token = self.request()?;
        self.wait(token)
    }

    fn request(&self) -> OnyxResult<u64> {
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock().unwrap();
        if state.shutdown {
            return Err(OnyxError::Config(
                "metadb checkpoint worker is shutting down".into(),
            ));
        }
        state.requested = state
            .requested
            .checked_add(1)
            .ok_or_else(|| OnyxError::Config("metadb checkpoint token overflow".into()))?;
        let token = state.requested;
        state.force_requested = state.force_requested.max(token);
        cvar.notify_one();
        Ok(token)
    }

    fn wait(&self, token: u64) -> OnyxResult<()> {
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock().unwrap();
        while state.completed < token {
            state = cvar.wait(state).unwrap();
        }
        if let Some(failure) = state
            .failures
            .iter()
            .find(|failure| failure.start <= token && token <= failure.end)
        {
            return Err(OnyxError::Config(format!(
                "metadb checkpoint failed: {}",
                failure.message
            )));
        }
        Ok(())
    }
}

impl Drop for AsyncCheckpoint {
    fn drop(&mut self) {
        {
            let (lock, cvar) = &*self.state;
            let mut state = lock.lock().unwrap();
            state.shutdown = true;
            cvar.notify_all();
        }
        let handle = self.thread.lock().unwrap().take();
        if let Some(handle) = handle {
            let _ = handle.join();
        }
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
    cfg.lsm_bloom_bits_per_entry = config.lsm_bloom_bits_per_entry();
    cfg.group_commit_max_batch_bytes = 16 * 1024 * 1024;
    cfg.index_pin_bytes = config.index_pin_bytes() as u64;
    cfg.group_commit_timeout_us = config.group_commit_timeout_us();
    cfg.unlogged_commits_enabled = config.unlogged_flush_commits;
    cfg.dedup_shards = config.dedup_shards;
    cfg.shards_per_partition = config.shards_per_partition;
    cfg.dedup_cuckoo_buckets = config.dedup_cuckoo_buckets;
    cfg.dedup_l1_cache_entries = config.dedup_l1_cache_entries;
    cfg.refcount_drainer_enabled = config.refcount_drainer_enabled;
    cfg.refcount_drainer_interval_ms = config.refcount_drainer_interval_ms;
    cfg.refcount_drainer_threshold_entries = config.refcount_drainer_threshold_entries;
    cfg.refcount_drainer_max_entries_per_cycle = config.refcount_drainer_max_entries_per_cycle;
    cfg.refcount_drainer_alloc_run_size = config.refcount_drainer_alloc_run_size;
    cfg.refcount_drainer_backpressure_pages = config.refcount_drainer_backpressure_pages;
    // L2P streaming writeback: continuously seal dirty L2P pages and
    // write them through metadb's IoSubmitter outside `apply_gate.write()`.
    // Onyx defaults this on so checkpoint sample's gate-hold stays
    // bounded by lifecycle work, not by clone/seal of accumulated
    // dirty pages. Mirrors the runtime knobs exposed in `[meta]` so
    // operators can tune without rebuilding.
    cfg.l2p_writeback_enabled = config.l2p_writeback_enabled;
    cfg.l2p_writeback_idle_sleep_us = config.l2p_writeback_idle_sleep_us;
    cfg.l2p_writeback_min_dirty_pages = config.l2p_writeback_min_dirty_pages;
    cfg.l2p_writeback_max_pages_per_cycle = config.l2p_writeback_max_pages_per_cycle;
    cfg.flush_dirty_pages_target = config.flush_dirty_pages_target as usize;
    cfg.io_submitter_bg_inflight_cap = config.io_submitter_bg_inflight_cap as usize;
    cfg.l2p_buffer_enabled = config.l2p_buffer_enabled;
    cfg.l2p_buffer_soft_entries = config.l2p_buffer_soft_entries;
    cfg.l2p_buffer_hard_entries = config.l2p_buffer_hard_entries;
    cfg.l2p_buffer_max_interval_ms = config.l2p_buffer_max_interval_ms;
    cfg.flush_select_budget = config.flush_select_budget as usize;
    cfg.async_reclaim_enabled = config.async_reclaim_enabled;
    cfg.async_reclaim_max_pages_per_cycle = config.async_reclaim_max_pages_per_cycle as usize;
    cfg.async_reclaim_idle_interval_ms = config.async_reclaim_idle_interval_ms;
    // Onyx treats startup as a data-plane path. Full page-file scans are
    // available through offline metadb-verify, but should not gate service
    // restart on large metadata files.
    cfg.rebuild_free_list_on_open = false;
    cfg.reclaim_orphans_on_open = false;
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
) -> OnyxResult<(HashMap<Pba, RemapCleanup>, Vec<bool>)>
where
    I: IntoIterator<Item = BlockmapValue>,
{
    let new_values: Vec<BlockmapValue> = new_values.into_iter().collect();
    let mut new_iter = new_values.into_iter();
    let mut decrements: HashMap<Pba, RemapCleanup> = HashMap::new();
    let mut freed = std::collections::HashSet::new();
    let mut accepted: Vec<bool> = Vec::new();

    // Per-LBA handler shared by L2pRemap and L2pRemapRange outcomes —
    // semantics are identical at the LBA level (range op is just a
    // compact transport for the same decision table).
    let mut handle_lba = |new_value: BlockmapValue,
                          applied: bool,
                          prev: Option<L2pValue>,
                          decrements: &mut HashMap<Pba, RemapCleanup>|
     -> OnyxResult<()> {
        accepted.push(applied);
        if !applied {
            return Ok(());
        }
        let Some(prev) = prev else {
            return Ok(());
        };
        let old = decode_l2p_value(prev)?;
        if old.is_zero() || old.pba == new_value.pba {
            return Ok(());
        }
        let blocks = freed_blocks_for_l2p_value(&old);
        decrements
            .entry(old.pba)
            .and_modify(|entry| entry.merge(RemapCleanup::new(old, blocks)))
            .or_insert_with(|| RemapCleanup::new(old, blocks));
        Ok(())
    };

    for outcome in outcomes {
        match outcome {
            ApplyOutcome::L2pRemap {
                applied,
                prev,
                freed_pba,
            } => {
                let new_value = new_iter.next().ok_or_else(|| {
                    OnyxError::Config(
                        "metadb outcomes consumed more values than the batch produced".into(),
                    )
                })?;
                if let Some(pba) = freed_pba {
                    freed.insert(from_metadb_pba(pba));
                }
                handle_lba(new_value, applied, prev, &mut decrements)?;
            }
            ApplyOutcome::L2pRemapRange {
                applied,
                prevs,
                freed_pbas,
            } => {
                if applied.len() != prevs.len() {
                    return Err(OnyxError::Config(format!(
                        "metadb L2pRemapRange applied/prevs length mismatch: {} vs {}",
                        applied.len(),
                        prevs.len(),
                    )));
                }
                for pba in &freed_pbas {
                    freed.insert(from_metadb_pba(*pba));
                }
                for (i, app) in applied.iter().enumerate() {
                    let new_value = new_iter.next().ok_or_else(|| {
                        OnyxError::Config(
                            "metadb outcomes consumed more values than the batch produced".into(),
                        )
                    })?;
                    handle_lba(new_value, *app, prevs[i], &mut decrements)?;
                }
            }
            // The onyx adapter may append explicit refcount ops after the
            // remap ops to cover multi-PBA raw extents. Those outcomes do not
            // consume a batch value; freed old mappings are already reported
            // through the remap outcome's `freed_pba(s)` fields.
            ApplyOutcome::RefcountNew(_) => {}
            _ => {
                return Err(OnyxError::Config(
                    "metadb returned non-remap outcome for remap batch".into(),
                ));
            }
        }
    }

    if new_iter.next().is_some() {
        return Err(OnyxError::Config(
            "metadb outcomes consumed fewer values than the batch produced".into(),
        ));
    }

    for (pba, cleanup) in decrements.iter_mut() {
        cleanup.pba_freed = freed.contains(pba);
    }
    decrements.retain(|_, cleanup| cleanup.pba_freed);
    Ok((decrements, accepted))
}

/// Emit `tx.l2p_remap_range` for each maximal contiguous LBA run in
/// `batch_values`, mapping each LBA's `BlockmapValue` to its metadb
/// `L2pValue` (with the corresponding seq from `seqs`).
///
/// `seq_base` is the offset of `batch_values[0]` inside the caller's
/// flat seqs vec — used by `atomic_batch_write_multi_with_dedup` which
/// passes a shared seqs vec spanning multiple units.
///
/// In the common passthrough case (`batch_values` is one CompressedUnit
/// with strictly contiguous LBAs) this emits exactly one range op. A
/// defensive gap or sub-range cap split produces multiple range ops.
fn emit_l2p_remap_runs(
    tx: &mut Transaction<'_>,
    ord: VolumeOrdinal,
    batch_values: &[(Lba, BlockmapValue)],
    seqs: &[u64],
    seq_base: usize,
) {
    if batch_values.is_empty() {
        return;
    }
    let cap = onyx_metadb::wal::op::MAX_REMAP_RANGE_LBAS;
    let mut i = 0;
    while i < batch_values.len() {
        let run_start = i;
        let start_lba = batch_values[i].0;
        // Extend while next LBA is contiguous and run length is below the
        // metadb-side cap. The cap is a defensive bound (4096) far above
        // passthrough's natural ceiling (`coalesce_max_lbas`), but enforce
        // it here so a future caller can't trip the decoder's reject.
        while i + 1 < batch_values.len()
            && batch_values[i + 1].0 .0 == batch_values[i].0 .0 + 1
            && (i + 1 - run_start) < cap
        {
            i += 1;
        }
        let run_end_inclusive = i;
        let count = run_end_inclusive - run_start + 1;
        let mut values: Vec<L2pValue> = Vec::with_capacity(count);
        for (off, (_lba, value)) in batch_values[run_start..=run_end_inclusive]
            .iter()
            .enumerate()
        {
            values.push(to_l2p_value_with_seq(
                value,
                seq_for(seqs, seq_base + run_start + off),
            ));
        }
        tx.l2p_remap_range(ord, start_lba.0, values.into_boxed_slice());
        i += 1;
    }
}

fn dedup_hit_results_from_remaps(
    hits: &[(Lba, BlockmapValue, ContentHash)],
    outcomes: Vec<ApplyOutcome>,
) -> OnyxResult<(Vec<DedupHitResult>, HashMap<Pba, RemapCleanup>)> {
    // The tx queues `hits.len()` L2pRemap ops followed by promote-side
    // DedupPut ops. commit_with_outcomes returns one outcome per WAL op in
    // submission order, so the L2pRemap outcomes are exactly the first
    // `hits.len()` entries.
    if outcomes.len() < hits.len() {
        return Err(OnyxError::Config(format!(
            "metadb dedup outcome length mismatch: {} hits, {} outcomes",
            hits.len(),
            outcomes.len()
        )));
    }

    let mut results = Vec::with_capacity(hits.len());
    let mut old_mappings: HashMap<Pba, RemapCleanup> = HashMap::new();
    let mut freed = std::collections::HashSet::new();
    for ((_, new_value, _), outcome) in hits.iter().zip(outcomes.into_iter().take(hits.len())) {
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

        if let Some(pba) = freed_pba {
            freed.insert(from_metadb_pba(pba));
        }

        if let Some(prev) = prev {
            let old = decode_l2p_value(prev)?;
            if !old.is_zero() && old.pba != new_value.pba {
                let blocks = freed_blocks_for_l2p_value(&old);
                let cleanup = RemapCleanup::new(old, blocks);
                old_mappings
                    .entry(old.pba)
                    .and_modify(|entry| entry.merge(cleanup.clone()))
                    .or_insert(cleanup);
            }
        }
        results.push(DedupHitResult::Accepted(None));
    }
    for (pba, cleanup) in old_mappings.iter_mut() {
        cleanup.pba_freed = freed.contains(pba);
    }
    Ok((results, old_mappings))
}

pub(crate) fn to_metadb_pba(pba: Pba) -> onyx_metadb::Pba {
    pba.0
}

pub(crate) fn from_metadb_pba(pba: onyx_metadb::Pba) -> Pba {
    Pba(pba)
}

/// Sort + dedup + run-length compress a list of freed PBAs into the
/// minimum set of contiguous extents the allocator's retire path
/// expects. Input order is unconstrained; duplicates are collapsed.
pub(crate) fn coalesce_free_pbas_to_extents(pbas: &[Pba]) -> Vec<Extent> {
    if pbas.is_empty() {
        return Vec::new();
    }
    let mut sorted: Vec<u64> = pbas.iter().map(|p| p.0).collect();
    sorted.sort_unstable();
    sorted.dedup();

    let mut out = Vec::new();
    let mut run_start = sorted[0];
    let mut run_len: u64 = 1;
    for &pba in &sorted[1..] {
        if pba == run_start + run_len {
            run_len += 1;
        } else {
            out.push(Extent::new(Pba(run_start), run_len as u32));
            run_start = pba;
            run_len = 1;
        }
    }
    out.push(Extent::new(Pba(run_start), run_len as u32));
    out
}

pub(crate) fn to_l2p_value(value: &BlockmapValue) -> L2pValue {
    L2pValue(blockmap_to_l2p_bytes(value))
}

pub(crate) fn to_l2p_value_with_seq(value: &BlockmapValue, seq: u64) -> L2pValue {
    L2pValue(blockmap_to_l2p_bytes_with_seq(value, seq))
}

#[inline]
fn seq_for(seqs: &[u64], i: usize) -> u64 {
    if seqs.is_empty() {
        0
    } else {
        seqs[i]
    }
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
            index_pin_mb: 64,
            lsm_bloom_bits_per_entry: 10,
            checkpoint_interval_ms: 5000,
            group_commit_timeout_us: 1,
            wal_dir: None,
            dedup_shards: 1,
            dedup_cuckoo_buckets: 1_000_000,
            dedup_l1_cache_entries: 256_000,
            ..Default::default()
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
            index_pin_mb: 64,
            lsm_bloom_bits_per_entry: 10,
            checkpoint_interval_ms: 5000,
            group_commit_timeout_us: 1,
            wal_dir: None,
            dedup_shards: 1,
            dedup_cuckoo_buckets: 1_000_000,
            dedup_l1_cache_entries: 256_000,
            ..Default::default()
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
            backend
                .multi_get_mappings_ord(ord, &[Lba(2), Lba(3)])
                .unwrap(),
            vec![None, Some(value)]
        );
        assert_eq!(
            backend.get_mappings_range(&vol.id, Lba(0), Lba(8)).unwrap(),
            vec![(Lba(3), value)]
        );
        assert_eq!(
            backend
                .get_mappings_range_unordered_ord(ord, Lba(0), Lba(8))
                .unwrap(),
            vec![(Lba(3), value)]
        );
    }

    #[test]
    fn atomic_batch_write_updates_refcounts_and_reports_freed_pba() {
        // Phase 5: hot-path `atomic_batch_write` no longer mutates
        // global rc, and the L2pRemap outcome's `freed_pba` is always
        // None. `newly_zeroed_from_remaps` filters its decrements map
        // to only `pba_freed=true` entries — so the returned cleanups
        // map is empty even when an old PBA was logically overwritten.
        // The retire flow now runs via the dead-list → Lineage GC →
        // FreePbas channel (not exercised here).
        let dir = tempfile::tempdir().unwrap();
        let meta = MetaConfig {
            path: Some(dir.path().to_path_buf()),
            block_cache_mb: 8,
            memtable_budget_mb: 64,
            index_pin_mb: 64,
            lsm_bloom_bits_per_entry: 10,
            checkpoint_interval_ms: 5000,
            group_commit_timeout_us: 1,
            wal_dir: None,
            dedup_shards: 1,
            dedup_cuckoo_buckets: 1_000_000,
            dedup_l1_cache_entries: 256_000,
            ..Default::default()
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
        assert_eq!(backend.get_refcount(Pba(10)).unwrap(), 0);

        let freed = backend
            .atomic_batch_write(&vol.id, &[(Lba(0), new), (Lba(1), new)], 2)
            .unwrap();

        assert_eq!(backend.get_refcount(Pba(10)).unwrap(), 0);
        assert_eq!(backend.get_refcount(Pba(20)).unwrap(), 0);
        assert!(
            freed.is_empty(),
            "Phase 5: L2pRemap surfaces no freed_pba on the hot path; cleanups map empty"
        );
        // L2P state moved to the new mapping.
        let m = backend.get_mapping(&vol.id, Lba(0)).unwrap().unwrap();
        assert_eq!(m.pba, Pba(20));
    }

    /// Sanity check that passthrough's range-emission helper turns a
    /// contiguous batch into one range op and persists every LBA
    /// correctly. The metadb-side apply test
    /// (`l2p_remap_range_writes_each_lba_and_increfs_distinct_pbas`)
    /// already validates the apply lane; this test guards the onyx
    /// glue: helper construction, ordinal lookup, refcount aggregation,
    /// freed-pba decoder.
    #[test]
    fn atomic_batch_write_range_contiguous_lbas() {
        let dir = tempfile::tempdir().unwrap();
        let meta = MetaConfig {
            path: Some(dir.path().to_path_buf()),
            block_cache_mb: 8,
            memtable_budget_mb: 64,
            index_pin_mb: 64,
            lsm_bloom_bits_per_entry: 10,
            checkpoint_interval_ms: 5000,
            group_commit_timeout_us: 1,
            wal_dir: None,
            dedup_shards: 1,
            dedup_cuckoo_buckets: 1_000_000,
            dedup_l1_cache_entries: 256_000,
            ..Default::default()
        };
        let vol = VolumeConfig {
            id: VolumeId("vol-a".to_string()),
            size_bytes: 4096 * 32,
            block_size: 4096,
            compression: CompressionAlgo::Lz4,
            created_at: 10,
            zone_count: 4,
        };
        let backend = MetadbBackend::open(&meta).unwrap();
        backend.put_volume(&vol).unwrap();

        // 8 LBAs starting at LBA 0, each pointing at a distinct PBA so we
        // can verify per-LBA writes survive the range emission.
        let batch: Vec<(Lba, BlockmapValue)> = (0..8u64)
            .map(|i| {
                let v = BlockmapValue {
                    pba: Pba(100 + i),
                    compression: 1,
                    unit_compressed_size: 4096,
                    unit_original_size: 4096,
                    unit_lba_count: 1,
                    offset_in_unit: 0,
                    crc32: i as u32,
                    slot_offset: 0,
                    flags: 0,
                };
                (Lba(i), v)
            })
            .collect();
        backend.atomic_batch_write(&vol.id, &batch, 1).unwrap();
        for i in 0..8u64 {
            let m = backend.get_mapping(&vol.id, Lba(i)).unwrap().unwrap();
            assert_eq!(m.pba, Pba(100 + i));
            // Phase 5: hot-path atomic_batch_write doesn't touch rc.
            assert_eq!(backend.get_refcount(Pba(100 + i)).unwrap(), 0);
        }
    }

    /// LBAs with a gap split into two range ops. Both must commit and
    /// the outcomes decoder must walk new_values in order across both
    /// outcomes — a regression here would either crash on the
    /// "outcomes consumed more/fewer values than the batch produced"
    /// error or lose the second range's mappings.
    #[test]
    fn atomic_batch_write_range_with_gap_splits_into_two_ops() {
        let dir = tempfile::tempdir().unwrap();
        let meta = MetaConfig {
            path: Some(dir.path().to_path_buf()),
            block_cache_mb: 8,
            memtable_budget_mb: 64,
            index_pin_mb: 64,
            lsm_bloom_bits_per_entry: 10,
            checkpoint_interval_ms: 5000,
            group_commit_timeout_us: 1,
            wal_dir: None,
            dedup_shards: 1,
            dedup_cuckoo_buckets: 1_000_000,
            dedup_l1_cache_entries: 256_000,
            ..Default::default()
        };
        let vol = VolumeConfig {
            id: VolumeId("vol-a".to_string()),
            size_bytes: 4096 * 32,
            block_size: 4096,
            compression: CompressionAlgo::Lz4,
            created_at: 10,
            zone_count: 4,
        };
        let backend = MetadbBackend::open(&meta).unwrap();
        backend.put_volume(&vol).unwrap();

        let bv = |pba: u64, crc: u32| BlockmapValue {
            pba: Pba(pba),
            compression: 1,
            unit_compressed_size: 4096,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: crc,
            slot_offset: 0,
            flags: 0,
        };

        // Two contiguous runs separated by a gap: [0..3) and [10..12).
        let batch = vec![
            (Lba(0), bv(200, 0)),
            (Lba(1), bv(201, 1)),
            (Lba(2), bv(202, 2)),
            (Lba(10), bv(210, 10)),
            (Lba(11), bv(211, 11)),
        ];
        backend.atomic_batch_write(&vol.id, &batch, 1).unwrap();

        for i in 0..3u64 {
            assert_eq!(
                backend.get_mapping(&vol.id, Lba(i)).unwrap().unwrap().pba,
                Pba(200 + i),
            );
        }
        for (lba, pba) in [(10u64, 210u64), (11, 211)] {
            assert_eq!(
                backend.get_mapping(&vol.id, Lba(lba)).unwrap().unwrap().pba,
                Pba(pba),
            );
        }
        assert!(backend.get_mapping(&vol.id, Lba(5)).unwrap().is_none());
    }

    #[test]
    fn dedup_entries_and_flag_scan_round_trip() {
        // Phase 5: `dedup_entry_is_live` checks rc(entry.pba)>0 to
        // catch entries pointing at PBAs whose final decref already
        // landed. Hot-path L2pRemap no longer maintains rc, so we
        // seed rc explicitly via the test helper to mirror the
        // production state shape (a dedup index entry is only put
        // for a verified-shared PBA whose rc was bumped by a
        // promotion or prior put_dedup).
        let dir = tempfile::tempdir().unwrap();
        let meta = MetaConfig {
            path: Some(dir.path().to_path_buf()),
            block_cache_mb: 8,
            memtable_budget_mb: 64,
            index_pin_mb: 64,
            lsm_bloom_bits_per_entry: 10,
            checkpoint_interval_ms: 5000,
            group_commit_timeout_us: 1,
            wal_dir: None,
            dedup_shards: 1,
            dedup_cuckoo_buckets: 1_000_000,
            dedup_l1_cache_entries: 256_000,
            ..Default::default()
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
        backend.set_refcount(value.pba, 1).unwrap(); // Phase 5 rc seed

        let hash = [9u8; 8];
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
        // Phase 5: hot-path L2pRemap doesn't touch rc, so the
        // replacement leaves rc(value.pba) at its prior value. The
        // earlier `put_dedup_entries` issued a `WalOp::DedupPut`
        // whose apply increfs the new head_pba by 1 (rc 1 → 2). Old
        // mapping cleanup of the displaced PBA flows through the
        // dead-list / Lineage GC retire path, not the hot path.
        assert_eq!(backend.get_refcount(value.pba).unwrap(), 2);
    }

    #[test]
    fn delete_range_and_volume_report_freed_extents() {
        let dir = tempfile::tempdir().unwrap();
        let meta = MetaConfig {
            path: Some(dir.path().to_path_buf()),
            block_cache_mb: 8,
            memtable_budget_mb: 64,
            index_pin_mb: 64,
            lsm_bloom_bits_per_entry: 10,
            checkpoint_interval_ms: 5000,
            group_commit_timeout_us: 1,
            wal_dir: None,
            dedup_shards: 1,
            dedup_cuckoo_buckets: 1_000_000,
            dedup_l1_cache_entries: 256_000,
            ..Default::default()
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
        // Phase 5: hot-path atomic_batch_write doesn't bump rc, but
        // delete_blockmap_range / delete_volume still decref. Seed rc
        // to mirror the post-Phase-5 production shape (lineage events
        // are the ones bumping rc).
        backend.set_refcount(Pba(100), 1).unwrap();
        backend.set_refcount(Pba(200), 2).unwrap();

        let freed = backend
            .delete_blockmap_range(&vol.id, Lba(1), Lba(3))
            .unwrap();
        assert_eq!(freed.len(), 1);
        assert_eq!(freed[0].pba, Pba(200));
        assert_eq!(freed[0].blocks, 1);
        assert!(freed[0].pba_freed);
        assert_eq!(backend.count_blockmap_refs_for_pba(Pba(100)).unwrap(), 1);

        let freed = backend.delete_volume(&vol.id).unwrap();
        assert_eq!(freed.len(), 1);
        assert_eq!(freed[0].pba, Pba(100));
        assert_eq!(freed[0].blocks, 1);
        assert!(freed[0].pba_freed);
        assert!(backend.get_volume(&vol.id).unwrap().is_none());
    }

    #[test]
    fn diagnostic_helpers_track_refs_and_allocated_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let meta = MetaConfig {
            path: Some(dir.path().to_path_buf()),
            block_cache_mb: 8,
            memtable_budget_mb: 64,
            index_pin_mb: 64,
            lsm_bloom_bits_per_entry: 10,
            checkpoint_interval_ms: 5000,
            group_commit_timeout_us: 1,
            wal_dir: None,
            dedup_shards: 1,
            dedup_cuckoo_buckets: 1_000_000,
            dedup_l1_cache_entries: 256_000,
            ..Default::default()
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
        // Phase 5: exclusive PBAs (no dedup_index entry) never get
        // their global rc bumped on write — only lineage events
        // (clone promotion + drop_volume) touch rc.
        assert_eq!(backend.iter_refcounts().unwrap(), Vec::<(Pba, u32)>::new());
        assert_eq!(backend.iter_allocated_blocks().unwrap(), vec![(Pba(55), 1)]);
        assert!(backend.has_any_blockmap_ref(Pba(55)).unwrap());

        backend.delete_mapping(&vol.id, Lba(0)).unwrap();
        assert_eq!(backend.get_refcount(Pba(55)).unwrap(), 0);
    }

    /// Documents the underlying metadb behaviour the fix relies on
    /// **avoiding**: any `l2p_remap` whose encoded `L2pValue` carries
    /// seq=0 silently passes `seq_guard_rejects` regardless of the
    /// stored seq, because the guard short-circuits on `new_seq == 0`
    /// (`metadb/src/db/apply.rs::seq_guard_rejects`).
    ///
    /// This was the P0 mapping-loss vector. Before the fix,
    /// `update_blockmap_flags` emitted seq=0 (via `to_l2p_value` ->
    /// `blockmap_to_l2p_bytes_with_seq(v, 0)`), so a `DedupScanner`
    /// flag-clear that raced a buffer-flusher commit would clobber the
    /// newer mapping back to a stale PBA. Production fio crc32c verify
    /// (`tier2b-stage1-verify-20260516T151612Z`) saw 84 silent verify
    /// errors and read_path.unmapped=45M.
    ///
    /// The fix is at the *caller* layer: `update_blockmap_flags` now
    /// carries the observed seq forward so apply's `seq_guard_rejects`
    /// can actually reject losing-the-race cases. This test pins the
    /// underlying metadb behaviour so a future contributor doesn't
    /// re-introduce a seq=0 emitter assuming the guard catches it.
    #[test]
    fn metadb_seq0_in_l2p_remap_bypasses_guard_and_clobbers_newer_write() {
        let dir = tempfile::tempdir().unwrap();
        let meta = MetaConfig {
            path: Some(dir.path().to_path_buf()),
            block_cache_mb: 8,
            memtable_budget_mb: 64,
            index_pin_mb: 64,
            lsm_bloom_bits_per_entry: 10,
            checkpoint_interval_ms: 5000,
            group_commit_timeout_us: 1,
            wal_dir: None,
            dedup_shards: 1,
            dedup_cuckoo_buckets: 1_000_000,
            dedup_l1_cache_entries: 256_000,
            ..Default::default()
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

        // Newer write commits at seq=200, pba=P2.
        let bv_new = BlockmapValue {
            pba: Pba(40),
            compression: 1,
            unit_compressed_size: 2048,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 0xBBBB_BBBB,
            slot_offset: 0,
            flags: 0,
        };
        backend
            .atomic_batch_write_with_dedup(&vol.id, &[(Lba(0), bv_new)], 1, &[], &[200])
            .unwrap();
        assert_eq!(
            backend.get_mapping(&vol.id, Lba(0)).unwrap().unwrap().pba,
            Pba(40)
        );

        // A stale seq=0 update (simulating the pre-fix `update_blockmap_flags`
        // path, or any other caller that omits the seq) directly via the
        // dedup batch API — empty `seqs` slice -> seq_for returns 0.
        let stale_bv = BlockmapValue {
            pba: Pba(30),
            crc32: 0xAAAA_AAAA,
            ..bv_new
        };
        backend
            .atomic_batch_write_with_dedup(&vol.id, &[(Lba(0), stale_bv)], 1, &[], &[])
            .unwrap();

        // `seq_guard_rejects(0, _)` returns false, so apply accepts the
        // stale update unconditionally. The newer write is gone. Refcount
        // on the freed P2 drops to 0 -> allocator-reclaimable -> data loss.
        let after = backend.get_mapping(&vol.id, Lba(0)).unwrap().unwrap();
        assert_eq!(
            after.pba,
            Pba(30),
            "seq=0 sentinel must bypass seq_guard at the metadb layer; \
             callers must not emit seq=0 with this race shape"
        );
        assert_eq!(after.crc32, 0xAAAA_AAAA);
        // Phase 5: hot-path atomic_batch_write_with_dedup no longer
        // moves global rc, so both PBAs are observed at 0. The L2P
        // clobber (the actual data-loss vector under test) is still
        // there — that's the load-bearing assertion above.
        assert_eq!(backend.get_refcount(Pba(40)).unwrap(), 0);
        assert_eq!(backend.get_refcount(Pba(30)).unwrap(), 0);
    }

    /// Regression test for the fix: `update_blockmap_flags` must carry
    /// the observed L2pValue seq through to its `l2p_remap` so apply's
    /// `seq_guard_rejects` can guard against the scanner-versus-flusher
    /// race documented above. The pre-fix path emitted seq=0
    /// unconditionally and silently won races it should have lost.
    #[test]
    fn update_blockmap_flags_preserves_observed_seq() {
        let dir = tempfile::tempdir().unwrap();
        let meta = MetaConfig {
            path: Some(dir.path().to_path_buf()),
            block_cache_mb: 8,
            memtable_budget_mb: 64,
            index_pin_mb: 64,
            lsm_bloom_bits_per_entry: 10,
            checkpoint_interval_ms: 5000,
            group_commit_timeout_us: 1,
            wal_dir: None,
            dedup_shards: 1,
            dedup_cuckoo_buckets: 1_000_000,
            dedup_l1_cache_entries: 256_000,
            ..Default::default()
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

        // Initial write at seq=100 with DEDUP_SKIPPED.
        let bv = BlockmapValue {
            pba: Pba(30),
            compression: 1,
            unit_compressed_size: 2048,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 0xAAAA_AAAA,
            slot_offset: 0,
            flags: FLAG_DEDUP_SKIPPED,
        };
        backend
            .atomic_batch_write_with_dedup(&vol.id, &[(Lba(0), bv)], 1, &[], &[100])
            .unwrap();
        let ord = backend.volume_ordinal(&vol.id).unwrap();
        assert_eq!(backend.db.get(ord, 0).unwrap().unwrap().seq(), 100);

        // The flag-clear path.
        backend.update_blockmap_flags(&vol.id, Lba(0), 0).unwrap();

        // Flags cleared, PBA preserved, and — critically — the seq is
        // still 100. Pre-fix the seq would be 0, which is the sentinel
        // value that bypasses seq_guard.
        let raw_after = backend.db.get(ord, 0).unwrap().unwrap();
        assert_eq!(
            raw_after.seq(),
            100,
            "update_blockmap_flags must preserve the observed seq so apply's \
             seq_guard can reject losing-the-race callers"
        );
        let bv_after = blockmap_from_l2p_bytes(&raw_after.0).unwrap();
        assert_eq!(bv_after.flags, 0);
        assert_eq!(bv_after.pba, Pba(30));
        assert_eq!(bv_after.crc32, 0xAAAA_AAAA);

        // A concurrent newer write at seq=200 must win against a
        // subsequent flag-clear that observed the seq=100 state. Here
        // we encode that interleaving explicitly: commit the newer
        // write first (so update_blockmap_flags will see it and
        // early-return on matching flags), then a fresh DEDUP_SKIPPED
        // write at seq=300, then update_blockmap_flags. The new
        // observed seq (300) is preserved and the apply accepts.
        let bv_newer = BlockmapValue {
            pba: Pba(40),
            crc32: 0xBBBB_BBBB,
            flags: FLAG_DEDUP_SKIPPED,
            ..bv
        };
        backend
            .atomic_batch_write_with_dedup(&vol.id, &[(Lba(0), bv_newer)], 1, &[], &[300])
            .unwrap();
        backend.update_blockmap_flags(&vol.id, Lba(0), 0).unwrap();
        let raw_after = backend.db.get(ord, 0).unwrap().unwrap();
        assert_eq!(raw_after.seq(), 300);
        let bv_after = blockmap_from_l2p_bytes(&raw_after.0).unwrap();
        assert_eq!(bv_after.flags, 0);
        assert_eq!(bv_after.pba, Pba(40));
    }

    #[test]
    fn coalesce_free_pbas_empty_is_empty() {
        assert!(coalesce_free_pbas_to_extents(&[]).is_empty());
    }

    #[test]
    fn coalesce_free_pbas_merges_contiguous_runs() {
        let pbas = vec![Pba(10), Pba(11), Pba(12), Pba(20), Pba(21), Pba(50)];
        let extents = coalesce_free_pbas_to_extents(&pbas);
        assert_eq!(
            extents,
            vec![
                Extent::new(Pba(10), 3),
                Extent::new(Pba(20), 2),
                Extent::new(Pba(50), 1),
            ]
        );
    }

    #[test]
    fn coalesce_free_pbas_sorts_and_dedups_unsorted_input() {
        // Reordered + duplicates: walker may emit the same PBA more than
        // once across overlapping segments. Coalesce must collapse them.
        let pbas = vec![
            Pba(21),
            Pba(10),
            Pba(11),
            Pba(12),
            Pba(20),
            Pba(10),
            Pba(21),
            Pba(11),
        ];
        let extents = coalesce_free_pbas_to_extents(&pbas);
        assert_eq!(
            extents,
            vec![Extent::new(Pba(10), 3), Extent::new(Pba(20), 2)]
        );
    }

    #[test]
    fn coalesce_free_pbas_singleton() {
        let extents = coalesce_free_pbas_to_extents(&[Pba(7)]);
        assert_eq!(extents, vec![Extent::new(Pba(7), 1)]);
    }

    #[test]
    fn drain_lineage_freed_pbas_returns_empty_when_idle() {
        let dir = tempfile::tempdir().unwrap();
        let meta = MetaConfig {
            path: Some(dir.path().to_path_buf()),
            block_cache_mb: 8,
            memtable_budget_mb: 64,
            index_pin_mb: 64,
            lsm_bloom_bits_per_entry: 10,
            checkpoint_interval_ms: 5000,
            group_commit_timeout_us: 1,
            wal_dir: None,
            dedup_shards: 1,
            dedup_cuckoo_buckets: 1_000_000,
            dedup_l1_cache_entries: 256_000,
            ..Default::default()
        };
        let backend = MetadbBackend::open(&meta).unwrap();
        assert!(backend.drain_lineage_freed_pbas().is_empty());
    }

    /// End-to-end: simulate metadb's GC dispatching a freed-PBA outcome
    /// through the sink. The sink ingests via the cloned sender (mirrors
    /// the path metadb's GC driver thread takes), and `drain_lineage_freed_pbas`
    /// returns the queued PBAs in arrival order.
    #[test]
    fn drain_lineage_freed_pbas_returns_dispatched_outcomes() {
        let dir = tempfile::tempdir().unwrap();
        let meta = MetaConfig {
            path: Some(dir.path().to_path_buf()),
            block_cache_mb: 8,
            memtable_budget_mb: 64,
            index_pin_mb: 64,
            lsm_bloom_bits_per_entry: 10,
            checkpoint_interval_ms: 5000,
            group_commit_timeout_us: 1,
            wal_dir: None,
            dedup_shards: 1,
            dedup_cuckoo_buckets: 1_000_000,
            dedup_l1_cache_entries: 256_000,
            ..Default::default()
        };
        let backend = MetadbBackend::open(&meta).unwrap();
        // Simulate the sink firing. In production this is invoked by the
        // closure registered with `Db::set_freed_pbas_sink` inside
        // `MetadbBackend::open`; here we drive the channel directly so the
        // test does not depend on a fully wired GC cycle and the
        // lineage_gc_emit_freepbas flag.
        let tx = backend.lineage_freed_pbas_sender();
        tx.send(Pba(101)).unwrap();
        tx.send(Pba(102)).unwrap();
        tx.send(Pba(200)).unwrap();

        let drained = backend.drain_lineage_freed_pbas();
        assert_eq!(drained, vec![Pba(101), Pba(102), Pba(200)]);
        // Second drain is empty — channel is fully consumed.
        assert!(backend.drain_lineage_freed_pbas().is_empty());

        let extents = coalesce_free_pbas_to_extents(&drained);
        assert_eq!(
            extents,
            vec![Extent::new(Pba(101), 2), Extent::new(Pba(200), 1)]
        );
    }
}
