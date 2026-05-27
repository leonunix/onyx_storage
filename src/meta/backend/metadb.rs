use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crossbeam_channel::{unbounded, Receiver, Sender};
use dashmap::DashMap;
use onyx_metadb::{
    Config as MetaDbConfig, Db, DedupValue, DeferredOutcomeHandle, Lsn, VolumeOrdinal,
};

use crate::config::MetaConfig;
use crate::error::{OnyxError, OnyxResult};
use crate::meta::schema::{BlockmapValue, ContentHash, DedupEntry, FLAG_DEDUP_SKIPPED};
use crate::meta::store::{DedupHitResult, RemapCleanup};
use crate::metrics::MetaMemorySnapshot;
use crate::types::{Lba, Pba, VolumeConfig, VolumeId};

use super::codec::{blockmap_from_l2p_bytes, freed_blocks_for_l2p_value};

const METADB_DEDUP_VALUE_BYTES: usize = 28;
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

mod catalog;
mod checkpoint;
mod values;

use catalog::{VolumeCatalog, VolumeCatalogEntry, CATALOG_FILE};
use checkpoint::AsyncCheckpoint;
pub(crate) use values::coalesce_free_pbas_to_extents;
use values::{
    decode_dedup_value, decode_l2p_value, dedup_hit_results_from_remaps, emit_l2p_remap_runs,
    from_l2p_value, from_metadb_pba, newly_zeroed_from_remaps, seq_for, to_dedup_value,
    to_l2p_value, to_l2p_value_with_seq, to_metadb_pba,
};

/// ZFS-TXG-clone Phase 2 onyx-side wrapper around a metadb
/// [`DeferredOutcomeHandle`]. Carries the `new_values` slice the
/// caller had buffered so that the cleanup tuple
/// `(HashMap<Pba, RemapCleanup>, Vec<bool>)` can be assembled inside
/// `recv()` after the deferred outcomes arrive.
///
/// Two construction modes:
///   * `ready(value)` — outcome already known (empty batch / unlogged
///     fallback). `recv` returns immediately.
///   * `wrap(handle, new_values, remap_count)` — outcomes arrive
///     later via the metadb compactor's per-pass drain.
pub(crate) struct DeferredCleanupHandle {
    state: DeferredCleanupHandleState,
}

enum DeferredCleanupHandleState {
    Ready(OnyxResult<(HashMap<Pba, RemapCleanup>, Vec<bool>)>),
    Pending {
        inner: DeferredOutcomeHandle,
        new_values: Vec<BlockmapValue>,
        remap_count: usize,
    },
}

impl DeferredCleanupHandle {
    /// Build a handle whose cleanup tuple is already known. Used by
    /// the empty-batch and unlogged-flush paths inside the metadb
    /// adapter so all entry points can return `DeferredCleanupHandle`
    /// uniformly.
    pub(crate) fn ready(
        value: OnyxResult<(HashMap<Pba, RemapCleanup>, Vec<bool>)>,
    ) -> Self {
        Self {
            state: DeferredCleanupHandleState::Ready(value),
        }
    }

    /// Wrap a metadb deferred-outcome handle plus the `new_values`
    /// slice the caller pushed before issuing the commit. `recv()`
    /// will block on the metadb handle, then run
    /// `newly_zeroed_from_remaps` to materialise the cleanup tuple.
    pub(crate) fn wrap(
        inner: DeferredOutcomeHandle,
        new_values: Vec<BlockmapValue>,
        remap_count: usize,
    ) -> Self {
        Self {
            state: DeferredCleanupHandleState::Pending {
                inner,
                new_values,
                remap_count,
            },
        }
    }

    /// Resolve the cleanup tuple. Consumes the handle.
    pub(crate) fn recv(self) -> OnyxResult<(HashMap<Pba, RemapCleanup>, Vec<bool>)> {
        match self.state {
            DeferredCleanupHandleState::Ready(value) => value,
            DeferredCleanupHandleState::Pending {
                inner,
                new_values,
                remap_count,
            } => {
                let outcomes = inner.recv().map_err(|err| {
                    OnyxError::Config(format!("metadb deferred outcome recv failed: {err}"))
                })?;
                newly_zeroed_from_remaps(
                    new_values,
                    outcomes.into_iter().take(remap_count).collect::<Vec<_>>(),
                )
            }
        }
    }

    /// Non-consuming readiness probe. `true` when `recv` would
    /// resolve without blocking — either because the handle was
    /// constructed via [`Self::ready`] or because the metadb
    /// compactor has already released the staged outcome.
    pub(crate) fn is_ready(&self) -> bool {
        match &self.state {
            DeferredCleanupHandleState::Ready(_) => true,
            DeferredCleanupHandleState::Pending { inner, .. } => inner.is_ready(),
        }
    }

    /// Non-blocking probe; consumes the handle on `Ok`, returns it
    /// back on `Err(self)` if the metadb compactor has not yet
    /// released the staged outcome. Reserved for future opportunistic
    /// drain paths in `commit_worker/passthrough.rs`; the current
    /// pipeline relies on `recv()` only.
    #[allow(dead_code)]
    pub(crate) fn try_recv(self) -> Result<OnyxResult<(HashMap<Pba, RemapCleanup>, Vec<bool>)>, Self>
    {
        match self.state {
            DeferredCleanupHandleState::Ready(value) => Ok(value),
            DeferredCleanupHandleState::Pending {
                inner,
                new_values,
                remap_count,
            } => match inner.try_recv() {
                Ok(outcomes_result) => {
                    let outcomes = match outcomes_result {
                        Ok(outcomes) => outcomes,
                        Err(err) => {
                            return Ok(Err(OnyxError::Config(format!(
                                "metadb deferred outcome recv failed: {err}"
                            ))));
                        }
                    };
                    Ok(newly_zeroed_from_remaps(
                        new_values,
                        outcomes.into_iter().take(remap_count).collect::<Vec<_>>(),
                    ))
                }
                Err(inner) => Err(Self {
                    state: DeferredCleanupHandleState::Pending {
                        inner,
                        new_values,
                        remap_count,
                    },
                }),
            },
        }
    }

    /// Return the metadb-side LSN this commit was assigned. Available
    /// before `recv()` resolves so callers can sequence per-volume
    /// FIFO without waiting for outcome delivery. `None` for `Ready`
    /// handles whose cleanup work happened inline.
    #[allow(dead_code)]
    pub(crate) fn lsn(&self) -> Option<Lsn> {
        match &self.state {
            DeferredCleanupHandleState::Pending { inner, .. } => Some(inner.lsn()),
            DeferredCleanupHandleState::Ready(_) => None,
        }
    }
}

impl MetadbBackend {
    pub(crate) fn open(config: &MetaConfig) -> OnyxResult<Self> {
        let path = config.path().ok_or_else(|| {
            OnyxError::Config("meta.path is required to open metadb backend".into())
        })?;
        fs::create_dir_all(path)?;

        let db_config = metadb_config_from_onyx(path, config);
        // metadb's `open_with_config` / `create_with_config` return
        // `Arc<Db>` directly (Phase 4 Step 8: needed so the
        // `TxgSyncThread`'s `sync_work` callback can capture
        // `Weak<Db>` without circular shutdown). No extra `Arc::new`
        // wrap.
        let db = if path.join(METADB_PAGE_FILE).exists() {
            Db::open_with_config(db_config)?
        } else {
            Db::create_with_config(db_config)?
        };

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
    /// L2P-shard routing helper for the commit_worker pre-shard path
    /// (see `buffer/flush/writer/commit_worker/passthrough.rs::
    /// commit_passthrough_chunk`). Delegates to metadb so the
    /// client-side bucketing stays in lockstep with apply-side
    /// `shard_for_key_l2p` — divergence would cause sub-commits to
    /// claim L2P shards they don't actually touch.
    pub(crate) fn l2p_shard_of(&self, lba: Lba) -> usize {
        self.db.l2p_shard_for(lba.0)
    }

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
        self.atomic_batch_write_multi_with_dedup_deferred(units, dedup_entries, seqs)?
            .recv()
    }

    /// ZFS-TXG-clone Phase 2: deferred counterpart to
    /// [`Self::atomic_batch_write_multi_with_dedup`]. Issues the
    /// commit and returns a handle that resolves to the same
    /// `(HashMap<Pba, RemapCleanup>, Vec<bool>)` tuple — synchronously
    /// when the metadb-side flag is off (handle pre-populated), at
    /// the next L2P compactor pass otherwise.
    ///
    /// Onyx commit_worker can pipeline multiple in-flight commits per
    /// volume by issuing several `_deferred` calls before draining
    /// their handles; the sync wrapper above is the back-compat
    /// entry point (`recv()` immediately on the handle).
    pub(crate) fn atomic_batch_write_multi_with_dedup_deferred(
        &self,
        units: &[(&VolumeId, &[(Lba, BlockmapValue)], u32)],
        dedup_entries: &[(ContentHash, DedupEntry)],
        seqs: &[u64],
    ) -> OnyxResult<DeferredCleanupHandle> {
        let total_lbas: usize = units.iter().map(|(_, b, _)| b.len()).sum();
        debug_assert!(seqs.is_empty() || seqs.len() == total_lbas);
        if units.is_empty() {
            self.put_dedup_entries(dedup_entries)?;
            return Ok(DeferredCleanupHandle::ready(Ok((HashMap::new(), Vec::new()))));
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
        // The unlogged-flush path stays sync (no metadb-side
        // deferred-outcome support for unlogged commits today). Wrap
        // its result inline into a `Ready` cleanup handle so call
        // sites can route through the deferred surface uniformly.
        if self.unlogged_flush_commits {
            let (_, outcomes) = tx.commit_unlogged_with_outcomes()?;
            let remap_count = new_values.len();
            let result = newly_zeroed_from_remaps(
                new_values,
                outcomes.into_iter().take(remap_count).collect::<Vec<_>>(),
            );
            return Ok(DeferredCleanupHandle::ready(result));
        }
        let (_, inner_handle) = tx.commit_deferred_with_outcomes()?;
        let remap_count = new_values.len();
        Ok(DeferredCleanupHandle::wrap(inner_handle, new_values, remap_count))
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
fn metadb_config_from_onyx(path: &Path, config: &MetaConfig) -> MetaDbConfig {
    let mut cfg = MetaDbConfig::new(path);
    cfg.page_cache_bytes = config.block_cache_bytes() as u64;
    cfg.lsm_memtable_bytes = config.memtable_budget_bytes() as u64;
    cfg.lsm_bloom_bits_per_entry = config.lsm_bloom_bits_per_entry();
    cfg.group_commit_max_batch_bytes = 16 * 1024 * 1024;
    cfg.index_pin_bytes = config.index_pin_bytes() as u64;
    cfg.group_commit_timeout_us = config.group_commit_timeout_us();
    cfg.wal_async_group_commit_window_us = config.wal_async_group_commit_window_us();
    // Phase D.5b retired metadb's internal WAL; `Buffer` is now the
    // only engine-side journal mode. Onyx's three-variant enum still
    // governs onyx-side semantics (checkpoint watermark, buffer
    // replay), but the metadb engine itself sees only Buffer.
    let _ = config.journal_mode;
    cfg.journal_mode = onyx_metadb::MetaDbJournalMode::Buffer;
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
    cfg.commit_direct_apply_enabled = config.commit_direct_apply_enabled;
    cfg.commit_deferred_outcomes_enabled = config.commit_deferred_outcomes_enabled;
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
#[cfg(test)]
mod tests;
