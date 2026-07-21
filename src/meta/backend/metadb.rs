use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crossbeam_channel::{unbounded, Receiver, Sender};
use dashmap::DashMap;
use onyx_metadb::dedup::{DedupMigrationStatus, MigrateStepStats};
use onyx_metadb::{
    Config as MetaDbConfig, Db, DedupValue, DeferredOutcomeHandle, L2pValue, Lsn, VolumeOrdinal,
};
use serde::Serialize;

use crate::config::MetaConfig;
use crate::error::{OnyxError, OnyxResult};
use crate::meta::schema::{BlockmapValue, ContentHash, DedupEntry, FLAG_DEDUP_SKIPPED};
use crate::meta::store::{DedupHitResult, RemapCleanup, SnapshotInfo, SnapshotRestoreStats};
use crate::metrics::MetaMemorySnapshot;
use crate::types::{Lba, Pba, VolumeConfig, VolumeId};

use super::codec::{
    blockmap_from_l2p_bytes, freed_blocks_for_l2p_value, L2P_BIRTH_OFFSET, L2P_SEQ_OFFSET,
};

const METADB_DEDUP_VALUE_BYTES: usize = 28;
const METADB_PAGE_FILE: &str = "pages.onyx_meta";
const BLOCKMAP_SCAN_CHUNK_LBAS: u64 = 262_144; // 1 GiB of 4 KiB LBAs.

/// Internal chunk size for the *windowed* background scan (`scan_blockmap_range`,
/// used by the resident GC compactor, the heat refresh, and the dedup cold-tail
/// scanner). The chunk is the granularity at which metadb's
/// `scan_range_unordered_chunked` acquires/releases a shard's L2P read view
/// (`active_readers`): while a shard is being walked, concurrent commit-apply to
/// that shard cannot take its empty-overlay fast path and falls to the COW-clone
/// slow path. Passing the caller's whole window (up to ~1M LBAs) as one chunk
/// pinned each shard for milliseconds, near-constantly forcing commits onto the
/// slow path (~180× commit-apply collapse observed with the compactor on). A
/// small chunk keeps each per-shard hold to ~tens of µs with frequent
/// `active_readers == 0` gaps, so commits keep hitting the fast path. The chunk
/// is purely a lock-granularity knob — the callback still fires for every entry
/// in the window, so aggregation results are unchanged (a unit straddling a
/// chunk boundary is still seen whole by the caller's accumulator). Smaller =
/// shorter holds but more acquire overhead; 8192 LBAs ≈ a few hundred entries
/// per shard per hold.
const BACKGROUND_SCAN_CHUNK_LBAS: u64 = 8_192;
const DEDUP_PERSIST_BATCH_LIMIT: usize = crate::dedup::config::DEDUP_PUT_BATCH_HARD_MAX_ENTRIES;

pub(crate) struct MetadbBackend {
    db: Arc<Db>,
    checkpoint: AsyncCheckpoint,
    unlogged_flush_commits: bool,
    catalog: Mutex<VolumeCatalog>,
    volume_ordinals: DashMap<String, VolumeOrdinal>,
    /// Where the volume catalog is persisted: a host-FS file (default path) or
    /// A/B slots on the meta LD (chunklet backend).
    catalog_store: CatalogStore,
    /// The chunklet Pool metadb was opened over (device path only). Kept alive
    /// here for the meta LD's lifetime and handed to the engine's LV3/LV2 open on
    /// the standby→active upgrade so the pool is never opened twice.
    chunklet_pool: Option<Arc<onyx_chunklet::Pool>>,
    /// Online-grow handle for the meta LD page window (device path only). `None`
    /// on the file backend. Drives `swap_ld` + OMET superblock rewrite + metadb
    /// device ceiling widen when the meta LD is extended online.
    meta_grower: Option<meta_ld::MetaLdGrower>,
    // Lineage GC freed-PBA signal channel.
    //
    // `metadb` invokes `freed_pbas_sink` synchronously on its GC driver
    // thread. The sink does a non-blocking enqueue here so the GC thread
    // never blocks on onyx's retire pipeline. The engine drains this
    // channel and feeds the PBAs through the existing allocator retire
    // path (coalesced into extents).
    //
    // Rc-neutral mode requires `lineage_gc_emit_freepbas=true`, so this channel is
    // the normal Lineage-GC retire signal path.
    lineage_freed_pbas_tx: Sender<Pba>,
    lineage_freed_pbas_rx: Receiver<Pba>,
}

mod catalog;
mod checkpoint;
mod meta_ld;
mod values;

/// Where the volume catalog is persisted. The file path uses the host-FS
/// tmp+rename `atomic_write`; the chunklet path uses generational A/B slots on
/// the meta LD (no host FS involved).
enum CatalogStore {
    File(PathBuf),
    Ld(meta_ld::MetaLdCatalog),
}

impl CatalogStore {
    fn persist(&self, catalog: &VolumeCatalog) -> OnyxResult<()> {
        match self {
            CatalogStore::File(path) => catalog.persist(path),
            CatalogStore::Ld(slots) => slots.persist(catalog),
        }
    }
}

use catalog::{SnapshotCatalogEntry, VolumeCatalog, VolumeCatalogEntry, CATALOG_FILE};
use checkpoint::AsyncCheckpoint;
pub(crate) use values::coalesce_free_pbas_to_extents;
use values::{
    decode_dedup_value, decode_l2p_value, dedup_hit_results_from_remaps, emit_l2p_remap_runs,
    from_l2p_value, from_metadb_pba, newly_zeroed_from_remaps, seq_for, to_dedup_value,
    to_l2p_value, to_l2p_value_with_seq, to_metadb_pba,
};

/// Onyx-side wrapper around a metadb [`DeferredOutcomeHandle`].
/// Carries the `new_values` slice the caller had buffered so that
/// the cleanup tuple
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
    Ready {
        value: OnyxResult<(HashMap<Pba, RemapCleanup>, Vec<bool>)>,
        /// LSN assigned by metadb when the stage path resolved the
        /// commit inline. `None` only for the empty-batch shortcut
        /// (no LSN allocated).
        lsn: Option<Lsn>,
    },
    Pending {
        inner: DeferredOutcomeHandle,
        new_values: Vec<BlockmapValue>,
        remap_count: usize,
    },
}

impl DeferredCleanupHandle {
    /// Build a handle whose cleanup tuple is already known. Used by
    /// the empty-batch path inside the metadb adapter so all entry
    /// points can return `DeferredCleanupHandle` uniformly. The LSN is
    /// `None` because no metadb commit happened.
    pub(crate) fn ready(value: OnyxResult<(HashMap<Pba, RemapCleanup>, Vec<bool>)>) -> Self {
        Self {
            state: DeferredCleanupHandleState::Ready { value, lsn: None },
        }
    }

    /// Like [`Self::ready`] but carries the metadb-assigned LSN. Used
    /// by the staged metadb commit path. The commit completes
    /// synchronously on the caller thread, but downstream code (the
    /// commit_worker per-volume sequencer) still wants `lsn()` to
    /// return `Some(lsn)`.
    pub(crate) fn ready_with_lsn(
        value: OnyxResult<(HashMap<Pba, RemapCleanup>, Vec<bool>)>,
        lsn: Lsn,
    ) -> Self {
        Self {
            state: DeferredCleanupHandleState::Ready {
                value,
                lsn: Some(lsn),
            },
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
            DeferredCleanupHandleState::Ready { value, .. } => value,
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
    /// constructed via [`Self::ready`] / [`Self::ready_with_lsn`] or
    /// because the metadb compactor has already released the staged
    /// outcome.
    pub(crate) fn is_ready(&self) -> bool {
        match &self.state {
            DeferredCleanupHandleState::Ready { .. } => true,
            DeferredCleanupHandleState::Pending { inner, .. } => inner.is_ready(),
        }
    }

    /// Non-blocking probe; consumes the handle on `Ok`, returns it
    /// back on `Err(self)` if the metadb compactor has not yet
    /// released the staged outcome. Reserved for future opportunistic
    /// drain paths in `commit_worker/passthrough.rs`; the current
    /// pipeline relies on `recv()` only.
    #[allow(dead_code)]
    pub(crate) fn try_recv(
        self,
    ) -> Result<OnyxResult<(HashMap<Pba, RemapCleanup>, Vec<bool>)>, Self> {
        match self.state {
            DeferredCleanupHandleState::Ready { value, .. } => Ok(value),
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
    /// FIFO without waiting for outcome delivery. `None` only for the
    /// empty-batch shortcut (no metadb commit happened).
    #[allow(dead_code)]
    pub(crate) fn lsn(&self) -> Option<Lsn> {
        match &self.state {
            DeferredCleanupHandleState::Pending { inner, .. } => Some(inner.lsn()),
            DeferredCleanupHandleState::Ready { lsn, .. } => *lsn,
        }
    }
}

fn snapshot_info_from_entry(entry: &SnapshotCatalogEntry) -> SnapshotInfo {
    SnapshotInfo {
        volume: entry.volume.clone(),
        name: entry.name.clone(),
        snapshot_id: entry.snapshot_id,
        created_lsn: entry.created_lsn,
        created_at: entry.created_at,
        size_bytes: entry.size_bytes,
    }
}

impl MetadbBackend {
    /// Open (or create) the metadb backend on the host filesystem at
    /// `config.meta.path` (the default `backend = "file"` path).
    pub(crate) fn open(config: &MetaConfig) -> OnyxResult<Self> {
        let path = config.path().ok_or_else(|| {
            OnyxError::Config("meta.path is required to open metadb backend".into())
        })?;
        fs::create_dir_all(path)?;

        let db_config = metadb_config_from_onyx(path, config);
        // metadb's `open_with_config` / `create_with_config` return
        // `Arc<Db>` directly so `BfgSyncThread` can hold a `Weak<Db>`
        // without creating a shutdown cycle. No extra `Arc::new` wrap.
        let db = if path.join(METADB_PAGE_FILE).exists() {
            Db::open_with_config(db_config)?
        } else {
            Db::create_with_config(db_config)?
        };

        let catalog_path = path.join(CATALOG_FILE);
        let catalog = VolumeCatalog::load(&catalog_path)?;
        Self::assemble(
            config,
            db,
            catalog,
            CatalogStore::File(catalog_path),
            None,
            None,
        )
    }

    /// Open (or first-time create) the metadb backend on a chunklet meta
    /// LogicalDisk (`backend = "chunklet"`). `meta_backend` is the meta-role LD
    /// backend; `pool` is the shared chunklet Pool (kept alive here + handed to a
    /// later LV3/LV2 open so the pool is never opened twice).
    pub(crate) fn open_on_meta_ld(
        config: &MetaConfig,
        meta_backend: Arc<crate::io::block_backend::ChunkletBackend>,
        pool: Arc<onyx_chunklet::Pool>,
    ) -> OnyxResult<Self> {
        // `config.meta.path` is only a diagnostic label on the device path; still
        // build the metadb Config from it (it carries every non-path knob).
        let label = config
            .path()
            .cloned()
            .unwrap_or_else(|| PathBuf::from("<meta-ld>"));
        let db_config = metadb_config_from_onyx(&label, config);
        let meta_ld = meta_ld::open_or_create(meta_backend, db_config)?;
        Self::assemble(
            config,
            meta_ld.db,
            meta_ld.catalog,
            CatalogStore::Ld(meta_ld.catalog_store),
            Some(pool),
            Some(meta_ld.grower),
        )
    }

    /// Propagate an online meta-LD extend: swap the freshly-opened extended LD
    /// into the meta `ChunkletBackend`, rewrite the OMET superblock's page-window
    /// size, and widen the metadb page device so a full-meta `CapacityExhausted`
    /// clears. Errors on the file backend (no growable device). `new_ld` is a
    /// re-`open_ld` of the SAME meta LD after `pool.extend_ld`.
    pub(crate) fn grow_meta_capacity(
        &self,
        new_ld: Arc<dyn onyx_chunklet::ld::LogicalDisk>,
    ) -> OnyxResult<()> {
        match &self.meta_grower {
            Some(g) => g.grow(&self.db, new_ld),
            None => Err(OnyxError::Config(
                "meta backend is not on a growable chunklet LD".into(),
            )),
        }
    }

    /// Shared tail of both open paths: validate the catalog against the store,
    /// seed the ordinal cache, start the async checkpoint, and wire the lineage
    /// freed-PBA sink.
    fn assemble(
        config: &MetaConfig,
        db: Arc<Db>,
        catalog: VolumeCatalog,
        catalog_store: CatalogStore,
        chunklet_pool: Option<Arc<onyx_chunklet::Pool>>,
        meta_grower: Option<meta_ld::MetaLdGrower>,
    ) -> OnyxResult<Self> {
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
            catalog_store,
            chunklet_pool,
            meta_grower,
            lineage_freed_pbas_tx,
            lineage_freed_pbas_rx,
        })
    }

    /// The chunklet Pool this backend was opened over (device path), for reuse by
    /// the engine's LV3/LV2 open on the standby→active upgrade. `None` on the file
    /// path.
    pub(crate) fn chunklet_pool(&self) -> Option<Arc<onyx_chunklet::Pool>> {
        self.chunklet_pool.clone()
    }

    pub(crate) fn chunklet_io_scheduler(
        &self,
    ) -> Option<Arc<crate::io::block_backend::ChunkletIoScheduler>> {
        self.meta_grower
            .as_ref()
            .and_then(meta_ld::MetaLdGrower::io_scheduler)
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

    pub(crate) fn rc_authoritative_reclaim(&self) -> bool {
        self.db.rc_authoritative_reclaim()
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
            self.catalog_store.persist(&catalog)?;
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
        self.catalog_store.persist(&catalog)?;
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

        // metadb refuses `drop_volume` while live snapshots pin the volume, so
        // drop every named snapshot of this volume first. Their refcount-zeroed
        // PBAs are surfaced to the caller as `pba_freed` cleanups (blocks=1,
        // empty mappings) so the engine retires them through the same
        // PbaLifecycle path as overwrite cleanups. The catalog entries are
        // removed in one shot by the `retain` + single `persist` below, so the
        // metadb drops here run without per-snapshot catalog writes.
        let snap_ids: Vec<u64> = {
            let catalog = self.catalog.lock().unwrap();
            catalog
                .snapshots
                .iter()
                .filter(|s| s.volume == id.0)
                .map(|s| s.snapshot_id)
                .collect()
        };
        let mut snapshot_cleanups = Vec::new();
        for snapshot_id in snap_ids {
            if let Some(report) = self.db.drop_snapshot(snapshot_id)? {
                for pba in report.freed_pbas {
                    snapshot_cleanups.push(RemapCleanup {
                        pba: from_metadb_pba(pba),
                        decrements: 1,
                        blocks: 1,
                        pba_freed: true,
                        mappings: Vec::new(),
                    });
                }
            }
        }

        let end = Lba(config.size_bytes / u64::from(config.block_size));
        let mut cleanups = self.delete_blockmap_range(id, Lba(0), end)?;
        // Dropping a clone can reveal promotion over-pin PBAs whose global
        // refcount reached 0 and that no surviving root maps. Retire them
        // through the same PbaLifecycle path as snapshot / overwrite
        // cleanups; the by-PBA HashMap merge downstream absorbs duplicate
        // FreePbas idempotently.
        if let Some(report) = self.db.drop_volume(ordinal)? {
            for pba in report.freed_pbas {
                cleanups.push(RemapCleanup {
                    pba: from_metadb_pba(pba),
                    decrements: 1,
                    blocks: 1,
                    pba_freed: true,
                    mappings: Vec::new(),
                });
            }
        }

        let mut catalog = self.catalog.lock().unwrap();
        catalog.by_id.remove(&id.0);
        catalog.snapshots.retain(|s| s.volume != id.0);
        self.volume_ordinals.remove(&id.0);
        self.catalog_store.persist(&catalog)?;

        cleanups.extend(snapshot_cleanups);
        Ok(cleanups)
    }

    // ── Snapshot lifecycle ──────────────────────────────────────────────────

    pub(crate) fn create_snapshot(
        &self,
        vol_id: &VolumeId,
        snap_name: &str,
        created_at: u64,
    ) -> OnyxResult<SnapshotInfo> {
        if snap_name.is_empty() {
            return Err(OnyxError::Config("snapshot name must not be empty".into()));
        }
        let (ordinal, size_bytes) = {
            let catalog = self.catalog.lock().unwrap();
            let entry = catalog
                .by_id
                .get(&vol_id.0)
                .ok_or_else(|| OnyxError::VolumeNotFound(vol_id.0.clone()))?;
            if catalog.find_snapshot(&vol_id.0, snap_name).is_some() {
                return Err(OnyxError::Config(format!(
                    "snapshot '{}' already exists for volume '{}'",
                    snap_name, vol_id.0
                )));
            }
            (entry.ordinal, entry.config.size_bytes)
        };

        let snapshot_id = self.db.take_snapshot(ordinal)?;
        // Recover the capture LSN from the manifest entry metadb just wrote.
        let created_lsn = self
            .db
            .snapshots_for(ordinal)
            .into_iter()
            .find(|s| s.id == snapshot_id)
            .map(|s| s.created_lsn)
            .unwrap_or(0);

        let entry = SnapshotCatalogEntry {
            volume: vol_id.0.clone(),
            name: snap_name.to_string(),
            snapshot_id,
            vol_ord: ordinal,
            created_lsn,
            created_at,
            size_bytes,
        };
        let info = snapshot_info_from_entry(&entry);
        let mut catalog = self.catalog.lock().unwrap();
        catalog.snapshots.push(entry);
        self.catalog_store.persist(&catalog)?;
        Ok(info)
    }

    pub(crate) fn list_snapshots(&self, volume: Option<&str>) -> OnyxResult<Vec<SnapshotInfo>> {
        let catalog = self.catalog.lock().unwrap();
        let mut out: Vec<SnapshotInfo> = catalog
            .snapshots
            .iter()
            .filter(|s| volume.map_or(true, |v| s.volume == v))
            .map(snapshot_info_from_entry)
            .collect();
        out.sort_by(|a, b| {
            a.volume
                .cmp(&b.volume)
                .then(a.created_at.cmp(&b.created_at))
                .then(a.name.cmp(&b.name))
        });
        Ok(out)
    }

    pub(crate) fn delete_snapshot(
        &self,
        vol_id: &VolumeId,
        snap_name: &str,
    ) -> OnyxResult<Vec<Pba>> {
        let snapshot_id = {
            let catalog = self.catalog.lock().unwrap();
            match catalog.find_snapshot(&vol_id.0, snap_name) {
                Some(s) => s.snapshot_id,
                None => return Ok(Vec::new()),
            }
        };

        let freed = match self.db.drop_snapshot(snapshot_id)? {
            Some(report) => report.freed_pbas.into_iter().map(from_metadb_pba).collect(),
            None => Vec::new(),
        };

        let mut catalog = self.catalog.lock().unwrap();
        catalog.remove_snapshot(&vol_id.0, snap_name);
        self.catalog_store.persist(&catalog)?;
        Ok(freed)
    }

    pub(crate) fn clone_snapshot(
        &self,
        vol_id: &VolumeId,
        snap_name: &str,
        new_name: &str,
        created_at: u64,
    ) -> OnyxResult<VolumeConfig> {
        if new_name.is_empty() {
            return Err(OnyxError::Config(
                "clone target volume name must not be empty".into(),
            ));
        }
        let (snapshot_id, mut new_config) = {
            let catalog = self.catalog.lock().unwrap();
            if catalog.by_id.contains_key(new_name) {
                return Err(OnyxError::Config(format!(
                    "volume '{}' already exists",
                    new_name
                )));
            }
            let snap = catalog.find_snapshot(&vol_id.0, snap_name).ok_or_else(|| {
                OnyxError::Config(format!(
                    "snapshot '{}' not found for volume '{}'",
                    snap_name, vol_id.0
                ))
            })?;
            let src = catalog
                .by_id
                .get(&vol_id.0)
                .ok_or_else(|| OnyxError::VolumeNotFound(vol_id.0.clone()))?;
            (snap.snapshot_id, src.config.clone())
        };
        new_config.id = VolumeId(new_name.to_string());
        new_config.created_at = created_at;

        let new_ordinal = self.db.clone_volume(snapshot_id)?;

        let mut catalog = self.catalog.lock().unwrap();
        catalog.by_id.insert(
            new_name.to_string(),
            VolumeCatalogEntry {
                ordinal: new_ordinal,
                config: new_config.clone(),
            },
        );
        self.volume_ordinals
            .insert(new_name.to_string(), new_ordinal);
        self.catalog_store.persist(&catalog)?;
        Ok(new_config)
    }

    pub(crate) fn restore_snapshot(
        &self,
        vol_id: &VolumeId,
        snap_name: &str,
    ) -> OnyxResult<SnapshotRestoreStats> {
        let snapshot_id = {
            let catalog = self.catalog.lock().unwrap();
            catalog
                .find_snapshot(&vol_id.0, snap_name)
                .ok_or_else(|| {
                    OnyxError::Config(format!(
                        "snapshot '{}' not found for volume '{}'",
                        snap_name, vol_id.0
                    ))
                })?
                .snapshot_id
        };
        let report = self.db.restore_volume_to_snapshot(snapshot_id)?;
        Ok(SnapshotRestoreStats {
            lbas_remapped: report.lbas_remapped,
            lbas_deleted: report.lbas_deleted,
        })
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

    /// Fold-consistent refcount read (see metadb
    /// `Db::multi_get_refcount_consistent`). The plain `multi_get_refcounts`
    /// can transiently read a live PBA's rc as a spurious 0 when it straddles
    /// a refcount fold's publish-before-clear window. That is harmless for the
    /// dedup-hit guard (reversible demote-to-miss) but, under
    /// `rc_authoritative_reclaim`, fatal for the GC reclaim gate: it skips the
    /// Gate-2 blockmap reverify and frees the PBA on rc==0 alone, so a
    /// spurious 0 frees a still-referenced PBA → reuse → read CRC. Reclaim
    /// uses THIS for the irreversible free decision.
    pub(crate) fn multi_get_refcounts_consistent(&self, pbas: &[Pba]) -> OnyxResult<Vec<u32>> {
        let pbas: Vec<onyx_metadb::Pba> = pbas.iter().map(|pba| to_metadb_pba(*pba)).collect();
        Ok(self.db.multi_get_refcount_consistent(&pbas)?)
    }

    /// Test-only refcount seed. Rc-neutral mode removed the per-write refcount
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
        let remap_ops = emit_l2p_remap_runs(&mut tx, ord, batch_values, seqs, 0);
        for (hash, entry) in dedup_entries {
            tx.put_dedup(*hash, to_dedup_value(entry));
        }
        // Stage the metadb update without waiting for the old per-LSN
        // dispatch path. Durability is via the LV2 buffer; the metadb
        // fold runs at the next BFG sync.
        let (_, outcomes) = tx.commit_staged_with_outcomes()?;
        newly_zeroed_from_remaps(
            batch_values.iter().map(|(_, value)| *value),
            outcomes.into_iter().take(remap_ops).collect(),
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
        // Same staged metadb commit path as `atomic_batch_write_with_dedup`.
        let (_, outcomes) = tx.commit_staged_with_outcomes()?;
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

    /// Deferred counterpart to [`Self::atomic_batch_write_multi_with_dedup`].
    /// Issues the commit and returns a handle that resolves to the same
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
            return Ok(DeferredCleanupHandle::ready(Ok((
                HashMap::new(),
                Vec::new(),
            ))));
        }
        let mut tx = self.db.begin();
        let mut new_values = Vec::new();
        let mut flat_idx: usize = 0;
        let mut remap_ops = 0usize;
        for (vol_id, batch_values, new_refcount) in units {
            let _ = new_refcount;
            let ord = self.volume_ordinal(vol_id)?;
            // Each unit's batch_values is contiguous LBAs by construction
            // (one CompressedUnit / one passthrough sub-batch); emit one
            // range op per maximal contiguous LBA run within the unit.
            remap_ops += emit_l2p_remap_runs(&mut tx, ord, batch_values, seqs, flat_idx);
            for (_, value) in *batch_values {
                new_values.push(*value);
            }
            flat_idx += batch_values.len();
        }
        for (hash, entry) in dedup_entries {
            tx.put_dedup(*hash, to_dedup_value(entry));
        }
        // The old deferred-outcome surface parked outcomes in the L2P
        // compactor's per-pass drain so the caller could pipeline
        // multiple in-flight commits before paying the per-LSN dispatch
        // wait. The staged path removes that wait entirely: outcomes
        // are materialised synchronously on the caller thread, so the
        // handle returned here is always pre-populated (`Ready`). Onyx
        // commit_worker still drives the call site through the deferred
        // surface, but `.recv()` returns immediately. The LSN flows
        // through `ready_with_lsn` so the per-volume sequencer's
        // `handle.lsn()` contract is preserved.
        let (lsn, outcomes) = tx.commit_staged_with_outcomes()?;
        let result = newly_zeroed_from_remaps(
            new_values,
            outcomes.into_iter().take(remap_ops).collect::<Vec<_>>(),
        );
        Ok(DeferredCleanupHandle::ready_with_lsn(result, lsn))
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
        let scan_result = self.db.scan_range_unordered_chunked(
            ord,
            start_lba.0,
            end,
            BACKGROUND_SCAN_CHUNK_LBAS,
            |lba, value| {
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
            },
        );
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

    /// Batched counterpart to the old single-extent blockmap-reference check:
    /// given candidate extents as `(start_pba, blocks)`, perform ONE all-volume
    /// L2P scan and return, per candidate (input order), whether any live
    /// blockmap entry references a PBA inside it. Early-exits the scan once
    /// every candidate has been marked referenced.
    ///
    /// This collapses GC retired-extent reclaim from `O(retired × all_L2P)`
    /// (one full scan per extent) to a single scan per cycle. Retired extents
    /// handed in by the reclaim path are coalesced and non-overlapping, so each
    /// PBA is covered by at most one candidate; the covering lookup is a binary
    /// search over candidates sorted by start.
    pub(crate) fn referenced_extents(&self, extents: &[(Pba, u32)]) -> OnyxResult<Vec<bool>> {
        if extents.is_empty() {
            return Ok(Vec::new());
        }
        // Indices sorted by start PBA (skipping zero-length candidates) for the
        // O(log n) covering lookup inside the scan callback.
        let mut order: Vec<usize> = (0..extents.len()).filter(|&i| extents[i].1 != 0).collect();
        order.sort_unstable_by_key(|&i| extents[i].0 .0);

        let mut referenced = vec![false; extents.len()];
        let mut remaining = order.len();
        if remaining == 0 {
            return Ok(referenced);
        }

        // Set once every candidate is marked, to swallow the abort sentinel the
        // scan callback returns (the `decode_error` idiom below distinguishes a
        // real corruption error from this control-flow early stop).
        let mut early = false;
        let volumes = self.list_volumes()?;
        'volumes: for volume in volumes {
            let ord = self.volume_ordinal(&volume.id)?;

            // ONE fold-consistent scan per volume: metadb holds `tree.read()`
            // per shard across BOTH the folded read-view scan AND the l2p_buffer
            // scan, so a concurrent BFG fold can't make a migrating reference
            // (e.g. a packed-slot sibling mid-fold) transiently invisible
            // between two unsynchronised passes. This is LOAD-BEARING: a plain
            // two-pass reverify (even buffer-first + publish-before-clear, even
            // paired with the reclaim-age grace) let the premature-free CRC back
            // in (soak-proven 2026-06-08 AND 2026-06-09). ⚠ The `tree.read()`
            // blocks the fold/checkpoint `tree.write()` for the whole walk →
            // multi-second commit-apply spikes; a latency fix must keep this
            // consistency, not drop it. See `Db::scan_l2p_live_consistent` /
            // fixb_soak_exposed_referenced_extents_race.
            let mut decode_error = None;
            let scan_result = self.db.scan_l2p_live_consistent(ord, |_lba, value| {
                match Self::cover_referenced(
                    value,
                    &order,
                    extents,
                    &mut referenced,
                    &mut remaining,
                ) {
                    Ok(true) => Err(onyx_metadb::MetaDbError::Corruption(
                        "referenced_extents: all candidates marked (early stop)".into(),
                    )),
                    Ok(false) => Ok(()),
                    Err(err) => {
                        decode_error = Some(err);
                        Err(onyx_metadb::MetaDbError::Corruption(
                            "onyx blockmap decode failed".into(),
                        ))
                    }
                }
            });
            if let Some(err) = decode_error {
                return Err(err);
            }
            if remaining == 0 {
                early = true;
            } else {
                scan_result?;
            }

            if early {
                break 'volumes;
            }
        }
        Ok(referenced)
    }

    /// Mark every candidate extent covered by one decoded L2pValue. Returns
    /// `Ok(true)` once every candidate is marked (early-stop signal), invoked
    /// for each live entry by the fold-consistent scan in `referenced_extents`.
    fn cover_referenced(
        value: L2pValue,
        order: &[usize],
        extents: &[(Pba, u32)],
        referenced: &mut [bool],
        remaining: &mut usize,
    ) -> OnyxResult<bool> {
        let decoded = decode_l2p_value(value)?;
        if decoded.is_zero() {
            return Ok(false);
        }
        for pba in decoded.physical_pbas(crate::types::BLOCK_SIZE) {
            // Rightmost candidate whose start <= pba is the only one that can
            // cover it (candidates are non-overlapping).
            let pos = order.partition_point(|&i| extents[i].0 .0 <= pba.0);
            if pos == 0 {
                continue;
            }
            let idx = order[pos - 1];
            let (start, blocks) = extents[idx];
            if pba.0 < start.0 + u64::from(blocks) && !referenced[idx] {
                referenced[idx] = true;
                *remaining -= 1;
                if *remaining == 0 {
                    return Ok(true);
                }
            }
        }
        Ok(false)
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

    /// Resumable, bounded scan over the dedup forward index — the scalable
    /// alternative to [`Self::iter_dedup_entries`] for background sweeps. Returns
    /// up to `limit` decoded entries from `cursor`, plus the resume cursor and a
    /// `wrapped` flag (a full pass completed). O(`limit`) — does not materialise
    /// the whole index, so it scales to a multi-billion-entry index where
    /// `iter_dedup_entries` would allocate hundreds of GiB.
    pub(crate) fn scan_dedup_from(
        &self,
        cursor: onyx_metadb::DedupScanCursor,
        limit: usize,
    ) -> OnyxResult<(
        Vec<(ContentHash, DedupEntry)>,
        onyx_metadb::DedupScanCursor,
        bool,
    )> {
        let batch = self.db.scan_dedup_from(cursor, limit)?;
        let entries = batch
            .entries
            .into_iter()
            .map(|(hash, value)| Ok((hash, decode_dedup_value(value)?)))
            .collect::<OnyxResult<Vec<_>>>()?;
        Ok((entries, batch.next, batch.wrapped))
    }

    pub(crate) fn iter_allocated_blocks(&self) -> OnyxResult<Vec<(Pba, u32)>> {
        // Diagnostic classifier (ONYX_PBA_TRACE=1): snapshot which source
        // reserved each PBA in this scan so CRC sites can report membership.
        crate::space::free_trace::classifier_reset();
        let mut blocks: Vec<(Pba, u32)> = Vec::new();
        // COMPLETE L2P read-view = folded paged tree UNION the l2p_buffer
        // (committed-but-not-yet-folded staged mappings). `scan_all_blockmap_entries`
        // / `scan_range_unordered_chunked` only see the folded tree; on a heavily
        // overwritten (REUSED) pool the l2p_buffer holds many committed single-block
        // mappings that have not yet drained. Omitting them makes
        // `rebuild_from_metadata` treat a still-live block as a free gap → the next
        // allocation reuses it → foreground CRC + `retire_extent ... overlaps free
        // extent` (soak-reproduced on a reused chunklet pool 2026-07-02;
        // fresh-init-each-run masked it because the l2p_buffer was ~empty). The GC
        // reclaim gate (`referenced_extents`) already scans tree ∪ l2p_buffer via the
        // same `scan_l2p_live_consistent`; the free-list builder MUST use the same
        // read-view or it frees exactly what the reclaim gate would protect.
        let volumes = self.list_volumes()?;
        for volume in volumes {
            let ord = self.volume_ordinal(&volume.id)?;
            let mut decode_error = None;
            let scan_result = self.db.scan_l2p_live_consistent(ord, |_lba, value| {
                match decode_l2p_value(value) {
                    Ok(decoded) => {
                        if !decoded.is_zero() {
                            for pba in decoded.physical_pbas(crate::types::BLOCK_SIZE) {
                                crate::space::free_trace::mark_reserved_blockmap(pba);
                                blocks.push((pba, 1));
                            }
                        }
                        Ok(())
                    }
                    Err(err) => {
                        decode_error = Some(err);
                        Err(onyx_metadb::MetaDbError::Corruption(
                            "onyx blockmap decode failed".into(),
                        ))
                    }
                }
            });
            if let Some(err) = decode_error {
                return Err(err);
            }
            scan_result?;
        }
        // Also reserve PBAs referenced by the dedup_index. A promoted dedup
        // entry (hash → pba) keeps its block alive at rc>0 even after every L2P
        // sharer LBA has been overwritten (until orphan-reclaim demotes it).
        // Such "dedup-only" blocks are NOT in the blockmap scan above; omitting
        // them makes `rebuild_from_metadata` treat a still-referenced block as a
        // free gap on restart → the next allocation reuses it → CRC corruption +
        // a flood of dedup verify mismatches ("dedup entry overlaps free
        // extent"). rebuild rebuilds only the allocator free list (not refcount),
        // so the freed block still has rc>0 and the two views desynchronize.
        for (_, entry) in self.iter_dedup_entries()? {
            for pba in entry
                .to_blockmap_value()
                .physical_pbas(crate::types::BLOCK_SIZE)
            {
                crate::space::free_trace::mark_reserved_dedup(pba);
                blocks.push((pba, 1));
            }
        }
        blocks.sort_unstable_by_key(|(pba, _)| *pba);
        blocks.dedup_by_key(|(pba, _)| *pba);
        Ok(blocks)
    }

    pub(crate) fn sync_durable(&self) -> OnyxResult<u64> {
        self.checkpoint.sync()
    }

    pub(crate) fn drain_deferred_reclaim_durable(&self) -> OnyxResult<usize> {
        Ok(self.db.drain_deferred_reclaim_durable()?)
    }

    pub(crate) fn set_buffer_applied_watermark(&self, seq: u64) {
        self.db.set_buffer_applied_watermark(seq);
    }

    pub(crate) fn durable_buffer_applied_watermark(&self) -> u64 {
        self.db.durable_buffer_applied_watermark()
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

    // ---- online cuckoo dedup-index modulus resize (driven by DedupScanner) ----

    pub(crate) fn dedup_migration_status(&self) -> DedupMigrationStatus {
        self.db.dedup_migration_status()
    }

    pub(crate) fn dedup_resize_begin(&self, target_bucket_count: u64) -> OnyxResult<()> {
        Ok(self.db.dedup_resize_begin(target_bucket_count)?)
    }

    pub(crate) fn dedup_migrate_step(
        &self,
        start_page: usize,
        max_pages: usize,
    ) -> OnyxResult<MigrateStepStats> {
        Ok(self.db.dedup_migrate_step(start_page, max_pages)?)
    }

    pub(crate) fn dedup_resize_finish(&self) -> OnyxResult<()> {
        Ok(self.db.dedup_resize_finish()?)
    }

    pub(crate) fn try_request_durable_checkpoint_token(&self) -> OnyxResult<Option<u64>> {
        self.checkpoint.try_request_async_token()
    }

    pub(crate) fn durable_checkpoint_outcome(
        &self,
        token: u64,
    ) -> OnyxResult<Option<crate::meta::store::DurableCheckpointOutcome>> {
        self.checkpoint.checkpoint_outcome(token)
    }

    /// Test-only: arm this backend's checkpoint failpoint (injects a fatal
    /// `CapacityExhausted` on the next checkpoint) to exercise the fence path.
    #[cfg(test)]
    pub(crate) fn arm_checkpoint_capacity_fail(&self) {
        self.checkpoint.arm_capacity_fail();
    }

    pub(crate) fn memory_stats(&self) -> OnyxResult<MetaMemorySnapshot> {
        Ok(MetaMemorySnapshot::from_metadb(
            self.db.last_applied_lsn_best_effort(),
            self.db.high_water(),
            self.db.free_list_len() as u64,
            self.db.dedup_tier_sizes_best_effort(),
            self.db.cache_stats(),
            self.db.metrics_snapshot(),
            self.db.pending_state_best_effort(),
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
        // Same staged metadb commit path as `atomic_batch_write_with_dedup`.
        // No LV3 write; this is a flag-bit edit on an existing mapping.
        // Crash safety: if this flag-clear is lost, the next scanner
        // cycle re-processes the LBA — idempotent.
        tx.commit_staged_with_outcomes()?;
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
        // data loss. Uses the same staged metadb commit path as
        // `atomic_batch_write_with_dedup`.
        let (_, outcomes) = tx.commit_staged_with_outcomes()?;
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
        // Rc-neutral path: per-WalOp apply runs on per-shard lanes
        // (`apply_l2p_bucket` for L2P, `apply_dedup_indices_to` for
        // dedup), so even within a single tx the L2pRemap may execute
        // before the paired DedupPut bumps rc. Hits whose target PBA
        // is being promoted **must not** carry an `rc >= 1` guard:
        // the target's rc is 0 before this tx, the lane racing the
        // dedup lane would observe rc=0 and reject the remap, and
        // pre-rc-neutral the hot-path L2pRemap itself drove rc so the
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
        // decref-to-zero, and crash rolls back atomically.
        // Same staged metadb commit path as `atomic_batch_write_with_dedup`.
        let (_, outcomes) = tx.commit_staged_with_outcomes()?;
        dedup_hit_results_from_remaps(hits, outcomes)
    }
}

/// Offline audit entry point: open metadb on the meta LD `meta_backend` and
/// run the same reachability/orphan-page scan `metadb-verify` runs for the
/// plain-file backend. The audit open never initialises a fresh MetaDB and
/// disables continuous background mutation, but it is not a strictly
/// read-only open: chunklet may already have reconciled the pool, and MetaDB
/// may replay lifecycle records and persist the recovery result. Callers own
/// opening the chunklet pool exclusively first (e.g.
/// `chunklet_pool::open_role_backend`) — the live engine must not hold its
/// flock.
pub fn verify_meta_ld(
    config: &MetaConfig,
    meta_backend: Arc<crate::io::block_backend::ChunkletBackend>,
    options: onyx_metadb::VerifyOptions,
) -> OnyxResult<onyx_metadb::VerifyReport> {
    let db = open_meta_ld_for_offline_audit(config, meta_backend)?;
    Ok(db.verify(options)?)
}

/// One volume's result in an offline chunklet-backed MetaDB point probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MetaDbProbeVolume {
    pub volume_ordinal: VolumeOrdinal,
    pub mapping: Option<MetaDbProbeMapping>,
}

/// Decoded Onyx payload plus the MetaDB-owned L2P trailer fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MetaDbProbeMapping {
    pub pba: u64,
    /// `None` for an explicit zero mapping, whose PBA field is ignored.
    pub refcount: Option<u32>,
    pub commit_seq: u64,
    pub birth_lsn: u64,
    pub compression: u8,
    pub unit_compressed_size: u32,
    pub unit_original_size: u32,
    pub unit_lba_count: u16,
    pub offset_in_unit: u16,
    pub crc32: u32,
    pub slot_offset: u16,
    pub flags: u8,
    pub is_zero: bool,
}

/// Optional independent refcount point query requested with `--pba`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MetaDbProbePba {
    pub pba: u64,
    pub refcount: u32,
}

/// Complete result of one bounded, offline MetaDB probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MetaDbProbeReport {
    pub lba: u64,
    pub volumes: Vec<MetaDbProbeVolume>,
    pub explicit_pba: Option<MetaDbProbePba>,
}

/// Open chunklet-backed MetaDB for an offline audit and perform bounded point
/// lookups.
///
/// The caller must own an exclusive chunklet pool open, which naturally
/// enforces the offline-only contract. The open can perform required pool
/// reconciliation and MetaDB lifecycle recovery before the query, but all
/// continuous background mutators are disabled. The query walks only the
/// manifest's volume ordinals, performs one L2P lookup per volume, and batches
/// the mapped/explicit PBAs through MetaDB's fold-consistent refcount reader.
/// It never scans L2P or reads LV3 payload data.
pub fn probe_meta_ld(
    config: &MetaConfig,
    meta_backend: Arc<crate::io::block_backend::ChunkletBackend>,
    lba: u64,
    explicit_pba: Option<u64>,
) -> OnyxResult<MetaDbProbeReport> {
    let db = open_meta_ld_for_offline_audit(config, meta_backend)?;

    let mut volumes = Vec::new();
    let mut refcount_pbas = Vec::new();
    for volume in db.manifest().volumes {
        let mapping = match db.get(volume.ord, lba)? {
            Some(raw) => {
                let value = decode_l2p_value(raw).map_err(|err| {
                    OnyxError::Config(format!(
                        "metadb L2P value for volume {} LBA {} is invalid: {err}",
                        volume.ord, lba
                    ))
                })?;
                if !value.is_zero() {
                    refcount_pbas.push(value.pba.0);
                }
                let commit_seq = u64::from_be_bytes(
                    raw.0[L2P_SEQ_OFFSET..L2P_BIRTH_OFFSET]
                        .try_into()
                        .expect("MetaDB L2P seq field has fixed length"),
                );
                let birth_lsn = u64::from_be_bytes(
                    raw.0[L2P_BIRTH_OFFSET..]
                        .try_into()
                        .expect("MetaDB L2P birth field has fixed length"),
                );
                Some(MetaDbProbeMapping {
                    pba: value.pba.0,
                    refcount: None,
                    commit_seq,
                    birth_lsn,
                    compression: value.compression,
                    unit_compressed_size: value.unit_compressed_size,
                    unit_original_size: value.unit_original_size,
                    unit_lba_count: value.unit_lba_count,
                    offset_in_unit: value.offset_in_unit,
                    crc32: value.crc32,
                    slot_offset: value.slot_offset,
                    flags: value.flags,
                    is_zero: value.is_zero(),
                })
            }
            None => None,
        };
        volumes.push(MetaDbProbeVolume {
            volume_ordinal: volume.ord,
            mapping,
        });
    }
    if let Some(pba) = explicit_pba {
        refcount_pbas.push(pba);
    }
    refcount_pbas.sort_unstable();
    refcount_pbas.dedup();

    let refcounts = db.multi_get_refcount_consistent(&refcount_pbas)?;
    let refcounts: HashMap<u64, u32> = refcount_pbas.into_iter().zip(refcounts).collect();
    for volume in &mut volumes {
        if let Some(mapping) = &mut volume.mapping {
            if !mapping.is_zero {
                mapping.refcount = refcounts.get(&mapping.pba).copied();
            }
        }
    }
    let explicit_pba = explicit_pba.map(|pba| MetaDbProbePba {
        pba,
        refcount: refcounts.get(&pba).copied().unwrap_or(0),
    });

    Ok(MetaDbProbeReport {
        lba,
        volumes,
        explicit_pba,
    })
}

/// Build and open the shared offline-audit MetaDB view used by point probes
/// and full verification.
///
/// This deliberately preserves layout/recovery settings from production while
/// disabling every configured background writer. It does not suppress the
/// one-time mutations required to make an existing store coherent: the caller's
/// `Pool::open` may reconcile chunklet state, and `Db::open_on_device` may replay
/// lifecycle records and commit their recovered roots.
fn open_meta_ld_for_offline_audit(
    config: &MetaConfig,
    meta_backend: Arc<crate::io::block_backend::ChunkletBackend>,
) -> OnyxResult<Arc<Db>> {
    let label = config
        .path()
        .cloned()
        .unwrap_or_else(|| PathBuf::from("<meta-ld>"));
    let db_config = metadb_config_for_offline_audit(&label, config);
    meta_ld::open_for_offline_audit(meta_backend, db_config)
}

fn metadb_config_for_offline_audit(path: &Path, config: &MetaConfig) -> MetaDbConfig {
    let mut cfg = metadb_config_from_onyx(path, config);
    sanitize_offline_audit_config(&mut cfg);
    cfg
}

/// Turn a production MetaDB config into an offline-audit config. Keep this in
/// one place so every audit entry point stays inert after open-time recovery.
pub(super) fn sanitize_offline_audit_config(cfg: &mut MetaDbConfig) {
    cfg.bfg_threads_enabled = false;
    cfg.parallel_l2p_drain_enabled = false;
    cfg.l2p_checkpoint_pipeline_enabled = false;
    cfg.l2p_writeback_enabled = false;
    cfg.dedup_drainer_enabled = false;
    // The current refcount fold is inline, but keep the legacy/configured
    // drainer disabled so a future implementation cannot silently re-arm it.
    cfg.refcount_drainer_enabled = false;
    cfg.async_reclaim_enabled = false;
    cfg.livelist_condense_min_segments = 0;
    cfg.lineage_gc_enabled = false;
    cfg.reclaim_orphans_on_open = false;
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
    cfg.dedup_drainer_enabled = config.dedup_drainer_enabled;
    cfg.dedup_drainer_interval_ms = config.dedup_drainer_interval_ms;
    cfg.dedup_drainer_threshold_entries = config.dedup_drainer_threshold_entries;
    cfg.dedup_drainer_max_entries_per_cycle = config.dedup_drainer_max_entries_per_cycle;
    cfg.dedup_drainer_backpressure_entries = config.dedup_drainer_backpressure_entries;
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
    // Spawn metadb's background BFG quiesce/sync workers so the open BFG
    // rolls on a timer and sync drains only the frozen slot. That keeps
    // buffer-ring reclaim moving instead of tying it to one giant inline
    // checkpoint.
    cfg.bfg_threads_enabled = config.bfg_threads_enabled;
    cfg.rc_checkpoint_streaming_enabled = config.rc_checkpoint_streaming_enabled;
    cfg.rc_delta_run_shadow_enabled = config.rc_delta_run_shadow_enabled;
    cfg.rc_delta_run_persist_enabled = config.rc_delta_run_persist_enabled;
    cfg.rc_condense_interval_cycles = config.rc_condense_interval_cycles;
    cfg.rc_segment_overlay_max_entries = config.rc_segment_overlay_max_entries;
    // Keep the BFG roll timer aligned with Onyx's configured L2P buffering
    // window. Leaving this at metadb's 5 s default defeats a larger durable
    // LV2 window by forcing page-tree materialisation five times per 30 s.
    cfg.bfg_timeout_ms = config.l2p_buffer_max_interval_ms;
    cfg.parallel_l2p_drain_enabled = config.parallel_l2p_drain_enabled;
    cfg.parallel_l2p_drain_workers = config.parallel_l2p_drain_workers;
    cfg.l2p_drain_chunk_entries = config.l2p_drain_chunk_entries;
    cfg.l2p_checkpoint_pipeline_enabled = config.l2p_checkpoint_pipeline_enabled;
    cfg.rc_authoritative_reclaim = config.rc_authoritative_reclaim;
    cfg.flush_select_budget = config.flush_select_budget as usize;
    cfg.async_reclaim_enabled = config.async_reclaim_enabled;
    cfg.async_reclaim_max_pages_per_cycle = config.async_reclaim_max_pages_per_cycle as usize;
    cfg.async_reclaim_idle_interval_ms = config.async_reclaim_idle_interval_ms;
    // Lineage GC driver — the production trigger for FreePbas-emitting PBA
    // reclaim. metadb defaults this OFF; onyx turns it ON (default).
    cfg.lineage_gc_enabled = config.lineage_gc_enabled;
    cfg.lineage_gc_interval_ms = config.lineage_gc_interval_ms;
    cfg.lineage_gc_max_cycles_per_wake = config.lineage_gc_max_cycles_per_wake;
    cfg.lineage_gc_drop_dedup_shared = config.lineage_gc_drop_dedup_shared;
    // Onyx treats startup as a data-plane path. Full page-file scans are
    // available through offline metadb-verify, but should not gate service
    // restart on large metadata files.
    cfg.rebuild_free_list_on_open = false;
    cfg.reclaim_orphans_on_open = false;
    cfg
}
#[cfg(test)]
mod tests;
