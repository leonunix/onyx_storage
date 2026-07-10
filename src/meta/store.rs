use std::collections::HashMap;
use std::sync::Arc;

use crate::config::MetaConfig;
use crate::error::{OnyxError, OnyxResult};
use crate::meta::backend::metadb::MetadbBackend;
use crate::meta::schema::{
    encode_blockmap_key, encode_blockmap_value, BlockmapValue, ContentHash, DedupEntry,
    MAX_VOLUME_ID_BYTES,
};
use crate::metrics::MetaMemorySnapshot;
use crate::types::{CompressionAlgo, Lba, Pba, VolumeConfig, VolumeId, BLOCK_SIZE};
use onyx_metadb::VolumeOrdinal;

/// Old physical metadata observed while replacing blockmap entries.
///
/// The cleanup thread uses the full old mapping to reconstruct the original
/// 4 KiB payload, recompute its short dedup hash, and conditionally remove the
/// matching forward dedup_index entry without maintaining a persistent reverse
/// table.
#[derive(Debug, Clone)]
pub struct RemapCleanup {
    pub pba: Pba,
    pub decrements: u32,
    pub blocks: u32,
    pub pba_freed: bool,
    pub mappings: Vec<BlockmapValue>,
}

impl RemapCleanup {
    pub fn new(mapping: BlockmapValue, blocks: u32) -> Self {
        Self {
            pba: mapping.pba,
            decrements: 1,
            blocks,
            pba_freed: false,
            mappings: vec![mapping],
        }
    }

    pub fn merge(&mut self, other: RemapCleanup) {
        debug_assert_eq!(self.pba, other.pba);
        self.decrements = self.decrements.saturating_add(other.decrements);
        self.blocks = self.blocks.max(other.blocks);
        self.pba_freed |= other.pba_freed;
        self.mappings.extend(other.mappings);
    }
}

/// A named snapshot as tracked by the onyx snapshot catalog. `(volume, name)` is
/// the user-facing identity; `snapshot_id` is the metadb `SnapshotId` the COW
/// primitives operate on. `created_lsn` is metadb's LSN at capture time;
/// `created_at` is the engine generation stamp (nanos) for human display.
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    pub volume: String,
    pub name: String,
    pub snapshot_id: u64,
    pub created_lsn: u64,
    pub created_at: u64,
    pub size_bytes: u64,
}

/// At-a-glance capacity accounting for one volume, derived from an on-demand
/// L2P scan. Ratios are internally consistent: `data_reduction = dedup × compress`.
///
/// - `dedup_ratio`     = mapped logical bytes / unique-unit original bytes
/// - `compress_ratio`  = unique-unit original bytes / unique-unit compressed bytes
/// - `data_reduction_ratio` = mapped logical bytes / physical bytes
///
/// `physical_bytes` is the LV3 footprint attributable to this volume (each
/// compressed unit counted once); it is approximate where a unit is shared
/// across volumes by global dedup.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct VolumeUsage {
    pub volume: String,
    pub logical_size_bytes: u64,
    pub mapped_lbas: u64,
    pub mapped_bytes: u64,
    pub physical_bytes: u64,
    pub unique_blocks: u64,
    pub dedup_ratio: f64,
    pub compress_ratio: f64,
    pub data_reduction_ratio: f64,
    /// Epoch seconds when this was computed. Usage is cold data served from a
    /// TTL cache, so callers should treat it as "as of" rather than live.
    pub computed_at: u64,
}

/// Outcome of an in-place snapshot restore: how many LBAs were rolled back.
#[derive(Debug, Clone, Default)]
pub struct SnapshotRestoreStats {
    /// LBAs re-pointed back to their snapshot value (overwrites + re-added deletes).
    pub lbas_remapped: u64,
    /// LBAs deleted (written after the snapshot, absent in it).
    pub lbas_deleted: u64,
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
    backend: MetadbBackend,
}

impl MetaStore {
    pub fn open(config: &MetaConfig) -> OnyxResult<Self> {
        Ok(Self {
            backend: MetadbBackend::open(config)?,
        })
    }

    /// Open the metadb store on a chunklet meta LogicalDisk. `meta_backend` is the
    /// meta-role LD backend; `pool` is the shared chunklet Pool (kept alive by the
    /// backend and reused for LV3/LV2 on a standby→active upgrade).
    pub fn open_on_meta_ld(
        config: &MetaConfig,
        meta_backend: Arc<crate::io::block_backend::ChunkletBackend>,
        pool: Arc<onyx_chunklet::Pool>,
    ) -> OnyxResult<Self> {
        Ok(Self {
            backend: MetadbBackend::open_on_meta_ld(config, meta_backend, pool)?,
        })
    }

    /// The chunklet Pool metadb was opened over (device path), or `None` on the
    /// file path.
    pub fn chunklet_pool(&self) -> Option<Arc<onyx_chunklet::Pool>> {
        self.backend.chunklet_pool()
    }

    /// Propagate an online meta-LD extend into the metadb page device (swap the
    /// extended LD, rewrite the OMET superblock, widen the ceiling). `new_ld` is
    /// a fresh `pool.open_ld` of the meta LD after `pool.extend_ld`. Errors on
    /// the file backend.
    pub fn grow_meta_capacity(
        &self,
        new_ld: Arc<dyn onyx_chunklet::ld::LogicalDisk>,
    ) -> OnyxResult<()> {
        self.backend.grow_meta_capacity(new_ld)
    }

    pub fn sync_durable(&self) -> OnyxResult<()> {
        self.backend.sync_durable()
    }

    /// [[no-refcount-hot-path-design]] Rc-neutral path: pull every PBA that the
    /// metadb-side Lineage GC has surfaced as freed (via
    /// `WalOp::FreePbas`) since the last call. The engine's
    /// [`LineageFreedPbaDrainHandle`] thread feeds these into the
    /// allocator's free list. Non-blocking; returns `Vec::new()` when
    /// the channel is empty.
    pub fn drain_lineage_freed_pbas(&self) -> Vec<Pba> {
        self.backend.drain_lineage_freed_pbas()
    }

    pub fn request_durable_checkpoint(&self) -> OnyxResult<()> {
        self.backend.request_durable_checkpoint()
    }

    pub fn try_request_durable_checkpoint(&self) -> OnyxResult<bool> {
        self.backend.try_request_durable_checkpoint()
    }

    pub(crate) fn try_request_durable_checkpoint_token(&self) -> OnyxResult<Option<u64>> {
        self.backend.try_request_durable_checkpoint_token()
    }

    /// Best-effort count of in-memory dirty work (L2P dirty page
    /// buffer + RC pending deltas). The watermark thread uses this
    /// to threshold-trigger an early checkpoint when configured.
    pub fn dirty_pages_estimate(&self) -> usize {
        self.backend.dirty_pages_estimate()
    }

    // ---- online cuckoo dedup-index modulus resize (driven by DedupScanner) ----

    /// Current resize phase + modulus + per-table live counts.
    pub fn dedup_migration_status(&self) -> onyx_metadb::dedup::DedupMigrationStatus {
        self.backend.dedup_migration_status()
    }

    /// Enter the Growing phase, targeting `target_bucket_count` buckets.
    /// Idempotent if a resize is already in progress.
    pub fn dedup_resize_begin(&self, target_bucket_count: u64) -> OnyxResult<()> {
        self.backend.dedup_resize_begin(target_bucket_count)
    }

    /// Copy up to `max_pages` OLD pages into NEW (from `start_page`, wrapping).
    /// Returns progress + resume cursor + `wrapped` (a full pass completed).
    pub fn dedup_migrate_step(
        &self,
        start_page: usize,
        max_pages: usize,
    ) -> OnyxResult<onyx_metadb::dedup::MigrateStepStats> {
        self.backend.dedup_migrate_step(start_page, max_pages)
    }

    /// Complete the resize (drop OLD, persist Single, free OLD pages).
    pub fn dedup_resize_finish(&self) -> OnyxResult<()> {
        self.backend.dedup_resize_finish()
    }

    pub(crate) fn durable_checkpoint_outcome(&self, token: u64) -> OnyxResult<Option<bool>> {
        self.backend.durable_checkpoint_outcome(token)
    }

    /// Test-only: arm the checkpoint failpoint so the next durable checkpoint
    /// reports a fatal `CapacityExhausted`, driving the durability-thread fence.
    #[cfg(test)]
    pub(crate) fn arm_checkpoint_capacity_fail(&self) {
        self.backend.arm_checkpoint_capacity_fail();
    }

    /// L2P-shard routing for the commit_worker pre-shard path. Flusher
    /// uses this to bucket a passthrough chunk's `(Lba, BlockmapValue)`
    /// pairs by L2P shard before issuing one metadb commit per shard,
    /// so each sub-commit's dispatch footprint is `{L2p(vol, sid)}`
    /// rather than `{L2p(vol, 0), ..., L2p(vol, N-1)}`. Combined with
    /// the metadb-side precise rc footprint (lanes.rs::
    /// build_lane_dispatch_plan), this lets concurrent commit_workers
    /// on a single volume dispatch in parallel instead of serializing
    /// on the dispatch_cvar.
    pub fn l2p_shard_of(&self, lba: Lba) -> usize {
        self.backend.l2p_shard_of(lba)
    }

    /// Whether PBA refcount is authoritative for all live L2P references. When
    /// true, GC reclaim frees on `rc==0` alone (Gate 1) and skips the
    /// full-volume `referenced_extents` reverify scan (Gate 2).
    pub fn rc_authoritative_reclaim(&self) -> bool {
        self.backend.rc_authoritative_reclaim()
    }

    pub fn memory_stats(&self) -> OnyxResult<MetaMemorySnapshot> {
        self.backend.memory_stats()
    }

    pub fn create_blockmap_cf(&self, vol_id: &str) -> OnyxResult<()> {
        if self.get_volume(&VolumeId(vol_id.to_string()))?.is_some() {
            return Ok(());
        }
        self.put_volume(&VolumeConfig {
            id: VolumeId(vol_id.to_string()),
            size_bytes: u64::from(BLOCK_SIZE) * 1024,
            block_size: BLOCK_SIZE,
            compression: CompressionAlgo::None,
            created_at: 0,
            zone_count: 1,
        })
    }

    pub fn drop_blockmap_cf(&self, vol_id: &str) -> OnyxResult<()> {
        let _ = self.delete_volume(&VolumeId(vol_id.to_string()))?;
        Ok(())
    }

    pub fn put_volume(&self, config: &VolumeConfig) -> OnyxResult<()> {
        let id_len = config.id.0.len();
        if id_len == 0 || id_len > MAX_VOLUME_ID_BYTES {
            return Err(OnyxError::Config(format!(
                "volume ID must be 1..{} bytes, got {}",
                MAX_VOLUME_ID_BYTES, id_len
            )));
        }
        self.backend.put_volume(config)
    }

    pub fn get_volume(&self, id: &VolumeId) -> OnyxResult<Option<VolumeConfig>> {
        self.backend.get_volume(id)
    }

    pub fn volume_ordinal_str(&self, id: &str) -> OnyxResult<VolumeOrdinal> {
        self.backend.volume_ordinal_str(id)
    }

    pub fn list_volumes(&self) -> OnyxResult<Vec<VolumeConfig>> {
        self.backend.list_volumes()
    }

    pub fn delete_volume(&self, id: &VolumeId) -> OnyxResult<Vec<RemapCleanup>> {
        self.backend.delete_volume(id)
    }

    // ── Snapshot lifecycle (see docs/onyx-phase2-snapshots.md) ──────────────

    /// Take a named point-in-time snapshot of `vol_id`. `created_at` is the
    /// engine generation stamp recorded for display. Rejects a duplicate
    /// `(volume, name)`.
    pub fn create_snapshot(
        &self,
        vol_id: &VolumeId,
        snap_name: &str,
        created_at: u64,
    ) -> OnyxResult<SnapshotInfo> {
        self.backend.create_snapshot(vol_id, snap_name, created_at)
    }

    /// List snapshots, optionally filtered to one volume.
    pub fn list_snapshots(&self, volume: Option<&str>) -> OnyxResult<Vec<SnapshotInfo>> {
        self.backend.list_snapshots(volume)
    }

    /// Drop a named snapshot. Returns the PBAs whose refcount hit zero as a
    /// result — the caller MUST retire (not direct-free) these via the
    /// `PbaLifecycle` retire path so `GcRunner` Gate 1/2 reclaims them (same
    /// contract as lineage-GC-surfaced freed PBAs).
    pub fn delete_snapshot(&self, vol_id: &VolumeId, snap_name: &str) -> OnyxResult<Vec<Pba>> {
        self.backend.delete_snapshot(vol_id, snap_name)
    }

    /// Clone a snapshot into a new writable volume `new_name`. The clone shares
    /// the snapshot's L2P pages copy-on-write; the source volume is untouched.
    /// Returns the new volume's config.
    pub fn clone_snapshot(
        &self,
        vol_id: &VolumeId,
        snap_name: &str,
        new_name: &str,
        created_at: u64,
    ) -> OnyxResult<VolumeConfig> {
        self.backend
            .clone_snapshot(vol_id, snap_name, new_name, created_at)
    }

    /// Restore `vol_id` in place to a named snapshot (destructive rollback).
    /// Replays the snapshot→current diff through metadb's atomic remap path;
    /// the caller MUST have quiesced the volume (stopped, buffer drained,
    /// metadb synced) first. Diverged PBAs are reclaimed by the lineage path.
    pub fn restore_snapshot(
        &self,
        vol_id: &VolumeId,
        snap_name: &str,
    ) -> OnyxResult<SnapshotRestoreStats> {
        self.backend.restore_snapshot(vol_id, snap_name)
    }

    pub fn put_mapping(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        value: &BlockmapValue,
    ) -> OnyxResult<()> {
        self.backend.put_mapping(vol_id, lba, value)
    }

    pub fn get_mapping(&self, vol_id: &VolumeId, lba: Lba) -> OnyxResult<Option<BlockmapValue>> {
        self.backend.get_mapping(vol_id, lba)
    }

    /// Read the L2P entry along with its committed seq. Callers that
    /// submit a derived update (DedupScanner's flag-clear and
    /// dedup-hit promotion paths) MUST forward this seq to the
    /// subsequent `l2p_remap` so apply's `seq_guard_rejects` can
    /// distinguish "I read this and nobody else has touched it" from
    /// "a newer flusher commit landed while I was working" — emitting
    /// seq=0 silently bypasses the guard and clobbers the newer write.
    pub fn get_mapping_with_seq(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
    ) -> OnyxResult<Option<(BlockmapValue, u64)>> {
        self.backend.get_mapping_with_seq(vol_id, lba)
    }

    pub fn get_mapping_str(&self, vol_id: &str, lba: Lba) -> OnyxResult<Option<BlockmapValue>> {
        self.backend.get_mapping_str(vol_id, lba)
    }

    pub fn get_mapping_ord(
        &self,
        ord: VolumeOrdinal,
        lba: Lba,
    ) -> OnyxResult<Option<BlockmapValue>> {
        self.backend.get_mapping_ord(ord, lba)
    }

    pub fn multi_get_mappings(
        &self,
        vol_id: &VolumeId,
        lbas: &[Lba],
    ) -> OnyxResult<Vec<Option<BlockmapValue>>> {
        self.backend.multi_get_mappings(vol_id, lbas)
    }

    pub fn multi_get_mappings_str(
        &self,
        vol_id: &str,
        lbas: &[Lba],
    ) -> OnyxResult<Vec<Option<BlockmapValue>>> {
        self.backend.multi_get_mappings_str(vol_id, lbas)
    }

    pub fn multi_get_mappings_ord(
        &self,
        ord: VolumeOrdinal,
        lbas: &[Lba],
    ) -> OnyxResult<Vec<Option<BlockmapValue>>> {
        self.backend.multi_get_mappings_ord(ord, lbas)
    }

    pub fn multi_get_mappings_raw_ord(
        &self,
        ord: VolumeOrdinal,
        lbas: &[onyx_metadb::Lba],
    ) -> OnyxResult<Vec<Option<BlockmapValue>>> {
        self.backend.multi_get_mappings_raw_ord(ord, lbas)
    }

    pub fn delete_mapping(&self, vol_id: &VolumeId, lba: Lba) -> OnyxResult<()> {
        self.backend.delete_mapping(vol_id, lba)
    }

    pub fn get_mappings_range(
        &self,
        vol_id: &VolumeId,
        start: Lba,
        end: Lba,
    ) -> OnyxResult<Vec<(Lba, BlockmapValue)>> {
        self.backend.get_mappings_range(vol_id, start, end)
    }

    pub fn get_mappings_range_unordered(
        &self,
        vol_id: &VolumeId,
        start: Lba,
        end: Lba,
    ) -> OnyxResult<Vec<(Lba, BlockmapValue)>> {
        self.backend
            .get_mappings_range_unordered(vol_id, start, end)
    }

    pub fn get_mappings_range_unordered_str(
        &self,
        vol_id: &str,
        start: Lba,
        end: Lba,
    ) -> OnyxResult<Vec<(Lba, BlockmapValue)>> {
        self.backend
            .get_mappings_range_unordered_str(vol_id, start, end)
    }

    pub fn get_mappings_range_unordered_ord(
        &self,
        ord: VolumeOrdinal,
        start: Lba,
        end: Lba,
    ) -> OnyxResult<Vec<(Lba, BlockmapValue)>> {
        self.backend
            .get_mappings_range_unordered_ord(ord, start, end)
    }

    pub fn delete_blockmap_range(
        &self,
        vol_id: &VolumeId,
        start_lba: Lba,
        end_lba: Lba,
    ) -> OnyxResult<Vec<RemapCleanup>> {
        self.backend
            .delete_blockmap_range(vol_id, start_lba, end_lba)
    }

    pub fn atomic_write_mapping(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        value: &BlockmapValue,
    ) -> OnyxResult<()> {
        self.backend.atomic_write_mapping(vol_id, lba, value)
    }

    pub fn atomic_remap(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        old_pba: Option<Pba>,
        new_value: &BlockmapValue,
    ) -> OnyxResult<()> {
        self.backend.atomic_remap(vol_id, lba, old_pba, new_value)
    }

    pub fn atomic_batch_write(
        &self,
        vol_id: &VolumeId,
        batch_values: &[(Lba, BlockmapValue)],
        new_refcount: u32,
    ) -> OnyxResult<HashMap<Pba, RemapCleanup>> {
        self.backend
            .atomic_batch_write(vol_id, batch_values, new_refcount)
    }

    pub fn atomic_batch_write_with_dedup(
        &self,
        vol_id: &VolumeId,
        batch_values: &[(Lba, BlockmapValue)],
        new_refcount: u32,
        dedup_entries: &[(ContentHash, DedupEntry)],
        seqs: &[u64],
    ) -> OnyxResult<(HashMap<Pba, RemapCleanup>, Vec<bool>)> {
        self.backend.atomic_batch_write_with_dedup(
            vol_id,
            batch_values,
            new_refcount,
            dedup_entries,
            seqs,
        )
    }

    pub fn atomic_batch_write_packed(
        &self,
        batch_values: &[(VolumeId, Lba, BlockmapValue)],
        new_pba: Pba,
        new_refcount: u32,
    ) -> OnyxResult<HashMap<Pba, RemapCleanup>> {
        self.backend
            .atomic_batch_write_packed(batch_values, new_pba, new_refcount)
    }

    pub fn atomic_batch_write_packed_with_dedup(
        &self,
        batch_values: &[(VolumeId, Lba, BlockmapValue)],
        new_pba: Pba,
        new_refcount: u32,
        dedup_entries: &[(ContentHash, DedupEntry)],
        seqs: &[u64],
    ) -> OnyxResult<(HashMap<Pba, RemapCleanup>, Vec<bool>)> {
        self.backend.atomic_batch_write_packed_with_dedup(
            batch_values,
            new_pba,
            new_refcount,
            dedup_entries,
            seqs,
        )
    }

    pub fn atomic_batch_write_multi(
        &self,
        units: &[(&VolumeId, &[(Lba, BlockmapValue)], u32)],
    ) -> OnyxResult<HashMap<Pba, RemapCleanup>> {
        self.backend.atomic_batch_write_multi(units)
    }

    pub fn atomic_batch_write_multi_with_dedup(
        &self,
        units: &[(&VolumeId, &[(Lba, BlockmapValue)], u32)],
        dedup_entries: &[(ContentHash, DedupEntry)],
        seqs: &[u64],
    ) -> OnyxResult<(HashMap<Pba, RemapCleanup>, Vec<bool>)> {
        self.backend
            .atomic_batch_write_multi_with_dedup(units, dedup_entries, seqs)
    }

    /// Deferred-outcome surface used by the commit_worker pipeline
    /// (see `commit_worker/passthrough.rs`).
    /// `recv()` on the returned handle reproduces the sync tuple
    /// returned by [`Self::atomic_batch_write_multi_with_dedup`].
    pub fn atomic_batch_write_multi_with_dedup_deferred(
        &self,
        units: &[(&VolumeId, &[(Lba, BlockmapValue)], u32)],
        dedup_entries: &[(ContentHash, DedupEntry)],
        seqs: &[u64],
    ) -> OnyxResult<crate::meta::backend::metadb::DeferredCleanupHandle> {
        self.backend
            .atomic_batch_write_multi_with_dedup_deferred(units, dedup_entries, seqs)
    }

    pub fn get_refcount(&self, pba: Pba) -> OnyxResult<u32> {
        self.backend.get_refcount(pba)
    }

    pub fn multi_get_refcounts(&self, pbas: &[Pba]) -> OnyxResult<Vec<u32>> {
        self.backend.multi_get_refcounts(pbas)
    }

    /// Fold-consistent refcount read for the GC reclaim path. See
    /// `MetadbBackend::multi_get_refcounts_consistent`: the plain read can
    /// transiently floor a live rc to 0 across a refcount fold, which under
    /// `rc_authoritative_reclaim` would free a still-referenced PBA. The
    /// irreversible reclaim decision MUST use this.
    pub fn multi_get_refcounts_consistent(&self, pbas: &[Pba]) -> OnyxResult<Vec<u32>> {
        self.backend.multi_get_refcounts_consistent(pbas)
    }

    /// Test-only seed; see `MetadbBackend::set_refcount`. Rc-neutral
    /// writes removed the per-write refcount path, but several tests need
    /// to prep an existing rc value on a PBA before exercising
    /// dedup-hit / packed-slot scenarios. Production code must not
    /// call this.
    #[doc(hidden)]
    pub fn set_refcount(&self, pba: Pba, count: u32) -> OnyxResult<()> {
        self.backend.set_refcount(pba, count)
    }

    pub fn atomic_dedup_hit(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        new_value: &BlockmapValue,
        hash: &ContentHash,
        seq: u64,
    ) -> OnyxResult<Option<RemapCleanup>> {
        self.backend
            .atomic_dedup_hit(vol_id, lba, new_value, hash, seq)
    }

    pub fn atomic_batch_dedup_hits(
        &self,
        vol_id: &VolumeId,
        hits: &[(Lba, BlockmapValue, ContentHash)],
    ) -> OnyxResult<(Vec<DedupHitResult>, HashMap<Pba, RemapCleanup>)> {
        self.backend.atomic_batch_dedup_hits(vol_id, hits)
    }

    /// Same as [`atomic_batch_dedup_hits`] but also writes
    /// `promote_entries` into `dedup_index` in the same metadb
    /// transaction. Used by the promote-on-verified-hit path so the
    /// dedup_index registration and the LBA remap land atomically — a
    /// crash between the two would leave the cache promotion half-applied.
    pub fn atomic_batch_dedup_hits_with_promote(
        &self,
        vol_id: &VolumeId,
        hits: &[(Lba, BlockmapValue, ContentHash)],
        promote_entries: &[(ContentHash, DedupEntry)],
        seqs: &[u64],
    ) -> OnyxResult<(Vec<DedupHitResult>, HashMap<Pba, RemapCleanup>)> {
        self.backend
            .atomic_batch_dedup_hits_with_promote(vol_id, hits, promote_entries, seqs)
    }

    pub fn get_dedup_entry(&self, hash: &ContentHash) -> OnyxResult<Option<DedupEntry>> {
        self.backend.get_dedup(hash)
    }

    pub fn multi_get_dedup_entries(
        &self,
        hashes: &[ContentHash],
    ) -> OnyxResult<Vec<Option<DedupEntry>>> {
        self.backend.multi_get_dedup(hashes)
    }

    pub fn put_dedup_entries(&self, entries: &[(ContentHash, DedupEntry)]) -> OnyxResult<()> {
        self.backend.put_dedup_entries(entries)
    }

    pub fn delete_dedup_index(&self, hash: &ContentHash) -> OnyxResult<()> {
        self.backend.delete_dedup_index(hash)
    }

    pub fn delete_dedup_index_if_matches(
        &self,
        hash: &ContentHash,
        mapping: &BlockmapValue,
    ) -> OnyxResult<bool> {
        self.backend.delete_dedup_index_if_matches(hash, mapping)
    }

    pub fn compare_put_dedup_index(
        &self,
        hash: &ContentHash,
        old_entry: &DedupEntry,
        new_entry: &DedupEntry,
    ) -> OnyxResult<bool> {
        self.backend
            .compare_put_dedup_index(hash, old_entry, new_entry)
    }

    pub fn dedup_entry_is_live(&self, hash: &ContentHash, entry: &DedupEntry) -> OnyxResult<bool> {
        self.backend.dedup_entry_is_live(hash, entry)
    }

    pub fn multi_dedup_entries_are_live(
        &self,
        entries: &[(ContentHash, DedupEntry)],
    ) -> OnyxResult<Vec<bool>> {
        self.backend.multi_dedup_entries_are_live(entries)
    }

    pub fn scan_dedup_skipped(
        &self,
        limit: usize,
    ) -> OnyxResult<Vec<(String, Lba, BlockmapValue)>> {
        self.backend.scan_dedup_skipped(limit)
    }

    pub fn update_blockmap_flags(
        &self,
        vol_id: &VolumeId,
        lba: Lba,
        new_flags: u8,
    ) -> OnyxResult<()> {
        self.backend.update_blockmap_flags(vol_id, lba, new_flags)
    }

    pub fn has_any_blockmap_ref(&self, target_pba: Pba) -> OnyxResult<bool> {
        self.backend.has_any_blockmap_ref(target_pba)
    }

    /// Batched blockmap-reference check for GC retired-extent reclaim: one
    /// all-volume L2P scan answers all `(start_pba, blocks)` candidates at once.
    /// Returns one bool per candidate (input order): true iff some live
    /// blockmap entry references a PBA inside it.
    pub fn referenced_extents(&self, extents: &[(Pba, u32)]) -> OnyxResult<Vec<bool>> {
        self.backend.referenced_extents(extents)
    }

    pub fn count_blockmap_refs_for_pba(&self, target_pba: Pba) -> OnyxResult<u32> {
        self.backend.count_blockmap_refs_for_pba(target_pba)
    }

    pub fn scan_all_blockmap_entries(
        &self,
        callback: &mut dyn FnMut(&str, &[u8], &[u8]),
    ) -> OnyxResult<()> {
        self.backend
            .scan_all_blockmap_entries_with(&mut |vol_id, lba, value| {
                let key = encode_blockmap_key(lba);
                let val = encode_blockmap_value(&value);
                callback(&vol_id.0, &key, &val);
            })?;
        Ok(())
    }

    /// Visit live blockmap entries for one volume in `[start_lba, start_lba + count)`.
    /// Order within the range is shard-internal (not LBA-sorted), so callers that
    /// only need to bound work per cycle can checkpoint by `start + count` and
    /// resume on the next cycle. Used by the dedup scanner's cold-tail warming
    /// pass to walk live mappings whose hashes are not yet in the candidate cache.
    pub fn scan_blockmap_range(
        &self,
        vol_id: &VolumeId,
        start_lba: Lba,
        count: u64,
        callback: &mut dyn FnMut(Lba, BlockmapValue),
    ) -> OnyxResult<()> {
        self.backend
            .scan_blockmap_range(vol_id, start_lba, count, callback)
    }

    /// Refcount recovery is owned by metadb's WAL/BFG replay (performed during
    /// [`MetaStore::open`]); there is no separate onyx-side rebuild that walks
    /// the per-volume blockmaps. This is intentionally a validation/no-op hook
    /// the dirty-startup path calls so the recovery contract has a named home
    /// and a place to add cheap assertions later. Do NOT reintroduce a
    /// "rebuild" that re-derives refcount from L2P — that double-counts against
    /// metadb's authoritative ledger.
    pub fn recover_or_validate_refcount(&self) -> OnyxResult<()> {
        Ok(())
    }

    pub fn iter_refcounts(&self) -> OnyxResult<Vec<(Pba, u32)>> {
        self.backend.iter_refcounts()
    }

    pub fn iter_dedup_entries(&self) -> OnyxResult<Vec<(ContentHash, DedupEntry)>> {
        self.backend.iter_dedup_entries()
    }

    /// Resumable, bounded scan over the dedup forward index (the scalable
    /// alternative to [`Self::iter_dedup_entries`] for background sweeps): up to
    /// `limit` entries from `cursor`, plus the resume cursor and `wrapped`.
    pub fn scan_dedup_from(
        &self,
        cursor: onyx_metadb::DedupScanCursor,
        limit: usize,
    ) -> OnyxResult<(Vec<(ContentHash, DedupEntry)>, onyx_metadb::DedupScanCursor, bool)> {
        self.backend.scan_dedup_from(cursor, limit)
    }

    /// PBAs the allocator must reserve on rebuild — the COMPLETE liveness
    /// read-view (folded L2P tree ∪ l2p_buffer ∪ dedup_index), delegated to the
    /// backend so it is the single source of truth shared with the GC reclaim
    /// gate (`referenced_extents`).
    ///
    /// The previous body scanned the blockmap only, which OMITS dedup-only
    /// blocks: a promoted dedup entry keeps its 4K block live at rc>0 after every
    /// L2P sharer LBA has been overwritten (until orphan-reclaim demotes it). On a
    /// reused pool those blocks are absent from the blockmap, so
    /// `rebuild_from_metadata` treated a still-referenced PBA as a free gap → the
    /// next allocation reused a live block → foreground CRC. The dedup-union fix
    /// had been applied to `MetadbBackend::iter_allocated_blocks`, but rebuild
    /// reaches the free-list through THIS wrapper, so the fix never ran on the
    /// live path (root-caused 2026-07-02).
    pub fn iter_allocated_blocks(&self) -> OnyxResult<Vec<Pba>> {
        Ok(self
            .backend
            .iter_allocated_blocks()?
            .into_iter()
            .map(|(pba, _)| pba)
            .collect())
    }
}
