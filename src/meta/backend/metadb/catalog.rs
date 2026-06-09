use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use onyx_metadb::{Config as MetaDbConfig, Db, VolumeOrdinal};
use serde::{Deserialize, Serialize};

use crate::config::MetaConfig;
use crate::error::{OnyxError, OnyxResult};
use crate::types::VolumeConfig;

pub(super) const CATALOG_FILE: &str = "onyx-volume-catalog.bin";
const CATALOG_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct VolumeCatalogFile {
    version: u32,
    volumes: Vec<VolumeCatalogEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct VolumeCatalogEntry {
    pub(super) ordinal: VolumeOrdinal,
    pub(super) config: VolumeConfig,
}

#[derive(Clone, Debug, Default)]
pub(super) struct VolumeCatalog {
    pub(super) by_id: HashMap<String, VolumeCatalogEntry>,
}
impl VolumeCatalog {
    pub(super) fn load(path: &Path) -> OnyxResult<Self> {
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

    pub(super) fn persist(&self, path: &Path) -> OnyxResult<()> {
        let mut volumes: Vec<VolumeCatalogEntry> = self.by_id.values().cloned().collect();
        volumes.sort_by_key(|entry| entry.ordinal);
        let file = VolumeCatalogFile {
            version: CATALOG_VERSION,
            volumes,
        };
        let bytes = bincode::serialize(&file).map_err(|e| OnyxError::Config(e.to_string()))?;
        atomic_write(path, &bytes)
    }

    pub(super) fn validate_against_db(&self, db: &Db) -> OnyxResult<()> {
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
    cfg.commit_direct_apply_enabled = config.commit_direct_apply_enabled;
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
