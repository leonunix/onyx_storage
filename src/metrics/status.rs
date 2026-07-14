use std::fmt::Write;

use serde::Serialize;

use super::{BufferShardSnapshot, EngineMetricsSnapshot, MetaMemorySnapshot};
use crate::gc::heatmap::HeatSummary;

#[derive(Debug, Clone, Default, Serialize)]
pub struct EngineStatusSnapshot {
    /// "active", "standby", or "meta-only"
    pub mode: String,
    pub volume_count: usize,
    pub live_handle_count: usize,
    pub zone_count: Option<u32>,
    pub buffer_pending_entries: Option<u64>,
    /// Unapplied logical work as a percentage of LV2 capacity.
    pub buffer_fill_pct: Option<u8>,
    /// Fullest shard's physical ring occupancy, including applied entries
    /// retained until the next metadb checkpoint covers them.
    pub buffer_physical_fill_pct: Option<u8>,
    pub buffer_payload_memory_bytes: Option<u64>,
    pub buffer_payload_memory_limit_bytes: Option<u64>,
    pub metadb_memory: Option<MetaMemorySnapshot>,
    pub buffer_shards: Vec<BufferShardSnapshot>,
    pub allocator_free_blocks: Option<u64>,
    pub allocator_total_blocks: Option<u64>,
    /// Free-pool contiguity (fragmentation) snapshot: fragment count, largest
    /// contiguous run, whole-stripe-capable blocks, and the free-set block
    /// total they are measured against (excludes lane caches). None in
    /// meta-only/standby or (for stripe_capable) without RAID geometry.
    pub allocator_free_extents: Option<u64>,
    pub allocator_largest_run_blocks: Option<u64>,
    pub allocator_stripe_capable_blocks: Option<u64>,
    pub allocator_free_blocks_in_set: Option<u64>,
    pub allocator_stripe_reserve_blocks: Option<u64>,
    pub allocator_quarantine_target_blocks: Option<u64>,
    pub allocator_quarantine_free_blocks: Option<u64>,
    /// Adaptive reclaim heat-map summary (None when the map is disabled or in
    /// standby). Observe-only in Stage A.
    pub heat: Option<HeatSummary>,
    /// Metadb persistence fence reason, if tripped. `Some` means the durability
    /// path has latched off new writes (metadb checkpoints failing fatally or
    /// repeatedly); the engine must be restarted to clear it.
    pub meta_fenced: Option<String>,
    /// chunklet pool topology + health snapshot: `Some` under the chunklet
    /// backend, `None` on the file backend or if the live `pool.metrics()` read
    /// fails. Serialized verbatim into `status-json`; the text form renders a
    /// pool summary plus a per-LD line for any degraded/rebuilding LD.
    pub chunklet: Option<onyx_chunklet::ops::PoolSnapshot>,
    /// Live cuckoo dedup-index bucket count (modulus). Grows online as the
    /// unique-hash working set reveals itself (see the online-resize scanner).
    pub dedup_cuckoo_buckets: u64,
    /// True while an online cuckoo modulus resize (Growing phase) is in flight.
    pub dedup_resize_growing: bool,
    /// The OLD (smaller, draining) modulus during a resize; 0 when not resizing.
    pub dedup_resize_old_buckets: u64,
    pub metrics: EngineMetricsSnapshot,
}

impl EngineStatusSnapshot {
    pub fn render_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "mode: {}", self.mode);
        if let Some(reason) = &self.meta_fenced {
            let _ = writeln!(out, "META_FENCED: {} (restart required)", reason);
        }
        let _ = writeln!(out, "uptime_secs: {}", self.metrics.uptime_secs);
        let _ = writeln!(out, "volumes: {}", self.volume_count);
        let _ = writeln!(out, "live_handles: {}", self.live_handle_count);
        if let Some(zone_count) = self.zone_count {
            let _ = writeln!(out, "zones: {}", zone_count);
        }
        if let Some(pending) = self.buffer_pending_entries {
            let _ = writeln!(out, "buffer_pending_entries: {}", pending);
        }
        if let Some(fill_pct) = self.buffer_fill_pct {
            let _ = writeln!(out, "buffer_fill_pct: {}", fill_pct);
        }
        if let Some(fill_pct) = self.buffer_physical_fill_pct {
            let _ = writeln!(out, "buffer_physical_fill_pct: {}", fill_pct);
        }
        if let Some(payload_bytes) = self.buffer_payload_memory_bytes {
            let limit = self.buffer_payload_memory_limit_bytes.unwrap_or(0);
            let _ = writeln!(
                out,
                "buffer_payload_memory_bytes: {}/{}",
                payload_bytes, limit
            );
        }
        if let Some(metadb) = &self.metadb_memory {
            let _ = writeln!(
                out,
                "metadb_block_cache_bytes: usage={} pinned={} capacity={}",
                metadb.block_cache_usage_bytes.unwrap_or(0),
                metadb.block_cache_pinned_usage_bytes.unwrap_or(0),
                metadb.block_cache_capacity_bytes.unwrap_or(0)
            );
            let _ = writeln!(
                out,
                "metadb_meta_bytes: memtables_current={} memtables_total={} table_readers={}",
                metadb.cur_size_all_mem_tables_bytes,
                metadb.size_all_mem_tables_bytes,
                metadb.estimate_table_readers_mem_bytes
            );
            if let Some(total) = metadb.total_estimate_bytes() {
                let _ = writeln!(out, "metadb_total_estimate_bytes: {}", total);
            }
            let _ = writeln!(
                out,
                "metadb_state: last_applied_lsn={} high_water_pages={} free_list_pages={}",
                metadb.last_applied_lsn, metadb.high_water_pages, metadb.free_list_pages
            );
            let _ = writeln!(
                out,
                "metadb_dedup_lsm: index_ssts={} index_records={}",
                metadb.dedup_index_ssts, metadb.dedup_index_records
            );
            let _ = writeln!(
                out,
                "metadb_dedup_cuckoo: l0_distinct_fps={} l0_approx_bytes={} l1_entries={}",
                metadb.dedup_l0_distinct_fps, metadb.dedup_l0_approx_bytes, metadb.dedup_l1_entries
            );
            if self.dedup_resize_growing {
                let _ = writeln!(
                    out,
                    "metadb_dedup_resize: GROWING {} -> {} buckets (migrating)",
                    self.dedup_resize_old_buckets, self.dedup_cuckoo_buckets
                );
            } else {
                let _ = writeln!(
                    out,
                    "metadb_dedup_resize: steady {} buckets",
                    self.dedup_cuckoo_buckets
                );
            }
            let _ = writeln!(
                out,
                "metadb_cache: hits={} misses={} evictions={} pages={}/{} pinned_pages={} pin_budget_bytes={}",
                metadb.cache_hits,
                metadb.cache_misses,
                metadb.cache_evictions,
                metadb.cache_current_pages,
                metadb.cache_capacity_bytes / 4096,
                metadb.cache_pinned_pages,
                metadb.cache_pin_budget_bytes
            );
            let _ = writeln!(
                out,
                "metadb_pending: dispatch={} deferred_free={} dedup_lane_q={} l2p_apply_q={} l2p_priv={} l2p_retired={} l2p_buf_total={} l2p_buf_dirty={} rc_apply_q={} rc_priv={} rc_retired={} rc_buf_total={} rc_buf_dirty={}",
                metadb.pending_dispatch,
                metadb.pending_deferred_free,
                metadb.pending_dedup_lane_queue,
                metadb.pending_l2p_apply_queue,
                metadb.pending_l2p_private_pages,
                metadb.pending_l2p_retired_pages,
                metadb.pending_l2p_pagebuf_total,
                metadb.pending_l2p_pagebuf_dirty,
                metadb.pending_rc_apply_queue,
                metadb.pending_rc_private_pages,
                metadb.pending_rc_retired_pages,
                metadb.pending_rc_pagebuf_total,
                metadb.pending_rc_pagebuf_dirty
            );
            let _ = writeln!(
                out,
                "metadb_flush: calls={} pages={} bytes={} total_us={} max_us={} gate_us={} gate_max_us={} sample_us={} sample_max_us={} io_us={} io_max_us={} manifest_us={} manifest_max_us={} install_us={} install_max_us={} reclaim_us={} reclaim_max_us={} reclaim_budget_pages={} reclaim_selected_pages={} reclaim_reclaimed_pages={} reclaim_blocked_pages={}",
                metadb.flush_calls,
                metadb.flush_pages_written,
                metadb.flush_io_bytes_total,
                metadb.flush_total_us,
                metadb.flush_total_max_us,
                metadb.flush_gate_wait_us,
                metadb.flush_gate_wait_max_us,
                metadb.flush_sample_us,
                metadb.flush_sample_max_us,
                metadb.flush_io_us,
                metadb.flush_io_max_us,
                metadb.flush_manifest_us,
                metadb.flush_manifest_max_us,
                metadb.flush_install_us,
                metadb.flush_install_max_us,
                metadb.flush_reclaim_us,
                metadb.flush_reclaim_max_us,
                metadb.flush_reclaim_budget_pages,
                metadb.flush_reclaim_selected_pages,
                metadb.flush_reclaim_reclaimed_pages,
                metadb.flush_reclaim_blocked_pages
            );
            let _ = writeln!(
                out,
                "metadb_lineage_gc: advanced={} dead_pbas={} dropped_dedup_shared={} skipped_rc={} skipped_snap={} skipped_descendant={} blocked_rc0_pbas={}",
                metadb.lineage_gc_head_advanced,
                metadb.lineage_gc_head_dead_pbas,
                metadb.lineage_gc_head_dropped_dedup_shared,
                metadb.lineage_gc_head_skipped_rc,
                metadb.lineage_gc_head_skipped_snap,
                metadb.lineage_gc_head_skipped_descendant,
                metadb.lineage_gc_head_blocked_rc0_pbas
            );
            let _ = writeln!(
                out,
                "metadb_meta_io_write: calls={} ops={} bytes={} us={} max_us={} batch_ops_max={} batch_bytes_max={}",
                metadb.meta_io_write_calls,
                metadb.meta_io_write_ops,
                metadb.meta_io_write_bytes,
                metadb.meta_io_write_us,
                metadb.meta_io_write_max_us,
                metadb.meta_io_write_batch_ops_max,
                metadb.meta_io_write_batch_bytes_max
            );
            let _ = writeln!(
                out,
                "metadb_meta_io_read: calls={} ops={} bytes={} us={} max_us={} batch_ops_max={}",
                metadb.meta_io_read_calls,
                metadb.meta_io_read_ops,
                metadb.meta_io_read_bytes,
                metadb.meta_io_read_us,
                metadb.meta_io_read_max_us,
                metadb.meta_io_read_batch_ops_max
            );
            let _ = writeln!(
                out,
                "metadb_meta_io_fsync: calls={} us={} max_us={}",
                metadb.meta_io_fsync_calls, metadb.meta_io_fsync_us, metadb.meta_io_fsync_max_us
            );
            let _ = writeln!(
                out,
                "metadb_write_uring_lock: acquires={} wait_us={} wait_max_us={}",
                metadb.meta_io_write_uring_lock_acquires,
                metadb.meta_io_write_uring_lock_wait_us,
                metadb.meta_io_write_uring_lock_wait_max_us
            );
            let _ = writeln!(
                out,
                "metadb_io_submitter: iterations={} sqes_submitted={} channel_pending_max={} submit_batch_size_max={} inflight_max={} bg_deferred={} bg_inflight_max={} bg_deferred_max={}",
                metadb.io_submitter_iterations,
                metadb.io_submitter_sqes_submitted,
                metadb.io_submitter_channel_pending_max,
                metadb.io_submitter_submit_batch_size_max,
                metadb.io_submitter_inflight_max,
                metadb.io_submitter_bg_deferred,
                metadb.io_submitter_bg_inflight_max,
                metadb.io_submitter_bg_deferred_max,
            );
            let _ = writeln!(
                out,
                "metadb_commit_ops: total={} per_tx_max={}",
                metadb.commit_ops, metadb.commit_ops_per_tx_max,
            );
            let _ = writeln!(
                out,
                "metadb_flush_kind: steady_calls={} steady_total_us={} steady_total_max_us={} steady_sample_us={} steady_sample_max_us={} forced_calls={} forced_total_us={} forced_total_max_us={} forced_sample_us={} forced_sample_max_us={}",
                metadb.flush_calls_steady,
                metadb.flush_total_us_steady,
                metadb.flush_total_max_us_steady,
                metadb.flush_sample_us_steady,
                metadb.flush_sample_max_us_steady,
                metadb.flush_calls_forced,
                metadb.flush_total_us_forced,
                metadb.flush_total_max_us_forced,
                metadb.flush_sample_us_forced,
                metadb.flush_sample_max_us_forced
            );
            let _ = writeln!(
                out,
                "metadb_flush_sample_work: l2p_dirty_pages={} l2p_dirty_pages_max={} rc_drained_deltas={} rc_drained_deltas_max={} rc_fresh_pages={} rc_fresh_pages_max={}",
                metadb.flush_sample_l2p_dirty_pages,
                metadb.flush_sample_l2p_dirty_pages_max,
                metadb.flush_sample_rc_drained_deltas,
                metadb.flush_sample_rc_drained_deltas_max,
                metadb.flush_sample_rc_fresh_pages,
                metadb.flush_sample_rc_fresh_pages_max
            );
            let _ = writeln!(
                out,
                "metadb_flush_sample_breakdown: lock_us={} lock_max_us={} l2p_walk_us={} l2p_walk_max_us={} rc_drain_us={} rc_drain_max_us={}",
                metadb.flush_sample_lock_us,
                metadb.flush_sample_lock_max_us,
                metadb.flush_sample_l2p_walk_us,
                metadb.flush_sample_l2p_walk_max_us,
                metadb.flush_sample_rc_drain_us,
                metadb.flush_sample_rc_drain_max_us
            );
            let _ = writeln!(
                out,
                "metadb_flush_prepare: dedup_drain_us={} dedup_drain_max_us={} l2p_fold_us={} l2p_fold_max_us={}",
                metadb.flush_dedup_drain_us,
                metadb.flush_dedup_drain_max_us,
                metadb.flush_l2p_fold_us,
                metadb.flush_l2p_fold_max_us
            );
            let _ = writeln!(
                out,
                "metadb_l2p_checkpoint_pipeline: attempts={} completed={} skipped={} errors={} work_us={} work_max_us={} wait_us={} wait_max_us={}",
                metadb.l2p_prefold_attempts,
                metadb.l2p_prefold_completed,
                metadb.l2p_prefold_skipped,
                metadb.l2p_prefold_errors,
                metadb.l2p_prefold_us,
                metadb.l2p_prefold_max_us,
                metadb.l2p_prefold_wait_us,
                metadb.l2p_prefold_wait_max_us
            );
            let _ = writeln!(
                out,
                "metadb_l2p_fold_pipeline: shard_cycles={} entries={} leaves={} chunks={} work_us={} shard_max_us={} plan_us={} plan_max_us={} tree_wait_us={} tree_wait_max_us={} apply_us={} apply_max_us={} publish_us={} publish_max_us={} finish_us={} finish_max_us={} sync_phase_us={} sync_phase_max_us={}",
                metadb.l2p_buffer_compaction_cycles,
                metadb.l2p_buffer_compaction_entries,
                metadb.l2p_buffer_compaction_leaves,
                metadb.l2p_buffer_compaction_chunks,
                metadb.l2p_buffer_compaction_us,
                metadb.l2p_buffer_compaction_max_us,
                metadb.l2p_buffer_compaction_plan_us,
                metadb.l2p_buffer_compaction_plan_max_us,
                metadb.l2p_buffer_compaction_tree_wait_us,
                metadb.l2p_buffer_compaction_tree_wait_max_us,
                metadb.l2p_buffer_compaction_apply_us,
                metadb.l2p_buffer_compaction_apply_max_us,
                metadb.l2p_buffer_compaction_publish_us,
                metadb.l2p_buffer_compaction_publish_max_us,
                metadb.l2p_buffer_compaction_finish_us,
                metadb.l2p_buffer_compaction_finish_max_us,
                metadb.flush_l2p_fold_us,
                metadb.flush_l2p_fold_max_us
            );
            let _ = writeln!(
                out,
                "metadb_flush_io_breakdown: seal_us={} seal_max_us={} page_write_us={} page_write_max_us={} rc_meta_us={} rc_meta_max_us={} fsync_us={} fsync_max_us={}",
                metadb.flush_io_seal_us,
                metadb.flush_io_seal_max_us,
                metadb.flush_io_page_write_us,
                metadb.flush_io_page_write_max_us,
                metadb.flush_io_rc_meta_us,
                metadb.flush_io_rc_meta_max_us,
                metadb.flush_io_sync_us,
                metadb.flush_io_sync_max_us
            );
            let _ = writeln!(
                out,
                "metadb_flush_publish: barrier_wait_us={} barrier_wait_max_us={} gate_wait_us={} gate_wait_max_us={} gate_hold_us={} gate_hold_max_us={} manifest_us={} manifest_max_us={} stage_us={} stage_max_us={} publish_us={} publish_max_us={} cleanup_us={} cleanup_max_us={}",
                metadb.flush_publish_barrier_wait_us,
                metadb.flush_publish_barrier_wait_max_us,
                metadb.flush_gate_wait_us,
                metadb.flush_gate_wait_max_us,
                metadb.flush_gate_hold_us,
                metadb.flush_gate_hold_max_us,
                metadb.flush_manifest_us,
                metadb.flush_manifest_max_us,
                metadb.flush_manifest_stage_us,
                metadb.flush_manifest_stage_max_us,
                metadb.flush_manifest_publish_us,
                metadb.flush_manifest_publish_max_us,
                metadb.flush_manifest_cleanup_us,
                metadb.flush_manifest_cleanup_max_us
            );
            let _ = writeln!(
                out,
                "metadb_async_reclaim: cycles={} selected_pages={} reclaimed_pages={} cycle_us={} cycle_max_us={}",
                metadb.async_reclaim_cycles,
                metadb.async_reclaim_selected_pages,
                metadb.async_reclaim_reclaimed_pages,
                metadb.async_reclaim_cycle_us,
                metadb.async_reclaim_cycle_max_us
            );
            let _ = writeln!(
                out,
                "metadb_rc_drainer: cycles={} wakes={} preempts={} drained_entries={} pages_built={} cycle_us={} cycle_max_us={} overlay_size_max_pages={} checkpoint_wait_us={} checkpoint_wait_max_us={} backpressure_fallbacks={} pool_refills={}",
                metadb.rc_drainer_cycles,
                metadb.rc_drainer_wakes,
                metadb.rc_drainer_preempts,
                metadb.rc_drainer_drained_entries,
                metadb.rc_drainer_pages_built,
                metadb.rc_drainer_cycle_us,
                metadb.rc_drainer_cycle_max_us,
                metadb.rc_drainer_overlay_size_max_pages,
                metadb.rc_drainer_checkpoint_wait_us,
                metadb.rc_drainer_checkpoint_wait_max_us,
                metadb.rc_drainer_backpressure_fallbacks,
                metadb.rc_drainer_pool_refills
            );
            let _ = writeln!(
                out,
                "metadb_commit: attempts={} success={} errors={} empty={} ops={} wal_body_bytes={} wal_body_bytes_max={} total_us={} max_us={} drop_gate_wait_us={} drop_gate_wait_max_us={} wal_submit_us={} wal_submit_max_us={} apply_wait_us={} apply_wait_max_us={} apply_gate_wait_us={} apply_gate_wait_max_us={} finish_global_wait_us={} finish_global_wait_max_us={} apply_us={} apply_max_us={} plan_us={} plan_max_us={} encode_us={} encode_max_us={} unlogged_gate_wait_us={} unlogged_gate_wait_max_us={} checkpoint_unlogged_us={} checkpoint_unlogged_max_us={} read_held_us={} read_held_max_us={}",
                metadb.commit_attempts,
                metadb.commit_success,
                metadb.commit_errors,
                metadb.commit_empty,
                metadb.commit_ops,
                metadb.commit_wal_body_bytes,
                metadb.commit_wal_body_bytes_max,
                metadb.commit_total_us,
                metadb.commit_total_max_us,
                metadb.commit_drop_gate_wait_us,
                metadb.commit_drop_gate_wait_max_us,
                metadb.commit_wal_submit_us,
                metadb.commit_wal_submit_max_us,
                metadb.commit_apply_wait_us,
                metadb.commit_apply_wait_max_us,
                metadb.commit_apply_gate_wait_us,
                metadb.commit_apply_gate_wait_max_us,
                metadb.commit_finish_global_wait_us,
                metadb.commit_finish_global_wait_max_us,
                metadb.commit_apply_us,
                metadb.commit_apply_max_us,
                metadb.commit_plan_us,
                metadb.commit_plan_max_us,
                metadb.commit_encode_us,
                metadb.commit_encode_max_us,
                metadb.commit_unlogged_gate_wait_us,
                metadb.commit_unlogged_gate_wait_max_us,
                metadb.commit_checkpoint_unlogged_us,
                metadb.commit_checkpoint_unlogged_max_us,
                metadb.commit_read_held_us,
                metadb.commit_read_held_max_us,
            );
            let _ = writeln!(
                out,
                "metadb_commit_direct_apply: count={} us={} max_us={}",
                metadb.commit_direct_apply_count,
                metadb.commit_direct_apply_us,
                metadb.commit_direct_apply_max_us,
            );
            let _ = writeln!(
                out,
                "metadb_commit_apply_split: l2p_wait_us={} l2p_wait_max_us={} rc_enqueue_us={} rc_enqueue_max_us={} rc_wait_us={} rc_wait_max_us={} dedup_enqueue_us={} dedup_enqueue_max_us={} dedup_wait_us={} dedup_wait_max_us={}",
                metadb.commit_apply_l2p_wait_us,
                metadb.commit_apply_l2p_wait_max_us,
                metadb.commit_apply_rc_enqueue_us,
                metadb.commit_apply_rc_enqueue_max_us,
                metadb.commit_apply_rc_wait_us,
                metadb.commit_apply_rc_wait_max_us,
                metadb.commit_apply_dedup_enqueue_us,
                metadb.commit_apply_dedup_enqueue_max_us,
                metadb.commit_apply_dedup_wait_us,
                metadb.commit_apply_dedup_wait_max_us
            );
            let _ = writeln!(
                out,
                "metadb_wal: submits={} batches={} records={} bytes={} rotates={} fsyncs={} write_us={} write_max_us={} fsync_us={} fsync_max_us={} batch_records_max={} batch_bytes_max={}",
                metadb.wal_submit_calls,
                metadb.wal_batches,
                metadb.wal_records,
                metadb.wal_bytes,
                metadb.wal_rotates,
                metadb.wal_fsyncs,
                metadb.wal_write_us,
                metadb.wal_write_max_us,
                metadb.wal_fsync_us,
                metadb.wal_fsync_max_us,
                metadb.wal_batch_records_max,
                metadb.wal_batch_bytes_max
            );
            let _ = writeln!(
                out,
                "metadb_wal_submit_breakdown: queue_wait_us={} queue_wait_max_us={} writer_busy_us={} writer_busy_max_us={} wake_roundtrip_us={} wake_roundtrip_max_us={}",
                metadb.wal_queue_wait_us,
                metadb.wal_queue_wait_max_us,
                metadb.wal_writer_busy_us,
                metadb.wal_writer_busy_max_us,
                metadb.wal_wake_roundtrip_us,
                metadb.wal_wake_roundtrip_max_us
            );
            let _ = writeln!(
                out,
                "metadb_range_delete: calls={} success={} errors={} noop={} captured={} chunks={} total_us={} max_us={}",
                metadb.range_delete_calls,
                metadb.range_delete_success,
                metadb.range_delete_errors,
                metadb.range_delete_noop,
                metadb.range_delete_captured_entries,
                metadb.range_delete_chunks,
                metadb.range_delete_total_us,
                metadb.range_delete_total_max_us
            );
            let _ = writeln!(
                out,
                "metadb_apply_by_op: l2p_put={}/{} (max={}) l2p_delete={}/{} (max={}) l2p_remap={}/{} (max={}) l2p_remap_range={}/{}/{} (max={}) l2p_range_delete={}/{} (max={}) refcount={}/{} (max={}) dedup={}/{} (max={})",
                metadb.apply_l2p_put_count,
                metadb.apply_l2p_put_us,
                metadb.apply_l2p_put_max_us,
                metadb.apply_l2p_delete_count,
                metadb.apply_l2p_delete_us,
                metadb.apply_l2p_delete_max_us,
                metadb.apply_l2p_remap_count,
                metadb.apply_l2p_remap_us,
                metadb.apply_l2p_remap_max_us,
                metadb.apply_l2p_remap_range_count,
                metadb.apply_l2p_remap_range_lbas,
                metadb.apply_l2p_remap_range_us,
                metadb.apply_l2p_remap_range_max_us,
                metadb.apply_l2p_range_delete_count,
                metadb.apply_l2p_range_delete_us,
                metadb.apply_l2p_range_delete_max_us,
                metadb.apply_refcount_count,
                metadb.apply_refcount_us,
                metadb.apply_refcount_max_us,
                metadb.apply_dedup_count,
                metadb.apply_dedup_us,
                metadb.apply_dedup_max_us
            );
            let _ = writeln!(
                out,
                "metadb_apply_refcount_batch: batches={} actions={} pbas={} sampled_pbas={} grouping_us={} (max={}) base_lookup_us={} (max={}) pending_scan_us={} (max={}) delta_merge_us={} (max={})",
                metadb.apply_refcount_batch_count,
                metadb.apply_refcount_batch_actions,
                metadb.apply_refcount_batch_pbas,
                metadb.apply_refcount_breakdown_sampled_pbas,
                metadb.apply_refcount_pba_grouping_us,
                metadb.apply_refcount_pba_grouping_max_us,
                metadb.apply_refcount_base_page_lookup_us,
                metadb.apply_refcount_base_page_lookup_max_us,
                metadb.apply_refcount_pending_slot_scan_us,
                metadb.apply_refcount_pending_slot_scan_max_us,
                metadb.apply_refcount_delta_merge_us,
                metadb.apply_refcount_delta_merge_max_us,
            );
            let _ = writeln!(
                out,
                "metadb_apply_lane_l2p: tasks={} queue_depth_max={} queue_wait_us={} queue_wait_max_us={} exec_us={} exec_max_us={} idle_us={} idle_max_us={} wakeups={} empty_wakeups={} burst_total={} burst_max={}",
                metadb.l2p_apply_lane_tasks,
                metadb.l2p_apply_lane_queue_depth_max,
                metadb.l2p_apply_lane_queue_wait_us,
                metadb.l2p_apply_lane_queue_wait_max_us,
                metadb.l2p_apply_lane_exec_us,
                metadb.l2p_apply_lane_exec_max_us,
                metadb.l2p_apply_lane_idle_us,
                metadb.l2p_apply_lane_idle_max_us,
                metadb.l2p_apply_lane_wakeups,
                metadb.l2p_apply_lane_empty_wakeups,
                metadb.l2p_apply_lane_burst_total,
                metadb.l2p_apply_lane_burst_max
            );
            let _ = writeln!(
                out,
                "metadb_apply_lane_rc: tasks={} queue_depth_max={} queue_wait_us={} queue_wait_max_us={} exec_us={} exec_max_us={} idle_us={} idle_max_us={} pending_set_wait_us={} pending_set_wait_max_us={} wakeups={} empty_wakeups={} burst_total={} burst_max={}",
                metadb.rc_apply_lane_tasks,
                metadb.rc_apply_lane_queue_depth_max,
                metadb.rc_apply_lane_queue_wait_us,
                metadb.rc_apply_lane_queue_wait_max_us,
                metadb.rc_apply_lane_exec_us,
                metadb.rc_apply_lane_exec_max_us,
                metadb.rc_apply_lane_idle_us,
                metadb.rc_apply_lane_idle_max_us,
                metadb.rc_apply_lane_pending_set_wait_us,
                metadb.rc_apply_lane_pending_set_wait_max_us,
                metadb.rc_apply_lane_wakeups,
                metadb.rc_apply_lane_empty_wakeups,
                metadb.rc_apply_lane_burst_total,
                metadb.rc_apply_lane_burst_max
            );
            let _ = writeln!(
                out,
                "metadb_dedup_lane: tasks={} ops={} queue_depth_max={} ready_queue_wait_us={} ready_queue_wait_max_us={} exec_us={} exec_max_us={} idle_us={} idle_max_us={} wakeups={} empty_wakeups={} burst_total={} burst_max={}",
                metadb.dedup_lane_tasks,
                metadb.dedup_lane_ops,
                metadb.dedup_lane_queue_depth_max,
                metadb.dedup_lane_ready_queue_wait_us,
                metadb.dedup_lane_ready_queue_wait_max_us,
                metadb.dedup_lane_exec_us,
                metadb.dedup_lane_exec_max_us,
                metadb.dedup_lane_idle_us,
                metadb.dedup_lane_idle_max_us,
                metadb.dedup_lane_wakeups,
                metadb.dedup_lane_empty_wakeups,
                metadb.dedup_lane_burst_total,
                metadb.dedup_lane_burst_max
            );
            let _ = writeln!(
                out,
                "metadb_l2p_get: calls={} lock_wait_us={} lock_wait_max_us={} tree_walk_us={} tree_walk_max_us={}",
                metadb.l2p_get_calls,
                metadb.l2p_get_lock_wait_us,
                metadb.l2p_get_lock_wait_max_us,
                metadb.l2p_get_tree_walk_us,
                metadb.l2p_get_tree_walk_max_us
            );
            let _ = writeln!(
                out,
                "metadb_l2p_multi_get: calls={} lbas={} pin_us={} pin_max_us={} volume_us={} volume_max_us={} sort_us={} sort_max_us={} view_us={} view_max_us={} tree_us={} tree_max_us={}",
                metadb.l2p_multi_get_calls,
                metadb.l2p_multi_get_lbas,
                metadb.l2p_multi_get_pin_us,
                metadb.l2p_multi_get_pin_max_us,
                metadb.l2p_multi_get_volume_us,
                metadb.l2p_multi_get_volume_max_us,
                metadb.l2p_multi_get_sort_us,
                metadb.l2p_multi_get_sort_max_us,
                metadb.l2p_multi_get_view_us,
                metadb.l2p_multi_get_view_max_us,
                metadb.l2p_multi_get_tree_us,
                metadb.l2p_multi_get_tree_max_us
            );
            let _ = writeln!(
                out,
                "metadb_cleanup: calls={} success={} errors={} noop={} pbas={} hashes_found={} forward_checks={} tombstones={} tx_ops={} total_us={} max_us={} scan_us={} scan_max_us={} forward_check_us={} forward_check_max_us={} commit_us={} commit_max_us={}",
                metadb.cleanup_calls,
                metadb.cleanup_success,
                metadb.cleanup_errors,
                metadb.cleanup_noop,
                metadb.cleanup_pbas,
                metadb.cleanup_hashes_found,
                metadb.cleanup_forward_checks,
                metadb.cleanup_tombstones_emitted,
                metadb.cleanup_tx_ops,
                metadb.cleanup_total_us,
                metadb.cleanup_total_max_us,
                metadb.cleanup_scan_us,
                metadb.cleanup_scan_max_us,
                metadb.cleanup_forward_check_us,
                metadb.cleanup_forward_check_max_us,
                metadb.cleanup_commit_us,
                metadb.cleanup_commit_max_us
            );
        }
        if let (Some(free), Some(total)) = (self.allocator_free_blocks, self.allocator_total_blocks)
        {
            let _ = writeln!(out, "allocator_free_blocks: {}/{}", free, total);
        }
        if let (Some(extents), Some(largest), Some(in_set)) = (
            self.allocator_free_extents,
            self.allocator_largest_run_blocks,
            self.allocator_free_blocks_in_set,
        ) {
            let capable = self.allocator_stripe_capable_blocks;
            let capable_pct = match capable {
                Some(cap) if in_set > 0 => Some(cap * 100 / in_set),
                _ => None,
            };
            let _ = writeln!(
                out,
                "allocator_contiguity: free_extents={} largest_run={} stripe_capable={}/{} ({}%) reserve={} quarantine={}/{}",
                extents,
                largest,
                capable.unwrap_or(0),
                in_set,
                capable_pct.map_or_else(|| "-".to_string(), |p| p.to_string()),
                self.allocator_stripe_reserve_blocks.unwrap_or(0),
                self.allocator_quarantine_free_blocks.unwrap_or(0),
                self.allocator_quarantine_target_blocks.unwrap_or(0),
            );
        }
        if let Some(ck) = &self.chunklet {
            let _ = writeln!(
                out,
                "chunklet_pool: id={} pds={}/{} healthy (failed={} draining={} drained={}) lds={} cpgs={}",
                ck.pool_id,
                ck.healthy_pds,
                ck.pd_count,
                ck.failed_pds,
                ck.draining_pds,
                ck.drained_pds,
                ck.ld_count,
                ck.cpg_count,
            );
            let _ = writeln!(
                out,
                "chunklet_capacity: used={}/{} user_bytes spare={} bad={} migrating={} (raw={})",
                ck.used_bytes,
                ck.user_bytes,
                ck.spare_bytes,
                ck.bad_bytes,
                ck.migrating_bytes,
                ck.raw_bytes,
            );
            // One line per LD that is not fully healthy (any member down/degraded
            // or a rebuild target). Healthy LDs stay quiet to keep status terse.
            for ld in &ck.lds {
                let degraded = ld.unavailable_members
                    + ld.bad_members
                    + ld.failed_members
                    + ld.draining_members
                    + ld.drained_members;
                if degraded > 0 {
                    let _ = writeln!(
                        out,
                        "  chunklet_ld[{}] {} degraded: unavailable={} bad={} failed={} draining={} drained={} (members={})",
                        ld.ld_id,
                        ld.raid_level,
                        ld.unavailable_members,
                        ld.bad_members,
                        ld.failed_members,
                        ld.draining_members,
                        ld.drained_members,
                        ld.member_count,
                    );
                }
            }
        }
        let _ = writeln!(
            out,
            "volume_ops: create={} delete={} open={} read={} write={} discard={}",
            self.metrics.volume_create_ops,
            self.metrics.volume_delete_ops,
            self.metrics.volume_open_ops,
            self.metrics.volume_read_ops,
            self.metrics.volume_write_ops,
            self.metrics.volume_discard_ops
        );
        let _ = writeln!(
            out,
            "volume_bytes: read={} write={}",
            self.metrics.volume_read_bytes, self.metrics.volume_write_bytes
        );
        let _ = writeln!(
            out,
            "read_path: buffer_hits={} lv3_hits={} unmapped={} crc_errors={} crc_fg={} dedup_verify_mismatches={} crc_dedup_scanner={} decompress_errors={}",
            self.metrics.read_buffer_hits,
            self.metrics.read_lv3_hits,
            self.metrics.read_unmapped,
            self.metrics.read_crc_errors,
            self.metrics.read_crc_errors_foreground,
            self.metrics.dedup_verify_mismatches,
            self.metrics.read_crc_errors_dedup_scanner,
            self.metrics.read_decompress_errors
        );
        let _ = writeln!(
            out,
            "buffer: appends={} append_bytes={} write_ops={} write_bytes={} read_ops={} read_bytes={} lookup_hits={} lookup_misses={} backpressure_events={} throttle_count={} throttle_us_total={} throttle_us_max={} hydration_skips={} hydration_head_bypass={}",
            self.metrics.buffer_appends,
            self.metrics.buffer_append_bytes,
            self.metrics.buffer_write_ops,
            self.metrics.buffer_write_bytes,
            self.metrics.buffer_read_ops,
            self.metrics.buffer_read_bytes,
            self.metrics.buffer_lookup_hits,
            self.metrics.buffer_lookup_misses,
            self.metrics.buffer_backpressure_events,
            self.metrics.buffer_throttle_count,
            self.metrics.buffer_throttle_us_total,
            self.metrics.buffer_throttle_us_max,
            self.metrics.buffer_hydration_skipped_due_to_mem_limit,
            self.metrics.buffer_hydration_head_bypass_count
        );
        let _ = writeln!(
            out,
            "buffer_lookup_split: index_ns={} hydrate_ns={} hydrate_ops={}",
            self.metrics.buffer_lookup_index_ns,
            self.metrics.buffer_lookup_hydrate_ns,
            self.metrics.buffer_lookup_hydrate_ops
        );
        let _ = writeln!(
            out,
            "buffer_payload_cache: evict_entries={} evict_bytes={}",
            self.metrics.buffer_payload_cache_evict_entries,
            self.metrics.buffer_payload_cache_evict_bytes
        );
        let _ = writeln!(
            out,
            "buffer_coalesce_hydrate: ns={} ops={} entries={} memory={} disk={}",
            self.metrics.buffer_coalesce_hydrate_ns,
            self.metrics.buffer_coalesce_hydrate_ops,
            self.metrics.buffer_coalesce_hydrate_entries,
            self.metrics.buffer_coalesce_hydrate_memory_entries,
            self.metrics.buffer_coalesce_hydrate_disk_entries,
        );
        let _ = writeln!(
            out,
            "lv3_io: read_ops={} read_compressed_bytes={} read_decompressed_bytes={} write_ops={} write_compressed_bytes={} write_batch_calls={} write_batch_ops={} write_batch_bytes={} write_batch_ns={} write_batch_ns_max={} write_batch_inflight={} write_batch_inflight_max={} write_slab_allocs={} write_slab_bytes={}",
            self.metrics.lv3_read_ops,
            self.metrics.lv3_read_compressed_bytes,
            self.metrics.lv3_read_decompressed_bytes,
            self.metrics.lv3_write_ops,
            self.metrics.lv3_write_compressed_bytes,
            self.metrics.lv3_write_batch_calls,
            self.metrics.lv3_write_batch_ops,
            self.metrics.lv3_write_batch_bytes,
            self.metrics.lv3_write_batch_ns,
            self.metrics.lv3_write_batch_ns_max,
            self.metrics.lv3_write_batch_inflight,
            self.metrics.lv3_write_batch_inflight_max,
            self.metrics.lv3_write_slab_allocs,
            self.metrics.lv3_write_slab_bytes
        );
        let _ = writeln!(
            out,
            "read_pool: requests={} batches={} batch_ops={} queue_wait_ns={} coalesce_wait_ns={} alloc_ns={} submit_wait_ns={} decode_ns={}",
            self.metrics.read_pool_requests,
            self.metrics.read_pool_batches,
            self.metrics.read_pool_batch_ops,
            self.metrics.read_pool_queue_wait_ns,
            self.metrics.read_pool_coalesce_wait_ns,
            self.metrics.read_pool_alloc_ns,
            self.metrics.read_pool_submit_wait_ns,
            self.metrics.read_pool_decode_ns
        );
        let _ = writeln!(
            out,
            "read_submit: calls={} total_ns={} buffer_lookup_ns={} meta_get_ns={} unit_io_ns={}",
            self.metrics.read_submit_calls,
            self.metrics.read_submit_total_ns,
            self.metrics.read_submit_buffer_lookup_ns,
            self.metrics.read_submit_meta_get_ns,
            self.metrics.read_submit_unit_io_ns
        );
        let _ = writeln!(
            out,
            "read_submit_meta_split: query_ns={} route_ns={}",
            self.metrics.read_submit_meta_query_ns, self.metrics.read_submit_meta_route_ns
        );
        let _ = writeln!(
            out,
            "user_io_latency_ns: read_total={} write_total={}",
            self.metrics.volume_read_total_ns, self.metrics.volume_write_total_ns
        );
        let _ = writeln!(
            out,
            "ublk_read_split_ns: queue_wait={} worker={} completion_wait={}",
            self.metrics.ublk_read_queue_wait_ns,
            self.metrics.ublk_read_worker_ns,
            self.metrics.ublk_read_completion_wait_ns
        );
        let _ = writeln!(
            out,
            "ublk_write_split_ns: queue_wait={} worker={} completion_wait={}",
            self.metrics.ublk_write_queue_wait_ns,
            self.metrics.ublk_write_worker_ns,
            self.metrics.ublk_write_completion_wait_ns
        );
        let _ = writeln!(
            out,
            "front_write_ns: zone_submit={} zone_worker={} append_total={} append_prepare={} append_order_wait={} append_order_hold={} append_order_wait_max={} append_order_hold_max={} append_log_write={} append_wait_durable={} append_backpressure_wait={} sync_batches={} sync_flushes={} sync_batch_ns={} sync_sleep_ns={} sync_epochs={}",
            self.metrics.zone_submit_write_ns,
            self.metrics.zone_worker_write_ns,
            self.metrics.buffer_append_total_ns,
            self.metrics.buffer_append_prepare_ns,
            self.metrics.buffer_append_order_wait_ns,
            self.metrics.buffer_append_order_hold_ns,
            self.metrics.buffer_append_order_wait_max_ns,
            self.metrics.buffer_append_order_hold_max_ns,
            self.metrics.buffer_append_log_write_ns,
            self.metrics.buffer_append_wait_durable_ns,
            self.metrics.buffer_backpressure_wait_ns,
            self.metrics.buffer_sync_batches,
            self.metrics.buffer_sync_flushes,
            self.metrics.buffer_sync_batch_ns,
            self.metrics.buffer_sync_sleep_ns,
            self.metrics.buffer_sync_epochs_committed
        );
        let _ = writeln!(
            out,
            "buffer_sync_batch: entries={} bytes={} entries_max={} bytes_max={}",
            self.metrics.buffer_sync_entries,
            self.metrics.buffer_sync_bytes,
            self.metrics.buffer_sync_entries_max,
            self.metrics.buffer_sync_bytes_max
        );
        let _ = writeln!(
            out,
            "zone: write_dispatches={} write_splits={} write_lbas={} read_dispatches={}",
            self.metrics.zone_write_dispatches,
            self.metrics.zone_write_split_ops,
            self.metrics.zone_write_lbas,
            self.metrics.zone_read_dispatches
        );
        let _ = writeln!(
            out,
            "flush: coalesce_runs={} units={} lbas={} raw_bytes={} superseded_entries={} superseded_lbas={} compressed_units={} compressed_in={} compressed_out={} compression_bypass_units={} compression_bypass_bytes={} written_units={} written_bytes={} packed_slots={} packed_fragments={} packed_bytes={} stale_discards={} seq_rejects={} errors={}",
            self.metrics.coalesce_runs,
            self.metrics.coalesced_units,
            self.metrics.coalesced_lbas,
            self.metrics.coalesced_bytes,
            self.metrics.coalesce_superseded_entries,
            self.metrics.coalesce_superseded_lbas,
            self.metrics.compress_units,
            self.metrics.compress_input_bytes,
            self.metrics.compress_output_bytes,
            self.metrics.compress_bypass_units,
            self.metrics.compress_bypass_bytes,
            self.metrics.flush_units_written,
            self.metrics.flush_unit_bytes,
            self.metrics.flush_packed_slots_written,
            self.metrics.flush_packed_fragments_written,
            self.metrics.flush_packed_bytes,
            self.metrics.flush_stale_discards,
            self.metrics.flush_seq_rejects,
            self.metrics.flush_errors
        );
        let _ = writeln!(
            out,
            "flush_writer_ns: total={} alloc={} io={} meta={} meta_build={} meta_commit={} meta_candidate={} meta_repair={} cleanup={} dedup_index={} mark_flushed={} precheck_live_pba={}",
            self.metrics.flush_writer_total_ns,
            self.metrics.flush_writer_alloc_ns,
            self.metrics.flush_writer_io_ns,
            self.metrics.flush_writer_meta_ns,
            self.metrics.flush_writer_meta_build_ns,
            self.metrics.flush_writer_meta_commit_ns,
            self.metrics.flush_writer_meta_candidate_ns,
            self.metrics.flush_writer_meta_repair_ns,
            self.metrics.flush_writer_cleanup_ns,
            self.metrics.flush_writer_dedup_index_ns,
            self.metrics.flush_writer_mark_flushed_ns,
            self.metrics.flush_writer_precheck_live_pba_ns
        );
        let _ = writeln!(
            out,
            "flush_writer_meta: commits={} lbas={} pt_commits={} pt_lbas={} packed_commits={} packed_lbas={}",
            self.metrics.flush_writer_meta_commits,
            self.metrics.flush_writer_meta_lbas,
            self.metrics.flush_writer_meta_pt_commits,
            self.metrics.flush_writer_meta_pt_lbas,
            self.metrics.flush_writer_meta_packed_commits,
            self.metrics.flush_writer_meta_packed_lbas,
        );
        let _ = writeln!(
            out,
            "flush_qos: mode={} rate_bps={} foreground_bps={} foreground_outstanding={} durable_p99_ns={} logical_pct={} physical_pct={} payload_pct={} admitted_bytes={} wait_ns={} wait_events={} wait_max_ns={} waiters={} waiters_max={} idle_bypass={} emergency_bypass={} emergency_transitions={} rate_up={} rate_down={}",
            self.metrics.flush_qos_mode,
            self.metrics.flush_qos_rate_bytes_per_sec,
            self.metrics.flush_qos_foreground_bytes_per_sec,
            self.metrics.foreground_io_outstanding,
            self.metrics.flush_qos_durable_p99_ns,
            self.metrics.flush_qos_logical_fill_pct,
            self.metrics.flush_qos_physical_fill_pct,
            self.metrics.flush_qos_payload_fill_pct,
            self.metrics.flush_qos_admitted_bytes,
            self.metrics.flush_qos_wait_ns,
            self.metrics.flush_qos_wait_events,
            self.metrics.flush_qos_wait_max_ns,
            self.metrics.flush_qos_waiters,
            self.metrics.flush_qos_waiters_max,
            self.metrics.flush_qos_idle_bypasses,
            self.metrics.flush_qos_emergency_bypasses,
            self.metrics.flush_qos_emergency_transitions,
            self.metrics.flush_qos_rate_increases,
            self.metrics.flush_qos_rate_decreases,
        );
        let _ = writeln!(
            out,
            "flush_writer_batch: cycles={} cycles_full={} cycles_partial={} read_active_cycles={} drained_units={} drained_units_max={} rx_pending_max={} commit_send_ns={} commit_send_ops={} commit_send_len_max={} commit_worker_queue_wait_ns={} commit_worker_aggregator_residence_ns={} commit_worker_executor_queue_wait_ns={} commit_worker_service_ns={} commit_worker_jobs={} commit_worker_job_lbas={} commit_worker_drain_batches={} commit_worker_drain_jobs={} commit_worker_drain_lbas={} commit_worker_drain_jobs_max={} commit_worker_drain_lbas_max={} pt_batches={} pt_units={} pt_lbas={} pt_io_ops={} pt_units_max={} pt_lbas_max={} pt_io_ops_max={} packed_batches={} packed_slots={} packed_lbas={} packed_io_ops={} packed_slots_max={} packed_lbas_max={} packed_io_ops_max={}",
            self.metrics.flush_writer_cycles,
            self.metrics.flush_writer_cycles_full,
            self.metrics.flush_writer_cycles_partial,
            self.metrics.flush_writer_read_active_cycles,
            self.metrics.flush_writer_drained_units,
            self.metrics.flush_writer_drained_units_max,
            self.metrics.flush_writer_rx_pending_max,
            self.metrics.flush_writer_commit_send_ns,
            self.metrics.flush_writer_commit_send_ops,
            self.metrics.flush_writer_commit_send_len_max,
            self.metrics.flush_commit_worker_queue_wait_ns,
            self.metrics.flush_commit_worker_aggregator_residence_ns,
            self.metrics.flush_commit_worker_executor_queue_wait_ns,
            self.metrics.flush_commit_worker_service_ns,
            self.metrics.flush_commit_worker_jobs,
            self.metrics.flush_commit_worker_job_lbas,
            self.metrics.flush_commit_worker_drain_batches,
            self.metrics.flush_commit_worker_drain_jobs,
            self.metrics.flush_commit_worker_drain_lbas,
            self.metrics.flush_commit_worker_drain_jobs_max,
            self.metrics.flush_commit_worker_drain_lbas_max,
            self.metrics.flush_writer_pt_batches,
            self.metrics.flush_writer_pt_units,
            self.metrics.flush_writer_pt_lbas,
            self.metrics.flush_writer_pt_io_ops,
            self.metrics.flush_writer_pt_units_max,
            self.metrics.flush_writer_pt_lbas_max,
            self.metrics.flush_writer_pt_io_ops_max,
            self.metrics.flush_writer_packed_batches,
            self.metrics.flush_writer_packed_batch_slots,
            self.metrics.flush_writer_packed_batch_lbas,
            self.metrics.flush_writer_packed_batch_io_ops,
            self.metrics.flush_writer_packed_batch_slots_max,
            self.metrics.flush_writer_packed_batch_lbas_max,
            self.metrics.flush_writer_packed_batch_io_ops_max
        );
        let _ = writeln!(
            out,
            "commit_executor_load: queue_depth={} queue_depth_max={} active={} active_max={} executor_queue_wait_max_ns={} service_batches={}",
            self.metrics.flush_commit_executor_queue_depth,
            self.metrics.flush_commit_executor_queue_depth_max,
            self.metrics.flush_commit_executors_active,
            self.metrics.flush_commit_executors_active_max,
            self.metrics
                .flush_commit_worker_executor_queue_wait_max_ns,
            self.metrics.flush_commit_worker_service_batches,
        );
        let _ = writeln!(
            out,
            "commit_aggregator_seals: target={} capacity={} deadline={} adaptive_underfill={} pressure={} shutdown={}",
            self.metrics.flush_commit_aggregator_seals_target,
            self.metrics.flush_commit_aggregator_seals_capacity,
            self.metrics.flush_commit_aggregator_seals_deadline,
            self.metrics
                .flush_commit_aggregator_seals_adaptive_underfill,
            self.metrics.flush_commit_aggregator_seals_pressure,
            self.metrics.flush_commit_aggregator_seals_shutdown,
        );
        let _ = writeln!(
            out,
            "flush_stage_send: coalesce_ns={} coalesce_ops={} coalesce_len_sum={} coalesce_len_max={} dedup_ns={} dedup_ops={} dedup_len_sum={} dedup_len_max={} compress_ns={} compress_ops={} compress_len_sum={} compress_len_max={}",
            self.metrics.flush_stage_coalesce_send_ns,
            self.metrics.flush_stage_coalesce_send_ops,
            self.metrics.flush_stage_coalesce_send_len_sum,
            self.metrics.flush_stage_coalesce_send_len_max,
            self.metrics.flush_stage_dedup_send_ns,
            self.metrics.flush_stage_dedup_send_ops,
            self.metrics.flush_stage_dedup_send_len_sum,
            self.metrics.flush_stage_dedup_send_len_max,
            self.metrics.flush_stage_compress_send_ns,
            self.metrics.flush_stage_compress_send_ops,
            self.metrics.flush_stage_compress_send_len_sum,
            self.metrics.flush_stage_compress_send_len_max
        );
        let _ = writeln!(
            out,
            "flush_stage_busy: coalesce_active_ns={} coalesce_idle_ns={} dedup_active_ns={} dedup_idle_ns={} dedup_iters={} compress_active_ns={} compress_idle_ns={} commit_rx_idle_ns={} commit_rx_idle_iters={}",
            self.metrics.flush_coalesce_active_ns,
            self.metrics.flush_coalesce_idle_ns,
            self.metrics.flush_dedup_worker_active_ns,
            self.metrics.flush_dedup_worker_idle_ns,
            self.metrics.flush_dedup_worker_iters,
            self.metrics.flush_compress_worker_active_ns,
            self.metrics.flush_compress_worker_idle_ns,
            self.metrics.flush_commit_worker_rx_idle_ns,
            self.metrics.flush_commit_worker_rx_idle_iters
        );
        let _ = writeln!(
            out,
            "flush_coalesce_inside: pending_ns={} pending_ops={} phase2_dedup_ns={} phase3_sort_ns={} phase4_merge_ns={}",
            self.metrics.flush_coalesce_pending_ns,
            self.metrics.flush_coalesce_pending_ops,
            self.metrics.flush_coalesce_phase2_dedup_ns,
            self.metrics.flush_coalesce_phase3_sort_ns,
            self.metrics.flush_coalesce_phase4_merge_ns,
        );
        let _ = writeln!(
            out,
            "flush_compress_inside: raw_build_ns={} codec_ns={} crc_ns={}",
            self.metrics.flush_compress_raw_build_ns,
            self.metrics.flush_compress_codec_ns,
            self.metrics.flush_compress_crc_ns,
        );
        let _ = writeln!(
            out,
            "flush_writer_mark: pt_calls={} pt_lbas={} pt_calls_max={} packed_calls={} packed_lbas={} packed_calls_max={}",
            self.metrics.flush_writer_pt_mark_calls,
            self.metrics.flush_writer_pt_mark_lbas,
            self.metrics.flush_writer_pt_mark_calls_max,
            self.metrics.flush_writer_packed_mark_calls,
            self.metrics.flush_writer_packed_mark_lbas,
            self.metrics.flush_writer_packed_mark_calls_max
        );
        let _ = writeln!(
            out,
            "flush_cleanup_thread: ns={} batches={}",
            self.metrics.flush_cleanup_thread_ns, self.metrics.flush_cleanup_thread_batches
        );
        let _ = writeln!(
            out,
            "flush_writer_precheck: live_pba_ops={} live_pba_failures={}",
            self.metrics.flush_writer_precheck_live_pba_ops,
            self.metrics.flush_writer_precheck_live_pba_failures
        );
        let _ = writeln!(
            out,
            "dedup: hits={} misses={} skipped_units={} hit_failures={} lookups={} live_checks={} stale_entries={} hit_commits={} promotions_committed={} promotions_failed={} promote_skipped_inflight={} cleanup_reconstruct_errors={} cleanup_delete_errors={} rescan_cycles={} rescan_skipped_cycles={} rescan_blocks={} rescan_hits={} rescan_misses={} rescan_errors={} cold_tail_blocks={} cold_tail_already_warm={} cold_tail_errors={} cold_tail_remaps={} cold_tail_drained={} orphan_demoted={} orphan_retired={} orphan_skipped_hot={} scrub_retired={} ref_bitmap_published={}",
            self.metrics.dedup_hits,
            self.metrics.dedup_misses,
            self.metrics.dedup_skipped_units,
            self.metrics.dedup_hit_failures,
            self.metrics.dedup_lookup_ops,
            self.metrics.dedup_live_check_ops,
            self.metrics.dedup_stale_index_entries,
            self.metrics.dedup_hit_commit_ops,
            self.metrics.dedup_promotions_committed,
            self.metrics.dedup_promotions_failed,
            self.metrics.dedup_promote_skipped_inflight,
            self.metrics.dedup_cleanup_reconstruct_errors,
            self.metrics.dedup_cleanup_delete_errors,
            self.metrics.dedup_rescan_cycles,
            self.metrics.dedup_rescan_skipped_cycles,
            self.metrics.dedup_rescan_blocks,
            self.metrics.dedup_rescan_hits,
            self.metrics.dedup_rescan_misses,
            self.metrics.dedup_rescan_errors,
            self.metrics.dedup_cold_tail_blocks,
            self.metrics.dedup_cold_tail_already_warm,
            self.metrics.dedup_cold_tail_errors,
            self.metrics.dedup_cold_tail_remaps,
            self.metrics.dedup_cold_tail_drained,
            self.metrics.dedup_orphan_demoted,
            self.metrics.dedup_orphan_retired,
            self.metrics.dedup_orphan_skipped_hot,
            self.metrics.dedup_scrub_retired,
            self.metrics.dedup_ref_bitmap_published
        );
        let _ = writeln!(
            out,
            "dedup_ns: lookup={} live_check={} stale_delete={} hit_commit={}",
            self.metrics.dedup_lookup_ns,
            self.metrics.dedup_live_check_ns,
            self.metrics.dedup_stale_delete_ns,
            self.metrics.dedup_hit_commit_ns
        );
        let _ = writeln!(
            out,
            "gc: cycles={} paused_cycles={} candidates={} rewrite_attempts={} rewritten_blocks={} retired_in={} retired_reclaimed={} retired_depth={} grace_deferred={} rc_rejected={} compactable_dead={} slot_evac_cand={} slot_evac_blocks={} slot_evac_incomplete={} slot_evac_costcap={} lineage_freed={} lineage_idempotent={} reclaim_stuck={} premature_free_averted={} errors={}",
            self.metrics.gc_cycles,
            self.metrics.gc_paused_cycles,
            self.metrics.gc_candidates_found,
            self.metrics.gc_rewrite_attempts,
            self.metrics.gc_blocks_rewritten,
            self.metrics.pba_blocks_retired,
            self.metrics.gc_retired_blocks_reclaimed,
            self.metrics.gc_retired_blocks_depth,
            self.metrics.gc_reclaim_grace_deferred,
            self.metrics.gc_reclaim_rc_rejected,
            self.metrics.gc_compactable_dead_blocks,
            self.metrics.gc_slot_evac_candidates,
            self.metrics.gc_slot_evac_blocks,
            self.metrics.gc_slot_evac_incomplete_skips,
            self.metrics.gc_slot_evac_cost_cap_skips,
            self.metrics.gc_lineage_freed_blocks,
            self.metrics.gc_lineage_idempotent_frees,
            self.metrics.pba_reclaim_stuck,
            self.metrics.gc_reclaim_premature_free_averted,
            self.metrics.gc_errors
        );
        let _ = writeln!(
            out,
            "defrag: mode={} targets={} target_blocks={} quarantine_free={} reserve={} completed={} cancelled={} walk_extents={} clusters_ok={} clusters_rej={} candidates={} selected={} reappended={} dedup_rejected={} stripe_starved_batches={} group_aligned={} group_unaligned={} group_short={} group_fallback_units={} group_unused_blocks={}",
            self.metrics.gc_defrag_mode_active,
            self.metrics.gc_defrag_targets_active,
            self.metrics.gc_defrag_target_blocks,
            self.metrics.allocator_quarantine_free_blocks,
            self.metrics.allocator_stripe_reserve_blocks,
            self.metrics.gc_defrag_segments_completed,
            self.metrics.gc_defrag_segments_cancelled,
            self.metrics.gc_defrag_walk_extents,
            self.metrics.gc_defrag_clusters_qualified,
            self.metrics.gc_defrag_clusters_rejected,
            self.metrics.gc_defrag_candidates,
            self.metrics.gc_defrag_blocks_selected,
            self.metrics.gc_defrag_blocks_moved,
            self.metrics.gc_defrag_dedup_hits_rejected,
            self.metrics.flush_writer_stripe_starved_batches,
            self.metrics.flush_writer_group_aligned_ops,
            self.metrics.flush_writer_group_unaligned_ops,
            self.metrics.flush_writer_group_short_extent_allocs,
            self.metrics.flush_writer_group_fallback_units,
            self.metrics.flush_writer_group_unused_blocks
        );
        if let Some(h) = &self.heat {
            let _ = writeln!(
                out,
                "heat: buckets={} nonzero={} never_scanned={} epoch={} max_count={} bucket_blocks={} refresh_cycles={} lbas_scanned={} bumps={} sweeps={} adaptive_cycles={} reclaim_deferred={} reclaim_confirmed={} scans_skipped={} force_confirm={} defer_suppressed={} yield_milli={} cold_tail_pushed={} cold_tail_dropped={}",
                h.n_buckets,
                h.nonzero_buckets,
                h.never_scanned_buckets,
                h.current_epoch,
                h.max_count,
                h.bucket_size_blocks,
                self.metrics.heat_refresh_cycles,
                self.metrics.heat_refresh_lbas_scanned,
                self.metrics.heat_bumps,
                self.metrics.heat_sweeps_completed,
                self.metrics.heat_refresh_adaptive_cycles,
                self.metrics.gc_heat_deferred_extents,
                self.metrics.gc_heat_confirmed_extents,
                self.metrics.gc_heat_scans_skipped,
                self.metrics.gc_heat_force_confirm_passes,
                self.metrics.gc_heat_defer_suppressed,
                h.confirm_yield_milli,
                self.metrics.gc_heat_cold_tail_pushed,
                self.metrics.gc_heat_cold_tail_dropped
            );
        }
        let _ = writeln!(
            out,
            "discard: ops={} lbas={} blocks_freed={}",
            self.metrics.volume_discard_ops,
            self.metrics.volume_discard_lbas,
            self.metrics.discard_blocks_freed
        );
        out
    }
}
