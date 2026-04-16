use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use serde::Serialize;

/// Per-volume IO counters.
#[derive(Debug)]
pub struct VolumeMetrics {
    pub read_ops: AtomicU64,
    pub read_bytes: AtomicU64,
    pub write_ops: AtomicU64,
    pub write_bytes: AtomicU64,
    pub read_errors: AtomicU64,
    pub write_errors: AtomicU64,
}

impl VolumeMetrics {
    pub fn new() -> Self {
        Self {
            read_ops: AtomicU64::new(0),
            read_bytes: AtomicU64::new(0),
            write_ops: AtomicU64::new(0),
            write_bytes: AtomicU64::new(0),
            read_errors: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
        }
    }

    pub fn snapshot(&self) -> VolumeMetricsSnapshot {
        VolumeMetricsSnapshot {
            read_ops: self.read_ops.load(Ordering::Relaxed),
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            write_ops: self.write_ops.load(Ordering::Relaxed),
            write_bytes: self.write_bytes.load(Ordering::Relaxed),
            read_errors: self.read_errors.load(Ordering::Relaxed),
            write_errors: self.write_errors.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct VolumeMetricsSnapshot {
    pub read_ops: u64,
    pub read_bytes: u64,
    pub write_ops: u64,
    pub write_bytes: u64,
    pub read_errors: u64,
    pub write_errors: u64,
}

#[derive(Debug)]
pub struct EngineMetrics {
    started_at: Instant,
    /// Per-volume counters keyed by volume name.
    pub volume_metrics: DashMap<String, Arc<VolumeMetrics>>,
    pub volume_create_ops: AtomicU64,
    pub volume_delete_ops: AtomicU64,
    pub volume_open_ops: AtomicU64,
    pub volume_read_ops: AtomicU64,
    pub volume_read_bytes: AtomicU64,
    pub volume_partial_read_ops: AtomicU64,
    pub volume_write_ops: AtomicU64,
    pub volume_write_bytes: AtomicU64,
    pub volume_partial_write_ops: AtomicU64,
    pub zone_write_dispatches: AtomicU64,
    pub zone_submit_write_ns: AtomicU64,
    pub zone_worker_write_ns: AtomicU64,
    pub zone_write_split_ops: AtomicU64,
    pub zone_write_lbas: AtomicU64,
    pub zone_read_dispatches: AtomicU64,
    pub buffer_appends: AtomicU64,
    pub buffer_append_bytes: AtomicU64,
    pub buffer_write_ops: AtomicU64,
    pub buffer_write_bytes: AtomicU64,
    pub buffer_append_total_ns: AtomicU64,
    pub buffer_append_prepare_ns: AtomicU64,
    pub buffer_append_log_write_ns: AtomicU64,
    pub buffer_append_wait_durable_ns: AtomicU64,
    pub buffer_sync_batches: AtomicU64,
    pub buffer_sync_batch_ns: AtomicU64,
    pub buffer_sync_sleep_ns: AtomicU64,
    pub buffer_sync_epochs_committed: AtomicU64,
    pub buffer_backpressure_events: AtomicU64,
    pub buffer_backpressure_wait_ns: AtomicU64,
    pub buffer_hydration_skipped_due_to_mem_limit: AtomicU64,
    pub buffer_hydration_head_bypass_count: AtomicU64,
    pub buffer_lookup_hits: AtomicU64,
    pub buffer_lookup_misses: AtomicU64,
    pub buffer_read_ops: AtomicU64,
    pub buffer_read_bytes: AtomicU64,
    pub read_buffer_hits: AtomicU64,
    pub read_lv3_hits: AtomicU64,
    pub lv3_read_ops: AtomicU64,
    pub lv3_read_bytes: AtomicU64,
    pub lv3_write_ops: AtomicU64,
    pub lv3_write_bytes: AtomicU64,
    pub read_unmapped: AtomicU64,
    pub read_crc_errors: AtomicU64,
    pub read_decompress_errors: AtomicU64,
    pub coalesce_runs: AtomicU64,
    pub coalesced_units: AtomicU64,
    pub coalesced_lbas: AtomicU64,
    pub coalesced_bytes: AtomicU64,
    pub compress_units: AtomicU64,
    pub compress_input_bytes: AtomicU64,
    pub compress_output_bytes: AtomicU64,
    pub dedup_hits: AtomicU64,
    pub dedup_misses: AtomicU64,
    pub dedup_skipped_units: AtomicU64,
    pub dedup_hit_failures: AtomicU64,
    pub dedup_lookup_ops: AtomicU64,
    pub dedup_lookup_ns: AtomicU64,
    pub dedup_live_check_ops: AtomicU64,
    pub dedup_live_check_ns: AtomicU64,
    pub dedup_stale_index_entries: AtomicU64,
    pub dedup_stale_delete_ns: AtomicU64,
    pub dedup_hit_commit_ops: AtomicU64,
    pub dedup_hit_commit_ns: AtomicU64,
    pub flush_units_written: AtomicU64,
    pub flush_unit_bytes: AtomicU64,
    pub flush_packed_slots_written: AtomicU64,
    pub flush_packed_fragments_written: AtomicU64,
    pub flush_packed_bytes: AtomicU64,
    pub flush_stale_discards: AtomicU64,
    pub flush_errors: AtomicU64,
    pub flush_writer_total_ns: AtomicU64,
    pub flush_writer_alloc_ns: AtomicU64,
    pub flush_writer_io_ns: AtomicU64,
    pub flush_writer_meta_ns: AtomicU64,
    pub flush_writer_cleanup_ns: AtomicU64,
    pub flush_writer_dedup_index_ns: AtomicU64,
    pub flush_writer_mark_flushed_ns: AtomicU64,
    pub flush_writer_precheck_live_pba_ops: AtomicU64,
    pub flush_writer_precheck_live_pba_ns: AtomicU64,
    pub flush_writer_precheck_live_pba_failures: AtomicU64,
    pub gc_cycles: AtomicU64,
    pub gc_paused_cycles: AtomicU64,
    pub gc_candidates_found: AtomicU64,
    pub gc_rewrite_attempts: AtomicU64,
    pub gc_blocks_rewritten: AtomicU64,
    pub gc_errors: AtomicU64,
    pub dedup_rescan_cycles: AtomicU64,
    pub dedup_rescan_skipped_cycles: AtomicU64,
    pub dedup_rescan_blocks: AtomicU64,
    pub dedup_rescan_hits: AtomicU64,
    pub dedup_rescan_misses: AtomicU64,
    pub dedup_rescan_errors: AtomicU64,
    pub volume_discard_ops: AtomicU64,
    pub volume_discard_lbas: AtomicU64,
    pub discard_blocks_freed: AtomicU64,
}

impl EngineMetrics {
    /// Get or create per-volume metrics counters.
    pub fn get_volume_metrics(&self, vol_id: &str) -> Arc<VolumeMetrics> {
        self.volume_metrics
            .entry(vol_id.to_string())
            .or_insert_with(|| Arc::new(VolumeMetrics::new()))
            .clone()
    }

    /// Snapshot all per-volume metrics.
    pub fn volume_metrics_snapshot(&self) -> Vec<(String, VolumeMetricsSnapshot)> {
        self.volume_metrics
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().snapshot()))
            .collect()
    }

    /// Remove per-volume metrics (called on volume delete).
    pub fn remove_volume_metrics(&self, vol_id: &str) {
        self.volume_metrics.remove(vol_id);
    }
}

impl Default for EngineMetrics {
    fn default() -> Self {
        Self {
            started_at: Instant::now(),
            volume_metrics: DashMap::new(),
            volume_create_ops: AtomicU64::new(0),
            volume_delete_ops: AtomicU64::new(0),
            volume_open_ops: AtomicU64::new(0),
            volume_read_ops: AtomicU64::new(0),
            volume_read_bytes: AtomicU64::new(0),
            volume_partial_read_ops: AtomicU64::new(0),
            volume_write_ops: AtomicU64::new(0),
            volume_write_bytes: AtomicU64::new(0),
            volume_partial_write_ops: AtomicU64::new(0),
            zone_write_dispatches: AtomicU64::new(0),
            zone_submit_write_ns: AtomicU64::new(0),
            zone_worker_write_ns: AtomicU64::new(0),
            zone_write_split_ops: AtomicU64::new(0),
            zone_write_lbas: AtomicU64::new(0),
            zone_read_dispatches: AtomicU64::new(0),
            buffer_appends: AtomicU64::new(0),
            buffer_append_bytes: AtomicU64::new(0),
            buffer_write_ops: AtomicU64::new(0),
            buffer_write_bytes: AtomicU64::new(0),
            buffer_append_total_ns: AtomicU64::new(0),
            buffer_append_prepare_ns: AtomicU64::new(0),
            buffer_append_log_write_ns: AtomicU64::new(0),
            buffer_append_wait_durable_ns: AtomicU64::new(0),
            buffer_sync_batches: AtomicU64::new(0),
            buffer_sync_batch_ns: AtomicU64::new(0),
            buffer_sync_sleep_ns: AtomicU64::new(0),
            buffer_sync_epochs_committed: AtomicU64::new(0),
            buffer_backpressure_events: AtomicU64::new(0),
            buffer_backpressure_wait_ns: AtomicU64::new(0),
            buffer_hydration_skipped_due_to_mem_limit: AtomicU64::new(0),
            buffer_hydration_head_bypass_count: AtomicU64::new(0),
            buffer_lookup_hits: AtomicU64::new(0),
            buffer_lookup_misses: AtomicU64::new(0),
            buffer_read_ops: AtomicU64::new(0),
            buffer_read_bytes: AtomicU64::new(0),
            read_buffer_hits: AtomicU64::new(0),
            read_lv3_hits: AtomicU64::new(0),
            lv3_read_ops: AtomicU64::new(0),
            lv3_read_bytes: AtomicU64::new(0),
            lv3_write_ops: AtomicU64::new(0),
            lv3_write_bytes: AtomicU64::new(0),
            read_unmapped: AtomicU64::new(0),
            read_crc_errors: AtomicU64::new(0),
            read_decompress_errors: AtomicU64::new(0),
            coalesce_runs: AtomicU64::new(0),
            coalesced_units: AtomicU64::new(0),
            coalesced_lbas: AtomicU64::new(0),
            coalesced_bytes: AtomicU64::new(0),
            compress_units: AtomicU64::new(0),
            compress_input_bytes: AtomicU64::new(0),
            compress_output_bytes: AtomicU64::new(0),
            dedup_hits: AtomicU64::new(0),
            dedup_misses: AtomicU64::new(0),
            dedup_skipped_units: AtomicU64::new(0),
            dedup_hit_failures: AtomicU64::new(0),
            dedup_lookup_ops: AtomicU64::new(0),
            dedup_lookup_ns: AtomicU64::new(0),
            dedup_live_check_ops: AtomicU64::new(0),
            dedup_live_check_ns: AtomicU64::new(0),
            dedup_stale_index_entries: AtomicU64::new(0),
            dedup_stale_delete_ns: AtomicU64::new(0),
            dedup_hit_commit_ops: AtomicU64::new(0),
            dedup_hit_commit_ns: AtomicU64::new(0),
            flush_units_written: AtomicU64::new(0),
            flush_unit_bytes: AtomicU64::new(0),
            flush_packed_slots_written: AtomicU64::new(0),
            flush_packed_fragments_written: AtomicU64::new(0),
            flush_packed_bytes: AtomicU64::new(0),
            flush_stale_discards: AtomicU64::new(0),
            flush_errors: AtomicU64::new(0),
            flush_writer_total_ns: AtomicU64::new(0),
            flush_writer_alloc_ns: AtomicU64::new(0),
            flush_writer_io_ns: AtomicU64::new(0),
            flush_writer_meta_ns: AtomicU64::new(0),
            flush_writer_cleanup_ns: AtomicU64::new(0),
            flush_writer_dedup_index_ns: AtomicU64::new(0),
            flush_writer_mark_flushed_ns: AtomicU64::new(0),
            flush_writer_precheck_live_pba_ops: AtomicU64::new(0),
            flush_writer_precheck_live_pba_ns: AtomicU64::new(0),
            flush_writer_precheck_live_pba_failures: AtomicU64::new(0),
            gc_cycles: AtomicU64::new(0),
            gc_paused_cycles: AtomicU64::new(0),
            gc_candidates_found: AtomicU64::new(0),
            gc_rewrite_attempts: AtomicU64::new(0),
            gc_blocks_rewritten: AtomicU64::new(0),
            gc_errors: AtomicU64::new(0),
            dedup_rescan_cycles: AtomicU64::new(0),
            dedup_rescan_skipped_cycles: AtomicU64::new(0),
            dedup_rescan_blocks: AtomicU64::new(0),
            dedup_rescan_hits: AtomicU64::new(0),
            dedup_rescan_misses: AtomicU64::new(0),
            dedup_rescan_errors: AtomicU64::new(0),
            volume_discard_ops: AtomicU64::new(0),
            volume_discard_lbas: AtomicU64::new(0),
            discard_blocks_freed: AtomicU64::new(0),
        }
    }
}

impl EngineMetrics {
    pub fn snapshot(&self) -> EngineMetricsSnapshot {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        EngineMetricsSnapshot {
            uptime_secs: self.started_at.elapsed().as_secs(),
            volume_create_ops: load(&self.volume_create_ops),
            volume_delete_ops: load(&self.volume_delete_ops),
            volume_open_ops: load(&self.volume_open_ops),
            volume_read_ops: load(&self.volume_read_ops),
            volume_read_bytes: load(&self.volume_read_bytes),
            volume_partial_read_ops: load(&self.volume_partial_read_ops),
            volume_write_ops: load(&self.volume_write_ops),
            volume_write_bytes: load(&self.volume_write_bytes),
            volume_partial_write_ops: load(&self.volume_partial_write_ops),
            zone_write_dispatches: load(&self.zone_write_dispatches),
            zone_submit_write_ns: load(&self.zone_submit_write_ns),
            zone_worker_write_ns: load(&self.zone_worker_write_ns),
            zone_write_split_ops: load(&self.zone_write_split_ops),
            zone_write_lbas: load(&self.zone_write_lbas),
            zone_read_dispatches: load(&self.zone_read_dispatches),
            buffer_appends: load(&self.buffer_appends),
            buffer_append_bytes: load(&self.buffer_append_bytes),
            buffer_write_ops: load(&self.buffer_write_ops),
            buffer_write_bytes: load(&self.buffer_write_bytes),
            buffer_append_total_ns: load(&self.buffer_append_total_ns),
            buffer_append_prepare_ns: load(&self.buffer_append_prepare_ns),
            buffer_append_log_write_ns: load(&self.buffer_append_log_write_ns),
            buffer_append_wait_durable_ns: load(&self.buffer_append_wait_durable_ns),
            buffer_sync_batches: load(&self.buffer_sync_batches),
            buffer_sync_batch_ns: load(&self.buffer_sync_batch_ns),
            buffer_sync_sleep_ns: load(&self.buffer_sync_sleep_ns),
            buffer_sync_epochs_committed: load(&self.buffer_sync_epochs_committed),
            buffer_backpressure_events: load(&self.buffer_backpressure_events),
            buffer_backpressure_wait_ns: load(&self.buffer_backpressure_wait_ns),
            buffer_hydration_skipped_due_to_mem_limit: load(
                &self.buffer_hydration_skipped_due_to_mem_limit,
            ),
            buffer_hydration_head_bypass_count: load(&self.buffer_hydration_head_bypass_count),
            buffer_lookup_hits: load(&self.buffer_lookup_hits),
            buffer_lookup_misses: load(&self.buffer_lookup_misses),
            buffer_read_ops: load(&self.buffer_read_ops),
            buffer_read_bytes: load(&self.buffer_read_bytes),
            read_buffer_hits: load(&self.read_buffer_hits),
            read_lv3_hits: load(&self.read_lv3_hits),
            lv3_read_ops: load(&self.lv3_read_ops),
            lv3_read_bytes: load(&self.lv3_read_bytes),
            lv3_write_ops: load(&self.lv3_write_ops),
            lv3_write_bytes: load(&self.lv3_write_bytes),
            read_unmapped: load(&self.read_unmapped),
            read_crc_errors: load(&self.read_crc_errors),
            read_decompress_errors: load(&self.read_decompress_errors),
            coalesce_runs: load(&self.coalesce_runs),
            coalesced_units: load(&self.coalesced_units),
            coalesced_lbas: load(&self.coalesced_lbas),
            coalesced_bytes: load(&self.coalesced_bytes),
            compress_units: load(&self.compress_units),
            compress_input_bytes: load(&self.compress_input_bytes),
            compress_output_bytes: load(&self.compress_output_bytes),
            dedup_hits: load(&self.dedup_hits),
            dedup_misses: load(&self.dedup_misses),
            dedup_skipped_units: load(&self.dedup_skipped_units),
            dedup_hit_failures: load(&self.dedup_hit_failures),
            dedup_lookup_ops: load(&self.dedup_lookup_ops),
            dedup_lookup_ns: load(&self.dedup_lookup_ns),
            dedup_live_check_ops: load(&self.dedup_live_check_ops),
            dedup_live_check_ns: load(&self.dedup_live_check_ns),
            dedup_stale_index_entries: load(&self.dedup_stale_index_entries),
            dedup_stale_delete_ns: load(&self.dedup_stale_delete_ns),
            dedup_hit_commit_ops: load(&self.dedup_hit_commit_ops),
            dedup_hit_commit_ns: load(&self.dedup_hit_commit_ns),
            flush_units_written: load(&self.flush_units_written),
            flush_unit_bytes: load(&self.flush_unit_bytes),
            flush_packed_slots_written: load(&self.flush_packed_slots_written),
            flush_packed_fragments_written: load(&self.flush_packed_fragments_written),
            flush_packed_bytes: load(&self.flush_packed_bytes),
            flush_stale_discards: load(&self.flush_stale_discards),
            flush_errors: load(&self.flush_errors),
            flush_writer_total_ns: load(&self.flush_writer_total_ns),
            flush_writer_alloc_ns: load(&self.flush_writer_alloc_ns),
            flush_writer_io_ns: load(&self.flush_writer_io_ns),
            flush_writer_meta_ns: load(&self.flush_writer_meta_ns),
            flush_writer_cleanup_ns: load(&self.flush_writer_cleanup_ns),
            flush_writer_dedup_index_ns: load(&self.flush_writer_dedup_index_ns),
            flush_writer_mark_flushed_ns: load(&self.flush_writer_mark_flushed_ns),
            flush_writer_precheck_live_pba_ops: load(&self.flush_writer_precheck_live_pba_ops),
            flush_writer_precheck_live_pba_ns: load(&self.flush_writer_precheck_live_pba_ns),
            flush_writer_precheck_live_pba_failures: load(
                &self.flush_writer_precheck_live_pba_failures,
            ),
            gc_cycles: load(&self.gc_cycles),
            gc_paused_cycles: load(&self.gc_paused_cycles),
            gc_candidates_found: load(&self.gc_candidates_found),
            gc_rewrite_attempts: load(&self.gc_rewrite_attempts),
            gc_blocks_rewritten: load(&self.gc_blocks_rewritten),
            gc_errors: load(&self.gc_errors),
            dedup_rescan_cycles: load(&self.dedup_rescan_cycles),
            dedup_rescan_skipped_cycles: load(&self.dedup_rescan_skipped_cycles),
            dedup_rescan_blocks: load(&self.dedup_rescan_blocks),
            dedup_rescan_hits: load(&self.dedup_rescan_hits),
            dedup_rescan_misses: load(&self.dedup_rescan_misses),
            dedup_rescan_errors: load(&self.dedup_rescan_errors),
            volume_discard_ops: load(&self.volume_discard_ops),
            volume_discard_lbas: load(&self.volume_discard_lbas),
            discard_blocks_freed: load(&self.discard_blocks_freed),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct EngineMetricsSnapshot {
    pub uptime_secs: u64,
    pub volume_create_ops: u64,
    pub volume_delete_ops: u64,
    pub volume_open_ops: u64,
    pub volume_read_ops: u64,
    pub volume_read_bytes: u64,
    pub volume_partial_read_ops: u64,
    pub volume_write_ops: u64,
    pub volume_write_bytes: u64,
    pub volume_partial_write_ops: u64,
    pub zone_write_dispatches: u64,
    pub zone_submit_write_ns: u64,
    pub zone_worker_write_ns: u64,
    pub zone_write_split_ops: u64,
    pub zone_write_lbas: u64,
    pub zone_read_dispatches: u64,
    pub buffer_appends: u64,
    pub buffer_append_bytes: u64,
    pub buffer_write_ops: u64,
    pub buffer_write_bytes: u64,
    pub buffer_append_total_ns: u64,
    pub buffer_append_prepare_ns: u64,
    pub buffer_append_log_write_ns: u64,
    pub buffer_append_wait_durable_ns: u64,
    pub buffer_sync_batches: u64,
    pub buffer_sync_batch_ns: u64,
    pub buffer_sync_sleep_ns: u64,
    pub buffer_sync_epochs_committed: u64,
    pub buffer_backpressure_events: u64,
    pub buffer_backpressure_wait_ns: u64,
    pub buffer_hydration_skipped_due_to_mem_limit: u64,
    pub buffer_hydration_head_bypass_count: u64,
    pub buffer_lookup_hits: u64,
    pub buffer_lookup_misses: u64,
    pub buffer_read_ops: u64,
    pub buffer_read_bytes: u64,
    pub read_buffer_hits: u64,
    pub read_lv3_hits: u64,
    pub lv3_read_ops: u64,
    pub lv3_read_bytes: u64,
    pub lv3_write_ops: u64,
    pub lv3_write_bytes: u64,
    pub read_unmapped: u64,
    pub read_crc_errors: u64,
    pub read_decompress_errors: u64,
    pub coalesce_runs: u64,
    pub coalesced_units: u64,
    pub coalesced_lbas: u64,
    pub coalesced_bytes: u64,
    pub compress_units: u64,
    pub compress_input_bytes: u64,
    pub compress_output_bytes: u64,
    pub dedup_hits: u64,
    pub dedup_misses: u64,
    pub dedup_skipped_units: u64,
    pub dedup_hit_failures: u64,
    pub dedup_lookup_ops: u64,
    pub dedup_lookup_ns: u64,
    pub dedup_live_check_ops: u64,
    pub dedup_live_check_ns: u64,
    pub dedup_stale_index_entries: u64,
    pub dedup_stale_delete_ns: u64,
    pub dedup_hit_commit_ops: u64,
    pub dedup_hit_commit_ns: u64,
    pub flush_units_written: u64,
    pub flush_unit_bytes: u64,
    pub flush_packed_slots_written: u64,
    pub flush_packed_fragments_written: u64,
    pub flush_packed_bytes: u64,
    pub flush_stale_discards: u64,
    pub flush_errors: u64,
    pub flush_writer_total_ns: u64,
    pub flush_writer_alloc_ns: u64,
    pub flush_writer_io_ns: u64,
    pub flush_writer_meta_ns: u64,
    pub flush_writer_cleanup_ns: u64,
    pub flush_writer_dedup_index_ns: u64,
    pub flush_writer_mark_flushed_ns: u64,
    pub flush_writer_precheck_live_pba_ops: u64,
    pub flush_writer_precheck_live_pba_ns: u64,
    pub flush_writer_precheck_live_pba_failures: u64,
    pub gc_cycles: u64,
    pub gc_paused_cycles: u64,
    pub gc_candidates_found: u64,
    pub gc_rewrite_attempts: u64,
    pub gc_blocks_rewritten: u64,
    pub gc_errors: u64,
    pub dedup_rescan_cycles: u64,
    pub dedup_rescan_skipped_cycles: u64,
    pub dedup_rescan_blocks: u64,
    pub dedup_rescan_hits: u64,
    pub dedup_rescan_misses: u64,
    pub dedup_rescan_errors: u64,
    pub volume_discard_ops: u64,
    pub volume_discard_lbas: u64,
    pub discard_blocks_freed: u64,
}

impl EngineMetricsSnapshot {
    pub fn saturating_sub(&self, earlier: &Self) -> Self {
        macro_rules! build_sub {
            ($($field:ident),+ $(,)?) => {
                Self {
                    $(
                        $field: self.$field.saturating_sub(earlier.$field),
                    )+
                }
            };
        }

        build_sub! {
            uptime_secs,
            volume_create_ops,
            volume_delete_ops,
            volume_open_ops,
            volume_read_ops,
            volume_read_bytes,
            volume_partial_read_ops,
            volume_write_ops,
            volume_write_bytes,
            volume_partial_write_ops,
            zone_write_dispatches,
            zone_submit_write_ns,
            zone_worker_write_ns,
            zone_write_split_ops,
            zone_write_lbas,
            zone_read_dispatches,
            buffer_appends,
            buffer_append_bytes,
            buffer_write_ops,
            buffer_write_bytes,
            buffer_append_total_ns,
            buffer_append_prepare_ns,
            buffer_append_log_write_ns,
            buffer_append_wait_durable_ns,
            buffer_sync_batches,
            buffer_sync_batch_ns,
            buffer_sync_sleep_ns,
            buffer_sync_epochs_committed,
            buffer_backpressure_events,
            buffer_backpressure_wait_ns,
            buffer_hydration_skipped_due_to_mem_limit,
            buffer_hydration_head_bypass_count,
            buffer_lookup_hits,
            buffer_lookup_misses,
            buffer_read_ops,
            buffer_read_bytes,
            read_buffer_hits,
            read_lv3_hits,
            lv3_read_ops,
            lv3_read_bytes,
            lv3_write_ops,
            lv3_write_bytes,
            read_unmapped,
            read_crc_errors,
            read_decompress_errors,
            coalesce_runs,
            coalesced_units,
            coalesced_lbas,
            coalesced_bytes,
            compress_units,
            compress_input_bytes,
            compress_output_bytes,
            dedup_hits,
            dedup_misses,
            dedup_skipped_units,
            dedup_hit_failures,
            dedup_lookup_ops,
            dedup_lookup_ns,
            dedup_live_check_ops,
            dedup_live_check_ns,
            dedup_stale_index_entries,
            dedup_stale_delete_ns,
            dedup_hit_commit_ops,
            dedup_hit_commit_ns,
            flush_units_written,
            flush_unit_bytes,
            flush_packed_slots_written,
            flush_packed_fragments_written,
            flush_packed_bytes,
            flush_stale_discards,
            flush_errors,
            flush_writer_total_ns,
            flush_writer_alloc_ns,
            flush_writer_io_ns,
            flush_writer_meta_ns,
            flush_writer_cleanup_ns,
            flush_writer_dedup_index_ns,
            flush_writer_mark_flushed_ns,
            flush_writer_precheck_live_pba_ops,
            flush_writer_precheck_live_pba_ns,
            flush_writer_precheck_live_pba_failures,
            gc_cycles,
            gc_paused_cycles,
            gc_candidates_found,
            gc_rewrite_attempts,
            gc_blocks_rewritten,
            gc_errors,
            dedup_rescan_cycles,
            dedup_rescan_skipped_cycles,
            dedup_rescan_blocks,
            dedup_rescan_hits,
            dedup_rescan_misses,
            dedup_rescan_errors,
            volume_discard_ops,
            volume_discard_lbas,
            discard_blocks_freed,
        }
    }
}

/// Per-shard buffer ring statistics.
#[derive(Debug, Clone, Default, Serialize)]
pub struct BufferShardSnapshot {
    pub shard_idx: usize,
    pub used_bytes: u64,
    pub capacity_bytes: u64,
    pub fill_pct: u8,
    pub pending_entries: u64,
    pub head_offset: u64,
    pub tail_offset: u64,
    /// Total entries in log_order (both flushed and unflushed).
    pub log_order_len: usize,
    /// Entries flushed but stuck behind the head-of-line blocker.
    pub flushed_seqs_len: usize,
    /// Seq of the front entry in log_order (the head-of-line blocker), if any.
    pub head_seq: Option<u64>,
    /// Remaining unflushed 4KB blocks for the head-of-line seq, if known.
    pub head_remaining_lbas: Option<u32>,
    /// Age of the head-of-line seq in milliseconds, if known.
    pub head_age_ms: Option<u64>,
    /// How long the current seq has continuously remained at the head/front.
    pub head_residency_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RocksDbMemorySnapshot {
    pub block_cache_capacity_bytes: Option<u64>,
    pub block_cache_usage_bytes: Option<u64>,
    pub block_cache_pinned_usage_bytes: Option<u64>,
    pub cur_size_all_mem_tables_bytes: u64,
    pub size_all_mem_tables_bytes: u64,
    pub estimate_table_readers_mem_bytes: u64,
}

impl RocksDbMemorySnapshot {
    pub fn total_estimate_bytes(&self) -> Option<u64> {
        self.block_cache_usage_bytes.map(|block_cache| {
            block_cache
                .saturating_add(self.size_all_mem_tables_bytes)
                .saturating_add(self.estimate_table_readers_mem_bytes)
        })
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct EngineStatusSnapshot {
    /// "active", "standby", or "meta-only"
    pub mode: String,
    pub volume_count: usize,
    pub live_handle_count: usize,
    pub zone_count: Option<u32>,
    pub buffer_pending_entries: Option<u64>,
    pub buffer_fill_pct: Option<u8>,
    pub buffer_payload_memory_bytes: Option<u64>,
    pub buffer_payload_memory_limit_bytes: Option<u64>,
    pub rocksdb_memory: Option<RocksDbMemorySnapshot>,
    pub buffer_shards: Vec<BufferShardSnapshot>,
    pub allocator_free_blocks: Option<u64>,
    pub allocator_total_blocks: Option<u64>,
    pub metrics: EngineMetricsSnapshot,
}

impl EngineStatusSnapshot {
    pub fn render_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "mode: {}", self.mode);
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
        if let Some(payload_bytes) = self.buffer_payload_memory_bytes {
            let limit = self.buffer_payload_memory_limit_bytes.unwrap_or(0);
            let _ = writeln!(
                out,
                "buffer_payload_memory_bytes: {}/{}",
                payload_bytes, limit
            );
        }
        if let Some(rocksdb) = &self.rocksdb_memory {
            let _ = writeln!(
                out,
                "rocksdb_block_cache_bytes: usage={} pinned={} capacity={}",
                rocksdb.block_cache_usage_bytes.unwrap_or(0),
                rocksdb.block_cache_pinned_usage_bytes.unwrap_or(0),
                rocksdb.block_cache_capacity_bytes.unwrap_or(0)
            );
            let _ = writeln!(
                out,
                "rocksdb_meta_bytes: memtables_current={} memtables_total={} table_readers={}",
                rocksdb.cur_size_all_mem_tables_bytes,
                rocksdb.size_all_mem_tables_bytes,
                rocksdb.estimate_table_readers_mem_bytes
            );
            if let Some(total) = rocksdb.total_estimate_bytes() {
                let _ = writeln!(out, "rocksdb_total_estimate_bytes: {}", total);
            }
        }
        if let (Some(free), Some(total)) = (self.allocator_free_blocks, self.allocator_total_blocks)
        {
            let _ = writeln!(out, "allocator_free_blocks: {}/{}", free, total);
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
            "read_path: buffer_hits={} lv3_hits={} unmapped={} crc_errors={} decompress_errors={}",
            self.metrics.read_buffer_hits,
            self.metrics.read_lv3_hits,
            self.metrics.read_unmapped,
            self.metrics.read_crc_errors,
            self.metrics.read_decompress_errors
        );
        let _ = writeln!(
            out,
            "buffer: appends={} append_bytes={} write_ops={} write_bytes={} read_ops={} read_bytes={} lookup_hits={} lookup_misses={} backpressure_events={} hydration_skips={} hydration_head_bypass={}",
            self.metrics.buffer_appends,
            self.metrics.buffer_append_bytes,
            self.metrics.buffer_write_ops,
            self.metrics.buffer_write_bytes,
            self.metrics.buffer_read_ops,
            self.metrics.buffer_read_bytes,
            self.metrics.buffer_lookup_hits,
            self.metrics.buffer_lookup_misses,
            self.metrics.buffer_backpressure_events,
            self.metrics.buffer_hydration_skipped_due_to_mem_limit,
            self.metrics.buffer_hydration_head_bypass_count
        );
        let _ = writeln!(
            out,
            "lv3_io: read_ops={} read_bytes={} write_ops={} write_bytes={}",
            self.metrics.lv3_read_ops,
            self.metrics.lv3_read_bytes,
            self.metrics.lv3_write_ops,
            self.metrics.lv3_write_bytes
        );
        let _ = writeln!(
            out,
            "front_write_ns: zone_submit={} zone_worker={} append_total={} append_prepare={} append_log_write={} append_wait_durable={} append_backpressure_wait={} sync_batches={} sync_batch_ns={} sync_sleep_ns={} sync_epochs={}",
            self.metrics.zone_submit_write_ns,
            self.metrics.zone_worker_write_ns,
            self.metrics.buffer_append_total_ns,
            self.metrics.buffer_append_prepare_ns,
            self.metrics.buffer_append_log_write_ns,
            self.metrics.buffer_append_wait_durable_ns,
            self.metrics.buffer_backpressure_wait_ns,
            self.metrics.buffer_sync_batches,
            self.metrics.buffer_sync_batch_ns,
            self.metrics.buffer_sync_sleep_ns,
            self.metrics.buffer_sync_epochs_committed
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
            "flush: coalesce_runs={} units={} lbas={} raw_bytes={} compressed_units={} compressed_in={} compressed_out={} written_units={} written_bytes={} packed_slots={} packed_fragments={} packed_bytes={} stale_discards={} errors={}",
            self.metrics.coalesce_runs,
            self.metrics.coalesced_units,
            self.metrics.coalesced_lbas,
            self.metrics.coalesced_bytes,
            self.metrics.compress_units,
            self.metrics.compress_input_bytes,
            self.metrics.compress_output_bytes,
            self.metrics.flush_units_written,
            self.metrics.flush_unit_bytes,
            self.metrics.flush_packed_slots_written,
            self.metrics.flush_packed_fragments_written,
            self.metrics.flush_packed_bytes,
            self.metrics.flush_stale_discards,
            self.metrics.flush_errors
        );
        let _ = writeln!(
            out,
            "flush_writer_ns: total={} alloc={} io={} meta={} cleanup={} dedup_index={} mark_flushed={} precheck_live_pba={}",
            self.metrics.flush_writer_total_ns,
            self.metrics.flush_writer_alloc_ns,
            self.metrics.flush_writer_io_ns,
            self.metrics.flush_writer_meta_ns,
            self.metrics.flush_writer_cleanup_ns,
            self.metrics.flush_writer_dedup_index_ns,
            self.metrics.flush_writer_mark_flushed_ns,
            self.metrics.flush_writer_precheck_live_pba_ns
        );
        let _ = writeln!(
            out,
            "flush_writer_precheck: live_pba_ops={} live_pba_failures={}",
            self.metrics.flush_writer_precheck_live_pba_ops,
            self.metrics.flush_writer_precheck_live_pba_failures
        );
        let _ = writeln!(
            out,
            "dedup: hits={} misses={} skipped_units={} hit_failures={} lookups={} live_checks={} stale_entries={} hit_commits={} rescan_cycles={} rescan_skipped_cycles={} rescan_blocks={} rescan_hits={} rescan_misses={} rescan_errors={}",
            self.metrics.dedup_hits,
            self.metrics.dedup_misses,
            self.metrics.dedup_skipped_units,
            self.metrics.dedup_hit_failures,
            self.metrics.dedup_lookup_ops,
            self.metrics.dedup_live_check_ops,
            self.metrics.dedup_stale_index_entries,
            self.metrics.dedup_hit_commit_ops,
            self.metrics.dedup_rescan_cycles,
            self.metrics.dedup_rescan_skipped_cycles,
            self.metrics.dedup_rescan_blocks,
            self.metrics.dedup_rescan_hits,
            self.metrics.dedup_rescan_misses,
            self.metrics.dedup_rescan_errors
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
            "gc: cycles={} paused_cycles={} candidates={} rewrite_attempts={} rewritten_blocks={} errors={}",
            self.metrics.gc_cycles,
            self.metrics.gc_paused_cycles,
            self.metrics.gc_candidates_found,
            self.metrics.gc_rewrite_attempts,
            self.metrics.gc_blocks_rewritten,
            self.metrics.gc_errors
        );
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
