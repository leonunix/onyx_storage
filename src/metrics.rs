use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Debug)]
pub struct EngineMetrics {
    started_at: Instant,
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
    pub zone_write_split_ops: AtomicU64,
    pub zone_write_lbas: AtomicU64,
    pub zone_read_dispatches: AtomicU64,
    pub buffer_appends: AtomicU64,
    pub buffer_append_bytes: AtomicU64,
    pub buffer_lookup_hits: AtomicU64,
    pub buffer_lookup_misses: AtomicU64,
    pub read_buffer_hits: AtomicU64,
    pub read_lv3_hits: AtomicU64,
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
    pub flush_units_written: AtomicU64,
    pub flush_unit_bytes: AtomicU64,
    pub flush_packed_slots_written: AtomicU64,
    pub flush_packed_fragments_written: AtomicU64,
    pub flush_packed_bytes: AtomicU64,
    pub flush_hole_fills: AtomicU64,
    pub flush_hole_fill_bytes: AtomicU64,
    pub flush_stale_discards: AtomicU64,
    pub flush_errors: AtomicU64,
    pub hole_detections: AtomicU64,
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
}

impl Default for EngineMetrics {
    fn default() -> Self {
        Self {
            started_at: Instant::now(),
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
            zone_write_split_ops: AtomicU64::new(0),
            zone_write_lbas: AtomicU64::new(0),
            zone_read_dispatches: AtomicU64::new(0),
            buffer_appends: AtomicU64::new(0),
            buffer_append_bytes: AtomicU64::new(0),
            buffer_lookup_hits: AtomicU64::new(0),
            buffer_lookup_misses: AtomicU64::new(0),
            read_buffer_hits: AtomicU64::new(0),
            read_lv3_hits: AtomicU64::new(0),
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
            flush_units_written: AtomicU64::new(0),
            flush_unit_bytes: AtomicU64::new(0),
            flush_packed_slots_written: AtomicU64::new(0),
            flush_packed_fragments_written: AtomicU64::new(0),
            flush_packed_bytes: AtomicU64::new(0),
            flush_hole_fills: AtomicU64::new(0),
            flush_hole_fill_bytes: AtomicU64::new(0),
            flush_stale_discards: AtomicU64::new(0),
            flush_errors: AtomicU64::new(0),
            hole_detections: AtomicU64::new(0),
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
            zone_write_split_ops: load(&self.zone_write_split_ops),
            zone_write_lbas: load(&self.zone_write_lbas),
            zone_read_dispatches: load(&self.zone_read_dispatches),
            buffer_appends: load(&self.buffer_appends),
            buffer_append_bytes: load(&self.buffer_append_bytes),
            buffer_lookup_hits: load(&self.buffer_lookup_hits),
            buffer_lookup_misses: load(&self.buffer_lookup_misses),
            read_buffer_hits: load(&self.read_buffer_hits),
            read_lv3_hits: load(&self.read_lv3_hits),
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
            flush_units_written: load(&self.flush_units_written),
            flush_unit_bytes: load(&self.flush_unit_bytes),
            flush_packed_slots_written: load(&self.flush_packed_slots_written),
            flush_packed_fragments_written: load(&self.flush_packed_fragments_written),
            flush_packed_bytes: load(&self.flush_packed_bytes),
            flush_hole_fills: load(&self.flush_hole_fills),
            flush_hole_fill_bytes: load(&self.flush_hole_fill_bytes),
            flush_stale_discards: load(&self.flush_stale_discards),
            flush_errors: load(&self.flush_errors),
            hole_detections: load(&self.hole_detections),
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
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
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
    pub zone_write_split_ops: u64,
    pub zone_write_lbas: u64,
    pub zone_read_dispatches: u64,
    pub buffer_appends: u64,
    pub buffer_append_bytes: u64,
    pub buffer_lookup_hits: u64,
    pub buffer_lookup_misses: u64,
    pub read_buffer_hits: u64,
    pub read_lv3_hits: u64,
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
    pub flush_units_written: u64,
    pub flush_unit_bytes: u64,
    pub flush_packed_slots_written: u64,
    pub flush_packed_fragments_written: u64,
    pub flush_packed_bytes: u64,
    pub flush_hole_fills: u64,
    pub flush_hole_fill_bytes: u64,
    pub flush_stale_discards: u64,
    pub flush_errors: u64,
    pub hole_detections: u64,
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
            zone_write_split_ops,
            zone_write_lbas,
            zone_read_dispatches,
            buffer_appends,
            buffer_append_bytes,
            buffer_lookup_hits,
            buffer_lookup_misses,
            read_buffer_hits,
            read_lv3_hits,
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
            flush_units_written,
            flush_unit_bytes,
            flush_packed_slots_written,
            flush_packed_fragments_written,
            flush_packed_bytes,
            flush_hole_fills,
            flush_hole_fill_bytes,
            flush_stale_discards,
            flush_errors,
            hole_detections,
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
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EngineStatusSnapshot {
    pub full_mode: bool,
    pub volume_count: usize,
    pub live_handle_count: usize,
    pub zone_count: Option<u32>,
    pub buffer_pending_entries: Option<u64>,
    pub buffer_fill_pct: Option<u8>,
    pub allocator_free_blocks: Option<u64>,
    pub allocator_total_blocks: Option<u64>,
    pub metrics: EngineMetricsSnapshot,
}

impl EngineStatusSnapshot {
    pub fn render_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            out,
            "mode: {}",
            if self.full_mode { "full" } else { "meta-only" }
        );
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
        if let (Some(free), Some(total)) = (self.allocator_free_blocks, self.allocator_total_blocks)
        {
            let _ = writeln!(out, "allocator_free_blocks: {}/{}", free, total);
        }
        let _ = writeln!(
            out,
            "volume_ops: create={} delete={} open={} read={} write={}",
            self.metrics.volume_create_ops,
            self.metrics.volume_delete_ops,
            self.metrics.volume_open_ops,
            self.metrics.volume_read_ops,
            self.metrics.volume_write_ops
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
            "buffer: appends={} append_bytes={} lookup_hits={} lookup_misses={}",
            self.metrics.buffer_appends,
            self.metrics.buffer_append_bytes,
            self.metrics.buffer_lookup_hits,
            self.metrics.buffer_lookup_misses
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
            "flush: coalesce_runs={} units={} lbas={} raw_bytes={} compressed_units={} compressed_in={} compressed_out={} written_units={} written_bytes={} packed_slots={} packed_fragments={} packed_bytes={} hole_fills={} hole_fill_bytes={} stale_discards={} errors={} holes_detected={}",
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
            self.metrics.flush_hole_fills,
            self.metrics.flush_hole_fill_bytes,
            self.metrics.flush_stale_discards,
            self.metrics.flush_errors,
            self.metrics.hole_detections
        );
        let _ = writeln!(
            out,
            "dedup: hits={} misses={} skipped_units={} hit_failures={} rescan_cycles={} rescan_skipped_cycles={} rescan_blocks={} rescan_hits={} rescan_misses={} rescan_errors={}",
            self.metrics.dedup_hits,
            self.metrics.dedup_misses,
            self.metrics.dedup_skipped_units,
            self.metrics.dedup_hit_failures,
            self.metrics.dedup_rescan_cycles,
            self.metrics.dedup_rescan_skipped_cycles,
            self.metrics.dedup_rescan_blocks,
            self.metrics.dedup_rescan_hits,
            self.metrics.dedup_rescan_misses,
            self.metrics.dedup_rescan_errors
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
        out
    }
}
