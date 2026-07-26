use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use serde::Serialize;

const LATENCY_BUCKETS: usize = 64;
const FINE_LATENCY_DIRECT_BUCKETS: usize = 16;
const FINE_LATENCY_SUB_BUCKETS: usize = 16;
const FINE_LATENCY_FIRST_EXPONENT: usize = 4;
const FINE_LATENCY_LAST_EXPONENT: usize = 63;
const FINE_LATENCY_BUCKETS: usize = FINE_LATENCY_DIRECT_BUCKETS
    + (FINE_LATENCY_LAST_EXPONENT - FINE_LATENCY_FIRST_EXPONENT + 1) * FINE_LATENCY_SUB_BUCKETS;
const MAX_READ_POOL_WORKERS: usize = 128;

fn latency_bucket(ns: u64) -> usize {
    if ns == 0 {
        0
    } else {
        (u64::BITS as usize - ns.leading_zeros() as usize).min(LATENCY_BUCKETS - 1)
    }
}

fn record_latency_bucket(buckets: &[AtomicU64; LATENCY_BUCKETS], ns: u64) {
    buckets[latency_bucket(ns)].fetch_add(1, Ordering::Relaxed);
}

fn load_latency_buckets(buckets: &[AtomicU64; LATENCY_BUCKETS]) -> Vec<u64> {
    load_atomic_slice(buckets)
}

fn load_atomic_slice(counters: &[AtomicU64]) -> Vec<u64> {
    counters
        .iter()
        .map(|counter| counter.load(Ordering::Relaxed))
        .collect()
}

fn sub_latency_buckets(now: &[u64], earlier: &[u64]) -> Vec<u64> {
    now.iter()
        .enumerate()
        .map(|(idx, value)| value.saturating_sub(earlier.get(idx).copied().unwrap_or(0)))
        .collect()
}

/// A compact log-linear latency histogram. Values below 16 ns have exact
/// integer buckets; every subsequent power-of-two range is split into 16
/// equal-width buckets. A sample therefore costs one relaxed atomic increment
/// while retaining roughly 3.1-6.25% resolution across the full u64 range.
#[derive(Debug)]
pub(crate) struct FineLatencyHistogram {
    buckets: [AtomicU64; FINE_LATENCY_BUCKETS],
}

impl Default for FineLatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl FineLatencyHistogram {
    fn record(&self, ns: u64) {
        self.buckets[fine_latency_bucket(ns)].fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> Vec<u64> {
        load_atomic_slice(&self.buckets)
    }
}

fn histogram_p99_delta(
    histogram: &FineLatencyHistogram,
    previous: &mut Vec<u64>,
) -> Option<(u64, u64)> {
    let current = histogram.snapshot();
    if previous.len() != current.len() {
        *previous = current;
        return None;
    }
    let deltas = sub_latency_buckets(&current, previous);
    *previous = current;
    let total = deltas.iter().copied().sum::<u64>();
    if total == 0 {
        return None;
    }
    let rank = total.saturating_mul(99).div_ceil(100).max(1);
    let bounds = fine_latency_bucket_upper_bounds_ns();
    let mut cumulative = 0u64;
    for (idx, count) in deltas.into_iter().enumerate() {
        cumulative = cumulative.saturating_add(count);
        if cumulative >= rank {
            return Some((bounds[idx], total));
        }
    }
    bounds.last().copied().map(|bound| (bound, total))
}

fn fine_latency_bucket(ns: u64) -> usize {
    if ns < FINE_LATENCY_DIRECT_BUCKETS as u64 {
        return ns as usize;
    }
    let exponent = (u64::BITS - 1 - ns.leading_zeros()) as usize;
    let width_shift = exponent - FINE_LATENCY_FIRST_EXPONENT;
    let base = 1u64 << exponent;
    let sub_bucket = ((ns - base) >> width_shift) as usize;
    FINE_LATENCY_DIRECT_BUCKETS
        + (exponent - FINE_LATENCY_FIRST_EXPONENT) * FINE_LATENCY_SUB_BUCKETS
        + sub_bucket.min(FINE_LATENCY_SUB_BUCKETS - 1)
}

fn fine_latency_bucket_upper_bounds_ns() -> Vec<u64> {
    (0..FINE_LATENCY_BUCKETS)
        .map(|idx| {
            if idx < FINE_LATENCY_DIRECT_BUCKETS {
                return idx as u64;
            }
            let offset = idx - FINE_LATENCY_DIRECT_BUCKETS;
            let exponent = FINE_LATENCY_FIRST_EXPONENT + offset / FINE_LATENCY_SUB_BUCKETS;
            let sub_bucket = offset % FINE_LATENCY_SUB_BUCKETS;
            let base = 1u128 << exponent;
            let width = 1u128 << (exponent - FINE_LATENCY_FIRST_EXPONENT);
            (base + (sub_bucket as u128 + 1) * width - 1) as u64
        })
        .collect()
}

pub(crate) fn record_counter_max(counter: &AtomicU64, value: u64) {
    let mut current = counter.load(Ordering::Relaxed);
    while value > current {
        match counter.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}

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
    pub snapshot_create_ops: AtomicU64,
    pub snapshot_delete_ops: AtomicU64,
    pub snapshot_clone_ops: AtomicU64,
    pub snapshot_restore_ops: AtomicU64,
    pub volume_open_ops: AtomicU64,
    pub volume_read_ops: AtomicU64,
    pub volume_read_bytes: AtomicU64,
    pub volume_read_total_ns: AtomicU64,
    pub ublk_read_queue_wait_ns: AtomicU64,
    pub ublk_read_worker_ns: AtomicU64,
    pub ublk_read_completion_wait_ns: AtomicU64,
    pub volume_partial_read_ops: AtomicU64,
    pub volume_write_ops: AtomicU64,
    pub volume_write_bytes: AtomicU64,
    pub volume_write_total_ns: AtomicU64,
    pub ublk_write_queue_wait_ns: AtomicU64,
    pub ublk_write_worker_ns: AtomicU64,
    pub ublk_write_completion_wait_ns: AtomicU64,
    pub ublk_write_queue_wait_latency_buckets: [AtomicU64; LATENCY_BUCKETS],
    pub ublk_write_worker_latency_buckets: [AtomicU64; LATENCY_BUCKETS],
    pub ublk_write_completion_wait_latency_buckets: [AtomicU64; LATENCY_BUCKETS],
    pub volume_partial_write_ops: AtomicU64,
    /// Foreground read/write requests that have entered the engine but have not
    /// yet completed, failed, or been abandoned by their frontend. One logical
    /// request owns one shared RAII lease even when it spans multiple LV2
    /// tickets, so this remains a request count rather than a shard count.
    pub foreground_io_outstanding: AtomicU64,
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
    pub buffer_append_order_wait_ns: AtomicU64,
    pub buffer_append_order_hold_ns: AtomicU64,
    pub buffer_append_order_wait_max_ns: AtomicU64,
    pub buffer_append_order_hold_max_ns: AtomicU64,
    pub buffer_append_log_write_ns: AtomicU64,
    pub buffer_append_wait_durable_ns: AtomicU64,
    pub buffer_append_prepare_latency_buckets: [AtomicU64; LATENCY_BUCKETS],
    pub buffer_append_order_wait_latency_buckets: [AtomicU64; LATENCY_BUCKETS],
    pub buffer_append_order_hold_latency_buckets: [AtomicU64; LATENCY_BUCKETS],
    pub buffer_append_wait_durable_latency_buckets: [AtomicU64; LATENCY_BUCKETS],
    pub(crate) buffer_append_wait_durable_fine_latency: FineLatencyHistogram,
    /// LV2 durability samples from foreground-tagged tickets only. GC and other
    /// internal appenders deliberately remain in the aggregate histogram above
    /// but cannot dilute the flush QoS feedback signal.
    pub(crate) buffer_append_wait_durable_foreground_fine_latency: FineLatencyHistogram,
    pub buffer_sync_batches: AtomicU64,
    /// Actual backend durability barriers. On a multi-shard chunklet LV2 this
    /// is lower than `buffer_sync_batches` because one root flush covers a
    /// cross-shard group-commit epoch.
    pub buffer_sync_flushes: AtomicU64,
    pub buffer_sync_batch_ns: AtomicU64,
    pub buffer_sync_sleep_ns: AtomicU64,
    pub buffer_sync_epochs_committed: AtomicU64,
    pub buffer_sync_entries: AtomicU64,
    pub buffer_sync_bytes: AtomicU64,
    pub buffer_sync_entries_max: AtomicU64,
    pub buffer_sync_bytes_max: AtomicU64,
    /// Fine-grained stage histograms for the multi-shard global LV2 durability
    /// path. Raw buckets are exported so metrics-json snapshots can be
    /// subtracted over an arbitrary probe interval. All stages share the
    /// `buffer_lv2_latency_bucket_upper_bounds_ns` bounds in the snapshot.
    pub(crate) buffer_lv2_staging_queue_latency: FineLatencyHistogram,
    pub(crate) buffer_lv2_prepared_queue_latency: FineLatencyHistogram,
    pub(crate) buffer_lv2_group_collect_latency: FineLatencyHistogram,
    pub(crate) buffer_lv2_payload_write_latency: FineLatencyHistogram,
    pub(crate) buffer_lv2_checkpoint_write_latency: FineLatencyHistogram,
    pub(crate) buffer_lv2_root_flush_latency: FineLatencyHistogram,
    pub(crate) buffer_lv2_watermark_dispatch_latency: FineLatencyHistogram,
    /// Sampled (1/64 root writes) wall/CPU split for the chunklet LV2 payload
    /// write. `offcpu_ns = wall_ns - cpu_ns` covers scheduler, lock, and device
    /// blocking; it intentionally excludes the group-commit and flush stages.
    pub buffer_lv2_payload_profile_samples: AtomicU64,
    pub buffer_lv2_payload_profile_wall_ns: AtomicU64,
    pub buffer_lv2_payload_profile_cpu_ns: AtomicU64,
    pub buffer_lv2_payload_profile_offcpu_ns: AtomicU64,
    pub buffer_lv2_payload_profile_wall_max_ns: AtomicU64,
    pub buffer_lv2_payload_profile_cpu_max_ns: AtomicU64,
    pub buffer_lv2_payload_profile_offcpu_max_ns: AtomicU64,
    /// Duty-cycle ledger for the three stages of the global LV2 sync pipeline
    /// (`prepare_threads` shard preparers -> `lane_threads` write lanes -> one
    /// durability coordinator). Each stage's wall clock is fully accounted:
    ///
    ///   prepare = idle + build + send_block
    ///   lane    = idle + collect + opsbuild + write + send_block
    ///   coord   = idle + ckpt_encode + ckpt_write + flush + publish
    ///
    /// `busy / (threads * interval)` is the stage's duty cycle; a residue in the
    /// identities above means a segment is still unmeasured. The coordinator is
    /// single-threaded, so its ratio is the one that can cap the pipeline.
    pub buffer_lv2_prepare_threads: AtomicU64,
    pub buffer_lv2_prepare_idle_ns: AtomicU64,
    pub buffer_lv2_prepare_build_ns: AtomicU64,
    pub buffer_lv2_prepare_send_block_ns: AtomicU64,
    pub buffer_lv2_prepare_batches: AtomicU64,
    pub buffer_lv2_lane_threads: AtomicU64,
    pub buffer_lv2_lane_idle_ns: AtomicU64,
    pub buffer_lv2_lane_collect_ns: AtomicU64,
    pub buffer_lv2_lane_opsbuild_ns: AtomicU64,
    pub buffer_lv2_lane_write_ns: AtomicU64,
    pub buffer_lv2_lane_send_block_ns: AtomicU64,
    pub buffer_lv2_lane_epochs: AtomicU64,
    pub buffer_lv2_coord_idle_ns: AtomicU64,
    pub buffer_lv2_coord_ckpt_encode_ns: AtomicU64,
    pub buffer_lv2_coord_ckpt_write_ns: AtomicU64,
    pub buffer_lv2_coord_flush_ns: AtomicU64,
    pub buffer_lv2_coord_publish_ns: AtomicU64,
    pub buffer_lv2_coord_busy_max_ns: AtomicU64,
    pub buffer_lv2_coord_epochs: AtomicU64,
    pub buffer_backpressure_events: AtomicU64,
    pub buffer_backpressure_wait_ns: AtomicU64,
    /// Tier 1.B (ZFS-inspired) hyperbolic LV2 write throttle. `count` =
    /// number of appends that paid a non-zero sleep; `us_total` = aggregate
    /// microseconds slept; `us_max` = largest single sleep observed.
    /// All zero when the throttle is disabled (default).
    pub buffer_throttle_count: AtomicU64,
    pub buffer_throttle_us_total: AtomicU64,
    pub buffer_throttle_us_max: AtomicU64,
    pub buffer_backend_debt_throttle_count: AtomicU64,
    pub buffer_backend_debt_throttle_us_total: AtomicU64,
    pub buffer_backend_debt_throttle_us_max: AtomicU64,
    pub buffer_hydration_skipped_due_to_mem_limit: AtomicU64,
    pub buffer_hydration_head_bypass_count: AtomicU64,
    pub buffer_payload_cache_evict_entries: AtomicU64,
    pub buffer_payload_cache_evict_bytes: AtomicU64,
    pub buffer_lookup_hits: AtomicU64,
    pub buffer_lookup_misses: AtomicU64,
    /// Inner timing for `BufferPool::lookup`. `index_ns` is the time spent
    /// probing the DashMap index across all shards (cheap path that returns
    /// `None` when nothing is pending). `hydrate_ns` / `hydrate_ops` cover
    /// the lazy `read_payload_from_disk` path taken when a hit's payload
    /// isn't resident — this is the suspected ms-level cost source on the
    /// read path. Both counters accumulate per-call across all shards.
    pub buffer_lookup_index_ns: AtomicU64,
    pub buffer_lookup_hydrate_ns: AtomicU64,
    pub buffer_lookup_hydrate_ops: AtomicU64,
    pub buffer_coalesce_hydrate_ns: AtomicU64,
    pub buffer_coalesce_hydrate_ops: AtomicU64,
    pub buffer_coalesce_hydrate_entries: AtomicU64,
    pub buffer_coalesce_hydrate_memory_entries: AtomicU64,
    pub buffer_coalesce_hydrate_disk_entries: AtomicU64,
    pub buffer_read_ops: AtomicU64,
    pub buffer_read_bytes: AtomicU64,
    pub read_buffer_hits: AtomicU64,
    /// Per-LBA counter: number of 4 KB LBAs served out of an LV3 compression
    /// unit. With unit coalescing, one io_uring read can serve many LBAs, so
    /// this can be much larger than `lv3_read_ops`.
    pub read_lv3_hits: AtomicU64,
    /// Per-io_uring-read counter: number of LV3 read operations issued. One
    /// read covers one compression unit and may serve many LBAs
    /// (`read_lv3_hits / lv3_read_ops` ≈ LBAs per unit).
    pub lv3_read_ops: AtomicU64,
    pub lv3_read_compressed_bytes: AtomicU64,
    pub lv3_read_decompressed_bytes: AtomicU64,
    /// ReadPool timing split. `requests` counts caller-submitted LV3 read
    /// work items; `batches` counts worker io_uring submissions; `batch_ops`
    /// is the number of requests folded into those batches. Queue wait is
    /// measured from caller enqueue to worker processing. Submit wait covers
    /// `io_uring submit_batch`, including kernel/device completion wait.
    pub read_pool_requests: AtomicU64,
    pub read_pool_batches: AtomicU64,
    pub read_pool_batch_ops: AtomicU64,
    pub read_pool_queue_wait_ns: AtomicU64,
    pub read_pool_coalesce_wait_ns: AtomicU64,
    pub read_pool_alloc_ns: AtomicU64,
    pub read_pool_submit_wait_ns: AtomicU64,
    pub read_pool_decode_ns: AtomicU64,
    pub read_pool_queue_wait_latency_buckets: [AtomicU64; LATENCY_BUCKETS],
    pub read_pool_submit_wait_latency_buckets: [AtomicU64; LATENCY_BUCKETS],
    pub read_pool_decode_latency_buckets: [AtomicU64; LATENCY_BUCKETS],
    pub read_pool_worker_requests: [AtomicU64; MAX_READ_POOL_WORKERS],
    pub read_pool_worker_batches: [AtomicU64; MAX_READ_POOL_WORKERS],
    pub read_pool_worker_queue_wait_ns: [AtomicU64; MAX_READ_POOL_WORKERS],
    pub read_pool_worker_submit_wait_ns: [AtomicU64; MAX_READ_POOL_WORKERS],
    pub lv3_write_ops: AtomicU64,
    pub lv3_write_compressed_bytes: AtomicU64,
    pub lv3_write_batch_calls: AtomicU64,
    pub lv3_write_batch_ops: AtomicU64,
    pub lv3_write_batch_bytes: AtomicU64,
    pub lv3_write_batch_ns: AtomicU64,
    pub lv3_write_batch_ns_max: AtomicU64,
    pub lv3_write_batch_inflight: AtomicU64,
    pub lv3_write_batch_inflight_max: AtomicU64,
    pub lv3_write_slab_allocs: AtomicU64,
    pub lv3_write_slab_bytes: AtomicU64,
    pub read_unmapped: AtomicU64,
    pub read_crc_errors: AtomicU64,
    pub read_crc_errors_foreground: AtomicU64,
    pub dedup_verify_mismatches: AtomicU64,
    pub read_crc_errors_dedup_scanner: AtomicU64,
    pub read_decompress_errors: AtomicU64,
    /// `ZoneManager::submit_reads` timing breakdown. `*_calls` increments once
    /// per outer `submit_reads` invocation; the three `*_ns` counters split
    /// the wall time across the three phases. Phase counters only accumulate
    /// when that phase actually ran (e.g. `meta_get_ns` is 0 if every LBA hit
    /// the buffer; `unit_io_ns` is 0 if no LBA was mapped).
    pub read_submit_calls: AtomicU64,
    pub read_submit_total_ns: AtomicU64,
    pub read_submit_buffer_lookup_ns: AtomicU64,
    pub read_submit_meta_get_ns: AtomicU64,
    pub read_submit_meta_query_ns: AtomicU64,
    pub read_submit_meta_route_ns: AtomicU64,
    pub read_submit_unit_io_ns: AtomicU64,
    pub read_submit_unit_io_latency_buckets: [AtomicU64; LATENCY_BUCKETS],
    pub coalesce_runs: AtomicU64,
    pub coalesced_units: AtomicU64,
    pub coalesced_lbas: AtomicU64,
    pub coalesced_bytes: AtomicU64,
    /// Entries dropped at coalesce time because every LBA they covered was
    /// superseded by a later pending write. Each such drop avoids one round
    /// of SHA-256 + compress + dedup_index insert/delete + LV3 write.
    pub coalesce_superseded_entries: AtomicU64,
    pub coalesce_superseded_lbas: AtomicU64,
    pub compress_units: AtomicU64,
    pub compress_input_bytes: AtomicU64,
    pub compress_output_bytes: AtomicU64,
    pub compress_bypass_units: AtomicU64,
    pub compress_bypass_bytes: AtomicU64,
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
    /// Number of candidate-cache hits that have been verified
    /// (LV3 byte-compare passed) and successfully promoted into the
    /// persistent dedup_index table in the dedup
    /// worker's atomic commit batch. Counts confirmed-duplicate
    /// promotions only — first-occurrence inserts into the candidate
    /// cache do not bump this counter.
    pub dedup_promotions_committed: AtomicU64,
    /// Promotions that were lost to a failed atomic commit (entire
    /// chunk rolled back). The candidate cache slot stays around so
    /// the next sighting will retry; counter helps detect persistent
    /// commit-failure stuck states.
    pub dedup_promotions_failed: AtomicU64,
    /// `DedupPut`s dropped because another lane was already promoting the
    /// same content hash (the in-flight promote gate fired). The hit's
    /// rc-neutral L2pRemap still applies; only the duplicate registration
    /// is skipped. A high rate means concurrent same-content writes are
    /// common (e.g. dedupe-heavy benchmarks) — a small dedup-ratio dip,
    /// not a correctness issue. Prevents the concurrent double-decref rc
    /// underflow.
    pub dedup_promote_skipped_inflight: AtomicU64,
    /// Old-mapping cleanup failed to reconstruct a freed block from LV3, so
    /// the corresponding forward dedup_index entry was left in place.
    pub dedup_cleanup_reconstruct_errors: AtomicU64,
    /// Old-mapping cleanup reconstructed the block but failed to delete the
    /// matching forward dedup_index entry.
    pub dedup_cleanup_delete_errors: AtomicU64,
    pub flush_units_written: AtomicU64,
    pub flush_unit_bytes: AtomicU64,
    pub flush_packed_slots_written: AtomicU64,
    pub flush_packed_fragments_written: AtomicU64,
    pub flush_packed_bytes: AtomicU64,
    pub flush_stale_discards: AtomicU64,
    /// Per-LBA L2P remaps rejected by metadb's apply-time seq_guard
    /// (our seq <= stored seq). Expected to stay near zero while the
    /// onyx-side commit lock is still in place; ramps up after the
    /// lock is removed and concurrent same-LBA commits race.
    pub flush_seq_rejects: AtomicU64,
    pub flush_errors: AtomicU64,
    /// Global coalescer admission QoS. Mode: 0 idle/disabled, 1 protected,
    /// 2 occupancy recovery, 3 physical-ring emergency.
    pub flush_qos_mode: AtomicU64,
    pub flush_qos_rate_bytes_per_sec: AtomicU64,
    pub flush_qos_foreground_bytes_per_sec: AtomicU64,
    pub flush_qos_durable_p99_ns: AtomicU64,
    pub flush_qos_logical_fill_pct: AtomicU64,
    pub flush_qos_physical_fill_pct: AtomicU64,
    pub flush_qos_payload_fill_pct: AtomicU64,
    pub flush_qos_admitted_bytes: AtomicU64,
    pub flush_qos_wait_ns: AtomicU64,
    pub flush_qos_wait_events: AtomicU64,
    /// Longest end-to-end wait of one admission request since process start.
    pub flush_qos_wait_max_ns: AtomicU64,
    /// Requests currently queued in the device-wide FIFO and its lifetime HWM.
    pub flush_qos_waiters: AtomicU64,
    pub flush_qos_waiters_max: AtomicU64,
    pub flush_qos_idle_bypasses: AtomicU64,
    pub flush_qos_emergency_bypasses: AtomicU64,
    pub flush_qos_emergency_transitions: AtomicU64,
    pub flush_qos_rate_increases: AtomicU64,
    pub flush_qos_rate_decreases: AtomicU64,
    pub flush_writer_total_ns: AtomicU64,
    pub flush_writer_alloc_ns: AtomicU64,
    pub flush_writer_io_ns: AtomicU64,
    /// Split of the writer's `io` window: buffer allocation, the blanket
    /// zero-fill, payload assembly, and the actual submit. Measurement showed
    /// only 8.4 % of that window is real LV3 write time.
    pub flush_writer_bufalloc_ns: AtomicU64,
    pub flush_writer_bufzero_ns: AtomicU64,
    pub flush_writer_assemble_ns: AtomicU64,
    pub flush_writer_submit_ns: AtomicU64,
    /// Split of `flush_writer_submit_ns`, which measured as 92 % of the io
    /// window while `lv3_io.write_batch_ns` -- the real device write inside it
    /// -- was only 26 % of that window. `ops` is op-vector assembly (including
    /// `allocator.wait_for_readers`), `io` the batch submit call, `rollback`
    /// the per-op failure unwind, and `padding` the allocator round trip that
    /// returns never-mapped full-stripe padding.
    pub flush_writer_submit_ops_ns: AtomicU64,
    pub flush_writer_submit_io_ns: AtomicU64,
    pub flush_writer_submit_rollback_ns: AtomicU64,
    pub flush_writer_submit_padding_ns: AtomicU64,
    pub flush_writer_meta_ns: AtomicU64,
    pub flush_writer_meta_build_ns: AtomicU64,
    pub flush_writer_meta_commit_ns: AtomicU64,
    pub flush_writer_meta_candidate_ns: AtomicU64,
    pub flush_writer_meta_repair_ns: AtomicU64,
    pub flush_writer_meta_commits: AtomicU64,
    pub flush_writer_meta_lbas: AtomicU64,
    pub flush_writer_meta_pt_commits: AtomicU64,
    pub flush_writer_meta_pt_lbas: AtomicU64,
    pub flush_writer_meta_packed_commits: AtomicU64,
    pub flush_writer_meta_packed_lbas: AtomicU64,
    pub flush_writer_cycles: AtomicU64,
    pub flush_writer_cycles_full: AtomicU64,
    pub flush_writer_cycles_partial: AtomicU64,
    pub flush_writer_read_active_cycles: AtomicU64,
    pub flush_writer_drained_units: AtomicU64,
    pub flush_writer_drained_units_max: AtomicU64,
    /// `write_rx.len()` sampled at the start of each writer cycle, *before*
    /// the drain loop pulls anything. High values mean compress/dedup is
    /// producing faster than the writer commits — i.e. the bottleneck is
    /// downstream of the channel (metadb commit / LV3 IO), not upstream.
    pub flush_writer_rx_pending_max: AtomicU64,
    pub flush_writer_commit_send_ns: AtomicU64,
    pub flush_writer_commit_send_ops: AtomicU64,
    pub flush_writer_commit_send_len_max: AtomicU64,
    /// Total residence from raw job enqueue through executor dequeue.
    pub flush_commit_worker_queue_wait_ns: AtomicU64,
    /// Portion before a batch is sealed. This includes producer send
    /// backpressure, raw-queue residence, and intentional coalescing.
    pub flush_commit_worker_aggregator_residence_ns: AtomicU64,
    /// Portion after a batch is sealed and before an executor dequeues it.
    pub flush_commit_worker_executor_queue_wait_ns: AtomicU64,
    /// Maximum executor-queue residence of one sealed batch.
    pub flush_commit_worker_executor_queue_wait_max_ns: AtomicU64,
    /// Shared downstream batch backlog and high-water mark. The current depth
    /// conservatively includes the sole aggregator's blocked handoff when the
    /// bounded channel is full.
    pub flush_commit_executor_queue_depth: AtomicU64,
    pub flush_commit_executor_queue_depth_max: AtomicU64,
    /// Configured executor count. This is a live capacity gauge, not a
    /// lifetime high-water proxy.
    pub flush_commit_executors_limit: AtomicU64,
    /// Executors currently dispatching a commit batch and their high-water
    /// mark. These gauges drive adaptive-seal admission as well as observability.
    pub flush_commit_executors_active: AtomicU64,
    pub flush_commit_executors_active_max: AtomicU64,
    /// Aggregator seal reasons. `capacity` means the next indivisible job would
    /// cross the target; it is neither a full-target recovery nor an underfill
    /// sample. `deadline` is the hard oldest-job residence ceiling;
    /// `adaptive_underfill` is the half-window useful-fill path. `pressure` is
    /// the LV2 physical-fill escape hatch, while `shutdown` proves the final
    /// partial batch was forwarded during orderly drain.
    pub flush_commit_aggregator_seals_target: AtomicU64,
    pub flush_commit_aggregator_seals_capacity: AtomicU64,
    pub flush_commit_aggregator_seals_deadline: AtomicU64,
    pub flush_commit_aggregator_seals_adaptive_underfill: AtomicU64,
    pub flush_commit_aggregator_seals_pressure: AtomicU64,
    pub flush_commit_aggregator_seals_shutdown: AtomicU64,
    /// Number of timed service invocations represented by
    /// `flush_commit_worker_service_ns`. Unlike raw jobs, this has the same
    /// accounting boundary as the service timer and is its correct divisor.
    pub flush_commit_worker_service_batches: AtomicU64,
    pub flush_commit_worker_service_ns: AtomicU64,
    pub flush_commit_worker_jobs: AtomicU64,
    pub flush_commit_worker_job_lbas: AtomicU64,
    pub flush_commit_worker_drain_batches: AtomicU64,
    pub flush_commit_worker_drain_jobs: AtomicU64,
    pub flush_commit_worker_drain_lbas: AtomicU64,
    pub flush_commit_worker_drain_jobs_max: AtomicU64,
    pub flush_commit_worker_drain_lbas_max: AtomicU64,
    /// Deferred commit pipeline observability. `pipeline_issues`
    /// counts passthrough chunks fed into the per-volume deferred-outcome
    /// deque (`flush_or_queue_passthrough_chunk` calls that produced a
    /// `PendingPassthroughChunk`). `pipeline_depth_max` is the high-water
    /// mark of `pending_q.len()` observed across all volumes — when
    /// `commit_worker_deferred_outcomes=false` this stays at 1; with the
    /// flag on it approaches `commit_worker_pipeline_depth`.
    /// `pipeline_block_drains` counts how often the front of the deque
    /// had to be drained inline because the deque was at `depth_cap`;
    /// `pipeline_block_drain_ns` accumulates that wall time. Both
    /// counters are 0 when nothing is in flight (legacy sync pacing).
    pub flush_commit_worker_pipeline_issues: AtomicU64,
    pub flush_commit_worker_pipeline_depth_max: AtomicU64,
    pub flush_commit_worker_pipeline_block_drains: AtomicU64,
    pub flush_commit_worker_pipeline_block_drain_ns: AtomicU64,
    /// Opportunistic forward-drain hits: count of pipeline chunks
    /// whose metadb deferred-outcome handles were already ready when
    /// `flush_or_queue_passthrough_chunk` probed the deque front
    /// before issuing the next chunk. Each hit avoids a future
    /// `pipeline_block_drains` event when the next issue arrives.
    pub flush_commit_worker_pipeline_opportunistic_drains: AtomicU64,
    pub flush_stage_coalesce_send_ns: AtomicU64,
    pub flush_stage_coalesce_send_ops: AtomicU64,
    pub flush_stage_coalesce_send_len_sum: AtomicU64,
    pub flush_stage_coalesce_send_len_max: AtomicU64,
    pub flush_stage_dedup_send_ns: AtomicU64,
    pub flush_stage_dedup_send_ops: AtomicU64,
    pub flush_stage_dedup_send_len_sum: AtomicU64,
    pub flush_stage_dedup_send_len_max: AtomicU64,
    pub flush_stage_compress_send_ns: AtomicU64,
    pub flush_stage_compress_send_ops: AtomicU64,
    pub flush_stage_compress_send_len_sum: AtomicU64,
    pub flush_stage_compress_send_len_max: AtomicU64,
    /// Upstream-pipeline stage timing. Each `_active_ns` counter
    /// accumulates time the worker spent processing work; the
    /// matching `_idle_ns` counter accumulates time it was blocked on
    /// `recv_timeout` with no input. `active / (active + idle)` is
    /// the worker's utilization — values near 100% mean that stage
    /// is the upstream bottleneck feeding commit_worker.
    pub flush_coalesce_active_ns: AtomicU64,
    pub flush_coalesce_idle_ns: AtomicU64,
    /// Sub-breakdown of coalesce CPU. `flush_coalesce_active_ns` is
    /// the gross "time the worker thread wasn't blocked on recv" and
    /// thus mixes pure CPU with `tx.send(unit)` blocking + hydrate.
    /// These four counters isolate the inside-`coalesce_pending` cost
    /// so a `coalesce 97% busy` signal can be split into "stuck on
    /// channel send" vs "actually burning CPU in coalesce_slices".
    /// Sum of the dedup/sort/merge timers will be smaller than `pending_ns`
    /// because LbaSlice build time and per-call overhead are not attributed
    /// to any of those timers.
    pub flush_coalesce_pending_ns: AtomicU64,
    pub flush_coalesce_pending_ops: AtomicU64,
    pub flush_coalesce_phase2_dedup_ns: AtomicU64,
    pub flush_coalesce_phase3_sort_ns: AtomicU64,
    pub flush_coalesce_phase4_merge_ns: AtomicU64,
    pub flush_dedup_worker_active_ns: AtomicU64,
    pub flush_dedup_worker_idle_ns: AtomicU64,
    pub flush_dedup_worker_iters: AtomicU64,
    pub flush_compress_worker_active_ns: AtomicU64,
    pub flush_compress_worker_idle_ns: AtomicU64,
    /// Sub-breakdown of `flush_compress_worker_active_ns`. Helps split
    /// "is compress slow because of LZ4 itself" vs "is it slow because
    /// of alloc/memcpy ceremony around LZ4". `raw_build_ns` covers the
    /// `Vec::with_capacity + extend_from_slice` loop that materialises
    /// the unit's raw bytes; `codec_ns` covers the inner compressor
    /// call (`lz4_flex::compress` etc.); `crc_ns` covers the final
    /// crc32fast over the compressed (or bypass) buffer. Sum is less
    /// than `active_ns` because the CoalesceUnit destructure, the
    /// Vec drops, and the channel send are not attributed.
    pub flush_compress_raw_build_ns: AtomicU64,
    pub flush_compress_codec_ns: AtomicU64,
    pub flush_compress_crc_ns: AtomicU64,
    pub flush_commit_worker_rx_idle_ns: AtomicU64,
    pub flush_commit_worker_rx_idle_iters: AtomicU64,
    pub flush_writer_pt_batches: AtomicU64,
    pub flush_writer_pt_units: AtomicU64,
    pub flush_writer_pt_lbas: AtomicU64,
    pub flush_writer_pt_io_ops: AtomicU64,
    pub flush_writer_pt_mark_calls: AtomicU64,
    pub flush_writer_pt_mark_lbas: AtomicU64,
    pub flush_writer_pt_units_max: AtomicU64,
    pub flush_writer_pt_lbas_max: AtomicU64,
    pub flush_writer_pt_io_ops_max: AtomicU64,
    pub flush_writer_pt_mark_calls_max: AtomicU64,
    pub flush_writer_packed_batches: AtomicU64,
    pub flush_writer_packed_batch_slots: AtomicU64,
    pub flush_writer_packed_batch_lbas: AtomicU64,
    pub flush_writer_packed_batch_io_ops: AtomicU64,
    pub flush_writer_packed_mark_calls: AtomicU64,
    pub flush_writer_packed_mark_lbas: AtomicU64,
    pub flush_writer_packed_batch_slots_max: AtomicU64,
    pub flush_writer_packed_batch_lbas_max: AtomicU64,
    pub flush_writer_packed_batch_io_ops_max: AtomicU64,
    pub flush_writer_packed_mark_calls_max: AtomicU64,
    pub flush_writer_cleanup_ns: AtomicU64,
    /// Async cleanup-thread time: dedup-index repair + extra PBA
    /// freelisting performed off the writer hot path. Sibling to
    /// `flush_writer_cleanup_ns` (which is strictly the writer-inline
    /// post-commit work). Together they expose where the per-unit
    /// "cleanup" cost actually lives.
    pub flush_cleanup_thread_ns: AtomicU64,
    /// Number of cleanup-thread batches processed (each may cover
    /// many units). Denominator for per-batch averages —
    /// `flush_units_written` is the wrong denominator for the async
    /// counter because batches coalesce across units.
    pub flush_cleanup_thread_batches: AtomicU64,
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
    pub gc_retired_blocks_reclaimed: AtomicU64,
    /// Resident-compactor debt gauge: estimated compactable dead blocks across
    /// the volume set, published once per full compactor sweep (sum of
    /// `unit_lba_count - live_count` over multi-LBA units). Rising = packing
    /// slack accumulating faster than the compactor reclaims it; flat/falling =
    /// the resident compactor is keeping debt bounded.
    pub gc_compactable_dead_blocks: AtomicU64,
    /// Slot-aware compaction (`compactor_slot_evac_enabled`). `candidates` =
    /// fragments promoted because their whole packed slot is mostly dead by
    /// bytes (so the slot can be evacuated and freed); `blocks` = live blocks
    /// selected for that evacuation (the rewrite cost — pair with
    /// `gc_retired_blocks_reclaimed` to watch efficiency; rewritten ≫ reclaimed
    /// means the byte-deadness heuristic is churning); `incomplete_skips` =
    /// slots that passed the byte-deadness gate but whose window did NOT see all
    /// of the slot's live references (`rc != visible_live`), so evacuating now
    /// would be wasted — a large value sizes the sweep accumulator.
    /// `cost_cap_skips` is the COMPLEMENT: byte-dead slots seen completely
    /// (`rc == visible_live`) but pinned by more than `compactor_slot_evac_max_live`
    /// live blocks — too expensive to relocate; the sweep accumulator does
    /// not help these.
    pub gc_slot_evac_candidates: AtomicU64,
    pub gc_slot_evac_blocks: AtomicU64,
    pub gc_slot_evac_incomplete_skips: AtomicU64,
    pub gc_slot_evac_cost_cap_skips: AtomicU64,
    /// Defrag (physical-neighborhood compaction; `gc.defrag_*` knobs).
    /// Gauges: `mode_active` (trigger latched), `targets_active` /
    /// `target_blocks` (published target ranges and their span). Counters:
    /// `walk_extents` (free extents visited by the descending selection walk),
    /// `clusters_qualified/rejected` (selection outcomes), `candidates` /
    /// `blocks_selected` (scanner-side promotions inside target ranges), and
    /// `blocks_moved` (legacy name: blocks durably re-appended to LV2, not proof
    /// of a physical remap). `segments_completed` is the authoritative physical
    /// outcome: it advances only after every PBA in a quarantine is reclaimed.
    pub gc_defrag_mode_active: AtomicU64,
    pub gc_defrag_targets_active: AtomicU64,
    pub gc_defrag_target_blocks: AtomicU64,
    pub gc_defrag_walk_extents: AtomicU64,
    pub gc_defrag_clusters_qualified: AtomicU64,
    pub gc_defrag_clusters_rejected: AtomicU64,
    pub gc_defrag_candidates: AtomicU64,
    pub gc_defrag_blocks_selected: AtomicU64,
    pub gc_defrag_blocks_moved: AtomicU64,
    /// Full stripe quarantine targets that reached 100% physically free and
    /// were atomically published into the allocator reserve.
    pub gc_defrag_segments_completed: AtomicU64,
    /// Quarantine targets released without completion (mode exit or no-progress
    /// watchdog). Cancellation is safe and loses only cleaner progress.
    pub gc_defrag_segments_cancelled: AtomicU64,
    /// Dedup hits deliberately converted to fresh writes because the candidate
    /// was the relocation source or overlapped an active quarantine target.
    pub gc_defrag_dedup_hits_rejected: AtomicU64,
    /// Allocator free-pool contiguity gauges (published once per GC cycle from
    /// `contiguity_stats`). `free_blocks_in_set` excludes lane caches, so
    /// `stripe_capable_blocks / free_blocks_in_set` = the defrag trigger
    /// fraction; `free_extents` counts fragments (confetti belt size);
    /// `largest_free_run` is the biggest contiguous run in blocks.
    pub allocator_free_extents: AtomicU64,
    pub allocator_largest_free_run: AtomicU64,
    pub allocator_stripe_capable_blocks: AtomicU64,
    pub allocator_free_blocks_in_set: AtomicU64,
    pub allocator_stripe_reserve_blocks: AtomicU64,
    pub allocator_quarantine_target_blocks: AtomicU64,
    pub allocator_quarantine_free_blocks: AtomicU64,
    /// Flush-writer batches that hit stripe starvation (aligned allocation
    /// failed → the whole remaining batch degraded to per-unit unaligned
    /// writes → RAID partial-stripe RMW downstream). Rate ≈ share of flush
    /// batches running degraded — the direct fragmentation-pathology signal.
    pub flush_writer_stripe_starved_batches: AtomicU64,
    pub flush_writer_group_aligned_ops: AtomicU64,
    pub flush_writer_group_unaligned_ops: AtomicU64,
    pub flush_writer_group_short_extent_allocs: AtomicU64,
    pub flush_writer_group_fallback_units: AtomicU64,
    pub flush_writer_group_unused_blocks: AtomicU64,
    pub gc_errors: AtomicU64,
    /// Number of batched all-volume L2P scans the retired-extent reclaim path
    /// has run. With batching this is ~1 per GC cycle (was up to
    /// `MAX_RETIRED_RECLAIM_PER_CYCLE` per cycle, one full scan per extent).
    pub gc_reclaim_blockmap_scans: AtomicU64,
    /// Tripwire: retired extents that passed the cheap Gate-1 rc==0 filter
    /// (a plain, fold-racy `multi_get_refcounts`) but were caught still
    /// referenced by the post-barrier FOLD-CONSISTENT rc recheck under
    /// `rc_authoritative_reclaim`. Each one is a premature free (→ reuse →
    /// read CRC) averted: the Gate-1 read had transiently floored a live
    /// rc to a spurious 0 across a refcount fold's publish-before-clear
    /// window. Should be small but NONZERO under dedup-heavy load (it firing
    /// is proof the consistent recheck is load-bearing); a sustained 0 across
    /// a known-racy workload means the recheck isn't engaging.
    pub gc_reclaim_premature_free_averted: AtomicU64,
    /// [[no-refcount-hot-path-design]] Rc-neutral path: count of PBAs that
    /// flowed back to the allocator via Lineage GC's `WalOp::FreePbas`
    /// surface, drained by `LineageFreedPbaDrainHandle`. Counts blocks
    /// (not PBAs) so it stays comparable with `gc_blocks_rewritten`.
    pub gc_lineage_freed_blocks: AtomicU64,
    /// Lineage GC `FreePbas` extents that were already free/retired when the
    /// drain tried to free them (duplicate surface absorbed idempotently —
    /// counter + debug, not a warn). A persistently rising value means metadb
    /// is re-surfacing the same dead-list segment; see
    /// `space::pba_lifecycle::PbaLifecycle::free_lineage_gc_proven`.
    pub gc_lineage_idempotent_frees: AtomicU64,
    /// Current depth of the committed-PBA retire retry queue. A committed dead
    /// PBA whose `allocator.retire_*` failed after its metadata commit is
    /// deferred here and re-driven with backoff; a non-zero steady value means
    /// space is stuck un-reclaimable. See `space::pba_lifecycle`.
    pub pba_reclaim_stuck: AtomicU64,
    /// Cumulative blocks newly moved into the allocator's retired set via the
    /// committed-PBA retire path (`PbaLifecycle::retire_committed`) — the INPUT
    /// to the GC reclaim Gate. Pair with `gc_retired_blocks_reclaimed` (the
    /// OUTPUT): if `pba_blocks_retired − gc_retired_blocks_reclaimed` grows
    /// without bound the reclaim Gate is the bottleneck; if input ≈ output yet
    /// the device keeps filling, dead PBAs are never reaching retire at all
    /// (e.g. packed/shared slots staying rc>0 behind a live sibling fragment).
    /// Excludes the lineage direct-free path (see `gc_lineage_freed_blocks`).
    pub pba_blocks_retired: AtomicU64,
    /// Reclaim-stage diagnostics (localize why `gc_retired_blocks_reclaimed`
    /// lags `pba_blocks_retired`). All per-cycle block counts, re-counted each
    /// cycle for still-stuck extents, so the RATIO between them is the signal:
    /// - `gc_reclaim_grace_deferred`: rc==0 blocks held back by the reclaim-age
    ///   grace (not yet aged). Dominant ⇒ the grace (re-aging when retired
    ///   extents coalesce) is the bottleneck.
    /// - `gc_reclaim_rc_rejected`: grace-aged blocks rejected by Gate-1 because a
    ///   PBA is still referenced (rc>0). Dominant ⇒ rc>0 zombies (e.g. dedup
    ///   re-reference of a retired PBA) wedge the retired set.
    /// - `gc_retired_blocks_depth`: gauge of the current full retired-set block
    ///   count (the live backlog; rising = reclaim losing to retire).
    pub gc_reclaim_grace_deferred: AtomicU64,
    pub gc_reclaim_rc_rejected: AtomicU64,
    pub gc_retired_blocks_depth: AtomicU64,
    /// Adaptive reclaim heat map (observe-only, Stage A). `heat_refresh_cycles`
    /// = GC cycles that ran a heat-refresh step; `heat_refresh_lbas_scanned` =
    /// live blockmap entries walked (≈ budget × cycles, the "no silent
    /// truncation" signal); `heat_bumps` = per-PBA region increments;
    /// `heat_sweeps_completed` = full sweeps over the volume set (epoch ticks).
    pub heat_refresh_cycles: AtomicU64,
    pub heat_refresh_lbas_scanned: AtomicU64,
    pub heat_bumps: AtomicU64,
    pub heat_sweeps_completed: AtomicU64,
    /// Stage-B2: cycles where the adaptive (per-volume churn-weighted) refresh
    /// budget split was applied (only when `heat_adaptive_refresh_enabled` and
    /// >1 volume). 0 means the refresh ran uniform/round-robin.
    pub heat_refresh_adaptive_cycles: AtomicU64,
    /// Stage-B reclaim consumption of the heat map (only move when
    /// `heat_reclaim_enabled`). `gc_heat_deferred_extents` = retired rc==0
    /// extents whose hot+fresh region let reclaim skip the confirm scan this
    /// cycle (stay retired, re-presented later); `gc_heat_confirmed_extents` =
    /// extents sent to the confirm scan; `gc_heat_scans_skipped` = cycles where
    /// every survivor was deferred so the whole all-volume scan was skipped
    /// (the headline win); `gc_heat_force_confirm_passes` = periodic
    /// belt-and-suspenders cycles that confirmed all survivors regardless of
    /// heat.
    pub gc_heat_deferred_extents: AtomicU64,
    pub gc_heat_confirmed_extents: AtomicU64,
    pub gc_heat_scans_skipped: AtomicU64,
    pub gc_heat_force_confirm_passes: AtomicU64,
    /// Cycles where the yield gate SUPPRESSED deferral (confirm-all) because a
    /// recent confirm scan was productive, free space was tight, or it was a
    /// periodic recalibration — deferring was judged not worth it. Excludes the
    /// anti-starvation force-confirm pass (`gc_heat_force_confirm_passes`).
    pub gc_heat_defer_suppressed: AtomicU64,
    /// Stage-4 fold: cold candidates the heat-refresh walk emitted into the
    /// cold-tail channel for the dedup scanner to warm (`gc_heat_cold_tail_pushed`),
    /// and ones it had to drop because the channel was full or the consumer had
    /// gone away (`gc_heat_cold_tail_dropped`). The dropped count is the
    /// "no silent truncation" signal for the fold.
    pub gc_heat_cold_tail_pushed: AtomicU64,
    pub gc_heat_cold_tail_dropped: AtomicU64,
    pub dedup_rescan_cycles: AtomicU64,
    pub dedup_rescan_skipped_cycles: AtomicU64,
    pub dedup_rescan_blocks: AtomicU64,
    pub dedup_rescan_hits: AtomicU64,
    pub dedup_rescan_misses: AtomicU64,
    pub dedup_rescan_errors: AtomicU64,
    /// Cold-tail rescan: live blockmap entries the scanner walked, hashed,
    /// and inserted into the candidate cache so a future duplicate write
    /// can verify-and-promote against an already-warmed fingerprint.
    pub dedup_cold_tail_blocks: AtomicU64,
    /// Cold-tail entries skipped because the scanner already had a hash
    /// recorded for that PBA (warmed by a prior cycle or the writer).
    pub dedup_cold_tail_already_warm: AtomicU64,
    /// Cold-tail read/decode failures (CRC mismatch, decompress error,
    /// LV3 IO error). The scanner moves on; the cursor still advances so
    /// one bad block does not stall progress.
    pub dedup_cold_tail_errors: AtomicU64,
    /// Cold-tail rescan: live blockmap entries whose content hash was
    /// already a live dedup_index entry pointing at a different PBA, so
    /// the scanner remapped the LBA onto the existing dedup target and
    /// decref'd the now-orphaned old PBA. These are "evicted-window"
    /// duplicates the candidate cache lost before the 2nd write arrived;
    /// reclaiming them is the cold-tail pass's backend safety-net role.
    pub dedup_cold_tail_remaps: AtomicU64,
    /// Stage-4 fold: cold candidates the dedup scanner drained from the fold
    /// channel and fed to the ReadPool this cycle (the consumer-side analog of
    /// `gc_heat_cold_tail_pushed`). Zero when the fold is off (legacy scan path).
    pub dedup_cold_tail_drained: AtomicU64,
    /// §6 orphan dedup-PBA reclaim: cold-region dedup entries deleted from the
    /// index (`demoted`); of those, PBAs that dropped to rc==0 and were retired
    /// for the GC confirm scan to free (`retired`); entries skipped because
    /// their region was hot / never-scanned (`skipped_hot`, the selector).
    pub dedup_orphan_demoted: AtomicU64,
    pub dedup_orphan_retired: AtomicU64,
    pub dedup_orphan_skipped_hot: AtomicU64,
    /// Stale (hash-mismatch) scrub entries whose now-rc==0 PBA was retired for
    /// reclaim (previously leaked — the decref's freed PBA was dropped).
    pub dedup_scrub_retired: AtomicU64,
    /// Stage-5 per-PBA orphan reclaim: completed lap-barriers published into the
    /// referenced bitmap (one per full cover of the live volume set). Zero when
    /// per-PBA mode is off; rising confirms the barrier is producing snapshots.
    pub dedup_ref_bitmap_published: AtomicU64,
    pub volume_discard_ops: AtomicU64,
    pub volume_discard_lbas: AtomicU64,
    pub discard_blocks_freed: AtomicU64,
}

#[derive(Debug)]
pub(crate) struct ForegroundIoLease {
    metrics: Arc<EngineMetrics>,
}

impl ForegroundIoLease {
    pub(crate) fn record_buffer_append_wait_durable_ns(&self, ns: u64) {
        self.metrics
            .record_buffer_append_wait_durable_foreground_ns(ns);
    }
}

impl Drop for ForegroundIoLease {
    fn drop(&mut self) {
        let result = self.metrics.foreground_io_outstanding.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| current.checked_sub(1),
        );
        debug_assert!(result.is_ok(), "foreground IO gauge underflow");
        if result.is_err() {
            tracing::error!("foreground IO gauge underflow prevented");
        }
    }
}

impl EngineMetrics {
    /// Mark one logical foreground IO active until the last holder drops.
    ///
    /// Callers may clone the returned `Arc` across split LV2 tickets. The gauge
    /// is decremented by the lease object's `Drop`, so success, error, abandon,
    /// channel disconnect, and unwind paths share one accounting boundary.
    pub(crate) fn begin_foreground_io(self: &Arc<Self>) -> Arc<ForegroundIoLease> {
        self.foreground_io_outstanding
            .fetch_add(1, Ordering::Relaxed);
        Arc::new(ForegroundIoLease {
            metrics: Arc::clone(self),
        })
    }

    pub fn record_ublk_write_stages(
        &self,
        queue_wait_ns: u64,
        worker_ns: u64,
        completion_wait_ns: u64,
    ) {
        record_latency_bucket(&self.ublk_write_queue_wait_latency_buckets, queue_wait_ns);
        record_latency_bucket(&self.ublk_write_worker_latency_buckets, worker_ns);
        record_latency_bucket(
            &self.ublk_write_completion_wait_latency_buckets,
            completion_wait_ns,
        );
    }

    pub fn record_buffer_append_prepare_ns(&self, ns: u64) {
        self.buffer_append_prepare_ns
            .fetch_add(ns, Ordering::Relaxed);
        record_latency_bucket(&self.buffer_append_prepare_latency_buckets, ns);
    }

    pub fn record_buffer_append_order_wait_ns(&self, ns: u64) {
        self.buffer_append_order_wait_ns
            .fetch_add(ns, Ordering::Relaxed);
        record_counter_max(&self.buffer_append_order_wait_max_ns, ns);
        record_latency_bucket(&self.buffer_append_order_wait_latency_buckets, ns);
    }

    pub fn record_buffer_append_order_hold_ns(&self, ns: u64) {
        self.buffer_append_order_hold_ns
            .fetch_add(ns, Ordering::Relaxed);
        record_counter_max(&self.buffer_append_order_hold_max_ns, ns);
        record_latency_bucket(&self.buffer_append_order_hold_latency_buckets, ns);
    }

    pub fn record_buffer_append_wait_durable_ns(&self, ns: u64) {
        self.buffer_append_wait_durable_ns
            .fetch_add(ns, Ordering::Relaxed);
        record_latency_bucket(&self.buffer_append_wait_durable_latency_buckets, ns);
        self.buffer_append_wait_durable_fine_latency.record(ns);
    }

    pub(crate) fn record_buffer_append_wait_durable_foreground_ns(&self, ns: u64) {
        self.buffer_append_wait_durable_foreground_fine_latency
            .record(ns);
    }

    #[cfg(test)]
    pub(crate) fn buffer_append_wait_durable_p99_delta(
        &self,
        previous: &mut Vec<u64>,
    ) -> Option<(u64, u64)> {
        histogram_p99_delta(&self.buffer_append_wait_durable_fine_latency, previous)
    }

    #[cfg(test)]
    pub(crate) fn buffer_append_wait_durable_fine_snapshot(&self) -> Vec<u64> {
        self.buffer_append_wait_durable_fine_latency.snapshot()
    }

    pub(crate) fn buffer_append_wait_durable_foreground_p99_delta(
        &self,
        previous: &mut Vec<u64>,
    ) -> Option<(u64, u64)> {
        histogram_p99_delta(
            &self.buffer_append_wait_durable_foreground_fine_latency,
            previous,
        )
    }

    pub(crate) fn buffer_append_wait_durable_foreground_fine_snapshot(&self) -> Vec<u64> {
        self.buffer_append_wait_durable_foreground_fine_latency
            .snapshot()
    }

    pub(crate) fn record_buffer_lv2_staging_queue_ns(&self, ns: u64) {
        self.buffer_lv2_staging_queue_latency.record(ns);
    }

    pub(crate) fn record_buffer_lv2_prepared_queue_ns(&self, ns: u64) {
        self.buffer_lv2_prepared_queue_latency.record(ns);
    }

    pub(crate) fn record_buffer_lv2_group_collect_ns(&self, ns: u64) {
        self.buffer_lv2_group_collect_latency.record(ns);
    }

    pub(crate) fn record_buffer_lv2_payload_write_ns(&self, ns: u64) {
        self.buffer_lv2_payload_write_latency.record(ns);
    }

    pub(crate) fn record_buffer_lv2_payload_profile(&self, wall_ns: u64, cpu_ns: u64) {
        let cpu_ns = cpu_ns.min(wall_ns);
        let offcpu_ns = wall_ns.saturating_sub(cpu_ns);
        self.buffer_lv2_payload_profile_samples
            .fetch_add(1, Ordering::Relaxed);
        self.buffer_lv2_payload_profile_wall_ns
            .fetch_add(wall_ns, Ordering::Relaxed);
        self.buffer_lv2_payload_profile_cpu_ns
            .fetch_add(cpu_ns, Ordering::Relaxed);
        self.buffer_lv2_payload_profile_offcpu_ns
            .fetch_add(offcpu_ns, Ordering::Relaxed);
        record_counter_max(&self.buffer_lv2_payload_profile_wall_max_ns, wall_ns);
        record_counter_max(&self.buffer_lv2_payload_profile_cpu_max_ns, cpu_ns);
        record_counter_max(&self.buffer_lv2_payload_profile_offcpu_max_ns, offcpu_ns);
    }

    pub(crate) fn record_buffer_lv2_checkpoint_write_ns(&self, ns: u64) {
        self.buffer_lv2_checkpoint_write_latency.record(ns);
    }

    pub(crate) fn record_buffer_lv2_root_flush_ns(&self, ns: u64) {
        self.buffer_lv2_root_flush_latency.record(ns);
    }

    pub(crate) fn record_buffer_lv2_watermark_dispatch_ns(&self, ns: u64) {
        self.buffer_lv2_watermark_dispatch_latency.record(ns);
    }

    /// One completed coordinator epoch. `idle` is the time blocked waiting for
    /// a written batch; the rest is the serial critical section that every
    /// acknowledged append waits behind.
    pub(crate) fn record_buffer_lv2_coord_epoch(
        &self,
        idle_ns: u64,
        ckpt_encode_ns: u64,
        ckpt_write_ns: u64,
        flush_ns: u64,
        publish_ns: u64,
    ) {
        self.buffer_lv2_coord_idle_ns
            .fetch_add(idle_ns, Ordering::Relaxed);
        self.buffer_lv2_coord_ckpt_encode_ns
            .fetch_add(ckpt_encode_ns, Ordering::Relaxed);
        self.buffer_lv2_coord_ckpt_write_ns
            .fetch_add(ckpt_write_ns, Ordering::Relaxed);
        self.buffer_lv2_coord_flush_ns
            .fetch_add(flush_ns, Ordering::Relaxed);
        self.buffer_lv2_coord_publish_ns
            .fetch_add(publish_ns, Ordering::Relaxed);
        self.buffer_lv2_coord_epochs.fetch_add(1, Ordering::Relaxed);
        let busy = ckpt_encode_ns
            .saturating_add(ckpt_write_ns)
            .saturating_add(flush_ns)
            .saturating_add(publish_ns);
        record_counter_max(&self.buffer_lv2_coord_busy_max_ns, busy);
    }

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

    pub fn record_read_pool_queue_wait_ns(&self, ns: u64) {
        self.read_pool_queue_wait_ns
            .fetch_add(ns, Ordering::Relaxed);
        record_latency_bucket(&self.read_pool_queue_wait_latency_buckets, ns);
    }

    pub fn record_read_pool_worker_queue_wait_ns(&self, worker_idx: usize, ns: u64) {
        if let Some(counter) = self.read_pool_worker_queue_wait_ns.get(worker_idx) {
            counter.fetch_add(ns, Ordering::Relaxed);
        }
        self.record_read_pool_queue_wait_ns(ns);
    }

    pub fn record_read_pool_submit_wait_ns(&self, worker_idx: usize, ns: u64) {
        self.read_pool_submit_wait_ns
            .fetch_add(ns, Ordering::Relaxed);
        record_latency_bucket(&self.read_pool_submit_wait_latency_buckets, ns);
        if let Some(counter) = self.read_pool_worker_submit_wait_ns.get(worker_idx) {
            counter.fetch_add(ns, Ordering::Relaxed);
        }
    }

    pub fn record_read_pool_decode_ns(&self, ns: u64) {
        self.read_pool_decode_ns.fetch_add(ns, Ordering::Relaxed);
        record_latency_bucket(&self.read_pool_decode_latency_buckets, ns);
    }

    pub fn record_read_pool_worker_batch(&self, worker_idx: usize, requests: u64) {
        if let Some(counter) = self.read_pool_worker_requests.get(worker_idx) {
            counter.fetch_add(requests, Ordering::Relaxed);
        }
        if let Some(counter) = self.read_pool_worker_batches.get(worker_idx) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_read_submit_unit_io_ns(&self, ns: u64) {
        self.read_submit_unit_io_ns.fetch_add(ns, Ordering::Relaxed);
        record_latency_bucket(&self.read_submit_unit_io_latency_buckets, ns);
    }
}

mod defaults;
mod meta_memory;
mod snapshot;
mod status;

pub use meta_memory::{BufferShardSnapshot, MetaMemorySnapshot};
pub use snapshot::EngineMetricsSnapshot;
pub use status::{
    ChunkletIoExecutionClassSnapshot, ChunkletIoExecutionSnapshot, ChunkletPdIoClassSnapshot,
    ChunkletPdIoPdSnapshot, ChunkletPdIoSchedulerSnapshot, EngineStatusSnapshot,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foreground_io_lease_is_shared_and_decrements_exactly_once() {
        let metrics = Arc::new(EngineMetrics::default());
        let first = metrics.begin_foreground_io();
        let second = Arc::clone(&first);
        assert_eq!(metrics.foreground_io_outstanding.load(Ordering::Relaxed), 1);

        drop(first);
        assert_eq!(metrics.foreground_io_outstanding.load(Ordering::Relaxed), 1);
        drop(second);
        assert_eq!(metrics.foreground_io_outstanding.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn fine_latency_bucket_bounds_are_monotonic_and_contain_samples() {
        let bounds = fine_latency_bucket_upper_bounds_ns();
        assert_eq!(bounds.len(), FINE_LATENCY_BUCKETS);
        assert_eq!(&bounds[..17], &(0u64..=16).collect::<Vec<_>>());
        assert_eq!(bounds.last().copied(), Some(u64::MAX));
        assert!(bounds.windows(2).all(|pair| pair[0] < pair[1]));

        for ns in [
            0,
            1,
            15,
            16,
            17,
            31,
            32,
            1_000,
            440_000,
            1_048_576,
            u32::MAX as u64,
            u64::MAX,
        ] {
            let idx = fine_latency_bucket(ns);
            assert!(bounds[idx] >= ns, "bucket {idx} does not cover {ns}");
            if idx > 0 {
                assert!(bounds[idx - 1] < ns, "bucket {idx} starts after {ns}");
            }
        }
    }

    #[test]
    fn fine_latency_histograms_survive_snapshot_delta() {
        let metrics = EngineMetrics::default();
        metrics.record_buffer_lv2_root_flush_ns(100);
        let earlier = metrics.snapshot();
        metrics.record_buffer_lv2_root_flush_ns(440_000);
        metrics.record_buffer_lv2_root_flush_ns(1_000_000);

        let delta = metrics.snapshot().saturating_sub(&earlier);
        assert_eq!(
            delta
                .buffer_lv2_root_flush_latency_buckets
                .iter()
                .sum::<u64>(),
            2
        );
        assert_eq!(
            delta.buffer_lv2_latency_bucket_upper_bounds_ns,
            fine_latency_bucket_upper_bounds_ns()
        );
    }

    #[test]
    fn durable_p99_delta_advances_without_full_metrics_snapshot() {
        let metrics = EngineMetrics::default();
        let mut previous = metrics.buffer_append_wait_durable_fine_snapshot();
        for _ in 0..99 {
            metrics.record_buffer_append_wait_durable_ns(10_000_000);
        }
        metrics.record_buffer_append_wait_durable_ns(200_000_000);

        let (p99_ns, samples) = metrics
            .buffer_append_wait_durable_p99_delta(&mut previous)
            .unwrap();
        assert_eq!(samples, 100);
        assert!(p99_ns >= 10_000_000);
        assert!(p99_ns < 200_000_000);
        assert!(metrics
            .buffer_append_wait_durable_p99_delta(&mut previous)
            .is_none());
    }

    #[test]
    fn foreground_durable_histogram_excludes_aggregate_only_samples() {
        let metrics = EngineMetrics::default();
        let mut previous = metrics.buffer_append_wait_durable_foreground_fine_snapshot();
        metrics.record_buffer_append_wait_durable_ns(900_000_000);
        assert!(metrics
            .buffer_append_wait_durable_foreground_p99_delta(&mut previous)
            .is_none());

        for _ in 0..64 {
            metrics.record_buffer_append_wait_durable_foreground_ns(10_000_000);
        }
        let (p99_ns, samples) = metrics
            .buffer_append_wait_durable_foreground_p99_delta(&mut previous)
            .unwrap();
        assert_eq!(samples, 64);
        assert!(p99_ns >= 10_000_000);
        assert!(p99_ns < 900_000_000);
    }

    #[test]
    fn lv2_payload_profile_splits_wall_and_cpu_time() {
        let metrics = EngineMetrics::default();
        metrics.record_buffer_lv2_payload_profile(1_000, 400);
        metrics.record_buffer_lv2_payload_profile(800, 900);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.buffer_lv2_payload_profile_samples, 2);
        assert_eq!(snapshot.buffer_lv2_payload_profile_wall_ns, 1_800);
        assert_eq!(snapshot.buffer_lv2_payload_profile_cpu_ns, 1_200);
        assert_eq!(snapshot.buffer_lv2_payload_profile_offcpu_ns, 600);
        assert_eq!(snapshot.buffer_lv2_payload_profile_wall_max_ns, 1_000);
        assert_eq!(snapshot.buffer_lv2_payload_profile_cpu_max_ns, 800);
        assert_eq!(snapshot.buffer_lv2_payload_profile_offcpu_max_ns, 600);
    }
}
