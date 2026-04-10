use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, unbounded, Receiver, Sender};

use sha2::{Digest, Sha256};

use crate::buffer::pipeline::{coalesce, CoalesceUnit, CompressedUnit};
use crate::buffer::pool::WriteBufferPool;
use crate::compress::codec::create_compressor;
use crate::config::FlushConfig;
use crate::dedup::config::DedupConfig;
use crate::error::OnyxResult;
use crate::io::engine::IoEngine;
use crate::lifecycle::VolumeLifecycleManager;
use crate::meta::schema::{BlockmapValue, ContentHash, DedupEntry, FLAG_DEDUP_SKIPPED};
use crate::meta::store::MetaStore;
use crate::metrics::EngineMetrics;
use crate::packer::packer::{HoleFill, HoleMap, PackResult, Packer, SealedSlot};
use crate::space::allocator::SpaceAllocator;
use crate::space::extent::Extent;
use crate::types::{CompressionAlgo, Lba, Pba, VolumeId, BLOCK_SIZE};

/// 3-stage flusher pipeline:
///   Stage 1 (coalescer): drain ready queue → filter in-flight → coalesce → dispatch
///   Stage 2 (N compress workers): parallel compression
///   Stage 3 (writer): write LV3 → update metadata → report completed seqs
///
/// The coalescer maintains an in-flight set of seq numbers currently being
/// processed by stages 2+3. This prevents the same entry from being dispatched
/// twice when the coalescer loops faster than the writer commits.
pub struct BufferFlusher {
    running: Arc<AtomicBool>,
    lanes: Vec<FlusherLane>,
    in_flight: Arc<FlusherInFlightTracker>,
}

struct FlusherLane {
    coalesce_handle: Option<JoinHandle<()>>,
    dedup_handles: Vec<JoinHandle<()>>,
    compress_handles: Vec<JoinHandle<()>>,
    writer_handle: Option<JoinHandle<()>>,
}

#[derive(Debug, Clone)]
struct ActiveSeq {
    vol_id: String,
    vol_created_at: u64,
}

#[derive(Default)]
struct FlusherInFlightTracker {
    active: Mutex<HashMap<u64, ActiveSeq>>,
    cv: Condvar,
}

impl FlusherInFlightTracker {
    fn track_seq_start(&self, seq: u64, vol_id: &str, vol_created_at: u64) {
        let mut active = self.active.lock().unwrap();
        active.entry(seq).or_insert_with(|| ActiveSeq {
            vol_id: vol_id.to_string(),
            vol_created_at,
        });
    }

    fn track_seq_done(&self, seq: u64) {
        let mut active = self.active.lock().unwrap();
        if active.remove(&seq).is_some() {
            self.cv.notify_all();
        }
    }

    fn wait_volume_generation_idle(
        &self,
        vol_id: &str,
        vol_created_at: u64,
        timeout: Duration,
    ) -> bool {
        let deadline = Instant::now() + timeout;
        let mut active = self.active.lock().unwrap();
        loop {
            let still_active = active
                .values()
                .any(|seq| seq.vol_id == vol_id && seq.vol_created_at == vol_created_at);
            if !still_active {
                return true;
            }

            let now = Instant::now();
            if now >= deadline {
                return false;
            }

            let wait = deadline.saturating_duration_since(now);
            let (guard, _) = self.cv.wait_timeout(active, wait).unwrap();
            active = guard;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum FlushFailStage {
    BeforeIoWrite,
    BeforeMetaWrite,
}

#[derive(Debug, Clone)]
struct FlushFailRule {
    vol_id: String,
    start_lba: Lba,
    stage: FlushFailStage,
    remaining_hits: Option<u32>,
}

static TEST_FAIL_RULES: OnceLock<Mutex<Vec<FlushFailRule>>> = OnceLock::new();
static TEST_DEDUP_HIT_FAIL_RULES: OnceLock<Mutex<Vec<DedupHitFailRule>>> = OnceLock::new();
#[doc(hidden)]
pub struct PackedPauseState {
    hit: bool,
    released: bool,
}

struct PackedPauseHook {
    vol_id: String,
    state: Arc<(Mutex<PackedPauseState>, Condvar)>,
}

#[derive(Debug, Clone)]
struct DedupHitFailRule {
    vol_id: String,
    lba: Lba,
    remaining_hits: Option<u32>,
}

static TEST_PACKED_PAUSE_HOOK: OnceLock<Mutex<Option<PackedPauseHook>>> = OnceLock::new();

fn test_fail_rules() -> &'static Mutex<Vec<FlushFailRule>> {
    TEST_FAIL_RULES.get_or_init(|| Mutex::new(Vec::new()))
}

fn test_dedup_hit_fail_rules() -> &'static Mutex<Vec<DedupHitFailRule>> {
    TEST_DEDUP_HIT_FAIL_RULES.get_or_init(|| Mutex::new(Vec::new()))
}

fn test_packed_pause_hook() -> &'static Mutex<Option<PackedPauseHook>> {
    TEST_PACKED_PAUSE_HOOK.get_or_init(|| Mutex::new(None))
}

fn maybe_inject_test_failure(
    vol_id: &str,
    start_lba: Lba,
    stage: FlushFailStage,
) -> OnyxResult<()> {
    let mut rules = test_fail_rules().lock().unwrap();
    if let Some(idx) = rules.iter().position(|rule| {
        rule.vol_id == vol_id && rule.start_lba == start_lba && rule.stage == stage
    }) {
        let mut remove = false;
        if let Some(remaining) = rules[idx].remaining_hits.as_mut() {
            if *remaining > 0 {
                *remaining -= 1;
            }
            if *remaining == 0 {
                remove = true;
            }
        }
        if remove {
            rules.remove(idx);
        }
        return Err(crate::error::OnyxError::Io(std::io::Error::other(format!(
            "injected flush failure at {:?} for {}:{}",
            stage, vol_id, start_lba.0
        ))));
    }
    Ok(())
}

fn maybe_inject_test_failure_packed(
    fragments: &[crate::packer::packer::SlotFragment],
    stage: FlushFailStage,
) -> OnyxResult<()> {
    for frag in fragments {
        maybe_inject_test_failure(&frag.unit.vol_id, frag.unit.start_lba, stage)?;
    }
    Ok(())
}

fn maybe_inject_dedup_hit_failure(vol_id: &str, lba: Lba) -> OnyxResult<()> {
    let mut rules = test_dedup_hit_fail_rules().lock().unwrap();
    if let Some(idx) = rules
        .iter()
        .position(|rule| rule.vol_id == vol_id && rule.lba == lba)
    {
        let mut remove = false;
        if let Some(remaining) = rules[idx].remaining_hits.as_mut() {
            if *remaining > 0 {
                *remaining -= 1;
            }
            if *remaining == 0 {
                remove = true;
            }
        }
        if remove {
            rules.remove(idx);
        }
        return Err(crate::error::OnyxError::Io(std::io::Error::other(format!(
            "injected dedup hit failure for {}:{}",
            vol_id, lba.0
        ))));
    }
    Ok(())
}

fn maybe_pause_before_packed_meta_write(
    fragments: &[crate::packer::packer::SlotFragment],
) -> OnyxResult<()> {
    let state = {
        let hook = test_packed_pause_hook().lock().unwrap();
        let Some(hook) = hook.as_ref() else {
            return Ok(());
        };
        if !fragments.iter().any(|f| f.unit.vol_id == hook.vol_id) {
            return Ok(());
        }
        hook.state.clone()
    };

    let (lock, cv) = &*state;
    let mut guard = lock.lock().unwrap();
    guard.hit = true;
    cv.notify_all();
    while !guard.released {
        guard = cv.wait(guard).unwrap();
    }
    Ok(())
}

#[doc(hidden)]
pub fn install_test_failpoint(
    vol_id: &str,
    start_lba: Lba,
    stage: FlushFailStage,
    remaining_hits: Option<u32>,
) {
    let mut rules = test_fail_rules().lock().unwrap();
    rules.push(FlushFailRule {
        vol_id: vol_id.to_string(),
        start_lba,
        stage,
        remaining_hits,
    });
}

#[doc(hidden)]
pub fn clear_test_failpoint(vol_id: &str, start_lba: Lba, stage: FlushFailStage) {
    let mut rules = test_fail_rules().lock().unwrap();
    rules.retain(|rule| {
        !(rule.vol_id == vol_id && rule.start_lba == start_lba && rule.stage == stage)
    });
}

#[doc(hidden)]
pub fn install_test_dedup_hit_failpoint(vol_id: &str, lba: Lba, remaining_hits: Option<u32>) {
    let mut rules = test_dedup_hit_fail_rules().lock().unwrap();
    rules.push(DedupHitFailRule {
        vol_id: vol_id.to_string(),
        lba,
        remaining_hits,
    });
}

#[doc(hidden)]
pub fn clear_test_dedup_hit_failpoint(vol_id: &str, lba: Lba) {
    let mut rules = test_dedup_hit_fail_rules().lock().unwrap();
    rules.retain(|rule| !(rule.vol_id == vol_id && rule.lba == lba));
}

#[doc(hidden)]
pub fn install_test_packed_pause_hook(vol_id: &str) -> Arc<(Mutex<PackedPauseState>, Condvar)> {
    let state = Arc::new((
        Mutex::new(PackedPauseState {
            hit: false,
            released: false,
        }),
        Condvar::new(),
    ));
    let mut hook = test_packed_pause_hook().lock().unwrap();
    *hook = Some(PackedPauseHook {
        vol_id: vol_id.to_string(),
        state: state.clone(),
    });
    state
}

#[doc(hidden)]
pub fn clear_test_packed_pause_hook() {
    let mut hook = test_packed_pause_hook().lock().unwrap();
    *hook = None;
}

#[doc(hidden)]
pub fn wait_for_test_packed_pause_hit(
    state: &Arc<(Mutex<PackedPauseState>, Condvar)>,
    timeout: Duration,
) -> bool {
    let (lock, cv) = &**state;
    let guard = lock.lock().unwrap();
    let (guard, _) = cv.wait_timeout_while(guard, timeout, |s| !s.hit).unwrap();
    guard.hit
}

#[doc(hidden)]
pub fn release_test_packed_pause_hook(state: &Arc<(Mutex<PackedPauseState>, Condvar)>) {
    let (lock, cv) = &**state;
    let mut guard = lock.lock().unwrap();
    guard.released = true;
    cv.notify_all();
}

enum Allocation {
    Single(Pba),
    Extent(Extent),
}

impl Allocation {
    fn start_pba(&self) -> Pba {
        match self {
            Self::Single(pba) => *pba,
            Self::Extent(extent) => extent.start,
        }
    }

    fn free(&self, allocator: &SpaceAllocator) -> OnyxResult<()> {
        match self {
            Self::Single(pba) => allocator.free_one(*pba),
            Self::Extent(extent) => allocator.free_extent(*extent),
        }
    }
}

impl BufferFlusher {
    /// compress_workers / dedup.workers are now **per-lane** counts.
    /// No division — each lane gets the configured number of workers.
    fn per_lane_worker_count(configured: usize, _lane_count: usize) -> usize {
        configured.max(1)
    }

    fn elapsed_ns(start: Instant) -> u64 {
        start.elapsed().as_nanos().min(u64::MAX as u128) as u64
    }

    fn record_elapsed(counter: &std::sync::atomic::AtomicU64, start: Instant) {
        counter.fetch_add(Self::elapsed_ns(start), Ordering::Relaxed);
    }

    pub fn start(
        pool: Arc<WriteBufferPool>,
        meta: Arc<MetaStore>,
        lifecycle: Arc<VolumeLifecycleManager>,
        allocator: Arc<SpaceAllocator>,
        io_engine: Arc<IoEngine>,
        config: &FlushConfig,
        hole_map: HoleMap,
        dedup_config: &DedupConfig,
    ) -> Self {
        Self::start_with_metrics(
            pool,
            meta,
            lifecycle,
            allocator,
            io_engine,
            config,
            hole_map,
            dedup_config,
            Arc::new(EngineMetrics::default()),
        )
    }

    pub fn start_with_metrics(
        pool: Arc<WriteBufferPool>,
        meta: Arc<MetaStore>,
        lifecycle: Arc<VolumeLifecycleManager>,
        allocator: Arc<SpaceAllocator>,
        io_engine: Arc<IoEngine>,
        config: &FlushConfig,
        hole_map: HoleMap,
        dedup_config: &DedupConfig,
        metrics: Arc<EngineMetrics>,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let in_flight = Arc::new(FlusherInFlightTracker::default());
        let lane_count = pool.shard_count().max(1);
        let compress_workers =
            Self::per_lane_worker_count(config.compress_workers.max(1), lane_count);
        let max_raw = config.coalesce_max_raw_bytes;
        let max_lbas = config.coalesce_max_lbas;
        let dedup_enabled = dedup_config.enabled;
        let dedup_workers = Self::per_lane_worker_count(dedup_config.workers.max(1), lane_count);
        let dedup_skip_threshold = dedup_config.buffer_skip_threshold_pct;
        let mut lanes = Vec::with_capacity(lane_count);

        for shard_idx in 0..lane_count {
            // Stage 1 → Stage 1.5 (dedup) or Stage 2 (compress)
            let (dedup_tx, dedup_rx) = bounded::<CoalesceUnit>(dedup_workers * 4);
            // Stage 1.5 → Stage 2
            let (compress_tx, compress_rx) = bounded::<CoalesceUnit>(compress_workers * 4);
            // Stage 2 → Stage 3
            let (write_tx, write_rx) = bounded::<CompressedUnit>(compress_workers * 4);
            // Stage 3 → Stage 1 (feedback: completed seqs)
            let (done_tx, done_rx) = unbounded::<Vec<u64>>();

            let running_c = running.clone();
            let pool_c = pool.clone();
            let meta_c = meta.clone();
            let metrics_c = metrics.clone();
            let in_flight_c = in_flight.clone();
            let coalesce_out_tx = if dedup_enabled {
                dedup_tx.clone()
            } else {
                compress_tx.clone()
            };
            let coalesce_handle = thread::Builder::new()
                .name(format!("flusher-coalesce-{}", shard_idx))
                .spawn(move || {
                    Self::coalesce_loop(
                        shard_idx,
                        &pool_c,
                        &meta_c,
                        &coalesce_out_tx,
                        &done_rx,
                        &running_c,
                        &in_flight_c,
                        &metrics_c,
                        max_raw,
                        max_lbas,
                    );
                })
                .expect("failed to spawn coalescer thread");

            let mut dedup_handles = Vec::new();
            if dedup_enabled {
                for worker_idx in 0..dedup_workers {
                    let rx = dedup_rx.clone();
                    let miss_tx = compress_tx.clone();
                    let running_d = running.clone();
                    let meta_d = meta.clone();
                    let pool_d = pool.clone();
                    let lifecycle_d = lifecycle.clone();
                    let allocator_d = allocator.clone();
                    let done_tx_d = done_tx.clone();
                    let metrics_d = metrics.clone();
                    let h = thread::Builder::new()
                        .name(format!("flusher-dedup-{}-{}", shard_idx, worker_idx))
                        .spawn(move || {
                            Self::dedup_loop(
                                shard_idx,
                                &rx,
                                &miss_tx,
                                &meta_d,
                                &pool_d,
                                &lifecycle_d,
                                &allocator_d,
                                &done_tx_d,
                                &running_d,
                                dedup_skip_threshold,
                                &metrics_d,
                            );
                        })
                        .expect("failed to spawn dedup worker");
                    dedup_handles.push(h);
                }
            }
            drop(dedup_rx);
            drop(dedup_tx);
            drop(compress_tx);

            let mut compress_handles = Vec::with_capacity(compress_workers);
            for worker_idx in 0..compress_workers {
                let rx = compress_rx.clone();
                let tx = write_tx.clone();
                let running_w = running.clone();
                let metrics_w = metrics.clone();
                let h = thread::Builder::new()
                    .name(format!("flusher-compress-{}-{}", shard_idx, worker_idx))
                    .spawn(move || {
                        Self::compress_loop(&rx, &tx, &running_w, &metrics_w);
                    })
                    .expect("failed to spawn compress worker");
                compress_handles.push(h);
            }
            drop(compress_rx);
            drop(write_tx);

            let running_w = running.clone();
            let pool_w = pool.clone();
            let meta_w = meta.clone();
            let lifecycle_w = lifecycle.clone();
            let allocator_w = allocator.clone();
            let io_engine_w = io_engine.clone();
            let metrics_w = metrics.clone();
            let hole_map_w = hole_map.clone();
            let writer_handle = thread::Builder::new()
                .name(format!("flusher-writer-{}", shard_idx))
                .spawn(move || {
                    let mut packer = Packer::new_with_lane(allocator_w.clone(), hole_map_w, shard_idx);
                    Self::writer_loop(
                        shard_idx,
                        &write_rx,
                        &pool_w,
                        &meta_w,
                        &lifecycle_w,
                        &allocator_w,
                        &io_engine_w,
                        &done_tx,
                        &running_w,
                        &mut packer,
                        &metrics_w,
                    );
                })
                .expect("failed to spawn writer thread");

            lanes.push(FlusherLane {
                coalesce_handle: Some(coalesce_handle),
                dedup_handles,
                compress_handles,
                writer_handle: Some(writer_handle),
            });
        }

        Self {
            running,
            lanes,
            in_flight,
        }
    }

    fn coalesce_loop(
        shard_idx: usize,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        tx: &Sender<CoalesceUnit>,
        done_rx: &Receiver<Vec<u64>>,
        running: &AtomicBool,
        in_flight_tracker: &FlusherInFlightTracker,
        metrics: &EngineMetrics,
        max_raw: usize,
        max_lbas: u32,
    ) {
        // in_flight tracks how many pipeline units still reference each seq.
        // A multi-LBA entry split into 2 units → refcount=2 for that seq.
        // Only when refcount hits 0 does the seq leave in_flight.
        let mut in_flight: HashMap<u64, u32> = HashMap::new();

        // Cache per-volume compression to avoid repeated MetaStore lookups.
        let mut vol_compression_cache: HashMap<String, CompressionAlgo> = HashMap::new();
        let vol_compression = |vol_id: &str| -> CompressionAlgo {
            // Can't use cache from closure due to borrow rules — inlined below
            if let Ok(Some(vc)) = meta.get_volume(&crate::types::VolumeId(vol_id.to_string())) {
                vc.compression
            } else {
                CompressionAlgo::None
            }
        };
        let ready_timeout = Duration::from_millis(10);
        let retry_snapshot_interval = Duration::from_millis(100);
        let mut last_retry_snapshot = Instant::now();

        while running.load(Ordering::Relaxed) {
            // Drain completed seqs from writer feedback — decrement refcounts
            while let Ok(seqs) = done_rx.try_recv() {
                for seq in seqs {
                    if let Some(count) = in_flight.get_mut(&seq) {
                        *count -= 1;
                        if *count == 0 {
                            in_flight.remove(&seq);
                            in_flight_tracker.track_seq_done(seq);
                        }
                    }
                }
            }

            let mut new_entries = Vec::new();
            let mut seen = std::collections::HashSet::new();

            match pool.recv_ready_timeout_for_shard(shard_idx, ready_timeout) {
                Ok(seq) => {
                    if !in_flight.contains_key(&seq) && seen.insert(seq) {
                        if let Some(entry) = pool.pending_entry(seq) {
                            new_entries.push(entry);
                        }
                    }
                    while let Ok(seq) = pool.try_recv_ready_for_shard(shard_idx) {
                        if !in_flight.contains_key(&seq) && seen.insert(seq) {
                            if let Some(entry) = pool.pending_entry(seq) {
                                new_entries.push(entry);
                            }
                        }
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
            }

            // Safety net for retried/failed units: periodically snapshot the in-memory
            // pending set instead of rescanning the on-disk log on every loop.
            if new_entries.is_empty() && last_retry_snapshot.elapsed() >= retry_snapshot_interval {
                last_retry_snapshot = Instant::now();
                for entry in pool.pending_entries_snapshot_for_shard(shard_idx) {
                    if !in_flight.contains_key(&entry.seq) && seen.insert(entry.seq) {
                        new_entries.push(entry);
                    }
                }
            }

            if new_entries.is_empty() {
                continue;
            }

            // Build per-volume compression lookup using cache
            for entry in &new_entries {
                vol_compression_cache
                    .entry(entry.vol_id.clone())
                    .or_insert_with(|| vol_compression(&entry.vol_id));
            }
            let cache_ref = &vol_compression_cache;
            let units = coalesce(&new_entries, max_raw, max_lbas, &|vid| {
                cache_ref.get(vid).copied().unwrap_or(CompressionAlgo::None)
            });
            if !units.is_empty() {
                metrics.coalesce_runs.fetch_add(1, Ordering::Relaxed);
                metrics
                    .coalesced_units
                    .fetch_add(units.len() as u64, Ordering::Relaxed);
                metrics.coalesced_lbas.fetch_add(
                    units.iter().map(|u| u.lba_count as u64).sum::<u64>(),
                    Ordering::Relaxed,
                );
                metrics.coalesced_bytes.fetch_add(
                    units.iter().map(|u| u.raw_data.len() as u64).sum::<u64>(),
                    Ordering::Relaxed,
                );
            }

            // Count how many units reference each seq
            for unit in &units {
                for (seq, _, _) in &unit.seq_lba_ranges {
                    let count = in_flight.entry(*seq).or_insert(0);
                    if *count == 0 {
                        in_flight_tracker.track_seq_start(*seq, &unit.vol_id, unit.vol_created_at);
                    }
                    *count += 1;
                }
            }

            for unit in units {
                if tx.send(unit).is_err() {
                    return;
                }
            }
        }
    }

    fn compress_loop(
        rx: &Receiver<CoalesceUnit>,
        tx: &Sender<CompressedUnit>,
        running: &AtomicBool,
        metrics: &EngineMetrics,
    ) {
        while running.load(Ordering::Relaxed) {
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(unit) => {
                    let CoalesceUnit {
                        vol_id,
                        start_lba,
                        lba_count,
                        raw_data,
                        compression: algo,
                        vol_created_at,
                        seq_lba_ranges,
                        dedup_skipped,
                        block_hashes,
                        dedup_completion,
                    } = unit;

                    let original_size = raw_data.len();
                    let (compression_byte, compressed_data) = match algo {
                        CompressionAlgo::None => (0u8, raw_data),
                        _ => {
                            let compressor = create_compressor(algo);
                            let max_out = compressor.max_compressed_size(original_size);
                            let mut compressed_buf = vec![0u8; max_out];
                            match compressor.compress(&raw_data, &mut compressed_buf) {
                                Some(size) => (algo.to_u8(), compressed_buf[..size].to_vec()),
                                None => (0u8, raw_data),
                            }
                        }
                    };
                    metrics.compress_units.fetch_add(1, Ordering::Relaxed);
                    metrics
                        .compress_input_bytes
                        .fetch_add(original_size as u64, Ordering::Relaxed);
                    metrics
                        .compress_output_bytes
                        .fetch_add(compressed_data.len() as u64, Ordering::Relaxed);

                    let crc32 = crc32fast::hash(&compressed_data);

                    let cu = CompressedUnit {
                        vol_id,
                        start_lba,
                        lba_count,
                        original_size: original_size as u32,
                        compressed_data,
                        compression: compression_byte,
                        crc32,
                        vol_created_at,
                        seq_lba_ranges,
                        block_hashes,
                        dedup_skipped,
                        dedup_completion,
                    };

                    if tx.send(cu).is_err() {
                        return;
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    /// Dedup stage: hash 4KB blocks, check dedup index, handle hits inline.
    ///
    /// Seq lifecycle: the coalescer tracks one refcount per original unit. The
    /// dedup stage handles hits directly (metadata update + mark_flushed + done_tx)
    /// and sends only miss sub-units to the compress pipeline. If an original unit
    /// has both hits and misses, the miss sub-units inherit seq_lba_ranges for their
    /// LBA range, and the writer does done_tx for them. If ALL blocks are hits,
    /// the dedup worker does done_tx for the whole unit.
    ///
    /// To avoid double-counting seqs in done_tx: each seq from the original unit
    /// is sent to done_tx exactly once — either by the dedup worker (for hit-only seqs)
    /// or by the writer (for seqs that have miss blocks flowing through the pipeline).
    fn dedup_loop(
        shard_idx: usize,
        rx: &Receiver<CoalesceUnit>,
        miss_tx: &Sender<CoalesceUnit>,
        meta: &MetaStore,
        pool: &WriteBufferPool,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        done_tx: &Sender<Vec<u64>>,
        running: &AtomicBool,
        skip_threshold_pct: u8,
        metrics: &EngineMetrics,
    ) {
        let bs = BLOCK_SIZE as usize;
        while running.load(Ordering::Relaxed) {
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(mut unit) => {
                    // Backpressure: skip dedup if this shard's buffer is filling up
                    if pool.fill_percentage_for_shard(shard_idx) > skip_threshold_pct as u8 {
                        unit.dedup_skipped = true;
                        metrics.dedup_skipped_units.fetch_add(1, Ordering::Relaxed);
                        if miss_tx.send(unit).is_err() {
                            return;
                        }
                        continue;
                    }

                    // Split into 4KB blocks, hash each, check dedup index
                    let lba_count = unit.lba_count as usize;
                    let mut is_hit: Vec<bool> = vec![false; lba_count];
                    let mut all_hashes: Vec<ContentHash> = Vec::with_capacity(lba_count);
                    // Collect hit info for processing
                    let mut hit_infos: Vec<(usize, BlockmapValue)> = Vec::new();

                    for i in 0..lba_count {
                        let offset = i * bs;
                        let end = offset + bs;
                        if end > unit.raw_data.len() {
                            all_hashes.push([0u8; 32]);
                            continue;
                        }
                        let block_data = &unit.raw_data[offset..end];
                        let hash: ContentHash = Sha256::digest(block_data).into();
                        all_hashes.push(hash);

                        // Look up dedup index
                        match meta.get_dedup_entry(&hash) {
                            Ok(Some(entry)) => match meta.get_refcount(entry.pba) {
                                Ok(rc) if rc > 0 => {
                                    is_hit[i] = true;
                                    hit_infos.push((i, entry.to_blockmap_value()));
                                }
                                _ => {
                                    let _ = meta.delete_dedup_index(&hash);
                                }
                            },
                            _ => {}
                        }
                    }

                    // Process hits directly in this thread.
                    // Track which hits succeeded so we only mark_flushed for those.
                    let mut successful_hit_indices: Vec<usize> = Vec::new();
                    for (i, existing_value) in &hit_infos {
                        let lba = Lba(unit.start_lba.0 + *i as u64);
                        let vol_id_str = &unit.vol_id;
                        let vol_id = VolumeId(vol_id_str.clone());

                        let result = lifecycle.with_read_lock(vol_id_str, || -> OnyxResult<()> {
                            // Generation check
                            let should_discard = match meta.get_volume(&vol_id)? {
                                None => true,
                                Some(vc)
                                    if unit.vol_created_at != 0
                                        && vc.created_at != unit.vol_created_at =>
                                {
                                    true
                                }
                                _ => false,
                            };
                            if should_discard {
                                return Ok(());
                            }

                            maybe_inject_dedup_hit_failure(vol_id_str, lba)?;

                            let old_mapping = meta.get_mapping(&vol_id, lba)?;
                            meta.atomic_dedup_hit(
                                &vol_id,
                                lba,
                                existing_value,
                                old_mapping.as_ref().map(|m| m.pba),
                            )?;

                            // Free old PBA if refcount dropped to 0
                            if let Some(old) = old_mapping {
                                if old.pba != existing_value.pba {
                                    let remaining = meta.get_refcount(old.pba)?;
                                    if remaining == 0 {
                                        meta.cleanup_dedup_for_pba_standalone(old.pba)?;
                                        let blocks = old.unit_compressed_size.div_ceil(BLOCK_SIZE);
                                        if blocks <= 1 {
                                            allocator.free_one(old.pba)?;
                                        } else {
                                            allocator.free_extent(Extent::new(old.pba, blocks))?;
                                        }
                                    }
                                }
                            }
                            Ok(())
                        });

                        match result {
                            Ok(()) => {
                                successful_hit_indices.push(*i);
                            }
                            Err(e) => {
                                metrics.dedup_hit_failures.fetch_add(1, Ordering::Relaxed);
                                // Hit failed — demote to miss so it goes through
                                // the normal write path on next coalesce cycle.
                                is_hit[*i] = false;
                                tracing::error!(
                                    vol = unit.vol_id,
                                    lba = lba.0,
                                    error = %e,
                                    "dedup worker: hit failed, demoting to miss"
                                );
                            }
                        }
                    }

                    // Only mark_flushed for hits that actually succeeded
                    if !successful_hit_indices.is_empty() {
                        for i in &successful_hit_indices {
                            let lba = Lba(unit.start_lba.0 + *i as u64);
                            for (seq, range_start, range_count) in &unit.seq_lba_ranges {
                                if lba.0 >= range_start.0
                                    && lba.0 < range_start.0 + *range_count as u64
                                {
                                    let _ = pool.mark_flushed(*seq, lba, 1);
                                }
                            }
                        }
                        let _ = pool.advance_tail_for_shard(shard_idx);
                    }

                    // Recheck has_misses after potential demotions
                    let has_misses = is_hit.iter().any(|h| !h);
                    metrics
                        .dedup_hits
                        .fetch_add(successful_hit_indices.len() as u64, Ordering::Relaxed);
                    metrics.dedup_misses.fetch_add(
                        is_hit.iter().filter(|hit| !**hit).count() as u64,
                        Ordering::Relaxed,
                    );
                    if !has_misses {
                        // All blocks were hits — send done_tx for the original unit's seqs
                        let seqs: Vec<u64> =
                            unit.seq_lba_ranges.iter().map(|(s, _, _)| *s).collect();
                        let _ = done_tx.send(seqs);
                        continue;
                    }

                    // Re-coalesce consecutive miss blocks into CoalesceUnits.
                    // Count miss sub-units first to create a shared DedupCompletion.
                    let mut miss_ranges: Vec<(usize, usize)> = Vec::new();
                    let mut miss_start: Option<usize> = None;
                    for i in 0..lba_count {
                        if !is_hit[i] {
                            if miss_start.is_none() {
                                miss_start = Some(i);
                            }
                        } else if let Some(start) = miss_start.take() {
                            miss_ranges.push((start, i));
                        }
                    }
                    if let Some(start) = miss_start {
                        miss_ranges.push((start, lba_count));
                    }

                    // Create shared countdown: all miss sub-units share this.
                    // The LAST sub-unit to complete sends done_tx with the
                    // original unit's full seq list.
                    let all_seqs: Vec<u64> =
                        unit.seq_lba_ranges.iter().map(|(s, _, _)| *s).collect();
                    let completion = crate::buffer::pipeline::DedupCompletion::new(
                        miss_ranges.len() as u32,
                        all_seqs,
                    );

                    let mut miss_units: Vec<CoalesceUnit> = Vec::new();
                    for (start, end) in &miss_ranges {
                        miss_units.push(Self::build_miss_unit(
                            &unit,
                            *start,
                            *end,
                            &all_hashes,
                            Some(completion.clone()),
                        ));
                    }

                    // Send miss units to compress stage
                    for mu in miss_units {
                        if miss_tx.send(mu).is_err() {
                            return;
                        }
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    /// Build a CoalesceUnit from a contiguous range of miss blocks [start, end).
    fn build_miss_unit(
        original: &CoalesceUnit,
        start_idx: usize,
        end_idx: usize,
        hashes: &[ContentHash],
        dedup_completion: Option<Arc<crate::buffer::pipeline::DedupCompletion>>,
    ) -> CoalesceUnit {
        let bs = BLOCK_SIZE as usize;
        let start_lba = Lba(original.start_lba.0 + start_idx as u64);
        let lba_count = (end_idx - start_idx) as u32;
        let data_start = start_idx * bs;
        let data_end = end_idx * bs;
        let raw_data =
            original.raw_data[data_start..data_end.min(original.raw_data.len())].to_vec();

        // Build seq_lba_ranges for the sub-range
        let mut seq_lba_ranges = Vec::new();
        for i in start_idx..end_idx {
            let lba = Lba(original.start_lba.0 + i as u64);
            for (seq, range_start, range_count) in &original.seq_lba_ranges {
                if lba.0 >= range_start.0 && lba.0 < range_start.0 + *range_count as u64 {
                    // Use add_seq_lba logic: extend or start new range
                    if let Some(existing) = seq_lba_ranges.iter_mut().find(
                        |(s, start, count): &&mut (u64, Lba, u32)| {
                            *s == *seq && start.0 + *count as u64 == lba.0
                        },
                    ) {
                        existing.2 += 1;
                    } else {
                        seq_lba_ranges.push((*seq, lba, 1));
                    }
                    // Don't break: multiple seqs can reference the same LBA
                    // (e.g., overwrite dedup in coalescer keeps all seqs)
                }
            }
        }

        let block_hashes_slice = hashes[start_idx..end_idx].to_vec();
        CoalesceUnit {
            vol_id: original.vol_id.clone(),
            start_lba,
            lba_count,
            raw_data,
            compression: original.compression,
            vol_created_at: original.vol_created_at,
            seq_lba_ranges,
            dedup_skipped: false,
            block_hashes: Some(block_hashes_slice),
            dedup_completion,
        }
    }

    const WRITER_BATCH_SIZE: usize = 32;

    fn writer_loop(
        shard_idx: usize,
        rx: &Receiver<CompressedUnit>,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        done_tx: &Sender<Vec<u64>>,
        running: &AtomicBool,
        packer: &mut Packer,
        metrics: &EngineMetrics,
    ) {
        let mut buffered_seqs: Vec<u64> = Vec::new();
        let mut buffered_completions: Vec<Arc<crate::buffer::pipeline::DedupCompletion>> =
            Vec::new();
        let mut tail_dirty = false;

        /// Helper: flush accumulated Passthrough units through write_units_batch.
        macro_rules! flush_pt_batch {
            ($batch:expr, $batch_seqs:expr, $batch_completions:expr) => {
                if !$batch.is_empty() {
                    let results = Self::write_units_batch(
                        shard_idx, &$batch, pool, meta, lifecycle, allocator,
                        io_engine, packer.hole_map(), metrics,
                    );
                    for (idx, result) in results.into_iter().enumerate() {
                        if let Err(e) = result {
                            metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                            tracing::error!(
                                vol = $batch[idx].vol_id,
                                start_lba = $batch[idx].start_lba.0,
                                error = %e,
                                "writer: failed to flush unit in batch"
                            );
                        }
                        match &$batch_completions[idx] {
                            None => { let _ = done_tx.send($batch_seqs[idx].clone()); }
                            Some(dc) => {
                                if let Some(original_seqs) = dc.decrement() {
                                    let _ = done_tx.send(original_seqs);
                                }
                            }
                        }
                    }
                    $batch.clear();
                    $batch_seqs.clear();
                    $batch_completions.clear();
                    tail_dirty = true;
                }
            };
        }

        while running.load(Ordering::Relaxed) {
            let first = match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(unit) => unit,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if let Some(sealed) = packer.flush_open_slot() {
                        if let Err(e) = Self::write_packed_slot(
                            shard_idx,
                            &sealed,
                            pool,
                            meta,
                            lifecycle,
                            allocator,
                            io_engine,
                            packer.hole_map(),
                            metrics,
                        ) {
                            metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                            tracing::error!(pba = sealed.pba.0, error = %e,
                                "writer: failed to flush packed slot on idle");
                        }
                        Self::flush_buffered_done(
                            &mut buffered_seqs,
                            &mut buffered_completions,
                            done_tx,
                        );
                        tail_dirty = true;
                    }
                    if tail_dirty {
                        let _ = pool.advance_tail_for_shard(shard_idx);
                        tail_dirty = false;
                    }
                    continue;
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            };

            // Drain up to WRITER_BATCH_SIZE units.
            let mut incoming = vec![first];
            while incoming.len() < Self::WRITER_BATCH_SIZE {
                match rx.try_recv() {
                    Ok(unit) => incoming.push(unit),
                    Err(_) => break,
                }
            }

            // Run through packer, collect Passthrough units for batching.
            let mut pt_batch: Vec<CompressedUnit> = Vec::new();
            let mut pt_seqs: Vec<Vec<u64>> = Vec::new();
            let mut pt_completions: Vec<Option<Arc<crate::buffer::pipeline::DedupCompletion>>> =
                Vec::new();

            for unit in incoming {
                let seqs: Vec<u64> = unit.seq_lba_ranges.iter().map(|(s, _, _)| *s).collect();
                let completion = unit.dedup_completion.clone();

                match packer.pack_or_passthrough(unit) {
                    Ok(PackResult::Passthrough(unit)) => {
                        pt_batch.push(unit);
                        pt_seqs.push(seqs);
                        pt_completions.push(completion);
                    }
                    Ok(PackResult::Buffered) => match &completion {
                        None => buffered_seqs.extend(&seqs),
                        Some(dc) => buffered_completions.push(dc.clone()),
                    },
                    Ok(PackResult::SealedSlot(sealed)) => {
                        flush_pt_batch!(pt_batch, pt_seqs, pt_completions);
                        if let Err(e) = Self::write_packed_slot(
                            shard_idx,
                            &sealed,
                            pool,
                            meta,
                            lifecycle,
                            allocator,
                            io_engine,
                            packer.hole_map(),
                            metrics,
                        ) {
                            metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                            tracing::error!(pba = sealed.pba.0, error = %e,
                                "writer: failed to flush packed slot");
                        }
                        Self::flush_buffered_done(
                            &mut buffered_seqs,
                            &mut buffered_completions,
                            done_tx,
                        );
                        match &completion {
                            None => buffered_seqs.extend(&seqs),
                            Some(dc) => buffered_completions.push(dc.clone()),
                        }
                    }
                    Ok(PackResult::SealedSlotAndPassthrough(sealed, unit)) => {
                        flush_pt_batch!(pt_batch, pt_seqs, pt_completions);
                        if let Err(e) = Self::write_packed_slot(
                            shard_idx,
                            &sealed,
                            pool,
                            meta,
                            lifecycle,
                            allocator,
                            io_engine,
                            packer.hole_map(),
                            metrics,
                        ) {
                            metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                            tracing::error!(pba = sealed.pba.0, error = %e,
                                "writer: failed to flush packed slot (passthrough fallback)");
                        }
                        Self::flush_buffered_done(
                            &mut buffered_seqs,
                            &mut buffered_completions,
                            done_tx,
                        );
                        pt_batch.push(unit);
                        pt_seqs.push(seqs);
                        pt_completions.push(completion);
                    }
                    Ok(PackResult::FillHole(fill)) => {
                        flush_pt_batch!(pt_batch, pt_seqs, pt_completions);
                        if let Err(e) = Self::write_hole_fill(
                            shard_idx,
                            &fill,
                            pool,
                            meta,
                            lifecycle,
                            allocator,
                            io_engine,
                            packer.hole_map(),
                            metrics,
                        ) {
                            metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                            tracing::error!(error = %e, "writer: failed to fill hole");
                        }
                        match &completion {
                            None => {
                                let _ = done_tx.send(seqs);
                            }
                            Some(dc) => {
                                if let Some(original_seqs) = dc.decrement() {
                                    let _ = done_tx.send(original_seqs);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                        tracing::error!(error = %e, "writer: packer error");
                        match &completion {
                            None => {
                                let _ = done_tx.send(seqs);
                            }
                            Some(dc) => {
                                if let Some(original_seqs) = dc.decrement() {
                                    let _ = done_tx.send(original_seqs);
                                }
                            }
                        }
                    }
                }
            }

            flush_pt_batch!(pt_batch, pt_seqs, pt_completions);
        }

        // Drain remaining on shutdown (use per-unit path for simplicity).
        while let Ok(unit) = rx.try_recv() {
            Self::handle_compressed_unit(
                shard_idx,
                unit,
                pool,
                meta,
                lifecycle,
                allocator,
                io_engine,
                done_tx,
                packer,
                &mut buffered_seqs,
                &mut buffered_completions,
                metrics,
            );
            tail_dirty = true;
        }

        if let Some(sealed) = packer.flush_open_slot() {
            if let Err(e) = Self::write_packed_slot(
                shard_idx,
                &sealed,
                pool,
                meta,
                lifecycle,
                allocator,
                io_engine,
                packer.hole_map(),
                metrics,
            ) {
                metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                tracing::error!(pba = sealed.pba.0, error = %e,
                    "writer: failed to flush final packed slot on shutdown");
            }
            Self::flush_buffered_done(&mut buffered_seqs, &mut buffered_completions, done_tx);
            tail_dirty = true;
        }

        if tail_dirty {
            let _ = pool.advance_tail_for_shard(shard_idx);
        }
    }

    /// Flush buffered done_tx for sealed packer slots.
    /// Handles both normal seqs and dedup completion counters.
    fn flush_buffered_done(
        buffered_seqs: &mut Vec<u64>,
        buffered_completions: &mut Vec<Arc<crate::buffer::pipeline::DedupCompletion>>,
        done_tx: &Sender<Vec<u64>>,
    ) {
        // Normal (non-dedup) buffered seqs
        let normal_seqs: Vec<u64> = buffered_seqs.drain(..).collect();
        if !normal_seqs.is_empty() {
            let _ = done_tx.send(normal_seqs);
        }
        // Dedup completion counters
        for dc in buffered_completions.drain(..) {
            if let Some(original_seqs) = dc.decrement() {
                let _ = done_tx.send(original_seqs);
            }
        }
    }

    /// Handle a compressed unit in the writer thread.
    fn handle_compressed_unit(
        shard_idx: usize,
        unit: CompressedUnit,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        done_tx: &Sender<Vec<u64>>,
        packer: &mut Packer,
        buffered_seqs: &mut Vec<u64>,
        buffered_completions: &mut Vec<Arc<crate::buffer::pipeline::DedupCompletion>>,
        metrics: &EngineMetrics,
    ) {
        let seqs: Vec<u64> = unit.seq_lba_ranges.iter().map(|(s, _, _)| *s).collect();
        let completion = unit.dedup_completion.clone();

        /// Send done_tx for this unit's completion.
        /// - Normal path (no dedup_completion): send seqs directly.
        /// - Dedup split path: decrement the shared counter; only the last
        ///   sub-unit to finish sends done_tx with the ORIGINAL unit's full seqs.
        macro_rules! signal_done {
            ($own_seqs:expr) => {
                match &completion {
                    None => {
                        let _ = done_tx.send($own_seqs);
                    }
                    Some(dc) => {
                        if let Some(original_seqs) = dc.decrement() {
                            let _ = done_tx.send(original_seqs);
                        }
                    }
                }
            };
        }

        match packer.pack_or_passthrough(unit) {
            Ok(PackResult::Passthrough(unit)) => {
                if let Err(e) = Self::write_unit(
                    shard_idx,
                    &unit,
                    pool,
                    meta,
                    lifecycle,
                    allocator,
                    io_engine,
                    packer.hole_map(),
                    metrics,
                ) {
                    metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        vol = unit.vol_id,
                        start_lba = unit.start_lba.0,
                        lba_count = unit.lba_count,
                        error = %e,
                        "writer: failed to flush unit"
                    );
                }
                signal_done!(seqs);
            }
            Ok(PackResult::Buffered) => match &completion {
                None => buffered_seqs.extend(&seqs),
                Some(dc) => buffered_completions.push(dc.clone()),
            },
            Ok(PackResult::SealedSlot(sealed)) => {
                if let Err(e) = Self::write_packed_slot(
                    shard_idx,
                    &sealed,
                    pool,
                    meta,
                    lifecycle,
                    allocator,
                    io_engine,
                    packer.hole_map(),
                    metrics,
                ) {
                    metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        pba = sealed.pba.0,
                        fragments = sealed.fragments.len(),
                        error = %e,
                        "writer: failed to flush packed slot"
                    );
                }
                // Flush done_tx for previously buffered units in the sealed slot
                Self::flush_buffered_done(buffered_seqs, buffered_completions, done_tx);
                // Current unit goes into the new open slot
                match &completion {
                    None => buffered_seqs.extend(&seqs),
                    Some(dc) => buffered_completions.push(dc.clone()),
                }
            }
            Ok(PackResult::SealedSlotAndPassthrough(sealed, unit)) => {
                if let Err(e) = Self::write_packed_slot(
                    shard_idx,
                    &sealed,
                    pool,
                    meta,
                    lifecycle,
                    allocator,
                    io_engine,
                    packer.hole_map(),
                    metrics,
                ) {
                    metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        pba = sealed.pba.0,
                        error = %e,
                        "writer: failed to flush packed slot (alloc fallback)"
                    );
                }
                Self::flush_buffered_done(buffered_seqs, buffered_completions, done_tx);

                if let Err(e) = Self::write_unit(
                    shard_idx,
                    &unit,
                    pool,
                    meta,
                    lifecycle,
                    allocator,
                    io_engine,
                    packer.hole_map(),
                    metrics,
                ) {
                    metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        vol = unit.vol_id,
                        error = %e,
                        "writer: failed to flush unit (alloc fallback)"
                    );
                }
                signal_done!(seqs);
            }
            Ok(PackResult::FillHole(fill)) => {
                if let Err(e) = Self::write_hole_fill(
                    shard_idx,
                    &fill,
                    pool,
                    meta,
                    lifecycle,
                    allocator,
                    io_engine,
                    packer.hole_map(),
                    metrics,
                ) {
                    metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        pba = fill.pba.0,
                        slot_offset = fill.slot_offset,
                        error = %e,
                        "writer: failed to fill hole"
                    );
                }
                signal_done!(seqs);
            }
            Err(e) => {
                metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                tracing::error!(error = %e, "writer: packer error");
                signal_done!(seqs);
            }
        }
    }

    fn write_unit(
        shard_idx: usize,
        unit: &CompressedUnit,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        hole_map: &HoleMap,
        metrics: &EngineMetrics,
    ) -> OnyxResult<()> {
        lifecycle.with_read_lock(&unit.vol_id, || {
            let total_start = Instant::now();
            // Hold the lifecycle read lock from generation validation through
            // metadata commit so delete/create cannot interleave with this flush.
            let vol_id = VolumeId(unit.vol_id.clone());
            let should_discard = match meta.get_volume(&vol_id)? {
                None => true,
                Some(vc) if unit.vol_created_at != 0 && vc.created_at != unit.vol_created_at => {
                    tracing::debug!(
                        vol = unit.vol_id,
                        entry_gen = unit.vol_created_at,
                        current_gen = vc.created_at,
                        "write_unit: generation mismatch, discarding stale unit"
                    );
                    true
                }
                _ => false,
            };
            if should_discard {
                metrics.flush_stale_discards.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    vol = unit.vol_id,
                    "write_unit: discarding unit (volume deleted or generation mismatch)"
                );
                for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                    let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
                }
                let _ = pool.advance_tail_for_shard(shard_idx);
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                return Ok(());
            }

            let bs = BLOCK_SIZE as usize;
            let alloc_start = Instant::now();
            let blocks_needed = (unit.compressed_data.len() + bs - 1) / bs;

            let allocation = if blocks_needed == 1 {
                Allocation::Single(allocator.allocate_one_for_lane(shard_idx)?)
            } else {
                let extent = allocator.allocate_extent(blocks_needed as u32)?;
                if (extent.count as usize) < blocks_needed {
                    allocator.free_extent(extent)?;
                    Self::record_elapsed(&metrics.flush_writer_alloc_ns, alloc_start);
                    Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                    return Err(crate::error::OnyxError::SpaceExhausted);
                }
                Allocation::Extent(extent)
            };
            Self::record_elapsed(&metrics.flush_writer_alloc_ns, alloc_start);
            let pba = allocation.start_pba();

            let io_start = Instant::now();
            if let Err(e) = maybe_inject_test_failure(
                &unit.vol_id,
                unit.start_lba,
                FlushFailStage::BeforeIoWrite,
            ) {
                allocation.free(allocator)?;
                Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                return Err(e);
            }

            if let Err(e) = io_engine.write_blocks(pba, &unit.compressed_data) {
                allocation.free(allocator)?;
                Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                return Err(e);
            }
            Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);

            let meta_start = Instant::now();
            let mut old_pba_meta: HashMap<Pba, (u32, u32)> = HashMap::new();
            // Per-fragment tracking for hole detection: (pba, slot_offset) → (decrement, unit_lba_count, compressed_size)
            let mut old_frag_meta: HashMap<(Pba, u16), (u32, u16, u32)> = HashMap::new();

            // Batch read old mappings via multi_get
            let lbas: Vec<Lba> = (0..unit.lba_count)
                .map(|i| Lba(unit.start_lba.0 + i as u64))
                .collect();
            let old_mappings = meta.multi_get_mappings(&vol_id, &lbas)?;

            let mut batch_values = Vec::with_capacity(unit.lba_count as usize);
            for (i, old) in old_mappings.into_iter().enumerate() {
                if let Some(old) = old {
                    let old_blocks = old.unit_compressed_size.div_ceil(BLOCK_SIZE) as u32;
                    let entry = old_pba_meta.entry(old.pba).or_insert((0, old_blocks));
                    entry.0 += 1;
                    entry.1 = entry.1.max(old_blocks);
                    let frag = old_frag_meta.entry((old.pba, old.slot_offset)).or_insert((
                        0,
                        old.unit_lba_count,
                        old.unit_compressed_size,
                    ));
                    frag.0 += 1;
                }
                let flags = if unit.dedup_skipped {
                    FLAG_DEDUP_SKIPPED
                } else {
                    0
                };
                batch_values.push((
                    lbas[i],
                    BlockmapValue {
                        pba,
                        compression: unit.compression,
                        unit_compressed_size: unit.compressed_data.len() as u32,
                        unit_original_size: unit.original_size,
                        unit_lba_count: unit.lba_count as u16,
                        offset_in_unit: i as u16,
                        crc32: unit.crc32,
                        slot_offset: 0,
                        flags,
                    },
                ));
            }

            let old_pba_decrements: HashMap<Pba, u32> = old_pba_meta
                .iter()
                .map(|(old_pba, (decrement, _))| (*old_pba, *decrement))
                .collect();

            if let Err(e) = maybe_inject_test_failure(
                &unit.vol_id,
                unit.start_lba,
                FlushFailStage::BeforeMetaWrite,
            ) {
                allocation.free(allocator)?;
                Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                return Err(e);
            }

            if let Err(e) =
                meta.atomic_batch_write(&vol_id, &batch_values, unit.lba_count, &old_pba_decrements)
            {
                allocation.free(allocator)?;
                Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
                Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
                return Err(e);
            }
            Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);

            let cleanup_start = Instant::now();
            for (old_pba, (_, old_blocks)) in &old_pba_meta {
                let remaining = meta.get_refcount(*old_pba)?;
                if remaining == 0 {
                    // Clean up dedup index entries for freed PBA
                    meta.cleanup_dedup_for_pba_standalone(*old_pba)?;
                    if *old_blocks == 1 {
                        allocator.free_one(*old_pba)?;
                    } else {
                        allocator.free_extent(Extent::new(*old_pba, *old_blocks))?;
                    }
                }
            }
            Self::record_elapsed(&metrics.flush_writer_cleanup_ns, cleanup_start);

            // Populate dedup index for newly written blocks
            let dedup_start = Instant::now();
            if let Some(ref hashes) = unit.block_hashes {
                let mut dedup_entries = Vec::new();
                for (i, hash) in hashes.iter().enumerate() {
                    if *hash == [0u8; 32] {
                        continue; // Skip empty hashes
                    }
                    dedup_entries.push((
                        *hash,
                        DedupEntry {
                            pba,
                            slot_offset: 0,
                            compression: unit.compression,
                            unit_compressed_size: unit.compressed_data.len() as u32,
                            unit_original_size: unit.original_size,
                            unit_lba_count: unit.lba_count as u16,
                            offset_in_unit: i as u16,
                            crc32: unit.crc32,
                        },
                    ));
                }
                if !dedup_entries.is_empty() {
                    meta.put_dedup_entries(&dedup_entries)?;
                }
            }
            Self::record_elapsed(&metrics.flush_writer_dedup_index_ns, dedup_start);

            // Detect holes: fully-dead fragments in still-live packed PBAs
            let hole_start = Instant::now();
            Self::detect_holes(&old_frag_meta, meta, hole_map, metrics)?;
            Self::record_elapsed(&metrics.flush_writer_hole_detect_ns, hole_start);
            metrics.flush_units_written.fetch_add(1, Ordering::Relaxed);
            metrics
                .flush_unit_bytes
                .fetch_add(unit.compressed_data.len() as u64, Ordering::Relaxed);

            let mark_start = Instant::now();
            for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                if let Err(e) = pool.mark_flushed(*seq, *lba_start, *lba_count) {
                    tracing::warn!(seq, error = %e, "failed to mark entry flushed");
                }
            }
            Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);

            tracing::debug!(
                vol = unit.vol_id,
                start_lba = unit.start_lba.0,
                lba_count = unit.lba_count,
                pba = pba.0,
                compressed = unit.compressed_data.len(),
                original = unit.original_size,
                "flushed compression unit"
            );

            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            Ok(())
        })
    }

    /// Batch-write multiple Passthrough units in one go:
    /// 1. Acquire lifecycle read locks for all unique volumes (sorted, no deadlock)
    /// 2. Validate generations, discard stale units
    /// 3. Allocate PBAs for all units
    /// 4. Batch IO writes
    /// 5. Batch multi_get old mappings
    /// 6. ONE RocksDB WriteBatch for all blockmap + refcount updates
    /// 7. Batch cleanup + dedup index + mark_flushed
    fn write_units_batch(
        shard_idx: usize,
        units: &[CompressedUnit],
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        hole_map: &HoleMap,
        metrics: &EngineMetrics,
    ) -> Vec<OnyxResult<()>> {
        if units.is_empty() {
            return Vec::new();
        }
        let total_start = Instant::now();

        // Acquire lifecycle read locks for all unique volumes (sorted to prevent deadlock).
        let mut vol_ids: Vec<String> = units.iter().map(|u| u.vol_id.clone()).collect();
        vol_ids.sort();
        vol_ids.dedup();
        let locks: Vec<_> = vol_ids.iter().map(|vid| lifecycle.get_lock(vid)).collect();
        let _guards: Vec<_> = locks.iter().map(|l| l.read().unwrap()).collect();

        // Per-unit state tracking.
        let n = units.len();
        let mut results: Vec<OnyxResult<()>> = (0..n).map(|_| Ok(())).collect();
        let mut skip = vec![false; n]; // true = discard (stale volume)
        let mut pbas: Vec<Option<Pba>> = vec![None; n];
        let mut alloc_blocks: Vec<u32> = vec![0; n];

        // Phase 1: Validate generations.
        for (i, unit) in units.iter().enumerate() {
            let vol_id = VolumeId(unit.vol_id.clone());
            let should_discard = match meta.get_volume(&vol_id) {
                Ok(None) => true,
                Ok(Some(vc))
                    if unit.vol_created_at != 0 && vc.created_at != unit.vol_created_at =>
                {
                    true
                }
                Ok(_) => false,
                Err(e) => {
                    results[i] = Err(e);
                    skip[i] = true;
                    continue;
                }
            };
            if should_discard {
                metrics.flush_stale_discards.fetch_add(1, Ordering::Relaxed);
                skip[i] = true;
                for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                    let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
                }
            }
        }

        // Phase 2: Allocate PBAs for non-skipped units.
        let alloc_start = Instant::now();
        for (i, unit) in units.iter().enumerate() {
            if skip[i] {
                continue;
            }
            let bs = BLOCK_SIZE as usize;
            let blocks_needed = (unit.compressed_data.len() + bs - 1) / bs;
            alloc_blocks[i] = blocks_needed as u32;

            let allocation = if blocks_needed == 1 {
                allocator.allocate_one_for_lane(shard_idx).map(|pba| (pba, 1u32))
            } else {
                allocator
                    .allocate_extent(blocks_needed as u32)
                    .and_then(|ext| {
                        if (ext.count as usize) < blocks_needed {
                            allocator.free_extent(ext)?;
                            Err(crate::error::OnyxError::SpaceExhausted)
                        } else {
                            Ok((ext.start, ext.count))
                        }
                    })
            };
            match allocation {
                Ok((pba, _)) => pbas[i] = Some(pba),
                Err(e) => {
                    results[i] = Err(e);
                    skip[i] = true;
                }
            }
        }
        Self::record_elapsed(&metrics.flush_writer_alloc_ns, alloc_start);

        // Phase 3: Parallel IO writes (one scoped thread per unit for NVMe QD > 1).
        let io_start = Instant::now();
        {
            let io_errors: Vec<Option<crate::error::OnyxError>> = std::thread::scope(|s| {
                let handles: Vec<_> = (0..n)
                    .filter(|i| !skip[*i])
                    .map(|i| {
                        let pba = pbas[i].unwrap();
                        let data = &units[i].compressed_data;
                        let blk = alloc_blocks[i];
                        (i, blk, s.spawn(move || io_engine.write_blocks(pba, data)))
                    })
                    .collect();
                let mut errs: Vec<Option<crate::error::OnyxError>> = (0..n).map(|_| None).collect();
                for (i, blk, handle) in handles {
                    match handle.join() {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            let pba = pbas[i].unwrap();
                            if blk == 1 {
                                let _ = allocator.free_one(pba);
                            } else {
                                let _ = allocator.free_extent(Extent::new(pba, blk));
                            }
                            errs[i] = Some(e);
                        }
                        Err(_) => {
                            let pba = pbas[i].unwrap();
                            if blk == 1 {
                                let _ = allocator.free_one(pba);
                            } else {
                                let _ = allocator.free_extent(Extent::new(pba, blk));
                            }
                            errs[i] = Some(crate::error::OnyxError::Io(std::io::Error::other(
                                "IO thread panicked",
                            )));
                        }
                    }
                }
                errs
            });
            for (i, err) in io_errors.into_iter().enumerate() {
                if let Some(e) = err {
                    results[i] = Err(crate::error::OnyxError::Io(std::io::Error::other(
                        format!("IO write failed: {e}"),
                    )));
                    skip[i] = true;
                }
            }
        }
        Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);

        // Phase 4: Batch multi_get old mappings.
        let meta_start = Instant::now();
        // Per-unit: (batch_values, old_pba_meta, old_frag_meta)
        struct UnitMeta {
            batch_values: Vec<(Lba, BlockmapValue)>,
            old_pba_meta: HashMap<Pba, (u32, u32)>,
            old_frag_meta: HashMap<(Pba, u16), (u32, u16, u32)>,
        }
        let mut unit_metas: Vec<Option<UnitMeta>> = (0..n).map(|_| None).collect();

        for (i, unit) in units.iter().enumerate() {
            if skip[i] {
                continue;
            }
            let pba = pbas[i].unwrap();
            let vol_id = VolumeId(unit.vol_id.clone());
            let lbas: Vec<Lba> = (0..unit.lba_count)
                .map(|j| Lba(unit.start_lba.0 + j as u64))
                .collect();
            let old_mappings = match meta.multi_get_mappings(&vol_id, &lbas) {
                Ok(m) => m,
                Err(e) => {
                    if alloc_blocks[i] == 1 {
                        let _ = allocator.free_one(pba);
                    } else {
                        let _ = allocator.free_extent(Extent::new(pba, alloc_blocks[i]));
                    }
                    results[i] = Err(e);
                    skip[i] = true;
                    continue;
                }
            };

            let mut old_pba_meta: HashMap<Pba, (u32, u32)> = HashMap::new();
            let mut old_frag_meta: HashMap<(Pba, u16), (u32, u16, u32)> = HashMap::new();
            let mut batch_values = Vec::with_capacity(unit.lba_count as usize);

            for (j, old) in old_mappings.into_iter().enumerate() {
                if let Some(old) = old {
                    let old_blocks = old.unit_compressed_size.div_ceil(BLOCK_SIZE) as u32;
                    let entry = old_pba_meta.entry(old.pba).or_insert((0, old_blocks));
                    entry.0 += 1;
                    entry.1 = entry.1.max(old_blocks);
                    let frag = old_frag_meta.entry((old.pba, old.slot_offset)).or_insert((
                        0,
                        old.unit_lba_count,
                        old.unit_compressed_size,
                    ));
                    frag.0 += 1;
                }
                let flags = if unit.dedup_skipped {
                    FLAG_DEDUP_SKIPPED
                } else {
                    0
                };
                batch_values.push((
                    lbas[j],
                    BlockmapValue {
                        pba,
                        compression: unit.compression,
                        unit_compressed_size: unit.compressed_data.len() as u32,
                        unit_original_size: unit.original_size,
                        unit_lba_count: unit.lba_count as u16,
                        offset_in_unit: j as u16,
                        crc32: unit.crc32,
                        slot_offset: 0,
                        flags,
                    },
                ));
            }
            unit_metas[i] = Some(UnitMeta {
                batch_values,
                old_pba_meta,
                old_frag_meta,
            });
        }

        // Phase 5: ONE combined WriteBatch for all units.
        let mut vol_ids_owned: Vec<VolumeId> = Vec::new();
        let mut decrements_owned: Vec<HashMap<Pba, u32>> = Vec::new();
        let mut meta_indices: Vec<usize> = Vec::new(); // which units go into the batch

        for (i, unit) in units.iter().enumerate() {
            if skip[i] || unit_metas[i].is_none() {
                continue;
            }
            vol_ids_owned.push(VolumeId(unit.vol_id.clone()));
            let um = unit_metas[i].as_ref().unwrap();
            decrements_owned.push(
                um.old_pba_meta
                    .iter()
                    .map(|(old_pba, (dec, _))| (*old_pba, *dec))
                    .collect(),
            );
            meta_indices.push(i);
        }

        if !meta_indices.is_empty() {
            let batch_args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32, &HashMap<Pba, u32>)> =
                meta_indices
                    .iter()
                    .enumerate()
                    .map(|(batch_idx, &unit_idx)| {
                        let um = unit_metas[unit_idx].as_ref().unwrap();
                        (
                            &vol_ids_owned[batch_idx],
                            um.batch_values.as_slice(),
                            units[unit_idx].lba_count,
                            &decrements_owned[batch_idx],
                        )
                    })
                    .collect();

            if let Err(e) = meta.atomic_batch_write_multi(&batch_args) {
                // Entire batch failed — rollback all allocations.
                for &unit_idx in &meta_indices {
                    let pba = pbas[unit_idx].unwrap();
                    if alloc_blocks[unit_idx] == 1 {
                        let _ = allocator.free_one(pba);
                    } else {
                        let _ = allocator.free_extent(Extent::new(pba, alloc_blocks[unit_idx]));
                    }
                    results[unit_idx] = Err(crate::error::OnyxError::Io(std::io::Error::other(
                        format!("batch write failed: {e}"),
                    )));
                    skip[unit_idx] = true;
                }
            }
        }
        Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);

        // Phase 6: Cleanup (free old PBAs) + dedup index + hole detection.
        // Collect all old PBAs across units, batch-read refcounts, batch-cleanup dedup.
        let cleanup_start = Instant::now();

        // Collect all unique old PBAs with their block counts.
        let mut all_old_pbas: Vec<(Pba, u32)> = Vec::new();
        for &unit_idx in &meta_indices {
            if skip[unit_idx] {
                continue;
            }
            let um = unit_metas[unit_idx].as_ref().unwrap();
            for (old_pba, (_, old_blocks)) in &um.old_pba_meta {
                all_old_pbas.push((*old_pba, *old_blocks));
            }
        }

        // Batch-read refcounts for all old PBAs.
        let old_pba_keys: Vec<Pba> = all_old_pbas.iter().map(|(pba, _)| *pba).collect();
        let refcounts = meta.multi_get_refcounts(&old_pba_keys).unwrap_or_default();
        let mut dead_pbas: Vec<Pba> = Vec::new();
        for (i, (old_pba, old_blocks)) in all_old_pbas.iter().enumerate() {
            let remaining = refcounts.get(i).copied().unwrap_or(0);
            if remaining == 0 {
                dead_pbas.push(*old_pba);
                if *old_blocks == 1 {
                    let _ = allocator.free_one(*old_pba);
                } else {
                    let _ = allocator.free_extent(Extent::new(*old_pba, *old_blocks));
                }
            }
        }

        let mut all_dedup_entries: Vec<(ContentHash, DedupEntry)> = Vec::new();

        for &unit_idx in &meta_indices {
            if skip[unit_idx] {
                continue;
            }
            let um = unit_metas[unit_idx].as_ref().unwrap();
            let unit = &units[unit_idx];
            let pba = pbas[unit_idx].unwrap();

            // Collect dedup index entries for batch write
            if let Some(ref hashes) = unit.block_hashes {
                for (j, hash) in hashes.iter().enumerate() {
                    if *hash == [0u8; 32] {
                        continue;
                    }
                    all_dedup_entries.push((
                        *hash,
                        DedupEntry {
                            pba,
                            slot_offset: 0,
                            compression: unit.compression,
                            unit_compressed_size: unit.compressed_data.len() as u32,
                            unit_original_size: unit.original_size,
                            unit_lba_count: unit.lba_count as u16,
                            offset_in_unit: j as u16,
                            crc32: unit.crc32,
                        },
                    ));
                }
            }

            // Hole detection
            let _ = Self::detect_holes(&um.old_frag_meta, meta, hole_map, metrics);

            metrics.flush_units_written.fetch_add(1, Ordering::Relaxed);
            metrics
                .flush_unit_bytes
                .fetch_add(unit.compressed_data.len() as u64, Ordering::Relaxed);
        }

        // One batch for all dead PBA dedup cleanup (was N standalone WriteBatch commits)
        if !dead_pbas.is_empty() {
            let _ = meta.cleanup_dedup_for_pbas_batch(&dead_pbas);
        }
        // One batch for all new dedup index entries
        if !all_dedup_entries.is_empty() {
            let _ = meta.put_dedup_entries(&all_dedup_entries);
        }
        Self::record_elapsed(&metrics.flush_writer_cleanup_ns, cleanup_start);

        // Phase 7: Batch mark_flushed (memory-only, fast).
        let mark_start = Instant::now();
        for (i, unit) in units.iter().enumerate() {
            if skip[i] {
                continue;
            }
            for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
            }
        }
        Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);
        let _ = pool.advance_tail_for_shard(shard_idx);

        Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
        results
    }

    fn write_packed_slot(
        shard_idx: usize,
        sealed: &SealedSlot,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        hole_map: &HoleMap,
        metrics: &EngineMetrics,
    ) -> OnyxResult<()> {
        let total_start = Instant::now();
        // Collect unique volume IDs and acquire read locks on ALL of them
        // BEFORE doing any work. Sorted to prevent deadlock. Held until
        // metadata commit completes — mirrors write_unit()'s with_read_lock
        // guarantee that delete/create cannot interleave with this flush.
        let mut vol_ids: Vec<String> = sealed
            .fragments
            .iter()
            .map(|f| f.unit.vol_id.clone())
            .collect();
        vol_ids.sort();
        vol_ids.dedup();

        let locks: Vec<_> = vol_ids.iter().map(|vid| lifecycle.get_lock(vid)).collect();
        let _guards: Vec<_> = locks.iter().map(|l| l.read().unwrap()).collect();

        // Under lifecycle read locks: check generation, build batch, IO, commit

        // Build blockmap entries and collect old PBA decrements across all fragments
        let mut batch_values: Vec<(VolumeId, Lba, BlockmapValue)> = Vec::new();
        let mut old_pba_meta: HashMap<Pba, (u32, u32)> = HashMap::new();
        let mut old_frag_meta: HashMap<(Pba, u16), (u32, u16, u32)> = HashMap::new();
        let mut total_refcount: u32 = 0;
        let mut all_seq_lba_ranges: Vec<(u64, Lba, u32)> = Vec::new();
        let mut any_discarded = false;

        for frag in &sealed.fragments {
            let unit = &frag.unit;
            let vol_id = VolumeId(unit.vol_id.clone());

            // Lifecycle check: verify volume still exists and generation matches
            let should_discard = match meta.get_volume(&vol_id)? {
                None => true,
                Some(vc) if unit.vol_created_at != 0 && vc.created_at != unit.vol_created_at => {
                    true
                }
                _ => false,
            };

            if should_discard {
                metrics.flush_stale_discards.fetch_add(1, Ordering::Relaxed);
                any_discarded = true;
                for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                    let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
                }
                continue;
            }

            // Batch read old mappings via multi_get
            let frag_lbas: Vec<Lba> = (0..unit.lba_count)
                .map(|i| Lba(unit.start_lba.0 + i as u64))
                .collect();
            let old_mappings = meta.multi_get_mappings(&vol_id, &frag_lbas)?;

            for (i, old) in old_mappings.into_iter().enumerate() {
                if let Some(old) = old {
                    let old_blocks = old.unit_compressed_size.div_ceil(BLOCK_SIZE) as u32;
                    let entry = old_pba_meta.entry(old.pba).or_insert((0, old_blocks));
                    entry.0 += 1;
                    entry.1 = entry.1.max(old_blocks);
                    let frag_entry = old_frag_meta.entry((old.pba, old.slot_offset)).or_insert((
                        0,
                        old.unit_lba_count,
                        old.unit_compressed_size,
                    ));
                    frag_entry.0 += 1;
                }
                let flags = if unit.dedup_skipped {
                    FLAG_DEDUP_SKIPPED
                } else {
                    0
                };
                batch_values.push((
                    vol_id.clone(),
                    frag_lbas[i],
                    BlockmapValue {
                        pba: sealed.pba,
                        compression: unit.compression,
                        unit_compressed_size: unit.compressed_data.len() as u32,
                        unit_original_size: unit.original_size,
                        unit_lba_count: unit.lba_count as u16,
                        offset_in_unit: i as u16,
                        crc32: unit.crc32,
                        slot_offset: frag.slot_offset,
                        flags,
                    },
                ));
            }
            total_refcount += unit.lba_count;
            all_seq_lba_ranges.extend(unit.seq_lba_ranges.iter().cloned());
        }

        // If all fragments were discarded, free the slot PBA
        if batch_values.is_empty() {
            allocator.free_one(sealed.pba)?;
            let _ = pool.advance_tail_for_shard(shard_idx);
            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            return Ok(());
        }

        let io_start = Instant::now();
        if let Err(e) =
            maybe_inject_test_failure_packed(&sealed.fragments, FlushFailStage::BeforeIoWrite)
        {
            allocator.free_one(sealed.pba)?;
            Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);
            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            return Err(e);
        }

        // Write the 4KB slot data to LV3
        if let Err(e) = io_engine.write_blocks(sealed.pba, &sealed.data) {
            allocator.free_one(sealed.pba)?;
            Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);
            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            return Err(e);
        }
        Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);

        let meta_start = Instant::now();
        let old_pba_decrements: HashMap<Pba, u32> = old_pba_meta
            .iter()
            .map(|(old_pba, (decrement, _))| (*old_pba, *decrement))
            .collect();

        if let Err(e) =
            maybe_inject_test_failure_packed(&sealed.fragments, FlushFailStage::BeforeMetaWrite)
        {
            allocator.free_one(sealed.pba)?;
            Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            return Err(e);
        }
        maybe_pause_before_packed_meta_write(&sealed.fragments)?;

        // Metadata commit — if this fails, free the PBA to prevent orphaned block
        if let Err(e) = meta.atomic_batch_write_packed(
            &batch_values,
            sealed.pba,
            total_refcount,
            &old_pba_decrements,
        ) {
            allocator.free_one(sealed.pba)?;
            Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);
            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            return Err(e);
        }
        Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);

        // Free old PBAs whose refcount dropped to 0
        let cleanup_start = Instant::now();
        for (old_pba, (_, old_blocks)) in &old_pba_meta {
            let remaining = meta.get_refcount(*old_pba)?;
            if remaining == 0 {
                meta.cleanup_dedup_for_pba_standalone(*old_pba)?;
                if *old_blocks == 1 {
                    allocator.free_one(*old_pba)?;
                } else {
                    allocator.free_extent(Extent::new(*old_pba, *old_blocks))?;
                }
            }
        }
        Self::record_elapsed(&metrics.flush_writer_cleanup_ns, cleanup_start);

        // Populate dedup index for newly written fragments
        let dedup_start = Instant::now();
        for frag in &sealed.fragments {
            if let Some(ref hashes) = frag.unit.block_hashes {
                let mut dedup_entries = Vec::new();
                for (i, hash) in hashes.iter().enumerate() {
                    if *hash == [0u8; 32] {
                        continue;
                    }
                    dedup_entries.push((
                        *hash,
                        DedupEntry {
                            pba: sealed.pba,
                            slot_offset: frag.slot_offset,
                            compression: frag.unit.compression,
                            unit_compressed_size: frag.unit.compressed_data.len() as u32,
                            unit_original_size: frag.unit.original_size,
                            unit_lba_count: frag.unit.lba_count as u16,
                            offset_in_unit: i as u16,
                            crc32: frag.unit.crc32,
                        },
                    ));
                }
                if !dedup_entries.is_empty() {
                    meta.put_dedup_entries(&dedup_entries)?;
                }
            }
        }
        Self::record_elapsed(&metrics.flush_writer_dedup_index_ns, dedup_start);

        // Detect holes in packed slots
        let hole_start = Instant::now();
        Self::detect_holes(&old_frag_meta, meta, hole_map, metrics)?;
        Self::record_elapsed(&metrics.flush_writer_hole_detect_ns, hole_start);
        metrics
            .flush_packed_slots_written
            .fetch_add(1, Ordering::Relaxed);
        metrics
            .flush_packed_fragments_written
            .fetch_add(sealed.fragments.len() as u64, Ordering::Relaxed);
        metrics
            .flush_packed_bytes
            .fetch_add(sealed.data.len() as u64, Ordering::Relaxed);

        // Mark entries flushed
        let mark_start = Instant::now();
        for (seq, lba_start, lba_count) in &all_seq_lba_ranges {
            if let Err(e) = pool.mark_flushed(*seq, *lba_start, *lba_count) {
                tracing::warn!(seq, error = %e, "failed to mark entry flushed (packed)");
            }
        }
        Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);

        tracing::debug!(
            pba = sealed.pba.0,
            fragments = sealed.fragments.len(),
            total_lbas = total_refcount,
            discarded = any_discarded,
            "flushed packed slot"
        );

        Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
        Ok(())
    }

    /// Detect holes created by overwrites: when ALL LBAs of a fragment in a
    /// packed slot are overwritten in this batch, and the PBA still has other
    /// live fragments (refcount > 0), the dead fragment's space is a hole.
    ///
    /// Called from write_unit and write_packed_slot after metadata commit.
    fn detect_holes(
        old_frag_meta: &HashMap<(Pba, u16), (u32, u16, u32)>,
        meta: &MetaStore,
        hole_map: &HoleMap,
        metrics: &EngineMetrics,
    ) -> OnyxResult<()> {
        for (&(old_pba, slot_offset), &(decrement, unit_lba_count, compressed_size)) in
            old_frag_meta
        {
            // Only interested in packed fragments (unit_compressed_size < BLOCK_SIZE implies packed)
            // and only if we overwrote ALL LBAs of this fragment in this batch
            if decrement < unit_lba_count as u32 {
                continue;
            }

            // Check PBA still has other live fragments (not entirely freed)
            let remaining = meta.get_refcount(old_pba)?;
            if remaining == 0 {
                continue; // PBA is freed, no hole to track
            }

            // This fragment is fully dead but PBA is still live → hole
            let size = compressed_size as u16;
            crate::packer::packer::insert_hole_coalesced(hole_map, old_pba, slot_offset, size);
            metrics.hole_detections.fetch_add(1, Ordering::Relaxed);

            tracing::debug!(
                pba = old_pba.0,
                slot_offset,
                size,
                "detected hole in packed slot"
            );
        }
        Ok(())
    }

    /// Fill a hole in an existing packed slot: read-modify-write.
    ///
    /// Locks ALL volumes in the packed slot (not just the new fragment's volume)
    /// to prevent concurrent delete_volume from corrupting refcount.
    /// Validates the hole is still free before writing.
    fn write_hole_fill(
        shard_idx: usize,
        fill: &HoleFill,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        hole_map: &HoleMap,
        metrics: &EngineMetrics,
    ) -> OnyxResult<()> {
        let total_start = Instant::now();
        let unit = &fill.unit;
        let vol_id = VolumeId(unit.vol_id.clone());

        // Lock ALL volumes in this packed slot, not just the new fragment's volume.
        // This prevents delete_volume(other_vol) from racing with our refcount update.
        let mut all_vol_ids = meta.find_volume_ids_by_pba(fill.pba)?;
        if !all_vol_ids.contains(&unit.vol_id) {
            all_vol_ids.push(unit.vol_id.clone());
        }
        all_vol_ids.sort();
        all_vol_ids.dedup();

        let locks: Vec<_> = all_vol_ids
            .iter()
            .map(|vid| lifecycle.get_lock(vid))
            .collect();
        let _guards: Vec<_> = locks.iter().map(|l| l.read().unwrap()).collect();

        // Generation check for the writing volume
        let should_discard = match meta.get_volume(&vol_id)? {
            None => true,
            Some(vc) if unit.vol_created_at != 0 && vc.created_at != unit.vol_created_at => true,
            _ => false,
        };
        if should_discard {
            metrics.flush_stale_discards.fetch_add(1, Ordering::Relaxed);
            for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
            }
            let _ = pool.advance_tail_for_shard(shard_idx);
            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            return Ok(());
        }

        // Validate the hole is still free: check that no existing fragment at this
        // PBA overlaps with [slot_offset, slot_offset + compressed_data.len()).
        let fill_size = unit.compressed_data.len() as u16;
        if meta.has_overlap_at_pba(fill.pba, fill.slot_offset, fill_size)? {
            tracing::debug!(
                pba = fill.pba.0,
                slot_offset = fill.slot_offset,
                "hole fill: byte range overlaps with live fragment, skipping"
            );
            // Don't mark flushed — the unit stays in the buffer and will be
            // re-dispatched by the coalescer on the next pass.
            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            return Ok(());
        }

        let io_start = Instant::now();
        // Read existing slot
        let mut slot_data = io_engine.read_blocks(fill.pba, BLOCK_SIZE as usize)?;

        // Overlay new fragment
        let start = fill.slot_offset as usize;
        let end = start + unit.compressed_data.len();
        if end > slot_data.len() {
            Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);
            Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
            return Err(crate::error::OnyxError::Compress(format!(
                "hole fill out of bounds: offset={} + size={} > {}",
                start,
                unit.compressed_data.len(),
                slot_data.len()
            )));
        }
        slot_data[start..end].copy_from_slice(&unit.compressed_data);

        // Write back
        io_engine.write_blocks(fill.pba, &slot_data)?;
        Self::record_elapsed(&metrics.flush_writer_io_ns, io_start);

        // Collect old PBA decrements
        let meta_start = Instant::now();
        let mut old_pba_decrements: HashMap<Pba, u32> = HashMap::new();
        let mut old_pba_meta: HashMap<Pba, (u32, u32)> = HashMap::new();
        let mut old_frag_meta: HashMap<(Pba, u16), (u32, u16, u32)> = HashMap::new();

        let mut batch_values = Vec::with_capacity(unit.lba_count as usize);
        for i in 0..unit.lba_count {
            let lba = Lba(unit.start_lba.0 + i as u64);
            if let Some(old) = meta.get_mapping(&vol_id, lba)? {
                let old_blocks = old.unit_compressed_size.div_ceil(BLOCK_SIZE) as u32;
                let entry = old_pba_meta.entry(old.pba).or_insert((0, old_blocks));
                entry.0 += 1;
                entry.1 = entry.1.max(old_blocks);
                let frag = old_frag_meta.entry((old.pba, old.slot_offset)).or_insert((
                    0,
                    old.unit_lba_count,
                    old.unit_compressed_size,
                ));
                frag.0 += 1;
            }
            let flags = if unit.dedup_skipped {
                FLAG_DEDUP_SKIPPED
            } else {
                0
            };
            batch_values.push((
                lba,
                BlockmapValue {
                    pba: fill.pba,
                    compression: unit.compression,
                    unit_compressed_size: unit.compressed_data.len() as u32,
                    unit_original_size: unit.original_size,
                    unit_lba_count: unit.lba_count as u16,
                    offset_in_unit: i as u16,
                    crc32: unit.crc32,
                    slot_offset: fill.slot_offset,
                    flags,
                },
            ));
        }

        // Separate self-referencing decrements (old_pba == fill.pba) from external.
        // atomic_batch_write SETs the new PBA's refcount, then DECREMENTS old PBAs.
        // If old_pba == fill.pba, both target the same key and the last write wins.
        // Fix: fold self-decrements into the new_rc calculation.
        let mut self_decrement: u32 = 0;
        for (old_pba, (decrement, _)) in &old_pba_meta {
            if *old_pba == fill.pba {
                self_decrement += decrement;
            } else {
                old_pba_decrements.insert(*old_pba, *decrement);
            }
        }

        let net_increment = unit.lba_count - self_decrement;
        meta.atomic_batch_write_hole_fill(&vol_id, &batch_values, fill.pba, net_increment, &old_pba_decrements)?;
        Self::record_elapsed(&metrics.flush_writer_meta_ns, meta_start);

        // Free old PBAs whose refcount dropped to 0
        let cleanup_start = Instant::now();
        for (old_pba, (_, old_blocks)) in &old_pba_meta {
            let remaining = meta.get_refcount(*old_pba)?;
            if remaining == 0 {
                meta.cleanup_dedup_for_pba_standalone(*old_pba)?;
                if *old_blocks == 1 {
                    allocator.free_one(*old_pba)?;
                } else {
                    allocator.free_extent(Extent::new(*old_pba, *old_blocks))?;
                }
            }
        }
        Self::record_elapsed(&metrics.flush_writer_cleanup_ns, cleanup_start);

        // Populate dedup index for newly written blocks (same as write_unit path)
        let dedup_start = Instant::now();
        if let Some(ref hashes) = unit.block_hashes {
            let mut dedup_entries = Vec::new();
            for (i, hash) in hashes.iter().enumerate() {
                if *hash == [0u8; 32] {
                    continue;
                }
                dedup_entries.push((
                    *hash,
                    DedupEntry {
                        pba: fill.pba,
                        slot_offset: fill.slot_offset,
                        compression: unit.compression,
                        unit_compressed_size: unit.compressed_data.len() as u32,
                        unit_original_size: unit.original_size,
                        unit_lba_count: unit.lba_count as u16,
                        offset_in_unit: i as u16,
                        crc32: unit.crc32,
                    },
                ));
            }
            if !dedup_entries.is_empty() {
                meta.put_dedup_entries(&dedup_entries)?;
            }
        }
        Self::record_elapsed(&metrics.flush_writer_dedup_index_ns, dedup_start);

        // Detect any new holes from the old PBAs
        let hole_start = Instant::now();
        Self::detect_holes(&old_frag_meta, meta, hole_map, metrics)?;
        Self::record_elapsed(&metrics.flush_writer_hole_detect_ns, hole_start);
        metrics.flush_hole_fills.fetch_add(1, Ordering::Relaxed);
        metrics
            .flush_hole_fill_bytes
            .fetch_add(unit.compressed_data.len() as u64, Ordering::Relaxed);

        // Mark flushed
        let mark_start = Instant::now();
        for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
            if let Err(e) = pool.mark_flushed(*seq, *lba_start, *lba_count) {
                tracing::warn!(seq, error = %e, "failed to mark flushed (hole fill)");
            }
        }
        Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);

        // Inject remainder back into hole_map only after successful write.
        let frag_size = unit.compressed_data.len() as u16;
        let remainder = fill.hole_size.saturating_sub(frag_size);
        if remainder > 0 {
            crate::packer::packer::insert_hole_coalesced(
                hole_map,
                fill.pba,
                fill.slot_offset + frag_size,
                remainder,
            );
        }

        tracing::debug!(
            pba = fill.pba.0,
            slot_offset = fill.slot_offset,
            remainder,
            vol = unit.vol_id,
            lba_count = unit.lba_count,
            "filled hole in packed slot"
        );

        Self::record_elapsed(&metrics.flush_writer_total_ns, total_start);
        Ok(())
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        self.join_lanes();
    }

    pub(crate) fn wait_volume_generation_idle(
        &self,
        vol_id: &str,
        vol_created_at: u64,
        timeout: Duration,
    ) -> bool {
        self.in_flight
            .wait_volume_generation_idle(vol_id, vol_created_at, timeout)
    }

    /// Wait for all pending buffer entries to be flushed, then stop.
    /// Used during graceful shutdown to ensure the buffer device is clean
    /// (e.g. before a shard count change on next startup).
    pub fn drain_and_stop(&mut self, pool: &crate::buffer::pool::WriteBufferPool) {
        // Keep flusher running while there are pending entries
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            let pending = pool.pending_count();
            if pending == 0 {
                tracing::info!("flusher drain complete — buffer is clean");
                break;
            }
            if std::time::Instant::now() > deadline {
                tracing::warn!(
                    pending,
                    "flusher drain timeout after 60s — stopping with unflushed entries"
                );
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        self.running.store(false, Ordering::Relaxed);
        self.join_lanes();
    }

    fn join_lanes(&mut self) {
        for lane in &mut self.lanes {
            if let Some(h) = lane.coalesce_handle.take() {
                let _ = h.join();
            }
            for h in lane.dedup_handles.drain(..) {
                let _ = h.join();
            }
            for h in lane.compress_handles.drain(..) {
                let _ = h.join();
            }
            if let Some(h) = lane.writer_handle.take() {
                let _ = h.join();
            }
        }
    }
}

impl Drop for BufferFlusher {
    fn drop(&mut self) {
        self.stop();
    }
}
