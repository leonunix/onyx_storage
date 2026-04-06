use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver, Sender};

use crate::buffer::pipeline::{coalesce, CoalesceUnit, CompressedUnit};
use crate::buffer::pool::WriteBufferPool;
use crate::compress::codec::create_compressor;
use crate::config::FlushConfig;
use crate::error::OnyxResult;
use crate::io::engine::IoEngine;
use crate::lifecycle::VolumeLifecycleManager;
use crate::meta::schema::BlockmapValue;
use crate::meta::store::MetaStore;
use crate::packer::packer::{PackResult, Packer, SealedSlot};
use crate::space::allocator::SpaceAllocator;
use crate::space::extent::Extent;
use crate::types::{CompressionAlgo, Lba, Pba, VolumeId, BLOCK_SIZE};

/// 3-stage flusher pipeline:
///   Stage 1 (coalescer): drain buffer → filter in-flight → coalesce → dispatch
///   Stage 2 (N compress workers): parallel compression
///   Stage 3 (writer): write LV3 → update metadata → report completed seqs
///
/// The coalescer maintains an in-flight set of seq numbers currently being
/// processed by stages 2+3. This prevents the same entry from being dispatched
/// twice when the coalescer loops faster than the writer commits.
pub struct BufferFlusher {
    running: Arc<AtomicBool>,
    coalesce_handle: Option<JoinHandle<()>>,
    compress_handles: Vec<JoinHandle<()>>,
    writer_handle: Option<JoinHandle<()>>,
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
#[doc(hidden)]
pub struct PackedPauseState {
    hit: bool,
    released: bool,
}

struct PackedPauseHook {
    vol_id: String,
    state: Arc<(Mutex<PackedPauseState>, Condvar)>,
}

static TEST_PACKED_PAUSE_HOOK: OnceLock<Mutex<Option<PackedPauseHook>>> = OnceLock::new();

fn test_fail_rules() -> &'static Mutex<Vec<FlushFailRule>> {
    TEST_FAIL_RULES.get_or_init(|| Mutex::new(Vec::new()))
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
    pub fn start(
        pool: Arc<WriteBufferPool>,
        meta: Arc<MetaStore>,
        lifecycle: Arc<VolumeLifecycleManager>,
        allocator: Arc<SpaceAllocator>,
        io_engine: Arc<IoEngine>,
        config: &FlushConfig,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let workers = config.compress_workers.max(1);
        let max_raw = config.coalesce_max_raw_bytes;
        let max_lbas = config.coalesce_max_lbas;

        // Stage 1 → Stage 2
        let (compress_tx, compress_rx) = bounded::<CoalesceUnit>(workers * 4);
        // Stage 2 → Stage 3
        let (write_tx, write_rx) = bounded::<CompressedUnit>(workers * 4);
        // Stage 3 → Stage 1 (feedback: completed seqs)
        let (done_tx, done_rx) = bounded::<Vec<u64>>(workers * 8);

        // Stage 1: Coalescer — needs MetaStore to look up per-volume compression
        let running_c = running.clone();
        let pool_c = pool.clone();
        let meta_c = meta.clone();
        let coalesce_handle = thread::Builder::new()
            .name("flusher-coalesce".into())
            .spawn(move || {
                Self::coalesce_loop(
                    &pool_c,
                    &meta_c,
                    &compress_tx,
                    &done_rx,
                    &running_c,
                    max_raw,
                    max_lbas,
                );
            })
            .expect("failed to spawn coalescer thread");

        // Stage 2: Compress workers (use per-unit compression from CoalesceUnit)
        let mut compress_handles = Vec::with_capacity(workers);
        for i in 0..workers {
            let rx = compress_rx.clone();
            let tx = write_tx.clone();
            let running_w = running.clone();
            let h = thread::Builder::new()
                .name(format!("flusher-compress-{}", i))
                .spawn(move || {
                    Self::compress_loop(&rx, &tx, &running_w);
                })
                .expect("failed to spawn compress worker");
            compress_handles.push(h);
        }
        drop(compress_rx);
        drop(write_tx);

        // Stage 3: Writer (owns the packer)
        let running_w = running.clone();
        let pool_w = pool.clone();
        let allocator_w = allocator.clone();
        let writer_handle = thread::Builder::new()
            .name("flusher-writer".into())
            .spawn(move || {
                let mut packer = Packer::new(allocator_w);
                Self::writer_loop(
                    &write_rx, &pool_w, &meta, &lifecycle, &allocator, &io_engine, &done_tx,
                    &running_w, &mut packer,
                );
            })
            .expect("failed to spawn writer thread");

        Self {
            running,
            coalesce_handle: Some(coalesce_handle),
            compress_handles,
            writer_handle: Some(writer_handle),
        }
    }

    fn coalesce_loop(
        pool: &WriteBufferPool,
        meta: &MetaStore,
        tx: &Sender<CoalesceUnit>,
        done_rx: &Receiver<Vec<u64>>,
        running: &AtomicBool,
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

        while running.load(Ordering::Relaxed) {
            // Drain completed seqs from writer feedback — decrement refcounts
            while let Ok(seqs) = done_rx.try_recv() {
                for seq in seqs {
                    if let Some(count) = in_flight.get_mut(&seq) {
                        *count -= 1;
                        if *count == 0 {
                            in_flight.remove(&seq);
                        }
                    }
                }
            }

            match pool.recover() {
                Ok(entries) if !entries.is_empty() => {
                    // Filter out entries that still have any units in-flight
                    let new_entries: Vec<_> = entries
                        .into_iter()
                        .filter(|e| !in_flight.contains_key(&e.seq))
                        .collect();

                    if new_entries.is_empty() {
                        thread::sleep(Duration::from_millis(5));
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

                    // Count how many units reference each seq
                    for unit in &units {
                        for (seq, _, _) in &unit.seq_lba_ranges {
                            *in_flight.entry(*seq).or_insert(0) += 1;
                        }
                    }

                    for unit in units {
                        if tx.send(unit).is_err() {
                            return;
                        }
                    }
                }
                Ok(_) => thread::sleep(Duration::from_millis(10)),
                Err(e) => {
                    tracing::error!(error = %e, "coalescer: recover failed");
                    thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }

    fn compress_loop(
        rx: &Receiver<CoalesceUnit>,
        tx: &Sender<CompressedUnit>,
        running: &AtomicBool,
    ) {
        while running.load(Ordering::Relaxed) {
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(unit) => {
                    let algo = unit.compression;
                    let compressor = create_compressor(algo);
                    let original_size = unit.raw_data.len();
                    let max_out = compressor.max_compressed_size(original_size);
                    let mut compressed_buf = vec![0u8; max_out];

                    let (compression_byte, compressed_data) =
                        match compressor.compress(&unit.raw_data, &mut compressed_buf) {
                            Some(size) => (algo.to_u8(), compressed_buf[..size].to_vec()),
                            None => (0u8, unit.raw_data.clone()),
                        };

                    let crc32 = crc32fast::hash(&compressed_data);

                    let cu = CompressedUnit {
                        vol_id: unit.vol_id,
                        start_lba: unit.start_lba,
                        lba_count: unit.lba_count,
                        original_size: original_size as u32,
                        compressed_data,
                        compression: compression_byte,
                        crc32,
                        vol_created_at: unit.vol_created_at,
                        seq_lba_ranges: unit.seq_lba_ranges,
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

    fn writer_loop(
        rx: &Receiver<CompressedUnit>,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        done_tx: &Sender<Vec<u64>>,
        running: &AtomicBool,
        packer: &mut Packer,
    ) {
        // Seqs buffered inside the packer's open slot, reported when the slot is sealed.
        let mut buffered_seqs: Vec<u64> = Vec::new();

        while running.load(Ordering::Relaxed) {
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(unit) => {
                    let seqs: Vec<u64> = unit.seq_lba_ranges.iter().map(|(s, _, _)| *s).collect();

                    match packer.pack_or_passthrough(unit) {
                        Ok(PackResult::Passthrough(unit)) => {
                            if let Err(e) =
                                Self::write_unit(&unit, pool, meta, lifecycle, allocator, io_engine)
                            {
                                tracing::error!(
                                    vol = unit.vol_id,
                                    start_lba = unit.start_lba.0,
                                    lba_count = unit.lba_count,
                                    error = %e,
                                    "writer: failed to flush unit"
                                );
                            }
                            let _ = done_tx.send(seqs);
                        }
                        Ok(PackResult::Buffered) => {
                            // Don't report seqs as done yet — entries must stay in-flight
                            // until the packed slot is sealed and written. Otherwise the
                            // coalescer will re-dispatch them.
                            buffered_seqs.extend(&seqs);
                        }
                        Ok(PackResult::SealedSlot(sealed)) => {
                            // Write the sealed slot
                            if let Err(e) = Self::write_packed_slot(
                                &sealed, pool, meta, lifecycle, allocator, io_engine,
                            ) {
                                tracing::error!(
                                    pba = sealed.pba.0,
                                    fragments = sealed.fragments.len(),
                                    error = %e,
                                    "writer: failed to flush packed slot"
                                );
                            }
                            // Report all seqs from the sealed slot as done.
                            // The new fragment that triggered the seal is now buffered
                            // in the new open slot — its seqs stay in buffered_seqs.
                            let sealed_seqs = std::mem::take(&mut buffered_seqs);
                            let _ = done_tx.send(sealed_seqs);
                            // Current unit's seqs go into buffered_seqs for the new slot
                            buffered_seqs.extend(&seqs);
                        }
                        Ok(PackResult::SealedSlotAndPassthrough(sealed, unit)) => {
                            // Sealed slot must be written; unit falls back to direct write.
                            if let Err(e) = Self::write_packed_slot(
                                &sealed, pool, meta, lifecycle, allocator, io_engine,
                            ) {
                                tracing::error!(
                                    pba = sealed.pba.0,
                                    error = %e,
                                    "writer: failed to flush packed slot (alloc fallback)"
                                );
                            }
                            let sealed_seqs = std::mem::take(&mut buffered_seqs);
                            let _ = done_tx.send(sealed_seqs);

                            // Write the unit directly (Passthrough path)
                            if let Err(e) =
                                Self::write_unit(&unit, pool, meta, lifecycle, allocator, io_engine)
                            {
                                tracing::error!(
                                    vol = unit.vol_id,
                                    error = %e,
                                    "writer: failed to flush unit (alloc fallback)"
                                );
                            }
                            let _ = done_tx.send(seqs);
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "writer: packer error");
                            let _ = done_tx.send(seqs);
                        }
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    // Flush the packer's open slot on idle to avoid holding
                    // buffered fragments indefinitely.
                    if let Some(sealed) = packer.flush_open_slot() {
                        if let Err(e) = Self::write_packed_slot(
                            &sealed, pool, meta, lifecycle, allocator, io_engine,
                        ) {
                            tracing::error!(
                                pba = sealed.pba.0,
                                error = %e,
                                "writer: failed to flush packed slot on idle"
                            );
                        }
                        if !buffered_seqs.is_empty() {
                            let _ = done_tx.send(std::mem::take(&mut buffered_seqs));
                        }
                    }
                    continue;
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }
        }

        // Flush any remaining open slot on shutdown
        if let Some(sealed) = packer.flush_open_slot() {
            if let Err(e) =
                Self::write_packed_slot(&sealed, pool, meta, lifecycle, allocator, io_engine)
            {
                tracing::error!(
                    pba = sealed.pba.0,
                    error = %e,
                    "writer: failed to flush final packed slot on shutdown"
                );
            }
            if !buffered_seqs.is_empty() {
                let _ = done_tx.send(buffered_seqs);
            }
        }
    }

    fn write_unit(
        unit: &CompressedUnit,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
    ) -> OnyxResult<()> {
        lifecycle.with_read_lock(&unit.vol_id, || {
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
                tracing::debug!(
                    vol = unit.vol_id,
                    "write_unit: discarding unit (volume deleted or generation mismatch)"
                );
                for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                    let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
                }
                let _ = pool.advance_tail();
                return Ok(());
            }

            let bs = BLOCK_SIZE as usize;
            let blocks_needed = (unit.compressed_data.len() + bs - 1) / bs;

            let allocation = if blocks_needed == 1 {
                Allocation::Single(allocator.allocate_one()?)
            } else {
                let extent = allocator.allocate_extent(blocks_needed as u32)?;
                if (extent.count as usize) < blocks_needed {
                    allocator.free_extent(extent)?;
                    return Err(crate::error::OnyxError::SpaceExhausted);
                }
                Allocation::Extent(extent)
            };
            let pba = allocation.start_pba();

            if let Err(e) = maybe_inject_test_failure(
                &unit.vol_id,
                unit.start_lba,
                FlushFailStage::BeforeIoWrite,
            ) {
                allocation.free(allocator)?;
                return Err(e);
            }

            if let Err(e) = io_engine.write_blocks(pba, &unit.compressed_data) {
                allocation.free(allocator)?;
                return Err(e);
            }

            let mut old_pba_meta: HashMap<Pba, (u32, u32)> = HashMap::new();

            let mut batch_values = Vec::with_capacity(unit.lba_count as usize);
            for i in 0..unit.lba_count {
                let lba = Lba(unit.start_lba.0 + i as u64);
                if let Some(old) = meta.get_mapping(&vol_id, lba)? {
                    let old_blocks = old.unit_compressed_size.div_ceil(BLOCK_SIZE) as u32;
                    let entry = old_pba_meta.entry(old.pba).or_insert((0, old_blocks));
                    entry.0 += 1;
                    entry.1 = entry.1.max(old_blocks);
                }
                batch_values.push((
                    lba,
                    BlockmapValue {
                        pba,
                        compression: unit.compression,
                        unit_compressed_size: unit.compressed_data.len() as u32,
                        unit_original_size: unit.original_size,
                        unit_lba_count: unit.lba_count as u16,
                        offset_in_unit: i as u16,
                        crc32: unit.crc32,
                        slot_offset: 0,
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
                return Err(e);
            }

            if let Err(e) =
                meta.atomic_batch_write(&vol_id, &batch_values, unit.lba_count, &old_pba_decrements)
            {
                allocation.free(allocator)?;
                return Err(e);
            }

            for (old_pba, (_, old_blocks)) in &old_pba_meta {
                let remaining = meta.get_refcount(*old_pba)?;
                if remaining == 0 {
                    if *old_blocks == 1 {
                        allocator.free_one(*old_pba)?;
                    } else {
                        allocator.free_extent(Extent::new(*old_pba, *old_blocks))?;
                    }
                }
            }

            for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                if let Err(e) = pool.mark_flushed(*seq, *lba_start, *lba_count) {
                    tracing::warn!(seq, error = %e, "failed to mark entry flushed");
                }
            }
            pool.advance_tail()?;

            tracing::debug!(
                vol = unit.vol_id,
                start_lba = unit.start_lba.0,
                lba_count = unit.lba_count,
                pba = pba.0,
                compressed = unit.compressed_data.len(),
                original = unit.original_size,
                "flushed compression unit"
            );

            Ok(())
        })
    }

    fn write_packed_slot(
        sealed: &SealedSlot,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        lifecycle: &VolumeLifecycleManager,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
    ) -> OnyxResult<()> {
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

        let locks: Vec<_> = vol_ids
            .iter()
            .map(|vid| lifecycle.get_lock(vid))
            .collect();
        let _guards: Vec<_> = locks.iter().map(|l| l.read().unwrap()).collect();

        // Under lifecycle read locks: check generation, build batch, IO, commit

        // Build blockmap entries and collect old PBA decrements across all fragments
        let mut batch_values: Vec<(VolumeId, Lba, BlockmapValue)> = Vec::new();
        let mut old_pba_meta: HashMap<Pba, (u32, u32)> = HashMap::new();
        let mut total_refcount: u32 = 0;
        let mut all_seq_lba_ranges: Vec<(u64, Lba, u32)> = Vec::new();
        let mut any_discarded = false;

        for frag in &sealed.fragments {
            let unit = &frag.unit;
            let vol_id = VolumeId(unit.vol_id.clone());

            // Lifecycle check: verify volume still exists and generation matches
            let should_discard = match meta.get_volume(&vol_id)? {
                None => true,
                Some(vc)
                    if unit.vol_created_at != 0 && vc.created_at != unit.vol_created_at =>
                {
                    true
                }
                _ => false,
            };

            if should_discard {
                any_discarded = true;
                for (seq, lba_start, lba_count) in &unit.seq_lba_ranges {
                    let _ = pool.mark_flushed(*seq, *lba_start, *lba_count);
                }
                continue;
            }

            for i in 0..unit.lba_count {
                let lba = Lba(unit.start_lba.0 + i as u64);
                if let Some(old) = meta.get_mapping(&vol_id, lba)? {
                    let old_blocks = old.unit_compressed_size.div_ceil(BLOCK_SIZE) as u32;
                    let entry = old_pba_meta.entry(old.pba).or_insert((0, old_blocks));
                    entry.0 += 1;
                    entry.1 = entry.1.max(old_blocks);
                }
                batch_values.push((
                    vol_id.clone(),
                    lba,
                    BlockmapValue {
                        pba: sealed.pba,
                        compression: unit.compression,
                        unit_compressed_size: unit.compressed_data.len() as u32,
                        unit_original_size: unit.original_size,
                        unit_lba_count: unit.lba_count as u16,
                        offset_in_unit: i as u16,
                        crc32: unit.crc32,
                        slot_offset: frag.slot_offset,
                    },
                ));
            }
            total_refcount += unit.lba_count;
            all_seq_lba_ranges.extend(unit.seq_lba_ranges.iter().cloned());
        }

        // If all fragments were discarded, free the slot PBA
        if batch_values.is_empty() {
            allocator.free_one(sealed.pba)?;
            let _ = pool.advance_tail();
            return Ok(());
        }

        maybe_inject_test_failure_packed(&sealed.fragments, FlushFailStage::BeforeIoWrite)?;

        // Write the 4KB slot data to LV3
        if let Err(e) = io_engine.write_blocks(sealed.pba, &sealed.data) {
            allocator.free_one(sealed.pba)?;
            return Err(e);
        }

        let old_pba_decrements: HashMap<Pba, u32> = old_pba_meta
            .iter()
            .map(|(old_pba, (decrement, _))| (*old_pba, *decrement))
            .collect();

        maybe_inject_test_failure_packed(&sealed.fragments, FlushFailStage::BeforeMetaWrite)?;
        maybe_pause_before_packed_meta_write(&sealed.fragments)?;

        // Metadata commit — if this fails, free the PBA to prevent orphaned block
        if let Err(e) = meta.atomic_batch_write_packed(
            &batch_values,
            sealed.pba,
            total_refcount,
            &old_pba_decrements,
        ) {
            allocator.free_one(sealed.pba)?;
            return Err(e);
        }

        // Free old PBAs whose refcount dropped to 0
        for (old_pba, (_, old_blocks)) in &old_pba_meta {
            let remaining = meta.get_refcount(*old_pba)?;
            if remaining == 0 {
                if *old_blocks == 1 {
                    allocator.free_one(*old_pba)?;
                } else {
                    allocator.free_extent(Extent::new(*old_pba, *old_blocks))?;
                }
            }
        }

        // Lifecycle read locks are still held here — metadata is committed,
        // so delete_volume cannot have interleaved.

        // Mark entries flushed
        for (seq, lba_start, lba_count) in &all_seq_lba_ranges {
            if let Err(e) = pool.mark_flushed(*seq, *lba_start, *lba_count) {
                tracing::warn!(seq, error = %e, "failed to mark entry flushed (packed)");
            }
        }
        pool.advance_tail()?;

        tracing::debug!(
            pba = sealed.pba.0,
            fragments = sealed.fragments.len(),
            total_lbas = total_refcount,
            discarded = any_discarded,
            "flushed packed slot"
        );

        Ok(())
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(h) = self.coalesce_handle.take() {
            let _ = h.join();
        }
        for h in self.compress_handles.drain(..) {
            let _ = h.join();
        }
        if let Some(h) = self.writer_handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for BufferFlusher {
    fn drop(&mut self) {
        self.stop();
    }
}
