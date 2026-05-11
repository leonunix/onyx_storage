use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::buffer::flush::BufferFlusher;
use crate::buffer::pool::WriteBufferPool;
use crate::error::OnyxResult;
use crate::io::engine::IoEngine;
use crate::io::read_pool::ReadPool;
use crate::meta::schema::BlockmapValue;
use crate::meta::store::MetaStore;
use crate::metrics::EngineMetrics;
use crate::space::allocator::SpaceAllocator;
use crate::space::hazard::PbaHazardGuard;
use crate::types::{Lba, VolumeId, ZoneId, BLOCK_SIZE};
use crate::zone::read;
use onyx_metadb::VolumeOrdinal;

// Normal ublk reads top out at 1 MiB (256 LBAs in the detailed profile). Once
// metadb range reads only touch the leaf shards covered by the request, the
// range path is cheaper for medium/large reads because it skips per-LBA hole
// probes on sparse volumes. Tiny reads stay on multi_get: range iterator setup
// is measurably more expensive for the 4K-32K fio mix.
const RANGE_META_LOOKUP_MIN_LBAS: u32 = 32;
type ReadUnitKey = (u64, u16, u32);

struct ReadUnitGroup {
    mapping: BlockmapValue,
    members: Vec<(usize, u16)>, // (out_buf slot, offset_in_unit)
    _hazard_guards: Vec<PbaHazardGuard>,
}

struct RawExtentGroup {
    start_slot: usize,
    mappings: Vec<BlockmapValue>,
    _hazard_guards: Vec<PbaHazardGuard>,
}

struct PinnedMapping {
    slot: usize,
    mapping: BlockmapValue,
    hazard_guard: Option<PbaHazardGuard>,
}

enum ReadBatchOutcome {
    Complete,
    Retry(VolumeOrdinal),
}

impl RawExtentGroup {
    fn new(slot: usize, mapping: BlockmapValue) -> Self {
        Self {
            start_slot: slot,
            mappings: vec![mapping],
            _hazard_guards: Vec::new(),
        }
    }

    fn can_extend(&self, slot: usize, mapping: &BlockmapValue) -> bool {
        let Some(prev) = self.mappings.last() else {
            return false;
        };
        slot == self.start_slot + self.mappings.len()
            && is_single_raw_block(prev)
            && is_single_raw_block(mapping)
            && mapping.pba.0 == prev.pba.0 + 1
    }
}

#[inline]
fn is_single_raw_block(mapping: &BlockmapValue) -> bool {
    mapping.compression == 0
        && mapping.slot_offset == 0
        && mapping.unit_compressed_size == BLOCK_SIZE
        && mapping.unit_original_size == BLOCK_SIZE
        && mapping.unit_lba_count == 1
        && mapping.offset_in_unit == 0
}

fn push_read_unit_group(
    groups: &mut HashMap<ReadUnitKey, ReadUnitGroup>,
    slot: usize,
    mapping: BlockmapValue,
    hazard_guard: Option<PbaHazardGuard>,
) {
    let key: ReadUnitKey = (
        mapping.pba.0,
        mapping.slot_offset,
        mapping.unit_compressed_size,
    );
    let entry = groups.entry(key).or_insert_with(|| ReadUnitGroup {
        mapping,
        members: Vec::new(),
        _hazard_guards: Vec::new(),
    });
    if let Some(guard) = hazard_guard {
        entry._hazard_guards.push(guard);
    }
    entry.members.push((slot, mapping.offset_in_unit));
}

fn copy_pending_block(
    pending: &crate::buffer::commit_log::PendingEntry,
    lba: u64,
    vol_created_at: u64,
    dst: &mut [u8],
) -> bool {
    if vol_created_at != 0 && pending.vol_created_at != vol_created_at {
        return false;
    }
    let entry_end = pending.start_lba.0.saturating_add(pending.lba_count as u64);
    if lba < pending.start_lba.0 || lba >= entry_end {
        return false;
    }
    let Some(payload) = pending.payload.as_ref() else {
        return false;
    };
    let offset = (lba - pending.start_lba.0) as usize * BLOCK_SIZE as usize;
    let end = offset + BLOCK_SIZE as usize;
    if end > payload.len() {
        return false;
    }
    dst.copy_from_slice(&payload[offset..end]);
    true
}

/// Routes IO across LBAs.
///
/// Both reads and writes execute inline on the caller thread:
/// * Writes go straight into `WriteBufferPool::append` — same-zone ordering is
///   preserved by the per-shard append lock inside the pool.
/// * Reads run via `crate::zone::read::execute_read` — the buffer DashMap is
///   lock-free and `MetaStore::get_mapping` is a metadb point-get, so no
///   external serialization is needed.
///
/// `zone_count` / `zone_size_blocks` are kept for write-splitting at zone
/// boundaries (so a wide write doesn't span shards on disk) and for metric
/// labelling, but no per-zone worker threads are spawned anymore.
pub struct ZoneManager {
    zone_size_blocks: u64,
    zone_count: u32,
    io_engine: Arc<IoEngine>,
    buffer_pool: Arc<WriteBufferPool>,
    meta: Arc<MetaStore>,
    allocator: Option<Arc<SpaceAllocator>>,
    candidate: crate::dedup::CandidateCache,
    metrics: Arc<EngineMetrics>,
    /// Optional LV3 read pool. When present, mapped reads dispatch here for
    /// batched io_uring submission + parallel decompression. When absent,
    /// reads fall back to inline `IoEngine::read_blocks` on the caller thread.
    read_pool: Option<Arc<ReadPool>>,
}

#[inline]
fn elapsed_ns(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

/// RAII guard that records elapsed wall time on drop. Used so `submit_reads`
/// charges the total ns counter once on every exit (early-return paths and
/// the success path) without having to thread the bookkeeping through each
/// branch.
struct ReadSubmitTimer<'a> {
    counter: &'a std::sync::atomic::AtomicU64,
    start: Instant,
}

impl<'a> ReadSubmitTimer<'a> {
    fn new(counter: &'a std::sync::atomic::AtomicU64, start: Instant) -> Self {
        Self { counter, start }
    }
}

impl Drop for ReadSubmitTimer<'_> {
    fn drop(&mut self) {
        self.counter
            .fetch_add(elapsed_ns(self.start), std::sync::atomic::Ordering::Relaxed);
    }
}

impl ZoneManager {
    pub fn new(
        zone_count: u32,
        zone_size_blocks: u64,
        meta: Arc<MetaStore>,
        buffer_pool: Arc<WriteBufferPool>,
        io_engine: Arc<IoEngine>,
    ) -> OnyxResult<Self> {
        Self::new_with_metrics(
            zone_count,
            zone_size_blocks,
            meta,
            buffer_pool,
            io_engine,
            Arc::new(EngineMetrics::default()),
            None,
            crate::dedup::CandidateCache::new(1, 1),
        )
    }

    pub fn new_with_metrics(
        zone_count: u32,
        zone_size_blocks: u64,
        meta: Arc<MetaStore>,
        buffer_pool: Arc<WriteBufferPool>,
        io_engine: Arc<IoEngine>,
        metrics: Arc<EngineMetrics>,
        allocator: Option<Arc<SpaceAllocator>>,
        candidate: crate::dedup::CandidateCache,
    ) -> OnyxResult<Self> {
        Self::new_full(
            zone_count,
            zone_size_blocks,
            meta,
            buffer_pool,
            io_engine,
            metrics,
            allocator,
            candidate,
            None,
        )
    }

    /// Full constructor that additionally takes a shared LV3 `ReadPool`. Pass
    /// `Some(pool)` in production (built from `config.storage.read_pool_workers`)
    /// so mapped reads enjoy batched io_uring + parallel decompress; pass
    /// `None` to keep the legacy inline LV3 path.
    pub fn new_full(
        zone_count: u32,
        zone_size_blocks: u64,
        meta: Arc<MetaStore>,
        buffer_pool: Arc<WriteBufferPool>,
        io_engine: Arc<IoEngine>,
        metrics: Arc<EngineMetrics>,
        allocator: Option<Arc<SpaceAllocator>>,
        candidate: crate::dedup::CandidateCache,
        read_pool: Option<Arc<ReadPool>>,
    ) -> OnyxResult<Self> {
        tracing::info!(
            zone_count,
            zone_size_blocks,
            read_pool_workers = read_pool.as_ref().map(|p| p.worker_count()).unwrap_or(0),
            "zone manager initialised (inline read/write — no zone worker threads)"
        );

        Ok(Self {
            zone_size_blocks,
            zone_count,
            io_engine,
            buffer_pool,
            meta,
            allocator,
            candidate,
            metrics,
            read_pool,
        })
    }

    /// Access the shared engine metrics.
    pub fn metrics(&self) -> &Arc<EngineMetrics> {
        &self.metrics
    }

    pub fn volume_ordinal(&self, vol_id: &str) -> OnyxResult<VolumeOrdinal> {
        self.meta.volume_ordinal_str(vol_id)
    }

    /// Determine which zone handles a given LBA
    pub fn zone_for_lba(&self, lba: Lba) -> ZoneId {
        let zone_idx = (lba.0 / self.zone_size_blocks) % self.zone_count as u64;
        ZoneId(zone_idx as u32)
    }

    /// Submit a write IO covering one or more contiguous LBAs.
    /// Automatically splits at zone boundaries to preserve the "same zone = serial" invariant.
    pub fn submit_write(
        &self,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        data: &[u8],
        vol_created_at: u64,
    ) -> OnyxResult<()> {
        let total_start = Instant::now();
        let result = (|| {
            if lba_count == 0 {
                return Ok(());
            }

            let chunks = if lba_count == 0 {
                0
            } else {
                let first_zone = start_lba.0 / self.zone_size_blocks;
                let last_lba = start_lba.0 + lba_count as u64 - 1;
                let last_zone = last_lba / self.zone_size_blocks;
                last_zone.saturating_sub(first_zone) + 1
            };

            let block_size = BLOCK_SIZE as usize;
            let mut remaining_lbas = lba_count as u64;
            let mut current_lba = start_lba.0;
            let mut data_offset = 0usize;

            while remaining_lbas > 0 {
                let zone_end_lba =
                    ((current_lba / self.zone_size_blocks) + 1) * self.zone_size_blocks;
                let lbas_in_this_zone =
                    remaining_lbas.min(zone_end_lba.saturating_sub(current_lba));
                let byte_len = lbas_in_this_zone as usize * block_size;
                let end = data_offset + byte_len;
                self.buffer_pool.append(
                    vol_id,
                    Lba(current_lba),
                    lbas_in_this_zone as u32,
                    &data[data_offset..end],
                    vol_created_at,
                )?;
                current_lba += lbas_in_this_zone;
                remaining_lbas -= lbas_in_this_zone;
                data_offset = end;
            }

            self.metrics
                .zone_write_dispatches
                .fetch_add(chunks, std::sync::atomic::Ordering::Relaxed);
            self.metrics
                .zone_write_lbas
                .fetch_add(lba_count as u64, std::sync::atomic::Ordering::Relaxed);
            if chunks > 1 {
                self.metrics
                    .zone_write_split_ops
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }

            Ok(())
        })();

        self.metrics.zone_submit_write_ns.fetch_add(
            total_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        result
    }

    /// Submit a read IO. Executes inline on the caller thread — no zone-worker
    /// channel hop, no per-zone serialization. Returns `Ok(Some(data))` if
    /// mapped, `Ok(None)` if unmapped, `Err` on failure.
    pub fn submit_read(&self, vol_id: &str, lba: Lba) -> OnyxResult<Option<Vec<u8>>> {
        self.submit_read_with_generation(vol_id, lba, 0)
    }

    pub fn submit_read_with_generation(
        &self,
        vol_id: &str,
        lba: Lba,
        vol_created_at: u64,
    ) -> OnyxResult<Option<Vec<u8>>> {
        self.metrics
            .zone_read_dispatches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        read::execute_read(
            &self.meta,
            &self.buffer_pool,
            &self.io_engine,
            &self.metrics,
            self.read_pool.as_deref(),
            self.allocator.as_ref().map(|allocator| allocator.hazards()),
            vol_id,
            lba,
            vol_created_at,
        )
    }

    /// Vectorized block-aligned read for a contiguous LBA range. Writes
    /// directly into `out_buf`, which must be `count * BLOCK_SIZE` bytes.
    ///
    /// Why it's faster than looping `submit_read_with_generation` per LBA:
    /// one `multi_get_mappings` instead of N metadb point gets; LBAs sharing
    /// a compression unit on disk do **one** io_uring read + **one**
    /// decompression, then fan out into N output slots; per-LBA `to_vec()`
    /// intermediate copies are eliminated by copying straight into `out_buf`.
    ///
    /// Unmapped LBAs are zero-filled. A unit-level error aborts the batch and
    /// returns the first error — callers (ublk) map it to EIO for the whole
    /// user IO, matching the previous per-LBA loop's behavior.
    pub fn submit_reads(
        &self,
        vol_id: &str,
        start_lba: Lba,
        count: u32,
        vol_created_at: u64,
        out_buf: &mut [u8],
    ) -> OnyxResult<()> {
        self.submit_reads_with_ordinal(vol_id, None, start_lba, count, vol_created_at, out_buf)
    }

    pub fn submit_reads_with_ordinal(
        &self,
        vol_id: &str,
        vol_ord: Option<VolumeOrdinal>,
        start_lba: Lba,
        count: u32,
        vol_created_at: u64,
        out_buf: &mut [u8],
    ) -> OnyxResult<()> {
        let mut vol_ord = vol_ord;
        loop {
            match self.submit_reads_with_ordinal_once(
                vol_id,
                vol_ord,
                start_lba,
                count,
                vol_created_at,
                out_buf,
            )? {
                ReadBatchOutcome::Complete => return Ok(()),
                ReadBatchOutcome::Retry(ord) => vol_ord = Some(ord),
            }
        }
    }

    fn submit_reads_with_ordinal_once(
        &self,
        vol_id: &str,
        vol_ord: Option<VolumeOrdinal>,
        start_lba: Lba,
        count: u32,
        vol_created_at: u64,
        out_buf: &mut [u8],
    ) -> OnyxResult<ReadBatchOutcome> {
        use std::sync::atomic::Ordering;

        if count == 0 {
            return Ok(ReadBatchOutcome::Complete);
        }
        let bs = BLOCK_SIZE as usize;
        let total = count as usize * bs;
        if out_buf.len() < total {
            return Err(crate::error::OnyxError::Config(format!(
                "submit_reads: out_buf too small (got {} bytes, need {total})",
                out_buf.len()
            )));
        }

        self.metrics
            .zone_read_dispatches
            .fetch_add(count as u64, Ordering::Relaxed);
        self.metrics
            .read_submit_calls
            .fetch_add(1, Ordering::Relaxed);
        let total_start = Instant::now();
        let _scope_total = ReadSubmitTimer::new(&self.metrics.read_submit_total_ns, total_start);

        // Pass 1: buffer lookups. Hits write directly into `out_buf`; misses
        // queue up for a single blockmap batch.
        let pass1_start = Instant::now();
        let mut pending_lbas: Vec<u64> = Vec::new();
        let mut pending_slots: Vec<u32> = Vec::new();
        // Always consult the buffer index. `pending_count` is an aggregate
        // diagnostic counter, not a publication barrier; append publishes the
        // LBA index before bumping it, so using it as a read-path shortcut can
        // make a just-acked buffered write look like a persistent hole.
        let buffer_hits = self
            .buffer_pool
            .lookup_primary_range(vol_id, start_lba, count)?;
        for (i, hit) in buffer_hits.into_iter().enumerate() {
            let lba = start_lba.0 + i as u64;
            let slot = i;
            let dst = &mut out_buf[slot * bs..slot * bs + bs];
            let hit = hit
                .as_ref()
                .is_some_and(|pending| copy_pending_block(pending, lba, vol_created_at, dst));
            if hit {
                self.metrics
                    .read_buffer_hits
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics.buffer_read_ops.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .buffer_read_bytes
                    .fetch_add(bs as u64, Ordering::Relaxed);
            }
            if !hit {
                pending_lbas.push(lba);
                pending_slots.push(i as u32);
            }
        }
        self.metrics
            .read_submit_buffer_lookup_ns
            .fetch_add(elapsed_ns(pass1_start), Ordering::Relaxed);

        if pending_lbas.is_empty() {
            return Ok(ReadBatchOutcome::Complete);
        }

        let hazards = self.allocator.as_ref().map(|allocator| allocator.hazards());
        let lookup_ord = if hazards.is_some() {
            match vol_ord {
                Some(ord) => Some(ord),
                None => Some(self.meta.volume_ordinal_str(vol_id)?),
            }
        } else {
            None
        };

        // Pass 3 input: mapped LBAs still keyed by output slot. We sort after
        // metadata lookup so unordered range scans can still form contiguous
        // raw extents.
        let mut mapped_units: Vec<(usize, BlockmapValue)> = Vec::with_capacity(pending_lbas.len());

        // Pass 2: for real read requests we are looking up one contiguous LBA
        // span. A range scan returns only mapped entries, which avoids paying
        // one point lookup per hole when the volume is still sparse. Small
        // reads stay on multi_get to keep the one-block path minimal.
        let pass2_start = Instant::now();
        let meta_query_ns;
        if count >= RANGE_META_LOOKUP_MIN_LBAS {
            for &slot in &pending_slots {
                let slot = slot as usize;
                out_buf[slot * bs..slot * bs + bs].fill(0);
            }

            let end_lba = Lba(start_lba.0 + count as u64);
            let query_start = Instant::now();
            let mapped = if let Some(ord) = vol_ord {
                self.meta
                    .get_mappings_range_unordered_ord(ord, start_lba, end_lba)?
            } else {
                self.meta
                    .get_mappings_range_unordered_str(vol_id, start_lba, end_lba)?
            };
            meta_query_ns = elapsed_ns(query_start);
            let mut mapped_slot = vec![false; count as usize];
            if pending_lbas.len() == count as usize {
                for (lba, mapping) in mapped {
                    let slot = (lba.0 - start_lba.0) as usize;
                    if slot < count as usize {
                        mapped_slot[slot] = true;
                        if mapping.is_zero() {
                            out_buf[slot * bs..slot * bs + bs].fill(0);
                        } else {
                            mapped_units.push((slot, mapping));
                        }
                    }
                }
            } else {
                let mut pending_slot_by_lba: HashMap<u64, usize> =
                    HashMap::with_capacity(pending_lbas.len());
                for (lba, slot) in pending_lbas.iter().zip(pending_slots.iter().copied()) {
                    pending_slot_by_lba.insert(*lba, slot as usize);
                }
                for (lba, mapping) in mapped {
                    if let Some(slot) = pending_slot_by_lba.get(&lba.0).copied() {
                        mapped_slot[slot] = true;
                        if mapping.is_zero() {
                            out_buf[slot * bs..slot * bs + bs].fill(0);
                        } else {
                            mapped_units.push((slot, mapping));
                        }
                    }
                }
            }
            for (lba, slot) in pending_lbas.iter().zip(pending_slots.iter().copied()) {
                let slot = slot as usize;
                if mapped_slot.get(slot).copied().unwrap_or(false) {
                    continue;
                }
                let dst = &mut out_buf[slot * bs..slot * bs + bs];
                if let Some(pending) = self.buffer_pool.lookup(vol_id, Lba(*lba))? {
                    if copy_pending_block(&pending, *lba, vol_created_at, dst) {
                        self.metrics
                            .read_buffer_hits
                            .fetch_add(1, Ordering::Relaxed);
                        self.metrics.buffer_read_ops.fetch_add(1, Ordering::Relaxed);
                        self.metrics
                            .buffer_read_bytes
                            .fetch_add(bs as u64, Ordering::Relaxed);
                        continue;
                    }
                }
                self.metrics.read_unmapped.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            let query_start = Instant::now();
            let mappings = if let Some(ord) = vol_ord {
                self.meta.multi_get_mappings_raw_ord(ord, &pending_lbas)?
            } else {
                let pending_lbas: Vec<Lba> = pending_lbas.iter().copied().map(Lba).collect();
                self.meta.multi_get_mappings_str(vol_id, &pending_lbas)?
            };
            meta_query_ns = elapsed_ns(query_start);
            for (idx, mapping_opt) in mappings.into_iter().enumerate() {
                let slot = pending_slots[idx] as usize;
                match mapping_opt {
                    None => {
                        let lba = pending_lbas[idx];
                        let dst = &mut out_buf[slot * bs..slot * bs + bs];
                        if let Some(pending) = self.buffer_pool.lookup(vol_id, Lba(lba))? {
                            if copy_pending_block(&pending, lba, vol_created_at, dst) {
                                self.metrics
                                    .read_buffer_hits
                                    .fetch_add(1, Ordering::Relaxed);
                                self.metrics.buffer_read_ops.fetch_add(1, Ordering::Relaxed);
                                self.metrics
                                    .buffer_read_bytes
                                    .fetch_add(bs as u64, Ordering::Relaxed);
                                continue;
                            }
                        }
                        dst.fill(0);
                        self.metrics.read_unmapped.fetch_add(1, Ordering::Relaxed);
                    }
                    Some(mapping) => {
                        if mapping.is_zero() {
                            let dst = &mut out_buf[slot * bs..slot * bs + bs];
                            dst.fill(0);
                        } else {
                            mapped_units.push((slot, mapping));
                        }
                    }
                }
            }
        }
        self.metrics
            .read_submit_meta_get_ns
            .fetch_add(elapsed_ns(pass2_start), Ordering::Relaxed);
        self.metrics
            .read_submit_meta_query_ns
            .fetch_add(meta_query_ns, Ordering::Relaxed);
        self.metrics.read_submit_meta_route_ns.fetch_add(
            elapsed_ns(pass2_start).saturating_sub(meta_query_ns),
            Ordering::Relaxed,
        );

        if mapped_units.is_empty() {
            return Ok(ReadBatchOutcome::Complete);
        }

        let pinned_units = if let (Some(hazards), Some(ord)) = (hazards.as_ref(), lookup_ord) {
            let mut pinned = Vec::with_capacity(mapped_units.len());
            let mut stale_mapping_seen = false;
            for (slot, mapping) in mapped_units {
                let lba = start_lba.0 + slot as u64;
                let guard = hazards.pin_many(mapping.physical_pbas(BLOCK_SIZE));
                if self.meta.get_mapping_ord(ord, Lba(lba))? == Some(mapping) {
                    pinned.push(PinnedMapping {
                        slot,
                        mapping,
                        hazard_guard: Some(guard),
                    });
                } else {
                    stale_mapping_seen = true;
                    break;
                }
            }
            if stale_mapping_seen {
                return Ok(ReadBatchOutcome::Retry(ord));
            }
            pinned
        } else {
            mapped_units
                .into_iter()
                .map(|(slot, mapping)| PinnedMapping {
                    slot,
                    mapping,
                    hazard_guard: None,
                })
                .collect()
        };

        if pinned_units.is_empty() {
            return Ok(ReadBatchOutcome::Complete);
        }

        // Pass 3: split mapped LBAs into two shapes:
        // - raw single-block mappings with consecutive output slots and PBAs
        //   become one extent read (4K/8K/16K/32K as one SQE);
        // - compressed/packed units stay grouped by unit identity.
        let mut groups: HashMap<ReadUnitKey, ReadUnitGroup> = HashMap::new();
        let mut raw_extents: Vec<RawExtentGroup> = Vec::new();
        let mut pinned_units = pinned_units;
        pinned_units.sort_unstable_by_key(|unit| unit.slot);
        let mut current_raw: Option<RawExtentGroup> = None;
        for pinned in pinned_units {
            let slot = pinned.slot;
            let mapping = pinned.mapping;
            if is_single_raw_block(&mapping) {
                if let Some(raw) = current_raw.as_mut() {
                    if raw.can_extend(slot, &mapping) {
                        raw.mappings.push(mapping);
                        if let Some(guard) = pinned.hazard_guard {
                            raw._hazard_guards.push(guard);
                        }
                        continue;
                    }
                }
                if let Some(raw) = current_raw.take() {
                    raw_extents.push(raw);
                }
                let mut raw = RawExtentGroup::new(slot, mapping);
                if let Some(guard) = pinned.hazard_guard {
                    raw._hazard_guards.push(guard);
                }
                current_raw = Some(raw);
            } else {
                if let Some(raw) = current_raw.take() {
                    raw_extents.push(raw);
                }
                push_read_unit_group(&mut groups, slot, mapping, pinned.hazard_guard);
            }
        }
        if let Some(raw) = current_raw {
            raw_extents.push(raw);
        }

        // Pass 4: fan out unit reads. Send all first, then drain — the
        // ReadPool worker coalesces same-worker requests into one io_uring
        // submit, and other workers run in parallel.
        let pass4_start = Instant::now();
        let pass4_result = if let Some(pool) = self.read_pool.as_deref() {
            let mut raw_receivers = Vec::with_capacity(raw_extents.len());
            for raw in raw_extents.into_iter() {
                let rx = pool.submit_raw_extent_read_async(raw.mappings)?;
                raw_receivers.push((rx, raw.start_slot, raw._hazard_guards));
            }
            let mut receivers = Vec::with_capacity(groups.len());
            let mut units: Vec<ReadUnitGroup> = Vec::with_capacity(groups.len());
            for (_, group) in groups.into_iter() {
                let rx = pool.submit_unit_read_async(group.mapping)?;
                receivers.push(rx);
                units.push(group);
            }
            let mut result: OnyxResult<()> = Ok(());
            for (rx, start_slot, _hazard_guards) in raw_receivers {
                match rx.recv().map_err(|_| {
                    crate::error::OnyxError::Io(std::io::Error::other("read-pool reply dropped"))
                }) {
                    Ok(Ok(payload)) => {
                        let start = start_slot * bs;
                        let end = start + payload.len();
                        if end > out_buf.len() {
                            result = Err(crate::error::OnyxError::Compress(format!(
                                "raw extent output out of bounds: {start}..{end} > {}",
                                out_buf.len()
                            )));
                            break;
                        }
                        out_buf[start..end].copy_from_slice(&payload);
                    }
                    Ok(Err(e)) | Err(e) => {
                        result = Err(e);
                        break;
                    }
                }
            }
            for (rx, group) in receivers.into_iter().zip(units.into_iter()) {
                if result.is_err() {
                    break;
                }
                match rx.recv().map_err(|_| {
                    crate::error::OnyxError::Io(std::io::Error::other("read-pool reply dropped"))
                }) {
                    Ok(Ok(payload)) => {
                        if let Err(e) = self.fan_out_unit(&payload, &group.members, out_buf) {
                            result = Err(e);
                            break;
                        }
                    }
                    Ok(Err(e)) | Err(e) => {
                        result = Err(e);
                        break;
                    }
                }
            }
            result
        } else {
            // Inline fallback: no pool configured. Still benefits from unit
            // coalescing — one IoEngine read + decompress per unit.
            let mut result: OnyxResult<()> = Ok(());
            for raw in raw_extents.into_iter() {
                let read_size = raw.mappings.len() * bs;
                let raw_payload = match self.io_engine.read_blocks(raw.mappings[0].pba, read_size) {
                    Ok(r) => r,
                    Err(e) => {
                        result = Err(e);
                        break;
                    }
                };
                for (idx, mapping) in raw.mappings.iter().enumerate() {
                    let off = idx * bs;
                    let block = &raw_payload[off..off + bs];
                    let actual_crc = crc32fast::hash(block);
                    if actual_crc != mapping.crc32 {
                        self.metrics.read_crc_errors.fetch_add(1, Ordering::Relaxed);
                        result = Err(crate::error::OnyxError::CrcMismatch {
                            expected: mapping.crc32,
                            actual: actual_crc,
                        });
                        break;
                    }
                }
                if result.is_err() {
                    break;
                }
                let start = raw.start_slot * bs;
                let end = start + raw_payload.len();
                out_buf[start..end].copy_from_slice(&raw_payload);
                self.metrics
                    .read_lv3_hits
                    .fetch_add(raw.mappings.len() as u64, Ordering::Relaxed);
                self.metrics
                    .lv3_read_decompressed_bytes
                    .fetch_add(read_size as u64, Ordering::Relaxed);
            }
            for (_, group) in groups.into_iter() {
                if result.is_err() {
                    break;
                }
                let read_size = group.mapping.compressed_read_size(bs);
                let raw = match self.io_engine.read_blocks(group.mapping.pba, read_size) {
                    Ok(r) => r,
                    Err(e) => {
                        result = Err(e);
                        break;
                    }
                };
                let payload = match read::decode_unit(&raw, &group.mapping, &self.metrics) {
                    Ok(p) => p,
                    Err(e) => {
                        result = Err(e);
                        break;
                    }
                };
                let payload_buf = payload.into_owned();
                if let Err(e) = self.fan_out_unit(&payload_buf, &group.members, out_buf) {
                    result = Err(e);
                    break;
                }
            }
            result
        };
        self.metrics
            .record_read_submit_unit_io_ns(elapsed_ns(pass4_start));
        pass4_result.map(|_| ReadBatchOutcome::Complete)
    }

    /// Copy each `(slot, offset_in_unit)` member's 4 KB from the decoded unit
    /// payload into the caller's output buffer, bumping per-LBA metrics.
    fn fan_out_unit(
        &self,
        payload: &[u8],
        members: &[(usize, u16)],
        out_buf: &mut [u8],
    ) -> OnyxResult<()> {
        use std::sync::atomic::Ordering;
        let bs = BLOCK_SIZE as usize;
        for (slot, offset_in_unit) in members.iter().copied() {
            let off = offset_in_unit as usize * bs;
            let end = off + bs;
            if end > payload.len() {
                return Err(crate::error::OnyxError::Compress(format!(
                    "unit payload too small for offset_in_unit={offset_in_unit}: need {off}..{end}, have {}",
                    payload.len()
                )));
            }
            out_buf[slot * bs..slot * bs + bs].copy_from_slice(&payload[off..end]);
            self.metrics.read_lv3_hits.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .lv3_read_decompressed_bytes
                .fetch_add(bs as u64, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Submit a DISCARD (TRIM) request for a range of LBAs.
    ///
    /// Invalidates buffer index entries, deletes blockmap mappings,
    /// decrements refcounts, and frees PBAs with zero refcount.
    /// No LV3 IO is performed — this is purely a metadata operation.
    pub fn submit_discard(&self, vol_id: &str, start_lba: Lba, lba_count: u32) -> OnyxResult<()> {
        if lba_count == 0 {
            return Ok(());
        }

        // Step 1: invalidate buffer index so reads see unmapped immediately
        self.buffer_pool
            .invalidate_lba_range(vol_id, start_lba, lba_count);

        // Step 2: delete blockmap entries + decrement refcounts atomically
        let vol_id_obj = VolumeId(vol_id.to_string());
        let end_lba = Lba(start_lba.0 + lba_count as u64);
        let cleanups = self
            .meta
            .delete_blockmap_range(&vol_id_obj, start_lba, end_lba)?;

        // Step 3: return freed PBAs to allocator
        if let Some(allocator) = &self.allocator {
            BufferFlusher::cleanup_dead_pbas_batch(
                allocator,
                &self.candidate,
                &cleanups,
                "discard",
            );
            let mut blocks_freed = 0u64;
            for cleanup in &cleanups {
                if cleanup.pba_freed {
                    blocks_freed += cleanup.blocks as u64;
                }
            }
            if blocks_freed > 0 {
                self.metrics
                    .discard_blocks_freed
                    .fetch_add(blocks_freed, std::sync::atomic::Ordering::Relaxed);
            }
        }

        Ok(())
    }

    /// Graceful shutdown — currently a no-op since reads/writes run inline.
    /// Kept on the API surface for callers (engine.rs) that expect to be able
    /// to call it.
    pub fn shutdown(&mut self) -> OnyxResult<()> {
        tracing::info!("zone manager stopped");
        Ok(())
    }

    pub fn zone_count(&self) -> u32 {
        self.zone_count
    }
}

impl Drop for ZoneManager {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
