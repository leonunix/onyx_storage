use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::error::{OnyxError, OnyxResult};
use crate::meta::store::MetaStore;
use crate::space::extent::Extent;
use crate::types::{Pba, BLOCK_SIZE, RESERVED_BLOCKS};

/// Number of blocks to refill a lane cache from the global free list at once.
const LANE_CACHE_REFILL_SIZE: u32 = 256;

pub struct SpaceAllocator {
    total_blocks: u64,
    free_extents: Mutex<BTreeSet<Extent>>,
    allocated_blocks: AtomicU64,
    free_blocks: AtomicU64,
    /// Per-lane single-block caches. Each flush lane pops from its own cache
    /// to avoid contending on `free_extents`. Refilled in bulk from global.
    lane_caches: Vec<Mutex<Vec<Pba>>>,
}

impl SpaceAllocator {
    /// Create a new allocator for a device of the given size.
    /// Blocks 0..RESERVED_BLOCKS are reserved for superblock/heartbeat/HA lock.
    /// Allocatable space starts at PBA RESERVED_BLOCKS.
    pub fn new(device_size_bytes: u64, num_lanes: usize) -> Self {
        let total_blocks = device_size_bytes / BLOCK_SIZE as u64;
        let usable_blocks = total_blocks.saturating_sub(RESERVED_BLOCKS);
        let mut free_extents = BTreeSet::new();
        if usable_blocks > 0 {
            free_extents.insert(Extent::new(
                Pba(RESERVED_BLOCKS),
                usable_blocks.min(u32::MAX as u64) as u32,
            ));
        }
        let lane_caches = (0..num_lanes).map(|_| Mutex::new(Vec::new())).collect();
        Self {
            total_blocks,
            free_extents: Mutex::new(free_extents),
            allocated_blocks: AtomicU64::new(0),
            free_blocks: AtomicU64::new(usable_blocks),
            lane_caches,
        }
    }

    /// Rebuild the free list from MetaStore metadata.
    /// Blockmap is the source of truth so multi-block compression units reserve
    /// all occupied PBAs, not just the starting block.
    /// PBAs below RESERVED_BLOCKS are excluded (reserved for superblock/HA).
    pub fn rebuild_from_metadata(&self, meta: &MetaStore) -> OnyxResult<()> {
        // Collect all allocated PBAs into a sorted vec, filtering out reserved region
        let mut allocated: Vec<u64> = meta
            .iter_allocated_blocks()?
            .into_iter()
            .map(|pba| pba.0)
            .filter(|&pba| pba >= RESERVED_BLOCKS)
            .collect();
        allocated.sort_unstable();

        // Build free extents from gaps (starting at RESERVED_BLOCKS)
        let mut free = BTreeSet::new();
        let mut pos: u64 = RESERVED_BLOCKS;

        for &alloc_pba in &allocated {
            if alloc_pba > pos {
                let gap = alloc_pba - pos;
                // Split into u32-sized extents if needed
                let mut start = pos;
                let mut remaining = gap;
                while remaining > 0 {
                    let count = remaining.min(u32::MAX as u64) as u32;
                    free.insert(Extent::new(Pba(start), count));
                    start += count as u64;
                    remaining -= count as u64;
                }
            }
            pos = alloc_pba + 1;
        }

        // Trailing free space
        if pos < self.total_blocks {
            let gap = self.total_blocks - pos;
            let mut start = pos;
            let mut remaining = gap;
            while remaining > 0 {
                let count = remaining.min(u32::MAX as u64) as u32;
                free.insert(Extent::new(Pba(start), count));
                start += count as u64;
                remaining -= count as u64;
            }
        }

        let usable_blocks = self.total_blocks.saturating_sub(RESERVED_BLOCKS);
        let alloc_count = allocated.len() as u64;
        let free_count = usable_blocks - alloc_count;

        *self.free_extents.lock().unwrap() = free;
        self.allocated_blocks.store(alloc_count, Ordering::Relaxed);
        self.free_blocks.store(free_count, Ordering::Relaxed);

        tracing::info!(
            total = self.total_blocks,
            allocated = alloc_count,
            free = free_count,
            extents = self.free_extents.lock().unwrap().len(),
            "space allocator rebuilt from metadata"
        );

        Ok(())
    }

    /// Allocate a single block. Returns PBA.
    pub fn allocate_one(&self) -> OnyxResult<Pba> {
        {
            let mut free = self.free_extents.lock().unwrap();
            if let Some(pba) = Self::alloc_one_from_set(&mut free) {
                self.allocated_blocks.fetch_add(1, Ordering::Relaxed);
                self.free_blocks.fetch_sub(1, Ordering::Relaxed);
                return Ok(pba);
            }
        }
        // Global pool empty — drain lane caches and retry
        if !self.lane_caches.is_empty() {
            self.drain_lane_caches();
            let mut free = self.free_extents.lock().unwrap();
            if let Some(pba) = Self::alloc_one_from_set(&mut free) {
                self.allocated_blocks.fetch_add(1, Ordering::Relaxed);
                self.free_blocks.fetch_sub(1, Ordering::Relaxed);
                return Ok(pba);
            }
        }
        Err(OnyxError::SpaceExhausted)
    }

    /// Helper: take one block from the free set (no counter update).
    fn alloc_one_from_set(free: &mut BTreeSet<Extent>) -> Option<Pba> {
        let extent = free.iter().next().copied()?;
        free.remove(&extent);
        let pba = extent.start;
        if extent.count > 1 {
            free.insert(Extent::new(Pba(pba.0 + 1), extent.count - 1));
        }
        Some(pba)
    }

    /// Allocate a single block using the per-lane cache to avoid global lock contention.
    /// Falls back to global allocation with bulk refill when the cache is empty.
    pub fn allocate_one_for_lane(&self, lane: usize) -> OnyxResult<Pba> {
        if lane >= self.lane_caches.len() {
            return self.allocate_one();
        }
        // Fast path: pop from lane cache (no global lock)
        {
            let mut cache = self.lane_caches[lane].lock().unwrap();
            if let Some(pba) = cache.pop() {
                // Count as allocated only when given to caller
                self.allocated_blocks.fetch_add(1, Ordering::Relaxed);
                self.free_blocks.fetch_sub(1, Ordering::Relaxed);
                return Ok(pba);
            }
        }
        // Slow path: refill from global (blocks stay logically "free" in the cache)
        let (first_pba, refill) = {
            let mut free = self.free_extents.lock().unwrap();
            let extent = free
                .iter()
                .next()
                .copied()
                .ok_or(OnyxError::SpaceExhausted)?;
            free.remove(&extent);
            let take = extent.count.min(LANE_CACHE_REFILL_SIZE);
            if extent.count > take {
                free.insert(Extent::new(
                    Pba(extent.start.0 + take as u64),
                    extent.count - take,
                ));
            }
            (extent.start, take)
        };
        // First block goes to caller (counted as allocated), rest into cache
        self.allocated_blocks.fetch_add(1, Ordering::Relaxed);
        self.free_blocks.fetch_sub(1, Ordering::Relaxed);
        if refill > 1 {
            let mut cache = self.lane_caches[lane].lock().unwrap();
            for i in 1..refill {
                cache.push(Pba(first_pba.0 + i as u64));
            }
        }
        Ok(first_pba)
    }

    /// Return all cached blocks from lane caches to the global free list.
    /// Called during shutdown to prevent block leaks.
    pub fn drain_lane_caches(&self) {
        for cache_mutex in &self.lane_caches {
            let mut cache = cache_mutex.lock().unwrap();
            if cache.is_empty() {
                continue;
            }
            let mut free = self.free_extents.lock().unwrap();
            for pba in cache.drain(..) {
                self.coalesce_and_insert(&mut free, Extent::single(pba));
            }
            // No counter adjustment needed: cached blocks were never counted as allocated
        }
    }

    /// Free a single block.
    ///
    /// Returns error if the PBA is out of bounds, already free (in the global
    /// free list **or** a lane cache), or would underflow counters.
    pub fn free_one(&self, pba: Pba) -> OnyxResult<()> {
        if pba.0 >= self.total_blocks {
            return Err(OnyxError::Config(format!(
                "free_one: PBA {} out of bounds (total {})",
                pba.0, self.total_blocks
            )));
        }

        // Check lane caches first (no global lock needed)
        for (lane_idx, cache_mutex) in self.lane_caches.iter().enumerate() {
            let cache = cache_mutex.lock().unwrap();
            if cache.contains(&pba) {
                return Err(OnyxError::Config(format!(
                    "free_one: PBA {} is already free (in lane cache {})",
                    pba.0, lane_idx
                )));
            }
        }

        let mut free = self.free_extents.lock().unwrap();

        // Check not already free (contained in any existing free extent)
        for e in free.iter() {
            if e.contains(pba) {
                return Err(OnyxError::Config(format!(
                    "free_one: PBA {} is already free (in extent {:?})",
                    pba.0, e
                )));
            }
        }

        let current_alloc = self.allocated_blocks.load(Ordering::Relaxed);
        if current_alloc == 0 {
            return Err(OnyxError::Config(
                "free_one: allocated_blocks would underflow".into(),
            ));
        }

        self.coalesce_and_insert(&mut free, Extent::single(pba));
        self.allocated_blocks.fetch_sub(1, Ordering::Relaxed);
        self.free_blocks.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Return true if the single block is free — either in the global free list
    /// or sitting in a lane cache (allocated from the free list but not yet
    /// handed out to a caller).
    pub fn is_free(&self, pba: Pba) -> bool {
        let free = self.free_extents.lock().unwrap();
        if free.iter().any(|extent| extent.contains(pba)) {
            return true;
        }
        drop(free);
        for cache_mutex in &self.lane_caches {
            let cache = cache_mutex.lock().unwrap();
            if cache.contains(&pba) {
                return true;
            }
        }
        false
    }

    /// Allocate up to `count` contiguous blocks. Returns the extent actually allocated
    /// (may be smaller than requested if no large enough contiguous region exists).
    pub fn allocate_extent(&self, count: u32) -> OnyxResult<Extent> {
        if count == 0 {
            return Err(OnyxError::Config("cannot allocate 0 blocks".into()));
        }

        // Try allocation from global free list. If insufficient, drain lane caches and retry.
        for attempt in 0..2 {
            let mut free = self.free_extents.lock().unwrap();

            // Find first extent that's large enough
            let exact = free.iter().find(|e| e.count >= count).copied();

            if let Some(extent) = exact {
                free.remove(&extent);
                let result = Extent::new(extent.start, count);
                if extent.count > count {
                    free.insert(Extent::new(
                        Pba(extent.start.0 + count as u64),
                        extent.count - count,
                    ));
                }
                self.allocated_blocks
                    .fetch_add(count as u64, Ordering::Relaxed);
                self.free_blocks.fetch_sub(count as u64, Ordering::Relaxed);
                return Ok(result);
            }

            // No contiguous extent large enough — return the largest available
            let largest = free.iter().max_by_key(|e| e.count).copied();
            if let Some(extent) = largest {
                free.remove(&extent);
                self.allocated_blocks
                    .fetch_add(extent.count as u64, Ordering::Relaxed);
                self.free_blocks
                    .fetch_sub(extent.count as u64, Ordering::Relaxed);
                return Ok(extent);
            }

            // No free extents at all — drain lane caches and retry once
            drop(free);
            if attempt == 0 && !self.lane_caches.is_empty() {
                self.drain_lane_caches();
                continue;
            }
            break;
        }
        Err(OnyxError::SpaceExhausted)
    }

    /// Free an extent.
    ///
    /// Returns error if the extent is out of bounds, overlaps existing free space,
    /// or would underflow counters.
    pub fn free_extent(&self, extent: Extent) -> OnyxResult<()> {
        if extent.count == 0 {
            return Err(OnyxError::Config(
                "free_extent: cannot free 0 blocks".into(),
            ));
        }
        if extent.end_pba().0 > self.total_blocks {
            return Err(OnyxError::Config(format!(
                "free_extent: extent {:?} exceeds total blocks {}",
                extent, self.total_blocks
            )));
        }

        let mut free = self.free_extents.lock().unwrap();

        // Check no overlap with existing free extents
        for e in free.iter() {
            if extent.start.0 < e.end_pba().0 && extent.end_pba().0 > e.start.0 {
                return Err(OnyxError::Config(format!(
                    "free_extent: extent {:?} overlaps free extent {:?}",
                    extent, e
                )));
            }
        }

        let current_alloc = self.allocated_blocks.load(Ordering::Relaxed);
        if (extent.count as u64) > current_alloc {
            return Err(OnyxError::Config(format!(
                "free_extent: freeing {} blocks but only {} allocated",
                extent.count, current_alloc
            )));
        }

        self.coalesce_and_insert(&mut free, extent);
        self.allocated_blocks
            .fetch_sub(extent.count as u64, Ordering::Relaxed);
        self.free_blocks
            .fetch_add(extent.count as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Return true if the whole extent is already covered by a free extent
    /// or all its blocks are sitting in lane caches.
    pub fn is_extent_free(&self, extent: Extent) -> bool {
        let free = self.free_extents.lock().unwrap();
        if free.iter().any(|existing| {
            extent.start.0 >= existing.start.0 && extent.end_pba().0 <= existing.end_pba().0
        }) {
            return true;
        }
        drop(free);
        // Fallback: check if every block in the extent is in a lane cache.
        (0..extent.count).all(|i| {
            let pba = Pba(extent.start.0 + i as u64);
            self.lane_caches
                .iter()
                .any(|c| c.lock().unwrap().contains(&pba))
        })
    }

    pub fn free_block_count(&self) -> u64 {
        self.free_blocks.load(Ordering::Relaxed)
    }

    pub fn allocated_block_count(&self) -> u64 {
        self.allocated_blocks.load(Ordering::Relaxed)
    }

    pub fn total_block_count(&self) -> u64 {
        self.total_blocks
    }

    /// Insert an extent and merge with adjacent free extents.
    fn coalesce_and_insert(&self, free: &mut BTreeSet<Extent>, new: Extent) {
        let mut merged_start = new.start.0;
        let mut merged_end = new.end_pba().0;
        let mut to_remove = Vec::new();

        // Check for extent immediately before
        for e in free.iter() {
            if e.end_pba().0 == merged_start {
                merged_start = e.start.0;
                to_remove.push(*e);
            } else if e.start.0 == merged_end {
                merged_end = e.end_pba().0;
                to_remove.push(*e);
            }
        }

        for e in &to_remove {
            free.remove(e);
        }

        let count = (merged_end - merged_start) as u32;
        free.insert(Extent::new(Pba(merged_start), count));
    }
}
