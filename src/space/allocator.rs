use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::error::{OnyxError, OnyxResult};
use crate::meta::store::MetaStore;
use crate::space::extent::Extent;
use crate::types::{Pba, BLOCK_SIZE};

pub struct SpaceAllocator {
    total_blocks: u64,
    free_extents: Mutex<BTreeSet<Extent>>,
    allocated_blocks: AtomicU64,
    free_blocks: AtomicU64,
}

impl SpaceAllocator {
    /// Create a new allocator for a device of the given size.
    /// Initially all blocks are free.
    pub fn new(device_size_bytes: u64) -> Self {
        let total_blocks = device_size_bytes / BLOCK_SIZE as u64;
        let mut free_extents = BTreeSet::new();
        if total_blocks > 0 {
            free_extents.insert(Extent::new(Pba(0), total_blocks as u32));
        }
        Self {
            total_blocks,
            free_extents: Mutex::new(free_extents),
            allocated_blocks: AtomicU64::new(0),
            free_blocks: AtomicU64::new(total_blocks),
        }
    }

    /// Rebuild the free list from MetaStore metadata.
    /// Blockmap is the source of truth so multi-block compression units reserve
    /// all occupied PBAs, not just the starting block.
    pub fn rebuild_from_metadata(&self, meta: &MetaStore) -> OnyxResult<()> {
        // Collect all allocated PBAs into a sorted vec
        let mut allocated: Vec<u64> = meta
            .iter_allocated_blocks()?
            .into_iter()
            .map(|pba| pba.0)
            .collect();
        allocated.sort_unstable();

        // Build free extents from gaps
        let mut free = BTreeSet::new();
        let mut pos: u64 = 0;

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

        let alloc_count = allocated.len() as u64;
        let free_count = self.total_blocks - alloc_count;

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
        let mut free = self.free_extents.lock().unwrap();

        // Take from the first (lowest PBA) free extent
        let extent = free
            .iter()
            .next()
            .copied()
            .ok_or(OnyxError::SpaceExhausted)?;
        free.remove(&extent);

        let pba = extent.start;
        if extent.count > 1 {
            free.insert(Extent::new(Pba(pba.0 + 1), extent.count - 1));
        }

        self.allocated_blocks.fetch_add(1, Ordering::Relaxed);
        self.free_blocks.fetch_sub(1, Ordering::Relaxed);

        Ok(pba)
    }

    /// Free a single block.
    ///
    /// Returns error if the PBA is out of bounds, already free, or would underflow counters.
    pub fn free_one(&self, pba: Pba) -> OnyxResult<()> {
        if pba.0 >= self.total_blocks {
            return Err(OnyxError::Config(format!(
                "free_one: PBA {} out of bounds (total {})",
                pba.0, self.total_blocks
            )));
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

    /// Allocate up to `count` contiguous blocks. Returns the extent actually allocated
    /// (may be smaller than requested if no large enough contiguous region exists).
    pub fn allocate_extent(&self, count: u32) -> OnyxResult<Extent> {
        if count == 0 {
            return Err(OnyxError::Config("cannot allocate 0 blocks".into()));
        }

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
        match largest {
            Some(extent) => {
                free.remove(&extent);
                self.allocated_blocks
                    .fetch_add(extent.count as u64, Ordering::Relaxed);
                self.free_blocks
                    .fetch_sub(extent.count as u64, Ordering::Relaxed);
                Ok(extent)
            }
            None => Err(OnyxError::SpaceExhausted),
        }
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
