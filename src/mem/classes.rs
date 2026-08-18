//! Size classes for [`super::SlabArena`].
//!
//! The LV3 writer asks for buffers in whole 4 KiB blocks and the two shapes it
//! asks for are both bounded:
//!
//! - a **run** buffer is `extent.count` blocks, and every allocation path caps
//!   the extent at one stripe (`requested = remaining_blocks.min(stripe.max(1))`
//!   in `allocate_unaligned_write_runs`) — 6 blocks / 24 KiB on the box's
//!   RAID6 6+2 geometry;
//! - a **unit** buffer is `alloc_blocks[i]` blocks, bounded by
//!   `flush.coalesce_max_raw_bytes` (128 KiB default = 32 blocks).
//!
//! So the interesting range is small and dominated by exactly one width. The
//! table is therefore **exact for 1..=16 blocks** (no internal waste at all in
//! the sizes that carry ~100 % of the traffic) and geometric (ratio 1.5) above
//! that, which bounds waste at 50 % for the rare large unit while keeping the
//! class count — and therefore the arena's fixed footprint — small.
//!
//! Nothing here is derived from the RAID geometry on purpose: the table only has
//! to *contain* the geometry, and a table that changes shape when a config knob
//! moves would make two arms incomparable.

/// Largest block count served exactly (one class per block count).
const EXACT_BLOCKS: u32 = 16;

/// Maps a block count to a class index, and a class index to its slot width.
#[derive(Debug, Clone)]
pub struct ClassTable {
    /// Slot width in blocks, ascending. `slot_blocks[c]` is class `c`'s width.
    slot_blocks: Box<[u32]>,
    /// `lookup[blocks - 1]` = class index serving `blocks`. Length = max_blocks.
    lookup: Box<[u16]>,
}

impl ClassTable {
    /// Build a table covering `1..=max_blocks`. `max_blocks` is clamped to at
    /// least [`EXACT_BLOCKS`] so the exact range always exists.
    pub fn new(max_blocks: u32) -> Self {
        let max_blocks = max_blocks.max(EXACT_BLOCKS);
        let mut slot_blocks: Vec<u32> = (1..=EXACT_BLOCKS).collect();

        // Geometric with a 1.5x midpoint between powers of two: 16 -> 24 -> 32
        // -> 48 -> 64 -> 96 -> ... Waste for a request landing just above a
        // class boundary is therefore at most 1/3 of the slot.
        let mut width = EXACT_BLOCKS;
        while width < max_blocks {
            let next = if width.is_power_of_two() {
                width + width / 2
            } else {
                // width is a 1.5x midpoint; the next power of two is width*4/3.
                width / 3 * 4
            };
            let next = next.min(max_blocks);
            slot_blocks.push(next);
            width = next;
        }

        let mut lookup = vec![0u16; max_blocks as usize];
        let mut class = 0usize;
        for blocks in 1..=max_blocks {
            while slot_blocks[class] < blocks {
                class += 1;
            }
            lookup[(blocks - 1) as usize] = class as u16;
        }

        Self {
            slot_blocks: slot_blocks.into_boxed_slice(),
            lookup: lookup.into_boxed_slice(),
        }
    }

    /// Largest request this table can serve, in blocks.
    pub fn max_blocks(&self) -> u32 {
        self.lookup.len() as u32
    }

    pub fn class_count(&self) -> usize {
        self.slot_blocks.len()
    }

    /// Class serving `blocks`, or `None` when the request is out of range (the
    /// caller then falls back to the heap).
    pub fn class_of(&self, blocks: u32) -> Option<u16> {
        if blocks == 0 || blocks > self.max_blocks() {
            return None;
        }
        Some(self.lookup[(blocks - 1) as usize])
    }

    /// Slot width of `class`, in blocks.
    pub fn slot_blocks(&self, class: u16) -> u32 {
        self.slot_blocks[class as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_range_has_no_internal_waste() {
        let table = ClassTable::new(64);
        for blocks in 1..=EXACT_BLOCKS {
            let class = table.class_of(blocks).expect("in range");
            assert_eq!(
                table.slot_blocks(class),
                blocks,
                "blocks {} must be served exactly",
                blocks
            );
        }
    }

    #[test]
    fn classes_are_ascending_and_cover_every_request() {
        let table = ClassTable::new(256);
        for blocks in 1..=table.max_blocks() {
            let class = table.class_of(blocks).expect("in range");
            assert!(
                table.slot_blocks(class) >= blocks,
                "class {} width {} cannot hold {} blocks",
                class,
                table.slot_blocks(class),
                blocks
            );
            // Never over-serve by more than half: that is the memory cost of
            // not having an exact class, and it is what the 1.5x ratio buys.
            assert!(
                table.slot_blocks(class) * 2 <= blocks * 3 || blocks <= EXACT_BLOCKS,
                "blocks {} over-served by class width {}",
                blocks,
                table.slot_blocks(class)
            );
        }
        for c in 1..table.class_count() {
            assert!(table.slot_blocks(c as u16 - 1) < table.slot_blocks(c as u16));
        }
    }

    #[test]
    fn out_of_range_requests_have_no_class() {
        let table = ClassTable::new(32);
        assert_eq!(table.max_blocks(), 32);
        assert!(table.class_of(0).is_none());
        assert!(table.class_of(33).is_none());
        assert!(table.class_of(32).is_some());
    }

    #[test]
    fn small_max_still_keeps_the_exact_range() {
        let table = ClassTable::new(1);
        assert_eq!(table.max_blocks(), EXACT_BLOCKS);
        assert_eq!(table.class_count(), EXACT_BLOCKS as usize);
    }
}
