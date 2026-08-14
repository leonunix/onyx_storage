use super::*;

/// Address-region layout snapshot. Immutable for the duration of one allocator
/// operation: the layout is only ever rewritten by `set_geometry`, which holds
/// every region lock while it re-routes the whole free set.
///
/// Region `i` owns `[base + i*blocks, base + (i+1)*blocks)`, with two
/// deliberate asymmetries:
///   - region 0 also owns everything BELOW `base` (the reserved prefix plus the
///     ≤ stripe-1 blocks between `RESERVED_BLOCKS` and the first aligned PBA),
///   - the LAST region owns everything above its start, so an online
///     `grow_capacity` needs no re-layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RegionLayout {
    pub(super) base: u64,
    pub(super) blocks: u64,
    pub(super) count: usize,
}

impl RegionLayout {
    /// Single-region (sharding off) — byte-for-byte the pre-region behaviour.
    pub(super) fn single() -> Self {
        Self {
            base: 0,
            blocks: 0,
            count: 1,
        }
    }

    pub(super) fn sharded(&self) -> bool {
        self.count > 1 && self.blocks > 0
    }

    /// Owning region of a PBA. Total over all u64 (clamped both ends), so no
    /// caller has to bounds-check before routing.
    pub(super) fn of(&self, pba: u64) -> usize {
        if !self.sharded() {
            return 0;
        }
        ((pba.saturating_sub(self.base)) / self.blocks).min(self.count as u64 - 1) as usize
    }

    pub(super) fn start(&self, idx: usize) -> u64 {
        if idx == 0 || !self.sharded() {
            0
        } else {
            self.base + idx as u64 * self.blocks
        }
    }

    pub(super) fn end(&self, idx: usize) -> u64 {
        if !self.sharded() || idx + 1 >= self.count {
            u64::MAX
        } else {
            self.base + (idx + 1) as u64 * self.blocks
        }
    }

    /// Inclusive region index range spanned by `extent`. Zero-count extents
    /// (rejected downstream by `validate_extent_shape`) route to their start's
    /// region so the caller can still take a lock and report the failure.
    pub(super) fn span(&self, extent: Extent) -> (usize, usize) {
        let lo = self.of(extent.start.0);
        let last = extent.end_pba().0.max(extent.start.0 + 1) - 1;
        (lo, self.of(last).max(lo))
    }

    /// `extent` clipped to region `idx`, or `None` if it does not reach into it.
    pub(super) fn clip(&self, idx: usize, extent: Extent) -> Option<Extent> {
        let s = extent.start.0.max(self.start(idx));
        let e = extent.end_pba().0.min(self.end(idx));
        (s < e).then(|| Extent::new(Pba(s), (e - s) as u32))
    }

    /// Blocks per region for a device of `usable_blocks`, aiming at `regions`
    /// shards. Returns `None` when the device is too small to shard usefully.
    /// The result is a multiple of `stripe` so no region boundary can split a
    /// stripe window — without that, every boundary would strand up to
    /// `stripe - 1` blocks in the general pool instead of the reserve.
    pub(super) fn plan(usable_blocks: u64, regions: usize, stripe: u32) -> Option<(u64, usize)> {
        if regions <= 1 || usable_blocks == 0 {
            return None;
        }
        let stripe = u64::from(stripe.max(1));
        let want = usable_blocks
            .div_ceil(regions as u64)
            .max(MIN_REGION_BLOCKS);
        let blocks = want.div_ceil(stripe) * stripe;
        let count = usable_blocks.div_ceil(blocks) as usize;
        (count > 1).then_some((blocks, count))
    }
}
