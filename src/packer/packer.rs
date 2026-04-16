use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::buffer::pipeline::CompressedUnit;
use crate::error::OnyxResult;
use crate::space::allocator::SpaceAllocator;
use crate::types::{Pba, BLOCK_SIZE};

/// A fragment placed into a packed slot.
pub struct SlotFragment {
    pub unit: CompressedUnit,
    pub slot_offset: u16,
}

/// A sealed packed slot ready to be written to LV3.
pub struct SealedSlot {
    pub pba: Pba,
    pub data: Vec<u8>,
    pub fragments: Vec<SlotFragment>,
}

/// Result of attempting to pack a compressed unit.
pub enum PackResult {
    /// Unit is too large for packing (>= BLOCK_SIZE); write it directly.
    Passthrough(CompressedUnit),
    /// Fragment was buffered into the open slot; nothing to write yet.
    Buffered,
    /// The open slot was sealed (full) and a new slot opened for the incoming fragment.
    /// The sealed slot must be written to LV3.
    SealedSlot(SealedSlot),
    /// The open slot was sealed, but allocating a new slot for the incoming
    /// fragment failed. The sealed slot must still be written. The unit that
    /// could not be packed is returned for the caller to handle as Passthrough.
    SealedSlotAndPassthrough(SealedSlot, CompressedUnit),
}

struct OpenSlot {
    pba: Pba,
    data: Vec<u8>,
    used: u16,
    fragments: Vec<SlotFragment>,
    // Keep the first-fragment timestamp stable so sustained traffic cannot
    // perpetually reset the open-slot age and strand buffered seqs forever.
    opened_at: Instant,
}

/// Bin-packing of small compressed units into shared 4KB physical slots.
///
/// Owned by the flusher writer thread (single-threaded, no synchronization needed).
pub struct Packer {
    allocator: Arc<SpaceAllocator>,
    lane_id: usize,
    open_slot: Option<OpenSlot>,
}

impl Packer {
    pub fn new(allocator: Arc<SpaceAllocator>) -> Self {
        Self {
            allocator,
            lane_id: 0,
            open_slot: None,
        }
    }

    pub fn new_with_lane(allocator: Arc<SpaceAllocator>, lane_id: usize) -> Self {
        Self {
            allocator,
            lane_id,
            open_slot: None,
        }
    }

    /// Try to pack a compressed unit into the current open slot.
    pub fn pack_or_passthrough(&mut self, unit: CompressedUnit) -> OnyxResult<PackResult> {
        let frag_size = unit.compressed_data.len();

        // Large units bypass packing entirely
        if frag_size >= BLOCK_SIZE as usize {
            return Ok(PackResult::Passthrough(unit));
        }

        let frag_size_u16 = frag_size as u16;

        // 1. Try to fit into open slot
        if let Some(ref slot) = self.open_slot {
            if slot.used + frag_size_u16 <= BLOCK_SIZE as u16 {
                let slot = self.open_slot.as_mut().unwrap();
                let offset = slot.used;
                slot.data[offset as usize..offset as usize + frag_size]
                    .copy_from_slice(&unit.compressed_data);
                slot.used += frag_size_u16;
                slot.fragments.push(SlotFragment {
                    unit,
                    slot_offset: offset,
                });
                return Ok(PackResult::Buffered);
            }
        }

        // 2. Doesn't fit in open slot — seal + allocate new
        if self.open_slot.is_some() {
            match self.allocator.allocate_one_for_lane(self.lane_id) {
                Ok(new_pba) => {
                    let sealed = self.seal_open_slot();
                    tracing::debug!(
                        pba = new_pba.0,
                        lane = self.lane_id,
                        "packer: allocated PBA for new open slot (seal+new)"
                    );
                    self.place_into_new_slot(unit, new_pba);
                    Ok(PackResult::SealedSlot(sealed))
                }
                Err(_) => {
                    let sealed = self.seal_open_slot();
                    Ok(PackResult::SealedSlotAndPassthrough(sealed, unit))
                }
            }
        } else {
            // No open slot — start a new one
            let pba = self.allocator.allocate_one_for_lane(self.lane_id)?;
            tracing::debug!(
                pba = pba.0,
                lane = self.lane_id,
                "packer: allocated PBA for first open slot"
            );
            self.place_into_new_slot(unit, pba);
            Ok(PackResult::Buffered)
        }
    }

    /// Force-seal the open slot (e.g., on shutdown or flush).
    pub fn flush_open_slot(&mut self) -> Option<SealedSlot> {
        if self.open_slot.is_some() {
            Some(self.seal_open_slot())
        } else {
            None
        }
    }

    /// Seal the current open slot once it has been kept open longer than
    /// `max_age`, even if the writer lane never goes idle.
    pub fn flush_open_slot_if_older_than(&mut self, max_age: Duration) -> Option<SealedSlot> {
        let should_flush = self
            .open_slot
            .as_ref()
            .map(|slot| slot.opened_at.elapsed() >= max_age)
            .unwrap_or(false);
        if should_flush {
            Some(self.seal_open_slot())
        } else {
            None
        }
    }

    fn seal_open_slot(&mut self) -> SealedSlot {
        let slot = self.open_slot.take().expect("no open slot to seal");
        SealedSlot {
            pba: slot.pba,
            data: slot.data,
            fragments: slot.fragments,
        }
    }

    fn place_into_new_slot(&mut self, unit: CompressedUnit, pba: Pba) {
        let frag_size = unit.compressed_data.len();
        let mut data = vec![0u8; BLOCK_SIZE as usize];
        data[..frag_size].copy_from_slice(&unit.compressed_data);
        let frag_size_u16 = frag_size as u16;

        self.open_slot = Some(OpenSlot {
            pba,
            data,
            used: frag_size_u16,
            fragments: vec![SlotFragment {
                unit,
                slot_offset: 0,
            }],
            opened_at: Instant::now(),
        });
    }
}
