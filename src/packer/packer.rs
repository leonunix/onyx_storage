use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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

/// A hole in an existing packed slot, keyed by (pba, offset).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HoleKey {
    pub pba: Pba,
    pub offset: u16,
}

/// Shared hole map between write path (producer) and Packer (consumer).
/// Keyed by (pba, offset) → size. Dedup is automatic via HashMap.
pub type HoleMap = Arc<Mutex<HashMap<HoleKey, u16>>>;

/// Create a new empty shared hole map.
pub fn new_hole_map() -> HoleMap {
    Arc::new(Mutex::new(HashMap::new()))
}

/// Insert a hole into the map and coalesce with adjacent entries at the same PBA.
/// This is the only correct way to add holes — ensures the map stays merged.
pub fn insert_hole_coalesced(map: &HoleMap, pba: Pba, offset: u16, size: u16) {
    let mut holes = map.lock().unwrap();

    let mut new_offset = offset;
    let mut new_size = size;

    // Try to merge with the hole immediately before: (pba, prev_offset) where prev_offset + prev_size == new_offset
    // We need to scan for it since we don't have a reverse index.
    let before_key = holes.iter().find_map(|(k, &s)| {
        if k.pba == pba && k.offset + s == new_offset {
            Some(*k)
        } else {
            None
        }
    });
    if let Some(bk) = before_key {
        let bs = holes.remove(&bk).unwrap();
        new_offset = bk.offset;
        new_size += bs;
    }

    // Try to merge with the hole immediately after: (pba, new_offset + new_size)
    let after_key = HoleKey {
        pba,
        offset: new_offset + new_size,
    };
    if let Some(as_) = holes.remove(&after_key) {
        new_size += as_;
    }

    holes.insert(
        HoleKey {
            pba,
            offset: new_offset,
        },
        new_size,
    );
}

/// Describes filling a hole in an existing packed slot (read-modify-write).
pub struct HoleFill {
    pub pba: Pba,
    pub slot_offset: u16,
    pub hole_size: u16,
    pub unit: CompressedUnit,
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
    /// Fill a hole in an existing packed slot (read-modify-write on LV3).
    FillHole(HoleFill),
}

struct OpenSlot {
    pba: Pba,
    data: Vec<u8>,
    used: u16,
    fragments: Vec<SlotFragment>,
}

/// Bin-packing of small compressed units into shared 4KB physical slots.
///
/// Owned by the flusher writer thread (single-threaded, no synchronization needed).
/// The hole_map is shared with the write path which pushes holes when fragments
/// die; the packer drains fitting holes when packing new fragments.
pub struct Packer {
    allocator: Arc<SpaceAllocator>,
    open_slot: Option<OpenSlot>,
    hole_map: HoleMap,
}

impl Packer {
    pub fn new(allocator: Arc<SpaceAllocator>, hole_map: HoleMap) -> Self {
        Self {
            allocator,
            open_slot: None,
            hole_map,
        }
    }

    /// Access the shared hole map (for write path hole detection).
    pub fn hole_map(&self) -> &HoleMap {
        &self.hole_map
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

        // 2. Try hole map — find the smallest hole that fits (best-fit)
        if let Some((key, hole_size)) = self.take_best_hole(frag_size_u16) {
            // Don't inject remainder here — the writer will do it after
            // confirming the fill succeeded. Otherwise a failed fill leaves
            // a phantom sub-hole in the map.
            return Ok(PackResult::FillHole(HoleFill {
                pba: key.pba,
                slot_offset: key.offset,
                hole_size,
                unit,
            }));
        }

        // 3. Doesn't fit in open slot, no hole available — seal + allocate new
        if self.open_slot.is_some() {
            match self.allocator.allocate_one() {
                Ok(new_pba) => {
                    let sealed = self.seal_open_slot();
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
            let pba = self.allocator.allocate_one()?;
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

    /// Find the smallest hole >= frag_size, remove it, return (key, full_hole_size).
    fn take_best_hole(&self, frag_size: u16) -> Option<(HoleKey, u16)> {
        let mut holes = self.hole_map.lock().unwrap();
        if holes.is_empty() {
            return None;
        }

        let mut best_key = None;
        let mut best_waste = u16::MAX;
        for (key, &size) in holes.iter() {
            if size >= frag_size {
                let waste = size - frag_size;
                if waste < best_waste {
                    best_waste = waste;
                    best_key = Some(*key);
                }
            }
        }
        if let Some(key) = best_key {
            let size = holes.remove(&key).unwrap();
            Some((key, size))
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
        });
    }
}
