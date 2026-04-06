/// Unit tests for the Packer module (bin-packing of small compressed fragments).
use std::sync::Arc;

use onyx_storage::packer::packer::{new_hole_map, PackResult, Packer};
use onyx_storage::buffer::pipeline::CompressedUnit;
use onyx_storage::space::allocator::SpaceAllocator;
use onyx_storage::types::*;

fn make_unit(vol_id: &str, start_lba: u64, lba_count: u32, data_size: usize) -> CompressedUnit {
    CompressedUnit {
        vol_id: vol_id.to_string(),
        start_lba: Lba(start_lba),
        lba_count,
        original_size: (lba_count as u32) * BLOCK_SIZE,
        compressed_data: vec![0xAB; data_size],
        compression: 1, // LZ4
        crc32: crc32fast::hash(&vec![0xAB; data_size]),
        vol_created_at: 1000,
        seq_lba_ranges: vec![(1, Lba(start_lba), lba_count)],
        block_hashes: None,
        dedup_skipped: false,
        dedup_completion: None,
    }
}

fn make_allocator(blocks: u64) -> Arc<SpaceAllocator> {
    Arc::new(SpaceAllocator::new(blocks * BLOCK_SIZE as u64))
}

#[test]
fn passthrough_large_fragment() {
    let alloc = make_allocator(100);
    let mut packer = Packer::new(alloc.clone(), new_hole_map());

    // Fragment >= BLOCK_SIZE should be passed through
    let unit = make_unit("vol-a", 0, 1, BLOCK_SIZE as usize);
    match packer.pack_or_passthrough(unit).unwrap() {
        PackResult::Passthrough(u) => {
            assert_eq!(u.vol_id, "vol-a");
            assert_eq!(u.compressed_data.len(), BLOCK_SIZE as usize);
        }
        _ => panic!("expected Passthrough for large fragment"),
    }

    // No open slot should be created
    assert!(packer.flush_open_slot().is_none());
}

#[test]
fn buffer_small_fragment() {
    let alloc = make_allocator(100);
    let mut packer = Packer::new(alloc.clone(), new_hole_map());

    // Small fragment should be buffered
    let unit = make_unit("vol-a", 0, 1, 500);
    match packer.pack_or_passthrough(unit).unwrap() {
        PackResult::Buffered => {}
        _ => panic!("expected Buffered for small fragment"),
    }

    // Open slot should exist
    let sealed = packer.flush_open_slot().unwrap();
    assert_eq!(sealed.fragments.len(), 1);
    assert_eq!(sealed.fragments[0].slot_offset, 0);
    assert_eq!(&sealed.data[..500], &[0xAB; 500]);
    // Rest of slot is zero-padded
    assert!(sealed.data[500..].iter().all(|&b| b == 0));
}

#[test]
fn two_fragments_share_slot() {
    let alloc = make_allocator(100);
    let mut packer = Packer::new(alloc.clone(), new_hole_map());

    let unit1 = make_unit("vol-a", 0, 1, 1000);
    let unit2 = make_unit("vol-a", 1, 1, 1500);

    match packer.pack_or_passthrough(unit1).unwrap() {
        PackResult::Buffered => {}
        _ => panic!("expected Buffered"),
    }

    match packer.pack_or_passthrough(unit2).unwrap() {
        PackResult::Buffered => {}
        _ => panic!("expected Buffered for second fragment that fits"),
    }

    let sealed = packer.flush_open_slot().unwrap();
    assert_eq!(sealed.fragments.len(), 2);
    assert_eq!(sealed.fragments[0].slot_offset, 0);
    assert_eq!(sealed.fragments[1].slot_offset, 1000);
    assert_eq!(sealed.fragments[0].unit.compressed_data.len(), 1000);
    assert_eq!(sealed.fragments[1].unit.compressed_data.len(), 1500);
}

#[test]
fn fragment_doesnt_fit_seals_slot() {
    let alloc = make_allocator(100);
    let mut packer = Packer::new(alloc.clone(), new_hole_map());

    // Fill slot almost to capacity
    let unit1 = make_unit("vol-a", 0, 1, 3000);
    match packer.pack_or_passthrough(unit1).unwrap() {
        PackResult::Buffered => {}
        _ => panic!("expected Buffered"),
    }

    // This won't fit (3000 + 2000 > 4096)
    let unit2 = make_unit("vol-a", 1, 1, 2000);
    match packer.pack_or_passthrough(unit2).unwrap() {
        PackResult::SealedSlot(sealed) => {
            assert_eq!(sealed.fragments.len(), 1);
            assert_eq!(sealed.fragments[0].slot_offset, 0);
            assert_eq!(sealed.fragments[0].unit.compressed_data.len(), 3000);
        }
        _ => panic!("expected SealedSlot"),
    }

    // The new fragment should now be in a new open slot
    let sealed2 = packer.flush_open_slot().unwrap();
    assert_eq!(sealed2.fragments.len(), 1);
    assert_eq!(sealed2.fragments[0].slot_offset, 0);
    assert_eq!(sealed2.fragments[0].unit.compressed_data.len(), 2000);

    // Both slots should have different PBAs
    assert_ne!(sealed2.pba, Pba(0)); // PBA was allocated
}

#[test]
fn flush_empty_returns_none() {
    let alloc = make_allocator(100);
    let mut packer = Packer::new(alloc.clone(), new_hole_map());
    assert!(packer.flush_open_slot().is_none());
}

#[test]
fn multi_volume_packing() {
    let alloc = make_allocator(100);
    let mut packer = Packer::new(alloc.clone(), new_hole_map());

    // Fragments from different volumes can share a slot
    let unit1 = make_unit("vol-a", 0, 1, 1000);
    let unit2 = make_unit("vol-b", 5, 1, 500);

    match packer.pack_or_passthrough(unit1).unwrap() {
        PackResult::Buffered => {}
        _ => panic!("expected Buffered"),
    }
    match packer.pack_or_passthrough(unit2).unwrap() {
        PackResult::Buffered => {}
        _ => panic!("expected Buffered"),
    }

    let sealed = packer.flush_open_slot().unwrap();
    assert_eq!(sealed.fragments.len(), 2);
    assert_eq!(sealed.fragments[0].unit.vol_id, "vol-a");
    assert_eq!(sealed.fragments[1].unit.vol_id, "vol-b");
    assert_eq!(sealed.fragments[0].slot_offset, 0);
    assert_eq!(sealed.fragments[1].slot_offset, 1000);
}

#[test]
fn exact_fit_fragment() {
    let alloc = make_allocator(100);
    let mut packer = Packer::new(alloc.clone(), new_hole_map());

    // Fragment that exactly fills the remaining space
    let unit1 = make_unit("vol-a", 0, 1, 2048);
    let unit2 = make_unit("vol-a", 1, 1, 2048);

    match packer.pack_or_passthrough(unit1).unwrap() {
        PackResult::Buffered => {}
        _ => panic!("expected Buffered"),
    }
    // 2048 + 2048 = 4096 exactly fits
    match packer.pack_or_passthrough(unit2).unwrap() {
        PackResult::Buffered => {}
        _ => panic!("expected Buffered for exact fit"),
    }

    let sealed = packer.flush_open_slot().unwrap();
    assert_eq!(sealed.fragments.len(), 2);
}

#[test]
fn space_exhaustion_on_first_slot() {
    // No blocks available at all
    let alloc = Arc::new(SpaceAllocator::new(0));
    let mut packer = Packer::new(alloc, new_hole_map());

    let unit = make_unit("vol-a", 0, 1, 500);
    assert!(packer.pack_or_passthrough(unit).is_err());
    // No open slot was created
    assert!(packer.flush_open_slot().is_none());
}

#[test]
fn alloc_failure_after_seal_returns_sealed_and_passthrough() {
    // Allocator with exactly 2 blocks
    let alloc = make_allocator(2);
    let mut packer = Packer::new(alloc.clone(), new_hole_map());

    // First fragment opens a slot (allocates 1 block)
    let unit1 = make_unit("vol-a", 0, 1, 500);
    match packer.pack_or_passthrough(unit1).unwrap() {
        PackResult::Buffered => {}
        _ => panic!("expected Buffered"),
    }

    // Second fragment seals slot (allocates block 2) and opens new slot
    let unit2 = make_unit("vol-a", 1, 1, 4000);
    match packer.pack_or_passthrough(unit2).unwrap() {
        PackResult::SealedSlot(sealed) => {
            assert_eq!(sealed.fragments.len(), 1);
            assert_eq!(sealed.fragments[0].unit.compressed_data.len(), 500);
        }
        _ => panic!("expected SealedSlot"),
    }

    // Third fragment: seals current slot (unit2), but allocation for new slot fails.
    // Must return SealedSlotAndPassthrough — sealed slot has unit2, unit3 is returned.
    let unit3 = make_unit("vol-a", 2, 1, 3500);
    match packer.pack_or_passthrough(unit3).unwrap() {
        PackResult::SealedSlotAndPassthrough(sealed, passthrough_unit) => {
            // Sealed slot contains unit2
            assert_eq!(sealed.fragments.len(), 1);
            assert_eq!(sealed.fragments[0].unit.compressed_data.len(), 4000);
            // Passthrough unit is unit3
            assert_eq!(passthrough_unit.compressed_data.len(), 3500);
            assert_eq!(passthrough_unit.start_lba, Lba(2));
        }
        other => panic!(
            "expected SealedSlotAndPassthrough, got {}",
            match other {
                PackResult::Passthrough(_) => "Passthrough",
                PackResult::Buffered => "Buffered",
                PackResult::SealedSlot(_) => "SealedSlot",
                PackResult::SealedSlotAndPassthrough(_, _) | PackResult::FillHole(_) => unreachable!(),
            }
        ),
    }

    // No open slot should exist after the failure
    assert!(packer.flush_open_slot().is_none());
}

#[test]
fn sealed_slot_data_integrity() {
    let alloc = make_allocator(100);
    let mut packer = Packer::new(alloc.clone(), new_hole_map());

    // Use distinct data patterns to verify correct placement
    let mut unit1 = make_unit("vol-a", 0, 1, 100);
    unit1.compressed_data = vec![0x11; 100];
    unit1.crc32 = crc32fast::hash(&unit1.compressed_data);

    let mut unit2 = make_unit("vol-a", 1, 1, 200);
    unit2.compressed_data = vec![0x22; 200];
    unit2.crc32 = crc32fast::hash(&unit2.compressed_data);

    packer.pack_or_passthrough(unit1).unwrap();
    packer.pack_or_passthrough(unit2).unwrap();

    let sealed = packer.flush_open_slot().unwrap();

    // Verify data is correctly placed in the slot
    assert_eq!(&sealed.data[0..100], &[0x11; 100]);
    assert_eq!(&sealed.data[100..300], &[0x22; 200]);
    assert!(sealed.data[300..].iter().all(|&b| b == 0));
}

#[test]
fn hole_map_fills_hole_before_new_slot() {
    use onyx_storage::packer::packer::HoleKey;

    let alloc = make_allocator(100);
    let hole_map = new_hole_map();

    // Pre-populate hole map with a hole at PBA 42, offset 500, size 800
    hole_map.lock().unwrap().insert(HoleKey { pba: Pba(42), offset: 500 }, 800);

    let mut packer = Packer::new(alloc.clone(), hole_map.clone());

    // Fragment of 600 bytes — should fill the hole (800 >= 600)
    let unit = make_unit("vol-a", 0, 1, 600);
    match packer.pack_or_passthrough(unit).unwrap() {
        PackResult::FillHole(fill) => {
            assert_eq!(fill.pba, Pba(42));
            assert_eq!(fill.slot_offset, 500);
            assert_eq!(fill.unit.compressed_data.len(), 600);
        }
        _ => panic!("expected FillHole when hole map has a fitting hole"),
    }

    // Hole consumed from map. Remainder is NOT injected here —
    // it will be injected by the writer after confirming the fill succeeded.
    assert!(hole_map.lock().unwrap().is_empty());

    // Verify hole_size is carried for the writer to compute remainder
    // (already checked via fill.slot_offset and fill.unit above)
}

#[test]
fn hole_map_best_fit_selection() {
    use onyx_storage::packer::packer::HoleKey;

    let alloc = make_allocator(100);
    let hole_map = new_hole_map();

    // Two holes: 2000 bytes and 500 bytes
    {
        let mut map = hole_map.lock().unwrap();
        map.insert(HoleKey { pba: Pba(10), offset: 0 }, 2000);
        map.insert(HoleKey { pba: Pba(20), offset: 100 }, 500);
    }

    let mut packer = Packer::new(alloc.clone(), hole_map.clone());

    // 400-byte fragment should pick the 500-byte hole (best fit, least waste)
    let unit = make_unit("vol-a", 0, 1, 400);
    match packer.pack_or_passthrough(unit).unwrap() {
        PackResult::FillHole(fill) => {
            assert_eq!(fill.pba, Pba(20), "should pick smaller hole (best fit)");
            assert_eq!(fill.slot_offset, 100);
        }
        _ => panic!("expected FillHole"),
    }

    // The 2000-byte hole should still be in the map.
    // No remainder from the 500-byte hole yet (injected by writer on success).
    let remaining = hole_map.lock().unwrap();
    assert_eq!(remaining.len(), 1);
    assert!(remaining.contains_key(&HoleKey { pba: Pba(10), offset: 0 }));
}

#[test]
fn hole_map_too_small_falls_through() {
    use onyx_storage::packer::packer::HoleKey;

    let alloc = make_allocator(100);
    let hole_map = new_hole_map();

    // Hole of 100 bytes — too small for a 500-byte fragment
    hole_map.lock().unwrap().insert(HoleKey { pba: Pba(42), offset: 0 }, 100);

    let mut packer = Packer::new(alloc.clone(), hole_map.clone());

    // 500-byte fragment can't fit in 100-byte hole → should open new slot (Buffered)
    let unit = make_unit("vol-a", 0, 1, 500);
    match packer.pack_or_passthrough(unit).unwrap() {
        PackResult::Buffered => {}
        PackResult::FillHole(_) => panic!("should not fill hole that's too small"),
        _ => panic!("expected Buffered"),
    }

    // Hole should still be in the map (not consumed)
    assert_eq!(hole_map.lock().unwrap().len(), 1);
}

#[test]
fn hole_preferred_over_open_slot_when_doesnt_fit() {
    use onyx_storage::packer::packer::HoleKey;

    let alloc = make_allocator(100);
    let hole_map = new_hole_map();

    let mut packer = Packer::new(alloc.clone(), hole_map.clone());

    // Fill open slot to 3500 bytes
    let unit1 = make_unit("vol-a", 0, 1, 3500);
    match packer.pack_or_passthrough(unit1).unwrap() {
        PackResult::Buffered => {}
        _ => panic!("expected Buffered"),
    }

    // Add a hole that fits 1000 bytes
    hole_map.lock().unwrap().insert(HoleKey { pba: Pba(99), offset: 200 }, 1000);

    // 1000-byte fragment doesn't fit in open slot (3500 + 1000 > 4096)
    // but the hole fits → should FillHole
    let unit2 = make_unit("vol-a", 1, 1, 1000);
    match packer.pack_or_passthrough(unit2).unwrap() {
        PackResult::FillHole(fill) => {
            assert_eq!(fill.pba, Pba(99));
        }
        _ => panic!("expected FillHole when open slot is full but hole is available"),
    }

    // Open slot should still exist (not sealed)
    let sealed = packer.flush_open_slot();
    assert!(sealed.is_some());
    assert_eq!(sealed.unwrap().fragments.len(), 1); // only unit1
}

#[test]
fn hole_map_coalesces_adjacent_holes() {
    use onyx_storage::packer::packer::insert_hole_coalesced;

    let alloc = make_allocator(100);
    let hole_map = new_hole_map();

    // Two adjacent holes: [100..300) and [300..500) at the same PBA
    // Neither alone fits a 350-byte fragment, but merged they do.
    // Use insert_hole_coalesced which merges at insertion time.
    insert_hole_coalesced(&hole_map, Pba(10), 100, 200);
    insert_hole_coalesced(&hole_map, Pba(10), 300, 200);

    let mut packer = Packer::new(alloc.clone(), hole_map.clone());

    // 350-byte fragment — needs the merged 400-byte hole
    let unit = make_unit("vol-a", 0, 1, 350);
    match packer.pack_or_passthrough(unit).unwrap() {
        PackResult::FillHole(fill) => {
            assert_eq!(fill.pba, Pba(10));
            assert_eq!(fill.slot_offset, 100); // merged hole starts at 100
            assert_eq!(fill.hole_size, 400);   // 200 + 200 merged
        }
        _ => panic!("expected FillHole after adjacent holes are coalesced"),
    }

    // Hole map should be empty (consumed; remainder injected by writer)
    assert!(hole_map.lock().unwrap().is_empty());
}

#[test]
fn hole_map_does_not_coalesce_non_adjacent() {
    use onyx_storage::packer::packer::insert_hole_coalesced;

    let alloc = make_allocator(100);
    let hole_map = new_hole_map();

    // Two non-adjacent holes: [100..200) and [300..400) — gap at [200..300)
    insert_hole_coalesced(&hole_map, Pba(10), 100, 100);
    insert_hole_coalesced(&hole_map, Pba(10), 300, 100);

    let mut packer = Packer::new(alloc.clone(), hole_map.clone());

    // 150-byte fragment — neither hole (100 bytes each) fits
    let unit = make_unit("vol-a", 0, 1, 150);
    match packer.pack_or_passthrough(unit).unwrap() {
        PackResult::Buffered => {} // Falls through to new slot
        PackResult::FillHole(_) => panic!("should not fit in non-adjacent 100-byte holes"),
        _ => {}
    }

    // Both holes should still be in the map
    assert_eq!(hole_map.lock().unwrap().len(), 2);
}

#[test]
fn hole_map_coalesces_across_different_pbas_independently() {
    use onyx_storage::packer::packer::{HoleKey, insert_hole_coalesced};

    let alloc = make_allocator(100);
    let hole_map = new_hole_map();

    // Adjacent holes at PBA 10, and a separate hole at PBA 20
    insert_hole_coalesced(&hole_map, Pba(10), 0, 100);
    insert_hole_coalesced(&hole_map, Pba(10), 100, 100);
    insert_hole_coalesced(&hole_map, Pba(20), 0, 50);

    let mut packer = Packer::new(alloc.clone(), hole_map.clone());

    // 180-byte fragment — needs the merged 200-byte hole at PBA 10
    let unit = make_unit("vol-a", 0, 1, 180);
    match packer.pack_or_passthrough(unit).unwrap() {
        PackResult::FillHole(fill) => {
            assert_eq!(fill.pba, Pba(10));
            assert_eq!(fill.hole_size, 200);
        }
        _ => panic!("expected FillHole from merged PBA 10 holes"),
    }

    // PBA 20's hole should still be there
    let remaining = hole_map.lock().unwrap();
    assert_eq!(remaining.len(), 1);
    assert!(remaining.contains_key(&HoleKey { pba: Pba(20), offset: 0 }));
}

#[test]
fn hole_map_coalesces_three_segment_chain_when_bridge_inserted() {
    use onyx_storage::packer::packer::{HoleKey, insert_hole_coalesced};

    let alloc = make_allocator(100);
    let hole_map = new_hole_map();

    // Existing holes: [100..200) and [300..400), with a gap between them.
    // Inserting the bridging hole [200..300) should merge all three into
    // one contiguous hole [100..400).
    insert_hole_coalesced(&hole_map, Pba(10), 100, 100);
    insert_hole_coalesced(&hole_map, Pba(10), 300, 100);
    insert_hole_coalesced(&hole_map, Pba(10), 200, 100);

    {
        let map = hole_map.lock().unwrap();
        assert_eq!(map.len(), 1, "three chained holes should collapse into one");
        assert_eq!(
            map.get(&HoleKey {
                pba: Pba(10),
                offset: 100,
            })
            .copied(),
            Some(300),
            "merged chain should span [100..400)"
        );
    }

    let mut packer = Packer::new(alloc.clone(), hole_map.clone());

    // A 250-byte fragment only fits if the three segments really merged.
    let unit = make_unit("vol-a", 0, 1, 250);
    match packer.pack_or_passthrough(unit).unwrap() {
        PackResult::FillHole(fill) => {
            assert_eq!(fill.pba, Pba(10));
            assert_eq!(fill.slot_offset, 100);
            assert_eq!(fill.hole_size, 300);
        }
        _ => panic!("expected FillHole from a three-segment merged chain"),
    }

    // The merged hole was consumed; any remainder is writer-owned and not
    // reinserted here by the packer.
    assert!(hole_map.lock().unwrap().is_empty());
}
