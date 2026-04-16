/// Unit tests for the Packer module (bin-packing of small compressed fragments).
use std::sync::Arc;

use onyx_storage::buffer::pipeline::CompressedUnit;
use onyx_storage::packer::packer::{PackResult, Packer};
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

fn make_allocator(usable_blocks: u64) -> Arc<SpaceAllocator> {
    // Add RESERVED_BLOCKS so the allocator has the requested number of usable blocks.
    Arc::new(SpaceAllocator::new(
        (usable_blocks + onyx_storage::types::RESERVED_BLOCKS) * BLOCK_SIZE as u64,
        0,
    ))
}

#[test]
fn passthrough_large_fragment() {
    let alloc = make_allocator(100);
    let mut packer = Packer::new(alloc.clone());

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
    let mut packer = Packer::new(alloc.clone());

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
    let mut packer = Packer::new(alloc.clone());

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
    let mut packer = Packer::new(alloc.clone());

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
    let mut packer = Packer::new(alloc.clone());
    assert!(packer.flush_open_slot().is_none());
}

#[test]
fn multi_volume_packing() {
    let alloc = make_allocator(100);
    let mut packer = Packer::new(alloc.clone());

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
    let mut packer = Packer::new(alloc.clone());

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
    let alloc = Arc::new(SpaceAllocator::new(0, 0));
    let mut packer = Packer::new(alloc);

    let unit = make_unit("vol-a", 0, 1, 500);
    assert!(packer.pack_or_passthrough(unit).is_err());
    // No open slot was created
    assert!(packer.flush_open_slot().is_none());
}

#[test]
fn alloc_failure_after_seal_returns_sealed_and_passthrough() {
    // Allocator with exactly 2 blocks
    let alloc = make_allocator(2);
    let mut packer = Packer::new(alloc.clone());

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
                PackResult::SealedSlotAndPassthrough(_, _) => unreachable!(),
            }
        ),
    }

    // No open slot should exist after the failure
    assert!(packer.flush_open_slot().is_none());
}

#[test]
fn sealed_slot_data_integrity() {
    let alloc = make_allocator(100);
    let mut packer = Packer::new(alloc.clone());

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

