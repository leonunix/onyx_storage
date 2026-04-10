use onyx_storage::config::MetaConfig;
use onyx_storage::meta::store::MetaStore;
use onyx_storage::space::allocator::SpaceAllocator;
use onyx_storage::space::extent::Extent;
use onyx_storage::types::{Lba, Pba, VolumeId, RESERVED_BLOCKS};

// --- extent tests ---

#[test]
fn extent_basics() {
    let e = Extent::new(Pba(10), 5);
    assert_eq!(e.end_pba(), Pba(15));
    assert!(e.contains(Pba(10)));
    assert!(e.contains(Pba(14)));
    assert!(!e.contains(Pba(15)));
    assert!(!e.contains(Pba(9)));
}

#[test]
fn extent_ordering() {
    let e1 = Extent::new(Pba(10), 5);
    let e2 = Extent::new(Pba(20), 3);
    assert!(e1 < e2);
}

// --- allocator tests ---

#[test]
fn basic_allocate_free() {
    let alloc = SpaceAllocator::new(4096 * 100);
    let usable = 100 - RESERVED_BLOCKS;
    assert_eq!(alloc.free_block_count(), usable);
    assert_eq!(alloc.allocated_block_count(), 0);

    let pba = alloc.allocate_one().unwrap();
    assert_eq!(pba, Pba(RESERVED_BLOCKS));
    assert_eq!(alloc.free_block_count(), usable - 1);

    let pba2 = alloc.allocate_one().unwrap();
    assert_eq!(pba2, Pba(RESERVED_BLOCKS + 1));

    alloc.free_one(pba).unwrap();
    assert_eq!(alloc.free_block_count(), usable - 1);

    alloc.free_one(pba2).unwrap();
    assert_eq!(alloc.free_block_count(), usable);
}

#[test]
fn no_double_allocation() {
    // 18 total blocks = 10 usable (18 - 8 reserved)
    let alloc = SpaceAllocator::new(4096 * 18);
    let mut allocated = Vec::new();
    for _ in 0..10 {
        allocated.push(alloc.allocate_one().unwrap());
    }
    let mut sorted: Vec<u64> = allocated.iter().map(|p| p.0).collect();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), 10);
}

#[test]
fn exhaustion() {
    // 10 total = 2 usable (10 - 8 reserved)
    let alloc = SpaceAllocator::new(4096 * 10);
    alloc.allocate_one().unwrap();
    alloc.allocate_one().unwrap();
    assert!(alloc.allocate_one().is_err());
}

#[test]
fn extent_allocation() {
    let alloc = SpaceAllocator::new(4096 * 100);
    let usable = 100 - RESERVED_BLOCKS;

    let ext = alloc.allocate_extent(10).unwrap();
    assert_eq!(ext.count, 10);
    assert_eq!(alloc.free_block_count(), usable - 10);

    alloc.free_extent(ext).unwrap();
    assert_eq!(alloc.free_block_count(), usable);
}

#[test]
fn coalescing() {
    // 18 total = 10 usable
    let alloc = SpaceAllocator::new(4096 * 18);

    let mut pbas = Vec::new();
    for _ in 0..10 {
        pbas.push(alloc.allocate_one().unwrap());
    }
    assert_eq!(alloc.free_block_count(), 0);

    for pba in pbas {
        alloc.free_one(pba).unwrap();
    }
    assert_eq!(alloc.free_block_count(), 10);

    let ext = alloc.allocate_extent(10).unwrap();
    assert_eq!(ext.count, 10);
}

#[test]
fn rebuild_from_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let meta_config = MetaConfig {
        rocksdb_path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let meta = MetaStore::open(&meta_config).unwrap();

    // PBAs must be >= RESERVED_BLOCKS (8) to be in usable range
    meta.set_refcount(Pba(8), 1).unwrap();
    meta.set_refcount(Pba(9), 1).unwrap();
    meta.set_refcount(Pba(13), 2).unwrap();
    meta.set_refcount(Pba(17), 1).unwrap();

    // 18 total blocks = 10 usable (18 - 8)
    let alloc = SpaceAllocator::new(4096 * 18);
    alloc.rebuild_from_metadata(&meta).unwrap();

    assert_eq!(alloc.allocated_block_count(), 4);
    assert_eq!(alloc.free_block_count(), 6);

    let pba = alloc.allocate_one().unwrap();
    assert_eq!(pba, Pba(10));
}

/// Allocate 0 blocks → error.
#[test]
fn allocate_zero_blocks() {
    let alloc = SpaceAllocator::new(4096 * 18);
    assert!(alloc.allocate_extent(0).is_err());
}

/// Allocate extent larger than available → returns largest available.
#[test]
fn allocate_extent_partial() {
    // 18 total = 10 usable
    let alloc = SpaceAllocator::new(4096 * 18);
    // Allocate 7, leaving 3
    let _ = alloc.allocate_extent(7).unwrap();
    // Request 5 but only 3 are available → returns 3
    let ext = alloc.allocate_extent(5).unwrap();
    assert_eq!(ext.count, 3);
}

/// Allocate extent when fully exhausted → error.
#[test]
fn allocate_extent_exhausted() {
    // 13 total = 5 usable
    let alloc = SpaceAllocator::new(4096 * 13);
    let _ = alloc.allocate_extent(5).unwrap();
    assert!(alloc.allocate_extent(1).is_err());
}

/// Free extent reclaims all blocks.
#[test]
fn free_extent_reclaims() {
    let alloc = SpaceAllocator::new(4096 * 100);
    let usable = 100 - RESERVED_BLOCKS;
    let ext = alloc.allocate_extent(20).unwrap();
    assert_eq!(alloc.free_block_count(), usable - 20);
    alloc.free_extent(ext).unwrap();
    assert_eq!(alloc.free_block_count(), usable);
}

/// total_block_count returns correct value.
#[test]
fn total_block_count() {
    let alloc = SpaceAllocator::new(4096 * 50);
    assert_eq!(alloc.total_block_count(), 50);
}

/// Rebuild with empty metadata → all usable blocks free.
#[test]
fn rebuild_empty_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let meta_config = MetaConfig {
        rocksdb_path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let meta = MetaStore::open(&meta_config).unwrap();

    let alloc = SpaceAllocator::new(4096 * 100);
    alloc.rebuild_from_metadata(&meta).unwrap();

    let usable = 100 - RESERVED_BLOCKS;
    assert_eq!(alloc.allocated_block_count(), 0);
    assert_eq!(alloc.free_block_count(), usable);
}

/// Rebuild with all usable blocks allocated.
#[test]
fn rebuild_fully_allocated() {
    let dir = tempfile::tempdir().unwrap();
    let meta_config = MetaConfig {
        rocksdb_path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let meta = MetaStore::open(&meta_config).unwrap();

    // 18 total = 10 usable, allocate PBAs 8..17
    for i in RESERVED_BLOCKS..18u64 {
        meta.set_refcount(Pba(i), 1).unwrap();
    }

    let alloc = SpaceAllocator::new(4096 * 18);
    alloc.rebuild_from_metadata(&meta).unwrap();

    assert_eq!(alloc.allocated_block_count(), 10);
    assert_eq!(alloc.free_block_count(), 0);
    assert!(alloc.allocate_one().is_err());
}

#[test]
fn rebuild_from_blockmap_marks_multi_block_units() {
    let dir = tempfile::tempdir().unwrap();
    let meta_config = MetaConfig {
        rocksdb_path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let meta = MetaStore::open(&meta_config).unwrap();
    let vol_id = VolumeId("vol-a".into());

    // Use PBAs in usable range (>= RESERVED_BLOCKS)
    let value0 = onyx_storage::meta::schema::BlockmapValue {
        pba: Pba(12),
        compression: 0,
        unit_compressed_size: 8192,
        unit_original_size: 8192,
        unit_lba_count: 2,
        offset_in_unit: 0,
        crc32: 0x1234_5678,
        slot_offset: 0,
        flags: 0,
    };
    let value1 = onyx_storage::meta::schema::BlockmapValue {
        offset_in_unit: 1,
        ..value0
    };

    meta.put_mapping(&vol_id, Lba(0), &value0).unwrap();
    meta.put_mapping(&vol_id, Lba(1), &value1).unwrap();
    meta.set_refcount(Pba(12), 2).unwrap();

    // 18 total = 10 usable
    let alloc = SpaceAllocator::new(4096 * 18);
    alloc.rebuild_from_metadata(&meta).unwrap();

    assert_eq!(alloc.allocated_block_count(), 2);
    assert_eq!(alloc.free_block_count(), 8);
    assert_eq!(alloc.allocate_one().unwrap(), Pba(RESERVED_BLOCKS));
}

// --- allocator validation tests ---

/// Free a PBA beyond total_blocks → error.
#[test]
fn free_one_out_of_bounds() {
    let alloc = SpaceAllocator::new(4096 * 18); // 18 blocks: 0..17, usable 8..17
    assert!(alloc.free_one(Pba(18)).is_err());
    assert!(alloc.free_one(Pba(100)).is_err());
}

/// Double-free the same PBA → error on second free.
#[test]
fn free_one_double_free() {
    let alloc = SpaceAllocator::new(4096 * 18);
    let pba = alloc.allocate_one().unwrap();
    alloc.free_one(pba).unwrap();
    assert!(alloc.free_one(pba).is_err()); // already free
}

/// Free a PBA that was never allocated (initially all free) → error.
#[test]
fn free_one_never_allocated() {
    let alloc = SpaceAllocator::new(4096 * 18);
    // PBA 13 is in the initial free extent, never allocated
    assert!(alloc.free_one(Pba(13)).is_err());
}

/// Free extent out of bounds → error.
#[test]
fn free_extent_out_of_bounds() {
    // 18 total = 10 usable
    let alloc = SpaceAllocator::new(4096 * 18);
    let _ = alloc.allocate_extent(10).unwrap();
    // Try to free extent that goes beyond total blocks
    assert!(alloc.free_extent(Extent::new(Pba(16), 5)).is_err()); // 16+5=21 > 18
}

/// Free extent of 0 blocks → error.
#[test]
fn free_extent_zero_blocks() {
    let alloc = SpaceAllocator::new(4096 * 18);
    assert!(alloc
        .free_extent(Extent::new(Pba(RESERVED_BLOCKS), 0))
        .is_err());
}

/// Free extent overlapping existing free space → error.
#[test]
fn free_extent_overlap() {
    // 18 total = 10 usable (PBA 8..17)
    let alloc = SpaceAllocator::new(4096 * 18);
    let _ = alloc.allocate_extent(5).unwrap(); // allocates 8..12
    let ext2 = alloc.allocate_extent(5).unwrap(); // allocates 13..17
    alloc.free_extent(ext2).unwrap(); // 13..17 now free

    // Try to free 11..15 — overlaps with already-free 13..17
    assert!(alloc.free_extent(Extent::new(Pba(11), 5)).is_err());
}

/// Underflow protection: freeing more blocks than allocated → error.
#[test]
fn free_extent_underflow() {
    // 12 total = 4 usable (PBA 8..11)
    let alloc2 = SpaceAllocator::new(4096 * 12);
    let _ = alloc2.allocate_extent(4).unwrap(); // 4 allocated, 0 free
    alloc2
        .free_extent(Extent::new(Pba(RESERVED_BLOCKS), 2))
        .unwrap(); // free 8..9, 2 allocated
                   // Now try to free 3 blocks starting at PBA 10 — but only 2 are allocated
    assert!(alloc2.free_extent(Extent::new(Pba(10), 3)).is_err());
}
