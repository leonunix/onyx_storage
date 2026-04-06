use onyx_storage::config::MetaConfig;
use onyx_storage::meta::store::MetaStore;
use onyx_storage::space::allocator::SpaceAllocator;
use onyx_storage::space::extent::Extent;
use onyx_storage::types::{Lba, Pba, VolumeId};

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
    assert_eq!(alloc.free_block_count(), 100);
    assert_eq!(alloc.allocated_block_count(), 0);

    let pba = alloc.allocate_one().unwrap();
    assert_eq!(pba, Pba(0));
    assert_eq!(alloc.free_block_count(), 99);

    let pba2 = alloc.allocate_one().unwrap();
    assert_eq!(pba2, Pba(1));

    alloc.free_one(pba).unwrap();
    assert_eq!(alloc.free_block_count(), 99);

    alloc.free_one(pba2).unwrap();
    assert_eq!(alloc.free_block_count(), 100);
}

#[test]
fn no_double_allocation() {
    let alloc = SpaceAllocator::new(4096 * 10);
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
    let alloc = SpaceAllocator::new(4096 * 2);
    alloc.allocate_one().unwrap();
    alloc.allocate_one().unwrap();
    assert!(alloc.allocate_one().is_err());
}

#[test]
fn extent_allocation() {
    let alloc = SpaceAllocator::new(4096 * 100);

    let ext = alloc.allocate_extent(10).unwrap();
    assert_eq!(ext.count, 10);
    assert_eq!(alloc.free_block_count(), 90);

    alloc.free_extent(ext).unwrap();
    assert_eq!(alloc.free_block_count(), 100);
}

#[test]
fn coalescing() {
    let alloc = SpaceAllocator::new(4096 * 10);

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
        rocksdb_path: dir.path().to_path_buf(),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let meta = MetaStore::open(&meta_config).unwrap();

    meta.set_refcount(Pba(0), 1).unwrap();
    meta.set_refcount(Pba(1), 1).unwrap();
    meta.set_refcount(Pba(5), 2).unwrap();
    meta.set_refcount(Pba(9), 1).unwrap();

    let alloc = SpaceAllocator::new(4096 * 10);
    alloc.rebuild_from_metadata(&meta).unwrap();

    assert_eq!(alloc.allocated_block_count(), 4);
    assert_eq!(alloc.free_block_count(), 6);

    let pba = alloc.allocate_one().unwrap();
    assert_eq!(pba, Pba(2));
}

/// Allocate 0 blocks → error.
#[test]
fn allocate_zero_blocks() {
    let alloc = SpaceAllocator::new(4096 * 10);
    assert!(alloc.allocate_extent(0).is_err());
}

/// Allocate extent larger than available → returns largest available.
#[test]
fn allocate_extent_partial() {
    let alloc = SpaceAllocator::new(4096 * 10);
    // Allocate 7 blocks, leaving 3
    let _ = alloc.allocate_extent(7).unwrap();
    // Request 5 but only 3 are available → returns 3
    let ext = alloc.allocate_extent(5).unwrap();
    assert_eq!(ext.count, 3);
}

/// Allocate extent when fully exhausted → error.
#[test]
fn allocate_extent_exhausted() {
    let alloc = SpaceAllocator::new(4096 * 5);
    let _ = alloc.allocate_extent(5).unwrap();
    assert!(alloc.allocate_extent(1).is_err());
}

/// Free extent reclaims all blocks.
#[test]
fn free_extent_reclaims() {
    let alloc = SpaceAllocator::new(4096 * 100);
    let ext = alloc.allocate_extent(20).unwrap();
    assert_eq!(alloc.free_block_count(), 80);
    alloc.free_extent(ext).unwrap();
    assert_eq!(alloc.free_block_count(), 100);
}

/// total_block_count returns correct value.
#[test]
fn total_block_count() {
    let alloc = SpaceAllocator::new(4096 * 50);
    assert_eq!(alloc.total_block_count(), 50);
}

/// Rebuild with empty metadata → all blocks free.
#[test]
fn rebuild_empty_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let meta_config = MetaConfig {
        rocksdb_path: dir.path().to_path_buf(),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let meta = MetaStore::open(&meta_config).unwrap();

    let alloc = SpaceAllocator::new(4096 * 100);
    alloc.rebuild_from_metadata(&meta).unwrap();

    assert_eq!(alloc.allocated_block_count(), 0);
    assert_eq!(alloc.free_block_count(), 100);
}

/// Rebuild with all blocks allocated.
#[test]
fn rebuild_fully_allocated() {
    let dir = tempfile::tempdir().unwrap();
    let meta_config = MetaConfig {
        rocksdb_path: dir.path().to_path_buf(),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let meta = MetaStore::open(&meta_config).unwrap();

    for i in 0..10u64 {
        meta.set_refcount(Pba(i), 1).unwrap();
    }

    let alloc = SpaceAllocator::new(4096 * 10);
    alloc.rebuild_from_metadata(&meta).unwrap();

    assert_eq!(alloc.allocated_block_count(), 10);
    assert_eq!(alloc.free_block_count(), 0);
    assert!(alloc.allocate_one().is_err());
}

#[test]
fn rebuild_from_blockmap_marks_multi_block_units() {
    let dir = tempfile::tempdir().unwrap();
    let meta_config = MetaConfig {
        rocksdb_path: dir.path().to_path_buf(),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let meta = MetaStore::open(&meta_config).unwrap();
    let vol_id = VolumeId("vol-a".into());

    let value0 = onyx_storage::meta::schema::BlockmapValue {
        pba: Pba(4),
        compression: 0,
        unit_compressed_size: 8192,
        unit_original_size: 8192,
        unit_lba_count: 2,
        offset_in_unit: 0,
        crc32: 0x1234_5678,
        slot_offset: 0,
    };
    let value1 = onyx_storage::meta::schema::BlockmapValue {
        offset_in_unit: 1,
        ..value0
    };

    meta.put_mapping(&vol_id, Lba(0), &value0).unwrap();
    meta.put_mapping(&vol_id, Lba(1), &value1).unwrap();
    meta.set_refcount(Pba(4), 2).unwrap();

    let alloc = SpaceAllocator::new(4096 * 10);
    alloc.rebuild_from_metadata(&meta).unwrap();

    assert_eq!(alloc.allocated_block_count(), 2);
    assert_eq!(alloc.free_block_count(), 8);
    assert_eq!(alloc.allocate_one().unwrap(), Pba(0));
}

// --- allocator validation tests ---

/// Free a PBA beyond total_blocks → error.
#[test]
fn free_one_out_of_bounds() {
    let alloc = SpaceAllocator::new(4096 * 10); // 10 blocks: 0..9
    assert!(alloc.free_one(Pba(10)).is_err());
    assert!(alloc.free_one(Pba(100)).is_err());
}

/// Double-free the same PBA → error on second free.
#[test]
fn free_one_double_free() {
    let alloc = SpaceAllocator::new(4096 * 10);
    let pba = alloc.allocate_one().unwrap();
    alloc.free_one(pba).unwrap();
    assert!(alloc.free_one(pba).is_err()); // already free
}

/// Free a PBA that was never allocated (initially all free) → error.
#[test]
fn free_one_never_allocated() {
    let alloc = SpaceAllocator::new(4096 * 10);
    // PBA 5 is in the initial free extent, never allocated
    assert!(alloc.free_one(Pba(5)).is_err());
}

/// Free extent out of bounds → error.
#[test]
fn free_extent_out_of_bounds() {
    let alloc = SpaceAllocator::new(4096 * 10);
    let _ = alloc.allocate_extent(10).unwrap();
    // Try to free extent that goes beyond total blocks
    assert!(alloc.free_extent(Extent::new(Pba(8), 5)).is_err()); // 8+5=13 > 10
}

/// Free extent of 0 blocks → error.
#[test]
fn free_extent_zero_blocks() {
    let alloc = SpaceAllocator::new(4096 * 10);
    assert!(alloc.free_extent(Extent::new(Pba(0), 0)).is_err());
}

/// Free extent overlapping existing free space → error.
#[test]
fn free_extent_overlap() {
    let alloc = SpaceAllocator::new(4096 * 10);
    // Allocate 5, free 5 leaves 0..4 free and 5..9 allocated
    let _ = alloc.allocate_extent(5).unwrap(); // allocates 0..4
    let ext2 = alloc.allocate_extent(5).unwrap(); // allocates 5..9
    alloc.free_extent(ext2).unwrap(); // 5..9 now free

    // Try to free 3..7 — overlaps with already-free 5..9
    assert!(alloc.free_extent(Extent::new(Pba(3), 5)).is_err());
}

/// Underflow protection: freeing more blocks than allocated → error.
#[test]
fn free_extent_underflow() {
    let alloc = SpaceAllocator::new(4096 * 10);
    let ext = alloc.allocate_extent(2).unwrap(); // 2 allocated
    alloc.free_extent(ext).unwrap();
    // Now 0 allocated. Allocate 1, then try to free 5.
    let _ = alloc.allocate_one().unwrap(); // 1 allocated
                                           // Allocate_one gets PBA 0, so 1..9 is still free.
                                           // Trying to free PBA 0 again (which is allocated) is fine,
                                           // but trying to free an extent of 3 when only 1 is allocated → underflow
                                           // We need to be more creative: allocate 1, then try to free a large extent
                                           // that is actually allocated (by manipulating state)
                                           // Simply: the check is that extent.count > allocated_blocks
                                           // With 1 allocated, freeing extent(0,1) is OK, but freeing extent that claims 5 blocks
                                           // when only 1 is allocated would underflow (if those 5 blocks were somehow all allocated)
                                           // For a clean test: exhaust, partially free, then over-free
    let alloc2 = SpaceAllocator::new(4096 * 4);
    let _ = alloc2.allocate_extent(4).unwrap(); // 4 allocated, 0 free
    alloc2.free_extent(Extent::new(Pba(0), 2)).unwrap(); // free 0..1, 2 allocated
                                                         // Now try to free 3 blocks starting at PBA 2 — but only 2 are allocated
    assert!(alloc2.free_extent(Extent::new(Pba(2), 3)).is_err());
}
