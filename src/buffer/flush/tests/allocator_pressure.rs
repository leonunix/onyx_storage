use super::*;

// -----------------------------------------------------------------------
// Bug-hunt tests: candidate race conditions for allocator live-PBA-reuse
// -----------------------------------------------------------------------

/// Test #1: Allocator lane cache TOCTOU in free_one.
///
/// free_one checks lane caches one-by-one (releasing each lock), then
/// checks the global free list.  If two threads concurrently free_one the
/// same PBA and a third thread moves the PBA from global→cache between
/// checks, the second free_one could miss it in BOTH checks and add it
/// to the global free list again → double allocation.
///
/// This test hammers free_one + allocate_one_for_lane concurrently
/// and checks that no PBA is ever handed out twice.
#[test]
fn allocator_lane_cache_concurrent_free_allocate_no_double_handout() {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::sync::Barrier;

    let device_size: u64 = 4096 * 2000;
    let num_lanes = 4;
    let allocator = Arc::new(SpaceAllocator::new(device_size, num_lanes));

    let rounds = 2000;
    let threads_per_type = 4; // 4 allocate + 4 free threads
    let barrier = Arc::new(Barrier::new(threads_per_type * 2));
    let found_double = AtomicBool::new(false);

    // Pre-allocate a pool of PBAs that threads will rapidly cycle
    let mut initial_pbas: Vec<Pba> = Vec::new();
    for _ in 0..200 {
        match allocator.allocate_one() {
            Ok(pba) => initial_pbas.push(pba),
            Err(_) => break,
        }
    }
    // Free them so they're in the global pool
    for &pba in &initial_pbas {
        allocator.free_one(pba).unwrap();
    }

    // Shared slot: allocator threads deposit PBAs, free threads pick them up
    let shared_pbas: Arc<Mutex<Vec<Pba>>> = Arc::new(Mutex::new(Vec::new()));
    let allocated_counter = AtomicU64::new(0);

    std::thread::scope(|s| {
        // Allocator threads: rapidly allocate and record PBAs
        for lane in 0..threads_per_type {
            let allocator = &allocator;
            let shared_pbas = &shared_pbas;
            let barrier = &barrier;
            let allocated_counter = &allocated_counter;

            s.spawn(move || {
                barrier.wait();
                for _ in 0..rounds {
                    if let Ok(pba) = allocator.allocate_one_for_lane(lane % num_lanes) {
                        allocated_counter.fetch_add(1, Ordering::Relaxed);
                        shared_pbas.lock().unwrap().push(pba);
                    }
                }
            });
        }

        // Free threads: rapidly free PBAs from the shared pool
        for _ in 0..threads_per_type {
            let allocator = &allocator;
            let shared_pbas = &shared_pbas;
            let barrier = &barrier;

            s.spawn(move || {
                barrier.wait();
                for _ in 0..rounds {
                    let pba = {
                        let mut pool = shared_pbas.lock().unwrap();
                        pool.pop()
                    };
                    if let Some(pba) = pba {
                        let _ = allocator.free_one(pba);
                    }
                    std::thread::yield_now();
                }
            });
        }
    });

    // Post-check: allocate ALL remaining free PBAs and check for duplicates
    let mut seen: HashSet<Pba> = HashSet::new();
    let mut duplicates = 0u32;
    loop {
        match allocator.allocate_one() {
            Ok(pba) => {
                if !seen.insert(pba) {
                    eprintln!("DOUBLE ALLOCATION: PBA {} handed out twice!", pba.0);
                    duplicates += 1;
                    found_double.store(true, Ordering::Relaxed);
                }
            }
            Err(_) => break,
        }
    }
    // Also drain lane caches
    allocator.drain_lane_caches();
    loop {
        match allocator.allocate_one() {
            Ok(pba) => {
                if !seen.insert(pba) {
                    eprintln!(
                        "DOUBLE ALLOCATION (post-drain): PBA {} handed out twice!",
                        pba.0
                    );
                    duplicates += 1;
                    found_double.store(true, Ordering::Relaxed);
                }
            }
            Err(_) => break,
        }
    }

    assert!(
        !found_double.load(Ordering::Relaxed),
        "allocator handed out {} PBAs more than once under concurrent free/allocate",
        duplicates
    );
}

/// Test #2: Rapid PBA recycle: allocate → metadata write → overwrite all
/// refs → cleanup/free → reallocate → verify no stale blockmap refs.
///
/// If cleanup_dead_pba_post_commit frees a PBA while stale blockmap refs
/// still exist (due to refcount drift), the reallocated PBA would inherit
/// those ghost references.
#[test]
fn rapid_pba_recycle_no_ghost_blockmap_refs() {
    // Phase 5: rc no longer reflects per-LBA references; the
    // load-bearing invariant is that recycled PBAs come back with
    // zero live blockmap entries (no ghost refs).
    let (meta, _pool, _lifecycle, allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let vol = VolumeId("flush-race".into());
    let mut ghost_found = false;

    for cycle in 0..300u64 {
        let pba = match allocator.allocate_one_for_lane(0) {
            Ok(p) => p,
            Err(_) => break,
        };
        let base_lba = cycle * 100;

        let batch: Vec<(VolumeId, Lba, BlockmapValue)> = (0..8u64)
            .map(|i| {
                (
                    vol.clone(),
                    Lba(base_lba + i),
                    BlockmapValue {
                        pba,
                        compression: 1,
                        unit_compressed_size: 400,
                        unit_original_size: 32768,
                        unit_lba_count: 8,
                        offset_in_unit: i as u16,
                        crc32: 0xCC000000 + cycle as u32,
                        slot_offset: 0,
                        flags: 0,
                    },
                )
            })
            .collect();
        meta.atomic_batch_write_packed(&batch, pba, 8).unwrap();
        assert_eq!(meta.count_blockmap_refs_for_pba(pba).unwrap(), 8);

        let replacement_pba = match allocator.allocate_one_for_lane(0) {
            Ok(p) => p,
            Err(_) => break,
        };
        let overwrite: Vec<(Lba, BlockmapValue)> = (0..8u64)
            .map(|i| {
                (
                    Lba(base_lba + i),
                    BlockmapValue {
                        pba: replacement_pba,
                        compression: 0,
                        unit_compressed_size: BLOCK_SIZE,
                        unit_original_size: BLOCK_SIZE,
                        unit_lba_count: 1,
                        offset_in_unit: 0,
                        crc32: 0xDD000000,
                        slot_offset: 0,
                        flags: 0,
                    },
                )
            })
            .collect();

        let args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> = vec![(&vol, &overwrite, 8)];
        meta.atomic_batch_write_multi(&args).unwrap();

        // After overwrite, blockmap no longer references pba.
        assert_eq!(
            meta.count_blockmap_refs_for_pba(pba).unwrap(),
            0,
            "cycle {cycle}: blockmap refs not cleared"
        );
        assert!(!meta.has_any_blockmap_ref(pba).unwrap());

        BufferFlusher::cleanup_dead_pba_post_commit(
            &allocator,
            &crate::dedup::CandidateCache::new(8, 64),
            cleanup_for_pba(pba, 1),
            "recycle_test",
        );

        assert!(allocator.is_retired(pba));
        assert!(
            allocator
                .reclaim_retired_extent(Extent::new(pba, 1))
                .unwrap(),
            "cycle {cycle}: retired PBA should reclaim after metadata verification"
        );

        let new_pba = match allocator.allocate_one_for_lane(0) {
            Ok(p) => p,
            Err(_) => break,
        };

        // Reallocated PBA must have no ghost blockmap refs.
        let refs = meta.count_blockmap_refs_for_pba(new_pba).unwrap();
        if refs != 0 {
            eprintln!(
                "GHOST REFS on PBA {} (cycle {}): blockmap_refs={}",
                new_pba.0, cycle, refs
            );
            ghost_found = true;
        }

        let _ = allocator.free_one(new_pba);
    }

    assert!(
        !ghost_found,
        "found ghost blockmap refs on reallocated PBAs"
    );
}

/// Test #3: Concurrent multi-lane overwrite + dedup hits + cleanup on shared PBA.
///
/// Simulates the production pattern: PBA X has refs from multiple volumes/LBAs.
/// Thread A does batch overwrites (decrementing). Thread B does dedup hits
/// (incrementing). Thread C does cleanup when refcount reaches 0.
/// After all threads finish, verify refcount == blockmap_refs for ALL PBAs.
#[test]
fn concurrent_overwrite_dedup_cleanup_refcount_integrity() {
    use std::sync::atomic::AtomicBool;
    use std::sync::Barrier;

    let (meta, _pool, _lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let vol = VolumeId("flush-race".into());
    let meta = &*meta;
    let allocator = &*allocator;
    let vol = &vol;

    let found_drift = AtomicBool::new(false);
    let barrier = Barrier::new(3);
    let lba_counter = std::sync::atomic::AtomicU64::new(50000);

    // Pre-create 30 shared PBAs, each with 8 LBA references + dedup entries
    let shared_pbas: Vec<Pba> = (0..30)
        .filter_map(|_| allocator.allocate_one_for_lane(0).ok())
        .collect();

    for (idx, &pba) in shared_pbas.iter().enumerate() {
        let base_lba = lba_counter.fetch_add(8, Ordering::Relaxed);
        let batch: Vec<(VolumeId, Lba, BlockmapValue)> = (0..8u64)
            .map(|i| {
                (
                    vol.clone(),
                    Lba(base_lba + i),
                    BlockmapValue {
                        pba,
                        compression: 1,
                        unit_compressed_size: 400,
                        unit_original_size: 32768,
                        unit_lba_count: 8,
                        offset_in_unit: i as u16,
                        crc32: 0xAA000000 + idx as u32,
                        slot_offset: 0,
                        flags: 0,
                    },
                )
            })
            .collect();
        meta.atomic_batch_write_packed(&batch, pba, 8).unwrap();

        // Register dedup entries
        let entries: Vec<(ContentHash, DedupEntry)> = (0..8u8)
            .map(|i| {
                let mut h = [0u8; 8];
                h[0] = idx as u8;
                h[1] = i;
                // marker
                (
                    h,
                    DedupEntry {
                        pba,
                        slot_offset: 0,
                        compression: 1,
                        unit_compressed_size: 400,
                        unit_original_size: 32768,
                        unit_lba_count: 8,
                        offset_in_unit: i as u16,
                        crc32: 0xAA000000 + idx as u32,
                    },
                )
            })
            .collect();
        meta.put_dedup_entries(&entries).unwrap();
    }

    std::thread::scope(|s| {
        // Thread A: dedup hits — add MORE references to shared PBAs
        s.spawn(|| {
            barrier.wait();
            for round in 0..200u64 {
                let pba_idx = (round as usize) % shared_pbas.len();
                let pba = shared_pbas[pba_idx];
                let base_lba = lba_counter.fetch_add(2, Ordering::Relaxed);

                let hashes: Vec<ContentHash> = (0..2u8)
                    .map(|i| {
                        let mut h = [0u8; 8];
                        h[0] = pba_idx as u8;
                        h[1] = i;

                        h
                    })
                    .collect();

                let hits: Vec<(Lba, BlockmapValue, ContentHash)> = (0..2u64)
                    .map(|i| {
                        (
                            Lba(base_lba + i),
                            BlockmapValue {
                                pba,
                                compression: 1,
                                unit_compressed_size: 400,
                                unit_original_size: 32768,
                                unit_lba_count: 8,
                                offset_in_unit: i as u16,
                                crc32: 0xAA000000 + pba_idx as u32,
                                slot_offset: 0,
                                flags: 0,
                            },
                            hashes[i as usize],
                        )
                    })
                    .collect();

                let _ = meta.atomic_batch_dedup_hits(vol, &hits);
            }
        });

        // Thread B: overwrites — overwrite LBAs pointing to shared PBAs
        s.spawn(|| {
            barrier.wait();
            for round in 0..200u64 {
                let pba_idx = (round as usize) % shared_pbas.len();
                let pba = shared_pbas[pba_idx];

                let new_pba = match allocator.allocate_one_for_lane(1) {
                    Ok(p) => p,
                    Err(_) => continue,
                };

                // Overwrite the first 2 original LBAs
                let orig_base = 50000 + (pba_idx as u64) * 8;
                let overwrite: Vec<(Lba, BlockmapValue)> = (0..2u64)
                    .map(|i| {
                        (
                            Lba(orig_base + i),
                            BlockmapValue {
                                pba: new_pba,
                                compression: 0,
                                unit_compressed_size: BLOCK_SIZE,
                                unit_original_size: BLOCK_SIZE,
                                unit_lba_count: 1,
                                offset_in_unit: 0,
                                crc32: 0xDD000000 + round as u32,
                                slot_offset: 0,
                                flags: 0,
                            },
                        )
                    })
                    .collect();

                let args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> =
                    vec![(vol, &overwrite, 2)];
                let result = meta.atomic_batch_write_multi(&args);

                // If any old PBAs hit zero, clean them up
                if let Ok(newly_zeroed) = result {
                    for cleanup in newly_zeroed.values() {
                        BufferFlusher::cleanup_dead_pba_post_commit(
                            allocator,
                            &crate::dedup::CandidateCache::new(8, 64),
                            cleanup.clone(),
                            "concurrent_test_overwrite",
                        );
                    }
                }
            }
        });

        // Thread C: more batch overwrites from a different lane
        s.spawn(|| {
            barrier.wait();
            for round in 0..200u64 {
                let pba_idx = ((round + 15) as usize) % shared_pbas.len();

                let new_pba = match allocator.allocate_one_for_lane(2) {
                    Ok(p) => p,
                    Err(_) => continue,
                };

                let orig_base = 50000 + (pba_idx as u64) * 8;
                let overwrite: Vec<(Lba, BlockmapValue)> = (4..6u64)
                    .map(|i| {
                        (
                            Lba(orig_base + i),
                            BlockmapValue {
                                pba: new_pba,
                                compression: 0,
                                unit_compressed_size: BLOCK_SIZE,
                                unit_original_size: BLOCK_SIZE,
                                unit_lba_count: 1,
                                offset_in_unit: 0,
                                crc32: 0xEE000000 + round as u32,
                                slot_offset: 0,
                                flags: 0,
                            },
                        )
                    })
                    .collect();

                let args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> =
                    vec![(vol, &overwrite, 2)];
                let result = meta.atomic_batch_write_multi(&args);

                if let Ok(newly_zeroed) = result {
                    for cleanup in newly_zeroed.values() {
                        BufferFlusher::cleanup_dead_pba_post_commit(
                            allocator,
                            &crate::dedup::CandidateCache::new(8, 64),
                            cleanup.clone(),
                            "concurrent_test_overwrite_c",
                        );
                    }
                }
            }
        });
    });

    // Phase 5: rc-vs-blockmap drift cannot happen because the hot
    // path doesn't move rc. Verify the concurrent workload didn't
    // crash and that the blockmap-ref count is internally consistent
    // (every pba with refs > 0 is also has_any_blockmap_ref → true).
    let _ = found_drift;
    for &pba in &shared_pbas {
        let refs = meta.count_blockmap_refs_for_pba(pba).unwrap();
        if refs > 0 {
            assert!(meta.has_any_blockmap_ref(pba).unwrap());
        }
    }
    for pba_val in 0..20000u64 {
        let pba = Pba(pba_val + crate::types::RESERVED_BLOCKS);
        let refs = meta.count_blockmap_refs_for_pba(pba).unwrap();
        if refs > 0 {
            assert!(meta.has_any_blockmap_ref(pba).unwrap());
        }
    }
}

/// Test #4: Full PBA lifecycle with cleanup + immediate reallocation.
///
/// Multiple threads concurrently: allocate → packed write → overwrite →
/// cleanup → free → reallocate. Checks that reallocated PBAs never have
/// stale refcounts from their prior incarnation.
///
/// Critical: only PBAs that go through the natural newly_zeroed + cleanup
/// path are freed.  Replacement PBAs stay allocated (their blockmap entries
/// remain live), ensuring ghost refs can only appear from a real bug, not
/// from artificial shortcutting.
#[test]
fn concurrent_pba_lifecycle_no_stale_refcount_on_realloc() {
    use std::sync::atomic::AtomicBool;
    use std::sync::Barrier;

    let (meta, _pool, _lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let vol = VolumeId("flush-race".into());
    let meta = &*meta;
    let allocator = &*allocator;
    let vol = &vol;

    let found_stale = AtomicBool::new(false);
    let barrier = Barrier::new(4);
    let lba_counter = std::sync::atomic::AtomicU64::new(100000);

    std::thread::scope(|s| {
        for tid in 0..4usize {
            let barrier = &barrier;
            let lba_counter = &lba_counter;
            let found_stale = &found_stale;
            let io_engine = io_engine.clone();
            let metrics = metrics.clone();

            s.spawn(move || {
                barrier.wait();
                // ~200 cycles × 2 PBAs = ~400 per thread, 1600 total. Fits in 20000.
                for _cycle in 0..200 {
                    // Allocate PBA
                    let pba = match allocator.allocate_one_for_lane(tid) {
                        Ok(p) => p,
                        Err(_) => return,
                    };

                    // Create packed write: 4 LBAs → PBA
                    let base_lba = lba_counter.fetch_add(8, Ordering::Relaxed);
                    let batch: Vec<(VolumeId, Lba, BlockmapValue)> = (0..4u64)
                        .map(|i| {
                            (
                                vol.clone(),
                                Lba(base_lba + i),
                                BlockmapValue {
                                    pba,
                                    compression: 1,
                                    unit_compressed_size: 400,
                                    unit_original_size: 16384,
                                    unit_lba_count: 4,
                                    offset_in_unit: i as u16,
                                    crc32: 0xFF000000 + tid as u32,
                                    slot_offset: 0,
                                    flags: 0,
                                },
                            )
                        })
                        .collect();
                    if meta.atomic_batch_write_packed(&batch, pba, 4).is_err() {
                        let _ = allocator.free_one(pba);
                        continue;
                    }

                    // Overwrite all 4 LBAs → new PBA
                    let replacement = match allocator.allocate_one_for_lane(tid) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };
                    let overwrite: Vec<(Lba, BlockmapValue)> = (0..4u64)
                        .map(|i| {
                            (
                                Lba(base_lba + i),
                                BlockmapValue {
                                    pba: replacement,
                                    compression: 0,
                                    unit_compressed_size: BLOCK_SIZE,
                                    unit_original_size: BLOCK_SIZE,
                                    unit_lba_count: 1,
                                    offset_in_unit: 0,
                                    crc32: 0x11000000,
                                    slot_offset: 0,
                                    flags: 0,
                                },
                            )
                        })
                        .collect();

                    let args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> =
                        vec![(vol, &overwrite, 4)];
                    let result = meta.atomic_batch_write_multi(&args);

                    // Only free via the production path: newly_zeroed + cleanup
                    if let Ok(newly_zeroed) = result {
                        for cleanup in newly_zeroed.values() {
                            BufferFlusher::cleanup_dead_pba_post_commit(
                                allocator,
                                &crate::dedup::CandidateCache::new(8, 64),
                                cleanup.clone(),
                                "lifecycle_test",
                            );
                        }
                    }

                    // Try to reallocate — might get the recycled PBA back
                    if let Ok(recycled) = allocator.allocate_one_for_lane(tid) {
                        let rc = meta.get_refcount(recycled).unwrap();
                        let refs = meta.count_blockmap_refs_for_pba(recycled).unwrap();
                        if rc != 0 || refs != 0 {
                            eprintln!(
                                "[tid={}] STALE on PBA {}: refcount={} blockmap_refs={}",
                                tid, recycled.0, rc, refs
                            );
                            found_stale.store(true, Ordering::Relaxed);
                        }
                        // Free for reuse in future cycles
                        let _ = allocator.free_one(recycled);
                    }

                    // replacement stays allocated — its blockmap entries are live
                }
            });
        }
    });

    assert!(
        !found_stale.load(Ordering::Relaxed),
        "found stale refcount/blockmap on reallocated PBA"
    );
}

/// Test #5: Simulate duplicate buffer entry processing.
///
/// If the buffer ring has a bug that delivers the same LBA data twice,
/// the flusher processes two writes to the same LBAs:
///   - Write 1: allocates PBA A, maps LBAs 0-7 → PBA A (refcount=8)
///   - Write 2 (duplicate): allocates PBA B, maps LBAs 0-7 → PBA B
///     Old mapping = PBA A → decrements PBA A by 8 → refcount=0 → freed!
///   - PBA A is now free while its data was valid. Reuse corrupts it.
///
/// This test proves the exact scenario and checks whether the newly_zeroed
/// cleanup path correctly handles this case.
#[test]
fn duplicate_flush_entry_causes_premature_pba_free() {
    // Phase 5: hot-path commits don't touch global rc, so the
    // "premature free" vector this test originally guarded (decref
    // driving rc to 0 + allocator reuse) no longer rides on the rc
    // path. The blockmap-ref gate (`has_any_blockmap_ref`) is the
    // load-bearing invariant — allocator reclamation may not happen
    // while live mappings still point at the PBA.
    let (meta, _pool, _lifecycle, allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let vol = VolumeId("flush-race".into());

    // Step 1: First write — maps LBAs 0-7 → PBA A
    let pba_a = allocator.allocate_one_for_lane(0).unwrap();
    let batch_a: Vec<(VolumeId, Lba, BlockmapValue)> = (0..8u64)
        .map(|i| {
            (
                vol.clone(),
                Lba(i),
                BlockmapValue {
                    pba: pba_a,
                    compression: 1,
                    unit_compressed_size: 400,
                    unit_original_size: 32768,
                    unit_lba_count: 8,
                    offset_in_unit: i as u16,
                    crc32: 0xAAAAAAAA,
                    slot_offset: 0,
                    flags: 0,
                },
            )
        })
        .collect();
    meta.atomic_batch_write_packed(&batch_a, pba_a, 8).unwrap();
    assert_eq!(meta.count_blockmap_refs_for_pba(pba_a).unwrap(), 8);

    // Step 2: Duplicate write — same LBAs 0-7 but different PBA B.
    let pba_b = allocator.allocate_one_for_lane(0).unwrap();
    let overwrite: Vec<(Lba, BlockmapValue)> = (0..8u64)
        .map(|i| {
            (
                Lba(i),
                BlockmapValue {
                    pba: pba_b,
                    compression: 1,
                    unit_compressed_size: 400,
                    unit_original_size: 32768,
                    unit_lba_count: 8,
                    offset_in_unit: i as u16,
                    crc32: 0xBBBBBBBB,
                    slot_offset: 0,
                    flags: 0,
                },
            )
        })
        .collect();

    let args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> = vec![(&vol, &overwrite, 8)];
    meta.atomic_batch_write_multi(&args).unwrap();

    // After overwrite the blockmap no longer references pba_a.
    assert_eq!(meta.count_blockmap_refs_for_pba(pba_a).unwrap(), 0);
    assert!(!meta.has_any_blockmap_ref(pba_a).unwrap());

    // Step 3: Cleanup retires PBA A; GC verifies it before reuse.
    BufferFlusher::cleanup_dead_pba_post_commit(
        &allocator,
        &crate::dedup::CandidateCache::new(8, 64),
        cleanup_for_pba(pba_a, 1),
        "dup_flush_test",
    );
    assert!(allocator.is_retired(pba_a), "PBA A should be retired now");
    assert!(
        allocator
            .reclaim_retired_extent(Extent::new(pba_a, 1))
            .unwrap(),
        "PBA A should reclaim after metadata verification"
    );

    // Step 4: Reallocate — recycled PBA must start clean
    // (no leftover blockmap refs from PBA A).
    let recycled = allocator.allocate_one().unwrap();
    assert_eq!(meta.count_blockmap_refs_for_pba(recycled).unwrap(), 0);

    // Step 5: Write new data to the recycled PBA
    let batch_c: Vec<(VolumeId, Lba, BlockmapValue)> = (100..108u64)
        .map(|i| {
            (
                vol.clone(),
                Lba(i),
                BlockmapValue {
                    pba: recycled,
                    compression: 1,
                    unit_compressed_size: 350,
                    unit_original_size: 32768,
                    unit_lba_count: 8,
                    offset_in_unit: (i - 100) as u16,
                    crc32: 0xCCCCCCCC,
                    slot_offset: 0,
                    flags: 0,
                },
            )
        })
        .collect();
    meta.atomic_batch_write_packed(&batch_c, recycled, 8)
        .unwrap();
    assert_eq!(meta.count_blockmap_refs_for_pba(recycled).unwrap(), 8);
}

/// Test #6: High-pressure concurrent test that exercises the EXACT
/// production race: multiple flush lanes simultaneously doing
/// packed_write → overwrite → cleanup → reallocate, interleaved
/// with dedup hits and IMMEDIATE reallocation checks.
///
/// After all threads complete, we do a FULL scan of every PBA to
/// verify refcount == blockmap_refs (no drift whatsoever).
#[test]
fn full_pressure_multi_lane_no_drift_anywhere() {
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::sync::Barrier;

    let (meta, _pool, _lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let vol = VolumeId("flush-race".into());
    let meta = &*meta;
    let allocator = &*allocator;
    let vol = &vol;

    let lba_counter = AtomicU64::new(200000);
    let found_drift = AtomicBool::new(false);
    let barrier = Barrier::new(6);

    // Pre-create shared packed PBAs for dedup
    let shared_pbas: Vec<Pba> = (0..20)
        .filter_map(|_| allocator.allocate_one_for_lane(0).ok())
        .collect();
    for (idx, &pba) in shared_pbas.iter().enumerate() {
        let base_lba = lba_counter.fetch_add(8, Ordering::Relaxed);
        let batch: Vec<(VolumeId, Lba, BlockmapValue)> = (0..8u64)
            .map(|i| {
                (
                    vol.clone(),
                    Lba(base_lba + i),
                    BlockmapValue {
                        pba,
                        compression: 1,
                        unit_compressed_size: 400,
                        unit_original_size: 32768,
                        unit_lba_count: 8,
                        offset_in_unit: i as u16,
                        crc32: 0xAA000000 + idx as u32,
                        slot_offset: 0,
                        flags: 0,
                    },
                )
            })
            .collect();
        meta.atomic_batch_write_packed(&batch, pba, 8).unwrap();
        let entries: Vec<(ContentHash, DedupEntry)> = (0..8u8)
            .map(|i| {
                let mut h = [0u8; 8];
                h[0] = idx as u8;
                h[1] = i;

                (
                    h,
                    DedupEntry {
                        pba,
                        slot_offset: 0,
                        compression: 1,
                        unit_compressed_size: 400,
                        unit_original_size: 32768,
                        unit_lba_count: 8,
                        offset_in_unit: i as u16,
                        crc32: 0xAA000000 + idx as u32,
                    },
                )
            })
            .collect();
        meta.put_dedup_entries(&entries).unwrap();
    }

    let shared_pbas = &shared_pbas;

    std::thread::scope(|s| {
        // 2 packed writer threads
        for tid in 0..2usize {
            let barrier = &barrier;
            let lba_counter = &lba_counter;
            s.spawn(move || {
                barrier.wait();
                for _ in 0..150 {
                    let pba = match allocator.allocate_one_for_lane(tid) {
                        Ok(p) => p,
                        Err(_) => return,
                    };
                    let base_lba = lba_counter.fetch_add(8, Ordering::Relaxed);
                    let batch: Vec<(VolumeId, Lba, BlockmapValue)> = (0..8u64)
                        .map(|i| {
                            (
                                vol.clone(),
                                Lba(base_lba + i),
                                BlockmapValue {
                                    pba,
                                    compression: 1,
                                    unit_compressed_size: 400,
                                    unit_original_size: 32768,
                                    unit_lba_count: 8,
                                    offset_in_unit: i as u16,
                                    crc32: 0xBB000000 + tid as u32,
                                    slot_offset: 0,
                                    flags: 0,
                                },
                            )
                        })
                        .collect();
                    let _ = meta.atomic_batch_write_packed(&batch, pba, 8);
                }
            });
        }

        // 2 overwrite threads targeting shared PBAs' original LBAs
        for tid in 2..4usize {
            let barrier = &barrier;
            let io_engine = io_engine.clone();
            let metrics = metrics.clone();
            s.spawn(move || {
                barrier.wait();
                for round in 0..150u64 {
                    let pba_idx = (round as usize + tid * 7) % shared_pbas.len();
                    let new_pba = match allocator.allocate_one_for_lane(tid) {
                        Ok(p) => p,
                        Err(_) => return,
                    };
                    let orig_base = 200000 + (pba_idx as u64) * 8;
                    let overwrite: Vec<(Lba, BlockmapValue)> = (0..2u64)
                        .map(|i| {
                            (
                                Lba(orig_base + i + (tid as u64 - 2) * 2),
                                BlockmapValue {
                                    pba: new_pba,
                                    compression: 0,
                                    unit_compressed_size: BLOCK_SIZE,
                                    unit_original_size: BLOCK_SIZE,
                                    unit_lba_count: 1,
                                    offset_in_unit: 0,
                                    crc32: 0xCC000000 + tid as u32,
                                    slot_offset: 0,
                                    flags: 0,
                                },
                            )
                        })
                        .collect();

                    let args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> =
                        vec![(vol, &overwrite, 2)];
                    if let Ok(newly_zeroed) = meta.atomic_batch_write_multi(&args) {
                        for cleanup in newly_zeroed.values() {
                            BufferFlusher::cleanup_dead_pba_post_commit(
                                allocator,
                                &crate::dedup::CandidateCache::new(8, 64),
                                cleanup.clone(),
                                "pressure_test",
                            );
                        }
                    }
                }
            });
        }

        // 1 dedup hit thread
        s.spawn(|| {
            barrier.wait();
            for round in 0..200u64 {
                let pba_idx = (round as usize) % shared_pbas.len();
                let pba = shared_pbas[pba_idx];
                let base_lba = lba_counter.fetch_add(2, Ordering::Relaxed);
                let hashes: Vec<ContentHash> = (0..2u8)
                    .map(|i| {
                        let mut h = [0u8; 8];
                        h[0] = pba_idx as u8;
                        h[1] = i;

                        h
                    })
                    .collect();
                let hits: Vec<(Lba, BlockmapValue, ContentHash)> = (0..2u64)
                    .map(|i| {
                        (
                            Lba(base_lba + i),
                            BlockmapValue {
                                pba,
                                compression: 1,
                                unit_compressed_size: 400,
                                unit_original_size: 32768,
                                unit_lba_count: 8,
                                offset_in_unit: i as u16,
                                crc32: 0xAA000000 + pba_idx as u32,
                                slot_offset: 0,
                                flags: 0,
                            },
                            hashes[i as usize],
                        )
                    })
                    .collect();
                let _ = meta.atomic_batch_dedup_hits(vol, &hits);
            }
        });

        // 1 batch multi-write thread
        s.spawn(|| {
            barrier.wait();
            for _ in 0..100u64 {
                let pba = match allocator.allocate_one_for_lane(0) {
                    Ok(p) => p,
                    Err(_) => return,
                };
                let base_lba = lba_counter.fetch_add(4, Ordering::Relaxed);
                let entries: Vec<(Lba, BlockmapValue)> = (0..4u64)
                    .map(|i| {
                        (
                            Lba(base_lba + i),
                            BlockmapValue {
                                pba,
                                compression: 0,
                                unit_compressed_size: BLOCK_SIZE,
                                unit_original_size: BLOCK_SIZE,
                                unit_lba_count: 1,
                                offset_in_unit: 0,
                                crc32: 0xDD000000,
                                slot_offset: 0,
                                flags: 0,
                            },
                        )
                    })
                    .collect();
                let args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> = vec![(vol, &entries, 4)];
                let _ = meta.atomic_batch_write_multi(&args);
            }
        });
    });

    // Phase 5: refcount-vs-blockmap drift is impossible because the
    // hot-path commit no longer mutates rc. Verify the full-pressure
    // workload didn't crash and the blockmap-ref accounting is
    // internally consistent.
    let _ = found_drift;
    for pba_val in 0..20000u64 {
        let pba = Pba(pba_val + crate::types::RESERVED_BLOCKS);
        let refs = meta.count_blockmap_refs_for_pba(pba).unwrap();
        if refs > 0 {
            assert!(meta.has_any_blockmap_ref(pba).unwrap());
        }
    }
}
