use super::*;

#[test]
fn dedup_hit_cleanup_deduplicates_repeated_old_pbas() {
    let (meta, _pool, _lifecycle, _allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let vol = VolumeId("flush-race".into());
    let old_pba = Pba(100);
    let new_pba = Pba(200);

    for lba in 0..3u64 {
        meta.put_mapping(
            &vol,
            Lba(lba),
            &BlockmapValue {
                pba: old_pba,
                compression: 0,
                unit_compressed_size: BLOCK_SIZE,
                unit_original_size: BLOCK_SIZE,
                unit_lba_count: 1,
                offset_in_unit: 0,
                crc32: 0,
                slot_offset: 0,
                flags: 0,
            },
        )
        .unwrap();
    }
    meta.set_refcount(old_pba, 3).unwrap();
    meta.set_refcount(new_pba, 8).unwrap();

    // Each LBA needs a forward dedup entry for the hit path.
    let hash_0: ContentHash = [0x01; 8];
    let hash_1: ContentHash = [0x02; 8];
    let hash_2: ContentHash = [0x03; 8];
    meta.put_dedup_entries(&[
        (
            hash_0,
            DedupEntry {
                pba: new_pba,
                slot_offset: 0,
                compression: 0,
                unit_compressed_size: BLOCK_SIZE,
                unit_original_size: BLOCK_SIZE,
                unit_lba_count: 1,
                offset_in_unit: 0,
                crc32: 0,
            },
        ),
        (
            hash_1,
            DedupEntry {
                pba: new_pba,
                slot_offset: 0,
                compression: 0,
                unit_compressed_size: BLOCK_SIZE,
                unit_original_size: BLOCK_SIZE,
                unit_lba_count: 1,
                offset_in_unit: 0,
                crc32: 0,
            },
        ),
        (
            hash_2,
            DedupEntry {
                pba: new_pba,
                slot_offset: 0,
                compression: 0,
                unit_compressed_size: BLOCK_SIZE,
                unit_original_size: BLOCK_SIZE,
                unit_lba_count: 1,
                offset_in_unit: 0,
                crc32: 0,
            },
        ),
    ])
    .unwrap();

    let (_results, newly_zeroed) = meta
        .atomic_batch_dedup_hits(
            &vol,
            &[
                (
                    Lba(0),
                    BlockmapValue {
                        pba: new_pba,
                        compression: 0,
                        unit_compressed_size: BLOCK_SIZE,
                        unit_original_size: BLOCK_SIZE,
                        unit_lba_count: 1,
                        offset_in_unit: 0,
                        crc32: 0,
                        slot_offset: 0,
                        flags: 0,
                    },
                    hash_0,
                ),
                (
                    Lba(1),
                    BlockmapValue {
                        pba: new_pba,
                        compression: 0,
                        unit_compressed_size: BLOCK_SIZE,
                        unit_original_size: BLOCK_SIZE,
                        unit_lba_count: 1,
                        offset_in_unit: 0,
                        crc32: 0,
                        slot_offset: 0,
                        flags: 0,
                    },
                    hash_1,
                ),
                (
                    Lba(2),
                    BlockmapValue {
                        pba: new_pba,
                        compression: 0,
                        unit_compressed_size: BLOCK_SIZE,
                        unit_original_size: BLOCK_SIZE,
                        unit_lba_count: 1,
                        offset_in_unit: 0,
                        crc32: 0,
                        slot_offset: 0,
                        flags: 0,
                    },
                    hash_2,
                ),
            ],
        )
        .unwrap();

    // All 3 hits replace old_pba mappings. old_pba had refcount=3, all 3
    // decremented → refcount=0 → newly_zeroed should contain old_pba.
    assert_eq!(newly_zeroed.len(), 1);
    assert!(newly_zeroed.contains_key(&old_pba));
}

#[test]
fn dedup_worker_cleanup_can_race_with_scanner_cleanup_on_same_dead_pba() {
    let (meta, _pool, _lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();

    let pba = allocator.allocate_one_for_lane(0).unwrap();
    let payload = vec![0xAB; BLOCK_SIZE as usize];
    io_engine.write_blocks(pba, &payload).unwrap();
    let hash: ContentHash = crate::meta::schema::compute_content_hash(&payload);
    let old_mapping = BlockmapValue {
        pba,
        compression: 0,
        unit_compressed_size: BLOCK_SIZE,
        unit_original_size: BLOCK_SIZE,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: crc32fast::hash(&payload),
        slot_offset: 0,
        flags: 0,
    };
    meta.put_dedup_entries(&[(
        hash,
        DedupEntry {
            pba,
            slot_offset: old_mapping.slot_offset,
            compression: old_mapping.compression,
            unit_compressed_size: old_mapping.unit_compressed_size,
            unit_original_size: old_mapping.unit_original_size,
            unit_lba_count: old_mapping.unit_lba_count,
            offset_in_unit: old_mapping.offset_in_unit,
            crc32: old_mapping.crc32,
        },
    )])
    .unwrap();
    // Phase 5: `WalOp::DedupPut` increfs the head PBA's rc by 1 to
    // represent the "shared via dedup_index" reference.
    assert_eq!(meta.get_refcount(pba).unwrap(), 1);

    let allocator_scanner = allocator.clone();
    let scanner_cleanup = RemapCleanup {
        mappings: vec![old_mapping],
        ..cleanup_for_pba(pba, 1)
    };
    let worker_cleanup = scanner_cleanup.clone();
    let (ready_tx, ready_rx) = bounded::<()>(1);
    let (resume_tx, resume_rx) = bounded::<()>(1);

    let scanner = thread::spawn(move || {
        ready_tx.send(()).unwrap();
        resume_rx.recv().unwrap();
        BufferFlusher::cleanup_dead_pba_post_commit(
            &allocator_scanner,
            &crate::dedup::CandidateCache::new(8, 64),
            scanner_cleanup,
            "dedup_scanner_cleanup",
        );
    });

    ready_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("scanner-style cleanup should reach allocator handoff");

    BufferFlusher::cleanup_dead_pba_post_commit(
        &allocator,
        &crate::dedup::CandidateCache::new(8, 64),
        worker_cleanup,
        "dedup_worker_hit_cleanup",
    );

    resume_tx.send(()).unwrap();
    scanner.join().unwrap();
    // `dedup_index` is a verified cache now: allocator cleanup must not
    // try to reconstruct hashes from the old PBA. Stale forward entries
    // are repaired by foreground compare-put or background scrub.
    assert!(
        allocator.is_retired(pba),
        "PBA should be retired after cleanup"
    );
    assert_eq!(
        meta.get_dedup_entry(&hash).unwrap(),
        Some(old_mapping.to_dedup_entry())
    );
}

#[test]
fn dedup_cleanup_does_not_reconstruct_old_pba_for_index_delete() {
    let (meta, _pool, _lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();

    let pba = allocator.allocate_one_for_lane(0).unwrap();
    let payload = vec![0xCD; BLOCK_SIZE as usize];
    io_engine.write_blocks(pba, &payload).unwrap();
    let hash: ContentHash = crate::meta::schema::compute_content_hash(&payload);
    let mapping = BlockmapValue {
        pba,
        compression: 0,
        unit_compressed_size: BLOCK_SIZE,
        unit_original_size: BLOCK_SIZE,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0xDEADBEEF,
        slot_offset: 0,
        flags: 0,
    };
    meta.put_dedup_entries(&[(hash, mapping.to_dedup_entry())])
        .unwrap();

    BufferFlusher::cleanup_dead_pba_post_commit(
        &allocator,
        &crate::dedup::CandidateCache::new(8, 64),
        RemapCleanup {
            mappings: vec![mapping],
            ..cleanup_for_pba(pba, 1)
        },
        "dedup_cleanup_reconstruct_failure_test",
    );

    assert!(
        meta.get_dedup_entry(&hash).unwrap().is_some(),
        "allocator cleanup must leave dedup_index hints to verified repair/scrub"
    );
    assert_eq!(
        metrics
            .dedup_cleanup_reconstruct_errors
            .load(Ordering::Relaxed),
        0
    );
    assert!(
        allocator.is_retired(pba),
        "allocator cleanup should retire the PBA for GC reclaim"
    );
}

#[test]
fn retired_reclaimer_skips_allocator_free_when_live_blockmap_ref_remains() {
    let (meta, _pool, _lifecycle, allocator, _io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();

    let pba = allocator.allocate_one_for_lane(0).unwrap();
    let mapping = BlockmapValue {
        pba,
        compression: 0,
        unit_compressed_size: BLOCK_SIZE,
        unit_original_size: BLOCK_SIZE,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0x1234_5678,
        slot_offset: 0,
        flags: 0,
    };
    let vol = VolumeId("flush-race".into());
    meta.atomic_batch_write(&vol, &[(Lba(1234), mapping)], 1)
        .unwrap();
    meta.set_refcount(pba, 0).unwrap();

    BufferFlusher::cleanup_dead_pba_post_commit(
        &allocator,
        &crate::dedup::CandidateCache::new(8, 64),
        RemapCleanup {
            mappings: vec![mapping],
            ..cleanup_for_pba(pba, 1)
        },
        "live_ref_guard_test",
    );

    assert!(
        !allocator.is_free(pba),
        "cleanup must not return a live-referenced PBA to the allocator"
    );
    assert!(
        allocator.is_retired(pba),
        "cleanup only retires the PBA; final verification belongs to GC"
    );
    assert_eq!(
        crate::gc::runner::GcRunner::reclaim_retired_extents(
            &metrics,
            &meta,
            &allocator,
            16,
            &AtomicBool::new(true),
        ),
        0,
        "GC must refuse to reclaim while blockmap still references the PBA"
    );
    assert!(
        !allocator.is_free(pba),
        "live-referenced retired PBA must not be returned to allocator"
    );
}

#[test]
fn retired_reclaimer_batches_one_scan_and_frees_only_unreferenced() {
    let (meta, _pool, _lifecycle, allocator, _io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();

    // Two rc==0 retired PBAs, kept non-adjacent by an allocated gap block so
    // the allocator does not coalesce them into one extent: one is still
    // referenced by a live blockmap entry, the other is not. Batched reclaim
    // must free exactly the unreferenced one, in a SINGLE all-volume scan.
    let referenced_pba = allocator.allocate_one_for_lane(0).unwrap();
    let _gap = allocator.allocate_one_for_lane(0).unwrap();
    let free_pba = allocator.allocate_one_for_lane(0).unwrap();
    assert_ne!(referenced_pba.0 + 1, free_pba.0, "test needs a non-adjacent gap");

    let mapping = BlockmapValue {
        pba: referenced_pba,
        compression: 0,
        unit_compressed_size: BLOCK_SIZE,
        unit_original_size: BLOCK_SIZE,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0x1234_5678,
        slot_offset: 0,
        flags: 0,
    };
    let vol = VolumeId("flush-race".into());
    meta.atomic_batch_write(&vol, &[(Lba(7), mapping)], 1).unwrap();
    meta.set_refcount(referenced_pba, 0).unwrap();
    meta.set_refcount(free_pba, 0).unwrap();

    allocator.retire_one(referenced_pba).unwrap();
    allocator.retire_one(free_pba).unwrap();

    let scans_before = metrics
        .gc_reclaim_blockmap_scans
        .load(std::sync::atomic::Ordering::Relaxed);
    let reclaimed = crate::gc::runner::GcRunner::reclaim_retired_extents(
        &metrics,
        &meta,
        &allocator,
        64,
        &AtomicBool::new(true),
    );

    assert_eq!(reclaimed, 1, "only the unreferenced rc==0 extent is reclaimed");
    assert!(
        allocator.is_free(free_pba),
        "unreferenced rc==0 PBA returned to allocator"
    );
    assert!(
        !allocator.is_free(referenced_pba) && allocator.is_retired(referenced_pba),
        "live-referenced PBA must stay retired, not freed"
    );
    assert_eq!(
        metrics
            .gc_reclaim_blockmap_scans
            .load(std::sync::atomic::Ordering::Relaxed)
            - scans_before,
        1,
        "all survivors checked in a single batched all-volume scan"
    );
}

#[test]
fn duplicate_dead_pba_cleanup_callers_without_shared_lock_double_free() {
    let (meta, _pool, _lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();

    let pba = allocator.allocate_one_for_lane(0).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(3));

    let run_cleanup =
        |meta: Arc<MetaStore>, allocator: Arc<SpaceAllocator>, barrier: Arc<std::sync::Barrier>| {
            thread::spawn(move || {
                assert_eq!(meta.get_refcount(pba).unwrap(), 0);
                barrier.wait();
                allocator.free_one(pba)
            })
        };

    let t1 = run_cleanup(meta.clone(), allocator.clone(), barrier.clone());
    let t2 = run_cleanup(meta.clone(), allocator.clone(), barrier.clone());
    barrier.wait();

    let results = [t1.join().unwrap(), t2.join().unwrap()];
    let ok = results.iter().filter(|r| r.is_ok()).count();
    // The losing caller hits SpaceAllocator's post-hazard-wait
    // detection, which formats the error as "overlaps free extent
    // ... after hazard wait". Either phrase ("overlaps free extent"
    // or the older "already free") signals double-free was caught.
    let already_free = results
        .iter()
        .filter_map(|r| r.as_ref().err())
        .filter(|e| {
            let msg = e.to_string();
            msg.contains("already free") || msg.contains("overlaps free extent")
        })
        .count();

    assert_eq!(ok, 1, "exactly one cleanup caller should win the free");
    assert_eq!(
        already_free, 1,
        "the second cleanup caller should hit allocator already-free"
    );
}

#[test]
fn dedup_worker_batches_hits_across_units() {
    let (meta, pool, lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    pool.durable_seq_handle().store(u64::MAX, Ordering::Release);

    let source = vec![0x7Bu8; BLOCK_SIZE as usize];
    let hash: ContentHash = crate::meta::schema::compute_content_hash(&source);
    let target = BlockmapValue {
        pba: Pba(77),
        compression: 0,
        unit_compressed_size: BLOCK_SIZE,
        unit_original_size: BLOCK_SIZE,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: crc32fast::hash(&source),
        slot_offset: 0,
        flags: 0,
    };
    meta.set_refcount(target.pba, 1).unwrap();
    meta.put_dedup_entries(&[(
        hash,
        DedupEntry {
            pba: target.pba,
            slot_offset: target.slot_offset,
            compression: target.compression,
            unit_compressed_size: target.unit_compressed_size,
            unit_original_size: target.unit_original_size,
            unit_lba_count: target.unit_lba_count,
            offset_in_unit: target.offset_in_unit,
            crc32: target.crc32,
        },
    )])
    .unwrap();

    let (dedup_tx, dedup_rx) = bounded::<CoalesceUnit>(128);
    let (miss_tx, miss_rx) = bounded::<CoalesceUnit>(128);
    let (done_tx, done_rx) = unbounded::<Vec<u64>>();
    let (cleanup_tx, _cleanup_rx) = unbounded::<CleanupBatch>();
    let running = AtomicBool::new(true);

    for i in 0..8u64 {
        let lba = Lba(10_000 + i);
        pool.note_latest_lba_seq_for_test("flush-race", lba, i, 1);
        dedup_tx
            .send(CoalesceUnit {
                vol_id: "flush-race".into(),
                start_lba: lba,
                lba_count: 1,
                raw_blocks: vec![crate::buffer::pipeline::RawBlockRef {
                    payload: Arc::from(source.clone()),
                    offset: 0,
                }],
                compression: CompressionAlgo::None,
                vol_created_at: 1,
                seq_lba_ranges: vec![(i, lba, 1)],
                dedup_skipped: false,
                block_hashes: None,
                dedup_stale_repairs: None,
                dedup_completion: None,
            })
            .unwrap();
    }
    drop(dedup_tx);

    let candidate = crate::dedup::CandidateCache::new(8, 64);
    BufferFlusher::dedup_loop(
        0,
        &dedup_rx,
        &miss_tx,
        &meta,
        &pool,
        &lifecycle,
        &allocator,
        &done_tx,
        &running,
        100,
        0,
        &metrics,
        &cleanup_tx,
        &candidate,
        // No verify in this unit-test path: hits trust hash. Tests
        // that need verify should construct their own ReadPool.
        None,
    );
    drop(miss_tx);

    assert!(
        miss_rx.is_empty(),
        "all duplicate blocks should be handled as metadata-only hits"
    );

    let mut done = Vec::new();
    while let Ok(seqs) = done_rx.try_recv() {
        done.extend(seqs);
    }
    done.sort_unstable();
    assert_eq!(done, (0u64..8).collect::<Vec<_>>());

    for i in 0..8u64 {
        let mapping = meta
            .get_mapping(&VolumeId("flush-race".into()), Lba(10_000 + i))
            .unwrap()
            .unwrap();
        assert_eq!(mapping.pba, target.pba);
    }

    let snap = metrics.snapshot();
    assert_eq!(snap.dedup_hits, 8);
    assert_eq!(snap.dedup_misses, 0);
    assert_eq!(snap.dedup_hit_commit_ops, 8);
    assert_eq!(
        meta.memory_stats().unwrap().commit_attempts,
        2,
        "setup: put_dedup_entries (set_refcount now routes via lifecycle journal, \
         not commit_ops); the 8 hits should share one commit"
    );
}
