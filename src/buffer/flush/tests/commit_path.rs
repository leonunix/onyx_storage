use super::*;

#[test]
fn old_write_unit_can_overwrite_newer_committed_mapping() {
    let (meta, pool, lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let (cleanup_tx, _cleanup_rx) = unbounded::<CleanupBatch>();

    let newer = make_unit(0x33, 2);
    pool.note_latest_lba_seq_for_test("flush-race", Lba(0), 2, 1);
    BufferFlusher::write_unit(
        0,
        &newer,
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        &metrics,
        &cleanup_tx,
        &crate::dedup::CandidateCache::new(8, 64),
    )
    .unwrap();

    let older = make_unit(0x11, 1);
    BufferFlusher::write_unit(
        0,
        &older,
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        &metrics,
        &cleanup_tx,
        &crate::dedup::CandidateCache::new(8, 64),
    )
    .unwrap();

    let worker = ZoneWorker::new(ZoneId(0), meta.clone(), pool.clone(), io_engine.clone());
    let actual = worker.handle_read("flush-race", Lba(0)).unwrap().unwrap();

    assert_eq!(
        actual,
        vec![0x33; BLOCK_SIZE as usize],
        "older write committed after newer write must not win",
    );
}

#[test]
fn raw_passthrough_unit_maps_each_lba_to_its_own_pba() {
    let (meta, pool, lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let (cleanup_tx, _cleanup_rx) = unbounded::<CleanupBatch>();

    let unit = make_raw_unit_at(100, 4, 0x40, 10);
    pool.note_latest_lba_seq_for_test("flush-race", Lba(100), 10, 1);
    pool.note_latest_lba_seq_for_test("flush-race", Lba(101), 10, 1);
    pool.note_latest_lba_seq_for_test("flush-race", Lba(102), 10, 1);
    pool.note_latest_lba_seq_for_test("flush-race", Lba(103), 10, 1);

    BufferFlusher::write_unit(
        0,
        &unit,
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        &metrics,
        &cleanup_tx,
        &crate::dedup::CandidateCache::new(8, 64),
    )
    .unwrap();

    let vol = VolumeId("flush-race".into());
    let first = meta.get_mapping(&vol, Lba(100)).unwrap().unwrap();
    assert_eq!(first.unit_compressed_size, BLOCK_SIZE);
    assert_eq!(first.unit_original_size, BLOCK_SIZE);
    assert_eq!(first.unit_lba_count, 1);
    assert_eq!(first.offset_in_unit, 0);
    assert_eq!(
        first.crc32,
        crc32fast::hash(&vec![0x40; BLOCK_SIZE as usize])
    );
    assert_eq!(
        first.compressed_read_size(BLOCK_SIZE as usize),
        BLOCK_SIZE as usize
    );

    for idx in 0..4u64 {
        let mapping = meta.get_mapping(&vol, Lba(100 + idx)).unwrap().unwrap();
        assert_eq!(mapping.pba, Pba(first.pba.0 + idx));
        assert_eq!(mapping.compression, 0);
        assert_eq!(mapping.unit_compressed_size, BLOCK_SIZE);
        assert_eq!(mapping.unit_original_size, BLOCK_SIZE);
        assert_eq!(mapping.unit_lba_count, 1);
        assert_eq!(mapping.offset_in_unit, 0);
        assert_eq!(mapping.slot_offset, 0);
        // Phase 5: hot-path write_unit no longer bumps global rc.
        // The allocator still tracks each PBA as allocated (verify
        // via has_any_blockmap_ref / iter_allocated_blocks).
        assert_eq!(meta.get_refcount(mapping.pba).unwrap(), 0);
        assert!(meta.has_any_blockmap_ref(mapping.pba).unwrap());

        let got = ZoneWorker::new(ZoneId(0), meta.clone(), pool.clone(), io_engine.clone())
            .handle_read("flush-race", Lba(100 + idx))
            .unwrap()
            .unwrap();
        assert_eq!(
            got,
            vec![0x40u8.wrapping_add(idx as u8); BLOCK_SIZE as usize]
        );
    }
}

#[test]
fn raw_passthrough_unit_frees_unreferenced_blocks() {
    let (meta, pool, lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let (cleanup_tx, _cleanup_rx) = unbounded::<CleanupBatch>();

    let unit = make_raw_unit_at(200, 4, 0x60, 20);
    pool.note_latest_lba_seq_for_test("flush-race", Lba(200), 20, 1);
    pool.note_latest_lba_seq_for_test("flush-race", Lba(201), 999, 1);
    pool.note_latest_lba_seq_for_test("flush-race", Lba(202), 20, 1);
    pool.note_latest_lba_seq_for_test("flush-race", Lba(203), 999, 1);

    BufferFlusher::write_unit(
        0,
        &unit,
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        &metrics,
        &cleanup_tx,
        &crate::dedup::CandidateCache::new(8, 64),
    )
    .unwrap();

    let vol = VolumeId("flush-race".into());
    let mapping0 = meta.get_mapping(&vol, Lba(200)).unwrap().unwrap();
    let mapping2 = meta.get_mapping(&vol, Lba(202)).unwrap().unwrap();
    assert_eq!(mapping2.pba, Pba(mapping0.pba.0 + 2));
    assert!(meta.get_mapping(&vol, Lba(201)).unwrap().is_none());
    assert!(meta.get_mapping(&vol, Lba(203)).unwrap().is_none());

    // Phase 5: hot-path write_unit no longer bumps rc on referenced
    // PBAs; the load-bearing invariant is the allocator-level state.
    assert_eq!(meta.get_refcount(mapping0.pba).unwrap(), 0);
    assert_eq!(meta.get_refcount(Pba(mapping0.pba.0 + 1)).unwrap(), 0);
    assert_eq!(meta.get_refcount(mapping2.pba).unwrap(), 0);
    assert_eq!(meta.get_refcount(Pba(mapping0.pba.0 + 3)).unwrap(), 0);
    assert!(meta.has_any_blockmap_ref(mapping0.pba).unwrap());
    assert!(meta.has_any_blockmap_ref(mapping2.pba).unwrap());
    assert!(allocator.is_free(Pba(mapping0.pba.0 + 1)));
    assert!(allocator.is_free(Pba(mapping0.pba.0 + 3)));
}

#[test]
fn write_unit_routes_first_occurrence_into_candidate_cache() {
    let (meta, pool, lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let (cleanup_tx, _cleanup_rx) = unbounded::<CleanupBatch>();
    let candidate = crate::dedup::CandidateCache::new(8, 64);

    let payload = vec![0x5A; BLOCK_SIZE as usize];
    let hash: ContentHash = crate::meta::schema::compute_content_hash(&payload);
    let seq = pool.append("flush-race", Lba(0), 1, &payload, 1).unwrap();
    let mut unit = make_unit(0x5A, seq);
    unit.block_hashes = Some(vec![hash]);

    BufferFlusher::write_unit(
        0,
        &unit,
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        &metrics,
        &cleanup_tx,
        &candidate,
    )
    .unwrap();

    let mapping = meta
        .get_mapping(&VolumeId("flush-race".into()), Lba(0))
        .unwrap()
        .unwrap();
    // Promote-on-verified-hit invariant: dedup_index is only written
    // after a candidate hit is byte-confirmed by LV3 verify, so a
    // first-occurrence write leaves dedup_index empty.
    assert!(
        meta.get_dedup_entry(&hash).unwrap().is_none(),
        "first-occurrence misses must not write dedup_index"
    );
    // Instead, the (hash, blockmap) lands in the RAM candidate cache.
    assert_eq!(
        candidate.lookup(&hash),
        Some(mapping),
        "first occurrence must register the (hash, blockmap) pair in the candidate cache"
    );
}

#[test]
fn write_packed_slot_routes_first_occurrence_into_candidate_cache() {
    let (meta, pool, lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let (cleanup_tx, _cleanup_rx) = unbounded::<CleanupBatch>();
    let candidate = crate::dedup::CandidateCache::new(8, 64);

    let payload = vec![0x6B; BLOCK_SIZE as usize];
    let hash: ContentHash = crate::meta::schema::compute_content_hash(&payload);
    let seq = pool.append("flush-race", Lba(0), 1, &payload, 1).unwrap();
    let mut unit = make_packed_unit(0x6B, seq);
    unit.block_hashes = Some(vec![hash]);

    let pba = allocator.allocate_one_for_lane(0).unwrap();
    let sealed = SealedSlot {
        pba,
        data: vec![0x6B; BLOCK_SIZE as usize],
        fragments: vec![crate::packer::packer::SlotFragment {
            unit,
            slot_offset: 0,
        }],
    };

    BufferFlusher::write_packed_slot(
        0,
        &sealed,
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        &metrics,
        &cleanup_tx,
        &candidate,
    )
    .unwrap();

    let mapping = meta
        .get_mapping(&VolumeId("flush-race".into()), Lba(0))
        .unwrap()
        .unwrap();
    assert!(
        meta.get_dedup_entry(&hash).unwrap().is_none(),
        "packed first-occurrence misses must not write dedup_index"
    );
    assert_eq!(
        candidate.lookup(&hash),
        Some(mapping),
        "packed first occurrence must register (hash, blockmap) in candidate cache"
    );
}

#[test]
fn write_packed_slots_batch_routes_first_occurrence_into_candidate_cache() {
    let (meta, pool, lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let (cleanup_tx, _cleanup_rx) = unbounded::<CleanupBatch>();
    let candidate = crate::dedup::CandidateCache::new(8, 64);

    let mut sealed_slots = Vec::new();
    let mut expected = Vec::new();
    for (idx, lba) in [10_u64, 20_u64].into_iter().enumerate() {
        let fill = 0x70 + idx as u8;
        let payload = vec![fill; BLOCK_SIZE as usize];
        let hash: ContentHash = crate::meta::schema::compute_content_hash(&payload);
        let seq = pool.append("flush-race", Lba(lba), 1, &payload, 1).unwrap();
        let mut unit = make_packed_unit_at(fill, seq, lba);
        unit.block_hashes = Some(vec![hash]);

        let pba = allocator.allocate_one_for_lane(0).unwrap();
        sealed_slots.push(SealedSlot {
            pba,
            data: vec![fill; BLOCK_SIZE as usize],
            fragments: vec![crate::packer::packer::SlotFragment {
                unit,
                slot_offset: 0,
            }],
        });
        expected.push((hash, Lba(lba)));
    }

    // Phase 1 of the per-volume commit architecture: the metadata
    // commit lives on the commit worker, not the shard writer. Drive
    // the commit path directly via `commit_packed_job` per slot —
    // skips the IO submit but still exercises the candidate-cache
    // fill behaviour the original test asserted.
    let in_flight = std::sync::Arc::new(super::FlusherInFlightTracker::default());
    let (done_tx, _done_rx) = unbounded::<Vec<u64>>();
    // Phase 2.2: spawn a transient post_commit thread so candidate
    // inserts + stale repairs land before the test asserts on them.
    let (post_commit_tx, post_commit_rx) = unbounded::<super::writer::PostCommitJob>();
    let pool_pc = pool.clone();
    let meta_pc = meta.clone();
    let candidate_pc = candidate.clone();
    let metrics_pc = metrics.clone();
    let done_txs_pc = vec![done_tx.clone()];
    let post_commit_handle = std::thread::spawn(move || {
        BufferFlusher::post_commit_loop(
            0,
            &post_commit_rx,
            &pool_pc,
            &meta_pc,
            &candidate_pc,
            &metrics_pc,
            &done_txs_pc,
        );
    });
    for sealed in sealed_slots.into_iter() {
        let job = super::writer::PackedCommitJob {
            sealed,
            shard_idx: 0,
            buffered_seqs: Vec::new(),
            buffered_completions: Vec::new(),
            enqueued_at: Instant::now(),
        };
        BufferFlusher::commit_packed_job(
            job,
            &pool,
            &meta,
            &lifecycle,
            &allocator,
            &in_flight,
            &metrics,
            &cleanup_tx,
            &candidate,
            &done_tx,
            &post_commit_tx,
        );
    }
    drop(post_commit_tx);
    post_commit_handle
        .join()
        .expect("post_commit_loop panicked");
    let _ = io_engine;

    // Promote-on-verified-hit: dedup_index stays empty after a fresh
    // batch; the (hash, blockmap) pairs land only in the candidate
    // cache and are promoted later when the dedup worker confirms a
    // duplicate via LV3 verify.
    for (hash, lba) in &expected {
        let mapping = meta
            .get_mapping(&VolumeId("flush-race".into()), *lba)
            .unwrap()
            .unwrap();
        assert!(
            meta.get_dedup_entry(hash).unwrap().is_none(),
            "packed batch first-occurrence misses must not write dedup_index"
        );
        assert_eq!(
            candidate.lookup(hash),
            Some(mapping),
            "packed batch first occurrence must register (hash, blockmap) in candidate cache"
        );
    }
}

// `write_packed_slots_batch_splits_oversized_metadata_commits` was
// retired with Phase 1 of the per-volume commit architecture: the
// per-slot metadata batch cap (`packed_meta_batch_max_lbas`) is gone.
// Each packed slot now becomes its own `PackedCommitJob` and commits
// alone (one slot = one tx). Sub-batch capping moved to
// `commit_passthrough_job` at TARGET_OPS_PER_COMMIT (=150 LBAs);
// covered by `passthrough_commit_job_subbatches_above_threshold`
// below.

#[test]
fn passthrough_commit_job_subbatches_above_threshold() {
    let (meta, pool, lifecycle, allocator, _io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let (cleanup_tx, _cleanup_rx) = unbounded::<CleanupBatch>();
    let candidate = crate::dedup::CandidateCache::new(8, 64);
    let in_flight = std::sync::Arc::new(super::FlusherInFlightTracker::default());
    let (done_tx, _done_rx) = unbounded::<Vec<u64>>();

    // Build 5 units of 100 LBAs each = 500 LBAs total. With
    // TARGET_OPS_PER_COMMIT=150 the worker should split into 4 sub
    // batches (after 1 unit=100, after 2 units=200>150 → flush; new
    // chunk 1, etc.).
    let lbas_per_unit: u32 = 100;
    let n_units = 5usize;
    let mut units = Vec::with_capacity(n_units);
    for u in 0..n_units {
        let start_lba = Lba((u as u64) * lbas_per_unit as u64);
        let seq = (u as u64) + 1;
        for off in 0..lbas_per_unit {
            pool.note_latest_lba_seq_for_test("flush-race", Lba(start_lba.0 + off as u64), seq, 1);
        }
        let pba = allocator.allocate_one_for_lane(0).unwrap();
        let fill = (u as u8).wrapping_add(1);
        let data = vec![fill; 1];
        let unit = CompressedUnit {
            vol_id: "flush-race".into(),
            start_lba,
            lba_count: lbas_per_unit,
            original_size: BLOCK_SIZE * lbas_per_unit,
            compressed_data: data.clone(),
            compression: 0,
            crc32: crc32fast::hash(&data),
            vol_created_at: 1,
            seq_lba_ranges: vec![(seq, start_lba, lbas_per_unit)],
            block_hashes: None,
            dedup_stale_repairs: None,
            dedup_skipped: false,
            compression_bypassed: false,
            dedup_completion: None,
        };
        units.push(super::writer::UnitCommitData {
            shard_idx: 0,
            unit,
            pba,
            alloc_blocks: 1,
            seqs: vec![seq],
            completion: None,
        });
    }

    let before = meta.memory_stats().unwrap();
    let job = super::writer::PassthroughCommitJob {
        vol_id: VolumeId("flush-race".into()),
        units,
        enqueued_at: Instant::now(),
    };
    let (post_commit_tx, post_commit_rx) = unbounded::<super::writer::PostCommitJob>();
    let pool_pc = pool.clone();
    let meta_pc = meta.clone();
    let candidate_pc = candidate.clone();
    let metrics_pc = metrics.clone();
    let done_txs_pc = vec![done_tx.clone()];
    let post_commit_handle = std::thread::spawn(move || {
        BufferFlusher::post_commit_loop(
            0,
            &post_commit_rx,
            &pool_pc,
            &meta_pc,
            &candidate_pc,
            &metrics_pc,
            &done_txs_pc,
        );
    });
    BufferFlusher::commit_passthrough_job(
        job,
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &in_flight,
        &metrics,
        &cleanup_tx,
        &candidate,
        std::slice::from_ref(&done_tx),
        &post_commit_tx,
        super::writer::TARGET_OPS_PER_COMMIT,
        1,
    );
    drop(post_commit_tx);
    post_commit_handle
        .join()
        .expect("post_commit_loop panicked");
    let after = meta.memory_stats().unwrap();
    let commits = after.commit_success - before.commit_success;
    assert!(
        commits >= 2,
        "500-LBA passthrough job should split into multiple sub-batches at TARGET_OPS_PER_COMMIT (got {commits} commits)"
    );
}

#[test]
fn coalesced_passthrough_done_routes_to_origin_shards() {
    let (meta, pool, lifecycle, allocator, _io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let (cleanup_tx, _cleanup_rx) = unbounded::<CleanupBatch>();
    let candidate = crate::dedup::CandidateCache::new(8, 64);
    let in_flight = std::sync::Arc::new(super::FlusherInFlightTracker::default());
    let (done0_tx, done0_rx) = unbounded::<Vec<u64>>();
    let (done1_tx, done1_rx) = unbounded::<Vec<u64>>();

    let mut units = Vec::new();
    for (shard_idx, seq, lba, fill) in [(0usize, 101u64, 10u64, 0x51), (1, 202, 20, 0x62)] {
        pool.note_latest_lba_seq_for_test("flush-race", Lba(lba), seq, 1);
        let pba = allocator.allocate_one_for_lane(shard_idx).unwrap();
        units.push(super::writer::UnitCommitData {
            shard_idx,
            unit: make_raw_unit_at(lba, 1, fill, seq),
            pba,
            alloc_blocks: 1,
            seqs: vec![seq],
            completion: None,
        });
    }

    let job = super::writer::PassthroughCommitJob {
        vol_id: VolumeId("flush-race".into()),
        units,
        enqueued_at: Instant::now(),
    };
    let (post_commit_tx, post_commit_rx) = unbounded::<super::writer::PostCommitJob>();
    let pool_pc = pool.clone();
    let meta_pc = meta.clone();
    let candidate_pc = candidate.clone();
    let metrics_pc = metrics.clone();
    let done_txs_pc = vec![done0_tx.clone(), done1_tx.clone()];
    let post_commit_handle = std::thread::spawn(move || {
        BufferFlusher::post_commit_loop(
            0,
            &post_commit_rx,
            &pool_pc,
            &meta_pc,
            &candidate_pc,
            &metrics_pc,
            &done_txs_pc,
        );
    });
    BufferFlusher::commit_passthrough_job(
        job,
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &in_flight,
        &metrics,
        &cleanup_tx,
        &candidate,
        &[done0_tx, done1_tx],
        &post_commit_tx,
        super::writer::TARGET_OPS_PER_COMMIT,
        1,
    );
    drop(post_commit_tx);
    post_commit_handle
        .join()
        .expect("post_commit_loop panicked");

    assert_eq!(
        done0_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        vec![101]
    );
    assert_eq!(
        done1_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        vec![202]
    );
}

#[test]
fn commit_worker_publishes_candidates_before_post_commit() {
    let (meta, pool, lifecycle, allocator, _io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let (cleanup_tx, _cleanup_rx) = unbounded::<CleanupBatch>();
    let candidate = crate::dedup::CandidateCache::new(8, 64);
    let in_flight = std::sync::Arc::new(super::FlusherInFlightTracker::default());
    let (done_tx, done_rx) = unbounded::<Vec<u64>>();
    let (post_commit_tx, post_commit_rx) = unbounded::<super::writer::PostCommitJob>();

    let seq = 303;
    let lba = Lba(30);
    let hash: ContentHash = [0xAB; 8];
    pool.note_latest_lba_seq_for_test("flush-race", lba, seq, 1);
    let pba = allocator.allocate_one_for_lane(0).unwrap();
    let mut unit = make_raw_unit_at(lba.0, 1, 0x73, seq);
    unit.block_hashes = Some(vec![hash]);
    let job = super::writer::PassthroughCommitJob {
        vol_id: VolumeId("flush-race".into()),
        units: vec![super::writer::UnitCommitData {
            shard_idx: 0,
            unit,
            pba,
            alloc_blocks: 1,
            seqs: vec![seq],
            completion: None,
        }],
        enqueued_at: Instant::now(),
    };

    BufferFlusher::commit_passthrough_job(
        job,
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &in_flight,
        &metrics,
        &cleanup_tx,
        &candidate,
        std::slice::from_ref(&done_tx),
        &post_commit_tx,
        super::writer::TARGET_OPS_PER_COMMIT,
        1,
    );

    let published = candidate
        .lookup(&hash)
        .expect("candidate must be visible before post_commit drains");
    assert_eq!(published.pba, pba);
    let post = post_commit_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("passthrough commit should defer mark_flushed");
    assert_eq!(post.mark_ranges, vec![(seq, lba, 1)]);
    assert_eq!(post.done_seqs, vec![seq]);
    assert!(
        post.candidate_pairs.is_empty(),
        "candidate publication stays synchronous with the commit worker"
    );
    assert!(post.stale_repairs.is_empty());
    assert!(done_rx.try_recv().is_err());
}

#[test]
fn packed_slot_flush_survives_already_freed_old_pba_cleanup() {
    let (meta, pool, lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let (cleanup_tx, _cleanup_rx) = unbounded::<CleanupBatch>();

    let seq = pool
        .append("flush-race", Lba(0), 1, &vec![0x44; BLOCK_SIZE as usize], 1)
        .unwrap();

    let old_pba = allocator.allocate_one_for_lane(0).unwrap();
    let new_pba = allocator.allocate_one_for_lane(0).unwrap();
    let old_value = BlockmapValue {
        pba: old_pba,
        compression: 0,
        unit_compressed_size: 512,
        unit_original_size: BLOCK_SIZE,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: crc32fast::hash(&[0x22; 512]),
        slot_offset: 0,
        flags: 0,
    };
    meta.put_mapping(&VolumeId("flush-race".into()), Lba(0), &old_value)
        .unwrap();
    meta.set_refcount(old_pba, 1).unwrap();

    // Simulate the allocator drift we observed in soak: metadata still
    // points at old_pba, but the allocator already handed it back.
    allocator.free_one(old_pba).unwrap();

    let sealed = SealedSlot {
        pba: new_pba,
        data: vec![0xAB; BLOCK_SIZE as usize],
        fragments: vec![crate::packer::packer::SlotFragment {
            unit: make_packed_unit(0x11, seq),
            slot_offset: 0,
        }],
    };

    BufferFlusher::write_packed_slot(
        0,
        &sealed,
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        &metrics,
        &cleanup_tx,
        &crate::dedup::CandidateCache::new(8, 64),
    )
    .unwrap();

    let mapping = meta
        .get_mapping(&VolumeId("flush-race".into()), Lba(0))
        .unwrap()
        .unwrap();
    assert_eq!(mapping.pba, new_pba);
    // Phase 5: hot-path write_packed_slot doesn't bump rc. The
    // seeded old_pba rc=1 stays (hot path also doesn't decref;
    // lineage GC owns the retirement path). The load-bearing
    // invariant is "no panic on already-freed-old-pba drift" — the
    // commit succeeded above.
    assert_eq!(meta.get_refcount(new_pba).unwrap(), 0);
    assert_eq!(meta.get_refcount(old_pba).unwrap(), 1);
    assert!(
        pool.pending_entry_arc(seq).is_none(),
        "post-commit cleanup drift must not leave the seq stuck in the buffer"
    );
}

#[test]
fn writer_flushes_packed_open_slot_while_lane_stays_busy() {
    let (meta, pool, lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let running = Arc::new(AtomicBool::new(true));
    let in_flight = FlusherInFlightTracker::default();
    let (tx, rx) = bounded::<CompressedUnit>(64);
    let (done_tx, done_rx) = unbounded::<Vec<u64>>();
    let (cleanup_tx, _cleanup_rx) = unbounded::<CleanupBatch>();

    let running_w = running.clone();
    let pool_w = pool.clone();
    let meta_w = meta.clone();
    let lifecycle_w = lifecycle.clone();
    let allocator_w = allocator.clone();
    let io_engine_w = io_engine.clone();
    let metrics_w = metrics.clone();
    let cleanup_tx_w = cleanup_tx.clone();

    let handle = thread::spawn(move || {
        let mut packer = Packer::new_with_lane(allocator_w.clone(), 0);
        // Empty commit_worker_txs: write_packed_slots_batch /
        // write_units_batch fall back to inline defer_retry + done_tx
        // for buffered seqs, which is enough to exercise the writer
        // loop's packer-aging / done_tx-on-flush behaviour without
        // pulling in a full commit-worker setup.
        let commit_worker_txs: Vec<
            crossbeam_channel::Sender<crate::buffer::flush::writer::CommitJob>,
        > = Vec::new();
        BufferFlusher::writer_loop(
            0,
            &rx,
            &pool_w,
            &meta_w,
            &lifecycle_w,
            &allocator_w,
            &io_engine_w,
            None,
            &done_tx,
            &running_w,
            &in_flight,
            &mut packer,
            &metrics_w,
            &cleanup_tx_w,
            &crate::dedup::CandidateCache::new(8, 64),
            DEFAULT_PACKED_META_BATCH_LBA_LIMIT,
            &commit_worker_txs,
            1,
            BufferFlusher::WRITER_BATCH_SIZE_READ_ACTIVE,
        );
    });

    // Keep the lane busy with small packed fragments and never leave a
    // 50ms idle gap. Without age-based flushing, buffered seqs would not
    // complete until traffic stops and recv_timeout finally fires.
    for i in 0..12u64 {
        tx.send(make_packed_unit_at(0x40 + i as u8, 10_000 + i, i))
            .unwrap();
        thread::sleep(Duration::from_millis(10));
    }

    let done = done_rx
        .recv_timeout(Duration::from_millis(150))
        .expect("busy writer lane should flush aged packed slot without waiting for idle");
    assert!(
        !done.is_empty(),
        "aged packed slot flush should signal at least one buffered seq"
    );

    running.store(false, Ordering::Relaxed);
    drop(tx);
    handle.join().unwrap();
}

#[test]
fn seq_rejected_packed_slot_keeps_candidate_clean_and_retires_pba() {
    let (meta, pool, lifecycle, allocator, _io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let (cleanup_tx, cleanup_rx) = unbounded::<CleanupBatch>();
    let candidate = crate::dedup::CandidateCache::new(8, 64);
    let vol = VolumeId("flush-race".into());

    // The buffer pool believes seq 1 is the latest write for LBA 30, but a
    // concurrent commit (seq 2) already landed in metadb — the window where
    // the unit passes the pool staleness gate and metadb's per-LBA
    // seq_guard rejects every remap in the slot.
    pool.note_latest_lba_seq_for_test("flush-race", Lba(30), 1, 1);
    let winner_pba = allocator.allocate_one_for_lane(0).unwrap();
    let winner_payload = vec![0x22u8; BLOCK_SIZE as usize];
    let winner = BlockmapValue {
        pba: winner_pba,
        compression: 0,
        unit_compressed_size: BLOCK_SIZE,
        unit_original_size: BLOCK_SIZE,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: crc32fast::hash(&winner_payload),
        slot_offset: 0,
        flags: 0,
    };
    meta.atomic_batch_write_packed_with_dedup(
        &[(vol.clone(), Lba(30), winner)],
        winner_pba,
        1,
        &[],
        &[2],
    )
    .unwrap();

    let payload = vec![0x7Fu8; BLOCK_SIZE as usize];
    let hash: ContentHash = crate::meta::schema::compute_content_hash(&payload);
    let mut unit = make_packed_unit_at(0x7F, 1, 30);
    unit.block_hashes = Some(vec![hash]);
    let slot_pba = allocator.allocate_one_for_lane(0).unwrap();
    let sealed = SealedSlot {
        pba: slot_pba,
        data: payload,
        fragments: vec![crate::packer::packer::SlotFragment {
            unit,
            slot_offset: 0,
        }],
    };

    let in_flight = std::sync::Arc::new(super::FlusherInFlightTracker::default());
    let (done_tx, _done_rx) = unbounded::<Vec<u64>>();
    let (post_commit_tx, _post_commit_rx) = unbounded::<super::writer::PostCommitJob>();
    let rejects_before = metrics.flush_seq_rejects.load(Ordering::Relaxed);
    BufferFlusher::commit_packed_job(
        super::writer::PackedCommitJob {
            sealed,
            shard_idx: 0,
            buffered_seqs: Vec::new(),
            buffered_completions: Vec::new(),
            enqueued_at: Instant::now(),
        },
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &in_flight,
        &metrics,
        &cleanup_tx,
        &candidate,
        &done_tx,
        &post_commit_tx,
    );
    assert!(
        metrics.flush_seq_rejects.load(Ordering::Relaxed) > rejects_before,
        "test must actually exercise the seq_guard reject path"
    );

    // The winning mapping is untouched.
    let mapping = meta.get_mapping(&vol, Lba(30)).unwrap().unwrap();
    assert_eq!(mapping.pba, winner_pba);

    // Regression for the candidate-cache poisoning premature-free CRC P0:
    // the rejected fragment's (hash → pba) pair must NOT reach the
    // candidate cache — a later same-content write would byte-verify
    // against the slot's still-intact LV3 bytes and promote a live
    // mapping onto a PBA the allocator can hand out again.
    assert!(
        candidate.lookup(&hash).is_none(),
        "rejected fragment's pair must not poison the candidate cache"
    );

    // The all-rejected slot PBA is routed through the retire cleanup
    // channel (candidate evict + grace + Gate-1), not direct-freed.
    assert!(
        !allocator.is_free(slot_pba),
        "all-rejected slot must not return straight to the free list"
    );
    let batch = cleanup_rx
        .try_recv()
        .expect("all-rejected slot must surface on the retire cleanup channel");
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].pba, slot_pba);
    assert!(batch[0].pba_freed);

    let pba_lifecycle = crate::space::pba_lifecycle::PbaLifecycle::new(
        allocator.clone(),
        candidate.clone(),
        metrics.clone(),
    );
    BufferFlusher::cleanup_dead_pbas_batch(&pba_lifecycle, &batch, "test_seq_reject");
    assert!(allocator.is_retired(slot_pba));
    assert!(!allocator.is_free(slot_pba));
}

#[test]
fn seq_rejected_passthrough_unit_keeps_candidate_clean_and_retires_pba() {
    let (meta, pool, lifecycle, allocator, _io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let (cleanup_tx, cleanup_rx) = unbounded::<CleanupBatch>();
    let candidate = crate::dedup::CandidateCache::new(8, 64);
    let vol = VolumeId("flush-race".into());

    // Same window as the packed variant: pool says seq 1 is latest, but
    // metadb already holds seq 2 for the LBA.
    pool.note_latest_lba_seq_for_test("flush-race", Lba(50), 1, 1);
    let winner_pba = allocator.allocate_one_for_lane(0).unwrap();
    let winner_payload = vec![0x22u8; BLOCK_SIZE as usize];
    let winner = BlockmapValue {
        pba: winner_pba,
        compression: 0,
        unit_compressed_size: BLOCK_SIZE,
        unit_original_size: BLOCK_SIZE,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: crc32fast::hash(&winner_payload),
        slot_offset: 0,
        flags: 0,
    };
    meta.atomic_batch_write_packed_with_dedup(
        &[(vol.clone(), Lba(50), winner)],
        winner_pba,
        1,
        &[],
        &[2],
    )
    .unwrap();

    let payload = vec![0x5Cu8; BLOCK_SIZE as usize];
    let hash: ContentHash = crate::meta::schema::compute_content_hash(&payload);
    let mut unit = make_packed_unit_at(0x5C, 1, 50);
    unit.block_hashes = Some(vec![hash]);
    let unit_pba = allocator.allocate_one_for_lane(0).unwrap();

    let in_flight = std::sync::Arc::new(super::FlusherInFlightTracker::default());
    let (done_tx, _done_rx) = unbounded::<Vec<u64>>();
    let (post_commit_tx, _post_commit_rx) = unbounded::<super::writer::PostCommitJob>();
    let rejects_before = metrics.flush_seq_rejects.load(Ordering::Relaxed);
    BufferFlusher::commit_passthrough_job(
        super::writer::PassthroughCommitJob {
            vol_id: vol.clone(),
            units: vec![super::writer::UnitCommitData {
                shard_idx: 0,
                unit,
                pba: unit_pba,
                alloc_blocks: 1,
                seqs: vec![1],
                completion: None,
            }],
            enqueued_at: Instant::now(),
        },
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &in_flight,
        &metrics,
        &cleanup_tx,
        &candidate,
        std::slice::from_ref(&done_tx),
        &post_commit_tx,
        super::writer::TARGET_OPS_PER_COMMIT,
        1,
    );
    assert!(
        metrics.flush_seq_rejects.load(Ordering::Relaxed) > rejects_before,
        "test must actually exercise the seq_guard reject path"
    );

    let mapping = meta.get_mapping(&vol, Lba(50)).unwrap().unwrap();
    assert_eq!(mapping.pba, winner_pba);

    assert!(
        candidate.lookup(&hash).is_none(),
        "rejected unit's pair must not poison the candidate cache"
    );
    assert!(
        !allocator.is_free(unit_pba),
        "all-rejected unit must not return straight to the free list"
    );
    let batch = cleanup_rx
        .try_recv()
        .expect("all-rejected unit must surface on the retire cleanup channel");
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].pba, unit_pba);
    assert!(batch[0].pba_freed);

    let pba_lifecycle = crate::space::pba_lifecycle::PbaLifecycle::new(
        allocator.clone(),
        candidate.clone(),
        metrics.clone(),
    );
    BufferFlusher::cleanup_dead_pbas_batch(&pba_lifecycle, &batch, "test_seq_reject_pt");
    assert!(allocator.is_retired(unit_pba));
    assert!(!allocator.is_free(unit_pba));
}
