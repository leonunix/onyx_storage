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
// `commit_passthrough_job` at TARGET_OPS_PER_COMMIT (=8192 LBAs);
// covered by `passthrough_commit_job_subbatches_above_threshold`
// below.

#[test]
fn passthrough_commit_job_keeps_500_lbas_in_one_transaction() {
    let (meta, pool, lifecycle, allocator, _io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let (cleanup_tx, _cleanup_rx) = unbounded::<CleanupBatch>();
    let candidate = crate::dedup::CandidateCache::new(8, 64);
    let in_flight = std::sync::Arc::new(super::FlusherInFlightTracker::default());
    let (done_tx, _done_rx) = unbounded::<Vec<u64>>();

    // Build 5 units of 100 LBAs each = 500 LBAs total. This used to be
    // split at 150 LBAs, which made RC apply dominate transaction cost.
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
            payload: CompressedPayload::Contiguous(data.clone()),
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
    assert_eq!(
        commits, 1,
        "500-LBA passthrough job should remain one effective transaction"
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

    let first_lba = 10u64;
    let first_sid = meta.l2p_shard_of(Lba(first_lba));
    let second_lba = (first_lba + 1..)
        .find(|lba| meta.l2p_shard_of(Lba(*lba)) != first_sid)
        .expect("test must find an LBA routed to a different metadb shard");
    let mut units = Vec::new();
    for (shard_idx, seq, lba, fill) in [
        (0usize, 101u64, first_lba, 0x51),
        (1, 202, second_lba, 0x62),
    ] {
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
    let before = meta.memory_stats().unwrap();
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
    let after = meta.memory_stats().unwrap();
    assert_eq!(
        after.commit_success - before.commit_success,
        1,
        "one passthrough chunk must stay one transaction even across L2P shards"
    );

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
fn commit_worker_batches_dedup_hit_jobs_and_splits_responses() {
    let (meta, _pool, lifecycle, _allocator, _io, metrics, _meta_dir, _buf, _data) =
        setup_flush_test_env();
    let vol = VolumeId("flush-race".into());
    let vol_created_at = meta.get_volume(&vol).unwrap().unwrap().created_at;
    let target = BlockmapValue {
        pba: Pba(42),
        compression: 0,
        unit_compressed_size: BLOCK_SIZE,
        unit_original_size: BLOCK_SIZE,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0x1234_5678,
        slot_offset: 0,
        flags: 0,
    };
    meta.set_refcount(target.pba, 1).unwrap();

    let (response_tx_1, response_rx_1) = bounded(1);
    let (response_tx_2, response_rx_2) = bounded(1);
    let jobs = vec![
        super::writer::DedupHitCommitJob {
            vol_id: vol.clone(),
            vol_created_at,
            hits: vec![(Lba(700), target, [1; 8])],
            promote_entries: Vec::new(),
            seqs: vec![1],
            response_tx: response_tx_1,
            enqueued_at: Instant::now(),
        },
        super::writer::DedupHitCommitJob {
            vol_id: vol.clone(),
            vol_created_at,
            hits: vec![(Lba(701), target, [2; 8])],
            promote_entries: Vec::new(),
            seqs: vec![2],
            response_tx: response_tx_2,
            enqueued_at: Instant::now(),
        },
    ];
    let attempts_before = meta.memory_stats().unwrap().commit_attempts;

    BufferFlusher::dispatch_dedup_hit_jobs(jobs, &meta, &lifecycle, &metrics);

    for response in [response_rx_1.recv().unwrap(), response_rx_2.recv().unwrap()] {
        match response {
            super::writer::DedupHitCommitResponse::Committed { results, .. } => {
                assert_eq!(results.len(), 1);
                assert!(matches!(results[0], DedupHitResult::Accepted(_)));
            }
            super::writer::DedupHitCommitResponse::Failed(error) => {
                panic!("dedup hit batch failed: {error}")
            }
        }
    }
    assert_eq!(
        meta.memory_stats().unwrap().commit_attempts - attempts_before,
        1,
        "two queued hit jobs must share one metadb transaction"
    );
    assert_eq!(
        meta.get_mapping(&vol, Lba(700)).unwrap().unwrap().pba,
        target.pba
    );
    assert_eq!(
        meta.get_mapping(&vol, Lba(701)).unwrap().unwrap().pba,
        target.pba
    );
}

#[test]
fn commit_worker_rejects_stale_dedup_hit_generation() {
    let (meta, _pool, lifecycle, _allocator, _io, metrics, _meta_dir, _buf, _data) =
        setup_flush_test_env();
    let vol = VolumeId("flush-race".into());
    let current_created_at = meta.get_volume(&vol).unwrap().unwrap().created_at;
    let target = BlockmapValue {
        pba: Pba(42),
        compression: 0,
        unit_compressed_size: BLOCK_SIZE,
        unit_original_size: BLOCK_SIZE,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0x1234_5678,
        slot_offset: 0,
        flags: 0,
    };
    meta.set_refcount(target.pba, 1).unwrap();

    let (response_tx, response_rx) = bounded(1);
    BufferFlusher::dispatch_dedup_hit_jobs(
        vec![super::writer::DedupHitCommitJob {
            vol_id: vol.clone(),
            vol_created_at: current_created_at.saturating_add(1),
            hits: vec![(Lba(702), target, [3; 8])],
            promote_entries: Vec::new(),
            seqs: vec![3],
            response_tx,
            enqueued_at: Instant::now(),
        }],
        &meta,
        &lifecycle,
        &metrics,
    );

    match response_rx.recv().unwrap() {
        super::writer::DedupHitCommitResponse::Failed(error) => {
            assert!(
                error.contains("has been deleted"),
                "unexpected error: {error}"
            );
        }
        super::writer::DedupHitCommitResponse::Committed { .. } => {
            panic!("stale volume generation must not commit")
        }
    }
    assert!(meta.get_mapping(&vol, Lba(702)).unwrap().is_none());
}

#[test]
fn commit_aggregator_forms_full_batch_and_flushes_disconnect_tail() {
    let (raw_tx, raw_rx) = bounded(8);
    let (batch_tx, batch_rx) = bounded(8);
    let metrics = Arc::new(EngineMetrics::default());

    for i in 0..5u64 {
        raw_tx.send(aggregator_job(i, 1)).unwrap();
    }
    drop(raw_tx);

    let aggregator_metrics = metrics.clone();
    let aggregator = std::thread::spawn(move || {
        BufferFlusher::commit_aggregator_loop(
            raw_rx,
            batch_tx,
            None,
            Some(aggregator_metrics),
            true,
            3,
            3,
            Duration::ZERO,
            0,
        )
    });

    assert_eq!(batch_rx.recv().unwrap().jobs.len(), 3);
    assert_eq!(batch_rx.recv().unwrap().jobs.len(), 2);
    aggregator.join().unwrap();
    assert!(batch_rx.recv().is_err());
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.flush_commit_aggregator_seals_target, 1);
    assert_eq!(snapshot.flush_commit_aggregator_seals_shutdown, 1);
}

#[test]
fn commit_aggregator_hard_deadline_is_not_extended_by_arrivals() {
    let (raw_tx, raw_rx) = bounded(8);
    let (batch_tx, batch_rx) = bounded(8);
    raw_tx
        .send(aggregator_job_enqueued_at(
            0,
            1,
            Instant::now() - Duration::from_millis(250),
        ))
        .unwrap();
    raw_tx.send(aggregator_job(1, 1)).unwrap();
    drop(raw_tx);

    let aggregator = std::thread::spawn(move || {
        BufferFlusher::commit_aggregator_loop(
            raw_rx,
            batch_tx,
            None,
            None,
            true,
            3,
            16,
            Duration::from_millis(200),
            0,
        )
    });

    assert_eq!(batch_rx.recv().unwrap().jobs.len(), 1);
    assert_eq!(batch_rx.recv().unwrap().jobs.len(), 1);
    aggregator.join().unwrap();
}

#[test]
fn commit_aggregator_adapts_after_sustained_underfill() {
    let (raw_tx, raw_rx) = bounded(8);
    let (batch_tx, batch_rx) = bounded(8);
    let metrics = Arc::new(EngineMetrics::default());
    let now = Instant::now();
    raw_tx
        .send(aggregator_job_enqueued_at(
            0,
            3_000,
            now - Duration::from_millis(100),
        ))
        .unwrap();
    raw_tx
        .send(aggregator_job_enqueued_at(
            3_000,
            3_000,
            now - Duration::from_millis(90),
        ))
        .unwrap();
    raw_tx
        .send(aggregator_job_enqueued_at(
            6_000,
            2_048,
            now - Duration::from_millis(40),
        ))
        .unwrap();
    drop(raw_tx);

    let aggregator_metrics = metrics.clone();
    let aggregator = std::thread::spawn(move || {
        BufferFlusher::commit_aggregator_loop(
            raw_rx,
            batch_tx,
            None,
            Some(aggregator_metrics),
            true,
            8_192,
            16_384,
            Duration::from_millis(75),
            0,
        )
    });

    assert_eq!(batch_rx.recv().unwrap().jobs.len(), 1);
    assert_eq!(batch_rx.recv().unwrap().jobs.len(), 1);
    assert_eq!(batch_rx.recv().unwrap().jobs.len(), 1);
    aggregator.join().unwrap();
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.flush_commit_aggregator_seals_deadline, 2);
    assert_eq!(snapshot.flush_commit_aggregator_seals_adaptive_underfill, 1);
}

#[test]
fn commit_aggregator_idle_flushes_partial_without_splitting_jobs() {
    let (raw_tx, raw_rx) = bounded(8);
    let (batch_tx, batch_rx) = bounded(8);
    let aggregator = std::thread::spawn(move || {
        BufferFlusher::commit_aggregator_loop(
            raw_rx,
            batch_tx,
            None,
            None,
            true,
            8,
            16,
            Duration::from_millis(20),
            0,
        )
    });

    raw_tx.send(aggregator_job(0, 6)).unwrap();
    let partial = batch_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(partial.jobs.len(), 1);
    assert!(matches!(
        &partial.jobs[0],
        super::writer::CommitJob::DedupHit(job) if job.hits.len() == 6
    ));

    raw_tx.send(aggregator_job(6, 10)).unwrap();
    let overshoot = batch_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(overshoot.jobs.len(), 1);
    assert!(matches!(
        &overshoot.jobs[0],
        super::writer::CommitJob::DedupHit(job) if job.hits.len() == 10
    ));
    drop(raw_tx);
    aggregator.join().unwrap();
}

#[test]
fn commit_aggregator_carries_whole_job_that_crosses_target() {
    let (raw_tx, raw_rx) = bounded(8);
    let (batch_tx, batch_rx) = bounded(8);
    let aggregator = std::thread::spawn(move || {
        BufferFlusher::commit_aggregator_loop(
            raw_rx,
            batch_tx,
            None,
            None,
            true,
            8,
            16,
            Duration::from_secs(1),
            0,
        )
    });

    raw_tx.send(aggregator_job(0, 6)).unwrap();
    raw_tx.send(aggregator_job(6, 4)).unwrap();
    let prefix = batch_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(matches!(
        prefix.jobs.as_slice(),
        [super::writer::CommitJob::DedupHit(job)] if job.hits.len() == 6
    ));

    raw_tx.send(aggregator_job(10, 4)).unwrap();
    let full = batch_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(full.jobs.len(), 2);
    assert_eq!(
        full.jobs
            .iter()
            .map(|job| match job {
                super::writer::CommitJob::DedupHit(job) => job.hits.len(),
                _ => 0,
            })
            .sum::<usize>(),
        8
    );
    drop(raw_tx);
    aggregator.join().unwrap();
}

#[test]
fn commit_aggregator_3162_lba_crossing_is_capacity_not_target() {
    let (raw_tx, raw_rx) = bounded(8);
    let (batch_tx, batch_rx) = bounded(8);
    let metrics = Arc::new(EngineMetrics::default());
    for start_lba in [0, 3_162, 6_324] {
        raw_tx.send(aggregator_job(start_lba, 3_162)).unwrap();
    }
    drop(raw_tx);

    let aggregator_metrics = metrics.clone();
    let aggregator = std::thread::spawn(move || {
        BufferFlusher::commit_aggregator_loop(
            raw_rx,
            batch_tx,
            None,
            Some(aggregator_metrics),
            true,
            8_192,
            16_384,
            Duration::from_millis(75),
            0,
        )
    });

    let prefix = batch_rx.recv().unwrap();
    assert_eq!(prefix.jobs.len(), 2);
    assert_eq!(
        prefix
            .jobs
            .iter()
            .map(|job| match job {
                super::writer::CommitJob::DedupHit(job) => job.hits.len(),
                _ => 0,
            })
            .sum::<usize>(),
        6_324
    );
    assert_eq!(batch_rx.recv().unwrap().jobs.len(), 1);
    aggregator.join().unwrap();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.flush_commit_aggregator_seals_capacity, 1);
    assert_eq!(snapshot.flush_commit_aggregator_seals_target, 0);
    assert_eq!(snapshot.flush_commit_aggregator_seals_shutdown, 1);
}

#[test]
fn commit_aggregator_retain_tail_gate_off_uses_legacy_budget() {
    assert!(!crate::config::FlushConfig::default().commit_retain_tail);

    let (raw_tx, raw_rx) = bounded(8);
    let (batch_tx, batch_rx) = bounded(8);
    for i in 0..5u64 {
        raw_tx.send(aggregator_job(i, 1)).unwrap();
    }
    drop(raw_tx);

    let aggregator = std::thread::spawn(move || {
        BufferFlusher::commit_aggregator_loop(
            raw_rx,
            batch_tx,
            None,
            None,
            false,
            3,
            5,
            Duration::ZERO,
            0,
        )
    });

    assert_eq!(
        batch_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .jobs
            .len(),
        5,
        "gate-off path must preserve the legacy coalesce budget"
    );
    aggregator.join().unwrap();
    assert!(batch_rx.recv().is_err());
}

#[test]
fn commit_aggregator_reports_executor_disconnect_to_dedup_waiter() {
    let (raw_tx, raw_rx) = bounded(1);
    let (batch_tx, batch_rx) = bounded(1);
    drop(batch_rx);
    let metrics = Arc::new(EngineMetrics::default());
    let (response_tx, response_rx) = bounded(1);
    let mut job = aggregator_job(0, 1);
    let super::writer::CommitJob::DedupHit(dedup) = &mut job else {
        unreachable!()
    };
    dedup.response_tx = response_tx;
    raw_tx.send(job).unwrap();
    drop(raw_tx);

    BufferFlusher::commit_aggregator_loop(
        raw_rx,
        batch_tx,
        None,
        Some(metrics.clone()),
        true,
        1,
        1,
        Duration::ZERO,
        0,
    );

    assert!(matches!(
        response_rx.recv().unwrap(),
        super::writer::DedupHitCommitResponse::Failed(error)
            if error.contains("executors disconnected")
    ));
    assert_eq!(metrics.snapshot().flush_errors, 1);
}

#[test]
fn passthrough_send_disconnect_rolls_back_and_fences() {
    let (_meta, pool, _lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let seq = 7_001;
    pool.note_latest_lba_seq_for_test("flush-race", Lba(901), seq, 1);
    let free_before = allocator.free_block_count();
    let (commit_tx, commit_rx) = bounded(1);
    drop(commit_rx);
    let (done_tx, done_rx) = bounded(1);
    let in_flight = FlusherInFlightTracker::default();

    BufferFlusher::write_units_batch(
        0,
        vec![make_raw_unit_at(901, 1, 0xA1, seq)],
        vec![vec![seq]],
        vec![None],
        &pool,
        &allocator,
        &io_engine,
        None,
        &metrics,
        &in_flight,
        &done_tx,
        &[commit_tx],
        1,
    );

    assert!(pool.is_meta_fenced());
    assert_eq!(
        done_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        vec![seq]
    );
    assert_eq!(allocator.free_block_count(), free_before);
    assert_eq!(metrics.snapshot().flush_errors, 1);
}

#[test]
fn packed_send_disconnect_rolls_back_and_fences() {
    let (_meta, pool, _lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let seq = 7_002;
    let free_before = allocator.free_block_count();
    let pba = allocator.allocate_one_for_lane(0).unwrap();
    let sealed = SealedSlot {
        pba,
        data: vec![0xB2; BLOCK_SIZE as usize],
        fragments: vec![crate::packer::packer::SlotFragment {
            unit: make_packed_unit_at(0xB2, seq, 902),
            slot_offset: 0,
        }],
    };
    let (commit_tx, commit_rx) = bounded(1);
    drop(commit_rx);
    let (done_tx, done_rx) = bounded(1);
    let in_flight = FlusherInFlightTracker::default();
    let mut retries = VecDeque::new();

    BufferFlusher::write_packed_slots_batch(
        0,
        vec![sealed],
        vec![(vec![seq], Vec::new())],
        &pool,
        &allocator,
        &io_engine,
        None,
        &metrics,
        &in_flight,
        &done_tx,
        &mut retries,
        &[commit_tx],
        1,
    );

    assert!(pool.is_meta_fenced());
    assert_eq!(
        done_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        vec![seq]
    );
    assert!(retries.is_empty());
    assert!(allocator.is_free(pba));
    assert_eq!(allocator.free_block_count(), free_before);
    assert_eq!(metrics.snapshot().flush_errors, 1);
}

fn aggregator_job(start_lba: u64, lba_count: usize) -> super::writer::CommitJob {
    let (response_tx, _response_rx) = bounded(1);
    let target = BlockmapValue::zero();
    super::writer::CommitJob::DedupHit(super::writer::DedupHitCommitJob {
        vol_id: VolumeId("aggregate".into()),
        vol_created_at: 1,
        hits: (0..lba_count)
            .map(|offset| {
                let lba = start_lba + offset as u64;
                (Lba(lba), target, [lba as u8; 8])
            })
            .collect(),
        promote_entries: Vec::new(),
        seqs: (0..lba_count)
            .map(|offset| start_lba + offset as u64 + 1)
            .collect(),
        response_tx,
        enqueued_at: Instant::now(),
    })
}

fn aggregator_job_enqueued_at(
    start_lba: u64,
    lba_count: usize,
    enqueued_at: Instant,
) -> super::writer::CommitJob {
    let mut job = aggregator_job(start_lba, lba_count);
    match &mut job {
        super::writer::CommitJob::DedupHit(job) => job.enqueued_at = enqueued_at,
        _ => unreachable!(),
    }
    job
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

/// Drain every `PassthroughCommitJob` dispatched to `cw_rx` and run its metadata
/// commit inline (mirrors the commit-worker loop), so a `write_units_batch` test
/// can read the published mappings back.
fn drain_passthrough_commits(
    cw_rx: crossbeam_channel::Receiver<super::writer::CommitJob>,
    pool: &Arc<WriteBufferPool>,
    meta: &Arc<MetaStore>,
    lifecycle: &Arc<VolumeLifecycleManager>,
    allocator: &Arc<SpaceAllocator>,
    in_flight: &Arc<super::FlusherInFlightTracker>,
    metrics: &Arc<EngineMetrics>,
    done_tx: &crossbeam_channel::Sender<Vec<u64>>,
    candidate: &crate::dedup::CandidateCache,
) {
    let (cleanup_tx, _cleanup_rx) = unbounded::<CleanupBatch>();
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
    for job in cw_rx.into_iter() {
        match job {
            super::writer::CommitJob::Passthrough(pj) => BufferFlusher::commit_passthrough_job(
                pj,
                pool,
                meta,
                lifecycle,
                allocator,
                in_flight,
                metrics,
                &cleanup_tx,
                candidate,
                std::slice::from_ref(done_tx),
                &post_commit_tx,
                super::writer::TARGET_OPS_PER_COMMIT,
                1,
            ),
            super::writer::CommitJob::Packed(_) | super::writer::CommitJob::DedupHit(_) => {
                panic!("expected passthrough commit job")
            }
        }
    }
    drop(post_commit_tx);
    post_commit_handle
        .join()
        .expect("post_commit_loop panicked");
}

/// Three sub-stripe (2-block) passthrough units at disjoint LBAs pack into ONE
/// 6-block RAID stripe: they land on six consecutive, stripe-aligned PBAs, are
/// written as a single full-stripe LV3 write, keep independent block-granular
/// mappings (`slot_offset == 0`), and every LBA round-trips its distinct bytes.
#[test]
fn passthrough_groups_sub_stripe_units_into_one_full_stripe() {
    let (meta, pool, lifecycle, allocator, _io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let data_tmp = NamedTempFile::new().unwrap();
    data_tmp.as_file().set_len(4096 * 20000).unwrap();
    let io_engine = stripe_io_engine(data_tmp.path(), 6, metrics.clone());
    assert_eq!(io_engine.stripe_blocks(), 6);
    assert_eq!(
        io_engine.stripe_phase(),
        (crate::types::RESERVED_BLOCKS % 6) as u32
    );

    // blocks_per_unit = [2, 2, 2] → exactly one stripe.
    let specs = [(10u64, 0x10u8), (20, 0x30), (30, 0x50)];
    let mut units = Vec::new();
    let mut seqs_per_unit = Vec::new();
    for (i, (lba, fill)) in specs.iter().enumerate() {
        let seq = (i as u64) + 1;
        pool.note_latest_lba_seq_for_test("flush-race", Lba(*lba), seq, 1);
        pool.note_latest_lba_seq_for_test("flush-race", Lba(*lba + 1), seq, 1);
        units.push(make_raw_unit_at(*lba, 2, *fill, seq));
        seqs_per_unit.push(vec![seq]);
    }
    let completions_per_unit = vec![None, None, None];

    let in_flight = Arc::new(super::FlusherInFlightTracker::default());
    let (done_tx, _done_rx) = unbounded::<Vec<u64>>();
    let (cw_tx, cw_rx) = unbounded::<super::writer::CommitJob>();

    BufferFlusher::write_units_batch(
        0,
        units,
        seqs_per_unit,
        completions_per_unit,
        &pool,
        &allocator,
        &io_engine,
        None,
        &metrics,
        &in_flight,
        &done_tx,
        std::slice::from_ref(&cw_tx),
        1,
    );
    drop(cw_tx);

    let candidate = crate::dedup::CandidateCache::new(8, 64);
    drain_passthrough_commits(
        cw_rx, &pool, &meta, &lifecycle, &allocator, &in_flight, &metrics, &done_tx, &candidate,
    );

    let vol = VolumeId("flush-race".into());
    let base = meta.get_mapping(&vol, Lba(10)).unwrap().unwrap().pba;
    assert_eq!(
        (base.0 + crate::types::RESERVED_BLOCKS) % 6,
        0,
        "grouped stripe must be aligned in device-offset space"
    );
    // (lba, expected sub-PBA offset from base, expected byte fill)
    let expect = [
        (10u64, 0u64, 0x10u8),
        (11, 1, 0x11),
        (20, 2, 0x30),
        (21, 3, 0x31),
        (30, 4, 0x50),
        (31, 5, 0x51),
    ];
    let worker = ZoneWorker::new(ZoneId(0), meta.clone(), pool.clone(), io_engine.clone());
    for (lba, off, fill) in expect {
        let m = meta.get_mapping(&vol, Lba(lba)).unwrap().unwrap();
        assert_eq!(
            m.pba,
            Pba(base.0 + off),
            "lba {lba} must map to its stripe sub-PBA"
        );
        assert_eq!(m.slot_offset, 0, "grouped members stay block-granular");
        let got = worker.handle_read("flush-race", Lba(lba)).unwrap().unwrap();
        assert_eq!(got, vec![fill; BLOCK_SIZE as usize], "lba {lba} round-trip");
    }
}

#[test]
fn passthrough_partial_group_pads_io_and_returns_unused_extent() {
    let (meta, pool, lifecycle, allocator, _io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let data_tmp = NamedTempFile::new().unwrap();
    data_tmp.as_file().set_len(4096 * 20000).unwrap();
    let io_engine = stripe_io_engine(data_tmp.path(), 6, metrics.clone());

    let mut units = Vec::new();
    let mut seqs_per_unit = Vec::new();
    for (idx, (lba, fill)) in [(80u64, 0x60u8), (90, 0x70)].into_iter().enumerate() {
        let seq = idx as u64 + 1;
        pool.note_latest_lba_seq_for_test("flush-race", Lba(lba), seq, 1);
        pool.note_latest_lba_seq_for_test("flush-race", Lba(lba + 1), seq, 1);
        units.push(make_raw_unit_at(lba, 2, fill, seq));
        seqs_per_unit.push(vec![seq]);
    }

    let free_before = allocator.free_block_count();
    let in_flight = Arc::new(super::FlusherInFlightTracker::default());
    let (done_tx, _done_rx) = unbounded::<Vec<u64>>();
    let (cw_tx, cw_rx) = unbounded::<super::writer::CommitJob>();
    BufferFlusher::write_units_batch(
        0,
        units,
        seqs_per_unit,
        vec![None, None],
        &pool,
        &allocator,
        &io_engine,
        None,
        &metrics,
        &in_flight,
        &done_tx,
        std::slice::from_ref(&cw_tx),
        1,
    );
    drop(cw_tx);
    let candidate = crate::dedup::CandidateCache::new(8, 64);
    drain_passthrough_commits(
        cw_rx, &pool, &meta, &lifecycle, &allocator, &in_flight, &metrics, &done_tx, &candidate,
    );

    assert_eq!(
        free_before - allocator.free_block_count(),
        4,
        "only mapped data blocks should remain allocated"
    );
    assert_eq!(metrics.lv3_write_ops.load(Ordering::Relaxed), 1);
    assert_eq!(
        metrics.lv3_write_compressed_bytes.load(Ordering::Relaxed),
        6 * BLOCK_SIZE as u64,
        "partial bin should issue one padded full-stripe write"
    );
}

/// A grouped full-stripe write that fails IO rolls the whole stripe extent back
/// exactly once (no 5-block leak, no double free) and publishes no mapping. Uses
/// unique high LBAs so the shared-`vol_id` failpoint cannot collide with a
/// parallel test's write.
#[test]
fn passthrough_group_io_failure_rolls_back_whole_stripe_without_leak() {
    let (meta, pool, lifecycle, allocator, _io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let data_tmp = NamedTempFile::new().unwrap();
    data_tmp.as_file().set_len(4096 * 20000).unwrap();
    let io_engine = stripe_io_engine(data_tmp.path(), 6, metrics.clone());

    let specs = [(700_010u64, 0x10u8), (700_020, 0x30), (700_030, 0x50)];
    let mut units = Vec::new();
    let mut seqs_per_unit = Vec::new();
    for (i, (lba, fill)) in specs.iter().enumerate() {
        let seq = (i as u64) + 1;
        pool.note_latest_lba_seq_for_test("flush-race", Lba(*lba), seq, 1);
        pool.note_latest_lba_seq_for_test("flush-race", Lba(*lba + 1), seq, 1);
        units.push(make_raw_unit_at(*lba, 2, *fill, seq));
        seqs_per_unit.push(vec![seq]);
    }

    // Fail the group's IO write via its first member's start_lba (auto-expiring
    // after a single hit so it cannot leak into other work).
    install_test_failpoint(
        "flush-race",
        Lba(700_010),
        FlushFailStage::BeforeIoWrite,
        Some(1),
    );

    let free_before = allocator.free_block_count();
    let runs_before = allocator.free_extent_run_count();

    let in_flight = Arc::new(super::FlusherInFlightTracker::default());
    let (done_tx, _done_rx) = unbounded::<Vec<u64>>();
    let (cw_tx, cw_rx) = unbounded::<super::writer::CommitJob>();

    BufferFlusher::write_units_batch(
        0,
        units,
        seqs_per_unit,
        vec![None, None, None],
        &pool,
        &allocator,
        &io_engine,
        None,
        &metrics,
        &in_flight,
        &done_tx,
        std::slice::from_ref(&cw_tx),
        1,
    );
    drop(cw_tx);
    clear_test_failpoint("flush-race", Lba(700_010), FlushFailStage::BeforeIoWrite);

    // The whole group failed IO → nothing dispatched to the commit worker.
    assert!(
        cw_rx.into_iter().next().is_none(),
        "a failed full-stripe group must not dispatch any commit job"
    );

    // The 6-block stripe extent was rolled back exactly once: free space and the
    // free-list run count both return to baseline (no leak, no free-list bloat).
    assert_eq!(
        allocator.free_block_count(),
        free_before,
        "whole stripe must be freed once — no 5-block leak, no double free"
    );
    // The rolled-back stripe goes back to the global free list while the lane
    // cache still holds its refill remainder, so a single rollback may split one
    // run into two — but it must not fragment further (a 5-block leak scattered
    // back would bloat the run count).
    assert!(
        allocator.free_extent_run_count() <= runs_before + 1,
        "rolled-back stripe must not fragment the free list (runs {} → {})",
        runs_before,
        allocator.free_extent_run_count()
    );
    // No mapping was published for any LBA.
    let vol = VolumeId("flush-race".into());
    for (lba, _) in specs {
        assert!(
            meta.get_mapping(&vol, Lba(lba)).unwrap().is_none(),
            "failed group must publish no mapping for lba {lba}"
        );
    }
}

/// Drain every `PackedCommitJob` dispatched to `cw_rx` and run its metadata
/// commit inline (mirrors the commit-worker loop).
fn drain_packed_commits(
    cw_rx: crossbeam_channel::Receiver<super::writer::CommitJob>,
    pool: &Arc<WriteBufferPool>,
    meta: &Arc<MetaStore>,
    lifecycle: &Arc<VolumeLifecycleManager>,
    allocator: &Arc<SpaceAllocator>,
    in_flight: &Arc<super::FlusherInFlightTracker>,
    metrics: &Arc<EngineMetrics>,
    done_tx: &crossbeam_channel::Sender<Vec<u64>>,
    candidate: &crate::dedup::CandidateCache,
) {
    let (cleanup_tx, _cleanup_rx) = unbounded::<CleanupBatch>();
    let (post_commit_tx, post_commit_rx) = unbounded::<super::writer::PostCommitJob>();
    let pool_pc = pool.clone();
    let meta_pc = meta.clone();
    let candidate_pc = candidate.clone();
    let metrics_pc = metrics.clone();
    let done_txs_pc = vec![done_tx.clone()];
    let handle = std::thread::spawn(move || {
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
    for job in cw_rx.into_iter() {
        match job {
            super::writer::CommitJob::Packed(pj) => BufferFlusher::commit_packed_job(
                pj,
                pool,
                meta,
                lifecycle,
                allocator,
                in_flight,
                metrics,
                &cleanup_tx,
                candidate,
                done_tx,
                &post_commit_tx,
            ),
            super::writer::CommitJob::Passthrough(_) | super::writer::CommitJob::DedupHit(_) => {
                panic!("expected packed commit job")
            }
        }
    }
    drop(post_commit_tx);
    handle.join().expect("post_commit_loop panicked");
}

/// Six 4 KiB packed slots pack into ONE 6-block RAID stripe: they land on six
/// consecutive stripe-aligned sub-PBAs, are written as a single full-stripe LV3
/// write, keep their per-slot `slot_offset`, and each slot's physical block is
/// assembled at its stripe position (verified by a direct device read, which
/// bypasses the unchanged decode path).
#[test]
fn packed_groups_six_slots_into_one_full_stripe() {
    let (meta, pool, lifecycle, allocator, _io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let data_tmp = NamedTempFile::new().unwrap();
    data_tmp.as_file().set_len(4096 * 20000).unwrap();
    let io_engine = stripe_io_engine(data_tmp.path(), 6, metrics.clone());

    let mut sealed_slots = Vec::new();
    let mut per_slot_buffers = Vec::new();
    let mut datas = Vec::new();
    for i in 0..6u64 {
        let lba = 100 + i;
        let seq = i + 1;
        let fill = 0x40 + i as u8;
        pool.note_latest_lba_seq_for_test("flush-race", Lba(lba), seq, 1);
        let unit = make_packed_unit_at(fill, seq, lba);
        let pba = allocator.allocate_one_for_lane(0).unwrap();
        let data = vec![fill; BLOCK_SIZE as usize];
        sealed_slots.push(SealedSlot {
            pba,
            data: data.clone(),
            fragments: vec![crate::packer::packer::SlotFragment {
                unit,
                slot_offset: 0,
            }],
        });
        per_slot_buffers.push((vec![seq], Vec::new()));
        datas.push(data);
    }

    let in_flight = Arc::new(super::FlusherInFlightTracker::default());
    let (done_tx, _done_rx) = unbounded::<Vec<u64>>();
    let (cw_tx, cw_rx) = unbounded::<super::writer::CommitJob>();
    let mut retries = std::collections::VecDeque::new();

    BufferFlusher::write_packed_slots_batch(
        0,
        sealed_slots,
        per_slot_buffers,
        &pool,
        &allocator,
        &io_engine,
        None,
        &metrics,
        &in_flight,
        &done_tx,
        &mut retries,
        std::slice::from_ref(&cw_tx),
        1,
    );
    drop(cw_tx);
    assert!(retries.is_empty(), "clean IO → no packed retries");

    let candidate = crate::dedup::CandidateCache::new(8, 64);
    drain_packed_commits(
        cw_rx, &pool, &meta, &lifecycle, &allocator, &in_flight, &metrics, &done_tx, &candidate,
    );

    let vol = VolumeId("flush-race".into());
    let base = meta.get_mapping(&vol, Lba(100)).unwrap().unwrap().pba;
    assert_eq!(
        (base.0 + crate::types::RESERVED_BLOCKS) % 6,
        0,
        "packed stripe must be aligned in device-offset space"
    );
    for i in 0..6u64 {
        let m = meta.get_mapping(&vol, Lba(100 + i)).unwrap().unwrap();
        assert_eq!(
            m.pba,
            Pba(base.0 + i),
            "slot {i} must map to its stripe sub-PBA"
        );
        assert_eq!(m.slot_offset, 0, "packed slot keeps its own slot_offset");
        let block = io_engine
            .read_blocks(Pba(base.0 + i), BLOCK_SIZE as usize)
            .unwrap();
        assert_eq!(
            block, datas[i as usize],
            "slot {i} 4 KiB block must be assembled at its stripe position"
        );
    }
}

/// A grouped packed full-stripe write that fails IO rolls the whole stripe back
/// once (no leak of the stripe or the freed packer PBAs, no double free) and
/// re-queues all six slots for retry, publishing no mapping.
#[test]
fn packed_group_io_failure_rolls_back_whole_stripe_without_leak() {
    let (meta, pool, lifecycle, allocator, _io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let data_tmp = NamedTempFile::new().unwrap();
    data_tmp.as_file().set_len(4096 * 20000).unwrap();
    let io_engine = stripe_io_engine(data_tmp.path(), 6, metrics.clone());

    let free_before = allocator.free_block_count();
    let runs_before = allocator.free_extent_run_count();

    let mut sealed_slots = Vec::new();
    let mut per_slot_buffers = Vec::new();
    for i in 0..6u64 {
        let lba = 800_100 + i;
        let seq = i + 1;
        let fill = 0x40 + i as u8;
        pool.note_latest_lba_seq_for_test("flush-race", Lba(lba), seq, 1);
        let unit = make_packed_unit_at(fill, seq, lba);
        let pba = allocator.allocate_one_for_lane(0).unwrap();
        sealed_slots.push(SealedSlot {
            pba,
            data: vec![fill; BLOCK_SIZE as usize],
            fragments: vec![crate::packer::packer::SlotFragment {
                unit,
                slot_offset: 0,
            }],
        });
        per_slot_buffers.push((vec![seq], Vec::new()));
    }

    install_test_failpoint(
        "flush-race",
        Lba(800_100),
        FlushFailStage::BeforeIoWrite,
        Some(1),
    );

    let in_flight = Arc::new(super::FlusherInFlightTracker::default());
    let (done_tx, _done_rx) = unbounded::<Vec<u64>>();
    let (cw_tx, cw_rx) = unbounded::<super::writer::CommitJob>();
    let mut retries = std::collections::VecDeque::new();

    BufferFlusher::write_packed_slots_batch(
        0,
        sealed_slots,
        per_slot_buffers,
        &pool,
        &allocator,
        &io_engine,
        None,
        &metrics,
        &in_flight,
        &done_tx,
        &mut retries,
        std::slice::from_ref(&cw_tx),
        1,
    );
    drop(cw_tx);
    clear_test_failpoint("flush-race", Lba(800_100), FlushFailStage::BeforeIoWrite);

    assert!(
        cw_rx.into_iter().next().is_none(),
        "a failed packed full-stripe group must dispatch no commit"
    );
    assert_eq!(
        retries.len(),
        6,
        "all six slots must be re-queued for retry"
    );
    // `free_block_count` == baseline is the exact no-leak / no-double-free
    // invariant: the stripe extent AND the six reassigned-away packer PBAs must
    // all be back on the free list. (Run count is intentionally not asserted —
    // the six individual packer allocations legitimately fragment the free list;
    // block count is the load-bearing check.)
    assert_eq!(
        allocator.free_block_count(),
        free_before,
        "stripe + freed packer PBAs must net to baseline — no leak, no double free"
    );
    let _ = runs_before;
    let vol = VolumeId("flush-race".into());
    for i in 0..6u64 {
        assert!(
            meta.get_mapping(&vol, Lba(800_100 + i)).unwrap().is_none(),
            "failed packed group must publish no mapping"
        );
    }
}
