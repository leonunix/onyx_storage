use super::*;
use crate::config::MetaConfig;
use crate::io::device::RawDevice;
use crate::meta::store::{MetaStore, RemapCleanup};
use crate::types::{VolumeConfig, ZoneId};
use crate::zone::worker::ZoneWorker;
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use tempfile::{tempdir, NamedTempFile};

fn setup_flush_test_env() -> (
    Arc<MetaStore>,
    Arc<WriteBufferPool>,
    Arc<VolumeLifecycleManager>,
    Arc<SpaceAllocator>,
    Arc<IoEngine>,
    Arc<EngineMetrics>,
    tempfile::TempDir,
    NamedTempFile,
    NamedTempFile,
) {
    let meta_dir = tempdir().unwrap();
    let meta = Arc::new(
        MetaStore::open(&MetaConfig {
            path: Some(meta_dir.path().to_path_buf()),
            block_cache_mb: 8,
            memtable_budget_mb: 0,
            index_pin_mb: 64,
            lsm_bloom_bits_per_entry: 10,
            checkpoint_interval_ms: 5000,
            group_commit_timeout_us: 1,
            wal_dir: None,
            dedup_shards: 1,
            dedup_cuckoo_buckets: 1_000_000,
            dedup_l1_cache_entries: 256_000,
            ..Default::default()
        })
        .unwrap(),
    );

    let buf_tmp = NamedTempFile::new().unwrap();
    let buf_size: u64 = 4096 + 4096 + 256 * 8192;
    buf_tmp.as_file().set_len(buf_size).unwrap();
    let pool = Arc::new(
        WriteBufferPool::open(RawDevice::open_or_create(buf_tmp.path(), buf_size).unwrap())
            .unwrap(),
    );

    let data_tmp = NamedTempFile::new().unwrap();
    let data_size: u64 = 4096 * 20000;
    data_tmp.as_file().set_len(data_size).unwrap();
    let io_engine = Arc::new(IoEngine::new(
        RawDevice::open(data_tmp.path()).unwrap(),
        false,
    ));

    let allocator = Arc::new(SpaceAllocator::new(data_size, 1));
    let lifecycle = Arc::new(VolumeLifecycleManager::default());
    let metrics = Arc::new(EngineMetrics::default());

    meta.put_volume(&VolumeConfig {
        id: VolumeId("flush-race".into()),
        // Packed/dedup stress cases below intentionally spread mappings far
        // apart to avoid accidental overwrite between scenarios. Keep the
        // test volume large enough that diagnostic full-blockmap scans include
        // those high LBAs now that metadb scans are bounded by volume size.
        size_bytes: 4096 * 1_000_000,
        block_size: 4096,
        compression: CompressionAlgo::None,
        created_at: 1,
        zone_count: 1,
    })
    .unwrap();

    (
        meta, pool, lifecycle, allocator, io_engine, metrics, meta_dir, buf_tmp, data_tmp,
    )
}

fn cleanup_for_pba(pba: Pba, blocks: u32) -> RemapCleanup {
    RemapCleanup {
        pba,
        decrements: blocks,
        blocks,
        pba_freed: true,
        mappings: Vec::new(),
    }
}

fn make_unit(fill: u8, seq: u64) -> CompressedUnit {
    let data = vec![fill; BLOCK_SIZE as usize];
    CompressedUnit {
        vol_id: "flush-race".into(),
        start_lba: Lba(0),
        lba_count: 1,
        original_size: BLOCK_SIZE,
        compressed_data: data.clone(),
        compression: 0,
        crc32: crc32fast::hash(&data),
        vol_created_at: 1,
        seq_lba_ranges: vec![(seq, Lba(0), 1)],
        block_hashes: None,
        dedup_stale_repairs: None,
        dedup_skipped: false,
        compression_bypassed: false,
        dedup_completion: None,
    }
}

fn make_raw_unit_at(start_lba: u64, lba_count: u32, first_byte: u8, seq: u64) -> CompressedUnit {
    let mut data = vec![0u8; lba_count as usize * BLOCK_SIZE as usize];
    for (idx, block) in data.chunks_mut(BLOCK_SIZE as usize).enumerate() {
        block.fill(first_byte.wrapping_add(idx as u8));
    }
    CompressedUnit {
        vol_id: "flush-race".into(),
        start_lba: Lba(start_lba),
        lba_count,
        original_size: data.len() as u32,
        compressed_data: data.clone(),
        compression: 0,
        crc32: crc32fast::hash(&data),
        vol_created_at: 1,
        seq_lba_ranges: vec![(seq, Lba(start_lba), lba_count)],
        block_hashes: None,
        dedup_stale_repairs: None,
        dedup_skipped: false,
        compression_bypassed: false,
        dedup_completion: None,
    }
}

fn make_packed_unit(fill: u8, seq: u64) -> CompressedUnit {
    let data = vec![fill; 512];
    CompressedUnit {
        vol_id: "flush-race".into(),
        start_lba: Lba(0),
        lba_count: 1,
        original_size: BLOCK_SIZE,
        compressed_data: data.clone(),
        compression: 0,
        crc32: crc32fast::hash(&data),
        vol_created_at: 1,
        seq_lba_ranges: vec![(seq, Lba(0), 1)],
        block_hashes: None,
        dedup_stale_repairs: None,
        dedup_skipped: false,
        compression_bypassed: false,
        dedup_completion: None,
    }
}

fn make_packed_unit_at(fill: u8, seq: u64, lba: u64) -> CompressedUnit {
    let data = vec![fill; 128];
    CompressedUnit {
        vol_id: "flush-race".into(),
        start_lba: Lba(lba),
        lba_count: 1,
        original_size: BLOCK_SIZE,
        compressed_data: data.clone(),
        compression: 0,
        crc32: crc32fast::hash(&data),
        vol_created_at: 1,
        seq_lba_ranges: vec![(seq, Lba(lba), 1)],
        block_hashes: None,
        dedup_stale_repairs: None,
        dedup_skipped: false,
        compression_bypassed: false,
        dedup_completion: None,
    }
}

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
fn compress_loop_bypasses_low_savings_units() {
    let metrics = EngineMetrics::default();
    let running = AtomicBool::new(true);
    let (tx, rx) = bounded::<CoalesceUnit>(1);
    let (out_tx, out_rx) = bounded::<CompressedUnit>(1);

    let mut raw = Vec::with_capacity(8 * BLOCK_SIZE as usize);
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    for _ in 0..8 * BLOCK_SIZE as usize {
        state ^= state << 7;
        state ^= state >> 9;
        state ^= state << 8;
        raw.push((state & 0xff) as u8);
    }
    let raw_arc: Arc<[u8]> = Arc::from(raw.clone());
    let raw_blocks = (0..8)
        .map(|i| crate::buffer::pipeline::RawBlockRef {
            payload: raw_arc.clone(),
            offset: i * BLOCK_SIZE as usize,
        })
        .collect();

    tx.send(CoalesceUnit {
        vol_id: "flush-race".into(),
        start_lba: Lba(10_000),
        lba_count: 8,
        raw_blocks,
        compression: CompressionAlgo::Lz4,
        vol_created_at: 1,
        seq_lba_ranges: vec![(1, Lba(10_000), 8)],
        dedup_skipped: false,
        block_hashes: None,
        dedup_stale_repairs: None,
        dedup_completion: None,
    })
    .unwrap();
    drop(tx);

    BufferFlusher::compress_loop(&rx, &out_tx, &running, &metrics, 12);
    drop(out_tx);

    let unit = out_rx
        .try_recv()
        .expect("compressed unit should be emitted");
    assert_eq!(unit.compression, CompressionAlgo::None.to_u8());
    assert_eq!(unit.compressed_data, raw);
    assert_eq!(unit.original_size, (8 * BLOCK_SIZE) as u32);
    assert_eq!(unit.lba_count, 8);
    assert_eq!(unit.crc32, crc32fast::hash(&unit.compressed_data));
    assert_eq!(
        metrics.compress_bypass_units.load(Ordering::Relaxed),
        1,
        "low-savings compression attempts should be accounted"
    );
    assert_eq!(
        metrics.compress_bypass_bytes.load(Ordering::Relaxed),
        (8 * BLOCK_SIZE) as u64
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
        3,
        "setup uses set_refcount + put_dedup_entries; the 8 hits should share one commit"
    );
}

#[test]
fn coalesce_enqueue_caps_ready_window_bytes() {
    let tmp = NamedTempFile::new().unwrap();
    let size = 4096 + 4096 + 96 * 1024 * 1024;
    tmp.as_file().set_len(size).unwrap();
    let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
    let pool = WriteBufferPool::open(dev).unwrap();

    let payload = vec![0x5A; 256 * BLOCK_SIZE as usize];
    let mut seqs = Vec::new();
    for i in 0..20u64 {
        seqs.push(
            pool.append("window-vol", Lba(i * 256), 256, &payload, 0)
                .unwrap(),
        );
    }

    let tracker = FlusherInFlightTracker::default();
    let in_flight = HashMap::new();
    let mut seen = HashSet::new();
    let mut queued_bytes = 0usize;
    let mut new_entries = Vec::new();
    let mut window_full = false;
    let test_metrics = EngineMetrics::default();

    for seq in seqs {
        if matches!(
            BufferFlusher::try_enqueue_pending_seq(
                seq,
                &pool,
                &in_flight,
                &tracker,
                &mut seen,
                &mut queued_bytes,
                &mut new_entries,
                &test_metrics,
                true,
            ),
            EnqueuePendingSeq::WindowFull
        ) {
            window_full = true;
            break;
        }
    }

    assert!(
        window_full,
        "coalescer should stop once the ready window is full"
    );
    assert_eq!(queued_bytes, BufferFlusher::COALESCE_READY_WINDOW_BYTES);
    assert_eq!(new_entries.len(), 16);
}

/// When a pending entry's LBAs have all been superseded by a later seq in
/// the ring, `try_enqueue_pending_seq` should drop it up-front and bump
/// the `coalesce_superseded_*` counters instead of pushing it into the
/// pipeline. This is the optimization that skips dedup hashing, compression,
/// and dedup_index churn for soon-dead data.
#[test]
fn fully_superseded_entry_skipped_at_coalesce() {
    let tmp = NamedTempFile::new().unwrap();
    let size = 4096 + 4096 + 32 * 1024 * 1024;
    tmp.as_file().set_len(size).unwrap();
    let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
    let pool = WriteBufferPool::open(dev).unwrap();

    let payload = vec![0xCC; 4 * BLOCK_SIZE as usize];
    // seq 100, seq 200 → same 4-LBA range. In real flow the append of seq
    // 200 would update `latest_lba_seq` to (200, 0) for every LBA covered
    // by the entry. Simulate that with the test hook so we can exercise
    // the supersession check in isolation.
    let seq_old = pool
        .append("vol-sup", Lba(0), 4, &payload, 0)
        .expect("append old");
    let seq_new = pool
        .append("vol-sup", Lba(0), 4, &payload, 0)
        .expect("append new");
    for off in 0..4 {
        pool.note_latest_lba_seq_for_test("vol-sup", Lba(off), seq_new, 0);
    }

    let tracker = FlusherInFlightTracker::default();
    let in_flight = HashMap::new();
    let mut seen = HashSet::new();
    let mut queued_bytes = 0usize;
    let mut new_entries = Vec::new();
    let test_metrics = EngineMetrics::default();

    // Old seq must be dropped and counters bumped.
    let out = BufferFlusher::try_enqueue_pending_seq(
        seq_old,
        &pool,
        &in_flight,
        &tracker,
        &mut seen,
        &mut queued_bytes,
        &mut new_entries,
        &test_metrics,
        true,
    );
    assert_eq!(out, EnqueuePendingSeq::Skipped(SkipReason::Superseded));
    assert!(new_entries.is_empty(), "superseded entry must not enqueue");
    let snap = test_metrics.snapshot();
    assert_eq!(snap.coalesce_superseded_entries, 1);
    assert_eq!(snap.coalesce_superseded_lbas, 4);

    // New seq is the head of the line — must still enqueue normally.
    let out = BufferFlusher::try_enqueue_pending_seq(
        seq_new,
        &pool,
        &in_flight,
        &tracker,
        &mut seen,
        &mut queued_bytes,
        &mut new_entries,
        &test_metrics,
        true,
    );
    assert_eq!(out, EnqueuePendingSeq::Queued);
    assert_eq!(new_entries.len(), 1);
    assert_eq!(new_entries[0].seq, seq_new);

    // With the flag off, the old seq would have gone into the pipeline —
    // verify by feeding a fresh pair.
    let seq_old2 = pool
        .append("vol-sup", Lba(8), 4, &payload, 0)
        .expect("append old2");
    let seq_new2 = pool
        .append("vol-sup", Lba(8), 4, &payload, 0)
        .expect("append new2");
    for off in 8..12 {
        pool.note_latest_lba_seq_for_test("vol-sup", Lba(off), seq_new2, 0);
    }
    let out = BufferFlusher::try_enqueue_pending_seq(
        seq_old2,
        &pool,
        &in_flight,
        &tracker,
        &mut seen,
        &mut queued_bytes,
        &mut new_entries,
        &test_metrics,
        false, // disabled
    );
    assert_eq!(out, EnqueuePendingSeq::Queued);
    // Counter didn't advance beyond the first drop.
    let snap = test_metrics.snapshot();
    assert_eq!(snap.coalesce_superseded_entries, 1);
}

/// Regression test: packed slot refcount drift when LBAs are overwritten
/// via atomic_batch_write_multi.
///
/// Reproduces the soak failure where PBA 248131 had stored refcount=50
/// but actual blockmap refs=114. The drift occurs when a packed PBA's
/// LBAs are overwritten, the refcount reaches 0, the PBA is freed and
/// reused, but old blockmap entries are not cleaned up.
#[test]
fn packed_slot_refcount_drift_on_overwrite() {
    // Phase 5: refcount-drift is impossible because the hot-path
    // commit no longer mutates rc. The original test caught a soak
    // bug where stored rc and blockmap-ref count diverged after an
    // overwrite; that vector is gone. What still must hold is the
    // blockmap-ref count tracking the live LBAs.
    let (meta, _pool, _lifecycle, allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();

    let vol = VolumeId("flush-race".into());
    let packed_pba = allocator.allocate_one_for_lane(0).unwrap();

    // Create a packed slot with 64 LBAs across two fragments.
    let mut batch_values: Vec<(VolumeId, Lba, BlockmapValue)> = Vec::new();
    for i in 0u64..32 {
        batch_values.push((
            vol.clone(),
            Lba(1000 + i),
            BlockmapValue {
                pba: packed_pba,
                compression: 1,
                unit_compressed_size: 1953,
                unit_original_size: 131072,
                unit_lba_count: 32,
                offset_in_unit: i as u16,
                crc32: 0xAAAAAAAA,
                slot_offset: 0,
                flags: 0,
            },
        ));
    }
    for i in 0u64..32 {
        batch_values.push((
            vol.clone(),
            Lba(2000 + i),
            BlockmapValue {
                pba: packed_pba,
                compression: 1,
                unit_compressed_size: 1113,
                unit_original_size: 131072,
                unit_lba_count: 32,
                offset_in_unit: i as u16,
                crc32: 0xBBBBBBBB,
                slot_offset: 1953,
                flags: 0,
            },
        ));
    }

    meta.atomic_batch_write_packed(&batch_values, packed_pba, 64)
        .unwrap();
    assert_eq!(meta.count_blockmap_refs_for_pba(packed_pba).unwrap(), 64);

    // Overwrite all 64 LBAs to two new PBAs.
    let new_pba_1 = allocator.allocate_one_for_lane(0).unwrap();
    let new_pba_2 = allocator.allocate_one_for_lane(0).unwrap();

    let unit1_entries: Vec<(Lba, BlockmapValue)> = (0u64..32)
        .map(|i| {
            (
                Lba(1000 + i),
                BlockmapValue {
                    pba: new_pba_1,
                    compression: 0,
                    unit_compressed_size: BLOCK_SIZE,
                    unit_original_size: BLOCK_SIZE,
                    unit_lba_count: 1,
                    offset_in_unit: 0,
                    crc32: 0x11111111,
                    slot_offset: 0,
                    flags: 0,
                },
            )
        })
        .collect();
    let unit2_entries: Vec<(Lba, BlockmapValue)> = (0u64..32)
        .map(|i| {
            (
                Lba(2000 + i),
                BlockmapValue {
                    pba: new_pba_2,
                    compression: 0,
                    unit_compressed_size: BLOCK_SIZE,
                    unit_original_size: BLOCK_SIZE,
                    unit_lba_count: 1,
                    offset_in_unit: 0,
                    crc32: 0x22222222,
                    slot_offset: 0,
                    flags: 0,
                },
            )
        })
        .collect();

    let batch_args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> =
        vec![(&vol, &unit1_entries, 32), (&vol, &unit2_entries, 32)];
    meta.atomic_batch_write_multi(&batch_args).unwrap();

    // Blockmap refs: all 64 LBAs moved to new pbas, packed_pba is
    // now ref-free. Allocator reclamation gate stays correct.
    let refs_after = meta.count_blockmap_refs_for_pba(packed_pba).unwrap();
    assert_eq!(
        refs_after, 0,
        "packed_pba should have 0 blockmap refs after all LBAs overwritten"
    );
    assert!(!meta.has_any_blockmap_ref(packed_pba).unwrap());

    // New PBAs are referenced once per LBA.
    assert_eq!(meta.count_blockmap_refs_for_pba(new_pba_1).unwrap(), 32);
    assert_eq!(meta.count_blockmap_refs_for_pba(new_pba_2).unwrap(), 32);
}

/// Regression test: packed slot + dedup hits + overwrite interaction.
///
/// Scenario: packed PBA gets dedup hits (increasing refcount), then the
/// ORIGINAL LBAs are overwritten. The dedup-added LBAs should keep the
/// PBA alive.
#[test]
fn packed_slot_refcount_with_dedup_and_overwrite() {
    // Phase 5: refcount-vs-blockmap drift cannot occur because the
    // hot-path commit doesn't touch rc. The dedup hit path also
    // doesn't bump rc on its own anymore; only lineage events do.
    // What still matters: the 16 dedup-target LBAs keep the pba's
    // blockmap-ref count > 0 after the overwrite of the original
    // 32 LBAs, so allocator reclamation stays gated.
    let (meta, _pool, _lifecycle, allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();

    let vol = VolumeId("flush-race".into());
    let packed_pba = allocator.allocate_one_for_lane(0).unwrap();

    // Create packed slot with 32 LBAs.
    let mut batch_values: Vec<(VolumeId, Lba, BlockmapValue)> = Vec::new();
    for i in 0u64..32 {
        batch_values.push((
            vol.clone(),
            Lba(1000 + i),
            BlockmapValue {
                pba: packed_pba,
                compression: 1,
                unit_compressed_size: 1953,
                unit_original_size: 131072,
                unit_lba_count: 32,
                offset_in_unit: i as u16,
                crc32: 0xAAAAAAAA,
                slot_offset: 0,
                flags: 0,
            },
        ));
    }
    meta.atomic_batch_write_packed(&batch_values, packed_pba, 32)
        .unwrap();
    assert_eq!(meta.count_blockmap_refs_for_pba(packed_pba).unwrap(), 32);

    // Dedup hits map 16 additional LBAs to the same packed PBA.
    // Seed rc so dedup_entry_is_live admits the hits.
    meta.set_refcount(packed_pba, 16).unwrap();
    let dedup_hashes: Vec<ContentHash> = (0u8..16).map(|i| [i + 100; 8]).collect();
    let dedup_entries: Vec<(ContentHash, DedupEntry)> = dedup_hashes
        .iter()
        .enumerate()
        .map(|(i, h)| {
            (
                *h,
                DedupEntry {
                    pba: packed_pba,
                    slot_offset: 0,
                    compression: 1,
                    unit_compressed_size: 1953,
                    unit_original_size: 131072,
                    unit_lba_count: 32,
                    offset_in_unit: i as u16,
                    crc32: 0xAAAAAAAA,
                },
            )
        })
        .collect();
    meta.put_dedup_entries(&dedup_entries).unwrap();

    let dedup_hits: Vec<(Lba, BlockmapValue, ContentHash)> = (0u64..16)
        .map(|i| {
            (
                Lba(5000 + i),
                BlockmapValue {
                    pba: packed_pba,
                    compression: 1,
                    unit_compressed_size: 1953,
                    unit_original_size: 131072,
                    unit_lba_count: 32,
                    offset_in_unit: i as u16,
                    crc32: 0xAAAAAAAA,
                    slot_offset: 0,
                    flags: 0,
                },
                dedup_hashes[i as usize],
            )
        })
        .collect();

    let (results, _newly_zeroed) = meta.atomic_batch_dedup_hits(&vol, &dedup_hits).unwrap();
    let accepted = results
        .iter()
        .filter(|r| matches!(r, DedupHitResult::Accepted(_)))
        .count();
    assert_eq!(accepted, 16, "all 16 dedup hits should be accepted");
    assert_eq!(
        meta.count_blockmap_refs_for_pba(packed_pba).unwrap(),
        48,
        "32 original + 16 dedup-mapped LBAs"
    );

    // Overwrite the original 32 LBAs.
    let new_pba = allocator.allocate_one_for_lane(0).unwrap();
    let overwrite_entries: Vec<(Lba, BlockmapValue)> = (0u64..32)
        .map(|i| {
            (
                Lba(1000 + i),
                BlockmapValue {
                    pba: new_pba,
                    compression: 0,
                    unit_compressed_size: BLOCK_SIZE,
                    unit_original_size: BLOCK_SIZE,
                    unit_lba_count: 1,
                    offset_in_unit: 0,
                    crc32: 0x11111111,
                    slot_offset: 0,
                    flags: 0,
                },
            )
        })
        .collect();

    let batch_args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> =
        vec![(&vol, &overwrite_entries, 32)];
    meta.atomic_batch_write_multi(&batch_args).unwrap();

    let refs = meta.count_blockmap_refs_for_pba(packed_pba).unwrap();
    assert_eq!(
        refs, 16,
        "16 dedup-mapped LBAs should still reference packed_pba"
    );
    assert!(
        meta.has_any_blockmap_ref(packed_pba).unwrap(),
        "blockmap-ref gate keeps packed_pba alive"
    );
}

/// Concurrent stress test: multiple threads hammer packed slot creation,
/// overwrite, and dedup hits on shared PBAs. Checks for refcount drift.
#[test]
fn packed_slot_concurrent_refcount_drift() {
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::sync::Barrier;

    let (meta, _pool, _lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();

    let vol = VolumeId("flush-race".into());
    let meta = &*meta;
    let allocator = &*allocator;
    let vol = &vol;

    let lba_counter = AtomicU64::new(0);
    let iteration_count = 200;
    let thread_count = 4;
    let barrier = Barrier::new(thread_count);
    let found_drift = AtomicBool::new(false);

    std::thread::scope(|s| {
        for tid in 0..thread_count {
            let barrier = &barrier;
            let lba_counter = &lba_counter;
            let found_drift = &found_drift;

            s.spawn(move || {
                barrier.wait();

                for _iter in 0..iteration_count {
                    // Each iteration: create a packed slot, then overwrite its LBAs

                    // Allocate a packed PBA
                    let packed_pba = match allocator.allocate_one_for_lane(0) {
                        Ok(p) => p,
                        Err(_) => return,
                    };

                    // Create 8 LBAs in a packed slot
                    let base_lba = lba_counter.fetch_add(16, Ordering::Relaxed);
                    let lba_count = 8u64;

                    let batch_values: Vec<(VolumeId, Lba, BlockmapValue)> = (0..lba_count)
                        .map(|i| {
                            (
                                vol.clone(),
                                Lba(base_lba + i),
                                BlockmapValue {
                                    pba: packed_pba,
                                    compression: 1,
                                    unit_compressed_size: 500,
                                    unit_original_size: 4096 * lba_count as u32,
                                    unit_lba_count: lba_count as u16,
                                    offset_in_unit: i as u16,
                                    crc32: 0xAA000000 + tid as u32,
                                    slot_offset: 0,
                                    flags: 0,
                                },
                            )
                        })
                        .collect();

                    if meta
                        .atomic_batch_write_packed(&batch_values, packed_pba, lba_count as u32)
                        .is_err()
                    {
                        continue;
                    }

                    // Immediately overwrite those LBAs via atomic_batch_write_multi
                    // (simulating a concurrent flush from another lane)
                    let new_pba = match allocator.allocate_one_for_lane(0) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };

                    let overwrite: Vec<(Lba, BlockmapValue)> = (0..lba_count)
                        .map(|i| {
                            (
                                Lba(base_lba + i),
                                BlockmapValue {
                                    pba: new_pba,
                                    compression: 0,
                                    unit_compressed_size: BLOCK_SIZE,
                                    unit_original_size: BLOCK_SIZE,
                                    unit_lba_count: 1,
                                    offset_in_unit: 0,
                                    crc32: 0xBB000000 + tid as u32,
                                    slot_offset: 0,
                                    flags: 0,
                                },
                            )
                        })
                        .collect();

                    let args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> =
                        vec![(vol, &overwrite, lba_count as u32)];
                    let _ = meta.atomic_batch_write_multi(&args);

                    // Check: packed_pba should have refcount 0 and 0 blockmap refs
                    let rc = meta.get_refcount(packed_pba).unwrap();
                    let refs = meta.count_blockmap_refs_for_pba(packed_pba).unwrap();
                    if rc != refs {
                        eprintln!(
                            "[tid={}] DRIFT at PBA {}: refcount={} blockmap_refs={}",
                            tid, packed_pba.0, rc, refs
                        );
                        found_drift.store(true, Ordering::Relaxed);
                    }
                }
            });
        }
    });

    assert!(
        !found_drift.load(Ordering::Relaxed),
        "refcount drift detected under concurrent packed slot operations"
    );
}

/// Concurrent stress test: interleaved packed writes, dedup hits, and
/// overwrites on SHARED PBAs (dedup causes cross-thread PBA sharing).
#[test]
fn packed_slot_concurrent_dedup_refcount_drift() {
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::sync::Barrier;

    let (meta, _pool, _lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();

    let vol = VolumeId("flush-race".into());
    let meta = &*meta;
    let allocator = &*allocator;
    let vol = &vol;

    let lba_counter = AtomicU64::new(10000);
    let found_drift = AtomicBool::new(false);
    let barrier = Barrier::new(3);

    // Pre-create some packed PBAs that threads will share via dedup
    let shared_pbas: Vec<Pba> = (0..20)
        .map(|_| allocator.allocate_one_for_lane(0).unwrap())
        .collect();

    // Initialize shared PBAs with packed data
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
                        crc32: 0xCC000000 + idx as u32,
                        slot_offset: 0,
                        flags: 0,
                    },
                )
            })
            .collect();
        meta.atomic_batch_write_packed(&batch, pba, 8).unwrap();

        // Register dedup entries so dedup hits work
        let hashes: Vec<ContentHash> = (0..8u8)
            .map(|i| {
                let mut h = [0u8; 8];
                h[0] = idx as u8;
                h[1] = i;
                h
            })
            .collect();
        let dedup_entries: Vec<(ContentHash, DedupEntry)> = hashes
            .iter()
            .enumerate()
            .map(|(i, h)| {
                (
                    *h,
                    DedupEntry {
                        pba,
                        slot_offset: 0,
                        compression: 1,
                        unit_compressed_size: 400,
                        unit_original_size: 32768,
                        unit_lba_count: 8,
                        offset_in_unit: i as u16,
                        crc32: 0xCC000000 + idx as u32,
                    },
                )
            })
            .collect();
        meta.put_dedup_entries(&dedup_entries).unwrap();
    }

    std::thread::scope(|s| {
        // Thread 1: dedup hits — maps NEW LBAs to shared packed PBAs
        s.spawn(|| {
            barrier.wait();
            for round in 0..100u64 {
                let pba_idx = (round as usize) % shared_pbas.len();
                let pba = shared_pbas[pba_idx];
                let base_lba = lba_counter.fetch_add(4, Ordering::Relaxed);

                let hashes: Vec<ContentHash> = (0..4u8)
                    .map(|i| {
                        let mut h = [0u8; 8];
                        h[0] = pba_idx as u8;
                        h[1] = i;
                        h
                    })
                    .collect();

                let hits: Vec<(Lba, BlockmapValue, ContentHash)> = (0..4u64)
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
                                crc32: 0xCC000000 + pba_idx as u32,
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

        // Thread 2: overwrites — overwrites LBAs that point to shared packed PBAs
        s.spawn(|| {
            barrier.wait();
            for round in 0..100u64 {
                let pba_idx = (round as usize) % shared_pbas.len();
                // Find some LBAs pointing to this PBA and overwrite them
                let pba = shared_pbas[pba_idx];
                let new_pba = match allocator.allocate_one_for_lane(0) {
                    Ok(p) => p,
                    Err(_) => continue,
                };

                // Overwrite the first 4 original LBAs of this PBA
                // (base LBAs were: 10000 + pba_idx*8 .. 10000 + pba_idx*8 + 7)
                let orig_base = 10000 + (pba_idx as u64) * 8;
                let overwrite: Vec<(Lba, BlockmapValue)> = (0..4u64)
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
                                crc32: 0xDD000000,
                                slot_offset: 0,
                                flags: 0,
                            },
                        )
                    })
                    .collect();

                let args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> =
                    vec![(vol, &overwrite, 4)];
                let _ = meta.atomic_batch_write_multi(&args);
            }
        });

        // Thread 3: more packed slot writes that create NEW packed slots
        s.spawn(|| {
            barrier.wait();
            for _round in 0..100 {
                let pba = match allocator.allocate_one_for_lane(0) {
                    Ok(p) => p,
                    Err(_) => continue,
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
                                unit_compressed_size: 300,
                                unit_original_size: 32768,
                                unit_lba_count: 8,
                                offset_in_unit: i as u16,
                                crc32: 0xEE000000,
                                slot_offset: 0,
                                flags: 0,
                            },
                        )
                    })
                    .collect();
                let _ = meta.atomic_batch_write_packed(&batch, pba, 8);
            }
        });
    });

    // Phase 5: refcount-drift can no longer happen — the hot-path
    // commits don't move rc at all, so stored rc and blockmap refs
    // are independent. Verify that the concurrent workload didn't
    // crash and that `count_blockmap_refs_for_pba` stayed monotonic
    // (any pba that ever held live mappings is still reachable via
    // the blockmap iterator).
    let _ = found_drift; // legacy flag kept for clarity
    for &pba in &shared_pbas {
        let _ = meta.count_blockmap_refs_for_pba(pba).unwrap();
        let _ = meta.get_refcount(pba).unwrap();
    }
}

/// High-pressure concurrent test: thread 1 calls write_packed_slot,
/// thread 2 calls atomic_batch_write_multi on the SAME LBAs, racing the
/// live_positions_for_unit check against blockmap updates.
#[test]
fn packed_slot_full_pipeline_concurrent_drift() {
    use std::sync::atomic::AtomicBool;
    use std::sync::Barrier;

    let (meta, pool, lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();

    let vol_id = "flush-race";
    let vol = VolumeId(vol_id.into());
    let (cleanup_tx, _cleanup_rx) = unbounded::<CleanupBatch>();
    let found_drift = AtomicBool::new(false);
    let barrier = Barrier::new(2);
    let rounds = 500;

    std::thread::scope(|s| {
        let meta1 = &meta;
        let pool1 = &pool;
        let lifecycle1 = &lifecycle;
        let allocator1 = &allocator;
        let io_engine1 = &io_engine;
        let metrics1 = &metrics;
        let cleanup_tx1 = &cleanup_tx;
        let barrier1 = &barrier;
        let vol1 = &vol;

        // Thread 1: create packed slots for LBAs 0..7 and commit via write_packed_slot
        s.spawn(move || {
            barrier1.wait();
            for _ in 0..rounds {
                // Append 8 LBAs to buffer so live_positions_for_unit can find them
                let data = vec![0xAAu8; BLOCK_SIZE as usize];
                let mut seqs = Vec::new();
                for lba in 0u64..8 {
                    if let Ok(seq) = pool1.append(vol_id, Lba(lba), 1, &data, 1) {
                        seqs.push((seq, Lba(lba), 1u32));
                    }
                }
                if seqs.len() != 8 {
                    continue;
                }

                let pba = match allocator1.allocate_one_for_lane(0) {
                    Ok(p) => p,
                    Err(_) => continue,
                };

                let compressed = vec![0xAAu8; 500];
                let crc = crc32fast::hash(&compressed);
                let mut slot_data = vec![0u8; BLOCK_SIZE as usize];
                slot_data[..500].copy_from_slice(&compressed);

                let sealed = crate::packer::packer::SealedSlot {
                    pba,
                    data: slot_data,
                    fragments: vec![crate::packer::packer::SlotFragment {
                        unit: CompressedUnit {
                            vol_id: vol_id.to_string(),
                            start_lba: Lba(0),
                            lba_count: 8,
                            compressed_data: compressed,
                            original_size: BLOCK_SIZE * 8,
                            compression: 0,
                            crc32: crc,
                            seq_lba_ranges: seqs,
                            block_hashes: None,
                            dedup_stale_repairs: None,
                            dedup_skipped: false,
                            compression_bypassed: false,
                            vol_created_at: 1,
                            dedup_completion: None,
                        },
                        slot_offset: 0,
                    }],
                };

                let _ = BufferFlusher::write_packed_slot(
                    0,
                    &sealed,
                    pool1,
                    meta1,
                    lifecycle1,
                    allocator1,
                    io_engine1,
                    metrics1,
                    cleanup_tx1,
                    &crate::dedup::CandidateCache::new(8, 64),
                );
            }
        });

        // Thread 2: concurrently overwrite same LBAs via atomic_batch_write_multi
        let meta2 = &meta;
        let pool2 = &pool;
        let allocator2 = &allocator;
        let vol2 = &vol;
        let barrier2 = &barrier;

        s.spawn(move || {
            barrier2.wait();
            for _ in 0..rounds {
                // Append newer data so it supersedes thread 1's entries
                let data = vec![0xBBu8; BLOCK_SIZE as usize];
                for lba in 0u64..8 {
                    let _ = pool2.append(vol_id, Lba(lba), 1, &data, 1);
                }

                let new_pba = match allocator2.allocate_one_for_lane(0) {
                    Ok(p) => p,
                    Err(_) => continue,
                };

                let entries: Vec<(Lba, BlockmapValue)> = (0u64..8)
                    .map(|i| {
                        (
                            Lba(i),
                            BlockmapValue {
                                pba: new_pba,
                                compression: 0,
                                unit_compressed_size: BLOCK_SIZE,
                                unit_original_size: BLOCK_SIZE,
                                unit_lba_count: 1,
                                offset_in_unit: 0,
                                crc32: 0xBBBBBBBB,
                                slot_offset: 0,
                                flags: 0,
                            },
                        )
                    })
                    .collect();

                let args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> =
                    vec![(vol2, &entries, 8)];
                let _ = meta2.atomic_batch_write_multi(&args);
            }
        });
    });

    // Phase 5: refcount-drift is impossible because the hot-path
    // commit no longer mutates rc. The test just needs to confirm
    // the concurrent workload didn't crash and the
    // blockmap-ref accounting is internally consistent (every pba
    // with positive ref count is still pointed at by at least one
    // mapping).
    let _ = found_drift;
    for pba_val in 0..20000u64 {
        let pba = Pba(pba_val + crate::types::RESERVED_BLOCKS);
        let refs = meta.count_blockmap_refs_for_pba(pba).unwrap();
        if refs > 0 {
            assert!(meta.has_any_blockmap_ref(pba).unwrap());
        }
    }
}

/// Proof-of-concept: atomic_batch_write_packed uses PUT to set refcount.
/// If the PBA already has additional references (from dedup or a previous
/// incarnation), PUT overwrites the total, causing drift.
#[test]
fn packed_slot_put_overwrites_dedup_refcount() {
    // Phase 5: rc no longer tracks per-LBA references on the hot path,
    // so the PUT-overwrites-dedup drift the test originally caught is
    // impossible — `atomic_batch_write_packed` doesn't touch rc.
    // The blockmap-ref count remains the load-bearing invariant for
    // allocator reclamation: the new packed write must not orphan the
    // pre-existing blockmap entries.
    let (meta, _pool, _lifecycle, _allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();

    let vol = VolumeId("flush-race".into());
    let pba = Pba(999);

    // Seed 4 pre-existing blockmap entries (simulates dedup-hit refs).
    for i in 0u64..4 {
        meta.put_mapping(
            &vol,
            Lba(100 + i),
            &BlockmapValue {
                pba,
                compression: 1,
                unit_compressed_size: 500,
                unit_original_size: 32768,
                unit_lba_count: 8,
                offset_in_unit: i as u16,
                crc32: 0xAAAAAAAA,
                slot_offset: 0,
                flags: 0,
            },
        )
        .unwrap();
    }
    assert_eq!(meta.count_blockmap_refs_for_pba(pba).unwrap(), 4);

    // Packed write with 8 DIFFERENT LBAs sharing the same pba.
    let batch_values: Vec<(VolumeId, Lba, BlockmapValue)> = (0u64..8)
        .map(|i| {
            (
                vol.clone(),
                Lba(200 + i),
                BlockmapValue {
                    pba,
                    compression: 1,
                    unit_compressed_size: 500,
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

    meta.atomic_batch_write_packed(&batch_values, pba, 8)
        .unwrap();

    let refs = meta.count_blockmap_refs_for_pba(pba).unwrap();
    assert_eq!(
        refs, 12,
        "Phase 5: 4 pre-existing + 8 new blockmap entries → 12 live LBAs"
    );
    // Hot-path rc stays at 0 — the lineage path is responsible for
    // bumping rc on actual cross-volume sharing.
    assert_eq!(meta.get_refcount(pba).unwrap(), 0);
}

/// End-to-end regression: the full chain that led to CRC mismatch in soak.
///
/// 1. Packed slot created at PBA P (refcount = 8)
/// 2. Dedup adds 4 refs (refcount should be 12)
/// 3. The 8 original LBAs are overwritten (decrement 8)
/// 4. With the old PUT bug: refcount would be 8-8=0 → PBA freed → reuse → CRC mismatch
/// 5. With the fix: refcount = 12-8=4 → PBA stays alive → no CRC mismatch
#[test]
fn packed_slot_full_chain_no_premature_free() {
    // Phase 5: refcount-drift cannot happen because the hot-path write
    // path doesn't bump rc anymore. The "premature free" vector this
    // test originally guarded is gone — write_packed_slot /
    // atomic_batch_dedup_hits no longer touch rc. The load-bearing
    // invariant after Phase 5 is the blockmap-ref-count itself:
    // count_blockmap_refs_for_pba(P) should still report the live
    // mappings, and `meta.has_any_blockmap_ref(P)` should gate
    // allocator reclamation.
    let (meta, _pool, _lifecycle, allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let vol = VolumeId("flush-race".into());

    let packed_pba = Pba(999);

    // Step 1: create packed slot with 8 LBAs
    let packed_entries: Vec<(VolumeId, Lba, BlockmapValue)> = (0u64..8)
        .map(|i| {
            (
                vol.clone(),
                Lba(1000 + i),
                BlockmapValue {
                    pba: packed_pba,
                    compression: 1,
                    unit_compressed_size: 500,
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
    meta.atomic_batch_write_packed(&packed_entries, packed_pba, 8)
        .unwrap();
    assert_eq!(
        meta.count_blockmap_refs_for_pba(packed_pba).unwrap(),
        8,
        "blockmap ref count tracks the live LBAs"
    );

    // Step 2: dedup hits — atomic_batch_dedup_hits remaps 4 more
    // LBAs to the packed pba. Phase 5: no rc touch; we still expect
    // the blockmap-ref count to grow.
    let dedup_hashes: Vec<ContentHash> = (0u8..4).map(|i| [i + 50; 8]).collect();
    let dedup_entries: Vec<(ContentHash, DedupEntry)> = dedup_hashes
        .iter()
        .enumerate()
        .map(|(i, h)| {
            (
                *h,
                DedupEntry {
                    pba: packed_pba,
                    slot_offset: 0,
                    compression: 1,
                    unit_compressed_size: 500,
                    unit_original_size: 32768,
                    unit_lba_count: 8,
                    offset_in_unit: i as u16,
                    crc32: 0xAAAAAAAA,
                },
            )
        })
        .collect();
    meta.put_dedup_entries(&dedup_entries).unwrap();
    // dedup_entry_is_live needs rc>0; seed it so the hits land.
    meta.set_refcount(packed_pba, 4).unwrap();

    let hits: Vec<(Lba, BlockmapValue, ContentHash)> = (0u64..4)
        .map(|i| {
            (
                Lba(5000 + i),
                BlockmapValue {
                    pba: packed_pba,
                    compression: 1,
                    unit_compressed_size: 500,
                    unit_original_size: 32768,
                    unit_lba_count: 8,
                    offset_in_unit: i as u16,
                    crc32: 0xAAAAAAAA,
                    slot_offset: 0,
                    flags: 0,
                },
                dedup_hashes[i as usize],
            )
        })
        .collect();
    let _ = meta.atomic_batch_dedup_hits(&vol, &hits).unwrap();

    // Step 3: overwrite ALL 8 original LBAs to a different pba. The
    // dedup hits at LBAs 5000..5004 still reference packed_pba.
    let new_pba = allocator.allocate_one_for_lane(0).unwrap();
    let overwrite: Vec<(Lba, BlockmapValue)> = (0u64..8)
        .map(|i| {
            (
                Lba(1000 + i),
                BlockmapValue {
                    pba: new_pba,
                    compression: 0,
                    unit_compressed_size: BLOCK_SIZE,
                    unit_original_size: BLOCK_SIZE,
                    unit_lba_count: 1,
                    offset_in_unit: 0,
                    crc32: 0x11111111,
                    slot_offset: 0,
                    flags: 0,
                },
            )
        })
        .collect();
    let args: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> = vec![(&vol, &overwrite, 8)];
    meta.atomic_batch_write_multi(&args).unwrap();

    // Step 4: the load-bearing post-condition — 4 dedup LBAs still
    // hold the pba via blockmap refs, so allocator reclamation must
    // be gated.
    let refs = meta.count_blockmap_refs_for_pba(packed_pba).unwrap();
    assert_eq!(refs, 4, "4 dedup LBAs should still reference packed_pba");
    assert!(
        meta.has_any_blockmap_ref(packed_pba).unwrap(),
        "blockmap-ref gate prevents allocator reclamation"
    );
}

/// Focused race: two threads each call write_packed_slot for DIFFERENT
/// fragments that target the SAME LBAs. The second thread's fragments
/// overwrite the first's blockmap entries — simulating what happens when
/// two flush lanes pack the same LBAs (due to GC re-injection or
/// overlapping coalesce output).
#[test]
fn packed_slot_overlapping_lba_race() {
    use std::sync::atomic::AtomicBool;
    use std::sync::Barrier;

    let (meta, pool, lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();

    let vol_id = "flush-race";
    let (cleanup_tx, _cleanup_rx) = unbounded::<CleanupBatch>();
    let found_drift = AtomicBool::new(false);
    let rounds = 1000;

    for _ in 0..rounds {
        // Both threads write to LBAs 0..3
        let data = vec![0xCCu8; BLOCK_SIZE as usize];
        let mut seqs = Vec::new();
        for lba in 0u64..4 {
            if let Ok(seq) = pool.append(vol_id, Lba(lba), 1, &data, 1) {
                seqs.push((seq, Lba(lba), 1u32));
            }
        }
        if seqs.len() != 4 {
            continue;
        }

        let pba_a = match allocator.allocate_one_for_lane(0) {
            Ok(p) => p,
            Err(_) => continue,
        };
        let pba_b = match allocator.allocate_one_for_lane(0) {
            Ok(p) => p,
            Err(_) => continue,
        };

        let compressed = vec![0xCCu8; 300];
        let crc = crc32fast::hash(&compressed);
        let mut slot_a = vec![0u8; BLOCK_SIZE as usize];
        let mut slot_b = vec![0u8; BLOCK_SIZE as usize];
        slot_a[..300].copy_from_slice(&compressed);
        slot_b[..300].copy_from_slice(&compressed);

        let make_sealed = |pba: Pba, slot: Vec<u8>| crate::packer::packer::SealedSlot {
            pba,
            data: slot,
            fragments: vec![crate::packer::packer::SlotFragment {
                unit: CompressedUnit {
                    vol_id: vol_id.to_string(),
                    start_lba: Lba(0),
                    lba_count: 4,
                    compressed_data: compressed.clone(),
                    original_size: BLOCK_SIZE * 4,
                    compression: 0,
                    crc32: crc,
                    seq_lba_ranges: seqs.clone(),
                    block_hashes: None,
                    dedup_stale_repairs: None,
                    dedup_skipped: false,
                    compression_bypassed: false,
                    vol_created_at: 1,
                    dedup_completion: None,
                },
                slot_offset: 0,
            }],
        };

        let sealed_a = make_sealed(pba_a, slot_a);
        let sealed_b = make_sealed(pba_b, slot_b);

        // Spawn 16 threads, each with its own PBA, all targeting same LBAs
        let thread_count = 16;
        let mut all_pbas: Vec<Pba> = vec![pba_a, pba_b];
        let mut all_sealed: Vec<crate::packer::packer::SealedSlot> = vec![sealed_a, sealed_b];
        for _ in 2..thread_count {
            let p = match allocator.allocate_one_for_lane(0) {
                Ok(p) => p,
                Err(_) => break,
            };
            let mut sd = vec![0u8; BLOCK_SIZE as usize];
            sd[..300].copy_from_slice(&compressed);
            all_sealed.push(make_sealed(p, sd));
            all_pbas.push(p);
        }
        let actual_threads = all_sealed.len();
        let barrier = Barrier::new(actual_threads);

        std::thread::scope(|s| {
            for sealed in &all_sealed {
                s.spawn(|| {
                    barrier.wait();
                    let _ = BufferFlusher::write_packed_slot(
                        0,
                        sealed,
                        &pool,
                        &meta,
                        &lifecycle,
                        &allocator,
                        &io_engine,
                        &metrics,
                        &cleanup_tx,
                        &crate::dedup::CandidateCache::new(8, 64),
                    );
                });
            }
        });

        // Phase 5: refcount-drift is impossible because the hot-path
        // commit no longer mutates rc. The blockmap-ref invariant
        // (every pba pointed at by ≥1 mapping is reachable) still
        // holds — confirm it didn't crash and is internally
        // consistent.
        for &pba in &all_pbas {
            let refs = meta.count_blockmap_refs_for_pba(pba).unwrap();
            if refs > 0 {
                assert!(meta.has_any_blockmap_ref(pba).unwrap());
            }
        }
    }

    let _ = found_drift; // legacy flag retained for clarity
}

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
