use super::*;
use crate::config::MetaConfig;
use crate::io::device::RawDevice;
use crate::meta::store::MetaStore;
use crate::types::{VolumeConfig, ZoneId};
use crate::zone::worker::ZoneWorker;
use std::collections::{HashMap, HashSet};
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
            wal_dir: None,
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
        size_bytes: 4096 * 1024,
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
        dedup_skipped: false,
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
        dedup_skipped: false,
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
        dedup_skipped: false,
        dedup_completion: None,
    }
}

#[test]
fn old_write_unit_can_overwrite_newer_committed_mapping() {
    let (meta, pool, lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let (cleanup_tx, _cleanup_rx) = unbounded::<Vec<(Pba, u32)>>();

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
fn packed_slot_flush_survives_already_freed_old_pba_cleanup() {
    let (meta, pool, lifecycle, allocator, io_engine, metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let (cleanup_tx, _cleanup_rx) = unbounded::<Vec<(Pba, u32)>>();

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
    )
    .unwrap();

    let mapping = meta
        .get_mapping(&VolumeId("flush-race".into()), Lba(0))
        .unwrap()
        .unwrap();
    assert_eq!(mapping.pba, new_pba);
    assert_eq!(meta.get_refcount(new_pba).unwrap(), 1);
    assert_eq!(meta.get_refcount(old_pba).unwrap(), 0);
    assert!(
        pool.pending_entry_arc(seq).is_none(),
        "post-commit cleanup drift must not leave the seq stuck in the buffer"
    );
}

#[test]
fn dedup_hit_cleanup_deduplicates_repeated_old_pbas() {
    let (meta, _pool, _lifecycle, _allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
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

    // Each LBA needs a dedup_reverse entry for the guard to accept the hit.
    let hash_0: ContentHash = [0x01; 32];
    let hash_1: ContentHash = [0x02; 32];
    let hash_2: ContentHash = [0x03; 32];
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
    let (meta, _pool, _lifecycle, allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();

    let pba = allocator.allocate_one_for_lane(0).unwrap();
    let hash: ContentHash = [0xAB; 32];
    meta.put_dedup_entries(&[(
        hash,
        DedupEntry {
            pba,
            slot_offset: 0,
            compression: 0,
            unit_compressed_size: BLOCK_SIZE,
            unit_original_size: BLOCK_SIZE,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 0xDEAD_BEEF,
        },
    )])
    .unwrap();
    assert_eq!(meta.get_refcount(pba).unwrap(), 0);

    let meta_scanner = meta.clone();
    let allocator_scanner = allocator.clone();
    let (ready_tx, ready_rx) = bounded::<()>(1);
    let (resume_tx, resume_rx) = bounded::<()>(1);

    let before = CLEANUP_FREE_ATTEMPTS.load(Ordering::SeqCst);

    let scanner = thread::spawn(move || {
        ready_tx.send(()).unwrap();
        resume_rx.recv().unwrap();
        BufferFlusher::cleanup_dead_pba_post_commit(
            &meta_scanner,
            &allocator_scanner,
            pba,
            1,
            "dedup_scanner_cleanup",
        );
    });

    ready_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("scanner-style cleanup should reach allocator handoff");

    BufferFlusher::cleanup_dead_pba_post_commit(
        &meta,
        &allocator,
        pba,
        1,
        "dedup_worker_hit_cleanup",
    );

    resume_tx.send(()).unwrap();
    scanner.join().unwrap();
    // The counter is global so other parallel tests can bump it.
    // Just verify the dedup entry was cleaned and the PBA was freed.
    assert!(allocator.is_free(pba), "PBA should be freed after cleanup");
    assert!(meta.get_dedup_entry(&hash).unwrap().is_none());
}

#[test]
fn duplicate_dead_pba_cleanup_callers_without_shared_lock_double_free() {
    let (meta, _pool, _lifecycle, allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
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
    let already_free = results
        .iter()
        .filter_map(|r| r.as_ref().err())
        .filter(|e| e.to_string().contains("already free"))
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
    let (cleanup_tx, _cleanup_rx) = unbounded::<Vec<(Pba, u32)>>();

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
    assert_eq!(out, EnqueuePendingSeq::Skipped);
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
    let (meta, _pool, _lifecycle, allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();

    let vol = VolumeId("flush-race".into());
    let packed_pba = allocator.allocate_one_for_lane(0).unwrap();

    // --- Round 1: create a packed slot with 2 fragments (32 + 32 = 64 LBAs) ---
    let mut batch_values: Vec<(VolumeId, Lba, BlockmapValue)> = Vec::new();
    // Fragment A: 32 LBAs at slot_offset=0
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
    // Fragment B: 32 LBAs at slot_offset=1953
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

    assert_eq!(meta.get_refcount(packed_pba).unwrap(), 64);
    assert_eq!(meta.count_blockmap_refs_for_pba(packed_pba).unwrap(), 64);

    // --- Overwrite ALL 64 LBAs via atomic_batch_write_multi (simulating normal writes) ---
    let new_pba_1 = allocator.allocate_one_for_lane(0).unwrap();
    let new_pba_2 = allocator.allocate_one_for_lane(0).unwrap();

    // Unit 1: overwrites LBAs 1000..1032 → new_pba_1
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

    // Unit 2: overwrites LBAs 2000..2032 → new_pba_2
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

    let old_pba_meta = meta.atomic_batch_write_multi(&batch_args).unwrap();

    // --- Verify: packed_pba's refcount should be 0, and no blockmap refs should remain ---
    let rc_after = meta.get_refcount(packed_pba).unwrap();
    let refs_after = meta.count_blockmap_refs_for_pba(packed_pba).unwrap();

    println!(
        "packed_pba after overwrite: refcount={}, blockmap_refs={}",
        rc_after, refs_after
    );

    // Both should be 0: all 64 LBAs were remapped to new PBAs
    assert_eq!(
        rc_after, 0,
        "packed_pba refcount should be 0 after all LBAs overwritten"
    );
    assert_eq!(
        refs_after, 0,
        "packed_pba should have 0 blockmap refs after all LBAs overwritten"
    );

    // Verify old PBAs were decremented
    assert!(
        old_pba_meta.contains_key(&packed_pba),
        "packed_pba should appear in old_pba_meta decrements"
    );

    // Verify new PBAs have correct refcounts
    assert_eq!(meta.get_refcount(new_pba_1).unwrap(), 32);
    assert_eq!(meta.get_refcount(new_pba_2).unwrap(), 32);
}

/// Regression test: packed slot + dedup hits + overwrite interaction.
///
/// Scenario: packed PBA gets dedup hits (increasing refcount), then the
/// ORIGINAL LBAs are overwritten. The dedup-added LBAs should keep the
/// PBA alive.
#[test]
fn packed_slot_refcount_with_dedup_and_overwrite() {
    let (meta, _pool, _lifecycle, allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();

    let vol = VolumeId("flush-race".into());
    let packed_pba = allocator.allocate_one_for_lane(0).unwrap();

    // --- Step 1: Create packed slot with 32 LBAs ---
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
    assert_eq!(meta.get_refcount(packed_pba).unwrap(), 32);

    // --- Step 2: Dedup hits map 16 additional LBAs to the same packed PBA ---
    // Register dedup_reverse entries so the guard passes
    let dedup_hashes: Vec<ContentHash> = (0u8..16).map(|i| [i + 100; 32]).collect();
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
                Lba(5000 + i), // different LBAs
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
        meta.get_refcount(packed_pba).unwrap(),
        48,
        "refcount should be 32 + 16 = 48"
    );

    // --- Step 3: Overwrite the ORIGINAL 32 LBAs via atomic_batch_write_multi ---
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

    // --- Verify: packed_pba should still have refcount 16 (dedup LBAs remain) ---
    let rc = meta.get_refcount(packed_pba).unwrap();
    let refs = meta.count_blockmap_refs_for_pba(packed_pba).unwrap();

    println!(
        "After dedup + overwrite: refcount={}, blockmap_refs={}",
        rc, refs
    );
    assert_eq!(
        refs, 16,
        "16 dedup-mapped LBAs should still reference packed_pba"
    );
    assert_eq!(
        rc, 16,
        "refcount should be 16 (original 32 overwritten, 16 dedup remain)"
    );
    assert_eq!(rc, refs, "refcount must match blockmap refs");
}

/// Concurrent stress test: multiple threads hammer packed slot creation,
/// overwrite, and dedup hits on shared PBAs. Checks for refcount drift.
#[test]
fn packed_slot_concurrent_refcount_drift() {
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::sync::Barrier;

    let (meta, _pool, _lifecycle, allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
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

    let (meta, _pool, _lifecycle, allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
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
                let mut h = [0u8; 32];
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
                        let mut h = [0u8; 32];
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

    // Final check: scan shared PBAs for drift
    let mut drift_count = 0u32;
    for &pba in &shared_pbas {
        let stored = meta.get_refcount(pba).unwrap();
        let actual = meta.count_blockmap_refs_for_pba(pba).unwrap();
        if stored != actual {
            eprintln!(
                "DRIFT PBA {}: stored={} actual={} diff={}",
                pba.0,
                stored,
                actual,
                actual as i64 - stored as i64
            );
            drift_count += 1;
            found_drift.store(true, Ordering::Relaxed);
        }
    }

    if drift_count > 0 {
        eprintln!("Total PBAs with drift: {}", drift_count);
    }
    assert!(
        !found_drift.load(Ordering::Relaxed),
        "refcount drift detected under concurrent packed + dedup + overwrite"
    );
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
    let (cleanup_tx, _cleanup_rx) = unbounded::<Vec<(Pba, u32)>>();
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
                            dedup_skipped: false,
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

    // Final drift check: scan all allocated PBAs
    let mut any_drift = false;
    let mut checked = 0u32;
    for pba_val in 0..20000u64 {
        let pba = Pba(pba_val + crate::types::RESERVED_BLOCKS);
        let rc = meta.get_refcount(pba).unwrap();
        let refs = meta.count_blockmap_refs_for_pba(pba).unwrap();
        if rc == 0 && refs == 0 {
            continue;
        }
        checked += 1;
        if rc != refs {
            eprintln!(
                "DRIFT PBA {}: stored={} actual={} diff={}",
                pba.0,
                rc,
                refs,
                refs as i64 - rc as i64
            );
            any_drift = true;
        }
    }
    eprintln!("Checked {} PBAs with non-zero state", checked);
    assert!(
        !any_drift,
        "refcount drift in full pipeline concurrent test"
    );
}

/// Proof-of-concept: atomic_batch_write_packed uses PUT to set refcount.
/// If the PBA already has additional references (from dedup or a previous
/// incarnation), PUT overwrites the total, causing drift.
#[test]
fn packed_slot_put_overwrites_dedup_refcount() {
    let (meta, _pool, _lifecycle, _allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();

    let vol = VolumeId("flush-race".into());
    let pba = Pba(999);

    // Step 1: Simulate dedup hits that already incremented this PBA's refcount.
    // In production this happens when dedup maps LBAs to an existing packed PBA.
    meta.set_refcount(pba, 4).unwrap(); // 4 dedup refs already exist
                                        // Create the 4 blockmap entries that these dedup refs represent
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
    assert_eq!(meta.get_refcount(pba).unwrap(), 4);
    assert_eq!(meta.count_blockmap_refs_for_pba(pba).unwrap(), 4);

    // Step 2: write_packed_slot calls atomic_batch_write_packed with 8 NEW LBAs.
    // This simulates a sealed slot being written to an already-referenced PBA.
    let batch_values: Vec<(VolumeId, Lba, BlockmapValue)> = (0u64..8)
        .map(|i| {
            (
                vol.clone(),
                Lba(200 + i), // DIFFERENT LBAs from the dedup ones
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

    // Step 3: Check for drift
    let rc = meta.get_refcount(pba).unwrap();
    let refs = meta.count_blockmap_refs_for_pba(pba).unwrap();

    eprintln!(
        "After packed write over dedup PBA: refcount={}, blockmap_refs={}",
        rc, refs
    );

    // BUG: refcount=8 (PUT overwrote the 4 dedup refs) but blockmap_refs=12 (4 dedup + 8 new)
    // EXPECTED (correct): refcount=12, blockmap_refs=12
    assert_eq!(
        refs, 12,
        "should have 4 dedup + 8 packed = 12 blockmap refs"
    );
    assert_eq!(
        rc, refs,
        "refcount must match blockmap refs — PUT overwrites dedup increment!"
    );
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
    let (meta, _pool, _lifecycle, allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let vol = VolumeId("flush-race".into());

    let packed_pba = Pba(999);
    meta.set_refcount(packed_pba, 0).unwrap();

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
    assert_eq!(meta.get_refcount(packed_pba).unwrap(), 8);

    // Step 2: dedup hits add 4 more refs
    let dedup_hashes: Vec<ContentHash> = (0u8..4).map(|i| [i + 50; 32]).collect();
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
    assert_eq!(meta.get_refcount(packed_pba).unwrap(), 12);

    // Step 3: overwrite ALL 8 original LBAs → decrement packed_pba by 8
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

    // Step 4: verify packed_pba is NOT prematurely freed
    let rc = meta.get_refcount(packed_pba).unwrap();
    let refs = meta.count_blockmap_refs_for_pba(packed_pba).unwrap();

    eprintln!("After full chain: refcount={}, blockmap_refs={}", rc, refs);

    assert_eq!(refs, 4, "4 dedup LBAs should still reference packed_pba");
    assert_eq!(
        rc, 4,
        "refcount should be 12 - 8 = 4, NOT 0 (premature free)"
    );

    // Step 5: verify cleanup would NOT free this PBA
    let live_rc = meta.get_refcount(packed_pba).unwrap();
    assert_eq!(
        live_rc, 4,
        "cleanup must see refcount > 0 and NOT free the PBA"
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
    let (cleanup_tx, _cleanup_rx) = unbounded::<Vec<(Pba, u32)>>();
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
                    dedup_skipped: false,
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
                    );
                });
            }
        });

        // One thread wins, the rest should correctly decrement losers' PBAs
        for &pba in &all_pbas {
            let rc = meta.get_refcount(pba).unwrap();
            let refs = meta.count_blockmap_refs_for_pba(pba).unwrap();
            if rc != refs {
                eprintln!(
                    "DRIFT PBA {}: stored={} actual={} diff={}",
                    pba.0,
                    rc,
                    refs,
                    refs as i64 - rc as i64
                );
                found_drift.store(true, Ordering::Relaxed);
            }
        }
    }

    assert!(
        !found_drift.load(Ordering::Relaxed),
        "refcount drift when two write_packed_slot calls race on same LBAs"
    );
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
    let (meta, _pool, _lifecycle, allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
        setup_flush_test_env();
    let vol = VolumeId("flush-race".into());
    let mut ghost_found = false;

    // Use a large device to avoid running out of PBAs.  Replacement PBAs
    // are left allocated (their blockmap entries remain live), so each
    // cycle consumes 2 PBAs: the recycled one + its replacement.
    for cycle in 0..300u64 {
        // Step 1: Allocate a PBA, create 8 blockmap entries pointing to it
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

        assert_eq!(meta.get_refcount(pba).unwrap(), 8);
        assert_eq!(meta.count_blockmap_refs_for_pba(pba).unwrap(), 8);

        // Step 2: Overwrite all 8 LBAs with a different PBA
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
        let newly_zeroed = meta.atomic_batch_write_multi(&args).unwrap();

        // Step 3: The overwrite should drive pba to refcount=0
        assert_eq!(
            meta.get_refcount(pba).unwrap(),
            0,
            "cycle {cycle}: refcount not zeroed"
        );
        assert_eq!(
            meta.count_blockmap_refs_for_pba(pba).unwrap(),
            0,
            "cycle {cycle}: blockmap refs not zeroed"
        );
        assert!(
            newly_zeroed.contains_key(&pba),
            "cycle {cycle}: pba should be in newly_zeroed"
        );

        // Step 4: Cleanup and free
        BufferFlusher::cleanup_dead_pba_post_commit(&meta, &allocator, pba, 1, "recycle_test");

        // Step 5: Reallocate — might get the same PBA back
        let new_pba = match allocator.allocate_one_for_lane(0) {
            Ok(p) => p,
            Err(_) => break,
        };

        // Step 6: Verify the new PBA has zero refcount and zero blockmap refs
        let rc = meta.get_refcount(new_pba).unwrap();
        let refs = meta.count_blockmap_refs_for_pba(new_pba).unwrap();
        if rc != 0 || refs != 0 {
            eprintln!(
                "GHOST REFS on PBA {} (cycle {}): refcount={} blockmap_refs={}",
                new_pba.0, cycle, rc, refs
            );
            ghost_found = true;
        }

        // Don't free replacement_pba — its blockmap entries are still live.
        // Free new_pba for reuse in future cycles.
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

    let (meta, _pool, _lifecycle, allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
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
                let mut h = [0u8; 32];
                h[0] = idx as u8;
                h[1] = i;
                h[31] = 0xFE; // marker
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
                        let mut h = [0u8; 32];
                        h[0] = pba_idx as u8;
                        h[1] = i;
                        h[31] = 0xFE;
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
                    for (dead_pba, (_, blocks)) in &newly_zeroed {
                        BufferFlusher::cleanup_dead_pba_post_commit(
                            meta,
                            allocator,
                            *dead_pba,
                            *blocks,
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
                    for (dead_pba, (_, blocks)) in &newly_zeroed {
                        BufferFlusher::cleanup_dead_pba_post_commit(
                            meta,
                            allocator,
                            *dead_pba,
                            *blocks,
                            "concurrent_test_overwrite_c",
                        );
                    }
                }
            }
        });
    });

    // Final integrity check: every PBA should have refcount == blockmap_refs
    let mut drift_count = 0u32;
    for &pba in &shared_pbas {
        let rc = meta.get_refcount(pba).unwrap();
        let refs = meta.count_blockmap_refs_for_pba(pba).unwrap();
        if rc != refs {
            eprintln!(
                "DRIFT PBA {}: stored={} actual={} diff={}",
                pba.0,
                rc,
                refs,
                refs as i64 - rc as i64
            );
            drift_count += 1;
            found_drift.store(true, Ordering::Relaxed);
        }
    }
    // Also check all the replacement PBAs
    for pba_val in 0..20000u64 {
        let pba = Pba(pba_val + crate::types::RESERVED_BLOCKS);
        let rc = meta.get_refcount(pba).unwrap();
        let refs = meta.count_blockmap_refs_for_pba(pba).unwrap();
        if rc == 0 && refs == 0 {
            continue;
        }
        if rc != refs {
            eprintln!(
                "DRIFT (replacement) PBA {}: stored={} actual={}",
                pba.0, rc, refs
            );
            drift_count += 1;
            found_drift.store(true, Ordering::Relaxed);
        }
    }
    if drift_count > 0 {
        eprintln!("Total PBAs with drift: {drift_count}");
    }
    assert!(
        !found_drift.load(Ordering::Relaxed),
        "refcount drift under concurrent overwrite + dedup + cleanup"
    );
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

    let (meta, _pool, _lifecycle, allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
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
                        for (dead_pba, (_, blocks)) in &newly_zeroed {
                            BufferFlusher::cleanup_dead_pba_post_commit(
                                meta,
                                allocator,
                                *dead_pba,
                                *blocks,
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
    assert_eq!(meta.get_refcount(pba_a).unwrap(), 8);

    // Step 2: Duplicate write — same LBAs 0-7 but different PBA B.
    // This simulates what happens if the buffer ring delivers the same
    // data twice and the flusher processes both copies.
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
    let newly_zeroed = meta.atomic_batch_write_multi(&args).unwrap();

    // PBA A should now have refcount=0 (all 8 entries overwritten)
    assert_eq!(meta.get_refcount(pba_a).unwrap(), 0);
    assert_eq!(meta.count_blockmap_refs_for_pba(pba_a).unwrap(), 0);
    assert!(
        newly_zeroed.contains_key(&pba_a),
        "PBA A should be newly_zeroed"
    );

    // Step 3: Cleanup frees PBA A
    BufferFlusher::cleanup_dead_pba_post_commit(&meta, &allocator, pba_a, 1, "dup_flush_test");
    assert!(allocator.is_free(pba_a), "PBA A should be free now");

    // Step 4: Reallocate — should get PBA A back (FIFO-ish)
    let recycled = allocator.allocate_one().unwrap();
    // Verify it's clean
    let rc = meta.get_refcount(recycled).unwrap();
    let refs = meta.count_blockmap_refs_for_pba(recycled).unwrap();
    assert_eq!(rc, 0, "recycled PBA should have refcount=0");
    assert_eq!(refs, 0, "recycled PBA should have 0 blockmap refs");

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
    assert_eq!(meta.get_refcount(recycled).unwrap(), 8);
    assert_eq!(meta.count_blockmap_refs_for_pba(recycled).unwrap(), 8);

    // This test passes: when the duplicate processing follows the normal
    // overwrite path, metadata stays consistent. The PBA is freed and
    // recycled cleanly. The PROBLEM is that this causes PBA A's physical
    // data to be lost — any IO that cached PBA A's physical location would
    // now read wrong data. The CRC in the new blockmap entry (0xCCCCCCCC)
    // doesn't match PBA A's original data (0xAAAAAAAA).
    eprintln!(
        "Demonstrated: duplicate flush entry causes PBA A ({}) to be freed \
         and recycled as PBA {} with different data. If PBA A == recycled, \
         the original 0xAAAAAAAA data is overwritten by 0xCCCCCCCC.",
        pba_a.0, recycled.0
    );
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

    let (meta, _pool, _lifecycle, allocator, _io_engine, _metrics, _meta_dir, _buf_tmp, _data_tmp) =
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
                let mut h = [0u8; 32];
                h[0] = idx as u8;
                h[1] = i;
                h[31] = 0xAA;
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
                        for (dead_pba, (_, blocks)) in &newly_zeroed {
                            BufferFlusher::cleanup_dead_pba_post_commit(
                                meta,
                                allocator,
                                *dead_pba,
                                *blocks,
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
                        let mut h = [0u8; 32];
                        h[0] = pba_idx as u8;
                        h[1] = i;
                        h[31] = 0xAA;
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

    // FULL SCAN: every PBA must have refcount == blockmap_refs
    let mut drift_count = 0u32;
    for pba_val in 0..20000u64 {
        let pba = Pba(pba_val + crate::types::RESERVED_BLOCKS);
        let rc = meta.get_refcount(pba).unwrap();
        let refs = meta.count_blockmap_refs_for_pba(pba).unwrap();
        if rc == 0 && refs == 0 {
            continue;
        }
        if rc != refs {
            eprintln!(
                "DRIFT PBA {}: stored={} actual={} diff={}",
                pba.0,
                rc,
                refs,
                refs as i64 - rc as i64
            );
            drift_count += 1;
            found_drift.store(true, Ordering::Relaxed);
        }
    }
    if drift_count > 0 {
        eprintln!("Total drifted PBAs: {drift_count}");
    }
    assert!(
        !found_drift.load(Ordering::Relaxed),
        "refcount drift found in full-pressure scan"
    );
}
