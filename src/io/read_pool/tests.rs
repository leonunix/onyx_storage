use super::*;
use crate::io::engine::IoEngine;
use crate::types::{Pba, BLOCK_SIZE};
use std::sync::Arc;
use std::sync::Barrier;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;

fn fresh_device() -> (RawDevice, NamedTempFile) {
    let tmp = NamedTempFile::new().unwrap();
    tmp.as_file().set_len(4 * 1024 * 1024).unwrap();
    let dev = RawDevice::open_or_create(tmp.path(), 4 * 1024 * 1024).unwrap();
    (dev, tmp)
}

fn write_uncompressed(engine: &IoEngine, pba: Pba, payload: &[u8]) {
    engine.write_blocks(pba, payload).unwrap();
}

fn make_mapping(pba: Pba, payload_len: u32, crc: u32) -> BlockmapValue {
    BlockmapValue {
        pba,
        compression: 0,
        unit_compressed_size: payload_len,
        unit_original_size: payload_len,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: crc,
        slot_offset: 0,
        flags: 0,
    }
}

fn queued_request(purpose: ReadPurpose, pba: Pba) -> ReadRequest {
    let (reply, _rx) = bounded(1);
    ReadRequest {
        mapping: make_mapping(pba, BLOCK_SIZE, 0),
        reply,
        enqueued_at: Instant::now(),
        purpose,
        return_unit: false,
        raw_extent: None,
    }
}

#[test]
fn reserves_half_of_multi_worker_pool_for_foreground() {
    assert_eq!(reserved_foreground_workers(1), 0);
    assert_eq!(reserved_foreground_workers(2), 1);
    assert_eq!(reserved_foreground_workers(3), 2);
    assert_eq!(reserved_foreground_workers(4), 2);
    assert_eq!(reserved_foreground_workers(32), 16);
}

#[test]
fn shared_worker_selects_foreground_at_batch_boundary() {
    let (foreground_tx, foreground_rx) = bounded(2);
    let (background_tx, background_rx) = bounded(2);
    background_tx
        .send(queued_request(ReadPurpose::DedupScanner, Pba(1)))
        .unwrap();
    foreground_tx
        .send(queued_request(ReadPurpose::Foreground, Pba(2)))
        .unwrap();

    let first = receive_next(&foreground_rx, Some(&background_rx)).unwrap();
    assert_eq!(first.purpose, ReadPurpose::Foreground);
    let second = receive_next(&foreground_rx, Some(&background_rx)).unwrap();
    assert_eq!(second.purpose, ReadPurpose::DedupScanner);
}

#[test]
fn full_background_queue_cannot_block_reserved_foreground_worker() {
    let (foreground_tx, foreground_rx) = bounded(1);
    let (background_tx, background_rx) = bounded(1);
    let worker = std::thread::spawn(move || {
        let request = receive_next(&foreground_rx, None).unwrap();
        assert_eq!(request.purpose, ReadPurpose::Foreground);
        request.reply.send(Ok(vec![0xA5])).unwrap();
    });
    let mut pool = ReadPool {
        senders: Some(RequestSenders {
            foreground: foreground_tx,
            background: background_tx,
        }),
        workers: vec![WorkerHandle { join: Some(worker) }],
    };

    let _background_reply = pool
        .submit_read_async_for(
            make_mapping(Pba(1), BLOCK_SIZE, 0),
            ReadPurpose::DedupScanner,
        )
        .unwrap();
    assert_eq!(background_rx.len(), 1);
    let foreground_reply = pool
        .submit_read_async(make_mapping(Pba(2), BLOCK_SIZE, 0))
        .unwrap();

    assert_eq!(
        foreground_reply
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap(),
        vec![0xA5]
    );
    assert_eq!(background_rx.len(), 1);
    pool.shutdown();
}

#[test]
fn shutdown_drains_both_priority_queues_without_hanging() {
    let (dev, tmp) = fresh_device();
    let engine = IoEngine::new_raw(dev, false);
    let payload = vec![0x5Au8; BLOCK_SIZE as usize];
    write_uncompressed(&engine, Pba(0), &payload);
    let mapping = make_mapping(Pba(0), BLOCK_SIZE, crc32fast::hash(&payload));

    let pool_dev = RawDevice::open_or_create(tmp.path(), 4 * 1024 * 1024).unwrap();
    let metrics = Arc::new(EngineMetrics::default());
    let mut pool = ReadPool::start(2, 16, &pool_dev, 0, BLOCK_SIZE, false, metrics).unwrap();

    let mut replies = Vec::new();
    for _ in 0..8 {
        replies.push(pool.submit_read_async(mapping).unwrap());
        replies.push(
            pool.submit_read_async_for(mapping, ReadPurpose::DedupScanner)
                .unwrap(),
        );
    }
    let (done_tx, done_rx) = bounded(1);
    let shutdown = std::thread::spawn(move || {
        pool.shutdown();
        done_tx.send(()).unwrap();
    });
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("ReadPool shutdown hung with both queues populated");
    shutdown.join().unwrap();

    for reply in replies {
        assert_eq!(
            reply.recv_timeout(Duration::from_secs(1)).unwrap().unwrap(),
            payload
        );
    }
}

#[test]
fn read_pool_round_trip_uncompressed() {
    let (dev, tmp) = fresh_device();
    let engine = IoEngine::new_raw(dev, false);

    let payload = vec![0xC3u8; BLOCK_SIZE as usize];
    write_uncompressed(&engine, Pba(0), &payload);
    let crc = crc32fast::hash(&payload);

    let pool_dev = RawDevice::open_or_create(tmp.path(), 4 * 1024 * 1024).unwrap();
    let metrics = Arc::new(EngineMetrics::default());
    let pool = ReadPool::start(2, 16, &pool_dev, 0, BLOCK_SIZE, false, metrics).unwrap();

    let got = pool
        .submit_read(make_mapping(Pba(0), BLOCK_SIZE, crc))
        .unwrap();
    assert_eq!(got, payload);
}

#[test]
fn read_pool_concurrent_reads_match_writes() {
    let (dev, tmp) = fresh_device();
    let engine = IoEngine::new_raw(dev, false);

    let payloads: Vec<Vec<u8>> = (0..32)
        .map(|i| vec![(i & 0xFF) as u8; BLOCK_SIZE as usize])
        .collect();
    for (i, p) in payloads.iter().enumerate() {
        write_uncompressed(&engine, Pba(i as u64), p);
    }
    let crcs: Vec<u32> = payloads.iter().map(|p| crc32fast::hash(p)).collect();

    let pool_dev = RawDevice::open_or_create(tmp.path(), 4 * 1024 * 1024).unwrap();
    let metrics = Arc::new(EngineMetrics::default());
    let pool = Arc::new(ReadPool::start(4, 64, &pool_dev, 0, BLOCK_SIZE, false, metrics).unwrap());

    let barrier = Arc::new(Barrier::new(8));
    let mut handles = Vec::new();
    for t in 0..8 {
        let pool = pool.clone();
        let payloads = payloads.clone();
        let crcs = crcs.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            for round in 0..5 {
                for i in 0..32usize {
                    let mapping = make_mapping(Pba(i as u64), BLOCK_SIZE, crcs[i]);
                    let got = pool.submit_read(mapping).unwrap();
                    assert_eq!(
                        got, payloads[i],
                        "thread {} round {} pba {} mismatch",
                        t, round, i
                    );
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

/// `submit_unit_read` must return the full decoded unit payload so that
/// callers can fan out multiple LBAs from one io_uring read + decompress.
#[test]
fn submit_unit_read_returns_full_payload() {
    let (dev, tmp) = fresh_device();
    let engine = IoEngine::new_raw(dev, false);

    // A 4-LBA uncompressed "unit" — all four 4 KB slots laid out
    // contiguously, each filled with a distinct byte pattern.
    let unit_bytes = 4 * BLOCK_SIZE as usize;
    let mut unit = vec![0u8; unit_bytes];
    for (i, chunk) in unit.chunks_mut(BLOCK_SIZE as usize).enumerate() {
        chunk.fill((i as u8) + 0x30);
    }
    engine.write_blocks(Pba(0), &unit).unwrap();
    let crc = crc32fast::hash(&unit);

    let pool_dev = RawDevice::open_or_create(tmp.path(), 4 * 1024 * 1024).unwrap();
    let metrics = Arc::new(EngineMetrics::default());
    let pool = ReadPool::start(1, 16, &pool_dev, 0, BLOCK_SIZE, false, metrics.clone()).unwrap();

    let mut mapping = make_mapping(Pba(0), unit_bytes as u32, crc);
    mapping.unit_original_size = unit_bytes as u32;
    mapping.unit_lba_count = 4;

    let got = pool.submit_unit_read(mapping).unwrap();
    assert_eq!(got.len(), unit_bytes);
    for (i, chunk) in got.chunks(BLOCK_SIZE as usize).enumerate() {
        assert_eq!(chunk, &[(i as u8) + 0x30; BLOCK_SIZE as usize]);
    }
    // One io_uring op served the whole unit — the bedrock of coalescing.
    assert_eq!(metrics.lv3_read_ops.load(Ordering::Relaxed), 1);
    // `submit_unit_read` does NOT bump per-LBA hits; callers do that in
    // their fan-out loop.
    assert_eq!(metrics.read_lv3_hits.load(Ordering::Relaxed), 0);
}

#[test]
fn submit_raw_extent_read_async_reads_one_contiguous_span() {
    let (dev, tmp) = fresh_device();
    let engine = IoEngine::new_raw(dev, false);

    let mut mappings = Vec::new();
    let mut expected = Vec::new();
    for i in 0..4u64 {
        let payload = vec![0x40 + i as u8; BLOCK_SIZE as usize];
        write_uncompressed(&engine, Pba(i), &payload);
        expected.extend_from_slice(&payload);
        mappings.push(make_mapping(Pba(i), BLOCK_SIZE, crc32fast::hash(&payload)));
    }

    let pool_dev = RawDevice::open_or_create(tmp.path(), 4 * 1024 * 1024).unwrap();
    let metrics = Arc::new(EngineMetrics::default());
    let pool = ReadPool::start(1, 16, &pool_dev, 0, BLOCK_SIZE, false, metrics.clone()).unwrap();

    let rx = pool.submit_raw_extent_read_async(mappings).unwrap();
    let got = rx.recv().unwrap().unwrap();
    assert_eq!(got, expected);
    assert_eq!(metrics.lv3_read_ops.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.read_lv3_hits.load(Ordering::Relaxed), 4);
}

/// `submit_read_async` lets callers fire N requests before draining any
/// reply — the worker batches them into one `submit_batch` io_uring enter.
#[test]
fn submit_read_async_fans_out_before_draining() {
    let (dev, tmp) = fresh_device();
    let engine = IoEngine::new_raw(dev, false);

    let payloads: Vec<Vec<u8>> = (0..8)
        .map(|i| vec![(i & 0xFF) as u8; BLOCK_SIZE as usize])
        .collect();
    for (i, p) in payloads.iter().enumerate() {
        write_uncompressed(&engine, Pba(i as u64), p);
    }
    let crcs: Vec<u32> = payloads.iter().map(|p| crc32fast::hash(p)).collect();

    // Single worker forces all requests to share one channel — this is
    // the scenario where pre-draining vs serial round-trip matters.
    let pool_dev = RawDevice::open_or_create(tmp.path(), 4 * 1024 * 1024).unwrap();
    let metrics = Arc::new(EngineMetrics::default());
    let pool = ReadPool::start(1, 16, &pool_dev, 0, BLOCK_SIZE, false, metrics).unwrap();

    let mut rxs = Vec::new();
    for i in 0..8 {
        let rx = pool
            .submit_read_async(make_mapping(Pba(i as u64), BLOCK_SIZE, crcs[i]))
            .unwrap();
        rxs.push(rx);
    }
    for (i, rx) in rxs.into_iter().enumerate() {
        let got = rx.recv().unwrap().unwrap();
        assert_eq!(got, payloads[i]);
    }
}

#[test]
fn read_pool_crc_mismatch_returns_error() {
    let (dev, tmp) = fresh_device();
    let engine = IoEngine::new_raw(dev, false);

    let payload = vec![0x77u8; BLOCK_SIZE as usize];
    write_uncompressed(&engine, Pba(0), &payload);
    let bad_crc = crc32fast::hash(&payload).wrapping_add(1);

    let pool_dev = RawDevice::open_or_create(tmp.path(), 4 * 1024 * 1024).unwrap();
    let metrics = Arc::new(EngineMetrics::default());
    let pool = ReadPool::start(1, 8, &pool_dev, 0, BLOCK_SIZE, false, metrics.clone()).unwrap();

    let err = pool
        .submit_read(make_mapping(Pba(0), BLOCK_SIZE, bad_crc))
        .unwrap_err();
    match err {
        OnyxError::CrcMismatch { .. } => {}
        other => panic!("expected CrcMismatch, got {other:?}"),
    }
    assert_eq!(metrics.read_crc_errors.load(Ordering::Relaxed), 1);
}

#[test]
fn dedup_verify_mismatch_is_not_counted_as_crc_error() {
    let (dev, tmp) = fresh_device();
    let engine = IoEngine::new_raw(dev, false);

    let payload = vec![0x91u8; BLOCK_SIZE as usize];
    write_uncompressed(&engine, Pba(0), &payload);
    let bad_crc = crc32fast::hash(&payload).wrapping_add(1);

    let pool_dev = RawDevice::open_or_create(tmp.path(), 4 * 1024 * 1024).unwrap();
    let metrics = Arc::new(EngineMetrics::default());
    let pool = ReadPool::start(1, 8, &pool_dev, 0, BLOCK_SIZE, false, metrics.clone()).unwrap();

    let rx = pool
        .submit_read_async_for(
            make_mapping(Pba(0), BLOCK_SIZE, bad_crc),
            ReadPurpose::DedupVerifyIndex,
        )
        .unwrap();
    let err = rx.recv().unwrap().unwrap_err();
    assert!(matches!(err, OnyxError::CrcMismatch { .. }));
    assert_eq!(metrics.read_crc_errors.load(Ordering::Relaxed), 0);
    assert_eq!(
        metrics.read_crc_errors_foreground.load(Ordering::Relaxed),
        0
    );
    assert_eq!(metrics.dedup_verify_mismatches.load(Ordering::Relaxed), 1);
}

/// `start_backend` round-trips an uncompressed block through a real chunklet
/// RAID6 LV3 LD — the worker reads via `read_many_at` (no per-worker fd/uring),
/// then the shared decode path validates CRC and returns the LBA.
#[test]
fn read_pool_backend_round_trip_raid6() {
    use crate::chunklet_pool::{self, LdRoleSel};
    use crate::config::{ChunkletConfig, ChunkletIoBackend};
    use crate::io::block_backend::BlockBackend;

    let dir = tempfile::tempdir().unwrap();
    let devices: Vec<_> = (0..8).map(|i| dir.path().join(format!("pd{i}"))).collect();
    for p in &devices {
        onyx_chunklet::io::RawDevice::open_or_create(p, 4 << 30).unwrap();
    }
    let mut cfg = ChunkletConfig {
        enabled: true,
        devices,
        io_backend: ChunkletIoBackend::Sync,
        spare_pct: 0,
        ..Default::default()
    };
    let (pool, lv3, _lv2, _meta) = chunklet_pool::init_pool(&cfg).unwrap();
    drop(pool);
    cfg.lv3_ld_id = Some(lv3.to_string());
    let (_pool2, backend) = chunklet_pool::open_role_backend(&cfg, LdRoleSel::Lv3).unwrap();

    // Write an uncompressed 4 KiB block at PBA 0 (pba_offset = 0).
    let payload = vec![0xC3u8; BLOCK_SIZE as usize];
    backend.write_at(&payload, 0).unwrap();
    backend.flush().unwrap();
    let crc = crc32fast::hash(&payload);

    let metrics = Arc::new(EngineMetrics::default());
    let pool = ReadPool::start_backend(2, backend, 0, BLOCK_SIZE, false, metrics).unwrap();
    let got = pool
        .submit_read(make_mapping(Pba(0), BLOCK_SIZE, crc))
        .unwrap();
    assert_eq!(got, payload);
}
