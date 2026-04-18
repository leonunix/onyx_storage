use std::sync::Arc;

use onyx_storage::buffer::pool::WriteBufferPool;
use onyx_storage::config::MetaConfig;
use onyx_storage::io::device::RawDevice;
use onyx_storage::io::engine::IoEngine;
use onyx_storage::meta::store::MetaStore;
use onyx_storage::types::*;
use onyx_storage::zone::manager::ZoneManager;
use onyx_storage::zone::worker::ZoneWorker;
use tempfile::{tempdir, NamedTempFile};

// --- helpers ---

fn setup_worker() -> (ZoneWorker, Arc<MetaStore>) {
    let meta_dir = tempdir().unwrap();
    let meta_config = MetaConfig {
        rocksdb_path: Some(meta_dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 0,
        wal_dir: None,
    };
    let meta = Arc::new(MetaStore::open(&meta_config).unwrap());

    let buf_tmp = NamedTempFile::new().unwrap();
    let buf_size = 4096 + 100 * 8192;
    buf_tmp.as_file().set_len(buf_size).unwrap();
    let buf_dev = RawDevice::open_or_create(buf_tmp.path(), buf_size).unwrap();
    let buffer_pool = Arc::new(WriteBufferPool::open(buf_dev).unwrap());

    let data_tmp = NamedTempFile::new().unwrap();
    data_tmp.as_file().set_len(4096 * 1000).unwrap();
    let data_dev = RawDevice::open(data_tmp.path()).unwrap();
    let io_engine = Arc::new(IoEngine::new(data_dev, false));

    std::mem::forget(meta_dir);
    std::mem::forget(buf_tmp);
    std::mem::forget(data_tmp);

    let worker = ZoneWorker::new(ZoneId(0), meta.clone(), buffer_pool, io_engine);
    (worker, meta)
}

fn setup_zone_manager(zone_count: u32) -> ZoneManager {
    let meta_dir = tempdir().unwrap();
    let meta_config = MetaConfig {
        rocksdb_path: Some(meta_dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 0,
        wal_dir: None,
    };
    let meta = Arc::new(MetaStore::open(&meta_config).unwrap());

    let buf_tmp = NamedTempFile::new().unwrap();
    let buf_size = 4096 + 1000 * 8192;
    buf_tmp.as_file().set_len(buf_size).unwrap();
    let buf_dev = RawDevice::open_or_create(buf_tmp.path(), buf_size).unwrap();
    let buffer_pool = Arc::new(WriteBufferPool::open(buf_dev).unwrap());

    let data_tmp = NamedTempFile::new().unwrap();
    data_tmp.as_file().set_len(4096 * 10000).unwrap();
    let data_dev = RawDevice::open(data_tmp.path()).unwrap();
    let io_engine = Arc::new(IoEngine::new(data_dev, false));

    std::mem::forget(meta_dir);
    std::mem::forget(buf_tmp);
    std::mem::forget(data_tmp);

    ZoneManager::new(zone_count, 256, meta, buffer_pool, io_engine).unwrap()
}

// --- worker tests ---

#[test]
fn write_appends_to_buffer() {
    let (worker, _meta) = setup_worker();
    let data = vec![0u8; 4096];
    worker
        .handle_write("test-vol", Lba(0), 1, &data, 0)
        .unwrap();
    assert_eq!(worker.buffer_pool.pending_count(), 1);
}

// --- manager tests ---

#[test]
fn zone_routing() {
    let zm = setup_zone_manager(4);
    assert_eq!(zm.zone_for_lba(Lba(0)), ZoneId(0));
    assert_eq!(zm.zone_for_lba(Lba(255)), ZoneId(0));
    assert_eq!(zm.zone_for_lba(Lba(256)), ZoneId(1));
    assert_eq!(zm.zone_for_lba(Lba(512)), ZoneId(2));
    assert_eq!(zm.zone_for_lba(Lba(768)), ZoneId(3));
    assert_eq!(zm.zone_for_lba(Lba(1024)), ZoneId(0));
}

#[test]
fn concurrent_writes() {
    let mut zm = setup_zone_manager(4);

    let data = vec![0u8; 4096];
    for i in 0..20u64 {
        zm.submit_write("test-vol", Lba(i * 256), 1, &data, 0)
            .unwrap();
    }

    zm.shutdown().unwrap();
}

#[test]
fn write_to_different_zones() {
    let mut zm = setup_zone_manager(4);

    for zone in 0..4u64 {
        let lba = Lba(zone * 256);
        let data = vec![zone as u8; 4096];
        zm.submit_write("test-vol", lba, 1, &data, 0).unwrap();
    }

    zm.shutdown().unwrap();
}

// --- regression tests for bug fixes ---

/// Fix #1: read-after-write must return the data just written,
/// even before the buffer flusher has moved it to LV3.
#[test]
fn read_after_write_before_flush() {
    let (worker, _meta) = setup_worker();

    let data = vec![0xAB; 4096];
    worker
        .handle_write("test-vol", Lba(0), 1, &data, 0)
        .unwrap();

    // Read immediately -- should see the data from the buffer, not from LV3
    let read_data = worker.handle_read("test-vol", Lba(0)).unwrap().unwrap();
    assert_eq!(read_data, data);
}

/// Reading an unmapped block returns Ok(None), not zeros.
/// Zero-fill is the ublk frontend's job, not the engine's.
#[test]
fn read_unmapped_returns_none() {
    let (worker, _meta) = setup_worker();

    let result = worker.handle_read("test-vol", Lba(999)).unwrap();
    assert!(result.is_none());
}

/// Overwrite same LBA -- read must return the latest write.
#[test]
fn read_after_overwrite() {
    let (worker, _meta) = setup_worker();

    let data1 = vec![0x11; 4096];
    let data2 = vec![0x22; 4096];
    worker
        .handle_write("test-vol", Lba(5), 1, &data1, 0)
        .unwrap();
    worker
        .handle_write("test-vol", Lba(5), 1, &data2, 0)
        .unwrap();

    let read_data = worker.handle_read("test-vol", Lba(5)).unwrap().unwrap();
    assert_eq!(read_data, data2);
}

/// Worker read with ZSTD compression roundtrips correctly.
#[test]
fn read_after_write_zstd() {
    let (worker, _meta) = setup_worker();

    let data = vec![0x42; 4096]; // compressible
    worker
        .handle_write("test-vol", Lba(10), 1, &data, 0)
        .unwrap();

    let read_data = worker.handle_read("test-vol", Lba(10)).unwrap().unwrap();
    assert_eq!(read_data, data);
}

/// Worker read with no compression roundtrips correctly.
#[test]
fn read_after_write_no_compression() {
    let (worker, _meta) = setup_worker();

    let data: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();
    worker
        .handle_write("test-vol", Lba(20), 1, &data, 0)
        .unwrap();

    let read_data = worker.handle_read("test-vol", Lba(20)).unwrap().unwrap();
    assert_eq!(read_data, data);
}

/// Multiple LBAs written and read back correctly.
#[test]
fn multi_lba_write_read() {
    let (worker, _meta) = setup_worker();

    for i in 0..10u64 {
        let data = vec![i as u8; 4096];
        worker
            .handle_write("test-vol", Lba(i), 1, &data, 0)
            .unwrap();
    }

    for i in 0..10u64 {
        let read_data = worker.handle_read("test-vol", Lba(i)).unwrap().unwrap();
        assert_eq!(read_data, vec![i as u8; 4096]);
    }
}

/// ZoneManager read-after-write through dispatch.
#[test]
fn zone_manager_read_after_write() {
    let mut zm = setup_zone_manager(4);

    let data = vec![0xDE; 4096];
    zm.submit_write("test-vol", Lba(0), 1, &data, 0).unwrap();

    let read_data = zm.submit_read("test-vol", Lba(0)).unwrap().unwrap();
    assert_eq!(read_data, data);

    zm.shutdown().unwrap();
}

/// ZoneManager read of unmapped block returns None.
#[test]
fn zone_manager_read_unmapped() {
    let mut zm = setup_zone_manager(2);

    let result = zm.submit_read("test-vol", Lba(500)).unwrap();
    assert!(result.is_none());

    zm.shutdown().unwrap();
}

#[test]
fn zone_manager_concurrent_reads_inline_no_serialization() {
    use std::thread;
    use std::time::Duration;
    use onyx_storage::io::uring::IoUringSession;

    // Build a zone manager that uses the io_uring backend so the inline
    // read path also exercises the new code in this configuration.
    let meta_dir = tempdir().unwrap();
    let meta_config = MetaConfig {
        rocksdb_path: Some(meta_dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 0,
        wal_dir: None,
    };
    let meta = Arc::new(MetaStore::open(&meta_config).unwrap());

    let buf_tmp = NamedTempFile::new().unwrap();
    let buf_size = 4096 + 4096 + 4096 * 8192;
    buf_tmp.as_file().set_len(buf_size).unwrap();
    let buf_dev = RawDevice::open_or_create(buf_tmp.path(), buf_size).unwrap();
    let buffer_pool = Arc::new(WriteBufferPool::open(buf_dev).unwrap());

    let data_tmp = NamedTempFile::new().unwrap();
    data_tmp.as_file().set_len(4096 * 10000).unwrap();
    let data_dev = RawDevice::open(data_tmp.path()).unwrap();
    let session = Arc::new(IoUringSession::new(64).unwrap());
    let io_engine = Arc::new(IoEngine::new_uring(data_dev, false, session));

    std::mem::forget(meta_dir);
    std::mem::forget(buf_tmp);
    std::mem::forget(data_tmp);

    // Use zone_count=1 deliberately — under the old code every LBA mapped
    // to zone 0, so all reads were serialised through one worker. With the
    // inline read design, parallel readers must complete in parallel.
    let zm = Arc::new(ZoneManager::new(1, 256, meta, buffer_pool, io_engine).unwrap());

    let vol = "concurrent-readers";
    for i in 0..32u64 {
        let mut data = vec![0u8; 4096];
        data[0] = i as u8;
        zm.submit_write(vol, Lba(i), 1, &data, 0).unwrap();
    }

    // Spin up 8 reader threads, each reading the full LBA range twice.
    // Without zone-level serialisation, all reads should succeed without
    // any deadlock or starvation.
    let mut handles = Vec::new();
    for _ in 0..8 {
        let zm = zm.clone();
        let vol = vol.to_string();
        handles.push(thread::spawn(move || {
            for _ in 0..2 {
                for i in 0..32u64 {
                    let data = zm
                        .submit_read(&vol, Lba(i))
                        .expect("read must succeed under concurrent inline path")
                        .expect("each LBA must map");
                    assert_eq!(data[0], i as u8, "wrong payload for lba {}", i);
                }
            }
        }));
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    for h in handles {
        let remaining = deadline.checked_duration_since(std::time::Instant::now());
        assert!(
            remaining.is_some(),
            "reader thread deadlocked — inline read path should not stall"
        );
        h.join().unwrap();
    }
}

#[test]
fn zone_manager_read_pool_decompresses_concurrently() {
    use std::sync::Barrier;
    use std::thread;
    use std::time::Duration;
    use onyx_storage::buffer::flush::BufferFlusher;
    use onyx_storage::config::FlushConfig;
    use onyx_storage::io::read_pool::ReadPool;
    use onyx_storage::io::uring::IoUringSession;
    use onyx_storage::lifecycle::VolumeLifecycleManager;
    use onyx_storage::space::allocator::SpaceAllocator;

    // Stand up the full read path: WriteBufferPool → MetaStore → IoEngine
    // (uring backend) → ReadPool. Then write LZ4-compressible data through
    // the flusher and read it back concurrently from many threads to verify
    // batched io_uring + parallel decompression works end-to-end.
    let meta_dir = tempdir().unwrap();
    let meta_config = MetaConfig {
        rocksdb_path: Some(meta_dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 0,
        wal_dir: None,
    };
    let meta = Arc::new(MetaStore::open(&meta_config).unwrap());

    let buf_tmp = NamedTempFile::new().unwrap();
    let buf_size = 4096 + 4096 + 1024 * 8192;
    buf_tmp.as_file().set_len(buf_size).unwrap();
    let buf_dev = RawDevice::open_or_create(buf_tmp.path(), buf_size).unwrap();
    let buffer_pool = Arc::new(WriteBufferPool::open(buf_dev).unwrap());

    let data_tmp = NamedTempFile::new().unwrap();
    let data_size = 4096 * 10000;
    data_tmp.as_file().set_len(data_size as u64).unwrap();
    let data_dev = RawDevice::open(data_tmp.path()).unwrap();
    let session = Arc::new(IoUringSession::new(64).unwrap());
    let io_engine = Arc::new(IoEngine::new_uring(data_dev, false, session));

    let lifecycle = Arc::new(VolumeLifecycleManager::default());
    let allocator = Arc::new(SpaceAllocator::new(data_size as u64, 0));
    let metrics = Arc::new(onyx_storage::metrics::EngineMetrics::default());

    // Build the ReadPool against the same data device.
    let pool_dev = RawDevice::open(data_tmp.path()).unwrap();
    let read_pool = Arc::new(
        ReadPool::start(
            4,
            64,
            &pool_dev,
            onyx_storage::types::RESERVED_BLOCKS,
            BLOCK_SIZE,
            false,
            metrics.clone(),
        )
        .unwrap(),
    );
    drop(pool_dev);

    std::mem::forget(meta_dir);
    std::mem::forget(buf_tmp);
    std::mem::forget(data_tmp);

    let zm = Arc::new(
        ZoneManager::new_full(
            1,
            256,
            meta.clone(),
            buffer_pool.clone(),
            io_engine.clone(),
            metrics.clone(),
            Some(allocator.clone()),
            Some(read_pool.clone()),
        )
        .unwrap(),
    );

    // Register a volume that uses LZ4 so reads exercise the decompression
    // path on the ReadPool worker.
    let vol = "rp-soak";
    meta.put_volume(&onyx_storage::types::VolumeConfig {
        id: VolumeId(vol.to_string()),
        size_bytes: data_size as u64,
        block_size: 4096,
        compression: CompressionAlgo::Lz4,
        created_at: 0,
        zone_count: 1,
    })
    .unwrap();

    // Compressible payload (repeating pattern → high LZ4 ratio).
    let payloads: Vec<Vec<u8>> = (0..32u64)
        .map(|i| {
            let mut v = Vec::with_capacity(4096);
            for j in 0..4096u32 {
                v.push(((j as u64 ^ i).wrapping_mul(0x100000001b3)) as u8);
            }
            v
        })
        .collect();

    for (i, p) in payloads.iter().enumerate() {
        zm.submit_write(vol, Lba(i as u64), 1, p, 0).unwrap();
    }

    // Flush to push everything through to LV3 so reads must go through the
    // ReadPool (buffer index will be empty after flush).
    let mut flusher = BufferFlusher::start(
        buffer_pool.clone(),
        meta.clone(),
        lifecycle.clone(),
        allocator.clone(),
        io_engine.clone(),
        &FlushConfig::default(),
        &onyx_storage::dedup::config::DedupConfig::default(),
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while buffer_pool.pending_count() > 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "flush did not drain in time"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    flusher.stop();

    // Fire 8 reader threads, each reading the full LBA range twice. With
    // ReadPool active these reads dispatch to 4 worker threads, get batched
    // into io_uring submits, then decompressed in parallel.
    let barrier = Arc::new(Barrier::new(8));
    let mut handles = Vec::new();
    for t in 0..8 {
        let zm = zm.clone();
        let payloads = payloads.clone();
        let barrier = barrier.clone();
        let vol = vol.to_string();
        handles.push(thread::spawn(move || {
            barrier.wait();
            for round in 0..2 {
                for i in 0..32u64 {
                    let got = zm
                        .submit_read(&vol, Lba(i))
                        .expect("read must succeed via ReadPool")
                        .expect("LBA must map after flush");
                    assert_eq!(
                        got,
                        payloads[i as usize],
                        "thread {} round {} lba {} mismatch",
                        t,
                        round,
                        i
                    );
                }
            }
        }));
    }
    let timeout = std::time::Instant::now() + Duration::from_secs(10);
    for h in handles {
        assert!(
            std::time::Instant::now() < timeout,
            "ReadPool reader thread deadlocked"
        );
        h.join().unwrap();
    }

    // Sanity: at least one CRC-clean LV3 hit went through the pool.
    let lv3_hits = metrics
        .read_lv3_hits
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(lv3_hits >= 32 * 16, "expected many lv3 hits, got {lv3_hits}");
    assert_eq!(
        metrics
            .read_crc_errors
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "no CRC errors expected on cold reads"
    );
}
