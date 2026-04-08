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
        rocksdb_path: meta_dir.path().to_path_buf(),
        block_cache_mb: 8,
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
        rocksdb_path: meta_dir.path().to_path_buf(),
        block_cache_mb: 8,
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
