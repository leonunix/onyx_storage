/// End-to-end tests exercising the full write->flush->read-from-LV3 path.
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use onyx_storage::buffer::entry::*;
use onyx_storage::buffer::flush::BufferFlusher;
use onyx_storage::buffer::pool::WriteBufferPool;
use onyx_storage::config::MetaConfig;
use onyx_storage::io::device::RawDevice;
use onyx_storage::io::engine::IoEngine;
use onyx_storage::meta::store::MetaStore;
use onyx_storage::space::allocator::SpaceAllocator;
use onyx_storage::types::*;
use onyx_storage::zone::worker::ZoneWorker;
use tempfile::{tempdir, NamedTempFile};

fn setup_e2e() -> (
    ZoneWorker,
    Arc<MetaStore>,
    Arc<SpaceAllocator>,
    Arc<WriteBufferPool>,
    Arc<IoEngine>,
) {
    let meta_dir = tempdir().unwrap();
    let meta_config = MetaConfig {
        rocksdb_path: meta_dir.path().to_path_buf(),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let meta = Arc::new(MetaStore::open(&meta_config).unwrap());

    let buf_tmp = NamedTempFile::new().unwrap();
    let buf_size = BUFFER_SUPERBLOCK_SIZE + 100 * BUFFER_ENTRY_SIZE;
    buf_tmp.as_file().set_len(buf_size).unwrap();
    let buf_dev = RawDevice::open_or_create(buf_tmp.path(), buf_size).unwrap();
    let buffer_pool = Arc::new(WriteBufferPool::open(buf_dev).unwrap());

    let data_tmp = NamedTempFile::new().unwrap();
    let data_size: u64 = 4096 * 10000;
    data_tmp.as_file().set_len(data_size).unwrap();
    let data_dev = RawDevice::open(data_tmp.path()).unwrap();
    let io_engine = Arc::new(IoEngine::new(data_dev, false));

    let allocator = Arc::new(SpaceAllocator::new(data_size));

    std::mem::forget(meta_dir);
    std::mem::forget(buf_tmp);
    std::mem::forget(data_tmp);

    let worker = ZoneWorker::new(
        ZoneId(0),
        meta.clone(),
        buffer_pool.clone(),
        io_engine.clone(),
        CompressionAlgo::Lz4,
    );

    (worker, meta, allocator, buffer_pool, io_engine)
}

fn wait_for_flush(pool: &WriteBufferPool, timeout_ms: u64) -> bool {
    let steps = timeout_ms / 10;
    for _ in 0..steps {
        if pool.pending_count() == 0 {
            return true;
        }
        thread::sleep(Duration::from_millis(10));
    }
    false
}

/// Write -> flush to LV3 -> read from LV3 (buffer empty). Full path with LZ4 compression.
#[test]
fn write_flush_read_from_lv3_lz4() {
    let (worker, meta, allocator, pool, io_engine) = setup_e2e();

    let data = vec![0x55; 4096];
    worker.handle_write("test-vol", Lba(0), &data).unwrap();

    // Start flusher and wait
    let mut flusher = BufferFlusher::start(
        pool.clone(),
        meta.clone(),
        allocator.clone(),
        io_engine.clone(),
        onyx_storage::types::CompressionAlgo::Lz4,
        &onyx_storage::config::FlushConfig::default(),
    );
    assert!(wait_for_flush(&pool, 3000), "flush timeout");
    flusher.stop();

    // Read should come from LV3 now (buffer is empty)
    assert_eq!(pool.pending_count(), 0);
    let read_data = worker.handle_read("test-vol", Lba(0)).unwrap().unwrap();
    assert_eq!(read_data, data);
}

/// Write -> flush -> overwrite -> flush -> read. Verify latest data and old PBA freed.
#[test]
fn overwrite_after_flush() {
    let (worker, meta, allocator, pool, io_engine) = setup_e2e();

    let initial_free = allocator.free_block_count();

    // First write
    let data1 = vec![0x11; 4096];
    worker.handle_write("test-vol", Lba(5), &data1).unwrap();

    let mut flusher = BufferFlusher::start(
        pool.clone(),
        meta.clone(),
        allocator.clone(),
        io_engine.clone(),
        onyx_storage::types::CompressionAlgo::Lz4,
        &onyx_storage::config::FlushConfig::default(),
    );
    assert!(wait_for_flush(&pool, 3000));
    flusher.stop();

    assert_eq!(allocator.free_block_count(), initial_free - 1);

    // Overwrite
    let data2 = vec![0x22; 4096];
    worker.handle_write("test-vol", Lba(5), &data2).unwrap();

    let mut flusher = BufferFlusher::start(
        pool.clone(),
        meta.clone(),
        allocator.clone(),
        io_engine.clone(),
        onyx_storage::types::CompressionAlgo::Lz4,
        &onyx_storage::config::FlushConfig::default(),
    );
    assert!(wait_for_flush(&pool, 3000));
    flusher.stop();

    // Still only 1 block allocated (old freed)
    assert_eq!(allocator.free_block_count(), initial_free - 1);

    // Read returns latest
    let read_data = worker.handle_read("test-vol", Lba(5)).unwrap().unwrap();
    assert_eq!(read_data, data2);
}

/// Write multiple LBAs -> flush -> read all back.
#[test]
fn bulk_write_flush_read() {
    let (worker, meta, allocator, pool, io_engine) = setup_e2e();

    for i in 0..20u64 {
        let data = vec![i as u8; 4096];
        worker.handle_write("test-vol", Lba(i), &data).unwrap();
    }

    let mut flusher = BufferFlusher::start(
        pool.clone(),
        meta.clone(),
        allocator.clone(),
        io_engine.clone(),
        onyx_storage::types::CompressionAlgo::Lz4,
        &onyx_storage::config::FlushConfig::default(),
    );
    assert!(wait_for_flush(&pool, 5000));
    flusher.stop();

    for i in 0..20u64 {
        let read_data = worker.handle_read("test-vol", Lba(i)).unwrap().unwrap();
        assert_eq!(read_data, vec![i as u8; 4096]);
    }
}

/// Write with ZSTD -> flush -> read from LV3.
#[test]
fn write_flush_read_zstd() {
    let meta_dir = tempdir().unwrap();
    let meta_config = MetaConfig {
        rocksdb_path: meta_dir.path().to_path_buf(),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let meta = Arc::new(MetaStore::open(&meta_config).unwrap());

    let buf_tmp = NamedTempFile::new().unwrap();
    let buf_size = BUFFER_SUPERBLOCK_SIZE + 100 * BUFFER_ENTRY_SIZE;
    buf_tmp.as_file().set_len(buf_size).unwrap();
    let buf_dev = RawDevice::open_or_create(buf_tmp.path(), buf_size).unwrap();
    let pool = Arc::new(WriteBufferPool::open(buf_dev).unwrap());

    let data_tmp = NamedTempFile::new().unwrap();
    let data_size: u64 = 4096 * 1000;
    data_tmp.as_file().set_len(data_size).unwrap();
    let data_dev = RawDevice::open(data_tmp.path()).unwrap();
    let io_engine = Arc::new(IoEngine::new(data_dev, false));
    let allocator = Arc::new(SpaceAllocator::new(data_size));

    std::mem::forget(meta_dir);
    std::mem::forget(buf_tmp);
    std::mem::forget(data_tmp);

    let worker = ZoneWorker::new(
        ZoneId(0),
        meta.clone(),
        pool.clone(),
        io_engine.clone(),
        CompressionAlgo::Zstd { level: 3 },
    );

    let data = vec![0x77; 4096]; // compressible
    worker.handle_write("test-vol", Lba(0), &data).unwrap();

    let mut flusher = BufferFlusher::start(
        pool.clone(),
        meta.clone(),
        allocator.clone(),
        io_engine.clone(),
        onyx_storage::types::CompressionAlgo::Lz4,
        &onyx_storage::config::FlushConfig::default(),
    );
    assert!(wait_for_flush(&pool, 3000));
    flusher.stop();

    // Read from LV3 -- worker must decompress ZSTD
    let read_data = worker.handle_read("test-vol", Lba(0)).unwrap().unwrap();
    assert_eq!(read_data, data);
}

/// Write with no compression -> flush -> read from LV3.
#[test]
fn write_flush_read_no_compression() {
    let meta_dir = tempdir().unwrap();
    let meta_config = MetaConfig {
        rocksdb_path: meta_dir.path().to_path_buf(),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let meta = Arc::new(MetaStore::open(&meta_config).unwrap());

    let buf_tmp = NamedTempFile::new().unwrap();
    let buf_size = BUFFER_SUPERBLOCK_SIZE + 100 * BUFFER_ENTRY_SIZE;
    buf_tmp.as_file().set_len(buf_size).unwrap();
    let buf_dev = RawDevice::open_or_create(buf_tmp.path(), buf_size).unwrap();
    let pool = Arc::new(WriteBufferPool::open(buf_dev).unwrap());

    let data_tmp = NamedTempFile::new().unwrap();
    let data_size: u64 = 4096 * 1000;
    data_tmp.as_file().set_len(data_size).unwrap();
    let data_dev = RawDevice::open(data_tmp.path()).unwrap();
    let io_engine = Arc::new(IoEngine::new(data_dev, false));
    let allocator = Arc::new(SpaceAllocator::new(data_size));

    std::mem::forget(meta_dir);
    std::mem::forget(buf_tmp);
    std::mem::forget(data_tmp);

    let worker = ZoneWorker::new(
        ZoneId(0),
        meta.clone(),
        pool.clone(),
        io_engine.clone(),
        CompressionAlgo::None,
    );

    // Data small enough to fit in a block (no header overhead now, full 4096 available).
    let data = vec![0x42; 4096];
    worker.handle_write("test-vol", Lba(0), &data).unwrap();

    // Read from buffer (before flush) works fine
    let read_data = worker.handle_read("test-vol", Lba(0)).unwrap().unwrap();
    assert_eq!(read_data, data);

    // With no on-disk header, full 4096-byte payload fits in a block now.
    let mut flusher = BufferFlusher::start(
        pool.clone(),
        meta.clone(),
        allocator.clone(),
        io_engine.clone(),
        onyx_storage::types::CompressionAlgo::Lz4,
        &onyx_storage::config::FlushConfig::default(),
    );
    assert!(wait_for_flush(&pool, 3000));
    flusher.stop();

    let read_data = worker.handle_read("test-vol", Lba(0)).unwrap().unwrap();
    assert_eq!(read_data, data);
}
