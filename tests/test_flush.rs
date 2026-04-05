use std::sync::Arc;
use std::thread;
use std::time::Duration;

use onyx_storage::buffer::entry::*;
use onyx_storage::buffer::flush::BufferFlusher;
use onyx_storage::buffer::pool::WriteBufferPool;
use onyx_storage::config::{FlushConfig, MetaConfig};
use onyx_storage::io::device::RawDevice;
use onyx_storage::io::engine::IoEngine;
use onyx_storage::meta::store::MetaStore;
use onyx_storage::space::allocator::SpaceAllocator;
use onyx_storage::types::*;
use tempfile::{tempdir, NamedTempFile};

fn setup_flush_env() -> (
    Arc<WriteBufferPool>,
    Arc<MetaStore>,
    Arc<SpaceAllocator>,
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
    let data_size = 4096 * 10000;
    data_tmp.as_file().set_len(data_size as u64).unwrap();
    let data_dev = RawDevice::open(data_tmp.path()).unwrap();
    let io_engine = Arc::new(IoEngine::new(data_dev, false));

    let allocator = Arc::new(SpaceAllocator::new(data_size as u64));

    std::mem::forget(meta_dir);
    std::mem::forget(buf_tmp);
    std::mem::forget(data_tmp);

    (buffer_pool, meta, allocator, io_engine)
}

fn start_flusher(
    pool: &Arc<WriteBufferPool>,
    meta: &Arc<MetaStore>,
    allocator: &Arc<SpaceAllocator>,
    io_engine: &Arc<IoEngine>,
) -> BufferFlusher {
    BufferFlusher::start(
        pool.clone(),
        meta.clone(),
        allocator.clone(),
        io_engine.clone(),
        CompressionAlgo::Lz4,
        &FlushConfig::default(),
    )
}

fn wait_flushed(pool: &WriteBufferPool, timeout_ms: u64) -> bool {
    for _ in 0..(timeout_ms / 10) {
        if pool.pending_count() == 0 {
            return true;
        }
        thread::sleep(Duration::from_millis(10));
    }
    false
}

/// Flusher moves buffered entries to LV3 and updates metadata.
#[test]
fn flusher_flushes_entries_to_lv3() {
    let (pool, meta, allocator, io_engine) = setup_flush_env();

    let data = vec![0xAB; 4096];
    pool.append("test-vol", Lba(0), &data).unwrap();
    pool.append("test-vol", Lba(1), &data).unwrap();

    let mut flusher = start_flusher(&pool, &meta, &allocator, &io_engine);
    assert!(wait_flushed(&pool, 5000), "flush timeout");
    flusher.stop();

    let vol_id = VolumeId("test-vol".into());
    let mapping0 = meta.get_mapping(&vol_id, Lba(0)).unwrap();
    assert!(
        mapping0.is_some(),
        "blockmap entry should exist after flush"
    );

    let mapping1 = meta.get_mapping(&vol_id, Lba(1)).unwrap();
    assert!(mapping1.is_some());
}

/// Flusher handles overwrite: old PBA freed, new mapping written.
#[test]
fn flusher_handles_overwrite() {
    let (pool, meta, allocator, io_engine) = setup_flush_env();
    let initial_free = allocator.free_block_count();

    pool.append("test-vol", Lba(5), &[0x11; 4096]).unwrap();

    let mut flusher = start_flusher(&pool, &meta, &allocator, &io_engine);
    assert!(wait_flushed(&pool, 5000));
    flusher.stop();

    // Overwrite
    pool.append("test-vol", Lba(5), &[0x22; 4096]).unwrap();

    let mut flusher = start_flusher(&pool, &meta, &allocator, &io_engine);
    assert!(wait_flushed(&pool, 5000));
    flusher.stop();

    // Space should be reclaimed (old PBA freed)
    // The exact free count depends on compression, but at most 2 blocks used
    assert!(allocator.free_block_count() >= initial_free - 2);
}

#[test]
fn flusher_coalesces_overwrites_before_flush() {
    let (pool, meta, allocator, io_engine) = setup_flush_env();

    pool.append("test-vol", Lba(7), &[0x11; 4096]).unwrap();
    pool.append("test-vol", Lba(7), &[0x22; 4096]).unwrap();

    let mut flusher = start_flusher(&pool, &meta, &allocator, &io_engine);
    assert!(wait_flushed(&pool, 5000), "flush timeout");
    flusher.stop();

    assert_eq!(
        pool.pending_count(),
        0,
        "superseded entries must be retired"
    );

    let mapping = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(7))
        .unwrap()
        .unwrap();
    assert_eq!(mapping.unit_lba_count, 1);
}

#[test]
fn flusher_reclaims_full_extent_for_multi_block_units() {
    let (pool, meta, allocator, io_engine) = setup_flush_env();
    let initial_free = allocator.free_block_count();

    let first0 = (0..4096).map(|i| (i % 251) as u8).collect::<Vec<_>>();
    let first1 = (0..4096).map(|i| ((i * 7) % 251) as u8).collect::<Vec<_>>();
    pool.append("test-vol", Lba(20), &first0).unwrap();
    pool.append("test-vol", Lba(21), &first1).unwrap();

    let mut flusher = BufferFlusher::start(
        pool.clone(),
        meta.clone(),
        allocator.clone(),
        io_engine.clone(),
        CompressionAlgo::None,
        &FlushConfig::default(),
    );
    assert!(wait_flushed(&pool, 5000), "first flush timeout");
    flusher.stop();

    assert_eq!(allocator.free_block_count(), initial_free - 2);

    pool.append("test-vol", Lba(20), &[0xAA; 4096]).unwrap();
    pool.append("test-vol", Lba(21), &[0xBB; 4096]).unwrap();

    let mut flusher = BufferFlusher::start(
        pool.clone(),
        meta.clone(),
        allocator.clone(),
        io_engine.clone(),
        CompressionAlgo::None,
        &FlushConfig::default(),
    );
    assert!(wait_flushed(&pool, 5000), "second flush timeout");
    flusher.stop();

    assert_eq!(
        allocator.free_block_count(),
        initial_free - 2,
        "overwriting a 2-block unit should not leak one block"
    );
}

/// Flusher stop is clean.
#[test]
fn flusher_stop_is_clean() {
    let (pool, meta, allocator, io_engine) = setup_flush_env();
    let mut flusher = start_flusher(&pool, &meta, &allocator, &io_engine);
    thread::sleep(Duration::from_millis(50));
    flusher.stop();
}

/// Flusher drop calls stop automatically.
#[test]
fn flusher_drop_calls_stop() {
    let (pool, meta, allocator, io_engine) = setup_flush_env();
    {
        let _flusher = start_flusher(&pool, &meta, &allocator, &io_engine);
        thread::sleep(Duration::from_millis(50));
    }
    // Drop should not panic
}
