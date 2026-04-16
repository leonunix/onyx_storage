use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use onyx_storage::buffer::flush::{
    clear_test_failpoint, install_test_failpoint, BufferFlusher, FlushFailStage,
};
use onyx_storage::buffer::pool::WriteBufferPool;
use onyx_storage::config::{FlushConfig, MetaConfig};
use onyx_storage::io::device::RawDevice;
use onyx_storage::io::engine::IoEngine;
use onyx_storage::lifecycle::VolumeLifecycleManager;
use onyx_storage::meta::store::MetaStore;
use onyx_storage::space::allocator::SpaceAllocator;
use onyx_storage::types::*;
use tempfile::{tempdir, NamedTempFile};

fn setup_flush_env() -> (
    Arc<WriteBufferPool>,
    Arc<MetaStore>,
    Arc<VolumeLifecycleManager>,
    Arc<SpaceAllocator>,
    Arc<IoEngine>,
) {
    let meta_dir = tempdir().unwrap();
    let meta_config = MetaConfig {
        rocksdb_path: Some(meta_dir.path().to_path_buf()),
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
    let data_size = 4096 * 10000;
    data_tmp.as_file().set_len(data_size as u64).unwrap();
    let data_dev = RawDevice::open(data_tmp.path()).unwrap();
    let io_engine = Arc::new(IoEngine::new(data_dev, false));

    let lifecycle = Arc::new(VolumeLifecycleManager::default());
    let allocator = Arc::new(SpaceAllocator::new(data_size as u64, 0));

    std::mem::forget(meta_dir);
    std::mem::forget(buf_tmp);
    std::mem::forget(data_tmp);

    (buffer_pool, meta, lifecycle, allocator, io_engine)
}

/// Register a volume in MetaStore so write_unit() doesn't discard it.
fn register_volume(meta: &MetaStore, name: &str) {
    meta.put_volume(&VolumeConfig {
        id: VolumeId(name.to_string()),
        size_bytes: 4096 * 10000,
        block_size: 4096,
        compression: CompressionAlgo::None,
        created_at: 0,
        zone_count: 1,
    })
    .unwrap();
}

fn start_flusher(
    pool: &Arc<WriteBufferPool>,
    meta: &Arc<MetaStore>,
    lifecycle: &Arc<VolumeLifecycleManager>,
    allocator: &Arc<SpaceAllocator>,
    io_engine: &Arc<IoEngine>,
) -> BufferFlusher {
    BufferFlusher::start(
        pool.clone(),
        meta.clone(),
        lifecycle.clone(),
        allocator.clone(),
        io_engine.clone(),
        &FlushConfig::default(),
        &onyx_storage::dedup::config::DedupConfig::default(),
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

fn wait_until<F>(timeout_ms: u64, condition: F) -> bool
where
    F: Fn() -> bool,
{
    for _ in 0..(timeout_ms / 10) {
        if condition() {
            return true;
        }
        thread::sleep(Duration::from_millis(10));
    }
    false
}

fn next_test_volume(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    format!("{}-{}", prefix, COUNTER.fetch_add(1, Ordering::Relaxed))
}

struct FailpointGuard {
    vol_id: String,
    start_lba: Lba,
    stage: FlushFailStage,
}

impl FailpointGuard {
    fn install(
        vol_id: &str,
        start_lba: Lba,
        stage: FlushFailStage,
        remaining_hits: Option<u32>,
    ) -> Self {
        install_test_failpoint(vol_id, start_lba, stage, remaining_hits);
        Self {
            vol_id: vol_id.to_string(),
            start_lba,
            stage,
        }
    }

    fn clear(&self) {
        clear_test_failpoint(&self.vol_id, self.start_lba, self.stage);
    }
}

impl Drop for FailpointGuard {
    fn drop(&mut self) {
        self.clear();
    }
}

/// Flusher moves buffered entries to LV3 and updates metadata.
#[test]
fn flusher_flushes_entries_to_lv3() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_flush_env();
    register_volume(&meta, "test-vol");

    let data = vec![0xAB; 4096];
    pool.append("test-vol", Lba(0), 1, &data, 0).unwrap();
    pool.append("test-vol", Lba(1), 1, &data, 0).unwrap();

    let mut flusher = start_flusher(&pool, &meta, &lifecycle, &allocator, &io_engine);
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
    let (pool, meta, lifecycle, allocator, io_engine) = setup_flush_env();
    register_volume(&meta, "test-vol");
    let initial_free = allocator.free_block_count();

    pool.append("test-vol", Lba(5), 1, &[0x11; 4096], 0)
        .unwrap();

    let mut flusher = start_flusher(&pool, &meta, &lifecycle, &allocator, &io_engine);
    assert!(wait_flushed(&pool, 5000));
    flusher.stop();

    // Overwrite
    pool.append("test-vol", Lba(5), 1, &[0x22; 4096], 0)
        .unwrap();

    let mut flusher = start_flusher(&pool, &meta, &lifecycle, &allocator, &io_engine);
    assert!(wait_flushed(&pool, 5000));
    flusher.stop();

    // Space should be reclaimed (old PBA freed)
    // The exact free count depends on compression, but at most 2 blocks used
    assert!(allocator.free_block_count() >= initial_free - 2);
}

#[test]
fn flusher_coalesces_overwrites_before_flush() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_flush_env();
    register_volume(&meta, "test-vol");

    pool.append("test-vol", Lba(7), 1, &[0x11; 4096], 0)
        .unwrap();
    pool.append("test-vol", Lba(7), 1, &[0x22; 4096], 0)
        .unwrap();

    let mut flusher = start_flusher(&pool, &meta, &lifecycle, &allocator, &io_engine);
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
    let (pool, meta, lifecycle, allocator, io_engine) = setup_flush_env();
    register_volume(&meta, "test-vol");
    let initial_free = allocator.free_block_count();

    let first0 = (0..4096).map(|i| (i % 251) as u8).collect::<Vec<_>>();
    let first1 = (0..4096).map(|i| ((i * 7) % 251) as u8).collect::<Vec<_>>();
    pool.append("test-vol", Lba(20), 1, &first0, 0).unwrap();
    pool.append("test-vol", Lba(21), 1, &first1, 0).unwrap();

    let mut flusher = start_flusher(&pool, &meta, &lifecycle, &allocator, &io_engine);
    assert!(wait_flushed(&pool, 5000), "first flush timeout");
    flusher.stop();

    assert_eq!(allocator.free_block_count(), initial_free - 2);

    pool.append("test-vol", Lba(20), 1, &[0xAA; 4096], 0)
        .unwrap();
    pool.append("test-vol", Lba(21), 1, &[0xBB; 4096], 0)
        .unwrap();

    let mut flusher = start_flusher(&pool, &meta, &lifecycle, &allocator, &io_engine);
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
    let (pool, meta, lifecycle, allocator, io_engine) = setup_flush_env();
    let mut flusher = start_flusher(&pool, &meta, &lifecycle, &allocator, &io_engine);
    thread::sleep(Duration::from_millis(50));
    flusher.stop();
}

/// Flusher drop calls stop automatically.
#[test]
fn flusher_drop_calls_stop() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_flush_env();
    {
        let _flusher = start_flusher(&pool, &meta, &lifecycle, &allocator, &io_engine);
        thread::sleep(Duration::from_millis(50));
    }
    // Drop should not panic
}

#[test]
fn flusher_retries_after_injected_lv3_write_failure() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_flush_env();
    let initial_free = allocator.free_block_count();
    let vol_id = next_test_volume("fail-lv3");
    register_volume(&meta, &vol_id);
    let lba = Lba(40);
    let _guard = FailpointGuard::install(&vol_id, lba, FlushFailStage::BeforeIoWrite, None);

    pool.append(&vol_id, lba, 1, &[0xAB; 4096], 0).unwrap();

    let mut flusher = start_flusher(&pool, &meta, &lifecycle, &allocator, &io_engine);

    assert!(
        wait_until(1000, || {
            pool.pending_count() == 1
                && meta
                    .get_mapping(&VolumeId(vol_id.clone()), lba)
                    .unwrap()
                    .is_none()
                && allocator.free_block_count() == initial_free
        }),
        "entry should remain pending and allocator must not leak while LV3 writes keep failing"
    );

    clear_test_failpoint(&vol_id, lba, FlushFailStage::BeforeIoWrite);

    assert!(
        wait_flushed(&pool, 5000),
        "flush retry timeout after LV3 failure"
    );
    flusher.stop();

    let mapping = meta.get_mapping(&VolumeId(vol_id.clone()), lba).unwrap();
    assert!(
        mapping.is_some(),
        "mapping should appear after retry succeeds"
    );
    assert_eq!(allocator.free_block_count(), initial_free - 1);
}

#[test]
fn flusher_retries_after_injected_metadata_failure() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_flush_env();
    let initial_free = allocator.free_block_count();
    let vol_id = next_test_volume("fail-meta");
    register_volume(&meta, &vol_id);
    let lba = Lba(50);
    let _guard = FailpointGuard::install(&vol_id, lba, FlushFailStage::BeforeMetaWrite, None);

    pool.append(&vol_id, lba, 1, &[0xCD; 4096], 0).unwrap();

    let mut flusher = start_flusher(&pool, &meta, &lifecycle, &allocator, &io_engine);

    assert!(
        wait_until(1000, || {
            pool.pending_count() == 1
                && meta.get_mapping(&VolumeId(vol_id.clone()), lba).unwrap().is_none()
                && allocator.free_block_count() == initial_free
        }),
        "entry should remain pending and allocator must not leak while metadata writes keep failing"
    );

    clear_test_failpoint(&vol_id, lba, FlushFailStage::BeforeMetaWrite);

    assert!(
        wait_flushed(&pool, 5000),
        "flush retry timeout after metadata failure"
    );
    flusher.stop();

    let mapping = meta.get_mapping(&VolumeId(vol_id.clone()), lba).unwrap();
    assert!(
        mapping.is_some(),
        "mapping should appear after retry succeeds"
    );
    assert_eq!(allocator.free_block_count(), initial_free - 1);
}

#[test]
fn flusher_retries_recovered_entries_during_sustained_new_writes() {
    let meta_dir = tempdir().unwrap();
    let meta = Arc::new(
        MetaStore::open(&MetaConfig {
            rocksdb_path: Some(meta_dir.path().to_path_buf()),
            block_cache_mb: 8,
            wal_dir: None,
        })
        .unwrap(),
    );
    register_volume(&meta, "retry-recovered");

    let buf_tmp = NamedTempFile::new().unwrap();
    let buf_size = 4096 + 4096 + 1024 * 8192;
    buf_tmp.as_file().set_len(buf_size).unwrap();

    {
        let buf_dev = RawDevice::open_or_create(buf_tmp.path(), buf_size).unwrap();
        let pool = WriteBufferPool::open(buf_dev).unwrap();
        for idx in 0..8u64 {
            let payload = vec![idx as u8; 32 * BLOCK_SIZE as usize];
            pool.append("retry-recovered", Lba(idx * 32), 32, &payload, 0)
                .unwrap();
        }
    }

    let recovered_pool = Arc::new(
        WriteBufferPool::open_with_options_and_memory_limit(
            RawDevice::open_or_create(buf_tmp.path(), buf_size).unwrap(),
            Duration::from_millis(1),
            1,
            256,
            Duration::from_secs(1),
            32 * BLOCK_SIZE as u64,
        )
        .unwrap(),
    );
    assert_eq!(recovered_pool.payload_memory_bytes(), 0);
    assert_eq!(
        recovered_pool.payload_memory_limit_bytes(),
        32 * BLOCK_SIZE as u64
    );
    assert_eq!(recovered_pool.pending_count(), 8);

    let data_tmp = NamedTempFile::new().unwrap();
    let data_size = 4096 * 20000;
    data_tmp.as_file().set_len(data_size as u64).unwrap();
    let io_engine = Arc::new(IoEngine::new(
        RawDevice::open(data_tmp.path()).unwrap(),
        false,
    ));
    let lifecycle = Arc::new(VolumeLifecycleManager::default());
    let allocator = Arc::new(SpaceAllocator::new(data_size as u64, 1));
    let mut flusher = BufferFlusher::start(
        recovered_pool.clone(),
        meta.clone(),
        lifecycle,
        allocator,
        io_engine,
        &FlushConfig::default(),
        &onyx_storage::dedup::config::DedupConfig {
            enabled: false,
            ..Default::default()
        },
    );

    let keep_writing = Arc::new(AtomicBool::new(true));
    let writer_flag = keep_writing.clone();
    let writer_pool = recovered_pool.clone();
    let writer = thread::spawn(move || {
        let mut i = 0u64;
        while writer_flag.load(Ordering::Relaxed) {
            let lba = Lba(4096 + (i % 64));
            let payload = vec![(i & 0xFF) as u8; BLOCK_SIZE as usize];
            let _ = writer_pool.append("retry-recovered", lba, 1, &payload, 0);
            i = i.wrapping_add(1);
        }
    });

    let old_vol = VolumeId("retry-recovered".into());
    let recovered_old_entries = wait_until(3000, || {
        (0..8u64).all(|idx| meta.get_mapping(&old_vol, Lba(idx * 32)).unwrap().is_some())
    });

    keep_writing.store(false, Ordering::Relaxed);
    writer.join().unwrap();
    assert!(
        recovered_old_entries,
        "recovered pending entries should keep retrying even while new writes keep arriving"
    );

    assert!(
        wait_flushed(&recovered_pool, 5000),
        "all pending entries should eventually drain"
    );
    flusher.stop();
}
