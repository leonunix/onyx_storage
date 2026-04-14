use std::sync::Arc;
use std::thread;
use std::time::Duration;

use onyx_storage::buffer::flush::{
    clear_test_dedup_hit_failpoint, clear_test_failpoint, install_test_dedup_hit_failpoint,
    install_test_failpoint, BufferFlusher, FlushFailStage,
};
use onyx_storage::buffer::pool::WriteBufferPool;
use onyx_storage::compress::codec::create_compressor;
use onyx_storage::config::FlushConfig;
use onyx_storage::dedup::config::DedupConfig;
use onyx_storage::dedup::scanner::DedupScanner;
use onyx_storage::io::device::RawDevice;
use onyx_storage::io::engine::IoEngine;
use onyx_storage::lifecycle::VolumeLifecycleManager;
use onyx_storage::meta::schema::*;
use onyx_storage::meta::store::MetaStore;
use onyx_storage::packer::packer::{new_hole_map, HoleKey, HoleMap};
use onyx_storage::space::allocator::SpaceAllocator;
use onyx_storage::types::*;
use onyx_storage::zone::worker::ZoneWorker;
use tempfile::{tempdir, NamedTempFile};

fn setup_dedup_env() -> (
    Arc<WriteBufferPool>,
    Arc<MetaStore>,
    Arc<VolumeLifecycleManager>,
    Arc<SpaceAllocator>,
    Arc<IoEngine>,
) {
    setup_dedup_env_with_sizes(1024 * 1024, 16 * 1024 * 1024)
}

fn setup_dedup_env_with_sizes(
    buf_bytes: u64,
    data_bytes: u64,
) -> (
    Arc<WriteBufferPool>,
    Arc<MetaStore>,
    Arc<VolumeLifecycleManager>,
    Arc<SpaceAllocator>,
    Arc<IoEngine>,
) {
    let meta_dir = tempdir().unwrap();
    let buf_tmp = NamedTempFile::new().unwrap();
    let data_tmp = NamedTempFile::new().unwrap();
    buf_tmp.as_file().set_len(buf_bytes).unwrap();
    data_tmp.as_file().set_len(data_bytes).unwrap();

    let meta_config = onyx_storage::config::MetaConfig {
        rocksdb_path: Some(meta_dir.path().to_path_buf()),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let meta = Arc::new(MetaStore::open(&meta_config).unwrap());

    let buf_dev = RawDevice::open(buf_tmp.path()).unwrap();
    let pool = Arc::new(WriteBufferPool::open(buf_dev).unwrap());

    let data_dev = RawDevice::open(data_tmp.path()).unwrap();
    let io_engine = Arc::new(IoEngine::new(data_dev, false));
    let allocator = Arc::new(SpaceAllocator::new(16 * 1024 * 1024, 0));
    let lifecycle = Arc::new(VolumeLifecycleManager::default());

    // Keep temp files alive by leaking them
    std::mem::forget(meta_dir);
    std::mem::forget(buf_tmp);
    std::mem::forget(data_tmp);

    (pool, meta, lifecycle, allocator, io_engine)
}

fn setup_dedup_env_small_buffer() -> (
    Arc<WriteBufferPool>,
    Arc<MetaStore>,
    Arc<VolumeLifecycleManager>,
    Arc<SpaceAllocator>,
    Arc<IoEngine>,
) {
    setup_dedup_env_with_sizes(64 * 1024, 16 * 1024 * 1024)
}

fn register_volume(meta: &MetaStore, name: &str) {
    register_volume_with(meta, name, CompressionAlgo::None, 1000);
}

fn register_volume_with(
    meta: &MetaStore,
    name: &str,
    compression: CompressionAlgo,
    created_at: u64,
) {
    meta.put_volume(&VolumeConfig {
        id: VolumeId(name.to_string()),
        size_bytes: 1024 * 1024 * 1024,
        block_size: 4096,
        compression,
        created_at,
        zone_count: 4,
    })
    .unwrap();
}

fn dedup_test_config() -> DedupConfig {
    DedupConfig {
        enabled: true,
        workers: 2,
        buffer_skip_threshold_pct: 90,
        ..Default::default()
    }
}

fn dedup_always_skip_config() -> DedupConfig {
    DedupConfig {
        buffer_skip_threshold_pct: 0,
        ..dedup_test_config()
    }
}

fn dedup_scanner_config(buffer_skip_threshold_pct: u8) -> DedupConfig {
    DedupConfig {
        buffer_skip_threshold_pct,
        rescan_interval_ms: 20,
        max_rescan_per_cycle: 256,
        ..dedup_test_config()
    }
}

fn start_flusher_with_dedup(
    pool: &Arc<WriteBufferPool>,
    meta: &Arc<MetaStore>,
    lifecycle: &Arc<VolumeLifecycleManager>,
    allocator: &Arc<SpaceAllocator>,
    io_engine: &Arc<IoEngine>,
) -> BufferFlusher {
    start_flusher_custom(
        pool,
        meta,
        lifecycle,
        allocator,
        io_engine,
        new_hole_map(),
        dedup_test_config(),
    )
}

fn start_flusher_custom(
    pool: &Arc<WriteBufferPool>,
    meta: &Arc<MetaStore>,
    lifecycle: &Arc<VolumeLifecycleManager>,
    allocator: &Arc<SpaceAllocator>,
    io_engine: &Arc<IoEngine>,
    hole_map: HoleMap,
    dedup_config: DedupConfig,
) -> BufferFlusher {
    BufferFlusher::start(
        pool.clone(),
        meta.clone(),
        lifecycle.clone(),
        allocator.clone(),
        io_engine.clone(),
        &FlushConfig::default(),
        hole_map,
        &dedup_config,
    )
}

fn start_scanner(
    pool: &Arc<WriteBufferPool>,
    meta: &Arc<MetaStore>,
    lifecycle: &Arc<VolumeLifecycleManager>,
    allocator: &Arc<SpaceAllocator>,
    io_engine: &Arc<IoEngine>,
    config: DedupConfig,
) -> DedupScanner {
    DedupScanner::start(
        meta.clone(),
        io_engine.clone(),
        allocator.clone(),
        lifecycle.clone(),
        pool.clone(),
        config,
    )
}

fn start_scanner_with_hole_map(
    pool: &Arc<WriteBufferPool>,
    meta: &Arc<MetaStore>,
    lifecycle: &Arc<VolumeLifecycleManager>,
    allocator: &Arc<SpaceAllocator>,
    io_engine: &Arc<IoEngine>,
    hole_map: HoleMap,
    config: DedupConfig,
) -> DedupScanner {
    DedupScanner::start_with_metrics(
        Arc::new(onyx_storage::metrics::EngineMetrics::default()),
        meta.clone(),
        io_engine.clone(),
        allocator.clone(),
        lifecycle.clone(),
        pool.clone(),
        hole_map,
        config,
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

fn wait_until(timeout_ms: u64, mut predicate: impl FnMut() -> bool) -> bool {
    for _ in 0..(timeout_ms / 10) {
        if predicate() {
            return true;
        }
        thread::sleep(Duration::from_millis(10));
    }
    false
}

// --- Schema tests ---

#[test]
fn dedup_entry_roundtrip() {
    let entry = DedupEntry {
        pba: Pba(42),
        slot_offset: 100,
        compression: 1,
        unit_compressed_size: 2048,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0xDEAD,
    };
    let encoded = encode_dedup_entry(&entry);
    assert_eq!(encoded.len(), 27);
    let decoded = decode_dedup_entry(&encoded).unwrap();
    assert_eq!(decoded, entry);
}

#[test]
fn dedup_reverse_key_roundtrip() {
    let pba = Pba(123);
    let hash: ContentHash = [0xAB; 32];
    let key = encode_dedup_reverse_key(pba, &hash);
    assert_eq!(key.len(), 40);
    let (decoded_pba, decoded_hash) = decode_dedup_reverse_key(&key).unwrap();
    assert_eq!(decoded_pba, pba);
    assert_eq!(decoded_hash, hash);
}

#[test]
fn dedup_entry_to_blockmap_value() {
    let entry = DedupEntry {
        pba: Pba(42),
        slot_offset: 100,
        compression: 1,
        unit_compressed_size: 2048,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0xDEAD,
    };
    let bv = entry.to_blockmap_value();
    assert_eq!(bv.pba, Pba(42));
    assert_eq!(bv.slot_offset, 100);
    assert_eq!(bv.flags, 0);
}

#[test]
fn blockmap_value_28byte_with_flags() {
    let v = BlockmapValue {
        pba: Pba(42),
        compression: 1,
        unit_compressed_size: 2048,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0xDEAD,
        slot_offset: 0,
        flags: FLAG_DEDUP_SKIPPED,
    };
    let encoded = encode_blockmap_value(&v);
    assert_eq!(encoded.len(), 28);
    let decoded = decode_blockmap_value(&encoded).unwrap();
    assert_eq!(decoded.flags, FLAG_DEDUP_SKIPPED);
}

#[test]
fn blockmap_value_rejects_27byte_format() {
    assert!(decode_blockmap_value(&[0u8; 27]).is_none());
}

// --- MetaStore dedup operations ---

#[test]
fn dedup_index_crud() {
    let dir = tempdir().unwrap();
    let config = onyx_storage::config::MetaConfig {
        rocksdb_path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let store = MetaStore::open(&config).unwrap();

    let hash: ContentHash = *blake3::hash(b"hello world").as_bytes();
    let entry = DedupEntry {
        pba: Pba(100),
        slot_offset: 0,
        compression: 0,
        unit_compressed_size: 4096,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0x1234,
    };

    // Initially empty
    assert!(store.get_dedup_entry(&hash).unwrap().is_none());

    // Insert
    store.put_dedup_entries(&[(hash, entry)]).unwrap();

    // Lookup
    let found = store.get_dedup_entry(&hash).unwrap().unwrap();
    assert_eq!(found.pba, Pba(100));
    assert_eq!(found.crc32, 0x1234);

    // Delete
    store.delete_dedup_index(&hash).unwrap();
    assert!(store.get_dedup_entry(&hash).unwrap().is_none());
}

#[test]
fn dedup_cleanup_on_pba_free() {
    let dir = tempdir().unwrap();
    let config = onyx_storage::config::MetaConfig {
        rocksdb_path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let store = MetaStore::open(&config).unwrap();

    let hash1: ContentHash = [0x01; 32];
    let hash2: ContentHash = [0x02; 32];
    let pba = Pba(200);

    let entry1 = DedupEntry {
        pba,
        slot_offset: 0,
        compression: 0,
        unit_compressed_size: 4096,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0xAAAA,
    };
    let entry2 = DedupEntry {
        pba,
        slot_offset: 0,
        compression: 0,
        unit_compressed_size: 4096,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 1,
        crc32: 0xBBBB,
    };

    // Insert two entries for same PBA
    store
        .put_dedup_entries(&[(hash1, entry1), (hash2, entry2)])
        .unwrap();
    assert!(store.get_dedup_entry(&hash1).unwrap().is_some());
    assert!(store.get_dedup_entry(&hash2).unwrap().is_some());

    // Cleanup for PBA
    store.cleanup_dedup_for_pba_standalone(pba).unwrap();

    // Both should be gone
    assert!(store.get_dedup_entry(&hash1).unwrap().is_none());
    assert!(store.get_dedup_entry(&hash2).unwrap().is_none());
}

#[test]
fn dedup_entry_liveness_requires_exact_fragment_identity() {
    let dir = tempdir().unwrap();
    let config = onyx_storage::config::MetaConfig {
        rocksdb_path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let store = MetaStore::open(&config).unwrap();
    let vol_id = VolumeId("dedup-live-check".into());
    let pba = Pba(321);

    let live_block = vec![0x5A; BLOCK_SIZE as usize];
    let stale_block = vec![0xA5; BLOCK_SIZE as usize];
    let live_hash: ContentHash = *blake3::hash(&live_block).as_bytes();
    let stale_hash: ContentHash = *blake3::hash(&stale_block).as_bytes();

    let live_mapping = BlockmapValue {
        pba,
        compression: 0,
        unit_compressed_size: BLOCK_SIZE,
        unit_original_size: BLOCK_SIZE,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: crc32fast::hash(&live_block),
        slot_offset: 0,
        flags: 0,
    };
    store.put_mapping(&vol_id, Lba(0), &live_mapping).unwrap();
    store.set_refcount(pba, 1).unwrap();

    let live_entry = DedupEntry {
        pba,
        slot_offset: 0,
        compression: 0,
        unit_compressed_size: BLOCK_SIZE,
        unit_original_size: BLOCK_SIZE,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: live_mapping.crc32,
    };
    let stale_entry = DedupEntry {
        pba,
        slot_offset: 0,
        compression: 0,
        unit_compressed_size: BLOCK_SIZE,
        unit_original_size: BLOCK_SIZE,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: crc32fast::hash(&stale_block),
    };
    store
        .put_dedup_entries(&[(live_hash, live_entry), (stale_hash, stale_entry)])
        .unwrap();

    assert!(store.dedup_entry_is_live(&live_hash, &live_entry).unwrap());
    assert!(
        !store
            .dedup_entry_is_live(&stale_hash, &stale_entry)
            .unwrap(),
        "a reused PBA must not keep an old fragment-looking dedup entry live"
    );
}

#[test]
fn scan_dedup_skipped() {
    let dir = tempdir().unwrap();
    let config = onyx_storage::config::MetaConfig {
        rocksdb_path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let store = MetaStore::open(&config).unwrap();
    let vol_id = VolumeId("test-vol".into());

    // Write entry with DEDUP_SKIPPED flag
    let val_skipped = BlockmapValue {
        pba: Pba(50),
        compression: 0,
        unit_compressed_size: 4096,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0,
        slot_offset: 0,
        flags: FLAG_DEDUP_SKIPPED,
    };
    store.put_mapping(&vol_id, Lba(0), &val_skipped).unwrap();

    // Write normal entry
    let val_normal = BlockmapValue {
        pba: Pba(60),
        compression: 0,
        unit_compressed_size: 4096,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0,
        slot_offset: 0,
        flags: 0,
    };
    store.put_mapping(&vol_id, Lba(1), &val_normal).unwrap();

    // Scan should find only the skipped one
    let results = store.scan_dedup_skipped(100).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, "test-vol");
    assert_eq!(results[0].1, Lba(0));
    assert_eq!(results[0].2.flags & FLAG_DEDUP_SKIPPED, FLAG_DEDUP_SKIPPED);
}

#[test]
fn update_blockmap_flags_clears_dedup_skipped() {
    let dir = tempdir().unwrap();
    let config = onyx_storage::config::MetaConfig {
        rocksdb_path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let store = MetaStore::open(&config).unwrap();
    let vol_id = VolumeId("test-vol".into());

    let val = BlockmapValue {
        pba: Pba(50),
        compression: 0,
        unit_compressed_size: 4096,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0,
        slot_offset: 0,
        flags: FLAG_DEDUP_SKIPPED,
    };
    store.put_mapping(&vol_id, Lba(0), &val).unwrap();

    // Clear the flag
    store.update_blockmap_flags(&vol_id, Lba(0), 0).unwrap();

    let loaded = store.get_mapping(&vol_id, Lba(0)).unwrap().unwrap();
    assert_eq!(loaded.flags, 0);

    // Scan should now find nothing
    assert!(store.scan_dedup_skipped(100).unwrap().is_empty());
}

// --- DedupConfig tests ---

#[test]
fn dedup_config_defaults() {
    let config = DedupConfig::default();
    assert!(config.enabled);
    assert_eq!(config.workers, 2);
    assert_eq!(config.buffer_skip_threshold_pct, 90);
    assert_eq!(config.rescan_interval_ms, 30000);
    assert_eq!(config.max_rescan_per_cycle, 256);
}

// --- Integration: flusher with dedup enabled ---

#[test]
fn dedup_miss_populates_index() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env();
    register_volume(&meta, "test-vol");

    // Write a unique block
    let data = vec![0xAA; 4096];
    let hash: ContentHash = *blake3::hash(&data).as_bytes();
    pool.append("test-vol", Lba(0), 1, &data, 1000).unwrap();

    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    assert!(wait_flushed(&pool, 10000), "flush timeout");
    flusher.stop();

    // Verify block was written
    let mapping = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(0))
        .unwrap()
        .unwrap();
    assert_eq!(mapping.flags, 0); // Not skipped

    // Verify dedup index was populated
    let dedup_entry = meta.get_dedup_entry(&hash).unwrap();
    assert!(
        dedup_entry.is_some(),
        "dedup index should be populated for miss blocks"
    );
    assert_eq!(dedup_entry.unwrap().pba, mapping.pba);
}

#[test]
fn dedup_hit_reuses_pba() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env();
    register_volume(&meta, "test-vol");

    // Write first block
    let data = vec![0xBB; 4096];
    pool.append("test-vol", Lba(0), 1, &data, 1000).unwrap();

    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    assert!(wait_flushed(&pool, 10000), "flush timeout for first write");

    let first_mapping = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(0))
        .unwrap()
        .unwrap();
    let first_pba = first_mapping.pba;

    // Write same data to different LBA — should be dedup hit
    pool.append("test-vol", Lba(1), 1, &data, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000), "flush timeout for dedup write");
    flusher.stop();

    let second_mapping = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(1))
        .unwrap()
        .unwrap();

    // Both LBAs should point to the same PBA (dedup hit)
    assert_eq!(second_mapping.pba, first_pba, "dedup hit should reuse PBA");

    // Refcount should be 2
    let rc = meta.get_refcount(first_pba).unwrap();
    assert_eq!(rc, 2, "refcount should be 2 for dedup'd PBA");
}

#[test]
fn delete_volume_cleans_dedup_index() {
    let dir = tempdir().unwrap();
    let config = onyx_storage::config::MetaConfig {
        rocksdb_path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let store = MetaStore::open(&config).unwrap();

    let vol = VolumeConfig {
        id: VolumeId("test-vol".into()),
        size_bytes: 1024 * 1024 * 1024,
        block_size: 4096,
        compression: CompressionAlgo::None,
        created_at: 1000,
        zone_count: 4,
    };
    store.put_volume(&vol).unwrap();

    // Set up a dedup entry pointing to PBA 100
    let hash: ContentHash = [0xCC; 32];
    let dedup_entry = DedupEntry {
        pba: Pba(100),
        slot_offset: 0,
        compression: 0,
        unit_compressed_size: 4096,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0,
    };
    store.put_dedup_entries(&[(hash, dedup_entry)]).unwrap();

    // Map LBA 0 to PBA 100, refcount = 1
    let bv = BlockmapValue {
        pba: Pba(100),
        compression: 0,
        unit_compressed_size: 4096,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0,
        slot_offset: 0,
        flags: 0,
    };
    store.atomic_write_mapping(&vol.id, Lba(0), &bv).unwrap();

    // Delete volume — should clean up dedup index for freed PBA
    store.delete_volume(&vol.id).unwrap();

    // Dedup entry should be cleaned up
    assert!(
        store.get_dedup_entry(&hash).unwrap().is_none(),
        "dedup index should be cleaned up when PBA is freed"
    );
}

#[test]
fn cleanup_old_pba_preserves_newer_forward_index() {
    let dir = tempdir().unwrap();
    let config = onyx_storage::config::MetaConfig {
        rocksdb_path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let store = MetaStore::open(&config).unwrap();

    let hash: ContentHash = [0xDD; 32];
    let entry_old = DedupEntry {
        pba: Pba(100),
        slot_offset: 0,
        compression: 0,
        unit_compressed_size: 4096,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0x1111,
    };
    let entry_new = DedupEntry {
        pba: Pba(200),
        slot_offset: 0,
        compression: 0,
        unit_compressed_size: 4096,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0x2222,
    };

    store.put_dedup_entries(&[(hash, entry_old)]).unwrap();
    store.put_dedup_entries(&[(hash, entry_new)]).unwrap();

    store.cleanup_dedup_for_pba_standalone(Pba(100)).unwrap();

    let current = store.get_dedup_entry(&hash).unwrap().unwrap();
    assert_eq!(
        current.pba,
        Pba(200),
        "cleaning the old reverse entry must not delete the newer forward mapping"
    );
}

// --- Concurrency: multiple dedup hits to same PBA ---

#[test]
fn dedup_concurrent_hits_correct_refcount() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env();
    register_volume(&meta, "test-vol");

    // Write a unique block — becomes dedup miss, populates index
    let data = vec![0xCC; 4096];
    pool.append("test-vol", Lba(0), 1, &data, 1000).unwrap();

    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    assert!(
        wait_flushed(&pool, 10000),
        "flush timeout for initial write"
    );

    let first_pba = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(0))
        .unwrap()
        .unwrap()
        .pba;
    assert_eq!(meta.get_refcount(first_pba).unwrap(), 1);

    // Write same content to 4 different LBAs — all should be dedup hits
    // With 2 dedup workers, these may be processed concurrently
    for lba in 1..5u64 {
        pool.append("test-vol", Lba(lba), 1, &data, 1000).unwrap();
    }
    assert!(
        wait_flushed(&pool, 10000),
        "flush timeout for concurrent dedup writes"
    );
    flusher.stop();

    // All 5 LBAs should point to the same PBA
    for lba in 0..5u64 {
        let mapping = meta
            .get_mapping(&VolumeId("test-vol".into()), Lba(lba))
            .unwrap()
            .unwrap();
        assert_eq!(
            mapping.pba, first_pba,
            "LBA {} should point to dedup'd PBA",
            lba
        );
    }

    // Refcount should be exactly 5 (no lost increments from concurrent hits)
    let rc = meta.get_refcount(first_pba).unwrap();
    assert_eq!(
        rc, 5,
        "refcount should be 5 after 4 concurrent dedup hits + 1 original"
    );
}

#[test]
fn dedup_interleaved_hit_miss_pattern() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env();
    register_volume(&meta, "test-vol");

    // Write 4 unique blocks — all misses, populate dedup index
    for i in 0..4u8 {
        let data = vec![i + 0xA0; 4096];
        pool.append("test-vol", Lba(i as u64), 1, &data, 1000)
            .unwrap();
    }
    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    assert!(
        wait_flushed(&pool, 10000),
        "flush timeout for initial writes"
    );

    // Write a mix of duplicate and new content to LBAs 10-17
    // hit, miss, hit, miss pattern
    let hit_data_0 = vec![0xA0u8; 4096]; // same as LBA 0
    let miss_data_0 = vec![0xF0u8; 4096]; // new
    let hit_data_1 = vec![0xA1u8; 4096]; // same as LBA 1
    let miss_data_1 = vec![0xF1u8; 4096]; // new

    pool.append("test-vol", Lba(10), 1, &hit_data_0, 1000)
        .unwrap();
    pool.append("test-vol", Lba(11), 1, &miss_data_0, 1000)
        .unwrap();
    pool.append("test-vol", Lba(12), 1, &hit_data_1, 1000)
        .unwrap();
    pool.append("test-vol", Lba(13), 1, &miss_data_1, 1000)
        .unwrap();

    assert!(
        wait_flushed(&pool, 10000),
        "flush timeout for mixed pattern"
    );
    flusher.stop();

    // All 8 LBAs should be mapped
    for lba in [0, 1, 2, 3, 10, 11, 12, 13] {
        assert!(
            meta.get_mapping(&VolumeId("test-vol".into()), Lba(lba))
                .unwrap()
                .is_some(),
            "LBA {} should be mapped",
            lba
        );
    }

    // Dedup hits: LBA 10 should share PBA with LBA 0, LBA 12 with LBA 1
    let pba_0 = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(0))
        .unwrap()
        .unwrap()
        .pba;
    let pba_10 = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(10))
        .unwrap()
        .unwrap()
        .pba;
    assert_eq!(pba_0, pba_10, "LBA 10 should dedup to LBA 0's PBA");

    let pba_1 = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(1))
        .unwrap()
        .unwrap()
        .pba;
    let pba_12 = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(12))
        .unwrap()
        .unwrap()
        .pba;
    assert_eq!(pba_1, pba_12, "LBA 12 should dedup to LBA 1's PBA");

    // Miss LBAs should have their own PBAs
    let pba_11 = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(11))
        .unwrap()
        .unwrap()
        .pba;
    let pba_13 = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(13))
        .unwrap()
        .unwrap()
        .pba;
    assert_ne!(pba_11, pba_0, "LBA 11 should have its own PBA");
    assert_ne!(pba_13, pba_1, "LBA 13 should have its own PBA");
}

/// Multi-LBA entry split by dedup into hit/miss/hit/miss pattern.
/// This specifically tests the DedupCompletion counter: the coalescer
/// must not re-dispatch the entry until ALL miss sub-units are flushed.
#[test]
fn dedup_multi_lba_entry_interleaved_hit_miss() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env();
    register_volume(&meta, "test-vol");

    // Write 4 unique blocks to populate dedup index
    let data_a = vec![0xA0u8; 4096];
    let data_b = vec![0xB0u8; 4096];
    for (lba, data) in [(0u64, &data_a), (2, &data_b)] {
        pool.append("test-vol", Lba(lba), 1, data, 1000).unwrap();
    }
    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    assert!(
        wait_flushed(&pool, 10000),
        "flush timeout for initial unique blocks"
    );

    // Now write a multi-LBA entry: [hit, miss, hit, miss]
    // LBA 10=0xA0(dup), 11=0xC0(new), 12=0xB0(dup), 13=0xD0(new)
    let data_c = vec![0xC0u8; 4096];
    let data_d = vec![0xD0u8; 4096];
    let mut multi_block = Vec::with_capacity(4 * 4096);
    multi_block.extend_from_slice(&data_a); // LBA 10 — dedup hit
    multi_block.extend_from_slice(&data_c); // LBA 11 — miss
    multi_block.extend_from_slice(&data_b); // LBA 12 — dedup hit
    multi_block.extend_from_slice(&data_d); // LBA 13 — miss
    pool.append("test-vol", Lba(10), 4, &multi_block, 1000)
        .unwrap();

    assert!(
        wait_flushed(&pool, 10000),
        "flush timeout for multi-LBA interleaved entry"
    );
    flusher.stop();

    // Verify all 4 LBAs are mapped
    for lba in 10..14u64 {
        assert!(
            meta.get_mapping(&VolumeId("test-vol".into()), Lba(lba))
                .unwrap()
                .is_some(),
            "LBA {} should be mapped",
            lba
        );
    }

    // Verify dedup hits share PBAs with originals
    let pba_0 = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(0))
        .unwrap()
        .unwrap()
        .pba;
    let pba_2 = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(2))
        .unwrap()
        .unwrap()
        .pba;
    let pba_10 = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(10))
        .unwrap()
        .unwrap()
        .pba;
    let pba_12 = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(12))
        .unwrap()
        .unwrap()
        .pba;
    assert_eq!(pba_0, pba_10, "LBA 10 should dedup to LBA 0's PBA");
    assert_eq!(pba_2, pba_12, "LBA 12 should dedup to LBA 2's PBA");

    // Verify misses have their own PBAs
    let pba_11 = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(11))
        .unwrap()
        .unwrap()
        .pba;
    let pba_13 = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(13))
        .unwrap()
        .unwrap()
        .pba;
    assert_ne!(pba_11, pba_0);
    assert_ne!(pba_13, pba_0);

    // Verify no pending entries left (coalescer correctly tracked all seqs)
    assert_eq!(
        pool.pending_count(),
        0,
        "all entries should be fully flushed"
    );
}

#[test]
fn scanner_hit_remaps_skipped_block_and_clears_flag() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env_small_buffer();
    register_volume(&meta, "test-vol");

    let data = vec![0x5A; 4096];
    let hash: ContentHash = *blake3::hash(&data).as_bytes();

    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    pool.append("test-vol", Lba(0), 1, &data, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000), "initial flush timeout");
    flusher.stop();

    let original_pba = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(0))
        .unwrap()
        .unwrap()
        .pba;
    assert_eq!(
        meta.get_dedup_entry(&hash).unwrap().unwrap().pba,
        original_pba
    );

    let mut skip_flusher = start_flusher_custom(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        new_hole_map(),
        dedup_always_skip_config(),
    );
    pool.append("test-vol", Lba(1), 1, &data, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000), "skipped flush timeout");
    skip_flusher.stop();

    let skipped_mapping = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(1))
        .unwrap()
        .unwrap();
    assert_ne!(
        skipped_mapping.pba, original_pba,
        "skipped dedup write should land on a fresh PBA"
    );
    assert_eq!(skipped_mapping.flags, FLAG_DEDUP_SKIPPED);

    let mut scanner = start_scanner(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        dedup_scanner_config(90),
    );

    assert!(
        wait_until(3000, || {
            let mapping = meta
                .get_mapping(&VolumeId("test-vol".into()), Lba(1))
                .unwrap()
                .unwrap();
            mapping.flags == 0 && mapping.pba == original_pba
        }),
        "scanner should remap skipped duplicate to existing PBA"
    );
    scanner.stop();

    assert_eq!(meta.get_refcount(skipped_mapping.pba).unwrap(), 0);
    assert_eq!(
        meta.get_dedup_entry(&hash).unwrap().unwrap().pba,
        original_pba
    );
}

#[test]
fn scanner_miss_registers_index_and_clears_flag() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env_small_buffer();
    register_volume(&meta, "test-vol");

    let data = vec![0x6B; 4096];
    let hash: ContentHash = *blake3::hash(&data).as_bytes();

    let mut flusher = start_flusher_custom(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        new_hole_map(),
        dedup_always_skip_config(),
    );
    pool.append("test-vol", Lba(0), 1, &data, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000), "skipped flush timeout");
    flusher.stop();

    let skipped_mapping = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(0))
        .unwrap()
        .unwrap();
    assert_eq!(skipped_mapping.flags, FLAG_DEDUP_SKIPPED);
    assert!(meta.get_dedup_entry(&hash).unwrap().is_none());

    let mut scanner = start_scanner(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        dedup_scanner_config(90),
    );
    assert!(
        wait_until(3000, || {
            let mapping = meta
                .get_mapping(&VolumeId("test-vol".into()), Lba(0))
                .unwrap()
                .unwrap();
            mapping.flags == 0
                && meta
                    .get_dedup_entry(&hash)
                    .unwrap()
                    .map(|e| e.pba == skipped_mapping.pba)
                    .unwrap_or(false)
        }),
        "scanner should register index and clear skipped flag for unique block"
    );
    scanner.stop();
}

#[test]
fn scanner_skips_under_pressure_then_resumes() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env_small_buffer();
    register_volume(&meta, "test-vol");

    let skipped_data = vec![0x7C; 4096];
    let skipped_hash: ContentHash = *blake3::hash(&skipped_data).as_bytes();

    let mut skip_flusher = start_flusher_custom(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        new_hole_map(),
        dedup_always_skip_config(),
    );
    pool.append("test-vol", Lba(0), 1, &skipped_data, 1000)
        .unwrap();
    assert!(wait_flushed(&pool, 10000), "skipped flush timeout");
    skip_flusher.stop();

    let skipped_mapping = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(0))
        .unwrap()
        .unwrap();
    assert_eq!(skipped_mapping.flags, FLAG_DEDUP_SKIPPED);

    let filler = vec![0xEE; 4096];
    pool.append("test-vol", Lba(100), 1, &filler, 1000).unwrap();

    let mut scanner = start_scanner(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        dedup_scanner_config(0),
    );

    thread::sleep(Duration::from_millis(150));
    assert_eq!(
        meta.get_mapping(&VolumeId("test-vol".into()), Lba(0))
            .unwrap()
            .unwrap()
            .flags,
        FLAG_DEDUP_SKIPPED,
        "scanner must skip rescans while buffer pressure is above threshold"
    );
    assert!(meta.get_dedup_entry(&skipped_hash).unwrap().is_none());

    let mut drain_flusher =
        start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    assert!(wait_flushed(&pool, 10000), "filler drain timeout");
    drain_flusher.stop();

    assert!(
        wait_until(3000, || {
            let mapping = meta
                .get_mapping(&VolumeId("test-vol".into()), Lba(0))
                .unwrap()
                .unwrap();
            mapping.flags == 0 && meta.get_dedup_entry(&skipped_hash).unwrap().is_some()
        }),
        "scanner should resume once buffer pressure is relieved"
    );
    scanner.stop();
}

#[test]
fn scanner_crc_mismatch_leaves_block_skipped() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env_small_buffer();
    register_volume(&meta, "test-vol");

    let data = vec![0x8D; 4096];
    let hash: ContentHash = *blake3::hash(&data).as_bytes();

    let mut flusher = start_flusher_custom(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        new_hole_map(),
        dedup_always_skip_config(),
    );
    pool.append("test-vol", Lba(0), 1, &data, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000), "skipped flush timeout");
    flusher.stop();

    let mapping = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(0))
        .unwrap()
        .unwrap();
    assert_eq!(mapping.flags, FLAG_DEDUP_SKIPPED);

    io_engine
        .write_blocks(mapping.pba, &vec![0xFF; 4096])
        .unwrap();

    let mut scanner = start_scanner(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        dedup_scanner_config(90),
    );
    thread::sleep(Duration::from_millis(150));
    scanner.stop();

    let after = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(0))
        .unwrap()
        .unwrap();
    assert_eq!(after.flags, FLAG_DEDUP_SKIPPED);
    assert!(meta.get_dedup_entry(&hash).unwrap().is_none());
}

#[test]
fn dedup_hole_fill_miss_populates_index() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env();
    let hole_map = new_hole_map();
    register_volume_with(&meta, "anchor", CompressionAlgo::Lz4, 100);
    register_volume_with(&meta, "fill", CompressionAlgo::Lz4, 200);

    let mut flusher = start_flusher_custom(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        hole_map.clone(),
        dedup_test_config(),
    );

    let anchor_data = vec![0x11; 4096];
    pool.append("anchor", Lba(0), 1, &anchor_data, 100).unwrap();
    assert!(wait_flushed(&pool, 10000), "anchor flush timeout");
    let anchor_mapping = meta
        .get_mapping(&VolumeId("anchor".into()), Lba(0))
        .unwrap()
        .unwrap();

    let hole_offset = anchor_mapping.slot_offset + anchor_mapping.unit_compressed_size as u16;
    let hole_size = 1024u16;
    assert!(hole_offset + hole_size <= BLOCK_SIZE as u16);
    hole_map.lock().unwrap().insert(
        HoleKey {
            pba: anchor_mapping.pba,
            offset: hole_offset,
        },
        hole_size,
    );

    let fill_data = vec![0x22; 4096];
    let fill_hash: ContentHash = *blake3::hash(&fill_data).as_bytes();
    pool.append("fill", Lba(0), 1, &fill_data, 200).unwrap();
    assert!(wait_flushed(&pool, 10000), "hole fill flush timeout");
    flusher.stop();

    let fill_mapping = meta
        .get_mapping(&VolumeId("fill".into()), Lba(0))
        .unwrap()
        .unwrap();
    assert_eq!(
        fill_mapping.pba, anchor_mapping.pba,
        "write should reuse the manual hole"
    );
    assert_eq!(fill_mapping.slot_offset, hole_offset);
    assert_eq!(fill_mapping.flags, 0);

    let entry = meta.get_dedup_entry(&fill_hash).unwrap().unwrap();
    assert_eq!(entry.pba, fill_mapping.pba);
    assert_eq!(entry.slot_offset, fill_mapping.slot_offset);
}

#[test]
fn dedup_skipped_hole_fill_is_rescanned() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env_small_buffer();
    let hole_map = new_hole_map();
    register_volume_with(&meta, "anchor", CompressionAlgo::Lz4, 100);
    register_volume_with(&meta, "fill", CompressionAlgo::Lz4, 200);

    let mut anchor_flusher = start_flusher_custom(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        hole_map.clone(),
        dedup_test_config(),
    );
    let anchor_data = vec![0x33; 4096];
    pool.append("anchor", Lba(0), 1, &anchor_data, 100).unwrap();
    assert!(wait_flushed(&pool, 10000), "anchor flush timeout");
    anchor_flusher.stop();

    let anchor_mapping = meta
        .get_mapping(&VolumeId("anchor".into()), Lba(0))
        .unwrap()
        .unwrap();
    let hole_offset = anchor_mapping.slot_offset + anchor_mapping.unit_compressed_size as u16;
    let hole_size = 1024u16;
    assert!(hole_offset + hole_size <= BLOCK_SIZE as u16);
    hole_map.lock().unwrap().insert(
        HoleKey {
            pba: anchor_mapping.pba,
            offset: hole_offset,
        },
        hole_size,
    );

    let fill_data = vec![0x44; 4096];
    let fill_hash: ContentHash = *blake3::hash(&fill_data).as_bytes();
    let mut skip_flusher = start_flusher_custom(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        hole_map.clone(),
        dedup_always_skip_config(),
    );
    pool.append("fill", Lba(0), 1, &fill_data, 200).unwrap();
    assert!(wait_flushed(&pool, 10000), "skipped hole fill timeout");
    skip_flusher.stop();

    let skipped_mapping = meta
        .get_mapping(&VolumeId("fill".into()), Lba(0))
        .unwrap()
        .unwrap();
    assert_eq!(skipped_mapping.pba, anchor_mapping.pba);
    assert_eq!(skipped_mapping.slot_offset, hole_offset);
    assert_eq!(skipped_mapping.flags, FLAG_DEDUP_SKIPPED);
    assert!(meta.get_dedup_entry(&fill_hash).unwrap().is_none());

    let mut scanner = start_scanner(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        dedup_scanner_config(90),
    );
    assert!(
        wait_until(3000, || {
            let mapping = meta
                .get_mapping(&VolumeId("fill".into()), Lba(0))
                .unwrap()
                .unwrap();
            mapping.flags == 0
                && meta
                    .get_dedup_entry(&fill_hash)
                    .unwrap()
                    .map(|e| e.pba == mapping.pba && e.slot_offset == mapping.slot_offset)
                    .unwrap_or(false)
        }),
        "scanner should rescan skipped hole-fill writes and register dedup metadata"
    );
    scanner.stop();
}

#[test]
fn dedup_scanner_free_purges_holes_for_released_pba() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env();
    let hole_map = new_hole_map();
    register_volume_with(&meta, "live", CompressionAlgo::None, 100);
    register_volume_with(&meta, "skip", CompressionAlgo::Lz4, 200);

    let block = vec![0x5A; BLOCK_SIZE as usize];
    let hash: ContentHash = *blake3::hash(&block).as_bytes();

    let live_pba = allocator.allocate_one().unwrap();
    io_engine.write_blocks(live_pba, &block).unwrap();
    let live_mapping = BlockmapValue {
        pba: live_pba,
        compression: 0,
        unit_compressed_size: BLOCK_SIZE,
        unit_original_size: BLOCK_SIZE,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: crc32fast::hash(&block),
        slot_offset: 0,
        flags: 0,
    };
    meta.atomic_write_mapping(&VolumeId("live".into()), Lba(0), &live_mapping)
        .unwrap();
    meta.put_dedup_entries(&[(
        hash,
        DedupEntry {
            pba: live_pba,
            slot_offset: 0,
            compression: 0,
            unit_compressed_size: BLOCK_SIZE,
            unit_original_size: BLOCK_SIZE,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: live_mapping.crc32,
        },
    )])
    .unwrap();

    let old_pba = allocator.allocate_one().unwrap();
    let compressor = create_compressor(CompressionAlgo::Lz4);
    let mut buf = vec![0u8; compressor.max_compressed_size(block.len())];
    let compressed_len = compressor.compress(&block, &mut buf).unwrap();
    let compressed = buf[..compressed_len].to_vec();
    let crc = crc32fast::hash(&compressed);
    let slot_offset = 512u16;
    let mut slot = vec![0u8; BLOCK_SIZE as usize];
    slot[slot_offset as usize..slot_offset as usize + compressed.len()]
        .copy_from_slice(&compressed);
    io_engine.write_blocks(old_pba, &slot).unwrap();
    meta.put_mapping(
        &VolumeId("skip".into()),
        Lba(0),
        &BlockmapValue {
            pba: old_pba,
            compression: 1,
            unit_compressed_size: compressed.len() as u32,
            unit_original_size: BLOCK_SIZE,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: crc,
            slot_offset,
            flags: FLAG_DEDUP_SKIPPED,
        },
    )
    .unwrap();
    meta.set_refcount(old_pba, 1).unwrap();

    hole_map.lock().unwrap().insert(
        HoleKey {
            pba: old_pba,
            offset: 0,
        },
        slot_offset,
    );
    assert!(
        hole_map.lock().unwrap().keys().any(|k| k.pba == old_pba),
        "fixture must install a stale hole for the soon-to-be-freed PBA"
    );

    let mut scanner = start_scanner_with_hole_map(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        hole_map.clone(),
        dedup_scanner_config(90),
    );
    assert!(
        wait_until(3000, || {
            meta.get_mapping(&VolumeId("skip".into()), Lba(0))
                .unwrap()
                .map(|m| m.pba == live_pba && m.flags == 0)
                .unwrap_or(false)
        }),
        "scanner should remap skipped block to existing live dedup target"
    );
    scanner.stop();

    assert_eq!(meta.get_refcount(old_pba).unwrap(), 0);
    assert!(
        !hole_map.lock().unwrap().keys().any(|k| k.pba == old_pba),
        "scanner cleanup must purge stale holes for a fully freed PBA"
    );
}

#[test]
fn dedup_live_reference_prevents_hole_publication_and_reuse() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env();
    let hole_map = new_hole_map();
    register_volume_with(&meta, "anchor", CompressionAlgo::Lz4, 100);
    register_volume_with(&meta, "peer", CompressionAlgo::Lz4, 200);
    register_volume_with(&meta, "dup", CompressionAlgo::Lz4, 300);
    register_volume_with(&meta, "fill", CompressionAlgo::Lz4, 400);

    let mut flusher = start_flusher_custom(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        hole_map.clone(),
        dedup_test_config(),
    );

    let anchor_data = vec![0x31; BLOCK_SIZE as usize];
    let peer_data = vec![0x52; BLOCK_SIZE as usize];
    let anchor_new = vec![0x73; BLOCK_SIZE as usize];
    let fill_data = vec![0x94; BLOCK_SIZE as usize];

    pool.append("anchor", Lba(0), 1, &anchor_data, 100).unwrap();
    pool.append("peer", Lba(0), 1, &peer_data, 200).unwrap();
    assert!(wait_flushed(&pool, 10000), "initial packed flush timeout");

    let anchor_mapping = meta
        .get_mapping(&VolumeId("anchor".into()), Lba(0))
        .unwrap()
        .unwrap();
    let peer_mapping = meta
        .get_mapping(&VolumeId("peer".into()), Lba(0))
        .unwrap()
        .unwrap();
    assert_eq!(
        anchor_mapping.pba, peer_mapping.pba,
        "anchor and peer should share a packed slot"
    );
    assert_ne!(
        anchor_mapping.slot_offset, peer_mapping.slot_offset,
        "packed fragments must occupy different byte ranges"
    );
    let shared_pba = anchor_mapping.pba;
    let anchor_hole = HoleKey {
        pba: shared_pba,
        offset: anchor_mapping.slot_offset,
    };

    pool.append("dup", Lba(0), 1, &anchor_data, 300).unwrap();
    assert!(wait_flushed(&pool, 10000), "dedup-hit flush timeout");

    let dup_mapping = meta
        .get_mapping(&VolumeId("dup".into()), Lba(0))
        .unwrap()
        .unwrap();
    assert_eq!(dup_mapping.pba, shared_pba);
    assert_eq!(dup_mapping.slot_offset, anchor_mapping.slot_offset);
    assert_eq!(dup_mapping.crc32, anchor_mapping.crc32);

    pool.append("anchor", Lba(0), 1, &anchor_new, 100).unwrap();
    assert!(wait_flushed(&pool, 10000), "anchor overwrite flush timeout");

    let anchor_new_mapping = meta
        .get_mapping(&VolumeId("anchor".into()), Lba(0))
        .unwrap()
        .unwrap();
    assert_ne!(
        anchor_new_mapping.pba, shared_pba,
        "overwrite should move anchor to a new location"
    );
    assert_eq!(
        meta.get_mapping(&VolumeId("dup".into()), Lba(0))
            .unwrap()
            .unwrap()
            .pba,
        shared_pba,
        "dedup reference must still point at the original fragment"
    );
    assert!(
        !hole_map.lock().unwrap().contains_key(&anchor_hole),
        "live dedup refs must prevent publishing a hole for the old fragment"
    );

    pool.append("fill", Lba(0), 1, &fill_data, 400).unwrap();
    assert!(wait_flushed(&pool, 10000), "fill flush timeout");

    let fill_mapping = meta
        .get_mapping(&VolumeId("fill".into()), Lba(0))
        .unwrap()
        .unwrap();
    assert!(
        fill_mapping.pba != shared_pba || fill_mapping.slot_offset != anchor_mapping.slot_offset,
        "hole fill must not reuse bytes still referenced through dedup"
    );

    let worker = ZoneWorker::new(ZoneId(0), meta.clone(), pool.clone(), io_engine.clone());
    assert_eq!(
        worker.handle_read("anchor", Lba(0)).unwrap().unwrap(),
        anchor_new
    );
    assert_eq!(
        worker.handle_read("peer", Lba(0)).unwrap().unwrap(),
        peer_data
    );
    assert_eq!(
        worker.handle_read("dup", Lba(0)).unwrap().unwrap(),
        anchor_data
    );

    flusher.stop();
}

#[test]
fn dedup_worker_ignores_stale_entry_for_reused_pba() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env();
    register_volume(&meta, "live");
    register_volume(&meta, "dup");

    let stale_block = vec![0xA5; BLOCK_SIZE as usize];
    let live_block = vec![0x5A; BLOCK_SIZE as usize];
    let stale_hash: ContentHash = *blake3::hash(&stale_block).as_bytes();

    let reused_pba = allocator.allocate_one().unwrap();
    io_engine.write_blocks(reused_pba, &live_block).unwrap();

    let live_mapping = BlockmapValue {
        pba: reused_pba,
        compression: 0,
        unit_compressed_size: BLOCK_SIZE,
        unit_original_size: BLOCK_SIZE,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: crc32fast::hash(&live_block),
        slot_offset: 0,
        flags: 0,
    };
    meta.atomic_write_mapping(&VolumeId("live".into()), Lba(0), &live_mapping)
        .unwrap();

    let stale_entry = DedupEntry {
        pba: reused_pba,
        slot_offset: 0,
        compression: 0,
        unit_compressed_size: BLOCK_SIZE,
        unit_original_size: BLOCK_SIZE,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: crc32fast::hash(&stale_block),
    };
    meta.put_dedup_entries(&[(stale_hash, stale_entry)])
        .unwrap();
    assert!(
        !meta
            .has_live_fragment_ref(&stale_entry.to_blockmap_value())
            .unwrap(),
        "test fixture must use a stale dedup entry"
    );

    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    pool.append("dup", Lba(0), 1, &stale_block, 1000).unwrap();
    assert!(
        wait_flushed(&pool, 10000),
        "flush timeout for stale dedup entry workload"
    );

    let worker = ZoneWorker::new(ZoneId(0), meta.clone(), pool.clone(), io_engine.clone());
    assert_eq!(
        worker.handle_read("dup", Lba(0)).unwrap().unwrap(),
        stale_block,
        "stale dedup entries on reused PBAs must be rejected and rewritten"
    );

    let dup_mapping = meta
        .get_mapping(&VolumeId("dup".into()), Lba(0))
        .unwrap()
        .unwrap();
    assert_ne!(
        dup_mapping.pba, reused_pba,
        "stale dedup entry must not be used as a hit target"
    );

    let current_entry = meta.get_dedup_entry(&stale_hash).unwrap().unwrap();
    assert_eq!(
        current_entry.pba, dup_mapping.pba,
        "dedup index should be refreshed to the newly written live fragment"
    );

    flusher.stop();
}

#[test]
fn dedup_miss_before_meta_write_recovers_and_populates_index() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env();
    register_volume(&meta, "test-vol-meta-fail");

    let data = vec![0x91; 4096];
    let hash: ContentHash = *blake3::hash(&data).as_bytes();
    install_test_failpoint(
        "test-vol-meta-fail",
        Lba(0),
        FlushFailStage::BeforeMetaWrite,
        Some(1),
    );

    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    pool.append("test-vol-meta-fail", Lba(0), 1, &data, 1000)
        .unwrap();
    assert!(
        wait_flushed(&pool, 10000),
        "write should retry and eventually flush after metadata failpoint"
    );
    flusher.stop();
    clear_test_failpoint(
        "test-vol-meta-fail",
        Lba(0),
        FlushFailStage::BeforeMetaWrite,
    );

    let mapping = meta
        .get_mapping(&VolumeId("test-vol-meta-fail".into()), Lba(0))
        .unwrap()
        .unwrap();
    assert_eq!(mapping.flags, 0);
    assert_eq!(meta.get_refcount(mapping.pba).unwrap(), 1);
    assert_eq!(
        meta.get_dedup_entry(&hash).unwrap().unwrap().pba,
        mapping.pba
    );
}

#[test]
fn dedup_hit_failure_demotes_to_miss() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env();
    register_volume(&meta, "test-vol-hit-fail");

    let data = vec![0xA5; 4096];
    let hash: ContentHash = *blake3::hash(&data).as_bytes();

    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    pool.append("test-vol-hit-fail", Lba(0), 1, &data, 1000)
        .unwrap();
    assert!(wait_flushed(&pool, 10000), "initial flush timeout");

    let original_mapping = meta
        .get_mapping(&VolumeId("test-vol-hit-fail".into()), Lba(0))
        .unwrap()
        .unwrap();

    install_test_dedup_hit_failpoint("test-vol-hit-fail", Lba(1), Some(1));
    pool.append("test-vol-hit-fail", Lba(1), 1, &data, 1000)
        .unwrap();
    assert!(wait_flushed(&pool, 10000), "demoted miss flush timeout");
    flusher.stop();
    clear_test_dedup_hit_failpoint("test-vol-hit-fail", Lba(1));

    let second_mapping = meta
        .get_mapping(&VolumeId("test-vol-hit-fail".into()), Lba(1))
        .unwrap()
        .unwrap();
    assert_ne!(
        second_mapping.pba, original_mapping.pba,
        "forced dedup-hit failure should demote the write to a fresh miss allocation"
    );
    assert_eq!(meta.get_refcount(original_mapping.pba).unwrap(), 1);
    assert_eq!(meta.get_refcount(second_mapping.pba).unwrap(), 1);
    assert_eq!(
        meta.get_dedup_entry(&hash).unwrap().unwrap().pba,
        second_mapping.pba
    );
}
