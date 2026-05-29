use std::sync::Arc;
use std::thread;
use std::time::Duration;

use onyx_storage::buffer::flush::{
    clear_test_dedup_hit_failpoint, clear_test_failpoint, install_test_dedup_hit_failpoint,
    install_test_failpoint, BufferFlusher, FlushFailStage,
};
use onyx_storage::buffer::pool::WriteBufferPool;
use onyx_storage::config::FlushConfig;
use onyx_storage::dedup::config::DedupConfig;
use onyx_storage::dedup::scanner::DedupScanner;
use onyx_storage::io::device::RawDevice;
use onyx_storage::io::engine::IoEngine;
use onyx_storage::io::read_pool::ReadPool;
use onyx_storage::lifecycle::VolumeLifecycleManager;
use onyx_storage::meta::schema::*;
use onyx_storage::meta::store::MetaStore;
use onyx_storage::space::allocator::SpaceAllocator;
use onyx_storage::types::*;
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
        path: Some(meta_dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 0,
        index_pin_mb: 0,
        lsm_bloom_bits_per_entry: 10,
        checkpoint_interval_ms: 5000,
        group_commit_timeout_us: 1,
        wal_dir: None,
        dedup_shards: 8,
        dedup_cuckoo_buckets: 1_000_000,
        dedup_l1_cache_entries: 256_000,
        ..onyx_storage::config::MetaConfig::default()
    };
    let meta = Arc::new(MetaStore::open(&meta_config).unwrap());

    let buf_dev = RawDevice::open(buf_tmp.path()).unwrap();
    let pool = Arc::new(WriteBufferPool::open(buf_dev).unwrap());
    // Simulate the engine's durability-watermark thread so mark_flushed'd
    // seqs actually release their ring slots. Without this the flusher's
    // mark_flushed would run but reclaim_log_prefix would stall on
    // durable_seq=0, buffer fill% would never drop, and the scanner
    // pressure-relief path would never see a green signal.
    pool.durable_seq_handle()
        .store(u64::MAX, std::sync::atomic::Ordering::Release);

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

/// Variant of [`setup_dedup_env`] that also returns a `ReadPool` and
/// the underlying data file path. Used by cold-tail tests: the scanner
/// only runs the cold-tail pass when a `ReadPool` is configured (the
/// pass batches LV3 reads through io_uring, so trying to do the
/// equivalent serially would dwarf the cycle budget).
fn setup_dedup_env_with_read_pool() -> (
    Arc<WriteBufferPool>,
    Arc<MetaStore>,
    Arc<VolumeLifecycleManager>,
    Arc<SpaceAllocator>,
    Arc<IoEngine>,
    Arc<ReadPool>,
) {
    let buf_bytes: u64 = 1024 * 1024;
    let data_bytes: u64 = 16 * 1024 * 1024;
    let meta_dir = tempdir().unwrap();
    let buf_tmp = NamedTempFile::new().unwrap();
    let data_tmp = NamedTempFile::new().unwrap();
    buf_tmp.as_file().set_len(buf_bytes).unwrap();
    data_tmp.as_file().set_len(data_bytes).unwrap();

    let meta_config = onyx_storage::config::MetaConfig {
        path: Some(meta_dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 0,
        index_pin_mb: 0,
        lsm_bloom_bits_per_entry: 10,
        checkpoint_interval_ms: 5000,
        group_commit_timeout_us: 1,
        wal_dir: None,
        dedup_shards: 8,
        dedup_cuckoo_buckets: 1_000_000,
        dedup_l1_cache_entries: 256_000,
        ..onyx_storage::config::MetaConfig::default()
    };
    let meta = Arc::new(MetaStore::open(&meta_config).unwrap());

    let buf_dev = RawDevice::open(buf_tmp.path()).unwrap();
    let pool = Arc::new(WriteBufferPool::open(buf_dev).unwrap());
    pool.durable_seq_handle()
        .store(u64::MAX, std::sync::atomic::Ordering::Release);

    let data_dev = RawDevice::open(data_tmp.path()).unwrap();
    let io_engine = Arc::new(IoEngine::new(data_dev, false));
    let allocator = Arc::new(SpaceAllocator::new(16 * 1024 * 1024, 0));
    let lifecycle = Arc::new(VolumeLifecycleManager::default());

    let metrics = Arc::new(onyx_storage::metrics::EngineMetrics::default());
    let pool_dev = RawDevice::open(data_tmp.path()).unwrap();
    let read_pool = Arc::new(
        ReadPool::start(
            2,
            32,
            &pool_dev,
            onyx_storage::types::RESERVED_BLOCKS,
            BLOCK_SIZE,
            false,
            metrics,
        )
        .unwrap(),
    );
    drop(pool_dev);

    std::mem::forget(meta_dir);
    std::mem::forget(buf_tmp);
    std::mem::forget(data_tmp);

    (pool, meta, lifecycle, allocator, io_engine, read_pool)
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
        dedup_test_config(),
    )
}

fn start_flusher_custom(
    pool: &Arc<WriteBufferPool>,
    meta: &Arc<MetaStore>,
    lifecycle: &Arc<VolumeLifecycleManager>,
    allocator: &Arc<SpaceAllocator>,
    io_engine: &Arc<IoEngine>,
    dedup_config: DedupConfig,
) -> BufferFlusher {
    BufferFlusher::start(
        pool.clone(),
        meta.clone(),
        lifecycle.clone(),
        allocator.clone(),
        io_engine.clone(),
        &FlushConfig::default(),
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
    let (scanner, _candidate) =
        start_scanner_with_candidate(pool, meta, lifecycle, allocator, io_engine, config);
    scanner
}

fn start_scanner_with_candidate(
    pool: &Arc<WriteBufferPool>,
    meta: &Arc<MetaStore>,
    lifecycle: &Arc<VolumeLifecycleManager>,
    allocator: &Arc<SpaceAllocator>,
    io_engine: &Arc<IoEngine>,
    config: DedupConfig,
) -> (DedupScanner, onyx_storage::dedup::CandidateCache) {
    start_scanner_with_candidate_and_read_pool(
        pool, meta, lifecycle, allocator, io_engine, None, config,
    )
}

fn start_scanner_with_candidate_and_read_pool(
    pool: &Arc<WriteBufferPool>,
    meta: &Arc<MetaStore>,
    lifecycle: &Arc<VolumeLifecycleManager>,
    allocator: &Arc<SpaceAllocator>,
    io_engine: &Arc<IoEngine>,
    read_pool: Option<Arc<ReadPool>>,
    config: DedupConfig,
) -> (DedupScanner, onyx_storage::dedup::CandidateCache) {
    let candidate = onyx_storage::dedup::CandidateCache::new(8, 64);
    let scanner = DedupScanner::start(
        meta.clone(),
        io_engine.clone(),
        allocator.clone(),
        lifecycle.clone(),
        pool.clone(),
        candidate.clone(),
        read_pool,
        config,
    );
    (scanner, candidate)
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
        path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 0,
        index_pin_mb: 0,
        lsm_bloom_bits_per_entry: 10,
        checkpoint_interval_ms: 5000,
        group_commit_timeout_us: 1,
        wal_dir: None,
        dedup_shards: 8,
        dedup_cuckoo_buckets: 1_000_000,
        dedup_l1_cache_entries: 256_000,
        ..onyx_storage::config::MetaConfig::default()
    };
    let store = MetaStore::open(&config).unwrap();

    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(b"hello world");
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
fn scan_dedup_skipped() {
    let dir = tempdir().unwrap();
    let config = onyx_storage::config::MetaConfig {
        path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 0,
        index_pin_mb: 0,
        lsm_bloom_bits_per_entry: 10,
        checkpoint_interval_ms: 5000,
        group_commit_timeout_us: 1,
        wal_dir: None,
        dedup_shards: 8,
        dedup_cuckoo_buckets: 1_000_000,
        dedup_l1_cache_entries: 256_000,
        ..onyx_storage::config::MetaConfig::default()
    };
    let store = MetaStore::open(&config).unwrap();
    store.create_blockmap_cf("test-vol").unwrap();
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
        path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 0,
        index_pin_mb: 0,
        lsm_bloom_bits_per_entry: 10,
        checkpoint_interval_ms: 5000,
        group_commit_timeout_us: 1,
        wal_dir: None,
        dedup_shards: 8,
        dedup_cuckoo_buckets: 1_000_000,
        dedup_l1_cache_entries: 256_000,
        ..onyx_storage::config::MetaConfig::default()
    };
    let store = MetaStore::open(&config).unwrap();
    store.create_blockmap_cf("test-vol").unwrap();
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
    assert_eq!(config.pending_skip_threshold_entries, 0);
    assert_eq!(config.rescan_interval_ms, 30000);
    assert_eq!(config.max_rescan_per_cycle, 256);
}

// --- Integration: flusher with dedup enabled ---

#[test]
fn dedup_miss_does_not_populate_index() {
    // Promote-on-verified-hit invariant: a single fresh write must NOT
    // publish to the persistent dedup_index. The first occurrence of a
    // fingerprint lives only in the in-memory candidate cache; the
    // index gets populated on the *second* sighting after LV3 verify.
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env();
    register_volume(&meta, "test-vol");

    let data = vec![0xAA; 4096];
    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(&data);
    pool.append("test-vol", Lba(0), 1, &data, 1000).unwrap();

    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    assert!(wait_flushed(&pool, 10000), "flush timeout");
    flusher.stop();

    let mapping = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(0))
        .unwrap()
        .unwrap();
    assert_eq!(mapping.flags, 0);

    assert!(
        meta.get_dedup_entry(&hash).unwrap().is_none(),
        "fresh write must not publish to dedup_index until a duplicate verifies"
    );
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

    // Phase 5: hot-path L2pRemap is rc-neutral. The dedup-promote
    // tx's DedupPut bumps rc(first_pba) 0→1 ("shared via
    // dedup_index" reference); the L2pRemap of the second LBA does
    // not add another rc. Pre-Phase-5 the L2pRemap also incref'd, so
    // this assertion was `rc == 2`.
    let rc = meta.get_refcount(first_pba).unwrap();
    assert_eq!(rc, 1, "Phase 5: dedup_index entry = 1 rc; L2pRemap rc-neutral");
}

#[test]
fn overwrite_shared_dedup_pba_keeps_forward_index() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env();
    register_volume(&meta, "test-vol");

    let data = vec![0xB7; 4096];
    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(&data);
    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);

    pool.append("test-vol", Lba(0), 1, &data, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000), "initial flush timeout");
    pool.append("test-vol", Lba(1), 1, &data, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000), "dedup promote flush timeout");

    let before = meta
        .get_dedup_entry(&hash)
        .unwrap()
        .expect("verified duplicate should promote dedup index");
    // Phase 5: DedupPut for the verified duplicate bumps rc(before.pba)
    // 0→1 ("shared via dedup_index" reference). The L2pRemap of LBA 1
    // is rc-neutral. Pre-Phase 5 the per-write incref made this rc=2.
    assert_eq!(meta.get_refcount(before.pba).unwrap(), 1);

    let replacement = vec![0x42; 4096];
    pool.append("test-vol", Lba(0), 1, &replacement, 1000)
        .unwrap();
    assert!(wait_flushed(&pool, 10000), "overwrite flush timeout");
    flusher.stop();

    // Phase 5: overwriting LBA 0 is rc-neutral at the L2pRemap layer;
    // the dedup_index entry for the original hash still exists with
    // rc=1 because no FreePbas / cleanup retired the source PBA yet
    // (LBA 1 still references it).
    assert_eq!(
        meta.get_refcount(before.pba).unwrap(),
        1,
        "dedup_index entry keeps rc=1; L2pRemap rc-neutral"
    );
    assert_eq!(
        meta.get_mapping(&VolumeId("test-vol".into()), Lba(1))
            .unwrap()
            .unwrap(),
        before.to_blockmap_value(),
        "the second LBA should still point at the canonical dedup mapping"
    );
    assert_eq!(
        meta.get_dedup_entry(&hash).unwrap(),
        Some(before),
        "cleanup must not delete forward dedup_index while shared PBA is still live"
    );
}

#[test]
fn delete_volume_leaves_dedup_index_hint_for_scrub() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env();
    register_volume(&meta, "test-vol");

    let data = vec![0xCC; 4096];
    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(&data);
    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);

    pool.append("test-vol", Lba(0), 1, &data, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000), "initial flush timeout");
    pool.append("test-vol", Lba(1), 1, &data, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000), "dedup promote flush timeout");

    assert!(
        meta.get_dedup_entry(&hash).unwrap().is_some(),
        "verified duplicate should promote the forward dedup entry"
    );

    let cleanups = meta
        .delete_volume(&VolumeId("test-vol".into()))
        .expect("delete volume should return old mappings for cleanup");
    flusher.cleanup_mappings_now(&allocator, &cleanups, "test_delete_volume_cleanup");
    flusher.stop();

    assert!(
        meta.get_dedup_entry(&hash).unwrap().is_some(),
        "delete-volume cleanup must not read old PBA content to infer the hash; stale hints are scrubbed separately"
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
    // Phase 5: hot-path L2pRemap is rc-neutral; fresh miss leaves
    // rc(first_pba)=0 until a promote bumps it.
    assert_eq!(meta.get_refcount(first_pba).unwrap(), 0);

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

    // Phase 5: rc tracks dedup_index entries (and PromotionChunks), not
    // per-LBA references. The first candidate-hit on this PBA emits one
    // DedupPut → rc(first_pba) = 1. Subsequent hits (whether through the
    // dedup_index path or via candidate cache for the same hash) are
    // de-duplicated by the dedup workers and do not generate additional
    // promotes. Pre-Phase-5 this assertion was `rc == 5` (one per LBA).
    //
    // The promote's DedupPut applies asynchronously on a metadb dedup
    // apply lane, so it can lag `flusher.stop()` under threads-on +
    // heavy concurrent test load. Poll until rc settles (bounded) before
    // asserting — a wrong/inflated rc still fails after the timeout, so
    // this tolerates apply latency without masking a real regression.
    wait_until(10000, || meta.get_refcount(first_pba).unwrap() == 1);
    let rc = meta.get_refcount(first_pba).unwrap();
    assert_eq!(
        rc, 1,
        "Phase 5: single dedup_index entry → rc=1; L2pRemap rc-neutral"
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
    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(&data);

    // Two writes of the same data force the verify-on-hit pipeline to
    // promote the (hash, blockmap) pair into the persistent
    // dedup_index. The scanner's hit path can then remap a later
    // skipped write onto that PBA. Without the second write the index
    // would stay empty under promote-on-verified-hit and the scanner
    // miss path would just warm the candidate cache.
    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    pool.append("test-vol", Lba(0), 1, &data, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000), "initial flush timeout");
    pool.append("test-vol", Lba(2), 1, &data, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000), "promote flush timeout");
    flusher.stop();

    let original_pba = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(0))
        .unwrap()
        .unwrap()
        .pba;
    assert_eq!(
        meta.get_dedup_entry(&hash).unwrap().unwrap().pba,
        original_pba,
        "second matching write should promote the fingerprint into dedup_index"
    );

    let mut skip_flusher = start_flusher_custom(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
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
fn scanner_miss_warms_candidate_and_clears_flag() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env_small_buffer();
    register_volume(&meta, "test-vol");

    let data = vec![0x6B; 4096];
    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(&data);

    let mut flusher = start_flusher_custom(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
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

    let (mut scanner, candidate) = start_scanner_with_candidate(
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
                && candidate
                    .lookup(&hash)
                    .map(|cached| cached.pba == skipped_mapping.pba)
                    .unwrap_or(false)
        }),
        "scanner should warm candidate cache and clear skipped flag for unique block"
    );
    scanner.stop();

    // Promote-on-verified-hit invariant: the persistent dedup_index
    // stays empty until a future duplicate verifies against this PBA.
    assert!(
        meta.get_dedup_entry(&hash).unwrap().is_none(),
        "scanner miss must not publish to dedup_index"
    );
}

#[test]
fn scanner_skips_under_pressure_then_resumes() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env_small_buffer();
    register_volume(&meta, "test-vol");

    let skipped_data = vec![0x7C; 4096];
    let skipped_hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(&skipped_data);

    let mut skip_flusher = start_flusher_custom(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
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

    let (mut scanner, candidate) = start_scanner_with_candidate(
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
    assert!(candidate.lookup(&skipped_hash).is_none());

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
            mapping.flags == 0 && candidate.lookup(&skipped_hash).is_some()
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
    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(&data);

    let mut flusher = start_flusher_custom(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
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
fn cold_tail_pass_warms_candidate_from_live_blockmap() {
    // Cold-tail rescan walks live (non-skipped) blockmap entries in
    // chunks and warms the candidate cache via batched io_uring reads.
    //
    // The scanner is a fresh process state — its candidate cache
    // starts empty, just like after an engine restart. Once the
    // cold-tail pass runs the cache must contain the fingerprint for
    // the live entry without any duplicate write driving it.
    let (pool, meta, lifecycle, allocator, io_engine, read_pool) = setup_dedup_env_with_read_pool();
    register_volume(&meta, "test-vol");

    let data = vec![0xC1; 4096];
    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(&data);

    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    pool.append("test-vol", Lba(0), 1, &data, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000), "flush timeout");
    flusher.stop();

    let mapping = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(0))
        .unwrap()
        .unwrap();

    let cold_cfg = DedupConfig {
        rescan_interval_ms: 20,
        max_rescan_per_cycle: 0, // No DEDUP_SKIPPED debt on this volume; cold-tail only.
        cold_tail_max_per_cycle: 64,
        index_scrub_max_per_cycle: 64,
        ..dedup_test_config()
    };
    let (mut scanner, candidate) = start_scanner_with_candidate_and_read_pool(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        Some(read_pool.clone()),
        cold_cfg,
    );

    assert!(
        wait_until(3000, || candidate
            .lookup(&hash)
            .map(|v| v.pba == mapping.pba)
            .unwrap_or(false)),
        "cold-tail pass should hash the live blockmap entry and warm the candidate cache"
    );
    scanner.stop();

    // Promote-on-verified-hit invariant: cold-tail warming never
    // publishes into the persistent dedup_index. The promote happens
    // later, only after a duplicate write byte-verifies against this
    // PBA.
    assert!(
        meta.get_dedup_entry(&hash).unwrap().is_none(),
        "cold-tail must not publish to dedup_index; promote stays gated on verified hits"
    );
}

#[test]
fn dedup_miss_recovers_after_meta_write_failure() {
    // Forced failure before the metadata write should be retried; the
    // recovered fresh write must commit a clean blockmap (flags=0,
    // refcount=1) and — under promote-on-verified-hit — leave the
    // dedup_index empty until a duplicate verifies.
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env();
    register_volume(&meta, "test-vol-meta-fail");

    let data = vec![0x91; 4096];
    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(&data);
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
    // Phase 5: fresh write is rc-neutral.
    assert_eq!(meta.get_refcount(mapping.pba).unwrap(), 0);
    assert!(
        meta.get_dedup_entry(&hash).unwrap().is_none(),
        "fresh write must not publish dedup_index until a duplicate verifies"
    );
}

#[test]
fn dedup_hit_failure_demotes_to_miss() {
    let (pool, meta, lifecycle, allocator, io_engine) = setup_dedup_env();
    register_volume(&meta, "test-vol-hit-fail");

    let data = vec![0xA5; 4096];
    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(&data);

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
    // Phase 5: both PBAs are fresh writes (no DedupPut, no promote);
    // hot-path L2pRemap is rc-neutral so rc stays at 0 for each.
    assert_eq!(meta.get_refcount(original_mapping.pba).unwrap(), 0);
    assert_eq!(meta.get_refcount(second_mapping.pba).unwrap(), 0);
    // Promote-on-verified-hit: the demoted write is a fresh miss, so
    // it warms the candidate cache rather than publishing into
    // dedup_index. Both fresh writes leave the persistent index empty.
    assert!(
        meta.get_dedup_entry(&hash).unwrap().is_none(),
        "demoted miss must not publish to dedup_index"
    );
}
