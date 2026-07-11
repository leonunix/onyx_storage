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

/// Register a small volume sized to an exact LBA count. Cold-tail tests
/// use this so a full random-phase lap covers the whole volume within
/// the wait window (the cold-tail walk starts at a random LBA, so a
/// large volume could take many cycles to reach LBA 0/1/2).
fn register_small_volume(meta: &MetaStore, name: &str, lba_count: u64) {
    meta.put_volume(&VolumeConfig {
        id: VolumeId(name.to_string()),
        size_bytes: lba_count * 4096,
        block_size: 4096,
        compression: CompressionAlgo::None,
        created_at: 1000,
        zone_count: 4,
    })
    .unwrap();
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
    start_scanner_with_candidate_read_pool_cold_rx(
        pool, meta, lifecycle, allocator, io_engine, read_pool, None, None, config,
    )
}

#[allow(clippy::too_many_arguments)]
fn start_scanner_with_candidate_read_pool_cold_rx(
    pool: &Arc<WriteBufferPool>,
    meta: &Arc<MetaStore>,
    lifecycle: &Arc<VolumeLifecycleManager>,
    allocator: &Arc<SpaceAllocator>,
    io_engine: &Arc<IoEngine>,
    read_pool: Option<Arc<ReadPool>>,
    cold_rx: Option<crossbeam_channel::Receiver<onyx_storage::dedup::ColdTailTarget>>,
    heat: Option<onyx_storage::gc::heatmap::HeatMap>,
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
        cold_rx,
        heat,
        None, // ref_bitmap: §6 region-mode helper; per-PBA tests use their own
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
    assert_eq!(
        rc, 1,
        "Phase 5: dedup_index entry = 1 rc; L2pRemap rc-neutral"
    );
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
    flusher.cleanup_mappings_now(&cleanups, "test_delete_volume_cleanup");
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
    // Small volume so a full random-phase cold-tail lap covers LBA 0
    // well within the wait window.
    register_small_volume(&meta, "test-vol", 64);

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
fn cold_tail_fold_drain_warms_candidate_from_channel() {
    // Stage-4 fold: with `cold_rx` wired, the dedup scanner does NOT run its
    // own `scan_blockmap_range` — it drains cold candidates fed (here, by the
    // test standing in for the GC heat walk) over the channel, then runs the
    // same read + hash + warm tail. Sending the live entry must warm the
    // candidate cache exactly like the legacy scan path, and must still NOT
    // publish to dedup_index (promote-on-verified-hit invariant).
    use crossbeam_channel::bounded;
    use onyx_storage::dedup::ColdTailTarget;

    let (pool, meta, lifecycle, allocator, io_engine, read_pool) = setup_dedup_env_with_read_pool();
    register_small_volume(&meta, "test-vol", 64);

    let data = vec![0xD7; 4096];
    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(&data);

    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    pool.append("test-vol", Lba(0), 1, &data, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000), "flush timeout");
    flusher.stop();

    let mapping = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(0))
        .unwrap()
        .unwrap();

    let (tx, rx) = bounded::<ColdTailTarget>(64);

    let cold_cfg = DedupConfig {
        rescan_interval_ms: 20,
        max_rescan_per_cycle: 0, // no DEDUP_SKIPPED debt — cold-tail drain only
        cold_tail_max_per_cycle: 64,
        index_scrub_max_per_cycle: 0,
        ..dedup_test_config()
    };
    let (mut scanner, candidate) = start_scanner_with_candidate_read_pool_cold_rx(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        Some(read_pool.clone()),
        Some(rx),
        None, // heat: §6 orphan reclaim off in cold-tail tests
        cold_cfg,
    );

    // Stand in for the GC heat-refresh walk: emit the live entry as a cold
    // candidate. (Re-send each loop so a drop on an early empty cycle can't
    // wedge the test.)
    assert!(
        wait_until(3000, || {
            let _ = tx.try_send(ColdTailTarget {
                vol_id: VolumeId("test-vol".into()),
                lba: Lba(0),
                bv: mapping,
            });
            candidate
                .lookup(&hash)
                .map(|v| v.pba == mapping.pba)
                .unwrap_or(false)
        }),
        "fold drain should hash the channel-fed live entry and warm the candidate cache"
    );
    scanner.stop();

    assert!(
        meta.get_dedup_entry(&hash).unwrap().is_none(),
        "fold drain must not publish to dedup_index; promote stays gated on verified hits"
    );
}

#[test]
fn cold_tail_fold_drain_skips_stale_mapping_before_lv3_read() {
    use crossbeam_channel::bounded;
    use onyx_storage::dedup::ColdTailTarget;

    let (pool, meta, lifecycle, allocator, io_engine, read_pool) = setup_dedup_env_with_read_pool();
    register_small_volume(&meta, "test-vol", 64);

    let first = vec![0x31; 4096];
    let second = vec![0x72; 4096];
    let disabled = DedupConfig {
        enabled: false,
        ..dedup_test_config()
    };
    let mut flusher = start_flusher_custom(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        disabled.clone(),
    );
    pool.append("test-vol", Lba(0), 1, &first, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000), "initial flush timeout");
    let stale = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(0))
        .unwrap()
        .unwrap();

    pool.append("test-vol", Lba(0), 1, &second, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000), "overwrite flush timeout");
    flusher.stop();
    assert_ne!(
        meta.get_mapping(&VolumeId("test-vol".into()), Lba(0))
            .unwrap()
            .unwrap()
            .pba,
        stale.pba
    );

    // Keep the old payload readable so the failure is not masked by CRC: without
    // pin-then-revalidate the scanner hashes it and warms a candidate for an
    // L2P target that no longer exists.
    io_engine.write_blocks(stale.pba, &first).unwrap();

    let (tx, rx) = bounded::<ColdTailTarget>(64);
    let cold_cfg = DedupConfig {
        enabled: true,
        rescan_interval_ms: 20,
        max_rescan_per_cycle: 0,
        cold_tail_max_per_cycle: 64,
        index_scrub_max_per_cycle: 0,
        ..dedup_test_config()
    };
    let (mut scanner, candidate) = start_scanner_with_candidate_read_pool_cold_rx(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        Some(read_pool),
        Some(rx),
        None,
        cold_cfg,
    );
    tx.send(ColdTailTarget {
        vol_id: VolumeId("test-vol".into()),
        lba: Lba(0),
        bv: stale,
    })
    .unwrap();
    thread::sleep(Duration::from_millis(200));
    scanner.stop();

    assert!(
        !candidate.has_pba(stale.pba),
        "a stale cold-tail target must be rejected before its recycled PBA is read"
    );
}

#[test]
fn cold_tail_fold_drain_skips_already_warm_target() {
    // The drain rejects targets whose PBA is already in the candidate cache
    // BEFORE spending a read on them (the cheap `has_pba` filter the consumer
    // owns). Pre-warming the candidate then feeding the same entry must leave
    // the cache entry untouched (no spurious re-insert / churn) and emit no
    // dedup_index entry.
    use crossbeam_channel::bounded;
    use onyx_storage::dedup::ColdTailTarget;

    let (pool, meta, lifecycle, allocator, io_engine, read_pool) = setup_dedup_env_with_read_pool();
    register_small_volume(&meta, "test-vol", 64);

    let data = vec![0xE3; 4096];
    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(&data);

    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    pool.append("test-vol", Lba(0), 1, &data, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000), "flush timeout");
    flusher.stop();

    let mapping = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(0))
        .unwrap()
        .unwrap();

    let (tx, rx) = bounded::<ColdTailTarget>(64);
    let cold_cfg = DedupConfig {
        rescan_interval_ms: 20,
        max_rescan_per_cycle: 0,
        cold_tail_max_per_cycle: 64,
        index_scrub_max_per_cycle: 0,
        ..dedup_test_config()
    };
    let (mut scanner, candidate) = start_scanner_with_candidate_read_pool_cold_rx(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        Some(read_pool.clone()),
        Some(rx),
        None, // heat: §6 orphan reclaim off in cold-tail tests
        cold_cfg,
    );

    // Pre-warm the candidate for this PBA so the drain must skip it.
    candidate.insert(hash, mapping);
    assert!(candidate.has_pba(mapping.pba));

    // Feed the (already-warm) entry for several cycles, then confirm the
    // cache still resolves the same mapping and dedup_index stays empty.
    for _ in 0..20 {
        let _ = tx.try_send(ColdTailTarget {
            vol_id: VolumeId("test-vol".into()),
            lba: Lba(0),
            bv: mapping,
        });
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    scanner.stop();

    assert!(
        candidate
            .lookup(&hash)
            .map(|v| v.pba == mapping.pba)
            .unwrap_or(false),
        "already-warm candidate entry must survive the drain unchanged"
    );
    assert!(
        meta.get_dedup_entry(&hash).unwrap().is_none(),
        "skipping an already-warm target must not touch dedup_index"
    );
}

#[test]
fn cold_tail_remaps_dedup_index_hit_and_decrefs_old() {
    // Cold-tail's backend safety-net role: an "evicted-window"
    // duplicate is a live block whose content was already promoted into
    // the persistent dedup_index, but whose candidate slot was evicted
    // before the duplicate write arrived — so it landed un-deduped on
    // its own fresh PBA (flags=0). The cold-tail pass must reclaim it by
    // remapping the LBA onto the existing dedup target and decref'ing
    // the orphaned old PBA, NOT merely warm the candidate cache (which
    // would only help a future write, leaving this block un-deduped).
    let (pool, meta, lifecycle, allocator, io_engine, read_pool) = setup_dedup_env_with_read_pool();
    // Small volume so a full random-phase cold-tail lap covers LBA 1
    // (the evicted-window duplicate) well within the wait window.
    register_small_volume(&meta, "test-vol", 64);

    let data = vec![0x5A; 4096];
    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(&data);

    // Promote the fingerprint into the persistent dedup_index via two
    // matching writes (promote-on-verified-hit). Lba(0) is the canonical
    // PBA P that the index points at.
    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    pool.append("test-vol", Lba(0), 1, &data, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000), "initial flush timeout");
    pool.append("test-vol", Lba(2), 1, &data, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000), "promote flush timeout");
    flusher.stop();

    let canonical_pba = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(0))
        .unwrap()
        .unwrap()
        .pba;
    assert_eq!(
        meta.get_dedup_entry(&hash).unwrap().unwrap().pba,
        canonical_pba,
        "second matching write should promote the fingerprint into dedup_index"
    );

    // Manufacture the evicted-window duplicate: write the same content
    // at Lba(1) with dedup *disabled* so the coalescer bypasses the
    // dedup workers entirely and lands a fresh PBA Q with flags=0 — an
    // un-deduped live block whose content hash is already in
    // dedup_index (Q != P). This is the state a promote-gate
    // racing-loser fallback (or a dedup toggle) leaves behind, and it
    // is NOT flagged FLAG_DEDUP_SKIPPED, so only the cold-tail pass can
    // reclaim it (the DEDUP_SKIPPED rescan never sees it).
    let mut fresh_flusher = start_flusher_custom(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        DedupConfig {
            enabled: false,
            ..dedup_test_config()
        },
    );
    pool.append("test-vol", Lba(1), 1, &data, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000), "fresh write flush timeout");
    fresh_flusher.stop();

    let dup_mapping = meta
        .get_mapping(&VolumeId("test-vol".into()), Lba(1))
        .unwrap()
        .unwrap();
    assert_ne!(
        dup_mapping.pba, canonical_pba,
        "dedup-disabled duplicate should land on a fresh PBA"
    );
    assert_eq!(
        dup_mapping.flags, 0,
        "dedup-disabled write is un-deduped but not FLAG_DEDUP_SKIPPED"
    );

    // Cold-tail-only scanner: no DEDUP_SKIPPED debt (max_rescan=0), no
    // index scrub, cold-tail enabled.
    let cold_cfg = DedupConfig {
        rescan_interval_ms: 20,
        max_rescan_per_cycle: 0,
        cold_tail_max_per_cycle: 64,
        index_scrub_max_per_cycle: 0,
        ..dedup_test_config()
    };
    let (mut scanner, _candidate) = start_scanner_with_candidate_and_read_pool(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        Some(read_pool.clone()),
        cold_cfg,
    );

    // The cold-tail pass remaps Lba(1) onto the canonical PBA and clears
    // any residual flags.
    assert!(
        wait_until(3000, || {
            let mapping = meta
                .get_mapping(&VolumeId("test-vol".into()), Lba(1))
                .unwrap()
                .unwrap();
            mapping.flags == 0 && mapping.pba == canonical_pba
        }),
        "cold-tail should remap the evicted-window duplicate onto the existing dedup PBA"
    );
    scanner.stop();

    // The orphaned fresh PBA Q is decref'd to zero, and the dedup_index
    // entry is unchanged (rc-neutral remap onto the canonical PBA).
    assert_eq!(
        meta.get_refcount(dup_mapping.pba).unwrap(),
        0,
        "remapped duplicate's old PBA should drop to refcount 0"
    );
    assert_eq!(
        meta.get_dedup_entry(&hash).unwrap().unwrap().pba,
        canonical_pba,
        "remap must not disturb the persistent dedup_index entry"
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

// ----- §6 orphan dedup-PBA reclaim -----

/// Promote H->P via two matching writes, returning the canonical PBA `P`.
fn promote_dedup_entry(
    pool: &Arc<WriteBufferPool>,
    meta: &Arc<MetaStore>,
    lifecycle: &Arc<VolumeLifecycleManager>,
    allocator: &Arc<SpaceAllocator>,
    io_engine: &Arc<IoEngine>,
    vol: &str,
    lba_a: u64,
    lba_b: u64,
    content: &[u8],
) -> Pba {
    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(content);
    let mut flusher = start_flusher_with_dedup(pool, meta, lifecycle, allocator, io_engine);
    pool.append(vol, Lba(lba_a), 1, content, 1000).unwrap();
    assert!(wait_flushed(pool, 10000), "first flush timeout");
    pool.append(vol, Lba(lba_b), 1, content, 1000).unwrap();
    assert!(wait_flushed(pool, 10000), "promote flush timeout");
    flusher.stop();
    meta.get_dedup_entry(&hash)
        .unwrap()
        .expect("second matching write should promote into dedup_index")
        .pba
}

/// Start a reclaim-only GC runner (no rewrite scan, no heat refresh) — just the
/// retired-extent confirm scan that frees rc==0, unreferenced PBAs.
fn start_reclaim_gc(
    meta: &Arc<MetaStore>,
    io_engine: &Arc<IoEngine>,
    pool: &Arc<WriteBufferPool>,
    lifecycle: &Arc<VolumeLifecycleManager>,
    allocator: &Arc<SpaceAllocator>,
) -> onyx_storage::gc::runner::GcRunner {
    let heat = onyx_storage::gc::heatmap::HeatMap::new(allocator.total_block_count(), 256);
    let cfg = onyx_storage::gc::config::GcConfig {
        enabled: false,
        heat_enabled: false,
        scan_interval_ms: 20,
        reclaim_grace_secs: 0, // tests need deterministic immediate reclaim
        ..Default::default()
    };
    onyx_storage::gc::runner::GcRunner::start_with_metrics(
        Arc::new(onyx_storage::metrics::EngineMetrics::default()),
        meta.clone(),
        io_engine.clone(),
        pool.clone(),
        lifecycle.clone(),
        allocator.clone(),
        heat,
        None, // ref_bitmap (Stage-5; off here)
        None, // cold_tx
        onyx_storage::space::pba_lifecycle::PbaLifecycle::new(
            allocator.clone(),
            onyx_storage::dedup::CandidateCache::new(1, 1),
            Arc::new(onyx_storage::metrics::EngineMetrics::default()),
        ),
        cfg,
    )
}

fn orphan_scanner_config() -> DedupConfig {
    DedupConfig {
        rescan_interval_ms: 20,
        max_rescan_per_cycle: 0,    // no DEDUP_SKIPPED debt
        cold_tail_max_per_cycle: 0, // no cold-tail
        index_scrub_max_per_cycle: 0,
        orphan_reclaim_enabled: true,
        orphan_reclaim_max_per_cycle: 256,
        orphan_reclaim_fresh_max_age: 1,
        orphan_reclaim_per_pba: false, // §6 region-mode tests (per-PBA default is now true)
        ..dedup_test_config()
    }
}

/// A converged heat map (epoch >= 2) for the hot-region test (then bump P).
fn converged_heat(allocator: &Arc<SpaceAllocator>) -> onyx_storage::gc::heatmap::HeatMap {
    let heat = onyx_storage::gc::heatmap::HeatMap::new(allocator.total_block_count(), 256);
    heat.advance_epoch(); // FIRST_EPOCH(1) -> 2 (>= convergence gate)
    heat
}

/// A heat map where P's region is STALE (was live, then not bumped for
/// > fresh_max_age(1) sweeps) — the overwrite-orphaned signature the orphan
/// selector demotes. (A never-scanned region is intentionally NOT demoted.)
fn stale_heat(allocator: &Arc<SpaceAllocator>, p: Pba) -> onyx_storage::gc::heatmap::HeatMap {
    let heat = onyx_storage::gc::heatmap::HeatMap::new(allocator.total_block_count(), 256);
    heat.advance_epoch(); // -> epoch 2
    heat.bump(p); // region (epoch 2, count 1) — was live
    heat.advance_epoch(); // -> 3
    heat.advance_epoch(); // -> 4  (age = 4 - 2 = 2 > fresh_max_age = 1)
    heat
}

#[test]
fn orphan_reclaim_demotes_cold_entry_retires_and_frees() {
    // The headline §6 case: an orphaned-but-valid dedup PBA (every live LBA
    // referencing it overwritten, rc stays 1 in the index, GC Gate-1 filters
    // it) used to leak forever. The orphan pass demotes it (delete index entry,
    // rc 1->0, retire the PBA) and the existing GC confirm scan frees it.
    let (pool, meta, lifecycle, allocator, io_engine, read_pool) = setup_dedup_env_with_read_pool();
    register_small_volume(&meta, "test-vol", 64);

    let c = vec![0x11u8; 4096];
    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(&c);
    let p = promote_dedup_entry(
        &pool, &meta, &lifecycle, &allocator, &io_engine, "test-vol", 0, 2, &c,
    );

    // Orphan it: overwrite BOTH referrers with UNIQUE content (each a miss, no
    // new dedup), so no live LBA references P. rc-neutral -> P stays rc==1.
    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    let mut d0 = vec![0x22u8; 4096];
    d0[0] = 1;
    let mut d1 = vec![0x33u8; 4096];
    d1[0] = 2;
    pool.append("test-vol", Lba(0), 1, &d0, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000));
    pool.append("test-vol", Lba(2), 1, &d1, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000));
    flusher.stop();
    assert_eq!(
        meta.get_refcount(p).unwrap(),
        1,
        "orphaned dedup PBA keeps its membership rc==1 (overwrite is rc-neutral)"
    );

    // Stale heat for P's region (was live, then went cold == overwrite-orphaned).
    let heat = stale_heat(&allocator, p);
    let (mut scanner, _cand) = start_scanner_with_candidate_read_pool_cold_rx(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        Some(read_pool.clone()),
        None,
        Some(heat.clone()),
        orphan_scanner_config(),
    );
    assert!(
        wait_until(3000, || meta.get_dedup_entry(&hash).unwrap().is_none()
            && allocator.is_retired(p)),
        "orphan pass should delete the dedup entry and retire the now-rc==0 PBA"
    );
    assert_eq!(
        meta.get_refcount(p).unwrap(),
        0,
        "delete drops membership rc to 0"
    );
    scanner.stop();

    // The existing GC confirm scan frees the unreferenced retired PBA.
    let mut gc = start_reclaim_gc(&meta, &io_engine, &pool, &lifecycle, &allocator);
    assert!(
        wait_until(3000, || !allocator.is_retired(p)),
        "GC confirm scan should free the orphaned dedup PBA (leak closed)"
    );
    gc.stop();
}

#[test]
fn orphan_reclaim_still_referenced_pba_is_never_freed() {
    // Safety on the data-loss-critical path: even if the heat selector is WRONG
    // (cold region but the dedup PBA is actually still referenced — e.g. heat
    // staleness), the demote never loses data. The exact Gate-2
    // `referenced_extents` scan keeps a still-referenced PBA retired (not
    // freed), and the live LBA's mapping + data stay intact. Only dedup
    // membership is lost (re-promotable).
    let (pool, meta, lifecycle, allocator, io_engine, read_pool) = setup_dedup_env_with_read_pool();
    register_small_volume(&meta, "test-vol", 64);

    let c = vec![0x44u8; 4096];
    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(&c);
    let p = promote_dedup_entry(
        &pool, &meta, &lifecycle, &allocator, &io_engine, "test-vol", 0, 2, &c,
    );

    // Overwrite ONLY Lba(0); Lba(2) STILL references P (a live referrer).
    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    let mut d0 = vec![0x55u8; 4096];
    d0[0] = 9;
    pool.append("test-vol", Lba(0), 1, &d0, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000));
    flusher.stop();
    assert_eq!(
        meta.get_mapping(&VolumeId("test-vol".into()), Lba(2))
            .unwrap()
            .unwrap()
            .pba,
        p,
        "Lba(2) still references P"
    );

    // Stale heat (wrong selector) -> the pass demotes the still-referenced entry.
    let heat = stale_heat(&allocator, p);
    let (mut scanner, _cand) = start_scanner_with_candidate_read_pool_cold_rx(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        Some(read_pool.clone()),
        None,
        Some(heat.clone()),
        orphan_scanner_config(),
    );
    assert!(
        wait_until(3000, || meta.get_dedup_entry(&hash).unwrap().is_none()
            && allocator.is_retired(p)),
        "the pass demotes + retires even a (wrongly-selected) referenced entry"
    );
    scanner.stop();

    // GC confirm scan must NOT free P — Lba(2) still references it.
    let mut gc = start_reclaim_gc(&meta, &io_engine, &pool, &lifecycle, &allocator);
    // Give the GC several cycles; P must stay retired (referenced), never freed.
    assert!(
        !wait_until(800, || !allocator.is_retired(p)),
        "Gate-2 must keep a still-referenced PBA retired (never free it)"
    );
    gc.stop();
    // Data integrity: Lba(2)'s mapping still points at P (intact, not freed).
    assert_eq!(
        meta.get_mapping(&VolumeId("test-vol".into()), Lba(2))
            .unwrap()
            .unwrap()
            .pba,
        p,
        "still-referenced data must survive the demote unchanged"
    );
}

#[test]
fn reclaim_does_not_free_pba_re_referenced_during_hazard_wait() {
    // P0 premature-free / resurrection regression.
    //
    // An unguarded candidate-promote (`atomic_batch_dedup_hits_with_promote`)
    // can commit `L2pRemap L->P` for a PBA P that was just demoted + retired.
    // Its hazard pin (taken at candidate-lookup, held across the remap commit)
    // is the only signal that such a remap is in flight. The fix has GC drain
    // those pins BEFORE its Gate-2 `referenced_extents` scan, so the scan
    // observes the committed `L->P` and leaves P retired instead of freeing it
    // out from under the live mapping.
    //
    // This reproduces the race deterministically with the hazard pin standing
    // in for the in-flight promote's pin:
    //   1. hold a pin on a retired, unreferenced P,
    //   2. start GC -> it parks at the pre-scan hazard barrier,
    //   3. commit the promote that re-references P,
    //   4. release the pin -> GC's scan runs and now sees `L->P`.
    // With the barrier P stays retired. Without it, GC scanned P as
    // unreferenced and freed it after the per-extent wait => P freed while
    // `L->P` live (the crc_fg corruption).
    let (pool, meta, lifecycle, allocator, io_engine, read_pool) = setup_dedup_env_with_read_pool();
    register_small_volume(&meta, "test-vol", 64);
    let vol = VolumeId("test-vol".into());

    // P as a dedup target: Lba(0) + Lba(2) -> P, content C, dedup entry H->P.
    let c = vec![0x66u8; 4096];
    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(&c);
    let p = promote_dedup_entry(
        &pool, &meta, &lifecycle, &allocator, &io_engine, "test-vol", 0, 2, &c,
    );
    // Snapshot P's mapping + dedup entry now, to replay as the in-flight
    // promote after P is orphaned/demoted.
    let blockmap_p = meta.get_mapping(&vol, Lba(2)).unwrap().unwrap();
    assert_eq!(blockmap_p.pba, p);
    let entry_p = meta.get_dedup_entry(&hash).unwrap().unwrap();

    // Orphan P: overwrite BOTH referrers with unique content (rc-neutral; P
    // keeps membership rc==1 but no live LBA references it).
    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    let mut d0 = vec![0x77u8; 4096];
    d0[0] = 1;
    let mut d2 = vec![0x88u8; 4096];
    d2[0] = 2;
    pool.append("test-vol", Lba(0), 1, &d0, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000));
    pool.append("test-vol", Lba(2), 1, &d2, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000));
    flusher.stop();

    // Demote P: delete H->P (rc -> 0) + retire P. Now P is retired, rc==0,
    // unreferenced — a free candidate for the GC confirm scan.
    let heat = stale_heat(&allocator, p);
    let (mut scanner, _cand) = start_scanner_with_candidate_read_pool_cold_rx(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        Some(read_pool.clone()),
        None,
        Some(heat.clone()),
        orphan_scanner_config(),
    );
    assert!(
        wait_until(3000, || meta.get_dedup_entry(&hash).unwrap().is_none()
            && allocator.is_retired(p)),
        "orphan pass should delete the dedup entry and retire P"
    );
    scanner.stop();
    assert_eq!(meta.get_refcount(p).unwrap(), 0);

    // Control: an unreferenced, unpinned retired PBA the GC will definitely
    // free. Its release proves the reclaim pass actually ran this cycle, so
    // P's survival can't be a false pass from GC simply never processing
    // anything (both P and Q are scanned together by the one Gate-2 scan).
    let q = allocator.allocate_one().unwrap();
    assert!(allocator.retire_one(q).unwrap() > 0);

    // Model the in-flight promote's hazard pin (taken at candidate-lookup,
    // held across the remap commit). With P pinned, GC parks at the pre-scan
    // hazard barrier before its Gate-2 scan.
    let guard = allocator.hazards().pin_one(p);
    let mut gc = start_reclaim_gc(&meta, &io_engine, &pool, &lifecycle, &allocator);

    // The in-flight promote commits while GC is parked: unguarded remap
    // Lba(10) -> P + re-put H->P, exactly what the candidate-promote path does.
    let _ = meta
        .atomic_batch_dedup_hits_with_promote(
            &vol,
            &[(Lba(10), blockmap_p, hash)],
            &[(hash, entry_p)],
            &[2000u64],
        )
        .unwrap();
    assert_eq!(
        meta.get_mapping(&vol, Lba(10)).unwrap().unwrap().pba,
        p,
        "promote re-referenced P at Lba(10)"
    );

    // Release the pin -> GC's barrier passes -> its Gate-2 scan now observes the
    // promote (Lba(10) -> P) committed during the wait. (Drop before the
    // assertions so a failure panics cleanly instead of deadlocking on gc's
    // join, which would block on the still-held pin.)
    drop(guard);

    // The reclaim pass ran (control Q freed) but P, re-referenced during the
    // hazard wait, is NOT freed.
    assert!(
        wait_until(2000, || !allocator.is_retired(q)),
        "GC reclaim must run and free the unreferenced control PBA"
    );
    assert!(
        !wait_until(500, || !allocator.is_retired(p)),
        "Gate-2 must observe the promote that committed during the hazard wait \
         and keep P retired (never free a re-referenced PBA)"
    );
    gc.stop();
    assert_eq!(
        meta.get_mapping(&vol, Lba(10)).unwrap().unwrap().pba,
        p,
        "re-referenced mapping survives — P was not freed/reused"
    );
}

#[test]
fn orphan_reclaim_skips_hot_region() {
    // Selector: a dedup entry whose PBA region is HOT (a live mapping bumped it)
    // is left alone — demoting it would only churn dedup ratio.
    let (pool, meta, lifecycle, allocator, io_engine, read_pool) = setup_dedup_env_with_read_pool();
    register_small_volume(&meta, "test-vol", 64);

    let c = vec![0x66u8; 4096];
    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(&c);
    let p = promote_dedup_entry(
        &pool, &meta, &lifecycle, &allocator, &io_engine, "test-vol", 0, 2, &c,
    );

    // Heat: converged AND P's region HOT (a live mapping references it).
    let heat = converged_heat(&allocator);
    heat.bump(p);

    let (mut scanner, _cand) = start_scanner_with_candidate_read_pool_cold_rx(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        Some(read_pool.clone()),
        None,
        Some(heat.clone()),
        orphan_scanner_config(),
    );
    // Give the pass several cycles; the hot entry must survive.
    assert!(
        !wait_until(800, || meta.get_dedup_entry(&hash).unwrap().is_none()),
        "orphan pass must not demote a dedup entry in a hot region"
    );
    assert!(
        !allocator.is_retired(p),
        "hot-region PBA must not be retired"
    );
    scanner.stop();
}

#[test]
fn orphan_reclaim_demotes_stale_overwrite_orphaned_region() {
    // The REAL orphan case: the region was LIVE (heat bumped it at promote
    // time), then every referrer was overwritten. The heat bucket KEEPS its
    // stale count>0 — it does not become count==0 — but its age grows each
    // sweep. A `count==0`-only selector would miss this; the `fresh_max_age`
    // staleness check catches it.
    let (pool, meta, lifecycle, allocator, io_engine, read_pool) = setup_dedup_env_with_read_pool();
    register_small_volume(&meta, "test-vol", 64);

    let c = vec![0x77u8; 4096];
    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(&c);
    let p = promote_dedup_entry(
        &pool, &meta, &lifecycle, &allocator, &io_engine, "test-vol", 0, 2, &c,
    );

    // Orphan it: overwrite both referrers with unique content.
    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    let mut d0 = vec![0x88u8; 4096];
    d0[0] = 3;
    let mut d1 = vec![0x99u8; 4096];
    d1[0] = 4;
    pool.append("test-vol", Lba(0), 1, &d0, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000));
    pool.append("test-vol", Lba(2), 1, &d1, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000));
    flusher.stop();

    // Heat: P's region was HOT (bumped while live), then went STALE (not bumped
    // for > fresh_max_age=1 sweeps) — the overwrite-orphaned signature.
    let heat = onyx_storage::gc::heatmap::HeatMap::new(allocator.total_block_count(), 256);
    heat.advance_epoch(); // -> epoch 2
    heat.bump(p); // region = (epoch 2, count 1) — simulate it was live
    heat.advance_epoch(); // -> 3
    heat.advance_epoch(); // -> 4  (age = 4 - 2 = 2 > fresh_max_age=1)
    let (age, count) = heat.region(p);
    assert!(
        count > 0 && age > 1,
        "region must be STALE (count>0, age>fresh), not count==0: ({age},{count})"
    );

    let (mut scanner, _cand) = start_scanner_with_candidate_read_pool_cold_rx(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        Some(read_pool.clone()),
        None,
        Some(heat.clone()),
        orphan_scanner_config(),
    );
    assert!(
        wait_until(3000, || meta.get_dedup_entry(&hash).unwrap().is_none()
            && allocator.is_retired(p)),
        "orphan pass must demote a STALE (overwrite-orphaned) region, not just count==0"
    );
    scanner.stop();
}

// ---- Stage-5 per-PBA orphan reclaim -----------------------------------------

/// Build a `RefBitmap` (retaining `k` snapshots) by publishing one lap-barrier
/// per slice in `barriers`, marking the listed PBAs referenced in that barrier.
/// Everything not listed reads unreferenced. Mirrors `stale_heat`/`converged_heat`
/// for the per-PBA selector.
fn ref_bitmap_with(
    allocator: &Arc<SpaceAllocator>,
    k: usize,
    barriers: &[&[Pba]],
) -> onyx_storage::gc::ref_bitmap::RefBitmap {
    let rb = onyx_storage::gc::ref_bitmap::RefBitmap::new(allocator.total_block_count(), k);
    for marked in barriers {
        let mut buf = rb.fresh_fill_buffer();
        for &p in *marked {
            onyx_storage::gc::ref_bitmap::RefBitmap::mark(&mut buf, p);
        }
        let _ = rb.publish(buf);
    }
    rb
}

/// Dedup config for the per-PBA orphan-reclaim pass.
fn orphan_scanner_config_per_pba(clean_sweeps: u32) -> DedupConfig {
    DedupConfig {
        orphan_reclaim_per_pba: true,
        orphan_reclaim_clean_sweeps: clean_sweeps,
        ..orphan_scanner_config()
    }
}

/// Start a dedup scanner with a per-PBA `RefBitmap` selector (cold_rx off).
#[allow(clippy::too_many_arguments)]
fn start_scanner_with_ref_bitmap(
    pool: &Arc<WriteBufferPool>,
    meta: &Arc<MetaStore>,
    lifecycle: &Arc<VolumeLifecycleManager>,
    allocator: &Arc<SpaceAllocator>,
    io_engine: &Arc<IoEngine>,
    read_pool: Option<Arc<ReadPool>>,
    heat: Option<onyx_storage::gc::heatmap::HeatMap>,
    ref_bitmap: Option<onyx_storage::gc::ref_bitmap::RefBitmap>,
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
        None, // cold_rx
        heat,
        ref_bitmap,
        config,
    );
    (scanner, candidate)
}

#[test]
fn orphan_reclaim_per_pba_demotes_interleaved_orphan_in_hot_region() {
    // The Stage-5 headline win: an orphaned dedup PBA `P` that shares its 1 MiB
    // heat region with LIVE data (a neighbour PBA keeps the region count > 0, so
    // the §6 region selector would skip P) is still reclaimed, because the
    // per-PBA bitmap shows P itself unreferenced across K=2 barriers.
    let (pool, meta, lifecycle, allocator, io_engine, read_pool) = setup_dedup_env_with_read_pool();
    register_small_volume(&meta, "test-vol", 64);

    let c = vec![0x11u8; 4096];
    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(&c);
    let p = promote_dedup_entry(
        &pool, &meta, &lifecycle, &allocator, &io_engine, "test-vol", 0, 2, &c,
    );

    // Orphan P: overwrite both referrers with unique content (rc-neutral → P
    // stays rc==1, no live LBA references it).
    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    let mut d0 = vec![0x22u8; 4096];
    d0[0] = 1;
    let mut d1 = vec![0x33u8; 4096];
    d1[0] = 2;
    pool.append("test-vol", Lba(0), 1, &d0, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000));
    pool.append("test-vol", Lba(2), 1, &d1, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000));
    flusher.stop();
    assert_eq!(
        meta.get_refcount(p).unwrap(),
        1,
        "orphaned dedup PBA keeps rc==1"
    );

    // HOT heat region for P (neighbour live) → §6 region mode would skip P.
    let heat = converged_heat(&allocator);
    heat.bump(p);
    let (region_age, region_count) = heat.region(p);
    assert!(
        region_count > 0 && region_age == 0,
        "region must look HOT to §6 (count>0, fresh): ({region_age},{region_count})"
    );

    // Per-PBA bitmap: a neighbour in P's region is referenced across both
    // barriers, but P itself is NOT — the interleaved-orphan signature.
    let neighbour = Pba(p.0 + 1);
    let rb = ref_bitmap_with(&allocator, 2, &[&[neighbour], &[neighbour]]);

    let (mut scanner, _cand) = start_scanner_with_ref_bitmap(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        Some(read_pool.clone()),
        Some(heat.clone()),
        Some(rb),
        orphan_scanner_config_per_pba(2),
    );
    assert!(
        wait_until(3000, || meta.get_dedup_entry(&hash).unwrap().is_none()
            && allocator.is_retired(p)),
        "per-PBA pass must demote+retire the interleaved orphan despite the hot region"
    );
    assert_eq!(
        meta.get_refcount(p).unwrap(),
        0,
        "delete drops membership rc to 0"
    );
    scanner.stop();

    // GC confirm scan frees the unreferenced retired PBA end-to-end.
    let mut gc = start_reclaim_gc(&meta, &io_engine, &pool, &lifecycle, &allocator);
    assert!(
        wait_until(3000, || !allocator.is_retired(p)),
        "GC confirm scan should free the interleaved orphan (Stage-5 leak closed)"
    );
    gc.stop();
}

#[test]
fn orphan_reclaim_per_pba_still_referenced_pba_is_never_freed() {
    // Safety: even if the bitmap is WRONG (P reads unreferenced though Lba(2)
    // still references it), the demote never loses data — the Gate-2
    // `referenced_extents` scan keeps P retired (not freed) and the mapping +
    // data survive. Only dedup membership is lost (re-promotable).
    let (pool, meta, lifecycle, allocator, io_engine, read_pool) = setup_dedup_env_with_read_pool();
    register_small_volume(&meta, "test-vol", 64);

    let c = vec![0x44u8; 4096];
    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(&c);
    let p = promote_dedup_entry(
        &pool, &meta, &lifecycle, &allocator, &io_engine, "test-vol", 0, 2, &c,
    );

    // Overwrite ONLY Lba(0); Lba(2) STILL references P.
    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    let mut d0 = vec![0x55u8; 4096];
    d0[0] = 9;
    pool.append("test-vol", Lba(0), 1, &d0, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000));
    flusher.stop();
    assert_eq!(
        meta.get_mapping(&VolumeId("test-vol".into()), Lba(2))
            .unwrap()
            .unwrap()
            .pba,
        p,
        "Lba(2) still references P"
    );

    // Wrong bitmap: P unreferenced across both barriers though it IS referenced.
    let rb = ref_bitmap_with(&allocator, 2, &[&[], &[]]);
    let (mut scanner, _cand) = start_scanner_with_ref_bitmap(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        Some(read_pool.clone()),
        None,
        Some(rb),
        orphan_scanner_config_per_pba(2),
    );
    assert!(
        wait_until(3000, || meta.get_dedup_entry(&hash).unwrap().is_none()
            && allocator.is_retired(p)),
        "the pass demotes + retires even a (wrongly-selected) referenced entry"
    );
    scanner.stop();

    // GC confirm scan must NOT free P — Lba(2) still references it.
    let mut gc = start_reclaim_gc(&meta, &io_engine, &pool, &lifecycle, &allocator);
    assert!(
        !wait_until(800, || !allocator.is_retired(p)),
        "Gate-2 must keep a still-referenced PBA retired (never free it)"
    );
    gc.stop();
    assert_eq!(
        meta.get_mapping(&VolumeId("test-vol".into()), Lba(2))
            .unwrap()
            .unwrap()
            .pba,
        p,
        "still-referenced data must survive the demote unchanged"
    );
}

#[test]
fn orphan_reclaim_per_pba_skips_referenced_pba() {
    // Selector: a PBA referenced in a recent barrier (bit==1) →
    // `unreferenced_in_recent` is Some(false) → never demoted.
    let (pool, meta, lifecycle, allocator, io_engine, read_pool) = setup_dedup_env_with_read_pool();
    register_small_volume(&meta, "test-vol", 64);

    let c = vec![0x66u8; 4096];
    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(&c);
    let p = promote_dedup_entry(
        &pool, &meta, &lifecycle, &allocator, &io_engine, "test-vol", 0, 2, &c,
    );

    // P referenced in the most recent barrier (and the prior one) → not an orphan.
    let rb = ref_bitmap_with(&allocator, 2, &[&[p], &[p]]);
    let (mut scanner, _cand) = start_scanner_with_ref_bitmap(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        Some(read_pool.clone()),
        None,
        Some(rb),
        orphan_scanner_config_per_pba(2),
    );
    assert!(
        !wait_until(800, || meta.get_dedup_entry(&hash).unwrap().is_none()),
        "per-PBA pass must not demote a PBA referenced in a recent barrier"
    );
    assert!(
        !allocator.is_retired(p),
        "referenced PBA must not be retired"
    );
    scanner.stop();
}

#[test]
fn orphan_reclaim_per_pba_waits_for_k_barriers() {
    // Convergence: with fewer than K=2 published barriers,
    // `unreferenced_in_recent` returns None → nothing demoted (a 0 bit is not yet
    // trustworthy). Publishing the K-th barrier then unblocks the demote — no
    // pre-convergence churn.
    let (pool, meta, lifecycle, allocator, io_engine, read_pool) = setup_dedup_env_with_read_pool();
    register_small_volume(&meta, "test-vol", 64);

    let c = vec![0xABu8; 4096];
    let hash: ContentHash = onyx_storage::meta::schema::compute_content_hash(&c);
    let p = promote_dedup_entry(
        &pool, &meta, &lifecycle, &allocator, &io_engine, "test-vol", 0, 2, &c,
    );

    // Orphan P (overwrite both referrers).
    let mut flusher = start_flusher_with_dedup(&pool, &meta, &lifecycle, &allocator, &io_engine);
    let mut d0 = vec![0xCCu8; 4096];
    d0[0] = 5;
    let mut d1 = vec![0xDDu8; 4096];
    d1[0] = 6;
    pool.append("test-vol", Lba(0), 1, &d0, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000));
    pool.append("test-vol", Lba(2), 1, &d1, 1000).unwrap();
    assert!(wait_flushed(&pool, 10000));
    flusher.stop();

    // Only ONE barrier published (K=2 required, P unmarked) → not converged.
    let rb = ref_bitmap_with(&allocator, 2, &[&[]]);
    assert_eq!(rb.published_count(), 1);
    let (mut scanner, _cand) = start_scanner_with_ref_bitmap(
        &pool,
        &meta,
        &lifecycle,
        &allocator,
        &io_engine,
        Some(read_pool.clone()),
        None,
        Some(rb.clone()),
        orphan_scanner_config_per_pba(2),
    );
    assert!(
        !wait_until(600, || meta.get_dedup_entry(&hash).unwrap().is_none()),
        "below K barriers, the per-PBA pass must demote nothing"
    );

    // Publish the K-th barrier (P still unmarked) → now converged → demote.
    let buf = rb.fresh_fill_buffer();
    let _ = rb.publish(buf);
    assert_eq!(rb.published_count(), 2);
    assert!(
        wait_until(3000, || meta.get_dedup_entry(&hash).unwrap().is_none()
            && allocator.is_retired(p)),
        "after the K-th clean barrier, the orphan is demoted+retired"
    );
    scanner.stop();
}
