/// Unit and integration tests for the GC module.
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use onyx_storage::buffer::flush::BufferFlusher;
use onyx_storage::buffer::pool::WriteBufferPool;
use onyx_storage::compress::codec::create_compressor;
use onyx_storage::config::{FlushConfig, MetaConfig};
use onyx_storage::gc::config::GcConfig;
use onyx_storage::gc::rewriter::rewrite_candidate;
use onyx_storage::gc::scanner::{
    scan_gc_candidates, scan_gc_candidates_window, DefragScanParams, GcCandidate, SlotEvacParams,
};
use onyx_storage::io::device::RawDevice;
use onyx_storage::io::engine::IoEngine;
use onyx_storage::lifecycle::VolumeLifecycleManager;
use onyx_storage::meta::schema::*;
use onyx_storage::meta::store::MetaStore;
use onyx_storage::space::allocator::SpaceAllocator;
use onyx_storage::types::*;
use tempfile::{tempdir, NamedTempFile};

fn register_volume(meta: &MetaStore, name: &str, compression: CompressionAlgo, created_at: u64) {
    meta.put_volume(&VolumeConfig {
        id: VolumeId(name.to_string()),
        size_bytes: 4096 * 1000,
        block_size: 4096,
        compression,
        created_at,
        zone_count: 1,
    })
    .unwrap();
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

struct TestEnv {
    meta: Arc<MetaStore>,
    lifecycle: Arc<VolumeLifecycleManager>,
    allocator: Arc<SpaceAllocator>,
    pool: Arc<WriteBufferPool>,
    io_engine: Arc<IoEngine>,
    _meta_dir: tempfile::TempDir,
    _buf_tmp: NamedTempFile,
    _data_tmp: NamedTempFile,
}

fn setup_gc_env() -> TestEnv {
    let meta_dir = tempdir().unwrap();
    let meta_config = MetaConfig {
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
        ..MetaConfig::default()
    };
    let meta = Arc::new(MetaStore::open(&meta_config).unwrap());

    let buf_tmp = NamedTempFile::new().unwrap();
    let buf_size: u64 = 4096 + 200 * 8192;
    buf_tmp.as_file().set_len(buf_size).unwrap();
    let buf_dev = RawDevice::open_or_create(buf_tmp.path(), buf_size).unwrap();
    let pool = Arc::new(WriteBufferPool::open(buf_dev).unwrap());

    let data_tmp = NamedTempFile::new().unwrap();
    let data_size: u64 = 4096 * 20000;
    data_tmp.as_file().set_len(data_size).unwrap();
    let data_dev = RawDevice::open(data_tmp.path()).unwrap();
    let io_engine = Arc::new(IoEngine::new(data_dev, false));

    let lifecycle = Arc::new(VolumeLifecycleManager::default());
    let allocator = Arc::new(SpaceAllocator::new(data_size, 0));

    TestEnv {
        meta,
        lifecycle,
        allocator,
        pool,
        io_engine,
        _meta_dir: meta_dir,
        _buf_tmp: buf_tmp,
        _data_tmp: data_tmp,
    }
}

// ---------- Scanner Tests ----------

#[test]
fn scanner_finds_candidates_with_dead_blocks() {
    let dir = tempdir().unwrap();
    let meta_config = MetaConfig {
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
        ..MetaConfig::default()
    };
    let meta = MetaStore::open(&meta_config).unwrap();
    let vol_id = VolumeId("vol-test".into());
    meta.create_blockmap_cf("vol-test").unwrap();

    // Simulate a compression unit with 4 LBAs, but only 2 still point to PBA 100
    // (the other 2 were overwritten to different PBAs)
    let bv = BlockmapValue {
        pba: Pba(100),
        compression: 1,
        unit_compressed_size: 8000,
        unit_original_size: 16384,
        unit_lba_count: 4,
        offset_in_unit: 0,
        crc32: 0xDEADBEEF,
        slot_offset: 0,
        flags: 0,
    };

    // LBA 0 and LBA 2 still point to PBA 100
    meta.put_mapping(&vol_id, Lba(0), &bv).unwrap();
    meta.put_mapping(
        &vol_id,
        Lba(2),
        &BlockmapValue {
            offset_in_unit: 2,
            ..bv
        },
    )
    .unwrap();

    // LBA 1 and LBA 3 were overwritten to different PBAs
    meta.put_mapping(
        &vol_id,
        Lba(1),
        &BlockmapValue {
            pba: Pba(200),
            unit_lba_count: 1,
            offset_in_unit: 0,
            unit_compressed_size: 4000,
            unit_original_size: 4096,
            ..bv
        },
    )
    .unwrap();
    meta.put_mapping(
        &vol_id,
        Lba(3),
        &BlockmapValue {
            pba: Pba(300),
            unit_lba_count: 1,
            offset_in_unit: 0,
            unit_compressed_size: 4000,
            unit_original_size: 4096,
            ..bv
        },
    )
    .unwrap();

    // Scan with threshold 0.25 (25% dead)
    let candidates = scan_gc_candidates(&meta, 0.25, 100).unwrap();

    assert_eq!(candidates.len(), 1);
    let c = &candidates[0];
    assert_eq!(c.pba, Pba(100));
    assert_eq!(c.unit_lba_count, 4);
    assert_eq!(c.live_lbas.len(), 2);
    assert!((c.dead_ratio - 0.5).abs() < 0.01); // 2/4 = 50% dead
}

#[test]
fn scanner_skips_below_threshold() {
    let dir = tempdir().unwrap();
    let meta_config = MetaConfig {
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
        ..MetaConfig::default()
    };
    let meta = MetaStore::open(&meta_config).unwrap();
    let vol_id = VolumeId("vol-test".into());
    meta.create_blockmap_cf("vol-test").unwrap();

    // 8-LBA unit, 7 still live → only 12.5% dead, below 25% threshold
    for i in 0..7u64 {
        let bv = BlockmapValue {
            pba: Pba(100),
            compression: 1,
            unit_compressed_size: 16000,
            unit_original_size: 32768,
            unit_lba_count: 8,
            offset_in_unit: i as u16,
            crc32: 0,
            slot_offset: 0,
            flags: 0,
        };
        meta.put_mapping(&vol_id, Lba(i), &bv).unwrap();
    }
    // LBA 7 was overwritten
    meta.put_mapping(
        &vol_id,
        Lba(7),
        &BlockmapValue {
            pba: Pba(500),
            compression: 1,
            unit_compressed_size: 4000,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 0,
            slot_offset: 0,
            flags: 0,
        },
    )
    .unwrap();

    let candidates = scan_gc_candidates(&meta, 0.25, 100).unwrap();
    assert!(
        candidates.is_empty(),
        "12.5% dead should be below 25% threshold"
    );
}

#[test]
fn scanner_skips_single_lba_units() {
    let dir = tempdir().unwrap();
    let meta_config = MetaConfig {
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
        ..MetaConfig::default()
    };
    let meta = MetaStore::open(&meta_config).unwrap();
    let vol_id = VolumeId("vol-test".into());
    meta.create_blockmap_cf("vol-test").unwrap();

    // Single-LBA units can't have dead blocks
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
    meta.put_mapping(&vol_id, Lba(0), &bv).unwrap();

    let candidates = scan_gc_candidates(&meta, 0.0, 100).unwrap();
    assert!(candidates.is_empty());
}

#[test]
fn scanner_sorts_by_dead_ratio_descending() {
    let dir = tempdir().unwrap();
    let meta_config = MetaConfig {
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
        ..MetaConfig::default()
    };
    let meta = MetaStore::open(&meta_config).unwrap();
    let vol_id = VolumeId("vol-test".into());
    meta.create_blockmap_cf("vol-test").unwrap();

    // Unit at PBA 100: 4-LBA, 1 live → 75% dead
    meta.put_mapping(
        &vol_id,
        Lba(0),
        &BlockmapValue {
            pba: Pba(100),
            compression: 1,
            unit_compressed_size: 8000,
            unit_original_size: 16384,
            unit_lba_count: 4,
            offset_in_unit: 0,
            crc32: 0,
            slot_offset: 0,
            flags: 0,
        },
    )
    .unwrap();

    // Unit at PBA 200: 4-LBA, 2 live → 50% dead
    for i in 0..2u64 {
        meta.put_mapping(
            &vol_id,
            Lba(10 + i),
            &BlockmapValue {
                pba: Pba(200),
                compression: 1,
                unit_compressed_size: 8000,
                unit_original_size: 16384,
                unit_lba_count: 4,
                offset_in_unit: i as u16,
                crc32: 0,
                slot_offset: 0,
                flags: 0,
            },
        )
        .unwrap();
    }

    let candidates = scan_gc_candidates(&meta, 0.25, 100).unwrap();
    assert_eq!(candidates.len(), 2);
    assert!((candidates[0].dead_ratio - 0.75).abs() < 0.01);
    assert!((candidates[1].dead_ratio - 0.50).abs() < 0.01);
}

// ---------- GC End-to-End Tests ----------

#[test]
fn gc_rewrite_overwritten_blocks() {
    let env = setup_gc_env();
    let vol_id = "vol-gc";
    let vol_created_at = 12345u64;
    register_volume(&env.meta, vol_id, CompressionAlgo::Lz4, vol_created_at);

    // Write 8 contiguous LBAs (forms 1 compression unit after coalescing)
    let mut original_data = Vec::new();
    for i in 0u8..8 {
        let block = vec![i + 10; BLOCK_SIZE as usize];
        original_data.push(block.clone());
        env.pool
            .append(vol_id, Lba(i as u64), 1, &block, vol_created_at)
            .unwrap();
    }

    // Flush to LV3
    let mut flusher = start_flusher(
        &env.pool,
        &env.meta,
        &env.lifecycle,
        &env.allocator,
        &env.io_engine,
    );
    assert!(wait_for_flush(&env.pool, 5000), "initial flush timeout");
    flusher.stop();

    // Verify all 8 LBAs are mapped
    let vid = VolumeId(vol_id.to_string());
    for i in 0..8u64 {
        assert!(env.meta.get_mapping(&vid, Lba(i)).unwrap().is_some());
    }

    // Get the PBA of the compression unit
    let old_mapping = env.meta.get_mapping(&vid, Lba(0)).unwrap().unwrap();
    let old_pba = old_mapping.pba;
    let _free_before_overwrite = env.allocator.free_block_count();

    // Overwrite 6 of the 8 LBAs (LBA 2-7) with new data
    for i in 2u8..8 {
        let new_block = vec![i + 100; BLOCK_SIZE as usize];
        env.pool
            .append(vol_id, Lba(i as u64), 1, &new_block, vol_created_at)
            .unwrap();
    }

    // Flush the overwrites
    let mut flusher2 = start_flusher(
        &env.pool,
        &env.meta,
        &env.lifecycle,
        &env.allocator,
        &env.io_engine,
    );
    assert!(wait_for_flush(&env.pool, 5000), "overwrite flush timeout");
    flusher2.stop();

    // Now LBA 0 and 1 still point to old_pba, LBA 2-7 point to new PBAs
    let mapping0 = env.meta.get_mapping(&vid, Lba(0)).unwrap().unwrap();
    let mapping1 = env.meta.get_mapping(&vid, Lba(1)).unwrap().unwrap();
    assert_eq!(mapping0.pba, old_pba);
    assert_eq!(mapping1.pba, old_pba);
    for i in 2..8u64 {
        let m = env.meta.get_mapping(&vid, Lba(i)).unwrap().unwrap();
        assert_ne!(m.pba, old_pba, "LBA {} should have new PBA", i);
    }

    // Scan for GC candidates — old unit has 6/8 dead = 75%
    let candidates = scan_gc_candidates(&env.meta, 0.25, 100).unwrap();
    assert!(!candidates.is_empty(), "should find GC candidate");

    let candidate = candidates.iter().find(|c| c.pba == old_pba).unwrap();
    assert_eq!(candidate.live_lbas.len(), 2);
    assert!((candidate.dead_ratio - 0.75).abs() < 0.01);

    // Windowed scan (resident compactor path): a window covering the whole
    // volume must surface the SAME candidate and report a nonzero compactable
    // dead-block estimate (the 6 dead members of the old unit).
    let (win_candidates, dead_estimate, _slot_stats, _defrag_stats) = scan_gc_candidates_window(
        &env.meta,
        &vid,
        Lba(0),
        64,
        0.25,
        100,
        onyx_storage::gc::scanner::SlotEvacParams::disabled(),
        &onyx_storage::gc::scanner::DefragScanParams::disabled(),
    )
    .unwrap();
    let win = win_candidates
        .iter()
        .find(|c| c.pba == old_pba)
        .expect("windowed scan should find the same candidate as the full scan");
    assert_eq!(win.live_lbas.len(), 2);
    assert!((win.dead_ratio - 0.75).abs() < 0.01);
    assert!(
        dead_estimate >= 6,
        "debt estimate should count the 6 dead members, got {dead_estimate}"
    );

    // Rewrite the candidate — live blocks go back to buffer
    let rewritten = rewrite_candidate(
        candidate,
        &env.io_engine,
        &env.pool,
        &env.meta,
        &env.lifecycle,
        None,
    )
    .unwrap();
    assert_eq!(rewritten, 2);

    // Flush the rewritten blocks
    let mut flusher3 = start_flusher(
        &env.pool,
        &env.meta,
        &env.lifecycle,
        &env.allocator,
        &env.io_engine,
    );
    assert!(wait_for_flush(&env.pool, 5000), "gc rewrite flush timeout");
    flusher3.stop();

    // After GC flush, LBA 0 and 1 should still be mapped.
    // With dedup enabled, the PBA might stay the same (dedup hit to same data).
    // Without dedup, the PBA would change to a new allocation.
    let new_mapping0 = env.meta.get_mapping(&vid, Lba(0)).unwrap().unwrap();
    let new_mapping1 = env.meta.get_mapping(&vid, Lba(1)).unwrap().unwrap();
    // Both LBAs should still be readable (mapped)
    assert!(new_mapping0.unit_lba_count > 0);
    assert!(new_mapping1.unit_lba_count > 0);

    // If PBAs changed (no dedup), old refcount should be 0.
    // If PBAs stayed same (dedup hit), refcount should be 2 (the 2 live LBAs).
    let old_rc = env.meta.get_refcount(old_pba).unwrap();
    if new_mapping0.pba != old_pba && new_mapping1.pba != old_pba {
        assert_eq!(old_rc, 0, "old PBA refcount should be 0 when PBAs changed");
    } else {
        assert_eq!(old_rc, 2, "refcount should be 2 for dedup'd live blocks");
    }

    // Verify data integrity — read all 8 blocks and check content
    use onyx_storage::zone::worker::ZoneWorker;
    let worker = ZoneWorker::new(
        ZoneId(0),
        env.meta.clone(),
        env.pool.clone(),
        env.io_engine.clone(),
    );

    // LBA 0 and 1 should still contain original data
    let data0 = worker.handle_read(vol_id, Lba(0)).unwrap().unwrap();
    assert_eq!(
        data0,
        vec![10u8; BLOCK_SIZE as usize],
        "LBA 0 data mismatch after GC"
    );
    let data1 = worker.handle_read(vol_id, Lba(1)).unwrap().unwrap();
    assert_eq!(
        data1,
        vec![11u8; BLOCK_SIZE as usize],
        "LBA 1 data mismatch after GC"
    );

    // LBA 2-7 should contain overwritten data
    for i in 2u8..8 {
        let data = worker.handle_read(vol_id, Lba(i as u64)).unwrap().unwrap();
        assert_eq!(
            data,
            vec![i + 100; BLOCK_SIZE as usize],
            "LBA {} data mismatch after GC",
            i
        );
    }
}

#[test]
fn gc_rewriter_skips_changed_lba() {
    let env = setup_gc_env();
    let vol_id = "vol-race";
    let vol_created_at = 99999u64;
    register_volume(&env.meta, vol_id, CompressionAlgo::Lz4, vol_created_at);

    // Write 4 LBAs
    for i in 0u8..4 {
        let block = vec![i + 10; BLOCK_SIZE as usize];
        env.pool
            .append(vol_id, Lba(i as u64), 1, &block, vol_created_at)
            .unwrap();
    }

    let mut flusher = start_flusher(
        &env.pool,
        &env.meta,
        &env.lifecycle,
        &env.allocator,
        &env.io_engine,
    );
    assert!(wait_for_flush(&env.pool, 5000));
    flusher.stop();

    let vid = VolumeId(vol_id.to_string());
    let old_mapping = env.meta.get_mapping(&vid, Lba(0)).unwrap().unwrap();

    // Overwrite LBA 1 and 2
    for i in 1u8..3 {
        let block = vec![i + 50; BLOCK_SIZE as usize];
        env.pool
            .append(vol_id, Lba(i as u64), 1, &block, vol_created_at)
            .unwrap();
    }
    let mut flusher2 = start_flusher(
        &env.pool,
        &env.meta,
        &env.lifecycle,
        &env.allocator,
        &env.io_engine,
    );
    assert!(wait_for_flush(&env.pool, 5000));
    flusher2.stop();

    // Scan for candidates
    let candidates = scan_gc_candidates(&env.meta, 0.25, 100).unwrap();
    let candidate = candidates
        .iter()
        .find(|c| {
            c.pba == old_mapping.pba
                && c.slot_offset == old_mapping.slot_offset
                && c.unit_compressed_size == old_mapping.unit_compressed_size
                && c.unit_lba_count == old_mapping.unit_lba_count
                && c.compression == old_mapping.compression
                && c.crc32 == old_mapping.crc32
        })
        .unwrap();
    assert_eq!(candidate.live_lbas.len(), 2); // LBA 0 and 3

    // Now overwrite LBA 0 AFTER the scan (simulating race condition)
    env.pool
        .append(
            vol_id,
            Lba(0),
            1,
            &vec![0xFF; BLOCK_SIZE as usize],
            vol_created_at,
        )
        .unwrap();
    let mut flusher3 = start_flusher(
        &env.pool,
        &env.meta,
        &env.lifecycle,
        &env.allocator,
        &env.io_engine,
    );
    assert!(wait_for_flush(&env.pool, 5000));
    flusher3.stop();

    // Now LBA 0 no longer points to old_pba — rewriter should skip it
    let rewritten = rewrite_candidate(
        candidate,
        &env.io_engine,
        &env.pool,
        &env.meta,
        &env.lifecycle,
        None,
    )
    .unwrap();

    // Only LBA 3 should be rewritten (LBA 0 was overwritten since scan)
    assert_eq!(rewritten, 1);
}

#[test]
fn gc_back_pressure_skips_when_buffer_full() {
    // This is a behavioral test — we verify the GcConfig back-pressure logic.
    // The actual GcRunner checks fill_percentage() in its loop.
    let env = setup_gc_env();

    // fill_percentage() with empty pool should be low
    let pct = env.pool.fill_percentage();
    assert!(pct < 80, "empty buffer should have low fill percentage");

    // Config says skip when > 80%
    let config = GcConfig {
        buffer_usage_max_pct: 80,
        buffer_usage_resume_pct: 50,
        ..GcConfig::default()
    };
    assert!(pct <= config.buffer_usage_max_pct);
}

// ---------- Schema backward compatibility tests ----------

#[test]
fn blockmap_value_28byte_roundtrip() {
    let v = BlockmapValue {
        pba: Pba(42),
        compression: 1,
        unit_compressed_size: 2048,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0xDEAD,
        slot_offset: 512,
        flags: 0,
    };
    let encoded = encode_blockmap_value(&v);
    assert_eq!(encoded.len(), 28);
    let decoded = decode_blockmap_value(&encoded).unwrap();
    assert_eq!(decoded, v);
    assert_eq!(decoded.slot_offset, 512);
}

#[test]
fn blockmap_value_rejects_wrong_length() {
    assert!(decode_blockmap_value(&[0u8; 25]).is_none());
    assert!(decode_blockmap_value(&[0u8; 17]).is_none());
    assert!(decode_blockmap_value(&[0u8; 27]).is_none());
}

// ---------- Fix validation tests ----------

/// Scanner must distinguish multiple fragments packed into the same PBA slot.
/// Before the fix, all fragments sharing a PBA were merged into one GcCandidate
/// with wrong vol_id/unit_lba_count/crc32.
#[test]
fn scanner_distinguishes_packed_fragments_same_pba() {
    let dir = tempdir().unwrap();
    let meta_config = MetaConfig {
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
        ..MetaConfig::default()
    };
    let meta = MetaStore::open(&meta_config).unwrap();

    let shared_pba = Pba(100);

    // Fragment A: vol-a, 4-LBA unit at slot_offset=0, 2 live
    let vol_a = VolumeId("vol-a".into());
    meta.create_blockmap_cf("vol-a").unwrap();
    for i in [0u16, 2] {
        meta.put_mapping(
            &vol_a,
            Lba(i as u64),
            &BlockmapValue {
                pba: shared_pba,
                compression: 1,
                unit_compressed_size: 1000,
                unit_original_size: 16384,
                unit_lba_count: 4,
                offset_in_unit: i,
                crc32: 0xAAAA,
                slot_offset: 0,
                flags: 0,
            },
        )
        .unwrap();
    }

    // Fragment B: vol-b, 4-LBA unit at slot_offset=1000, 3 live
    let vol_b = VolumeId("vol-b".into());
    meta.create_blockmap_cf("vol-b").unwrap();
    for i in [0u16, 1, 3] {
        meta.put_mapping(
            &vol_b,
            Lba(10 + i as u64),
            &BlockmapValue {
                pba: shared_pba,
                compression: 1,
                unit_compressed_size: 2000,
                unit_original_size: 16384,
                unit_lba_count: 4,
                offset_in_unit: i,
                crc32: 0xBBBB,
                slot_offset: 1000,
                flags: 0,
            },
        )
        .unwrap();
    }

    let candidates = scan_gc_candidates(&meta, 0.20, 100).unwrap();

    // Must find TWO separate candidates, not one merged one
    assert_eq!(
        candidates.len(),
        2,
        "should find 2 separate candidates for 2 fragments"
    );

    // Fragment A: 2/4 live → 50% dead
    let cand_a = candidates.iter().find(|c| c.slot_offset == 0).unwrap();
    assert_eq!(cand_a.vol_id, vol_a);
    assert_eq!(cand_a.unit_lba_count, 4);
    assert_eq!(cand_a.crc32, 0xAAAA);
    assert_eq!(cand_a.live_lbas.len(), 2);
    assert!((cand_a.dead_ratio - 0.5).abs() < 0.01);

    // Fragment B: 3/4 live → 25% dead
    let cand_b = candidates.iter().find(|c| c.slot_offset == 1000).unwrap();
    assert_eq!(cand_b.vol_id, vol_b);
    assert_eq!(cand_b.unit_lba_count, 4);
    assert_eq!(cand_b.crc32, 0xBBBB);
    assert_eq!(cand_b.live_lbas.len(), 3);
    assert!((cand_b.dead_ratio - 0.25).abs() < 0.01);
}

#[test]
fn scanner_does_not_merge_fragments_with_same_pba_offset_and_size_but_different_identity() {
    let dir = tempdir().unwrap();
    let meta_config = MetaConfig {
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
        ..MetaConfig::default()
    };
    let meta = MetaStore::open(&meta_config).unwrap();

    let shared_pba = Pba(4242);
    let vol_a = VolumeId("vol-a".into());
    meta.create_blockmap_cf("vol-a").unwrap();
    let vol_b = VolumeId("vol-b".into());
    meta.create_blockmap_cf("vol-b").unwrap();

    for i in [0u16, 2] {
        meta.put_mapping(
            &vol_a,
            Lba(i as u64),
            &BlockmapValue {
                pba: shared_pba,
                compression: 1,
                unit_compressed_size: 1000,
                unit_original_size: 16384,
                unit_lba_count: 4,
                offset_in_unit: i,
                crc32: 0xAAAA,
                slot_offset: 0,
                flags: 0,
            },
        )
        .unwrap();
    }

    for i in [0u16, 1] {
        meta.put_mapping(
            &vol_b,
            Lba(10 + i as u64),
            &BlockmapValue {
                pba: shared_pba,
                compression: 1,
                unit_compressed_size: 1000,
                unit_original_size: 8192,
                unit_lba_count: 2,
                offset_in_unit: i,
                crc32: 0xBBBB,
                slot_offset: 0,
                flags: 0,
            },
        )
        .unwrap();
    }

    let candidates = scan_gc_candidates(&meta, 0.0, 100).unwrap();
    assert_eq!(
        candidates.len(),
        2,
        "scanner must keep distinct fragment identities separate even when pba/offset/size match"
    );
    assert!(candidates
        .iter()
        .any(|c| c.vol_id == vol_a && c.crc32 == 0xAAAA));
    assert!(candidates
        .iter()
        .any(|c| c.vol_id == vol_b && c.crc32 == 0xBBBB));
}

/// GC rewriter must verify the FULL fragment identity, not just PBA.
/// If a live LBA was remapped to a different fragment in the same packed slot
/// (same PBA, different slot_offset / metadata), rewriting from the old
/// candidate would corrupt data.
#[test]
fn gc_rewriter_skips_lba_when_fragment_identity_changed_with_same_pba() {
    let env = setup_gc_env();

    let shared_pba = Pba(777);
    let vol_a = VolumeId("vol-a".into());
    env.meta.create_blockmap_cf("vol-a").unwrap();
    let vol_b = VolumeId("vol-b".into());
    env.meta.create_blockmap_cf("vol-b").unwrap();
    register_volume(&env.meta, &vol_a.0, CompressionAlgo::Lz4, 100);
    register_volume(&env.meta, &vol_b.0, CompressionAlgo::Lz4, 200);

    let old_plain = vec![0x11; 2 * BLOCK_SIZE as usize];
    let new_plain = vec![0x22; 2 * BLOCK_SIZE as usize];
    let compressor = create_compressor(CompressionAlgo::Lz4);

    let mut old_buf = vec![0u8; compressor.max_compressed_size(old_plain.len())];
    let old_size = compressor.compress(&old_plain, &mut old_buf).unwrap();
    let old_compressed = old_buf[..old_size].to_vec();
    let old_crc = crc32fast::hash(&old_compressed);

    let mut new_buf = vec![0u8; compressor.max_compressed_size(new_plain.len())];
    let new_size = compressor.compress(&new_plain, &mut new_buf).unwrap();
    let new_compressed = new_buf[..new_size].to_vec();
    let new_crc = crc32fast::hash(&new_compressed);

    let new_slot_offset = 1024u16;
    let mut slot = vec![0u8; BLOCK_SIZE as usize];
    slot[..old_compressed.len()].copy_from_slice(&old_compressed);
    slot[new_slot_offset as usize..new_slot_offset as usize + new_compressed.len()]
        .copy_from_slice(&new_compressed);
    env.io_engine.write_blocks(shared_pba, &slot).unwrap();

    // Old fragment A at slot_offset=0, 2 LBAs, with only LBA 0 still live.
    env.meta
        .put_mapping(
            &vol_a,
            Lba(0),
            &BlockmapValue {
                pba: shared_pba,
                compression: 1,
                unit_compressed_size: old_compressed.len() as u32,
                unit_original_size: 8192,
                unit_lba_count: 2,
                offset_in_unit: 0,
                crc32: old_crc,
                slot_offset: 0,
                flags: 0,
            },
        )
        .unwrap();

    // Different fragment B in the same slot.
    env.meta
        .put_mapping(
            &vol_b,
            Lba(10),
            &BlockmapValue {
                pba: shared_pba,
                compression: 1,
                unit_compressed_size: new_compressed.len() as u32,
                unit_original_size: 8192,
                unit_lba_count: 2,
                offset_in_unit: 0,
                crc32: new_crc,
                slot_offset: new_slot_offset,
                flags: 0,
            },
        )
        .unwrap();

    let candidate = GcCandidate {
        pba: shared_pba,
        vol_id: vol_a.clone(),
        compression: 1,
        unit_compressed_size: old_compressed.len() as u32,
        unit_original_size: 8192,
        unit_lba_count: 2,
        crc32: old_crc,
        slot_offset: 0,
        live_lbas: vec![(Lba(0), 0)],
        dead_ratio: 0.5,
        defrag: false,
    };

    // Simulate a race after scan: LBA 0 is remapped to a DIFFERENT fragment in
    // the same packed slot (same PBA, different slot_offset/metadata).
    env.meta
        .put_mapping(
            &vol_a,
            Lba(0),
            &BlockmapValue {
                pba: shared_pba,
                compression: 1,
                unit_compressed_size: new_compressed.len() as u32,
                unit_original_size: 8192,
                unit_lba_count: 2,
                offset_in_unit: 1,
                crc32: new_crc,
                slot_offset: new_slot_offset,
                flags: 0,
            },
        )
        .unwrap();

    // The old candidate no longer owns LBA 0 and must be skipped.
    let rewritten = rewrite_candidate(
        &candidate,
        &env.io_engine,
        &env.pool,
        &env.meta,
        &env.lifecycle,
        None,
    )
    .unwrap();
    assert_eq!(
        rewritten, 0,
        "GC must skip LBA when fragment identity changed within same PBA"
    );
}

/// Lifecycle lock must cover the entire packed write — from generation check
/// through metadata commit. This test verifies that delete_volume during a
/// packed flush does not leave stale blockmap entries.
#[test]
fn packed_flush_lifecycle_lock_covers_metadata_commit() {
    let env = setup_gc_env();
    let vol_id = "vol-lifecycle";
    let vol_created_at = 77777u64;
    register_volume(&env.meta, vol_id, CompressionAlgo::Lz4, vol_created_at);

    // Write some data
    let data = vec![42u8; BLOCK_SIZE as usize];
    env.pool
        .append(vol_id, Lba(0), 1, &data, vol_created_at)
        .unwrap();

    // Flush
    let mut flusher = start_flusher(
        &env.pool,
        &env.meta,
        &env.lifecycle,
        &env.allocator,
        &env.io_engine,
    );
    assert!(wait_for_flush(&env.pool, 5000));
    flusher.stop();

    // Verify blockmap entry exists
    let vid = VolumeId(vol_id.to_string());
    assert!(env.meta.get_mapping(&vid, Lba(0)).unwrap().is_some());

    // Now write more data and delete the volume before flushing
    env.pool
        .append(vol_id, Lba(1), 1, &data, vol_created_at)
        .unwrap();

    // Delete the volume using lifecycle write lock (simulating engine.delete_volume)
    env.lifecycle.with_write_lock(vol_id, || {
        env.pool.purge_volume(vol_id).unwrap();
        env.meta.delete_volume(&vid).unwrap();
    });

    // Verify volume is gone
    assert!(env.meta.get_volume(&vid).unwrap().is_none());

    // Any pending buffer entries should have been purged
    // If not, the flusher should discard them due to generation/volume check
    let mut flusher2 = start_flusher(
        &env.pool,
        &env.meta,
        &env.lifecycle,
        &env.allocator,
        &env.io_engine,
    );
    // Give flusher time to process any remaining entries
    std::thread::sleep(std::time::Duration::from_millis(200));
    flusher2.stop();

    // The deleted volume's blockmap should NOT have been recreated by the flusher
    assert!(
        env.meta.get_mapping(&vid, Lba(0)).unwrap().is_none(),
        "deleted volume's blockmap must not be recreated by packed flush"
    );
    assert!(
        env.meta.get_mapping(&vid, Lba(1)).unwrap().is_none(),
        "deleted volume's blockmap must not be recreated by packed flush"
    );
}

/// GC rewrite of a packed slot must correctly handle the slot_offset:
/// read the fragment from within the shared slot, not the whole slot.
#[test]
fn gc_rewrite_packed_fragment() {
    let env = setup_gc_env();
    let vol_id = "vol-packed-gc";
    let vol_created_at = 55555u64;
    register_volume(&env.meta, vol_id, CompressionAlgo::Lz4, vol_created_at);

    // Write 4 contiguous LBAs (will be coalesced, compressed, and packed)
    for i in 0u8..4 {
        let block = vec![i + 20; BLOCK_SIZE as usize];
        env.pool
            .append(vol_id, Lba(i as u64), 1, &block, vol_created_at)
            .unwrap();
    }

    let mut flusher = start_flusher(
        &env.pool,
        &env.meta,
        &env.lifecycle,
        &env.allocator,
        &env.io_engine,
    );
    assert!(wait_for_flush(&env.pool, 5000));
    flusher.stop();

    let vid = VolumeId(vol_id.to_string());
    let original_pba = env.meta.get_mapping(&vid, Lba(0)).unwrap().unwrap().pba;

    // Overwrite 3 of 4 LBAs
    for i in 1u8..4 {
        let block = vec![i + 200; BLOCK_SIZE as usize];
        env.pool
            .append(vol_id, Lba(i as u64), 1, &block, vol_created_at)
            .unwrap();
    }
    let mut flusher2 = start_flusher(
        &env.pool,
        &env.meta,
        &env.lifecycle,
        &env.allocator,
        &env.io_engine,
    );
    assert!(wait_for_flush(&env.pool, 5000));
    flusher2.stop();

    // GC scan + rewrite
    let candidates = scan_gc_candidates(&env.meta, 0.25, 100).unwrap();
    let candidate = candidates.iter().find(|c| c.pba == original_pba);
    if let Some(candidate) = candidate {
        assert_eq!(candidate.live_lbas.len(), 1); // only LBA 0
        let rewritten = rewrite_candidate(
            candidate,
            &env.io_engine,
            &env.pool,
            &env.meta,
            &env.lifecycle,
            None,
        )
        .unwrap();
        assert_eq!(rewritten, 1);

        // Flush rewritten block
        let mut flusher3 = start_flusher(
            &env.pool,
            &env.meta,
            &env.lifecycle,
            &env.allocator,
            &env.io_engine,
        );
        assert!(wait_for_flush(&env.pool, 5000));
        flusher3.stop();
    }

    // Verify data integrity after GC
    use onyx_storage::zone::worker::ZoneWorker;
    let worker = ZoneWorker::new(
        ZoneId(0),
        env.meta.clone(),
        env.pool.clone(),
        env.io_engine.clone(),
    );

    let data0 = worker.handle_read(vol_id, Lba(0)).unwrap().unwrap();
    assert_eq!(
        data0,
        vec![20u8; BLOCK_SIZE as usize],
        "LBA 0 data mismatch after packed GC"
    );

    for i in 1u8..4 {
        let data = worker.handle_read(vol_id, Lba(i as u64)).unwrap().unwrap();
        assert_eq!(
            data,
            vec![i + 200; BLOCK_SIZE as usize],
            "LBA {} data mismatch",
            i
        );
    }
}

/// Regression: after repeated write-overwrite cycles that cause PBA
/// recycling, no blockmap overlaps exist (the invariant that the soak test
/// catches when this race fires in production).
#[test]
fn no_blockmap_overlap_after_packed_slot_recycling() {
    let env = setup_gc_env();

    let vol_a = "overlap-a";
    let vol_b = "overlap-b";
    register_volume(&env.meta, vol_a, CompressionAlgo::Lz4, 300);
    register_volume(&env.meta, vol_b, CompressionAlgo::Lz4, 400);

    // Cycle: write two packed fragments, overwrite one, then overwrite the
    // other (frees PBA). Repeat to force PBA recycling.
    for cycle in 0u8..5 {
        let data_a = vec![cycle * 10 + 1; BLOCK_SIZE as usize];
        let data_b = vec![cycle * 10 + 2; BLOCK_SIZE as usize];
        env.pool.append(vol_a, Lba(0), 1, &data_a, 300).unwrap();
        env.pool.append(vol_b, Lba(0), 1, &data_b, 400).unwrap();

        let mut flusher = BufferFlusher::start(
            env.pool.clone(),
            env.meta.clone(),
            env.lifecycle.clone(),
            env.allocator.clone(),
            env.io_engine.clone(),
            &FlushConfig::default(),
            &onyx_storage::dedup::config::DedupConfig::default(),
        );
        assert!(
            wait_for_flush(&env.pool, 5000),
            "flush timeout at cycle {cycle}"
        );
        flusher.stop();
    }

    // Verify: no PBA has overlapping fragments in the blockmap.
    // Collect all live fragments per PBA, check byte ranges pairwise.
    let vid_a = VolumeId(vol_a.to_string());
    let vid_b = VolumeId(vol_b.to_string());
    let mut pba_fragments: std::collections::HashMap<u64, Vec<(u16, u32)>> =
        std::collections::HashMap::new();
    for vid in [&vid_a, &vid_b] {
        if let Some(bv) = env.meta.get_mapping(vid, Lba(0)).unwrap() {
            pba_fragments
                .entry(bv.pba.0)
                .or_default()
                .push((bv.slot_offset, bv.unit_compressed_size));
        }
    }
    for (pba, frags) in &pba_fragments {
        for i in 0..frags.len() {
            for j in (i + 1)..frags.len() {
                let (a_off, a_size) = frags[i];
                let (b_off, b_size) = frags[j];
                let a_end = a_off as u32 + a_size;
                let b_end = b_off as u32 + b_size;
                let overlap = (a_off as u32) < b_end && (b_off as u32) < a_end;
                assert!(
                    !overlap,
                    "overlapping fragments at PBA {pba}: [{a_off}..{a_end}) vs [{b_off}..{b_end})"
                );
            }
        }
    }
}

// ---------- Adaptive reclaim heat-map (Stage A, observe-only) ----------

use onyx_storage::gc::heatmap::HeatMap;
use onyx_storage::gc::runner::GcRunner;
use onyx_storage::metrics::EngineMetrics;

/// Put a live blockmap mapping (helper for the heat tests).
fn put_live(meta: &MetaStore, vol: &VolumeId, lba: u64, pba: u64) {
    let bv = BlockmapValue {
        pba: Pba(pba),
        compression: 0,
        unit_compressed_size: 4096,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0,
        slot_offset: 0,
        flags: 0,
    };
    meta.put_mapping(vol, Lba(lba), &bv).unwrap();
}

/// The background refresh walks live L2P and accumulates per-region counts; a
/// completed sweep advances the epoch. End-to-end through the real GcRunner.
#[test]
fn heat_refresh_counts_live_mappings_and_advances_epoch() {
    let env = setup_gc_env();
    let vol = VolumeId("heat-vol".into());
    register_volume(&env.meta, "heat-vol", CompressionAlgo::None, 1);
    env.meta.create_blockmap_cf("heat-vol").unwrap();

    // Three LBAs share heat bucket for PBA ~100; one LBA lands far away.
    put_live(&env.meta, &vol, 0, 100);
    put_live(&env.meta, &vol, 1, 101);
    put_live(&env.meta, &vol, 2, 102);
    put_live(&env.meta, &vol, 10, 5000);

    let metrics = Arc::new(EngineMetrics::default());
    let bucket_blocks = 64;
    let heat = HeatMap::new(env.allocator.total_block_count(), bucket_blocks);
    let cfg = GcConfig {
        enabled: false, // only reclaim + heat refresh run, no rewrite scan
        scan_interval_ms: 30,
        heat_enabled: true,
        heat_bucket_size_blocks: bucket_blocks,
        heat_refresh_max_lbas_per_cycle: 1000, // one full sweep of the 1000-LBA volume per cycle
        ..Default::default()
    };
    let mut gc = GcRunner::start_with_metrics(
        metrics.clone(),
        env.meta.clone(),
        env.io_engine.clone(),
        env.pool.clone(),
        env.lifecycle.clone(),
        env.allocator.clone(),
        heat.clone(),
        None, // ref_bitmap: Stage-5 per-PBA orphan reclaim off in this test
        None, // cold_tail_tx: Stage-4 fold off in this test
        onyx_storage::space::pba_lifecycle::PbaLifecycle::new(
            env.allocator.clone(),
            onyx_storage::dedup::CandidateCache::new(1, 1),
            metrics.clone(),
        ),
        cfg,
    );

    // ~8 cycles at 30 ms; wait for at least one completed sweep.
    let mut sweeps = 0;
    for _ in 0..50 {
        thread::sleep(Duration::from_millis(20));
        sweeps = metrics
            .heat_sweeps_completed
            .load(std::sync::atomic::Ordering::Relaxed);
        if sweeps >= 1 {
            break;
        }
    }
    gc.stop();

    assert!(sweeps >= 1, "expected >=1 completed sweep, got {sweeps}");
    assert!(
        metrics
            .heat_bumps
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 4,
        "expected >=4 region bumps for 4 live mappings"
    );
    let summary = heat.summary();
    assert!(
        summary.nonzero_buckets >= 2,
        "expected the two distinct PBA regions to be counted, got {}",
        summary.nonzero_buckets
    );
    assert!(
        summary.current_epoch >= 2,
        "epoch should advance past the initial 1"
    );
    // The bucket covering PBA 100..102 saw the three live mappings. Bounded
    // 1..=3 (a stop caught mid-sweep may show a partial count) — the key point
    // is it is counted and never accumulates past one sweep's worth (the 300×
    // runaway the per-sweep epoch advance fixes). Exact reset arithmetic is
    // covered by the heatmap unit tests.
    let c100 = heat.region(Pba(100)).1;
    assert!(
        (1..=3).contains(&c100),
        "PBA 100 region count {c100} out of 1..=3"
    );
    let c5000 = heat.region(Pba(5000)).1;
    assert!(
        (1..=1).contains(&c5000),
        "PBA 5000 region count {c5000} out of 1..=1"
    );
    // A region with no live mapping was never scanned to a non-zero count.
    assert_eq!(heat.region(Pba(9000)).1, 0);
}

/// Stage-4 fold producer: with `cold_tx` wired and a non-zero push budget,
/// the heat-refresh walk emits each non-zero live L2P entry it decodes as a
/// cold candidate over the channel (in addition to bumping heat), bounded by
/// `heat_fold_push_max_per_cycle`. This is the producer half the dedup
/// scanner's `cold_tail_drain` consumes — proving the duplicate scan is gone
/// (one walk feeds both heat + cold-tail).
#[test]
fn heat_refresh_fold_pushes_cold_candidates() {
    use crossbeam_channel::bounded;
    use std::sync::atomic::Ordering;

    let env = setup_gc_env();
    let vol = VolumeId("fold-vol".into());
    register_volume(&env.meta, "fold-vol", CompressionAlgo::None, 1);
    env.meta.create_blockmap_cf("fold-vol").unwrap();
    put_live(&env.meta, &vol, 0, 100);
    put_live(&env.meta, &vol, 1, 101);
    put_live(&env.meta, &vol, 2, 102);

    let metrics = Arc::new(EngineMetrics::default());
    let bucket_blocks = 64;
    let heat = HeatMap::new(env.allocator.total_block_count(), bucket_blocks);
    let (tx, rx) = bounded::<onyx_storage::dedup::ColdTailTarget>(64);
    let cfg = GcConfig {
        enabled: false,
        scan_interval_ms: 30,
        heat_enabled: true,
        heat_bucket_size_blocks: bucket_blocks,
        heat_refresh_max_lbas_per_cycle: 1000,
        heat_fold_cold_tail_enabled: true,
        heat_fold_push_max_per_cycle: 64,
        ..Default::default()
    };
    let mut gc = GcRunner::start_with_metrics(
        metrics.clone(),
        env.meta.clone(),
        env.io_engine.clone(),
        env.pool.clone(),
        env.lifecycle.clone(),
        env.allocator.clone(),
        heat.clone(),
        None, // ref_bitmap: Stage-5 per-PBA orphan reclaim off in this test
        Some(tx),
        onyx_storage::space::pba_lifecycle::PbaLifecycle::new(
            env.allocator.clone(),
            onyx_storage::dedup::CandidateCache::new(1, 1),
            metrics.clone(),
        ),
        cfg,
    );

    // Within a few cycles the heat walk should emit our 3 live entries.
    let mut got = 0usize;
    for _ in 0..50 {
        thread::sleep(Duration::from_millis(20));
        while let Ok(t) = rx.try_recv() {
            assert_eq!(t.vol_id.0, "fold-vol");
            got += 1;
        }
        if got >= 3 {
            break;
        }
    }
    gc.stop();

    assert!(
        got >= 3,
        "fold producer should emit >=3 cold candidates from 3 live mappings, got {got}"
    );
    assert!(
        metrics.gc_heat_cold_tail_pushed.load(Ordering::Relaxed) >= 3,
        "gc_heat_cold_tail_pushed should count the emitted candidates"
    );
}

/// With the refresh budget at 0 the heat map stays untouched and no heat
/// metrics move — the Stage-A observe-only / disabled no-op guarantee.
#[test]
fn heat_refresh_budget_zero_is_noop() {
    let env = setup_gc_env();
    let vol = VolumeId("heat-noop".into());
    register_volume(&env.meta, "heat-noop", CompressionAlgo::None, 1);
    env.meta.create_blockmap_cf("heat-noop").unwrap();
    put_live(&env.meta, &vol, 0, 100);
    put_live(&env.meta, &vol, 1, 101);

    let metrics = Arc::new(EngineMetrics::default());
    let heat = HeatMap::new(env.allocator.total_block_count(), 64);
    let cfg = GcConfig {
        enabled: false,
        scan_interval_ms: 20,
        heat_enabled: true,
        heat_bucket_size_blocks: 64,
        heat_refresh_max_lbas_per_cycle: 0, // disabled — gate skips the step
        ..Default::default()
    };
    let mut gc = GcRunner::start_with_metrics(
        metrics.clone(),
        env.meta.clone(),
        env.io_engine.clone(),
        env.pool.clone(),
        env.lifecycle.clone(),
        env.allocator.clone(),
        heat.clone(),
        None, // ref_bitmap: Stage-5 per-PBA orphan reclaim off in this test
        None, // cold_tail_tx: Stage-4 fold off in this test
        onyx_storage::space::pba_lifecycle::PbaLifecycle::new(
            env.allocator.clone(),
            onyx_storage::dedup::CandidateCache::new(1, 1),
            metrics.clone(),
        ),
        cfg,
    );
    thread::sleep(Duration::from_millis(120)); // several cycles
    gc.stop();

    use std::sync::atomic::Ordering;
    assert_eq!(metrics.heat_refresh_cycles.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.heat_bumps.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.heat_sweeps_completed.load(Ordering::Relaxed), 0);
    assert_eq!(heat.summary().nonzero_buckets, 0);
    assert_eq!(heat.current_epoch(), 1); // unchanged
}

// ---------- Slot-aware compaction (slot-evac) selection tests ----------
//
// These prove the SELECTION logic only (the rewriter and the retire/free path
// are unchanged and covered by gc_rewrite_packed_fragment +
// prove_background_gc_runner_reclaims_old_units). The scanner reads rc via
// multi_get_refcounts; set_refcount seeds it deterministically. The slot-evac
// path is driven by the SlotEvacParams.enabled flag passed here directly — the
// runtime `&& rc_authoritative_reclaim()` gate lives in the GcRunner, not the
// scanner.

fn slot_evac_meta() -> (tempfile::TempDir, MetaStore) {
    let dir = tempdir().unwrap();
    let meta_config = MetaConfig {
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
        ..MetaConfig::default()
    };
    let meta = MetaStore::open(&meta_config).unwrap();
    (dir, meta)
}

#[allow(clippy::too_many_arguments)]
fn put_frag(
    meta: &MetaStore,
    vol: &VolumeId,
    lba: u64,
    pba: Pba,
    slot_offset: u16,
    comp_size: u32,
    lba_count: u16,
    offset_in_unit: u16,
    crc: u32,
) {
    meta.put_mapping(
        vol,
        Lba(lba),
        &BlockmapValue {
            pba,
            compression: 1,
            unit_compressed_size: comp_size,
            // metadb v5 leaf compaction requires unit_original_size == lba_count * 4096.
            unit_original_size: u32::from(lba_count) * 4096,
            unit_lba_count: lba_count,
            offset_in_unit,
            crc32: crc,
            slot_offset,
            flags: 0,
        },
    )
    .unwrap();
}

fn se_params(max_live: u16) -> SlotEvacParams {
    SlotEvacParams {
        enabled: true,
        max_live,
        block_size: 4096,
    }
}

/// Invisibility fix + happy path: a mostly-byte-dead packed slot pinned by a
/// single-LBA live fragment is invisible to the legacy path but promoted by
/// slot-evac when the window sees all live refs (rc == visible).
#[test]
fn slot_evac_promotes_single_lba_packed_slot_when_rc_matches() {
    let (_dir, meta) = slot_evac_meta();
    let vol = VolumeId("vol-se".into());
    meta.create_blockmap_cf("vol-se").unwrap();
    let pba = Pba(200);
    // one live single-LBA fragment, 300 compressed bytes → ~93% byte-dead slot
    put_frag(&meta, &vol, 0, pba, 0, 300, 1, 0, 0x1111);
    meta.set_refcount(pba, 1).unwrap();

    // Slot-evac OFF: single-LBA fragment is invisible to compaction.
    let (cands_off, _de, stats_off, _ds) = scan_gc_candidates_window(
        &meta,
        &vol,
        Lba(0),
        64,
        0.25,
        100,
        SlotEvacParams::disabled(),
        &DefragScanParams::disabled(),
    )
    .unwrap();
    assert!(
        cands_off.is_empty(),
        "single-LBA fragment must be invisible when slot-evac is off"
    );
    assert_eq!(stats_off.candidates, 0);

    // Slot-evac ON: the slot is promoted (rc==visible==1, byte-dead >= 0.25).
    let (cands_on, _de2, stats_on, _ds2) = scan_gc_candidates_window(
        &meta,
        &vol,
        Lba(0),
        64,
        0.25,
        100,
        se_params(16),
        &DefragScanParams::disabled(),
    )
    .unwrap();
    assert_eq!(cands_on.len(), 1, "slot-evac promotes the pinning fragment");
    assert_eq!(cands_on[0].pba, pba);
    assert_eq!(stats_on.candidates, 1);
    assert_eq!(stats_on.blocks, 1);
    assert_eq!(stats_on.incomplete_skips, 0);
}

/// Completeness gate: if rc(P) > the live refs seen in this window (a sibling
/// fragment lives elsewhere), the slot is deferred, not evacuated.
#[test]
fn slot_evac_defers_when_window_misses_live_siblings() {
    let (_dir, meta) = slot_evac_meta();
    let vol = VolumeId("vol-se".into());
    meta.create_blockmap_cf("vol-se").unwrap();
    let pba = Pba(201);
    put_frag(&meta, &vol, 0, pba, 0, 300, 1, 0, 0x2222);
    // rc=3 simulates two live siblings outside this window/volume.
    meta.set_refcount(pba, 3).unwrap();

    let (cands, _de, stats, _ds) = scan_gc_candidates_window(
        &meta,
        &vol,
        Lba(0),
        64,
        0.25,
        100,
        se_params(16),
        &DefragScanParams::disabled(),
    )
    .unwrap();
    assert!(cands.is_empty(), "incomplete view must not evacuate");
    assert_eq!(stats.candidates, 0);
    assert_eq!(stats.incomplete_skips, 1);
}

/// ROI gate: a byte-full slot (live data nearly fills the 4 KiB) is not
/// evacuated even with rc==visible — relocating it would gain ~nothing.
#[test]
fn slot_evac_skips_byte_full_slot() {
    let (_dir, meta) = slot_evac_meta();
    let vol = VolumeId("vol-se".into());
    meta.create_blockmap_cf("vol-se").unwrap();
    let pba = Pba(202);
    // 4000 / 4096 bytes live → only ~2% byte-dead.
    put_frag(&meta, &vol, 0, pba, 0, 4000, 1, 0, 0x3333);
    meta.set_refcount(pba, 1).unwrap();

    let (cands, _de, stats, _ds) = scan_gc_candidates_window(
        &meta,
        &vol,
        Lba(0),
        64,
        0.25,
        100,
        se_params(16),
        &DefragScanParams::disabled(),
    )
    .unwrap();
    assert!(cands.is_empty(), "byte-full slot must not be evacuated");
    assert_eq!(stats.candidates, 0);
    // Failed the byte-deadness gate, not the completeness gate.
    assert_eq!(stats.incomplete_skips, 0);
}

/// Multi-fragment slot promoted whole, and a fragment that ALSO qualifies under
/// the per-fragment dead-ratio path is emitted exactly once (dedup).
#[test]
fn slot_evac_whole_slot_promote_dedups_against_per_fragment() {
    let (_dir, meta) = slot_evac_meta();
    let vol = VolumeId("vol-se".into());
    meta.create_blockmap_cf("vol-se").unwrap();
    let pba = Pba(203);
    // Frag A: 4-LBA unit, 1 live → dead_ratio 0.75 (also a per-fragment candidate).
    put_frag(&meta, &vol, 0, pba, 0, 300, 4, 0, 0xAAAA);
    // Frag B: single-LBA, 1 live → dead_ratio 0 (per-fragment never picks it).
    put_frag(&meta, &vol, 10, pba, 300, 200, 1, 0, 0xBBBB);
    // visible_live = 2, visible_bytes = 500 → ~88% byte-dead.
    meta.set_refcount(pba, 2).unwrap();

    let (cands, _de, stats, _ds) = scan_gc_candidates_window(
        &meta,
        &vol,
        Lba(0),
        64,
        0.25,
        100,
        se_params(16),
        &DefragScanParams::disabled(),
    )
    .unwrap();
    assert_eq!(cands.len(), 2, "whole slot promoted, no duplicate fragment");
    assert_eq!(
        cands.iter().filter(|c| c.slot_offset == 0).count(),
        1,
        "fragment A appears exactly once"
    );
    assert_eq!(
        cands.iter().filter(|c| c.slot_offset == 300).count(),
        1,
        "fragment B appears exactly once"
    );
    assert_eq!(stats.candidates, 2);
    assert_eq!(stats.blocks, 2);
}

/// Whole-slot atomicity: a slot whose fragment count exceeds the remaining
/// budget is deferred entirely, never partially evacuated (a partial evac never
/// reaches rc→0, so it would be wasted IO).
#[test]
fn slot_evac_does_not_partially_evacuate_a_slot_over_budget() {
    let (_dir, meta) = slot_evac_meta();
    let vol = VolumeId("vol-se".into());
    meta.create_blockmap_cf("vol-se").unwrap();
    let pba = Pba(204);
    // 3 single-LBA fragments in one byte-sparse slot (300 bytes each → ~78% dead).
    put_frag(&meta, &vol, 0, pba, 0, 300, 1, 0, 0xC001);
    put_frag(&meta, &vol, 1, pba, 300, 300, 1, 0, 0xC002);
    put_frag(&meta, &vol, 2, pba, 600, 300, 1, 0, 0xC003);
    meta.set_refcount(pba, 3).unwrap();

    // Budget of 2 < the slot's 3 fragments → must NOT partially evacuate.
    let (cands, _de, stats, _ds) = scan_gc_candidates_window(
        &meta,
        &vol,
        Lba(0),
        64,
        0.25,
        2,
        se_params(16),
        &DefragScanParams::disabled(),
    )
    .unwrap();
    assert!(
        cands.is_empty(),
        "a slot is all-or-nothing within the budget"
    );
    assert_eq!(stats.candidates, 0);
    // Passed completeness + byte-deadness; only the budget blocked it, so it is
    // NOT counted as an incomplete skip — it retries next cycle.
    assert_eq!(stats.incomplete_skips, 0);
}

/// Cost cap: a slot pinned by more live blocks than `max_live` is not evacuated
/// (too expensive to relocate), and that is a completeness-distinct skip.
#[test]
fn slot_evac_skips_slot_over_max_live() {
    let (_dir, meta) = slot_evac_meta();
    let vol = VolumeId("vol-se".into());
    meta.create_blockmap_cf("vol-se").unwrap();
    let pba = Pba(205);
    // 3 single-LBA live fragments, byte-sparse slot, rc==visible==3.
    put_frag(&meta, &vol, 0, pba, 0, 300, 1, 0, 0xD001);
    put_frag(&meta, &vol, 1, pba, 300, 300, 1, 0, 0xD002);
    put_frag(&meta, &vol, 2, pba, 600, 300, 1, 0, 0xD003);
    meta.set_refcount(pba, 3).unwrap();

    // max_live = 2 < 3 live → complete view (rc==visible==3) but too costly →
    // a COST-CAP skip, NOT a completeness/incomplete skip.
    let (cands, _de, stats, _ds) = scan_gc_candidates_window(
        &meta,
        &vol,
        Lba(0),
        64,
        0.25,
        100,
        se_params(2),
        &DefragScanParams::disabled(),
    )
    .unwrap();
    assert!(
        cands.is_empty(),
        "slot exceeding max_live must not be evacuated"
    );
    assert_eq!(stats.candidates, 0);
    assert_eq!(
        stats.cost_cap_skips, 1,
        "complete-but-too-costly is a cost-cap skip"
    );
    assert_eq!(stats.incomplete_skips, 0, "not an incomplete-view skip");
}

// ---------- Defrag bypass (physical-neighborhood compaction) selection tests ----------
//
// SELECTION only, like the slot-evac tests: the runner-side trigger/target walk
// is covered by src/gc/defrag.rs unit tests; here we prove the scanner promotes
// in-target fragments the dead-ratio path can never see, respects the block
// budget, and never double-emits against slot-evac.

fn defrag_params(
    targets: Vec<onyx_storage::space::extent::Extent>,
    max_blocks: u64,
) -> DefragScanParams {
    DefragScanParams {
        targets: std::sync::Arc::new(targets),
        max_blocks,
    }
}

fn ext(start: u64, count: u32) -> onyx_storage::space::extent::Extent {
    onyx_storage::space::extent::Extent::new(Pba(start), count)
}

/// A fully-live single-LBA unit (dead_ratio 0, skipped outright by the legacy
/// path even at threshold 0) inside a defrag target IS promoted, flagged
/// `defrag: true`; an identical unit outside the target stays invisible.
#[test]
fn defrag_bypass_promotes_in_target_fully_live_fragment() {
    let (_dir, meta) = slot_evac_meta();
    let vol = VolumeId("vol-df".into());
    meta.create_blockmap_cf("vol-df").unwrap();
    put_frag(&meta, &vol, 0, Pba(100), 0, 4096, 1, 0, 0xA001); // in target
    put_frag(&meta, &vol, 1, Pba(500), 0, 4096, 1, 0, 0xA002); // out of target

    let dp = defrag_params(vec![ext(90, 20)], 1024);
    let (cands, _de, _ss, ds) = scan_gc_candidates_window(
        &meta,
        &vol,
        Lba(0),
        64,
        0.85,
        100,
        SlotEvacParams::disabled(),
        &dp,
    )
    .unwrap();
    assert_eq!(cands.len(), 1, "only the in-target fragment is promoted");
    assert_eq!(cands[0].pba, Pba(100));
    assert!(cands[0].defrag);
    assert_eq!(cands[0].live_lbas.len(), 1);
    assert_eq!(ds.candidates, 1);
    assert_eq!(ds.blocks_selected, 1);

    // No targets → the bypass never fires (byte-identical legacy behavior).
    let (cands_off, _de2, _ss2, ds_off) = scan_gc_candidates_window(
        &meta,
        &vol,
        Lba(0),
        64,
        0.85,
        100,
        SlotEvacParams::disabled(),
        &DefragScanParams::disabled(),
    )
    .unwrap();
    assert!(cands_off.is_empty());
    assert_eq!(ds_off.candidates, 0);
}

/// Block budget: selection is highest-PBA-first (matching the descending
/// target walk) and stops once Σ live blocks reaches the budget.
#[test]
fn defrag_block_budget_caps_selection_descending() {
    let (_dir, meta) = slot_evac_meta();
    let vol = VolumeId("vol-df".into());
    meta.create_blockmap_cf("vol-df").unwrap();
    for i in 0..5u64 {
        put_frag(
            &meta,
            &vol,
            i,
            Pba(100 + i),
            0,
            4096,
            1,
            0,
            0xB000 + i as u32,
        );
    }
    let dp = defrag_params(vec![ext(100, 5)], 2);
    let (cands, _de, _ss, ds) = scan_gc_candidates_window(
        &meta,
        &vol,
        Lba(0),
        64,
        0.85,
        100,
        SlotEvacParams::disabled(),
        &dp,
    )
    .unwrap();
    assert_eq!(
        ds.candidates, 2,
        "budget of 2 blocks admits 2 single-LBA units"
    );
    assert_eq!(ds.blocks_selected, 2);
    let pbas: Vec<u64> = cands.iter().map(|c| c.pba.0).collect();
    assert_eq!(pbas, vec![104, 103], "highest PBA first");
    assert!(cands.iter().all(|c| c.defrag));
}

/// A multi-PBA passthrough unit whose FOOTPRINT overlaps a target (base PBA
/// outside it) is still promoted — the check uses physical_blocks, not just
/// the base address.
#[test]
fn defrag_footprint_overlap_catches_multi_pba_units() {
    let (_dir, meta) = slot_evac_meta();
    let vol = VolumeId("vol-df".into());
    meta.create_blockmap_cf("vol-df").unwrap();
    // 2-block raw unit at [100, 102); target covers only block 101.
    put_frag(&meta, &vol, 0, Pba(100), 0, 8192, 2, 0, 0xC001);
    put_frag(&meta, &vol, 1, Pba(100), 0, 8192, 2, 1, 0xC001);
    let dp = defrag_params(vec![ext(101, 1)], 1024);
    let (cands, _de, _ss, ds) = scan_gc_candidates_window(
        &meta,
        &vol,
        Lba(0),
        64,
        0.85,
        100,
        SlotEvacParams::disabled(),
        &dp,
    )
    .unwrap();
    assert_eq!(ds.candidates, 1);
    assert_eq!(cands.len(), 1);
    assert_eq!(cands[0].pba, Pba(100));
    assert!(cands[0].defrag);
    assert_eq!(cands[0].live_lbas.len(), 2, "both live members captured");
}

/// Coexistence with slot-evac: a defrag-selected fragment leaves the
/// accumulator before the slot-evac/legacy pass runs — never emitted twice.
#[test]
fn defrag_and_slot_evac_never_double_emit() {
    let (_dir, meta) = slot_evac_meta();
    let vol = VolumeId("vol-df".into());
    meta.create_blockmap_cf("vol-df").unwrap();
    let pba = Pba(300);
    // Byte-dead packed slot that slot-evac WOULD promote (rc==visible==1).
    put_frag(&meta, &vol, 0, pba, 0, 300, 1, 0, 0xD001);
    meta.set_refcount(pba, 1).unwrap();

    let dp = defrag_params(vec![ext(295, 10)], 1024);
    let (cands, _de, ss, ds) =
        scan_gc_candidates_window(&meta, &vol, Lba(0), 64, 0.25, 100, se_params(16), &dp).unwrap();
    assert_eq!(cands.len(), 1, "exactly one emission for the fragment");
    assert!(cands[0].defrag, "defrag takes precedence (extracted first)");
    assert_eq!(ds.candidates, 1);
    assert_eq!(
        ss.candidates, 0,
        "slot-evac must not re-emit the taken fragment"
    );
}
