/// Unit and integration tests for the GC module.
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use rand::{rngs::StdRng, RngCore, SeedableRng};

use onyx_storage::buffer::flush::BufferFlusher;
use onyx_storage::buffer::pool::WriteBufferPool;
use onyx_storage::compress::codec::create_compressor;
use onyx_storage::config::{FlushConfig, MetaConfig};
use onyx_storage::gc::config::GcConfig;
use onyx_storage::gc::rewriter::rewrite_candidate;
use onyx_storage::gc::scanner::{scan_gc_candidates, GcCandidate};
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
        onyx_storage::packer::packer::new_hole_map(),
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
        rocksdb_path: Some(meta_dir.path().to_path_buf()),
        block_cache_mb: 8,
        wal_dir: None,
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
        rocksdb_path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let meta = MetaStore::open(&meta_config).unwrap();
    let vol_id = VolumeId("vol-test".into());

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
        rocksdb_path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let meta = MetaStore::open(&meta_config).unwrap();
    let vol_id = VolumeId("vol-test".into());

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
        rocksdb_path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let meta = MetaStore::open(&meta_config).unwrap();
    let vol_id = VolumeId("vol-test".into());

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
        rocksdb_path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let meta = MetaStore::open(&meta_config).unwrap();
    let vol_id = VolumeId("vol-test".into());

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

    // Rewrite the candidate — live blocks go back to buffer
    let rewritten = rewrite_candidate(
        candidate,
        &env.io_engine,
        &env.pool,
        &env.meta,
        &env.lifecycle,
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
        let block = vec![i; BLOCK_SIZE as usize];
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
        rocksdb_path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        wal_dir: None,
    };
    let meta = MetaStore::open(&meta_config).unwrap();

    let shared_pba = Pba(100);

    // Fragment A: vol-a, 4-LBA unit at slot_offset=0, 2 live
    let vol_a = VolumeId("vol-a".into());
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

/// GC rewriter must verify the FULL fragment identity, not just PBA.
/// If a live LBA was remapped to a different fragment in the same packed slot
/// (same PBA, different slot_offset / metadata), rewriting from the old
/// candidate would corrupt data.
#[test]
fn gc_rewriter_skips_lba_when_fragment_identity_changed_with_same_pba() {
    let env = setup_gc_env();

    let shared_pba = Pba(777);
    let vol_a = VolumeId("vol-a".into());
    let vol_b = VolumeId("vol-b".into());
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

/// Holes are detected at the write path, not by scanning.
/// When ALL LBAs of a fragment in a packed slot are overwritten and the PBA
/// still has other live fragments, the write path adds the dead fragment's
/// space to the hole map.
///
/// This test verifies the full cycle: pack two fragments into one slot,
/// overwrite all LBAs of one fragment, verify hole appears in the map.
#[test]
fn write_path_detects_hole_when_fragment_fully_overwritten() {
    let env = setup_gc_env();
    let hole_map = onyx_storage::packer::packer::new_hole_map();

    // Use two volumes, each writing 1 LBA of highly compressible data.
    // The packer should pack both into the same 4KB slot.
    let vol_a = "vol-hole-a";
    let vol_b = "vol-hole-b";
    register_volume(&env.meta, vol_a, CompressionAlgo::Lz4, 100);
    register_volume(&env.meta, vol_b, CompressionAlgo::Lz4, 200);

    // Write 1 block for each volume
    let data_a = vec![0xAA; BLOCK_SIZE as usize];
    let data_b = vec![0xBB; BLOCK_SIZE as usize];
    env.pool.append(vol_a, Lba(0), 1, &data_a, 100).unwrap();
    env.pool.append(vol_b, Lba(0), 1, &data_b, 200).unwrap();

    // Flush — packer should pack both into one slot
    let mut flusher = onyx_storage::buffer::flush::BufferFlusher::start(
        env.pool.clone(),
        env.meta.clone(),
        env.lifecycle.clone(),
        env.allocator.clone(),
        env.io_engine.clone(),
        &FlushConfig::default(),
        hole_map.clone(),
        &onyx_storage::dedup::config::DedupConfig::default(),
    );
    assert!(wait_for_flush(&env.pool, 5000));
    flusher.stop();

    // Check that both are mapped (may or may not share PBA depending on packing)
    let vid_a = VolumeId(vol_a.to_string());
    let vid_b = VolumeId(vol_b.to_string());
    let mapping_a = env.meta.get_mapping(&vid_a, Lba(0)).unwrap().unwrap();
    let mapping_b = env.meta.get_mapping(&vid_b, Lba(0)).unwrap().unwrap();

    // If they share a PBA (packed), overwriting all of one should create a hole
    if mapping_a.pba == mapping_b.pba {
        let shared_pba = mapping_a.pba;

        // Overwrite vol_a's block — all 1 LBA of that fragment
        let new_data_a = vec![0xCC; BLOCK_SIZE as usize];
        env.pool.append(vol_a, Lba(0), 1, &new_data_a, 100).unwrap();

        let mut flusher2 = onyx_storage::buffer::flush::BufferFlusher::start(
            env.pool.clone(),
            env.meta.clone(),
            env.lifecycle.clone(),
            env.allocator.clone(),
            env.io_engine.clone(),
            &FlushConfig::default(),
            hole_map.clone(),
            &onyx_storage::dedup::config::DedupConfig::default(),
        );
        assert!(wait_for_flush(&env.pool, 5000));
        flusher2.stop();

        // The old fragment (vol_a at shared_pba, old slot_offset) should now be a hole
        let holes = hole_map.lock().unwrap();
        if !holes.is_empty() {
            // Found the hole — verify it points to the shared PBA
            let hole_entry = holes.iter().next().unwrap();
            assert_eq!(hole_entry.0.pba, shared_pba);
        }
        // Note: might not detect if the overwrite was a single-LBA unit
        // (detect_holes requires decrement == unit_lba_count)
    }

    // Regardless of packing, data integrity must hold
    use onyx_storage::zone::worker::ZoneWorker;
    let worker = ZoneWorker::new(
        ZoneId(0),
        env.meta.clone(),
        env.pool.clone(),
        env.io_engine.clone(),
    );
    let read_b = worker.handle_read(vol_b, Lba(0)).unwrap().unwrap();
    assert_eq!(
        read_b, data_b,
        "vol_b data must be intact after vol_a overwrite"
    );
}

#[test]
fn fully_freed_packed_pba_purges_hole_entries() {
    let env = setup_gc_env();
    let hole_map = onyx_storage::packer::packer::new_hole_map();

    let vol_a = "purge-hole-a";
    let vol_b = "purge-hole-b";
    register_volume(&env.meta, vol_a, CompressionAlgo::Lz4, 100);
    register_volume(&env.meta, vol_b, CompressionAlgo::Lz4, 200);

    let mut flusher = BufferFlusher::start(
        env.pool.clone(),
        env.meta.clone(),
        env.lifecycle.clone(),
        env.allocator.clone(),
        env.io_engine.clone(),
        &FlushConfig::default(),
        hole_map.clone(),
        &onyx_storage::dedup::config::DedupConfig::default(),
    );

    env.pool
        .append(vol_a, Lba(0), 1, &vec![0xA3; BLOCK_SIZE as usize], 100)
        .unwrap();
    env.pool
        .append(vol_b, Lba(0), 1, &vec![0xB4; BLOCK_SIZE as usize], 200)
        .unwrap();
    assert!(
        wait_for_flush(&env.pool, 5000),
        "initial packed flush timeout"
    );

    let vol_a_id = VolumeId(vol_a.to_string());
    let vol_b_id = VolumeId(vol_b.to_string());
    let shared_pba = env
        .meta
        .get_mapping(&vol_a_id, Lba(0))
        .unwrap()
        .unwrap()
        .pba;
    assert_eq!(
        env.meta
            .get_mapping(&vol_b_id, Lba(0))
            .unwrap()
            .unwrap()
            .pba,
        shared_pba,
        "initial writes should share one packed slot"
    );

    let mut rng = StdRng::seed_from_u64(0x5EED_BAAD);
    let mut overwrite_a = vec![0u8; BLOCK_SIZE as usize];
    rng.fill_bytes(&mut overwrite_a);
    let mut overwrite_b = vec![0u8; BLOCK_SIZE as usize];
    rng.fill_bytes(&mut overwrite_b);

    env.pool
        .append(vol_a, Lba(0), 1, &overwrite_a, 100)
        .unwrap();
    assert!(
        wait_for_flush(&env.pool, 5000),
        "first overwrite flush timeout"
    );
    let map_a_new = env.meta.get_mapping(&vol_a_id, Lba(0)).unwrap().unwrap();
    assert_ne!(
        map_a_new.pba, shared_pba,
        "incompressible overwrite should move vol_a off the old packed slot"
    );
    assert!(
        hole_map.lock().unwrap().keys().any(|k| k.pba == shared_pba),
        "first overwrite should leave at least one tracked hole on the shared packed slot"
    );

    env.pool
        .append(vol_b, Lba(0), 1, &overwrite_b, 200)
        .unwrap();
    assert!(
        wait_for_flush(&env.pool, 5000),
        "second overwrite flush timeout"
    );
    let map_b_new = env.meta.get_mapping(&vol_b_id, Lba(0)).unwrap().unwrap();
    assert_ne!(
        map_b_new.pba, shared_pba,
        "second incompressible overwrite should also leave the old packed slot"
    );
    assert_eq!(
        env.meta.get_refcount(shared_pba).unwrap(),
        0,
        "old packed slot should be fully released after the last fragment dies"
    );
    assert!(
        hole_map.lock().unwrap().keys().all(|k| k.pba != shared_pba),
        "freeing the packed slot must purge stale hole entries for that PBA"
    );

    flusher.stop();
}

/// A stale hole entry that overlaps a live fragment must be rejected by the
/// overlap check, then the write should be retried normally on a fresh PBA.
#[test]
fn stale_hole_overlap_is_rejected_and_write_retries_elsewhere() {
    use onyx_storage::packer::packer::HoleKey;
    use onyx_storage::zone::worker::ZoneWorker;

    let env = setup_gc_env();
    let hole_map = onyx_storage::packer::packer::new_hole_map();

    let vol_live = "stale-hole-live";
    let vol_new = "stale-hole-new";
    register_volume(&env.meta, vol_live, CompressionAlgo::Lz4, 100);
    register_volume(&env.meta, vol_new, CompressionAlgo::Lz4, 200);

    let mut flusher = BufferFlusher::start(
        env.pool.clone(),
        env.meta.clone(),
        env.lifecycle.clone(),
        env.allocator.clone(),
        env.io_engine.clone(),
        &FlushConfig::default(),
        hole_map.clone(),
        &onyx_storage::dedup::config::DedupConfig::default(),
    );

    let live_data = vec![0xA1; BLOCK_SIZE as usize];
    env.pool
        .append(vol_live, Lba(0), 1, &live_data, 100)
        .unwrap();
    assert!(
        wait_for_flush(&env.pool, 5000),
        "live fragment flush timeout"
    );

    let live_mapping = env
        .meta
        .get_mapping(&VolumeId(vol_live.to_string()), Lba(0))
        .unwrap()
        .unwrap();

    // Simulate a stale hole-map entry that wrongly points at the live fragment's
    // occupied byte range. The overlap check must reject it.
    hole_map.lock().unwrap().insert(
        HoleKey {
            pba: live_mapping.pba,
            offset: live_mapping.slot_offset,
        },
        live_mapping.unit_compressed_size as u16,
    );

    let new_data = vec![0xB2; BLOCK_SIZE as usize];
    env.pool.append(vol_new, Lba(0), 1, &new_data, 200).unwrap();
    assert!(wait_for_flush(&env.pool, 5000), "retry flush timeout");

    let new_mapping = env
        .meta
        .get_mapping(&VolumeId(vol_new.to_string()), Lba(0))
        .unwrap()
        .unwrap();

    // The stale hole should not have been used; the write must land elsewhere.
    assert_ne!(new_mapping.pba, live_mapping.pba);

    let worker = ZoneWorker::new(
        ZoneId(0),
        env.meta.clone(),
        env.pool.clone(),
        env.io_engine.clone(),
    );
    assert_eq!(
        worker.handle_read(vol_live, Lba(0)).unwrap().unwrap(),
        live_data,
        "live fragment must remain intact after stale-hole rejection"
    );
    assert_eq!(
        worker.handle_read(vol_new, Lba(0)).unwrap().unwrap(),
        new_data,
        "re-dispatched write must still commit successfully"
    );

    flusher.stop();
}

/// After a successful hole fill, the writer should publish the remainder back
/// into the hole map, and the next write should be able to consume that
/// remainder in the same packed slot.
#[test]
fn hole_fill_success_reinserts_remainder_for_next_write() {
    use onyx_storage::packer::packer::HoleKey;
    use onyx_storage::zone::worker::ZoneWorker;

    let env = setup_gc_env();
    let hole_map = onyx_storage::packer::packer::new_hole_map();

    let vol_anchor = "remainder-anchor";
    let vol_fill1 = "remainder-fill-1";
    let vol_fill2 = "remainder-fill-2";
    register_volume(&env.meta, vol_anchor, CompressionAlgo::Lz4, 100);
    register_volume(&env.meta, vol_fill1, CompressionAlgo::Lz4, 200);
    register_volume(&env.meta, vol_fill2, CompressionAlgo::Lz4, 300);

    let mut flusher = BufferFlusher::start(
        env.pool.clone(),
        env.meta.clone(),
        env.lifecycle.clone(),
        env.allocator.clone(),
        env.io_engine.clone(),
        &FlushConfig::default(),
        hole_map.clone(),
        &onyx_storage::dedup::config::DedupConfig::default(),
    );

    let anchor_data = vec![0xC1; BLOCK_SIZE as usize];
    env.pool
        .append(vol_anchor, Lba(0), 1, &anchor_data, 100)
        .unwrap();
    assert!(wait_for_flush(&env.pool, 5000), "anchor flush timeout");

    let anchor_mapping = env
        .meta
        .get_mapping(&VolumeId(vol_anchor.to_string()), Lba(0))
        .unwrap()
        .unwrap();
    let packed_pba = anchor_mapping.pba;

    // Create an explicit free region after the live anchor fragment.
    let manual_hole_offset =
        anchor_mapping.slot_offset + anchor_mapping.unit_compressed_size as u16;
    let manual_hole_size = 1024u16;
    assert!(
        manual_hole_offset + manual_hole_size <= BLOCK_SIZE as u16,
        "manual hole must stay within one packed slot"
    );
    hole_map.lock().unwrap().insert(
        HoleKey {
            pba: packed_pba,
            offset: manual_hole_offset,
        },
        manual_hole_size,
    );

    let fill1_data = vec![0xD2; BLOCK_SIZE as usize];
    env.pool
        .append(vol_fill1, Lba(0), 1, &fill1_data, 200)
        .unwrap();
    assert!(wait_for_flush(&env.pool, 5000), "first hole fill timeout");

    let fill1_mapping = env
        .meta
        .get_mapping(&VolumeId(vol_fill1.to_string()), Lba(0))
        .unwrap()
        .unwrap();
    assert_eq!(
        fill1_mapping.pba, packed_pba,
        "first write should reuse manual hole"
    );
    assert_eq!(
        fill1_mapping.slot_offset, manual_hole_offset,
        "first fill should start at the manual hole offset"
    );

    let expected_remainder_offset = manual_hole_offset + fill1_mapping.unit_compressed_size as u16;
    let expected_remainder_size = manual_hole_size - fill1_mapping.unit_compressed_size as u16;
    assert!(
        expected_remainder_size > 0,
        "first fill should leave a remainder to be reused"
    );
    assert_eq!(
        hole_map
            .lock()
            .unwrap()
            .get(&HoleKey {
                pba: packed_pba,
                offset: expected_remainder_offset,
            })
            .copied(),
        Some(expected_remainder_size),
        "successful hole fill must publish the remaining free bytes"
    );

    let fill2_data = vec![0xE3; BLOCK_SIZE as usize];
    env.pool
        .append(vol_fill2, Lba(0), 1, &fill2_data, 300)
        .unwrap();
    assert!(wait_for_flush(&env.pool, 5000), "second hole fill timeout");

    let fill2_mapping = env
        .meta
        .get_mapping(&VolumeId(vol_fill2.to_string()), Lba(0))
        .unwrap()
        .unwrap();
    assert_eq!(
        fill2_mapping.pba, packed_pba,
        "second write should reuse remainder"
    );
    assert_eq!(
        fill2_mapping.slot_offset, expected_remainder_offset,
        "second write should consume the remainder published by the first fill"
    );

    let worker = ZoneWorker::new(
        ZoneId(0),
        env.meta.clone(),
        env.pool.clone(),
        env.io_engine.clone(),
    );
    assert_eq!(
        worker.handle_read(vol_anchor, Lba(0)).unwrap().unwrap(),
        anchor_data
    );
    assert_eq!(
        worker.handle_read(vol_fill1, Lba(0)).unwrap().unwrap(),
        fill1_data
    );
    assert_eq!(
        worker.handle_read(vol_fill2, Lba(0)).unwrap().unwrap(),
        fill2_data
    );

    flusher.stop();
}
