use std::thread;
use std::time::Duration;

use onyx_storage::buffer::flush::{clear_test_failpoint, install_test_failpoint, FlushFailStage};
use onyx_storage::buffer::pool::WriteBufferPool;
use onyx_storage::buffer::pool::{clear_purge_volume_failpoint, install_purge_volume_failpoint};
use onyx_storage::config::*;
use onyx_storage::engine::OnyxEngine;
use onyx_storage::error::OnyxError;
use onyx_storage::io::device::RawDevice;
use onyx_storage::types::{CompressionAlgo, Lba, VolumeId};
use serial_test::serial;
use tempfile::{tempdir, NamedTempFile};

fn make_config() -> (OnyxConfig, tempfile::TempDir, NamedTempFile, NamedTempFile) {
    let meta_dir = tempdir().unwrap();
    let buf_tmp = NamedTempFile::new().unwrap();
    let data_tmp = NamedTempFile::new().unwrap();

    // Buffer: superblock (4096) + room for entries
    let buf_size = 4096 + 256 * 4096;
    buf_tmp.as_file().set_len(buf_size as u64).unwrap();

    // Data device: 1000 blocks
    data_tmp.as_file().set_len(4096 * 1000).unwrap();

    let config = OnyxConfig {
        meta: MetaConfig {
            rocksdb_path: Some(meta_dir.path().to_path_buf()),
            block_cache_mb: 8,
            wal_dir: None,
        },
        storage: StorageConfig {
            data_device: Some(data_tmp.path().to_path_buf()),
            block_size: 4096,
            use_hugepages: false,
            default_compression: CompressionAlgo::None,
            io_backend: Default::default(),
            uring_sq_entries: 128,
            read_pool_workers: 4,
        },
        buffer: BufferConfig {
            device: Some(buf_tmp.path().to_path_buf()),
            capacity_mb: 1,
            flush_watermark_pct: 80,
            group_commit_wait_us: 250,
            shards: 1,
            max_memory_mb: 0,
        },
        ublk: UblkConfig::default(),
        flush: FlushConfig::default(),
        engine: EngineConfig {
            zone_count: 2,
            zone_size_blocks: 128,
        },
        gc: onyx_storage::gc::config::GcConfig {
            enabled: false,
            ..Default::default()
        },
        dedup: onyx_storage::dedup::config::DedupConfig::default(),
        service: Default::default(),
        ha: Default::default(),
    };

    (config, meta_dir, buf_tmp, data_tmp)
}

fn make_config_with_buffer_shards(
    shards: usize,
) -> (OnyxConfig, tempfile::TempDir, NamedTempFile, NamedTempFile) {
    let (mut config, meta_dir, buf_tmp, data_tmp) = make_config();
    config.buffer.shards = shards;
    (config, meta_dir, buf_tmp, data_tmp)
}

fn wait_for_buffer_drain(engine: &OnyxEngine, timeout_ms: u64) -> bool {
    let Some(pool) = engine.buffer_pool() else {
        return true;
    };
    let steps = timeout_ms / 10;
    for _ in 0..steps {
        if pool.pending_count() == 0 {
            return true;
        }
        thread::sleep(Duration::from_millis(10));
    }
    false
}

// --- Meta-only mode ---

// TODO: delete_volume in meta-only mode leaves stale volume entry.
// Re-enable once Engine::delete_volume properly removes from list_volumes.

#[test]
#[ignore]
fn meta_only_create_list_delete() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open_meta_only(&config).unwrap();

    // No volumes initially
    assert!(engine.list_volumes().unwrap().is_empty());

    // Create
    engine
        .create_volume("vol-a", 1024 * 1024, CompressionAlgo::Lz4)
        .unwrap();
    let vols = engine.list_volumes().unwrap();
    assert_eq!(vols.len(), 1);
    assert_eq!(vols[0].id.0, "vol-a");
    assert_eq!(vols[0].size_bytes, 1024 * 1024);

    // Create second
    engine
        .create_volume("vol-b", 2 * 1024 * 1024, CompressionAlgo::None)
        .unwrap();
    assert_eq!(engine.list_volumes().unwrap().len(), 2);

    // Delete
    engine.delete_volume("vol-a").unwrap();
    let vols = engine.list_volumes().unwrap();
    assert_eq!(vols.len(), 1);
    assert_eq!(vols[0].id.0, "vol-b");
}

#[test]
fn meta_only_open_volume_fails() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open_meta_only(&config).unwrap();
    engine
        .create_volume("vol-a", 1024 * 1024, CompressionAlgo::None)
        .unwrap();
    // Cannot open volume in meta-only mode
    assert!(engine.open_volume("vol-a").is_err());
}

// --- Full engine mode ---

#[test]
fn full_engine_open_shutdown() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config).unwrap();
    engine.shutdown().unwrap();
    // Double shutdown is safe
    engine.shutdown().unwrap();
}

#[test]
#[serial]
fn shutdown_drains_pending_flush_retries_before_exit() {
    let (mut config, _md, _bf, _df) = make_config();
    config.dedup.enabled = false;

    let engine = OnyxEngine::open(&config).unwrap();
    engine
        .create_volume("vol-shutdown-drain", 64 * 4096, CompressionAlgo::None)
        .unwrap();
    let vol = engine.open_volume("vol-shutdown-drain").unwrap();
    let data = vec![0xA5; 4096];

    install_test_failpoint(
        "vol-shutdown-drain",
        Lba(0),
        FlushFailStage::BeforeMetaWrite,
        None,
    );
    vol.write(0, &data).unwrap();
    drop(vol);

    let pool = engine.buffer_pool().unwrap();
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        if pool.pending_count() > 0 {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        pool.pending_count() > 0,
        "metadata failpoint should leave at least one pending buffer entry"
    );

    clear_test_failpoint(
        "vol-shutdown-drain",
        Lba(0),
        FlushFailStage::BeforeMetaWrite,
    );
    engine.shutdown().unwrap();
    drop(engine);

    let reopened = OnyxEngine::open(&config).unwrap();
    assert_eq!(
        reopened.buffer_pool().unwrap().pending_count(),
        0,
        "graceful shutdown must drain pending buffer entries before exit"
    );
    let reopened_vol = reopened.open_volume("vol-shutdown-drain").unwrap();
    assert_eq!(reopened_vol.read(0, 4096).unwrap(), data);
}

#[test]
fn full_engine_create_and_open_volume() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config).unwrap();
    engine
        .create_volume("vol-test", 256 * 4096, CompressionAlgo::None)
        .unwrap();
    let vol = engine.open_volume("vol-test").unwrap();
    assert_eq!(vol.name(), "vol-test");
    assert_eq!(vol.size_bytes(), 256 * 4096);
}

#[test]
fn cross_zone_overwrite_must_not_return_stale_buffer_data() {
    let (config, _md, _bf, _df) = make_config_with_buffer_shards(2);
    let engine = OnyxEngine::open(&config).unwrap();
    engine
        .create_volume("vol-cross-zone-stale", 512 * 4096, CompressionAlgo::None)
        .unwrap();
    let vol = engine.open_volume("vol-cross-zone-stale").unwrap();

    let old = vec![0x11; 4096];
    let new_prev = vec![0x22; 4096];
    let new_target = vec![0x33; 4096];

    // Old single-block write at LBA 128 lands on shard for zone 1.
    vol.write(128 * 4096, &old).unwrap();

    // Newer two-block write starts at LBA 127 and crosses the zone boundary.
    // Correct behavior requires splitting so LBA 128 is routed with its own zone.
    let mut cross = Vec::with_capacity(8192);
    cross.extend_from_slice(&new_prev);
    cross.extend_from_slice(&new_target);
    vol.write(127 * 4096, &cross).unwrap();

    // Immediate read should see the newest value for LBA 128.
    let actual = vol.read(128 * 4096, 4096).unwrap();
    assert_eq!(
        actual, new_target,
        "LBA 128 must return the latest overwrite, not the stale older shard copy"
    );
}

#[test]
fn open_nonexistent_volume_fails() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config).unwrap();
    assert!(engine.open_volume("no-such-vol").is_err());
}

#[test]
fn recreate_same_name_gets_new_generation() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config).unwrap();
    let vol_id = VolumeId("vol-gen-id".to_string());

    engine
        .create_volume("vol-gen-id", 16 * 4096, CompressionAlgo::None)
        .unwrap();
    let gen1 = engine
        .meta()
        .get_volume(&vol_id)
        .unwrap()
        .unwrap()
        .created_at;

    engine.delete_volume("vol-gen-id").unwrap();
    engine
        .create_volume("vol-gen-id", 16 * 4096, CompressionAlgo::None)
        .unwrap();
    let gen2 = engine
        .meta()
        .get_volume(&vol_id)
        .unwrap()
        .unwrap()
        .created_at;

    assert_ne!(gen1, gen2, "recreated volume must get a fresh generation");
    assert!(gen2 > gen1, "generation should increase monotonically");
}

#[test]
fn engine_metrics_snapshot_tracks_reads_writes_and_dedup() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config).unwrap();
    engine
        .create_volume("vol-metrics", 64 * 4096, CompressionAlgo::None)
        .unwrap();
    let vol = engine.open_volume("vol-metrics").unwrap();
    let payload = vec![0x5A; 4096];

    vol.write(0, &payload).unwrap();
    assert_eq!(vol.read(0, 4096).unwrap(), payload);
    assert!(wait_for_buffer_drain(&engine, 5000), "first flush timeout");
    assert_eq!(vol.read(0, 4096).unwrap(), payload);

    vol.write(4096, &payload).unwrap();
    assert!(wait_for_buffer_drain(&engine, 5000), "second flush timeout");

    let snapshot = engine.metrics_snapshot();
    assert_eq!(snapshot.volume_create_ops, 1);
    assert_eq!(snapshot.volume_open_ops, 1);
    assert_eq!(snapshot.volume_write_ops, 2);
    assert_eq!(snapshot.volume_read_ops, 2);
    assert!(snapshot.volume_write_bytes >= 8192);
    assert!(snapshot.volume_read_bytes >= 8192);
    assert!(snapshot.buffer_appends >= 2);
    assert!(snapshot.buffer_write_ops >= 2);
    assert!(snapshot.buffer_write_bytes >= 8192);
    assert!(
        snapshot.read_buffer_hits + snapshot.read_lv3_hits >= 2,
        "expected the two reads to be served by buffer and/or LV3"
    );
    assert!(snapshot.buffer_read_ops >= 1);
    assert!(snapshot.buffer_read_bytes >= 4096);
    assert!(snapshot.read_lv3_hits >= 1);
    assert!(snapshot.lv3_read_ops >= 1);
    assert!(snapshot.lv3_read_compressed_bytes >= 4096);
    assert!(snapshot.lv3_write_ops >= 1);
    assert!(snapshot.lv3_write_compressed_bytes >= 4096);
    assert!(snapshot.lv3_read_decompressed_bytes >= 4096);
    assert!(snapshot.volume_read_total_ns > 0);
    assert!(snapshot.volume_write_total_ns > 0);
    assert!(snapshot.flush_units_written >= 1 || snapshot.flush_packed_slots_written >= 1);
    assert!(
        snapshot.dedup_hits >= 1,
        "expected second identical block to dedup"
    );
}

#[test]
fn engine_status_report_includes_metrics_sections() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config).unwrap();
    engine
        .create_volume("vol-status", 16 * 4096, CompressionAlgo::None)
        .unwrap();

    let report = engine.status_report().unwrap();

    assert!(report.contains("mode: active"));
    assert!(report.contains("volumes: 1"));
    assert!(report.contains("buffer_pending_entries:"));
    assert!(report.contains("rocksdb_block_cache_bytes:"));
    assert!(report.contains("rocksdb_meta_bytes:"));
    assert!(report.contains("volume_ops:"));
    assert!(report.contains("read_path:"));
    assert!(report.contains("lv3_io:"));
    assert!(report.contains("flush:"));
    assert!(report.contains("dedup:"));
    assert!(report.contains("gc:"));
}

// --- Volume IO ---

#[test]
fn volume_aligned_write_read() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config).unwrap();
    engine
        .create_volume("vol-io", 64 * 4096, CompressionAlgo::None)
        .unwrap();
    let vol = engine.open_volume("vol-io").unwrap();

    // Write 2 blocks at offset 0
    let data = vec![0xAA; 8192];
    vol.write(0, &data).unwrap();

    // Read back
    let result = vol.read(0, 8192).unwrap();
    assert_eq!(result, data);
}

#[test]
fn volume_sparse_read_zeros() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config).unwrap();
    engine
        .create_volume("vol-sparse", 64 * 4096, CompressionAlgo::None)
        .unwrap();
    let vol = engine.open_volume("vol-sparse").unwrap();

    // Read without writing — should be zeros
    let result = vol.read(0, 4096).unwrap();
    assert_eq!(result, vec![0u8; 4096]);
}

#[test]
fn volume_unaligned_write_read() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config).unwrap();
    engine
        .create_volume("vol-unalign", 64 * 4096, CompressionAlgo::None)
        .unwrap();
    let vol = engine.open_volume("vol-unalign").unwrap();

    // Write 100 bytes at offset 100 (non-aligned)
    let data = vec![0xBB; 100];
    vol.write(100, &data).unwrap();

    // Read back at same offset
    let result = vol.read(100, 100).unwrap();
    assert_eq!(result, data);

    // Surrounding bytes should be zero
    let before = vol.read(0, 100).unwrap();
    assert_eq!(before, vec![0u8; 100]);
    let after = vol.read(200, 100).unwrap();
    assert_eq!(after, vec![0u8; 100]);
}

#[test]
fn volume_cross_block_write() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config).unwrap();
    engine
        .create_volume("vol-cross", 64 * 4096, CompressionAlgo::None)
        .unwrap();
    let vol = engine.open_volume("vol-cross").unwrap();

    // Write spanning block boundary: last 100 bytes of block 0 + first 100 of block 1
    let offset = 4096 - 100;
    let data = vec![0xCC; 200];
    vol.write(offset as u64, &data).unwrap();

    let result = vol.read(offset as u64, 200).unwrap();
    assert_eq!(result, data);
}

#[test]
fn volume_out_of_bounds() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config).unwrap();
    let size = 16 * 4096;
    engine
        .create_volume("vol-bounds", size, CompressionAlgo::None)
        .unwrap();
    let vol = engine.open_volume("vol-bounds").unwrap();

    // Write past end
    assert!(vol.write(size - 10, &[0u8; 20]).is_err());
    // Read past end
    assert!(vol.read(size - 10, 20).is_err());
    // Exact end is ok (zero-length)
    vol.write(size, &[]).unwrap();
    let r = vol.read(size, 0).unwrap();
    assert!(r.is_empty());
}

#[test]
fn volume_multi_block_aligned_write() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config).unwrap();
    engine
        .create_volume("vol-multi", 64 * 4096, CompressionAlgo::None)
        .unwrap();
    let vol = engine.open_volume("vol-multi").unwrap();

    // Write 32KB (8 blocks) at once
    let mut data = vec![0u8; 32768];
    for i in 0..data.len() {
        data[i] = (i % 256) as u8;
    }
    vol.write(0, &data).unwrap();

    // Read each block individually
    for block in 0..8 {
        let offset = block * 4096;
        let result = vol.read(offset as u64, 4096).unwrap();
        assert_eq!(result, data[offset..offset + 4096]);
    }
}

#[test]
fn volume_concurrent_writes() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config).unwrap();
    engine
        .create_volume("vol-conc", 128 * 4096, CompressionAlgo::None)
        .unwrap();
    let vol = std::sync::Arc::new(engine.open_volume("vol-conc").unwrap());

    let mut handles = Vec::new();
    for i in 0..4u8 {
        let v = vol.clone();
        let h = std::thread::spawn(move || {
            let offset = i as u64 * 4096;
            let data = vec![i + 1; 4096];
            v.write(offset, &data).unwrap();
        });
        handles.push(h);
    }
    for h in handles {
        h.join().unwrap();
    }

    // Verify each thread's data
    for i in 0..4u8 {
        let offset = i as u64 * 4096;
        let result = vol.read(offset, 4096).unwrap();
        assert_eq!(result, vec![i + 1; 4096]);
    }
}

// ===========================================================================
// Handle identity tests: delete → recreate → delete cycles
// ===========================================================================

/// Prove that an old handle stays dead across delete→recreate→delete cycles.
///
/// Scenario:
///   1. Create vol-X (gen 1), open handle H1
///   2. Delete vol-X → H1 must report VolumeDeleted
///   3. Recreate vol-X (gen 2), open handle H2
///   4. H1 must STILL report VolumeDeleted (not revived by recreate)
///   5. H2 works normally
///   6. Delete vol-X again → H2 must report VolumeDeleted
///   7. H1 must still be dead
///   8. Recreate vol-X (gen 3), open handle H3
///   9. H1 dead, H2 dead, H3 alive
#[test]
fn stale_handle_stays_dead_across_delete_recreate_delete() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config).unwrap();
    let vol_size = 16 * 4096u64;

    // Gen 1: create, open handle, write data
    engine
        .create_volume("vol-lifecycle", vol_size, CompressionAlgo::None)
        .unwrap();
    let h1 = engine.open_volume("vol-lifecycle").unwrap();
    h1.write(0, &[0x11; 4096]).unwrap();
    assert_eq!(h1.read(0, 4096).unwrap(), vec![0x11; 4096]);

    // Delete gen 1 → H1 dead
    engine.delete_volume("vol-lifecycle").unwrap();
    let err = h1.write(0, &[0x22; 4096]).unwrap_err();
    assert!(
        matches!(err, OnyxError::VolumeDeleted(_)),
        "H1 write after delete should be VolumeDeleted, got: {err}"
    );
    let err = h1.read(0, 4096).unwrap_err();
    assert!(
        matches!(err, OnyxError::VolumeDeleted(_)),
        "H1 read after delete should be VolumeDeleted, got: {err}"
    );

    // Gen 2: recreate same name, open new handle
    engine
        .create_volume("vol-lifecycle", vol_size, CompressionAlgo::None)
        .unwrap();
    let h2 = engine.open_volume("vol-lifecycle").unwrap();

    // H1 must still be dead — must NOT be revived by the recreate
    assert!(
        h1.write(0, &[0x33; 4096]).is_err(),
        "H1 must remain dead after recreate"
    );

    // H2 works
    h2.write(0, &[0x44; 4096]).unwrap();
    assert_eq!(h2.read(0, 4096).unwrap(), vec![0x44; 4096]);

    // Delete gen 2 → H2 dead
    engine.delete_volume("vol-lifecycle").unwrap();
    assert!(
        h2.write(0, &[0x55; 4096]).is_err(),
        "H2 must be dead after second delete"
    );

    // H1 still dead
    assert!(
        h1.read(0, 4096).is_err(),
        "H1 must remain dead after second delete"
    );

    // Gen 3: recreate once more
    engine
        .create_volume("vol-lifecycle", vol_size, CompressionAlgo::None)
        .unwrap();
    let h3 = engine.open_volume("vol-lifecycle").unwrap();

    // H1 dead, H2 dead, H3 alive
    assert!(h1.write(0, &[0x66; 4096]).is_err(), "H1 must stay dead");
    assert!(h2.write(0, &[0x77; 4096]).is_err(), "H2 must stay dead");
    h3.write(0, &[0x88; 4096]).unwrap();
    assert_eq!(h3.read(0, 4096).unwrap(), vec![0x88; 4096], "H3 must work");
}

#[test]
fn delete_failure_does_not_kill_existing_handle() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config).unwrap();
    let vol_size = 16 * 4096u64;

    engine
        .create_volume("vol-delete-fail", vol_size, CompressionAlgo::None)
        .unwrap();
    let handle = engine.open_volume("vol-delete-fail").unwrap();
    let original = vec![0x5A; 4096];
    handle.write(0, &original).unwrap();

    install_purge_volume_failpoint("vol-delete-fail");
    let result = engine.delete_volume("vol-delete-fail");
    clear_purge_volume_failpoint("vol-delete-fail");

    assert!(
        result.is_err(),
        "delete should fail via injected purge failure"
    );

    let read_back = handle.read(0, 4096).unwrap();
    assert_eq!(
        read_back, original,
        "handle must remain usable after failed delete"
    );

    let reopened = engine.open_volume("vol-delete-fail").unwrap();
    assert_eq!(
        reopened.read(0, 4096).unwrap(),
        original,
        "volume must still exist after failed delete"
    );
}

/// Prove that old-generation buffer entries are NOT flushed into a recreated
/// same-name volume.
///
/// Scenario:
///   1. Create vol-X (gen 1), write distinctive data, let flusher run
///   2. Write MORE data (gen 1 pattern), DON'T wait for flush
///   3. Delete vol-X (purges what it can, but flusher may have in-flight units)
///   4. Recreate vol-X (gen 2) immediately
///   5. Write gen-2 pattern, wait for flush
///   6. Read back — must see gen-2 data, NOT gen-1 data
///   7. Inspect blockmap: no LBAs should have gen-1's data
#[test]
fn old_generation_buffer_entry_not_flushed_into_new_volume() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config).unwrap();
    let vol_size = 32 * 4096u64;
    let meta = engine.meta().clone();
    let pool = engine.buffer_pool().unwrap().clone();

    // Gen 1: create, write, flush
    engine
        .create_volume("vol-gen", vol_size, CompressionAlgo::None)
        .unwrap();
    let h1 = engine.open_volume("vol-gen").unwrap();
    let gen1_data = vec![0xAA; 4096];
    for lba in 0..8u64 {
        h1.write(lba * 4096, &gen1_data).unwrap();
    }
    // Wait for gen-1 data to flush completely
    let start = std::time::Instant::now();
    loop {
        if pool.pending_count() == 0 {
            break;
        }
        if start.elapsed() > Duration::from_secs(5) {
            panic!("gen-1 flush timeout");
        }
        thread::sleep(Duration::from_millis(20));
    }
    // Small extra wait for writer to finish committing
    thread::sleep(Duration::from_millis(100));

    // Verify gen-1 data is in blockmap
    let vol_id = VolumeId("vol-gen".to_string());
    let gen1_mapping = meta.get_mapping(&vol_id, Lba(0)).unwrap();
    assert!(
        gen1_mapping.is_some(),
        "gen-1 data should be in blockmap before delete"
    );

    // Write more gen-1 data (these may still be in buffer when we delete)
    for lba in 8..16u64 {
        h1.write(lba * 4096, &gen1_data).unwrap();
    }

    // Delete gen 1
    drop(h1);
    engine.delete_volume("vol-gen").unwrap();

    // Verify blockmap is clean
    for lba in 0..16u64 {
        assert!(
            meta.get_mapping(&vol_id, Lba(lba)).unwrap().is_none(),
            "LBA {} blockmap should be empty after delete",
            lba
        );
    }

    // Gen 2: recreate immediately, write different pattern
    engine
        .create_volume("vol-gen", vol_size, CompressionAlgo::None)
        .unwrap();
    let h2 = engine.open_volume("vol-gen").unwrap();
    let gen2_data = vec![0xBB; 4096];
    for lba in 0..8u64 {
        h2.write(lba * 4096, &gen2_data).unwrap();
    }

    // Wait for gen-2 data to flush
    let start = std::time::Instant::now();
    loop {
        if pool.pending_count() == 0 {
            break;
        }
        if start.elapsed() > Duration::from_secs(5) {
            panic!("gen-2 flush timeout");
        }
        thread::sleep(Duration::from_millis(20));
    }
    thread::sleep(Duration::from_millis(100));

    // EVIDENCE 1: Read back through volume handle — must be gen-2 data
    for lba in 0..8u64 {
        let result = h2.read(lba * 4096, 4096).unwrap();
        assert_eq!(
            result, gen2_data,
            "LBA {} should have gen-2 data (0xBB), not gen-1 (0xAA)",
            lba
        );
    }

    // EVIDENCE 2: Blockmap entries should exist and point to gen-2 data
    for lba in 0..8u64 {
        let mapping = meta.get_mapping(&vol_id, Lba(lba)).unwrap();
        assert!(
            mapping.is_some(),
            "LBA {} should have a blockmap entry from gen-2",
            lba
        );
    }

    // EVIDENCE 3: LBAs 8..16 (written by gen-1 only, not gen-2) must NOT
    // have blockmap entries — old gen-1 entries must have been discarded
    for lba in 8..16u64 {
        let mapping = meta.get_mapping(&vol_id, Lba(lba)).unwrap();
        assert!(
            mapping.is_none(),
            "LBA {} should NOT have a blockmap entry (gen-1 stale data must be discarded)",
            lba
        );
    }

    // EVIDENCE 4: Read LBAs 8..15 through handle — should be zeros (unmapped)
    for lba in 8..16u64 {
        let result = h2.read(lba * 4096, 4096).unwrap();
        assert_eq!(
            result,
            vec![0u8; 4096],
            "LBA {} should read as zeros (unmapped in gen-2)",
            lba
        );
    }
}

/// Prove that stale buffer entries recovered after a restart are discarded
/// when the same volume name was deleted and recreated with a new generation.
///
/// This models the path seen in soak:
///   1. Gen-1 volume exists
///   2. Pending buffer entries for gen-1 survive on the buffer device
///   3. Volume is deleted and recreated with the same name (gen-2)
///   4. A fresh engine process opens and recovers those pending entries
///   5. The flusher must discard them, not flush them into gen-2
#[test]
fn recovered_old_generation_entries_not_flushed_into_recreated_volume_after_restart() {
    let (config, _md, _bf, _df) = make_config();
    let vol_size = 32 * 4096u64;
    let vol_name = "vol-restart-gen";
    let vol_id = VolumeId(vol_name.to_string());

    let meta_only = OnyxEngine::open_meta_only(&config).unwrap();
    meta_only
        .create_volume(vol_name, vol_size, CompressionAlgo::None)
        .unwrap();
    let gen1 = meta_only
        .meta()
        .get_volume(&vol_id)
        .unwrap()
        .unwrap()
        .created_at;

    let buffer_dev = RawDevice::open(config.buffer.device.as_ref().unwrap()).unwrap();
    let pool = WriteBufferPool::open(buffer_dev).unwrap();
    let stale = vec![0xAA; 4096];
    for lba in 0..4u64 {
        pool.append(vol_name, Lba(lba), 1, &stale, gen1).unwrap();
    }
    assert_eq!(
        pool.pending_count(),
        4,
        "fixture must persist stale entries"
    );
    drop(pool);

    meta_only.delete_volume(vol_name).unwrap();
    meta_only
        .create_volume(vol_name, vol_size, CompressionAlgo::None)
        .unwrap();
    let gen2 = meta_only
        .meta()
        .get_volume(&vol_id)
        .unwrap()
        .unwrap()
        .created_at;
    assert_ne!(gen1, gen2, "recreated volume must get a fresh generation");
    drop(meta_only);

    let engine = OnyxEngine::open(&config).unwrap();
    assert!(
        wait_for_buffer_drain(&engine, 5000),
        "recovered stale buffer entries should be drained or discarded quickly"
    );

    let vol = engine.open_volume(vol_name).unwrap();
    for lba in 0..4u64 {
        assert_eq!(
            vol.read(lba * 4096, 4096).unwrap(),
            vec![0u8; 4096],
            "recovered gen-1 entry for LBA {} must not appear in gen-2",
            lba
        );
        assert!(
            engine
                .meta()
                .get_mapping(&vol_id, Lba(lba))
                .unwrap()
                .is_none(),
            "recovered stale entry for LBA {} must not create a blockmap mapping",
            lba
        );
    }

    let snapshot = engine.metrics_snapshot();
    assert!(
        snapshot.flush_stale_discards >= 1,
        "expected at least one recovered stale unit to be discarded, got {}",
        snapshot.flush_stale_discards
    );
}

/// Recovered buffer entries with vol_created_at=0 are especially dangerous:
/// zero is treated as a wildcard generation in the read/flush path. If such
/// entries survive on disk across delete+recreate of the same volume name,
/// they must still NOT be flushed into the new generation.
#[test]
fn recovered_zero_generation_entries_not_flushed_into_recreated_volume_after_restart() {
    let (config, _md, _bf, _df) = make_config();
    let vol_size = 32 * 4096u64;
    let vol_name = "vol-restart-zero-gen";
    let vol_id = VolumeId(vol_name.to_string());

    let meta_only = OnyxEngine::open_meta_only(&config).unwrap();
    meta_only
        .create_volume(vol_name, vol_size, CompressionAlgo::None)
        .unwrap();

    let buffer_dev = RawDevice::open(config.buffer.device.as_ref().unwrap()).unwrap();
    let pool = WriteBufferPool::open(buffer_dev).unwrap();
    let stale = vec![0xCC; 4096];
    for lba in 0..4u64 {
        pool.append(vol_name, Lba(lba), 1, &stale, 0).unwrap();
    }
    assert_eq!(
        pool.pending_count(),
        4,
        "fixture must persist zero-generation stale entries"
    );
    drop(pool);

    meta_only.delete_volume(vol_name).unwrap();
    meta_only
        .create_volume(vol_name, vol_size, CompressionAlgo::None)
        .unwrap();
    drop(meta_only);

    let engine = OnyxEngine::open(&config).unwrap();
    assert!(
        wait_for_buffer_drain(&engine, 5000),
        "recovered zero-generation entries should be drained quickly"
    );

    let vol = engine.open_volume(vol_name).unwrap();
    for lba in 0..4u64 {
        assert_eq!(
            vol.read(lba * 4096, 4096).unwrap(),
            vec![0u8; 4096],
            "zero-generation recovered entry for LBA {} must not appear after recreate",
            lba
        );
        assert!(
            engine
                .meta()
                .get_mapping(&vol_id, Lba(lba))
                .unwrap()
                .is_none(),
            "zero-generation recovered entry for LBA {} must not create a blockmap mapping",
            lba
        );
    }
}

// --- DISCARD/TRIM tests ---

#[test]
fn discard_basic_unmap() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config).unwrap();
    engine
        .create_volume("vol-discard", 64 * 4096, CompressionAlgo::None)
        .unwrap();
    let vol = engine.open_volume("vol-discard").unwrap();

    // Write 4 blocks (LBA 0..3)
    let payload = vec![0xAA; 4 * 4096];
    vol.write(0, &payload).unwrap();

    // Verify written
    let read = vol.read(0, 4 * 4096).unwrap();
    assert_eq!(read, payload, "data should be readable before discard");

    // Discard LBA 1..2 (offset 4096, len 8192)
    vol.discard(4096, 8192).unwrap();

    // LBA 0 still readable (not discarded)
    let lba0 = vol.read(0, 4096).unwrap();
    assert_eq!(lba0, vec![0xAA; 4096], "LBA 0 should still be mapped");

    // LBA 1 and 2 should read as zeros (unmapped)
    let lba1 = vol.read(4096, 4096).unwrap();
    assert_eq!(lba1, vec![0u8; 4096], "LBA 1 should be zeros after discard");
    let lba2 = vol.read(8192, 4096).unwrap();
    assert_eq!(lba2, vec![0u8; 4096], "LBA 2 should be zeros after discard");

    // LBA 3 still readable
    let lba3 = vol.read(12288, 4096).unwrap();
    assert_eq!(lba3, vec![0xAA; 4096], "LBA 3 should still be mapped");

    // Metrics should reflect discard
    let snap = engine.metrics_snapshot();
    assert!(snap.volume_discard_ops >= 1);
    assert!(snap.volume_discard_lbas >= 2);
}

#[test]
fn discard_with_flush_frees_space() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config).unwrap();
    engine
        .create_volume("vol-discard-space", 64 * 4096, CompressionAlgo::None)
        .unwrap();
    let vol = engine.open_volume("vol-discard-space").unwrap();

    // Write and flush to LV3
    let payload = vec![0xBB; 8 * 4096];
    vol.write(0, &payload).unwrap();
    assert!(wait_for_buffer_drain(&engine, 5000), "flush timeout");

    // Record free blocks before discard
    let free_before = engine
        .allocator()
        .map(|a| a.free_block_count())
        .unwrap_or(0);

    // Discard all 8 blocks
    vol.discard(0, 8 * 4096).unwrap();

    // Free blocks should increase (space reclaimed)
    let free_after = engine
        .allocator()
        .map(|a| a.free_block_count())
        .unwrap_or(0);
    assert!(
        free_after > free_before,
        "free blocks should increase after discard: before={}, after={}",
        free_before,
        free_after
    );

    // All blocks should read as zeros
    for lba in 0..8u64 {
        let data = vol.read(lba * 4096, 4096).unwrap();
        assert_eq!(
            data,
            vec![0u8; 4096],
            "LBA {} should be zeros after discard",
            lba
        );
    }

    // Blockmap should be empty for these LBAs
    let vol_id = VolumeId("vol-discard-space".to_string());
    let mappings = engine
        .meta()
        .get_mappings_range(&vol_id, Lba(0), Lba(8))
        .unwrap();
    assert!(
        mappings.is_empty(),
        "blockmap should have no entries after discard"
    );
}

#[test]
fn discard_empty_range_is_noop() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config).unwrap();
    engine
        .create_volume("vol-discard-noop", 64 * 4096, CompressionAlgo::None)
        .unwrap();
    let vol = engine.open_volume("vol-discard-noop").unwrap();

    // Zero-length discard
    vol.discard(0, 0).unwrap();

    // Sub-block discard (doesn't cover any full block)
    vol.discard(100, 200).unwrap();

    // Out-of-bounds discard should fail
    let result = vol.discard(0, 65 * 4096);
    assert!(result.is_err(), "discard beyond volume size should fail");
}

#[test]
fn discard_partial_block_alignment() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config).unwrap();
    engine
        .create_volume("vol-discard-align", 64 * 4096, CompressionAlgo::None)
        .unwrap();
    let vol = engine.open_volume("vol-discard-align").unwrap();

    // Write 4 blocks
    let payload = vec![0xCC; 4 * 4096];
    vol.write(0, &payload).unwrap();

    // Discard with unaligned start: offset=100, len=3*4096
    // This should only discard LBA 1 and 2 (start rounds up, end rounds down)
    vol.discard(100, 3 * 4096).unwrap();

    // LBA 0 should still be readable (start rounded up past it)
    let lba0 = vol.read(0, 4096).unwrap();
    assert_eq!(
        lba0,
        vec![0xCC; 4096],
        "LBA 0 should survive partial discard"
    );

    // LBA 1 and 2 should be discarded
    let lba1 = vol.read(4096, 4096).unwrap();
    assert_eq!(lba1, vec![0u8; 4096], "LBA 1 should be zeros");
    let lba2 = vol.read(8192, 4096).unwrap();
    assert_eq!(lba2, vec![0u8; 4096], "LBA 2 should be zeros");

    // LBA 3 should still be readable (end rounded down before it)
    let lba3 = vol.read(12288, 4096).unwrap();
    assert_eq!(
        lba3,
        vec![0xCC; 4096],
        "LBA 3 should survive partial discard"
    );
}
