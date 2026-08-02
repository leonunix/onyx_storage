//! Engine-level integration: boot OnyxEngine with LV3 on a chunklet RAID6 LD
//! AND LV2 on a chunklet RAID10 LD (both carved from one shared pool), then
//! create a volume and round-trip data through the full write/read stack
//! (buffer over the chunklet LV2 backend → flush → IoEngine over the chunklet
//! LV3 backend → metadb → ReadPool over the chunklet backend). The last test
//! also puts metadb itself on the chunklet meta LD (Phase 3d).

use onyx_storage::config::{
    BufferConfig, ChunkletConfig, ChunkletIoBackend, EngineConfig, FlushConfig, MetaBackendKind,
    MetaConfig, OnyxConfig, StorageConfig, UblkConfig,
};
use onyx_storage::engine::OnyxEngine;
use onyx_storage::types::{CompressionAlgo, BLOCK_SIZE};
use tempfile::{tempdir, NamedTempFile};

#[test]
fn engine_boots_lv3_on_chunklet_raid6_and_round_trips() {
    // 8 sparse PDs (4 GiB each) → RAID6 LV3 (6 data + 2 parity) fits, plus the
    // RAID10 LV2/meta LDs chunklet-init creates alongside.
    let dir = tempdir().unwrap();
    let devices: Vec<_> = (0..8).map(|i| dir.path().join(format!("pd{i}"))).collect();
    for p in &devices {
        let f = std::fs::File::create(p).unwrap();
        f.set_len(4 << 30).unwrap();
    }

    let mut chunklet = ChunkletConfig {
        enabled: true,
        devices,
        io_backend: ChunkletIoBackend::Sync,
        spare_pct: 0,
        ..Default::default()
    };
    // Create the pool + LDs, then pin the LV3 + LV2 ids like `chunklet-init`
    // would. With chunklet enabled both LV3 (RAID6) and LV2 (RAID10) are served
    // from the shared pool, so the buffer pool needs its LD id too.
    let (pool, lv3, lv2, _meta) = onyx_storage::chunklet_pool::init_pool(&chunklet).unwrap();
    drop(pool);
    chunklet.lv3_ld_id = Some(lv3.to_string());
    chunklet.lv2_ld_id = Some(lv2.to_string());

    let meta_dir = tempdir().unwrap();
    let buffer_file = NamedTempFile::new().unwrap();
    buffer_file.as_file().set_len(4096 + 1024 * 4096).unwrap();

    let config = OnyxConfig {
        meta: MetaConfig {
            path: Some(meta_dir.path().to_path_buf()),
            block_cache_mb: 32,
            checkpoint_interval_ms: 5000,
            group_commit_timeout_us: 1,
            dedup_shards: 8,
            dedup_cuckoo_buckets: 1_000_000,
            dedup_l1_cache_entries: 256_000,
            ..MetaConfig::default()
        },
        storage: StorageConfig {
            // LV3 comes from chunklet; no single data_device.
            data_device: None,
            block_size: BLOCK_SIZE,
            read_pool_workers: 2,
            ..StorageConfig::default()
        },
        buffer: BufferConfig {
            device: Some(buffer_file.path().to_path_buf()),
            capacity_mb: 4,
            shards: 1,
            ..BufferConfig::default()
        },
        ublk: UblkConfig::default(),
        flush: FlushConfig::default(),
        engine: EngineConfig {
            zone_count: 4,
            zone_size_blocks: 128,
            ..EngineConfig::default()
        },
        chunklet,
        ..Default::default()
    };

    let engine = OnyxEngine::open(&config).expect("engine boots with chunklet LV3");

    engine
        .create_volume("ck-vol", 64 * 1024 * 1024, CompressionAlgo::Lz4)
        .unwrap();
    let vol = engine.open_volume("ck-vol").unwrap();

    // Write two distinct blocks, read them back through the chunklet LV3 path.
    let a = vec![0xA7u8; BLOCK_SIZE as usize];
    let b: Vec<u8> = (0..BLOCK_SIZE).map(|i| (i % 251) as u8).collect();
    vol.write(0, &a).unwrap();
    vol.write(BLOCK_SIZE as u64, &b).unwrap();

    assert_eq!(vol.read(0, a.len()).unwrap(), a);
    assert_eq!(vol.read(BLOCK_SIZE as u64, b.len()).unwrap(), b);

    drop(vol);
    engine.shutdown().unwrap();
}

/// Full-stripe writes end to end: LV3 = 4 KiB-strip RAID6 (6-block / 24 KiB
/// stripe) with `raid_full_stripe_writes` ON. Write one 24 KiB incompressible
/// chunk — it stays raw (compression gate) at exactly one stripe, so the writer
/// takes the stripe-aligned allocation + full-stripe write path. Shut down
/// (drains the flusher → the aligned LV3 writes land), reopen, and read back
/// from LV3: correct data proves the aligned device offset + chunklet
/// full-stripe path round-trips. (The pad-free / no-RMW throughput win is
/// validated separately on the nvme-box A/B.)
#[test]
fn full_stripe_aligned_write_round_trips_after_reopen() {
    let dir = tempdir().unwrap();
    let devices: Vec<_> = (0..8).map(|i| dir.path().join(format!("pd{i}"))).collect();
    for p in &devices {
        let f = std::fs::File::create(p).unwrap();
        f.set_len(4 << 30).unwrap();
    }

    use onyx_storage::config::ChunkletLdGeom;
    let mut chunklet = ChunkletConfig {
        enabled: true,
        devices,
        io_backend: ChunkletIoBackend::Sync,
        spare_pct: 0,
        // LV3 = 6+2 RAID6 at a 4 KiB strip → 6-block (24 KiB) full stripe, small
        // enough that a single 24 KiB write fills exactly one stripe.
        lv3: ChunkletLdGeom {
            raid: "raid6".to_string(),
            set_size: 6,
            row_size: 1,
            num_rows: 1,
            strip_kib: 4,
        },
        ..Default::default()
    };
    let (pool, lv3, lv2, _meta) = onyx_storage::chunklet_pool::init_pool(&chunklet).unwrap();
    drop(pool);
    chunklet.lv3_ld_id = Some(lv3.to_string());
    chunklet.lv2_ld_id = Some(lv2.to_string());

    let meta_dir = tempdir().unwrap();
    let buffer_file = NamedTempFile::new().unwrap();
    buffer_file.as_file().set_len(4096 + 1024 * 4096).unwrap();

    let config = OnyxConfig {
        meta: MetaConfig {
            path: Some(meta_dir.path().to_path_buf()),
            block_cache_mb: 32,
            checkpoint_interval_ms: 5000,
            group_commit_timeout_us: 1,
            dedup_shards: 8,
            dedup_cuckoo_buckets: 1_000_000,
            dedup_l1_cache_entries: 256_000,
            ..MetaConfig::default()
        },
        storage: StorageConfig {
            data_device: None,
            block_size: BLOCK_SIZE,
            read_pool_workers: 2,
            // The feature under test.
            raid_full_stripe_writes: true,
            lv3_batch_coalesce_us: 0,
            lv3_batch_target_bytes: 0,
            lv3_batch_executors: 0,
            stripe_group_lifetime_affinity: false,
            allocator_regions: 0,
            stripe_refill_run_stripes: 0,
            ..StorageConfig::default()
        },
        buffer: BufferConfig {
            device: Some(buffer_file.path().to_path_buf()),
            capacity_mb: 4,
            shards: 1,
            ..BufferConfig::default()
        },
        ublk: UblkConfig::default(),
        flush: FlushConfig::default(),
        engine: EngineConfig {
            zone_count: 4,
            zone_size_blocks: 128,
            ..EngineConfig::default()
        },
        chunklet,
        ..Default::default()
    };

    // One 24 KiB incompressible chunk (6 LBAs). A cheap xorshift keeps lz4 from
    // finding savings, so it stores raw at exactly one 6-block stripe.
    let stripe_bytes = 6 * BLOCK_SIZE as usize;
    let payload: Vec<u8> = {
        let mut s: u32 = 0x9e3779b9;
        (0..stripe_bytes)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 17;
                s ^= s << 5;
                s as u8
            })
            .collect()
    };

    {
        let engine = OnyxEngine::open(&config).expect("engine boots with full-stripe writes");
        engine
            .create_volume("fs-vol", 64 * 1024 * 1024, CompressionAlgo::Lz4)
            .unwrap();
        let vol = engine.open_volume("fs-vol").unwrap();
        vol.write(0, &payload).unwrap();
        assert_eq!(vol.read(0, payload.len()).unwrap(), payload);
        drop(vol);
        // Drains the flusher → the stripe-aligned full-stripe LV3 writes land.
        engine.shutdown().unwrap();
    }

    // Reopen over the same chunklet pool + metadb; the buffer drained on clean
    // shutdown, so this read is served from LV3 through the aligned write.
    let engine = OnyxEngine::open(&config).expect("engine reopens");
    let vol = engine.open_volume("fs-vol").unwrap();
    assert_eq!(
        vol.read(0, payload.len()).unwrap(),
        payload,
        "24 KiB full-stripe write must round-trip from LV3 after reopen"
    );
    drop(vol);
    engine.shutdown().unwrap();
}

/// Phase 3d: metadb ON a chunklet meta LD (RAID10). The entire metadata surface
/// — page store, lifecycle journal ring, and the volume-catalog A/B slots —
/// lives inside the chunklet pool, so no host FS holds metadata. Boot → create
/// volume → write → clean shutdown → reopen exercises the device-path bounded
/// scan open, the catalog A/B slot load, and the lifecycle ring; the volume and
/// its data must survive with no `pages.onyx_meta` file anywhere.
#[test]
fn engine_metadb_on_chunklet_meta_ld_round_trips_after_reopen() {
    let dir = tempdir().unwrap();
    let devices: Vec<_> = (0..8).map(|i| dir.path().join(format!("pd{i}"))).collect();
    for p in &devices {
        std::fs::File::create(p).unwrap().set_len(4 << 30).unwrap();
    }

    // LV3/LV2/meta all use default geometries (RAID6 / RAID10 / RAID10).
    let mut chunklet = ChunkletConfig {
        enabled: true,
        devices,
        io_backend: ChunkletIoBackend::Sync,
        spare_pct: 0,
        ..Default::default()
    };
    let (pool, lv3, lv2, meta) = onyx_storage::chunklet_pool::init_pool(&chunklet).unwrap();
    drop(pool);
    chunklet.lv3_ld_id = Some(lv3.to_string());
    chunklet.lv2_ld_id = Some(lv2.to_string());
    chunklet.meta_ld_id = Some(meta.to_string());

    // `meta.path` is only a diagnostic label on the device path; metadb persists
    // nothing to it.
    let meta_label = tempdir().unwrap();
    let buffer_file = NamedTempFile::new().unwrap();
    buffer_file.as_file().set_len(4096 + 1024 * 4096).unwrap();

    let config = OnyxConfig {
        meta: MetaConfig {
            path: Some(meta_label.path().to_path_buf()),
            backend: MetaBackendKind::Chunklet,
            block_cache_mb: 32,
            checkpoint_interval_ms: 5000,
            group_commit_timeout_us: 1,
            dedup_shards: 8,
            dedup_cuckoo_buckets: 1_000_000,
            dedup_l1_cache_entries: 256_000,
            ..MetaConfig::default()
        },
        storage: StorageConfig {
            data_device: None,
            block_size: BLOCK_SIZE,
            read_pool_workers: 2,
            ..StorageConfig::default()
        },
        buffer: BufferConfig {
            device: Some(buffer_file.path().to_path_buf()),
            capacity_mb: 4,
            shards: 1,
            ..BufferConfig::default()
        },
        ublk: UblkConfig::default(),
        flush: FlushConfig::default(),
        engine: EngineConfig {
            zone_count: 4,
            zone_size_blocks: 128,
            ..EngineConfig::default()
        },
        chunklet,
        ..Default::default()
    };

    let payload: Vec<u8> = (0..16 * 1024).map(|i| (i % 251) as u8).collect();
    {
        let engine = OnyxEngine::open(&config).expect("engine boots with metadb on the meta LD");
        engine
            .create_volume("meta-ld-vol", 64 * 1024 * 1024, CompressionAlgo::Lz4)
            .unwrap();
        let vol = engine.open_volume("meta-ld-vol").unwrap();
        vol.write(0, &payload).unwrap();
        assert_eq!(vol.read(0, payload.len()).unwrap(), payload);
        drop(vol);
        engine.shutdown().unwrap();
    }

    // No metadb page file was ever written to the host FS label dir.
    assert!(
        !meta_label.path().join("pages.onyx_meta").exists(),
        "metadb must not write a page file on the host FS when backend = chunklet"
    );

    // Reopen: metadb recovers off the meta LD (bounded-scan device open + catalog
    // A/B slot load + lifecycle ring). The volume is known by name (catalog
    // survived) and its data round-trips.
    let engine = OnyxEngine::open(&config).expect("engine reopens with metadb on the meta LD");
    let vol = engine
        .open_volume("meta-ld-vol")
        .expect("volume survived reopen off the meta LD");
    assert_eq!(
        vol.read(0, payload.len()).unwrap(),
        payload,
        "data must round-trip after metadb reopens off the meta LD"
    );
    drop(vol);
    engine.shutdown().unwrap();
}
