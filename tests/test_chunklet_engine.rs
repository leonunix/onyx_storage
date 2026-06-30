//! Engine-level integration: boot OnyxEngine with LV3 on a chunklet RAID6 LD,
//! then create a volume and round-trip data through the full write/read stack
//! (buffer → flush → IoEngine over ChunkletBackend → metadb → ReadPool over the
//! chunklet backend). LV2 + metadb stay on their own files (Phase 1 scope).

use onyx_storage::config::{
    BufferConfig, ChunkletConfig, ChunkletIoBackend, EngineConfig, FlushConfig, MetaConfig,
    OnyxConfig, StorageConfig, UblkConfig,
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
    // Create the pool + LDs, then pin the LV3 id like `chunklet-init` would.
    let (pool, lv3, _lv2, _meta) = onyx_storage::chunklet_pool::init_pool(&chunklet).unwrap();
    drop(pool);
    chunklet.lv3_ld_id = Some(lv3.to_string());

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
