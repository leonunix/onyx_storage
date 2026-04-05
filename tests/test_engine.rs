use std::path::PathBuf;

use onyx_storage::config::*;
use onyx_storage::engine::OnyxEngine;
use onyx_storage::types::CompressionAlgo;
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
            rocksdb_path: meta_dir.path().to_path_buf(),
            block_cache_mb: 8,
            wal_dir: None,
        },
        storage: StorageConfig {
            data_device: data_tmp.path().to_path_buf(),
            block_size: 4096,
            use_hugepages: false,
            default_compression: CompressionAlgo::None,
        },
        buffer: BufferConfig {
            device: buf_tmp.path().to_path_buf(),
            capacity_mb: 1,
            flush_watermark_pct: 80,
        },
        ublk: UblkConfig::default(),
        flush: FlushConfig::default(),
        engine: EngineConfig {
            zone_count: 2,
            zone_size_blocks: 128,
        },
    };

    (config, meta_dir, buf_tmp, data_tmp)
}

// --- Meta-only mode ---

#[test]
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
    let engine = OnyxEngine::open(&config, CompressionAlgo::None).unwrap();
    engine.shutdown().unwrap();
    // Double shutdown is safe
    engine.shutdown().unwrap();
}

#[test]
fn full_engine_create_and_open_volume() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config, CompressionAlgo::None).unwrap();
    engine
        .create_volume("vol-test", 256 * 4096, CompressionAlgo::None)
        .unwrap();
    let vol = engine.open_volume("vol-test").unwrap();
    assert_eq!(vol.name(), "vol-test");
    assert_eq!(vol.size_bytes(), 256 * 4096);
}

#[test]
fn open_nonexistent_volume_fails() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config, CompressionAlgo::None).unwrap();
    assert!(engine.open_volume("no-such-vol").is_err());
}

// --- Volume IO ---

#[test]
fn volume_aligned_write_read() {
    let (config, _md, _bf, _df) = make_config();
    let engine = OnyxEngine::open(&config, CompressionAlgo::None).unwrap();
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
    let engine = OnyxEngine::open(&config, CompressionAlgo::None).unwrap();
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
    let engine = OnyxEngine::open(&config, CompressionAlgo::None).unwrap();
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
    let engine = OnyxEngine::open(&config, CompressionAlgo::None).unwrap();
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
    let engine = OnyxEngine::open(&config, CompressionAlgo::None).unwrap();
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
    let engine = OnyxEngine::open(&config, CompressionAlgo::None).unwrap();
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
    let engine = OnyxEngine::open(&config, CompressionAlgo::None).unwrap();
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
