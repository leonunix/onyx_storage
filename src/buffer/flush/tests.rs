use super::*;
use crate::config::MetaConfig;
use crate::io::device::RawDevice;
use crate::meta::store::{MetaStore, RemapCleanup};
use crate::types::{VolumeConfig, ZoneId};
use crate::zone::worker::ZoneWorker;
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use tempfile::{tempdir, NamedTempFile};

fn setup_flush_test_env() -> (
    Arc<MetaStore>,
    Arc<WriteBufferPool>,
    Arc<VolumeLifecycleManager>,
    Arc<SpaceAllocator>,
    Arc<IoEngine>,
    Arc<EngineMetrics>,
    tempfile::TempDir,
    NamedTempFile,
    NamedTempFile,
) {
    let meta_dir = tempdir().unwrap();
    let meta = Arc::new(
        MetaStore::open(&MetaConfig {
            path: Some(meta_dir.path().to_path_buf()),
            block_cache_mb: 8,
            memtable_budget_mb: 0,
            index_pin_mb: 64,
            lsm_bloom_bits_per_entry: 10,
            checkpoint_interval_ms: 5000,
            group_commit_timeout_us: 1,
            wal_dir: None,
            dedup_shards: 1,
            dedup_cuckoo_buckets: 1_000_000,
            dedup_l1_cache_entries: 256_000,
            ..Default::default()
        })
        .unwrap(),
    );

    let buf_tmp = NamedTempFile::new().unwrap();
    let buf_size: u64 = 4096 + 4096 + 256 * 8192;
    buf_tmp.as_file().set_len(buf_size).unwrap();
    let pool = Arc::new(
        WriteBufferPool::open(RawDevice::open_or_create(buf_tmp.path(), buf_size).unwrap())
            .unwrap(),
    );

    let data_tmp = NamedTempFile::new().unwrap();
    let data_size: u64 = 4096 * 20000;
    data_tmp.as_file().set_len(data_size).unwrap();
    let io_engine = Arc::new(IoEngine::new(
        RawDevice::open(data_tmp.path()).unwrap(),
        false,
    ));

    let allocator = Arc::new(SpaceAllocator::new(data_size, 1));
    let lifecycle = Arc::new(VolumeLifecycleManager::default());
    let metrics = Arc::new(EngineMetrics::default());

    meta.put_volume(&VolumeConfig {
        id: VolumeId("flush-race".into()),
        // Packed/dedup stress cases below intentionally spread mappings far
        // apart to avoid accidental overwrite between scenarios. Keep the
        // test volume large enough that diagnostic full-blockmap scans include
        // those high LBAs now that metadb scans are bounded by volume size.
        size_bytes: 4096 * 1_000_000,
        block_size: 4096,
        compression: CompressionAlgo::None,
        created_at: 1,
        zone_count: 1,
    })
    .unwrap();

    (
        meta, pool, lifecycle, allocator, io_engine, metrics, meta_dir, buf_tmp, data_tmp,
    )
}

fn cleanup_for_pba(pba: Pba, blocks: u32) -> RemapCleanup {
    RemapCleanup {
        pba,
        decrements: blocks,
        blocks,
        pba_freed: true,
        mappings: Vec::new(),
    }
}

fn make_unit(fill: u8, seq: u64) -> CompressedUnit {
    let data = vec![fill; BLOCK_SIZE as usize];
    CompressedUnit {
        vol_id: "flush-race".into(),
        start_lba: Lba(0),
        lba_count: 1,
        original_size: BLOCK_SIZE,
        compressed_data: data.clone(),
        compression: 0,
        crc32: crc32fast::hash(&data),
        vol_created_at: 1,
        seq_lba_ranges: vec![(seq, Lba(0), 1)],
        block_hashes: None,
        dedup_stale_repairs: None,
        dedup_skipped: false,
        compression_bypassed: false,
        dedup_completion: None,
    }
}

fn make_raw_unit_at(start_lba: u64, lba_count: u32, first_byte: u8, seq: u64) -> CompressedUnit {
    let mut data = vec![0u8; lba_count as usize * BLOCK_SIZE as usize];
    for (idx, block) in data.chunks_mut(BLOCK_SIZE as usize).enumerate() {
        block.fill(first_byte.wrapping_add(idx as u8));
    }
    CompressedUnit {
        vol_id: "flush-race".into(),
        start_lba: Lba(start_lba),
        lba_count,
        original_size: data.len() as u32,
        compressed_data: data.clone(),
        compression: 0,
        crc32: crc32fast::hash(&data),
        vol_created_at: 1,
        seq_lba_ranges: vec![(seq, Lba(start_lba), lba_count)],
        block_hashes: None,
        dedup_stale_repairs: None,
        dedup_skipped: false,
        compression_bypassed: false,
        dedup_completion: None,
    }
}

fn make_packed_unit(fill: u8, seq: u64) -> CompressedUnit {
    let data = vec![fill; 512];
    CompressedUnit {
        vol_id: "flush-race".into(),
        start_lba: Lba(0),
        lba_count: 1,
        original_size: BLOCK_SIZE,
        compressed_data: data.clone(),
        compression: 0,
        crc32: crc32fast::hash(&data),
        vol_created_at: 1,
        seq_lba_ranges: vec![(seq, Lba(0), 1)],
        block_hashes: None,
        dedup_stale_repairs: None,
        dedup_skipped: false,
        compression_bypassed: false,
        dedup_completion: None,
    }
}

fn make_packed_unit_at(fill: u8, seq: u64, lba: u64) -> CompressedUnit {
    let data = vec![fill; 128];
    CompressedUnit {
        vol_id: "flush-race".into(),
        start_lba: Lba(lba),
        lba_count: 1,
        original_size: BLOCK_SIZE,
        compressed_data: data.clone(),
        compression: 0,
        crc32: crc32fast::hash(&data),
        vol_created_at: 1,
        seq_lba_ranges: vec![(seq, Lba(lba), 1)],
        block_hashes: None,
        dedup_stale_repairs: None,
        dedup_skipped: false,
        compression_bypassed: false,
        dedup_completion: None,
    }
}

mod allocator_pressure;
mod commit_path;
mod dedup_cleanup;
mod packed_refcount;
mod pipeline;
mod replay;
