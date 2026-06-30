use clap::{Parser, Subcommand};

use onyx_storage::buffer::flush::install_test_dedup_hit_failpoint;
use onyx_storage::config::{
    BufferConfig, EngineConfig, FlushConfig, MetaConfig, OnyxConfig, StorageConfig, UblkConfig,
};
use onyx_storage::dedup::config::DedupConfig;
use onyx_storage::engine::OnyxEngine;
use onyx_storage::gc::config::GcConfig;
use onyx_storage::types::{CompressionAlgo, Lba, BLOCK_SIZE};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    WriteAndAbort {
        #[arg(long)]
        meta_dir: std::path::PathBuf,
        #[arg(long)]
        buffer: std::path::PathBuf,
        #[arg(long)]
        data: std::path::PathBuf,
    },
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::WriteAndAbort {
            meta_dir,
            buffer,
            data,
        } => write_and_abort(meta_dir, buffer, data),
    }
}

fn write_and_abort(
    meta_dir: std::path::PathBuf,
    buffer: std::path::PathBuf,
    data: std::path::PathBuf,
) -> ! {
    let config = OnyxConfig {
        meta: MetaConfig {
            path: Some(meta_dir),
            block_cache_mb: 32,
            memtable_budget_mb: 0,
            index_pin_mb: 64,
            lsm_bloom_bits_per_entry: 10,
            checkpoint_interval_ms: 5000,
            group_commit_timeout_us: 1000,
            wal_dir: None,
            // Must match the EngineHarness that created the metadb dir.
            // dedup_shards is part of the on-disk layout — opening with
            // a different value than the persisted manifest is a hard
            // error (see metadb/src/manifest.rs). The test's harness
            // uses the MetaConfig default (8); track it here.
            dedup_shards: 8,
            dedup_cuckoo_buckets: 4_000_000,
            dedup_l1_cache_entries: 256_000,
            ..Default::default()
        },
        storage: StorageConfig {
            data_device: Some(data),
            block_size: BLOCK_SIZE,
            use_hugepages: false,
            default_compression: CompressionAlgo::Lz4,
            io_backend: Default::default(),
            uring_sq_entries: 128,
            read_pool_workers: 4,
            lv3_per_shard_write_rings: true,
        },
        buffer: BufferConfig {
            device: Some(buffer),
            capacity_mb: 4,
            flush_watermark_pct: 80,
            group_commit_wait_us: 250,
            shards: 1,
            max_memory_mb: 0,
            ..BufferConfig::default()
        },
        ublk: UblkConfig::default(),
        flush: FlushConfig {
            compress_workers: 2,
            coalesce_max_raw_bytes: 131072,
            coalesce_max_lbas: 32,
            min_compression_savings_pct: 12,
            skip_fully_superseded: true,
            ..FlushConfig::default()
        },
        engine: EngineConfig {
            zone_count: 4,
            zone_size_blocks: 128,
            ..EngineConfig::default()
        },
        gc: GcConfig {
            enabled: false,
            ..Default::default()
        },
        dedup: DedupConfig {
            enabled: true,
            workers: 2,
            ..Default::default()
        },
        service: Default::default(),
        ha: Default::default(),
        threading: Default::default(),
        numa: Default::default(),
        chunklet: Default::default(),
    };

    let engine = OnyxEngine::open(&config).unwrap();
    let volume_size = 64 * BLOCK_SIZE as u64;
    if engine.open_volume("crash-vol").is_err() {
        engine
            .create_volume("crash-vol", volume_size, CompressionAlgo::Lz4)
            .unwrap();
    }

    let volume = engine.open_volume("crash-vol").unwrap();
    volume.write(0, &vec![0xAA; 4096]).unwrap();
    wait_for_drain(&engine, std::time::Duration::from_secs(5));

    install_test_dedup_hit_failpoint("crash-vol", Lba(1), None);
    let volume = engine.open_volume("crash-vol").unwrap();
    volume.write(4096, &vec![0xAA; 4096]).unwrap();

    std::thread::sleep(std::time::Duration::from_millis(100));
    std::process::abort();
}

fn wait_for_drain(engine: &OnyxEngine, timeout: std::time::Duration) {
    let Some(pool) = engine.buffer_pool() else {
        return;
    };
    let start = std::time::Instant::now();
    while pool.pending_count() > 0 {
        assert!(
            start.elapsed() <= timeout,
            "buffer drain timeout after {:?} (pending={})",
            timeout,
            pool.pending_count()
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    std::thread::sleep(std::time::Duration::from_millis(80));
}
