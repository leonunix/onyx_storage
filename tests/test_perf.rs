use std::env;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use onyx_storage::config::{
    BufferConfig, EngineConfig, FlushConfig, MetaConfig, OnyxConfig, StorageConfig, UblkConfig,
};
use onyx_storage::engine::OnyxEngine;
use onyx_storage::gc::config::GcConfig;
use onyx_storage::metrics::EngineMetricsSnapshot;
use onyx_storage::types::{CompressionAlgo, BLOCK_SIZE};
use tempfile::{tempdir, NamedTempFile};

#[derive(Debug, Clone, Copy)]
enum Scenario {
    SeqWrite,
    SeqRead,
    RandWrite,
    RandRead,
    Overwrite,
}

#[derive(Debug, Clone, Copy)]
enum Pattern {
    Zero,
    Repeat,
    Random,
}

#[derive(Debug, Clone)]
struct PerfConfig {
    volume: String,
    volume_size: u64,
    total_size: u64,
    io_size: u64,
    threads: usize,
    scenario: Scenario,
    pattern: Pattern,
    compression: CompressionAlgo,
    drain_timeout: Duration,
    group_commit_wait_us: u64,
}

struct PerfEnv {
    engine: OnyxEngine,
    _meta_dir: tempfile::TempDir,
    _buf_file: NamedTempFile,
    _data_file: NamedTempFile,
}

#[derive(Debug, Clone, Copy, Default)]
struct WorkStats {
    ops: u64,
    bytes: u64,
}

#[test]
#[ignore = "performance smoke test; run manually with --ignored --nocapture"]
fn local_file_perf_smoke() {
    let cfg = load_perf_config();
    let env = setup_perf_env(&cfg);

    env.engine
        .create_volume(&cfg.volume, cfg.volume_size, cfg.compression)
        .unwrap();

    if matches!(cfg.scenario, Scenario::SeqRead | Scenario::RandRead) {
        prefill_for_reads(&env.engine, &cfg);
    }

    let before = env.engine.metrics_snapshot();
    let start = Instant::now();
    let stats = run_workload(&env.engine, &cfg).unwrap();
    let timed = start.elapsed();

    let pending_after_workload = env.engine.buffer_pool().map(|pool| pool.pending_count());
    let drain_start = Instant::now();
    assert!(
        wait_for_drain(&env.engine, cfg.drain_timeout),
        "buffer drain timeout"
    );
    let drain = drain_start.elapsed();

    let delta = env.engine.metrics_snapshot().saturating_sub(&before);
    print_perf_report(&cfg, stats, timed, drain, pending_after_workload, &delta);

    assert!(stats.ops > 0);
    assert_eq!(env.engine.buffer_pool().unwrap().pending_count(), 0);
    assert_eq!(delta.flush_errors, 0);
    assert_eq!(delta.gc_errors, 0);
    assert_eq!(delta.read_crc_errors, 0);
    assert_eq!(delta.read_decompress_errors, 0);
}

fn load_perf_config() -> PerfConfig {
    let scenario = match env::var("ONYX_PERF_SCENARIO")
        .unwrap_or_else(|_| "seq-write".to_string())
        .as_str()
    {
        "seq-write" => Scenario::SeqWrite,
        "seq-read" => Scenario::SeqRead,
        "rand-write" => Scenario::RandWrite,
        "rand-read" => Scenario::RandRead,
        "overwrite" => Scenario::Overwrite,
        other => panic!("unsupported ONYX_PERF_SCENARIO '{}'", other),
    };

    let pattern = match env::var("ONYX_PERF_PATTERN")
        .unwrap_or_else(|_| "zero".to_string())
        .as_str()
    {
        "zero" => Pattern::Zero,
        "repeat" => Pattern::Repeat,
        "random" => Pattern::Random,
        other => panic!("unsupported ONYX_PERF_PATTERN '{}'", other),
    };

    let compression = match env::var("ONYX_PERF_COMPRESSION")
        .unwrap_or_else(|_| "lz4".to_string())
        .as_str()
    {
        "none" => CompressionAlgo::None,
        "lz4" => CompressionAlgo::Lz4,
        "zstd" => CompressionAlgo::Zstd { level: 3 },
        other => panic!("unsupported ONYX_PERF_COMPRESSION '{}'", other),
    };

    let volume_size =
        parse_size(&env::var("ONYX_PERF_VOLUME_SIZE").unwrap_or_else(|_| "1g".to_string()));
    let total_size =
        parse_size(&env::var("ONYX_PERF_TOTAL_SIZE").unwrap_or_else(|_| "256m".to_string()));
    let io_size = parse_size(&env::var("ONYX_PERF_BS").unwrap_or_else(|_| "128k".to_string()));
    let threads = env::var("ONYX_PERF_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1);
    let drain_timeout_secs = env::var("ONYX_PERF_DRAIN_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(60);
    let group_commit_wait_us = env::var("ONYX_PERF_GROUP_COMMIT_WAIT_US")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(250);

    assert!(
        volume_size >= total_size,
        "volume_size must be >= total_size"
    );
    assert!(io_size > 0 && io_size % BLOCK_SIZE as u64 == 0);
    assert!(total_size > 0 && total_size % io_size == 0);
    assert!(threads > 0);

    PerfConfig {
        volume: "perf-local".to_string(),
        volume_size,
        total_size,
        io_size,
        threads,
        scenario,
        pattern,
        compression,
        drain_timeout: Duration::from_secs(drain_timeout_secs),
        group_commit_wait_us,
    }
}

fn setup_perf_env(cfg: &PerfConfig) -> PerfEnv {
    let meta_dir = tempdir().unwrap();
    let buf_file = NamedTempFile::new().unwrap();
    let data_file = NamedTempFile::new().unwrap();

    let data_bytes = (cfg.volume_size * 2).max(4096 * 65536);
    let buf_bytes = (4096 + cfg.total_size.max(cfg.io_size) * 2).max(4096 + 256 * 4096);

    buf_file.as_file().set_len(buf_bytes).unwrap();
    data_file.as_file().set_len(data_bytes).unwrap();

    let config = OnyxConfig {
        meta: MetaConfig {
            rocksdb_path: Some(meta_dir.path().to_path_buf()),
            block_cache_mb: 64,
            wal_dir: None,
        },
        storage: StorageConfig {
            data_device: Some(data_file.path().to_path_buf()),
            block_size: BLOCK_SIZE,
            use_hugepages: false,
            default_compression: cfg.compression,
        },
        buffer: BufferConfig {
            device: Some(buf_file.path().to_path_buf()),
            capacity_mb: ((buf_bytes / 1024 / 1024).max(1)) as usize,
            flush_watermark_pct: 80,
            group_commit_wait_us: cfg.group_commit_wait_us,
            shards: 1,
        },
        ublk: UblkConfig::default(),
        flush: FlushConfig {
            compress_workers: 2,
            coalesce_max_raw_bytes: 131072,
            coalesce_max_lbas: 32,
        },
        engine: EngineConfig {
            zone_count: 4,
            zone_size_blocks: 256,
        },
        gc: GcConfig {
            enabled: env::var("ONYX_PERF_GC").ok().as_deref() == Some("1"),
            scan_interval_ms: 100,
            ..Default::default()
        },
        dedup: onyx_storage::dedup::config::DedupConfig {
            enabled: env::var("ONYX_PERF_DEDUP")
                .ok()
                .map(|v| v != "0")
                .unwrap_or(true),
            rescan_interval_ms: 100,
            ..Default::default()
        },
        service: Default::default(),
    };

    let engine = OnyxEngine::open(&config).unwrap();

    PerfEnv {
        engine,
        _meta_dir: meta_dir,
        _buf_file: buf_file,
        _data_file: data_file,
    }
}

fn prefill_for_reads(engine: &OnyxEngine, cfg: &PerfConfig) {
    let prefill_cfg = PerfConfig {
        scenario: Scenario::SeqWrite,
        ..cfg.clone()
    };
    run_workload(engine, &prefill_cfg).unwrap();
    assert!(
        wait_for_drain(engine, cfg.drain_timeout),
        "prefill drain timeout"
    );
}

fn run_workload(engine: &OnyxEngine, cfg: &PerfConfig) -> anyhow::Result<WorkStats> {
    let ops_total = cfg.total_size / cfg.io_size;
    let ops_per_thread = ops_total / cfg.threads as u64;
    let extra_ops = ops_total % cfg.threads as u64;
    let barrier = Arc::new(Barrier::new(cfg.threads + 1));
    let mut handles = Vec::with_capacity(cfg.threads);

    for tid in 0..cfg.threads {
        let volume = engine.open_volume(&cfg.volume)?;
        let barrier = barrier.clone();
        let scenario = cfg.scenario;
        let pattern = cfg.pattern;
        let io_size = cfg.io_size;
        let volume_size = volume.size_bytes();
        let ops = ops_per_thread + u64::from(tid < extra_ops as usize);
        let start_ops = tid as u64 * ops_per_thread + (tid as u64).min(extra_ops);
        let base_lba = (start_ops * io_size) / BLOCK_SIZE as u64;

        handles.push(thread::spawn(move || -> anyhow::Result<WorkStats> {
            let mut rng = XorShift64::seed(0xD1F2_3344_5566_7788 ^ tid as u64);
            let mut buf = vec![0u8; io_size as usize];
            fill_buffer(pattern, &mut buf, &mut rng, tid, 0);
            barrier.wait();

            let mut stats = WorkStats::default();
            for op_idx in 0..ops {
                let offset =
                    compute_offset(scenario, io_size, volume_size, base_lba, op_idx, &mut rng);
                if matches!(scenario, Scenario::SeqRead | Scenario::RandRead) {
                    let _ = volume.read(offset, buf.len())?;
                } else {
                    fill_buffer(pattern, &mut buf, &mut rng, tid, op_idx);
                    volume.write(offset, &buf)?;
                }
                stats.ops += 1;
                stats.bytes += io_size;
            }
            Ok(stats)
        }));
    }

    barrier.wait();

    let mut total = WorkStats::default();
    for handle in handles {
        let stats = handle.join().expect("worker thread panicked")?;
        total.ops += stats.ops;
        total.bytes += stats.bytes;
    }
    Ok(total)
}

fn compute_offset(
    scenario: Scenario,
    io_size: u64,
    volume_size: u64,
    base_lba: u64,
    op_idx: u64,
    rng: &mut XorShift64,
) -> u64 {
    let max_io = (volume_size / io_size).max(1);
    let lba_step = io_size / BLOCK_SIZE as u64;
    let io_index = match scenario {
        Scenario::SeqWrite | Scenario::SeqRead => base_lba / lba_step + op_idx,
        Scenario::Overwrite => rng.next_u64() % (max_io / 8).max(1),
        Scenario::RandWrite | Scenario::RandRead => rng.next_u64() % max_io,
    };
    io_index * io_size
}

fn fill_buffer(pattern: Pattern, buf: &mut [u8], rng: &mut XorShift64, tid: usize, op_idx: u64) {
    match pattern {
        Pattern::Zero => buf.fill(0),
        Pattern::Repeat => {
            let seed = ((tid as u64 * 97) ^ op_idx).to_le_bytes();
            for chunk in buf.chunks_mut(seed.len()) {
                let len = chunk.len();
                chunk.copy_from_slice(&seed[..len]);
            }
        }
        Pattern::Random => {
            for chunk in buf.chunks_mut(8) {
                let bytes = rng.next_u64().to_le_bytes();
                let len = chunk.len();
                chunk.copy_from_slice(&bytes[..len]);
            }
        }
    }
}

fn wait_for_drain(engine: &OnyxEngine, timeout: Duration) -> bool {
    let Some(pool) = engine.buffer_pool() else {
        return true;
    };
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if pool.pending_count() == 0 {
            return true;
        }
        thread::sleep(Duration::from_millis(10));
    }
    pool.pending_count() == 0
}

fn print_perf_report(
    cfg: &PerfConfig,
    stats: WorkStats,
    timed: Duration,
    drain: Duration,
    pending_after_workload: Option<u64>,
    delta: &EngineMetricsSnapshot,
) {
    let secs = timed.as_secs_f64().max(f64::MIN_POSITIVE);
    let throughput_mib = (stats.bytes as f64 / 1024.0 / 1024.0) / secs;
    let iops = stats.ops as f64 / secs;
    let avg_latency_us = timed.as_secs_f64() * 1_000_000.0 / stats.ops.max(1) as f64;

    eprintln!();
    eprintln!("========== ONYX LOCAL PERF ==========");
    eprintln!(
        "scenario={:?} pattern={:?} compression={:?} threads={} bs={} total={}",
        cfg.scenario,
        cfg.pattern,
        cfg.compression,
        cfg.threads,
        human_bytes(cfg.io_size),
        human_bytes(stats.bytes),
    );
    eprintln!("group_commit_wait_us={}", cfg.group_commit_wait_us);
    eprintln!(
        "timed={:.3}s throughput={:.2} MiB/s iops={:.0} avg_latency={:.2} us/op",
        timed.as_secs_f64(),
        throughput_mib,
        iops,
        avg_latency_us,
    );
    eprintln!(
        "buffer_pending_after_workload={:?} drain_catchup={:.3}s",
        pending_after_workload,
        drain.as_secs_f64(),
    );
    eprintln!(
        "metrics: writes={} reads={} read_buffer_hits={} read_lv3_hits={} flush_units={} packed_slots={} dedup_hits={} dedup_misses={} gc_rewritten={}",
        delta.volume_write_ops,
        delta.volume_read_ops,
        delta.read_buffer_hits,
        delta.read_lv3_hits,
        delta.flush_units_written,
        delta.flush_packed_slots_written,
        delta.dedup_hits,
        delta.dedup_misses,
        delta.gc_blocks_rewritten,
    );
    eprintln!(
        "bytes: write={} read={} coalesced={} compress_in={} compress_out={}",
        human_bytes(delta.volume_write_bytes),
        human_bytes(delta.volume_read_bytes),
        human_bytes(delta.coalesced_bytes),
        human_bytes(delta.compress_input_bytes),
        human_bytes(delta.compress_output_bytes),
    );
    eprintln!(
        "writer-stage: total={} alloc={} io={} meta={} cleanup={} dedup_index={} hole_detect={} mark_flushed={}",
        human_duration_ns(delta.flush_writer_total_ns),
        human_duration_ns(delta.flush_writer_alloc_ns),
        human_duration_ns(delta.flush_writer_io_ns),
        human_duration_ns(delta.flush_writer_meta_ns),
        human_duration_ns(delta.flush_writer_cleanup_ns),
        human_duration_ns(delta.flush_writer_dedup_index_ns),
        human_duration_ns(delta.flush_writer_hole_detect_ns),
        human_duration_ns(delta.flush_writer_mark_flushed_ns),
    );
}

fn parse_size(input: &str) -> u64 {
    let s = input.trim().to_ascii_lowercase();
    let split_idx = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (digits, suffix) = s.split_at(split_idx);
    let base: u64 = digits
        .parse()
        .unwrap_or_else(|_| panic!("invalid size '{}'", input));
    let multiplier = match suffix {
        "" | "b" => 1,
        "k" | "kb" => 1024,
        "m" | "mb" => 1024 * 1024,
        "g" | "gb" => 1024 * 1024 * 1024,
        other => panic!("unsupported size suffix '{}'", other),
    };
    base.checked_mul(multiplier)
        .unwrap_or_else(|| panic!("size '{}' overflows u64", input))
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", bytes, UNITS[unit])
    } else {
        format!("{:.2} {}", value, UNITS[unit])
    }
}

fn human_duration_ns(ns: u64) -> String {
    if ns >= 1_000_000_000 {
        format!("{:.3}s", ns as f64 / 1_000_000_000.0)
    } else if ns >= 1_000_000 {
        format!("{:.3}ms", ns as f64 / 1_000_000.0)
    } else if ns >= 1_000 {
        format!("{:.3}us", ns as f64 / 1_000.0)
    } else {
        format!("{ns}ns")
    }
}

#[derive(Debug, Clone, Copy)]
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn seed(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }
}
