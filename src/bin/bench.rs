use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use clap::{Parser, ValueEnum};

use onyx_storage::config::OnyxConfig;
use onyx_storage::engine::OnyxEngine;
use onyx_storage::metrics::EngineMetricsSnapshot;
use onyx_storage::types::{CompressionAlgo, BLOCK_SIZE};

#[derive(Debug, Clone, Parser)]
#[command(
    name = "onyx-bench",
    about = "Run benchmark workloads against OnyxEngine and correlate results with internal metrics"
)]
struct Cli {
    #[arg(short, long, default_value = "config/default.toml")]
    config: std::path::PathBuf,

    #[arg(long, default_value = "benchvol")]
    volume: String,

    #[arg(long, default_value = "1g", value_parser = parse_size)]
    volume_size: u64,

    #[arg(long, value_enum, default_value_t = Scenario::SeqWrite)]
    scenario: Scenario,

    #[arg(long, default_value = "256m", value_parser = parse_size)]
    total_size: u64,

    #[arg(long = "bs", default_value = "128k", value_parser = parse_size)]
    io_size: u64,

    #[arg(long, default_value_t = 1)]
    threads: usize,

    #[arg(long, value_enum, default_value_t = Pattern::Zero)]
    pattern: Pattern,

    #[arg(long, value_enum, default_value_t = CompressionChoice::Lz4)]
    compression: CompressionChoice,

    #[arg(long, default_value_t = 60)]
    drain_timeout_secs: u64,

    #[arg(long)]
    group_commit_wait_us: Option<u64>,

    #[arg(long, default_value_t = false)]
    skip_prefill: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum Scenario {
    SeqWrite,
    SeqRead,
    RandWrite,
    RandRead,
    Overwrite,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum Pattern {
    Zero,
    Repeat,
    Random,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum CompressionChoice {
    None,
    Lz4,
    Zstd,
}

impl CompressionChoice {
    fn into_algo(self) -> CompressionAlgo {
        match self {
            Self::None => CompressionAlgo::None,
            Self::Lz4 => CompressionAlgo::Lz4,
            Self::Zstd => CompressionAlgo::Zstd { level: 3 },
        }
    }
}

#[derive(Debug, Clone)]
struct BenchConfig {
    volume: String,
    scenario: Scenario,
    total_size: u64,
    io_size: u64,
    threads: usize,
    pattern: Pattern,
    group_commit_wait_us: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct WorkerStats {
    ops: u64,
    bytes: u64,
}

#[derive(Debug, Clone)]
struct BenchResult {
    timed_phase: Duration,
    drain_phase: Duration,
    stats: WorkerStats,
    metrics_delta: EngineMetricsSnapshot,
    pending_after_timed_phase: Option<u64>,
    pending_after_drain: Option<u64>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    validate_args(&cli)?;

    let mut config = OnyxConfig::load(&cli.config)?;
    if let Some(wait_us) = cli.group_commit_wait_us {
        config.buffer.group_commit_wait_us = wait_us;
    }
    let engine = OnyxEngine::open(&config)?;

    engine
        .delete_volume(&cli.volume)
        .with_context(|| format!("failed to reset benchmark volume '{}'", cli.volume))?;
    engine
        .create_volume(&cli.volume, cli.volume_size, cli.compression.into_algo())
        .with_context(|| format!("failed to create benchmark volume '{}'", cli.volume))?;

    if matches!(cli.scenario, Scenario::SeqRead | Scenario::RandRead) && !cli.skip_prefill {
        println!(
            "prefill: writing {} with {:?} before {:?}",
            human_bytes(cli.total_size),
            cli.pattern,
            cli.scenario
        );
        prefill_volume(
            &engine,
            &BenchConfig {
                volume: cli.volume.clone(),
                scenario: Scenario::SeqWrite,
                total_size: cli.total_size,
                io_size: cli.io_size,
                threads: cli.threads,
                pattern: cli.pattern,
                group_commit_wait_us: config.buffer.group_commit_wait_us,
            },
            cli.drain_timeout_secs,
        )?;
    }

    let bench = BenchConfig {
        volume: cli.volume.clone(),
        scenario: cli.scenario,
        total_size: cli.total_size,
        io_size: cli.io_size,
        threads: cli.threads,
        pattern: cli.pattern,
        group_commit_wait_us: config.buffer.group_commit_wait_us,
    };

    let result = run_benchmark(&engine, &bench, cli.drain_timeout_secs)?;
    print_report(&bench, &result);

    engine.shutdown()?;
    Ok(())
}

fn validate_args(cli: &Cli) -> Result<()> {
    if cli.io_size == 0 || cli.total_size == 0 || cli.volume_size == 0 {
        return Err(anyhow!("sizes must be > 0"));
    }
    if cli.threads == 0 {
        return Err(anyhow!("threads must be > 0"));
    }
    if cli.io_size % BLOCK_SIZE as u64 != 0 {
        return Err(anyhow!("--bs must be aligned to {} bytes", BLOCK_SIZE));
    }
    if cli.total_size % cli.io_size != 0 {
        return Err(anyhow!("--total-size must be a multiple of --bs"));
    }
    if cli.volume_size < cli.total_size {
        return Err(anyhow!(
            "--volume-size must be >= --total-size (got {} < {})",
            cli.volume_size,
            cli.total_size
        ));
    }
    Ok(())
}

fn prefill_volume(engine: &OnyxEngine, bench: &BenchConfig, drain_timeout_secs: u64) -> Result<()> {
    let before = engine.metrics_snapshot();
    let _ = run_workload(engine, bench)?;
    if !wait_for_drain(engine, Duration::from_secs(drain_timeout_secs)) {
        return Err(anyhow!("prefill drain timeout"));
    }
    let _ = engine.metrics_snapshot().saturating_sub(&before);
    Ok(())
}

fn run_benchmark(
    engine: &OnyxEngine,
    bench: &BenchConfig,
    drain_timeout_secs: u64,
) -> Result<BenchResult> {
    let before = engine.metrics_snapshot();

    let timed_start = Instant::now();
    let stats = run_workload(engine, bench)?;
    let timed_phase = timed_start.elapsed();

    let pending_after_timed_phase = engine.buffer_pool().map(|pool| pool.pending_count());
    let drain_start = Instant::now();
    let drain_ok = wait_for_drain(engine, Duration::from_secs(drain_timeout_secs));
    let drain_phase = drain_start.elapsed();
    let pending_after_drain = engine.buffer_pool().map(|pool| pool.pending_count());

    if !drain_ok {
        return Err(anyhow!(
            "buffer drain timeout after benchmark (pending={:?})",
            pending_after_drain
        ));
    }

    let metrics_delta = engine.metrics_snapshot().saturating_sub(&before);

    Ok(BenchResult {
        timed_phase,
        drain_phase,
        stats,
        metrics_delta,
        pending_after_timed_phase,
        pending_after_drain,
    })
}

fn run_workload(engine: &OnyxEngine, bench: &BenchConfig) -> Result<WorkerStats> {
    let barrier = Arc::new(Barrier::new(bench.threads + 1));
    let mut handles = Vec::with_capacity(bench.threads);
    let ops_per_thread = (bench.total_size / bench.io_size) / bench.threads as u64;
    let extra_ops = (bench.total_size / bench.io_size) % bench.threads as u64;

    for tid in 0..bench.threads {
        let volume = engine
            .open_volume(&bench.volume)
            .with_context(|| format!("failed to open volume '{}'", bench.volume))?;
        let barrier = barrier.clone();
        let scenario = bench.scenario;
        let pattern = bench.pattern;
        let io_size = bench.io_size;
        let volume_size = volume.size_bytes();
        let ops = ops_per_thread + u64::from(tid < extra_ops as usize);
        let start_ops = tid as u64 * ops_per_thread + (tid as u64).min(extra_ops);
        let base_lba = (start_ops * io_size) / BLOCK_SIZE as u64;

        let handle = thread::spawn(move || -> Result<WorkerStats> {
            let mut rng = XorShift64::seed(0x9E37_79B9_7F4A_7C15u64 ^ tid as u64);
            let mut io_buf = vec![0u8; io_size as usize];
            fill_buffer(pattern, &mut io_buf, &mut rng, tid, 0);
            barrier.wait();

            let mut stats = WorkerStats::default();
            for op_idx in 0..ops {
                let offset =
                    compute_offset(scenario, io_size, volume_size, base_lba, op_idx, &mut rng);

                if matches!(
                    scenario,
                    Scenario::SeqWrite | Scenario::RandWrite | Scenario::Overwrite
                ) {
                    fill_buffer(pattern, &mut io_buf, &mut rng, tid, op_idx);
                    volume.write(offset, &io_buf)?;
                } else {
                    let _ = volume.read(offset, io_buf.len())?;
                }

                stats.ops += 1;
                stats.bytes += io_size;
            }
            Ok(stats)
        });
        handles.push(handle);
    }

    barrier.wait();

    let mut total = WorkerStats::default();
    for handle in handles {
        let worker = handle
            .join()
            .map_err(|_| anyhow!("worker thread panicked"))??;
        total.ops += worker.ops;
        total.bytes += worker.bytes;
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
    let max_io = volume_size / io_size;
    let op = match scenario {
        Scenario::SeqWrite | Scenario::SeqRead => base_lba + op_idx * (io_size / BLOCK_SIZE as u64),
        Scenario::Overwrite => {
            let hot_ios = (max_io / 8).max(1);
            (rng.next_u64() % hot_ios) * (io_size / BLOCK_SIZE as u64)
        }
        Scenario::RandWrite | Scenario::RandRead => {
            (rng.next_u64() % max_io) * (io_size / BLOCK_SIZE as u64)
        }
    };
    op * BLOCK_SIZE as u64
}

fn fill_buffer(pattern: Pattern, buf: &mut [u8], rng: &mut XorShift64, tid: usize, op_idx: u64) {
    match pattern {
        Pattern::Zero => buf.fill(0),
        Pattern::Repeat => {
            let seed = ((tid as u64 * 131) ^ op_idx).to_le_bytes();
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

fn print_report(bench: &BenchConfig, result: &BenchResult) {
    let secs = result.timed_phase.as_secs_f64();
    let mbps = (result.stats.bytes as f64 / 1024.0 / 1024.0) / secs.max(f64::MIN_POSITIVE);
    let iops = result.stats.ops as f64 / secs.max(f64::MIN_POSITIVE);
    let avg_latency_us =
        result.timed_phase.as_secs_f64() * 1_000_000.0 / result.stats.ops.max(1) as f64;

    println!();
    println!("== Onyx Benchmark ==");
    println!("scenario: {:?}", bench.scenario);
    println!("pattern:  {:?}", bench.pattern);
    println!("threads:  {}", bench.threads);
    println!("bs:       {}", human_bytes(bench.io_size));
    println!("group commit wait: {} us", bench.group_commit_wait_us);
    println!("total:    {}", human_bytes(result.stats.bytes));
    println!("ops:      {}", result.stats.ops);
    println!("time:     {:.3}s", secs);
    println!("throughput: {:.2} MiB/s", mbps);
    println!("iops:       {:.0}", iops);
    println!("avg latency: {:.2} us/op", avg_latency_us);
    println!(
        "buffer pending: after timed phase={:?}, after drain={:?}",
        result.pending_after_timed_phase, result.pending_after_drain
    );
    println!("drain catch-up: {:.3}s", result.drain_phase.as_secs_f64());

    let m = &result.metrics_delta;
    println!();
    println!("== Metrics Delta ==");
    println!(
        "volume: writes={} write_bytes={} reads={} read_bytes={} partial_writes={} partial_reads={}",
        m.volume_write_ops,
        human_bytes(m.volume_write_bytes),
        m.volume_read_ops,
        human_bytes(m.volume_read_bytes),
        m.volume_partial_write_ops,
        m.volume_partial_read_ops,
    );
    println!(
        "read-path: buffer_hits={} lv3_hits={} unmapped={} crc_errors={} decompress_errors={}",
        m.read_buffer_hits,
        m.read_lv3_hits,
        m.read_unmapped,
        m.read_crc_errors,
        m.read_decompress_errors,
    );
    println!(
        "flush: coalesce_runs={} units={} lbas={} raw_bytes={} compress_in={} compress_out={} packed_slots={} packed_frags={} hole_fills={} errors={}",
        m.coalesce_runs,
        m.flush_units_written,
        m.coalesced_lbas,
        human_bytes(m.coalesced_bytes),
        human_bytes(m.compress_input_bytes),
        human_bytes(m.compress_output_bytes),
        m.flush_packed_slots_written,
        m.flush_packed_fragments_written,
        m.flush_hole_fills,
        m.flush_errors,
    );
    println!(
        "dedup: hits={} misses={} skipped_units={} hit_failures={} rescanned_blocks={}",
        m.dedup_hits,
        m.dedup_misses,
        m.dedup_skipped_units,
        m.dedup_hit_failures,
        m.dedup_rescan_blocks,
    );
    println!(
        "gc: cycles={} paused={} candidates={} rewrite_attempts={} rewritten_blocks={} errors={}",
        m.gc_cycles,
        m.gc_paused_cycles,
        m.gc_candidates_found,
        m.gc_rewrite_attempts,
        m.gc_blocks_rewritten,
        m.gc_errors,
    );

    println!();
    println!("== Hints ==");
    for hint in bottleneck_hints(bench, result) {
        println!("- {}", hint);
    }
}

fn bottleneck_hints(bench: &BenchConfig, result: &BenchResult) -> Vec<String> {
    let mut hints = Vec::new();
    let m = &result.metrics_delta;

    if result.drain_phase > result.timed_phase / 2
        && matches!(
            bench.scenario,
            Scenario::SeqWrite | Scenario::RandWrite | Scenario::Overwrite
        )
    {
        hints.push(
            "flush catch-up is significant after the timed phase; background flusher/compress/writer likely cannot keep up with front-end writes".to_string(),
        );
    }

    if m.read_buffer_hits > 0 && m.read_lv3_hits == 0 {
        hints.push(
            "reads were mostly served from the in-memory/buffer path; this is good for latency but does not represent cold-read LV3 performance".to_string(),
        );
    }

    if m.read_lv3_hits > 0 && m.compress_output_bytes > 0 {
        hints.push(
            "LV3 reads and decompression were exercised; if throughput is low here, inspect compression choice and packed fragment behavior".to_string(),
        );
    }

    if m.dedup_hits > 0 {
        hints.push(format!(
            "dedup is active and hit {} times during this run; compare against --pattern random or --compression none to isolate dedup cost/benefit",
            m.dedup_hits
        ));
    }

    if m.dedup_skipped_units > 0 {
        hints.push(
            "dedup skipped some units due to buffer pressure; this usually means the front-end is outrunning the background pipeline".to_string(),
        );
    }

    if m.gc_blocks_rewritten > 0 || m.gc_candidates_found > 0 {
        hints.push(
            "GC was active during the run; benchmark again with GC disabled to measure the pure foreground path".to_string(),
        );
    }

    if m.flush_packed_slots_written > 0 {
        hints.push(
            "packer was exercised; if CPU is high and throughput is low on small-block workloads, inspect packed slot write amplification and hole-fill frequency".to_string(),
        );
    }

    if m.flush_errors > 0
        || m.gc_errors > 0
        || m.read_crc_errors > 0
        || m.read_decompress_errors > 0
    {
        hints.push(
            "errors were observed in the benchmark metrics; resolve correctness/pathology issues before trusting throughput numbers".to_string(),
        );
    }

    if hints.is_empty() {
        hints.push(
            "no obvious pathology surfaced from the internal counters; compare this run across compression modes, patterns, and thread counts to isolate scaling limits".to_string(),
        );
    }

    hints
}

fn parse_size(input: &str) -> Result<u64, String> {
    let s = input.trim().to_ascii_lowercase();
    let split_idx = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (digits, suffix) = s.split_at(split_idx);
    if digits.is_empty() {
        return Err(format!("invalid size '{}'", input));
    }
    let base: u64 = digits
        .parse()
        .map_err(|_| format!("invalid size '{}'", input))?;
    let multiplier = match suffix {
        "" | "b" => 1,
        "k" | "kb" => 1024,
        "m" | "mb" => 1024 * 1024,
        "g" | "gb" => 1024 * 1024 * 1024,
        "t" | "tb" => 1024_u64.pow(4),
        _ => return Err(format!("unsupported size suffix '{}'", suffix)),
    };
    base.checked_mul(multiplier)
        .ok_or_else(|| format!("size '{}' overflows u64", input))
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
