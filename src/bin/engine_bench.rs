use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use clap::{Parser, ValueEnum};

use onyx_storage::config::OnyxConfig;
use onyx_storage::engine::OnyxEngine;
use onyx_storage::metrics::EngineMetricsSnapshot;
use onyx_storage::types::{CompressionAlgo, BLOCK_SIZE};

#[derive(Debug, Parser)]
#[command(
    name = "onyx-engine-bench",
    about = "Run fio-like mixed workloads directly against OnyxEngine without ublk"
)]
struct Cli {
    #[arg(short, long, default_value = "config/nvme-detailed.toml")]
    config: std::path::PathBuf,

    #[arg(long, default_value = "engine-bench")]
    volume: String,

    #[arg(long, default_value = "320g", value_parser = parse_size)]
    volume_size: u64,

    #[arg(long, default_value = "32g", value_parser = parse_size)]
    working_set: u64,

    #[arg(long, default_value_t = 60)]
    runtime_secs: u64,

    #[arg(long, default_value_t = 16)]
    jobs: usize,

    #[arg(long, default_value_t = 70)]
    rwmixread: u8,

    #[arg(long, default_value = "4k", value_parser = parse_size)]
    min_bs: u64,

    #[arg(long, default_value = "32k", value_parser = parse_size)]
    max_bs: u64,

    #[arg(long, value_enum, default_value_t = Pattern::Random)]
    pattern: Pattern,

    #[arg(long, value_enum, default_value_t = CompressionChoice::Lz4)]
    compression: CompressionChoice,

    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    reset: bool,

    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    reset_buffer: bool,

    #[arg(long, default_value = "64m", value_parser = parse_size)]
    reset_buffer_bytes: u64,

    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    prefill: bool,

    #[arg(long, default_value_t = 120)]
    drain_timeout_secs: u64,
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

#[derive(Debug, Default, Clone, Copy)]
struct BenchStats {
    read_ops: u64,
    read_bytes: u64,
    write_ops: u64,
    write_bytes: u64,
}

impl BenchStats {
    fn add(&mut self, other: Self) {
        self.read_ops += other.read_ops;
        self.read_bytes += other.read_bytes;
        self.write_ops += other.write_ops;
        self.write_bytes += other.write_bytes;
    }

    fn total_ops(&self) -> u64 {
        self.read_ops + self.write_ops
    }

    fn total_bytes(&self) -> u64 {
        self.read_bytes + self.write_bytes
    }
}

#[derive(Debug, Default)]
struct LatencySamples {
    read_ns: Vec<u64>,
    write_ns: Vec<u64>,
}

impl LatencySamples {
    fn merge(&mut self, mut other: Self) {
        self.read_ns.append(&mut other.read_ns);
        self.write_ns.append(&mut other.write_ns);
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "onyx_storage=warn".to_string()),
        )
        .init();

    let cli = Cli::parse();
    validate_args(&cli)?;

    if cli.reset {
        remove_configured_paths(&cli.config, cli.reset_buffer, cli.reset_buffer_bytes)?;
    }

    let config = OnyxConfig::load(&cli.config)?;
    let engine = OnyxEngine::open(&config)?;
    if cli.reset {
        let _ = engine.delete_volume(&cli.volume);
        engine
            .create_volume(&cli.volume, cli.volume_size, cli.compression.into_algo())
            .with_context(|| format!("failed to create volume '{}'", cli.volume))?;
    }

    let volume = engine
        .open_volume(&cli.volume)
        .with_context(|| format!("failed to open volume '{}'", cli.volume))?;

    if cli.prefill {
        println!(
            "prefill: {} over working_set={} bs={}",
            cli.volume,
            human_bytes(cli.working_set),
            human_bytes(cli.max_bs)
        );
        prefill(&volume, &cli)?;
        if !wait_for_drain(&engine, Duration::from_secs(cli.drain_timeout_secs)) {
            return Err(anyhow!("prefill drain timeout"));
        }
    }

    let before = engine.metrics_snapshot();
    let pending_before = engine.status_snapshot()?.buffer_pending_entries;
    let started = Instant::now();
    let (stats, samples) = run_mixed(&engine, &cli)?;
    let elapsed = started.elapsed();
    let pending_after = engine.status_snapshot()?.buffer_pending_entries;
    let metrics = engine.metrics_snapshot().saturating_sub(&before);

    print_report(
        &cli,
        stats,
        samples,
        elapsed,
        pending_before,
        pending_after,
        &metrics,
    );
    engine.shutdown()?;
    Ok(())
}

fn validate_args(cli: &Cli) -> Result<()> {
    if cli.jobs == 0 {
        return Err(anyhow!("--jobs must be > 0"));
    }
    if cli.runtime_secs == 0 {
        return Err(anyhow!("--runtime-secs must be > 0"));
    }
    if cli.rwmixread > 100 {
        return Err(anyhow!("--rwmixread must be <= 100"));
    }
    if cli.min_bs == 0 || cli.max_bs == 0 || cli.min_bs > cli.max_bs {
        return Err(anyhow!("invalid bs range"));
    }
    if cli.min_bs % BLOCK_SIZE as u64 != 0 || cli.max_bs % BLOCK_SIZE as u64 != 0 {
        return Err(anyhow!("bs range must be aligned to {BLOCK_SIZE} bytes"));
    }
    if cli.working_set == 0 || cli.working_set > cli.volume_size {
        return Err(anyhow!("--working-set must be in 1..=volume-size"));
    }
    if cli.working_set % BLOCK_SIZE as u64 != 0 || cli.volume_size % BLOCK_SIZE as u64 != 0 {
        return Err(anyhow!(
            "volume size and working set must be {BLOCK_SIZE}-byte aligned"
        ));
    }
    if cli.working_set < cli.max_bs {
        return Err(anyhow!("--working-set must be >= --max-bs"));
    }
    Ok(())
}

fn remove_configured_paths(
    config_path: &std::path::Path,
    reset_buffer: bool,
    reset_buffer_bytes: u64,
) -> Result<()> {
    let config = OnyxConfig::load(config_path)?;
    if let Some(path) = config.meta.path.as_ref() {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _ = std::fs::remove_dir_all(path);
    }
    if let Some(wal_dir) = config.meta.wal_dir.as_ref() {
        if let Some(parent) = wal_dir.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _ = std::fs::remove_dir_all(wal_dir);
    }
    if reset_buffer {
        reset_buffer_prefix(&config, reset_buffer_bytes)?;
    }
    Ok(())
}

fn reset_buffer_prefix(config: &OnyxConfig, bytes: u64) -> Result<()> {
    let Some(path) = config.buffer.device.as_ref() else {
        return Ok(());
    };
    if bytes == 0 {
        return Ok(());
    }

    let mut file = OpenOptions::new()
        .write(true)
        .open(path)
        .with_context(|| format!("failed to open buffer device {:?} for reset", path))?;
    file.seek(SeekFrom::Start(0))
        .with_context(|| format!("failed to seek buffer device {:?}", path))?;

    let zero = vec![0u8; 1024 * 1024];
    let mut remaining = bytes;
    while remaining > 0 {
        let len = zero.len().min(remaining as usize);
        file.write_all(&zero[..len])
            .with_context(|| format!("failed to zero buffer device {:?}", path))?;
        remaining -= len as u64;
    }
    file.sync_all()
        .with_context(|| format!("failed to sync buffer device {:?}", path))?;
    Ok(())
}

fn prefill(volume: &onyx_storage::volume::OnyxVolume, cli: &Cli) -> Result<()> {
    let mut rng = XorShift64::seed(0x1234_5678_9abc_def0);
    let mut buf = vec![0u8; cli.max_bs as usize];
    let mut offset = 0u64;
    while offset < cli.working_set {
        let len = (cli.max_bs).min(cli.working_set - offset);
        let len = align_down(len, BLOCK_SIZE as u64).max(BLOCK_SIZE as u64);
        fill_buffer(
            cli.pattern,
            &mut buf[..len as usize],
            &mut rng,
            0,
            offset / BLOCK_SIZE as u64,
        );
        volume.write(offset, &buf[..len as usize])?;
        offset += len;
    }
    Ok(())
}

fn run_mixed(engine: &OnyxEngine, cli: &Cli) -> Result<(BenchStats, LatencySamples)> {
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(cli.jobs + 1));
    let samples = Arc::new(Mutex::new(LatencySamples::default()));
    let mut handles = Vec::with_capacity(cli.jobs);

    for job in 0..cli.jobs {
        let volume = engine.open_volume(&cli.volume)?;
        let stop = stop.clone();
        let barrier = barrier.clone();
        let samples = samples.clone();
        let pattern = cli.pattern;
        let rwmixread = cli.rwmixread;
        let min_blocks = cli.min_bs / BLOCK_SIZE as u64;
        let max_blocks = cli.max_bs / BLOCK_SIZE as u64;
        let max_start_lba = (cli.working_set / BLOCK_SIZE as u64).saturating_sub(max_blocks);
        let handle = thread::Builder::new()
            .name(format!("engine-bench-{job}"))
            .spawn(move || -> Result<BenchStats> {
                let mut rng = XorShift64::seed(0x9e37_79b9_7f4a_7c15 ^ job as u64);
                let mut write_buf = vec![0u8; (max_blocks * BLOCK_SIZE as u64) as usize];
                let mut read_buf = vec![0u8; (max_blocks * BLOCK_SIZE as u64) as usize];
                let mut local_samples = LatencySamples::default();
                let mut stats = BenchStats::default();
                barrier.wait();

                while !stop.load(Ordering::Relaxed) {
                    let blocks = min_blocks + (rng.next_u64() % (max_blocks - min_blocks + 1));
                    let len = (blocks * BLOCK_SIZE as u64) as usize;
                    let start_lba = if max_start_lba == 0 {
                        0
                    } else {
                        rng.next_u64() % (max_start_lba + 1)
                    };
                    let offset = start_lba * BLOCK_SIZE as u64;
                    let is_read = (rng.next_u64() % 100) < rwmixread as u64;
                    let start = Instant::now();
                    if is_read {
                        volume.read_into(offset, &mut read_buf[..len])?;
                        local_samples
                            .read_ns
                            .push(start.elapsed().as_nanos() as u64);
                        stats.read_ops += 1;
                        stats.read_bytes += len as u64;
                    } else {
                        fill_buffer(
                            pattern,
                            &mut write_buf[..len],
                            &mut rng,
                            job,
                            stats.write_ops,
                        );
                        volume.write(offset, &write_buf[..len])?;
                        local_samples
                            .write_ns
                            .push(start.elapsed().as_nanos() as u64);
                        stats.write_ops += 1;
                        stats.write_bytes += len as u64;
                    }
                }

                let mut merged = samples.lock().unwrap();
                merged.merge(local_samples);
                Ok(stats)
            })?;
        handles.push(handle);
    }

    barrier.wait();
    thread::sleep(Duration::from_secs(cli.runtime_secs));
    stop.store(true, Ordering::Relaxed);

    let mut stats = BenchStats::default();
    for handle in handles {
        stats.add(handle.join().map_err(|_| anyhow!("worker panicked"))??);
    }
    let samples = Arc::try_unwrap(samples)
        .map_err(|_| anyhow!("latency samples still shared"))?
        .into_inner()
        .unwrap();
    Ok((stats, samples))
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
        thread::sleep(Duration::from_millis(50));
    }
    pool.pending_count() == 0
}

fn print_report(
    cli: &Cli,
    stats: BenchStats,
    mut samples: LatencySamples,
    elapsed: Duration,
    pending_before: Option<u64>,
    pending_after: Option<u64>,
    metrics: &EngineMetricsSnapshot,
) {
    samples.read_ns.sort_unstable();
    samples.write_ns.sort_unstable();
    let secs = elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
    let read_iops = stats.read_ops as f64 / secs;
    let write_iops = stats.write_ops as f64 / secs;
    let total_iops = stats.total_ops() as f64 / secs;
    let mibps = stats.total_bytes() as f64 / 1024.0 / 1024.0 / secs;

    println!("{{");
    println!("  \"kind\": \"onyx-engine-bench\",");
    println!("  \"volume\": {:?},", cli.volume);
    println!("  \"runtime_secs\": {:.3},", secs);
    println!("  \"jobs\": {},", cli.jobs);
    println!("  \"rwmixread\": {},", cli.rwmixread);
    println!("  \"working_set_bytes\": {},", cli.working_set);
    println!("  \"read_iops\": {:.3},", read_iops);
    println!("  \"write_iops\": {:.3},", write_iops);
    println!("  \"total_iops\": {:.3},", total_iops);
    println!("  \"throughput_mib_s\": {:.3},", mibps);
    println!("  \"read_p50_ns\": {},", percentile(&samples.read_ns, 50.0));
    println!("  \"read_p95_ns\": {},", percentile(&samples.read_ns, 95.0));
    println!("  \"read_p99_ns\": {},", percentile(&samples.read_ns, 99.0));
    println!(
        "  \"read_p999_ns\": {},",
        percentile(&samples.read_ns, 99.9)
    );
    println!(
        "  \"write_p99_ns\": {},",
        percentile(&samples.write_ns, 99.0)
    );
    println!(
        "  \"write_p999_ns\": {},",
        percentile(&samples.write_ns, 99.9)
    );
    println!("  \"pending_before\": {},", json_option_u64(pending_before));
    println!("  \"pending_after\": {},", json_option_u64(pending_after));
    println!("  \"read_submit_calls\": {},", metrics.read_submit_calls);
    println!(
        "  \"read_submit_total_ns\": {},",
        metrics.read_submit_total_ns
    );
    println!(
        "  \"read_submit_buffer_lookup_ns\": {},",
        metrics.read_submit_buffer_lookup_ns
    );
    println!(
        "  \"read_submit_meta_get_ns\": {},",
        metrics.read_submit_meta_get_ns
    );
    println!(
        "  \"read_submit_unit_io_ns\": {},",
        metrics.read_submit_unit_io_ns
    );
    println!("  \"read_buffer_hits\": {},", metrics.read_buffer_hits);
    println!("  \"read_lv3_hits\": {},", metrics.read_lv3_hits);
    println!("  \"read_unmapped\": {},", metrics.read_unmapped);
    println!("  \"dedup_lookups\": {},", metrics.dedup_lookup_ops);
    println!("  \"dedup_lookup_ns\": {},", metrics.dedup_lookup_ns);
    println!("  \"dedup_misses\": {},", metrics.dedup_misses);
    println!("  \"dedup_hits\": {},", metrics.dedup_hits);
    println!(
        "  \"flush_writer_total_ns\": {},",
        metrics.flush_writer_total_ns
    );
    println!(
        "  \"flush_writer_meta_ns\": {},",
        metrics.flush_writer_meta_ns
    );
    println!("  \"flush_errors\": {},", metrics.flush_errors);
    println!("  \"read_crc_errors\": {}", metrics.read_crc_errors);
    println!("}}");
}

fn json_option_u64(value: Option<u64>) -> String {
    value
        .map(|v| v.to_string())
        .unwrap_or_else(|| "null".to_string())
}

fn percentile(values: &[u64], pct: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let rank = ((pct / 100.0) * (values.len().saturating_sub(1) as f64)).round() as usize;
    values[rank.min(values.len() - 1)]
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

fn align_down(value: u64, align: u64) -> u64 {
    value / align * align
}

fn parse_size(input: &str) -> Result<u64, String> {
    let s = input.trim().to_ascii_lowercase();
    let split_idx = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (digits, suffix) = s.split_at(split_idx);
    if digits.is_empty() {
        return Err(format!("invalid size '{input}'"));
    }
    let base: u64 = digits
        .parse()
        .map_err(|_| format!("invalid size '{input}'"))?;
    let multiplier = match suffix {
        "" | "b" => 1,
        "k" | "kb" => 1024,
        "m" | "mb" => 1024 * 1024,
        "g" | "gb" => 1024 * 1024 * 1024,
        "t" | "tb" => 1024_u64.pow(4),
        _ => return Err(format!("unsupported size suffix '{suffix}'")),
    };
    base.checked_mul(multiplier)
        .ok_or_else(|| format!("size '{input}' overflows u64"))
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
