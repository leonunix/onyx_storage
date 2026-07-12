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
use onyx_storage::metrics::{EngineMetricsSnapshot, MetaMemorySnapshot};
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

    /// Comma/range CPU list to pin bench job threads to (e.g.
    /// "32,34,36,38,40,42,44,46,80,82,84,86,88,90,92,94"). Empty = inherit the
    /// process affinity mask. Used for core-isolation: keep the load generator
    /// on a disjoint node0 core set from the engine's internal threads so the
    /// test never competes with the sync/commit threads for scheduling.
    #[arg(long, default_value = "")]
    job_cpus: String,
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

    let status_before = engine.status_snapshot()?;
    let before = engine.metrics_snapshot();
    let pending_before = status_before.buffer_pending_entries;
    let meta_before = status_before.metadb_memory;
    let started = Instant::now();
    let (stats, samples) = run_mixed(&engine, &cli)?;
    let elapsed = started.elapsed();
    let status_after = engine.status_snapshot()?;
    let pending_after = status_after.buffer_pending_entries;
    let meta_delta = meta_before.and_then(|before| {
        status_after
            .metadb_memory
            .map(|after| after.saturating_sub(&before))
    });
    let metrics = engine.metrics_snapshot().saturating_sub(&before);

    print_report(
        &cli,
        stats,
        samples,
        elapsed,
        pending_before,
        pending_after,
        &metrics,
        meta_delta.as_ref(),
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

/// Parse a comma/range CPU spec (same grammar as the engine's affinity config:
/// "0-3,8,10-12"). Ranges expand to every integer in [start,end]; pass explicit
/// even-only lists when targeting one NUMA node on an interleaved topology.
fn parse_cpu_list(spec: &str) -> Vec<usize> {
    let mut cpus = Vec::new();
    for part in spec.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        if let Some((start, end)) = part.split_once('-') {
            if let (Ok(start), Ok(end)) =
                (start.trim().parse::<usize>(), end.trim().parse::<usize>())
            {
                if start <= end {
                    cpus.extend(start..=end);
                }
            }
        } else if let Ok(cpu) = part.parse::<usize>() {
            cpus.push(cpu);
        }
    }
    cpus.sort_unstable();
    cpus.dedup();
    cpus
}

/// Pin the calling thread to the given CPU set via sched_setaffinity. A thread
/// may move itself onto CPUs outside the mask it inherited from the process
/// (e.g. one set by `numactl --physcpubind`), so this is how the bench job
/// threads escape the engine's core set A onto the load-gen core set B.
#[cfg(target_os = "linux")]
fn pin_current_thread_to(cpus: &[usize]) -> std::io::Result<()> {
    if cpus.is_empty() {
        return Ok(());
    }
    const CPU_SETSIZE: usize = 1024;
    const BITS_PER_WORD: usize = 8 * std::mem::size_of::<libc::c_ulong>();
    let mut set = [0 as libc::c_ulong; CPU_SETSIZE / BITS_PER_WORD];
    for &cpu in cpus {
        if cpu < CPU_SETSIZE {
            set[cpu / BITS_PER_WORD] |= (1 as libc::c_ulong) << (cpu % BITS_PER_WORD);
        }
    }
    let rc = unsafe {
        libc::sched_setaffinity(
            0,
            std::mem::size_of_val(&set),
            set.as_ptr().cast::<libc::cpu_set_t>(),
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn pin_current_thread_to(_cpus: &[usize]) -> std::io::Result<()> {
    Ok(())
}

fn run_mixed(engine: &OnyxEngine, cli: &Cli) -> Result<(BenchStats, LatencySamples)> {
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(cli.jobs + 1));
    let samples = Arc::new(Mutex::new(LatencySamples::default()));
    let mut handles = Vec::with_capacity(cli.jobs);

    let job_cpus = Arc::new(parse_cpu_list(&cli.job_cpus));
    if !job_cpus.is_empty() {
        eprintln!(
            "pinning {} bench job threads to CPUs {:?}",
            cli.jobs, job_cpus
        );
    }

    for job in 0..cli.jobs {
        let volume = engine.open_volume(&cli.volume)?;
        let stop = stop.clone();
        let barrier = barrier.clone();
        let samples = samples.clone();
        let job_cpus = job_cpus.clone();
        let pattern = cli.pattern;
        let rwmixread = cli.rwmixread;
        let min_blocks = cli.min_bs / BLOCK_SIZE as u64;
        let max_blocks = cli.max_bs / BLOCK_SIZE as u64;
        let max_start_lba = (cli.working_set / BLOCK_SIZE as u64).saturating_sub(max_blocks);
        let handle = thread::Builder::new()
            .name(format!("engine-bench-{job}"))
            .spawn(move || -> Result<BenchStats> {
                if let Err(err) = pin_current_thread_to(&job_cpus) {
                    eprintln!("warn: failed to pin bench job {job} to {job_cpus:?}: {err}");
                }
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
    meta: Option<&MetaMemorySnapshot>,
) {
    samples.read_ns.sort_unstable();
    samples.write_ns.sort_unstable();
    let secs = elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
    let read_iops = stats.read_ops as f64 / secs;
    let write_iops = stats.write_ops as f64 / secs;
    let total_iops = stats.total_ops() as f64 / secs;
    let mibps = stats.total_bytes() as f64 / 1024.0 / 1024.0 / secs;
    let avg = |num: u64, den: u64| -> f64 {
        if den == 0 {
            0.0
        } else {
            num as f64 / den as f64
        }
    };
    let mapped_lbas = metrics.read_lv3_hits;
    let lv3_bytes_per_lba = avg(metrics.lv3_read_compressed_bytes, mapped_lbas);
    let lv3_reads_per_submit = avg(metrics.lv3_read_ops, metrics.read_submit_calls);
    let read_pool_batch_avg = avg(metrics.read_pool_batch_ops, metrics.read_pool_batches);
    let read_pool_queue_p95 =
        latency_bucket_percentile(&metrics.read_pool_queue_wait_latency_buckets, 95.0);
    let read_pool_queue_p99 =
        latency_bucket_percentile(&metrics.read_pool_queue_wait_latency_buckets, 99.0);
    let read_pool_queue_p999 =
        latency_bucket_percentile(&metrics.read_pool_queue_wait_latency_buckets, 99.9);
    let read_pool_submit_p95 =
        latency_bucket_percentile(&metrics.read_pool_submit_wait_latency_buckets, 95.0);
    let read_pool_submit_p99 =
        latency_bucket_percentile(&metrics.read_pool_submit_wait_latency_buckets, 99.0);
    let read_pool_submit_p999 =
        latency_bucket_percentile(&metrics.read_pool_submit_wait_latency_buckets, 99.9);
    let read_pool_decode_p99 =
        latency_bucket_percentile(&metrics.read_pool_decode_latency_buckets, 99.0);
    let worker_stats = read_pool_worker_stats(metrics);
    let read_pool_worker_top_share_pct = if metrics.read_pool_batch_ops == 0 {
        0.0
    } else {
        worker_stats.top_requests as f64 * 100.0 / metrics.read_pool_batch_ops as f64
    };
    let read_submit_unit_io_p95 =
        latency_bucket_percentile(&metrics.read_submit_unit_io_latency_buckets, 95.0);
    let read_submit_unit_io_p99 =
        latency_bucket_percentile(&metrics.read_submit_unit_io_latency_buckets, 99.0);
    let read_submit_unit_io_p999 =
        latency_bucket_percentile(&metrics.read_submit_unit_io_latency_buckets, 99.9);

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
    println!(
        "  \"read_submit_unit_io_p95_ns\": {},",
        read_submit_unit_io_p95
    );
    println!(
        "  \"read_submit_unit_io_p99_ns\": {},",
        read_submit_unit_io_p99
    );
    println!(
        "  \"read_submit_unit_io_p999_ns\": {},",
        read_submit_unit_io_p999
    );
    println!("  \"read_buffer_hits\": {},", metrics.read_buffer_hits);
    println!("  \"read_lv3_hits\": {},", metrics.read_lv3_hits);
    println!("  \"lv3_read_ops\": {},", metrics.lv3_read_ops);
    println!(
        "  \"lv3_read_compressed_bytes\": {},",
        metrics.lv3_read_compressed_bytes
    );
    println!(
        "  \"lv3_read_bytes_per_mapped_lba\": {:.3},",
        lv3_bytes_per_lba
    );
    println!("  \"lv3_reads_per_submit\": {:.3},", lv3_reads_per_submit);
    println!("  \"read_unmapped\": {},", metrics.read_unmapped);
    println!("  \"read_pool_requests\": {},", metrics.read_pool_requests);
    println!("  \"read_pool_batches\": {},", metrics.read_pool_batches);
    println!(
        "  \"read_pool_batch_ops\": {},",
        metrics.read_pool_batch_ops
    );
    println!("  \"read_pool_batch_avg\": {:.3},", read_pool_batch_avg);
    println!("  \"read_pool_worker_active\": {},", worker_stats.active);
    println!("  \"read_pool_worker_top_idx\": {},", worker_stats.top_idx);
    println!(
        "  \"read_pool_worker_top_requests\": {},",
        worker_stats.top_requests
    );
    println!(
        "  \"read_pool_worker_top_share_pct\": {:.3},",
        read_pool_worker_top_share_pct
    );
    println!(
        "  \"read_pool_worker_top_queue_wait_avg_ns\": {:.3},",
        avg_u64(worker_stats.top_queue_wait_ns, worker_stats.top_requests)
    );
    println!(
        "  \"read_pool_worker_top_submit_wait_avg_ns\": {:.3},",
        avg_u64(worker_stats.top_submit_wait_ns, worker_stats.top_batches)
    );
    println!(
        "  \"read_pool_queue_wait_ns\": {},",
        metrics.read_pool_queue_wait_ns
    );
    println!(
        "  \"read_pool_queue_wait_p95_ns\": {},",
        read_pool_queue_p95
    );
    println!(
        "  \"read_pool_queue_wait_p99_ns\": {},",
        read_pool_queue_p99
    );
    println!(
        "  \"read_pool_queue_wait_p999_ns\": {},",
        read_pool_queue_p999
    );
    println!(
        "  \"read_pool_coalesce_wait_ns\": {},",
        metrics.read_pool_coalesce_wait_ns
    );
    println!("  \"read_pool_alloc_ns\": {},", metrics.read_pool_alloc_ns);
    println!(
        "  \"read_pool_submit_wait_ns\": {},",
        metrics.read_pool_submit_wait_ns
    );
    println!(
        "  \"read_pool_submit_wait_p95_ns\": {},",
        read_pool_submit_p95
    );
    println!(
        "  \"read_pool_submit_wait_p99_ns\": {},",
        read_pool_submit_p99
    );
    println!(
        "  \"read_pool_submit_wait_p999_ns\": {},",
        read_pool_submit_p999
    );
    println!(
        "  \"read_pool_decode_ns\": {},",
        metrics.read_pool_decode_ns
    );
    println!("  \"read_pool_decode_p99_ns\": {},", read_pool_decode_p99);
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
    println!("  \"read_crc_errors\": {},", metrics.read_crc_errors);
    print_meta_report(meta);
    println!("}}");
}

fn print_meta_report(meta: Option<&MetaMemorySnapshot>) {
    let Some(meta) = meta else {
        println!("  \"metadb_available\": false");
        return;
    };
    println!("  \"metadb_available\": true,");
    println!("  \"metadb_commit_success\": {},", meta.commit_success);
    println!("  \"metadb_commit_ops\": {},", meta.commit_ops);
    println!("  \"metadb_commit_total_us\": {},", meta.commit_total_us);
    println!(
        "  \"metadb_commit_total_avg_us\": {:.3},",
        avg_u64(meta.commit_total_us, meta.commit_success)
    );
    println!(
        "  \"metadb_commit_total_max_us\": {},",
        meta.commit_total_max_us
    );
    println!(
        "  \"metadb_commit_wal_submit_us\": {},",
        meta.commit_wal_submit_us
    );
    println!(
        "  \"metadb_commit_wal_submit_avg_us\": {:.3},",
        avg_u64(meta.commit_wal_submit_us, meta.commit_success)
    );
    println!(
        "  \"metadb_commit_wal_submit_max_us\": {},",
        meta.commit_wal_submit_max_us
    );
    println!(
        "  \"metadb_commit_apply_wait_us\": {},",
        meta.commit_apply_wait_us
    );
    println!(
        "  \"metadb_commit_apply_wait_avg_us\": {:.3},",
        avg_u64(meta.commit_apply_wait_us, meta.commit_success)
    );
    println!(
        "  \"metadb_commit_apply_wait_max_us\": {},",
        meta.commit_apply_wait_max_us
    );
    println!(
        "  \"metadb_commit_apply_gate_wait_us\": {},",
        meta.commit_apply_gate_wait_us
    );
    println!(
        "  \"metadb_commit_apply_gate_wait_avg_us\": {:.3},",
        avg_u64(meta.commit_apply_gate_wait_us, meta.commit_success)
    );
    println!(
        "  \"metadb_commit_apply_gate_wait_max_us\": {},",
        meta.commit_apply_gate_wait_max_us
    );
    println!("  \"metadb_commit_apply_us\": {},", meta.commit_apply_us);
    println!(
        "  \"metadb_commit_apply_avg_us\": {:.3},",
        avg_u64(meta.commit_apply_us, meta.commit_success)
    );
    println!(
        "  \"metadb_commit_apply_max_us\": {},",
        meta.commit_apply_max_us
    );
    println!(
        "  \"metadb_commit_apply_l2p_wait_us\": {},",
        meta.commit_apply_l2p_wait_us
    );
    println!(
        "  \"metadb_commit_apply_rc_wait_us\": {},",
        meta.commit_apply_rc_wait_us
    );
    println!(
        "  \"metadb_commit_apply_dedup_wait_us\": {},",
        meta.commit_apply_dedup_wait_us
    );
    println!("  \"metadb_wal_submit_calls\": {},", meta.wal_submit_calls);
    println!(
        "  \"metadb_wal_submit_wait_us\": {},",
        meta.wal_submit_wait_us
    );
    println!(
        "  \"metadb_wal_submit_wait_avg_us\": {:.3},",
        avg_u64(meta.wal_submit_wait_us, meta.wal_submit_calls)
    );
    println!(
        "  \"metadb_wal_submit_wait_max_us\": {},",
        meta.wal_submit_wait_max_us
    );
    println!("  \"metadb_wal_batches\": {},", meta.wal_batches);
    println!("  \"metadb_wal_records\": {},", meta.wal_records);
    println!(
        "  \"metadb_wal_records_per_batch\": {:.3},",
        avg_u64(meta.wal_records, meta.wal_batches)
    );
    println!("  \"metadb_wal_bytes\": {},", meta.wal_bytes);
    println!("  \"metadb_wal_fsyncs\": {},", meta.wal_fsyncs);
    println!("  \"metadb_wal_write_us\": {},", meta.wal_write_us);
    println!(
        "  \"metadb_wal_write_avg_us\": {:.3},",
        avg_u64(meta.wal_write_us, meta.wal_batches)
    );
    println!("  \"metadb_wal_write_max_us\": {},", meta.wal_write_max_us);
    println!("  \"metadb_wal_fsync_us\": {},", meta.wal_fsync_us);
    println!(
        "  \"metadb_wal_fsync_avg_us\": {:.3},",
        avg_u64(meta.wal_fsync_us, meta.wal_fsyncs)
    );
    println!("  \"metadb_wal_fsync_max_us\": {},", meta.wal_fsync_max_us);
    println!(
        "  \"metadb_wal_batch_records_max\": {},",
        meta.wal_batch_records_max
    );
    println!(
        "  \"metadb_apply_l2p_remap_count\": {},",
        meta.apply_l2p_remap_count
    );
    println!(
        "  \"metadb_apply_l2p_remap_us\": {},",
        meta.apply_l2p_remap_us
    );
    println!(
        "  \"metadb_apply_l2p_remap_avg_us\": {:.3},",
        avg_u64(meta.apply_l2p_remap_us, meta.apply_l2p_remap_count)
    );
    println!(
        "  \"metadb_apply_l2p_remap_max_us\": {},",
        meta.apply_l2p_remap_max_us
    );
    println!(
        "  \"metadb_apply_refcount_count\": {},",
        meta.apply_refcount_count
    );
    println!(
        "  \"metadb_apply_refcount_us\": {},",
        meta.apply_refcount_us
    );
    println!(
        "  \"metadb_apply_refcount_avg_us\": {:.3},",
        avg_u64(meta.apply_refcount_us, meta.apply_refcount_count)
    );
    println!(
        "  \"metadb_apply_refcount_max_us\": {},",
        meta.apply_refcount_max_us
    );
    println!(
        "  \"metadb_apply_refcount_batch_count\": {},",
        meta.apply_refcount_batch_count
    );
    println!(
        "  \"metadb_apply_refcount_batch_actions\": {},",
        meta.apply_refcount_batch_actions
    );
    println!(
        "  \"metadb_apply_refcount_batch_pbas\": {},",
        meta.apply_refcount_batch_pbas
    );
    println!(
        "  \"metadb_apply_refcount_breakdown_sampled_pbas\": {},",
        meta.apply_refcount_breakdown_sampled_pbas
    );
    println!(
        "  \"metadb_apply_refcount_pba_grouping_us\": {},",
        meta.apply_refcount_pba_grouping_us
    );
    println!(
        "  \"metadb_apply_refcount_base_page_lookup_us\": {},",
        meta.apply_refcount_base_page_lookup_us
    );
    println!(
        "  \"metadb_apply_refcount_pending_slot_scan_us\": {},",
        meta.apply_refcount_pending_slot_scan_us
    );
    println!(
        "  \"metadb_apply_refcount_delta_merge_us\": {},",
        meta.apply_refcount_delta_merge_us
    );
    println!(
        "  \"metadb_apply_dedup_count\": {},",
        meta.apply_dedup_count
    );
    println!("  \"metadb_apply_dedup_us\": {},", meta.apply_dedup_us);
    println!(
        "  \"metadb_apply_dedup_avg_us\": {:.3},",
        avg_u64(meta.apply_dedup_us, meta.apply_dedup_count)
    );
    println!(
        "  \"metadb_apply_dedup_max_us\": {},",
        meta.apply_dedup_max_us
    );
    println!("  \"metadb_dedup_lane_tasks\": {},", meta.dedup_lane_tasks);
    println!("  \"metadb_dedup_lane_ops\": {},", meta.dedup_lane_ops);
    println!(
        "  \"metadb_dedup_lane_ops_per_task\": {:.3},",
        avg_u64(meta.dedup_lane_ops, meta.dedup_lane_tasks)
    );
    println!(
        "  \"metadb_dedup_lane_ready_queue_wait_us\": {},",
        meta.dedup_lane_ready_queue_wait_us
    );
    println!(
        "  \"metadb_dedup_lane_ready_queue_wait_avg_us\": {:.3},",
        avg_u64(meta.dedup_lane_ready_queue_wait_us, meta.dedup_lane_tasks)
    );
    println!(
        "  \"metadb_dedup_lane_ready_queue_wait_max_us\": {},",
        meta.dedup_lane_ready_queue_wait_max_us
    );
    println!(
        "  \"metadb_dedup_lane_exec_us\": {},",
        meta.dedup_lane_exec_us
    );
    println!(
        "  \"metadb_dedup_lane_exec_avg_us\": {:.3},",
        avg_u64(meta.dedup_lane_exec_us, meta.dedup_lane_tasks)
    );
    println!(
        "  \"metadb_dedup_lane_exec_max_us\": {},",
        meta.dedup_lane_exec_max_us
    );
    println!(
        "  \"metadb_dedup_forward_put_count\": {},",
        meta.dedup_apply_forward_put_count
    );
    println!(
        "  \"metadb_dedup_forward_put_us\": {},",
        meta.dedup_apply_forward_put_us
    );
    println!(
        "  \"metadb_dedup_forward_put_avg_us\": {:.3},",
        avg_u64(
            meta.dedup_apply_forward_put_us,
            meta.dedup_apply_forward_put_count
        )
    );
    println!(
        "  \"metadb_dedup_forward_put_max_us\": {},",
        meta.dedup_apply_forward_put_max_us
    );
    println!(
        "  \"metadb_dedup_guard_count\": {},",
        meta.dedup_apply_guard_count
    );
    println!(
        "  \"metadb_dedup_guard_us\": {},",
        meta.dedup_apply_guard_us
    );
    println!(
        "  \"metadb_dedup_guard_avg_us\": {:.3},",
        avg_u64(meta.dedup_apply_guard_us, meta.dedup_apply_guard_count)
    );
    println!(
        "  \"metadb_dedup_guard_max_us\": {},",
        meta.dedup_apply_guard_max_us
    );
    println!(
        "  \"metadb_dedup_forward_delete_count\": {},",
        meta.dedup_apply_forward_delete_count
    );
    println!(
        "  \"metadb_dedup_forward_delete_us\": {},",
        meta.dedup_apply_forward_delete_us
    );
    println!("  \"metadb_l2p_get_calls\": {},", meta.l2p_get_calls);
    println!(
        "  \"metadb_l2p_get_lock_wait_us\": {},",
        meta.l2p_get_lock_wait_us
    );
    println!(
        "  \"metadb_l2p_get_lock_wait_avg_us\": {:.3},",
        avg_u64(meta.l2p_get_lock_wait_us, meta.l2p_get_calls)
    );
    println!(
        "  \"metadb_l2p_get_lock_wait_max_us\": {},",
        meta.l2p_get_lock_wait_max_us
    );
    println!(
        "  \"metadb_l2p_get_tree_walk_us\": {},",
        meta.l2p_get_tree_walk_us
    );
    println!(
        "  \"metadb_l2p_get_tree_walk_avg_us\": {:.3},",
        avg_u64(meta.l2p_get_tree_walk_us, meta.l2p_get_calls)
    );
    println!(
        "  \"metadb_l2p_get_tree_walk_max_us\": {},",
        meta.l2p_get_tree_walk_max_us
    );
    println!(
        "  \"metadb_l2p_multi_get_calls\": {},",
        meta.l2p_multi_get_calls
    );
    println!(
        "  \"metadb_l2p_multi_get_lbas\": {},",
        meta.l2p_multi_get_lbas
    );
    println!(
        "  \"metadb_l2p_multi_get_pin_us\": {},",
        meta.l2p_multi_get_pin_us
    );
    println!(
        "  \"metadb_l2p_multi_get_view_us\": {},",
        meta.l2p_multi_get_view_us
    );
    println!(
        "  \"metadb_l2p_multi_get_tree_us\": {},",
        meta.l2p_multi_get_tree_us
    );
    println!(
        "  \"metadb_l2p_multi_get_tree_avg_us\": {:.3},",
        avg_u64(meta.l2p_multi_get_tree_us, meta.l2p_multi_get_calls)
    );
    println!(
        "  \"metadb_l2p_multi_get_tree_max_us\": {},",
        meta.l2p_multi_get_tree_max_us
    );
    println!("  \"metadb_flush_calls\": {},", meta.flush_calls);
    println!("  \"metadb_flush_total_us\": {},", meta.flush_total_us);
    println!(
        "  \"metadb_flush_total_avg_us\": {:.3},",
        avg_u64(meta.flush_total_us, meta.flush_calls)
    );
    println!(
        "  \"metadb_flush_total_max_us\": {},",
        meta.flush_total_max_us
    );
    println!(
        "  \"metadb_flush_gate_wait_us\": {},",
        meta.flush_gate_wait_us
    );
    println!("  \"metadb_flush_io_us\": {},", meta.flush_io_us);
    println!(
        "  \"metadb_flush_manifest_us\": {},",
        meta.flush_manifest_us
    );
    println!("  \"metadb_flush_install_us\": {},", meta.flush_install_us);
    println!("  \"metadb_flush_reclaim_us\": {},", meta.flush_reclaim_us);
    println!(
        "  \"metadb_flush_pages_written\": {},",
        meta.flush_pages_written
    );
    println!("  \"metadb_pending_dispatch\": {},", meta.pending_dispatch);
    println!(
        "  \"metadb_pending_l2p_apply_queue\": {},",
        meta.pending_l2p_apply_queue
    );
    println!(
        "  \"metadb_pending_rc_apply_queue\": {}",
        meta.pending_rc_apply_queue
    );
}

fn avg_u64(num: u64, den: u64) -> f64 {
    if den == 0 {
        0.0
    } else {
        num as f64 / den as f64
    }
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

fn latency_bucket_percentile(buckets: &[u64], pct: f64) -> u64 {
    let total: u64 = buckets.iter().sum();
    if total == 0 {
        return 0;
    }
    let rank = ((total as f64 * pct / 100.0).ceil() as u64).max(1);
    let mut seen = 0u64;
    for (idx, count) in buckets.iter().copied().enumerate() {
        seen = seen.saturating_add(count);
        if seen >= rank {
            if idx == 0 {
                return 0;
            }
            return 1u64.checked_shl(idx as u32).unwrap_or(u64::MAX);
        }
    }
    u64::MAX
}

struct ReadPoolWorkerStats {
    active: usize,
    top_idx: usize,
    top_requests: u64,
    top_batches: u64,
    top_queue_wait_ns: u64,
    top_submit_wait_ns: u64,
}

fn read_pool_worker_stats(metrics: &EngineMetricsSnapshot) -> ReadPoolWorkerStats {
    let mut stats = ReadPoolWorkerStats {
        active: 0,
        top_idx: 0,
        top_requests: 0,
        top_batches: 0,
        top_queue_wait_ns: 0,
        top_submit_wait_ns: 0,
    };

    for (idx, requests) in metrics
        .read_pool_worker_requests
        .iter()
        .copied()
        .enumerate()
    {
        if requests == 0 {
            continue;
        }
        stats.active += 1;
        if requests > stats.top_requests {
            stats.top_idx = idx;
            stats.top_requests = requests;
            stats.top_batches = metrics
                .read_pool_worker_batches
                .get(idx)
                .copied()
                .unwrap_or(0);
            stats.top_queue_wait_ns = metrics
                .read_pool_worker_queue_wait_ns
                .get(idx)
                .copied()
                .unwrap_or(0);
            stats.top_submit_wait_ns = metrics
                .read_pool_worker_submit_wait_ns
                .get(idx)
                .copied()
                .unwrap_or(0);
        }
    }

    stats
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
