use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use clap::{Parser, ValueEnum};
use crossbeam_channel::{Receiver, Sender};

use onyx_storage::config::OnyxConfig;
use onyx_storage::engine::OnyxEngine;
use onyx_storage::metrics::{EngineMetricsSnapshot, EngineStatusSnapshot, MetaMemorySnapshot};
use onyx_storage::types::{CompressionAlgo, BLOCK_SIZE};
use onyx_storage::volume::VolumeWriteTicket;

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

    #[arg(long, default_value_t = 8)]
    jobs: usize,

    /// Maximum durable writes in flight per job. Reads remain synchronous.
    #[arg(long, default_value_t = 64)]
    iodepth: usize,

    /// Threads performing synchronous LV2 append preparation. Zero derives
    /// the production topology from ublk.nr_queues * ublk.queue_workers.
    #[arg(long, default_value_t = 0)]
    submitters: usize,

    #[arg(long, default_value_t = 5)]
    ramp_secs: u64,

    #[arg(long, default_value_t = 70)]
    rwmixread: u8,

    #[arg(long, default_value = "4k", value_parser = parse_size)]
    min_bs: u64,

    #[arg(long, default_value = "32k", value_parser = parse_size)]
    max_bs: u64,

    /// Payload behavior. `fio-default` matches fio without refill_buffers:
    /// random initialization once, then a small per-submit scramble.
    #[arg(long, value_enum, default_value_t = Pattern::FioDefault)]
    pattern: Pattern,

    #[arg(long, value_enum, default_value_t = CompressionChoice::Lz4)]
    compression: CompressionChoice,

    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    reset: bool,

    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    reset_buffer: bool,

    #[arg(long, default_value = "64m", value_parser = parse_size)]
    reset_buffer_bytes: u64,

    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
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

    /// CPU set for append submitters. Use the same foreground set as the ublk
    /// workers when comparing direct-engine results with fio.
    #[arg(long, default_value = "")]
    submitter_cpus: String,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum Pattern {
    FioDefault,
    Zero,
    Repeat,
    Random,
}

impl Pattern {
    fn as_str(self) -> &'static str {
        match self {
            Self::FioDefault => "fio-default",
            Self::Zero => "zero",
            Self::Repeat => "repeat",
            Self::Random => "random",
        }
    }
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
    submit_queue_ns: Vec<u64>,
    append_worker_ns: Vec<u64>,
    post_submit_completion_ns: Vec<u64>,
    completion_delivery_ns: Vec<u64>,
    frontend_total_ns: Vec<u64>,
}

struct PendingWrite {
    ticket: Option<VolumeWriteTicket>,
    started: Instant,
    submitted: Instant,
    submit_queue_ns: u64,
    append_worker_ns: u64,
    bytes: u64,
    measured: bool,
}

impl PendingWrite {
    fn arm_wakeup(&self, tx: &Sender<()>) {
        self.ticket
            .as_ref()
            .expect("pending write ticket already completed")
            .arm_wakeup(tx);
    }

    fn is_durable(&self) -> bool {
        self.ticket
            .as_ref()
            .expect("pending write ticket already completed")
            .is_durable()
    }

    fn wait(mut self) {
        if let Some(ticket) = self.ticket.take() {
            ticket.wait();
        }
    }

    fn finish_durable(mut self) -> CompletedWrite {
        self.ticket
            .take()
            .expect("pending write ticket already completed")
            .finish();
        CompletedWrite {
            started: self.started,
            submitted: self.submitted,
            completed_at: Instant::now(),
            submit_queue_ns: self.submit_queue_ns,
            append_worker_ns: self.append_worker_ns,
            bytes: self.bytes,
            measured: self.measured,
        }
    }
}

impl Drop for PendingWrite {
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket.take() {
            ticket.wait();
        }
    }
}

struct SubmitTask {
    offset: u64,
    len: usize,
    buffer: Vec<u8>,
    started: Instant,
    measured: bool,
}

struct SubmitResult {
    outcome: std::result::Result<PendingWrite, String>,
    buffer: Vec<u8>,
}

struct CompletedWrite {
    started: Instant,
    submitted: Instant,
    completed_at: Instant,
    submit_queue_ns: u64,
    append_worker_ns: u64,
    bytes: u64,
    measured: bool,
}

enum JobEvent {
    Buffer(Vec<u8>),
    Complete(CompletedWrite),
    Failed { buffer: Vec<u8>, error: String },
    LaneFailed(String),
}

struct JobIoState {
    free_buffers: Vec<Vec<u8>>,
    outstanding: usize,
}

impl JobIoState {
    fn new(iodepth: usize, max_len: usize) -> Self {
        Self {
            free_buffers: (0..iodepth).map(|_| vec![0u8; max_len]).collect(),
            outstanding: 0,
        }
    }
}

struct TimedRun {
    stats: BenchStats,
    samples: LatencySamples,
    elapsed: Duration,
    completion_closure: Duration,
    baseline_status: EngineStatusSnapshot,
}

struct DrainReport {
    pending_before: Option<u64>,
    physical_used_before_bytes: u64,
    physical_fill_before_pct: Option<u8>,
    pending_after_timed: Option<u64>,
    physical_used_after_timed_bytes: u64,
    physical_fill_after_timed_pct: Option<u8>,
    pending_after_pending_drain: Option<u64>,
    physical_used_after_pending_drain_bytes: u64,
    physical_fill_after_pending_drain_pct: Option<u8>,
    pending_after_physical_drain: Option<u64>,
    physical_used_after_physical_drain_bytes: u64,
    physical_fill_after_physical_drain_pct: Option<u8>,
    pending_drained: bool,
    physical_drained: bool,
    pending_drain_elapsed: Duration,
    physical_drain_elapsed: Duration,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BenchPhase {
    Setup,
    Ramp,
    Measure,
    Stop,
}

struct BenchControlState {
    phase: BenchPhase,
    ready: usize,
    ramp_done: usize,
    error: Option<String>,
}

struct BenchControl {
    state: Mutex<BenchControlState>,
    changed: Condvar,
}

impl BenchControl {
    fn new() -> Self {
        Self {
            state: Mutex::new(BenchControlState {
                phase: BenchPhase::Setup,
                ready: 0,
                ramp_done: 0,
                error: None,
            }),
            changed: Condvar::new(),
        }
    }

    fn record_error(&self, error: &anyhow::Error) {
        let mut state = self.state.lock().unwrap();
        if state.error.is_none() {
            state.error = Some(format!("{error:#}"));
        }
        self.changed.notify_all();
    }
}

impl LatencySamples {
    fn merge(&mut self, mut other: Self) {
        self.read_ns.append(&mut other.read_ns);
        self.write_ns.append(&mut other.write_ns);
        self.submit_queue_ns.append(&mut other.submit_queue_ns);
        self.append_worker_ns.append(&mut other.append_worker_ns);
        self.post_submit_completion_ns
            .append(&mut other.post_submit_completion_ns);
        self.completion_delivery_ns
            .append(&mut other.completion_delivery_ns);
        self.frontend_total_ns.append(&mut other.frontend_total_ns);
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "onyx_storage=warn".to_string()),
        )
        .init();

    let mut cli = Cli::parse();
    validate_args(&cli)?;

    if cli.reset {
        remove_configured_paths(&cli.config, cli.reset_buffer, cli.reset_buffer_bytes)?;
    }

    let config = OnyxConfig::load(&cli.config)?;
    if cli.submitters == 0 {
        cli.submitters = config.ublk.nr_queues as usize * config.ublk.queue_workers.max(1);
    }
    if cli.submitters == 0 {
        return Err(anyhow!("--submitters must be > 0"));
    }
    if cli.submitters < cli.jobs {
        return Err(anyhow!(
            "--submitters must be >= --jobs so every submit lane has a worker"
        ));
    }
    if cli.iodepth > 1 && cli.job_cpus.trim().is_empty() {
        eprintln!(
            "warn: --iodepth > 1 without --job-cpus may let load-generator completion threads compete with engine threads"
        );
    }
    if cli.submitter_cpus.trim().is_empty() {
        eprintln!(
            "warn: append submitters have no explicit --submitter-cpus; NUMA confinement may place them with background engine work"
        );
    }
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
        eprintln!(
            "prefill: {} over working_set={} bs={}",
            cli.volume,
            human_bytes(cli.working_set),
            human_bytes(cli.max_bs)
        );
        prefill(&volume, &cli)?;
        if !wait_for_pending_drain(&engine, Duration::from_secs(cli.drain_timeout_secs)) {
            return Err(anyhow!("prefill drain timeout"));
        }
    }

    let run = run_mixed(&engine, &cli)?;
    // Freeze the performance window before waiting for either logical or
    // physical drain. Otherwise RC/apply work after load stops contaminates
    // the rates paired with timed IOPS and latency.
    let status_after_timed = engine.status_snapshot()?;
    let metrics = status_after_timed
        .metrics
        .saturating_sub(&run.baseline_status.metrics);
    let meta_delta = run
        .baseline_status
        .metadb_memory
        .as_ref()
        .and_then(|before| {
            status_after_timed
                .metadb_memory
                .as_ref()
                .map(|after| after.saturating_sub(before))
        });

    let drain_started = Instant::now();
    let drain_timeout = Duration::from_secs(cli.drain_timeout_secs);
    let pending_drained = wait_for_pending_drain(&engine, drain_timeout);
    let pending_drain_elapsed = drain_started.elapsed();
    let status_after_pending_drain = engine.status_snapshot()?;
    let physical_drained =
        wait_for_physical_drain(&engine, drain_timeout.saturating_sub(pending_drain_elapsed));
    let physical_drain_elapsed = drain_started.elapsed();
    let status_after_physical_drain = engine.status_snapshot()?;
    let drain = DrainReport {
        pending_before: run.baseline_status.buffer_pending_entries,
        physical_used_before_bytes: status_physical_used_bytes(&run.baseline_status),
        physical_fill_before_pct: run.baseline_status.buffer_physical_fill_pct,
        pending_after_timed: status_after_timed.buffer_pending_entries,
        physical_used_after_timed_bytes: status_physical_used_bytes(&status_after_timed),
        physical_fill_after_timed_pct: status_after_timed.buffer_physical_fill_pct,
        pending_after_pending_drain: status_after_pending_drain.buffer_pending_entries,
        physical_used_after_pending_drain_bytes: status_physical_used_bytes(
            &status_after_pending_drain,
        ),
        physical_fill_after_pending_drain_pct: status_after_pending_drain.buffer_physical_fill_pct,
        pending_after_physical_drain: status_after_physical_drain.buffer_pending_entries,
        physical_used_after_physical_drain_bytes: status_physical_used_bytes(
            &status_after_physical_drain,
        ),
        physical_fill_after_physical_drain_pct: status_after_physical_drain
            .buffer_physical_fill_pct,
        pending_drained,
        physical_drained,
        pending_drain_elapsed,
        physical_drain_elapsed,
    };

    print_report(
        &cli,
        run.stats,
        run.samples,
        run.elapsed,
        run.completion_closure,
        &drain,
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
    if cli.iodepth == 0 {
        return Err(anyhow!("--iodepth must be > 0"));
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

fn run_mixed(engine: &OnyxEngine, cli: &Cli) -> Result<TimedRun> {
    let stop = Arc::new(AtomicBool::new(false));
    let control = Arc::new(BenchControl::new());
    let samples = Arc::new(Mutex::new(LatencySamples::default()));
    let job_cpus = Arc::new(parse_cpu_list(&cli.job_cpus));
    let submitter_cpus = Arc::new(parse_cpu_list(&cli.submitter_cpus));
    if !cli.job_cpus.trim().is_empty() && job_cpus.is_empty() {
        return Err(anyhow!("--job-cpus did not contain any valid CPU"));
    }
    if !cli.submitter_cpus.trim().is_empty() && submitter_cpus.is_empty() {
        return Err(anyhow!("--submitter-cpus did not contain any valid CPU"));
    }
    if !job_cpus.is_empty() {
        eprintln!(
            "pinning {} bench job threads to CPUs {:?}",
            cli.jobs, job_cpus
        );
    }
    if !submitter_cpus.is_empty() {
        eprintln!(
            "pinning {} append submitters and {} durability dispatchers to CPUs {:?}",
            cli.submitters, cli.jobs, submitter_cpus
        );
    }

    // Open every handle before starting the pipeline so an open failure cannot
    // leave detached submit or durability threads behind.
    let mut submitter_volumes = Vec::with_capacity(cli.submitters);
    for _ in 0..cli.submitters {
        submitter_volumes.push(engine.open_volume(&cli.volume)?);
    }
    let mut job_volumes = Vec::with_capacity(cli.jobs);
    for _ in 0..cli.jobs {
        job_volumes.push(engine.open_volume(&cli.volume)?);
    }

    // Mirror ublk's topology: each kernel queue has its own request channel,
    // queue-worker group, and durability dispatcher. A global work-stealing
    // queue changes burst shape and hides per-queue head-of-line behavior.
    let mut submit_txs = Vec::with_capacity(cli.jobs);
    let mut submit_rxs = Vec::with_capacity(cli.jobs);
    for _ in 0..cli.jobs {
        let (tx, rx) = crossbeam_channel::bounded::<SubmitTask>(cli.iodepth);
        submit_txs.push(tx);
        submit_rxs.push(rx);
    }

    let mut submitted_txs = Vec::with_capacity(cli.jobs);
    let mut submitted_rxs = Vec::with_capacity(cli.jobs);
    let mut job_event_txs = Vec::with_capacity(cli.jobs);
    let mut job_event_rxs = Vec::with_capacity(cli.jobs);
    for _ in 0..cli.jobs {
        let (submitted_tx, submitted_rx) = crossbeam_channel::unbounded::<SubmitResult>();
        let (event_tx, event_rx) = crossbeam_channel::unbounded::<JobEvent>();
        submitted_txs.push(submitted_tx);
        submitted_rxs.push(submitted_rx);
        job_event_txs.push(event_tx);
        job_event_rxs.push(event_rx);
    }

    let mut durability_handles = Vec::with_capacity(cli.jobs);
    for lane in 0..cli.jobs {
        let submitted_rx = submitted_rxs[lane].clone();
        let event_tx = job_event_txs[lane].clone();
        let failure_tx = event_tx.clone();
        let durability_cpus = submitter_cpus.clone();
        let spawn_result = thread::Builder::new()
            .name(format!("engine-durable-{lane}"))
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run_durability_dispatcher(lane, submitted_rx, event_tx, &durability_cpus)
                }));
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        let _ = failure_tx.send(JobEvent::LaneFailed(format!(
                            "durability dispatcher {lane} failed: {error:#}"
                        )));
                    }
                    Err(_) => {
                        let _ = failure_tx.send(JobEvent::LaneFailed(format!(
                            "durability dispatcher {lane} panicked"
                        )));
                    }
                }
            });
        match spawn_result {
            Ok(handle) => durability_handles.push(handle),
            Err(error) => {
                drop(submitted_txs);
                drop(submitted_rxs);
                drop(job_event_txs);
                for handle in durability_handles {
                    let _ = handle.join();
                }
                return Err(error.into());
            }
        }
    }
    drop(submitted_rxs);

    let mut submitter_handles = Vec::with_capacity(cli.submitters);
    for (idx, volume) in submitter_volumes.into_iter().enumerate() {
        let lane = idx % cli.jobs;
        let worker_submit_rx = submit_rxs[lane].clone();
        let worker_result_tx = submitted_txs[lane].clone();
        let failure_tx = job_event_txs[lane].clone();
        let submitter_cpus = submitter_cpus.clone();
        let spawn_result = thread::Builder::new()
            .name(format!("engine-submit-{idx}"))
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    if let Err(error) = pin_current_thread_to(&submitter_cpus) {
                        eprintln!(
                            "warn: failed to pin append submitter {idx} to {submitter_cpus:?}: {error}"
                        );
                    }
                    while let Ok(task) = worker_submit_rx.recv() {
                        let SubmitTask {
                            offset,
                            len,
                            buffer,
                            started,
                            measured,
                        } = task;
                        let append_started = Instant::now();
                        let submit_queue_ns =
                            append_started.saturating_duration_since(started).as_nanos() as u64;
                        let append_result =
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                volume.write_aligned_deferred(offset, &buffer[..len])
                            }));
                        let outcome = match append_result {
                            Ok(Ok(ticket)) => {
                                let submitted = Instant::now();
                                Ok(PendingWrite {
                                    ticket: Some(ticket),
                                    started,
                                    submitted,
                                    submit_queue_ns,
                                    append_worker_ns: submitted
                                        .saturating_duration_since(append_started)
                                        .as_nanos()
                                        as u64,
                                    bytes: len as u64,
                                    measured,
                                })
                            }
                            Ok(Err(error)) => Err(error.to_string()),
                            Err(_) => Err(format!(
                                "append submitter {idx} panicked while processing IO"
                            )),
                        };
                        if let Err(error) = worker_result_tx.send(SubmitResult { outcome, buffer }) {
                            if let Ok(pending) = error.0.outcome {
                                pending.wait();
                            }
                            return;
                        }
                    }
                }));
                if result.is_err() {
                    let _ = failure_tx.send(JobEvent::LaneFailed(format!(
                        "append submitter {idx} panicked"
                    )));
                }
            });
        match spawn_result {
            Ok(handle) => submitter_handles.push(handle),
            Err(error) => {
                drop(submit_txs);
                drop(submit_rxs);
                for handle in submitter_handles {
                    let _ = handle.join();
                }
                drop(submitted_txs);
                drop(job_event_txs);
                for handle in durability_handles {
                    let _ = handle.join();
                }
                return Err(error.into());
            }
        }
    }
    drop(submit_rxs);
    drop(submitted_txs);
    drop(job_event_txs);
    let mut handles = Vec::with_capacity(cli.jobs);

    for (job, ((volume, job_submit_tx), event_rx)) in job_volumes
        .into_iter()
        .zip(submit_txs.iter().cloned())
        .zip(job_event_rxs.into_iter())
        .enumerate()
    {
        let worker_stop = stop.clone();
        let worker_control = control.clone();
        let samples = samples.clone();
        let job_cpus = job_cpus.clone();
        let pattern = cli.pattern;
        let rwmixread = cli.rwmixread;
        let iodepth = cli.iodepth;
        let ramp_secs = cli.ramp_secs;
        let min_blocks = cli.min_bs / BLOCK_SIZE as u64;
        let max_blocks = cli.max_bs / BLOCK_SIZE as u64;
        let max_start_lba = (cli.working_set / BLOCK_SIZE as u64).saturating_sub(max_blocks);
        let spawn_result = thread::Builder::new()
            .name(format!("engine-bench-{job}"))
            .spawn(move || -> Result<BenchStats> {
                if let Err(err) = pin_current_thread_to(&job_cpus) {
                    eprintln!("warn: failed to pin bench job {job} to {job_cpus:?}: {err}");
                }
                let mut rng = XorShift64::seed(0x9e37_79b9_7f4a_7c15 ^ job as u64);
                let mut read_buf = vec![0u8; (max_blocks * BLOCK_SIZE as u64) as usize];
                let mut local_samples = LatencySamples::default();
                let mut stats = BenchStats::default();
                let mut issued_writes = 0u64;
                let mut io = JobIoState::new(iodepth, (max_blocks * BLOCK_SIZE as u64) as usize);
                if matches!(pattern, Pattern::FioDefault) {
                    for buffer in &mut io.free_buffers {
                        fill_random_buffer(buffer, &mut rng);
                    }
                }
                {
                    let mut state = worker_control.state.lock().unwrap();
                    state.ready += 1;
                    worker_control.changed.notify_all();
                    while state.phase == BenchPhase::Setup {
                        state = worker_control.changed.wait(state).unwrap();
                    }
                    if state.phase == BenchPhase::Stop {
                        return Ok(stats);
                    }
                }

                let ramp_deadline = Instant::now() + Duration::from_secs(ramp_secs);
                let mut ramp_error = None;
                while Instant::now() < ramp_deadline {
                    if let Err(error) = issue_one(
                        &volume,
                        &job_submit_tx,
                        pattern,
                        rwmixread,
                        min_blocks,
                        max_blocks,
                        max_start_lba,
                        iodepth,
                        job,
                        false,
                        &mut rng,
                        &mut read_buf,
                        &mut io,
                        &mut local_samples,
                        &mut stats,
                        &mut issued_writes,
                        &worker_stop,
                        &event_rx,
                    ) {
                        ramp_error = Some(error);
                        break;
                    }
                }
                if let Err(error) = drain_writes(&mut io, &mut local_samples, &mut stats, &event_rx)
                {
                    if ramp_error.is_none() {
                        ramp_error = Some(error);
                    }
                }
                if ramp_error.is_none() && (io.outstanding != 0 || io.free_buffers.len() != iodepth)
                {
                    ramp_error = Some(anyhow!(
                        "job {job} ramp drain invariant failed: outstanding={}, buffers={}/{}",
                        io.outstanding,
                        io.free_buffers.len(),
                        iodepth
                    ));
                }

                {
                    let mut state = worker_control.state.lock().unwrap();
                    if let Some(error) = ramp_error.as_ref() {
                        if state.error.is_none() {
                            state.error = Some(format!("{error:#}"));
                        }
                    }
                    state.ramp_done += 1;
                    worker_control.changed.notify_all();
                    while state.phase == BenchPhase::Ramp {
                        state = worker_control.changed.wait(state).unwrap();
                    }
                    if state.phase == BenchPhase::Stop {
                        return ramp_error.map_or(Ok(stats), Err);
                    }
                }

                while !worker_stop.load(Ordering::Relaxed) {
                    if let Err(error) = issue_one(
                        &volume,
                        &job_submit_tx,
                        pattern,
                        rwmixread,
                        min_blocks,
                        max_blocks,
                        max_start_lba,
                        iodepth,
                        job,
                        true,
                        &mut rng,
                        &mut read_buf,
                        &mut io,
                        &mut local_samples,
                        &mut stats,
                        &mut issued_writes,
                        &worker_stop,
                        &event_rx,
                    ) {
                        worker_control.record_error(&error);
                        worker_stop.store(true, Ordering::Relaxed);
                        break;
                    }
                }
                if let Err(error) = drain_writes(&mut io, &mut local_samples, &mut stats, &event_rx)
                {
                    worker_control.record_error(&error);
                }

                let mut merged = samples.lock().unwrap();
                merged.merge(local_samples);
                Ok(stats)
            });
        match spawn_result {
            Ok(handle) => handles.push(handle),
            Err(error) => {
                stop.store(true, Ordering::Relaxed);
                let mut state = control.state.lock().unwrap();
                state.phase = BenchPhase::Stop;
                control.changed.notify_all();
                drop(state);
                for handle in handles {
                    let _ = handle.join();
                }
                let _ = shutdown_submit_pipeline(submit_txs, submitter_handles, durability_handles);
                return Err(error.into());
            }
        }
    }

    {
        let mut state = control.state.lock().unwrap();
        while state.ready < cli.jobs && state.error.is_none() {
            if handles.iter().any(|handle| handle.is_finished()) {
                state.error = Some("bench job exited during setup".to_string());
                break;
            }
            let (next, _) = control
                .changed
                .wait_timeout(state, Duration::from_millis(100))
                .unwrap();
            state = next;
        }
        if state.error.is_none() {
            state.phase = BenchPhase::Ramp;
            control.changed.notify_all();
            while state.ramp_done < cli.jobs && state.error.is_none() {
                if handles.iter().any(|handle| handle.is_finished()) {
                    state.error = Some("bench job exited during ramp".to_string());
                    break;
                }
                let (next, _) = control
                    .changed
                    .wait_timeout(state, Duration::from_millis(100))
                    .unwrap();
                state = next;
            }
        }
        if let Some(error) = state.error.clone() {
            state.phase = BenchPhase::Stop;
            control.changed.notify_all();
            drop(state);
            for handle in handles {
                let _ = handle.join();
            }
            let _ = shutdown_submit_pipeline(submit_txs, submitter_handles, durability_handles);
            return Err(anyhow!(error));
        }
    }

    let baseline_status = match engine.status_snapshot() {
        Ok(status) => status,
        Err(error) => {
            let mut state = control.state.lock().unwrap();
            state.phase = BenchPhase::Stop;
            control.changed.notify_all();
            drop(state);
            for handle in handles {
                let _ = handle.join();
            }
            let _ = shutdown_submit_pipeline(submit_txs, submitter_handles, durability_handles);
            return Err(error.into());
        }
    };
    let measured_started = Instant::now();
    {
        let mut state = control.state.lock().unwrap();
        state.phase = BenchPhase::Measure;
        control.changed.notify_all();
    }
    let deadline = measured_started + Duration::from_secs(cli.runtime_secs);
    let mut state = control.state.lock().unwrap();
    while state.error.is_none() && Instant::now() < deadline {
        if handles.iter().any(|handle| handle.is_finished()) {
            state.error = Some("bench job exited during measurement".to_string());
            break;
        }
        let timeout = deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(100));
        let (next, _) = control.changed.wait_timeout(state, timeout).unwrap();
        state = next;
    }
    let worker_error = state.error.clone();
    drop(state);
    let measured_elapsed = measured_started.elapsed();
    stop.store(true, Ordering::Relaxed);

    let mut stats = BenchStats::default();
    let mut join_error = None;
    for handle in handles {
        match handle.join() {
            Ok(Ok(worker_stats)) => stats.add(worker_stats),
            Ok(Err(error)) => {
                if join_error.is_none() {
                    join_error = Some(error);
                }
            }
            Err(_) => {
                if join_error.is_none() {
                    join_error = Some(anyhow!("worker panicked"));
                }
            }
        }
    }
    let completion_closure = measured_started.elapsed().saturating_sub(measured_elapsed);
    if let Some(error) = shutdown_submit_pipeline(submit_txs, submitter_handles, durability_handles)
    {
        if join_error.is_none() {
            join_error = Some(error);
        }
    }
    let worker_error = control.state.lock().unwrap().error.clone().or(worker_error);
    if let Some(error) = worker_error {
        return Err(anyhow!(error));
    }
    if let Some(error) = join_error {
        return Err(error);
    }
    let samples = Arc::try_unwrap(samples)
        .map_err(|_| anyhow!("latency samples still shared"))?
        .into_inner()
        .unwrap();
    validate_write_sample_cardinality(&samples, stats.write_ops)?;
    Ok(TimedRun {
        stats,
        samples,
        elapsed: measured_elapsed,
        completion_closure,
        baseline_status,
    })
}

fn validate_write_sample_cardinality(samples: &LatencySamples, write_ops: u64) -> Result<()> {
    let expected = samples.write_ns.len();
    let counts = [
        ("stats", write_ops as usize),
        ("submit_queue", samples.submit_queue_ns.len()),
        ("append_worker", samples.append_worker_ns.len()),
        (
            "post_submit_completion",
            samples.post_submit_completion_ns.len(),
        ),
        ("completion_delivery", samples.completion_delivery_ns.len()),
        ("frontend_total", samples.frontend_total_ns.len()),
    ];
    if let Some((name, actual)) = counts.into_iter().find(|(_, count)| *count != expected) {
        return Err(anyhow!(
            "write latency sample mismatch: write={expected}, {name}={actual}"
        ));
    }
    Ok(())
}

fn shutdown_submit_pipeline(
    submit_txs: Vec<Sender<SubmitTask>>,
    submitter_handles: Vec<thread::JoinHandle<()>>,
    durability_handles: Vec<thread::JoinHandle<()>>,
) -> Option<anyhow::Error> {
    drop(submit_txs);
    let mut first_error = None;
    for handle in submitter_handles {
        if handle.join().is_err() && first_error.is_none() {
            first_error = Some(anyhow!("append submitter panicked outside its supervisor"));
        }
    }
    for handle in durability_handles {
        if handle.join().is_err() && first_error.is_none() {
            first_error = Some(anyhow!(
                "durability dispatcher panicked outside its supervisor"
            ));
        }
    }
    first_error
}

fn run_durability_dispatcher(
    lane: usize,
    result_rx: Receiver<SubmitResult>,
    event_tx: Sender<JobEvent>,
    cpus: &[usize],
) -> Result<()> {
    if let Err(error) = pin_current_thread_to(cpus) {
        eprintln!("warn: failed to pin durability dispatcher {lane} to {cpus:?}: {error}");
    }

    // This is deliberately the same edge-coalesced scan model as the ublk
    // durability dispatcher. It timestamps completion independently of the
    // load-generator thread that eventually consumes the event.
    let (wake_tx, wake_rx) = crossbeam_channel::bounded::<()>(1);
    let mut pending = Vec::<PendingWrite>::new();
    let mut input_open = true;
    while input_open || !pending.is_empty() {
        if pending.is_empty() {
            match result_rx.recv() {
                Ok(result) => {
                    if !accept_dispatch_result(result, &wake_tx, &event_tx, &mut pending) {
                        return Ok(());
                    }
                }
                Err(_) => input_open = false,
            }
        } else if input_open {
            crossbeam_channel::select! {
                recv(result_rx) -> result => match result {
                    Ok(result) => {
                        if !accept_dispatch_result(result, &wake_tx, &event_tx, &mut pending) {
                            return Ok(());
                        }
                    }
                    Err(_) => input_open = false,
                },
                recv(wake_rx) -> _ => {},
            }
        } else {
            let _ = wake_rx.recv();
        }

        while let Ok(result) = result_rx.try_recv() {
            if !accept_dispatch_result(result, &wake_tx, &event_tx, &mut pending) {
                return Ok(());
            }
        }
        while wake_rx.try_recv().is_ok() {}
        if !dispatch_durable_writes(&mut pending, &event_tx) {
            return Ok(());
        }
    }
    Ok(())
}

fn accept_dispatch_result(
    result: SubmitResult,
    wake_tx: &Sender<()>,
    event_tx: &Sender<JobEvent>,
    pending: &mut Vec<PendingWrite>,
) -> bool {
    match result.outcome {
        Ok(write) => {
            write.arm_wakeup(wake_tx);
            if event_tx.send(JobEvent::Buffer(result.buffer)).is_err() {
                write.wait();
                return false;
            }
            pending.push(write);
        }
        Err(error) => {
            if event_tx
                .send(JobEvent::Failed {
                    buffer: result.buffer,
                    error,
                })
                .is_err()
            {
                return false;
            }
        }
    }
    true
}

fn dispatch_durable_writes(pending: &mut Vec<PendingWrite>, event_tx: &Sender<JobEvent>) -> bool {
    let mut idx = 0;
    while idx < pending.len() {
        if !pending[idx].is_durable() {
            idx += 1;
            continue;
        }
        let completed = pending.swap_remove(idx).finish_durable();
        if event_tx.send(JobEvent::Complete(completed)).is_err() {
            return false;
        }
    }
    true
}

#[allow(clippy::too_many_arguments)]
fn issue_one(
    volume: &onyx_storage::volume::OnyxVolume,
    submit_tx: &Sender<SubmitTask>,
    pattern: Pattern,
    rwmixread: u8,
    min_blocks: u64,
    max_blocks: u64,
    max_start_lba: u64,
    iodepth: usize,
    job: usize,
    measured: bool,
    rng: &mut XorShift64,
    read_buf: &mut [u8],
    io: &mut JobIoState,
    samples: &mut LatencySamples,
    stats: &mut BenchStats,
    issued_writes: &mut u64,
    stop: &AtomicBool,
    event_rx: &Receiver<JobEvent>,
) -> Result<()> {
    service_job_events(io, samples, stats, event_rx, false)?;
    while io.outstanding >= iodepth || io.free_buffers.is_empty() {
        service_job_events(io, samples, stats, event_rx, true)?;
    }
    if measured && stop.load(Ordering::Relaxed) {
        return Ok(());
    }
    let blocks = min_blocks + (rng.next_u64() % (max_blocks - min_blocks + 1));
    let len = (blocks * BLOCK_SIZE as u64) as usize;
    let start_lba = if max_start_lba == 0 {
        0
    } else {
        rng.next_u64() % (max_start_lba + 1)
    };
    let offset = start_lba * BLOCK_SIZE as u64;
    let is_read = (rng.next_u64() % 100) < rwmixread as u64;
    if is_read {
        let started = Instant::now();
        volume.read_into(offset, &mut read_buf[..len])?;
        if measured {
            samples.read_ns.push(started.elapsed().as_nanos() as u64);
            stats.read_ops += 1;
            stats.read_bytes += len as u64;
        }
        service_job_events(io, samples, stats, event_rx, false)?;
        return Ok(());
    }

    // Payload generation is load-generator work, not engine write latency.
    let mut buffer = io.free_buffers.pop().expect("capacity checked above");
    fill_buffer(pattern, &mut buffer[..len], rng, job, *issued_writes);
    *issued_writes = (*issued_writes).saturating_add(1);
    let started = Instant::now();
    let task = SubmitTask {
        offset,
        len,
        buffer,
        started,
        measured,
    };
    if let Err(error) = submit_tx.send(task) {
        io.free_buffers.push(error.0.buffer);
        return Err(anyhow!("append submitter queue closed"));
    }
    io.outstanding += 1;
    Ok(())
}

fn drain_writes(
    io: &mut JobIoState,
    samples: &mut LatencySamples,
    stats: &mut BenchStats,
    event_rx: &Receiver<JobEvent>,
) -> Result<()> {
    let mut first_error = None;
    while io.outstanding > 0 {
        if let Err(error) = service_job_events(io, samples, stats, event_rx, true) {
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn service_job_events(
    io: &mut JobIoState,
    samples: &mut LatencySamples,
    stats: &mut BenchStats,
    event_rx: &Receiver<JobEvent>,
    block: bool,
) -> Result<usize> {
    let mut progress = 0usize;
    let mut first_error = None;
    if block {
        match event_rx.recv() {
            Ok(event) => {
                progress += 1;
                if let Err(error) = accept_job_event(io, samples, stats, event) {
                    first_error = Some(error);
                }
            }
            Err(_) => {
                io.outstanding = 0;
                return Err(anyhow!("job event channel closed with IO outstanding"));
            }
        }
    }
    while let Ok(event) = event_rx.try_recv() {
        progress += 1;
        if let Err(error) = accept_job_event(io, samples, stats, event) {
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
    }

    match first_error {
        Some(error) => Err(error),
        None => Ok(progress),
    }
}

fn accept_job_event(
    io: &mut JobIoState,
    samples: &mut LatencySamples,
    stats: &mut BenchStats,
    event: JobEvent,
) -> Result<()> {
    match event {
        JobEvent::Buffer(buffer) => {
            io.free_buffers.push(buffer);
            Ok(())
        }
        JobEvent::Complete(completed) => {
            let received_at = Instant::now();
            if completed.measured {
                samples.write_ns.push(
                    completed
                        .completed_at
                        .saturating_duration_since(completed.started)
                        .as_nanos() as u64,
                );
                samples.submit_queue_ns.push(completed.submit_queue_ns);
                samples.append_worker_ns.push(completed.append_worker_ns);
                samples.post_submit_completion_ns.push(
                    completed
                        .completed_at
                        .saturating_duration_since(completed.submitted)
                        .as_nanos() as u64,
                );
                samples.completion_delivery_ns.push(
                    received_at
                        .saturating_duration_since(completed.completed_at)
                        .as_nanos() as u64,
                );
                samples.frontend_total_ns.push(
                    received_at
                        .saturating_duration_since(completed.started)
                        .as_nanos() as u64,
                );
                stats.write_ops += 1;
                stats.write_bytes += completed.bytes;
            }
            if io.outstanding == 0 {
                return Err(anyhow!("completion received with no outstanding IO"));
            }
            io.outstanding -= 1;
            Ok(())
        }
        JobEvent::Failed { buffer, error } => {
            io.free_buffers.push(buffer);
            if io.outstanding == 0 {
                return Err(anyhow!(
                    "append failure received with no outstanding IO: {error}"
                ));
            }
            io.outstanding -= 1;
            Err(anyhow!(error))
        }
        JobEvent::LaneFailed(error) => {
            io.outstanding = 0;
            Err(anyhow!(error))
        }
    }
}

fn wait_for_pending_drain(engine: &OnyxEngine, timeout: Duration) -> bool {
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

fn wait_for_physical_drain(engine: &OnyxEngine, timeout: Duration) -> bool {
    let Some(pool) = engine.buffer_pool() else {
        return true;
    };
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if pool.physical_is_empty() {
            return true;
        }
        thread::sleep(Duration::from_millis(50));
    }
    pool.physical_is_empty()
}

fn status_physical_used_bytes(status: &EngineStatusSnapshot) -> u64 {
    status
        .buffer_shards
        .iter()
        .map(|shard| shard.used_bytes)
        .sum()
}

fn print_report(
    cli: &Cli,
    stats: BenchStats,
    mut samples: LatencySamples,
    elapsed: Duration,
    completion_closure: Duration,
    drain: &DrainReport,
    metrics: &EngineMetricsSnapshot,
    meta: Option<&MetaMemorySnapshot>,
) {
    samples.read_ns.sort_unstable();
    samples.write_ns.sort_unstable();
    samples.submit_queue_ns.sort_unstable();
    samples.append_worker_ns.sort_unstable();
    samples.post_submit_completion_ns.sort_unstable();
    samples.completion_delivery_ns.sort_unstable();
    samples.frontend_total_ns.sort_unstable();
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
    let fine_p99 = |buckets: &[u64]| {
        bounded_latency_bucket_percentile(
            buckets,
            &metrics.buffer_lv2_latency_bucket_upper_bounds_ns,
            99.0,
        )
    };
    let fine_count = |buckets: &[u64]| buckets.iter().copied().sum::<u64>();
    let durable_p50 = bounded_latency_bucket_percentile(
        &metrics.buffer_append_wait_durable_fine_latency_buckets,
        &metrics.buffer_lv2_latency_bucket_upper_bounds_ns,
        50.0,
    );
    let durable_p95 = bounded_latency_bucket_percentile(
        &metrics.buffer_append_wait_durable_fine_latency_buckets,
        &metrics.buffer_lv2_latency_bucket_upper_bounds_ns,
        95.0,
    );
    let durable_p99 = fine_p99(&metrics.buffer_append_wait_durable_fine_latency_buckets);
    let durable_p999 = bounded_latency_bucket_percentile(
        &metrics.buffer_append_wait_durable_fine_latency_buckets,
        &metrics.buffer_lv2_latency_bucket_upper_bounds_ns,
        99.9,
    );

    println!("{{");
    println!("  \"kind\": \"onyx-engine-bench\",");
    println!(
        "  \"volume\": {},",
        serde_json::to_string(&cli.volume).unwrap()
    );
    println!("  \"runtime_secs\": {:.3},", secs);
    println!(
        "  \"completion_closure_secs\": {:.6},",
        completion_closure.as_secs_f64()
    );
    println!("  \"requested_runtime_secs\": {},", cli.runtime_secs);
    println!("  \"ramp_secs\": {},", cli.ramp_secs);
    println!("  \"jobs\": {},", cli.jobs);
    println!("  \"iodepth\": {},", cli.iodepth);
    println!("  \"submitters\": {},", cli.submitters);
    println!("  \"submit_lanes\": {},", cli.jobs);
    println!("  \"durability_dispatchers\": {},", cli.jobs);
    println!(
        "  \"submitters_per_lane_min\": {},",
        cli.submitters / cli.jobs
    );
    println!(
        "  \"submitters_per_lane_max\": {},",
        cli.submitters.div_ceil(cli.jobs)
    );
    println!(
        "  \"inflight_target\": {},",
        cli.jobs.saturating_mul(cli.iodepth)
    );
    println!("  \"rwmixread\": {},", cli.rwmixread);
    println!("  \"pattern\": \"{}\",", cli.pattern.as_str());
    println!("  \"working_set_bytes\": {},", cli.working_set);
    println!("  \"read_iops\": {:.3},", read_iops);
    println!("  \"write_iops\": {:.3},", write_iops);
    println!("  \"total_iops\": {:.3},", total_iops);
    println!("  \"throughput_mib_s\": {:.3},", mibps);
    println!("  \"write_latency_scope\": \"request_start_to_durable_dispatcher_confirmation\",");
    println!("  \"read_p50_ns\": {},", percentile(&samples.read_ns, 50.0));
    println!("  \"read_p95_ns\": {},", percentile(&samples.read_ns, 95.0));
    println!("  \"read_p99_ns\": {},", percentile(&samples.read_ns, 99.0));
    println!(
        "  \"read_p999_ns\": {},",
        percentile(&samples.read_ns, 99.9)
    );
    println!(
        "  \"write_p50_ns\": {},",
        percentile(&samples.write_ns, 50.0)
    );
    println!(
        "  \"write_p95_ns\": {},",
        percentile(&samples.write_ns, 95.0)
    );
    println!(
        "  \"write_p99_ns\": {},",
        percentile(&samples.write_ns, 99.0)
    );
    println!(
        "  \"write_p999_ns\": {},",
        percentile(&samples.write_ns, 99.9)
    );
    println!("  \"write_latency_samples\": {},", samples.write_ns.len());
    println!(
        "  \"frontend_total_p50_ns\": {},",
        percentile(&samples.frontend_total_ns, 50.0)
    );
    println!(
        "  \"frontend_total_p95_ns\": {},",
        percentile(&samples.frontend_total_ns, 95.0)
    );
    println!(
        "  \"frontend_total_p99_ns\": {},",
        percentile(&samples.frontend_total_ns, 99.0)
    );
    println!(
        "  \"frontend_total_p999_ns\": {},",
        percentile(&samples.frontend_total_ns, 99.9)
    );
    println!(
        "  \"frontend_total_samples\": {},",
        samples.frontend_total_ns.len()
    );
    println!(
        "  \"submit_queue_samples\": {},",
        samples.submit_queue_ns.len()
    );
    println!(
        "  \"submit_queue_p50_ns\": {},",
        percentile(&samples.submit_queue_ns, 50.0)
    );
    println!(
        "  \"submit_queue_p95_ns\": {},",
        percentile(&samples.submit_queue_ns, 95.0)
    );
    println!(
        "  \"submit_queue_p99_ns\": {},",
        percentile(&samples.submit_queue_ns, 99.0)
    );
    println!(
        "  \"submit_queue_p999_ns\": {},",
        percentile(&samples.submit_queue_ns, 99.9)
    );
    println!(
        "  \"append_worker_p50_ns\": {},",
        percentile(&samples.append_worker_ns, 50.0)
    );
    println!(
        "  \"append_worker_p95_ns\": {},",
        percentile(&samples.append_worker_ns, 95.0)
    );
    println!(
        "  \"append_worker_p99_ns\": {},",
        percentile(&samples.append_worker_ns, 99.0)
    );
    println!(
        "  \"append_worker_p999_ns\": {},",
        percentile(&samples.append_worker_ns, 99.9)
    );
    println!(
        "  \"append_worker_samples\": {},",
        samples.append_worker_ns.len()
    );
    println!(
        "  \"post_submit_completion_p50_ns\": {},",
        percentile(&samples.post_submit_completion_ns, 50.0)
    );
    println!(
        "  \"post_submit_completion_p95_ns\": {},",
        percentile(&samples.post_submit_completion_ns, 95.0)
    );
    println!(
        "  \"post_submit_completion_p99_ns\": {},",
        percentile(&samples.post_submit_completion_ns, 99.0)
    );
    println!(
        "  \"post_submit_completion_p999_ns\": {},",
        percentile(&samples.post_submit_completion_ns, 99.9)
    );
    println!(
        "  \"post_submit_completion_samples\": {},",
        samples.post_submit_completion_ns.len()
    );
    println!(
        "  \"completion_delivery_p50_ns\": {},",
        percentile(&samples.completion_delivery_ns, 50.0)
    );
    println!(
        "  \"completion_delivery_p95_ns\": {},",
        percentile(&samples.completion_delivery_ns, 95.0)
    );
    println!(
        "  \"completion_delivery_p99_ns\": {},",
        percentile(&samples.completion_delivery_ns, 99.0)
    );
    println!(
        "  \"completion_delivery_p999_ns\": {},",
        percentile(&samples.completion_delivery_ns, 99.9)
    );
    println!(
        "  \"completion_delivery_samples\": {},",
        samples.completion_delivery_ns.len()
    );
    println!(
        "  \"pending_before\": {},",
        json_option_u64(drain.pending_before)
    );
    println!(
        "  \"physical_used_before_bytes\": {},",
        drain.physical_used_before_bytes
    );
    println!(
        "  \"physical_fill_before_pct\": {},",
        json_option_u8(drain.physical_fill_before_pct)
    );
    println!(
        "  \"pending_after_timed\": {},",
        json_option_u64(drain.pending_after_timed)
    );
    println!(
        "  \"physical_used_after_timed_bytes\": {},",
        drain.physical_used_after_timed_bytes
    );
    println!(
        "  \"physical_fill_after_timed_pct\": {},",
        json_option_u8(drain.physical_fill_after_timed_pct)
    );
    println!("  \"pending_drained\": {},", drain.pending_drained);
    println!(
        "  \"pending_drain_secs\": {:.3},",
        drain.pending_drain_elapsed.as_secs_f64()
    );
    println!(
        "  \"pending_after_pending_drain\": {},",
        json_option_u64(drain.pending_after_pending_drain)
    );
    println!(
        "  \"physical_used_after_pending_drain_bytes\": {},",
        drain.physical_used_after_pending_drain_bytes
    );
    println!(
        "  \"physical_fill_after_pending_drain_pct\": {},",
        json_option_u8(drain.physical_fill_after_pending_drain_pct)
    );
    println!("  \"physical_drained\": {},", drain.physical_drained);
    println!(
        "  \"physical_drain_secs\": {:.3},",
        drain.physical_drain_elapsed.as_secs_f64()
    );
    println!(
        "  \"pending_after_physical_drain\": {},",
        json_option_u64(drain.pending_after_physical_drain)
    );
    println!(
        "  \"physical_used_after_physical_drain_bytes\": {},",
        drain.physical_used_after_physical_drain_bytes
    );
    println!(
        "  \"physical_fill_after_physical_drain_pct\": {},",
        json_option_u8(drain.physical_fill_after_physical_drain_pct)
    );
    println!("  \"buffer_appends\": {},", metrics.buffer_appends);
    println!(
        "  \"buffer_append_prepare_avg_ns\": {:.3},",
        avg(metrics.buffer_append_prepare_ns, metrics.buffer_appends)
    );
    println!(
        "  \"buffer_append_prepare_samples\": {},",
        fine_count(&metrics.buffer_append_prepare_latency_buckets)
    );
    println!(
        "  \"buffer_append_prepare_p99_ns\": {},",
        latency_bucket_percentile(&metrics.buffer_append_prepare_latency_buckets, 99.0)
    );
    println!(
        "  \"buffer_append_wait_durable_avg_ns\": {:.3},",
        avg(
            metrics.buffer_append_wait_durable_ns,
            metrics.buffer_appends
        )
    );
    println!(
        "  \"buffer_append_wait_durable_samples\": {},",
        fine_count(&metrics.buffer_append_wait_durable_fine_latency_buckets)
    );
    println!("  \"buffer_append_wait_durable_p50_ns\": {durable_p50},");
    println!("  \"buffer_append_wait_durable_p95_ns\": {durable_p95},");
    println!("  \"buffer_append_wait_durable_p99_ns\": {durable_p99},");
    println!("  \"buffer_append_wait_durable_p999_ns\": {durable_p999},");
    print_fine_stage(
        "buffer_lv2_staging_queue",
        &metrics.buffer_lv2_staging_queue_latency_buckets,
        fine_p99(&metrics.buffer_lv2_staging_queue_latency_buckets),
    );
    print_fine_stage(
        "buffer_lv2_prepared_queue",
        &metrics.buffer_lv2_prepared_queue_latency_buckets,
        fine_p99(&metrics.buffer_lv2_prepared_queue_latency_buckets),
    );
    print_fine_stage(
        "buffer_lv2_group_collect",
        &metrics.buffer_lv2_group_collect_latency_buckets,
        fine_p99(&metrics.buffer_lv2_group_collect_latency_buckets),
    );
    print_fine_stage(
        "buffer_lv2_payload_write",
        &metrics.buffer_lv2_payload_write_latency_buckets,
        fine_p99(&metrics.buffer_lv2_payload_write_latency_buckets),
    );
    print_fine_stage(
        "buffer_lv2_checkpoint_write",
        &metrics.buffer_lv2_checkpoint_write_latency_buckets,
        fine_p99(&metrics.buffer_lv2_checkpoint_write_latency_buckets),
    );
    print_fine_stage(
        "buffer_lv2_root_flush",
        &metrics.buffer_lv2_root_flush_latency_buckets,
        fine_p99(&metrics.buffer_lv2_root_flush_latency_buckets),
    );
    print_fine_stage(
        "buffer_lv2_watermark_dispatch",
        &metrics.buffer_lv2_watermark_dispatch_latency_buckets,
        fine_p99(&metrics.buffer_lv2_watermark_dispatch_latency_buckets),
    );
    println!(
        "  \"buffer_sync_entries_per_batch\": {:.3},",
        avg(metrics.buffer_sync_entries, metrics.buffer_sync_batches)
    );
    println!(
        "  \"buffer_backpressure_events\": {},",
        metrics.buffer_backpressure_events
    );
    println!(
        "  \"buffer_backpressure_wait_ns\": {},",
        metrics.buffer_backpressure_wait_ns
    );
    println!(
        "  \"buffer_throttle_count\": {},",
        metrics.buffer_throttle_count
    );
    println!(
        "  \"buffer_throttle_us_total\": {},",
        metrics.buffer_throttle_us_total
    );
    println!(
        "  \"buffer_throttle_us_max\": {},",
        metrics.buffer_throttle_us_max
    );
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

fn json_option_u8(value: Option<u8>) -> String {
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

fn bounded_latency_bucket_percentile(buckets: &[u64], bounds: &[u64], pct: f64) -> u64 {
    let total: u64 = buckets.iter().sum();
    if total == 0 || bounds.is_empty() {
        return 0;
    }
    let rank = ((total as f64 * pct / 100.0).ceil() as u64).max(1);
    let mut seen = 0u64;
    for (idx, count) in buckets.iter().copied().enumerate() {
        seen = seen.saturating_add(count);
        if seen >= rank {
            return bounds[idx.min(bounds.len() - 1)];
        }
    }
    *bounds.last().unwrap()
}

fn print_fine_stage(name: &str, buckets: &[u64], p99_ns: u64) {
    println!("  \"{name}_samples\": {},", buckets.iter().sum::<u64>());
    println!("  \"{name}_p99_ns\": {p99_ns},");
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
        Pattern::FioDefault => {
            // fio initializes each IO buffer once, then scramble_buffers=true
            // changes a small portion before every submit. This unique tuple
            // defeats block dedup without paying for a full 4 KiB refill.
            let op = op_idx.to_le_bytes();
            let thread = (tid as u64).to_le_bytes();
            let split = op.len().min(buf.len());
            buf[..split].copy_from_slice(&op[..split]);
            let tail = thread.len().min(buf.len().saturating_sub(split));
            buf[split..split + tail].copy_from_slice(&thread[..tail]);
        }
        Pattern::Zero => buf.fill(0),
        Pattern::Repeat => {
            let seed = ((tid as u64 * 131) ^ op_idx).to_le_bytes();
            for chunk in buf.chunks_mut(seed.len()) {
                let len = chunk.len();
                chunk.copy_from_slice(&seed[..len]);
            }
        }
        Pattern::Random => fill_random_buffer(buf, rng),
    }
}

fn fill_random_buffer(buf: &mut [u8], rng: &mut XorShift64) {
    for chunk in buf.chunks_mut(8) {
        let bytes = rng.next_u64().to_le_bytes();
        let len = chunk.len();
        chunk.copy_from_slice(&bytes[..len]);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn destructive_setup_is_opt_in() {
        let cli = Cli::try_parse_from(["onyx-engine-bench"]).unwrap();
        assert!(!cli.reset);
        assert!(!cli.reset_buffer);
        assert!(!cli.prefill);
        assert_eq!(cli.jobs, 8);
        assert_eq!(cli.iodepth, 64);
        assert_eq!(cli.ramp_secs, 5);
        assert_eq!(cli.pattern.as_str(), "fio-default");
    }

    #[test]
    fn fio_default_scrambles_only_a_small_unique_prefix() {
        let mut rng = XorShift64::seed(1);
        let mut buffer = vec![0xA5; BLOCK_SIZE as usize];
        fill_buffer(Pattern::FioDefault, &mut buffer, &mut rng, 7, 11);
        let first = buffer.clone();
        assert!(buffer[16..].iter().all(|byte| *byte == 0xA5));

        fill_buffer(Pattern::FioDefault, &mut buffer, &mut rng, 7, 12);
        assert_ne!(buffer, first);
        assert!(buffer[16..].iter().all(|byte| *byte == 0xA5));
    }

    #[test]
    fn bounded_histogram_percentile_uses_bucket_upper_bound() {
        let buckets = [1, 2, 7];
        let bounds = [9, 19, 29];
        assert_eq!(
            bounded_latency_bucket_percentile(&buckets, &bounds, 10.0),
            9
        );
        assert_eq!(
            bounded_latency_bucket_percentile(&buckets, &bounds, 50.0),
            29
        );
        assert_eq!(
            bounded_latency_bucket_percentile(&buckets, &bounds, 99.0),
            29
        );
        assert_eq!(bounded_latency_bucket_percentile(&[], &[], 99.0), 0);
    }

    #[test]
    fn write_latency_sample_cardinality_must_match() {
        let mut samples = LatencySamples::default();
        samples.write_ns.push(1);
        samples.submit_queue_ns.push(1);
        samples.append_worker_ns.push(1);
        samples.post_submit_completion_ns.push(1);
        samples.completion_delivery_ns.push(1);
        samples.frontend_total_ns.push(1);
        validate_write_sample_cardinality(&samples, 1).unwrap();

        samples.completion_delivery_ns.clear();
        assert!(validate_write_sample_cardinality(&samples, 1).is_err());
    }
}
