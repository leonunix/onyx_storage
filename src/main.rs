use std::path::PathBuf;
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::{fs, path::Path};

use clap::{Parser, Subcommand};

use onyx_storage::config::OnyxConfig;
use onyx_storage::engine::OnyxEngine;
use onyx_storage::error::OnyxError;
#[cfg(target_os = "linux")]
use onyx_storage::frontend::ublk::OnyxUblkTarget;
use onyx_storage::service::{self, ServiceController};
use onyx_storage::types::CompressionAlgo;

#[derive(Parser)]
#[command(
    name = "onyx-storage",
    version,
    about = "Userspace all-flash block storage engine"
)]
struct Cli {
    /// Path to configuration file
    #[arg(short, long, default_value = "/etc/onyx-storage/config.toml")]
    config: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the storage engine, serving one or more volumes via ublk
    Start {
        /// Volume name(s) to serve. Can be specified multiple times.
        /// If omitted, all existing volumes are started.
        #[arg(short, long)]
        volume: Vec<String>,
    },
    /// Stop a running storage engine (via Unix socket IPC)
    Stop,
    /// Reload configuration (equivalent to SIGHUP)
    Reload,
    /// Create a new volume
    CreateVolume {
        /// Volume name
        #[arg(short, long)]
        name: String,
        /// Volume size in bytes
        #[arg(short, long)]
        size: u64,
        /// Compression algorithm: none, lz4, zstd
        #[arg(long, default_value = "lz4")]
        compression: String,
    },
    /// Delete a volume
    DeleteVolume {
        /// Volume name
        #[arg(short, long)]
        name: String,
    },
    /// List volumes
    ListVolumes,
    /// Create a point-in-time snapshot of a volume
    SnapshotCreate {
        /// Source volume name
        #[arg(short, long)]
        volume: String,
        /// Snapshot name
        #[arg(short, long)]
        name: String,
    },
    /// Delete a snapshot
    SnapshotDelete {
        /// Source volume name
        #[arg(short, long)]
        volume: String,
        /// Snapshot name
        #[arg(short, long)]
        name: String,
    },
    /// List snapshots (optionally for one volume)
    SnapshotList {
        /// Restrict to one volume
        #[arg(short, long)]
        volume: Option<String>,
    },
    /// Clone a snapshot into a new writable volume
    SnapshotClone {
        /// Source volume name
        #[arg(short, long)]
        volume: String,
        /// Snapshot name
        #[arg(short, long)]
        name: String,
        /// New volume name to create from the snapshot
        #[arg(long)]
        to: String,
    },
    /// Restore a volume in place to a snapshot (destructive rollback; volume must be stopped)
    SnapshotRestore {
        /// Volume to roll back
        #[arg(short, long)]
        volume: String,
        /// Snapshot name to roll back to
        #[arg(short, long)]
        name: String,
    },
    /// Show engine status
    Status,
    /// Show capacity / dedup / compression usage for a volume
    VolumeUsage {
        /// Volume name
        #[arg(short, long)]
        volume: String,
    },
    /// Kill stale Linux ublk devices left behind after abnormal exit
    CleanupUblk,
    /// Initialize the chunklet RAID pool over `[chunklet].devices` and create
    /// the LV3/LV2/meta LDs. WIPES any existing pool state on those disks —
    /// verify the device list first, then re-run with `--force`.
    ChunkletInit {
        /// Acknowledge that `[chunklet].devices` will be wiped.
        #[arg(long)]
        force: bool,
    },
    /// chunklet RAID pool online operations (status / scrub / rebuild / job).
    /// Requires a running engine — the pool is flock-held by `start`, so these
    /// are routed to it over the IPC socket, never a second `Pool::open`.
    Chunklet {
        #[command(subcommand)]
        op: ChunkletOp,
    },
    /// Audit metadb for orphan pages (allocated but unreachable and not on
    /// the free list) on the chunklet-backed meta LD. Offline-only: the pool
    /// lock is exclusive, so the engine must be stopped first (`[meta]
    /// backend = "file"` deployments already have this via the standalone
    /// `metadb-verify <path>` binary).
    MetadbVerify {
        /// Escalate orphaned pages from a warning to a hard failure (nonzero
        /// exit even if nothing else is wrong).
        #[arg(long)]
        strict: bool,
        /// Print the report as JSON instead of human-readable text.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum ChunkletOp {
    /// Show pool topology + health as JSON (PDs, LDs, capacity, degraded state)
    Status,
    /// Start a background scrub (parity verify + quarantine) of an LD; prints a
    /// job id to poll with `chunklet job <id>`
    Scrub {
        /// Target LD id (as printed by `chunklet-init` / `chunklet status`)
        ld_id: String,
    },
    /// Start a background rebuild of an LD's failed members onto spares; prints
    /// a job id to poll with `chunklet job <id>`
    Rebuild {
        /// Target LD id
        ld_id: String,
    },
    /// Poll a background job by id, or list all jobs if no id is given
    Job {
        /// Job id from a prior scrub/rebuild; omit to list all jobs
        id: Option<u64>,
    },
    /// Grow a role LD by N chunklet rows online (`extend_ld` + live capacity
    /// propagation — the allocator/metadb pick it up without a restart)
    Extend {
        /// Which LD to grow: "lv3" (data) or "meta" (metadb)
        role: String,
        /// Additional chunklet rows to add
        rows: u16,
    },
    /// Reintegrate a physically-returned disk: wipe it and re-admit it under a
    /// fresh id reusing the failed slot (safety-gated). Prints a job id to poll
    Reintegrate {
        /// Raw device path of the returned disk (e.g. /dev/nvme3n1)
        device: String,
    },
    /// Rebalance data across PDs until per-PD used-skew converges; online +
    /// bounded. Prints a job id to poll
    Rebalance {
        /// Stop once worst-case per-PD used-skew is within this percent (default 20)
        #[arg(long, default_value_t = 20.0)]
        target_skew_pct: f64,
        /// Hard cap on committed moves this run (default 256)
        #[arg(long, default_value_t = 256)]
        max_moves: usize,
    },
    /// Reclaim `Used`-but-unreferenced chunklets pool-wide (returned-disk /
    /// drop-ld leftovers); skips if the pool is incomplete. Prints a job id
    Fsck,
    /// Drain a PD: migrate every member off it onto spares so it can be pulled.
    /// Prints a job id to poll
    Drain {
        /// Target PD id
        pd_id: String,
    },
    /// Retire a gone-for-good failed PD: drop its tombstone + shrink the pool
    /// (re-denses seqs). The disk must be Failed, absent, and unreferenced
    RetireFailed {
        /// Target PD id
        pd_id: String,
    },
    /// Clear a PD's Failed flag (e.g. after a transient fault cleared)
    ClearFailed {
        /// Target PD id
        pd_id: String,
    },
}

fn parse_compression(s: &str) -> CompressionAlgo {
    match s.to_lowercase().as_str() {
        "none" => CompressionAlgo::None,
        "lz4" => CompressionAlgo::Lz4,
        "zstd" => CompressionAlgo::Zstd { level: 3 },
        _ => CompressionAlgo::Lz4,
    }
}

fn is_stale_socket_error(err: &OnyxError) -> bool {
    matches!(err, OnyxError::Config(msg) if msg.contains("cannot connect to"))
}

/// Run a control-plane action against a running engine via IPC when the socket
/// is live, else open a metadata-only engine directly. Mirrors the IPC-first +
/// stale-socket fallback used by create-volume/delete-volume.
fn with_engine_or_ipc<T>(
    config: &OnyxConfig,
    via_ipc: impl FnOnce(&std::path::Path) -> Result<T, OnyxError>,
    via_engine: impl FnOnce(&OnyxEngine) -> Result<T, OnyxError>,
) -> anyhow::Result<T> {
    let sock = &config.service.socket_path;
    if sock.exists() {
        match via_ipc(sock) {
            Ok(v) => Ok(v),
            Err(err) if is_stale_socket_error(&err) => {
                eprintln!(
                    "stale socket {:?} detected, falling back to metadata-only path",
                    sock
                );
                let engine = OnyxEngine::open_meta_only(config)?;
                Ok(via_engine(&engine)?)
            }
            Err(err) => Err(err.into()),
        }
    } else {
        let engine = OnyxEngine::open_meta_only(config)?;
        Ok(via_engine(&engine)?)
    }
}

/// Mirrors `metadb-verify`'s own `print_human` (metadb/src/bin/metadb-verify.rs)
/// so the two tools read the same way regardless of which backend a
/// deployment uses.
fn print_verify_report_human(report: &onyx_metadb::VerifyReport) {
    if let Some(slot) = report.manifest_slot {
        println!("manifest_slot: {slot}");
    }
    if let Some(sequence) = report.manifest_sequence {
        println!("manifest_sequence: {sequence}");
    }
    if let Some(lsn) = report.checkpoint_lsn {
        println!("checkpoint_lsn: {lsn}");
    }
    println!("high_water: {}", report.high_water);
    println!("scanned_pages: {}", report.scanned_pages);
    println!("live_pages: {}", report.live_pages);
    println!("free_pages: {}", report.free_pages);
    println!("orphans: {}", report.orphan_pages.len());
    if !report.orphan_page_types.is_empty() {
        println!("orphan_page_types:");
        for (pid, page_type) in &report.orphan_page_types {
            println!("  - {pid}: {page_type:?}");
        }
    }
    if !report.warnings.is_empty() {
        println!("warnings:");
        for warning in &report.warnings {
            println!("  - {warning}");
        }
    }
    if !report.issues.is_empty() {
        println!("issues:");
        for issue in &report.issues {
            println!("  - {issue}");
        }
    }
    println!(
        "status: {}",
        if report.is_clean() { "clean" } else { "failed" }
    );
}

fn print_verify_report_json(report: &onyx_metadb::VerifyReport) {
    let orphan_ids: Vec<String> = report.orphan_pages.iter().map(u64::to_string).collect();
    let orphan_types: Vec<String> = report
        .orphan_page_types
        .iter()
        .map(|(pid, page_type)| format!("\"{pid}:{page_type:?}\""))
        .collect();
    let warnings: Vec<String> = report.warnings.iter().map(|w| format!("{:?}", w)).collect();
    let issues: Vec<String> = report.issues.iter().map(|i| format!("{:?}", i)).collect();
    println!("{{");
    println!(
        "  \"manifest_slot\": {},",
        report
            .manifest_slot
            .map(|v| v.to_string())
            .unwrap_or_else(|| "null".into())
    );
    println!(
        "  \"manifest_sequence\": {},",
        report
            .manifest_sequence
            .map(|v| v.to_string())
            .unwrap_or_else(|| "null".into())
    );
    println!(
        "  \"checkpoint_lsn\": {},",
        report
            .checkpoint_lsn
            .map(|v| v.to_string())
            .unwrap_or_else(|| "null".into())
    );
    println!("  \"high_water\": {},", report.high_water);
    println!("  \"scanned_pages\": {},", report.scanned_pages);
    println!("  \"live_pages\": {},", report.live_pages);
    println!("  \"free_pages\": {},", report.free_pages);
    println!("  \"orphan_pages\": [{}],", orphan_ids.join(", "));
    println!("  \"orphan_page_types\": [{}],", orphan_types.join(", "));
    println!("  \"warnings\": [{}],", warnings.join(", "));
    println!("  \"issues\": [{}]", issues.join(", "));
    println!("}}");
}

#[cfg(target_os = "linux")]
fn cleanup_stale_ublk_devices() -> anyhow::Result<usize> {
    fn parse_dev_id(path: &Path) -> Option<u32> {
        let name = path.file_name()?.to_str()?;
        name.strip_prefix("ublkc")?.parse().ok()
    }

    let mut cleaned = 0usize;
    for entry in fs::read_dir("/dev")? {
        let entry = entry?;
        let path = entry.path();
        let Some(dev_id) = parse_dev_id(&path) else {
            continue;
        };
        match OnyxUblkTarget::kill_device(dev_id) {
            Ok(()) => {
                cleaned += 1;
                eprintln!("cleaned stale ublk device id {}", dev_id);
            }
            Err(err) => {
                eprintln!("failed to clean ublk device id {}: {}", dev_id, err);
            }
        }
    }
    Ok(cleaned)
}

/// Raise `RLIMIT_NOFILE` to the hard limit and, under chunklet, warn if it is
/// still low. chunklet's io_uring backend pins one ring (≈1 fd) per onyx IO
/// thread (read-pool + per-shard flush writers + metadb writers) on top of
/// ublk's per-queue fds and the PD handles; the common 1024 soft default is
/// easily overrun. As of chunklet f0395a8 a ring that hits EMFILE degrades to
/// the syscall path instead of failing the write, so a low limit only costs
/// throughput — hence we warn (and point at `LimitNOFILE`) rather than refuse.
fn ensure_fd_limit(config: &OnyxConfig) {
    use nix::sys::resource::{getrlimit, setrlimit, Resource};

    let effective = match getrlimit(Resource::RLIMIT_NOFILE) {
        Ok((soft, hard)) => {
            if soft < hard {
                match setrlimit(Resource::RLIMIT_NOFILE, hard, hard) {
                    Ok(()) => {
                        tracing::info!(
                            old_soft = soft,
                            new_soft = hard,
                            hard,
                            "raised RLIMIT_NOFILE to hard limit"
                        );
                        hard
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, soft, hard, "failed to raise RLIMIT_NOFILE; using current soft limit");
                        soft
                    }
                }
            } else {
                soft
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "getrlimit(RLIMIT_NOFILE) failed; cannot tune fd limit");
            return;
        }
    };

    if config.chunklet.enabled {
        // Rough working-set: one ring per read-pool worker + per buffer shard,
        // a few ublk fds per queue, plus headroom for PDs/metadb/sockets.
        let recommended = 8
            * (config.storage.read_pool_workers as u64 + config.buffer.shards as u64)
            + 4 * config.ublk.nr_queues as u64
            + 4096;
        if effective < recommended.max(65536) {
            tracing::warn!(
                effective,
                recommended = recommended.max(65536),
                "RLIMIT_NOFILE is low for chunklet + ublk; io_uring will degrade to the \
                 syscall path under load (correct but slower). Raise it via systemd \
                 LimitNOFILE=1048576 (or `ulimit -n`) to keep the io_uring fast path"
            );
        }
    }
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("onyx_storage=info".parse()?),
        )
        .init();

    let cli = Cli::parse();
    let config = OnyxConfig::load(&cli.config)?;
    ensure_fd_limit(&config);
    onyx_storage::numa::setup(&config)?;

    match cli.command {
        Command::Start { volume } => {
            tracing::info!("starting onyx storage engine");

            let controller = ServiceController::new(config.clone(), cli.config.clone())?;
            let controller = Arc::new(controller);

            // Install SIGHUP handler for config reload
            onyx_storage::signal::install_signal_handlers();

            // Register Ctrl+C / SIGTERM handler for graceful shutdown
            let ctrl_for_signal = controller.clone();
            ctrlc::set_handler(move || {
                ctrl_for_signal.trigger_shutdown();
            })
            .expect("failed to set signal handler");

            if volume.is_empty() {
                tracing::info!("starting all volumes");
            } else {
                tracing::info!(volumes = ?volume, "starting specified volumes");
            }

            controller.run(&volume)?;
            tracing::info!("engine stopped");
        }
        Command::Stop => {
            service::send_stop_command(&config.service.socket_path)?;
            println!("engine stopped");
        }
        Command::Reload => {
            service::send_reload_command(&config.service.socket_path)?;
            println!("reload requested");
        }
        Command::CreateVolume {
            name,
            size,
            compression,
        } => {
            let sock = &config.service.socket_path;
            let use_meta_only = if sock.exists() {
                match service::send_create_volume(sock, &name, size, &compression) {
                    Ok(()) => false,
                    Err(err) if is_stale_socket_error(&err) => {
                        eprintln!(
                            "stale socket {:?} detected, falling back to metadata-only create",
                            sock
                        );
                        true
                    }
                    Err(err) => return Err(err.into()),
                }
            } else {
                true
            };

            if use_meta_only {
                let engine = OnyxEngine::open_meta_only(&config)?;
                let algo = parse_compression(&compression);
                engine.create_volume(&name, size, algo)?;
            }
            println!(
                "Volume '{}' created ({} bytes, compression={})",
                name, size, compression
            );
        }
        Command::DeleteVolume { name } => {
            let sock = &config.service.socket_path;
            let freed = if sock.exists() {
                match service::send_delete_volume(sock, &name) {
                    Ok(freed) => freed,
                    Err(err) if is_stale_socket_error(&err) => {
                        eprintln!(
                            "stale socket {:?} detected, falling back to metadata-only delete",
                            sock
                        );
                        let engine = OnyxEngine::open_meta_only(&config)?;
                        engine.delete_volume(&name)? as u64
                    }
                    Err(err) => return Err(err.into()),
                }
            } else {
                let engine = OnyxEngine::open_meta_only(&config)?;
                engine.delete_volume(&name)? as u64
            };
            println!(
                "Volume '{}' deleted ({} physical blocks freed)",
                name, freed
            );
        }
        Command::ListVolumes => {
            let sock = &config.service.socket_path;
            if sock.exists() {
                match service::send_list_volumes(sock) {
                    Ok(lines) => {
                        if lines.is_empty() {
                            println!("No volumes");
                        } else {
                            for line in &lines {
                                println!("  {}", line);
                            }
                        }
                    }
                    Err(err) if is_stale_socket_error(&err) => {
                        eprintln!(
                            "stale socket {:?} detected, falling back to metadata-only list",
                            sock
                        );
                        let engine = OnyxEngine::open_meta_only(&config)?;
                        let volumes = engine.list_volumes()?;
                        if volumes.is_empty() {
                            println!("No volumes");
                        } else {
                            for vol in &volumes {
                                println!(
                                    "  {} : {} bytes, zones={}, compression={:?}",
                                    vol.id, vol.size_bytes, vol.zone_count, vol.compression
                                );
                            }
                        }
                    }
                    Err(err) => return Err(err.into()),
                }
            } else {
                let engine = OnyxEngine::open_meta_only(&config)?;
                let volumes = engine.list_volumes()?;
                if volumes.is_empty() {
                    println!("No volumes");
                } else {
                    for vol in &volumes {
                        println!(
                            "  {} : {} bytes, zones={}, compression={:?}",
                            vol.id, vol.size_bytes, vol.zone_count, vol.compression
                        );
                    }
                }
            }
        }
        Command::SnapshotCreate { volume, name } => {
            with_engine_or_ipc(
                &config,
                |sock| service::send_snapshot_create(sock, &volume, &name),
                |engine| {
                    engine
                        .create_snapshot(&volume, &name)
                        .map(|info| info.snapshot_id)
                },
            )
            .map(|id| println!("Snapshot '{}@{}' created (id {})", volume, name, id))?;
        }
        Command::SnapshotDelete { volume, name } => {
            with_engine_or_ipc(
                &config,
                |sock| service::send_snapshot_delete(sock, &volume, &name),
                |engine| engine.delete_snapshot(&volume, &name).map(|n| n as u64),
            )
            .map(|freed| {
                println!(
                    "Snapshot '{}@{}' deleted ({} physical blocks freed)",
                    volume, name, freed
                )
            })?;
        }
        Command::SnapshotList { volume } => {
            let lines = with_engine_or_ipc(
                &config,
                |sock| service::send_snapshot_list(sock, volume.as_deref()),
                |engine| {
                    Ok(engine
                        .list_snapshots(volume.as_deref())?
                        .into_iter()
                        .map(|s| {
                            format!(
                                "{}@{} id={} created_lsn={} size={}",
                                s.volume, s.name, s.snapshot_id, s.created_lsn, s.size_bytes
                            )
                        })
                        .collect::<Vec<_>>())
                },
            )?;
            if lines.is_empty() {
                println!("No snapshots");
            } else {
                for line in &lines {
                    println!("  {}", line);
                }
            }
        }
        Command::SnapshotClone { volume, name, to } => {
            with_engine_or_ipc(
                &config,
                |sock| service::send_snapshot_clone(sock, &volume, &name, &to),
                |engine| {
                    engine
                        .clone_snapshot(&volume, &name, &to)
                        .map(|cfg| cfg.id.0)
                },
            )
            .map(|new_vol| {
                println!(
                    "Snapshot '{}@{}' cloned into new volume '{}'",
                    volume, name, new_vol
                )
            })?;
        }
        Command::SnapshotRestore { volume, name } => {
            with_engine_or_ipc(
                &config,
                |sock| service::send_snapshot_restore(sock, &volume, &name),
                |engine| engine.restore_snapshot(&volume, &name),
            )
            .map(|_| println!("Volume '{}' restored to snapshot '{}'", volume, name))?;
        }
        Command::VolumeUsage { volume } => {
            let u = with_engine_or_ipc(
                &config,
                |sock| {
                    let json = service::send_volume_usage(sock, &volume)?;
                    serde_json::from_str::<onyx_storage::meta::store::VolumeUsage>(&json)
                        .map_err(|e| OnyxError::Config(format!("invalid volume-usage reply: {e}")))
                },
                |engine| engine.volume_usage(&volume),
            )?;
            println!("Volume '{}' usage:", volume);
            println!("  logical size : {} bytes", u.logical_size_bytes);
            println!(
                "  mapped/used  : {} bytes ({} LBAs)",
                u.mapped_bytes, u.mapped_lbas
            );
            println!(
                "  physical     : {} bytes ({} unique units)",
                u.physical_bytes, u.unique_blocks
            );
            println!("  dedup        : {:.2}x", u.dedup_ratio);
            println!("  compression  : {:.2}x", u.compress_ratio);
            println!("  reduction    : {:.2}x", u.data_reduction_ratio);
            println!(
                "  computed_at  : {} (epoch s; cold cache, TTL 60s)",
                u.computed_at
            );
        }
        Command::Status => {
            let sock = &config.service.socket_path;
            println!("onyx-storage v{}", env!("CARGO_PKG_VERSION"));
            println!("config: {:?}", cli.config);

            if sock.exists() {
                // Prefer IPC query to running service
                match service::send_status_command(sock) {
                    Ok(lines) => {
                        for line in &lines {
                            println!("{}", line);
                        }
                    }
                    Err(err) if is_stale_socket_error(&err) => {
                        eprintln!(
                            "stale socket {:?} detected, falling back to direct status",
                            sock
                        );
                        match OnyxEngine::open(&config) {
                            Ok(engine) => {
                                print!("{}", engine.status_report()?);
                                engine.shutdown()?;
                            }
                            Err(full_err) => {
                                eprintln!("full status unavailable: {}", full_err);
                                let engine = OnyxEngine::open_meta_only(&config)?;
                                print!("{}", engine.status_report()?);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("failed to query running service: {}", e);
                    }
                }
            } else {
                // No running service, try direct engine access
                match OnyxEngine::open(&config) {
                    Ok(engine) => {
                        print!("{}", engine.status_report()?);
                        engine.shutdown()?;
                    }
                    Err(full_err) => {
                        eprintln!("full status unavailable: {}", full_err);
                        let engine = OnyxEngine::open_meta_only(&config)?;
                        print!("{}", engine.status_report()?);
                    }
                }
            }
        }
        Command::CleanupUblk => {
            #[cfg(target_os = "linux")]
            {
                let cleaned = cleanup_stale_ublk_devices()?;
                println!("cleaned {} stale ublk device(s)", cleaned);
            }
            #[cfg(not(target_os = "linux"))]
            {
                println!("cleanup-ublk is only supported on Linux");
            }
        }
        Command::ChunkletInit { force } => {
            let ck = &config.chunklet;
            if ck.devices.is_empty() {
                anyhow::bail!("[chunklet].devices is empty — configure the PD disk list first");
            }
            if !force {
                eprintln!("chunklet-init will WIPE pool state on these devices:");
                for d in &ck.devices {
                    eprintln!("  {}", d.display());
                }
                anyhow::bail!(
                    "refusing without --force (verify the disks, then re-run with --force)"
                );
            }
            let (_pool, lv3, lv2, meta) = onyx_storage::chunklet_pool::init_pool(ck)?;
            println!("chunklet pool initialized over {} PDs", ck.devices.len());
            println!("add these LD ids under [chunklet] in your config:");
            println!("  lv3_ld_id = \"{lv3}\"");
            println!("  lv2_ld_id = \"{lv2}\"");
            println!("  meta_ld_id = \"{meta}\"");
            println!();
            println!("to put metadb on the meta LD (removes the host-FS metadata SPOF), set:");
            println!("  [meta]");
            println!("  backend = \"chunklet\"");
        }
        Command::MetadbVerify { strict, json } => {
            if config.meta.backend != onyx_storage::config::MetaBackendKind::Chunklet {
                let path = config
                    .meta
                    .path()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "<unset>".to_string());
                println!(
                    "[meta] backend is not \"chunklet\" — use the standalone \
                     `metadb-verify {path}` binary instead"
                );
                return Ok(());
            }
            let (_pool, meta_backend) = onyx_storage::chunklet_pool::open_role_backend(
                &config.chunklet,
                onyx_storage::chunklet_pool::LdRoleSel::Meta,
            )
            .map_err(|err| {
                anyhow::anyhow!(
                    "{err} — if the engine is running against this pool, stop it first \
                     (metadb-verify needs an exclusive open, same as chunklet-init)"
                )
            })?;
            let report = onyx_storage::meta::backend::metadb::verify_meta_ld(
                &config.meta,
                meta_backend,
                onyx_metadb::VerifyOptions {
                    strict,
                    ..Default::default()
                },
            )?;
            if json {
                print_verify_report_json(&report);
            } else {
                print_verify_report_human(&report);
            }
            if !report.is_clean() {
                anyhow::bail!("metadb-verify found issues");
            }
        }
        Command::Chunklet { op } => {
            let sock = &config.service.socket_path;
            if !sock.exists() {
                anyhow::bail!(
                    "chunklet online ops require a running engine (socket {:?} not found) — \
                     start the engine first; offline pool setup is `chunklet-init`",
                    sock
                );
            }
            let cmd = match &op {
                ChunkletOp::Status => "chunklet-status-json".to_string(),
                ChunkletOp::Scrub { ld_id } => format!("chunklet-scrub {ld_id}"),
                ChunkletOp::Rebuild { ld_id } => format!("chunklet-rebuild {ld_id}"),
                ChunkletOp::Job { id } => match id {
                    Some(i) => format!("chunklet-job {i}"),
                    None => "chunklet-job".to_string(),
                },
                ChunkletOp::Extend { role, rows } => format!("chunklet-extend {role} {rows}"),
                ChunkletOp::Reintegrate { device } => format!("chunklet-reintegrate {device}"),
                ChunkletOp::Rebalance {
                    target_skew_pct,
                    max_moves,
                } => format!("chunklet-rebalance {target_skew_pct} {max_moves}"),
                ChunkletOp::Fsck => "chunklet-fsck".to_string(),
                ChunkletOp::Drain { pd_id } => format!("chunklet-drain {pd_id}"),
                ChunkletOp::RetireFailed { pd_id } => format!("chunklet-retire-failed {pd_id}"),
                ChunkletOp::ClearFailed { pd_id } => format!("chunklet-clear-failed {pd_id}"),
            };
            let lines = service::send_chunklet_command(sock, &cmd)?;
            match &op {
                ChunkletOp::Scrub { .. }
                | ChunkletOp::Rebuild { .. }
                | ChunkletOp::Reintegrate { .. }
                | ChunkletOp::Rebalance { .. }
                | ChunkletOp::Fsck
                | ChunkletOp::Drain { .. } => {
                    // Reply is `ok <job_id>`.
                    let job_id = lines
                        .iter()
                        .find_map(|l| l.strip_prefix("ok "))
                        .unwrap_or("?");
                    println!("started job {job_id}; poll with: chunklet job {job_id}");
                }
                ChunkletOp::Extend { role, .. } => {
                    // Reply is `ok <new_capacity_bytes>`.
                    let cap = lines
                        .iter()
                        .find_map(|l| l.strip_prefix("ok "))
                        .unwrap_or("?");
                    println!("extended {role}: new LD capacity = {cap} bytes");
                }
                _ => {
                    for line in &lines {
                        println!("{line}");
                    }
                }
            }
        }
    }

    Ok(())
}
