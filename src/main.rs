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

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("onyx_storage=info".parse()?),
        )
        .init();

    let cli = Cli::parse();
    let config = OnyxConfig::load(&cli.config)?;
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
                |engine| engine.create_snapshot(&volume, &name).map(|info| info.snapshot_id),
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
                |engine| engine.clone_snapshot(&volume, &name, &to).map(|cfg| cfg.id.0),
            )
            .map(|new_vol| {
                println!("Snapshot '{}@{}' cloned into new volume '{}'", volume, name, new_vol)
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
                anyhow::bail!("refusing without --force (verify the disks, then re-run with --force)");
            }
            let (_pool, lv3, lv2, meta) = onyx_storage::chunklet_pool::init_pool(ck)?;
            println!("chunklet pool initialized over {} PDs", ck.devices.len());
            println!("add these LD ids under [chunklet] in your config:");
            println!("  lv3_ld_id = \"{lv3}\"");
            println!("  lv2_ld_id = \"{lv2}\"");
            println!("  meta_ld_id = \"{meta}\"");
        }
    }

    Ok(())
}
