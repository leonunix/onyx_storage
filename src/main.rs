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
    /// Show engine status
    Status,
    /// Kill stale Linux ublk devices left behind after abnormal exit
    CleanupUblk,
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
    }

    Ok(())
}
