use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};

use onyx_storage::config::OnyxConfig;
use onyx_storage::engine::OnyxEngine;
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
    #[arg(short, long, default_value = "config/default.toml")]
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
}

fn parse_compression(s: &str) -> CompressionAlgo {
    match s.to_lowercase().as_str() {
        "none" => CompressionAlgo::None,
        "lz4" => CompressionAlgo::Lz4,
        "zstd" => CompressionAlgo::Zstd { level: 3 },
        _ => CompressionAlgo::Lz4,
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

    match cli.command {
        Command::Start { volume } => {
            tracing::info!("starting onyx storage engine");

            let controller = ServiceController::new(config.clone())?;
            let controller = Arc::new(controller);

            // Register signal handlers for graceful shutdown
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
        Command::CreateVolume {
            name,
            size,
            compression,
        } => {
            let sock = &config.service.socket_path;
            if sock.exists() {
                service::send_create_volume(sock, &name, size, &compression)?;
            } else {
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
                service::send_delete_volume(sock, &name)?
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
                let lines = service::send_list_volumes(sock)?;
                if lines.is_empty() {
                    println!("No volumes");
                } else {
                    for line in &lines {
                        println!("  {}", line);
                    }
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
            println!("onyx-storage v{}", env!("CARGO_PKG_VERSION"));
            println!("config: {:?}", cli.config);

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

    Ok(())
}
