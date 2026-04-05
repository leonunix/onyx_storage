use std::path::PathBuf;

use clap::{Parser, Subcommand};

use onyx_storage::config::OnyxConfig;
use onyx_storage::engine::OnyxEngine;
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
    /// Start the storage engine daemon
    Start {
        /// Volume name to serve
        #[arg(short, long)]
        volume: String,
        /// Compression algorithm: none, lz4, zstd
        #[arg(long, default_value = "lz4")]
        compression: String,
    },
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
        Command::Start {
            volume,
            compression,
        } => {
            tracing::info!("starting onyx storage engine");

            let algo = parse_compression(&compression);
            let engine = OnyxEngine::open(&config, algo)?;
            let _vol = engine.open_volume(&volume)?;

            #[cfg(target_os = "linux")]
            {
                use std::sync::Arc;
                let zm = engine.zone_manager().expect("engine has zone manager");
                let vol_config = engine
                    .meta()
                    .get_volume(&onyx_storage::types::VolumeId(volume.clone()))?
                    .expect("volume exists");
                let target = onyx_storage::frontend::ublk::OnyxUblkTarget::new(
                    &config.ublk,
                    zm.clone(),
                    &vol_config,
                )?;
                tracing::info!("starting ublk device for volume '{}'", volume);
                target.run()?;
            }

            #[cfg(not(target_os = "linux"))]
            {
                tracing::warn!("ublk is only available on Linux, engine will idle");
                tracing::info!("volume '{}' ready via library API", volume);
                let (tx, rx) = std::sync::mpsc::channel();
                ctrlc_channel(&tx);
                let _ = rx.recv();
                tracing::info!("shutting down");
                engine.shutdown()?;
            }

            tracing::info!("engine stopped");
        }
        Command::CreateVolume {
            name,
            size,
            compression,
        } => {
            let engine = OnyxEngine::open_meta_only(&config)?;
            let algo = parse_compression(&compression);
            engine.create_volume(&name, size, algo)?;
            println!(
                "Volume '{}' created ({} bytes, compression={})",
                name, size, compression
            );
        }
        Command::DeleteVolume { name } => {
            let engine = OnyxEngine::open_meta_only(&config)?;
            let freed = engine.delete_volume(&name)?;
            println!("Volume '{}' deleted ({} physical blocks freed)", name, freed);
        }
        Command::ListVolumes => {
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
        Command::Status => {
            println!("onyx-storage v{}", env!("CARGO_PKG_VERSION"));
            println!("config: {:?}", cli.config);
        }
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn ctrlc_channel(tx: &std::sync::mpsc::Sender<()>) {
    let tx = tx.clone();
    std::thread::spawn(move || {
        let mut input = String::new();
        println!("Press Enter to stop...");
        let _ = std::io::stdin().read_line(&mut input);
        let _ = tx.send(());
    });
}
