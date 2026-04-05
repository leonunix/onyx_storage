use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};

use onyx_storage::buffer::pool::WriteBufferPool;
use onyx_storage::config::OnyxConfig;
use onyx_storage::io::device::RawDevice;
use onyx_storage::io::engine::IoEngine;
use onyx_storage::meta::store::MetaStore;
use onyx_storage::space::allocator::SpaceAllocator;
use onyx_storage::types::{CompressionAlgo, VolumeConfig, VolumeId};
use onyx_storage::zone::manager::ZoneManager;

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
        /// Number of zones
        #[arg(long, default_value = "4")]
        zones: u32,
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

            // 1. Open MetaStore
            let meta = Arc::new(MetaStore::open(&config.meta)?);

            // 2. Find volume
            let vol_id = VolumeId(volume);
            let vol_config = meta
                .get_volume(&vol_id)?
                .ok_or_else(|| anyhow::anyhow!("volume '{}' not found", vol_id))?;

            // 3. Open data device + IO engine
            let data_dev = RawDevice::open(&config.storage.data_device)?;
            let device_size = data_dev.size();
            let io_engine = Arc::new(IoEngine::new(data_dev, config.storage.use_hugepages));

            // 4. Initialize space allocator
            let allocator = Arc::new(SpaceAllocator::new(device_size));
            allocator.rebuild_from_metadata(&meta)?;

            // 5. Open write buffer pool
            let buf_dev = RawDevice::open(&config.buffer.device)?;
            let buffer_pool = Arc::new(WriteBufferPool::open(buf_dev)?);

            // 6. Recover unflushed buffer entries
            let unflushed = buffer_pool.recover()?;
            if !unflushed.is_empty() {
                tracing::info!(
                    count = unflushed.len(),
                    "replaying unflushed buffer entries"
                );
                // Replay is handled by the flusher on startup
            }

            // 7. Start background flusher (3-stage pipeline)
            let _flusher = onyx_storage::buffer::flush::BufferFlusher::start(
                buffer_pool.clone(),
                meta.clone(),
                allocator.clone(),
                io_engine.clone(),
                vol_config.compression,
                &config.flush,
            );

            // 8. Start zone manager
            let zone_size = 256u64; // blocks per zone
            let mut zone_manager = ZoneManager::new(
                vol_config.zone_count,
                zone_size,
                meta.clone(),
                buffer_pool.clone(),
                io_engine.clone(),
                vol_config.compression,
            )?;

            // 9. Start ublk target (Linux only)
            #[cfg(target_os = "linux")]
            {
                let zone_manager = Arc::new(zone_manager);
                let target = onyx_storage::frontend::ublk::OnyxUblkTarget::new(
                    &config.ublk,
                    zone_manager,
                    &vol_config,
                )?;
                tracing::info!("starting ublk device for volume '{}'", vol_id);
                target.run()?;
            }

            #[cfg(not(target_os = "linux"))]
            {
                tracing::warn!("ublk is only available on Linux, engine will idle");
                tracing::info!(
                    "volume '{}': {} bytes, {} zones, compression={:?}",
                    vol_id,
                    vol_config.size_bytes,
                    vol_config.zone_count,
                    vol_config.compression,
                );
                // Wait for Ctrl+C
                let (tx, rx) = std::sync::mpsc::channel();
                ctrlc_channel(&tx);
                let _ = rx.recv();
                tracing::info!("shutting down");
                zone_manager.shutdown()?;
            }

            tracing::info!("engine stopped");
        }
        Command::CreateVolume {
            name,
            size,
            compression,
            zones,
        } => {
            let meta = MetaStore::open(&config.meta)?;
            let vol = VolumeConfig {
                id: VolumeId(name.clone()),
                size_bytes: size,
                block_size: 4096,
                compression: parse_compression(&compression),
                created_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                zone_count: zones,
            };
            meta.put_volume(&vol)?;
            println!(
                "Volume '{}' created ({} bytes, compression={})",
                name, size, compression
            );
        }
        Command::DeleteVolume { name } => {
            let meta = MetaStore::open(&config.meta)?;
            let freed_pbas = meta.delete_volume(&VolumeId(name.clone()))?;
            println!(
                "Volume '{}' deleted ({} physical blocks freed)",
                name,
                freed_pbas.len()
            );
        }
        Command::ListVolumes => {
            let meta = MetaStore::open(&config.meta)?;
            let volumes = meta.list_volumes()?;
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
        // Simple: wait for user input
        let mut input = String::new();
        println!("Press Enter to stop...");
        let _ = std::io::stdin().read_line(&mut input);
        let _ = tx.send(());
    });
}
