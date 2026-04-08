// Service controller: manages multi-volume ublk lifecycle and Unix socket IPC.
// This module is only compiled on Linux (ublk is Linux-only).

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use crate::config::OnyxConfig;
use crate::engine::OnyxEngine;
use crate::error::{OnyxError, OnyxResult};
#[cfg(target_os = "linux")]
use crate::frontend::ublk::OnyxUblkTarget;
use crate::types::{CompressionAlgo, VolumeConfig};

/// Manages the engine lifecycle: multiple ublk devices + Unix socket for IPC.
pub struct ServiceController {
    engine: Arc<OnyxEngine>,
    config: OnyxConfig,
    socket_path: PathBuf,
    shutdown: Arc<AtomicBool>,
    /// Kernel device IDs of running ublk devices, protected by mutex for
    /// concurrent access from device threads and socket listener.
    dev_ids: Arc<Mutex<Vec<u32>>>,
}

impl ServiceController {
    /// Create a new service controller. Does NOT start anything yet.
    pub fn new(config: OnyxConfig) -> OnyxResult<Self> {
        let engine = Arc::new(OnyxEngine::open(&config)?);
        let socket_path = config.service.socket_path.clone();

        Ok(Self {
            engine,
            config,
            socket_path,
            shutdown: Arc::new(AtomicBool::new(false)),
            dev_ids: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Start serving the given volumes (or all if empty).
    /// Blocks until shutdown is triggered (via socket or signal).
    pub fn run(&self, volume_names: &[String]) -> OnyxResult<()> {
        let volumes = self.resolve_volumes(volume_names)?;
        if volumes.is_empty() {
            return Err(OnyxError::Config("no volumes to start".into()));
        }

        for vol in &volumes {
            tracing::info!(volume = %vol.id.0, "opening volume");
            self.engine.open_volume(&vol.id.0)?;
        }

        // Clean up stale socket file
        if self.socket_path.exists() {
            let _ = std::fs::remove_file(&self.socket_path);
        }

        // Start socket listener thread
        let socket_handle = self.start_socket_listener()?;

        // Start ublk devices on Linux
        #[cfg(target_os = "linux")]
        let device_handles = self.start_ublk_devices(&volumes)?;

        #[cfg(not(target_os = "linux"))]
        let device_handles: Vec<JoinHandle<OnyxResult<()>>> = Vec::new();

        #[cfg(not(target_os = "linux"))]
        {
            tracing::warn!("ublk is only available on Linux, engine will idle");
            // Wait for shutdown signal on non-Linux
            while !self.shutdown.load(Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_millis(200));
            }
        }

        // Wait for all device threads to finish
        for handle in device_handles {
            if let Err(e) = handle.join() {
                tracing::error!("ublk device thread panicked: {:?}", e);
            }
        }

        // Graceful engine shutdown (flusher, GC, dedup, zones)
        self.engine.shutdown()?;

        // Stop socket listener
        self.shutdown.store(true, Ordering::Relaxed);
        // Connect to unblock the listener's accept()
        let _ = UnixStream::connect(&self.socket_path);
        if let Err(e) = socket_handle.join() {
            tracing::error!("socket listener thread panicked: {:?}", e);
        }
        let _ = std::fs::remove_file(&self.socket_path);

        tracing::info!("service stopped");
        Ok(())
    }

    /// Trigger graceful shutdown: kill all ublk devices, then engine shutdown
    /// will happen when run() resumes.
    pub fn trigger_shutdown(&self) {
        if self.shutdown.swap(true, Ordering::SeqCst) {
            return; // Already shutting down
        }
        tracing::info!("shutdown requested");

        // Kill all ublk devices — this causes run_target() to return in each thread
        #[cfg(target_os = "linux")]
        {
            let dev_ids = self.dev_ids.lock().unwrap().clone();
            for dev_id in dev_ids {
                tracing::info!(dev_id, "stopping ublk device");
                if let Err(e) = OnyxUblkTarget::kill_device(dev_id) {
                    tracing::warn!(dev_id, error = %e, "failed to kill ublk device");
                }
            }
        }
    }

    fn resolve_volumes(&self, names: &[String]) -> OnyxResult<Vec<VolumeConfig>> {
        if names.is_empty() {
            self.engine.list_volumes()
        } else {
            let mut volumes = Vec::with_capacity(names.len());
            for name in names {
                let vol = self
                    .engine
                    .meta()
                    .get_volume(&crate::types::VolumeId(name.clone()))?
                    .ok_or_else(|| OnyxError::VolumeNotFound(name.clone()))?;
                volumes.push(vol);
            }
            Ok(volumes)
        }
    }

    #[cfg(target_os = "linux")]
    fn start_ublk_devices(&self, volumes: &[VolumeConfig]) -> OnyxResult<Vec<JoinHandle<OnyxResult<()>>>> {
        let zm = self
            .engine
            .zone_manager()
            .ok_or_else(|| OnyxError::Config("no zone manager in meta-only mode".into()))?
            .clone();

        let mut handles = Vec::with_capacity(volumes.len());

        for vol in volumes {
            let target = OnyxUblkTarget::new(&self.config.ublk, zm.clone(), vol)?;
            let dev_ids = self.dev_ids.clone();
            let vol_name = vol.id.0.clone();

            let handle = thread::Builder::new()
                .name(format!("ublk-{}", vol_name))
                .spawn(move || {
                    let (tx, rx) = std::sync::mpsc::channel();
                    // Spawn a helper to register the dev_id once it's ready
                    let dev_ids_inner = dev_ids.clone();
                    let vol_name_inner = vol_name.clone();
                    thread::spawn(move || {
                        if let Ok(id) = rx.recv() {
                            tracing::info!(volume = %vol_name_inner, dev_id = id, "ublk device registered");
                            dev_ids_inner.lock().unwrap().push(id);
                        }
                    });
                    target.run(Some(tx))?;
                    tracing::info!(volume = %vol_name, "ublk device stopped");
                    Ok(())
                })
                .map_err(|e| OnyxError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

            handles.push(handle);
        }

        Ok(handles)
    }

    fn start_socket_listener(&self) -> OnyxResult<JoinHandle<()>> {
        // Ensure parent directory exists
        if let Some(parent) = self.socket_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                OnyxError::Config(format!(
                    "failed to create socket directory {:?}: {}",
                    parent, e
                ))
            })?;
        }

        let listener = UnixListener::bind(&self.socket_path).map_err(|e| {
            OnyxError::Config(format!(
                "failed to bind socket {:?}: {}",
                self.socket_path, e
            ))
        })?;
        listener
            .set_nonblocking(false)
            .map_err(|e| OnyxError::Io(e))?;

        let shutdown = self.shutdown.clone();
        let dev_ids = self.dev_ids.clone();
        let socket_path = self.socket_path.clone();
        let engine = self.engine.clone();

        let handle = thread::Builder::new()
            .name("ipc-listener".into())
            .spawn(move || {
                Self::socket_loop(&listener, &shutdown, &dev_ids, &socket_path, &engine);
            })
            .map_err(|e| OnyxError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

        tracing::info!(path = %self.socket_path.display(), "IPC socket listening");
        Ok(handle)
    }

    fn socket_loop(
        listener: &UnixListener,
        shutdown: &AtomicBool,
        dev_ids: &Mutex<Vec<u32>>,
        _socket_path: &Path,
        engine: &Arc<OnyxEngine>,
    ) {
        for stream in listener.incoming() {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            match stream {
                Ok(stream) => {
                    Self::handle_client(stream, shutdown, dev_ids, engine);
                }
                Err(e) => {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    tracing::warn!(error = %e, "socket accept error");
                }
            }
        }
    }

    fn handle_client(
        mut stream: UnixStream,
        shutdown: &AtomicBool,
        dev_ids: &Mutex<Vec<u32>>,
        engine: &Arc<OnyxEngine>,
    ) {
        let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(5)));

        let stream_clone = match stream.try_clone() {
            Ok(s) => s,
            Err(_) => return,
        };
        let reader = BufReader::new(stream_clone);
        for line in reader.lines() {
            let line = match line {
                Ok(l) => l,
                Err(_) => break,
            };
            let cmd = line.trim().to_string();
            let parts: Vec<&str> = cmd.splitn(4, ' ').collect();
            match parts[0] {
                "shutdown" | "stop" => {
                    tracing::info!("received shutdown command via socket");
                    shutdown.store(true, Ordering::SeqCst);

                    #[cfg(target_os = "linux")]
                    {
                        let ids = dev_ids.lock().unwrap().clone();
                        for dev_id in ids {
                            tracing::info!(dev_id, "stopping ublk device");
                            if let Err(e) = OnyxUblkTarget::kill_device(dev_id) {
                                tracing::warn!(dev_id, error = %e, "failed to kill ublk device");
                            }
                        }
                    }
                    let _ = stream.write_all(b"ok\n");
                    let _ = stream.flush();
                    return;
                }
                "create-volume" => {
                    // create-volume <name> <size> <compression>
                    if parts.len() < 4 {
                        let _ = stream.write_all(b"error: usage: create-volume <name> <size> <compression>\n");
                        let _ = stream.flush();
                        continue;
                    }
                    let name = parts[1];
                    let size: u64 = match parts[2].parse() {
                        Ok(s) => s,
                        Err(_) => {
                            let _ = stream.write_all(b"error: invalid size\n");
                            let _ = stream.flush();
                            continue;
                        }
                    };
                    let algo = match parts[3].to_lowercase().as_str() {
                        "none" => CompressionAlgo::None,
                        "lz4" => CompressionAlgo::Lz4,
                        "zstd" => CompressionAlgo::Zstd { level: 3 },
                        _ => CompressionAlgo::Lz4,
                    };
                    match engine.create_volume(name, size, algo) {
                        Ok(_) => {
                            let msg = format!("ok {}\n", name);
                            let _ = stream.write_all(msg.as_bytes());
                        }
                        Err(e) => {
                            let msg = format!("error: {}\n", e);
                            let _ = stream.write_all(msg.as_bytes());
                        }
                    }
                    let _ = stream.flush();
                }
                "delete-volume" => {
                    // delete-volume <name>
                    if parts.len() < 2 {
                        let _ = stream.write_all(b"error: usage: delete-volume <name>\n");
                        let _ = stream.flush();
                        continue;
                    }
                    let name = parts[1];
                    match engine.delete_volume(name) {
                        Ok(freed) => {
                            let msg = format!("ok {}\n", freed);
                            let _ = stream.write_all(msg.as_bytes());
                        }
                        Err(e) => {
                            let msg = format!("error: {}\n", e);
                            let _ = stream.write_all(msg.as_bytes());
                        }
                    }
                    let _ = stream.flush();
                }
                "list-volumes" => {
                    match engine.list_volumes() {
                        Ok(volumes) => {
                            for vol in &volumes {
                                let msg = format!(
                                    "{} {} {} {:?}\n",
                                    vol.id.0, vol.size_bytes, vol.zone_count, vol.compression
                                );
                                let _ = stream.write_all(msg.as_bytes());
                            }
                            let _ = stream.write_all(b"ok\n");
                        }
                        Err(e) => {
                            let msg = format!("error: {}\n", e);
                            let _ = stream.write_all(msg.as_bytes());
                        }
                    }
                    let _ = stream.flush();
                }
                "status" => {
                    let ids = dev_ids.lock().unwrap();
                    let msg = format!("running, {} ublk device(s): {:?}\n", ids.len(), *ids);
                    let _ = stream.write_all(msg.as_bytes());
                    let _ = stream.flush();
                }
                "ping" => {
                    let _ = stream.write_all(b"pong\n");
                    let _ = stream.flush();
                }
                _ => {
                    let _ = stream.write_all(b"error: unknown command\n");
                    let _ = stream.flush();
                }
            }
        }
    }
}

/// Send a command to a running service via its Unix socket.
/// Returns all response lines (without the trailing "ok" line).
fn send_ipc_command(socket_path: &Path, command: &str) -> OnyxResult<Vec<String>> {
    let mut stream = UnixStream::connect(socket_path).map_err(|e| {
        OnyxError::Config(format!(
            "cannot connect to {:?} — is the engine running? ({})",
            socket_path, e
        ))
    })?;

    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(30)))
        .map_err(OnyxError::Io)?;
    stream
        .write_all(format!("{}\n", command).as_bytes())
        .map_err(OnyxError::Io)?;
    stream.flush().map_err(OnyxError::Io)?;

    let reader = BufReader::new(stream);
    let mut lines = Vec::new();
    for line in reader.lines() {
        let line = line.map_err(OnyxError::Io)?;
        let trimmed = line.trim().to_string();
        if trimmed.starts_with("error:") {
            return Err(OnyxError::Config(trimmed["error:".len()..].trim().to_string()));
        }
        if trimmed == "ok" || trimmed.starts_with("ok ") {
            lines.push(trimmed);
            break;
        }
        lines.push(trimmed);
    }
    Ok(lines)
}

/// Send a stop command to a running service.
pub fn send_stop_command(socket_path: &Path) -> OnyxResult<()> {
    send_ipc_command(socket_path, "shutdown")?;
    Ok(())
}

/// Create a volume on a running service via IPC.
pub fn send_create_volume(
    socket_path: &Path,
    name: &str,
    size: u64,
    compression: &str,
) -> OnyxResult<()> {
    let cmd = format!("create-volume {} {} {}", name, size, compression);
    send_ipc_command(socket_path, &cmd)?;
    Ok(())
}

/// Delete a volume on a running service via IPC. Returns freed block count.
pub fn send_delete_volume(socket_path: &Path, name: &str) -> OnyxResult<u64> {
    let lines = send_ipc_command(socket_path, &format!("delete-volume {}", name))?;
    // Response: "ok <freed>"
    if let Some(line) = lines.first() {
        if let Some(freed_str) = line.strip_prefix("ok ") {
            return freed_str.parse().map_err(|_| {
                OnyxError::Config(format!("invalid freed count: {}", freed_str))
            });
        }
    }
    Ok(0)
}

/// List volumes on a running service via IPC.
pub fn send_list_volumes(socket_path: &Path) -> OnyxResult<Vec<String>> {
    let lines = send_ipc_command(socket_path, "list-volumes")?;
    // Filter out the trailing "ok"
    Ok(lines.into_iter().filter(|l| l != "ok").collect())
}
