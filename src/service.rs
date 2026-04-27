// Service controller: manages multi-volume ublk lifecycle, Unix socket IPC,
// bare/standby/active mode transitions, and config reload.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use arc_swap::ArcSwap;

use crate::config::{ConfiguredMode, OnyxConfig};
use crate::engine::OnyxEngine;
use crate::error::{OnyxError, OnyxResult};
#[cfg(target_os = "linux")]
use crate::frontend::ublk::OnyxUblkTarget;
use crate::types::{CompressionAlgo, VolumeConfig};

// ── Helpers for engine behind ArcSwap<Option<…>> ────────────────────────

/// Convenience: load the current engine from the ArcSwap, return error if bare.
/// Check that the engine is available; if bare mode, send error and `continue`.
macro_rules! require_engine {
    ($engine:expr, $stream:expr) => {{
        let __guard = $engine.load();
        let __opt: &Option<OnyxEngine> = &__guard;
        if __opt.is_none() {
            let _ = $stream.write_all(
                b"error: engine not initialised (bare mode) - configure and reload first\n",
            );
            let _ = $stream.flush();
            continue;
        }
    }};
}

/// Manages the engine lifecycle: multiple ublk devices + Unix socket for IPC.
pub struct ServiceController {
    /// `None` = bare mode (no engine). `Some` = standby or active.
    engine: Arc<ArcSwap<Option<OnyxEngine>>>,
    config: parking_lot::RwLock<OnyxConfig>,
    config_path: PathBuf,
    socket_path: PathBuf,
    shutdown: Arc<AtomicBool>,
    reload_signal: Arc<AtomicBool>,
    /// Kernel device IDs of running ublk devices.
    dev_ids: Arc<Mutex<Vec<u32>>>,
}

impl ServiceController {
    /// Create a new service controller.
    /// Auto-detects bare / standby / active mode from configuration.
    pub fn new(config: OnyxConfig, config_path: PathBuf) -> OnyxResult<Self> {
        let detected = config.detect_mode();

        let engine: Option<OnyxEngine> = match detected {
            ConfiguredMode::Bare => {
                tracing::info!("no storage paths configured — starting in bare mode (IPC only)");
                None
            }
            ConfiguredMode::Standby => {
                tracing::info!(
                    "storage devices not configured — starting in standby mode (metadata only)"
                );
                Some(OnyxEngine::open_meta_only(&config)?)
            }
            ConfiguredMode::Active => Some(OnyxEngine::open(&config)?),
        };

        let socket_path = config.service.socket_path.clone();

        Ok(Self {
            engine: Arc::new(ArcSwap::from_pointee(engine)),
            config: parking_lot::RwLock::new(config),
            config_path,
            socket_path,
            shutdown: Arc::new(AtomicBool::new(false)),
            reload_signal: Arc::new(AtomicBool::new(false)),
            dev_ids: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Start serving volumes (or idle in bare/standby mode).
    /// Blocks until shutdown is triggered (via socket, signal, or Ctrl+C).
    pub fn run(&self, volume_names: &[String]) -> OnyxResult<()> {
        // Clean up stale socket file
        if self.socket_path.exists() {
            let _ = std::fs::remove_file(&self.socket_path);
        }

        // Start socket listener thread (always, even in bare/standby)
        let socket_handle = self.start_socket_listener()?;

        // In active mode, open volumes and start ublk devices
        #[cfg(target_os = "linux")]
        let mut device_handles: Vec<JoinHandle<OnyxResult<()>>> = Vec::new();

        {
            let guard = self.engine.load();
            let opt: &Option<OnyxEngine> = &guard;
            match opt.as_ref() {
                Some(eng) if eng.is_full_mode() => {
                    let volumes = self.resolve_volumes(volume_names)?;
                    if !volumes.is_empty() {
                        for vol in &volumes {
                            tracing::info!(volume = %vol.id.0, "opening volume");
                            eng.open_volume(&vol.id.0)?;
                        }
                        #[cfg(target_os = "linux")]
                        {
                            device_handles = self.start_ublk_devices(&volumes)?;
                        }
                    }
                }
                Some(_) => {
                    tracing::info!("standby mode -- waiting for config reload to activate");
                }
                None => {
                    tracing::info!("bare mode -- waiting for config reload");
                }
            }
        }

        // Main loop: poll shutdown and reload flags
        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            let needs_reload = self.reload_signal.swap(false, Ordering::SeqCst)
                || crate::signal::take_reload_flag();
            if needs_reload {
                if let Err(e) = self.handle_reload() {
                    tracing::error!(error = %e, "config reload failed");
                }
            }

            std::thread::sleep(std::time::Duration::from_millis(200));
        }

        // Graceful shutdown
        {
            let guard = self.engine.load();
            let opt: &Option<OnyxEngine> = &guard;
            if let Some(eng) = opt.as_ref() {
                eng.shutdown()?;
            }
        }

        // Stop socket listener
        let _ = UnixStream::connect(&self.socket_path);
        if let Err(e) = socket_handle.join() {
            tracing::error!("socket listener thread panicked: {:?}", e);
        }
        let _ = std::fs::remove_file(&self.socket_path);

        // Wait for ublk device threads
        #[cfg(target_os = "linux")]
        for handle in device_handles {
            if let Err(e) = handle.join() {
                tracing::error!("ublk device thread panicked: {:?}", e);
            }
        }

        tracing::info!("service stopped");
        Ok(())
    }

    /// Trigger graceful shutdown.
    pub fn trigger_shutdown(&self) {
        if self.shutdown.swap(true, Ordering::SeqCst) {
            return;
        }
        tracing::info!("shutdown requested");

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

    /// Trigger a config reload (from IPC).
    pub fn trigger_reload(&self) {
        self.reload_signal.store(true, Ordering::SeqCst);
    }

    // ── Reload logic ────────────────────────────────────────────────────

    fn handle_reload(&self) -> OnyxResult<()> {
        tracing::info!(path = %self.config_path.display(), "reloading configuration");

        let new_config = OnyxConfig::load(&self.config_path)?;
        let new_mode = new_config.detect_mode();
        let guard = self.engine.load();
        let engine_ref: &Option<OnyxEngine> = &guard;
        let has_engine = engine_ref.is_some();
        let is_active = engine_ref
            .as_ref()
            .map(|e| e.is_full_mode())
            .unwrap_or(false);
        drop(guard);

        match (has_engine, is_active, new_mode) {
            // bare → standby
            (false, _, ConfiguredMode::Standby) => {
                tracing::info!("bare → standby: opening metadata store");
                let eng = OnyxEngine::open_meta_only(&new_config)?;
                self.engine.store(Arc::new(Some(eng)));
            }
            // bare → active
            (false, _, ConfiguredMode::Active) => {
                tracing::info!("bare → active: opening full engine");
                let eng = OnyxEngine::open(&new_config)?;
                let eng = Arc::new(Some(eng));
                self.engine.store(eng.clone());
                self.activate_volumes(&new_config)?;
            }
            // standby → active
            (true, false, ConfiguredMode::Active) => {
                self.transition_to_active(&new_config)?;
            }
            // active → active: hot-reload params
            (true, true, ConfiguredMode::Active) => {
                self.hot_reload_params(&new_config);
            }
            // still bare
            (false, _, ConfiguredMode::Bare) => {
                tracing::info!("config reload: still in bare mode (nothing configured)");
            }
            // still standby
            (true, false, ConfiguredMode::Standby) => {
                tracing::info!("config reload: still in standby (devices not configured)");
            }
            // active → downgrade not supported
            (true, true, ConfiguredMode::Standby | ConfiguredMode::Bare) => {
                tracing::warn!("config reload: downgrade from active requires a restart");
            }
            // bare → bare (already covered above but for completeness)
            _ => {}
        }

        *self.config.write() = new_config;
        Ok(())
    }

    fn transition_to_active(&self, new_config: &OnyxConfig) -> OnyxResult<()> {
        tracing::info!("transitioning from standby to active mode");

        // Extract MetaStore from old engine (avoid metadb double-open)
        let meta = {
            let guard = self.engine.load();
            let opt: &Option<OnyxEngine> = &guard;
            let old_engine = opt
                .as_ref()
                .ok_or_else(|| OnyxError::Config("expected standby engine but got bare".into()))?;
            let meta = old_engine.meta().clone();
            old_engine.shutdown()?;
            meta
        };

        // Build full engine reusing the existing MetaStore
        let new_engine = OnyxEngine::upgrade_from_meta_only(meta, new_config)?;
        self.engine.store(Arc::new(Some(new_engine)));

        self.activate_volumes(new_config)?;
        tracing::info!("engine activated successfully");
        Ok(())
    }

    /// Open all volumes and start ublk devices for them.
    fn activate_volumes(&self, _new_config: &OnyxConfig) -> OnyxResult<()> {
        let guard = self.engine.load();
        let opt: &Option<OnyxEngine> = &guard;
        let engine = opt.as_ref().unwrap();
        let volumes = engine.list_volumes()?;

        for vol in &volumes {
            tracing::info!(volume = %vol.id.0, "opening volume after activation");
            engine.open_volume(&vol.id.0)?;
        }

        #[cfg(target_os = "linux")]
        if !volumes.is_empty() {
            let config = self.config.read();
            let zm = engine
                .zone_manager()
                .ok_or_else(|| OnyxError::Config("no zone manager after activation".into()))?
                .clone();

            for vol in &volumes {
                let target = OnyxUblkTarget::new(&config.ublk, zm.clone(), vol)?;
                let dev_ids = self.dev_ids.clone();
                let vol_name = vol.id.0.clone();

                thread::Builder::new()
                    .name(format!("ublk-{}", vol_name))
                    .spawn(move || {
                        let (tx, rx) = std::sync::mpsc::channel();
                        let dev_ids_inner = dev_ids.clone();
                        let vol_name_inner = vol_name.clone();
                        thread::spawn(move || {
                            if let Ok(id) = rx.recv() {
                                tracing::info!(volume = %vol_name_inner, dev_id = id, "ublk device registered");
                                dev_ids_inner.lock().unwrap().push(id);
                            }
                        });
                        if let Err(e) = target.run(Some(tx)) {
                            tracing::error!(volume = %vol_name, error = %e, "ublk device failed");
                        }
                    })
                    .map_err(|e| OnyxError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
            }
        }

        Ok(())
    }

    fn hot_reload_params(&self, new_config: &OnyxConfig) {
        let old_config = self.config.read();

        if old_config.storage.data_device != new_config.storage.data_device {
            tracing::warn!("storage.data_device changed — requires restart to take effect");
        }
        if old_config.buffer.device != new_config.buffer.device {
            tracing::warn!("buffer.device changed — requires restart to take effect");
        }
        if old_config.meta.path() != new_config.meta.path() {
            tracing::warn!("meta.path changed — requires restart to take effect");
        }
        if old_config.engine.zone_count != new_config.engine.zone_count {
            tracing::warn!("engine.zone_count changed — requires restart to take effect");
        }
        if old_config.buffer.shards != new_config.buffer.shards {
            tracing::warn!(
                "buffer.shards changed — requires restart. \
                 Will auto-reinit if buffer is fully drained, otherwise start with old shard count first to drain"
            );
        }

        {
            let guard = self.engine.load();
            let opt: &Option<OnyxEngine> = &guard;
            if let Some(eng) = opt.as_ref() {
                eng.update_gc_config(new_config.gc.clone());
                eng.update_dedup_config(new_config.dedup.clone());
            }
        }

        tracing::info!("configuration hot-reloaded (gc, dedup parameters updated)");
    }

    // ── Volume / device helpers ─────────────────────────────────────────

    fn resolve_volumes(&self, names: &[String]) -> OnyxResult<Vec<VolumeConfig>> {
        let guard = self.engine.load();
        let opt: &Option<OnyxEngine> = &guard;
        let engine = opt
            .as_ref()
            .ok_or_else(|| OnyxError::Config("engine not initialised".into()))?;
        if names.is_empty() {
            engine.list_volumes()
        } else {
            let mut volumes = Vec::with_capacity(names.len());
            for name in names {
                let vol = engine
                    .meta()
                    .get_volume(&crate::types::VolumeId(name.clone()))?
                    .ok_or_else(|| OnyxError::VolumeNotFound(name.clone()))?;
                volumes.push(vol);
            }
            Ok(volumes)
        }
    }

    #[cfg(target_os = "linux")]
    fn start_ublk_devices(
        &self,
        volumes: &[VolumeConfig],
    ) -> OnyxResult<Vec<JoinHandle<OnyxResult<()>>>> {
        let guard = self.engine.load();
        let opt: &Option<OnyxEngine> = &guard;
        let engine = opt
            .as_ref()
            .ok_or_else(|| OnyxError::Config("engine not initialised".into()))?;
        let zm = engine
            .zone_manager()
            .ok_or_else(|| OnyxError::Config("no zone manager in meta-only mode".into()))?
            .clone();
        let config = self.config.read();

        let mut handles = Vec::with_capacity(volumes.len());

        for vol in volumes {
            let target = OnyxUblkTarget::new(&config.ublk, zm.clone(), vol)?;
            let dev_ids = self.dev_ids.clone();
            let vol_name = vol.id.0.clone();

            let handle = thread::Builder::new()
                .name(format!("ublk-{}", vol_name))
                .spawn(move || {
                    let (tx, rx) = std::sync::mpsc::channel();
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

    // ── IPC socket ──────────────────────────────────────────────────────

    fn start_socket_listener(&self) -> OnyxResult<JoinHandle<()>> {
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
        let reload_signal = self.reload_signal.clone();

        let handle = thread::Builder::new()
            .name("ipc-listener".into())
            .spawn(move || {
                Self::socket_loop(
                    &listener,
                    &shutdown,
                    &dev_ids,
                    &socket_path,
                    &engine,
                    &reload_signal,
                );
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
        engine: &Arc<ArcSwap<Option<OnyxEngine>>>,
        reload_signal: &AtomicBool,
    ) {
        for stream in listener.incoming() {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            match stream {
                Ok(stream) => {
                    Self::handle_client(stream, shutdown, dev_ids, engine, reload_signal);
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
        engine: &Arc<ArcSwap<Option<OnyxEngine>>>,
        reload_signal: &AtomicBool,
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
                // ── Always-available commands ────────────────────────
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
                "reload" => {
                    tracing::info!("received reload command via socket");
                    reload_signal.store(true, Ordering::SeqCst);
                    let _ = stream.write_all(b"ok\n");
                    let _ = stream.flush();
                }
                "ping" => {
                    let _ = stream.write_all(b"pong\n");
                    let _ = stream.flush();
                }
                "status" => {
                    let guard = engine.load();
                    let opt: &Option<OnyxEngine> = &guard;
                    let ids = dev_ids.lock().unwrap();
                    match opt.as_ref() {
                        None => {
                            let msg =
                                format!("mode: bare, {} ublk device(s): {:?}\n", ids.len(), *ids);
                            let _ = stream.write_all(msg.as_bytes());
                        }
                        Some(eng) => {
                            let mode_str = if eng.is_full_mode() {
                                "active"
                            } else {
                                "standby"
                            };
                            let msg = format!(
                                "mode: {}, {} ublk device(s): {:?}\n",
                                mode_str,
                                ids.len(),
                                *ids
                            );
                            let _ = stream.write_all(msg.as_bytes());
                            if let Ok(report) = eng.status_report() {
                                let _ = stream.write_all(report.as_bytes());
                            }
                        }
                    }
                    let _ = stream.write_all(b"ok\n");
                    let _ = stream.flush();
                }
                "mode" => {
                    let guard = engine.load();
                    let opt: &Option<OnyxEngine> = &guard;
                    let mode_str = match opt.as_ref() {
                        None => "bare",
                        Some(eng) if eng.is_full_mode() => "active",
                        Some(_) => "standby",
                    };
                    let msg = format!("{}\nok\n", mode_str);
                    let _ = stream.write_all(msg.as_bytes());
                    let _ = stream.flush();
                }

                // ── Commands that require an engine ─────────────────
                "create-volume" => {
                    require_engine!(engine, stream);
                    if parts.len() < 4 {
                        let _ = stream.write_all(
                            b"error: usage: create-volume <name> <size> <compression>\n",
                        );
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
                    let guard = engine.load();
                    let opt: &Option<OnyxEngine> = &guard;
                    let eng = opt.as_ref().unwrap();
                    match eng.create_volume(name, size, algo) {
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
                    require_engine!(engine, stream);
                    if parts.len() < 2 {
                        let _ = stream.write_all(b"error: usage: delete-volume <name>\n");
                        let _ = stream.flush();
                        continue;
                    }
                    let name = parts[1];
                    let guard = engine.load();
                    let opt: &Option<OnyxEngine> = &guard;
                    let eng = opt.as_ref().unwrap();
                    match eng.delete_volume(name) {
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
                    require_engine!(engine, stream);
                    let guard = engine.load();
                    let opt: &Option<OnyxEngine> = &guard;
                    let eng = opt.as_ref().unwrap();
                    match eng.list_volumes() {
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
                // ── JSON IPC commands (for dashboard) ────────────
                "status-json" => {
                    let guard = engine.load();
                    let opt: &Option<OnyxEngine> = &guard;
                    let ids = dev_ids.lock().unwrap().clone();
                    let mode_str = match opt.as_ref() {
                        None => "bare",
                        Some(eng) if eng.is_full_mode() => "active",
                        Some(_) => "standby",
                    };
                    let status = opt.as_ref().and_then(|eng| eng.status_snapshot().ok());
                    let payload = serde_json::json!({
                        "mode": mode_str,
                        "ublk_devices": ids,
                        "status": status,
                    });
                    let _ = stream.write_all(payload.to_string().as_bytes());
                    let _ = stream.write_all(b"\nok\n");
                    let _ = stream.flush();
                }
                "volumes-json" => {
                    let guard = engine.load();
                    let opt: &Option<OnyxEngine> = &guard;
                    match opt.as_ref() {
                        None => {
                            let _ = stream.write_all(b"[]\nok\n");
                        }
                        Some(eng) => match eng.list_volumes() {
                            Ok(volumes) => {
                                // Enrich each volume with per-volume IO metrics
                                let vol_metrics = eng.metrics_snapshot();
                                let per_vol = eng.volume_metrics_snapshot();
                                let enriched: Vec<serde_json::Value> = volumes
                                    .iter()
                                    .map(|v| {
                                        let vm = per_vol
                                            .iter()
                                            .find(|(name, _)| name == &v.id.0)
                                            .map(|(_, s)| s.clone());
                                        serde_json::json!({
                                            "id": v.id.0,
                                            "size_bytes": v.size_bytes,
                                            "block_size": v.block_size,
                                            "compression": v.compression,
                                            "created_at": v.created_at,
                                            "zone_count": v.zone_count,
                                            "metrics": vm,
                                        })
                                    })
                                    .collect();
                                let _ = vol_metrics; // suppress unused warning
                                let json = serde_json::to_string(&enriched)
                                    .unwrap_or_else(|_| "[]".into());
                                let _ = stream.write_all(json.as_bytes());
                                let _ = stream.write_all(b"\nok\n");
                            }
                            Err(e) => {
                                let msg = format!("error: {}\n", e);
                                let _ = stream.write_all(msg.as_bytes());
                            }
                        },
                    }
                    let _ = stream.flush();
                }
                "metrics-json" => {
                    let guard = engine.load();
                    let opt: &Option<OnyxEngine> = &guard;
                    match opt.as_ref() {
                        None => {
                            let _ = stream.write_all(b"{}\nok\n");
                        }
                        Some(eng) => {
                            let snapshot = eng.metrics_snapshot();
                            let json =
                                serde_json::to_string(&snapshot).unwrap_or_else(|_| "{}".into());
                            let _ = stream.write_all(json.as_bytes());
                            let _ = stream.write_all(b"\nok\n");
                        }
                    }
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

// ── IPC client helpers ──────────────────────────────────────────────────

/// Send a command to a running service via its Unix socket.
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
            return Err(OnyxError::Config(
                trimmed["error:".len()..].trim().to_string(),
            ));
        }
        if trimmed == "ok" || trimmed.starts_with("ok ") {
            lines.push(trimmed);
            break;
        }
        lines.push(trimmed);
    }
    Ok(lines)
}

pub fn send_stop_command(socket_path: &Path) -> OnyxResult<()> {
    send_ipc_command(socket_path, "shutdown")?;
    Ok(())
}

pub fn send_reload_command(socket_path: &Path) -> OnyxResult<()> {
    send_ipc_command(socket_path, "reload")?;
    Ok(())
}

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

pub fn send_delete_volume(socket_path: &Path, name: &str) -> OnyxResult<u64> {
    let lines = send_ipc_command(socket_path, &format!("delete-volume {}", name))?;
    if let Some(line) = lines.first() {
        if let Some(freed_str) = line.strip_prefix("ok ") {
            return freed_str
                .parse()
                .map_err(|_| OnyxError::Config(format!("invalid freed count: {}", freed_str)));
        }
    }
    Ok(0)
}

pub fn send_list_volumes(socket_path: &Path) -> OnyxResult<Vec<String>> {
    let lines = send_ipc_command(socket_path, "list-volumes")?;
    Ok(lines.into_iter().filter(|l| l != "ok").collect())
}

pub fn send_status_command(socket_path: &Path) -> OnyxResult<Vec<String>> {
    let lines = send_ipc_command(socket_path, "status")?;
    Ok(lines.into_iter().filter(|l| l != "ok").collect())
}

pub fn send_mode_command(socket_path: &Path) -> OnyxResult<String> {
    let lines = send_ipc_command(socket_path, "mode")?;
    Ok(lines.into_iter().find(|l| l != "ok").unwrap_or_default())
}
