// Service controller: manages multi-volume ublk lifecycle, Unix socket IPC,
// bare/standby/active mode transitions, and config reload.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use arc_swap::ArcSwap;

use crate::affinity::{self, ThreadRole};
use crate::config::{ConfiguredMode, OnyxConfig};
use crate::direct_io::DirectIoServer;
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

        // The binary data-plane listener must stop and join every session
        // before engine shutdown. Start it first so a control-socket bind
        // failure can still unwind it cleanly through Drop.
        let (nr_queues, queue_workers, direct_io_cpus) = {
            let config = self.config.read();
            (
                config.ublk.nr_queues as usize,
                config.ublk.queue_workers,
                config.service.direct_io_cpu_set()?,
            )
        };
        let mut direct_io_server = DirectIoServer::start(
            &self.socket_path,
            self.engine.clone(),
            nr_queues,
            queue_workers,
            direct_io_cpus,
        )
        .map_err(OnyxError::Io)?;

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

        // Stop accepting direct IO. Already-durable writes are acknowledged;
        // undurable tickets are failed after a bounded grace period so a dead
        // LV2 durability watermark cannot block engine shutdown forever.
        direct_io_server.shutdown_and_join();

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
        // Do not unlink the socket path on shutdown. A previous service can
        // still be unwinding while a new service has already bound the same
        // pathname; unlinking here would make the new listener unreachable
        // even though it continues to appear in `ss`. Startup already removes
        // stale socket files before binding.

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
    fn activate_volumes(&self, new_config: &OnyxConfig) -> OnyxResult<()> {
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
            let zm = engine
                .zone_manager()
                .ok_or_else(|| OnyxError::Config("no zone manager after activation".into()))?
                .clone();

            for vol in &volumes {
                let target = OnyxUblkTarget::new(&new_config.ublk, zm.clone(), vol)?;
                let dev_ids = self.dev_ids.clone();
                let vol_name = vol.id.0.clone();

                thread::Builder::new()
                    .name(format!("ublk-{}", vol_name))
                    .spawn(move || {
                        affinity::bind_current(ThreadRole::Ublk, 0);
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
        if old_config.meta.rc_checkpoint_streaming_enabled
            != new_config.meta.rc_checkpoint_streaming_enabled
        {
            tracing::warn!(
                old = old_config.meta.rc_checkpoint_streaming_enabled,
                new = new_config.meta.rc_checkpoint_streaming_enabled,
                "meta.rc_checkpoint_streaming_enabled changed — requires restart to take effect"
            );
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
        if old_config.buffer.lv2_prepared_queue_depth_per_lane
            != new_config.buffer.lv2_prepared_queue_depth_per_lane
        {
            tracing::warn!(
                "buffer.lv2_prepared_queue_depth_per_lane changed — requires restart to take effect"
            );
        }
        if old_config.buffer.throttle_min_pct != new_config.buffer.throttle_min_pct
            || old_config.buffer.throttle_max_pct != new_config.buffer.throttle_max_pct
            || old_config.buffer.throttle_scale_us != new_config.buffer.throttle_scale_us
            || old_config.buffer.throttle_cap_us != new_config.buffer.throttle_cap_us
            || old_config.buffer.throttle_backend_debt != new_config.buffer.throttle_backend_debt
        {
            tracing::warn!("buffer write throttle changed — requires restart to take effect");
        }
        if old_config.chunklet.write_max_active != new_config.chunklet.write_max_active
            || old_config.chunklet.write_foreground_active
                != new_config.chunklet.write_foreground_active
            || old_config.chunklet.write_lv3_active != new_config.chunklet.write_lv3_active
            || old_config.chunklet.write_meta_active != new_config.chunklet.write_meta_active
        {
            tracing::warn!("chunklet write scheduler changed — requires restart to take effect");
        }
        if old_config.chunklet.pd_write_max_active_blocks
            != new_config.chunklet.pd_write_max_active_blocks
            || old_config.chunklet.pd_write_foreground_min_blocks
                != new_config.chunklet.pd_write_foreground_min_blocks
            || old_config.chunklet.pd_write_lv3_min_blocks
                != new_config.chunklet.pd_write_lv3_min_blocks
            || old_config.chunklet.pd_write_meta_min_blocks
                != new_config.chunklet.pd_write_meta_min_blocks
            || old_config.chunklet.pd_write_maintenance_min_blocks
                != new_config.chunklet.pd_write_maintenance_min_blocks
        {
            tracing::warn!(
                "chunklet per-PD write scheduler changed — requires restart to take effect"
            );
        }
        if old_config.chunklet.pd_write_foreground_workers
            != new_config.chunklet.pd_write_foreground_workers
            || old_config.chunklet.pd_write_background_workers
                != new_config.chunklet.pd_write_background_workers
        {
            tracing::warn!(
                "chunklet persistent write execution pools changed — requires restart to take effect"
            );
        }
        if old_config.service.direct_io_cpus != new_config.service.direct_io_cpus {
            tracing::warn!(
                old = %old_config.service.direct_io_cpus,
                new = %new_config.service.direct_io_cpus,
                "service.direct_io_cpus changed — requires restart to rebuild direct IO and backend CPU masks"
            );
        }
        if old_config.ublk.nr_queues != new_config.ublk.nr_queues
            || old_config.ublk.queue_workers != new_config.ublk.queue_workers
        {
            tracing::warn!(
                "ublk submit topology changed — requires restart to rebuild direct IO submit lanes"
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
                    affinity::bind_current(ThreadRole::Ublk, 0);
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
        shutdown: &Arc<AtomicBool>,
        dev_ids: &Arc<Mutex<Vec<u32>>>,
        _socket_path: &Path,
        engine: &Arc<ArcSwap<Option<OnyxEngine>>>,
        reload_signal: &Arc<AtomicBool>,
    ) {
        for stream in listener.incoming() {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            match stream {
                Ok(stream) => {
                    let shutdown = Arc::clone(shutdown);
                    let dev_ids = Arc::clone(dev_ids);
                    let engine = Arc::clone(engine);
                    let reload_signal = Arc::clone(reload_signal);
                    if let Err(err) =
                        thread::Builder::new()
                            .name("ipc-client".into())
                            .spawn(move || {
                                Self::handle_client(
                                    stream,
                                    &shutdown,
                                    &dev_ids,
                                    &engine,
                                    &reload_signal,
                                );
                            })
                    {
                        tracing::warn!(error = %err, "failed to spawn IPC client handler");
                    }
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
                // ── Snapshot lifecycle commands ──────────────────
                "snapshot-create" => {
                    require_engine!(engine, stream);
                    if parts.len() < 3 {
                        let _ =
                            stream.write_all(b"error: usage: snapshot-create <volume> <name>\n");
                        let _ = stream.flush();
                        continue;
                    }
                    let guard = engine.load();
                    let opt: &Option<OnyxEngine> = &guard;
                    let eng = opt.as_ref().unwrap();
                    match eng.create_snapshot(parts[1], parts[2]) {
                        Ok(info) => {
                            let _ =
                                stream.write_all(format!("ok {}\n", info.snapshot_id).as_bytes());
                        }
                        Err(e) => {
                            let _ = stream.write_all(format!("error: {}\n", e).as_bytes());
                        }
                    }
                    let _ = stream.flush();
                }
                "snapshot-delete" => {
                    require_engine!(engine, stream);
                    if parts.len() < 3 {
                        let _ =
                            stream.write_all(b"error: usage: snapshot-delete <volume> <name>\n");
                        let _ = stream.flush();
                        continue;
                    }
                    let guard = engine.load();
                    let opt: &Option<OnyxEngine> = &guard;
                    let eng = opt.as_ref().unwrap();
                    match eng.delete_snapshot(parts[1], parts[2]) {
                        Ok(freed) => {
                            let _ = stream.write_all(format!("ok {}\n", freed).as_bytes());
                        }
                        Err(e) => {
                            let _ = stream.write_all(format!("error: {}\n", e).as_bytes());
                        }
                    }
                    let _ = stream.flush();
                }
                "snapshot-list" => {
                    require_engine!(engine, stream);
                    let guard = engine.load();
                    let opt: &Option<OnyxEngine> = &guard;
                    let eng = opt.as_ref().unwrap();
                    let volume = parts.get(1).copied().filter(|s| !s.is_empty());
                    match eng.list_snapshots(volume) {
                        Ok(snaps) => {
                            for s in &snaps {
                                let _ = stream.write_all(
                                    format!(
                                        "{}@{} id={} created_lsn={} created_at={} size={}\n",
                                        s.volume,
                                        s.name,
                                        s.snapshot_id,
                                        s.created_lsn,
                                        s.created_at,
                                        s.size_bytes
                                    )
                                    .as_bytes(),
                                );
                            }
                            let _ = stream.write_all(b"ok\n");
                        }
                        Err(e) => {
                            let _ = stream.write_all(format!("error: {}\n", e).as_bytes());
                        }
                    }
                    let _ = stream.flush();
                }
                "snapshot-clone" => {
                    require_engine!(engine, stream);
                    if parts.len() < 4 {
                        let _ = stream.write_all(
                            b"error: usage: snapshot-clone <volume> <name> <new_volume>\n",
                        );
                        let _ = stream.flush();
                        continue;
                    }
                    let guard = engine.load();
                    let opt: &Option<OnyxEngine> = &guard;
                    let eng = opt.as_ref().unwrap();
                    match eng.clone_snapshot(parts[1], parts[2], parts[3]) {
                        Ok(cfg) => {
                            let _ = stream.write_all(format!("ok {}\n", cfg.id.0).as_bytes());
                        }
                        Err(e) => {
                            let _ = stream.write_all(format!("error: {}\n", e).as_bytes());
                        }
                    }
                    let _ = stream.flush();
                }
                "snapshot-restore" => {
                    require_engine!(engine, stream);
                    if parts.len() < 3 {
                        let _ =
                            stream.write_all(b"error: usage: snapshot-restore <volume> <name>\n");
                        let _ = stream.flush();
                        continue;
                    }
                    let guard = engine.load();
                    let opt: &Option<OnyxEngine> = &guard;
                    let eng = opt.as_ref().unwrap();
                    match eng.restore_snapshot(parts[1], parts[2]) {
                        Ok(()) => {
                            let _ = stream.write_all(b"ok\n");
                        }
                        Err(e) => {
                            let _ = stream.write_all(format!("error: {}\n", e).as_bytes());
                        }
                    }
                    let _ = stream.flush();
                }
                "snapshots-json" => {
                    let guard = engine.load();
                    let opt: &Option<OnyxEngine> = &guard;
                    match opt.as_ref() {
                        None => {
                            let _ = stream.write_all(b"[]\nok\n");
                        }
                        Some(eng) => {
                            let volume = parts.get(1).copied().filter(|s| !s.is_empty());
                            match eng.list_snapshots(volume) {
                                Ok(snaps) => {
                                    let arr: Vec<serde_json::Value> = snaps
                                        .iter()
                                        .map(|s| {
                                            serde_json::json!({
                                                "volume": s.volume,
                                                "name": s.name,
                                                "snapshot_id": s.snapshot_id,
                                                // created_at is stored as epoch nanos
                                                // (matches volume.created_at); emit
                                                // epoch seconds for the dashboard.
                                                "created_at": s.created_at / 1_000_000_000,
                                                "created_lsn": s.created_lsn,
                                                "size_bytes": s.size_bytes,
                                            })
                                        })
                                        .collect();
                                    let json =
                                        serde_json::to_string(&arr).unwrap_or_else(|_| "[]".into());
                                    let _ = stream.write_all(json.as_bytes());
                                    let _ = stream.write_all(b"\nok\n");
                                }
                                Err(e) => {
                                    let _ = stream.write_all(format!("error: {}\n", e).as_bytes());
                                }
                            }
                        }
                    }
                    let _ = stream.flush();
                }

                "volume-usage" => {
                    let guard = engine.load();
                    let opt: &Option<OnyxEngine> = &guard;
                    match opt.as_ref() {
                        None => {
                            let _ = stream.write_all(b"error: engine not initialised\n");
                        }
                        Some(eng) => {
                            if parts.len() < 2 {
                                let _ = stream.write_all(b"error: usage: volume-usage <volume>\n");
                                let _ = stream.flush();
                                continue;
                            }
                            match eng.volume_usage(parts[1]) {
                                Ok(u) => {
                                    let payload =
                                        serde_json::to_string(&u).unwrap_or_else(|_| "{}".into());
                                    let _ = stream.write_all(payload.as_bytes());
                                    let _ = stream.write_all(b"\nok\n");
                                }
                                Err(e) => {
                                    let _ = stream.write_all(format!("error: {}\n", e).as_bytes());
                                }
                            }
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

                // ── chunklet online operator ops ─────────────────
                "chunklet-status-json" => {
                    let guard = engine.load();
                    let opt: &Option<OnyxEngine> = &guard;
                    match opt.as_ref().and_then(|eng| eng.chunklet_pool()) {
                        Some(pool) => match pool.metrics() {
                            Ok(m) => {
                                let snap = onyx_chunklet::ops::PoolSnapshot::from_metrics(&m);
                                let json =
                                    serde_json::to_string(&snap).unwrap_or_else(|_| "{}".into());
                                let _ = stream.write_all(json.as_bytes());
                                let _ = stream.write_all(b"\nok\n");
                            }
                            Err(e) => {
                                let _ = stream.write_all(format!("error: {}\n", e).as_bytes());
                            }
                        },
                        None => {
                            let _ = stream.write_all(b"error: chunklet backend not enabled\n");
                        }
                    }
                    let _ = stream.flush();
                }
                // rebuild/scrub hold the LD write lock for the whole op → spawn a
                // background worker and return a job id immediately; poll via
                // `chunklet-job`. Never block the handler thread on them.
                "chunklet-rebuild" | "chunklet-scrub" => {
                    require_engine!(engine, stream);
                    if parts.len() < 2 {
                        let _ = stream
                            .write_all(format!("error: usage: {} <ld_id>\n", parts[0]).as_bytes());
                        let _ = stream.flush();
                        continue;
                    }
                    let ld = parts[1];
                    let guard = engine.load();
                    let opt: &Option<OnyxEngine> = &guard;
                    let eng = opt.as_ref().unwrap();
                    match eng.chunklet_pool() {
                        Some(pool) => {
                            let res = if parts[0] == "chunklet-rebuild" {
                                crate::chunklet_ops::start_rebuild(&pool, ld)
                            } else {
                                crate::chunklet_ops::start_scrub(&pool, ld)
                            };
                            match res {
                                Ok(job_id) => {
                                    let _ = stream.write_all(format!("ok {}\n", job_id).as_bytes());
                                }
                                Err(e) => {
                                    let _ = stream.write_all(format!("error: {}\n", e).as_bytes());
                                }
                            }
                        }
                        None => {
                            let _ = stream.write_all(b"error: chunklet backend not enabled\n");
                        }
                    }
                    let _ = stream.flush();
                }
                // extend holds only the LD read lock (additive rows) → fast,
                // runs inline; returns the new LD capacity in bytes.
                "chunklet-extend" => {
                    require_engine!(engine, stream);
                    if parts.len() < 3 {
                        let _ = stream.write_all(
                            b"error: usage: chunklet-extend <lv3|meta> <additional_rows>\n",
                        );
                        let _ = stream.flush();
                        continue;
                    }
                    let role = parts[1];
                    let rows: u16 = match parts[2].parse() {
                        Ok(r) => r,
                        Err(_) => {
                            let _ = stream.write_all(b"error: invalid additional_rows\n");
                            let _ = stream.flush();
                            continue;
                        }
                    };
                    let guard = engine.load();
                    let opt: &Option<OnyxEngine> = &guard;
                    let eng = opt.as_ref().unwrap();
                    match eng.chunklet_extend(role, rows) {
                        Ok(new_cap) => {
                            let _ = stream.write_all(format!("ok {}\n", new_cap).as_bytes());
                        }
                        Err(e) => {
                            let _ = stream.write_all(format!("error: {}\n", e).as_bytes());
                        }
                    }
                    let _ = stream.flush();
                }
                "chunklet-job" => {
                    // Job state lives in a process-global registry, so this is
                    // serviceable without loading the engine. No arg = list all.
                    if parts.len() < 2 {
                        let jobs = crate::chunklet_ops::all_jobs();
                        let json = serde_json::to_string(&jobs).unwrap_or_else(|_| "[]".into());
                        let _ = stream.write_all(json.as_bytes());
                        let _ = stream.write_all(b"\nok\n");
                    } else {
                        match parts[1].parse::<u64>() {
                            Ok(id) => match crate::chunklet_ops::job_view(id) {
                                Some(v) => {
                                    let json =
                                        serde_json::to_string(&v).unwrap_or_else(|_| "{}".into());
                                    let _ = stream.write_all(json.as_bytes());
                                    let _ = stream.write_all(b"\nok\n");
                                }
                                None => {
                                    let _ = stream.write_all(b"error: no such job\n");
                                }
                            },
                            Err(_) => {
                                let _ = stream.write_all(b"error: invalid job id\n");
                            }
                        }
                    }
                    let _ = stream.flush();
                }

                // Pool-wide / PD-lifecycle background ops. All hold chunklet
                // locks for a while (rebalance/fsck take manifest_lock per
                // step; reintegrate/drain run rebuild_ld) → spawn a job and
                // return its id immediately, poll via `chunklet-job`.
                "chunklet-fsck"
                | "chunklet-rebalance"
                | "chunklet-reintegrate"
                | "chunklet-drain" => {
                    require_engine!(engine, stream);
                    let guard = engine.load();
                    let opt: &Option<OnyxEngine> = &guard;
                    let eng = opt.as_ref().unwrap();
                    match eng.chunklet_pool() {
                        Some(pool) => {
                            let res: Result<u64, _> = match parts[0] {
                                "chunklet-fsck" => crate::chunklet_ops::start_fsck(&pool),
                                "chunklet-rebalance" => {
                                    // optional: <target_skew_pct> <max_moves>
                                    let target = parts
                                        .get(1)
                                        .and_then(|s| s.parse::<f64>().ok())
                                        .unwrap_or(20.0);
                                    let max_moves = parts
                                        .get(2)
                                        .and_then(|s| s.parse::<usize>().ok())
                                        .unwrap_or(256);
                                    crate::chunklet_ops::start_rebalance(&pool, target, max_moves)
                                }
                                "chunklet-reintegrate" => {
                                    if parts.len() < 2 {
                                        let _ = stream.write_all(
                                            b"error: usage: chunklet-reintegrate <device_path>\n",
                                        );
                                        let _ = stream.flush();
                                        continue;
                                    }
                                    crate::chunklet_ops::start_reintegrate(&pool, parts[1])
                                }
                                _ /* chunklet-drain */ => {
                                    if parts.len() < 2 {
                                        let _ = stream
                                            .write_all(b"error: usage: chunklet-drain <pd_id>\n");
                                        let _ = stream.flush();
                                        continue;
                                    }
                                    crate::chunklet_ops::start_drain(&pool, parts[1])
                                }
                            };
                            match res {
                                Ok(job_id) => {
                                    let _ = stream.write_all(format!("ok {}\n", job_id).as_bytes());
                                }
                                Err(e) => {
                                    let _ = stream.write_all(format!("error: {}\n", e).as_bytes());
                                }
                            }
                        }
                        None => {
                            let _ = stream.write_all(b"error: chunklet backend not enabled\n");
                        }
                    }
                    let _ = stream.flush();
                }
                // Fast PD bookkeeping ops (single manifest commit each) → inline.
                "chunklet-retire-failed" | "chunklet-clear-failed" => {
                    require_engine!(engine, stream);
                    if parts.len() < 2 {
                        let _ = stream
                            .write_all(format!("error: usage: {} <pd_id>\n", parts[0]).as_bytes());
                        let _ = stream.flush();
                        continue;
                    }
                    let guard = engine.load();
                    let opt: &Option<OnyxEngine> = &guard;
                    let eng = opt.as_ref().unwrap();
                    match eng.chunklet_pool() {
                        Some(pool) => {
                            let res = onyx_chunklet::ops::parse_pd_id(parts[1]).and_then(|pd_id| {
                                if parts[0] == "chunklet-retire-failed" {
                                    pool.retire_failed_pd(pd_id)
                                } else {
                                    pool.clear_pd_failed(pd_id)
                                }
                            });
                            match res {
                                Ok(()) => {
                                    let _ = stream.write_all(b"ok\n");
                                }
                                Err(e) => {
                                    let _ = stream.write_all(format!("error: {}\n", e).as_bytes());
                                }
                            }
                        }
                        None => {
                            let _ = stream.write_all(b"error: chunklet backend not enabled\n");
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

/// Send a chunklet online-ops command (`chunklet-status-json` / `chunklet-scrub
/// <ld>` / `chunklet-rebuild <ld>` / `chunklet-job [id]`) and return the reply
/// payload lines with the bare `ok` terminator stripped (an `ok <id>` line is
/// kept so the caller can read a job id).
pub fn send_chunklet_command(socket_path: &Path, command: &str) -> OnyxResult<Vec<String>> {
    let lines = send_ipc_command(socket_path, command)?;
    Ok(lines.into_iter().filter(|l| l != "ok").collect())
}

/// Parse the `ok <value>` terminator from an IPC reply.
fn ok_suffix(lines: &[String]) -> Option<&str> {
    lines.iter().find_map(|l| l.strip_prefix("ok "))
}

pub fn send_snapshot_create(socket_path: &Path, volume: &str, name: &str) -> OnyxResult<u64> {
    let lines = send_ipc_command(socket_path, &format!("snapshot-create {} {}", volume, name))?;
    ok_suffix(&lines)
        .and_then(|s| s.trim().parse().ok())
        .ok_or_else(|| OnyxError::Config("snapshot-create: missing snapshot id in reply".into()))
}

pub fn send_snapshot_delete(socket_path: &Path, volume: &str, name: &str) -> OnyxResult<u64> {
    let lines = send_ipc_command(socket_path, &format!("snapshot-delete {} {}", volume, name))?;
    Ok(ok_suffix(&lines)
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0))
}

pub fn send_snapshot_list(socket_path: &Path, volume: Option<&str>) -> OnyxResult<Vec<String>> {
    let cmd = match volume {
        Some(v) => format!("snapshot-list {}", v),
        None => "snapshot-list".to_string(),
    };
    let lines = send_ipc_command(socket_path, &cmd)?;
    Ok(lines.into_iter().filter(|l| l != "ok").collect())
}

pub fn send_snapshot_clone(
    socket_path: &Path,
    volume: &str,
    name: &str,
    to: &str,
) -> OnyxResult<String> {
    let lines = send_ipc_command(
        socket_path,
        &format!("snapshot-clone {} {} {}", volume, name, to),
    )?;
    ok_suffix(&lines)
        .map(|s| s.trim().to_string())
        .ok_or_else(|| OnyxError::Config("snapshot-clone: missing new volume in reply".into()))
}

pub fn send_snapshot_restore(socket_path: &Path, volume: &str, name: &str) -> OnyxResult<()> {
    send_ipc_command(
        socket_path,
        &format!("snapshot-restore {} {}", volume, name),
    )?;
    Ok(())
}

/// Returns the `volume-usage` JSON payload line from a running engine.
pub fn send_volume_usage(socket_path: &Path, volume: &str) -> OnyxResult<String> {
    let lines = send_ipc_command(socket_path, &format!("volume-usage {}", volume))?;
    lines
        .into_iter()
        .find(|l| l != "ok" && l.starts_with('{'))
        .ok_or_else(|| OnyxError::Config("volume-usage: empty reply".into()))
}

pub fn send_mode_command(socket_path: &Path) -> OnyxResult<String> {
    let lines = send_ipc_command(socket_path, "mode")?;
    Ok(lines.into_iter().find(|l| l != "ok").unwrap_or_default())
}
