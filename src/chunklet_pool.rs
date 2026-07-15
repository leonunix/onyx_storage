//! onyx-side chunklet wiring: open/create the RAID Pool, map config geometry to
//! `LdSpec`, resolve per-role `LogicalDisk`s, and hand back a `ChunkletBackend`
//! for LV2/LV3/metadb.
//!
//! This is the only place onyx talks to chunklet's Pool lifecycle. Keeping it
//! in one module means the engine startup (step-0) and the `chunklet-init` CLI
//! share exactly one open/create/resolve path.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use onyx_chunklet::io::{
    IoBackendKind, IoClass as ChunkletIoClass, RawDevice as CkRawDevice, SchedulerConfig,
    UringPoolConfig,
};
use onyx_chunklet::ld::LogicalDisk;
use onyx_chunklet::ops;
use onyx_chunklet::pool::LdSpec;
use onyx_chunklet::types::LdId;
use onyx_chunklet::{Pool, PoolConfig};

use crate::config::{ChunkletConfig, ChunkletIoBackend, ChunkletLdGeom};
use crate::error::{OnyxError, OnyxResult};
use crate::io::block_backend::{ChunkletBackend, ChunkletIoScheduler, IoClass};

/// Which LV a resolved LD serves — selects the role's configured id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LdRoleSel {
    Lv3,
    Lv2,
    Meta,
}

impl ChunkletIoBackend {
    fn to_kind(self) -> IoBackendKind {
        match self {
            ChunkletIoBackend::Uring => IoBackendKind::Uring,
            ChunkletIoBackend::Sync => IoBackendKind::Sync,
        }
    }
}

fn require_devices(cfg: &ChunkletConfig) -> OnyxResult<()> {
    if cfg.devices.is_empty() {
        return Err(OnyxError::Config(
            "chunklet.enabled but chunklet.devices is empty".into(),
        ));
    }
    Ok(())
}

fn open_raw_devices(cfg: &ChunkletConfig) -> OnyxResult<Vec<CkRawDevice>> {
    require_devices(cfg)?;
    let mut raws = Vec::with_capacity(cfg.devices.len());
    for p in &cfg.devices {
        raws.push(CkRawDevice::open(p)?);
    }
    Ok(raws)
}

fn pd_write_scheduler_config(cfg: &ChunkletConfig) -> Option<SchedulerConfig> {
    (cfg.pd_write_max_active_blocks != 0).then(|| {
        SchedulerConfig::new(cfg.pd_write_max_active_blocks)
            .with_min_active_blocks(
                ChunkletIoClass::Foreground,
                cfg.pd_write_foreground_min_blocks,
            )
            .with_min_active_blocks(ChunkletIoClass::DrainData, cfg.pd_write_lv3_min_blocks)
            .with_min_active_blocks(ChunkletIoClass::DrainMeta, cfg.pd_write_meta_min_blocks)
            .with_min_active_blocks(
                ChunkletIoClass::Maintenance,
                cfg.pd_write_maintenance_min_blocks,
            )
    })
}

fn uring_pool_config(cfg: &ChunkletConfig) -> UringPoolConfig {
    UringPoolConfig {
        foreground_workers: cfg.pd_write_foreground_workers,
        background_workers: cfg.pd_write_background_workers,
        foreground_cpus: crate::affinity::role_cpu_set(crate::affinity::ThreadRole::BufferSync),
        background_cpus: crate::affinity::role_cpu_set(crate::affinity::ThreadRole::Lv3Batch),
    }
}

fn cpu_sets_disjoint(foreground: &[usize], background: &[usize]) -> bool {
    !foreground.is_empty()
        && !background.is_empty()
        && foreground.iter().all(|cpu| !background.contains(cpu))
}

fn validate_uring_execution_cpu_sets(
    cfg: &ChunkletConfig,
    uring: &UringPoolConfig,
    confine: bool,
) -> OnyxResult<bool> {
    if cfg.pd_write_foreground_workers == 0 || cfg.pd_write_background_workers == 0 {
        return Ok(false);
    }
    let disjoint = cpu_sets_disjoint(&uring.foreground_cpus, &uring.background_cpus);
    if confine && !disjoint {
        return Err(OnyxError::Config(format!(
            "chunklet persistent write execution requires disjoint confine CPU sets: foreground={:?}, background={:?}",
            uring.foreground_cpus, uring.background_cpus
        )));
    }
    Ok(!uring.foreground_cpus.is_empty() && !uring.background_cpus.is_empty() && !disjoint)
}

fn configure_pool_io_backend(pool: &Pool, cfg: &ChunkletConfig) -> OnyxResult<()> {
    let uring = uring_pool_config(cfg);
    if validate_uring_execution_cpu_sets(cfg, &uring, crate::affinity::is_confine_layout())? {
        tracing::warn!(
            foreground_cpus = ?uring.foreground_cpus,
            background_cpus = ?uring.background_cpus,
            "chunklet foreground/background execution CPU sets overlap; worker queues remain independent but CPU capacity is shared"
        );
    }
    match pd_write_scheduler_config(cfg) {
        Some(scheduler) => pool.set_scheduled_io_backend_with_uring_pool_config(
            cfg.io_backend.to_kind(),
            scheduler,
            uring,
        )?,
        None => pool.set_io_backend_with_uring_pool_config(cfg.io_backend.to_kind(), uring),
    }
    Ok(())
}

/// Open an existing pool (engine startup). Resolves the pool's PDs by on-disk
/// identity (`discover_pool_devices`, robust to `/dev/nvmeXnY` re-enumeration)
/// and, when `tolerant_open` is set, starts degraded if a PD is missing rather
/// than refusing to boot.
pub fn open_pool(cfg: &ChunkletConfig) -> OnyxResult<Arc<Pool>> {
    let paths = discover_pool_devices(cfg)?;
    let pool = if cfg.tolerant_open {
        open_pool_tolerant(&paths)?
    } else {
        let raws = open_raws_all(&paths)?;
        Pool::open(raws)?
    };
    configure_pool_io_backend(&pool, cfg)?;
    Ok(pool)
}

/// Resolve the set of raw-device paths that make up this pool.
///
/// With `device_discovery` on (default) and a `device_glob` set, every matching
/// device is probed for a chunklet pool superblock and the majority-`pool_id`
/// set is kept — membership follows the on-disk identity, not the path, so a
/// disk that returns under a new `/dev/nvmeXnY` name is still found. The static
/// `[chunklet].devices` list is always a seed candidate and the fallback when
/// discovery is off or nothing probes (e.g. a blank pre-`chunklet-init` disk
/// set).
pub fn discover_pool_devices(cfg: &ChunkletConfig) -> OnyxResult<Vec<PathBuf>> {
    let mut candidates: Vec<PathBuf> = cfg.devices.clone();
    if cfg.device_discovery {
        if let Some(glob) = &cfg.device_glob {
            for p in glob_dev(glob) {
                if !candidates.contains(&p) {
                    candidates.push(p);
                }
            }
        }
    }
    if candidates.is_empty() {
        return Err(OnyxError::Config(
            "chunklet: no candidate devices (empty [chunklet].devices and no device_glob match)"
                .into(),
        ));
    }
    if !cfg.device_discovery {
        return Ok(candidates);
    }
    // Content-addressed: probe each candidate, tally pool_ids, keep the majority.
    use std::collections::BTreeMap;
    let mut by_pool: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();
    for p in &candidates {
        let Ok(raw) = CkRawDevice::open(p) else {
            continue;
        };
        if let Ok(Some(pool_id)) = ops::probe_pool_id(&raw) {
            by_pool
                .entry(pool_id.to_string())
                .or_default()
                .push(p.clone());
        }
    }
    match by_pool.into_values().max_by_key(|v| v.len()) {
        Some(v) if !v.is_empty() => Ok(v),
        // Nothing carried a superblock (fresh disks): fall back to the static
        // list so a first open before init / a hand-listed pool still works.
        _ => Ok(candidates),
    }
}

/// Scan `device_glob` for physically-returned pool disks: devices whose on-disk
/// superblock carries `pool_id` and a pd_id in `failed` (a Failed tombstone).
/// The watchdog's auto-reintegrate uses this to spot a disk that came back under
/// a new `/dev/nvmeXnY` name. Returns `(path, old_pd_id)` pairs; devices that
/// can't be opened/probed are silently skipped.
pub fn find_returned_pool_disks(
    device_glob: &str,
    pool_id: onyx_chunklet::types::PoolId,
    failed: &std::collections::HashSet<onyx_chunklet::types::PdId>,
) -> Vec<(PathBuf, onyx_chunklet::types::PdId)> {
    let mut out = Vec::new();
    for p in glob_dev(device_glob) {
        let Ok(raw) = CkRawDevice::open(&p) else {
            continue;
        };
        if let Ok(Some((pid, pd_id))) = ops::probe_pool_and_pd_id(&raw) {
            if pid == pool_id && failed.contains(&pd_id) {
                out.push((p, pd_id));
            }
        }
    }
    out
}

fn open_raws_all(paths: &[PathBuf]) -> OnyxResult<Vec<CkRawDevice>> {
    let mut raws = Vec::with_capacity(paths.len());
    for p in paths {
        raws.push(CkRawDevice::open(p)?);
    }
    Ok(raws)
}

/// Strict open first (a complete pool is fully authoritative and also reverse-
/// reconciles stale capacity at open); on failure — typically a missing PD —
/// fall back to a degraded `open_with_missing`. `open_available_pool_devices`
/// opens only the reachable majority-pool devices, so a pulled disk simply
/// isn't in the set.
fn open_pool_tolerant(paths: &[PathBuf]) -> OnyxResult<Arc<Pool>> {
    let (raws, _probes, _pool_id) = ops::open_available_pool_devices(paths)?;
    match Pool::open(raws) {
        Ok(pool) => Ok(pool),
        Err(_strict_err) => {
            // The first raws were consumed by the failed strict open; re-probe
            // for the degraded retry (flocks were released on drop).
            let (raws2, _p, _id) = ops::open_available_pool_devices(paths)?;
            Ok(Pool::open_with_missing(raws2)?)
        }
    }
}

/// Minimal device glob: a directory plus a single-level filename pattern with
/// `*` wildcards (e.g. `/dev/nvme*n*`). Not a general glob — just enough for
/// block-device discovery. A missing directory yields no matches (not fatal).
fn glob_dev(glob: &str) -> Vec<PathBuf> {
    let path = Path::new(glob);
    let dir = path.parent().unwrap_or_else(|| Path::new("/"));
    let Some(pattern) = path.file_name().and_then(|s| s.to_str()) else {
        return Vec::new();
    };
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out: Vec<PathBuf> = entries
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|name| wildcard_match(pattern, name))
                .unwrap_or(false)
        })
        .map(|e| e.path())
        .collect();
    out.sort();
    out
}

/// Classic `*`-only wildcard match (no `?`, no char classes), two-pointer with
/// backtracking.
fn wildcard_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star, mut mark) = (None::<usize>, 0usize);
    while ti < t.len() {
        if pi < p.len() && p[pi] == t[ti] {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

/// Create a fresh pool over blank devices, then create the LV3/LV2/meta LDs from
/// the configured geometry. Returns the pool plus the three LD ids so the caller
/// (`chunklet-init`) can print them for the config. **Wipes the devices' pool
/// state** — only call on a confirmed-empty disk set.
pub fn init_pool(cfg: &ChunkletConfig) -> OnyxResult<(Arc<Pool>, LdId, LdId, LdId)> {
    let raws = open_raw_devices(cfg)?;
    let pool = Pool::create(
        raws,
        PoolConfig {
            spare_pct: cfg.spare_pct,
            io_backend: cfg.io_backend.to_kind(),
        },
    )?;
    configure_pool_io_backend(&pool, cfg)?;
    let lv3 = pool.create_ld(geom_to_spec(&cfg.lv3)?)?;
    let lv2 = pool.create_ld(geom_to_spec(&cfg.lv2)?)?;
    let meta = pool.create_ld(geom_to_spec(&cfg.meta)?)?;
    Ok((pool, lv3, lv2, meta))
}

/// Resolve the `LogicalDisk` serving `role` from an already-open pool. Prefers
/// the role's configured id; if unset and the pool holds exactly one LD, uses
/// that (Phase-1 single-LD convenience).
pub fn resolve_ld(
    pool: &Arc<Pool>,
    cfg: &ChunkletConfig,
    role: LdRoleSel,
) -> OnyxResult<Arc<dyn LogicalDisk>> {
    let configured = match role {
        LdRoleSel::Lv3 => &cfg.lv3_ld_id,
        LdRoleSel::Lv2 => &cfg.lv2_ld_id,
        LdRoleSel::Meta => &cfg.meta_ld_id,
    };
    let id = match configured {
        Some(s) => onyx_chunklet::ops::parse_ld_id(s)?,
        None => {
            let lds = pool.list_lds();
            if lds.len() == 1 {
                lds[0].id
            } else {
                return Err(OnyxError::Config(format!(
                    "chunklet {role:?} LD id unset and pool has {} LDs (expected 1); \
                     run chunklet-init and set the id in [chunklet]",
                    lds.len()
                )));
            }
        }
    };
    Ok(pool.open_ld(id)?)
}

/// Open the pool and build a `ChunkletBackend` for the given role in one step —
/// the single-role convenience (tests, `chunklet-init` verification). Returns
/// the pool (keep it alive for the process lifetime) and the backend.
///
/// The engine startup uses [`role_backend_from_pool`] instead so LV3 and LV2
/// share ONE `Pool::open` (a second in-process open over the same PDs would be
/// a separate in-memory pool state — two writers to one superblock).
pub fn open_role_backend(
    cfg: &ChunkletConfig,
    role: LdRoleSel,
) -> OnyxResult<(Arc<Pool>, Arc<ChunkletBackend>)> {
    let pool = open_pool(cfg)?;
    let backend = role_backend_from_pool(&pool, cfg, role)?;
    Ok((pool, backend))
}

/// Build a `ChunkletBackend` for `role` from an already-open pool. The engine
/// opens the pool ONCE (step-0) and derives the LV3 + LV2 backends from it, so
/// both roles' IO funnels through the same `Pool` (and its single manifest /
/// superblock writer). The returned backend keeps the pool alive (Phase-4 ops
/// surface reaches it through `ChunkletBackend::pool`).
pub fn role_backend_from_pool(
    pool: &Arc<Pool>,
    cfg: &ChunkletConfig,
    role: LdRoleSel,
) -> OnyxResult<Arc<ChunkletBackend>> {
    let ld = resolve_ld(pool, cfg, role)?;
    let class = match role {
        LdRoleSel::Lv2 => IoClass::Foreground,
        LdRoleSel::Lv3 => IoClass::Lv3,
        LdRoleSel::Meta => IoClass::Meta,
    };
    Ok(Arc::new(ChunkletBackend::with_pool_and_class(
        ld,
        pool.clone(),
        class,
    )))
}

/// Scheduled engine path. Every role derived from one Pool must receive the
/// same scheduler Arc; the role selects its protected write class.
pub(crate) fn scheduled_role_backend_from_pool(
    pool: &Arc<Pool>,
    cfg: &ChunkletConfig,
    role: LdRoleSel,
    scheduler: &Arc<ChunkletIoScheduler>,
) -> OnyxResult<Arc<ChunkletBackend>> {
    let ld = resolve_ld(pool, cfg, role)?;
    let class = match role {
        LdRoleSel::Lv2 => IoClass::Foreground,
        LdRoleSel::Lv3 => IoClass::Lv3,
        LdRoleSel::Meta => IoClass::Meta,
    };
    Ok(Arc::new(ChunkletBackend::with_pool_and_scheduler(
        ld,
        pool.clone(),
        class,
        scheduler.clone(),
    )))
}

/// Translate `strip_kib` into chunklet's `strip_size_log2`. 0 → 0 (one 4 KiB
/// block). Non-zero must be a power-of-two KiB whose byte size is ≥ 4 KiB.
fn strip_size_log2(strip_kib: u32) -> OnyxResult<u8> {
    if strip_kib == 0 {
        return Ok(0);
    }
    let bytes = (strip_kib as u64) * 1024;
    if !bytes.is_power_of_two() || bytes < 4096 {
        return Err(OnyxError::Config(format!(
            "chunklet strip_kib={strip_kib} must be a power of two ≥ 4 (KiB)"
        )));
    }
    Ok(bytes.trailing_zeros() as u8)
}

/// Map a config LD geometry onto the matching chunklet `LdSpec` constructor.
pub fn geom_to_spec(geom: &ChunkletLdGeom) -> OnyxResult<LdSpec> {
    let log2 = strip_size_log2(geom.strip_kib)?;
    let spec = match geom.raid.to_ascii_lowercase().as_str() {
        "raid6" => LdSpec::raid6(geom.set_size, geom.row_size, geom.num_rows, log2),
        "raid5" => LdSpec::raid5(geom.set_size, geom.row_size, geom.num_rows, log2),
        "raid10" | "mirror" => LdSpec::mirror(geom.set_size, geom.row_size, geom.num_rows, log2),
        // raid0: the stripe width is the set_size slot; rows chain capacity.
        "raid0" => LdSpec::raid0(geom.set_size as u16, geom.num_rows, log2),
        "plain" => LdSpec::plain(geom.num_rows),
        other => {
            return Err(OnyxError::Config(format!(
                "chunklet LD raid='{other}' unknown (raid6|raid5|raid10|raid0|plain)"
            )))
        }
    };
    Ok(spec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::block_backend::BlockBackend;

    fn geom(raid: &str, set: u8, row: u16, rows: u16, kib: u32) -> ChunkletLdGeom {
        ChunkletLdGeom {
            raid: raid.to_string(),
            set_size: set,
            row_size: row,
            num_rows: rows,
            strip_kib: kib,
        }
    }

    #[test]
    fn strip_log2_maps_powers_of_two() {
        assert_eq!(strip_size_log2(0).unwrap(), 0);
        assert_eq!(strip_size_log2(4).unwrap(), 12); // 4 KiB
        assert_eq!(strip_size_log2(256).unwrap(), 18); // 256 KiB
        assert!(strip_size_log2(3).is_err()); // not power of two
    }

    #[test]
    fn geom_to_spec_covers_levels() {
        assert!(geom_to_spec(&geom("raid6", 6, 1, 1, 256)).is_ok());
        assert!(geom_to_spec(&geom("raid10", 2, 2, 1, 0)).is_ok());
        assert!(geom_to_spec(&geom("plain", 0, 1, 3, 0)).is_ok());
        assert!(geom_to_spec(&geom("bogus", 1, 1, 1, 0)).is_err());
    }

    #[test]
    fn confine_requires_disjoint_execution_cpu_sets() {
        let cfg = ChunkletConfig {
            enabled: true,
            pd_write_foreground_workers: 2,
            pd_write_background_workers: 2,
            ..Default::default()
        };
        let overlapping = UringPoolConfig {
            foreground_workers: 2,
            background_workers: 2,
            foreground_cpus: vec![0, 2, 4],
            background_cpus: vec![4, 6, 8],
        };
        assert_eq!(
            validate_uring_execution_cpu_sets(&cfg, &overlapping, false).unwrap(),
            true
        );
        assert!(validate_uring_execution_cpu_sets(&cfg, &overlapping, true).is_err());

        let disjoint = UringPoolConfig {
            background_cpus: vec![1, 3, 5],
            ..overlapping
        };
        assert_eq!(
            validate_uring_execution_cpu_sets(&cfg, &disjoint, true).unwrap(),
            false
        );
    }

    fn init_cfg(dir: &std::path::Path, n: usize) -> ChunkletConfig {
        let devices: Vec<_> = (0..n).map(|i| dir.join(format!("pd{i}"))).collect();
        for p in &devices {
            CkRawDevice::open_or_create(p, 4 << 30).unwrap();
        }
        ChunkletConfig {
            enabled: true,
            devices,
            io_backend: ChunkletIoBackend::Sync,
            spare_pct: 0,
            ..Default::default()
        }
    }

    #[test]
    fn wildcard_match_basics() {
        assert!(wildcard_match("nvme*n*", "nvme0n1"));
        assert!(wildcard_match("nvme*n*", "nvme12n1"));
        assert!(wildcard_match("pd*", "pd7"));
        assert!(wildcard_match("*", "anything"));
        assert!(wildcard_match("pd0", "pd0"));
        assert!(!wildcard_match("pd*", "sda"));
        assert!(!wildcard_match("nvme*n*", "nvme0")); // needs the trailing n<x>
    }

    /// Content-addressed discovery finds the pool's PDs by superblock pool_id
    /// even with `devices` empty — only the glob points at them (the /dev rename
    /// scenario: config paths gone, disks found by identity).
    #[test]
    fn discover_finds_pool_by_id_via_glob() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = init_cfg(dir.path(), 8);
        let (pool, ..) = init_pool(&cfg).unwrap();
        drop(pool);

        // Simulate re-enumeration: forget the configured paths, discover by glob.
        let discover_cfg = ChunkletConfig {
            devices: Vec::new(),
            device_discovery: true,
            device_glob: Some(format!("{}/pd*", dir.path().display())),
            ..cfg.clone()
        };
        let found = discover_pool_devices(&discover_cfg).unwrap();
        assert_eq!(found.len(), 8, "all 8 PDs discovered by pool_id");

        let pool2 = open_pool(&discover_cfg).unwrap();
        assert_eq!(pool2.pd_count(), 8);
    }

    /// The auto-reintegrate detector finds exactly the Failed tombstone's device
    /// (by pool_id + pd_id), and nothing else in the glob.
    #[test]
    fn find_returned_pool_disks_matches_failed_tombstone() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = init_cfg(dir.path(), 8);
        let (pool, ..) = init_pool(&cfg).unwrap();
        let victim = pool.list_pds()[3].pd_id;
        pool.mark_pd_failed(victim).unwrap();
        let failed: std::collections::HashSet<_> = pool.failed_pds().into_iter().collect();
        assert!(failed.contains(&victim));

        let glob = format!("{}/pd*", dir.path().display());
        let found = find_returned_pool_disks(&glob, pool.id(), &failed);
        assert_eq!(found.len(), 1, "only the failed tombstone's device matches");
        assert_eq!(found[0].1, victim, "detector recovers the old pd_id");

        // A healthy pool (no tombstones) matches nothing.
        pool.clear_pd_failed(victim).unwrap();
        let none = find_returned_pool_disks(&glob, pool.id(), &std::collections::HashSet::new());
        assert!(none.is_empty(), "no Failed tombstones → no returned disks");
    }

    /// Tolerant open starts the engine degraded when one PD is absent at boot.
    #[test]
    fn tolerant_open_starts_degraded_with_missing_pd() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = init_cfg(dir.path(), 8);
        let (pool, ..) = init_pool(&cfg).unwrap();
        drop(pool);

        // Pull one disk before boot.
        std::fs::remove_file(dir.path().join("pd3")).unwrap();

        let open_cfg = ChunkletConfig {
            devices: Vec::new(),
            device_discovery: true,
            device_glob: Some(format!("{}/pd*", dir.path().display())),
            tolerant_open: true,
            ..cfg.clone()
        };
        let pool2 = open_pool(&open_cfg).unwrap();
        assert_eq!(pool2.pd_count(), 7, "degraded open with 7 of 8 PDs");

        // With tolerant_open off, the same missing PD refuses to boot.
        let strict_cfg = ChunkletConfig {
            tolerant_open: false,
            ..open_cfg.clone()
        };
        drop(pool2);
        assert!(
            open_pool(&strict_cfg).is_err(),
            "strict open must fail with a missing PD"
        );
    }

    /// init_pool over sparse files creates 3 LDs and reopen resolves LV3.
    #[test]
    fn init_then_resolve_lv3() {
        let dir = tempfile::tempdir().unwrap();
        let devices: Vec<_> = (0..8).map(|i| dir.path().join(format!("pd{i}"))).collect();
        for p in &devices {
            // chunklet RawDevice::open requires the file to exist & be sized.
            CkRawDevice::open_or_create(p, 4 << 30).unwrap();
        }
        let mut cfg = ChunkletConfig {
            enabled: true,
            devices: devices.clone(),
            io_backend: ChunkletIoBackend::Sync,
            pd_write_max_active_blocks: 64,
            pd_write_foreground_min_blocks: 16,
            pd_write_lv3_min_blocks: 24,
            pd_write_meta_min_blocks: 16,
            pd_write_maintenance_min_blocks: 8,
            spare_pct: 0,
            ..Default::default()
        };
        let (pool, lv3, _lv2, _meta) = init_pool(&cfg).unwrap();

        let status = crate::metrics::EngineStatusSnapshot {
            chunklet_pd_io_scheduler: Some(pool.io_scheduler_snapshot().unwrap().into()),
            ..Default::default()
        };
        let json = serde_json::to_value(&status).unwrap();
        let pds = json["chunklet_pd_io_scheduler"]["pds"].as_array().unwrap();
        assert_eq!(pds.len(), devices.len());
        assert_eq!(pds[0]["max_active_blocks"], 64);
        assert_eq!(pds[0]["total_active_blocks"], 0);
        assert_eq!(pds[0]["total_queued_blocks"], 0);
        let classes = pds[0]["classes"].as_array().unwrap();
        assert_eq!(classes.len(), ChunkletIoClass::ALL.len());
        assert_eq!(classes[0]["class"], "foreground");
        assert_eq!(classes[0]["configured_min_blocks"], 16);
        assert_eq!(classes[0]["completed_blocks"], 0);
        assert_eq!(classes[0]["service_ns"], 0);
        assert_eq!(classes[1]["class"], "drain_data");
        assert_eq!(classes[1]["configured_min_blocks"], 24);
        assert_eq!(classes[2]["class"], "drain_meta");
        assert_eq!(classes[2]["configured_min_blocks"], 16);
        assert_eq!(classes[3]["class"], "maintenance");
        assert_eq!(classes[3]["configured_min_blocks"], 8);
        assert!(status
            .render_text()
            .contains("chunklet_pd_write_scheduler: pds=8 active_blocks=0 queued_blocks=0"));

        pool.set_io_backend(IoBackendKind::Sync);
        assert!(pool.io_scheduler_snapshot().is_none());
        let execution_only = ChunkletConfig {
            io_backend: ChunkletIoBackend::Uring,
            pd_write_foreground_workers: 1,
            pd_write_background_workers: 1,
            pd_write_max_active_blocks: 0,
            pd_write_foreground_min_blocks: 0,
            pd_write_lv3_min_blocks: 0,
            pd_write_meta_min_blocks: 0,
            pd_write_maintenance_min_blocks: 0,
            ..cfg.clone()
        };
        configure_pool_io_backend(&pool, &execution_only).unwrap();
        assert!(pool.io_scheduler_snapshot().is_none());
        if let Some(execution) = pool.io_execution_snapshot() {
            assert!(execution.enabled);
            assert_eq!(execution.foreground_workers, 1);
            assert_eq!(execution.background_workers, 1);
            assert_eq!(execution.classes.len(), ChunkletIoClass::ALL.len());
        }
        configure_pool_io_backend(&pool, &cfg).unwrap();
        assert!(pool.io_execution_snapshot().is_none());
        drop(pool);

        cfg.io_backend = ChunkletIoBackend::Uring;
        cfg.pd_write_foreground_workers = 2;
        cfg.pd_write_background_workers = 2;
        cfg.lv3_ld_id = Some(lv3.to_string());
        let (pool2, backend) = open_role_backend(&cfg, LdRoleSel::Lv3).unwrap();
        assert!(backend.size() > 0);
        // Round-trip a block through the resolved RAID6 LV3 LD.
        let payload = vec![0x5au8; 64 << 10];
        backend.write_at(&payload, 0).unwrap();
        backend.flush().unwrap();
        let scheduler = pool2.io_scheduler_snapshot().unwrap();
        let lv3_admissions: u64 = scheduler
            .pds
            .iter()
            .flat_map(|pd| &pd.classes)
            .filter(|class| class.class == ChunkletIoClass::DrainData)
            .map(|class| class.admission_events)
            .sum();
        let lv3_admitted_blocks: u64 = scheduler
            .pds
            .iter()
            .flat_map(|pd| &pd.classes)
            .filter(|class| class.class == ChunkletIoClass::DrainData)
            .map(|class| class.admitted_blocks)
            .sum();
        let lv3_completed_blocks: u64 = scheduler
            .pds
            .iter()
            .flat_map(|pd| &pd.classes)
            .filter(|class| class.class == ChunkletIoClass::DrainData)
            .map(|class| class.completed_blocks)
            .sum();
        assert!(
            lv3_admissions > 0,
            "LV3 writes must retain their nested DrainData class while the legacy Onyx scheduler is off"
        );
        assert!(lv3_admitted_blocks > 0);
        assert_eq!(lv3_completed_blocks, lv3_admitted_blocks);
        if let Some(execution) = pool2.io_execution_snapshot() {
            assert!(execution.enabled);
            assert_eq!(execution.foreground_workers, 2);
            assert_eq!(execution.background_workers, 2);
            let drain_data = execution
                .classes
                .iter()
                .find(|class| class.class == ChunkletIoClass::DrainData)
                .unwrap();
            assert!(drain_data.batches > 0);
            assert!(drain_data.groups > 0);
            assert!(drain_data.ops > 0);

            let status = crate::metrics::EngineStatusSnapshot {
                chunklet_io_execution: Some(execution.into()),
                ..Default::default()
            };
            let json = serde_json::to_value(status).unwrap();
            assert_eq!(json["chunklet_io_execution"]["foreground_workers"], 2);
            assert_eq!(json["chunklet_io_execution"]["background_workers"], 2);
            assert_eq!(
                json["chunklet_io_execution"]["classes"]
                    .as_array()
                    .unwrap()
                    .len(),
                ChunkletIoClass::ALL.len()
            );
        }
        let mut got = vec![0u8; payload.len()];
        backend.read_at(&mut got, 0).unwrap();
        assert_eq!(got, payload);
        drop(pool2);
    }
}
