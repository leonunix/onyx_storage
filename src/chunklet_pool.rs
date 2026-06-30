//! onyx-side chunklet wiring: open/create the RAID Pool, map config geometry to
//! `LdSpec`, resolve per-role `LogicalDisk`s, and hand back a `ChunkletBackend`
//! for LV2/LV3/metadb.
//!
//! This is the only place onyx talks to chunklet's Pool lifecycle. Keeping it
//! in one module means the engine startup (step-0) and the `chunklet-init` CLI
//! share exactly one open/create/resolve path.

use std::sync::Arc;

use onyx_chunklet::io::{IoBackendKind, RawDevice as CkRawDevice};
use onyx_chunklet::ld::LogicalDisk;
use onyx_chunklet::pool::LdSpec;
use onyx_chunklet::types::LdId;
use onyx_chunklet::{Pool, PoolConfig};

use crate::config::{ChunkletConfig, ChunkletIoBackend, ChunkletLdGeom};
use crate::error::{OnyxError, OnyxResult};
use crate::io::block_backend::ChunkletBackend;

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

/// Open an existing pool (engine startup). All PDs must already carry a pool
/// superblock from a prior `chunklet-init`.
pub fn open_pool(cfg: &ChunkletConfig) -> OnyxResult<Arc<Pool>> {
    let raws = open_raw_devices(cfg)?;
    let pool = Pool::open(raws)?;
    pool.set_io_backend(cfg.io_backend.to_kind());
    Ok(pool)
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
    Ok(Arc::new(ChunkletBackend::with_pool(ld, pool.clone())))
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
            spare_pct: 0,
            ..Default::default()
        };
        let (pool, lv3, _lv2, _meta) = init_pool(&cfg).unwrap();
        drop(pool);

        cfg.lv3_ld_id = Some(lv3.to_string());
        let (pool2, backend) = open_role_backend(&cfg, LdRoleSel::Lv3).unwrap();
        assert!(backend.size() > 0);
        // Round-trip a block through the resolved RAID6 LV3 LD.
        let payload = vec![0x5au8; 64 << 10];
        backend.write_at(&payload, 0).unwrap();
        backend.flush().unwrap();
        let mut got = vec![0u8; payload.len()];
        backend.read_at(&mut got, 0).unwrap();
        assert_eq!(got, payload);
        drop(pool2);
    }
}
