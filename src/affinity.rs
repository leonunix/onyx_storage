use std::sync::OnceLock;

use crate::config::ThreadingConfig;

#[derive(Clone, Copy, Debug)]
pub enum ThreadRole {
    Ublk,
    ReadPool,
    BufferSync,
    FlusherCoalesce,
    FlusherDedup,
    FlusherCompress,
    FlusherWriter,
    FlusherCleanup,
    MetadbCheckpoint,
    Background,
}

#[derive(Clone, Debug, Default)]
struct AffinityLayout {
    ublk: CpuSet,
    read_pool: CpuSet,
    buffer_sync: CpuSet,
    flusher_coalesce: CpuSet,
    flusher_dedup: CpuSet,
    flusher_compress: CpuSet,
    flusher_writer: CpuSet,
    flusher_cleanup: CpuSet,
    metadb_checkpoint: CpuSet,
    background: CpuSet,
}

#[derive(Clone, Debug, Default)]
struct CpuSet {
    cpus: Vec<usize>,
}

static LAYOUT: OnceLock<Option<AffinityLayout>> = OnceLock::new();

pub fn init(config: &ThreadingConfig) {
    let _ = LAYOUT.set(AffinityLayout::from_config(config));
    if config.enabled {
        onyx_metadb::affinity::configure(onyx_metadb::affinity::AffinityConfig {
            wal_cpus: config.metadb_wal_cpus.clone(),
            l2p_apply_cpus: config.metadb_l2p_apply_cpus.clone(),
            refcount_apply_cpus: config.metadb_refcount_apply_cpus.clone(),
            dedup_apply_cpus: config.metadb_dedup_apply_cpus.clone(),
            // Refcount drainer CPU set is currently inherited from the
            // OS — onyx ThreadingConfig doesn't surface a knob for it
            // yet. The drainer is default-off, so this string only
            // matters once the flag flips on.
            refcount_drainer_cpus: String::new(),
        });
    }
}

pub fn bind_current(role: ThreadRole, ordinal: usize) {
    let Some(Some(layout)) = LAYOUT.get() else {
        return;
    };
    let Some(cpu) = layout.cpus_for(role).pick(ordinal) else {
        return;
    };
    if let Err(err) = set_current_cpu(cpu) {
        tracing::warn!(
            ?role,
            cpu,
            error = %err,
            "failed to set thread CPU affinity"
        );
    }
}

impl AffinityLayout {
    fn from_config(config: &ThreadingConfig) -> Option<Self> {
        if !config.enabled {
            return None;
        }
        Some(Self {
            ublk: CpuSet::parse(&config.ublk_cpus),
            read_pool: CpuSet::parse(&config.read_pool_cpus),
            buffer_sync: CpuSet::parse(&config.buffer_sync_cpus),
            flusher_coalesce: CpuSet::parse(&config.flusher_coalesce_cpus),
            flusher_dedup: CpuSet::parse(&config.flusher_dedup_cpus),
            flusher_compress: CpuSet::parse(&config.flusher_compress_cpus),
            flusher_writer: CpuSet::parse(&config.flusher_writer_cpus),
            flusher_cleanup: CpuSet::parse(&config.flusher_cleanup_cpus),
            metadb_checkpoint: CpuSet::parse(&config.metadb_checkpoint_cpus),
            background: CpuSet::parse(&config.background_cpus),
        })
    }

    fn cpus_for(&self, role: ThreadRole) -> &CpuSet {
        match role {
            ThreadRole::Ublk => &self.ublk,
            ThreadRole::ReadPool => &self.read_pool,
            ThreadRole::BufferSync => &self.buffer_sync,
            ThreadRole::FlusherCoalesce => &self.flusher_coalesce,
            ThreadRole::FlusherDedup => &self.flusher_dedup,
            ThreadRole::FlusherCompress => &self.flusher_compress,
            ThreadRole::FlusherWriter => &self.flusher_writer,
            ThreadRole::FlusherCleanup => &self.flusher_cleanup,
            ThreadRole::MetadbCheckpoint => &self.metadb_checkpoint,
            ThreadRole::Background => &self.background,
        }
    }
}

impl CpuSet {
    fn parse(spec: &str) -> Self {
        let mut cpus = Vec::new();
        for part in spec.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            if let Some((start, end)) = part.split_once('-') {
                let Ok(start) = start.trim().parse::<usize>() else {
                    tracing::warn!(spec, part, "ignoring invalid CPU range start");
                    continue;
                };
                let Ok(end) = end.trim().parse::<usize>() else {
                    tracing::warn!(spec, part, "ignoring invalid CPU range end");
                    continue;
                };
                if start > end {
                    tracing::warn!(spec, part, "ignoring descending CPU range");
                    continue;
                }
                cpus.extend(start..=end);
            } else if let Ok(cpu) = part.parse::<usize>() {
                cpus.push(cpu);
            } else {
                tracing::warn!(spec, part, "ignoring invalid CPU entry");
            }
        }
        cpus.sort_unstable();
        cpus.dedup();
        Self { cpus }
    }

    fn pick(&self, ordinal: usize) -> Option<usize> {
        if self.cpus.is_empty() {
            None
        } else {
            Some(self.cpus[ordinal % self.cpus.len()])
        }
    }
}

#[cfg(target_os = "linux")]
fn set_current_cpu(cpu: usize) -> std::io::Result<()> {
    // Keep the implementation local and tiny: CPU_SETSIZE is 1024 in glibc,
    // which is plenty for the machines this profile targets.
    const CPU_SETSIZE: usize = 1024;
    const BITS_PER_WORD: usize = 8 * std::mem::size_of::<libc::c_ulong>();
    let mut set = [0 as libc::c_ulong; CPU_SETSIZE / BITS_PER_WORD];
    if cpu >= CPU_SETSIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("cpu {cpu} >= CPU_SETSIZE {CPU_SETSIZE}"),
        ));
    }
    set[cpu / BITS_PER_WORD] |= (1 as libc::c_ulong) << (cpu % BITS_PER_WORD);
    let rc = unsafe {
        libc::sched_setaffinity(
            0,
            std::mem::size_of_val(&set),
            set.as_ptr().cast::<libc::cpu_set_t>(),
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn set_current_cpu(_cpu: usize) -> std::io::Result<()> {
    Ok(())
}
