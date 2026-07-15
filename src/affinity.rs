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
    /// Chunklet LV3 batch executors. RAID5/6 parity planning and encoding are
    /// streaming CPU/memory work, so partition mode spreads these across pods.
    Lv3Batch,
    FlusherWriter,
    FlusherCleanup,
    /// Per-volume commit worker (`hash(vol_id) % NUM_COMMIT_WORKERS`).
    /// Each worker calls `tx.commit_with_outcomes`, so cache-line
    /// traffic to metadb's L2P/RC apply lanes dominates the cost —
    /// pinning here to the same NUMA node as `metadb_l2p_apply` cuts
    /// the per-commit cross-socket bounce of ~0.5–1 ms that the
    /// previous "borrow flusher_writer_cpus" placement paid on v4.
    CommitWorker,
    /// Post-commit cleanup workers (fixed pool, `hash`-routed like
    /// CommitWorker). Distinct from `FlusherCleanup` so NUMA partition can
    /// home them with the commit workers; legacy `[threading]` configs fall
    /// back to `flusher_cleanup_cpus`.
    FlusherPostCommit,
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
    commit_worker: CpuSet,
    metadb_checkpoint: CpuSet,
    background: CpuSet,
}

#[derive(Clone, Debug, Default)]
struct CpuSet {
    cpus: Vec<usize>,
}

enum LayoutKind {
    /// Legacy `[threading]` per-role single-CPU pinning.
    PerRole(AffinityLayout),
    /// `[numa] mode = "confine"`: every role binds to the same CPU *set*
    /// (the home node minus reserved cores), keeping scheduler freedom
    /// inside the node — the in-engine equivalent of
    /// `numactl --cpunodebind`, but it also covers libublk's per-queue
    /// threads because the per-thread `bind_current` runs after libublk's
    /// own affinity call and overrides it.
    ConfineSets {
        foreground: Vec<usize>,
        background: Vec<usize>,
    },
    /// `[numa] mode = "partition"`: sharded roles bind to their shard's pod
    /// (one pod per data node), singletons bind to the home pod. Threads
    /// also set their own memory policy to prefer the pod's node so Tier A
    /// per-shard allocations first-touch locally.
    Partition(PartitionTopo),
}

/// One pod = one NUMA data node's engine CPU pool.
#[derive(Clone, Debug)]
pub struct PodCpus {
    pub node: usize,
    pub cpus: Vec<usize>,
}

/// Everything needed to map `(role, ordinal)` → pod under partition mode.
/// Ordinal conventions (must match the spawn sites):
/// - per-shard roles pass `shard_idx`
/// - FlusherDedup/FlusherCompress pass `shard_idx * workers + worker_idx`
/// - Ublk passes `qid * queue_workers + worker_idx` (queue daemon threads
///   pass `qid * queue_workers`)
/// - ReadPool passes `worker_idx`
#[derive(Clone, Debug)]
pub struct PartitionTopo {
    pub pods: Vec<PodCpus>,
    pub home_pod: usize,
    pub shards: usize,
    pub dedup_workers: usize,
    pub compress_workers: usize,
    pub queue_workers: usize,
    pub nr_queues: usize,
    pub read_pool_workers: usize,
}

impl PartitionTopo {
    /// Compute-offload model (2026-06-11, third partition iteration — see
    /// docs/numa-aware-design.md §field-notes): the front-end (ublk), the
    /// metadata path (LSN-ordered apply chain), and the shared md devices
    /// form ONE latency domain that cannot span sockets — every variant
    /// that split them capped the flush drain at ~13-16k remap/s
    /// (cross-socket hop per LSN ≈ 70µs ⇒ ~14k ceiling; md fsync 64µs →
    /// 334-540µs from the far socket; far-socket reads 5-8ms vs 1.65ms).
    /// The throughput-shaped compute stages (dedup hash/verify, compress)
    /// work in 128KB units that amortize one cross-socket hop, and they are
    /// exactly what crowds the home socket under confine (32 threads;
    /// node0 was 93% busy in the confine baseline) — so they move to the
    /// non-home pod(s) and everything else stays home.
    pub fn pod_index(&self, role: ThreadRole, ordinal: usize) -> usize {
        match role {
            ThreadRole::Lv3Batch => self.home_pod,
            // Compress is pure streaming CPU over 128KB units — the ideal
            // offload. Dedup looked similar but is NOT: its hot loop is
            // pointer-chasing home-socket metadata (cuckoo, candidate
            // cache, L2P) plus ReadPool verify round-trips, and offloading
            // it capped the drain at ~15k remap/s while the front-end ran
            // at 20k (2026-06-11 fourth iteration).
            ThreadRole::FlusherCompress => {
                self.non_home_pod(ordinal / self.compress_workers.max(1))
            }
            _ => self.home_pod,
        }
    }

    /// Spread `idx` across the pods that are NOT home (single-pod topologies
    /// degenerate to home).
    fn non_home_pod(&self, idx: usize) -> usize {
        let others: Vec<usize> = (0..self.pods.len())
            .filter(|&p| p != self.home_pod)
            .collect();
        if others.is_empty() {
            self.home_pod
        } else {
            others[idx % others.len()]
        }
    }

    /// Union of all pods' CPUs (the partition-mode "anywhere in the engine"
    /// set, used by the stray-thread enforcer).
    pub fn all_cpus(&self) -> Vec<usize> {
        let mut all: Vec<usize> = self
            .pods
            .iter()
            .flat_map(|p| p.cpus.iter().copied())
            .collect();
        all.sort_unstable();
        all.dedup();
        all
    }

    fn cpu_set_for_role(&self, role: ThreadRole) -> Vec<usize> {
        let pod_indices: Vec<usize> = match role {
            ThreadRole::FlusherCompress if self.pods.len() > 1 => (0..self.pods.len())
                .filter(|&pod| pod != self.home_pod)
                .collect(),
            _ => vec![self.home_pod],
        };
        let mut cpus: Vec<_> = pod_indices
            .into_iter()
            .flat_map(|pod| self.pods[pod].cpus.iter().copied())
            .collect();
        cpus.sort_unstable();
        cpus.dedup();
        cpus
    }
}

static LAYOUT: OnceLock<Option<LayoutKind>> = OnceLock::new();

pub fn init(config: &ThreadingConfig) {
    let _ = LAYOUT.set(AffinityLayout::from_config(config).map(LayoutKind::PerRole));
    if config.enabled {
        onyx_metadb::affinity::configure(onyx_metadb::affinity::AffinityConfig {
            wal_cpus: config.metadb_wal_cpus.clone(),
            l2p_apply_cpus: config.metadb_l2p_apply_cpus.clone(),
            refcount_apply_cpus: config.metadb_refcount_apply_cpus.clone(),
            dedup_apply_cpus: config.metadb_dedup_apply_cpus.clone(),
            refcount_drainer_cpus: config.metadb_refcount_drainer_cpus.clone(),
            l2p_compactor_cpus: config.metadb_l2p_compactor_cpus.clone(),
            io_submitter_cpus: config.metadb_io_submitter_cpus.clone(),
        });
    }
}

/// Confine-mode layout: all onyx roles bind to `cpus`. metadb threads are
/// deliberately NOT configured (`onyx_metadb::affinity` stays unset): they
/// inherit the caller's node-wide mask, which matches the proven
/// "numactl + threading.enabled=false" profile where metadb runs unpinned
/// inside the node.
pub fn init_confine(foreground: Vec<usize>, background: Vec<usize>) {
    let _ = LAYOUT.set(Some(LayoutKind::ConfineSets {
        foreground,
        background,
    }));
}

/// Partition-mode layout (see `PartitionTopo`).
pub fn init_partition(topo: PartitionTopo) {
    let _ = LAYOUT.set(Some(LayoutKind::Partition(topo)));
}

/// Return the complete CPU set assigned to a role by the active layout.
/// An empty vector means affinity is not configured and the caller should
/// inherit its creating thread's mask.
pub fn role_cpu_set(role: ThreadRole) -> Vec<usize> {
    let Some(Some(layout)) = LAYOUT.get() else {
        return Vec::new();
    };
    layout.cpu_set_for_role(role)
}

/// Whether the active runtime layout uses the strict foreground/background
/// confine split.
pub fn is_confine_layout() -> bool {
    matches!(LAYOUT.get(), Some(Some(LayoutKind::ConfineSets { .. })))
}

pub fn bind_current(role: ThreadRole, ordinal: usize) {
    let Some(Some(layout)) = LAYOUT.get() else {
        return;
    };
    let result = match layout {
        LayoutKind::PerRole(layout) => {
            let Some(cpu) = layout.cpus_for(role).pick(ordinal) else {
                return;
            };
            set_current_cpus(&[cpu])
        }
        LayoutKind::ConfineSets {
            foreground,
            background,
        } => set_current_cpus(if role_uses_foreground_set(role) {
            foreground
        } else {
            background
        }),
        LayoutKind::Partition(topo) => {
            let pod = &topo.pods[topo.pod_index(role, ordinal)];
            // Tier A first-touch locality: this thread's future allocations
            // prefer its pod's node (spill, never stall, when full).
            if let Err(err) = crate::numa::set_thread_preferred_node(pod.node) {
                tracing::warn!(?role, ordinal, node = pod.node, error = %err,
                    "failed to set thread memory policy");
            }
            set_current_cpus(&pod.cpus)
        }
    };
    if let Err(err) = result {
        tracing::warn!(
            ?role,
            ordinal,
            error = %err,
            "failed to set thread CPU affinity"
        );
    }
}

impl LayoutKind {
    fn cpu_set_for_role(&self, role: ThreadRole) -> Vec<usize> {
        match self {
            Self::PerRole(layout) => layout.cpus_for(role).cpus.clone(),
            Self::ConfineSets {
                foreground,
                background,
            } => {
                if role_uses_foreground_set(role) {
                    foreground.clone()
                } else {
                    background.clone()
                }
            }
            Self::Partition(topo) => topo.cpu_set_for_role(role),
        }
    }
}

fn role_uses_foreground_set(role: ThreadRole) -> bool {
    matches!(role, ThreadRole::Ublk | ThreadRole::BufferSync)
}

/// Bind the *calling* thread to `cpus`. Used by `numa::setup` on the main
/// thread before any engine thread exists, so every later spawn — including
/// metadb internals and libublk parents — inherits node confinement.
pub fn bind_current_thread_to(cpus: &[usize]) -> std::io::Result<()> {
    set_current_cpus(cpus)
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
            commit_worker: CpuSet::parse(&config.commit_worker_cpus),
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
            ThreadRole::Lv3Batch => &self.flusher_writer,
            ThreadRole::FlusherWriter => &self.flusher_writer,
            ThreadRole::FlusherCleanup => &self.flusher_cleanup,
            ThreadRole::CommitWorker => {
                // Operators who haven't carved out a dedicated CPU set
                // for the commit_worker fall back to `flusher_writer`'s
                // CPUs — that's the pre-1.B behaviour we are replacing.
                // Avoid a silent placement regression for configs that
                // ship without the new knob.
                if self.commit_worker.cpus.is_empty() {
                    &self.flusher_writer
                } else {
                    &self.commit_worker
                }
            }
            ThreadRole::FlusherPostCommit => {
                // Pre-partition behaviour: post-commit threads shared the
                // FlusherCleanup role; keep that placement for legacy
                // configs.
                &self.flusher_cleanup
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn topo2() -> PartitionTopo {
        PartitionTopo {
            pods: vec![
                PodCpus {
                    node: 0,
                    cpus: vec![0, 2, 4, 6],
                },
                PodCpus {
                    node: 1,
                    cpus: vec![1, 3, 5, 7],
                },
            ],
            home_pod: 0,
            shards: 16,
            dedup_workers: 2,
            compress_workers: 2,
            queue_workers: 4,
            nr_queues: 32,
            read_pool_workers: 16,
        }
    }

    #[test]
    fn latency_domain_roles_stay_home() {
        let t = topo2();
        for ord in [0usize, 7, 8, 15, 31, 127] {
            assert_eq!(t.pod_index(ThreadRole::FlusherWriter, ord), 0);
            assert_eq!(t.pod_index(ThreadRole::BufferSync, ord), 0);
            assert_eq!(t.pod_index(ThreadRole::FlusherCoalesce, ord), 0);
            assert_eq!(t.pod_index(ThreadRole::FlusherCleanup, ord), 0);
            assert_eq!(t.pod_index(ThreadRole::Ublk, ord), 0);
            assert_eq!(t.pod_index(ThreadRole::ReadPool, ord), 0);
        }
    }

    #[test]
    fn compute_roles_offload_to_non_home() {
        let t = topo2();
        // All compress workers land on the non-home pod regardless of shard
        // (2-node: everything on pod 1); dedup stays home (metadata-coupled).
        for ord in [0usize, 1, 7 * 2 + 1, 8 * 2, 15 * 2 + 1] {
            assert_eq!(t.pod_index(ThreadRole::FlusherDedup, ord), 0);
            assert_eq!(t.pod_index(ThreadRole::FlusherCompress, ord), 1);
        }
        // Single-pod topology degenerates to home.
        let mut single = topo2();
        single.pods.truncate(1);
        assert_eq!(single.pod_index(ThreadRole::FlusherDedup, 3), 0);
    }

    #[test]
    fn lv3_batch_executors_stay_with_lv3_locality() {
        let t = topo2();
        for ord in 0..8 {
            assert_eq!(t.pod_index(ThreadRole::Lv3Batch, ord), 0);
        }
        let mut single = topo2();
        single.pods.truncate(1);
        assert_eq!(single.pod_index(ThreadRole::Lv3Batch, 7), 0);
    }

    #[test]
    fn partition_singletons_go_home() {
        let t = topo2();
        for ord in [0usize, 5, 15] {
            assert_eq!(t.pod_index(ThreadRole::CommitWorker, ord), 0);
            assert_eq!(t.pod_index(ThreadRole::FlusherPostCommit, ord), 0);
            assert_eq!(t.pod_index(ThreadRole::Background, ord), 0);
            assert_eq!(t.pod_index(ThreadRole::MetadbCheckpoint, ord), 0);
        }
    }

    #[test]
    fn partition_all_cpus_union() {
        assert_eq!(topo2().all_cpus(), vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn role_cpu_sets_cover_per_role_confine_and_partition_layouts() {
        let config = ThreadingConfig {
            enabled: true,
            ublk_cpus: "7,3-4,3".into(),
            buffer_sync_cpus: "5-6".into(),
            flusher_writer_cpus: "8-9".into(),
            background_cpus: "10-11".into(),
            ..ThreadingConfig::default()
        };
        let per_role = LayoutKind::PerRole(AffinityLayout::from_config(&config).unwrap());
        assert_eq!(per_role.cpu_set_for_role(ThreadRole::Ublk), vec![3, 4, 7]);
        assert_eq!(
            per_role.cpu_set_for_role(ThreadRole::Background),
            vec![10, 11]
        );
        assert_eq!(
            per_role.cpu_set_for_role(ThreadRole::BufferSync),
            vec![5, 6]
        );
        assert_eq!(per_role.cpu_set_for_role(ThreadRole::Lv3Batch), vec![8, 9]);

        let confine = LayoutKind::ConfineSets {
            foreground: vec![0, 2],
            background: vec![4, 6],
        };
        assert_eq!(confine.cpu_set_for_role(ThreadRole::Ublk), vec![0, 2]);
        assert_eq!(confine.cpu_set_for_role(ThreadRole::BufferSync), vec![0, 2]);
        assert_eq!(confine.cpu_set_for_role(ThreadRole::Background), vec![4, 6]);
        assert_eq!(confine.cpu_set_for_role(ThreadRole::Lv3Batch), vec![4, 6]);

        let partition = LayoutKind::Partition(topo2());
        assert_eq!(
            partition.cpu_set_for_role(ThreadRole::Ublk),
            vec![0, 2, 4, 6]
        );
        assert_eq!(
            partition.cpu_set_for_role(ThreadRole::FlusherCompress),
            vec![1, 3, 5, 7]
        );
    }
}

#[cfg(target_os = "linux")]
fn set_current_cpus(cpus: &[usize]) -> std::io::Result<()> {
    // Keep the implementation local and tiny: CPU_SETSIZE is 1024 in glibc,
    // which is plenty for the machines this profile targets.
    const CPU_SETSIZE: usize = 1024;
    const BITS_PER_WORD: usize = 8 * std::mem::size_of::<libc::c_ulong>();
    if cpus.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "empty cpu set",
        ));
    }
    let mut set = [0 as libc::c_ulong; CPU_SETSIZE / BITS_PER_WORD];
    for &cpu in cpus {
        if cpu >= CPU_SETSIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("cpu {cpu} >= CPU_SETSIZE {CPU_SETSIZE}"),
            ));
        }
        set[cpu / BITS_PER_WORD] |= (1 as libc::c_ulong) << (cpu % BITS_PER_WORD);
    }
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
fn set_current_cpus(_cpus: &[usize]) -> std::io::Result<()> {
    Ok(())
}
