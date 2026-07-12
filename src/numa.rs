//! NUMA topology discovery, memory-policy helpers, and the per-node memory
//! plan used by `[numa]` confine/partition modes (see
//! `docs/numa-aware-design.md`).
//!
//! Concurrency model: `NumaTopology::detect()` reads sysfs once during
//! startup (before any engine threads exist); the mempolicy helpers are thin
//! Linux syscall wrappers that affect only the calling thread (children
//! inherit). Nothing here is called on a hot path.

use std::path::Path;

/// One NUMA node as discovered from sysfs.
#[derive(Debug, Clone)]
pub struct NumaNode {
    pub id: usize,
    /// All online logical CPUs on this node, ascending.
    pub cpus: Vec<usize>,
    /// Physical cores: each entry is the HT sibling group (1 or 2 logical
    /// CPUs) ordered by the lowest member. Used for "spread across physical
    /// cores first" allocation and for carving out reserve cores.
    pub cores: Vec<Vec<usize>>,
    /// `MemTotal` of the node in bytes; 0 when unknown (parse failure or
    /// fallback topology) — capacity checks must be skipped, not failed.
    pub mem_total_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct NumaTopology {
    pub nodes: Vec<NumaNode>,
}

impl NumaTopology {
    pub fn detect() -> Self {
        Self::detect_from_roots(
            Path::new("/sys/devices/system/node"),
            Path::new("/sys/devices/system/cpu"),
        )
    }

    /// Sysfs roots are injectable so tests can run against a fake tree.
    pub fn detect_from_roots(node_root: &Path, cpu_root: &Path) -> Self {
        let mut nodes = Vec::new();
        if let Ok(entries) = std::fs::read_dir(node_root) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                let Some(id) = name
                    .strip_prefix("node")
                    .and_then(|s| s.parse::<usize>().ok())
                else {
                    continue;
                };
                let cpulist = entry.path().join("cpulist");
                let Ok(raw) = std::fs::read_to_string(&cpulist) else {
                    continue;
                };
                let cpus = parse_cpu_list(&raw);
                if cpus.is_empty() {
                    continue;
                }
                let cores = sibling_groups(&cpus, cpu_root);
                let mem_total_bytes = std::fs::read_to_string(entry.path().join("meminfo"))
                    .map(|s| parse_node_memtotal(&s))
                    .unwrap_or(0);
                nodes.push(NumaNode {
                    id,
                    cpus,
                    cores,
                    mem_total_bytes,
                });
            }
        }
        nodes.sort_by_key(|n| n.id);
        if nodes.is_empty() {
            return Self::fallback();
        }
        Self { nodes }
    }

    /// Single-node degenerate topology: all schedulable CPUs in node 0,
    /// unknown memory. Used on non-Linux, in containers without sysfs, or
    /// when parsing fails — every NUMA feature becomes a no-op against it.
    fn fallback() -> Self {
        let n = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let cpus: Vec<usize> = (0..n).collect();
        let cores = cpus.iter().map(|&c| vec![c]).collect();
        Self {
            nodes: vec![NumaNode {
                id: 0,
                cpus,
                cores,
                mem_total_bytes: 0,
            }],
        }
    }

    pub fn node(&self, id: usize) -> Option<&NumaNode> {
        self.nodes.iter().find(|n| n.id == id)
    }
}

impl NumaNode {
    /// CPUs available to the engine on this node after removing
    /// `reserve_cores` physical cores (highest-numbered cores, both HT
    /// siblings) for the OS / IRQs / interactive use. Never returns an empty
    /// set: reservation is capped so at least one core remains.
    pub fn engine_cpus(&self, reserve_cores: usize) -> Vec<usize> {
        let reserve = reserve_cores.min(self.cores.len().saturating_sub(1));
        let reserved: std::collections::HashSet<usize> = self
            .cores
            .iter()
            .rev()
            .take(reserve)
            .flatten()
            .copied()
            .collect();
        self.cpus
            .iter()
            .copied()
            .filter(|c| !reserved.contains(c))
            .collect()
    }

    fn confine_cpu_sets(
        &self,
        reserve_cores: usize,
        foreground_cores: usize,
    ) -> (Vec<usize>, Vec<usize>) {
        let engine: std::collections::HashSet<usize> =
            self.engine_cpus(reserve_cores).into_iter().collect();
        let available: Vec<&Vec<usize>> = self
            .cores
            .iter()
            .filter(|core| core.iter().any(|cpu| engine.contains(cpu)))
            .collect();
        let foreground_count = foreground_cores.min(available.len().saturating_sub(1));
        if foreground_count == 0 {
            let cpus: Vec<usize> = engine.iter().copied().collect();
            return (cpus.clone(), cpus);
        }
        let foreground_set: std::collections::HashSet<usize> = available
            .iter()
            .take(foreground_count)
            .flat_map(|core| core.iter().copied())
            .filter(|cpu| engine.contains(cpu))
            .collect();
        let mut foreground: Vec<usize> = foreground_set.iter().copied().collect();
        let mut background: Vec<usize> = engine
            .iter()
            .copied()
            .filter(|cpu| !foreground_set.contains(cpu))
            .collect();
        foreground.sort_unstable();
        background.sort_unstable();
        (foreground, background)
    }
}

/// Parse a sysfs cpulist like `"0,2,4-6,8\n"`.
pub fn parse_cpu_list(raw: &str) -> Vec<usize> {
    let mut cpus = Vec::new();
    for part in raw
        .trim()
        .split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
    {
        if let Some((start, end)) = part.split_once('-') {
            if let (Ok(s), Ok(e)) = (start.parse::<usize>(), end.parse::<usize>()) {
                if s <= e {
                    cpus.extend(s..=e);
                }
            }
        } else if let Ok(c) = part.parse::<usize>() {
            cpus.push(c);
        }
    }
    cpus.sort_unstable();
    cpus.dedup();
    cpus
}

/// Group a node's CPUs into physical cores via
/// `cpu{N}/topology/thread_siblings_list`. Missing topology files degrade to
/// one core per CPU.
fn sibling_groups(cpus: &[usize], cpu_root: &Path) -> Vec<Vec<usize>> {
    let in_node: std::collections::HashSet<usize> = cpus.iter().copied().collect();
    let mut seen = std::collections::HashSet::new();
    let mut cores = Vec::new();
    for &cpu in cpus {
        if seen.contains(&cpu) {
            continue;
        }
        let path = cpu_root
            .join(format!("cpu{cpu}"))
            .join("topology")
            .join("thread_siblings_list");
        let group: Vec<usize> = match std::fs::read_to_string(&path) {
            Ok(raw) => parse_cpu_list(&raw)
                .into_iter()
                .filter(|c| in_node.contains(c))
                .collect(),
            Err(_) => vec![cpu],
        };
        let group = if group.is_empty() { vec![cpu] } else { group };
        for &c in &group {
            seen.insert(c);
        }
        cores.push(group);
    }
    cores.sort_by_key(|g| g.first().copied().unwrap_or(usize::MAX));
    cores
}

/// `Node 0 MemTotal:       131596528 kB` → bytes.
fn parse_node_memtotal(meminfo: &str) -> u64 {
    for line in meminfo.lines() {
        if line.contains("MemTotal:") {
            if let Some(kb) = line
                .split_whitespace()
                .rev()
                .nth(1)
                .and_then(|v| v.parse::<u64>().ok())
            {
                return kb * 1024;
            }
        }
    }
    0
}

// ---------------------------------------------------------------------------
// Memory plan
// ---------------------------------------------------------------------------

/// Engine memory budget split by placement tier (docs/numa-aware-design.md
/// §3.5). Tier A must stay node-local; Tier B is capacity cache that is
/// allowed (or directed) to spill.
#[derive(Debug, Clone, Copy)]
pub struct MemoryBudget {
    /// Tier A: metadb memtable/l2p_buffer budget.
    pub memtable_bytes: u64,
    /// Tier A: metadb pinned L2P index pages.
    pub index_pin_bytes: u64,
    /// Tier A: LV2 resident payload cache ceiling.
    pub buffer_payload_bytes: u64,
    /// Tier B: metadb block cache.
    pub block_cache_bytes: u64,
    /// Tier B: dedup RAM candidate cache estimate.
    pub candidate_cache_bytes: u64,
}

impl MemoryBudget {
    pub fn tier_a_bytes(&self) -> u64 {
        self.memtable_bytes + self.index_pin_bytes + self.buffer_payload_bytes
    }
    pub fn tier_b_bytes(&self) -> u64 {
        self.block_cache_bytes + self.candidate_cache_bytes
    }
}

/// Resolved Tier B placement after the budget check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColdCachePlacement {
    /// Everything fits on the home node.
    Home,
    /// Tier B spreads across all nodes (interleave / kernel spill); Tier A
    /// stays home.
    Interleave,
    /// Even Tier A exceeds home capacity — only valid with
    /// `allow_overcommit`.
    Overcommitted,
}

#[derive(Debug, Clone)]
pub struct MemoryPlan {
    pub home_node: usize,
    pub budget: MemoryBudget,
    pub home_capacity_bytes: u64,
    pub placement: ColdCachePlacement,
    /// False only when the plan requires overcommit and the config does not
    /// allow it — startup must refuse.
    pub acceptable: bool,
}

/// Fixed slack we refuse to plan into: OS / page cache / jemalloc metadata.
const PLAN_OS_FLOOR_BYTES: u64 = 4 << 30;
/// Multiplicative allocator/fragmentation overhead applied to the budget.
const PLAN_OVERHEAD_NUM: u64 = 11; // ×1.1
const PLAN_OVERHEAD_DEN: u64 = 10;

pub fn plan_confine(
    topo: &NumaTopology,
    home_node: usize,
    budget: MemoryBudget,
    cold_cache_policy: crate::config::ColdCachePolicy,
    allow_overcommit: bool,
) -> MemoryPlan {
    let capacity = topo.node(home_node).map(|n| n.mem_total_bytes).unwrap_or(0);
    let with_overhead = |b: u64| b * PLAN_OVERHEAD_NUM / PLAN_OVERHEAD_DEN;
    let tier_a = with_overhead(budget.tier_a_bytes());
    let tier_b = with_overhead(budget.tier_b_bytes());

    let placement = if capacity == 0 {
        // Unknown capacity (fallback topology): trust the operator's policy,
        // default to keeping everything home.
        match cold_cache_policy {
            crate::config::ColdCachePolicy::Interleave => ColdCachePlacement::Interleave,
            _ => ColdCachePlacement::Home,
        }
    } else {
        let usable = capacity.saturating_sub(PLAN_OS_FLOOR_BYTES);
        match cold_cache_policy {
            crate::config::ColdCachePolicy::Home => {
                if tier_a + tier_b <= usable {
                    ColdCachePlacement::Home
                } else {
                    ColdCachePlacement::Overcommitted
                }
            }
            crate::config::ColdCachePolicy::Interleave => {
                if tier_a <= usable {
                    ColdCachePlacement::Interleave
                } else {
                    ColdCachePlacement::Overcommitted
                }
            }
            crate::config::ColdCachePolicy::Auto => {
                if tier_a + tier_b <= usable {
                    ColdCachePlacement::Home
                } else if tier_a <= usable {
                    ColdCachePlacement::Interleave
                } else {
                    ColdCachePlacement::Overcommitted
                }
            }
        }
    };

    let acceptable = placement != ColdCachePlacement::Overcommitted || allow_overcommit;
    MemoryPlan {
        home_node,
        budget,
        home_capacity_bytes: capacity,
        placement,
        acceptable,
    }
}

/// Partition-mode plan: Tier A splits per pod (each shard's structures live
/// on its pod), Tier B interleaves across the data nodes. Each node must fit
/// its share; `home_capacity_bytes` reports the smallest data node.
pub fn plan_partition(
    topo: &NumaTopology,
    data_nodes: &[usize],
    budget: MemoryBudget,
    allow_overcommit: bool,
) -> MemoryPlan {
    let npods = data_nodes.len().max(1) as u64;
    let min_capacity = data_nodes
        .iter()
        .filter_map(|&id| topo.node(id).map(|n| n.mem_total_bytes))
        .min()
        .unwrap_or(0);
    let with_overhead = |b: u64| b * PLAN_OVERHEAD_NUM / PLAN_OVERHEAD_DEN;
    let per_node = with_overhead(budget.tier_a_bytes() + budget.tier_b_bytes()) / npods;
    let acceptable = if min_capacity == 0 {
        true // unknown capacity: trust the operator
    } else {
        per_node <= min_capacity.saturating_sub(PLAN_OS_FLOOR_BYTES) || allow_overcommit
    };
    MemoryPlan {
        home_node: data_nodes.first().copied().unwrap_or(0),
        budget,
        home_capacity_bytes: min_capacity,
        placement: ColdCachePlacement::Interleave,
        acceptable,
    }
}

impl MemoryPlan {
    pub fn log(&self) {
        let gib = |b: u64| b as f64 / (1u64 << 30) as f64;
        tracing::info!(
            home_node = self.home_node,
            capacity_gib = format!("{:.1}", gib(self.home_capacity_bytes)),
            tier_a_gib = format!("{:.1}", gib(self.budget.tier_a_bytes())),
            memtable_gib = format!("{:.1}", gib(self.budget.memtable_bytes)),
            index_pin_gib = format!("{:.1}", gib(self.budget.index_pin_bytes)),
            buffer_payload_gib = format!("{:.1}", gib(self.budget.buffer_payload_bytes)),
            tier_b_gib = format!("{:.1}", gib(self.budget.tier_b_bytes())),
            block_cache_gib = format!("{:.1}", gib(self.budget.block_cache_bytes)),
            candidate_gib = format!("{:.1}", gib(self.budget.candidate_cache_bytes)),
            placement = ?self.placement,
            acceptable = self.acceptable,
            "numa memory plan"
        );
    }
}

// ---------------------------------------------------------------------------
// Memory policy syscalls (Linux only; no-ops elsewhere)
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
mod policy {
    const MPOL_PREFERRED: i32 = 1;
    const MPOL_BIND: i32 = 2;
    const MPOL_INTERLEAVE: i32 = 3;

    fn nodemask(nodes: &[usize]) -> std::io::Result<u64> {
        let mut mask = 0u64;
        for &n in nodes {
            if n >= 64 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("node {n} >= 64 unsupported"),
                ));
            }
            mask |= 1 << n;
        }
        Ok(mask)
    }

    /// Set the calling thread's default policy to prefer `node`; child
    /// threads inherit. When the node fills the kernel falls back to other
    /// nodes instead of reclaiming.
    ///
    /// ⚠ Field data (2026-06-11 p0-2h soak): PREFERRED as the *process*
    /// policy under confine is a trap — WAL/XFS page cache fills the home
    /// node in ~85 min, after which every HOT allocation spills remote and
    /// flush throughput collapses ~50% into a buffer-pileup spiral. Confine
    /// uses `set_thread_bind_node` (numactl --membind semantics, proven over
    /// 8-12h soaks) instead; PREFERRED remains for per-pod worker threads
    /// under partition where the spill target is the sibling pod.
    pub fn set_thread_preferred_node(node: usize) -> std::io::Result<()> {
        set_mempolicy(MPOL_PREFERRED, &[node])
    }

    /// numactl --membind equivalent: allocations MUST come from `node`; the
    /// kernel reclaims (evicting cold page cache, e.g. recycled WAL
    /// segments) rather than spilling hot anonymous pages cross-socket.
    pub fn set_thread_bind_node(node: usize) -> std::io::Result<()> {
        set_mempolicy(MPOL_BIND, &[node])
    }

    fn set_mempolicy(mode: i32, nodes: &[usize]) -> std::io::Result<()> {
        let mask = nodemask(nodes)?;
        // maxnode counts bits and must cover the highest set bit; pass the
        // full word width + 1 per the syscall's off-by-one convention.
        let rc =
            unsafe { libc::syscall(libc::SYS_set_mempolicy, mode, &mask as *const u64, 65usize) };
        if rc == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    fn mbind(ptr: *mut u8, len: usize, mode: i32, mask: u64) -> std::io::Result<()> {
        let rc = unsafe {
            libc::syscall(
                libc::SYS_mbind,
                ptr,
                len,
                mode,
                &mask as *const u64,
                65usize,
                0usize, // flags: policy applies to not-yet-faulted pages
            )
        };
        if rc == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    /// Set the calling thread's default policy to interleave across `nodes`
    /// (children inherit). Used as the process default under partition mode
    /// so shared caches spread instead of piling on the startup node.
    pub fn set_thread_interleave(nodes: &[usize]) -> std::io::Result<()> {
        set_mempolicy(MPOL_INTERLEAVE, nodes)
    }

    /// Hard-bind an already-mmapped region to one node (Tier A).
    pub fn bind_region(ptr: *mut u8, len: usize, node: usize) -> std::io::Result<()> {
        mbind(ptr, len, MPOL_BIND, nodemask(&[node])?)
    }

    /// Interleave an already-mmapped region across `nodes` (Tier B).
    pub fn interleave_region(ptr: *mut u8, len: usize, nodes: &[usize]) -> std::io::Result<()> {
        mbind(ptr, len, MPOL_INTERLEAVE, nodemask(nodes)?)
    }
}

#[cfg(not(target_os = "linux"))]
mod policy {
    pub fn set_thread_preferred_node(_node: usize) -> std::io::Result<()> {
        Ok(())
    }
    pub fn set_thread_bind_node(_node: usize) -> std::io::Result<()> {
        Ok(())
    }
    pub fn set_thread_interleave(_nodes: &[usize]) -> std::io::Result<()> {
        Ok(())
    }
    pub fn bind_region(_ptr: *mut u8, _len: usize, _node: usize) -> std::io::Result<()> {
        Ok(())
    }
    pub fn interleave_region(_ptr: *mut u8, _len: usize, _nodes: &[usize]) -> std::io::Result<()> {
        Ok(())
    }
}

pub use policy::{
    bind_region, interleave_region, set_thread_bind_node, set_thread_interleave,
    set_thread_preferred_node,
};

// ---------------------------------------------------------------------------
// Startup orchestration
// ---------------------------------------------------------------------------

/// Process-wide NUMA + affinity setup. Call once from `main` before the
/// engine (and therefore before any engine/metadb/libublk thread) exists —
/// confinement works by inheritance from the calling thread.
pub fn setup(config: &crate::config::OnyxConfig) -> crate::error::OnyxResult<()> {
    use crate::config::NumaMode;
    match config.numa.mode {
        NumaMode::Off => {
            crate::affinity::init(&config.threading);
            Ok(())
        }
        NumaMode::Partition => setup_partition(config),
        NumaMode::Confine => setup_confine(config),
    }
}

fn setup_partition(config: &crate::config::OnyxConfig) -> crate::error::OnyxResult<()> {
    if config.threading.enabled {
        tracing::warn!(
            "numa.mode = \"partition\" is set: ignoring [threading] *_cpus pinning \
             (the two mechanisms must not stack)"
        );
    }
    let topo = NumaTopology::detect();
    let data_nodes: Vec<usize> = if config.numa.data_nodes.is_empty() {
        topo.nodes.iter().map(|n| n.id).collect()
    } else {
        config.numa.data_nodes.clone()
    };
    for &id in &data_nodes {
        if topo.node(id).is_none() {
            return Err(crate::error::OnyxError::Config(format!(
                "numa.data_nodes contains node {id} not present (detected: {:?})",
                topo.nodes.iter().map(|n| n.id).collect::<Vec<_>>()
            )));
        }
    }
    let Some(home_pod) = data_nodes.iter().position(|&n| n == config.numa.home_node) else {
        return Err(crate::error::OnyxError::Config(format!(
            "numa.home_node = {} must be one of numa.data_nodes {:?}",
            config.numa.home_node, data_nodes
        )));
    };
    if data_nodes.len() == 1 {
        tracing::info!("numa partition on a single node degenerates to confine");
        return setup_confine(config);
    }

    let pods: Vec<crate::affinity::PodCpus> = data_nodes
        .iter()
        .map(|&id| crate::affinity::PodCpus {
            node: id,
            cpus: topo
                .node(id)
                .unwrap()
                .engine_cpus(config.numa.reserve_cores_per_node),
        })
        .collect();

    // Memory model = same as confine: EVERYTHING except the offloaded
    // compress workers lives on the home node, under BIND. The first
    // partition iterations used a process-wide MPOL_INTERLEAVE — that
    // poisoned metadata locality cumulatively (50% of every L2P/cuckoo/
    // cache page remote ⇒ runs started at 19-24µs/remap and sank to
    // 400-700µs within minutes as the working set outgrew L3). The
    // offloaded compute threads override themselves with PREFERRED(pod)
    // at bind time, so their transient buffers land on their own node.
    let plan = plan_confine(
        &topo,
        config.numa.home_node,
        budget_from_config(config),
        config.numa.cold_cache_policy,
        config.numa.allow_overcommit,
    );
    plan.log();
    if !plan.acceptable
        || (plan.placement != ColdCachePlacement::Home && !config.numa.allow_overcommit)
    {
        return Err(crate::error::OnyxError::Config(format!(
            "numa partition: the home node hosts all metadata/caches (only \
             compute offloads) and the budget exceeds node {} ({:.1} GiB \
             usable) — shrink meta.block_cache_mb / buffer.max_memory_mb or \
             set numa.allow_overcommit = true",
            config.numa.home_node,
            plan.home_capacity_bytes.saturating_sub(PLAN_OS_FLOOR_BYTES) as f64
                / (1u64 << 30) as f64
        )));
    }
    if let Err(err) = set_thread_bind_node(config.numa.home_node) {
        tracing::warn!(error = %err, "set_mempolicy(MPOL_BIND) failed; \
             memory placement falls back to the OS default");
    }
    // Main thread lives on the home pod: metadb singletons (WAL/BFG sync/
    // manifest workers without explicit binds) and libublk parents inherit
    // home placement.
    if let Err(err) = crate::affinity::bind_current_thread_to(&pods[home_pod].cpus) {
        return Err(crate::error::OnyxError::Config(format!(
            "failed to bind main thread to home pod {:?}: {err}",
            pods[home_pod]
        )));
    }

    let shards = (config.buffer.shards.max(1)) as usize;
    let topo_part = crate::affinity::PartitionTopo {
        pods: pods.clone(),
        home_pod,
        shards,
        dedup_workers: config.dedup.workers.max(1),
        compress_workers: config.flush.compress_workers.max(1),
        queue_workers: config.ublk.queue_workers.max(1),
        nr_queues: config.ublk.nr_queues.max(1) as usize,
        read_pool_workers: config.storage.read_pool_workers.max(1),
    };
    let all_cpus = topo_part.all_cpus();
    crate::affinity::init_partition(topo_part);

    // metadb stays WHOLE on the home pod — measured 2026-06-11: splitting
    // the apply lanes / BFG drain across pods caps the flush drain at
    // ~13k remap/s regardless of variant, because the LSN-ordered apply
    // chain (last_applied_lsn + cvar) then includes a cross-socket hop per
    // LSN (~70µs ⇒ ~14k/s ceiling), and the shared md devices pay a
    // cross-socket submit tax (LV2 fsync 64µs → 334-540µs). Partition's
    // win comes from spreading the CPU-heavy DATA-plane compute
    // (dedup/compress/coalesce + ublk + reads) across sockets; metadata
    // and device-write paths are latency-serialized and stay home.
    let meta_shards = config.meta.shards_per_partition.max(1) as usize;
    let meta_dedup = config.meta.dedup_shards.max(1) as usize;
    onyx_metadb::affinity::configure_nodes(onyx_metadb::affinity::NodeAffinityConfig {
        pods: pods
            .iter()
            .map(|p| onyx_metadb::affinity::NodePod {
                node: p.node,
                cpus: p.cpus.clone(),
            })
            .collect(),
        home_pod,
        shard_pods: vec![home_pod; meta_shards],
        dedup_shard_pods: vec![home_pod; meta_dedup],
    });

    spawn_confine_enforcer(all_cpus.clone(), all_cpus.clone());
    tracing::info!(
        data_nodes = ?data_nodes,
        home_pod,
        pod_sizes = ?pods.iter().map(|p| p.cpus.len()).collect::<Vec<_>>(),
        shards,
        "numa partition active (pod-per-node data plane, home-pod control plane)"
    );
    Ok(())
}

fn setup_confine(config: &crate::config::OnyxConfig) -> crate::error::OnyxResult<()> {
    if config.threading.enabled {
        tracing::warn!(
            "numa.mode = \"confine\" is set: ignoring [threading] *_cpus pinning \
             (the two mechanisms must not stack)"
        );
    }
    let topo = NumaTopology::detect();
    let home = config.numa.home_node;
    let Some(node) = topo.node(home) else {
        return Err(crate::error::OnyxError::Config(format!(
            "numa.home_node = {home} not present (detected nodes: {:?})",
            topo.nodes.iter().map(|n| n.id).collect::<Vec<_>>()
        )));
    };
    let cpus = node.engine_cpus(config.numa.reserve_cores_per_node);
    let (foreground_cpus, background_cpus) = node.confine_cpu_sets(
        config.numa.reserve_cores_per_node,
        config.numa.foreground_cores_per_node,
    );

    let plan = plan_confine(
        &topo,
        home,
        budget_from_config(config),
        config.numa.cold_cache_policy,
        config.numa.allow_overcommit,
    );
    plan.log();
    if !plan.acceptable {
        return Err(crate::error::OnyxError::Config(format!(
            "numa confine memory plan does not fit node {home} \
             (capacity {:.1} GiB): Tier A alone exceeds it; shrink \
             meta.block_cache_mb / buffer.max_memory_mb / meta.index_pin_mb, \
             or set numa.allow_overcommit = true",
            plan.home_capacity_bytes as f64 / (1u64 << 30) as f64
        )));
    }
    if plan.placement != ColdCachePlacement::Home && !config.numa.allow_overcommit {
        // Field-proven hard rule (2026-06-11, two failed 2h soaks): when the
        // budget exceeds the node, NO mempolicy saves confine — PREFERRED
        // spills the hot path remote once the node fills (~85 min), BIND
        // pays a reclaim tax on every allocation instead (~28 min). Both
        // collapse flush throughput into a buffer-pileup spiral. Until
        // Phase 2 lands real per-region interleave, an over-budget confine
        // config is refused, not warned about.
        return Err(crate::error::OnyxError::Config(format!(
            "numa confine: the cache budget (Tier A {:.1} GiB + Tier B {:.1} \
             GiB, ×1.1 overhead) exceeds node {home}'s usable capacity \
             ({:.1} GiB) — a long run WILL collapse when the node fills. \
             Shrink meta.block_cache_mb / buffer.max_memory_mb, use \
             numa.mode = \"partition\", or set numa.allow_overcommit = true.",
            plan.budget.tier_a_bytes() as f64 / (1u64 << 30) as f64,
            plan.budget.tier_b_bytes() as f64 / (1u64 << 30) as f64,
            plan.home_capacity_bytes.saturating_sub(PLAN_OS_FLOOR_BYTES) as f64
                / (1u64 << 30) as f64,
        )));
    }

    // BIND, not PREFERRED: see set_thread_bind_node docs — the home node
    // WILL fill with WAL/XFS page cache within ~1.5h of sustained load, and
    // BIND makes the kernel evict that cold cache instead of spilling the
    // hot path remote (the numactl --membind behaviour proven over 8-12h
    // soaks; PREFERRED collapsed flush throughput at the 85-min mark).
    if let Err(err) = set_thread_bind_node(home) {
        tracing::warn!(error = %err, home, "set_mempolicy(MPOL_BIND) failed; \
             memory placement falls back to the OS default");
    }
    if let Err(err) = crate::affinity::bind_current_thread_to(&background_cpus) {
        return Err(crate::error::OnyxError::Config(format!(
            "failed to confine main thread to node {home} cpus {cpus:?}: {err}"
        )));
    }
    crate::affinity::init_confine(foreground_cpus.clone(), background_cpus.clone());
    spawn_confine_enforcer(foreground_cpus.clone(), background_cpus.clone());
    tracing::info!(
        home_node = home,
        engine_cpus = ?cpus,
        foreground_cpus = ?foreground_cpus,
        background_cpus = ?background_cpus,
        reserved_cores = config.numa.reserve_cores_per_node,
        "numa confine active (in-engine numactl equivalent; ublk queue \
         threads included)"
    );
    Ok(())
}

/// Inheritance + per-role binds are not enough: libublk's per-queue daemon
/// threads call sched_setaffinity on themselves with kernel-suggested masks
/// (mixed nodes) AFTER spawning — the documented "escape numactl" behaviour.
/// This low-frequency sweeper re-confines any thread of this process whose
/// mask strays outside the allowed set, catching libublk today and any
/// future self-pinning library. ~300 task dirs every 5s is noise.
#[cfg(target_os = "linux")]
fn spawn_confine_enforcer(foreground: Vec<usize>, background: Vec<usize>) {
    let _ = std::thread::Builder::new()
        .name("numa-confine".to_string())
        .spawn(move || loop {
            sweep_stray_threads(&foreground, &background);
            std::thread::sleep(std::time::Duration::from_secs(5));
        });
}

#[cfg(not(target_os = "linux"))]
fn spawn_confine_enforcer(_foreground: Vec<usize>, _background: Vec<usize>) {}

#[cfg(target_os = "linux")]
fn sweep_stray_threads(foreground: &[usize], background: &[usize]) {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return;
    };
    for entry in tasks.flatten() {
        let Some(tid) = entry
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<libc::pid_t>().ok())
        else {
            continue;
        };
        let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        let rc = unsafe {
            libc::sched_getaffinity(tid, std::mem::size_of::<libc::cpu_set_t>(), &mut set)
        };
        if rc != 0 {
            continue; // thread exited
        }
        let name = std::fs::read_to_string(entry.path().join("comm")).unwrap_or_default();
        let cpus = if name.starts_with("ublk-")
            || name.starts_with("persistent-slot")
            || name.starts_with("read-pool")
        {
            foreground
        } else {
            background
        };
        let matches =
            (0..1024usize).all(|c| (unsafe { libc::CPU_ISSET(c, &set) }) == cpus.contains(&c));
        if matches {
            continue;
        }
        let mut target: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        for &c in cpus {
            unsafe { libc::CPU_SET(c, &mut target) };
        }
        let rc = unsafe {
            libc::sched_setaffinity(tid, std::mem::size_of::<libc::cpu_set_t>(), &target)
        };
        if rc == 0 {
            tracing::info!(tid, "numa confine: re-bound stray thread (self-pinned outside the node, e.g. libublk queue daemon)");
        }
    }
}

/// Assemble the Tier A / Tier B budget from the engine config. Estimates are
/// intentionally coarse (the plan applies a ×1.1 overhead on top).
fn budget_from_config(config: &crate::config::OnyxConfig) -> MemoryBudget {
    let buffer_payload_bytes = if config.buffer.max_memory_mb > 0 {
        config.buffer.max_memory_mb as u64 * 1024 * 1024
    } else {
        // Mirror of OnyxEngine::auto_detect_max_payload_memory: 20% of
        // system memory capped at 8 GiB.
        let sys = std::fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("MemTotal:"))
                    .and_then(|l| l.split_whitespace().nth(1))
                    .and_then(|kb| kb.parse::<u64>().ok())
                    .map(|kb| kb * 1024)
            })
            .unwrap_or(8 << 30);
        (sys / 5).min(8 << 30)
    };
    let candidate_cache_bytes = if config.dedup.enabled {
        let shards = config.dedup.candidate_shards.unwrap_or(8) as u64;
        let per_shard = config
            .dedup
            .candidate_per_shard_capacity
            .unwrap_or(crate::dedup::candidate::DEFAULT_PER_SHARD_CAPACITY)
            as u64;
        // ~50 B/entry (see CandidateCache docs).
        shards * per_shard * 50
    } else {
        0
    };
    MemoryBudget {
        memtable_bytes: config.meta.memtable_budget_bytes() as u64,
        index_pin_bytes: config.meta.index_pin_bytes() as u64,
        buffer_payload_bytes,
        block_cache_bytes: config.meta.block_cache_bytes() as u64,
        candidate_cache_bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ColdCachePolicy;

    fn fake_sysfs(
        nodes: &[(usize, &str, u64)],
        siblings: &[(usize, &str)],
    ) -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let node_root = dir.path().join("node");
        let cpu_root = dir.path().join("cpu");
        for (id, cpulist, mem_kb) in nodes {
            let nd = node_root.join(format!("node{id}"));
            std::fs::create_dir_all(&nd).unwrap();
            std::fs::write(nd.join("cpulist"), format!("{cpulist}\n")).unwrap();
            std::fs::write(
                nd.join("meminfo"),
                format!("Node {id} MemTotal:       {mem_kb} kB\n"),
            )
            .unwrap();
        }
        for (cpu, list) in siblings {
            let td = cpu_root.join(format!("cpu{cpu}")).join("topology");
            std::fs::create_dir_all(&td).unwrap();
            std::fs::write(td.join("thread_siblings_list"), format!("{list}\n")).unwrap();
        }
        (dir, node_root, cpu_root)
    }

    #[test]
    fn parse_cpu_list_mixed() {
        assert_eq!(parse_cpu_list("0,2,4-6,8\n"), vec![0, 2, 4, 5, 6, 8]);
        assert_eq!(parse_cpu_list(""), Vec::<usize>::new());
        assert_eq!(parse_cpu_list("3-1"), Vec::<usize>::new());
        assert_eq!(parse_cpu_list("7"), vec![7]);
    }

    #[test]
    fn detect_interleaved_two_node_box() {
        // Mirror of nvme-box: node0 = even cpus, node1 = odd cpus, HT pairs
        // (0,4), (2,6) / (1,5), (3,7).
        let (_g, node_root, cpu_root) = fake_sysfs(
            &[(0, "0,2,4,6", 131596528), (1, "1,3,5,7", 131596528)],
            &[
                (0, "0,4"),
                (2, "2,6"),
                (4, "0,4"),
                (6, "2,6"),
                (1, "1,5"),
                (3, "3,7"),
                (5, "1,5"),
                (7, "3,7"),
            ],
        );
        let topo = NumaTopology::detect_from_roots(&node_root, &cpu_root);
        assert_eq!(topo.nodes.len(), 2);
        let n0 = topo.node(0).unwrap();
        assert_eq!(n0.cpus, vec![0, 2, 4, 6]);
        assert_eq!(n0.cores, vec![vec![0, 4], vec![2, 6]]);
        assert_eq!(n0.mem_total_bytes, 131596528 * 1024);
        let n1 = topo.node(1).unwrap();
        assert_eq!(n1.cpus, vec![1, 3, 5, 7]);
    }

    #[test]
    fn detect_falls_back_to_single_node() {
        let dir = tempfile::tempdir().unwrap();
        let topo = NumaTopology::detect_from_roots(
            &dir.path().join("missing-node"),
            &dir.path().join("missing-cpu"),
        );
        assert_eq!(topo.nodes.len(), 1);
        assert_eq!(topo.nodes[0].id, 0);
        assert!(!topo.nodes[0].cpus.is_empty());
        assert_eq!(topo.nodes[0].mem_total_bytes, 0);
    }

    #[test]
    fn engine_cpus_reserves_highest_cores() {
        let node = NumaNode {
            id: 0,
            cpus: vec![0, 2, 4, 6],
            cores: vec![vec![0, 4], vec![2, 6]],
            mem_total_bytes: 0,
        };
        assert_eq!(node.engine_cpus(0), vec![0, 2, 4, 6]);
        // Reserving 1 core drops the highest core's both siblings (2, 6).
        assert_eq!(node.engine_cpus(1), vec![0, 4]);
        // Reservation is capped: at least one core stays.
        assert_eq!(node.engine_cpus(5), vec![0, 4]);
    }

    #[test]
    fn missing_sibling_topology_degrades_to_one_core_per_cpu() {
        let (_g, node_root, cpu_root) = fake_sysfs(&[(0, "0,1", 1024)], &[]);
        let topo = NumaTopology::detect_from_roots(&node_root, &cpu_root);
        assert_eq!(topo.node(0).unwrap().cores, vec![vec![0], vec![1]]);
    }

    fn budget(tier_a_gib: u64, tier_b_gib: u64) -> MemoryBudget {
        MemoryBudget {
            memtable_bytes: tier_a_gib << 30,
            index_pin_bytes: 0,
            buffer_payload_bytes: 0,
            block_cache_bytes: tier_b_gib << 30,
            candidate_cache_bytes: 0,
        }
    }

    fn one_node_topo(mem_gib: u64) -> NumaTopology {
        NumaTopology {
            nodes: vec![NumaNode {
                id: 0,
                cpus: vec![0],
                cores: vec![vec![0]],
                mem_total_bytes: mem_gib << 30,
            }],
        }
    }

    #[test]
    fn plan_auto_fits_home() {
        let plan = plan_confine(
            &one_node_topo(128),
            0,
            budget(40, 60),
            ColdCachePolicy::Auto,
            false,
        );
        assert_eq!(plan.placement, ColdCachePlacement::Home);
        assert!(plan.acceptable);
    }

    #[test]
    fn plan_auto_degrades_to_interleave() {
        // 40 + 100 GiB (×1.1) > 128-4 usable, but Tier A alone fits.
        let plan = plan_confine(
            &one_node_topo(128),
            0,
            budget(40, 100),
            ColdCachePolicy::Auto,
            false,
        );
        assert_eq!(plan.placement, ColdCachePlacement::Interleave);
        assert!(plan.acceptable);
    }

    #[test]
    fn plan_refuses_tier_a_overflow_without_overcommit() {
        let plan = plan_confine(
            &one_node_topo(64),
            0,
            budget(100, 0),
            ColdCachePolicy::Auto,
            false,
        );
        assert_eq!(plan.placement, ColdCachePlacement::Overcommitted);
        assert!(!plan.acceptable);
        let plan = plan_confine(
            &one_node_topo(64),
            0,
            budget(100, 0),
            ColdCachePolicy::Auto,
            true,
        );
        assert!(plan.acceptable);
    }

    #[test]
    fn plan_forced_home_overflow_is_overcommit() {
        let plan = plan_confine(
            &one_node_topo(128),
            0,
            budget(40, 100),
            ColdCachePolicy::Home,
            false,
        );
        assert_eq!(plan.placement, ColdCachePlacement::Overcommitted);
        assert!(!plan.acceptable);
    }

    #[test]
    fn numa_config_toml_roundtrip() {
        let cfg: crate::config::NumaConfig = toml::from_str(
            r#"
            mode = "confine"
            home_node = 1
            reserve_cores_per_node = 3
            foreground_cores_per_node = 4
            cold_cache_policy = "interleave"
            allow_overcommit = true
            "#,
        )
        .unwrap();
        assert_eq!(cfg.mode, crate::config::NumaMode::Confine);
        assert_eq!(cfg.home_node, 1);
        assert_eq!(cfg.reserve_cores_per_node, 3);
        assert_eq!(cfg.foreground_cores_per_node, 4);
        assert_eq!(cfg.cold_cache_policy, ColdCachePolicy::Interleave);
        assert!(cfg.allow_overcommit);

        // Defaults: mode off, policy auto, 2 reserve cores.
        let cfg: crate::config::NumaConfig = toml::from_str("").unwrap();
        assert_eq!(cfg.mode, crate::config::NumaMode::Off);
        assert_eq!(cfg.cold_cache_policy, ColdCachePolicy::Auto);
        assert_eq!(cfg.reserve_cores_per_node, 2);
        assert_eq!(cfg.foreground_cores_per_node, 0);
        assert!(!cfg.allow_overcommit);
    }

    #[test]
    fn confine_cpu_sets_carve_physical_cores_for_foreground() {
        let node = NumaNode {
            id: 0,
            cpus: vec![0, 1, 2, 3, 4, 5, 6, 7],
            cores: vec![vec![0, 4], vec![1, 5], vec![2, 6], vec![3, 7]],
            mem_total_bytes: 0,
        };
        let (foreground, background) = node.confine_cpu_sets(1, 1);
        assert_eq!(foreground, vec![0, 4]);
        assert_eq!(background, vec![1, 2, 5, 6]);
    }

    #[test]
    fn setup_partition_rejects_absent_data_node() {
        // data_nodes referencing a node this machine doesn't have errors out
        // BEFORE any thread binding / policy side effect.
        let mut cfg = crate::config::OnyxConfig::default();
        cfg.numa.mode = crate::config::NumaMode::Partition;
        cfg.numa.data_nodes = vec![0, 63];
        assert!(matches!(
            setup(&cfg),
            Err(crate::error::OnyxError::Config(_))
        ));
    }

    fn two_node_topo(mem_gib: u64) -> NumaTopology {
        NumaTopology {
            nodes: (0..2)
                .map(|id| NumaNode {
                    id,
                    cpus: vec![id],
                    cores: vec![vec![id]],
                    mem_total_bytes: mem_gib << 30,
                })
                .collect(),
        }
    }

    #[test]
    fn plan_partition_splits_across_nodes() {
        // 40 + 100 GiB does NOT fit one 128 GiB node (see the confine test)
        // but split across two nodes each takes ~77 GiB -> fits.
        let plan = plan_partition(&two_node_topo(128), &[0, 1], budget(40, 100), false);
        assert_eq!(plan.placement, ColdCachePlacement::Interleave);
        assert!(plan.acceptable);
        // Whereas 300 GiB total does not fit even split.
        let plan = plan_partition(&two_node_topo(128), &[0, 1], budget(200, 100), false);
        assert!(!plan.acceptable);
        let plan = plan_partition(&two_node_topo(128), &[0, 1], budget(200, 100), true);
        assert!(plan.acceptable);
    }

    #[test]
    fn setup_partition_rejects_bad_nodes() {
        let mut cfg = crate::config::OnyxConfig::default();
        cfg.numa.mode = crate::config::NumaMode::Partition;
        cfg.numa.home_node = 7; // not in data_nodes
        cfg.numa.data_nodes = vec![0];
        assert!(matches!(
            setup(&cfg),
            Err(crate::error::OnyxError::Config(_))
        ));
    }

    #[test]
    fn plan_unknown_capacity_trusts_policy() {
        let plan = plan_confine(
            &one_node_topo(0),
            0,
            budget(100, 100),
            ColdCachePolicy::Auto,
            false,
        );
        assert_eq!(plan.placement, ColdCachePlacement::Home);
        assert!(plan.acceptable);
    }
}
