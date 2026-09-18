use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwap;

use crate::config::ThreadingConfig;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

impl ThreadRole {
    /// Number of variants — the width of [`CoreBudget`]'s per-role table.
    pub const COUNT: usize = 13;

    /// Dense index for per-role tables. Kept next to the enum so adding a
    /// variant without extending `COUNT` fails to compile on the match.
    pub const fn index(self) -> usize {
        match self {
            Self::Ublk => 0,
            Self::ReadPool => 1,
            Self::BufferSync => 2,
            Self::FlusherCoalesce => 3,
            Self::FlusherDedup => 4,
            Self::FlusherCompress => 5,
            Self::Lv3Batch => 6,
            Self::FlusherWriter => 7,
            Self::FlusherCleanup => 8,
            Self::CommitWorker => 9,
            Self::FlusherPostCommit => 10,
            Self::MetadbCheckpoint => 11,
            Self::Background => 12,
        }
    }
}

/// Which CPUs each role may run on under `numa.mode = "confine"`.
///
/// Before this existed, confine mode was two flat `Vec<usize>` (a foreground
/// and a background half) plus a *separate* string-prefix function in
/// `numa::confine_thread_placement` that the stray-thread enforcer used to
/// decide the same question. Two mappings answering "where does this thread
/// run" meant a role could not be given CPUs of its own: the enforcer does an
/// EXACT mask comparison every 5 s and re-binds anything narrower, so any
/// per-role pin was silently undone within one sweep. This type is the single
/// answer both paths now go through.
///
/// It is deliberately NOT a scheduler. It owns one decision — the CPU set per
/// role — so that a pool's thread count can stop being a function of config
/// topology (see [`crate::config::CoresConfig`]) and start being a function of
/// the cores actually available.
#[derive(Clone, Debug, Default)]
pub struct CoreBudget {
    /// Every engine-usable logical CPU on the home node, i.e.
    /// `NumaNode::engine_cpus(reserve_cores_per_node)`. This is the denominator
    /// that no pool in the engine used to consult.
    engine: Vec<usize>,
    /// Foreground half: ublk and the LV2 sync threads. Equal to `engine` when
    /// `numa.foreground_cores_per_node` is 0, which is the shipped default.
    foreground: Vec<usize>,
    /// Background half: everything else that has no dedicated set.
    background: Vec<usize>,
    /// Exclusive CPUs, indexed by [`ThreadRole::index`]. `None` means the role
    /// shares the foreground/background half it would have used anyway, so an
    /// all-`None` table reproduces the previous behaviour exactly.
    dedicated: Vec<Option<Vec<usize>>>,
}

impl CoreBudget {
    /// Build the shared-set budget: no role owns CPUs exclusively, which is
    /// byte-for-byte the pre-budget confine behaviour.
    pub fn new(engine: Vec<usize>, foreground: Vec<usize>, background: Vec<usize>) -> Self {
        Self {
            engine,
            foreground,
            background,
            dedicated: vec![None; ThreadRole::COUNT],
        }
    }

    /// Give `role` `core_count` physical cores of its own.
    ///
    /// Cores come off the END of the background half, matching
    /// `NumaNode::engine_cpus`, which reserves the highest-numbered cores for
    /// the OS — so the dedicated set lands adjacent to the OS reserve instead
    /// of fragmenting the middle of the node.
    ///
    /// Both HT siblings of a core move together: handing out one sibling while
    /// a shared pool keeps the other would leave the "dedicated" thread
    /// sharing execution resources with whatever landed next door, which is
    /// most of the interference this is meant to remove.
    ///
    /// ⚠ The taken CPUs are removed from **both** halves, not just the one
    /// they were drawn from. `numa.foreground_cores_per_node` defaults to 0,
    /// and `confine_cpu_sets` then returns the *same full engine set* for both
    /// halves — so removing them from the background alone would leave ublk
    /// and the LV2 sync threads still eligible to run there, and the
    /// reservation would mean nothing in exactly the shipped configuration.
    ///
    /// Errors rather than warns when the carve-out would empty either half. A
    /// config that starves other roles is a startup mistake, and the engine
    /// already refuses over-budget confine configs rather than limping (see
    /// `numa::setup_confine`).
    pub fn dedicate(
        &mut self,
        role: ThreadRole,
        cores: &[Vec<usize>],
        core_count: usize,
    ) -> crate::error::OnyxResult<()> {
        if core_count == 0 {
            return Ok(());
        }
        // Draw from the half this role would otherwise share, so a foreground
        // role (BufferSync) does not get cores carved out of the background
        // pool it never ran on. Only whole cores count: one straddling the
        // foreground/background boundary is not that half's to hand out.
        let own_half: std::collections::HashSet<usize> = if role_uses_foreground_set(role) {
            self.foreground.iter().copied().collect()
        } else {
            self.background.iter().copied().collect()
        };
        let candidates: Vec<&Vec<usize>> = cores
            .iter()
            .filter(|core| core.iter().all(|cpu| own_half.contains(cpu)))
            .collect();
        let taken: Vec<usize> = candidates
            .iter()
            .rev()
            .take(core_count)
            .flat_map(|core| core.iter().copied())
            .collect();
        let taken_set: std::collections::HashSet<usize> = taken.iter().copied().collect();
        let background_left = self
            .background
            .iter()
            .filter(|cpu| !taken_set.contains(cpu))
            .count();
        let foreground_left = self
            .foreground
            .iter()
            .filter(|cpu| !taken_set.contains(cpu))
            .count();
        if candidates.len() < core_count || background_left == 0 || foreground_left == 0 {
            return Err(crate::error::OnyxError::Config(format!(
                "cores.{}_dedicated_cores = {core_count} does not fit: this node offers \
                 {} whole core(s) in the half {role:?} shares, and the carve-out would \
                 leave {foreground_left} foreground / {background_left} shared-background \
                 CPU(s) (engine cpus {:?}). Lower it, lower another cores.* knob, or give \
                 cores back with numa.reserve_cores_per_node.",
                role_knob_name(role),
                candidates.len(),
                self.engine,
            )));
        }
        self.background.retain(|cpu| !taken_set.contains(cpu));
        self.foreground.retain(|cpu| !taken_set.contains(cpu));
        let mut taken = taken;
        taken.sort_unstable();
        self.dedicated[role.index()] = Some(taken);
        Ok(())
    }

    /// CPUs `role` may run on: its exclusive set if it has one, else the half
    /// it shares.
    pub fn cpus_for(&self, role: ThreadRole) -> &[usize] {
        if let Some(dedicated) = &self.dedicated[role.index()] {
            return dedicated;
        }
        if role_uses_foreground_set(role) {
            &self.foreground
        } else {
            &self.background
        }
    }

    /// CPUs a thread should be confined to, resolved from its `comm`.
    ///
    /// `None` means "leave this thread's affinity alone" — load generators and
    /// the direct-IO threads pin themselves deliberately.
    ///
    /// This is the enforcer's entry point, and it exists so the enforcer and
    /// [`bind_current`] cannot disagree. The fallback is the historical
    /// name-prefix rule, so a thread whose name maps to no role keeps landing
    /// exactly where it did before.
    pub fn cpus_for_thread_name(&self, name: &str) -> Option<&[usize]> {
        let name = name.trim_end();
        if thread_pins_itself(name) {
            return None;
        }
        // A role with CPUs of its own must be recognised here, or the enforcer
        // would widen it back to the shared half on the next sweep.
        if let Some(role) = role_for_thread_name(name) {
            if self.dedicated[role.index()].is_some() {
                return Some(self.cpus_for(role));
            }
        }
        Some(if name_uses_foreground_set(name) {
            &self.foreground
        } else {
            &self.background
        })
    }

    pub fn engine_cpus(&self) -> &[usize] {
        &self.engine
    }

    pub fn foreground_cpus(&self) -> &[usize] {
        &self.foreground
    }

    pub fn background_cpus(&self) -> &[usize] {
        &self.background
    }

    /// Roles holding exclusive CPUs, for the startup log.
    pub fn dedications(&self) -> Vec<(ThreadRole, &[usize])> {
        ALL_THREAD_ROLES
            .iter()
            .filter_map(|&role| {
                self.dedicated[role.index()]
                    .as_deref()
                    .map(|cpus| (role, cpus))
            })
            .collect()
    }
}

const ALL_THREAD_ROLES: [ThreadRole; ThreadRole::COUNT] = [
    ThreadRole::Ublk,
    ThreadRole::ReadPool,
    ThreadRole::BufferSync,
    ThreadRole::FlusherCoalesce,
    ThreadRole::FlusherDedup,
    ThreadRole::FlusherCompress,
    ThreadRole::Lv3Batch,
    ThreadRole::FlusherWriter,
    ThreadRole::FlusherCleanup,
    ThreadRole::CommitWorker,
    ThreadRole::FlusherPostCommit,
    ThreadRole::MetadbCheckpoint,
    ThreadRole::Background,
];

/// Config key stem for a role, used only in the `dedicate` error message.
fn role_knob_name(role: ThreadRole) -> &'static str {
    match role {
        ThreadRole::Lv3Batch => "lv3",
        ThreadRole::BufferSync => "lv2",
        other => {
            debug_assert!(false, "no cores.* knob defined for {other:?}");
            "unknown"
        }
    }
}

/// Threads whose affinity the engine must not touch: load generators pin
/// themselves to model a client, and the direct-IO frontend pins its own
/// workers from `service.direct_io_cpus`. Re-confining either would erase a
/// deliberate placement.
///
/// Shared by the confine budget and by partition mode's enforcer so the two
/// cannot drift on which threads are off-limits.
pub fn thread_pins_itself(name: &str) -> bool {
    let name = name.trim_end();
    name.starts_with("engine-bench-")
        || name.starts_with("engine-submit-")
        || name.starts_with("engine-durable-")
        || name.starts_with("direct-io-")
}

/// Map a thread's `comm` back to its role.
///
/// Only roles that can hold dedicated CPUs need an entry — everything else
/// falls through to [`name_uses_foreground_set`], which reproduces the
/// historical placement. ⚠ Adding a `cores.*_dedicated_cores` knob for a new
/// role REQUIRES adding its name prefixes here, or the stray-thread enforcer
/// will undo the pin on its next sweep.
///
/// ⚠ Linux truncates `comm` to 15 characters (`TASK_COMM_LEN`), so match on
/// prefixes short enough to survive it: `lv3-batch-aggregate` arrives as
/// `lv3-batch-aggre`.
fn role_for_thread_name(name: &str) -> Option<ThreadRole> {
    if name.starts_with("lv3-batch-") {
        return Some(ThreadRole::Lv3Batch);
    }
    // The LV2 commit-log sync threads: one `persistent-slot-sync-global`
    // coordinator plus `persistent-slot-sync-<shard>`, and the prepare/lane
    // threads inside the global loop bind under the same role.
    if name.starts_with("persistent-slot") {
        return Some(ThreadRole::BufferSync);
    }
    // chunklet's persistent write-execution pools take their CPU sets from
    // these same two roles (`chunklet_pool::uring_pool_config` reads
    // `role_cpu_set(BufferSync)` / `role_cpu_set(Lv3Batch)`), so the enforcer
    // has to resolve them the same way or it would fight chunklet's own
    // pinning every 5 s. With no dedication in play both fall through to the
    // historical foreground/background rule, unchanged.
    if name.starts_with("ckuring-bg-") {
        return Some(ThreadRole::Lv3Batch);
    }
    if name.starts_with("ckuring-fg-") {
        return Some(ThreadRole::BufferSync);
    }
    None
}

/// The historical foreground/background rule, by thread name. Kept verbatim
/// from `numa::confine_thread_placement` so the budget is a refactor and not a
/// behaviour change: only ublk and the LV2 sync threads are foreground, plus
/// chunklet's explicitly-tagged uring pools.
fn name_uses_foreground_set(name: &str) -> bool {
    if name.starts_with("ckuring-fg-") {
        return true;
    }
    if name.starts_with("ckuring-bg-") {
        return false;
    }
    name.starts_with("ublk-") || name.starts_with("persistent-slot")
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
    /// `[numa] mode = "confine"`: roles bind to a CPU *set* from the
    /// [`CoreBudget`] (the home node minus reserved cores, split into a
    /// foreground/background half plus any dedicated carve-outs), keeping
    /// scheduler freedom inside the set — the in-engine equivalent of
    /// `numactl --cpunodebind`, but it also covers libublk's per-queue
    /// threads because the per-thread `bind_current` runs after libublk's
    /// own affinity call and overrides it.
    Confine(CoreBudget),
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

/// The active layout, swappable so a core-budget A/B can run its arms inside
/// ONE engine process at one pool age instead of paying a restart per arm.
///
/// This used to be a `OnceLock<Option<LayoutKind>>`. The reason it can become
/// mutable safely is that nothing caches a CPU mask for long: `bind_current`
/// reads the layout when a thread spawns, and every already-running thread is
/// swept back onto its role's set every 5 s by
/// [`crate::numa::sweep_stray_threads`], whose test is EXACT set equality — so
/// a swap converges in both directions (a new dedication narrows masks, and
/// dropping one widens them again) within two sweeps.
///
/// ⚠ The one exception is `chunklet_pool::uring_pool_config`, which copies
/// `role_cpu_set()` into chunklet's uring execution pool when that pool is
/// built. Those CPUs do NOT follow a swap. `swap_confine` warns when the pool
/// is live; on the canonical config `chunklet_io_execution` is disabled and no
/// such pool exists.
static LAYOUT: OnceLock<ArcSwap<Option<LayoutKind>>> = OnceLock::new();

fn layout_cell() -> &'static ArcSwap<Option<LayoutKind>> {
    LAYOUT.get_or_init(|| ArcSwap::from_pointee(None))
}

pub fn init(config: &ThreadingConfig) {
    layout_cell().store(Arc::new(
        AffinityLayout::from_config(config).map(LayoutKind::PerRole),
    ));
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
pub fn init_confine(budget: CoreBudget) {
    layout_cell().store(Arc::new(Some(LayoutKind::Confine(budget))));
}

/// Replace the live confine budget. Already-running threads converge on the
/// new masks within two enforcer sweeps (~10 s); see [`LAYOUT`].
///
/// Refuses to act under any other layout: swapping a budget in while
/// `[threading]` per-role pinning or partition mode is active would silently
/// change which mechanism owns placement.
pub fn swap_confine(budget: CoreBudget) -> Result<(), &'static str> {
    if !is_confine_layout() {
        return Err("numa.mode is not \"confine\"; there is no core budget to swap");
    }
    layout_cell().store(Arc::new(Some(LayoutKind::Confine(budget))));
    Ok(())
}

/// Partition-mode layout (see `PartitionTopo`).
pub fn init_partition(topo: PartitionTopo) {
    layout_cell().store(Arc::new(Some(LayoutKind::Partition(topo))));
}

/// Return the complete CPU set assigned to a role by the active layout.
/// An empty vector means affinity is not configured and the caller should
/// inherit its creating thread's mask.
pub fn role_cpu_set(role: ThreadRole) -> Vec<usize> {
    let guard = layout_cell().load();
    let Some(layout) = guard.as_ref().as_ref() else {
        return Vec::new();
    };
    layout.cpu_set_for_role(role)
}

/// Whether the active runtime layout uses the strict foreground/background
/// confine split.
pub fn is_confine_layout() -> bool {
    matches!(
        layout_cell().load().as_ref().as_ref(),
        Some(LayoutKind::Confine(_))
    )
}

/// Run `f` against the active confine budget, or against `None` under any
/// other layout.
///
/// Closure-based rather than returning a reference: the budget now lives behind
/// an `ArcSwap`, and the caller must hold the guard for as long as it reads the
/// budget. The stray-thread enforcer takes it once and sweeps every task under
/// that one guard, so a swap mid-sweep cannot hand it two different budgets.
pub fn with_confine_budget<R>(f: impl FnOnce(Option<&CoreBudget>) -> R) -> R {
    let guard = layout_cell().load();
    match guard.as_ref().as_ref() {
        Some(LayoutKind::Confine(budget)) => f(Some(budget)),
        _ => f(None),
    }
}

pub fn bind_current(role: ThreadRole, ordinal: usize) {
    let guard = layout_cell().load();
    let Some(layout) = guard.as_ref().as_ref() else {
        return;
    };
    let result = match layout {
        LayoutKind::PerRole(layout) => {
            let Some(cpu) = layout.cpus_for(role).pick(ordinal) else {
                return;
            };
            set_current_cpus(&[cpu])
        }
        LayoutKind::Confine(budget) => set_current_cpus(budget.cpus_for(role)),
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
            Self::Confine(budget) => budget.cpus_for(role).to_vec(),
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
mod budget_tests {
    use super::*;

    /// 4 physical cores, SMT2, mirroring the box layout (even CPUs = node 0).
    fn cores4() -> Vec<Vec<usize>> {
        vec![vec![0, 8], vec![2, 10], vec![4, 12], vec![6, 14]]
    }

    fn shared_budget() -> CoreBudget {
        let engine: Vec<usize> = cores4().into_iter().flatten().collect();
        let mut engine = engine;
        engine.sort_unstable();
        CoreBudget::new(engine.clone(), engine.clone(), engine)
    }

    /// The budget became swappable so a core-budget A/B can keep its arms in
    /// one engine process. Two things have to hold for that to be safe, and
    /// both are asserted here in ONE test so the assertions cannot race each
    /// other through the process-global `LAYOUT`:
    ///
    /// 1. A swap is refused unless confine is the active layout — otherwise it
    ///    would silently take placement away from `[threading]` per-role
    ///    pinning or from partition mode.
    /// 2. The swap is visible in BOTH directions. Narrowing (adding a
    ///    dedication) is the easy case; widening matters just as much, because
    ///    an arm that returns to the baseline has to actually give the cores
    ///    back. The stray-thread enforcer converges on whatever
    ///    `cpus_for_thread_name` reports, so that is what this checks.
    ///
    /// ⚠ This is the only test that writes `LAYOUT`. It resets it at both ends;
    /// if a second test ever needs it, serialise them.
    #[test]
    fn a_live_budget_swap_is_refused_off_confine_and_visible_both_ways() {
        layout_cell().store(Arc::new(None));

        assert!(
            swap_confine(shared_budget()).is_err(),
            "swapping a budget in with no confine layout must be refused"
        );

        let all: &[usize] = &[0, 2, 4, 6, 8, 10, 12, 14];
        init_confine(shared_budget());
        assert_eq!(
            with_confine_budget(|b| b
                .expect("confine is active")
                .cpus_for_thread_name("persistent-slot")
                .map(<[usize]>::to_vec)),
            Some(all.to_vec())
        );

        // Narrow: LV2 takes a core off its own half.
        let mut narrowed = shared_budget();
        narrowed
            .dedicate(ThreadRole::BufferSync, &cores4(), 1)
            .expect("one core of four is dedicable");
        swap_confine(narrowed).expect("confine is active");
        let lv2_narrow = with_confine_budget(|b| {
            b.expect("confine is active")
                .cpus_for_thread_name("persistent-slot")
                .map(<[usize]>::to_vec)
        })
        .expect("LV2 resolves to a set");
        assert_eq!(
            lv2_narrow.len(),
            2,
            "one dedicated physical core is 2 logical CPUs, got {lv2_narrow:?}"
        );
        // And the cores really left the shared halves.
        let shared_after = with_confine_budget(|b| {
            b.expect("confine is active")
                .cpus_for_thread_name("flusher-coalesce-3")
                .map(<[usize]>::to_vec)
        })
        .expect("a shared role resolves to a set");
        assert!(
            lv2_narrow.iter().all(|cpu| !shared_after.contains(cpu)),
            "dedicated CPUs {lv2_narrow:?} still appear in the shared set {shared_after:?}"
        );

        // Widen again: dropping the dedication must hand the cores back, which
        // is what lets an arm return to its baseline.
        swap_confine(shared_budget()).expect("confine is active");
        assert_eq!(
            with_confine_budget(|b| b
                .expect("confine is active")
                .cpus_for_thread_name("persistent-slot")
                .map(<[usize]>::to_vec)),
            Some(all.to_vec()),
            "the swap back did not widen LV2 to the full engine set"
        );

        layout_cell().store(Arc::new(None));
    }

    /// `numa.foreground_cores_per_node = 0` makes both halves the full engine
    /// set, so with no dedications every role resolves to the same CPUs — this
    /// is the pre-budget behaviour the refactor must preserve.
    #[test]
    fn no_dedications_gives_every_role_the_whole_engine_set() {
        let budget = shared_budget();
        let all: &[usize] = &[0, 2, 4, 6, 8, 10, 12, 14];
        for role in ALL_THREAD_ROLES {
            assert_eq!(budget.cpus_for(role), all, "{role:?}");
        }
        assert!(budget.dedications().is_empty());
    }

    #[test]
    fn dedicate_takes_whole_cores_off_the_end_and_removes_them_from_background() {
        let mut budget = shared_budget();
        budget
            .dedicate(ThreadRole::Lv3Batch, &cores4(), 1)
            .expect("1 of 4 cores leaves 3 shared");

        // Highest core taken, both HT siblings together.
        assert_eq!(budget.cpus_for(ThreadRole::Lv3Batch), &[6, 14]);
        // Gone from the shared half, so nothing else can be scheduled there.
        assert_eq!(budget.background_cpus(), &[0, 2, 4, 8, 10, 12]);
        for cpu in [6, 14] {
            assert!(
                !budget.cpus_for(ThreadRole::FlusherWriter).contains(&cpu),
                "cpu {cpu} is dedicated and must not appear in a shared set"
            );
        }
        assert_eq!(
            budget.dedications().len(),
            1,
            "only Lv3Batch holds exclusive CPUs"
        );
    }

    /// The enforcer compares masks exactly and re-binds anything narrower, so
    /// a dedicated thread is only stable if its name resolves to the dedicated
    /// set. This is the assertion that would have caught the pin being undone
    /// every 5 seconds.
    #[test]
    fn enforcer_resolves_dedicated_threads_to_their_own_cores() {
        let mut budget = shared_budget();
        budget.dedicate(ThreadRole::Lv3Batch, &cores4(), 1).unwrap();

        // Both spawn-site names, including the one Linux truncates at 15 chars
        // ("lv3-batch-aggregate" -> "lv3-batch-aggre").
        for name in ["lv3-batch-exec-0\n", "lv3-batch-aggre\n"] {
            assert_eq!(
                budget.cpus_for_thread_name(name),
                Some(&[6usize, 14][..]),
                "{name:?} must sweep to the dedicated set, not the shared half"
            );
        }
        // A role without dedicated CPUs still lands on the shared half.
        assert_eq!(
            budget.cpus_for_thread_name("flusher-compress-1-0\n"),
            Some(&[0usize, 2, 4, 8, 10, 12][..])
        );
    }

    /// chunklet pins its own write-execution workers from the same two roles,
    /// so the enforcer must resolve them identically or the two will fight.
    #[test]
    fn chunklet_uring_pools_track_the_roles_they_borrow_cpus_from() {
        // No dedications: both fall through to the historical rule, which is
        // what makes this mapping safe to add on its own.
        let shared = shared_budget();
        let all: &[usize] = &[0, 2, 4, 6, 8, 10, 12, 14];
        assert_eq!(shared.cpus_for_thread_name("ckuring-bg-3\n"), Some(all));
        assert_eq!(shared.cpus_for_thread_name("ckuring-fg-3\n"), Some(all));

        // With Lv3Batch dedicated, the background uring pool follows it —
        // matching `chunklet_pool::uring_pool_config`'s
        // `role_cpu_set(Lv3Batch)`.
        let mut budget = shared_budget();
        budget.dedicate(ThreadRole::Lv3Batch, &cores4(), 1).unwrap();
        assert_eq!(
            budget.cpus_for_thread_name("ckuring-bg-3\n"),
            Some(&[6usize, 14][..])
        );
        // The foreground pool borrows BufferSync, which has no dedication —
        // but the dedicated CPUs are gone from the foreground half too, so it
        // cannot land on LV3's cores either.
        assert_eq!(
            budget.cpus_for_thread_name("ckuring-fg-3\n"),
            Some(&[0usize, 2, 4, 8, 10, 12][..])
        );
    }

    /// The reservation is only real if it holds against the FOREGROUND half as
    /// well. With `numa.foreground_cores_per_node = 0` both halves are the same
    /// full engine set, so a carve-out that only edited the background would
    /// leave ublk and the LV2 sync threads free to run on the dedicated cores —
    /// i.e. it would be a no-op in exactly the shipped configuration.
    #[test]
    fn dedicated_cores_leave_the_foreground_half_too() {
        let mut budget = shared_budget();
        assert_eq!(budget.foreground_cpus(), budget.background_cpus());
        budget.dedicate(ThreadRole::Lv3Batch, &cores4(), 1).unwrap();

        for cpu in [6, 14] {
            assert!(
                !budget.foreground_cpus().contains(&cpu),
                "cpu {cpu} is dedicated to LV3 and must not stay in the foreground half"
            );
        }
        // Concretely: ublk and LV2 sync can no longer be scheduled there.
        for role in [ThreadRole::Ublk, ThreadRole::BufferSync] {
            assert_eq!(budget.cpus_for(role), &[0, 2, 4, 8, 10, 12], "{role:?}");
        }
    }

    /// Every role named in `role_for_thread_name` must have a `cores.*` knob
    /// name, because `dedicate`'s error message quotes it.
    #[test]
    fn dedicated_capable_roles_have_a_knob_name() {
        assert_eq!(role_knob_name(ThreadRole::Lv3Batch), "lv3");
        assert_eq!(role_knob_name(ThreadRole::BufferSync), "lv2");
        assert_eq!(
            role_for_thread_name("lv3-batch-exec-3"),
            Some(ThreadRole::Lv3Batch)
        );
        for name in [
            "persistent-slot-sync-7",
            "persistent-slot-sync-global",
            "persistent-slot", // 15-char comm truncation
        ] {
            assert_eq!(
                role_for_thread_name(name),
                Some(ThreadRole::BufferSync),
                "{name:?}"
            );
        }
    }

    /// BufferSync is a FOREGROUND role, so its cores must come out of the
    /// foreground half — carving them from the background pool it never ran on
    /// would reserve the wrong CPUs and leave it sharing as before.
    #[test]
    fn dedicating_a_foreground_role_draws_from_the_foreground_half() {
        let engine: Vec<usize> = vec![0, 2, 4, 6, 8, 10, 12, 14];
        // Foreground owns cores {0,8} and {2,10}; background owns the rest.
        // Both halves sorted, as `NumaNode::confine_cpu_sets` returns them.
        let mut budget = CoreBudget::new(engine, vec![0, 2, 8, 10], vec![4, 6, 12, 14]);
        budget
            .dedicate(ThreadRole::BufferSync, &cores4(), 1)
            .unwrap();
        // The highest whole FOREGROUND core, not the highest core overall.
        assert_eq!(budget.cpus_for(ThreadRole::BufferSync), &[2, 10]);
        assert_eq!(budget.foreground_cpus(), &[0, 8]);
        // Background is untouched: the carve-out did not raid the other half.
        assert_eq!(budget.background_cpus(), &[4, 6, 12, 14]);
    }

    /// Both knobs at once: LV2 is dedicated first, so LV3 draws from what is
    /// left and the two sets stay disjoint.
    #[test]
    fn two_dedications_do_not_overlap() {
        let mut budget = shared_budget();
        budget
            .dedicate(ThreadRole::BufferSync, &cores4(), 1)
            .unwrap();
        budget.dedicate(ThreadRole::Lv3Batch, &cores4(), 1).unwrap();

        let lv2 = budget.cpus_for(ThreadRole::BufferSync).to_vec();
        let lv3 = budget.cpus_for(ThreadRole::Lv3Batch).to_vec();
        assert_eq!(lv2, vec![6, 14], "LV2 claims first, so it takes the top core");
        assert_eq!(lv3, vec![4, 12]);
        assert!(
            lv2.iter().all(|cpu| !lv3.contains(cpu)),
            "dedicated sets must be disjoint: lv2={lv2:?} lv3={lv3:?}"
        );
        // Neither set remains schedulable by anyone else.
        for cpu in lv2.iter().chain(lv3.iter()) {
            assert!(!budget.cpus_for(ThreadRole::FlusherWriter).contains(cpu));
            assert!(!budget.cpus_for(ThreadRole::Ublk).contains(cpu));
        }
        assert_eq!(budget.dedications().len(), 2);
    }

    #[test]
    fn dedicating_every_core_is_a_startup_error_not_a_warning() {
        let mut budget = shared_budget();
        // 4 whole cores exist; asking for all 4 would leave the shared half
        // empty, and so would asking for more.
        for ask in [4, 5, 99] {
            let mut b = budget.clone();
            assert!(
                matches!(
                    b.dedicate(ThreadRole::Lv3Batch, &cores4(), ask),
                    Err(crate::error::OnyxError::Config(_))
                ),
                "dedicating {ask} of 4 cores must be refused"
            );
        }
        // 3 of 4 is the most that can be granted.
        budget
            .dedicate(ThreadRole::Lv3Batch, &cores4(), 3)
            .expect("3 of 4 cores leaves 1 shared");
        assert_eq!(budget.background_cpus(), &[0, 8]);
    }

    #[test]
    fn dedicate_zero_is_a_no_op() {
        let mut budget = shared_budget();
        let before = budget.background_cpus().to_vec();
        budget.dedicate(ThreadRole::Lv3Batch, &cores4(), 0).unwrap();
        assert_eq!(budget.background_cpus(), before.as_slice());
        assert!(budget.dedications().is_empty());
    }

    /// A core straddling the foreground boundary is not the background half's
    /// to hand out.
    #[test]
    fn dedicate_only_takes_cores_wholly_inside_the_background_half() {
        let engine: Vec<usize> = vec![0, 2, 4, 6, 8, 10, 12, 14];
        // Foreground owns core {6,14}; background owns the other three.
        let mut budget = CoreBudget::new(engine, vec![6, 14], vec![0, 2, 4, 8, 10, 12]);
        budget.dedicate(ThreadRole::Lv3Batch, &cores4(), 1).unwrap();
        // {6,14} is foreground, so the highest *background* core is taken.
        assert_eq!(budget.cpus_for(ThreadRole::Lv3Batch), &[4, 12]);
        assert_eq!(budget.foreground_cpus(), &[6, 14]);
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

        let confine = LayoutKind::Confine(CoreBudget::new(
            vec![0, 2, 4, 6],
            vec![0, 2],
            vec![4, 6],
        ));
        assert_eq!(confine.cpu_set_for_role(ThreadRole::Ublk), vec![0, 2]);
        assert_eq!(confine.cpu_set_for_role(ThreadRole::BufferSync), vec![0, 2]);
        assert_eq!(confine.cpu_set_for_role(ThreadRole::Background), vec![4, 6]);
        assert_eq!(confine.cpu_set_for_role(ThreadRole::Lv3Batch), vec![4, 6]);

        // Same layout, but Lv3Batch now owns core {6}: it leaves the shared
        // background set and nothing else follows it there.
        let mut dedicated_budget = CoreBudget::new(vec![0, 2, 4, 6], vec![0, 2], vec![4, 6]);
        dedicated_budget
            .dedicate(ThreadRole::Lv3Batch, &[vec![0], vec![2], vec![4], vec![6]], 1)
            .unwrap();
        let confine = LayoutKind::Confine(dedicated_budget);
        assert_eq!(confine.cpu_set_for_role(ThreadRole::Lv3Batch), vec![6]);
        assert_eq!(confine.cpu_set_for_role(ThreadRole::Background), vec![4]);
        assert_eq!(confine.cpu_set_for_role(ThreadRole::Ublk), vec![0, 2]);

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
