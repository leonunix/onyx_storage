# Threading / NUMA Layout for nvme-box

This document captures the threading-config / NUMA-affinity findings from
the H2 apply-lane investigation (see `docs/metadb-nvme-drain-plan.md`).
It is the reference for any future perf testing on this hardware: read
this before tuning thread CPU sets, before adding new lane roles, and
before drawing conclusions from a per-shard latency table.

## TL;DR

- nvme-box's 96 logical CPUs are **interleaved** across 2 NUMA nodes:
  node 0 = even CPUs, node 1 = odd CPUs.
- Any threading-config CPU range that spans both nodes (e.g. the
  classic `64-79`) splits a per-role lane group across NUMA nodes via
  the round-robin `cpus[ordinal % len]` mapping. For metadb apply
  lanes this caused a **3.51× exec-time penalty** on cross-NUMA shards.
- Splitting work across NUMA nodes for I/O paths only helps when the
  *data* the threads touch is also split. metadb's PageCache is a
  single shared structure, so all threads that read it benefit from
  being NUMA-local with the apply lanes that pin its working set.
- The current best layout is **`config/nvme-detailed-numa-split.toml`**
  ("v4"). It pins all metadb apply lanes + flusher_writer/cleanup +
  background to node 0 (even CPUs 48+), keeps ublk + read_pool on
  mixed-NUMA `0-31` / `32-47` so reads don't pay an unconditional
  cross-NUMA tax, and assigns flusher upstream stages (coalesce / dedup
  / compress / buffer_sync) to node 1 odd CPUs that do not overlap
  with anything else.
- Per-shard apply-lane metrics (`metadb_apply_lane_l2p` /
  `metadb_apply_lane_rc` text rows; `MetaMemorySnapshot.*_apply_lane_shard_*`
  vectors over the socket) are the diagnostic that surfaces this kind
  of problem. **Aggregate averages alone hide NUMA bias entirely** —
  the original 16-shard L2P aggregate looked healthy until per-shard
  data showed half the lanes running 3.5× slower than the other half.

## Hardware

```
$ cat /sys/devices/system/node/node0/cpulist
0,2,4,6,8,...,94    (48 CPUs)
$ cat /sys/devices/system/node/node1/cpulist
1,3,5,7,9,...,95    (48 CPUs)
```

Two Intel Xeon Gold 5318Y sockets, 24 physical cores each, HT enabled
→ 48 logical CPUs per node. Memory channels are NUMA-bound to their
socket. Cross-socket access costs ~80 ns per cache line vs ~30 ns
NUMA-local; under sustained traffic the effective slowdown observed
in metadb apply was 3-4×.

## Diagnostic Methodology

The H2 work added per-task timing metrics on the metadb L2P /
refcount apply lanes, both aggregated across shards and broken down
per shard ordinal:

```
metadb_apply_lane_l2p: tasks=375848 queue_depth_max=16
                      queue_wait_us=...  exec_us=...  idle_us=...
metadb_apply_lane_rc:  tasks=444366 queue_depth_max=15
                      queue_wait_us=...  exec_us=...  idle_us=...
                      pending_set_wait_us=...
```

Per-shard arrays (`MetaMemorySnapshot.l2p_apply_lane_shard_*`,
`MetaMemorySnapshot.rc_apply_lane_shard_*`) show a row per shard
ordinal so a single hot/slow lane stands out against its peers. In
practice the reading order is:

1. Aggregate `queue_wait_us` >> `exec_us` → lane is queueing not
   computing. Look at per-shard data.
2. Per-shard `tasks` count CV ≈ 0 → load is balanced (the hash router
   is fine). Look at per-shard `exec_us` distribution instead.
3. Per-shard `exec_us` shows a stable bimodal split (e.g. odd-ordinal
   shards consistently slower than even-ordinal) → NUMA / affinity
   issue. Map ordinal → CPU via `cpus[ordinal % len]` against
   `/sys/devices/system/node/*/cpulist` to confirm.

Compare scripts in `/tmp` (not checked in but used during the
investigation): `summarize_run_h2_pershard.py`, `compare_h2_runs.py`,
`compare_fio_lat.py`.

## What Was Tried (and What Failed)

The j8/iodepth=4/3-min `randrw 50/50, dedupe=0, compress=15%` workload
was the test bed. All numbers below are headline `fio_total_iops` and
the most informative metadb fields; full per-shard breakdowns are in
`.dev/fio-dedupe-compress-soak/20260508T0*Z-h2-*` on nvme-box.

| layout / change | fio iops | meta_lbas/s | commit_max | NUMA odd/even | verdict |
|---|---|---|---|---|---|
| baseline (16 shard, original CPU sets) | 55,276 | 42,059 | 13.88s | **3.51** | per-shard data revealed NUMA bias |
| metadb apply CPUs → all-even node 0 only (no other changes) | 48,019 | 77,737 | 4.29s | 0.89 | NUMA fixed but L2P CPUs collided with flusher_writer/cleanup, fio dropped |
| 32 shards (16 → 32, original CPU layout) | 45,557 | 38,801 | 14.12s | 0.27 | Doubling shards halves per-shard task rate but RC still fans out to all shards every commit; CPU oversub got worse |
| numa-split v1 (writer on 4 node-0 CPUs, ublk fully on node 1) | 77,696* | 8,821 | 9.02s | 1.05 | front-end flew, backend starved (writer at 4× oversub couldn't drain). pending climbed to 6 M |
| numa-split v2 (writer 12 CPUs, ublk fully on node 1) | 37,901 | 65,517 | 1.42s | 0.99 | back-end great, but ublk → metadb lookup was 100% cross-NUMA → read p99.9 doubled |
| numa-split v3 (v2 + ublk 24 odd CPUs) | 52,874 | 61,299 | 3.01s | 1.05 | partial recovery; READ p99 still 4.9 ms (vs baseline 3.3 ms) |
| **numa-split v4** (ublk back to mixed `0-31`, metadb apply on 48+ even) | **56,602** | **81,089** | **4.82s** | **1.00** | drain rate +93%, commit tail -65%, READ p99 -7%, NUMA gone |

*v1's high fio reading is misleading: the buffer was filling up at
6 M pending entries because the writer couldn't keep up. Sustained
load would have hit the buffer ceiling and stalled.

Lessons crystallized along the way:

1. **Don't pin lane roles to a CPU range that spans NUMA nodes.** The
   `cpus[ordinal % len]` round-robin will alternate nodes for
   sequential ordinals, and any per-shard metric will show a
   matching even/odd performance split.
2. **Don't pin lane workers and the threads that hand them work to
   different NUMA nodes.** The original config had ublk on `0-31`
   (mixed) and metadb apply on `64-79` (mixed) — half the
   ublk→metadb traffic was already cross-NUMA but it was masked by
   the 3.5× variance from (1). Once (1) was fixed, putting ublk
   *fully* on the opposite node made every single read pay the
   cross-NUMA tax (v2/v3 read p99.9 jumped from 18 ms to 35 ms).
3. **Adding shards is only useful if work fans out across them.**
   L2P partition shards are useful: each commit only touches the
   shards its volume's lba set hashes to. Refcount shards are global:
   every commit fans rc actions to all of them, so doubling shard
   count doesn't reduce per-shard pressure.
4. **Don't trust aggregate lane metrics for distribution claims.**
   16-lane aggregate `queue_wait_avg=11ms` looked uniform until
   per-shard showed it was an even-vs-odd 3.5× split.

## Final Layout (`config/nvme-detailed-numa-split.toml`)

```
node 0 (even CPUs)                     node 1 (odd CPUs)
================                       ================
ublk         0,2,...,30 (ub./read_pool ublk        1,3,...,31
read_pool    32,34,...,46  share these)read_pool   33,35,...,47
                                       buffer_sync 49,51,53,55
metadb_l2p_apply       48-62 even (8)  flusher_coalesce 57,59,61,63
metadb_refcount_apply  64,66,68,70 (4) flusher_dedup    65-79 odd (8)
metadb_dedup_apply     72,74      (2)  flusher_compress 81-95 odd (8)
metadb_wal             76         (1)
metadb_checkpoint      78         (1)
flusher_writer         80,82,84,86 (4)
flusher_cleanup        88,90      (2)
background             92,94      (2)
```

Rationale per role:

- **ublk + read_pool on mixed NUMA `0-31` / `32-47`**: keeps the
  original baseline behavior so ublk threads have a 50% chance of
  being NUMA-local with metadb's PageCache on a blockmap lookup.
  Going single-node here was the v3 regression source.
- **metadb apply on node-0 even CPUs 48+**: zero overlap with
  ublk/read_pool ranges, every metadb apply lane on node 0, no
  `cpus[ordinal % len]` interleaving across nodes. Eliminates the
  3.5× odd/even bias.
- **flusher_writer + cleanup on node 0**: writer calls `commit_ops`
  which mutates metadb state. cleanup runs the dedup-cleanup
  read-back path on metadb. Both benefit from being NUMA-local
  with the apply lanes' PageCache.
- **flusher upstream (coalesce / dedup / compress / buffer_sync)
  on node 1 odd CPUs**: these stages produce data that flows into
  the writer; cross-NUMA on the inter-stage handoff is one-time
  per-unit cost, much cheaper than cross-NUMA on every metadb page
  access.
- **No role overlap on either node** (modulo the deliberate
  ublk/read_pool sharing of `0-47`): each lane group has dedicated
  CPUs, so the kernel scheduler isn't shuffling unrelated roles
  through the same time-slice.

### Tier-1 additions (drainer / compactor / commit_worker)

Three role knobs were introduced for the "smoothness" Tier-1 work
(`/root/.claude/plans/ticklish-sparking-barto.md`). When the v4 layout
is updated to set them, place all three on **node 0** alongside the
metadb apply lanes — they all read/write the same PageCache state:

- **`metadb_refcount_drainer_cpus`** — one drainer thread per refcount
  shard (16 with the default `shards_per_partition=16`). Pin to ~4
  even-numbered CPUs on node 0 next to `metadb_refcount_apply` (e.g.
  `64,66,68,70` if shared, or a new ~`72,74,76,78`-style block when
  carved out). The `cpus[ordinal % len]` rotation distributes the 16
  drainers evenly. Leaving this empty is functional but the kernel can
  starve drainers on a busy box — the symptom is rising
  `flush_sample_breakdown.rc_drain_max_us` and an in-gate priority-1
  fallback.
- **`metadb_l2p_compactor_cpus`** — single serial thread. 1–2 CPUs on
  node 0 next to `metadb_l2p_apply` (e.g. piggyback on
  `48-62`). Pinning here removes the apply-lane exec-max tail spike
  observed when the kernel co-locates the compactor on a busy apply CPU
  during a flush. The compactor is intentionally serial
  (`metadb/src/db/l2p_compactor.rs:191-201` documents the regression
  the parallel version triggered), so more than ~2 CPUs is wasted.
- **`commit_worker_cpus`** — the per-volume onyx commit workers (16
  channels, `hash(vol_id) % NUM_COMMIT_WORKERS`). Each commit_worker
  calls `tx.commit_with_outcomes`, so it should share a NUMA node with
  `metadb_l2p_apply` / `metadb_refcount_apply`. The pre-Tier-1
  placement re-used `flusher_writer_cpus` (~4 CPUs on node 0), which is
  acceptable but causes the commit-worker threads to contend with
  alloc/IO threads on the same cores. A dedicated 4-CPU set on node 0
  is preferred. When `commit_worker_cpus` is empty, the binding
  silently falls back to `flusher_writer_cpus` so legacy configs do not
  silently regress to OS scheduling.

The drainer + compactor binds happen inside metadb
(`metadb/src/refcount/shard.rs::DrainerWorker` and
`metadb/src/db/l2p_compactor.rs::run_worker`); the commit_worker bind
happens in `src/buffer/flush/mod.rs` next to the existing
`FlusherWriter` bind. All three are gated on `threading.enabled = true`
in the onyx config — empty strings inherit the OS default with no
behavioural change vs. the pre-Tier-1 build.

## Caveats / What to Verify When Workload Changes

The v4 layout is validated on **`dedupe_percentage=0`,
`buffer_compress_percentage=15`** (the lightest flusher load). For
other workloads:

- **dedup-enabled (`dedupe_percentage > 0`)**: `flusher_dedup` (8 odd
  CPUs for 32 dedup workers) and `metadb_dedup_apply` (2 even CPUs
  for 8 dedup apply lanes, 4× oversub) become real CPU consumers.
  Before drawing conclusions, run with the target dedupe ratio and
  re-check per-shard `dedup_apply_lane_*` data — those metrics are
  not yet broken out per shard (only L2P / RC are; see
  `metadb/src/metrics.rs`).
- **higher compression ratio**: `flusher_compress` (8 odd CPUs for
  32 compress workers) gets busier. The current 4× oversub may need
  to drop, possibly stealing CPUs from `flusher_dedup`.
- **stronger fio rate** (e.g. `--rate-iops` raised, or removed): the
  ublk pool is at 4× oversub on `0-31` (32 CPUs for 128 threads).
  Higher offered load may saturate it; if so, expand to `0-47`
  (24 even / 24 odd) at the cost of stealing from read_pool.
- **multi-volume**: per-volume L2P apply lanes share the same shard
  ordinal → the same metric slot. Per-shard counters then aggregate
  across volumes, which is fine for capacity but hides per-volume
  hotspots. A multi-volume workload should sample per-volume metrics
  (`volumes-json` socket cmd) to disambiguate.

## Heuristics for Future Tuning

1. **Always read the per-shard table first**, not just aggregates.
   Look for stable bimodal patterns or single-shard outliers.
2. **Map ordinal → CPU → NUMA node by hand** for any new lane role.
   `cpus[ordinal % len]` is the routing rule. If the CPU range
   crosses nodes, expect to see a matching performance split.
3. **Threads that share a memory structure should share a NUMA node.**
   Specifically: anyone touching the metadb PageCache (apply lanes,
   commit_ops via writer/cleanup, blockmap lookups via ublk and
   read_pool) wants to be on node 0 — and if not, at least
   *partially* on node 0 to keep the average tax down.
4. **Don't fix one path at the cost of another.** v2/v3 fixed write
   tail dramatically while doubling read p99.9. Any "win" that moves
   tail from one workload mix to another should be flagged as a
   regression for the moved-from mix.
5. **Aggregate `commit_total_max_us` is a great single number for
   tail health.** Before / after this work it dropped from 13.88s to
   4.82s on the same workload — a much sharper signal than fio iops
   alone, which moved less than the natural run-to-run variance.
6. **Don't over-pin frontend roles.** Frontend (ublk, read_pool)
   benefits more from latency-friendly mixed-NUMA placement than
   from NUMA-clean exclusivity. The opposite is true for backend
   (metadb apply, writer) where cache locality dominates.
7. **Distinguish "shared internal state" from "external dependency"
   when placing modules.** metadb is onyx's shared internal state:
   ublk / read_pool / writer / cleanup / dedup-cleanup all read or
   mutate the same PageCache through high-frequency small-cache-line
   accesses. Anything in this set must share a NUMA node with the
   PageCache. Future modules like `chunklet` (RAID / block IO
   orchestration, integrated through a `BlockBackend` boundary in
   Phase 8) are **external dependencies**: they own their own state,
   the only cross-module traffic is per-IO data buffers and a
   completion notification. Cross-NUMA cost there scales with IO
   count, not with cache-line count, and is dominated by NVMe
   submit/complete latency. Such modules can — and should — live
   on the *opposite* NUMA node so the otherwise-idle hardware does
   real work. The intended Phase 8 layout is:

   ```
   node 0: engine + metadb (high-freq shared state, NUMA-co-located)
       ublk / read_pool / buffer / flusher_writer / metadb apply /
       WAL / PageCache
   node 1: chunklet (independent module with its own state)
       RAID encode / chunklet allocator / io_uring submit /
       completion handling
   ```

   When chunklet lands, the boundary `flusher_writer (node 0) →
   chunklet.write_at` should batch payloads so the per-IO cross-NUMA
   buffer pass amortizes, and chunklet's completion callbacks should
   route back via channel rather than inline calls into metadb.
