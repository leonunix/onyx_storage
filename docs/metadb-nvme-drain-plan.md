# Metadb NVMe Drain Plan

This plan tracks the next performance investigation after the refcount drainer
correctness fix and the LV3 writer batching experiments.

The goal is not to make foreground writes look faster by moving work into an
unbounded backlog. The goal is to raise the real drain rate of the full backend:

```text
buffer entries -> LV3 writes -> metadb commit/apply -> metadb writeback/checkpoint
```

Metadb is on NVMe too, so it should be driven like an NVMe subsystem: large
batches, high enough queue depth, parallel lanes where safe, and explicit
backpressure when the real drain rate cannot keep up.

## Current Data Point

Workload:

```text
jobs=8
iodepth=4
randrw 50/50
bs=4k-32k
dedupe=0
buffer_compress=15
refcount_drainer_enabled=true
runtime=180s
```

Baseline:

```text
run_dir: /root/onyx_storage/.dev/fio-dedupe-compress-soak/20260507T090120Z-pipeline-batch-metrics-j8-3m
fio_total_iops         71.7k
zone_write_lbas        157.8k/s
flush_writer_meta_lbas 112.1k/s
pending_final          1.68M
flush_writer_io_core   7.33
metadb_commit_core     3.71
```

Best candidate so far:

```text
run_dir: /root/onyx_storage/.dev/fio-dedupe-compress-soak/20260507T103126Z-slab-coalesce250-j8-3m
change: write-only io_uring slab + writer 250us batch coalesce
fio_total_iops         70.2k
zone_write_lbas        154.6k/s
flush_writer_meta_lbas 133.2k/s
pending_final          0.79M
flush_writer_io_core   4.67
metadb_commit_core     5.05
```

Negative experiments:

```text
single global write owner / write_pool:
  flush_writer_meta_lbas 39.5k/s
  pending_final          5.54M
  conclusion: large batch with lost writer parallelism is wrong

writer coalesce 100us:
  flush_writer_meta_lbas 93.5k/s
  conclusion: too fragmented

writer coalesce 1ms:
  flush_writer_meta_lbas 78.6k/s
  conclusion: also fragmented / bad scheduling interaction
```

## Non-Goals

- Do not call a change a win if it only moves work behind a dirty backlog.
- Do not optimize foreground acceptance while metadb drain rate falls.
- Do not hide checkpoint/writeback debt without bounded watermarks and measured
  catch-up.
- Do not serialize all writer lanes through one owner thread just to get a
  large submit batch.

## Working Principles

- Real win means higher sustained `flush_writer_meta_lbas/s` and lower or stable
  pending entries at the same foreground workload.
- Metadb writeback is acceptable only if it increases real batch size, IO depth,
  or overlap. It is not acceptable as a pure debt deferral mechanism.
- Every async queue needs a measured service rate and a hard watermark.
- The best next change should preserve writer lane parallelism.
- Keep foreground read latency visible, because backend flush and reads share
  CPU, LV3, and metadb resources even when code paths differ.

## 2026-05-09 Correctness Gate: LV2/LV3 Device Isolation

Root cause found during the dedup mixed-workload failures on `nvme-box`:

```text
bad config:
  buffer.device        = /dev/md/onyx-lv2
  storage.data_device  = /dev/nvme2n1

actual topology:
  /dev/md/onyx-lv2 = raid0(nvme1n1, nvme6n1, nvme2n1, nvme7n1)
```

So LV2 buffer log writes and LV3 data writes were sharing the same physical
NVMe namespace. The failure signatures looked like metadb/dedup races because
the metadata still pointed at a valid unit:

```text
mapping_crc == last_lv3_write_payload_crc
actual_crc  != mapping_crc
allocator duplicate tracker: no duplicate allocation
dedup_verify_mismatches: 0
read_pool disabled + syscall write serial: still failed
```

But the raw bytes were later overwritten by LV2 RAID traffic. This also
explains the intermittent buffer hydration parse errors seen in the same runs.

Validation after moving LV3 to a non-LV2 member disk:

```text
config: storage.data_device = /dev/nvme4n1

serial/syscall diagnostic run:
  run_dir                .dev/fio-dedupe-compress-soak/codex-dedup-mixed-isolated-lv3-90s
  fio_returncode         0
  read_crc_errors        0
  dedup_verify_mismatches 0
  flush_errors           0

default uring/read_pool run:
  run_dir                .dev/fio-dedupe-compress-soak/codex-dedup-mixed-fixed-default-90s
  fio_returncode         0
  read_crc_errors        0
  dedup_verify_mismatches 0
  flush_errors           0
```

Guardrail added: Linux full-mode startup now rejects configurations where
`storage.data_device` and `buffer.device` are the same block device or overlap
through sysfs holder/slave topology. The old `/dev/nvme2n1` config now fails
before opening metadb:

```text
storage.data_device (/dev/nvme2n1) overlaps buffer.device (/dev/md/onyx-lv2)
through block-device holder/slave topology; LV2 and LV3 must not share
physical devices
```

## Hypotheses

### H1: Metadb Commit Fixed Cost Dominates

Symptom:

```text
small writer batches -> many metadb commits -> high commit_core
flush_writer_meta_lbas/cycle drops
```

Evidence so far:

```text
slab-only:
  drained/cycle          165
  meta_lbas/cycle        707
  metadb_commit_core     5.86
  fio_total_iops         47.8k

slab + 250us:
  drained/cycle          396
  meta_lbas/cycle        1700
  metadb_commit_core     5.05
  fio_total_iops         70.2k
```

Metrics to add or confirm:

```text
metadb commit count per second
metadb commit lbas per tx
metadb commit bytes per tx
commit apply lane wait
commit apply lane run
commit WAL append
commit WAL fsync / durability wait
commit dirty page publish
```

Experiments:

```text
1. Sweep writer coalesce around 200us, 250us, 400us, 600us.
2. Add direct metadb tx size histogram.
3. Try commit aggregation at the metadb adapter boundary, preserving ordering.
```

Win condition:

```text
flush_writer_meta_lbas/s increases
pending_final decreases or stays bounded
fio_total_iops does not materially regress
metadb commit count/s decreases while lbas/tx increases
```

2026-05-09 codex bounded commit coalesce on nvme-box:

```text
code: commit worker has configurable commit_target_lbas_per_tx,
      commit_coalesce_lba_budget, commit_coalesce_timeout_us.
      Adjacent same-volume passthrough jobs can be merged while preserving
      packed/different-volume FIFO boundaries; done/mark_flushed route back
      to each unit's original buffer shard.

workload: fio pure randwrite, jobs=8, iodepth=4, rate_iops=12500/job,
          bs=4k-32k, dedup disabled, compression=lz4, runtime=180s,
          config derived from config/N1-nodedup.toml on nvme-box.

old nodedup-N1:
  fio_write_iops         45.1k
  flush_writer_meta_lbas 32.2k/s
  pending_final          6.24M
  commits/s              1.59k
  lbas/commit            20.2
  l2p_lock_hold_core     0.89

new code, coalesce disabled (commit_coalesce_lba_budget=0):
  run_dir                .dev/fio-dedupe-compress-soak/codex-N1-nodedup-nocoalesce-3m
  fio_write_iops         48.3k
  flush_writer_meta_lbas 37.9k/s
  pending_final          6.52M
  commits/s              1.85k
  lbas/commit            20.5
  l2p_lock_hold_core     0.89
  verdict: safe baseline; slightly better drain, no foreground regression.

new code, coalesce budget=600, timeout=50us:
  run_dir                .dev/fio-dedupe-compress-soak/codex-N1-nodedup-coalesce-3m
  fio_write_iops         23.2k
  flush_writer_meta_lbas 53.5k/s
  pending_final          1.94M
  commits/s              2.17k
  lbas/commit            24.7
  l2p_lock_hold_core     0.88
  verdict: improves backend drain but halves foreground acceptance; not a
           default. Keep as an explicit experiment knob only.
```

Default after this run: coalesce disabled (`commit_coalesce_lba_budget=0`,
`commit_coalesce_timeout_us=0`). Next useful sweep is tiny try-drain only:
budget 150/300 with timeout 0, then optionally 5-10us. Avoid 50us as default.

Follow-up timeout=0 sweep:

```text
All rows use the same nodedup pure-randwrite workload as above.
timeout=0 means the commit worker never waits; it only drains jobs that are
already queued.

no coalesce:
  run_dir                .dev/fio-dedupe-compress-soak/codex-N1-nodedup-nocoalesce-3m
  fio_write_iops         48.3k
  flush_writer_meta_lbas 37.9k/s
  pending_final          6.52M
  commits/s              1.85k
  lbas/commit            20.5

budget=150, timeout=0:
  run_dir                .dev/fio-dedupe-compress-soak/codex-N1-nodedup-trycoalesce-b150-t0-3m
  fio_write_iops         42.9k
  flush_writer_meta_lbas 49.7k/s
  pending_final          5.25M
  commits/s              2.23k
  lbas/commit            22.2

budget=300, timeout=0:
  run_dir                .dev/fio-dedupe-compress-soak/codex-N1-nodedup-trycoalesce-b300-t0-3m
  fio_write_iops         47.6k
  flush_writer_meta_lbas 46.5k/s
  pending_final          6.10M
  commits/s              2.19k
  lbas/commit            21.2
  verdict: best balance in this sweep; near no-coalesce foreground rate with
           +23% metadb drain.

budget=600, timeout=0:
  run_dir                .dev/fio-dedupe-compress-soak/codex-N1-nodedup-trycoalesce-b600-t0-3m
  fio_write_iops         43.8k
  flush_writer_meta_lbas 51.7k/s
  pending_final          5.32M
  commits/s              2.32k
  lbas/commit            22.3
```

Mixed randrw 50/50 check:

```text
budget=300, timeout=0:
  run_dir                .dev/fio-dedupe-compress-soak/codex-N1-nodedup-mixed-b300-t0-3m
  fio_total_iops         22.4k
  fio_write_iops         11.2k
  flush_writer_meta_lbas 45.6k/s
  pending_final          155k
  verdict: backend drain is excellent, but foreground mixed IOPS collapses.

no coalesce:
  run_dir                .dev/fio-dedupe-compress-soak/codex-N1-nodedup-mixed-nocoalesce-3m
  fio_total_iops         57.5k
  fio_write_iops         28.7k
  flush_writer_meta_lbas 14.3k/s
  pending_final          4.38M
```

Conclusion: `budget=300, timeout=0` is a purewrite drain experiment, not a
general nvme default. Mixed workload needs pressure-gated coalesce or a
write-only/background-only trigger. Keep default and nvme config at 0/0 for now.

2026-05-09 dedup pressure gate on isolated LV3:

```text
workload:
  fio dedupe/compress mixed randrw 50/50
  jobs=8, iodepth=4, rate_iops=12500/job
  bs=4k-32k, compression=lz4, dedup enabled, runtime=90s
  config base = config/nvme-detailed-numa-split.toml
  LV2=/dev/md/onyx-lv2, LV3=/dev/nvme4n1

baseline, pending_skip_threshold_entries=0:
  run_dir                .dev/fio-dedupe-compress-soak/codex-dedup-mixed-fixed-default-90s
  fio_read_iops          34.8k
  fio_write_iops         34.8k
  write_p99/p99.9        2.3ms / 10.3ms
  flush_writer_meta_lbas 18.2k/s
  coalesced_lbas         30.3k/s
  pending_final          2.35M

pending_skip_threshold_entries=4096:
  run_dir                .dev/fio-dedupe-compress-soak/codex-dedup-pending-skip-4096-90s
  fio_read_iops          36.7k
  fio_write_iops         36.7k
  write_p99/p99.9        1.7ms / 4.6ms
  flush_writer_meta_lbas 38.0k/s
  coalesced_lbas         40.0k/s
  pending_final          2.30M

pending_skip_threshold_entries=8192:
  run_dir                .dev/fio-dedupe-compress-soak/codex-dedup-pending-skip-8192-90s
  fio_read_iops          23.1k
  fio_write_iops         23.1k
  write_p99/p99.9        7.5ms / 27.9ms
  flush_writer_meta_lbas 44.1k/s
  pending_final          1.09M
  verdict: backend looks best because foreground accepted less work; do not
           treat this single run as the default target.

pending_skip_threshold_entries=8192, repeat:
  run_dir                .dev/fio-dedupe-compress-soak/codex-dedup-pending-skip-8192-r2-90s
  fio_read_iops          36.9k
  fio_write_iops         36.9k
  flush_writer_meta_lbas 37.5k/s
  pending_final          2.32M
  verdict: 8192 is not reliably better than 16384 once foreground stays active.

pending_skip_threshold_entries=16384:
  run_dir                .dev/fio-dedupe-compress-soak/codex-dedup-pending-skip-16384-90s
  fio_read_iops          36.7k
  fio_write_iops         36.7k
  write_p99/p99.9        1.8ms / 5.5ms
  flush_writer_meta_lbas 39.3k/s
  coalesced_lbas         41.6k/s
  pending_final          2.27M
  verdict: best stable mixed-workload default in this sweep. It roughly
           doubles backend drain with slightly better foreground tails than
           baseline.

pending_skip_threshold_entries=16384, writer_read_active_batch_size=1024:
  run_dir                .dev/fio-dedupe-compress-soak/codex-dedup-skip16384-wrab1024-90s
  fio_read_iops          32.2k
  fio_write_iops         32.3k
  write_p99/p99.9        3.4ms / 12.6ms
  flush_writer_meta_lbas 42.2k/s
  pending_final          1.86M
  verdict: useful emergency drain knob, but not the latency-safe default.

pending_skip_threshold_entries=16384, writer_read_active_batch_size=768:
  run_dir                .dev/fio-dedupe-compress-soak/codex-dedup-skip16384-wrab768-90s
  fio_read_iops          7.4k
  fio_write_iops         7.4k
  write_p99/p99.9        22.9ms / 90.7ms
  verdict: rejected; non-linear bad queue/commit shape.
```

Default after this sweep:

```toml
[dedup]
pending_skip_threshold_entries = 16384

[flush]
writer_read_active_batch_size = 512
commit_coalesce_lba_budget = 0
commit_coalesce_timeout_us = 0
```

Correctness for all rows above:

```text
fio_returncode            0
read_crc_errors           0
dedup_verify_mismatches   0
flush_errors              0
```

2026-05-09 payload-cache pressure A/B:

Hypothesis: the foreground slowdown is not captured by
`buffer_backpressure_events`, because that counter is not the durable payload
cache pressure signal. With `max_memory_mb=32768`, the payload cache reaches
its ceiling, starts evicting ready payloads, and both foreground reads and the
coalescer rehydrate payloads from the LV2 buffer log.

Instrumentation added:

```text
buffer_payload_cache_evict_entries / bytes
buffer_coalesce_hydrate_{memory,volatile,disk}_entries
buffer_coalesce_hydrate_ns / ops
```

Same 90s mixed dedupe/compress workload as above:

```text
max_memory_mb=32768:
  run_dir                     .dev/fio-dedupe-compress-soak/codex-mem32-instrumented-90s
  fio_read_iops/write_iops    35.4k / 35.4k
  write_p99/p99.9             1.65ms / 5.15ms
  flush_writer_meta_lbas      28.9k/s
  coalesced_lbas              30.9k/s
  payload_cache_final         32.0 GiB / 32.0 GiB
  payload_cache_evict         589k entries, 10.2 GiB
  coalesce_hydrate_disk       121,449 entries
  coalesce_hydrate_ns         10.7s
  read_lookup_hydrate         28,718 ops, 11.6s

max_memory_mb=65536:
  run_dir                     .dev/fio-dedupe-compress-soak/codex-mem64-instrumented-90s
  fio_read_iops/write_iops    36.4k / 36.4k
  write_p99/p99.9             1.61ms / 4.56ms
  flush_writer_meta_lbas      30.1k/s
  coalesced_lbas              32.1k/s
  payload_cache_final         40.6 GiB / 64.0 GiB
  payload_cache_evict         0 entries, 0 GiB
  coalesce_hydrate_disk       38 entries
  coalesce_hydrate_ns         0.34s
  read_lookup_hydrate         9 ops, 0.002s
```

Conclusion: the user's suspicion was right. `buffer_backpressure_events=0`
does not mean the buffer is healthy; the 32 GiB durable payload cache was full
and forcing expensive LV2 rehydration. Raising the nvme-box durable payload
cache to 64 GiB removes almost all rehydration and modestly improves both
foreground and drain. It is not enough to eliminate pending debt, so writer /
metadb drain remains the next bottleneck, but payload-cache pressure is a real
secondary brake and should not be ignored.

Later decision: restore nvme-box to 32 GiB. The 64 GiB run proved that
rehydration was a secondary brake, but it did not materially change the
sustained throughput ceiling. Treat larger payload memory as a latency/cache
knob, not the primary fix.

```toml
[buffer]
max_memory_mb = 32768
```

2026-05-09 packed-slot commit batching A/B:

Question: after `pending_skip_threshold_entries=16384`, dedup/compress were
still able to feed writer, but writer spent too many metadb transactions on
single-fragment packed slots. Can we batch adjacent packed-slot metadata commits
without changing foreground durability semantics?

Implementation:

```text
new config:
  [flush]
  packed_commit_try_drain_lba_budget = 0

default 0:
  preserve old behavior: one packed slot per metadb commit

experiment >0:
  when the next queued commit is Packed, no-wait try-drain consecutive Packed
  jobs up to the LBA budget, then submit one metadb transaction containing all
  their BlockmapValue updates. Per-shard done_tx, mark_flushed, tail advance,
  lifecycle locks, stale dedup repair, and discarded-fragment handling stay
  attached to each original job.
```

Same 90s dedupe/compress mixed randrw 50/50 workload as above:

```text
baseline, packed_commit_try_drain_lba_budget=0:
  run_dir                     .dev/fio-dedupe-compress-soak/codex-feed-writer-32g-90s
  fio_read_iops/write_iops    36.0k / 36.0k
  write_p99                   1.97ms
  writer_lbas/s               38.2k
  coalesced_lbas/s            40.7k
  commits/s                   1.83k
  lbas/commit                 20.9
  packed_slots/fragments      143k / 143k
  writer_cycles_full          92.3%
  pending_final               2.24M
  payload_cache_final         32 GiB / 32 GiB
  payload_cache_evict         7.03 GiB
  read LV3/buffer hits        658k / 1.72M

packed_commit_try_drain_lba_budget=150:
  run_dir                     .dev/fio-dedupe-compress-soak/codex-packed-batch-32g-90s
  fio_read_iops/write_iops    28.9k / 28.8k
  write_p99                   3.56ms
  writer_lbas/s               69.5k
  coalesced_lbas/s            78.7k
  commits/s                   522
  lbas/commit                 133.1
  packed_slots/fragments      258k / 258k
  pending_final               921k
  verdict: metadb small-transaction bottleneck is real and this removes most
           of it, but foreground mixed IOPS drops because hot data leaves the
           buffer index sooner and reads hit LV3/decompress more often.

packed_commit_try_drain_lba_budget=64:
  run_dir                     .dev/fio-dedupe-compress-soak/codex-packed-batch64-32g-90s
  fio_read_iops/write_iops    28.4k / 28.4k
  write_p99                   9.63ms
  writer_lbas/s               1.65k
  coalesced_lbas/s            3.45k
  commits/s                   13.5
  lbas/commit                 122.0
  pending_final               2.40M
  verdict: rejected; this queue shape starves useful drain and hurts latency.

packed_commit_try_drain_lba_budget=150, catch-up run:
  run_dir                     .dev/fio-dedupe-compress-soak/codex-packed150-catchup-90s
  fio_read_iops/write_iops    16.2k / 16.2k
  read_p99/write_p99          7.96ms / 8.85ms
  fio_end_writer_lbas         194k
  fio_end_coalesced_lbas      362k
  fio_end_commits             1.54k
  fio_end_lbas/commit         126.3
  fio_end_pending             1.41M entries

  post-fio catch-up:
    ~40s after fio end: pending 349k, writer_lbas 4.78M
    ~60s after fio end: pending 117k, writer_lbas 5.75M
    ~70s after fio end: pending 0,    writer_lbas 6.21M

  verdict: for "background must catch up" this is the right direction. The
           closed-loop mixed fio number is lower because read latency throttles
           the job stream, but the backend does drain the accumulated work
           after foreground pressure stops.
```

Correctness:

```text
fio_returncode            0
read_crc_errors           0
dedup_verify_mismatches   0
flush_errors              0
```

Conclusion: packed-slot commit batching is the sharpest proof that metadb tx
amortization was a real writer bottleneck: `lbas/commit` rose from ~21 to
~126-133 and the backend can catch up instead of leaving a permanently growing
pending tail. It does expose the next bottleneck: mixed fio is closed-loop, so
read latency throttles both reads and writes once hot data falls through to
LV3/decompress. For the current goal ("background can catch up"), keep
`packed_commit_try_drain_lba_budget=150` in the nvme profile.

2026-05-09 foreground-active drain sweep:

The harder question is whether the backend can keep draining while foreground
mixed IO is still active. The answer is not yet "yes" for the 8 jobs x iodepth
4 x 50/50 randrw workload, but the bottleneck shape is now clear.

```text
packed=150, writer_read_active_batch_size=512:
  run_dir          .dev/fio-dedupe-compress-soak/codex-packed150-catchup-90s
  zone_write       ~72k LBA/s
  writer_lbas      ~2.1k LBA/s
  lbas/commit      126.3
  pending_at_end   1.41M
  verdict          read-active cap too low; writer starves while reads flow.

packed=150, writer_read_active_batch_size=1024, per_vol=1:
  run_dir          .dev/fio-dedupe-compress-soak/codex-packed150-wrab1024-90s
  zone_write       ~180k LBA/s
  coalesced        ~48k LBA/s
  writer_lbas      ~46k LBA/s
  commits          ~333/s
  lbas/commit      137.2
  pending_at_end   2.67M
  verdict          1024 fixes the writer starvation but not the sustained
                   drain gap. Dedup/compress are feeding writer; downstream is
                   still slower than foreground.

target_lbas_per_tx=600, writer_read_active=1024, per_vol=1:
  run_dir          .dev/fio-dedupe-compress-soak/codex-target600-wrab1024-90s
  writer_lbas      ~1.5k LBA/s
  commits          ~3.9/s
  lbas/commit      389.8
  verdict          rejected; giant tx makes the commit/mark chain too lumpy.

packed=150, writer_read_active=1024, per_vol=4:
  run_dir          .dev/fio-dedupe-compress-soak/codex-pervol4-wrab1024-90s
  zone_write       ~153k LBA/s
  coalesced        ~61k LBA/s
  writer_lbas      ~58k LBA/s
  commits          ~427/s
  lbas/commit      136.6
  l2p_hold_core    ~2.96
  l2p_wait_core    ~0.36
  pending_at_end   1.86M
  verdict          best foreground-active drain point so far. More parallel
                   commit helps a single volume, but starts spending real CPU
                   in L2P commit locks.

packed=150, writer_read_active=1024, per_vol=8:
  run_dir          .dev/fio-dedupe-compress-soak/codex-pervol8-wrab1024-90s
  writer_lbas      ~42k LBA/s
  commits          ~2.6k/s
  lbas/commit      16.2
  verdict          rejected; too much fanout shatters batching.

writer batch cap 2048, writer_read_active=2048, per_vol=1:
  run_dir          .dev/fio-dedupe-compress-soak/codex-wrab2048-pervol1-90s
  writer_lbas      ~45k LBA/s
  drained_max      2048
  verdict          rejected as default; 1024 was not the hard wall.
```

Instrumentation added after this sweep:

```text
flush_writer_commit_send_ns
flush_writer_commit_send_ops
flush_writer_commit_send_len_max
```

One instrumented per_vol=1 run hit `commit_send_len_max=128` and spent 1343s
of aggregate shard-writer time blocked on bounded commit-worker `send()`. That
confirms the active-drain bottleneck is behind writer IO: commit-worker
consumption / metadb publish / mark-flushed throughput, not dedup/compress
production. A packed-batch-as-one-queue-job experiment reduced send ops but did
not improve drain, so it was reverted. The current best nvme profile is:

```toml
[flush]
writer_read_active_batch_size = 1024
commit_workers_per_volume = 4
commit_target_lbas_per_tx = 150
packed_commit_try_drain_lba_budget = 150
```

Next useful cut: reduce the per-commit-worker post-publish work under the L2P
lock / mark-flushed path, or add a per-volume ordered publish queue that lets
multiple workers preserve large commit batches without fragmenting
`lbas/commit`.
```

### H2: Metadb Apply Lanes Are Under-Batched Or Mis-Scheduled

Symptom:

```text
commit threads spend time waiting for apply lane scheduling
apply lanes run many small jobs
CPU rises without proportional drain-rate gain
```

Metrics to add:

```text
apply_lane_queue_depth per lane
apply_lane_batch_items
apply_lane_batch_lbas
apply_lane_wait_ns
apply_lane_run_ns
apply_lane_wakeup_count
apply_lane_empty_wakeup_count
```

Experiments:

```text
1. Compare l2p/refcount/dedup lane utilization.
2. Batch adjacent apply jobs by lane before wakeup.
3. Pin apply lanes separately from writer lanes and read pool.
4. Sweep apply lane count if configurable.
```

Win condition:

```text
less wakeup/s
higher apply items/wakeup
lower apply wait tail
higher metadb committed lbas/s
```

### H3: WAL Or Durability Barrier Is The Metadb Gate

Symptom:

```text
commit latency follows WAL fsync / durability wait
larger batches help because they amortize one sync
NVMe device still has unused write bandwidth
```

Metrics to add:

```text
wal_append_bytes
wal_append_ns
wal_fsync_ns
wal_group_commit_wait_ns
wal_group_commit_batch_count
wal_group_commit_bytes
durable_lsn_lag
```

Experiments:

```text
1. Sweep metadb group commit window.
2. Compare fdatasync per tx vs grouped fdatasync.
3. Measure nvme iostat during WAL-heavy runs.
4. Isolate WAL device pressure from page-store/checkpoint pressure.
```

Win condition:

```text
same durability semantics
higher WAL bytes/fsync
lower fsync/s
higher committed lbas/s
bounded durable_lsn_lag
```

### H4: Page-Store Writeback / Checkpoint IO Is Blocking Commit

This is the case where writeback work matters, but only if it removes IO from
the critical path while increasing real writeback throughput.

Symptom:

```text
commit tail aligns with page flush/checkpoint writeback
dirty page count or checkpoint lag grows
NVMe write queue is shallow or fragmented
```

Metrics to add:

```text
dirty_page_count
dirty_bytes
dirty_oldest_age_ms
writeback_pages/s
writeback_bytes/s
writeback_batch_pages
writeback_submit_ns
writeback_wait_ns
checkpoint_pages
checkpoint_bytes
checkpoint_submit_ns
checkpoint_wait_ns
checkpoint_overlap_with_commit_ns
```

Experiments:

```text
1. Split page-store writeback into submit and wait metrics.
2. Raise writeback batch size and queue depth.
3. Keep hard dirty watermarks; fail a change if dirty backlog grows without catch-up.
4. Run post-load drain measurement: time from fio stop to clean/low-dirty state.
```

Win condition:

```text
commit path waits less on page IO
writeback bytes/s increases
dirty bytes stay bounded
post-load drain time does not increase
checkpoint lag stays bounded
```

Failure condition:

```text
foreground or commit looks faster, but dirty backlog grows
post-load drain time grows
checkpoint lag grows
recovery work grows
```

### H5: Metadb NVMe IO Submission Is Too Shallow Or Too Fragmented

Symptom:

```text
metadb IO device utilization is low
writeback/checkpoint uses small writes or low queue depth
CPU is waiting on individual IO completions
```

Metrics to add:

```text
metadb_io_write_ops
metadb_io_write_bytes
metadb_io_write_batch_ops
metadb_io_write_batch_bytes
metadb_io_qd_estimate
metadb_io_submit_ns
metadb_io_wait_ns
metadb_io_short_write_or_retry
```

Experiments:

```text
1. Add io_uring slab/batch pattern to metadb page writes if missing.
2. Compare single ring mutex vs per-lane rings for metadb IO.
3. Sweep queue depth.
4. Measure NVMe util, await, svctm-equivalent, and bandwidth while metadb is the bottleneck.
```

Win condition:

```text
higher metadb write MB/s
higher metadb write IOPS if small writes remain
lower IO wait core
no increase in dirty backlog
```

### H6: CPU Copy / Allocation Cost Still Gates Backend

Symptom:

```text
IO wait drops but total writer or metadb CPU remains high
allocator/copy profiles dominate
batch size changes do not increase drain proportionally
```

Evidence so far:

```text
write-only LV3 slab reduced flush_writer_io_core from 7.33 to 4.67.
```

Metrics to add:

```text
aligned_alloc_count
aligned_alloc_bytes
aligned_alloc_ns
copy_bytes
copy_ns
slab_batch_bytes
slab_batch_ops
```

Experiments:

```text
1. Reuse per-writer slabs instead of allocating per batch.
2. Add similar slab strategy to metadb page writes.
3. Profile CPU during j8 mixed run.
```

Win condition:

```text
lower CPU core cost
same or higher drain rate
same correctness
```

### H7: Foreground Reads Indirectly Starve Backend

The code paths differ, but they share CPU cores, read pool, LV3, and scheduler
resources.

Symptom:

```text
read-active mode changes writer batch behavior
increasing read threads lowers flush drain more than expected
read latency spikes align with flush IO or metadb commit bursts
```

Metrics to add:

```text
read_pool_queue_depth
read_pool_batch_ops
read_pool_wait_ns
read_latency_histogram
writer_read_active_cycles
foreground_read_iops
foreground_read_p99
```

Experiments:

```text
1. Run write-heavy 90/10 and compare against 50/50.
2. Pin read pool away from metadb apply and writer lanes.
3. Sweep read_pool_workers with fixed total fio rate.
```

Win condition:

```text
backend drain rises without read tail exploding
read-active writer cap remains justified by data
```

### H8: Refcount/Dedup Maintenance Is Not The Current Bottleneck But Can Reappear

Current no-dedupe workload avoids the dedup index. The production path still
needs separate validation with dedupe/compress enabled.

Metrics to watch:

```text
refcount_drainer_cycle_ns
refcount_drainer_checkpoint_wait_ns
refcount_pending_entries
dedup_put_ns
dedup_page_write_publish_ns
dedup_bucket_lock_wait_ns
dedup_eviction_chain_len
```

Experiments:

```text
1. Repeat best metadb changes with dedupe=70 compress=70.
2. Verify no regression in refcount drainer correctness.
3. Verify dedup put tails after metadb IO changes.
```

Win condition:

```text
no underflow
no read CRC errors
dedup/refcount tails do not dominate commit tail
```

## Investigation Order

1. Lock in the current LV3 writer candidate:

```text
write-only io_uring slab
writer batch coalesce = 250us
```

2. Add metadb commit/apply metrics:

```text
commit count/s
lbas/tx
bytes/tx
apply wait/run
WAL append/fsync
dirty publish
```

3. Run the same j8 3m workload and classify the commit bottleneck:

```text
fixed commit cost?
apply lane scheduling?
WAL/durability?
page writeback/checkpoint?
CPU copy/allocation?
```

4. Optimize the classified bottleneck with one change at a time.

5. After every candidate, record:

```text
run_dir
fio read/write/total IOPS
zone_write_lbas/s
flush_writer_meta_lbas/s
pending max/final
dirty backlog max/final
post-load drain time
commit/apply/WAL/writeback core cost
read tail latency
correctness counters
```

6. Only then run the dedupe/compress workload again.

## Decision Rules

A change is accepted only if most of these are true:

```text
flush_writer_meta_lbas/s increases
pending_final decreases or stays bounded
dirty backlog does not grow unbounded
post-load drain time does not increase
fio total IOPS does not materially regress
read tail does not materially regress
correctness counters stay clean
```

A change is rejected if:

```text
it improves foreground accept latency by growing dirty debt
it lowers real drain rate
it serializes independent lanes
it hides checkpoint/recovery cost
it requires unbounded memory or queue growth
```
