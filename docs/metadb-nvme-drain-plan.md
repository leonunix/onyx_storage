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

