import sys
def parse(p):
    d = {}
    for line in open(p):
        pre, _, rest = line.partition(":")
        pre = pre.strip()
        for tok in rest.split():
            k, _, v = tok.partition("=")
            if v.isdigit():
                d[pre + "." + k] = int(v)
    return d
a, b = parse(sys.argv[1]), parse(sys.argv[2])
W = float(sys.argv[3])
def d(k):
    return b.get(k, 0) - a.get(k, 0)
print("== flush writer window (%.0fs, wall-clock per writer thread) ==" % W)
tot = d("flush_writer_ns.total")
for k in ["total", "alloc", "io", "meta_build", "meta_commit", "meta_candidate", "cleanup", "mark_flushed"]:
    v = d("flush_writer_ns." + k)
    print("  %-16s %9.2f s  %5.1f%% of total" % (k, v / 1e9, v / tot * 100 if tot else 0))
alloc = d("flush_writer_ns.alloc")
print("  -- alloc split (which free pool served it) --")
accounted = 0
for label in ["aligned", "unaligned", "reserve_miss"]:
    v = d("flush_writer_alloc_split." + label + "_ns")
    ops = d("flush_writer_alloc_split." + label + "_ops")
    accounted += v
    print("  %-16s %9.2f s  %5.1f%% of alloc  %8d ops (%6.0f/s)  %7.2f us/op" % (
        label, v / 1e9, v / alloc * 100 if alloc else 0, ops, ops / W,
        v / ops / 1000 if ops else 0))
# The three buckets time the allocator calls; `alloc` is the whole Phase A wall
# clock, so the residual is planning + bookkeeping, NOT a missing pool. A large
# residual means Phase A grew a new cost centre that nothing measures.
print("  %-16s %9.2f s  %5.1f%% of alloc   (planning/bookkeeping residual)" % (
    "unaccounted", (alloc - accounted) / 1e9,
    (alloc - accounted) / alloc * 100 if alloc else 0))
attempts = d("flush_writer_alloc_split.aligned_ops") + d("flush_writer_alloc_split.reserve_miss_ops")
if attempts:
    # A rising miss rate = the stripe reserve is draining faster than free/defrag
    # refills it, which is what pushes the writer onto the general pool.
    print("  stripe reserve miss %.2f%% of %d aligned attempts  (starved_batches %d)" % (
        d("flush_writer_alloc_split.reserve_miss_ops") / attempts * 100, attempts,
        d("defrag.stripe_starved_batches")))
refills = d("allocator_supply.refills")
if refills:
    # The lane cache is supposed to absorb most allocations and touch the global
    # free lock only to refill. allocs_per_refill IS that amplification: at ~1.0
    # every full-stripe allocation serializes on the global lock (the aged-pool
    # state where the stripe reserve is nothing but isolated single stripes).
    # drains are the expensive shape: one global hold that re-inserts every
    # cached extent from ALL lanes.
    allocs = d("allocator_supply.aligned_allocs")
    print("  -- lane-cache supply (global free-lock amplification) --")
    print("  refills %d (%.0f/s)  allocs/refill %.2f  blocks/refill %.1f  runs/refill %.1f" % (
        refills, refills / W, allocs / refills, d("allocator_supply.refill_blocks") / refills,
        d("allocator_supply.refill_runs") / refills))
    print("  drains %d (%.2f/s) covering %d blocks" % (
        d("allocator_supply.drains"), d("allocator_supply.drains") / W,
        d("allocator_supply.drain_blocks")))

# Per-site free_pools wait/hold attribution. THE question this answers: of the
# writer's alloc time, how much is waiting, and whose hold was it? `wait_ns` and
# `hold_ns` are monotonic so they difference; hold_max_us is a high-water mark
# (reported from the LAST sample, not differenced).
sites = sorted({k.split(".")[1] for k in b if k.startswith("free_lock.") and k.endswith(".acquisitions")})
if sites:
    print("  -- free_pools lock attribution (who holds it while the writer waits) --")
    tot_hold = sum(d("free_lock.%s.hold_ns" % s) for s in sites)
    for s in sorted(sites, key=lambda s: -d("free_lock.%s.hold_ns" % s)):
        acq = d("free_lock.%s.acquisitions" % s)
        if acq == 0 and d("free_lock.%s.hold_ns" % s) == 0:
            continue
        hold = d("free_lock.%s.hold_ns" % s)
        wait = d("free_lock.%s.wait_ns" % s)
        print("  %-17s acq %9d (%7.0f/s)  wait %8.2f s (%8.2f us/acq)  "
              "hold %8.2f s (%8.2f us/acq) %5.1f%% of holds  hold_max %8.2f ms" % (
              s, acq, acq / W, wait / 1e9, (wait / 1e3 / acq) if acq else 0.0,
              hold / 1e9, (hold / 1e3 / acq) if acq else 0.0,
              (hold / tot_hold * 100) if tot_hold else 0.0,
              b.get("free_lock.%s.hold_ns_max" % s, 0) / 1e6))
    # With one global lock, sum-of-holds / wall IS that lock's busy fraction
    # (68.4% on the 2026-07-29 box read). Sharded into N address regions the sum
    # spans N independent locks, so the comparable number -- "how busy is the lock
    # a writer actually queues on" -- is that sum divided by N. Both are printed:
    # the total still says how much allocator work there is, the per-lock number
    # is the one to compare against the 68.4% baseline.
    regions = max(1, b.get("allocator_regions.regions", 1))
    print("  lock busy %.1f%% of wall (sum of holds / %ds)" % (tot_hold / 1e9 / W * 100, W))
    if regions > 1:
        print("  lock busy %.2f%% per region (%d regions, blocks/region %d, serialized=%d)" % (
            tot_hold / 1e9 / W * 100 / regions, regions,
            b.get("allocator_regions.region_blocks", 0),
            b.get("allocator_regions.serialized", 0)))
        # A lane that keeps moving region, or refills that keep coming up empty,
        # means the regions are mis-sized for the lane count -- the wrong shape,
        # not the wrong idea.
        print("  region switches %d (%.2f/s)  refill misses %d" % (
            d("allocator_regions.switches"), d("allocator_regions.switches") / W,
            d("allocator_regions.refill_misses")))

grp = d("flush_writer_stripe_groups.total")
if grp:
    # Exactly-full single-volume stripes can be freed as a whole and hand their
    # 24 KiB window back to the reserve; mixed ones stay part-pinned.
    sv = d("flush_writer_stripe_groups.single_volume")
    print("  stripes emitted %d (%.0f/s)  single-volume exact %d (%.1f%%)" % (
        grp, grp / W, sv, sv / grp * 100))
io = d("flush_writer_ns.io")
print("  -- io split --")
for k in ["bufalloc", "bufzero", "assemble", "submit"]:
    v = d("flush_writer_io_split." + k)
    print("  %-16s %9.2f s  %5.1f%% of io" % (k, v / 1e9, v / io * 100 if io else 0))
lv3 = d("lv3_io.write_batch_ns")
print("  %-16s %9.2f s  %5.1f%% of io   (real LV3 device write)" % ("lv3 write_batch", lv3 / 1e9, lv3 / io * 100 if io else 0))
print("== throughput ==")
print("  lv3 write bytes %.1f MB/s  ops %.0f/s  batch calls %.0f/s" % (
    d("lv3_io.write_compressed_bytes") / W / 1e6, d("lv3_io.write_ops") / W, d("lv3_io.write_batch_calls") / W))
print("  flush lbas %.0f/s  units %.0f/s  meta commits %.0f/s" % (
    d("flush.lbas") / W, d("flush.units") / W, d("flush_writer_meta.commits") / W))
print("  gc retired_in %.0f/s reclaimed %.0f/s" % (d("gc.retired_in") / W, d("gc.retired_reclaimed") / W))

sub = d("flush_writer_io_split.submit")
print("  -- submit split (%.1f%% of io) --" % (sub / io * 100 if io else 0))
for k in ["ops", "io", "rollback", "padding"]:
    v = d("flush_writer_submit_split." + k)
    print("  %-16s %9.2f s  %5.1f%% of submit" % (k, v / 1e9, v / sub * 100 if sub else 0))
print("  %-16s %9.2f s  %5.1f%% of submit   (device write inside submit.io)"
      % ("of which lv3", lv3 / 1e9, lv3 / sub * 100 if sub else 0))

print("== LV3 write batcher ==")
reqs = d("lv3_batch.requests")
batches = d("lv3_batch.window_timeouts") + d("lv3_batch.target_hits")
wait = d("lv3_batch.wait")
print("  producer wait   %9.2f s over %d submit_many calls" % (wait / 1e9, d("lv3_batch.wait_calls")))
for label, key in [("enqueue", "lv3_batch.enqueue"), ("pickup", "lv3_batch.pickup"),
                   ("window", "lv3_batch.window"), ("exec_queue", "lv3_batch.exec_queue")]:
    v = d(key)
    print("    %-12s %9.2f s  %5.1f%% of wait" % (label, v / 1e9, v / wait * 100 if wait else 0))
print("    %-12s %9.2f s  %5.1f%% of wait   (device)" % ("lv3 write", lv3 / 1e9, lv3 / wait * 100 if wait else 0))
if batches:
    print("  batches %d (%.0f/s)  requests %d (%.2f per batch)" % (batches, batches / W, reqs, reqs / batches))
    print("  dispatch reason: TIMEOUT %d (%.1f%%)  target_hit %d" % (
        d("lv3_batch.window_timeouts"), d("lv3_batch.window_timeouts") / batches * 100,
        d("lv3_batch.target_hits")))
    print("  bytes at dispatch %.0f KiB avg (target 4096 KiB)" % (
        d("lv3_batch.bytes_at_dispatch") / batches / 1024))
if reqs:
    print("  per request: pickup %.0f us  window %.0f us  exec_queue %.0f us" % (
        d("lv3_batch.pickup") / reqs / 1000, d("lv3_batch.window") / reqs / 1000,
        d("lv3_batch.exec_queue") / reqs / 1000))
