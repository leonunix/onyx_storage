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

# Per-site wait/hold attribution for the two allocator locks. THE question the
# free_pools table answers: of the writer's alloc time, how much is waiting, and
# whose hold was it? `retired_lock` answers the follow-up: the retire and free
# batch paths take a retired shard INSIDE their region hold, so a wait there is
# reported as region hold. `wait_ns`/`hold_ns`/`items` are monotonic so they
# difference; the `_max` fields are high-water marks (reported from the LAST
# sample, not differenced).
def site_table(prefix, title):
    sites = sorted({k.split(".")[1] for k in b
                    if k.startswith(prefix + ".") and k.endswith(".acquisitions")})
    if not sites:
        return None
    print("  -- %s --" % title)
    tot_hold = sum(d("%s.%s.hold_ns" % (prefix, s)) for s in sites)
    for s in sorted(sites, key=lambda s: -d("%s.%s.hold_ns" % (prefix, s))):
        acq = d("%s.%s.acquisitions" % (prefix, s))
        hold = d("%s.%s.hold_ns" % (prefix, s))
        if acq == 0 and hold == 0:
            continue
        wait = d("%s.%s.wait_ns" % (prefix, s))
        items = d("%s.%s.items" % (prefix, s))
        # us/item is the drift-insensitive one: sharding cuts one hold into many,
        # so us/acq moves even when the per-extent work is identical.
        print("  %-17s acq %9d (%7.0f/s)  wait %8.2f s (%8.2f us/acq)  "
              "hold %8.2f s (%8.2f us/acq) %5.1f%% of holds  hold_max %8.2f ms"
              "  wait_max %8.2f ms  items/acq %6.1f  hold %7.2f us/item" % (
              s, acq, acq / W, wait / 1e9, (wait / 1e3 / acq) if acq else 0.0,
              hold / 1e9, (hold / 1e3 / acq) if acq else 0.0,
              (hold / tot_hold * 100) if tot_hold else 0.0,
              b.get("%s.%s.hold_ns_max" % (prefix, s), 0) / 1e6,
              b.get("%s.%s.wait_ns_max" % (prefix, s), 0) / 1e6,
              (items / acq) if acq else 0.0,
              (hold / 1e3 / items) if items else 0.0))
    return tot_hold

tot_hold = site_table("free_lock", "free_pools lock attribution (who holds it while the writer waits)")
if tot_hold is not None:
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

ret_hold = site_table("retired_lock", "retired shard attribution (retired set + age log, same layout as free_lock)")
if ret_hold is not None:
    # Sharded on the same layout as the free pool, so the comparable number --
    # "how busy is the shard a retire actually queues on" -- is the sum divided by
    # the region count. Both are printed: the total says how much retired-side work
    # there is, the per-shard number is what to compare against the 49.1% that one
    # global retired mutex measured on 2026-07-30.
    regions = max(1, b.get("allocator_regions.regions", 1))
    print("  retired lock busy %.1f%% of wall (sum of holds / %ds)" % (ret_hold / 1e9 / W * 100, W))
    if regions > 1:
        print("  retired lock busy %.3f%% per shard (%d shards)" % (
            ret_hold / 1e9 / W * 100 / regions, regions))

# THE discriminating ledger. `retire_extents_batch` and `free_extents_batch` hold
# a region lock across their retired-shard acquisition, so their region hold
# decomposes into: wait for the shard + hold of the shard + residual = the
# per-extent work done under the region lock only.
#   residual dominant  -> the work itself got more expensive (colder/more sets):
#                         fix = fewer regions / a different structure.
#   retired wait dominant -> the region hold is just queueing behind one global
#                         mutex: fix = stop waiting for `retired` under a region
#                         lock (shard `retired`, or take it outside).
for site in ("retire_batch", "free_batch"):
    region_hold = d("free_lock.%s.hold_ns" % site)
    if not region_hold:
        continue
    rwait = d("retired_lock.%s.wait_ns" % site)
    rhold = d("retired_lock.%s.hold_ns" % site)
    print("  -- %s nested ledger (region hold = retired wait + retired hold + residual) --" % site)
    for label, v in (("region hold", region_hold),
                     ("  retired wait", rwait),
                     ("  retired hold", rhold),
                     ("  residual (work under region lock only)",
                      region_hold - rwait - rhold)):
        print("  %-42s %9.2f s  %6.1f%% of region hold" % (
            label, v / 1e9, v / region_hold * 100))

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
# ⚠ Un-amortised: this counts each batch's device call ONCE while `submit` counts
# it once per blocked request. The comparable number is in the ledger below.
print("  %-16s %9.2f s  %5.1f%% of submit   (device wall, NOT its share of blocked time)"
      % ("of which lv3", lv3 / 1e9, lv3 / sub * 100 if sub else 0))

print("== LV3 write batcher ==")
reqs = d("lv3_batch.requests")
batches = d("lv3_batch.window_timeouts") + d("lv3_batch.target_hits")
wait = d("lv3_batch.wait")
calls = d("lv3_batch.wait_calls")
print("  producer wait   %9.2f s over %d submit_many calls" % (wait / 1e9, calls))
if batches:
    print("  batches %d (%.0f/s)  requests %d (%.2f per batch)  device %.2f ms/batch" % (
        batches, batches / W, reqs, reqs / batches, lv3 / batches / 1e6))
    print("  dispatch reason: TIMEOUT %d (%.1f%%)  target_hit %d" % (
        d("lv3_batch.window_timeouts"), d("lv3_batch.window_timeouts") / batches * 100,
        d("lv3_batch.target_hits")))
    print("  bytes at dispatch %.0f KiB avg (target 4096 KiB)" % (
        d("lv3_batch.bytes_at_dispatch") / batches / 1024))
    print("  device concurrency %.2f calls in flight (write_batch_ns / wall)" % (lv3 / 1e9 / W))
# THE ledger, and the one place the bases must not be mixed. `pickup` / `window`
# / `reply` are accumulated once per REQUEST; `exec_queue` / `exec_prep` and
# `lv3_write_batch_ns` once per BATCH. Every request in a batch blocks for that
# batch's WHOLE device call, so the device's share of blocked time is
# `write_batch_ns * requests/batches` -- reading it raw is what made the device
# leg look like 5.8% of `submit.io` when it is 57%. A large `residual` means the
# blocked path grew a leg nothing measures.
if reqs and batches and wait:
    fan = reqs / batches
    terms = [
        ("pickup", d("lv3_batch.pickup"), "req"),
        ("window", d("lv3_batch.window"), "req"),
        ("exec_queue", d("lv3_batch.exec_queue") * fan, "batch*fan"),
        ("exec_prep", d("lv3_batch.exec_prep") * fan, "batch*fan"),
        # Request-weighted when the counter exists (post-2026-08-01 builds);
        # older samples fall back to the biased-low mean*fan estimate.
        ("device", d("lv3_batch.device") or lv3 * fan,
         "per-req" if d("lv3_batch.device") else "batch*fan (biased low)"),
        ("reply", d("lv3_batch.reply"), "req"),
    ]
    print("  -- blocked-time ledger (amortised to per-request) --")
    accounted = 0
    for label, v, base in terms:
        accounted += v
        print("    %-12s %9.2f s  %5.1f%% of wait  %8.2f ms/request   [%s]" % (
            label, v / 1e9, v / wait * 100, v / reqs / 1e6, base))
    print("    %-12s %9.2f s  %5.1f%% of wait  %8.2f ms/request" % (
        "residual", (wait - accounted) / 1e9, (wait - accounted) / wait * 100,
        (wait - accounted) / reqs / 1e6))
    print("    %-12s %9.2f s               %8.2f ms/request   (submit.io per request)" % (
        "TOTAL wait", wait / 1e9, wait / reqs / 1e6))

# chunklet's own view of the same call. `lv3_io.write_batch_ns` is the caller's
# wall clock around `write_many_at`; these are the phases INSIDE it, so
# `r6_total` should track it within a percent. The discriminating questions:
#   compute dominant  -> P/Q recompute is the cost (SIMD / strip width).
#   lock dominant     -> stripe-bucket queueing (readers or another executor).
#   write dominant    -> the submit waves; then `waves/call` and `sqes/wave`
#                        say whether it is barrier count or per-IO service time.
r6c = d("chunklet_r6_batch.calls")
if r6c:
    r6_total = d("chunklet_r6_batch.total_ns")
    print("== chunklet RAID6 batched write (inside lv3 write_batch) ==")
    print("  calls %d (%.0f/s)  ops/call %.1f  stripes/call %.1f  serial_bails %d" % (
        r6c, r6c / W, d("chunklet_r6_batch.ops") / r6c,
        d("chunklet_r6_batch.stripes") / r6c, d("chunklet_r6_batch.serial_bails")))
    print("  total %.2f ms/call   (caller-side lv3 write_batch %.2f ms/call)" % (
        r6_total / r6c / 1e6,
        lv3 / d("lv3_io.write_batch_calls") / 1e6 if d("lv3_io.write_batch_calls") else 0))
    acc = 0
    for label in ["plan", "lock", "read", "compute", "write"]:
        v = d("chunklet_r6_batch.%s_ns" % label)
        acc += v
        print("    %-10s %9.2f s  %5.1f%% of total  %8.3f ms/call" % (
            label, v / 1e9, v / r6_total * 100 if r6_total else 0, v / r6c / 1e6))
    print("    %-10s %9.2f s  %5.1f%% of total  %8.3f ms/call  (unmeasured)" % (
        "residual", (r6_total - acc) / 1e9,
        (r6_total - acc) / r6_total * 100 if r6_total else 0, (r6_total - acc) / r6c / 1e6))
    print("  total_ns_max %.2f ms (high-water, last sample)" % (
        b.get("chunklet_r6_batch.total_ns_max", 0) / 1e6))
# Per IoClass, because LV3 (drain_data), LV2 (foreground) and metadb (drain_meta)
# share one backend and one global set would report a wave width belonging to no
# caller. LV3's row is the one that composes with the r6 phases above.
print("  -- submit waves per IoClass (uring_write_chunk_ops / uring_coalesced_wait) --")
for cls in ["drain_data", "foreground", "drain_meta", "maintenance"]:
    pre = "chunklet_submit_" + cls
    sc = d(pre + ".calls")
    if not sc:
        continue
    waves = d(pre + ".waves")
    sqes = d(pre + ".sqes")
    ops = d(pre + ".ops")
    wait = d(pre + ".wait_ns")
    print("  %-11s calls %8d (%6.0f/s)  waves/call %5.2f  strip ops/call %6.1f  sqes/call %6.1f"
          "  merge %5.2fx" % (
        cls, sc, sc / W, waves / sc, ops / sc, sqes / sc, ops / sqes if sqes else 0))
    print("  %-11s sqes/wave %5.1f  wait %7.3f ms/wave (%7.3f ms/call)  %6.1f us/sqe"
          "  bounce %6.0f KiB/call in %6.3f ms" % (
        "", sqes / waves if waves else 0, wait / waves / 1e6 if waves else 0, wait / sc / 1e6,
        wait / sqes / 1e3 if sqes else 0,
        d(pre + ".bounce_bytes") / sc / 1024, d(pre + ".bounce_ns") / sc / 1e6))
