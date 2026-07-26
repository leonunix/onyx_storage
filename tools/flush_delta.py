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
