#!/usr/bin/env python3
"""Host-queue budget + read-path decomposition from two `onyx status` samples.

Answers "what pins write DEMAND" by accounting for where fio's fixed queue depth
actually sits. Two independent readings, both from the same delta:

  * occupancy (Little's law)  = ops/s * mean host latency, per direction.
    Their sum is the host queue depth fio is holding (QD256 for j16d16), so the
    split says which direction is consuming the slots.
  * server busy               = sum(worker_ns)/W = mean number of ublk worker
    threads executing an IO. The frontend serves each READ synchronously on a
    worker thread (writes hand off to the per-queue durability dispatcher), so
    this is capped at `ublk.nr_queues * ublk.queue_workers`. If it pins at the
    cap while `queue_wait` is a large share of read latency, the worker pool --
    not the device, not LV2 -- is what bounds read throughput, and the write
    demand follows it through fio's fixed rwmix.

Usage:  read_delta.py <status_a> <status_b> <window_seconds> [worker_cap]
"""
import re
import sys


def parse(path):
    d = {}
    for line in open(path):
        pre, _, rest = line.partition(":")
        pre = pre.strip()
        for tok in rest.split():
            k, _, v = tok.partition("=")
            if v.isdigit():
                d[pre + "." + k] = int(v)
        m = re.match(r"^(buffer_\w+):\s+(\d+)$", line.strip())
        if m:
            d[m.group(1)] = int(m.group(2))
    return d


def main():
    a, b = parse(sys.argv[1]), parse(sys.argv[2])
    w = float(sys.argv[3])
    cap = int(sys.argv[4]) if len(sys.argv) > 4 else None

    def d(k):
        return b.get(k, 0) - a.get(k, 0)

    r_ops, w_ops = d("volume_ops.read"), d("volume_ops.write")
    r_bytes, w_bytes = d("volume_bytes.read"), d("volume_bytes.write")
    r_ns, w_ns = d("user_io_latency_ns.read_total"), d("user_io_latency_ns.write_total")

    print("window %.1f s   read %d ops (%.1f%%)  write %d ops" % (
        w, r_ops, 100.0 * r_ops / max(1, r_ops + w_ops), w_ops))

    occ_total = 0.0
    for name, ops, byts, tot in (("read", r_ops, r_bytes, r_ns), ("write", w_ops, w_bytes, w_ns)):
        if not ops:
            continue
        lat = tot / ops / 1e6            # ms
        occ = tot / w / 1e9              # ops * lat / window = concurrency
        occ_total += occ
        print("  %-5s %8.0f iops  %8.1f MiB/s  bs %5.1f KiB  lat %8.3f ms  "
              "occupancy %7.1f slots" % (
                  name, ops / w, byts / w / 1048576.0, byts / ops / 1024.0, lat, occ))
    print("  host queue depth held (sum of occupancies) = %.1f" % occ_total)

    print("ublk stage split (us/op, share of host latency):")
    busy = 0.0
    for name, pre, ops, tot in (("read", "ublk_read_split_ns", r_ops, r_ns),
                                ("write", "ublk_write_split_ns", w_ops, w_ns)):
        if not ops:
            continue
        qw, wk, cw = d(pre + ".queue_wait"), d(pre + ".worker"), d(pre + ".completion_wait")
        busy += wk / w / 1e9
        print("  %-5s queue_wait %9.1f (%5.1f%%)  worker %9.1f (%5.1f%%)  "
              "completion_wait %8.1f (%5.1f%%)" % (
                  name, qw / ops / 1000, 100.0 * qw / max(1, tot),
                  wk / ops / 1000, 100.0 * wk / max(1, tot),
                  cw / ops / 1000, 100.0 * cw / max(1, tot)))
    print("  worker threads busy (mean) = %.1f%s" % (
        busy, "   cap %d  => %.1f%% saturated" % (cap, 100.0 * busy / cap) if cap else ""))

    calls = d("read_submit.calls")
    if calls:
        print("read_submit: %d calls, %.1f us/call" % (calls, d("read_submit.total_ns") / calls / 1000))
        for k in ("buffer_lookup_ns", "meta_get_ns", "unit_io_ns"):
            v = d("read_submit." + k)
            print("  %-16s %9.1f us/call  %5.1f%%" % (
                k[:-3], v / calls / 1000, 100.0 * v / max(1, d("read_submit.total_ns"))))
        for k in ("query_ns", "route_ns"):
            print("    meta.%-12s %9.1f us/call" % (
                k[:-3], d("read_submit_meta_split." + k) / calls / 1000))
        # Everything between pass 2 and pass 4 — the per-unit hazard pin +
        # mapping re-verify and the group/extent build — is inside `total` but
        # in none of the three stages, so it only shows up as this residual.
        resid = (d("read_submit.total_ns") - d("read_submit.buffer_lookup_ns")
                 - d("read_submit.meta_get_ns") - d("read_submit.unit_io_ns"))
        print("  %-16s %9.1f us/call  %5.1f%%  (hazard pin + re-verify + grouping)" % (
            "residual", resid / calls / 1000, 100.0 * resid / max(1, d("read_submit.total_ns"))))

    reqs = d("read_pool.requests")
    if reqs:
        batches, bops = d("read_pool.batches"), d("read_pool.batch_ops")
        print("read_pool: %d requests, %d batches, %.2f ops/batch" % (
            reqs, batches, bops / max(1, batches)))
        for k in ("queue_wait_ns", "coalesce_wait_ns", "alloc_ns", "submit_wait_ns", "decode_ns"):
            print("  %-18s %9.1f us/request" % (k[:-3], d("read_pool." + k) / reqs / 1000))

    hits = d("read_path.buffer_hits")
    lv3 = d("read_path.lv3_hits")
    print("read_path: buffer_hits %d  lv3_hits %d  unmapped %d  crc_fg %d" % (
        hits, lv3, d("read_path.unmapped"), d("read_path.crc_fg")))


main()
