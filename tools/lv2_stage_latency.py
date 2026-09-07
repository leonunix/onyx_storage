#!/usr/bin/env python3
"""Per-stage LV2 latency percentiles from two metrics-json samples."""
import json, os, socket, sys, time

SOCK = os.environ.get("ONYX_SOCKET", "/tmp/onyx-storage-nvme.sock")
STAGES = [
    ("staging_queue", "buffer_lv2_staging_queue_latency_buckets"),
    ("prepared_queue", "buffer_lv2_prepared_queue_latency_buckets"),
    ("group_collect", "buffer_lv2_group_collect_latency_buckets"),
    ("payload_write", "buffer_lv2_payload_write_latency_buckets"),
    ("checkpoint_write", "buffer_lv2_checkpoint_write_latency_buckets"),
    ("root_flush", "buffer_lv2_root_flush_latency_buckets"),
    ("watermark_dispatch", "buffer_lv2_watermark_dispatch_latency_buckets"),
    ("APPEND_wait_durable", "buffer_append_wait_durable_fine_latency_buckets"),
]

# Cumulative spans, NOT additive with the stages above -- they overlap them.
# `written_queue` and `written_to_durable` both start at "lane write returned",
# so `written_to_durable - written_queue` is the coordinator's own serial work
# (sibling drain + checkpoint + device flush + its position in the publish loop).
SPANS = [
    ("entry_write", "buffer_lv2_entry_write_latency_buckets"),
    ("written_queue", "buffer_lv2_written_queue_latency_buckets"),
    ("written_to_durable", "buffer_lv2_written_to_durable_latency_buckets"),
    ("STAGED_to_durable", "buffer_lv2_staged_to_durable_latency_buckets"),
]

def fetch():
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(SOCK)
    s.sendall(b"metrics-json\n")
    buf = b""
    while not buf.endswith(b"\nok\n"):
        chunk = s.recv(1 << 20)
        if not chunk:
            break
        buf += chunk
    s.close()
    return json.loads(buf[: -len(b"\nok\n")].decode())

def pct(buckets, bounds, q):
    total = sum(buckets)
    if not total:
        return None
    want, seen = total * q, 0
    for i, c in enumerate(buckets):
        seen += c
        if seen >= want:
            return bounds[i] if i < len(bounds) else bounds[-1]
    return bounds[-1]

def mean_ub(buckets, bounds):
    """Bucket-weighted mean using each bucket's UPPER bound, i.e. an
    OVERESTIMATE. Percentiles do not add, so reconciling a per-stage ledger
    against `APPEND_wait_durable` needs means -- and an upper-bound mean is the
    honest direction to err: if the overestimated stages still sum well below
    the total, the missing time is real and unmeasured, not a rounding artifact.
    """
    total = sum(buckets)
    if not total:
        return None
    acc = 0
    for i, c in enumerate(buckets):
        acc += c * (bounds[i] if i < len(bounds) else bounds[-1])
    return acc / total


a = fetch()
time.sleep(float(sys.argv[1]) if len(sys.argv) > 1 else 30)
b = fetch()
bounds = b["buffer_lv2_latency_bucket_upper_bounds_ns"]
print("%-20s %10s %10s %10s %10s %10s %12s" % (
    "stage", "mean<= us", "p50 us", "p90 us", "p99 us", "p999 us", "samples"))
stage_mean = {}
for name, key in STAGES:
    d = [y - x for x, y in zip(a[key], b[key])]
    n = sum(d)
    if not n:
        print("%-20s %10s %10s %10s %10s %10s %12d" % (name, "-", "-", "-", "-", "-", 0))
        continue
    m = mean_ub(d, bounds) / 1000
    stage_mean[name] = m
    print("%-20s %10.1f %10.1f %10.1f %10.1f %10.1f %12d" % (
        name, m, pct(d, bounds, 0.5) / 1000, pct(d, bounds, 0.9) / 1000,
        pct(d, bounds, 0.99) / 1000, pct(d, bounds, 0.999) / 1000, n))

span_mean = {}
print()
for name, key in SPANS:
    d = [y - x for x, y in zip(a[key], b[key])]
    n = sum(d)
    if not n:
        print("%-20s %10s %10s %10s %10s %10s %12d" % (name, "-", "-", "-", "-", "-", 0))
        continue
    m = mean_ub(d, bounds) / 1000
    span_mean[name] = m
    print("%-20s %10.1f %10.1f %10.1f %10.1f %10.1f %12d" % (
        name, m, pct(d, bounds, 0.5) / 1000, pct(d, bounds, 0.9) / 1000,
        pct(d, bounds, 0.99) / 1000, pct(d, bounds, 0.999) / 1000, n))
if "written_to_durable" in span_mean and "written_queue" in span_mean:
    print("%-20s %10.1f %10s %10s %10s %10s %12s" % (
        "  coord_service", span_mean["written_to_durable"] - span_mean["written_queue"],
        "", "", "", "", "(derived)"))

# Ledger close, in two steps.
#
# 1. Anchor check: wait_durable - watermark_dispatch must equal STAGED_to_durable.
#    Both are per entry and share their endpoints, so a gap here means an
#    instrument is wrong, not that a stage is missing.
# 2. Attribution: the chain stages must add up to STAGED_to_durable. Use the
#    ENTRY-weighted write (`entry_write`), not the epoch-weighted `payload_write`.
total = stage_mean.get("APPEND_wait_durable")
staged = span_mean.get("STAGED_to_durable")
if total and staged:
    dispatch = stage_mean.get("watermark_dispatch", 0)
    print("\nanchor: wait_durable %.1f - dispatch %.1f = %.1f us vs "
          "STAGED_to_durable %.1f us (gap %.1f)" % (
              total, dispatch, total - dispatch, staged, total - dispatch - staged))
    parts = ["staging_queue", "prepared_queue", "group_collect"]
    accounted = sum(stage_mean.get(k, 0) for k in parts)
    accounted += span_mean.get("entry_write", 0)
    accounted += span_mean.get("written_to_durable", 0)
    print("attribution: accounted <= %.1f us of %.1f us in-pipeline (%.0f%%), "
          "residual >= %.1f us" % (
              accounted, staged, accounted / staged * 100, staged - accounted))
    print("  chain: staging_queue + prepared_queue + group_collect + entry_write")
    print("         + written_to_durable   (prepare `build` is NOT in this list;")
    print("         read it from the lv2_prepare status line)")
    print("  ⚠ group_collect is still epoch-weighted and every mean is an UPPER")
    print("    bound, so `accounted` errs high.")
