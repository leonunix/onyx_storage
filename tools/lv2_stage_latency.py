#!/usr/bin/env python3
"""Per-stage LV2 latency percentiles from two metrics-json samples."""
import json, socket, sys, time

SOCK = "/tmp/onyx-storage-nvme.sock"
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

a = fetch()
time.sleep(float(sys.argv[1]) if len(sys.argv) > 1 else 30)
b = fetch()
bounds = b["buffer_lv2_latency_bucket_upper_bounds_ns"]
print("%-20s %10s %10s %10s %10s %12s" % ("stage", "p50 us", "p90 us", "p99 us", "p999 us", "samples"))
for name, key in STAGES:
    d = [y - x for x, y in zip(a[key], b[key])]
    n = sum(d)
    if not n:
        print("%-20s %10s %10s %10s %10s %12d" % (name, "-", "-", "-", "-", 0))
        continue
    print("%-20s %10.1f %10.1f %10.1f %10.1f %12d" % (
        name, pct(d, bounds, 0.5) / 1000, pct(d, bounds, 0.9) / 1000,
        pct(d, bounds, 0.99) / 1000, pct(d, bounds, 0.999) / 1000, n))
