import re, sys
def parse(p):
    d = {}
    for line in open(p):
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
a, b = parse(sys.argv[1]), parse(sys.argv[2])
W = float(sys.argv[3])
appends = b["buffer.appends"] - a["buffer.appends"]
print("appends/s=%.0f  bytes/s=%.1f MB/s" % (appends / W, (b["buffer.append_bytes"] - a["buffer.append_bytes"]) / W / 1e6))
print("fill_pct %s -> %s   physical %s -> %s" % (a.get("buffer_fill_pct"), b.get("buffer_fill_pct"), a.get("buffer_physical_fill_pct"), b.get("buffer_physical_fill_pct")))
for k in ["append_total", "append_prepare", "append_order_wait", "append_order_hold", "append_log_write", "append_wait_durable", "append_backpressure_wait"]:
    d = b["front_write_ns." + k] - a["front_write_ns." + k]
    print("  %-26s %9.2f s   %9.1f us/append" % (k, d / 1e9, d / appends / 1000 if appends else 0))
print("backpressure_events", b["buffer.backpressure_events"] - a["buffer.backpressure_events"])
print("sync_batches", b["front_write_ns.sync_batches"] - a["front_write_ns.sync_batches"], "sync_flushes", b["front_write_ns.sync_flushes"] - a["front_write_ns.sync_flushes"])
