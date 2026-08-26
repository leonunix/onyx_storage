#!/usr/bin/env python3
"""LV2 ring release ledger, straight out of an `RUST_LOG=info` engine.log.

    checkpoint_delta.py <engine.log> [more.log ...] [--skip-s N] [--host-mbs X]

WHY. `DurabilityWatermarkHandle` (src/engine/durability.rs) is the ONLY caller
of `pool.release_below`, so LV2 ring bytes come back **only** at the instant a
metadb checkpoint is confirmed durable, and only one checkpoint may be in
flight. Three triggers exist; `checkpoint_ring_fill_pct` defaults to 0 (= ring
trigger DEAD) and `nvme-chunklet.toml` does not set it, so on the box the live
triggers are the 5 s interval and `flush_dirty_pages_threshold`.

That makes the ring a **checkpoint-cadence resource**, and it is why a
foreground append can pay `backpressure_wait` while `buffer_pending_entries` is
0 and the drives are 80% idle: the flusher is caught up, but the flushed prefix
cannot be recycled until the next checkpoint lands.

THE NUMBER THIS PRINTS. `release MiB/s` = Sum(released_bytes) / wall span. Compare
it to the host write rate (pass `--host-mbs`, or read `bytes/s` out of
tools/front_delta.py):

  release ~= host, and fill_before pinned at 100   -> the release path is the
      governor; appends are rate-limited by checkpoint throughput, not by the
      drain and not by the device.
  release >> host                                  -> the ring is not the
      binding constraint in this window; the 100% gauge is just unreclaimed
      space ([[lv2_write_lane_cap_is_the_append_wall]] gauge warning) and the
      wall is elsewhere in the append pipeline.

⚠ Do NOT read this over a fresh pool's burst phase; `--skip-s` exists to drop
it (the CLAUDE.md rule is >= 6 min at RWMIX=0).

⛔ Prior art, do not re-walk: forcing the cadence with
`meta.checkpoint_ring_fill_pct = 50` was measured 2026-07-25 and LOST 40% of
throughput -- each checkpoint then released ~5x fewer entries for the same
bytes/s. See memory `checkpoint_interval_duty_cycle_lockin`. That measurement
was taken in the degraded regime where the drain itself was saturated; this
script exists to establish whether that premise still holds.
"""
import re
import sys
from datetime import datetime

# tracing's default fmt layer colours the timestamp and every field name, so a
# raw engine.log line starts with an escape and reads `token\x1b[0m\x1b[2m=...`.
# Strip that first or nothing parses (the timestamp regex silently fails and
# every release is dropped).
ANSI = re.compile(r"\x1b\[[0-9;]*m")

REQ = re.compile(r"durability watermark requested metadb checkpoint\s+(.*)$")
REL = re.compile(r"durable checkpoint released LV2 ring prefix\s+(.*)$")
TS = re.compile(r"^(\d{4}-\d\d-\d\dT[\d:.]+)Z?")


def fields(rest):
    out = {}
    for tok in rest.split():
        k, _, v = tok.partition("=")
        if not v:
            continue
        try:
            out[k] = int(v)
        except ValueError:
            out[k] = v.strip('"')
    return out


def stamp(line):
    m = TS.match(line)
    if not m:
        return None
    try:
        return datetime.fromisoformat(m.group(1))
    except ValueError:
        return None


def main(argv):
    paths, skip_s, host_mbs = [], 0.0, None
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--skip-s":
            skip_s = float(argv[i + 1]); i += 2
        elif a == "--host-mbs":
            host_mbs = float(argv[i + 1]); i += 2
        else:
            paths.append(a); i += 1
    if not paths:
        print(__doc__)
        return 1

    requests, releases = {}, []
    for path in paths:
        for raw in open(path, errors="replace"):
            line = ANSI.sub("", raw)
            m = REQ.search(line)
            if m:
                f = fields(m.group(1))
                t = stamp(line)
                if t and "token" in f:
                    requests[f["token"]] = (t, f)
                continue
            m = REL.search(line)
            if m:
                f = fields(m.group(1))
                t = stamp(line)
                if t:
                    releases.append((t, f))

    if not releases:
        print("no `durable checkpoint released LV2 ring prefix` lines found -- "
              "was the engine started with RUST_LOG=info?")
        return 1

    releases.sort(key=lambda r: r[0])
    t0 = releases[0][0]
    rows, prev = [], None
    for t, f in releases:
        age = (t - t0).total_seconds()
        req = requests.get(f.get("token"))
        latency_ms = (t - req[0]).total_seconds() * 1e3 if req else float("nan")
        trigger = req[1].get("trigger", "?") if req else "?"
        gap = (t - prev).total_seconds() if prev else float("nan")
        prev = t
        rows.append((age, t, trigger, latency_ms, gap, f))

    kept = [r for r in rows if r[0] >= skip_s]
    if len(kept) < 2:
        print("fewer than 2 releases after --skip-s %.0f (have %d of %d)"
              % (skip_s, len(kept), len(rows)))
        return 1

    print("%-9s %-10s %9s %8s %10s %12s %11s" % (
        "t+s", "trigger", "req_ms", "gap_s", "entries", "MiB", "fill"))
    for age, _t, trigger, lat, gap, f in kept:
        print("%-9.1f %-10s %9.1f %8.2f %10d %12.1f %4s->%-4s" % (
            age, trigger, lat, gap,
            f.get("released_entries", 0),
            f.get("released_bytes", 0) / 2**20,
            f.get("fill_before", "?"), f.get("fill_after", "?")))

    span = kept[-1][0] - kept[0][0]
    # First kept release delivered bytes accumulated BEFORE the window opened;
    # rate is over the releases that land strictly inside it.
    inside = kept[1:]
    ent = sum(f.get("released_entries", 0) for *_r, f in inside)
    byt = sum(f.get("released_bytes", 0) for *_r, f in inside)
    lats = sorted(r[3] for r in kept if r[3] == r[3])
    gaps = sorted(r[4] for r in inside if r[4] == r[4])
    full = sum(1 for *_r, f in kept if f.get("fill_before", 0) >= 100)
    trig = {}
    for _a, _t, tr, *_r in kept:
        trig[tr] = trig.get(tr, 0) + 1

    print("\n-- %d releases over %.0f s (skipped first %.0f s) --" % (len(kept), span, skip_s))
    print("release rate        %9.1f MiB/s   %8.0f entries/s" % (byt / span / 2**20, ent / span))
    if host_mbs is not None:
        ratio = (byt / span / 1e6) / host_mbs if host_mbs else float("inf")
        print("host write rate     %9.1f MB/s    release/host = %.2fx" % (host_mbs, ratio))
        if ratio < 1.3:
            print("  ^ release rate TRACKS the host rate: the checkpoint path is the governor.")
        else:
            print("  ^ release rate exceeds the host rate: the ring is NOT the binding")
            print("    constraint in this window -- look at the append pipeline instead.")
    print("cadence   p50 %.2f s  p95 %.2f s  max %.2f s" % (
        gaps[len(gaps) // 2] if gaps else float("nan"),
        gaps[int(len(gaps) * 0.95)] if gaps else float("nan"),
        gaps[-1] if gaps else float("nan")))
    print("req->release  p50 %.1f ms  p95 %.1f ms  max %.1f ms" % (
        lats[len(lats) // 2] if lats else float("nan"),
        lats[int(len(lats) * 0.95)] if lats else float("nan"),
        lats[-1] if lats else float("nan")))
    print("per release   %.0f entries  %.1f MiB (mean)" % (
        ent / max(1, len(inside)), byt / max(1, len(inside)) / 2**20))
    print("arrived at a FULL ring (fill_before >= 100): %d of %d = %.0f%%" % (
        full, len(kept), 100.0 * full / len(kept)))
    print("triggers    " + "  ".join("%s=%d" % kv for kv in sorted(trig.items())))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
