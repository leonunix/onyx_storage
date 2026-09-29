#!/usr/bin/env python3
"""Report a `page_pool_ab.sh` run in the order the judge has to read it.

Mechanism first, throughput last. The pool removes allocator churn (a 4 KiB
page is a one-region jemalloc slab, so every page alloc/free was an extent op
and every freed page a dirty extent to purge); the direct effect is fewer
`madvise` calls and fewer TLB-shootdown IPIs. Whether freed CPU turns into
MiB/s is a separate, weaker question.

    1. self-proof: pool takes / fresh / bypass follow the knob
    2. madvise/s, total and per thread (metadb-bfg-sync first)
    3. TLB / CAL IPI/s
    4. MiB/s, inside the same-arm bracket

Usage:  page_pool_report.py <rundir>
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ab_mechanism_report import read_irq, read_madvise  # noqa: E402

WATCH = ("metadb-bfg-sync", "metadb-async-re")


def read_status(path):
    out = {}
    if not os.path.exists(path):
        return out
    # ⛔ status shares an ANSI-coloured writer with engine.log; strip first.
    text = re.sub(r"\x1b\[[0-9;]*m", "", open(path, errors="replace").read())
    for line in text.splitlines():
        if line.startswith("metadb_page_pool:"):
            for key, value in re.findall(r"(\w+)=(\d+)", line):
                out[f"pool_{key}"] = int(value)
            cap = re.search(r"\bfree_pages=\d+/(\d+)", line)
            if cap:
                out["pool_max_free_pages"] = int(cap.group(1))
        elif line.startswith("metadb_flush:"):
            for key, value in re.findall(r"(\w+)=(\d+)", line):
                out[f"flush_{key}"] = int(value)
        elif line.startswith("volume_bytes:"):
            for key, value in re.findall(r"(\w+)=(\d+)", line):
                out[f"bytes_{key}"] = int(value)
        elif line.startswith("buffer_physical_fill_pct:"):
            out["ring_fill_pct"] = int(line.split(":")[1])
    return out


def read_int(path):
    try:
        return int(open(path).read().strip())
    except (OSError, ValueError):
        return None


def read_comm(path):
    counts = {}
    if os.path.exists(path):
        for line in open(path):
            parts = line.split()
            if len(parts) == 2:
                counts[parts[1]] = int(parts[0])
    return counts


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    run = sys.argv[1]
    segments = []
    for name in os.listdir(run):
        match = re.fullmatch(r"t\.(\w+)\.start", name)
        if match:
            segments.append(match.group(1))
    # Chronological, which is what the drift check needs.
    segments.sort(key=lambda s: os.path.getmtime(os.path.join(run, f"t.{s}.start")))

    rows = []
    for seg in segments:
        start = read_status(os.path.join(run, f"status.{seg}.start"))
        end = read_status(os.path.join(run, f"status.{seg}.end"))
        if not start or not end:
            continue
        try:
            wall = float(open(os.path.join(run, f"t.{seg}.end")).read()) - float(
                open(os.path.join(run, f"t.{seg}.start")).read()
            )
        except OSError:
            continue
        delta = lambda key: end.get(key, 0) - start.get(key, 0)  # noqa: E731
        irq_a = read_irq(os.path.join(run, f"irq.{seg}.start"))
        irq_b = read_irq(os.path.join(run, f"irq.{seg}.end"))
        madv, _, madv_secs = read_madvise(os.path.join(run, f"madvise.{seg}"))
        comm = read_comm(os.path.join(run, f"madvise_comm.{seg}"))
        knob_path = os.path.join(run, f"knob.{seg}")
        knob = open(knob_path).read().strip() if os.path.exists(knob_path) else "?"
        takes = delta("pool_takes")
        row = {
            "seg": seg,
            "knob": knob,
            "takes_s": takes / wall,
            "miss_pct": 100.0 * delta("pool_fresh") / takes if takes else None,
            "bypass_s": delta("pool_bypass") / wall,
            "overflow_s": delta("pool_overflow") / wall,
            "free_pages": end.get("pool_free_pages"),
            "peak_free": end.get("pool_peak_free_pages"),
            "cap_pages": end.get("pool_max_free_pages"),
            "madv_s": (madv / madv_secs) if madv and madv_secs else None,
            "tlb_s": (irq_b.get("TLB", 0) - irq_a.get("TLB", 0)) / wall if irq_a and irq_b else None,
            "cal_s": (irq_b.get("CAL", 0) - irq_a.get("CAL", 0)) / wall if irq_a and irq_b else None,
            "mibs": (delta("bytes_write") + delta("bytes_read")) / wall / (1 << 20),
            "write_mibs": delta("bytes_write") / wall / (1 << 20),
            "ring": end.get("ring_fill_pct"),
            # Pages per checkpoint: the regime marker. ring% oscillates too much
            # under checkpoint pressure to match arms on.
            "ckpt_pages": delta("flush_pages") / delta("flush_calls") if delta("flush_calls") else None,
            "rss_gib": (read_int(os.path.join(run, f"rss.{seg}.end")) or 0) / (1 << 20) or None,
            "comm": comm,
            "secs": madv_secs,
        }
        # An empty per-comm file means "not captured", NOT "0 calls".
        for name in WATCH:
            row[name] = (comm.get(name, 0) / madv_secs) if madv_secs and comm else None
        rows.append(row)

    if not rows:
        sys.exit(f"no complete segments in {run}")

    def fmt(v, spec):
        if v is not None:
            return format(v, spec)
        width = re.match(r"\d*", spec).group(0)
        return format("-", ">" + width) if width else "-"
    print(f"== {run}   arms: A = page pool on, B = heap pages (baseline)\n")
    print(
        f"{'seg':>4} {'knob':>4} {'takes/s':>9} {'miss%':>6} {'bypass/s':>9} {'ovfl/s':>7} "
        f"{'madvise/s':>10} {'bfg-sync/s':>10} {'async-re/s':>10} {'TLB IPI/s':>10} "
        f"{'CAL IPI/s':>10} {'MiB/s':>8} {'ring%':>6} {'pg/ckpt':>8} {'RSS GiB':>8}"
    )
    for r in rows:
        print(
            f"{r['seg']:>4} {r['knob']:>4} {r['takes_s']:>9,.0f} {fmt(r['miss_pct'], '6.2f')} "
            f"{r['bypass_s']:>9,.0f} {r['overflow_s']:>7,.0f} {fmt(r['madv_s'], '10,.0f')} "
            f"{fmt(r['metadb-bfg-sync'], '10,.0f')} {fmt(r['metadb-async-re'], '10,.0f')} "
            f"{fmt(r['tlb_s'], '10,.0f')} {fmt(r['cal_s'], '10,.0f')} {r['mibs']:>8.1f} "
            f"{r['ring'] if r['ring'] is not None else '-':>6} {fmt(r['ckpt_pages'], '8,.0f')} "
            f"{fmt(r['rss_gib'], '8.2f')}"
        )

    # Self-proof: ON must be served by the pool, OFF must bypass it entirely.
    bad = []
    for r in rows:
        if r["knob"] == "on" and (r["takes_s"] <= 0 or (r["miss_pct"] or 0) > 5.0):
            bad.append(r["seg"])
        # A few takes can land in an OFF window: counters are tallied per thread
        # and folded every 256 operations, so takes made just before the flip
        # are published just after it. Bound them relative to the bypass rate.
        if r["knob"] == "off" and (r["bypass_s"] <= 0 or r["takes_s"] > 0.001 * r["bypass_s"]):
            bad.append(r["seg"])
    if bad:
        print(f"\n⛔ SELF-PROOF FAILED in {bad}: the pool counters did not follow the knob — "
              "those segments are not the arm they are labelled as.")
    rings = [r["ring"] for r in rows if r["ring"] is not None]
    if rings and max(rings) - min(rings) > 10:
        print(f"\n⚠ ring% moved {min(rings)}..{max(rings)} across segments — the run spans more "
              "than one regime; compare only adjacent A/B pairs with matching ring%.")

    def arm(prefix, key):
        return [r[key] for r in rows if r["seg"].startswith(prefix) and r[key] is not None]

    mean = lambda xs: sum(xs) / len(xs) if xs else None  # noqa: E731
    print("\n-- arm means (A = pool on, B = baseline), with each arm's own spread")
    for key, label, better in (
        ("madv_s", "madvise/s", "lower"),
        ("metadb-bfg-sync", "bfg-sync/s", "lower"),
        ("tlb_s", "TLB IPI/s", "lower"),
        ("cal_s", "CAL IPI/s", "lower"),
        ("mibs", "MiB/s", "higher"),
        ("rss_gib", "RSS GiB", "lower"),
    ):
        a, b = arm("A", key), arm("B", key)
        if not a or not b:
            continue
        ma, mb = mean(a), mean(b)
        delta = 100.0 * (ma - mb) / mb if mb else float("nan")
        spread = lambda xs, m: 100.0 * (max(xs) - min(xs)) / m if m else float("nan")  # noqa: E731
        verdict = "EFFECT > both spreads" if abs(delta) > max(spread(a, ma), spread(b, mb)) else "inside the spread"
        print(
            f"   {label:<11} A {ma:>11,.1f} (±{spread(a, ma):4.1f}%)   B {mb:>11,.1f} (±{spread(b, mb):4.1f}%)"
            f"   A vs B {delta:+7.1f}%  ({better}; {verdict})"
        )

    print("\n-- madvise/s by thread (arm mean, top 12 by baseline)")
    comms = {}
    for r in rows:
        if not r["secs"] or not r["comm"]:
            continue
        for name, count in r["comm"].items():
            comms.setdefault(name, {"A": [], "B": []})[r["seg"][0]].append(count / r["secs"])
    avg = lambda xs: sum(xs) / len(xs) if xs else 0.0  # noqa: E731
    for name, arms in sorted(comms.items(), key=lambda kv: -avg(kv[1]["B"]))[:12]:
        print(f"   {name:<18} A {avg(arms['A']):>9,.0f}   B {avg(arms['B']):>9,.0f}")

    print("\n-- pool state at segment end (A arms); peak is the process-lifetime high-water")
    for r in rows:
        if r["knob"] == "on":
            print(
                f"   {r['seg']}: free_pages={r['free_pages']}  peak={r['peak_free']}  cap={r['cap_pages']}"
                f"  overflow/s={r['overflow_s']:,.1f}  miss%={fmt(r['miss_pct'], '.2f')}"
            )


if __name__ == "__main__":
    main()
