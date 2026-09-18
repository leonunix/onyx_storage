#!/usr/bin/env python3
"""One-line-per-segment table for tools/core_budget_ab.sh (and thread_budget_ab.sh) runs.

    core_budget_report.py <run_dir> [group ...]

Reads the layout both harnesses write: `t.<seg>.{start,end}` (window bounds),
`status.<seg>.{start,end}` (counter deltas) and `census.<seg>`
(tools/thread_census.py output). Segments are ordered by their start time, not
by name, so the drift check below reads chronologically.

COLUMN ORDER IS THE JUDGE ORDER. A core-budget arm is rejected at the first
column that fails, and the first two are NOT throughput:

  aw us/c   metadb apply_wait per commit. THE KILL CRITERION for any arm that
            fences metadb's apply lanes: a commit needs every touched shard's
            lane started, so crowding them shows up here first
            (memory `metadb_apply_lane_count_is_topology_not_pool_size`).
  fenceR    sum of the fenced groups' mean runnable, and its utilisation of the
            fenced CPUs. <80% means the reservation is manufacturing private
            IDLE capacity, which is exactly how the LV2 dedication lost
            (memory `lv2_dedication_throughput_retracted`). Judge SIZE here.
  vol/MiB   voluntary switches per MiB of host IO — handoff work per unit data.
            This is the only column that moved for a thread cut so far
            (memory `phase1_thread_cuts_remove_handoff_work`: 314 -> 275).
  inv/MiB   involuntary per MiB — preemption per unit data. Has been flat for
            every cut and ROSE 17.5% for the wait-class fence.
  MiB/s     read LAST, and only against the spread of the repeated segments.

⛔ The footer prints the segments in time order specifically so a MONOTONE
batch is visible: p13's four restart-per-arm segments rose 786.8 -> 948.3 and
could not resolve a 9.6% effect. If the baseline segments are monotone, the
throughput column is unusable no matter how many arms were run.
"""
import glob
import os
import re
import sys

# The wait class (`cores.wait_class_groups`), by census group name.
FENCE_GROUPS = ["onyx-metadb-app", "metaio", "flusher-writer-", "flusher-post-co"]
# The groups a fence is supposed to STOP preempting, i.e. where its mechanism
# has to show up. Falling `nv/thr/s` here is the arm actually happening.
BUSY_GROUPS = [
    "read-pool-fg",
    "read-pool",
    "ublk-io-worker-",
    "flusher-dedup-s",
    "flusher-compres",
    "persistent-slot",
    "lv3-batch-exec-",
]


def status(path):
    """`onyx status` -> {"<prefix>.<key>": int}. Non-numeric values are skipped."""
    out = {}
    for line in open(path):
        prefix, _, rest = line.partition(":")
        prefix = prefix.strip()
        for token in rest.split():
            key, _, value = token.partition("=")
            if value.isdigit():
                out[prefix + "." + key] = int(value)
    return out


def census(path):
    head = open(path).readline()
    threads = int(re.search(r"(\d+) threads", head).group(1))
    mean_runnable = float(re.search(r"mean runnable ([\d.]+)", head).group(1))
    rows = {}
    for line in open(path):
        f = line.split()
        # threads meanR maxR R% D nv/s nv/thr/s vol/s, and not the header row.
        if len(f) == 9 and f[4].endswith("%") and f[0] != "group":
            rows[f[0]] = {
                "thr": int(f[1]),
                "meanR": float(f[2]),
                "maxR": int(f[3]),
                "nv_s": float(f[6]),
                "nv_thr": float(f[7]),
                "vol_s": float(f[8]),
            }
    return threads, mean_runnable, rows


def fence_cpus(run_dir, seg):
    """Logical CPUs in the fence, read off the budget the engine echoed back."""
    path = os.path.join(run_dir, "budget." + seg)
    if not os.path.exists(path):
        return 0
    m = re.search(r"wait_fence=\[([^\]]*)\]", open(path).read())
    if not m or not m.group(1).strip():
        return 0
    return len([x for x in m.group(1).split(",") if x.strip()])


def main(argv):
    if not argv:
        print(__doc__)
        return 1
    run_dir = argv[0]
    extra_groups = argv[1:]

    segs = [
        re.sub(r"^t\.|\.start$", "", os.path.basename(p))
        for p in glob.glob(os.path.join(run_dir, "t.*.start"))
    ]
    segs = [s for s in segs if os.path.exists(os.path.join(run_dir, "status.%s.end" % s))]
    if not segs:
        print("no completed segments in %s" % run_dir)
        return 1
    segs.sort(key=lambda s: int(open(os.path.join(run_dir, "t.%s.start" % s)).read()))

    print(
        "%-22s %5s %6s %8s %7s %8s %8s %9s %6s %5s"
        % ("seg", "thr", "meanR", "aw us/c", "fenceR", "vol/MiB", "inv/MiB", "MiB/s", "fill%", "err")
    )
    data = {}
    for seg in segs:
        a = status(os.path.join(run_dir, "status.%s.start" % seg))
        b = status(os.path.join(run_dir, "status.%s.end" % seg))
        t0 = int(open(os.path.join(run_dir, "t.%s.start" % seg)).read())
        t1 = int(open(os.path.join(run_dir, "t.%s.end" % seg)).read())
        window = max(1, t1 - t0)

        def d(key):
            return b.get(key, 0) - a.get(key, 0)

        mib = (d("volume_bytes.read") + d("volume_bytes.write")) / window / 2**20
        commits = d("metadb_commit.success")
        apply_wait = d("metadb_commit.apply_wait_us") / commits if commits else float("nan")
        threads, mean_runnable, rows = census(os.path.join(run_dir, "census.%s" % seg))
        invol = sum(r["nv_s"] for r in rows.values())
        vol = sum(r["vol_s"] for r in rows.values())
        fenced = sum(r["meanR"] for g, r in rows.items() if g in FENCE_GROUPS)
        cpus = fence_cpus(run_dir, seg)
        fill = [
            int(l.split(":")[1])
            for l in open(os.path.join(run_dir, "status.%s.end" % seg))
            if l.startswith("buffer_physical_fill_pct")
        ]
        print(
            "%-22s %5d %6.1f %8.0f %7.2f %8.1f %8.1f %9.1f %6s %5d"
            % (
                seg,
                threads,
                mean_runnable,
                apply_wait,
                fenced,
                vol / mib if mib else float("nan"),
                invol / mib if mib else float("nan"),
                mib,
                fill[0] if fill else "-",
                d("flush.errors"),
            )
        )
        data[seg] = dict(
            thr=threads, meanR=mean_runnable, aw=apply_wait, fenced=fenced, cpus=cpus,
            vol=vol, invol=invol, mib=mib, rows=rows,
        )

    # ⚠ Arms are grouped by the budget the engine ACTUALLY ECHOED BACK, never by
    # the segment name. Name-prefix grouping silently merged `W5` (a 5-core
    # fence) into `W` (4 cores), because stripping trailing digits cannot tell
    # an arm's own digit from its instance number — and the merged row then
    # reported a fence utilisation belonging to neither arm.
    arms, labels = {}, {}
    for seg in segs:
        budget = os.path.join(run_dir, "budget." + seg)
        if os.path.exists(budget):
            key = open(budget).read().strip()
        else:
            # thread_budget_ab.sh names segments `<idx>.<knob-string>`; the knob
            # string IS the arm identity.
            m = re.match(r"^\d+\.(.*)$", seg)
            key = m.group(1) if m else seg.rstrip("0123456789")
        arms.setdefault(key, []).append(seg)
        labels.setdefault(key, seg)
    print()
    base = None
    order_keys = sorted(arms, key=lambda k: segs.index(arms[k][0]))
    for arm_key in order_keys:
        members = arms[arm_key]
        arm = labels[arm_key][:22]
        mibs = [data[s]["mib"] for s in members]
        mean = sum(mibs) / len(mibs)
        spread = (max(mibs) - min(mibs)) / mean * 100 if len(mibs) > 1 else 0.0
        avg = lambda k: sum(data[s][k] for s in members) / len(members)  # noqa: E731
        cpus = max(data[s]["cpus"] for s in members)
        util = " fence %3.0f%% used" % (avg("fenced") / cpus * 100) if cpus else ""
        if base is None:
            base = mean
        print(
            "%-22s n=%d thr %3.0f meanR %5.1f | aw %5.0f us | vol/MiB %6.1f inv/MiB %6.1f | "
            "MiB/s %6.1f spread %5.2f%% vs first arm %+6.2f%%%s"
            % (arm, len(members), avg("thr"), avg("meanR"), avg("aw"), avg("vol") / mean,
               avg("invol") / mean, mean, spread, (mean - base) / base * 100, util)
        )

    # ⛔ Drift check. A monotone batch means the throughput column is unusable.
    order = [data[s]["mib"] for s in segs]
    rising = all(x < y for x, y in zip(order, order[1:]))
    falling = all(x > y for x, y in zip(order, order[1:]))
    print()
    print("time order: " + " -> ".join("%.1f" % x for x in order))
    if rising or falling:
        print(
            "⛔ MONOTONE in time: this batch is dominated by drift, NOT by the knob. "
            "Do not claim a throughput magnitude."
        )

    print()
    print("-- nv/thr/s per group (preemption), first arm vs each other arm --")
    names = list(dict.fromkeys(BUSY_GROUPS + FENCE_GROUPS + extra_groups))
    first = order_keys[0]
    header = "%-18s %5s %9s" % ("group", "thr", labels[first][:9])
    others = order_keys[1:]
    for a in others:
        header += " %9s %7s" % (labels[a][:9], "delta")
    print(header)
    for g in names:
        def gmean(arm, key):
            vals = [data[s]["rows"][g][key] for s in arms[arm] if g in data[s]["rows"]]
            return sum(vals) / len(vals) if vals else float("nan")

        if all(g not in data[s]["rows"] for s in segs):
            continue
        base_v = gmean(first, "nv_thr")
        line = "%-18s %5.0f %9.1f" % (g, gmean(first, "thr"), base_v)
        for a in others:
            v = gmean(a, "nv_thr")
            line += " %9.1f %6.1f%%" % (v, (v - base_v) / base_v * 100 if base_v else float("nan"))
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
