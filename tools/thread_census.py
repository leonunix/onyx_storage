#!/usr/bin/env python3
"""Thread census: how many of the engine's threads are actually contending for CPU.

Thread COUNT is not CPU contention. `onyx status` says nothing about it and
neither does `ps`: a cvar-parked thread costs a wake, not a timeslice. This
samples `/proc/<pid>/task/*/stat` repeatedly over a window and reports, per
thread GROUP, the mean number of threads found Runnable — plus the involuntary
context-switch rate, which is what preemption actually looks like from inside
a thread.

Why this exists: memory `chunklet_call_slowdown_is_cpu_starvation` proved that
40 competing spinners inflate chunklet's `compute` phase 5.1x and collapse
throughput 803 -> 23 MiB/s, but left one step open -- whether onyx's own
executor threads actually starve. `nonvoluntary_ctxt_switches` per group is the
discriminator. If utilisation is low and involuntary switches are rare, the
starvation reading is wrong.

⛔ Sampling only. Never gdb-attach or SIGSTOP a live ublk engine: the kernel
ublk timeout kills the device (memory `never_stop_the_world_on_live_ublk`).
Reading /proc is safe -- it neither stops nor signals the target.

Usage:
    thread_census.py <pid> [--samples 100] [--interval 0.2] [--top 40]
    thread_census.py $(pgrep -nx onyx-storage)

Output columns:
    threads     threads in the group at the last sample
    meanR       mean count in state R (runnable: running OR waiting for CPU)
    maxR        highest R count seen in any single sample
    R%          meanR / threads -- the group's own runnable fraction
    D           mean count in uninterruptible sleep (blocked in kernel IO)
    nv/s        involuntary context switches per second, summed over the group
    nv/thr/s    involuntary switches per second PER THREAD -- compare across
                groups regardless of group size. This is the preemption rate.
"""

import argparse
import collections
import os
import re
import sys
import time

# `comm` in /proc/<tid>/stat is capped at 15 chars by the kernel (TASK_COMM_LEN
# is 16 including the NUL), so long names arrive pre-truncated:
# "flusher-compress-12" -> "flusher-compres". Grouping therefore has to tolerate
# a name that lost its ordinal to truncation as well as one that still has it.
TRAILING_ORDINAL = re.compile(r"[-_]?\d+$")


def group_of(comm):
    """Collapse a thread name to its pool name: drop trailing ordinals.

    Applied repeatedly because several pools are named "<pool>-<shard>-<worker>"
    (e.g. "flusher-dedup-3-1"), and truncation can leave a partial ordinal.
    """
    name = comm
    while True:
        stripped = TRAILING_ORDINAL.sub("", name)
        if stripped == name or not stripped:
            return name
        name = stripped


def read_stat(pid, tid):
    """Return (comm, state) or None if the thread vanished mid-sample.

    `comm` can itself contain spaces and parens, so the only safe split is at
    the LAST ')' -- field order after it is fixed.
    """
    try:
        with open(f"/proc/{pid}/task/{tid}/stat", "rb") as fh:
            raw = fh.read().decode("utf-8", "replace")
    except OSError:
        return None
    close = raw.rfind(")")
    open_paren = raw.find("(")
    if close < 0 or open_paren < 0 or close < open_paren:
        return None
    comm = raw[open_paren + 1 : close]
    rest = raw[close + 2 :].split()
    if not rest:
        return None
    return comm, rest[0]


def read_switches(pid, tid):
    """Return (voluntary, nonvoluntary) switch counts, or None."""
    try:
        with open(f"/proc/{pid}/task/{tid}/status", "rb") as fh:
            vol = nonvol = None
            for line in fh.read().decode("utf-8", "replace").splitlines():
                if line.startswith("voluntary_ctxt_switches:"):
                    vol = int(line.split()[1])
                elif line.startswith("nonvoluntary_ctxt_switches:"):
                    nonvol = int(line.split()[1])
            if vol is None or nonvol is None:
                return None
            return vol, nonvol
    except (OSError, ValueError, IndexError):
        return None


def tids(pid):
    try:
        return os.listdir(f"/proc/{pid}/task")
    except OSError:
        sys.exit(f"pid {pid} is gone (or /proc/{pid}/task is unreadable)")


def switch_snapshot(pid):
    """tid -> (comm_group, voluntary, nonvoluntary) for every live thread."""
    out = {}
    for tid in tids(pid):
        stat = read_stat(pid, tid)
        sw = read_switches(pid, tid)
        if stat is None or sw is None:
            continue
        out[tid] = (group_of(stat[0]), sw[0], sw[1])
    return out


def main():
    ap = argparse.ArgumentParser(
        description="Per-group runnable and preemption census for a live engine."
    )
    ap.add_argument("pid", type=int)
    ap.add_argument(
        "--samples", type=int, default=100, help="state samples to take (default 100)"
    )
    ap.add_argument(
        "--interval",
        type=float,
        default=0.2,
        help="seconds between state samples (default 0.2 -> 20 s window at 100)",
    )
    ap.add_argument(
        "--top", type=int, default=40, help="groups to print, by meanR (default 40)"
    )
    args = ap.parse_args()

    if not os.path.isdir(f"/proc/{args.pid}"):
        sys.exit(f"no such pid: {args.pid}")

    # Switch counters are cumulative since thread start, so the rate needs a
    # first/last pair around the same window the state sampling covers.
    first_switches = switch_snapshot(args.pid)
    t_start = time.monotonic()

    runnable_per_sample = collections.defaultdict(list)
    d_per_sample = collections.defaultdict(list)
    last_total = collections.Counter()
    samples_taken = 0

    for _ in range(args.samples):
        counts_r = collections.Counter()
        counts_d = collections.Counter()
        counts_all = collections.Counter()
        for tid in tids(args.pid):
            stat = read_stat(args.pid, tid)
            if stat is None:
                continue
            group = group_of(stat[0])
            counts_all[group] += 1
            if stat[1] == "R":
                counts_r[group] += 1
            elif stat[1] == "D":
                counts_d[group] += 1
        # A group absent from this sample still contributes a 0, otherwise its
        # mean is computed over fewer samples than its peers and reads high.
        for group in counts_all:
            last_total[group] = counts_all[group]
        for group in last_total:
            runnable_per_sample[group].append(counts_r.get(group, 0))
            d_per_sample[group].append(counts_d.get(group, 0))
        samples_taken += 1
        time.sleep(args.interval)

    last_snapshot = switch_snapshot(args.pid)
    elapsed = time.monotonic() - t_start

    # Only threads present in BOTH snapshots have a valid delta; a thread that
    # started mid-window would otherwise report its entire lifetime as rate.
    nonvol_delta = collections.Counter()
    vol_delta = collections.Counter()
    delta_threads = collections.Counter()
    for tid, (group, vol0, nonvol0) in first_switches.items():
        later = last_snapshot.get(tid)
        if later is None or later[0] != group:
            continue
        vol_delta[group] += max(0, later[1] - vol0)
        nonvol_delta[group] += max(0, later[2] - nonvol0)
        delta_threads[group] += 1

    total_threads = sum(last_total.values())
    mean_r_total = sum(
        sum(v) / len(v) for v in runnable_per_sample.values() if v
    )
    nonvol_total = sum(nonvol_delta.values())

    print(
        f"pid {args.pid} | {samples_taken} samples over {elapsed:.1f}s "
        f"| {total_threads} threads | mean runnable {mean_r_total:.1f}"
    )
    print(
        f"{'group':<26}{'threads':>8}{'meanR':>8}{'maxR':>6}{'R%':>7}"
        f"{'D':>7}{'nv/s':>10}{'nv/thr/s':>10}{'vol/s':>10}"
    )
    print("-" * 92)

    rows = []
    for group, samples in runnable_per_sample.items():
        if not samples:
            continue
        threads = last_total[group]
        mean_r = sum(samples) / len(samples)
        mean_d = sum(d_per_sample[group]) / len(d_per_sample[group])
        nv_rate = nonvol_delta[group] / elapsed if elapsed > 0 else 0.0
        per_thread = nv_rate / delta_threads[group] if delta_threads[group] else 0.0
        vol_rate = vol_delta[group] / elapsed if elapsed > 0 else 0.0
        rows.append((mean_r, group, threads, mean_r, max(samples), mean_d, nv_rate, per_thread, vol_rate))

    rows.sort(reverse=True)
    for _, group, threads, mean_r, max_r, mean_d, nv_rate, per_thread, vol_rate in rows[
        : args.top
    ]:
        pct = 100.0 * mean_r / threads if threads else 0.0
        print(
            f"{group:<26}{threads:>8}{mean_r:>8.2f}{max_r:>6}{pct:>6.1f}%"
            f"{mean_d:>7.2f}{nv_rate:>10.1f}{per_thread:>10.2f}{vol_rate:>10.1f}"
        )

    if len(rows) > args.top:
        print(f"... {len(rows) - args.top} more groups (raise --top)")
    print("-" * 92)
    print(
        f"{'TOTAL':<26}{total_threads:>8}{mean_r_total:>8.2f}{'':>6}{'':>7}"
        f"{'':>7}{nonvol_total / elapsed if elapsed else 0:>10.1f}"
    )
    print()
    print(
        "meanR is the count CONTENDING for CPU. Compare mean runnable against the\n"
        "confined core count (nvme-box: 44) -- that ratio, not the thread total, is\n"
        "the oversubscription. nv/thr/s is how hard each thread is being preempted."
    )


if __name__ == "__main__":
    main()
