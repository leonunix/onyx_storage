#!/usr/bin/env python3
"""Coalesce-stage ledger from two `onyx status` samples.

The stage that binds the LV3 drain, decomposed. Existing counters accounted for
only 7.6% of these threads' wall time while the OS reported 93% CPU, so this
prints all three views side by side and never hides the residual:

  loop_ns      independent whole-iteration wall (anchor)
  active/idle  the loop's own split -- `idle` is a 10 ms blocking recv, so a
               large idle share with high thread CPU means that wait is not free
  walk_*       the admission walk, which restarts at the OLDEST pending seq on
               every cycle; `arcs` vs `queued` is how much of it re-examined
               entries that are still in flight
  pending_ns   coalesce_pending (dedup + sort + merge), i.e. real unit building

Usage: coalesce_delta.py <status_a> <status_b> <window_s> [coalesce_threads]
"""
import sys


def parse(path):
    d = {}
    for line in open(path):
        pre, _, rest = line.partition(":")
        for tok in rest.split():
            k, _, v = tok.partition("=")
            if v.isdigit():
                d[pre.strip() + "." + k] = int(v)
    return d


a, b = parse(sys.argv[1]), parse(sys.argv[2])
w = float(sys.argv[3])
threads = int(sys.argv[4]) if len(sys.argv) > 4 else 16
budget = threads * w


def d(k):
    return b.get(k, 0) - a.get(k, 0)


def row(label, ns, note=""):
    print("  %-26s %9.1f s  %6.1f%% of %d threads  %s" % (
        label, ns / 1e9, ns / 1e9 / budget * 100, threads, note))


iters = d("flush_coalesce_admit.loop_iters")
print("window %.0f s   %d coalesce threads   %d iterations (%.0f/s/thread)" % (
    w, threads, iters, iters / w / threads if iters else 0))
row("loop total", d("flush_coalesce_admit.loop_ns"), "independent anchor")
row("  active (loop's view)", d("flush_stage_busy.coalesce_active_ns"))
row("  idle (10 ms recv)", d("flush_stage_busy.coalesce_idle_ns"),
    "⚠ compare against thread CPU: a parked wait must not cost CPU")
row("  admission walk", d("flush_coalesce_admit.walk_ns"))
row("  coalesce_pending", d("flush_coalesce_inside.pending_ns"))
row("    phase2 dedup", d("flush_coalesce_inside.phase2_dedup_ns"))
row("    phase3 sort", d("flush_coalesce_inside.phase3_sort_ns"))
row("    phase4 merge", d("flush_coalesce_inside.phase4_merge_ns"))
resid = (d("flush_coalesce_admit.loop_ns") - d("flush_stage_busy.coalesce_idle_ns")
         - d("flush_coalesce_admit.walk_ns") - d("flush_coalesce_inside.pending_ns"))
row("  residual", resid, "loop - idle - walk - pending")

calls = d("flush_coalesce_admit.walk_calls")
arcs = d("flush_coalesce_admit.walk_arcs")
queued = d("flush_coalesce_admit.queued")
skips = {k: d("flush_coalesce_admit.skip_" + k)
         for k in ("inflight", "seen", "window", "other")}
print("admission: %d walks (%.0f/s), %d arcs cloned (%.1f per walk), %d queued" % (
    calls, calls / w, arcs, arcs / max(1, calls), queued))
if arcs:
    print("  waste ratio arcs/queued = %.1fx   (1.0 = every cloned entry was used)"
          % (arcs / max(1, queued)))
print("  skips: " + "  ".join("%s=%d (%.0f/s)" % (k, v, v / w) for k, v in skips.items()))
if calls:
    print("  walk cost %.1f us/call, %.2f us/arc" % (
        d("flush_coalesce_admit.walk_ns") / calls / 1000,
        d("flush_coalesce_admit.walk_ns") / max(1, arcs) / 1000))
lbas = d("flush.lbas")
print("drain: %.0f lbas/s = %.1f MiB/s of LBA payload" % (lbas / w, lbas * 4096 / w / 1048576))
