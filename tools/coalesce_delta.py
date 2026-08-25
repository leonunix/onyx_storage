#!/usr/bin/env python3
"""Coalesce-stage ledger from two `onyx status` samples.

The stage that binds the LV3 drain, decomposed. Existing counters accounted for
only 7.6% of these threads' wall time while the OS reported 93% CPU, so this
prints all three views side by side and never hides the residual:

  loop_ns      independent whole-iteration wall (anchor)
  active/idle  the loop's own split -- `idle` is a 10 ms blocking recv, so a
               large idle share with high thread CPU means that wait is not free
  walk_*       the admission walk; `arcs` vs `queued` is how much of it
               re-examined entries that are still in flight
  stop_*       ⭐ WHY the walk stopped, the discriminator for "nothing is
               saturated and the ring still backs up". The walk range is clamped
               at the shard's LV2 `synced_seq`:
                 exhausted -> no durable pending work left, so the coalescer is
                              PACED BY LV2 DURABILITY (look upstream, at the LV2
                              persistent-slot prepare/lane/coord threads)
                 budget    -> the entry cap or the 16 MiB window cut the walk
                              short, so an admissible backlog exists and the
                              constraint is DOWNSTREAM (chain latency / in-flight
                              depth)
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

exhausted = d("flush_coalesce_admit.stop_exhausted")
budget_stop = d("flush_coalesce_admit.stop_budget")
debt = d("flush_coalesce_admit.undurable_debt_sum")
stops = exhausted + budget_stop
if stops:
    print("  stop reason: exhausted=%d (%.1f%%)  budget=%d (%.1f%%)" % (
        exhausted, exhausted / stops * 100, budget_stop, budget_stop / stops * 100))
    print("  avg LV2 undurable debt %.1f entries beyond the watermark" % (
        debt / max(1, calls)))
    if exhausted / stops > 0.8:
        print("  ⭐ VERDICT: admission is LV2-DURABILITY PACED -- the coalescer ran")
        print("     out of durable work, so downstream idleness is a SYMPTOM. Look")
        print("     at the LV2 persistent-slot prepare/lane/coord ledger next")
        print("     (tools/lv2_epoch_delta.py), not at LV3 in-flight depth.")
    elif budget_stop / stops > 0.8:
        print("  ⭐ VERDICT: an admissible backlog EXISTS every cycle -- admission is")
        print("     not the pace-setter. The drain is bound DOWNSTREAM: chain")
        print("     latency / in-flight depth (tools/flush_delta.py).")
    else:
        print("  ⭐ VERDICT: mixed -- neither side dominates; widen the window.")
lbas = d("flush.lbas")
print("drain: %.0f lbas/s = %.1f MiB/s of LBA payload" % (lbas / w, lbas * 4096 / w / 1048576))
