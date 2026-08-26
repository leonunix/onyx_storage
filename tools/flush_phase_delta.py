#!/usr/bin/env python3
"""metadb checkpoint (flush) phase ledger from two `status` samples.

    flush_phase_delta.py <status_a> <status_b> <window_s>

WHY. Post-WAL the LV2 ring is released ONLY when a metadb checkpoint lands
(src/engine/durability.rs is the only `release_below` caller), one at a time,
and box-measured 2026-08-26 the request->release latency is **3.5-4.7 s p50**
while the release rate tracks the host write rate at only ~1.15x. So a
foreground append's `backpressure_wait` is, mechanically, a wait for a
checkpoint. This prints where those seconds go.

`metadb_flush: total_us` is the wall clock of one checkpoint and its phases are
additive (gate / sample / io / manifest / install / reclaim). `l2p_fold` and
`dedup_drain` are the PREPARE phases that run before `sample`.

⭐ The concurrency column is the point. `l2p_fold` is wall time for a fan-out of
one job per (volume, L2P shard) bounded by `metadb.parallel_l2p_drain_workers`
(default **4**, metadb/src/db/lifecycle.rs `run_scoped_jobs_bounded`), while
`l2p_fold_pipeline.apply_us` is the SUM over those shard jobs. apply/wall is
therefore the achieved worker concurrency:

  concurrency ~= the cap   -> the cap is binding; raising it shortens the
      checkpoint, which is what the ring is waiting for.
  concurrency << the cap   -> the fan-out is starved or serialising on
      something else (per-shard `tree.write()`, page alloc refill) and raising
      the cap will do nothing.

⚠ Judge an A/B of that knob on `l2p_fold` wall AND on the RC line: the default
of 4 is documented as "leave room for RC apply", so a fold win paid for out of
`rc_fold_lock_wait` is not a win.

⚠⚠ DENOMINATOR. Half of `metadb_flush.calls` are STEADY no-ops with zero
recorded time (`metadb_flush.total_us == forced_total_us` exactly), so charging
phases to `calls` halves all of them. This script charges to FORCED calls, which
is what makes the ledger close against the log-measured `req->release`.
"""
import sys


def parse(path):
    d = {}
    for line in open(path, errors="replace"):
        pre, _, rest = line.partition(":")
        pre = pre.strip()
        for tok in rest.split():
            k, _, v = tok.partition("=")
            if v.isdigit():
                d[pre + "." + k] = int(v)
    return d


def main(argv):
    if len(argv) < 3:
        print(__doc__)
        return 1
    a, b, window = parse(argv[0]), parse(argv[1]), float(argv[2])

    def d(key):
        return b.get(key, 0) - a.get(key, 0)

    all_calls = d("metadb_flush.calls")
    forced = d("metadb_flush_kind.forced_calls")
    steady = d("metadb_flush_kind.steady_calls")
    if all_calls <= 0:
        print("no checkpoints completed in this window (calls delta %d)" % all_calls)
        return 1

    # ⚠ HALF of `metadb_flush.calls` are STEADY no-ops that contribute ZERO
    # time: box 2026-08-26 measured `steady_total_us = 0` and
    # `metadb_flush.total_us == forced_total_us` to the microsecond. Dividing by
    # `calls` therefore halves every phase -- the same per-batch-vs-per-item
    # weighting trap that hid 2.46x in the LV2 ledger. Charge to FORCED calls:
    # those are the ones the onyx durability watermark requests and waits on.
    calls = forced if forced > 0 else all_calls
    print("flush calls: %d total = %d FORCED + %d steady-noop  (charging to forced)"
          % (all_calls, forced, steady))
    print("checkpoints: %d in %.0f s  -> one every %.2f s" % (calls, window, window / calls))
    print("pages %d  bytes %.1f MiB  (%.1f MiB per checkpoint)" % (
        d("metadb_flush.pages"), d("metadb_flush.bytes") / 2**20,
        d("metadb_flush.bytes") / calls / 2**20))
    print()

    total = d("metadb_flush.total_us")
    print("%-22s %11s %9s %7s" % ("phase", "ms/ckpt", "share", "max_ms"))
    rows = [
        ("PREPARE dedup_drain", "metadb_flush_prepare.dedup_drain_us", "metadb_flush_prepare.dedup_drain_max_us"),
        ("PREPARE l2p_fold", "metadb_flush_prepare.l2p_fold_us", "metadb_flush_prepare.l2p_fold_max_us"),
        ("gate", "metadb_flush.gate_us", "metadb_flush.gate_max_us"),
        ("sample", "metadb_flush.sample_us", "metadb_flush.sample_max_us"),
        ("io", "metadb_flush.io_us", "metadb_flush.io_max_us"),
        ("manifest", "metadb_flush.manifest_us", "metadb_flush.manifest_max_us"),
        ("install", "metadb_flush.install_us", "metadb_flush.install_max_us"),
        ("reclaim", "metadb_flush.reclaim_us", "metadb_flush.reclaim_max_us"),
    ]
    for label, key, maxkey in rows:
        v = d(key)
        print("%-22s %11.1f %8.1f%% %7.0f" % (
            label, v / calls / 1e3, 100.0 * v / total if total else 0.0,
            b.get(maxkey, 0) / 1e3))
    print("%-22s %11.1f %8s" % ("TOTAL (flush wall)", total / calls / 1e3, ""))
    print("  ^ PREPARE phases are outside `total_us`; the request->release")
    print("    latency seen by the ring is prepare + total + dispatch slack.")

    print("\n-- sample phase detail --")
    for label, key in [
        ("lock", "metadb_flush_sample_breakdown.lock_us"),
        ("l2p_walk", "metadb_flush_sample_breakdown.l2p_walk_us"),
        ("rc_checkpoint_wall", "metadb_flush_sample_breakdown.rc_checkpoint_wall_us"),
    ]:
        print("  %-20s %9.1f ms/ckpt" % (label, d(key) / calls / 1e3))

    print("\n-- io phase detail --")
    for label, key in [
        ("seal", "metadb_flush_io_breakdown.seal_us"),
        ("page_write", "metadb_flush_io_breakdown.page_write_us"),
        ("rc_meta", "metadb_flush_io_breakdown.rc_meta_us"),
        ("fsync", "metadb_flush_io_breakdown.fsync_us"),
    ]:
        print("  %-20s %9.1f ms/ckpt" % (label, d(key) / calls / 1e3))

    print("\n-- L2P fold fan-out (the ring's critical path) --")
    fold_wall = d("metadb_flush_prepare.l2p_fold_us")
    apply_cpu = d("metadb_l2p_fold_pipeline.apply_us")
    work_cpu = d("metadb_l2p_fold_pipeline.work_us")
    cycles = d("metadb_l2p_fold_pipeline.shard_cycles")
    print("  shard folds        %9d   (%.1f per checkpoint)" % (cycles, cycles / calls))
    print("  fold wall          %9.1f ms/ckpt" % (fold_wall / calls / 1e3))
    print("  fold CPU (work)    %9.1f ms/ckpt   of which apply %.0f%%" % (
        work_cpu / calls / 1e3, 100.0 * apply_cpu / work_cpu if work_cpu else 0.0))
    if fold_wall:
        print("  achieved concurrency %7.2f   <-- compare to parallel_l2p_drain_workers"
              % (work_cpu / fold_wall))
        print("  per-shard fold     %9.1f ms" % (work_cpu / cycles / 1e3 if cycles else 0))
        print("  ideal wall at cap N: %.0f ms (N=4)   %.0f ms (N=8)   %.0f ms (N=16)" % tuple(
            work_cpu / calls / 1e3 / n for n in (4, 8, 16)))

    print("\n-- L2P prefold pipeline (overlaps the fold with page IO) --")
    for label, key in [
        ("attempts", "metadb_l2p_checkpoint_pipeline.attempts"),
        ("completed", "metadb_l2p_checkpoint_pipeline.completed"),
        ("skipped", "metadb_l2p_checkpoint_pipeline.skipped"),
    ]:
        print("  %-20s %9d" % (label, d(key)))
    for label, key in [
        ("work", "metadb_l2p_checkpoint_pipeline.work_us"),
        ("wait", "metadb_l2p_checkpoint_pipeline.wait_us"),
    ]:
        print("  %-20s %9.1f ms/ckpt" % (label, d(key) / calls / 1e3))

    print("\n-- RC checkpoint (the knob's documented trade partner) --")
    for label, key in [
        ("fold_lock_wait", "metadb_flush_rc_checkpoint.fold_lock_wait_us"),
        ("fold_service", "metadb_flush_rc_checkpoint.fold_service_us"),
        ("stream_service", "metadb_flush_rc_checkpoint.stream_service_us"),
    ]:
        print("  %-20s %9.1f ms/ckpt" % (label, d(key) / calls / 1e3))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
