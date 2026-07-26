#!/usr/bin/env python3
"""Attribute the LV2 global durability epoch from two `onyx-storage status` samples.

The global sync pipeline is three stages -- N shard preparers, M write lanes,
and ONE durability coordinator -- and `append_wait_durable` is the time a
foreground write spends behind all three. Every stage exports a closed ledger
(`idle` + its busy segments == wall clock per thread), so a delta over a
sampling interval says which stage is saturated rather than which one is slow.

Usage:
    tools/lv2_epoch_delta.py --cmd "onyx-storage -c config/nvme-chunklet.toml status" --interval 30
    tools/lv2_epoch_delta.py before.txt after.txt --interval 30
"""

import argparse
import subprocess
import sys
import time

STAGES = {
    "lv2_prepare": ("prepare", "threads", ["build", "send_block"], "batches"),
    "lv2_lane": ("lane", "threads", ["collect", "opsbuild", "write", "send_block"], "epochs"),
    "lv2_coord": ("coord", None, ["ckpt_encode", "ckpt_write", "flush", "publish"], "epochs"),
}


def parse(text):
    """Pull the three `lv2_*` status lines into {prefix: {key: int}}."""
    out = {}
    for line in text.splitlines():
        prefix, _, rest = line.partition(":")
        if prefix.strip() not in STAGES:
            continue
        fields = {}
        for token in rest.split():
            key, _, value = token.partition("=")
            try:
                fields[key] = int(value)
            except ValueError:
                continue
        out[prefix.strip()] = fields
    missing = set(STAGES) - set(out)
    if missing:
        sys.exit(f"status output has no {', '.join(sorted(missing))} line(s)")
    return out


def sample(cmd):
    done = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"status command failed ({done.returncode}): {done.stderr.strip()}")
    return done.stdout


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="*", help="two saved status outputs (before after)")
    ap.add_argument("--cmd", help="status command to sample twice instead of reading files")
    ap.add_argument("--interval", type=float, default=30.0, help="seconds between samples")
    args = ap.parse_args()

    if args.cmd:
        before_text = sample(args.cmd)
        started = time.monotonic()
        time.sleep(args.interval)
        after_text = sample(args.cmd)
        interval = time.monotonic() - started
    elif len(args.files) == 2:
        before_text = open(args.files[0]).read()
        after_text = open(args.files[1]).read()
        interval = args.interval
    else:
        ap.error("pass --cmd or exactly two status files")

    before, after = parse(before_text), parse(after_text)
    window_ns = interval * 1e9

    print(f"interval {interval:.1f}s")
    for prefix, (name, thread_key, segments, count_key) in STAGES.items():
        b, a = before[prefix], after[prefix]
        threads = a.get(thread_key, 1) if thread_key else 1
        threads = max(threads, 1)
        capacity = window_ns * threads
        idle = a.get("idle", 0) - b.get("idle", 0)
        parts = {seg: a.get(seg, 0) - b.get(seg, 0) for seg in segments}
        busy = sum(parts.values())
        count = a.get(count_key, 0) - b.get(count_key, 0)

        # `duty` is the fraction of the stage's total thread-time it is doing
        # work. A single-threaded stage approaching 100 % IS the ceiling.
        duty = busy / capacity * 100 if capacity else 0.0
        accounted = (busy + idle) / capacity * 100 if capacity else 0.0
        per = busy / count / 1000 if count else 0.0
        print(
            f"\n{name:<8} threads={threads:<3} duty={duty:6.2f}%  "
            f"idle={idle / capacity * 100 if capacity else 0:6.2f}%  "
            f"accounted={accounted:6.1f}%  {count_key}={count} ({per:.1f} us busy each)"
        )
        for seg, ns in parts.items():
            share = ns / busy * 100 if busy else 0.0
            print(
                f"    {seg:<12} {ns / 1e9:9.3f} s  {ns / capacity * 100 if capacity else 0:6.2f}% of capacity"
                f"  {share:5.1f}% of busy"
            )

    print(
        "\naccounted% far from 100 means a segment is unmeasured (or threads "
        "started/stopped mid-window); duty near 100 on `coord` means the single "
        "coordinator thread is the pipeline ceiling."
    )


if __name__ == "__main__":
    main()
