#!/usr/bin/env python3
"""Attribute the sampled append-order critical section from two status samples.

Usage:
    tools/append_order_delta.py before.txt after.txt
    tools/append_order_delta.py --cmd "onyx-storage -c config.toml status" --interval 30
"""

import argparse
import subprocess
import time


DETAILS = [
    ("reserve", "reserve_ns"),
    ("supersede_scan", "supersede_scan_ns"),
    ("index_publish", "index_publish_ns"),
    ("ring_publish", "ring_publish_ns"),
    ("cache_publish", "cache_publish_ns"),
    ("stage_turn_wait", "stage_turn_wait_ns"),
    ("stage_send", "stage_send_ns"),
]

RESERVE_DETAILS = [
    ("initial_ring_lock", "reserve_ring_lock_ns"),
    ("frontier_read_lock", "reserve_frontier_lock_ns"),
    ("ring_relock", "reserve_ring_relock_ns"),
]


def parse(text):
    result = {}
    for line in text.splitlines():
        prefix, _, rest = line.partition(":")
        prefix = prefix.strip()
        if prefix not in {"buffer", "front_write_ns", "append_order_detail", "stage_reorder"}:
            continue
        fields = {}
        for token in rest.split():
            key, _, value = token.partition("=")
            try:
                fields[key] = int(value)
            except ValueError:
                continue
        result[prefix] = fields
    return result


def sample(command):
    done = subprocess.run(command, shell=True, capture_output=True, text=True)
    done.check_returncode()
    return done.stdout


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="*")
    parser.add_argument("--cmd")
    parser.add_argument("--interval", type=float, default=30.0)
    args = parser.parse_args()

    if args.cmd:
        before_text = sample(args.cmd)
        time.sleep(args.interval)
        after_text = sample(args.cmd)
    elif len(args.files) == 2:
        before_text = open(args.files[0]).read()
        after_text = open(args.files[1]).read()
    else:
        parser.error("pass --cmd or exactly two status files")

    before = parse(before_text)
    after = parse(after_text)
    for prefix in ("buffer", "front_write_ns", "append_order_detail", "stage_reorder"):
        if prefix not in before or prefix not in after:
            raise SystemExit(f"status output has no {prefix} line")

    def delta(prefix, field):
        return after[prefix].get(field, 0) - before[prefix].get(field, 0)

    samples = delta("append_order_detail", "samples")
    backpressure_skips = delta("append_order_detail", "backpressure_skips")
    appends = delta("buffer", "appends")
    if samples <= 0 or appends <= 0:
        raise SystemExit(f"no samples: detail={samples} appends={appends}")

    hold_us = delta("front_write_ns", "append_order_hold") / appends / 1000
    backpressure_us = delta("front_write_ns", "append_backpressure_wait") / appends / 1000
    print(
        f"appends={appends} detail_samples={samples} "
        f"backpressure_skips={backpressure_skips} sample_rate=1/{appends / samples:.1f}"
    )
    print(f"append_order_hold(all appends)={hold_us:.1f} us")
    window_waits = delta("stage_reorder", "stage_window_wait_events")
    window_wait_us = (
        delta("stage_reorder", "stage_window_wait_ns") / window_waits / 1000
        if window_waits
        else 0
    )
    out_of_order = delta("stage_reorder", "out_of_order")
    gap_events = delta("stage_reorder", "gap_events")
    gap_wait_us = (
        delta("stage_reorder", "gap_wait_ns") / gap_events / 1000 if gap_events else 0
    )
    print(f"stage_window_wait(slow path)={window_wait_us:.1f} us events={window_waits}")
    print(
        f"reorder out_of_order={out_of_order} ({out_of_order / appends * 100:.2f}%) "
        f"gaps={gap_events} ({gap_events / appends * 100:.2f}%) "
        f"gap_wait={gap_wait_us:.1f} us/event current={after['stage_reorder'].get('current', 0)} "
        f"max={after['stage_reorder'].get('max', 0)} "
        f"max_distance={after['stage_reorder'].get('max_distance', 0)}"
    )
    print(
        "reorder faults={} duplicate={} stale={}".format(
            delta("stage_reorder", "faults"),
            delta("stage_reorder", "duplicate"),
            delta("stage_reorder", "stale"),
        )
    )
    print()
    print(f"{'stage':<22} {'mean us':>12} {'share':>9}")
    values = []
    for label, field in DETAILS:
        mean_us = delta("append_order_detail", field) / samples / 1000
        values.append((label, mean_us))
    accounted = sum(value for _, value in values)
    for label, mean_us in values:
        share = mean_us / accounted * 100 if accounted else 0
        print(f"{label:<22} {mean_us:12.1f} {share:8.1f}%")
    closed = accounted + backpressure_us
    print(f"{'sampled_accounted':<22} {accounted:12.1f}")
    print(f"{'backpressure_wait':<22} {backpressure_us:12.1f}")
    print(f"{'accounted_with_bp':<22} {closed:12.1f}")
    print(f"{'hold_minus_closed':<22} {hold_us - closed:12.1f}")
    print()
    print("reserve breakdown (parts of reserve, not additive above):")
    reserve_parts = []
    for label, field in RESERVE_DETAILS:
        mean_us = delta("append_order_detail", field) / samples / 1000
        reserve_parts.append((label, mean_us))
        print(f"  {label:<20} {mean_us:10.1f} us")
    reserve_mean = dict(values)["reserve"]
    print(f"  {'reserve_other':<20} {reserve_mean - sum(v for _, v in reserve_parts):10.1f} us")


if __name__ == "__main__":
    main()
