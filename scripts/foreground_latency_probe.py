#!/usr/bin/env python3
"""Sample foreground latency, per-shard ring movement, and scheduler delay."""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import socket
import time
from typing import Any


def command(path: pathlib.Path, name: str, timeout: float) -> dict[str, Any]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.connect(str(path))
        sock.sendall((name + "\n").encode())
        lines = []
        for raw in sock.makefile("r", encoding="utf-8"):
            line = raw.strip()
            if line == "ok" or line.startswith("ok "):
                break
            if line.startswith("error:"):
                raise RuntimeError(line)
            if line:
                lines.append(line)
    return json.loads(lines[0]) if lines else {}


def process_id(explicit: int | None) -> int:
    if explicit:
        return explicit
    for entry in pathlib.Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            if (entry / "comm").read_text().strip() == "onyx-storage":
                return int(entry.name)
        except OSError:
            pass
    raise RuntimeError("onyx-storage process not found")


def schedstat(pid: int) -> dict[str, dict[str, int]]:
    totals: dict[str, dict[str, int]] = {}
    for task in pathlib.Path(f"/proc/{pid}/task").iterdir():
        try:
            name = (task / "comm").read_text().strip()
            runtime, delay, switches = map(int, (task / "schedstat").read_text().split()[:3])
        except (OSError, ValueError):
            continue
        role = name.split("-")[0]
        row = totals.setdefault(role, {"threads": 0, "runtime_ns": 0, "runqueue_ns": 0, "switches": 0})
        row["threads"] += 1
        row["runtime_ns"] += runtime
        row["runqueue_ns"] += delay
        row["switches"] += switches
    return totals


def nonnegative(now: int, old: int) -> int:
    return max(0, now - old)


def percentile_ns(buckets: list[int], percentile: float) -> int:
    total = sum(buckets)
    if total == 0:
        return 0
    target = max(1, int((total * percentile + 99) // 100))
    seen = 0
    for idx, count in enumerate(buckets):
        seen += count
        if seen >= target:
            return 0 if idx == 0 else 1 << idx
    return 1 << (len(buckets) - 1)


def bounded_percentile_ns(buckets: list[int], bounds: list[int], percentile: float) -> int:
    total = sum(buckets)
    if total == 0 or not bounds:
        return 0
    target = max(1, int((total * percentile + 99) // 100))
    seen = 0
    for idx, count in enumerate(buckets):
        seen += count
        if seen >= target:
            return int(bounds[min(idx, len(bounds) - 1)])
    return int(bounds[-1])


def bucket_delta(now: dict[str, Any], old: dict[str, Any], key: str) -> list[int]:
    current = now.get(key, [])
    previous = old.get(key, [])
    return [nonnegative(int(value), int(previous[idx]) if idx < len(previous) else 0) for idx, value in enumerate(current)]


def fine_latency_summary(
    metrics: dict[str, Any], old_metrics: dict[str, Any], key: str, bounds: list[int]
) -> dict[str, int]:
    buckets = bucket_delta(metrics, old_metrics, key)
    return {
        "samples": sum(buckets),
        "p50_ns": bounded_percentile_ns(buckets, bounds, 50),
        "p95_ns": bounded_percentile_ns(buckets, bounds, 95),
        "p99_ns": bounded_percentile_ns(buckets, bounds, 99),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket-path", default="/tmp/onyx-storage-nvme.sock")
    parser.add_argument("--pid", type=int)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--count", type=int, default=0, help="0 means run until interrupted")
    parser.add_argument("--timeout", type=float, default=3.0)
    args = parser.parse_args()
    pid = process_id(args.pid)
    path = pathlib.Path(args.socket_path)
    previous: tuple[float, dict[str, Any], dict[str, Any], dict[str, dict[str, int]]] | None = None
    sample = 0
    while args.count == 0 or sample < args.count:
        started = time.monotonic()
        metrics = command(path, "metrics-json", args.timeout)
        status = command(path, "status-json", args.timeout)
        status = status.get("status", status)
        sched = schedstat(pid)
        now = time.time()
        if previous:
            old_ts, old_metrics, old_status, old_sched = previous
            elapsed = max(0.001, now - old_ts)
            stages = {}
            for key in (
                "ublk_write_queue_wait_latency_buckets",
                "ublk_write_worker_latency_buckets",
                "ublk_write_completion_wait_latency_buckets",
                "buffer_append_prepare_latency_buckets",
                "buffer_append_wait_durable_latency_buckets",
            ):
                stages[key.removesuffix("_latency_buckets")] = percentile_ns(bucket_delta(metrics, old_metrics, key), 99)
            fine_bounds = [
                int(value)
                for value in metrics.get("buffer_lv2_latency_bucket_upper_bounds_ns", [])
            ]
            lv2_stage_keys = {
                "durable_wait": "buffer_append_wait_durable_fine_latency_buckets",
                "staging_queue": "buffer_lv2_staging_queue_latency_buckets",
                "prepared_queue": "buffer_lv2_prepared_queue_latency_buckets",
                "group_collect": "buffer_lv2_group_collect_latency_buckets",
                "payload_write": "buffer_lv2_payload_write_latency_buckets",
                "checkpoint_write": "buffer_lv2_checkpoint_write_latency_buckets",
                "root_flush": "buffer_lv2_root_flush_latency_buckets",
                "watermark_dispatch": "buffer_lv2_watermark_dispatch_latency_buckets",
            }
            lv2_stages = {
                name: fine_latency_summary(metrics, old_metrics, key, fine_bounds)
                for name, key in lv2_stage_keys.items()
            }
            old_shards = {int(s["shard_idx"]): s for s in old_status.get("buffer_shards", [])}
            shards = []
            for shard in status.get("buffer_shards", []):
                idx = int(shard["shard_idx"])
                old = old_shards.get(idx, {})
                capacity = int(shard.get("capacity_bytes", 0))
                head_delta = (int(shard.get("head_offset", 0)) - int(old.get("head_offset", 0))) % capacity if capacity else 0
                tail_delta = (int(shard.get("tail_offset", 0)) - int(old.get("tail_offset", 0))) % capacity if capacity else 0
                shards.append({
                    "shard": idx,
                    "iops": nonnegative(int(shard.get("append_ops", 0)), int(old.get("append_ops", 0))) / elapsed,
                    "mib_s": nonnegative(int(shard.get("append_bytes", 0)), int(old.get("append_bytes", 0))) / elapsed / 1048576,
                    "used_delta": int(shard.get("used_bytes", 0)) - int(old.get("used_bytes", 0)),
                    "head_delta": head_delta,
                    "tail_delta": tail_delta,
                    "reserve_wait_ms": nonnegative(int(shard.get("reserve_wait_ns", 0)), int(old.get("reserve_wait_ns", 0))) / 1e6,
                    "fill_pct": shard.get("fill_pct", 0),
                })
            scheduler = {}
            for role, row in sched.items():
                old = old_sched.get(role, {})
                scheduler[role] = {
                    "threads": row["threads"],
                    "cpu_pct": nonnegative(row["runtime_ns"], old.get("runtime_ns", 0)) / elapsed / 1e7,
                    "runqueue_ms": nonnegative(row["runqueue_ns"], old.get("runqueue_ns", 0)) / 1e6,
                    "switches": nonnegative(row["switches"], old.get("switches", 0)),
                }
            print(
                json.dumps(
                    {
                        "ts": now,
                        "elapsed": elapsed,
                        "write_p99_ns": stages,
                        "lv2_durable_stages": lv2_stages,
                        "lv2_percentiles_are_bucket_upper_bounds": True,
                        "shards": shards,
                        "scheduler": scheduler,
                    },
                    separators=(",", ":"),
                ),
                flush=True,
            )
        previous = (now, metrics, status, sched)
        sample += 1
        time.sleep(max(0.0, args.interval - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
