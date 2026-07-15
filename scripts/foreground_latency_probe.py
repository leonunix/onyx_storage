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
            commit_jobs = nonnegative(
                int(metrics.get("flush_commit_worker_jobs", 0)),
                int(old_metrics.get("flush_commit_worker_jobs", 0)),
            )
            commit_batches = nonnegative(
                int(metrics.get("flush_commit_worker_drain_batches", 0)),
                int(old_metrics.get("flush_commit_worker_drain_batches", 0)),
            )
            commit_lbas = nonnegative(
                int(metrics.get("flush_commit_worker_drain_lbas", 0)),
                int(old_metrics.get("flush_commit_worker_drain_lbas", 0)),
            )
            commit_queue_wait_ns = nonnegative(
                int(metrics.get("flush_commit_worker_queue_wait_ns", 0)),
                int(old_metrics.get("flush_commit_worker_queue_wait_ns", 0)),
            )
            commit_aggregator_residence_ns = nonnegative(
                int(metrics.get("flush_commit_worker_aggregator_residence_ns", 0)),
                int(old_metrics.get("flush_commit_worker_aggregator_residence_ns", 0)),
            )
            commit_executor_queue_wait_ns = nonnegative(
                int(metrics.get("flush_commit_worker_executor_queue_wait_ns", 0)),
                int(old_metrics.get("flush_commit_worker_executor_queue_wait_ns", 0)),
            )
            commit_service_ns = nonnegative(
                int(metrics.get("flush_commit_worker_service_ns", 0)),
                int(old_metrics.get("flush_commit_worker_service_ns", 0)),
            )
            commit_service_batches = nonnegative(
                int(metrics.get("flush_commit_worker_service_batches", 0)),
                int(old_metrics.get("flush_commit_worker_service_batches", 0)),
            )
            # Older engines do not export the service-boundary counter. Their
            # drain-batch count is the closest compatible divisor; keep the old
            # raw-job-normalized value under explicit legacy names below.
            service_batch_divisor = commit_service_batches or commit_batches
            service_job_avg_ns = commit_service_ns // commit_jobs if commit_jobs else 0
            commit_worker = {
                "jobs": commit_jobs,
                "batches": commit_batches,
                "service_batches": commit_service_batches,
                "lbas": commit_lbas,
                "jobs_per_batch": commit_jobs / commit_batches if commit_batches else 0,
                "lbas_per_batch": commit_lbas / commit_batches if commit_batches else 0,
                "queue_wait_avg_ns": commit_queue_wait_ns // commit_jobs if commit_jobs else 0,
                "aggregator_residence_avg_ns": commit_aggregator_residence_ns // commit_jobs
                if commit_jobs
                else 0,
                "executor_queue_wait_avg_ns": commit_executor_queue_wait_ns // commit_jobs
                if commit_jobs
                else 0,
                "service_batch_avg_ns": commit_service_ns // service_batch_divisor
                if service_batch_divisor
                else 0,
                "service_job_avg_ns_legacy": service_job_avg_ns,
                # Backward-compatible alias for existing probe consumers.
                "service_avg_ns": service_job_avg_ns,
                "executor_queue_wait_max_ns": int(
                    metrics.get("flush_commit_worker_executor_queue_wait_max_ns", 0)
                ),
                "executor_queue_depth": int(
                    metrics.get("flush_commit_executor_queue_depth", 0)
                ),
                "executor_queue_depth_max": int(
                    metrics.get("flush_commit_executor_queue_depth_max", 0)
                ),
                "executors_active": int(metrics.get("flush_commit_executors_active", 0)),
                "executors_limit": int(metrics.get("flush_commit_executors_limit", 0)),
                "executors_active_max": int(
                    metrics.get("flush_commit_executors_active_max", 0)
                ),
                "pipeline_depth_max": int(metrics.get("flush_commit_worker_pipeline_depth_max", 0)),
                "pipeline_issues": nonnegative(
                    int(metrics.get("flush_commit_worker_pipeline_issues", 0)),
                    int(old_metrics.get("flush_commit_worker_pipeline_issues", 0)),
                ),
            }
            sync_flushes = nonnegative(
                int(metrics.get("buffer_sync_flushes", 0)),
                int(old_metrics.get("buffer_sync_flushes", 0)),
            )
            sync_entries = nonnegative(
                int(metrics.get("buffer_sync_entries", 0)),
                int(old_metrics.get("buffer_sync_entries", 0)),
            )
            buffer_sync = {
                "appends": nonnegative(
                    int(metrics.get("buffer_appends", 0)),
                    int(old_metrics.get("buffer_appends", 0)),
                ),
                "batches": nonnegative(
                    int(metrics.get("buffer_sync_batches", 0)),
                    int(old_metrics.get("buffer_sync_batches", 0)),
                ),
                "flushes": sync_flushes,
                "entries": sync_entries,
                "entries_per_flush": sync_entries / sync_flushes if sync_flushes else 0,
            }
            flush_qos = {
                "mode": int(metrics.get("flush_qos_mode", 0)),
                "foreground_outstanding": int(
                    metrics.get("foreground_io_outstanding", 0)
                ),
                "rate_mib_s": int(metrics.get("flush_qos_rate_bytes_per_sec", 0)) / 1048576,
                "foreground_mib_s": int(
                    metrics.get("flush_qos_foreground_bytes_per_sec", 0)
                )
                / 1048576,
                "durable_p99_ms": int(metrics.get("flush_qos_durable_p99_ns", 0)) / 1e6,
                "logical_fill_pct": int(metrics.get("flush_qos_logical_fill_pct", 0)),
                "physical_fill_pct": int(metrics.get("flush_qos_physical_fill_pct", 0)),
                "payload_fill_pct": int(metrics.get("flush_qos_payload_fill_pct", 0)),
                "admitted_mib_s": nonnegative(
                    int(metrics.get("flush_qos_admitted_bytes", 0)),
                    int(old_metrics.get("flush_qos_admitted_bytes", 0)),
                )
                / elapsed
                / 1048576,
                "wait_ms": nonnegative(
                    int(metrics.get("flush_qos_wait_ns", 0)),
                    int(old_metrics.get("flush_qos_wait_ns", 0)),
                )
                / 1e6,
                "wait_events": nonnegative(
                    int(metrics.get("flush_qos_wait_events", 0)),
                    int(old_metrics.get("flush_qos_wait_events", 0)),
                ),
                "wait_max_ms_lifetime": int(metrics.get("flush_qos_wait_max_ns", 0)) / 1e6,
                "waiters": int(metrics.get("flush_qos_waiters", 0)),
                "waiters_max_lifetime": int(metrics.get("flush_qos_waiters_max", 0)),
                "emergency_transitions": nonnegative(
                    int(metrics.get("flush_qos_emergency_transitions", 0)),
                    int(old_metrics.get("flush_qos_emergency_transitions", 0)),
                ),
            }
            write_throttle = {
                "count": nonnegative(
                    int(metrics.get("buffer_throttle_count", 0)),
                    int(old_metrics.get("buffer_throttle_count", 0)),
                ),
                "wait_ms": nonnegative(
                    int(metrics.get("buffer_throttle_us_total", 0)),
                    int(old_metrics.get("buffer_throttle_us_total", 0)),
                )
                / 1000,
                "wait_max_ms_lifetime": int(metrics.get("buffer_throttle_us_max", 0))
                / 1000,
                "backend_debt_count": nonnegative(
                    int(metrics.get("buffer_backend_debt_throttle_count", 0)),
                    int(old_metrics.get("buffer_backend_debt_throttle_count", 0)),
                ),
                "backend_debt_wait_ms": nonnegative(
                    int(metrics.get("buffer_backend_debt_throttle_us_total", 0)),
                    int(old_metrics.get("buffer_backend_debt_throttle_us_total", 0)),
                )
                / 1000,
                "backend_debt_wait_max_ms_lifetime": int(
                    metrics.get("buffer_backend_debt_throttle_us_max", 0)
                )
                / 1000,
            }
            io_scheduler_now = status.get("chunklet_io_scheduler") or {}
            io_scheduler_old = old_status.get("chunklet_io_scheduler") or {}
            chunklet_io_scheduler: dict[str, Any] = {}
            if io_scheduler_now:
                for class_name in ("foreground", "lv3", "meta"):
                    current_class = io_scheduler_now.get(class_name, {})
                    old_class = io_scheduler_old.get(class_name, {})
                    chunklet_io_scheduler[class_name] = {
                        "reserved": int(current_class.get("reserved", 0)),
                        "active": int(current_class.get("active", 0)),
                        "waiters": int(current_class.get("waiters", 0)),
                        "max_active": int(current_class.get("max_active", 0)),
                        "max_waiters": int(current_class.get("max_waiters", 0)),
                        "admissions": nonnegative(
                            int(current_class.get("admissions", 0)),
                            int(old_class.get("admissions", 0)),
                        ),
                        "wait_ms": nonnegative(
                            int(current_class.get("wait_ns", 0)),
                            int(old_class.get("wait_ns", 0)),
                        )
                        / 1e6,
                        "wait_max_ms_lifetime": int(current_class.get("wait_max_ns", 0))
                        / 1e6,
                        "borrowed_admissions": nonnegative(
                            int(current_class.get("borrowed_admissions", 0)),
                            int(old_class.get("borrowed_admissions", 0)),
                        ),
                        "reclaim_max_ms_lifetime": int(
                            current_class.get("reclaim_max_ns", 0)
                        )
                        / 1e6,
                        "reclaim_current_ms": int(
                            current_class.get("reclaim_current_ns", 0)
                        )
                        / 1e6,
                        "reclaim_events": nonnegative(
                            int(current_class.get("reclaim_events", 0)),
                            int(old_class.get("reclaim_events", 0)),
                        ),
                        "reclaim_in_progress": bool(
                            current_class.get("reclaim_in_progress", False)
                        ),
                    }
                chunklet_io_scheduler["total_limit"] = int(
                    io_scheduler_now.get("total_limit", 0)
                )
                chunklet_io_scheduler["total_active"] = int(
                    io_scheduler_now.get("total_active", 0)
                )
                chunklet_io_scheduler["total_max_active"] = int(
                    io_scheduler_now.get("total_max_active", 0)
                )
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
                        "commit_worker": commit_worker,
                        "flush_qos": flush_qos,
                        "write_throttle": write_throttle,
                        "chunklet_io_scheduler": chunklet_io_scheduler,
                        "buffer_sync": buffer_sync,
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
