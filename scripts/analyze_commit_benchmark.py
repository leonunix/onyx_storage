#!/usr/bin/env python3
"""Summarize an Onyx commit-path fio benchmark from captured JSON files."""

from __future__ import annotations

import argparse
import json
import pathlib
from typing import Any


JsonObject = dict[str, Any]


def load_object(path: pathlib.Path, label: str) -> JsonObject:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {label} JSON from {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} JSON must contain an object")
    return value


def number(value: Any, label: str) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be numeric")
    return value


def counter(payload: JsonObject, key: str) -> int:
    value = number(payload.get(key, 0), key)
    if value < 0:
        raise ValueError(f"{key} must be non-negative")
    return int(value)


def counter_delta(after: JsonObject, before: JsonObject, key: str) -> int:
    if key not in after:
        raise ValueError(f"after snapshot omitted required counter {key}")
    if before and key not in before:
        raise ValueError(f"before snapshot omitted required counter {key}")
    after_value = counter(after, key)
    before_value = counter(before, key) if before else 0
    if after_value < before_value:
        raise ValueError(
            f"counter {key} moved backwards ({before_value} -> {after_value}); "
            "snapshots likely cross a process restart"
        )
    return after_value - before_value


def optional_ratio(numerator: int | float, denominator: int | float) -> float | None:
    return float(numerator) / float(denominator) if denominator else None


def unwrap_metrics(payload: JsonObject) -> JsonObject:
    metrics = payload.get("metrics", payload)
    if not isinstance(metrics, dict):
        raise ValueError("metrics JSON must contain an object")
    return metrics


def unwrap_metadb_status(payload: JsonObject | None) -> JsonObject:
    if payload is None:
        return {}
    status = payload.get("status", payload)
    if not isinstance(status, dict):
        raise ValueError("status JSON 'status' value must be an object")
    if "metadb_memory" in status:
        metadb = status["metadb_memory"]
    elif "metadb" in status:
        metadb = status["metadb"]
    elif "commit_apply_us" in status or "apply_refcount_us" in status:
        metadb = status
    else:
        raise ValueError("status JSON does not contain metadb metrics")
    if not isinstance(metadb, dict):
        raise ValueError("status JSON metadb metrics must be an object")
    return metadb


def fio_summary(payload: JsonObject) -> tuple[float, int | float, int]:
    jobs = payload.get("jobs")
    if not isinstance(jobs, list) or not jobs or not isinstance(jobs[0], dict):
        raise ValueError("fio JSON must contain one grouped job object")
    if len(jobs) != 1:
        raise ValueError("fio JSON must be produced with group_reporting=1")
    job = jobs[0]
    write = job.get("write")
    if not isinstance(write, dict):
        raise ValueError("fio job must contain write results")
    clat = write.get("clat_ns")
    if not isinstance(clat, dict):
        raise ValueError("fio write results must contain clat_ns")
    percentiles = clat.get("percentile")
    if not isinstance(percentiles, dict) or "99.000000" not in percentiles:
        raise ValueError("fio clat_ns must contain the 99.000000 percentile")

    iops = float(number(write.get("iops"), "fio write.iops"))
    p99_ns = number(percentiles["99.000000"], "fio write p99")
    error = int(number(job.get("error", 0), "fio job.error"))
    return iops, p99_ns, error


def drain_summary(
    payload: JsonObject | None,
) -> tuple[int | float | None, int | float | None, bool | None]:
    if payload is None:
        return None, None, None

    def optional_number(*keys: str) -> int | float | None:
        for key in keys:
            if key in payload and payload[key] is not None:
                return number(payload[key], f"drain {key}")
        return None

    pending = optional_number("pending_zero_secs", "pending_zero_seconds")
    ring = optional_number(
        "physical_ring_zero_secs",
        "physical_ring_zero_seconds",
        "ring_zero_secs",
        "ring_zero_seconds",
    )
    samples = payload.get("samples", [])
    if (pending is None or ring is None) and samples:
        if not isinstance(samples, list):
            raise ValueError("drain samples must be an array")
        for sample in samples:
            if not isinstance(sample, dict):
                raise ValueError("each drain sample must be an object")
            elapsed = number(sample.get("elapsed_secs"), "drain sample elapsed_secs")
            if pending is None and number(
                sample.get("pending_entries"), "drain sample pending_entries"
            ) == 0:
                pending = elapsed
            if ring is None and number(
                sample.get("physical_used_bytes"), "drain sample physical_used_bytes"
            ) == 0:
                ring = elapsed
            if pending is not None and ring is not None:
                break
    timed_out_value = payload.get("timed_out")
    if timed_out_value is None:
        timed_out = pending is None or ring is None
    elif isinstance(timed_out_value, bool):
        timed_out = timed_out_value
    else:
        raise ValueError("drain timed_out must be boolean")
    return pending, ring, timed_out


def build_summary(
    before_payload: JsonObject,
    after_payload: JsonObject,
    fio_payload: JsonObject,
    status_before_payload: JsonObject | None,
    status_after_payload: JsonObject | None,
    drain_payload: JsonObject | None,
) -> JsonObject:
    before = unwrap_metrics(before_payload)
    after = unwrap_metrics(after_payload)
    iops, p99_ns, fio_error = fio_summary(fio_payload)

    meta_lbas = counter_delta(after, before, "flush_writer_meta_lbas")
    transactions = counter_delta(after, before, "flush_writer_meta_commits")
    commit_jobs_key = (
        "flush_commit_worker_jobs"
        if "flush_commit_worker_jobs" in after or "flush_commit_worker_jobs" in before
        else "flush_commit_worker_drain_jobs"
    )
    commit_jobs = counter_delta(after, before, commit_jobs_key)
    queue_wait_ns = counter_delta(after, before, "flush_commit_worker_queue_wait_ns")

    status_available = status_after_payload is not None
    status_before = unwrap_metadb_status(status_before_payload)
    status_after = unwrap_metadb_status(status_after_payload)
    apply_lbas = (
        counter_delta(status_after, status_before, "apply_l2p_remap_range_lbas")
        if status_available
        else None
    )
    commit_apply_us = (
        counter_delta(status_after, status_before, "commit_apply_us")
        if status_available
        else None
    )
    refcount_apply_us = (
        counter_delta(status_after, status_before, "apply_refcount_us")
        if status_available
        else None
    )
    l2p_apply_us = (
        counter_delta(status_after, status_before, "apply_l2p_remap_range_us")
        if status_available
        else None
    )
    refcount_batch = None
    grouping_us = 0
    if status_available and "apply_refcount_batch_count" in status_after:
        batch_count = counter_delta(
            status_after, status_before, "apply_refcount_batch_count"
        )
        batch_actions = counter_delta(
            status_after, status_before, "apply_refcount_batch_actions"
        )
        batch_pbas = counter_delta(
            status_after, status_before, "apply_refcount_batch_pbas"
        )
        sampled_pbas = counter_delta(
            status_after,
            status_before,
            "apply_refcount_breakdown_sampled_pbas",
        )
        grouping_us = counter_delta(
            status_after, status_before, "apply_refcount_pba_grouping_us"
        )
        base_lookup_us = counter_delta(
            status_after, status_before, "apply_refcount_base_page_lookup_us"
        )
        pending_scan_us = counter_delta(
            status_after, status_before, "apply_refcount_pending_slot_scan_us"
        )
        delta_merge_us = counter_delta(
            status_after, status_before, "apply_refcount_delta_merge_us"
        )
        measured_stage_total_us = base_lookup_us + pending_scan_us + delta_merge_us
        estimated_measured_components_us = (
            measured_stage_total_us * batch_pbas / sampled_pbas
            if sampled_pbas
            else None
        )
        unattributed_stage_us = (
            max(0, refcount_apply_us - estimated_measured_components_us)
            if refcount_apply_us is not None
            and estimated_measured_components_us is not None
            else None
        )
        refcount_batch = {
            "batches": batch_count,
            "actions": batch_actions,
            "pbas": batch_pbas,
            "sampled_pbas": sampled_pbas,
            "pbas_per_batch": optional_ratio(batch_pbas, batch_count),
            "sampled_pba_pct": optional_ratio(sampled_pbas * 100, batch_pbas),
            "grouping_us": grouping_us,
            "grouping_us_per_action": optional_ratio(grouping_us, batch_actions),
            "grouping_us_per_pba": optional_ratio(grouping_us, batch_pbas),
            "base_lookup_us": base_lookup_us,
            "base_lookup_us_per_sampled_pba": optional_ratio(
                base_lookup_us, sampled_pbas
            ),
            "pending_scan_us": pending_scan_us,
            "pending_scan_us_per_sampled_pba": optional_ratio(
                pending_scan_us, sampled_pbas
            ),
            "delta_merge_us": delta_merge_us,
            "delta_merge_us_per_sampled_pba": optional_ratio(
                delta_merge_us, sampled_pbas
            ),
            "measured_stage_total_us": measured_stage_total_us,
            "estimated_measured_components_us": estimated_measured_components_us,
            "unattributed_stage_us": unattributed_stage_us,
            "unattributed_stage_us_per_pba": (
                optional_ratio(unattributed_stage_us, batch_pbas)
                if unattributed_stage_us is not None
                else None
            ),
        }
    full_refcount_apply_us = (
        refcount_apply_us + grouping_us if refcount_apply_us is not None else None
    )
    other_apply_us = (
        max(0, commit_apply_us - full_refcount_apply_us - l2p_apply_us)
        if commit_apply_us is not None
        and full_refcount_apply_us is not None
        and l2p_apply_us is not None
        else None
    )

    pending_zero_seconds, ring_zero_seconds, drain_timed_out = drain_summary(
        drain_payload
    )
    apply_totals_us = {
        "commit": commit_apply_us,
        "refcount_stage": refcount_apply_us,
        "refcount_full": full_refcount_apply_us,
        "l2p": l2p_apply_us,
        "other": other_apply_us,
    }
    apply_totals_seconds = {
        key: (value / 1_000_000 if value is not None else None)
        for key, value in apply_totals_us.items()
    }

    return {
        "iops": iops,
        "p99_ns": p99_ns,
        "p99_ms": float(p99_ns) / 1_000_000,
        "fio_error": fio_error,
        "meta_lbas": meta_lbas,
        "metadb_tx": transactions,
        "lbas_per_tx": optional_ratio(meta_lbas, transactions),
        "commit_jobs": commit_jobs,
        "queue_wait_ms_per_job": optional_ratio(
            queue_wait_ns, commit_jobs * 1_000_000
        ),
        "apply_lbas": apply_lbas,
        "rc_apply_us_per_lba": (
            optional_ratio(refcount_apply_us, apply_lbas)
            if refcount_apply_us is not None and apply_lbas is not None
            else None
        ),
        "l2p_apply_us_per_lba": (
            optional_ratio(l2p_apply_us, apply_lbas)
            if l2p_apply_us is not None and apply_lbas is not None
            else None
        ),
        "apply_totals_us": apply_totals_us,
        "apply_totals_seconds": apply_totals_seconds,
        "refcount_batch_breakdown": refcount_batch,
        "pending_zero_seconds": pending_zero_seconds,
        "physical_ring_zero_seconds": ring_zero_seconds,
        "drain_timed_out": drain_timed_out,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=pathlib.Path, required=True)
    parser.add_argument("--after", type=pathlib.Path, required=True)
    parser.add_argument("--fio", type=pathlib.Path, required=True)
    parser.add_argument("--status-before", type=pathlib.Path)
    parser.add_argument("--status-after", type=pathlib.Path)
    parser.add_argument("--drain", type=pathlib.Path)
    args = parser.parse_args()
    if args.status_before and not args.status_after:
        parser.error("--status-before requires --status-after")

    try:
        summary = build_summary(
            load_object(args.before, "before metrics"),
            load_object(args.after, "after metrics"),
            load_object(args.fio, "fio"),
            load_object(args.status_before, "before status")
            if args.status_before
            else None,
            load_object(args.status_after, "after status")
            if args.status_after
            else None,
            load_object(args.drain, "drain") if args.drain else None,
        )
    except ValueError as exc:
        parser.error(str(exc))
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
