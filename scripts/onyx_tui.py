#!/usr/bin/env python3
from __future__ import annotations

import argparse
import curses
import json
import os
import pathlib
import socket
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Optional

try:
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover
    tomllib = None


NS_PER_SEC = 1_000_000_000
US_PER_SEC = 1_000_000
CLK_TCK = os.sysconf(os.sysconf_names.get("SC_CLK_TCK", "SC_CLK_TCK"))
PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")


def load_socket_path(config_path: pathlib.Path, explicit: Optional[str]) -> pathlib.Path:
    if explicit:
        return pathlib.Path(explicit)
    if tomllib is None:
        raise SystemExit("python tomllib unavailable; pass --socket-path")
    with config_path.open("rb") as fh:
        payload = tomllib.load(fh)
    path = payload.get("service", {}).get("socket_path")
    if not path:
        raise SystemExit("service.socket_path missing from config")
    return pathlib.Path(path)


def send_socket_cmd(socket_path: pathlib.Path, cmd: str, timeout: float) -> dict[str, Any]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.connect(str(socket_path))
        sock.sendall((cmd + "\n").encode("utf-8"))
        fileobj = sock.makefile("r", encoding="utf-8", newline="\n")
        lines: list[str] = []
        while True:
            line = fileobj.readline()
            if line == "":
                break
            line = line.strip()
            if not line:
                continue
            if line == "ok" or line.startswith("ok "):
                break
            if line.startswith("error:"):
                raise RuntimeError(line.removeprefix("error:").strip())
            lines.append(line)
    if not lines:
        return {}
    return json.loads(lines[0])


def num(payload: dict[str, Any], key: str, default: float = 0.0) -> float:
    value = payload.get(key, default)
    return float(value) if isinstance(value, (int, float)) else default


def nested(payload: dict[str, Any], *keys: str, default: Any = None) -> Any:
    cur: Any = payload
    for key in keys:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def delta(now: dict[str, Any], prev: Optional[dict[str, Any]], key: str) -> float:
    if prev is None:
        return 0.0
    return max(0.0, num(now, key) - num(prev, key))


def rate(now: dict[str, Any], prev: Optional[dict[str, Any]], key: str, interval: float) -> float:
    if interval <= 0:
        return 0.0
    return delta(now, prev, key) / interval


def avg_time(
    now: dict[str, Any],
    prev: Optional[dict[str, Any]],
    ns_key: str,
    count_key: str,
    *,
    ns_per_unit: float = 1.0,
) -> float:
    count = delta(now, prev, count_key)
    if count <= 0:
        return 0.0
    return delta(now, prev, ns_key) / count / ns_per_unit


def avg_time_by_counts(
    now: dict[str, Any],
    prev: Optional[dict[str, Any]],
    ns_key: str,
    count_keys: tuple[str, ...],
) -> float:
    count = sum(delta(now, prev, key) for key in count_keys)
    if count <= 0:
        return 0.0
    return delta(now, prev, ns_key) / count


def avg_residual_time(
    now: dict[str, Any],
    prev: Optional[dict[str, Any]],
    total_key: str,
    component_keys: tuple[str, ...],
    count_key: str,
) -> float:
    count = delta(now, prev, count_key)
    if count <= 0:
        return 0.0
    total = delta(now, prev, total_key)
    components = sum(delta(now, prev, key) for key in component_keys)
    return max(0.0, total - components) / count


def avg_writer_time(
    now: dict[str, Any], prev: Optional[dict[str, Any]], ns_key: str
) -> float:
    # flush_units_written excludes packed slots. Writer timers cover both
    # paths, so using that counter alone inflates latency whenever packing is
    # active (often by more than an order of magnitude).
    return avg_time_by_counts(
        now,
        prev,
        ns_key,
        ("flush_units_written", "flush_packed_slots_written"),
    )


def fmt_count(value: float) -> str:
    value = float(value)
    for suffix in ("", "K", "M", "G", "T"):
        if abs(value) < 1000.0:
            return f"{value:,.0f}{suffix}"
        value /= 1000.0
    return f"{value:,.0f}P"


def fmt_bytes(value: float) -> str:
    value = float(value)
    for suffix in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if abs(value) < 1024.0:
            return f"{value:,.1f} {suffix}"
        value /= 1024.0
    return f"{value:,.1f} EiB"


def fmt_rate_bytes(value: float) -> str:
    return f"{fmt_bytes(value)}/s"


def fmt_ms(ns: float) -> str:
    return f"{ns / 1_000_000.0:,.3f} ms"


def fmt_us(us: float) -> str:
    if us >= 1000.0:
        return f"{us / 1000.0:,.3f} ms"
    return f"{us:,.1f} us"


def ratio(numer: float, denom: float) -> str:
    if denom <= 0:
        return "-"
    return f"{numer / denom:,.2f}x"


def pct(numer: float, denom: float) -> str:
    if denom <= 0:
        return "-"
    return f"{numer * 100.0 / denom:,.1f}%"


def bar(value: float, limit: float, width: int) -> str:
    if width <= 0:
        return ""
    fraction = 0.0 if limit <= 0 else max(0.0, min(1.0, value / limit))
    fill = int(round(fraction * width))
    return "[" + "#" * fill + "." * (width - fill) + "]"


def gauge(value: float, limit: float, width: int) -> str:
    width = max(1, width)
    fraction = 0.0 if limit <= 0 else max(0.0, min(1.0, value / limit))
    fill = int(round(fraction * width))
    if fill >= width:
        return "=" * width
    if fill <= 0:
        return "." * width
    return "=" * (fill - 1) + ">" + "." * (width - fill)


def spark(values: list[float], width: int) -> str:
    chars = "._-~=*#"
    width = max(1, width)
    tail = values[-width:]
    if not tail:
        return "." * width
    hi = max(tail)
    lo = min(tail)
    if hi <= lo:
        body = chars[0] * len(tail)
    else:
        scale = (len(chars) - 1) / (hi - lo)
        body = "".join(chars[int((value - lo) * scale)] for value in tail)
    return body.rjust(width, ".")


def fnum(value: float, width: int = 8, decimals: int = 1) -> str:
    return f"{value:>{width},.{decimals}f}"


def safe_div(numer: float, denom: float) -> float:
    return numer / denom if denom > 0 else 0.0


def read_cpu_times() -> tuple[int, int]:
    try:
        first = pathlib.Path("/proc/stat").read_text(encoding="utf-8").splitlines()[0]
    except OSError:
        return (0, 0)
    parts = [int(value) for value in first.split()[1:]]
    idle = parts[3] + (parts[4] if len(parts) > 4 else 0)
    total = sum(parts)
    return total, idle


def read_meminfo() -> dict[str, int]:
    out: dict[str, int] = {}
    try:
        lines = pathlib.Path("/proc/meminfo").read_text(encoding="utf-8").splitlines()
    except OSError:
        return out
    for line in lines:
        key, raw = line.split(":", 1)
        parts = raw.strip().split()
        if parts:
            out[key] = int(parts[0]) * 1024
    return out


def read_process_totals() -> dict[str, float]:
    totals = {
        "onyx_cpu_ticks": 0.0,
        "onyx_rss_bytes": 0.0,
        "fio_cpu_ticks": 0.0,
        "fio_rss_bytes": 0.0,
    }
    proc = pathlib.Path("/proc")
    for entry in proc.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            comm = (entry / "comm").read_text(encoding="utf-8").strip()
            stat = (entry / "stat").read_text(encoding="utf-8")
        except OSError:
            continue
        if comm not in {"onyx-storage", "fio"}:
            continue
        try:
            after = stat.rsplit(")", 1)[1].split()
            utime = int(after[11])
            stime = int(after[12])
            rss_pages = int(after[21])
        except (IndexError, ValueError):
            continue
        prefix = "onyx" if comm == "onyx-storage" else "fio"
        totals[f"{prefix}_cpu_ticks"] += utime + stime
        totals[f"{prefix}_rss_bytes"] += rss_pages * PAGE_SIZE
    return totals


def read_system_snapshot() -> dict[str, float]:
    total, idle = read_cpu_times()
    mem = read_meminfo()
    proc = read_process_totals()
    mem_total = float(mem.get("MemTotal", 0))
    mem_avail = float(mem.get("MemAvailable", 0))
    load = os.getloadavg() if hasattr(os, "getloadavg") else (0.0, 0.0, 0.0)
    return {
        "cpu_total": float(total),
        "cpu_idle": float(idle),
        "mem_total": mem_total,
        "mem_available": mem_avail,
        "mem_used": max(0.0, mem_total - mem_avail),
        "load1": float(load[0]),
        "load5": float(load[1]),
        "load15": float(load[2]),
        **proc,
    }


@dataclass
class Sample:
    ts: float
    status: dict[str, Any]
    metrics: dict[str, Any]
    system: dict[str, float]


class Monitor:
    def __init__(
        self,
        socket_path: pathlib.Path,
        timeout: float,
        status_timeout: float,
        status_interval: float,
        no_status: bool,
    ) -> None:
        self.socket_path = socket_path
        self.timeout = timeout
        self.status_timeout = status_timeout
        self.status_interval = status_interval
        self.no_status = no_status
        self.prev: Optional[Sample] = None
        self.cur: Optional[Sample] = None
        self.error: Optional[str] = None
        self.status_error: Optional[str] = None
        self.status: dict[str, Any] = {"mode": "status-off"} if no_status else {}
        self.next_status_poll = 0.0
        self.history: deque[dict[str, float]] = deque(maxlen=120)

    def poll(self, force_status: bool = False) -> None:
        now = time.time()
        try:
            metrics = send_socket_cmd(self.socket_path, "metrics-json", self.timeout)
        except Exception as exc:
            self.error = str(exc)
            return

        self.error = None
        if not self.no_status and (force_status or now >= self.next_status_poll):
            self.next_status_poll = now + max(0.2, self.status_interval)
            try:
                raw_status = send_socket_cmd(self.socket_path, "status-json", self.status_timeout)
                status = raw_status.get("status", raw_status)
                if isinstance(status, dict) and "mode" not in status and "mode" in raw_status:
                    status = {**status, "mode": raw_status.get("mode")}
                if isinstance(status, dict):
                    self.status = status
                    self.status_error = None
            except Exception as exc:
                self.status_error = str(exc)

        self.error = None
        if self.cur is not None:
            self.prev = self.cur
        self.cur = Sample(now, self.status, metrics, read_system_snapshot())
        self._record_history()

    def interval(self) -> float:
        if self.cur is None or self.prev is None:
            return 0.0
        return max(0.001, self.cur.ts - self.prev.ts)

    def _record_history(self) -> None:
        if self.cur is None or self.prev is None:
            return
        metrics = self.cur.metrics
        prev_metrics = self.prev.metrics
        system = self.cur.system
        prev_system = self.prev.system
        meta = nested(self.cur.status, "metadb_memory", default={}) or {}
        prev_meta = nested(self.prev.status, "metadb_memory", default={}) or {}
        interval = self.interval()
        cpu_delta = max(0.0, system.get("cpu_total", 0.0) - prev_system.get("cpu_total", 0.0))
        idle_delta = max(0.0, system.get("cpu_idle", 0.0) - prev_system.get("cpu_idle", 0.0))
        self.history.append(
            {
                "cpu_pct": safe_div(cpu_delta - idle_delta, cpu_delta) * 100.0,
                "read_iops": rate(metrics, prev_metrics, "volume_read_ops", interval),
                "write_iops": rate(metrics, prev_metrics, "volume_write_ops", interval),
                "write_mib": rate(metrics, prev_metrics, "volume_write_bytes", interval)
                / (1024 * 1024),
                "pending": float(nested(self.cur.status, "buffer_pending_entries", default=0) or 0),
                "dedup_hit_pct": safe_div(
                    num(metrics, "dedup_hits"),
                    num(metrics, "dedup_hits") + num(metrics, "dedup_misses"),
                )
                * 100.0,
                "commit_ms": avg_time(
                    meta,
                    prev_meta,
                    "commit_total_us",
                    "commit_ops",
                    ns_per_unit=1.0,
                )
                / 1000.0,
                "writer_meta_ms": avg_writer_time(
                    metrics, prev_metrics, "flush_writer_meta_ns"
                )
                / 1_000_000.0,
            }
        )


def collect_errors(metrics: dict[str, Any], status: dict[str, Any]) -> list[tuple[str, float]]:
    keys = [
        "read_crc_errors",
        "read_crc_errors_foreground",
        "dedup_verify_mismatches",
        "read_crc_errors_dedup_scanner",
        "read_decompress_errors",
        "flush_errors",
        "gc_errors",
        "dedup_rescan_errors",
        "dedup_hit_failures",
        "dedup_promotions_failed",
        "dedup_cleanup_reconstruct_errors",
        "dedup_cleanup_delete_errors",
        "volume_partial_read_ops",
        "volume_partial_write_ops",
    ]
    found = [(key, num(metrics, key)) for key in keys if num(metrics, key) > 0]
    meta = nested(status, "metadb_memory", default={}) or {}
    for key in ("commit_errors", "range_delete_errors", "cleanup_errors"):
        value = num(meta, key)
        if value > 0:
            found.append((f"meta.{key}", value))
    return found


def build_lines(cur: Sample, prev: Optional[Sample], socket_path: pathlib.Path) -> list[str]:
    status = cur.status
    metrics = cur.metrics
    system = cur.system
    prev_metrics = prev.metrics if prev is not None else None
    prev_system = prev.system if prev is not None else None
    interval = 0.0 if prev is None else max(0.001, cur.ts - prev.ts)
    meta = nested(status, "metadb_memory", default={}) or {}
    cpu_delta = 0.0 if prev_system is None else max(0.0, system.get("cpu_total", 0.0) - prev_system.get("cpu_total", 0.0))
    idle_delta = 0.0 if prev_system is None else max(0.0, system.get("cpu_idle", 0.0) - prev_system.get("cpu_idle", 0.0))
    cpu_pct = safe_div(cpu_delta - idle_delta, cpu_delta) * 100.0
    onyx_cpu = (
        safe_div(
            system.get("onyx_cpu_ticks", 0.0) - (prev_system or {}).get("onyx_cpu_ticks", 0.0),
            CLK_TCK * interval,
        )
        * 100.0
        if interval > 0
        else 0.0
    )
    fio_cpu = (
        safe_div(
            system.get("fio_cpu_ticks", 0.0) - (prev_system or {}).get("fio_cpu_ticks", 0.0),
            CLK_TCK * interval,
        )
        * 100.0
        if interval > 0
        else 0.0
    )

    read_iops = rate(metrics, prev_metrics, "volume_read_ops", interval)
    write_iops = rate(metrics, prev_metrics, "volume_write_ops", interval)
    read_bps = rate(metrics, prev_metrics, "volume_read_bytes", interval)
    write_bps = rate(metrics, prev_metrics, "volume_write_bytes", interval)
    total_dedup = num(metrics, "dedup_hits") + num(metrics, "dedup_misses")
    logical = num(metrics, "volume_write_bytes") or num(metrics, "buffer_append_bytes")
    lv3_bytes = num(metrics, "lv3_write_compressed_bytes")
    comp_in = num(metrics, "compress_input_bytes")
    comp_out = num(metrics, "compress_output_bytes")
    pending = nested(status, "buffer_pending_entries", default=0) or 0
    fill_pct = nested(status, "buffer_fill_pct", default=0) or 0
    physical_fill_pct = nested(status, "buffer_physical_fill_pct", default=fill_pct) or 0
    payload_mem = nested(status, "buffer_payload_memory_bytes", default=0) or 0
    payload_limit = nested(status, "buffer_payload_memory_limit_bytes", default=0) or 0
    volatile_mem = nested(status, "buffer_volatile_payload_memory_bytes", default=0) or 0
    volatile_limit = nested(status, "buffer_volatile_payload_memory_limit_bytes", default=0) or 0
    errors = collect_errors(metrics, status)
    health = "OK" if not errors else "WARN"

    lines = [
        f"Onyx live monitor  {health}  socket={socket_path}  {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(cur.ts))}",
        f"mode={status.get('mode', '-')} uptime={fmt_count(num(metrics, 'uptime_secs'))}s volumes={status.get('volume_count', '-')} zones={status.get('zone_count', '-')}",
        f"Host   cpu={cpu_pct:5.1f}% load={system.get('load1', 0.0):.1f}/{system.get('load5', 0.0):.1f}/{system.get('load15', 0.0):.1f}"
        f" mem={fmt_bytes(system.get('mem_used', 0.0))}/{fmt_bytes(system.get('mem_total', 0.0))} avail={fmt_bytes(system.get('mem_available', 0.0))}"
        f" onyx_cpu={onyx_cpu:5.1f}% fio_cpu={fio_cpu:5.1f}% onyx_rss={fmt_bytes(system.get('onyx_rss_bytes', 0.0))}",
        "",
        f"IO     read {read_iops:9.1f} iops {fmt_rate_bytes(read_bps):>14} avg {fmt_ms(avg_time(metrics, prev_metrics, 'volume_read_total_ns', 'volume_read_ops')):>10}"
        f" | write {write_iops:9.1f} iops {fmt_rate_bytes(write_bps):>14} avg {fmt_ms(avg_time(metrics, prev_metrics, 'volume_write_total_ns', 'volume_write_ops')):>10}",
        f"Ublk   read q {fmt_ms(avg_time(metrics, prev_metrics, 'ublk_read_queue_wait_ns', 'volume_read_ops')):>10}"
        f" worker {fmt_ms(avg_time(metrics, prev_metrics, 'ublk_read_worker_ns', 'volume_read_ops')):>10}"
        f" done {fmt_ms(avg_time(metrics, prev_metrics, 'ublk_read_completion_wait_ns', 'volume_read_ops')):>10}"
        f" | write q {fmt_ms(avg_time(metrics, prev_metrics, 'ublk_write_queue_wait_ns', 'volume_write_ops')):>10}"
        f" worker {fmt_ms(avg_time(metrics, prev_metrics, 'ublk_write_worker_ns', 'volume_write_ops')):>10}"
        f" durable {fmt_ms(avg_residual_time(metrics, prev_metrics, 'volume_write_total_ns', ('ublk_write_queue_wait_ns', 'ublk_write_worker_ns', 'ublk_write_completion_wait_ns'), 'volume_write_ops')):>10}"
        f" done {fmt_ms(avg_time(metrics, prev_metrics, 'ublk_write_completion_wait_ns', 'volume_write_ops')):>10}",
        f"Read   submit {fmt_ms(avg_time(metrics, prev_metrics, 'read_submit_total_ns', 'read_submit_calls')):>10}"
        f" lookup {fmt_ms(avg_time(metrics, prev_metrics, 'read_submit_buffer_lookup_ns', 'read_submit_calls')):>10}"
        f" meta {fmt_ms(avg_time(metrics, prev_metrics, 'read_submit_meta_get_ns', 'read_submit_calls')):>10}"
        f" unit_io {fmt_ms(avg_time(metrics, prev_metrics, 'read_submit_unit_io_ns', 'read_submit_calls')):>10}",
        f"Pool   req {rate(metrics, prev_metrics, 'read_pool_requests', interval):9.1f}/s"
        f" queue {fmt_ms(avg_time(metrics, prev_metrics, 'read_pool_queue_wait_ns', 'read_pool_requests')):>10}"
        f" submit {fmt_ms(avg_time(metrics, prev_metrics, 'read_pool_submit_wait_ns', 'read_pool_requests')):>10}"
        f" decode {fmt_ms(avg_time(metrics, prev_metrics, 'read_pool_decode_ns', 'read_pool_requests')):>10}",
        "",
        f"Buffer pending={fmt_count(float(pending)):>8} work={fill_pct:>3}% ring={physical_fill_pct:>3}% {bar(float(physical_fill_pct), 100.0, 24)}"
        f" payload={fmt_bytes(float(payload_mem))}/{fmt_bytes(float(payload_limit))}"
        f" volatile={fmt_bytes(float(volatile_mem))}/{fmt_bytes(float(volatile_limit))}",
        f"Buffer append {rate(metrics, prev_metrics, 'buffer_appends', interval):9.1f}/s {fmt_rate_bytes(rate(metrics, prev_metrics, 'buffer_append_bytes', interval)):>14}"
        f" sync_batches {rate(metrics, prev_metrics, 'buffer_sync_batches', interval):8.1f}/s"
        f" flushes {rate(metrics, prev_metrics, 'buffer_sync_flushes', interval):8.1f}/s"
        f" backpressure +{fmt_count(delta(metrics, prev_metrics, 'buffer_backpressure_events'))}",
        "",
        f"Reduce logical={fmt_bytes(logical)} lv3={fmt_bytes(lv3_bytes)} total={ratio(logical, lv3_bytes)}"
        f" compress={ratio(comp_in, comp_out)} dedup_hit={pct(num(metrics, 'dedup_hits'), total_dedup)}",
        f"Dedup  hits={fmt_count(num(metrics, 'dedup_hits'))} misses={fmt_count(num(metrics, 'dedup_misses'))}"
        f" promote={fmt_count(num(metrics, 'dedup_promotions_committed'))}"
        f" skipped={fmt_count(num(metrics, 'dedup_skipped_units'))}"
        f" lookup_avg={fmt_ms(avg_time(metrics, prev_metrics, 'dedup_lookup_ns', 'dedup_lookup_ops'))}",
        f"Index  records={fmt_count(num(meta, 'dedup_index_records'))}"
        f" l0={fmt_count(num(meta, 'dedup_l0_distinct_fps'))}"
        f" l1={fmt_count(num(meta, 'dedup_l1_entries'))}"
        f" ssts={fmt_count(num(meta, 'dedup_index_ssts'))}",
        f"Defrag mode={fmt_count(num(metrics, 'gc_defrag_mode_active'))}"
        f" targets={fmt_count(num(metrics, 'gc_defrag_targets_active'))}"
        f" qfree={fmt_count(num(metrics, 'allocator_quarantine_free_blocks'))}/{fmt_count(num(metrics, 'allocator_quarantine_target_blocks'))}"
        f" reserve={fmt_count(num(metrics, 'allocator_stripe_reserve_blocks'))}"
        f" completed +{fmt_count(delta(metrics, prev_metrics, 'gc_defrag_segments_completed'))}"
        f" cancelled +{fmt_count(delta(metrics, prev_metrics, 'gc_defrag_segments_cancelled'))}"
        f" dedup_reject +{fmt_count(delta(metrics, prev_metrics, 'gc_defrag_dedup_hits_rejected'))}",
        f"Runs   aligned {rate(metrics, prev_metrics, 'flush_writer_group_aligned_ops', interval):8.1f}/s"
        f" unaligned {rate(metrics, prev_metrics, 'flush_writer_group_unaligned_ops', interval):8.1f}/s"
        f" short +{fmt_count(delta(metrics, prev_metrics, 'flush_writer_group_short_extent_allocs'))}"
        f" fallback +{fmt_count(delta(metrics, prev_metrics, 'flush_writer_group_fallback_units'))}"
        f" unused +{fmt_count(delta(metrics, prev_metrics, 'flush_writer_group_unused_blocks'))} blocks",
        "",
        f"Flush  units {rate(metrics, prev_metrics, 'flush_units_written', interval):9.1f}/s"
        f" packed_slots {rate(metrics, prev_metrics, 'flush_packed_slots_written', interval):8.1f}/s"
        f" stale +{fmt_count(delta(metrics, prev_metrics, 'flush_stale_discards'))}"
        f" writer_avg/unit {fmt_ms(avg_writer_time(metrics, prev_metrics, 'flush_writer_total_ns'))}",
        f"Writer/unit alloc {fmt_ms(avg_writer_time(metrics, prev_metrics, 'flush_writer_alloc_ns')):>10}"
        f" io {fmt_ms(avg_writer_time(metrics, prev_metrics, 'flush_writer_io_ns')):>10}"
        f" meta {fmt_ms(avg_writer_time(metrics, prev_metrics, 'flush_writer_meta_ns')):>10}"
        f" cleanup_w {fmt_ms(avg_writer_time(metrics, prev_metrics, 'flush_writer_cleanup_ns')):>10}"
        f" cleanup_th {fmt_ms(avg_time(metrics, prev_metrics, 'flush_cleanup_thread_ns', 'flush_cleanup_thread_batches')):>10}",
        f"Writer/tx meta {fmt_ms(avg_time(metrics, prev_metrics, 'flush_writer_meta_ns', 'flush_writer_meta_commits')):>10}"
        f" service/job {fmt_ms(avg_time(metrics, prev_metrics, 'flush_commit_worker_service_ns', 'flush_commit_worker_jobs')):>10}",
        f"Commit/job total {fmt_ms(avg_time(metrics, prev_metrics, 'flush_commit_worker_queue_wait_ns', 'flush_commit_worker_jobs')):>10}"
        f" aggregator {fmt_ms(avg_time(metrics, prev_metrics, 'flush_commit_worker_aggregator_residence_ns', 'flush_commit_worker_jobs')):>10}"
        f" executor_q {fmt_ms(avg_time(metrics, prev_metrics, 'flush_commit_worker_executor_queue_wait_ns', 'flush_commit_worker_jobs')):>10}",
        f"Commit seals target +{fmt_count(delta(metrics, prev_metrics, 'flush_commit_aggregator_seals_target'))}"
        f" capacity +{fmt_count(delta(metrics, prev_metrics, 'flush_commit_aggregator_seals_capacity'))}"
        f" deadline +{fmt_count(delta(metrics, prev_metrics, 'flush_commit_aggregator_seals_deadline'))}"
        f" adaptive +{fmt_count(delta(metrics, prev_metrics, 'flush_commit_aggregator_seals_adaptive_underfill'))}"
        f" pressure +{fmt_count(delta(metrics, prev_metrics, 'flush_commit_aggregator_seals_pressure'))}",
        "",
        f"Meta   commit {rate(meta, prev.status.get('metadb_memory') if prev else None, 'commit_ops', interval):8.1f}/s"
        f" avg {fmt_us(avg_time(meta, prev.status.get('metadb_memory') if prev else None, 'commit_total_us', 'commit_ops', ns_per_unit=1.0))}"
        f" max {fmt_us(num(meta, 'commit_total_max_us'))}"
        f" wal_fsync {rate(meta, prev.status.get('metadb_memory') if prev else None, 'wal_fsyncs', interval):7.1f}/s",
        f"Sample steady max={fmt_us(num(meta, 'flush_sample_max_us_steady'))}"
        f" forced max={fmt_us(num(meta, 'flush_sample_max_us_forced'))}"
        f" calls steady={fmt_count(num(meta, 'flush_calls_steady'))} forced={fmt_count(num(meta, 'flush_calls_forced'))}",
        f"SampleSize l2p_dirty avg={fmt_count(safe_div(num(meta, 'flush_sample_l2p_dirty_pages'), num(meta, 'flush_calls')))}"
        f" max={fmt_count(num(meta, 'flush_sample_l2p_dirty_pages_max'))}"
        f" rc_drained avg={fmt_count(safe_div(num(meta, 'flush_sample_rc_drained_deltas'), num(meta, 'flush_calls')))}"
        f" max={fmt_count(num(meta, 'flush_sample_rc_drained_deltas_max'))}"
        f" rc_fresh avg={fmt_count(safe_div(num(meta, 'flush_sample_rc_fresh_pages'), num(meta, 'flush_calls')))}"
        f" max={fmt_count(num(meta, 'flush_sample_rc_fresh_pages_max'))}",
        f"RcDrain cycles={fmt_count(num(meta, 'rc_drainer_cycles'))}"
        f" drained={fmt_count(num(meta, 'rc_drainer_drained_entries'))}"
        f" wait_max={fmt_us(num(meta, 'rc_drainer_checkpoint_wait_max_us'))}"
        f" overlay_max={fmt_count(num(meta, 'rc_drainer_overlay_size_max_pages'))}p"
        f" fallback={fmt_count(num(meta, 'rc_drainer_backpressure_fallbacks'))}",
        f"Reclaim budget={fmt_count(rate(meta, prev.status.get('metadb_memory') if prev else None, 'flush_reclaim_budget_pages', interval)):>8}/s"
        f" selected={fmt_count(rate(meta, prev.status.get('metadb_memory') if prev else None, 'flush_reclaim_selected_pages', interval)):>8}/s"
        f" freed={fmt_count(rate(meta, prev.status.get('metadb_memory') if prev else None, 'flush_reclaim_reclaimed_pages', interval)):>8}/s"
        f" blocked={fmt_count(rate(meta, prev.status.get('metadb_memory') if prev else None, 'flush_reclaim_blocked_pages', interval)):>8}/s",
        f"Apply  l2p_q={fmt_count(num(meta, 'pending_l2p_apply_queue'))}"
        f" rc_q={fmt_count(num(meta, 'pending_rc_apply_queue'))}"
        f" dedup_q={fmt_count(num(meta, 'pending_dedup_lane_queue'))}"
        f" dispatch={fmt_count(num(meta, 'pending_dispatch'))}"
        f" pagebuf_dirty={fmt_count(num(meta, 'pending_l2p_pagebuf_dirty') + num(meta, 'pending_rc_pagebuf_dirty'))}",
        "",
        "Errors " + (", ".join(f"{k}={fmt_count(v)}" for k, v in errors[:6]) if errors else "none"),
    ]
    return lines


def pair(stdscr: Any, idx: int) -> int:
    return curses.color_pair(idx) if curses.has_colors() else 0


def put(stdscr: Any, y: int, x: int, text: str, attr: int = 0) -> None:
    rows, cols = stdscr.getmaxyx()
    # Curses may return ERR when drawing into the lower/rightmost cell because
    # that can imply a scroll. Keep one column of breathing room.
    if y < 0 or y >= rows or x >= cols - 1:
        return
    if x < 0:
        text = text[-x:]
        x = 0
    if not text:
        return
    max_chars = cols - x - 1
    if max_chars <= 0:
        return
    try:
        stdscr.addnstr(y, x, text, max_chars, attr)
    except curses.error:
        pass


def hline(stdscr: Any, y: int, x: int, width: int, title: str, attr: int = 0) -> None:
    if width <= 2:
        return
    title = f" {title} " if title else ""
    line = title + "-" * max(0, width - len(title))
    put(stdscr, y, x, line[:width], attr)


def panel(stdscr: Any, y: int, x: int, height: int, width: int, title: str, attr: int = 0) -> None:
    if height <= 1 or width <= 2:
        return
    put(stdscr, y, x, "+" + "-" * (width - 2) + "+", attr)
    for row in range(1, height - 1):
        put(stdscr, y + row, x, "|", attr)
        put(stdscr, y + row, x + width - 1, "|", attr)
    put(stdscr, y + height - 1, x, "+" + "-" * (width - 2) + "+", attr)
    if title:
        put(stdscr, y, x + 2, f" {title} ", attr | curses.A_BOLD)


def metric(stdscr: Any, y: int, x: int, label: str, value: str, width: int, attr: int = 0) -> None:
    if width < 10:
        put(stdscr, y, x, f"{label} {value}", attr)
        return
    label_width = min(14, max(6, width // 3))
    put(stdscr, y, x, label[:label_width].ljust(label_width), curses.A_DIM)
    put(stdscr, y, x + label_width + 1, value[: max(0, width - label_width - 1)], attr)


def draw_dashboard(stdscr: Any, monitor: Monitor) -> bool:
    if monitor.cur is None:
        return False

    height, width = stdscr.getmaxyx()
    if height < 35 or width < 96:
        return False

    cur = monitor.cur
    prev = monitor.prev
    status = cur.status
    metrics = cur.metrics
    system = cur.system
    prev_metrics = prev.metrics if prev is not None else None
    prev_system = prev.system if prev is not None else None
    interval = monitor.interval()
    meta = nested(status, "metadb_memory", default={}) or {}
    prev_meta = nested(prev.status, "metadb_memory", default={}) if prev else None
    errors = collect_errors(metrics, status)

    read_iops = rate(metrics, prev_metrics, "volume_read_ops", interval)
    write_iops = rate(metrics, prev_metrics, "volume_write_ops", interval)
    read_bps = rate(metrics, prev_metrics, "volume_read_bytes", interval)
    write_bps = rate(metrics, prev_metrics, "volume_write_bytes", interval)
    pending = float(nested(status, "buffer_pending_entries", default=0) or 0)
    fill_pct = float(nested(status, "buffer_fill_pct", default=0) or 0)
    physical_fill_pct = float(
        nested(status, "buffer_physical_fill_pct", default=fill_pct) or 0
    )
    payload_mem = float(nested(status, "buffer_payload_memory_bytes", default=0) or 0)
    payload_limit = float(nested(status, "buffer_payload_memory_limit_bytes", default=0) or 0)
    volatile_mem = float(nested(status, "buffer_volatile_payload_memory_bytes", default=0) or 0)
    volatile_limit = float(nested(status, "buffer_volatile_payload_memory_limit_bytes", default=0) or 0)
    logical = num(metrics, "volume_write_bytes") or num(metrics, "buffer_append_bytes")
    lv3_bytes = num(metrics, "lv3_write_compressed_bytes")
    comp_in = num(metrics, "compress_input_bytes")
    comp_out = num(metrics, "compress_output_bytes")
    total_dedup = num(metrics, "dedup_hits") + num(metrics, "dedup_misses")
    total_ratio = safe_div(logical, lv3_bytes)
    comp_ratio = safe_div(comp_in, comp_out)
    dedup_hit_pct = safe_div(num(metrics, "dedup_hits"), total_dedup) * 100.0
    commit_avg_us = avg_time(meta, prev_meta, "commit_total_us", "commit_ops", ns_per_unit=1.0)
    writer_total_ms = avg_writer_time(metrics, prev_metrics, "flush_writer_total_ns") / 1_000_000.0
    writer_io_ms = avg_writer_time(metrics, prev_metrics, "flush_writer_io_ns") / 1_000_000.0
    writer_meta_ms = avg_writer_time(metrics, prev_metrics, "flush_writer_meta_ns") / 1_000_000.0
    writer_meta_tx_ms = avg_time(
        metrics, prev_metrics, "flush_writer_meta_ns", "flush_writer_meta_commits"
    ) / 1_000_000.0
    writer_queue_job_ms = avg_time(
        metrics,
        prev_metrics,
        "flush_commit_worker_queue_wait_ns",
        "flush_commit_worker_jobs",
    ) / 1_000_000.0
    writer_aggregator_job_ms = avg_time(
        metrics,
        prev_metrics,
        "flush_commit_worker_aggregator_residence_ns",
        "flush_commit_worker_jobs",
    ) / 1_000_000.0
    writer_executor_queue_job_ms = avg_time(
        metrics,
        prev_metrics,
        "flush_commit_worker_executor_queue_wait_ns",
        "flush_commit_worker_jobs",
    ) / 1_000_000.0
    cpu_delta = 0.0 if prev_system is None else max(0.0, system.get("cpu_total", 0.0) - prev_system.get("cpu_total", 0.0))
    idle_delta = 0.0 if prev_system is None else max(0.0, system.get("cpu_idle", 0.0) - prev_system.get("cpu_idle", 0.0))
    cpu_pct = safe_div(cpu_delta - idle_delta, cpu_delta) * 100.0
    onyx_cpu = (
        safe_div(
            system.get("onyx_cpu_ticks", 0.0) - (prev_system or {}).get("onyx_cpu_ticks", 0.0),
            CLK_TCK * interval,
        )
        * 100.0
        if interval > 0
        else 0.0
    )
    fio_cpu = (
        safe_div(
            system.get("fio_cpu_ticks", 0.0) - (prev_system or {}).get("fio_cpu_ticks", 0.0),
            CLK_TCK * interval,
        )
        * 100.0
        if interval > 0
        else 0.0
    )
    mem_total = system.get("mem_total", 0.0)
    mem_used = system.get("mem_used", 0.0)
    mem_avail = system.get("mem_available", 0.0)
    health_attr = pair(stdscr, 1) if not errors else pair(stdscr, 3) | curses.A_BOLD

    title_attr = pair(stdscr, 6) | curses.A_BOLD
    dim = curses.A_DIM
    accent = pair(stdscr, 4) | curses.A_BOLD
    good = pair(stdscr, 1)
    warn = pair(stdscr, 2)

    header = " ONYX STORAGE LIVE "
    put(stdscr, 0, 0, "=" * width, pair(stdscr, 4))
    put(stdscr, 0, 2, header, accent)
    put(stdscr, 0, 24, f"{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(cur.ts))}", dim)
    put(
        stdscr,
        0,
        width - 38,
        f"{'OK' if not errors else 'WARN':>5}  mode={status.get('mode', '-')}",
        health_attr,
    )
    put(
        stdscr,
        1,
        2,
        f"socket {monitor.socket_path} | uptime {fmt_count(num(metrics, 'uptime_secs'))}s | volumes {status.get('volume_count', '-')} | zones {status.get('zone_count', '-')}"
        f" | cpu {cpu_pct:4.1f}% | mem {fmt_bytes(mem_used)}/{fmt_bytes(mem_total)}",
        dim,
    )

    usable_width = max(1, width - 1)
    left_w = usable_width // 2 - 1
    right_x = left_w + 1
    right_w = usable_width - right_x

    panel(stdscr, 3, 0, 6, usable_width, "Host", title_attr)
    host_gauge_w = max(8, usable_width // 3 - 18)
    put(stdscr, 5, 2, f"cpu   {cpu_pct:5.1f}% {gauge(cpu_pct, 100.0, host_gauge_w)} load {system.get('load1', 0.0):.1f}/{system.get('load5', 0.0):.1f}/{system.get('load15', 0.0):.1f}", warn if cpu_pct > 80 else good)
    put(stdscr, 5, usable_width // 3 + 2, f"mem   {safe_div(mem_used, mem_total) * 100.0:5.1f}% {gauge(mem_used, mem_total, host_gauge_w)} avail {fmt_bytes(mem_avail)}", warn if safe_div(mem_used, mem_total) > 0.85 else good)
    put(stdscr, 5, 2 * usable_width // 3 + 2, f"proc  onyx {onyx_cpu:5.1f}% {fmt_bytes(system.get('onyx_rss_bytes', 0.0))}  fio {fio_cpu:5.1f}% {fmt_bytes(system.get('fio_rss_bytes', 0.0))}", accent)

    panel(stdscr, 10, 0, 8, left_w, "Frontdoor IO", title_attr)
    metric(stdscr, 12, 2, "read iops", f"{fnum(read_iops)}  {fmt_rate_bytes(read_bps)}", left_w - 4, good)
    metric(stdscr, 13, 2, "write iops", f"{fnum(write_iops)}  {fmt_rate_bytes(write_bps)}", left_w - 4, good)
    metric(
        stdscr,
        14,
        2,
        "lat avg",
        f"read {fmt_ms(avg_time(metrics, prev_metrics, 'volume_read_total_ns', 'volume_read_ops'))}  write {fmt_ms(avg_time(metrics, prev_metrics, 'volume_write_total_ns', 'volume_write_ops'))}",
        left_w - 4,
    )
    io_hist = [item["read_iops"] + item["write_iops"] for item in monitor.history]
    put(stdscr, 16, 2, "iops " + spark(io_hist, left_w - 9), accent)

    panel(stdscr, 10, right_x, 8, right_w, "Read Path", title_attr)
    metric(
        stdscr,
        12,
        right_x + 2,
        "submit",
        f"{fmt_ms(avg_time(metrics, prev_metrics, 'read_submit_total_ns', 'read_submit_calls'))}",
        right_w - 4,
    )
    metric(
        stdscr,
        13,
        right_x + 2,
        "split",
        f"lookup {fmt_ms(avg_time(metrics, prev_metrics, 'read_submit_buffer_lookup_ns', 'read_submit_calls'))}  meta {fmt_ms(avg_time(metrics, prev_metrics, 'read_submit_meta_get_ns', 'read_submit_calls'))}  unit {fmt_ms(avg_time(metrics, prev_metrics, 'read_submit_unit_io_ns', 'read_submit_calls'))}",
        right_w - 4,
    )
    metric(
        stdscr,
        14,
        right_x + 2,
        "pool",
        f"req {fnum(rate(metrics, prev_metrics, 'read_pool_requests', interval))}/s  q {fmt_ms(avg_time(metrics, prev_metrics, 'read_pool_queue_wait_ns', 'read_pool_requests'))}  submit {fmt_ms(avg_time(metrics, prev_metrics, 'read_pool_submit_wait_ns', 'read_pool_requests'))}",
        right_w - 4,
    )
    pool_hist = [item["write_mib"] for item in monitor.history]
    put(stdscr, 16, right_x + 2, "mb/s " + spark(pool_hist, right_w - 10), accent)

    panel(stdscr, 19, 0, 8, left_w, "Buffer Pressure", title_attr)
    put(stdscr, 21, 2, f"work    {fill_pct:5.1f}% {gauge(fill_pct, 100.0, left_w - 20)}", warn if fill_pct > 70 else good)
    put(
        stdscr,
        22,
        2,
        f"ring    {physical_fill_pct:5.1f}% {gauge(physical_fill_pct, 100.0, left_w - 20)}",
        warn if physical_fill_pct > 70 else good,
    )
    put(
        stdscr,
        23,
        2,
        f"payload {fmt_bytes(payload_mem):>10} {gauge(payload_mem, payload_limit, left_w - 24)} {fmt_bytes(payload_limit)}",
        good,
    )
    metric(
        stdscr,
        25,
        2,
        "pending",
        f"{fmt_count(pending)}  append {fnum(rate(metrics, prev_metrics, 'buffer_appends', interval))}/s  sync {fnum(rate(metrics, prev_metrics, 'buffer_sync_batches', interval))}/s  backpressure +{fmt_count(delta(metrics, prev_metrics, 'buffer_backpressure_events'))}",
        left_w - 4,
        warn if pending > 10_000 else 0,
    )

    panel(stdscr, 19, right_x, 8, right_w, "Data Reduction", title_attr)
    put(stdscr, 21, right_x + 2, f"total   {total_ratio:5.2f}x {gauge(total_ratio, 3.5, right_w - 20)} target 3.5x", accent)
    put(stdscr, 22, right_x + 2, f"compress{comp_ratio:5.2f}x {gauge(comp_ratio, 3.0, right_w - 20)}", good)
    put(stdscr, 23, right_x + 2, f"dedup   {dedup_hit_pct:5.1f}% {gauge(dedup_hit_pct, 45.0, right_w - 20)}", good)
    metric(
        stdscr,
        25,
        right_x + 2,
        "index",
        f"hits {fmt_count(num(metrics, 'dedup_hits'))}  promote {fmt_count(num(metrics, 'dedup_promotions_committed'))}  l0 {fmt_count(num(meta, 'dedup_l0_distinct_fps'))}  l1 {fmt_count(num(meta, 'dedup_l1_entries'))}",
        right_w - 4,
    )

    panel(stdscr, 28, 0, 7, left_w, "Flush Writer", title_attr)
    metric(
        stdscr,
        30,
        2,
        "throughput",
        f"units {fnum(rate(metrics, prev_metrics, 'flush_units_written', interval))}/s  packed {fnum(rate(metrics, prev_metrics, 'flush_packed_slots_written', interval))}/s",
        left_w - 4,
    )
    metric(
        stdscr,
        31,
        2,
        "per unit",
        f"total {writer_total_ms:,.2f} ms  io {writer_io_ms:,.2f} ms  meta {writer_meta_ms:,.2f} ms",
        left_w - 4,
        warn if writer_meta_ms > 5 else 0,
    )
    metric(
        stdscr,
        32,
        2,
        "tx / total",
        f"meta/tx {writer_meta_tx_ms:,.2f} ms  total/job {writer_queue_job_ms:,.2f} ms",
        left_w - 4,
    )
    metric(
        stdscr,
        33,
        2,
        "queue split",
        f"aggregator/job {writer_aggregator_job_ms:,.2f} ms  executor/job {writer_executor_queue_job_ms:,.2f} ms",
        left_w - 4,
        warn if writer_executor_queue_job_ms > 10 else 0,
    )

    panel(stdscr, 28, right_x, 7, right_w, "MetaDB", title_attr)
    metric(
        stdscr,
        30,
        right_x + 2,
        "commit",
        f"{fnum(rate(meta, prev_meta, 'commit_ops', interval))}/s  avg {fmt_us(commit_avg_us)}  max {fmt_us(num(meta, 'commit_total_max_us'))}",
        right_w - 4,
        warn if commit_avg_us > 10_000 else 0,
    )
    metric(
        stdscr,
        31,
        right_x + 2,
        "apply q",
        f"l2p {fmt_count(num(meta, 'pending_l2p_apply_queue'))}  rc {fmt_count(num(meta, 'pending_rc_apply_queue'))}  dedup {fmt_count(num(meta, 'pending_dedup_lane_queue'))}  dispatch {fmt_count(num(meta, 'pending_dispatch'))}",
        right_w - 4,
    )
    metric(
        stdscr,
        32,
        right_x + 2,
        "reclaim",
        f"free {fmt_count(num(meta, 'free_list_pages'))}  deferred {fmt_count(num(meta, 'pending_deferred_free'))}  freed {fnum(rate(meta, prev_meta, 'flush_reclaim_reclaimed_pages', interval))}/s",
        right_w - 4,
        warn if num(meta, "pending_deferred_free") > 1_000_000 else 0,
    )
    put(stdscr, 33, right_x + 2, "commit " + spark([item["commit_ms"] for item in monitor.history], right_w - 11), accent)

    if height > 36:
        hline(stdscr, 36, 0, width, "Shard Heads", title_attr)
        shards = nested(status, "buffer_shards", default=[]) or []
        cells = []
        for shard in shards[: min(len(shards), max(1, (width - 2) // 10))]:
            age = shard.get("head_age_ms")
            pend = shard.get("pending_entries", 0)
            marker = "!" if isinstance(age, int) and age > 10_000 else "."
            cells.append(f"{int(shard.get('shard_idx', 0)):02d}{marker}{int(pend):03d}")
        put(stdscr, 37, 2, " ".join(cells), dim)

    footer_y = height - 2
    error_text = "none" if not errors else ", ".join(f"{key}={fmt_count(value)}" for key, value in errors[:5])
    put(stdscr, footer_y, 0, "-" * usable_width, pair(stdscr, 4))
    put(stdscr, footer_y + 1, 2, f"Errors {error_text}", health_attr)
    put(stdscr, footer_y + 1, width - 36, "q quit | r refresh | t text", dim)
    return True


def draw(stdscr: Any, monitor: Monitor, interval: float) -> None:
    curses.curs_set(0)
    stdscr.nodelay(True)
    stdscr.timeout(100)
    text_mode = False
    if curses.has_colors():
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_GREEN, -1)
        curses.init_pair(2, curses.COLOR_YELLOW, -1)
        curses.init_pair(3, curses.COLOR_RED, -1)
        curses.init_pair(4, curses.COLOR_CYAN, -1)
        curses.init_pair(5, curses.COLOR_MAGENTA, -1)
        curses.init_pair(6, curses.COLOR_BLUE, -1)
    next_poll = 0.0
    while True:
        now = time.time()
        if now >= next_poll:
            monitor.poll()
            next_poll = now + interval
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        attr = curses.color_pair(4) if curses.has_colors() else 0
        if monitor.cur is None:
            line = f"waiting for {monitor.socket_path}"
            if monitor.error:
                line += f"  error={monitor.error}"
            stdscr.addnstr(0, 0, line, width - 1, attr)
        else:
            rendered = False if text_mode else draw_dashboard(stdscr, monitor)
            if not rendered:
                lines = build_lines(monitor.cur, monitor.prev, monitor.socket_path)
                for y, line in enumerate(lines[: max(0, height - 2)]):
                    line_attr = 0
                    if y == 0:
                        line_attr = attr
                    if line.startswith("Errors ") and "none" not in line:
                        line_attr = pair(stdscr, 3) if curses.has_colors() else curses.A_BOLD
                    stdscr.addnstr(y, 0, line, width - 1, line_attr)
                footer = "q quit | r refresh | t dashboard/text | Ctrl-C exit"
                if monitor.error:
                    footer += f" | metrics error: {monitor.error}"
                if monitor.status_error:
                    footer += f" | status error: {monitor.status_error}"
                stdscr.addnstr(height - 1, 0, footer, width - 1, curses.A_DIM)
        stdscr.refresh()
        ch = stdscr.getch()
        if ch in (ord("q"), ord("Q")):
            return
        if ch in (ord("r"), ord("R")):
            monitor.next_status_poll = 0.0
            next_poll = 0.0
        if ch in (ord("t"), ord("T")):
            text_mode = not text_mode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live TUI for Onyx status-json and metrics-json")
    parser.add_argument("--config", default="config/nvme-detailed.toml")
    parser.add_argument("--socket-path")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--timeout", type=float, default=2.0)
    parser.add_argument(
        "--status-interval",
        type=float,
        default=10.0,
        help="Seconds between status-json polls; metrics-json still uses --interval",
    )
    parser.add_argument(
        "--status-timeout",
        type=float,
        default=0.5,
        help="Timeout for status-json polls; keep short during performance tests",
    )
    parser.add_argument(
        "--no-status",
        action="store_true",
        help="Do not poll status-json/metadb_memory; display metrics-json only",
    )
    parser.add_argument("--once", action="store_true", help="Print one text snapshot instead of opening curses")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = pathlib.Path(args.config)
    socket_path = load_socket_path(config_path, args.socket_path)
    monitor = Monitor(
        socket_path,
        args.timeout,
        args.status_timeout,
        args.status_interval,
        args.no_status,
    )
    monitor.poll(force_status=True)
    if args.once:
        time.sleep(max(0.2, args.interval))
        monitor.poll()
        if monitor.cur is None:
            raise SystemExit(monitor.error or "no sample")
        print("\n".join(build_lines(monitor.cur, monitor.prev, socket_path)))
        return 0
    curses.wrapper(draw, monitor, max(0.2, args.interval))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
