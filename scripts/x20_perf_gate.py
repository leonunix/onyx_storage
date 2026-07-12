#!/usr/bin/env python3
"""Run the repeatable Onyx X20-class latency and overload gate."""

from __future__ import annotations

import argparse
import json
import pathlib
import socket
import subprocess
import tempfile
from typing import Any


def socket_command(path: pathlib.Path, command: str, timeout: float = 5.0) -> dict[str, Any]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.connect(str(path))
        sock.sendall((command + "\n").encode())
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


def run_fio(
    device: str,
    rw: str,
    jobs: int,
    depth: int,
    runtime: int,
    ramp_time: int,
) -> dict[str, Any]:
    with tempfile.NamedTemporaryFile(suffix=".json") as output:
        subprocess.run(
            [
                "fio",
                f"--name=x20-{rw}-{jobs}j-qd{depth}",
                f"--filename={device}",
                f"--rw={rw}",
                "--bs=4k",
                "--ioengine=libaio",
                "--direct=1",
                f"--iodepth={depth}",
                f"--numjobs={jobs}",
                "--time_based=1",
                f"--runtime={runtime}",
                f"--ramp_time={ramp_time}",
                "--group_reporting=1",
                "--output-format=json",
                f"--output={output.name}",
            ],
            check=True,
        )
        job = json.load(output)["jobs"][0]
    direction = "read" if rw == "randread" else "write"
    data = job[direction]
    percentiles = data["clat_ns"]["percentile"]
    return {
        "rw": rw,
        "jobs": jobs,
        "iodepth": depth,
        "requested_total_qd": jobs * depth,
        "iops": data["iops"],
        "bandwidth_kib_s": data["bw"],
        "errors": data.get("total_err", job.get("error", 0)),
        "p50_ns": percentiles["50.000000"],
        "p99_ns": percentiles["99.000000"],
        "p999_ns": percentiles["99.900000"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--device", default="/dev/ublkb13")
    parser.add_argument("--socket-path", default="/tmp/onyx-storage-nvme.sock")
    parser.add_argument("--runtime", type=int, default=30)
    parser.add_argument("--ramp-time", type=int, default=5)
    parser.add_argument("--overload-runtime", type=int, default=10)
    parser.add_argument("--skip-overload", action="store_true")
    parser.add_argument("--output")
    args = parser.parse_args()

    socket_path = pathlib.Path(args.socket_path)
    before = socket_command(socket_path, "status-json")
    cases = [
        ("read-latency", "randread", 4, 8, 1_000_000),
        ("write-latency", "randwrite", 4, 8, 2_000_000),
    ]
    results = []
    failures = []
    for name, rw, jobs, depth, p99_limit_ns in cases:
        result = run_fio(args.device, rw, jobs, depth, args.runtime, args.ramp_time)
        result.update({"name": name, "p99_limit_ns": p99_limit_ns})
        result["passed"] = result["errors"] == 0 and result["p99_ns"] <= p99_limit_ns
        results.append(result)
        if not result["passed"]:
            failures.append(name)

    if not args.skip_overload:
        for jobs in (1, 2):
            name = f"overload-qd{jobs * 4096}"
            result = run_fio(
                args.device,
                "randwrite",
                jobs,
                4096,
                args.overload_runtime,
                min(2, args.ramp_time),
            )
            try:
                socket_command(socket_path, "metrics-json")
                responsive = True
            except (OSError, RuntimeError, json.JSONDecodeError):
                responsive = False
            result.update({"name": name, "responsive": responsive})
            result["passed"] = result["errors"] == 0 and responsive
            results.append(result)
            if not result["passed"]:
                failures.append(name)

    after = socket_command(socket_path, "status-json")
    report = {
        "profile": "x20-class-v1",
        "passed": not failures,
        "failures": failures,
        "cases": results,
        "buffer_physical_fill_before_pct": before.get("status", before).get(
            "buffer_physical_fill_pct"
        ),
        "buffer_physical_fill_after_pct": after.get("status", after).get(
            "buffer_physical_fill_pct"
        ),
    }
    rendered = json.dumps(report, indent=2)
    if args.output:
        pathlib.Path(args.output).write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
