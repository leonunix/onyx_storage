#!/usr/bin/env python3
"""Measure logical-buffer and physical-ring drain times over the Onyx IPC socket."""

from __future__ import annotations

import argparse
import json
import pathlib
import socket
import time


def send_socket_cmd(socket_path: pathlib.Path, cmd: str, timeout: float) -> dict:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.connect(str(socket_path))
        sock.sendall((cmd + "\n").encode())
        lines = []
        with sock.makefile("r", encoding="utf-8") as stream:
            for raw_line in stream:
                line = raw_line.strip()
                if not line:
                    continue
                if line == "ok" or line.startswith("ok "):
                    break
                if line.startswith("error:"):
                    raise RuntimeError(line.removeprefix("error:").strip())
                lines.append(line)
    return json.loads(lines[0]) if lines else {}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--status-timeout", type=float, default=30.0)
    args = parser.parse_args()

    started = time.monotonic()
    pending_zero_secs = None
    ring_zero_secs = None
    samples = []
    while time.monotonic() - started <= args.timeout:
        payload = send_socket_cmd(args.socket, "status-json", args.status_timeout)
        status = payload.get("status", payload)
        elapsed = time.monotonic() - started
        if status.get("buffer_pending_entries") is None:
            raise RuntimeError("status-json omitted buffer_pending_entries")
        shards = status.get("buffer_shards")
        if not isinstance(shards, list) or not shards:
            raise RuntimeError("status-json omitted non-empty buffer_shards")
        pending = int(status["buffer_pending_entries"])
        ring_pct = int(status.get("buffer_physical_fill_pct", 0))
        physical_used_bytes = sum(int(shard["used_bytes"]) for shard in shards)
        sample = {
            "elapsed_secs": round(elapsed, 3),
            "pending_entries": pending,
            "physical_fill_pct": ring_pct,
            "physical_used_bytes": physical_used_bytes,
        }
        samples.append(sample)
        print(json.dumps(sample), flush=True)
        if pending == 0 and pending_zero_secs is None:
            pending_zero_secs = elapsed
        if physical_used_bytes == 0 and ring_zero_secs is None:
            ring_zero_secs = elapsed
        if pending_zero_secs is not None and ring_zero_secs is not None:
            break
        time.sleep(max(0.0, args.interval))

    result = {
        "pending_zero_secs": (
            round(pending_zero_secs, 3) if pending_zero_secs is not None else None
        ),
        "physical_ring_zero_secs": (
            round(ring_zero_secs, 3) if ring_zero_secs is not None else None
        ),
        "timed_out": pending_zero_secs is None or ring_zero_secs is None,
        "samples": samples,
    }
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return 1 if result["timed_out"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
