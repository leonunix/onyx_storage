#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import shlex
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional

try:
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover
    tomllib = None


SIZE_SUFFIXES = {
    "k": 1024,
    "m": 1024**2,
    "g": 1024**3,
    "t": 1024**4,
}
TIME_SUFFIXES = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
}


class WorkflowError(RuntimeError):
    pass


def parse_size(text: str) -> int:
    raw = text.strip().lower()
    if not raw:
        raise ValueError("empty size")
    if raw[-1].isdigit():
        return int(raw)
    suffix = raw[-1]
    if suffix not in SIZE_SUFFIXES:
        raise ValueError(f"unsupported size suffix: {text}")
    return int(float(raw[:-1]) * SIZE_SUFFIXES[suffix])


def parse_duration(text: str) -> int:
    raw = text.strip().lower()
    if not raw:
        raise ValueError("empty duration")
    if raw[-1].isdigit():
        return int(raw)
    suffix = raw[-1]
    if suffix not in TIME_SUFFIXES:
        raise ValueError(f"unsupported duration suffix: {text}")
    return int(float(raw[:-1]) * TIME_SUFFIXES[suffix])


def format_duration(seconds: float) -> str:
    seconds = int(seconds)
    parts: list[str] = []
    for label, unit in (("d", 86400), ("h", 3600), ("m", 60), ("s", 1)):
        if seconds >= unit or (label == "s" and not parts):
            value, seconds = divmod(seconds, unit)
            parts.append(f"{value}{label}")
    return "".join(parts)


def load_socket_path(config_path: pathlib.Path, explicit: Optional[str]) -> pathlib.Path:
    if explicit:
        return pathlib.Path(explicit)
    if tomllib is None:
        raise WorkflowError("python tomllib unavailable; pass --socket-path explicitly")
    with config_path.open("rb") as fh:
        payload = tomllib.load(fh)
    socket_path = payload.get("service", {}).get("socket_path")
    if not socket_path:
        raise WorkflowError("service.socket_path missing from config")
    return pathlib.Path(socket_path)


def load_toml_config(config_path: pathlib.Path) -> dict[str, Any]:
    if tomllib is None:
        raise WorkflowError("python tomllib unavailable")
    with config_path.open("rb") as fh:
        return tomllib.load(fh)


def reset_configured_state(config_path: pathlib.Path, reset_buffer_bytes: int) -> None:
    payload = load_toml_config(config_path)
    meta = payload.get("meta", {})
    for key in ("path", "wal_dir"):
        raw = meta.get(key)
        if not raw:
            continue
        path = pathlib.Path(raw)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if path.is_dir():
                import shutil

                shutil.rmtree(path)
            else:
                path.unlink()

    if reset_buffer_bytes <= 0:
        return
    raw_buffer = payload.get("buffer", {}).get("device")
    if not raw_buffer:
        return
    buffer_path = pathlib.Path(raw_buffer)
    if not buffer_path.exists():
        raise WorkflowError(f"buffer device does not exist: {buffer_path}")
    with buffer_path.open("r+b", buffering=0) as fh:
        zero = bytes(1024 * 1024)
        remaining = reset_buffer_bytes
        while remaining > 0:
            chunk = zero if remaining >= len(zero) else zero[:remaining]
            fh.write(chunk)
            remaining -= len(chunk)
        fh.flush()
        os.fsync(fh.fileno())


def run_cmd(
    cmd: list[str],
    *,
    cwd: pathlib.Path,
    env: dict[str, str],
    check: bool = True,
    stdout=None,
    stderr=None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        check=check,
        text=True,
        stdout=stdout,
        stderr=stderr,
    )


def wait_for(predicate, timeout_secs: int, interval_secs: float = 0.5) -> None:
    deadline = time.time() + timeout_secs
    last_error: Optional[Exception] = None
    while time.time() < deadline:
        try:
            if predicate():
                return
        except Exception as exc:
            last_error = exc
        time.sleep(interval_secs)
    if last_error is not None:
        raise WorkflowError(f"timeout waiting for condition: {last_error}")
    raise WorkflowError("timeout waiting for condition")


@dataclass
class OnyxService:
    repo_root: pathlib.Path
    engine_cmd: list[str]
    config_path: pathlib.Path
    socket_path: pathlib.Path
    volume: str
    env: dict[str, str]
    engine_log: pathlib.Path
    startup_timeout_secs: int
    stop_timeout_secs: int

    proc: Optional[subprocess.Popen[bytes]] = None
    log_fh: Optional[object] = None

    def cli(self, *args: str) -> list[str]:
        return [*self.engine_cmd, "-c", str(self.config_path), *args]

    def send_socket_cmd(self, cmd: str) -> list[str]:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(5.0)
            sock.connect(str(self.socket_path))
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
                    raise WorkflowError(line.removeprefix("error:").strip())
                lines.append(line)
        return lines

    def status_json(self) -> dict[str, object]:
        lines = self.send_socket_cmd("status-json")
        if not lines:
            raise WorkflowError("empty status-json response")
        return json.loads(lines[0])

    def metrics_json(self) -> dict[str, object]:
        lines = self.send_socket_cmd("metrics-json")
        if not lines:
            return {}
        return json.loads(lines[0])

    def cleanup_ublk(self) -> None:
        run_cmd(
            self.cli("cleanup-ublk"),
            cwd=self.repo_root,
            env=self.env,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def create_volume(self, volume_size: int, compression: str) -> None:
        run_cmd(
            self.cli(
                "create-volume",
                "-n",
                self.volume,
                "-s",
                str(volume_size),
                "--compression",
                compression,
            ),
            cwd=self.repo_root,
            env=self.env,
        )

    def start(self) -> pathlib.Path:
        self.log_fh = self.engine_log.open("ab")
        self.proc = subprocess.Popen(
            self.cli("start", "-v", self.volume),
            cwd=str(self.repo_root),
            env=self.env,
            stdout=self.log_fh,
            stderr=subprocess.STDOUT,
        )

        def ready() -> bool:
            if self.proc is not None and self.proc.poll() is not None:
                raise WorkflowError(f"engine exited early with code {self.proc.returncode}")
            payload = self.status_json()
            return bool(payload.get("ublk_devices") or [])

        wait_for(ready, self.startup_timeout_secs, interval_secs=1.0)
        return self.resolve_device()

    def resolve_device(self) -> pathlib.Path:
        payload = self.status_json()
        dev_ids = payload.get("ublk_devices") or []
        if len(dev_ids) != 1:
            raise WorkflowError(f"expected exactly one ublk device, got {dev_ids}")
        path = pathlib.Path(f"/dev/ublkb{int(dev_ids[0])}")
        wait_for(lambda: path.exists(), timeout_secs=30, interval_secs=0.5)
        return path

    def stop(self) -> None:
        try:
            run_cmd(
                self.cli("stop"),
                cwd=self.repo_root,
                env=self.env,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            pass
        if self.proc is not None:
            try:
                self.proc.wait(timeout=self.stop_timeout_secs)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=30)
            self.proc = None
        if self.log_fh is not None:
            self.log_fh.close()
            self.log_fh = None


class Sampler:
    def __init__(self, service: OnyxService, run_dir: pathlib.Path, interval_secs: int) -> None:
        self.service = service
        self.run_dir = run_dir
        self.interval_secs = interval_secs
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="fio-soak-sampler")

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=max(5, self.interval_secs + 5))

    def _append_jsonl(self, name: str, payload: dict[str, object]) -> None:
        with (self.run_dir / name).open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, sort_keys=True) + "\n")

    def _run(self) -> None:
        while not self.stop_event.is_set():
            ts = time.time()
            try:
                status = self.service.status_json()
                self._append_jsonl("status.jsonl", {"ts": ts, "payload": status})
            except Exception as exc:
                self._append_jsonl("events.jsonl", {"ts": ts, "event": "status-error", "error": str(exc)})
            try:
                metrics = self.service.metrics_json()
                self._append_jsonl("metrics.jsonl", {"ts": ts, "payload": metrics})
            except Exception as exc:
                self._append_jsonl("events.jsonl", {"ts": ts, "event": "metrics-error", "error": str(exc)})
            self.stop_event.wait(self.interval_secs)


def render_jobfile(template: pathlib.Path, output: pathlib.Path, values: dict[str, object]) -> None:
    text = template.read_text(encoding="utf-8")
    for key, value in values.items():
        text = text.replace(f"__{key}__", str(value))
    output.write_text(text, encoding="utf-8")


def fio_runtime_arg(seconds: int) -> str:
    return f"{seconds}s"


def render_verify_options(args: argparse.Namespace) -> str:
    if args.verify_mode == "none":
        return "\n".join(
            [
                "verify=0",
                "# Onyx still validates payload CRCs internally on reads; fio checksum",
                "# headers are disabled here to preserve 4K dedupe semantics.",
            ]
        )
    if args.verify_mode == "null":
        return "verify=null"
    return "\n".join(
        [
            f"verify={args.verify_mode}",
            "verify_fatal=1",
            f"verify_async={args.verify_async}",
            f"verify_backlog={args.verify_backlog}",
            f"verify_backlog_batch={args.verify_backlog_batch}",
            "verify_state_save=1",
        ]
    )


ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
PURPOSE_RE = re.compile(r"\bpurpose=([A-Za-z0-9_]+)\b")
NON_FATAL_CRC_PURPOSES = {
    "DedupVerify",
    "DedupVerifyIndex",
    "DedupVerifyCandidate",
    "DedupScanner",
}


def scan_engine_log(engine_log: pathlib.Path) -> dict[str, object]:
    diagnostics: dict[str, object] = {
        "read_pool_crc_mismatches": 0,
        "read_pool_crc_mismatches_fatal": 0,
        "read_pool_crc_mismatches_by_purpose": {},
        "dedup_verify_mismatches": 0,
        "dedup_verify_mismatches_by_purpose": {},
        "dedup_hit_rejected": 0,
    }
    if not engine_log.exists():
        return diagnostics

    by_purpose: dict[str, int] = {}
    dedup_verify_by_purpose: dict[str, int] = {}
    with engine_log.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if "read-pool: dedup verify mismatch" in line:
                diagnostics["dedup_verify_mismatches"] = (
                    int(diagnostics["dedup_verify_mismatches"]) + 1
                )
                clean_line = ANSI_RE.sub("", line)
                match = PURPOSE_RE.search(clean_line)
                purpose = match.group(1) if match else "unknown"
                dedup_verify_by_purpose[purpose] = dedup_verify_by_purpose.get(purpose, 0) + 1
            elif "read-pool: CRC mismatch" in line:
                diagnostics["read_pool_crc_mismatches"] = int(diagnostics["read_pool_crc_mismatches"]) + 1
                clean_line = ANSI_RE.sub("", line)
                match = PURPOSE_RE.search(clean_line)
                purpose = match.group(1) if match else "unknown"
                by_purpose[purpose] = by_purpose.get(purpose, 0) + 1
                if purpose in NON_FATAL_CRC_PURPOSES:
                    diagnostics["dedup_verify_mismatches"] = (
                        int(diagnostics["dedup_verify_mismatches"]) + 1
                    )
                    dedup_verify_by_purpose[purpose] = dedup_verify_by_purpose.get(purpose, 0) + 1
                else:
                    diagnostics["read_pool_crc_mismatches_fatal"] = (
                        int(diagnostics["read_pool_crc_mismatches_fatal"]) + 1
                    )
            if "dedup worker: hit rejected" in line:
                diagnostics["dedup_hit_rejected"] = int(diagnostics["dedup_hit_rejected"]) + 1
    diagnostics["read_pool_crc_mismatches_by_purpose"] = by_purpose
    diagnostics["dedup_verify_mismatches_by_purpose"] = dedup_verify_by_purpose
    return diagnostics


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a long fio dedupe+compression integrity soak against Onyx ublk"
    )
    parser.add_argument("--repo-root", default=".", help="Repository root")
    parser.add_argument("--config", default="config/nvme-detailed.toml", help="Onyx config TOML")
    parser.add_argument(
        "--engine-cmd",
        default="target/release/onyx-storage",
        help="Engine command, e.g. 'target/release/onyx-storage'",
    )
    parser.add_argument("--socket-path", help="Override service.socket_path from config")
    parser.add_argument("--run-dir", help="Run directory; defaults under .dev/fio-dedupe-compress-soak")
    parser.add_argument("--volume", default="fio-dedupe-compress-soak")
    parser.add_argument("--volume-size", type=parse_size, default=parse_size("320g"))
    parser.add_argument("--compression", choices=["none", "lz4", "zstd"], default="lz4")
    parser.add_argument("--runtime", type=parse_duration, default=parse_duration("12h"))
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--iodepth", type=int, default=4)
    parser.add_argument("--rwmixread", type=int, default=50)
    parser.add_argument("--min-bs", default="4k")
    parser.add_argument("--max-bs", default="32k")
    parser.add_argument(
        "--rate-iops",
        type=int,
        default=200,
        help="fio rate_iops per job; default 200 means about 800 total IOPS",
    )
    parser.add_argument("--dedupe-percentage", type=int, default=45)
    parser.add_argument("--dedupe-working-set-percentage", type=int, default=5)
    parser.add_argument("--buffer-compress-percentage", type=int, default=50)
    parser.add_argument("--buffer-compress-chunk", default="4k")
    parser.add_argument(
        "--verify-mode",
        choices=["none", "null", "crc32c", "crc32", "xxhash", "sha256"],
        default="none",
        help=(
            "fio verification mode. Default 'none' preserves dedupe/compression "
            "workload shape; checksum modes write per-block headers and defeat "
            "4K dedupe, so use them as a separate strict fio-verify run."
        ),
    )
    parser.add_argument("--verify-async", type=int, default=2)
    parser.add_argument("--verify-backlog", type=int, default=4096)
    parser.add_argument("--verify-backlog-batch", type=int, default=512)
    parser.add_argument("--sample-interval", type=parse_duration, default=parse_duration("60s"))
    parser.add_argument("--startup-timeout", type=parse_duration, default=parse_duration("20m"))
    parser.add_argument("--stop-timeout", type=parse_duration, default=parse_duration("5m"))
    parser.add_argument("--template", default="config/fio-dedupe-compress-soak.fio")
    parser.add_argument(
        "--no-reset",
        action="store_true",
        help="Do not reset configured meta/wal/LV2 state first",
    )
    parser.add_argument(
        "--reset-buffer-bytes",
        type=parse_size,
        default=parse_size("64m"),
        help="Bytes to zero at the front of the configured LV2 buffer during reset",
    )
    parser.add_argument("--leave-running", action="store_true", help="Leave the engine running at the end")
    args = parser.parse_args(argv)

    if args.jobs <= 0 or args.iodepth <= 0:
        parser.error("--jobs and --iodepth must be > 0")
    if not 0 <= args.rwmixread <= 100:
        parser.error("--rwmixread must be in [0, 100]")
    for name in ("dedupe_percentage", "dedupe_working_set_percentage", "buffer_compress_percentage"):
        value = getattr(args, name)
        if not 0 <= value <= 100:
            parser.error(f"--{name.replace('_', '-')} must be in [0, 100]")
    if args.rate_iops <= 0:
        parser.error("--rate-iops must be > 0")
    if args.runtime <= 0:
        parser.error("--runtime must be > 0")
    if args.verify_async < 0:
        parser.error("--verify-async must be >= 0")
    if args.verify_backlog <= 0 or args.verify_backlog_batch <= 0:
        parser.error("--verify-backlog and --verify-backlog-batch must be > 0")
    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    repo_root = pathlib.Path(args.repo_root).resolve()
    config_path = (repo_root / args.config).resolve() if not pathlib.Path(args.config).is_absolute() else pathlib.Path(args.config)
    template = (repo_root / args.template).resolve() if not pathlib.Path(args.template).is_absolute() else pathlib.Path(args.template)
    socket_path = load_socket_path(config_path, args.socket_path)
    engine_cmd = shlex.split(args.engine_cmd)
    env = os.environ.copy()
    env["PATH"] = f"{env.get('HOME', str(pathlib.Path.home()))}/.cargo/bin:{env.get('PATH', '')}"

    run_dir = pathlib.Path(args.run_dir) if args.run_dir else repo_root / ".dev" / "fio-dedupe-compress-soak" / time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    if not run_dir.is_absolute():
        run_dir = repo_root / run_dir
    run_dir.mkdir(parents=True, exist_ok=True)

    service = OnyxService(
        repo_root=repo_root,
        engine_cmd=engine_cmd,
        config_path=config_path,
        socket_path=socket_path,
        volume=args.volume,
        env=env,
        engine_log=run_dir / "engine.log",
        startup_timeout_secs=args.startup_timeout,
        stop_timeout_secs=args.stop_timeout,
    )

    summary: dict[str, object] = {
        "final": False,
        "failure": None,
        "run_dir": str(run_dir),
        "volume": args.volume,
        "runtime_secs": args.runtime,
        "jobs": args.jobs,
        "iodepth": args.iodepth,
        "target_outstanding": args.jobs * args.iodepth,
        "rate_iops_per_job": args.rate_iops,
        "dedupe_percentage": args.dedupe_percentage,
        "dedupe_working_set_percentage": args.dedupe_working_set_percentage,
        "buffer_compress_percentage": args.buffer_compress_percentage,
        "verify_mode": args.verify_mode,
    }
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    sampler: Optional[Sampler] = None
    started = time.time()
    try:
        service.cleanup_ublk()
        if not args.no_reset:
            reset_configured_state(config_path, args.reset_buffer_bytes)
            service.create_volume(args.volume_size, args.compression)
        device = service.start()

        rendered_job = run_dir / "job.fio"
        render_jobfile(
            template,
            rendered_job,
            {
                "RUNTIME": fio_runtime_arg(args.runtime),
                "FILENAME": device,
                "SIZE": args.volume_size,
                "RWMIXREAD": args.rwmixread,
                "MIN_BS": args.min_bs,
                "MAX_BS": args.max_bs,
                "JOBS": args.jobs,
                "IODEPTH": args.iodepth,
                "RATE_IOPS": args.rate_iops,
                "VERIFY_OPTIONS": render_verify_options(args),
                "DEDUPE_PERCENTAGE": args.dedupe_percentage,
                "DEDUPE_WORKING_SET_PERCENTAGE": args.dedupe_working_set_percentage,
                "BUFFER_COMPRESS_PERCENTAGE": args.buffer_compress_percentage,
                "BUFFER_COMPRESS_CHUNK": args.buffer_compress_chunk,
            },
        )

        sampler = Sampler(service, run_dir, args.sample_interval)
        sampler.start()
        fio_json = run_dir / "fio-result.json"
        fio_log = run_dir / "fio.log"
        fio_cmd = ["fio", str(rendered_job), "--output", str(fio_json), "--output-format=json"]
        (run_dir / "command.sh").write_text(
            " ".join(shlex.quote(part) for part in fio_cmd) + "\n",
            encoding="utf-8",
        )
        with fio_log.open("w", encoding="utf-8") as log:
            proc = subprocess.run(
                fio_cmd,
                cwd=str(repo_root),
                env=env,
                text=True,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        summary["fio_returncode"] = proc.returncode
        if proc.returncode != 0:
            summary["failure"] = f"fio exited with {proc.returncode}"

        try:
            summary["final_status"] = service.status_json()
            summary["final_metrics"] = service.metrics_json()
        except Exception as exc:
            summary["status_error"] = str(exc)
    except Exception as exc:
        summary["failure"] = str(exc)
        return_code = 1
    else:
        return_code = 0 if summary.get("failure") is None else 1
    finally:
        if sampler is not None:
            sampler.stop()
        if not args.leave_running:
            try:
                service.stop()
            except Exception as exc:
                summary["stop_error"] = str(exc)
                return_code = 1
        engine_log_diagnostics = scan_engine_log(service.engine_log)
        summary["engine_log_diagnostics"] = engine_log_diagnostics
        fatal_crc_mismatches = int(engine_log_diagnostics.get("read_pool_crc_mismatches_fatal", 0))
        if fatal_crc_mismatches and summary.get("failure") is None:
            summary["failure"] = f"engine logged {fatal_crc_mismatches} fatal read-pool CRC mismatches"
            return_code = 1
        summary["elapsed_secs"] = time.time() - started
        summary["elapsed_human"] = format_duration(float(summary["elapsed_secs"]))
        summary["final"] = True
        (run_dir / "summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    print(json.dumps(summary, indent=2, sort_keys=True))
    return return_code


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
