#!/usr/bin/env python3
from __future__ import annotations

import argparse
import collections
import contextlib
import errno
import json
import mmap
import os
import pathlib
import random
import shlex
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from typing import Deque, Iterable, Optional

try:
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover
    tomllib = None


BLOCK_SIZE = 4096
ZERO_BLOCK = bytes(BLOCK_SIZE)
STATE_BYTES = 8
MODE_ZERO = 0
MODE_REPEAT_A = 1
MODE_REPEAT_B = 2
MODE_REPEAT_C = 3
MODE_NAMES = {
    MODE_ZERO: "zero",
    MODE_REPEAT_A: "repeat-a",
    MODE_REPEAT_B: "repeat-b",
    MODE_REPEAT_C: "repeat-c",
}
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


class HarnessError(RuntimeError):
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


def format_bytes(value: float) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    idx = 0
    while value >= 1024 and idx < len(units) - 1:
        value /= 1024
        idx += 1
    return f"{value:.2f} {units[idx]}"


def format_duration(seconds: float) -> str:
    seconds = int(seconds)
    parts = []
    for label, unit in (("d", 86400), ("h", 3600), ("m", 60), ("s", 1)):
        if seconds >= unit or (label == "s" and not parts):
            value, seconds = divmod(seconds, unit)
            parts.append(f"{value}{label}")
    return "".join(parts)


def hexdump_prefix(data: bytes, width: int = 64) -> str:
    return data[:width].hex()


def first_mismatch(expected: bytes, actual: bytes) -> Optional[int]:
    limit = min(len(expected), len(actual))
    for idx in range(limit):
        if expected[idx] != actual[idx]:
            return idx
    if len(expected) != len(actual):
        return limit
    return None


def load_socket_path(config_path: pathlib.Path, explicit: Optional[str]) -> pathlib.Path:
    if explicit:
        return pathlib.Path(explicit)
    if tomllib is None:
        raise HarnessError("python tomllib unavailable; pass --socket-path explicitly")
    with config_path.open("rb") as fh:
        payload = tomllib.load(fh)
    socket_path = payload.get("service", {}).get("socket_path")
    if not socket_path:
        raise HarnessError("service.socket_path missing from config")
    return pathlib.Path(socket_path)


def run_cmd(
    cmd: list[str],
    *,
    cwd: pathlib.Path,
    env: dict[str, str],
    check: bool = True,
    capture: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        check=check,
        text=True,
        capture_output=capture,
    )


def wait_for(predicate, timeout_secs: int, interval_secs: float = 0.5) -> None:
    deadline = time.time() + timeout_secs
    last_error: Optional[Exception] = None
    while time.time() < deadline:
        try:
            if predicate():
                return
        except Exception as exc:  # pragma: no cover
            last_error = exc
        time.sleep(interval_secs)
    if last_error is not None:
        raise HarnessError(f"timeout waiting for condition: {last_error}")
    raise HarnessError("timeout waiting for condition")


def build_block(seed: int, state: int, block_index: int) -> bytes:
    if state == 0:
        return ZERO_BLOCK

    mode = state & 0x3
    stamp = state >> 2
    word0 = seed & 0xFFFFFFFFFFFFFFFF
    word1 = stamp & 0xFFFFFFFFFFFFFFFF
    word2 = block_index & 0xFFFFFFFFFFFFFFFF
    word3 = (seed ^ stamp ^ (block_index * 0x9E3779B185EBCA87)) & 0xFFFFFFFFFFFFFFFF

    if mode == MODE_ZERO:
        return ZERO_BLOCK
    if mode == MODE_REPEAT_A:
        pattern = struct.pack("<QQQQ", word0, word1, word2, word3)
        return pattern * (BLOCK_SIZE // len(pattern))
    if mode == MODE_REPEAT_B:
        pattern = struct.pack(
            "<QQQQQQQQ",
            word3,
            word2 ^ 0xAAAAAAAAAAAAAAAA,
            word1 ^ 0x5555555555555555,
            word0 ^ 0x3333333333333333,
            (word0 + word2) & 0xFFFFFFFFFFFFFFFF,
            (word1 + word3) & 0xFFFFFFFFFFFFFFFF,
            (word0 - word1) & 0xFFFFFFFFFFFFFFFF,
            (word2 - word3) & 0xFFFFFFFFFFFFFFFF,
        )
        return pattern * (BLOCK_SIZE // len(pattern))
    if mode == MODE_REPEAT_C:
        pattern = struct.pack(
            "<QQQQQQQQ",
            ((word0 << 7) | (word0 >> 57)) & 0xFFFFFFFFFFFFFFFF,
            ((word1 << 13) | (word1 >> 51)) & 0xFFFFFFFFFFFFFFFF,
            ((word2 << 29) | (word2 >> 35)) & 0xFFFFFFFFFFFFFFFF,
            ((word3 << 31) | (word3 >> 33)) & 0xFFFFFFFFFFFFFFFF,
            word0 ^ word2,
            word1 ^ word3,
            (word0 + word1 + word2) & 0xFFFFFFFFFFFFFFFF,
            (word3 + 0xD6E8FEB86659FD93) & 0xFFFFFFFFFFFFFFFF,
        )
        return pattern * (BLOCK_SIZE // len(pattern))
    raise HarnessError(f"unsupported mode {mode}")


@dataclass
class OperationRecord:
    timestamp: float
    worker: int
    kind: str
    offset: int
    length: int
    stamp: Optional[int]
    mode: Optional[str]
    detail: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "ts": self.timestamp,
            "worker": self.worker,
            "kind": self.kind,
            "offset": self.offset,
            "length": self.length,
            "stamp": self.stamp,
            "mode": self.mode,
            "detail": self.detail,
        }


class EventLog:
    def __init__(self, path: pathlib.Path, recent_max: int = 2048) -> None:
        self._fh = path.open("a", encoding="utf-8")
        self._lock = threading.Lock()
        self._recent: Deque[dict[str, object]] = collections.deque(maxlen=recent_max)

    def append(self, payload: dict[str, object]) -> None:
        line = json.dumps(payload, sort_keys=True)
        with self._lock:
            self._fh.write(line + "\n")
            self._fh.flush()
            self._recent.append(payload)

    def remember(self, payload: dict[str, object]) -> None:
        with self._lock:
            self._recent.append(payload)

    def recent(self) -> list[dict[str, object]]:
        with self._lock:
            return list(self._recent)

    def close(self) -> None:
        with self._lock:
            self._fh.close()


class BlockStateMap:
    def __init__(self, path: pathlib.Path, total_blocks: int) -> None:
        self.path = path
        self.total_blocks = total_blocks
        byte_len = total_blocks * STATE_BYTES
        self._fh = path.open("w+b")
        self._fh.truncate(byte_len)
        self._mm = mmap.mmap(self._fh.fileno(), byte_len, access=mmap.ACCESS_WRITE)

    def read_states(self, start_block: int, count: int) -> list[int]:
        base = start_block * STATE_BYTES
        data = self._mm[base : base + count * STATE_BYTES]
        return [
            struct.unpack_from("<Q", data, idx * STATE_BYTES)[0]
            for idx in range(count)
        ]

    def fill(self, start_block: int, count: int, state: int) -> None:
        base = start_block * STATE_BYTES
        packed = struct.pack("<Q", state)
        self._mm[base : base + count * STATE_BYTES] = packed * count

    def flush(self) -> None:
        self._mm.flush()
        self._fh.flush()

    def close(self) -> None:
        self._mm.flush()
        self._mm.close()
        self._fh.close()


class RangeLocks:
    def __init__(self, total_blocks: int, blocks_per_lock: int) -> None:
        count = max(1, (total_blocks + blocks_per_lock - 1) // blocks_per_lock)
        self.blocks_per_lock = blocks_per_lock
        self._locks = [threading.Lock() for _ in range(count)]

    @contextlib.contextmanager
    def hold(self, start_block: int, block_count: int) -> Iterable[None]:
        first = start_block // self.blocks_per_lock
        last = (start_block + block_count - 1) // self.blocks_per_lock
        locks = self._locks[first : last + 1]
        for lock in locks:
            lock.acquire()
        try:
            yield
        finally:
            for lock in reversed(locks):
                lock.release()


class PauseGate:
    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._paused = False
        self._active = 0

    @contextlib.contextmanager
    def enter(self) -> Iterable[None]:
        with self._cond:
            while self._paused:
                self._cond.wait()
            self._active += 1
        try:
            yield
        finally:
            with self._cond:
                self._active -= 1
                if self._paused and self._active == 0:
                    self._cond.notify_all()

    def pause_and_drain(self) -> None:
        with self._cond:
            self._paused = True
            while self._active > 0:
                self._cond.wait()

    def resume(self) -> None:
        with self._cond:
            self._paused = False
            self._cond.notify_all()


class DeviceHandle:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._fd: Optional[int] = None
        self.path: Optional[pathlib.Path] = None

    def replace(self, path: pathlib.Path) -> None:
        fd = os.open(path, os.O_RDWR)
        old_fd: Optional[int] = None
        with self._lock:
            old_fd = self._fd
            self._fd = fd
            self.path = path
        if old_fd is not None:
            os.close(old_fd)

    def close(self) -> None:
        with self._lock:
            fd = self._fd
            self._fd = None
        if fd is not None:
            os.close(fd)

    def read(self, offset: int, length: int) -> bytes:
        with self._lock:
            if self._fd is None:
                raise HarnessError("device not open")
            fd = self._fd
        return os.pread(fd, length, offset)

    def write(self, offset: int, data: bytes) -> int:
        with self._lock:
            if self._fd is None:
                raise HarnessError("device not open")
            fd = self._fd
        return os.pwrite(fd, data, offset)

    def fsync(self) -> None:
        with self._lock:
            if self._fd is None:
                raise HarnessError("device not open")
            fd = self._fd
        os.fsync(fd)


class ServiceManager:
    DEVICE_REMOVAL_WAIT_SECS = 20.0
    CLEANUP_UBLK_TIMEOUT_SECS = 5.0

    def __init__(
        self,
        *,
        repo_root: pathlib.Path,
        engine_cmd: list[str],
        config_path: pathlib.Path,
        socket_path: pathlib.Path,
        volume: str,
        env: dict[str, str],
        log_path: pathlib.Path,
        event_log: EventLog,
        startup_timeout_secs: int,
        startup_drain_timeout_secs: int,
    ) -> None:
        self.repo_root = repo_root
        self.engine_cmd = engine_cmd
        self.config_path = config_path
        self.socket_path = socket_path
        self.volume = volume
        self.env = env
        self.log_path = log_path
        self.event_log = event_log
        self.startup_timeout_secs = startup_timeout_secs
        self.startup_drain_timeout_secs = startup_drain_timeout_secs
        self.proc: Optional[subprocess.Popen[bytes]] = None
        self._log_fh = None

    def cli(self, *args: str) -> list[str]:
        return [*self.engine_cmd, "-c", str(self.config_path), *args]

    def cleanup_ublk_devices(self) -> None:
        proc = None
        try:
            proc = subprocess.Popen(
                self.cli("cleanup-ublk"),
                cwd=str(self.repo_root),
                env=self.env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            proc.wait(timeout=self.CLEANUP_UBLK_TIMEOUT_SECS)
            self.event_log.append(
                {
                    "event": "cleanup-ublk-finished",
                    "returncode": proc.returncode,
                    "ts": time.time(),
                }
            )
        except subprocess.TimeoutExpired:
            if proc is not None:
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()
                try:
                    proc.wait(timeout=1.0)
                except subprocess.TimeoutExpired:
                    self.event_log.append(
                        {
                            "event": "cleanup-ublk-timeout",
                            "pid": proc.pid,
                            "timeout_secs": self.CLEANUP_UBLK_TIMEOUT_SECS,
                            "ts": time.time(),
                        }
                    )
                    return
            self.event_log.append(
                {
                    "event": "cleanup-ublk-timeout",
                    "timeout_secs": self.CLEANUP_UBLK_TIMEOUT_SECS,
                    "ts": time.time(),
                }
            )
        except Exception:
            self.event_log.append(
                {
                    "event": "cleanup-ublk-error",
                    "ts": time.time(),
                }
            )

    def _status_json_or_none(self) -> Optional[dict[str, object]]:
        try:
            return self.status_json()
        except Exception:
            return None

    def _tracked_ublk_nodes(self) -> list[pathlib.Path]:
        payload = self._status_json_or_none() or {}
        dev_ids = payload.get("ublk_devices") or []
        nodes: list[pathlib.Path] = []
        for raw_dev_id in dev_ids:
            try:
                dev_id = int(raw_dev_id)
            except (TypeError, ValueError):
                continue
            nodes.append(pathlib.Path(f"/dev/ublkb{dev_id}"))
            nodes.append(pathlib.Path(f"/dev/ublkc{dev_id}"))
        return nodes

    def _wait_for_ublk_nodes_removed(self, nodes: list[pathlib.Path]) -> bool:
        if not nodes:
            return True
        deadline = time.time() + self.DEVICE_REMOVAL_WAIT_SECS
        while time.time() < deadline:
            if all(not node.exists() for node in nodes):
                return True
            time.sleep(0.5)
        return all(not node.exists() for node in nodes)

    def _send_socket_cmd(self, cmd: str) -> list[str]:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(2.0)
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
                    raise HarnessError(line.removeprefix("error:").strip())
                lines.append(line)
        return lines

    def status_json(self) -> dict[str, object]:
        lines = self._send_socket_cmd("status-json")
        if not lines:
            raise HarnessError("empty status-json response")
        return json.loads(lines[0])

    def wait_for_recovered_buffer_drain(self) -> None:
        if self.startup_drain_timeout_secs <= 0:
            return
        payload = self.status_json()
        status = payload.get("status") or {}
        pending = int(status.get("buffer_pending_entries") or 0)
        if pending <= 0:
            return

        started = time.time()
        self.event_log.append(
            {
                "event": "startup-drain-begin",
                "pending_entries": pending,
                "ts": started,
            }
        )

        def drained() -> bool:
            if self.proc is not None and self.proc.poll() is not None:
                raise HarnessError(f"engine exited early with code {self.proc.returncode}")
            status = (self.status_json().get("status") or {})
            return int(status.get("buffer_pending_entries") or 0) == 0

        wait_for(
            drained,
            timeout_secs=self.startup_drain_timeout_secs,
            interval_secs=1.0,
        )
        final_pending = int(
            (self.status_json().get("status") or {}).get("buffer_pending_entries") or 0
        )
        self.event_log.append(
            {
                "event": "startup-drain-end",
                "pending_entries": final_pending,
                "elapsed_secs": time.time() - started,
                "ts": time.time(),
            }
        )

    def best_effort_stop(self) -> None:
        tracked_nodes = self._tracked_ublk_nodes()
        try:
            run_cmd(self.cli("stop"), cwd=self.repo_root, env=self.env, check=False)
        except Exception:
            pass
        if self.proc is not None:
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
            self.proc = None
        deadline = time.time() + 30
        while time.time() < deadline:
            if not self.socket_path.exists():
                break
            try:
                self._send_socket_cmd("ping")
            except Exception:
                break
            time.sleep(0.5)
        if tracked_nodes and not self._wait_for_ublk_nodes_removed(tracked_nodes):
            self.event_log.append(
                {
                    "event": "ublk-nodes-still-present-after-stop",
                    "nodes": [str(node) for node in tracked_nodes if node.exists()],
                    "wait_secs": self.DEVICE_REMOVAL_WAIT_SECS,
                    "ts": time.time(),
                }
            )

    def recreate_volume(self, size_bytes: int, compression: str) -> None:
        run_cmd(self.cli("delete-volume", "-n", self.volume), cwd=self.repo_root, env=self.env, check=False)
        run_cmd(
            self.cli(
                "create-volume",
                "-n",
                self.volume,
                "-s",
                str(size_bytes),
                "--compression",
                compression,
            ),
            cwd=self.repo_root,
            env=self.env,
        )
        self.event_log.append(
            {
                "event": "volume-created",
                "volume": self.volume,
                "size_bytes": size_bytes,
                "compression": compression,
                "ts": time.time(),
            }
        )

    def start(self) -> pathlib.Path:
        if self._log_fh is None:
            self._log_fh = self.log_path.open("ab")
        self.proc = subprocess.Popen(
            self.cli("start", "-v", self.volume),
            cwd=str(self.repo_root),
            env=self.env,
            stdout=self._log_fh,
            stderr=subprocess.STDOUT,
        )
        self.event_log.append({"event": "service-start", "ts": time.time()})

        def ready() -> bool:
            if self.proc is not None and self.proc.poll() is not None:
                raise HarnessError(f"engine exited early with code {self.proc.returncode}")
            payload = self.status_json()
            dev_ids = payload.get("ublk_devices") or []
            return bool(dev_ids)

        wait_for(ready, timeout_secs=self.startup_timeout_secs, interval_secs=1.0)
        path = self.resolve_device_path()
        self.event_log.append({"event": "device-ready", "device": str(path), "ts": time.time()})
        self.wait_for_recovered_buffer_drain()
        return path

    def resolve_device_path(self) -> pathlib.Path:
        payload = self.status_json()
        dev_ids = payload.get("ublk_devices") or []
        if len(dev_ids) != 1:
            raise HarnessError(f"expected exactly one ublk device, got {dev_ids}")
        path = pathlib.Path(f"/dev/ublkb{int(dev_ids[0])}")
        wait_for(lambda: path.exists(), timeout_secs=30, interval_secs=0.5)
        return path

    def restart(self) -> pathlib.Path:
        self.event_log.append({"event": "service-restart-begin", "ts": time.time()})
        self.best_effort_stop()
        time.sleep(1.0)
        path = self.start()
        self.event_log.append({"event": "service-restart-end", "device": str(path), "ts": time.time()})
        return path

    def close(self) -> None:
        self.best_effort_stop()
        if self._log_fh is not None:
            self._log_fh.close()
            self._log_fh = None


@dataclass
class Stats:
    write_ops: int = 0
    read_ops: int = 0
    flush_ops: int = 0
    write_bytes: int = 0
    read_bytes: int = 0
    restarts: int = 0
    scrub_checks: int = 0
    mismatches: int = 0
    io_errors: int = 0


@dataclass
class SummaryCursor:
    timestamp: float
    stats: Stats


class StatsTracker:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stats = Stats()

    def add(self, **kwargs: int) -> None:
        with self._lock:
            for key, value in kwargs.items():
                setattr(self._stats, key, getattr(self._stats, key) + value)

    def snapshot(self) -> Stats:
        with self._lock:
            return Stats(**self._stats.__dict__)


class IntegrityHarness:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.seed = args.seed
        self.repo_root = pathlib.Path(args.repo_root).resolve()
        self.config_path = pathlib.Path(args.config).resolve()
        self.socket_path = load_socket_path(self.config_path, args.socket_path)
        self.run_dir = pathlib.Path(args.run_dir).resolve()
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.event_log = EventLog(self.run_dir / "events.jsonl")
        self.state_map = BlockStateMap(
            self.run_dir / "shadow-state.bin",
            args.volume_size // BLOCK_SIZE,
        )
        self.total_blocks = args.volume_size // BLOCK_SIZE
        self.range_locks = RangeLocks(self.total_blocks, args.blocks_per_lock)
        self.pause_gate = PauseGate()
        self.device = DeviceHandle()
        self.stop_event = threading.Event()
        self.normal_stop_requested = threading.Event()
        self.teardown_started = threading.Event()
        self.failure: Optional[str] = None
        self.failure_lock = threading.Lock()
        self.stats = StatsTracker()
        self.summary_cursor_lock = threading.Lock()
        self.summary_cursor: Optional[SummaryCursor] = None
        self.next_stamp = 1
        self.next_stamp_lock = threading.Lock()
        self.op_trace_counter = 0
        self.op_trace_lock = threading.Lock()
        self.hot_windows = self._make_hot_windows()
        self.engine_cmd = shlex.split(args.engine_cmd)
        if not self.engine_cmd:
            raise HarnessError("--engine-cmd cannot be empty")
        self.env = os.environ.copy()
        self.service = ServiceManager(
            repo_root=self.repo_root,
            engine_cmd=self.engine_cmd,
            config_path=self.config_path,
            socket_path=self.socket_path,
            volume=args.volume,
            env=self.env,
            log_path=self.run_dir / "engine.log",
            event_log=self.event_log,
            startup_timeout_secs=args.startup_timeout_secs,
            startup_drain_timeout_secs=args.startup_drain_timeout_secs,
        )

    def _should_ignore_io_exception(self, exc: Exception) -> bool:
        if not (self.normal_stop_requested.is_set() or self.teardown_started.is_set()):
            return False
        if isinstance(exc, HarnessError):
            text = str(exc)
            return (
                text == "device not open"
                or text.startswith("short read: expected ")
                or text.startswith("short write: expected ")
            )
        if isinstance(exc, OSError):
            return exc.errno in {
                errno.EBADF,
                errno.ENODEV,
                errno.ENXIO,
                errno.EIO,
            }
        return False

    def _handle_thread_exception(self, context: str, exc: Exception) -> None:
        if self._should_ignore_io_exception(exc):
            self.event_log.remember(
                {
                    "event": "ignored-shutdown-io-error",
                    "context": context,
                    "message": str(exc),
                    "ts": time.time(),
                }
            )
            return
        self.stats.add(io_errors=1)
        self._record_failure(f"{context}: {exc}")

    def _make_hot_windows(self) -> list[tuple[int, int]]:
        rng = random.Random(self.seed ^ 0x5EEDFACE)
        hot_blocks = max(1, int(self.total_blocks * self.args.hotset_ratio))
        hot_blocks = min(hot_blocks, self.total_blocks)
        window_count = min(self.args.hot_windows, max(1, self.total_blocks // hot_blocks))
        windows = []
        for _ in range(window_count):
            if self.total_blocks == hot_blocks:
                start = 0
            else:
                start = rng.randrange(0, self.total_blocks - hot_blocks)
            windows.append((start, hot_blocks))
        return windows or [(0, self.total_blocks)]

    def _new_stamp(self) -> int:
        with self.next_stamp_lock:
            stamp = self.next_stamp
            self.next_stamp += 1
            return stamp

    def _record_failure(self, message: str, extra: Optional[dict[str, object]] = None) -> None:
        with self.failure_lock:
            if self.failure is None:
                self.failure = message
                payload = {"event": "failure", "message": message, "ts": time.time()}
                if extra:
                    payload.update(extra)
                self.event_log.append(payload)
                self.stop_event.set()

    def _record_operation(self, payload: dict[str, object]) -> None:
        with self.op_trace_lock:
            self.op_trace_counter += 1
            should_persist = (
                self.args.operation_log_stride > 0
                and self.op_trace_counter % self.args.operation_log_stride == 0
            )
        if should_persist:
            self.event_log.append(payload)
        else:
            self.event_log.remember(payload)

    def _choose_block_span(self, rng: random.Random) -> tuple[int, int]:
        block_count = rng.choice(self.args.io_sizes_blocks)
        block_count = min(block_count, self.total_blocks)
        use_hot = rng.random() < self.args.hotset_probability
        if use_hot:
            hot_start, hot_blocks = rng.choice(self.hot_windows)
            if hot_blocks <= block_count:
                return hot_start, hot_blocks
            start = hot_start + rng.randrange(0, hot_blocks - block_count + 1)
            return start, block_count
        if self.total_blocks <= block_count:
            return 0, self.total_blocks
        return rng.randrange(0, self.total_blocks - block_count + 1), block_count

    def _compose_write_payload(
        self,
        start_block: int,
        block_count: int,
        state: int,
    ) -> bytes:
        out = bytearray(block_count * BLOCK_SIZE)
        for idx in range(block_count):
            block = build_block(self.seed, state, start_block + idx)
            start = idx * BLOCK_SIZE
            out[start : start + BLOCK_SIZE] = block
        return bytes(out)

    def _expected_read_bytes(self, start_block: int, count: int) -> bytes:
        states = self.state_map.read_states(start_block, count)
        out = bytearray(count * BLOCK_SIZE)
        for idx, state in enumerate(states):
            start = idx * BLOCK_SIZE
            out[start : start + BLOCK_SIZE] = build_block(self.seed, state, start_block + idx)
        return bytes(out)

    def _write_once(self, worker_id: int, rng: random.Random) -> None:
        start_block, block_count = self._choose_block_span(rng)
        offset = start_block * BLOCK_SIZE
        length = block_count * BLOCK_SIZE
        mode = rng.choices(
            [MODE_ZERO, MODE_REPEAT_A, MODE_REPEAT_B, MODE_REPEAT_C],
            weights=self.args.write_mode_weights,
            k=1,
        )[0]
        stamp = self._new_stamp()
        state = (stamp << 2) | mode

        with self.pause_gate.enter():
            with self.range_locks.hold(start_block, block_count):
                payload = self._compose_write_payload(start_block, block_count, state)
                written = self.device.write(offset, payload)
                if written != length:
                    raise HarnessError(f"short write: expected {length}, got {written}")
                self.state_map.fill(start_block, block_count, state)

        self.stats.add(write_ops=1, write_bytes=length)
        self._record_operation(
            OperationRecord(
                timestamp=time.time(),
                worker=worker_id,
                kind="write",
                offset=offset,
                length=length,
                stamp=stamp,
                mode=MODE_NAMES[mode],
            ).as_dict()
        )

    def _read_once(self, worker_id: int, rng: random.Random) -> None:
        start_block, block_count = self._choose_block_span(rng)
        offset = start_block * BLOCK_SIZE
        length = block_count * BLOCK_SIZE

        with self.pause_gate.enter():
            with self.range_locks.hold(start_block, block_count):
                expected = self._expected_read_bytes(start_block, block_count)
                actual = self.device.read(offset, length)

        if len(actual) != length:
            raise HarnessError(f"short read: expected {length}, got {len(actual)}")
        if actual != expected:
            self.stats.add(mismatches=1)
            mismatch_at = first_mismatch(expected, actual)
            block_idx = (mismatch_at // BLOCK_SIZE) if mismatch_at is not None else 0
            block_base = block_idx * BLOCK_SIZE
            block_lba = start_block + block_idx
            detail = {
                "offset": offset,
                "length": length,
                "expected_prefix": hexdump_prefix(expected),
                "actual_prefix": hexdump_prefix(actual),
                "first_mismatch_byte": mismatch_at,
                "first_bad_block_index": block_idx,
                "first_bad_block_lba": block_lba,
                "expected_block_prefix": hexdump_prefix(
                    expected[block_base : block_base + BLOCK_SIZE]
                ),
                "actual_block_prefix": hexdump_prefix(
                    actual[block_base : block_base + BLOCK_SIZE]
                ),
                "worker": worker_id,
            }
            self._record_failure("read mismatch", detail)
            return

        self.stats.add(read_ops=1, read_bytes=length)
        self._record_operation(
            OperationRecord(
                timestamp=time.time(),
                worker=worker_id,
                kind="read",
                offset=offset,
                length=length,
                stamp=None,
                mode=None,
            ).as_dict()
        )

    def _flush_once(self, worker_id: int) -> None:
        with self.pause_gate.enter():
            self.device.fsync()
        self.stats.add(flush_ops=1)
        self._record_operation(
            OperationRecord(
                timestamp=time.time(),
                worker=worker_id,
                kind="fsync",
                offset=0,
                length=0,
                stamp=None,
                mode=None,
            ).as_dict()
        )

    def worker_loop(self, worker_id: int) -> None:
        rng = random.Random(self.seed ^ (worker_id * 0x9E3779B1))
        while not self.stop_event.is_set():
            try:
                roll = rng.random()
                if roll < self.args.write_probability:
                    self._write_once(worker_id, rng)
                elif roll < self.args.write_probability + self.args.read_probability:
                    self._read_once(worker_id, rng)
                else:
                    self._flush_once(worker_id)
            except Exception as exc:
                self._handle_thread_exception(f"worker-{worker_id}", exc)
                return

    def scrub_sample_once(self, worker_id: int = -1) -> None:
        rng = random.Random(self.seed ^ int(time.time() * 1000))
        start_block, block_count = self._choose_block_span(rng)
        offset = start_block * BLOCK_SIZE
        length = block_count * BLOCK_SIZE

        with self.pause_gate.enter():
            with self.range_locks.hold(start_block, block_count):
                expected = self._expected_read_bytes(start_block, block_count)
                actual = self.device.read(offset, length)

        if actual != expected:
            self.stats.add(mismatches=1)
            mismatch_at = first_mismatch(expected, actual)
            block_idx = (mismatch_at // BLOCK_SIZE) if mismatch_at is not None else 0
            block_base = block_idx * BLOCK_SIZE
            block_lba = start_block + block_idx
            self._record_failure(
                "scrub mismatch",
                {
                    "offset": offset,
                    "length": length,
                    "expected_prefix": hexdump_prefix(expected),
                    "actual_prefix": hexdump_prefix(actual),
                    "first_mismatch_byte": mismatch_at,
                    "first_bad_block_index": block_idx,
                    "first_bad_block_lba": block_lba,
                    "expected_block_prefix": hexdump_prefix(
                        expected[block_base : block_base + BLOCK_SIZE]
                    ),
                    "actual_block_prefix": hexdump_prefix(
                        actual[block_base : block_base + BLOCK_SIZE]
                    ),
                    "worker": worker_id,
                },
            )
            return

        self.stats.add(scrub_checks=1)
        self._record_operation(
            OperationRecord(
                timestamp=time.time(),
                worker=worker_id,
                kind="sample-scrub",
                offset=offset,
                length=length,
                stamp=None,
                mode=None,
            ).as_dict()
        )

    def sample_scrubber_loop(self) -> None:
        while not self.stop_event.wait(self.args.sample_scrub_interval_secs):
            try:
                self.scrub_sample_once()
            except Exception as exc:
                self._handle_thread_exception("sample-scrubber", exc)
                return

    def reporter_loop(self, start_time: float) -> None:
        while not self.stop_event.wait(self.args.report_interval_secs):
            self.write_summary(start_time, final=False)

    def restarter_loop(self) -> None:
        if self.args.restart_interval_secs <= 0:
            return
        while not self.stop_event.wait(self.args.restart_interval_secs):
            try:
                self.pause_gate.pause_and_drain()
                self.state_map.flush()
                self.device.fsync()
                path = self.service.restart()
                self.device.replace(path)
                self.stats.add(restarts=1)
            except Exception as exc:
                self._handle_thread_exception("restarter", exc)
                return
            finally:
                self.pause_gate.resume()

    def verify_full_image(self) -> None:
        chunk_blocks = self.args.verify_chunk_bytes // BLOCK_SIZE
        if chunk_blocks <= 0:
            raise HarnessError("verify chunk must be at least one block")

        self.pause_gate.pause_and_drain()
        try:
            self.device.fsync()
            for start_block in range(0, self.total_blocks, chunk_blocks):
                block_count = min(chunk_blocks, self.total_blocks - start_block)
                offset = start_block * BLOCK_SIZE
                length = block_count * BLOCK_SIZE
                with self.range_locks.hold(start_block, block_count):
                    expected = self._expected_read_bytes(start_block, block_count)
                    actual = self.device.read(offset, length)
                if actual != expected:
                    self.stats.add(mismatches=1)
                    mismatch_at = first_mismatch(expected, actual)
                    block_idx = (mismatch_at // BLOCK_SIZE) if mismatch_at is not None else 0
                    block_base = block_idx * BLOCK_SIZE
                    block_lba = start_block + block_idx
                    self._record_failure(
                        "full scrub mismatch",
                        {
                            "offset": offset,
                            "length": length,
                            "expected_prefix": hexdump_prefix(expected),
                            "actual_prefix": hexdump_prefix(actual),
                            "first_mismatch_byte": mismatch_at,
                            "first_bad_block_index": block_idx,
                            "first_bad_block_lba": block_lba,
                            "expected_block_prefix": hexdump_prefix(
                                expected[block_base : block_base + BLOCK_SIZE]
                            ),
                            "actual_block_prefix": hexdump_prefix(
                                actual[block_base : block_base + BLOCK_SIZE]
                            ),
                        },
                    )
                    return
                if start_block % (chunk_blocks * 64) == 0:
                    self.event_log.append(
                        {
                            "event": "full-scrub-progress",
                            "offset_bytes": offset,
                            "checked_bytes": offset + length,
                            "ts": time.time(),
                        }
                    )
        finally:
            self.pause_gate.resume()

    def _build_summary(self, start_time: float, now: Optional[float] = None) -> dict[str, object]:
        now = time.time() if now is None else now
        stats = self.stats.snapshot()
        elapsed = max(1.0, now - start_time)
        avg_write_bw = format_bytes(stats.write_bytes / elapsed) + "/s"
        avg_read_bw = format_bytes(stats.read_bytes / elapsed) + "/s"

        with self.summary_cursor_lock:
            previous = self.summary_cursor
            if previous is None or now < previous.timestamp:
                recent_window_secs = elapsed
                recent_write_ops = stats.write_ops
                recent_read_ops = stats.read_ops
                recent_flush_ops = stats.flush_ops
                recent_restarts = stats.restarts
                recent_scrub_checks = stats.scrub_checks
                recent_mismatches = stats.mismatches
                recent_io_errors = stats.io_errors
                recent_write_bytes = stats.write_bytes
                recent_read_bytes = stats.read_bytes
            else:
                recent_window_secs = max(1.0, now - previous.timestamp)
                recent_write_ops = max(0, stats.write_ops - previous.stats.write_ops)
                recent_read_ops = max(0, stats.read_ops - previous.stats.read_ops)
                recent_flush_ops = max(0, stats.flush_ops - previous.stats.flush_ops)
                recent_restarts = max(0, stats.restarts - previous.stats.restarts)
                recent_scrub_checks = max(0, stats.scrub_checks - previous.stats.scrub_checks)
                recent_mismatches = max(0, stats.mismatches - previous.stats.mismatches)
                recent_io_errors = max(0, stats.io_errors - previous.stats.io_errors)
                recent_write_bytes = max(0, stats.write_bytes - previous.stats.write_bytes)
                recent_read_bytes = max(0, stats.read_bytes - previous.stats.read_bytes)
            self.summary_cursor = SummaryCursor(timestamp=now, stats=stats)

        summary = {
            "seed": self.seed,
            "elapsed_secs": elapsed,
            "elapsed_human": format_duration(elapsed),
            "write_ops": stats.write_ops,
            "read_ops": stats.read_ops,
            "flush_ops": stats.flush_ops,
            "restarts": stats.restarts,
            "scrub_checks": stats.scrub_checks,
            "mismatches": stats.mismatches,
            "io_errors": stats.io_errors,
            "write_bytes": stats.write_bytes,
            "read_bytes": stats.read_bytes,
            "write_bw": avg_write_bw,
            "read_bw": avg_read_bw,
            "write_bw_avg": avg_write_bw,
            "read_bw_avg": avg_read_bw,
            "recent_window_secs": recent_window_secs,
            "recent_write_ops": recent_write_ops,
            "recent_read_ops": recent_read_ops,
            "recent_flush_ops": recent_flush_ops,
            "recent_restarts": recent_restarts,
            "recent_scrub_checks": recent_scrub_checks,
            "recent_mismatches": recent_mismatches,
            "recent_io_errors": recent_io_errors,
            "recent_write_bytes": recent_write_bytes,
            "recent_read_bytes": recent_read_bytes,
            "write_bw_recent": format_bytes(recent_write_bytes / recent_window_secs) + "/s",
            "read_bw_recent": format_bytes(recent_read_bytes / recent_window_secs) + "/s",
            "failure": self.failure,
            "device": str(self.device.path) if self.device.path else None,
        }
        return summary

    def write_summary(self, start_time: float, final: bool) -> None:
        summary = self._build_summary(start_time)
        summary["final"] = final
        (self.run_dir / "summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        status = "FINAL" if final else "STAT"
        print(
            f"[{status}] elapsed={summary['elapsed_human']} "
            f"write_ops={summary['write_ops']} read_ops={summary['read_ops']} "
            f"write_bw_avg={summary['write_bw_avg']} write_bw_recent={summary['write_bw_recent']} "
            f"read_bw_avg={summary['read_bw_avg']} read_bw_recent={summary['read_bw_recent']} "
            f"restarts={summary['restarts']} scrubs={summary['scrub_checks']} "
            f"errors={summary['io_errors']} mismatches={summary['mismatches']}",
            flush=True,
        )

    def setup(self) -> None:
        self.service.best_effort_stop()
        self.service.cleanup_ublk_devices()
        self.service.recreate_volume(self.args.volume_size, self.args.compression)
        device_path = self.service.start()
        self.device.replace(device_path)
        size_cmd = run_cmd(
            ["blockdev", "--getsize64", str(device_path)],
            cwd=self.repo_root,
            env=self.env,
        )
        actual_size = int(size_cmd.stdout.strip())
        if actual_size != self.args.volume_size:
            raise HarnessError(
                f"volume size mismatch: expected {self.args.volume_size}, got {actual_size}"
            )
        self.event_log.append(
            {
                "event": "setup-complete",
                "device": str(device_path),
                "volume_size": self.args.volume_size,
                "ts": time.time(),
            }
        )

    def teardown(self) -> None:
        self.teardown_started.set()
        self.device.close()
        self.state_map.flush()
        self.state_map.close()
        self.service.close()
        self.event_log.close()

    def run(self) -> int:
        start_time = time.time()
        self.setup()

        threads = [
            threading.Thread(target=self.worker_loop, name=f"worker-{idx}", args=(idx,))
            for idx in range(self.args.workers)
        ]
        extra_threads = [
            threading.Thread(target=self.sample_scrubber_loop, name="sample-scrubber"),
            threading.Thread(target=self.reporter_loop, name="reporter", args=(start_time,)),
        ]
        if self.args.restart_interval_secs > 0:
            extra_threads.append(threading.Thread(target=self.restarter_loop, name="restarter"))

        for thread in [*threads, *extra_threads]:
            thread.daemon = True
            thread.start()

        end_time = start_time + self.args.duration_secs
        while time.time() < end_time and not self.stop_event.wait(1.0):
            pass

        if self.failure is None:
            self.normal_stop_requested.set()
        self.stop_event.set()
        for thread in threads:
            thread.join(timeout=10)
        for thread in extra_threads:
            thread.join(timeout=10)

        if self.failure is None:
            self.verify_full_image()

        if self.failure is None and self.args.final_restart_verify:
            self.pause_gate.pause_and_drain()
            try:
                self.state_map.flush()
                self.device.fsync()
                path = self.service.restart()
                self.device.replace(path)
                self.stats.add(restarts=1)
            finally:
                self.pause_gate.resume()
            self.verify_full_image()

        self.write_summary(start_time, final=True)

        if self.failure is not None:
            recent = self.event_log.recent()
            (self.run_dir / "recent-events.json").write_text(
                json.dumps(recent, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            return 1
        return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Long-running OS-level integrity soak harness for onyx-storage"
    )
    parser.add_argument("--config", required=True, help="Onyx config TOML")
    parser.add_argument(
        "--repo-root",
        default=".",
        help="Repository root used when launching the engine command",
    )
    parser.add_argument(
        "--engine-cmd",
        default="target/release/onyx-storage",
        help="Engine command, for example 'target/release/onyx-storage' or 'cargo run --release --bin onyx-storage --'",
    )
    parser.add_argument("--socket-path", help="Override service.socket_path from config")
    parser.add_argument("--run-dir", required=True, help="Directory for logs and shadow state")
    parser.add_argument("--volume", default="soak-volume", help="Volume name used by the test")
    parser.add_argument(
        "--volume-size",
        required=True,
        type=parse_size,
        help="Volume size in bytes, or with k/m/g/t suffix",
    )
    parser.add_argument(
        "--compression",
        default="lz4",
        choices=["none", "lz4", "zstd"],
        help="Compression used when creating the volume",
    )
    parser.add_argument(
        "--duration",
        default="48h",
        type=parse_duration,
        dest="duration_secs",
        help="Soak duration, for example 12h or 48h",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=max(4, (os.cpu_count() or 8) // 2),
        help="Concurrent worker threads",
    )
    parser.add_argument(
        "--io-sizes",
        default="4k,8k,16k,32k,64k,128k,256k,512k,1m",
        help="Comma-separated IO sizes; each must be 4KiB aligned",
    )
    parser.add_argument(
        "--blocks-per-lock",
        type=int,
        default=256,
        help="Lock stripe size in 4KiB blocks; default 256 = 1MiB",
    )
    parser.add_argument(
        "--hotset-ratio",
        type=float,
        default=0.10,
        help="Fraction of address space used by each hot window",
    )
    parser.add_argument(
        "--hotset-probability",
        type=float,
        default=0.70,
        help="Probability that an op targets a hot window",
    )
    parser.add_argument(
        "--hot-windows",
        type=int,
        default=4,
        help="How many hot windows to spread across the volume",
    )
    parser.add_argument(
        "--write-probability",
        type=float,
        default=0.60,
        help="Probability of a write operation",
    )
    parser.add_argument(
        "--read-probability",
        type=float,
        default=0.35,
        help="Probability of a read/verify operation",
    )
    parser.add_argument(
        "--sample-scrub-interval",
        type=parse_duration,
        default="60s",
        dest="sample_scrub_interval_secs",
        help="Random scrub cadence during the run",
    )
    parser.add_argument(
        "--restart-interval",
        type=parse_duration,
        default="2h",
        dest="restart_interval_secs",
        help="Graceful restart cadence; use 0 to disable",
    )
    parser.add_argument(
        "--report-interval",
        type=parse_duration,
        default="30s",
        dest="report_interval_secs",
        help="Stats report cadence",
    )
    parser.add_argument(
        "--verify-chunk",
        type=parse_size,
        default=parse_size("4m"),
        dest="verify_chunk_bytes",
        help="Chunk size for final full-image verification",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=int(time.time()),
        help="Deterministic seed for address and payload generation",
    )
    parser.add_argument(
        "--final-restart-verify",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Restart once more and scrub again after the soak",
    )
    parser.add_argument(
        "--startup-timeout",
        type=parse_duration,
        default="15m",
        dest="startup_timeout_secs",
        help="How long to wait for the engine socket and ublk device to become ready",
    )
    parser.add_argument(
        "--startup-drain-timeout",
        type=parse_duration,
        default="30m",
        dest="startup_drain_timeout_secs",
        help="How long to wait for recovered pending buffer entries to drain before starting workload; use 0 to disable",
    )
    parser.add_argument(
        "--operation-log-stride",
        type=int,
        default=1024,
        help="Persist one operation event every N ops; recent events stay in memory regardless",
    )

    args = parser.parse_args(argv)
    if args.volume_size % BLOCK_SIZE != 0:
        parser.error("--volume-size must be 4KiB aligned")
    if args.workers <= 0:
        parser.error("--workers must be > 0")
    if args.blocks_per_lock <= 0:
        parser.error("--blocks-per-lock must be > 0")
    if args.hot_windows <= 0:
        parser.error("--hot-windows must be > 0")
    if args.operation_log_stride < 0:
        parser.error("--operation-log-stride must be >= 0")
    if args.startup_timeout_secs <= 0:
        parser.error("--startup-timeout must be > 0")
    if args.startup_drain_timeout_secs < 0:
        parser.error("--startup-drain-timeout must be >= 0")
    if args.hotset_ratio <= 0 or args.hotset_ratio > 1.0:
        parser.error("--hotset-ratio must be in (0, 1]")
    if args.hotset_probability < 0 or args.hotset_probability > 1.0:
        parser.error("--hotset-probability must be in [0, 1]")
    if args.write_probability < 0 or args.read_probability < 0:
        parser.error("probabilities must be >= 0")
    total_probability = args.write_probability + args.read_probability
    if total_probability > 1.0:
        parser.error("write + read probability must be <= 1.0")

    io_sizes = []
    for raw in args.io_sizes.split(","):
        size = parse_size(raw.strip())
        if size <= 0 or size % BLOCK_SIZE != 0:
            parser.error("all --io-sizes entries must be positive and 4KiB aligned")
        io_sizes.append(size)
    args.io_sizes_blocks = [size // BLOCK_SIZE for size in sorted(set(io_sizes))]
    args.write_mode_weights = [10, 35, 25, 30]
    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    harness = IntegrityHarness(args)

    def handle_signal(signum: int, _frame) -> None:
        harness._record_failure(f"received signal {signum}")

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        return harness.run()
    except KeyboardInterrupt:
        harness._record_failure("keyboard interrupt")
        return 1
    except Exception as exc:
        message = f"fatal: {exc}"
        try:
            harness._record_failure(message)
            harness.write_summary(time.time(), final=True)
        except Exception:
            pass
        print(message, file=sys.stderr)
        return 1
    finally:
        with contextlib.suppress(Exception):
            harness.teardown()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
