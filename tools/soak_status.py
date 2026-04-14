#!/usr/bin/env python3
"""Summarize a soak run from summary.json, events.jsonl, and engine.log."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Iterable


ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
LOG_RE = re.compile(
    r"^(?P<ts>\S+)\s+(?P<level>TRACE|DEBUG|INFO|WARN|ERROR)\s+(?P<target>[\w:]+):\s+(?P<msg>.*)$"
)
ISSUE_HINT_RE = re.compile(
    r"(panic|error|failed|failure|mismatch|corrupt|crc|overlap|rejected|timeout|drift)",
    re.IGNORECASE,
)


def strip_ansi(text: str) -> str:
    return ANSI_RE.sub("", text)


def latest_run_dir(root: Path) -> Path:
    soak_root = root / ".dev" / "soak"
    candidates = sorted(
        p for p in soak_root.iterdir() if p.is_dir() and (p / "summary.json").exists()
    )
    if not candidates:
        raise FileNotFoundError(f"no soak runs found under {soak_root}")
    return candidates[-1]


def normalize_issue(message: str) -> str:
    text = re.sub(r"Pba\(\d+\)", "Pba(#)", message)
    text = re.sub(r"\b0x[0-9a-fA-F]+\b", "0x#", text)
    text = re.sub(r"\b\d+\b", "#", text)
    return text


def load_summary(run_dir: Path) -> dict:
    return json.loads((run_dir / "summary.json").read_text())


def load_events(run_dir: Path) -> list[dict]:
    events_path = run_dir / "events.jsonl"
    events = []
    if not events_path.exists():
        return events
    for raw in events_path.read_text().splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            events.append(json.loads(raw))
        except json.JSONDecodeError:
            continue
    return events


def iter_engine_lines(run_dir: Path) -> Iterable[str]:
    path = run_dir / "engine.log"
    if not path.exists():
        return []
    return [strip_ansi(line.rstrip("\n")) for line in path.read_text().splitlines()]


def extract_issues(lines: Iterable[str]) -> tuple[list[dict], Counter[str]]:
    extracted = []
    counts: Counter[str] = Counter()
    for line in lines:
        match = LOG_RE.match(line)
        if match:
            level = match.group("level")
            message = match.group("msg")
            target = match.group("target")
            if level in {"WARN", "ERROR"} or ISSUE_HINT_RE.search(message):
                item = {
                    "ts": match.group("ts"),
                    "level": level,
                    "target": target,
                    "message": message,
                    "raw": line,
                }
                extracted.append(item)
                counts[normalize_issue(message)] += 1
        elif ISSUE_HINT_RE.search(line):
            extracted.append({"ts": "", "level": "TEXT", "target": "", "message": line, "raw": line})
            counts[normalize_issue(line)] += 1
    return extracted, counts


def print_section(title: str) -> None:
    print(f"\n== {title} ==")


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize an Onyx soak run")
    parser.add_argument("run_dir", nargs="?", help="Path to a specific soak run directory")
    parser.add_argument(
        "--repo-root",
        default=".",
        help="Repo root used for --latest auto-discovery (default: current directory)",
    )
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Auto-select the latest run under .dev/soak",
    )
    parser.add_argument(
        "--issues",
        type=int,
        default=12,
        help="How many recent issue lines to print from engine.log (default: 12)",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=8,
        help="How many normalized issue groups to print (default: 8)",
    )
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    run_dir = Path(args.run_dir).resolve() if args.run_dir else None
    if args.latest or run_dir is None:
        run_dir = latest_run_dir(repo_root)

    summary = load_summary(run_dir)
    events = load_events(run_dir)
    engine_lines = list(iter_engine_lines(run_dir))
    issues, issue_counts = extract_issues(engine_lines)
    failure_event = next((e for e in reversed(events) if e.get("event") == "failure"), None)

    print(f"run_dir: {run_dir}")
    print(f"device:  {summary.get('device', '-')}")
    print(f"elapsed: {summary.get('elapsed_human', summary.get('elapsed_secs', '-'))}")
    print(f"final:   {summary.get('final', False)}")
    print(f"failure: {summary.get('failure', '-')}")
    print(
        "bw:      "
        f"write_avg={summary.get('write_bw_avg', summary.get('write_bw', '-'))} "
        f"read_avg={summary.get('read_bw_avg', summary.get('read_bw', '-'))}"
    )
    if "write_bw_recent" in summary or "read_bw_recent" in summary:
        window_secs = summary.get("recent_window_secs")
        if isinstance(window_secs, (int, float)):
            window_text = f"{window_secs:.1f}s"
        else:
            window_text = "-"
        print(
            "recent:  "
            f"window={window_text} "
            f"write={summary.get('write_bw_recent', '-')} "
            f"read={summary.get('read_bw_recent', '-')}"
        )
    print(
        "ops:     "
        f"writes={summary.get('write_ops', 0)} reads={summary.get('read_ops', 0)} "
        f"scrubs={summary.get('scrub_checks', 0)} mismatches={summary.get('mismatches', 0)} "
        f"io_errors={summary.get('io_errors', 0)}"
    )

    if failure_event:
        print_section("Failure Event")
        interesting = [
            "message",
            "first_bad_block_lba",
            "first_bad_block_index",
            "first_mismatch_byte",
            "offset",
            "length",
        ]
        for key in interesting:
            if key in failure_event:
                print(f"{key}: {failure_event[key]}")

    if issue_counts:
        print_section("Top Engine Issues")
        for message, count in issue_counts.most_common(args.top):
            print(f"{count:>4}  {message}")

    if issues:
        print_section("Recent Engine Issues")
        for item in issues[-args.issues :]:
            prefix = f"{item['ts']} {item['level']} {item['target']}".strip()
            print(f"{prefix}: {item['message']}")

    signals = {
        "overlap": sum("overlap" in item["message"].lower() for item in issues),
        "crc_mismatch": sum("crc mismatch" in item["message"].lower() for item in issues),
        "hole_fill_rejected": sum(
            "hole fill rejected" in item["message"].lower() for item in issues
        ),
        "refcount_drift": sum("refcount drift" in item["message"].lower() for item in issues),
    }
    if any(signals.values()):
        print_section("Signals")
        for key, value in signals.items():
            print(f"{key}: {value}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
