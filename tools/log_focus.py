#!/usr/bin/env python3
"""Print matching log lines with context and ANSI stripping."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def strip_ansi(text: str) -> str:
    return ANSI_RE.sub("", text)


def main() -> int:
    parser = argparse.ArgumentParser(description="Show focused log context around a regex")
    parser.add_argument("path", help="Path to the log file")
    parser.add_argument("pattern", help="Regex pattern to search")
    parser.add_argument("-B", "--before", type=int, default=3, help="Lines of context before")
    parser.add_argument("-A", "--after", type=int, default=3, help="Lines of context after")
    parser.add_argument("-i", "--ignore-case", action="store_true", help="Case-insensitive regex")
    args = parser.parse_args()

    path = Path(args.path)
    lines = [strip_ansi(line.rstrip("\n")) for line in path.read_text().splitlines()]
    flags = re.IGNORECASE if args.ignore_case else 0
    pattern = re.compile(args.pattern, flags)

    matches = [idx for idx, line in enumerate(lines) if pattern.search(line)]
    if not matches:
        print("no matches")
        return 1

    emitted_until = -1
    for idx in matches:
        start = max(0, idx - args.before)
        end = min(len(lines), idx + args.after + 1)
        if start <= emitted_until:
            start = emitted_until + 1
        if start >= end:
            continue
        print(f"\n== match at line {idx + 1} ==")
        for lineno in range(start, end):
            marker = ">" if lineno == idx else " "
            print(f"{marker} {lineno + 1:6} {lines[lineno]}")
        emitted_until = end - 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
