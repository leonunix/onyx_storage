#!/usr/bin/env python3
"""Inspect Onyx RocksDB blockmap/refcount metadata via ldb."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tomllib
from collections import defaultdict
from pathlib import Path


BLOCK_SIZE = 4096
RESERVED_BLOCKS = 8
CF_BLOCKMAP = "blockmap"
CF_REFCOUNT = "refcount"


def die(message: str) -> int:
    print(message, file=sys.stderr)
    return 1


def compression_name(value: int) -> str:
    return {
        0: "none",
        1: "lz4",
        2: "zstd",
    }.get(value, f"unknown({value})")


def resolve_db_path(db: str | None, config: str | None) -> Path:
    if db:
        return Path(db).resolve()
    if not config:
        raise ValueError("either --db or --config is required")
    config_path = Path(config).resolve()
    data = tomllib.loads(config_path.read_text())
    rocksdb_path = data.get("meta", {}).get("rocksdb_path")
    if not rocksdb_path:
        raise ValueError(f"meta.rocksdb_path missing in {config_path}")
    candidate = Path(rocksdb_path)
    if not candidate.is_absolute():
        candidate = (config_path.parent / candidate).resolve()
    return candidate


def run_ldb(
    db_path: Path,
    command: list[str],
    *,
    column_family: str | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    argv = ["ldb", f"--db={db_path}"]
    if column_family:
        argv.append(f"--column_family={column_family}")
    argv.extend(command)
    proc = subprocess.run(argv, capture_output=True, text=True)
    if check and proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "ldb failed")
    return proc


def ldb_get_hex(db_path: Path, column_family: str, key: bytes) -> bytes | None:
    proc = run_ldb(
        db_path,
        ["get", "--hex", f"0x{key.hex()}"],
        column_family=column_family,
        check=False,
    )
    if proc.returncode != 0:
        stderr = proc.stderr.strip()
        if "NotFound" in stderr:
            return None
        raise RuntimeError(stderr or proc.stdout.strip() or "ldb get failed")
    output = proc.stdout.strip()
    if not output:
        return None
    if not output.startswith("0x"):
        raise RuntimeError(f"unexpected ldb get output: {output}")
    return bytes.fromhex(output[2:])


def ldb_scan_hex(
    db_path: Path,
    column_family: str,
    *,
    from_key: bytes | None = None,
    to_key: bytes | None = None,
    max_keys: int | None = None,
) -> list[tuple[bytes, bytes]]:
    cmd = ["scan", "--hex"]
    if from_key is not None:
        cmd.append(f"--from=0x{from_key.hex()}")
    if to_key is not None:
        cmd.append(f"--to=0x{to_key.hex()}")
    if max_keys is not None:
        cmd.append(f"--max_keys={max_keys}")
    proc = run_ldb(db_path, cmd, column_family=column_family)
    rows: list[tuple[bytes, bytes]] = []
    for raw in proc.stdout.splitlines():
        raw = raw.strip()
        if not raw:
            continue
        if " ==> " not in raw:
            continue
        key_hex, value_hex = raw.split(" ==> ", 1)
        if not key_hex.startswith("0x") or not value_hex.startswith("0x"):
            continue
        rows.append((bytes.fromhex(key_hex[2:]), bytes.fromhex(value_hex[2:])))
    return rows


def encode_blockmap_key(volume: str, lba: int) -> bytes:
    vol_bytes = volume.encode("utf-8")
    if not 1 <= len(vol_bytes) <= 255:
        raise ValueError(f"volume length must be 1..255 bytes, got {len(vol_bytes)}")
    return bytes([len(vol_bytes)]) + vol_bytes + int(lba).to_bytes(8, "big")


def blockmap_prefix(volume: str) -> bytes:
    vol_bytes = volume.encode("utf-8")
    if not 1 <= len(vol_bytes) <= 255:
        raise ValueError(f"volume length must be 1..255 bytes, got {len(vol_bytes)}")
    return bytes([len(vol_bytes)]) + vol_bytes


def decode_blockmap_key(key: bytes) -> tuple[str, int]:
    if len(key) < 9:
        raise ValueError(f"invalid blockmap key length: {len(key)}")
    vol_len = key[0]
    if len(key) != 1 + vol_len + 8:
        raise ValueError(f"invalid blockmap key length: {len(key)}")
    volume = key[1 : 1 + vol_len].decode("utf-8")
    lba = int.from_bytes(key[1 + vol_len :], "big")
    return volume, lba


def decode_blockmap_value(value: bytes) -> dict:
    if len(value) != 28:
        raise ValueError(f"invalid blockmap value length: {len(value)}")
    pba = int.from_bytes(value[0:8], "big")
    compression = value[8]
    unit_compressed_size = int.from_bytes(value[9:13], "big")
    unit_original_size = int.from_bytes(value[13:17], "big")
    unit_lba_count = int.from_bytes(value[17:19], "big")
    offset_in_unit = int.from_bytes(value[19:21], "big")
    crc32 = int.from_bytes(value[21:25], "big")
    slot_offset = int.from_bytes(value[25:27], "big")
    flags = value[27]
    return {
        "pba": pba,
        "compression": compression,
        "compression_name": compression_name(compression),
        "unit_compressed_size": unit_compressed_size,
        "unit_original_size": unit_original_size,
        "unit_lba_count": unit_lba_count,
        "offset_in_unit": offset_in_unit,
        "crc32": f"0x{crc32:08x}",
        "slot_offset": slot_offset,
        "flags": flags,
        "flags_hex": f"0x{flags:02x}",
        "lv3_slot_offset_bytes": (pba + RESERVED_BLOCKS) * BLOCK_SIZE,
        "lv3_fragment_offset_bytes": (pba + RESERVED_BLOCKS) * BLOCK_SIZE + slot_offset,
        "packed": unit_compressed_size < BLOCK_SIZE,
    }


def encode_refcount_key(pba: int) -> bytes:
    return int(pba).to_bytes(8, "big")


def decode_refcount_value(value: bytes | None) -> int:
    if not value:
        return 0
    if len(value) != 4:
        raise ValueError(f"invalid refcount value length: {len(value)}")
    return int.from_bytes(value, "big")


def collect_fragments_for_pba(db_path: Path, pba: int) -> list[dict]:
    rows = ldb_scan_hex(db_path, CF_BLOCKMAP)
    fragments: list[dict] = []
    for key, value in rows:
        volume, lba = decode_blockmap_key(key)
        mapping = decode_blockmap_value(value)
        if mapping["pba"] != pba:
            continue
        fragments.append(
            {
                "volume": volume,
                "lba": lba,
                **mapping,
            }
        )
    fragments.sort(key=lambda item: (item["slot_offset"], item["volume"], item["lba"]))
    return fragments


def unique_fragment_groups(fragments: list[dict]) -> list[dict]:
    groups: dict[tuple, dict] = {}
    for frag in fragments:
        key = (
            frag["slot_offset"],
            frag["unit_compressed_size"],
            frag["unit_original_size"],
            frag["unit_lba_count"],
            frag["compression"],
            frag["crc32"],
            frag["flags"],
        )
        group = groups.setdefault(
            key,
            {
                "slot_offset": frag["slot_offset"],
                "unit_compressed_size": frag["unit_compressed_size"],
                "unit_original_size": frag["unit_original_size"],
                "unit_lba_count": frag["unit_lba_count"],
                "compression": frag["compression"],
                "compression_name": frag["compression_name"],
                "crc32": frag["crc32"],
                "flags": frag["flags"],
                "refs": [],
            },
        )
        group["refs"].append(f"{frag['volume']}:{frag['lba']}")
    out = list(groups.values())
    out.sort(key=lambda item: (item["slot_offset"], item["unit_compressed_size"], item["crc32"]))
    return out


def find_overlaps(groups: list[dict]) -> list[dict]:
    overlaps = []
    for i in range(len(groups)):
        left = groups[i]
        left_start = left["slot_offset"]
        left_end = left_start + left["unit_compressed_size"]
        for right in groups[i + 1 :]:
            right_start = right["slot_offset"]
            right_end = right_start + right["unit_compressed_size"]
            if left_start < right_end and right_start < left_end:
                overlaps.append(
                    {
                        "left": {
                            "range": [left_start, left_end],
                            "crc32": left["crc32"],
                            "refs": left["refs"],
                        },
                        "right": {
                            "range": [right_start, right_end],
                            "crc32": right["crc32"],
                            "refs": right["refs"],
                        },
                    }
                )
    return overlaps


def cmd_lba(args: argparse.Namespace) -> int:
    db_path = resolve_db_path(args.db, args.config)
    key = encode_blockmap_key(args.volume, args.lba)
    value = ldb_get_hex(db_path, CF_BLOCKMAP, key)
    result = {
        "db_path": str(db_path),
        "volume": args.volume,
        "lba": args.lba,
        "found": value is not None,
    }
    if value is not None:
        result["mapping"] = decode_blockmap_value(value)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def cmd_pba(args: argparse.Namespace) -> int:
    db_path = resolve_db_path(args.db, args.config)
    refcount = decode_refcount_value(
        ldb_get_hex(db_path, CF_REFCOUNT, encode_refcount_key(args.pba))
    )
    fragments = collect_fragments_for_pba(db_path, args.pba)
    groups = unique_fragment_groups(fragments)
    result = {
        "db_path": str(db_path),
        "pba": args.pba,
        "refcount": refcount,
        "blockmap_refs": len(fragments),
        "unique_fragments": groups,
        "overlaps": find_overlaps(groups),
    }
    if args.limit is not None:
        result["fragments"] = fragments[: args.limit]
        result["fragments_truncated"] = max(0, len(fragments) - args.limit)
    else:
        result["fragments"] = fragments
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def cmd_range(args: argparse.Namespace) -> int:
    db_path = resolve_db_path(args.db, args.config)
    prefix = blockmap_prefix(args.volume)
    rows = ldb_scan_hex(db_path, CF_BLOCKMAP, from_key=prefix)
    result_rows = []
    end_lba = args.start + args.count
    for key, value in rows:
        if not key.startswith(prefix):
            break
        volume, lba = decode_blockmap_key(key)
        if volume != args.volume:
            break
        if lba < args.start:
            continue
        if lba >= end_lba:
            break
        result_rows.append(
            {
                "volume": volume,
                "lba": lba,
                "mapping": decode_blockmap_value(value),
            }
        )
    print(
        json.dumps(
            {
                "db_path": str(db_path),
                "volume": args.volume,
                "start_lba": args.start,
                "count": args.count,
                "rows": result_rows,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect Onyx blockmap/refcount metadata")
    parser.add_argument("--db", help="Path to RocksDB directory")
    parser.add_argument("--config", help="Path to config TOML (uses meta.rocksdb_path)")

    subparsers = parser.add_subparsers(dest="command", required=True)

    p_lba = subparsers.add_parser("lba", help="Lookup one volume+lba blockmap entry")
    p_lba.add_argument("--volume", required=True, help="Volume name")
    p_lba.add_argument("--lba", required=True, type=int, help="Logical block address")
    p_lba.set_defaults(func=cmd_lba)

    p_pba = subparsers.add_parser("pba", help="Inspect one PBA: refcount + live fragments")
    p_pba.add_argument("--pba", required=True, type=int, help="Physical block address")
    p_pba.add_argument(
        "--limit",
        type=int,
        default=32,
        help="Limit detailed fragment rows in output (default: 32, use -1 for all)",
    )
    p_pba.set_defaults(func=cmd_pba)

    p_range = subparsers.add_parser("range", help="Scan a volume LBA range from blockmap")
    p_range.add_argument("--volume", required=True, help="Volume name")
    p_range.add_argument("--start", required=True, type=int, help="Start LBA")
    p_range.add_argument("--count", required=True, type=int, help="Number of LBAs")
    p_range.set_defaults(func=cmd_range)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "pba" and args.limit is not None and args.limit < 0:
        args.limit = None
    try:
        return args.func(args)
    except Exception as exc:  # noqa: BLE001
        return die(str(exc))


if __name__ == "__main__":
    sys.exit(main())
