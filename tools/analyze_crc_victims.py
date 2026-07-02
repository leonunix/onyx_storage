#!/usr/bin/env python3
"""Classify replay-CRC victims from an engine log with forensics enabled.

Joins three WARN line families emitted at each CRC site:
  - "CRC mismatch"                      (victim: mapping pba + expected/actual crc)
  - "last LV3 write record for covered PBA"  (who last wrote the physical block)
  - "PBA lifecycle trail"               (allocator alloc/free/retire/reclaim + reserved bits)

Verdict per victim mapping:
  REUSED_AFTER_FREE   trail shows free/reclaim>0 or alloc_count>1  -> onyx-side
                      premature free / double alloc (theory B).
  MAPPING_WRONG       trail clean (<=1 alloc, 0 free) AND the last write record's
                      payload_crc == actual_crc: the block holds exactly what its
                      last writer wrote — the *mapping* expects someone else's
                      data -> metadb read-side / L2P corruption (theory A).
  EXPECT_OK_DATA_BAD  record.payload_crc == mapping_crc but actual differs:
                      the mapping and its writer agree, the device returned
                      something else -> physical/offset bug (chunklet layer).
  NO_WRITE_RECORD     no post-restart write touched the block (pre-crash data)
                      and trail is clean; needs pre-crash context.
  UNCLASSIFIED        anything else.

Usage: analyze_crc_victims.py <engine.log> [more.log ...]
"""
import re
import sys
from collections import defaultdict

ANSI = re.compile(r"\x1b\[[0-9;]*m")


def fields(line):
    return {k: v for k, v in re.findall(r"(\w+)=([^\s]+)", line)}


def main(paths):
    victims = {}          # mapping_pba -> {expected, actual, purpose, line}
    records = {}          # covered_pba -> fields
    trails = {}           # covered_pba -> trail string
    covered_of = defaultdict(set)  # mapping_pba -> {covered pbas}

    for path in paths:
        with open(path, errors="replace") as fh:
            for raw in fh:
                line = ANSI.sub("", raw)
                if "CRC mismatch" in line and "verify mismatch" not in line:
                    f = fields(line)
                    if "pba" in f:
                        victims.setdefault(int(f["pba"]), {
                            "expected": f.get("expected_crc"),
                            "actual": f.get("actual_crc"),
                            "purpose": f.get("purpose", "?"),
                            "line": line.strip()[:220],
                        })
                elif "last LV3 write record" in line:
                    f = fields(line)
                    if "pba" in f:
                        pba = int(f["pba"])
                        records[pba] = f
                        if "mapping_start_pba" in f:
                            covered_of[int(f["mapping_start_pba"])].add(pba)
                elif "PBA lifecycle trail" in line:
                    f = fields(line)
                    m = re.search(r'trail="?([^"]+?)"?\s*$', line.strip())
                    if "pba" in f:
                        pba = int(f["pba"])
                        trails[pba] = m.group(1) if m else line.strip()
                        if "mapping_start_pba" in f:
                            covered_of[int(f["mapping_start_pba"])].add(pba)

    def trail_stats(t):
        # "alloc#2@g123(ctx) free#1@g99(free_extent) retire#0 reclaim#0 | reserved=bm:Y,dd:N"
        g = lambda name: int(re.search(rf"{name}#(\d+)", t).group(1)) if re.search(rf"{name}#(\d+)", t) else 0
        res = re.search(r"reserved=bm:(\w),dd:(\w)", t)
        return {
            "alloc": g("alloc"), "free": g("free"),
            "retire": g("retire"), "reclaim": g("reclaim"),
            "bm": res.group(1) if res else "?", "dd": res.group(2) if res else "?",
        }

    verdicts = defaultdict(list)
    for vpba, v in sorted(victims.items()):
        cov = sorted(covered_of.get(vpba, {vpba}))
        vt = [trail_stats(trails[p]) for p in cov if p in trails]
        vr = [records[p] for p in cov if p in records]
        reused = any(t["free"] > 0 or t["reclaim"] > 0 or t["alloc"] > 1 for t in vt)
        clean = vt and all(t["free"] == 0 and t["reclaim"] == 0 and t["alloc"] <= 1 for t in vt)
        actual = v.get("actual")
        rec_match_actual = any(r.get("write_payload_crc") == actual or r.get("write_padded_crc") == actual for r in vr)
        rec_match_expect = any(r.get("write_payload_crc") == v.get("expected") for r in vr)
        if reused:
            verdict = "REUSED_AFTER_FREE"
        elif clean and vr and rec_match_actual and not rec_match_expect:
            verdict = "MAPPING_WRONG"
        elif vr and rec_match_expect and not rec_match_actual:
            verdict = "EXPECT_OK_DATA_BAD"
        elif clean and not vr:
            verdict = "NO_WRITE_RECORD"
        else:
            verdict = "UNCLASSIFIED"
        verdicts[verdict].append((vpba, v, vt, vr))

    total = sum(len(v) for v in verdicts.values())
    print(f"victim mappings: {total}\n")
    for verdict in ("REUSED_AFTER_FREE", "MAPPING_WRONG", "EXPECT_OK_DATA_BAD",
                    "NO_WRITE_RECORD", "UNCLASSIFIED"):
        rows = verdicts.get(verdict, [])
        if not rows:
            continue
        print(f"== {verdict}: {len(rows)} ==")
        for vpba, v, vt, vr in rows[:5]:
            print(f"  pba={vpba} purpose={v['purpose']} expect={v['expected']} actual={v['actual']}")
            for t in vt[:2]:
                print(f"    trail: alloc={t['alloc']} free={t['free']} retire={t['retire']} "
                      f"reclaim={t['reclaim']} reserved=bm:{t['bm']},dd:{t['dd']}")
            for r in vr[:2]:
                print(f"    record: seq={r.get('write_seq')} start={r.get('write_start_pba')} "
                      f"payload_crc={r.get('write_payload_crc')} padded_crc={r.get('write_padded_crc')}")
        if len(rows) > 5:
            print(f"  ... +{len(rows) - 5} more")
        print()

    # PBA clustering (the c3 signature was 9M-13.5M).
    if victims:
        pbas = sorted(victims)
        print(f"pba range: {pbas[0]} .. {pbas[-1]}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    main(sys.argv[1:])
