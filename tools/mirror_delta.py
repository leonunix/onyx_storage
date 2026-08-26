#!/usr/bin/env python3
"""Phase ledger for chunklet's MIRROR (RAID10) batched write, from engine.log.

    mirror_delta.py <engine.log> [more.log ...]

WHY. onyx's LV2 ring and the metadb page window both live on RAID10 LDs, and
`buffer_lv2_entry_write_latency` — the single largest term in the LV2 durability
ledger ([[lv2_write_lane_cap_is_the_append_wall]]) — is exactly one
`root_device.write_many_at(&ops)` call into `LdMirror::write_many_at`.

That function already emits a full phase split, but only as a `tracing::warn!`
when the call takes >= 5 ms ("slow mirror write_many"), and there is no
cumulative counter for it. So this parses the warns. ⚠ THAT MEANS THIS IS THE
TAIL, NOT THE MEAN: everything here is conditioned on >= 5 ms. Box 2026-08-26
those events were 5.7% of total lane time. The tail is still what feeds the
append's p99, which is why it is worth a ledger.

BASELINE (box 2026-08-26, RWMIX=0 QD256 j16d16, aged 256 GiB volume, one arm,
91 795 events, `parallel_l2p_drain_workers=4`):

    field            mean us    p50      p99     share
    submit            5223     4593    16145    58.6%
    stripe_wait       1736       80    15824    19.5%
    coalesce          1534      930     9207    17.2%
    plan               123       97      636     1.4%
    absorb               3        2       12     0.0%
    total             8921     7315    26922

    ops p50 256   bytes p50 1 MiB   strip_writes p50 512

⭐ 512 strip writes for a 1 MiB logical write = 256 4-KiB segments x 2 mirror
copies, because LV2's RAID10 LD is configured `strip_kib = 0` = one 4 KiB block
per strip (`ChunkletLdGeom::lv2_default`), against `strip_kib = 256` for LV3.
The LV2 ring is a strictly SEQUENTIAL append log, so this is the same 4-KiB
fan-out shape as [[submit_io_is_a_563_way_4k_fanout]] on LV3 — where both
submit-shape knobs were falsified at 0%, and where
[[nvme_box_write_bandwidth_calibration]] explicitly retracted "4 KiB strip caps
payload via command rate". So `submit` being 58.6% is per-IO SOFTWARE cost, not
device command rate. ⚠ Changing `strip_kib` needs `chunklet-init --force` = a
fresh pool = re-fill and re-age before anything can be measured.

Two cheaper targets, both with an already-proven mechanism:

- `coalesce` used `AlignedBuf::new` (zero-filling) for a buffer it overwrites
  byte for byte. 3.4 GB/s effective for a plain copy is what a zero-fill plus
  first-touch faults costs. Fixed by switching to `AlignedBuf::uninit`, whose
  doc comment describes this exact case; compare the `coalesce` row before and
  after. Related: [[lv3_slab_arena_landed]] measured the redundant `fill(0)`
  alone at 18% on onyx's LV3 buffers.
- `stripe_wait` is the lock FOOTPRINT, not sharing: ~512 keys of 65536 buckets
  makes two concurrent calls disjoint with probability exp(-512^2/65536) = 1.8%,
  the same arithmetic that gave RAID6 a 126x blowup before grouping
  ([[stripe_lock_footprint_scales_with_call_size]]). Mirror is excluded from
  grouping on the grounds that "a ring log writes ADJACENT keys from independent
  callers" (`chunklet/src/pool/mod.rs::lock_group_shift_for`) — but each LV2
  shard owns a DISJOINT 768 MiB slice of the LD and, since onyx `aaf9bd0`, one
  lane per shard, so the independent callers are 768 MiB apart while a
  `group_shift = 10` group spans 4 MiB. That rationale no longer describes this
  workload.
"""
import re
import statistics
import sys

FIELDS = [
    "ops", "effective_ops", "strip_writes", "bytes",
    "coalesce_us", "plan_us", "stripe_wait_us", "submit_us", "absorb_us", "total_us",
]
ANSI = re.compile(r"\x1b\[[0-9;]*m")
PATS = {f: re.compile(r"\b" + f + r"=(\d+)") for f in FIELDS}
TS = re.compile(r"^(\d{4}-\d\d-\d\dT[\d:.]+)")


def main(argv):
    """`--from`/`--to` take the ISO timestamp prefix the log itself carries
    (e.g. `--from 2026-08-26T06:15`). They exist so a knob that is
    RUNTIME-flippable can be A/B'd inside ONE process at ONE pool age, which is
    the only way to escape this box's restart-order drift."""
    paths, lo, hi = [], None, None
    i = 0
    while i < len(argv):
        if argv[i] == "--from":
            lo = argv[i + 1]; i += 2
        elif argv[i] == "--to":
            hi = argv[i + 1]; i += 2
        else:
            paths.append(argv[i]); i += 1
    if not paths:
        print(__doc__)
        return 1
    acc = {f: [] for f in FIELDS}
    n = 0
    for path in paths:
        for raw in open(path, errors="replace"):
            if "slow mirror write_many" not in raw:
                continue
            line = ANSI.sub("", raw)
            if lo is not None or hi is not None:
                m = TS.match(line)
                if not m:
                    continue
                stamp = m.group(1)
                if lo is not None and stamp < lo:
                    continue
                if hi is not None and stamp >= hi:
                    continue
            row = {}
            for f, pat in PATS.items():
                m = pat.search(line)
                if m:
                    row[f] = int(m.group(1))
            if len(row) != len(FIELDS):
                continue
            n += 1
            for f in FIELDS:
                acc[f].append(row[f])
    if n == 0:
        print("no `slow mirror write_many` lines in range. Either the run was "
              "healthy (no call reached the 5 ms warn threshold), the log is not "
              "from RUST_LOG=info, or --from/--to excluded everything.")
        return 1

    total = sum(acc["total_us"])
    if lo is not None or hi is not None:
        print("window: %s .. %s" % (lo or "-inf", hi or "+inf"))
    print("slow mirror write_many events: %d   (>= 5 ms only -- this is the TAIL)" % n)
    print("aggregate wall in them: %.1f s   bytes: %.1f GiB" % (
        total / 1e6, sum(acc["bytes"]) / 2**30))
    print()
    print("%-16s %10s %10s %10s %8s" % ("field", "mean", "p50", "p99", "share"))
    for f in FIELDS:
        v = sorted(acc[f])
        share = ""
        if f.endswith("_us"):
            share = "%7.1f%%" % (100.0 * sum(acc[f]) / total) if total else ""
        print("%-16s %10.1f %10.1f %10.1f %8s" % (
            f, statistics.mean(acc[f]), v[len(v) // 2], v[int(len(v) * 0.99)], share))

    print()
    # Effective copy rate through the coalesce bounce buffer. A plain aligned
    # memcpy on this box should manage 10+ GB/s; anything near 3 GB/s means the
    # allocation (zero-fill, first-touch faults) dominates the copy.
    rates = [b / max(1, c) for b, c in zip(acc["bytes"], acc["coalesce_us"])]
    print("coalesce effective rate   %.1f MB/s mean  (bytes / coalesce_us)"
          % statistics.mean(rates))
    print("merge ratio ops/effective %.2f" % (
        statistics.mean(acc["ops"]) / max(1e-9, statistics.mean(acc["effective_ops"]))))
    print("strip writes per MiB      %.0f" % (
        statistics.mean(acc["strip_writes"]) / (statistics.mean(acc["bytes"]) / 2**20)))
    print("submit per strip write    %.1f us" % (
        statistics.mean(acc["submit_us"]) / max(1e-9, statistics.mean(acc["strip_writes"]))))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
