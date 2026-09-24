#!/usr/bin/env python3
"""CPU cost per unit data for each arm of a frontend_ab.sh run.

Usage:  frontend_cpu_report.py <run_dir> [arm ...]

⭐ THE JUDGE QUANTITY IS `CPU-s/GiB`, NOT CPU SECONDS. The two frontends are
already known to reach the same throughput, but "already known" is not "did in
this run", and a 3% throughput difference would otherwise masquerade as a 3% CPU
difference. Normalising by the bytes the ENGINE says it moved (`volume_bytes`,
recorded by both paths) removes that confound.

⚠ Read `total` before `engine`. The plugin runs its protocol client inside fio's
threads while ublk copies in the kernel on fio's behalf, so cost moves between
the `engine` and `client` columns without changing what the machine paid. Only
`total` (system-wide, from /proc/stat) is invariant to where the work is billed,
and only it sees softirq/IRQ time that no process's utime/stime contains.

Columns:
  win_s      window length from the CPU snapshots' own monotonic clock
  GiB        user data the engine moved in the window (read + write)
  MiB/s      derived from those two, i.e. the arm's real throughput
  eng_u/s    engine CPUs' worth of user time   (CPU-seconds per wall second)
  eng_s/s    engine CPUs' worth of system time
  cli_u/s    fio CPUs' worth of user time
  cli_s/s    fio CPUs' worth of system time
  busy/s     system-wide non-idle CPUs (includes iowait/irq/softirq/steal)
  sys/s      system-wide system+irq+softirq CPUs -- the kernel-tax column
  tot/GiB    busy CPU-seconds per GiB  <- lead with this
  sys/GiB    system-wide kernel CPU-seconds per GiB
"""
import os
import sys

IDLE_FIELDS = ("idle",)


def parse_kv(path):
    values = {}
    with open(path) as handle:
        for line in handle:
            key, _, raw = line.strip().partition("=")
            if not key:
                continue
            try:
                values[key] = int(raw)
            except ValueError:
                values[key] = raw
    return values


def parse_status(path):
    """`onyx status` as {label: {key: int}}.

    Keyed by LABEL, not flattened. `read=`/`write=` appear under several labels
    (`volume_ops` counts operations, `volume_bytes` counts bytes, and more), so a
    flat dict silently returns whichever line came last — op counts where bytes
    were meant, off by ~10^4.
    """
    sections = {}
    with open(path) as handle:
        for line in handle:
            label, sep, rest = line.partition(":")
            if not sep:
                continue
            bucket = sections.setdefault(label.strip(), {})
            for token in rest.split():
                key, _, raw = token.partition("=")
                try:
                    bucket[key] = int(raw)
                except ValueError:
                    pass
    return sections


def volume_bytes(sections, path):
    section = sections.get("volume_bytes")
    if not section or "read" not in section or "write" not in section:
        raise SystemExit(f"{path}: no `volume_bytes: read=.. write=..` line")
    return section["read"], section["write"]


def arms_in(run_dir):
    """Arms in the order they actually RAN.

    Ordered by each arm's own start-snapshot clock rather than by name: a drift
    check is only readable in run order, and sorting names would put P1 before
    U1 while also assuming a naming convention that nothing enforces.
    """
    found = []
    for name in os.listdir(run_dir):
        if name.startswith("cpu.") and name.endswith(".start"):
            arm = name[len("cpu."):-len(".start")]
            started = parse_kv(os.path.join(run_dir, name)).get("wall_ns", 0)
            found.append((started, arm))
    return [arm for _, arm in sorted(found)]


def collect(run_dir, arm):
    cpu_a = parse_kv(os.path.join(run_dir, f"cpu.{arm}.start"))
    cpu_b = parse_kv(os.path.join(run_dir, f"cpu.{arm}.end"))
    path_a = os.path.join(run_dir, f"status.{arm}.start")
    path_b = os.path.join(run_dir, f"status.{arm}.end")
    read_a, write_a = volume_bytes(parse_status(path_a), path_a)
    read_b, write_b = volume_bytes(parse_status(path_b), path_b)

    tck = cpu_a.get("clk_tck") or 100
    win_s = (cpu_b["wall_ns"] - cpu_a["wall_ns"]) / 1e9
    if win_s <= 0:
        raise SystemExit(f"{arm}: non-positive window {win_s}s")

    def ticks(key):
        return (cpu_b.get(key, 0) - cpu_a.get(key, 0)) / tck

    cpu_keys = [k for k in cpu_a if k.startswith("cpu_") and k != "cpu_total"]
    busy = sum(ticks(k) for k in cpu_keys if k[len("cpu_"):] not in IDLE_FIELDS)
    kernel = sum(ticks(f"cpu_{n}") for n in ("system", "irq", "softirq"))

    read_bytes = read_b - read_a
    write_bytes = write_b - write_a
    gib = (read_bytes + write_bytes) / 2**30

    return {
        "arm": arm,
        "win_s": win_s,
        "gib": gib,
        "mibs": (read_bytes + write_bytes) / 2**20 / win_s,
        "eng_u": ticks("engine_utime") / win_s,
        "eng_s": ticks("engine_stime") / win_s,
        "cli_u": ticks("client_utime") / win_s,
        "cli_s": ticks("client_stime") / win_s,
        "busy": busy / win_s,
        "kernel": kernel / win_s,
        "tot_gib": busy / gib if gib > 0 else float("nan"),
        "sys_gib": kernel / gib if gib > 0 else float("nan"),
        "ncpu": cpu_a.get("ncpu", 0),
        "eng_procs": cpu_a.get("engine_procs", 0),
        "cli_procs_a": cpu_a.get("client_procs", 0),
        "cli_procs_b": cpu_b.get("client_procs", 0),
    }


def mean(values):
    return sum(values) / len(values) if values else float("nan")


def main():
    if len(sys.argv) < 2:
        print(__doc__.strip(), file=sys.stderr)
        return 2
    run_dir = sys.argv[1]
    arms = sys.argv[2:] or arms_in(run_dir)
    if not arms:
        print(f"no cpu.*.start snapshots in {run_dir}", file=sys.stderr)
        return 1

    rows = [collect(run_dir, arm) for arm in arms]

    header = (f"{'arm':<5}{'win_s':>7}{'GiB':>8}{'MiB/s':>8}"
              f"{'eng_u/s':>9}{'eng_s/s':>9}{'cli_u/s':>9}{'cli_s/s':>9}"
              f"{'busy/s':>8}{'sys/s':>7}{'tot/GiB':>9}{'sys/GiB':>9}")
    print(header)
    print("-" * len(header))
    for row in rows:
        print(f"{row['arm']:<5}{row['win_s']:>7.1f}{row['gib']:>8.1f}{row['mibs']:>8.1f}"
              f"{row['eng_u']:>9.2f}{row['eng_s']:>9.2f}{row['cli_u']:>9.2f}{row['cli_s']:>9.2f}"
              f"{row['busy']:>8.2f}{row['kernel']:>7.2f}{row['tot_gib']:>9.2f}{row['sys_gib']:>9.2f}")

    ublk = [r for r in rows if r["arm"].startswith("U")]
    plug = [r for r in rows if r["arm"].startswith("P")]
    if not (ublk and plug):
        print("\nneed at least one U and one P arm to compare", file=sys.stderr)
        return 0

    print(f"\nmachine: {rows[0]['ncpu']} logical CPUs; "
          f"fio procs {rows[0]['cli_procs_a']}->{rows[0]['cli_procs_b']} at the window edges")

    print("\n=== ublk (U) vs plugin (P), means over arms ===")
    print(f"{'metric':<22}{'ublk':>10}{'plugin':>10}{'delta':>10}{'P vs U':>9}")
    for label, key in (
        ("MiB/s", "mibs"),
        ("total CPU-s/GiB", "tot_gib"),
        ("kernel CPU-s/GiB", "sys_gib"),
        ("busy CPUs", "busy"),
        ("system-wide sys CPUs", "kernel"),
        ("engine user CPUs", "eng_u"),
        ("engine sys CPUs", "eng_s"),
        ("fio user CPUs", "cli_u"),
        ("fio sys CPUs", "cli_s"),
    ):
        u = mean([r[key] for r in ublk])
        p = mean([r[key] for r in plug])
        pct = (p - u) / u * 100 if u else float("nan")
        print(f"{label:<22}{u:>10.2f}{p:>10.2f}{p - u:>10.2f}{pct:>8.1f}%")

    # Within-arm spread first: an effect smaller than the repeat spread of the
    # same arm is not an effect. Two same-frontend arms bracket the run, so
    # their disagreement IS the drift estimate.
    print("\n=== repeatability (same frontend, different time) ===")
    for name, group in (("U", ublk), ("P", plug)):
        if len(group) < 2:
            print(f"{name}: only {len(group)} arm — no spread available "
                  f"⛔ no magnitude may be quoted")
            continue
        for key, label in (("mibs", "MiB/s"), ("tot_gib", "total CPU-s/GiB")):
            values = [r[key] for r in group]
            lo, hi = min(values), max(values)
            spread = (hi - lo) / mean(values) * 100 if mean(values) else float("nan")
            print(f"{name} {label:<18} {' '.join(f'{v:.2f}' for v in values)}"
                  f"   spread {spread:.2f}%")

    u_tot = mean([r["tot_gib"] for r in ublk])
    p_tot = mean([r["tot_gib"] for r in plug])
    spreads = []
    for group in (ublk, plug):
        if len(group) >= 2:
            vals = [r["tot_gib"] for r in group]
            spreads.append((max(vals) - min(vals)) / mean(vals) * 100)
    effect = abs(p_tot - u_tot) / u_tot * 100 if u_tot else float("nan")
    if not spreads:
        print(f"\neffect on total CPU/GiB: {effect:.2f}%; spread UNKNOWN "
              f"(one arm per frontend) ⛔ no magnitude may be quoted")
    else:
        worst = max(spreads)
        print(f"\neffect on total CPU/GiB: {effect:.2f}%; "
              f"worst same-frontend spread: {worst:.2f}%")
        if effect <= worst:
            print("⛔ effect is INSIDE the noise — direction only, no magnitude.")
        else:
            print("⭐ effect exceeds the same-frontend spread; magnitude is arguable "
                  "(still report the spread beside it).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
