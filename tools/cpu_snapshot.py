#!/usr/bin/env python3
"""One CPU-accounting snapshot, for differencing across an A/B measurement window.

Usage:  cpu_snapshot.py <outfile> [engine_comm] [client_comm]

Writes `key=value` lines. Three independent accounts, deliberately:

  * SYSTEM-WIDE (`/proc/stat`) — the ground truth. Every CPU tick the machine
    spent, including kernel work that belongs to no process's own utime/stime
    and softirq/IRQ time that a per-process reading cannot see at all.
  * the ENGINE process — what Onyx itself burns.
  * the CLIENT (fio) process tree — ⚠ load-bearing for the frontend A/B, not a
    nice-to-have. The plugin runs the protocol client INSIDE fio's threads
    (header encode, one staging memcpy per write, frame parse, one payload
    memcpy per read), while ublk instead copies in the kernel on fio's behalf
    (`copy_to/from_user`, billed to fio's stime). Measure only the engine and
    the plugin looks cheap because its cost moved house rather than
    disappearing. The comparable quantity is total machine CPU per GiB.

`cutime`/`cstime` are ignored on purpose: they are only populated after a child
is reaped, so they read 0 mid-run and would silently under-count. fio's
`--numjobs` children are enumerated live instead.
"""
import os
import sys
import time


def proc_stat_cpu():
    """Aggregate `cpu` line from /proc/stat, in ticks."""
    with open("/proc/stat") as handle:
        for line in handle:
            if line.startswith("cpu "):
                fields = [int(value) for value in line.split()[1:]]
                names = [
                    "user", "nice", "system", "idle", "iowait",
                    "irq", "softirq", "steal", "guest", "guest_nice",
                ]
                return dict(zip(names, fields))
    raise RuntimeError("/proc/stat has no aggregate cpu line")


def pids_of(comm):
    """Every live pid whose comm matches exactly.

    Matches on /proc/<pid>/comm rather than the cmdline, so it behaves like
    `pgrep -x` — nvme-box requires that (memory: `pkill -f` there has matched
    the wrong process before).
    """
    found = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/comm") as handle:
                if handle.read().strip() == comm:
                    found.append(int(entry))
        except OSError:
            continue  # exited between listdir and open
    return sorted(found)


def cpu_ticks_of(pid):
    """(utime, stime) for a pid, in ticks, or None if it is gone.

    Splits on the LAST ')' because field 2 is the comm in parentheses and may
    itself contain spaces or parentheses.
    """
    try:
        with open(f"/proc/{pid}/stat") as handle:
            raw = handle.read()
    except OSError:
        return None
    tail = raw[raw.rindex(")") + 2:].split()
    # After comm and state, utime is field 14 and stime 15 (1-based), i.e.
    # index 11 and 12 of the remainder.
    return int(tail[11]), int(tail[12])


def sum_tree(comm):
    total_utime = 0
    total_stime = 0
    pids = pids_of(comm)
    counted = 0
    for pid in pids:
        ticks = cpu_ticks_of(pid)
        if ticks is None:
            continue
        total_utime += ticks[0]
        total_stime += ticks[1]
        counted += 1
    return counted, total_utime, total_stime


def main():
    if len(sys.argv) < 2:
        print(__doc__.strip().splitlines()[2], file=sys.stderr)
        return 2
    out = sys.argv[1]
    engine_comm = sys.argv[2] if len(sys.argv) > 2 else "onyx-storage"
    client_comm = sys.argv[3] if len(sys.argv) > 3 else "fio"

    # Read the cheap system counter FIRST and the wall clock beside it, so the
    # window the report divides by is the window /proc/stat actually covers.
    cpu = proc_stat_cpu()
    wall_ns = time.monotonic_ns()

    engine_n, engine_utime, engine_stime = sum_tree(engine_comm)
    client_n, client_utime, client_stime = sum_tree(client_comm)

    lines = [
        f"wall_ns={wall_ns}",
        f"clk_tck={os.sysconf('SC_CLK_TCK')}",
        f"ncpu={os.cpu_count()}",
        f"engine_comm={engine_comm}",
        f"engine_procs={engine_n}",
        f"engine_utime={engine_utime}",
        f"engine_stime={engine_stime}",
        f"client_comm={client_comm}",
        f"client_procs={client_n}",
        f"client_utime={client_utime}",
        f"client_stime={client_stime}",
    ]
    lines += [f"cpu_{name}={value}" for name, value in cpu.items()]
    lines.append(f"cpu_total={sum(cpu.values())}")

    with open(out, "w") as handle:
        handle.write("\n".join(lines) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
