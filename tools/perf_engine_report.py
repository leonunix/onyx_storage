#!/usr/bin/env python3
"""Turn a system-wide `perf record` into a CPU-seconds ledger, with a residual check.

Companion to `tools/perf_engine.sh`. The unit everywhere is **CPU-seconds over
the window**, not "% overhead", because the question this profile exists to
answer is stated in absolute CPUs: system-wide kernel time on this box is ~20
CPUs, 57% of it billed to the engine, and the frontend axis cannot reach it
([[ublk_kernel_tax_measured_engine_owns_the_sys_time]]). Percent-of-profile
would silently renormalise away exactly the comparison that matters.

A `cycles` sample at `-F <rate>` stands for 1/rate CPU-seconds on the thread it
hit, so `samples / rate` is directly comparable to the `/proc/stat` delta that
`cpu_snapshot.py` captured around the same window. That ratio is printed as
`accounted%` and is the load-bearing number: read it before any breakdown.
Below ~90% the profile is missing CPU time (NMI-blind regions, lost samples)
and the buckets underneath it are not a partition of anything.

Usage:  perf_engine_report.py <rundir> [flat|idle] [--top N]
"""
import collections
import os
import re
import subprocess
import sys

# Kernel symbol -> bucket. First match wins, so order is meaningful: the
# specific driver/subsystem rules come before the generic ones they sit under.
KERNEL_RULES = [
    # The first four families are DATA-DERIVED: they are the ones that came out
    # of the p1 profile's "kernel other" lump, which was 53% of the engine's
    # kernel time until they were named. None of them is IO work — they are the
    # per-wake and per-syscall cost of running 258 threads on 44 cores.
    ("IPI + TLB shootdown", r"flush_tlb|smp_call_function|^__flush_smp_call|"
                            r"^llist_(reverse_order|add_batch)"),
    ("entry/exit + cpu mitigations", r"irq_return_iret|clear_bhb_loop|its_return_thunk|"
                                     r"^sync_regs|apic_msr_eoi|arch_exit_to_user_mode|"
                                     r"entry_SYSRETQ|entry_SYSCALL_64_after|^error_entry|"
                                     r"srso|^verw|^mds_|retbleed|spectre|indirect_thunk|"
                                     r"^native_load_gs|^switch_mm"),
    ("ctx-switch FPU/regs", r"fpregs|^fpu_|switch_fpu|xsave|xrstor|^os_xsave"),
    ("O_DIRECT page pinning", r"^gup_|grab_folio|pin_user_pages|unpin_user|^__gup"),
    ("ublk driver", r"^ublk"),
    ("io_uring", r"^io_(uring|wq|submit|issue|sq|cq|req|poll|ring|iopoll|prep|rw|do_iopoll)|^__io_"),
    ("user copy", r"copy_user|^copy_(to|from)_|^_copy_|iov_iter|^copyout|^copyin"),
    ("block+nvme", r"^(nvme|blk|bio|submit_bio|__blk|sbitmap|scsi|dm_|disk_)"),
    ("page fault+mm", r"^(handle_mm_fault|__handle_mm_fault|do_user_addr_fault|exc_page_fault|"
                      r"filemap|__do_fault|do_fault|finish_fault|set_pte|__pte|pte_|clear_page|"
                      r"get_page_from_freelist|__alloc_pages|alloc_pages|rmqueue|free_unref|"
                      r"free_pcppages|__free_|release_pages|zap_|unmap_|tlb_|flush_tlb|"
                      r"vma_|__vma|mmap_|do_mmap|do_munmap|madvise|page_counter|folio_|"
                      r"__folio|lru_|mem_cgroup|charge_)"),
    ("sched+wake", r"^(schedule|__schedule|pick_next|enqueue_task|dequeue_task|try_to_wake_up|"
                   r"ttwu|__wake_up|wake_up_|autoremove_wake|prepare_to_wait|finish_wait|"
                   r"update_curr|update_rq|update_load|set_next_entity|put_prev|"
                   r"sched_|__cond_resched|resched|idle_cpu|available_idle|select_task_rq|"
                   r"__switch_to|finish_task_switch|newidle|load_balance|reweight)|"
                   # ⭐ data-derived: CFS/EEVDF internals the anchored names miss,
                   # and they are 1.0 CPU on their own.
                   r"(pick_task_fair|pick_eevdf|select_idle|update_min_vruntime|"
                   r"update_load_avg|dequeue_entity|enqueue_entity|task_h_load|"
                   r"__calc_delta|sched_clock|set_next_buddy|check_preempt|"
                   r"place_entity|entity_tick|rq_clock|cpuacct|cgroup_rstat)"),
    ("futex", r"futex"),
    ("epoll+poll", r"^(ep_|do_epoll|eventpoll|do_poll|do_select|poll_)"),
    ("kernel locks", r"(queued_spin_lock_slowpath|osq_lock|rwsem|mutex_|down_read|up_read|"
                     r"down_write|up_write|_raw_spin|smp_call)"),
    ("syscall entry", r"^(do_syscall_64|entry_SYSCALL|syscall_|__x64_sys_|__se_sys_|x64_sys_call|"
                      r"syscall_exit|syscall_enter)"),
    ("irq+softirq", r"^(__softirqentry|irq_|__irq|handle_irq|handle_edge|do_softirq|"
                     r"asm_call_irq|common_interrupt|sysvec_|apic_|__sysvec)"),
    ("timer+rcu", r"^(rcu_|__rcu|hrtimer|timer_|run_timer|tick_|ktime|clockevents|"
                   r"update_process_times|read_tsc|getnstimeofday|__x64_sys_clock)"),
    ("fs+socket", r"^(vfs_|__vfs|do_iter|new_sync|generic_file|iomap|xfs_|ext4_|sock_|unix_|"
                   r"skb_|__skb|tcp_|net_|__sys_|do_sendfile|fdget|__fdget|fput|fget)"),
]

# User symbol -> bucket, matched on the DEMANGLED name. The crate rules mean a
# bucket maps to a place in the tree you can actually go and read.
USER_RULES = [
    ("onyx: chunklet (RAID/io_uring)", r"^(onyx_)?chunklet::|^onyx_chunklet"),
    ("onyx: metadb (metadata)", r"^(onyx_)?metadb::|^onyx_metadb"),
    ("onyx: engine", r"^onyx_storage::"),
    ("compress lz4", r"lz4|LZ4"),
    ("compress zstd", r"[Zz][Ss][Tt][Dd]"),
    ("hash xxh3/crc", r"xxh|XXH|crc32|Crc|crc64"),
    ("raid6 parity (gf/xor)", r"gf_|galois|parity|raid6|xor_"),
    ("allocator (jemalloc)", r"^(je_|_rjem|arena_|tcache_|extent_|malloc|free$|calloc|realloc|"
                             r"rallocx|mallocx|sdallocx|tsd_)"),
    ("libc memcpy/memset", r"^__(memcpy|memmove|memset|memcmp|mempcpy)|^(memcpy|memmove|memset)"),
    ("std sync/park", r"^std::sys|^std::thread|parking_lot|crossbeam|^core::sync|^std::sync|"
                      # v0-mangled names perf does not demangle (1.3% of samples);
                      # matched on the raw form rather than dropped into "other".
                      r"lock_contended|3std3sys|4sync5mutex|5futex"),
    ("rust std/core", r"^(std::|core::|alloc::)"),
    ("libc other", r"^(__libc|_int_|pthread|__pthread|__GI|__sched|clock_|gettime|syscall$)"),
]


def soften(sym):
    """Best-effort demangle of what `perf` leaves mangled.

    perf demangles plain legacy Rust symbols but gives up on two forms that are
    common in this binary, and both then land in the "other" bucket and read as
    noise:

      * LTO-local symbols carrying a `.llvm.<hash>` suffix;
      * v0 (`_R...`) names, which perf cannot demangle at all (1.3% of samples
        here, left as-is — the crate name is still inside the mangled string, so
        the bucket rules match it anyway).

    Legacy `_ZN` names are strictly length-prefixed, so walking them is exact
    rather than heuristic.
    """
    sym = re.sub(r"\.llvm\.\d+$", "", sym)
    if not sym.startswith("_ZN"):
        return sym
    i, parts = 3, []
    while i < len(sym) and sym[i].isdigit():
        j = i
        while j < len(sym) and sym[j].isdigit():
            j += 1
        length = int(sym[i:j])
        parts.append(sym[j:j + length])
        i = j + length
    if not parts:
        return sym
    if re.fullmatch(r"h[0-9a-f]{16}", parts[-1]):
        parts.pop()  # the trailing type hash
    pretty = []
    for part in parts:
        for raw, cooked in (("$LT$", "<"), ("$GT$", ">"), ("$u20$", " "),
                            ("$C$", ","), ("$RF$", "&"), ("$u27$", "'"), ("..", "::")):
            part = part.replace(raw, cooked)
        pretty.append(part)
    return "::".join(pretty)


def bucket(name, rules, default):
    for label, pattern in rules:
        if re.search(pattern, name):
            return label
    return default


def read_snapshot(path):
    values = {}
    with open(path) as handle:
        for line in handle:
            if "=" in line:
                key, _, value = line.partition("=")
                values[key.strip()] = value.strip()
    return values


def snapshot_busy(start, end):
    """Busy CPU-seconds system-wide, and the engine's own utime/stime, over a window.

    `cpu_snapshot.py` writes ticks; /proc/stat's non-idle classes are summed
    because the profile can sample any of them (softirq and IRQ time is
    precisely the part a per-process reading cannot see).
    """
    hz = float(os.sysconf("SC_CLK_TCK"))
    busy_keys = ["user", "nice", "system", "irq", "softirq", "steal"]
    total = 0.0
    for key in busy_keys:
        a = start.get(f"cpu_{key}")
        b = end.get(f"cpu_{key}")
        if a is not None and b is not None:
            total += (float(b) - float(a)) / hz
    out = {"busy": total}
    for who in ("engine", "client"):
        for kind in ("utime", "stime"):
            a = start.get(f"{who}_{kind}")
            b = end.get(f"{who}_{kind}")
            if a is not None and b is not None:
                out[f"{who}_{kind}"] = (float(b) - float(a)) / hz
    return out


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    rundir = args[0] if args else sys.exit(__doc__)
    which = args[1] if len(args) > 1 else "flat"
    top = 25
    for a in sys.argv[1:]:
        if a.startswith("--top"):
            top = int(a.split("=")[1]) if "=" in a else top

    data = os.path.join(rundir, f"{which}.data")
    rate = 299.0 if which in ("flat", "idle") else 99.0

    engine_pid = None
    pid_path = os.path.join(rundir, "engine.pid")
    if os.path.exists(pid_path):
        engine_pid = open(pid_path).read().strip()

    # -F with no callchain fields keeps this a flat attribution; `perf script`
    # rather than `perf report` so the bucketing is ours and auditable.
    script = subprocess.run(
        # ⚠ `sym`/`dso` print EMPTY unless `ip` is also in the field list —
        # perf ties symbol resolution to the ip field, and does not warn.
        ["perf", "script", "-i", data, "-F", "comm,pid,tid,ip,sym,dso", "--no-inline"],
        capture_output=True, text=True, check=True,
    ).stdout

    per_comm = collections.Counter()
    per_proc = collections.Counter()
    eng_kernel = collections.Counter()
    eng_user = collections.Counter()
    eng_kernel_sym = collections.Counter()
    eng_user_sym = collections.Counter()
    # The residual, symbol by symbol. A bucket called "other" that nobody can
    # open is how a profile hides its own biggest term, so it is always
    # printable: `--other` lists what fell through the rules.
    other_sym = collections.Counter()
    total = 0
    engine_total = 0
    idle_samples = 0
    kernel_thread_samples = 0
    per_proc_kernel = collections.Counter()

    for line in script.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # `comm tid/pid ... sym (dso)` — the dso is the last parenthesised field.
        dso = ""
        if line.endswith(")"):
            idx = line.rfind(" (")
            if idx > 0:
                dso = line[idx + 2:-1]
                line = line[:idx]
        fields = line.split()
        if len(fields) < 2:
            continue
        # comm may contain spaces; tid/pid is the first numeric-ish field.
        pid = tid = None
        sym_start = 1
        for i, field in enumerate(fields[:4]):
            if "/" in field and field.replace("/", "").isdigit():
                # ⚠ `perf script -F pid,tid` prints "pid/tid" in THAT order.
                # Reading it the other way round silently attributes every
                # engine thread to a process of its own, and the engine's
                # share of the profile comes out as zero.
                pid, tid = field.split("/")
                sym_start = i + 1
                break
            if field.isdigit():
                tid = pid = field
                sym_start = i + 1
                break
        comm = " ".join(fields[:max(sym_start - 1, 1)])
        # Drop the ip column that had to be requested for symbols to resolve.
        if sym_start < len(fields) and re.fullmatch(r"[0-9a-f]{6,}", fields[sym_start]):
            sym_start += 1
        sym = soften(" ".join(fields[sym_start:])) or "[unknown]"

        is_kernel = dso.startswith("[") or dso.endswith(".ko") or dso == "[kernel.kallsyms]"
        mine = engine_pid is not None and pid == engine_pid
        if mine:
            engine_total += 1
            per_comm[comm] += 1
            if is_kernel:
                label = bucket(sym, KERNEL_RULES, "kernel other")
                eng_kernel[label] += 1
                eng_kernel_sym[sym] += 1
                if label == "kernel other":
                    other_sym[f"[k] {sym}"] += 1
            else:
                label = bucket(sym, USER_RULES, "user other")
                eng_user[label] += 1
                eng_user_sym[sym] += 1
                if label == "user other":
                    other_sym[f"[u] {sym}"] += 1
            per_proc["engine"] += 1
        elif comm.startswith("swapper"):
            # Idle. The `cycles` event still fires on this box's shallow idle,
            # so swapper samples are REAL samples that are not busy time —
            # counting them would inflate the denominator and make every
            # bucket's share too small.
            idle_samples += 1
            continue
        elif comm.startswith(("ksoftirqd", "kworker", "rcu_", "migration",
                              "irq/", "kcompactd", "kswapd", "khugepaged")):
            # ⚠ These are the CPUs `cpu_snapshot.py` cannot see per-process and
            # that the frontend A/B had to leave "billed to nobody".
            per_proc[f"kernel thread: {comm.split('/')[0]}"] += 1
            kernel_thread_samples += 1
        else:
            # Everything else stays SEPARATE by comm rather than being lumped
            # into one "other": fio names its worker threads after the job, so
            # the client shows up under that name and must not hide.
            per_proc[f"proc: {comm}"] += 1
            if is_kernel:
                per_proc_kernel[f"proc: {comm}"] += 1
        total += 1

    start = read_snapshot(os.path.join(rundir, f"cpu.{which}.start"))
    end = read_snapshot(os.path.join(rundir, f"cpu.{which}.end"))
    wall = float(open(os.path.join(rundir, f"t.{which}.end")).read()) - \
        float(open(os.path.join(rundir, f"t.{which}.start")).read())
    stat = snapshot_busy(start, end)

    # ⚠ The perf window and the /proc/stat window are NOT the same length: the
    # snapshots bracket a `status` call that takes tens of seconds under load.
    # So the calibration is done in CPUs (a rate), never in CPU-seconds, and
    # the sampled record's own duration is the denominator for the profile.
    record = float(os.environ.get("PERF_WINDOW", "0")) or None
    # Everything below is in CPUs: samples / rate / seconds-of-record.
    cpus = lambda n: n / rate / (record or wall)
    print(f"== {rundir} [{which}]  rate {rate:.0f}Hz  busy samples {total}"
          f"  idle(swapper) samples {idle_samples}")
    prof_cpus = cpus(total)
    print(f"   profile: {prof_cpus:.2f} busy CPUs  (over the {record or wall:.0f}s record)")
    print(f"   /proc/stat: {stat['busy'] / wall:.2f} busy CPUs  (over a {wall:.0f}s window that"
          f" also spans the status calls)  => ratio {100.0 * prof_cpus * wall / stat['busy']:.1f}%")
    if "engine_utime" in stat:
        own = (stat["engine_utime"] + stat["engine_stime"]) / wall
        print(f"   engine's own utime+stime: {own:.2f} CPUs "
              f"(u {stat['engine_utime'] / wall:.2f} / s {stat['engine_stime'] / wall:.2f}); "
              f"profile bills the engine {cpus(engine_total):.2f} CPUs")

    def table(title, counter, denom):
        print(f"\n-- {title}")
        for name, count in counter.most_common(top):
            share = 100.0 * count / denom if denom else 0.0
            print(f"   {cpus(count):7.2f} CPUs  {share:5.1f}%  {name}")

    table("by process", per_proc, total)
    print(f"   {cpus(kernel_thread_samples):7.2f} CPUs  "
          f"{100.0 * kernel_thread_samples / max(total, 1):5.1f}%  = all kernel threads together")
    ek, eu = sum(eng_kernel.values()), sum(eng_user.values())
    print(f"\n== ENGINE {cpus(engine_total):.2f} CPUs = user {cpus(eu):.2f} "
          f"({100.0 * eu / max(engine_total, 1):.0f}%) + kernel {cpus(ek):.2f} "
          f"({100.0 * ek / max(engine_total, 1):.0f}%)")
    if per_proc_kernel:
        print("   (kernel time billed to NON-engine user processes, i.e. the client's "
              "syscalls and copies:)")
        for name, count in per_proc_kernel.most_common(6):
            print(f"   {cpus(count):7.2f} CPUs  {name}")
    table("engine KERNEL by subsystem", eng_kernel, ek)
    table("engine USER by crate/bucket", eng_user, eu)
    table("engine by thread name (comm)", per_comm, engine_total)
    if "--other" in sys.argv:
        table("engine UNBUCKETED residual, symbol by symbol", other_sym,
              sum(other_sym.values()))
    table("engine top kernel symbols", eng_kernel_sym, ek)
    table("engine top user symbols", eng_user_sym, eu)


if __name__ == "__main__":
    main()
