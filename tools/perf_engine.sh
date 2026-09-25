#!/usr/bin/env bash
#
# Point `perf` INSIDE the engine — the follow-up that
# [[ublk_kernel_tax_measured_engine_owns_the_sys_time]] asks for.
#
# That measurement established, in ABSOLUTE CPUs, that the frontend axis cannot
# reach the ~20 system CPUs this box burns: removing ublk entirely moves
# system-wide kernel time only -4.3%, because 57% of it is billed to the engine
# itself. So the next question is not "which transport" but "which engine code",
# and that needs a sampling profile rather than another A/B.
#
# Three things this harness insists on:
#
#  * SYSTEM-WIDE (`-a`), not `-p <engine>`. The engine's own utime/stime misses
#    the 17% of kernel CPU billed to nobody (softirq, IRQ, kworkers, the ublk
#    kernel threads), which is exactly the part under suspicion. One system-wide
#    dataset is also a superset: engine, fio and kernel threads separate later
#    by `comm`, at report time, for free.
#
#  * `--call-graph lbr`, not `fp` and not `dwarf`. The release profile is
#    `debug = "line-tables-only"` with no `force-frame-pointers`, so `fp`
#    callchains are broken for Rust frames. `dwarf` would work but copies the
#    user stack on every sample. This box is Ice Lake-SP, so the LBR gives
#    32-deep stacks with no stack copy and no frame pointers — verified working
#    before this script was written.
#
#  * A CLEAN WINDOW BEFORE THE PROFILED ONE. `perf` perturbs, and nobody knows
#    by how much on this workload. The baseline window measures throughput with
#    no profiler attached, so the cost of the instrument is a number in the
#    artifacts instead of an assumption.
#
# ⛔ SAMPLING ONLY, deliberately. Never gdb-attach or SIGSTOP an engine with a
#    live ublk device — the kernel's ublk timeout kills the device.
#
# Usage:  perf_engine.sh <tag> [config]
#         BURN=60 BASE=60 FLAT=60 CG=30 tools/perf_engine.sh p1
set -u

TAG=${1:?usage: perf_engine.sh <tag> [config]}
CFG=${2:-/root/onyx_storage/config/nvme-chunklet.toml}
B=/root/onyx_storage/target/release/onyx-storage
SNAP=/root/onyx_storage/tools/cpu_snapshot.py
OUT=/root/perfrun/$TAG

# The burn is 60 s on an AGED pool (the 420 s figure belongs to a fresh ring).
BURN=${BURN:-60}
BASE=${BASE:-60}     # profiler-free window, so perf's own cost is measurable
FLAT=${FLAT:-60}     # flat profile: leaf attribution, cheap, high rate
CG=${CG:-30}         # LBR call-graph: expensive, lower rate
RWMIX=${RWMIX:-70}
JOBS=${JOBS:-16}
DEPTH=${DEPTH:-16}
BS=${BS:-4k-32k}

# ⛔ A RANGE MUST GO THROUGH --bsrange (fio 3.36 reads `--bs=4k-32k` as
# "4k reads, 32k writes" and does not warn).
case "$BS" in
    *-*) BS_OPT="--bsrange=$BS" ;;
    *)   BS_OPT="--bs=$BS" ;;
esac
UBLK=${UBLK:-$(ls -1 /dev/ublkb* 2>/dev/null | head -1)}

mkdir -p "$OUT"
[ -n "$UBLK" ] && [ -e "$UBLK" ] || { echo "FAIL: no /dev/ublkb* device" >&2; exit 1; }
[ -f "$SNAP" ] || { echo "FAIL: missing $SNAP" >&2; exit 1; }
command -v perf >/dev/null || { echo "FAIL: no perf" >&2; exit 1; }

PID=$(pgrep -x -n onyx-storage) || { echo "FAIL: engine not running" >&2; exit 1; }
echo "=== perf_engine $TAG: ublk=$UBLK engine_pid=$PID bs=$BS j${JOBS}d${DEPTH} rwmix=$RWMIX"
echo "=== burn=${BURN}s baseline=${BASE}s flat=${FLAT}s callgraph=${CG}s"
echo "$PID" >"$OUT/engine.pid"

# A window is a (status, cpu) pair at each end, taken as close together as
# possible so the throughput delta and the CPU delta describe the same interval.
mark() {
    "$B" -c "$CFG" status >"$OUT/status.$1" 2>/dev/null
    python3 "$SNAP" "$OUT/cpu.$1"
    date +%s >"$OUT/t.$1"
}

RUNTIME=$((BURN + BASE + FLAT + CG + 120))
fio --name="perf-$TAG" --filename="$UBLK" --direct=1 --ioengine=io_uring \
    --rw=randrw --rwmixread="$RWMIX" $BS_OPT --iodepth="$DEPTH" --numjobs="$JOBS" \
    --runtime=$RUNTIME --time_based --group_reporting --refill_buffers \
    --write_bw_log="$OUT/bw" --log_avg_msec=5000 >"$OUT/fio.log" 2>&1 &
FIOPID=$!
echo "=== fio pid $FIOPID runtime ${RUNTIME}s, burning ${BURN}s $(date -Is)"
sleep "$BURN"

# ---- window 1: no profiler attached. The control.
mark base.start
iostat -xm "$BASE" 2 >"$OUT/iostat.base" 2>/dev/null &
IOSTAT=$!
sleep "$BASE"
mark base.end
wait "$IOSTAT" 2>/dev/null

# ---- window 2: flat, system-wide, high rate. The primary artifact.
mark flat.start
perf record -a -F 299 -o "$OUT/flat.data" -- sleep "$FLAT" >"$OUT/perf.flat.log" 2>&1
mark flat.end

# ---- window 3: LBR call-graph. Answers "called from where", including the
#      kernel side, which is the whole point of the exercise.
mark cg.start
perf record -a -F 99 --call-graph lbr -o "$OUT/cg.data" -- sleep "$CG" >"$OUT/perf.cg.log" 2>&1
mark cg.end

# ---- window 4: what the machine does with NO load, same instrument. Separates
#      steady-state background work (GC, checkpoints, defrag) from IO-driven.
echo "=== stopping fio, then an idle-engine profile for subtraction $(date -Is)"
kill "$FIOPID" 2>/dev/null
wait "$FIOPID" 2>/dev/null
sleep 20
mark idle.start
perf record -a -F 299 -o "$OUT/idle.data" -- sleep 30 >"$OUT/perf.idle.log" 2>&1
mark idle.end

echo "=== done $(date -Is); artifacts in $OUT"
ls -la "$OUT"
