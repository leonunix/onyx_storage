#!/bin/bash
# Interleaved A/B for the LV2 commit-log sync slab arena, INSIDE ONE PROCESS.
#
#   lv2_arena_ab.sh <tag> [config]
#
# What is being tested: `encode_entries_into_spans` used to ask
# `AlignedBuf::new` for one span buffer per ~1.4 write ops. The thread-local
# pool almost never hit (its condition is `capacity >= size`, and a span's
# length is a wide random variable under 4k-32k), so nearly every span called
# `alloc_zeroed`, which jemalloc satisfies off a recycled extent by PURGING it.
# A 2026-09-24 in-engine `perf` profile measured the result: ~12.9k madvise/s
# from this thread class, 62 % of the box's 20.7k/s, driving ~810k
# TLB-shootdown IPIs/s for ~2.8 CPUs of the machine's 53.4 busy
# (memory `perf_inside_engine_first_cpu_ledger`).
#
# ⭐ THE JUDGE IS THE MECHANISM, NOT THE THROUGHPUT, and in this order:
#
#   1. madvise/s            (`perf stat -e syscalls:sys_enter_madvise`)
#   2. TLB shootdowns/s     (`/proc/interrupts` TLB and CAL rows)
#   3. arena hit rate       (`mem_arena_lv2: hits/takes`, `overflow` MUST be 0)
#   4. throughput           (status delta, inside the A1-vs-A2 bracket)
#
# Throughput is last because freeing CPU does not have to convert: the LV3
# arena freed ~1.6 cores and converted 6 %. The engine is at 40.5 of its 44
# confined CPUs, which argues it should convert here, but that is a prediction,
# not a result.
#
# Interleaved and flipped over IPC rather than one arm per restart, because an
# arm-per-restart comparison on this box measures run-order drift: two
# IDENTICAL baseline arms once came out 2.13x apart (memory
# `allocator_global_lock_is_the_writer_wall`). A-B-A-B gives within-arm
# reproducibility (A1 vs A2) AND separates the knob from the pool's ageing.
set -u

TAG=${1:?usage: lv2_arena_ab.sh <tag> [config]}
CFG=${2:-/root/onyx_storage/config/nvme-chunklet.toml}
B=/root/onyx_storage/target/release/onyx-storage
OUT=/root/lv2arena/$TAG
# 60 s, not 420: the burn is 60 s on an AGED pool (memory
# `aged_pool_burn_is_60s_not_420s`). The box pool must NOT be cleared.
BURN=${BURN:-90}
SEG=${SEG:-180}
# QD256 j16d16 4k-32k randrw 70/30 — the canonical shape, and the one the perf
# ledger this change is judged against was taken at. ⛔ Any other QD must be
# agreed first and stated in the conclusion (memory `qd_ladder_congestion_collapse`).
RWMIX=${RWMIX:-70}
JOBS=${JOBS:-16}
DEPTH=${DEPTH:-16}
SEGMENTS=${SEGMENTS:-"A1 B1 A2 B2"}
ATTACH=${ATTACH:-0}

mkdir -p "$OUT"
if [ "$ATTACH" = "0" ]; then
    RUST_LOG=${RUST_LOG:-"info,onyx_storage::io::block_backend=error,onyx_chunklet::pool::ld_ops=error"} \
        nohup "$B" -c "$CFG" start -v fio-volume >"$OUT/engine.log" 2>&1 &
fi

# ⚠ Discovered, not hardcoded: the ublk device id increments every time a
# killed engine leaves a stale one behind (the box is on ublkb1).
UBLK=""
for _ in $(seq 1 240); do
    UBLK=$(ls -1 /dev/ublkb* 2>/dev/null | head -1)
    [ -n "$UBLK" ] && break
    sleep 2
done
[ -n "$UBLK" ] || { echo "FAIL: no /dev/ublkb* (see $OUT/engine.log)" >&2; exit 1; }
sleep 5

# ⭐ Self-proof, BOTH ways, before any measurement: if the IPC command does not
# actually move the switch, every segment below is the same arm wearing two
# names. Refuse to start rather than produce a null result.
#
# ⚠ `tail -1`, and stderr dropped: the CLI writes tracing lines (RLIMIT, the
# NUMA plan) before the reply, so `head -1` reads a log line and the self-proof
# fails against a perfectly good engine. The echoed state is the LAST line.
for want in off on; do
    got=$("$B" -c "$CFG" mem-arena-lv2 "$want" 2>/dev/null | tail -1)
    if [ "$got" != "$want" ]; then
        echo "FAIL self-proof: asked for '$want', engine echoed '$got'" >&2
        exit 2
    fi
done
echo "self-proof ok: mem-arena-lv2 moves both ways" >"$OUT/plan"

NSEG=$(echo "$SEGMENTS" | wc -w)
RUNTIME=$((BURN + NSEG * SEG + 300))
fio --name=onyx-lv2arena --filename="$UBLK" --direct=1 --ioengine=io_uring \
    --rw=randrw --rwmixread="$RWMIX" --bsrange=4k-32k --iodepth="$DEPTH" \
    --numjobs="$JOBS" --runtime="$RUNTIME" --time_based --group_reporting \
    --refill_buffers --write_bw_log="$OUT/bw" --log_avg_msec=5000 \
    >"$OUT/fio.log" 2>&1 &
FIOPID=$!
echo "fio pid $FIOPID ublk=$UBLK runtime ${RUNTIME}s segments: $SEGMENTS" >>"$OUT/plan"

# Sum the per-CPU columns of one /proc/interrupts row. TLB = shootdowns, CAL =
# function-call IPIs; a madvise(MADV_DONTNEED) on a mapping this many threads
# share broadcasts one per CPU, which is why these rows are the mechanism.
irq_snapshot() {
    grep -E "^ *(TLB|CAL):" /proc/interrupts |
        awk '{s=0; for (i=2; i<=NF-2; i++) s+=$i; print $1, s}'
}

sleep "$BURN"
echo "burn done $(date -Is)" >>"$OUT/plan"

for seg in $SEGMENTS; do
    case "$seg" in
        A*) want=on ;;
        B*) want=off ;;
    esac
    got=$("$B" -c "$CFG" mem-arena-lv2 "$want" 2>/dev/null | tail -1)
    if [ "$got" != "$want" ]; then
        echo "FAIL: segment $seg wanted '$want', engine echoed '$got'" >&2
        exit 2
    fi
    echo "$got" >"$OUT/knob.$seg"

    date +%s >"$OUT/t.$seg.start"
    irq_snapshot >"$OUT/irq.$seg.start"
    "$B" -c "$CFG" status >"$OUT/status.$seg.start" 2>/dev/null
    # perf stat runs inside the window, concurrently with it, so the madvise
    # rate and the status delta describe the same interval.
    perf stat -a -e syscalls:sys_enter_madvise -- sleep $((SEG / 3)) \
        2>"$OUT/madvise.$seg" &
    PERFPID=$!
    sleep $((SEG - 40))
    wait "$PERFPID" 2>/dev/null
    iostat -xm 20 2 >"$OUT/iostat.$seg" 2>/dev/null
    "$B" -c "$CFG" status >"$OUT/status.$seg.end" 2>/dev/null
    irq_snapshot >"$OUT/irq.$seg.end"
    date +%s >"$OUT/t.$seg.end"
    echo "segment $seg ($want) done $(date -Is)" >>"$OUT/plan"
done

echo "done; engine and fio still running. Report with:"
echo "  tools/lv2_arena_report.py $OUT"
echo "Stop with:  pkill -x fio; $B -c $CFG stop"
