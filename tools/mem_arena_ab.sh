#!/bin/bash
# Interleaved A/B for the LV3 slab arena (src/mem), INSIDE ONE PROCESS.
#
#   mem_arena_ab.sh <tag> [config]
#
# Why interleaved and not one arm per run: on this box an arm-per-restart
# comparison measures run-order drift, not the knob -- two IDENTICAL baseline arms
# once came out 2.13x apart (119.2 vs 253.3 MB/s, memory
# allocator_global_lock_is_the_writer_wall). So the arena is flipped over the IPC
# socket (`mem-arena on|off`) without restarting, and each arm is sampled TWICE
# in A-B-A-B order. That gives two things the restart design cannot:
#
#   * within-arm reproducibility (A1 vs A2, B1 vs B2), and
#   * separation of the knob's effect from the pool's monotonic ageing, which
#     shows up as the A1->A2 and B1->B2 drift.
#
# RWMIX=0 by default: at rwmixread=70 on a healthy pool the LV2 ring sits at ~4%,
# LV3 write is DEMAND-limited, and no LV3-side change can move it. Pure random
# write is what makes the drain bind.
#
# Every ratio is a WINDOW DELTA between two `status` samples. fio's bw log gives
# an independent per-segment throughput series (never trust a whole-run average
# here -- it mixes the burst phase with steady state).
set -u

TAG=${1:?usage: mem_arena_ab.sh <tag> [config]}
CFG=${2:-/root/onyx_storage/config/nvme-chunklet.toml}
B=/root/onyx_storage/target/release/onyx-storage
OUT=/root/lv3run/$TAG
BURN=${BURN:-420}     # fresh-ring burst: throughput is 3-12x steady state before this
SEG=${SEG:-360}       # per-segment sampling window
RWMIX=${RWMIX:-0}
SEGMENTS=${SEGMENTS:-"A1 B1 A2 B2"}
# ATTACH=1 reuses an already-running engine instead of starting one. Preferred for
# a follow-up pass: it keeps every segment inside the SAME process AND the same
# pool age, which is the whole point of the design. Also skips the ~1 min open.
ATTACH=${ATTACH:-0}

mkdir -p "$OUT"
if [ "$ATTACH" = "0" ]; then
    # ~870 lines/s of slow-op warnings would be ~350 MB/arm and cost real CPU.
    RUST_LOG=${RUST_LOG:-"info,onyx_storage::io::block_backend=error,onyx_chunklet::pool::ld_ops=error"} \
        nohup "$B" -c "$CFG" start -v fio-volume >"$OUT/engine.log" 2>&1 &
fi

for _ in $(seq 1 240); do
    [ -e /dev/ublkb0 ] && break
    sleep 2
done
if [ ! -e /dev/ublkb0 ]; then
    echo "FAIL: no /dev/ublkb0 (see $OUT/engine.log)" >&2
    exit 1
fi
sleep 5

NSEG=$(echo "$SEGMENTS" | wc -w)
RUNTIME=$((BURN + NSEG * SEG + 240))
fio --name=onyx-arena --filename=/dev/ublkb0 --direct=1 --ioengine=io_uring \
    --rw=randrw --rwmixread="$RWMIX" --bsrange=4k-32k --iodepth=16 --numjobs=16 \
    --runtime="$RUNTIME" --time_based --group_reporting --refill_buffers \
    --write_bw_log="$OUT/bw" --log_avg_msec=5000 \
    >"$OUT/fio.log" 2>&1 &
echo "fio pid $! runtime ${RUNTIME}s segments: $SEGMENTS" >"$OUT/plan"

date +%s >"$OUT/t_fio_start"
sleep "$BURN"
echo "burn done $(date -Is)" >>"$OUT/plan"

for seg in $SEGMENTS; do
    case "$seg" in
        A*) "$B" -c "$CFG" mem-arena on  >"$OUT/knob.$seg" 2>&1 ;;
        B*) "$B" -c "$CFG" mem-arena off >"$OUT/knob.$seg" 2>&1 ;;
    esac
    date +%s >"$OUT/t.$seg.start"
    "$B" -c "$CFG" status >"$OUT/status.$seg.start" 2>/dev/null
    sleep $((SEG - 40))
    iostat -xm 20 2 >"$OUT/iostat.$seg" 2>/dev/null
    "$B" -c "$CFG" status >"$OUT/status.$seg.end" 2>/dev/null
    date +%s >"$OUT/t.$seg.end"
    echo "segment $seg done $(date -Is)" >>"$OUT/plan"
done

echo "done; engine and fio still running. Stop with:"
echo "  pkill -x fio; $B -c $CFG stop"
