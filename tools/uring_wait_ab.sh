#!/bin/bash
# Interleaved A/B for chunklet's batched-submit knobs, INSIDE ONE PROCESS.
#
#   uring_wait_ab.sh <tag> [config]
#
# What is under test (box 2026-08-18, memory lv3_write_leg_syscall_handoff): the
# LV3 write leg is 15.78 ms/call against a device floor of 1.55 ms, and 91.3% of
# it is completion WAIT -- one `io_uring_enter` per completed 4 KiB strip, ~1186
# per call, because `chunklet.uring_coalesced_wait` is off. `uring-wait on` waits
# for a whole wave in one enter; `uring-wave N` sets how wide a wave is.
#
# ⛔ `uring-wave` ALONE is near-useless: with a per-CQE wake the enter count
# tracks total SQEs no matter how wide the wave is. So the rounds are
#
#   round 1 (design A)      ARM_A="on 0"     vs  ARM_B="off 0"
#   round 2 (design A+B)    ARM_A="on 256"   vs  ARM_B="on 0"
#
# and round 2 is only worth running if round 1 moved. Each arm is "<wait> <wave>";
# wave 0 means chunklet's historical 64, and the value is clamped to the ring
# depth (256), so 256 needs no rebuild.
#
# Why interleaved and not one arm per run: on this box an arm-per-restart
# comparison measures run-order drift, not the knob -- two IDENTICAL baseline arms
# once came out 2.13x apart (memory allocator_global_lock_is_the_writer_wall). So
# the knobs are flipped over the IPC socket without restarting and each arm is
# sampled TWICE in A-B-A-B order, which gives within-arm reproducibility AND lets
# mean(A1,A2) - B1 cancel the pool's linear ageing trend without fitting a slope.
#
# Flipping mid-IO is safe: both knobs are process-global atomics read once per
# drain, so a wave already in flight finishes under the value it started with.
#
# RWMIX=0 by default: at rwmixread=70 on a healthy pool the LV2 ring sits at ~4%,
# LV3 write is DEMAND-limited, and no LV3-side change can move it. Pure random
# write is what makes the drain bind.
#
# Read the result with:
#   tools/flush_delta.py $OUT/status.<seg>.start $OUT/status.<seg>.end <window_s>
# The discriminator is the `enters` line on the `drain_data` class: it must fall
# from ~sqes/call to ~waves/call. If enters does NOT move, the knob did not take
# (a completion observer is intercepting the drain -- check that
# `chunklet_io_execution: enabled=false` in status) and any throughput delta is
# something else. ⛔ Discard any segment whose `flush:errors` delta is non-zero.
set -u

TAG=${1:?usage: uring_wait_ab.sh <tag> [config]}
CFG=${2:-/root/onyx_storage/config/nvme-chunklet.toml}
B=/root/onyx_storage/target/release/onyx-storage
OUT=/root/lv3run/$TAG
BURN=${BURN:-420}     # fresh-ring burst: throughput is 3-12x steady state before this
SEG=${SEG:-360}       # per-segment sampling window
RWMIX=${RWMIX:-0}
SEGMENTS=${SEGMENTS:-"A1 B1 A2 B2"}
ARM_A=${ARM_A:-"on 0"}
ARM_B=${ARM_B:-"off 0"}
# ATTACH=1 reuses an already-running engine instead of starting one. Preferred for
# a follow-up round: it keeps every segment inside the SAME process AND the same
# pool age, which is the whole point of the design. Also skips the slow open.
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

# One `set_arm` per segment boundary. Both knobs are re-asserted every time even
# when only one of them differs, so a segment's state never depends on which
# segment ran before it.
set_arm() {
    local arm="$1" seg="$2"
    local wait wave
    read -r wait wave <<<"$arm"
    {
        echo "arm '$arm'"
        "$B" -c "$CFG" uring-wait "$wait"
        "$B" -c "$CFG" uring-wave "$wave"
    } >"$OUT/knob.$seg" 2>&1
}

NSEG=$(echo "$SEGMENTS" | wc -w)
RUNTIME=$((BURN + NSEG * SEG + 240))
fio --name=onyx-uring --filename=/dev/ublkb0 --direct=1 --ioengine=io_uring \
    --rw=randrw --rwmixread="$RWMIX" --bsrange=4k-32k --iodepth=16 --numjobs=16 \
    --runtime="$RUNTIME" --time_based --group_reporting --refill_buffers \
    --write_bw_log="$OUT/bw" --log_avg_msec=5000 \
    >"$OUT/fio.log" 2>&1 &
{
    echo "fio pid $! runtime ${RUNTIME}s segments: $SEGMENTS"
    echo "A = '$ARM_A'  B = '$ARM_B'   (wait wave)"
} >"$OUT/plan"

date +%s >"$OUT/t_fio_start"
sleep "$BURN"
echo "burn done $(date -Is)" >>"$OUT/plan"

for seg in $SEGMENTS; do
    case "$seg" in
        A*) set_arm "$ARM_A" "$seg" ;;
        B*) set_arm "$ARM_B" "$seg" ;;
        *)  echo "unknown segment name $seg (must start with A or B)" >&2; exit 2 ;;
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
echo "  pkill -x fio   # wait for retired_depth to drain BEFORE stop, or the box wedges"
echo "  $B -c $CFG stop"
