#!/bin/bash
# Interleaved A/B for design D — PBA-contiguous LV3 write bundles — INSIDE ONE
# PROCESS.
#
#   stripe_run_ab.sh <tag> [config]
#
# What is under test. A passthrough batch used to emit one write op per RAID
# stripe, each with its own stripe-wide allocation, so consecutive stripes landed
# at unrelated PBAs: box 2026-08-19 measured 285 ops -> 2277 x 4 KiB strip writes
# -> 1245 SQEs (adjacency merge 1.9x) and `r6.write` = 16.2 ms of a 28.8 ms call.
# Neither syscall-shaped fix moved it (memory
# lv3_submit_knobs_ab_syscall_model_falsified): they changed how the same IOs are
# waited on, not how many there are.
#
#   D1  `stripe-run W`         up to W consecutive exactly-full stripe groups
#                              share ONE contiguous extent, one buffer, one op.
#   D2  `refill-width-bias on` refill the lane cache from the region's WIDEST free
#                              runs, so a bundle can actually reach W. Without it
#                              the supply is ~2.1 stripes/run on an aged pool.
#
# D1 alone is bounded by supply, so the arms are off / D1 / D1+D2 and the
# discriminator is per-call op count, NOT throughput:
#
#   chunklet_r6_batch      ops/call must fall ~= mean bundle width, stripes/call
#                          must NOT move (same parity work, same stripe locks)
#   chunklet_submit_data   sqes/call down, merge x up, enters/call down
#   allocator_stripe_runs  stripes_per_run = the mean bundle width achieved
#   allocator_supply       blocks_per_run = the supply D2 is meant to widen
#   allocator_contiguity   largest_run — ⚠ D2 consumes wide material FIRST, so
#                          watch this decay across segments; a lever that eats its
#                          own supply looks great for one window and then stops
#
# Throughput is reported and never used as the gate (memory
# qd_ladder_congestion_collapse). ⛔ Discard any segment whose `flush:errors` delta
# is non-zero — the pool hits the reclaim wall late in every run.
#
# Interleaved and same-process because arm-per-restart on this box measures
# run-order drift, not the knob (two identical baseline arms once 2.13x apart).
# Both knobs are runtime-flippable; the arena class table is sized at OPEN from
# `flush.stripe_run_max_stripes`, so set that in the config to the widest W any arm
# will use or wide bundles fall back to the heap (`mem_arena.overflow`).
set -u

TAG=${1:?usage: stripe_run_ab.sh <tag> [config]}
CFG=${2:-/root/onyx_storage/config/nvme-chunklet.toml}
B=/root/onyx_storage/target/release/onyx-storage
OUT=/root/lv3run/$TAG
BURN=${BURN:-420}
SEG=${SEG:-150}
RWMIX=${RWMIX:-0}
WIDTH=${WIDTH:-32}
# Arm names: A* = D1, B* = baseline, C* = D1+D2. Order is the caller's business;
# keep each arm adjacent to the other so mean(X_i, X_i+1) - Y cancels the ageing
# trend without fitting a slope.
SEGMENTS=${SEGMENTS:-"B1 A1 B2 A2 C1 B3 C2 A3"}
ATTACH=${ATTACH:-0}

mkdir -p "$OUT"
if [ "$ATTACH" = "0" ]; then
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

# Both knobs are re-asserted at every boundary, so a segment's state never depends
# on which segment ran before it.
set_arm() {
    local seg="$1" run="$2" bias="$3"
    {
        echo "arm: stripe-run=$run refill-width-bias=$bias"
        "$B" -c "$CFG" stripe-run "$run"
        "$B" -c "$CFG" refill-width-bias "$bias"
    } >"$OUT/knob.$seg" 2>&1
}

NSEG=$(echo "$SEGMENTS" | wc -w)
RUNTIME=$((BURN + NSEG * SEG + 240))
fio --name=onyx-striperun --filename=/dev/ublkb0 --direct=1 --ioengine=io_uring \
    --rw=randrw --rwmixread="$RWMIX" --bsrange=4k-32k --iodepth=16 --numjobs=16 \
    --runtime="$RUNTIME" --time_based --group_reporting --refill_buffers \
    --write_bw_log="$OUT/bw" --log_avg_msec=5000 \
    >"$OUT/fio.log" 2>&1 &
{
    echo "fio pid $! runtime ${RUNTIME}s segments: $SEGMENTS"
    echo "A = stripe-run $WIDTH / bias off   B = stripe-run 1 / bias off   C = stripe-run $WIDTH / bias on"
} >"$OUT/plan"

date +%s >"$OUT/t_fio_start"
sleep "$BURN"
echo "burn done $(date -Is)" >>"$OUT/plan"

for seg in $SEGMENTS; do
    case "$seg" in
        A*) set_arm "$seg" "$WIDTH" off ;;
        B*) set_arm "$seg" 1 off ;;
        C*) set_arm "$seg" "$WIDTH" on ;;
        *)  echo "unknown segment $seg (must start with A, B or C)" >&2; exit 2 ;;
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
echo "  pkill -x fio   # then WAIT for retired_depth to drain before stop, or the box wedges"
echo "  $B -c $CFG stop"
