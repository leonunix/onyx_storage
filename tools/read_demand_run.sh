#!/bin/bash
# One-arm measurement of what holds fio's queue depth at rwmix 70/30 — the
# "what pins WRITE DEMAND" question (memory onyx_write_demand_is_the_next_question).
#
#   read_demand_run.sh <tag> [config]
#
# Env: BURN (default 420 s, must be >= 6 min so the LV2 ring has pinned and the
# burst phase is over), SEG (window seconds per sample, default 240), QDS (space
# separated "<numjobs>d<iodepth>" ladder — the ladder is the in-process A/B for
# the server-concurrency hypothesis: if reads are capped by the ublk worker pool
# then read IOPS stops scaling once outstanding reads pass nr_queues*queue_workers,
# while `worker threads busy` pins at that cap), RWMIX (default 70).
#
# The engine must ALREADY be running with an aged/filled volume — reads on an
# unmapped volume are zero-IO inline and measure nothing.
set -u

TAG=${1:?usage: read_demand_run.sh <tag> [config]}
CFG=${2:-/root/onyx_storage/config/nvme-chunklet.toml}
B=/root/onyx_storage/target/release/onyx-storage
OUT=/root/readrun/$TAG
BURN=${BURN:-420}
SEG=${SEG:-240}
RWMIX=${RWMIX:-70}
QDS=${QDS:-"16d16"}

mkdir -p "$OUT"
if [ ! -e /dev/ublkb0 ]; then
    echo "FAIL: no /dev/ublkb0 — start the engine first" >&2
    exit 1
fi

# Arm spec: <numjobs>d<iodepth>[.<label>]. The optional label lets the same
# (numjobs, iodepth) pair appear twice — a repeated arm is how the run bounds
# this box's drift band.
for spec in $QDS; do
    geom=${spec%%.*}
    j=${geom%d*}
    q=${geom#*d}
    echo "=== arm $spec (numjobs=$j iodepth=$q rwmixread=$RWMIX) burn ${BURN}s window ${SEG}s"
    fio --name="onyx-$spec" --filename=/dev/ublkb0 --direct=1 --ioengine=io_uring \
        --rw=randrw --rwmixread="$RWMIX" --bsrange=4k-32k --iodepth="$q" --numjobs="$j" \
        --runtime=$((BURN + SEG + 60)) --time_based --group_reporting --refill_buffers \
        --write_bw_log="$OUT/bw.$spec" --log_avg_msec=5000 >"$OUT/fio.$spec" 2>&1 &
    fiopid=$!
    sleep "$BURN"
    "$B" -c "$CFG" status >"$OUT/status.$spec.start" 2>/dev/null
    date +%s >"$OUT/t.$spec.start"
    sleep $((SEG - 40))
    iostat -xm 20 2 >"$OUT/iostat.$spec" 2>/dev/null
    "$B" -c "$CFG" status >"$OUT/status.$spec.end" 2>/dev/null
    date +%s >"$OUT/t.$spec.end"
    wait $fiopid 2>/dev/null
    echo "arm $spec done $(date -Is)"
done
echo "analyse: tools/read_delta.py $OUT/status.<arm>.{start,end} <window_s> <cap>"
