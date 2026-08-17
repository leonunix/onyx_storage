#!/bin/bash
# One LV3-write measurement arm on nvme-box.
#
#   lv3_run.sh <tag> [config]
#
# Starts the engine, drives fio at QD256 (j16d16 — the only stable point, see
# memory qd_ladder_congestion_collapse), burns the fresh-ring burst phase, then
# takes two `status` samples so every ratio is a WINDOW DELTA rather than a
# cumulative average. Leaves the engine running for follow-up sampling; the
# caller stops it.
set -u

TAG=${1:?usage: lv3_run.sh <tag> [config]}
CFG=${2:-/root/onyx_storage/config/nvme-chunklet.toml}
B=/root/onyx_storage/target/release/onyx-storage
OUT=/root/lv3run/$TAG
BURN=${BURN:-420}
WINDOW=${WINDOW:-420}
# 70 = the standard mix. 0 = pure random write, which is how you make the LV2
# ring saturate so the DRAIN binds instead of the front end — at rwmixread=70 on
# a healthy pool the ring sits at 4% and LV3 write is demand-limited, so no LV3
# change can move it (box-measured 2026-08-14).
RWMIX=${RWMIX:-70}

mkdir -p "$OUT"
RUST_LOG=${RUST_LOG:-info} nohup "$B" -c "$CFG" start -v fio-volume >"$OUT/engine.log" 2>&1 &

for _ in $(seq 1 240); do
    [ -e /dev/ublkb0 ] && break
    sleep 2
done
if [ ! -e /dev/ublkb0 ]; then
    echo "FAIL: no /dev/ublkb0" >&2
    exit 1
fi
sleep 5

# --write_bw_log from the START: the whole allocator series ended with no
# end-to-end bandwidth number because fio was always killed before its summary.
fio --name=onyx-mix --filename=/dev/ublkb0 --direct=1 --ioengine=io_uring \
    --rw=randrw --rwmixread="$RWMIX" --bsrange=4k-32k --iodepth=16 --numjobs=16 \
    --runtime=$((BURN + WINDOW + 180)) --time_based --group_reporting \
    --refill_buffers --write_bw_log="$OUT/bw" --log_avg_msec=5000 \
    >"$OUT/fio.log" 2>&1 &
FIO=$!

sleep "$BURN"
"$B" -c "$CFG" status >"$OUT/status.A" 2>/dev/null
iostat -xm 5 2 >"$OUT/iostat.A" 2>/dev/null
sleep "$WINDOW"
"$B" -c "$CFG" status >"$OUT/status.B" 2>/dev/null
iostat -xm 5 2 >"$OUT/iostat.B" 2>/dev/null

echo "arm $TAG sampled; fio pid $FIO still running"
