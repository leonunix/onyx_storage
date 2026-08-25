#!/bin/bash
# A/B the LV2 global root-write LANE COUNT (`buffer.lv2_write_lanes`).
#
#   lv2_lanes_ab.sh <tag> [config] [arms...]
#
# WHY. Box-measured 2026-08-25 (RWMIX=0, QD256 j16d16, aged 256 GiB volume,
# 430 s burn, ledger closed to 91% with a 0.6% anchor error): the foreground
# append spends 6.71 ms inside the LV2 pipeline, and 75% of it is behind the
# lanes --
#
#   entry_write        3464 us  51.6%   the epoch payload write, ENTRY-weighted
#   prepared_queue     1588 us  23.7%   waiting for a lane to pick the batch up
#   staging_queue       414 us   6.2%
#   written_to_durable  414 us   6.2%   lane done -> watermark advance
#   group_collect       259 us   3.9%
#
# -- while the lanes are only 57% busy and the drives 74-94% idle. The lane
# count was hardcoded `min(shards, 8)` to "match the eight foreground ublk
# queues"; that queue model was replaced by one shared io-worker pool in
# 086a47a, so with 16 shards it pairs two shards per lane with no work stealing.
#
# RESULT of the first run of this script (arms 8/16/8, 16 shards):
#
#   metric                  lanes=8   lanes=16   lanes=8
#   append_total          8570 us    5386 us    9153 us   -39%
#   wait_durable p99     37749 us   12059 us   39846 us   -68%
#   entry_write           3304 us    1091 us    3561 us   -67%
#   prepared_queue        1503 us     448 us    1627 us   -70%
#   lane epochs            758 k     1792 k      738 k    2.4x
#   host MB/s              609.9      568.1      579.4    FLAT
#
# The 8-arms bracket the 16-arm within 8%, so the 2.3-3.3x is the knob. The
# default is now one lane per shard; re-run this to re-validate a change, and
# remember throughput is NOT what this knob buys (in-flight appends fell
# 159 -> 93 at fixed QD256 -- the appends stopped being the queue).
#
# ⚠ RESTART PER ARM. The knob is read at pool open, so this cannot be
# interleaved in one process like tools/lv3_submit_ab.sh. That makes HOST
# THROUGHPUT a weak judge on this box (it drifts 2x at constant clock), so the
# arms are ordered 8 -> 16 -> 8 to bracket the drift, and the verdict is the
# TIME DECOMPOSITION, in this order:
#
#   1. prepared_queue mean   did the wait for a lane shrink        (direct effect)
#   2. entry_write mean      did more concurrency shorten the write
#   3. STAGED_to_durable     did the total in-pipeline wait shrink
#   4. iostat aqu-sz/%util   did the DEVICE get deeper
#   5. host MiB/s            only believable if 1-3 moved together
#
# ⚠ Every arm pays ~240 s of meta-LD open before ublkb0 appears, then 430 s of
# burn: at RWMIX=0 a fresh LV2 ring runs 3-12x steady state for the first ~7 min.
set -u

TAG=${1:?usage: lv2_lanes_ab.sh <tag> [config] [arms...]}
CFG=${2:-/root/onyx_storage/config/nvme-chunklet.toml}
shift 2 2>/dev/null || shift $#
ARMS=${*:-"8 16 8"}

B=/root/onyx_storage/target/release/onyx-storage
OUT=/root/lv3run/$TAG
BURN=${BURN:-430}
SAMPLE=${SAMPLE:-240}
VOL=${VOL:-fio-volume}

mkdir -p "$OUT"
echo "arms: $ARMS   burn ${BURN}s   sample ${SAMPLE}s   config $CFG" | tee "$OUT/plan"

set_lanes() {
    # 0 = compiled default. Rewrite the knob in place, adding it under [buffer]
    # if absent.
    local lanes="$1"
    if grep -q '^lv2_write_lanes' "$CFG"; then
        sed -i "s/^lv2_write_lanes.*/lv2_write_lanes = $lanes/" "$CFG"
    else
        sed -i "0,/^\[buffer\]/s//[buffer]\nlv2_write_lanes = $lanes/" "$CFG"
    fi
    grep -n '^lv2_write_lanes' "$CFG"
}

# Arms are labelled `<index>.<lanes>` so a repeated arm (the drift bracket, e.g.
# "8 16 8") does not overwrite its own earlier output.
IDX=0
for lanes in $ARMS; do
    IDX=$((IDX + 1))
    arm="$IDX.$lanes"
    echo "=== arm $arm (lanes=$lanes)  $(date -Is) ===" | tee -a "$OUT/plan"
    pkill -x fio 2>/dev/null
    sleep 2
    "$B" -c "$CFG" stop >/dev/null 2>&1
    for _ in $(seq 1 40); do pgrep -x onyx-storage >/dev/null || break; sleep 5; done
    pgrep -x onyx-storage >/dev/null && { echo "FAIL: engine still up" >&2; exit 1; }

    set_lanes "$lanes" | tee -a "$OUT/plan"
    "$B" -c "$CFG" cleanup-ublk >/dev/null 2>&1
    sleep 3
    RUST_LOG=info nohup "$B" -c "$CFG" start -v "$VOL" >"$OUT/engine.$arm.log" 2>&1 &
    for _ in $(seq 1 60); do [ -e /dev/ublkb0 ] && break; sleep 10; done
    [ -e /dev/ublkb0 ] || { echo "FAIL: no ublkb0 for arm $arm" >&2; exit 1; }
    grep -m1 'LV2 global sync queue topology' "$OUT/engine.$arm.log" | tee -a "$OUT/plan"

    fio --name=onyx-lanes --filename=/dev/ublkb0 --direct=1 --ioengine=io_uring \
        --rw=randwrite --bs=4k-32k --iodepth=16 --numjobs=16 \
        --runtime=$((BURN + SAMPLE + 180)) --time_based --group_reporting \
        --refill_buffers --write_bw_log="$OUT/bw.$arm" --log_avg_msec=5000 \
        >"$OUT/fio.$arm.log" 2>&1 &
    sleep "$BURN"

    date +%s >"$OUT/t.$arm.start"
    "$B" -c "$CFG" status >"$OUT/status.$arm.start" 2>/dev/null
    python3 "$(dirname "$0")/lv2_stage_latency.py" "$SAMPLE" >"$OUT/lat.$arm" 2>&1 &
    LAT_PID=$!
    sleep $((SAMPLE - 40))
    iostat -xm 20 2 >"$OUT/iostat.$arm" 2>/dev/null
    wait $LAT_PID
    "$B" -c "$CFG" status >"$OUT/status.$arm.end" 2>/dev/null
    date +%s >"$OUT/t.$arm.end"
    echo "arm $arm (lanes=$lanes) done $(date -Is)" | tee -a "$OUT/plan"
done

pkill -x fio 2>/dev/null
set_lanes 0 | tee -a "$OUT/plan"
echo "done — knob restored to 0 (compiled default). Compare $OUT/lat.* ."
