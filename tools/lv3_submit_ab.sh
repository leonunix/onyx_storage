#!/bin/bash
# Interleaved A/B for the LV3 SUBMIT SHAPE — inside ONE process, one pool age.
#
#   lv3_submit_ab.sh <tag> [config]
#
# WHY THIS LOAD IS RWMIX=0. At rwmix 70/30 the LV3 ring sits at ~4% and its
# device leg is 17% of the writer's blocked time, so a submit-shape knob is
# unmeasurable there — that is how both knobs were previously recorded as
# "inert". Under pure write the ring pins at 100%, host throughput IS the drain
# rate, and the box measured a 22.3 ms RAID6 call as plan 1.70 + compute 3.92 +
# write 16.49 ms, of which 15.2 ms is 37 SEQUENTIAL waves each waiting 0.41 ms
# for 34 SQEs while the drives answer in 0.02-0.06 ms at aqu-sz 0.17-1.83.
#
# ARMS
#   B  baseline           uring-window 0   r6-pipeline 0   (per-wave barrier,
#                                                           two-phase writer)
#   A  windowed submit    uring-window $W  r6-pipeline 0
#   C  + pipelined parity uring-window $W  r6-pipeline $P
#
# ⚠ `uring-window` is in SQEs and must EXCEED the wave size (34 measured) or the
# window is NARROWER than the barrier it replaces. 8 is chunklet's test-suite
# stress value, not a perf value.
#
# ⚠ The knob is global to chunklet's uring backend, so LV2 foreground and metadb
# writes are windowed too. Their calls are tiny (foreground: 1.3 waves, 7.5 sqes)
# so the effect should be nil — but `chunklet_submit_foreground` is printed by
# tools/flush_delta.py and must be read, not assumed.
#
# JUDGE ON (all from tools/flush_delta.py + iostat, in this order):
#   drain_data waves/call, wait ms/wave, enters/call   did the barrier go away
#   r6_batch write ms/call, compute ms/call            did the call get shorter
#   iostat aqu-sz + %util per drive                    did the DEVICE get deeper
#   lv3 write MB/s, host MiB/s                         did it turn into throughput
# Host throughput is a valid judge HERE (unlike at 70/30) because the ring is
# pinned, so the foreground is drain-limited by construction.
set -u

TAG=${1:?usage: lv3_submit_ab.sh <tag> [config]}
CFG=${2:-/root/onyx_storage/config/nvme-chunklet.toml}
B=/root/onyx_storage/target/release/onyx-storage
OUT=/root/lv3run/$TAG
BURN=${BURN:-420}
SEG=${SEG:-180}
WINDOW=${WINDOW:-256}
PIPE=${PIPE:-4}
SEGMENTS=${SEGMENTS:-"B1 A1 B2 C1 B3 A2 B4 C2"}

mkdir -p "$OUT"
[ -e /dev/ublkb0 ] || { echo "FAIL: no /dev/ublkb0 — start the engine first" >&2; exit 1; }

set_arm() {
    local seg="$1" window="$2" pipe="$3"
    {
        echo "arm $seg: uring-window=$window r6-pipeline=$pipe"
        "$B" -c "$CFG" uring-window "$window"
        "$B" -c "$CFG" r6-pipeline "$pipe"
    } >"$OUT/knob.$seg" 2>&1
}

NSEG=$(echo "$SEGMENTS" | wc -w)
RUNTIME=$((BURN + NSEG * SEG + 240))
fio --name=onyx-lv3submit --filename=/dev/ublkb0 --direct=1 --ioengine=io_uring \
    --rw=randwrite --bs=4k-32k --iodepth=16 --numjobs=16 \
    --runtime="$RUNTIME" --time_based --group_reporting --refill_buffers \
    --write_bw_log="$OUT/bw" --log_avg_msec=5000 >"$OUT/fio.log" 2>&1 &
FIO_PID=$!
{
    echo "fio pid $FIO_PID runtime ${RUNTIME}s segments: $SEGMENTS"
    echo "B = barrier/two-phase   A = window $WINDOW   C = window $WINDOW + pipeline $PIPE"
} >"$OUT/plan"

# The ring has to pin before any segment: at RWMIX=0 it climbs 6% -> 100% over
# roughly the first 7 minutes, and a segment sampled during the climb measures
# burst length instead of drain rate.
sleep "$BURN"
echo "burn done $(date -Is)" >>"$OUT/plan"

for seg in $SEGMENTS; do
    case "$seg" in
        B*) set_arm "$seg" 0 0 ;;
        A*) set_arm "$seg" "$WINDOW" 0 ;;
        C*) set_arm "$seg" "$WINDOW" "$PIPE" ;;
        *)  echo "unknown segment $seg (must start with B, A or C)" >&2; exit 2 ;;
    esac
    date +%s >"$OUT/t.$seg.start"
    "$B" -c "$CFG" status >"$OUT/status.$seg.start" 2>/dev/null
    sleep $((SEG - 40))
    iostat -xm 20 2 >"$OUT/iostat.$seg" 2>/dev/null
    "$B" -c "$CFG" status >"$OUT/status.$seg.end" 2>/dev/null
    date +%s >"$OUT/t.$seg.end"
    echo "segment $seg done $(date -Is)" >>"$OUT/plan"
done

# Leave the knobs at the shipped default so a later run is not silently armed.
set_arm restore 0 0
echo "done; engine and fio still running. Discard any segment whose flush:errors delta is non-zero."
