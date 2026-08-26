#!/bin/bash
# In-process A/B of chunklet's two SUBMIT-SHAPE knobs on the LV2 mirror path.
#
#   uring_wave_window_ab.sh <tag> [config]
#
# WHY. Box 2026-08-26: onyx runs the ten NVMe drives at a time-averaged
# `aqu-sz` of 0.38-1.60 with `w_await` 0.03-0.06 ms and `%util` 9-35% — the
# drives answer in 40 us and are 70% idle, and there is essentially no queue.
# Inside a single LV2 mirror write (`tools/mirror_delta.py`), `submit` is 58-65%
# of the >= 5 ms tail: 5.2 ms to push 512 strip writes = 11.5 us per op, for
# 128 ops per drive, which is 128 x 40 us if they run one at a time.
#
# The code path that could explain that: `submit_writes_detailed_inline`
# publishes the batch in WAVES of `DEFAULT_WRITE_CHUNK_OPS = 64` and waits for
# ALL of each wave ("one submit_waves per stop-and-wait barrier"), so a 512-op
# mirror batch is 8 sequential barriers.
#
# ⭐ THE DISCRIMINATOR this script exists for. A wave of 64 spans 16 ops per
# drive; if those were concurrent a wave would cost ~one device latency and the
# whole batch ~0.5 ms, not 5.2 ms. So either
#
#   (a) the barrier IS the cost  -> `wave=256` (2 barriers) and `window=256`
#       (no barrier at all) both cut `submit` sharply, or
#   (b) the drives/queue really do serialise  -> `submit` barely moves and the
#       10x gap has to be looked for elsewhere.
#
# Nobody has run this on the MIRROR path. ⛔ Prior art on the RAID6/LV3 path,
# which shares the same global knobs: killing the 37-wave barrier there DOUBLED
# per-drive queue depth for ZERO throughput change
# ([[lv3_drain_is_coalesce_admission_bound]]), and both submit-shape knobs were
# separately falsified at 0% ([[submit_io_is_a_563_way_4k_fanout]]). So the
# THROUGHPUT prior is bad. This run is for the TIME DECOMPOSITION: whether the
# barrier explains the mirror's submit leg at all.
#
# ══ RESULT of the first run (2026-08-26, arms a1/a2/a3; the a4 bracket was LOST
#    to the fio-runtime bug fixed below, so absolute levels are unbracketed) ══
#
#   knob            submit mean  submit p50  aqu-sz  util  host MB/s  stripe_wait p50
#   wave 64 (base)     5298 us      4830      1.42    28%    592.5         84 us
#   wave 256           4763         4240      1.73    19%    555.2       1497
#   window 256         4529         3869      2.19    13%    436.6       1919
#
# ⛔ ANSWER: (b). Eight barriers -> two -> NONE moved `submit` only -10% and
# -15%, not -75%. The barrier is not the mirror's submit cost.
#
# ⭐ But the run paid for itself. Queue depth rose exactly as designed
# (1.42 -> 2.19) while util and host throughput FELL, and the reason is the last
# column: `stripe_wait` p50 exploded 18-23x. A deeper submit makes one mirror
# call hold its ~512 stripe-lock buckets across MORE device time, so concurrent
# callers serialise on the lock FOOTPRINT — depth up, caller concurrency down.
# That also explains why chunklet-standalone reaches aqu-sz 9 and the 2.16%
# window probe never transferred: the standalone probe writes raw devices and
# has NO stripe locks. ⇒ Bound the mirror's lock footprint FIRST
# ([[stripe_lock_footprint_scales_with_call_size]] did it for RAID6); only then
# is re-testing a submit shape meaningful.
#
# ⚠ Both knobs are clamped to `URING_DEPTH` = 256 (a wave must fit the SQ
# atomically), so 256 is the ceiling without also growing the ring — and 256 is
# already enough to turn a 512-op mirror batch's 8 barriers into 2. Raising
# `URING_DEPTH` costs ~800 KiB per THREAD (rings are thread-local) and no
# observed batch exceeds ~1632 ops, so 8192 would be far past any use.
#
# ⚠ `window` must EXCEED the wave size to deepen the queue; a window narrower
# than the barrier it replaces is a REGRESSION. Hence window=256 vs wave=64.
#
# ⭐ Arms run in ONE process at ONE pool age, flipped over the IPC socket. Both
# knobs are read once per batch, so a batch already in flight finishes under the
# value it started with. This is the only shape of A/B on this box that is not
# contaminated by restart-order drift, which is why the arms are knob flips and
# not process restarts.
set -u

TAG=${1:?usage: uring_wave_window_ab.sh <tag> [config]}
CFG=${2:-/root/onyx_storage/config/nvme-chunklet.toml}

B=/root/onyx_storage/target/release/onyx-storage
OUT=/root/lv3run/$TAG
BURN=${BURN:-400}
SAMPLE=${SAMPLE:-240}
LOG=${LOG:?set LOG=<engine.log the running engine is appending to>}

mkdir -p "$OUT"
[ -e /dev/ublkb0 ] || { echo "FAIL: no /dev/ublkb0 — start the engine first" >&2; exit 1; }
pgrep -x onyx-storage >/dev/null || { echo "FAIL: engine not running" >&2; exit 1; }

# arm = "<label> <wave> <window>"; the two 64/0 arms bracket in-process drift.
ARMS=(
    "a1.base    0   0"
    "a2.wave256 256 0"
    "a3.win256  0   256"
    "a4.base    0   0"
)

echo "log=$LOG  burn ${BURN}s  sample ${SAMPLE}s" | tee "$OUT/plan"
"$B" -c "$CFG" uring-wave 0   >>"$OUT/plan" 2>&1
"$B" -c "$CFG" uring-window 0 >>"$OUT/plan" 2>&1

pkill -x fio 2>/dev/null
sleep 2
# ⚠ fio's runtime must outlive the WHOLE arm sequence including the per-arm
# analysis, which is NOT free: mirror_delta re-scans a log that keeps growing,
# and the first run of this script lost its bracket arm because fio died 74 s
# into it. Analysis is deferred to after the loop now, but keep the runtime
# generous anyway — the final `pkill -x fio` is what actually stops it.
fio --name=onyx-wave --filename=/dev/ublkb0 --direct=1 --ioengine=io_uring \
    --rw=randwrite --bs=4k-32k --iodepth=16 --numjobs=16 \
    --runtime=$((BURN + 4 * (SAMPLE + 60) + 900)) --time_based --group_reporting \
    --refill_buffers --write_bw_log="$OUT/bw" --log_avg_msec=5000 \
    >"$OUT/fio.log" 2>&1 &
echo "burning ${BURN}s so the LV2 ring pins before the first arm" | tee -a "$OUT/plan"
sleep "$BURN"

for arm in "${ARMS[@]}"; do
    set -- $arm
    label=$1; wave=$2; window=$3
    echo "=== $label wave=$wave window=$window $(date -Is) ===" | tee -a "$OUT/plan"
    # Order matters: clear the window BEFORE setting a wave (a non-zero window
    # disables wave chunking entirely, so the wave value would be dead).
    "$B" -c "$CFG" uring-window "$window" | tee -a "$OUT/plan"
    "$B" -c "$CFG" uring-wave "$wave"     | tee -a "$OUT/plan"
    # Let batches in flight under the previous value drain out of the window.
    sleep 20
    date -u +%Y-%m-%dT%H:%M:%S >"$OUT/t.$label.from"
    "$B" -c "$CFG" status >"$OUT/status.$label.start" 2>/dev/null
    sleep $((SAMPLE - 40))
    iostat -xm 20 2 >"$OUT/iostat.$label" 2>/dev/null
    date -u +%Y-%m-%dT%H:%M:%S >"$OUT/t.$label.to"
    "$B" -c "$CFG" status >"$OUT/status.$label.end" 2>/dev/null
    echo "$label sampled $(cat "$OUT/t.$label.from") .. $(cat "$OUT/t.$label.to")" \
        | tee -a "$OUT/plan"
done

# Analysis AFTER the arms, never between them: scanning a multi-hundred-MB log
# takes minutes and that time silently ate the bracket arm's load on the first
# run of this script.
pkill -x fio 2>/dev/null
for arm in "${ARMS[@]}"; do
    set -- $arm
    label=$1
    python3 "$(dirname "$0")/mirror_delta.py" "$LOG" \
        --from "$(cat "$OUT/t.$label.from")" --to "$(cat "$OUT/t.$label.to")" \
        >"$OUT/mirror.$label" 2>&1
    python3 "$(dirname "$0")/front_delta.py" "$OUT/status.$label.start" \
        "$OUT/status.$label.end" "$SAMPLE" >"$OUT/front.$label" 2>&1
    echo "--- $label ---" | tee -a "$OUT/plan"
    grep -E "submit_us|total_us|stripe_wait|events" "$OUT/mirror.$label" | tee -a "$OUT/plan"
    grep -E "bytes/s|append_total|wait_durable" "$OUT/front.$label" | tee -a "$OUT/plan"
done
"$B" -c "$CFG" uring-window 0 | tee -a "$OUT/plan"
"$B" -c "$CFG" uring-wave 0   | tee -a "$OUT/plan"
echo "done — knobs restored to defaults (wave=64, window=off)."
echo "verdict: $OUT/mirror.*   front: $OUT/front.*   queue depth: $OUT/iostat.*"
