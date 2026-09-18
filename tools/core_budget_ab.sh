#!/bin/bash
# Interleaved A/B for the confine CORE BUDGET — inside ONE process, one pool age.
#
#   core_budget_ab.sh <tag> [config]
#
# WHY INTERLEAVED. `cores.lv2_dedicated_cores` and `numa.reserve_cores_per_node`
# are nothing but CPU masks, so they have no business costing a restart. The
# restart-per-arm harness (tools/thread_budget_ab.sh) pays engine stop/start +
# a burn per arm; at 8 arms that is ~44 min of overhead to buy 20 min of
# measurement. Here one burn covers the whole batch:
#
#   restart-per-arm   8 x (150 burn + 150 sample + ~30 restart) = 44 min
#   interleaved       150 burn + 8 x 180                        = 27 min
#
# and, more importantly, the LV2 ring state is CONTINUOUS across arms instead of
# being reset by every restart — which removes the confound memory
# `range_lock_11us_per_key_is_wait_not_footprint` warns about and that showed up
# as `fill%` 25/35/33 spread across supposedly identical arms.
#
# HOW IT IS SAFE. `onyx-storage cores <lv2> <lv3> <reserve>` rebuilds the budget
# and swaps it into the live layout. Already-running threads are re-pinned by
# the stray-thread enforcer in src/numa.rs, whose test is EXACT set equality —
# so it converges whether the new budget is narrower (a dedication appears) or
# wider (one goes away). Convergence takes up to one 5 s sweep; this script
# waits SETTLE=15 s (three sweeps) before opening a window.
#
# ⚠ The rebuild reads the config SNAPSHOT taken at confine setup, never the file
# on disk. So unlike thread_budget_ab.sh this script does NOT rewrite the
# config, which makes it rsync-safe while it runs.
#
# ARMS (each 180 s, `cores <lv2> <lv3> <reserve>`)
#   B  baseline        cores 0 0 2   engine_cpus 44
#   R  reserve back    cores 0 0 0   engine_cpus 48  — grow the denominator
#   S  reserve back x1 cores 0 0 1   engine_cpus 46  — the never-measured middle
#   D  LV2 dedicated   cores 5 0 0   engine_cpus 48 + BufferSync owns 5 cores
#
# JUDGE ON, in this order:
#   1. persistent-slot nv/thr/s      the MECHANISM. Box-measured 60-70% lower
#                                    with the dedication EVERY time (memory
#                                    `lv2_dedicated_cores_plus_reserve_giveback`).
#                                    If this does not move, the arm did not
#                                    happen — do not read anything else.
#   2. total involuntary sw/s        a dedication RAISES this while REMOVING it
#                                    from the critical path; that is the trade.
#   3. host MiB/s vs the THREE B     report the spread of the B segments FIRST.
#      segments                      The box resolved to 5.62% on 2026-09-17;
#                                    the expected effect is +5.4%, i.e. right at
#                                    the edge, so a magnitude is only claimable
#                                    if the B segments are tight.
#   4. buffer_physical_fill_pct      recorded per segment; continuous here, so a
#                                    jump means something else moved.
#
# ⛔ Do NOT clear the pool to "get a clean run". The spread this box can resolve
# is a property of an AGED pool and cost 3.5 h of soak to reach.
set -u

TAG=${1:?usage: core_budget_ab.sh <tag> [config]}
CFG=${2:-/root/onyx_storage/config/nvme-chunklet.toml}

B=/root/onyx_storage/target/release/onyx-storage
CENSUS="$(dirname "$0")/thread_census.py"
OUT=/root/p1/$TAG
# 150, not 420: on an aged pool the post-restart burst is 1.17x and lasts ~60 s.
# See the calibration comment in tools/thread_budget_ab.sh. Only ONE burn is
# paid here anyway, so this is not where the time goes.
BURN=${BURN:-150}
SEG=${SEG:-180}
SETTLE=${SETTLE:-15}
SEGMENTS=${SEGMENTS:-"B1 R1 D1 B2 D2 S1 B3 D3"}

mkdir -p "$OUT"
DEV=$(ls /dev/ublkb* 2>/dev/null | head -1)
[ -n "$DEV" ] || { echo "FAIL: no /dev/ublkb* — start the engine first" >&2; exit 1; }
P=$(pgrep -nx onyx-storage) || { echo "FAIL: engine not running" >&2; exit 1; }

# Prove confine is even active before burning 150 s: `cores` reports
# "inactive" under any other layout, and every arm below would then be a
# duplicate baseline.
before=$("$B" -c "$CFG" cores 2>&1 | head -1)
case "$before" in
    *inactive*|*error*)
        echo "FAIL: core budget not available — engine says: $before" >&2
        exit 1 ;;
esac
{
    echo "device $DEV  pid $P  segments: $SEGMENTS"
    echo "burn ${BURN}s  segment ${SEG}s  settle ${SETTLE}s"
    echo "budget at start: $before"
} | tee "$OUT/plan"

# Each segment SELF-PROVES: the effective budget is read back off the live
# object, and a mismatch aborts rather than producing a silent duplicate arm
# (memory `checkpoint_interval_duty_cycle_lockin`).
set_arm() {
    local seg="$1" lv2="$2" lv3="$3" rsv="$4" want_cpus="$5"
    local got
    got=$("$B" -c "$CFG" cores "$lv2" "$lv3" "$rsv" 2>&1 | head -1)
    echo "arm $seg: requested lv2=$lv2 lv3=$lv3 reserve=$rsv -> $got" | tee -a "$OUT/plan"
    case "$got" in
        *"engine_cpus=$want_cpus"*) : ;;
        *) echo "FAIL: $seg wanted engine_cpus=$want_cpus, engine reports: $got" >&2; exit 1 ;;
    esac
    if [ "$lv2" = "0" ]; then
        case "$got" in
            *BufferSync*) echo "FAIL: $seg wanted no LV2 dedication, got: $got" >&2; exit 1 ;;
        esac
    else
        case "$got" in
            *BufferSync*) : ;;
            *) echo "FAIL: $seg wanted an LV2 dedication, got: $got" >&2; exit 1 ;;
        esac
    fi
    echo "$got" >"$OUT/budget.$seg"
}

NSEG=$(echo "$SEGMENTS" | wc -w)
RUNTIME=$((BURN + NSEG * (SEG + SETTLE) + 240))
fio --name=onyx-corebudget --filename="$DEV" --direct=1 --ioengine=io_uring \
    --rw=randrw --rwmixread=70 --bsrange=4k-32k --iodepth=16 --numjobs=16 \
    --runtime="$RUNTIME" --time_based --group_reporting --refill_buffers \
    --write_bw_log="$OUT/bw" --log_avg_msec=5000 >"$OUT/fio.log" 2>&1 &
echo "fio runtime ${RUNTIME}s" | tee -a "$OUT/plan"

sleep "$BURN"
echo "burn done $(date -Is)" | tee -a "$OUT/plan"

for seg in $SEGMENTS; do
    case "$seg" in
        B*) set_arm "$seg" 0 0 2 44 ;;
        R*) set_arm "$seg" 0 0 0 48 ;;
        S*) set_arm "$seg" 0 0 1 46 ;;
        D*) set_arm "$seg" 5 0 0 48 ;;
        *)  echo "unknown segment $seg (must start with B, R, S or D)" >&2; exit 2 ;;
    esac
    # Let the enforcer converge every already-running thread onto the new masks
    # before anything is measured. Three sweeps.
    sleep "$SETTLE"

    date +%s >"$OUT/t.$seg.start"
    "$B" -c "$CFG" status >"$OUT/status.$seg.start" 2>/dev/null
    python3 "$CENSUS" "$P" --samples 150 --interval 0.2 --top 45 >"$OUT/census.$seg" 2>&1
    "$B" -c "$CFG" status >"$OUT/status.$seg.end" 2>/dev/null
    date +%s >"$OUT/t.$seg.end"

    head -2 "$OUT/census.$seg" | tee -a "$OUT/plan"
    grep -E 'buffer_physical_fill_pct' "$OUT/status.$seg.end" | tee -a "$OUT/plan"
    echo "segment $seg done $(date -Is)" | tee -a "$OUT/plan"
done

pkill -x fio 2>/dev/null
# Back to the shipped budget so a later run is not silently armed. The config
# was never touched, so this is the only state to restore.
set_arm restore 0 0 2 44
echo "done — budget restored. Discard any segment whose flush errors delta is non-zero."
