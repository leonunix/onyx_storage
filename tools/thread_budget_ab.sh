#!/bin/bash
# A/B the two OVERSIZED thread pools: `ublk.io_workers` and
# `storage.read_pool_workers`.
#
#   thread_budget_ab.sh <tag> [config] [arms...]
#
#   arm = <io_workers>:<read_pool_workers>[:<lv3_dedicated_cores>]
#         the third field is optional and defaults to 0 (no dedication), so the
#         two-field arms recorded in memory `thread_budget_io_readpool_ab`
#         still mean what they meant.
#
# WHY. Box-measured 2026-09-15 (memory `thread_starvation_confirmed_census`),
# QD256 j16d16 randrw 70/30 on an aged 256 GiB volume, two censuses 20+ min in:
# the engine runs **614 OS threads with mean RUNNABLE 96.5-116.7 against 44
# NUMA-confined cores** (2.2-2.7x oversubscription), mpstat 92% with most cores
# 94-100% busy, and **195-220k involuntary context switches/s**. Starvation is
# confirmed, not inferred.
#
# ⭐ But the priority order is NOT the thread-count order. Per-group census:
#
#   group                thr   meanR    R%    nv/thr/s
#   ublk-io-worker-      128   30.8-36.6  24-29%   583-661   <- top contender
#   read-pool-fg          16    9.1-10.5  57-66%  1487-1604  <- most preempted
#   read-pool (bg)        16    6.3-7.0   39-44%   551-668
#   flusher, all 5 stages 112   15.1       8-38%    49-849
#   onyx-metadb-app       64    3.2-4.4    5-6.8%    26-33   <- parked, skip it
#   direct-io-submi      128    0.00       0%         0.0    <- ZERO cpu, skip it
#
# read_pool's 32 threads contribute as much runnable load as the ENTIRE
# 112-thread flusher pipeline, and read-pool-fg is the most-preempted group in
# the engine. Those are the two knobs worth cutting; the flusher shared pools
# and direct-io pool are not (see the memory note).
#
# ⚠ RESTART PER ARM. `io_workers` is read when the ublk device starts and
# `read_pool_workers` when the engine opens, so this cannot be interleaved in
# one process like tools/lv3_submit_ab.sh. Arms are ordered so the FIRST and
# LAST are the same baseline, bracketing this box's throughput drift (it moves
# 2x at constant clock -- see memory `two_regimes_healthy_vs_degraded`).
#
# ⚠ Restart-per-arm does NOT align LV2 ring state. `buffer_physical_fill_pct`
# is recorded per arm; a mismatch between arms is a CONFOUND, not a result
# (memory `range_lock_11us_per_key_is_wait_not_focus`). Note it is also
# ANTI-correlated with backlog, so do not read it as health.
#
# JUDGE ORDER. The census is the judge, because the census is what the change
# acts on. Throughput may well be flat -- the shared-pool series went 638->554
# threads for zero throughput change.
#
#   1. mean RUNNABLE total        did contention drop toward 44
#   2. involuntary switches/s     total, and per-thread for the cut groups
#   3. per-group meanR            ⭐ this attributes a COMBINED arm to each knob
#                                 separately: the io cut must show in
#                                 `ublk-io-worker-`, the read cut in `read-pool*`
#   4. lv3-batch-exec- nv/thr/s   the 9.65% compute inflation payoff (was 119.6)
#   5. mpstat %idle / %sys        26% sys at baseline; switch overhead
#   6. host MiB/s + p99           only believable if 1-3 moved together
#
# Each arm is SELF-PROVING: the effective pool sizes are grepped back out of
# the engine log and the arm aborts on a mismatch, so a typo cannot silently
# produce a duplicate baseline (memory `checkpoint_interval_duty_cycle_lockin`).
set -u

TAG=${1:?usage: thread_budget_ab.sh <tag> [config] [arms...]}
CFG=${2:-/root/onyx_storage/config/nvme-chunklet.toml}
shift 2 2>/dev/null || shift $#
# Default: baseline -> cut io -> cut io+read -> baseline again (drift bracket).
# 0 for io_workers means "nr_queues * queue_workers", i.e. the shipped 128.
ARMS=${*:-"0:32 32:32 32:12 0:32"}

B=/root/onyx_storage/target/release/onyx-storage
CENSUS="$(dirname "$0")/thread_census.py"
OUT=/root/p1/$TAG
BURN=${BURN:-420}          # randrw 70/30 pins the LV2 ring at ~6 min
SAMPLE=${SAMPLE:-150}
VOL=${VOL:-fio-volume}

mkdir -p "$OUT"
echo "arms: $ARMS   burn ${BURN}s   sample ${SAMPLE}s   config $CFG" | tee "$OUT/plan"

# Rewrite a knob in place. Inserts it under its section if the key is absent,
# and appends the section itself if that is missing too (`[cores]` is new, so a
# config predating it has no such header to insert after).
set_knob() {
    local key="$1" val="$2" section="$3"
    if grep -q "^${key}[[:space:]]*=" "$CFG"; then
        sed -i "s/^${key}[[:space:]]*=.*/${key} = ${val}/" "$CFG"
    elif grep -q "^\\[${section}\\]" "$CFG"; then
        sed -i "0,/^\\[${section}\\]/s//[${section}]\\n${key} = ${val}/" "$CFG"
    else
        # At EOF, so no later bare key can be captured by the new section.
        printf '\n[%s]\n%s = %s\n' "$section" "$key" "$val" >>"$CFG"
    fi
    grep -n "^${key}[[:space:]]*=" "$CFG"
}

IDX=0
for arm_spec in $ARMS; do
    IDX=$((IDX + 1))
    IFS=: read -r io rp lv3 <<<"$arm_spec"
    lv3=${lv3:-0}
    arm="$IDX.io$io-rp$rp-lv3$lv3"
    echo "=== arm $arm (io_workers=$io read_pool_workers=$rp lv3_dedicated_cores=$lv3)  $(date -Is) ===" \
        | tee -a "$OUT/plan"

    pkill -x fio 2>/dev/null
    sleep 2
    "$B" -c "$CFG" stop >/dev/null 2>&1
    for _ in $(seq 1 60); do pgrep -x onyx-storage >/dev/null || break; sleep 5; done
    pgrep -x onyx-storage >/dev/null && { echo "FAIL: engine still up" >&2; exit 1; }

    set_knob io_workers "$io" ublk | tee -a "$OUT/plan"
    set_knob read_pool_workers "$rp" storage | tee -a "$OUT/plan"
    set_knob lv3_dedicated_cores "$lv3" cores | tee -a "$OUT/plan"

    "$B" -c "$CFG" cleanup-ublk >/dev/null 2>&1
    sleep 3
    RUST_LOG=info nohup env RUST_LOG="info,onyx_storage::io::block_backend=error,onyx_chunklet::pool::ld_ops=error" \
        "$B" -c "$CFG" start -v "$VOL" >"$OUT/engine.$arm.log" 2>&1 &
    # A FILLED volume pays a long meta-LD bounded scan before the device appears.
    DEV=""
    for _ in $(seq 1 90); do
        DEV=$(ls /dev/ublkb* 2>/dev/null | head -1)
        [ -n "$DEV" ] && break
        sleep 5
    done
    [ -n "$DEV" ] || { echo "FAIL: no ublkb device for arm $arm" >&2; exit 1; }
    echo "device $DEV" | tee -a "$OUT/plan"

    # ── self-proof: the pools must actually be the size this arm asked for ──
    sleep 5
    eff_io=$(sed -E 's/\x1b\[[0-9;]*m//g' "$OUT/engine.$arm.log" \
        | grep -m1 'ublk shared io worker pool started' \
        | grep -oE 'workers=[0-9]+' | head -1 | cut -d= -f2)
    # Both ReadPool constructors log "read pool started with foreground
    # isolation" with `workers=N foreground_workers=N shared_workers=N`; `\b`
    # keeps the first match off the `*_workers=` siblings (`_` is a word char).
    eff_rp=$(sed -E 's/\x1b\[[0-9;]*m//g' "$OUT/engine.$arm.log" \
        | grep -m1 'read pool started with foreground isolation' \
        | grep -oE '\bworkers=[0-9]+' | head -1 | cut -d= -f2)
    want_io=$io
    [ "$io" = "0" ] && want_io=128      # nr_queues(32) * queue_workers(4)
    echo "effective: io_workers=$eff_io (want $want_io)  read_pool_workers=$eff_rp (want $rp)" \
        | tee -a "$OUT/plan"
    if [ "$eff_io" != "$want_io" ]; then
        echo "FAIL: arm $arm asked io_workers=$want_io but engine started $eff_io" >&2
        exit 1
    fi
    if [ -n "$eff_rp" ] && [ "$eff_rp" != "$rp" ]; then
        echo "FAIL: arm $arm asked read_pool_workers=$rp but engine started $eff_rp" >&2
        exit 1
    fi
    # `dedicated=[]` when nothing is carved out, `dedicated=[("Lv3Batch", [..])]`
    # otherwise. Proving this per arm matters more than the other two: a pin
    # that the stray-thread enforcer widens back would look identical in config
    # and produce a silent duplicate baseline.
    eff_ded=$(sed -E 's/\x1b\[[0-9;]*m//g' "$OUT/engine.$arm.log" \
        | grep -m1 'numa confine active' | grep -oE 'dedicated=\[[^]]*\]?[^ ]*' | head -1)
    echo "effective: $eff_ded" | tee -a "$OUT/plan"
    if [ "$lv3" = "0" ]; then
        case "$eff_ded" in
            'dedicated=[]') : ;;
            *) echo "FAIL: arm $arm asked for no dedication but got $eff_ded" >&2; exit 1 ;;
        esac
    else
        case "$eff_ded" in
            *Lv3Batch*) : ;;
            *) echo "FAIL: arm $arm asked lv3_dedicated_cores=$lv3 but got $eff_ded" >&2; exit 1 ;;
        esac
    fi
    P=$(pgrep -nx onyx-storage)
    echo "threads at start: $(ls /proc/$P/task | wc -l)" | tee -a "$OUT/plan"

    fio --name=onyx-threads --filename="$DEV" --direct=1 --ioengine=io_uring \
        --rw=randrw --rwmixread=70 --bsrange=4k-32k --iodepth=16 --numjobs=16 \
        --runtime=$((BURN + SAMPLE + 180)) --time_based --group_reporting \
        --refill_buffers --write_bw_log="$OUT/bw.$arm" --log_avg_msec=5000 \
        >"$OUT/fio.$arm.log" 2>&1 &
    sleep "$BURN"

    # ── measurement window ──
    date +%s >"$OUT/t.$arm.start"
    "$B" -c "$CFG" status >"$OUT/status.$arm.start" 2>/dev/null
    mpstat -P ALL 20 2 >"$OUT/mpstat.$arm" 2>&1 &
    MP=$!
    python3 "$CENSUS" "$P" --samples 150 --interval 0.2 --top 45 >"$OUT/census.$arm" 2>&1
    wait $MP
    "$B" -c "$CFG" status >"$OUT/status.$arm.end" 2>/dev/null
    date +%s >"$OUT/t.$arm.end"

    head -2 "$OUT/census.$arm" | tee -a "$OUT/plan"
    grep -E 'buffer_physical_fill_pct' "$OUT/status.$arm.end" | tee -a "$OUT/plan"
    echo "arm $arm done $(date -Is)" | tee -a "$OUT/plan"
done

pkill -x fio 2>/dev/null
# Restore what the repo ships so a later run does not inherit the last arm.
# ⚠ io_workers / read_pool_workers are now COMMITTED at 32 / 12 (they earned
# it — memory `thread_budget_io_readpool_ab`), so restore those values, not the
# pre-Phase-1 defaults. lv3_dedicated_cores ships at 0.
set_knob io_workers 32 ublk | tee -a "$OUT/plan"
set_knob read_pool_workers 12 storage | tee -a "$OUT/plan"
set_knob lv3_dedicated_cores 0 cores | tee -a "$OUT/plan"
echo "done — knobs restored to the shipped values. Compare $OUT/census.* ."
