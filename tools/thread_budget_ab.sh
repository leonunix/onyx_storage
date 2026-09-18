#!/bin/bash
# A/B the two OVERSIZED thread pools: `ublk.io_workers` and
# `storage.read_pool_workers`.
#
#   thread_budget_ab.sh <tag> [config] [arms...]
#
#   arm = <io_workers>:<read_pool_workers>[:<lv3_cores>[:<lv2_cores>[:<reserve_cores>[:<nr_queues>[:<coalesce_drivers>[:<page_write_workers>]]]]]]
#         Trailing fields are optional and default to the PRE-2026-09-18 shipped
#         values (lv3=0, lv2=0, reserve=2, nr_queues=32, coalesce_drivers=0,
#         page_write_workers=0=compiled 32), so the shorter arms recorded in
#         memory `thread_budget_io_readpool_ab` / `lv3_dedicated_cores_zero_sum`
#         still mean what they meant. ⚠ The config now SHIPS the cut values, so
#         a baseline arm has to state the old ones explicitly.
#
#   page_write_workers is `meta.page_write_workers`, the `metaio-*` pool (32
#   threads at 0/default, 2.41 meanR / maxR 20 / 7.5% duty on the box). ⚠ It is
#   the checkpoint's DEVICE parallelism -- one io_uring per worker -- and a
#   metadb checkpoint is the only thing that releases LV2 ring space, so judge
#   it on `metadb_commit.total_us` + release cadence before thread count.
#
#   coalesce_drivers is `flush.coalesce_pool_workers`; 0 leaves
#   `shared_coalesce_pool` OFF (one dedicated admission thread per shard).
#   Any N > 0 turns the pool on with N drivers.
#
#   nr_queues is `ublk.nr_queues`, the kernel device's queue count, and each
#   queue costs one libublk `ublk-<vol>` OS thread (33 threads at 32, 9 at 8).
#   ⚠ It is ALSO the default source for `io_workers` (`nr_queues *
#   queue_workers`) and for the direct-io submit pool, so an arm that moves it
#   must pin `io_workers` explicitly — otherwise two knobs move at once and the
#   census cannot attribute either. Pass io_workers=32 (the shipped value) on
#   every nr_queues arm; the self-proof below enforces that they are consistent.
#
#   reserve_cores is `numa.reserve_cores_per_node`. Lowering it GROWS the
#   budget's denominator (44 logical CPUs at 2, 46 at 1, 48 at 0 on this box),
#   which is the only way a dedication stops being strictly zero-sum — see the
#   note on `cores.lv2_dedicated_cores`.
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
# ⭐ 150, not 420. Box 2026-09-17: the 420 s figure was calibrated on a FRESH
# pool, where the burst is 3-12x steady state. An engine restart clears the LV2
# ring but NOT the pool, so on an AGED pool -- the only kind we A/B on -- the
# ring refills fast and the burst is both short and shallow. Measured off p8's
# own 5 s bw buckets (three identical arms, 30 s window means):
#
#   arm 1  662 650 | 542 543 565 544 561 538 562 608 551 552 531 571 582 578
#   arm 2  591 563 | 543 547 561 567 566 555 571 553 569 546 569 553 575 566
#   arm 3  597 587 | 558 551 557 558 569 575 543 538 541 546 559 561 576 547
#
# Steady from window 3 (t=60 s); burst is 1.17x, not 3-12x. Sampling t=60-270
# gives 556.0 MiB/s against 560.6 for the t=240-485 window the 420 s burn
# actually measured -- a 0.8% difference, and the EARLY windows are tighter
# across arms (1.4% vs 2.7%). So 150 carries 2.5x margin over the observed
# burst and costs nothing.
#
# ⚠ SCOPED TO randrw 70/30 ON AN AGED POOL. At RWMIX=0 the ring climbs
# 6% -> 100% over ~7 minutes (see tools/lv3_submit_ab.sh) and 420 still stands;
# on a fresh pool it stands too.
BURN=${BURN:-150}
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
    IFS=: read -r io rp lv3 lv2 rsv nrq cpool pww <<<"$arm_spec"
    lv3=${lv3:-0}; lv2=${lv2:-0}; rsv=${rsv:-2}; nrq=${nrq:-32}; cpool=${cpool:-0}; pww=${pww:-0}
    arm="$IDX.io$io-rp$rp-lv3$lv3-lv2$lv2-rsv$rsv-nrq$nrq-cp$cpool-pww$pww"
    echo "=== arm $arm (io_workers=$io read_pool_workers=$rp lv3_cores=$lv3 lv2_cores=$lv2 reserve_cores=$rsv nr_queues=$nrq coalesce_drivers=$cpool page_write_workers=$pww)  $(date -Is) ===" \
        | tee -a "$OUT/plan"
    if [ "$nrq" != "32" ] && [ "$io" = "0" ]; then
        echo "FAIL: arm $arm moves nr_queues with io_workers=0, so io_workers would move too" >&2
        exit 1
    fi

    pkill -x fio 2>/dev/null
    sleep 2
    "$B" -c "$CFG" stop >/dev/null 2>&1
    for _ in $(seq 1 60); do pgrep -x onyx-storage >/dev/null || break; sleep 5; done
    pgrep -x onyx-storage >/dev/null && { echo "FAIL: engine still up" >&2; exit 1; }

    set_knob io_workers "$io" ublk | tee -a "$OUT/plan"
    set_knob read_pool_workers "$rp" storage | tee -a "$OUT/plan"
    set_knob lv3_dedicated_cores "$lv3" cores | tee -a "$OUT/plan"
    set_knob lv2_dedicated_cores "$lv2" cores | tee -a "$OUT/plan"
    set_knob reserve_cores_per_node "$rsv" numa | tee -a "$OUT/plan"
    set_knob nr_queues "$nrq" ublk | tee -a "$OUT/plan"
    set_knob page_write_workers "$pww" meta | tee -a "$OUT/plan"
    if [ "$cpool" = "0" ]; then
        set_knob shared_coalesce_pool false flush | tee -a "$OUT/plan"
        set_knob coalesce_pool_workers 0 flush | tee -a "$OUT/plan"
    else
        set_knob shared_coalesce_pool true flush | tee -a "$OUT/plan"
        set_knob coalesce_pool_workers "$cpool" flush | tee -a "$OUT/plan"
    fi

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
    [ "$io" = "0" ] && want_io=$((nrq * 4))   # nr_queues * queue_workers(4)
    # Same log line carries `nr_queues`, so the device geometry proves itself
    # from the engine's own view rather than from the config we just wrote.
    eff_nrq=$(sed -E 's/\x1b\[[0-9;]*m//g' "$OUT/engine.$arm.log" \
        | grep -m1 'ublk shared io worker pool started' \
        | grep -oE 'nr_queues=[0-9]+' | head -1 | cut -d= -f2)
    echo "effective: io_workers=$eff_io (want $want_io)  read_pool_workers=$eff_rp (want $rp)  nr_queues=$eff_nrq (want $nrq)" \
        | tee -a "$OUT/plan"
    if [ "$eff_io" != "$want_io" ]; then
        echo "FAIL: arm $arm asked io_workers=$want_io but engine started $eff_io" >&2
        exit 1
    fi
    if [ "$eff_nrq" != "$nrq" ]; then
        echo "FAIL: arm $arm asked nr_queues=$nrq but engine started $eff_nrq" >&2
        exit 1
    fi
    # The admission pool logs `drivers=N lanes=M` only when it is ON, so its
    # absence IS the proof for a `cpool=0` arm. Without this, a config typo
    # would produce a silent duplicate baseline.
    eff_cpool=$(sed -E 's/\x1b\[[0-9;]*m//g' "$OUT/engine.$arm.log" \
        | grep -m1 'flusher shared coalesce pool started' \
        | grep -oE 'drivers=[0-9]+' | head -1 | cut -d= -f2)
    echo "effective: coalesce_drivers=${eff_cpool:-off} (want ${cpool})" | tee -a "$OUT/plan"
    if [ "$cpool" = "0" ]; then
        if [ -n "$eff_cpool" ]; then
            echo "FAIL: arm $arm wanted no coalesce pool, engine started $eff_cpool drivers" >&2
            exit 1
        fi
    elif [ "$eff_cpool" != "$cpool" ]; then
        echo "FAIL: arm $arm asked coalesce_drivers=$cpool but engine started ${eff_cpool:-off}" >&2
        exit 1
    fi
    # The page-write pool logs its resolved size at open, so `0` proves itself
    # as the compiled 32 rather than as "absent".
    eff_pww=$(sed -E 's/\x1b\[[0-9;]*m//g' "$OUT/engine.$arm.log" \
        | grep -m1 'metadb meta-LD page-write pool sized' \
        | grep -oE 'page_write_workers=[0-9]+' | head -1 | cut -d= -f2)
    want_pww=$pww
    [ "$pww" = "0" ] && want_pww=32
    echo "effective: page_write_workers=${eff_pww:-none} (want $want_pww)" | tee -a "$OUT/plan"
    if [ -z "$eff_pww" ]; then
        echo "FAIL: arm $arm found no page-write pool line -- the box binary predates meta.page_write_workers; rebuild it" >&2
        exit 1
    fi
    if [ "$eff_pww" != "$want_pww" ]; then
        echo "FAIL: arm $arm asked page_write_workers=$want_pww but engine started $eff_pww" >&2
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
    confine_line=$(sed -E 's/\x1b\[[0-9;]*m//g' "$OUT/engine.$arm.log" \
        | grep -m1 'numa confine active')
    eff_ded=$(echo "$confine_line" | grep -oE 'dedicated=\[.*\] direct_io_cpus' \
        | sed 's/ direct_io_cpus//')
    # engine_cpus is the budget denominator; `reserve_cores` shows the knob the
    # engine actually read back. Counting the list proves the reserve arm did
    # something (44 CPUs at reserve=2, 46 at 1, 48 at 0 on this box).
    eff_engine=$(echo "$confine_line" | grep -oE 'engine_cpus=\[[^]]*\]' \
        | tr ',' '\n' | grep -c '[0-9]')
    eff_rsv=$(echo "$confine_line" | grep -oE 'reserved_cores=[0-9]+' | cut -d= -f2)
    echo "effective: $eff_ded | engine_cpus=$eff_engine reserved_cores=$eff_rsv" \
        | tee -a "$OUT/plan"
    if [ "$eff_rsv" != "$rsv" ]; then
        echo "FAIL: arm $arm asked reserve_cores_per_node=$rsv but engine read $eff_rsv" >&2
        exit 1
    fi
    for pair in "Lv3Batch:$lv3" "BufferSync:$lv2"; do
        role=${pair%%:*}; want=${pair##*:}
        if [ "$want" = "0" ]; then
            case "$eff_ded" in
                *"$role"*) echo "FAIL: arm $arm wanted no $role dedication, got $eff_ded" >&2; exit 1 ;;
            esac
        else
            case "$eff_ded" in
                *"$role"*) : ;;
                *) echo "FAIL: arm $arm wanted $role x$want cores, got $eff_ded" >&2; exit 1 ;;
            esac
        fi
    done
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
# Restore what the repo SHIPS so a later run does not inherit the last arm.
# ⚠ Keep this list in sync with config/nvme-chunklet.toml whenever a cut earns
# its way in — a stale restore silently leaves the box on an older baseline.
# Committed cuts so far: io_workers 32 + read_pool_workers 12 (memory
# `thread_budget_io_readpool_ab`), nr_queues 8 + the shared coalesce pool
# (`combined_screen_nrq8_plus_coalesce_pool`), page_write_workers 16. Every
# `cores.*` dedication ships at 0 (both directions measured, both lose).
set_knob io_workers 32 ublk | tee -a "$OUT/plan"
set_knob read_pool_workers 12 storage | tee -a "$OUT/plan"
set_knob lv3_dedicated_cores 0 cores | tee -a "$OUT/plan"
set_knob lv2_dedicated_cores 0 cores | tee -a "$OUT/plan"
set_knob wait_class_cores 0 cores | tee -a "$OUT/plan"
set_knob reserve_cores_per_node 2 numa | tee -a "$OUT/plan"
set_knob nr_queues 4 ublk | tee -a "$OUT/plan"
set_knob shared_coalesce_pool true flush | tee -a "$OUT/plan"
set_knob coalesce_pool_workers 0 flush | tee -a "$OUT/plan"
set_knob page_write_workers 16 meta | tee -a "$OUT/plan"
echo "done — knobs restored to the shipped values. Compare $OUT/census.* ."
