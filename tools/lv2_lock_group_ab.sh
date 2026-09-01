#!/bin/bash
# A/B the LV2 mirror LD's STRIPE-LOCK FOOTPRINT (`[chunklet] lv2_lock_group_shift`).
#
#   lv2_lock_group_ab.sh <tag> [config] [arms...]
#
# WHY. A chunklet batched write holds EVERY stripe-lock bucket it touches for the
# whole call, out of 65536 buckets, so two concurrent calls of N keys are
# disjoint only with probability exp(-N^2/65536). One 1 MiB LV2 mirror write fans
# out to ~512 strip writes (256 4-KiB segments x 2 copies -- LV2's RAID10 LD runs
# `strip_kib = 0`), which puts P(disjoint) at 1.8%. Grouping by `key >> 10`
# collapses a contiguous run onto ONE bucket, so the footprint tracks RUNS, not
# keys -- the same fix RAID6/LV3 got in chunklet f71a7f0 (`lock` 5.669 -> 0.082
# ms/call). Mirror was excluded then because "a ring log writes ADJACENT keys
# from independent callers"; that stopped describing LV2 once onyx aaf9bd0 gave
# each buffer shard its own write lane, because the shards own DISJOINT 768 MiB
# slices of the LD while a `shift = 10` group spans only 4 MiB.
#
# ⭐ WHAT MADE THIS THE NEXT MOVE, and why the submit-shape knobs come AFTER.
# tools/uring_wave_window_ab.sh (box 2026-08-26, in-process arms) killed the
# submit barrier on this exact path: 8 barriers -> 2 -> NONE moved `submit` only
# -10% / -15%, so the barrier is NOT the mirror's submit cost. But queue depth
# rose exactly as designed (aqu-sz 1.42 -> 2.19) while util FELL 28% -> 13% and
# host throughput FELL 592 -> 437 MB/s, because `stripe_wait` p50 exploded
# 84 -> 1497 -> 1919 us (23x). A deeper submit holds those ~512 buckets across
# MORE device time, so depth goes up and CALLER concurrency goes down. That is
# also why chunklet-standalone's 2.16x window probe never transferred: it writes
# raw devices and has no stripe locks at all. ⇒ bound the footprint first; only
# then is re-running uring_wave_window_ab.sh a test of the submit shape rather
# than a test of the lock.
#
# ⚠ RESTART PER ARM. The shift is consumed at `Pool::open` and is IMMUTABLE for
# the LD's life by design: `StripeLockTable::bucket` is the sole key->bucket
# mapping, so two callers under different shifts would map one key to different
# buckets, i.e. exclude on different buckets = not exclude at all. There is
# deliberately no runtime setter, so this cannot be an in-process A/B like
# tools/lv3_submit_ab.sh.
#
# ⚠ Which means HOST THROUGHPUT IS NOT THE JUDGE -- it drifts 2x on this box at
# constant clock ([[two_regimes_healthy_vs_degraded]]), and the whole allocator
# lock-machinery series landed with throughput FLAT. Arms are ordered 0 -> 10 -> 0
# so the two null arms bracket the drift, and the verdict is read in this order:
#
#   1. mirror_delta.py `stripe_wait` MEAN and p50   the direct effect
#   2. mirror_delta.py `total_us`                   did the tail call shrink
#   3. mirror_delta.py `submit_us`                  did a bounded footprint let
#                                                   the submit leg deepen
#   4. lv2_stage_latency.py entry_write / append    did the append see it
#   5. iostat aqu-sz / %util                        did the DEVICE get deeper
#   6. host MiB/s                                   only believable if 1-3 moved
#
# ⚠ mirror_delta.py is conditioned on the >= 5 ms "slow mirror write_many" warn,
# so every number it prints is the TAIL (box 2026-08-26: 5.7% of lane time), not
# the mean. That is fine here -- the tail is what feeds the append p99 -- but a
# change in EVENT COUNT between arms moves the population, so read `events`
# alongside the means and do not compare a 90k-event arm to a 3k-event one
# without saying so.
#
# ⚠ Every arm pays ~240 s of meta-LD bounded-scan open before ublkb0 appears,
# then BURN seconds of burn: at RWMIX=0 a fresh LV2 ring runs 3-12x steady state
# for the first ~7 min, so sampling before that measures burst LENGTH, not the
# knob.
set -u

TAG=${1:?usage: lv2_lock_group_ab.sh <tag> [config] [arms...]}
CFG=${2:-/root/onyx_storage/config/nvme-chunklet.toml}
shift 2 2>/dev/null || shift $#
ARMS=${*:-"0 10 0"}

B=/root/onyx_storage/target/release/onyx-storage
OUT=/root/lv3run/$TAG
BURN=${BURN:-430}
SAMPLE=${SAMPLE:-240}
VOL=${VOL:-fio-volume}

mkdir -p "$OUT"
echo "arms: $ARMS   burn ${BURN}s   sample ${SAMPLE}s   config $CFG" | tee "$OUT/plan"

set_shift() {
    # 0 = chunklet's per-RAID-level default (ungrouped for a mirror).
    local shift_val="$1"
    if grep -q '^lv2_lock_group_shift' "$CFG"; then
        sed -i "s/^lv2_lock_group_shift.*/lv2_lock_group_shift = $shift_val/" "$CFG"
    else
        # Must land inside [chunklet]; append after the lv2_ld_id line, which the
        # knob requires anyway (the id has to be known before the pool opens).
        sed -i "s/^\(lv2_ld_id = .*\)$/\1\nlv2_lock_group_shift = $shift_val/" "$CFG"
    fi
    grep -n '^lv2_lock_group_shift' "$CFG"
}

# Arms are labelled `<index>.<shift>` so the repeated null arm (the drift
# bracket) does not overwrite its own earlier output.
IDX=0
for shift_val in $ARMS; do
    IDX=$((IDX + 1))
    arm="$IDX.$shift_val"
    echo "=== arm $arm (shift=$shift_val)  $(date -Is) ===" | tee -a "$OUT/plan"
    pkill -x fio 2>/dev/null
    sleep 2
    "$B" -c "$CFG" stop >/dev/null 2>&1
    for _ in $(seq 1 40); do pgrep -x onyx-storage >/dev/null || break; sleep 5; done
    pgrep -x onyx-storage >/dev/null && { echo "FAIL: engine still up" >&2; exit 1; }

    set_shift "$shift_val" | tee -a "$OUT/plan"
    "$B" -c "$CFG" cleanup-ublk >/dev/null 2>&1
    sleep 3
    RUST_LOG=info nohup "$B" -c "$CFG" start -v "$VOL" >"$OUT/engine.$arm.log" 2>&1 &
    for _ in $(seq 1 60); do [ -e /dev/ublkb0 ] && break; sleep 10; done
    [ -e /dev/ublkb0 ] || { echo "FAIL: no ublkb0 for arm $arm" >&2; exit 1; }

    # RWMIX=0: the mirror/append path is the only load where LV2 is measurable.
    fio --name=onyx-lockgroup --filename=/dev/ublkb0 --direct=1 --ioengine=io_uring \
        --rw=randwrite --bs=4k-32k --iodepth=16 --numjobs=16 \
        --runtime=$((BURN + SAMPLE + 180)) --time_based --group_reporting \
        --refill_buffers --write_bw_log="$OUT/bw.$arm" --log_avg_msec=5000 \
        >"$OUT/fio.$arm.log" 2>&1 &
    sleep "$BURN"

    # The sample window is delimited by timestamps so mirror_delta.py can be
    # restricted to it with --from/--to, excluding the burst phase.
    date -Is >"$OUT/t.$arm.start"
    "$B" -c "$CFG" status >"$OUT/status.$arm.start" 2>/dev/null
    python3 "$(dirname "$0")/lv2_stage_latency.py" "$SAMPLE" >"$OUT/lat.$arm" 2>&1 &
    LAT_PID=$!
    sleep $((SAMPLE - 40))
    iostat -xm 20 2 >"$OUT/iostat.$arm" 2>/dev/null
    wait $LAT_PID
    "$B" -c "$CFG" status >"$OUT/status.$arm.end" 2>/dev/null
    date -Is >"$OUT/t.$arm.end"
    echo "arm $arm (shift=$shift_val) done $(date -Is)" | tee -a "$OUT/plan"
done

pkill -x fio 2>/dev/null
set_shift 0 | tee -a "$OUT/plan"
echo "done — knob restored to 0. Per-arm ledgers (analysis deferred to AFTER the"
echo "loop on purpose: the first uring_wave_window_ab.sh run LOST its bracket arm"
echo "because per-arm analysis ran between arms and outlived fio):"
for f in "$OUT"/engine.*.log; do
    arm=$(basename "$f" .log); arm=${arm#engine.}
    echo "  python3 tools/mirror_delta.py $f --from \$(cat $OUT/t.$arm.start | cut -c1-16) --to \$(cat $OUT/t.$arm.end | cut -c1-16)"
done
