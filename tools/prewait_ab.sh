#!/bin/bash
# A/B `buffer.prewait_ring_space_outside_order`, and harvest the LV2 ring
# release ledger from the same arms.
#
#   prewait_ab.sh <tag> [config] [arms...]        arms: false|true, e.g. "false true false"
#
# WHY (B). At RWMIX=0, lanes=16, the foreground append is 5386 us and
# `append_prepare` is 46% of it -- almost entirely the LBA stripe locks:
#
#   order_wait   1477 us      waiting to acquire the append-order stripes
#   order_hold    932 us      holding them
#   backpressure  811 us      of which: waiting for LV2 ring space
#
# `order_hold ~= backpressure_wait` is the tell. With the pre-wait OFF (the
# default) the ring-space wait happens INSIDE the stripe locks
# (src/buffer/commit_log/shard.rs, `wait_for_ring_space` doc), so one appender
# parked on a full ring pins every stripe its own LBAs hash to and blocks
# appenders with COMPLETELY UNRELATED LBAs behind it -- false serialisation.
# The knob drains that wait before the locks are taken. Coded, default-off,
# NEVER A/B'd.
#
# WHY (A). Each arm runs with RUST_LOG=info, so `tools/checkpoint_delta.py`
# gets the ring-release ledger for free from the same log: LV2 bytes come back
# only when a metadb checkpoint lands (src/engine/durability.rs is the only
# `release_below` caller), and `meta.checkpoint_ring_fill_pct` is 0 = the ring
# trigger is dead. If the release rate tracks the host write rate while the ring
# sits at 100%, the checkpoint path -- not the drain, not the device -- is what
# `backpressure_wait` is actually waiting for.
#
# HOW TO JUDGE. Throughput is a WEAK judge here (restart per arm, and this box
# drifts 2x at a constant clock -- hence the false/true/false bracket). Read, in
# order, from `front.<arm>`:
#
#   1. order_wait        must COLLAPSE                       (direct effect)
#   2. order_hold        must drop by ~backpressure_wait     (the wait moved out)
#   3. backpressure_wait CONSERVED or HIGHER, never vanishing (both paths feed
#                        the same counter via `record_reserve_wait`; with the
#                        pre-wait on, one append can pay it TWICE -- bounded
#                        pre-wait, then the in-lock wait -- so a rise is
#                        expected. If it disappears, the arm is not measuring
#                        what it claims)
#   4. append_prepare    should fall by ~the order_wait delta
#   5. append_total      the actual question
#   6. iostat aqu-sz     did removing false serialisation deepen the device
#   7. host MiB/s        believable only if 1-5 moved together
#
# ⚠ RESTART PER ARM: the knob is read at pool open. Every arm pays ~240 s of
# meta-LD open before ublkb0 appears, then BURN s of burn (a fresh LV2 ring runs
# 3-12x steady state for the first ~7 min at RWMIX=0). Budget ~16 min/arm.
# ⚠ Runs on the EXISTING aged pool. Do NOT blkdiscard / chunklet-init between
# arms -- that reintroduces the burst-phase confound that invalidated every
# single-pair A/B on this rig (memory checkpoint_interval_duty_cycle_lockin).
set -u

TAG=${1:?usage: prewait_ab.sh <tag> [config] [arms...]}
CFG=${2:-/root/onyx_storage/config/nvme-chunklet.toml}
shift 2 2>/dev/null || shift $#
ARMS=${*:-"false true false"}

B=/root/onyx_storage/target/release/onyx-storage
OUT=/root/lv3run/$TAG
BURN=${BURN:-430}
SAMPLE=${SAMPLE:-240}
VOL=${VOL:-fio-volume}
HERE=$(dirname "$0")

mkdir -p "$OUT"
echo "arms: $ARMS   burn ${BURN}s   sample ${SAMPLE}s   config $CFG" | tee "$OUT/plan"

set_prewait() {
    local val="$1"
    if grep -q '^prewait_ring_space_outside_order' "$CFG"; then
        sed -i "s/^prewait_ring_space_outside_order.*/prewait_ring_space_outside_order = $val/" "$CFG"
    else
        sed -i "0,/^\[buffer\]/s//[buffer]\nprewait_ring_space_outside_order = $val/" "$CFG"
    fi
    grep -n '^prewait_ring_space_outside_order' "$CFG"
}

IDX=0
for val in $ARMS; do
    IDX=$((IDX + 1))
    arm="$IDX.$val"
    echo "=== arm $arm (prewait=$val)  $(date -Is) ===" | tee -a "$OUT/plan"
    pkill -x fio 2>/dev/null
    sleep 2
    "$B" -c "$CFG" stop >/dev/null 2>&1
    for _ in $(seq 1 40); do pgrep -x onyx-storage >/dev/null || break; sleep 5; done
    pgrep -x onyx-storage >/dev/null && { echo "FAIL: engine still up" >&2; exit 1; }

    set_prewait "$val" | tee -a "$OUT/plan"
    "$B" -c "$CFG" cleanup-ublk >/dev/null 2>&1
    rm -f /tmp/onyx-storage-nvme.sock*
    sleep 3
    RUST_LOG=info nohup "$B" -c "$CFG" start -v "$VOL" >"$OUT/engine.$arm.log" 2>&1 &
    for _ in $(seq 1 60); do [ -e /dev/ublkb0 ] && break; sleep 10; done
    [ -e /dev/ublkb0 ] || { echo "FAIL: no ublkb0 for arm $arm" >&2; exit 1; }

    fio --name=onyx-prewait --filename=/dev/ublkb0 --direct=1 --ioengine=io_uring \
        --rw=randwrite --bs=4k-32k --iodepth=16 --numjobs=16 \
        --runtime=$((BURN + SAMPLE + 180)) --time_based --group_reporting \
        --refill_buffers --write_bw_log="$OUT/bw.$arm" --log_avg_msec=5000 \
        >"$OUT/fio.$arm.log" 2>&1 &
    sleep "$BURN"

    date +%s >"$OUT/t.$arm.start"
    "$B" -c "$CFG" status >"$OUT/status.$arm.start" 2>/dev/null
    python3 "$HERE/lv2_stage_latency.py" "$SAMPLE" >"$OUT/lat.$arm" 2>&1 &
    LAT_PID=$!
    sleep $((SAMPLE - 40))
    iostat -xm 20 2 >"$OUT/iostat.$arm" 2>/dev/null
    wait $LAT_PID
    "$B" -c "$CFG" status >"$OUT/status.$arm.end" 2>/dev/null
    date +%s >"$OUT/t.$arm.end"

    # (B) the append decomposition -- the primary verdict.
    python3 "$HERE/front_delta.py" "$OUT/status.$arm.start" "$OUT/status.$arm.end" \
        "$SAMPLE" >"$OUT/front.$arm" 2>&1
    # In-service foreground concurrency + real backlog, both ends of the window.
    # `foreground_outstanding` is the cheapest live read of whether removing the
    # false serialisation actually deepened the pipeline; `pending_entries` is
    # the real backlog gauge (physical_fill_pct is NOT -- it is anti-correlated).
    for end in start end; do
        printf '%-6s %s | %s\n' "$end" \
            "$(sed -n 's/.*foreground_outstanding=\([0-9]*\).*/foreground_outstanding=\1/p' "$OUT/status.$arm.$end" | head -1)" \
            "$(grep -m1 '^buffer_pending_entries' "$OUT/status.$arm.$end")"
    done >>"$OUT/front.$arm" 2>&1
    # (A) the ring release ledger, skipping this arm's burn phase.
    HOST_MBS=$(sed -n 's/.*bytes\/s=\([0-9.]*\) MB\/s.*/\1/p' "$OUT/front.$arm" | head -1)
    python3 "$HERE/checkpoint_delta.py" "$OUT/engine.$arm.log" --skip-s "$BURN" \
        ${HOST_MBS:+--host-mbs "$HOST_MBS"} >"$OUT/ckpt.$arm" 2>&1
    echo "arm $arm done $(date -Is)  host=${HOST_MBS:-?} MB/s" | tee -a "$OUT/plan"
    tail -12 "$OUT/ckpt.$arm" | tee -a "$OUT/plan"
done

pkill -x fio 2>/dev/null
set_prewait false | tee -a "$OUT/plan"
echo "done — knob restored to false (compiled default)."
echo "verdict: $OUT/front.*   ring ledger: $OUT/ckpt.*   stages: $OUT/lat.*"
