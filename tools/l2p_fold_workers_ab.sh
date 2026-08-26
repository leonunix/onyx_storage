#!/bin/bash
# A/B `meta.parallel_l2p_drain_workers` -- the L2P checkpoint fold fan-out cap.
#
#   l2p_fold_workers_ab.sh <tag> [config] [arms...]      arms: 4 8 4
#
# WHY. Box-measured 2026-08-26 (RWMIX=0, QD256 j16d16, aged 256 GiB volume,
# three bracketed arms). The chain, established in that session:
#
#   1. LV2 ring bytes are released ONLY when a metadb checkpoint lands
#      (src/engine/durability.rs is the only `release_below` caller, one
#      checkpoint in flight at a time).
#   2. The release rate tracks the host write rate at only **1.12-1.20x**, and
#      61-69% of releases arrive at a ring already at 100%. So the foreground's
#      `backpressure_wait` (724-821 us mean, multi-second tail stalls) is
#      mechanically a wait for a checkpoint.
#   3. `req->release` is **3.55-4.71 s p50**. tools/flush_phase_delta.py splits
#      it (86 metadb flushes / 240 s = 2 per onyx checkpoint, 2 x 1741 ms =
#      3.48 s, which closes against the 3.55 s measured):
#
#        PREPARE l2p_fold   843 ms   48.4%   <-- THIS
#        io                 453 ms   26.0%   (seal 244 + page_write 191)
#        sample              93 ms    5.3%
#        install             22 ms    1.2%
#        flush wall        1741 ms
#
#   4. That fold is a fan-out of one job per (volume, L2P shard) bounded by
#      `parallel_l2p_drain_workers` (default 4). Measured: **8.0 shard folds per
#      checkpoint, 414 ms each, 3315 ms of CPU in 843 ms of wall = achieved
#      concurrency 3.93 of the cap 4 (98%)**. The cap is hard-binding on a box
#      with 96 cores and every other stage under 20% duty.
#
# ⭐ N=8 IS THE CEILING, not a step on the way up: there are exactly 8 jobs, so
# 8 workers puts the wall at one shard's fold (~414 ms) and N=16 buys nothing.
# Expected: fold wall 843 -> ~414 ms, flush wall 1741 -> ~1310, onyx checkpoint
# 3.48 -> ~2.6 s, release headroom 1.15x -> ~1.5x.
#
# HOW TO JUDGE, in order (host throughput is a WEAK judge -- restart per arm and
# this box drifts 2x at a constant clock, hence the 4/8/4 bracket):
#
#   1. flush_phase_delta `l2p_fold` wall     must fall ~2x      (direct effect)
#   2. flush_phase_delta achieved concurrency 3.93 -> ~7.8       (the cap lifted)
#   3. flush_phase_delta TOTAL flush wall    1741 -> ~1310
#   4. checkpoint_delta `req->release` p50   3.5 -> ~2.6 s
#   5. checkpoint_delta release/host         1.15x -> higher headroom
#   6. front_delta backpressure_wait + events   should FALL
#   7. front_delta append_total                 the actual question
#   8. host MiB/s                               only if 1-7 moved together
#
# ⚠ THE DOCUMENTED TRADE. The default of 4 exists to "leave room for RC apply
# while parallel L2P folds are active" (src/config.rs). A fold win paid for out
# of RC is not a win -- check the RC block of flush_phase_delta:
# `fold_lock_wait` (0.6 ms/ckpt at N=4) and `fold_service` (825 ms/ckpt, today
# OFF the critical path) must not move onto it.
#
# ⛔ Do NOT reach for `meta.checkpoint_ring_fill_pct` instead. Forcing the
# release cadence was measured 2026-07-25 and lost 40% of throughput: each
# checkpoint then released ~5x fewer entries for the same bytes/s. Cadence is
# the stall SHAPE; this knob attacks the checkpoint COST, which is the level.
#
# ⚠ Runs on the EXISTING aged pool. No blkdiscard / chunklet-init between arms.
# Every arm pays ~240 s of meta-LD open + BURN s of burn. Budget ~16 min/arm.
set -u

TAG=${1:?usage: l2p_fold_workers_ab.sh <tag> [config] [arms...]}
CFG=${2:-/root/onyx_storage/config/nvme-chunklet.toml}
shift 2 2>/dev/null || shift $#
ARMS=${*:-"4 8 4"}

B=/root/onyx_storage/target/release/onyx-storage
OUT=/root/lv3run/$TAG
BURN=${BURN:-430}
SAMPLE=${SAMPLE:-240}
VOL=${VOL:-fio-volume}
HERE=$(dirname "$0")

mkdir -p "$OUT"
echo "arms: $ARMS   burn ${BURN}s   sample ${SAMPLE}s   config $CFG" | tee "$OUT/plan"

set_workers() {
    local n="$1"
    if grep -q '^parallel_l2p_drain_workers' "$CFG"; then
        sed -i "s/^parallel_l2p_drain_workers.*/parallel_l2p_drain_workers = $n/" "$CFG"
    else
        sed -i "0,/^\[meta\]/s//[meta]\nparallel_l2p_drain_workers = $n/" "$CFG"
    fi
    grep -n '^parallel_l2p_drain_workers' "$CFG"
}

IDX=0
for n in $ARMS; do
    IDX=$((IDX + 1))
    arm="$IDX.$n"
    echo "=== arm $arm (workers=$n)  $(date -Is) ===" | tee -a "$OUT/plan"
    pkill -x fio 2>/dev/null
    sleep 2
    "$B" -c "$CFG" stop >/dev/null 2>&1
    for _ in $(seq 1 40); do pgrep -x onyx-storage >/dev/null || break; sleep 5; done
    pgrep -x onyx-storage >/dev/null && { echo "FAIL: engine still up" >&2; exit 1; }

    set_workers "$n" | tee -a "$OUT/plan"
    "$B" -c "$CFG" cleanup-ublk >/dev/null 2>&1
    rm -f /tmp/onyx-storage-nvme.sock*
    sleep 3
    RUST_LOG=info nohup "$B" -c "$CFG" start -v "$VOL" >"$OUT/engine.$arm.log" 2>&1 &
    for _ in $(seq 1 60); do [ -e /dev/ublkb0 ] && break; sleep 10; done
    [ -e /dev/ublkb0 ] || { echo "FAIL: no ublkb0 for arm $arm" >&2; exit 1; }

    fio --name=onyx-fold --filename=/dev/ublkb0 --direct=1 --ioengine=io_uring \
        --rw=randwrite --bs=4k-32k --iodepth=16 --numjobs=16 \
        --runtime=$((BURN + SAMPLE + 180)) --time_based --group_reporting \
        --refill_buffers --write_bw_log="$OUT/bw.$arm" --log_avg_msec=5000 \
        >"$OUT/fio.$arm.log" 2>&1 &
    sleep "$BURN"

    date +%s >"$OUT/t.$arm.start"
    "$B" -c "$CFG" status >"$OUT/status.$arm.start" 2>/dev/null
    sleep $((SAMPLE - 40))
    iostat -xm 20 2 >"$OUT/iostat.$arm" 2>/dev/null
    "$B" -c "$CFG" status >"$OUT/status.$arm.end" 2>/dev/null
    date +%s >"$OUT/t.$arm.end"

    # 1-3: the checkpoint phase ledger -- the primary verdict.
    python3 "$HERE/flush_phase_delta.py" "$OUT/status.$arm.start" "$OUT/status.$arm.end" \
        "$SAMPLE" >"$OUT/flush.$arm" 2>&1
    # 6-8: the foreground append decomposition.
    python3 "$HERE/front_delta.py" "$OUT/status.$arm.start" "$OUT/status.$arm.end" \
        "$SAMPLE" >"$OUT/front.$arm" 2>&1
    # 4-5: the ring release ledger.
    HOST_MBS=$(sed -n 's/.*bytes\/s=\([0-9.]*\) MB\/s.*/\1/p' "$OUT/front.$arm" | head -1)
    python3 "$HERE/checkpoint_delta.py" "$OUT/engine.$arm.log" --skip-s "$BURN" \
        ${HOST_MBS:+--host-mbs "$HOST_MBS"} >"$OUT/ckpt.$arm" 2>&1
    echo "arm $arm done $(date -Is)  host=${HOST_MBS:-?} MB/s" | tee -a "$OUT/plan"
    grep -E "l2p_fold|concurrency|TOTAL" "$OUT/flush.$arm" | tee -a "$OUT/plan"
    grep -E "req->release|release/host" "$OUT/ckpt.$arm" | tee -a "$OUT/plan"
done

pkill -x fio 2>/dev/null
set_workers 4 | tee -a "$OUT/plan"
echo "done — knob restored to 4 (compiled default)."
echo "verdict: $OUT/flush.*   ring: $OUT/ckpt.*   front: $OUT/front.*"
