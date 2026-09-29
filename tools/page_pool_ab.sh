#!/bin/bash
# Interleaved A/B for metadb's process-wide Page buffer pool, INSIDE ONE PROCESS.
#
#   page_pool_ab.sh <tag> [config]
#
# What is being tested: a metadb `Page` is a `Box<[u8; 4096]>`, and 4096 is a
# jemalloc small class with ONE region per slab, so every page alloc/free was an
# extent operation and every freed page a dirty extent that decay purges with a
# `madvise` — a TLB-shootdown broadcast to every CPU sharing the mm. LBR on
# 2026-09-25: the checkpoint thread spent 22.4 % of its CPU in jemalloc, 62 % of
# it under `DirtySnapshot::seal` and 29 % under `PageCache::invalidate`. `on`
# recycles page buffers through the pool; `off` is a fresh heap buffer per page.
#
# BASELINE: master with jemalloc's DEFAULT decay (no `_rjem_malloc_conf`
# symbol, no `_RJEM_MALLOC_CONF` in the environment) and without the parked
# reclaim-reuse change — root cause first, band-aids later
# (memory `feedback_root_cause_before_bandaid`).
#
# JUDGE ORDER -- mechanism first, throughput last:
#
#   1. self-proof   -- `metadb_page_pool` delta: ON fresh/takes ~0 and takes
#                      moving; OFF takes frozen and bypass moving. If it does
#                      not follow the knob, the arms are one arm.
#   2. madvise/s    -- total (`perf stat`) and per thread (`perf record`, same
#                      window); `metadb-bfg-sync` is the thread to watch
#   3. TLB IPI/s    -- `/proc/interrupts` TLB and CAL rows
#   4. throughput   -- status delta, inside the same-arm bracket
#
# ⚠ The burn is GATED on the LV2 ring settling, not a fixed time: in the last
# arm of this kind the ring climbed 19 -> 100 % across the run and the final
# segment was measured in a different regime (memory
# `reclaim_reuse_moved_purges_to_bfg_sync`).
set -u

TAG=${1:?usage: page_pool_ab.sh <tag> [config]}
CFG=${2:-/root/onyx_storage/config/nvme-chunklet.toml}
B=/root/onyx_storage/target/release/onyx-storage
OUT=/root/pagepool/$TAG
# ⚠ p1 (2026-09-29) declared "settled" at 150 s on 6/3/4 % and then climbed to
# 99 % over the next 25 min: a slow climb looks flat in a 1-minute window. So
# a long floor, and a window of RING_N samples.
# ⚠ p2: once climbed, the ring OSCILLATES 2<->100 % (checkpoint-bound) and never
# satisfies the gate, so the default is a fixed 20 min burn (p2 reached the
# heavy regime at ~17 min). Match regimes on pages/ckpt in the report instead.
BURN_MIN=${BURN_MIN:-1200}
BURN_MAX=${BURN_MAX:-1200}
# Settled = RING_N consecutive samples, RING_STEP s apart, within RING_TOL points.
RING_STEP=${RING_STEP:-30}
RING_N=${RING_N:-5}
RING_TOL=${RING_TOL:-5}
# Global-stack cap for the ON arm, in bytes; empty keeps the engine's config.
CAP=${CAP:-}
# jemalloc's default dirty decay is 10 s (muzzy 0): after a flip, a few decay
# periods retire the previous arm's frees.
SETTLE=${SETTLE:-30}
MEASURE=${MEASURE:-120}
# QD256 j16d16 4k-32k randrw 70/30 — the canonical shape. ⛔ Any other QD must
# be agreed first and stated in the conclusion (memory
# `qd_ladder_congestion_collapse`).
RWMIX=${RWMIX:-70}
JOBS=${JOBS:-16}
DEPTH=${DEPTH:-16}
SEGMENTS=${SEGMENTS:-"A1 B1 A2 B2 A3 B3"}
ATTACH=${ATTACH:-0}
KNOB=metadb-page-pool

mkdir -p "$OUT"
SOCK=$(sed -n 's/^socket_path *= *"\(.*\)"/\1/p' "$CFG" | head -1)

if [ -n "${_RJEM_MALLOC_CONF:-}" ] || [ -n "${MALLOC_CONF:-}" ]; then
    echo "FAIL: allocator env override set; this arm is judged on jemalloc defaults" >&2
    exit 2
fi

# ⚠ `start` opens the chunklet pool twice inside ONE process and the second
# flock can race the first one's release: ~30-50% of starts die with "pool
# device ... is held by another process" although nothing else holds it
# (memory `chunklet_fresh_pool_double_open_flock_race`). So: wait until every
# PD is lockable, drop the stale socket a graceful stop leaves behind (it
# sends CLIs down an extra direct-open path), and retry ONLY that failure.
start_engine() {
    local attempt pid busy d
    for attempt in 1 2 3 4 5 6 7 8; do
        for _ in $(seq 1 30); do
            busy=0
            for d in /dev/nvme[0-9]n1; do
                flock -n "$d" true 2>/dev/null || busy=1
            done
            [ "$busy" = 0 ] && break
            sleep 1
        done
        [ -n "$SOCK" ] && rm -f "$SOCK" "$SOCK.io"
        echo "=== start attempt $attempt $(date -Is)" >>"$OUT/engine.log"
        RUST_LOG=${RUST_LOG:-"info,onyx_storage::io::block_backend=error,onyx_chunklet::pool::ld_ops=error"} \
            nohup "$B" -c "$CFG" start -v fio-volume >>"$OUT/engine.log" 2>&1 </dev/null &
        pid=$!
        # The pool opens happen in the first seconds; past 20 s the race is over.
        for _ in $(seq 1 20); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 1
        done
        if kill -0 "$pid" 2>/dev/null; then
            echo "engine pid $pid (start attempt $attempt)" >>"$OUT/plan"
            ENGINE_PID=$pid
            return 0
        fi
        if ! tail -5 "$OUT/engine.log" | grep -qE "flock|held by another|PoolLocked"; then
            echo "FAIL: engine exited, and not on the flock race (see $OUT/engine.log)" >&2
            return 1
        fi
        sleep 3
    done
    echo "FAIL: engine lost the flock race 8 times in a row" >&2
    return 1
}

if [ "$ATTACH" = "0" ]; then
    start_engine || exit 1
fi

UBLK=""
ENGINE_PID=${ENGINE_PID:-}
for _ in $(seq 1 360); do
    UBLK=$(ls -1 /dev/ublkb* 2>/dev/null | head -1)
    [ -n "$UBLK" ] && break
    if [ -n "$ENGINE_PID" ] && ! kill -0 "$ENGINE_PID" 2>/dev/null; then
        echo "FAIL: engine died before ublk came up (see $OUT/engine.log)" >&2
        exit 1
    fi
    sleep 2
done
[ -n "$UBLK" ] || { echo "FAIL: no /dev/ublkb* (see $OUT/engine.log)" >&2; exit 1; }
sleep 5

# ⭐ Self-proof of the SWITCH, both ways, before any measurement. The status
# delta proves the switch reached the page path; this proves the IPC moves.
# ⚠ `tail -1`, stderr dropped: the CLI logs before the reply.
for want in off on; do
    got=$("$B" -c "$CFG" $KNOB "$want" 2>/dev/null | tail -1)
    if [ "$got" != "$want" ]; then
        echo "FAIL self-proof: asked for '$want', engine echoed '$got'" >&2
        exit 2
    fi
done
echo "self-proof ok: $KNOB moves both ways" >>"$OUT/plan"
if [ -n "$CAP" ]; then
    want_cap=$(( CAP / 4096 * 4096 ))
    got=$("$B" -c "$CFG" $KNOB cap "$CAP" 2>/dev/null | tail -1)
    if [ "$got" != "$want_cap" ]; then
        echo "FAIL: asked for cap $want_cap, engine echoed '$got'" >&2
        exit 2
    fi
    echo "cap set to $got bytes" >>"$OUT/plan"
fi

ring_pct() {
    "$B" -c "$CFG" status 2>/dev/null | sed 's/\x1b\[[0-9;]*m//g' |
        sed -n 's/^buffer_physical_fill_pct: *\([0-9]*\).*/\1/p' | head -1
}

NSEG=$(echo "$SEGMENTS" | wc -w)
RUNTIME=$((BURN_MAX + NSEG * (SETTLE + MEASURE + 60) + 300))
fio --name=onyx-pagepool --filename="$UBLK" --direct=1 --ioengine=io_uring \
    --rw=randrw --rwmixread="$RWMIX" --bsrange=4k-32k --iodepth="$DEPTH" \
    --numjobs="$JOBS" --runtime="$RUNTIME" --time_based --group_reporting \
    --refill_buffers --write_bw_log="$OUT/bw" --log_avg_msec=5000 \
    >"$OUT/fio.log" 2>&1 </dev/null &
FIOPID=$!
echo "fio pid $FIOPID ublk=$UBLK runtime ${RUNTIME}s segments: $SEGMENTS" >>"$OUT/plan"

# Engine RSS in KiB. The pool keeps up to its cap resident by design, so memory
# is a judge of this arm, not a footnote.
rss_kib() {
    local pid
    pid=${ENGINE_PID:-$(pgrep -x onyx-storage | head -1)}
    awk '/^VmRSS:/ {print $2}' "/proc/$pid/status" 2>/dev/null
}

irq_snapshot() {
    grep -E "^ *(TLB|CAL):" /proc/interrupts |
        awk '{s=0; for (i=2; i<=NF-2; i++) s+=$i; print $1, s}'
}

# Gated burn: at least BURN_MIN, then until the ring holds still.
sleep "$BURN_MIN"
waited=$BURN_MIN
samples=""
while :; do
    r=$(ring_pct)
    samples="$samples ${r:-x}"
    echo "burn t=${waited}s ring=${r:-?}%" >>"$OUT/burn"
    lastn=$(echo "$samples" | awk -v k="$RING_N" '{n=NF; if (n<k) exit 1; s=""; for (i=n-k+1; i<=n; i++) s=s" "$i; print s}') || lastn=""
    if [ -n "$lastn" ] && ! echo "$lastn" | grep -q x; then
        spread=$(echo "$lastn" | awk '{mn=$1; mx=$1; for(i=2;i<=NF;i++){if($i<mn)mn=$i; if($i>mx)mx=$i}; print mx-mn}')
        [ "$spread" -le "$RING_TOL" ] && break
    fi
    if [ "$waited" -ge "$BURN_MAX" ]; then
        echo "⚠ ring never settled within ${BURN_MAX}s; measuring anyway — READ THE ring% COLUMN" >>"$OUT/plan"
        break
    fi
    sleep "$RING_STEP"
    waited=$((waited + RING_STEP))
done
echo "burn done after ${waited}s (ring samples:$samples) $(date -Is)" >>"$OUT/plan"

for seg in $SEGMENTS; do
    case "$seg" in
        A*) want=on ;;
        B*) want=off ;;
    esac
    got=$("$B" -c "$CFG" $KNOB "$want" 2>/dev/null | tail -1)
    if [ "$got" != "$want" ]; then
        echo "FAIL: segment $seg wanted '$want', engine echoed '$got'" >&2
        exit 2
    fi
    echo "$got" >"$OUT/knob.$seg"
    sleep "$SETTLE"

    date +%s >"$OUT/t.$seg.start"
    irq_snapshot >"$OUT/irq.$seg.start"
    rss_kib >"$OUT/rss.$seg.start"
    "$B" -c "$CFG" status >"$OUT/status.$seg.start" 2>/dev/null
    # Both perf runs sit inside the window with the status delta, so every
    # number in a row describes the same interval.
    perf stat -a -e syscalls:sys_enter_madvise -- sleep "$MEASURE" \
        2>"$OUT/madvise.$seg" &
    STATPID=$!
    perf record -q -a -e syscalls:sys_enter_madvise -o "$OUT/madv.$seg.data" \
        -- sleep "$MEASURE" >/dev/null 2>&1 &
    RECPID=$!
    wait "$STATPID" "$RECPID" 2>/dev/null
    "$B" -c "$CFG" status >"$OUT/status.$seg.end" 2>/dev/null
    irq_snapshot >"$OUT/irq.$seg.end"
    rss_kib >"$OUT/rss.$seg.end"
    date +%s >"$OUT/t.$seg.end"
    perf script -i "$OUT/madv.$seg.data" -F comm 2>/dev/null |
        awk '{print $1}' | sort | uniq -c | sort -rn >"$OUT/madvise_comm.$seg"
    rm -f "$OUT/madv.$seg.data"
    echo "segment $seg ($want) done $(date -Is)" >>"$OUT/plan"
done

echo "done; engine and fio still running. Report with:"
echo "  tools/page_pool_report.py $OUT"
echo "Stop with:  pkill -x fio; $B -c $CFG stop"
