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
# ARMS (each 180 s, `cores <lv2> <lv3> <reserve> [<wait> [<groups>]]`)
#   B  baseline        cores 0 0 2 0   engine_cpus 44
#   R  reserve back    cores 0 0 0 0   engine_cpus 48  — grow the denominator
#   S  reserve back x1 cores 0 0 1 0   engine_cpus 46  — the never-measured middle
#   D  LV2 dedicated   cores 5 0 0 0   engine_cpus 48 + BufferSync owns 5 cores
#   W  wait fence      cores 0 0 2 4   120 wait-class threads onto 4 cores
#   W5 wait fence x5   cores 0 0 2 5   the size sweep: SMT siblings are not
#                                      two cores, so 8 logical CPUs for 7.67
#                                      meanR may be tight
#   WA metadb only     cores 0 0 2 3 metadb-apply,metaio
#                                      96 threads @ 6.06 meanR; leaves
#                                      flusher-writer on the LV3 submit path
#
# ⭐ WHY THE W ARMS EXIST. Every D-style arm gives cores to a BUSY role and all
# of them lost: demand is 70.5 mean-runnable CPUs against 44, so a dedication
# converts shared capacity into private IDLE capacity (memory
# `lv2_dedication_throughput_retracted`). The W arms are the inverse — 176 of
# 310 threads ask for 19.49 CPUs between them at 5-8% duty, so packing them is
# cheap and it takes 120 threads off the runqueues of the other 36 CPUs.
#
# JUDGE A W ARM IN THIS ORDER — and the first two are NOT throughput:
#   1. metadb apply_wait / checkpoint total_us   the apply lanes are IN the
#      (status)                                  fence and a commit needs every
#                                                touched shard's lane started.
#                                                This is the kill criterion.
#   2. fence utilisation                         sum meanR of the fenced groups
#      (census: the 4 groups)                    / logical CPUs in the fence.
#                                                <80% ⇒ the fence is too big and
#                                                is making private idle capacity,
#                                                which is exactly how the LV2 arm
#                                                lost. Judge SIZE on this, not on
#                                                throughput.
#   3. nv/thr/s of read-pool / ublk-io-worker /  the mechanism: these are who the
#      flusher-dedup / flusher-compress          fence is supposed to stop
#                                                preempting. If they do not fall,
#                                                the arm did not happen.
#   4. then the throughput order below.
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
# ⚠ SEG is NOT the measurement window. The window is however long the census
# takes, and that is set by SAMPLES: 150 samples at --interval 0.2 measured
# **68-81 s** on the box (p11 and p12 both), because each sample walks ~310
# /proc task dirs and that costs ~0.27 s on top of the interval. SEG only sizes
# fio's `--runtime` budget, so it has to be >= the real window or fio exits
# mid-batch. Raise SAMPLES to lengthen an arm; ~5 samples per second of window.
SAMPLES=${SAMPLES:-150}
SEG=${SEG:-180}
SETTLE=${SETTLE:-15}
# Default batch is the wait-class fence: three B segments bracketing two
# same-size W segments (so W has a spread of its own), plus one size sweep and
# one subset arm. ⚠ Keep the B segments spread through the batch, not bunched:
# the 2026-09-18 run drifted monotonically and B1 was higher than every later
# segment.
SEGMENTS=${SEGMENTS:-"B1 W1 B2 W2 WA1 W51 B3"}
# Spelled out rather than "all": the engine echoes the expanded list, and the
# arm's group check is an exact comparison against what it asked for.
ALL_WAIT_GROUPS="metadb-apply,metaio,flusher-writer,flusher-post-commit"

mkdir -p "$OUT"
DEV=$(ls /dev/ublkb* 2>/dev/null | head -1)
[ -n "$DEV" ] || { echo "FAIL: no /dev/ublkb* — start the engine first" >&2; exit 1; }
P=$(pgrep -nx onyx-storage) || { echo "FAIL: engine not running" >&2; exit 1; }

# Prove confine is even active before burning 150 s: `cores` reports
# "inactive" under any other layout, and every arm below would then be a
# duplicate baseline.
# ⚠ ANCHOR the match to the line start; do not take "the first line", and do
# not match `reserved_cores=` unanchored. The CLI process runs its own numa
# setup on startup and logs `numa confine active ... reserved_cores=2` for
# ITSELF — on STDOUT, and that line contains `reserved_cores=` too. Only the
# IPC reply begins with it.
before=$("$B" -c "$CFG" cores 2>/dev/null | grep -m1 -E '^reserved_cores=|^error|^inactive')
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
    local seg="$1" lv2="$2" lv3="$3" rsv="$4" want_cpus="$5" wait="${6:-0}" groups="${7:-}"
    local got
    got=$("$B" -c "$CFG" cores "$lv2" "$lv3" "$rsv" "$wait" ${groups:+"$groups"} 2>/dev/null \
        | grep -m1 -E '^reserved_cores=|^error|^inactive')
    echo "arm $seg: requested lv2=$lv2 lv3=$lv3 reserve=$rsv wait=$wait groups=${groups:-<config>} -> $got" \
        | tee -a "$OUT/plan"
    case "$got" in
        *"engine_cpus=$want_cpus"*) : ;;
        *) echo "FAIL: $seg wanted engine_cpus=$want_cpus, engine reports: $got" >&2; exit 1 ;;
    esac
    # Each dedication is proved BOTH ways — present when asked for, absent when
    # not. Only checking presence lets an arm that failed to disarm pass as the
    # baseline, which is the silent-duplicate-arm failure this whole harness is
    # built to refuse.
    prove_dedication() { # <label> <asked> <needle>
        if [ "$2" = "0" ]; then
            case "$got" in
                *"$3"*) echo "FAIL: $seg wanted no $1 dedication, got: $got" >&2; exit 1 ;;
            esac
        else
            case "$got" in
                *"$3"*) : ;;
                *) echo "FAIL: $seg wanted a $1 dedication, got: $got" >&2; exit 1 ;;
            esac
        fi
    }
    prove_dedication LV2 "$lv2" BufferSync
    prove_dedication LV3 "$lv3" Lv3Batch
    # The fence prints as `wait_fence=[...]` even when empty, so both cases
    # assert on the same token. SMT2 ⇒ 2 logical CPUs per physical core.
    #
    # ⛔ The token's ABSENCE means the running engine predates the fence, which
    # is the rsync-≠-rebuild failure that cost /root/p1/p9 a whole run: serde
    # ignores unknown config fields and a missing token would otherwise read as
    # "no fence", i.e. every W arm would silently be a duplicate baseline.
    case "$got" in
        *wait_fence=*) : ;;
        *) echo "FAIL: $seg — this engine has no wait_fence in its budget summary; \
the release binary on this box predates the fence. Rebuild it (cargo build --release \
--manifest-path /root/onyx_storage/Cargo.toml) and restart. Reply was: $got" >&2; exit 1 ;;
    esac
    local fence want_fence_cpus
    fence=$(echo "$got" | sed -n 's/.*wait_fence=\[\([^]]*\)\].*/\1/p')
    if [ "$wait" = "0" ]; then
        [ -z "$fence" ] || { echo "FAIL: $seg wanted no wait fence, got [$fence]" >&2; exit 1; }
    else
        want_fence_cpus=$((wait * ${SMT:-2}))
        local n
        n=$(echo "$fence" | tr ',' '\n' | grep -c '[0-9]')
        [ "$n" = "$want_fence_cpus" ] || {
            echo "FAIL: $seg wanted $want_fence_cpus fenced CPUs ($wait cores x ${SMT:-2}), got $n: [$fence]" >&2
            exit 1; }
        # ⚠ EXACT comparison, not a `case` glob: `wait_groups=metadb-apply,metaio`
        # is a PREFIX of the full four-group list, so a substring match would
        # accept the whole fence as the two-group subset arm. And the engine
        # echoes the EXPANDED list, never the literal "all" — which is why the
        # W arms below pass the four names explicitly.
        if [ -n "$groups" ]; then
            local got_groups
            got_groups=$(echo "$got" | sed -n 's/.*wait_groups=\([^ ]*\).*/\1/p')
            [ "$got_groups" = "$groups" ] || {
                echo "FAIL: $seg wanted wait_groups=$groups, engine reports $got_groups" >&2
                exit 1; }
        fi
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
    # ⚠ Order matters: W5 and WA must be matched before the bare W.
    case "$seg" in
        B*)  set_arm "$seg" 0 0 2 44 0 ;;
        R*)  set_arm "$seg" 0 0 0 48 0 ;;
        S*)  set_arm "$seg" 0 0 1 46 0 ;;
        D*)  set_arm "$seg" 5 0 0 48 0 ;;
        W5*) set_arm "$seg" 0 0 2 44 5 "$ALL_WAIT_GROUPS" ;;
        WA*) set_arm "$seg" 0 0 2 44 3 metadb-apply,metaio ;;
        W*)  set_arm "$seg" 0 0 2 44 4 "$ALL_WAIT_GROUPS" ;;
        *)   echo "unknown segment $seg (must start with B, R, S, D, W, W5 or WA)" >&2; exit 2 ;;
    esac
    # Let the enforcer converge every already-running thread onto the new masks
    # before anything is measured. Three sweeps.
    sleep "$SETTLE"

    date +%s >"$OUT/t.$seg.start"
    "$B" -c "$CFG" status >"$OUT/status.$seg.start" 2>/dev/null
    python3 "$CENSUS" "$P" --samples "$SAMPLES" --interval 0.2 --top 45 >"$OUT/census.$seg" 2>&1
    "$B" -c "$CFG" status >"$OUT/status.$seg.end" 2>/dev/null
    date +%s >"$OUT/t.$seg.end"

    echo "window $(( $(cat "$OUT/t.$seg.end") - $(cat "$OUT/t.$seg.start") ))s ($SAMPLES census samples)" \
        | tee -a "$OUT/plan"
    head -2 "$OUT/census.$seg" | tee -a "$OUT/plan"
    grep -E 'buffer_physical_fill_pct' "$OUT/status.$seg.end" | tee -a "$OUT/plan"
    # The W arms' KILL CRITERION, printed per segment so it is read before any
    # throughput number: metadb's apply lanes are inside the fence, and a
    # commit needs every touched shard's lane started
    # (memory `metadb_apply_lane_count_is_topology_not_pool_size`), so packing
    # 64 lanes onto a few CPUs can lengthen exactly this wait.
    python3 - "$OUT/status.$seg.start" "$OUT/status.$seg.end" <<'PY' | tee -a "$OUT/plan"
import sys
def parse(p):
    d = {}
    for line in open(p):
        pre, _, rest = line.partition(":")
        pre = pre.strip()
        for tok in rest.split():
            k, _, v = tok.partition("=")
            if v.isdigit():
                d[pre + "." + k] = int(v)
    return d
a, b = parse(sys.argv[1]), parse(sys.argv[2])
def d(k): return b.get("metadb_commit." + k, 0) - a.get("metadb_commit." + k, 0)
ok = d("success")
if ok:
    print("  metadb_commit: %d commits  apply_wait %.0f us/commit  total %.0f us/commit"
          "  apply_wait_max %d us  max %d us" % (
        ok, d("apply_wait_us") / ok, d("total_us") / ok,
        b.get("metadb_commit.apply_wait_max_us", 0), b.get("metadb_commit.max_us", 0)))
else:
    print("  metadb_commit: no commits in this window")
PY
    echo "segment $seg done $(date -Is)" | tee -a "$OUT/plan"
done

pkill -x fio 2>/dev/null
# Back to the shipped budget so a later run is not silently armed. The config
# was never touched, so this is the only state to restore.
set_arm restore 0 0 2 44 0
echo "done — budget restored. Discard any segment whose flush errors delta is non-zero."
