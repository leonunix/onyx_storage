#!/bin/bash
# Interleaved A/B for LV3 DEVICE CONCURRENCY — inside ONE process, one pool age.
#
#   lv3_concurrency_ab.sh <tag> [config]
#
# THE PREMISE. The drain sits at ~250 MB/s in BOTH load shapes while the drives
# are 74-94% idle and every software stage is under 20% duty. The arithmetic
# closes on concurrency, not capacity: one chunklet write_many_at carries 71.7
# stripes / 1.72 MB in 5.87 ms = 293 MB/s per call, so 250 MB/s IS ~0.85 calls in
# flight against six executors. The same LD, same RAID6 6+2, same 24 KiB
# full-stripe random writes does 1738 MiB/s at 6 concurrent callers with ONE
# stripe per call and 3589 at 24 (chunklet_perf). Call size is worth +27%;
# concurrency is worth 6x.
#
# WHY IT IS 0.85. Sixteen writer lanes block synchronously in submit_many; the
# single aggregator folds 8.6-9.8 of their requests into one batch and one batch
# goes to one executor. Each request is only 128-218 KiB because the writer lane
# breaks its drain at `batch_target` — 32 units under read_active, which the
# GLOBAL read counter makes permanent in any mixed workload.
#
# WHY THE OBVIOUS FIXES ALL FAILED BEFORE. Every previous arm moved ONE side of a
# closed loop. `idle_dispatch` (2026-08-14) raised concurrency 0.97 -> 1.35 and
# lost 1007 -> 771 MiB/s, because releasing a batch the instant an executor frees
# up also shortens the WRITER's cycle — chunklet 68.4 -> 2.4 stripes/call, merge
# 2.93 -> 1.05x, metadb commits 583 -> 2949/s. So the arms here always move the
# aggregator AND the producer's quantum together.
#
# ARMS (all over IPC, no restart)
#   B  baseline            lv3-batch 0 0            writer-batch 0 0 0
#   A  concurrency         lv3-batch 1 MiB 1 MiB    writer-batch 0 0 0
#   C  A + lane decoupling lv3-batch 1 MiB 1 MiB    writer-batch 256 2000 0
#   D  lane decoupling     lv3-batch 0 0            writer-batch 256 2000 0
#
# D exists to separate the two halves. If C wins and D does not, the win is the
# aggregator; if D alone wins, the read-active target was the whole story.
#
# BOX RESULTS 2026-09-14 (run /root/lv3conc/conc_w70, RWMIX=70, 9 segments,
# aged 256 GiB volume, engine NOT restarted between arms). Read these before
# re-running — three of them change how you must set the run up.
#
#   seg  arm   ra%  req/b  KiB/b  conc  sqe/MB  cmt/s   host  sc%      err
#   B1    B   95.3   2.42   2819  2.76     261   45.2  379.8    2        0
#   A1    A   94.0   2.01   1479  3.79     308   40.1  347.9    0   197758
#   B2    B   92.1   2.05   3314  3.86     322   47.2  330.4    0   413148
#   C1    C   94.4   1.94   1452  3.75     312   61.9  339.8    0   579633
#   B3    B   94.2   2.11   2483  2.90     306  122.5  306.0    0  1432915
#   A2    A   96.0   2.26   1332  3.14     293  101.1  272.0    1   810918
#   B4    B   97.4   2.64   2653  2.76     285  133.8  326.0    0   754807
#   C2    C   94.7   2.07   1423  3.46     311   93.0  299.9    0   972395
#   B5    B   94.6   2.25   2847  2.78     305  127.2  295.3    0  1176944
#
# ✅ 1. `ra%` 92-97% ON EVERY SEGMENT. The read_active premise is CONFIRMED: at
#       70/30 a writer lane is read-active on ~95% of its cycles, so the 32-unit
#       read-active target — not the 512-unit idle one — is the steady-state
#       batch target for every lane.
# ✅ 2. The knob engages perfectly and reproducibly: `KiB/b` classifies all nine
#       segments correctly with no overlap (B 2483-3314, A/C 1332-1479), and
#       `conc` separates 2.76-2.90 (B, excl. the B2 outlier) from 3.14-3.79
#       (A/C) — about +26%. `sqe/MB` does NOT rise, so the concurrency is not
#       being bought with extra device IOs.
# ✅ 3. The `idle_dispatch` failure mode did NOT reproduce: within each bracket
#       the A/C arm's `cmt/s` is at or BELOW its B neighbours (C2 93 vs B4 134 /
#       B5 127; A2 101 vs B3 122 / B4 134). The floor did its job.
# ⛔ 4. THROUGHPUT IS UNMEASURABLE ON AN AGED POOL. `sc%` (stripe_capable) hit
#       0 during the BURN and stayed there for 8 of 9 segments, with `err`
#       climbing to 1.4 M — the writers were spinning in alloc. `host` therefore
#       decays with TIME, not with arm (B3 306 -> B4 326 -> B5 295 on the SAME
#       shipped settings), so the -3% mean bracket delta for A/C means nothing.
#       The pool entering this session was already at largest_run=60 /
#       stripe_capable=21%, and pure randwrite burns that in ~5 minutes.
#       ⇒ A throughput verdict needs a REBUILT pool (chunklet-init --force,
#         refill the volume once, then A/B). Idling recovers the reserve
#         (11% -> 26% in 8 min, retired_depth 7.0M -> 66) but not the
#         contiguity: largest_run stayed at 144.
# ⚠ 5. FIXED AFTER THIS RUN: `req/b` sat at 1.94-2.26 instead of ~1.0 because
#       the aggregator tested the floor only on `try_recv == Empty`, and a
#       sibling chunk of the same producer request is always already queued —
#       so it paired two chunks and never consulted the floor. The floor is now
#       tested before `try_recv`, and the derived floor is target/2 rather than
#       target (a chunk lands just UNDER the target, so target was unreachable).
#       Expect `req/b` -> ~1.0 and `conc` above 3.79 on the next run. ⚠ Set
#       FLOOR below TARGET for the same reason; 3/4 of it is the default here.
#
# JUDGE ON (attribution first, throughput LAST), all from tools/flush_delta.py
# against the per-segment status pair unless noted:
#   1. flush_writer_batch read_active_cycles/cycles   from `status` directly.
#      The falsifiable premise for C/D: must be ~1.0 under 70/30.
#   2. lv3_batch requests/batch          8.6-9.8 -> ~1
#      lv3_batch bytes_at_dispatch/batch must NOT fall below the 1 MiB target
#      (if it does, the floor did not hold and this is idle_dispatch again)
#   3. device concurrency                0.85 -> >=3   <- THE primary metric
#   4. chunklet_submit_drain_data merge + sqes/call. merge WILL fall (cross-lane
#      adjacency is what the fan-in was buying). sqes per BYTE must not rise —
#      if it does, the arm is buying concurrency with extra device IOs.
#   5. flush_writer_ns.meta_commit share, metadb commits/s, commit_executor_load
#      queue_depth. commits/s must NOT rise; C should LOWER it. A 5x rise is the
#      idle_dispatch failure mode — stop the run.
#   6. iostat aqu-sz ($22) / %util ($23), filtered to /^nvme[0-9]+n1 /.
#   7. host MiB/s, buffer_pending_entries and fill_pct (NOT physical_fill_pct,
#      which is anti-correlated with real backlog).
#   8. flush:errors / crc_errors / decompress_errors must be 0. Discard any
#      segment with a non-zero delta.
#
# RWMIX: default 0 (pure write) because that is where host throughput IS the
# drain rate. Run it a SECOND time with RWMIX=70 — the 250 MB/s shows up in both
# shapes, and only the 70/30 run can confirm judge #1.
set -u

TAG=${1:?usage: lv3_concurrency_ab.sh <tag> [config]}
CFG=${2:-/root/onyx_storage/config/nvme-chunklet.toml}
B=/root/onyx_storage/target/release/onyx-storage
OUT=/root/lv3conc/$TAG
BURN=${BURN:-420}
SEG=${SEG:-180}
RWMIX=${RWMIX:-0}
# One 1 MiB chunk is ~42 RAID6 6+2 stripes — still a full-size device call, and
# it turns a 4 MiB lane batch into four independent ones.
TARGET=${TARGET:-1048576}
# 3/4 of TARGET: a chunk is cut just UNDER the target, so a floor equal to
# the target is unreachable and early dispatch silently never fires.
FLOOR=${FLOOR:-786432}
# 256 units ~ 1 MiB, so one lane request clears the aggregator's target on its
# own. The deadline MUST rise with the target or the lane just times out at the
# old size.
WUNITS=${WUNITS:-256}
WCOALESCE=${WCOALESCE:-2000}
SEGMENTS=${SEGMENTS:-"B1 A1 B2 C1 B3 D1 B4 A2 C2"}

mkdir -p "$OUT"
# The ublk dev_id INCREMENTS every time a device is torn down, so a hardcoded
# ublkb0 silently targets the wrong (or no) device after any restart. Take the
# one the engine actually published, and refuse to guess if there are several.
if [ -n "${DEV:-}" ]; then
    :
else
    mapfile -t devs < <(ls /dev/ublkb* 2>/dev/null)
    case ${#devs[@]} in
        0) echo "FAIL: no /dev/ublkb* — start the engine first" >&2; exit 1 ;;
        1) DEV=${devs[0]} ;;
        *) echo "FAIL: several ublk devices (${devs[*]}) — pass DEV=/dev/ublkbN" >&2; exit 1 ;;
    esac
fi
[ -b "$DEV" ] || { echo "FAIL: $DEV is not a block device" >&2; exit 1; }

# Every arm reads the knob back off the LIVE engine and refuses to continue on a
# mismatch, so a segment can never silently measure the previous arm.
set_arm() {
    local seg="$1" target="$2" floor="$3" wunits="$4" wcoalesce="$5"
    {
        echo "arm $seg: lv3-batch target=$target floor=$floor  writer-batch units=$wunits coalesce_us=$wcoalesce"
        "$B" -c "$CFG" lv3-batch "$target" "$floor"
        "$B" -c "$CFG" writer-batch "$wunits" "$wcoalesce" 0
    } >"$OUT/knob.$seg" 2>&1
    local want_target=$target want_units=$wunits
    [ "$target" = 0 ] && want_target=4194304
    [ "$wunits" = 0 ] && want_units=32
    grep -q "target_bytes=$want_target" "$OUT/knob.$seg" || {
        echo "FAIL: $seg lv3-batch target did not reach the engine" >&2
        cat "$OUT/knob.$seg" >&2; exit 3; }
    grep -q "read_active_target_units=$want_units" "$OUT/knob.$seg" || {
        echo "FAIL: $seg writer-batch target did not reach the engine" >&2
        cat "$OUT/knob.$seg" >&2; exit 3; }
}

NSEG=$(echo "$SEGMENTS" | wc -w)
RUNTIME=$((BURN + NSEG * SEG + 240))
if [ "$RWMIX" = 0 ]; then
    RW_ARGS="--rw=randwrite"
else
    RW_ARGS="--rw=randrw --rwmixread=$RWMIX"
fi
# shellcheck disable=SC2086
fio --name=onyx-lv3conc --filename="$DEV" --direct=1 --ioengine=io_uring \
    $RW_ARGS --bs=4k-32k --iodepth=16 --numjobs=16 \
    --runtime="$RUNTIME" --time_based --group_reporting --refill_buffers \
    --write_bw_log="$OUT/bw" --log_avg_msec=5000 >"$OUT/fio.log" 2>&1 &
FIO_PID=$!
{
    echo "fio pid $FIO_PID dev $DEV runtime ${RUNTIME}s rwmix=$RWMIX segments: $SEGMENTS"
    echo "B = shipped   A = target/floor $TARGET/$FLOOR   C = A + writer $WUNITS/$WCOALESCE   D = writer only"
} >"$OUT/plan"

# The ring has to pin before any segment: at RWMIX=0 it climbs 6% -> 100% over
# roughly the first 7 minutes, and a segment sampled during the climb measures
# burst length instead of drain rate.
sleep "$BURN"
echo "burn done $(date -Is)" >>"$OUT/plan"

for seg in $SEGMENTS; do
    case "$seg" in
        B*) set_arm "$seg" 0 0 0 0 ;;
        A*) set_arm "$seg" "$TARGET" "$FLOOR" 0 0 ;;
        C*) set_arm "$seg" "$TARGET" "$FLOOR" "$WUNITS" "$WCOALESCE" ;;
        D*) set_arm "$seg" 0 0 "$WUNITS" "$WCOALESCE" ;;
        *)  echo "unknown segment $seg (must start with B, A, C or D)" >&2; exit 2 ;;
    esac
    date +%s >"$OUT/t.$seg.start"
    "$B" -c "$CFG" status >"$OUT/status.$seg.start" 2>/dev/null
    sleep $((SEG - 40))
    iostat -xm 20 2 >"$OUT/iostat.$seg" 2>/dev/null
    "$B" -c "$CFG" status >"$OUT/status.$seg.end" 2>/dev/null
    date +%s >"$OUT/t.$seg.end"
    echo "segment $seg done $(date -Is)" >>"$OUT/plan"
done

# Leave the knobs on the shipped default so a later run is not silently armed.
set_arm restore 0 0 0 0
echo "done; engine and fio still running."
echo "report: tools/lv3_concurrency_report.sh $TAG"
