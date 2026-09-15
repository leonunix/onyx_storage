#!/bin/bash
# One-line-per-segment table for tools/lv3_concurrency_ab.sh runs.
#
#   lv3_concurrency_report.sh <run_dir> [segments...]
#
# Column order IS the order the claim has to survive, so a segment can be
# rejected at the first column that fails instead of on throughput:
#
#   ra%      read_active_cycles / cycles      the premise for the C/D arms
#   req/b    lv3_batch requests per batch     did the fan-in go away (8.6-9.8 -> ~1)
#   KiB/b    bytes at dispatch per batch      is each call still full-size (>= target)
#   conc     device calls in flight           THE metric (0.85 -> >=3)
#   sqe/MB   submit SQEs per MB of LV3 data   did concurrency cost extra device IOs
#   merge    chunklet adjacency merge         expected to FALL; only sqe/MB matters
#   cmt/s    metadb commits per second        must NOT rise (idle_dispatch failed here)
#   mc%      meta_commit share of the writer  same failure mode, second view
#   aqu      mean per-drive queue depth       did the DEVICE finally get deeper
#   host     host MiB/s                       read LAST
#   sc%      stripe_capable at segment END    ⚠ THE dominant confound on an aged
#                                             pool: pure randwrite burns the whole
#                                             stripe reserve in ~5 min, and once it
#                                             hits 0 the writers spin in alloc and
#                                             every later column measures the STALL,
#                                             not the arm. A segment whose sc% ends
#                                             at 0 is unusable, and any pair whose
#                                             sc% differs a lot is not comparable.
#   err      flush errors delta               non-zero => discard the segment
set -u
DIR=${1:?usage: lv3_concurrency_report.sh <run_dir> [segments...]}
shift || true
SEGS=${*:-$(ls "$DIR" | sed -n 's/^t\.\(.*\)\.start$/\1/p' | sort)}
TOOLS=$(dirname "$0")

printf "%-5s %5s %6s %7s %6s %8s %6s %7s %5s %6s %8s %5s %6s\n" \
    seg "ra%" req/b KiB/b conc sqe/MB merge cmt/s "mc%" aqu host "sc%" err
for seg in $SEGS; do
    s="$DIR/status.$seg.start"; e="$DIR/status.$seg.end"
    [ -f "$e" ] || continue
    w=$(( $(cat "$DIR/t.$seg.end") - $(cat "$DIR/t.$seg.start") ))

    # Everything that is a plain counter delta comes straight off the two status
    # files; only the derived ledger goes through flush_delta.py.
    read -r ra req kib cmt host sc err <<EOF
$(python3 - "$s" "$e" "$w" <<'PY'
import sys
def parse(p):
    d={}
    for line in open(p):
        pre,_,rest=line.partition(":")
        pre=pre.strip()
        for tok in rest.split():
            k,_,v=tok.partition("=")
            if v.isdigit(): d[pre+"."+k]=int(v)
    return d
a,b=parse(sys.argv[1]),parse(sys.argv[2]); w=float(sys.argv[3])
def d(k): return b.get(k,0)-a.get(k,0)
cycles=d("flush_writer_batch.cycles")
ra=100.0*d("flush_writer_batch.read_active_cycles")/cycles if cycles else float("nan")
batches=(d("lv3_batch.window_timeouts")+d("lv3_batch.target_hits")
         +d("lv3_batch.idle_dispatches"))
reqs=d("lv3_batch.requests")
req_per_batch=reqs/batches if batches else float("nan")
kib=d("lv3_batch.bytes_at_dispatch")/batches/1024 if batches else float("nan")
# metadb transactions the writer path issued; falls back to commit-worker jobs
# on an older status format.
cmt=d("flush_writer_meta.commits") or d("flush_writer_batch.commit_worker_jobs")
# stripe_capable is a percentage in the status text, not a counter pair, so it
# is parsed from the END sample rather than differenced.
sc=float("nan")
for line in open(sys.argv[2]):
    if line.startswith("allocator_contiguity:") and "stripe_capable=" in line:
        if "(" in line.split("stripe_capable=",1)[1]:
            pct=line.split("stripe_capable=",1)[1].split("(",1)[1].split(")")[0]
            sc=float(pct.rstrip("%"))
        break
print("%.1f %.2f %.0f %.1f %.1f %.0f %d" % (
    ra, req_per_batch, kib, cmt/w,
    (d("volume_bytes.write"))/w/1048576, sc, d("flush.errors")))
PY
)
EOF

    fd=$(python3 "$TOOLS/flush_delta.py" "$s" "$e" "$w" 2>/dev/null)
    conc=$(echo "$fd" | sed -n 's/.*device concurrency \([0-9.]*\) calls.*/\1/p' | head -1)
    lv3mb=$(echo "$fd" | sed -n 's/.*lv3 write bytes \([0-9.]*\) MB\/s.*/\1/p' | head -1)
    dd=$(echo "$fd"   | grep -A2 "^  drain_data")
    sqe=$(echo "$dd"  | sed -n 's/.*sqes\/call *\([0-9.]*\).*/\1/p' | head -1)
    mrg=$(echo "$dd"  | sed -n 's/.*merge *\([0-9.]*\)x.*/\1/p' | head -1)
    mc=$(echo "$fd"   | sed -n 's/^ *meta_commit *[0-9.]* s *\([0-9.]*\)%.*/\1/p' | head -1)
    # SQEs per MB of LV3 payload — the only form in which the merge collapse
    # matters. Needs the per-call sqe count and calls/s, both from flush_delta.
    calls=$(echo "$fd" | sed -n 's/.*batches \([0-9]*\) (\([0-9.]*\)\/s).*/\2/p' | head -1)
    sqepmb=$(python3 -c "
import sys
try:
    sqe=float('$sqe'); calls=float('$calls'); mb=float('$lv3mb')
    print('%.0f' % (sqe*calls/mb)) if mb else print('-')
except Exception: print('-')
")
    aqu=$(awk '/^Device/{n++} n==2 && /^nvme[0-9]n1/ {s+=$(NF-1); c++} END{if(c) printf "%.2f", s/c}' \
        "$DIR/iostat.$seg" 2>/dev/null)

    printf "%-5s %5s %6s %7s %6s %8s %6s %7s %5s %6s %8s %5s %6s\n" \
        "$seg" "$ra" "$req" "$kib" "${conc:--}" "${sqepmb:--}" "${mrg:--}" \
        "$cmt" "${mc:--}" "${aqu:--}" "$host" "$sc" "$err"
done
