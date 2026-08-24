#!/bin/bash
# One-line-per-segment table for tools/lv3_submit_ab.sh runs.
#
#   lv3_submit_report.sh <run_dir> [segments...]
#
# Pulls the judge metrics out of tools/flush_delta.py (the submit-shape ones) and
# out of the run's iostat samples (the device-depth ones), so a segment's verdict
# can be read without re-deriving anything. Order of the columns is the order the
# claim has to survive: barrier gone -> call shorter -> device deeper -> throughput.
set -u
DIR=${1:?usage: lv3_submit_report.sh <run_dir> [segments...]}
shift || true
SEGS=${*:-$(ls "$DIR" | sed -n 's/^t\.\(.*\)\.start$/\1/p' | sort)}
TOOLS=$(dirname "$0")

printf "%-5s %8s %8s %8s %8s %8s %8s %8s %8s %7s %7s %7s %6s\n" \
    seg host_MiB LV3_MB/s ms/call write_ms comp_ms wav/call ms/wave ent/call sqe/cl merge conc aqu
for seg in $SEGS; do
    s="$DIR/status.$seg.start"; e="$DIR/status.$seg.end"
    [ -f "$e" ] || continue
    w=$(( $(cat "$DIR/t.$seg.end") - $(cat "$DIR/t.$seg.start") ))
    host=$(python3 - "$s" "$e" "$w" <<'PY'
import sys,re
def parse(p):
    d={}
    for line in open(p):
        pre,_,rest=line.partition(":")
        for tok in rest.split():
            k,_,v=tok.partition("=")
            if v.isdigit(): d[pre.strip()+"."+k]=int(v)
    return d
a,b=parse(sys.argv[1]),parse(sys.argv[2]); w=float(sys.argv[3])
print("%.1f" % ((b.get("volume_bytes.write",0)-a.get("volume_bytes.write",0))/w/1048576))
PY
)
    fd=$(python3 "$TOOLS/flush_delta.py" "$s" "$e" "$w" 2>/dev/null)
    lv3=$(echo "$fd"   | sed -n 's/.*lv3 write bytes \([0-9.]*\) MB\/s.*/\1/p' | head -1)
    conc=$(echo "$fd"  | sed -n 's/.*device concurrency \([0-9.]*\) calls.*/\1/p' | head -1)
    r6=$(echo "$fd"    | sed -n 's/^  total *\([0-9.]*\) ms\/call.*/\1/p' | head -1)
    wr=$(echo "$fd"    | sed -n 's/^    write  *[0-9.]* s  *[0-9.]*% of total  *\([0-9.]*\) ms\/call.*/\1/p' | head -1)
    cp=$(echo "$fd"    | sed -n 's/^    compute  *[0-9.]* s  *[0-9.]*% of total  *\([0-9.]*\) ms\/call.*/\1/p' | head -1)
    dd=$(echo "$fd"    | grep -A2 "^  drain_data")
    wav=$(echo "$dd"   | sed -n 's/.*waves\/call *\([0-9.]*\).*/\1/p' | head -1)
    sqe=$(echo "$dd"   | sed -n 's/.*sqes\/call *\([0-9.]*\).*/\1/p' | head -1)
    mrg=$(echo "$dd"   | sed -n 's/.*merge *\([0-9.]*\)x.*/\1/p' | head -1)
    msw=$(echo "$dd"   | sed -n 's/.*wait *\([0-9.]*\) ms\/wave.*/\1/p' | head -1)
    ent=$(echo "$dd"   | sed -n 's/.*enters *\([0-9.]*\) \/call.*/\1/p' | head -1)
    # Second iostat sample only (the first is since-boot), base devices only.
    aqu=$(awk '/^Device/{n++} n==2 && /^nvme[0-9]n1/ {s+=$(NF-1); c++} END{if(c) printf "%.2f", s/c}' \
        "$DIR/iostat.$seg" 2>/dev/null)
    printf "%-5s %8s %8s %8s %8s %8s %8s %8s %8s %7s %7s %7s %6s\n" \
        "$seg" "$host" "${lv3:--}" "${r6:--}" "${wr:--}" "${cp:--}" "${wav:--}" \
        "${msw:--}" "${ent:--}" "${sqe:--}" "${mrg:--}" "${conc:--}" "${aqu:--}"
done
