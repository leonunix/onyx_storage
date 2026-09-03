#!/bin/bash
# Interleaved ublk-vs-plugin A/B at a FIXED 4 KiB IO shape, inside ONE engine
# process (restart-per-arm on nvme-box measures run-order drift, not the arm).
#
#   frontend_ab.sh <tag> [config]
#
# U arms drive /dev/ublkb0 through the kernel + the ublk frontend; P arms drive
# the same engine through the `fio/onyx_fio_engine.so` external ioengine over
# the Direct IO unix socket, which bypasses ublk (and the kernel block layer)
# entirely. The plugin's protocol is 4 KiB-per-IO only, so the U arms are pinned
# to --bs=4k as well — this is NOT the 4k-32k profile and its absolute numbers
# are not comparable to it.
#
# What the pair separates: everything ABOVE the volume API (kernel round trip,
# ublk tag/buffer handling, per-queue dispatch) from everything BELOW it. The
# engine-side counters are recorded by both paths (`volume_ops`,
# `user_io_latency_ns`, `flush_qos foreground_outstanding`), so
# tools/read_delta.py works on either arm; `ublk_*_split_ns` is ublk-only and
# reads 0 on a P arm.
set -u

TAG=${1:?usage: frontend_ab.sh <tag> [config]}
CFG=${2:-/root/onyx_storage/config/nvme-chunklet.toml}
B=/root/onyx_storage/target/release/onyx-storage
ENGINE_SO=/root/onyx_storage/fio/onyx_fio_engine.so
SOCK=$(sed -n 's/^socket_path *= *"\(.*\)"/\1/p' "$CFG" | head -1)
OUT=/root/frontrun/$TAG
BURN=${BURN:-300}
SEG=${SEG:-240}
RWMIX=${RWMIX:-70}
JOBS=${JOBS:-16}
DEPTH=${DEPTH:-16}
VOL=${VOL:-fio-volume}
SIZE=${SIZE:-256G}
ARMS=${ARMS:-"U1 P1 U2 P2"}

mkdir -p "$OUT"
[ -e /dev/ublkb0 ] || { echo "FAIL: no /dev/ublkb0" >&2; exit 1; }
[ -S "$SOCK.io" ] || { echo "FAIL: no direct-io socket $SOCK.io" >&2; exit 1; }
[ -f "$ENGINE_SO" ] || { echo "FAIL: build the plugin first (make -C fio FIO_SOURCE_DIR=...)" >&2; exit 1; }

for arm in $ARMS; do
    case "$arm" in
        U*) fio --name="u-$arm" --filename=/dev/ublkb0 --direct=1 --ioengine=io_uring \
                --rw=randrw --rwmixread="$RWMIX" --bs=4k --iodepth="$DEPTH" --numjobs="$JOBS" \
                --runtime=$((BURN + SEG + 60)) --time_based --group_reporting --refill_buffers \
                --write_bw_log="$OUT/bw.$arm" --log_avg_msec=5000 >"$OUT/fio.$arm" 2>&1 & ;;
        P*) fio --name="p-$arm" --ioengine="$ENGINE_SO" \
                --onyx_socket="$SOCK" --onyx_volume="$VOL" --size="$SIZE" \
                --rw=randrw --rwmixread="$RWMIX" --bs=4k --iodepth="$DEPTH" --numjobs="$JOBS" \
                --runtime=$((BURN + SEG + 60)) --time_based --group_reporting --refill_buffers \
                --write_bw_log="$OUT/bw.$arm" --log_avg_msec=5000 >"$OUT/fio.$arm" 2>&1 & ;;
        *)  echo "unknown arm $arm (must start with U or P)" >&2; exit 2 ;;
    esac
    fiopid=$!
    echo "=== arm $arm pid $fiopid burn ${BURN}s window ${SEG}s $(date -Is)"
    sleep "$BURN"
    "$B" -c "$CFG" status >"$OUT/status.$arm.start" 2>/dev/null
    date +%s >"$OUT/t.$arm.start"
    sleep $((SEG - 40))
    iostat -xm 20 2 >"$OUT/iostat.$arm" 2>/dev/null
    "$B" -c "$CFG" status >"$OUT/status.$arm.end" 2>/dev/null
    date +%s >"$OUT/t.$arm.end"
    wait $fiopid 2>/dev/null
    echo "arm $arm done $(date -Is)"
    sleep 20
done
echo "analyse: tools/read_delta.py $OUT/status.<arm>.{start,end} <window_s>"
