#!/bin/bash
# Interleaved ublk-vs-plugin A/B inside ONE engine process (restart-per-arm on
# nvme-box measures run-order drift, not the arm).
#
#   frontend_ab.sh <tag> [config]
#
# U arms drive the ublk block device through the kernel + the ublk frontend; P
# arms drive the same engine through the `fio/onyx_fio_engine.so` external
# ioengine over the Direct IO unix socket, which bypasses ublk (and the kernel
# block layer) entirely.
#
# ⭐ WHAT THIS PAIR IS FOR, as of protocol 3.
#
# Throughput is already settled and is NOT the question: at matched QD256 the
# two frontends measured identical to within 0.4% on every metric including
# every clat percentile (memory `direct_io_matches_ublk_at_matched_qd`), because
# the ceiling lives below both of them in the shared engine core (READ is 98%
# `engine_ns`, WRITE 85% `durable_wait_ns`).
#
# The open question is CPU: ublk copies every payload through
# `copy_to/from_user` on fio's behalf, the plugin does not, and that copy is the
# prime suspect for the ~20 CPUs of %sys nobody has profiled. Two arms at the
# SAME IO shape and (known) the same throughput turn the CPU difference into a
# direct read-out of ublk's kernel tax.
#
# ⚠ Which is why this records THREE CPU accounts per window (see
# tools/cpu_snapshot.py): system-wide, the engine, and the fio tree. The fio one
# is load-bearing, not decoration — the plugin runs its protocol client inside
# fio's own threads, so measuring only the engine would show the plugin's cost
# having moved house and call it a saving. Judge on total machine CPU per GiB.
#
# Engine-side counters are recorded by both paths (`volume_bytes`, `volume_ops`,
# `user_io_latency_ns`, `flush_qos foreground_outstanding`), so
# tools/read_delta.py works on either arm; `ublk_*_split_ns` is ublk-only and
# reads 0 on a P arm.
#
# BS defaults to the canonical 4k-32k, which protocol 3 made possible on the
# plugin (it was 4 KiB-only before, so the old pairs pinned BOTH arms to 4k and
# were not comparable to the 712 MiB/s baseline). Both arms always use the same
# shape; that is the one thing that must not vary.
set -u

TAG=${1:?usage: frontend_ab.sh <tag> [config]}
CFG=${2:-/root/onyx_storage/config/nvme-chunklet.toml}
B=/root/onyx_storage/target/release/onyx-storage
ENGINE_SO=/root/onyx_storage/fio/onyx_fio_engine.so
SNAP=/root/onyx_storage/tools/cpu_snapshot.py
SOCK=$(sed -n 's/^socket_path *= *"\(.*\)"/\1/p' "$CFG" | head -1)
OUT=/root/frontrun/$TAG
BURN=${BURN:-300}
SEG=${SEG:-240}
RWMIX=${RWMIX:-70}
JOBS=${JOBS:-16}
DEPTH=${DEPTH:-16}
VOL=${VOL:-fio-volume}
BS=${BS:-4k-32k}
ARMS=${ARMS:-"U1 P1 U2 P2"}

# ⛔ A RANGE MUST GO THROUGH --bsrange, NEVER --bs. Verified on fio 3.36:
#   --bs=4k-32k       -> bs=(R) 4096B-4096B, (W) 32.0KiB-32.0KiB
#   --bsrange=4k-32k  -> bs=(R) 4096B-32.0KiB, (W) 4096B-32.0KiB
# `--bs` takes a per-direction LIST, so it reads "4k-32k" as "4k for reads,
# 32k for writes" and runs a completely different workload WITHOUT complaining.
# `--bsrange` in turn rejects a bare single value, so the form is chosen here
# rather than left to whoever sets BS.
case "$BS" in
    *-*) BS_OPT="--bsrange=$BS" ;;
    *)   BS_OPT="--bs=$BS" ;;
esac
# The device id increments every time a killed engine leaves a stale one behind,
# so it is discovered, not assumed. Override with UBLK=/dev/ublkbN.
UBLK=${UBLK:-$(ls -1 /dev/ublkb* 2>/dev/null | head -1)}

mkdir -p "$OUT"
[ -n "$UBLK" ] && [ -e "$UBLK" ] || { echo "FAIL: no /dev/ublkb* device" >&2; exit 1; }
[ -S "$SOCK.io" ] || { echo "FAIL: no direct-io socket $SOCK.io" >&2; exit 1; }
[ -f "$ENGINE_SO" ] || { echo "FAIL: build the plugin first (make -C fio FIO_SOURCE_DIR=...)" >&2; exit 1; }
[ -f "$SNAP" ] || { echo "FAIL: missing $SNAP" >&2; exit 1; }
command -v mpstat >/dev/null || echo "note: mpstat absent; /proc/stat deltas still cover the window" >&2

echo "=== frontend_ab $TAG: ublk=$UBLK bs=$BS rwmix=$RWMIX j${JOBS}d${DEPTH} burn=${BURN}s window=${SEG}s"
echo "=== arms: $ARMS"

for arm in $ARMS; do
    case "$arm" in
        U*) fio --name="u-$arm" --filename="$UBLK" --direct=1 --ioengine=io_uring \
                --rw=randrw --rwmixread="$RWMIX" $BS_OPT --iodepth="$DEPTH" --numjobs="$JOBS" \
                --runtime=$((BURN + SEG + 60)) --time_based --group_reporting --refill_buffers \
                --write_bw_log="$OUT/bw.$arm" --log_avg_msec=5000 >"$OUT/fio.$arm" 2>&1 & ;;
        # No --size: protocol 3 reports the volume capacity at HELLO, so fio
        # derives the same range the U arms get from the block device.
        P*) fio --name="p-$arm" --ioengine="$ENGINE_SO" \
                --onyx_socket="$SOCK" --onyx_volume="$VOL" \
                --rw=randrw --rwmixread="$RWMIX" $BS_OPT --iodepth="$DEPTH" --numjobs="$JOBS" \
                --runtime=$((BURN + SEG + 60)) --time_based --group_reporting --refill_buffers \
                --write_bw_log="$OUT/bw.$arm" --log_avg_msec=5000 >"$OUT/fio.$arm" 2>&1 & ;;
        *)  echo "unknown arm $arm (must start with U or P)" >&2; exit 2 ;;
    esac
    fiopid=$!
    echo "=== arm $arm pid $fiopid burn ${BURN}s window ${SEG}s $(date -Is)"
    sleep "$BURN"

    # Window opens. The CPU snapshot is taken as close to the status read as
    # possible so both deltas cover the same interval.
    "$B" -c "$CFG" status >"$OUT/status.$arm.start" 2>/dev/null
    python3 "$SNAP" "$OUT/cpu.$arm.start"
    date +%s >"$OUT/t.$arm.start"

    # mpstat and iostat run CONCURRENTLY with the window rather than eating the
    # last 40 s of it, so the CPU delta and the device view describe the same
    # interval instead of adjacent ones.
    #
    # ⚠ Their pids are waited on INDIVIDUALLY. A bare `wait` would also wait on
    # $fiopid, which outlives the window by design, and the window would silently
    # stretch to fio's whole runtime.
    samplers=""
    if command -v mpstat >/dev/null; then
        mpstat 20 $((SEG / 20)) >"$OUT/mpstat.$arm" 2>/dev/null &
        samplers="$samplers $!"
    fi
    iostat -xm 20 $((SEG / 20)) >"$OUT/iostat.$arm" 2>/dev/null &
    samplers="$samplers $!"
    sleep "$SEG"
    for pid in $samplers; do wait "$pid" 2>/dev/null; done

    python3 "$SNAP" "$OUT/cpu.$arm.end"
    "$B" -c "$CFG" status >"$OUT/status.$arm.end" 2>/dev/null
    date +%s >"$OUT/t.$arm.end"

    # fio is allowed to finish rather than killed, so its summary survives:
    # `err=`, the realised bs distribution, and clat percentiles are all worth
    # having. ⛔ But fio's averages span burn+window, so they are NOT this arm's
    # throughput — that comes from the status delta.
    wait "$fiopid" 2>/dev/null
    echo "arm $arm done $(date -Is)"
    sleep 20
done

echo
echo "analyse: tools/frontend_cpu_report.py $OUT"
echo "         tools/read_delta.py $OUT/status.<arm>.{start,end} $SEG"
