#!/bin/bash
# Replay-residual CRC capture (2026-07-02 handoff, theory A/B discriminator).
#
# Forces the c3/c4 failure shape DIRECTLY instead of waiting for natural churn:
# write hard, kill -9 onyx WITHOUT draining (mark_flushed is memory-only, so a
# huge un-checkpointed tail survives in the LV2 ring), restart, and read-verify
# immediately after replay finishes. Three kill-cycles accumulate churn.
#
# Diagnostics (all env-gated, compiled in):
#   ONYX_PBA_TRACE=1        per-PBA alloc/free/retire/reclaim trail + rebuild
#                           reserved-set classifier (bm/dd bits), dumped at
#                           every CRC site as "PBA lifecycle trail".
#   ONYX_TRACE_LV3_WRITES=1 per-PBA last-LV3-write record (seq + payload crc),
#                           dumped at every CRC site.
#   ONYX_ALLOC_TRACK=1      allocator live-set: double-alloc = hard error,
#                           free-of-non-live = warn.
#
# Verdict key (per CRC victim):
#   trail free/reclaim>0 + re-alloc  -> onyx premature-free (theory B)
#   trail single-alloc/no-free, write record's payload_crc == actual_crc but
#   != mapping_crc                   -> mapping itself is wrong -> metadb
#                                       read-side corruption (theory A)
#   metadb fails to start (got Free) -> theory A hard evidence; metadb dir is
#                                       tar'd automatically as the forensic.
set -u
CFG=/root/onyx_storage/config/nvme-chunklet.toml
BIN=/root/onyx_storage/target/release/onyx-storage
DEV=/dev/ublkb0
POOL="/dev/nvme0n1 /dev/nvme1n1 /dev/nvme3n1 /dev/nvme4n1 /dev/nvme5n1 /dev/nvme6n1 /dev/nvme7n1 /dev/nvme8n1 /dev/nvme9n1"
# NB: ONYX_ALLOC_TRACK deliberately NOT set — its global BTreeSet mutex
# serialises all 16 writers and collapsed throughput on the first capture run
# (12386s cumulative alloc time, front-end stall). free_trace's alloc_count>1
# covers the double-alloc signal at a fraction of the cost.
DIAG_ENV="ONYX_PBA_TRACE=1 ONYX_TRACE_LV3_WRITES=1"
RLOG="warn,onyx_storage::engine=info"
CYCLES=${CYCLES:-3}

clean(){ sed -E 's/\x1b\[[0-9;]*m//g'; }
stopw(){ local w; $BIN -c $CFG stop >/dev/null 2>&1; for w in $(seq 1 120); do pgrep -x onyx-storage >/dev/null || break; sleep 1; done; }
cf(){ $BIN -c $CFG status 2>/dev/null | clean | grep -oE "crc_fg=[0-9]+" | head -1 | grep -oE "[0-9]+"; }
lv3h(){ $BIN -c $CFG status 2>/dev/null | clean | grep -oE "lv3_hits=[0-9]+" | head -1 | grep -oE "[0-9]+"; }

# Start onyx with diagnostics; wait for ANY ublk device (kill -9 leaks the old
# dev id, so the restart may come up as ublkb1/b2/... — discover instead of
# assuming b0; the device appears only AFTER buffer replay completes, so this
# wait IS the replay wait). Sets DEV. Returns 1 when the process died before
# the device came up (c4 metadb-corruption shape). NB: loop vars must be
# `local` — bash for-vars are process-global and would clobber the outer
# cycle counter (the "CYCLE 38" garbage from the earlier capture runs).
startw(){
  local log="$1" w d
  nohup env $DIAG_ENV RUST_LOG=$RLOG $BIN -c $CFG start -v fio-volume >> "$log" 2>&1 &
  for w in $(seq 1 400); do
    for d in /dev/ublkb*; do
      [ -e "$d" ] && { DEV="$d"; echo "(device: $DEV)"; return 0; }
    done
    if ! pgrep -x onyx-storage >/dev/null; then
      echo "!!! onyx-storage EXITED before device came up (see $log)"
      return 1
    fi
    sleep 2
  done
  echo "!!! device never appeared after 800s"
  return 1
}

metadb_forensic(){
  local tag="$1"
  local tarball="/root/metadb-corrupt-${tag}-$(date +%H%M%S).tar"
  echo "!!! preserving metadb dir -> $tarball"
  tar -cf "$tarball" -C /mnt/onyx-meta . 2>/dev/null
  ls -lh "$tarball"
}

if [ "${SKIP_INIT:-0}" != "1" ]; then
  echo "### INIT $(date) ###"
  stopw
  $BIN -c $CFG cleanup-ublk >/dev/null 2>&1
  modprobe ublk_drv 2>/dev/null
  lsblk -o NAME,SIZE,MODEL,MOUNTPOINTS | grep -v sda
  for d in $POOL; do blkdiscard "$d" && echo "discarded $d"; done
  rm -rf /mnt/onyx-meta/metadb /mnt/onyx-meta/wal
  OUT=$($BIN -c $CFG chunklet-init --force 2>/dev/null)
  L3=$(echo "$OUT" | grep -oE 'lv3_ld_id = "[^"]+"' | grep -oE '"[^"]+"' | tr -d '"')
  L2=$(echo "$OUT" | grep -oE 'lv2_ld_id = "[^"]+"' | grep -oE '"[^"]+"' | tr -d '"')
  LM=$(echo "$OUT" | grep -oE 'meta_ld_id = "[^"]+"' | grep -oE '"[^"]+"' | tr -d '"')
  [ -z "$L3" ] && { echo "chunklet-init failed"; exit 1; }
  sed -i -e "s|^lv3_ld_id = .*|lv3_ld_id = \"$L3\"|" -e "s|^lv2_ld_id = .*|lv2_ld_id = \"$L2\"|" -e "s|^meta_ld_id = .*|meta_ld_id = \"$LM\"|" $CFG
  $BIN -c $CFG create-volume -n fio-volume -s 34359738368 --compression lz4 >/dev/null 2>&1

  echo "### POPULATE $(date) (graceful stop at end) ###"
  startw /root/replay-c0.log || { echo "populate start failed"; exit 1; }
  fio --name=pf --filename=$DEV --direct=1 --ioengine=io_uring --rw=write --bs=24k \
      --iodepth=32 --numjobs=1 --size=32G --refill_buffers >/dev/null 2>&1
  fio --name=mix --filename=$DEV --direct=1 --ioengine=io_uring --rw=randrw --rwmixread=50 \
      --bsrange=4k-32k --iodepth=16 --numjobs=16 --runtime=60 --time_based \
      --refill_buffers --group_reporting >/dev/null 2>&1
  sleep 20
  stopw
fi

CYCLE_START=${CYCLE_START:-1}
for i in $(seq $CYCLE_START $CYCLES); do
  LOG=/root/replay-c$i.log; : > "$LOG"
  echo ""
  echo "### CYCLE $i: build tail -> kill -9 $(date) ###"
  # A previous cycle (or a manual session) may have left the engine running
  # with the device up — reuse it instead of double-starting.
  if pgrep -x onyx-storage >/dev/null && ls /dev/ublkb* >/dev/null 2>&1; then
    DEV=$(ls /dev/ublkb* | head -1)
    echo "(engine already running, device: $DEV)"
  else
    startw "$LOG" || { echo "!!! start failed BEFORE kill (unexpected)"; metadb_forensic "c$i-pre"; exit 1; }
  fi
  # Hammer writes; kill onyx mid-flight so the un-checkpointed tail is maximal.
  timeout 200 fio --name=slam --filename=$DEV --direct=1 --ioengine=io_uring \
      --rw=randwrite --bsrange=4k-32k --iodepth=32 --numjobs=16 --runtime=120 \
      --time_based --refill_buffers --group_reporting >/dev/null 2>&1 &
  FIO_PID=$!
  sleep 80
  $BIN -c $CFG status 2>/dev/null | clean | grep -aiE "used|pending|queue" | head -8
  echo ">>> KILL -9 onyx (fio still writing) $(date)"
  pkill -9 -x onyx-storage
  wait $FIO_PID 2>/dev/null
  $BIN -c $CFG cleanup-ublk >/dev/null 2>&1
  sleep 3

  echo "### CYCLE $i: restart -> replay -> read-verify $(date) ###"
  if ! startw "$LOG"; then
    echo "!!! CYCLE $i: engine failed to start after kill — c4 shape?"
    grep -aE "got Free|corruption|paged format" "$LOG" | clean | tail -10
    metadb_forensic "c$i"
    exit 2
  fi
  grep -aE "buffer recovery drained cleanly|pending_at_start|dirty startup" "$LOG" | clean | tail -4
  # Read-heavy window right after replay: forces foreground CRC checks over
  # replay-written blocks while DedupScanner scrubs in the background.
  timeout 200 fio --name=verify --filename=$DEV --direct=1 --ioengine=io_uring \
      --rw=randrw --rwmixread=70 --bsrange=4k-32k --iodepth=16 --numjobs=16 \
      --runtime=90 --time_based --refill_buffers --group_reporting >/dev/null 2>&1
  sleep 10
  CF=$(cf); H=$(lv3h)
  echo ">>> CYCLE $i verdict: crc_fg=${CF:-?} lv3_hits=${H:-?}"
  echo "--- CRC victims (first 6) ---"
  grep -aE "read-pool: CRC mismatch|inline read: CRC mismatch" "$LOG" | clean | sed -E 's/.*(purpose=[^ ]+ )?(pba=[0-9]+.*)/\2/' | head -6
  echo "--- write records at victims (first 8) ---"
  grep -aE "last LV3 write record" "$LOG" | clean | sed -E 's/.*(pba=[0-9]+.*)/\1/' | head -8
  echo "--- lifecycle trails at victims (first 12) ---"
  grep -aE "PBA lifecycle trail" "$LOG" | clean | sed -E 's/.*(pba=[0-9]+.*)/\1/' | head -12
  echo "--- allocator tracker hits ---"
  grep -acE "duplicate allocation" "$LOG"; grep -acE "released a non-live PBA" "$LOG"
  echo "--- overlaps free extent ---"
  grep -acE "overlaps free extent" "$LOG"
  echo "--- victim count total ---"
  grep -acE "read-pool: CRC mismatch|inline read: CRC mismatch" "$LOG"
done

echo ""
echo "### stopping engine, CAPTURE DONE $(date) ###"
stopw
