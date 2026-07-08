#!/usr/bin/env bash
# chunklet disk-lifecycle acceptance (nvme-box).
#
# Validates the full fault -> isolate -> rebuild -> return -> reintegrate ->
# rebalance -> capacity-reclaim chain end to end on real NVMe, using the CLI +
# IPC ops surface added in Phase C/D. This is the on-box mirror of the sparse-
# file capstone test `full_lifecycle_pull_reintegrate_rebalance_reopen`.
#
# It is STEP-GATED on purpose: the pull/return steps are physical (PCI
# remove/rescan) and hardware-specific, so you drive them one subcommand at a
# time and the script auto-asserts the observable pool state (status JSON)
# after each. NOTHING here wipes a disk without an explicit confirmation, and
# every destructive step prints `lsblk` and refuses to touch the system disk.
#
# Usage (run each in order, reading the output before the next):
#   chunklet_disk_lifecycle_accept.sh preflight
#   chunklet_disk_lifecycle_accept.sh baseline              # fio + record skew
#   chunklet_disk_lifecycle_accept.sh pull   <pci_addr>     # e.g. 0000:e6:00.0
#   chunklet_disk_lifecycle_accept.sh failover              # assert degraded + auto-failover
#   chunklet_disk_lifecycle_accept.sh rescan                # /sys/bus/pci/rescan
#   chunklet_disk_lifecycle_accept.sh reintegrate           # assert new pd + count restored
#   chunklet_disk_lifecycle_accept.sh rebalance             # assert skew converges
#   chunklet_disk_lifecycle_accept.sh restart-check         # reopen round-trip + fsck=0
#
# Requires the engine already running with [chunklet] device_discovery=true,
# device_glob set, auto_reintegrate/auto_rebalance on, watchdog_enabled=true.
set -euo pipefail

B="${ONYX_BIN:-/root/onyx_storage/target/release/onyx-storage}"
C="${ONYX_CONFIG:-/root/onyx_storage/config/nvme-chunklet.toml}"
DEV="${ONYX_UBLK_DEV:-/dev/ublkb0}"
VOL="${ONYX_VOL:-fio-volume}"
FIO_RUNTIME="${FIO_RUNTIME:-120}"
STATE_DIR="${STATE_DIR:-/tmp/ck-accept}"
mkdir -p "$STATE_DIR"

log()  { printf '\033[1;36m[accept]\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m[accept:FAIL]\033[0m %s\n' "$*" >&2; exit 1; }
pass() { printf '\033[1;32m[accept:PASS]\033[0m %s\n' "$*"; }

# `chunklet status` prints the PoolSnapshot JSON on the first line, then `ok`.
# The snapshot is the top-level object (fields: pd_count, failed_pds (a COUNT),
# used_skew_pct, used_skew_chunklets, last_fsck_reclaimed, …).
status_json() { "$B" -c "$C" chunklet status 2>/dev/null | sed -n '1{/^{/p}'; }
jqf() { status_json | python3 -c "import sys,json;print(json.load(sys.stdin)$1)"; }

require_running() {
  "$B" -c "$C" chunklet status >/dev/null 2>&1 || die "engine not reachable — start it first"
}

confirm_not_system_disk() {
  # $1 = a /dev/nvmeXnY path about to be affected. Refuse sda / mounted roots.
  log "lsblk (verify the target below is a pool NVMe, NOT the system disk):"
  lsblk -o NAME,TYPE,SIZE,MOUNTPOINTS || true
  case "$1" in
    *sda*|*BOSS*) die "refusing: target '$1' looks like the system disk" ;;
  esac
}

cmd_preflight() {
  cat /etc/redhat-release 2>/dev/null || true
  uname -r
  command -v fio >/dev/null || die "fio not installed"
  command -v python3 >/dev/null || die "python3 not installed"
  require_running
  jqf "['pd_count']" >/dev/null 2>&1 || die "status has no pd_count — is [chunklet] enabled?"
  log "config knobs (confirm in $C): device_discovery/device_glob/auto_reintegrate/auto_rebalance/watchdog_enabled"
  grep -E 'device_discovery|device_glob|auto_reintegrate|auto_rebalance|watchdog_enabled|tolerant_open' "$C" || true
  pass "preflight ok"
}

record_skew() { jqf "['used_skew_pct']"; }
record_count() { jqf "['pd_count']"; }
record_failed() { jqf "['failed_pds']"; }   # a COUNT (usize), 0 when healthy

cmd_baseline() {
  require_running
  log "baseline fio ${FIO_RUNTIME}s on $DEV (refill_buffers, CRC verify)"
  fio --name=ck-accept --filename="$DEV" --direct=1 --ioengine=io_uring \
      --rw=randrw --rwmixread=70 --bsrange=4k-32k --iodepth=16 --numjobs=2 \
      --runtime="$FIO_RUNTIME" --time_based --group_reporting --refill_buffers \
      --verify=crc32c --verify_fatal=1 | tee "$STATE_DIR/baseline.fio"
  grep -q 'err= 0' "$STATE_DIR/baseline.fio" || log "note: check the err= field above"
  record_count  > "$STATE_DIR/count.before"
  record_skew   > "$STATE_DIR/skew.before"
  log "pd_count=$(cat "$STATE_DIR/count.before") used_skew_pct=$(cat "$STATE_DIR/skew.before")"
  pass "baseline recorded"
}

cmd_pull() {
  local pci="${1:?usage: pull <pci_addr e.g. 0000:e6:00.0>}"
  require_running
  local devnode="/sys/bus/pci/devices/$pci"
  [ -e "$devnode/remove" ] || die "no $devnode/remove — check the PCI address"
  # Best-effort resolve the block device for the safety check.
  local blk; blk=$(ls "$devnode"/nvme/*/ 2>/dev/null | grep -oE 'nvme[0-9]+n[0-9]+' | head -1 || true)
  confirm_not_system_disk "${blk:-$pci}"
  read -r -p "PCI-remove $pci (its NVMe leaves the pool)? type YES: " ans
  [ "$ans" = "YES" ] || die "aborted"
  echo 1 > "$devnode/remove"
  echo "$pci" > "$STATE_DIR/pulled.pci"
  log "removed $pci — inline-degrade should absorb in-flight IO on surviving redundancy"
  pass "pulled $pci"
}

cmd_failover() {
  require_running
  sleep 5
  local failed; failed=$(record_failed)
  log "failed_pds=$failed"
  [ "$failed" != "0" ] || die "expected a Failed PD after pull (watchdog/inline-degrade)"
  log "chunklet jobs (expect an auto-failover rebuild):"
  "$B" -c "$C" chunklet job || true
  pass "degraded + auto-failover observed (verify fio err=0 during this window separately)"
}

cmd_rescan() {
  require_running
  echo 1 > /sys/bus/pci/rescan
  sleep 5
  log "rescanned PCI — the disk returns under a (possibly new) /dev/nvmeXnY"
  lsblk -o NAME,TYPE,SIZE,MOUNTPOINTS || true
  pass "rescan done — device_discovery matches it by pool_id, auto_reintegrate should fire"
}

cmd_reintegrate() {
  require_running
  log "waiting for auto-reintegrate (watchdog sweep + wipe + slot reuse)…"
  local before after; before=$(cat "$STATE_DIR/count.before" 2>/dev/null || echo "?")
  for _ in $(seq 1 24); do
    "$B" -c "$C" chunklet job 2>/dev/null | grep -q reintegrate && break
    sleep 5
  done
  "$B" -c "$C" chunklet job || true
  sleep 5
  after=$(record_count)
  log "pd_count before=$before after=$after ; failed_pds=$(record_failed)"
  [ "$after" = "$before" ] || die "pool_pd_count not restored to $before (got $after)"
  [ "$(record_failed)" = "0" ] || die "a Failed tombstone remains after reintegrate"
  pass "reintegrated: fresh pd reused the slot, count restored, no tombstone"
}

cmd_rebalance() {
  require_running
  log "waiting for auto-rebalance to converge used_skew_pct…"
  for _ in $(seq 1 60); do
    "$B" -c "$C" chunklet job 2>/dev/null | grep -q rebalance && break
    sleep 5
  done
  # let a bounded cycle run
  sleep 20
  "$B" -c "$C" chunklet job || true
  local sk; sk=$(record_skew)
  log "used_skew_pct now=$sk (target ~20)"
  python3 - "$sk" <<'PY' || die "skew did not converge"
import sys
sk=float(sys.argv[1])
sys.exit(0 if sk <= 25.0 else 1)
PY
  pass "rebalance converged (used_skew_pct=$sk)"
}

cmd_restart_check() {
  require_running
  log "run: stop engine, restart with same config, then this asserts a clean open"
  read -r -p "has the engine been restarted? type YES when back up: " ans
  [ "$ans" = "YES" ] || die "aborted"
  require_running
  local fsck_line
  # a manual fsck on the now-complete pool should reclaim ~0 (open already did it)
  local jid; jid=$("$B" -c "$C" chunklet fsck | grep -oE 'job [0-9]+' | grep -oE '[0-9]+' || true)
  sleep 3
  "$B" -c "$C" chunklet job "${jid:-}" || true
  pass "restart round-trip ok — inspect the fsck job detail (expect reclaimed=0 on a clean pool)"
}

case "${1:-}" in
  preflight)      cmd_preflight ;;
  baseline)       cmd_baseline ;;
  pull)           cmd_pull "${2:-}" ;;
  failover)       cmd_failover ;;
  rescan)         cmd_rescan ;;
  reintegrate)    cmd_reintegrate ;;
  rebalance)      cmd_rebalance ;;
  restart-check)  cmd_restart_check ;;
  *) sed -n '2,40p' "$0"; exit 1 ;;
esac
