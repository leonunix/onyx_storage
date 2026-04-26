#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY_NAME="onyx-storage"
INSTALL_BIN="/usr/local/bin/${BINARY_NAME}"
CONFIG_DIR="/etc/onyx-storage"
CONFIG_FILE="${CONFIG_DIR}/config.toml"
DATA_DIR="/var/lib/onyx-storage"
SERVICE_FILE="/etc/systemd/system/${BINARY_NAME}.service"

usage() {
    echo "Usage: $0 [--skip-build]"
    echo ""
    echo "Install the Onyx Storage Engine as a systemd service."
    echo ""
    echo "Options:"
    echo "  --skip-build   Skip cargo build (use existing binary)"
    echo "  --help         Show this help"
    exit 0
}

SKIP_BUILD=0
for arg in "$@"; do
    case "$arg" in
        --skip-build) SKIP_BUILD=1 ;;
        --help|-h)    usage ;;
        *)            echo "Unknown option: $arg"; usage ;;
    esac
done

# Must be root
if [ "$(id -u)" -ne 0 ]; then
    echo "Error: this script must be run as root (sudo $0)"
    exit 1
fi

echo "=== Onyx Storage Engine Installer ==="
echo ""

# 1. Build release binary
if [ "$SKIP_BUILD" -eq 0 ]; then
    echo "[1/6] Building release binary..."
    cd "$PROJECT_DIR"
    cargo build --release
    echo "      Build complete."
else
    echo "[1/6] Skipping build (--skip-build)"
    if [ ! -f "${PROJECT_DIR}/target/release/${BINARY_NAME}" ]; then
        echo "Error: ${PROJECT_DIR}/target/release/${BINARY_NAME} not found."
        echo "       Run 'cargo build --release' first, or remove --skip-build."
        exit 1
    fi
fi

# 2. Install binary
echo "[2/6] Installing binary to ${INSTALL_BIN}..."
install -m 0755 "${PROJECT_DIR}/target/release/${BINARY_NAME}" "${INSTALL_BIN}"

# 3. Create config directory + default standby config
echo "[3/6] Setting up configuration..."
mkdir -p "${CONFIG_DIR}"
if [ ! -f "${CONFIG_FILE}" ]; then
    cat > "${CONFIG_FILE}" << 'TOML'
# Onyx Storage Engine Configuration
#
# On first install, everything is commented out.
# The engine starts in BARE mode (IPC socket only, no storage).
#
# Configure via dashboard or edit this file, then:
#   systemctl reload onyx-storage
#
# Mode transitions:
#   bare   → standby : configure [meta] section (path)
#   standby → active  : also configure [storage] + [buffer] sections
#   bare   → active   : configure all three at once

# [meta]
# path = "/var/lib/onyx-storage/meta"
# block_cache_mb = 256

# [storage]
# data_device = "/dev/vg0/onyx-data"
# block_size = 4096
# use_hugepages = false
# default_compression = "Lz4"

# [buffer]
# device = "/dev/vg0/onyx-buffer"
# capacity_mb = 16384
# flush_watermark_pct = 80
# group_commit_wait_us = 250

# [ublk]
# nr_queues = 4
# queue_depth = 128
# io_buf_bytes = 1048576

# [gc]
# enabled = true
# scan_interval_ms = 5000
# dead_ratio_threshold = 0.25

# [dedup]
# enabled = true
# workers = 2

# [service]
# socket_path = "/var/run/onyx-storage.sock"
TOML
    echo "      Default config written to ${CONFIG_FILE}"
else
    echo "      Config already exists at ${CONFIG_FILE} (not overwritten)"
fi

# 4. Create data directory (only if not on a separate LV)
echo "[4/6] Creating data directory (if needed)..."
mkdir -p "${DATA_DIR}"

# 5. Install systemd service file
echo "[5/6] Installing systemd service..."
install -m 0644 "${PROJECT_DIR}/dist/${BINARY_NAME}.service" "${SERVICE_FILE}"
systemctl daemon-reload

# 6. Enable service (but don't start yet)
echo "[6/6] Enabling service..."
systemctl enable "${BINARY_NAME}"

echo ""
echo "=== Installation complete ==="
echo ""
echo "The engine is installed but NOT started yet."
echo ""
echo "Quick start (standby mode, no devices needed):"
echo "  systemctl start ${BINARY_NAME}"
echo ""
echo "To configure storage devices:"
echo "  1. Set up dm-raid / LVM as needed"
echo "  2. Edit ${CONFIG_FILE}"
echo "     - Uncomment [storage] and set data_device"
echo "     - Uncomment [buffer] and set device"
echo "  3. systemctl reload ${BINARY_NAME}"
echo "     (or: ${BINARY_NAME} --config ${CONFIG_FILE} reload)"
echo ""
echo "To check status:"
echo "  systemctl status ${BINARY_NAME}"
echo "  ${BINARY_NAME} --config ${CONFIG_FILE} status"
echo ""
