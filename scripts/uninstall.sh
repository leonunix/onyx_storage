#!/usr/bin/env bash
set -euo pipefail

BINARY_NAME="onyx-storage"
INSTALL_BIN="/usr/local/bin/${BINARY_NAME}"
CONFIG_DIR="/etc/onyx-storage"
DATA_DIR="/var/lib/onyx-storage"
SERVICE_FILE="/etc/systemd/system/${BINARY_NAME}.service"
SOCKET_PATH="/var/run/onyx-storage.sock"

# Must be root
if [ "$(id -u)" -ne 0 ]; then
    echo "Error: this script must be run as root (sudo $0)"
    exit 1
fi

echo "=== Onyx Storage Engine Uninstaller ==="
echo ""

# 1. Stop service if running
echo "[1/5] Stopping service..."
if systemctl is-active --quiet "${BINARY_NAME}" 2>/dev/null; then
    systemctl stop "${BINARY_NAME}"
    echo "      Service stopped."
else
    echo "      Service not running."
fi

# 2. Disable service
echo "[2/5] Disabling service..."
if systemctl is-enabled --quiet "${BINARY_NAME}" 2>/dev/null; then
    systemctl disable "${BINARY_NAME}"
fi

# 3. Remove service file and binary
echo "[3/5] Removing service file and binary..."
rm -f "${SERVICE_FILE}"
systemctl daemon-reload
rm -f "${INSTALL_BIN}"
rm -f "${SOCKET_PATH}"

# 4. Ask about config
echo "[4/5] Configuration directory: ${CONFIG_DIR}"
if [ -d "${CONFIG_DIR}" ]; then
    read -rp "      Remove configuration? [y/N] " answer
    if [[ "${answer}" =~ ^[Yy] ]]; then
        rm -rf "${CONFIG_DIR}"
        echo "      Removed."
    else
        echo "      Kept."
    fi
fi

# 5. Ask about data
echo "[5/5] Data directory: ${DATA_DIR}"
if [ -d "${DATA_DIR}" ]; then
    read -rp "      Remove data (metadata)? [y/N] " answer
    if [[ "${answer}" =~ ^[Yy] ]]; then
        rm -rf "${DATA_DIR}"
        echo "      Removed."
    else
        echo "      Kept."
    fi
fi

echo ""
echo "=== Uninstall complete ==="
