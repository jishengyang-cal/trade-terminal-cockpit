#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
UNIT_DIR="${HOME}/.config/systemd/user"
UNIT_NAME="trade-terminal-cockpit-paper-bridge.service"
ENV_FILE="${TRADE_COCKPIT_ENV_FILE:-${HOME}/.config/trade-terminal-cockpit/external.env}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

mkdir -p "${UNIT_DIR}"

cat >"${UNIT_DIR}/${UNIT_NAME}" <<UNIT
[Unit]
Description=trade-terminal-cockpit paper account/strategy observability bridge
After=trade-terminal-cockpit-state-projectiond.service
Wants=trade-terminal-cockpit-state-projectiond.service

[Service]
Type=simple
WorkingDirectory=${ROOT}
ExecStart=${PYTHON_BIN} ${ROOT}/tools/publish_paper_cockpit_bridge.py --env-file ${ENV_FILE} --interval-sec 2 --json
Restart=always
RestartSec=2
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=default.target
UNIT

systemctl --user daemon-reload
systemctl --user enable --now "${UNIT_NAME}"

if command -v loginctl >/dev/null 2>&1; then
  loginctl enable-linger "${USER}" >/dev/null 2>&1 || true
fi

systemctl --user --no-pager status "${UNIT_NAME}"
