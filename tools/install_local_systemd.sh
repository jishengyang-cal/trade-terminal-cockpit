#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
USER_SYSTEMD_DIR="${XDG_CONFIG_HOME:-${HOME}/.config}/systemd/user"
CONFIG_DIR="${XDG_CONFIG_HOME:-${HOME}/.config}/trade-terminal-cockpit"

mkdir -p "${USER_SYSTEMD_DIR}" "${CONFIG_DIR}"

if [ ! -f "${CONFIG_DIR}/external.env" ]; then
  cp "${ROOT_DIR}/config/external.env.example" "${CONFIG_DIR}/external.env"
  printf 'created_config=%s\n' "${CONFIG_DIR}/external.env"
fi

ln -sf "${ROOT_DIR}/systemd/user/trade-terminal-cockpit-state-projectiond.service" \
  "${USER_SYSTEMD_DIR}/trade-terminal-cockpit-state-projectiond.service"
ln -sf "${ROOT_DIR}/systemd/user/trade-terminal-cockpit-command-gateway.service" \
  "${USER_SYSTEMD_DIR}/trade-terminal-cockpit-command-gateway.service"

systemctl --user daemon-reload

cat <<EOF
installed_units:
  trade-terminal-cockpit-state-projectiond.service
  trade-terminal-cockpit-command-gateway.service
config:
  ${CONFIG_DIR}/external.env

These units are installed but not started. Run preflight first:
  tools/check_external_integration.py --env-file "${CONFIG_DIR}/external.env"
EOF
