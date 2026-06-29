#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_DIR="${XDG_CONFIG_HOME:-${HOME}/.config}/trade-terminal-cockpit"

mkdir -p "${CONFIG_DIR}"

if [ ! -f "${CONFIG_DIR}/external.env" ]; then
  cp "${ROOT_DIR}/config/external.env.example" "${CONFIG_DIR}/external.env"
  printf 'created_config=%s\n' "${CONFIG_DIR}/external.env"
fi

cat <<EOF
config:
  ${CONFIG_DIR}/external.env

User services are managed by the Imperativ target-runtime registry. This helper
only prepares the local editable profile; it does not install, reload, start, or
stop systemd units.

Run preflight first:
  tools/check_external_integration.py --env-file "${CONFIG_DIR}/external.env"

Then inspect and plan through:
  cd "${HOME}/projects/imperativ-main"
  python3 tools/target_machine_runtime_control.py validate --json
  python3 tools/target_machine_runtime_control.py status service.trade_terminal_cockpit.projectiond --json
  python3 tools/target_machine_runtime_control.py status service.trade_terminal_cockpit.command_gateway --json
  python3 tools/target_machine_runtime_control.py plan service.trade_terminal_cockpit.projectiond start --json
  python3 tools/target_machine_runtime_control.py plan service.trade_terminal_cockpit.command_gateway start --json
EOF
