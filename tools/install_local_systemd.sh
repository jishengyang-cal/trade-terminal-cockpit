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

This helper only prepares the local editable profile. Service installation,
reload, start, stop, restart policy, and boot-time activation belong to the
trading-machine deployment process, not this terminal cockpit repository.

Run preflight first:
  tools/check_external_integration.py --env-file "${CONFIG_DIR}/external.env"

Then use the trading-machine deployment process for any service mutation.
EOF
