#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

ENV_FILE=${TRADE_COCKPIT_ENV_FILE:-${XDG_CONFIG_HOME:-${HOME}/.config}/trade-terminal-cockpit/external.env}
RUN_PREFLIGHT=1

usage() {
  cat <<'EOF'
usage: tools/open_external_tui.sh [--env-file PATH] [--skip-preflight] [-- EXTRA_TUI_ARGS...]

Open the local terminal cockpit against external production boundaries.
This never runs cargo and never SSHes to a build VM.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --env-file)
      ENV_FILE=$2
      shift 2
      ;;
    --skip-preflight)
      RUN_PREFLIGHT=0
      shift
      ;;
    --)
      shift
      break
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      break
      ;;
  esac
done

if [ -f "${ENV_FILE}" ]; then
  set -a
  # shellcheck source=/dev/null
  . "${ENV_FILE}"
  set +a
else
  printf 'missing_env_file=%s\n' "${ENV_FILE}" >&2
  printf 'copy config/external.env.example to that path and edit it for the live integration\n' >&2
  exit 78
fi

if [ "${RUN_PREFLIGHT}" -eq 1 ]; then
  tools/check_external_integration.py --env-file "${ENV_FILE}"
fi

NATS_URL=${TRADE_COCKPIT_NATS_URL:-nats://127.0.0.1:14222}
JETSTREAM_STREAM=${TRADE_COCKPIT_JETSTREAM_STREAM:-TRADING_EVENTS}
NATS_SUBJECT=${TRADE_COCKPIT_NATS_SUBJECT:-trading.event.>}
EVENT_CODEC=${TRADE_COCKPIT_EVENT_CODEC:-protobuf}
JETSTREAM_DURABLE=${TRADE_COCKPIT_JETSTREAM_DURABLE:-trade-terminal-cockpit-local}
COMMAND_GATEWAY_ADDR=${TRADE_COCKPIT_COMMAND_GATEWAY_ADDR:-127.0.0.1:39732}
OPERATOR_ID=${TRADE_COCKPIT_OPERATOR_ID:-${USER:-operator-local}}
SESSION_ID=${TRADE_COCKPIT_SESSION_ID:-trade-terminal-cockpit-local}
TARGET_ENVIRONMENT=${TRADE_COCKPIT_TARGET_ENVIRONMENT:-paper}

args=(
  --nats-url "${NATS_URL}"
  --jetstream-stream "${JETSTREAM_STREAM}"
  --jetstream-durable "${JETSTREAM_DURABLE}"
  --nats-subject "${NATS_SUBJECT}"
  --event-codec "${EVENT_CODEC}"
  --command-gateway-addr "${COMMAND_GATEWAY_ADDR}"
  --operator-id "${OPERATOR_ID}"
  --session-id "${SESSION_ID}"
  --target-environment "${TARGET_ENVIRONMENT}"
)

if [ -n "${TRADE_COCKPIT_COMMAND_AUDIT_JSONL:-}" ]; then
  args+=(--command-gateway-audit-jsonl "${TRADE_COCKPIT_COMMAND_AUDIT_JSONL}")
fi

exec tools/open_local_tui.sh "${args[@]}" "$@"
