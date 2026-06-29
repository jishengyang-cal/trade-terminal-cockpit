#!/usr/bin/env bash
set -euo pipefail

expand_path() {
  local value=$1
  value=${value/#\~/${HOME}}
  value=${value/#\$HOME/${HOME}}
  printf '%s' "${value}"
}

exec "$(expand_path "${TRADE_COCKPIT_STATE_PROJECTIOND_BIN:-.run/bin/state-projectiond}")" \
  --serve "${TRADE_COCKPIT_PROJECTION_ADDR:-127.0.0.1:39731}" \
  --nats-url "${TRADE_COCKPIT_NATS_URL:-nats://127.0.0.1:14222}" \
  --jetstream-stream "${TRADE_COCKPIT_JETSTREAM_STREAM:-TRADING_EVENTS}" \
  --jetstream-durable "${TRADE_COCKPIT_PROJECTION_DURABLE:-trade-terminal-cockpit-projectiond}" \
  --nats-subject "${TRADE_COCKPIT_NATS_SUBJECT:-trading.event.>}" \
  --event-codec "${TRADE_COCKPIT_EVENT_CODEC:-protobuf}"
