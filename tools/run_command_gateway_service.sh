#!/usr/bin/env bash
set -euo pipefail

expand_path() {
  local value=$1
  value=${value/#\~/${HOME}}
  value=${value/#\$HOME/${HOME}}
  printf '%s' "${value}"
}

args=(
  "$(expand_path "${TRADE_COCKPIT_COMMAND_GATEWAY_BIN:-.run/bin/command-gateway}")"
  --serve "${TRADE_COCKPIT_COMMAND_GATEWAY_ADDR:-127.0.0.1:39732}"
  --audit-jsonl "$(expand_path "${TRADE_COCKPIT_COMMAND_AUDIT_JSONL:-.run/command-gateway-audit.jsonl}")"
  --adapter-timeout-ms "${TRADE_COCKPIT_ADAPTER_TIMEOUT_MS:-750}"
)

if [[ -n "${TRADE_COCKPIT_POLICY_JSON:-}" ]]; then
  args+=(--policy-json "$(expand_path "${TRADE_COCKPIT_POLICY_JSON}")")
fi
if [[ -n "${TRADE_COCKPIT_RISK_CHECK_BIN:-}" ]]; then
  args+=(--risk-check-bin "$(expand_path "${TRADE_COCKPIT_RISK_CHECK_BIN}")")
fi
if [[ -n "${TRADE_COCKPIT_STRATEGY_CONTROL_BIN:-}" ]]; then
  args+=(--strategy-control-bin "$(expand_path "${TRADE_COCKPIT_STRATEGY_CONTROL_BIN}")")
fi
if [[ -n "${TRADE_COCKPIT_ORDER_GATEWAY_BIN:-}" ]]; then
  args+=(--order-gateway-bin "$(expand_path "${TRADE_COCKPIT_ORDER_GATEWAY_BIN}")")
fi
if [[ -n "${TRADE_COCKPIT_ALERT_SERVICE_BIN:-}" ]]; then
  args+=(--alert-service-bin "$(expand_path "${TRADE_COCKPIT_ALERT_SERVICE_BIN}")")
fi

if [[ "${TRADE_COCKPIT_ENABLE_BROKER_CONTROL:-0}" == "1" ]]; then
  args+=(--execute-broker-control)
  if [[ -n "${TRADE_COCKPIT_BROKER_RUNTIME_DIR:-}" ]]; then
    args+=(--broker-runtime-dir "$(expand_path "${TRADE_COCKPIT_BROKER_RUNTIME_DIR}")")
  fi
  if [[ -n "${TRADE_COCKPIT_BROKER_CONTROL_BIN:-}" ]]; then
    args+=(--broker-control-bin "$(expand_path "${TRADE_COCKPIT_BROKER_CONTROL_BIN}")")
  fi
  IFS=, read -ra slots <<< "${TRADE_COCKPIT_BROKER_ACCOUNT_SLOTS:-}"
  for slot in "${slots[@]}"; do
    [[ -n "${slot}" ]] && args+=(--broker-account-slot "${slot}")
  done
fi

exec "${args[@]}"
