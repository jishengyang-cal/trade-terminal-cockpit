#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TAILSCALE_IP="$(tailscale ip -4 | head -n 1)"
DNS_NAME="$(tailscale status --json | python3 -c 'import json,sys; print(json.load(sys.stdin)["Self"].get("DNSName","").rstrip("."))')"
USER_NAME="$(id -un)"

if [ -z "${TAILSCALE_IP}" ]; then
  echo "tailscale is not reporting an IPv4 address" >&2
  exit 70
fi

if [ -n "${DNS_NAME}" ]; then
  printf 'trade_terminal_cockpit_ssh=%s\n' "ssh://${USER_NAME}@${DNS_NAME}"
fi
printf 'trade_terminal_cockpit_ssh_ip=%s\n' "ssh://${USER_NAME}@${TAILSCALE_IP}"
printf 'trade_terminal_cockpit_run=%s\n' "cd ${ROOT_DIR} && cargo run -p trade-tui -- --mock"
printf 'trade_terminal_cockpit_replay=%s\n' "cd ${ROOT_DIR} && cargo run -p trade-tui -- --replay --mock"
