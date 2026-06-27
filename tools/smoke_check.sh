#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

cargo check --workspace
cargo test --workspace
cargo run -p trade-tui -- --plain | grep -q 'mode=READ_ONLY'
cargo run -p trade-tui -- --plain --replay --from 2026-06-25T09:30:00 --to 2026-06-25T10:00:00 | grep -q 'mode=REPLAY'
cargo run -p trade-tui -- --help | grep -q -- '--follow'
cargo run -p tradectl -- \
  --operator-id smoke-operator \
  --session-id smoke-session \
  --reason smoke-test \
  --capability strategy.control \
  pause-strategy open-scalp | grep -q '"command_type":"PauseStrategyRequested"'

if cargo run -p tradectl -- \
  --operator-id smoke-operator \
  --session-id smoke-session \
  --reason smoke-test \
  --capability account.kill \
  global-kill-switch paper-main >/tmp/trade-terminal-cockpit-danger.out 2>&1; then
  echo "dangerous command unexpectedly succeeded without confirmation" >&2
  cat /tmp/trade-terminal-cockpit-danger.out >&2
  exit 70
fi

cargo run -p tradectl -- \
  --operator-id smoke-operator \
  --session-id smoke-session \
  --reason smoke-test \
  --capability account.kill \
  global-kill-switch paper-main \
  --confirm 'KILL paper-main' | grep -q '"danger_level":"dangerous"'

tools/check_repo_boundary.sh

echo "trade-terminal-cockpit smoke passed"
