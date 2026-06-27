#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

cargo check --workspace
cargo test --workspace
cargo run -p trade-tui -- --plain | grep -q 'mode=READ_ONLY'
cargo run -p trade-tui -- --plain --replay --from 2026-06-25T09:30:00 --to 2026-06-25T10:00:00 | grep -q 'mode=REPLAY'
cargo run -p trade-tui -- --plain --symbol MU | grep -q 'filter="symbol=MU"'
cargo run -p trade-tui -- --plain --snapshot-json fixtures/projection_snapshot.json | grep -q 'account=paper-snapshot'
cargo run -p trade-tui -- \
  --plain \
  --event-jsonl fixtures/order_lifecycle_events.jsonl \
  --replay \
  --from 2026-06-25T09:30:00 \
  --to 2026-06-25T09:30:12 \
  --correlation-id corr-fixture-001 |
  grep -q 'orders=1 positions=1 open_alerts=1 last_seq=12'
cargo run -p trade-tui -- --help | grep -q -- '--follow'
cargo run -p trade-tui -- --help | grep -q -- '--correlation-id'
cargo run -p trade-tui -- --help | grep -q -- '--snapshot-json'
cargo run -p tradectl -- \
  --operator-id smoke-operator \
  --session-id smoke-session \
  --reason smoke-test \
  --capability strategy.control \
  pause-strategy open-scalp | grep -q '"command_type":"PauseStrategyRequested"'

rm -f /tmp/trade-terminal-cockpit-audit.jsonl
cargo run -p tradectl -- \
  --operator-id smoke-operator \
  --session-id smoke-session \
  --reason smoke-test \
  --capability strategy.control \
  --audit-jsonl /tmp/trade-terminal-cockpit-audit.jsonl \
  pause-strategy open-scalp | grep -q '"command_type":"PauseStrategyRequested"'
grep -q '"command_type":"PauseStrategyRequested"' /tmp/trade-terminal-cockpit-audit.jsonl

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
