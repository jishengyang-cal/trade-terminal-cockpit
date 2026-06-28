#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

cargo check --workspace
cargo test --workspace
cargo run -p trade-tui -- --plain | grep -q 'mode=READ_ONLY'
cargo run -p trade-tui -- --plain | grep -q 'events_ingested='
TRADE_TUI_BIN="$ROOT_DIR/target/debug/trade-tui" tools/open_local_tui.sh --plain --mock | grep -q 'mode=READ_ONLY'
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
cargo run -p trade-tui -- --help | grep -q -- '--nats-url'
cargo run -p trade-tui -- --help | grep -q -- '--nats-subject'
cargo run -p trade-tui -- --help | grep -q -- '--jetstream-durable'
cargo run -p trade-tui -- --help | grep -q -- '--otel-stdout'
rm -f /tmp/trade-terminal-cockpit-otel.out
cargo run -p trade-tui -- \
  --plain \
  --mock \
  --otel-stdout \
  --otel-service-name trade-tui-smoke >/tmp/trade-terminal-cockpit-otel.out
grep -q 'trade_tui.state_projection' /tmp/trade-terminal-cockpit-otel.out
grep -q 'tui_events_ingested_total' /tmp/trade-terminal-cockpit-otel.out
cargo run -p state-projectiond -- \
  --event-jsonl fixtures/order_lifecycle_events.jsonl |
  grep -q '"source": "state-projectiond-jsonl"'
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

rm -f /tmp/trade-terminal-cockpit-evidence.json
cargo run -p tradectl -- \
  evidence-bundle \
  --event-jsonl fixtures/order_lifecycle_events.jsonl \
  --audit-jsonl /tmp/trade-terminal-cockpit-audit.jsonl \
  --correlation-id corr-fixture-001 \
  --output-json /tmp/trade-terminal-cockpit-evidence.json
grep -q '"schema_version":"trading.evidence.v1"' /tmp/trade-terminal-cockpit-evidence.json
grep -q '"event_count":12' /tmp/trade-terminal-cockpit-evidence.json

cargo run -p tradectl -- \
  --operator-id smoke-operator \
  --session-id smoke-session \
  --reason smoke-test \
  --capability strategy.control \
  pause-strategy open-scalp >/tmp/trade-terminal-cockpit-command.json
rm -f /tmp/trade-terminal-cockpit-gateway-audit.jsonl
cargo run -p command-gateway -- \
  --command-json /tmp/trade-terminal-cockpit-command.json \
  --audit-jsonl /tmp/trade-terminal-cockpit-gateway-audit.jsonl
grep -q '"status":"accepted"' /tmp/trade-terminal-cockpit-gateway-audit.jsonl

cargo run -p tradectl -- \
  --operator-id smoke-operator \
  --session-id smoke-session \
  --reason smoke-test \
  --capability wrong.capability \
  pause-strategy open-scalp >/tmp/trade-terminal-cockpit-bad-capability-command.json
rm -f /tmp/trade-terminal-cockpit-bad-capability-audit.jsonl
cargo run -p command-gateway -- \
  --command-json /tmp/trade-terminal-cockpit-bad-capability-command.json \
  --audit-jsonl /tmp/trade-terminal-cockpit-bad-capability-audit.jsonl
grep -q '"status":"rejected"' /tmp/trade-terminal-cockpit-bad-capability-audit.jsonl
grep -q 'capability mismatch' /tmp/trade-terminal-cockpit-bad-capability-audit.jsonl

if cargo run -p tradectl -- \
  --operator-id smoke-operator \
  --session-id smoke-session \
  --reason smoke-test \
  --capability account.kill \
  account-kill-switch paper-main >/tmp/trade-terminal-cockpit-danger.out 2>&1; then
  echo "dangerous command unexpectedly succeeded without confirmation" >&2
  cat /tmp/trade-terminal-cockpit-danger.out >&2
  exit 70
fi

cargo run -p tradectl -- \
  --operator-id smoke-operator \
  --session-id smoke-session \
  --reason smoke-test \
  --capability account.kill \
  account-kill-switch paper-main \
  --confirm 'KILL ACCOUNT paper-main' | grep -q '"danger_level":"dangerous"'

cargo run -p tradectl -- \
  --operator-id smoke-operator \
  --session-id smoke-session \
  --reason smoke-test \
  --capability account.kill \
  account-kill-switch paper-main \
  --confirm 'KILL ACCOUNT paper-main' >/tmp/trade-terminal-cockpit-danger-command.json
rm -f /tmp/trade-terminal-cockpit-danger-audit.jsonl
cargo run -p command-gateway -- \
  --command-json /tmp/trade-terminal-cockpit-danger-command.json \
  --audit-jsonl /tmp/trade-terminal-cockpit-danger-audit.jsonl
grep -q '"status":"rejected"' /tmp/trade-terminal-cockpit-danger-audit.jsonl

cat >/tmp/trade-terminal-cockpit-fake-broker-control <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >/tmp/trade-terminal-cockpit-fake-broker-control.args
[[ "$1" == "--write-runtime-control-plan" ]]
printf 'fake broker-control runtime plan written\n'
EOF
chmod +x /tmp/trade-terminal-cockpit-fake-broker-control
mkdir -p /tmp/trade-terminal-cockpit-broker-runtime

cargo run -p tradectl -- \
  --operator-id smoke-operator \
  --session-id smoke-session \
  --reason smoke-test \
  --capability account.kill \
  global-kill-switch global \
  --confirm 'KILL global' >/tmp/trade-terminal-cockpit-global-kill-command.json
rm -f /tmp/trade-terminal-cockpit-global-kill-audit.jsonl
cargo run -p command-gateway -- \
  --command-json /tmp/trade-terminal-cockpit-global-kill-command.json \
  --audit-jsonl /tmp/trade-terminal-cockpit-global-kill-audit.jsonl \
  --allow-dangerous \
  --execute-broker-control \
  --broker-runtime-dir /tmp/trade-terminal-cockpit-broker-runtime \
  --broker-control-bin /tmp/trade-terminal-cockpit-fake-broker-control
grep -q '"status":"dispatched"' /tmp/trade-terminal-cockpit-global-kill-audit.jsonl
grep -q -- '--family global_kill' /tmp/trade-terminal-cockpit-fake-broker-control.args
grep -q -- '--scope global' /tmp/trade-terminal-cockpit-fake-broker-control.args

cargo run -p tradectl -- \
  --operator-id smoke-operator \
  --session-id smoke-session \
  --reason smoke-test \
  --capability order.cancel \
  cancel-all-orders-for-symbol paper-main '*' \
  --confirm 'CANCEL ALL paper-main *' >/tmp/trade-terminal-cockpit-account-cancel-all-command.json
rm -f /tmp/trade-terminal-cockpit-account-cancel-all-audit.jsonl
cargo run -p command-gateway -- \
  --command-json /tmp/trade-terminal-cockpit-account-cancel-all-command.json \
  --audit-jsonl /tmp/trade-terminal-cockpit-account-cancel-all-audit.jsonl \
  --allow-dangerous \
  --execute-broker-control \
  --broker-runtime-dir /tmp/trade-terminal-cockpit-broker-runtime \
  --broker-control-bin /tmp/trade-terminal-cockpit-fake-broker-control \
  --broker-account-slot paper-main=7
grep -q '"status":"dispatched"' /tmp/trade-terminal-cockpit-account-cancel-all-audit.jsonl
grep -q -- '--family cancel_all' /tmp/trade-terminal-cockpit-fake-broker-control.args
grep -q -- '--scope account_slot' /tmp/trade-terminal-cockpit-fake-broker-control.args
grep -q -- '--account-slot 7' /tmp/trade-terminal-cockpit-fake-broker-control.args

cargo run -p tradectl -- \
  --operator-id smoke-operator \
  --session-id smoke-session \
  --reason smoke-test \
  --capability account.kill \
  account-kill-switch paper-main \
  --confirm 'KILL ACCOUNT paper-main' >/tmp/trade-terminal-cockpit-account-kill-command.json
rm -f /tmp/trade-terminal-cockpit-account-kill-audit.jsonl
cargo run -p command-gateway -- \
  --command-json /tmp/trade-terminal-cockpit-account-kill-command.json \
  --audit-jsonl /tmp/trade-terminal-cockpit-account-kill-audit.jsonl \
  --allow-dangerous \
  --execute-broker-control \
  --broker-runtime-dir /tmp/trade-terminal-cockpit-broker-runtime \
  --broker-control-bin /tmp/trade-terminal-cockpit-fake-broker-control \
  --broker-account-slot paper-main=7
grep -q '"status":"dispatched"' /tmp/trade-terminal-cockpit-account-kill-audit.jsonl
grep -q '"target":"paper-main"' /tmp/trade-terminal-cockpit-account-kill-audit.jsonl
grep -q -- '--family cancel_all' /tmp/trade-terminal-cockpit-fake-broker-control.args
grep -q -- '--scope account_slot' /tmp/trade-terminal-cockpit-fake-broker-control.args

cargo run -p tradectl -- \
  --operator-id smoke-operator \
  --session-id smoke-session \
  --reason smoke-test \
  --capability account.flatten \
  flatten-account paper-main \
  --confirm 'FLATTEN ACCOUNT paper-main' >/tmp/trade-terminal-cockpit-account-flatten-command.json
rm -f /tmp/trade-terminal-cockpit-account-flatten-audit.jsonl
cargo run -p command-gateway -- \
  --command-json /tmp/trade-terminal-cockpit-account-flatten-command.json \
  --audit-jsonl /tmp/trade-terminal-cockpit-account-flatten-audit.jsonl \
  --allow-dangerous \
  --execute-broker-control \
  --broker-runtime-dir /tmp/trade-terminal-cockpit-broker-runtime \
  --broker-control-bin /tmp/trade-terminal-cockpit-fake-broker-control \
  --broker-account-slot paper-main=7
grep -q '"status":"dispatched"' /tmp/trade-terminal-cockpit-account-flatten-audit.jsonl
grep -q '"target":"paper-main"' /tmp/trade-terminal-cockpit-account-flatten-audit.jsonl
grep -q -- '--family flatten_only' /tmp/trade-terminal-cockpit-fake-broker-control.args
grep -q -- '--scope account_slot' /tmp/trade-terminal-cockpit-fake-broker-control.args

cargo run -p tradectl -- \
  --operator-id smoke-operator \
  --session-id smoke-session \
  --reason smoke-test \
  --capability account.flatten \
  flatten-symbol paper-main MU \
  --confirm 'FLATTEN paper-main MU' >/tmp/trade-terminal-cockpit-flatten-command.json
rm -f /tmp/trade-terminal-cockpit-flatten-audit.jsonl
cargo run -p command-gateway -- \
  --command-json /tmp/trade-terminal-cockpit-flatten-command.json \
  --audit-jsonl /tmp/trade-terminal-cockpit-flatten-audit.jsonl \
  --allow-dangerous \
  --execute-broker-control \
  --broker-runtime-dir /tmp/trade-terminal-cockpit-broker-runtime \
  --broker-control-bin /tmp/trade-terminal-cockpit-fake-broker-control
grep -q '"status":"unsupported_execution"' /tmp/trade-terminal-cockpit-flatten-audit.jsonl
grep -q 'no scope broadening' /tmp/trade-terminal-cockpit-flatten-audit.jsonl

tools/check_repo_boundary.sh

echo "trade-terminal-cockpit smoke passed"
