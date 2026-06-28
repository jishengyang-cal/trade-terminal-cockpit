# trade-terminal-cockpit

Independent terminal cockpit for trading-domain state, order lifecycle evidence,
risk status, and audited command requests.

This is not a webpage control panel, generic DB UI, log tail tool, broker
adapter, strategy runner, or service manager. It owns the terminal frontend and
shared event/command projection types only.

## Boundary

```text
event store / JetStream / state projection service
  -> trade-core reducer/projection state
  -> trade-tui read-only terminal cockpit
  -> local terminal / local tmux / local Zellij

operator / automation
  -> tradectl command envelope
  -> command-gateway
  -> authority / risk / domain services
  -> event stream
```

Rules:

- `trade-tui` is read-only in this repository. It renders events and materialized
  projections into a trading cockpit.
- `tradectl` emits command envelopes. It does not execute commands.
- Dangerous commands require exact confirmation text and remain replayable from
  the CLI.
- Broker execution, risk authority, strategy runtime, market data adapters, and
  audit storage stay outside this repo. The included `command-gateway` is the
  boundary process; it can optionally dispatch to an external
  `broker-control-gateway` binary, but it does not link broker crates or call a
  broker API directly.
- Existing terminal/display repos and Xu Ya design/calculation work are design
  references only. This repo must not import their crates or internal modules.

## Workspace

```text
trade-core/       event, command, reducer, and view-state types
trade-tui/        Ratatui/Crossterm read-only terminal cockpit
tradectl/         non-interactive command-envelope emitter
services/
  state-projectiond/  JSONL-to-projection boundary service
  command-gateway/    command validation/audit boundary service
contracts/proto/  language-neutral trading contracts
fixtures/         sanitized projection/event fixtures for local cockpit checks
.ai/              construction governance profiles and hooks
tools/            local boundary and smoke checks
```

## Local Terminal

This repo intentionally has no HTTP frontend address. The frontend is the local
terminal UI itself. Google VM is only a build/test worker; it is not a
deployment target for the trading frontend.

```bash
tools/open_local_tui.sh --mock
tools/open_local_tui.sh --snapshot-json fixtures/projection_snapshot.json
tools/open_local_tui.sh --event-jsonl fixtures/order_lifecycle_events.jsonl \
  --replay \
  --from 2026-06-25T09:30:00 \
  --to 2026-06-25T09:30:12 \
  --correlation-id corr-fixture-001
```

`tools/open_local_tui.sh` does not run `cargo` and does not SSH anywhere. It
only opens an existing local `trade-tui` binary from `.run/bin/` or
`target/debug/`.

To avoid compiling on the workstation, build and test on the Google VM, then
copy the VM-built binaries back for local terminal use:

```bash
tools/verify_on_google_vm.sh --copy-binaries
tools/open_local_tui.sh --mock
```

Useful cockpit keys:

```text
F1          help
F2-F8       switch cockpit screens
F9          command previews
F10         exit
Tab         next screen
Shift-Tab   previous screen
/           search current cockpit list
:           command palette input
Up/Down     select account, order chain, or event row
j/k         select account, order chain, or event row
K           risk page global kill-switch preview
A           risk page account kill-switch preview
F           risk page flatten selected account preview
q           exit
```

## Multi-Account Cockpit

The cockpit is account-aware. `AppState` keeps an `accounts.by_id` matrix and a
separate aggregate `account` view for compatibility/plain summaries. Order
chains and positions retain `account_id`, and Overview/Risk render the selected
account alongside global status.

Global and account controls are intentionally separate:

```text
global-kill-switch global
  -> broker-control global_kill / scope=global

account-kill-switch <account_id>
  -> broker-control cancel_all / scope=account_slot

flatten-account <account_id>
  -> broker-control flatten_only / scope=account_slot

cancel-all-orders-for-account <account_id>
  -> broker-control cancel_all / scope=account_slot
```

`global_kill` is never sent with account scope because broker-core rejects that
combination. Account-scoped commands require
`--broker-account-slot ACCOUNT_ID=SLOT` at the gateway; the frontend never
guesses slot mappings.

## Tailnet Access

Tailnet access is auxiliary remote-operator documentation only. It is not the
default frontend path and it is not a Google VM deployment path:

```bash
tools/tailnet_cockpit_url.sh
```

## Development

Run Rust builds, tests, and smoke checks on the Google VM, not on the local
workstation. The command is launched locally, but the compile/test work happens
on the VM and the frontend still runs locally from the copied binary.

```bash
tools/verify_on_google_vm.sh
```

GitHub Actions is kept as a manual `workflow_dispatch` fallback. The normal
local replacement for GitHub-hosted CI is `tools/verify_on_google_vm.sh`.

Useful VM checks:

```bash
cargo run -p trade-tui -- --plain
cargo run -p trade-tui -- --plain --snapshot-json fixtures/projection_snapshot.json
cargo run -p trade-tui -- --plain --event-jsonl fixtures/order_lifecycle_events.jsonl \
  --replay \
  --from 2026-06-25T09:30:00 \
  --to 2026-06-25T09:30:12 \
  --correlation-id corr-fixture-001
cargo run -p trade-tui -- --plain --replay --from 2026-06-25T09:30:00 --to 2026-06-25T10:00:00
cargo run -p trade-tui -- --event-jsonl /path/to/events.jsonl --follow
cargo run -p trade-tui -- \
  --nats-url nats://127.0.0.1:4222 \
  --nats-subject trading.order.lifecycle.paper-main.ord-demo-001 \
  --nats-subject trading.risk.decision.open-scalp.MU
cargo run -p trade-tui -- \
  --nats-url nats://127.0.0.1:4222 \
  --jetstream-stream TRADING_EVENTS \
  --jetstream-durable trade-tui-local \
  --nats-subject trading.order.lifecycle.paper-main.ord-demo-001
cargo run -p trade-tui -- --event-jsonl /path/to/events.jsonl --replay \
  --from 2026-06-25T09:30:00 \
  --to 2026-06-25T10:00:00 \
  --strategy-id open-scalp \
  --symbol MU \
  --correlation-id corr-demo-001
cargo run -p tradectl -- \
  --operator-id operator-demo \
  --session-id session-demo \
  --reason smoke-test \
  --capability strategy.control \
  --audit-jsonl /tmp/trade-terminal-cockpit-commands.jsonl \
  --pretty \
  pause-strategy open-scalp

cargo run -p trade-tui -- \
  --plain \
  --mock \
  --otel-stdout \
  --otel-service-name trade-tui-local
```

`--audit-jsonl` appends the emitted command envelope to a local evidence file.
`tradectl` does not execute the command; execution is always through
`command-gateway`.

Projection and command boundary services:

```bash
cargo run -p state-projectiond -- \
  --event-jsonl fixtures/order_lifecycle_events.jsonl

cargo run -p tradectl -- \
  --operator-id operator-demo \
  --session-id session-demo \
  --reason smoke-test \
  --capability strategy.control \
  pause-strategy open-scalp >/tmp/trade-command.json

cargo run -p command-gateway -- \
  --command-json /tmp/trade-command.json \
  --audit-jsonl /tmp/trade-command-audit.jsonl

cargo run -p tradectl -- \
  evidence-bundle \
  --event-jsonl fixtures/order_lifecycle_events.jsonl \
  --audit-jsonl /tmp/trade-terminal-cockpit-commands.jsonl \
  --correlation-id corr-fixture-001 \
  --output-json /tmp/trade-terminal-cockpit-evidence.json
```

`command-gateway` validates required operator/session/reason/capability fields
and writes audit events. It also checks command type against the expected
capability, with optional `--allow-capability` allowlisting. Dangerous envelopes
are rejected by default unless the gateway is started with an explicit
`--allow-dangerous` flag.

With `--execute-broker-control`, the gateway can dispatch semantically exact
runtime controls to an external `broker-control-gateway`:

```bash
cargo run -p tradectl -- \
  --operator-id operator-demo \
  --session-id session-demo \
  --reason emergency-test \
  --capability account.kill \
  global-kill-switch global \
  --confirm 'KILL global' >/tmp/trade-global-kill.json

BROKER_RUNTIME_DIR="$HOME/.local/state/broker-core/runtime" \
BROKER_CONTROL_BIN="$HOME/.local/bin/broker-control-gateway" \
cargo run -p command-gateway -- \
  --command-json /tmp/trade-global-kill.json \
  --audit-jsonl /tmp/trade-command-audit.jsonl \
  --allow-dangerous \
  --execute-broker-control
```

Supported broker-control mappings:

```text
GlobalKillSwitchRequested account_id=global|all|*
  -> broker-control family=global_kill scope=global mode=assert

FlattenSymbolRequested account_id=global|all|* symbol=*
  -> broker-control family=flatten_only scope=global mode=assert

CancelAllOrdersForSymbolRequested account_id=global|all|* symbol=*
  -> broker-control family=cancel_all scope=global mode=assert

FlattenSymbolRequested or CancelAllOrdersForSymbolRequested with symbol=*
and --broker-account-slot ACCOUNT_ID=SLOT
  -> broker-control scope=account_slot mode=assert

FlattenAccountRequested account_id=<account_id>
  -> broker-control family=flatten_only scope=account_slot mode=assert

CancelAllOrdersForAccountRequested account_id=<account_id>
  -> broker-control family=cancel_all scope=account_slot mode=assert

AccountKillSwitchRequested account_id=<account_id>
  -> broker-control family=cancel_all scope=account_slot mode=assert
```

Single-symbol flatten/cancel-all, single-order cancel, and strategy controls are
not widened to account/global scope. They are audited as `unsupported_execution`
until the matching order-gateway or strategy-control adapter exists.

Dangerous command example:

```bash
cargo run -p tradectl -- \
  --operator-id operator-demo \
  --session-id session-demo \
  --reason smoke-test \
  --capability account.kill \
  --pretty \
  account-kill-switch paper-main \
  --confirm 'KILL ACCOUNT paper-main'
```
