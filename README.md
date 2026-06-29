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
  -> trade-tui terminal cockpit
  -> local terminal / local tmux / local Zellij

operator / automation
  -> tradectl command envelope
  -> command-gateway
  -> authority / risk / domain services
  -> event stream
```

Rules:

- `trade-tui` renders events/materialized projections and can submit
  `CommandEnvelope` requests to `command-gateway`. It still never calls broker,
  risk, or strategy runtime APIs directly.
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
trade-tui/        Ratatui/Crossterm terminal cockpit
tradectl/         non-interactive command-envelope emitter
services/
  state-projectiond/  projection snapshot/timeline query boundary service
  command-gateway/    command validation/audit/dispatch boundary service
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
F9          command evidence / gateway status
F10         exit
Tab         next screen
Shift-Tab   previous screen
/           search current cockpit list
:           command palette input
Up/Down     select account, order chain, or event row
j/k         select account, order chain, or event row
c           events page: copy correlation_id by OSC52
o           events page: open selected event's order chain
s           events page: open selected event's strategy
y           events page: copy decoded event payload by OSC52
K           risk page global kill-switch command
A           risk page account kill-switch command
F           risk page flatten selected account command
p/r/d/k     strategy page pause/resume/drain/kill commands
x/X         order page cancel order / cancel all selected symbol commands
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

## Trading Evidence Model

Core trading evidence avoids naked floating-point prices in order lifecycle
state. `trade-core` exposes `Price` and `Money` fixed-scale structs, while JSON
deserialization remains backward-compatible with existing numeric fixtures.

Account projections accept `AccountSnapshot` events with fixed-scale cash,
buying power, PnL, margin, net liquidation, PDT/restriction, and exposure
fields. The cockpit keeps both per-account views and an aggregate multi-account
view; the aggregate sums Money fields and keeps max/risk-style percentages
conservative.

Strategy projections accept `StrategyHealthUpdated` events for trading window,
phase, universe version, watched/active/L2 symbol counts, one-minute rates,
latency averages, consecutive stops, trade budget, parameters, and risk gates.

Order chains retain broker and routing evidence such as `client_order_id`,
`broker_order_id`, `perm_id`, route/exchange/destination, submitted and
remaining quantity, fill execution IDs, cumulative fill quantity, latency
timestamps, and lifecycle anomalies. Reducer state is idempotent by `event_id`
and records duplicate, out-of-order, and sequence-gap counters.

Risk decisions can carry evaluated rule snapshots. The reducer projects those
into structured rule rows and deduplicated active blocks, so Risk is a current
state surface rather than a growing log tail.

`tradectl evidence-bundle` includes filtered events/commands, rebuilt
projection state, input file SHA-256 hashes, event ID counts, duplicate counts,
sequence gap counts, schema versions, generator name, and best-effort git
commit metadata.

NATS Core, JetStream, and JSONL follow ingestion errors are domain-visible:
connect/reconnect, subscribe, decode, and filter diagnostics are emitted as
`IngestDiagnosticRecorded` events and reduced into TUI connection health. The
TUI also tracks per-tick drain counts, render duration, slow frames, and backlog
estimates.

## Tailnet Access

Tailnet access is auxiliary remote-operator documentation only. It is not the
default frontend path and it is not a Google VM deployment path:

```bash
tools/tailnet_cockpit_url.sh
```

## External Production Boundaries

The cockpit has a production profile, but it refuses to treat the phase-1 OPS
control-bus as trading-domain truth. `OPS_EVENTS` / `ops.event.>` is for
systemd/Docker/runtime health. Order lifecycle, strategy, risk, and account
projections must come from a trading-domain stream such as `TRADING_EVENTS`
with subjects like `trading.event.>`. Command authority/audit events belong in
`TRADING_AUDIT` on `trading.command.>` / `trading.audit.>`, so the two streams
do not overlap.

Create the editable local profile if it does not already exist:

```bash
mkdir -p "${XDG_CONFIG_HOME:-$HOME/.config}/trade-terminal-cockpit"
cp -n config/external.env.example \
  "${XDG_CONFIG_HOME:-$HOME/.config}/trade-terminal-cockpit/external.env"
```

Then edit:

```text
$XDG_CONFIG_HOME/trade-terminal-cockpit/external.env
```

Before starting services, run the preflight. It checks that the configured NATS
JetStream stream exists, that it is not an OPS/control-bus stream, and that
the event projection subject is a bounded `trading.event.*` namespace, not a
hot/raw/control namespace or a broad `trading.>` catch-all. It also checks that
configured risk/broker/order adapters are present:

```bash
tools/check_external_integration.py \
  --env-file "$XDG_CONFIG_HOME/trade-terminal-cockpit/external.env"
```

If the trading streams do not exist yet, initialize the non-overlapping stream
surface first:

```bash
tools/init_trading_streams.py \
  --env-file "$XDG_CONFIG_HOME/trade-terminal-cockpit/external.env"
```

Open the local TUI against the external profile:

```bash
tools/open_external_tui.sh
```

The local user services are managed by the Imperativ target-runtime registry,
not by ad hoc shell commands. Inspect and plan through
`$HOME/projects/imperativ-main`:

```bash
cd "$HOME/projects/imperativ-main"
python3 tools/target_machine_runtime_control.py validate --json
python3 tools/target_machine_runtime_control.py status service.trade_terminal_cockpit.projectiond --json
python3 tools/target_machine_runtime_control.py status service.trade_terminal_cockpit.command_gateway --json
python3 tools/target_machine_runtime_control.py plan service.trade_terminal_cockpit.projectiond start --json
python3 tools/target_machine_runtime_control.py plan service.trade_terminal_cockpit.command_gateway start --json
```

The printed shell commands are evidence for the CommandGateway handoff, not
permission to run `systemctl` directly. Mutating actions must go through the
Imperativ Workbench/control-plane approval path.

Run a non-broker end-to-end verification:

```bash
tools/run_external_e2e.py \
  --env-file "$XDG_CONFIG_HOME/trade-terminal-cockpit/external.env"

tools/run_external_e2e.py \
  --env-file "$XDG_CONFIG_HOME/trade-terminal-cockpit/external.env" \
  --event-codec protobuf
```

The E2E creates/updates the trading streams, publishes a synthetic order
lifecycle into `TRADING_EVENTS` as JSON or protobuf `EventEnvelope` wire bytes,
verifies `state-projectiond` can reconstruct a filled order chain, sends a
low-risk `AcknowledgeAlertRequested` command through `command-gateway` and the
risk adapter, verifies a dangerous command is rejected, and forwards command
audit JSONL into `TRADING_AUDIT`. It does not execute broker-control, cancel,
flatten, or kill actions.

`trade-terminal-cockpit-command-gateway.service` does not enable broker-control
execution by default. Set `TRADE_COCKPIT_ENABLE_BROKER_CONTROL=1` only when the
broker runtime, account-slot mapping, operator policy, and risk adapter are all
intentionally live. Dangerous commands still require exact confirmation in the
TUI/CLI and policy acceptance in `command-gateway`.

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
  --nats-subject trading.event.order.lifecycle.paper-main.ord-demo-001 \
  --nats-subject trading.event.risk.decision.open-scalp.MU
cargo run -p trade-tui -- \
  --nats-url nats://127.0.0.1:4222 \
  --jetstream-stream TRADING_EVENTS \
  --jetstream-durable trade-tui-local \
  --nats-subject trading.event.order.lifecycle.paper-main.ord-demo-001
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

cargo run -p trade-tui -- \
  --event-store-query-bin /path/to/event-store-query \
  --event-store-uri postgres://redacted/event_store \
  --correlation-id corr-demo-001

COMMAND_GATEWAY_ADDR=127.0.0.1:39732 \
cargo run -p trade-tui -- \
  --command-gateway-addr 127.0.0.1:39732 \
  --risk-check-bin /path/to/risk-check \
  --strategy-control-bin /path/to/strategy-control \
  --order-gateway-bin /path/to/order-gateway \
  --alert-service-bin /path/to/alert-service
```

`--audit-jsonl` appends the emitted command envelope to a local evidence file.
`tradectl` does not execute the command; execution is always through
`command-gateway`.

`--event-store-query-bin` is an adapter boundary for Postgres/event-store replay.
The adapter is invoked with `--query-events`, receives a JSON request on stdin,
and must emit `EventEnvelope` JSONL on stdout. This keeps database/event-store
client code out of the TUI.
`state-projectiond` supports the same adapter as a startup catch-up source, then
continues with NATS/JetStream live ingest when configured. This keeps the
projection daemon on the trading-domain boundary without linking database
drivers into the terminal cockpit. The startup adapter is bounded by
`--event-store-timeout-ms`; live NATS/JetStream ingest does not wait on it after
startup.

Projection and command boundary services:

```bash
cargo run -p state-projectiond -- \
  --event-jsonl fixtures/order_lifecycle_events.jsonl

cargo run -p state-projectiond -- \
  --event-store-query-bin /path/to/event-store-query \
  --event-store-uri postgres://redacted/event_store \
  --event-store-timeout-ms 5000 \
  --nats-url nats://127.0.0.1:4222 \
  --nats-subject 'trading.event.>' \
  --event-codec protobuf

cargo run -p state-projectiond -- \
  --event-jsonl fixtures/order_lifecycle_events.jsonl \
  --serve 127.0.0.1:39731

printf '%s\n' '{"method":"GetOrderTimeline","correlation_id":"corr-fixture-001"}' \
  | nc 127.0.0.1 39731

cargo run -p tradectl -- \
  --operator-id operator-demo \
  --session-id session-demo \
  --reason smoke-test \
  --capability strategy.control \
  pause-strategy open-scalp >/tmp/trade-command.json

cargo run -p command-gateway -- \
  --command-json /tmp/trade-command.json \
  --audit-jsonl /tmp/trade-command-audit.jsonl

cargo run -p command-gateway -- \
  --serve 127.0.0.1:39732 \
  --audit-jsonl /tmp/trade-command-audit.jsonl \
  --adapter-timeout-ms 750 \
  --risk-check-bin /path/to/risk-check \
  --strategy-control-bin /path/to/strategy-control \
  --order-gateway-bin /path/to/order-gateway \
  --alert-service-bin /path/to/alert-service

cat /tmp/trade-command.json | nc 127.0.0.1 39732

cargo run -p tradectl -- \
  evidence-bundle \
  --event-jsonl fixtures/order_lifecycle_events.jsonl \
  --audit-jsonl /tmp/trade-terminal-cockpit-commands.jsonl \
  --correlation-id corr-fixture-001 \
  --output-json /tmp/trade-terminal-cockpit-evidence.json
```

`command-gateway` validates required operator/session/reason/capability fields,
writes a `CommandAuthorityDecided` event, then writes the final audit/dispatch
event. It also checks command type against the expected capability, with
optional `--allow-capability` allowlisting. Dangerous envelopes are rejected by
default unless the gateway is started with an explicit `--allow-dangerous` flag.
The TUI Commands screen shows authority status, audit status, policy ids, reason
codes, capability, and scope from those events.

`command-gateway --serve` accepts one JSON `CommandEnvelope` per line over TCP
and returns a JSON gateway response containing status plus emitted authority and
audit events. `trade-tui --command-gateway-addr` uses that transport; without it,
the TUI falls back to launching the local `command-gateway` binary and reading
the emitted events from the configured audit file.

The adapter flags are process boundaries, not linked dependencies. A risk
adapter receives the command envelope on stdin with `--check-command-risk` and
can return `accepted`, `rejected`, or another authority status with reason codes
and policy ids. Strategy, order, and alert adapters receive the envelope with
`--execute-command` and return a dispatch status/reason.
External adapter execution is bounded by `--adapter-timeout-ms` so a slow
broker/risk/strategy process cannot stall the gateway control path.

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
