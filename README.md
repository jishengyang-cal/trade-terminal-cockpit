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
- Broker execution, risk authority, strategy runtime, market data adapters,
  command-gateway, projection daemon, and audit storage stay outside this repo.
- Existing terminal/display repos and Xu Ya design/calculation work are design
  references only. This repo must not import their crates or internal modules.

## Workspace

```text
trade-core/       event, command, reducer, and view-state types
trade-tui/        Ratatui/Crossterm read-only terminal cockpit
tradectl/         non-interactive command-envelope emitter
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
cargo run -p trade-tui -- --plain --replay --from 2026-06-25T09:30:00 --to 2026-06-25T10:00:00
cargo run -p trade-tui -- --event-jsonl /path/to/events.jsonl --follow
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
  --pretty \
  pause-strategy open-scalp
```

Dangerous command example:

```bash
cargo run -p tradectl -- \
  --operator-id operator-demo \
  --session-id session-demo \
  --reason smoke-test \
  --capability account.kill \
  --pretty \
  global-kill-switch paper-main \
  --confirm 'KILL paper-main'
```
