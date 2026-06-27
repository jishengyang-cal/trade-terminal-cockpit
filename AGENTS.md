# Agent Rules

This repository is an independent trading domain terminal cockpit. It is not a
web console, generic DB UI, log tail tool, broker adapter, strategy runner, or
service manager.

Hard boundaries:

- Do not make `trade-tui` call brokers, order gateways, risk engines, strategy
  runners, databases, systemd, Nomad, Docker, or Homarr directly.
- Do not add direct broker order, cancel, flatten, risk override, kill switch,
  or service restart actions to the TUI.
- `trade-tui` is a projection client: it consumes event/snapshot projections and
  renders `AppState`.
- `tradectl` emits command envelopes only. Command execution belongs to an
  external command-gateway with auth, capability checks, confirmation policy,
  risk checks, and audit.
- Do not store credentials, account ids, API keys, tokens, broker configs, or
  private runtime config in this repo.
- Do not let the terminal render loop query production databases, scan runtime
  directories, or consume vendor-native market data directly.
- Keep strategy execution, broker execution, risk authority, market-data
  adapters, state projection services, and audit storage outside this repo.
- Existing repos such as `orderbook-terminal`, `market-extremes-terminal`,
  `backtest-terminal`, and Xu Ya design/calculation work may be used as design
  references only. Do not import their crates or internal modules into this
  repository.

Expected production boundary:

```text
event store / JetStream / state projection service
  -> trade-core reducer/projection state
  -> trade-tui read-only terminal cockpit
  -> terminal / SSH / tmux / Zellij

operator / automation
  -> tradectl command envelope
  -> command-gateway
  -> authority / risk / domain services
  -> event stream
```

Development expectations:

- Prefer Rust, Ratatui, Crossterm, protobuf contracts, serde, and strongly typed
  reducers.
- Keep rendering code independent from transport code.
- Keep dangerous actions replayable through `tradectl` and auditable through
  command envelopes.
- Do not run Rust compilation, tests, `cargo run`, or smoke checks on the local
  workstation. Run build-heavy commands such as `cargo check`, `cargo test`,
  `cargo build`, `cargo run`, and `tools/smoke_check.sh` on the Google VM.
- Local workstation work is limited to light file inspection, editing,
  formatting that does not trigger compilation, git operations, and
  non-compiling policy checks.
- Update `Cargo.lock` on the Google VM when dependency changes are required,
  and keep the lockfile compatible with the repository `rust-version` unless the
  toolchain policy is explicitly changed.
- Before publishing changes, validate on the Google VM with
  `cargo fmt --all -- --check`, `cargo test --workspace`, and
  `tools/smoke_check.sh`.
