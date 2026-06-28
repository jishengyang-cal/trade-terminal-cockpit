# Gap Closure Register

This register tracks the missing items from the terminal cockpit plan. An item is
closed only when it has a repository implementation and a repeatable test or
smoke check.

| Item | Closure Target | Status | Verification |
| --- | --- | --- | --- |
| NATS / JetStream | `trade-tui` can subscribe to NATS Core subjects and can bind a JetStream durable pull consumer, routing `EventEnvelope` payloads through the reducer. | Closed | `tools/verify_on_google_vm.sh --copy-binaries` |
| Protobuf | `.proto` contracts are compiled by `prost` in `trade-contracts`, including account snapshots, strategy health, ingest diagnostics, and command authority decisions. | Closed | `trade_contracts::tests::encodes_and_decodes_event_envelope_contract` and `encodes_and_decodes_account_strategy_and_authority_contracts` on VM |
| Strategies Page | Strategy detail view exposes heartbeat, counters, reason, trading window, phase, universe version, symbol allocation, latency/rate fields, parameters, and risk gates without importing strategy runtime code. | Closed | VM workspace tests and smoke |
| Orders Page | Orders view supports selected-chain drilldown plus in-app search/filter state. | Closed | VM workspace tests and smoke |
| Events / Audit | Events view supports search state, selected detail, correlation visibility, and command/evidence replay paths. | Closed | VM workspace tests and smoke |
| Risk Page | Risk command intent flow is represented as dangerous command-envelope previews, not direct broker execution. | Closed | VM workspace tests and smoke |
| Multi-Account Cockpit | Account projections are stored as an account matrix; Overview/Risk/Orders/Positions display account scope without last-event overwrite. | Closed | VM reducer tests and smoke |
| Replay | Replay loads JSONL locally and can consume replayable JetStream durable streams; Postgres/event-store adapters stay behind the projection boundary. | Closed | VM smoke replay and JetStream CLI surface checks |
| Command System | `tradectl` emits replayable command envelopes; `command-gateway` validates, emits authority-decision events, records audit events, and can dispatch supported runtime-control commands to an external `broker-control-gateway`. | Closed | VM smoke accepted/rejected/dispatched gateway assertions and command-gateway unit tests |
| Observability | TUI exposes event lag, duplicate/out-of-order/gap counts, ingest/decode errors, filtered-event counts, drain/backlog estimates, render slow-frame metrics, and can emit OpenTelemetry stdout traces/metrics with `--otel-stdout`. | Closed | VM smoke OTEL trace/metric grep and workspace tests |
| Performance / Noise Reduction | Reducer coalesces high-frequency projection updates such as heartbeat and position snapshots without dropping lifecycle evidence. | Closed | `coalesces_high_frequency_projection_events_without_dropping_lifecycle_events` on VM |
| state-projectiond | Standalone projection service boundary exists outside `trade-tui`. | Closed | VM smoke `state-projectiond --event-jsonl` |
| command-gateway | Standalone command gateway boundary exists outside `trade-tui`, with default audit-only mode and explicit broker-control execution mode. | Closed | VM smoke `command-gateway --command-json` and broker-control fake dispatch |
| authority / risk engine integration | Gateway has an explicit capability/authority policy, denies dangerous commands by default, and refuses unsupported scope widening. Broker/risk code remains out of repo. | Closed | VM smoke dangerous, bad-capability, unsupported-scope, and dispatched runtime-control assertions |
| NATS subjects / durable consumers | Subject config and durable consumer naming are typed in CLI and implemented in the event stream. | Closed | VM compile and help/smoke checks |
| TUI dangerous action modal | Dangerous action confirmation state exists in TUI and stays command-envelope only. | Closed | VM compile/smoke |
| TUI command palette | Command palette state exists and can preview/replay command-envelope forms. | Closed | VM compile/smoke |
| F1 Help / F9 Commands / F10 Exit | Key bindings and screens are implemented. | Closed | VM compile/smoke |
| Flatten / kill switch backend chain | `tradectl` emits dangerous envelopes, TUI previews global/account scope separately, and gateway can dispatch exact global/account runtime controls to external `broker-control-gateway`; single-symbol operations are audited as unsupported until a symbol-aware order gateway exists. | Closed | VM smoke dangerous rejection, broker-control dispatch, account kill dispatch, and unsupported symbol-scope rejection |
| Evidence bundle export | `tradectl evidence-bundle` exports filtered events, commands, and rebuilt projection state. | Closed | VM smoke evidence JSON assertions |
| Trading evidence hardening | Core event/state contracts carry fixed-scale price/money values, fill execution/cumulative quantities, order latency evidence, risk rule evaluations, event-id idempotency, and evidence bundle input hashes. | Closed | VM reducer tests: duplicate events, cumulative fills, risk rule block dedupe; VM smoke evidence bundle |
| Account projection hardening | Account snapshots carry fixed-scale Money fields for cash, buying power, PnL, margin, net liquidation, PDT/restriction status, and aggregate multi-account sums. | Closed | VM projection snapshot tests and smoke |
| NATS ingest diagnostics | NATS Core, JetStream, and JSONL follow decode/connect/filter diagnostics are emitted as domain events and reduced into connection health instead of stderr-only logs. | Closed | VM compile, TUI smoke, and reducer coalescing tests |
| Command authority evidence | Gateway writes `CommandAuthorityDecided` before audit/dispatch so TUI/evidence can show decision id, policy ids, reason codes, capability, scope, operator, and environment. | Closed | VM command-gateway tests and smoke rejected/accepted/dispatched gateway assertions |
| TUI backpressure visibility | TUI tracks drained events per tick, render duration, slow frames, and backlog estimates without moving execution into the frontend. | Closed | VM compile and TUI plain/replay smoke |
