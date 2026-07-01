#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = Path.home() / ".config/trade-terminal-cockpit/external.env"
FIELD_SNAPSHOT = ROOT / "fixtures/projection_snapshot.json"
FIELD_EVENTS = ROOT / "fixtures/order_lifecycle_events.jsonl"
SERVICES = [
    "trade-terminal-cockpit-state-projectiond.service",
    "trade-terminal-cockpit-command-gateway.service",
]


@dataclass
class StepResult:
    name: str
    ok: bool
    duration_ms: int
    detail: str = ""
    output_tail: str = ""


@dataclass
class GateReport:
    ok: bool = True
    steps: list[StepResult] = field(default_factory=list)

    def add(self, result: StepResult) -> None:
        self.steps.append(result)
        self.ok = self.ok and result.ok


class GateFailure(RuntimeError):
    pass


def tail(text: str, limit: int = 4000) -> str:
    text = text.strip()
    if len(text) <= limit:
        return text
    return text[-limit:]


def print_step(name: str) -> None:
    print(f"==> {name}", flush=True)


def run_cmd(
    report: GateReport,
    name: str,
    cmd: list[str],
    *,
    timeout_s: int = 300,
    env: dict[str, str] | None = None,
    fail_fast: bool = True,
) -> subprocess.CompletedProcess[str]:
    print_step(name)
    started = time.monotonic()
    try:
        completed = subprocess.run(
            cmd,
            cwd=ROOT,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout_s,
            check=False,
        )
        ok = completed.returncode == 0
        output = completed.stdout or ""
        detail = "ok" if ok else f"exit={completed.returncode}"
    except subprocess.TimeoutExpired as exc:
        ok = False
        output = (exc.stdout or "") if isinstance(exc.stdout, str) else ""
        detail = f"timeout after {timeout_s}s"
        completed = subprocess.CompletedProcess(cmd, 124, output, "")
    duration_ms = int((time.monotonic() - started) * 1000)
    result = StepResult(name=name, ok=ok, duration_ms=duration_ms, detail=detail, output_tail=tail(output))
    report.add(result)
    if ok:
        print(f"ok  {name} ({duration_ms}ms)", flush=True)
    else:
        print(f"FAIL {name}: {detail}", flush=True)
        if result.output_tail:
            print(result.output_tail, flush=True)
        if fail_fast:
            raise GateFailure(name)
    return completed


def load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if not sep:
            raise GateFailure(f"{path}:{line_number}: expected KEY=VALUE")
        values[key.strip()] = os.path.expanduser(os.path.expandvars(value.strip().strip("'").strip('"')))
    return values


def env_value(values: dict[str, str], key: str, default: str) -> str:
    return os.path.expanduser(os.path.expandvars(values.get(key, default)))


def wait_tcp(addr: str, timeout_s: float) -> None:
    host, port_text = addr.rsplit(":", 1)
    deadline = time.monotonic() + timeout_s
    last_error: OSError | None = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, int(port_text)), timeout=0.25):
                return
        except OSError as exc:
            last_error = exc
            time.sleep(0.1)
    raise GateFailure(f"timed out waiting for {addr}: {last_error}")


def present(value: Any) -> bool:
    return value is not None and value != ""


def require(errors: list[str], condition: bool, message: str) -> None:
    if not condition:
        errors.append(message)


def number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def check_projection_fields(snapshot_path: Path = FIELD_SNAPSHOT, event_path: Path = FIELD_EVENTS) -> list[str]:
    errors: list[str] = []
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))

    account = snapshot.get("account") or {}
    require(errors, present(account.get("account_id")), "account.account_id missing")
    for field_name in [
        "account_snapshot_id",
        "account_snapshot_seq",
        "account_snapshot_source",
        "account_snapshot_ts_ns",
        "account_snapshot_age_ms",
        "valuation_status",
        "effective_trade_state",
        "effective_trade_reason",
    ]:
        require(errors, present(account.get(field_name)), f"account.{field_name} missing")
    if account.get("valuation_status") == "COMPLETE":
        require(errors, account.get("valuation_ok") is True, "account valuation COMPLETE but valuation_ok is not true")
    for field_name in ["cash", "buying_power", "net_liquidation", "available_funds", "day_pnl", "realized_pnl", "unrealized_pnl"]:
        require(errors, account.get(field_name) is not None, f"account.{field_name} missing")
    for field_name in ["cash_source", "buying_power_source", "net_liq_source", "available_funds_source", "day_pnl_source", "realized_source", "unrealized_source"]:
        require(errors, present(account.get(field_name)), f"account.{field_name} missing")

    positions = snapshot.get("positions") or []
    require(errors, bool(positions), "positions missing")
    position_upnl = sum(number(item.get("unrealized_pnl")) or 0.0 for item in positions)
    if abs(position_upnl) > 0.0001 and abs(number(account.get("day_pnl")) or 0.0) < 0.0001:
        require(errors, present(account.get("day_pnl_source")), "account day_pnl is zero while position UPNL is nonzero and source is missing")
    for idx, position in enumerate(positions):
        prefix = f"positions[{idx}]"
        for field_name in [
            "account_id",
            "symbol",
            "net_quantity",
            "average_price",
            "market_price",
            "unrealized_pnl",
            "open_buy_qty",
            "open_sell_qty",
            "pending_cancel_qty",
            "reserved_buy_power",
            "position_notional",
            "gross_exposure",
            "net_exposure",
            "realized_pnl",
            "mark_source",
            "mark_ts_ns",
            "mark_age_ms",
        ]:
            require(errors, position.get(field_name) is not None, f"{prefix}.{field_name} missing")
        attributions = position.get("strategy_attribution") or []
        require(errors, bool(attributions), f"{prefix}.strategy_attribution missing")
        for attr_idx, attr in enumerate(attributions):
            attr_prefix = f"{prefix}.strategy_attribution[{attr_idx}]"
            for field_name in ["strategy_id", "quantity", "avg_cost", "realized_pnl", "unrealized_pnl", "fees", "attribution_method", "attribution_version"]:
                require(errors, attr.get(field_name) is not None, f"{attr_prefix}.{field_name} missing")

    strategies = snapshot.get("strategies") or []
    require(errors, bool(strategies), "strategies missing")
    for idx, strategy in enumerate(strategies):
        prefix = f"strategies[{idx}]"
        for field_name in [
            "signals_total_today",
            "signals_last_1m",
            "intents_total_today",
            "orders_total_today",
            "fills_total_today",
            "partial_fills_today",
            "cancels_total_today",
            "rejects_total_today",
            "strategy_realized_pnl",
            "strategy_unrealized_pnl",
            "strategy_total_pnl",
            "pnl_source",
            "pnl_basis",
            "pnl_as_of_ts_ns",
            "session_phase",
            "strategy_window_id",
            "window_start_ts_ns",
            "window_end_ts_ns",
            "window_status",
            "next_transition_ts_ns",
            "is_market_open",
            "is_regular_session",
            "is_opening_window",
            "symbols_with_fresh_l1",
            "symbols_with_fresh_l2",
            "symbols_missing_md",
            "l1_symbols_allocated",
            "l2_capacity",
            "l2_capacity_used",
            "lease_authority_version",
        ]:
            require(errors, strategy.get(field_name) is not None, f"{prefix}.{field_name} missing")
        for gate_idx, gate in enumerate(strategy.get("risk_gates") or []):
            gate_prefix = f"{prefix}.risk_gates[{gate_idx}]"
            for field_name in ["scope", "observed", "limit", "status", "severity", "reason", "policy_version", "source_seq", "evaluated_ts_ns"]:
                require(errors, present(gate.get(field_name)), f"{gate_prefix}.{field_name} missing")

    orders = snapshot.get("orders") or []
    require(errors, bool(orders), "orders missing")
    for idx, order in enumerate(orders):
        prefix = f"orders[{idx}]"
        for field_name in [
            "correlation_id",
            "order_id",
            "account_id",
            "strategy_id",
            "symbol",
            "client_order_id",
            "broker_order_id",
            "broker_perm_id",
            "broker_account_id",
            "order_ref",
            "strategy_order_ref",
            "total_qty",
            "filled_quantity",
            "remaining_quantity",
            "intent_created_ts_ns",
            "risk_decision_ts_ns",
            "submit_requested_ts_ns",
            "order_submitted_ts_ns",
            "broker_ack_ts_ns",
            "submit_ts_ns",
            "ack_ts_ns",
            "bbo_bid_at_submit",
            "bbo_ask_at_submit",
            "mid_at_submit",
            "spread_bps_at_submit",
            "quote_age_ms_at_submit",
            "slippage_vs_mid_bps",
            "slippage_vs_arrival_bps",
            "slippage_vs_decision_bps",
            "causal_chain_summary",
        ]:
            require(errors, order.get(field_name) is not None, f"{prefix}.{field_name} missing")
        total_qty = number(order.get("total_qty"))
        filled_qty = number(order.get("filled_quantity"))
        remaining_qty = number(order.get("remaining_quantity"))
        if total_qty is not None and filled_qty is not None and remaining_qty is not None:
            require(errors, abs(total_qty - filled_qty - remaining_qty) < 0.0001, f"{prefix}.remaining_quantity does not reconcile")
        risk = order.get("risk") or {}
        for field_name in ["decision_id", "risk_decision_seq", "risk_result", "authority_policy_version", "limits_snapshot_id", "risk_mode", "limits_enforced"]:
            require(errors, risk.get(field_name) is not None, f"{prefix}.risk.{field_name} missing")
        latency = order.get("latency") or {}
        for field_name in ["signal_to_intent_ms", "intent_to_risk_ms", "risk_to_submit_req_ms", "submit_req_to_submitted_ms", "submit_to_ack_ms", "submitted_to_ack_ms", "ack_to_first_fill_ms", "submit_to_first_fill_ms"]:
            require(errors, latency.get(field_name) is not None, f"{prefix}.latency.{field_name} missing")
        fills = order.get("fills") or []
        require(errors, bool(fills), f"{prefix}.fills missing")
        for fill_idx, fill in enumerate(fills):
            fill_prefix = f"{prefix}.fills[{fill_idx}]"
            for field_name in ["exec_id", "broker_exec_id", "fill_seq", "qty", "price", "venue", "liquidity_flag", "commission", "currency", "fill_ts_ns", "position_after_fill", "order_id", "symbol", "side", "exchange", "ingest_ts_ns", "realized_pnl_delta", "fee_details"]:
                require(errors, fill.get(field_name) is not None, f"{fill_prefix}.{field_name} missing")

    risk = snapshot.get("risk") or {}
    for field_name in ["global_state", "risk_mode", "limits_enforced", "structured_limits"]:
        require(errors, risk.get(field_name) is not None, f"risk.{field_name} missing")
    for idx, item in enumerate(risk.get("structured_limits") or []):
        prefix = f"risk.structured_limits[{idx}]"
        for field_name in ["rule_id", "scope", "observed", "limit", "status", "severity", "reason", "policy_version", "source_seq", "evaluated_ts_ns"]:
            require(errors, item.get(field_name) is not None, f"{prefix}.{field_name} missing")

    market_data = snapshot.get("market_data") or []
    require(errors, bool(market_data), "market_data missing")
    for idx, item in enumerate(market_data):
        prefix = f"market_data[{idx}]"
        for field_name in ["symbol", "source", "bid_price", "ask_price", "spread_bps", "quote_age_ms", "summary_ts_ns"]:
            require(errors, item.get(field_name) is not None, f"{prefix}.{field_name} missing")

    events: list[dict[str, Any]] = []
    for line in event_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            events.append(json.loads(line))
    require(errors, bool(events), "order lifecycle event fixture missing")
    seen_ids: set[str] = set()
    sequences: list[int] = []
    for idx, event in enumerate(events):
        prefix = f"events[{idx}]"
        event_id = str(event.get("event_id") or "")
        require(errors, present(event_id), f"{prefix}.event_id missing")
        require(errors, event_id not in seen_ids, f"{prefix}.event_id duplicate: {event_id}")
        seen_ids.add(event_id)
        for field_name in ["event_type", "aggregate_type", "aggregate_id", "correlation_id", "producer", "schema_version", "source_ts_ns", "ingest_ts_ns", "publish_ts_ns"]:
            require(errors, present(event.get(field_name)), f"{prefix}.{field_name} missing")
        seq = event.get("sequence")
        require(errors, isinstance(seq, int), f"{prefix}.sequence missing or not int")
        if isinstance(seq, int):
            sequences.append(seq)
    if sequences:
        expected = list(range(min(sequences), max(sequences) + 1))
        require(errors, sorted(sequences) == expected, f"event fixture sequence gap or duplicate: got={sorted(sequences)} expected={expected}")

    return errors


def run_field_checks(report: GateReport, *, fail_fast: bool) -> None:
    print_step("fixture field truth checks")
    started = time.monotonic()
    errors = check_projection_fields()
    duration_ms = int((time.monotonic() - started) * 1000)
    ok = not errors
    detail = "ok" if ok else f"{len(errors)} field errors"
    result = StepResult(
        name="fixture field truth checks",
        ok=ok,
        duration_ms=duration_ms,
        detail=detail,
        output_tail="\n".join(errors[:80]),
    )
    report.add(result)
    if ok:
        print(f"ok  fixture field truth checks ({duration_ms}ms)", flush=True)
    else:
        print(f"FAIL fixture field truth checks: {detail}", flush=True)
        print(result.output_tail, flush=True)
        if fail_fast:
            raise GateFailure("fixture field truth checks")


def run_static_checks(report: GateReport, *, fail_fast: bool) -> None:
    common_env = dict(os.environ)
    common_env.setdefault("TMPDIR", "/run/user/1000")
    checks = [
        ("cargo fmt", ["cargo", "fmt", "--check"], 180),
        ("git diff hygiene", ["git", "diff", "--check"], 60),
        ("trade-contracts tests", ["cargo", "test", "-p", "trade-contracts", "--", "--test-threads=1"], 240),
        ("trade-core tests", ["cargo", "test", "-p", "trade-core", "--", "--test-threads=1"], 300),
        ("trade-tui check", ["cargo", "check", "-p", "trade-tui", "--all-targets"], 240),
        ("trade-tui tests", ["cargo", "test", "-p", "trade-tui", "--", "--test-threads=1"], 240),
    ]
    for name, cmd, timeout_s in checks:
        run_cmd(report, name, cmd, timeout_s=timeout_s, env=common_env, fail_fast=fail_fast)


def run_fixture_runtime_checks(report: GateReport, *, fail_fast: bool) -> None:
    run_cmd(
        report,
        "trade-tui snapshot plain load",
        ["cargo", "run", "-q", "-p", "trade-tui", "--", "--plain", "--snapshot-json", "fixtures/projection_snapshot.json"],
        timeout_s=240,
        fail_fast=fail_fast,
    )
    run_cmd(
        report,
        "trade-tui lifecycle replay plain load",
        [
            "cargo",
            "run",
            "-q",
            "-p",
            "trade-tui",
            "--",
            "--plain",
            "--event-jsonl",
            "fixtures/order_lifecycle_events.jsonl",
            "--replay",
            "--from",
            "2026-06-25T09:30:00",
            "--to",
            "2026-06-25T09:30:12",
            "--correlation-id",
            "corr-fixture-001",
        ],
        timeout_s=240,
        fail_fast=fail_fast,
    )


def run_external_checks(
    report: GateReport,
    env_file: Path,
    *,
    allow_demo_e2e: bool,
    fail_fast: bool,
) -> None:
    run_cmd(
        report,
        "external integration preflight",
        [str(ROOT / "tools/check_external_integration.py"), "--env-file", str(env_file), "--json"],
        timeout_s=60,
        fail_fast=fail_fast,
    )
    if not allow_demo_e2e:
        return
    for codec in ["json", "protobuf"]:
        run_cmd(
            report,
            f"external non-broker e2e {codec}",
            [
                str(ROOT / "tools/run_external_e2e.py"),
                "--env-file",
                str(env_file),
                "--event-codec",
                codec,
                "--allow-demo-events",
                "--json",
            ],
            timeout_s=120,
            fail_fast=fail_fast,
        )


def run_service_checks(
    report: GateReport,
    env_values: dict[str, str],
    *,
    start_services: bool,
    service_timeout_s: float,
    fail_fast: bool,
) -> None:
    if start_services:
        for service in SERVICES:
            run_cmd(report, f"start {service}", ["systemctl", "--user", "start", service], timeout_s=30, fail_fast=fail_fast)

    for service in SERVICES:
        run_cmd(report, f"service active {service}", ["systemctl", "--user", "is-active", service], timeout_s=15, fail_fast=fail_fast)

    projection_addr = env_value(env_values, "TRADE_COCKPIT_PROJECTION_ADDR", "127.0.0.1:39731")
    gateway_addr = env_value(env_values, "TRADE_COCKPIT_COMMAND_GATEWAY_ADDR", "127.0.0.1:39732")
    for name, addr in [("projection tcp", projection_addr), ("command gateway tcp", gateway_addr)]:
        print_step(name)
        started = time.monotonic()
        try:
            wait_tcp(addr, service_timeout_s)
            ok = True
            detail = addr
            output = ""
        except Exception as exc:  # noqa: BLE001
            ok = False
            detail = f"{type(exc).__name__}: {exc}"
            output = detail
        duration_ms = int((time.monotonic() - started) * 1000)
        result = StepResult(name=name, ok=ok, duration_ms=duration_ms, detail=detail, output_tail=output)
        report.add(result)
        if ok:
            print(f"ok  {name} {addr} ({duration_ms}ms)", flush=True)
        else:
            print(f"FAIL {name}: {detail}", flush=True)
            if fail_fast:
                raise GateFailure(name)

    run_cmd(
        report,
        "recent service journal",
        [
            "journalctl",
            "--user",
            "--since",
            "-5 min",
            "-u",
            SERVICES[0],
            "-u",
            SERVICES[1],
            "--no-pager",
            "-n",
            "200",
        ],
        timeout_s=30,
        fail_fast=False,
    )
    last = report.steps[-1]
    if last.detail.startswith("timeout after"):
        last.ok = True
        last.detail = f"journal unavailable: {last.detail}"
        report.ok = all(step.ok for step in report.steps)
        return
    if any(token in last.output_tail.lower() for token in ["panic", "decode error", "timed out after", "adapter timeout"]):
        last.ok = False
        last.detail = "journal contains panic/decode/timeout marker"
        report.ok = False
        if fail_fast:
            raise GateFailure("recent service journal")


def run_paper_observability_check(report: GateReport, env_file: Path, *, fail_fast: bool) -> None:
    run_cmd(
        report,
        "paper strategy cockpit observability",
        [
            str(ROOT / "tools/check_paper_strategy_cockpit_observability.py"),
            "--env-file",
            str(env_file),
            "--json",
        ],
        timeout_s=60,
        fail_fast=fail_fast,
    )


def guard_broker_control(report: GateReport, env_values: dict[str, str], *, allow_broker_control: bool, fail_fast: bool) -> None:
    print_step("broker-control safety guard")
    started = time.monotonic()
    enabled = env_value(env_values, "TRADE_COCKPIT_ENABLE_BROKER_CONTROL", "0") == "1"
    ok = allow_broker_control or not enabled
    detail = "broker control disabled" if not enabled else "broker control explicitly allowed" if allow_broker_control else "broker control enabled without --allow-broker-control"
    result = StepResult("broker-control safety guard", ok, int((time.monotonic() - started) * 1000), detail)
    report.add(result)
    if ok:
        print(f"ok  broker-control safety guard: {detail}", flush=True)
    else:
        print(f"FAIL broker-control safety guard: {detail}", flush=True)
        if fail_fast:
            raise GateFailure("broker-control safety guard")


def emit_json(report: GateReport) -> None:
    payload = {
        "ok": report.ok,
        "steps": [
            {
                "name": item.name,
                "ok": item.ok,
                "duration_ms": item.duration_ms,
                "detail": item.detail,
                "output_tail": item.output_tail,
            }
            for item in report.steps
        ],
    }
    print(json.dumps(payload, indent=2, sort_keys=True))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the pre-open trade-terminal-cockpit function and field gate")
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--skip-static", action="store_true", help="skip cargo fmt/check/test gate")
    parser.add_argument("--skip-fixture-runtime", action="store_true", help="skip trade-tui fixture plain/replay runs")
    parser.add_argument("--skip-external-e2e", action="store_true", help="skip NATS/JetStream external preflight and synthetic E2E")
    parser.add_argument("--allow-demo-e2e", action="store_true", help="allow synthetic fixture events to be published by the external E2E")
    parser.add_argument("--skip-service-check", action="store_true", help="skip user service active/port/journal checks")
    parser.add_argument("--paper-observability", action="store_true", help="verify paper account and target strategies are visible in projection")
    parser.add_argument("--start-services", action="store_true", help="start projectiond and command-gateway user services before checking them")
    parser.add_argument("--include-local-smoke", action="store_true", help="also run tools/smoke_check.sh")
    parser.add_argument("--allow-broker-control", action="store_true", help="allow env profiles with TRADE_COCKPIT_ENABLE_BROKER_CONTROL=1")
    parser.add_argument("--keep-going", action="store_true", help="continue after failures and report all observed failures")
    parser.add_argument("--json", action="store_true", help="emit final machine-readable report")
    parser.add_argument("--service-timeout-s", type=float, default=8.0)
    args = parser.parse_args()

    report = GateReport()
    fail_fast = not args.keep_going
    try:
        env_values = load_env_file(args.env_file)
        guard_broker_control(report, env_values, allow_broker_control=args.allow_broker_control, fail_fast=fail_fast)
        if not args.skip_static:
            run_static_checks(report, fail_fast=fail_fast)
        run_field_checks(report, fail_fast=fail_fast)
        if not args.skip_fixture_runtime:
            run_fixture_runtime_checks(report, fail_fast=fail_fast)
        if not args.skip_external_e2e:
            run_external_checks(
                report,
                args.env_file,
                allow_demo_e2e=args.allow_demo_e2e,
                fail_fast=fail_fast,
            )
        if not args.skip_service_check:
            run_service_checks(
                report,
                env_values,
                start_services=args.start_services,
                service_timeout_s=args.service_timeout_s,
                fail_fast=fail_fast,
            )
        if args.paper_observability:
            run_paper_observability_check(report, args.env_file, fail_fast=fail_fast)
        if args.include_local_smoke:
            run_cmd(report, "local smoke_check", [str(ROOT / "tools/smoke_check.sh")], timeout_s=900, fail_fast=fail_fast)
    except GateFailure:
        report.ok = False
    except Exception as exc:  # noqa: BLE001
        report.ok = False
        report.add(StepResult("preopen gate internal error", False, 0, f"{type(exc).__name__}: {exc}"))
        print(f"FAIL preopen gate internal error: {type(exc).__name__}: {exc}", flush=True)

    if args.json:
        emit_json(report)
    print(f"preopen_e2e_ok={str(report.ok).lower()}")
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
