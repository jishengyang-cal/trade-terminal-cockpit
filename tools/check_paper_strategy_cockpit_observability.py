#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "tools") not in sys.path:
    sys.path.insert(0, str(ROOT / "tools"))

from trade_nats_lite import env_value, load_env_file


DEFAULT_ENV_FILE = Path.home() / ".config/trade-terminal-cockpit/external.env"
DEFAULT_STRATEGIES = [
    "open-scalp",
    "order-flow-scalp",
    "passive-liquidity-provision",
    "stat-arb-pairs",
    "l2-liquidity-momentum",
    "spread-capture",
    "liquidity-vacuum",
    "orderbook-equilibrium",
    "lob-dynamics",
]


def request_tcp(addr: str, payload: dict[str, Any], timeout: float = 5.0) -> dict[str, Any]:
    host, port_text = addr.rsplit(":", 1)
    with socket.create_connection((host, int(port_text)), timeout=timeout) as sock:
        sock.sendall(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n")
        line = sock.makefile("r", encoding="utf-8").readline()
    if not line:
        raise RuntimeError(f"{addr} returned empty response")
    value = json.loads(line)
    if not isinstance(value, dict):
        raise RuntimeError(f"{addr} returned non-object response")
    return value


def ok_response(response: dict[str, Any], method: str) -> Any:
    if response.get("status") != "ok":
        raise RuntimeError(f"{method} failed: {response.get('error')}")
    return response.get("data")


def wait_for_projection(addr: str, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            ok_response(request_tcp(addr, {"method": "overview"}, timeout=1.0), "overview")
            return
        except Exception as exc:  # noqa: BLE001 - report final projection error.
            last_error = exc
            time.sleep(0.2)
    raise RuntimeError(f"projection not ready at {addr}: {last_error}")


def as_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def money_value(value: Any) -> float | None:
    if isinstance(value, dict) and "value" in value:
        try:
            return float(value["value"]) / float(10 ** int(value.get("scale", 2)))
        except (TypeError, ValueError):
            return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify paper strategies are observable in trade-terminal-cockpit projection")
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--projection-addr")
    parser.add_argument("--account-id", default="DUP278164+paper")
    parser.add_argument("--strategy", action="append", dest="strategies")
    parser.add_argument("--max-account-age-ms", type=int, default=120_000)
    parser.add_argument("--require-account-fresh", action="store_true")
    parser.add_argument("--allow-synthetic-paper-main", action="store_true")
    parser.add_argument("--wait-sec", type=float, default=8.0)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    values = load_env_file(args.env_file)
    projection_addr = args.projection_addr or env_value(values, "TRADE_COCKPIT_PROJECTION_ADDR", "127.0.0.1:39731")
    strategies_expected = args.strategies or DEFAULT_STRATEGIES
    wait_for_projection(projection_addr, args.wait_sec)

    overview = ok_response(request_tcp(projection_addr, {"method": "overview"}), "overview")
    strategies = as_list(ok_response(request_tcp(projection_addr, {"method": "strategies"}), "strategies"))
    risk = ok_response(request_tcp(projection_addr, {"method": "risk"}), "risk")

    accounts = as_list((overview or {}).get("accounts"))
    account_by_id = {str(item.get("account_id")): item for item in accounts}
    strategy_by_id = {str(item.get("strategy_id")): item for item in strategies}
    errors: list[str] = []
    warnings: list[str] = []

    account = account_by_id.get(args.account_id)
    if not account:
        errors.append(f"account {args.account_id} missing from projection")
    else:
        if account.get("canonical_account_id") not in {None, "", args.account_id}:
            errors.append(f"account {args.account_id} canonical mismatch: {account.get('canonical_account_id')}")
        age_ms = account.get("account_snapshot_age_ms")
        if args.require_account_fresh and (not isinstance(age_ms, int) or age_ms > args.max_account_age_ms):
            errors.append(f"account {args.account_id} stale age_ms={age_ms}")
        if account.get("valuation_status") in {None, "", "MISSING"}:
            errors.append(f"account {args.account_id} valuation not populated: {account.get('valuation_status')}")
        for field in ["cash_source", "day_pnl_source", "unrealized_source", "account_snapshot_source"]:
            if not account.get(field):
                errors.append(f"account {args.account_id} missing {field}")
        if money_value(account.get("day_pnl_value")) is None and account.get("day_pnl") in {None, 0, 0.0}:
            warnings.append(f"account {args.account_id} day_pnl is not populated")

    if not args.allow_synthetic_paper_main and "paper-main" in account_by_id and args.account_id not in account_by_id:
        errors.append("projection only exposes synthetic paper-main account")

    missing_strategies = [strategy for strategy in strategies_expected if strategy not in strategy_by_id]
    if missing_strategies:
        errors.append("missing strategies: " + ",".join(missing_strategies))

    strategy_reports = []
    for strategy_id in strategies_expected:
        strategy = strategy_by_id.get(strategy_id)
        if not strategy:
            continue
        state = strategy.get("state")
        reason = strategy.get("last_reason")
        gates = strategy.get("risk_gates") or []
        parameters = strategy.get("parameters") or {}
        if state in {None, "", "UNKNOWN"}:
            errors.append(f"strategy {strategy_id} has no concrete state")
        if state in {"BLOCKED", "FAILED", "STOPPED"} and not reason:
            errors.append(f"strategy {strategy_id} is {state} without reason")
        if not isinstance(gates, list) or not gates:
            warnings.append(f"strategy {strategy_id} has no risk gates")
        if not isinstance(parameters, dict) or "component_role" not in parameters:
            warnings.append(f"strategy {strategy_id} missing component_role parameter")
        strategy_reports.append(
            {
                "strategy_id": strategy_id,
                "state": state,
                "reason": reason,
                "gate_count": len(gates) if isinstance(gates, list) else 0,
            }
        )

    active_blocks = risk.get("active_blocks") if isinstance(risk, dict) else []
    block_reports = []
    if isinstance(active_blocks, list):
        for block in active_blocks:
            if not isinstance(block, dict):
                continue
            scope = str(block.get("scope") or "")
            strategy_id = str(block.get("strategy_id") or "")
            if args.account_id in scope or strategy_id in strategies_expected:
                block_reports.append(
                    {
                        "block_id": block.get("block_id"),
                        "scope": block.get("scope"),
                        "strategy_id": block.get("strategy_id"),
                        "severity": block.get("severity"),
                        "message": block.get("message"),
                    }
                )
    if account and account.get("effective_trade_state") != "TRADE" and not block_reports:
        warnings.append("account is not TRADE but no matching active block was found")

    report = {
        "ok": not errors,
        "projection_addr": projection_addr,
        "account_id": args.account_id,
        "account": account,
        "strategies": strategy_reports,
        "blocks": block_reports,
        "errors": errors,
        "warnings": warnings,
    }
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(f"paper_cockpit_observability_ok={str(report['ok']).lower()}")
        for item in strategy_reports:
            print(f"strategy\t{item['strategy_id']}\t{item['state']}\t{item.get('reason') or ''}")
        for block in block_reports:
            print(f"block\t{block.get('severity')}\t{block.get('scope')}\t{block.get('message')}")
        for warning in warnings:
            print(f"warn\t{warning}")
        for error in errors:
            print(f"fail\t{error}")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
