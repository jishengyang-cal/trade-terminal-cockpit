#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


DEFAULT_RISK_ENGINE = Path.home() / "projects/trading/repos/trading-risk-engine/target/debug/risk-engine-service"
DEFAULT_AUTHORITY = Path.home() / "projects/trading/repos/trading-risk-engine/authority/trading_risk_engine_authority.toml"


def command_symbol(command: dict) -> str:
    payload = command.get("payload") if isinstance(command.get("payload"), dict) else {}
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    return str(data.get("symbol") or "AMD")


def command_strategy(command: dict) -> str:
    payload = command.get("payload") if isinstance(command.get("payload"), dict) else {}
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    return str(data.get("strategy_id") or "opening_scalp").replace("-", "_")


def run_risk_liveness(command: dict, timeout_ms: int) -> dict:
    risk_bin = Path(os.path.expanduser(os.path.expandvars(os.environ.get("TRADE_COCKPIT_RISK_ENGINE_BIN", str(DEFAULT_RISK_ENGINE)))))
    authority = Path(os.path.expanduser(os.path.expandvars(os.environ.get("TRADE_COCKPIT_RISK_AUTHORITY_TOML", str(DEFAULT_AUTHORITY)))))
    if not risk_bin.exists():
        return {"ok": False, "reason": f"risk engine missing: {risk_bin}"}
    if not authority.exists():
        return {"ok": False, "reason": f"risk authority missing: {authority}"}
    cmd = [
        str(risk_bin),
        "--demo",
        "--emit",
        "policy",
        "--strategy-id",
        command_strategy(command),
        "--symbol",
        command_symbol(command),
        "--sector",
        os.environ.get("TRADE_COCKPIT_RISK_SECTOR", "SEMICONDUCTOR"),
        "--authority-toml",
        str(authority),
    ]
    try:
        completed = subprocess.run(
            cmd,
            cwd=str(risk_bin.parents[2]) if len(risk_bin.parents) >= 3 else None,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=max(timeout_ms, 1) / 1000,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "reason": f"risk engine liveness timed out after {timeout_ms}ms"}
    if completed.returncode != 0:
        reason = " ".join(completed.stderr.split())[:240] or f"exit={completed.returncode}"
        return {"ok": False, "reason": f"risk engine liveness failed: {reason}"}
    try:
        policy = json.loads(completed.stdout)
    except json.JSONDecodeError:
        return {"ok": False, "reason": "risk engine returned non-json policy"}
    return {"ok": True, "policy_id": policy.get("policy_id"), "risk_level": policy.get("risk_level")}


def response(status: str, reason: str, reason_codes: list[str], matched_policy_ids: list[str]) -> int:
    print(
        json.dumps(
            {
                "status": status,
                "reason": reason,
                "reason_codes": reason_codes,
                "matched_policy_ids": matched_policy_ids,
                "approved_by": ["risk-command-adapter"],
            },
            sort_keys=True,
        )
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="command-gateway risk adapter for trading cockpit")
    parser.add_argument("--check-command-risk", action="store_true")
    parser.add_argument("--adapter-probe", action="store_true")
    parser.add_argument("--timeout-ms", type=int, default=int(os.environ.get("TRADE_COCKPIT_RISK_ADAPTER_TIMEOUT_MS", "600")))
    args = parser.parse_args()

    if args.adapter_probe:
        probe = run_risk_liveness({"payload": {"data": {"strategy_id": "opening_scalp", "symbol": "AMD"}}}, args.timeout_ms)
        print(json.dumps({"status": "ok" if probe["ok"] else "error", **probe}, sort_keys=True))
        return 0 if probe["ok"] else 1

    if not args.check_command_risk:
        parser.error("expected --check-command-risk or --adapter-probe")

    raw = sys.stdin.read().strip()
    if not raw:
        return response("rejected", "empty command envelope", ["empty_command"], ["risk.adapter"])
    command = json.loads(raw)
    if command.get("target_environment") not in {"paper", "replay", "sim", "live"}:
        return response(
            "rejected",
            "unknown target environment",
            ["target_environment_unknown"],
            ["risk.adapter"],
        )
    if command.get("danger_level") == "dangerous" and not command.get("confirmation_text"):
        return response(
            "rejected",
            "dangerous command has no exact confirmation text",
            ["dangerous_confirmation_missing"],
            ["risk.adapter"],
        )

    liveness = run_risk_liveness(command, args.timeout_ms)
    if not liveness["ok"]:
        return response(
            "rejected",
            liveness["reason"],
            ["risk_engine_liveness_failed"],
            ["risk.adapter", "risk-engine-service"],
        )
    return response(
        "accepted",
        f"risk adapter accepted command; risk_engine_policy={liveness.get('policy_id')}",
        ["risk_adapter_ok", f"risk_level={liveness.get('risk_level')}"],
        ["risk.adapter", "risk-engine-service"],
    )


if __name__ == "__main__":
    raise SystemExit(main())
