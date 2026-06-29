#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path


DEFAULT_RISK_ENGINE = Path.home() / "projects/trading/repos/trading-risk-engine/target/debug/risk-engine-service"
DEFAULT_AUTHORITY = Path.home() / "projects/trading/repos/trading-risk-engine/authority/trading_risk_engine_authority.toml"
DEFAULT_CACHE_TTL_MS = 30_000


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


def runtime_cache_path() -> Path:
    configured = os.environ.get("TRADE_COCKPIT_RISK_LIVENESS_CACHE")
    if configured:
        return Path(os.path.expanduser(os.path.expandvars(configured)))
    runtime_root = Path(os.environ.get("XDG_RUNTIME_DIR", f"/tmp/trade-terminal-cockpit-{os.getuid()}"))
    return runtime_root / "trade-terminal-cockpit" / "risk-liveness-cache.json"


def cache_ttl_ms() -> int:
    raw = os.environ.get("TRADE_COCKPIT_RISK_LIVENESS_CACHE_TTL_MS", str(DEFAULT_CACHE_TTL_MS))
    try:
        return max(1, int(raw))
    except ValueError:
        return DEFAULT_CACHE_TTL_MS


def write_liveness_cache(probe: dict) -> None:
    if not probe.get("ok"):
        return
    path = runtime_cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "cached_ts_ns": time.time_ns(),
        "policy_id": probe.get("policy_id"),
        "risk_level": probe.get("risk_level"),
    }
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def read_liveness_cache() -> dict:
    path = runtime_cache_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"ok": False, "reason": f"risk liveness cache unavailable: {exc}"}
    cached_ts_ns = payload.get("cached_ts_ns")
    if not isinstance(cached_ts_ns, int):
        return {"ok": False, "reason": "risk liveness cache missing cached_ts_ns"}
    age_ms = max(0, (time.time_ns() - cached_ts_ns) // 1_000_000)
    if age_ms > cache_ttl_ms():
        return {"ok": False, "reason": f"risk liveness cache stale: age_ms={age_ms}"}
    return {
        "ok": True,
        "age_ms": age_ms,
        "policy_id": payload.get("policy_id"),
        "risk_level": payload.get("risk_level"),
    }


def live_on_cache_miss() -> bool:
    return os.environ.get("TRADE_COCKPIT_RISK_LIVE_ON_CACHE_MISS", "0") == "1"


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
        write_liveness_cache(probe)
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
    if command.get("danger_level") == "dangerous":
        return response(
            "rejected",
            "dangerous commands require a live authority service, not cached liveness",
            ["dangerous_live_authority_required"],
            ["risk.adapter"],
        )

    liveness = read_liveness_cache()
    if not liveness["ok"] and live_on_cache_miss():
        liveness = run_risk_liveness(command, args.timeout_ms)
        write_liveness_cache(liveness)
    if not liveness["ok"]:
        return response(
            "rejected",
            liveness["reason"],
            ["risk_engine_liveness_failed"],
            ["risk.adapter", "risk-engine-service"],
        )
    return response(
        "accepted",
        f"risk adapter accepted command from fresh liveness cache; risk_engine_policy={liveness.get('policy_id')}",
        ["risk_adapter_ok", "risk_liveness_cached", f"risk_level={liveness.get('risk_level')}"],
        ["risk.adapter", "risk-engine-service"],
    )


if __name__ == "__main__":
    raise SystemExit(main())
