#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "tools") not in sys.path:
    sys.path.insert(0, str(ROOT / "tools"))

from nats_boundaries import is_trading_event_subject, validate_event_stream_boundary
from trade_nats_lite import NatsLite, env_value, load_env_file


DEFAULT_ENV_FILE = Path.home() / ".config/trade-terminal-cockpit/external.env"
DEFAULT_STATE_FILE = Path.home() / ".local/state/trade-terminal-cockpit/paper_cockpit_bridge_state.json"
DEFAULT_ACCOUNT_CALLBACK = Path.home() / ".local/state/broker-core/runtime/account_callback_stream.jsonl"
DEFAULT_ACCOUNT_OBSERVATION = Path.home() / ".local/state/broker-core/runtime/account_state_observation.log.jsonl"
DEFAULT_EXECUTION_OBSERVATION = Path.home() / ".local/state/broker-core/runtime/execution_order_observation.log.jsonl"
DEFAULT_BROKER_EXECUTION_PROJECTION = Path.home() / ".local/state/broker-core/runtime/broker_execution_projection.log.jsonl"
DEFAULT_ACCOUNT_BINDING = Path.home() / ".local/state/hot-runtime/account/account_slot_binding_manifest.toml"
DEFAULT_RUNTIME_STATE = Path.home() / ".local/state"
DEFAULT_CONFIG_ROOT = Path.home() / ".config"
MAX_ACCOUNT_STALE_NS = 60_000_000_000
SCALE_2 = 100
KNOWN_STRATEGY_BLOCKERS = [
    "NO_RUNTIME_SERVICE",
    "RUNTIME_SERVICE_INACTIVE",
    "RUNTIME_FAILED",
    "RUNTIME_STOPPED",
    "RUNTIME_STATUS_MISSING",
    "RUNTIME_CONFIG_MISSING",
    "SUBMIT_DISABLED",
    "ARTIFACT_MISSING",
    "ACCOUNT_STATE_STALE",
    "OUTBOUND_DRAIN_DISABLED",
    "EXECUTION_COST_MODEL_MISSING",
    "NO_ACTIVE_ROUTE",
    "LOB_DYNAMICS_UNAVAILABLE",
    "FEATURE_SLAB_MISSING",
]


@dataclass(frozen=True)
class StrategySpec:
    strategy_id: str
    runtime_strategy_id: int | None = None
    runtime_config: Path | None = None
    operator_status: Path | None = None
    activation_manifest: Path | None = None
    env_file: Path | None = None
    artifact_path: Path | None = None
    service: str | None = None
    component_role: str = "strategy"
    lob_dynamics_required: bool = False


def money(value: float | int | None, currency: str = "USD") -> dict[str, Any] | None:
    if value is None:
        return None
    return {"value": int(round(float(value) * SCALE_2)), "scale": 2, "currency": currency}


def ns_now() -> int:
    return time.time_ns()


def expand(path: str | Path | None) -> Path | None:
    if not path:
        return None
    return Path(path).expanduser()


def read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def read_toml(path: Path | None) -> dict[str, Any]:
    if path is None or not path.is_file():
        return {}
    try:
        with path.open("rb") as handle:
            value = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def read_env(path: Path | None) -> dict[str, str]:
    result: dict[str, str] = {}
    if path is None or not path.is_file():
        return result
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        result[key.strip()] = os.path.expandvars(value.strip().strip("'").strip('"'))
    return result


def tail_json_objects(path: Path, max_lines: int = 800, max_bytes: int = 1_048_576) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            offset = max(0, size - max_bytes)
            handle.seek(offset)
            data = handle.read()
    except OSError:
        return []
    lines = data.decode("utf-8", errors="replace").splitlines()
    if offset:
        lines = lines[1:]
    lines = lines[-max_lines:]
    rows: list[dict[str, Any]] = []
    for line in lines:
        text = line.strip()
        if not text:
            continue
        try:
            value = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def last_json_object(path: Path) -> dict[str, Any]:
    rows = tail_json_objects(path, max_lines=80)
    return rows[-1] if rows else {}


def state_load(path: Path) -> dict[str, Any]:
    value = read_json(path)
    if not value:
        return {"sequence": 0}
    return value


def state_save(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def next_sequence(state: dict[str, Any]) -> int:
    value = int(state.get("sequence") or 0) + 1
    state["sequence"] = value
    return value


def envelope(
    *,
    state: dict[str, Any],
    payload_type: str,
    payload_data: dict[str, Any],
    producer: str,
    correlation_id: str,
    causation_id: str = "",
    source_ts_ns: int | None = None,
    aggregate_type: str | None = None,
    aggregate_id: str | None = None,
    environment: str = "paper",
) -> dict[str, Any]:
    seq = next_sequence(state)
    now = ns_now()
    source_ts_ns = source_ts_ns or now
    event_type = "".join(part.capitalize() for part in payload_type.split("_"))
    if aggregate_type is None:
        aggregate_type = {
            "account_snapshot": "account",
            "strategy_heartbeat": "strategy",
            "strategy_health_updated": "strategy",
            "strategy_state_changed": "strategy",
            "risk_limit_breached": "risk",
            "ingest_diagnostic_recorded": "ingest",
            "market_data_summary": "market_data",
        }.get(payload_type, "paper_observability")
    if aggregate_id is None:
        aggregate_id = (
            str(payload_data.get("account_id") or "")
            or str(payload_data.get("strategy_id") or "")
            or str(payload_data.get("scope") or "")
            or "paper"
        )
    return {
        "event_id": f"evt-paper-bridge-{seq:020d}",
        "event_type": event_type,
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id,
        "correlation_id": correlation_id,
        "causation_id": causation_id,
        "source_ts_ns": source_ts_ns,
        "ingest_ts_ns": now,
        "publish_ts_ns": now,
        "sequence": seq,
        "producer": producer,
        "schema_version": "trading.events.v1",
        "partition_key": aggregate_id,
        "environment": environment,
        "payload": {"type": payload_type, "data": payload_data},
    }


def subject_for(event: dict[str, Any], prefix: str) -> str:
    event_type = str(event.get("event_type") or "event").lower()
    aggregate_type = str(event.get("aggregate_type") or "aggregate").lower()
    aggregate_id = str(event.get("aggregate_id") or "paper").replace("+", ".").replace(":", ".").replace("/", ".")
    return f"{prefix}.{aggregate_type}.{event_type}.{aggregate_id}"


def ps_args() -> list[str]:
    completed = subprocess.run(
        ["ps", "-eo", "args="],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if completed.returncode != 0:
        return []
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]


def systemd_is_active(unit: str | None) -> bool | None:
    if not unit:
        return None
    completed = subprocess.run(
        ["systemctl", "--user", "is-active", unit],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    status = completed.stdout.strip()
    if status == "active":
        return True
    if status in {"inactive", "failed", "activating", "deactivating", "unknown"}:
        return False
    return None


def parse_runtime_strategy(runtime_config: Path | None, name: str, runtime_id: int | None) -> dict[str, Any]:
    config = read_toml(runtime_config)
    result: dict[str, Any] = {}
    for row in ((config.get("strategies") or {}).get("list") or []):
        if not isinstance(row, dict):
            continue
        row_name = row.get("name")
        row_id = row.get("id")
        if row_name == name and (runtime_id is None or int(row_id or -1) == runtime_id):
            result.update(row)
            break
    runtime = config.get("runtime") if isinstance(config.get("runtime"), dict) else {}
    if runtime:
        result.setdefault("runtime_universe_activation_manifest_path", runtime.get("runtime_universe_activation_manifest_path"))
        result.setdefault("operator_status_path", runtime.get("operator_status_path"))
    return result


def parse_account_callback(path: Path) -> dict[str, Any]:
    metrics: dict[str, float] = {}
    captured_at = 0
    sequence = 0
    preferred: dict[str, tuple[int, float]] = {}
    for row in tail_json_objects(path, max_lines=1200):
        fields = row.get("fields")
        if not isinstance(fields, list) or len(fields) < 8:
            continue
        key = str(fields[5])
        raw_value = str(fields[6])
        currency = str(fields[7])
        if currency not in {"USD", "BASE"}:
            continue
        try:
            value = float(raw_value)
        except ValueError:
            continue
        rank = 2 if currency == "USD" else 1
        current = preferred.get(key)
        if current is None or rank >= current[0]:
            preferred[key] = (rank, value)
        captured_at = max(captured_at, int(row.get("captured_at_ns_utc") or 0))
        sequence = max(sequence, int(row.get("sequence") or 0))
    for key, (_, value) in preferred.items():
        metrics[key] = value
    metrics["_captured_at_ns_utc"] = float(captured_at)
    metrics["_sequence"] = float(sequence)
    return metrics


def latest_broker_connection(execution_path: Path, projection_path: Path) -> tuple[bool, str | None, int | None, int]:
    latest = {}
    for path in [execution_path, projection_path]:
        row = last_json_object(path)
        if row and int(row.get("captured_at_ns_utc") or 0) >= int(latest.get("captured_at_ns_utc") or 0):
            latest = row
    code = latest.get("error_code")
    try:
        code_int = int(code) if code is not None else None
    except (TypeError, ValueError):
        code_int = None
    text = latest.get("error_text")
    ts_ns = int(latest.get("captured_at_ns_utc") or 0)
    disconnected_codes = {1100, 1101, 1300}
    # Order-level IBKR rejects/cancels should remain visible in broker logs, but
    # they do not mean the broker transport or account channel is down.
    benign_error_codes = {
        102,
        202,
        10243,
        10185,
        2103,
        2104,
        2105,
        2106,
        2107,
        2108,
        2158,
    }
    if code_int in disconnected_codes:
        return False, str(text or f"IBKR error {code_int}"), code_int, ts_ns
    if latest.get("kind") == "error" and code_int not in benign_error_codes and code_int is not None:
        return False, str(text or f"IBKR error {code_int}"), code_int, ts_ns
    return True, None, code_int, ts_ns


def build_account_event(args: argparse.Namespace, state: dict[str, Any], producer: str) -> list[dict[str, Any]]:
    callback = parse_account_callback(args.account_callback)
    observation = last_json_object(args.account_observation)
    binding = read_toml(args.account_binding)
    binding_row = {}
    for row in binding.get("bindings") or []:
        if isinstance(row, dict) and row.get("canonical_account_id") == args.account_id:
            binding_row = row
            break
    account_id = args.account_id
    broker_account_id = str(binding_row.get("account_id") or args.broker_account_id)
    captured_at = int(callback.get("_captured_at_ns_utc") or observation.get("observed_at_ns_utc") or 0)
    source_ts = captured_at or ns_now()
    age_ms = max(0, (ns_now() - source_ts) // 1_000_000)
    broker_connected, broker_error, broker_error_code, broker_ts = latest_broker_connection(
        args.execution_observation,
        args.broker_execution_projection,
    )
    if age_ms > args.max_account_stale_ns // 1_000_000:
        broker_connected = False
        broker_error = broker_error or "account callback is stale"

    cash = callback.get("TotalCashBalance", callback.get("CashBalance"))
    net_liq = callback.get("NetLiquidationByCurrency", callback.get("NetLiquidation"))
    available = callback.get("AvailableFunds")
    buying_power = callback.get("BuyingPower", available)
    realized = callback.get("RealizedPnL", 0.0 if "RealizedPnL" in callback else None)
    unrealized = callback.get("UnrealizedPnL")
    if unrealized is None and observation.get("total_unrealized_pnl_x100") is not None:
        unrealized = float(observation["total_unrealized_pnl_x100"]) / 100.0
    day_pnl = None
    if realized is not None or unrealized is not None:
        day_pnl = float(realized or 0.0) + float(unrealized or 0.0)
    stock_value = callback.get("StockMarketValue")
    total_position = observation.get("total_position_usd_x100")
    exposure = float(total_position) / 100.0 if total_position is not None else stock_value

    missing = []
    for name, value in [
        ("cash", cash),
        ("buy_power", buying_power),
        ("net_liq", net_liq),
        ("available", available),
        ("day_pnl", day_pnl),
        ("realized", realized),
        ("unrealized", unrealized),
    ]:
        if value is None:
            missing.append(name)
    if len(missing) == 7:
        valuation = "MISSING"
    elif age_ms > args.max_account_stale_ns // 1_000_000:
        valuation = "STALE"
    elif missing:
        valuation = "PARTIAL"
    else:
        valuation = "COMPLETE"
    source_name = "broker_core_account_callback_stream"
    data: dict[str, Any] = {
        "account_id": account_id,
        "canonical_account_id": account_id,
        "account_slot": int(binding_row.get("account_slot") or args.account_slot),
        "account_id_hash_hex": str(binding_row.get("account_id_hash_hex") or ""),
        "endpoint_id": "paper.account",
        "gateway_tier": "paper",
        "account_role": "data_and_trade",
        "role_bits": 3,
        "readonly": bool(binding_row.get("readonly", False)),
        "mode": "PAPER",
        "broker": "ibkr_tws",
        "broker_connected": broker_connected,
        "account_currency": "USD",
        "short_permission": False,
        "margin_account": True,
        "account_type": "margin",
        "account_snapshot_id": f"broker-core-account-{int(callback.get('_sequence') or observation.get('observation_sequence') or 0)}",
        "account_snapshot_seq": int(callback.get("_sequence") or observation.get("observation_sequence") or 0),
        "account_snapshot_source": source_name,
        "account_snapshot_ts_ns": source_ts,
        "account_snapshot_age_ms": age_ms,
        "valuation_status": valuation,
        "valuation_ok": valuation == "COMPLETE",
        "valuation_stale": valuation == "STALE",
        "valuation_incomplete_reason": None if not missing else f"missing {','.join(missing)}",
        "cash_source": source_name if cash is not None else None,
        "buying_power_source": source_name if buying_power is not None else None,
        "net_liq_source": source_name if net_liq is not None else None,
        "available_funds_source": source_name if available is not None else None,
        "day_pnl_source": source_name if day_pnl is not None else None,
        "realized_source": source_name if realized is not None else None,
        "unrealized_source": source_name if unrealized is not None else None,
        "valuation_source": source_name,
        "total_fee_today": 0.0,
        "commission_today": 0.0,
        "fees_today": 0.0,
    }
    optional_money = {
        "cash": cash,
        "buying_power": buying_power,
        "available_funds": available,
        "net_liquidation": net_liq,
        "day_pnl": day_pnl,
        "realized_pnl": realized,
        "unrealized_pnl": unrealized,
        "gross_exposure": exposure,
        "net_exposure": exposure,
        "long_market_value": stock_value,
    }
    for key, value in optional_money.items():
        encoded = money(value)
        if encoded is not None:
            data[key] = encoded
    if net_liq and exposure is not None:
        data["exposure_pct"] = abs(float(exposure)) / float(net_liq) * 100.0
    data["trading_restriction"] = (
        f"BROKER_ERROR_{broker_error_code}:{broker_error}" if broker_error and broker_error_code else broker_error or ""
    )

    return [
        envelope(
            state=state,
            payload_type="account_snapshot",
            payload_data=data,
            producer=producer,
            correlation_id=f"paper-account-{account_id}",
            source_ts_ns=source_ts,
            aggregate_id=account_id,
        )
    ]


def activation_counts(path: Path | None) -> dict[str, int | str]:
    manifest = read_json(path)
    if not manifest:
        return {}
    result: dict[str, int | str] = {}
    for key in ["member_count", "strategy_route_count", "entry_budget", "l1_budget", "l2_budget"]:
        if key in manifest:
            try:
                result[key] = int(manifest[key])
            except (TypeError, ValueError):
                pass
    for key in ["trading_date", "activation_id", "universe_version"]:
        if key in manifest:
            result[key] = str(manifest[key])
    return result


def gate(name: str, passed: bool, detail: str, severity: str = "HARD_BLOCK", reason: str | None = None) -> dict[str, Any]:
    return {
        "name": name,
        "passed": passed,
        "detail": detail,
        "scope": "paper",
        "observed": "PASS" if passed else "FAIL",
        "limit": "PASS",
        "status": "PASS" if passed else "FAIL",
        "severity": "INFO" if passed else severity,
        "reason": reason or ("OK" if passed else name),
        "policy_version": "paper-cockpit-observability.v1",
        "evaluated_ts_ns": ns_now(),
    }


def build_strategy_events(
    args: argparse.Namespace,
    state: dict[str, Any],
    specs: list[StrategySpec],
    producer: str,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    processes = ps_args()
    broker_disable_drain = any("--disable-outbound-drain" in row and "broker-core-service" in row for row in processes)
    execution_transport_active = systemd_is_active("broker-core-transport-execution.service")
    execution_cost_model = args.execution_cost_model.expanduser() if args.execution_cost_model else None
    execution_cost_model_present = bool(execution_cost_model and execution_cost_model.is_file())
    for spec in specs:
        status = read_toml(spec.operator_status)
        env = read_env(spec.env_file)
        runtime_row = parse_runtime_strategy(spec.runtime_config, spec.strategy_id, spec.runtime_strategy_id)
        if not spec.artifact_path and env.get("HOT_L3_RUNTIME_L2_LIQUIDITY_MOMENTUM_ARTIFACT_PATH"):
            artifact_path = expand(env.get("HOT_L3_RUNTIME_L2_LIQUIDITY_MOMENTUM_ARTIFACT_PATH"))
        elif not spec.artifact_path and env.get("HOT_L3_RUNTIME_SPREAD_CAPTURE_ARTIFACT_PATH"):
            artifact_path = expand(env.get("HOT_L3_RUNTIME_SPREAD_CAPTURE_ARTIFACT_PATH"))
        elif not spec.artifact_path and env.get("HOT_L3_RUNTIME_LIQUIDITY_VACUUM_ARTIFACT_PATH"):
            artifact_path = expand(env.get("HOT_L3_RUNTIME_LIQUIDITY_VACUUM_ARTIFACT_PATH"))
        else:
            artifact_path = spec.artifact_path
        manifest_path = spec.activation_manifest or expand(
            str(
                runtime_row.get("runtime_universe_activation_manifest_path")
                or env.get("HOT_L3_RUNTIME_UNIVERSE_ACTIVATION_MANIFEST_PATH")
                or ""
            )
        )
        counts = activation_counts(manifest_path)
        service_active = systemd_is_active(spec.service)
        blockers: list[str] = []
        warnings: list[str] = []

        if spec.component_role == "market_data_feature":
            feature_path = expand(env.get("HOT_L3_RUNTIME_FEATURE_SLAB_PATH")) if env else None
            if feature_path and feature_path.exists():
                warnings.append("FEATURE_SLAB_PRESENT")
            else:
                blockers.append("FEATURE_SLAB_MISSING")
        elif not spec.operator_status and not spec.runtime_config and not env:
            blockers.append("NO_RUNTIME_SERVICE")

        if service_active is False and spec.service:
            blockers.append("RUNTIME_SERVICE_INACTIVE")
        process_state = str(status.get("process_state") or "").lower()
        if process_state == "failed":
            blockers.append("RUNTIME_FAILED")
        elif process_state == "stopped":
            blockers.append("RUNTIME_STOPPED")
        elif spec.operator_status and not status:
            blockers.append("RUNTIME_STATUS_MISSING")

        if runtime_row and runtime_row.get("submit_allowed") is not True:
            blockers.append("SUBMIT_DISABLED")
        if env and env.get("HOT_L3_RUNTIME_SUBMIT_ALLOWED") not in {"1", "true", "TRUE", "yes"}:
            blockers.append("SUBMIT_DISABLED")
        if spec.runtime_config and not spec.runtime_config.is_file():
            blockers.append("RUNTIME_CONFIG_MISSING")
        if artifact_path and not artifact_path.is_file():
            blockers.append("ARTIFACT_MISSING")
        if spec.lob_dynamics_required:
            depth_path = expand(env.get("HOT_L3_RUNTIME_LOB_DYNAMICS_DEPTH_SLAB_PATH"))
            if not depth_path or not depth_path.exists():
                blockers.append("LOB_DYNAMICS_UNAVAILABLE")
        stale_age_ns = status.get("account_state_stale_age_ns")
        if isinstance(stale_age_ns, int) and stale_age_ns > args.max_account_stale_ns:
            blockers.append("ACCOUNT_STATE_STALE")
        if env and spec.component_role != "market_data_feature" and int(counts.get("strategy_route_count") or 0) == 0:
            blockers.append("NO_ACTIVE_ROUTE")
        if broker_disable_drain and spec.strategy_id == "order-flow-scalp" and execution_transport_active is not True:
            blockers.append("OUTBOUND_DRAIN_DISABLED")
        if spec.component_role != "market_data_feature" and not execution_cost_model_present:
            blockers.append("EXECUTION_COST_MODEL_MISSING")

        if blockers:
            strategy_state = "FAILED" if "RUNTIME_FAILED" in blockers else "BLOCKED"
            reason = ",".join(dict.fromkeys(blockers))
        else:
            strategy_state = "RUNNING" if process_state == "running" or service_active is True else "IDLE"
            reason = "OK" if not warnings else ",".join(warnings)

        correlation_id = f"paper-strategy-{spec.strategy_id}"
        state_event = envelope(
            state=state,
            payload_type="strategy_state_changed",
            payload_data={
                "strategy_id": spec.strategy_id,
                "state": strategy_state,
                "mode": "PAPER",
                "reason": reason,
            },
            producer=producer,
            correlation_id=correlation_id,
            aggregate_id=spec.strategy_id,
        )
        events.append(state_event)
        events.append(
            envelope(
                state=state,
                payload_type="strategy_heartbeat",
                payload_data={
                    "strategy_id": spec.strategy_id,
                    "state": strategy_state,
                    "mode": "PAPER",
                    "heartbeat_lag_ms": 0,
                },
                producer=producer,
                correlation_id=correlation_id,
                causation_id=state_event["event_id"],
                aggregate_id=spec.strategy_id,
            )
        )

        parameters = {
            "component_role": spec.component_role,
            "runtime_strategy_id": "" if spec.runtime_strategy_id is None else str(spec.runtime_strategy_id),
            "service": spec.service or "",
            "process_state": process_state or "unknown",
            "startup_converged": str(status.get("startup_converged", "")),
            "circuit_bits": str(status.get("circuit_bits", "")),
            "decisions_seen": str(status.get("decisions_seen", 0)),
            "active_written": str(status.get("active_written", 0)),
            "shadow_written": str(status.get("shadow_written", 0)),
            "account_state_stale_age_ns": str(status.get("account_state_stale_age_ns", "")),
            "runtime_config": str(spec.runtime_config or ""),
            "operator_status": str(spec.operator_status or ""),
            "activation_manifest": str(manifest_path or ""),
            "artifact_path": str(artifact_path or ""),
            "broker_outbound_drain_disabled": str(broker_disable_drain).lower(),
            "execution_cost_model": str(execution_cost_model or ""),
        }
        health = {
            "strategy_id": spec.strategy_id,
            "enabled": strategy_state not in {"FAILED"},
            "trading_window": "paper",
            "current_phase": strategy_state,
            "universe_version": str(counts.get("activation_id") or counts.get("trading_date") or "unknown"),
            "universe_count": int(counts.get("member_count") or 0),
            "active_symbol_count": int(counts.get("strategy_route_count") or 0),
            "watched_symbol_count": int(counts.get("member_count") or 0),
            "l2_allocated_symbol_count": int(counts.get("l2_budget") or 0),
            "signals_total_today": int(status.get("decisions_seen") or 0),
            "signals_last_1m": 0,
            "intents_total_today": int(status.get("active_written") or 0),
            "orders_total_today": int(status.get("active_written") or 0),
            "fills_total_today": 0,
            "partial_fills_today": 0,
            "cancels_total_today": 0,
            "rejects_total_today": 0,
            "strategy_realized_pnl": money(0),
            "strategy_unrealized_pnl": money(0),
            "strategy_total_pnl": money(0),
            "pnl_source": "not_published_by_runtime",
            "pnl_basis": "runtime_status_only",
            "pnl_as_of_ts_ns": int(status.get("captured_at_ns_utc") or ns_now()),
            "session_phase": strategy_state,
            "strategy_window_id": "paper-runtime",
            "window_status": strategy_state,
            "is_market_open": None,
            "is_regular_session": None,
            "is_opening_window": None,
            "symbols_blocked": len(blockers),
            "symbols_with_fresh_l1": int(counts.get("l1_budget") or 0),
            "symbols_with_fresh_l2": int(counts.get("l2_budget") or 0),
            "symbols_missing_md": 0 if counts else 1,
            "l1_symbols_allocated": int(counts.get("l1_budget") or 0),
            "l2_capacity": int(counts.get("l2_budget") or 0),
            "l2_capacity_used": int(counts.get("l2_budget") or 0),
            "lease_authority_version": str(counts.get("activation_id") or counts.get("trading_date") or "unknown"),
            "parameters": parameters,
            "risk_gates": [],
        }
        health["risk_gates"].append(gate("runtime_present", "NO_RUNTIME_SERVICE" not in blockers, reason))
        health["risk_gates"].append(gate("runtime_state", process_state not in {"failed", "stopped"} and "RUNTIME_STATUS_MISSING" not in blockers, process_state or "unknown"))
        health["risk_gates"].append(gate("account_state_fresh", "ACCOUNT_STATE_STALE" not in blockers, str(stale_age_ns or "unknown")))
        health["risk_gates"].append(gate("submit_allowed", "SUBMIT_DISABLED" not in blockers, str(runtime_row.get("submit_allowed", env.get("HOT_L3_RUNTIME_SUBMIT_ALLOWED", "unknown")))))
        health["risk_gates"].append(gate("runtime_universe_route", "NO_ACTIVE_ROUTE" not in blockers, str(counts.get("strategy_route_count", "unknown"))))
        health["risk_gates"].append(gate("lob_dynamics", "LOB_DYNAMICS_UNAVAILABLE" not in blockers, "required" if spec.lob_dynamics_required else "not_required"))
        health["risk_gates"].append(gate("execution_cost_model", "EXECUTION_COST_MODEL_MISSING" not in blockers, str(execution_cost_model or "not_configured")))
        events.append(
            envelope(
                state=state,
                payload_type="strategy_health_updated",
                payload_data=health,
                producer=producer,
                correlation_id=correlation_id,
                causation_id=state_event["event_id"],
                aggregate_id=spec.strategy_id,
            )
        )
        for blocker in dict.fromkeys(blockers):
            events.append(
                envelope(
                    state=state,
                    payload_type="risk_limit_breached",
                    payload_data={
                        "scope": f"strategy:{spec.strategy_id}",
                        "severity": "HARD_BLOCK",
                        "message": blocker,
                        "block_id": f"{spec.strategy_id}:{blocker}",
                        "rule_id": blocker,
                        "first_seen_ts_ns": ns_now(),
                        "last_seen_ts_ns": ns_now(),
                        "strategy_id": spec.strategy_id,
                    },
                    producer=producer,
                    correlation_id=correlation_id,
                    causation_id=state_event["event_id"],
                    aggregate_type="risk",
                    aggregate_id=f"strategy:{spec.strategy_id}",
                )
            )
        current_blockers = set(blockers)
        for blocker in KNOWN_STRATEGY_BLOCKERS:
            if blocker in current_blockers:
                continue
            events.append(
                envelope(
                    state=state,
                    payload_type="risk_limit_breached",
                    payload_data={
                        "scope": f"strategy:{spec.strategy_id}",
                        "severity": "INFO",
                        "message": f"cleared {blocker}",
                        "block_id": f"{spec.strategy_id}:{blocker}",
                        "rule_id": blocker,
                        "first_seen_ts_ns": ns_now(),
                        "last_seen_ts_ns": ns_now(),
                        "cleared_ts_ns": ns_now(),
                        "strategy_id": spec.strategy_id,
                    },
                    producer=producer,
                    correlation_id=correlation_id,
                    causation_id=state_event["event_id"],
                    aggregate_type="risk",
                    aggregate_id=f"strategy:{spec.strategy_id}",
                )
            )
    return events


def build_specs(args: argparse.Namespace) -> list[StrategySpec]:
    state_root = args.runtime_state_root
    config_root = args.config_root
    micro_root = config_root / "hot-runtime-microstructure-paper"
    return [
        StrategySpec("open-scalp"),
        StrategySpec(
            "order-flow-scalp",
            1301,
            config_root / "hot-runtime-order-flow-scalp/order_flow_scalp.news_impulse.paper.runtime.toml",
            state_root / "hot-runtime-order-flow-scalp/news_impulse_paper_runtime_operator_status.toml",
            state_root / "hot-runtime-universe/news-impulse-paper/latest.activation_manifest.json",
            service="hot-runtime-order-flow-scalp-news-impulse-paper.service",
        ),
        StrategySpec(
            "passive-liquidity-provision",
            1302,
            config_root / "hot-runtime-passive-liquidity-provision-paper/passive-liquidity-provision.paper.runtime.toml",
            state_root / "hot-runtime-passive-liquidity-provision-paper/runtime_operator_status.toml",
            state_root / "hot-runtime-universe/passive-liquidity-provision-paper/latest.activation_manifest.json",
            service="hot-runtime-passive-liquidity-provision-paper.service",
        ),
        StrategySpec(
            "stat-arb-pairs",
            14,
            config_root / "hot-runtime-stat-arb-pairs/stat_arb_pairs.paper.runtime.toml",
            state_root / "hot-runtime-stat-arb-pairs/runtime_operator_status.toml",
            artifact_path=state_root / "hot-runtime-stat-arb-pairs/artifacts/stat_arb_pairs_pack.toml",
            service="hot-runtime-stat-arb-pairs.service",
        ),
        StrategySpec(
            "l2-liquidity-momentum",
            1401,
            operator_status=state_root / "hot-runtime-microstructure-paper/l2-liquidity-momentum/runtime_operator_status.toml",
            env_file=micro_root / "l2-liquidity-momentum.env",
            service="hot-runtime-microstructure-l2-liquidity-momentum-paper.service",
        ),
        StrategySpec(
            "spread-capture",
            1402,
            operator_status=state_root / "hot-runtime-microstructure-paper/spread-capture/runtime_operator_status.toml",
            env_file=micro_root / "spread-capture.env",
            service="hot-runtime-microstructure-spread-capture-paper.service",
            lob_dynamics_required=True,
        ),
        StrategySpec(
            "liquidity-vacuum",
            1403,
            operator_status=state_root / "hot-runtime-microstructure-paper/liquidity-vacuum/runtime_operator_status.toml",
            env_file=micro_root / "liquidity-vacuum.env",
            service="hot-runtime-microstructure-liquidity-vacuum-paper.service",
            lob_dynamics_required=True,
        ),
        StrategySpec("orderbook-equilibrium", component_role="market_data_feature", env_file=micro_root / "l2-liquidity-momentum.env"),
        StrategySpec("lob-dynamics", component_role="market_data_feature", env_file=micro_root / "spread-capture.env", lob_dynamics_required=True),
    ]


def build_events(args: argparse.Namespace, state: dict[str, Any]) -> list[dict[str, Any]]:
    producer = args.producer
    events: list[dict[str, Any]] = []
    events.extend(build_account_event(args, state, producer))
    events.extend(build_strategy_events(args, state, build_specs(args), producer))
    return events


def publish_events(args: argparse.Namespace, events: list[dict[str, Any]], values: dict[str, str]) -> list[dict[str, Any]]:
    nats_url = env_value(values, "TRADE_COCKPIT_NATS_URL", "nats://127.0.0.1:14222")
    event_stream = env_value(values, "TRADE_COCKPIT_JETSTREAM_STREAM", "TRADING_EVENTS")
    configured_subject = env_value(values, "TRADE_COCKPIT_NATS_SUBJECT", "trading.event.>")
    ok, detail = validate_event_stream_boundary(event_stream, configured_subject)
    if not ok:
        raise RuntimeError(detail)
    published: list[dict[str, Any]] = []
    with NatsLite(nats_url, name="paper-cockpit-bridge") as nc:
        for event in events:
            subject = subject_for(event, args.subject_prefix)
            if not is_trading_event_subject(subject):
                raise RuntimeError(f"invalid trading event subject: {subject}")
            event["stream"] = event_stream
            event["subject"] = subject
            body = json.dumps(event, sort_keys=True, separators=(",", ":")).encode("utf-8")
            response = nc.request(subject, body)
            if isinstance(response, dict) and response.get("error"):
                raise RuntimeError(f"publish failed {subject}: {response['error']}")
            published.append(
                {
                    "event_id": event["event_id"],
                    "event_type": event["event_type"],
                    "subject": subject,
                    "seq": response.get("seq") if isinstance(response, dict) else None,
                }
            )
    return published


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish paper strategy/account observability into trade-terminal-cockpit")
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--state-file", type=Path, default=DEFAULT_STATE_FILE)
    parser.add_argument("--runtime-state-root", type=Path, default=DEFAULT_RUNTIME_STATE)
    parser.add_argument("--config-root", type=Path, default=DEFAULT_CONFIG_ROOT)
    parser.add_argument("--account-callback", type=Path, default=DEFAULT_ACCOUNT_CALLBACK)
    parser.add_argument("--account-observation", type=Path, default=DEFAULT_ACCOUNT_OBSERVATION)
    parser.add_argument("--execution-observation", type=Path, default=DEFAULT_EXECUTION_OBSERVATION)
    parser.add_argument("--broker-execution-projection", type=Path, default=DEFAULT_BROKER_EXECUTION_PROJECTION)
    parser.add_argument("--execution-cost-model", type=Path)
    parser.add_argument("--account-binding", type=Path, default=DEFAULT_ACCOUNT_BINDING)
    parser.add_argument("--account-id", default="DUP278164+paper")
    parser.add_argument("--broker-account-id", default="DUP278164")
    parser.add_argument("--account-slot", type=int, default=1)
    parser.add_argument("--max-account-stale-ns", type=int, default=MAX_ACCOUNT_STALE_NS)
    parser.add_argument("--subject-prefix", default="trading.event.paper")
    parser.add_argument("--producer", default="paper-cockpit-bridge")
    parser.add_argument("--interval-sec", type=float, default=2.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    values = load_env_file(args.env_file)
    if args.execution_cost_model is None:
        configured_cost_model = env_value(values, "TRADE_COCKPIT_EXECUTION_COST_MODEL", "")
        if configured_cost_model:
            args.execution_cost_model = Path(configured_cost_model)
    last_report: dict[str, Any] = {}
    while True:
        state = state_load(args.state_file)
        events = build_events(args, state)
        if args.dry_run:
            for event in events:
                print(json.dumps(event, sort_keys=True, separators=(",", ":")))
            published: list[dict[str, Any]] = []
        else:
            published = publish_events(args, events, values)
        state["last_publish_ts_ns"] = ns_now()
        state["last_event_count"] = len(events)
        state_save(args.state_file, state)
        last_report = {"ok": True, "events": len(events), "published": len(published), "last_sequence": state["sequence"]}
        if args.once:
            break
        if args.json:
            print(json.dumps(last_report, sort_keys=True), flush=True)
        time.sleep(args.interval_sec)
    if args.json:
        print(json.dumps(last_report, indent=2, sort_keys=True))
    else:
        print(f"paper_cockpit_bridge_events={last_report.get('events', 0)} published={last_report.get('published', 0)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
