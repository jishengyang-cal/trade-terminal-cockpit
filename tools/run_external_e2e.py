#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

from trade_nats_lite import NatsLite, env_value, load_env_file


ROOT = Path(__file__).resolve().parents[1]


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_tcp(addr: str, timeout: float = 5.0) -> None:
    host, port_text = addr.rsplit(":", 1)
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, int(port_text)), timeout=0.25):
                return
        except OSError as exc:
            last_error = exc
            time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for {addr}: {last_error}")


def request_tcp(addr: str, payload: dict, timeout: float = 5.0) -> dict:
    host, port_text = addr.rsplit(":", 1)
    with socket.create_connection((host, int(port_text)), timeout=timeout) as sock:
        sock.sendall(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode() + b"\n")
        file = sock.makefile("r", encoding="utf-8")
        line = file.readline()
    if not line:
        raise RuntimeError(f"{addr} returned empty response")
    return json.loads(line)


def run(cmd: list[str], *, env: dict[str, str] | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        cmd,
        cwd=str(ROOT),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if check and completed.returncode != 0:
        raise RuntimeError(
            f"{' '.join(cmd)} failed exit={completed.returncode} stderr={' '.join(completed.stderr.split())[:400]}"
        )
    return completed


def start(cmd: list[str], env: dict[str, str] | None = None) -> subprocess.Popen[str]:
    return subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def command_envelope(run_id: str, command_type: str, payload: dict, capability: str, danger: str = "controlled") -> dict:
    command_id = f"cmd-e2e-{command_type.lower()}-{run_id}"
    aggregate_type = "alert" if command_type == "AcknowledgeAlertRequested" else "account"
    aggregate_id = payload["data"].get("alert_id") or payload["data"].get("account_id") or "global"
    return {
        "command_id": command_id,
        "command_type": command_type,
        "operator_id": "operator-example",
        "session_id": "local-terminal-example",
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id,
        "correlation_id": f"corr-e2e-{run_id}",
        "requested_ts_ns": int(time.time_ns()),
        "reason": "external e2e verification",
        "capability": capability,
        "danger_level": danger,
        "idempotency_key": command_id,
        "expires_at_ns": int(time.time_ns() + 60_000_000_000),
        "dry_run": False,
        "source": "external-e2e",
        "host_id": socket.gethostname(),
        "terminal_session_id": "external-e2e",
        "approval_id": None,
        "authority_policy_version": "command-gateway.policy.example.v1",
        "requested_by_role": "trader",
        "target_environment": "paper",
        "requires_mfa": False,
        "confirmation_text": None,
        "command_hash": f"{command_type}:{aggregate_type}:{aggregate_id}",
        "payload": payload,
    }


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    events = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            events.append(json.loads(line))
    return events


def stream_info(nats_url: str, stream: str) -> dict:
    with NatsLite(nats_url, name="trade-cockpit-e2e-info") as nc:
        return nc.request(f"$JS.API.STREAM.INFO.{stream}", {})


def main() -> int:
    parser = argparse.ArgumentParser(description="Run external NATS/projection/gateway E2E without broker execution")
    parser.add_argument("--env-file", type=Path, default=Path.home() / ".config/trade-terminal-cockpit/external.env")
    parser.add_argument("--event-codec", choices=["json", "protobuf"], default="json")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    values = load_env_file(args.env_file)
    nats_url = env_value(values, "TRADE_COCKPIT_NATS_URL", "nats://127.0.0.1:14222")
    event_stream = env_value(values, "TRADE_COCKPIT_JETSTREAM_STREAM", "TRADING_EVENTS")
    event_subject = env_value(values, "TRADE_COCKPIT_NATS_SUBJECT", "trading.event.>")
    run_id = str(int(time.time() * 1000))
    projection_addr = f"127.0.0.1:{free_port()}"
    gateway_addr = f"127.0.0.1:{free_port()}"
    audit_jsonl = ROOT / ".run" / "e2e" / f"command-gateway-{run_id}.jsonl"
    audit_jsonl.parent.mkdir(parents=True, exist_ok=True)

    env = dict(os.environ)
    env.update(values)

    checks: list[dict] = []
    processes: list[subprocess.Popen[str]] = []
    try:
        run([str(ROOT / "tools/init_trading_streams.py"), "--env-file", str(args.env_file)])
        run([str(ROOT / "tools/check_external_integration.py"), "--env-file", str(args.env_file)])
        checks.append({"name": "preflight", "ok": True})

        projection = start(
            [
                str(ROOT / ".run/bin/state-projectiond"),
                "--serve",
                projection_addr,
                "--nats-url",
                nats_url,
                "--jetstream-stream",
                event_stream,
                "--jetstream-durable",
                f"trade-cockpit-e2e-{run_id}",
                "--nats-subject",
                event_subject,
                "--event-codec",
                args.event_codec,
            ],
            env,
        )
        processes.append(projection)
        wait_tcp(projection_addr)
        checks.append({"name": "state_projectiond_started", "ok": True, "addr": projection_addr})

        publish = run(
            [
                str(ROOT / "tools/publish_event_jsonl_to_nats.py"),
                "--env-file",
                str(args.env_file),
                "--event-jsonl",
                str(ROOT / "fixtures/order_lifecycle_events.jsonl"),
                "--codec",
                args.event_codec,
                "--rewrite-run-id",
                run_id,
                "--json",
            ],
            env=env,
        )
        published = json.loads(publish.stdout)
        checks.append({"name": "events_published", "ok": published["published"] >= 10, "count": published["published"]})

        corr = f"corr-e2e-{run_id}"
        order_seen = None
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            response = request_tcp(projection_addr, {"method": "orders", "correlation_id": corr})
            orders = response.get("data") if response.get("status") == "ok" else []
            if orders:
                order_seen = orders[0]
                break
            time.sleep(0.1)
        if not order_seen:
            raise RuntimeError("projection did not expose e2e order chain")
        checks.append(
            {
                "name": "projection_order_chain",
                "ok": True,
                "state": order_seen.get("state"),
                "order_id": order_seen.get("order_id"),
            }
        )

        gateway = start(
            [
                str(ROOT / ".run/bin/command-gateway"),
                "--serve",
                gateway_addr,
                "--audit-jsonl",
                str(audit_jsonl),
                "--policy-json",
                str(ROOT / "examples/command-gateway-policy.example.json"),
                "--risk-check-bin",
                str(ROOT / "tools/risk_command_adapter.py"),
            ],
            env,
        )
        processes.append(gateway)
        wait_tcp(gateway_addr)
        checks.append({"name": "command_gateway_started", "ok": True, "addr": gateway_addr})

        ack = command_envelope(
            run_id,
            "AcknowledgeAlertRequested",
            {"type": "acknowledge_alert_requested", "data": {"alert_id": f"alert-e2e-{run_id}"}},
            "alert.ack",
        )
        ack_response = request_tcp(gateway_addr, ack)
        if ack_response.get("status") != "accepted":
            raise RuntimeError(f"ack command was not accepted: {ack_response}")
        checks.append({"name": "command_ack_alert", "ok": True, "status": ack_response.get("status")})

        dangerous = command_envelope(
            run_id,
            "GlobalKillSwitchRequested",
            {"type": "global_kill_switch_requested", "data": {"account_id": "global"}},
            "account.kill",
            "dangerous",
        )
        dangerous_response = request_tcp(gateway_addr, dangerous)
        if dangerous_response.get("status") != "rejected":
            raise RuntimeError(f"dangerous command was not rejected: {dangerous_response}")
        checks.append({"name": "dangerous_command_rejected", "ok": True, "status": dangerous_response.get("status")})

        audit_events = read_jsonl(audit_jsonl)
        if len(audit_events) < 4:
            raise RuntimeError(f"expected command audit events, got {len(audit_events)}")
        checks.append({"name": "command_audit_jsonl", "ok": True, "events": len(audit_events)})

        run(
            [
                str(ROOT / "tools/publish_event_jsonl_to_nats.py"),
                "--env-file",
                str(args.env_file),
                "--event-jsonl",
                str(audit_jsonl),
                "--stream",
                env_value(values, "TRADE_COCKPIT_AUDIT_STREAM", "TRADING_AUDIT"),
                "--subject",
                "trading.command.>",
                "--json",
            ],
            env=env,
        )
        audit_info = stream_info(nats_url, env_value(values, "TRADE_COCKPIT_AUDIT_STREAM", "TRADING_AUDIT"))
        audit_state = audit_info.get("state") if isinstance(audit_info.get("state"), dict) else {}
        checks.append({"name": "audit_stream_written", "ok": int(audit_state.get("messages") or 0) >= len(audit_events), "messages": audit_state.get("messages")})

        report = {"ok": all(item.get("ok") for item in checks), "run_id": run_id, "checks": checks}
    except Exception as exc:  # noqa: BLE001
        report = {"ok": False, "run_id": run_id, "checks": checks, "error": f"{type(exc).__name__}: {exc}"}
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(f"external_e2e_ok={str(report['ok']).lower()} run_id={report['run_id']}")
        for item in report["checks"]:
            print(f"{'ok' if item.get('ok') else 'fail'}\t{item.get('name')}\t{json.dumps(item, sort_keys=True)}")
        if not report["ok"]:
            print(report.get("error", "unknown error"), file=sys.stderr)
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
