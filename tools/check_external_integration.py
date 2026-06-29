#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse


CONTROL_BUS_STREAM_PREFIXES = ("OPS_",)
CONTROL_BUS_SUBJECT_PREFIXES = ("ops.",)


@dataclass(frozen=True)
class Check:
    name: str
    ok: bool
    detail: str


@dataclass(frozen=True)
class NatsAddress:
    host: str
    port: int


class NatsLite:
    def __init__(self, url: str, name: str) -> None:
        self.address = parse_nats_url(url)
        self.name = name
        self.sock: socket.socket | None = None
        self.sid = 0

    def connect(self, timeout: float = 3.0) -> None:
        self.close()
        sock = socket.create_connection((self.address.host, self.address.port), timeout=timeout)
        sock.settimeout(timeout)
        self.sock = sock
        line = self._read_line()
        if not line.startswith("INFO "):
            raise RuntimeError(f"expected INFO from NATS, got {line[:80]!r}")
        connect = {
            "verbose": False,
            "pedantic": False,
            "name": self.name,
            "lang": "python-stdlib",
            "version": "1",
            "protocol": 1,
            "echo": False,
        }
        self._send(f"CONNECT {json.dumps(connect, separators=(',', ':'))}\r\n".encode())
        self._send(b"PING\r\n")
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            line = self._read_line()
            if line == "PONG":
                return
            if line.startswith("-ERR"):
                raise RuntimeError(line)
        raise TimeoutError("nats PONG timeout")

    def close(self) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            finally:
                self.sock = None

    def request(self, subject: str, payload: dict, timeout: float = 3.0) -> dict:
        if self.sock is None:
            self.connect(timeout=timeout)
        self.sid += 1
        sid = str(self.sid)
        inbox = f"_INBOX.trade_cockpit.{uuid.uuid4().hex}"
        body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        self._send(f"SUB {inbox} {sid}\r\n".encode())
        self._send(f"UNSUB {sid} 1\r\n".encode())
        self._send(f"PUB {subject} {inbox} {len(body)}\r\n".encode() + body + b"\r\n")
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            line = self._read_line()
            if line == "PING":
                self._send(b"PONG\r\n")
                continue
            if line in {"PONG", "+OK"}:
                continue
            if line.startswith("-ERR"):
                raise RuntimeError(line)
            if line.startswith("MSG "):
                parts = line.split()
                size = int(parts[-1])
                data = self._read_exact(size)
                self._read_exact(2)
                if len(parts) >= 3 and parts[2] == sid:
                    return json.loads(data.decode()) if data else {}
        raise TimeoutError(f"NATS request timed out: {subject}")

    def _send(self, data: bytes) -> None:
        if self.sock is None:
            raise RuntimeError("NATS socket is not connected")
        self.sock.sendall(data)

    def _read_line(self) -> str:
        data = bytearray()
        while True:
            data += self._read_exact(1)
            if data.endswith(b"\r\n"):
                return data[:-2].decode(errors="replace")

    def _read_exact(self, size: int) -> bytes:
        if self.sock is None:
            raise RuntimeError("NATS socket is not connected")
        chunks = bytearray()
        while len(chunks) < size:
            chunk = self.sock.recv(size - len(chunks))
            if not chunk:
                raise ConnectionError("NATS socket closed")
            chunks += chunk
        return bytes(chunks)

    def __enter__(self) -> NatsLite:
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def parse_nats_url(raw: str) -> NatsAddress:
    parsed = urlparse(raw)
    if parsed.scheme and parsed.scheme != "nats":
        raise ValueError(f"unsupported NATS URL scheme: {parsed.scheme}")
    if parsed.scheme:
        return NatsAddress(parsed.hostname or "127.0.0.1", parsed.port or 4222)
    host, _, port = raw.partition(":")
    return NatsAddress(host or "127.0.0.1", int(port or "4222"))


def load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        raise FileNotFoundError(path)
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if not sep or not key:
            raise ValueError(f"{path}:{line_number}: expected KEY=VALUE")
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def merged_env(env_file: Path | None) -> dict[str, str]:
    values = dict(os.environ)
    if env_file is not None:
        for key, value in load_env_file(env_file).items():
            values[key] = os.path.expandvars(value)
    return values


def env_value(values: dict[str, str], key: str, default: str = "") -> str:
    value = values.get(key, default)
    return os.path.expanduser(os.path.expandvars(value.strip()))


def is_control_bus_stream(stream: str) -> bool:
    return stream.upper().startswith(CONTROL_BUS_STREAM_PREFIXES)


def is_control_bus_subject(subject: str) -> bool:
    return subject.lower().startswith(CONTROL_BUS_SUBJECT_PREFIXES)


def subject_compatible(config_subjects: list[str], requested: str) -> bool:
    if not requested:
        return True
    if requested in config_subjects:
        return True
    requested_root = requested.split(".", 1)[0]
    for configured in config_subjects:
        if configured == ">":
            return True
        if configured.endswith(".>") and requested.startswith(configured[:-1]):
            return True
        if requested.endswith(".>") and configured.startswith(requested[:-1]):
            return True
        if configured.split(".", 1)[0] == requested_root:
            return True
    return False


def check_path(name: str, path_text: str, required: bool) -> Check:
    if not path_text:
        return Check(name, not required, "not configured" if not required else "missing config")
    path = Path(path_text)
    if not path.exists():
        return Check(name, False, f"missing: {redact_home(path)}")
    if path.is_file() and not os.access(path, os.X_OK):
        return Check(name, False, f"not executable: {redact_home(path)}")
    return Check(name, True, redact_home(path))


def check_adapter_probe(name: str, path_text: str, enabled: bool) -> Check:
    if not path_text:
        return Check(name, True, "not configured")
    if not enabled:
        return Check(name, True, "probe skipped")
    path = Path(path_text)
    try:
        completed = subprocess.run(
            [str(path), "--adapter-probe"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=3,
            check=False,
        )
    except Exception as exc:  # noqa: BLE001 - preflight should report all failures.
        return Check(name, False, f"{type(exc).__name__}: {exc}")
    if completed.returncode != 0:
        stderr = " ".join(completed.stderr.split())[:240]
        stdout = " ".join(completed.stdout.split())[:240]
        return Check(name, False, stderr or stdout or f"exit={completed.returncode}")
    return Check(name, True, "adapter probe ok")


def redact_home(path: Path) -> str:
    home = Path.home()
    try:
        return "$HOME/" + str(path.resolve().relative_to(home))
    except ValueError:
        return str(path)


def stream_names(nc: NatsLite) -> list[str]:
    response = nc.request("$JS.API.STREAM.NAMES", {"offset": 0})
    return list(response.get("streams") or [])


def run_checks(values: dict[str, str]) -> tuple[list[Check], dict]:
    nats_url = env_value(values, "TRADE_COCKPIT_NATS_URL", "nats://127.0.0.1:14222")
    stream = env_value(values, "TRADE_COCKPIT_JETSTREAM_STREAM", "TRADING_EVENTS")
    subject = env_value(values, "TRADE_COCKPIT_NATS_SUBJECT", "trading.>")
    codec = env_value(values, "TRADE_COCKPIT_EVENT_CODEC", "protobuf")
    enable_broker = env_value(values, "TRADE_COCKPIT_ENABLE_BROKER_CONTROL", "0") == "1"

    checks: list[Check] = []
    summary = {
        "nats_url": nats_url,
        "stream": stream,
        "subject": subject,
        "event_codec": codec,
        "broker_control_enabled": enable_broker,
    }

    if is_control_bus_stream(stream) or is_control_bus_subject(subject):
        checks.append(
            Check(
                "domain_stream_boundary",
                False,
                "refusing OPS/control-bus stream for trading-domain cockpit events",
            )
        )
    else:
        checks.append(Check("domain_stream_boundary", True, "trading-domain stream requested"))

    try:
        with NatsLite(nats_url, name="trade-cockpit-preflight") as nc:
            names = stream_names(nc)
            summary["streams"] = names
            if stream not in names:
                checks.append(Check("jetstream_stream", False, f"{stream} not found; available={','.join(names) or '-'}"))
            else:
                response = nc.request(f"$JS.API.STREAM.INFO.{stream}", {})
                if response.get("error"):
                    checks.append(Check("jetstream_stream", False, str(response["error"])))
                else:
                    config = response.get("config") if isinstance(response.get("config"), dict) else {}
                    state = response.get("state") if isinstance(response.get("state"), dict) else {}
                    subjects = list(config.get("subjects") or [])
                    summary["stream_subjects"] = subjects
                    summary["stream_messages"] = state.get("messages")
                    summary["stream_last_seq"] = state.get("last_seq")
                    checks.append(Check("jetstream_stream", True, f"{stream} messages={state.get('messages', 0)} last_seq={state.get('last_seq', 0)}"))
                    checks.append(
                        Check(
                            "jetstream_subject",
                            subject_compatible(subjects, subject),
                            f"requested={subject} configured={','.join(subjects) or '-'}",
                        )
                    )
    except Exception as exc:  # noqa: BLE001 - preflight must report all local checks.
        checks.append(Check("nats_connect", False, f"{type(exc).__name__}: {exc}"))

    risk_check_bin = env_value(values, "TRADE_COCKPIT_RISK_CHECK_BIN")
    checks.extend(
        [
            check_path("trade_tui_bin", env_value(values, "TRADE_TUI_BIN", ".run/bin/trade-tui"), True),
            check_path("state_projectiond_bin", env_value(values, "TRADE_COCKPIT_STATE_PROJECTIOND_BIN", ".run/bin/state-projectiond"), True),
            check_path("command_gateway_bin", env_value(values, "TRADE_COCKPIT_COMMAND_GATEWAY_BIN", ".run/bin/command-gateway"), True),
            check_path("risk_check_bin", risk_check_bin, False),
            check_adapter_probe("risk_check_adapter_probe", risk_check_bin, bool(risk_check_bin)),
            check_path("strategy_control_bin", env_value(values, "TRADE_COCKPIT_STRATEGY_CONTROL_BIN"), False),
            check_path("order_gateway_bin", env_value(values, "TRADE_COCKPIT_ORDER_GATEWAY_BIN"), False),
            check_path("alert_service_bin", env_value(values, "TRADE_COCKPIT_ALERT_SERVICE_BIN"), False),
        ]
    )
    checks.append(
        check_path(
            "broker_runtime_dir",
            env_value(values, "TRADE_COCKPIT_BROKER_RUNTIME_DIR"),
            enable_broker,
        )
    )
    checks.append(
        check_path(
            "broker_control_bin",
            env_value(values, "TRADE_COCKPIT_BROKER_CONTROL_BIN"),
            enable_broker,
        )
    )
    return checks, summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Preflight external trading cockpit integrations")
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    values = merged_env(args.env_file)
    checks, summary = run_checks(values)
    ok = all(check.ok for check in checks)
    payload = {
        "ok": ok,
        "summary": summary,
        "checks": [{"name": item.name, "ok": item.ok, "detail": item.detail} for item in checks],
    }
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"external_integration_ok={str(ok).lower()}")
        for check in checks:
            status = "ok" if check.ok else "fail"
            print(f"{status}\t{check.name}\t{check.detail}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
