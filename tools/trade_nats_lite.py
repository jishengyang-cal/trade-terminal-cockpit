#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import socket
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse


DAY_NS = 24 * 60 * 60 * 1_000_000_000


@dataclass(frozen=True)
class NatsAddress:
    host: str
    port: int


def parse_nats_url(raw: str | None = None) -> NatsAddress:
    value = raw or os.environ.get("TRADE_COCKPIT_NATS_URL") or "nats://127.0.0.1:14222"
    parsed = urlparse(value)
    if parsed.scheme and parsed.scheme != "nats":
        raise ValueError(f"unsupported_nats_scheme:{parsed.scheme}")
    if parsed.scheme:
        return NatsAddress(parsed.hostname or "127.0.0.1", parsed.port or 4222)
    host, _, port = value.partition(":")
    return NatsAddress(host or "127.0.0.1", int(port or "4222"))


def load_env_file(path: Path | None) -> dict[str, str]:
    values = dict(os.environ)
    if path is None:
        return values
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if not sep:
            raise ValueError(f"{path}:{line_number}: expected KEY=VALUE")
        values[key.strip()] = os.path.expanduser(os.path.expandvars(value.strip().strip('"').strip("'")))
    return values


def env_value(values: dict[str, str], key: str, default: str = "") -> str:
    return os.path.expanduser(os.path.expandvars(values.get(key, default).strip()))


class NatsLite:
    def __init__(self, url: str | None = None, *, name: str = "trade-cockpit") -> None:
        self.address = parse_nats_url(url)
        self.name = name
        self.sock: socket.socket | None = None
        self._sid = 0

    def connect(self, timeout: float = 5.0) -> None:
        self.close()
        sock = socket.create_connection((self.address.host, self.address.port), timeout=timeout)
        sock.settimeout(timeout)
        self.sock = sock
        line = self._read_line()
        if not line.startswith("INFO "):
            raise RuntimeError(f"expected_INFO_got:{line[:80]}")
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
        raise TimeoutError("nats_connect_pong_timeout")

    def close(self) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            finally:
                self.sock = None

    def publish(self, subject: str, payload: bytes | dict | list | str) -> None:
        if self.sock is None:
            self.connect()
        body = self._payload_bytes(payload)
        self._send(f"PUB {subject} {len(body)}\r\n".encode() + body + b"\r\n")

    def request(
        self,
        subject: str,
        payload: bytes | dict | list | str,
        timeout: float = 5.0,
    ) -> dict:
        if self.sock is None:
            self.connect(timeout=timeout)
        self._sid += 1
        sid = str(self._sid)
        inbox = f"_INBOX.trade_cockpit.{uuid.uuid4().hex}"
        self._send(f"SUB {inbox} {sid}\r\n".encode())
        self._send(f"UNSUB {sid} 1\r\n".encode())
        body = self._payload_bytes(payload)
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
                    if not data:
                        return {}
                    return json.loads(data.decode("utf-8"))
        raise TimeoutError(f"nats_request_timeout:{subject}")

    def _payload_bytes(self, payload: bytes | dict | list | str) -> bytes:
        if isinstance(payload, bytes):
            return payload
        if isinstance(payload, str):
            return payload.encode("utf-8")
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")

    def _send(self, data: bytes) -> None:
        if self.sock is None:
            raise RuntimeError("not_connected")
        self.sock.sendall(data)

    def _read_line(self) -> str:
        data = bytearray()
        while True:
            data += self._read_exact(1)
            if data.endswith(b"\r\n"):
                return data[:-2].decode("utf-8", errors="replace")

    def _read_exact(self, size: int) -> bytes:
        if self.sock is None:
            raise RuntimeError("not_connected")
        chunks = bytearray()
        while len(chunks) < size:
            chunk = self.sock.recv(size - len(chunks))
            if not chunk:
                raise ConnectionError("nats_socket_closed")
            chunks += chunk
        return bytes(chunks)

    def __enter__(self) -> "NatsLite":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
