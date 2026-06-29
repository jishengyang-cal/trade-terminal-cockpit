#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from trade_nats_lite import DAY_NS, NatsLite, env_value, load_env_file


def has_error(response: dict, code: int | None = None) -> bool:
    error = response.get("error")
    return isinstance(error, dict) and (code is None or int(error.get("code") or 0) == code)


def stream_config(name: str, subjects: list[str], max_age_days: int, description: str) -> dict:
    return {
        "name": name,
        "description": description,
        "subjects": subjects,
        "retention": "limits",
        "max_consumers": -1,
        "max_msgs": -1,
        "max_bytes": -1,
        "discard": "old",
        "max_age": max_age_days * DAY_NS,
        "max_msgs_per_subject": -1,
        "max_msg_size": -1,
        "storage": "file",
        "num_replicas": 1,
        "duplicate_window": 2 * 60 * 1_000_000_000,
        "allow_rollup_hdrs": False,
        "deny_delete": False,
        "deny_purge": False,
        "allow_direct": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Initialize trading-domain JetStream streams")
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    values = load_env_file(args.env_file)
    nats_url = env_value(values, "TRADE_COCKPIT_NATS_URL", "nats://127.0.0.1:14222")
    event_stream = env_value(values, "TRADE_COCKPIT_JETSTREAM_STREAM", "TRADING_EVENTS")
    event_subject = env_value(values, "TRADE_COCKPIT_NATS_SUBJECT", "trading.>")
    audit_stream = env_value(values, "TRADE_COCKPIT_AUDIT_STREAM", "TRADING_AUDIT")
    audit_subjects = [
        item.strip()
        for item in env_value(
            values,
            "TRADE_COCKPIT_AUDIT_SUBJECTS",
            "trading.audit.>,trading.command.>",
        ).split(",")
        if item.strip()
    ]

    specs = [
        stream_config(
            event_stream,
            [event_subject],
            14,
            "Trading-domain events for terminal cockpit projections",
        ),
        stream_config(
            audit_stream,
            audit_subjects,
            90,
            "Trading command authority and audit events",
        ),
    ]

    results = []
    with NatsLite(nats_url, name="trade-cockpit-init-streams") as nc:
        for config in specs:
            name = config["name"]
            info = nc.request(f"$JS.API.STREAM.INFO.{name}", {})
            if has_error(info, 404):
                response = nc.request(f"$JS.API.STREAM.CREATE.{name}", config)
                action = "created"
            else:
                response = nc.request(f"$JS.API.STREAM.UPDATE.{name}", config)
                action = "updated"
            results.append(
                {
                    "stream": name,
                    "subjects": config["subjects"],
                    "action": action,
                    "ok": not has_error(response),
                    "error": response.get("error"),
                }
            )

    report = {"ok": all(item["ok"] for item in results), "streams": results}
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(f"trading_streams_ok={str(report['ok']).lower()}")
        for item in results:
            status = "ok" if item["ok"] else "fail"
            print(f"{status}\t{item['stream']}\t{item['action']}\t{','.join(item['subjects'])}")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
