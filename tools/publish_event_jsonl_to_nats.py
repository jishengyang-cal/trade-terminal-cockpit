#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from protobuf_event_codec import encode_event_envelope
from trade_nats_lite import NatsLite, env_value, load_env_file


def subject_for_event(event: dict, default_subject: str) -> str:
    if event.get("subject"):
        return str(event["subject"])
    event_type = str(event.get("event_type") or "event").lower()
    aggregate_type = str(event.get("aggregate_type") or "aggregate").lower()
    aggregate_id = str(event.get("aggregate_id") or "unknown").replace(":", ".")
    if default_subject.endswith(".>"):
        return f"{default_subject[:-2]}.{aggregate_type}.{event_type}.{aggregate_id}"
    return default_subject


def rewrite_event(event: dict, run_id: str, stream: str, subject: str, environment: str) -> dict:
    if not run_id:
        event.setdefault("stream", stream)
        event.setdefault("subject", subject_for_event(event, subject))
        event.setdefault("environment", environment)
        return event

    old_corr = str(event.get("correlation_id") or "")
    corr = f"corr-e2e-{run_id}"
    event["event_id"] = f"{event.get('event_id', 'evt')}-{run_id}"
    event["correlation_id"] = corr
    if event.get("aggregate_id") == old_corr:
        event["aggregate_id"] = corr
    if event.get("causation_id"):
        event["causation_id"] = f"{event['causation_id']}-{run_id}"
    event["producer"] = "trade-terminal-cockpit-e2e"
    event["stream"] = stream
    event["environment"] = environment

    payload = event.get("payload")
    data = payload.get("data") if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else None
    if data is not None:
        if "correlation_id" in data:
            data["correlation_id"] = corr
        if "order_id" in data:
            data["order_id"] = f"ord-e2e-{run_id}"
        if "broker_order_id" in data:
            data["broker_order_id"] = f"brk-e2e-{run_id}"
        if "alert_id" in data:
            data["alert_id"] = f"alert-e2e-{run_id}"

    event["subject"] = subject_for_event(event, subject)
    return event


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish cockpit EventEnvelope JSONL to NATS")
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--event-jsonl", type=Path, required=True)
    parser.add_argument("--stream")
    parser.add_argument("--subject")
    parser.add_argument("--codec", choices=["json", "protobuf"], default="json")
    parser.add_argument("--rewrite-run-id", default="")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    values = load_env_file(args.env_file)
    nats_url = env_value(values, "TRADE_COCKPIT_NATS_URL", "nats://127.0.0.1:14222")
    stream = args.stream or env_value(values, "TRADE_COCKPIT_JETSTREAM_STREAM", "TRADING_EVENTS")
    subject = args.subject or env_value(values, "TRADE_COCKPIT_NATS_SUBJECT", "trading.event.>")
    environment = env_value(values, "TRADE_COCKPIT_TARGET_ENVIRONMENT", "paper")

    published = []
    with NatsLite(nats_url, name="trade-cockpit-publish-events") as nc:
        for line_number, line in enumerate(args.event_jsonl.read_text(encoding="utf-8").splitlines(), start=1):
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            event = rewrite_event(event, args.rewrite_run_id, stream, subject, environment)
            if args.codec == "protobuf":
                body = encode_event_envelope(event)
            else:
                body = json.dumps(event, sort_keys=True, separators=(",", ":")).encode("utf-8")
            response = nc.request(event["subject"], body)
            error = response.get("error") if isinstance(response, dict) else None
            if error:
                raise RuntimeError(f"publish failed line {line_number}: {error}")
            published.append(
                {
                    "event_id": event["event_id"],
                    "event_type": event.get("event_type"),
                    "subject": event["subject"],
                    "stream": response.get("stream"),
                    "seq": response.get("seq"),
                }
            )

    report = {"ok": True, "published": len(published), "events": published}
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(f"published={len(published)}")
        for item in published:
            print(f"{item['seq']}\t{item['event_type']}\t{item['subject']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
