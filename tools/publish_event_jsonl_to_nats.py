#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from nats_boundaries import (
    is_command_audit_subject,
    is_trading_event_subject,
    validate_audit_stream_boundary,
    validate_event_stream_boundary,
)
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
        event["stream"] = stream
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


def parse_subjects(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def infer_domain(domain: str, stream: str, subject: str, audit_stream: str) -> str:
    if domain != "auto":
        return domain
    if stream == audit_stream or is_command_audit_subject(subject):
        return "audit"
    return "event"


def validate_publish_boundary(
    *,
    domain: str,
    stream: str,
    subject: str,
    event_stream: str,
    audit_stream: str,
    audit_subjects: list[str],
) -> None:
    if domain == "event":
        if stream == audit_stream:
            raise ValueError("event payloads must not be published to the audit stream")
        ok, detail = validate_event_stream_boundary(stream, subject)
        if not ok:
            raise ValueError(detail)
        if not is_trading_event_subject(subject):
            raise ValueError(f"event subject must be trading.event.*: {subject}")
        if event_stream and stream != event_stream:
            raise ValueError(f"event stream mismatch: configured={event_stream} requested={stream}")
        return

    if domain == "audit":
        if stream == event_stream:
            raise ValueError("command/audit payloads must not be published to the event stream")
        ok, detail = validate_audit_stream_boundary(stream, audit_subjects)
        if not ok:
            raise ValueError(detail)
        if not is_command_audit_subject(subject):
            raise ValueError(f"audit subject must be trading.command.* or trading.audit.*: {subject}")
        if stream != audit_stream:
            raise ValueError(f"audit stream mismatch: configured={audit_stream} requested={stream}")
        return

    raise ValueError(f"unknown publish domain: {domain}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish cockpit EventEnvelope JSONL to NATS")
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--event-jsonl", type=Path, required=True)
    parser.add_argument("--stream")
    parser.add_argument("--subject")
    parser.add_argument("--domain", choices=["auto", "event", "audit"], default="auto")
    parser.add_argument("--codec", choices=["json", "protobuf"], default="json")
    parser.add_argument("--rewrite-run-id", default="")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    values = load_env_file(args.env_file)
    nats_url = env_value(values, "TRADE_COCKPIT_NATS_URL", "nats://127.0.0.1:14222")
    event_stream = env_value(values, "TRADE_COCKPIT_JETSTREAM_STREAM", "TRADING_EVENTS")
    event_subject = env_value(values, "TRADE_COCKPIT_NATS_SUBJECT", "trading.event.>")
    audit_stream = env_value(values, "TRADE_COCKPIT_AUDIT_STREAM", "TRADING_AUDIT")
    audit_subjects = parse_subjects(env_value(values, "TRADE_COCKPIT_AUDIT_SUBJECTS", "trading.audit.>,trading.command.>"))
    stream = args.stream or event_stream
    subject = args.subject or event_subject
    environment = env_value(values, "TRADE_COCKPIT_TARGET_ENVIRONMENT", "paper")

    published = []
    with NatsLite(nats_url, name="trade-cockpit-publish-events") as nc:
        for line_number, line in enumerate(args.event_jsonl.read_text(encoding="utf-8").splitlines(), start=1):
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            event = rewrite_event(event, args.rewrite_run_id, stream, subject, environment)
            event_subject_actual = str(event["subject"])
            domain = infer_domain(args.domain, stream, event_subject_actual, audit_stream)
            validate_publish_boundary(
                domain=domain,
                stream=stream,
                subject=event_subject_actual,
                event_stream=event_stream,
                audit_stream=audit_stream,
                audit_subjects=audit_subjects,
            )
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
