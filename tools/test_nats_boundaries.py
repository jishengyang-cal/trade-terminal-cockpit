#!/usr/bin/env python3
from __future__ import annotations

from nats_boundaries import (
    is_command_audit_subject,
    is_trading_event_subject,
    validate_audit_stream_boundary,
    validate_event_stream_boundary,
    validate_non_overlapping_streams,
)
from publish_event_jsonl_to_nats import validate_publish_boundary


def assert_ok(result: tuple[bool, str]) -> None:
    ok, detail = result
    assert ok, detail


def assert_fail(result: tuple[bool, str]) -> None:
    ok, _detail = result
    assert not ok


def assert_raises_value_error(fn) -> None:
    try:
        fn()
    except ValueError:
        return
    raise AssertionError("expected ValueError")


def main() -> int:
    assert is_trading_event_subject("trading.event.order.lifecycle.account.order")
    assert is_trading_event_subject("trading.event.>")
    assert not is_trading_event_subject("trading.command.accepted.pause")
    assert not is_trading_event_subject("ops.event.systemd.unit")

    assert is_command_audit_subject("trading.command.authority.pausestrategyrequested")
    assert is_command_audit_subject("trading.audit.event.command.cmd-1")
    assert is_command_audit_subject("trading.command.>")
    assert is_command_audit_subject("trading.audit.>")
    assert not is_command_audit_subject("trading.event.order.lifecycle.account.order")

    assert_ok(validate_event_stream_boundary("TRADING_EVENTS", "trading.event.>"))
    assert_fail(validate_event_stream_boundary("OPS_EVENTS", "trading.event.>"))
    assert_fail(validate_event_stream_boundary("TRADING_EVENTS", "trading.>"))
    assert_fail(validate_event_stream_boundary("TRADING_EVENTS", "ops.event.>"))
    assert_fail(validate_event_stream_boundary("TRADING_EVENTS", "marketdata.raw.>"))
    assert_fail(validate_event_stream_boundary("TRADING_EVENTS", "trading.command.>"))

    assert_ok(validate_audit_stream_boundary("TRADING_AUDIT", ["trading.audit.>", "trading.command.>"]))
    assert_fail(validate_audit_stream_boundary("TRADING_AUDIT", ["trading.event.>"]))
    assert_fail(validate_audit_stream_boundary("OPS_AUDIT", ["trading.audit.>"]))

    assert_ok(
        validate_non_overlapping_streams(
            "TRADING_EVENTS",
            "trading.event.>",
            "TRADING_AUDIT",
            ["trading.audit.>", "trading.command.>"],
        )
    )
    assert_fail(
        validate_non_overlapping_streams(
            "TRADING_EVENTS",
            "trading.event.>",
            "TRADING_EVENTS",
            ["trading.audit.>"],
        )
    )
    assert_fail(
        validate_non_overlapping_streams(
            "TRADING_EVENTS",
            "trading.command.>",
            "TRADING_AUDIT",
            ["trading.audit.>"],
        )
    )
    assert_fail(
        validate_non_overlapping_streams(
            "TRADING_EVENTS",
            "trading.event.>",
            "TRADING_AUDIT",
            ["trading.event.>"],
        )
    )

    validate_publish_boundary(
        domain="event",
        stream="TRADING_EVENTS",
        subject="trading.event.order.lifecycle.account.order",
        event_stream="TRADING_EVENTS",
        audit_stream="TRADING_AUDIT",
        audit_subjects=["trading.audit.>", "trading.command.>"],
    )
    validate_publish_boundary(
        domain="audit",
        stream="TRADING_AUDIT",
        subject="trading.command.authority.pausestrategyrequested",
        event_stream="TRADING_EVENTS",
        audit_stream="TRADING_AUDIT",
        audit_subjects=["trading.audit.>", "trading.command.>"],
    )
    assert_raises_value_error(
        lambda: validate_publish_boundary(
            domain="event",
            stream="TRADING_AUDIT",
            subject="trading.event.order.lifecycle.account.order",
            event_stream="TRADING_EVENTS",
            audit_stream="TRADING_AUDIT",
            audit_subjects=["trading.audit.>", "trading.command.>"],
        )
    )
    assert_raises_value_error(
        lambda: validate_publish_boundary(
            domain="audit",
            stream="TRADING_EVENTS",
            subject="trading.command.authority.pausestrategyrequested",
            event_stream="TRADING_EVENTS",
            audit_stream="TRADING_AUDIT",
            audit_subjects=["trading.audit.>", "trading.command.>"],
        )
    )

    print("nats_boundaries_ok=true")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
