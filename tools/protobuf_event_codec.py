from __future__ import annotations

import struct
from decimal import Decimal, ROUND_HALF_UP


def encode_event_envelope(event: dict) -> bytes:
    payload = encode_domain_payload(event)
    fields: list[bytes] = []
    add_string(fields, 1, event.get("event_id"))
    add_string(fields, 2, event.get("event_type"))
    add_string(fields, 3, event.get("aggregate_type"))
    add_string(fields, 4, event.get("aggregate_id"))
    add_string(fields, 5, event.get("correlation_id"))
    add_string(fields, 6, event.get("causation_id"))
    add_int64(fields, 7, event.get("source_ts_ns"))
    add_int64(fields, 8, event.get("ingest_ts_ns"))
    add_int64(fields, 9, event.get("publish_ts_ns"))
    add_uint64(fields, 10, event.get("sequence"))
    add_string(fields, 11, event.get("producer"))
    add_string(fields, 12, event.get("schema_version"))
    add_bytes(fields, 13, payload)
    add_string(fields, 14, event.get("stream"))
    add_string(fields, 15, event.get("subject"))
    add_string(fields, 16, event.get("partition_key"))
    add_string(fields, 17, event.get("replay_id"))
    add_string(fields, 18, event.get("environment"))
    add_int64(fields, 19, event.get("venue_ts_ns"))
    add_int64(fields, 20, event.get("receive_ts_ns"))
    add_int64(fields, 21, event.get("monotonic_ns"))
    add_string(fields, 22, event.get("trace_id"))
    add_string(fields, 23, event.get("span_id"))
    add_string(fields, 24, event.get("checksum"))
    return b"".join(fields)


def encode_domain_payload(event: dict) -> bytes:
    event_type = str(event.get("event_type") or "")
    payload = event.get("payload") or {}
    data = payload.get("data") if isinstance(payload, dict) else {}
    if not isinstance(data, dict):
        data = {}

    encoders = {
        "AccountSnapshot": encode_account_snapshot,
        "MarketDataSummary": encode_market_data_summary,
        "StrategyHeartbeat": encode_strategy_heartbeat,
        "SignalGenerated": encode_signal_generated,
        "IntentCreated": encode_intent_created,
        "RiskDecisionMade": encode_risk_decision_made,
        "OrderSubmitRequested": encode_order_submit_requested,
        "OrderSubmitted": encode_order_submitted,
        "BrokerAckReceived": encode_broker_ack_received,
        "OrderPartiallyFilled": encode_order_fill,
        "OrderFilled": encode_order_fill,
        "PositionSnapshot": encode_position_snapshot,
        "AlertRaised": encode_alert_raised,
        "CommandAuditRecorded": encode_command_audit_recorded,
    }
    try:
        return encoders[event_type](data)
    except KeyError as exc:
        raise ValueError(f"protobuf encoder does not support event_type {event_type}") from exc


def encode_account_snapshot(data: dict) -> bytes:
    fields: list[bytes] = []
    add_string(fields, 1, data.get("account_id"))
    add_string(fields, 2, data.get("mode"))
    add_string(fields, 3, data.get("broker"))
    add_bool(fields, 4, data.get("broker_connected"))
    add_string(fields, 5, data.get("account_currency"))
    for number, key in [
        (6, "cash"),
        (7, "buying_power"),
        (8, "day_pnl"),
        (9, "realized_pnl"),
        (10, "unrealized_pnl"),
        (11, "net_liquidation"),
        (12, "equity_with_loan"),
        (13, "initial_margin"),
        (14, "maintenance_margin"),
        (15, "excess_liquidity"),
        (16, "available_funds"),
        (17, "sma"),
        (21, "settled_cash"),
        (22, "unsettled_cash"),
        (23, "gross_exposure"),
        (24, "net_exposure"),
        (25, "long_market_value"),
        (26, "short_market_value"),
    ]:
        add_message(fields, number, encode_money(data.get(key)))
    add_int32(fields, 18, data.get("day_trades_remaining"))
    add_string(fields, 19, data.get("pdt_status"))
    add_string(fields, 20, data.get("trading_restriction"))
    add_double(fields, 27, data.get("exposure_pct"))
    add_double(fields, 28, data.get("margin_usage_pct"))
    add_bool(fields, 29, data.get("short_permission"))
    add_uint64(fields, 30, data.get("short_intents_blocked_today"))
    add_string(fields, 31, data.get("canonical_account_id"))
    add_uint32(fields, 32, data.get("account_slot"))
    add_string(fields, 33, data.get("account_id_hash_hex"))
    add_string(fields, 34, data.get("endpoint_id"))
    add_int32(fields, 35, data.get("client_id"))
    add_string(fields, 36, data.get("gateway_tier"))
    add_string(fields, 37, data.get("account_role"))
    add_uint32(fields, 38, data.get("role_bits"))
    add_bool(fields, 39, data.get("readonly"))
    add_bool(fields, 40, data.get("margin_account"))
    add_string(fields, 41, data.get("account_type"))
    return b"".join(fields)


def encode_market_data_summary(data: dict) -> bytes:
    fields: list[bytes] = []
    add_string(fields, 1, data.get("symbol"))
    add_string(fields, 2, data.get("source"))
    add_message(fields, 3, encode_price(data.get("bid_price")))
    add_message(fields, 4, encode_price(data.get("ask_price")))
    add_double(fields, 5, data.get("spread_bps"))
    add_double(fields, 6, data.get("imbalance"))
    add_message(fields, 7, encode_price(data.get("microprice")))
    add_uint64(fields, 8, data.get("quote_age_ms"))
    add_double(fields, 9, data.get("event_rate_per_sec"))
    add_int64(fields, 10, data.get("wall_size"))
    add_int64(fields, 11, data.get("summary_ts_ns"))
    return b"".join(fields)


def encode_strategy_heartbeat(data: dict) -> bytes:
    fields: list[bytes] = []
    add_string(fields, 1, data.get("strategy_id"))
    add_int32(fields, 2, strategy_state_value(data.get("state")))
    add_int32(fields, 3, account_mode_value(data.get("mode")))
    add_uint64(fields, 4, data.get("heartbeat_lag_ms"))
    return b"".join(fields)


def encode_signal_generated(data: dict) -> bytes:
    fields: list[bytes] = []
    add_string(fields, 1, data.get("correlation_id"))
    add_string(fields, 2, data.get("strategy_id"))
    add_string(fields, 3, data.get("symbol"))
    add_string(fields, 4, data.get("signal_name"))
    add_double(fields, 5, data.get("score"))
    add_string(fields, 6, data.get("reason"))
    add_string(fields, 7, data.get("account_id"))
    add_string(fields, 8, data.get("side_hint"))
    add_uint64(fields, 9, data.get("horizon_ms"))
    add_double(fields, 10, data.get("expected_edge_bps"))
    add_double(fields, 11, data.get("confidence"))
    add_string(fields, 12, data.get("feature_version"))
    add_string(fields, 13, data.get("model_version"))
    add_string(fields, 14, data.get("market_snapshot_id"))
    add_message(fields, 15, encode_price(data.get("reference_price")))
    add_message(fields, 16, encode_price(data.get("bid_price")))
    add_message(fields, 17, encode_price(data.get("ask_price")))
    add_double(fields, 18, data.get("spread_bps"))
    add_double(fields, 19, data.get("imbalance"))
    add_message(fields, 20, encode_price(data.get("microprice")))
    add_double(fields, 21, data.get("volatility_bps"))
    add_double(fields, 22, data.get("liquidity_score"))
    return b"".join(fields)


def encode_intent_created(data: dict) -> bytes:
    fields: list[bytes] = []
    add_string(fields, 1, data.get("correlation_id"))
    add_string(fields, 2, data.get("strategy_id"))
    add_string(fields, 3, data.get("symbol"))
    add_int32(fields, 4, order_side_value(data.get("side")))
    add_int64(fields, 5, data.get("quantity"))
    add_string(fields, 6, data.get("reason"))
    add_string(fields, 7, data.get("account_id"))
    add_string(fields, 8, data.get("intent_id"))
    add_string(fields, 9, data.get("parent_intent_id"))
    add_string(fields, 10, data.get("instrument_id"))
    add_string(fields, 11, data.get("asset_class"))
    add_string(fields, 12, data.get("currency"))
    add_string(fields, 13, data.get("quantity_type"))
    add_message(fields, 14, encode_money(data.get("notional")))
    add_message(fields, 15, encode_price(data.get("limit_price_hint")))
    add_message(fields, 16, encode_price(data.get("stop_price_hint")))
    add_string(fields, 17, data.get("time_in_force_hint"))
    add_string(fields, 18, data.get("urgency"))
    add_string(fields, 19, data.get("position_effect"))
    add_double(fields, 20, data.get("max_slippage_bps"))
    add_int64(fields, 21, data.get("expires_at_ns"))
    return b"".join(fields)


def encode_risk_decision_made(data: dict) -> bytes:
    fields: list[bytes] = []
    add_string(fields, 1, data.get("correlation_id"))
    add_string(fields, 2, data.get("strategy_id"))
    add_string(fields, 3, data.get("symbol"))
    add_int32(fields, 4, risk_decision_value(data))
    for reason in data.get("reason_codes") or []:
        add_string(fields, 5, reason)
    add_string(fields, 6, data.get("decision_id"))
    add_string(fields, 7, data.get("intent_id"))
    add_string(fields, 8, data.get("severity"))
    for rule in data.get("evaluated_rules") or []:
        add_message(fields, 9, encode_risk_rule(rule))
    add_string(fields, 10, data.get("risk_snapshot_id"))
    add_message(fields, 11, encode_money(data.get("account_day_pnl")))
    add_message(fields, 12, encode_money(data.get("strategy_day_pnl")))
    add_message(fields, 13, encode_money(data.get("symbol_exposure")))
    add_message(fields, 14, encode_money(data.get("account_exposure")))
    add_int64(fields, 15, data.get("remaining_trade_budget"))
    add_message(fields, 16, encode_money(data.get("remaining_loss_budget")))
    add_uint64(fields, 17, data.get("market_data_age_ms"))
    add_uint64(fields, 18, data.get("quote_staleness_ms"))
    add_bool(fields, 19, data.get("short_permission"))
    add_string(fields, 20, data.get("authority_policy_version"))
    return b"".join(fields)


def encode_order_submit_requested(data: dict) -> bytes:
    fields: list[bytes] = []
    add_string(fields, 1, data.get("correlation_id"))
    add_string(fields, 2, data.get("account_id"))
    add_string(fields, 3, data.get("order_id"))
    add_string(fields, 4, data.get("order_type"))
    add_message(fields, 5, encode_price(data.get("limit_price")))
    add_string(fields, 6, data.get("tif"))
    add_string(fields, 7, data.get("client_order_id"))
    add_string(fields, 8, data.get("broker_order_id"))
    add_string(fields, 9, data.get("perm_id"))
    add_string(fields, 10, data.get("parent_order_id"))
    add_string(fields, 11, data.get("oca_group"))
    add_string(fields, 12, data.get("route"))
    add_string(fields, 13, data.get("destination"))
    add_string(fields, 14, data.get("exchange"))
    add_string(fields, 15, data.get("order_ref"))
    add_int32(fields, 16, order_side_value(data.get("side")))
    add_int64(fields, 17, data.get("quantity"))
    add_int64(fields, 18, data.get("remaining_quantity"))
    add_message(fields, 19, encode_price(data.get("stop_price")))
    add_message(fields, 20, encode_price(data.get("aux_price")))
    add_bool(fields, 21, data.get("outside_rth"))
    add_bool(fields, 22, data.get("extended_hours"))
    add_bool(fields, 23, data.get("allow_preopen"))
    add_bool(fields, 24, data.get("allow_after_hours"))
    add_int64(fields, 25, data.get("min_qty"))
    add_int64(fields, 26, data.get("display_size"))
    add_message(fields, 27, encode_price(data.get("discretionary_amount")))
    add_bool(fields, 28, data.get("transmit", True))
    return b"".join(fields)


def encode_order_submitted(data: dict) -> bytes:
    fields: list[bytes] = []
    add_string(fields, 1, data.get("correlation_id"))
    add_string(fields, 2, data.get("account_id"))
    add_string(fields, 3, data.get("order_id"))
    add_string(fields, 4, data.get("broker"))
    add_string(fields, 5, data.get("client_order_id"))
    add_string(fields, 6, data.get("broker_order_id"))
    add_string(fields, 7, data.get("perm_id"))
    add_string(fields, 8, data.get("route"))
    add_string(fields, 9, data.get("exchange"))
    add_string(fields, 10, data.get("destination"))
    return b"".join(fields)


def encode_broker_ack_received(data: dict) -> bytes:
    fields: list[bytes] = []
    add_string(fields, 1, data.get("correlation_id"))
    add_string(fields, 2, data.get("account_id"))
    add_string(fields, 3, data.get("order_id"))
    add_string(fields, 4, data.get("broker_order_id"))
    add_string(fields, 5, data.get("broker_status"))
    add_string(fields, 6, data.get("perm_id"))
    add_int64(fields, 7, data.get("remaining_quantity"))
    add_int64(fields, 8, data.get("receive_ts_ns"))
    return b"".join(fields)


def encode_order_fill(data: dict) -> bytes:
    fields: list[bytes] = []
    add_string(fields, 1, data.get("correlation_id"))
    add_string(fields, 2, data.get("account_id"))
    add_string(fields, 3, data.get("order_id"))
    add_int64(fields, 4, data.get("filled_quantity"))
    add_message(fields, 5, encode_price(data.get("fill_price")))
    add_string(fields, 6, data.get("execution_id"))
    add_string(fields, 7, data.get("broker_execution_id"))
    add_int64(fields, 8, data.get("last_quantity"))
    add_int64(fields, 9, data.get("cumulative_quantity"))
    add_int64(fields, 10, data.get("remaining_quantity"))
    add_message(fields, 11, encode_price(data.get("last_price")))
    add_message(fields, 12, encode_price(data.get("average_price")))
    add_string(fields, 13, data.get("venue"))
    add_string(fields, 14, data.get("liquidity"))
    add_message(fields, 15, encode_money(data.get("commission")))
    for fee in data.get("fees") or []:
        add_message(fields, 16, encode_fee(fee))
    add_int64(fields, 17, data.get("trade_ts_ns"))
    add_int64(fields, 18, data.get("report_ts_ns"))
    add_string(fields, 19, data.get("settlement_currency"))
    return b"".join(fields)


def encode_position_snapshot(data: dict) -> bytes:
    fields: list[bytes] = []
    add_string(fields, 1, data.get("account_id"))
    add_string(fields, 2, data.get("symbol"))
    add_int64(fields, 3, data.get("net_quantity"))
    add_message(fields, 4, encode_price(data.get("average_price")))
    add_message(fields, 5, encode_price(data.get("market_price")))
    for item in data.get("strategy_attribution") or []:
        add_message(fields, 6, encode_strategy_position_attribution(item))
    return b"".join(fields)


def encode_alert_raised(data: dict) -> bytes:
    fields: list[bytes] = []
    add_string(fields, 1, data.get("alert_id"))
    add_string(fields, 2, data.get("severity"))
    add_string(fields, 3, data.get("domain"))
    add_string(fields, 4, data.get("message"))
    return b"".join(fields)


def encode_command_audit_recorded(data: dict) -> bytes:
    fields: list[bytes] = []
    add_string(fields, 1, data.get("command_id"))
    add_string(fields, 2, data.get("operator_id"))
    add_string(fields, 3, data.get("command_type"))
    add_string(fields, 4, data.get("status"))
    add_string(fields, 5, data.get("reason"))
    add_string(fields, 6, data.get("target"))
    return b"".join(fields)


def encode_strategy_position_attribution(data: dict) -> bytes:
    fields: list[bytes] = []
    add_string(fields, 1, data.get("strategy_id"))
    add_int64(fields, 2, data.get("quantity"))
    return b"".join(fields)


def encode_fee(data: dict) -> bytes:
    fields: list[bytes] = []
    add_string(fields, 1, data.get("name"))
    add_message(fields, 2, encode_money(data.get("amount")))
    return b"".join(fields)


def encode_risk_rule(data: dict) -> bytes:
    fields: list[bytes] = []
    add_string(fields, 1, data.get("rule_id"))
    add_string(fields, 2, data.get("rule_name"))
    add_bool(fields, 3, data.get("passed"))
    add_string(fields, 4, data.get("observed"))
    add_string(fields, 5, data.get("threshold"))
    add_string(fields, 6, data.get("unit"))
    return b"".join(fields)


def encode_price(value: object) -> bytes:
    if value is None:
        return b""
    if isinstance(value, dict):
        amount = int(value.get("value", 0))
        scale = int(value.get("scale", 4))
        currency = str(value.get("currency") or "USD")
    else:
        amount = scaled_integer(value, 4)
        scale = 4
        currency = "USD"
    fields: list[bytes] = []
    add_int64(fields, 1, amount)
    add_int32(fields, 2, scale)
    add_string(fields, 3, currency)
    return b"".join(fields)


def encode_money(value: object) -> bytes:
    if value is None:
        return b""
    if isinstance(value, dict):
        amount = int(value.get("value", 0))
        scale = int(value.get("scale", 2))
        currency = str(value.get("currency") or "USD")
    else:
        amount = scaled_integer(value, 2)
        scale = 2
        currency = "USD"
    fields: list[bytes] = []
    add_int64(fields, 1, amount)
    add_int32(fields, 2, scale)
    add_string(fields, 3, currency)
    return b"".join(fields)


def scaled_integer(value: object, scale: int) -> int:
    quant = Decimal(str(value)) * (Decimal(10) ** scale)
    return int(quant.to_integral_value(rounding=ROUND_HALF_UP))


def account_mode_value(value: object) -> int:
    return {
        "PAPER": 1,
        "LIVE": 2,
        "REPLAY": 3,
    }.get(str(value or "").upper(), 0)


def strategy_state_value(value: object) -> int:
    return {
        "IDLE": 1,
        "RUN": 2,
        "RUNNING": 2,
        "PAUSED": 3,
        "PAUSE": 3,
        "DRAINING": 4,
        "DRAIN": 4,
        "KILLED": 5,
        "KILL": 5,
    }.get(str(value or "").upper(), 0)


def order_side_value(value: object) -> int:
    return {
        "BUY": 1,
        "SELL": 2,
        "SELL_SHORT": 3,
        "BUY_TO_COVER": 4,
    }.get(str(value or "").upper(), 0)


def risk_decision_value(data: dict) -> int:
    if "decision" in data:
        return {
            "APPROVED": 1,
            "REJECTED": 2,
            "WARNING": 3,
        }.get(str(data.get("decision") or "").upper(), 0)
    return 1 if data.get("approved") else 2


def add_string(fields: list[bytes], number: int, value: object) -> None:
    if value is None or value == "":
        return
    add_bytes(fields, number, str(value).encode("utf-8"))


def add_bytes(fields: list[bytes], number: int, value: bytes | bytearray | None) -> None:
    if value is None or len(value) == 0:
        return
    body = bytes(value)
    fields.append(tag(number, 2) + varint(len(body)) + body)


def add_message(fields: list[bytes], number: int, value: bytes) -> None:
    add_bytes(fields, number, value)


def add_bool(fields: list[bytes], number: int, value: object) -> None:
    if value is None:
        return
    fields.append(tag(number, 0) + varint(1 if bool(value) else 0))


def add_int32(fields: list[bytes], number: int, value: object) -> None:
    if value is None:
        return
    add_int64(fields, number, int(value))


def add_uint32(fields: list[bytes], number: int, value: object) -> None:
    if value is None:
        return
    add_uint64(fields, number, int(value))


def add_int64(fields: list[bytes], number: int, value: object) -> None:
    if value is None:
        return
    encoded = int(value)
    if encoded < 0:
        encoded += 1 << 64
    fields.append(tag(number, 0) + varint(encoded))


def add_uint64(fields: list[bytes], number: int, value: object) -> None:
    if value is None:
        return
    fields.append(tag(number, 0) + varint(int(value)))


def add_double(fields: list[bytes], number: int, value: object) -> None:
    if value is None:
        return
    fields.append(tag(number, 1) + struct.pack("<d", float(value)))


def tag(number: int, wire_type: int) -> bytes:
    return varint((number << 3) | wire_type)


def varint(value: int) -> bytes:
    if value < 0:
        raise ValueError("varint cannot encode negative value")
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)
