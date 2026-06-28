use anyhow::{Context, Result};
use prost::Message;
use std::str::FromStr;
use trade_contracts::trading::v1 as pb;
use trade_core::events as domain;
use trade_core::{DomainEvent, EventEnvelope, Money, Price};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventCodec {
    Json,
    Protobuf,
}

impl FromStr for EventCodec {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "protobuf" | "proto" | "pb" => Ok(Self::Protobuf),
            _ => anyhow::bail!("unsupported event codec {value}; expected json or protobuf"),
        }
    }
}

pub fn decode_event_envelope(bytes: &[u8], codec: EventCodec) -> Result<EventEnvelope> {
    match codec {
        EventCodec::Json => serde_json::from_slice::<EventEnvelope>(bytes)
            .context("failed to decode JSON EventEnvelope"),
        EventCodec::Protobuf => decode_protobuf_event_envelope(bytes),
    }
}

fn decode_protobuf_event_envelope(bytes: &[u8]) -> Result<EventEnvelope> {
    let envelope =
        pb::EventEnvelope::decode(bytes).context("failed to decode protobuf envelope")?;
    let payload = decode_protobuf_payload(&envelope.event_type, &envelope.payload)?;
    Ok(EventEnvelope {
        event_id: envelope.event_id,
        event_type: envelope.event_type,
        aggregate_type: envelope.aggregate_type,
        aggregate_id: envelope.aggregate_id,
        correlation_id: envelope.correlation_id,
        causation_id: envelope.causation_id,
        source_ts_ns: envelope.source_ts_ns,
        ingest_ts_ns: envelope.ingest_ts_ns,
        publish_ts_ns: envelope.publish_ts_ns,
        sequence: envelope.sequence,
        producer: envelope.producer,
        schema_version: envelope.schema_version,
        stream: envelope.stream,
        subject: envelope.subject,
        partition_key: envelope.partition_key,
        replay_id: empty_to_none(envelope.replay_id),
        environment: envelope.environment,
        venue_ts_ns: envelope.venue_ts_ns,
        receive_ts_ns: envelope.receive_ts_ns,
        monotonic_ns: envelope.monotonic_ns,
        trace_id: empty_to_none(envelope.trace_id),
        span_id: empty_to_none(envelope.span_id),
        checksum: empty_to_none(envelope.checksum),
        payload,
    })
}

fn decode_protobuf_payload(event_type: &str, payload: &[u8]) -> Result<DomainEvent> {
    Ok(match event_type {
        "AccountSnapshot" => DomainEvent::AccountSnapshot(map_account_snapshot(
            pb::AccountSnapshot::decode(payload)?,
        )),
        "MarketDataSummary" => DomainEvent::MarketDataSummary(map_market_data_summary(
            pb::MarketDataSummary::decode(payload)?,
        )),
        "StrategyHeartbeat" => DomainEvent::StrategyHeartbeat(map_strategy_heartbeat(
            pb::StrategyHeartbeat::decode(payload)?,
        )),
        "StrategyHealthUpdated" => DomainEvent::StrategyHealthUpdated(map_strategy_health(
            pb::StrategyHealthUpdated::decode(payload)?,
        )),
        "StrategyStateChanged" => DomainEvent::StrategyStateChanged(map_strategy_state_changed(
            pb::StrategyStateChanged::decode(payload)?,
        )),
        "SignalGenerated" => DomainEvent::SignalGenerated(map_signal_generated(
            pb::SignalGenerated::decode(payload)?,
        )),
        "IntentCreated" => {
            DomainEvent::IntentCreated(map_intent_created(pb::IntentCreated::decode(payload)?))
        }
        "RiskDecisionMade" => {
            DomainEvent::RiskDecisionMade(map_risk_decision(pb::RiskDecisionMade::decode(payload)?))
        }
        "OrderSubmitRequested" => DomainEvent::OrderSubmitRequested(map_order_submit_requested(
            pb::OrderSubmitRequested::decode(payload)?,
        )),
        "OrderSubmitted" => {
            DomainEvent::OrderSubmitted(map_order_submitted(pb::OrderSubmitted::decode(payload)?))
        }
        "BrokerAckReceived" => {
            DomainEvent::BrokerAckReceived(map_broker_ack(pb::BrokerAckReceived::decode(payload)?))
        }
        "OrderPartiallyFilled" => {
            DomainEvent::OrderPartiallyFilled(map_order_fill(pb::OrderFill::decode(payload)?))
        }
        "OrderFilled" => DomainEvent::OrderFilled(map_order_fill(pb::OrderFill::decode(payload)?)),
        "CancelRequested" => DomainEvent::CancelRequested(map_cancel_requested(
            pb::CancelRequested::decode(payload)?,
        )),
        "CancelRejected" => {
            DomainEvent::CancelRejected(map_cancel_rejected(pb::CancelRejected::decode(payload)?))
        }
        "OrderCancelled" => {
            DomainEvent::OrderCancelled(map_order_cancelled(pb::OrderCancelled::decode(payload)?))
        }
        "OrderRejected" => {
            DomainEvent::OrderRejected(map_order_rejected(pb::OrderRejected::decode(payload)?))
        }
        "PositionSnapshot" => DomainEvent::PositionSnapshot(map_position_snapshot(
            pb::PositionSnapshot::decode(payload)?,
        )),
        "RiskLimitBreached" => DomainEvent::RiskLimitBreached(map_risk_limit_breached(
            pb::RiskLimitBreached::decode(payload)?,
        )),
        "AlertRaised" => {
            DomainEvent::AlertRaised(map_alert_raised(pb::AlertRaised::decode(payload)?))
        }
        "AlertAcknowledged" => DomainEvent::AlertAcknowledged(map_alert_acknowledged(
            pb::AlertAcknowledged::decode(payload)?,
        )),
        "IngestDiagnosticRecorded" => DomainEvent::IngestDiagnosticRecorded(map_ingest_diagnostic(
            pb::IngestDiagnosticRecorded::decode(payload)?,
        )),
        "CommandAuthorityDecided" => DomainEvent::CommandAuthorityDecided(map_command_authority(
            pb::CommandAuthorityDecided::decode(payload)?,
        )),
        "CommandAuditRecorded" => DomainEvent::CommandAuditRecorded(map_command_audit(
            pb::CommandAuditRecorded::decode(payload)?,
        )),
        _ => anyhow::bail!("unsupported protobuf event_type {event_type}"),
    })
}

fn map_account_snapshot(event: pb::AccountSnapshot) -> domain::AccountSnapshot {
    domain::AccountSnapshot {
        account_id: event.account_id,
        canonical_account_id: empty_to_none(event.canonical_account_id),
        account_slot: event.account_slot.and_then(u32_to_u8),
        account_id_hash_hex: empty_to_none(event.account_id_hash_hex),
        endpoint_id: empty_to_none(event.endpoint_id),
        client_id: event.client_id,
        gateway_tier: empty_to_none(event.gateway_tier),
        account_role: empty_to_none(event.account_role),
        role_bits: event.role_bits.and_then(u32_to_u8),
        readonly: event.readonly,
        mode: empty_to_none(event.mode),
        broker: empty_to_none(event.broker),
        broker_connected: event.broker_connected,
        account_currency: empty_to_none(event.account_currency),
        cash: map_money(event.cash),
        buying_power: map_money(event.buying_power),
        day_pnl: map_money(event.day_pnl),
        realized_pnl: map_money(event.realized_pnl),
        unrealized_pnl: map_money(event.unrealized_pnl),
        net_liquidation: map_money(event.net_liquidation),
        equity_with_loan: map_money(event.equity_with_loan),
        initial_margin: map_money(event.initial_margin),
        maintenance_margin: map_money(event.maintenance_margin),
        excess_liquidity: map_money(event.excess_liquidity),
        available_funds: map_money(event.available_funds),
        sma: map_money(event.sma),
        day_trades_remaining: event.day_trades_remaining,
        pdt_status: empty_to_none(event.pdt_status),
        trading_restriction: empty_to_none(event.trading_restriction),
        settled_cash: map_money(event.settled_cash),
        unsettled_cash: map_money(event.unsettled_cash),
        gross_exposure: map_money(event.gross_exposure),
        net_exposure: map_money(event.net_exposure),
        long_market_value: map_money(event.long_market_value),
        short_market_value: map_money(event.short_market_value),
        exposure_pct: event.exposure_pct,
        margin_usage_pct: event.margin_usage_pct,
        short_permission: event.short_permission,
        margin_account: event.margin_account,
        account_type: empty_to_none(event.account_type),
        short_intents_blocked_today: event.short_intents_blocked_today,
    }
}

fn map_market_data_summary(event: pb::MarketDataSummary) -> domain::MarketDataSummary {
    domain::MarketDataSummary {
        symbol: event.symbol,
        source: empty_to_none(event.source),
        bid_price: map_price(event.bid_price),
        ask_price: map_price(event.ask_price),
        spread_bps: event.spread_bps,
        imbalance: event.imbalance,
        microprice: map_price(event.microprice),
        quote_age_ms: event.quote_age_ms,
        event_rate_per_sec: event.event_rate_per_sec,
        wall_size: event.wall_size,
        summary_ts_ns: event.summary_ts_ns,
    }
}

fn map_strategy_heartbeat(event: pb::StrategyHeartbeat) -> domain::StrategyHeartbeat {
    domain::StrategyHeartbeat {
        strategy_id: event.strategy_id,
        state: strategy_state(event.state),
        mode: account_mode(event.mode),
        heartbeat_lag_ms: event.heartbeat_lag_ms,
    }
}

fn map_strategy_health(event: pb::StrategyHealthUpdated) -> domain::StrategyHealthUpdated {
    domain::StrategyHealthUpdated {
        strategy_id: event.strategy_id,
        enabled: event.enabled,
        trading_window: empty_to_none(event.trading_window),
        current_phase: empty_to_none(event.current_phase),
        universe_version: empty_to_none(event.universe_version),
        universe_count: event.universe_count,
        active_symbol_count: event.active_symbol_count,
        watched_symbol_count: event.watched_symbol_count,
        l2_allocated_symbol_count: event.l2_allocated_symbol_count,
        signal_rate_1m: event.signal_rate_1m,
        reject_rate_1m: event.reject_rate_1m,
        fill_rate_1m: event.fill_rate_1m,
        cancel_rate_1m: event.cancel_rate_1m,
        avg_intent_to_submit_ms: event.avg_intent_to_submit_ms,
        avg_submit_to_ack_ms: event.avg_submit_to_ack_ms,
        avg_ack_to_fill_ms: event.avg_ack_to_fill_ms,
        consecutive_stops: event.consecutive_stops,
        trades_today: event.trades_today,
        max_trades_today: event.max_trades_today,
        daily_loss_used_pct: event.daily_loss_used_pct,
        parameters: event.parameters.into_iter().collect(),
        risk_gates: event
            .risk_gates
            .into_iter()
            .map(|gate| domain::StrategyRiskGateProjection {
                name: gate.name,
                passed: gate.passed,
                detail: gate.detail,
            })
            .collect(),
    }
}

fn map_strategy_state_changed(event: pb::StrategyStateChanged) -> domain::StrategyStateChanged {
    domain::StrategyStateChanged {
        strategy_id: event.strategy_id,
        state: strategy_state(event.state),
        mode: account_mode(event.mode),
        reason: event.reason,
    }
}

fn map_signal_generated(event: pb::SignalGenerated) -> domain::SignalGenerated {
    domain::SignalGenerated {
        correlation_id: event.correlation_id,
        strategy_id: event.strategy_id,
        symbol: event.symbol,
        signal_name: event.signal_name,
        score: event.score,
        reason: event.reason,
        account_id: empty_to_none(event.account_id),
        side_hint: empty_to_none(event.side_hint),
        horizon_ms: event.horizon_ms,
        expected_edge_bps: event.expected_edge_bps,
        confidence: event.confidence,
        feature_version: empty_to_none(event.feature_version),
        model_version: empty_to_none(event.model_version),
        market_snapshot_id: empty_to_none(event.market_snapshot_id),
        reference_price: map_price(event.reference_price),
        bid_price: map_price(event.bid_price),
        ask_price: map_price(event.ask_price),
        spread_bps: event.spread_bps,
        imbalance: event.imbalance,
        microprice: map_price(event.microprice),
        volatility_bps: event.volatility_bps,
        liquidity_score: event.liquidity_score,
    }
}

fn map_intent_created(event: pb::IntentCreated) -> domain::IntentCreated {
    domain::IntentCreated {
        correlation_id: event.correlation_id,
        strategy_id: event.strategy_id,
        symbol: event.symbol,
        side: order_side(event.side),
        quantity: event.quantity,
        reason: event.reason,
        account_id: empty_to_none(event.account_id),
        intent_id: empty_to_none(event.intent_id),
        parent_intent_id: empty_to_none(event.parent_intent_id),
        instrument_id: empty_to_none(event.instrument_id),
        asset_class: empty_to_none(event.asset_class),
        currency: empty_to_none(event.currency),
        quantity_type: empty_to_none(event.quantity_type),
        notional: map_money(event.notional),
        limit_price_hint: map_price(event.limit_price_hint),
        stop_price_hint: map_price(event.stop_price_hint),
        time_in_force_hint: empty_to_none(event.time_in_force_hint),
        urgency: empty_to_none(event.urgency),
        position_effect: empty_to_none(event.position_effect),
        max_slippage_bps: event.max_slippage_bps,
        expires_at_ns: event.expires_at_ns,
    }
}

fn map_risk_decision(event: pb::RiskDecisionMade) -> domain::RiskDecisionMade {
    domain::RiskDecisionMade {
        correlation_id: event.correlation_id,
        strategy_id: event.strategy_id,
        symbol: event.symbol,
        approved: event.decision == 1,
        reason_codes: event.reason_codes,
        decision_id: empty_to_none(event.decision_id),
        intent_id: empty_to_none(event.intent_id),
        severity: empty_to_none(event.severity),
        evaluated_rules: event
            .evaluated_rules
            .into_iter()
            .map(|rule| domain::RiskRuleEval {
                rule_id: rule.rule_id,
                rule_name: rule.rule_name,
                passed: rule.passed,
                observed: rule.observed,
                threshold: rule.threshold,
                unit: rule.unit,
            })
            .collect(),
        risk_snapshot_id: empty_to_none(event.risk_snapshot_id),
        account_day_pnl: map_money(event.account_day_pnl),
        strategy_day_pnl: map_money(event.strategy_day_pnl),
        symbol_exposure: map_money(event.symbol_exposure),
        account_exposure: map_money(event.account_exposure),
        remaining_trade_budget: event.remaining_trade_budget,
        remaining_loss_budget: map_money(event.remaining_loss_budget),
        market_data_age_ms: event.market_data_age_ms,
        quote_staleness_ms: event.quote_staleness_ms,
        short_permission: event.short_permission,
        authority_policy_version: empty_to_none(event.authority_policy_version),
    }
}

fn map_order_submit_requested(event: pb::OrderSubmitRequested) -> domain::OrderSubmitRequested {
    domain::OrderSubmitRequested {
        correlation_id: event.correlation_id,
        account_id: event.account_id,
        order_id: event.order_id,
        order_type: event.order_type,
        limit_price: map_price(event.limit_price_value),
        tif: event.tif,
        client_order_id: empty_to_none(event.client_order_id),
        broker_order_id: empty_to_none(event.broker_order_id),
        perm_id: empty_to_none(event.perm_id),
        parent_order_id: empty_to_none(event.parent_order_id),
        oca_group: empty_to_none(event.oca_group),
        route: empty_to_none(event.route),
        destination: empty_to_none(event.destination),
        exchange: empty_to_none(event.exchange),
        order_ref: empty_to_none(event.order_ref),
        side: empty_to_none(order_side(event.side)),
        quantity: event.quantity,
        remaining_quantity: event.remaining_quantity,
        stop_price: map_price(event.stop_price),
        aux_price: map_price(event.aux_price),
        outside_rth: event.outside_rth,
        extended_hours: event.extended_hours,
        allow_preopen: event.allow_preopen,
        allow_after_hours: event.allow_after_hours,
        min_qty: event.min_qty,
        display_size: event.display_size,
        discretionary_amount: map_price(event.discretionary_amount),
        transmit: event.transmit,
    }
}

fn map_order_submitted(event: pb::OrderSubmitted) -> domain::OrderSubmitted {
    domain::OrderSubmitted {
        correlation_id: event.correlation_id,
        account_id: event.account_id,
        order_id: event.order_id,
        broker: event.broker,
        client_order_id: empty_to_none(event.client_order_id),
        broker_order_id: empty_to_none(event.broker_order_id),
        perm_id: empty_to_none(event.perm_id),
        route: empty_to_none(event.route),
        exchange: empty_to_none(event.exchange),
        destination: empty_to_none(event.destination),
    }
}

fn map_broker_ack(event: pb::BrokerAckReceived) -> domain::BrokerAckReceived {
    domain::BrokerAckReceived {
        correlation_id: event.correlation_id,
        account_id: event.account_id,
        order_id: event.order_id,
        broker_order_id: event.broker_order_id,
        broker_status: event.broker_status,
        perm_id: empty_to_none(event.perm_id),
        remaining_quantity: event.remaining_quantity,
        receive_ts_ns: event.receive_ts_ns,
    }
}

fn map_order_fill(event: pb::OrderFill) -> domain::OrderFill {
    domain::OrderFill {
        correlation_id: event.correlation_id,
        account_id: event.account_id,
        order_id: event.order_id,
        filled_quantity: event.filled_quantity,
        fill_price: event.fill_price.map(map_required_price).unwrap_or_default(),
        execution_id: empty_to_none(event.execution_id),
        broker_execution_id: empty_to_none(event.broker_execution_id),
        last_quantity: event.last_quantity,
        cumulative_quantity: event.cumulative_quantity,
        remaining_quantity: event.remaining_quantity,
        last_price: map_price(event.last_price),
        average_price: map_price(event.average_price),
        venue: empty_to_none(event.venue),
        liquidity: empty_to_none(event.liquidity),
        commission: map_money(event.commission),
        fees: event
            .fees
            .into_iter()
            .map(|fee| domain::Fee {
                name: fee.name,
                amount: fee.amount.map(map_required_money).unwrap_or_default(),
            })
            .collect(),
        trade_ts_ns: event.trade_ts_ns,
        report_ts_ns: event.report_ts_ns,
        settlement_currency: empty_to_none(event.settlement_currency),
    }
}

fn map_cancel_requested(event: pb::CancelRequested) -> domain::CancelRequested {
    domain::CancelRequested {
        correlation_id: event.correlation_id,
        account_id: event.account_id,
        order_id: event.order_id,
        reason: event.reason,
    }
}

fn map_cancel_rejected(event: pb::CancelRejected) -> domain::CancelRejected {
    domain::CancelRejected {
        correlation_id: event.correlation_id,
        account_id: event.account_id,
        order_id: event.order_id,
        reason: event.reason,
    }
}

fn map_order_cancelled(event: pb::OrderCancelled) -> domain::OrderCancelled {
    domain::OrderCancelled {
        correlation_id: event.correlation_id,
        account_id: event.account_id,
        order_id: event.order_id,
    }
}

fn map_order_rejected(event: pb::OrderRejected) -> domain::OrderRejected {
    domain::OrderRejected {
        correlation_id: event.correlation_id,
        account_id: event.account_id,
        order_id: event.order_id,
        reason: event.reason,
    }
}

fn map_position_snapshot(event: pb::PositionSnapshot) -> domain::PositionSnapshot {
    domain::PositionSnapshot {
        account_id: event.account_id,
        symbol: event.symbol,
        net_quantity: event.net_quantity,
        average_price: event
            .average_price
            .map(map_required_price)
            .unwrap_or_default(),
        market_price: event
            .market_price
            .map(map_required_price)
            .unwrap_or_default(),
        strategy_attribution: event
            .strategy_attribution
            .into_iter()
            .map(|item| domain::StrategyPositionAttribution {
                strategy_id: item.strategy_id,
                quantity: item.quantity,
            })
            .collect(),
    }
}

fn map_risk_limit_breached(event: pb::RiskLimitBreached) -> domain::RiskLimitBreached {
    domain::RiskLimitBreached {
        scope: event.scope,
        severity: event.severity,
        message: event.message,
        block_id: empty_to_none(event.block_id),
        rule_id: empty_to_none(event.rule_id),
        first_seen_ts_ns: event.first_seen_ts_ns,
        last_seen_ts_ns: event.last_seen_ts_ns,
        cleared_ts_ns: event.cleared_ts_ns,
        correlation_id: empty_to_none(event.correlation_id),
        symbol: empty_to_none(event.symbol),
        strategy_id: empty_to_none(event.strategy_id),
    }
}

fn map_alert_raised(event: pb::AlertRaised) -> domain::AlertRaised {
    domain::AlertRaised {
        alert_id: event.alert_id,
        severity: event.severity,
        domain: event.domain,
        message: event.message,
    }
}

fn map_alert_acknowledged(event: pb::AlertAcknowledged) -> domain::AlertAcknowledged {
    domain::AlertAcknowledged {
        alert_id: event.alert_id,
        operator_id: event.operator_id,
        reason: event.reason,
    }
}

fn map_ingest_diagnostic(event: pb::IngestDiagnosticRecorded) -> domain::IngestDiagnosticRecorded {
    domain::IngestDiagnosticRecorded {
        source: event.source,
        stream: empty_to_none(event.stream),
        consumer: empty_to_none(event.consumer),
        subject: empty_to_none(event.subject),
        severity: event.severity,
        message: event.message,
        error_kind: empty_to_none(event.error_kind),
        reconnect: event.reconnect,
        decode_error: event.decode_error,
        filtered_count: event.filtered_count,
        acked_count: event.acked_count,
    }
}

fn map_command_authority(event: pb::CommandAuthorityDecided) -> domain::CommandAuthorityDecided {
    domain::CommandAuthorityDecided {
        decision_id: event.decision_id,
        command_id: event.command_id,
        status: event.status,
        reason_codes: event.reason_codes,
        matched_policy_ids: event.matched_policy_ids,
        operator_id: event.operator_id,
        command_type: event.command_type,
        capability: event.capability,
        scope: event.scope,
        approved_by: event.approved_by,
        decided_ts_ns: event.decided_ts_ns,
        authority_policy_version: event.authority_policy_version,
        target_environment: event.target_environment,
    }
}

fn map_command_audit(event: pb::CommandAuditRecorded) -> domain::CommandAuditRecorded {
    domain::CommandAuditRecorded {
        command_id: event.command_id,
        operator_id: event.operator_id,
        command_type: event.command_type,
        status: event.status,
        reason: event.reason,
        target: empty_to_none(event.target),
    }
}

fn map_price(value: Option<pb::Price>) -> Option<Price> {
    value.map(map_required_price)
}

fn map_required_price(value: pb::Price) -> Price {
    Price::new(value.value, value.scale, value.currency)
}

fn map_money(value: Option<pb::Money>) -> Option<Money> {
    value.map(map_required_money)
}

fn map_required_money(value: pb::Money) -> Money {
    Money::new(value.value, value.scale, value.currency)
}

fn account_mode(value: i32) -> String {
    match value {
        1 => "PAPER",
        2 => "LIVE",
        3 => "REPLAY",
        _ => "UNKNOWN",
    }
    .to_string()
}

fn strategy_state(value: i32) -> String {
    match value {
        1 => "IDLE",
        2 => "RUNNING",
        3 => "PAUSED",
        4 => "DRAINING",
        5 => "KILLED",
        _ => "UNKNOWN",
    }
    .to_string()
}

fn order_side(value: i32) -> String {
    match value {
        1 => "BUY",
        2 => "SELL",
        3 => "SELL_SHORT",
        4 => "BUY_TO_COVER",
        _ => "UNSPECIFIED",
    }
    .to_string()
}

fn empty_to_none(value: String) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn u32_to_u8(value: u32) -> Option<u8> {
    u8::try_from(value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_protobuf_account_snapshot_envelope() {
        let account = pb::AccountSnapshot {
            account_id: "paper-main".to_string(),
            mode: "PAPER".to_string(),
            canonical_account_id: "paper-main+paper".to_string(),
            account_slot: Some(0),
            gateway_tier: "paper".to_string(),
            account_role: "data_and_trade".to_string(),
            role_bits: Some(3),
            readonly: Some(false),
            short_permission: Some(true),
            margin_account: Some(true),
            account_type: "margin".to_string(),
            ..Default::default()
        };
        let mut payload = Vec::new();
        account.encode(&mut payload).unwrap();

        let envelope = pb::EventEnvelope {
            event_id: "evt-proto-account".to_string(),
            event_type: "AccountSnapshot".to_string(),
            aggregate_type: "account".to_string(),
            aggregate_id: "paper-main".to_string(),
            correlation_id: "corr-proto-account".to_string(),
            causation_id: String::new(),
            source_ts_ns: 1,
            ingest_ts_ns: 2,
            publish_ts_ns: 3,
            sequence: 4,
            producer: "codec-test".to_string(),
            schema_version: "trading.events.v1".to_string(),
            payload,
            stream: "TRADING_EVENTS".to_string(),
            subject: "trading.account.snapshot.paper-main".to_string(),
            partition_key: "paper-main".to_string(),
            replay_id: String::new(),
            environment: "paper".to_string(),
            venue_ts_ns: None,
            receive_ts_ns: None,
            monotonic_ns: None,
            trace_id: String::new(),
            span_id: String::new(),
            checksum: String::new(),
        };
        let mut bytes = Vec::new();
        envelope.encode(&mut bytes).unwrap();

        let decoded = decode_event_envelope(&bytes, EventCodec::Protobuf).unwrap();
        assert_eq!(decoded.event_id, "evt-proto-account");
        let DomainEvent::AccountSnapshot(snapshot) = decoded.payload else {
            panic!("expected account snapshot");
        };
        assert_eq!(
            snapshot.canonical_account_id.as_deref(),
            Some("paper-main+paper")
        );
        assert_eq!(snapshot.account_slot, Some(0));
        assert_eq!(snapshot.account_role.as_deref(), Some("data_and_trade"));
        assert_eq!(snapshot.short_permission, Some(true));
        assert_eq!(snapshot.margin_account, Some(true));
    }
}
