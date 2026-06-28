use crate::events::{
    AlertAcknowledged, AlertRaised, BrokerAckReceived, CancelRejected, CancelRequested,
    CommandAuditRecorded, DomainEvent, EventEnvelope, IntentCreated, OrderCancelled, OrderFill,
    OrderRejected, OrderSubmitRequested, OrderSubmitted, PositionSnapshot, RiskDecisionMade,
    RiskLimitBreached, SignalGenerated, StrategyHeartbeat, StrategyStateChanged,
};
use crate::state::{
    AlertView, AppState, EventSummary, OrderLifecycleState, PositionView, RiskBlock,
    RiskDecisionView, StrategyPositionView,
};

pub fn reduce_event(state: &mut AppState, envelope: EventEnvelope) {
    state.connection.last_event_sequence = Some(envelope.sequence);
    state.connection.last_event_ts_ns = Some(envelope.publish_ts_ns);
    state.connection.events_ingested += 1;
    let coalescible = is_coalescible_projection_event(&envelope.payload);
    let summary = summarize(&envelope);
    if coalescible {
        if state.audit.push_or_replace_coalesced(summary) {
            state.connection.events_coalesced += 1;
        }
    } else {
        state.audit.push(summary);
    }
    state.connection.audit_events_retained = state.audit.len();
    let sequence = envelope.sequence;
    let publish_ts_ns = envelope.publish_ts_ns;

    match envelope.payload {
        DomainEvent::StrategyHeartbeat(event) => {
            reduce_strategy_heartbeat(state, sequence, event);
        }
        DomainEvent::StrategyStateChanged(event) => {
            reduce_strategy_state_changed(state, sequence, event);
        }
        DomainEvent::SignalGenerated(event) => {
            reduce_signal_generated(state, sequence, publish_ts_ns, event);
        }
        DomainEvent::IntentCreated(event) => {
            reduce_intent_created(state, sequence, publish_ts_ns, event);
        }
        DomainEvent::RiskDecisionMade(event) => {
            reduce_risk_decision(state, sequence, publish_ts_ns, event);
        }
        DomainEvent::OrderSubmitRequested(event) => {
            reduce_order_submit_requested(state, sequence, publish_ts_ns, event);
        }
        DomainEvent::OrderSubmitted(event) => {
            reduce_order_submitted(state, sequence, publish_ts_ns, event);
        }
        DomainEvent::BrokerAckReceived(event) => {
            reduce_broker_ack(state, sequence, publish_ts_ns, event);
        }
        DomainEvent::OrderPartiallyFilled(event) => {
            reduce_order_fill(state, sequence, publish_ts_ns, event, false);
        }
        DomainEvent::OrderFilled(event) => {
            reduce_order_fill(state, sequence, publish_ts_ns, event, true);
        }
        DomainEvent::CancelRequested(event) => {
            reduce_cancel_requested(state, sequence, publish_ts_ns, event);
        }
        DomainEvent::CancelRejected(event) => {
            reduce_cancel_rejected(state, sequence, publish_ts_ns, event);
        }
        DomainEvent::OrderCancelled(event) => {
            reduce_order_cancelled(state, sequence, publish_ts_ns, event);
        }
        DomainEvent::OrderRejected(event) => {
            reduce_order_rejected(state, sequence, publish_ts_ns, event);
        }
        DomainEvent::PositionSnapshot(event) => reduce_position_snapshot(state, event),
        DomainEvent::RiskLimitBreached(event) => reduce_risk_limit_breached(state, event),
        DomainEvent::AlertRaised(event) => reduce_alert_raised(state, event),
        DomainEvent::AlertAcknowledged(event) => reduce_alert_acknowledged(state, event),
        DomainEvent::CommandAuditRecorded(event) => reduce_command_audit_recorded(state, event),
    }
}

fn is_coalescible_projection_event(event: &DomainEvent) -> bool {
    matches!(
        event,
        DomainEvent::StrategyHeartbeat(_) | DomainEvent::PositionSnapshot(_)
    )
}

fn reduce_strategy_heartbeat(state: &mut AppState, sequence: u64, event: StrategyHeartbeat) {
    let strategy = state.strategies.get_or_insert(&event.strategy_id);
    strategy.state = event.state;
    strategy.mode = event.mode;
    strategy.heartbeat_lag_ms = Some(event.heartbeat_lag_ms);
    strategy.last_event_sequence = Some(sequence);
}

fn reduce_strategy_state_changed(state: &mut AppState, sequence: u64, event: StrategyStateChanged) {
    let strategy = state.strategies.get_or_insert(&event.strategy_id);
    strategy.state = event.state;
    strategy.mode = event.mode;
    strategy.last_reason = Some(event.reason);
    strategy.last_event_sequence = Some(sequence);
}

fn reduce_signal_generated(
    state: &mut AppState,
    sequence: u64,
    publish_ts_ns: i64,
    event: SignalGenerated,
) {
    let strategy = state.strategies.get_or_insert(&event.strategy_id);
    strategy.signals += 1;
    strategy.last_event_sequence = Some(sequence);
    strategy.last_signal_sequence = Some(sequence);

    let score = event
        .score
        .map(|score| format!(" score={score:.4}"))
        .unwrap_or_default();
    let chain = state.orders.get_or_insert_chain(&event.correlation_id);
    chain.strategy_id = Some(event.strategy_id);
    chain.symbol = Some(event.symbol.clone());
    chain.state = OrderLifecycleState::SignalGenerated;
    chain.push_timeline(
        sequence,
        publish_ts_ns,
        "SIGNAL",
        format!(
            "{} {}{} reason={}",
            event.symbol, event.signal_name, score, event.reason
        ),
    );
}

fn reduce_intent_created(
    state: &mut AppState,
    sequence: u64,
    publish_ts_ns: i64,
    event: IntentCreated,
) {
    let strategy = state.strategies.get_or_insert(&event.strategy_id);
    strategy.intents += 1;
    strategy.last_event_sequence = Some(sequence);
    strategy.last_intent_sequence = Some(sequence);

    let chain = state.orders.get_or_insert_chain(&event.correlation_id);
    chain.strategy_id = Some(event.strategy_id);
    chain.symbol = Some(event.symbol.clone());
    chain.side = Some(event.side.clone());
    chain.intended_quantity = Some(event.quantity);
    chain.state = OrderLifecycleState::IntentCreated;
    chain.push_timeline(
        sequence,
        publish_ts_ns,
        "INTENT",
        format!(
            "{} {} {} reason={}",
            event.side, event.quantity, event.symbol, event.reason
        ),
    );
}

fn reduce_risk_decision(
    state: &mut AppState,
    sequence: u64,
    publish_ts_ns: i64,
    event: RiskDecisionMade,
) {
    let chain = state.orders.get_or_insert_chain(&event.correlation_id);
    chain.strategy_id.get_or_insert(event.strategy_id.clone());
    chain.symbol.get_or_insert(event.symbol.clone());
    chain.risk = Some(RiskDecisionView {
        approved: event.approved,
        reason_codes: event.reason_codes.clone(),
    });
    chain.state = if event.approved {
        OrderLifecycleState::RiskApproved
    } else {
        OrderLifecycleState::RiskRejected
    };
    let status = if event.approved { "PASS" } else { "REJECT" };
    chain.push_timeline(
        sequence,
        publish_ts_ns,
        "RISK",
        format!("{status} {}", event.reason_codes.join(",")),
    );

    if !event.approved {
        state.risk.active_blocks.push(RiskBlock {
            scope: format!("{}/{}", event.strategy_id, event.symbol),
            severity: "block".to_string(),
            message: event.reason_codes.join(","),
        });
        if event
            .reason_codes
            .iter()
            .any(|reason| reason.contains("short_permission"))
        {
            state.account.short_intents_blocked_today += 1;
        }
    }
}

fn reduce_order_submit_requested(
    state: &mut AppState,
    sequence: u64,
    publish_ts_ns: i64,
    event: OrderSubmitRequested,
) {
    let chain = state.orders.get_or_insert_chain(&event.correlation_id);
    chain.account_id = Some(event.account_id.clone());
    chain.order_id = Some(event.order_id.clone());
    chain.order_type = Some(event.order_type.clone());
    chain.limit_price = event.limit_price;
    chain.tif = Some(event.tif.clone());
    chain.state = OrderLifecycleState::SubmitRequested;
    chain.push_timeline(
        sequence,
        publish_ts_ns,
        "SUBMIT_REQ",
        format!(
            "{} {} tif={}",
            event.order_type,
            event
                .limit_price
                .map(|price| price.to_string())
                .unwrap_or_else(|| "MKT".to_string()),
            event.tif
        ),
    );
    state
        .orders
        .index_order(&event.account_id, &event.order_id, &event.correlation_id);
    state.account.account_id = event.account_id;
}

fn reduce_order_submitted(
    state: &mut AppState,
    sequence: u64,
    publish_ts_ns: i64,
    event: OrderSubmitted,
) {
    let strategy_id = {
        let chain = state.orders.get_or_insert_chain(&event.correlation_id);
        chain.account_id = Some(event.account_id.clone());
        chain.order_id = Some(event.order_id.clone());
        chain.broker = Some(event.broker.clone());
        chain.state = OrderLifecycleState::SubmittedToBroker;
        chain.push_timeline(
            sequence,
            publish_ts_ns,
            "SUBMITTED",
            format!("{} order_id={}", event.broker, event.order_id),
        );
        chain.strategy_id.clone()
    };
    state
        .orders
        .index_order(&event.account_id, &event.order_id, &event.correlation_id);
    if let Some(strategy_id) = strategy_id {
        let strategy = state.strategies.get_or_insert(&strategy_id);
        strategy.orders += 1;
        strategy.last_order_sequence = Some(sequence);
    }
    state.account.account_id = event.account_id;
    state.account.broker = event.broker;
    state.account.broker_connected = true;
}

fn reduce_broker_ack(
    state: &mut AppState,
    sequence: u64,
    publish_ts_ns: i64,
    event: BrokerAckReceived,
) {
    let chain = state.orders.get_or_insert_chain(&event.correlation_id);
    chain.account_id = Some(event.account_id.clone());
    chain.order_id = Some(event.order_id.clone());
    chain.broker_order_id = Some(event.broker_order_id.clone());
    chain.broker_status = Some(event.broker_status.clone());
    chain.state = OrderLifecycleState::BrokerAckReceived;
    chain.push_timeline(
        sequence,
        publish_ts_ns,
        "BROKER_ACK",
        format!(
            "broker_order_id={} status={}",
            event.broker_order_id, event.broker_status
        ),
    );
    state
        .orders
        .index_order(&event.account_id, &event.order_id, &event.correlation_id);
}

fn reduce_order_fill(
    state: &mut AppState,
    sequence: u64,
    publish_ts_ns: i64,
    event: OrderFill,
    terminal_fill: bool,
) {
    let chain = state.orders.get_or_insert_chain(&event.correlation_id);
    chain.account_id = Some(event.account_id.clone());
    chain.order_id = Some(event.order_id.clone());
    chain.apply_fill(event.filled_quantity, event.fill_price);
    chain.state = if terminal_fill {
        OrderLifecycleState::Filled
    } else {
        OrderLifecycleState::PartiallyFilled
    };
    chain.push_timeline(
        sequence,
        publish_ts_ns,
        if terminal_fill {
            "FILL"
        } else {
            "PARTIAL_FILL"
        },
        format!("{} @ {:.4}", event.filled_quantity, event.fill_price),
    );
    state
        .orders
        .index_order(&event.account_id, &event.order_id, &event.correlation_id);
}

fn reduce_cancel_requested(
    state: &mut AppState,
    sequence: u64,
    publish_ts_ns: i64,
    event: CancelRequested,
) {
    let chain = state.orders.get_or_insert_chain(&event.correlation_id);
    chain.account_id = Some(event.account_id.clone());
    chain.order_id = Some(event.order_id.clone());
    chain.state = OrderLifecycleState::CancelRequested;
    chain.push_timeline(sequence, publish_ts_ns, "CANCEL_REQ", event.reason);
    state
        .orders
        .index_order(&event.account_id, &event.order_id, &event.correlation_id);
}

fn reduce_cancel_rejected(
    state: &mut AppState,
    sequence: u64,
    publish_ts_ns: i64,
    event: CancelRejected,
) {
    let chain = state.orders.get_or_insert_chain(&event.correlation_id);
    chain.account_id = Some(event.account_id.clone());
    chain.order_id = Some(event.order_id.clone());
    chain.state = OrderLifecycleState::CancelRejected;
    chain.push_timeline(sequence, publish_ts_ns, "CANCEL_REJECT", event.reason);
    state
        .orders
        .index_order(&event.account_id, &event.order_id, &event.correlation_id);
}

fn reduce_order_cancelled(
    state: &mut AppState,
    sequence: u64,
    publish_ts_ns: i64,
    event: OrderCancelled,
) {
    let chain = state.orders.get_or_insert_chain(&event.correlation_id);
    chain.account_id = Some(event.account_id.clone());
    chain.order_id = Some(event.order_id.clone());
    chain.state = OrderLifecycleState::Cancelled;
    chain.push_timeline(
        sequence,
        publish_ts_ns,
        "CANCELLED",
        format!("order_id={}", event.order_id),
    );
    state
        .orders
        .index_order(&event.account_id, &event.order_id, &event.correlation_id);
}

fn reduce_order_rejected(
    state: &mut AppState,
    sequence: u64,
    publish_ts_ns: i64,
    event: OrderRejected,
) {
    let chain = state.orders.get_or_insert_chain(&event.correlation_id);
    chain.account_id = Some(event.account_id.clone());
    chain.order_id = Some(event.order_id.clone());
    chain.state = OrderLifecycleState::BrokerRejected;
    chain.push_timeline(sequence, publish_ts_ns, "REJECTED", event.reason);
    state
        .orders
        .index_order(&event.account_id, &event.order_id, &event.correlation_id);
}

fn reduce_position_snapshot(state: &mut AppState, event: PositionSnapshot) {
    let key = format!("{}:{}", event.account_id, event.symbol);
    let unrealized_pnl = (event.market_price - event.average_price) * event.net_quantity as f64;
    state.positions.upsert(PositionView {
        key,
        account_id: event.account_id.clone(),
        symbol: event.symbol,
        net_quantity: event.net_quantity,
        average_price: event.average_price,
        market_price: event.market_price,
        unrealized_pnl,
        strategy_attribution: event
            .strategy_attribution
            .into_iter()
            .map(|item| StrategyPositionView {
                strategy_id: item.strategy_id,
                quantity: item.quantity,
            })
            .collect(),
    });
    state.account.account_id = event.account_id;
    state.account.unrealized_pnl = state
        .positions
        .by_key
        .values()
        .map(|position| position.unrealized_pnl)
        .sum();
    state.account.day_pnl = state.account.realized_pnl + state.account.unrealized_pnl;
}

fn reduce_risk_limit_breached(state: &mut AppState, event: RiskLimitBreached) {
    state.risk.global_state = "BLOCKED".to_string();
    state.risk.active_blocks.push(RiskBlock {
        scope: event.scope,
        severity: event.severity,
        message: event.message,
    });
}

fn reduce_alert_raised(state: &mut AppState, event: AlertRaised) {
    state.alerts.by_id.insert(
        event.alert_id.clone(),
        AlertView {
            alert_id: event.alert_id,
            severity: event.severity,
            domain: event.domain,
            message: event.message,
            acknowledged: false,
            acknowledged_by: None,
            acknowledge_reason: None,
        },
    );
}

fn reduce_alert_acknowledged(state: &mut AppState, event: AlertAcknowledged) {
    if let Some(alert) = state.alerts.by_id.get_mut(&event.alert_id) {
        alert.acknowledged = true;
        alert.acknowledged_by = Some(event.operator_id);
        alert.acknowledge_reason = Some(event.reason);
    }
}

fn reduce_command_audit_recorded(state: &mut AppState, event: CommandAuditRecorded) {
    if event.command_type == "GlobalKillSwitchRequested" && event.status == "accepted" {
        state.risk.kill_switch_active = true;
        state.risk.global_state = "KILL_SWITCH".to_string();
    }
}

fn summarize(envelope: &EventEnvelope) -> EventSummary {
    EventSummary {
        sequence: envelope.sequence,
        ts_ns: envelope.publish_ts_ns,
        event_type: envelope.event_type.clone(),
        aggregate_type: envelope.aggregate_type.clone(),
        aggregate_id: envelope.aggregate_id.clone(),
        correlation_id: envelope.correlation_id.clone(),
        producer: envelope.producer.clone(),
        headline: headline(&envelope.payload),
    }
}

fn headline(event: &DomainEvent) -> String {
    match event {
        DomainEvent::StrategyHeartbeat(event) => {
            format!(
                "{} {} lag={}ms",
                event.strategy_id, event.state, event.heartbeat_lag_ms
            )
        }
        DomainEvent::StrategyStateChanged(event) => {
            format!(
                "{} -> {} ({})",
                event.strategy_id, event.state, event.reason
            )
        }
        DomainEvent::SignalGenerated(event) => {
            let score = event
                .score
                .map(|score| format!(" score={score:.4}"))
                .unwrap_or_default();
            format!(
                "{} {} {}{}",
                event.strategy_id, event.symbol, event.signal_name, score
            )
        }
        DomainEvent::IntentCreated(event) => format!(
            "{} {} {} {}",
            event.strategy_id, event.side, event.quantity, event.symbol
        ),
        DomainEvent::RiskDecisionMade(event) => {
            let status = if event.approved { "PASS" } else { "REJECT" };
            format!("{} {} {}", status, event.strategy_id, event.symbol)
        }
        DomainEvent::OrderSubmitRequested(event) => {
            format!("submit {} {}", event.order_type, event.order_id)
        }
        DomainEvent::OrderSubmitted(event) => {
            format!("submitted {} {}", event.broker, event.order_id)
        }
        DomainEvent::BrokerAckReceived(event) => {
            format!("ack {} {}", event.broker_order_id, event.broker_status)
        }
        DomainEvent::OrderPartiallyFilled(event) => {
            format!(
                "partial {} @ {:.4}",
                event.filled_quantity, event.fill_price
            )
        }
        DomainEvent::OrderFilled(event) => {
            format!("fill {} @ {:.4}", event.filled_quantity, event.fill_price)
        }
        DomainEvent::CancelRequested(event) => format!("cancel requested {}", event.order_id),
        DomainEvent::CancelRejected(event) => format!("cancel rejected {}", event.reason),
        DomainEvent::OrderCancelled(event) => format!("cancelled {}", event.order_id),
        DomainEvent::OrderRejected(event) => format!("rejected {}", event.reason),
        DomainEvent::PositionSnapshot(event) => {
            format!("position {} qty={}", event.symbol, event.net_quantity)
        }
        DomainEvent::RiskLimitBreached(event) => {
            format!("risk {} {}", event.scope, event.message)
        }
        DomainEvent::AlertRaised(event) => {
            format!("{} {} {}", event.severity, event.domain, event.message)
        }
        DomainEvent::AlertAcknowledged(event) => {
            format!("ack {} by {}", event.alert_id, event.operator_id)
        }
        DomainEvent::CommandAuditRecorded(event) => {
            format!("command {} {}", event.command_type, event.status)
        }
    }
}
