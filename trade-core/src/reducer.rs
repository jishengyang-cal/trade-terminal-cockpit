use crate::events::{
    AccountSnapshot, AlertAcknowledged, AlertRaised, BrokerAckReceived, CancelRejected,
    CancelRequested, CommandAuditRecorded, CommandAuthorityDecided, DomainEvent, EventEnvelope,
    IngestDiagnosticRecorded, IntentCreated, OrderCancelled, OrderFill, OrderRejected,
    OrderSubmitRequested, OrderSubmitted, PositionSnapshot, RiskDecisionMade, RiskLimitBreached,
    SignalGenerated, StrategyHealthUpdated, StrategyHeartbeat, StrategyStateChanged,
};
use crate::state::{
    AlertView, AppState, EventSummary, OrderLifecycleState, PositionView, RiskBlock,
    RiskDecisionView, RiskLimitView, StrategyPositionView,
};

pub fn reduce_event(state: &mut AppState, envelope: EventEnvelope) {
    if !state.seen_event_ids.insert(envelope.event_id.clone()) {
        state.connection.duplicate_events += 1;
        return;
    }

    if let Some(previous_sequence) = state
        .last_sequence_by_producer
        .insert(envelope.producer.clone(), envelope.sequence)
    {
        if envelope.sequence <= previous_sequence {
            state.connection.out_of_order_events += 1;
        } else if envelope.sequence > previous_sequence + 1 {
            state.connection.sequence_gaps += envelope.sequence - previous_sequence - 1;
        }
    }

    state.connection.last_event_sequence = Some(envelope.sequence);
    state.connection.last_event_ts_ns = Some(envelope.publish_ts_ns);
    state.connection.last_source_ts_ns = Some(envelope.source_ts_ns);
    state.connection.last_ingest_ts_ns = Some(envelope.ingest_ts_ns);
    state.connection.last_publish_ts_ns = Some(envelope.publish_ts_ns);
    state.connection.source_to_ingest_lag_ms =
        non_negative_delta_ms(envelope.ingest_ts_ns, envelope.source_ts_ns);
    state.connection.ingest_to_render_lag_ms =
        non_negative_delta_ms(crate::unix_ts_ns(), envelope.ingest_ts_ns);
    state.connection.event_lag_ms =
        non_negative_delta_ms(crate::unix_ts_ns(), envelope.publish_ts_ns).unwrap_or_default();
    if !envelope.stream.is_empty() {
        state.connection.stream_name = Some(envelope.stream.clone());
    }
    if !envelope.subject.is_empty() {
        state.connection.last_nats_subject = Some(envelope.subject.clone());
    }
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
    let event_id = envelope.event_id.clone();

    match envelope.payload {
        DomainEvent::AccountSnapshot(event) => {
            reduce_account_snapshot(state, event);
        }
        DomainEvent::StrategyHeartbeat(event) => {
            reduce_strategy_heartbeat(state, sequence, event);
        }
        DomainEvent::StrategyHealthUpdated(event) => {
            reduce_strategy_health_updated(state, event);
        }
        DomainEvent::StrategyStateChanged(event) => {
            reduce_strategy_state_changed(state, sequence, event);
        }
        DomainEvent::SignalGenerated(event) => {
            reduce_signal_generated(state, sequence, publish_ts_ns, &event_id, event);
        }
        DomainEvent::IntentCreated(event) => {
            reduce_intent_created(state, sequence, publish_ts_ns, &event_id, event);
        }
        DomainEvent::RiskDecisionMade(event) => {
            reduce_risk_decision(state, sequence, publish_ts_ns, &event_id, event);
        }
        DomainEvent::OrderSubmitRequested(event) => {
            reduce_order_submit_requested(state, sequence, publish_ts_ns, &event_id, event);
        }
        DomainEvent::OrderSubmitted(event) => {
            reduce_order_submitted(state, sequence, publish_ts_ns, &event_id, event);
        }
        DomainEvent::BrokerAckReceived(event) => {
            reduce_broker_ack(state, sequence, publish_ts_ns, &event_id, event);
        }
        DomainEvent::OrderPartiallyFilled(event) => {
            reduce_order_fill(state, sequence, publish_ts_ns, &event_id, event, false);
        }
        DomainEvent::OrderFilled(event) => {
            reduce_order_fill(state, sequence, publish_ts_ns, &event_id, event, true);
        }
        DomainEvent::CancelRequested(event) => {
            reduce_cancel_requested(state, sequence, publish_ts_ns, &event_id, event);
        }
        DomainEvent::CancelRejected(event) => {
            reduce_cancel_rejected(state, sequence, publish_ts_ns, &event_id, event);
        }
        DomainEvent::OrderCancelled(event) => {
            reduce_order_cancelled(state, sequence, publish_ts_ns, &event_id, event);
        }
        DomainEvent::OrderRejected(event) => {
            reduce_order_rejected(state, sequence, publish_ts_ns, &event_id, event);
        }
        DomainEvent::PositionSnapshot(event) => reduce_position_snapshot(state, event),
        DomainEvent::RiskLimitBreached(event) => {
            reduce_risk_limit_breached(state, publish_ts_ns, event)
        }
        DomainEvent::AlertRaised(event) => reduce_alert_raised(state, event),
        DomainEvent::AlertAcknowledged(event) => reduce_alert_acknowledged(state, event),
        DomainEvent::IngestDiagnosticRecorded(event) => {
            reduce_ingest_diagnostic(state, publish_ts_ns, event)
        }
        DomainEvent::CommandAuthorityDecided(event) => {
            reduce_command_authority_decided(state, event)
        }
        DomainEvent::CommandAuditRecorded(event) => reduce_command_audit_recorded(state, event),
    }
}

fn is_coalescible_projection_event(event: &DomainEvent) -> bool {
    matches!(
        event,
        DomainEvent::AccountSnapshot(_)
            | DomainEvent::StrategyHeartbeat(_)
            | DomainEvent::StrategyHealthUpdated(_)
            | DomainEvent::PositionSnapshot(_)
            | DomainEvent::IngestDiagnosticRecorded(_)
    )
}

fn reduce_account_snapshot(state: &mut AppState, event: AccountSnapshot) {
    let account = state.accounts.get_or_insert(&event.account_id);
    if let Some(mode) = event.mode {
        account.mode = mode;
    }
    if let Some(broker) = event.broker {
        account.broker = broker;
    }
    if let Some(connected) = event.broker_connected {
        account.broker_connected = connected;
    }
    if let Some(currency) = event.account_currency {
        account.account_currency = currency;
    }
    if let Some(value) = event.cash {
        account.cash_value = value;
    }
    if let Some(value) = event.buying_power {
        account.buying_power_value = value;
    }
    if let Some(value) = event.day_pnl {
        account.day_pnl_value = value;
    }
    if let Some(value) = event.realized_pnl {
        account.realized_pnl_value = value;
    }
    if let Some(value) = event.unrealized_pnl {
        account.unrealized_pnl_value = value;
    }
    if let Some(value) = event.net_liquidation {
        account.net_liquidation = value;
    }
    if let Some(value) = event.equity_with_loan {
        account.equity_with_loan = value;
    }
    if let Some(value) = event.initial_margin {
        account.initial_margin = value;
    }
    if let Some(value) = event.maintenance_margin {
        account.maintenance_margin = value;
    }
    if let Some(value) = event.excess_liquidity {
        account.excess_liquidity = value;
    }
    if let Some(value) = event.available_funds {
        account.available_funds = value;
    }
    if event.sma.is_some() {
        account.sma = event.sma;
    }
    if let Some(value) = event.day_trades_remaining {
        account.day_trades_remaining = Some(value);
    }
    if let Some(value) = event.pdt_status {
        account.pdt_status = Some(value);
    }
    if let Some(value) = event.trading_restriction {
        account.trading_restriction = Some(value);
    }
    if event.settled_cash.is_some() {
        account.settled_cash = event.settled_cash;
    }
    if event.unsettled_cash.is_some() {
        account.unsettled_cash = event.unsettled_cash;
    }
    if let Some(value) = event.gross_exposure {
        account.gross_exposure = value;
    }
    if let Some(value) = event.net_exposure {
        account.net_exposure = value;
    }
    if let Some(value) = event.long_market_value {
        account.long_market_value = value;
    }
    if let Some(value) = event.short_market_value {
        account.short_market_value = value;
    }
    if let Some(value) = event.exposure_pct {
        account.exposure_pct = value;
    }
    if let Some(value) = event.margin_usage_pct {
        account.margin_usage_pct = value;
    }
    if let Some(value) = event.short_permission {
        account.short_permission = value;
    }
    if let Some(value) = event.short_intents_blocked_today {
        account.short_intents_blocked_today = value;
    }
    account.sync_legacy_f64_from_money();
    refresh_account_aggregate(state);
}

fn reduce_strategy_heartbeat(state: &mut AppState, sequence: u64, event: StrategyHeartbeat) {
    let strategy = state.strategies.get_or_insert(&event.strategy_id);
    strategy.state = event.state;
    strategy.mode = event.mode;
    strategy.heartbeat_lag_ms = Some(event.heartbeat_lag_ms);
    strategy.last_event_sequence = Some(sequence);
}

fn reduce_strategy_health_updated(state: &mut AppState, event: StrategyHealthUpdated) {
    let strategy = state.strategies.get_or_insert(&event.strategy_id);
    if let Some(value) = event.enabled {
        strategy.enabled = value;
    }
    if let Some(value) = event.trading_window {
        strategy.trading_window = Some(value);
    }
    if let Some(value) = event.current_phase {
        strategy.current_phase = value;
    }
    if let Some(value) = event.universe_version {
        strategy.universe_version = Some(value);
    }
    if let Some(value) = event.universe_count {
        strategy.universe_count = value;
    }
    if let Some(value) = event.active_symbol_count {
        strategy.active_symbol_count = value;
    }
    if let Some(value) = event.watched_symbol_count {
        strategy.watched_symbol_count = value;
    }
    if let Some(value) = event.l2_allocated_symbol_count {
        strategy.l2_allocated_symbol_count = value;
    }
    if let Some(value) = event.signal_rate_1m {
        strategy.signal_rate_1m = value;
    }
    if let Some(value) = event.reject_rate_1m {
        strategy.reject_rate_1m = value;
    }
    if let Some(value) = event.fill_rate_1m {
        strategy.fill_rate_1m = value;
    }
    if let Some(value) = event.cancel_rate_1m {
        strategy.cancel_rate_1m = value;
    }
    if let Some(value) = event.avg_intent_to_submit_ms {
        strategy.avg_intent_to_submit_ms = Some(value);
    }
    if let Some(value) = event.avg_submit_to_ack_ms {
        strategy.avg_submit_to_ack_ms = Some(value);
    }
    if let Some(value) = event.avg_ack_to_fill_ms {
        strategy.avg_ack_to_fill_ms = Some(value);
    }
    if let Some(value) = event.consecutive_stops {
        strategy.consecutive_stops = value;
    }
    if let Some(value) = event.trades_today {
        strategy.trades_today = value;
    }
    if let Some(value) = event.max_trades_today {
        strategy.max_trades_today = value;
    }
    if let Some(value) = event.daily_loss_used_pct {
        strategy.daily_loss_used_pct = value;
    }
    if !event.parameters.is_empty() {
        strategy.parameters = event.parameters;
    }
    if !event.risk_gates.is_empty() {
        strategy.risk_gates = event
            .risk_gates
            .into_iter()
            .map(|gate| crate::state::StrategyRiskGateView {
                name: gate.name,
                passed: gate.passed,
                detail: gate.detail,
            })
            .collect();
    }
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
    event_id: &str,
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
    chain.arrival_price = event.reference_price.or(event.microprice);
    chain.transition_state(OrderLifecycleState::SignalGenerated, event_id, sequence);
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
    event_id: &str,
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
    if let Some(account_id) = event.account_id {
        chain.account_id = Some(account_id);
    }
    chain.notional = event.notional;
    chain.decision_price = event.limit_price_hint.clone();
    chain.limit_price = event.limit_price_hint;
    chain.stop_price = event.stop_price_hint;
    if let Some(tif) = event.time_in_force_hint {
        chain.tif = Some(tif);
    }
    if let Some(currency) = event.currency {
        chain.currency = currency;
    }
    chain.transition_state(OrderLifecycleState::IntentCreated, event_id, sequence);
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
    event_id: &str,
    event: RiskDecisionMade,
) {
    let chain = state.orders.get_or_insert_chain(&event.correlation_id);
    chain.strategy_id.get_or_insert(event.strategy_id.clone());
    chain.symbol.get_or_insert(event.symbol.clone());
    chain.risk = Some(RiskDecisionView {
        approved: event.approved,
        reason_codes: event.reason_codes.clone(),
        severity: event.severity.clone(),
        decision_id: event.decision_id.clone(),
        risk_snapshot_id: event.risk_snapshot_id.clone(),
        evaluated_rules: event.evaluated_rules.clone(),
        authority_policy_version: event.authority_policy_version.clone(),
    });
    let next_state = if event.approved {
        OrderLifecycleState::RiskApproved
    } else {
        OrderLifecycleState::RiskRejected
    };
    chain.transition_state(next_state, event_id, sequence);
    let status = if event.approved { "PASS" } else { "REJECT" };
    chain.push_timeline(
        sequence,
        publish_ts_ns,
        "RISK",
        format!("{status} {}", event.reason_codes.join(",")),
    );
    let chain_account_id = chain.account_id.clone();

    for rule in &event.evaluated_rules {
        state.risk.structured_limits.push(RiskLimitView {
            rule_id: rule.rule_id.clone(),
            scope: format!("{}/{}", event.strategy_id, event.symbol),
            metric: rule.rule_name.clone(),
            observed: rule.observed.clone(),
            limit: rule.threshold.clone(),
            unit: rule.unit.clone(),
            status: if rule.passed { "ok" } else { "block" }.to_string(),
            updated_ts_ns: publish_ts_ns,
        });
    }

    if !event.approved {
        let message = if event.reason_codes.is_empty() {
            event
                .evaluated_rules
                .iter()
                .filter(|rule| !rule.passed)
                .map(|rule| rule.rule_id.as_str())
                .collect::<Vec<_>>()
                .join(",")
        } else {
            event.reason_codes.join(",")
        };
        upsert_risk_block(
            state,
            RiskBlock {
                block_id: event.decision_id.clone().unwrap_or_else(|| {
                    format!("risk_decision:{}:{}", event.strategy_id, event.symbol)
                }),
                rule_id: event
                    .evaluated_rules
                    .iter()
                    .find(|rule| !rule.passed)
                    .map(|rule| rule.rule_id.clone())
                    .unwrap_or_default(),
                scope: format!("{}/{}", event.strategy_id, event.symbol),
                severity: event.severity.unwrap_or_else(|| "block".to_string()),
                message,
                first_seen_ts_ns: publish_ts_ns,
                last_seen_ts_ns: publish_ts_ns,
                cleared_ts_ns: None,
                correlation_id: Some(event.correlation_id),
                symbol: Some(event.symbol),
                strategy_id: Some(event.strategy_id),
            },
        );
        if event
            .reason_codes
            .iter()
            .any(|reason| reason.contains("short_permission"))
        {
            if let Some(account_id) = chain_account_id {
                state
                    .accounts
                    .get_or_insert(&account_id)
                    .short_intents_blocked_today += 1;
                refresh_account_aggregate(state);
            } else {
                state.account.short_intents_blocked_today += 1;
            }
        }
    }
}

fn reduce_order_submit_requested(
    state: &mut AppState,
    sequence: u64,
    publish_ts_ns: i64,
    event_id: &str,
    event: OrderSubmitRequested,
) {
    let chain = state.orders.get_or_insert_chain(&event.correlation_id);
    chain.account_id = Some(event.account_id.clone());
    chain.order_id = Some(event.order_id.clone());
    chain.client_order_id = event.client_order_id.clone();
    chain.broker_order_id = event.broker_order_id.clone();
    chain.perm_id = event.perm_id.clone();
    chain.parent_order_id = event.parent_order_id.clone();
    chain.order_type = Some(event.order_type.clone());
    chain.limit_price = event.limit_price.clone();
    chain.stop_price = event.stop_price.clone();
    chain.tif = Some(event.tif.clone());
    chain.route = event.route.clone();
    chain.exchange = event.exchange.clone();
    chain.destination = event.destination.clone();
    chain.submitted_quantity = event.quantity;
    chain.remaining_quantity = event.remaining_quantity.or(event.quantity);
    chain.submit_ts_ns = Some(publish_ts_ns);
    chain.transition_state(OrderLifecycleState::SubmitRequested, event_id, sequence);
    chain.push_timeline(
        sequence,
        publish_ts_ns,
        "SUBMIT_REQ",
        format!(
            "{} {} tif={}",
            event.order_type,
            event
                .limit_price
                .as_ref()
                .map(|price| price.to_string())
                .unwrap_or_else(|| "MKT".to_string()),
            event.tif
        ),
    );
    state
        .orders
        .index_order(&event.account_id, &event.order_id, &event.correlation_id);
    touch_account(state, &event.account_id);
}

fn reduce_order_submitted(
    state: &mut AppState,
    sequence: u64,
    publish_ts_ns: i64,
    event_id: &str,
    event: OrderSubmitted,
) {
    let strategy_id = {
        let chain = state.orders.get_or_insert_chain(&event.correlation_id);
        chain.account_id = Some(event.account_id.clone());
        chain.order_id = Some(event.order_id.clone());
        chain.broker = Some(event.broker.clone());
        chain.client_order_id = event.client_order_id.clone();
        if let Some(broker_order_id) = event.broker_order_id.clone() {
            chain.broker_order_id = Some(broker_order_id);
        }
        if let Some(perm_id) = event.perm_id.clone() {
            chain.perm_id = Some(perm_id);
        }
        chain.route = event.route.clone().or(chain.route.clone());
        chain.exchange = event.exchange.clone().or(chain.exchange.clone());
        chain.destination = event.destination.clone().or(chain.destination.clone());
        chain.transition_state(OrderLifecycleState::SubmittedToBroker, event_id, sequence);
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
    {
        let account = state.accounts.get_or_insert(&event.account_id);
        account.broker = event.broker;
        account.broker_connected = true;
    }
    refresh_account_aggregate(state);
}

fn reduce_broker_ack(
    state: &mut AppState,
    sequence: u64,
    publish_ts_ns: i64,
    event_id: &str,
    event: BrokerAckReceived,
) {
    let chain = state.orders.get_or_insert_chain(&event.correlation_id);
    chain.account_id = Some(event.account_id.clone());
    chain.order_id = Some(event.order_id.clone());
    chain.broker_order_id = Some(event.broker_order_id.clone());
    if let Some(perm_id) = event.perm_id.clone() {
        chain.perm_id = Some(perm_id);
    }
    chain.broker_status = Some(event.broker_status.clone());
    chain.remaining_quantity = event.remaining_quantity;
    chain.ack_ts_ns = Some(publish_ts_ns);
    if let Some(submit_ts_ns) = chain.submit_ts_ns {
        chain.latency.submit_to_ack_ms = non_negative_delta_ms(publish_ts_ns, submit_ts_ns);
    }
    chain.transition_state(OrderLifecycleState::BrokerAckReceived, event_id, sequence);
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
    event_id: &str,
    event: OrderFill,
    terminal_fill: bool,
) {
    let chain = state.orders.get_or_insert_chain(&event.correlation_id);
    chain.account_id = Some(event.account_id.clone());
    chain.order_id = Some(event.order_id.clone());
    let last_price = event
        .last_price
        .clone()
        .unwrap_or_else(|| event.fill_price.clone());
    let last_quantity = event.last_quantity.unwrap_or(event.filled_quantity);
    let applied = chain.apply_fill(
        event.execution_id.as_deref(),
        last_quantity,
        event.cumulative_quantity,
        event.remaining_quantity,
        last_price.clone(),
    );
    if let Some(average_price) = event.average_price {
        chain.average_fill_price = Some(average_price);
    }
    if let Some(commission) = event.commission {
        chain.commission = Some(commission);
    }
    if applied {
        chain.first_fill_ts_ns.get_or_insert(publish_ts_ns);
        chain.last_fill_ts_ns = Some(publish_ts_ns);
        if let Some(ack_ts_ns) = chain.ack_ts_ns {
            chain.latency.ack_to_first_fill_ms = chain
                .latency
                .ack_to_first_fill_ms
                .or_else(|| non_negative_delta_ms(publish_ts_ns, ack_ts_ns));
        }
    }
    let next_state = if terminal_fill {
        OrderLifecycleState::Filled
    } else {
        OrderLifecycleState::PartiallyFilled
    };
    if terminal_fill {
        chain.terminal_ts_ns = Some(publish_ts_ns);
        if let Some(submit_ts_ns) = chain.submit_ts_ns {
            chain.latency.submit_to_terminal_ms =
                non_negative_delta_ms(publish_ts_ns, submit_ts_ns);
        }
    }
    chain.transition_state(next_state, event_id, sequence);
    chain.push_timeline(
        sequence,
        publish_ts_ns,
        if terminal_fill {
            "FILL"
        } else {
            "PARTIAL_FILL"
        },
        format!("{} @ {}", last_quantity, last_price),
    );
    state
        .orders
        .index_order(&event.account_id, &event.order_id, &event.correlation_id);
}

fn reduce_cancel_requested(
    state: &mut AppState,
    sequence: u64,
    publish_ts_ns: i64,
    event_id: &str,
    event: CancelRequested,
) {
    let chain = state.orders.get_or_insert_chain(&event.correlation_id);
    chain.account_id = Some(event.account_id.clone());
    chain.order_id = Some(event.order_id.clone());
    chain.transition_state(OrderLifecycleState::CancelRequested, event_id, sequence);
    chain.push_timeline(sequence, publish_ts_ns, "CANCEL_REQ", event.reason);
    state
        .orders
        .index_order(&event.account_id, &event.order_id, &event.correlation_id);
}

fn reduce_cancel_rejected(
    state: &mut AppState,
    sequence: u64,
    publish_ts_ns: i64,
    event_id: &str,
    event: CancelRejected,
) {
    let chain = state.orders.get_or_insert_chain(&event.correlation_id);
    chain.account_id = Some(event.account_id.clone());
    chain.order_id = Some(event.order_id.clone());
    chain.transition_state(OrderLifecycleState::CancelRejected, event_id, sequence);
    chain.push_timeline(sequence, publish_ts_ns, "CANCEL_REJECT", event.reason);
    state
        .orders
        .index_order(&event.account_id, &event.order_id, &event.correlation_id);
}

fn reduce_order_cancelled(
    state: &mut AppState,
    sequence: u64,
    publish_ts_ns: i64,
    event_id: &str,
    event: OrderCancelled,
) {
    let chain = state.orders.get_or_insert_chain(&event.correlation_id);
    chain.account_id = Some(event.account_id.clone());
    chain.order_id = Some(event.order_id.clone());
    chain.terminal_ts_ns = Some(publish_ts_ns);
    chain.transition_state(OrderLifecycleState::Cancelled, event_id, sequence);
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
    event_id: &str,
    event: OrderRejected,
) {
    let chain = state.orders.get_or_insert_chain(&event.correlation_id);
    chain.account_id = Some(event.account_id.clone());
    chain.order_id = Some(event.order_id.clone());
    chain.terminal_ts_ns = Some(publish_ts_ns);
    chain.transition_state(OrderLifecycleState::BrokerRejected, event_id, sequence);
    chain.push_timeline(sequence, publish_ts_ns, "REJECTED", event.reason);
    state
        .orders
        .index_order(&event.account_id, &event.order_id, &event.correlation_id);
}

fn reduce_position_snapshot(state: &mut AppState, event: PositionSnapshot) {
    let account_id = event.account_id.clone();
    let key = format!("{}:{}", event.account_id, event.symbol);
    let unrealized_pnl =
        (event.market_price.as_f64() - event.average_price.as_f64()) * event.net_quantity as f64;
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
    recalculate_account_position_pnl(state, &account_id);
}

fn reduce_risk_limit_breached(state: &mut AppState, publish_ts_ns: i64, event: RiskLimitBreached) {
    state.risk.global_state = "BLOCKED".to_string();
    upsert_risk_block(
        state,
        RiskBlock {
            block_id: event
                .block_id
                .clone()
                .unwrap_or_else(|| format!("risk_limit:{}", event.scope)),
            rule_id: event.rule_id.unwrap_or_default(),
            scope: event.scope,
            severity: event.severity,
            message: event.message,
            first_seen_ts_ns: event.first_seen_ts_ns.unwrap_or(publish_ts_ns),
            last_seen_ts_ns: event.last_seen_ts_ns.unwrap_or(publish_ts_ns),
            cleared_ts_ns: event.cleared_ts_ns,
            correlation_id: event.correlation_id,
            symbol: event.symbol,
            strategy_id: event.strategy_id,
        },
    );
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

fn reduce_ingest_diagnostic(
    state: &mut AppState,
    publish_ts_ns: i64,
    event: IngestDiagnosticRecorded,
) {
    if let Some(stream) = event.stream {
        state.connection.stream_name = Some(stream);
    }
    if let Some(consumer) = event.consumer {
        state.connection.consumer_name = Some(consumer);
    }
    if let Some(subject) = event.subject {
        state.connection.last_nats_subject = Some(subject);
    }
    state.connection.filtered_events += event.filtered_count;
    state.connection.jetstream_acks += event.acked_count;
    if event.reconnect {
        state.connection.nats_reconnect_count += 1;
        state.connection.last_reconnect_ts_ns = Some(publish_ts_ns);
    }
    if event.decode_error {
        state.connection.decode_errors += 1;
    }
    if matches!(event.severity.as_str(), "warn" | "error") {
        state.connection.ingest_errors += 1;
        state.connection.last_error = Some(event.message);
    }
    if event.source.contains("nats") || event.source.contains("jetstream") {
        state.connection.nats = if matches!(event.severity.as_str(), "error") {
            "degraded".to_string()
        } else {
            "connected".to_string()
        };
    }
}

fn reduce_command_authority_decided(state: &mut AppState, event: CommandAuthorityDecided) {
    state.connection.command_gateway = event.status.clone();
    let command = state.commands.get_or_insert(&event.command_id);
    command.operator_id = Some(event.operator_id);
    command.command_type = Some(event.command_type);
    command.authority_decision_id = Some(event.decision_id);
    command.authority_status = Some(event.status);
    command.reason_codes = event.reason_codes;
    command.matched_policy_ids = event.matched_policy_ids;
    command.capability = Some(event.capability);
    command.scope = Some(event.scope);
    command.approved_by = event.approved_by;
    command.decided_ts_ns = Some(event.decided_ts_ns);
    command.authority_policy_version = Some(event.authority_policy_version);
    command.target_environment = Some(event.target_environment);
}

fn reduce_command_audit_recorded(state: &mut AppState, event: CommandAuditRecorded) {
    state.connection.command_gateway = event.status.clone();
    let command = state.commands.get_or_insert(&event.command_id);
    command.operator_id = Some(event.operator_id.clone());
    command.command_type = Some(event.command_type.clone());
    command.audit_status = Some(event.status.clone());
    command.audit_reason = Some(event.reason.clone());
    command.target = event.target.clone();
    command.aggregate_id = event.target.clone();

    if event.command_type == "GlobalKillSwitchRequested" && command_was_applied(&event.status) {
        state.risk.kill_switch_active = true;
        state.risk.global_state = "KILL_SWITCH".to_string();
    }

    if !command_was_applied(&event.status) {
        return;
    }

    match event.command_type.as_str() {
        "AccountKillSwitchRequested" | "CancelAllOrdersForAccountRequested" => {
            if let Some(account_id) = event.target.as_deref() {
                state
                    .accounts
                    .get_or_insert(account_id)
                    .runtime_controls
                    .cancel_all = true;
                refresh_account_aggregate(state);
            }
        }
        "FlattenAccountRequested" => {
            if let Some(account_id) = event.target.as_deref() {
                state
                    .accounts
                    .get_or_insert(account_id)
                    .runtime_controls
                    .flatten_only = true;
                refresh_account_aggregate(state);
            }
        }
        "CancelAllOrdersForSymbolRequested" => {
            if let Some((account_id, symbol)) =
                event.target.as_deref().and_then(split_account_symbol)
            {
                if symbol == "*" {
                    state
                        .accounts
                        .get_or_insert(account_id)
                        .runtime_controls
                        .cancel_all = true;
                    refresh_account_aggregate(state);
                }
            }
        }
        "FlattenSymbolRequested" => {
            if let Some((account_id, symbol)) =
                event.target.as_deref().and_then(split_account_symbol)
            {
                if symbol == "*" {
                    state
                        .accounts
                        .get_or_insert(account_id)
                        .runtime_controls
                        .flatten_only = true;
                    refresh_account_aggregate(state);
                }
            }
        }
        _ => {}
    }
}

fn touch_account(state: &mut AppState, account_id: &str) {
    state.accounts.get_or_insert(account_id);
    refresh_account_aggregate(state);
}

fn recalculate_account_position_pnl(state: &mut AppState, account_id: &str) {
    let unrealized_pnl = state
        .positions
        .by_key
        .values()
        .filter(|position| position.account_id == account_id)
        .map(|position| position.unrealized_pnl)
        .sum::<f64>();
    let account = state.accounts.get_or_insert(account_id);
    account.unrealized_pnl = unrealized_pnl;
    account.day_pnl = account.realized_pnl + account.unrealized_pnl;
    refresh_account_aggregate(state);
}

fn refresh_account_aggregate(state: &mut AppState) {
    state.account = state.accounts.aggregate_view();
}

fn command_was_applied(status: &str) -> bool {
    matches!(status, "dispatched" | "applied" | "executed")
}

fn split_account_symbol(value: &str) -> Option<(&str, &str)> {
    value.split_once(':')
}

fn non_negative_delta_ms(later_ns: i64, earlier_ns: i64) -> Option<u64> {
    later_ns
        .checked_sub(earlier_ns)
        .filter(|delta| *delta >= 0)
        .map(|delta| (delta / 1_000_000) as u64)
}

fn upsert_risk_block(state: &mut AppState, mut next: RiskBlock) {
    if next.block_id.is_empty() {
        next.block_id = format!("{}:{}", next.scope, next.rule_id);
    }

    if let Some(existing) = state.risk.active_blocks.iter_mut().find(|existing| {
        (!next.block_id.is_empty() && existing.block_id == next.block_id)
            || (!next.rule_id.is_empty()
                && existing.rule_id == next.rule_id
                && existing.scope == next.scope)
    }) {
        if existing.first_seen_ts_ns == 0 {
            existing.first_seen_ts_ns = next.first_seen_ts_ns;
        }
        existing.last_seen_ts_ns = next.last_seen_ts_ns;
        existing.severity = next.severity;
        existing.message = next.message;
        existing.cleared_ts_ns = next.cleared_ts_ns;
        existing.correlation_id = next.correlation_id;
        existing.symbol = next.symbol;
        existing.strategy_id = next.strategy_id;
        return;
    }

    state.risk.active_blocks.push(next);
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
        payload_json: serde_json::to_string(&envelope.payload).ok(),
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
        DomainEvent::AccountSnapshot(event) => {
            format!(
                "account {} {}",
                event.account_id,
                event.mode.as_deref().unwrap_or("-")
            )
        }
        DomainEvent::StrategyHealthUpdated(event) => {
            format!("strategy health {}", event.strategy_id)
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
            format!("partial {} @ {}", event.filled_quantity, event.fill_price)
        }
        DomainEvent::OrderFilled(event) => {
            format!("fill {} @ {}", event.filled_quantity, event.fill_price)
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
        DomainEvent::IngestDiagnosticRecorded(event) => {
            format!("ingest {} {}", event.severity, event.message)
        }
        DomainEvent::CommandAuthorityDecided(event) => {
            format!(
                "authority {} {} {}",
                event.command_type, event.status, event.capability
            )
        }
        DomainEvent::CommandAuditRecorded(event) => {
            let target = event
                .target
                .as_deref()
                .map(|target| format!(" target={target}"))
                .unwrap_or_default();
            format!("command {} {}{}", event.command_type, event.status, target)
        }
    }
}
