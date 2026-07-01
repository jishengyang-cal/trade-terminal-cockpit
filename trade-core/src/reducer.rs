use crate::events::{
    AccountSnapshot, AlertAcknowledged, AlertRaised, BrokerAckReceived, CancelRejected,
    CancelRequested, CommandAuditRecorded, CommandAuthorityDecided, DomainEvent, EventEnvelope,
    IngestDiagnosticRecorded, IntentCreated, OrderCancelled, OrderFill, OrderRejected,
    OrderSubmitRequested, OrderSubmitted, PositionSnapshot, RiskDecisionMade, RiskLimitBreached,
    SignalGenerated, StrategyHealthUpdated, StrategyHeartbeat, StrategyStateChanged,
};
use crate::state::{
    refresh_account_safety_state, AlertView, AppState, EventSummary, FillExecutionView,
    MarketDataSummaryView, OrderLifecycleState, PositionView, RiskBlock, RiskDecisionView,
    RiskLimitView, StrategyPositionView,
};
use crate::types::Money;
use std::collections::BTreeMap;

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
    let producer = envelope.producer.clone();

    match envelope.payload {
        DomainEvent::AccountSnapshot(event) => {
            reduce_account_snapshot(state, sequence, publish_ts_ns, &event_id, &producer, event);
        }
        DomainEvent::MarketDataSummary(event) => {
            state.market_data.upsert(MarketDataSummaryView {
                symbol: event.symbol,
                source: event.source,
                bid_price: event.bid_price,
                ask_price: event.ask_price,
                spread_bps: event.spread_bps,
                imbalance: event.imbalance,
                microprice: event.microprice,
                quote_age_ms: event.quote_age_ms,
                event_rate_per_sec: event.event_rate_per_sec,
                wall_size: event.wall_size,
                summary_ts_ns: event.summary_ts_ns,
            });
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
            | DomainEvent::MarketDataSummary(_)
            | DomainEvent::StrategyHeartbeat(_)
            | DomainEvent::StrategyHealthUpdated(_)
            | DomainEvent::PositionSnapshot(_)
            | DomainEvent::IngestDiagnosticRecorded(_)
    )
}

fn reduce_account_snapshot(
    state: &mut AppState,
    sequence: u64,
    publish_ts_ns: i64,
    event_id: &str,
    producer: &str,
    event: AccountSnapshot,
) {
    let account = state.accounts.get_or_insert(&event.account_id);
    let source = if producer.is_empty() {
        "account_snapshot"
    } else {
        producer
    };
    if event.canonical_account_id.is_some() {
        account.canonical_account_id = event.canonical_account_id;
    }
    if let Some(value) = event.account_slot {
        account.account_slot = Some(value);
    }
    if event.account_id_hash_hex.is_some() {
        account.account_id_hash_hex = event.account_id_hash_hex;
    }
    if event.endpoint_id.is_some() {
        account.endpoint_id = event.endpoint_id;
    }
    if let Some(value) = event.client_id {
        account.client_id = Some(value);
    }
    if event.gateway_tier.is_some() {
        account.gateway_tier = event.gateway_tier;
    }
    if event.account_role.is_some() {
        account.account_role = event.account_role;
    }
    if let Some(value) = event.role_bits {
        account.role_bits = Some(value);
    }
    if let Some(value) = event.readonly {
        account.readonly = Some(value);
    }
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
        account.cash_source = Some(source.to_string());
    }
    if let Some(value) = event.buying_power {
        account.buying_power_value = value;
        account.buying_power_source = Some(source.to_string());
    }
    if let Some(value) = event.day_pnl {
        account.day_pnl_value = value;
        account.day_pnl_source = Some(source.to_string());
    }
    if let Some(value) = event.realized_pnl {
        account.realized_pnl_value = value;
        account.realized_source = Some(source.to_string());
    }
    if let Some(value) = event.unrealized_pnl {
        account.unrealized_pnl_value = value;
        account.unrealized_source = Some(source.to_string());
    }
    if let Some(value) = event.net_liquidation {
        account.net_liquidation = value;
        account.net_liq_source = Some(source.to_string());
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
        account.available_funds_source = Some(source.to_string());
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
    if let Some(value) = event.margin_account {
        account.margin_account = Some(value);
    }
    if event.account_type.is_some() {
        account.account_type = event.account_type;
    }
    if let Some(value) = event.short_intents_blocked_today {
        account.short_intents_blocked_today = value;
    }
    account.cash_source = event.cash_source.or(account.cash_source.take());
    account.buying_power_source = event
        .buying_power_source
        .or(account.buying_power_source.take());
    account.net_liq_source = event.net_liq_source.or(account.net_liq_source.take());
    account.available_funds_source = event
        .available_funds_source
        .or(account.available_funds_source.take());
    account.day_pnl_source = event.day_pnl_source.or(account.day_pnl_source.take());
    account.realized_source = event.realized_source.or(account.realized_source.take());
    account.unrealized_source = event.unrealized_source.or(account.unrealized_source.take());
    account.refresh_ocam_authority_mapping();
    account.sync_legacy_f64_from_money();
    let snapshot_source = event
        .account_snapshot_source
        .or(event.valuation_source)
        .unwrap_or_else(|| source.to_string());
    let snapshot_id = event
        .account_snapshot_id
        .unwrap_or_else(|| event_id.to_string());
    account.mark_account_snapshot(
        snapshot_id,
        event.account_snapshot_seq.or(Some(sequence)),
        snapshot_source,
        event.account_snapshot_ts_ns.unwrap_or(publish_ts_ns),
    );
    if let Some(age_ms) = event.account_snapshot_age_ms {
        account.account_snapshot_age_ms = Some(age_ms);
    }
    if let Some(stale) = event.valuation_stale {
        account.valuation_stale = stale;
    }
    if let Some(reason) = event.valuation_incomplete_reason {
        account.valuation_incomplete_reason = Some(reason);
    }
    if let Some(status) = event.valuation_status {
        account.valuation_status = status;
        account.valuation_ok = event
            .valuation_ok
            .unwrap_or(account.valuation_status == "COMPLETE");
    } else if let Some(ok) = event.valuation_ok {
        account.valuation_ok = ok;
    }
    let has_explicit_paper_endpoint = state.accounts.by_id.values().any(|account| {
        let gateway_tier_paper = account
            .gateway_tier
            .as_deref()
            .map(|tier| tier.eq_ignore_ascii_case("paper"))
            .unwrap_or(false);
        let endpoint_present = account
            .endpoint_id
            .as_deref()
            .map(|endpoint_id| !endpoint_id.is_empty())
            .unwrap_or(false);
        let broker_is_ibkr = account.broker.eq_ignore_ascii_case("ibkr_tws");

        broker_is_ibkr && endpoint_present && gateway_tier_paper
    });
    if has_explicit_paper_endpoint {
        state.risk.broker_order_channel_ok = state.accounts.by_id.values().any(|account| {
            let gateway_tier_paper = account
                .gateway_tier
                .as_deref()
                .map(|tier| tier.eq_ignore_ascii_case("paper"))
                .unwrap_or(false);
            let snapshot_fresh = account
                .account_snapshot_age_ms
                .map(|age_ms| age_ms <= 120_000)
                .unwrap_or(false);
            let endpoint_present = account
                .endpoint_id
                .as_deref()
                .map(|endpoint_id| !endpoint_id.is_empty())
                .unwrap_or(false);
            let broker_is_ibkr = account.broker.eq_ignore_ascii_case("ibkr_tws");
            let readonly = account.readonly.unwrap_or(true);

            account.broker_connected
                && broker_is_ibkr
                && endpoint_present
                && gateway_tier_paper
                && snapshot_fresh
                && !readonly
        });
    }
    refresh_account_safety_state(state, publish_ts_ns);
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
    if let Some(value) = event.signals_total_today {
        strategy.signals_total_today = value;
    }
    if let Some(value) = event.signals_last_1m {
        strategy.signals_last_1m = value;
    }
    if let Some(value) = event.intents_total_today {
        strategy.intents_total_today = value;
    }
    if let Some(value) = event.orders_total_today {
        strategy.orders_total_today = value;
    }
    if let Some(value) = event.fills_total_today {
        strategy.fills_total_today = value;
    }
    if let Some(value) = event.partial_fills_today {
        strategy.partial_fills_today = value;
    }
    if let Some(value) = event.cancels_total_today {
        strategy.cancels_total_today = value;
    }
    if let Some(value) = event.rejects_total_today {
        strategy.rejects_total_today = value;
    }
    strategy.strategy_realized_pnl = event
        .strategy_realized_pnl
        .or(strategy.strategy_realized_pnl.take());
    strategy.strategy_unrealized_pnl = event
        .strategy_unrealized_pnl
        .or(strategy.strategy_unrealized_pnl.take());
    strategy.strategy_total_pnl = event
        .strategy_total_pnl
        .or(strategy.strategy_total_pnl.take());
    strategy.pnl_source = event.pnl_source.or(strategy.pnl_source.take());
    strategy.pnl_basis = event.pnl_basis.or(strategy.pnl_basis.take());
    strategy.pnl_diff_vs_account = event
        .pnl_diff_vs_account
        .or(strategy.pnl_diff_vs_account.take());
    strategy.pnl_as_of_ts_ns = event.pnl_as_of_ts_ns.or(strategy.pnl_as_of_ts_ns);
    strategy.session_phase = event.session_phase.or(strategy.session_phase.take());
    strategy.strategy_window_id = event
        .strategy_window_id
        .or(strategy.strategy_window_id.take());
    strategy.window_start_ts_ns = event.window_start_ts_ns.or(strategy.window_start_ts_ns);
    strategy.window_end_ts_ns = event.window_end_ts_ns.or(strategy.window_end_ts_ns);
    strategy.window_status = event.window_status.or(strategy.window_status.take());
    strategy.next_transition_ts_ns = event
        .next_transition_ts_ns
        .or(strategy.next_transition_ts_ns);
    strategy.is_market_open = event.is_market_open.or(strategy.is_market_open);
    strategy.is_regular_session = event.is_regular_session.or(strategy.is_regular_session);
    strategy.is_opening_window = event.is_opening_window.or(strategy.is_opening_window);
    if let Some(value) = event.symbols_blocked {
        strategy.symbols_blocked = value;
    }
    if let Some(value) = event.symbols_with_fresh_l1 {
        strategy.symbols_with_fresh_l1 = value;
    }
    if let Some(value) = event.symbols_with_fresh_l2 {
        strategy.symbols_with_fresh_l2 = value;
    }
    if let Some(value) = event.symbols_missing_md {
        strategy.symbols_missing_md = value;
    }
    if let Some(value) = event.l1_symbols_allocated {
        strategy.l1_symbols_allocated = value;
    }
    if let Some(value) = event.l2_capacity {
        strategy.l2_capacity = value;
    }
    if let Some(value) = event.l2_capacity_used {
        strategy.l2_capacity_used = value;
    }
    if !event.l2_denied_symbols.is_empty() {
        strategy.l2_denied_symbols = event.l2_denied_symbols;
    }
    strategy.lease_authority_version = event
        .lease_authority_version
        .or(strategy.lease_authority_version.take());
    if !event.risk_gates.is_empty() {
        strategy.risk_gates = event
            .risk_gates
            .into_iter()
            .map(|gate| crate::state::StrategyRiskGateView {
                name: gate.name,
                passed: gate.passed,
                detail: gate.detail,
                scope: gate.scope,
                observed: gate.observed,
                limit: gate.limit,
                status: gate.status,
                severity: gate.severity,
                reason: gate.reason,
                policy_version: gate.policy_version,
                source_seq: gate.source_seq,
                evaluated_ts_ns: gate.evaluated_ts_ns,
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
    chain.bbo_bid_at_signal = event.bid_price;
    chain.bbo_ask_at_signal = event.ask_price;
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
    chain.total_qty.get_or_insert(event.quantity);
    chain.intent_created_ts_ns = Some(publish_ts_ns);
    if let Some(account_id) = event.account_id {
        chain.account_id = Some(account_id);
    }
    chain.notional = event.notional;
    chain.decision_price = event.limit_price_hint.clone();
    chain.limit_price = event.limit_price_hint;
    chain.bbo_bid_at_intent = chain.bbo_bid_at_signal.clone();
    chain.bbo_ask_at_intent = chain.bbo_ask_at_signal.clone();
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
        risk_decision_seq: event.risk_decision_seq.or(Some(sequence)),
        risk_result: event
            .risk_result
            .clone()
            .or_else(|| Some(if event.approved { "PASS" } else { "FAIL" }.to_string())),
        limits_snapshot_id: event.limits_snapshot_id.clone(),
        risk_mode: event.risk_mode.clone(),
        limits_enforced: event.limits_enforced,
    });
    chain.risk_decision_ts_ns = event.evaluated_ts_ns.or(Some(publish_ts_ns));
    if let Some(risk_mode) = event.risk_mode.clone() {
        state.risk.risk_mode = Some(risk_mode);
    }
    if let Some(limits_enforced) = event.limits_enforced {
        state.risk.limits_enforced = Some(limits_enforced);
    }
    chain.latency.intent_to_risk_ms = chain
        .intent_created_ts_ns
        .and_then(|intent_ts_ns| non_negative_delta_ms(publish_ts_ns, intent_ts_ns));
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
            severity: rule.severity.clone(),
            reason: rule.reason.clone(),
            policy_version: rule
                .policy_version
                .clone()
                .or(event.authority_policy_version.clone()),
            source_seq: rule.source_seq.or(Some(sequence)),
            evaluated_ts_ns: rule
                .evaluated_ts_ns
                .or(event.evaluated_ts_ns)
                .or(Some(publish_ts_ns)),
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
        let risk_block_scope = format!("{}/{}", event.strategy_id, event.symbol);
        let risk_block_rule = event
            .evaluated_rules
            .iter()
            .find(|rule| !rule.passed)
            .map(|rule| rule.rule_id.clone());
        upsert_risk_block(
            state,
            RiskBlock {
                block_id: event.decision_id.clone().unwrap_or_else(|| {
                    format!("risk_decision:{}:{}", event.strategy_id, event.symbol)
                }),
                rule_id: risk_block_rule.clone().unwrap_or_default(),
                scope: risk_block_scope.clone(),
                severity: event
                    .severity
                    .clone()
                    .unwrap_or_else(|| "block".to_string()),
                message: message.clone(),
                source: "risk_decision".to_string(),
                first_seen_ts_ns: publish_ts_ns,
                last_seen_ts_ns: publish_ts_ns,
                cleared_ts_ns: None,
                correlation_id: Some(event.correlation_id.clone()),
                symbol: Some(event.symbol.clone()),
                strategy_id: Some(event.strategy_id.clone()),
                blocks_order_submit: true,
                blocks_cancel: false,
                blocks_short: true,
                blocks_command: true,
                last_seen_seq: event.risk_decision_seq.or(Some(sequence)),
                scope_type: Some("strategy_symbol".to_string()),
                scope_id: Some(risk_block_scope),
                reason_code: risk_block_rule,
                reason_text: Some(message.clone()),
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
    chain.order_ref = event.order_ref.clone();
    chain.broker_account_id = event.broker_account_id.clone();
    chain.broker_perm_id = event.broker_perm_id.clone().or(event.perm_id.clone());
    chain.broker_client_id = event.client_id;
    chain.strategy_order_ref = event.order_ref.clone().or(chain.strategy_order_ref.clone());
    chain.side = event.side.clone().or(chain.side.clone());
    chain.order_type = Some(event.order_type.clone());
    chain.limit_price = event.limit_price.clone();
    chain.stop_price = event.stop_price.clone();
    chain.tif = Some(event.tif.clone());
    chain.route = event.route.clone();
    chain.exchange = event.exchange.clone();
    chain.destination = event.destination.clone();
    if chain.intended_quantity.is_none() {
        chain.intended_quantity = event.quantity;
    }
    chain.submitted_quantity = event.quantity;
    chain.total_qty = event.quantity.or(chain.total_qty);
    chain.remaining_quantity = event.remaining_quantity.or(event.quantity);
    chain.cum_qty_i64 = Some(0);
    chain.leaves_qty_i64 = chain.remaining_quantity;
    chain.display_qty = event.display_size;
    chain.min_qty = event.min_qty;
    chain.submit_requested_ts_ns = event.submit_requested_ts_ns.or(Some(publish_ts_ns));
    chain.submit_ts_ns = chain.submit_requested_ts_ns;
    chain.bbo_bid_at_submit = event.bbo_bid_at_submit;
    chain.bbo_ask_at_submit = event.bbo_ask_at_submit;
    chain.mid_at_submit = event.mid_at_submit;
    chain.spread_bps_at_submit = event.spread_bps_at_submit;
    chain.quote_age_ms_at_submit = event.quote_age_ms_at_submit;
    chain.queue_position_estimate = event.queue_position_estimate;
    chain.slippage_vs_mid_bps = event.slippage_vs_mid_bps;
    chain.slippage_vs_decision_bps = event.slippage_vs_decision_bps;
    chain.latency.risk_to_submit_req_ms = chain
        .risk_decision_ts_ns
        .and_then(|risk_ts_ns| non_negative_delta_ms(publish_ts_ns, risk_ts_ns));
    chain.latency.risk_to_submit_ms = chain.latency.risk_to_submit_req_ms;
    chain.refresh_quantity_reason();
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
        chain.broker_account_id = event
            .broker_account_id
            .clone()
            .or(chain.broker_account_id.clone());
        chain.broker_perm_id = event
            .broker_perm_id
            .clone()
            .or(event.perm_id.clone())
            .or(chain.broker_perm_id.clone());
        chain.broker_client_id = event.client_id.or(chain.broker_client_id);
        chain.order_submitted_ts_ns = event.order_submitted_ts_ns.or(Some(publish_ts_ns));
        chain.bbo_bid_at_submit = event.bbo_bid_at_submit.or(chain.bbo_bid_at_submit.clone());
        chain.bbo_ask_at_submit = event.bbo_ask_at_submit.or(chain.bbo_ask_at_submit.clone());
        chain.latency.submit_req_to_submitted_ms = chain
            .submit_requested_ts_ns
            .and_then(|submit_req_ts_ns| non_negative_delta_ms(publish_ts_ns, submit_req_ts_ns));
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
    chain.broker_account_id = event
        .broker_account_id
        .clone()
        .or(chain.broker_account_id.clone());
    chain.broker_perm_id = event
        .broker_perm_id
        .clone()
        .or(event.perm_id.clone())
        .or(chain.broker_perm_id.clone());
    chain.remaining_quantity = event.remaining_quantity;
    chain.leaves_qty_i64 = event.remaining_quantity;
    chain.broker_ack_ts_ns = event
        .broker_ack_ts_ns
        .or(event.receive_ts_ns)
        .or(Some(publish_ts_ns));
    chain.ack_ts_ns = chain.broker_ack_ts_ns;
    chain.bbo_bid_at_ack = event.bbo_bid_at_ack;
    chain.bbo_ask_at_ack = event.bbo_ask_at_ack;
    if let Some(submit_ts_ns) = chain.submit_ts_ns {
        chain.latency.submit_to_ack_ms = non_negative_delta_ms(publish_ts_ns, submit_ts_ns);
    }
    if let Some(submitted_ts_ns) = chain.order_submitted_ts_ns {
        chain.latency.submitted_to_ack_ms = non_negative_delta_ms(publish_ts_ns, submitted_ts_ns);
    }
    chain.refresh_quantity_reason();
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
    let fill_view = FillExecutionView {
        exec_id: event.execution_id.clone(),
        broker_exec_id: event.broker_execution_id.clone(),
        fill_seq: sequence,
        qty: last_quantity,
        price: last_price.clone(),
        order_id: Some(event.order_id.clone()),
        symbol: event.symbol.clone().or_else(|| chain.symbol.clone()),
        side: event.side.clone().or_else(|| chain.side.clone()),
        exchange: event.exchange.clone().or_else(|| chain.exchange.clone()),
        venue: event.venue.clone(),
        liquidity_flag: event.liquidity.clone(),
        commission: event.commission.clone(),
        fees: event.fees.iter().map(|fee| fee.amount.clone()).collect(),
        fee_details: event
            .fees
            .iter()
            .map(|fee| crate::state::FeeDetailView {
                name: fee.name.clone(),
                amount: fee.amount.clone(),
            })
            .collect(),
        currency: event
            .settlement_currency
            .clone()
            .or_else(|| Some(last_price.currency.clone())),
        fill_ts_ns: event.trade_ts_ns.or(Some(publish_ts_ns)),
        report_ts_ns: event.report_ts_ns,
        ingest_ts_ns: event.ingest_ts_ns,
        position_after_fill: event.position_after_fill,
        realized_pnl_delta: event.realized_pnl_delta.clone(),
    };
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
    if let Some(commission) = event.commission.clone() {
        chain.commission = Some(commission);
    }
    chain.bbo_bid_at_fill = event.bbo_bid_at_fill.or(chain.bbo_bid_at_fill.clone());
    chain.bbo_ask_at_fill = event.bbo_ask_at_fill.or(chain.bbo_ask_at_fill.clone());
    chain.slippage_vs_mid_bps = event.slippage_vs_mid_bps.or(chain.slippage_vs_mid_bps);
    chain.slippage_vs_arrival_bps = event
        .slippage_vs_arrival_bps
        .or(chain.slippage_vs_arrival_bps);
    chain.slippage_vs_decision_bps = event
        .slippage_vs_decision_bps
        .or(chain.slippage_vs_decision_bps);
    chain.refresh_quantity_reason();
    if applied {
        chain.fills.push(fill_view);
        chain.first_fill_ts_ns.get_or_insert(publish_ts_ns);
        chain.last_fill_ts_ns = Some(publish_ts_ns);
        if let Some(ack_ts_ns) = chain.ack_ts_ns {
            chain.latency.ack_to_first_fill_ms = chain
                .latency
                .ack_to_first_fill_ms
                .or_else(|| non_negative_delta_ms(publish_ts_ns, ack_ts_ns));
        }
        if let Some(submit_ts_ns) = chain.submit_ts_ns {
            chain.latency.submit_to_first_fill_ms = chain
                .latency
                .submit_to_first_fill_ms
                .or_else(|| non_negative_delta_ms(publish_ts_ns, submit_ts_ns));
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
        if let Some(first_fill_ts_ns) = chain.first_fill_ts_ns {
            chain.latency.partial_to_full_fill_ms =
                non_negative_delta_ms(publish_ts_ns, first_fill_ts_ns);
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
    refresh_order_fee_totals(state, &event.correlation_id);
    refresh_account_fee_totals(state);
    refresh_account_aggregate(state);
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
    chain.cancel_requested_ts_ns = event.cancel_requested_ts_ns.or(Some(publish_ts_ns));
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
    chain.cancel_ack_ts_ns = event.cancel_ack_ts_ns.or(Some(publish_ts_ns));
    if let (Some(ack_ts_ns), Some(request_ts_ns)) =
        (chain.cancel_ack_ts_ns, chain.cancel_requested_ts_ns)
    {
        chain.latency.cancel_to_ack_ms = non_negative_delta_ms(ack_ts_ns, request_ts_ns);
    }
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
    let calculated_unrealized_pnl =
        (event.market_price.as_f64() - event.average_price.as_f64()) * event.net_quantity as f64;
    let unrealized_pnl = event
        .unrealized_pnl
        .as_ref()
        .map(|money| money.as_f64())
        .unwrap_or(calculated_unrealized_pnl);
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
                avg_cost: item.avg_cost,
                realized_pnl: item.realized_pnl,
                unrealized_pnl: item.unrealized_pnl,
                fees: item.fees,
                attribution_method: item.attribution_method,
                attribution_version: item.attribution_version,
                avg_cost_ts_ns: item.avg_cost_ts_ns,
            })
            .collect(),
        open_buy_qty: event.open_buy_qty,
        open_sell_qty: event.open_sell_qty,
        pending_cancel_qty: event.pending_cancel_qty,
        reserved_buy_power: event.reserved_buy_power,
        position_notional: event.position_notional,
        gross_exposure: event.gross_exposure,
        net_exposure: event.net_exposure,
        realized_pnl: event.realized_pnl,
        mark_source: event.mark_source,
        mark_ts_ns: event.mark_ts_ns,
        mark_age_ms: event.mark_age_ms,
    });
    recalculate_account_position_pnl(state, &account_id);
}

fn reduce_risk_limit_breached(state: &mut AppState, publish_ts_ns: i64, event: RiskLimitBreached) {
    state.risk.global_state = "BLOCKED".to_string();
    let scope = event.scope.clone();
    let (scope_type, scope_id) = scope
        .split_once(':')
        .map(|(scope_type, scope_id)| (Some(scope_type.to_string()), Some(scope_id.to_string())))
        .unwrap_or((None, None));
    upsert_risk_block(
        state,
        RiskBlock {
            block_id: event
                .block_id
                .clone()
                .unwrap_or_else(|| format!("risk_limit:{}", scope)),
            rule_id: event.rule_id.unwrap_or_default(),
            scope,
            severity: event.severity,
            message: event.message,
            source: "risk_limit".to_string(),
            first_seen_ts_ns: event.first_seen_ts_ns.unwrap_or(publish_ts_ns),
            last_seen_ts_ns: event.last_seen_ts_ns.unwrap_or(publish_ts_ns),
            cleared_ts_ns: event.cleared_ts_ns,
            correlation_id: event.correlation_id,
            symbol: event.symbol,
            strategy_id: event.strategy_id,
            blocks_order_submit: true,
            blocks_cancel: false,
            blocks_short: true,
            blocks_command: true,
            last_seen_seq: None,
            scope_type,
            scope_id,
            reason_code: None,
            reason_text: None,
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
    command.session = event.session;
    command.requested_at_ts_ns = event.requested_at_ts_ns;
    command.risk_checked = event.risk_checked;
    command.dry_run = event.dry_run;
    command.execute_broker = event.execute_broker;
    command.approval_id = event.approval_id;
    command.status = command.authority_status.clone();
}

fn reduce_command_audit_recorded(state: &mut AppState, event: CommandAuditRecorded) {
    state.connection.command_gateway = event.status.clone();
    let command = state.commands.get_or_insert(&event.command_id);
    command.operator_id = Some(event.operator_id.clone());
    command.command_type = Some(event.command_type.clone());
    command.audit_status = Some(event.status.clone());
    command.audit_reason = Some(event.reason.clone());
    command.status = Some(event.status.clone());
    command.target = event.target.clone();
    command.aggregate_id = event.target.clone();
    command.result_event_id = event.result_event_id;
    command.error_code = event.error_code;
    command.error_message = event.error_message;
    command.rollback_command_id = event.rollback_command_id;
    command.execute_broker = event.execute_broker.or(command.execute_broker);
    command.dry_run = event.dry_run.or(command.dry_run);
    command.requested_at_ts_ns = event.requested_at_ts_ns.or(command.requested_at_ts_ns);

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
    account.apply_position_mark_pnl(unrealized_pnl);
    refresh_account_aggregate(state);
}

fn refresh_order_fee_totals(state: &mut AppState, correlation_id: &str) {
    let Some(chain) = state.orders.by_correlation_id.get_mut(correlation_id) else {
        return;
    };

    let mut total_commission = None;
    let mut total_fees = None;
    for fill in &chain.fills {
        if let Some(commission) = fill.commission.as_ref() {
            add_money(&mut total_commission, commission);
        }
        for fee in &fill.fees {
            add_money(&mut total_fees, fee);
        }
    }

    let mut total_fee = total_commission.clone();
    if let Some(fees) = total_fees.as_ref() {
        add_money(&mut total_fee, fees);
    }

    chain.total_commission = total_commission;
    chain.total_fees = total_fees;
    chain.total_fee = total_fee;
}

fn refresh_account_fee_totals(state: &mut AppState) {
    let mut totals = BTreeMap::<String, (Option<Money>, Option<Money>, Option<Money>)>::new();
    for chain in state.orders.by_correlation_id.values() {
        let Some(account_id) = chain.account_id.as_ref() else {
            continue;
        };
        let entry = totals.entry(account_id.clone()).or_default();
        if let Some(commission) = chain.total_commission.as_ref() {
            add_money(&mut entry.0, commission);
        }
        if let Some(fees) = chain.total_fees.as_ref() {
            add_money(&mut entry.1, fees);
        }
        if let Some(total_fee) = chain.total_fee.as_ref() {
            add_money(&mut entry.2, total_fee);
        }
    }

    for account in state.accounts.by_id.values_mut() {
        account.commission_today = None;
        account.fees_today = None;
        account.total_fee_today = None;
    }

    for (account_id, (commission, fees, total_fee)) in totals {
        let account = state.accounts.get_or_insert(&account_id);
        account.commission_today = commission;
        account.fees_today = fees;
        account.total_fee_today = total_fee;
    }
}

fn add_money(total: &mut Option<Money>, value: &Money) {
    match total {
        Some(existing) if existing.scale == value.scale && existing.currency == value.currency => {
            existing.value += value.value;
        }
        Some(existing) => {
            *existing = Money::from_f64(
                existing.as_f64() + value.as_f64(),
                existing.currency.clone(),
            );
        }
        None => {
            *total = Some(value.clone());
        }
    }
}

fn refresh_account_aggregate(state: &mut AppState) {
    refresh_account_safety_state(state, crate::unix_ts_ns());
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

    if next.cleared_ts_ns.is_some() {
        state.risk.active_blocks.retain(|existing| {
            !((!next.block_id.is_empty() && existing.block_id == next.block_id)
                || (!next.rule_id.is_empty()
                    && existing.rule_id == next.rule_id
                    && existing.scope == next.scope))
        });
        return;
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
        existing.source = next.source;
        existing.cleared_ts_ns = next.cleared_ts_ns;
        existing.correlation_id = next.correlation_id;
        existing.symbol = next.symbol;
        existing.strategy_id = next.strategy_id;
        existing.blocks_order_submit = next.blocks_order_submit;
        existing.blocks_cancel = next.blocks_cancel;
        existing.blocks_short = next.blocks_short;
        existing.blocks_command = next.blocks_command;
        existing.last_seen_seq = next.last_seen_seq.or(existing.last_seen_seq);
        existing.scope_type = next.scope_type.or(existing.scope_type.take());
        existing.scope_id = next.scope_id.or(existing.scope_id.take());
        existing.reason_code = next.reason_code.or(existing.reason_code.take());
        existing.reason_text = next.reason_text.or(existing.reason_text.take());
        return;
    }

    state.risk.active_blocks.push(next);
}

fn summarize(envelope: &EventEnvelope) -> EventSummary {
    EventSummary {
        event_id: envelope.event_id.clone(),
        sequence: envelope.sequence,
        ts_ns: envelope.publish_ts_ns,
        source_ts_ns: envelope.source_ts_ns,
        ingest_ts_ns: envelope.ingest_ts_ns,
        publish_ts_ns: envelope.publish_ts_ns,
        event_type: envelope.event_type.clone(),
        aggregate_type: envelope.aggregate_type.clone(),
        aggregate_id: envelope.aggregate_id.clone(),
        correlation_id: envelope.correlation_id.clone(),
        causation_id: envelope.causation_id.clone(),
        producer: envelope.producer.clone(),
        schema_version: envelope.schema_version.clone(),
        stream: envelope.stream.clone(),
        subject: envelope.subject.clone(),
        partition_key: envelope.partition_key.clone(),
        environment: envelope.environment.clone(),
        trace_id: envelope.trace_id.clone(),
        span_id: envelope.span_id.clone(),
        checksum: envelope.checksum.clone(),
        event_hash: envelope.event_hash.clone(),
        prev_event_hash: envelope.prev_event_hash.clone(),
        aggregate_version: envelope.aggregate_version,
        aggregate_hash: envelope.aggregate_hash.clone(),
        projection_version: envelope.projection_version.clone(),
        marker: None,
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
        DomainEvent::MarketDataSummary(event) => {
            let quote_age = event
                .quote_age_ms
                .map(|value| format!(" age={value}ms"))
                .unwrap_or_default();
            let imbalance = event
                .imbalance
                .map(|value| format!(" imb={value:.2}"))
                .unwrap_or_default();
            format!("md {}{}{}", event.symbol, quote_age, imbalance)
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
