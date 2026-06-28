use std::collections::BTreeMap;
use trade_core::events::{DomainEvent, EventEnvelope, OrderFill};
use trade_core::state::{
    AccountView, AlertView, AppState, OrderChain, OrderLifecycleState, PositionView, RiskView,
    StrategyPositionView, StrategyRiskGateView, StrategyView,
};
use trade_core::{apply_projection_snapshot, reduce_event, Price, ProjectionSnapshot};

#[test]
fn projection_snapshot_initializes_trade_cockpit_state() {
    let mut state = AppState::default();
    apply_projection_snapshot(&mut state, sample_snapshot());

    assert_eq!(state.account.account_id, "paper-main");
    assert_eq!(state.accounts.by_id.len(), 1);
    assert!(state.accounts.by_id.contains_key("paper-main"));
    assert_eq!(state.strategies.by_id.len(), 1);
    assert_eq!(state.orders.by_correlation_id.len(), 1);
    assert_eq!(
        state.orders.order_id_index.get("paper-main:ord-snap-001"),
        Some(&"corr-snap-001".to_string())
    );
    assert_eq!(state.positions.by_key.len(), 1);
    assert_eq!(state.alerts.open_count(), 1);
    assert_eq!(state.connection.nats, "state-projectiond-json");
    assert_eq!(state.connection.last_event_sequence, Some(41));
}

#[test]
fn events_continue_from_loaded_projection_snapshot() {
    let mut state = AppState::default();
    apply_projection_snapshot(&mut state, sample_snapshot());

    reduce_event(
        &mut state,
        EventEnvelope::new(
            "evt-after-snapshot-001",
            "corr-snap-001",
            42,
            "test",
            DomainEvent::OrderFilled(OrderFill {
                correlation_id: "corr-snap-001".to_string(),
                account_id: "paper-main".to_string(),
                order_id: "ord-snap-001".to_string(),
                filled_quantity: 50,
                fill_price: Price::from_f64(124.10, "USD"),
                last_quantity: Some(50),
                cumulative_quantity: Some(100),
                remaining_quantity: Some(0),
                ..Default::default()
            }),
        ),
    );

    let chain = state.orders.by_correlation_id.get("corr-snap-001").unwrap();
    assert_eq!(chain.state, OrderLifecycleState::Filled);
    assert_eq!(chain.filled_quantity, 100);
    assert!(chain
        .timeline
        .last()
        .unwrap()
        .summary
        .contains("50 @ 124.1000 USD"));
    assert_eq!(state.connection.last_event_sequence, Some(42));
}

fn sample_snapshot() -> ProjectionSnapshot {
    let mut chain = OrderChain::new("corr-snap-001");
    chain.strategy_id = Some("open-scalp".to_string());
    chain.account_id = Some("paper-main".to_string());
    chain.symbol = Some("MU".to_string());
    chain.side = Some("BUY".to_string());
    chain.intended_quantity = Some(100);
    chain.state = OrderLifecycleState::PartiallyFilled;
    chain.order_id = Some("ord-snap-001".to_string());
    chain.filled_quantity = 50;
    chain.average_fill_price = Some(Price::from_f64(123.45, "USD"));
    chain.push_timeline(41, 41_000, "PARTIAL_FILL", "50 @ 123.4500");

    ProjectionSnapshot {
        schema_version: "trading.projections.v1".to_string(),
        snapshot_ts_ns: 41_000,
        source: "state-projectiond-json".to_string(),
        last_event_sequence: Some(41),
        account: Some(AccountView {
            account_id: "paper-main".to_string(),
            mode: "PAPER".to_string(),
            broker: "BROKER_SIM".to_string(),
            broker_connected: true,
            cash: 10_000.0,
            buying_power: 20_000.0,
            day_pnl: 12.34,
            realized_pnl: 0.0,
            unrealized_pnl: 12.34,
            exposure_pct: 5.0,
            margin_usage_pct: 1.0,
            short_permission: false,
            short_intents_blocked_today: 2,
            runtime_controls: Default::default(),
            ..AccountView::new("paper-main")
        }),
        accounts: Vec::new(),
        strategies: vec![StrategyView {
            strategy_id: "open-scalp".to_string(),
            state: "RUN".to_string(),
            mode: "PAPER".to_string(),
            universe_count: 80,
            signals: 120,
            intents: 4,
            orders: 1,
            pnl: 12.34,
            heartbeat_lag_ms: Some(83),
            last_event_sequence: Some(41),
            last_reason: None,
            last_signal_sequence: Some(37),
            last_intent_sequence: Some(38),
            last_order_sequence: Some(39),
            parameters: BTreeMap::from([
                ("cooldown_ms".to_string(), "800".to_string()),
                ("imbalance_threshold".to_string(), "0.73".to_string()),
            ]),
            risk_gates: vec![StrategyRiskGateView {
                name: "quote_freshness".to_string(),
                passed: true,
                detail: "83ms".to_string(),
            }],
        }],
        orders: vec![chain],
        positions: vec![PositionView {
            key: "paper-main:MU".to_string(),
            account_id: "paper-main".to_string(),
            symbol: "MU".to_string(),
            net_quantity: 50,
            average_price: Price::from_f64(123.45, "USD"),
            market_price: Price::from_f64(123.70, "USD"),
            unrealized_pnl: 12.50,
            strategy_attribution: vec![StrategyPositionView {
                strategy_id: "open-scalp".to_string(),
                quantity: 50,
            }],
        }],
        risk: Some(RiskView::default()),
        alerts: vec![AlertView {
            alert_id: "alert-snap-001".to_string(),
            severity: "WARN".to_string(),
            domain: "market-data".to_string(),
            message: "quote stale MU 120ms".to_string(),
            acknowledged: false,
            acknowledged_by: None,
            acknowledge_reason: None,
        }],
    }
}
