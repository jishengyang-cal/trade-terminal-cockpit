use trade_core::events::{
    CancelRejected, CancelRequested, DomainEvent, EventEnvelope, IntentCreated,
    OrderSubmitRequested, OrderSubmitted, PositionSnapshot, RiskDecisionMade, SignalGenerated,
    StrategyPositionAttribution,
};
use trade_core::state::OrderLifecycleState;
use trade_core::{reduce_event, AppState};

#[test]
fn reconstructs_order_lifecycle_from_event_sequence() {
    let mut state = AppState::default();

    for event in trade_core::sample::sample_events() {
        reduce_event(&mut state, event);
    }

    let chain = state
        .orders
        .by_correlation_id
        .get("corr-demo-001")
        .expect("sample order chain should be present");

    assert_eq!(chain.state, OrderLifecycleState::Filled);
    assert_eq!(chain.strategy_id.as_deref(), Some("open-scalp"));
    assert_eq!(chain.symbol.as_deref(), Some("MU"));
    assert_eq!(chain.filled_quantity, 100);
    assert!((chain.average_fill_price.unwrap() - 123.456).abs() < 0.0001);
    assert_eq!(chain.timeline.len(), 8);
    assert_eq!(chain.timeline[0].kind, "SIGNAL");

    let strategy = state.strategies.by_id.get("open-scalp").unwrap();
    assert_eq!(strategy.signals, 1);

    let position = state
        .positions
        .by_key
        .get("paper-main:MU")
        .expect("position snapshot should be present");
    assert_eq!(position.net_quantity, 100);
    assert_eq!(position.strategy_attribution[0].strategy_id, "open-scalp");
}

#[test]
fn risk_rejection_blocks_chain_before_submit() {
    let mut state = AppState::default();
    let correlation_id = "corr-risk-reject";
    let events = vec![
        EventEnvelope::new(
            "evt-risk-001",
            correlation_id,
            1,
            "test",
            DomainEvent::IntentCreated(IntentCreated {
                correlation_id: correlation_id.to_string(),
                strategy_id: "l1-l2-imbalance".to_string(),
                symbol: "NVDA".to_string(),
                side: "SELL_SHORT".to_string(),
                quantity: 10,
                reason: "book-imbalance".to_string(),
            }),
        ),
        EventEnvelope::new(
            "evt-risk-002",
            correlation_id,
            2,
            "test",
            DomainEvent::RiskDecisionMade(RiskDecisionMade {
                correlation_id: correlation_id.to_string(),
                strategy_id: "l1-l2-imbalance".to_string(),
                symbol: "NVDA".to_string(),
                approved: false,
                reason_codes: vec!["short_permission=false".to_string()],
            }),
        ),
    ];

    for event in events {
        reduce_event(&mut state, event);
    }

    let chain = state
        .orders
        .by_correlation_id
        .get(correlation_id)
        .expect("rejected chain should be retained for evidence");

    assert_eq!(chain.state, OrderLifecycleState::RiskRejected);
    assert_eq!(chain.order_id, None);
    assert_eq!(state.account.short_intents_blocked_today, 1);
    assert_eq!(state.risk.active_blocks.len(), 1);
}

#[test]
fn cancel_rejection_is_retained_in_order_lifecycle() {
    let mut state = AppState::default();
    let correlation_id = "corr-cancel-reject";
    let account_id = "paper-main";
    let order_id = "ord-cancel-reject";
    let events = vec![
        EventEnvelope::new(
            "evt-cancel-reject-001",
            correlation_id,
            1,
            "test",
            DomainEvent::SignalGenerated(SignalGenerated {
                correlation_id: correlation_id.to_string(),
                strategy_id: "open-scalp".to_string(),
                symbol: "MU".to_string(),
                signal_name: "gap_continuation".to_string(),
                score: Some(0.82),
                reason: "open-window".to_string(),
            }),
        ),
        EventEnvelope::new(
            "evt-cancel-reject-002",
            correlation_id,
            2,
            "test",
            DomainEvent::IntentCreated(IntentCreated {
                correlation_id: correlation_id.to_string(),
                strategy_id: "open-scalp".to_string(),
                symbol: "MU".to_string(),
                side: "BUY".to_string(),
                quantity: 100,
                reason: "open-window".to_string(),
            }),
        ),
        EventEnvelope::new(
            "evt-cancel-reject-003",
            correlation_id,
            3,
            "test",
            DomainEvent::RiskDecisionMade(RiskDecisionMade {
                correlation_id: correlation_id.to_string(),
                strategy_id: "open-scalp".to_string(),
                symbol: "MU".to_string(),
                approved: true,
                reason_codes: vec!["quote_fresh=17ms".to_string()],
            }),
        ),
        EventEnvelope::new(
            "evt-cancel-reject-004",
            correlation_id,
            4,
            "test",
            DomainEvent::OrderSubmitRequested(OrderSubmitRequested {
                correlation_id: correlation_id.to_string(),
                account_id: account_id.to_string(),
                order_id: order_id.to_string(),
                order_type: "LMT".to_string(),
                limit_price: Some(123.45),
                tif: "DAY".to_string(),
            }),
        ),
        EventEnvelope::new(
            "evt-cancel-reject-005",
            correlation_id,
            5,
            "test",
            DomainEvent::OrderSubmitted(OrderSubmitted {
                correlation_id: correlation_id.to_string(),
                account_id: account_id.to_string(),
                order_id: order_id.to_string(),
                broker: "BROKER_SIM".to_string(),
            }),
        ),
        EventEnvelope::new(
            "evt-cancel-reject-006",
            correlation_id,
            6,
            "test",
            DomainEvent::CancelRequested(CancelRequested {
                correlation_id: correlation_id.to_string(),
                account_id: account_id.to_string(),
                order_id: order_id.to_string(),
                reason: "operator requested cancel".to_string(),
            }),
        ),
        EventEnvelope::new(
            "evt-cancel-reject-007",
            correlation_id,
            7,
            "test",
            DomainEvent::CancelRejected(CancelRejected {
                correlation_id: correlation_id.to_string(),
                account_id: account_id.to_string(),
                order_id: order_id.to_string(),
                reason: "broker already filling".to_string(),
            }),
        ),
    ];

    for event in events {
        reduce_event(&mut state, event);
    }

    let chain = state
        .orders
        .by_correlation_id
        .get(correlation_id)
        .expect("cancel-rejected chain should be retained for evidence");

    assert_eq!(chain.state, OrderLifecycleState::CancelRejected);
    assert_eq!(chain.order_id.as_deref(), Some(order_id));
    assert_eq!(
        state
            .orders
            .order_id_index
            .get("paper-main:ord-cancel-reject"),
        Some(&correlation_id.to_string())
    );
    assert_eq!(chain.timeline.last().unwrap().kind, "CANCEL_REJECT");
}

#[test]
fn position_unrealized_pnl_is_projection_only() {
    let mut state = AppState::default();
    reduce_event(
        &mut state,
        EventEnvelope::new(
            "evt-pos-001",
            "corr-pos",
            1,
            "test",
            DomainEvent::PositionSnapshot(PositionSnapshot {
                account_id: "paper-main".to_string(),
                symbol: "AMD".to_string(),
                net_quantity: 50,
                average_price: 165.10,
                market_price: 164.92,
                strategy_attribution: vec![StrategyPositionAttribution {
                    strategy_id: "l1-l2-imbalance".to_string(),
                    quantity: 50,
                }],
            }),
        ),
    );

    let position = state.positions.by_key.get("paper-main:AMD").unwrap();
    assert!((position.unrealized_pnl - -9.0).abs() < 0.0001);
}
