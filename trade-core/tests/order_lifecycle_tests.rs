use trade_core::events::{
    DomainEvent, EventEnvelope, IntentCreated, PositionSnapshot, RiskDecisionMade,
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
    assert_eq!(chain.timeline.len(), 7);

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
