use trade_core::events::{
    CancelRejected, CancelRequested, CommandAuditRecorded, DomainEvent, EventEnvelope,
    IntentCreated, OrderFill, OrderSubmitRequested, OrderSubmitted, PositionSnapshot,
    RiskDecisionMade, RiskRuleEval, SignalGenerated, StrategyHeartbeat,
    StrategyPositionAttribution,
};
use trade_core::state::OrderLifecycleState;
use trade_core::{reduce_event, AppState, Price};

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
    assert!((chain.average_fill_price.as_ref().unwrap().as_f64() - 123.456).abs() < 0.0001);
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
                ..Default::default()
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
                ..Default::default()
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
fn duplicate_event_ids_are_idempotent() {
    let mut state = AppState::default();
    let event = EventEnvelope::new(
        "evt-duplicate-001",
        "corr-duplicate",
        1,
        "test",
        DomainEvent::SignalGenerated(SignalGenerated {
            correlation_id: "corr-duplicate".to_string(),
            strategy_id: "open-scalp".to_string(),
            symbol: "MU".to_string(),
            signal_name: "gap_continuation".to_string(),
            score: Some(0.82),
            reason: "open-window".to_string(),
            ..Default::default()
        }),
    );

    reduce_event(&mut state, event.clone());
    reduce_event(&mut state, event);

    let strategy = state.strategies.by_id.get("open-scalp").unwrap();
    assert_eq!(strategy.signals, 1);
    assert_eq!(state.connection.events_ingested, 1);
    assert_eq!(state.connection.duplicate_events, 1);
}

#[test]
fn cumulative_fills_do_not_double_count_quantity() {
    let mut state = AppState::default();
    for (event_id, sequence, last_quantity, cumulative_quantity, remaining_quantity, price) in [
        ("evt-fill-cumulative-001", 1, 40, 40, 60, 123.45),
        ("evt-fill-cumulative-002", 2, 60, 100, 0, 123.46),
    ] {
        reduce_event(
            &mut state,
            EventEnvelope::new(
                event_id,
                "corr-fill-cumulative",
                sequence,
                "test",
                DomainEvent::OrderFilled(OrderFill {
                    correlation_id: "corr-fill-cumulative".to_string(),
                    account_id: "paper-main".to_string(),
                    order_id: "ord-fill-cumulative".to_string(),
                    execution_id: Some(format!("exec-{sequence}")),
                    filled_quantity: last_quantity,
                    fill_price: Price::from_f64(price, "USD"),
                    last_quantity: Some(last_quantity),
                    cumulative_quantity: Some(cumulative_quantity),
                    remaining_quantity: Some(remaining_quantity),
                    ..Default::default()
                }),
            ),
        );
    }

    let chain = state
        .orders
        .by_correlation_id
        .get("corr-fill-cumulative")
        .unwrap();
    assert_eq!(chain.filled_quantity, 100);
    assert_eq!(chain.remaining_quantity, Some(0));
    assert!((chain.average_fill_price.as_ref().unwrap().as_f64() - 123.456).abs() < 0.0001);
}

#[test]
fn risk_rule_evals_update_rule_table_and_dedupe_blocks() {
    let mut state = AppState::default();
    for sequence in [1, 2] {
        reduce_event(
            &mut state,
            EventEnvelope::new(
                format!("evt-risk-rule-{sequence}"),
                "corr-risk-rule",
                sequence,
                "risk-engine",
                DomainEvent::RiskDecisionMade(RiskDecisionMade {
                    correlation_id: "corr-risk-rule".to_string(),
                    strategy_id: "l1-l2-imbalance".to_string(),
                    symbol: "NVDA".to_string(),
                    approved: false,
                    reason_codes: vec!["quote_staleness_ms".to_string()],
                    decision_id: Some("risk-decision-001".to_string()),
                    severity: Some("block".to_string()),
                    evaluated_rules: vec![RiskRuleEval {
                        rule_id: "quote_staleness_ms".to_string(),
                        rule_name: "quote staleness".to_string(),
                        passed: false,
                        observed: "820".to_string(),
                        threshold: "500".to_string(),
                        unit: "ms".to_string(),
                    }],
                    risk_snapshot_id: Some("risk-snapshot-001".to_string()),
                    authority_policy_version: Some("policy-v1".to_string()),
                    ..Default::default()
                }),
            ),
        );
    }

    assert_eq!(state.risk.structured_limits.len(), 2);
    assert_eq!(state.risk.active_blocks.len(), 1);
    let block = &state.risk.active_blocks[0];
    assert_eq!(block.block_id, "risk-decision-001");
    assert_eq!(block.rule_id, "quote_staleness_ms");
    assert_eq!(block.scope, "l1-l2-imbalance/NVDA");
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
                limit_price: Some(Price::from_f64(123.45, "USD")),
                tif: "DAY".to_string(),
                ..Default::default()
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
                ..Default::default()
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
                average_price: Price::from_f64(165.10, "USD"),
                market_price: Price::from_f64(164.92, "USD"),
                strategy_attribution: vec![StrategyPositionAttribution {
                    strategy_id: "l1-l2-imbalance".to_string(),
                    quantity: 50,
                }],
                ..Default::default()
            }),
        ),
    );

    let position = state.positions.by_key.get("paper-main:AMD").unwrap();
    assert!((position.unrealized_pnl - -9.0).abs() < 0.0001);
}

#[test]
fn multi_account_positions_do_not_overwrite_account_matrix() {
    let mut state = AppState::default();
    for (sequence, account_id, net_quantity, average_price, market_price) in [
        (1, "paper-main", 100, 10.0, 11.0),
        (2, "paper-alt", 50, 20.0, 18.0),
    ] {
        reduce_event(
            &mut state,
            EventEnvelope::new(
                format!("evt-pos-{sequence}"),
                format!("corr-pos-{sequence}"),
                sequence,
                "test",
                DomainEvent::PositionSnapshot(PositionSnapshot {
                    account_id: account_id.to_string(),
                    symbol: "MU".to_string(),
                    net_quantity,
                    average_price: Price::from_f64(average_price, "USD"),
                    market_price: Price::from_f64(market_price, "USD"),
                    strategy_attribution: vec![StrategyPositionAttribution {
                        strategy_id: "open-scalp".to_string(),
                        quantity: net_quantity,
                    }],
                    ..Default::default()
                }),
            ),
        );
    }

    assert_eq!(state.positions.by_key.len(), 2);
    assert!(state.positions.by_key.contains_key("paper-main:MU"));
    assert!(state.positions.by_key.contains_key("paper-alt:MU"));
    assert_eq!(state.accounts.by_id.len(), 2);
    assert_eq!(state.account.account_id, "ALL(2)");
    assert!((state.accounts.by_id["paper-main"].unrealized_pnl - 100.0).abs() < 0.0001);
    assert!((state.accounts.by_id["paper-alt"].unrealized_pnl - -100.0).abs() < 0.0001);
    assert!((state.account.unrealized_pnl - 0.0).abs() < 0.0001);
}

#[test]
fn account_command_audit_updates_only_target_account_runtime_state() {
    let mut state = AppState::default();
    reduce_event(
        &mut state,
        EventEnvelope::new(
            "evt-account-kill-audit",
            "cmd-account-kill",
            1,
            "command-gateway",
            DomainEvent::CommandAuditRecorded(CommandAuditRecorded {
                command_id: "cmd-account-kill".to_string(),
                operator_id: "operator-test".to_string(),
                command_type: "AccountKillSwitchRequested".to_string(),
                status: "dispatched".to_string(),
                reason: "broker-control runtime plan dispatched".to_string(),
                target: Some("paper-main".to_string()),
            }),
        ),
    );

    assert!(
        state.accounts.by_id["paper-main"]
            .runtime_controls
            .cancel_all
    );
    assert!(state.account.runtime_controls.cancel_all);
    assert!(!state.risk.kill_switch_active);
}

#[test]
fn coalesces_high_frequency_projection_events_without_dropping_lifecycle_events() {
    let mut state = AppState::default();

    reduce_event(
        &mut state,
        EventEnvelope::new(
            "evt-heartbeat-001",
            "corr-heartbeat",
            1,
            "test",
            DomainEvent::StrategyHeartbeat(StrategyHeartbeat {
                strategy_id: "open-scalp".to_string(),
                state: "RUN".to_string(),
                mode: "PAPER".to_string(),
                heartbeat_lag_ms: 83,
            }),
        ),
    );
    reduce_event(
        &mut state,
        EventEnvelope::new(
            "evt-heartbeat-002",
            "corr-heartbeat",
            2,
            "test",
            DomainEvent::StrategyHeartbeat(StrategyHeartbeat {
                strategy_id: "open-scalp".to_string(),
                state: "RUN".to_string(),
                mode: "PAPER".to_string(),
                heartbeat_lag_ms: 11,
            }),
        ),
    );

    reduce_event(
        &mut state,
        EventEnvelope::new(
            "evt-signal-001",
            "corr-noise-001",
            3,
            "test",
            DomainEvent::SignalGenerated(SignalGenerated {
                correlation_id: "corr-noise-001".to_string(),
                strategy_id: "open-scalp".to_string(),
                symbol: "MU".to_string(),
                signal_name: "gap_continuation".to_string(),
                score: Some(0.82),
                reason: "open-window".to_string(),
                ..Default::default()
            }),
        ),
    );
    reduce_event(
        &mut state,
        EventEnvelope::new(
            "evt-intent-001",
            "corr-noise-001",
            4,
            "test",
            DomainEvent::IntentCreated(IntentCreated {
                correlation_id: "corr-noise-001".to_string(),
                strategy_id: "open-scalp".to_string(),
                symbol: "MU".to_string(),
                side: "BUY".to_string(),
                quantity: 100,
                reason: "open-window".to_string(),
                ..Default::default()
            }),
        ),
    );

    assert_eq!(state.connection.events_ingested, 4);
    assert_eq!(state.connection.events_coalesced, 1);
    assert_eq!(state.audit.events.len(), 3);
    assert_eq!(state.connection.audit_events_retained, 3);
    assert!(state
        .audit
        .events
        .iter()
        .any(|event| event.event_type == "SignalGenerated"));
    assert!(state
        .audit
        .events
        .iter()
        .any(|event| event.event_type == "IntentCreated"));
}
