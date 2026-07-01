use trade_core::events::{
    AccountSnapshot, CancelRejected, CancelRequested, CommandAuditRecorded, DomainEvent,
    EventEnvelope, Fee, IntentCreated, MarketDataSummary, OrderFill, OrderSubmitRequested,
    OrderSubmitted, PositionSnapshot, RiskDecisionMade, RiskRuleEval, SignalGenerated,
    StrategyHeartbeat, StrategyPositionAttribution,
};
use trade_core::state::OrderLifecycleState;
use trade_core::{reduce_event, AppState, Money, Price};

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
fn aggregates_order_and_account_fee_totals_from_fills() {
    let mut state = AppState::default();
    reduce_event(
        &mut state,
        EventEnvelope::new(
            "evt-fee-fill-001",
            "corr-fee",
            1,
            "test",
            DomainEvent::OrderFilled(OrderFill {
                correlation_id: "corr-fee".to_string(),
                account_id: "paper-main".to_string(),
                order_id: "ord-fee".to_string(),
                execution_id: Some("exec-fee-001".to_string()),
                filled_quantity: 100,
                fill_price: Price::from_f64(12.34, "USD"),
                last_quantity: Some(100),
                cumulative_quantity: Some(100),
                remaining_quantity: Some(0),
                commission: Some(Money::from_f64(0.35, "USD")),
                fees: vec![
                    Fee {
                        name: "sec".to_string(),
                        amount: Money::from_f64(0.02, "USD"),
                    },
                    Fee {
                        name: "taf".to_string(),
                        amount: Money::from_f64(0.01, "USD"),
                    },
                ],
                ..Default::default()
            }),
        ),
    );

    let chain = state.orders.by_correlation_id.get("corr-fee").unwrap();
    assert_money(chain.total_commission.as_ref(), 0.35);
    assert_money(chain.total_fees.as_ref(), 0.03);
    assert_money(chain.total_fee.as_ref(), 0.38);

    let account = state.accounts.by_id.get("paper-main").unwrap();
    assert_money(account.commission_today.as_ref(), 0.35);
    assert_money(account.fees_today.as_ref(), 0.03);
    assert_money(account.total_fee_today.as_ref(), 0.38);
    assert_money(state.account.total_fee_today.as_ref(), 0.38);
}

#[test]
fn account_snapshot_derives_ocam_authority_mapping() {
    let mut state = AppState::default();
    state.risk.broker_order_channel_ok = true;
    reduce_event(
        &mut state,
        EventEnvelope::new(
            "evt-account-authority-001",
            "corr-account-authority",
            1,
            "account-projection",
            DomainEvent::AccountSnapshot(AccountSnapshot {
                account_id: "paper-main".to_string(),
                mode: Some("PAPER".to_string()),
                gateway_tier: Some("paper".to_string()),
                account_slot: Some(0),
                role_bits: Some(0b11),
                readonly: Some(false),
                short_permission: Some(true),
                margin_account: Some(true),
                ..Default::default()
            }),
        ),
    );

    let account = state.accounts.by_id.get("paper-main").unwrap();
    assert_eq!(
        account.canonical_account_id.as_deref(),
        Some("paper-main+paper")
    );
    assert_eq!(account.account_role.as_deref(), Some("data_and_trade"));
    assert_eq!(account.short_permission_label(), "CAN_SHORT");
    assert_eq!(account.margin_permission_label(), "MARGIN");
    assert_eq!(account.mutation_permission_label(), "TRADE");
    assert!(account
        .account_id_hash_hex
        .as_deref()
        .unwrap()
        .starts_with("0x"));
}

fn assert_money(value: Option<&Money>, expected: f64) {
    let value = value.expect("money should be present");
    assert!((value.as_f64() - expected).abs() < 0.0001);
}

#[test]
fn account_no_trade_mutation_creates_account_block() {
    let mut state = AppState::default();
    state.risk.broker_order_channel_ok = true;
    reduce_event(
        &mut state,
        EventEnvelope::new(
            "evt-account-no-trade",
            "corr-account-no-trade",
            1,
            "account-projection",
            DomainEvent::AccountSnapshot(AccountSnapshot {
                account_id: "paper-data-only".to_string(),
                broker_connected: Some(true),
                role_bits: Some(0b01),
                readonly: Some(false),
                short_permission: Some(true),
                ..Default::default()
            }),
        ),
    );

    let account = state.accounts.by_id.get("paper-data-only").unwrap();
    assert_eq!(account.effective_trade_state, "NO_TRADE");
    assert_eq!(
        account.effective_trade_reason.as_deref(),
        Some("NO_TRADE_ACCOUNT_MUTATION")
    );
    assert!(state.risk.active_blocks.iter().any(|block| {
        block.scope == "account:paper-data-only"
            && block.rule_id == "NO_TRADE_ACCOUNT_MUTATION"
            && block.blocks_order_submit
    }));
}

#[test]
fn order_channel_down_creates_account_block_without_hardcoded_account() {
    let mut state = AppState::default();
    state.risk.broker_order_channel_ok = false;
    for (sequence, account_id) in [(1, "paper-main"), (2, "paper-alt")] {
        reduce_event(
            &mut state,
            EventEnvelope::new(
                format!("evt-account-channel-{sequence}"),
                format!("corr-account-channel-{sequence}"),
                sequence,
                "account-projection",
                DomainEvent::AccountSnapshot(AccountSnapshot {
                    account_id: account_id.to_string(),
                    broker_connected: Some(true),
                    role_bits: Some(0b11),
                    readonly: Some(false),
                    short_permission: Some(true),
                    ..Default::default()
                }),
            ),
        );
    }

    for account_id in ["paper-main", "paper-alt"] {
        let account = state.accounts.by_id.get(account_id).unwrap();
        assert_eq!(
            account.effective_trade_reason.as_deref(),
            Some("ORDER_CHANNEL_DOWN")
        );
        assert!(state.risk.active_blocks.iter().any(|block| {
            block.scope == format!("account:{account_id}")
                && block.rule_id == "ORDER_CHANNEL_DOWN"
                && block.blocks_order_submit
                && block.blocks_cancel
        }));
    }
}

#[test]
fn cumulative_fills_do_not_double_count_quantity() {
    let mut state = AppState::default();
    reduce_event(
        &mut state,
        EventEnvelope::new(
            "evt-fill-submit-001",
            "corr-fill-cumulative",
            1,
            "test",
            DomainEvent::OrderSubmitRequested(OrderSubmitRequested {
                correlation_id: "corr-fill-cumulative".to_string(),
                account_id: "paper-main".to_string(),
                order_id: "ord-fill-cumulative".to_string(),
                order_type: "LMT".to_string(),
                limit_price: Some(Price::from_f64(123.45, "USD")),
                tif: "DAY".to_string(),
                side: Some("BUY".to_string()),
                quantity: Some(100),
                remaining_quantity: Some(100),
                order_ref: Some("open-scalp:ord-fill-cumulative".to_string()),
                min_qty: Some(10),
                display_size: Some(50),
                ..Default::default()
            }),
        ),
    );
    for (event_id, sequence, last_quantity, cumulative_quantity, remaining_quantity, price) in [
        ("evt-fill-cumulative-001", 2, 40, 40, 60, 123.45),
        ("evt-fill-cumulative-002", 3, 60, 100, 0, 123.46),
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
                    broker_execution_id: Some(format!("broker-exec-{sequence}")),
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
    assert_eq!(chain.total_quantity(), Some(100));
    assert_eq!(chain.cum_qty_i64, Some(100));
    assert_eq!(chain.leaves_qty_i64, Some(0));
    assert_eq!(chain.quantity_status(), "OK");
    assert_eq!(
        chain.order_ref.as_deref(),
        Some("open-scalp:ord-fill-cumulative")
    );
    assert_eq!(chain.display_qty, Some(50));
    assert_eq!(chain.min_qty, Some(10));
    assert_eq!(chain.fills.len(), 2);
    assert_eq!(chain.fills[0].exec_id.as_deref(), Some("exec-2"));
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
                        ..Default::default()
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
                cancel_requested_ts_ns: None,
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
                cancel_ack_ts_ns: None,
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
                    ..Default::default()
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
                        ..Default::default()
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
                result_event_id: None,
                error_code: None,
                error_message: None,
                rollback_command_id: None,
                execute_broker: None,
                dry_run: None,
                requested_at_ts_ns: None,
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

#[test]
fn coalesces_market_data_summaries_by_symbol() {
    let mut state = AppState::default();
    for (sequence, quote_age_ms, imbalance) in [(1, 17, 0.41), (2, 24, -0.12)] {
        reduce_event(
            &mut state,
            EventEnvelope::new(
                format!("evt-md-{sequence}"),
                "corr-md",
                sequence,
                "market-data-summary",
                DomainEvent::MarketDataSummary(MarketDataSummary {
                    symbol: "MU".to_string(),
                    quote_age_ms: Some(quote_age_ms),
                    imbalance: Some(imbalance),
                    ..Default::default()
                }),
            ),
        );
    }

    let summary = state.market_data.by_symbol.get("MU").unwrap();
    assert_eq!(summary.quote_age_ms, Some(24));
    assert_eq!(summary.imbalance, Some(-0.12));
    assert_eq!(state.connection.events_coalesced, 1);
    assert_eq!(
        state
            .audit
            .events
            .iter()
            .filter(|event| event.event_type == "MarketDataSummary")
            .count(),
        1
    );
}
