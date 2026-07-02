use crate::events::{
    AccountSnapshot, AlertRaised, BrokerAckReceived, DomainEvent, EventEnvelope, IntentCreated,
    MarketDataSummary, OrderFill, OrderSubmitRequested, OrderSubmitted, PositionSnapshot,
    RiskDecisionMade, RiskRuleEval, SignalGenerated, StrategyHealthUpdated, StrategyHeartbeat,
    StrategyPositionAttribution, StrategyRiskGateProjection,
};
use crate::{Money, Price};
use std::collections::BTreeMap;

pub fn sample_events() -> Vec<EventEnvelope> {
    let correlation_id = "corr-demo-001";
    let mut sequence = 1_u64;
    let mut next = |payload: DomainEvent| {
        let event = EventEnvelope::new(
            format!("evt-demo-{sequence:04}"),
            correlation_id,
            sequence,
            "trade-core-sample",
            payload,
        );
        sequence += 1;
        event
    };

    vec![
        next(DomainEvent::AccountSnapshot(AccountSnapshot {
            account_id: "paper-main".to_string(),
            canonical_account_id: Some("paper-main+paper".to_string()),
            account_slot: Some(0),
            endpoint_id: Some("ibkr-paper-main".to_string()),
            client_id: Some(80),
            gateway_tier: Some("paper".to_string()),
            account_role: Some("data_and_trade".to_string()),
            role_bits: Some(0b11),
            readonly: Some(false),
            mode: Some("PAPER".to_string()),
            broker: Some("BROKER_SIM".to_string()),
            broker_connected: Some(true),
            account_currency: Some("USD".to_string()),
            cash: Some(Money::from_f64(25_000.0, "USD")),
            buying_power: Some(Money::from_f64(100_000.0, "USD")),
            day_pnl: Some(Money::from_f64(123.45, "USD")),
            realized_pnl: Some(Money::from_f64(66.45, "USD")),
            unrealized_pnl: Some(Money::from_f64(57.0, "USD")),
            net_liquidation: Some(Money::from_f64(101_234.56, "USD")),
            maintenance_margin: Some(Money::from_f64(7_800.0, "USD")),
            available_funds: Some(Money::from_f64(42_000.0, "USD")),
            gross_exposure: Some(Money::from_f64(38_000.0, "USD")),
            net_exposure: Some(Money::from_f64(12_000.0, "USD")),
            exposure_pct: Some(38.0),
            margin_usage_pct: Some(7.8),
            short_permission: Some(false),
            margin_account: Some(true),
            account_type: Some("margin".to_string()),
            short_intents_blocked_today: Some(17),
            day_trades_remaining: Some(3),
            pdt_status: Some("ok".to_string()),
            ..Default::default()
        })),
        next(DomainEvent::MarketDataSummary(MarketDataSummary {
            symbol: "MU".to_string(),
            source: Some("market-data-summary".to_string()),
            bid_price: Some(Price::from_f64(123.44, "USD")),
            ask_price: Some(Price::from_f64(123.45, "USD")),
            spread_bps: Some(0.81),
            imbalance: Some(0.41),
            microprice: Some(Price::from_f64(123.446, "USD")),
            quote_age_ms: Some(17),
            event_rate_per_sec: Some(182.0),
            wall_size: Some(750_000),
            summary_ts_ns: Some(crate::unix_ts_ns()),
        })),
        next(DomainEvent::StrategyHeartbeat(StrategyHeartbeat {
            strategy_id: "open-scalp".to_string(),
            canonical_strategy_id: None,
            strategy_instance_id: None,
            account_id: None,
            state: "RUN".to_string(),
            mode: "PAPER".to_string(),
            heartbeat_lag_ms: 83,
        })),
        next(DomainEvent::StrategyHealthUpdated(StrategyHealthUpdated {
            strategy_id: "open-scalp".to_string(),
            canonical_strategy_id: None,
            strategy_instance_id: None,
            account_id: None,
            enabled: Some(true),
            trading_window: Some("09:32-09:40".to_string()),
            current_phase: Some("active".to_string()),
            universe_version: Some("open-gap-v3".to_string()),
            universe_count: Some(80),
            active_symbol_count: Some(12),
            watched_symbol_count: Some(80),
            l2_allocated_symbol_count: Some(20),
            signal_rate_1m: Some(16.2),
            reject_rate_1m: Some(0.7),
            fill_rate_1m: Some(2.1),
            cancel_rate_1m: Some(1.0),
            avg_intent_to_submit_ms: Some(14),
            avg_submit_to_ack_ms: Some(241),
            avg_ack_to_fill_ms: Some(529),
            consecutive_stops: Some(0),
            trades_today: Some(2),
            max_trades_today: Some(3),
            daily_loss_used_pct: Some(12.0),
            parameters: BTreeMap::from([
                ("imbalance_threshold".to_string(), "0.73".to_string()),
                ("cooldown_ms".to_string(), "800".to_string()),
            ]),
            risk_gates: vec![
                StrategyRiskGateProjection {
                    name: "quote_freshness".to_string(),
                    passed: true,
                    detail: "17ms".to_string(),
                    ..Default::default()
                },
                StrategyRiskGateProjection {
                    name: "short_permission".to_string(),
                    passed: false,
                    detail: "short intents blocked".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        })),
        next(DomainEvent::SignalGenerated(SignalGenerated {
            correlation_id: correlation_id.to_string(),
            strategy_id: "open-scalp".to_string(),
            symbol: "MU".to_string(),
            signal_name: "gap_continuation".to_string(),
            score: Some(0.82),
            reason: "open-window".to_string(),
            ..Default::default()
        })),
        next(DomainEvent::IntentCreated(IntentCreated {
            correlation_id: correlation_id.to_string(),
            strategy_id: "open-scalp".to_string(),
            symbol: "MU".to_string(),
            side: "BUY".to_string(),
            quantity: 100,
            reason: "open-window".to_string(),
            ..Default::default()
        })),
        next(DomainEvent::RiskDecisionMade(RiskDecisionMade {
            correlation_id: correlation_id.to_string(),
            strategy_id: "open-scalp".to_string(),
            symbol: "MU".to_string(),
            approved: true,
            reason_codes: vec!["quote_fresh=17ms".to_string(), "max_loss_ok".to_string()],
            evaluated_rules: vec![RiskRuleEval {
                rule_id: "quote_staleness_ms".to_string(),
                rule_name: "quote freshness".to_string(),
                passed: true,
                observed: "17".to_string(),
                threshold: "500".to_string(),
                unit: "ms".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        })),
        next(DomainEvent::OrderSubmitRequested(OrderSubmitRequested {
            correlation_id: correlation_id.to_string(),
            account_id: "paper-main".to_string(),
            order_id: "ord-demo-001".to_string(),
            order_type: "LMT".to_string(),
            limit_price: Some(Price::from_f64(123.45, "USD")),
            tif: "DAY".to_string(),
            ..Default::default()
        })),
        next(DomainEvent::OrderSubmitted(OrderSubmitted {
            correlation_id: correlation_id.to_string(),
            account_id: "paper-main".to_string(),
            order_id: "ord-demo-001".to_string(),
            broker: "BROKER_SIM".to_string(),
            ..Default::default()
        })),
        next(DomainEvent::BrokerAckReceived(BrokerAckReceived {
            correlation_id: correlation_id.to_string(),
            account_id: "paper-main".to_string(),
            order_id: "ord-demo-001".to_string(),
            broker_order_id: "9182".to_string(),
            broker_status: "PreSubmitted".to_string(),
            ..Default::default()
        })),
        next(DomainEvent::OrderPartiallyFilled(OrderFill {
            correlation_id: correlation_id.to_string(),
            account_id: "paper-main".to_string(),
            order_id: "ord-demo-001".to_string(),
            filled_quantity: 40,
            fill_price: Price::from_f64(123.45, "USD"),
            last_quantity: Some(40),
            cumulative_quantity: Some(40),
            remaining_quantity: Some(60),
            ..Default::default()
        })),
        next(DomainEvent::OrderFilled(OrderFill {
            correlation_id: correlation_id.to_string(),
            account_id: "paper-main".to_string(),
            order_id: "ord-demo-001".to_string(),
            filled_quantity: 60,
            fill_price: Price::from_f64(123.46, "USD"),
            last_quantity: Some(60),
            cumulative_quantity: Some(100),
            remaining_quantity: Some(0),
            ..Default::default()
        })),
        next(DomainEvent::PositionSnapshot(PositionSnapshot {
            account_id: "paper-main".to_string(),
            symbol: "MU".to_string(),
            net_quantity: 100,
            average_price: Price::from_f64(123.456, "USD"),
            market_price: Price::from_f64(124.02, "USD"),
            strategy_attribution: vec![StrategyPositionAttribution {
                strategy_id: "open-scalp".to_string(),
                quantity: 100,
                ..Default::default()
            }],
            ..Default::default()
        })),
        next(DomainEvent::AlertRaised(AlertRaised {
            alert_id: "alert-demo-001".to_string(),
            severity: "WARN".to_string(),
            domain: "market-data".to_string(),
            message: "quote stale NVDA 231ms".to_string(),
        })),
    ]
}
