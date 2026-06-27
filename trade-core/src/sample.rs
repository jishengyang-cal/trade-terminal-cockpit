use crate::events::{
    AlertRaised, BrokerAckReceived, DomainEvent, EventEnvelope, IntentCreated, OrderFill,
    OrderSubmitRequested, OrderSubmitted, PositionSnapshot, RiskDecisionMade, SignalGenerated,
    StrategyHeartbeat, StrategyPositionAttribution,
};

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
        next(DomainEvent::StrategyHeartbeat(StrategyHeartbeat {
            strategy_id: "open-scalp".to_string(),
            state: "RUN".to_string(),
            mode: "PAPER".to_string(),
            heartbeat_lag_ms: 83,
        })),
        next(DomainEvent::SignalGenerated(SignalGenerated {
            correlation_id: correlation_id.to_string(),
            strategy_id: "open-scalp".to_string(),
            symbol: "MU".to_string(),
            signal_name: "gap_continuation".to_string(),
            score: Some(0.82),
            reason: "open-window".to_string(),
        })),
        next(DomainEvent::IntentCreated(IntentCreated {
            correlation_id: correlation_id.to_string(),
            strategy_id: "open-scalp".to_string(),
            symbol: "MU".to_string(),
            side: "BUY".to_string(),
            quantity: 100,
            reason: "open-window".to_string(),
        })),
        next(DomainEvent::RiskDecisionMade(RiskDecisionMade {
            correlation_id: correlation_id.to_string(),
            strategy_id: "open-scalp".to_string(),
            symbol: "MU".to_string(),
            approved: true,
            reason_codes: vec!["quote_fresh=17ms".to_string(), "max_loss_ok".to_string()],
        })),
        next(DomainEvent::OrderSubmitRequested(OrderSubmitRequested {
            correlation_id: correlation_id.to_string(),
            account_id: "paper-main".to_string(),
            order_id: "ord-demo-001".to_string(),
            order_type: "LMT".to_string(),
            limit_price: Some(123.45),
            tif: "DAY".to_string(),
        })),
        next(DomainEvent::OrderSubmitted(OrderSubmitted {
            correlation_id: correlation_id.to_string(),
            account_id: "paper-main".to_string(),
            order_id: "ord-demo-001".to_string(),
            broker: "BROKER_SIM".to_string(),
        })),
        next(DomainEvent::BrokerAckReceived(BrokerAckReceived {
            correlation_id: correlation_id.to_string(),
            account_id: "paper-main".to_string(),
            order_id: "ord-demo-001".to_string(),
            broker_order_id: "9182".to_string(),
            broker_status: "PreSubmitted".to_string(),
        })),
        next(DomainEvent::OrderPartiallyFilled(OrderFill {
            correlation_id: correlation_id.to_string(),
            account_id: "paper-main".to_string(),
            order_id: "ord-demo-001".to_string(),
            filled_quantity: 40,
            fill_price: 123.45,
        })),
        next(DomainEvent::OrderFilled(OrderFill {
            correlation_id: correlation_id.to_string(),
            account_id: "paper-main".to_string(),
            order_id: "ord-demo-001".to_string(),
            filled_quantity: 60,
            fill_price: 123.46,
        })),
        next(DomainEvent::PositionSnapshot(PositionSnapshot {
            account_id: "paper-main".to_string(),
            symbol: "MU".to_string(),
            net_quantity: 100,
            average_price: 123.456,
            market_price: 124.02,
            strategy_attribution: vec![StrategyPositionAttribution {
                strategy_id: "open-scalp".to_string(),
                quantity: 100,
            }],
        })),
        next(DomainEvent::AlertRaised(AlertRaised {
            alert_id: "alert-demo-001".to_string(),
            severity: "WARN".to_string(),
            domain: "market-data".to_string(),
            message: "quote stale NVDA 231ms".to_string(),
        })),
    ]
}
