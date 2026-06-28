pub mod trading {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/trading.v1.rs"));
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    #[test]
    fn encodes_and_decodes_event_envelope_contract() {
        let envelope = super::trading::v1::EventEnvelope {
            event_id: "evt-contract-001".to_string(),
            event_type: "StrategyHeartbeat".to_string(),
            aggregate_type: "strategy".to_string(),
            aggregate_id: "open-scalp".to_string(),
            correlation_id: "corr-contract-001".to_string(),
            causation_id: String::new(),
            source_ts_ns: 1,
            ingest_ts_ns: 2,
            publish_ts_ns: 3,
            sequence: 4,
            producer: "trade-contracts-test".to_string(),
            schema_version: "trading.events.v1".to_string(),
            payload: Vec::new(),
            stream: String::new(),
            subject: String::new(),
            partition_key: String::new(),
            replay_id: String::new(),
            environment: "paper".to_string(),
            venue_ts_ns: None,
            receive_ts_ns: None,
            monotonic_ns: None,
            trace_id: String::new(),
            span_id: String::new(),
            checksum: String::new(),
        };

        let mut bytes = Vec::new();
        envelope.encode(&mut bytes).expect("encode envelope");
        let decoded =
            super::trading::v1::EventEnvelope::decode(bytes.as_slice()).expect("decode envelope");

        assert_eq!(decoded.event_id, "evt-contract-001");
        assert_eq!(decoded.sequence, 4);
        assert_eq!(decoded.correlation_id, "corr-contract-001");
    }

    #[test]
    fn encodes_and_decodes_account_strategy_and_authority_contracts() {
        let money = super::trading::v1::Money {
            value: 12345,
            scale: 2,
            currency: "USD".to_string(),
        };
        let account = super::trading::v1::AccountSnapshot {
            account_id: "paper-main".to_string(),
            mode: "PAPER".to_string(),
            broker: "BROKER_SIM".to_string(),
            broker_connected: Some(true),
            account_currency: "USD".to_string(),
            cash: Some(money.clone()),
            buying_power: Some(money.clone()),
            day_pnl: Some(money.clone()),
            realized_pnl: Some(money.clone()),
            unrealized_pnl: Some(money.clone()),
            net_liquidation: Some(money.clone()),
            equity_with_loan: Some(money.clone()),
            initial_margin: Some(money.clone()),
            maintenance_margin: Some(money.clone()),
            excess_liquidity: Some(money.clone()),
            available_funds: Some(money.clone()),
            sma: Some(money.clone()),
            day_trades_remaining: Some(3),
            pdt_status: "ok".to_string(),
            trading_restriction: String::new(),
            settled_cash: Some(money.clone()),
            unsettled_cash: Some(money.clone()),
            gross_exposure: Some(money.clone()),
            net_exposure: Some(money.clone()),
            long_market_value: Some(money.clone()),
            short_market_value: Some(money.clone()),
            exposure_pct: Some(38.0),
            margin_usage_pct: Some(7.8),
            short_permission: Some(false),
            short_intents_blocked_today: Some(17),
            canonical_account_id: "paper-main+paper".to_string(),
            account_slot: Some(0),
            account_id_hash_hex: "0x0f3406d4a9b7b70c".to_string(),
            endpoint_id: "ibkr-paper-main".to_string(),
            client_id: Some(80),
            gateway_tier: "paper".to_string(),
            account_role: "data_and_trade".to_string(),
            role_bits: Some(3),
            readonly: Some(false),
            margin_account: Some(true),
            account_type: "margin".to_string(),
        };

        let mut bytes = Vec::new();
        account.encode(&mut bytes).expect("encode account");
        let decoded =
            super::trading::v1::AccountSnapshot::decode(bytes.as_slice()).expect("decode account");
        assert_eq!(decoded.account_id, "paper-main");
        assert_eq!(decoded.cash.expect("cash").value, 12345);

        let strategy = super::trading::v1::StrategyHealthUpdated {
            strategy_id: "open-scalp".to_string(),
            enabled: Some(true),
            trading_window: "09:32-09:40".to_string(),
            current_phase: "active".to_string(),
            universe_version: "open-gap-v3".to_string(),
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
            parameters: std::collections::HashMap::from([(
                "imbalance_threshold".to_string(),
                "0.73".to_string(),
            )]),
            risk_gates: vec![super::trading::v1::StrategyRiskGateProjection {
                name: "quote_freshness".to_string(),
                passed: true,
                detail: "17ms".to_string(),
            }],
        };
        bytes.clear();
        strategy.encode(&mut bytes).expect("encode strategy health");
        let decoded = super::trading::v1::StrategyHealthUpdated::decode(bytes.as_slice())
            .expect("decode strategy health");
        assert_eq!(decoded.strategy_id, "open-scalp");
        assert_eq!(decoded.avg_submit_to_ack_ms, Some(241));

        let authority = super::trading::v1::CommandAuthorityDecided {
            decision_id: "decision-cmd-1".to_string(),
            command_id: "cmd-1".to_string(),
            status: "accepted".to_string(),
            reason_codes: vec!["capability_ok".to_string()],
            matched_policy_ids: vec!["capability.required".to_string()],
            operator_id: "operator".to_string(),
            command_type: "PauseStrategyRequested".to_string(),
            capability: "strategy.control".to_string(),
            scope: "open-scalp".to_string(),
            approved_by: vec!["command-gateway".to_string()],
            decided_ts_ns: 42,
            authority_policy_version: "test-policy".to_string(),
            target_environment: "paper".to_string(),
        };
        bytes.clear();
        authority.encode(&mut bytes).expect("encode authority");
        let decoded = super::trading::v1::CommandAuthorityDecided::decode(bytes.as_slice())
            .expect("decode authority");
        assert_eq!(decoded.command_id, "cmd-1");
        assert_eq!(decoded.matched_policy_ids[0], "capability.required");
    }
}
