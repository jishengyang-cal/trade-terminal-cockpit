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

    #[test]
    fn encodes_and_decodes_terminal_projection_truth_contracts() {
        let money = super::trading::v1::Money {
            value: 1250,
            scale: 2,
            currency: "USD".to_string(),
        };
        let price = super::trading::v1::Price {
            value: 1234500,
            scale: 4,
            currency: "USD".to_string(),
        };
        let account = super::trading::v1::OverviewSnapshot {
            account_id: "paper-main".to_string(),
            mode: 1,
            broker: "BROKER_SIM".to_string(),
            broker_connected: true,
            cash: 10_000.0,
            buying_power: 20_000.0,
            day_pnl: 12.50,
            realized_pnl: 0.0,
            unrealized_pnl: 12.50,
            exposure_pct: 5.0,
            margin_usage_pct: 1.0,
            short_permission: false,
            short_intents_blocked_today: 2,
            risk_state: "NORMAL".to_string(),
            runtime_controls: Some(super::trading::v1::AccountRuntimeControls::default()),
            cash_value: Some(money.clone()),
            buying_power_value: Some(money.clone()),
            day_pnl_value: Some(money.clone()),
            realized_pnl_value: Some(money.clone()),
            unrealized_pnl_value: Some(money.clone()),
            account_currency: "USD".to_string(),
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
            account_snapshot_id: "acct-snap-1".to_string(),
            account_snapshot_seq: Some(41),
            account_snapshot_source: "state-projectiond".to_string(),
            account_snapshot_ts_ns: Some(1782379800000000000),
            account_snapshot_age_ms: Some(83),
            valuation_status: "COMPLETE".to_string(),
            valuation_stale: false,
            valuation_incomplete_reason: String::new(),
            cash_source: "broker".to_string(),
            buying_power_source: "broker".to_string(),
            net_liq_source: "broker".to_string(),
            available_funds_source: "broker".to_string(),
            day_pnl_source: "internal_position_mark".to_string(),
            realized_source: "broker".to_string(),
            unrealized_source: "internal_position_mark".to_string(),
            effective_trade_state: "TRADE".to_string(),
            effective_trade_reason: "OK".to_string(),
            can_submit_order: true,
            can_cancel_order: true,
            can_modify_order: true,
            can_liquidate: true,
            can_short: false,
            can_open_long: true,
            can_close_position: true,
        };
        let order = super::trading::v1::OrderTimeline {
            correlation_id: "corr-1".to_string(),
            strategy_id: "open-scalp".to_string(),
            account_id: "paper-main".to_string(),
            symbol: "MU".to_string(),
            side: 1,
            intended_quantity: 100,
            state: 12,
            order_id: "ord-1".to_string(),
            broker_order_id: "9182".to_string(),
            filled_quantity: 50,
            average_fill_price: Some(price.clone()),
            timeline: vec![super::trading::v1::OrderTimelineEntry {
                sequence: 41,
                ts_ns: 1782379801000000000,
                kind: "PARTIAL_FILL".to_string(),
                summary: "50 @ 123.45".to_string(),
            }],
            client_order_id: "client-1".to_string(),
            perm_id: "perm-1".to_string(),
            route: "SMART".to_string(),
            exchange: "SIM".to_string(),
            destination: "SIM".to_string(),
            submitted_quantity: Some(100),
            remaining_quantity: Some(50),
            last_fill_price: Some(price.clone()),
            submit_ts_ns: Some(1782379800000000000),
            ack_ts_ns: Some(1782379800500000000),
            first_fill_ts_ns: Some(1782379801000000000),
            last_fill_ts_ns: Some(1782379801000000000),
            terminal_ts_ns: None,
            latency: Some(super::trading::v1::LatencyBreakdown {
                signal_to_intent_ms: None,
                intent_to_risk_ms: None,
                risk_to_submit_ms: None,
                submit_to_ack_ms: Some(500),
                ack_to_first_fill_ms: Some(500),
                submit_to_terminal_ms: None,
            }),
            anomalies: Vec::new(),
            order_ref: "open-scalp:ord-1".to_string(),
            strategy_order_ref: "open-scalp:ord-1".to_string(),
            cum_qty_i64: Some(50),
            leaves_qty_i64: Some(50),
            display_qty: Some(100),
            min_qty: Some(1),
            fills: vec![super::trading::v1::OrderFillProjection {
                exec_id: "exec-1".to_string(),
                broker_exec_id: "ib-exec-1".to_string(),
                fill_seq: 41,
                qty: 50,
                price: Some(price.clone()),
                venue: "SIM".to_string(),
                liquidity_flag: "M".to_string(),
                commission: Some(money.clone()),
                fees: vec![money.clone()],
                currency: "USD".to_string(),
                fill_ts_ns: Some(1782379801000000000),
                report_ts_ns: Some(1782379801100000000),
                position_after_fill: Some(50),
            }],
            risk: Some(super::trading::v1::OrderRiskProjection {
                approved: true,
                reason_codes: vec!["quote_fresh=17ms".to_string()],
                severity: "INFO".to_string(),
                decision_id: "risk-1".to_string(),
                risk_snapshot_id: "risk-snap-1".to_string(),
                authority_policy_version: "policy-v1".to_string(),
            }),
        };
        let risk = super::trading::v1::RiskSnapshot {
            global_state: "NORMAL".to_string(),
            kill_switch_active: false,
            market_data_fresh: true,
            broker_order_channel_ok: true,
            day_max_loss_breached: false,
            quote_staleness_ok: true,
            short_permission: false,
            active_blocks: vec![super::trading::v1::RiskBlock {
                scope: "account:paper-main".to_string(),
                severity: "HARD_BLOCK".to_string(),
                message: "account is long-only".to_string(),
                block_id: "paper-main:NO_SHORT_ACCOUNT_PERMISSION".to_string(),
                rule_id: "NO_SHORT_ACCOUNT_PERMISSION".to_string(),
                first_seen_ts_ns: 1782379800000000000,
                last_seen_ts_ns: 1782379800000000000,
                cleared_ts_ns: None,
                correlation_id: String::new(),
                symbol: String::new(),
                strategy_id: String::new(),
                source: "account_effective_state".to_string(),
                blocks_order_submit: false,
                blocks_cancel: false,
                blocks_short: true,
                blocks_command: false,
            }],
            structured_limits: Vec::new(),
        };
        let snapshot = super::trading::v1::TerminalProjectionSnapshot {
            schema_version: "trading.projections.v1".to_string(),
            snapshot_ts_ns: 1782379800000000000,
            source: "state-projectiond".to_string(),
            last_event_sequence: Some(41),
            account: Some(account),
            strategies: Vec::new(),
            orders: vec![order],
            positions: Vec::new(),
            risk: Some(risk),
            alerts: Vec::new(),
            accounts: Vec::new(),
            commands: Vec::new(),
            market_data: Vec::new(),
        };

        let mut bytes = Vec::new();
        snapshot
            .encode(&mut bytes)
            .expect("encode projection snapshot");
        let decoded = super::trading::v1::TerminalProjectionSnapshot::decode(bytes.as_slice())
            .expect("decode projection snapshot");
        let account = decoded.account.expect("account");
        assert_eq!(account.valuation_status, "COMPLETE");
        assert!(account.can_submit_order);
        let order = decoded.orders.first().expect("order");
        assert_eq!(order.cum_qty_i64, Some(50));
        assert_eq!(order.fills[0].broker_exec_id, "ib-exec-1");
        let risk = decoded.risk.expect("risk");
        assert!(risk.active_blocks[0].blocks_short);
    }
}
