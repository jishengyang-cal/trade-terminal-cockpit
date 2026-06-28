use crate::types::{Money, Price};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: String,
    pub event_type: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub correlation_id: String,
    pub causation_id: String,
    pub source_ts_ns: i64,
    pub ingest_ts_ns: i64,
    pub publish_ts_ns: i64,
    pub sequence: u64,
    pub producer: String,
    pub schema_version: String,
    #[serde(default)]
    pub stream: String,
    #[serde(default)]
    pub subject: String,
    #[serde(default)]
    pub partition_key: String,
    #[serde(default)]
    pub replay_id: Option<String>,
    #[serde(default)]
    pub environment: String,
    #[serde(default)]
    pub venue_ts_ns: Option<i64>,
    #[serde(default)]
    pub receive_ts_ns: Option<i64>,
    #[serde(default)]
    pub monotonic_ns: Option<i64>,
    #[serde(default)]
    pub trace_id: Option<String>,
    #[serde(default)]
    pub span_id: Option<String>,
    #[serde(default)]
    pub checksum: Option<String>,
    pub payload: DomainEvent,
}

impl EventEnvelope {
    pub fn new(
        event_id: impl Into<String>,
        correlation_id: impl Into<String>,
        sequence: u64,
        producer: impl Into<String>,
        payload: DomainEvent,
    ) -> Self {
        let event_type = payload.event_type().to_string();
        let aggregate_type = payload.aggregate_type().to_string();
        let aggregate_id = payload.aggregate_id();
        let ts = crate::unix_ts_ns();
        Self {
            event_id: event_id.into(),
            event_type,
            aggregate_type,
            aggregate_id,
            correlation_id: correlation_id.into(),
            causation_id: String::new(),
            source_ts_ns: ts,
            ingest_ts_ns: ts,
            publish_ts_ns: ts,
            sequence,
            producer: producer.into(),
            schema_version: "trading.events.v1".to_string(),
            stream: String::new(),
            subject: String::new(),
            partition_key: String::new(),
            replay_id: None,
            environment: "paper".to_string(),
            venue_ts_ns: None,
            receive_ts_ns: None,
            monotonic_ns: None,
            trace_id: None,
            span_id: None,
            checksum: None,
            payload,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum DomainEvent {
    AccountSnapshot(AccountSnapshot),
    StrategyHeartbeat(StrategyHeartbeat),
    StrategyHealthUpdated(StrategyHealthUpdated),
    StrategyStateChanged(StrategyStateChanged),
    SignalGenerated(SignalGenerated),
    IntentCreated(IntentCreated),
    RiskDecisionMade(RiskDecisionMade),
    OrderSubmitRequested(OrderSubmitRequested),
    OrderSubmitted(OrderSubmitted),
    BrokerAckReceived(BrokerAckReceived),
    OrderPartiallyFilled(OrderFill),
    OrderFilled(OrderFill),
    CancelRequested(CancelRequested),
    CancelRejected(CancelRejected),
    OrderCancelled(OrderCancelled),
    OrderRejected(OrderRejected),
    PositionSnapshot(PositionSnapshot),
    RiskLimitBreached(RiskLimitBreached),
    AlertRaised(AlertRaised),
    AlertAcknowledged(AlertAcknowledged),
    IngestDiagnosticRecorded(IngestDiagnosticRecorded),
    CommandAuthorityDecided(CommandAuthorityDecided),
    CommandAuditRecorded(CommandAuditRecorded),
}

impl DomainEvent {
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::AccountSnapshot(_) => "AccountSnapshot",
            Self::StrategyHeartbeat(_) => "StrategyHeartbeat",
            Self::StrategyHealthUpdated(_) => "StrategyHealthUpdated",
            Self::StrategyStateChanged(_) => "StrategyStateChanged",
            Self::SignalGenerated(_) => "SignalGenerated",
            Self::IntentCreated(_) => "IntentCreated",
            Self::RiskDecisionMade(_) => "RiskDecisionMade",
            Self::OrderSubmitRequested(_) => "OrderSubmitRequested",
            Self::OrderSubmitted(_) => "OrderSubmitted",
            Self::BrokerAckReceived(_) => "BrokerAckReceived",
            Self::OrderPartiallyFilled(_) => "OrderPartiallyFilled",
            Self::OrderFilled(_) => "OrderFilled",
            Self::CancelRequested(_) => "CancelRequested",
            Self::CancelRejected(_) => "CancelRejected",
            Self::OrderCancelled(_) => "OrderCancelled",
            Self::OrderRejected(_) => "OrderRejected",
            Self::PositionSnapshot(_) => "PositionSnapshot",
            Self::RiskLimitBreached(_) => "RiskLimitBreached",
            Self::AlertRaised(_) => "AlertRaised",
            Self::AlertAcknowledged(_) => "AlertAcknowledged",
            Self::IngestDiagnosticRecorded(_) => "IngestDiagnosticRecorded",
            Self::CommandAuthorityDecided(_) => "CommandAuthorityDecided",
            Self::CommandAuditRecorded(_) => "CommandAuditRecorded",
        }
    }

    pub fn aggregate_type(&self) -> &'static str {
        match self {
            Self::AccountSnapshot(_) => "account",
            Self::StrategyHeartbeat(_)
            | Self::StrategyHealthUpdated(_)
            | Self::StrategyStateChanged(_) => "strategy",
            Self::SignalGenerated(_)
            | Self::IntentCreated(_)
            | Self::RiskDecisionMade(_)
            | Self::OrderSubmitRequested(_)
            | Self::OrderSubmitted(_)
            | Self::BrokerAckReceived(_)
            | Self::OrderPartiallyFilled(_)
            | Self::OrderFilled(_)
            | Self::CancelRequested(_)
            | Self::CancelRejected(_)
            | Self::OrderCancelled(_)
            | Self::OrderRejected(_) => "order_chain",
            Self::PositionSnapshot(_) => "position",
            Self::RiskLimitBreached(_) => "risk",
            Self::AlertRaised(_) | Self::AlertAcknowledged(_) => "alert",
            Self::IngestDiagnosticRecorded(_) => "ingest",
            Self::CommandAuthorityDecided(_) | Self::CommandAuditRecorded(_) => "command",
        }
    }

    pub fn aggregate_id(&self) -> String {
        match self {
            Self::AccountSnapshot(event) => event.account_id.clone(),
            Self::StrategyHeartbeat(event) => event.strategy_id.clone(),
            Self::StrategyHealthUpdated(event) => event.strategy_id.clone(),
            Self::StrategyStateChanged(event) => event.strategy_id.clone(),
            Self::SignalGenerated(event) => event.correlation_id.clone(),
            Self::IntentCreated(event) => event.correlation_id.clone(),
            Self::RiskDecisionMade(event) => event.correlation_id.clone(),
            Self::OrderSubmitRequested(event) => event.correlation_id.clone(),
            Self::OrderSubmitted(event) => event.correlation_id.clone(),
            Self::BrokerAckReceived(event) => event.correlation_id.clone(),
            Self::OrderPartiallyFilled(event) | Self::OrderFilled(event) => {
                event.correlation_id.clone()
            }
            Self::CancelRequested(event) => event.correlation_id.clone(),
            Self::CancelRejected(event) => event.correlation_id.clone(),
            Self::OrderCancelled(event) => event.correlation_id.clone(),
            Self::OrderRejected(event) => event.correlation_id.clone(),
            Self::PositionSnapshot(event) => format!("{}:{}", event.account_id, event.symbol),
            Self::RiskLimitBreached(event) => event.scope.clone(),
            Self::AlertRaised(event) => event.alert_id.clone(),
            Self::AlertAcknowledged(event) => event.alert_id.clone(),
            Self::IngestDiagnosticRecorded(event) => event
                .subject
                .clone()
                .or_else(|| event.stream.clone())
                .unwrap_or_else(|| event.source.clone()),
            Self::CommandAuthorityDecided(event) => event.command_id.clone(),
            Self::CommandAuditRecorded(event) => event.command_id.clone(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AccountSnapshot {
    pub account_id: String,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub broker: Option<String>,
    #[serde(default)]
    pub broker_connected: Option<bool>,
    #[serde(default)]
    pub account_currency: Option<String>,
    #[serde(default)]
    pub cash: Option<Money>,
    #[serde(default)]
    pub buying_power: Option<Money>,
    #[serde(default)]
    pub day_pnl: Option<Money>,
    #[serde(default)]
    pub realized_pnl: Option<Money>,
    #[serde(default)]
    pub unrealized_pnl: Option<Money>,
    #[serde(default)]
    pub net_liquidation: Option<Money>,
    #[serde(default)]
    pub equity_with_loan: Option<Money>,
    #[serde(default)]
    pub initial_margin: Option<Money>,
    #[serde(default)]
    pub maintenance_margin: Option<Money>,
    #[serde(default)]
    pub excess_liquidity: Option<Money>,
    #[serde(default)]
    pub available_funds: Option<Money>,
    #[serde(default)]
    pub sma: Option<Money>,
    #[serde(default)]
    pub day_trades_remaining: Option<i32>,
    #[serde(default)]
    pub pdt_status: Option<String>,
    #[serde(default)]
    pub trading_restriction: Option<String>,
    #[serde(default)]
    pub settled_cash: Option<Money>,
    #[serde(default)]
    pub unsettled_cash: Option<Money>,
    #[serde(default)]
    pub gross_exposure: Option<Money>,
    #[serde(default)]
    pub net_exposure: Option<Money>,
    #[serde(default)]
    pub long_market_value: Option<Money>,
    #[serde(default)]
    pub short_market_value: Option<Money>,
    #[serde(default)]
    pub exposure_pct: Option<f64>,
    #[serde(default)]
    pub margin_usage_pct: Option<f64>,
    #[serde(default)]
    pub short_permission: Option<bool>,
    #[serde(default)]
    pub short_intents_blocked_today: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StrategyHeartbeat {
    pub strategy_id: String,
    pub state: String,
    pub mode: String,
    pub heartbeat_lag_ms: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StrategyHealthUpdated {
    pub strategy_id: String,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub trading_window: Option<String>,
    #[serde(default)]
    pub current_phase: Option<String>,
    #[serde(default)]
    pub universe_version: Option<String>,
    #[serde(default)]
    pub universe_count: Option<u64>,
    #[serde(default)]
    pub active_symbol_count: Option<u64>,
    #[serde(default)]
    pub watched_symbol_count: Option<u64>,
    #[serde(default)]
    pub l2_allocated_symbol_count: Option<u64>,
    #[serde(default)]
    pub signal_rate_1m: Option<f64>,
    #[serde(default)]
    pub reject_rate_1m: Option<f64>,
    #[serde(default)]
    pub fill_rate_1m: Option<f64>,
    #[serde(default)]
    pub cancel_rate_1m: Option<f64>,
    #[serde(default)]
    pub avg_intent_to_submit_ms: Option<u64>,
    #[serde(default)]
    pub avg_submit_to_ack_ms: Option<u64>,
    #[serde(default)]
    pub avg_ack_to_fill_ms: Option<u64>,
    #[serde(default)]
    pub consecutive_stops: Option<u64>,
    #[serde(default)]
    pub trades_today: Option<u64>,
    #[serde(default)]
    pub max_trades_today: Option<u64>,
    #[serde(default)]
    pub daily_loss_used_pct: Option<f64>,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
    #[serde(default)]
    pub risk_gates: Vec<StrategyRiskGateProjection>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StrategyRiskGateProjection {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StrategyStateChanged {
    pub strategy_id: String,
    pub state: String,
    pub mode: String,
    pub reason: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SignalGenerated {
    pub correlation_id: String,
    pub strategy_id: String,
    pub symbol: String,
    pub signal_name: String,
    pub score: Option<f64>,
    pub reason: String,
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default)]
    pub side_hint: Option<String>,
    #[serde(default)]
    pub horizon_ms: Option<u64>,
    #[serde(default)]
    pub expected_edge_bps: Option<f64>,
    #[serde(default)]
    pub confidence: Option<f64>,
    #[serde(default)]
    pub feature_version: Option<String>,
    #[serde(default)]
    pub model_version: Option<String>,
    #[serde(default)]
    pub market_snapshot_id: Option<String>,
    #[serde(default)]
    pub reference_price: Option<Price>,
    #[serde(default)]
    pub bid_price: Option<Price>,
    #[serde(default)]
    pub ask_price: Option<Price>,
    #[serde(default)]
    pub spread_bps: Option<f64>,
    #[serde(default)]
    pub imbalance: Option<f64>,
    #[serde(default)]
    pub microprice: Option<Price>,
    #[serde(default)]
    pub volatility_bps: Option<f64>,
    #[serde(default)]
    pub liquidity_score: Option<f64>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct IntentCreated {
    pub correlation_id: String,
    pub strategy_id: String,
    pub symbol: String,
    pub side: String,
    pub quantity: i64,
    pub reason: String,
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default)]
    pub intent_id: Option<String>,
    #[serde(default)]
    pub parent_intent_id: Option<String>,
    #[serde(default)]
    pub instrument_id: Option<String>,
    #[serde(default)]
    pub asset_class: Option<String>,
    #[serde(default)]
    pub currency: Option<String>,
    #[serde(default)]
    pub quantity_type: Option<String>,
    #[serde(default)]
    pub notional: Option<Money>,
    #[serde(default)]
    pub limit_price_hint: Option<Price>,
    #[serde(default)]
    pub stop_price_hint: Option<Price>,
    #[serde(default)]
    pub time_in_force_hint: Option<String>,
    #[serde(default)]
    pub urgency: Option<String>,
    #[serde(default)]
    pub position_effect: Option<String>,
    #[serde(default)]
    pub max_slippage_bps: Option<f64>,
    #[serde(default)]
    pub expires_at_ns: Option<i64>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RiskDecisionMade {
    pub correlation_id: String,
    pub strategy_id: String,
    pub symbol: String,
    pub approved: bool,
    pub reason_codes: Vec<String>,
    #[serde(default)]
    pub decision_id: Option<String>,
    #[serde(default)]
    pub intent_id: Option<String>,
    #[serde(default)]
    pub severity: Option<String>,
    #[serde(default)]
    pub evaluated_rules: Vec<RiskRuleEval>,
    #[serde(default)]
    pub risk_snapshot_id: Option<String>,
    #[serde(default)]
    pub account_day_pnl: Option<Money>,
    #[serde(default)]
    pub strategy_day_pnl: Option<Money>,
    #[serde(default)]
    pub symbol_exposure: Option<Money>,
    #[serde(default)]
    pub account_exposure: Option<Money>,
    #[serde(default)]
    pub remaining_trade_budget: Option<i64>,
    #[serde(default)]
    pub remaining_loss_budget: Option<Money>,
    #[serde(default)]
    pub market_data_age_ms: Option<u64>,
    #[serde(default)]
    pub quote_staleness_ms: Option<u64>,
    #[serde(default)]
    pub short_permission: Option<bool>,
    #[serde(default)]
    pub authority_policy_version: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RiskRuleEval {
    pub rule_id: String,
    pub rule_name: String,
    pub passed: bool,
    pub observed: String,
    pub threshold: String,
    pub unit: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct OrderSubmitRequested {
    pub correlation_id: String,
    pub account_id: String,
    pub order_id: String,
    pub order_type: String,
    pub limit_price: Option<Price>,
    pub tif: String,
    #[serde(default)]
    pub client_order_id: Option<String>,
    #[serde(default)]
    pub broker_order_id: Option<String>,
    #[serde(default)]
    pub perm_id: Option<String>,
    #[serde(default)]
    pub parent_order_id: Option<String>,
    #[serde(default)]
    pub oca_group: Option<String>,
    #[serde(default)]
    pub route: Option<String>,
    #[serde(default)]
    pub destination: Option<String>,
    #[serde(default)]
    pub exchange: Option<String>,
    #[serde(default)]
    pub order_ref: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub quantity: Option<i64>,
    #[serde(default)]
    pub remaining_quantity: Option<i64>,
    #[serde(default)]
    pub stop_price: Option<Price>,
    #[serde(default)]
    pub aux_price: Option<Price>,
    #[serde(default)]
    pub outside_rth: bool,
    #[serde(default)]
    pub extended_hours: bool,
    #[serde(default)]
    pub allow_preopen: bool,
    #[serde(default)]
    pub allow_after_hours: bool,
    #[serde(default)]
    pub min_qty: Option<i64>,
    #[serde(default)]
    pub display_size: Option<i64>,
    #[serde(default)]
    pub discretionary_amount: Option<Price>,
    #[serde(default = "default_transmit")]
    pub transmit: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct OrderSubmitted {
    pub correlation_id: String,
    pub account_id: String,
    pub order_id: String,
    pub broker: String,
    #[serde(default)]
    pub client_order_id: Option<String>,
    #[serde(default)]
    pub broker_order_id: Option<String>,
    #[serde(default)]
    pub perm_id: Option<String>,
    #[serde(default)]
    pub route: Option<String>,
    #[serde(default)]
    pub exchange: Option<String>,
    #[serde(default)]
    pub destination: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct BrokerAckReceived {
    pub correlation_id: String,
    pub account_id: String,
    pub order_id: String,
    pub broker_order_id: String,
    pub broker_status: String,
    #[serde(default)]
    pub perm_id: Option<String>,
    #[serde(default)]
    pub remaining_quantity: Option<i64>,
    #[serde(default)]
    pub receive_ts_ns: Option<i64>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct OrderFill {
    pub correlation_id: String,
    pub account_id: String,
    pub order_id: String,
    #[serde(default)]
    pub execution_id: Option<String>,
    #[serde(default)]
    pub broker_execution_id: Option<String>,
    pub filled_quantity: i64,
    pub fill_price: Price,
    #[serde(default)]
    pub last_quantity: Option<i64>,
    #[serde(default)]
    pub cumulative_quantity: Option<i64>,
    #[serde(default)]
    pub remaining_quantity: Option<i64>,
    #[serde(default)]
    pub last_price: Option<Price>,
    #[serde(default)]
    pub average_price: Option<Price>,
    #[serde(default)]
    pub venue: Option<String>,
    #[serde(default)]
    pub liquidity: Option<String>,
    #[serde(default)]
    pub commission: Option<Money>,
    #[serde(default)]
    pub fees: Vec<Fee>,
    #[serde(default)]
    pub trade_ts_ns: Option<i64>,
    #[serde(default)]
    pub report_ts_ns: Option<i64>,
    #[serde(default)]
    pub settlement_currency: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Fee {
    pub name: String,
    pub amount: Money,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CancelRequested {
    pub correlation_id: String,
    pub account_id: String,
    pub order_id: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CancelRejected {
    pub correlation_id: String,
    pub account_id: String,
    pub order_id: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderCancelled {
    pub correlation_id: String,
    pub account_id: String,
    pub order_id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderRejected {
    pub correlation_id: String,
    pub account_id: String,
    pub order_id: String,
    pub reason: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PositionSnapshot {
    pub account_id: String,
    pub symbol: String,
    pub net_quantity: i64,
    pub average_price: Price,
    pub market_price: Price,
    pub strategy_attribution: Vec<StrategyPositionAttribution>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StrategyPositionAttribution {
    pub strategy_id: String,
    pub quantity: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RiskLimitBreached {
    pub scope: String,
    pub severity: String,
    pub message: String,
    #[serde(default)]
    pub block_id: Option<String>,
    #[serde(default)]
    pub rule_id: Option<String>,
    #[serde(default)]
    pub first_seen_ts_ns: Option<i64>,
    #[serde(default)]
    pub last_seen_ts_ns: Option<i64>,
    #[serde(default)]
    pub cleared_ts_ns: Option<i64>,
    #[serde(default)]
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default)]
    pub strategy_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AlertRaised {
    pub alert_id: String,
    pub severity: String,
    pub domain: String,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AlertAcknowledged {
    pub alert_id: String,
    pub operator_id: String,
    pub reason: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct IngestDiagnosticRecorded {
    pub source: String,
    #[serde(default)]
    pub stream: Option<String>,
    #[serde(default)]
    pub consumer: Option<String>,
    #[serde(default)]
    pub subject: Option<String>,
    pub severity: String,
    pub message: String,
    #[serde(default)]
    pub error_kind: Option<String>,
    #[serde(default)]
    pub reconnect: bool,
    #[serde(default)]
    pub decode_error: bool,
    #[serde(default)]
    pub filtered_count: u64,
    #[serde(default)]
    pub acked_count: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CommandAuthorityDecided {
    pub decision_id: String,
    pub command_id: String,
    pub status: String,
    pub reason_codes: Vec<String>,
    pub matched_policy_ids: Vec<String>,
    pub operator_id: String,
    pub command_type: String,
    pub capability: String,
    pub scope: String,
    #[serde(default)]
    pub approved_by: Vec<String>,
    pub decided_ts_ns: i64,
    #[serde(default)]
    pub authority_policy_version: String,
    #[serde(default)]
    pub target_environment: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommandAuditRecorded {
    pub command_id: String,
    pub operator_id: String,
    pub command_type: String,
    pub status: String,
    pub reason: String,
    #[serde(default)]
    pub target: Option<String>,
}

fn default_transmit() -> bool {
    true
}
