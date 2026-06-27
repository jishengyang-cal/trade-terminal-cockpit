use serde::{Deserialize, Serialize};

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
            payload,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum DomainEvent {
    StrategyHeartbeat(StrategyHeartbeat),
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
    CommandAuditRecorded(CommandAuditRecorded),
}

impl DomainEvent {
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::StrategyHeartbeat(_) => "StrategyHeartbeat",
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
            Self::CommandAuditRecorded(_) => "CommandAuditRecorded",
        }
    }

    pub fn aggregate_type(&self) -> &'static str {
        match self {
            Self::StrategyHeartbeat(_) | Self::StrategyStateChanged(_) => "strategy",
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
            Self::CommandAuditRecorded(_) => "command",
        }
    }

    pub fn aggregate_id(&self) -> String {
        match self {
            Self::StrategyHeartbeat(event) => event.strategy_id.clone(),
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
            Self::CommandAuditRecorded(event) => event.command_id.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StrategyHeartbeat {
    pub strategy_id: String,
    pub state: String,
    pub mode: String,
    pub heartbeat_lag_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StrategyStateChanged {
    pub strategy_id: String,
    pub state: String,
    pub mode: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SignalGenerated {
    pub correlation_id: String,
    pub strategy_id: String,
    pub symbol: String,
    pub signal_name: String,
    pub score: Option<f64>,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntentCreated {
    pub correlation_id: String,
    pub strategy_id: String,
    pub symbol: String,
    pub side: String,
    pub quantity: i64,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RiskDecisionMade {
    pub correlation_id: String,
    pub strategy_id: String,
    pub symbol: String,
    pub approved: bool,
    pub reason_codes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderSubmitRequested {
    pub correlation_id: String,
    pub account_id: String,
    pub order_id: String,
    pub order_type: String,
    pub limit_price: Option<f64>,
    pub tif: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderSubmitted {
    pub correlation_id: String,
    pub account_id: String,
    pub order_id: String,
    pub broker: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BrokerAckReceived {
    pub correlation_id: String,
    pub account_id: String,
    pub order_id: String,
    pub broker_order_id: String,
    pub broker_status: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderFill {
    pub correlation_id: String,
    pub account_id: String,
    pub order_id: String,
    pub filled_quantity: i64,
    pub fill_price: f64,
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PositionSnapshot {
    pub account_id: String,
    pub symbol: String,
    pub net_quantity: i64,
    pub average_price: f64,
    pub market_price: f64,
    pub strategy_attribution: Vec<StrategyPositionAttribution>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StrategyPositionAttribution {
    pub strategy_id: String,
    pub quantity: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RiskLimitBreached {
    pub scope: String,
    pub severity: String,
    pub message: String,
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommandAuditRecorded {
    pub command_id: String,
    pub operator_id: String,
    pub command_type: String,
    pub status: String,
    pub reason: String,
}
