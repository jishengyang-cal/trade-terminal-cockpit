use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AppState {
    pub connection: ConnectionState,
    pub account: AccountView,
    pub strategies: StrategyStore,
    pub orders: OrderStore,
    pub positions: PositionStore,
    pub risk: RiskView,
    pub alerts: AlertStore,
    pub audit: EventRingBuffer,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            connection: ConnectionState::default(),
            account: AccountView::default(),
            strategies: StrategyStore::default(),
            orders: OrderStore::default(),
            positions: PositionStore::default(),
            risk: RiskView::default(),
            alerts: AlertStore::default(),
            audit: EventRingBuffer::new(500),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConnectionState {
    pub nats: String,
    pub command_gateway: String,
    pub event_lag_ms: u64,
    pub render_fps: u16,
    pub last_event_sequence: Option<u64>,
    pub last_event_ts_ns: Option<i64>,
    pub events_ingested: u64,
    pub events_coalesced: u64,
    pub audit_events_retained: usize,
    pub dropped_market_updates: u64,
    pub nats_reconnect_count: u64,
    pub command_roundtrip_ms: u64,
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self {
            nats: "disconnected".to_string(),
            command_gateway: "disabled".to_string(),
            event_lag_ms: 0,
            render_fps: 20,
            last_event_sequence: None,
            last_event_ts_ns: None,
            events_ingested: 0,
            events_coalesced: 0,
            audit_events_retained: 0,
            dropped_market_updates: 0,
            nats_reconnect_count: 0,
            command_roundtrip_ms: 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AccountView {
    pub account_id: String,
    pub mode: String,
    pub broker: String,
    pub broker_connected: bool,
    pub cash: f64,
    pub buying_power: f64,
    pub day_pnl: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub exposure_pct: f64,
    pub margin_usage_pct: f64,
    pub short_permission: bool,
    pub short_intents_blocked_today: u64,
}

impl Default for AccountView {
    fn default() -> Self {
        Self {
            account_id: "paper".to_string(),
            mode: "PAPER".to_string(),
            broker: "unknown".to_string(),
            broker_connected: false,
            cash: 0.0,
            buying_power: 0.0,
            day_pnl: 0.0,
            realized_pnl: 0.0,
            unrealized_pnl: 0.0,
            exposure_pct: 0.0,
            margin_usage_pct: 0.0,
            short_permission: false,
            short_intents_blocked_today: 0,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StrategyStore {
    pub by_id: BTreeMap<String, StrategyView>,
}

impl StrategyStore {
    pub fn get_or_insert(&mut self, strategy_id: &str) -> &mut StrategyView {
        self.by_id
            .entry(strategy_id.to_string())
            .or_insert_with(|| StrategyView::new(strategy_id))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StrategyView {
    pub strategy_id: String,
    pub state: String,
    pub mode: String,
    pub universe_count: u64,
    pub signals: u64,
    pub intents: u64,
    pub orders: u64,
    pub pnl: f64,
    pub heartbeat_lag_ms: Option<u64>,
    pub last_event_sequence: Option<u64>,
    pub last_reason: Option<String>,
    #[serde(default)]
    pub last_signal_sequence: Option<u64>,
    #[serde(default)]
    pub last_intent_sequence: Option<u64>,
    #[serde(default)]
    pub last_order_sequence: Option<u64>,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
    #[serde(default)]
    pub risk_gates: Vec<StrategyRiskGateView>,
}

impl StrategyView {
    pub fn new(strategy_id: &str) -> Self {
        Self {
            strategy_id: strategy_id.to_string(),
            state: "UNKNOWN".to_string(),
            mode: "PAPER".to_string(),
            universe_count: 0,
            signals: 0,
            intents: 0,
            orders: 0,
            pnl: 0.0,
            heartbeat_lag_ms: None,
            last_event_sequence: None,
            last_reason: None,
            last_signal_sequence: None,
            last_intent_sequence: None,
            last_order_sequence: None,
            parameters: BTreeMap::new(),
            risk_gates: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StrategyRiskGateView {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct OrderStore {
    pub by_correlation_id: BTreeMap<String, OrderChain>,
    pub order_id_index: BTreeMap<String, String>,
}

impl OrderStore {
    pub fn get_or_insert_chain(&mut self, correlation_id: &str) -> &mut OrderChain {
        self.by_correlation_id
            .entry(correlation_id.to_string())
            .or_insert_with(|| OrderChain::new(correlation_id))
    }

    pub fn index_order(&mut self, account_id: &str, order_id: &str, correlation_id: &str) {
        self.order_id_index.insert(
            format!("{account_id}:{order_id}"),
            correlation_id.to_string(),
        );
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderChain {
    pub correlation_id: String,
    pub strategy_id: Option<String>,
    pub account_id: Option<String>,
    pub symbol: Option<String>,
    pub side: Option<String>,
    pub intended_quantity: Option<i64>,
    pub state: OrderLifecycleState,
    pub order_id: Option<String>,
    pub broker: Option<String>,
    pub broker_order_id: Option<String>,
    pub broker_status: Option<String>,
    pub order_type: Option<String>,
    pub limit_price: Option<f64>,
    pub tif: Option<String>,
    pub risk: Option<RiskDecisionView>,
    pub filled_quantity: i64,
    pub average_fill_price: Option<f64>,
    pub timeline: Vec<TimelineEntry>,
}

impl OrderChain {
    pub fn new(correlation_id: &str) -> Self {
        Self {
            correlation_id: correlation_id.to_string(),
            strategy_id: None,
            account_id: None,
            symbol: None,
            side: None,
            intended_quantity: None,
            state: OrderLifecycleState::Observed,
            order_id: None,
            broker: None,
            broker_order_id: None,
            broker_status: None,
            order_type: None,
            limit_price: None,
            tif: None,
            risk: None,
            filled_quantity: 0,
            average_fill_price: None,
            timeline: Vec::new(),
        }
    }

    pub fn push_timeline(
        &mut self,
        sequence: u64,
        ts_ns: i64,
        kind: impl Into<String>,
        summary: impl Into<String>,
    ) {
        self.timeline.push(TimelineEntry {
            sequence,
            ts_ns,
            kind: kind.into(),
            summary: summary.into(),
        });
    }

    pub fn apply_fill(&mut self, quantity: i64, price: f64) {
        let previous_quantity = self.filled_quantity;
        let previous_notional = self.average_fill_price.unwrap_or(0.0) * previous_quantity as f64;
        let new_quantity = previous_quantity + quantity;
        if new_quantity > 0 {
            let new_notional = previous_notional + price * quantity as f64;
            self.average_fill_price = Some(new_notional / new_quantity as f64);
        }
        self.filled_quantity = new_quantity;
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderLifecycleState {
    #[default]
    Observed,
    SignalGenerated,
    IntentCreated,
    RiskPending,
    RiskRejected,
    RiskApproved,
    SubmitRequested,
    SubmittedToBroker,
    BrokerAckReceived,
    PartiallyFilled,
    Filled,
    CancelRequested,
    CancelRejected,
    Cancelled,
    BrokerRejected,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RiskDecisionView {
    pub approved: bool,
    pub reason_codes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TimelineEntry {
    pub sequence: u64,
    pub ts_ns: i64,
    pub kind: String,
    pub summary: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PositionStore {
    pub by_key: BTreeMap<String, PositionView>,
}

impl PositionStore {
    pub fn upsert(&mut self, position: PositionView) {
        self.by_key.insert(position.key.clone(), position);
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PositionView {
    pub key: String,
    pub account_id: String,
    pub symbol: String,
    pub net_quantity: i64,
    pub average_price: f64,
    pub market_price: f64,
    pub unrealized_pnl: f64,
    pub strategy_attribution: Vec<StrategyPositionView>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StrategyPositionView {
    pub strategy_id: String,
    pub quantity: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RiskView {
    pub global_state: String,
    pub kill_switch_active: bool,
    pub market_data_fresh: bool,
    pub broker_order_channel_ok: bool,
    pub day_max_loss_breached: bool,
    pub quote_staleness_ok: bool,
    pub short_permission: bool,
    pub limits: BTreeMap<String, String>,
    pub active_blocks: Vec<RiskBlock>,
}

impl Default for RiskView {
    fn default() -> Self {
        let mut limits = BTreeMap::new();
        limits.insert("day_max_loss_pct".to_string(), "1.00%".to_string());
        limits.insert("strategy_max_consecutive_stop".to_string(), "3".to_string());
        limits.insert("max_trades_per_symbol_day".to_string(), "3".to_string());
        limits.insert("max_order_notional".to_string(), "1500".to_string());
        limits.insert("max_position_notional".to_string(), "3000".to_string());
        Self {
            global_state: "NORMAL".to_string(),
            kill_switch_active: false,
            market_data_fresh: true,
            broker_order_channel_ok: false,
            day_max_loss_breached: false,
            quote_staleness_ok: true,
            short_permission: false,
            limits,
            active_blocks: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RiskBlock {
    pub scope: String,
    pub severity: String,
    pub message: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AlertStore {
    pub by_id: BTreeMap<String, AlertView>,
}

impl AlertStore {
    pub fn open_count(&self) -> usize {
        self.by_id
            .values()
            .filter(|alert| !alert.acknowledged)
            .count()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AlertView {
    pub alert_id: String,
    pub severity: String,
    pub domain: String,
    pub message: String,
    pub acknowledged: bool,
    pub acknowledged_by: Option<String>,
    pub acknowledge_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventRingBuffer {
    capacity: usize,
    pub events: VecDeque<EventSummary>,
}

impl EventRingBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            events: VecDeque::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, event: EventSummary) {
        if self.events.len() == self.capacity {
            self.events.pop_front();
        }
        self.events.push_back(event);
    }

    pub fn push_or_replace_coalesced(&mut self, event: EventSummary) -> bool {
        if let Some(existing) = self.events.iter_mut().rev().find(|existing| {
            existing.event_type == event.event_type && existing.aggregate_id == event.aggregate_id
        }) {
            *existing = event;
            return true;
        }

        self.push(event);
        false
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }
}

impl Default for EventRingBuffer {
    fn default() -> Self {
        Self::new(500)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventSummary {
    pub sequence: u64,
    pub ts_ns: i64,
    pub event_type: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub correlation_id: String,
    pub producer: String,
    pub headline: String,
}
