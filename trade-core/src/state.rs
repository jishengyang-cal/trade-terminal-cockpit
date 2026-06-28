use crate::events::RiskRuleEval;
use crate::types::{LatencyBreakdown, Money, Price};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AppState {
    pub connection: ConnectionState,
    pub account: AccountView,
    #[serde(default)]
    pub accounts: AccountStore,
    #[serde(default)]
    pub commands: CommandStore,
    pub strategies: StrategyStore,
    pub orders: OrderStore,
    pub positions: PositionStore,
    pub risk: RiskView,
    pub alerts: AlertStore,
    pub audit: EventRingBuffer,
    #[serde(default, skip_serializing)]
    pub seen_event_ids: BTreeSet<String>,
    #[serde(default, skip_serializing)]
    pub last_sequence_by_producer: BTreeMap<String, u64>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            connection: ConnectionState::default(),
            account: AccountView::default(),
            accounts: AccountStore::default(),
            commands: CommandStore::default(),
            strategies: StrategyStore::default(),
            orders: OrderStore::default(),
            positions: PositionStore::default(),
            risk: RiskView::default(),
            alerts: AlertStore::default(),
            audit: EventRingBuffer::new(500),
            seen_event_ids: BTreeSet::new(),
            last_sequence_by_producer: BTreeMap::new(),
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
    #[serde(default)]
    pub duplicate_events: u64,
    #[serde(default)]
    pub out_of_order_events: u64,
    #[serde(default)]
    pub sequence_gaps: u64,
    #[serde(default)]
    pub event_backlog: u64,
    #[serde(default)]
    pub last_source_ts_ns: Option<i64>,
    #[serde(default)]
    pub last_ingest_ts_ns: Option<i64>,
    #[serde(default)]
    pub last_publish_ts_ns: Option<i64>,
    #[serde(default)]
    pub source_to_ingest_lag_ms: Option<u64>,
    #[serde(default)]
    pub ingest_to_render_lag_ms: Option<u64>,
    #[serde(default)]
    pub clock_skew_ms: Option<i64>,
    #[serde(default)]
    pub stream_name: Option<String>,
    #[serde(default)]
    pub consumer_name: Option<String>,
    #[serde(default)]
    pub last_nats_subject: Option<String>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub last_disconnect_ts_ns: Option<i64>,
    #[serde(default)]
    pub last_reconnect_ts_ns: Option<i64>,
    #[serde(default)]
    pub ingest_errors: u64,
    #[serde(default)]
    pub decode_errors: u64,
    #[serde(default)]
    pub filtered_events: u64,
    #[serde(default)]
    pub jetstream_acks: u64,
    #[serde(default)]
    pub events_drained_last_tick: u64,
    #[serde(default)]
    pub max_drain_per_tick: u64,
    #[serde(default)]
    pub render_slow_frames: u64,
    #[serde(default)]
    pub last_render_duration_ms: u64,
    #[serde(default)]
    pub event_rx_backlog_estimate: Option<u64>,
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
            duplicate_events: 0,
            out_of_order_events: 0,
            sequence_gaps: 0,
            event_backlog: 0,
            last_source_ts_ns: None,
            last_ingest_ts_ns: None,
            last_publish_ts_ns: None,
            source_to_ingest_lag_ms: None,
            ingest_to_render_lag_ms: None,
            clock_skew_ms: None,
            stream_name: None,
            consumer_name: None,
            last_nats_subject: None,
            last_error: None,
            last_disconnect_ts_ns: None,
            last_reconnect_ts_ns: None,
            ingest_errors: 0,
            decode_errors: 0,
            filtered_events: 0,
            jetstream_acks: 0,
            events_drained_last_tick: 0,
            max_drain_per_tick: 5_000,
            render_slow_frames: 0,
            last_render_duration_ms: 0,
            event_rx_backlog_estimate: None,
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
    #[serde(default)]
    pub cash_value: Money,
    pub buying_power: f64,
    #[serde(default)]
    pub buying_power_value: Money,
    pub day_pnl: f64,
    #[serde(default)]
    pub day_pnl_value: Money,
    pub realized_pnl: f64,
    #[serde(default)]
    pub realized_pnl_value: Money,
    pub unrealized_pnl: f64,
    #[serde(default)]
    pub unrealized_pnl_value: Money,
    pub exposure_pct: f64,
    pub margin_usage_pct: f64,
    pub short_permission: bool,
    pub short_intents_blocked_today: u64,
    #[serde(default)]
    pub runtime_controls: AccountRuntimeControls,
    #[serde(default)]
    pub account_currency: String,
    #[serde(default)]
    pub net_liquidation: Money,
    #[serde(default)]
    pub equity_with_loan: Money,
    #[serde(default)]
    pub initial_margin: Money,
    #[serde(default)]
    pub maintenance_margin: Money,
    #[serde(default)]
    pub excess_liquidity: Money,
    #[serde(default)]
    pub available_funds: Money,
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
    pub gross_exposure: Money,
    #[serde(default)]
    pub net_exposure: Money,
    #[serde(default)]
    pub long_market_value: Money,
    #[serde(default)]
    pub short_market_value: Money,
}

impl Default for AccountView {
    fn default() -> Self {
        Self::new("paper")
    }
}

impl AccountView {
    pub fn new(account_id: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            mode: "PAPER".to_string(),
            broker: "unknown".to_string(),
            broker_connected: false,
            cash: 0.0,
            cash_value: Money::default(),
            buying_power: 0.0,
            buying_power_value: Money::default(),
            day_pnl: 0.0,
            day_pnl_value: Money::default(),
            realized_pnl: 0.0,
            realized_pnl_value: Money::default(),
            unrealized_pnl: 0.0,
            unrealized_pnl_value: Money::default(),
            exposure_pct: 0.0,
            margin_usage_pct: 0.0,
            short_permission: false,
            short_intents_blocked_today: 0,
            runtime_controls: AccountRuntimeControls::default(),
            account_currency: "USD".to_string(),
            net_liquidation: Money::default(),
            equity_with_loan: Money::default(),
            initial_margin: Money::default(),
            maintenance_margin: Money::default(),
            excess_liquidity: Money::default(),
            available_funds: Money::default(),
            sma: None,
            day_trades_remaining: None,
            pdt_status: None,
            trading_restriction: None,
            settled_cash: None,
            unsettled_cash: None,
            gross_exposure: Money::default(),
            net_exposure: Money::default(),
            long_market_value: Money::default(),
            short_market_value: Money::default(),
        }
    }

    pub fn sync_legacy_money_from_f64(&mut self) {
        let currency = if self.account_currency.is_empty() {
            "USD".to_string()
        } else {
            self.account_currency.clone()
        };
        self.cash_value = Money::from_f64(self.cash, currency.clone());
        self.buying_power_value = Money::from_f64(self.buying_power, currency.clone());
        self.day_pnl_value = Money::from_f64(self.day_pnl, currency.clone());
        self.realized_pnl_value = Money::from_f64(self.realized_pnl, currency.clone());
        self.unrealized_pnl_value = Money::from_f64(self.unrealized_pnl, currency);
    }

    pub fn sync_legacy_f64_from_money(&mut self) {
        self.cash = self.cash_value.as_f64();
        self.buying_power = self.buying_power_value.as_f64();
        self.day_pnl = self.day_pnl_value.as_f64();
        self.realized_pnl = self.realized_pnl_value.as_f64();
        self.unrealized_pnl = self.unrealized_pnl_value.as_f64();
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AccountRuntimeControls {
    pub entry_disabled: bool,
    pub reduce_only: bool,
    pub flatten_only: bool,
    pub cancel_all: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AccountStore {
    pub by_id: BTreeMap<String, AccountView>,
}

impl Default for AccountStore {
    fn default() -> Self {
        Self {
            by_id: BTreeMap::new(),
        }
    }
}

impl AccountStore {
    pub fn get_or_insert(&mut self, account_id: &str) -> &mut AccountView {
        self.by_id
            .entry(account_id.to_string())
            .or_insert_with(|| AccountView::new(account_id))
    }

    pub fn selected_or_first(&self, selected_index: usize) -> Option<&AccountView> {
        self.by_id
            .values()
            .nth(selected_index)
            .or_else(|| self.by_id.values().next())
    }

    pub fn selected_account_id(&self, selected_index: usize) -> String {
        self.selected_or_first(selected_index)
            .map(|account| account.account_id.clone())
            .unwrap_or_else(|| AccountView::default().account_id)
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn aggregate_view(&self) -> AccountView {
        let mut values = self.by_id.values();
        let Some(first) = values.next() else {
            return AccountView::default();
        };
        if self.by_id.len() == 1 {
            return first.clone();
        }

        let mut aggregate = AccountView::new(&format!("ALL({})", self.by_id.len()));
        aggregate.mode = "MULTI".to_string();
        aggregate.broker = "mixed".to_string();
        aggregate.broker_connected = self.by_id.values().all(|account| account.broker_connected);
        aggregate.cash = self.by_id.values().map(|account| account.cash).sum();
        aggregate.cash_value = sum_money(self.by_id.values().map(|account| &account.cash_value));
        aggregate.buying_power = self
            .by_id
            .values()
            .map(|account| account.buying_power)
            .sum();
        aggregate.buying_power_value = sum_money(
            self.by_id
                .values()
                .map(|account| &account.buying_power_value),
        );
        aggregate.day_pnl = self.by_id.values().map(|account| account.day_pnl).sum();
        aggregate.day_pnl_value =
            sum_money(self.by_id.values().map(|account| &account.day_pnl_value));
        aggregate.realized_pnl = self
            .by_id
            .values()
            .map(|account| account.realized_pnl)
            .sum();
        aggregate.realized_pnl_value = sum_money(
            self.by_id
                .values()
                .map(|account| &account.realized_pnl_value),
        );
        aggregate.unrealized_pnl = self
            .by_id
            .values()
            .map(|account| account.unrealized_pnl)
            .sum();
        aggregate.unrealized_pnl_value = sum_money(
            self.by_id
                .values()
                .map(|account| &account.unrealized_pnl_value),
        );
        aggregate.exposure_pct = self
            .by_id
            .values()
            .map(|account| account.exposure_pct)
            .fold(0.0, f64::max);
        aggregate.margin_usage_pct = self
            .by_id
            .values()
            .map(|account| account.margin_usage_pct)
            .fold(0.0, f64::max);
        aggregate.short_permission = self.by_id.values().all(|account| account.short_permission);
        aggregate.short_intents_blocked_today = self
            .by_id
            .values()
            .map(|account| account.short_intents_blocked_today)
            .sum();
        aggregate.runtime_controls.entry_disabled = self
            .by_id
            .values()
            .any(|account| account.runtime_controls.entry_disabled);
        aggregate.runtime_controls.reduce_only = self
            .by_id
            .values()
            .any(|account| account.runtime_controls.reduce_only);
        aggregate.runtime_controls.flatten_only = self
            .by_id
            .values()
            .any(|account| account.runtime_controls.flatten_only);
        aggregate.runtime_controls.cancel_all = self
            .by_id
            .values()
            .any(|account| account.runtime_controls.cancel_all);
        aggregate.account_currency =
            common_currency(self.by_id.values()).unwrap_or_else(|| "MIXED".to_string());
        aggregate.net_liquidation =
            sum_money(self.by_id.values().map(|account| &account.net_liquidation));
        aggregate.equity_with_loan =
            sum_money(self.by_id.values().map(|account| &account.equity_with_loan));
        aggregate.initial_margin =
            sum_money(self.by_id.values().map(|account| &account.initial_margin));
        aggregate.maintenance_margin = sum_money(
            self.by_id
                .values()
                .map(|account| &account.maintenance_margin),
        );
        aggregate.excess_liquidity =
            sum_money(self.by_id.values().map(|account| &account.excess_liquidity));
        aggregate.available_funds =
            sum_money(self.by_id.values().map(|account| &account.available_funds));
        aggregate.sma = sum_optional_money(self.by_id.values().map(|account| account.sma.as_ref()));
        aggregate.settled_cash = sum_optional_money(
            self.by_id
                .values()
                .map(|account| account.settled_cash.as_ref()),
        );
        aggregate.unsettled_cash = sum_optional_money(
            self.by_id
                .values()
                .map(|account| account.unsettled_cash.as_ref()),
        );
        aggregate.gross_exposure =
            sum_money(self.by_id.values().map(|account| &account.gross_exposure));
        aggregate.net_exposure =
            sum_money(self.by_id.values().map(|account| &account.net_exposure));
        aggregate.long_market_value = sum_money(
            self.by_id
                .values()
                .map(|account| &account.long_market_value),
        );
        aggregate.short_market_value = sum_money(
            self.by_id
                .values()
                .map(|account| &account.short_market_value),
        );
        aggregate.day_trades_remaining = self
            .by_id
            .values()
            .filter_map(|account| account.day_trades_remaining)
            .min();
        aggregate.pdt_status = if self
            .by_id
            .values()
            .any(|account| account.pdt_status.as_deref() == Some("restricted"))
        {
            Some("restricted".to_string())
        } else {
            None
        };
        let trading_restriction = self
            .by_id
            .values()
            .filter_map(|account| account.trading_restriction.clone())
            .collect::<Vec<_>>()
            .join(",");
        aggregate.trading_restriction = if trading_restriction.is_empty() {
            None
        } else {
            Some(trading_restriction)
        };
        aggregate
    }
}

fn sum_money<'a>(values: impl IntoIterator<Item = &'a Money>) -> Money {
    let mut total = Money::default();
    let mut initialized = false;
    for value in values {
        if !initialized {
            total = Money::new(0, value.scale, value.currency.clone());
            initialized = true;
        }
        if value.scale == total.scale && value.currency == total.currency {
            total.value += value.value;
        } else {
            total = Money::from_f64(total.as_f64() + value.as_f64(), total.currency.clone());
        }
    }
    total
}

fn sum_optional_money<'a>(values: impl IntoIterator<Item = Option<&'a Money>>) -> Option<Money> {
    let collected = values.into_iter().flatten().collect::<Vec<_>>();
    if collected.is_empty() {
        None
    } else {
        Some(sum_money(collected))
    }
}

fn common_currency<'a>(accounts: impl IntoIterator<Item = &'a AccountView>) -> Option<String> {
    let mut currency = None::<String>;
    for account in accounts {
        if account.account_currency.is_empty() {
            continue;
        }
        match currency.as_deref() {
            None => currency = Some(account.account_currency.clone()),
            Some(existing) if existing == account.account_currency => {}
            Some(_) => return None,
        }
    }
    currency
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CommandStore {
    pub by_id: BTreeMap<String, CommandEvidenceView>,
}

impl CommandStore {
    pub fn get_or_insert(&mut self, command_id: &str) -> &mut CommandEvidenceView {
        self.by_id
            .entry(command_id.to_string())
            .or_insert_with(|| CommandEvidenceView::new(command_id))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommandEvidenceView {
    pub command_id: String,
    pub operator_id: Option<String>,
    pub command_type: Option<String>,
    pub aggregate_id: Option<String>,
    pub audit_status: Option<String>,
    pub audit_reason: Option<String>,
    pub target: Option<String>,
    pub authority_decision_id: Option<String>,
    pub authority_status: Option<String>,
    pub reason_codes: Vec<String>,
    pub matched_policy_ids: Vec<String>,
    pub capability: Option<String>,
    pub scope: Option<String>,
    pub approved_by: Vec<String>,
    pub decided_ts_ns: Option<i64>,
    pub authority_policy_version: Option<String>,
    pub target_environment: Option<String>,
}

impl CommandEvidenceView {
    pub fn new(command_id: &str) -> Self {
        Self {
            command_id: command_id.to_string(),
            operator_id: None,
            command_type: None,
            aggregate_id: None,
            audit_status: None,
            audit_reason: None,
            target: None,
            authority_decision_id: None,
            authority_status: None,
            reason_codes: Vec::new(),
            matched_policy_ids: Vec::new(),
            capability: None,
            scope: None,
            approved_by: Vec::new(),
            decided_ts_ns: None,
            authority_policy_version: None,
            target_environment: None,
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
    #[serde(default = "default_strategy_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub trading_window: Option<String>,
    #[serde(default)]
    pub current_phase: String,
    #[serde(default)]
    pub universe_version: Option<String>,
    #[serde(default)]
    pub active_symbol_count: u64,
    #[serde(default)]
    pub watched_symbol_count: u64,
    #[serde(default)]
    pub l2_allocated_symbol_count: u64,
    #[serde(default)]
    pub signal_rate_1m: f64,
    #[serde(default)]
    pub reject_rate_1m: f64,
    #[serde(default)]
    pub fill_rate_1m: f64,
    #[serde(default)]
    pub cancel_rate_1m: f64,
    #[serde(default)]
    pub avg_intent_to_submit_ms: Option<u64>,
    #[serde(default)]
    pub avg_submit_to_ack_ms: Option<u64>,
    #[serde(default)]
    pub avg_ack_to_fill_ms: Option<u64>,
    #[serde(default)]
    pub consecutive_stops: u64,
    #[serde(default)]
    pub trades_today: u64,
    #[serde(default)]
    pub max_trades_today: u64,
    #[serde(default)]
    pub daily_loss_used_pct: f64,
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
            enabled: true,
            trading_window: None,
            current_phase: "unknown".to_string(),
            universe_version: None,
            active_symbol_count: 0,
            watched_symbol_count: 0,
            l2_allocated_symbol_count: 0,
            signal_rate_1m: 0.0,
            reject_rate_1m: 0.0,
            fill_rate_1m: 0.0,
            cancel_rate_1m: 0.0,
            avg_intent_to_submit_ms: None,
            avg_submit_to_ack_ms: None,
            avg_ack_to_fill_ms: None,
            consecutive_stops: 0,
            trades_today: 0,
            max_trades_today: 0,
            daily_loss_used_pct: 0.0,
        }
    }
}

fn default_strategy_enabled() -> bool {
    true
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
    #[serde(default)]
    pub client_order_id: Option<String>,
    pub broker_order_id: Option<String>,
    #[serde(default)]
    pub perm_id: Option<String>,
    #[serde(default)]
    pub parent_order_id: Option<String>,
    #[serde(default)]
    pub child_order_ids: Vec<String>,
    pub broker_status: Option<String>,
    pub order_type: Option<String>,
    pub limit_price: Option<Price>,
    #[serde(default)]
    pub stop_price: Option<Price>,
    pub tif: Option<String>,
    #[serde(default)]
    pub route: Option<String>,
    #[serde(default)]
    pub exchange: Option<String>,
    #[serde(default)]
    pub destination: Option<String>,
    #[serde(default)]
    pub currency: String,
    #[serde(default)]
    pub submitted_quantity: Option<i64>,
    #[serde(default)]
    pub remaining_quantity: Option<i64>,
    #[serde(default)]
    pub cancelled_quantity: Option<i64>,
    #[serde(default)]
    pub rejected_quantity: Option<i64>,
    pub risk: Option<RiskDecisionView>,
    pub filled_quantity: i64,
    pub average_fill_price: Option<Price>,
    #[serde(default)]
    pub last_fill_price: Option<Price>,
    #[serde(default)]
    pub notional: Option<Money>,
    #[serde(default)]
    pub realized_pnl: Option<Money>,
    #[serde(default)]
    pub commission: Option<Money>,
    #[serde(default)]
    pub slippage_bps: Option<f64>,
    #[serde(default)]
    pub arrival_price: Option<Price>,
    #[serde(default)]
    pub decision_price: Option<Price>,
    #[serde(default)]
    pub submit_ts_ns: Option<i64>,
    #[serde(default)]
    pub ack_ts_ns: Option<i64>,
    #[serde(default)]
    pub first_fill_ts_ns: Option<i64>,
    #[serde(default)]
    pub last_fill_ts_ns: Option<i64>,
    #[serde(default)]
    pub terminal_ts_ns: Option<i64>,
    #[serde(default)]
    pub latency: LatencyBreakdown,
    #[serde(default)]
    pub anomalies: Vec<String>,
    #[serde(default)]
    pub execution_ids: Vec<String>,
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
            client_order_id: None,
            broker_order_id: None,
            perm_id: None,
            parent_order_id: None,
            child_order_ids: Vec::new(),
            broker_status: None,
            order_type: None,
            limit_price: None,
            stop_price: None,
            tif: None,
            route: None,
            exchange: None,
            destination: None,
            currency: "USD".to_string(),
            submitted_quantity: None,
            remaining_quantity: None,
            cancelled_quantity: None,
            rejected_quantity: None,
            risk: None,
            filled_quantity: 0,
            average_fill_price: None,
            last_fill_price: None,
            notional: None,
            realized_pnl: None,
            commission: None,
            slippage_bps: None,
            arrival_price: None,
            decision_price: None,
            submit_ts_ns: None,
            ack_ts_ns: None,
            first_fill_ts_ns: None,
            last_fill_ts_ns: None,
            terminal_ts_ns: None,
            latency: LatencyBreakdown::default(),
            anomalies: Vec::new(),
            execution_ids: Vec::new(),
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

    pub fn transition_state(&mut self, next: OrderLifecycleState, event_id: &str, sequence: u64) {
        if !is_valid_order_transition(&self.state, &next) {
            self.anomalies.push(format!(
                "invalid_transition {:?}->{:?} event_id={} sequence={}",
                self.state, next, event_id, sequence
            ));
        }
        self.state = next;
    }

    pub fn apply_fill(
        &mut self,
        execution_id: Option<&str>,
        last_quantity: i64,
        cumulative_quantity: Option<i64>,
        remaining_quantity: Option<i64>,
        price: Price,
    ) -> bool {
        if let Some(execution_id) = execution_id {
            if self
                .execution_ids
                .iter()
                .any(|existing| existing == execution_id)
            {
                self.anomalies
                    .push(format!("duplicate_execution_id {execution_id}"));
                return false;
            }
            self.execution_ids.push(execution_id.to_string());
        }

        let previous_quantity = self.filled_quantity;
        let new_quantity = cumulative_quantity.unwrap_or(previous_quantity + last_quantity);
        let effective_last_quantity = if cumulative_quantity.is_some() {
            new_quantity.saturating_sub(previous_quantity)
        } else {
            last_quantity
        };

        if let Some(cumulative_quantity) = cumulative_quantity {
            if cumulative_quantity < previous_quantity {
                self.anomalies.push(format!(
                    "cumulative_fill_regressed previous={} next={}",
                    previous_quantity, cumulative_quantity
                ));
            }
        }

        if new_quantity > 0 {
            let previous_notional = self
                .average_fill_price
                .as_ref()
                .map(|price| price.as_f64() * previous_quantity as f64)
                .unwrap_or(0.0);
            let new_notional = previous_notional + price.as_f64() * effective_last_quantity as f64;
            self.average_fill_price = Some(Price::from_f64_with_scale(
                new_notional / new_quantity as f64,
                price.scale,
                price.currency.clone(),
            ));
        }
        self.filled_quantity = new_quantity;
        self.remaining_quantity = remaining_quantity;
        self.last_fill_price = Some(price);
        true
    }
}

fn is_valid_order_transition(current: &OrderLifecycleState, next: &OrderLifecycleState) -> bool {
    use OrderLifecycleState::*;
    match (current, next) {
        (Observed, _)
        | (SignalGenerated, IntentCreated)
        | (SignalGenerated, RiskApproved)
        | (SignalGenerated, RiskRejected)
        | (IntentCreated, RiskPending)
        | (IntentCreated, RiskApproved)
        | (IntentCreated, RiskRejected)
        | (RiskPending, RiskApproved)
        | (RiskPending, RiskRejected)
        | (RiskApproved, SubmitRequested)
        | (SubmitRequested, SubmittedToBroker)
        | (SubmittedToBroker, BrokerAckReceived)
        | (BrokerAckReceived, PartiallyFilled)
        | (BrokerAckReceived, Filled)
        | (PartiallyFilled, PartiallyFilled)
        | (PartiallyFilled, Filled)
        | (SubmittedToBroker, CancelRequested)
        | (BrokerAckReceived, CancelRequested)
        | (PartiallyFilled, CancelRequested)
        | (CancelRequested, CancelRejected)
        | (CancelRequested, Cancelled)
        | (SubmittedToBroker, BrokerRejected)
        | (BrokerAckReceived, BrokerRejected) => true,
        (Filled, Filled) | (Cancelled, Cancelled) | (BrokerRejected, BrokerRejected) => true,
        _ if current == next => true,
        _ => false,
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
    #[serde(default)]
    pub severity: Option<String>,
    #[serde(default)]
    pub decision_id: Option<String>,
    #[serde(default)]
    pub risk_snapshot_id: Option<String>,
    #[serde(default)]
    pub evaluated_rules: Vec<RiskRuleEval>,
    #[serde(default)]
    pub authority_policy_version: Option<String>,
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
    pub average_price: Price,
    pub market_price: Price,
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
    #[serde(default)]
    pub structured_limits: Vec<RiskLimitView>,
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
            structured_limits: Vec::new(),
            active_blocks: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RiskLimitView {
    pub rule_id: String,
    pub scope: String,
    pub metric: String,
    pub observed: String,
    pub limit: String,
    pub unit: String,
    pub status: String,
    pub updated_ts_ns: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RiskBlock {
    #[serde(default)]
    pub block_id: String,
    #[serde(default)]
    pub rule_id: String,
    pub scope: String,
    pub severity: String,
    pub message: String,
    #[serde(default)]
    pub first_seen_ts_ns: i64,
    #[serde(default)]
    pub last_seen_ts_ns: i64,
    #[serde(default)]
    pub cleared_ts_ns: Option<i64>,
    #[serde(default)]
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default)]
    pub strategy_id: Option<String>,
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
