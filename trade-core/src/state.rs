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
    #[serde(default)]
    pub market_data: MarketDataStore,
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
            market_data: MarketDataStore::default(),
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
    #[serde(default)]
    pub canonical_account_id: Option<String>,
    #[serde(default)]
    pub account_slot: Option<u8>,
    #[serde(default)]
    pub account_id_hash_hex: Option<String>,
    #[serde(default)]
    pub endpoint_id: Option<String>,
    #[serde(default)]
    pub client_id: Option<i32>,
    #[serde(default)]
    pub gateway_tier: Option<String>,
    #[serde(default)]
    pub account_role: Option<String>,
    #[serde(default)]
    pub role_bits: Option<u8>,
    #[serde(default)]
    pub readonly: Option<bool>,
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
    #[serde(default)]
    pub margin_account: Option<bool>,
    #[serde(default)]
    pub account_type: Option<String>,
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
    #[serde(default)]
    pub account_snapshot_id: Option<String>,
    #[serde(default)]
    pub account_snapshot_seq: Option<u64>,
    #[serde(default)]
    pub account_snapshot_source: Option<String>,
    #[serde(default)]
    pub account_snapshot_ts_ns: Option<i64>,
    #[serde(default)]
    pub account_snapshot_age_ms: Option<u64>,
    #[serde(default = "default_valuation_status")]
    pub valuation_status: String,
    #[serde(default)]
    pub valuation_ok: bool,
    #[serde(default)]
    pub valuation_stale: bool,
    #[serde(default)]
    pub valuation_incomplete_reason: Option<String>,
    #[serde(default)]
    pub cash_source: Option<String>,
    #[serde(default)]
    pub buying_power_source: Option<String>,
    #[serde(default)]
    pub net_liq_source: Option<String>,
    #[serde(default)]
    pub available_funds_source: Option<String>,
    #[serde(default)]
    pub day_pnl_source: Option<String>,
    #[serde(default)]
    pub realized_source: Option<String>,
    #[serde(default)]
    pub unrealized_source: Option<String>,
    #[serde(default = "default_effective_trade_state")]
    pub effective_trade_state: String,
    #[serde(default)]
    pub effective_trade_reason: Option<String>,
    #[serde(default)]
    pub can_submit_order: bool,
    #[serde(default)]
    pub can_cancel_order: bool,
    #[serde(default)]
    pub can_modify_order: bool,
    #[serde(default)]
    pub can_liquidate: bool,
    #[serde(default)]
    pub can_short: bool,
    #[serde(default)]
    pub can_open_long: bool,
    #[serde(default)]
    pub can_close_position: bool,
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
            canonical_account_id: None,
            account_slot: None,
            account_id_hash_hex: None,
            endpoint_id: None,
            client_id: None,
            gateway_tier: None,
            account_role: None,
            role_bits: None,
            readonly: None,
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
            margin_account: None,
            account_type: None,
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
            account_snapshot_id: None,
            account_snapshot_seq: None,
            account_snapshot_source: None,
            account_snapshot_ts_ns: None,
            account_snapshot_age_ms: None,
            valuation_status: default_valuation_status(),
            valuation_ok: false,
            valuation_stale: false,
            valuation_incomplete_reason: Some("no valuation snapshot".to_string()),
            cash_source: None,
            buying_power_source: None,
            net_liq_source: None,
            available_funds_source: None,
            day_pnl_source: None,
            realized_source: None,
            unrealized_source: None,
            effective_trade_state: default_effective_trade_state(),
            effective_trade_reason: Some("NO_ACCOUNT_SNAPSHOT".to_string()),
            can_submit_order: false,
            can_cancel_order: false,
            can_modify_order: false,
            can_liquidate: false,
            can_short: false,
            can_open_long: false,
            can_close_position: false,
        }
    }

    pub fn normalize_legacy_money_fields(&mut self) {
        if self.has_legacy_money_values() && self.money_fields_are_default() {
            self.sync_legacy_money_from_f64();
        } else {
            self.sync_legacy_f64_from_money();
        }
    }

    fn has_legacy_money_values(&self) -> bool {
        self.cash != 0.0
            || self.buying_power != 0.0
            || self.day_pnl != 0.0
            || self.realized_pnl != 0.0
            || self.unrealized_pnl != 0.0
    }

    fn money_fields_are_default(&self) -> bool {
        self.cash_value == Money::default()
            && self.buying_power_value == Money::default()
            && self.day_pnl_value == Money::default()
            && self.realized_pnl_value == Money::default()
            && self.unrealized_pnl_value == Money::default()
    }

    pub fn mark_account_snapshot(
        &mut self,
        snapshot_id: impl Into<String>,
        sequence: Option<u64>,
        source: impl Into<String>,
        ts_ns: i64,
    ) {
        let source = source.into();
        self.account_snapshot_id = Some(snapshot_id.into());
        self.account_snapshot_seq = sequence;
        self.account_snapshot_source = Some(source.clone());
        self.account_snapshot_ts_ns = Some(ts_ns);
        self.account_snapshot_age_ms = crate::unix_ts_ns()
            .checked_sub(ts_ns)
            .filter(|delta| *delta >= 0)
            .map(|delta| (delta / 1_000_000) as u64);
        self.refresh_valuation_status();
    }

    pub fn set_missing_money_sources(&mut self, source: &str) {
        set_missing_source(&mut self.cash_source, source);
        set_missing_source(&mut self.buying_power_source, source);
        set_missing_source(&mut self.day_pnl_source, source);
        set_missing_source(&mut self.realized_source, source);
        set_missing_source(&mut self.unrealized_source, source);
        if self.net_liquidation != Money::default() {
            set_missing_source(&mut self.net_liq_source, source);
        }
        if self.available_funds != Money::default() {
            set_missing_source(&mut self.available_funds_source, source);
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

    pub fn apply_position_mark_pnl(&mut self, unrealized_pnl: f64) {
        self.unrealized_pnl = unrealized_pnl;
        self.day_pnl = self.realized_pnl + self.unrealized_pnl;
        let currency = if self.account_currency.is_empty() {
            "USD".to_string()
        } else {
            self.account_currency.clone()
        };
        self.unrealized_pnl_value = Money::from_f64(self.unrealized_pnl, currency.clone());
        self.day_pnl_value = Money::from_f64(self.day_pnl, currency);
        self.unrealized_source = Some("internal_position_mark".to_string());
        self.day_pnl_source = Some("internal_position_mark".to_string());
        self.refresh_valuation_status();
    }

    pub fn refresh_valuation_status(&mut self) {
        let mut missing = Vec::new();
        if self.cash_source.is_none() {
            missing.push("cash");
        }
        if self.buying_power_source.is_none() {
            missing.push("buy_power");
        }
        if self.net_liq_source.is_none() {
            missing.push("net_liq");
        }
        if self.available_funds_source.is_none() {
            missing.push("available");
        }
        if self.day_pnl_source.is_none() {
            missing.push("day_pnl");
        }
        if self.realized_source.is_none() {
            missing.push("realized");
        }
        if self.unrealized_source.is_none() {
            missing.push("unrealized");
        }

        self.valuation_stale = self
            .account_snapshot_age_ms
            .map(|age_ms| age_ms > 5_000)
            .unwrap_or(false);
        self.valuation_status = if missing.len() == 7 {
            "MISSING".to_string()
        } else if self.valuation_stale {
            "STALE".to_string()
        } else if missing.is_empty() {
            "COMPLETE".to_string()
        } else {
            "PARTIAL".to_string()
        };
        self.valuation_ok = self.valuation_status == "COMPLETE";
        self.valuation_incomplete_reason = if missing.is_empty() {
            None
        } else {
            Some(format!("missing {}", missing.join(",")))
        };
    }

    pub fn refresh_effective_trade_state(&mut self, order_channel_ok: bool) {
        let mutation = self.mutation_permission_label();
        let restriction = self
            .trading_restriction
            .as_deref()
            .filter(|value| !value.is_empty());

        let (state, reason) = if !self.broker_connected {
            ("NO_TRADE", "BROKER_DOWN")
        } else if !order_channel_ok {
            ("NO_TRADE", "ORDER_CHANNEL_DOWN")
        } else if mutation == "NO_TRADE" {
            ("NO_TRADE", "NO_TRADE_ACCOUNT_MUTATION")
        } else if mutation == "READONLY" {
            ("READ_ONLY", "ACCOUNT_READ_ONLY")
        } else if restriction.is_some() {
            ("NO_TRADE", "ACCOUNT_RESTRICTION")
        } else if self.valuation_status == "MISSING" {
            ("NO_TRADE", "VALUATION_MISSING")
        } else if self.valuation_status == "STALE" {
            ("NO_TRADE", "VALUATION_STALE")
        } else if self.valuation_status == "PARTIAL" {
            ("DEGRADED", "VALUATION_PARTIAL")
        } else if mutation == "TRADE?" {
            ("DEGRADED", "ACCOUNT_MUTATION_UNKNOWN")
        } else {
            ("TRADE", "OK")
        };

        self.effective_trade_state = state.to_string();
        self.effective_trade_reason = Some(reason.to_string());
        let can_trade = state == "TRADE";
        let can_operate_orders = self.broker_connected && order_channel_ok;
        self.can_submit_order = can_trade;
        self.can_cancel_order = can_operate_orders && state != "NO_TRADE";
        self.can_modify_order = can_trade;
        self.can_liquidate = can_operate_orders && state != "NO_TRADE";
        self.can_short = can_trade && self.short_permission;
        self.can_open_long = can_trade;
        self.can_close_position = can_operate_orders && state != "NO_TRADE";
    }

    pub fn refresh_ocam_authority_mapping(&mut self) {
        if self.gateway_tier.is_none() {
            self.gateway_tier = gateway_tier_from_mode(&self.mode);
        }
        if self.role_bits.is_none() {
            self.role_bits = self.account_role.as_deref().and_then(role_bits_from_role);
        }
        if self.account_role.is_none() {
            self.account_role = self.role_bits.and_then(role_from_role_bits);
        }
        if self.canonical_account_id.is_none() {
            if let Some(tier) = self
                .gateway_tier
                .as_deref()
                .and_then(normalize_gateway_tier)
            {
                self.canonical_account_id = Some(format!("{}+{}", self.account_id, tier));
            }
        }
        if self.account_id_hash_hex.is_none() {
            if let Some(identity) = self.canonical_account_id.as_deref() {
                self.account_id_hash_hex = Some(format!("0x{:016x}", fnv1a_64(identity)));
            }
        }
    }

    pub fn short_permission_label(&self) -> &'static str {
        if self.short_permission {
            "CAN_SHORT"
        } else {
            "NO_SHORT"
        }
    }

    pub fn margin_permission_label(&self) -> &'static str {
        match self.margin_account {
            Some(true) => "MARGIN",
            Some(false) => "CASH",
            None => match self
                .account_type
                .as_deref()
                .map(|value| value.to_ascii_lowercase())
            {
                Some(value) if value.contains("margin") => "MARGIN",
                Some(value) if value.contains("cash") => "CASH",
                _ => "UNKNOWN",
            },
        }
    }

    pub fn mutation_permission_label(&self) -> &'static str {
        match (self.role_bits.unwrap_or(0) & 0b10 != 0, self.readonly) {
            (true, Some(false)) => "TRADE",
            (true, Some(true)) => "READONLY",
            (true, None) => "TRADE?",
            (false, _) => "NO_TRADE",
        }
    }

    pub fn permission_summary(&self) -> String {
        format!(
            "{}/{}/{}",
            self.short_permission_label(),
            self.margin_permission_label(),
            self.mutation_permission_label()
        )
    }
}

fn gateway_tier_from_mode(mode: &str) -> Option<String> {
    match mode.to_ascii_lowercase().as_str() {
        "paper" | "account_mode_paper" => Some("paper".to_string()),
        "live" | "account_mode_live" => Some("live".to_string()),
        "replay" | "account_mode_replay" => Some("replay".to_string()),
        _ => None,
    }
}

fn normalize_gateway_tier(value: &str) -> Option<&'static str> {
    match value.to_ascii_lowercase().as_str() {
        "paper" | "account_mode_paper" => Some("paper"),
        "live" | "account_mode_live" => Some("live"),
        "replay" | "account_mode_replay" => Some("replay"),
        _ => None,
    }
}

fn role_bits_from_role(value: &str) -> Option<u8> {
    match value.to_ascii_lowercase().replace('-', "_").as_str() {
        "data_only" | "data" => Some(0b01),
        "trade_only" | "trade" => Some(0b10),
        "data_and_trade" | "data_trade" | "data+trade" | "both" => Some(0b11),
        _ => None,
    }
}

fn role_from_role_bits(value: u8) -> Option<String> {
    match value {
        0b01 => Some("data_only".to_string()),
        0b10 => Some("trade_only".to_string()),
        0b11 => Some("data_and_trade".to_string()),
        _ => None,
    }
}

fn fnv1a_64(value: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
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
        aggregate.gateway_tier = common_optional_string(
            self.by_id
                .values()
                .filter_map(|account| account.gateway_tier.as_deref()),
        )
        .or_else(|| Some("mixed".to_string()));
        aggregate.account_role = common_optional_string(
            self.by_id
                .values()
                .filter_map(|account| account.account_role.as_deref()),
        )
        .or_else(|| Some("mixed".to_string()));
        aggregate.role_bits =
            common_optional_u8(self.by_id.values().filter_map(|account| account.role_bits));
        aggregate.readonly =
            common_optional_bool(self.by_id.values().filter_map(|account| account.readonly));
        aggregate.margin_account = common_optional_bool(
            self.by_id
                .values()
                .filter_map(|account| account.margin_account),
        );
        aggregate.account_type = common_optional_string(
            self.by_id
                .values()
                .filter_map(|account| account.account_type.as_deref()),
        )
        .or_else(|| {
            if aggregate.margin_account.is_none() {
                Some("mixed".to_string())
            } else {
                None
            }
        });
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
        aggregate.account_snapshot_source = Some("aggregate".to_string());
        aggregate.account_snapshot_seq = self
            .by_id
            .values()
            .filter_map(|account| account.account_snapshot_seq)
            .max();
        aggregate.account_snapshot_ts_ns = self
            .by_id
            .values()
            .filter_map(|account| account.account_snapshot_ts_ns)
            .min();
        aggregate.account_snapshot_age_ms = self
            .by_id
            .values()
            .filter_map(|account| account.account_snapshot_age_ms)
            .max();
        aggregate.cash_source = aggregate_source(
            self.by_id
                .values()
                .map(|account| account.cash_source.as_deref()),
        );
        aggregate.buying_power_source = aggregate_source(
            self.by_id
                .values()
                .map(|account| account.buying_power_source.as_deref()),
        );
        aggregate.net_liq_source = aggregate_source(
            self.by_id
                .values()
                .map(|account| account.net_liq_source.as_deref()),
        );
        aggregate.available_funds_source = aggregate_source(
            self.by_id
                .values()
                .map(|account| account.available_funds_source.as_deref()),
        );
        aggregate.day_pnl_source = aggregate_source(
            self.by_id
                .values()
                .map(|account| account.day_pnl_source.as_deref()),
        );
        aggregate.realized_source = aggregate_source(
            self.by_id
                .values()
                .map(|account| account.realized_source.as_deref()),
        );
        aggregate.unrealized_source = aggregate_source(
            self.by_id
                .values()
                .map(|account| account.unrealized_source.as_deref()),
        );
        aggregate.valuation_status = aggregate_valuation_status(
            self.by_id
                .values()
                .map(|account| account.valuation_status.as_str()),
        );
        aggregate.valuation_stale = self.by_id.values().any(|account| account.valuation_stale);
        let valuation_reasons = self
            .by_id
            .values()
            .filter_map(|account| account.valuation_incomplete_reason.as_deref())
            .collect::<Vec<_>>();
        aggregate.valuation_incomplete_reason = if valuation_reasons.is_empty() {
            None
        } else {
            Some(valuation_reasons.join(";"))
        };
        aggregate.effective_trade_state = aggregate_trade_state(
            self.by_id
                .values()
                .map(|account| account.effective_trade_state.as_str()),
        );
        aggregate.effective_trade_reason = if aggregate.effective_trade_state == "TRADE" {
            Some("OK".to_string())
        } else {
            Some(
                self.by_id
                    .values()
                    .filter_map(|account| account.effective_trade_reason.as_deref())
                    .collect::<Vec<_>>()
                    .join(","),
            )
        };
        aggregate.can_submit_order = self.by_id.values().all(|account| account.can_submit_order);
        aggregate.can_cancel_order = self.by_id.values().all(|account| account.can_cancel_order);
        aggregate.can_modify_order = self.by_id.values().all(|account| account.can_modify_order);
        aggregate.can_liquidate = self.by_id.values().all(|account| account.can_liquidate);
        aggregate.can_short = self.by_id.values().all(|account| account.can_short);
        aggregate.can_open_long = self.by_id.values().all(|account| account.can_open_long);
        aggregate.can_close_position = self
            .by_id
            .values()
            .all(|account| account.can_close_position);
        aggregate
    }
}

fn default_valuation_status() -> String {
    "MISSING".to_string()
}

fn default_effective_trade_state() -> String {
    "NO_TRADE".to_string()
}

fn set_missing_source(target: &mut Option<String>, source: &str) {
    if target.is_none() && !source.is_empty() {
        *target = Some(source.to_string());
    }
}

fn aggregate_source<'a>(values: impl IntoIterator<Item = Option<&'a str>>) -> Option<String> {
    let mut sources = values
        .into_iter()
        .flatten()
        .filter(|value| !value.is_empty());
    let first = sources.next()?;
    if sources.all(|source| source == first) {
        Some(first.to_string())
    } else {
        Some("mixed".to_string())
    }
}

fn aggregate_valuation_status<'a>(values: impl IntoIterator<Item = &'a str>) -> String {
    let mut status = "COMPLETE";
    for value in values {
        match value {
            "MISSING" => return "MISSING".to_string(),
            "STALE" => status = "STALE",
            "PARTIAL" if status == "COMPLETE" => status = "PARTIAL",
            _ => {}
        }
    }
    status.to_string()
}

fn aggregate_trade_state<'a>(values: impl IntoIterator<Item = &'a str>) -> String {
    let mut state = "TRADE";
    for value in values {
        match value {
            "NO_TRADE" => return "NO_TRADE".to_string(),
            "READ_ONLY" => state = "READ_ONLY",
            "DEGRADED" if state == "TRADE" => state = "DEGRADED",
            _ => {}
        }
    }
    state.to_string()
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

fn common_optional_string<'a>(values: impl IntoIterator<Item = &'a str>) -> Option<String> {
    let mut common = None::<String>;
    for value in values {
        match common.as_deref() {
            None => common = Some(value.to_string()),
            Some(existing) if existing == value => {}
            Some(_) => return None,
        }
    }
    common
}

fn common_optional_bool(values: impl IntoIterator<Item = bool>) -> Option<bool> {
    let mut common = None::<bool>;
    for value in values {
        match common {
            None => common = Some(value),
            Some(existing) if existing == value => {}
            Some(_) => return None,
        }
    }
    common
}

fn common_optional_u8(values: impl IntoIterator<Item = u8>) -> Option<u8> {
    let mut common = None::<u8>;
    for value in values {
        match common {
            None => common = Some(value),
            Some(existing) if existing == value => {}
            Some(_) => return None,
        }
    }
    common
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct MarketDataStore {
    pub by_symbol: BTreeMap<String, MarketDataSummaryView>,
}

impl MarketDataStore {
    pub fn upsert(&mut self, summary: MarketDataSummaryView) {
        self.by_symbol.insert(summary.symbol.clone(), summary);
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketDataSummaryView {
    pub symbol: String,
    pub source: Option<String>,
    pub bid_price: Option<Price>,
    pub ask_price: Option<Price>,
    pub spread_bps: Option<f64>,
    pub imbalance: Option<f64>,
    pub microprice: Option<Price>,
    pub quote_age_ms: Option<u64>,
    pub event_rate_per_sec: Option<f64>,
    pub wall_size: Option<i64>,
    pub summary_ts_ns: Option<i64>,
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
    #[serde(default)]
    pub session: Option<String>,
    #[serde(default)]
    pub requested_at_ts_ns: Option<i64>,
    #[serde(default)]
    pub risk_checked: Option<bool>,
    #[serde(default)]
    pub dry_run: Option<bool>,
    #[serde(default)]
    pub execute_broker: Option<bool>,
    #[serde(default)]
    pub approval_id: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub result_event_id: Option<String>,
    #[serde(default)]
    pub error_code: Option<String>,
    #[serde(default)]
    pub error_message: Option<String>,
    #[serde(default)]
    pub rollback_command_id: Option<String>,
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
            session: None,
            requested_at_ts_ns: None,
            risk_checked: None,
            dry_run: None,
            execute_broker: None,
            approval_id: None,
            status: None,
            result_event_id: None,
            error_code: None,
            error_message: None,
            rollback_command_id: None,
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
    #[serde(default)]
    pub signals_total_today: u64,
    #[serde(default)]
    pub signals_last_1m: u64,
    #[serde(default)]
    pub intents_total_today: u64,
    #[serde(default)]
    pub orders_total_today: u64,
    #[serde(default)]
    pub fills_total_today: u64,
    #[serde(default)]
    pub partial_fills_today: u64,
    #[serde(default)]
    pub cancels_total_today: u64,
    #[serde(default)]
    pub rejects_total_today: u64,
    #[serde(default)]
    pub strategy_realized_pnl: Option<Money>,
    #[serde(default)]
    pub strategy_unrealized_pnl: Option<Money>,
    #[serde(default)]
    pub strategy_total_pnl: Option<Money>,
    #[serde(default)]
    pub pnl_source: Option<String>,
    #[serde(default)]
    pub pnl_basis: Option<String>,
    #[serde(default)]
    pub pnl_diff_vs_account: Option<Money>,
    #[serde(default)]
    pub pnl_as_of_ts_ns: Option<i64>,
    #[serde(default)]
    pub session_phase: Option<String>,
    #[serde(default)]
    pub strategy_window_id: Option<String>,
    #[serde(default)]
    pub window_start_ts_ns: Option<i64>,
    #[serde(default)]
    pub window_end_ts_ns: Option<i64>,
    #[serde(default)]
    pub window_status: Option<String>,
    #[serde(default)]
    pub next_transition_ts_ns: Option<i64>,
    #[serde(default)]
    pub is_market_open: Option<bool>,
    #[serde(default)]
    pub is_regular_session: Option<bool>,
    #[serde(default)]
    pub is_opening_window: Option<bool>,
    #[serde(default)]
    pub symbols_blocked: u64,
    #[serde(default)]
    pub symbols_with_fresh_l1: u64,
    #[serde(default)]
    pub symbols_with_fresh_l2: u64,
    #[serde(default)]
    pub symbols_missing_md: u64,
    #[serde(default)]
    pub l1_symbols_allocated: u64,
    #[serde(default)]
    pub l2_capacity: u64,
    #[serde(default)]
    pub l2_capacity_used: u64,
    #[serde(default)]
    pub l2_denied_symbols: Vec<String>,
    #[serde(default)]
    pub lease_authority_version: Option<String>,
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
            signals_total_today: 0,
            signals_last_1m: 0,
            intents_total_today: 0,
            orders_total_today: 0,
            fills_total_today: 0,
            partial_fills_today: 0,
            cancels_total_today: 0,
            rejects_total_today: 0,
            strategy_realized_pnl: None,
            strategy_unrealized_pnl: None,
            strategy_total_pnl: None,
            pnl_source: None,
            pnl_basis: None,
            pnl_diff_vs_account: None,
            pnl_as_of_ts_ns: None,
            session_phase: None,
            strategy_window_id: None,
            window_start_ts_ns: None,
            window_end_ts_ns: None,
            window_status: None,
            next_transition_ts_ns: None,
            is_market_open: None,
            is_regular_session: None,
            is_opening_window: None,
            symbols_blocked: 0,
            symbols_with_fresh_l1: 0,
            symbols_with_fresh_l2: 0,
            symbols_missing_md: 0,
            l1_symbols_allocated: 0,
            l2_capacity: 0,
            l2_capacity_used: 0,
            l2_denied_symbols: Vec::new(),
            lease_authority_version: None,
        }
    }
}

fn default_strategy_enabled() -> bool {
    true
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StrategyRiskGateView {
    pub name: String,
    pub passed: bool,
    pub detail: String,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub observed: Option<String>,
    #[serde(default)]
    pub limit: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub severity: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub policy_version: Option<String>,
    #[serde(default)]
    pub source_seq: Option<u64>,
    #[serde(default)]
    pub evaluated_ts_ns: Option<i64>,
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
    #[serde(default)]
    pub order_ref: Option<String>,
    #[serde(default)]
    pub strategy_order_ref: Option<String>,
    #[serde(default)]
    pub broker_account_id: Option<String>,
    #[serde(default)]
    pub broker_perm_id: Option<String>,
    #[serde(default)]
    pub broker_client_id: Option<i32>,
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
    pub cum_qty_i64: Option<i64>,
    #[serde(default)]
    pub leaves_qty_i64: Option<i64>,
    #[serde(default)]
    pub display_qty: Option<i64>,
    #[serde(default)]
    pub min_qty: Option<i64>,
    #[serde(default)]
    pub total_qty: Option<i64>,
    #[serde(default)]
    pub remaining_reason: Option<String>,
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
    pub intent_created_ts_ns: Option<i64>,
    #[serde(default)]
    pub risk_decision_ts_ns: Option<i64>,
    #[serde(default)]
    pub submit_requested_ts_ns: Option<i64>,
    #[serde(default)]
    pub order_submitted_ts_ns: Option<i64>,
    #[serde(default)]
    pub broker_ack_ts_ns: Option<i64>,
    #[serde(default)]
    pub cancel_requested_ts_ns: Option<i64>,
    #[serde(default)]
    pub cancel_ack_ts_ns: Option<i64>,
    #[serde(default)]
    pub bbo_bid_at_signal: Option<Price>,
    #[serde(default)]
    pub bbo_ask_at_signal: Option<Price>,
    #[serde(default)]
    pub bbo_bid_at_intent: Option<Price>,
    #[serde(default)]
    pub bbo_ask_at_intent: Option<Price>,
    #[serde(default)]
    pub bbo_bid_at_submit: Option<Price>,
    #[serde(default)]
    pub bbo_ask_at_submit: Option<Price>,
    #[serde(default)]
    pub bbo_bid_at_ack: Option<Price>,
    #[serde(default)]
    pub bbo_ask_at_ack: Option<Price>,
    #[serde(default)]
    pub bbo_bid_at_fill: Option<Price>,
    #[serde(default)]
    pub bbo_ask_at_fill: Option<Price>,
    #[serde(default)]
    pub mid_at_submit: Option<Price>,
    #[serde(default)]
    pub spread_bps_at_submit: Option<f64>,
    #[serde(default)]
    pub quote_age_ms_at_submit: Option<u64>,
    #[serde(default)]
    pub queue_position_estimate: Option<f64>,
    #[serde(default)]
    pub slippage_vs_mid_bps: Option<f64>,
    #[serde(default)]
    pub slippage_vs_arrival_bps: Option<f64>,
    #[serde(default)]
    pub slippage_vs_decision_bps: Option<f64>,
    #[serde(default)]
    pub causal_chain_summary: Option<String>,
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
    #[serde(default)]
    pub fills: Vec<FillExecutionView>,
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
            order_ref: None,
            strategy_order_ref: None,
            broker_account_id: None,
            broker_perm_id: None,
            broker_client_id: None,
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
            cum_qty_i64: None,
            leaves_qty_i64: None,
            display_qty: None,
            min_qty: None,
            total_qty: None,
            remaining_reason: None,
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
            intent_created_ts_ns: None,
            risk_decision_ts_ns: None,
            submit_requested_ts_ns: None,
            order_submitted_ts_ns: None,
            broker_ack_ts_ns: None,
            cancel_requested_ts_ns: None,
            cancel_ack_ts_ns: None,
            bbo_bid_at_signal: None,
            bbo_ask_at_signal: None,
            bbo_bid_at_intent: None,
            bbo_ask_at_intent: None,
            bbo_bid_at_submit: None,
            bbo_ask_at_submit: None,
            bbo_bid_at_ack: None,
            bbo_ask_at_ack: None,
            bbo_bid_at_fill: None,
            bbo_ask_at_fill: None,
            mid_at_submit: None,
            spread_bps_at_submit: None,
            quote_age_ms_at_submit: None,
            queue_position_estimate: None,
            slippage_vs_mid_bps: None,
            slippage_vs_arrival_bps: None,
            slippage_vs_decision_bps: None,
            causal_chain_summary: None,
            submit_ts_ns: None,
            ack_ts_ns: None,
            first_fill_ts_ns: None,
            last_fill_ts_ns: None,
            terminal_ts_ns: None,
            latency: LatencyBreakdown::default(),
            anomalies: Vec::new(),
            execution_ids: Vec::new(),
            fills: Vec::new(),
            timeline: Vec::new(),
        }
    }

    pub fn total_quantity(&self) -> Option<i64> {
        self.total_qty
            .or(self.intended_quantity)
            .or(self.submitted_quantity)
            .or_else(|| {
                self.remaining_quantity
                    .map(|remaining| self.filled_quantity + remaining)
            })
    }

    pub fn leaves_quantity(&self) -> Option<i64> {
        self.leaves_qty_i64.or(self.remaining_quantity)
    }

    pub fn quantity_status(&self) -> &'static str {
        match (self.total_quantity(), self.leaves_quantity()) {
            (Some(total), Some(remaining)) if self.filled_quantity + remaining == total => "OK",
            (Some(_), Some(_)) => "INCONSISTENT",
            (Some(_), None) => "MISSING_LEAVES",
            (None, _) => "MISSING_TOTAL",
        }
    }

    pub fn refresh_quantity_reason(&mut self) {
        self.remaining_reason = match self.quantity_status() {
            "OK" => None,
            "INCONSISTENT" => Some("filled_plus_remaining_ne_total".to_string()),
            "MISSING_LEAVES" => Some("missing_remaining_or_leaves_qty".to_string()),
            "MISSING_TOTAL" => Some("missing_total_qty".to_string()),
            other => Some(other.to_ascii_lowercase()),
        };
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
        self.cum_qty_i64 = Some(new_quantity);
        self.leaves_qty_i64 = remaining_quantity;
        self.last_fill_price = Some(price);
        true
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FillExecutionView {
    pub exec_id: Option<String>,
    #[serde(default)]
    pub broker_exec_id: Option<String>,
    pub fill_seq: u64,
    pub qty: i64,
    pub price: Price,
    #[serde(default)]
    pub order_id: Option<String>,
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub exchange: Option<String>,
    #[serde(default)]
    pub venue: Option<String>,
    #[serde(default)]
    pub liquidity_flag: Option<String>,
    #[serde(default)]
    pub commission: Option<Money>,
    #[serde(default)]
    pub fees: Vec<Money>,
    #[serde(default)]
    pub fee_details: Vec<FeeDetailView>,
    #[serde(default)]
    pub currency: Option<String>,
    #[serde(default)]
    pub fill_ts_ns: Option<i64>,
    #[serde(default)]
    pub report_ts_ns: Option<i64>,
    #[serde(default)]
    pub ingest_ts_ns: Option<i64>,
    #[serde(default)]
    pub position_after_fill: Option<i64>,
    #[serde(default)]
    pub realized_pnl_delta: Option<Money>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FeeDetailView {
    pub name: String,
    pub amount: Money,
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
    #[serde(default)]
    pub risk_decision_seq: Option<u64>,
    #[serde(default)]
    pub risk_result: Option<String>,
    #[serde(default)]
    pub limits_snapshot_id: Option<String>,
    #[serde(default)]
    pub risk_mode: Option<String>,
    #[serde(default)]
    pub limits_enforced: Option<bool>,
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

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PositionView {
    pub key: String,
    pub account_id: String,
    pub symbol: String,
    pub net_quantity: i64,
    pub average_price: Price,
    pub market_price: Price,
    pub unrealized_pnl: f64,
    pub strategy_attribution: Vec<StrategyPositionView>,
    #[serde(default)]
    pub open_buy_qty: Option<i64>,
    #[serde(default)]
    pub open_sell_qty: Option<i64>,
    #[serde(default)]
    pub pending_cancel_qty: Option<i64>,
    #[serde(default)]
    pub reserved_buy_power: Option<Money>,
    #[serde(default)]
    pub position_notional: Option<Money>,
    #[serde(default)]
    pub gross_exposure: Option<Money>,
    #[serde(default)]
    pub net_exposure: Option<Money>,
    #[serde(default)]
    pub realized_pnl: Option<Money>,
    #[serde(default)]
    pub mark_source: Option<String>,
    #[serde(default)]
    pub mark_ts_ns: Option<i64>,
    #[serde(default)]
    pub mark_age_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StrategyPositionView {
    pub strategy_id: String,
    pub quantity: i64,
    #[serde(default)]
    pub avg_cost: Option<Price>,
    #[serde(default)]
    pub realized_pnl: Option<Money>,
    #[serde(default)]
    pub unrealized_pnl: Option<Money>,
    #[serde(default)]
    pub fees: Vec<Money>,
    #[serde(default)]
    pub attribution_method: Option<String>,
    #[serde(default)]
    pub attribution_version: Option<String>,
    #[serde(default)]
    pub avg_cost_ts_ns: Option<i64>,
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
    #[serde(default)]
    pub risk_mode: Option<String>,
    #[serde(default)]
    pub limits_enforced: Option<bool>,
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
            risk_mode: None,
            limits_enforced: None,
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
    #[serde(default)]
    pub severity: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub policy_version: Option<String>,
    #[serde(default)]
    pub source_seq: Option<u64>,
    #[serde(default)]
    pub evaluated_ts_ns: Option<i64>,
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
    pub source: String,
    #[serde(default)]
    pub first_seen_ts_ns: i64,
    #[serde(default)]
    pub last_seen_ts_ns: i64,
    #[serde(default)]
    pub last_seen_seq: Option<u64>,
    #[serde(default)]
    pub cleared_ts_ns: Option<i64>,
    #[serde(default)]
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default)]
    pub strategy_id: Option<String>,
    #[serde(default)]
    pub blocks_order_submit: bool,
    #[serde(default)]
    pub blocks_cancel: bool,
    #[serde(default)]
    pub blocks_short: bool,
    #[serde(default)]
    pub blocks_command: bool,
    #[serde(default)]
    pub scope_type: Option<String>,
    #[serde(default)]
    pub scope_id: Option<String>,
    #[serde(default)]
    pub reason_code: Option<String>,
    #[serde(default)]
    pub reason_text: Option<String>,
}

pub fn refresh_account_safety_state(state: &mut AppState, ts_ns: i64) {
    let order_channel_ok = state.risk.broker_order_channel_ok;
    let mut derived_blocks = Vec::new();
    for account in state.accounts.by_id.values_mut() {
        account.refresh_valuation_status();
        account.refresh_effective_trade_state(order_channel_ok);
        derived_blocks.extend(derived_blocks_for_account(account, ts_ns, order_channel_ok));
    }

    state
        .risk
        .active_blocks
        .retain(|block| block.source != "account_effective_state");
    state.risk.active_blocks.extend(derived_blocks);
    state.account = state.accounts.aggregate_view();
}

fn derived_blocks_for_account(
    account: &AccountView,
    ts_ns: i64,
    order_channel_ok: bool,
) -> Vec<RiskBlock> {
    let mut blocks = Vec::new();
    let scope = format!("account:{}", account.account_id);
    if !account.broker_connected {
        blocks.push(account_block(
            account,
            &scope,
            "BROKER_DOWN",
            "broker connection is not healthy",
            ts_ns,
            true,
            true,
            true,
            true,
        ));
    }
    if !order_channel_ok {
        blocks.push(account_block(
            account,
            &scope,
            "ORDER_CHANNEL_DOWN",
            "order channel is unavailable",
            ts_ns,
            true,
            true,
            true,
            true,
        ));
    }
    match account.mutation_permission_label() {
        "NO_TRADE" => blocks.push(account_block(
            account,
            &scope,
            "NO_TRADE_ACCOUNT_MUTATION",
            "account permission mutation is NO_TRADE",
            ts_ns,
            true,
            true,
            true,
            true,
        )),
        "READONLY" => blocks.push(account_block(
            account,
            &scope,
            "READ_ONLY_ACCOUNT_MUTATION",
            "account permission mutation is READONLY",
            ts_ns,
            true,
            false,
            true,
            false,
        )),
        "TRADE?" => blocks.push(account_block(
            account,
            &scope,
            "UNKNOWN_ACCOUNT_MUTATION",
            "account trade mutation is unknown",
            ts_ns,
            true,
            false,
            true,
            false,
        )),
        _ => {}
    }
    if !account.short_permission {
        blocks.push(account_block(
            account,
            &scope,
            "NO_SHORT_ACCOUNT_PERMISSION",
            "account is long-only",
            ts_ns,
            false,
            false,
            true,
            false,
        ));
    }
    if let Some(reason) = account
        .trading_restriction
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        blocks.push(account_block(
            account,
            &scope,
            "ACCOUNT_RESTRICTION",
            reason,
            ts_ns,
            true,
            false,
            true,
            false,
        ));
    }
    if matches!(
        account.valuation_status.as_str(),
        "MISSING" | "STALE" | "PARTIAL"
    ) {
        blocks.push(account_block(
            account,
            &scope,
            &format!("VALUATION_{}", account.valuation_status),
            account
                .valuation_incomplete_reason
                .as_deref()
                .unwrap_or("account valuation is not complete"),
            ts_ns,
            matches!(account.valuation_status.as_str(), "MISSING" | "STALE"),
            false,
            false,
            false,
        ));
    }
    blocks
}

fn account_block(
    account: &AccountView,
    scope: &str,
    reason_code: &str,
    message: &str,
    ts_ns: i64,
    blocks_order_submit: bool,
    blocks_cancel: bool,
    blocks_short: bool,
    blocks_command: bool,
) -> RiskBlock {
    RiskBlock {
        block_id: format!("{}:{reason_code}", account.account_id),
        rule_id: reason_code.to_string(),
        scope: scope.to_string(),
        severity: if blocks_order_submit || blocks_cancel || blocks_short {
            "HARD_BLOCK".to_string()
        } else {
            "SOFT_WARN".to_string()
        },
        message: message.to_string(),
        source: "account_effective_state".to_string(),
        first_seen_ts_ns: ts_ns,
        last_seen_ts_ns: ts_ns,
        last_seen_seq: None,
        cleared_ts_ns: None,
        correlation_id: None,
        symbol: None,
        strategy_id: None,
        blocks_order_submit,
        blocks_cancel,
        blocks_short,
        blocks_command,
        scope_type: Some("account".to_string()),
        scope_id: Some(account.account_id.clone()),
        reason_code: Some(reason_code.to_string()),
        reason_text: Some(message.to_string()),
    }
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
    pub event_id: String,
    pub sequence: u64,
    pub ts_ns: i64,
    #[serde(default)]
    pub source_ts_ns: i64,
    #[serde(default)]
    pub ingest_ts_ns: i64,
    #[serde(default)]
    pub publish_ts_ns: i64,
    pub event_type: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub correlation_id: String,
    #[serde(default)]
    pub causation_id: String,
    pub producer: String,
    #[serde(default)]
    pub schema_version: String,
    #[serde(default)]
    pub stream: String,
    #[serde(default)]
    pub subject: String,
    #[serde(default)]
    pub partition_key: String,
    #[serde(default)]
    pub environment: String,
    #[serde(default)]
    pub trace_id: Option<String>,
    #[serde(default)]
    pub span_id: Option<String>,
    #[serde(default)]
    pub checksum: Option<String>,
    #[serde(default)]
    pub event_hash: Option<String>,
    #[serde(default)]
    pub prev_event_hash: Option<String>,
    #[serde(default)]
    pub aggregate_version: Option<u64>,
    #[serde(default)]
    pub aggregate_hash: Option<String>,
    #[serde(default)]
    pub projection_version: Option<String>,
    #[serde(default)]
    pub marker: Option<String>,
    pub headline: String,
    #[serde(default)]
    pub payload_json: Option<String>,
}
