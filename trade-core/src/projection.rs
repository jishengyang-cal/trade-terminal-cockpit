use crate::state::{
    refresh_account_safety_state, AccountView, AlertView, AppState, MarketDataSummaryView,
    OrderChain, PositionView, RiskView, StrategyView,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ProjectionSnapshot {
    pub schema_version: String,
    pub snapshot_ts_ns: i64,
    pub source: String,
    pub last_event_sequence: Option<u64>,
    pub account: Option<AccountView>,
    #[serde(default)]
    pub accounts: Vec<AccountView>,
    #[serde(default)]
    pub market_data: Vec<MarketDataSummaryView>,
    pub strategies: Vec<StrategyView>,
    pub orders: Vec<OrderChain>,
    pub positions: Vec<PositionView>,
    pub risk: Option<RiskView>,
    pub alerts: Vec<AlertView>,
}

pub fn apply_projection_snapshot(state: &mut AppState, snapshot: ProjectionSnapshot) {
    state.accounts.by_id.clear();
    if let Some(account) = snapshot.account {
        let mut account = account;
        normalize_projection_account(
            &mut account,
            &snapshot.source,
            snapshot.last_event_sequence,
            snapshot.snapshot_ts_ns,
        );
        state
            .accounts
            .by_id
            .insert(account.account_id.clone(), account);
    }
    for account in snapshot.accounts {
        let mut account = account;
        normalize_projection_account(
            &mut account,
            &snapshot.source,
            snapshot.last_event_sequence,
            snapshot.snapshot_ts_ns,
        );
        state
            .accounts
            .by_id
            .insert(account.account_id.clone(), account);
    }

    state.market_data.by_symbol.clear();
    for summary in snapshot.market_data {
        state.market_data.upsert(summary);
    }

    state.strategies.by_id.clear();
    for strategy in snapshot.strategies {
        state
            .strategies
            .by_id
            .insert(strategy.strategy_id.clone(), strategy);
    }

    state.orders.by_correlation_id.clear();
    state.orders.order_id_index.clear();
    for order in snapshot.orders {
        if let (Some(account_id), Some(order_id)) =
            (order.account_id.as_deref(), order.order_id.as_deref())
        {
            state
                .orders
                .index_order(account_id, order_id, &order.correlation_id);
        }
        state
            .orders
            .by_correlation_id
            .insert(order.correlation_id.clone(), order);
    }

    state.positions.by_key.clear();
    for position in snapshot.positions {
        state.accounts.get_or_insert(&position.account_id);
        state.positions.upsert(position);
    }
    recalculate_account_pnls_from_positions(state);
    if state.accounts.by_id.is_empty() {
        let account = AccountView::default();
        state
            .accounts
            .by_id
            .insert(account.account_id.clone(), account);
    }

    if let Some(risk) = snapshot.risk {
        state.risk = risk;
    }
    refresh_account_safety_state(state, snapshot.snapshot_ts_ns);

    state.alerts.by_id.clear();
    for alert in snapshot.alerts {
        state.alerts.by_id.insert(alert.alert_id.clone(), alert);
    }

    state.connection.nats = snapshot.source;
    state.connection.last_event_sequence = snapshot.last_event_sequence;
    state.connection.last_event_ts_ns = Some(snapshot.snapshot_ts_ns);
}

fn recalculate_account_pnls_from_positions(state: &mut AppState) {
    for account in state.accounts.by_id.values_mut() {
        let unrealized_pnl = state
            .positions
            .by_key
            .values()
            .filter(|position| position.account_id == account.account_id)
            .map(|position| position.unrealized_pnl)
            .sum();
        account.apply_position_mark_pnl(unrealized_pnl);
    }
}

fn normalize_projection_account(
    account: &mut AccountView,
    source: &str,
    sequence: Option<u64>,
    snapshot_ts_ns: i64,
) {
    account.normalize_legacy_money_fields();
    account.refresh_ocam_authority_mapping();
    account.set_missing_money_sources(source);
    account.mark_account_snapshot(
        format!("projection:{}:{}", source, account.account_id),
        sequence,
        source,
        snapshot_ts_ns,
    );
}
