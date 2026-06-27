use crate::state::{
    AccountView, AlertView, AppState, OrderChain, PositionView, RiskView, StrategyView,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ProjectionSnapshot {
    pub schema_version: String,
    pub snapshot_ts_ns: i64,
    pub source: String,
    pub last_event_sequence: Option<u64>,
    pub account: Option<AccountView>,
    pub strategies: Vec<StrategyView>,
    pub orders: Vec<OrderChain>,
    pub positions: Vec<PositionView>,
    pub risk: Option<RiskView>,
    pub alerts: Vec<AlertView>,
}

pub fn apply_projection_snapshot(state: &mut AppState, snapshot: ProjectionSnapshot) {
    if let Some(account) = snapshot.account {
        state.account = account;
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
        state.positions.upsert(position);
    }

    if let Some(risk) = snapshot.risk {
        state.risk = risk;
    }

    state.alerts.by_id.clear();
    for alert in snapshot.alerts {
        state.alerts.by_id.insert(alert.alert_id.clone(), alert);
    }

    state.connection.nats = snapshot.source;
    state.connection.last_event_sequence = snapshot.last_event_sequence;
    state.connection.last_event_ts_ns = Some(snapshot.snapshot_ts_ns);
}
