use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DangerLevel {
    ReadOnly,
    Controlled,
    Dangerous,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommandEnvelope {
    pub command_id: String,
    pub command_type: String,
    pub operator_id: String,
    pub session_id: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub correlation_id: String,
    pub requested_ts_ns: i64,
    pub reason: String,
    pub capability: String,
    pub danger_level: DangerLevel,
    pub payload: CommandPayload,
}

impl CommandEnvelope {
    pub fn new(
        command_id: impl Into<String>,
        operator_id: impl Into<String>,
        session_id: impl Into<String>,
        correlation_id: impl Into<String>,
        reason: impl Into<String>,
        capability: impl Into<String>,
        payload: CommandPayload,
    ) -> Self {
        let command_type = payload.command_type().to_string();
        let aggregate_type = payload.aggregate_type().to_string();
        let aggregate_id = payload.aggregate_id();
        let danger_level = payload.danger_level();
        Self {
            command_id: command_id.into(),
            command_type,
            operator_id: operator_id.into(),
            session_id: session_id.into(),
            aggregate_type,
            aggregate_id,
            correlation_id: correlation_id.into(),
            requested_ts_ns: crate::unix_ts_ns(),
            reason: reason.into(),
            capability: capability.into(),
            danger_level,
            payload,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum CommandPayload {
    PauseStrategyRequested {
        strategy_id: String,
    },
    ResumeStrategyRequested {
        strategy_id: String,
    },
    DrainStrategyRequested {
        strategy_id: String,
    },
    KillStrategyRequested {
        strategy_id: String,
    },
    CancelOrderRequested {
        account_id: String,
        order_id: String,
    },
    CancelAllOrdersForSymbolRequested {
        account_id: String,
        symbol: String,
    },
    FlattenSymbolRequested {
        account_id: String,
        symbol: String,
    },
    GlobalKillSwitchRequested {
        account_id: String,
    },
    AcknowledgeAlertRequested {
        alert_id: String,
    },
}

impl CommandPayload {
    pub fn command_type(&self) -> &'static str {
        match self {
            Self::PauseStrategyRequested { .. } => "PauseStrategyRequested",
            Self::ResumeStrategyRequested { .. } => "ResumeStrategyRequested",
            Self::DrainStrategyRequested { .. } => "DrainStrategyRequested",
            Self::KillStrategyRequested { .. } => "KillStrategyRequested",
            Self::CancelOrderRequested { .. } => "CancelOrderRequested",
            Self::CancelAllOrdersForSymbolRequested { .. } => "CancelAllOrdersForSymbolRequested",
            Self::FlattenSymbolRequested { .. } => "FlattenSymbolRequested",
            Self::GlobalKillSwitchRequested { .. } => "GlobalKillSwitchRequested",
            Self::AcknowledgeAlertRequested { .. } => "AcknowledgeAlertRequested",
        }
    }

    pub fn aggregate_type(&self) -> &'static str {
        match self {
            Self::PauseStrategyRequested { .. }
            | Self::ResumeStrategyRequested { .. }
            | Self::DrainStrategyRequested { .. }
            | Self::KillStrategyRequested { .. } => "strategy",
            Self::CancelOrderRequested { .. } => "order",
            Self::CancelAllOrdersForSymbolRequested { .. }
            | Self::FlattenSymbolRequested { .. } => "symbol",
            Self::GlobalKillSwitchRequested { .. } => "account",
            Self::AcknowledgeAlertRequested { .. } => "alert",
        }
    }

    pub fn aggregate_id(&self) -> String {
        match self {
            Self::PauseStrategyRequested { strategy_id }
            | Self::ResumeStrategyRequested { strategy_id }
            | Self::DrainStrategyRequested { strategy_id }
            | Self::KillStrategyRequested { strategy_id } => strategy_id.clone(),
            Self::CancelOrderRequested {
                account_id,
                order_id,
            } => format!("{account_id}:{order_id}"),
            Self::CancelAllOrdersForSymbolRequested { account_id, symbol }
            | Self::FlattenSymbolRequested { account_id, symbol } => {
                format!("{account_id}:{symbol}")
            }
            Self::GlobalKillSwitchRequested { account_id } => account_id.clone(),
            Self::AcknowledgeAlertRequested { alert_id } => alert_id.clone(),
        }
    }

    pub fn danger_level(&self) -> DangerLevel {
        match self {
            Self::PauseStrategyRequested { .. }
            | Self::ResumeStrategyRequested { .. }
            | Self::DrainStrategyRequested { .. }
            | Self::CancelOrderRequested { .. }
            | Self::AcknowledgeAlertRequested { .. } => DangerLevel::Controlled,
            Self::KillStrategyRequested { .. }
            | Self::CancelAllOrdersForSymbolRequested { .. }
            | Self::FlattenSymbolRequested { .. }
            | Self::GlobalKillSwitchRequested { .. } => DangerLevel::Dangerous,
        }
    }
}
