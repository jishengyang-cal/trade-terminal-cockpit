use crate::events::{DomainEvent, EventEnvelope};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventFilter {
    pub account_id: Option<String>,
    pub strategy_id: Option<String>,
    pub symbol: Option<String>,
    pub order_id: Option<String>,
    pub broker_order_id: Option<String>,
    pub perm_id: Option<String>,
    pub command_id: Option<String>,
    pub correlation_id: Option<String>,
    pub event_type: Option<String>,
    pub severity: Option<String>,
    pub source: Option<String>,
    pub producer: Option<String>,
    pub stream: Option<String>,
    pub subject: Option<String>,
    pub min_sequence: Option<u64>,
    pub max_sequence: Option<u64>,
    pub state: Option<String>,
    pub risk_rule_id: Option<String>,
    pub venue: Option<String>,
    pub route: Option<String>,
    pub from_ts_ns: Option<i64>,
    pub to_ts_ns: Option<i64>,
}

impl EventFilter {
    pub fn is_empty(&self) -> bool {
        self.account_id.is_none()
            && self.strategy_id.is_none()
            && self.symbol.is_none()
            && self.order_id.is_none()
            && self.broker_order_id.is_none()
            && self.perm_id.is_none()
            && self.command_id.is_none()
            && self.correlation_id.is_none()
            && self.event_type.is_none()
            && self.severity.is_none()
            && self.source.is_none()
            && self.producer.is_none()
            && self.stream.is_none()
            && self.subject.is_none()
            && self.min_sequence.is_none()
            && self.max_sequence.is_none()
            && self.state.is_none()
            && self.risk_rule_id.is_none()
            && self.venue.is_none()
            && self.route.is_none()
            && self.from_ts_ns.is_none()
            && self.to_ts_ns.is_none()
    }

    pub fn matches(&self, envelope: &EventEnvelope) -> bool {
        self.matches_timestamp(envelope)
            && self.matches_sequence(envelope)
            && self.matches_text(
                self.correlation_id.as_deref(),
                Some(envelope.correlation_id.as_str()),
            )
            && self.matches_text(
                self.event_type.as_deref(),
                Some(envelope.event_type.as_str()),
            )
            && self.matches_text(self.source.as_deref(), Some(envelope.producer.as_str()))
            && self.matches_text(self.producer.as_deref(), Some(envelope.producer.as_str()))
            && self.matches_text(self.stream.as_deref(), Some(envelope.stream.as_str()))
            && self.matches_text(self.subject.as_deref(), Some(envelope.subject.as_str()))
            && self.matches_text(
                self.account_id.as_deref(),
                event_account_id(&envelope.payload),
            )
            && self.matches_text(
                self.strategy_id.as_deref(),
                event_strategy_id(&envelope.payload),
            )
            && self.matches_text(self.symbol.as_deref(), event_symbol(&envelope.payload))
            && self.matches_text(self.order_id.as_deref(), event_order_id(&envelope.payload))
            && self.matches_text(
                self.broker_order_id.as_deref(),
                event_broker_order_id(&envelope.payload),
            )
            && self.matches_text(self.perm_id.as_deref(), event_perm_id(&envelope.payload))
            && self.matches_text(
                self.command_id.as_deref(),
                event_command_id(&envelope.payload),
            )
            && self.matches_text(self.severity.as_deref(), event_severity(&envelope.payload))
            && self.matches_text(self.state.as_deref(), event_state(&envelope.payload))
            && self.matches_text(
                self.risk_rule_id.as_deref(),
                event_risk_rule_id(&envelope.payload),
            )
            && self.matches_text(self.venue.as_deref(), event_venue(&envelope.payload))
            && self.matches_text(self.route.as_deref(), event_route(&envelope.payload))
    }

    pub fn summary(&self) -> Option<String> {
        if self.is_empty() {
            return None;
        }

        let mut parts = Vec::new();
        push_part(&mut parts, "account", self.account_id.as_deref());
        push_part(&mut parts, "strategy", self.strategy_id.as_deref());
        push_part(&mut parts, "symbol", self.symbol.as_deref());
        push_part(&mut parts, "order", self.order_id.as_deref());
        push_part(&mut parts, "broker_order", self.broker_order_id.as_deref());
        push_part(&mut parts, "perm", self.perm_id.as_deref());
        push_part(&mut parts, "command", self.command_id.as_deref());
        push_part(&mut parts, "corr", self.correlation_id.as_deref());
        push_part(&mut parts, "type", self.event_type.as_deref());
        push_part(&mut parts, "severity", self.severity.as_deref());
        push_part(&mut parts, "source", self.source.as_deref());
        push_part(&mut parts, "producer", self.producer.as_deref());
        push_part(&mut parts, "stream", self.stream.as_deref());
        push_part(&mut parts, "subject", self.subject.as_deref());
        push_part(&mut parts, "state", self.state.as_deref());
        push_part(&mut parts, "risk_rule", self.risk_rule_id.as_deref());
        push_part(&mut parts, "venue", self.venue.as_deref());
        push_part(&mut parts, "route", self.route.as_deref());
        if let Some(min_sequence) = self.min_sequence {
            parts.push(format!("min_sequence={min_sequence}"));
        }
        if let Some(max_sequence) = self.max_sequence {
            parts.push(format!("max_sequence={max_sequence}"));
        }
        if let Some(from_ts_ns) = self.from_ts_ns {
            parts.push(format!("from_ts_ns={from_ts_ns}"));
        }
        if let Some(to_ts_ns) = self.to_ts_ns {
            parts.push(format!("to_ts_ns={to_ts_ns}"));
        }

        Some(parts.join(" "))
    }

    fn matches_timestamp(&self, envelope: &EventEnvelope) -> bool {
        if let Some(from_ts_ns) = self.from_ts_ns {
            if envelope.publish_ts_ns < from_ts_ns {
                return false;
            }
        }
        if let Some(to_ts_ns) = self.to_ts_ns {
            if envelope.publish_ts_ns > to_ts_ns {
                return false;
            }
        }
        true
    }

    fn matches_sequence(&self, envelope: &EventEnvelope) -> bool {
        if let Some(min_sequence) = self.min_sequence {
            if envelope.sequence < min_sequence {
                return false;
            }
        }
        if let Some(max_sequence) = self.max_sequence {
            if envelope.sequence > max_sequence {
                return false;
            }
        }
        true
    }

    fn matches_text(&self, expected: Option<&str>, actual: Option<&str>) -> bool {
        match expected {
            Some(expected) => actual
                .map(|actual| actual.eq_ignore_ascii_case(expected))
                .unwrap_or(false),
            None => true,
        }
    }
}

fn push_part(parts: &mut Vec<String>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        parts.push(format!("{key}={value}"));
    }
}

fn event_account_id(event: &DomainEvent) -> Option<&str> {
    match event {
        DomainEvent::SignalGenerated(event) => event.account_id.as_deref(),
        DomainEvent::IntentCreated(event) => event.account_id.as_deref(),
        DomainEvent::OrderSubmitRequested(event) => Some(&event.account_id),
        DomainEvent::OrderSubmitted(event) => Some(&event.account_id),
        DomainEvent::BrokerAckReceived(event) => Some(&event.account_id),
        DomainEvent::OrderPartiallyFilled(event) => Some(&event.account_id),
        DomainEvent::OrderFilled(event) => Some(&event.account_id),
        DomainEvent::CancelRequested(event) => Some(&event.account_id),
        DomainEvent::CancelRejected(event) => Some(&event.account_id),
        DomainEvent::OrderCancelled(event) => Some(&event.account_id),
        DomainEvent::OrderRejected(event) => Some(&event.account_id),
        DomainEvent::PositionSnapshot(event) => Some(&event.account_id),
        _ => None,
    }
}

fn event_strategy_id(event: &DomainEvent) -> Option<&str> {
    match event {
        DomainEvent::StrategyHeartbeat(event) => Some(&event.strategy_id),
        DomainEvent::StrategyStateChanged(event) => Some(&event.strategy_id),
        DomainEvent::SignalGenerated(event) => Some(&event.strategy_id),
        DomainEvent::IntentCreated(event) => Some(&event.strategy_id),
        DomainEvent::RiskDecisionMade(event) => Some(&event.strategy_id),
        _ => None,
    }
}

fn event_symbol(event: &DomainEvent) -> Option<&str> {
    match event {
        DomainEvent::SignalGenerated(event) => Some(&event.symbol),
        DomainEvent::IntentCreated(event) => Some(&event.symbol),
        DomainEvent::RiskDecisionMade(event) => Some(&event.symbol),
        DomainEvent::PositionSnapshot(event) => Some(&event.symbol),
        _ => None,
    }
}

fn event_order_id(event: &DomainEvent) -> Option<&str> {
    match event {
        DomainEvent::OrderSubmitRequested(event) => Some(&event.order_id),
        DomainEvent::OrderSubmitted(event) => Some(&event.order_id),
        DomainEvent::BrokerAckReceived(event) => Some(&event.order_id),
        DomainEvent::OrderPartiallyFilled(event) => Some(&event.order_id),
        DomainEvent::OrderFilled(event) => Some(&event.order_id),
        DomainEvent::CancelRequested(event) => Some(&event.order_id),
        DomainEvent::CancelRejected(event) => Some(&event.order_id),
        DomainEvent::OrderCancelled(event) => Some(&event.order_id),
        DomainEvent::OrderRejected(event) => Some(&event.order_id),
        _ => None,
    }
}

fn event_broker_order_id(event: &DomainEvent) -> Option<&str> {
    match event {
        DomainEvent::OrderSubmitRequested(event) => event.broker_order_id.as_deref(),
        DomainEvent::OrderSubmitted(event) => event.broker_order_id.as_deref(),
        DomainEvent::BrokerAckReceived(event) => Some(&event.broker_order_id),
        _ => None,
    }
}

fn event_perm_id(event: &DomainEvent) -> Option<&str> {
    match event {
        DomainEvent::OrderSubmitRequested(event) => event.perm_id.as_deref(),
        DomainEvent::OrderSubmitted(event) => event.perm_id.as_deref(),
        DomainEvent::BrokerAckReceived(event) => event.perm_id.as_deref(),
        _ => None,
    }
}

fn event_command_id(event: &DomainEvent) -> Option<&str> {
    match event {
        DomainEvent::CommandAuditRecorded(event) => Some(&event.command_id),
        _ => None,
    }
}

fn event_severity(event: &DomainEvent) -> Option<&str> {
    match event {
        DomainEvent::RiskLimitBreached(event) => Some(&event.severity),
        DomainEvent::AlertRaised(event) => Some(&event.severity),
        _ => None,
    }
}

fn event_state(event: &DomainEvent) -> Option<&str> {
    match event {
        DomainEvent::StrategyHeartbeat(event) => Some(&event.state),
        DomainEvent::StrategyStateChanged(event) => Some(&event.state),
        DomainEvent::BrokerAckReceived(event) => Some(&event.broker_status),
        _ => None,
    }
}

fn event_risk_rule_id(event: &DomainEvent) -> Option<&str> {
    match event {
        DomainEvent::RiskDecisionMade(event) => event
            .evaluated_rules
            .iter()
            .find(|rule| !rule.passed)
            .or_else(|| event.evaluated_rules.first())
            .map(|rule| rule.rule_id.as_str()),
        DomainEvent::RiskLimitBreached(event) => event.rule_id.as_deref(),
        _ => None,
    }
}

fn event_venue(event: &DomainEvent) -> Option<&str> {
    match event {
        DomainEvent::OrderPartiallyFilled(event) | DomainEvent::OrderFilled(event) => {
            event.venue.as_deref()
        }
        _ => None,
    }
}

fn event_route(event: &DomainEvent) -> Option<&str> {
    match event {
        DomainEvent::OrderSubmitRequested(event) => event.route.as_deref(),
        DomainEvent::OrderSubmitted(event) => event.route.as_deref(),
        _ => None,
    }
}
