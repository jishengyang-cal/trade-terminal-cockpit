use crate::events::{DomainEvent, EventEnvelope};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EventFilter {
    pub strategy_id: Option<String>,
    pub symbol: Option<String>,
    pub order_id: Option<String>,
    pub correlation_id: Option<String>,
    pub event_type: Option<String>,
    pub severity: Option<String>,
    pub source: Option<String>,
    pub from_ts_ns: Option<i64>,
    pub to_ts_ns: Option<i64>,
}

impl EventFilter {
    pub fn is_empty(&self) -> bool {
        self.strategy_id.is_none()
            && self.symbol.is_none()
            && self.order_id.is_none()
            && self.correlation_id.is_none()
            && self.event_type.is_none()
            && self.severity.is_none()
            && self.source.is_none()
            && self.from_ts_ns.is_none()
            && self.to_ts_ns.is_none()
    }

    pub fn matches(&self, envelope: &EventEnvelope) -> bool {
        self.matches_timestamp(envelope)
            && self.matches_text(
                self.correlation_id.as_deref(),
                Some(envelope.correlation_id.as_str()),
            )
            && self.matches_text(
                self.event_type.as_deref(),
                Some(envelope.event_type.as_str()),
            )
            && self.matches_text(self.source.as_deref(), Some(envelope.producer.as_str()))
            && self.matches_text(
                self.strategy_id.as_deref(),
                event_strategy_id(&envelope.payload),
            )
            && self.matches_text(self.symbol.as_deref(), event_symbol(&envelope.payload))
            && self.matches_text(self.order_id.as_deref(), event_order_id(&envelope.payload))
            && self.matches_text(self.severity.as_deref(), event_severity(&envelope.payload))
    }

    pub fn summary(&self) -> Option<String> {
        if self.is_empty() {
            return None;
        }

        let mut parts = Vec::new();
        push_part(&mut parts, "strategy", self.strategy_id.as_deref());
        push_part(&mut parts, "symbol", self.symbol.as_deref());
        push_part(&mut parts, "order", self.order_id.as_deref());
        push_part(&mut parts, "corr", self.correlation_id.as_deref());
        push_part(&mut parts, "type", self.event_type.as_deref());
        push_part(&mut parts, "severity", self.severity.as_deref());
        push_part(&mut parts, "source", self.source.as_deref());
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

fn event_severity(event: &DomainEvent) -> Option<&str> {
    match event {
        DomainEvent::RiskLimitBreached(event) => Some(&event.severity),
        DomainEvent::AlertRaised(event) => Some(&event.severity),
        _ => None,
    }
}
