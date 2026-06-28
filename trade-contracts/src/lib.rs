pub mod trading {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/trading.v1.rs"));
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    #[test]
    fn encodes_and_decodes_event_envelope_contract() {
        let envelope = super::trading::v1::EventEnvelope {
            event_id: "evt-contract-001".to_string(),
            event_type: "StrategyHeartbeat".to_string(),
            aggregate_type: "strategy".to_string(),
            aggregate_id: "open-scalp".to_string(),
            correlation_id: "corr-contract-001".to_string(),
            causation_id: String::new(),
            source_ts_ns: 1,
            ingest_ts_ns: 2,
            publish_ts_ns: 3,
            sequence: 4,
            producer: "trade-contracts-test".to_string(),
            schema_version: "trading.events.v1".to_string(),
            payload: Vec::new(),
        };

        let mut bytes = Vec::new();
        envelope.encode(&mut bytes).expect("encode envelope");
        let decoded =
            super::trading::v1::EventEnvelope::decode(bytes.as_slice()).expect("decode envelope");

        assert_eq!(decoded.event_id, "evt-contract-001");
        assert_eq!(decoded.sequence, 4);
        assert_eq!(decoded.correlation_id, "corr-contract-001");
    }
}
