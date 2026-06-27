use crate::cli::Cli;
use anyhow::{Context, Result};
use std::fs;
use trade_core::EventEnvelope;

pub fn load_events(cli: &Cli) -> Result<Vec<EventEnvelope>> {
    if cli.mock || cli.event_jsonl.is_none() {
        return Ok(trade_core::sample::sample_events());
    }

    let path = cli.event_jsonl.as_ref().expect("checked above");
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read event jsonl {}", path.display()))?;
    let mut events = Vec::new();
    for (index, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let event = serde_json::from_str::<EventEnvelope>(line).with_context(|| {
            format!(
                "failed to decode event jsonl line {} from {}",
                index + 1,
                path.display()
            )
        })?;
        events.push(event);
    }
    Ok(events)
}
