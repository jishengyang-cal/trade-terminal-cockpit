use anyhow::{Context, Result};
use clap::Parser;
use std::fs;
use std::path::PathBuf;
use trade_core::state::{AlertView, AppState};
use trade_core::{reduce_event, EventEnvelope, ProjectionSnapshot};

#[derive(Debug, Parser)]
#[command(name = "state-projectiond")]
#[command(about = "Build terminal projection snapshots from trading event JSONL")]
struct Cli {
    #[arg(long, value_name = "PATH")]
    event_jsonl: PathBuf,

    #[arg(long, value_name = "PATH")]
    output_json: Option<PathBuf>,

    #[arg(long, default_value = "trading.projections.v1")]
    schema_version: String,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let content = fs::read_to_string(&cli.event_jsonl)
        .with_context(|| format!("failed to read {}", cli.event_jsonl.display()))?;
    let mut state = AppState::default();
    for (index, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let event = serde_json::from_str::<EventEnvelope>(line).with_context(|| {
            format!(
                "failed to decode event jsonl line {} from {}",
                index + 1,
                cli.event_jsonl.display()
            )
        })?;
        reduce_event(&mut state, event);
    }

    let snapshot = ProjectionSnapshot {
        schema_version: cli.schema_version,
        snapshot_ts_ns: state.connection.last_event_ts_ns.unwrap_or_default(),
        source: "state-projectiond-jsonl".to_string(),
        last_event_sequence: state.connection.last_event_sequence,
        account: Some(state.account),
        accounts: state.accounts.by_id.into_values().collect(),
        strategies: state.strategies.by_id.into_values().collect(),
        orders: state.orders.by_correlation_id.into_values().collect(),
        positions: state.positions.by_key.into_values().collect(),
        risk: Some(state.risk),
        alerts: state.alerts.by_id.into_values().collect::<Vec<AlertView>>(),
    };

    let json = serde_json::to_string_pretty(&snapshot)?;
    if let Some(path) = cli.output_json {
        fs::write(&path, json).with_context(|| format!("failed to write {}", path.display()))?;
    } else {
        println!("{json}");
    }
    Ok(())
}
