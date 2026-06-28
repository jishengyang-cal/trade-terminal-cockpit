use anyhow::{Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
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

    #[arg(long, value_name = "ADDR")]
    serve: Option<String>,

    #[arg(long, default_value = "trading.projections.v1")]
    schema_version: String,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let state = load_state(&cli.event_jsonl)?;
    let snapshot = build_snapshot(state.clone(), cli.schema_version.clone());

    if let Some(addr) = cli.serve.as_deref() {
        return serve_projection(addr, state, snapshot);
    }

    let json = serde_json::to_string_pretty(&snapshot)?;
    if let Some(path) = cli.output_json {
        fs::write(&path, json).with_context(|| format!("failed to write {}", path.display()))?;
    } else {
        println!("{json}");
    }
    Ok(())
}

fn load_state(path: &PathBuf) -> Result<AppState> {
    let content =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
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
                path.display()
            )
        })?;
        reduce_event(&mut state, event);
    }
    Ok(state)
}

fn build_snapshot(state: AppState, schema_version: String) -> ProjectionSnapshot {
    ProjectionSnapshot {
        schema_version,
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
    }
}

fn serve_projection(addr: &str, state: AppState, snapshot: ProjectionSnapshot) -> Result<()> {
    let listener = TcpListener::bind(addr)
        .with_context(|| format!("failed to bind state-projectiond on {addr}"))?;
    eprintln!("state-projectiond listening on {addr}");
    for stream in listener.incoming() {
        let stream = stream.context("failed to accept state-projectiond client")?;
        handle_client(stream, &state, &snapshot)?;
    }
    Ok(())
}

fn handle_client(
    mut stream: TcpStream,
    state: &AppState,
    snapshot: &ProjectionSnapshot,
) -> Result<()> {
    let peer = stream.peer_addr().ok();
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    while reader.read_line(&mut line)? != 0 {
        let response = match serde_json::from_str::<ProjectionQuery>(line.trim()) {
            Ok(query) => query_projection(state, snapshot, query),
            Err(error) => ProjectionResponse::error("invalid_request", error.to_string()),
        };
        writeln!(stream, "{}", serde_json::to_string(&response)?)?;
        line.clear();
    }
    if let Some(peer) = peer {
        eprintln!("state-projectiond client disconnected: {peer}");
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize)]
struct ProjectionQuery {
    method: String,
    #[serde(default)]
    account_id: Option<String>,
    #[serde(default)]
    strategy_id: Option<String>,
    #[serde(default)]
    order_id: Option<String>,
    #[serde(default)]
    correlation_id: Option<String>,
    #[serde(default)]
    symbol: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct ProjectionResponse {
    status: String,
    method: String,
    data: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl ProjectionResponse {
    fn ok(method: impl Into<String>, data: Value) -> Self {
        Self {
            status: "ok".to_string(),
            method: method.into(),
            data,
            error: None,
        }
    }

    fn error(method: impl Into<String>, error: String) -> Self {
        Self {
            status: "error".to_string(),
            method: method.into(),
            data: Value::Null,
            error: Some(error),
        }
    }
}

fn query_projection(
    state: &AppState,
    snapshot: &ProjectionSnapshot,
    query: ProjectionQuery,
) -> ProjectionResponse {
    match query.method.as_str() {
        "GetProjectionSnapshot" | "projection_snapshot" => {
            ProjectionResponse::ok(query.method, json!(snapshot))
        }
        "GetOverviewSnapshot" | "overview" => ProjectionResponse::ok(
            query.method,
            json!({
                "connection": state.connection,
                "account": state.account,
                "accounts": state.accounts.by_id.values().collect::<Vec<_>>(),
                "risk": state.risk,
                "alerts_open": state.alerts.open_count(),
            }),
        ),
        "GetStrategySnapshot" | "strategies" => {
            let strategies = state
                .strategies
                .by_id
                .values()
                .filter(|strategy| {
                    query
                        .strategy_id
                        .as_deref()
                        .map(|id| strategy.strategy_id == id)
                        .unwrap_or(true)
                })
                .collect::<Vec<_>>();
            ProjectionResponse::ok(query.method, json!(strategies))
        }
        "GetOrderSnapshot" | "orders" => {
            let orders = state
                .orders
                .by_correlation_id
                .values()
                .filter(|order| order_matches(order, &query))
                .collect::<Vec<_>>();
            ProjectionResponse::ok(query.method, json!(orders))
        }
        "GetPositionSnapshot" | "positions" => {
            let positions = state
                .positions
                .by_key
                .values()
                .filter(|position| {
                    query
                        .account_id
                        .as_deref()
                        .map(|account_id| position.account_id == account_id)
                        .unwrap_or(true)
                        && query
                            .symbol
                            .as_deref()
                            .map(|symbol| position.symbol == symbol)
                            .unwrap_or(true)
                })
                .collect::<Vec<_>>();
            ProjectionResponse::ok(query.method, json!(positions))
        }
        "GetRiskSnapshot" | "risk" => ProjectionResponse::ok(query.method, json!(state.risk)),
        "GetOrderTimeline" | "order_timeline" => {
            let timelines = state
                .orders
                .by_correlation_id
                .values()
                .filter(|order| order_matches(order, &query))
                .map(|order| {
                    json!({
                        "correlation_id": order.correlation_id,
                        "account_id": order.account_id,
                        "order_id": order.order_id,
                        "timeline": order.timeline,
                        "anomalies": order.anomalies,
                    })
                })
                .collect::<Vec<_>>();
            ProjectionResponse::ok(query.method, json!(timelines))
        }
        "GetEventsByCorrelationId" | "events_by_correlation_id" => {
            let events = state
                .audit
                .events
                .iter()
                .filter(|event| {
                    query
                        .correlation_id
                        .as_deref()
                        .map(|correlation_id| event.correlation_id == correlation_id)
                        .unwrap_or(true)
                })
                .collect::<Vec<_>>();
            ProjectionResponse::ok(query.method, json!(events))
        }
        other => ProjectionResponse::error(other, format!("unknown projection method {other}")),
    }
}

fn order_matches(order: &trade_core::state::OrderChain, query: &ProjectionQuery) -> bool {
    query
        .correlation_id
        .as_deref()
        .map(|correlation_id| order.correlation_id == correlation_id)
        .unwrap_or(true)
        && query
            .account_id
            .as_deref()
            .map(|account_id| order.account_id.as_deref() == Some(account_id))
            .unwrap_or(true)
        && query
            .order_id
            .as_deref()
            .map(|order_id| order.order_id.as_deref() == Some(order_id))
            .unwrap_or(true)
        && query
            .symbol
            .as_deref()
            .map(|symbol| order.symbol.as_deref() == Some(symbol))
            .unwrap_or(true)
}
