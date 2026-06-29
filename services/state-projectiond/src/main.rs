use anyhow::{Context, Result};
use clap::Parser;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};
use trade_core::events::IngestDiagnosticRecorded;
use trade_core::state::{AlertView, AppState};
use trade_core::EventFilter;
use trade_core::{
    decode_event_envelope, reduce_event, DomainEvent, EventCodec, EventEnvelope, ProjectionSnapshot,
};

#[derive(Debug, Parser)]
#[command(name = "state-projectiond")]
#[command(about = "Build terminal projection snapshots from trading event JSONL")]
struct Cli {
    #[arg(long, value_name = "PATH")]
    event_jsonl: Option<PathBuf>,

    #[arg(long, value_name = "PATH")]
    event_store_query_bin: Option<PathBuf>,

    #[arg(long, value_name = "URI")]
    event_store_uri: Option<String>,

    #[arg(long, default_value_t = 5000)]
    event_store_timeout_ms: u64,

    #[arg(long, value_name = "URL")]
    nats_url: Option<String>,

    #[arg(long = "nats-subject", value_name = "SUBJECT", requires = "nats_url")]
    nats_subjects: Vec<String>,

    #[arg(long, default_value = "json", value_parser = ["json", "protobuf"])]
    event_codec: String,

    #[arg(long, value_name = "STREAM", requires = "nats_url")]
    jetstream_stream: Option<String>,

    #[arg(long, value_name = "DURABLE", requires = "nats_url")]
    jetstream_durable: Option<String>,

    #[arg(long, value_name = "PATH")]
    output_json: Option<PathBuf>,

    #[arg(long, value_name = "ADDR")]
    serve: Option<String>,

    #[arg(long, default_value = "trading.projections.v1")]
    schema_version: String,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut state = load_state(&cli)?;
    state.connection.nats = projection_source(&cli);
    let state = Arc::new(RwLock::new(state));
    let event_codec = cli.event_codec.parse::<EventCodec>()?;
    spawn_live_ingest(&cli, event_codec, state.clone())?;

    if let Some(addr) = cli.serve.as_deref() {
        return serve_projection(addr, state, cli.schema_version);
    }

    let snapshot = {
        let state = state.read().expect("state lock poisoned").clone();
        build_snapshot(state, cli.schema_version.clone(), projection_source(&cli))
    };
    let json = serde_json::to_string_pretty(&snapshot)?;
    if let Some(path) = cli.output_json {
        fs::write(&path, json).with_context(|| format!("failed to write {}", path.display()))?;
    } else {
        println!("{json}");
    }
    Ok(())
}

fn load_state(cli: &Cli) -> Result<AppState> {
    let mut state = AppState::default();
    if let Some(path) = cli.event_jsonl.as_ref() {
        reduce_events(&mut state, load_events_jsonl(path)?);
    }
    if cli.event_store_query_bin.is_some() {
        reduce_events(&mut state, load_events_from_event_store(cli)?);
    }
    Ok(state)
}

fn reduce_events(state: &mut AppState, events: impl IntoIterator<Item = EventEnvelope>) {
    for event in events {
        reduce_event(state, event);
    }
}

fn load_events_jsonl(path: &PathBuf) -> Result<Vec<EventEnvelope>> {
    let content =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
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

fn load_events_from_event_store(cli: &Cli) -> Result<Vec<EventEnvelope>> {
    let bin = cli
        .event_store_query_bin
        .as_ref()
        .expect("checked by caller");
    let mut process = Command::new(bin)
        .arg("--query-events")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to launch event-store adapter {}", bin.display()))?;
    {
        let mut stdin = process
            .stdin
            .take()
            .context("event-store adapter stdin unavailable")?;
        let request = serde_json::json!({
            "event_store_uri": cli.event_store_uri,
            "filter": EventFilter::default(),
        });
        writeln!(stdin, "{}", serde_json::to_string(&request)?)?;
    }
    let output = wait_with_timeout(
        process,
        Duration::from_millis(cli.event_store_timeout_ms.max(1)),
        &format!("event-store adapter {}", bin.display()),
    )?;
    if !output.status.success() {
        anyhow::bail!(
            "event-store adapter {} exited with {}: {}",
            bin.display(),
            output
                .status
                .code()
                .map_or_else(|| "signal".to_string(), |code| code.to_string()),
            compact_process_text(&output.stderr)
        );
    }
    decode_event_jsonl_bytes(&output.stdout, "event-store adapter stdout")
}

fn wait_with_timeout(mut process: Child, timeout: Duration, label: &str) -> Result<Output> {
    let start = Instant::now();
    loop {
        if process
            .try_wait()
            .with_context(|| format!("failed to poll {label}"))?
            .is_some()
        {
            return process
                .wait_with_output()
                .with_context(|| format!("failed to collect {label} output"));
        }
        if start.elapsed() >= timeout {
            let _ = process.kill();
            let output = process
                .wait_with_output()
                .with_context(|| format!("failed to collect timed out {label} output"))?;
            anyhow::bail!(
                "{label} timed out after {}ms stderr={}",
                timeout.as_millis(),
                compact_process_text(&output.stderr)
            );
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn decode_event_jsonl_bytes(bytes: &[u8], source: &str) -> Result<Vec<EventEnvelope>> {
    let text = String::from_utf8_lossy(bytes);
    let mut events = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let event = serde_json::from_str::<EventEnvelope>(line)
            .with_context(|| format!("failed to decode {source} line {}", index + 1))?;
        events.push(event);
    }
    Ok(events)
}

fn compact_process_text(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.is_empty() {
        "<empty>".to_string()
    } else if compact.chars().count() > 240 {
        format!("{}...", compact.chars().take(240).collect::<String>())
    } else {
        compact
    }
}

fn projection_source(cli: &Cli) -> String {
    if cli.jetstream_stream.is_some() || cli.jetstream_durable.is_some() {
        "state-projectiond-jetstream".to_string()
    } else if cli.nats_url.is_some() {
        "state-projectiond-nats".to_string()
    } else if cli.event_store_query_bin.is_some() {
        "state-projectiond-event-store".to_string()
    } else if cli.event_jsonl.is_some() {
        "state-projectiond-jsonl".to_string()
    } else {
        "state-projectiond-empty".to_string()
    }
}

fn spawn_live_ingest(
    cli: &Cli,
    event_codec: EventCodec,
    state: Arc<RwLock<AppState>>,
) -> Result<()> {
    let Some(url) = cli.nats_url.as_deref() else {
        return Ok(());
    };

    if let (Some(stream), Some(durable)) = (
        cli.jetstream_stream.as_deref(),
        cli.jetstream_durable.as_deref(),
    ) {
        spawn_jetstream_consumer(
            url.to_string(),
            stream.to_string(),
            durable.to_string(),
            cli.nats_subjects.clone(),
            event_codec,
            state,
        )?;
        return Ok(());
    }

    for subject in &cli.nats_subjects {
        spawn_nats_subject(url.to_string(), subject.clone(), event_codec, state.clone())?;
    }
    Ok(())
}

fn spawn_jetstream_consumer(
    url: String,
    stream_name: String,
    durable_name: String,
    subjects: Vec<String>,
    event_codec: EventCodec,
    state: Arc<RwLock<AppState>>,
) -> Result<()> {
    thread::Builder::new()
        .name(format!("state-projectiond-js-{stream_name}-{durable_name}"))
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    apply_ingest_diagnostic(
                        &state,
                        "jetstream",
                        Some(&stream_name),
                        Some(&durable_name),
                        None,
                        "error",
                        format!("failed to create runtime: {error}"),
                        Some("runtime"),
                        false,
                        false,
                        0,
                        0,
                    );
                    return;
                }
            };
            runtime.block_on(async move {
                loop {
                    if let Err(error) = run_jetstream_once(
                        &url,
                        &stream_name,
                        &durable_name,
                        &subjects,
                        event_codec,
                        &state,
                    )
                    .await
                    {
                        apply_ingest_diagnostic(
                            &state,
                            "jetstream",
                            Some(&stream_name),
                            Some(&durable_name),
                            None,
                            "error",
                            format!("{stream_name}/{durable_name} disconnected: {error}"),
                            Some("disconnect"),
                            true,
                            false,
                            0,
                            0,
                        );
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            });
        })
        .context("failed to spawn state-projectiond jetstream thread")?;
    Ok(())
}

async fn run_jetstream_once(
    url: &str,
    stream_name: &str,
    durable_name: &str,
    subjects: &[String],
    event_codec: EventCodec,
    state: &Arc<RwLock<AppState>>,
) -> Result<()> {
    let client = async_nats::connect(url.to_string()).await?;
    let jetstream = async_nats::jetstream::new(client);
    let stream = jetstream.get_stream(stream_name.to_string()).await?;
    let filter_subject = subjects
        .iter()
        .find(|subject| !subject.trim().is_empty())
        .cloned()
        .unwrap_or_default();
    let consumer = stream
        .get_or_create_consumer(
            durable_name,
            async_nats::jetstream::consumer::pull::Config {
                durable_name: Some(durable_name.to_string()),
                filter_subject: filter_subject.clone(),
                ..Default::default()
            },
        )
        .await?;
    apply_ingest_diagnostic(
        state,
        "jetstream",
        Some(stream_name),
        Some(durable_name),
        Some(&filter_subject),
        "info",
        "consumer connected",
        None,
        false,
        false,
        0,
        0,
    );

    let mut messages = consumer.messages().await?;
    while let Some(message) = messages.next().await {
        let message = message?;
        match decode_event_envelope(message.payload.as_ref(), event_codec) {
            Ok(mut event) => {
                stamp_ingested_event(&mut event, Some(stream_name), Some(&filter_subject));
                apply_event(state, event);
                message
                    .ack()
                    .await
                    .map_err(|error| anyhow::anyhow!("jetstream ack failed: {error}"))?;
            }
            Err(error) => {
                apply_ingest_diagnostic(
                    state,
                    "jetstream",
                    Some(stream_name),
                    Some(durable_name),
                    Some(&filter_subject),
                    "error",
                    format!(
                        "failed to decode {stream_name} with {:?}: {error}",
                        event_codec
                    ),
                    Some("decode"),
                    false,
                    true,
                    0,
                    0,
                );
                message
                    .ack()
                    .await
                    .map_err(|error| anyhow::anyhow!("jetstream ack failed: {error}"))?;
            }
        }
    }
    Ok(())
}

fn spawn_nats_subject(
    url: String,
    subject: String,
    event_codec: EventCodec,
    state: Arc<RwLock<AppState>>,
) -> Result<()> {
    thread::Builder::new()
        .name(format!("state-projectiond-nats-{subject}"))
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    apply_ingest_diagnostic(
                        &state,
                        "nats",
                        None,
                        None,
                        Some(&subject),
                        "error",
                        format!("failed to create runtime: {error}"),
                        Some("runtime"),
                        false,
                        false,
                        0,
                        0,
                    );
                    return;
                }
            };
            runtime.block_on(async move {
                loop {
                    match async_nats::connect(url.clone()).await {
                        Ok(client) => match client.subscribe(subject.clone()).await {
                            Ok(mut subscriber) => {
                                apply_ingest_diagnostic(
                                    &state,
                                    "nats",
                                    None,
                                    None,
                                    Some(&subject),
                                    "info",
                                    "subject subscribed",
                                    None,
                                    false,
                                    false,
                                    0,
                                    0,
                                );
                                while let Some(message) = subscriber.next().await {
                                    match decode_event_envelope(
                                        message.payload.as_ref(),
                                        event_codec,
                                    ) {
                                        Ok(mut event) => {
                                            stamp_ingested_event(&mut event, None, Some(&subject));
                                            apply_event(&state, event);
                                        }
                                        Err(error) => apply_ingest_diagnostic(
                                            &state,
                                            "nats",
                                            None,
                                            None,
                                            Some(&subject),
                                            "error",
                                            format!(
                                                "failed to decode {subject} with {:?}: {error}",
                                                event_codec
                                            ),
                                            Some("decode"),
                                            false,
                                            true,
                                            0,
                                            0,
                                        ),
                                    }
                                }
                            }
                            Err(error) => apply_ingest_diagnostic(
                                &state,
                                "nats",
                                None,
                                None,
                                Some(&subject),
                                "error",
                                format!("failed to subscribe {subject}: {error}"),
                                Some("subscribe"),
                                true,
                                false,
                                0,
                                0,
                            ),
                        },
                        Err(error) => apply_ingest_diagnostic(
                            &state,
                            "nats",
                            None,
                            None,
                            Some(&subject),
                            "error",
                            format!("failed to connect {url}: {error}"),
                            Some("connect"),
                            true,
                            false,
                            0,
                            0,
                        ),
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            });
        })
        .context("failed to spawn state-projectiond nats thread")?;
    Ok(())
}

fn stamp_ingested_event(event: &mut EventEnvelope, stream: Option<&str>, subject: Option<&str>) {
    let now = trade_core::unix_ts_ns();
    event.receive_ts_ns = Some(now);
    event.ingest_ts_ns = now;
    if event.stream.is_empty() {
        if let Some(stream) = stream {
            event.stream = stream.to_string();
        }
    }
    if event.subject.is_empty() {
        if let Some(subject) = subject {
            event.subject = subject.to_string();
        }
    }
}

fn apply_event(state: &Arc<RwLock<AppState>>, event: EventEnvelope) {
    let mut guard = state.write().expect("state lock poisoned");
    reduce_event(&mut guard, event);
}

#[allow(clippy::too_many_arguments)]
fn apply_ingest_diagnostic(
    state: &Arc<RwLock<AppState>>,
    source: &str,
    stream: Option<&str>,
    consumer: Option<&str>,
    subject: Option<&str>,
    severity: &str,
    message: impl Into<String>,
    error_kind: Option<&str>,
    reconnect: bool,
    decode_error: bool,
    filtered_count: u64,
    acked_count: u64,
) {
    let ts = trade_core::unix_ts_ns();
    let mut envelope = EventEnvelope::new(
        format!("projectiond-ingest-{source}-{ts}"),
        source.to_string(),
        ts as u64,
        "state-projectiond-ingest",
        DomainEvent::IngestDiagnosticRecorded(IngestDiagnosticRecorded {
            source: source.to_string(),
            stream: stream.map(str::to_string),
            consumer: consumer.map(str::to_string),
            subject: subject.map(str::to_string),
            severity: severity.to_string(),
            message: message.into(),
            error_kind: error_kind.map(str::to_string),
            reconnect,
            decode_error,
            filtered_count,
            acked_count,
        }),
    );
    envelope.stream = stream.unwrap_or_default().to_string();
    envelope.subject = subject.unwrap_or_default().to_string();
    apply_event(state, envelope);
}

fn build_snapshot(state: AppState, schema_version: String, source: String) -> ProjectionSnapshot {
    ProjectionSnapshot {
        schema_version,
        snapshot_ts_ns: state.connection.last_event_ts_ns.unwrap_or_default(),
        source,
        last_event_sequence: state.connection.last_event_sequence,
        account: Some(state.account),
        accounts: state.accounts.by_id.into_values().collect(),
        market_data: state.market_data.by_symbol.into_values().collect(),
        strategies: state.strategies.by_id.into_values().collect(),
        orders: state.orders.by_correlation_id.into_values().collect(),
        positions: state.positions.by_key.into_values().collect(),
        risk: Some(state.risk),
        alerts: state.alerts.by_id.into_values().collect::<Vec<AlertView>>(),
    }
}

fn serve_projection(
    addr: &str,
    state: Arc<RwLock<AppState>>,
    schema_version: String,
) -> Result<()> {
    let listener = TcpListener::bind(addr)
        .with_context(|| format!("failed to bind state-projectiond on {addr}"))?;
    eprintln!("state-projectiond listening on {addr}");
    for stream in listener.incoming() {
        let stream = stream.context("failed to accept state-projectiond client")?;
        let state = state.clone();
        let schema_version = schema_version.clone();
        thread::Builder::new()
            .name("state-projectiond-client".to_string())
            .spawn(move || {
                if let Err(error) = handle_client(stream, state, schema_version) {
                    eprintln!("state-projectiond client error: {error}");
                }
            })
            .context("failed to spawn state-projectiond client thread")?;
    }
    Ok(())
}

fn handle_client(
    mut stream: TcpStream,
    state: Arc<RwLock<AppState>>,
    schema_version: String,
) -> Result<()> {
    let peer = stream.peer_addr().ok();
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    while reader.read_line(&mut line)? != 0 {
        let response = match serde_json::from_str::<ProjectionQuery>(line.trim()) {
            Ok(query) => {
                let state = state.read().expect("state lock poisoned").clone();
                let snapshot_source = state.connection.nats.clone();
                let snapshot =
                    build_snapshot(state.clone(), schema_version.clone(), snapshot_source);
                query_projection(&state, &snapshot, query)
            }
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
