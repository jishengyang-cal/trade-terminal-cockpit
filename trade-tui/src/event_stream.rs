use crate::cli::Cli;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Duration;
use trade_core::events::IngestDiagnosticRecorded;
use trade_core::{DomainEvent, EventEnvelope, EventFilter};

pub fn load_events(cli: &Cli, filter: &EventFilter) -> Result<Vec<EventEnvelope>> {
    let events = if cli.event_store_query_bin.is_some() {
        load_events_from_event_store(cli, filter)?
    } else if cli.mock || (cli.event_jsonl.is_none() && cli.snapshot_json.is_none()) {
        trade_core::sample::sample_events()
    } else if cli.event_jsonl.is_none() {
        Vec::new()
    } else {
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
        events
    };

    Ok(events
        .into_iter()
        .filter(|event| filter.matches(event))
        .collect())
}

fn load_events_from_event_store(cli: &Cli, filter: &EventFilter) -> Result<Vec<EventEnvelope>> {
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
        let stdin = process
            .stdin
            .as_mut()
            .context("event-store adapter stdin unavailable")?;
        let request = serde_json::json!({
            "event_store_uri": cli.event_store_uri,
            "filter": filter,
        });
        writeln!(stdin, "{}", serde_json::to_string(&request)?)?;
    }
    let output = process.wait_with_output()?;
    if !output.status.success() {
        anyhow::bail!(
            "event-store adapter exited with {}: {}",
            output
                .status
                .code()
                .map_or_else(|| "signal".to_string(), |code| code.to_string()),
            compact_process_text(&output.stderr)
        );
    }
    decode_event_jsonl_bytes(&output.stdout, "event-store adapter stdout")
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

pub fn spawn_event_sources(
    cli: &Cli,
    filter: EventFilter,
) -> Result<Option<Receiver<EventEnvelope>>> {
    if cli.plain {
        return Ok(None);
    }

    let (tx, rx) = mpsc::channel();
    let mut spawned = false;

    if cli.follow {
        spawn_jsonl_follow(cli, filter.clone(), tx.clone())?;
        spawned = true;
    }

    if let Some(url) = cli.nats_url.as_deref() {
        let using_jetstream = if let (Some(stream), Some(durable)) = (
            cli.jetstream_stream.as_deref(),
            cli.jetstream_durable.as_deref(),
        ) {
            spawn_jetstream_consumer(
                url.to_string(),
                stream.to_string(),
                durable.to_string(),
                cli.nats_subjects.clone(),
                filter.clone(),
                tx.clone(),
            )?;
            spawned = true;
            true
        } else {
            false
        };

        if !using_jetstream {
            for subject in &cli.nats_subjects {
                spawn_nats_subject(url.to_string(), subject.clone(), filter.clone(), tx.clone())?;
                spawned = true;
            }
        }
    }

    if spawned {
        Ok(Some(rx))
    } else {
        Ok(None)
    }
}

fn spawn_jetstream_consumer(
    url: String,
    stream_name: String,
    durable_name: String,
    subjects: Vec<String>,
    filter: EventFilter,
    tx: Sender<EventEnvelope>,
) -> Result<()> {
    let thread_name = format!("jetstream-{stream_name}-{durable_name}");
    thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = send_ingest_diagnostic(
                        &tx,
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
                    if let Err(error) = run_jetstream_consumer_once(
                        &url,
                        &stream_name,
                        &durable_name,
                        &subjects,
                        &filter,
                        &tx,
                    )
                    .await
                    {
                        let _ = send_ingest_diagnostic(
                            &tx,
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
        .context("failed to spawn jetstream consumer thread")?;

    Ok(())
}

async fn run_jetstream_consumer_once(
    url: &str,
    stream_name: &str,
    durable_name: &str,
    subjects: &[String],
    filter: &EventFilter,
    tx: &Sender<EventEnvelope>,
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

    let _ = send_ingest_diagnostic(
        tx,
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
        match serde_json::from_slice::<EventEnvelope>(message.payload.as_ref()) {
            Ok(mut event) => {
                stamp_ingested_event(&mut event, Some(stream_name), Some(&filter_subject));
                if filter.matches(&event) {
                    tx.send(event)
                        .map_err(|_| anyhow::anyhow!("event receiver closed"))?;
                } else {
                    let _ = send_ingest_diagnostic(
                        tx,
                        "jetstream",
                        Some(stream_name),
                        Some(durable_name),
                        Some(&filter_subject),
                        "info",
                        "event filtered by active TUI filter",
                        None,
                        false,
                        false,
                        1,
                        0,
                    );
                }
                message
                    .ack()
                    .await
                    .map_err(|error| anyhow::anyhow!("jetstream ack failed: {error}"))?;
            }
            Err(error) => {
                let _ = send_ingest_diagnostic(
                    tx,
                    "jetstream",
                    Some(stream_name),
                    Some(durable_name),
                    Some(&filter_subject),
                    "error",
                    format!("failed to decode {stream_name}: {error}"),
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

fn spawn_jsonl_follow(cli: &Cli, filter: EventFilter, tx: Sender<EventEnvelope>) -> Result<()> {
    let path = cli
        .event_jsonl
        .clone()
        .expect("clap requires --event-jsonl when --follow is used");
    let poll = Duration::from_millis(cli.follow_poll_ms.max(10));

    thread::Builder::new()
        .name("event-jsonl-follow".to_string())
        .spawn(move || {
            let mut line_number = 0_u64;
            loop {
                let file = match File::open(&path) {
                    Ok(file) => file,
                    Err(error) => {
                        let _ = send_ingest_diagnostic(
                            &tx,
                            "jsonl-follow",
                            None,
                            None,
                            Some(&path.display().to_string()),
                            "error",
                            format!("failed to open {}: {error}", path.display()),
                            Some("open"),
                            false,
                            false,
                            0,
                            0,
                        );
                        thread::sleep(poll);
                        continue;
                    }
                };
                let mut reader = BufReader::new(file);
                if let Err(error) = reader.seek(SeekFrom::End(0)) {
                    let _ = send_ingest_diagnostic(
                        &tx,
                        "jsonl-follow",
                        None,
                        None,
                        Some(&path.display().to_string()),
                        "error",
                        format!("failed to seek {}: {error}", path.display()),
                        Some("seek"),
                        false,
                        false,
                        0,
                        0,
                    );
                    thread::sleep(poll);
                    continue;
                }

                loop {
                    let mut line = String::new();
                    match reader.read_line(&mut line) {
                        Ok(0) => thread::sleep(poll),
                        Ok(_) => {
                            line_number += 1;
                            let line = line.trim();
                            if line.is_empty() {
                                continue;
                            }
                            match serde_json::from_str::<EventEnvelope>(line) {
                                Ok(event) => {
                                    if !filter.matches(&event) {
                                        continue;
                                    }
                                    if tx.send(event).is_err() {
                                        return;
                                    }
                                }
                                Err(error) => {
                                    let _ = send_ingest_diagnostic(
                                        &tx,
                                        "jsonl-follow",
                                        None,
                                        None,
                                        Some(&path.display().to_string()),
                                        "error",
                                        format!(
                                            "failed to decode appended line {} from {}: {error}",
                                            line_number,
                                            path.display()
                                        ),
                                        Some("decode"),
                                        false,
                                        true,
                                        0,
                                        0,
                                    );
                                }
                            }
                        }
                        Err(error) => {
                            let _ = send_ingest_diagnostic(
                                &tx,
                                "jsonl-follow",
                                None,
                                None,
                                Some(&path.display().to_string()),
                                "error",
                                format!("read error from {}: {error}", path.display()),
                                Some("read"),
                                false,
                                false,
                                0,
                                0,
                            );
                            thread::sleep(poll);
                            break;
                        }
                    }
                }
            }
        })
        .context("failed to spawn event jsonl follow thread")?;

    Ok(())
}

fn spawn_nats_subject(
    url: String,
    subject: String,
    filter: EventFilter,
    tx: Sender<EventEnvelope>,
) -> Result<()> {
    let thread_name = format!("nats-subscribe-{subject}");
    thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = send_ingest_diagnostic(
                        &tx,
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
                                let _ = send_ingest_diagnostic(
                                    &tx,
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
                                    match serde_json::from_slice::<EventEnvelope>(
                                        message.payload.as_ref(),
                                    ) {
                                        Ok(mut event) => {
                                            stamp_ingested_event(&mut event, None, Some(&subject));
                                            if !filter.matches(&event) {
                                                let _ = send_ingest_diagnostic(
                                                    &tx,
                                                    "nats",
                                                    None,
                                                    None,
                                                    Some(&subject),
                                                    "info",
                                                    "event filtered by active TUI filter",
                                                    None,
                                                    false,
                                                    false,
                                                    1,
                                                    0,
                                                );
                                                continue;
                                            }
                                            if tx.send(event).is_err() {
                                                return;
                                            }
                                        }
                                        Err(error) => {
                                            let _ = send_ingest_diagnostic(
                                                &tx,
                                                "nats",
                                                None,
                                                None,
                                                Some(&subject),
                                                "error",
                                                format!("failed to decode {subject}: {error}"),
                                                Some("decode"),
                                                false,
                                                true,
                                                0,
                                                0,
                                            );
                                        }
                                    }
                                }
                            }
                            Err(error) => {
                                let _ = send_ingest_diagnostic(
                                    &tx,
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
                                );
                            }
                        },
                        Err(error) => {
                            let _ = send_ingest_diagnostic(
                                &tx,
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
                            );
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            });
        })
        .context("failed to spawn nats subscribe thread")?;

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

#[allow(clippy::too_many_arguments)]
fn send_ingest_diagnostic(
    tx: &Sender<EventEnvelope>,
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
) -> bool {
    let ts = trade_core::unix_ts_ns();
    let mut envelope = EventEnvelope::new(
        format!("ingest-{source}-{ts}"),
        source.to_string(),
        ts as u64,
        "trade-tui-ingest",
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
    tx.send(envelope).is_ok()
}
