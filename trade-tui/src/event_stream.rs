use crate::cli::Cli;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Duration;
use trade_core::{EventEnvelope, EventFilter};

pub fn load_events(cli: &Cli, filter: &EventFilter) -> Result<Vec<EventEnvelope>> {
    let events = if cli.mock || (cli.event_jsonl.is_none() && cli.snapshot_json.is_none()) {
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
                    eprintln!("trade-tui jetstream: failed to create runtime: {error}");
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
                        eprintln!(
                            "trade-tui jetstream: {stream_name}/{durable_name} disconnected: {error}"
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
                filter_subject,
                ..Default::default()
            },
        )
        .await?;

    let mut messages = consumer.messages().await?;
    while let Some(message) = messages.next().await {
        let message = message?;
        match serde_json::from_slice::<EventEnvelope>(message.payload.as_ref()) {
            Ok(event) => {
                if filter.matches(&event) {
                    tx.send(event)
                        .map_err(|_| anyhow::anyhow!("event receiver closed"))?;
                }
                message
                    .ack()
                    .await
                    .map_err(|error| anyhow::anyhow!("jetstream ack failed: {error}"))?;
            }
            Err(error) => {
                eprintln!("trade-tui jetstream: failed to decode {stream_name}: {error}");
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
                        eprintln!("trade-tui follow: failed to open {}: {error}", path.display());
                        thread::sleep(poll);
                        continue;
                    }
                };
                let mut reader = BufReader::new(file);
                if let Err(error) = reader.seek(SeekFrom::End(0)) {
                    eprintln!("trade-tui follow: failed to seek {}: {error}", path.display());
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
                                Err(error) => eprintln!(
                                    "trade-tui follow: failed to decode appended line {} from {}: {error}",
                                    line_number,
                                    path.display()
                                ),
                            }
                        }
                        Err(error) => {
                            eprintln!("trade-tui follow: read error from {}: {error}", path.display());
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
                    eprintln!("trade-tui nats: failed to create runtime: {error}");
                    return;
                }
            };

            runtime.block_on(async move {
                loop {
                    match async_nats::connect(url.clone()).await {
                        Ok(client) => match client.subscribe(subject.clone()).await {
                            Ok(mut subscriber) => {
                                while let Some(message) = subscriber.next().await {
                                    match serde_json::from_slice::<EventEnvelope>(
                                        message.payload.as_ref(),
                                    ) {
                                        Ok(event) => {
                                            if !filter.matches(&event) {
                                                continue;
                                            }
                                            if tx.send(event).is_err() {
                                                return;
                                            }
                                        }
                                        Err(error) => eprintln!(
                                            "trade-tui nats: failed to decode {subject}: {error}"
                                        ),
                                    }
                                }
                            }
                            Err(error) => {
                                eprintln!("trade-tui nats: failed to subscribe {subject}: {error}")
                            }
                        },
                        Err(error) => eprintln!("trade-tui nats: failed to connect {url}: {error}"),
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            });
        })
        .context("failed to spawn nats subscribe thread")?;

    Ok(())
}
