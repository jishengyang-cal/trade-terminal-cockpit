use crate::cli::Cli;
use anyhow::{Context, Result};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::Duration;
use trade_core::{EventEnvelope, EventFilter};

pub fn load_events(cli: &Cli, filter: &EventFilter) -> Result<Vec<EventEnvelope>> {
    let events = if cli.mock || cli.event_jsonl.is_none() {
        trade_core::sample::sample_events()
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

pub fn spawn_follow(cli: &Cli, filter: EventFilter) -> Result<Option<Receiver<EventEnvelope>>> {
    if !cli.follow || cli.plain {
        return Ok(None);
    }

    let path = cli
        .event_jsonl
        .clone()
        .expect("clap requires --event-jsonl when --follow is used");
    let poll = Duration::from_millis(cli.follow_poll_ms.max(10));
    let (tx, rx) = mpsc::channel();

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

    Ok(Some(rx))
}
