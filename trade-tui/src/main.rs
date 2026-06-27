mod app;
mod cli;
mod event_stream;
mod input;
mod render;
mod snapshot_client;

use anyhow::Result;
use clap::Parser;
use cli::Cli;
use trade_core::{apply_projection_snapshot, reduce_event, AppState};

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "trade_tui=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    let filter = cli.event_filter()?;
    let filter_summary = filter.summary();
    let events = event_stream::load_events(&cli, &filter)?;
    let mut state = AppState::default();
    if let Some(snapshot) = snapshot_client::load_snapshot(&cli)? {
        apply_projection_snapshot(&mut state, snapshot);
    }
    state.connection.nats = if cli.replay {
        "replay".to_string()
    } else if cli.snapshot_json.is_some() && cli.event_jsonl.is_none() && !cli.mock {
        "snapshot".to_string()
    } else if cli.event_jsonl.is_some() && !cli.mock {
        "jsonl".to_string()
    } else {
        "mock".to_string()
    };
    state.connection.render_fps = cli.fps;

    for event in events {
        reduce_event(&mut state, event);
    }

    let event_rx = event_stream::spawn_follow(&cli, filter)?;

    if cli.plain {
        println!(
            "{}",
            render::plain_summary(&state, cli.replay, filter_summary.as_deref())
        );
        return Ok(());
    }

    app::run(state, cli, filter_summary, event_rx)
}
