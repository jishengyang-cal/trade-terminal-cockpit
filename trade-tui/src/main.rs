mod app;
mod cli;
mod event_stream;
mod input;
mod render;

use anyhow::Result;
use clap::Parser;
use cli::Cli;
use trade_core::{reduce_event, AppState};

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "trade_tui=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    let events = event_stream::load_events(&cli)?;
    let mut state = AppState::default();
    state.connection.nats = if cli.replay {
        "replay".to_string()
    } else if cli.event_jsonl.is_some() && !cli.mock {
        "jsonl".to_string()
    } else {
        "mock".to_string()
    };
    state.connection.render_fps = cli.fps;

    for event in events {
        reduce_event(&mut state, event);
    }

    let event_rx = event_stream::spawn_follow(&cli)?;

    if cli.plain {
        println!("{}", render::plain_summary(&state, cli.replay));
        return Ok(());
    }

    app::run(state, cli, event_rx)
}
