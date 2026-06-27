use clap::Parser;
use std::path::PathBuf;

#[derive(Clone, Debug, Parser)]
#[command(name = "trade-tui")]
#[command(about = "Read-only trading domain terminal cockpit")]
pub struct Cli {
    #[arg(long, value_name = "PATH")]
    pub event_jsonl: Option<PathBuf>,

    #[arg(long)]
    pub mock: bool,

    #[arg(long)]
    pub plain: bool,

    #[arg(long, requires = "event_jsonl")]
    pub follow: bool,

    #[arg(long, default_value_t = 250)]
    pub follow_poll_ms: u64,

    #[arg(long)]
    pub replay: bool,

    #[arg(long)]
    pub from: Option<String>,

    #[arg(long)]
    pub to: Option<String>,

    #[arg(long, default_value_t = 20)]
    pub fps: u16,
}
