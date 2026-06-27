use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use trade_core::{CommandEnvelope, CommandPayload};

#[derive(Debug, Parser)]
#[command(name = "tradectl")]
#[command(about = "Emit audited trading command envelopes")]
struct Cli {
    #[arg(long)]
    operator_id: String,

    #[arg(long)]
    session_id: String,

    #[arg(long)]
    reason: String,

    #[arg(long)]
    capability: String,

    #[arg(long)]
    command_id: Option<String>,

    #[arg(long)]
    correlation_id: Option<String>,

    #[arg(long)]
    pretty: bool,

    #[arg(long)]
    audit_jsonl: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    PauseStrategy {
        strategy_id: String,
    },
    ResumeStrategy {
        strategy_id: String,
    },
    DrainStrategy {
        strategy_id: String,
    },
    KillStrategy {
        strategy_id: String,
        #[arg(long)]
        confirm: Option<String>,
    },
    CancelOrder {
        account_id: String,
        order_id: String,
    },
    CancelAllOrdersForSymbol {
        account_id: String,
        symbol: String,
        #[arg(long)]
        confirm: Option<String>,
    },
    FlattenSymbol {
        account_id: String,
        symbol: String,
        #[arg(long)]
        confirm: Option<String>,
    },
    GlobalKillSwitch {
        account_id: String,
        #[arg(long)]
        confirm: Option<String>,
    },
    AckAlert {
        alert_id: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let command_id = cli.command_id.unwrap_or_else(|| new_id("cmd"));
    let correlation_id = cli.correlation_id.unwrap_or_else(|| command_id.clone());
    let payload = payload_from_command(&cli.command)?;

    let envelope = CommandEnvelope::new(
        command_id,
        cli.operator_id,
        cli.session_id,
        correlation_id,
        cli.reason,
        cli.capability,
        payload,
    );

    let compact_json = serde_json::to_string(&envelope)?;
    if let Some(path) = cli.audit_jsonl.as_deref() {
        append_audit_jsonl(path, &compact_json)?;
    }

    if cli.pretty {
        println!("{}", serde_json::to_string_pretty(&envelope)?);
    } else {
        println!("{compact_json}");
    }

    Ok(())
}

fn append_audit_jsonl(path: &Path, compact_json: &str) -> Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{compact_json}")?;
    Ok(())
}

fn payload_from_command(command: &Command) -> Result<CommandPayload> {
    match command {
        Command::PauseStrategy { strategy_id } => Ok(CommandPayload::PauseStrategyRequested {
            strategy_id: strategy_id.clone(),
        }),
        Command::ResumeStrategy { strategy_id } => Ok(CommandPayload::ResumeStrategyRequested {
            strategy_id: strategy_id.clone(),
        }),
        Command::DrainStrategy { strategy_id } => Ok(CommandPayload::DrainStrategyRequested {
            strategy_id: strategy_id.clone(),
        }),
        Command::KillStrategy {
            strategy_id,
            confirm,
        } => {
            require_confirmation(confirm.as_deref(), &format!("KILL STRATEGY {strategy_id}"))?;
            Ok(CommandPayload::KillStrategyRequested {
                strategy_id: strategy_id.clone(),
            })
        }
        Command::CancelOrder {
            account_id,
            order_id,
        } => Ok(CommandPayload::CancelOrderRequested {
            account_id: account_id.clone(),
            order_id: order_id.clone(),
        }),
        Command::CancelAllOrdersForSymbol {
            account_id,
            symbol,
            confirm,
        } => {
            require_confirmation(
                confirm.as_deref(),
                &format!("CANCEL ALL {account_id} {symbol}"),
            )?;
            Ok(CommandPayload::CancelAllOrdersForSymbolRequested {
                account_id: account_id.clone(),
                symbol: symbol.clone(),
            })
        }
        Command::FlattenSymbol {
            account_id,
            symbol,
            confirm,
        } => {
            require_confirmation(
                confirm.as_deref(),
                &format!("FLATTEN {account_id} {symbol}"),
            )?;
            Ok(CommandPayload::FlattenSymbolRequested {
                account_id: account_id.clone(),
                symbol: symbol.clone(),
            })
        }
        Command::GlobalKillSwitch {
            account_id,
            confirm,
        } => {
            require_confirmation(confirm.as_deref(), &format!("KILL {account_id}"))?;
            Ok(CommandPayload::GlobalKillSwitchRequested {
                account_id: account_id.clone(),
            })
        }
        Command::AckAlert { alert_id } => Ok(CommandPayload::AcknowledgeAlertRequested {
            alert_id: alert_id.clone(),
        }),
    }
}

fn require_confirmation(actual: Option<&str>, expected: &str) -> Result<()> {
    if actual == Some(expected) {
        return Ok(());
    }

    bail!("dangerous command requires --confirm '{}'", expected)
}

fn new_id(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        trade_core::unix_ts_ns(),
        std::process::id()
    )
}
