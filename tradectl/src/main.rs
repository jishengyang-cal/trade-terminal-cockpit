use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use trade_core::{
    reduce_event, AppState, CommandEnvelope, CommandPayload, EventEnvelope, EventFilter,
};

#[derive(Debug, Parser)]
#[command(name = "tradectl")]
#[command(about = "Emit audited trading command envelopes")]
struct Cli {
    #[arg(long)]
    operator_id: Option<String>,

    #[arg(long)]
    session_id: Option<String>,

    #[arg(long)]
    reason: Option<String>,

    #[arg(long)]
    capability: Option<String>,

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
    CancelAllOrdersForAccount {
        account_id: String,
        #[arg(long)]
        confirm: Option<String>,
    },
    FlattenSymbol {
        account_id: String,
        symbol: String,
        #[arg(long)]
        confirm: Option<String>,
    },
    FlattenAccount {
        account_id: String,
        #[arg(long)]
        confirm: Option<String>,
    },
    GlobalKillSwitch {
        account_id: String,
        #[arg(long)]
        confirm: Option<String>,
    },
    AccountKillSwitch {
        account_id: String,
        #[arg(long)]
        confirm: Option<String>,
    },
    AckAlert {
        alert_id: String,
    },
    EvidenceBundle {
        #[arg(long, value_name = "PATH")]
        event_jsonl: PathBuf,
        #[arg(long, value_name = "PATH")]
        audit_jsonl: Option<PathBuf>,
        #[arg(long, value_name = "PATH")]
        output_json: Option<PathBuf>,
        #[arg(long)]
        account_id: Option<String>,
        #[arg(long)]
        correlation_id: Option<String>,
        #[arg(long)]
        order_id: Option<String>,
        #[arg(long)]
        broker_order_id: Option<String>,
        #[arg(long)]
        perm_id: Option<String>,
        #[arg(long)]
        command_id: Option<String>,
        #[arg(long)]
        strategy_id: Option<String>,
        #[arg(long)]
        symbol: Option<String>,
        #[arg(long)]
        stream: Option<String>,
        #[arg(long)]
        subject: Option<String>,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    if let Command::EvidenceBundle {
        event_jsonl,
        audit_jsonl,
        output_json,
        account_id,
        correlation_id,
        order_id,
        broker_order_id,
        perm_id,
        command_id,
        strategy_id,
        symbol,
        stream,
        subject,
    } = &cli.command
    {
        let filter = EventFilter {
            account_id: account_id.clone(),
            strategy_id: strategy_id.clone(),
            symbol: symbol.clone(),
            order_id: order_id.clone(),
            broker_order_id: broker_order_id.clone(),
            perm_id: perm_id.clone(),
            command_id: command_id.clone(),
            correlation_id: correlation_id.clone(),
            stream: stream.clone(),
            subject: subject.clone(),
            ..EventFilter::default()
        };
        let bundle = build_evidence_bundle(event_jsonl, audit_jsonl.as_deref(), filter)?;
        let json = if cli.pretty {
            serde_json::to_string_pretty(&bundle)?
        } else {
            serde_json::to_string(&bundle)?
        };
        if let Some(path) = output_json.as_deref() {
            fs::write(path, format!("{json}\n"))?;
        } else {
            println!("{json}");
        }
        return Ok(());
    }

    let command_id = cli.command_id.unwrap_or_else(|| new_id("cmd"));
    let correlation_id = cli.correlation_id.unwrap_or_else(|| command_id.clone());
    let operator_id = required_global(cli.operator_id, "--operator-id")?;
    let session_id = required_global(cli.session_id, "--session-id")?;
    let reason = required_global(cli.reason, "--reason")?;
    let capability = required_global(cli.capability, "--capability")?;
    let payload = payload_from_command(&cli.command)?;

    let envelope = CommandEnvelope::new(
        command_id,
        operator_id,
        session_id,
        correlation_id,
        reason,
        capability,
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

#[derive(Debug, Serialize)]
struct EvidenceBundle {
    schema_version: String,
    generated_ts_ns: i64,
    generated_by: String,
    git_commit: Option<String>,
    filters: EventFilter,
    input_files: Vec<EvidenceInput>,
    input_hashes: Vec<String>,
    event_id_count: usize,
    duplicate_event_count: usize,
    sequence_gap_count: u64,
    schema_versions: Vec<String>,
    event_count: usize,
    command_count: usize,
    projection: AppState,
    events: Vec<EventEnvelope>,
    commands: Vec<CommandEnvelope>,
}

#[derive(Debug, Serialize)]
struct EvidenceInput {
    path: String,
    sha256: String,
    bytes: u64,
}

fn build_evidence_bundle(
    event_jsonl: &Path,
    audit_jsonl: Option<&Path>,
    filter: EventFilter,
) -> Result<EvidenceBundle> {
    let mut input_files = vec![evidence_input(event_jsonl)?];
    if let Some(path) = audit_jsonl {
        input_files.push(evidence_input(path)?);
    }
    let input_hashes = input_files
        .iter()
        .map(|input| input.sha256.clone())
        .collect::<Vec<_>>();

    let events = read_event_jsonl(event_jsonl)?
        .into_iter()
        .filter(|event| filter.matches(event))
        .collect::<Vec<_>>();

    let mut projection = AppState::default();
    for event in events.iter().cloned() {
        reduce_event(&mut projection, event);
    }
    let event_ids = events
        .iter()
        .map(|event| event.event_id.clone())
        .collect::<BTreeSet<_>>();
    let schema_versions = events
        .iter()
        .map(|event| event.schema_version.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    let commands = if let Some(path) = audit_jsonl {
        read_command_jsonl(path)?
            .into_iter()
            .filter(|command| command_matches_filter(command, &filter))
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    Ok(EvidenceBundle {
        schema_version: "trading.evidence.v1".to_string(),
        generated_ts_ns: trade_core::unix_ts_ns(),
        generated_by: "tradectl evidence-bundle".to_string(),
        git_commit: git_commit(),
        filters: filter,
        input_files,
        input_hashes,
        event_id_count: event_ids.len(),
        duplicate_event_count: projection.connection.duplicate_events as usize,
        sequence_gap_count: projection.connection.sequence_gaps,
        schema_versions,
        event_count: events.len(),
        command_count: commands.len(),
        projection,
        events,
        commands,
    })
}

fn evidence_input(path: &Path) -> Result<EvidenceInput> {
    let bytes = fs::read(path)?;
    let digest = Sha256::digest(&bytes);
    Ok(EvidenceInput {
        path: path.display().to_string(),
        sha256: format!("{digest:x}"),
        bytes: bytes.len() as u64,
    })
}

fn read_event_jsonl(path: &Path) -> Result<Vec<EventEnvelope>> {
    let content = fs::read_to_string(path)?;
    let mut events = Vec::new();
    for (index, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let event = serde_json::from_str::<EventEnvelope>(line)
            .map_err(|error| anyhow::anyhow!("{} line {}: {error}", path.display(), index + 1))?;
        events.push(event);
    }
    Ok(events)
}

fn read_command_jsonl(path: &Path) -> Result<Vec<CommandEnvelope>> {
    let content = fs::read_to_string(path)?;
    let mut commands = Vec::new();
    for (index, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Ok(command) = serde_json::from_str::<CommandEnvelope>(line) {
            commands.push(command);
            continue;
        }
        if serde_json::from_str::<EventEnvelope>(line).is_ok() {
            continue;
        }
        bail!(
            "{} line {} is not a command envelope or domain event",
            path.display(),
            index + 1
        );
    }
    Ok(commands)
}

fn command_matches_filter(command: &CommandEnvelope, filter: &EventFilter) -> bool {
    if let Some(command_id) = filter.command_id.as_deref() {
        if command.command_id != command_id {
            return false;
        }
    }
    if let Some(correlation_id) = filter.correlation_id.as_deref() {
        if command.correlation_id != correlation_id {
            return false;
        }
    }
    if let Some(account_id) = filter.account_id.as_deref() {
        if !command.aggregate_id.contains(account_id) {
            return false;
        }
    }
    if let Some(order_id) = filter.order_id.as_deref() {
        if !command.aggregate_id.contains(order_id) {
            return false;
        }
    }
    if let Some(strategy_id) = filter.strategy_id.as_deref() {
        if command.aggregate_type == "strategy" && command.aggregate_id != strategy_id {
            return false;
        }
    }
    if let Some(symbol) = filter.symbol.as_deref() {
        if !command.aggregate_id.contains(symbol) {
            return false;
        }
    }
    true
}

fn git_commit() -> Option<String> {
    ProcessCommand::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn required_global(value: Option<String>, name: &str) -> Result<String> {
    value.ok_or_else(|| anyhow::anyhow!("{name} is required for command-envelope emission"))
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
        Command::CancelAllOrdersForAccount {
            account_id,
            confirm,
        } => {
            require_confirmation(
                confirm.as_deref(),
                &format!("CANCEL ALL ACCOUNT {account_id}"),
            )?;
            Ok(CommandPayload::CancelAllOrdersForAccountRequested {
                account_id: account_id.clone(),
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
        Command::FlattenAccount {
            account_id,
            confirm,
        } => {
            require_confirmation(confirm.as_deref(), &format!("FLATTEN ACCOUNT {account_id}"))?;
            Ok(CommandPayload::FlattenAccountRequested {
                account_id: account_id.clone(),
            })
        }
        Command::GlobalKillSwitch {
            account_id,
            confirm,
        } => {
            if !is_global_account_alias(account_id) {
                bail!(
                    "global-kill-switch requires account_id global/all/*; use account-kill-switch for {account_id}"
                );
            }
            require_confirmation(confirm.as_deref(), &format!("KILL {account_id}"))?;
            Ok(CommandPayload::GlobalKillSwitchRequested {
                account_id: account_id.clone(),
            })
        }
        Command::AccountKillSwitch {
            account_id,
            confirm,
        } => {
            require_confirmation(confirm.as_deref(), &format!("KILL ACCOUNT {account_id}"))?;
            Ok(CommandPayload::AccountKillSwitchRequested {
                account_id: account_id.clone(),
            })
        }
        Command::AckAlert { alert_id } => Ok(CommandPayload::AcknowledgeAlertRequested {
            alert_id: alert_id.clone(),
        }),
        Command::EvidenceBundle { .. } => unreachable!("handled before command envelope emission"),
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

fn is_global_account_alias(account_id: &str) -> bool {
    matches!(account_id, "global" | "all" | "*")
}
