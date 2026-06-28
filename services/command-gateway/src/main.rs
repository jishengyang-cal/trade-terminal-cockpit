use anyhow::{bail, Context, Result};
use clap::Parser;
use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use trade_core::events::CommandAuditRecorded;
use trade_core::{CommandEnvelope, DangerLevel, DomainEvent, EventEnvelope};

#[derive(Debug, Parser)]
#[command(name = "command-gateway")]
#[command(about = "Validate and audit trading command envelopes without broker execution")]
struct Cli {
    #[arg(long, value_name = "PATH")]
    command_json: PathBuf,

    #[arg(long, value_name = "PATH")]
    audit_jsonl: PathBuf,

    #[arg(long)]
    allow_dangerous: bool,

    #[arg(long = "allow-capability", value_name = "CAPABILITY")]
    allow_capabilities: Vec<String>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let command = read_command(&cli.command_json)?;
    let (status, reason) = decide(&command, cli.allow_dangerous, &cli.allow_capabilities)?;
    let event = EventEnvelope::new(
        format!("audit-{}", command.command_id),
        command.correlation_id.clone(),
        0,
        "command-gateway",
        DomainEvent::CommandAuditRecorded(CommandAuditRecorded {
            command_id: command.command_id,
            operator_id: command.operator_id,
            command_type: command.command_type,
            status,
            reason,
        }),
    );

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&cli.audit_jsonl)
        .with_context(|| format!("failed to open {}", cli.audit_jsonl.display()))?;
    writeln!(file, "{}", serde_json::to_string(&event)?)?;
    Ok(())
}

fn read_command(path: &PathBuf) -> Result<CommandEnvelope> {
    let content =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let command = serde_json::from_str::<CommandEnvelope>(&content)
        .with_context(|| format!("failed to decode {}", path.display()))?;
    if command.operator_id.trim().is_empty()
        || command.session_id.trim().is_empty()
        || command.reason.trim().is_empty()
        || command.capability.trim().is_empty()
        || command.correlation_id.trim().is_empty()
    {
        bail!("command envelope missing operator/session/reason/capability/correlation fields");
    }
    Ok(command)
}

fn decide(
    command: &CommandEnvelope,
    allow_dangerous: bool,
    allow_capabilities: &[String],
) -> Result<(String, String)> {
    if let Some(reason) = capability_rejection_reason(command, allow_capabilities) {
        return Ok(("rejected".to_string(), reason));
    }

    match command.danger_level {
        DangerLevel::ReadOnly | DangerLevel::Controlled => Ok((
            "accepted".to_string(),
            "gateway accepted envelope".to_string(),
        )),
        DangerLevel::Dangerous if allow_dangerous => Ok((
            "accepted".to_string(),
            "dangerous envelope accepted by explicit gateway flag".to_string(),
        )),
        DangerLevel::Dangerous => Ok((
            "rejected".to_string(),
            "dangerous envelope rejected by default authority policy".to_string(),
        )),
    }
}

fn capability_rejection_reason(
    command: &CommandEnvelope,
    allow_capabilities: &[String],
) -> Option<String> {
    let expected = expected_capability(command.command_type.as_str())?;
    if command.capability != expected {
        return Some(format!(
            "capability mismatch: expected {expected}, got {}",
            command.capability
        ));
    }

    if !allow_capabilities.is_empty() {
        let allowed = allow_capabilities
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        if !allowed.contains(command.capability.as_str()) {
            return Some(format!(
                "capability {} is not in gateway allowlist",
                command.capability
            ));
        }
    }

    None
}

fn expected_capability(command_type: &str) -> Option<&'static str> {
    match command_type {
        "PauseStrategyRequested"
        | "ResumeStrategyRequested"
        | "DrainStrategyRequested"
        | "KillStrategyRequested" => Some("strategy.control"),
        "CancelOrderRequested" | "CancelAllOrdersForSymbolRequested" => Some("order.cancel"),
        "FlattenSymbolRequested" => Some("account.flatten"),
        "GlobalKillSwitchRequested" => Some("account.kill"),
        "AcknowledgeAlertRequested" => Some("alert.ack"),
        _ => None,
    }
}
