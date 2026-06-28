use anyhow::{bail, Context, Result};
use clap::Parser;
use std::collections::BTreeSet;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use trade_core::events::CommandAuditRecorded;
use trade_core::{CommandEnvelope, CommandPayload, DangerLevel, DomainEvent, EventEnvelope};

#[derive(Debug, Parser)]
#[command(name = "command-gateway")]
#[command(about = "Validate, audit, and optionally dispatch trading command envelopes")]
struct Cli {
    #[arg(long, value_name = "PATH")]
    command_json: PathBuf,

    #[arg(long, value_name = "PATH")]
    audit_jsonl: PathBuf,

    #[arg(long)]
    allow_dangerous: bool,

    #[arg(long = "allow-capability", value_name = "CAPABILITY")]
    allow_capabilities: Vec<String>,

    #[arg(long)]
    execute_broker_control: bool,

    #[arg(long, value_name = "PATH")]
    broker_runtime_dir: Option<PathBuf>,

    #[arg(long, value_name = "PATH")]
    broker_control_bin: Option<PathBuf>,

    #[arg(long = "broker-account-slot", value_name = "ACCOUNT_ID=SLOT")]
    broker_account_slots: Vec<String>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let command = read_command(&cli.command_json)?;
    let (status, reason) = decide(&command, cli.allow_dangerous, &cli.allow_capabilities)?;
    let (status, reason) = if status == "accepted" && cli.execute_broker_control {
        dispatch_broker_control(&command, &cli)?
    } else {
        (status, reason)
    };
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BrokerControlRequest {
    scope: BrokerControlScope,
    family: &'static str,
    mode: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BrokerControlScope {
    Global,
    AccountSlot(u8),
}

fn dispatch_broker_control(command: &CommandEnvelope, cli: &Cli) -> Result<(String, String)> {
    let request = match broker_control_request(command, &cli.broker_account_slots) {
        Ok(request) => request,
        Err(reason) => return Ok(("unsupported_execution".to_string(), reason)),
    };

    let runtime_dir = match broker_runtime_dir(cli) {
        Ok(runtime_dir) => runtime_dir,
        Err(error) => return Ok(("execution_failed".to_string(), error.to_string())),
    };
    let broker_control_bin = broker_control_bin(cli);
    let generation = match runtime_control_generation(command) {
        Ok(generation) => generation,
        Err(error) => return Ok(("execution_failed".to_string(), error.to_string())),
    };
    let mut process = Command::new(&broker_control_bin);
    process
        .arg("--write-runtime-control-plan")
        .arg(&runtime_dir)
        .arg("--scope");
    match request.scope {
        BrokerControlScope::Global => {
            process.arg("global");
        }
        BrokerControlScope::AccountSlot(account_slot) => {
            process
                .arg("account_slot")
                .arg("--account-slot")
                .arg(account_slot.to_string());
        }
    }
    process
        .arg("--family")
        .arg(request.family)
        .arg("--mode")
        .arg(request.mode)
        .arg("--generation")
        .arg(generation.to_string())
        .arg("--request-id")
        .arg(&command.command_id);

    let output = match process.output() {
        Ok(output) => output,
        Err(error) => {
            return Ok((
                "execution_failed".to_string(),
                format!("failed to launch broker-control-gateway: {error}"),
            ));
        }
    };

    if output.status.success() {
        return Ok((
            "dispatched".to_string(),
            format!(
                "broker-control runtime plan dispatched: scope={} family={} mode={}",
                request.scope.as_arg(),
                request.family,
                request.mode
            ),
        ));
    }

    Ok((
        "execution_failed".to_string(),
        format!(
            "broker-control runtime plan failed: exit={} stderr={}",
            output
                .status
                .code()
                .map_or_else(|| "signal".to_string(), |code| code.to_string()),
            compact_process_text(&output.stderr)
        ),
    ))
}

fn broker_control_request(
    command: &CommandEnvelope,
    account_slots: &[String],
) -> std::result::Result<BrokerControlRequest, String> {
    match &command.payload {
        CommandPayload::GlobalKillSwitchRequested { account_id }
            if is_global_account_alias(account_id) =>
        {
            Ok(BrokerControlRequest {
                scope: BrokerControlScope::Global,
                family: "global_kill",
                mode: "assert",
            })
        }
        CommandPayload::GlobalKillSwitchRequested { account_id } => Err(format!(
            "broker-control adapter only supports global kill for account_id=global/all/*; refusing to broaden account-specific kill switch for {account_id}"
        )),
        CommandPayload::FlattenSymbolRequested { account_id, symbol }
            if is_all_symbol_wildcard(symbol) =>
        {
            let scope = broker_control_scope(account_id, account_slots)?;
            Ok(BrokerControlRequest {
                scope,
                family: "flatten_only",
                mode: "assert",
            })
        }
        CommandPayload::FlattenSymbolRequested { account_id, symbol } => Err(format!(
            "broker-control adapter cannot execute symbol-scoped flatten for {account_id}:{symbol}; use symbol=* with account_id=global/all/* or --broker-account-slot to request a supported broker runtime scope; no scope broadening was performed"
        )),
        CommandPayload::CancelAllOrdersForSymbolRequested { account_id, symbol }
            if is_all_symbol_wildcard(symbol) =>
        {
            let scope = broker_control_scope(account_id, account_slots)?;
            Ok(BrokerControlRequest {
                scope,
                family: "cancel_all",
                mode: "assert",
            })
        }
        CommandPayload::CancelAllOrdersForSymbolRequested { account_id, symbol } => Err(format!(
            "broker-control adapter cannot execute symbol-scoped cancel-all for {account_id}:{symbol}; use symbol=* with account_id=global/all/* or --broker-account-slot to request a supported broker runtime scope; no scope broadening was performed"
        )),
        CommandPayload::KillStrategyRequested { strategy_id } => Err(format!(
            "no strategy-control adapter configured for kill strategy {strategy_id}"
        )),
        CommandPayload::PauseStrategyRequested { strategy_id } => Err(format!(
            "no strategy-control adapter configured for pause strategy {strategy_id}"
        )),
        CommandPayload::ResumeStrategyRequested { strategy_id } => Err(format!(
            "no strategy-control adapter configured for resume strategy {strategy_id}"
        )),
        CommandPayload::DrainStrategyRequested { strategy_id } => Err(format!(
            "no strategy-control adapter configured for drain strategy {strategy_id}"
        )),
        CommandPayload::CancelOrderRequested {
            account_id,
            order_id,
        } => Err(format!(
            "no order-gateway adapter configured for cancel order {account_id}:{order_id}"
        )),
        CommandPayload::AcknowledgeAlertRequested { alert_id } => Err(format!(
            "no alert-service adapter configured for acknowledge alert {alert_id}"
        )),
    }
}

fn broker_control_scope(
    account_id: &str,
    account_slots: &[String],
) -> std::result::Result<BrokerControlScope, String> {
    if is_global_account_alias(account_id) {
        return Ok(BrokerControlScope::Global);
    }

    match account_slot_for(account_id, account_slots)? {
        Some(account_slot) => Ok(BrokerControlScope::AccountSlot(account_slot)),
        None => Err(format!(
            "broker-control adapter requires account_id=global/all/* or --broker-account-slot {account_id}=SLOT for account-scoped runtime control; no scope broadening was performed"
        )),
    }
}

fn account_slot_for(
    account_id: &str,
    account_slots: &[String],
) -> std::result::Result<Option<u8>, String> {
    for mapping in account_slots {
        let (mapped_account_id, slot) = mapping.split_once('=').ok_or_else(|| {
            format!("invalid --broker-account-slot {mapping}; expected ACCOUNT_ID=SLOT")
        })?;
        if mapped_account_id != account_id {
            continue;
        }
        let slot = slot
            .parse::<u8>()
            .map_err(|_| format!("invalid account slot in --broker-account-slot {mapping}"))?;
        return Ok(Some(slot));
    }

    Ok(None)
}

fn broker_runtime_dir(cli: &Cli) -> Result<PathBuf> {
    cli.broker_runtime_dir
        .clone()
        .or_else(|| env::var_os("BROKER_RUNTIME_DIR").map(PathBuf::from))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "--broker-runtime-dir or BROKER_RUNTIME_DIR is required with --execute-broker-control"
            )
        })
}

fn broker_control_bin(cli: &Cli) -> PathBuf {
    cli.broker_control_bin
        .clone()
        .or_else(|| env::var_os("BROKER_CONTROL_BIN").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("broker-control-gateway"))
}

fn runtime_control_generation(command: &CommandEnvelope) -> Result<u64> {
    u64::try_from(command.requested_ts_ns)
        .ok()
        .filter(|generation| *generation > 0)
        .ok_or_else(|| anyhow::anyhow!("command requested_ts_ns must be a positive u64 generation"))
}

fn is_global_account_alias(account_id: &str) -> bool {
    matches!(account_id, "global" | "all" | "*")
}

fn is_all_symbol_wildcard(symbol: &str) -> bool {
    symbol == "*"
}

fn compact_process_text(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let compact = sanitize_runtime_text(&text)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if compact.is_empty() {
        "<empty>".to_string()
    } else if compact.chars().count() > 240 {
        format!("{}...", compact.chars().take(240).collect::<String>())
    } else {
        compact
    }
}

fn sanitize_runtime_text(text: &str) -> String {
    match env::var("HOME") {
        Ok(home) if !home.is_empty() => text.replace(&home, "$HOME"),
        _ => text.to_string(),
    }
}

impl BrokerControlScope {
    fn as_arg(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::AccountSlot(_) => "account_slot",
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn command(payload: CommandPayload, capability: &str) -> CommandEnvelope {
        CommandEnvelope::new(
            "cmd-test",
            "operator-test",
            "session-test",
            "corr-test",
            "unit test",
            capability,
            payload,
        )
    }

    #[test]
    fn maps_global_kill_alias_to_broker_control() {
        let command = command(
            CommandPayload::GlobalKillSwitchRequested {
                account_id: "global".to_string(),
            },
            "account.kill",
        );

        let request = broker_control_request(&command, &[]).expect("request should map");

        assert_eq!(
            request,
            BrokerControlRequest {
                scope: BrokerControlScope::Global,
                family: "global_kill",
                mode: "assert",
            }
        );
    }

    #[test]
    fn rejects_account_specific_global_kill_without_broadening() {
        let command = command(
            CommandPayload::GlobalKillSwitchRequested {
                account_id: "paper-main".to_string(),
            },
            "account.kill",
        );

        let reason =
            broker_control_request(&command, &[]).expect_err("request must not broaden scope");

        assert!(reason.contains("refusing to broaden account-specific kill switch"));
    }

    #[test]
    fn rejects_symbol_flatten_without_broadening() {
        let command = command(
            CommandPayload::FlattenSymbolRequested {
                account_id: "paper-main".to_string(),
                symbol: "MU".to_string(),
            },
            "account.flatten",
        );

        let reason =
            broker_control_request(&command, &[]).expect_err("request must not broaden scope");

        assert!(reason.contains("cannot execute symbol-scoped flatten"));
        assert!(reason.contains("symbol=*"));
    }

    #[test]
    fn maps_global_wildcard_flatten_to_broker_control() {
        let command = command(
            CommandPayload::FlattenSymbolRequested {
                account_id: "global".to_string(),
                symbol: "*".to_string(),
            },
            "account.flatten",
        );

        let request = broker_control_request(&command, &[]).expect("request should map");

        assert_eq!(
            request,
            BrokerControlRequest {
                scope: BrokerControlScope::Global,
                family: "flatten_only",
                mode: "assert",
            }
        );
    }

    #[test]
    fn maps_account_wildcard_cancel_all_to_account_slot() {
        let command = command(
            CommandPayload::CancelAllOrdersForSymbolRequested {
                account_id: "paper-main".to_string(),
                symbol: "*".to_string(),
            },
            "order.cancel",
        );
        let account_slots = vec!["paper-main=7".to_string()];

        let request = broker_control_request(&command, &account_slots).expect("request should map");

        assert_eq!(
            request,
            BrokerControlRequest {
                scope: BrokerControlScope::AccountSlot(7),
                family: "cancel_all",
                mode: "assert",
            }
        );
    }

    #[test]
    fn dangerous_commands_still_require_gateway_flag() {
        let command = command(
            CommandPayload::GlobalKillSwitchRequested {
                account_id: "global".to_string(),
            },
            "account.kill",
        );

        let (status, reason) = decide(&command, false, &[]).expect("decision should succeed");

        assert_eq!(status, "rejected");
        assert!(reason.contains("dangerous envelope rejected"));
    }
}
