use crate::cli::Cli;
use anyhow::{Context, Result};
use serde::Deserialize;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::Command;
use trade_core::{CommandEnvelope, CommandPayload, EventEnvelope};

#[derive(Clone, Debug)]
pub struct CommandClient {
    config: CommandClientConfig,
}

#[derive(Clone, Debug)]
pub struct CommandClientConfig {
    pub operator_id: String,
    pub session_id: String,
    pub reason: String,
    pub gateway_bin: PathBuf,
    pub gateway_addr: Option<String>,
    pub audit_jsonl: PathBuf,
    pub allow_dangerous: bool,
    pub execute_broker_control: bool,
    pub broker_runtime_dir: Option<PathBuf>,
    pub broker_control_bin: Option<PathBuf>,
    pub broker_account_slots: Vec<String>,
    pub risk_check_bin: Option<PathBuf>,
    pub strategy_control_bin: Option<PathBuf>,
    pub order_gateway_bin: Option<PathBuf>,
    pub alert_service_bin: Option<PathBuf>,
    pub target_environment: String,
}

#[derive(Clone, Debug)]
pub struct CommandSubmission {
    pub command_id: String,
    pub status: String,
    pub events: Vec<EventEnvelope>,
}

impl CommandClient {
    pub fn from_cli(cli: &Cli) -> Self {
        Self {
            config: CommandClientConfig {
                operator_id: cli
                    .operator_id
                    .clone()
                    .or_else(|| std::env::var("USER").ok())
                    .unwrap_or_else(|| "operator-local".to_string()),
                session_id: cli.session_id.clone().unwrap_or_else(default_session_id),
                reason: cli.command_reason.clone(),
                gateway_bin: cli
                    .command_gateway_bin
                    .clone()
                    .or_else(|| std::env::var_os("COMMAND_GATEWAY_BIN").map(PathBuf::from))
                    .unwrap_or_else(default_gateway_bin),
                gateway_addr: cli
                    .command_gateway_addr
                    .clone()
                    .or_else(|| std::env::var("COMMAND_GATEWAY_ADDR").ok()),
                audit_jsonl: cli.command_gateway_audit_jsonl.clone(),
                allow_dangerous: cli.command_gateway_allow_dangerous,
                execute_broker_control: cli.command_gateway_execute_broker_control,
                broker_runtime_dir: cli.broker_runtime_dir.clone(),
                broker_control_bin: cli.broker_control_bin.clone(),
                broker_account_slots: cli.broker_account_slots.clone(),
                risk_check_bin: cli.risk_check_bin.clone(),
                strategy_control_bin: cli.strategy_control_bin.clone(),
                order_gateway_bin: cli.order_gateway_bin.clone(),
                alert_service_bin: cli.alert_service_bin.clone(),
                target_environment: cli.target_environment.clone(),
            },
        }
    }

    pub fn config(&self) -> &CommandClientConfig {
        &self.config
    }

    pub fn submit(
        &self,
        payload: CommandPayload,
        capability: &str,
        reason: &str,
        confirmation_text: &str,
    ) -> Result<CommandSubmission> {
        let command_id = new_id("tui-cmd");
        let mut envelope = CommandEnvelope::new(
            command_id.clone(),
            self.config.operator_id.clone(),
            self.config.session_id.clone(),
            command_id.clone(),
            reason.to_string(),
            capability.to_string(),
            payload,
        );
        envelope.source = "trade-tui".to_string();
        envelope.confirmation_text = Some(confirmation_text.to_string());
        envelope.target_environment = self.config.target_environment.clone();
        envelope.authority_policy_version = "command-gateway.local".to_string();

        let command_json = command_json_path(&command_id)?;
        write_command_json(&command_json, &envelope)?;

        if let Some(addr) = self.config.gateway_addr.as_deref() {
            return self.submit_tcp(addr, &envelope);
        }

        let mut process = Command::new(&self.config.gateway_bin);
        process
            .arg("--command-json")
            .arg(&command_json)
            .arg("--audit-jsonl")
            .arg(&self.config.audit_jsonl);
        if self.config.allow_dangerous {
            process.arg("--allow-dangerous");
        }
        if self.config.execute_broker_control {
            process.arg("--execute-broker-control");
        }
        if let Some(path) = &self.config.broker_runtime_dir {
            process.arg("--broker-runtime-dir").arg(path);
        }
        if let Some(path) = &self.config.broker_control_bin {
            process.arg("--broker-control-bin").arg(path);
        }
        for mapping in &self.config.broker_account_slots {
            process.arg("--broker-account-slot").arg(mapping);
        }
        if let Some(path) = &self.config.risk_check_bin {
            process.arg("--risk-check-bin").arg(path);
        }
        if let Some(path) = &self.config.strategy_control_bin {
            process.arg("--strategy-control-bin").arg(path);
        }
        if let Some(path) = &self.config.order_gateway_bin {
            process.arg("--order-gateway-bin").arg(path);
        }
        if let Some(path) = &self.config.alert_service_bin {
            process.arg("--alert-service-bin").arg(path);
        }

        let output = process.output().with_context(|| {
            format!(
                "failed to launch command-gateway at {}",
                self.config.gateway_bin.display()
            )
        })?;
        if !output.status.success() {
            anyhow::bail!(
                "command-gateway exited with {}: {}",
                output
                    .status
                    .code()
                    .map_or_else(|| "signal".to_string(), |code| code.to_string()),
                compact_process_text(&output.stderr)
            );
        }

        let events = read_command_events(&self.config.audit_jsonl, &command_id)?;
        let status = events
            .iter()
            .rev()
            .find_map(command_event_status)
            .unwrap_or_else(|| "submitted".to_string());
        Ok(CommandSubmission {
            command_id,
            status,
            events,
        })
    }

    fn submit_tcp(&self, addr: &str, envelope: &CommandEnvelope) -> Result<CommandSubmission> {
        let mut stream = TcpStream::connect(addr)
            .with_context(|| format!("failed to connect command-gateway at {addr}"))?;
        writeln!(stream, "{}", serde_json::to_string(envelope)?)?;
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader
            .read_line(&mut line)
            .with_context(|| format!("failed to read command-gateway response from {addr}"))?;
        let response = serde_json::from_str::<GatewayResponse>(line.trim())
            .with_context(|| format!("failed to decode command-gateway response from {addr}"))?;
        if response.status == "error" {
            anyhow::bail!(
                "command-gateway rejected request: {}",
                response
                    .error
                    .unwrap_or_else(|| "unknown error".to_string())
            );
        }
        Ok(CommandSubmission {
            command_id: response.command_id,
            status: response.status,
            events: response.events,
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
struct GatewayResponse {
    command_id: String,
    status: String,
    events: Vec<EventEnvelope>,
    #[serde(default)]
    error: Option<String>,
}

fn default_gateway_bin() -> PathBuf {
    for path in [".run/bin/command-gateway", "target/debug/command-gateway"] {
        let path = PathBuf::from(path);
        if path.is_file() {
            return path;
        }
    }
    PathBuf::from("command-gateway")
}

fn default_session_id() -> String {
    std::env::var("ZELLIJ_SESSION_NAME")
        .ok()
        .or_else(|| std::env::var("TMUX").ok())
        .unwrap_or_else(|| format!("trade-tui-{}", std::process::id()))
}

fn command_json_path(command_id: &str) -> Result<PathBuf> {
    let dir = PathBuf::from(".run/tui-commands");
    fs::create_dir_all(&dir).context("failed to create .run/tui-commands")?;
    Ok(dir.join(format!("{command_id}.json")))
}

fn write_command_json(path: &PathBuf, envelope: &CommandEnvelope) -> Result<()> {
    let json = serde_json::to_string(envelope)?;
    fs::write(path, format!("{json}\n"))
        .with_context(|| format!("failed to write {}", path.display()))
}

fn read_command_events(path: &PathBuf, command_id: &str) -> Result<Vec<EventEnvelope>> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read command audit {}", path.display()))?;
    let mut events = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(event) = serde_json::from_str::<EventEnvelope>(line) else {
            continue;
        };
        if event.aggregate_type == "command" && event.aggregate_id == command_id {
            events.push(event);
            continue;
        }
        if command_event_id(&event).as_deref() == Some(command_id) {
            events.push(event);
        }
    }
    Ok(events)
}

fn command_event_id(event: &EventEnvelope) -> Option<String> {
    match &event.payload {
        trade_core::DomainEvent::CommandAuthorityDecided(event) => Some(event.command_id.clone()),
        trade_core::DomainEvent::CommandAuditRecorded(event) => Some(event.command_id.clone()),
        _ => None,
    }
}

fn command_event_status(event: &EventEnvelope) -> Option<String> {
    match &event.payload {
        trade_core::DomainEvent::CommandAuthorityDecided(event) => Some(event.status.clone()),
        trade_core::DomainEvent::CommandAuditRecorded(event) => Some(event.status.clone()),
        _ => None,
    }
}

fn new_id(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        trade_core::unix_ts_ns(),
        std::process::id()
    )
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
    match std::env::var("HOME") {
        Ok(home) if !home.is_empty() => text.replace(&home, "$HOME"),
        _ => text.to_string(),
    }
}
