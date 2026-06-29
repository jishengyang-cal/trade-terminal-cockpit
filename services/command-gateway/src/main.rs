use anyhow::{bail, Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use trade_core::events::{CommandAuditRecorded, CommandAuthorityDecided};
use trade_core::{CommandEnvelope, CommandPayload, DangerLevel, DomainEvent, EventEnvelope};

#[derive(Clone, Debug, Parser)]
#[command(name = "command-gateway")]
#[command(about = "Validate, audit, and optionally dispatch trading command envelopes")]
struct Cli {
    #[arg(long, value_name = "PATH", required_unless_present = "serve")]
    command_json: Option<PathBuf>,

    #[arg(
        long,
        value_name = "PATH",
        default_value = ".run/command-gateway-audit.jsonl"
    )]
    audit_jsonl: PathBuf,

    #[arg(long, value_name = "ADDR")]
    serve: Option<String>,

    #[arg(long)]
    allow_dangerous: bool,

    #[arg(long = "allow-capability", value_name = "CAPABILITY")]
    allow_capabilities: Vec<String>,

    #[arg(long, value_name = "PATH")]
    policy_json: Option<PathBuf>,

    #[arg(long)]
    execute_broker_control: bool,

    #[arg(long, value_name = "PATH")]
    broker_runtime_dir: Option<PathBuf>,

    #[arg(long, value_name = "PATH")]
    broker_control_bin: Option<PathBuf>,

    #[arg(long = "broker-account-slot", value_name = "ACCOUNT_ID=SLOT")]
    broker_account_slots: Vec<String>,

    #[arg(long, value_name = "PATH")]
    risk_check_bin: Option<PathBuf>,

    #[arg(long, value_name = "PATH")]
    strategy_control_bin: Option<PathBuf>,

    #[arg(long, value_name = "PATH")]
    order_gateway_bin: Option<PathBuf>,

    #[arg(long, value_name = "PATH")]
    alert_service_bin: Option<PathBuf>,

    #[arg(long, default_value_t = 750)]
    adapter_timeout_ms: u64,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    if let Some(addr) = cli.serve.as_deref() {
        return serve_gateway(addr, &cli);
    }
    let command_json = cli
        .command_json
        .as_ref()
        .expect("clap requires --command-json unless --serve is set");
    let command = read_command(command_json)?;
    process_command(command, &cli)?;
    Ok(())
}

impl Cli {
    fn adapter_timeout(&self) -> Duration {
        Duration::from_millis(self.adapter_timeout_ms.max(1))
    }
}

fn process_command(command: CommandEnvelope, cli: &Cli) -> Result<GatewayResponse> {
    validate_command(&command)?;
    let policy = load_policy(cli)?;
    let decision = decide(
        &command,
        cli.allow_dangerous,
        &cli.allow_capabilities,
        policy.as_ref(),
    )?;
    let decision = if decision.status == "accepted" {
        apply_external_risk_check(&command, decision, cli)?
    } else {
        decision
    };
    let mut authority_event = EventEnvelope::new(
        format!("authority-{}", command.command_id),
        command.correlation_id.clone(),
        0,
        "command-gateway",
        DomainEvent::CommandAuthorityDecided(CommandAuthorityDecided {
            decision_id: format!("decision-{}", command.command_id),
            command_id: command.command_id.clone(),
            status: decision.status.clone(),
            reason_codes: decision.reason_codes.clone(),
            matched_policy_ids: decision.matched_policy_ids.clone(),
            operator_id: command.operator_id.clone(),
            command_type: command.command_type.clone(),
            capability: command.capability.clone(),
            scope: command.aggregate_id.clone(),
            approved_by: decision.approved_by.clone(),
            decided_ts_ns: trade_core::unix_ts_ns(),
            authority_policy_version: policy
                .as_ref()
                .map(|policy| policy.policy_version.clone())
                .unwrap_or_else(|| command.authority_policy_version.clone()),
            target_environment: command.target_environment.clone(),
        }),
    );
    stamp_command_event(&mut authority_event, &command, "authority");
    let (status, reason) = if decision.status == "accepted" {
        dispatch_command(&command, cli, &decision.reason)?
    } else {
        (decision.status, decision.reason)
    };
    let mut audit_event = EventEnvelope::new(
        format!("audit-{}", command.command_id),
        command.correlation_id.clone(),
        0,
        "command-gateway",
        DomainEvent::CommandAuditRecorded(CommandAuditRecorded {
            command_id: command.command_id.clone(),
            operator_id: command.operator_id.clone(),
            command_type: command.command_type.clone(),
            status,
            reason,
            target: Some(command.aggregate_id.clone()),
        }),
    );
    stamp_command_event(&mut audit_event, &command, "audit");

    append_audit_events(cli, [&authority_event, &audit_event])?;
    Ok(GatewayResponse {
        command_id: command.command_id.clone(),
        status: command_event_status(&audit_event),
        events: vec![authority_event, audit_event],
        error: None,
    })
}

fn stamp_command_event(envelope: &mut EventEnvelope, command: &CommandEnvelope, stage: &str) {
    envelope.partition_key = command.aggregate_id.clone();
    envelope.environment = command.target_environment.clone();
    envelope.subject = format!(
        "trading.command.{stage}.{}",
        command.command_type.to_ascii_lowercase()
    );
    envelope.trace_id = Some(command.correlation_id.clone());
    envelope.span_id = Some(format!("command-gateway.{stage}.{}", command.command_id));
    if !command.command_hash.is_empty() {
        envelope.checksum = Some(command.command_hash.clone());
    }
}

fn append_audit_events<'a>(
    cli: &Cli,
    events: impl IntoIterator<Item = &'a EventEnvelope>,
) -> Result<()> {
    if let Some(parent) = cli
        .audit_jsonl
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&cli.audit_jsonl)
        .with_context(|| format!("failed to open {}", cli.audit_jsonl.display()))?;
    for event in events {
        writeln!(file, "{}", serde_json::to_string(event)?)?;
    }
    Ok(())
}

fn serve_gateway(addr: &str, cli: &Cli) -> Result<()> {
    let listener = TcpListener::bind(addr)
        .with_context(|| format!("failed to bind command-gateway on {addr}"))?;
    eprintln!("command-gateway listening on {addr}");
    for stream in listener.incoming() {
        let stream = stream.context("failed to accept command-gateway client")?;
        let cli = cli.clone();
        thread::Builder::new()
            .name("command-gateway-client".to_string())
            .spawn(move || {
                if let Err(error) = handle_gateway_client(stream, &cli) {
                    eprintln!("command-gateway client error: {error}");
                }
            })
            .context("failed to spawn command-gateway client thread")?;
    }
    Ok(())
}

fn handle_gateway_client(mut stream: TcpStream, cli: &Cli) -> Result<()> {
    let peer = stream.peer_addr().ok();
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    while reader.read_line(&mut line)? != 0 {
        let response = match serde_json::from_str::<CommandEnvelope>(line.trim()) {
            Ok(command) => match process_command(command, cli) {
                Ok(response) => response,
                Err(error) => GatewayResponse::error(error.to_string()),
            },
            Err(error) => GatewayResponse::error(format!("invalid command envelope: {error}")),
        };
        writeln!(stream, "{}", serde_json::to_string(&response)?)?;
        line.clear();
    }
    if let Some(peer) = peer {
        eprintln!("command-gateway client disconnected: {peer}");
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GatewayResponse {
    command_id: String,
    status: String,
    events: Vec<EventEnvelope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl GatewayResponse {
    fn error(error: String) -> Self {
        Self {
            command_id: String::new(),
            status: "error".to_string(),
            events: Vec::new(),
            error: Some(error),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AuthorityDecision {
    status: String,
    reason: String,
    reason_codes: Vec<String>,
    matched_policy_ids: Vec<String>,
    approved_by: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct AuthorityPolicy {
    #[serde(default = "default_policy_version")]
    policy_version: String,
    #[serde(default)]
    allow_capabilities: Vec<String>,
    #[serde(default)]
    command_capabilities: BTreeMap<String, String>,
    #[serde(default)]
    sessions: Vec<OperatorSessionPolicy>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct OperatorSessionPolicy {
    operator_id: String,
    session_id: String,
    #[serde(default)]
    roles: Vec<String>,
    #[serde(default)]
    capabilities: Vec<String>,
    #[serde(default)]
    target_environments: Vec<String>,
    #[serde(default)]
    allow_dangerous: bool,
    #[serde(default)]
    mfa_verified: bool,
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

#[derive(Clone, Debug, Deserialize)]
struct ExternalAdapterResponse {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    reason_codes: Vec<String>,
    #[serde(default)]
    matched_policy_ids: Vec<String>,
    #[serde(default)]
    approved_by: Vec<String>,
}

fn dispatch_command(
    command: &CommandEnvelope,
    cli: &Cli,
    accepted_reason: &str,
) -> Result<(String, String)> {
    if let Some(path) = adapter_for_command(command, cli) {
        return run_external_dispatch_adapter(path, command, cli.adapter_timeout());
    }
    if cli.execute_broker_control {
        return dispatch_broker_control(command, cli);
    }
    Ok(("accepted".to_string(), accepted_reason.to_string()))
}

fn adapter_for_command<'a>(command: &CommandEnvelope, cli: &'a Cli) -> Option<&'a PathBuf> {
    match &command.payload {
        CommandPayload::PauseStrategyRequested { .. }
        | CommandPayload::ResumeStrategyRequested { .. }
        | CommandPayload::DrainStrategyRequested { .. }
        | CommandPayload::KillStrategyRequested { .. } => cli.strategy_control_bin.as_ref(),
        CommandPayload::CancelOrderRequested { .. }
        | CommandPayload::CancelAllOrdersForSymbolRequested { .. }
        | CommandPayload::CancelAllOrdersForAccountRequested { .. } => {
            cli.order_gateway_bin.as_ref()
        }
        CommandPayload::AcknowledgeAlertRequested { .. } => cli.alert_service_bin.as_ref(),
        _ => None,
    }
}

fn apply_external_risk_check(
    command: &CommandEnvelope,
    decision: AuthorityDecision,
    cli: &Cli,
) -> Result<AuthorityDecision> {
    let Some(path) = cli.risk_check_bin.as_ref() else {
        return Ok(decision);
    };
    let output = run_json_adapter(
        path,
        command,
        &["--check-command-risk"],
        cli.adapter_timeout(),
    )?;
    let Some(response) = output else {
        return Ok(decision);
    };
    let status = response.status.unwrap_or_else(|| "accepted".to_string());
    if status == "accepted" {
        let mut merged = decision;
        if let Some(reason) = response.reason {
            merged.reason = reason;
        }
        merged.reason_codes.extend(response.reason_codes);
        merged
            .matched_policy_ids
            .extend(response.matched_policy_ids);
        merged.approved_by.extend(response.approved_by);
        return Ok(merged);
    }

    Ok(AuthorityDecision {
        status,
        reason: response
            .reason
            .unwrap_or_else(|| "external risk check rejected command".to_string()),
        reason_codes: if response.reason_codes.is_empty() {
            vec!["external_risk_rejected".to_string()]
        } else {
            response.reason_codes
        },
        matched_policy_ids: if response.matched_policy_ids.is_empty() {
            vec!["external.risk".to_string()]
        } else {
            response.matched_policy_ids
        },
        approved_by: response.approved_by,
    })
}

fn run_external_dispatch_adapter(
    path: &PathBuf,
    command: &CommandEnvelope,
    timeout: Duration,
) -> Result<(String, String)> {
    let output = run_json_adapter(path, command, &["--execute-command"], timeout)?;
    let Some(response) = output else {
        return Ok((
            "dispatched".to_string(),
            format!("external adapter dispatched {}", command.command_type),
        ));
    };
    Ok((
        response.status.unwrap_or_else(|| "dispatched".to_string()),
        response
            .reason
            .unwrap_or_else(|| format!("external adapter dispatched {}", command.command_type)),
    ))
}

fn run_json_adapter(
    path: &PathBuf,
    command: &CommandEnvelope,
    args: &[&str],
    timeout: Duration,
) -> Result<Option<ExternalAdapterResponse>> {
    let mut process = Command::new(path)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to launch external adapter {}", path.display()))?;
    {
        let mut stdin = process
            .stdin
            .take()
            .context("external adapter stdin unavailable")?;
        writeln!(stdin, "{}", serde_json::to_string(command)?)?;
    }
    let output = wait_with_timeout(
        process,
        timeout,
        &format!("external adapter {}", path.display()),
    )?;
    if !output.status.success() {
        anyhow::bail!(
            "external adapter {} failed: exit={} stderr={}",
            path.display(),
            output
                .status
                .code()
                .map_or_else(|| "signal".to_string(), |code| code.to_string()),
            compact_process_text(&output.stderr)
        );
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stdout = stdout.trim();
    if stdout.is_empty() {
        return Ok(None);
    }
    let response = serde_json::from_str::<ExternalAdapterResponse>(stdout).with_context(|| {
        format!(
            "external adapter {} returned invalid JSON response",
            path.display()
        )
    })?;
    Ok(Some(response))
}

fn wait_with_timeout(mut process: Child, timeout: Duration, label: &str) -> Result<Output> {
    let start = Instant::now();
    loop {
        if process
            .try_wait()
            .with_context(|| format!("failed to poll {label}"))?
            .is_some()
        {
            return process
                .wait_with_output()
                .with_context(|| format!("failed to collect {label} output"));
        }
        if start.elapsed() >= timeout {
            let _ = process.kill();
            let output = process
                .wait_with_output()
                .with_context(|| format!("failed to collect timed out {label} output"))?;
            anyhow::bail!(
                "{label} timed out after {}ms stderr={}",
                timeout.as_millis(),
                compact_process_text(&output.stderr)
            );
        }
        thread::sleep(Duration::from_millis(1));
    }
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
        .arg(&command.command_id)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let output = match process.spawn() {
        Ok(process) => wait_with_timeout(process, cli.adapter_timeout(), "broker-control-gateway"),
        Err(error) => Err(error).context("failed to launch broker-control-gateway"),
    };
    let output = match output {
        Ok(output) => output,
        Err(error) => {
            return Ok((
                "execution_failed".to_string(),
                format!("broker-control runtime plan failed: {error}"),
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
        CommandPayload::FlattenAccountRequested { account_id } => {
            let scope = broker_control_account_slot_scope(account_id, account_slots)?;
            Ok(BrokerControlRequest {
                scope,
                family: "flatten_only",
                mode: "assert",
            })
        }
        CommandPayload::CancelAllOrdersForAccountRequested { account_id } => {
            let scope = broker_control_account_slot_scope(account_id, account_slots)?;
            Ok(BrokerControlRequest {
                scope,
                family: "cancel_all",
                mode: "assert",
            })
        }
        CommandPayload::AccountKillSwitchRequested { account_id } => {
            let scope = broker_control_account_slot_scope(account_id, account_slots)?;
            Ok(BrokerControlRequest {
                scope,
                family: "cancel_all",
                mode: "assert",
            })
        }
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

fn broker_control_account_slot_scope(
    account_id: &str,
    account_slots: &[String],
) -> std::result::Result<BrokerControlScope, String> {
    if is_global_account_alias(account_id) {
        return Err(
            "account-scoped runtime control requires a concrete account_id; use the global command for account_id=global/all/*".to_string(),
        );
    }

    match account_slot_for(account_id, account_slots)? {
        Some(account_slot) => Ok(BrokerControlScope::AccountSlot(account_slot)),
        None => Err(format!(
            "account-scoped runtime control requires --broker-account-slot {account_id}=SLOT; no global scope fallback was performed"
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
    validate_command(&command)?;
    Ok(command)
}

fn validate_command(command: &CommandEnvelope) -> Result<()> {
    if command.operator_id.trim().is_empty()
        || command.session_id.trim().is_empty()
        || command.reason.trim().is_empty()
        || command.capability.trim().is_empty()
        || command.correlation_id.trim().is_empty()
    {
        bail!("command envelope missing operator/session/reason/capability/correlation fields");
    }
    if let Some(expires_at_ns) = command.expires_at_ns {
        if expires_at_ns <= trade_core::unix_ts_ns() {
            bail!("command envelope expired before reaching command-gateway");
        }
    }
    Ok(())
}

fn load_policy(cli: &Cli) -> Result<Option<AuthorityPolicy>> {
    let Some(path) = cli.policy_json.as_ref() else {
        return Ok(None);
    };
    let content =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let policy = serde_json::from_str::<AuthorityPolicy>(&content)
        .with_context(|| format!("failed to decode {}", path.display()))?;
    Ok(Some(policy))
}

fn default_policy_version() -> String {
    "command-gateway.policy.v1".to_string()
}

fn command_event_status(event: &EventEnvelope) -> String {
    match &event.payload {
        DomainEvent::CommandAuditRecorded(event) => event.status.clone(),
        DomainEvent::CommandAuthorityDecided(event) => event.status.clone(),
        _ => "unknown".to_string(),
    }
}

fn decide(
    command: &CommandEnvelope,
    allow_dangerous: bool,
    allow_capabilities: &[String],
    policy: Option<&AuthorityPolicy>,
) -> Result<AuthorityDecision> {
    if let Some(reason) = capability_rejection_reason(command, allow_capabilities, policy) {
        return Ok(AuthorityDecision {
            status: "rejected".to_string(),
            reason: reason.clone(),
            reason_codes: vec!["capability_rejected".to_string(), reason],
            matched_policy_ids: vec!["capability.required".to_string()],
            approved_by: Vec::new(),
        });
    }
    if let Some(reason) = policy_rejection_reason(command, policy) {
        return Ok(AuthorityDecision {
            status: "rejected".to_string(),
            reason: reason.clone(),
            reason_codes: vec!["policy_rejected".to_string(), reason],
            matched_policy_ids: vec![policy_id(policy, "operator.session")],
            approved_by: Vec::new(),
        });
    }

    match command.danger_level {
        DangerLevel::ReadOnly | DangerLevel::Controlled => Ok(AuthorityDecision {
            status: "accepted".to_string(),
            reason: "gateway accepted envelope".to_string(),
            reason_codes: vec!["capability_ok".to_string(), "danger_level_ok".to_string()],
            matched_policy_ids: vec![
                "capability.required".to_string(),
                "danger.controlled".to_string(),
                policy_id(policy, "operator.session"),
            ],
            approved_by: approved_by(policy),
        }),
        DangerLevel::Dangerous if dangerous_allowed(command, allow_dangerous, policy) => {
            Ok(AuthorityDecision {
                status: "accepted".to_string(),
                reason: "dangerous envelope accepted by authority policy".to_string(),
                reason_codes: vec![
                    "capability_ok".to_string(),
                    "dangerous_authority_allowed".to_string(),
                ],
                matched_policy_ids: vec![
                    "capability.required".to_string(),
                    "danger.explicit_allow".to_string(),
                    policy_id(policy, "operator.session"),
                ],
                approved_by: approved_by(policy),
            })
        }
        DangerLevel::Dangerous => Ok(AuthorityDecision {
            status: "rejected".to_string(),
            reason: "dangerous envelope rejected by default authority policy".to_string(),
            reason_codes: vec!["dangerous_rejected_by_default".to_string()],
            matched_policy_ids: vec![
                "capability.required".to_string(),
                "danger.default_reject".to_string(),
            ],
            approved_by: Vec::new(),
        }),
    }
}

fn capability_rejection_reason(
    command: &CommandEnvelope,
    allow_capabilities: &[String],
    policy: Option<&AuthorityPolicy>,
) -> Option<String> {
    let expected = policy
        .and_then(|policy| {
            policy
                .command_capabilities
                .get(command.command_type.as_str())
                .map(String::as_str)
        })
        .or_else(|| expected_capability(command.command_type.as_str()))?;
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
    if let Some(policy) = policy {
        let allowed = policy
            .allow_capabilities
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        if !allowed.is_empty() && !allowed.contains(command.capability.as_str()) {
            return Some(format!(
                "capability {} is not allowed by policy {}",
                command.capability, policy.policy_version
            ));
        }
    }

    None
}

fn policy_rejection_reason(
    command: &CommandEnvelope,
    policy: Option<&AuthorityPolicy>,
) -> Option<String> {
    let policy = policy?;
    let Some(session) = matching_session(command, policy) else {
        return Some(format!(
            "no authority policy session for operator {} session {}",
            command.operator_id, command.session_id
        ));
    };
    if let Some(role) = command.requested_by_role.as_deref() {
        if !session.roles.is_empty() && !session.roles.iter().any(|allowed| allowed == role) {
            return Some(format!(
                "operator {} session {} lacks requested role {}",
                command.operator_id, command.session_id, role
            ));
        }
    }
    if !session.capabilities.is_empty()
        && !session
            .capabilities
            .iter()
            .any(|capability| capability == &command.capability)
    {
        return Some(format!(
            "operator {} session {} lacks capability {}",
            command.operator_id, command.session_id, command.capability
        ));
    }
    if !session.target_environments.is_empty()
        && !session
            .target_environments
            .iter()
            .any(|environment| environment == &command.target_environment)
    {
        return Some(format!(
            "operator {} session {} cannot target environment {}",
            command.operator_id, command.session_id, command.target_environment
        ));
    }
    if command.requires_mfa && !session.mfa_verified {
        return Some(format!(
            "operator {} session {} has no verified MFA for command {}",
            command.operator_id, command.session_id, command.command_id
        ));
    }
    None
}

fn matching_session<'a>(
    command: &CommandEnvelope,
    policy: &'a AuthorityPolicy,
) -> Option<&'a OperatorSessionPolicy> {
    policy.sessions.iter().find(|session| {
        session.operator_id == command.operator_id && session.session_id == command.session_id
    })
}

fn dangerous_allowed(
    command: &CommandEnvelope,
    allow_dangerous: bool,
    policy: Option<&AuthorityPolicy>,
) -> bool {
    if allow_dangerous && policy.is_none() {
        return true;
    }
    policy
        .and_then(|policy| matching_session(command, policy))
        .map(|session| session.allow_dangerous)
        .unwrap_or(false)
}

fn policy_id(policy: Option<&AuthorityPolicy>, suffix: &str) -> String {
    policy
        .map(|policy| format!("{}:{suffix}", policy.policy_version))
        .unwrap_or_else(|| suffix.to_string())
}

fn approved_by(policy: Option<&AuthorityPolicy>) -> Vec<String> {
    policy
        .map(|policy| vec![format!("command-gateway/{}", policy.policy_version)])
        .unwrap_or_else(|| vec!["command-gateway".to_string()])
}

fn expected_capability(command_type: &str) -> Option<&'static str> {
    match command_type {
        "PauseStrategyRequested"
        | "ResumeStrategyRequested"
        | "DrainStrategyRequested"
        | "KillStrategyRequested" => Some("strategy.control"),
        "CancelOrderRequested"
        | "CancelAllOrdersForSymbolRequested"
        | "CancelAllOrdersForAccountRequested" => Some("order.cancel"),
        "FlattenSymbolRequested" | "FlattenAccountRequested" => Some("account.flatten"),
        "GlobalKillSwitchRequested" | "AccountKillSwitchRequested" => Some("account.kill"),
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
    fn maps_account_kill_to_account_slot_cancel_all() {
        let command = command(
            CommandPayload::AccountKillSwitchRequested {
                account_id: "paper-main".to_string(),
            },
            "account.kill",
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
    fn account_kill_requires_account_slot_mapping() {
        let command = command(
            CommandPayload::AccountKillSwitchRequested {
                account_id: "paper-main".to_string(),
            },
            "account.kill",
        );

        let reason =
            broker_control_request(&command, &[]).expect_err("request must require slot mapping");

        assert!(reason.contains("--broker-account-slot paper-main=SLOT"));
    }

    #[test]
    fn dangerous_commands_still_require_gateway_flag() {
        let command = command(
            CommandPayload::GlobalKillSwitchRequested {
                account_id: "global".to_string(),
            },
            "account.kill",
        );

        let decision = decide(&command, false, &[], None).expect("decision should succeed");

        assert_eq!(decision.status, "rejected");
        assert!(decision.reason.contains("dangerous envelope rejected"));
        assert!(decision
            .reason_codes
            .contains(&"dangerous_rejected_by_default".to_string()));
    }

    fn policy() -> AuthorityPolicy {
        AuthorityPolicy {
            policy_version: "policy-test".to_string(),
            allow_capabilities: vec!["strategy.control".to_string(), "account.kill".to_string()],
            command_capabilities: BTreeMap::new(),
            sessions: vec![OperatorSessionPolicy {
                operator_id: "operator-test".to_string(),
                session_id: "session-test".to_string(),
                roles: vec!["trader".to_string()],
                capabilities: vec!["strategy.control".to_string()],
                target_environments: vec!["paper".to_string()],
                allow_dangerous: false,
                mfa_verified: false,
            }],
        }
    }

    #[test]
    fn policy_accepts_matching_controlled_command() {
        let mut command = command(
            CommandPayload::PauseStrategyRequested {
                strategy_id: "open-scalp".to_string(),
            },
            "strategy.control",
        );
        command.target_environment = "paper".to_string();
        command.requested_by_role = Some("trader".to_string());
        let policy = policy();

        let decision =
            decide(&command, false, &[], Some(&policy)).expect("decision should succeed");

        assert_eq!(decision.status, "accepted");
        assert!(decision
            .matched_policy_ids
            .iter()
            .any(|policy_id| policy_id == "policy-test:operator.session"));
    }

    #[test]
    fn policy_rejects_unknown_session() {
        let mut command = command(
            CommandPayload::PauseStrategyRequested {
                strategy_id: "open-scalp".to_string(),
            },
            "strategy.control",
        );
        command.session_id = "unknown-session".to_string();
        let policy = policy();

        let decision =
            decide(&command, false, &[], Some(&policy)).expect("decision should succeed");

        assert_eq!(decision.status, "rejected");
        assert!(decision.reason.contains("no authority policy session"));
    }

    #[test]
    fn policy_rejects_dangerous_without_session_grant() {
        let command = command(
            CommandPayload::GlobalKillSwitchRequested {
                account_id: "global".to_string(),
            },
            "account.kill",
        );
        let policy = policy();

        let decision = decide(&command, true, &[], Some(&policy)).expect("decision should succeed");

        assert_eq!(decision.status, "rejected");
    }
}
