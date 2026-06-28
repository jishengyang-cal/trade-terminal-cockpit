use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use trade_core::EventFilter;

#[derive(Clone, Debug, Parser)]
#[command(name = "trade-tui")]
#[command(about = "Trading domain terminal cockpit")]
pub struct Cli {
    #[arg(long, value_name = "PATH")]
    pub snapshot_json: Option<PathBuf>,

    #[arg(long, value_name = "PATH")]
    pub event_jsonl: Option<PathBuf>,

    #[arg(long, value_name = "PATH")]
    pub event_store_query_bin: Option<PathBuf>,

    #[arg(long, value_name = "URI")]
    pub event_store_uri: Option<String>,

    #[arg(long)]
    pub mock: bool,

    #[arg(long)]
    pub plain: bool,

    #[arg(long, requires = "event_jsonl")]
    pub follow: bool,

    #[arg(long, default_value_t = 250)]
    pub follow_poll_ms: u64,

    #[arg(long, value_name = "URL")]
    pub nats_url: Option<String>,

    #[arg(long = "nats-subject", value_name = "SUBJECT", requires = "nats_url")]
    pub nats_subjects: Vec<String>,

    #[arg(long, default_value = "json", value_parser = ["json", "protobuf"])]
    pub event_codec: String,

    #[arg(long, value_name = "STREAM", requires = "nats_url")]
    pub jetstream_stream: Option<String>,

    #[arg(long, value_name = "DURABLE", requires = "nats_url")]
    pub jetstream_durable: Option<String>,

    #[arg(long)]
    pub replay: bool,

    #[arg(long)]
    pub from: Option<String>,

    #[arg(long)]
    pub to: Option<String>,

    #[arg(long)]
    pub strategy_id: Option<String>,

    #[arg(long)]
    pub symbol: Option<String>,

    #[arg(long)]
    pub order_id: Option<String>,

    #[arg(long)]
    pub correlation_id: Option<String>,

    #[arg(long)]
    pub event_type: Option<String>,

    #[arg(long)]
    pub severity: Option<String>,

    #[arg(long)]
    pub source: Option<String>,

    #[arg(long, default_value_t = 20)]
    pub fps: u16,

    #[arg(long)]
    pub otel_stdout: bool,

    #[arg(long, default_value = "trade-tui")]
    pub otel_service_name: String,

    #[arg(long, value_name = "OPERATOR")]
    pub operator_id: Option<String>,

    #[arg(long, value_name = "SESSION")]
    pub session_id: Option<String>,

    #[arg(long, default_value = "trade-tui interactive command")]
    pub command_reason: String,

    #[arg(long, value_name = "PATH")]
    pub command_gateway_bin: Option<PathBuf>,

    #[arg(long, value_name = "ADDR")]
    pub command_gateway_addr: Option<String>,

    #[arg(
        long,
        value_name = "PATH",
        default_value = ".run/trade-tui-command-audit.jsonl"
    )]
    pub command_gateway_audit_jsonl: PathBuf,

    #[arg(long)]
    pub command_gateway_allow_dangerous: bool,

    #[arg(long)]
    pub command_gateway_execute_broker_control: bool,

    #[arg(long, value_name = "PATH")]
    pub broker_runtime_dir: Option<PathBuf>,

    #[arg(long, value_name = "PATH")]
    pub broker_control_bin: Option<PathBuf>,

    #[arg(long = "broker-account-slot", value_name = "ACCOUNT_ID=SLOT")]
    pub broker_account_slots: Vec<String>,

    #[arg(long, value_name = "PATH")]
    pub risk_check_bin: Option<PathBuf>,

    #[arg(long, value_name = "PATH")]
    pub strategy_control_bin: Option<PathBuf>,

    #[arg(long, value_name = "PATH")]
    pub order_gateway_bin: Option<PathBuf>,

    #[arg(long, value_name = "PATH")]
    pub alert_service_bin: Option<PathBuf>,

    #[arg(long, default_value = "paper")]
    pub target_environment: String,
}

impl Cli {
    pub fn event_filter(&self) -> Result<EventFilter> {
        Ok(EventFilter {
            strategy_id: self.strategy_id.clone(),
            symbol: self.symbol.clone(),
            order_id: self.order_id.clone(),
            correlation_id: self.correlation_id.clone(),
            event_type: self.event_type.clone(),
            severity: self.severity.clone(),
            source: self.source.clone(),
            from_ts_ns: parse_time_bound(self.from.as_deref(), "--from")?,
            to_ts_ns: parse_time_bound(self.to.as_deref(), "--to")?,
            ..EventFilter::default()
        })
    }
}

fn parse_time_bound(value: Option<&str>, name: &str) -> Result<Option<i64>> {
    value
        .map(|value| parse_timestamp_ns(value, name))
        .transpose()
}

fn parse_timestamp_ns(value: &str, name: &str) -> Result<i64> {
    let value = value.trim();
    if value.chars().all(|ch| ch.is_ascii_digit()) {
        return Ok(value.parse()?);
    }

    parse_iso_utc_ns(value).ok_or_else(|| {
        anyhow::anyhow!("{name} must be unix nanoseconds or UTC timestamp like 2026-06-25T09:30:00")
    })
}

fn parse_iso_utc_ns(value: &str) -> Option<i64> {
    let value = value.strip_suffix('Z').unwrap_or(value);
    let (date, time) = value.split_once('T').or_else(|| value.split_once(' '))?;

    let mut date_parts = date.split('-');
    let year = date_parts.next()?.parse::<i64>().ok()?;
    let month = date_parts.next()?.parse::<i64>().ok()?;
    let day = date_parts.next()?.parse::<i64>().ok()?;
    if date_parts.next().is_some() {
        return None;
    }

    let mut time_parts = time.split(':');
    let hour = time_parts.next()?.parse::<i64>().ok()?;
    let minute = time_parts.next()?.parse::<i64>().ok()?;
    let second_part = time_parts.next()?;
    if time_parts.next().is_some() {
        return None;
    }

    let (second_text, fraction_text) = second_part
        .split_once('.')
        .map(|(second, fraction)| (second, Some(fraction)))
        .unwrap_or((second_part, None));
    let second = second_text.parse::<i64>().ok()?;
    let nanos = parse_fraction_ns(fraction_text)?;

    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || !(0..=23).contains(&hour)
        || !(0..=59).contains(&minute)
        || !(0..=59).contains(&second)
    {
        return None;
    }

    let days = days_from_civil(year, month, day);
    let seconds = (((days * 24 + hour) * 60 + minute) * 60) + second;
    seconds.checked_mul(1_000_000_000)?.checked_add(nanos)
}

fn parse_fraction_ns(fraction: Option<&str>) -> Option<i64> {
    let Some(fraction) = fraction else {
        return Some(0);
    };
    if fraction.is_empty() || fraction.len() > 9 || !fraction.chars().all(|ch| ch.is_ascii_digit())
    {
        return None;
    }

    let mut padded = fraction.to_string();
    while padded.len() < 9 {
        padded.push('0');
    }
    padded.parse::<i64>().ok()
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}
