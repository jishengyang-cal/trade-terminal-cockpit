use crate::app::{App, Screen};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Paragraph, Tabs, Wrap};
use ratatui::Frame;
use trade_core::state::{AppState, EventSummary, OrderChain};

pub fn plain_summary(state: &AppState, replay: bool, filter_summary: Option<&str>) -> String {
    let mut summary = format!(
        "mode={} account={} risk={} strategies={} orders={} positions={} open_alerts={} last_seq={}",
        if replay { "REPLAY" } else { "READ_ONLY" },
        state.account.account_id,
        state.risk.global_state,
        state.strategies.by_id.len(),
        state.orders.by_correlation_id.len(),
        state.positions.by_key.len(),
        state.alerts.open_count(),
        state
            .connection
            .last_event_sequence
            .map(|seq| seq.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    if let Some(filter_summary) = filter_summary {
        summary.push_str(" filter=\"");
        summary.push_str(filter_summary);
        summary.push('"');
    }
    summary
}

pub fn render(frame: &mut Frame<'_>, app: &App) {
    let area = frame.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(1),
        ])
        .split(area);

    render_status(frame, chunks[0], app);
    render_tabs(frame, chunks[1], app.screen);
    match app.screen {
        Screen::Overview => render_overview(frame, chunks[2], &app.state),
        Screen::Strategies => render_strategies(frame, chunks[2], &app.state),
        Screen::Orders => render_orders(frame, chunks[2], app),
        Screen::Positions => render_positions(frame, chunks[2], &app.state),
        Screen::Risk => render_risk(frame, chunks[2], &app.state),
        Screen::Events => render_events(frame, chunks[2], app),
        Screen::Replay => render_replay(frame, chunks[2], app),
    }
    render_footer(frame, chunks[3], app);
}

fn render_status(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let state = &app.state;
    let mode = if app.replay {
        "REPLAY"
    } else {
        state.account.mode.as_str()
    };
    let status = format!(
        " ACCT:{} | {} | {} | PNL:{:+.2} | EXP:{:.1}% | SRC:{} ",
        truncate(&state.account.account_id, 14),
        truncate(mode, 6),
        truncate(&state.risk.global_state, 10),
        state.account.day_pnl,
        state.account.exposure_pct,
        truncate(&state.connection.nats, 8),
    );
    frame.render_widget(
        Paragraph::new(status).style(Style::default().bg(Color::Black).fg(Color::Green)),
        area,
    );
}

fn render_tabs(frame: &mut Frame<'_>, area: Rect, screen: Screen) {
    let titles = Screen::ALL
        .iter()
        .map(|screen| Line::from(screen.title()))
        .collect::<Vec<_>>();
    let tabs = Tabs::new(titles)
        .select(screen.index())
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Trading Cockpit"),
        )
        .style(Style::default().fg(Color::Gray))
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        );
    frame.render_widget(tabs, area);
}

fn render_overview(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    let sections = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(36),
            Constraint::Percentage(32),
            Constraint::Percentage(32),
        ])
        .split(area);

    let account = vec![
        kv("account", &state.account.account_id),
        kv("mode", &state.account.mode),
        kv("broker", &state.account.broker),
        kv("broker_ok", mark(state.account.broker_connected)),
        kv("cash", &format!("{:.2}", state.account.cash)),
        kv("buy_power", &format!("{:.2}", state.account.buying_power)),
        kv("day_pnl", &format!("{:+.2}", state.account.day_pnl)),
        kv("realized", &format!("{:+.2}", state.account.realized_pnl)),
        kv(
            "unrealized",
            &format!("{:+.2}", state.account.unrealized_pnl),
        ),
        kv("exposure", &format!("{:.1}%", state.account.exposure_pct)),
        kv("margin", &format!("{:.1}%", state.account.margin_usage_pct)),
    ];
    frame.render_widget(panel("Account", account), sections[0]);

    let system = vec![
        kv("nats", &state.connection.nats),
        kv("cmd_gw", &state.connection.command_gateway),
        kv(
            "last_seq",
            &state
                .connection
                .last_event_sequence
                .map(|seq| seq.to_string())
                .unwrap_or_else(|| "-".to_string()),
        ),
        kv("fps", &state.connection.render_fps.to_string()),
        kv("strategies", &state.strategies.by_id.len().to_string()),
        kv("orders", &state.orders.by_correlation_id.len().to_string()),
        kv("positions", &state.positions.by_key.len().to_string()),
        kv("alerts", &state.alerts.open_count().to_string()),
    ];
    frame.render_widget(panel("System", system), sections[1]);

    let risk = vec![
        kv("state", &state.risk.global_state),
        kv("kill_ok", mark(!state.risk.kill_switch_active)),
        kv("md_fresh", mark(state.risk.market_data_fresh)),
        kv("orders_ok", mark(state.risk.broker_order_channel_ok)),
        kv("loss_ok", mark(!state.risk.day_max_loss_breached)),
        kv("quote_ok", mark(state.risk.quote_staleness_ok)),
        kv("short_ok", &state.account.short_permission.to_string()),
        kv(
            "short_blk",
            &state.account.short_intents_blocked_today.to_string(),
        ),
    ];
    frame.render_widget(panel("Risk", risk), sections[2]);
}

fn render_strategies(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    let mut lines = vec![format!(
        "{:<24} {:<8} {:<7} {:>7} {:>8} {:>8} {:>8} {:>9}",
        "STRATEGY", "STATE", "MODE", "UNIV", "SIGNALS", "INTENTS", "ORDERS", "PNL"
    )];
    for strategy in state.strategies.by_id.values() {
        lines.push(format!(
            "{:<24} {:<8} {:<7} {:>7} {:>8} {:>8} {:>8} {:>+9.2}",
            truncate(&strategy.strategy_id, 24),
            truncate(&strategy.state, 8),
            truncate(&strategy.mode, 7),
            strategy.universe_count,
            strategy.signals,
            strategy.intents,
            strategy.orders,
            strategy.pnl,
        ));
    }
    frame.render_widget(panel("Strategies", lines), area);
}

fn render_orders(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let state = &app.state;
    let sections = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(area);

    let mut rows = vec![format!(
        "  {:<12} {:<7} {:<4} {:>4}",
        "CORR", "STATE", "SYM", "FILL"
    )];
    for (index, chain) in state.orders.by_correlation_id.values().enumerate() {
        rows.push(format_order_row(chain, index == app.selected_order_index));
    }
    frame.render_widget(panel("Order Chains", rows), sections[0]);

    let timeline = selected_chain(state, app.selected_order_index)
        .map(|chain| {
            let mut lines = vec![
                kv_wide("correlation_id", &chain.correlation_id),
                kv_wide("state", &format!("{:?}", chain.state)),
                kv_wide("order_id", chain.order_id.as_deref().unwrap_or("-")),
                kv_wide("symbol", chain.symbol.as_deref().unwrap_or("-")),
                kv_wide("side", chain.side.as_deref().unwrap_or("-")),
                kv_wide("filled_qty", &chain.filled_quantity.to_string()),
                String::new(),
            ];
            for entry in &chain.timeline {
                lines.push(format!(
                    "#{:<4} {:<13} {}",
                    entry.sequence,
                    truncate(&entry.kind, 13),
                    entry.summary
                ));
            }
            lines
        })
        .unwrap_or_else(|| vec!["no order chain events".to_string()]);
    frame.render_widget(panel("Lifecycle", timeline), sections[1]);
}

fn render_positions(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    let mut lines = vec![format!(
        "{:<8} {:>8} {:>10} {:>10} {:>10}  {}",
        "SYMBOL", "NET_QTY", "AVG_PX", "MKT_PX", "UPNL", "ATTRIBUTION"
    )];
    for position in state.positions.by_key.values() {
        let attribution = position
            .strategy_attribution
            .iter()
            .map(|item| format!("{}:{}", item.strategy_id, item.quantity))
            .collect::<Vec<_>>()
            .join(", ");
        lines.push(format!(
            "{:<8} {:>8} {:>10.2} {:>10.2} {:>+10.2}  {}",
            position.symbol,
            position.net_quantity,
            position.average_price,
            position.market_price,
            position.unrealized_pnl,
            attribution,
        ));
    }
    lines.push(String::new());
    lines.push(format!(
        "SHORT_PERMISSION: {}",
        state.account.short_permission
    ));
    lines.push(format!(
        "SHORT_INTENTS_BLOCKED_TODAY: {}",
        state.account.short_intents_blocked_today
    ));
    frame.render_widget(panel("Positions", lines), area);
}

fn render_risk(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    let sections = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(area);

    let mut global = vec![
        kv_wide("account_connected", mark(state.account.broker_connected)),
        kv_wide("market_data_fresh", mark(state.risk.market_data_fresh)),
        kv_wide("order_channel_ok", mark(state.risk.broker_order_channel_ok)),
        kv_wide("day_loss_ok", mark(!state.risk.day_max_loss_breached)),
        kv_wide("quote_stale_ok", mark(state.risk.quote_staleness_ok)),
        kv_wide(
            "short_permission",
            &state.account.short_permission.to_string(),
        ),
        String::new(),
        "LIMITS".to_string(),
    ];
    for (key, value) in &state.risk.limits {
        global.push(kv_wide(key, value));
    }
    frame.render_widget(panel("Global Risk", global), sections[0]);

    let mut blocks = Vec::new();
    if state.risk.active_blocks.is_empty() {
        blocks.push("no active blocks".to_string());
    } else {
        for block in &state.risk.active_blocks {
            blocks.push(format!(
                "{} / {} / {}",
                block.scope, block.severity, block.message
            ));
        }
    }
    frame.render_widget(panel("Active Blocks", blocks), sections[1]);
}

fn render_events(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let state = &app.state;
    let sections = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(62), Constraint::Percentage(38)])
        .split(area);

    let lines = state
        .audit
        .events
        .iter()
        .rev()
        .take(200)
        .enumerate()
        .map(|(index, event)| format_event_row(event, index == app.selected_event_index))
        .collect::<Vec<_>>();
    frame.render_widget(panel("Events / Audit", lines), sections[0]);

    let detail = selected_event(state, app.selected_event_index)
        .map(|event| {
            vec![
                kv_wide("sequence", &event.sequence.to_string()),
                kv_wide("event_type", &event.event_type),
                kv_wide("aggregate_type", &event.aggregate_type),
                kv_wide("aggregate_id", &event.aggregate_id),
                kv_wide("correlation_id", &event.correlation_id),
                kv_wide("producer", &event.producer),
                kv_wide("publish_ts_ns", &event.ts_ns.to_string()),
                String::new(),
                event.headline.clone(),
            ]
        })
        .unwrap_or_else(|| vec!["no events".to_string()]);
    frame.render_widget(panel("Event Detail", detail), sections[1]);
}

fn render_replay(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let lines = vec![
        "REPLAY MODE".to_string(),
        "live command gateway is disabled".to_string(),
        format!(
            "from                  {}",
            app.replay_from.as_deref().unwrap_or("-")
        ),
        format!(
            "to                    {}",
            app.replay_to.as_deref().unwrap_or("-")
        ),
        format!("events_loaded          {}", app.state.audit.events.len()),
        format!(
            "orders_rebuilt         {}",
            app.state.orders.by_correlation_id.len()
        ),
        format!(
            "active_filter          {}",
            app.filter_summary.as_deref().unwrap_or("-")
        ),
    ];
    frame.render_widget(panel("Replay", lines), area);
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let text = match app.screen {
        Screen::Orders | Screen::Events if app.replay => {
            " REPLAY: no live commands | up/down or j/k select | q exits "
        }
        Screen::Orders | Screen::Events => {
            " READ ONLY: up/down or j/k select | commands are externalized through tradectl | q exits "
        }
        _ if app.replay => " REPLAY: no live commands | q exits ",
        _ => {
            " READ ONLY: command execution is externalized through tradectl/command-gateway | q exits "
        }
    };
    frame.render_widget(
        Paragraph::new(text).style(Style::default().bg(Color::Black).fg(Color::Gray)),
        area,
    );
}

fn panel(title: &'static str, lines: Vec<String>) -> Paragraph<'static> {
    let lines = lines.into_iter().map(Line::from).collect::<Vec<_>>();
    Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title(title))
        .wrap(Wrap { trim: false })
}

fn selected_chain(state: &AppState, selected_index: usize) -> Option<&OrderChain> {
    state.orders.by_correlation_id.values().nth(selected_index)
}

fn selected_event(state: &AppState, selected_index: usize) -> Option<&EventSummary> {
    state.audit.events.iter().rev().nth(selected_index)
}

fn format_order_row(chain: &OrderChain, selected: bool) -> String {
    format!(
        "{} {:<12} {:<7} {:<4} {:>4}",
        if selected { ">" } else { " " },
        truncate(&chain.correlation_id, 12),
        truncate(&format!("{:?}", chain.state), 7),
        truncate(chain.symbol.as_deref().unwrap_or("-"), 4),
        chain.filled_quantity,
    )
}

fn format_event_row(event: &EventSummary, selected: bool) -> String {
    format!(
        "{} #{:<4} {:<22} {:<12} corr={} {}",
        if selected { ">" } else { " " },
        event.sequence,
        truncate(&event.event_type, 22),
        truncate(&event.aggregate_type, 12),
        truncate(&event.correlation_id, 14),
        event.headline
    )
}

fn mark(ok: bool) -> &'static str {
    if ok {
        "[x]"
    } else {
        "[ ]"
    }
}

fn truncate(value: &str, max_chars: usize) -> String {
    let mut output = String::new();
    for (index, ch) in value.chars().enumerate() {
        if index == max_chars {
            break;
        }
        output.push(ch);
    }
    output
}

fn kv(label: &str, value: &str) -> String {
    format!("{:<11} {}", truncate(label, 11), value)
}

fn kv_wide(label: &str, value: &str) -> String {
    format!("{:<21} {}", truncate(label, 21), value)
}
