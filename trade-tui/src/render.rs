use crate::app::{App, Screen};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Tabs, Wrap};
use ratatui::Frame;
use trade_core::state::{AccountView, AppState, EventSummary, OrderChain, StrategyView};

pub fn plain_summary(state: &AppState, replay: bool, filter_summary: Option<&str>) -> String {
    let mut summary = format!(
        "mode={} account={} accounts={} risk={} strategies={} orders={} positions={} open_alerts={} last_seq={} events_ingested={} events_coalesced={} audit_retained={}",
        if replay { "REPLAY" } else { "READ_ONLY" },
        state.account.account_id,
        state.accounts.len(),
        state.risk.global_state,
        state.strategies.by_id.len(),
        state.orders.by_correlation_id.len(),
        state.positions.by_key.len(),
        state.alerts.open_count(),
        state
            .connection
            .last_event_sequence
            .map(|seq| seq.to_string())
            .unwrap_or_else(|| "-".to_string()),
        state.connection.events_ingested,
        state.connection.events_coalesced,
        state.connection.audit_events_retained
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
        Screen::Help => render_help(frame, chunks[2]),
        Screen::Overview => render_overview(frame, chunks[2], app),
        Screen::Strategies => render_strategies(frame, chunks[2], app),
        Screen::Orders => render_orders(frame, chunks[2], app),
        Screen::Positions => render_positions(frame, chunks[2], &app.state),
        Screen::Risk => render_risk(frame, chunks[2], app),
        Screen::Events => render_events(frame, chunks[2], app),
        Screen::Replay => render_replay(frame, chunks[2], app),
        Screen::Commands => render_commands(frame, chunks[2], app),
    }
    render_footer(frame, chunks[3], app);
    if app.dangerous_action.is_some() {
        render_dangerous_modal(frame, area, app);
    }
}

fn render_status(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let state = &app.state;
    let selected_account = selected_account(state, app.selected_account_index);
    let mode = if app.replay {
        "REPLAY"
    } else {
        selected_account.mode.as_str()
    };
    let status = format!(
        " ACCTS:{} | SEL:{} | {} | RISK:{} | PNL:{:+.2} | EXP:{:.1}% | LAG:{}ms | SRC:{} ",
        state.accounts.len(),
        truncate(&selected_account_id(app), 14),
        truncate(mode, 6),
        truncate(&state.risk.global_state, 10),
        state.account.day_pnl,
        state.account.exposure_pct,
        state.connection.event_lag_ms,
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

fn render_overview(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let state = &app.state;
    let sections = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(36),
            Constraint::Percentage(32),
            Constraint::Percentage(32),
        ])
        .split(area);

    let mut accounts = vec![format!(
        "  {:<14} {:<6} {:<8} {:>9} {:>6} {}",
        "ACCOUNT", "MODE", "BROKER", "DAY_PNL", "EXP", "CTRL"
    )];
    for (index, account) in state.accounts.by_id.values().enumerate() {
        accounts.push(format_account_row(
            account,
            index == app.selected_account_index,
        ));
    }
    frame.render_widget(panel("Accounts", accounts), sections[0]);

    let selected_account = selected_account(state, app.selected_account_index);
    let account_detail = vec![
        kv("account", &selected_account.account_id),
        kv("mode", &selected_account.mode),
        kv("broker", &selected_account.broker),
        kv("broker_ok", mark(selected_account.broker_connected)),
        kv("cash", &selected_account.cash_value.to_string()),
        kv(
            "buy_power",
            &selected_account.buying_power_value.to_string(),
        ),
        kv("day_pnl", &selected_account.day_pnl_value.to_string()),
        kv("realized", &selected_account.realized_pnl_value.to_string()),
        kv(
            "unrealized",
            &selected_account.unrealized_pnl_value.to_string(),
        ),
        kv("net_liq", &selected_account.net_liquidation.to_string()),
        kv("avail", &selected_account.available_funds.to_string()),
        kv(
            "maint_mgn",
            &selected_account.maintenance_margin.to_string(),
        ),
        kv("pdt", selected_account.pdt_status.as_deref().unwrap_or("-")),
        kv(
            "day_trades",
            &selected_account
                .day_trades_remaining
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
        ),
        kv(
            "exposure",
            &format!("{:.1}%", selected_account.exposure_pct),
        ),
        kv(
            "margin",
            &format!("{:.1}%", selected_account.margin_usage_pct),
        ),
        kv("runtime", &runtime_flags(selected_account)),
    ];
    frame.render_widget(panel("Selected Account", account_detail), sections[1]);

    let system = vec![
        kv("global", &state.risk.global_state),
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
        kv(
            "render_ms",
            &state.connection.last_render_duration_ms.to_string(),
        ),
        kv(
            "slow_frames",
            &state.connection.render_slow_frames.to_string(),
        ),
        kv(
            "drained",
            &state.connection.events_drained_last_tick.to_string(),
        ),
        kv(
            "backlog",
            &state
                .connection
                .event_rx_backlog_estimate
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
        ),
        kv("ingested", &state.connection.events_ingested.to_string()),
        kv("coalesced", &state.connection.events_coalesced.to_string()),
        kv("dupes", &state.connection.duplicate_events.to_string()),
        kv("gaps", &state.connection.sequence_gaps.to_string()),
        kv("decode_err", &state.connection.decode_errors.to_string()),
        kv("ingest_err", &state.connection.ingest_errors.to_string()),
        kv("filtered", &state.connection.filtered_events.to_string()),
        kv("js_acks", &state.connection.jetstream_acks.to_string()),
        kv(
            "retained",
            &state.connection.audit_events_retained.to_string(),
        ),
        kv(
            "md_dropped",
            &state.connection.dropped_market_updates.to_string(),
        ),
        kv(
            "nats_reconn",
            &state.connection.nats_reconnect_count.to_string(),
        ),
        kv("strategies", &state.strategies.by_id.len().to_string()),
        kv("orders", &state.orders.by_correlation_id.len().to_string()),
        kv("commands", &state.commands.by_id.len().to_string()),
        kv("positions", &state.positions.by_key.len().to_string()),
        kv("alerts", &state.alerts.open_count().to_string()),
        kv(
            "last_err",
            state.connection.last_error.as_deref().unwrap_or("-"),
        ),
    ];
    frame.render_widget(panel("System", system), sections[2]);
}

fn render_help(frame: &mut Frame<'_>, area: Rect) {
    let lines = vec![
        "Terminal cockpit keys".to_string(),
        String::new(),
        "F1 Help        F2 Overview     F3 Strategies".to_string(),
        "F4 Orders      F5 Positions    F6 Risk".to_string(),
        "F7 Events      F8 Replay       F9 Commands".to_string(),
        "F10 Exit       Tab next        Shift-Tab previous".to_string(),
        "/ search       : palette       q/Esc exit view".to_string(),
        "Up/Down        j/k select rows".to_string(),
        String::new(),
        "Risk actions".to_string(),
        "K global kill switch preview".to_string(),
        "A account kill switch preview".to_string(),
        "F flatten selected account preview".to_string(),
        String::new(),
        "All actions remain command-envelope previews. Broker execution stays outside trade-tui."
            .to_string(),
    ];
    frame.render_widget(panel("Help", lines), area);
}

fn render_strategies(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let state = &app.state;
    let sections = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(56), Constraint::Percentage(44)])
        .split(area);

    let mut lines = vec![format!(
        "  {:<18} {:<7} {:<5} {:>5} {:>5} {:>5}",
        "STRATEGY", "STATE", "MODE", "SIG", "INT", "ORD"
    )];
    for (index, strategy) in state
        .strategies
        .by_id
        .values()
        .filter(|strategy| strategy_matches_search(strategy, &app.search_query))
        .enumerate()
    {
        lines.push(format_strategy_row(
            strategy,
            index == app.selected_strategy_index,
        ));
    }
    frame.render_widget(panel("Strategies", lines), sections[0]);

    let detail = selected_strategy(state, &app.search_query, app.selected_strategy_index)
        .map(strategy_detail_lines)
        .unwrap_or_else(|| vec!["no strategy projection".to_string()]);
    frame.render_widget(panel("Strategy Detail", detail), sections[1]);
}

fn render_orders(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let state = &app.state;
    let sections = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(area);

    let mut rows = vec![format!(
        "  {:<10} {:<7} {:<8} {:<5} {:<4} {:>9} {:<6} {:<10} {:>7}",
        "CORR", "STATE", "ACCT", "SIDE", "SYM", "FILL/REM", "TYPE", "BRK_STAT", "ACK_MS"
    )];
    for (index, chain) in state
        .orders
        .by_correlation_id
        .values()
        .filter(|chain| order_matches_search(chain, &app.search_query))
        .enumerate()
    {
        rows.push(format_order_row(chain, index == app.selected_order_index));
    }
    frame.render_widget(panel("Order Chains", rows), sections[0]);

    let timeline = selected_chain(state, &app.search_query, app.selected_order_index)
        .map(|chain| {
            let anomalies = if chain.anomalies.is_empty() {
                "-".to_string()
            } else {
                chain.anomalies.join(";")
            };
            let mut lines = vec![
                kv_wide("correlation_id", &chain.correlation_id),
                kv_wide("state", &format!("{:?}", chain.state)),
                kv_wide("order_id", chain.order_id.as_deref().unwrap_or("-")),
                kv_wide(
                    "client_order_id",
                    chain.client_order_id.as_deref().unwrap_or("-"),
                ),
                kv_wide(
                    "broker_order_id",
                    chain.broker_order_id.as_deref().unwrap_or("-"),
                ),
                kv_wide("perm_id", chain.perm_id.as_deref().unwrap_or("-")),
                kv_wide("symbol", chain.symbol.as_deref().unwrap_or("-")),
                kv_wide("side", chain.side.as_deref().unwrap_or("-")),
                kv_wide("order_type", chain.order_type.as_deref().unwrap_or("-")),
                kv_wide("limit", &format_optional_price(chain.limit_price.as_ref())),
                kv_wide("route", chain.route.as_deref().unwrap_or("-")),
                kv_wide("exchange", chain.exchange.as_deref().unwrap_or("-")),
                kv_wide(
                    "broker_status",
                    chain.broker_status.as_deref().unwrap_or("-"),
                ),
                kv_wide("filled_qty", &chain.filled_quantity.to_string()),
                kv_wide(
                    "remaining_qty",
                    &chain
                        .remaining_quantity
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                ),
                kv_wide(
                    "avg_fill",
                    &format_optional_price(chain.average_fill_price.as_ref()),
                ),
                kv_wide(
                    "last_fill",
                    &format_optional_price(chain.last_fill_price.as_ref()),
                ),
                kv_wide(
                    "submit_ts_ns",
                    &chain
                        .submit_ts_ns
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                ),
                kv_wide(
                    "ack_ts_ns",
                    &chain
                        .ack_ts_ns
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                ),
                kv_wide(
                    "first_fill_ts_ns",
                    &chain
                        .first_fill_ts_ns
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                ),
                kv_wide(
                    "terminal_ts_ns",
                    &chain
                        .terminal_ts_ns
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                ),
                kv_wide(
                    "submit_to_ack_ms",
                    &chain
                        .latency
                        .submit_to_ack_ms
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                ),
                kv_wide(
                    "ack_to_fill_ms",
                    &chain
                        .latency
                        .ack_to_first_fill_ms
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                ),
                kv_wide(
                    "submit_to_term_ms",
                    &chain
                        .latency
                        .submit_to_terminal_ms
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                ),
                kv_wide("anomalies", &anomalies),
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
        "{:<14} {:<8} {:>8} {:>10} {:>10} {:>10}  {}",
        "ACCOUNT", "SYMBOL", "NET_QTY", "AVG_PX", "MKT_PX", "UPNL", "ATTRIBUTION"
    )];
    for position in state.positions.by_key.values() {
        let attribution = position
            .strategy_attribution
            .iter()
            .map(|item| format!("{}:{}", item.strategy_id, item.quantity))
            .collect::<Vec<_>>()
            .join(", ");
        lines.push(format!(
            "{:<14} {:<8} {:>8} {:>10} {:>10} {:>+10.2}  {}",
            truncate(&position.account_id, 14),
            position.symbol,
            position.net_quantity,
            position.average_price.display_value(),
            position.market_price.display_value(),
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

fn render_risk(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let state = &app.state;
    let sections = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(34),
            Constraint::Percentage(33),
            Constraint::Percentage(33),
        ])
        .split(area);

    let mut global = vec![
        kv_wide("accounts_connected", mark(state.account.broker_connected)),
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
    if state.risk.structured_limits.is_empty() {
        for (key, value) in &state.risk.limits {
            global.push(kv_wide(key, value));
        }
    } else {
        global.push(format!(
            "{:<16} {:<10} {:>10} {:>10} {:<6}",
            "RULE", "SCOPE", "OBSERVED", "LIMIT", "STATUS"
        ));
        for limit in state.risk.structured_limits.iter().rev().take(10).rev() {
            global.push(format!(
                "{:<16} {:<10} {:>10} {:>10} {:<6}",
                truncate(&limit.rule_id, 16),
                truncate(&limit.scope, 10),
                truncate(&limit.observed, 10),
                truncate(&limit.limit, 10),
                truncate(&limit.status, 6)
            ));
        }
    }
    frame.render_widget(panel("Global Risk", global), sections[0]);

    let selected_account = selected_account(state, app.selected_account_index);
    let account = vec![
        kv_wide("account", &selected_account.account_id),
        kv_wide("broker_ok", mark(selected_account.broker_connected)),
        kv_wide("net_liq", &selected_account.net_liquidation.to_string()),
        kv_wide("available", &selected_account.available_funds.to_string()),
        kv_wide(
            "maint_margin",
            &selected_account.maintenance_margin.to_string(),
        ),
        kv_wide("day_pnl", &selected_account.day_pnl_value.to_string()),
        kv_wide(
            "unrealized",
            &selected_account.unrealized_pnl_value.to_string(),
        ),
        kv_wide(
            "gross_exposure",
            &selected_account.gross_exposure.to_string(),
        ),
        kv_wide("net_exposure", &selected_account.net_exposure.to_string()),
        kv_wide(
            "exposure",
            &format!("{:.1}%", selected_account.exposure_pct),
        ),
        kv_wide(
            "short_permission",
            &selected_account.short_permission.to_string(),
        ),
        kv_wide(
            "short_blocked",
            &selected_account.short_intents_blocked_today.to_string(),
        ),
        kv_wide("pdt", selected_account.pdt_status.as_deref().unwrap_or("-")),
        kv_wide(
            "restriction",
            selected_account
                .trading_restriction
                .as_deref()
                .unwrap_or("-"),
        ),
        kv_wide("runtime", &runtime_flags(selected_account)),
    ];
    frame.render_widget(panel("Selected Account Risk", account), sections[1]);

    let mut blocks = Vec::new();
    if state.risk.active_blocks.is_empty() {
        blocks.push("no active blocks".to_string());
    } else {
        for block in &state.risk.active_blocks {
            blocks.push(format!(
                "{} / {} / {} / {}",
                block.scope, block.rule_id, block.severity, block.message
            ));
        }
    }
    frame.render_widget(panel("Active Blocks", blocks), sections[2]);
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
        .filter(|event| event_matches_search(event, &app.search_query))
        .enumerate()
        .map(|(index, event)| format_event_row(event, index == app.selected_event_index))
        .collect::<Vec<_>>();
    frame.render_widget(panel("Events / Audit", lines), sections[0]);

    let detail = selected_event(state, &app.search_query, app.selected_event_index)
        .map(|event| {
            vec![
                kv_narrow("seq", &event.sequence.to_string()),
                kv_narrow("type", &event.event_type),
                kv_narrow("agg", &event.aggregate_type),
                kv_narrow("id", &event.aggregate_id),
                kv_narrow("corr", &event.correlation_id),
                kv_narrow("prod", &event.producer),
                kv_narrow("ts_ns", &event.ts_ns.to_string()),
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

fn render_commands(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let mut lines = vec![
        "Command palette".to_string(),
        kv_wide("input", &app.command_palette_input),
        String::new(),
        "Recent command evidence".to_string(),
    ];
    if app.state.commands.by_id.is_empty() {
        lines.push("no command authority/audit events".to_string());
    } else {
        lines.push(format!(
            "{:<16} {:<20} {:<10} {:<12} {}",
            "COMMAND_ID", "TYPE", "AUTH", "AUDIT", "SCOPE"
        ));
        for command in app.state.commands.by_id.values().rev().take(12) {
            lines.push(format!(
                "{:<16} {:<20} {:<10} {:<12} {}",
                truncate(&command.command_id, 16),
                truncate(command.command_type.as_deref().unwrap_or("-"), 20),
                truncate(command.authority_status.as_deref().unwrap_or("-"), 10),
                truncate(command.audit_status.as_deref().unwrap_or("-"), 12),
                truncate(
                    command
                        .scope
                        .as_deref()
                        .or(command.target.as_deref())
                        .unwrap_or("-"),
                    24
                )
            ));
            if !command.reason_codes.is_empty() {
                lines.push(format!("  reason_codes {}", command.reason_codes.join(",")));
            }
            if !command.matched_policy_ids.is_empty() {
                lines.push(format!(
                    "  policies     {}",
                    command.matched_policy_ids.join(",")
                ));
            }
        }
    }
    lines.extend([
        String::new(),
        "Replayable tradectl examples".to_string(),
        "pause-strategy <strategy_id>".to_string(),
        "resume-strategy <strategy_id>".to_string(),
        "drain-strategy <strategy_id>".to_string(),
        "cancel-order <account_id> <order_id>".to_string(),
        "cancel-all-orders-for-account <account_id> --confirm 'CANCEL ALL ACCOUNT <account_id>'"
            .to_string(),
        "flatten-account <account_id> --confirm 'FLATTEN ACCOUNT <account_id>'".to_string(),
        "account-kill-switch <account_id> --confirm 'KILL ACCOUNT <account_id>'".to_string(),
        "flatten-symbol <account_id> <symbol> --confirm 'FLATTEN <account_id> <symbol>'"
            .to_string(),
        "global-kill-switch global --confirm 'KILL global'".to_string(),
        String::new(),
        "TUI does not send these commands. Use tradectl or command-gateway audit flow.".to_string(),
    ]);
    frame.render_widget(panel("Commands", lines), area);
}

fn render_dangerous_modal(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let Some(action) = app.dangerous_action.as_ref() else {
        return;
    };
    let modal = centered_rect(74, 60, area);
    frame.render_widget(Clear, modal);

    let mut lines = vec![
        "DANGEROUS ACTION".to_string(),
        String::new(),
        kv_wide("action", &action.action),
        kv_wide("target", &action.target),
        String::new(),
        "Effect".to_string(),
    ];
    for effect in &action.effects {
        lines.push(format!("- {effect}"));
    }
    lines.extend([
        String::new(),
        "Type exactly".to_string(),
        action.expected_confirmation.clone(),
        kv_wide("input", &app.dangerous_confirmation),
        String::new(),
        "Replay with tradectl".to_string(),
        action.tradectl_replay.clone(),
        String::new(),
        "Enter/Esc closes. This modal does not execute the command.".to_string(),
    ]);
    frame.render_widget(panel("Confirm", lines), modal);
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let text = if app.dangerous_action.is_some() {
        " DANGEROUS ACTION: type confirmation | Enter/Esc closes | no command is sent ".to_string()
    } else if app.command_palette_active {
        format!(
            " COMMAND :{} | Enter/Esc closes | Backspace deletes ",
            app.command_palette_input
        )
    } else if app.search_active {
        format!(
            " SEARCH /{} | Enter/Esc closes | Backspace deletes ",
            app.search_query
        )
    } else {
        match app.screen {
        Screen::Strategies | Screen::Orders | Screen::Events if app.replay => {
            " REPLAY: / search | no live commands | up/down or j/k select | q exits ".to_string()
        }
        Screen::Overview | Screen::Risk if app.replay => {
            " REPLAY: up/down or j/k select account | no live commands | q exits ".to_string()
        }
        Screen::Overview | Screen::Risk => {
            " READ ONLY: up/down or j/k select account | K global | A account kill | F account flatten | q exits ".to_string()
        }
        Screen::Strategies | Screen::Orders | Screen::Events => {
            " READ ONLY: / search | up/down or j/k select | commands externalized through tradectl | q exits ".to_string()
        }
        _ if app.replay => " REPLAY: no live commands | q exits ".to_string(),
        _ => {
            " READ ONLY: command execution is externalized through tradectl/command-gateway | q exits ".to_string()
        }
        }
    };
    frame.render_widget(
        Paragraph::new(text).style(Style::default().bg(Color::Black).fg(Color::Gray)),
        area,
    );
}

fn centered_rect(width_pct: u16, height_pct: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - height_pct) / 2),
            Constraint::Percentage(height_pct),
            Constraint::Percentage((100 - height_pct) / 2),
        ])
        .split(area);
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - width_pct) / 2),
            Constraint::Percentage(width_pct),
            Constraint::Percentage((100 - width_pct) / 2),
        ])
        .split(vertical[1]);
    horizontal[1]
}

fn panel(title: &'static str, lines: Vec<String>) -> Paragraph<'static> {
    let lines = lines.into_iter().map(Line::from).collect::<Vec<_>>();
    Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title(title))
        .wrap(Wrap { trim: false })
}

fn selected_chain<'a>(
    state: &'a AppState,
    query: &str,
    selected_index: usize,
) -> Option<&'a OrderChain> {
    state
        .orders
        .by_correlation_id
        .values()
        .filter(|chain| order_matches_search(chain, query))
        .nth(selected_index)
}

fn selected_strategy<'a>(
    state: &'a AppState,
    query: &str,
    selected_index: usize,
) -> Option<&'a StrategyView> {
    state
        .strategies
        .by_id
        .values()
        .filter(|strategy| strategy_matches_search(strategy, query))
        .nth(selected_index)
}

fn selected_event<'a>(
    state: &'a AppState,
    query: &str,
    selected_index: usize,
) -> Option<&'a EventSummary> {
    state
        .audit
        .events
        .iter()
        .rev()
        .take(200)
        .filter(|event| event_matches_search(event, query))
        .nth(selected_index)
}

fn selected_account(state: &AppState, selected_index: usize) -> &AccountView {
    state
        .accounts
        .selected_or_first(selected_index)
        .unwrap_or(&state.account)
}

fn selected_account_id(app: &App) -> String {
    selected_account(&app.state, app.selected_account_index)
        .account_id
        .clone()
}

fn format_account_row(account: &AccountView, selected: bool) -> String {
    format!(
        "{} {:<14} {:<6} {:<8} {:>+9.2} {:>5.1}% {}",
        if selected { ">" } else { " " },
        truncate(&account.account_id, 14),
        truncate(&account.mode, 6),
        truncate(&account.broker, 8),
        account.day_pnl,
        account.exposure_pct,
        runtime_flags(account),
    )
}

fn format_strategy_row(strategy: &StrategyView, selected: bool) -> String {
    format!(
        "{} {:<18} {:<7} {:<5} {:>5} {:>5} {:>5}",
        if selected { ">" } else { " " },
        truncate(&strategy.strategy_id, 18),
        truncate(&strategy.state, 7),
        truncate(&strategy.mode, 5),
        strategy.signals,
        strategy.intents,
        strategy.orders,
    )
}

fn strategy_detail_lines(strategy: &StrategyView) -> Vec<String> {
    let mut lines = vec![
        kv_wide("strategy", &strategy.strategy_id),
        kv_wide("state", &strategy.state),
        kv_wide("mode", &strategy.mode),
        kv_wide("enabled", &strategy.enabled.to_string()),
        kv_wide("window", strategy.trading_window.as_deref().unwrap_or("-")),
        kv_wide("phase", &strategy.current_phase),
        kv_wide(
            "universe_version",
            strategy.universe_version.as_deref().unwrap_or("-"),
        ),
        kv_wide("universe", &strategy.universe_count.to_string()),
        kv_wide(
            "symbols active/watch/l2",
            &format!(
                "{}/{}/{}",
                strategy.active_symbol_count,
                strategy.watched_symbol_count,
                strategy.l2_allocated_symbol_count
            ),
        ),
        kv_wide("pnl", &format!("{:+.2}", strategy.pnl)),
        kv_wide(
            "rates sig/rej/fill/cxl",
            &format!(
                "{:.1}/{:.1}/{:.1}/{:.1}",
                strategy.signal_rate_1m,
                strategy.reject_rate_1m,
                strategy.fill_rate_1m,
                strategy.cancel_rate_1m
            ),
        ),
        kv_wide(
            "latency i2s/s2a/a2f",
            &format!(
                "{}/{}/{}",
                format_optional_u64(strategy.avg_intent_to_submit_ms),
                format_optional_u64(strategy.avg_submit_to_ack_ms),
                format_optional_u64(strategy.avg_ack_to_fill_ms)
            ),
        ),
        kv_wide(
            "trades today/max",
            &format!("{}/{}", strategy.trades_today, strategy.max_trades_today),
        ),
        kv_wide("stops", &strategy.consecutive_stops.to_string()),
        kv_wide(
            "loss_budget_used",
            &format!("{:.1}%", strategy.daily_loss_used_pct),
        ),
        kv_wide(
            "heartbeat_lag_ms",
            &strategy
                .heartbeat_lag_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
        ),
        kv_wide(
            "last_signal_seq",
            &strategy
                .last_signal_sequence
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
        ),
        kv_wide(
            "last_intent_seq",
            &strategy
                .last_intent_sequence
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
        ),
        kv_wide(
            "last_order_seq",
            &strategy
                .last_order_sequence
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
        ),
        kv_wide(
            "last_reason",
            strategy.last_reason.as_deref().unwrap_or("-"),
        ),
        String::new(),
        "Risk gates".to_string(),
    ];

    if strategy.risk_gates.is_empty() {
        lines.push("no risk gate projection".to_string());
    } else {
        for gate in &strategy.risk_gates {
            lines.push(format!(
                "{} {:<18} {}",
                mark(gate.passed),
                truncate(&gate.name, 18),
                gate.detail
            ));
        }
    }

    lines.push(String::new());
    lines.push("Parameters".to_string());
    if strategy.parameters.is_empty() {
        lines.push("no parameter projection".to_string());
    } else {
        for (key, value) in &strategy.parameters {
            lines.push(kv_wide(key, value));
        }
    }

    lines
}

fn format_order_row(chain: &OrderChain, selected: bool) -> String {
    let fill_remaining = format!(
        "{}/{}",
        chain.filled_quantity,
        chain
            .remaining_quantity
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    format!(
        "{} {:<10} {:<7} {:<8} {:<5} {:<4} {:>9} {:<6} {:<10} {:>7}",
        if selected { ">" } else { " " },
        truncate(&chain.correlation_id, 10),
        truncate(&format!("{:?}", chain.state), 7),
        truncate(chain.account_id.as_deref().unwrap_or("-"), 8),
        truncate(chain.side.as_deref().unwrap_or("-"), 5),
        truncate(chain.symbol.as_deref().unwrap_or("-"), 4),
        fill_remaining,
        truncate(chain.order_type.as_deref().unwrap_or("-"), 6),
        truncate(chain.broker_status.as_deref().unwrap_or("-"), 10),
        chain
            .latency
            .submit_to_ack_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
    )
}

fn format_event_row(event: &EventSummary, selected: bool) -> String {
    format!(
        "{}#{:<4} {:<17} {:<7} {}",
        if selected { ">" } else { " " },
        event.sequence,
        truncate(&event.event_type, 17),
        truncate(&event.aggregate_type, 7),
        truncate(&event.headline, 13)
    )
}

fn strategy_matches_search(strategy: &StrategyView, query: &str) -> bool {
    query_matches(
        query,
        [
            strategy.strategy_id.as_str(),
            strategy.state.as_str(),
            strategy.mode.as_str(),
            strategy.last_reason.as_deref().unwrap_or_default(),
        ],
    )
}

fn order_matches_search(chain: &OrderChain, query: &str) -> bool {
    query_matches(
        query,
        [
            chain.correlation_id.as_str(),
            chain.strategy_id.as_deref().unwrap_or_default(),
            chain.account_id.as_deref().unwrap_or_default(),
            chain.symbol.as_deref().unwrap_or_default(),
            chain.order_id.as_deref().unwrap_or_default(),
            chain.broker_order_id.as_deref().unwrap_or_default(),
        ],
    )
}

fn event_matches_search(event: &EventSummary, query: &str) -> bool {
    query_matches(
        query,
        [
            event.event_type.as_str(),
            event.aggregate_type.as_str(),
            event.aggregate_id.as_str(),
            event.correlation_id.as_str(),
            event.producer.as_str(),
            event.headline.as_str(),
        ],
    )
}

fn query_matches<'a>(query: &str, values: impl IntoIterator<Item = &'a str>) -> bool {
    let query = query.trim();
    if query.is_empty() {
        return true;
    }
    let query = query.to_ascii_lowercase();
    values
        .into_iter()
        .any(|value| value.to_ascii_lowercase().contains(&query))
}

fn mark(ok: bool) -> &'static str {
    if ok {
        "[x]"
    } else {
        "[ ]"
    }
}

fn runtime_flags(account: &AccountView) -> String {
    let mut flags = Vec::new();
    if account.runtime_controls.entry_disabled {
        flags.push("entry_off");
    }
    if account.runtime_controls.reduce_only {
        flags.push("reduce");
    }
    if account.runtime_controls.flatten_only {
        flags.push("flatten");
    }
    if account.runtime_controls.cancel_all {
        flags.push("cancel_all");
    }
    if flags.is_empty() {
        "-".to_string()
    } else {
        flags.join(",")
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

fn kv_narrow(label: &str, value: &str) -> String {
    format!("{:<6} {}", truncate(label, 6), truncate(value, 21))
}

fn format_optional_price(price: Option<&trade_core::Price>) -> String {
    price
        .map(ToString::to_string)
        .unwrap_or_else(|| "-".to_string())
}

fn format_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}
