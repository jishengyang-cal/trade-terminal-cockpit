use crate::cli::Cli;
use crate::command_client::CommandClient;
use crate::{input, render};
use anyhow::Result;
use crossterm::event;
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use std::io;
use std::io::Write;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};
use trade_core::state::{EventSummary, OrderChain, StrategyView};
use trade_core::{reduce_event, AppState, CommandPayload, EventEnvelope};

pub fn run(
    state: AppState,
    cli: Cli,
    filter_summary: Option<String>,
    event_rx: Option<Receiver<EventEnvelope>>,
) -> Result<()> {
    let mut app = App::new(state, cli, filter_summary, event_rx);
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = app.run_loop(&mut terminal);

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

pub struct App {
    pub state: AppState,
    pub screen: Screen,
    pub replay: bool,
    pub replay_from: Option<String>,
    pub replay_to: Option<String>,
    pub filter_summary: Option<String>,
    pub search_active: bool,
    pub search_query: String,
    pub command_palette_active: bool,
    pub command_palette_input: String,
    pub dangerous_action: Option<PendingCommandAction>,
    pub dangerous_confirmation: String,
    pub last_command_message: Option<String>,
    pub command_client: CommandClient,
    pub selected_account_index: usize,
    pub selected_strategy_index: usize,
    pub selected_order_index: usize,
    pub selected_event_index: usize,
    pub should_quit: bool,
    event_rx: Option<Receiver<EventEnvelope>>,
}

impl App {
    pub fn new(
        state: AppState,
        cli: Cli,
        filter_summary: Option<String>,
        event_rx: Option<Receiver<EventEnvelope>>,
    ) -> Self {
        let command_client = CommandClient::from_cli(&cli);
        Self {
            state,
            screen: if cli.replay {
                Screen::Replay
            } else {
                Screen::Overview
            },
            replay: cli.replay,
            replay_from: cli.from,
            replay_to: cli.to,
            filter_summary,
            search_active: false,
            search_query: String::new(),
            command_palette_active: false,
            command_palette_input: String::new(),
            dangerous_action: None,
            dangerous_confirmation: String::new(),
            last_command_message: None,
            command_client,
            selected_account_index: 0,
            selected_strategy_index: 0,
            selected_order_index: 0,
            selected_event_index: 0,
            should_quit: false,
            event_rx,
        }
    }

    fn run_loop(&mut self, terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> Result<()> {
        let fps = self.state.connection.render_fps.max(1) as u64;
        let tick_rate = Duration::from_millis((1000 / fps).max(10));

        while !self.should_quit {
            self.drain_events();
            let render_started = Instant::now();
            terminal.draw(|frame| render::render(frame, self))?;
            let render_duration = render_started.elapsed();
            self.state.connection.last_render_duration_ms =
                render_duration.as_millis().min(u128::from(u64::MAX)) as u64;
            if render_duration > tick_rate {
                self.state.connection.render_slow_frames += 1;
            }
            if event::poll(tick_rate)? {
                if let event::Event::Key(key) = event::read()? {
                    input::handle_key(self, key);
                }
            }
        }

        Ok(())
    }

    fn drain_events(&mut self) {
        let mut drained = 0_u64;
        if let Some(rx) = &self.event_rx {
            while drained < self.state.connection.max_drain_per_tick {
                let Ok(event) = rx.try_recv() else {
                    break;
                };
                reduce_event(&mut self.state, event);
                drained += 1;
            }
            if drained == self.state.connection.max_drain_per_tick {
                self.state.connection.event_backlog =
                    self.state.connection.event_backlog.saturating_add(1);
                self.state.connection.event_rx_backlog_estimate =
                    Some(self.state.connection.event_backlog);
            } else {
                self.state.connection.event_rx_backlog_estimate = None;
            }
        }
        self.state.connection.events_drained_last_tick = drained;
    }

    pub fn next_screen(&mut self) {
        self.screen = self.screen.next();
    }

    pub fn previous_screen(&mut self) {
        self.screen = self.screen.previous();
    }

    pub fn select_next(&mut self) {
        match self.screen {
            Screen::Overview | Screen::Risk => {
                let len = self.visible_account_count();
                self.selected_account_index = next_index(self.selected_account_index, len);
            }
            Screen::Strategies => {
                let len = self.visible_strategy_count();
                self.selected_strategy_index = next_index(self.selected_strategy_index, len);
            }
            Screen::Orders => {
                let len = self.visible_order_count();
                self.selected_order_index = next_index(self.selected_order_index, len);
            }
            Screen::Events => {
                let len = self.visible_event_count();
                self.selected_event_index = next_index(self.selected_event_index, len);
            }
            _ => {}
        }
    }

    pub fn select_previous(&mut self) {
        match self.screen {
            Screen::Overview | Screen::Risk => {
                self.selected_account_index = self.selected_account_index.saturating_sub(1);
            }
            Screen::Strategies => {
                self.selected_strategy_index = self.selected_strategy_index.saturating_sub(1);
            }
            Screen::Orders => {
                self.selected_order_index = self.selected_order_index.saturating_sub(1);
            }
            Screen::Events => {
                self.selected_event_index = self.selected_event_index.saturating_sub(1);
            }
            _ => {}
        }
    }

    pub fn begin_search(&mut self) {
        self.search_active = true;
        self.search_query.clear();
        self.reset_selection();
    }

    pub fn close_search(&mut self) {
        self.search_active = false;
    }

    pub fn push_search_char(&mut self, ch: char) {
        if !ch.is_control() {
            self.search_query.push(ch);
            self.reset_selection();
        }
    }

    pub fn pop_search_char(&mut self) {
        self.search_query.pop();
        self.reset_selection();
    }

    pub fn begin_command_palette(&mut self) {
        self.command_palette_active = true;
        self.command_palette_input.clear();
    }

    pub fn close_command_palette(&mut self) {
        self.command_palette_active = false;
    }

    pub fn submit_command_palette(&mut self) {
        let input = self.command_palette_input.trim().to_string();
        if input.is_empty() {
            self.close_command_palette();
            return;
        }
        match self.parse_command_palette(&input) {
            Ok(action) => {
                self.command_palette_active = false;
                self.open_command_modal(action);
            }
            Err(message) => {
                self.last_command_message = Some(message);
            }
        }
    }

    pub fn push_command_palette_char(&mut self, ch: char) {
        if !ch.is_control() {
            self.command_palette_input.push(ch);
        }
    }

    pub fn pop_command_palette_char(&mut self) {
        self.command_palette_input.pop();
    }

    fn parse_command_palette(
        &self,
        input: &str,
    ) -> std::result::Result<PendingCommandAction, String> {
        let parts = input.split_whitespace().collect::<Vec<_>>();
        let Some(command) = parts.first().copied() else {
            return Err("empty command".to_string());
        };
        let reason = self.command_client.config().reason.clone();
        match command {
            "pause" | "pause-strategy" if parts.len() == 2 => Ok(strategy_action(
                "PAUSE STRATEGY",
                parts[1],
                format!("PAUSE {}", parts[1]),
                format!("tradectl pause-strategy {}", parts[1]),
                CommandPayload::PauseStrategyRequested {
                    strategy_id: parts[1].to_string(),
                },
                reason,
            )),
            "resume" | "resume-strategy" if parts.len() == 2 => Ok(strategy_action(
                "RESUME STRATEGY",
                parts[1],
                format!("RESUME {}", parts[1]),
                format!("tradectl resume-strategy {}", parts[1]),
                CommandPayload::ResumeStrategyRequested {
                    strategy_id: parts[1].to_string(),
                },
                reason,
            )),
            "drain" | "drain-strategy" if parts.len() == 2 => Ok(strategy_action(
                "DRAIN STRATEGY",
                parts[1],
                format!("DRAIN {}", parts[1]),
                format!("tradectl drain-strategy {}", parts[1]),
                CommandPayload::DrainStrategyRequested {
                    strategy_id: parts[1].to_string(),
                },
                reason,
            )),
            "kill-strategy" if parts.len() == 2 => Ok(strategy_action(
                "KILL STRATEGY",
                parts[1],
                format!("KILL STRATEGY {}", parts[1]),
                format!(
                    "tradectl kill-strategy {} --confirm 'KILL STRATEGY {}'",
                    parts[1], parts[1]
                ),
                CommandPayload::KillStrategyRequested {
                    strategy_id: parts[1].to_string(),
                },
                reason,
            )),
            "cancel-order" if parts.len() == 3 => Ok(PendingCommandAction {
                action: "CANCEL ORDER".to_string(),
                target: format!("{}:{}", parts[1], parts[2]),
                effects: vec!["send CancelOrderRequested to command-gateway".to_string()],
                expected_confirmation: format!("CANCEL {} {}", parts[1], parts[2]),
                tradectl_replay: format!("tradectl cancel-order {} {}", parts[1], parts[2]),
                payload: CommandPayload::CancelOrderRequested {
                    account_id: parts[1].to_string(),
                    order_id: parts[2].to_string(),
                },
                capability: "order.cancel".to_string(),
                reason,
            }),
            "cancel-all" | "cancel-all-orders-for-symbol" if parts.len() == 3 => {
                Ok(PendingCommandAction {
                    action: "CANCEL ALL FOR SYMBOL".to_string(),
                    target: format!("{}:{}", parts[1], parts[2]),
                    effects: vec![
                        "send CancelAllOrdersForSymbolRequested to command-gateway".to_string()
                    ],
                    expected_confirmation: format!("CANCEL ALL {} {}", parts[1], parts[2]),
                    tradectl_replay: format!(
                        "tradectl cancel-all-orders-for-symbol {} {} --confirm 'CANCEL ALL {} {}'",
                        parts[1], parts[2], parts[1], parts[2]
                    ),
                    payload: CommandPayload::CancelAllOrdersForSymbolRequested {
                        account_id: parts[1].to_string(),
                        symbol: parts[2].to_string(),
                    },
                    capability: "order.cancel".to_string(),
                    reason,
                })
            }
            "flatten-account" if parts.len() == 2 => Ok(PendingCommandAction {
                action: "FLATTEN ACCOUNT".to_string(),
                target: parts[1].to_string(),
                effects: vec!["send FlattenAccountRequested to command-gateway".to_string()],
                expected_confirmation: format!("FLATTEN ACCOUNT {}", parts[1]),
                tradectl_replay: format!(
                    "tradectl flatten-account {} --confirm 'FLATTEN ACCOUNT {}'",
                    parts[1], parts[1]
                ),
                payload: CommandPayload::FlattenAccountRequested {
                    account_id: parts[1].to_string(),
                },
                capability: "account.flatten".to_string(),
                reason,
            }),
            "account-kill" | "account-kill-switch" if parts.len() == 2 => {
                Ok(PendingCommandAction {
                    action: "ACCOUNT KILL SWITCH".to_string(),
                    target: parts[1].to_string(),
                    effects: vec!["send AccountKillSwitchRequested to command-gateway".to_string()],
                    expected_confirmation: format!("KILL ACCOUNT {}", parts[1]),
                    tradectl_replay: format!(
                        "tradectl account-kill-switch {} --confirm 'KILL ACCOUNT {}'",
                        parts[1], parts[1]
                    ),
                    payload: CommandPayload::AccountKillSwitchRequested {
                        account_id: parts[1].to_string(),
                    },
                    capability: "account.kill".to_string(),
                    reason,
                })
            }
            "global-kill" | "global-kill-switch" if parts.len() == 2 => Ok(PendingCommandAction {
                action: "GLOBAL KILL SWITCH".to_string(),
                target: parts[1].to_string(),
                effects: vec!["send GlobalKillSwitchRequested to command-gateway".to_string()],
                expected_confirmation: format!("KILL {}", parts[1]),
                tradectl_replay: format!(
                    "tradectl global-kill-switch {} --confirm 'KILL {}'",
                    parts[1], parts[1]
                ),
                payload: CommandPayload::GlobalKillSwitchRequested {
                    account_id: parts[1].to_string(),
                },
                capability: "account.kill".to_string(),
                reason,
            }),
            "ack-alert" if parts.len() == 2 => Ok(PendingCommandAction {
                action: "ACK ALERT".to_string(),
                target: parts[1].to_string(),
                effects: vec!["send AcknowledgeAlertRequested to command-gateway".to_string()],
                expected_confirmation: format!("ACK {}", parts[1]),
                tradectl_replay: format!("tradectl ack-alert {}", parts[1]),
                payload: CommandPayload::AcknowledgeAlertRequested {
                    alert_id: parts[1].to_string(),
                },
                capability: "alert.ack".to_string(),
                reason,
            }),
            _ => Err(format!("unknown command palette input: {input}")),
        }
    }

    pub fn open_global_kill_modal(&mut self) {
        self.open_command_modal(PendingCommandAction {
            action: "GLOBAL KILL SWITCH".to_string(),
            target: "global".to_string(),
            effects: vec![
                "set broker global_kill runtime control".to_string(),
                "blocks all account entry through global circuit".to_string(),
                "must be replayed through command-gateway audit".to_string(),
            ],
            expected_confirmation: "KILL global".to_string(),
            tradectl_replay: "tradectl global-kill-switch global --confirm 'KILL global'"
                .to_string(),
            payload: CommandPayload::GlobalKillSwitchRequested {
                account_id: "global".to_string(),
            },
            capability: "account.kill".to_string(),
            reason: self.command_client.config().reason.clone(),
        });
    }

    pub fn open_account_kill_modal(&mut self) {
        let account_id = self.selected_account_id();
        self.open_command_modal(PendingCommandAction {
            action: "ACCOUNT KILL SWITCH".to_string(),
            target: account_id.clone(),
            effects: vec![
                "set account-scoped cancel_all runtime control".to_string(),
                "blocks new entry for this account".to_string(),
                "requires --broker-account-slot account=slot at gateway".to_string(),
            ],
            expected_confirmation: format!("KILL ACCOUNT {account_id}"),
            tradectl_replay: format!(
                "tradectl account-kill-switch {account_id} --confirm 'KILL ACCOUNT {account_id}'"
            ),
            payload: CommandPayload::AccountKillSwitchRequested {
                account_id: account_id.clone(),
            },
            capability: "account.kill".to_string(),
            reason: self.command_client.config().reason.clone(),
        });
    }

    pub fn open_flatten_modal(&mut self) {
        let account_id = self.selected_account_id();
        self.open_command_modal(PendingCommandAction {
            action: "FLATTEN ACCOUNT".to_string(),
            target: account_id.clone(),
            effects: vec![
                "set account-scoped flatten_only runtime control".to_string(),
                "only flattening intents remain admissible".to_string(),
                "requires --broker-account-slot account=slot at gateway".to_string(),
            ],
            expected_confirmation: format!("FLATTEN ACCOUNT {account_id}"),
            tradectl_replay: format!(
                "tradectl flatten-account {account_id} --confirm 'FLATTEN ACCOUNT {account_id}'"
            ),
            payload: CommandPayload::FlattenAccountRequested {
                account_id: account_id.clone(),
            },
            capability: "account.flatten".to_string(),
            reason: self.command_client.config().reason.clone(),
        });
    }

    pub fn open_strategy_pause_modal(&mut self) {
        if let Some(strategy_id) = self.selected_strategy_id() {
            self.open_command_modal(PendingCommandAction {
                action: "PAUSE STRATEGY".to_string(),
                target: strategy_id.clone(),
                effects: vec![
                    "send PauseStrategyRequested to command-gateway".to_string(),
                    "state changes only after authority/audit events are reduced".to_string(),
                ],
                expected_confirmation: format!("PAUSE {strategy_id}"),
                tradectl_replay: format!("tradectl pause-strategy {strategy_id}"),
                payload: CommandPayload::PauseStrategyRequested {
                    strategy_id: strategy_id.clone(),
                },
                capability: "strategy.control".to_string(),
                reason: self.command_client.config().reason.clone(),
            });
        } else {
            self.last_command_message = Some("no selected strategy".to_string());
        }
    }

    pub fn open_strategy_resume_modal(&mut self) {
        if let Some(strategy_id) = self.selected_strategy_id() {
            self.open_command_modal(PendingCommandAction {
                action: "RESUME STRATEGY".to_string(),
                target: strategy_id.clone(),
                effects: vec![
                    "send ResumeStrategyRequested to command-gateway".to_string(),
                    "strategy state updates only from the event stream/audit result".to_string(),
                ],
                expected_confirmation: format!("RESUME {strategy_id}"),
                tradectl_replay: format!("tradectl resume-strategy {strategy_id}"),
                payload: CommandPayload::ResumeStrategyRequested {
                    strategy_id: strategy_id.clone(),
                },
                capability: "strategy.control".to_string(),
                reason: self.command_client.config().reason.clone(),
            });
        } else {
            self.last_command_message = Some("no selected strategy".to_string());
        }
    }

    pub fn open_strategy_drain_modal(&mut self) {
        if let Some(strategy_id) = self.selected_strategy_id() {
            self.open_command_modal(PendingCommandAction {
                action: "DRAIN STRATEGY".to_string(),
                target: strategy_id.clone(),
                effects: vec![
                    "send DrainStrategyRequested to command-gateway".to_string(),
                    "no direct strategy runtime call is made by trade-tui".to_string(),
                ],
                expected_confirmation: format!("DRAIN {strategy_id}"),
                tradectl_replay: format!("tradectl drain-strategy {strategy_id}"),
                payload: CommandPayload::DrainStrategyRequested {
                    strategy_id: strategy_id.clone(),
                },
                capability: "strategy.control".to_string(),
                reason: self.command_client.config().reason.clone(),
            });
        } else {
            self.last_command_message = Some("no selected strategy".to_string());
        }
    }

    pub fn open_strategy_kill_modal(&mut self) {
        if let Some(strategy_id) = self.selected_strategy_id() {
            self.open_command_modal(PendingCommandAction {
                action: "KILL STRATEGY".to_string(),
                target: strategy_id.clone(),
                effects: vec![
                    "send KillStrategyRequested to command-gateway".to_string(),
                    "requires gateway dangerous-command policy to accept it".to_string(),
                ],
                expected_confirmation: format!("KILL STRATEGY {strategy_id}"),
                tradectl_replay: format!(
                    "tradectl kill-strategy {strategy_id} --confirm 'KILL STRATEGY {strategy_id}'"
                ),
                payload: CommandPayload::KillStrategyRequested {
                    strategy_id: strategy_id.clone(),
                },
                capability: "strategy.control".to_string(),
                reason: self.command_client.config().reason.clone(),
            });
        } else {
            self.last_command_message = Some("no selected strategy".to_string());
        }
    }

    pub fn open_cancel_order_modal(&mut self) {
        let Some((account_id, order_id)) = self.selected_order_account_and_id() else {
            self.last_command_message =
                Some("selected order is missing account_id/order_id".to_string());
            return;
        };
        self.open_command_modal(PendingCommandAction {
            action: "CANCEL ORDER".to_string(),
            target: format!("{account_id}:{order_id}"),
            effects: vec![
                "send CancelOrderRequested to command-gateway".to_string(),
                "order cancellation result must arrive as order lifecycle events".to_string(),
            ],
            expected_confirmation: format!("CANCEL {account_id} {order_id}"),
            tradectl_replay: format!("tradectl cancel-order {account_id} {order_id}"),
            payload: CommandPayload::CancelOrderRequested {
                account_id,
                order_id,
            },
            capability: "order.cancel".to_string(),
            reason: self.command_client.config().reason.clone(),
        });
    }

    pub fn open_cancel_all_for_symbol_modal(&mut self) {
        let Some((account_id, symbol)) = self.selected_order_account_and_symbol() else {
            self.last_command_message =
                Some("selected order is missing account_id/symbol".to_string());
            return;
        };
        self.open_command_modal(PendingCommandAction {
            action: "CANCEL ALL FOR SYMBOL".to_string(),
            target: format!("{account_id}:{symbol}"),
            effects: vec![
                "send CancelAllOrdersForSymbolRequested to command-gateway".to_string(),
                "gateway must refuse unsupported scope widening".to_string(),
            ],
            expected_confirmation: format!("CANCEL ALL {account_id} {symbol}"),
            tradectl_replay: format!(
                "tradectl cancel-all-orders-for-symbol {account_id} {symbol} --confirm 'CANCEL ALL {account_id} {symbol}'"
            ),
            payload: CommandPayload::CancelAllOrdersForSymbolRequested {
                account_id,
                symbol,
            },
            capability: "order.cancel".to_string(),
            reason: self.command_client.config().reason.clone(),
        });
    }

    fn open_command_modal(&mut self, action: PendingCommandAction) {
        self.dangerous_action = Some(action);
        self.dangerous_confirmation.clear();
        self.last_command_message = None;
    }

    pub fn close_dangerous_modal(&mut self) {
        self.dangerous_action = None;
        self.dangerous_confirmation.clear();
    }

    pub fn push_dangerous_confirmation_char(&mut self, ch: char) {
        if !ch.is_control() {
            self.dangerous_confirmation.push(ch);
        }
    }

    pub fn pop_dangerous_confirmation_char(&mut self) {
        self.dangerous_confirmation.pop();
    }

    pub fn submit_pending_command(&mut self) {
        let Some(action) = self.dangerous_action.clone() else {
            return;
        };
        if self.replay {
            self.last_command_message =
                Some("replay mode blocks live command submission".to_string());
            return;
        }
        if self.dangerous_confirmation != action.expected_confirmation {
            self.last_command_message = Some(format!(
                "confirmation mismatch; type exactly: {}",
                action.expected_confirmation
            ));
            return;
        }

        match self.command_client.submit(
            action.payload,
            &action.capability,
            &action.reason,
            &action.expected_confirmation,
        ) {
            Ok(submission) => {
                let event_count = submission.events.len();
                for event in submission.events {
                    reduce_event(&mut self.state, event);
                }
                self.last_command_message = Some(format!(
                    "command {} {} ({} gateway events)",
                    submission.command_id, submission.status, event_count
                ));
                self.close_dangerous_modal();
            }
            Err(error) => {
                self.last_command_message = Some(format!("command failed: {error}"));
            }
        }
    }

    pub fn copy_selected_event_correlation(&mut self) {
        let Some(event) = self.selected_event_summary() else {
            self.last_command_message = Some("no selected event".to_string());
            return;
        };
        let value = event.correlation_id.clone();
        self.copy_text("correlation_id", &value);
    }

    pub fn copy_selected_event_payload(&mut self) {
        let Some(event) = self.selected_event_summary() else {
            self.last_command_message = Some("no selected event".to_string());
            return;
        };
        let value = event
            .payload_json
            .clone()
            .unwrap_or_else(|| event.headline.clone());
        self.copy_text("payload", &value);
    }

    pub fn open_selected_event_order_chain(&mut self) {
        let Some(event) = self.selected_event_summary() else {
            self.last_command_message = Some("no selected event".to_string());
            return;
        };
        self.search_query = event.correlation_id.clone();
        self.selected_order_index = 0;
        self.screen = Screen::Orders;
    }

    pub fn open_selected_event_strategy(&mut self) {
        let Some(event) = self.selected_event_summary() else {
            self.last_command_message = Some("no selected event".to_string());
            return;
        };
        self.search_query = if event.aggregate_type == "strategy" {
            event.aggregate_id.clone()
        } else {
            event
                .headline
                .split_whitespace()
                .next()
                .unwrap_or(event.aggregate_id.as_str())
                .to_string()
        };
        self.selected_strategy_index = 0;
        self.screen = Screen::Strategies;
    }

    fn copy_text(&mut self, label: &str, value: &str) {
        match write_osc52_clipboard(value) {
            Ok(()) => {
                self.last_command_message = Some(format!("copied {label} to terminal clipboard"));
            }
            Err(error) => {
                self.last_command_message = Some(format!("failed to copy {label}: {error}"));
            }
        }
    }

    fn reset_selection(&mut self) {
        self.selected_account_index = 0;
        self.selected_strategy_index = 0;
        self.selected_order_index = 0;
        self.selected_event_index = 0;
    }

    fn selected_account_id(&self) -> String {
        self.state
            .accounts
            .selected_account_id(self.selected_account_index)
    }

    fn visible_account_count(&self) -> usize {
        self.state.accounts.len()
    }

    fn visible_strategy_count(&self) -> usize {
        self.state
            .strategies
            .by_id
            .values()
            .filter(|strategy| strategy_matches_search(strategy, &self.search_query))
            .count()
    }

    fn visible_order_count(&self) -> usize {
        self.state
            .orders
            .by_correlation_id
            .values()
            .filter(|chain| order_matches_search(chain, &self.search_query))
            .count()
    }

    fn visible_event_count(&self) -> usize {
        self.state
            .audit
            .events
            .iter()
            .rev()
            .take(200)
            .filter(|event| event_matches_search(event, &self.search_query))
            .count()
    }

    fn selected_strategy_id(&self) -> Option<String> {
        self.state
            .strategies
            .by_id
            .values()
            .filter(|strategy| strategy_matches_search(strategy, &self.search_query))
            .nth(self.selected_strategy_index)
            .map(|strategy| strategy.strategy_id.clone())
    }

    fn selected_order_chain(&self) -> Option<&OrderChain> {
        self.state
            .orders
            .by_correlation_id
            .values()
            .filter(|chain| order_matches_search(chain, &self.search_query))
            .nth(self.selected_order_index)
    }

    fn selected_order_account_and_id(&self) -> Option<(String, String)> {
        let chain = self.selected_order_chain()?;
        Some((chain.account_id.clone()?, chain.order_id.clone()?))
    }

    fn selected_order_account_and_symbol(&self) -> Option<(String, String)> {
        let chain = self.selected_order_chain()?;
        Some((chain.account_id.clone()?, chain.symbol.clone()?))
    }

    fn selected_event_summary(&self) -> Option<EventSummary> {
        self.state
            .audit
            .events
            .iter()
            .rev()
            .take(200)
            .filter(|event| event_matches_search(event, &self.search_query))
            .nth(self.selected_event_index)
            .cloned()
    }
}

fn next_index(current: usize, len: usize) -> usize {
    if len == 0 {
        0
    } else {
        (current + 1).min(len - 1)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PendingCommandAction {
    pub action: String,
    pub target: String,
    pub effects: Vec<String>,
    pub expected_confirmation: String,
    pub tradectl_replay: String,
    pub payload: CommandPayload,
    pub capability: String,
    pub reason: String,
}

fn strategy_action(
    action: &str,
    strategy_id: &str,
    expected_confirmation: String,
    tradectl_replay: String,
    payload: CommandPayload,
    reason: String,
) -> PendingCommandAction {
    PendingCommandAction {
        action: action.to_string(),
        target: strategy_id.to_string(),
        effects: vec![format!(
            "send {} to command-gateway",
            payload.command_type()
        )],
        expected_confirmation,
        tradectl_replay,
        payload,
        capability: "strategy.control".to_string(),
        reason,
    }
}

fn write_osc52_clipboard(value: &str) -> io::Result<()> {
    let encoded = base64_encode(value.as_bytes());
    let mut stdout = io::stdout();
    write!(stdout, "\x1b]52;c;{encoded}\x07")?;
    stdout.flush()
}

fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        output.push(TABLE[(b0 >> 2) as usize] as char);
        output.push(TABLE[(((b0 & 0b0000_0011) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            output.push(TABLE[(((b1 & 0b0000_1111) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            output.push('=');
        }
        if chunk.len() > 2 {
            output.push(TABLE[(b2 & 0b0011_1111) as usize] as char);
        } else {
            output.push('=');
        }
    }
    output
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Screen {
    Help,
    Overview,
    Strategies,
    Orders,
    Positions,
    Risk,
    Events,
    Replay,
    Commands,
}

impl Screen {
    pub const ALL: [Self; 9] = [
        Self::Help,
        Self::Overview,
        Self::Strategies,
        Self::Orders,
        Self::Positions,
        Self::Risk,
        Self::Events,
        Self::Replay,
        Self::Commands,
    ];

    pub fn title(self) -> &'static str {
        match self {
            Self::Help => "F1 Help",
            Self::Overview => "F2 Overview",
            Self::Strategies => "F3 Strategies",
            Self::Orders => "F4 Orders",
            Self::Positions => "F5 Positions",
            Self::Risk => "F6 Risk",
            Self::Events => "F7 Events",
            Self::Replay => "F8 Replay",
            Self::Commands => "F9 Commands",
        }
    }

    pub fn index(self) -> usize {
        Self::ALL
            .iter()
            .position(|screen| *screen == self)
            .unwrap_or_default()
    }

    pub fn next(self) -> Self {
        Self::ALL[(self.index() + 1) % Self::ALL.len()]
    }

    pub fn previous(self) -> Self {
        let index = self.index();
        Self::ALL[(index + Self::ALL.len() - 1) % Self::ALL.len()]
    }

    pub fn from_function_key(key: u8) -> Option<Self> {
        match key {
            1 => Some(Self::Help),
            2 => Some(Self::Overview),
            3 => Some(Self::Strategies),
            4 => Some(Self::Orders),
            5 => Some(Self::Positions),
            6 => Some(Self::Risk),
            7 => Some(Self::Events),
            8 => Some(Self::Replay),
            9 => Some(Self::Commands),
            _ => None,
        }
    }
}
