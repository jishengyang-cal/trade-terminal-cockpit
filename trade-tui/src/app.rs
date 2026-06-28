use crate::cli::Cli;
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
use std::sync::mpsc::Receiver;
use std::time::Duration;
use trade_core::state::{EventSummary, OrderChain, StrategyView};
use trade_core::{reduce_event, AppState, EventEnvelope};

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
    pub dangerous_action: Option<DangerousAction>,
    pub dangerous_confirmation: String,
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
            terminal.draw(|frame| render::render(frame, self))?;
            if event::poll(tick_rate)? {
                if let event::Event::Key(key) = event::read()? {
                    input::handle_key(self, key);
                }
            }
        }

        Ok(())
    }

    fn drain_events(&mut self) {
        if let Some(rx) = &self.event_rx {
            while let Ok(event) = rx.try_recv() {
                reduce_event(&mut self.state, event);
            }
        }
    }

    pub fn next_screen(&mut self) {
        self.screen = self.screen.next();
    }

    pub fn previous_screen(&mut self) {
        self.screen = self.screen.previous();
    }

    pub fn select_next(&mut self) {
        match self.screen {
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

    pub fn push_command_palette_char(&mut self, ch: char) {
        if !ch.is_control() {
            self.command_palette_input.push(ch);
        }
    }

    pub fn pop_command_palette_char(&mut self) {
        self.command_palette_input.pop();
    }

    pub fn open_global_kill_modal(&mut self) {
        let account_id = self.state.account.account_id.clone();
        self.dangerous_action = Some(DangerousAction {
            action: "GLOBAL KILL SWITCH".to_string(),
            target: account_id.clone(),
            effects: vec![
                "pause all strategies".to_string(),
                "cancel open orders through command-gateway".to_string(),
                "block new intents through risk authority".to_string(),
            ],
            expected_confirmation: format!("KILL {account_id}"),
            tradectl_replay: format!(
                "tradectl global-kill-switch {account_id} --confirm 'KILL {account_id}'"
            ),
        });
        self.dangerous_confirmation.clear();
    }

    pub fn open_flatten_modal(&mut self) {
        let account_id = self.state.account.account_id.clone();
        let symbol = self
            .state
            .positions
            .by_key
            .values()
            .next()
            .map(|position| position.symbol.clone())
            .unwrap_or_else(|| "<symbol>".to_string());
        self.dangerous_action = Some(DangerousAction {
            action: "FLATTEN SYMBOL".to_string(),
            target: format!("{account_id}:{symbol}"),
            effects: vec![
                "request flatten through command-gateway".to_string(),
                "risk authority must approve".to_string(),
                "order gateway owns broker execution".to_string(),
            ],
            expected_confirmation: format!("FLATTEN {account_id} {symbol}"),
            tradectl_replay: format!(
                "tradectl flatten-symbol {account_id} {symbol} --confirm 'FLATTEN {account_id} {symbol}'"
            ),
        });
        self.dangerous_confirmation.clear();
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

    fn reset_selection(&mut self) {
        self.selected_strategy_index = 0;
        self.selected_order_index = 0;
        self.selected_event_index = 0;
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
}

fn next_index(current: usize, len: usize) -> usize {
    if len == 0 {
        0
    } else {
        (current + 1).min(len - 1)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DangerousAction {
    pub action: String,
    pub target: String,
    pub effects: Vec<String>,
    pub expected_confirmation: String,
    pub tradectl_replay: String,
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
