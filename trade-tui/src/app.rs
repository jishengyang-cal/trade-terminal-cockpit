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
use trade_core::{reduce_event, AppState, EventEnvelope};

pub fn run(state: AppState, cli: Cli, event_rx: Option<Receiver<EventEnvelope>>) -> Result<()> {
    let mut app = App::new(state, cli, event_rx);
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
    pub should_quit: bool,
    event_rx: Option<Receiver<EventEnvelope>>,
}

impl App {
    pub fn new(state: AppState, cli: Cli, event_rx: Option<Receiver<EventEnvelope>>) -> Self {
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Screen {
    Overview,
    Strategies,
    Orders,
    Positions,
    Risk,
    Events,
    Replay,
}

impl Screen {
    pub const ALL: [Self; 7] = [
        Self::Overview,
        Self::Strategies,
        Self::Orders,
        Self::Positions,
        Self::Risk,
        Self::Events,
        Self::Replay,
    ];

    pub fn title(self) -> &'static str {
        match self {
            Self::Overview => "F2 Overview",
            Self::Strategies => "F3 Strategies",
            Self::Orders => "F4 Orders",
            Self::Positions => "F5 Positions",
            Self::Risk => "F6 Risk",
            Self::Events => "F7 Events",
            Self::Replay => "F8 Replay",
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
            2 => Some(Self::Overview),
            3 => Some(Self::Strategies),
            4 => Some(Self::Orders),
            5 => Some(Self::Positions),
            6 => Some(Self::Risk),
            7 => Some(Self::Events),
            8 => Some(Self::Replay),
            _ => None,
        }
    }
}
