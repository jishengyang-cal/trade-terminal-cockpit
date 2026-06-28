use crate::app::{App, Screen};
use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

pub fn handle_key(app: &mut App, key: KeyEvent) {
    if key.kind == KeyEventKind::Release {
        return;
    }

    if matches!(key.code, KeyCode::Char('c')) && key.modifiers.contains(KeyModifiers::CONTROL) {
        app.should_quit = true;
        return;
    }

    if app.dangerous_action.is_some() {
        match key.code {
            KeyCode::Esc | KeyCode::Enter => app.close_dangerous_modal(),
            KeyCode::Backspace => app.pop_dangerous_confirmation_char(),
            KeyCode::Char(ch) => app.push_dangerous_confirmation_char(ch),
            _ => {}
        }
        return;
    }

    if app.command_palette_active {
        match key.code {
            KeyCode::Esc | KeyCode::Enter => app.close_command_palette(),
            KeyCode::Backspace => app.pop_command_palette_char(),
            KeyCode::Char(ch) => app.push_command_palette_char(ch),
            _ => {}
        }
        return;
    }

    if app.search_active {
        match key.code {
            KeyCode::Esc | KeyCode::Enter => app.close_search(),
            KeyCode::Backspace => app.pop_search_char(),
            KeyCode::Char(ch) => app.push_search_char(ch),
            _ => {}
        }
        return;
    }

    match key.code {
        KeyCode::Char(':') => app.begin_command_palette(),
        KeyCode::Char('/') => app.begin_search(),
        KeyCode::Char('q') | KeyCode::Esc => {
            app.should_quit = true;
        }
        KeyCode::Tab => app.next_screen(),
        KeyCode::BackTab => app.previous_screen(),
        KeyCode::Down | KeyCode::Char('j') => app.select_next(),
        KeyCode::Up | KeyCode::Char('k') => app.select_previous(),
        KeyCode::F(key) => {
            if key == 10 {
                app.should_quit = true;
            } else if let Some(screen) = Screen::from_function_key(key) {
                app.screen = screen;
            }
        }
        KeyCode::Char('K') if app.screen == Screen::Risk => app.open_global_kill_modal(),
        KeyCode::Char('A') if app.screen == Screen::Risk => app.open_account_kill_modal(),
        KeyCode::Char('F') if app.screen == Screen::Risk => app.open_flatten_modal(),
        _ => {}
    }
}
