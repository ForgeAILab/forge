//! Keyboard-to-reducer mapping for Forge Solo.
//!
//! The mapping is intentionally context-aware.  In particular, a letter typed
//! into the composer is never interpreted as an approval/review action; those
//! actions exist only while their typed modal is focused.

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

use crate::app::{
    AppInput, AppState, FocusTarget, PrimaryView, ProjectTab, RuntimeState, SetupState,
};

/// A small configurable keymap.  The defaults are documented by
/// [`Keymap::default`], while keeping the physical event translation in one
/// place makes alternate layouts and controller tests straightforward.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Keymap {
    pub quit: KeyChord,
    pub cancel: KeyChord,
    pub help: KeyChord,
    pub activity: KeyChord,
    pub retry: KeyChord,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyChord {
    pub code: KeyCode,
    pub modifiers: KeyModifiers,
}

impl KeyChord {
    pub const fn new(code: KeyCode, modifiers: KeyModifiers) -> Self {
        Self { code, modifiers }
    }

    fn matches(self, event: KeyEvent) -> bool {
        event.code == self.code && event.modifiers == self.modifiers
    }
}

impl Default for Keymap {
    fn default() -> Self {
        Self {
            quit: KeyChord::new(KeyCode::Char('q'), KeyModifiers::NONE),
            cancel: KeyChord::new(KeyCode::Esc, KeyModifiers::NONE),
            help: KeyChord::new(KeyCode::Char('?'), KeyModifiers::NONE),
            activity: KeyChord::new(KeyCode::Char('a'), KeyModifiers::NONE),
            retry: KeyChord::new(KeyCode::Char('r'), KeyModifiers::NONE),
        }
    }
}

impl Keymap {
    /// Translate one pressed key into a pure reducer input.
    pub fn map(self, state: &AppState, event: KeyEvent) -> Option<AppInput> {
        if event.kind != KeyEventKind::Press {
            return None;
        }

        let modifiers = event.modifiers;
        let code = event.code;

        // Interrupts are deliberately resolved before ordinary Ctrl-C input.
        // A live turn gets a typed cancellation card; a quiet UI begins the
        // graceful supervisor shutdown; a second interrupt forces restoration.
        if code == KeyCode::Char('c') && modifiers.contains(KeyModifiers::CONTROL) {
            return Some(
                if state.header.runtime == RuntimeState::ShuttingDown
                    || state.header.runtime == RuntimeState::ForcedShutdown
                {
                    AppInput::ForceQuit
                } else if state.live_turn_id().is_some() {
                    AppInput::RequestCancel
                } else {
                    AppInput::RequestQuit
                },
            );
        }

        if self.cancel.matches(event) {
            return state.modal.as_ref().map(|_| AppInput::Cancel);
        }

        // `?` is printable composer input. F1 remains a global help key,
        // while the printable help chord is reserved outside the composer.
        if code == KeyCode::F(1)
            || (self.help.matches(event) && state.focus != FocusTarget::Composer)
        {
            return Some(AppInput::ToggleHelp);
        }

        if state.modal.is_some() {
            return self.map_modal(code, modifiers);
        }

        // F2 is the dependable terminal-native view switch. Keep the more
        // explicit Ctrl shortcuts below for terminals that report them, but
        // do not make the primary navigation depend on control sequences that
        // some terminal emulators cannot distinguish from Esc or NUL.
        if code == KeyCode::F(2) {
            return Some(AppInput::NextPrimaryView);
        }

        if matches!(state.setup, SetupState::AgentPicker { .. }) {
            match code {
                KeyCode::Up => return Some(AppInput::SelectSetupPrevious),
                KeyCode::Down => return Some(AppInput::SelectSetupNext),
                KeyCode::Enter => return Some(AppInput::Confirm),
                KeyCode::Esc => return Some(AppInput::Cancel),
                _ => {}
            }
        }

        match (code, modifiers) {
            (KeyCode::Char('1'), mods) if mods.contains(KeyModifiers::CONTROL) => {
                return Some(AppInput::SelectPrimaryView(PrimaryView::Kanban));
            }
            (KeyCode::Char('2'), mods) if mods.contains(KeyModifiers::CONTROL) => {
                return Some(AppInput::SelectPrimaryView(PrimaryView::MainChat));
            }
            (KeyCode::Char('['), mods) if mods.contains(KeyModifiers::CONTROL) => {
                return Some(AppInput::PreviousPrimaryView);
            }
            (KeyCode::Char(']'), mods) if mods.contains(KeyModifiers::CONTROL) => {
                return Some(AppInput::NextPrimaryView);
            }
            _ => {}
        }

        // Ctrl-Q remains available while the composer owns focus; `q` itself
        // is reserved only outside the composer so normal prose is untouched.
        if (code == KeyCode::Char('q')
            && modifiers.is_empty()
            && state.focus != FocusTarget::Composer)
            || (code == KeyCode::Char('q') && modifiers == KeyModifiers::CONTROL)
            || self.quit.matches(event) && state.focus != FocusTarget::Composer
        {
            return Some(AppInput::RequestQuit);
        }

        if state.focus != FocusTarget::Composer
            && state.primary_view == PrimaryView::MainChat
            && self.activity.matches(event)
            && state.live_activity.is_some()
        {
            return Some(AppInput::ToggleActivity);
        }
        if state.focus != FocusTarget::Composer
            && self.retry.matches(event)
            && (state.retryable_turn.is_some()
                || matches!(
                    state.setup,
                    SetupState::Unavailable {
                        retryable: true,
                        ..
                    }
                ))
        {
            return Some(AppInput::Retry);
        }
        if state.primary_view == PrimaryView::Kanban && state.focus != FocusTarget::Composer {
            match (code, modifiers) {
                (KeyCode::Char('t'), mods) if mods.is_empty() => {
                    return Some(AppInput::SelectProjectTab(ProjectTab::Tasks));
                }
                (KeyCode::Char('a'), mods) if mods.is_empty() => {
                    return Some(AppInput::SelectProjectTab(ProjectTab::Attention));
                }
                (KeyCode::Char('v'), mods) if mods.is_empty() => {
                    return Some(AppInput::SelectProjectTab(ProjectTab::Approvals));
                }
                _ => {}
            }
        }

        match (code, modifiers) {
            (KeyCode::Tab, mods) if mods.contains(KeyModifiers::SHIFT) => {
                Some(AppInput::FocusPrevious)
            }
            (KeyCode::Tab, _) => Some(AppInput::FocusNext),
            (KeyCode::BackTab, _) => Some(AppInput::FocusPrevious),
            (KeyCode::Enter, mods) if mods.contains(KeyModifiers::SHIFT) => Some(AppInput::NewLine),
            (KeyCode::Enter, _) if state.focus == FocusTarget::ProjectRail => {
                Some(AppInput::OpenSelected)
            }
            (KeyCode::Enter, _) => Some(AppInput::Submit),
            (KeyCode::Backspace, _) => Some(AppInput::Backspace),
            (KeyCode::Delete, _) => Some(AppInput::Delete),
            (KeyCode::Left, _) => Some(AppInput::MoveLeft),
            (KeyCode::Right, _) => Some(AppInput::MoveRight),
            (KeyCode::Home, _) => Some(if state.focus == FocusTarget::Composer {
                AppInput::Home
            } else {
                AppInput::TimelineTop
            }),
            (KeyCode::End, _) => Some(if state.focus == FocusTarget::Composer {
                AppInput::End
            } else {
                AppInput::TimelineBottom
            }),
            (KeyCode::Up, _) => Some(AppInput::Up),
            (KeyCode::Down, _) => Some(AppInput::Down),
            (KeyCode::PageUp, _) => Some(AppInput::PageUp),
            (KeyCode::PageDown, _) => Some(AppInput::PageDown),
            (KeyCode::Char(character), mods)
                if state.focus != FocusTarget::Composer
                    && mods.contains(KeyModifiers::SHIFT)
                    && character.eq_ignore_ascii_case(&'r') =>
            {
                Some(AppInput::ToggleReasoning)
            }
            (KeyCode::Char(character), mods)
                if !mods.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                Some(AppInput::Insert(character))
            }
            _ => None,
        }
    }

    fn map_modal(self, code: KeyCode, modifiers: KeyModifiers) -> Option<AppInput> {
        match (code, modifiers) {
            (KeyCode::Enter, _) => Some(AppInput::Confirm),
            (KeyCode::Esc, _) => Some(AppInput::Cancel),
            (KeyCode::Up, _) => Some(AppInput::Up),
            (KeyCode::Down, _) => Some(AppInput::Down),
            (KeyCode::PageUp, _) => Some(AppInput::SelectPrevious),
            (KeyCode::PageDown, _) => Some(AppInput::SelectNext),
            (KeyCode::Char('y'), _) | (KeyCode::Char('a'), _) => Some(AppInput::Accept),
            (KeyCode::Char('n'), _) => Some(AppInput::Reject),
            (KeyCode::Char('r'), _) => Some(AppInput::Retry),
            (KeyCode::Char('c'), mods) if mods.contains(KeyModifiers::CONTROL) => {
                Some(AppInput::Cancel)
            }
            _ => None,
        }
    }
}

/// Convenience wrapper for callers that do not need a custom keymap.
pub fn map_key(state: &AppState, event: KeyEvent) -> Option<AppInput> {
    Keymap::default().map(state, event)
}

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyEvent, KeyEventState};

    use super::*;
    use crate::app::{LayoutMode, LiveActivity, ModalState, RuntimeState};

    fn key(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
        KeyEvent {
            code,
            modifiers,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        }
    }

    #[test]
    fn ctrl_c_opens_cancel_for_live_turn_and_quits_when_idle() {
        let mut state = AppState::new();
        assert_eq!(
            map_key(&state, key(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            Some(AppInput::RequestQuit)
        );
        state.live_activity = Some(LiveActivity::new("turn", 1, "working"));
        assert_eq!(
            map_key(&state, key(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            Some(AppInput::RequestCancel)
        );
        state.header.runtime = RuntimeState::ShuttingDown;
        assert_eq!(
            map_key(&state, key(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            Some(AppInput::ForceQuit)
        );
    }

    #[test]
    fn composer_letters_are_not_approval_shortcuts() {
        let mut state = AppState::new();
        state.primary_view = PrimaryView::MainChat;
        state.focus = FocusTarget::Composer;
        assert_eq!(
            map_key(&state, key(KeyCode::Char('a'), KeyModifiers::NONE)),
            Some(AppInput::Insert('a'))
        );
    }

    #[test]
    fn approval_shortcuts_only_exist_inside_modal() {
        let mut state = AppState::new();
        state.modal = Some(ModalState::Help);
        assert_eq!(
            map_key(&state, key(KeyCode::Char('y'), KeyModifiers::NONE)),
            Some(AppInput::Accept)
        );
    }

    #[test]
    fn release_events_are_ignored() {
        let event = KeyEvent {
            code: KeyCode::Char('x'),
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Release,
            state: KeyEventState::NONE,
        };
        assert_eq!(map_key(&AppState::new(), event), None);
    }

    #[test]
    fn reasoning_toggle_accepts_crossterm_uppercase_shift_event() {
        let mut state = AppState::new();
        state.focus = FocusTarget::Activity;
        for character in ['r', 'R'] {
            assert_eq!(
                map_key(&state, key(KeyCode::Char(character), KeyModifiers::SHIFT),),
                Some(AppInput::ToggleReasoning)
            );
        }
    }

    #[test]
    fn composer_keeps_printable_shortcuts_and_narrow_tab_digits() {
        let mut state = AppState::new();
        state.primary_view = PrimaryView::MainChat;
        state.focus = FocusTarget::Composer;
        state.layout = LayoutMode::Narrow;
        for (character, modifiers) in [
            ('R', KeyModifiers::SHIFT),
            ('u', KeyModifiers::NONE),
            ('n', KeyModifiers::NONE),
            ('?', KeyModifiers::NONE),
            ('1', KeyModifiers::NONE),
            ('2', KeyModifiers::NONE),
            ('3', KeyModifiers::NONE),
        ] {
            assert_eq!(
                map_key(&state, key(KeyCode::Char(character), modifiers)),
                Some(AppInput::Insert(character)),
                "{character:?} should remain composer input",
            );
        }
    }

    #[test]
    fn kanban_shortcuts_open_secondary_task_lists() {
        let mut state = AppState::new();
        state.layout = LayoutMode::Narrow;
        state.focus = FocusTarget::ProjectRail;
        assert_eq!(
            map_key(&state, key(KeyCode::Char('?'), KeyModifiers::NONE)),
            Some(AppInput::ToggleHelp)
        );
        assert_eq!(
            map_key(&state, key(KeyCode::Char('a'), KeyModifiers::NONE)),
            Some(AppInput::SelectProjectTab(ProjectTab::Attention))
        );
        assert_eq!(
            map_key(&state, key(KeyCode::Char('v'), KeyModifiers::NONE)),
            Some(AppInput::SelectProjectTab(ProjectTab::Approvals))
        );
    }

    #[test]
    fn primary_view_shortcuts_work_even_when_chat_composer_is_focused() {
        let mut state = AppState::new();
        state.primary_view = PrimaryView::MainChat;
        state.focus = FocusTarget::Composer;
        assert_eq!(
            map_key(&state, key(KeyCode::Char('1'), KeyModifiers::CONTROL)),
            Some(AppInput::SelectPrimaryView(PrimaryView::Kanban))
        );
        assert_eq!(
            map_key(&state, key(KeyCode::Char('['), KeyModifiers::CONTROL)),
            Some(AppInput::PreviousPrimaryView)
        );
        assert_eq!(
            map_key(&state, key(KeyCode::F(2), KeyModifiers::NONE)),
            Some(AppInput::NextPrimaryView)
        );
    }

    #[test]
    fn enter_opens_the_selected_project_card_outside_composer() {
        let mut state = AppState::new();
        state.focus = FocusTarget::ProjectRail;
        assert_eq!(
            map_key(&state, key(KeyCode::Enter, KeyModifiers::NONE)),
            Some(AppInput::OpenSelected)
        );
    }

    #[test]
    fn enter_still_submits_only_when_composer_owns_focus() {
        let mut state = AppState::new();
        state.primary_view = PrimaryView::MainChat;
        state.focus = FocusTarget::Composer;
        state.layout = LayoutMode::Narrow;
        assert_eq!(
            map_key(&state, key(KeyCode::Enter, KeyModifiers::NONE)),
            Some(AppInput::Submit)
        );
    }
}
