//! Terminal lifecycle and restoration helpers.
//!
//! A `TerminalGuard` owns the alternate screen/raw-mode transition.  Its
//! `Drop` implementation is deliberately best-effort because it is also the
//! last line of defence during an error or panic.  Callers should use
//! [`TerminalGuard::restore`] on the normal shutdown path so restoration
//! failures can be reported.

use std::{
    io::{self, IsTerminal, Stdout, Write},
    panic::{self, PanicHookInfo},
    sync::{Arc, Mutex},
};

use crossterm::{
    cursor::{Hide, Show},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};

type PanicHook = Box<dyn Fn(&PanicHookInfo<'_>) + Send + Sync + 'static>;
type SharedPanicHook = Arc<Mutex<Option<PanicHook>>>;

/// Owns terminal state while the Solo TUI is running.
pub struct TerminalGuard<W: Write = Stdout> {
    writer: Option<W>,
    raw_mode: bool,
    alternate_screen: bool,
}

impl TerminalGuard<Stdout> {
    /// Enter the TUI only when both standard streams are interactive.
    ///
    /// This check happens before enabling raw mode or changing the screen, so
    /// piping `forge-solo` cannot leave a caller's terminal half-mutated.
    pub fn enter() -> io::Result<Self> {
        if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "forge-solo requires an interactive stdin/stdout terminal",
            ));
        }
        Self::enter_with(io::stdout())
    }
}

impl<W: Write> TerminalGuard<W> {
    /// Enter raw mode and the alternate screen using a caller-provided
    /// writer.  This is useful for embedding and for deterministic tests.
    pub fn enter_with(mut writer: W) -> io::Result<Self> {
        enable_raw_mode()?;
        if let Err(error) = execute!(writer, EnterAlternateScreen, Hide) {
            // The command may have entered the alternate screen before a
            // later escape failed. Always attempt the complete inverse before
            // returning the original error.
            let _ = execute!(writer, Show, LeaveAlternateScreen);
            let _ = disable_raw_mode();
            return Err(error);
        }
        if let Err(error) = writer.flush() {
            let _ = execute!(writer, Show, LeaveAlternateScreen);
            let _ = disable_raw_mode();
            return Err(error);
        }
        Ok(Self {
            writer: Some(writer),
            raw_mode: true,
            alternate_screen: true,
        })
    }

    /// Construct a guard for a writer that is already in an alternate screen.
    ///
    /// This does not touch the process terminal and is intended for tests or
    /// hosts that perform setup themselves.  Drop/`restore` still emits the
    /// restoration sequence and remains idempotent.
    pub fn from_active_writer(writer: W) -> Self {
        Self {
            writer: Some(writer),
            raw_mode: false,
            alternate_screen: true,
        }
    }

    pub fn writer_mut(&mut self) -> Option<&mut W> {
        self.writer.as_mut()
    }

    pub fn is_active(&self) -> bool {
        self.raw_mode || self.alternate_screen
    }

    /// Restore raw mode, cursor visibility, and the previous screen.
    pub fn restore(&mut self) -> io::Result<()> {
        let mut first_error = None;
        if self.raw_mode {
            match disable_raw_mode() {
                Ok(()) => self.raw_mode = false,
                Err(error) => first_error = Some(error),
            }
        }
        if self.alternate_screen {
            let Some(writer) = self.writer.as_mut() else {
                if first_error.is_none() {
                    first_error = Some(io::Error::other(
                        "terminal writer unavailable during restoration",
                    ));
                }
                return first_error.map_or(Ok(()), Err);
            };
            let mut restored = true;
            if let Err(error) = execute!(writer, Show, LeaveAlternateScreen) {
                restored = false;
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
            if let Err(error) = writer.flush() {
                restored = false;
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
            if restored {
                self.alternate_screen = false;
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Restore the terminal and return the underlying writer.
    pub fn into_writer(mut self) -> io::Result<W> {
        self.restore()?;
        self.writer
            .take()
            .ok_or_else(|| io::Error::other("terminal writer already taken"))
    }
}

impl<W: Write> Drop for TerminalGuard<W> {
    fn drop(&mut self) {
        let _ = self.restore();
    }
}

/// Installs a panic hook that restores the process terminal before delegating
/// to the previous hook.  Keep the returned guard alive for the TUI lifetime;
/// dropping it restores the previous panic hook.
pub struct PanicHookGuard {
    previous: SharedPanicHook,
    active: bool,
}

impl PanicHookGuard {
    pub fn install() -> Self {
        let previous = Arc::new(Mutex::new(Some(panic::take_hook())));
        let hook_previous = Arc::clone(&previous);
        panic::set_hook(Box::new(move |info| {
            restore_process_terminal();
            if let Ok(previous) = hook_previous.lock() {
                if let Some(previous) = previous.as_ref() {
                    previous(info);
                }
            }
        }));
        Self {
            previous,
            active: true,
        }
    }
}

impl Drop for PanicHookGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut previous) = self.previous.lock() {
            if let Some(previous) = previous.take() {
                panic::set_hook(previous);
            }
        }
        self.active = false;
    }
}

/// Best-effort process-level restoration used from the panic hook.  The
/// normal guard remains authoritative for reporting restoration errors.
pub fn restore_process_terminal() {
    let _ = disable_raw_mode();
    let mut stdout = io::stdout();
    let _ = execute!(stdout, Show, LeaveAlternateScreen);
    let _ = stdout.flush();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restoration_is_idempotent_and_emits_cursor_and_screen_sequence() {
        let guard = TerminalGuard::from_active_writer(Vec::<u8>::new());
        let mut guard = guard;
        guard.restore().expect("restore writer");
        guard.restore().expect("second restore is a no-op");
        let bytes = guard.into_writer().expect("take writer");
        let text = String::from_utf8(bytes).expect("ANSI bytes are UTF-8");
        assert!(text.contains("\u{1b}[?25h"));
        assert!(text.contains("\u{1b}[?1049l"));
    }

    #[test]
    fn non_interactive_guard_entry_fails_before_mutation() {
        // `enter` checks the real process streams.  The test runner is not a
        // terminal, so this also protects against accidentally making the
        // test process raw when this test runs in a local terminal.
        if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
            let error = match TerminalGuard::enter() {
                Ok(_) => panic!("non-TTY entry unexpectedly succeeded"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        }
    }

    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _bytes: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "test writer"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "test writer"))
        }
    }

    #[test]
    fn failed_restore_keeps_state_for_drop_or_retry() {
        let mut guard = TerminalGuard::from_active_writer(FailingWriter);
        assert!(guard.restore().is_err());
        assert!(guard.is_active());
    }
}
