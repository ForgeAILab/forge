//! Production terminal integration for Forge Solo.
//!
//! This module is deliberately a thin boundary around the pure controller and
//! reducer.  Crossterm owns the process-facing event reader, while the
//! controller continues to consume the small keyboard-independent vocabulary
//! in [`crate::controller`].  The renderer is hosted by [`TuiHost`], which
//! keeps Ratatui's terminal and the terminal restoration guards together.
//!
//! The event reader is isolated on one blocking thread.  `event::poll` always
//! waits for a non-zero interval, so a terminal with no input does not turn
//! the Tokio runtime (or a CPU) into a busy loop.  The channel is bounded; if
//! a caller stops polling it, the blocking sender is released when the source
//! is dropped and no terminal thread is leaked.

use std::{
    fmt::{self, Display, Write as _},
    io::{self, Stdout, Write},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use crossterm::event::{
    self, Event as CrosstermEvent, KeyCode as CrosstermKeyCode, KeyEvent as CrosstermKeyEvent,
    KeyEventKind, KeyModifiers as CrosstermKeyModifiers,
};
use ratatui::{backend::CrosstermBackend, Frame, Terminal};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    backend::BackendFuture,
    controller::{
        ControllerStatus, InputEvent, InputSourcePoll, KeyCode, KeyEvent, KeyModifiers, RenderHook,
        TerminalEventSource,
    },
    terminal::{PanicHookGuard, TerminalGuard},
};

/// Number of events retained by a default terminal source.
pub const DEFAULT_EVENT_CHANNEL_CAPACITY: usize = 128;

/// Poll interval used by [`CrosstermEventSource::with_defaults`].
pub const DEFAULT_EVENT_POLL_INTERVAL: Duration = Duration::from_millis(50);

const MAX_SOURCE_ERROR_DETAIL_CHARS: usize = 256;

/// Normalize an event channel capacity without allowing an unbuffered source
/// to accidentally turn terminal delivery into a rendezvous with the
/// controller.
fn normalized_capacity(capacity: usize) -> usize {
    capacity.max(1)
}

fn normalized_poll_interval(interval: Duration) -> Duration {
    if interval.is_zero() {
        Duration::from_millis(1)
    } else {
        interval
    }
}

/// A formatting sink that keeps error messages safe for terminal rendering.
/// Error implementations are allowed to return arbitrary text, including
/// control sequences and very large payloads, so details are sanitized while
/// they are formatted instead of allocating an unbounded intermediate
/// `String`.
struct BoundedDetail {
    text: String,
    truncated: bool,
}

impl BoundedDetail {
    fn new() -> Self {
        Self {
            text: String::new(),
            truncated: false,
        }
    }

    fn finish(mut self) -> String {
        if self.truncated {
            while self.text.chars().count() >= MAX_SOURCE_ERROR_DETAIL_CHARS {
                self.text.pop();
            }
            self.text.push('…');
        }
        self.text
    }
}

impl fmt::Write for BoundedDetail {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let remaining = MAX_SOURCE_ERROR_DETAIL_CHARS.saturating_sub(self.text.chars().count());
        for character in value.chars().take(remaining) {
            self.text.push(if character.is_control() {
                ' '
            } else {
                character
            });
        }
        if value.chars().count() > remaining {
            self.truncated = true;
        }
        Ok(())
    }
}

fn bounded_source_error(context: &str, error: &dyn Display) -> String {
    let mut detail = BoundedDetail::new();
    let _ = write!(&mut detail, "{context}: {error}");
    detail.finish()
}

/// Messages carried by the physical source before they become controller
/// polling results.  Keeping errors separate from `InputEvent` prevents a
/// physical terminal failure from being mistaken for a user command.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SourceMessage {
    Event(InputEvent),
    Unavailable { detail: String },
    Closed,
}

impl From<SourceMessage> for InputSourcePoll {
    fn from(message: SourceMessage) -> Self {
        match message {
            SourceMessage::Event(event) => Self::Event(event),
            SourceMessage::Unavailable { detail } => Self::Unavailable { detail },
            SourceMessage::Closed => Self::Closed,
        }
    }
}

/// A production Crossterm event source for [`crate::controller::SoloController`].
///
/// Crossterm's event API is synchronous.  Reading it on a dedicated thread
/// keeps `TerminalEventSource::recv` asynchronous from the controller's point
/// of view, without running a blocking syscall on a Tokio worker.  The source
/// does not enter raw mode itself; callers must enter [`TerminalGuard`] only
/// after startup preflight has succeeded.
pub struct CrosstermEventSource {
    sender: mpsc::Sender<SourceMessage>,
    receiver: Option<mpsc::Receiver<SourceMessage>>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
    signal_task: Option<JoinHandle<()>>,
    poll_interval: Duration,
}

impl CrosstermEventSource {
    /// Start a source with a bounded queue and a non-zero poll interval.
    ///
    /// The thread is started immediately, but this function performs no
    /// terminal mutation.  Crossterm reports an unavailable terminal through
    /// [`InputSourcePoll::Unavailable`] when polling fails.
    pub fn new(capacity: usize, poll_interval: Duration) -> io::Result<Self> {
        let capacity = normalized_capacity(capacity);
        let poll_interval = normalized_poll_interval(poll_interval);
        let (sender, receiver) = mpsc::channel(capacity);
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread_sender = sender.clone();
        let thread = thread::Builder::new()
            .name("forge-solo-terminal-input".to_owned())
            .spawn(move || run_crossterm_reader(thread_sender, thread_stop, poll_interval))?;

        Ok(Self {
            sender,
            receiver: Some(receiver),
            stop,
            thread: Some(thread),
            signal_task: None,
            poll_interval,
        })
    }

    /// Start a source with the standard queue and poll interval.
    pub fn with_defaults() -> io::Result<Self> {
        Self::new(DEFAULT_EVENT_CHANNEL_CAPACITY, DEFAULT_EVENT_POLL_INTERVAL)
    }

    /// Start the Crossterm source and install the supported process signal
    /// bridge in the same bounded input queue.
    ///
    /// On Unix, SIGINT becomes [`InputEvent::Interrupt`] and SIGTERM becomes
    /// [`InputEvent::Quit`].  On platforms without Unix signal kinds,
    /// `tokio::signal::ctrl_c` is mapped to `Interrupt`.  A Tokio runtime must
    /// already be active when this method is called.
    pub fn with_termination_signals(capacity: usize, poll_interval: Duration) -> io::Result<Self> {
        let mut source = Self::new(capacity, poll_interval)?;
        source.signal_task = Some(spawn_signal_task(source.sender.clone())?);
        Ok(source)
    }

    /// The effective poll interval, useful for diagnostics and deterministic
    /// embedding configuration.
    pub fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    /// Ask the reader to stop and join it.  This is idempotent and is also run
    /// by `Drop`.  Taking the receiver first releases a sender blocked by a
    /// full bounded queue, so shutdown never waits for a consumer that has
    /// already gone away.
    pub fn shutdown(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.receiver.take();
        if let Some(task) = self.signal_task.take() {
            task.abort();
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl TerminalEventSource for CrosstermEventSource {
    fn recv(&mut self) -> BackendFuture<'_, InputSourcePoll> {
        let Some(receiver) = self.receiver.as_mut() else {
            return Box::pin(async { InputSourcePoll::Closed });
        };
        Box::pin(async move {
            receiver
                .recv()
                .await
                .map(InputSourcePoll::from)
                .unwrap_or(InputSourcePoll::Closed)
        })
    }
}

impl Drop for CrosstermEventSource {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn run_crossterm_reader(
    sender: mpsc::Sender<SourceMessage>,
    stop: Arc<AtomicBool>,
    poll_interval: Duration,
) {
    while !stop.load(Ordering::Relaxed) {
        let has_event = match event::poll(poll_interval) {
            Ok(has_event) => has_event,
            Err(error) => {
                send_reader_message(
                    &sender,
                    SourceMessage::Unavailable {
                        detail: bounded_source_error("terminal event polling failed", &error),
                    },
                );
                send_reader_message(&sender, SourceMessage::Closed);
                return;
            }
        };
        if !has_event {
            continue;
        }

        let event = match event::read() {
            Ok(event) => event,
            Err(error) => {
                send_reader_message(
                    &sender,
                    SourceMessage::Unavailable {
                        detail: bounded_source_error("terminal event read failed", &error),
                    },
                );
                send_reader_message(&sender, SourceMessage::Closed);
                return;
            }
        };

        if let Some(input) = map_crossterm_event(event) {
            send_reader_message(&sender, SourceMessage::Event(input));
        }
    }
}

fn send_reader_message(sender: &mpsc::Sender<SourceMessage>, message: SourceMessage) {
    // `blocking_send` is intentional: this code runs on the dedicated reader
    // thread.  Dropping the receiver during shutdown unblocks it even when a
    // consumer stopped draining the bounded queue.
    let _ = sender.blocking_send(message);
}

/// Map one Crossterm event into the controller's keyboard-independent input
/// vocabulary.  Mouse, paste, focus, and key-release events are intentionally
/// ignored because the v1 TUI is keyboard-only.
pub fn map_crossterm_event(event: CrosstermEvent) -> Option<InputEvent> {
    match event {
        CrosstermEvent::Key(key) => map_crossterm_key_event(key),
        CrosstermEvent::Resize(width, height) => Some(InputEvent::Resize { width, height }),
        // Keep the wildcard for optional Crossterm variants such as
        // bracketed-paste, which are not compiled when the crate's optional
        // feature is disabled.
        _ => None,
    }
}

/// Map a pressed or repeated Crossterm key into the controller vocabulary.
pub fn map_crossterm_key_event(key: CrosstermKeyEvent) -> Option<InputEvent> {
    if key.kind == KeyEventKind::Release {
        return None;
    }

    if key.modifiers.contains(CrosstermKeyModifiers::CONTROL)
        && matches!(key.code, CrosstermKeyCode::Char(character) if character.eq_ignore_ascii_case(&'c'))
    {
        return Some(InputEvent::Interrupt);
    }

    let code = match key.code {
        CrosstermKeyCode::Char(character) => KeyCode::Char(character),
        CrosstermKeyCode::Enter => KeyCode::Enter,
        CrosstermKeyCode::Esc => KeyCode::Esc,
        CrosstermKeyCode::Backspace => KeyCode::Backspace,
        CrosstermKeyCode::Delete => KeyCode::Delete,
        CrosstermKeyCode::Left => KeyCode::Left,
        CrosstermKeyCode::Right => KeyCode::Right,
        CrosstermKeyCode::Up => KeyCode::Up,
        CrosstermKeyCode::Down => KeyCode::Down,
        CrosstermKeyCode::Home => KeyCode::Home,
        CrosstermKeyCode::End => KeyCode::End,
        CrosstermKeyCode::PageUp => KeyCode::PageUp,
        CrosstermKeyCode::PageDown => KeyCode::PageDown,
        CrosstermKeyCode::Tab => KeyCode::Tab,
        CrosstermKeyCode::BackTab => KeyCode::BackTab,
        CrosstermKeyCode::F(number) => KeyCode::F(number),
        _ => return None,
    };

    Some(InputEvent::Key(KeyEvent {
        code,
        modifiers: KeyModifiers {
            ctrl: key.modifiers.contains(CrosstermKeyModifiers::CONTROL),
            alt: key.modifiers.contains(CrosstermKeyModifiers::ALT),
            shift: key.modifiers.contains(CrosstermKeyModifiers::SHIFT),
        },
    }))
}

/// A one-shot bridge from supported process termination signals to the same
/// typed input vocabulary consumed by the controller.
///
/// The source intentionally remains open after delivering its signal.  This
/// lets the controller show its first-interrupt cancellation confirmation and
/// lets the normal shutdown path restore the terminal.  Dropping the source
/// aborts the signal task.
pub struct SignalInputSource {
    sender: mpsc::Sender<SourceMessage>,
    receiver: Option<mpsc::Receiver<SourceMessage>>,
    task: Option<JoinHandle<()>>,
}

impl SignalInputSource {
    /// Create a signal source.  A Tokio runtime must already be active.
    pub fn new(capacity: usize) -> io::Result<Self> {
        let _ = tokio::runtime::Handle::try_current().map_err(|_| {
            io::Error::other("termination signal source requires an active Tokio runtime")
        })?;
        let (sender, receiver) = mpsc::channel(normalized_capacity(capacity));
        let task = spawn_signal_task(sender.clone())?;
        Ok(Self {
            sender,
            receiver: Some(receiver),
            task: Some(task),
        })
    }

    /// Whether the receiver is still available for controller polling.
    pub fn is_open(&self) -> bool {
        self.receiver.is_some() && !self.sender.is_closed()
    }

    /// Stop listening for signals.  This is also performed by `Drop`.
    pub fn shutdown(&mut self) {
        self.receiver.take();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl TerminalEventSource for SignalInputSource {
    fn recv(&mut self) -> BackendFuture<'_, InputSourcePoll> {
        let Some(receiver) = self.receiver.as_mut() else {
            return Box::pin(async { InputSourcePoll::Closed });
        };
        Box::pin(async move {
            receiver
                .recv()
                .await
                .map(InputSourcePoll::from)
                .unwrap_or(InputSourcePoll::Closed)
        })
    }
}

impl Drop for SignalInputSource {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn spawn_signal_task(sender: mpsc::Sender<SourceMessage>) -> io::Result<JoinHandle<()>> {
    let _ = tokio::runtime::Handle::try_current().map_err(|_| {
        io::Error::other("termination signal source requires an active Tokio runtime")
    })?;
    Ok(tokio::spawn(async move {
        if let Err(detail) = wait_for_termination_signal(&sender).await {
            let _ = sender.send(SourceMessage::Unavailable { detail }).await;
            let _ = sender.send(SourceMessage::Closed).await;
        }
    }))
}

async fn wait_for_termination_signal(sender: &mpsc::Sender<SourceMessage>) -> Result<(), String> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut interrupt = signal(SignalKind::interrupt())
            .map_err(|error| bounded_source_error("could not register SIGINT handler", &error))?;
        let mut terminate = signal(SignalKind::terminate())
            .map_err(|error| bounded_source_error("could not register SIGTERM handler", &error))?;

        tokio::select! {
            signal = interrupt.recv() => {
                if signal.is_some() {
                    let _ = sender.send(SourceMessage::Event(InputEvent::Interrupt)).await;
                }
            }
            signal = terminate.recv() => {
                if signal.is_some() {
                    let _ = sender.send(SourceMessage::Event(InputEvent::Quit)).await;
                }
            }
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .map_err(|error| bounded_source_error("could not register Ctrl-C handler", &error))?;
        let _ = sender
            .send(SourceMessage::Event(InputEvent::Interrupt))
            .await;
        Ok(())
    }
}

/// Restore a guard without consuming it on failure.  `TerminalGuard` retains
/// the raw/alternate flags that failed, allowing a later retry (including the
/// guard's own `Drop` fallback) to issue the missing inverse terminal command.
fn restore_guard<W: Write>(
    guard: &mut Option<TerminalGuard<W>>,
    first_error: &mut Option<io::Error>,
) -> bool {
    let Some(guard) = guard.as_mut() else {
        return true;
    };
    match guard.restore() {
        Ok(()) => true,
        Err(error) => {
            if first_error.is_none() {
                *first_error = Some(error);
            }
            false
        }
    }
}

/// Ratatui terminal host for one Solo controller.
///
/// The host owns the alternate-screen/raw-mode [`TerminalGuard`] and the
/// panic hook guard for the same lifetime as Ratatui's terminal.  Its render
/// hook is controller-compatible: it captures no reducer mutation and records
/// draw failures for the main loop to surface as a bounded startup/runtime
/// error.
pub struct TuiHost {
    terminal: Arc<Mutex<Terminal<CrosstermBackend<Stdout>>>>,
    guard: Option<TerminalGuard>,
    panic_hook: Option<PanicHookGuard>,
    render_error: Arc<Mutex<Option<String>>>,
}

impl TuiHost {
    /// Enter raw mode and the alternate screen, then create Ratatui's
    /// Crossterm terminal and install panic restoration.
    pub fn enter() -> io::Result<Self> {
        let guard = TerminalGuard::enter()?;
        let backend = CrosstermBackend::new(io::stdout());
        let terminal = match Terminal::new(backend) {
            Ok(terminal) => terminal,
            Err(error) => {
                let mut guard = guard;
                let _ = guard.restore();
                return Err(error);
            }
        };
        Ok(Self::from_terminal(terminal, guard))
    }

    /// Build a host around an already-created terminal and active guard.
    ///
    /// This is useful for embedders that need to perform their own preflight
    /// or use a custom Ratatui viewport.  The host still installs the panic
    /// hook and owns restoration from this point forward.
    pub fn from_terminal(
        terminal: Terminal<CrosstermBackend<Stdout>>,
        guard: TerminalGuard,
    ) -> Self {
        Self {
            terminal: Arc::new(Mutex::new(terminal)),
            guard: Some(guard),
            panic_hook: Some(PanicHookGuard::install()),
            render_error: Arc::new(Mutex::new(None)),
        }
    }

    /// Draw one frame synchronously on the host's terminal.
    pub fn draw<F>(&self, render: F) -> io::Result<()>
    where
        F: FnOnce(&mut Frame<'_>),
    {
        let mut terminal = self
            .terminal
            .lock()
            .map_err(|_| io::Error::other("TUI terminal lock poisoned"))?;
        terminal.draw(render).map(|_| ())
    }

    /// Make a controller-compatible redraw hook.
    ///
    /// `render` is constrained to `Fn` because the returned hook may be called
    /// repeatedly by the controller.  Use a mutable interior value such as an
    /// `Arc<Mutex<_>>` when a renderer needs local counters.
    pub fn render_hook<R, F>(&self, render: F) -> RenderHook<R>
    where
        R: Send + 'static,
        F: Fn(&mut Frame<'_>, &R, &ControllerStatus) + Send + Sync + 'static,
    {
        let terminal = Arc::clone(&self.terminal);
        let render_error = Arc::clone(&self.render_error);
        Box::new(move |state, status| {
            let result = terminal
                .lock()
                .map_err(|_| io::Error::other("TUI terminal lock poisoned"))
                .and_then(|mut terminal| {
                    terminal
                        .draw(|frame| render(frame, state, status))
                        .map(|_| ())
                });
            if let Err(error) = result {
                if let Ok(mut slot) = render_error.lock() {
                    if slot.is_none() {
                        *slot = Some(error.to_string());
                    }
                }
            }
        })
    }

    /// Take and clear the first draw error recorded by a render hook.
    pub fn take_render_error(&self) -> Option<String> {
        self.render_error
            .lock()
            .ok()
            .and_then(|mut slot| slot.take())
    }

    /// Show the cursor and restore raw mode and the original screen.  Calling
    /// this more than once is safe; `Drop` performs the same best-effort path.
    pub fn restore(&mut self) -> io::Result<()> {
        let mut first_error = None;
        match self.terminal.lock() {
            Ok(mut terminal) => {
                if let Err(error) = terminal.show_cursor() {
                    first_error = Some(io::Error::other(format!(
                        "could not show terminal cursor: {error}"
                    )));
                }
            }
            Err(_) => {
                first_error = Some(io::Error::other(
                    "TUI terminal lock poisoned during restore",
                ));
            }
        }

        let guard_restored = restore_guard(&mut self.guard, &mut first_error);
        if guard_restored && first_error.is_none() {
            self.guard.take();
            // Restoring the previous hook after the terminal has been
            // restored keeps subsequent panics in the host process from
            // using a stale TUI hook.  If either terminal operation failed,
            // retain the hook alongside the guard for the next retry.
            self.panic_hook.take();
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl Drop for TuiHost {
    fn drop(&mut self) {
        // `restore` keeps both guards when an inverse command fails; dropping
        // the retained TerminalGuard then provides one final retry.
        let _ = self.restore();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(code: CrosstermKeyCode, modifiers: CrosstermKeyModifiers) -> CrosstermKeyEvent {
        CrosstermKeyEvent::new_with_kind(code, modifiers, KeyEventKind::Press)
    }

    #[test]
    fn maps_resize_and_keyboard_vocabulary() {
        assert_eq!(
            map_crossterm_event(CrosstermEvent::Resize(103, 41)),
            Some(InputEvent::Resize {
                width: 103,
                height: 41,
            })
        );
        assert_eq!(
            map_crossterm_event(CrosstermEvent::Key(key(
                CrosstermKeyCode::F(7),
                CrosstermKeyModifiers::SHIFT,
            ))),
            Some(InputEvent::Key(KeyEvent {
                code: KeyCode::F(7),
                modifiers: KeyModifiers {
                    ctrl: false,
                    alt: false,
                    shift: true,
                },
            }))
        );
        assert_eq!(
            map_crossterm_event(CrosstermEvent::Key(key(
                CrosstermKeyCode::BackTab,
                CrosstermKeyModifiers::SHIFT,
            ))),
            Some(InputEvent::Key(KeyEvent {
                code: KeyCode::BackTab,
                modifiers: KeyModifiers {
                    ctrl: false,
                    alt: false,
                    shift: true,
                },
            }))
        );
    }

    #[test]
    fn maps_ctrl_c_to_interrupt_and_ignores_non_input_events() {
        assert_eq!(
            map_crossterm_event(CrosstermEvent::Key(key(
                CrosstermKeyCode::Char('c'),
                CrosstermKeyModifiers::CONTROL,
            ))),
            Some(InputEvent::Interrupt)
        );
        assert_eq!(
            map_crossterm_event(CrosstermEvent::Key(key(
                CrosstermKeyCode::Char('C'),
                CrosstermKeyModifiers::CONTROL,
            ))),
            Some(InputEvent::Interrupt)
        );
        assert_eq!(
            map_crossterm_event(CrosstermEvent::Key(CrosstermKeyEvent::new_with_kind(
                CrosstermKeyCode::Char('x'),
                CrosstermKeyModifiers::NONE,
                KeyEventKind::Release,
            ))),
            None
        );
        assert_eq!(
            map_crossterm_event(CrosstermEvent::Mouse(crossterm::event::MouseEvent {
                kind: crossterm::event::MouseEventKind::Moved,
                column: 1,
                row: 2,
                modifiers: CrosstermKeyModifiers::NONE,
            })),
            None
        );
    }

    #[test]
    fn normalizes_zero_poll_interval_and_capacity() {
        assert_eq!(normalized_capacity(0), 1);
        assert_eq!(normalized_capacity(4), 4);
        assert_eq!(
            normalized_poll_interval(Duration::ZERO),
            Duration::from_millis(1)
        );
        assert_eq!(
            normalized_poll_interval(Duration::from_millis(4)),
            Duration::from_millis(4)
        );
    }

    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _bytes: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("forced terminal write failure"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::other("forced terminal flush failure"))
        }
    }

    #[test]
    fn failed_guard_is_retained_for_a_later_restore_retry() {
        let mut guard = Some(TerminalGuard::from_active_writer(FailingWriter));
        let mut first_error = None;
        assert!(!restore_guard(&mut guard, &mut first_error));
        assert!(guard.as_ref().is_some_and(TerminalGuard::is_active));
        assert!(first_error.is_some());
    }

    struct NoisyError;

    impl Display for NoisyError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("\u{1b}[31m")?;
            formatter.write_str(&"x".repeat(MAX_SOURCE_ERROR_DETAIL_CHARS * 4))?;
            formatter.write_str("\n\t")
        }
    }

    #[test]
    fn source_error_detail_is_sanitized_and_bounded() {
        let detail = bounded_source_error("terminal event read failed", &NoisyError);
        assert!(detail.chars().count() <= MAX_SOURCE_ERROR_DETAIL_CHARS);
        assert!(!detail.chars().any(char::is_control));
        assert!(detail.ends_with('…'));
    }

    #[tokio::test]
    async fn source_can_be_closed_without_waiting_for_a_consumer() {
        let mut source = CrosstermEventSource::new(1, Duration::from_millis(1)).unwrap();
        source.shutdown();
        assert!(matches!(source.recv().await, InputSourcePoll::Closed));
    }

    #[tokio::test]
    async fn signal_source_requires_no_terminal_and_is_droppable() {
        let source = SignalInputSource::new(1).unwrap();
        drop(source);
    }
}
