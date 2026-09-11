//! TUI-safe tracing setup.
//!
//! Routine diagnostics are written to a rolling file and a bounded in-memory
//! sink that the TUI may surface in its status area. Nothing is written to
//! stderr after the alternate screen is active, so log lines cannot corrupt a
//! rendered frame.

use std::collections::VecDeque;
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::EnvFilter;

/// Conservative default filter for the local TUI process.
pub const DEFAULT_LOG_FILTER: &str = "forge=info,forge_solo=info,services=info,review=info,cli_adapters=info,executors=info,db=warn,sqlx=warn";
const MAX_TUI_LOG_LINES: usize = 256;
const MAX_TUI_LOG_LINE_BYTES: usize = 4096;

/// Errors returned before the TUI starts rendering.
#[derive(Debug, thiserror::Error)]
pub enum TracingError {
    #[error("failed to initialize Solo tracing: {0}")]
    Io(#[from] io::Error),

    #[error("failed to create Solo log appender: {0}")]
    Appender(#[from] tracing_appender::rolling::InitError),

    #[error("a tracing subscriber is already installed")]
    AlreadyInitialized,
}

/// Bounded, cloneable sink for status/help views.
#[derive(Default)]
struct TuiLogState {
    lines: VecDeque<String>,
    pending: String,
}

impl TuiLogState {
    fn commit_pending(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        if self.lines.len() == MAX_TUI_LOG_LINES {
            self.lines.pop_front();
        }
        self.lines.push_back(std::mem::take(&mut self.pending));
    }
}

#[derive(Clone, Default)]
pub struct TuiLogBuffer {
    state: Arc<Mutex<TuiLogState>>,
}

impl std::fmt::Debug for TuiLogBuffer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let lines = self
            .state
            .lock()
            .map(|state| state.lines.len() + usize::from(!state.pending.is_empty()))
            .unwrap_or(0);
        formatter
            .debug_struct("TuiLogBuffer")
            .field("line_count", &lines)
            .finish()
    }
}

impl TuiLogBuffer {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a bounded snapshot in display order.
    #[must_use]
    pub fn snapshot(&self) -> Vec<String> {
        self.state
            .lock()
            .map(|state| {
                let mut lines = state.lines.iter().cloned().collect::<Vec<_>>();
                if !state.pending.is_empty() {
                    lines.push(state.pending.clone());
                }
                lines
            })
            .unwrap_or_default()
    }

    /// Drain all currently buffered messages.
    pub fn drain(&self) -> Vec<String> {
        self.state
            .lock()
            .map(|mut state| {
                state.commit_pending();
                state.lines.drain(..).collect()
            })
            .unwrap_or_default()
    }
}

impl Write for TuiLogBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        // A tracing formatter may hand a single event to the writer in one
        // very large buffer. Bound the conversion before allocating so one
        // event cannot exhaust the TUI's memory budget.
        let bounded_bytes = &bytes[..bytes.len().min(MAX_TUI_LOG_LINE_BYTES)];
        let text = String::from_utf8_lossy(bounded_bytes);
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("TUI log buffer lock poisoned"))?;
        for character in text.chars() {
            match character {
                '\n' => state.commit_pending(),
                '\r' => {}
                character if character.is_control() => {}
                character => {
                    let character_bytes = character.len_utf8();
                    if state.pending.len() + character_bytes <= MAX_TUI_LOG_LINE_BYTES {
                        state.pending.push(character);
                    }
                }
            }
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for TuiLogBuffer {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Own the non-blocking file worker for the lifetime of the subscriber.
/// Dropping this guard flushes and stops the appender worker.
pub struct TracingGuard {
    _worker_guard: tracing_appender::non_blocking::WorkerGuard,
    tui_buffer: TuiLogBuffer,
}

impl std::fmt::Debug for TracingGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TracingGuard")
            .field("tui_buffer", &self.tui_buffer)
            .finish_non_exhaustive()
    }
}

impl TracingGuard {
    #[must_use]
    pub fn tui_buffer(&self) -> TuiLogBuffer {
        self.tui_buffer.clone()
    }
}

/// Install the Solo subscriber and return its lifetime guard plus TUI-safe
/// status sink.
pub fn init_tracing(log_dir: &Path) -> Result<TracingGuard, TracingError> {
    let log_dir = validate_log_dir(log_dir)?;
    let file_appender = tracing_appender::rolling::RollingFileAppender::builder()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix("forge-solo")
        .filename_suffix("log")
        .build(log_dir)?;
    let (file_writer, worker_guard) = tracing_appender::non_blocking(file_appender);
    let tui_buffer = TuiLogBuffer::new();
    let writer = file_writer.and(tui_buffer.clone());
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_FILTER));

    let result = if matches!(
        std::env::var("FORGE_LOG_FORMAT").as_deref(),
        Ok("json" | "JSON")
    ) {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(writer)
            .json()
            .try_init()
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(writer)
            .compact()
            .try_init()
    };
    result.map_err(|_| TracingError::AlreadyInitialized)?;

    Ok(TracingGuard {
        _worker_guard: worker_guard,
        tui_buffer,
    })
}

/// Validate the log directory before the rolling appender receives it. This
/// rejects existing symlink components and rechecks after creation; std still
/// lacks a portable no-follow directory-open API for closing a final rename
/// race.
fn validate_log_dir(path: &Path) -> io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    reject_symlink_components(&absolute)?;
    if let Err(error) = std::fs::symlink_metadata(&absolute) {
        if error.kind() == io::ErrorKind::NotFound {
            std::fs::create_dir_all(&absolute)?;
        } else {
            return Err(error);
        }
    }
    reject_symlink_components(&absolute)?;
    let metadata = std::fs::symlink_metadata(&absolute)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Solo log directory must be a real directory",
        ));
    }
    Ok(absolute)
}

fn reject_symlink_components(path: &Path) -> io::Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => current.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => current.push(component.as_os_str()),
            Component::Normal(name) => {
                current.push(name);
                match std::fs::symlink_metadata(&current) {
                    Ok(metadata)
                        if metadata.file_type().is_symlink()
                            && !is_trusted_platform_alias(&current) =>
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "Solo log directory cannot contain symlink components",
                        ));
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                    Err(error) => return Err(error),
                }
            }
        }
    }
    Ok(())
}

fn is_trusted_platform_alias(path: &Path) -> bool {
    #[cfg(target_os = "macos")]
    {
        matches!(path.to_str(), Some("/var" | "/tmp" | "/etc"))
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = path;
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tui_sink_is_bounded_and_does_not_write_to_terminal() {
        let mut sink = TuiLogBuffer::new();
        sink.write_all(b"first\nsecond\n").unwrap();
        assert_eq!(sink.snapshot(), ["first", "second"]);
        for index in 0..(MAX_TUI_LOG_LINES + 5) {
            writeln!(sink, "line-{index}").unwrap();
        }
        let lines = sink.snapshot();
        assert_eq!(lines.len(), MAX_TUI_LOG_LINES);
        assert_eq!(lines.first().map(String::as_str), Some("line-5"));
        assert_eq!(sink.drain().len(), MAX_TUI_LOG_LINES);
        assert!(sink.snapshot().is_empty());
    }

    #[test]
    fn tui_sink_strips_controls_and_bounds_each_line_by_bytes() {
        let mut sink = TuiLogBuffer::new();
        let long_line = format!(
            "before\u{1b}[31m{}after\n",
            "x".repeat(MAX_TUI_LOG_LINE_BYTES + 128)
        );
        sink.write_all(long_line.as_bytes()).unwrap();
        let line = sink.snapshot().pop().expect("one sanitized line");
        assert!(!line.contains('\u{1b}'));
        assert!(!line.chars().any(char::is_control));
        assert!(line.len() <= MAX_TUI_LOG_LINE_BYTES);
    }

    #[cfg(unix)]
    #[test]
    fn log_directory_symlink_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("target");
        let link = root.path().join("logs");
        std::fs::create_dir(&target).unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert!(validate_log_dir(&link).is_err());
    }
}
