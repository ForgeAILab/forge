//! Command-line parsing and startup preflight for `forge-solo`.
//!
//! This module deliberately contains no repository or database side effects.
//! The binary should call [`ensure_interactive`] before resolving a marker or
//! creating a Solo data root so a redirected invocation fails cleanly.

use std::io::{self, IsTerminal};
use std::path::{Path, PathBuf};

use clap::{Parser, ValueEnum};

/// The local CLI harnesses supported by the first Solo release.
///
/// Embedded, shell, and null adapters are intentionally not represented here:
/// they are useful for server/demo/test plumbing, but are not user-selectable
/// first-run Solo agents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ValueEnum)]
#[value(rename_all = "verbatim")]
pub enum AgentExecutor {
    #[value(name = "codex")]
    Codex,
    #[value(name = "claude_code")]
    ClaudeCode,
    #[value(name = "cursor")]
    Cursor,
    #[value(name = "opencode")]
    OpenCode,
    #[value(name = "gemini")]
    Gemini,
    #[value(name = "smith")]
    Smith,
}

impl AgentExecutor {
    /// Return the stable command-line spelling used by adapter configuration.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::ClaudeCode => "claude_code",
            Self::Cursor => "cursor",
            Self::OpenCode => "opencode",
            Self::Gemini => "gemini",
            Self::Smith => "smith",
        }
    }
}

impl std::fmt::Display for AgentExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Additive command-line contract for Solo.
#[derive(Debug, Clone, PartialEq, Eq, Parser)]
#[command(
    name = "forge-solo",
    version,
    about = "Forge — chat-first workflow control for one Git repository"
)]
pub struct Cli {
    /// Existing Git repository (or a directory below its primary worktree).
    #[arg(value_name = "PATH", default_value = ".")]
    pub path: PathBuf,

    /// Exact Solo data root. Defaults to <Forge data root>/solo/<repo-id>.
    #[arg(long = "data-dir", value_name = "PATH")]
    pub data_dir: Option<PathBuf>,

    /// Local CLI harness to use for the Project Agent.
    #[arg(long = "agent", value_name = "EXECUTOR")]
    pub agent: Option<AgentExecutor>,
}

impl Cli {
    /// Resolve the launch argument against an explicit current directory.
    ///
    /// Keeping this operation lexical and side-effect free lets callers run
    /// terminal preflight before touching the filesystem. Repository
    /// resolution performs canonicalization once startup has passed preflight.
    #[must_use]
    pub fn path_from(&self, current_dir: &Path) -> PathBuf {
        make_absolute(current_dir, &self.path)
    }

    /// Resolve `PATH` against the process current directory.
    pub fn resolve_path(&self) -> io::Result<PathBuf> {
        Ok(self.path_from(&std::env::current_dir()?))
    }
}

fn make_absolute(current_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else if path == Path::new(".") {
        current_dir.to_path_buf()
    } else {
        current_dir.join(path)
    }
}

/// The result of terminal preflight. It is retained so the controller can use
/// the same checked state when entering raw mode, and so tests need not mock
/// process-global stdin/stdout handles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalPreflight {
    pub stdin_is_terminal: bool,
    pub stdout_is_terminal: bool,
}

impl TerminalPreflight {
    #[must_use]
    pub const fn is_interactive(self) -> bool {
        self.stdin_is_terminal && self.stdout_is_terminal
    }
}

/// Startup errors that are safe to print before entering the alternate screen.
#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum PreflightError {
    #[error(
        "forge-solo requires an interactive terminal (stdin is {stdin_status}, stdout is {stdout_status}); run it from a TTY"
    )]
    NonInteractive {
        stdin_status: TerminalStatus,
        stdout_status: TerminalStatus,
    },
}

/// Human-readable terminal state used in the preflight error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalStatus {
    Terminal,
    Redirected,
}

impl std::fmt::Display for TerminalStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Terminal => "a terminal",
            Self::Redirected => "not a terminal",
        })
    }
}

/// Check the process stdin/stdout handles before any startup mutation.
pub fn preflight() -> Result<TerminalPreflight, PreflightError> {
    preflight_with(io::stdin().is_terminal(), io::stdout().is_terminal())
}

/// Check supplied terminal states. This is the deterministic seam used by
/// tests and by embedders that own their terminal handles.
pub fn preflight_with(
    stdin_is_terminal: bool,
    stdout_is_terminal: bool,
) -> Result<TerminalPreflight, PreflightError> {
    let result = TerminalPreflight {
        stdin_is_terminal,
        stdout_is_terminal,
    };
    if result.is_interactive() {
        Ok(result)
    } else {
        Err(PreflightError::NonInteractive {
            stdin_status: if stdin_is_terminal {
                TerminalStatus::Terminal
            } else {
                TerminalStatus::Redirected
            },
            stdout_status: if stdout_is_terminal {
                TerminalStatus::Terminal
            } else {
                TerminalStatus::Redirected
            },
        })
    }
}

/// Convenience wrapper for startup code that only needs success/failure.
pub fn ensure_interactive() -> Result<(), PreflightError> {
    preflight().map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_additive_cli_contract() {
        let cli = Cli::try_parse_from([
            "forge-solo",
            "nested/repository",
            "--data-dir",
            "/tmp/solo-state",
            "--agent",
            "claude_code",
        ])
        .expect("CLI contract should parse");

        assert_eq!(cli.path, PathBuf::from("nested/repository"));
        assert_eq!(cli.data_dir, Some(PathBuf::from("/tmp/solo-state")));
        assert_eq!(cli.agent, Some(AgentExecutor::ClaudeCode));
        assert_eq!(cli.agent.map(AgentExecutor::as_str), Some("claude_code"));
    }

    #[test]
    fn path_defaults_to_current_directory_without_canonicalizing() {
        let cli = Cli::try_parse_from(["forge-solo"]).expect("default path should parse");
        assert_eq!(cli.path, PathBuf::from("."));
        assert_eq!(
            cli.path_from(Path::new("/tmp/project")),
            PathBuf::from("/tmp/project")
        );
    }

    #[test]
    fn unsupported_or_internal_agents_are_not_accepted() {
        for value in ["embedded", "shell", "null", "unknown"] {
            assert!(Cli::try_parse_from(["forge-solo", "--agent", value]).is_err());
        }
    }

    #[test]
    fn preflight_rejects_redirected_input_or_output() {
        assert!(preflight_with(false, true).is_err());
        assert!(preflight_with(true, false).is_err());
        assert!(preflight_with(false, false).is_err());
        assert_eq!(
            preflight_with(true, true).expect("both handles are terminals"),
            TerminalPreflight {
                stdin_is_terminal: true,
                stdout_is_terminal: true,
            }
        );
    }

    #[test]
    fn preflight_error_names_the_redirected_handle() {
        let error = preflight_with(true, false).expect_err("stdout is redirected");
        let message = error.to_string();
        assert!(message.contains("stdin is a terminal"));
        assert!(message.contains("stdout is not a terminal"));
        assert!(message.contains("run it from a TTY"));
    }
}
