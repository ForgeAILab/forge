//! What a review comes to, and what can stop Forge from reaching it.
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReviewOutcome {
    Passed,
    PassedCiOnly,
    AwaitingHuman,
    AuditorFailed {
        reason: String,
    },
    CiFailed {
        failing_steps: Vec<StepResult>,
    },
    MergeConflict {
        conflict_paths: Vec<PathBuf>,
        conflict_summary: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepResult {
    pub index: usize,
    pub command: String,
    pub exit_code: i32,
    pub stderr_tail: String,
    pub output_tail: String,
    pub started_at: String,
    pub finished_at: String,
}

#[derive(Debug, Error)]
pub enum ReviewError {
    /// No check may be admitted until the owner has a complete answer.
    #[error("check environment attestation unavailable: the server's login shell did not report its environment, so no check can be identified or run")]
    CheckEnvironmentUnavailable,

    #[error("workspace owner unavailable: daemon {daemon_id}")]
    OwnerUnavailable { daemon_id: String },

    #[error("{0}")]
    WorkspaceInfrastructure(String),

    #[error("{0}")]
    Workspace(String),

    #[error(transparent)]
    Db(#[from] db::DbError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Serde(#[from] serde_json::Error),

    #[error(transparent)]
    Executor(#[from] executors::ExecutorError),

    #[error(transparent)]
    Git(#[from] git::GitError),
}
