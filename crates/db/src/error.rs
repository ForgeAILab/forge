use std::{io, path::PathBuf};

pub type Result<T> = std::result::Result<T, DbError>;

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[cfg(feature = "test-template")]
    #[error("test database template failed: {0}")]
    TestTemplate(String),

    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("not found")]
    NotFound,

    #[error("Agent Chat turn is not retryable")]
    TurnNotRetryable,

    #[error("another Agent Chat turn is live")]
    ChatTurnLive,

    #[error("dead letter is not a whole event and cannot be replayed")]
    DeadLetterNotReplayable,

    #[error("version conflict")]
    VersionConflict,
    #[error("task_busy: {pending_steps} pending steps; retry after {retry_after_ms} ms")]
    TaskBusy {
        pending_steps: i64,
        retry_after_ms: u64,
    },

    #[error("idempotency key conflicts with a different mutation")]
    IdempotencyConflict,

    #[error("task version conflict: expected {expected}, actual {actual}")]
    TaskVersionConflict { expected: i64, actual: i64 },

    #[error("board revision conflict: expected {expected}, actual {actual}")]
    BoardRevisionConflict { expected: i64, actual: i64 },

    #[error("move operation conflict: {operation_id}")]
    MoveOperationConflict { operation_id: String },

    #[error("move operation is incomplete: {operation_id}")]
    MoveOperationIncomplete { operation_id: String },

    #[error("invalid task move: {0}")]
    InvalidTaskMove(String),

    #[error("invalid transition")]
    InvalidTransition,

    #[error("invalid soft delete")]
    InvalidSoftDelete,

    #[error("agent at capacity")]
    AgentAtCapacity,
    #[error("machine has no available run capacity")]
    MachineAtCapacity,

    #[error("agent {agent_id} is paused")]
    AgentPaused { agent_id: String },

    #[error("project {project_id} is paused")]
    ProjectPaused { project_id: String },

    #[error("repo {repo_id} has active executions or workspace leases")]
    RepoInUse { repo_id: String },

    #[error("project {project_id} has {running_executions} running execution(s) and {active_leases} active workspace lease(s)")]
    ProjectInUse {
        project_id: String,
        running_executions: i64,
        active_leases: i64,
    },

    #[error("dependency gate")]
    DependencyGate,

    #[error("cycle detected")]
    CycleDetected,

    #[error("invalid cursor")]
    InvalidCursor,

    #[error("check constraint failed: {0}")]
    Check(String),

    #[error("review {review_id} has corrupt persisted step_results_json: {reason}")]
    ReviewDetailsCorrupt { review_id: String, reason: String },

    #[error("{scope} execution already running: {execution_id}")]
    ExecutionAlreadyRunning { scope: String, execution_id: String },

    #[error("failed to read migration directory {path}: {source}")]
    ReadMigrationDir { path: PathBuf, source: io::Error },

    #[error("failed to read migration file {path}: {source}")]
    ReadMigrationFile { path: PathBuf, source: io::Error },

    #[error("invalid migration filename {path}")]
    InvalidMigrationFilename { path: PathBuf },

    #[error("invalid migration version in {path}: {source}")]
    InvalidMigrationVersion {
        path: PathBuf,
        source: std::num::ParseIntError,
    },

    #[error(
        "migrations `{first}` and `{second}` both claim version {version}; give the newer one a \
         timestamp version (VYYYYMMDDHHMM__name.sql)"
    )]
    DuplicateMigrationVersion {
        version: i64,
        first: String,
        second: String,
    },

    #[error(
        "the database applied migration {version} as `{applied}`, but this build ships \
         `{bundled}` under that version, so `{bundled}` would never run; refusing to migrate"
    )]
    AppliedMigrationMismatch {
        version: i64,
        applied: String,
        bundled: String,
    },
}

impl DbError {
    /// Worker retry classification, deliberately narrower than "any DB error".
    pub fn is_transient(&self) -> bool {
        match self {
            Self::TaskBusy { .. }
            | Self::VersionConflict
            | Self::TaskVersionConflict { .. }
            | Self::BoardRevisionConflict { .. } => true,
            Self::Sqlx(
                sqlx::Error::PoolTimedOut
                | sqlx::Error::PoolClosed
                | sqlx::Error::WorkerCrashed
                | sqlx::Error::Io(_),
            ) => true,
            Self::Sqlx(sqlx::Error::Database(error)) => error
                .code()
                .and_then(|code| code.parse::<i32>().ok())
                .is_some_and(|code| matches!(code & 0xff, 5 | 6 | 10 | 14)),
            _ => false,
        }
    }
}
