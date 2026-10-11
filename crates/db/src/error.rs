use std::{io, path::PathBuf};

pub type Result<T> = std::result::Result<T, DbError>;

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error(transparent)]
    PermissionDocument(#[from] operation_registry::authority::PermissionDocumentError),
    #[error("Agent Chat topic request denied: {0:?}")]
    AgentChatTopicDenied(crate::AgentChatTopicDenialReason),
    #[error("LCM timeline ownership changed")]
    LcmTimelineOwned {
        owner: Option<String>,
        generation: i64,
    },
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
    #[error("Stop the daemon before removing this connected machine")]
    MachineConnected,
    #[error("The embedded server machine cannot be removed")]
    LocalMachine,
    #[error("This machine is registered to another user")]
    MachineOwnedByAnotherUser,

    #[error("machine has no available run capacity")]
    MachineAtCapacity,

    #[error("agent {agent_id} is paused")]
    AgentPaused { agent_id: String },

    #[error("project {project_id} is paused")]
    ProjectPaused { project_id: String },

    #[error("repo {repo_id} has active executions or workspace leases")]
    RepoInUse { repo_id: String },

    #[error("project {project_id} has {running_executions} running execution(s), {active_leases} active workspace lease(s) and {live_check_runs} unfinished check run(s)")]
    ProjectInUse {
        project_id: String,
        running_executions: i64,
        active_leases: i64,
        /// Check runs that are queued, running, being cancelled or cleaned,
        /// or whose result is not known yet.
        live_check_runs: i64,
    },

    #[error("{resource} is in use: {reason}")]
    ResourceInUse { resource: String, reason: String },

    #[error("dependency gate")]
    DependencyGate,

    #[error("cycle detected")]
    CycleDetected,

    #[error("invalid cursor")]
    InvalidCursor,

    #[error("check constraint failed: {0}")]
    Check(String),

    /// The Task's stored condition is recognisably a newer build's encoding.
    /// It is never rewritten here, so a statement over it is refused whole.
    #[error("Task {task_id} has a condition written by a newer Forge build; it is quarantined until a build that understands it runs")]
    TaskConditionQuarantined { task_id: String },

    #[error("review {review_id} has corrupt persisted step_results_json: {reason}")]
    ReviewDetailsCorrupt { review_id: String, reason: String },

    /// A stored transition bridge this build cannot decode, for example a kind
    /// written by a newer binary. Never interpreted as an unclassified row.
    #[error("transition {transition_log_id} has an unreadable bridge: {reason}")]
    TransitionBridgeCorrupt {
        transition_log_id: String,
        reason: String,
    },

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
    /// A delete or update was refused because another row still references
    /// the target. Callers surface this as a typed conflict, never a 500.
    pub fn is_foreign_key_violation(&self) -> bool {
        // SQLite reports an immediate `ON DELETE RESTRICT` refusal as
        // SQLITE_CONSTRAINT_TRIGGER (1811), not SQLITE_CONSTRAINT_FOREIGNKEY
        // (787), with the same message.
        matches!(self, Self::Sqlx(sqlx::Error::Database(error))
            if error.kind() == sqlx::error::ErrorKind::ForeignKeyViolation
                || error.message().contains("FOREIGN KEY constraint failed"))
    }

    /// SQLite refused or gave up on a lock (`SQLITE_BUSY`, `SQLITE_LOCKED`
    /// and their extended codes): nothing was written, and the same
    /// statement succeeds once the other writer has committed.
    pub fn is_busy(&self) -> bool {
        matches!(self, Self::Sqlx(sqlx::Error::Database(error))
            if error
                .code()
                .and_then(|code| code.parse::<i32>().ok())
                .is_some_and(|code| matches!(code & 0xff, 5 | 6)))
    }

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
