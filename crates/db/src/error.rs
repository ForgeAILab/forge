use std::{io, path::PathBuf};

pub type Result<T> = std::result::Result<T, DbError>;

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("not found")]
    NotFound,

    #[error("version conflict")]
    VersionConflict,

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
}
