use std::{fmt, str::FromStr};

use crate::pagination::PageRequest;
use crate::repository::{AdmitAgentChatTurn, CreateAgentActionExecution, CreateCommandReceipt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{sqlite::SqliteRow, Row};

macro_rules! enum_strings {
    ($enum:ident { $($variant:ident => $value:literal),+ $(,)? }) => {
        impl fmt::Display for $enum {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                let value = match self {
                    $(Self::$variant => $value,)+
                };
                formatter.write_str(value)
            }
        }

        impl FromStr for $enum {
            type Err = String;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    _ => Err(format!("invalid {} value: {value}", stringify!($enum))),
                }
            }
        }
    };
}

mod agent;
mod agent_chat;
mod events;
mod execution;
mod identity;
mod media;
mod memory;
mod money;
mod pricing;
mod project;
mod task;
mod usage;
mod workspace;

pub use agent::*;
pub use agent_chat::*;
pub use events::*;
pub use execution::*;
pub use identity::*;
pub use media::*;
pub use memory::*;
pub use money::*;
pub use pricing::*;
pub use project::*;
pub use task::*;
pub use usage::*;
pub use workspace::*;

enum_strings!(RepoLocationOwnerKind {
    Server => "server",
    Daemon => "daemon",
});

enum_strings!(RepoLocationKind {
    PrimaryCheckout => "primary_checkout",
    ManagedClone => "managed_clone",
    SharedMount => "shared_mount",
});

enum_strings!(RepoLocationStatus {
    Unverified => "unverified",
    Ready => "ready",
    Unavailable => "unavailable",
    Invalid => "invalid",
});

enum_strings!(PlacementOwnerKind {
    Server => "server",
    Daemon => "daemon",
});

enum_strings!(PlacementState {
    Reserved => "reserved",
    Preparing => "preparing",
    Ready => "ready",
    Disconnected => "disconnected",
    Cleaning => "cleaning",
    Cleaned => "cleaned",
    Failed => "failed",
});

enum_strings!(PlacementSelectedBy {
    Scheduler => "scheduler",
    Pin => "pin",
    Inherited => "inherited",
    Backfill => "backfill",
});

enum_strings!(PlacementFailureCause {
    PlacementUnavailable => "placement_unavailable",
    PrepareFailed => "prepare_failed",
    OwnerDisconnected => "owner_disconnected",
    OwnerDisconnectedTimeout => "owner_disconnected_timeout",
    OwnerLostExecution => "owner_lost_execution",
    StaleGeneration => "stale_generation",
    WrongOwner => "wrong_owner",
});

enum_strings!(IntegrationPlatform {
    Github => "github",
    Gitea => "gitea",
});

enum_strings!(AgentStatus {
    Idle => "idle",
    Busy => "busy",
    Error => "error",
    Offline => "offline",
});

enum_strings!(WorkspaceStatus {
    Creating => "creating",
    Ready => "ready",
    Error => "error",
    Cleaning => "cleaning",
    Cleaned => "cleaned",
});

enum_strings!(DaemonStatus {
    Online => "online",
    Offline => "offline",
});

enum_strings!(RuntimeStatus {
    Ready => "ready",
    Degraded => "degraded",
    Offline => "offline",
});

enum_strings!(ExecutionStatus {
    Running => "running",
    Completed => "completed",
    Failed => "failed",
    Cancelled => "cancelled",
});

enum_strings!(AgentChatMessageAuthorType {
    User => "user",
    Agent => "agent",
    System => "system",
    Handoff => "handoff",
});

enum_strings!(AgentChatMessageStatus {
    Complete => "complete",
    Failed => "failed",
    Cancelled => "cancelled",
});

enum_strings!(AgentChatTurnState {
    Queued => "queued",
    Leased => "leased",
    AwaitingInput => "awaiting_input",
    RetryWait => "retry_wait",
    Succeeded => "succeeded",
    Failed => "failed",
    Cancelled => "cancelled",
});

enum_strings!(AgentHandoffStatus {
    Pending => "pending",
    Delivered => "delivered",
    Failed => "failed",
    Cancelled => "cancelled",
});

enum_strings!(MemoryKind {
    Observation => "observation",
    Decision => "decision",
    Handoff => "handoff",
    Failure => "failure",
    ReviewResult => "review_result",
    ExecutionSummary => "execution_summary",
    Comment => "comment",
    Transition => "transition",
    Artifact => "artifact",
    Lesson => "lesson",
    ContextPack => "context_pack",
});

enum_strings!(MemorySourceType {
    Execution => "execution",
    Review => "review",
    Comment => "comment",
    Transition => "transition",
});

enum_strings!(MemoryConfidence {
    Confirmed => "confirmed",
    Partial => "partial",
    Unconfirmed => "unconfirmed",
});

enum_strings!(StopReason {
    UserCancelled => "user_cancelled",
    TaskCancelled => "task_cancelled",
    RoleReassigned => "role_reassigned",
    GracefulShutdown => "graceful_shutdown",
    CrashRecovery => "crash_recovery",
    AgentTimeout => "agent_timeout",
    ExecutionStalled => "execution_stalled",
    DaemonDisconnected => "daemon_disconnected",
    ExecutorCancelled => "executor_cancelled",
    ExecutorFailed => "executor_failed",
    LegacyUnknown => "legacy_unknown",
});

enum_strings!(ResumePolicy {
    Auto => "auto",
    Manual => "manual",
    None => "none",
});

enum_strings!(ReviewStatus {
    Running => "running",
    AwaitingHuman => "awaiting_human",
    Passed => "passed",
    Failed => "failed",
    Cancelled => "cancelled",
});

enum_strings!(ProjectHookRunStatus {
    Queued => "queued",
    Running => "running",
    Dispatched => "dispatched",
    Skipped => "skipped",
    Failed => "failed",
    Completed => "completed",
});

enum_strings!(CommentAuthorType {
    User => "user",
    Agent => "agent",
    System => "system",
});

enum_strings!(TerminalSessionStatus {
    Starting => "starting",
    Running => "running",
    Exited => "exited",
    Terminated => "terminated",
    TimedOut => "timed_out",
    Orphaned => "orphaned",
    CleanupTerminated => "cleanup_terminated",
});
