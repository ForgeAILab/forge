//! A repository-scoped, presentation-neutral facade for Forge Solo.
//!
//! `forge-solo` is intentionally a shell over the normal Forge services.  It
//! must not make repository calls from its reducer or renderer, and it must
//! not grow a second task/chat state machine.  This module is the narrow
//! boundary between the shared services and that shell:
//!
//! * [`SoloSessionService`] binds every read and command to one owner,
//!   Project, repository, and Project Agent Chat;
//! * [`SoloSessionSnapshot`] contains only bounded, redaction-safe values
//!   suitable for any presentation.
//!
//! Runtime composition and the `forge-solo` crate can construct this facade
//! after bootstrap without introducing a second persistence boundary.

use std::{
    collections::HashSet,
    fmt,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use db::{
    AgentChat, AgentChatMessage, AgentChatMessageAuthorType, AgentChatMessageRepo,
    AgentChatMessageStatus, AgentChatRepo, AgentChatTurnJob, AgentChatTurnJobRepo,
    AgentChatTurnState, AgentRepo, AttentionListQuery, AttentionProjection, AttentionRepo,
    Execution, ExecutionRepo, PageRequest, Project, ProjectAgentBinding, ProjectAgentBindingRepo,
    ProjectCharterRecord, ProjectCharterRevisionRecord, ProjectOrchestrationRepo, ProjectRepo,
    Repo, RepoRepo, Review, ReviewRepo, ReviewStatus, SortBy, SortOrder, SqliteDb, Task,
    TaskListQuery, TaskRepo, TaskRoleAssignment, TaskRoleAssignmentRepo,
};
use events::{EventBus, ForgeEvent};
use executors::{LogEntry, LogKind, LogReader};
use forge_agent_host::{
    InteractionAnswer, InteractionAnswerValue, InteractionBrokerHandle, ProtectedInteractionSummary,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::broadcast;

use services::{
    project_artifact_commands::ProjectCommandAuthorization,
    project_charter_commands::{ProjectCharterApprovalCommand, ProjectCharterCommandService},
    task_service::TaskService,
    Result, ServiceError,
};
use services_agents::{
    agent_chat_service::{
        AgentChatService, CancelAgentChatTurnInput, RetryAgentChatTurnInput,
        SendAgentChatMessageInput,
    },
    agent_chat_turn_worker::AgentChatTurnLogRoot,
};

/// Contract revision for the presentation-neutral Solo facade.  This is a
/// source-level contract marker, not a database migration or a rollout flag.
pub const SOLO_SESSION_CONTRACT_REVISION: &str = "forge.solo-session/v1";

/// Bounds are deliberately kept here instead of relying on terminal width.
/// A headless client and a future UI must receive the same finite projection.
pub const SOLO_MAX_MESSAGES: i64 = 100;
pub const SOLO_MAX_TURNS: i64 = 64;
pub const SOLO_MAX_TASKS: i64 = 100;
pub const SOLO_MAX_ATTENTION: i64 = 100;
pub const SOLO_MAX_EXECUTIONS_PER_TASK: i64 = 32;
pub const SOLO_MAX_ROLES_PER_TASK: i64 = 16;
pub const SOLO_MAX_ACTIVITY_ENTRIES: usize = 100;
pub const SOLO_MAX_ID_CHARS: usize = 256;
pub const SOLO_MAX_TEXT_CHARS: usize = 16_384;
pub const SOLO_MAX_SHORT_TEXT_CHARS: usize = 2_048;
pub const SOLO_MAX_JSON_CHARS: usize = 16_384;
pub const SOLO_MAX_INTERACTION_VALUES: usize = 64;

const PROJECT_CHAT_KIND: &str = "project";
const READY_BINDING_STATE: &str = "active";
const SETUP_BINDING_STATE: &str = "agent_setup_required";
const CHARTER_BACKED_STATUS: &str = "charter_backed";
const LEGACY_UNVERIFIED_STATUS: &str = "legacy_unverified";
const PROJECT_AGENT_POLICY_REVISION: &str = "forge.project-agent-policy/v1";
const APPROVE_PROJECT_CHARTER_PERMISSION: &str = "approve_project_charter";
const PROJECT_CHARTER_APPROVAL_ACTION: &str = "project_charter.approval";

/// Immutable identity of the one Project exposed by a Solo process.
///
/// IDs are opaque references.  In particular, a TUI must never replace one
/// of these fields after bootstrap and then use the facade as a wider query
/// API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloSessionScope {
    pub owner_id: String,
    pub project_id: String,
    pub repo_id: String,
    pub project_chat_id: String,
}

impl SoloSessionScope {
    #[must_use]
    pub fn new(
        owner_id: impl Into<String>,
        project_id: impl Into<String>,
        repo_id: impl Into<String>,
        project_chat_id: impl Into<String>,
    ) -> Self {
        Self {
            owner_id: owner_id.into(),
            project_id: project_id.into(),
            repo_id: repo_id.into(),
            project_chat_id: project_chat_id.into(),
        }
    }

    fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("owner_id", self.owner_id.as_str()),
            ("project_id", self.project_id.as_str()),
            ("repo_id", self.repo_id.as_str()),
            ("project_chat_id", self.project_chat_id.as_str()),
        ] {
            path_component_id(name, value.to_owned())?;
        }
        Ok(())
    }
}

/// Construction-time dependencies supplied by the shared runtime builder.
///
/// The optional interaction broker is intentional.  The current protected
/// interaction API needs the owning Agent Runtime session row in addition to
/// the Project Chat turn.  Until the runtime builder exposes that exact
/// session handle, commands fail closed with a typed `Unsupported` or
/// `NotReady` outcome rather than reaching into protected tables here.
pub struct SoloSessionDependencies {
    pub db: Arc<SqliteDb>,
    pub event_bus: Arc<EventBus>,
    pub agent_chat_service: Arc<AgentChatService<SqliteDb>>,
    pub task_service: Arc<TaskService>,
    pub turn_logs: AgentChatTurnLogRoot,
    interaction_broker: Option<InteractionBrokerHandle>,
    forge_session_id: Option<String>,
    /// Workspace root containing the canonical `.forge/logs/<project>/<task>`
    /// execution-log tree.  It is optional only so callers that do not expose
    /// Task activity can still construct the session; reads fail closed when
    /// it is absent.
    execution_logs_root: Option<PathBuf>,
}

impl SoloSessionDependencies {
    #[must_use]
    pub fn new(
        db: Arc<SqliteDb>,
        event_bus: Arc<EventBus>,
        agent_chat_service: Arc<AgentChatService<SqliteDb>>,
        task_service: Arc<TaskService>,
        turn_logs: AgentChatTurnLogRoot,
    ) -> Self {
        Self {
            db,
            event_bus,
            agent_chat_service,
            task_service,
            turn_logs,
            interaction_broker: None,
            forge_session_id: None,
            execution_logs_root: None,
        }
    }

    /// Attach the configured workspace root used by the existing Task log
    /// writer.  The session accepts a persisted execution path only when it
    /// remains under this root and names the exact bound execution.
    #[must_use]
    pub fn with_execution_logs_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.execution_logs_root = Some(root.into());
        self
    }
}

/// Typed finite availability result used by operations that depend on a
/// runtime component which may not be wired in a given assembly mode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SoloOperationOutcome<T> {
    Applied(T),
    NotReady { operation: String, reason: String },
    Unsupported { operation: String, reason: String },
}

impl<T> SoloOperationOutcome<T> {
    fn not_ready(operation: &'static str, reason: &'static str) -> Self {
        Self::NotReady {
            operation: operation.to_owned(),
            reason: reason.to_owned(),
        }
    }

    fn unsupported(operation: &'static str, reason: &'static str) -> Self {
        Self::Unsupported {
            operation: operation.to_owned(),
            reason: reason.to_owned(),
        }
    }
}

/// Whether a runtime-backed capability is available to the current facade.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SoloCapability {
    Available,
    NotReady { reason: String },
    Unsupported { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloCapabilities {
    pub runtime_interactions: SoloCapability,
}

/// Project readiness is derived only from typed Project/binding/Charter
/// records.  Unknown persisted values remain unknown rather than being
/// interpreted as operational.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SoloProjectReadiness {
    SetupRequired,
    Operational,
    Paused,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloProjectSnapshot {
    pub id: String,
    pub name: String,
    pub version: i64,
    pub readiness: SoloProjectReadiness,
    pub paused_at: Option<String>,
    pub charter_status: String,
    pub charter_setup_required: bool,
    pub binding: Option<SoloProjectAgentBindingSnapshot>,
    pub charter: Option<SoloCharterSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloProjectAgentBindingSnapshot {
    pub id: String,
    pub state: String,
    pub identity_id: Option<String>,
    pub profile_id: Option<String>,
    pub operating_skill_revision_id: Option<String>,
    pub policy_revision: String,
    pub policy_digest: String,
    pub version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloCharterSnapshot {
    pub id: String,
    pub project_id: Option<String>,
    pub version: i64,
    pub project_mode: String,
    pub maturity: String,
    pub lifecycle: String,
    pub current_draft_revision_id: Option<String>,
    pub current_approved_revision_id: Option<String>,
    pub current_draft: Option<SoloCharterRevisionSnapshot>,
    pub current_approved: Option<SoloCharterRevisionSnapshot>,
    pub approval_target: Option<SoloCharterApprovalTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloCharterRevisionSnapshot {
    pub id: String,
    pub charter_id: String,
    pub revision: i64,
    pub lifecycle: String,
    pub render_version: String,
    pub content_digest: String,
    pub rendered_digest: String,
    pub rendered_view: String,
    pub change_summary: String,
    /// Typed identity fields copied from the canonical Charter content. They
    /// let an approval adapter send the exact name/slug target without
    /// interpreting the rendered Charter prose.
    pub working_name: Option<String>,
    pub slug_proposal: Option<String>,
}

/// Exact server-owned Charter target shown by the TUI.  Approval commands
/// accept this complete value and compare it to a fresh authoritative target
/// before invoking `ProjectCharterCommandService`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloCharterApprovalTarget {
    pub kind: SoloCharterApprovalKind,
    pub project_id: String,
    pub charter_id: String,
    pub revision_id: String,
    pub content_digest: String,
    pub rendered_digest: String,
    pub expected_charter_version: i64,
    pub expected_project_version: i64,
    pub approved_project_name: String,
    pub approved_project_slug: Option<String>,
    pub project_mode: String,
    pub selected_project_agent_identity_id: String,
    pub selected_project_agent_profile_revision_id: String,
    pub selected_project_agent_operating_skill_revision: String,
    pub selected_project_agent_policy_digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SoloCharterApprovalKind {
    Adoption,
    Amendment,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloRepositorySnapshot {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub local_path: Option<String>,
    pub default_branch: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloChatSnapshot {
    pub id: String,
    pub status: String,
    pub message_count: i64,
    pub last_message_at: Option<String>,
    pub version: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SoloMessageAuthor {
    User,
    Agent,
    System,
    Handoff,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SoloMessageStatus {
    Complete,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloChatMessageSnapshot {
    pub id: String,
    pub chat_id: String,
    pub sequence: i64,
    pub author: SoloMessageAuthor,
    pub author_id: Option<String>,
    /// Safe content is always bounded.  A protected or suspicious record is
    /// represented by the fixed marker and never by its original body.
    pub content: String,
    pub content_redacted: bool,
    pub status: SoloMessageStatus,
    pub outcome: Option<String>,
    pub model: Option<String>,
    pub response_turn_id: Option<String>,
    pub correlation_id: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SoloTurnStatus {
    Queued,
    Leased,
    AwaitingInput,
    RetryWait,
    Succeeded,
    Failed,
    Cancelled,
}

impl SoloTurnStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloTurnSnapshot {
    pub id: String,
    pub chat_id: String,
    pub triggering_message_id: String,
    pub responder_identity_id: Option<String>,
    pub profile_id: Option<String>,
    pub status: SoloTurnStatus,
    pub pending_interaction_id: Option<String>,
    pub attempt_count: i64,
    pub max_attempts: i64,
    pub lease_expires_at: Option<String>,
    pub next_attempt_at: Option<String>,
    pub response_message_id: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub retry_action: Option<api_types::RetryTurnAction>,
    pub correlation_id: String,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloTaskInterruption {
    pub failure_kind: Option<api_types::FailureKind>,
    pub reason: Option<String>,
    pub source: Option<String>,
    pub execution_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloTaskExecutionSnapshot {
    pub id: String,
    pub task_id: String,
    pub role: String,
    pub agent_id: Option<String>,
    pub status: String,
    pub stop_reason: Option<String>,
    pub agent_session_id: Option<String>,
    pub before_sha: Option<String>,
    pub after_sha: Option<String>,
    pub summary: Option<String>,
    pub error: Option<String>,
    pub logs_available: bool,
    pub execution_version: i64,
    pub last_activity_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloCheckSnapshot {
    pub index: i64,
    pub command: Option<String>,
    pub exit_code: Option<i64>,
    pub success: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SoloReviewStatus {
    Running,
    AwaitingHuman,
    Passed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloReviewSnapshot {
    pub id: String,
    pub task_id: String,
    pub execution_id: String,
    pub attempt_number: i64,
    pub status: SoloReviewStatus,
    pub checks: Vec<SoloCheckSnapshot>,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloTaskRoleSnapshot {
    pub role: String,
    pub assignee_type: Option<String>,
    pub assignee_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloTaskSnapshot {
    pub id: String,
    pub project_id: String,
    pub title: String,
    pub status: String,
    pub version: i64,
    pub assignee_type: Option<String>,
    pub assignee_id: Option<String>,
    pub priority: i64,
    pub interruption: Option<SoloTaskInterruption>,
    pub available_actions: Vec<api_types::Offer>,
    pub roles: Vec<SoloTaskRoleSnapshot>,
    pub executions: Vec<SoloTaskExecutionSnapshot>,
    pub latest_review: Option<SoloReviewSnapshot>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SoloAttentionStatus {
    Open,
    Acknowledged,
    Snoozed,
    Resolved,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloAttentionSnapshot {
    pub id: String,
    pub attention_type: String,
    pub scope_type: String,
    pub scope_id: String,
    pub source_event_id: String,
    pub priority: i64,
    pub status: SoloAttentionStatus,
    pub summary: String,
    pub details_json: Option<String>,
    pub recommended_action: String,
    pub version: i64,
    pub occurred_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloInteractionSnapshot {
    pub id: String,
    pub session_id: String,
    pub interaction_kind: String,
    pub prompt_redacted: String,
    pub status: String,
    pub expires_at: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloSessionSnapshot {
    pub contract_revision: String,
    pub scope: SoloSessionScope,
    pub project: SoloProjectSnapshot,
    pub repository: SoloRepositorySnapshot,
    pub chat: SoloChatSnapshot,
    pub messages: Vec<SoloChatMessageSnapshot>,
    pub messages_has_more: bool,
    pub turns: Vec<SoloTurnSnapshot>,
    pub tasks: Vec<SoloTaskSnapshot>,
    pub attention: Vec<SoloAttentionSnapshot>,
    pub interactions: Vec<SoloInteractionSnapshot>,
    pub capabilities: SoloCapabilities,
    pub authoritative_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloRefreshLimits {
    pub messages: i64,
    pub turns: i64,
    pub tasks: i64,
    pub attention: i64,
}

impl Default for SoloRefreshLimits {
    fn default() -> Self {
        Self {
            messages: SOLO_MAX_MESSAGES,
            turns: SOLO_MAX_TURNS,
            tasks: SOLO_MAX_TASKS,
            attention: SOLO_MAX_ATTENTION,
        }
    }
}

impl SoloRefreshLimits {
    fn bounded(self) -> Result<Self> {
        Ok(Self {
            messages: bounded_limit("messages", self.messages, SOLO_MAX_MESSAGES)?,
            turns: bounded_limit("turns", self.turns, SOLO_MAX_TURNS)?,
            tasks: bounded_limit("tasks", self.tasks, SOLO_MAX_TASKS)?,
            attention: bounded_limit("attention", self.attention, SOLO_MAX_ATTENTION)?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SoloActivityCursor {
    pub next_sequence: u64,
    pub file_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloActivityPage {
    pub entries: Vec<SoloActivityEntry>,
    pub cursor: SoloActivityCursor,
    pub has_more: bool,
    pub reset: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SoloActivityKind {
    AttemptDivider,
    ToolCall,
    ToolResult,
    AssistantDelta,
    Assistant,
    Thinking,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloActivityEntry {
    pub sequence: u64,
    pub kind: SoloActivityKind,
    pub tool_name: Option<String>,
    pub argument_keys: Vec<String>,
    pub summary: Option<String>,
    pub text: Option<String>,
    pub attempt: Option<i64>,
    pub collapsed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloSendMessageInput {
    pub content: String,
    pub dedupe_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloSendMessageResult {
    pub message: SoloChatMessageSnapshot,
    pub turn: SoloTurnSnapshot,
}

/// Retry one failed turn with an optimistic source version and a stable
/// command idempotency key.  The canonical Agent Chat service derives the
/// durable child identity from the source turn and next attempt; the caller
/// key protects the presentation command boundary from unbounded or empty
/// replay tokens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloRetryTurnInput {
    pub turn_job_id: String,
    pub expected_version: i64,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloInteractionAnswerInput {
    pub interaction_id: String,
    pub expected_version: i64,
    pub values: Vec<SoloInteractionAnswerValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SoloInteractionAnswerValue {
    Choice {
        question_id: String,
        choice_id: String,
    },
    FreeForm {
        question_id: String,
        value: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloReviewDecisionInput {
    pub task_id: String,
    pub expected_task_version: i64,
    pub decision: SoloReviewDecision,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SoloReviewDecision {
    Accept,
    RequestChanges,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloReviewDecisionResult {
    pub task_id: String,
    pub decision: SoloReviewDecision,
    pub committed_task_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloCharterApprovalAuthorization {
    pub authorization_event_id: String,
    pub authorization_basis: String,
    pub authorization_occurred_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloCharterApprovalInput {
    pub target: SoloCharterApprovalTarget,
    pub idempotency_key: String,
    pub authorization: SoloCharterApprovalAuthorization,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloCharterApprovalResult {
    pub approval_id: String,
    pub project_id: String,
    pub project_version: i64,
    pub project_charter_id: String,
    pub project_charter_revision_id: String,
    pub project_agent_binding_id: String,
    pub project_chat_id: String,
}

struct AuthorizedScopeRecords {
    project: Project,
    repo: Repo,
    chat: AgentChat,
    binding: Option<ProjectAgentBinding>,
}

fn turn_belongs_to_scope(job: &AgentChatTurnJob, scope: &SoloSessionScope) -> bool {
    job.chat_id == scope.project_chat_id
        && job.canonical_scope_type == "agent_chat"
        && job.canonical_scope_id == scope.project_chat_id
}

fn repository_snapshot(repo: &Repo) -> SoloRepositorySnapshot {
    SoloRepositorySnapshot {
        id: safe_identifier(&repo.id),
        project_id: safe_identifier(&repo.project_id),
        name: safe_diagnostic(&repo.name).unwrap_or_else(|| "[redacted]".to_owned()),
        local_path: repo
            .local_path
            .as_deref()
            .map(|path| safe_text(path, SOLO_MAX_SHORT_TEXT_CHARS)),
        default_branch: safe_identifier(&repo.default_branch),
    }
}

fn chat_snapshot(chat: &AgentChat) -> SoloChatSnapshot {
    SoloChatSnapshot {
        id: safe_identifier(&chat.id),
        status: safe_identifier(&chat.status),
        message_count: chat.message_count,
        last_message_at: chat
            .last_message_at
            .as_deref()
            .map(|value| safe_text(value, SOLO_MAX_SHORT_TEXT_CHARS)),
        version: chat.version,
    }
}

fn binding_snapshot(binding: &ProjectAgentBinding) -> SoloProjectAgentBindingSnapshot {
    SoloProjectAgentBindingSnapshot {
        id: safe_identifier(&binding.id),
        state: safe_identifier(&binding.state),
        identity_id: binding.identity_id.as_deref().map(safe_identifier),
        profile_id: binding.profile_id.as_deref().map(safe_identifier),
        operating_skill_revision_id: binding
            .operating_skill_revision_id
            .as_deref()
            .map(safe_identifier),
        policy_revision: safe_identifier(&binding.policy_revision),
        policy_digest: safe_identifier(&binding.policy_digest),
        version: binding.version,
    }
}

fn project_readiness(
    project: &Project,
    binding: Option<&ProjectAgentBinding>,
) -> SoloProjectReadiness {
    if project.paused_at.is_some() {
        return SoloProjectReadiness::Paused;
    }
    match (
        project.charter_status.as_str(),
        project.charter_setup_required,
        binding.map(|binding| binding.state.as_str()),
    ) {
        (CHARTER_BACKED_STATUS, false, Some(READY_BINDING_STATE)) => {
            SoloProjectReadiness::Operational
        }
        (LEGACY_UNVERIFIED_STATUS, true, _)
        | (LEGACY_UNVERIFIED_STATUS, false, Some(SETUP_BINDING_STATE))
        | (LEGACY_UNVERIFIED_STATUS, false, None) => SoloProjectReadiness::SetupRequired,
        _ => SoloProjectReadiness::Unknown,
    }
}

fn message_snapshot(message: &AgentChatMessage) -> SoloChatMessageSnapshot {
    let (content, content_redacted) = visible_content(&message.content, &message.sensitivity);
    SoloChatMessageSnapshot {
        id: safe_identifier(&message.id),
        chat_id: safe_identifier(&message.chat_id),
        sequence: message.sequence,
        author: match message.author_type {
            AgentChatMessageAuthorType::User => SoloMessageAuthor::User,
            AgentChatMessageAuthorType::Agent => SoloMessageAuthor::Agent,
            AgentChatMessageAuthorType::System => SoloMessageAuthor::System,
            AgentChatMessageAuthorType::Handoff => SoloMessageAuthor::Handoff,
        },
        author_id: message.author_id.as_deref().map(safe_identifier),
        content,
        content_redacted,
        status: match message.status {
            AgentChatMessageStatus::Complete => SoloMessageStatus::Complete,
            AgentChatMessageStatus::Failed => SoloMessageStatus::Failed,
            AgentChatMessageStatus::Cancelled => SoloMessageStatus::Cancelled,
        },
        outcome: message.outcome.as_deref().map(safe_identifier),
        model: message
            .model
            .as_deref()
            .map(|value| safe_text(value, SOLO_MAX_SHORT_TEXT_CHARS)),
        response_turn_id: message.source_id.as_deref().map(safe_identifier),
        correlation_id: safe_identifier(&message.correlation_id),
        created_at: safe_text(&message.created_at, SOLO_MAX_SHORT_TEXT_CHARS),
    }
}

fn turn_snapshot(job: &AgentChatTurnJob) -> SoloTurnSnapshot {
    SoloTurnSnapshot {
        id: safe_identifier(&job.id),
        chat_id: safe_identifier(&job.chat_id),
        triggering_message_id: safe_identifier(&job.triggering_message_id),
        responder_identity_id: job.responder_identity_id.as_deref().map(safe_identifier),
        profile_id: job.profile_id.as_deref().map(safe_identifier),
        status: match job.status {
            AgentChatTurnState::Queued => SoloTurnStatus::Queued,
            AgentChatTurnState::Leased => SoloTurnStatus::Leased,
            AgentChatTurnState::AwaitingInput => SoloTurnStatus::AwaitingInput,
            AgentChatTurnState::RetryWait => SoloTurnStatus::RetryWait,
            AgentChatTurnState::Succeeded => SoloTurnStatus::Succeeded,
            AgentChatTurnState::Failed => SoloTurnStatus::Failed,
            AgentChatTurnState::Cancelled => SoloTurnStatus::Cancelled,
        },
        pending_interaction_id: job.pending_interaction_id.as_deref().map(safe_identifier),
        attempt_count: job.attempt_count,
        max_attempts: job.max_attempts,
        lease_expires_at: job
            .leased_until
            .as_deref()
            .map(|value| safe_text(value, SOLO_MAX_SHORT_TEXT_CHARS)),
        next_attempt_at: job
            .next_attempt_at
            .as_deref()
            .map(|value| safe_text(value, SOLO_MAX_SHORT_TEXT_CHARS)),
        response_message_id: job.response_message_id.as_deref().map(safe_identifier),
        error_code: job.error_code.as_deref().map(safe_identifier),
        error_message: job.error_message.as_deref().and_then(safe_diagnostic),
        retry_action: job.retry_action(),
        correlation_id: safe_identifier(&job.correlation_id),
        version: job.version,
        created_at: safe_text(&job.created_at, SOLO_MAX_SHORT_TEXT_CHARS),
        updated_at: safe_text(&job.updated_at, SOLO_MAX_SHORT_TEXT_CHARS),
    }
}

fn task_interruption(task: &Task) -> Option<SoloTaskInterruption> {
    let read = task.condition.read();
    if let Some(annotation) = read.diagnostic {
        return Some(SoloTaskInterruption {
            failure_kind: Some(annotation.annotation_type),
            reason: safe_optional_diagnostic(
                annotation
                    .message
                    .as_deref()
                    .or(Some(annotation.blocking_reason.as_str())),
            ),
            source: None,
            execution_id: annotation
                .blocked_execution_id
                .as_deref()
                .map(safe_identifier),
        });
    }
    read.interruption.map(|metadata| SoloTaskInterruption {
        failure_kind: metadata.kind,
        reason: safe_optional_diagnostic(Some(metadata.reason.as_str())),
        source: metadata.source.and_then(|source| safe_diagnostic(&source)),
        execution_id: metadata.execution_id.as_deref().map(safe_identifier),
    })
}

fn execution_snapshot(execution: &Execution) -> SoloTaskExecutionSnapshot {
    SoloTaskExecutionSnapshot {
        id: safe_identifier(&execution.id),
        task_id: safe_identifier(&execution.task_id),
        role: safe_identifier(&execution.role),
        agent_id: execution.agent_id.as_deref().map(safe_identifier),
        status: safe_identifier(&execution.status.to_string()),
        stop_reason: execution
            .stop_reason
            .as_ref()
            .map(ToString::to_string)
            .map(|value| safe_identifier(&value)),
        agent_session_id: execution.agent_session_id.as_deref().map(safe_identifier),
        before_sha: execution.before_sha.as_deref().map(safe_identifier),
        after_sha: execution.after_sha.as_deref().map(safe_identifier),
        summary: execution.summary.as_deref().and_then(safe_diagnostic),
        error: execution.error.as_deref().and_then(safe_diagnostic),
        logs_available: execution
            .logs_path
            .as_ref()
            .is_some_and(|path| !path.trim().is_empty()),
        execution_version: execution.execution_version,
        last_activity_at: execution
            .last_activity_at
            .as_deref()
            .map(|value| safe_text(value, SOLO_MAX_SHORT_TEXT_CHARS)),
        created_at: safe_text(&execution.created_at, SOLO_MAX_SHORT_TEXT_CHARS),
        updated_at: safe_text(&execution.updated_at, SOLO_MAX_SHORT_TEXT_CHARS),
    }
}

fn role_snapshot(role: TaskRoleAssignment) -> SoloTaskRoleSnapshot {
    SoloTaskRoleSnapshot {
        role: safe_identifier(&role.role_name),
        assignee_type: role
            .assignee_type
            .map(|value| safe_identifier(&value.to_string())),
        assignee_id: role.assignee_id.as_deref().map(safe_identifier),
    }
}

fn review_snapshot(review: &Review) -> SoloReviewSnapshot {
    SoloReviewSnapshot {
        id: safe_identifier(&review.id),
        task_id: safe_identifier(&review.task_id),
        execution_id: safe_identifier(&review.execution_id),
        attempt_number: review.attempt_number,
        status: match review.status {
            ReviewStatus::Running => SoloReviewStatus::Running,
            ReviewStatus::AwaitingHuman => SoloReviewStatus::AwaitingHuman,
            ReviewStatus::Passed => SoloReviewStatus::Passed,
            ReviewStatus::Failed => SoloReviewStatus::Failed,
            ReviewStatus::Cancelled => SoloReviewStatus::Cancelled,
        },
        checks: parse_review_checks(&review.step_results_json),
        started_at: safe_text(&review.started_at, SOLO_MAX_SHORT_TEXT_CHARS),
        finished_at: review
            .finished_at
            .as_deref()
            .map(|value| safe_text(value, SOLO_MAX_SHORT_TEXT_CHARS)),
        updated_at: safe_text(&review.updated_at, SOLO_MAX_SHORT_TEXT_CHARS),
    }
}

fn parse_review_checks(raw: &str) -> Vec<SoloCheckSnapshot> {
    if raw.chars().count() > SOLO_MAX_JSON_CHARS {
        return Vec::new();
    }
    let Ok(value) = serde_json::from_str::<Value>(raw) else {
        return Vec::new();
    };
    let Some(checks) = value
        .get("ci_steps")
        .and_then(Value::as_array)
        .or_else(|| value.as_array())
    else {
        return Vec::new();
    };
    checks
        .iter()
        .enumerate()
        .take(64)
        .filter_map(|(index, check)| {
            let object = check.as_object()?;
            let index = object
                .get("index")
                .and_then(Value::as_i64)
                .unwrap_or(index as i64);
            let command = object
                .get("command")
                .and_then(Value::as_str)
                .and_then(safe_diagnostic);
            let exit_code = object.get("exit_code").and_then(Value::as_i64);
            // Durable review CI rows use exit_code as the canonical
            // outcome today; older/custom producers may also supply the
            // explicit boolean. Keep both shapes visible in Solo instead of
            // rendering a successful completed check as pending.
            let success = object
                .get("success")
                .and_then(Value::as_bool)
                .or_else(|| exit_code.map(|code| code == 0));
            Some(SoloCheckSnapshot {
                index,
                command,
                exit_code,
                success,
            })
        })
        .collect()
}

fn attention_snapshot(item: AttentionProjection) -> SoloAttentionSnapshot {
    SoloAttentionSnapshot {
        id: safe_identifier(&item.id),
        attention_type: safe_identifier(&item.attention_type),
        scope_type: safe_identifier(&item.scope_type),
        scope_id: safe_identifier(&item.scope_id),
        source_event_id: safe_identifier(&item.source_event_id),
        priority: item.priority,
        status: match item.status.as_str() {
            "open" | "pending" => SoloAttentionStatus::Open,
            "acknowledged" => SoloAttentionStatus::Acknowledged,
            "snoozed" => SoloAttentionStatus::Snoozed,
            "resolved" => SoloAttentionStatus::Resolved,
            _ => SoloAttentionStatus::Unknown,
        },
        summary: safe_diagnostic(&item.summary).unwrap_or_else(|| "[redacted]".to_owned()),
        details_json: safe_json(&item.details_json),
        recommended_action: safe_identifier(&item.recommended_action),
        version: item.version,
        occurred_at: safe_text(&item.occurred_at, SOLO_MAX_SHORT_TEXT_CHARS),
        updated_at: safe_text(&item.updated_at, SOLO_MAX_SHORT_TEXT_CHARS),
    }
}

fn interaction_snapshot(summary: &ProtectedInteractionSummary) -> SoloInteractionSnapshot {
    SoloInteractionSnapshot {
        id: safe_identifier(&summary.id),
        session_id: safe_identifier(&summary.session_id),
        interaction_kind: safe_identifier(&summary.interaction_kind),
        prompt_redacted: safe_text(&summary.prompt_redacted, SOLO_MAX_SHORT_TEXT_CHARS),
        status: safe_identifier(&summary.status),
        expires_at: summary
            .expires_at
            .as_deref()
            .map(|value| safe_text(value, SOLO_MAX_SHORT_TEXT_CHARS)),
        version: summary.version,
        created_at: safe_text(&summary.created_at, SOLO_MAX_SHORT_TEXT_CHARS),
        updated_at: safe_text(&summary.updated_at, SOLO_MAX_SHORT_TEXT_CHARS),
    }
}

fn charter_revision_snapshot(
    revision: &ProjectCharterRevisionRecord,
) -> SoloCharterRevisionSnapshot {
    let (working_name, slug_proposal) = (revision.content_json.chars().count()
        <= SOLO_MAX_JSON_CHARS)
        .then(|| {
            serde_json::from_str::<api_types::ProjectCharterContent>(&revision.content_json).ok()
        })
        .flatten()
        .and_then(|content| {
            let working_name =
                safe_exact_text(&content.identity.working_name, SOLO_MAX_SHORT_TEXT_CHARS)?;
            let slug_proposal = match content.identity.slug_proposal.as_deref() {
                Some(slug) => Some(safe_exact_text(slug, SOLO_MAX_SHORT_TEXT_CHARS)?),
                None => None,
            };
            Some((Some(working_name), slug_proposal))
        })
        .unwrap_or((None, None));
    SoloCharterRevisionSnapshot {
        id: safe_identifier(&revision.id),
        charter_id: safe_identifier(&revision.charter_id),
        revision: revision.revision,
        lifecycle: safe_identifier(&revision.lifecycle),
        render_version: safe_identifier(&revision.render_version),
        content_digest: safe_identifier(&revision.content_digest),
        rendered_digest: safe_identifier(&revision.rendered_digest),
        rendered_view: safe_diagnostic(&revision.rendered_view)
            .map_or_else(String::new, |value| safe_text(&value, SOLO_MAX_JSON_CHARS)),
        change_summary: safe_diagnostic(&revision.change_summary)
            .unwrap_or_else(|| "[redacted]".to_owned()),
        working_name,
        slug_proposal,
    }
}

fn interaction_answer_value(value: SoloInteractionAnswerValue) -> InteractionAnswerValue {
    match value {
        SoloInteractionAnswerValue::Choice {
            question_id,
            choice_id,
        } => InteractionAnswerValue::Choice {
            question_id,
            choice_id,
        },
        SoloInteractionAnswerValue::FreeForm { question_id, value } => {
            InteractionAnswerValue::FreeForm { question_id, value }
        }
    }
}

fn map_interaction_error(error: forge_agent_host::AgentHostError) -> ServiceError {
    match error {
        forge_agent_host::AgentHostError::VersionConflict => {
            ServiceError::Db(db::DbError::VersionConflict)
        }
        forge_agent_host::AgentHostError::SessionNotFound => {
            ServiceError::conflict("the protected runtime interaction session is unavailable")
        }
        forge_agent_host::AgentHostError::Authority(_) => ServiceError::conflict(
            "the protected runtime interaction is unavailable or its version changed",
        ),
        forge_agent_host::AgentHostError::Unsupported(_) => ServiceError::invalid_operation(
            "the protected runtime interaction operation is unsupported",
        ),
        _ => ServiceError::Domain("protected runtime interaction failed".to_owned()),
    }
}

fn validate_approval_input(input: &SoloCharterApprovalInput) -> Result<()> {
    for (name, value) in [
        ("approval idempotency_key", input.idempotency_key.as_str()),
        (
            "authorization_event_id",
            input.authorization.authorization_event_id.as_str(),
        ),
        (
            "authorization_basis",
            input.authorization.authorization_basis.as_str(),
        ),
        (
            "authorization_occurred_at",
            input.authorization.authorization_occurred_at.as_str(),
        ),
    ] {
        bounded_required(name, value, SOLO_MAX_ID_CHARS)?;
    }
    let target = &input.target;
    for (name, value) in [
        ("target.project_id", target.project_id.as_str()),
        ("target.charter_id", target.charter_id.as_str()),
        ("target.revision_id", target.revision_id.as_str()),
        ("target.content_digest", target.content_digest.as_str()),
        ("target.rendered_digest", target.rendered_digest.as_str()),
        ("target.project_mode", target.project_mode.as_str()),
        (
            "target.selected_project_agent_identity_id",
            target.selected_project_agent_identity_id.as_str(),
        ),
        (
            "target.selected_project_agent_profile_revision_id",
            target.selected_project_agent_profile_revision_id.as_str(),
        ),
        (
            "target.selected_project_agent_operating_skill_revision",
            target
                .selected_project_agent_operating_skill_revision
                .as_str(),
        ),
        (
            "target.selected_project_agent_policy_digest",
            target.selected_project_agent_policy_digest.as_str(),
        ),
    ] {
        bounded_required(name, value, SOLO_MAX_ID_CHARS)?;
    }
    bounded_required(
        "target.approved_project_name",
        target.approved_project_name.as_str(),
        SOLO_MAX_SHORT_TEXT_CHARS,
    )?;
    if let Some(slug) = target.approved_project_slug.as_deref() {
        bounded_required(
            "target.approved_project_slug",
            slug,
            SOLO_MAX_SHORT_TEXT_CHARS,
        )?;
    }
    if target.expected_charter_version < 1 || target.expected_project_version < 1 {
        return Err(ServiceError::invalid_operation(
            "Charter approval versions must be positive",
        ));
    }
    Ok(())
}

async fn read_activity_path(
    path: &Path,
    root: &Path,
    expected_execution_id: &str,
    cursor: SoloActivityCursor,
    limit: usize,
) -> Result<SoloActivityPage> {
    validate_activity_limit(limit)?;
    ensure_scoped_log_path(root, path).await?;
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(SoloActivityPage {
                entries: Vec::new(),
                cursor,
                has_more: false,
                reset: false,
            });
        }
        Err(_) => {
            return Err(ServiceError::invalid_operation(
                "activity log is not readable",
            ));
        }
    };
    if !metadata.file_type().is_file() {
        return Err(ServiceError::invalid_operation(
            "activity log is not a regular file",
        ));
    }
    let reset = metadata.len() < cursor.file_size;
    let read_from = if reset { 0 } else { cursor.next_sequence };
    let result = LogReader::read(path, read_from, limit)
        .await
        .map_err(|_| ServiceError::invalid_operation("activity log is not readable"))?;
    // The reader API is path based, so check again after it has opened/read
    // the file. This closes the normal symlink-swap window and turns any
    // replacement with a non-regular file into a fail-closed result.
    ensure_scoped_log_path(root, path).await?;
    let after = tokio::fs::symlink_metadata(path)
        .await
        .map_err(|_| ServiceError::invalid_operation("activity log is not a regular file"))?;
    if !after.file_type().is_file() {
        return Err(ServiceError::invalid_operation(
            "activity log is not a regular file",
        ));
    }
    let next_sequence = result.next_sequence.unwrap_or(read_from).max(read_from);
    let entries = result
        .entries
        .iter()
        .filter(|entry| entry.execution_id.as_str() == expected_execution_id)
        .filter_map(activity_entry)
        .collect();
    Ok(SoloActivityPage {
        entries,
        cursor: SoloActivityCursor {
            next_sequence,
            file_size: metadata.len(),
        },
        has_more: result.has_more,
        reset,
    })
}

fn validate_activity_limit(limit: usize) -> Result<()> {
    if limit == 0 || limit > SOLO_MAX_ACTIVITY_ENTRIES {
        return Err(ServiceError::invalid_operation(
            "activity page limit is outside the bounded range",
        ));
    }
    Ok(())
}

/// Keep path-bearing IDs as strict single filename components. Database IDs
/// are UUIDs in production, while this deliberately permits simple fixture
/// IDs used by service tests without permitting either slash, dot traversal,
/// or platform-specific path prefixes.
fn path_component_id(name: &str, value: String) -> Result<String> {
    let value = bounded_owned(name, value, SOLO_MAX_ID_CHARS)?;
    if value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || Path::new(&value).is_absolute()
        || !value
            .bytes()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, b'-' | b'_'))
    {
        return Err(ServiceError::invalid_operation(format!(
            "{name} is not a safe path identifier"
        )));
    }
    Ok(value)
}

fn validate_retry_input(input: SoloRetryTurnInput) -> Result<(String, String, i64)> {
    let turn_job_id = path_component_id("turn_job_id", input.turn_job_id)?;
    let idempotency_key = bounded_owned(
        "turn retry idempotency_key",
        input.idempotency_key,
        SOLO_MAX_ID_CHARS,
    )?;
    if input.expected_version < 1 {
        return Err(ServiceError::invalid_operation(
            "turn expected_version must be positive",
        ));
    }
    Ok((turn_job_id, idempotency_key, input.expected_version))
}

fn scoped_execution_log_path(
    root: &Path,
    project_id: &str,
    task_id: &str,
    execution_id: &str,
    persisted_path: &str,
) -> Result<PathBuf> {
    bounded_required(
        "execution logs_path",
        persisted_path,
        SOLO_MAX_SHORT_TEXT_CHARS,
    )?;
    if persisted_path.chars().any(char::is_control) {
        return Err(ServiceError::invalid_operation(
            "execution logs_path contains control characters",
        ));
    }
    let project_id = path_component_id("project_id", project_id.to_owned())?;
    let task_id = path_component_id("task_id", task_id.to_owned())?;
    let execution_id = path_component_id("execution_id", execution_id.to_owned())?;
    let persisted = Path::new(persisted_path);
    let path = if persisted.is_absolute() {
        persisted.to_path_buf()
    } else {
        root.join(persisted)
    };
    let path = normalize_scoped_path(root, &path)?;
    let expected = normalize_scoped_path(
        root,
        &root
            .join(".forge")
            .join("logs")
            .join(project_id)
            .join(task_id)
            .join(format!("{execution_id}.jsonl")),
    )?;
    if path != expected {
        return Err(ServiceError::invalid_operation(
            "execution log path is outside the canonical Solo execution-log scope",
        ));
    }
    Ok(path)
}

fn normalize_scoped_path(root: &Path, path: &Path) -> Result<PathBuf> {
    if root.as_os_str().is_empty()
        || !root.is_absolute()
        || root
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(ServiceError::invalid_operation(
            "activity log root must be an absolute stable path",
        ));
    }
    let relative = path.strip_prefix(root).map_err(|_| {
        ServiceError::invalid_operation("activity log path is outside the configured root")
    })?;
    if relative.components().any(|component| {
        matches!(
            component,
            Component::CurDir | Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err(ServiceError::invalid_operation(
            "activity log path contains an unsafe component",
        ));
    }
    if relative.as_os_str().is_empty() {
        return Err(ServiceError::invalid_operation(
            "activity log path must name a file",
        ));
    }
    Ok(path.to_path_buf())
}

/// Check every existing component without following symlinks. Missing
/// components are allowed because a queued execution may not have emitted a
/// log yet; a later refresh repeats this check before reading.
async fn ensure_scoped_log_path(root: &Path, path: &Path) -> Result<()> {
    let path = normalize_scoped_path(root, path)?;
    let relative = path.strip_prefix(root).expect("normalized path is scoped");
    let root_metadata = match tokio::fs::symlink_metadata(root).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => {
            return Err(ServiceError::invalid_operation(
                "activity log root is not readable",
            ));
        }
    };
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        return Err(ServiceError::invalid_operation(
            "activity log root is not a stable directory",
        ));
    }
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(component) = component else {
            return Err(ServiceError::invalid_operation(
                "activity log path contains an unsafe component",
            ));
        };
        current.push(component);
        match tokio::fs::symlink_metadata(&current).await {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(ServiceError::invalid_operation(
                    "activity log path contains a symlink",
                ));
            }
            Ok(metadata) if current != path && !metadata.is_dir() => {
                return Err(ServiceError::invalid_operation(
                    "activity log path contains a non-directory component",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(_) => {
                return Err(ServiceError::invalid_operation(
                    "activity log path is not readable",
                ));
            }
        }
    }
    Ok(())
}

fn activity_entry(entry: &LogEntry) -> Option<SoloActivityEntry> {
    let payload = &entry.payload;
    match &entry.kind {
        LogKind::System if payload.get("type").and_then(Value::as_str) == Some("turn_divider") => {
            Some(SoloActivityEntry {
                sequence: entry.sequence,
                kind: SoloActivityKind::AttemptDivider,
                tool_name: None,
                argument_keys: Vec::new(),
                summary: safe_optional_diagnostic(payload.get("label").and_then(Value::as_str)),
                text: None,
                attempt: payload.get("attempt").and_then(Value::as_i64),
                collapsed: false,
            })
        }
        LogKind::ToolCall => {
            let argument_keys = payload
                .get("argument_keys")
                .and_then(Value::as_array)
                .map(|keys| {
                    keys.iter()
                        .filter_map(Value::as_str)
                        .take(64)
                        .map(safe_identifier)
                        .collect()
                })
                .unwrap_or_default();
            Some(SoloActivityEntry {
                sequence: entry.sequence,
                kind: SoloActivityKind::ToolCall,
                tool_name: payload
                    .get("name")
                    .and_then(Value::as_str)
                    .and_then(safe_diagnostic),
                argument_keys,
                summary: None,
                text: None,
                attempt: None,
                collapsed: true,
            })
        }
        LogKind::ToolResult => Some(SoloActivityEntry {
            sequence: entry.sequence,
            kind: SoloActivityKind::ToolResult,
            tool_name: payload
                .get("name")
                .and_then(Value::as_str)
                .and_then(safe_diagnostic),
            argument_keys: Vec::new(),
            summary: tool_result_summary(payload.get("summary")),
            text: None,
            attempt: None,
            collapsed: true,
        }),
        LogKind::AssistantDelta => Some(SoloActivityEntry {
            sequence: entry.sequence,
            kind: SoloActivityKind::AssistantDelta,
            tool_name: None,
            argument_keys: Vec::new(),
            summary: None,
            text: payload
                .get("text")
                .and_then(Value::as_str)
                .and_then(safe_diagnostic),
            attempt: None,
            collapsed: false,
        }),
        LogKind::Assistant => Some(SoloActivityEntry {
            sequence: entry.sequence,
            kind: SoloActivityKind::Assistant,
            tool_name: None,
            argument_keys: Vec::new(),
            summary: None,
            text: payload
                .get("text")
                .and_then(Value::as_str)
                .and_then(safe_diagnostic),
            attempt: None,
            collapsed: false,
        }),
        // Reasoning is retained as a typed collapsed marker only.  The
        // existing sink omits redacted reasoning; ordinary reasoning is not
        // made a terminal-visible transcript by this facade.
        LogKind::Thinking => Some(SoloActivityEntry {
            sequence: entry.sequence,
            kind: SoloActivityKind::Thinking,
            tool_name: None,
            argument_keys: Vec::new(),
            summary: None,
            text: None,
            attempt: None,
            collapsed: true,
        }),
        _ => None,
    }
}

fn tool_result_summary(value: Option<&Value>) -> Option<String> {
    let object = value?.as_object()?;
    let mut safe = serde_json::Map::new();
    for key in [
        "status",
        "code",
        "safe_message",
        "correlation_id",
        "recovery_action",
    ] {
        if let Some(value) = object.get(key) {
            if let Some(value) = value.as_str() {
                safe.insert(
                    key.to_owned(),
                    Value::String(safe_text(value, SOLO_MAX_SHORT_TEXT_CHARS)),
                );
            }
        }
    }
    serde_json::to_string(&safe)
        .ok()
        .and_then(|value| safe_diagnostic(&value))
}

fn visible_content(content: &str, sensitivity: &str) -> (String, bool) {
    let protected_sensitivity = matches!(
        sensitivity.to_ascii_lowercase().as_str(),
        "secret" | "protected" | "sensitive" | "credential"
    );
    if protected_sensitivity || contains_secret_marker(content) {
        ("[redacted]".to_owned(), true)
    } else {
        (safe_text(content, SOLO_MAX_TEXT_CHARS), false)
    }
}

fn safe_optional_diagnostic(value: Option<&str>) -> Option<String> {
    value.and_then(safe_diagnostic)
}

fn safe_diagnostic(value: &str) -> Option<String> {
    if contains_secret_marker(value) {
        return None;
    }
    Some(safe_text(value, SOLO_MAX_SHORT_TEXT_CHARS))
}

fn safe_text(value: &str, limit: usize) -> String {
    value
        .chars()
        .filter(|character| !character.is_control() || matches!(character, '\n' | '\t'))
        .take(limit)
        .collect()
}

/// Preserve a typed identity value exactly only when it is safe to echo back
/// into an approval command. Invalid/oversized/secret-looking values make the
/// complete Charter approval target unavailable instead of silently
/// truncating a project name or slug into a different command.
fn safe_exact_text(value: &str, limit: usize) -> Option<String> {
    if value.trim().is_empty()
        || value.chars().count() > limit
        || value.chars().any(char::is_control)
        || contains_secret_marker(value)
    {
        None
    } else {
        Some(value.to_owned())
    }
}

fn safe_identifier(value: &str) -> String {
    safe_text(value, SOLO_MAX_ID_CHARS)
}

fn safe_json(value: &str) -> Option<String> {
    if value.chars().count() > SOLO_MAX_JSON_CHARS || contains_secret_marker(value) {
        return None;
    }
    let parsed = serde_json::from_str::<Value>(value).ok()?;
    serde_json::to_string(&parsed)
        .ok()
        .and_then(|value| (value.chars().count() <= SOLO_MAX_JSON_CHARS).then_some(value))
}

fn contains_secret_marker(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    [
        "api_key=",
        "api-key=",
        "apikey=",
        "authorization:",
        "authorization=",
        "bearer ",
        "client_secret",
        "password=",
        "private_key",
        "refresh_token",
        "secret=",
        "token=",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
        || ["ghp_", "gho_", "ghu_", "ghs_", "ghr_", "github_pat_", "sk-"]
            .iter()
            .any(|marker| contains_prefixed_marker(&lower, marker))
}

fn contains_prefixed_marker(value: &str, marker: &str) -> bool {
    value.match_indices(marker).any(|(index, _)| {
        index == 0
            || value
                .as_bytes()
                .get(index.saturating_sub(1))
                .is_some_and(|character| !character.is_ascii_alphanumeric())
    })
}

fn bounded_required(name: &str, value: &str, max_chars: usize) -> Result<()> {
    if value.trim().is_empty() {
        return Err(ServiceError::invalid_operation(format!(
            "{name} is required"
        )));
    }
    if value.chars().count() > max_chars {
        return Err(ServiceError::invalid_operation(format!(
            "{name} exceeds the bounded limit"
        )));
    }
    Ok(())
}

fn bounded_owned(name: &str, value: String, max_chars: usize) -> Result<String> {
    bounded_required(name, &value, max_chars)?;
    Ok(value)
}

fn bounded_limit(name: &str, value: i64, max: i64) -> Result<i64> {
    if !(1..=max).contains(&value) {
        return Err(ServiceError::invalid_operation(format!(
            "{name} limit must be between 1 and {max}"
        )));
    }
    Ok(value)
}

fn solo_review_action(
    decision: SoloReviewDecision,
    reason: Option<String>,
) -> Result<api_types::TaskAction> {
    let reason = reason.and_then(|reason| safe_diagnostic(&reason));
    match decision {
        SoloReviewDecision::Accept => Ok(api_types::TaskAction::Approve {
            reason,
            override_checks: Some(false),
        }),
        SoloReviewDecision::RequestChanges => {
            let guidance = reason
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    ServiceError::invalid_operation("send_back requires nonblank guidance")
                })?;
            Ok(api_types::TaskAction::SendBack { guidance })
        }
    }
}

mod service;
pub use service::SoloSessionService;

#[cfg(test)]
mod tests;
