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
//!   suitable for any presentation; and
//! * [`SoloProjection`] treats live events as invalidation hints and always
//!   obtains rendered state from an authoritative refresh.
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

use crate::{
    agent_chat_service::{
        AgentChatService, CancelAgentChatTurnInput, RetryAgentChatTurnInput,
        SendAgentChatMessageInput,
    },
    agent_chat_turn_worker::AgentChatTurnLogRoot,
    project_artifact_commands::ProjectCommandAuthorization,
    project_charter_commands::{ProjectCharterApprovalCommand, ProjectCharterCommandService},
    task_service::TaskService,
    Result, ServiceError,
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

    /// Attach the protected runtime interaction broker and its exact durable
    /// Agent Session identifier.  An empty session identifier is rejected at
    /// command time and is not persisted in a presentation object.
    #[must_use]
    pub fn with_interaction_broker(
        mut self,
        broker: InteractionBrokerHandle,
        forge_session_id: impl Into<String>,
    ) -> Self {
        self.interaction_broker = Some(broker);
        self.forge_session_id = Some(forge_session_id.into());
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
    pub correlation_id: String,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SoloTaskAction {
    Start,
    Pause,
    Resume,
    Submit,
    RequestChanges,
    Approve,
    Cancel,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloTaskInterruption {
    pub failure_kind: Option<api_types::FailureKind>,
    pub reason: Option<String>,
    pub source: Option<String>,
    pub execution_id: Option<String>,
    pub recovery_actions: Vec<api_types::RecoveryAction>,
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
    pub available_actions: Vec<SoloTaskAction>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloProjectionInvalidation {
    pub event_type: String,
    pub entity_id: Option<String>,
    pub lagged: bool,
}

#[derive(Clone)]
pub struct SoloSessionService {
    scope: SoloSessionScope,
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
    agent_chat_service: Arc<AgentChatService<SqliteDb>>,
    task_service: Arc<TaskService>,
    turn_logs: AgentChatTurnLogRoot,
    interaction_broker: Option<InteractionBrokerHandle>,
    forge_session_id: Option<String>,
    execution_logs_root: Option<PathBuf>,
}

impl fmt::Debug for SoloSessionService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SoloSessionService")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl SoloSessionService {
    /// Bind one immutable scope to the shared runtime services.
    pub fn new(scope: SoloSessionScope, dependencies: SoloSessionDependencies) -> Result<Self> {
        scope.validate()?;
        Ok(Self {
            scope,
            db: dependencies.db,
            event_bus: dependencies.event_bus,
            agent_chat_service: dependencies.agent_chat_service,
            task_service: dependencies.task_service,
            turn_logs: dependencies.turn_logs,
            interaction_broker: dependencies.interaction_broker,
            forge_session_id: dependencies.forge_session_id,
            execution_logs_root: dependencies.execution_logs_root,
        })
    }

    #[must_use]
    pub fn scope(&self) -> &SoloSessionScope {
        &self.scope
    }

    /// Subscribe to invalidation hints for this Project Chat.  The receiver
    /// is also available through [`SoloProjection::new`].
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<ForgeEvent> {
        self.event_bus.subscribe()
    }

    #[must_use]
    pub fn projection(&self) -> SoloProjection {
        SoloProjection::new(Arc::new(self.clone()))
    }

    /// Read every visible Solo surface from current authoritative records.
    /// Event delivery never substitutes for this read.
    pub async fn refresh(&self) -> Result<SoloSessionSnapshot> {
        self.refresh_with_limits(SoloRefreshLimits::default()).await
    }

    pub async fn refresh_with_limits(
        &self,
        limits: SoloRefreshLimits,
    ) -> Result<SoloSessionSnapshot> {
        let limits = limits.bounded()?;
        let records = self.load_scope_records().await?;
        let (messages, messages_has_more) = self.load_messages(limits.messages).await?;
        let turns = self.load_turns(limits.turns).await?;
        let tasks = self.load_tasks(limits.tasks).await?;
        let attention = self.load_attention(limits.attention).await?;
        let interactions = self.load_interactions(&turns).await?;
        let charter = self
            .load_charter_snapshot(&records.project, records.binding.as_ref())
            .await?;

        let project = SoloProjectSnapshot {
            id: safe_identifier(&records.project.id),
            name: safe_diagnostic(&records.project.name).unwrap_or_else(|| "[redacted]".to_owned()),
            version: records.project.version,
            readiness: project_readiness(&records.project, records.binding.as_ref()),
            paused_at: records
                .project
                .paused_at
                .as_deref()
                .map(|value| safe_text(value, SOLO_MAX_SHORT_TEXT_CHARS)),
            charter_status: safe_identifier(&records.project.charter_status),
            charter_setup_required: records.project.charter_setup_required,
            binding: records.binding.as_ref().map(binding_snapshot),
            charter,
        };

        Ok(SoloSessionSnapshot {
            contract_revision: SOLO_SESSION_CONTRACT_REVISION.to_owned(),
            scope: self.scope.clone(),
            project,
            repository: repository_snapshot(&records.repo),
            chat: chat_snapshot(&records.chat),
            messages,
            messages_has_more,
            turns,
            tasks,
            attention,
            interactions,
            capabilities: self.capabilities(),
            authoritative_at: db::now_rfc3339(),
        })
    }

    /// Admit one user message and one finite turn through the canonical
    /// AgentChatService.  The caller must provide a stable dedupe key so an
    /// interrupted response can safely replay the command.
    pub async fn send_message(&self, input: SoloSendMessageInput) -> Result<SoloSendMessageResult> {
        self.ensure_scope_ready_for_chat().await?;
        bounded_required("message content", &input.content, SOLO_MAX_TEXT_CHARS)?;
        bounded_required("message dedupe_key", &input.dedupe_key, SOLO_MAX_ID_CHARS)?;
        let admitted = self
            .agent_chat_service
            .send_message(SendAgentChatMessageInput {
                actor_user_id: self.scope.owner_id.clone(),
                chat_id: self.scope.project_chat_id.clone(),
                content: input.content,
                dedupe_key: Some(input.dedupe_key),
            })
            .await?;
        Ok(SoloSendMessageResult {
            message: message_snapshot(&admitted.message),
            turn: turn_snapshot(&admitted.turn_job),
        })
    }

    /// Cancel one exact non-terminal turn with the current optimistic version
    /// and a stable cancellation idempotency key.
    pub async fn cancel_turn(
        &self,
        turn_job_id: impl Into<String>,
        expected_version: i64,
        idempotency_key: impl Into<String>,
    ) -> Result<SoloTurnSnapshot> {
        let turn_job_id = bounded_owned("turn_job_id", turn_job_id.into(), SOLO_MAX_ID_CHARS)?;
        let idempotency_key = bounded_owned(
            "turn cancellation idempotency_key",
            idempotency_key.into(),
            SOLO_MAX_ID_CHARS,
        )?;
        if expected_version < 1 {
            return Err(ServiceError::invalid_operation(
                "turn expected_version must be positive",
            ));
        }
        let job = self.authorized_turn(&turn_job_id).await?;
        if turn_snapshot(&job).status.is_terminal() {
            return Err(ServiceError::conflict(
                "Agent Chat turn is already terminal",
            ));
        }
        let cancelled = self
            .agent_chat_service
            .cancel_turn(CancelAgentChatTurnInput {
                actor_user_id: self.scope.owner_id.clone(),
                chat_id: self.scope.project_chat_id.clone(),
                turn_job_id,
                expected_version,
                idempotency_key,
            })
            .await?;
        Ok(turn_snapshot(&cancelled))
    }

    /// Retry a failed turn from its original triggering message.  The source
    /// version is checked before any replay, and the canonical deterministic
    /// child is returned when a prior response was lost.
    pub async fn retry_turn(&self, input: SoloRetryTurnInput) -> Result<SoloTurnSnapshot> {
        let (turn_job_id, _idempotency_key, expected_version) = validate_retry_input(input)?;
        let job = self.authorized_turn(&turn_job_id).await?;
        ensure_expected_turn_version(job.version, expected_version)?;
        if job.status != AgentChatTurnState::Failed {
            return Err(ServiceError::conflict(
                "only a failed Agent Chat turn can be retried",
            ));
        }
        if job.attempt_count < 0 {
            return Err(ServiceError::invalid_operation(
                "failed Agent Chat turn has an invalid attempt count",
            ));
        }
        let next_attempt = job.attempt_count.checked_add(1).ok_or_else(|| {
            ServiceError::invalid_operation("failed Agent Chat turn attempt count overflowed")
        })?;
        let dedupe_key = retry_dedupe_key(&job.id, next_attempt)?;

        // A response can be lost after the canonical service commits its
        // child.  Resolve that child before admission so replay converges to
        // one durable retry rather than attempting a second turn.
        if let Some(existing) = self.find_retry_child(&job, &dedupe_key).await? {
            return Ok(turn_snapshot(&existing));
        }

        let retried = self
            .agent_chat_service
            .retry_turn(RetryAgentChatTurnInput {
                actor_user_id: self.scope.owner_id.clone(),
                chat_id: self.scope.project_chat_id.clone(),
                turn_job_id: turn_job_id.clone(),
            })
            .await;
        match retried {
            Ok(retried) => {
                // Re-authorize the returned row instead of trusting the
                // service result's scope metadata at this presentation
                // boundary.
                let retried = self.authorized_turn(&retried.id).await?;
                Ok(turn_snapshot(&retried))
            }
            Err(error) => {
                // A concurrent retry may have won the insert between the
                // preflight read and the canonical admission.  If its
                // deterministic child is now durable, return it as the
                // replay result; otherwise preserve the canonical error.
                if let Some(existing) = self.find_retry_child(&job, &dedupe_key).await? {
                    Ok(turn_snapshot(&existing))
                } else {
                    Err(error)
                }
            }
        }
    }

    /// Answer a protected runtime questionnaire only when the exact pending
    /// interaction is referenced by a turn in this bound Project Chat.
    pub async fn answer_interaction(
        &self,
        input: SoloInteractionAnswerInput,
    ) -> Result<SoloOperationOutcome<SoloInteractionSnapshot>> {
        const OPERATION: &str = "solo.answer_interaction";
        let interaction_id =
            bounded_owned("interaction_id", input.interaction_id, SOLO_MAX_ID_CHARS)?;
        if input.expected_version < 1 {
            return Err(ServiceError::invalid_operation(
                "interaction expected_version must be positive",
            ));
        }
        self.validate_interaction_values(&input.values)?;
        let turns = self.load_turns(SOLO_MAX_TURNS).await?;
        let Some(turn) = turns
            .iter()
            .find(|turn| turn.pending_interaction_id.as_deref() == Some(interaction_id.as_str()))
        else {
            return Err(ServiceError::not_found("agent_interaction", interaction_id));
        };
        if turn.status != SoloTurnStatus::AwaitingInput {
            return Ok(SoloOperationOutcome::not_ready(
                OPERATION,
                "the referenced turn is not awaiting typed input",
            ));
        }
        let Some(broker) = self.interaction_broker.as_ref() else {
            return Ok(SoloOperationOutcome::unsupported(
                OPERATION,
                "the protected runtime interaction broker is not attached",
            ));
        };
        let Some(forge_session_id) = self
            .forge_session_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        else {
            return Ok(SoloOperationOutcome::not_ready(
                OPERATION,
                "the owning Agent Runtime session is not available",
            ));
        };
        let values = input
            .values
            .into_iter()
            .map(interaction_answer_value)
            .collect();
        let answered = broker
            .answer_for_session(
                &self.scope.owner_id,
                forge_session_id,
                InteractionAnswer::new(interaction_id, input.expected_version, values),
            )
            .await
            .map_err(map_interaction_error)?;
        Ok(SoloOperationOutcome::Applied(interaction_snapshot(
            &answered,
        )))
    }

    /// Execute one exact Project Charter adoption/amendment target through the
    /// existing typed command service.  The target is reloaded immediately
    /// before command execution so an open TUI card can never approve a stale
    /// digest, revision, binding, or Project version.
    pub async fn approve_charter(
        &self,
        input: SoloCharterApprovalInput,
    ) -> Result<SoloOperationOutcome<SoloCharterApprovalResult>> {
        const OPERATION: &str = "solo.approve_charter";
        validate_approval_input(&input)?;
        if input.target.project_id != self.scope.project_id {
            return Err(ServiceError::not_found(
                "project",
                input.target.project_id.clone(),
            ));
        }
        let records = self.load_scope_records().await?;
        let current = self
            .load_charter_snapshot(&records.project, records.binding.as_ref())
            .await?
            .and_then(|charter| charter.approval_target);
        let Some(current) = current else {
            return Ok(SoloOperationOutcome::not_ready(
                OPERATION,
                "no exact pending Project Charter approval target is available",
            ));
        };
        if current != input.target {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        let authorization_json = serde_json::to_string(&json!({
            "principal_type": "user",
            "principal_id": self.scope.owner_id.clone(),
            "authorization_basis": input.authorization.authorization_basis.clone(),
            "action": PROJECT_CHARTER_APPROVAL_ACTION,
            "event_id": input.authorization.authorization_event_id.clone(),
            "occurred_at": input.authorization.authorization_occurred_at.clone(),
        }))
        .map_err(|_| {
            ServiceError::invalid_operation("Charter approval authorization is invalid")
        })?;
        let authorization = ProjectCommandAuthorization {
            principal_type: "user".to_owned(),
            principal_id: self.scope.owner_id.clone(),
            policy_result: "allowed".to_owned(),
            policy_revision: Some(PROJECT_AGENT_POLICY_REVISION.to_owned()),
            policy_digest: Some(input.target.selected_project_agent_policy_digest.clone()),
            requested_permission: Some(APPROVE_PROJECT_CHARTER_PERMISSION.to_owned()),
            correlation_id: input.idempotency_key.clone(),
            causation_id: None,
            causation_depth: 0,
            authorization_event_id: input.authorization.authorization_event_id.clone(),
            authorization_basis: input.authorization.authorization_basis.clone(),
            authorization_action: PROJECT_CHARTER_APPROVAL_ACTION.to_owned(),
            authorization_occurred_at: input.authorization.authorization_occurred_at.clone(),
            authorization_json,
        };
        let outcome = ProjectCharterCommandService::new(Arc::clone(&self.db))
            .approve(
                ProjectCharterApprovalCommand {
                    project_id: input.target.project_id.clone(),
                    charter_id: input.target.charter_id.clone(),
                    revision_id: input.target.revision_id.clone(),
                    content_digest: input.target.content_digest.clone(),
                    rendered_digest: input.target.rendered_digest.clone(),
                    expected_charter_version: input.target.expected_charter_version,
                    expected_project_version: input.target.expected_project_version,
                    approved_project_name: input.target.approved_project_name.clone(),
                    approved_project_slug: input.target.approved_project_slug.clone(),
                    project_mode: input.target.project_mode.clone(),
                    selected_project_agent_identity_id: input
                        .target
                        .selected_project_agent_identity_id
                        .clone(),
                    selected_project_agent_profile_revision_id: input
                        .target
                        .selected_project_agent_profile_revision_id
                        .clone(),
                    selected_project_agent_operating_skill_revision: input
                        .target
                        .selected_project_agent_operating_skill_revision
                        .clone(),
                    selected_project_agent_policy_digest: input
                        .target
                        .selected_project_agent_policy_digest
                        .clone(),
                    idempotency_key: input.idempotency_key,
                    authorization,
                },
                None,
            )
            .await?;
        Ok(SoloOperationOutcome::Applied(SoloCharterApprovalResult {
            approval_id: outcome.approval.id,
            project_id: outcome.project_id,
            project_version: outcome.project_version,
            project_charter_id: outcome.project_charter_id,
            project_charter_revision_id: outcome.project_charter_revision_id,
            project_agent_binding_id: outcome.project_agent_binding_id,
            project_chat_id: outcome.project_chat_id,
        }))
    }

    /// Accept or request changes on the exact human review gate through the
    /// existing workflow-aware TaskService.  The task version is checked by
    /// that service and no direct state transition is duplicated here.
    pub async fn decide_review(
        &self,
        input: SoloReviewDecisionInput,
    ) -> Result<SoloReviewDecisionResult> {
        let task_id = bounded_owned("task_id", input.task_id, SOLO_MAX_ID_CHARS)?;
        let task = self.authorized_task(&task_id).await?;
        if input.expected_task_version < 1 {
            return Err(ServiceError::invalid_operation(
                "expected_task_version must be positive",
            ));
        }
        let reason = input.reason.and_then(|reason| safe_diagnostic(&reason));
        let action = match input.decision {
            SoloReviewDecision::Accept => api_types::TaskAction::Approve,
            SoloReviewDecision::RequestChanges => api_types::TaskAction::RequestChanges,
        };
        let result = self
            .task_service
            .perform_task_action(
                task.id.clone(),
                action,
                reason,
                Some(input.expected_task_version),
            )
            .await?;
        Ok(SoloReviewDecisionResult {
            task_id: result.task.id,
            decision: input.decision,
            committed_task_version: result.task.version,
        })
    }

    /// Read bounded activity for one Project Chat turn using its shared JSONL
    /// log root.  A file shrink/rotation resets the sequence cursor and is
    /// reported explicitly to the caller.
    pub async fn read_turn_activity(
        &self,
        turn_job_id: impl Into<String>,
        cursor: SoloActivityCursor,
        limit: usize,
    ) -> Result<SoloActivityPage> {
        validate_activity_limit(limit)?;
        let turn_job_id = path_component_id("turn_job_id", turn_job_id.into())?;
        let turn = self.authorized_turn(&turn_job_id).await?;
        let turn_id = path_component_id("turn id", turn.id.clone())?;
        let root = self.turn_logs.root();
        let path = self.turn_logs.path_for(&turn_id);
        read_activity_path(&path, &root, &turn_id, cursor, limit).await
    }

    /// Read bounded activity for a Task execution after proving both the Task
    /// and execution belong to this Project/repository scope.
    pub async fn read_execution_activity(
        &self,
        task_id: impl Into<String>,
        execution_id: impl Into<String>,
        cursor: SoloActivityCursor,
        limit: usize,
    ) -> Result<SoloActivityPage> {
        validate_activity_limit(limit)?;
        let task_id = path_component_id("task_id", task_id.into())?;
        let execution_id = path_component_id("execution_id", execution_id.into())?;
        let task = self.authorized_task(&task_id).await?;
        let execution = ExecutionRepo::get_by_id(&*self.db, &execution_id)
            .await?
            .filter(|execution| execution.task_id == task.id);
        let Some(execution) = execution else {
            return Err(ServiceError::not_found("execution", execution_id));
        };
        let Some(path) = execution.logs_path else {
            return Ok(SoloActivityPage {
                entries: Vec::new(),
                cursor,
                has_more: false,
                reset: false,
            });
        };
        let Some(root) = self.execution_logs_root.as_deref() else {
            return Err(ServiceError::invalid_operation(
                "execution log root is not attached to the Solo session",
            ));
        };
        let path = scoped_execution_log_path(
            root,
            &self.scope.project_id,
            &task.id,
            &execution.id,
            &path,
        )?;
        read_activity_path(&path, root, &execution.id, cursor, limit).await
    }

    fn capabilities(&self) -> SoloCapabilities {
        let runtime_interactions = match (&self.interaction_broker, &self.forge_session_id) {
            (None, _) => SoloCapability::Unsupported {
                reason: "the protected runtime interaction broker is not attached".to_owned(),
            },
            (Some(_), Some(session_id)) if !session_id.trim().is_empty() => {
                SoloCapability::Available
            }
            (Some(_), _) => SoloCapability::NotReady {
                reason: "the owning Agent Runtime session is not available".to_owned(),
            },
        };
        SoloCapabilities {
            runtime_interactions,
        }
    }

    async fn ensure_scope_ready_for_chat(&self) -> Result<()> {
        let records = self.load_scope_records().await?;
        if records.chat.status.trim().is_empty() {
            return Err(ServiceError::conflict(
                "Project Chat has no lifecycle status",
            ));
        }
        Ok(())
    }

    async fn load_scope_records(&self) -> Result<AuthorizedScopeRecords> {
        let project = ProjectRepo::get_by_id(&*self.db, &self.scope.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", self.scope.project_id.clone()))?;
        let owner_matches = project.owner_id.as_deref() == Some(self.scope.owner_id.as_str());
        if !owner_matches {
            return Err(ServiceError::not_found(
                "project",
                self.scope.project_id.clone(),
            ));
        }
        if project.primary_repo_id.as_deref() != Some(self.scope.repo_id.as_str()) {
            return Err(ServiceError::not_found("repo", self.scope.repo_id.clone()));
        }
        let repo = RepoRepo::get_by_id(&*self.db, &self.scope.repo_id)
            .await?
            .filter(|repo| repo.project_id == self.scope.project_id)
            .ok_or_else(|| ServiceError::not_found("repo", self.scope.repo_id.clone()))?;
        let chat = AgentChatRepo::get_agent_chat(&*self.db, &self.scope.project_chat_id)
            .await?
            .filter(|chat| {
                chat.kind == PROJECT_CHAT_KIND
                    && chat.project_id.as_deref() == Some(self.scope.project_id.as_str())
            })
            .ok_or_else(|| {
                ServiceError::not_found("agent_chat", self.scope.project_chat_id.clone())
            })?;
        let binding =
            ProjectAgentBindingRepo::get_active_project_binding(&*self.db, &self.scope.project_id)
                .await?
                .filter(|binding| binding.project_id == self.scope.project_id);
        if let Some(binding) = binding.as_ref() {
            if let Some(identity_id) = binding.identity_id.as_deref() {
                if let Some(identity) = AgentRepo::get_by_id(&*self.db, identity_id).await? {
                    if identity.owner_id.as_deref() != Some(self.scope.owner_id.as_str())
                        || binding.profile_id.as_deref() != Some(identity.profile_id.as_str())
                    {
                        return Err(ServiceError::not_found(
                            "project_agent_binding",
                            binding.id.clone(),
                        ));
                    }
                } else {
                    return Err(ServiceError::not_found(
                        "agent_identity",
                        identity_id.to_owned(),
                    ));
                }
            }
        }
        Ok(AuthorizedScopeRecords {
            project,
            repo,
            chat,
            binding,
        })
    }

    async fn authorized_turn(&self, turn_job_id: &str) -> Result<AgentChatTurnJob> {
        let job = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*self.db, turn_job_id)
            .await?
            .filter(|job| turn_belongs_to_scope(job, &self.scope))
            .ok_or_else(|| ServiceError::not_found("agent_chat_turn", turn_job_id.to_owned()))?;
        // This second check ensures a tampered/legacy chat row cannot make a
        // turn look Project-scoped merely by sharing the chat id.
        self.load_scope_records().await?;
        Ok(job)
    }

    async fn find_retry_child(
        &self,
        source: &AgentChatTurnJob,
        dedupe_key: &str,
    ) -> Result<Option<AgentChatTurnJob>> {
        let mut matches =
            AgentChatTurnJobRepo::list_agent_chat_turn_jobs(&*self.db, &self.scope.project_chat_id)
                .await?
                .into_iter()
                .filter(|job| {
                    job.id != source.id
                        && job.dedupe_key == dedupe_key
                        && job.triggering_message_id == source.triggering_message_id
                        && turn_belongs_to_scope(job, &self.scope)
                })
                .collect::<Vec<_>>();
        match matches.len() {
            0 => Ok(None),
            1 => Ok(matches.pop()),
            _ => Err(ServiceError::conflict(
                "multiple deterministic retry children exist for the failed turn",
            )),
        }
    }

    async fn authorized_task(&self, task_id: &str) -> Result<Task> {
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .filter(|task| task.project_id == self.scope.project_id)
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        self.load_scope_records().await?;
        Ok(task)
    }

    async fn load_messages(&self, limit: i64) -> Result<(Vec<SoloChatMessageSnapshot>, bool)> {
        let page = AgentChatMessageRepo::list_agent_chat_messages(
            &*self.db,
            db::AgentChatMessageListQuery {
                chat_id: self.scope.project_chat_id.clone(),
                before_sequence: None,
                page: PageRequest {
                    cursor: None,
                    limit,
                    include_total: false,
                    sort_by: SortBy::CreatedAt,
                    sort_order: SortOrder::Asc,
                },
            },
        )
        .await?;
        let messages = page
            .items
            .into_iter()
            .filter(|message| message.chat_id == self.scope.project_chat_id)
            .map(|message| message_snapshot(&message))
            .collect();
        Ok((messages, page.next_cursor.is_some()))
    }

    async fn load_turns(&self, limit: i64) -> Result<Vec<SoloTurnSnapshot>> {
        let mut jobs =
            AgentChatTurnJobRepo::list_agent_chat_turn_jobs(&*self.db, &self.scope.project_chat_id)
                .await?;
        jobs.retain(|job| turn_belongs_to_scope(job, &self.scope));
        jobs.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        // The UI needs the newest turn as its retry/progress fence. Keeping
        // the oldest page would hide every new turn once the bound is full.
        let omitted = jobs.len().saturating_sub(limit as usize);
        jobs.drain(..omitted);
        Ok(jobs.iter().map(turn_snapshot).collect())
    }

    async fn load_tasks(&self, limit: i64) -> Result<Vec<SoloTaskSnapshot>> {
        let page = TaskRepo::list(
            &*self.db,
            TaskListQuery {
                project_id: self.scope.project_id.clone(),
                q: None,
                statuses: Vec::new(),
                agent_ids: Vec::new(),
                assignee_types: Vec::new(),
                assignee_ids: Vec::new(),
                priority: None,
                include_archived: false,
                include_cancelled: true,
                include_deleted: false,
                page: PageRequest {
                    cursor: None,
                    limit,
                    include_total: false,
                    sort_by: SortBy::UpdatedAt,
                    sort_order: SortOrder::Desc,
                },
            },
        )
        .await?;
        let mut tasks = Vec::new();
        for task in page.items {
            if task.project_id != self.scope.project_id {
                continue;
            }
            tasks.push(self.task_snapshot(task).await?);
        }
        Ok(tasks)
    }

    async fn task_snapshot(&self, task: Task) -> Result<SoloTaskSnapshot> {
        let task_id = task.id.clone();
        let interruption = task_interruption(&task);
        let mut executions = ExecutionRepo::list_by_task(
            &*self.db,
            &task_id,
            PageRequest {
                cursor: None,
                limit: SOLO_MAX_EXECUTIONS_PER_TASK,
                include_total: false,
                sort_by: SortBy::CreatedAt,
                sort_order: SortOrder::Desc,
            },
        )
        .await?
        .items;
        executions.retain(|execution| execution.task_id == task_id);
        executions.truncate(SOLO_MAX_EXECUTIONS_PER_TASK as usize);
        let roles = TaskRoleAssignmentRepo::list_by_task(&*self.db, &task_id)
            .await?
            .into_iter()
            .take(SOLO_MAX_ROLES_PER_TASK as usize)
            .map(role_snapshot)
            .collect::<Vec<_>>();
        let review = ReviewRepo::list_by_task(&*self.db, &task_id)
            .await?
            .into_iter()
            .max_by(|left, right| {
                left.attempt_number
                    .cmp(&right.attempt_number)
                    .then_with(|| left.created_at.cmp(&right.created_at))
                    .then_with(|| left.id.cmp(&right.id))
            });
        let available_actions = match self
            .task_service
            .available_task_actions(task_id.clone())
            .await
        {
            Ok(actions) => actions.into_iter().map(task_action).collect(),
            Err(ServiceError::InvalidOperation { .. }) => Vec::new(),
            Err(error) => return Err(error),
        };
        Ok(SoloTaskSnapshot {
            id: safe_identifier(&task.id),
            project_id: safe_identifier(&task.project_id),
            title: safe_diagnostic(&task.title).unwrap_or_else(|| "[redacted]".to_owned()),
            status: safe_identifier(&task.status),
            version: task.version,
            assignee_type: task.assignee_type.as_deref().map(safe_identifier),
            assignee_id: task.assignee_id.as_deref().map(safe_identifier),
            priority: task.priority,
            interruption,
            available_actions,
            roles,
            executions: executions
                .iter()
                .map(execution_snapshot)
                .collect::<Vec<_>>(),
            latest_review: review.as_ref().map(review_snapshot),
            created_at: safe_text(&task.created_at, SOLO_MAX_SHORT_TEXT_CHARS),
            updated_at: safe_text(&task.updated_at, SOLO_MAX_SHORT_TEXT_CHARS),
        })
    }

    async fn load_attention(&self, limit: i64) -> Result<Vec<SoloAttentionSnapshot>> {
        let page = AttentionRepo::list_attention(
            &*self.db,
            AttentionListQuery {
                account_id: Some(self.scope.owner_id.clone()),
                project_id: Some(self.scope.project_id.clone()),
                scope_type: Some("project".to_owned()),
                status: None,
                include_snoozed: false,
                page: PageRequest {
                    cursor: None,
                    limit,
                    include_total: false,
                    sort_by: SortBy::Priority,
                    sort_order: SortOrder::Desc,
                },
            },
        )
        .await?;
        Ok(page
            .items
            .into_iter()
            .filter(|item| item.scope_type == "project" && item.scope_id == self.scope.project_id)
            .map(attention_snapshot)
            .collect())
    }

    async fn load_interactions(
        &self,
        turns: &[SoloTurnSnapshot],
    ) -> Result<Vec<SoloInteractionSnapshot>> {
        let Some(broker) = self.interaction_broker.as_ref() else {
            return Ok(Vec::new());
        };
        let Some(session_id) = self
            .forge_session_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        else {
            return Ok(Vec::new());
        };
        let referenced: HashSet<&str> = turns
            .iter()
            .filter_map(|turn| turn.pending_interaction_id.as_deref())
            .collect();
        if referenced.is_empty() {
            return Ok(Vec::new());
        }
        let summaries = broker
            .list_pending_for_owner(&self.scope.owner_id, session_id)
            .await
            .map_err(map_interaction_error)?;
        Ok(summaries
            .into_iter()
            .filter(|summary| referenced.contains(summary.id.as_str()))
            .map(|summary| interaction_snapshot(&summary))
            .collect())
    }

    async fn load_charter_snapshot(
        &self,
        project: &Project,
        binding: Option<&ProjectAgentBinding>,
    ) -> Result<Option<SoloCharterSnapshot>> {
        let Some(charter) = ProjectOrchestrationRepo::get_project_charter_by_project_id(
            &*self.db,
            &self.scope.project_id,
        )
        .await?
        else {
            return Ok(None);
        };
        if charter.project_id.as_deref() != Some(self.scope.project_id.as_str())
            || charter.account_id != self.scope.owner_id
        {
            return Err(ServiceError::not_found(
                "project_charter",
                charter.id.clone(),
            ));
        }
        let current_draft = self
            .load_charter_revision(&charter, charter.current_draft_revision_id.as_deref())
            .await?;
        let current_approved = self
            .load_charter_revision(&charter, charter.current_approved_revision_id.as_deref())
            .await?;
        let approval_target = self
            .charter_approval_target(project, binding, &charter, current_draft.as_ref())
            .await?;
        Ok(Some(SoloCharterSnapshot {
            id: safe_identifier(&charter.id),
            project_id: charter.project_id.as_deref().map(safe_identifier),
            version: charter.version,
            project_mode: safe_identifier(&charter.project_mode),
            maturity: safe_identifier(&charter.maturity),
            lifecycle: safe_identifier(&charter.lifecycle),
            current_draft_revision_id: charter
                .current_draft_revision_id
                .as_deref()
                .map(safe_identifier),
            current_approved_revision_id: charter
                .current_approved_revision_id
                .as_deref()
                .map(safe_identifier),
            current_draft,
            current_approved,
            approval_target,
        }))
    }

    async fn load_charter_revision(
        &self,
        charter: &ProjectCharterRecord,
        revision_id: Option<&str>,
    ) -> Result<Option<SoloCharterRevisionSnapshot>> {
        let Some(revision_id) = revision_id else {
            return Ok(None);
        };
        let revision =
            ProjectOrchestrationRepo::get_project_charter_revision(&*self.db, revision_id)
                .await?
                .filter(|revision| revision.charter_id == charter.id)
                .ok_or_else(|| {
                    ServiceError::not_found("project_charter_revision", revision_id.to_owned())
                })?;
        Ok(Some(charter_revision_snapshot(&revision)))
    }

    async fn charter_approval_target(
        &self,
        project: &Project,
        binding: Option<&ProjectAgentBinding>,
        charter: &ProjectCharterRecord,
        draft: Option<&SoloCharterRevisionSnapshot>,
    ) -> Result<Option<SoloCharterApprovalTarget>> {
        let kind = match project.charter_status.as_str() {
            LEGACY_UNVERIFIED_STATUS => SoloCharterApprovalKind::Adoption,
            CHARTER_BACKED_STATUS => SoloCharterApprovalKind::Amendment,
            _ => return Ok(None),
        };
        let pointer_matches = match kind {
            SoloCharterApprovalKind::Adoption => {
                project.charter_setup_required
                    && project.current_charter_id.is_none()
                    && project.current_charter_revision_id.is_none()
                    && charter.current_approved_revision_id.is_none()
            }
            SoloCharterApprovalKind::Amendment => {
                !project.charter_setup_required
                    && project.current_charter_id.as_deref() == Some(charter.id.as_str())
                    && project.current_charter_revision_id.as_deref()
                        == charter.current_approved_revision_id.as_deref()
                    && charter.current_approved_revision_id.is_some()
            }
        };
        if !pointer_matches {
            return Ok(None);
        }
        let Some(draft) = draft else {
            return Ok(None);
        };
        let Some(binding) = binding else {
            return Ok(None);
        };
        if !matches!(
            binding.state.as_str(),
            READY_BINDING_STATE | SETUP_BINDING_STATE
        ) {
            return Ok(None);
        }
        let (Some(identity_id), Some(profile_id), Some(skill_revision)) = (
            binding.identity_id.as_ref(),
            binding.profile_id.as_ref(),
            binding.operating_skill_revision_id.as_ref(),
        ) else {
            return Ok(None);
        };
        if binding.policy_digest.trim().is_empty() {
            return Ok(None);
        }
        let Some(working_name) = draft
            .working_name
            .as_deref()
            .filter(|name| !name.trim().is_empty())
        else {
            return Ok(None);
        };
        bounded_required(
            "Charter working name",
            working_name,
            SOLO_MAX_SHORT_TEXT_CHARS,
        )?;
        if let Some(slug) = draft.slug_proposal.as_deref() {
            bounded_required("Charter slug proposal", slug, SOLO_MAX_SHORT_TEXT_CHARS)?;
        }
        let charter_id = path_component_id("charter_id", charter.id.clone())?;
        let revision_id = path_component_id("revision_id", draft.id.clone())?;
        let identity_id = path_component_id("project agent identity_id", identity_id.clone())?;
        let profile_id = path_component_id("project agent profile_id", profile_id.clone())?;
        // This is an opaque revision token (for example `forge.project.orchestration/v1@1`),
        // not a filename. Keep it bounded without rejecting its punctuation.
        let skill_revision = bounded_owned(
            "project agent skill revision",
            skill_revision.clone(),
            SOLO_MAX_ID_CHARS,
        )?;
        bounded_required(
            "Charter content digest",
            &draft.content_digest,
            SOLO_MAX_ID_CHARS,
        )?;
        bounded_required(
            "Charter rendered digest",
            &draft.rendered_digest,
            SOLO_MAX_ID_CHARS,
        )?;
        let policy_digest =
            path_component_id("project agent policy digest", binding.policy_digest.clone())?;
        Ok(Some(SoloCharterApprovalTarget {
            kind,
            project_id: self.scope.project_id.clone(),
            charter_id,
            revision_id,
            content_digest: draft.content_digest.clone(),
            rendered_digest: draft.rendered_digest.clone(),
            expected_charter_version: charter.version,
            expected_project_version: project.version,
            approved_project_name: working_name.to_owned(),
            approved_project_slug: draft.slug_proposal.clone(),
            project_mode: safe_identifier(&charter.project_mode),
            selected_project_agent_identity_id: identity_id,
            selected_project_agent_profile_revision_id: profile_id,
            selected_project_agent_operating_skill_revision: skill_revision,
            selected_project_agent_policy_digest: policy_digest,
        }))
    }

    fn validate_interaction_values(&self, values: &[SoloInteractionAnswerValue]) -> Result<()> {
        if values.len() > SOLO_MAX_INTERACTION_VALUES {
            return Err(ServiceError::invalid_operation(
                "interaction answer exceeds the bounded question limit",
            ));
        }
        for value in values {
            match value {
                SoloInteractionAnswerValue::Choice {
                    question_id,
                    choice_id,
                } => {
                    bounded_required("interaction question_id", question_id, SOLO_MAX_ID_CHARS)?;
                    bounded_required("interaction choice_id", choice_id, SOLO_MAX_ID_CHARS)?;
                }
                SoloInteractionAnswerValue::FreeForm { question_id, value } => {
                    bounded_required("interaction question_id", question_id, SOLO_MAX_ID_CHARS)?;
                    bounded_required("interaction answer", value, SOLO_MAX_TEXT_CHARS)?;
                }
            }
        }
        Ok(())
    }
}

/// Event-backed projection.  Events only mark this object dirty; rendering
/// code receives no event payload and therefore cannot mistake a delta for an
/// authoritative state transition.
pub struct SoloProjection {
    service: Arc<SoloSessionService>,
    receiver: broadcast::Receiver<ForgeEvent>,
    invalidated: bool,
    snapshot: Option<SoloSessionSnapshot>,
}

impl fmt::Debug for SoloProjection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SoloProjection")
            .field("invalidated", &self.invalidated)
            .field("has_snapshot", &self.snapshot.is_some())
            .finish_non_exhaustive()
    }
}

impl SoloProjection {
    #[must_use]
    pub fn new(service: Arc<SoloSessionService>) -> Self {
        let receiver = service.subscribe();
        Self {
            service,
            receiver,
            invalidated: true,
            snapshot: None,
        }
    }

    #[must_use]
    pub fn is_invalidated(&self) -> bool {
        self.invalidated
    }

    pub async fn refresh(&mut self) -> Result<SoloSessionSnapshot> {
        let snapshot = self.service.refresh().await?;
        self.snapshot = Some(snapshot.clone());
        self.invalidated = false;
        Ok(snapshot)
    }

    pub async fn refresh_if_invalidated(&mut self) -> Result<Option<SoloSessionSnapshot>> {
        if self.invalidated || self.snapshot.is_none() {
            return self.refresh().await.map(Some);
        }
        Ok(None)
    }

    #[must_use]
    pub fn snapshot(&self) -> Option<&SoloSessionSnapshot> {
        self.snapshot.as_ref()
    }

    /// Wait for the next relevant event. A lagged receiver is itself a dirty
    /// signal: the next refresh reads all state instead of trying to replay a
    /// partial event stream.
    pub async fn next_invalidation(&mut self) -> Option<SoloProjectionInvalidation> {
        loop {
            match self.receiver.recv().await {
                Ok(event) => {
                    let Some(entity_id) = relevant_event_entity(&self.service.scope, &event) else {
                        continue;
                    };
                    self.invalidated = true;
                    return Some(SoloProjectionInvalidation {
                        event_type: safe_identifier(&event.event_type),
                        entity_id,
                        lagged: false,
                    });
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    self.invalidated = true;
                    return Some(SoloProjectionInvalidation {
                        event_type: "events.lagged".to_owned(),
                        entity_id: None,
                        lagged: true,
                    });
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
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
        correlation_id: safe_identifier(&job.correlation_id),
        version: job.version,
        created_at: safe_text(&job.created_at, SOLO_MAX_SHORT_TEXT_CHARS),
        updated_at: safe_text(&job.updated_at, SOLO_MAX_SHORT_TEXT_CHARS),
    }
}

fn task_action(action: api_types::TaskAction) -> SoloTaskAction {
    match action {
        api_types::TaskAction::Start => SoloTaskAction::Start,
        api_types::TaskAction::Pause => SoloTaskAction::Pause,
        api_types::TaskAction::Resume => SoloTaskAction::Resume,
        api_types::TaskAction::Submit => SoloTaskAction::Submit,
        api_types::TaskAction::RequestChanges => SoloTaskAction::RequestChanges,
        api_types::TaskAction::Approve => SoloTaskAction::Approve,
        api_types::TaskAction::Cancel => SoloTaskAction::Cancel,
    }
}

fn task_interruption(task: &Task) -> Option<SoloTaskInterruption> {
    if let Some(raw) = task.error_annotation.as_deref() {
        if raw.chars().count() <= SOLO_MAX_JSON_CHARS {
            if let Ok(api_types::TaskAnnotation::Blocking(annotation)) =
                serde_json::from_str::<api_types::TaskAnnotation>(raw)
            {
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
                    recovery_actions: annotation
                        .recovery_actions
                        .into_iter()
                        .take(SOLO_MAX_ROLES_PER_TASK as usize)
                        .collect(),
                });
            }
        }
    }
    task.blocked_json
        .as_deref()
        .or(task.failed_json.as_deref())
        .filter(|raw| raw.chars().count() <= SOLO_MAX_JSON_CHARS)
        .and_then(|raw| serde_json::from_str::<api_types::InterruptionMetadata>(raw).ok())
        .map(|metadata| SoloTaskInterruption {
            failure_kind: metadata.kind,
            reason: safe_optional_diagnostic(Some(metadata.reason.as_str())),
            source: metadata.source.and_then(|source| safe_diagnostic(&source)),
            execution_id: metadata.execution_id.as_deref().map(safe_identifier),
            recovery_actions: Vec::new(),
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
            // ReviewRunner's durable CI rows use exit_code as the canonical
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

fn retry_dedupe_key(source_id: &str, next_attempt: i64) -> Result<String> {
    if next_attempt < 1 {
        return Err(ServiceError::invalid_operation(
            "retry attempt must be positive",
        ));
    }
    let source_id = path_component_id("turn_job_id", source_id.to_owned())?;
    bounded_owned(
        "retry dedupe_key",
        format!("retry:{source_id}:{next_attempt}"),
        SOLO_MAX_ID_CHARS,
    )
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

fn ensure_expected_turn_version(actual: i64, expected: i64) -> Result<()> {
    if actual != expected {
        return Err(ServiceError::Db(db::DbError::VersionConflict));
    }
    Ok(())
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

fn relevant_event_entity(scope: &SoloSessionScope, event: &ForgeEvent) -> Option<Option<String>> {
    let context = serde_json::to_value(&event.context).ok()?;
    let entity_hint = || safe_event_entity_id(&event.entity_id);
    let exact_project = context
        .get("project_id")
        .and_then(Value::as_str)
        .is_some_and(|project_id| project_id == scope.project_id)
        || (context.get("scope_type").and_then(Value::as_str) == Some("project")
            && context
                .get("scope_id")
                .and_then(Value::as_str)
                .is_some_and(|scope_id| scope_id == scope.project_id));
    if exact_project {
        return Some(entity_hint());
    }
    let exact_chat = context
        .get("chat_id")
        .and_then(Value::as_str)
        .is_some_and(|chat_id| chat_id == scope.project_chat_id)
        || (context.get("scope_type").and_then(Value::as_str) == Some("agent_chat")
            && context
                .get("scope_id")
                .and_then(Value::as_str)
                .is_some_and(|scope_id| scope_id == scope.project_chat_id));
    if exact_chat {
        return Some(entity_hint());
    }
    if event.entity_id == scope.project_id {
        return Some(Some(scope.project_id.clone()));
    }
    // Task/review/execution events often carry only a Task id.  They are
    // harmless invalidation hints; do not echo that opaque id to the TUI until
    // an authoritative refresh proves it belongs to this Project.
    if event.event_type.starts_with("task.")
        || event.event_type.starts_with("review.")
        || event.event_type.starts_with("execution.")
    {
        return Some(None);
    }
    None
}

fn safe_event_entity_id(entity_id: &str) -> Option<String> {
    if entity_id.trim().is_empty()
        || entity_id.chars().count() > SOLO_MAX_ID_CHARS
        || entity_id.chars().any(char::is_control)
        || contains_secret_marker(entity_id)
    {
        None
    } else {
        Some(safe_identifier(entity_id))
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use events::EventContext;

    #[test]
    fn finite_turn_states_keep_terminal_and_waiting_distinct() {
        assert!(!SoloTurnStatus::Queued.is_terminal());
        assert!(!SoloTurnStatus::Leased.is_terminal());
        assert!(!SoloTurnStatus::AwaitingInput.is_terminal());
        assert!(!SoloTurnStatus::RetryWait.is_terminal());
        assert!(SoloTurnStatus::Succeeded.is_terminal());
        assert!(SoloTurnStatus::Failed.is_terminal());
        assert!(SoloTurnStatus::Cancelled.is_terminal());
    }

    #[test]
    fn retry_input_requires_positive_version_and_bounded_key() {
        let input = SoloRetryTurnInput {
            turn_job_id: "turn-123".to_owned(),
            expected_version: 7,
            idempotency_key: "retry-command-7".to_owned(),
        };
        let (turn_job_id, idempotency_key, version) =
            validate_retry_input(input).expect("valid retry input");
        assert_eq!(turn_job_id, "turn-123");
        assert_eq!(idempotency_key, "retry-command-7");
        assert_eq!(version, 7);

        assert!(validate_retry_input(SoloRetryTurnInput {
            turn_job_id: "turn-123".to_owned(),
            expected_version: 0,
            idempotency_key: "retry-command".to_owned(),
        })
        .is_err());
        assert!(validate_retry_input(SoloRetryTurnInput {
            turn_job_id: "../turn".to_owned(),
            expected_version: 1,
            idempotency_key: "retry-command".to_owned(),
        })
        .is_err());
        assert!(validate_retry_input(SoloRetryTurnInput {
            turn_job_id: "turn-123".to_owned(),
            expected_version: 1,
            idempotency_key: " ".to_owned(),
        })
        .is_err());
        assert!(validate_retry_input(SoloRetryTurnInput {
            turn_job_id: "turn-123".to_owned(),
            expected_version: 1,
            idempotency_key: "k".repeat(SOLO_MAX_ID_CHARS + 1),
        })
        .is_err());
        assert!(ensure_expected_turn_version(7, 7).is_ok());
        assert!(matches!(
            ensure_expected_turn_version(8, 7),
            Err(ServiceError::Db(db::DbError::VersionConflict))
        ));
    }

    #[test]
    fn retry_child_dedupe_key_is_deterministic_and_bounded() {
        assert_eq!(
            retry_dedupe_key("turn-123", 2).expect("retry key"),
            "retry:turn-123:2"
        );
        assert!(retry_dedupe_key("turn-123", 0).is_err());
        assert!(retry_dedupe_key("../turn", 2).is_err());
        assert!(retry_dedupe_key(&"t".repeat(SOLO_MAX_ID_CHARS), 2).is_err());
    }

    #[test]
    fn scope_and_refresh_limits_fail_closed_at_the_boundary() {
        let invalid_scope = SoloSessionScope::new("", "project", "repo", "chat");
        assert!(invalid_scope.validate().is_err());
        let escaping_scope = SoloSessionScope::new("owner", "../project", "repo", "chat");
        assert!(escaping_scope.validate().is_err());
        let invalid_limits = SoloRefreshLimits {
            messages: SOLO_MAX_MESSAGES + 1,
            ..SoloRefreshLimits::default()
        };
        assert!(invalid_limits.bounded().is_err());
    }

    #[test]
    fn activity_projection_hides_reasoning_body_and_unknown_payloads() {
        let thinking = LogEntry {
            schema_version: 1,
            sequence: 1,
            timestamp: "2026-01-01T00:00:00Z".to_owned(),
            execution_id: "turn".to_owned(),
            kind: LogKind::Thinking,
            stream: executors::LogStream::Main,
            payload: json!({"text": "private reasoning"}),
            truncated: false,
        };
        let unknown = LogEntry {
            kind: LogKind::Stdout,
            ..thinking.clone()
        };
        let projected = activity_entry(&thinking).expect("thinking marker");
        assert_eq!(projected.kind, SoloActivityKind::Thinking);
        assert!(projected.text.is_none());
        assert!(activity_entry(&unknown).is_none());
    }

    #[test]
    fn suspicious_content_is_replaced_with_a_fixed_marker() {
        let (content, redacted) = visible_content("password=hunter2", "internal");
        assert_eq!(content, "[redacted]");
        assert!(redacted);
        let (content, redacted) = visible_content("ordinary answer", "internal");
        assert_eq!(content, "ordinary answer");
        assert!(!redacted);
        let (content, redacted) = visible_content("task-123 completed", "internal");
        assert_eq!(content, "task-123 completed");
        assert!(!redacted);
        assert!(contains_secret_marker("sk-live-secret"));
        assert!(!contains_secret_marker("task-123"));
    }

    #[test]
    fn unrelated_project_events_are_not_projected_as_entity_hints() {
        let scope = SoloSessionScope::new("owner", "project", "repo", "chat");
        let event = ForgeEvent {
            event_type: "task.updated".to_owned(),
            entity_id: "task-other".to_owned(),
            timestamp: "2026-01-01T00:00:00Z".to_owned(),
            context: EventContext::TaskUpdated {
                project_id: "other-project".to_owned(),
            },
        };
        assert_eq!(relevant_event_entity(&scope, &event), Some(None));
    }

    #[test]
    fn approval_input_requires_positive_versions_and_bounded_ids() {
        let target = SoloCharterApprovalTarget {
            kind: SoloCharterApprovalKind::Adoption,
            project_id: "project".to_owned(),
            charter_id: "charter".to_owned(),
            revision_id: "revision".to_owned(),
            content_digest: "content".to_owned(),
            rendered_digest: "render".to_owned(),
            expected_charter_version: 1,
            expected_project_version: 1,
            approved_project_name: "Project".to_owned(),
            approved_project_slug: None,
            project_mode: "compact".to_owned(),
            selected_project_agent_identity_id: "agent".to_owned(),
            selected_project_agent_profile_revision_id: "profile".to_owned(),
            selected_project_agent_operating_skill_revision: "forge.project.orchestration/v1@1"
                .to_owned(),
            selected_project_agent_policy_digest: "digest".to_owned(),
        };
        let input = SoloCharterApprovalInput {
            target,
            idempotency_key: "approval-key".to_owned(),
            authorization: SoloCharterApprovalAuthorization {
                authorization_event_id: "event".to_owned(),
                authorization_basis: "focused approval".to_owned(),
                authorization_occurred_at: "2026-01-01T00:00:00Z".to_owned(),
            },
        };
        assert!(validate_approval_input(&input).is_ok());
        let mut invalid = input;
        invalid.target.expected_project_version = 0;
        assert!(validate_approval_input(&invalid).is_err());
    }

    #[test]
    fn activity_identifiers_reject_path_escape_components() {
        assert!(path_component_id("turn_job_id", "turn-123".to_owned()).is_ok());
        for value in [
            "../turn",
            "turn/other",
            "turn\\other",
            "/tmp/turn",
            ".",
            "..",
        ] {
            assert!(
                path_component_id("turn_job_id", value.to_owned()).is_err(),
                "{value}"
            );
        }
    }

    #[test]
    fn review_checks_derive_completion_from_exit_code() {
        let checks = parse_review_checks(
            r#"{"ci_steps":[
                {"index":0,"command":"cargo test","exit_code":0},
                {"index":1,"command":"cargo clippy","exit_code":1},
                {"index":2,"command":"manual","success":true}
            ]}"#,
        );

        assert_eq!(checks.len(), 3);
        assert_eq!(checks[0].success, Some(true));
        assert_eq!(checks[1].success, Some(false));
        assert_eq!(checks[2].success, Some(true));
    }

    #[test]
    fn execution_log_path_requires_exact_bound_canonical_location() {
        let root = Path::new("/tmp/solo-worktrees");
        let expected = root
            .join(".forge/logs/project/task/execution.jsonl")
            .to_string_lossy()
            .into_owned();
        assert!(scoped_execution_log_path(root, "project", "task", "execution", &expected).is_ok());
        assert!(scoped_execution_log_path(
            root,
            "project",
            "task",
            "execution",
            "/tmp/other/execution.jsonl"
        )
        .is_err());
        assert!(scoped_execution_log_path(
            root,
            "project",
            "task",
            "execution",
            &root
                .join(".forge/logs/project/task/other.jsonl")
                .to_string_lossy()
        )
        .is_err());
        assert!(scoped_execution_log_path(
            root,
            "project",
            "task",
            "execution",
            &root
                .join(".forge/logs/project/../other/execution.jsonl")
                .to_string_lossy()
        )
        .is_err());
        assert!(normalize_scoped_path(root, &root.join("../outside.jsonl")).is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn activity_reader_rejects_symlinked_log_files() {
        use std::{fs, os::unix::fs::symlink};

        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("logs");
        fs::create_dir_all(&root).expect("log root");
        let target = temporary.path().join("outside.jsonl");
        fs::write(&target, "{}").expect("outside log");
        let linked = root.join("turn.jsonl");
        symlink(&target, &linked).expect("symlink log");

        let result =
            read_activity_path(&linked, &root, "turn", SoloActivityCursor::default(), 1).await;
        assert!(result.is_err());
    }
}
