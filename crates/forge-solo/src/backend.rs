//! Presentation-neutral boundary for a Forge Solo session.
//!
//! The TUI deliberately talks to this module instead of to `db` repositories
//! or individual services.  A local implementation can adapt
//! `services::SoloSessionService` (or the equivalent shared service facade)
//! while tests can provide a small in-memory implementation.  The types in
//! this file contain only information that is safe and useful to render: the
//! adapter is responsible for applying the existing authorization, redaction,
//! optimistic-version, and Project-scope rules before returning a value.

use std::{error::Error, fmt, future::Future, pin::Pin, time::Duration};

use tokio::sync::mpsc;

/// A boxed asynchronous result used by the backend and event-source traits.
///
/// Keeping the future type in the boundary means an adapter may use either
/// `async_trait` or hand-written `Box::pin` implementations without forcing
/// those choices onto the TUI crate.
pub type BackendFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Result returned by a Solo backend operation.
pub type BackendResult<T> = Result<T, BackendError>;

/// Stable identity of the one Project a Solo process may expose.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SoloScope {
    pub owner_id: String,
    pub project_id: String,
    pub repository_id: String,
    pub chat_id: String,
}

impl SoloScope {
    /// Construct a scope from the canonical identifiers resolved by bootstrap.
    pub fn new(
        owner_id: impl Into<String>,
        project_id: impl Into<String>,
        repository_id: impl Into<String>,
        chat_id: impl Into<String>,
    ) -> Self {
        Self {
            owner_id: owner_id.into(),
            project_id: project_id.into(),
            repository_id: repository_id.into(),
            chat_id: chat_id.into(),
        }
    }

    /// Return whether another scope is exactly the same bound Project.
    pub fn same_project(&self, other: &Self) -> bool {
        self.owner_id == other.owner_id
            && self.project_id == other.project_id
            && self.repository_id == other.repository_id
            && self.chat_id == other.chat_id
    }
}

/// Limits applied to durable projection reads.  Keeping these limits on the
/// request makes bounded memory an explicit backend contract rather than a
/// renderer convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotLimits {
    pub messages: usize,
    pub tasks: usize,
    pub attention: usize,
    pub approvals: usize,
    pub interactions: usize,
}

impl Default for SnapshotLimits {
    fn default() -> Self {
        Self {
            messages: 200,
            tasks: 64,
            attention: 64,
            approvals: 32,
            interactions: 32,
        }
    }
}

impl SnapshotLimits {
    /// Apply hard upper bounds even if a caller accidentally requests a very
    /// large page.  These are intentionally conservative for a terminal UI.
    pub fn bounded(self) -> Self {
        Self {
            messages: self.messages.min(2_000),
            tasks: self.tasks.min(512),
            attention: self.attention.min(512),
            approvals: self.approvals.min(256),
            interactions: self.interactions.min(256),
        }
    }
}

/// Authoritative snapshot request.  Older messages may be fetched on demand
/// by increasing `messages` in a later request; the initial projection stays
/// bounded.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SnapshotRequest {
    pub limits: SnapshotLimits,
}

impl SnapshotRequest {
    pub fn bounded(self) -> Self {
        Self {
            limits: self.limits.bounded(),
        }
    }
}

/// Project readiness as understood by the canonical bootstrap/adoption
/// service.  This is intentionally not a second workflow state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectReadiness {
    Setup,
    AwaitingAgent,
    AwaitingCharter,
    AwaitingApproval,
    Operational,
    RecoveryRequired,
}

/// State of the in-process Forge runtime relevant to presentation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeState {
    Starting,
    Recovering { detail: String },
    Ready,
    Degraded { detail: String },
    ShuttingDown,
    Stopped,
}

/// A bounded repository identity shown in the header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositorySnapshot {
    pub id: String,
    pub name: String,
    pub root: String,
    pub default_branch: String,
}

/// Project-level presentation state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectSnapshot {
    pub id: String,
    pub name: String,
    pub readiness: ProjectReadiness,
    pub runtime: RuntimeState,
    pub workflow: String,
    pub selected_agent: Option<AgentSnapshot>,
}

/// Agent identity safe to display in the Solo header/activity provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentSnapshot {
    pub id: String,
    pub name: String,
    pub harness: String,
}

/// Structured local-harness candidate shown by the first-run setup picker.
///
/// This is kept separate from [`AgentSnapshot`]: a selected Agent is a
/// durable Project binding, while setup candidates also carry the latest
/// authentication/availability observation and may all be ineligible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetupAgentSnapshot {
    pub id: String,
    pub name: String,
    pub harness: String,
    pub available: bool,
    pub authenticated: bool,
    pub detail: String,
}

/// Author type for an immutable chat message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageRole {
    User,
    Assistant,
    System,
}

/// Durable message status.  A message is never removed to represent a
/// transient reply; terminal outcomes are represented by the turn and its
/// canonical assistant message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageStatus {
    Complete,
    Pending,
    Failed,
}

/// Immutable chat message projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageSnapshot {
    pub id: String,
    pub sequence: i64,
    pub role: MessageRole,
    pub content: String,
    pub created_at: String,
    pub turn_id: Option<String>,
    pub status: MessageStatus,
}

/// Finite Agent Chat turn states.  The names intentionally match the durable
/// turn service states rather than inventing Solo-specific states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnState {
    Queued,
    Leased,
    AwaitingInput,
    RetryWait,
    Succeeded,
    Failed,
    Cancelled,
}

/// A typed failure shown in an attention card or turn row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureSnapshot {
    pub kind: String,
    pub headline: String,
    pub detail: String,
    pub retryable: bool,
}

/// Durable finite turn projection.  `reply` is only a bounded preview; the
/// canonical assistant message is authoritative once the turn succeeds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnSnapshot {
    pub id: String,
    pub triggering_message_id: String,
    pub state: TurnState,
    pub version: i64,
    pub attempt: u32,
    pub reply: Option<String>,
    pub assistant_message_id: Option<String>,
    pub failure: Option<FailureSnapshot>,
    pub retryable: bool,
}

/// A typed runtime question.  The adapter must omit protected values and
/// secret answers from this presentation projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InteractionSnapshot {
    pub id: String,
    pub turn_id: String,
    pub prompt: String,
    pub fields: Vec<InteractionField>,
    pub expected_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InteractionField {
    pub id: String,
    pub label: String,
    pub required: bool,
    pub choices: Vec<String>,
}

/// The chat portion of a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatSnapshot {
    pub id: String,
    pub messages: Vec<MessageSnapshot>,
    pub has_older_messages: bool,
    pub active_turns: Vec<TurnSnapshot>,
    pub interactions: Vec<InteractionSnapshot>,
}

/// Workflow task state shown in the Project rail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Todo,
    InProgress,
    Blocked,
    Review,
    Done,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckStatus {
    Pending,
    Running,
    Passed,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckSnapshot {
    pub name: String,
    pub status: CheckStatus,
    pub summary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitEvidence {
    pub commit: Option<String>,
    pub changed_files: Vec<String>,
    pub merged: bool,
    pub summary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSnapshot {
    pub id: String,
    pub title: String,
    pub state: TaskState,
    pub version: i64,
    pub worker: Option<AgentSnapshot>,
    pub reviewer: Option<AgentSnapshot>,
    pub checks: Vec<CheckSnapshot>,
    pub commit: Option<CommitEvidence>,
    pub blocker: Option<FailureSnapshot>,
    pub retryable: bool,
}

/// Typed attention kind.  The renderer should use this kind and
/// `permitted_action`, not infer a recovery affordance from prose.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttentionKind {
    Approval,
    FailedCheck,
    RetryExhausted,
    AgentUnavailable,
    RecoveryRequired,
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttentionAction {
    Approve,
    Reject,
    Retry,
    Cancel,
    Refresh,
    Recover,
    Review,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttentionSnapshot {
    pub id: String,
    pub kind: AttentionKind,
    pub headline: String,
    pub detail: String,
    pub affected_id: Option<String>,
    pub permitted_action: Option<AttentionAction>,
}

/// Exact approval target.  The expected version and digest are both sent
/// back to the canonical command service; a stale target is a conflict, never
/// a best-effort approval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalKind {
    CharterAdoption,
    AgentAction,
    Recovery,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalAction {
    Approve,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalSnapshot {
    pub id: String,
    pub kind: ApprovalKind,
    pub title: String,
    pub summary: String,
    pub revision: String,
    pub digest: String,
    /// Operating-skill revision participating in the exact approval target,
    /// when the target is a Charter/adoption receipt.
    pub operating_skill_revision: Option<String>,
    pub expected_version: i64,
    pub selected_agent: Option<AgentSnapshot>,
    pub permitted_actions: Vec<ApprovalAction>,
}

/// The authoritative, bounded projection rendered by the TUI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SoloSnapshot {
    pub scope: SoloScope,
    pub repository: RepositorySnapshot,
    pub project: ProjectSnapshot,
    pub chat: ChatSnapshot,
    /// Bounded structured candidates for setup/recovery. This is refreshed
    /// from local adapter discovery so a CLI authenticated after launch can
    /// appear without restarting the process.
    pub setup_agents: Vec<SetupAgentSnapshot>,
    /// Bounded live activity summaries for active turn/execution targets.
    /// Additional log entries are fetched using `ActivityReadRequest`.
    pub live_activity: Vec<LiveActivitySnapshot>,
    pub tasks: Vec<TaskSnapshot>,
    pub attention: Vec<AttentionSnapshot>,
    pub approvals: Vec<ApprovalSnapshot>,
    pub refreshed_at: String,
}

/// A stable idempotency identity.  The same value must be reused when the
/// result of a send/cancel/retry is temporarily unknown.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.trim().is_empty()
    }
}

impl From<String> for IdempotencyKey {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for IdempotencyKey {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

/// Request to append one immutable user message and admit its turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendMessageRequest {
    pub content: String,
    pub idempotency_key: IdempotencyKey,
}

impl SendMessageRequest {
    pub fn new(content: impl Into<String>, idempotency_key: impl Into<IdempotencyKey>) -> Self {
        Self {
            content: content.into(),
            idempotency_key: idempotency_key.into(),
        }
    }
}

/// A typed interaction answer.  Values are sent through the canonical
/// interaction service and are not treated as approval prose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InteractionAnswerRequest {
    pub interaction_id: String,
    pub turn_id: String,
    pub expected_version: i64,
    pub answers: Vec<InteractionAnswer>,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InteractionAnswer {
    pub field_id: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnMutationRequest {
    pub turn_id: String,
    pub expected_version: i64,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewDecision {
    Accept,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewDecisionRequest {
    pub task_id: String,
    pub expected_version: i64,
    pub target_digest: String,
    pub decision: ReviewDecision,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalDecisionRequest {
    pub approval_id: String,
    pub expected_version: i64,
    pub target_digest: String,
    pub action: ApprovalAction,
    pub idempotency_key: IdempotencyKey,
}

/// Request to persist the explicitly selected Project Agent during setup.
///
/// Agent discovery/eligibility is owned by the bootstrap adapter; the TUI only
/// sends the stable identity chosen by the user and the idempotency key
/// generated by its pending command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectAgentRequest {
    pub agent_id: String,
    pub idempotency_key: IdempotencyKey,
}

/// Commands exposed by a bound Solo backend.  Every mutation carries an
/// expected version/digest where the underlying service supports one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendCommand {
    SelectAgent(SelectAgentRequest),
    SendMessage(SendMessageRequest),
    AnswerInteraction(InteractionAnswerRequest),
    CancelTurn(TurnMutationRequest),
    RetryTurn(TurnMutationRequest),
    DecideReview(ReviewDecisionRequest),
    DecideApproval(ApprovalDecisionRequest),
}

impl BackendCommand {
    pub fn idempotency_key(&self) -> &IdempotencyKey {
        match self {
            Self::SelectAgent(request) => &request.idempotency_key,
            Self::SendMessage(request) => &request.idempotency_key,
            Self::AnswerInteraction(request) => &request.idempotency_key,
            Self::CancelTurn(request) | Self::RetryTurn(request) => &request.idempotency_key,
            Self::DecideReview(request) => &request.idempotency_key,
            Self::DecideApproval(request) => &request.idempotency_key,
        }
    }

    pub fn target_id(&self) -> Option<&str> {
        match self {
            Self::SelectAgent(request) => Some(&request.agent_id),
            Self::SendMessage(_) => None,
            Self::AnswerInteraction(request) => Some(&request.interaction_id),
            Self::CancelTurn(request) | Self::RetryTurn(request) => Some(&request.turn_id),
            Self::DecideReview(request) => Some(&request.task_id),
            Self::DecideApproval(request) => Some(&request.approval_id),
        }
    }
}

/// Result of a command.  The returned record is suitable for immediate
/// reducer application; a later authoritative refresh remains the source of
/// truth after invalidations or conflicts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendCommandResult {
    AgentSelected {
        agent_id: String,
        replayed: bool,
    },
    MessageSent {
        message: MessageSnapshot,
        turn: TurnSnapshot,
        replayed: bool,
    },
    InteractionAnswered {
        turn: TurnSnapshot,
        replayed: bool,
    },
    TurnCancelled {
        turn: TurnSnapshot,
        replayed: bool,
    },
    TurnRetried {
        turn: TurnSnapshot,
        replayed: bool,
    },
    ReviewDecided {
        task: TaskSnapshot,
        replayed: bool,
    },
    ApprovalDecided {
        approval_id: String,
        project: ProjectSnapshot,
        replayed: bool,
    },
}

/// Activity is read from the existing Forge JSONL logs through the adapter's
/// `executors::LogReader` integration.  No raw/protected payload is present in
/// this type; adapters should provide only bounded summaries/previews.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ActivityTarget {
    pub project_id: String,
    pub turn_id: Option<String>,
    pub task_id: Option<String>,
    pub execution_id: String,
    pub attempt: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ActivityCursor {
    pub execution_id: String,
    pub attempt: u32,
    pub next_sequence: u64,
    /// File length observed with `next_sequence`. A shrink/rotation makes
    /// this cursor stale even when the sequence number happens to repeat.
    pub file_size: u64,
}

impl ActivityCursor {
    pub fn beginning(target: &ActivityTarget) -> Self {
        Self {
            execution_id: target.execution_id.clone(),
            attempt: target.attempt,
            next_sequence: 0,
            file_size: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityKind {
    ToolCall,
    ToolResult,
    AssistantDelta,
    Assistant,
    ExecutionState,
    FileChange,
    ShellCommand,
    User,
    System,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityEntry {
    pub sequence: u64,
    pub attempt: u32,
    pub kind: ActivityKind,
    pub summary: String,
    pub preview: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityReadRequest {
    pub target: ActivityTarget,
    pub cursor: ActivityCursor,
    pub limit: usize,
}

impl ActivityReadRequest {
    pub fn bounded(mut self, max_entries: usize) -> Self {
        self.limit = self.limit.min(max_entries.max(1));
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityBatch {
    pub target: ActivityTarget,
    pub entries: Vec<ActivityEntry>,
    pub next_cursor: ActivityCursor,
    pub has_more: bool,
    /// Set when the log rotated or was truncated and the requested sequence
    /// is no longer valid.  The reducer should preserve attempt provenance
    /// while replacing the cursor with `next_cursor`.
    pub cursor_reset: bool,
    pub finished: bool,
}

/// Current bounded live activity for one active turn or Task execution.  The
/// adapter may populate this from the durable turn/execution record plus the
/// latest JSONL page; the cursor lets the controller continue incrementally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveActivitySnapshot {
    pub target: ActivityTarget,
    pub state: TurnState,
    pub summary: String,
    pub worker: Option<AgentSnapshot>,
    pub reviewer: Option<AgentSnapshot>,
    pub entries: Vec<ActivityEntry>,
    pub cursor: ActivityCursor,
}

/// Why a durable projection should be refreshed.  Events are hints only;
/// authoritative reads determine the final rendered state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidationReason {
    Chat,
    Turn,
    Task,
    Attention,
    Approval,
    Project,
    Runtime,
    Recovery,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendEventKind {
    SnapshotInvalidated { reason: InvalidationReason },
    ActivityAvailable { target: ActivityTarget },
    RuntimeStateChanged { state: RuntimeState },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendEvent {
    pub scope: SoloScope,
    pub sequence: u64,
    pub kind: BackendEventKind,
}

impl BackendEvent {
    pub fn invalidate(scope: SoloScope, sequence: u64, reason: InvalidationReason) -> Self {
        Self {
            scope,
            sequence,
            kind: BackendEventKind::SnapshotInvalidated { reason },
        }
    }

    pub fn activity(scope: SoloScope, sequence: u64, target: ActivityTarget) -> Self {
        Self {
            scope,
            sequence,
            kind: BackendEventKind::ActivityAvailable { target },
        }
    }
}

/// Poll result from a durable EventBus adapter.  A lagged broadcast receiver
/// is deliberately represented as a refresh hint instead of a fatal error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendEventPoll {
    Event(BackendEvent),
    Lagged { skipped: u64 },
    Closed,
    Unavailable { detail: String },
}

/// Source of typed backend invalidation/activity hints.
pub trait BackendEventSource: Send {
    fn recv(&mut self) -> BackendFuture<'_, BackendEventPoll>;
}

/// A Tokio channel-backed event source useful for the local EventBus adapter
/// and deterministic controller tests.
pub struct ChannelBackendEventSource {
    receiver: mpsc::Receiver<BackendEvent>,
}

impl ChannelBackendEventSource {
    pub fn new(receiver: mpsc::Receiver<BackendEvent>) -> Self {
        Self { receiver }
    }
}

impl BackendEventSource for ChannelBackendEventSource {
    fn recv(&mut self) -> BackendFuture<'_, BackendEventPoll> {
        Box::pin(async move {
            match self.receiver.recv().await {
                Some(event) => BackendEventPoll::Event(event),
                None => BackendEventPoll::Closed,
            }
        })
    }
}

/// Construct a bounded event channel and its receiving source.
pub fn backend_event_channel(
    capacity: usize,
) -> (mpsc::Sender<BackendEvent>, ChannelBackendEventSource) {
    let (sender, receiver) = mpsc::channel(capacity.max(1));
    (sender, ChannelBackendEventSource::new(receiver))
}

/// Intent passed to the runtime supervisor when the controller is leaving.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownIntent {
    UserQuit,
    InputClosed,
    Signal,
    StartupFailure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShutdownOutcome {
    pub completed: bool,
    pub timed_out: bool,
    pub active_operations: usize,
}

impl ShutdownOutcome {
    pub fn completed() -> Self {
        Self {
            completed: true,
            timed_out: false,
            active_operations: 0,
        }
    }

    pub fn timed_out(active_operations: usize) -> Self {
        Self {
            completed: false,
            timed_out: true,
            active_operations,
        }
    }
}

/// Bounded, renderer-safe backend errors.  A service adapter should map
/// `DbError`/`ServiceError` into this type at the boundary and avoid exposing
/// lower-layer implementation details to the TUI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendErrorKind {
    Conflict,
    ScopeViolation,
    InvalidInput,
    Unavailable,
    Cancelled,
    Transport,
    Internal,
}

/// Whether an error message may be rendered.  A service adapter should use
/// `Protected` for provider/internal bodies and provide a separate safe
/// headline through the normal attention snapshot when one is available.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorVisibility {
    Public,
    Protected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictTarget {
    pub kind: String,
    pub id: String,
    pub expected_version: Option<i64>,
    pub expected_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendError {
    pub kind: BackendErrorKind,
    pub message: String,
    pub target: Option<ConflictTarget>,
    pub visibility: ErrorVisibility,
}

impl BackendError {
    pub fn new(kind: BackendErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: bounded_error_message(message.into()),
            target: None,
            visibility: ErrorVisibility::Public,
        }
    }

    pub fn conflict(message: impl Into<String>, target: Option<ConflictTarget>) -> Self {
        Self {
            kind: BackendErrorKind::Conflict,
            message: bounded_error_message(message.into()),
            target,
            visibility: ErrorVisibility::Public,
        }
    }

    pub fn protected(kind: BackendErrorKind) -> Self {
        Self {
            kind,
            message: String::new(),
            target: None,
            visibility: ErrorVisibility::Protected,
        }
    }

    pub fn scope_violation(message: impl Into<String>) -> Self {
        Self::new(BackendErrorKind::ScopeViolation, message)
    }

    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::new(BackendErrorKind::InvalidInput, message)
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(BackendErrorKind::Unavailable, message)
    }

    pub fn transport(message: impl Into<String>) -> Self {
        Self::new(BackendErrorKind::Transport, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(BackendErrorKind::Internal, message)
    }

    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::new(BackendErrorKind::Cancelled, message)
    }

    pub fn is_conflict(&self) -> bool {
        self.kind == BackendErrorKind::Conflict
    }

    pub fn is_public(&self) -> bool {
        self.visibility == ErrorVisibility::Public
    }
}

impl fmt::Display for BackendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for BackendError {}

const MAX_BACKEND_ERROR_CHARS: usize = 2_048;

fn bounded_error_message(message: String) -> String {
    message.chars().take(MAX_BACKEND_ERROR_CHARS).collect()
}

/// Adapter contract for the local, already-scoped Solo session.
///
/// A concrete implementation should bind this trait to one immutable
/// `SoloScope`, delegate `execute` to the existing typed command services,
/// build snapshots from authoritative durable queries, and use
/// `executors::LogReader` for `read_activity`.  The controller never receives
/// a database handle and never infers domain meaning from text.
pub trait SoloBackend: Send + Sync + 'static {
    fn scope(&self) -> SoloScope;

    fn refresh(&self, request: SnapshotRequest) -> BackendFuture<'_, BackendResult<SoloSnapshot>>;

    fn read_activity(
        &self,
        request: ActivityReadRequest,
    ) -> BackendFuture<'_, BackendResult<ActivityBatch>>;

    fn execute(
        &self,
        command: BackendCommand,
    ) -> BackendFuture<'_, BackendResult<BackendCommandResult>>;

    fn shutdown(
        &self,
        intent: ShutdownIntent,
        deadline: Duration,
    ) -> BackendFuture<'_, BackendResult<ShutdownOutcome>>;
}

/// Ensure an authoritative snapshot cannot cross the session boundary.
pub fn ensure_snapshot_scope(expected: &SoloScope, snapshot: &SoloSnapshot) -> BackendResult<()> {
    if expected.same_project(&snapshot.scope)
        && snapshot.project.id == expected.project_id
        && snapshot.repository.id == expected.repository_id
        && snapshot.chat.id == expected.chat_id
    {
        Ok(())
    } else {
        Err(BackendError::scope_violation(
            "backend returned a snapshot outside the bound Solo Project",
        ))
    }
}

/// Ensure an activity request/result belongs to the bound Project.
pub fn ensure_activity_scope(expected: &SoloScope, target: &ActivityTarget) -> BackendResult<()> {
    if expected.project_id == target.project_id {
        Ok(())
    } else {
        Err(BackendError::scope_violation(
            "activity target is outside the bound Solo Project",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activity_requests_are_hard_bounded_and_keep_cursor() {
        let target = ActivityTarget {
            project_id: "project".into(),
            turn_id: Some("turn".into()),
            task_id: None,
            execution_id: "execution".into(),
            attempt: 2,
        };
        let request = ActivityReadRequest {
            cursor: ActivityCursor::beginning(&target),
            target,
            limit: usize::MAX,
        }
        .bounded(64);

        assert_eq!(request.limit, 64);
        assert_eq!(request.cursor.next_sequence, 0);
        assert_eq!(request.cursor.attempt, 2);
    }

    #[test]
    fn command_keys_are_stable_across_retry_and_cancel() {
        let cancel = BackendCommand::CancelTurn(TurnMutationRequest {
            turn_id: "turn".into(),
            expected_version: 7,
            idempotency_key: IdempotencyKey::from("turn-cancel-7"),
        });
        let retry = BackendCommand::RetryTurn(TurnMutationRequest {
            turn_id: "turn".into(),
            expected_version: 8,
            idempotency_key: IdempotencyKey::from("turn-retry-8"),
        });

        assert_eq!(cancel.idempotency_key().as_str(), "turn-cancel-7");
        assert_eq!(retry.idempotency_key().as_str(), "turn-retry-8");
        assert_eq!(cancel.target_id(), Some("turn"));
        assert_eq!(retry.target_id(), Some("turn"));
    }

    #[test]
    fn scope_checks_reject_cross_project_records() {
        let expected = SoloScope::new("owner", "project-a", "repo", "chat");
        let actual = SoloScope::new("owner", "project-b", "repo", "chat");
        let snapshot = SoloSnapshot {
            scope: actual,
            repository: RepositorySnapshot {
                id: "repo".into(),
                name: "repo".into(),
                root: "/repo".into(),
                default_branch: "main".into(),
            },
            project: ProjectSnapshot {
                id: "project-b".into(),
                name: "other".into(),
                readiness: ProjectReadiness::Operational,
                runtime: RuntimeState::Ready,
                workflow: "autonomous_v1".into(),
                selected_agent: None,
            },
            chat: ChatSnapshot {
                id: "chat".into(),
                messages: Vec::new(),
                has_older_messages: false,
                active_turns: Vec::new(),
                interactions: Vec::new(),
            },
            setup_agents: Vec::new(),
            live_activity: Vec::new(),
            tasks: Vec::new(),
            attention: Vec::new(),
            approvals: Vec::new(),
            refreshed_at: "now".into(),
        };

        assert!(ensure_snapshot_scope(&expected, &snapshot).is_err());
    }

    #[test]
    fn scope_checks_reject_mismatched_nested_snapshot_ids() {
        let expected = SoloScope::new("owner", "project", "repo", "chat");
        let mut snapshot = SoloSnapshot {
            scope: expected.clone(),
            repository: RepositorySnapshot {
                id: "repo".into(),
                name: "repo".into(),
                root: "/repo".into(),
                default_branch: "main".into(),
            },
            project: ProjectSnapshot {
                id: "project".into(),
                name: "project".into(),
                readiness: ProjectReadiness::Setup,
                runtime: RuntimeState::Starting,
                workflow: "autonomous_v1".into(),
                selected_agent: None,
            },
            chat: ChatSnapshot {
                id: "chat".into(),
                messages: Vec::new(),
                has_older_messages: false,
                active_turns: Vec::new(),
                interactions: Vec::new(),
            },
            setup_agents: Vec::new(),
            live_activity: Vec::new(),
            tasks: Vec::new(),
            attention: Vec::new(),
            approvals: Vec::new(),
            refreshed_at: "now".into(),
        };
        snapshot.chat.id = "other-chat".into();
        assert!(ensure_snapshot_scope(&expected, &snapshot).is_err());
    }

    #[test]
    fn backend_errors_are_bounded_and_can_hide_protected_bodies() {
        let public = BackendError::internal("x".repeat(3_000));
        assert_eq!(public.message.chars().count(), 2_048);
        assert_eq!(public.visibility, ErrorVisibility::Public);

        let protected = BackendError::protected(BackendErrorKind::Transport);
        assert!(protected.message.is_empty());
        assert_eq!(protected.visibility, ErrorVisibility::Protected);
    }
}
