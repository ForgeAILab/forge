//! Reducer-driven state for the Forge Solo terminal UI.
//!
//! This module deliberately has no dependency on Forge's database or service
//! traits.  [`AppState`] is a presentation model: the backend turns durable
//! records into the small, typed snapshots below and the controller feeds
//! commands produced by [`AppState::reduce`] back to the service facade.  The
//! separation is useful in its own right (the reducer and renderer are easy to
//! test with deterministic data) and keeps it impossible for a render pass to
//! mutate a Project.

use std::{collections::BTreeMap, fmt};

use uuid::Uuid;

/// The maximum amount of state retained by the UI.  Durable history remains
/// in Forge; these limits only protect a long-lived terminal process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppLimits {
    pub timeline_messages: usize,
    pub activity_items: usize,
    pub tasks: usize,
    pub attention_items: usize,
    pub checks_per_task: usize,
    pub notifications: usize,
    pub composer_chars: usize,
}

impl Default for AppLimits {
    fn default() -> Self {
        Self {
            timeline_messages: 240,
            activity_items: 120,
            tasks: 48,
            attention_items: 48,
            checks_per_task: 24,
            notifications: 8,
            composer_chars: 32_768,
        }
    }
}

/// Responsive breakpoints used by the view.  The narrow view is intentionally
/// a real layout, rather than a clipped wide layout.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LayoutMode {
    #[default]
    Wide,
    Narrow,
}

impl LayoutMode {
    /// Choose the layout from the drawable terminal size.
    pub fn for_size(width: u16, height: u16) -> Self {
        if width < 104 || height < 24 {
            Self::Narrow
        } else {
            Self::Wide
        }
    }
}

/// The two top-level Solo work surfaces.
///
/// Kanban is intentionally the default: repository work should be legible
/// before a person chooses to enter the Project Agent conversation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PrimaryView {
    #[default]
    Kanban,
    MainChat,
}

impl PrimaryView {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Kanban => "KANBAN",
            Self::MainChat => "MAIN CHAT",
        }
    }

    pub const fn next(self) -> Self {
        match self {
            Self::Kanban => Self::MainChat,
            Self::MainChat => Self::Kanban,
        }
    }

    pub const fn previous(self) -> Self {
        self.next()
    }

    pub const fn default_focus(self) -> FocusTarget {
        match self {
            Self::Kanban => FocusTarget::ProjectRail,
            Self::MainChat => FocusTarget::Composer,
        }
    }
}

/// The portion of the interface that receives keyboard focus.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FocusTarget {
    #[default]
    Composer,
    Timeline,
    Activity,
    ProjectRail,
    Modal,
    SetupPicker,
    Help,
}

impl FocusTarget {
    fn next(self, view: PrimaryView) -> Self {
        match (view, self) {
            (_, Self::Modal | Self::Help | Self::SetupPicker) => self,
            (PrimaryView::Kanban, _) => Self::ProjectRail,
            (PrimaryView::MainChat, Self::Composer) => Self::Timeline,
            (PrimaryView::MainChat, Self::Timeline) => Self::Activity,
            (PrimaryView::MainChat, Self::Activity) => Self::Composer,
            (PrimaryView::MainChat, Self::ProjectRail) => Self::Composer,
        }
    }

    fn previous(self, view: PrimaryView) -> Self {
        match (view, self) {
            (_, Self::Modal | Self::Help | Self::SetupPicker) => self,
            (PrimaryView::Kanban, _) => Self::ProjectRail,
            (PrimaryView::MainChat, Self::Composer) => Self::Activity,
            (PrimaryView::MainChat, Self::Timeline) => Self::Composer,
            (PrimaryView::MainChat, Self::Activity) => Self::Timeline,
            (PrimaryView::MainChat, Self::ProjectRail) => Self::Activity,
        }
    }
}

/// Whether content may be shown to the person at the terminal.
///
/// Backends should mark internal/provider error bodies as `Protected`.  The
/// view omits protected bodies entirely; it never attempts to redact them by
/// guessing at provider-specific formats.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ContentVisibility {
    #[default]
    Public,
    Protected,
}

impl ContentVisibility {
    pub const fn is_public(self) -> bool {
        matches!(self, Self::Public)
    }
}

/// Readiness of the repository-scoped Project.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectReadiness {
    Setup { step: SetupStep },
    AwaitingAdoption,
    Recovering,
    Ready,
    Blocked { reason: String },
    Unavailable { reason: String },
}

impl Default for ProjectReadiness {
    fn default() -> Self {
        Self::Setup {
            step: SetupStep::ChooseAgent,
        }
    }
}

impl ProjectReadiness {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Setup { .. } => "SETUP",
            Self::AwaitingAdoption => "ADOPTION REQUIRED",
            Self::Recovering => "RECOVERING",
            Self::Ready => "READY",
            Self::Blocked { .. } => "BLOCKED",
            Self::Unavailable { .. } => "UNAVAILABLE",
        }
    }

    pub fn allows_chat(&self) -> bool {
        matches!(self, Self::Ready | Self::AwaitingAdoption)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetupStep {
    ChooseAgent,
    ConfirmAgent,
    DraftCharter,
    ApproveAdoption,
    Ready,
}

impl SetupStep {
    pub const fn label(self) -> &'static str {
        match self {
            Self::ChooseAgent => "choose an authenticated local Agent",
            Self::ConfirmAgent => "confirm the Agent",
            Self::DraftCharter => "draft the Project Charter",
            Self::ApproveAdoption => "approve Project adoption",
            Self::Ready => "ready",
        }
    }
}

/// Identity shown in the first-run picker.  The backend is responsible for
/// including only structured availability/authentication results.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentCandidate {
    pub id: String,
    pub label: String,
    pub kind: String,
    pub available: bool,
    pub authenticated: bool,
    pub detail: String,
}

impl AgentCandidate {
    pub fn eligible(&self) -> bool {
        self.available && self.authenticated
    }
}

/// Bootstrap/setup projection.  This is intentionally separate from the
/// durable Project snapshot so the picker can be rendered before a Project is
/// fully admitted.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum SetupState {
    #[default]
    NotStarted,
    AgentPicker {
        candidates: Vec<AgentCandidate>,
        selected: usize,
        detail: String,
    },
    Unavailable {
        detail: String,
        retryable: bool,
    },
    Adoption {
        outcome: String,
        approval: Option<ApprovalCard>,
    },
    Ready,
}

impl SetupState {
    pub fn selected_candidate(&self) -> Option<&AgentCandidate> {
        match self {
            Self::AgentPicker {
                candidates,
                selected,
                ..
            } => candidates.get(*selected),
            _ => None,
        }
    }

    fn move_selection(&mut self, delta: isize) {
        let Self::AgentPicker {
            candidates,
            selected,
            ..
        } = self
        else {
            return;
        };
        if candidates.is_empty() {
            *selected = 0;
            return;
        }
        let len = candidates.len();
        let next = (*selected as isize + delta).rem_euclid(len as isize) as usize;
        *selected = next;
    }
}

/// Header data is supplied by the backend and rendered without looking up
/// any domain records during a draw.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeaderState {
    pub repository: String,
    pub project: String,
    pub agent: String,
    pub readiness: ProjectReadiness,
    pub runtime: RuntimeState,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RuntimeState {
    #[default]
    Starting,
    Recovering,
    Ready,
    Busy,
    ShuttingDown,
    ForcedShutdown,
    Stopped,
    Failed,
}

impl RuntimeState {
    pub fn label(self) -> &'static str {
        match self {
            Self::Starting => "STARTING",
            Self::Recovering => "RECOVERING",
            Self::Ready => "READY",
            Self::Busy => "BUSY",
            Self::ShuttingDown => "SHUTTING DOWN",
            Self::ForcedShutdown => "FORCED SHUTDOWN",
            Self::Stopped => "STOPPED",
            Self::Failed => "FAILED",
        }
    }

    pub fn accepts_input(self) -> bool {
        matches!(self, Self::Ready | Self::Busy)
    }
}

/// Role of a durable message in the Project Agent Chat timeline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageRole {
    User,
    Assistant,
    System,
    Worker,
    Reviewer,
    Error,
}

impl MessageRole {
    pub const fn label(self) -> &'static str {
        match self {
            Self::User => "YOU",
            Self::Assistant => "AGENT",
            Self::System => "SYSTEM",
            Self::Worker => "WORKER",
            Self::Reviewer => "REVIEWER",
            Self::Error => "ERROR",
        }
    }
}

/// One authoritative chat message.  `id` is a durable message ID; it is also
/// used to replace optimistic/provisional entries when a fresh projection is
/// applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChatMessage {
    pub id: String,
    pub role: MessageRole,
    pub content: String,
    pub timestamp: String,
    pub attempt: Option<u32>,
    pub visibility: ContentVisibility,
}

impl ChatMessage {
    pub fn new(
        id: impl Into<String>,
        role: MessageRole,
        content: impl Into<String>,
        timestamp: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            role,
            content: content.into(),
            timestamp: timestamp.into(),
            attempt: None,
            visibility: ContentVisibility::Public,
        }
    }

    pub fn protected(
        id: impl Into<String>,
        role: MessageRole,
        timestamp: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            role,
            content: String::new(),
            timestamp: timestamp.into(),
            attempt: None,
            visibility: ContentVisibility::Protected,
        }
    }

    pub fn is_renderable(&self) -> bool {
        self.visibility.is_public()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TurnState {
    #[default]
    Queued,
    Running,
    AwaitingInput,
    AwaitingApproval,
    Cancelling,
    RetryWait,
    Failed,
    Cancelled,
    Succeeded,
}

impl TurnState {
    pub fn label(self) -> &'static str {
        match self {
            Self::Queued => "QUEUED",
            Self::Running => "LIVE",
            Self::AwaitingInput => "AWAITING INPUT",
            Self::AwaitingApproval => "AWAITING APPROVAL",
            Self::Cancelling => "CANCELLING",
            Self::RetryWait => "RETRY WAIT",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
            Self::Succeeded => "DONE",
        }
    }

    pub const fn is_live(self) -> bool {
        matches!(
            self,
            Self::Queued
                | Self::Running
                | Self::AwaitingInput
                | Self::AwaitingApproval
                | Self::Cancelling
                | Self::RetryWait
        )
    }

    pub const fn is_retryable(self) -> bool {
        matches!(self, Self::Failed | Self::Cancelled | Self::RetryWait)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivityKind {
    Status,
    Tool,
    Execution,
    Worker,
    Reviewer,
    Reasoning,
    Error,
}

impl ActivityKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Status => "STATUS",
            Self::Tool => "TOOL",
            Self::Execution => "EXEC",
            Self::Worker => "WORKER",
            Self::Reviewer => "REVIEW",
            Self::Reasoning => "THOUGHT",
            Self::Error => "ERROR",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ActivityStatus {
    #[default]
    Running,
    Complete,
    Failed,
    Skipped,
}

impl ActivityStatus {
    pub const fn marker(self) -> &'static str {
        match self {
            Self::Running => "…",
            Self::Complete => "OK",
            Self::Failed => "FAIL",
            Self::Skipped => "--",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivityItem {
    pub sequence: u64,
    pub kind: ActivityKind,
    pub status: ActivityStatus,
    pub label: String,
    pub detail: String,
    pub visibility: ContentVisibility,
}

impl ActivityItem {
    pub fn new(
        sequence: u64,
        kind: ActivityKind,
        status: ActivityStatus,
        label: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            sequence,
            kind,
            status,
            label: label.into(),
            detail: detail.into(),
            visibility: ContentVisibility::Public,
        }
    }

    pub fn protected(
        sequence: u64,
        kind: ActivityKind,
        status: ActivityStatus,
        label: impl Into<String>,
    ) -> Self {
        Self {
            sequence,
            kind,
            status,
            label: label.into(),
            detail: String::new(),
            visibility: ContentVisibility::Protected,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveActivity {
    pub turn_id: String,
    pub attempt: u32,
    pub state: TurnState,
    pub summary: String,
    pub worker: String,
    pub items: Vec<ActivityItem>,
    pub expanded: bool,
    pub reasoning_expanded: bool,
}

impl LiveActivity {
    pub fn new(turn_id: impl Into<String>, attempt: u32, summary: impl Into<String>) -> Self {
        Self {
            turn_id: turn_id.into(),
            attempt,
            state: TurnState::Running,
            summary: summary.into(),
            worker: String::new(),
            items: Vec::new(),
            expanded: false,
            reasoning_expanded: false,
        }
    }

    pub fn append(&mut self, item: ActivityItem, max_items: usize) {
        if self
            .items
            .last()
            .is_some_and(|existing| item.sequence <= existing.sequence)
        {
            return;
        }
        self.items.push(item);
        retain_latest(&mut self.items, max_items);
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TaskState {
    #[default]
    Queued,
    Running,
    AwaitingReview,
    Blocked,
    Failed,
    Merging,
    CleaningUp,
    Succeeded,
    Cancelled,
}

/// Stable columns used by the Solo Kanban projection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum KanbanLane {
    #[default]
    Queued,
    Active,
    Review,
    Blocked,
    Done,
}

impl KanbanLane {
    pub const ALL: [Self; 5] = [
        Self::Queued,
        Self::Active,
        Self::Review,
        Self::Blocked,
        Self::Done,
    ];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Queued => "QUEUED",
            Self::Active => "ACTIVE",
            Self::Review => "REVIEW",
            Self::Blocked => "BLOCKED",
            Self::Done => "DONE",
        }
    }

    pub const fn index(self) -> usize {
        match self {
            Self::Queued => 0,
            Self::Active => 1,
            Self::Review => 2,
            Self::Blocked => 3,
            Self::Done => 4,
        }
    }

    pub const fn next(self) -> Self {
        Self::ALL[(self.index() + 1) % Self::ALL.len()]
    }

    pub const fn previous(self) -> Self {
        Self::ALL[(self.index() + Self::ALL.len() - 1) % Self::ALL.len()]
    }
}

impl TaskState {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Queued => "QUEUED",
            Self::Running => "RUNNING",
            Self::AwaitingReview => "REVIEW",
            Self::Blocked => "BLOCKED",
            Self::Failed => "FAILED",
            Self::Merging => "MERGING",
            Self::CleaningUp => "CLEANUP",
            Self::Succeeded => "DONE",
            Self::Cancelled => "CANCELLED",
        }
    }

    pub const fn marker(self) -> &'static str {
        match self {
            Self::Queued => "○",
            Self::Running => "▶",
            Self::AwaitingReview => "?",
            Self::Blocked => "!",
            Self::Failed => "×",
            Self::Merging => "⇢",
            Self::CleaningUp => "↻",
            Self::Succeeded => "✓",
            Self::Cancelled => "−",
        }
    }

    pub const fn kanban_lane(self) -> KanbanLane {
        match self {
            Self::Queued => KanbanLane::Queued,
            Self::Running | Self::Merging | Self::CleaningUp => KanbanLane::Active,
            Self::AwaitingReview => KanbanLane::Review,
            Self::Blocked | Self::Failed => KanbanLane::Blocked,
            Self::Succeeded | Self::Cancelled => KanbanLane::Done,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CheckState {
    #[default]
    Pending,
    Running,
    Passed,
    Failed,
    Skipped,
}

impl CheckState {
    pub const fn marker(self) -> &'static str {
        match self {
            Self::Pending => "○",
            Self::Running => "…",
            Self::Passed => "✓",
            Self::Failed => "×",
            Self::Skipped => "−",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckSummary {
    pub name: String,
    pub state: CheckState,
    pub detail: String,
    pub visibility: ContentVisibility,
}

impl CheckSummary {
    pub fn new(name: impl Into<String>, state: CheckState, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            state,
            detail: detail.into(),
            visibility: ContentVisibility::Public,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskSummary {
    pub id: String,
    pub title: String,
    pub state: TaskState,
    pub worker: String,
    pub reviewer: String,
    pub checks: Vec<CheckSummary>,
    pub changed_files: Vec<String>,
    pub commit: Option<String>,
    pub merge_commit: Option<String>,
    pub blocker: Option<String>,
    pub version: u64,
    pub selected: bool,
    pub review_actions: Vec<ApprovalAction>,
    pub review_reason_required: bool,
}

impl TaskSummary {
    pub fn new(id: impl Into<String>, title: impl Into<String>, state: TaskState) -> Self {
        Self {
            id: id.into(),
            title: title.into(),
            state,
            worker: String::new(),
            reviewer: String::new(),
            checks: Vec::new(),
            changed_files: Vec::new(),
            commit: None,
            merge_commit: None,
            blocker: None,
            version: 0,
            selected: false,
            review_actions: Vec::new(),
            review_reason_required: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AttentionSeverity {
    #[default]
    Info,
    Warning,
    Blocking,
}

impl AttentionSeverity {
    pub const fn marker(self) -> &'static str {
        match self {
            Self::Info => "i",
            Self::Warning => "!",
            Self::Blocking => "!!",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttentionItem {
    pub id: String,
    pub title: String,
    pub detail: String,
    pub severity: AttentionSeverity,
    pub action: Option<String>,
    pub visibility: ContentVisibility,
}

impl AttentionItem {
    pub fn new(
        id: impl Into<String>,
        title: impl Into<String>,
        detail: impl Into<String>,
        severity: AttentionSeverity,
    ) -> Self {
        Self {
            id: id.into(),
            title: title.into(),
            detail: detail.into(),
            severity,
            action: None,
            visibility: ContentVisibility::Public,
        }
    }
}

/// Actions are supplied by the domain service.  The UI may only emit an
/// action present in a card's `permitted_actions` list.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApprovalAction {
    Approve,
    Reject,
    Accept,
    RequestChanges,
    Retry,
    Cancel,
}

impl ApprovalAction {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Approve => "Approve",
            Self::Reject => "Reject",
            Self::Accept => "Accept",
            Self::RequestChanges => "Request changes",
            Self::Retry => "Retry",
            Self::Cancel => "Cancel",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApprovalKind {
    CharterAdoption,
    TaskReview,
    Repository,
    Generic,
}

impl ApprovalKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::CharterAdoption => "PROJECT ADOPTION",
            Self::TaskReview => "TASK REVIEW",
            Self::Repository => "REPOSITORY ACTION",
            Self::Generic => "APPROVAL",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApprovalCard {
    pub id: String,
    pub kind: ApprovalKind,
    pub title: String,
    pub target: String,
    pub impact: String,
    pub expected_version: Option<u64>,
    pub expected_digest: Option<String>,
    pub details: Vec<String>,
    pub permitted_actions: Vec<ApprovalAction>,
    pub selected_action: usize,
    pub visibility: ContentVisibility,
}

impl ApprovalCard {
    pub fn selected_action(&self) -> Option<ApprovalAction> {
        self.permitted_actions.get(self.selected_action).copied()
    }

    pub fn move_action(&mut self, delta: isize) {
        if self.permitted_actions.is_empty() {
            self.selected_action = 0;
            return;
        }
        let len = self.permitted_actions.len();
        self.selected_action =
            (self.selected_action as isize + delta).rem_euclid(len as isize) as usize;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuestionOption {
    pub id: String,
    pub label: String,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuestionCard {
    pub id: String,
    pub title: String,
    pub prompt: String,
    pub options: Vec<QuestionOption>,
    pub selected_option: usize,
    pub allow_freeform: bool,
    pub visibility: ContentVisibility,
}

impl QuestionCard {
    pub fn selected_option(&self) -> Option<&QuestionOption> {
        self.options.get(self.selected_option)
    }

    pub fn move_option(&mut self, delta: isize) {
        if self.options.is_empty() {
            self.selected_option = 0;
            return;
        }
        let len = self.options.len();
        self.selected_option =
            (self.selected_option as isize + delta).rem_euclid(len as isize) as usize;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReviewCard {
    pub id: String,
    pub task_id: String,
    pub title: String,
    pub status: TaskState,
    pub checks: Vec<CheckSummary>,
    pub changed_files: Vec<String>,
    pub commit: Option<String>,
    pub merge_commit: Option<String>,
    pub worker: String,
    pub reviewer: String,
    pub expected_version: u64,
    pub permitted_actions: Vec<ApprovalAction>,
    pub selected_action: usize,
    pub guidance: String,
    pub editing_guidance: bool,
    pub requires_reason: bool,
    pub visibility: ContentVisibility,
}

impl ReviewCard {
    pub(crate) fn from_task_summary(task: &TaskSummary) -> Self {
        Self {
            id: task.id.clone(),
            task_id: task.id.clone(),
            title: task.title.clone(),
            status: task.state,
            checks: task.checks.clone(),
            changed_files: task.changed_files.clone(),
            commit: task.commit.clone(),
            merge_commit: task.merge_commit.clone(),
            worker: task.worker.clone(),
            reviewer: task.reviewer.clone(),
            expected_version: task.version,
            permitted_actions: task.review_actions.clone(),
            selected_action: 0,
            guidance: String::new(),
            editing_guidance: false,
            requires_reason: task.review_reason_required,
            visibility: ContentVisibility::Public,
        }
    }

    pub fn selected_action(&self) -> Option<ApprovalAction> {
        self.permitted_actions.get(self.selected_action).copied()
    }

    pub fn move_action(&mut self, delta: isize) {
        if self.permitted_actions.is_empty() {
            self.selected_action = 0;
            return;
        }
        let len = self.permitted_actions.len();
        self.selected_action =
            (self.selected_action as isize + delta).rem_euclid(len as isize) as usize;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelCard {
    pub turn_id: String,
    pub attempt: u32,
    pub expected_version: u64,
    pub summary: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModalState {
    Question(QuestionCard),
    Approval(ApprovalCard),
    Review(ReviewCard),
    Task(TaskSummary),
    Cancel(CancelCard),
    Help,
    Error(FailureNotice),
}

impl ModalState {
    pub const fn title(&self) -> &'static str {
        match self {
            Self::Question(_) => "QUESTION",
            Self::Approval(_) => "CONFIRM ACTION",
            Self::Review(_) => "REVIEW TASK",
            Self::Task(_) => "TASK DETAILS",
            Self::Cancel(_) => "CANCEL TURN",
            Self::Help => "KEYBOARD HELP",
            Self::Error(_) => "ACTION FAILED",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailureNotice {
    pub message: String,
    pub retryable: bool,
    pub conflict: bool,
    pub visibility: ContentVisibility,
}

impl FailureNotice {
    pub fn public(message: impl Into<String>, retryable: bool) -> Self {
        Self {
            message: message.into(),
            retryable,
            conflict: false,
            visibility: ContentVisibility::Public,
        }
    }

    pub fn protected(retryable: bool) -> Self {
        Self {
            message: String::new(),
            retryable,
            conflict: false,
            visibility: ContentVisibility::Protected,
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
            conflict: true,
            visibility: ContentVisibility::Public,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ComposerState {
    pub text: String,
    /// UTF-8 byte offset.  The reducer only moves this cursor across char
    /// boundaries, so editing remains Unicode-safe without a UI dependency.
    pub cursor: usize,
    pub submitting: bool,
    pub error: Option<FailureNotice>,
}

impl ComposerState {
    pub fn move_left(&mut self) {
        self.cursor = previous_boundary(&self.text, self.cursor);
    }

    pub fn move_right(&mut self) {
        self.cursor = next_boundary(&self.text, self.cursor);
    }

    pub fn move_home(&mut self) {
        self.cursor = 0;
    }

    pub fn move_end(&mut self) {
        self.cursor = self.text.len();
    }

    pub fn insert(&mut self, character: char, max_chars: usize) -> bool {
        if self.text.chars().count() >= max_chars {
            return false;
        }
        self.text.insert(self.cursor, character);
        self.cursor += character.len_utf8();
        self.error = None;
        true
    }

    pub fn newline(&mut self, max_chars: usize) -> bool {
        self.insert('\n', max_chars)
    }

    pub fn backspace(&mut self) -> bool {
        if self.cursor == 0 {
            return false;
        }
        let start = previous_boundary(&self.text, self.cursor);
        self.text.drain(start..self.cursor);
        self.cursor = start;
        self.error = None;
        true
    }

    pub fn delete(&mut self) -> bool {
        if self.cursor >= self.text.len() {
            return false;
        }
        let end = next_boundary(&self.text, self.cursor);
        self.text.drain(self.cursor..end);
        self.error = None;
        true
    }

    pub fn clear(&mut self) {
        self.text.clear();
        self.cursor = 0;
        self.submitting = false;
        self.error = None;
    }

    pub fn is_blank(&self) -> bool {
        self.text.trim().is_empty()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ProjectTab {
    #[default]
    Tasks,
    Attention,
    Approvals,
}

impl ProjectTab {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Tasks => "TASKS",
            Self::Attention => "ATTENTION",
            Self::Approvals => "APPROVALS",
        }
    }

    fn next(self) -> Self {
        match self {
            Self::Tasks => Self::Attention,
            Self::Attention => Self::Approvals,
            Self::Approvals => Self::Tasks,
        }
    }

    fn previous(self) -> Self {
        match self {
            Self::Tasks => Self::Approvals,
            Self::Attention => Self::Tasks,
            Self::Approvals => Self::Attention,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RailState {
    pub tab: ProjectTab,
    pub lane: KanbanLane,
    pub selected_task: usize,
    /// Selected approval row when the approvals tab owns focus.
    pub selected_approval: usize,
}

impl Default for RailState {
    fn default() -> Self {
        Self {
            tab: ProjectTab::Tasks,
            lane: KanbanLane::Queued,
            selected_task: 0,
            selected_approval: 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScrollState {
    pub timeline_offset: usize,
    pub activity_offset: usize,
    pub follow_tail: bool,
}

impl Default for ScrollState {
    fn default() -> Self {
        Self {
            timeline_offset: 0,
            activity_offset: 0,
            follow_tail: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Notification {
    pub message: String,
    pub visibility: ContentVisibility,
}

impl Notification {
    pub fn public(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            visibility: ContentVisibility::Public,
        }
    }

    pub fn protected() -> Self {
        Self {
            message: String::new(),
            visibility: ContentVisibility::Protected,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnReference {
    pub turn_id: String,
    pub attempt: u32,
    pub expected_version: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProjectionSnapshot {
    pub header: HeaderState,
    pub setup: SetupState,
    pub timeline: Vec<ChatMessage>,
    pub live_activity: Option<LiveActivity>,
    pub retryable_turn: Option<TurnReference>,
    pub tasks: Vec<TaskSummary>,
    /// Review cards derived from the same authoritative task snapshot as
    /// `tasks`. Keeping the full card here lets the reducer open a review
    /// modal without reconstructing or guessing commit/check evidence.
    pub review_cards: Vec<ReviewCard>,
    pub attention: Vec<AttentionItem>,
    pub approvals: Vec<ApprovalCard>,
    pub notifications: Vec<Notification>,
    pub modal: Option<ModalState>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppState {
    pub header: HeaderState,
    pub setup: SetupState,
    pub timeline: Vec<ChatMessage>,
    pub live_activity: Option<LiveActivity>,
    pub retryable_turn: Option<TurnReference>,
    pub tasks: Vec<TaskSummary>,
    pub review_cards: Vec<ReviewCard>,
    pub attention: Vec<AttentionItem>,
    pub approvals: Vec<ApprovalCard>,
    pub notifications: Vec<Notification>,
    pub composer: ComposerState,
    pub modal: Option<ModalState>,
    pub primary_view: PrimaryView,
    pub focus: FocusTarget,
    pub layout: LayoutMode,
    pub terminal_width: u16,
    pub terminal_height: u16,
    pub rail: RailState,
    pub scroll: ScrollState,
    /// Optimistic version associated with the live turn.  Backends can update
    /// this when a durable turn projection advances; cancellation then carries
    /// the exact version instead of guessing from chat text.
    pub active_turn_version: u64,
    pub shutdown_requested: bool,
    pub force_shutdown: bool,
    pub limits: AppLimits,
    pub pending_commands: BTreeMap<u64, PendingCommand>,
    next_command_id: u64,
    pub idempotency_prefix: String,
    /// The last send command is retained after a retryable failure so a
    /// second submit of the unchanged draft replays the same durable
    /// admission instead of creating a duplicate message.
    retryable_send: Option<Command>,
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

/// Input actions intentionally do not carry crossterm types.  `keymap` maps
/// physical key events to this enum, while controller tests can construct it
/// directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppInput {
    Insert(char),
    NewLine,
    Backspace,
    Delete,
    MoveLeft,
    MoveRight,
    Home,
    End,
    Submit,
    Cancel,
    RequestQuit,
    ForceQuit,
    RequestCancel,
    Retry,
    ToggleHelp,
    ToggleActivity,
    ToggleReasoning,
    FocusNext,
    FocusPrevious,
    Up,
    Down,
    PageUp,
    PageDown,
    TimelineTop,
    TimelineBottom,
    NextPrimaryView,
    PreviousPrimaryView,
    SelectPrimaryView(PrimaryView),
    NextProjectTab,
    PreviousProjectTab,
    SelectProjectTab(ProjectTab),
    SelectNext,
    SelectPrevious,
    SelectSetupNext,
    SelectSetupPrevious,
    Confirm,
    OpenSelected,
    Reject,
    Approve,
    Accept,
    RequestChanges,
}

/// State transitions coming from the controller/backend.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppAction {
    Input(AppInput),
    Resize {
        width: u16,
        height: u16,
    },
    ReplaceSnapshot(ProjectionSnapshot),
    SetRuntime(RuntimeState),
    SetReadiness(ProjectReadiness),
    SetSetup(SetupState),
    AddMessage(ChatMessage),
    TurnStarted {
        turn_id: String,
        attempt: u32,
        summary: String,
    },
    SetTurnVersion {
        turn_id: String,
        expected_version: u64,
    },
    Activity(ActivityItem),
    SetLiveActivity(Option<LiveActivity>),
    TurnStateChanged {
        turn_id: String,
        state: TurnState,
    },
    TurnFinished {
        turn_id: String,
        state: TurnState,
        assistant_message: Option<ChatMessage>,
        retryable: bool,
        expected_version: u64,
    },
    OpenModal(ModalState),
    CloseModal,
    Notice(Notification),
    CommandFinished(CommandResult),
    ShutdownComplete,
    Tick,
}

/// Commands are opaque to the view.  The backend maps each request to the
/// corresponding typed SoloSessionService operation and uses the supplied
/// idempotency key when crossing the service boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Command {
    pub id: u64,
    pub idempotency_key: String,
    pub request: CommandRequest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandRequest {
    SendMessage {
        text: String,
    },
    AnswerQuestion {
        question_id: String,
        option_id: String,
    },
    Approval {
        approval_id: String,
        action: ApprovalAction,
        expected_version: Option<u64>,
        expected_digest: Option<String>,
    },
    Review {
        review_id: String,
        task_id: String,
        guidance: Option<String>,
        action: ApprovalAction,
        expected_version: u64,
    },
    CancelTurn {
        turn_id: String,
        expected_version: u64,
    },
    RetryTurn {
        turn_id: String,
        expected_version: u64,
    },
    SelectAgent {
        agent_id: String,
    },
    Refresh,
    Shutdown,
    ForceShutdown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingCommand {
    pub command_id: u64,
    pub request: CommandRequest,
}

impl PendingCommand {
    fn new(command_id: u64, request: CommandRequest) -> Self {
        Self {
            command_id,
            request,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandResult {
    pub command_id: u64,
    pub outcome: CommandOutcome,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandOutcome {
    Succeeded { server_id: Option<String> },
    Failed { error: FailureNotice },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Reduction {
    pub commands: Vec<Command>,
}

impl Reduction {
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }
}

fn retain_latest<T>(items: &mut Vec<T>, max: usize) {
    if items.len() > max {
        let remove = items.len() - max;
        items.drain(..remove);
    }
}

fn previous_boundary(text: &str, cursor: usize) -> usize {
    text[..cursor]
        .char_indices()
        .next_back()
        .map_or(0, |(index, _)| index)
}

fn next_boundary(text: &str, cursor: usize) -> usize {
    text[cursor..]
        .chars()
        .next()
        .map_or(text.len(), |character| cursor + character.len_utf8())
}

impl fmt::Display for ProjectTab {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.label())
    }
}

mod reducer;

#[cfg(test)]
mod tests;
