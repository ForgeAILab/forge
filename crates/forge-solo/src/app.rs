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

    pub fn allows_mutating_tasks(&self) -> bool {
        matches!(self, Self::Ready)
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

    pub fn selected_index(&self) -> usize {
        match self {
            Self::AgentPicker { selected, .. } => *selected,
            _ => 0,
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
        }
    }

    pub fn checks_passed(&self) -> bool {
        !self.checks.is_empty()
            && self
                .checks
                .iter()
                .all(|check| check.state == CheckState::Passed)
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
            permitted_actions: vec![ApprovalAction::Accept, ApprovalAction::RequestChanges],
            selected_action: 0,
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

/// Alias used by callers that prefer the product-level name.
pub type SoloApp = AppState;

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

impl AppState {
    pub fn new() -> Self {
        Self {
            header: HeaderState::default(),
            setup: SetupState::default(),
            timeline: Vec::new(),
            live_activity: None,
            retryable_turn: None,
            tasks: Vec::new(),
            review_cards: Vec::new(),
            attention: Vec::new(),
            approvals: Vec::new(),
            notifications: Vec::new(),
            composer: ComposerState::default(),
            modal: None,
            primary_view: PrimaryView::Kanban,
            focus: FocusTarget::ProjectRail,
            layout: LayoutMode::Wide,
            terminal_width: 120,
            terminal_height: 40,
            rail: RailState::default(),
            scroll: ScrollState::default(),
            active_turn_version: 0,
            shutdown_requested: false,
            force_shutdown: false,
            limits: AppLimits::default(),
            pending_commands: BTreeMap::new(),
            next_command_id: 1,
            idempotency_prefix: format!("solo-ui-{}", Uuid::new_v4()),
            retryable_send: None,
        }
    }

    pub fn with_limits(mut self, limits: AppLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn with_idempotency_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.idempotency_prefix = prefix.into();
        self
    }

    pub fn composer_enabled(&self) -> bool {
        self.header.runtime.accepts_input()
            && self.header.readiness.allows_chat()
            && !self.composer.submitting
            && self.modal.is_none()
            && !self.shutdown_requested
    }

    pub fn live_turn_id(&self) -> Option<&str> {
        self.live_activity
            .as_ref()
            .filter(|activity| activity.state.is_live())
            .map(|activity| activity.turn_id.as_str())
    }

    pub fn selected_task(&self) -> Option<&TaskSummary> {
        self.tasks
            .get(self.rail.selected_task)
            .filter(|task| task.state.kanban_lane() == self.rail.lane)
    }

    pub fn reduce(&mut self, action: AppAction) -> Reduction {
        let mut commands = Vec::new();
        match action {
            AppAction::Input(input) => self.reduce_input(input, &mut commands),
            AppAction::Resize { width, height } => {
                self.set_size(width, height);
            }
            AppAction::ReplaceSnapshot(snapshot) => self.replace_snapshot(snapshot),
            AppAction::SetRuntime(runtime) => {
                self.header.runtime = runtime;
            }
            AppAction::SetReadiness(readiness) => {
                self.header.readiness = readiness;
            }
            AppAction::SetSetup(setup) => {
                self.setup = setup;
                normalize_setup(&mut self.setup);
                self.clamp_selections();
                if matches!(
                    self.setup,
                    SetupState::AgentPicker { .. } | SetupState::Unavailable { .. }
                ) && self.modal.is_none()
                {
                    self.focus = FocusTarget::SetupPicker;
                }
            }
            AppAction::AddMessage(message) => {
                self.upsert_message(message);
            }
            AppAction::TurnStarted {
                turn_id,
                attempt,
                summary,
            } => {
                self.header.runtime = RuntimeState::Busy;
                self.live_activity = Some(LiveActivity::new(turn_id, attempt, summary));
                self.active_turn_version = 0;
                self.scroll.activity_offset = 0;
            }
            AppAction::SetTurnVersion {
                turn_id,
                expected_version,
            } => {
                if self
                    .live_activity
                    .as_ref()
                    .is_some_and(|activity| activity.turn_id == turn_id)
                {
                    self.active_turn_version = expected_version;
                }
            }
            AppAction::Activity(item) => {
                if let Some(activity) = self.live_activity.as_mut() {
                    activity.append(item, self.limits.activity_items);
                }
            }
            AppAction::SetLiveActivity(activity) => {
                self.live_activity = activity.map(|mut activity| {
                    retain_latest(&mut activity.items, self.limits.activity_items);
                    activity
                });
            }
            AppAction::TurnStateChanged { turn_id, state } => {
                if let Some(activity) = self.live_activity.as_mut() {
                    if activity.turn_id == turn_id {
                        activity.state = state;
                    }
                }
                self.header.runtime = if state.is_live() {
                    RuntimeState::Busy
                } else {
                    RuntimeState::Ready
                };
            }
            AppAction::TurnFinished {
                turn_id,
                state,
                assistant_message,
                retryable,
                expected_version,
            } => {
                // A delayed terminal event for an older turn must not replace
                // the current turn's live/failure presentation or expose an
                // obsolete retry target. Authoritative snapshots still
                // reconcile the complete projection.
                if self
                    .live_activity
                    .as_ref()
                    .is_none_or(|activity| activity.turn_id == turn_id || !activity.state.is_live())
                {
                    let attempt = self
                        .live_activity
                        .as_ref()
                        .filter(|activity| activity.turn_id == turn_id)
                        .map_or(0, |activity| activity.attempt);
                    let matches_live = self
                        .live_activity
                        .as_ref()
                        .is_some_and(|activity| activity.turn_id == turn_id);
                    if matches_live {
                        self.live_activity = None;
                    }
                    if let Some(message) = assistant_message {
                        self.upsert_message(message);
                    }
                    self.retryable_turn = retryable.then_some(TurnReference {
                        turn_id,
                        attempt,
                        expected_version,
                    });
                    self.active_turn_version = 0;
                    self.header.runtime = RuntimeState::Ready;
                    if !state.is_retryable() {
                        self.retryable_turn = None;
                    }
                }
            }
            AppAction::OpenModal(modal) => {
                let mut modal = modal;
                normalize_modal(&mut modal, &self.limits);
                self.modal = Some(modal);
                self.focus = FocusTarget::Modal;
            }
            AppAction::CloseModal => self.close_modal(),
            AppAction::Notice(notification) => self.push_notification(notification),
            AppAction::CommandFinished(result) => {
                self.finish_command(result, &mut commands);
            }
            AppAction::ShutdownComplete => {
                self.header.runtime = if self.force_shutdown {
                    RuntimeState::ForcedShutdown
                } else {
                    RuntimeState::Stopped
                };
                self.shutdown_requested = true;
            }
            AppAction::Tick => {
                if self.scroll.follow_tail {
                    self.scroll.timeline_offset = 0;
                }
            }
        }
        Reduction { commands }
    }

    pub fn apply_snapshot(&mut self, snapshot: ProjectionSnapshot) {
        self.replace_snapshot(snapshot);
    }

    fn replace_snapshot(&mut self, snapshot: ProjectionSnapshot) {
        let old_live_turn = self.live_turn_id().map(str::to_owned);
        self.header = snapshot.header;
        self.setup = snapshot.setup;
        normalize_setup(&mut self.setup);
        if matches!(
            self.setup,
            SetupState::AgentPicker { .. } | SetupState::Unavailable { .. }
        ) && self.modal.is_none()
        {
            self.focus = FocusTarget::SetupPicker;
        }
        self.timeline = bounded_messages(snapshot.timeline, self.limits.timeline_messages);
        self.live_activity = snapshot.live_activity.map(|mut activity| {
            retain_latest(&mut activity.items, self.limits.activity_items);
            activity
        });
        self.retryable_turn = snapshot.retryable_turn;
        self.tasks = snapshot.tasks;
        for task in &mut self.tasks {
            retain_latest(&mut task.checks, self.limits.checks_per_task);
            retain_latest(&mut task.changed_files, 128);
        }
        retain_latest(&mut self.tasks, self.limits.tasks);
        self.review_cards = snapshot.review_cards;
        retain_latest(&mut self.review_cards, self.limits.tasks);
        self.attention = snapshot.attention;
        retain_latest(&mut self.attention, self.limits.attention_items);
        self.approvals = snapshot.approvals;
        retain_latest(&mut self.approvals, self.limits.attention_items);
        // Notifications are transient local feedback.  Backend projections
        // currently carry no notifications, so an ordinary refresh must not
        // erase a conflict/error before the user can read it.  If a future
        // authoritative projection supplies notifications, replace the local
        // list with that non-empty projection.
        if !snapshot.notifications.is_empty() {
            self.notifications = snapshot.notifications;
        }
        retain_latest(&mut self.notifications, self.limits.notifications);
        // Durable refreshes can arrive while a local confirmation/help card
        // is open.  Preserve those transient cards unless the authoritative
        // projection supplies a replacement interaction/approval/review.
        match snapshot.modal {
            Some(modal) => self.modal = Some(modal),
            None if matches!(
                self.modal,
                Some(
                    ModalState::Approval(_)
                        | ModalState::Review(_)
                        | ModalState::Task(_)
                        | ModalState::Cancel(_)
                        | ModalState::Help
                )
            ) => {}
            None => self.modal = None,
        }
        if let Some(modal) = self.modal.as_mut() {
            normalize_modal(modal, &self.limits);
        }
        if self.modal.is_some() {
            self.focus = FocusTarget::Modal;
        } else if matches!(self.focus, FocusTarget::Modal) {
            self.focus = self.primary_view.default_focus();
        }
        if old_live_turn != self.live_turn_id().map(str::to_owned) {
            self.scroll.activity_offset = 0;
        }
        self.clamp_selections();
    }

    fn reduce_input(&mut self, input: AppInput, commands: &mut Vec<Command>) {
        if self.force_shutdown {
            return;
        }

        if self.modal.is_some() {
            self.reduce_modal_input(input, commands);
            return;
        }

        match input {
            AppInput::Insert(character) if self.focus == FocusTarget::Composer => {
                self.composer.insert(character, self.limits.composer_chars);
            }
            AppInput::NewLine if self.focus == FocusTarget::Composer => {
                self.composer.newline(self.limits.composer_chars);
            }
            AppInput::Backspace if self.focus == FocusTarget::Composer => {
                self.composer.backspace();
            }
            AppInput::Delete if self.focus == FocusTarget::Composer => {
                self.composer.delete();
            }
            AppInput::MoveLeft if self.focus == FocusTarget::Composer => self.composer.move_left(),
            AppInput::MoveLeft
                if self.primary_view == PrimaryView::Kanban
                    && self.focus == FocusTarget::ProjectRail
                    && self.rail.tab == ProjectTab::Tasks =>
            {
                self.select_lane(-1);
            }
            AppInput::MoveRight if self.focus == FocusTarget::Composer => {
                self.composer.move_right();
            }
            AppInput::MoveRight
                if self.primary_view == PrimaryView::Kanban
                    && self.focus == FocusTarget::ProjectRail
                    && self.rail.tab == ProjectTab::Tasks =>
            {
                self.select_lane(1);
            }
            AppInput::Home if self.focus == FocusTarget::Composer => self.composer.move_home(),
            AppInput::End if self.focus == FocusTarget::Composer => self.composer.move_end(),
            AppInput::Submit => self.submit_message(commands),
            AppInput::Cancel => {
                self.close_modal();
            }
            AppInput::RequestQuit => self.request_quit(commands),
            AppInput::ForceQuit => {
                self.force_shutdown = true;
                self.shutdown_requested = true;
                self.header.runtime = RuntimeState::ForcedShutdown;
                self.push_notification(Notification::public(
                    "Forced quit requested; durable recovery will run on next launch.",
                ));
                self.queue_command(CommandRequest::ForceShutdown, commands);
            }
            AppInput::RequestCancel => self.open_cancel_for_live_turn(),
            AppInput::Retry => {
                if matches!(
                    self.setup,
                    SetupState::Unavailable {
                        retryable: true,
                        ..
                    }
                ) {
                    self.queue_command(CommandRequest::Refresh, commands);
                } else {
                    self.retry_turn(commands);
                }
            }
            AppInput::ToggleHelp => {
                self.modal = Some(ModalState::Help);
                self.focus = FocusTarget::Help;
            }
            AppInput::ToggleActivity => {
                if let Some(activity) = self.live_activity.as_mut() {
                    activity.expanded = !activity.expanded;
                    self.focus = FocusTarget::Activity;
                }
            }
            AppInput::ToggleReasoning => {
                if let Some(activity) = self.live_activity.as_mut() {
                    activity.reasoning_expanded = !activity.reasoning_expanded;
                }
            }
            AppInput::FocusNext => {
                self.focus = self.focus.next(self.primary_view);
            }
            AppInput::FocusPrevious => {
                self.focus = self.focus.previous(self.primary_view);
            }
            AppInput::Up => self.move_up(),
            AppInput::Down => self.move_down(),
            AppInput::PageUp => self.scroll_page(-1),
            AppInput::PageDown => self.scroll_page(1),
            AppInput::TimelineTop => {
                // The renderer clamps this sentinel against the number of
                // wrapped rows available at the current terminal width.
                self.scroll.timeline_offset = usize::MAX;
                self.scroll.follow_tail = false;
            }
            AppInput::TimelineBottom => {
                self.scroll.timeline_offset = 0;
                self.scroll.follow_tail = true;
            }
            AppInput::NextPrimaryView => self.select_primary_view(self.primary_view.next()),
            AppInput::PreviousPrimaryView => {
                self.select_primary_view(self.primary_view.previous());
            }
            AppInput::SelectPrimaryView(view) => self.select_primary_view(view),
            AppInput::NextProjectTab => self.select_project_tab(self.rail.tab.next()),
            AppInput::PreviousProjectTab => self.select_project_tab(self.rail.tab.previous()),
            AppInput::SelectProjectTab(tab) => self.select_project_tab(tab),
            AppInput::SelectNext => self.select_task(1),
            AppInput::SelectPrevious => self.select_task(-1),
            AppInput::SelectSetupNext => {
                self.setup.move_selection(1);
            }
            AppInput::SelectSetupPrevious => {
                self.setup.move_selection(-1);
            }
            AppInput::Confirm => self.confirm_setup(commands),
            AppInput::OpenSelected => self.open_selected_card(),
            AppInput::Reject => {}
            // Modal-only actions are ignored when no modal is open.  Keeping
            // them in the enum lets keymap/controller code stay explicit.
            AppInput::Approve | AppInput::Accept | AppInput::RequestChanges => {}
            _ => {}
        }
    }

    fn reduce_modal_input(&mut self, input: AppInput, commands: &mut Vec<Command>) {
        let Some(modal) = self.modal.take() else {
            return;
        };
        match modal {
            ModalState::Help => match input {
                AppInput::Cancel | AppInput::ToggleHelp | AppInput::Confirm => self.close_modal(),
                _ => {
                    self.modal = Some(ModalState::Help);
                    self.focus = FocusTarget::Help;
                }
            },
            ModalState::Cancel(card) => match input {
                AppInput::Cancel | AppInput::Reject => self.close_modal(),
                AppInput::Confirm | AppInput::Accept | AppInput::Approve => {
                    let request = CommandRequest::CancelTurn {
                        turn_id: card.turn_id.clone(),
                        expected_version: card.expected_version,
                    };
                    self.close_modal();
                    self.queue_command(request, commands);
                    if let Some(activity) = self.live_activity.as_mut() {
                        if activity.turn_id == card.turn_id {
                            activity.state = TurnState::Cancelling;
                        }
                    }
                }
                _ => {
                    self.modal = Some(ModalState::Cancel(card));
                    self.focus = FocusTarget::Modal;
                }
            },
            ModalState::Approval(mut card) => match input {
                AppInput::Cancel => self.close_modal(),
                AppInput::Up | AppInput::SelectPrevious => {
                    card.move_action(-1);
                    self.modal = Some(ModalState::Approval(card));
                    self.focus = FocusTarget::Modal;
                }
                AppInput::Down | AppInput::SelectNext => {
                    card.move_action(1);
                    self.modal = Some(ModalState::Approval(card));
                    self.focus = FocusTarget::Modal;
                }
                AppInput::Confirm | AppInput::Approve | AppInput::Accept => {
                    if let Some(action) = card.selected_action() {
                        let request = CommandRequest::Approval {
                            approval_id: card.id.clone(),
                            action,
                            expected_version: card.expected_version,
                            expected_digest: card.expected_digest.clone(),
                        };
                        self.close_modal();
                        self.queue_command(request, commands);
                    } else {
                        self.modal = Some(ModalState::Approval(card));
                        self.focus = FocusTarget::Modal;
                    }
                }
                AppInput::Reject | AppInput::RequestChanges
                    if card.permitted_actions.contains(&ApprovalAction::Reject)
                        || card
                            .permitted_actions
                            .contains(&ApprovalAction::RequestChanges) =>
                {
                    let action = if card.permitted_actions.contains(&ApprovalAction::Reject) {
                        ApprovalAction::Reject
                    } else {
                        ApprovalAction::RequestChanges
                    };
                    let request = CommandRequest::Approval {
                        approval_id: card.id.clone(),
                        action,
                        expected_version: card.expected_version,
                        expected_digest: card.expected_digest.clone(),
                    };
                    self.close_modal();
                    self.queue_command(request, commands);
                }
                _ => {
                    self.modal = Some(ModalState::Approval(card));
                    self.focus = FocusTarget::Modal;
                }
            },
            ModalState::Review(mut card) => match input {
                AppInput::Cancel => self.close_modal(),
                AppInput::Up | AppInput::SelectPrevious => {
                    card.move_action(-1);
                    self.modal = Some(ModalState::Review(card));
                    self.focus = FocusTarget::Modal;
                }
                AppInput::Down | AppInput::SelectNext => {
                    card.move_action(1);
                    self.modal = Some(ModalState::Review(card));
                    self.focus = FocusTarget::Modal;
                }
                AppInput::Confirm | AppInput::Approve | AppInput::Accept => {
                    if let Some(action) = card.selected_action() {
                        let request = CommandRequest::Review {
                            review_id: card.id.clone(),
                            task_id: card.task_id.clone(),
                            action,
                            expected_version: card.expected_version,
                        };
                        self.close_modal();
                        self.queue_command(request, commands);
                    } else {
                        self.modal = Some(ModalState::Review(card));
                        self.focus = FocusTarget::Modal;
                    }
                }
                AppInput::Reject | AppInput::RequestChanges
                    if card.permitted_actions.contains(&ApprovalAction::Reject)
                        || card
                            .permitted_actions
                            .contains(&ApprovalAction::RequestChanges) =>
                {
                    let action = if card.permitted_actions.contains(&ApprovalAction::Reject) {
                        ApprovalAction::Reject
                    } else {
                        ApprovalAction::RequestChanges
                    };
                    let request = CommandRequest::Review {
                        review_id: card.id.clone(),
                        task_id: card.task_id.clone(),
                        action,
                        expected_version: card.expected_version,
                    };
                    self.close_modal();
                    self.queue_command(request, commands);
                }
                _ => {
                    self.modal = Some(ModalState::Review(card));
                    self.focus = FocusTarget::Modal;
                }
            },
            ModalState::Question(mut card) => match input {
                AppInput::Cancel => self.close_modal(),
                AppInput::Up | AppInput::SelectPrevious => {
                    card.move_option(-1);
                    self.modal = Some(ModalState::Question(card));
                    self.focus = FocusTarget::Modal;
                }
                AppInput::Down | AppInput::SelectNext => {
                    card.move_option(1);
                    self.modal = Some(ModalState::Question(card));
                    self.focus = FocusTarget::Modal;
                }
                AppInput::Confirm => {
                    if let Some(option) = card.selected_option() {
                        let request = CommandRequest::AnswerQuestion {
                            question_id: card.id.clone(),
                            option_id: option.id.clone(),
                        };
                        self.close_modal();
                        self.queue_command(request, commands);
                    } else {
                        self.modal = Some(ModalState::Question(card));
                        self.focus = FocusTarget::Modal;
                    }
                }
                _ => {
                    self.modal = Some(ModalState::Question(card));
                    self.focus = FocusTarget::Modal;
                }
            },
            ModalState::Task(card) => match input {
                AppInput::Cancel | AppInput::Confirm => self.close_modal(),
                _ => {
                    self.modal = Some(ModalState::Task(card));
                    self.focus = FocusTarget::Modal;
                }
            },
            ModalState::Error(notice) => match input {
                AppInput::Cancel | AppInput::Confirm => self.close_modal(),
                AppInput::Retry if notice.retryable => {
                    self.close_modal();
                    self.retry_turn(commands);
                }
                _ => {
                    self.modal = Some(ModalState::Error(notice));
                    self.focus = FocusTarget::Modal;
                }
            },
        }
    }

    fn submit_message(&mut self, commands: &mut Vec<Command>) {
        if !self.composer_enabled() || self.composer.is_blank() || self.has_pending_send() {
            return;
        }
        let text = self.composer.text.clone();
        self.composer.submitting = true;
        self.composer.error = None;

        // A retry of a send is the same logical admission when the draft is
        // unchanged. Reuse the original command id and idempotency key so a
        // response that was committed before a transport failure is replayed
        // by the service instead of admitted a second time.
        if let Some(command) = self.retryable_send.clone() {
            let same_draft = matches!(
                &command.request,
                CommandRequest::SendMessage { text: command_text }
                    if command_text == &text
            );
            if same_draft && !self.pending_commands.contains_key(&command.id) {
                self.pending_commands.insert(
                    command.id,
                    PendingCommand::new(command.id, command.request.clone()),
                );
                commands.push(command);
                return;
            }
            if !same_draft {
                self.retryable_send = None;
            }
        }
        self.queue_command(CommandRequest::SendMessage { text }, commands);
    }

    fn request_quit(&mut self, commands: &mut Vec<Command>) {
        if self.shutdown_requested {
            return;
        }
        if let Some(activity) = self
            .live_activity
            .as_ref()
            .filter(|activity| activity.state.is_live())
        {
            self.modal = Some(ModalState::Cancel(CancelCard {
                turn_id: activity.turn_id.clone(),
                attempt: activity.attempt,
                expected_version: self.active_turn_version,
                summary: activity.summary.clone(),
            }));
            self.focus = FocusTarget::Modal;
            return;
        }
        self.shutdown_requested = true;
        self.header.runtime = RuntimeState::ShuttingDown;
        self.queue_command(CommandRequest::Shutdown, commands);
    }

    fn open_cancel_for_live_turn(&mut self) {
        let Some(activity) = self
            .live_activity
            .as_ref()
            .filter(|activity| activity.state.is_live())
        else {
            return;
        };
        self.modal = Some(ModalState::Cancel(CancelCard {
            turn_id: activity.turn_id.clone(),
            attempt: activity.attempt,
            expected_version: self.active_turn_version,
            summary: activity.summary.clone(),
        }));
        self.focus = FocusTarget::Modal;
    }

    fn retry_turn(&mut self, commands: &mut Vec<Command>) {
        let Some(turn) = self.retryable_turn.clone() else {
            return;
        };
        if !turn.turn_id.is_empty() && !self.has_pending_retry() {
            self.queue_command(
                CommandRequest::RetryTurn {
                    turn_id: turn.turn_id,
                    expected_version: turn.expected_version,
                },
                commands,
            );
        }
    }

    fn confirm_setup(&mut self, commands: &mut Vec<Command>) {
        let Some(candidate) = self.setup.selected_candidate() else {
            return;
        };
        if !candidate.eligible() {
            self.push_notification(Notification::public(
                "That Agent is unavailable or not authenticated; choose an eligible Agent.",
            ));
            return;
        }
        self.queue_command(
            CommandRequest::SelectAgent {
                agent_id: candidate.id.clone(),
            },
            commands,
        );
    }

    /// Open the card represented by the currently selected Project row.
    ///
    /// This action only changes local modal state. The existing modal reducer
    /// is the sole path that emits an approval/review mutation, so opening a
    /// card can never approve or accept it implicitly.
    fn open_selected_card(&mut self) {
        let modal = match self.rail.tab {
            ProjectTab::Approvals => self
                .approvals
                .get(self.rail.selected_approval)
                .cloned()
                .filter(|card| card.visibility.is_public())
                .map(ModalState::Approval),
            ProjectTab::Tasks => self.selected_task().cloned().map(|task| {
                if task.state == TaskState::AwaitingReview {
                    self.review_cards
                        .iter()
                        .find(|card| card.task_id == task.id)
                        .cloned()
                        .filter(|card| card.visibility.is_public())
                        .map_or_else(|| ModalState::Task(task), ModalState::Review)
                } else {
                    ModalState::Task(task)
                }
            }),
            ProjectTab::Attention => None,
        };
        if let Some(modal) = modal {
            self.modal = Some(modal);
            self.focus = FocusTarget::Modal;
        }
    }

    fn queue_command(&mut self, request: CommandRequest, commands: &mut Vec<Command>) {
        let id = self.next_command_id;
        self.next_command_id = self.next_command_id.saturating_add(1);
        let idempotency_key = format!("{}-{id}", self.idempotency_prefix);
        let command = Command {
            id,
            idempotency_key,
            request,
        };
        let pending = PendingCommand::new(command.id, command.request.clone());
        self.pending_commands.insert(id, pending);
        if matches!(&command.request, CommandRequest::SendMessage { .. }) {
            self.retryable_send = Some(command.clone());
        }
        commands.push(command);
    }

    fn finish_command(&mut self, result: CommandResult, commands: &mut Vec<Command>) {
        let Some(pending) = self.pending_commands.remove(&result.command_id) else {
            return;
        };
        match result.outcome {
            CommandOutcome::Succeeded { .. } => match pending.request {
                CommandRequest::SendMessage { text } => {
                    if self
                        .retryable_send
                        .as_ref()
                        .is_some_and(|command| command.id == pending.command_id)
                    {
                        self.retryable_send = None;
                    }
                    if self.composer.text == text {
                        self.composer.clear();
                    } else {
                        self.composer.submitting = false;
                    }
                    self.push_notification(Notification::public("Message sent."));
                }
                CommandRequest::CancelTurn { turn_id, .. } => {
                    if let Some(activity) = self.live_activity.as_mut() {
                        if activity.turn_id == turn_id {
                            activity.state = TurnState::Cancelled;
                        }
                    }
                    self.push_notification(Notification::public("Turn cancellation requested."));
                }
                CommandRequest::RetryTurn { turn_id, .. } => {
                    self.retryable_turn = None;
                    self.push_notification(Notification::public(format!(
                        "Retry requested for turn {turn_id}."
                    )));
                }
                CommandRequest::Approval { .. }
                | CommandRequest::Review { .. }
                | CommandRequest::AnswerQuestion { .. }
                | CommandRequest::SelectAgent { .. } => {
                    self.push_notification(Notification::public("Action accepted."));
                }
                CommandRequest::Refresh
                | CommandRequest::Shutdown
                | CommandRequest::ForceShutdown => {}
            },
            CommandOutcome::Failed { error } => {
                let retryable = error.retryable;
                if let CommandRequest::SendMessage { .. } = pending.request {
                    self.composer.submitting = false;
                    self.composer.error = Some(error.clone());
                    if !retryable
                        && self
                            .retryable_send
                            .as_ref()
                            .is_some_and(|command| command.id == pending.command_id)
                    {
                        self.retryable_send = None;
                    }
                }
                let mut failure = error;
                if failure.visibility == ContentVisibility::Protected {
                    // Keep the state useful without retaining an internal
                    // provider error in a notification rendered later.
                    failure.message.clear();
                }
                self.push_notification(if failure.visibility.is_public() {
                    Notification::public(if failure.message.is_empty() {
                        "Action failed; no details were provided.".to_owned()
                    } else {
                        failure.message.clone()
                    })
                } else {
                    Notification::protected()
                });
                if failure.conflict {
                    commands.push(self.make_refresh_command());
                } else if retryable {
                    self.retryable_turn = self.retryable_turn.take();
                }
            }
        }
    }

    fn make_refresh_command(&mut self) -> Command {
        let id = self.next_command_id;
        self.next_command_id = self.next_command_id.saturating_add(1);
        let request = CommandRequest::Refresh;
        let idempotency_key = format!("{}-{id}", self.idempotency_prefix);
        self.pending_commands
            .insert(id, PendingCommand::new(id, request.clone()));
        Command {
            id,
            idempotency_key,
            request,
        }
    }

    fn close_modal(&mut self) {
        self.modal = None;
        self.focus = if matches!(
            self.setup,
            SetupState::AgentPicker { .. } | SetupState::Unavailable { .. }
        ) {
            FocusTarget::SetupPicker
        } else {
            self.primary_view.default_focus()
        };
    }

    fn push_notification(&mut self, notification: Notification) {
        self.notifications.push(notification);
        retain_latest(&mut self.notifications, self.limits.notifications);
    }

    fn upsert_message(&mut self, message: ChatMessage) {
        if let Some(existing) = self.timeline.iter_mut().find(|item| item.id == message.id) {
            *existing = message;
        } else {
            self.timeline.push(message);
        }
        retain_latest(&mut self.timeline, self.limits.timeline_messages);
        if self.scroll.follow_tail {
            self.scroll.timeline_offset = 0;
        }
    }

    fn set_size(&mut self, width: u16, height: u16) {
        self.terminal_width = width;
        self.terminal_height = height;
        self.layout = LayoutMode::for_size(width, height);
        self.clamp_selections();
    }

    fn select_primary_view(&mut self, view: PrimaryView) {
        self.primary_view = view;
        self.focus = if matches!(
            self.setup,
            SetupState::AgentPicker { .. } | SetupState::Unavailable { .. }
        ) {
            FocusTarget::SetupPicker
        } else {
            view.default_focus()
        };
    }

    fn select_project_tab(&mut self, tab: ProjectTab) {
        self.primary_view = PrimaryView::Kanban;
        self.rail.tab = tab;
        self.focus = FocusTarget::ProjectRail;
    }

    fn clamp_selections(&mut self) {
        if self.tasks.is_empty() {
            self.rail.selected_task = 0;
            self.rail.lane = KanbanLane::Queued;
        } else {
            self.rail.selected_task = self.rail.selected_task.min(self.tasks.len() - 1);
            if !self
                .tasks
                .iter()
                .any(|task| task.state.kanban_lane() == self.rail.lane)
            {
                self.rail.lane = self.tasks[self.rail.selected_task].state.kanban_lane();
            }
            if self.tasks[self.rail.selected_task].state.kanban_lane() != self.rail.lane {
                self.rail.selected_task = self
                    .tasks
                    .iter()
                    .position(|task| task.state.kanban_lane() == self.rail.lane)
                    .unwrap_or(0);
            }
            for (index, task) in self.tasks.iter_mut().enumerate() {
                task.selected = index == self.rail.selected_task;
            }
        }
        if self.approvals.is_empty() {
            self.rail.selected_approval = 0;
        } else {
            self.rail.selected_approval = self.rail.selected_approval.min(self.approvals.len() - 1);
        }
        if let SetupState::AgentPicker {
            candidates,
            selected,
            ..
        } = &mut self.setup
        {
            *selected = if candidates.is_empty() {
                0
            } else {
                (*selected).min(candidates.len() - 1)
            };
        }
    }

    fn move_up(&mut self) {
        match self.focus {
            FocusTarget::Timeline => self.scroll_page(-1),
            FocusTarget::Activity => self.scroll_activity(-1),
            FocusTarget::ProjectRail => self.select_task(-1),
            FocusTarget::SetupPicker => self.setup.move_selection(-1),
            _ => {}
        }
    }

    fn move_down(&mut self) {
        match self.focus {
            FocusTarget::Timeline => self.scroll_page(1),
            FocusTarget::Activity => self.scroll_activity(1),
            FocusTarget::ProjectRail => self.select_task(1),
            FocusTarget::SetupPicker => self.setup.move_selection(1),
            _ => {}
        }
    }

    fn scroll_page(&mut self, direction: i8) {
        let amount = 8;
        if direction < 0 {
            self.scroll.timeline_offset = self.scroll.timeline_offset.saturating_add(amount);
            self.scroll.follow_tail = false;
        } else {
            self.scroll.timeline_offset = self.scroll.timeline_offset.saturating_sub(amount);
            if self.scroll.timeline_offset == 0 {
                self.scroll.follow_tail = true;
            }
        }
    }

    fn scroll_activity(&mut self, direction: i8) {
        if direction < 0 {
            self.scroll.activity_offset = self.scroll.activity_offset.saturating_add(4);
        } else {
            self.scroll.activity_offset = self.scroll.activity_offset.saturating_sub(4);
        }
    }

    fn select_task(&mut self, delta: isize) {
        if self.rail.tab == ProjectTab::Approvals {
            if self.approvals.is_empty() {
                self.rail.selected_approval = 0;
                return;
            }
            let len = self.approvals.len();
            self.rail.selected_approval =
                (self.rail.selected_approval as isize + delta).rem_euclid(len as isize) as usize;
            return;
        }
        if self.rail.tab != ProjectTab::Tasks {
            return;
        }

        let lane_tasks = self
            .tasks
            .iter()
            .enumerate()
            .filter_map(|(index, task)| {
                (task.state.kanban_lane() == self.rail.lane).then_some(index)
            })
            .collect::<Vec<_>>();
        if lane_tasks.is_empty() {
            return;
        }
        let current = lane_tasks
            .iter()
            .position(|index| *index == self.rail.selected_task)
            .unwrap_or(0);
        let next = (current as isize + delta).rem_euclid(lane_tasks.len() as isize) as usize;
        self.set_selected_task(lane_tasks[next]);
    }

    fn select_lane(&mut self, delta: isize) {
        let current_row = self
            .tasks
            .iter()
            .enumerate()
            .filter(|(_, task)| task.state.kanban_lane() == self.rail.lane)
            .position(|(index, _)| index == self.rail.selected_task)
            .unwrap_or(0);

        let mut lane = self.rail.lane;
        for _ in 0..KanbanLane::ALL.len() {
            lane = if delta < 0 {
                lane.previous()
            } else {
                lane.next()
            };
            let lane_tasks = self
                .tasks
                .iter()
                .enumerate()
                .filter_map(|(index, task)| (task.state.kanban_lane() == lane).then_some(index))
                .collect::<Vec<_>>();
            if !lane_tasks.is_empty() {
                let next = lane_tasks[current_row.min(lane_tasks.len() - 1)];
                self.rail.lane = lane;
                self.set_selected_task(next);
                return;
            }
        }

        self.rail.lane = if delta < 0 {
            self.rail.lane.previous()
        } else {
            self.rail.lane.next()
        };
        for task in &mut self.tasks {
            task.selected = false;
        }
    }

    fn set_selected_task(&mut self, selected: usize) {
        self.rail.selected_task = selected.min(self.tasks.len().saturating_sub(1));
        if let Some(task) = self.tasks.get(self.rail.selected_task) {
            self.rail.lane = task.state.kanban_lane();
        }
        for (index, task) in self.tasks.iter_mut().enumerate() {
            task.selected = index == self.rail.selected_task;
        }
    }

    fn has_pending_send(&self) -> bool {
        self.pending_commands
            .values()
            .any(|pending| matches!(pending.request, CommandRequest::SendMessage { .. }))
    }

    fn has_pending_retry(&self) -> bool {
        self.pending_commands
            .values()
            .any(|pending| matches!(pending.request, CommandRequest::RetryTurn { .. }))
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

fn bounded_messages(mut messages: Vec<ChatMessage>, max: usize) -> Vec<ChatMessage> {
    let mut result = Vec::with_capacity(messages.len().min(max));
    for message in messages.drain(..) {
        if let Some(existing) = result
            .iter_mut()
            .find(|item: &&mut ChatMessage| item.id == message.id)
        {
            *existing = message;
        } else {
            result.push(message);
        }
    }
    retain_latest(&mut result, max);
    result
}

fn normalize_setup(setup: &mut SetupState) {
    if let SetupState::AgentPicker {
        candidates,
        selected,
        ..
    } = setup
    {
        // Harness discovery is expected to return a small set.  Keep a hard
        // ceiling here as a last line of defence for a malformed adapter.
        candidates.truncate(32);
        if *selected >= candidates.len() {
            *selected = candidates.len().saturating_sub(1);
        }
    }
}

fn normalize_modal(modal: &mut ModalState, limits: &AppLimits) {
    match modal {
        ModalState::Question(card) => {
            card.options.truncate(32);
            card.selected_option = card
                .selected_option
                .min(card.options.len().saturating_sub(1));
        }
        ModalState::Approval(card) => {
            card.details.truncate(48);
            card.permitted_actions.truncate(8);
            card.selected_action = card
                .selected_action
                .min(card.permitted_actions.len().saturating_sub(1));
        }
        ModalState::Review(card) => {
            card.checks.truncate(limits.checks_per_task);
            card.changed_files.truncate(128);
            card.permitted_actions.truncate(8);
            card.selected_action = card
                .selected_action
                .min(card.permitted_actions.len().saturating_sub(1));
        }
        ModalState::Task(task) => {
            task.checks.truncate(limits.checks_per_task);
            task.changed_files.truncate(128);
        }
        ModalState::Cancel(_) | ModalState::Help | ModalState::Error(_) => {}
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

#[cfg(test)]
mod tests {
    use super::*;

    fn ready_state() -> AppState {
        let mut state = AppState::new();
        state.header.runtime = RuntimeState::Ready;
        state.header.readiness = ProjectReadiness::Ready;
        state.primary_view = PrimaryView::MainChat;
        state.focus = FocusTarget::Composer;
        state
    }

    #[test]
    fn solo_opens_on_kanban_and_switches_to_one_chat_view() {
        let mut state = AppState::new();
        assert_eq!(state.primary_view, PrimaryView::Kanban);
        assert_eq!(state.focus, FocusTarget::ProjectRail);

        state.reduce(AppAction::Input(AppInput::SelectPrimaryView(
            PrimaryView::MainChat,
        )));
        assert_eq!(state.primary_view, PrimaryView::MainChat);
        assert_eq!(state.focus, FocusTarget::Composer);

        state.reduce(AppAction::Input(AppInput::PreviousPrimaryView));
        assert_eq!(state.primary_view, PrimaryView::Kanban);
        assert_eq!(state.focus, FocusTarget::ProjectRail);
    }

    #[test]
    fn kanban_arrows_move_within_and_between_nonempty_lanes() {
        let mut state = AppState::new();
        state.tasks = vec![
            TaskSummary::new("q1", "Queued one", TaskState::Queued),
            TaskSummary::new("q2", "Queued two", TaskState::Queued),
            TaskSummary::new("a1", "Active one", TaskState::Running),
            TaskSummary::new("d1", "Done one", TaskState::Succeeded),
        ];
        state.clamp_selections();

        state.reduce(AppAction::Input(AppInput::Down));
        assert_eq!(
            state.selected_task().map(|task| task.id.as_str()),
            Some("q2")
        );

        state.reduce(AppAction::Input(AppInput::MoveRight));
        assert_eq!(state.rail.lane, KanbanLane::Active);
        assert_eq!(
            state.selected_task().map(|task| task.id.as_str()),
            Some("a1")
        );

        state.reduce(AppAction::Input(AppInput::MoveRight));
        assert_eq!(state.rail.lane, KanbanLane::Done);
        assert_eq!(
            state.selected_task().map(|task| task.id.as_str()),
            Some("d1")
        );
    }

    #[test]
    fn composer_moves_and_edits_unicode_without_splitting_bytes() {
        let mut composer = ComposerState::default();
        composer.insert('a', 20);
        composer.insert('é', 20);
        composer.insert('中', 20);
        composer.move_left();
        composer.backspace();
        assert_eq!(composer.text, "a中");
        assert_eq!(composer.cursor, "a".len());
        composer.move_home();
        composer.delete();
        assert_eq!(composer.text, "中");
    }

    #[test]
    fn timeline_and_activity_are_bounded() {
        let limits = AppLimits {
            timeline_messages: 2,
            activity_items: 2,
            ..AppLimits::default()
        };
        let mut state = AppState::new().with_limits(limits);
        for number in 0..4 {
            state.reduce(AppAction::AddMessage(ChatMessage::new(
                number.to_string(),
                MessageRole::User,
                number.to_string(),
                "now",
            )));
        }
        state.reduce(AppAction::TurnStarted {
            turn_id: "turn".to_owned(),
            attempt: 1,
            summary: "working".to_owned(),
        });
        for number in 0..4 {
            state.reduce(AppAction::Activity(ActivityItem::new(
                number,
                ActivityKind::Tool,
                ActivityStatus::Complete,
                "tool",
                number.to_string(),
            )));
        }
        assert_eq!(state.timeline.len(), 2);
        assert_eq!(state.timeline[0].id, "2");
        assert_eq!(
            state
                .live_activity
                .as_ref()
                .expect("live activity")
                .items
                .len(),
            2
        );
        assert_eq!(
            state.live_activity.as_ref().expect("live activity").items[0].sequence,
            2
        );
    }

    #[test]
    fn duplicate_submit_is_deduplicated_until_command_finishes() {
        let mut state = ready_state();
        state.reduce(AppAction::Input(AppInput::Insert('h')));
        state.reduce(AppAction::Input(AppInput::Insert('i')));
        let first = state.reduce(AppAction::Input(AppInput::Submit));
        let second = state.reduce(AppAction::Input(AppInput::Submit));
        assert_eq!(first.commands.len(), 1);
        assert!(second.is_empty());
        assert_eq!(state.pending_commands.len(), 1);
        assert!(state.composer.submitting);
    }

    #[test]
    fn independent_instances_namespace_commands_and_failed_send_replays_identity() {
        let mut first = ready_state();
        first.reduce(AppAction::Input(AppInput::Insert('h')));
        let first_command = first.reduce(AppAction::Input(AppInput::Submit)).commands[0].clone();

        let mut second = ready_state();
        second.reduce(AppAction::Input(AppInput::Insert('h')));
        let second_command = second.reduce(AppAction::Input(AppInput::Submit)).commands[0].clone();

        assert_ne!(
            first_command.idempotency_key, second_command.idempotency_key,
            "separate Solo launches must not reuse durable message keys"
        );

        first.reduce(AppAction::CommandFinished(CommandResult {
            command_id: first_command.id,
            outcome: CommandOutcome::Failed {
                error: FailureNotice::public("temporary transport failure", true),
            },
        }));
        let replay = first.reduce(AppAction::Input(AppInput::Submit));
        assert_eq!(replay.commands.len(), 1);
        assert_eq!(replay.commands[0].id, first_command.id);
        assert_eq!(
            replay.commands[0].idempotency_key, first_command.idempotency_key,
            "retrying the unchanged draft must replay the same command identity"
        );
    }

    #[test]
    fn failed_send_preserves_draft_and_conflict_requests_refresh() {
        let mut state = ready_state();
        state.reduce(AppAction::Input(AppInput::Insert('x')));
        let reduction = state.reduce(AppAction::Input(AppInput::Submit));
        let command_id = reduction.commands[0].id;
        let result = state.reduce(AppAction::CommandFinished(CommandResult {
            command_id,
            outcome: CommandOutcome::Failed {
                error: FailureNotice::conflict("state changed; refresh required"),
            },
        }));
        assert_eq!(state.composer.text, "x");
        assert!(!state.composer.submitting);
        assert_eq!(result.commands.len(), 1);
        assert!(matches!(
            result.commands[0].request,
            CommandRequest::Refresh
        ));
    }

    #[test]
    fn ordinary_cancel_or_quit_never_executes_an_approval() {
        let mut state = ready_state();
        state.reduce(AppAction::OpenModal(ModalState::Approval(ApprovalCard {
            id: "approval".to_owned(),
            kind: ApprovalKind::CharterAdoption,
            title: "Adopt".to_owned(),
            target: "project".to_owned(),
            impact: "enables task admission".to_owned(),
            expected_version: Some(4),
            expected_digest: Some("digest".to_owned()),
            details: vec!["exact target".to_owned()],
            permitted_actions: vec![ApprovalAction::Approve],
            selected_action: 0,
            visibility: ContentVisibility::Public,
        })));
        let cancel = state.reduce(AppAction::Input(AppInput::Cancel));
        assert!(cancel.is_empty());
        assert!(state.modal.is_none());
    }

    #[test]
    fn snapshot_replaces_transient_activity_and_dedupes_messages() {
        let mut state = ready_state();
        state.reduce(AppAction::TurnStarted {
            turn_id: "live".to_owned(),
            attempt: 1,
            summary: "draft".to_owned(),
        });
        state.apply_snapshot(ProjectionSnapshot {
            header: state.header.clone(),
            timeline: vec![
                ChatMessage::new("m", MessageRole::Assistant, "first", "t1"),
                ChatMessage::new("m", MessageRole::Assistant, "authoritative", "t2"),
            ],
            ..ProjectionSnapshot::default()
        });
        assert!(state.live_activity.is_none());
        assert_eq!(state.timeline.len(), 1);
        assert_eq!(state.timeline[0].content, "authoritative");
    }

    #[test]
    fn snapshot_refresh_preserves_local_error_notification() {
        let mut state = ready_state();
        state.reduce(AppAction::Notice(Notification::public(
            "turn version changed; refresh and retry",
        )));
        state.apply_snapshot(ProjectionSnapshot {
            header: state.header.clone(),
            ..ProjectionSnapshot::default()
        });
        assert_eq!(
            state
                .notifications
                .last()
                .map(|notice| notice.message.as_str()),
            Some("turn version changed; refresh and retry")
        );
    }

    #[test]
    fn opening_review_uses_authoritative_card_and_otherwise_shows_task_details() {
        let mut state = ready_state();
        let mut task = TaskSummary::new("task", "Review task", TaskState::AwaitingReview);
        task.selected = true;
        state.tasks.push(task);
        state.primary_view = PrimaryView::Kanban;
        state.rail.tab = ProjectTab::Tasks;
        state.rail.lane = KanbanLane::Review;
        state.focus = FocusTarget::ProjectRail;
        state.reduce(AppAction::Input(AppInput::OpenSelected));
        assert!(matches!(state.modal, Some(ModalState::Task(_))));
        state.reduce(AppAction::Input(AppInput::Cancel));

        let task = state.tasks[0].clone();
        state
            .review_cards
            .push(ReviewCard::from_task_summary(&task));
        state.reduce(AppAction::Input(AppInput::OpenSelected));
        assert!(matches!(state.modal, Some(ModalState::Review(card)) if card.task_id == "task"));
    }
}
