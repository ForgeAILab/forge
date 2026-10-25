//! Typed backend snapshots and approval offers mapped into Solo app presentation state.

use super::*;

/// Convert one authoritative backend snapshot into the app projection.  The
/// app layer deliberately has no backend types, so each enum is mapped
/// exhaustively here.
pub fn to_projection_snapshot(snapshot: &SoloSnapshot) -> ProjectionSnapshot {
    let approvals = snapshot
        .approvals
        .iter()
        .map(to_approval_card)
        .collect::<Vec<_>>();
    let latest_turn = snapshot.chat.active_turns.last();
    let live_activity = latest_turn
        .and_then(|turn| projected_activity_for_turn(snapshot, turn))
        .or_else(|| {
            // Older/in-memory test backends may provide live activity without
            // a turn summary. Preserve that boundary behavior when there is
            // no authoritative turn list to fence it.
            if latest_turn.is_none() {
                snapshot.live_activity.first().map(to_live_activity)
            } else {
                None
            }
        });
    ProjectionSnapshot {
        header: to_header(&snapshot.repository, &snapshot.project),
        setup: to_setup_state(snapshot),
        timeline: snapshot.chat.messages.iter().map(to_chat_message).collect(),
        live_activity,
        retryable_turn: latest_retryable_terminal_turn(&snapshot.chat.active_turns)
            .map(to_turn_reference),
        tasks: snapshot.tasks.iter().map(to_task_summary).collect(),
        review_cards: snapshot
            .tasks
            .iter()
            .filter(|task| task.state == BackendTaskState::Review)
            .map(to_review_card)
            .collect(),
        attention: snapshot.attention.iter().map(to_attention_item).collect(),
        approvals,
        notifications: Vec::new(),
        // Runtime interactions are the only modal opened automatically: an
        // awaiting-input turn must be answerable, while approval/review cards
        // remain visible in the rail and never consume chat prose.
        modal: snapshot.chat.interactions.first().map(to_question_modal),
    }
}

/// Select activity only for the newest projected turn. In particular, an old
/// failed turn must not remain visible or retryable after a newer turn is
/// running or has succeeded.
fn projected_activity_for_turn(
    snapshot: &SoloSnapshot,
    turn: &TurnSnapshot,
) -> Option<LiveActivity> {
    if !turn_is_activity_visible(turn.state) {
        return None;
    }
    let activity = snapshot
        .live_activity
        .iter()
        .find(|activity| activity.target.turn_id.as_deref() == Some(turn.id.as_str()));
    activity
        .map(to_live_activity)
        .or_else(|| turn_is_activity_visible(turn.state).then(|| to_turn_activity(turn)))
}

fn latest_retryable_terminal_turn(turns: &[TurnSnapshot]) -> Option<&TurnSnapshot> {
    turns.last().filter(|turn| {
        matches!(
            turn.state,
            BackendTurnState::Failed | BackendTurnState::Cancelled
        ) && turn.retryable
    })
}

fn turn_is_activity_visible(state: BackendTurnState) -> bool {
    matches!(
        state,
        BackendTurnState::Queued
            | BackendTurnState::Leased
            | BackendTurnState::AwaitingInput
            | BackendTurnState::RetryWait
            | BackendTurnState::Failed
    )
}

fn to_turn_activity(turn: &TurnSnapshot) -> LiveActivity {
    LiveActivity {
        turn_id: turn.id.clone(),
        attempt: turn.attempt.max(1),
        state: to_app_turn_state(turn.state),
        summary: turn_summary(turn),
        worker: String::new(),
        items: Vec::new(),
        expanded: false,
        reasoning_expanded: false,
    }
}

pub fn to_header(repository: &RepositorySnapshot, project: &ProjectSnapshot) -> HeaderState {
    HeaderState {
        repository: repository.name.clone(),
        project: project.name.clone(),
        agent: project
            .selected_agent
            .as_ref()
            .map_or_else(String::new, agent_label),
        readiness: to_app_readiness(project.readiness),
        runtime: to_app_runtime(&project.runtime),
    }
}

pub fn to_app_readiness(readiness: BackendProjectReadiness) -> ProjectReadiness {
    match readiness {
        BackendProjectReadiness::Setup | BackendProjectReadiness::AwaitingAgent => {
            ProjectReadiness::Setup {
                step: SetupStep::ChooseAgent,
            }
        }
        // Charter drafting happens through the canonical Project Agent chat,
        // so this state must admit composer input while Task mutation remains
        // gated until exact adoption approval.
        BackendProjectReadiness::AwaitingCharter => ProjectReadiness::AwaitingAdoption,
        BackendProjectReadiness::AwaitingApproval => ProjectReadiness::AwaitingAdoption,
        BackendProjectReadiness::Operational => ProjectReadiness::Ready,
        BackendProjectReadiness::RecoveryRequired => ProjectReadiness::Recovering,
    }
}

pub fn to_app_runtime(runtime: &BackendRuntimeState) -> AppRuntimeState {
    match runtime {
        BackendRuntimeState::Starting => AppRuntimeState::Starting,
        BackendRuntimeState::Recovering { .. } => AppRuntimeState::Recovering,
        BackendRuntimeState::Ready => AppRuntimeState::Ready,
        BackendRuntimeState::Degraded { .. } => AppRuntimeState::Failed,
        BackendRuntimeState::ShuttingDown => AppRuntimeState::ShuttingDown,
        BackendRuntimeState::Stopped => AppRuntimeState::Stopped,
    }
}

pub fn to_app_turn_state(state: BackendTurnState) -> AppTurnState {
    match state {
        BackendTurnState::Queued => AppTurnState::Queued,
        BackendTurnState::Leased => AppTurnState::Running,
        BackendTurnState::AwaitingInput => AppTurnState::AwaitingInput,
        BackendTurnState::RetryWait => AppTurnState::RetryWait,
        BackendTurnState::Succeeded => AppTurnState::Succeeded,
        BackendTurnState::Failed => AppTurnState::Failed,
        BackendTurnState::Cancelled => AppTurnState::Cancelled,
    }
}

pub fn to_app_task_state(state: BackendTaskState) -> AppTaskState {
    match state {
        BackendTaskState::Todo => AppTaskState::Queued,
        BackendTaskState::InProgress => AppTaskState::Running,
        BackendTaskState::Blocked => AppTaskState::Blocked,
        BackendTaskState::Review => AppTaskState::AwaitingReview,
        BackendTaskState::Merging => AppTaskState::Merging,
        BackendTaskState::CleaningUp => AppTaskState::CleaningUp,
        BackendTaskState::Done => AppTaskState::Succeeded,
        BackendTaskState::Failed => AppTaskState::Failed,
        BackendTaskState::Cancelled => AppTaskState::Cancelled,
    }
}

pub fn to_app_check_state(state: CheckStatus) -> CheckState {
    match state {
        CheckStatus::Pending => CheckState::Pending,
        CheckStatus::Running => CheckState::Running,
        CheckStatus::Passed => CheckState::Passed,
        CheckStatus::Failed => CheckState::Failed,
        CheckStatus::Skipped => CheckState::Skipped,
    }
}

pub fn to_app_message_role(role: MessageRole) -> AppMessageRole {
    match role {
        MessageRole::User => AppMessageRole::User,
        MessageRole::Assistant => AppMessageRole::Assistant,
        MessageRole::System => AppMessageRole::System,
    }
}

pub fn to_app_activity_kind(kind: ActivityKind) -> AppActivityKind {
    match kind {
        ActivityKind::ToolCall
        | ActivityKind::ToolResult
        | ActivityKind::FileChange
        | ActivityKind::ShellCommand => AppActivityKind::Tool,
        ActivityKind::AssistantDelta => AppActivityKind::Reasoning,
        ActivityKind::Assistant => AppActivityKind::Status,
        ActivityKind::ExecutionState => AppActivityKind::Execution,
        ActivityKind::User | ActivityKind::System | ActivityKind::Other => AppActivityKind::Status,
    }
}

pub fn to_chat_message(message: &MessageSnapshot) -> ChatMessage {
    ChatMessage {
        id: message.id.clone(),
        role: to_app_message_role(message.role),
        content: message.content.clone(),
        timestamp: message.created_at.clone(),
        attempt: None,
        visibility: ContentVisibility::Public,
    }
}

pub fn to_live_activity(activity: &LiveActivitySnapshot) -> LiveActivity {
    LiveActivity {
        turn_id: activity
            .target
            .turn_id
            .clone()
            .unwrap_or_else(|| activity.target.execution_id.clone()),
        attempt: activity.target.attempt,
        state: to_app_turn_state(activity.state),
        summary: activity.summary.clone(),
        worker: activity
            .worker
            .as_ref()
            .map_or_else(String::new, agent_label),
        items: activity.entries.iter().map(to_activity_item).collect(),
        expanded: false,
        reasoning_expanded: false,
    }
}

pub fn to_activity_item(entry: &ActivityEntry) -> ActivityItem {
    let (status, detail) = match entry.kind {
        ActivityKind::AssistantDelta => (
            ActivityStatus::Running,
            entry
                .preview
                .clone()
                .unwrap_or_else(|| entry.summary.clone()),
        ),
        ActivityKind::ToolResult => (
            ActivityStatus::Complete,
            entry
                .preview
                .clone()
                .unwrap_or_else(|| entry.summary.clone()),
        ),
        ActivityKind::Other => (
            ActivityStatus::Complete,
            entry
                .preview
                .clone()
                .unwrap_or_else(|| entry.summary.clone()),
        ),
        ActivityKind::ToolCall
        | ActivityKind::Assistant
        | ActivityKind::ExecutionState
        | ActivityKind::FileChange
        | ActivityKind::ShellCommand
        | ActivityKind::User
        | ActivityKind::System => (
            ActivityStatus::Complete,
            entry
                .preview
                .clone()
                .unwrap_or_else(|| entry.summary.clone()),
        ),
    };
    ActivityItem::new(
        entry.sequence,
        to_app_activity_kind(entry.kind),
        status,
        entry.summary.clone(),
        detail,
    )
}

pub fn to_task_summary(task: &TaskSnapshot) -> TaskSummary {
    let mut result = TaskSummary::new(
        task.id.clone(),
        task.title.clone(),
        to_app_task_state(task.state),
    );
    result.version = nonnegative_u64(task.version);
    result.review_actions = task
        .review_actions
        .iter()
        .filter_map(|offer| match offer.action {
            api_types::TaskAction::Approve {
                override_checks: Some(false),
                ..
            } => Some(AppApprovalAction::Accept),
            api_types::TaskAction::SendBack { .. } => Some(AppApprovalAction::RequestChanges),
            _ => None,
        })
        .collect();
    result.review_reason_required = task.review_actions.iter().any(|offer| {
        matches!(
            offer.action,
            api_types::TaskAction::Approve {
                override_checks: Some(false),
                ..
            }
        ) && offer
            .parameters
            .iter()
            .any(|spec| spec.name == "reason" && spec.required)
    });
    result.worker = task.worker.as_ref().map_or_else(String::new, agent_label);
    result.reviewer = task.reviewer.as_ref().map_or_else(String::new, agent_label);
    result.checks = task.checks.iter().map(to_check_summary).collect();
    if let Some(commit) = &task.commit {
        result.commit = commit.commit.clone();
        result.merge_commit = commit.merged.then(|| commit.commit.clone()).flatten();
        result.changed_files = commit.changed_files.clone();
    }
    result.blocker = task
        .blocker
        .as_ref()
        .map(|failure| failure.headline.clone());
    result.selected = false;
    result
}

pub fn to_check_summary(check: &CheckSnapshot) -> CheckSummary {
    CheckSummary::new(
        check.name.clone(),
        to_app_check_state(check.status),
        check.summary.clone().unwrap_or_default(),
    )
}

pub fn to_attention_item(
    attention: &crate::backend::AttentionSnapshot,
) -> crate::app::AttentionItem {
    let severity = match attention.kind {
        crate::backend::AttentionKind::Approval
        | crate::backend::AttentionKind::RecoveryRequired
        | crate::backend::AttentionKind::Blocked => crate::app::AttentionSeverity::Blocking,
        crate::backend::AttentionKind::FailedCheck
        | crate::backend::AttentionKind::RetryExhausted
        | crate::backend::AttentionKind::AgentUnavailable => crate::app::AttentionSeverity::Warning,
    };
    let mut result = crate::app::AttentionItem::new(
        attention.id.clone(),
        attention.headline.clone(),
        attention.detail.clone(),
        severity,
    );
    result.action = attention.permitted_action.map(attention_action_label);
    result
}

pub fn to_approval_card(approval: &ApprovalSnapshot) -> ApprovalCard {
    ApprovalCard {
        id: approval.id.clone(),
        kind: to_app_approval_kind(approval.kind),
        title: approval.title.clone(),
        target: approval.revision.clone(),
        impact: approval.summary.clone(),
        expected_version: u64::try_from(approval.expected_version).ok(),
        expected_digest: Some(approval.digest.clone()),
        details: approval_details(approval),
        permitted_actions: approval
            .permitted_actions
            .iter()
            .map(to_app_approval_action)
            .collect(),
        selected_action: 0,
        visibility: ContentVisibility::Public,
    }
}

pub fn to_review_card(task: &TaskSnapshot) -> ReviewCard {
    let summary = to_task_summary(task);
    ReviewCard {
        id: task.id.clone(),
        task_id: task.id.clone(),
        title: task.title.clone(),
        status: summary.state,
        checks: summary.checks,
        changed_files: summary.changed_files,
        commit: summary.commit,
        merge_commit: summary.merge_commit,
        worker: summary.worker,
        reviewer: summary.reviewer,
        expected_version: summary.version,
        permitted_actions: summary.review_actions,
        guidance: String::new(),
        editing_guidance: false,
        requires_reason: summary.review_reason_required,
        selected_action: 0,
        visibility: ContentVisibility::Public,
    }
}

pub fn to_question_card(interaction: &InteractionSnapshot) -> crate::app::QuestionCard {
    let multiple_fields = interaction.fields.len() > 1;
    let mut options = Vec::new();
    for field in &interaction.fields {
        for choice in &field.choices {
            let id = if multiple_fields {
                format!("{}\0{choice}", field.id)
            } else {
                choice.clone()
            };
            let label = if multiple_fields {
                format!("{}: {choice}", field.label)
            } else {
                choice.clone()
            };
            options.push(crate::app::QuestionOption {
                id,
                label,
                detail: String::new(),
            });
        }
    }
    crate::app::QuestionCard {
        id: interaction.id.clone(),
        title: "Agent input".to_owned(),
        prompt: interaction.prompt.clone(),
        options,
        selected_option: 0,
        allow_freeform: interaction
            .fields
            .iter()
            .any(|field| field.choices.is_empty()),
        visibility: ContentVisibility::Public,
    }
}

fn to_question_modal(interaction: &InteractionSnapshot) -> ModalState {
    ModalState::Question(to_question_card(interaction))
}

pub fn to_setup_state(snapshot: &SoloSnapshot) -> SetupState {
    let approvals = snapshot
        .approvals
        .iter()
        .map(to_approval_card)
        .collect::<Vec<_>>();
    let mut setup = to_setup_state_from_project(&snapshot.project, &approvals);
    if let SetupState::AgentPicker {
        candidates,
        selected,
        detail,
    } = &mut setup
    {
        if !snapshot.setup_agents.is_empty() {
            *candidates = snapshot
                .setup_agents
                .iter()
                .map(to_setup_agent_candidate)
                .collect();
        }
        *selected = (*selected).min(candidates.len().saturating_sub(1));
        if candidates.is_empty() {
            *detail = "No authenticated local CLI harness is currently available.".to_owned();
        }
    }
    setup
}

pub(super) fn to_setup_state_from_project(
    project: &ProjectSnapshot,
    approvals: &[ApprovalCard],
) -> SetupState {
    match project.readiness {
        BackendProjectReadiness::Setup | BackendProjectReadiness::AwaitingAgent => {
            SetupState::AgentPicker {
                candidates: project
                    .selected_agent
                    .as_ref()
                    .map(|agent| to_agent_candidate(agent, true, true))
                    .into_iter()
                    .collect(),
                selected: 0,
                detail: "Choose an authenticated local CLI harness.".to_owned(),
            }
        }
        BackendProjectReadiness::AwaitingCharter => SetupState::Adoption {
            outcome: "Draft the Project Charter before adoption.".to_owned(),
            approval: None,
        },
        BackendProjectReadiness::AwaitingApproval => SetupState::Adoption {
            outcome: "The exact Project Charter adoption target is ready for approval.".to_owned(),
            approval: approvals.first().cloned(),
        },
        BackendProjectReadiness::Operational => SetupState::Ready,
        BackendProjectReadiness::RecoveryRequired => SetupState::Unavailable {
            detail: "Project recovery is required before new work can start.".to_owned(),
            retryable: true,
        },
    }
}

pub(super) fn to_agent_candidate(
    agent: &AgentSnapshot,
    available: bool,
    authenticated: bool,
) -> AgentCandidate {
    AgentCandidate {
        id: agent.id.clone(),
        label: agent.name.clone(),
        kind: agent.harness.clone(),
        available,
        authenticated,
        detail: agent.harness.clone(),
    }
}

pub(super) fn to_setup_agent_candidate(candidate: &SetupAgentSnapshot) -> AgentCandidate {
    AgentCandidate {
        id: candidate.id.clone(),
        label: candidate.name.clone(),
        kind: candidate.harness.clone(),
        available: candidate.available,
        authenticated: candidate.authenticated,
        detail: candidate.detail.clone(),
    }
}

pub(super) fn agent_label(agent: &AgentSnapshot) -> String {
    if agent.name.is_empty() {
        agent.id.clone()
    } else {
        agent.name.clone()
    }
}

fn to_turn_reference(turn: &TurnSnapshot) -> TurnReference {
    TurnReference {
        turn_id: turn.id.clone(),
        attempt: turn.attempt,
        expected_version: nonnegative_u64(turn.version),
    }
}

pub(super) fn turn_summary(turn: &TurnSnapshot) -> String {
    let failure_summary = matches!(
        turn.state,
        BackendTurnState::RetryWait | BackendTurnState::Failed
    )
    .then(|| {
        turn.failure
            .as_ref()
            .map(|failure| failure.headline.clone())
    })
    .flatten();
    failure_summary
        .or_else(|| turn.reply.clone())
        .unwrap_or_else(|| to_app_turn_state(turn.state).label().to_owned())
}

fn to_app_approval_kind(kind: ApprovalKind) -> AppApprovalKind {
    match kind {
        ApprovalKind::CharterAdoption => AppApprovalKind::CharterAdoption,
        ApprovalKind::AgentAction => AppApprovalKind::Repository,
        ApprovalKind::Recovery => AppApprovalKind::Generic,
    }
}

fn to_app_approval_action(action: &ApprovalAction) -> AppApprovalAction {
    match action {
        ApprovalAction::Approve => AppApprovalAction::Approve,
        ApprovalAction::Reject => AppApprovalAction::Reject,
    }
}

fn approval_details(approval: &ApprovalSnapshot) -> Vec<String> {
    let mut details = vec![format!("Revision: {}", approval.revision)];
    if !approval.digest.is_empty() {
        details.push(format!("Digest: {}", approval.digest));
    }
    if let Some(skill) = &approval.operating_skill_revision {
        details.push(format!("Operating skill: {skill}"));
    }
    if let Some(agent) = &approval.selected_agent {
        details.push(format!("Selected Agent: {}", agent_label(agent)));
    }
    details
}

fn attention_action_label(action: crate::backend::AttentionAction) -> String {
    match action {
        crate::backend::AttentionAction::Approve => "Approve".to_owned(),
        crate::backend::AttentionAction::Reject => "Reject".to_owned(),
        crate::backend::AttentionAction::Retry => "Retry".to_owned(),
        crate::backend::AttentionAction::Cancel => "Cancel".to_owned(),
        crate::backend::AttentionAction::Refresh => "Refresh".to_owned(),
        crate::backend::AttentionAction::Recover => "Recover".to_owned(),
        crate::backend::AttentionAction::Review => "Review".to_owned(),
    }
}
