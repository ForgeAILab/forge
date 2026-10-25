//! Bridge between the keyboard/controller boundary and the pure Solo app.
//!
//! [`AppState`](crate::app::AppState) owns all transient UI state (including
//! the composer draft, focus, modal target, and pending local command IDs).
//! [`AppReducer`] is the small adapter that lets the asynchronous
//! [`SoloController`](crate::controller::SoloController) drive that state
//! without teaching either side about the other's implementation details.
//!
//! Backend events are treated as hints.  A snapshot or activity page is
//! applied only after the controller has performed its scope checks, and a
//! later authoritative refresh may replace any optimistic presentation.  The
//! bridge never infers approval, retryability, or task state from prose.

use std::collections::HashMap;

use crossterm::event::{
    KeyCode as TerminalKeyCode, KeyEvent as TerminalKeyEvent, KeyEventKind, KeyEventState,
    KeyModifiers as TerminalKeyModifiers,
};

use crate::{
    app::{
        ActivityItem, ActivityKind as AppActivityKind, ActivityStatus, AgentCandidate, AppAction,
        AppInput, AppState, ApprovalAction as AppApprovalAction, ApprovalCard,
        ApprovalKind as AppApprovalKind, ChatMessage, CheckState, CheckSummary, Command,
        CommandOutcome, CommandRequest, CommandResult, ContentVisibility, FailureNotice,
        HeaderState, LiveActivity, MessageRole as AppMessageRole, ModalState, Notification,
        ProjectReadiness, ProjectionSnapshot, ReviewCard, RuntimeState as AppRuntimeState,
        SetupState, SetupStep, TaskState as AppTaskState, TaskSummary, TurnReference,
        TurnState as AppTurnState,
    },
    backend::{
        ActivityBatch, ActivityCursor, ActivityEntry, ActivityKind, ActivityReadRequest,
        ActivityTarget, AgentSnapshot, ApprovalAction, ApprovalDecisionRequest, ApprovalKind,
        ApprovalSnapshot, BackendCommand, BackendCommandResult, BackendError, BackendErrorKind,
        BackendEvent, BackendEventKind, BackendResult, CheckSnapshot, CheckStatus,
        InteractionAnswer, InteractionAnswerRequest, InteractionSnapshot, InvalidationReason,
        LiveActivitySnapshot, MessageRole, MessageSnapshot,
        ProjectReadiness as BackendProjectReadiness, ProjectSnapshot, RepositorySnapshot,
        RuntimeState as BackendRuntimeState, SelectAgentRequest, SendMessageRequest,
        SetupAgentSnapshot, SoloScope, SoloSnapshot, TaskSnapshot, TaskState as BackendTaskState,
        TurnMutationRequest, TurnSnapshot, TurnState as BackendTurnState,
    },
    controller::{
        ControllerEffect, ControllerEvent, InputEvent, KeyCode as ControllerKeyCode,
        KeyEvent as ControllerKeyEvent, KeyModifiers as ControllerKeyModifiers, SoloReducer,
    },
    keymap::Keymap,
};

const DEFAULT_ACTIVITY_PAGE: usize = 128;

/// Identity used to retain a JSONL cursor for one turn or Task execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ActivityKey {
    project_id: String,
    turn_id: Option<String>,
    task_id: Option<String>,
    execution_id: String,
    attempt: u32,
}

impl From<&ActivityTarget> for ActivityKey {
    fn from(target: &ActivityTarget) -> Self {
        Self {
            project_id: target.project_id.clone(),
            turn_id: target.turn_id.clone(),
            task_id: target.task_id.clone(),
            execution_id: target.execution_id.clone(),
            attempt: target.attempt,
        }
    }
}

#[derive(Debug, Clone)]
struct ActivityState {
    target: ActivityTarget,
    cursor: ActivityCursor,
}

/// Pure-app reducer adapter consumed by [`SoloController`].
///
/// The bridge owns only adapter metadata: the bound scope learned from the
/// latest snapshot, authoritative interaction context needed to answer a
/// question, log cursors, and the mapping from backend idempotency keys back
/// to `AppState`'s local pending command IDs.  The actual UI state remains in
/// [`AppState`], so a snapshot refresh cannot erase a composer draft.
#[derive(Debug)]
pub struct AppReducer {
    state: AppState,
    keymap: Keymap,
    scope: Option<SoloScope>,
    interactions: HashMap<String, InteractionSnapshot>,
    activities: HashMap<ActivityKey, ActivityState>,
    pending_backend_commands: HashMap<String, u64>,
    setup_candidates: Vec<AgentCandidate>,
}

impl Default for AppReducer {
    fn default() -> Self {
        Self::new(AppState::new())
    }
}

impl AppReducer {
    /// Wrap an existing app state.  This is useful when bootstrap has already
    /// populated the setup picker before the first authoritative snapshot.
    pub fn new(state: AppState) -> Self {
        Self {
            state,
            keymap: Keymap::default(),
            scope: None,
            interactions: HashMap::new(),
            activities: HashMap::new(),
            pending_backend_commands: HashMap::new(),
            setup_candidates: Vec::new(),
        }
    }

    /// Seed or replace the structured setup candidates discovered by
    /// bootstrap.  The backend snapshot intentionally carries only the
    /// selected Agent, while discovery/authentication stays at bootstrap.
    pub fn with_setup_candidates(mut self, candidates: Vec<AgentCandidate>) -> Self {
        self.setup_candidates = candidates;
        self
    }

    pub fn state(&self) -> &AppState {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut AppState {
        &mut self.state
    }

    pub fn scope(&self) -> Option<&SoloScope> {
        self.scope.as_ref()
    }

    /// Convert one controller event into reducer effects.
    fn reduce_event(&mut self, event: ControllerEvent) -> Vec<ControllerEffect> {
        match event {
            ControllerEvent::Input(input) => self.reduce_input(input),
            ControllerEvent::Backend(event) => self.reduce_backend_event(event),
            ControllerEvent::BackendLagged { skipped } => {
                let mut effects =
                    self.apply_action(AppAction::Notice(Notification::public(format!(
                        "Runtime events were delayed ({skipped}); refreshing authoritative state."
                    ))));
                effects.push(ControllerEffect::Refresh);
                effects
            }
            ControllerEvent::BackendSourceClosed => {
                self.apply_action(AppAction::Notice(Notification::public(
                    "Runtime event stream closed; durable state will continue to refresh.",
                )))
            }
            ControllerEvent::BackendSourceUnavailable { detail } => self.apply_action(
                AppAction::Notice(Notification::public(if detail.is_empty() {
                    "Runtime event stream is unavailable; durable state will continue to refresh."
                        .to_owned()
                } else {
                    format!("Runtime events unavailable: {detail}")
                })),
            ),
            ControllerEvent::SnapshotUpdated { result, .. } => match result {
                Ok(snapshot) => self.reduce_snapshot(snapshot),
                Err(error) => self.reduce_backend_error(error),
            },
            ControllerEvent::ActivityUpdated { result, .. } => match result {
                Ok(batch) => self.reduce_activity(batch),
                Err(error) => self.reduce_backend_error(error),
            },
            ControllerEvent::CommandCompleted {
                id,
                command,
                result,
            } => self.reduce_command_completed(id, command, result),
            ControllerEvent::ShutdownCompleted { result, .. } => match result {
                Ok(_) => self.apply_action(AppAction::ShutdownComplete),
                Err(error) => self.reduce_backend_error(error),
            },
            ControllerEvent::EffectRejected { effect, error } => {
                self.reduce_effect_rejected(effect, error)
            }
            ControllerEvent::BackendError(error) => self.reduce_backend_error(error),
            ControllerEvent::Tick => self.apply_action(AppAction::Tick),
            ControllerEvent::ActivityPollDue => self.activity_effects(None),
        }
    }

    fn reduce_input(&mut self, input: InputEvent) -> Vec<ControllerEffect> {
        let Some(action) = app_action_from_input(&self.state, self.keymap, input) else {
            return Vec::new();
        };
        self.apply_action(action)
    }

    fn reduce_snapshot(&mut self, snapshot: SoloSnapshot) -> Vec<ControllerEffect> {
        // Candidate discovery is authoritative and may change while this
        // process is running (for example, after the user logs into a CLI).
        // Replace the picker input on every refresh, including an empty list,
        // so stale identities cannot remain selectable.
        self.setup_candidates = snapshot
            .setup_agents
            .iter()
            .map(to_setup_agent_candidate)
            .collect();
        self.remember_snapshot(&snapshot);
        let mut projection = to_projection_snapshot(&snapshot);
        projection.setup = self.setup_for_snapshot(&snapshot, projection.setup);
        // A durable refresh must not replace a user's approval/review,
        // cancellation, help, or error card while it is being read. A fresh
        // authoritative interaction is allowed to replace an old question
        // card because its target/version may have changed.
        if matches!(
            self.state.modal,
            Some(
                ModalState::Approval(_)
                    | ModalState::Review(_)
                    | ModalState::Cancel(_)
                    | ModalState::Help
                    | ModalState::Error(_)
            )
        ) {
            projection.modal = self.state.modal.clone();
        }
        let mut effects = self.apply_action(AppAction::ReplaceSnapshot(projection));
        // Snapshot refreshes are the authoritative source for a live turn's
        // optimistic mutation version.  Command results also carry this
        // version, but a restart or the first refresh after a retry only has
        // the durable snapshot.  Keep cancellation fenced to that exact
        // version instead of leaving AppState's launch-time zero in place.
        if let Some(turn) = snapshot.chat.active_turns.iter().rev().find(|turn| {
            turn.id
                == self
                    .state
                    .live_activity
                    .as_ref()
                    .map(|activity| activity.turn_id.as_str())
                    .unwrap_or_default()
                && matches!(
                    turn.state,
                    BackendTurnState::Queued
                        | BackendTurnState::Leased
                        | BackendTurnState::AwaitingInput
                        | BackendTurnState::RetryWait
                )
        }) {
            effects.extend(self.apply_action(AppAction::SetTurnVersion {
                turn_id: turn.id.clone(),
                expected_version: nonnegative_u64(turn.version),
            }));
        }
        effects.extend(self.activity_effects(None));
        effects
    }

    fn reduce_activity(&mut self, batch: ActivityBatch) -> Vec<ControllerEffect> {
        let key = ActivityKey::from(&batch.target);
        let next_cursor = batch.next_cursor.clone();
        let target = batch.target.clone();
        let matches_live = self.activity_matches_live(&target);
        self.activities.insert(
            key.clone(),
            ActivityState {
                target: target.clone(),
                cursor: next_cursor.clone(),
            },
        );

        let mut effects = Vec::new();
        if matches_live && batch.cursor_reset {
            // A rotated/truncated log starts a new durable cursor.  Clear
            // only the bounded in-memory entries; retain turn/attempt
            // identity so the renderer never merges attempts.
            if let Some(activity) = self.state.live_activity.clone() {
                if target_matches_activity(&target, &activity) {
                    let mut reset = activity;
                    reset.items.clear();
                    effects.extend(self.apply_action(AppAction::SetLiveActivity(Some(reset))));
                }
            }
        }
        if matches_live {
            for entry in &batch.entries {
                if entry.attempt == target.attempt {
                    effects.extend(self.apply_action(AppAction::Activity(to_activity_item(entry))));
                }
            }
        }
        if matches_live && batch.has_more {
            effects.push(ControllerEffect::ReadActivity(ActivityReadRequest {
                target,
                cursor: next_cursor,
                limit: DEFAULT_ACTIVITY_PAGE,
            }));
        }
        effects
    }

    fn reduce_backend_event(&mut self, event: BackendEvent) -> Vec<ControllerEffect> {
        match event.kind {
            // Events are invalidation hints, not a second source of truth.
            // The controller schedules the durable refresh for these cases.
            BackendEventKind::SnapshotInvalidated {
                reason:
                    InvalidationReason::Chat
                    | InvalidationReason::Turn
                    | InvalidationReason::Task
                    | InvalidationReason::Attention
                    | InvalidationReason::Approval
                    | InvalidationReason::Project
                    | InvalidationReason::Runtime
                    | InvalidationReason::Recovery
                    | InvalidationReason::Unknown,
            } => Vec::new(),
            BackendEventKind::RuntimeStateChanged { .. } => Vec::new(),
            BackendEventKind::ActivityAvailable { target } => self.activity_effects(Some(target)),
        }
    }

    fn reduce_command_completed(
        &mut self,
        controller_id: u64,
        command: BackendCommand,
        result: BackendResult<BackendCommandResult>,
    ) -> Vec<ControllerEffect> {
        let key = command.idempotency_key().as_str().to_owned();
        let local_id = self
            .pending_backend_commands
            .remove(&key)
            .unwrap_or(controller_id);
        match result {
            Ok(result) => {
                let server_id = command_result_server_id(&result);
                let mut effects = self.apply_action(AppAction::CommandFinished(CommandResult {
                    command_id: local_id,
                    outcome: CommandOutcome::Succeeded { server_id },
                }));
                effects.extend(self.apply_command_result(result));
                effects
            }
            Err(error) => self.apply_action(AppAction::CommandFinished(CommandResult {
                command_id: local_id,
                outcome: CommandOutcome::Failed {
                    error: to_failure_notice(&error),
                },
            })),
        }
    }

    fn apply_command_result(&mut self, result: BackendCommandResult) -> Vec<ControllerEffect> {
        match result {
            BackendCommandResult::AgentSelected { .. } => vec![ControllerEffect::Refresh],
            BackendCommandResult::MessageSent { message, turn, .. } => {
                let mut effects =
                    self.apply_action(AppAction::AddMessage(to_chat_message(&message)));
                effects.extend(self.apply_turn_snapshot(&turn));
                effects
            }
            BackendCommandResult::InteractionAnswered { turn, .. }
            | BackendCommandResult::TurnCancelled { turn, .. }
            | BackendCommandResult::TurnRetried { turn, .. } => self.apply_turn_snapshot(&turn),
            BackendCommandResult::ReviewDecided { task, .. } => {
                self.replace_task(to_task_summary(&task))
            }
            BackendCommandResult::ApprovalDecided {
                approval_id,
                project,
                ..
            } => self.apply_approval_result(&approval_id, &project),
        }
    }

    fn apply_turn_snapshot(&mut self, turn: &TurnSnapshot) -> Vec<ControllerEffect> {
        let state = to_app_turn_state(turn.state);
        let mut effects = Vec::new();
        if state.is_live() {
            let needs_start = self
                .state
                .live_activity
                .as_ref()
                .is_none_or(|activity| activity.turn_id != turn.id);
            if needs_start {
                effects.extend(self.apply_action(AppAction::TurnStarted {
                    turn_id: turn.id.clone(),
                    attempt: turn.attempt,
                    summary: turn_summary(turn),
                }));
            }
            effects.extend(self.apply_action(AppAction::SetTurnVersion {
                turn_id: turn.id.clone(),
                expected_version: nonnegative_u64(turn.version),
            }));
            effects.extend(self.apply_action(AppAction::TurnStateChanged {
                turn_id: turn.id.clone(),
                state,
            }));
        } else {
            effects.extend(self.apply_action(AppAction::TurnFinished {
                turn_id: turn.id.clone(),
                state,
                assistant_message: None,
                retryable: turn.retryable,
                expected_version: nonnegative_u64(turn.version),
            }));
        }
        effects
    }

    fn apply_approval_result(
        &mut self,
        approval_id: &str,
        project: &ProjectSnapshot,
    ) -> Vec<ControllerEffect> {
        let mut projection = self.current_projection();
        projection
            .approvals
            .retain(|approval| approval.id != approval_id);
        projection.header = HeaderState {
            repository: self.state.header.repository.clone(),
            project: project.name.clone(),
            agent: project
                .selected_agent
                .as_ref()
                .map_or_else(String::new, agent_label),
            readiness: to_app_readiness(project.readiness),
            runtime: to_app_runtime(&project.runtime),
        };
        projection.setup = to_setup_state_from_project(project, &projection.approvals);
        self.apply_action(AppAction::ReplaceSnapshot(projection))
    }

    fn replace_task(&mut self, task: TaskSummary) -> Vec<ControllerEffect> {
        let mut projection = self.current_projection();
        projection
            .review_cards
            .retain(|card| card.task_id != task.id);
        if task.state == AppTaskState::AwaitingReview {
            projection
                .review_cards
                .push(ReviewCard::from_task_summary(&task));
        }
        if let Some(existing) = projection
            .tasks
            .iter_mut()
            .find(|existing| existing.id == task.id)
        {
            *existing = task;
        } else {
            projection.tasks.push(task);
        }
        self.apply_action(AppAction::ReplaceSnapshot(projection))
    }

    fn reduce_effect_rejected(
        &mut self,
        effect: ControllerEffect,
        error: BackendError,
    ) -> Vec<ControllerEffect> {
        let ControllerEffect::Command(command) = effect else {
            return self.reduce_backend_error(error);
        };
        let key = command.idempotency_key().as_str().to_owned();
        let Some(local_id) = self.pending_backend_commands.remove(&key) else {
            return self.reduce_backend_error(error);
        };
        self.apply_action(AppAction::CommandFinished(CommandResult {
            command_id: local_id,
            outcome: CommandOutcome::Failed {
                error: to_failure_notice(&error),
            },
        }))
    }

    fn reduce_backend_error(&mut self, error: BackendError) -> Vec<ControllerEffect> {
        self.apply_action(AppAction::Notice(notification_for_error(&error)))
    }

    fn apply_action(&mut self, action: AppAction) -> Vec<ControllerEffect> {
        let reduction = self.state.reduce(action);
        self.effects_for_reduction(reduction)
    }

    fn effects_for_reduction(&mut self, reduction: crate::app::Reduction) -> Vec<ControllerEffect> {
        let mut effects = Vec::new();
        for command in reduction.commands {
            effects.extend(self.effect_for_command(command));
        }
        effects
    }

    fn effect_for_command(&mut self, command: Command) -> Vec<ControllerEffect> {
        let command_id = command.id;
        let key = command.idempotency_key.clone();
        let request = command.request;
        match request {
            CommandRequest::SendMessage { text } => {
                let backend =
                    BackendCommand::SendMessage(SendMessageRequest::new(text, key.clone()));
                self.pending_backend_commands.insert(key, command_id);
                vec![ControllerEffect::Command(backend)]
            }
            CommandRequest::AnswerQuestion {
                question_id,
                option_id,
            } => {
                let Some(interaction) = self.interactions.get(&question_id).cloned() else {
                    return self.complete_local_failure(
                        command_id,
                        BackendError::conflict(
                            "the runtime interaction changed; refresh before answering",
                            None,
                        ),
                    );
                };
                let Some(answer) = interaction_answer(&interaction, &option_id) else {
                    return self.complete_local_failure(
                        command_id,
                        BackendError::invalid_input("the selected interaction answer is not valid"),
                    );
                };
                let backend = BackendCommand::AnswerInteraction(InteractionAnswerRequest {
                    interaction_id: interaction.id,
                    turn_id: interaction.turn_id,
                    expected_version: interaction.expected_version,
                    answers: vec![answer],
                    idempotency_key: key.clone().into(),
                });
                self.pending_backend_commands.insert(key, command_id);
                vec![ControllerEffect::Command(backend)]
            }
            CommandRequest::Approval {
                approval_id,
                action,
                expected_version,
                expected_digest,
            } => {
                let Some(action) = to_backend_approval_action(action) else {
                    return self.complete_local_failure(
                        command_id,
                        BackendError::invalid_input("that approval action is not permitted"),
                    );
                };
                let Some(expected_version) = expected_version else {
                    return self.complete_local_failure(
                        command_id,
                        BackendError::invalid_input("approval is missing its expected version"),
                    );
                };
                let Some(target_digest) = expected_digest else {
                    return self.complete_local_failure(
                        command_id,
                        BackendError::invalid_input("approval is missing its expected digest"),
                    );
                };
                let backend = BackendCommand::DecideApproval(ApprovalDecisionRequest {
                    approval_id,
                    expected_version: i64::try_from(expected_version).unwrap_or(i64::MAX),
                    target_digest,
                    action,
                    idempotency_key: key.clone().into(),
                });
                self.pending_backend_commands.insert(key, command_id);
                vec![ControllerEffect::Command(backend)]
            }
            CommandRequest::Review {
                review_id: _,
                guidance,
                task_id,
                action,
                expected_version,
            } => {
                let Some(decision) = to_review_decision(action) else {
                    return self.complete_local_failure(
                        command_id,
                        BackendError::invalid_input("that review action is not permitted"),
                    );
                };
                let backend = BackendCommand::DecideReview(crate::backend::ReviewDecisionRequest {
                    task_id,
                    expected_version: i64::try_from(expected_version).unwrap_or(i64::MAX),
                    // Task reviews currently use the authoritative Task
                    // version.  A future digest-bearing review card can add
                    // it at this boundary without changing AppState.
                    target_digest: String::new(),
                    decision,
                    guidance,
                    idempotency_key: key.clone().into(),
                });
                self.pending_backend_commands.insert(key, command_id);
                vec![ControllerEffect::Command(backend)]
            }
            CommandRequest::CancelTurn {
                turn_id,
                expected_version,
            } => {
                let backend = BackendCommand::CancelTurn(TurnMutationRequest {
                    turn_id,
                    expected_version: i64::try_from(expected_version).unwrap_or(i64::MAX),
                    idempotency_key: key.clone().into(),
                });
                self.pending_backend_commands.insert(key, command_id);
                vec![ControllerEffect::Command(backend)]
            }
            CommandRequest::RetryTurn {
                turn_id,
                expected_version,
            } => {
                let backend = BackendCommand::RetryTurn(TurnMutationRequest {
                    turn_id,
                    expected_version: i64::try_from(expected_version).unwrap_or(i64::MAX),
                    idempotency_key: key.clone().into(),
                });
                self.pending_backend_commands.insert(key, command_id);
                vec![ControllerEffect::Command(backend)]
            }
            CommandRequest::SelectAgent { agent_id } => {
                if agent_id.trim().is_empty() {
                    return self.complete_local_failure(
                        command_id,
                        BackendError::invalid_input("an Agent identity is required"),
                    );
                }
                let backend = BackendCommand::SelectAgent(SelectAgentRequest {
                    agent_id,
                    idempotency_key: key.clone().into(),
                });
                self.pending_backend_commands.insert(key, command_id);
                vec![ControllerEffect::Command(backend)]
            }
            CommandRequest::Refresh => {
                let mut effects = self.complete_local_success(command_id);
                effects.push(ControllerEffect::Refresh);
                effects
            }
            CommandRequest::Shutdown => {
                let mut effects = self.complete_local_success(command_id);
                effects.push(ControllerEffect::RequestShutdown(
                    crate::backend::ShutdownIntent::UserQuit,
                ));
                effects
            }
            CommandRequest::ForceShutdown => {
                let mut effects = self.complete_local_success(command_id);
                effects.push(ControllerEffect::RequestShutdown(
                    crate::backend::ShutdownIntent::Signal,
                ));
                effects
            }
        }
    }

    fn complete_local_success(&mut self, command_id: u64) -> Vec<ControllerEffect> {
        let reduction = self.state.reduce(AppAction::CommandFinished(CommandResult {
            command_id,
            outcome: CommandOutcome::Succeeded { server_id: None },
        }));
        self.effects_for_reduction(reduction)
    }

    fn complete_local_failure(
        &mut self,
        command_id: u64,
        error: BackendError,
    ) -> Vec<ControllerEffect> {
        let reduction = self.state.reduce(AppAction::CommandFinished(CommandResult {
            command_id,
            outcome: CommandOutcome::Failed {
                error: to_failure_notice(&error),
            },
        }));
        self.effects_for_reduction(reduction)
    }

    fn remember_snapshot(&mut self, snapshot: &SoloSnapshot) {
        self.scope = Some(snapshot.scope.clone());
        self.interactions = snapshot
            .chat
            .interactions
            .iter()
            .cloned()
            .map(|interaction| (interaction.id.clone(), interaction))
            .collect();
        let mut activities = HashMap::new();
        for live in &snapshot.live_activity {
            let key = ActivityKey::from(&live.target);
            let cursor = self
                .activities
                .get(&key)
                .map(|activity| activity.cursor.clone())
                .unwrap_or_else(|| live.cursor.clone());
            activities.insert(
                key,
                ActivityState {
                    target: live.target.clone(),
                    cursor,
                },
            );
        }
        self.activities = activities;
    }

    fn setup_for_snapshot(&self, snapshot: &SoloSnapshot, fallback: SetupState) -> SetupState {
        let mut setup = to_setup_state(snapshot);
        if matches!(setup, SetupState::AgentPicker { .. }) {
            let selected = match &self.state.setup {
                SetupState::AgentPicker { selected, .. } => *selected,
                _ => 0,
            };
            let fallback_candidates = match &fallback {
                SetupState::AgentPicker { candidates, .. } => candidates.clone(),
                _ => Vec::new(),
            };
            let fallback_detail = match &fallback {
                SetupState::AgentPicker { detail, .. } => detail.clone(),
                _ => "Choose an authenticated local CLI harness.".to_owned(),
            };
            let candidates = if self.setup_candidates.is_empty() {
                if !fallback_candidates.is_empty() {
                    fallback_candidates
                } else {
                    match snapshot.project.selected_agent.as_ref() {
                        Some(agent) => vec![to_agent_candidate(agent, true, true)],
                        None => Vec::new(),
                    }
                }
            } else {
                self.setup_candidates.clone()
            };
            setup = SetupState::AgentPicker {
                candidates,
                selected,
                detail: fallback_detail,
            };
        }
        setup
    }

    fn current_projection(&self) -> ProjectionSnapshot {
        ProjectionSnapshot {
            header: self.state.header.clone(),
            setup: self.state.setup.clone(),
            timeline: self.state.timeline.clone(),
            live_activity: self.state.live_activity.clone(),
            retryable_turn: self.state.retryable_turn.clone(),
            tasks: self.state.tasks.clone(),
            review_cards: self.state.review_cards.clone(),
            attention: self.state.attention.clone(),
            approvals: self.state.approvals.clone(),
            notifications: self.state.notifications.clone(),
            modal: self.state.modal.clone(),
        }
    }

    fn activity_effects(&mut self, requested: Option<ActivityTarget>) -> Vec<ControllerEffect> {
        if let Some(target) = requested {
            let key = ActivityKey::from(&target);
            let cursor = self
                .activities
                .get(&key)
                .map(|activity| activity.cursor.clone())
                .unwrap_or_else(|| ActivityCursor::beginning(&target));
            self.activities.entry(key).or_insert_with(|| ActivityState {
                target: target.clone(),
                cursor: cursor.clone(),
            });
            return if self.activity_matches_live(&target) {
                vec![ControllerEffect::ReadActivity(ActivityReadRequest {
                    target,
                    cursor,
                    limit: DEFAULT_ACTIVITY_PAGE,
                })]
            } else {
                Vec::new()
            };
        }

        self.activities
            .values()
            .filter(|activity| self.activity_matches_live(&activity.target))
            .map(|activity| {
                ControllerEffect::ReadActivity(ActivityReadRequest {
                    target: activity.target.clone(),
                    cursor: activity.cursor.clone(),
                    limit: DEFAULT_ACTIVITY_PAGE,
                })
            })
            .collect()
    }

    fn activity_matches_live(&self, target: &ActivityTarget) -> bool {
        self.state.live_activity.as_ref().is_some_and(|activity| {
            activity.state.is_live() && target_matches_activity(target, activity)
        })
    }
}

impl SoloReducer for AppReducer {
    fn reduce(&mut self, event: ControllerEvent) -> Vec<ControllerEffect> {
        self.reduce_event(event)
    }
}

/// Convert the controller's terminal-independent key to the existing
/// context-aware keymap and then to a pure [`AppAction`].
pub fn app_action_from_input(
    state: &AppState,
    keymap: Keymap,
    input: InputEvent,
) -> Option<AppAction> {
    match input {
        InputEvent::Key(key) => keymap
            .map(state, to_terminal_key_event(key))
            .map(AppInput::into_action),
        InputEvent::Resize { width, height } => Some(AppAction::Resize { width, height }),
        InputEvent::Interrupt => Some(AppAction::Input(
            if matches!(
                state.header.runtime,
                AppRuntimeState::ShuttingDown | AppRuntimeState::ForcedShutdown
            ) {
                AppInput::ForceQuit
            } else if state.live_turn_id().is_some() {
                AppInput::RequestCancel
            } else {
                AppInput::RequestQuit
            },
        )),
        InputEvent::Quit | InputEvent::Closed => Some(AppAction::Input(AppInput::RequestQuit)),
    }
}

impl AppInput {
    fn into_action(self) -> AppAction {
        AppAction::Input(self)
    }
}

fn to_terminal_key_event(key: ControllerKeyEvent) -> TerminalKeyEvent {
    TerminalKeyEvent {
        code: match key.code {
            ControllerKeyCode::Char(value) => TerminalKeyCode::Char(value),
            ControllerKeyCode::Enter => TerminalKeyCode::Enter,
            ControllerKeyCode::Esc => TerminalKeyCode::Esc,
            ControllerKeyCode::Backspace => TerminalKeyCode::Backspace,
            ControllerKeyCode::Delete => TerminalKeyCode::Delete,
            ControllerKeyCode::Left => TerminalKeyCode::Left,
            ControllerKeyCode::Right => TerminalKeyCode::Right,
            ControllerKeyCode::Up => TerminalKeyCode::Up,
            ControllerKeyCode::Down => TerminalKeyCode::Down,
            ControllerKeyCode::Home => TerminalKeyCode::Home,
            ControllerKeyCode::End => TerminalKeyCode::End,
            ControllerKeyCode::PageUp => TerminalKeyCode::PageUp,
            ControllerKeyCode::PageDown => TerminalKeyCode::PageDown,
            ControllerKeyCode::Tab => TerminalKeyCode::Tab,
            ControllerKeyCode::BackTab => TerminalKeyCode::BackTab,
            ControllerKeyCode::F(number) => TerminalKeyCode::F(number),
        },
        modifiers: to_terminal_modifiers(key.modifiers),
        kind: KeyEventKind::Press,
        state: KeyEventState::NONE,
    }
}

fn to_terminal_modifiers(modifiers: ControllerKeyModifiers) -> TerminalKeyModifiers {
    let mut result = TerminalKeyModifiers::empty();
    if modifiers.ctrl {
        result |= TerminalKeyModifiers::CONTROL;
    }
    if modifiers.alt {
        result |= TerminalKeyModifiers::ALT;
    }
    if modifiers.shift {
        result |= TerminalKeyModifiers::SHIFT;
    }
    result
}

fn target_matches_activity(target: &ActivityTarget, activity: &LiveActivity) -> bool {
    target.attempt == activity.attempt
        && (target.turn_id.as_deref() == Some(activity.turn_id.as_str())
            || target.execution_id == activity.turn_id)
}

fn interaction_answer(
    interaction: &InteractionSnapshot,
    option_id: &str,
) -> Option<InteractionAnswer> {
    if interaction.fields.is_empty() {
        return Some(InteractionAnswer {
            field_id: interaction.id.clone(),
            value: option_id.to_owned(),
        });
    }
    if interaction.fields.len() == 1 {
        let field = &interaction.fields[0];
        if field.choices.is_empty() || field.choices.iter().any(|choice| choice == option_id) {
            return Some(InteractionAnswer {
                field_id: field.id.clone(),
                value: option_id.to_owned(),
            });
        }
        return None;
    }
    let (field_id, value) = option_id.split_once('\0')?;
    let field = interaction
        .fields
        .iter()
        .find(|field| field.id == field_id)?;
    if field.choices.iter().any(|choice| choice == value) {
        Some(InteractionAnswer {
            field_id: field.id.clone(),
            value: value.to_owned(),
        })
    } else {
        None
    }
}

fn to_backend_approval_action(action: AppApprovalAction) -> Option<ApprovalAction> {
    match action {
        AppApprovalAction::Approve => Some(ApprovalAction::Approve),
        AppApprovalAction::Reject => Some(ApprovalAction::Reject),
        AppApprovalAction::Accept
        | AppApprovalAction::RequestChanges
        | AppApprovalAction::Retry
        | AppApprovalAction::Cancel => None,
    }
}

fn to_review_decision(action: AppApprovalAction) -> Option<crate::backend::ReviewDecision> {
    match action {
        AppApprovalAction::Accept => Some(crate::backend::ReviewDecision::Accept),
        AppApprovalAction::Reject | AppApprovalAction::RequestChanges => {
            Some(crate::backend::ReviewDecision::Reject)
        }
        AppApprovalAction::Approve | AppApprovalAction::Retry | AppApprovalAction::Cancel => None,
    }
}

fn command_result_server_id(result: &BackendCommandResult) -> Option<String> {
    match result {
        BackendCommandResult::AgentSelected { agent_id, .. } => Some(agent_id.clone()),
        BackendCommandResult::MessageSent { message, .. } => Some(message.id.clone()),
        BackendCommandResult::InteractionAnswered { turn, .. }
        | BackendCommandResult::TurnCancelled { turn, .. }
        | BackendCommandResult::TurnRetried { turn, .. } => Some(turn.id.clone()),
        BackendCommandResult::ReviewDecided { task, .. } => Some(task.id.clone()),
        BackendCommandResult::ApprovalDecided { approval_id, .. } => Some(approval_id.clone()),
    }
}

pub fn to_failure_notice(error: &BackendError) -> FailureNotice {
    let retryable = matches!(
        error.kind,
        BackendErrorKind::Conflict
            | BackendErrorKind::Unavailable
            | BackendErrorKind::Transport
            | BackendErrorKind::Internal
    );
    if error.is_public() {
        let message = if error.message.is_empty() {
            "Action failed; no details were provided.".to_owned()
        } else {
            error.message.clone()
        };
        let mut notice = if error.is_conflict() {
            FailureNotice::conflict(message)
        } else {
            FailureNotice::public(message, retryable)
        };
        notice.retryable = retryable;
        notice
    } else {
        FailureNotice::protected(retryable)
    }
}

fn notification_for_error(error: &BackendError) -> Notification {
    if error.is_public() {
        Notification::public(if error.message.is_empty() {
            "Action failed; no details were provided.".to_owned()
        } else {
            error.message.clone()
        })
    } else {
        Notification::protected()
    }
}

fn nonnegative_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or_default()
}

mod projection;

pub use projection::{
    to_activity_item, to_app_activity_kind, to_app_check_state, to_app_message_role,
    to_app_readiness, to_app_runtime, to_app_task_state, to_app_turn_state, to_approval_card,
    to_attention_item, to_chat_message, to_check_summary, to_header, to_live_activity,
    to_projection_snapshot, to_question_card, to_review_card, to_setup_state, to_task_summary,
};

use projection::{
    agent_label, to_agent_candidate, to_setup_agent_candidate, to_setup_state_from_project,
    turn_summary,
};

#[cfg(test)]
mod tests;
