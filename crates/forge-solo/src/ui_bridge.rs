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

/// Product-oriented alias for callers that prefer the Solo name.
pub type SoloAppReducer = AppReducer;

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

    /// Install the physical-key policy used when converting controller keys
    /// to `AppInput`.  The policy remains independent of crossterm events.
    pub fn with_keymap(mut self, keymap: Keymap) -> Self {
        self.keymap = keymap;
        self
    }

    /// Seed or replace the structured setup candidates discovered by
    /// bootstrap.  The backend snapshot intentionally carries only the
    /// selected Agent, while discovery/authentication stays at bootstrap.
    pub fn with_setup_candidates(mut self, candidates: Vec<AgentCandidate>) -> Self {
        self.setup_candidates = candidates;
        self
    }

    pub fn set_setup_candidates(&mut self, candidates: Vec<AgentCandidate>) {
        self.setup_candidates = candidates;
    }

    pub fn state(&self) -> &AppState {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut AppState {
        &mut self.state
    }

    pub fn into_state(self) -> AppState {
        self.state
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
        retryable_turn: latest_retryable_failed_turn(&snapshot.chat.active_turns)
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

fn latest_retryable_failed_turn(turns: &[TurnSnapshot]) -> Option<&TurnSnapshot> {
    turns
        .last()
        .filter(|turn| turn.state == BackendTurnState::Failed && turn.retryable)
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
        permitted_actions: vec![AppApprovalAction::Accept, AppApprovalAction::RequestChanges],
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

fn to_setup_state_from_project(
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

fn to_agent_candidate(
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

fn to_setup_agent_candidate(candidate: &SetupAgentSnapshot) -> AgentCandidate {
    AgentCandidate {
        id: candidate.id.clone(),
        label: candidate.name.clone(),
        kind: candidate.harness.clone(),
        available: candidate.available,
        authenticated: candidate.authenticated,
        detail: candidate.detail.clone(),
    }
}

fn agent_label(agent: &AgentSnapshot) -> String {
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

fn turn_summary(turn: &TurnSnapshot) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{
        ActivityCursor, ActivityEntry, ActivityTarget, ChatSnapshot, CommitEvidence,
        ConflictTarget, FailureSnapshot, InteractionField, MessageStatus, ProjectSnapshot,
        SnapshotRequest, TurnSnapshot,
    };

    fn scope() -> SoloScope {
        SoloScope::new("owner", "project", "repo", "chat")
    }

    fn snapshot() -> SoloSnapshot {
        let target = ActivityTarget {
            project_id: "project".into(),
            turn_id: Some("turn".into()),
            task_id: None,
            execution_id: "execution".into(),
            attempt: 2,
        };
        SoloSnapshot {
            scope: scope(),
            repository: RepositorySnapshot {
                id: "repo".into(),
                name: "repo".into(),
                root: "/repo".into(),
                default_branch: "main".into(),
            },
            project: ProjectSnapshot {
                id: "project".into(),
                name: "Project".into(),
                readiness: BackendProjectReadiness::AwaitingApproval,
                runtime: BackendRuntimeState::Ready,
                workflow: "autonomous_v1".into(),
                selected_agent: Some(AgentSnapshot {
                    id: "agent".into(),
                    name: "Codex".into(),
                    harness: "codex".into(),
                }),
            },
            chat: ChatSnapshot {
                id: "chat".into(),
                messages: vec![MessageSnapshot {
                    id: "message".into(),
                    sequence: 1,
                    role: MessageRole::User,
                    content: "hello".into(),
                    created_at: "now".into(),
                    turn_id: None,
                    status: MessageStatus::Complete,
                }],
                has_older_messages: false,
                active_turns: Vec::new(),
                interactions: vec![InteractionSnapshot {
                    id: "question".into(),
                    turn_id: "turn".into(),
                    prompt: "Choose one".into(),
                    fields: vec![InteractionField {
                        id: "choice".into(),
                        label: "Choice".into(),
                        required: true,
                        choices: vec!["yes".into(), "no".into()],
                    }],
                    expected_version: 7,
                }],
            },
            setup_agents: Vec::new(),
            live_activity: vec![LiveActivitySnapshot {
                target: target.clone(),
                state: BackendTurnState::Leased,
                summary: "working".into(),
                worker: None,
                reviewer: None,
                entries: vec![ActivityEntry {
                    sequence: 0,
                    attempt: 2,
                    kind: ActivityKind::ToolCall,
                    summary: "run tests".into(),
                    preview: Some("cargo test".into()),
                }],
                cursor: ActivityCursor::beginning(&target),
            }],
            tasks: vec![TaskSnapshot {
                id: "task".into(),
                title: "Review me".into(),
                state: BackendTaskState::Review,
                version: 4,
                worker: None,
                reviewer: None,
                checks: vec![CheckSnapshot {
                    name: "tests".into(),
                    status: CheckStatus::Passed,
                    summary: Some("passed".into()),
                }],
                commit: Some(CommitEvidence {
                    commit: Some("abc".into()),
                    changed_files: vec!["src/lib.rs".into()],
                    merged: false,
                    summary: None,
                }),
                blocker: Some(FailureSnapshot {
                    kind: "review".into(),
                    headline: "needs review".into(),
                    detail: "review target".into(),
                    retryable: false,
                }),
                retryable: false,
            }],
            attention: vec![crate::backend::AttentionSnapshot {
                id: "attention".into(),
                kind: crate::backend::AttentionKind::Approval,
                headline: "Approve".into(),
                detail: "exact target".into(),
                affected_id: Some("approval".into()),
                permitted_action: Some(crate::backend::AttentionAction::Approve),
            }],
            approvals: vec![ApprovalSnapshot {
                id: "approval".into(),
                kind: ApprovalKind::CharterAdoption,
                title: "Adopt".into(),
                summary: "adopt charter".into(),
                revision: "rev-1".into(),
                digest: "sha256:abc".into(),
                operating_skill_revision: Some("skill-1".into()),
                expected_version: 3,
                selected_agent: None,
                permitted_actions: vec![ApprovalAction::Approve],
            }],
            refreshed_at: "now".into(),
        }
    }

    fn turn(id: &str, state: BackendTurnState, version: i64, retryable: bool) -> TurnSnapshot {
        TurnSnapshot {
            id: id.into(),
            triggering_message_id: format!("message-{id}"),
            state,
            version,
            attempt: 2,
            reply: None,
            assistant_message_id: None,
            failure: (state == BackendTurnState::Failed).then(|| FailureSnapshot {
                kind: "executor".into(),
                headline: format!("{id} failed"),
                detail: "durable failure".into(),
                retryable,
            }),
            retryable,
        }
    }

    #[test]
    fn snapshot_mapping_preserves_typed_setup_chat_activity_tasks_and_approval() {
        let projection = to_projection_snapshot(&snapshot());
        assert_eq!(projection.header.project, "Project");
        assert_eq!(projection.header.agent, "Codex");
        assert!(matches!(projection.setup, SetupState::Adoption { .. }));
        assert_eq!(projection.timeline[0].content, "hello");
        assert_eq!(
            projection.approvals[0].expected_digest.as_deref(),
            Some("sha256:abc")
        );
        assert!(matches!(projection.modal, Some(ModalState::Question(_))));
        assert_eq!(projection.tasks[0].version, 4);
        assert_eq!(
            projection.live_activity.as_ref().map(|a| a.attempt),
            Some(2)
        );
    }

    #[test]
    fn failed_turn_snapshot_remains_visible_and_supplies_canonical_retry() {
        let mut snapshot = snapshot();
        snapshot.project.readiness = BackendProjectReadiness::Operational;
        snapshot.chat.interactions.clear();
        snapshot.chat.active_turns = vec![turn("failed", BackendTurnState::Failed, 9, true)];
        snapshot.live_activity.clear();

        let projection = to_projection_snapshot(&snapshot);
        let activity = projection
            .live_activity
            .expect("durable failure should remain visible in activity");
        assert_eq!(activity.turn_id, "failed");
        assert_eq!(activity.state, AppTurnState::Failed);
        assert_eq!(activity.summary, "failed failed");
        assert_eq!(
            projection.retryable_turn,
            Some(TurnReference {
                turn_id: "failed".into(),
                attempt: 2,
                expected_version: 9,
            })
        );
    }

    #[test]
    fn newer_running_or_succeeded_turn_fences_an_older_failure() {
        let mut snapshot = snapshot();
        snapshot.project.readiness = BackendProjectReadiness::Operational;
        snapshot.chat.interactions.clear();
        snapshot.chat.active_turns = vec![
            turn("old-failure", BackendTurnState::Failed, 3, true),
            turn("new-running", BackendTurnState::Leased, 4, false),
        ];
        snapshot.live_activity.clear();

        let running = to_projection_snapshot(&snapshot);
        assert_eq!(
            running
                .live_activity
                .as_ref()
                .map(|activity| activity.turn_id.as_str()),
            Some("new-running")
        );
        assert_eq!(
            running
                .live_activity
                .as_ref()
                .map(|activity| activity.state),
            Some(AppTurnState::Running)
        );
        assert!(running.retryable_turn.is_none());

        snapshot.chat.active_turns = vec![
            turn("old-failure", BackendTurnState::Failed, 3, true),
            turn("new-success", BackendTurnState::Succeeded, 5, false),
        ];
        snapshot.live_activity = vec![LiveActivitySnapshot {
            target: ActivityTarget {
                project_id: "project".into(),
                turn_id: Some("old-failure".into()),
                task_id: None,
                execution_id: "old-failure".into(),
                attempt: 2,
            },
            state: BackendTurnState::Failed,
            summary: "old failure".into(),
            worker: None,
            reviewer: None,
            entries: Vec::new(),
            cursor: ActivityCursor::beginning(&ActivityTarget {
                project_id: "project".into(),
                turn_id: Some("old-failure".into()),
                task_id: None,
                execution_id: "old-failure".into(),
                attempt: 2,
            }),
        }];
        let succeeded = to_projection_snapshot(&snapshot);
        assert!(succeeded.live_activity.is_none());
        assert!(succeeded.retryable_turn.is_none());
    }

    #[test]
    fn failed_snapshot_reaches_retry_command_without_becoming_cancellable() {
        let mut snapshot = snapshot();
        snapshot.project.readiness = BackendProjectReadiness::Operational;
        snapshot.chat.interactions.clear();
        snapshot.chat.active_turns = vec![turn("failed", BackendTurnState::Failed, 9, true)];
        snapshot.live_activity.clear();
        let mut reducer = AppReducer::new(AppState::new());
        let initial = reducer.reduce(ControllerEvent::SnapshotUpdated {
            request: SnapshotRequest::default(),
            result: Ok(snapshot),
        });
        assert!(initial
            .iter()
            .all(|effect| !matches!(effect, ControllerEffect::ReadActivity(_))));
        assert_eq!(reducer.state().live_turn_id(), None);
        assert_eq!(
            reducer.state().live_activity.as_ref().map(|a| a.state),
            Some(AppTurnState::Failed)
        );

        reducer.reduce(ControllerEvent::Input(InputEvent::Key(
            ControllerKeyEvent::new(ControllerKeyCode::Tab),
        )));
        let effects = reducer.reduce(ControllerEvent::Input(InputEvent::Key(
            ControllerKeyEvent::new(ControllerKeyCode::Char('r')),
        )));
        assert!(effects.iter().any(|effect| matches!(
            effect,
            ControllerEffect::Command(BackendCommand::RetryTurn(request))
                if request.turn_id == "failed" && request.expected_version == 9
        )));
    }

    #[test]
    fn live_snapshot_refreshes_exact_version_for_cancel_command() {
        let mut snapshot = snapshot();
        snapshot.project.readiness = BackendProjectReadiness::Operational;
        snapshot.chat.interactions.clear();
        snapshot.chat.active_turns = vec![turn("live", BackendTurnState::Leased, 7, false)];
        snapshot.live_activity.clear();

        let mut reducer = AppReducer::new(AppState::new());
        reducer.reduce(ControllerEvent::SnapshotUpdated {
            request: SnapshotRequest::default(),
            result: Ok(snapshot),
        });
        assert_eq!(reducer.state().active_turn_version, 7);

        reducer.reduce(ControllerEvent::Input(InputEvent::Key(
            ControllerKeyEvent::ctrl(ControllerKeyCode::Char('c')),
        )));
        let effects = reducer.reduce(ControllerEvent::Input(InputEvent::Key(
            ControllerKeyEvent::new(ControllerKeyCode::Enter),
        )));
        assert!(effects.iter().any(|effect| matches!(
            effect,
            ControllerEffect::Command(BackendCommand::CancelTurn(request))
                if request.turn_id == "live" && request.expected_version == 7
        )));
    }

    #[test]
    fn wide_project_rail_enters_exact_approval_then_requires_modal_accept() {
        let mut snapshot = snapshot();
        snapshot.project.readiness = BackendProjectReadiness::Operational;
        snapshot.chat.interactions.clear();
        let mut reducer = AppReducer::new(AppState::new());
        reducer.reduce(ControllerEvent::SnapshotUpdated {
            request: SnapshotRequest::default(),
            result: Ok(snapshot),
        });
        reducer.state_mut().focus = crate::app::FocusTarget::ProjectRail;
        reducer.state_mut().rail.tab = crate::app::ProjectTab::Approvals;

        let opened = reducer.reduce(ControllerEvent::Input(InputEvent::Key(
            ControllerKeyEvent::new(ControllerKeyCode::Enter),
        )));
        assert!(opened.is_empty(), "opening a card must not execute it");
        assert!(matches!(
            &reducer.state().modal,
            Some(ModalState::Approval(card))
                if card.id == "approval"
                    && card.expected_version == Some(3)
                    && card.expected_digest.as_deref() == Some("sha256:abc")
        ));

        let effects = reducer.reduce(ControllerEvent::Input(InputEvent::Key(
            ControllerKeyEvent::new(ControllerKeyCode::Enter),
        )));
        assert!(effects.iter().any(|effect| matches!(
            effect,
            ControllerEffect::Command(BackendCommand::DecideApproval(request))
                if request.approval_id == "approval"
                    && request.expected_version == 3
                    && request.target_digest == "sha256:abc"
                    && request.action == crate::backend::ApprovalAction::Approve
        )));
    }

    #[test]
    fn narrow_task_entry_opens_authoritative_review_then_emits_exact_decision() {
        let mut snapshot = snapshot();
        snapshot.project.readiness = BackendProjectReadiness::Operational;
        snapshot.chat.interactions.clear();
        let mut reducer = AppReducer::new(AppState::new());
        reducer.reduce(ControllerEvent::SnapshotUpdated {
            request: SnapshotRequest::default(),
            result: Ok(snapshot),
        });
        reducer.state_mut().focus = crate::app::FocusTarget::Timeline;
        reducer.state_mut().layout = crate::app::LayoutMode::Narrow;
        reducer.state_mut().rail.tab = crate::app::ProjectTab::Tasks;

        let opened = reducer.reduce(ControllerEvent::Input(InputEvent::Key(
            ControllerKeyEvent::new(ControllerKeyCode::Enter),
        )));
        assert!(opened.is_empty(), "opening a card must not execute it");
        assert!(matches!(
            &reducer.state().modal,
            Some(ModalState::Review(card))
                if card.id == "task"
                    && card.task_id == "task"
                    && card.expected_version == 4
                    && card.checks.len() == 1
                    && card.changed_files == vec!["src/lib.rs"]
        ));

        let effects = reducer.reduce(ControllerEvent::Input(InputEvent::Key(
            ControllerKeyEvent::new(ControllerKeyCode::Enter),
        )));
        assert!(effects.iter().any(|effect| matches!(
            effect,
            ControllerEffect::Command(BackendCommand::DecideReview(request))
                if request.task_id == "task"
                    && request.expected_version == 4
                    && request.decision == crate::backend::ReviewDecision::Accept
        )));
    }

    #[test]
    fn charter_drafting_admits_chat_but_not_task_mutation() {
        let readiness = to_app_readiness(BackendProjectReadiness::AwaitingCharter);
        assert!(readiness.allows_chat());
        assert!(!readiness.allows_mutating_tasks());
    }

    #[test]
    fn setup_snapshot_candidates_replace_the_picker_projection() {
        let mut snapshot = snapshot();
        snapshot.project.readiness = BackendProjectReadiness::AwaitingAgent;
        snapshot.setup_agents = vec![SetupAgentSnapshot {
            id: "new-agent".into(),
            name: "New Codex".into(),
            harness: "codex".into(),
            available: true,
            authenticated: true,
            detail: "authenticated after launch".into(),
        }];

        let projection = to_projection_snapshot(&snapshot);
        let SetupState::AgentPicker { candidates, .. } = projection.setup else {
            panic!("awaiting-agent snapshot should render the setup picker");
        };
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].id, "new-agent");
        assert_eq!(candidates[0].detail, "authenticated after launch");
    }

    #[test]
    fn refresh_keeps_activity_file_size_cursor_across_snapshots() {
        let mut snapshot = snapshot();
        snapshot.live_activity[0].cursor.file_size = 42;
        let target = snapshot.live_activity[0].target.clone();
        let mut reducer = AppReducer::default();
        reducer.reduce(ControllerEvent::SnapshotUpdated {
            request: SnapshotRequest::default(),
            result: Ok(snapshot.clone()),
        });

        let key = ActivityKey::from(&target);
        assert_eq!(reducer.activities[&key].cursor.file_size, 42);

        snapshot.live_activity[0].cursor.file_size = 7;
        reducer.reduce(ControllerEvent::SnapshotUpdated {
            request: SnapshotRequest::default(),
            result: Ok(snapshot),
        });
        assert_eq!(
            reducer.activities[&key].cursor.file_size, 42,
            "an authoritative refresh must not erase an incremental log cursor"
        );
    }

    #[test]
    fn key_mapping_uses_app_context_and_resize_is_pure() {
        let mut state = AppState::new();
        state.header.runtime = AppRuntimeState::Ready;
        state.header.readiness = ProjectReadiness::Ready;
        assert_eq!(
            app_action_from_input(
                &state,
                Keymap::default(),
                InputEvent::Key(ControllerKeyEvent::new(ControllerKeyCode::Char('x'))),
            ),
            Some(AppAction::Input(AppInput::Insert('x')))
        );
        assert_eq!(
            app_action_from_input(
                &state,
                Keymap::default(),
                InputEvent::Resize {
                    width: 80,
                    height: 20,
                },
            ),
            Some(AppAction::Resize {
                width: 80,
                height: 20,
            })
        );
    }

    #[test]
    fn send_and_select_agent_commands_cross_typed_backend_boundary() {
        let mut reducer = AppReducer::new({
            let mut state = AppState::new();
            state.header.runtime = AppRuntimeState::Ready;
            state.header.readiness = ProjectReadiness::Ready;
            state
        });
        reducer
            .state_mut()
            .reduce(AppAction::Input(AppInput::Insert('h')));
        let send_effects = reducer.reduce(ControllerEvent::Input(InputEvent::Key(
            ControllerKeyEvent::new(ControllerKeyCode::Enter),
        )));
        assert!(matches!(
            send_effects.as_slice(),
            [ControllerEffect::Command(BackendCommand::SendMessage(_))]
        ));
        let mut setup = AppState::new();
        setup.setup = SetupState::AgentPicker {
            candidates: vec![AgentCandidate {
                id: "agent".into(),
                label: "Codex".into(),
                kind: "codex".into(),
                available: true,
                authenticated: true,
                detail: String::new(),
            }],
            selected: 0,
            detail: String::new(),
        };
        let mut setup_reducer = AppReducer::new(setup);
        let effects = setup_reducer.reduce(ControllerEvent::Input(InputEvent::Key(
            ControllerKeyEvent::new(ControllerKeyCode::Enter),
        )));
        assert!(matches!(
            effects.as_slice(),
            [ControllerEffect::Command(BackendCommand::SelectAgent(_))]
        ));
    }

    #[test]
    fn interaction_answer_uses_authoritative_turn_version_and_idempotency() {
        let mut reducer = AppReducer::new(AppState::new());
        let mut snap = snapshot();
        snap.project.readiness = BackendProjectReadiness::Operational;
        reducer.reduce(ControllerEvent::SnapshotUpdated {
            request: SnapshotRequest::default(),
            result: Ok(snap),
        });
        let effects = reducer.reduce(ControllerEvent::Input(InputEvent::Key(
            ControllerKeyEvent::new(ControllerKeyCode::Enter),
        )));
        assert!(matches!(
            effects.as_slice(),
            [ControllerEffect::Command(BackendCommand::AnswerInteraction(request))]
                if request.interaction_id == "question"
                    && request.turn_id == "turn"
                    && request.expected_version == 7
                    && request.answers[0].value == "yes"
        ));
    }

    #[test]
    fn conflict_preserves_draft_and_requests_authoritative_refresh() {
        let mut state = AppState::new();
        state.header.runtime = AppRuntimeState::Ready;
        state.header.readiness = ProjectReadiness::Ready;
        let mut reducer = AppReducer::new(state);
        reducer.reduce(ControllerEvent::Input(InputEvent::Key(
            ControllerKeyEvent::new(ControllerKeyCode::Char('d')),
        )));
        let effects = reducer.reduce(ControllerEvent::Input(InputEvent::Key(
            ControllerKeyEvent::new(ControllerKeyCode::Enter),
        )));
        let ControllerEffect::Command(command) = effects[0].clone() else {
            panic!("send command expected");
        };
        let effects = reducer.reduce(ControllerEvent::CommandCompleted {
            id: 42,
            command,
            result: Err(BackendError::conflict(
                "stale target",
                Some(ConflictTarget {
                    kind: "turn".into(),
                    id: "turn".into(),
                    expected_version: Some(1),
                    expected_digest: None,
                }),
            )),
        });
        assert_eq!(reducer.state().composer.text, "d");
        assert!(effects
            .iter()
            .any(|effect| matches!(effect, ControllerEffect::Refresh)));
    }

    #[test]
    fn activity_poll_uses_latest_cursor_and_bounded_page() {
        let mut reducer = AppReducer::new(AppState::new());
        let snap = snapshot();
        reducer.reduce(ControllerEvent::SnapshotUpdated {
            request: SnapshotRequest::default(),
            result: Ok(snap),
        });
        let effects = reducer.reduce(ControllerEvent::ActivityPollDue);
        assert!(effects.iter().any(|effect| matches!(
            effect,
            ControllerEffect::ReadActivity(request)
                if request.limit == DEFAULT_ACTIVITY_PAGE && request.cursor.next_sequence == 0
        )));
    }
}
