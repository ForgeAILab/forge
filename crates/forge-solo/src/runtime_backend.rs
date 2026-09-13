//! Production adapter from the shared Solo session facade to the terminal
//! controller.
//!
//! The controller and view remain transport/domain agnostic. This module is
//! the boundary that maps bounded snapshots and typed commands to services.

use std::{
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use db::DbError;
use events::ForgeEvent;
use executors::{AdapterRegistry, AvailabilityStatus, ExecutorKind};
use services::{
    solo_bootstrap::{
        SoloAgentAvailability, SoloAgentCandidate, SoloAgentCandidateInput,
        SoloAuthenticatedAgentRegistration, SoloBootstrapReadiness, SoloBootstrapRequest,
        SoloBootstrapResult, SoloBootstrapService,
    },
    solo_session::{
        SoloActivityCursor, SoloActivityKind, SoloActivityPage, SoloAttentionSnapshot,
        SoloCharterApprovalAuthorization, SoloCharterApprovalInput, SoloCharterApprovalTarget,
        SoloInteractionAnswerInput, SoloInteractionAnswerValue, SoloMessageAuthor,
        SoloMessageStatus, SoloOperationOutcome, SoloProjectReadiness, SoloRetryTurnInput,
        SoloReviewDecision, SoloReviewDecisionInput, SoloSendMessageInput, SoloSessionService,
        SoloSessionSnapshot, SoloTaskSnapshot, SoloTurnSnapshot, SoloTurnStatus,
    },
    EmbeddedDaemon, RuntimeSupervisor, ServiceError,
};
use tokio::{sync::mpsc, task::JoinHandle, time};

use crate::backend::{
    self, backend_event_channel, ActivityBatch, ActivityEntry, ActivityKind, ActivityReadRequest,
    AgentSnapshot, ApprovalAction, ApprovalDecisionRequest, ApprovalKind, ApprovalSnapshot,
    BackendCommand, BackendCommandResult, BackendError, BackendEvent, BackendFuture, BackendResult,
    ChannelBackendEventSource, ChatSnapshot, CheckSnapshot, CheckStatus, CommitEvidence,
    FailureSnapshot, InteractionField, InteractionSnapshot, InvalidationReason,
    LiveActivitySnapshot, MessageRole, MessageSnapshot, MessageStatus, ProjectReadiness,
    ProjectSnapshot, RepositorySnapshot, RuntimeState, SetupAgentSnapshot, ShutdownIntent,
    ShutdownOutcome, SnapshotRequest, SoloBackend, SoloScope, SoloSnapshot, TaskSnapshot,
    TaskState, TurnSnapshot, TurnState,
};

const EVENT_CHANNEL_CAPACITY: usize = 256;
const MAX_SELECTION_DETAIL_CHARS: usize = 512;
const DISCOVERY_REFRESH_INTERVAL: Duration = Duration::from_secs(2);

/// Shared service adapter bound to exactly one Solo Project.
pub struct RuntimeBackend {
    scope: SoloScope,
    session: Arc<SoloSessionService>,
    bootstrap_service: SoloBootstrapService,
    bootstrap_template: SoloBootstrapRequest,
    bootstrap: Arc<RwLock<SoloBootstrapResult>>,
    supervisor: Arc<tokio::sync::Mutex<RuntimeSupervisor>>,
    daemon: Arc<EmbeddedDaemon>,
    daemon_id: String,
    daemon_handle: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
    adapter_registry: Arc<AdapterRegistry>,
    discovery_lock: Arc<tokio::sync::Mutex<()>>,
    last_discovery_at: Arc<tokio::sync::Mutex<Option<Instant>>>,
    runtime_state: Arc<RwLock<RuntimeState>>,
    event_tx: mpsc::Sender<BackendEvent>,
    event_forwarder: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
}

impl std::fmt::Debug for RuntimeBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeBackend")
            .field("scope", &self.scope)
            .field("bootstrap", &self.bootstrap_result().readiness)
            .finish_non_exhaustive()
    }
}

impl RuntimeBackend {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session: Arc<SoloSessionService>,
        bootstrap_service: SoloBootstrapService,
        bootstrap_template: SoloBootstrapRequest,
        bootstrap: SoloBootstrapResult,
        supervisor: Arc<tokio::sync::Mutex<RuntimeSupervisor>>,
        daemon: Arc<EmbeddedDaemon>,
        daemon_id: String,
        daemon_handle: JoinHandle<()>,
        adapter_registry: Arc<AdapterRegistry>,
    ) -> (Arc<Self>, ChannelBackendEventSource) {
        let scope = SoloScope::new(
            bootstrap.owner_id.clone(),
            bootstrap.project_id.clone(),
            bootstrap.repo_id.clone(),
            bootstrap.project_chat_id.clone(),
        );
        let (event_tx, event_source) = backend_event_channel(EVENT_CHANNEL_CAPACITY);
        let mut receiver = session.subscribe();
        let forward_scope = scope.clone();
        let forward_sender = event_tx.clone();
        let forwarder = tokio::spawn(async move {
            let mut sequence = 0_u64;
            while let Ok(ForgeEvent { .. })
            | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) = receiver.recv().await
            {
                sequence = sequence.saturating_add(1);
                if forward_sender
                    .send(BackendEvent::invalidate(
                        forward_scope.clone(),
                        sequence,
                        InvalidationReason::Unknown,
                    ))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        let backend = Arc::new(Self {
            scope,
            session,
            bootstrap_service,
            bootstrap_template,
            bootstrap: Arc::new(RwLock::new(bootstrap)),
            supervisor,
            daemon,
            daemon_id,
            daemon_handle: Arc::new(tokio::sync::Mutex::new(Some(daemon_handle))),
            adapter_registry,
            discovery_lock: Arc::new(tokio::sync::Mutex::new(())),
            last_discovery_at: Arc::new(tokio::sync::Mutex::new(None)),
            runtime_state: Arc::new(RwLock::new(RuntimeState::Starting)),
            event_tx,
            event_forwarder: Arc::new(tokio::sync::Mutex::new(Some(forwarder))),
        });
        (backend, event_source)
    }

    #[must_use]
    pub fn bootstrap_result(&self) -> SoloBootstrapResult {
        self.bootstrap
            .read()
            .map(|result| result.clone())
            .unwrap_or_else(|poisoned| poisoned.into_inner().clone())
    }

    #[must_use]
    pub fn runtime_state(&self) -> RuntimeState {
        self.runtime_state
            .read()
            .map(|state| state.clone())
            .unwrap_or_else(|poisoned| poisoned.into_inner().clone())
    }

    pub fn mark_runtime_ready(&self) {
        if let Ok(mut state) = self.runtime_state.write() {
            *state = RuntimeState::Ready;
        }
    }

    async fn execute_agent_selection(
        &self,
        request: backend::SelectAgentRequest,
    ) -> BackendResult<BackendCommandResult> {
        let identity_id = request.agent_id.trim().to_owned();
        if identity_id.is_empty() {
            return Err(BackendError::invalid_input(
                "Agent selection requires a non-empty identity",
            ));
        }
        let current = self.bootstrap_result();
        let replayed = match current.readiness {
            // A Project selection alone is not enough to replay: the same
            // identity may still need to be persisted as the Task Worker.
            SoloBootstrapReadiness::WorkerSelectionRequired => {
                current.selected_worker_agent_id.as_deref() == Some(identity_id.as_str())
            }
            // Once setup has advanced, replay only when both independent
            // durable selections already name this identity.
            SoloBootstrapReadiness::CharterAdoptionRequired | SoloBootstrapReadiness::Ready => {
                current.selected_project_agent_id.as_deref() == Some(identity_id.as_str())
                    && current.selected_worker_agent_id.as_deref() == Some(identity_id.as_str())
            }
            SoloBootstrapReadiness::AgentSelectionRequired => false,
        };
        if replayed {
            return Ok(BackendCommandResult::AgentSelected {
                agent_id: identity_id,
                replayed: true,
            });
        }
        let candidate = current
            .agent_candidates
            .iter()
            .find(|candidate| candidate.identity_id == identity_id)
            .ok_or_else(|| BackendError::unavailable("selected Agent is not in discovery"))?;
        if !candidate.eligible {
            return Err(BackendError::unavailable(
                candidate
                    .reason
                    .clone()
                    .unwrap_or_else(|| "selected Agent is unavailable".to_owned()),
            ));
        }

        let mut bootstrap_request = self.bootstrap_template.clone();
        match current.readiness {
            SoloBootstrapReadiness::AgentSelectionRequired => {
                bootstrap_request.selected_project_agent_id = Some(identity_id.clone());
                bootstrap_request.selected_worker_agent_id = Some(identity_id.clone());
            }
            SoloBootstrapReadiness::WorkerSelectionRequired => {
                bootstrap_request.selected_project_agent_id =
                    current.selected_project_agent_id.clone();
                bootstrap_request.selected_worker_agent_id = Some(identity_id.clone());
            }
            SoloBootstrapReadiness::CharterAdoptionRequired | SoloBootstrapReadiness::Ready => {
                return Err(BackendError::unavailable(
                    "Agent selection is no longer pending",
                ));
            }
        }

        let result = self
            .bootstrap_service
            .bootstrap(bootstrap_request)
            .await
            .map_err(map_service_error)?;
        if let Ok(mut slot) = self.bootstrap.write() {
            *slot = result;
        }
        let _ = self
            .event_tx
            .send(BackendEvent::invalidate(
                self.scope.clone(),
                0,
                InvalidationReason::Project,
            ))
            .await;
        Ok(BackendCommandResult::AgentSelected {
            agent_id: identity_id,
            replayed: false,
        })
    }

    /// Re-run structured local-harness discovery while setup is pending. This
    /// is called from the normal refresh path so a user can authenticate a
    /// CLI after launch and press the existing retry key without restarting
    /// the process or creating a second Project.
    async fn refresh_agent_discovery(&self) -> BackendResult<()> {
        let _guard = self.discovery_lock.lock().await;
        let current = self.bootstrap_result();
        if !matches!(
            current.readiness,
            SoloBootstrapReadiness::AgentSelectionRequired
                | SoloBootstrapReadiness::WorkerSelectionRequired
        ) {
            return Ok(());
        }

        // The controller refreshes frequently while the TUI is active. Keep
        // the authoritative setup projection live without probing every CLI
        // adapter on every 250ms refresh tick.
        let now = Instant::now();
        let mut last_discovery = self.last_discovery_at.lock().await;
        if last_discovery
            .as_ref()
            .is_some_and(|last| now.saturating_duration_since(*last) < DISCOVERY_REFRESH_INTERVAL)
        {
            return Ok(());
        }
        *last_discovery = Some(now);
        drop(last_discovery);

        let mut candidates = Vec::new();
        for executor in [
            crate::cli::AgentExecutor::Codex,
            crate::cli::AgentExecutor::ClaudeCode,
            crate::cli::AgentExecutor::Cursor,
            crate::cli::AgentExecutor::OpenCode,
            crate::cli::AgentExecutor::Gemini,
            crate::cli::AgentExecutor::Smith,
        ] {
            let executor_type = executor.as_str();
            let kind = executor_type
                .parse::<ExecutorKind>()
                .map_err(BackendError::internal)?;
            let Some(adapter) = self.adapter_registry.get(&kind) else {
                continue;
            };
            if !matches!(
                adapter.check_availability().status,
                AvailabilityStatus::Authenticated
            ) {
                continue;
            }
            let registration = SoloAuthenticatedAgentRegistration::authenticated(
                format!(
                    "forge-solo:{}:{executor_type}",
                    services::embedded_daemon::embedded_machine_id()
                ),
                format!("Forge Solo {executor_type}"),
                executor_type,
                Some(self.daemon_id.clone()),
            );
            let candidate = self
                .bootstrap_service
                .ensure_authenticated_agent(&current.owner_id, registration)
                .await
                .map_err(map_service_error)?;
            candidates.push(SoloAgentCandidateInput {
                identity_id: candidate.identity_id,
                profile_id: candidate.profile_id,
                executor_type: candidate.executor_type,
                availability: candidate.availability,
                display_name: Some(candidate.display_name),
            });
        }

        let mut request = self.bootstrap_template.clone();
        request.agent_candidates = candidates;
        request.selected_project_agent_id = current.selected_project_agent_id.clone();
        request.selected_worker_agent_id = current.selected_worker_agent_id.clone();
        let result = self
            .bootstrap_service
            .bootstrap(request)
            .await
            .map_err(map_service_error)?;
        if result != current {
            if let Ok(mut slot) = self.bootstrap.write() {
                *slot = result;
            }
            let _ = self
                .event_tx
                .send(BackendEvent::invalidate(
                    self.scope.clone(),
                    0,
                    InvalidationReason::Project,
                ))
                .await;
        }
        Ok(())
    }

    async fn shutdown_runtime(&self, deadline: Duration) -> BackendResult<ShutdownOutcome> {
        if let Ok(mut state) = self.runtime_state.write() {
            *state = RuntimeState::ShuttingDown;
        }
        self.daemon.stop();
        let started = Instant::now();

        let daemon_handle = self.daemon_handle.lock().await.take();
        if let Some(mut handle) = daemon_handle {
            let remaining = deadline.saturating_sub(started.elapsed());
            if time::timeout(remaining, &mut handle).await.is_err() {
                handle.abort();
                let _ = handle.await;
            }
        }

        let remaining = deadline.saturating_sub(started.elapsed());
        let supervisor = Arc::clone(&self.supervisor);
        let supervisor_result = time::timeout(remaining, async move {
            let mut supervisor = supervisor.lock().await;
            supervisor.shutdown().await
        })
        .await;

        if let Some(forwarder) = self.event_forwarder.lock().await.take() {
            forwarder.abort();
            let _ = forwarder.await;
        }

        match supervisor_result {
            Ok(Ok(())) => {
                if let Ok(mut state) = self.runtime_state.write() {
                    *state = RuntimeState::Stopped;
                }
                Ok(ShutdownOutcome::completed())
            }
            Ok(Err(error)) => Err(map_service_error(error)),
            Err(_) => {
                if let Ok(mut state) = self.runtime_state.write() {
                    *state = RuntimeState::Stopped;
                }
                Ok(ShutdownOutcome::timed_out(1))
            }
        }
    }
}

impl SoloBackend for RuntimeBackend {
    fn scope(&self) -> SoloScope {
        self.scope.clone()
    }

    fn refresh(&self, _request: SnapshotRequest) -> BackendFuture<'_, BackendResult<SoloSnapshot>> {
        let backend = self;
        Box::pin(async move {
            backend.refresh_agent_discovery().await?;
            let session = Arc::clone(&backend.session);
            let bootstrap = backend.bootstrap_result();
            let runtime_state = backend.runtime_state();
            let snapshot = session.refresh().await.map_err(map_service_error)?;
            Ok(to_backend_snapshot(&snapshot, &bootstrap, runtime_state))
        })
    }

    fn read_activity(
        &self,
        request: ActivityReadRequest,
    ) -> BackendFuture<'_, BackendResult<ActivityBatch>> {
        let session = Arc::clone(&self.session);
        let expected_scope = self.scope.clone();
        Box::pin(async move {
            if request.target.project_id != expected_scope.project_id {
                return Err(BackendError::scope_violation(
                    "activity target is outside the bound Solo Project",
                ));
            }
            let cursor = SoloActivityCursor {
                next_sequence: request.cursor.next_sequence,
                file_size: request.cursor.file_size,
            };
            let limit = request.limit.clamp(1, 100);
            let page = if let Some(turn_id) = request.target.turn_id.as_deref() {
                session
                    .read_turn_activity(turn_id, cursor, limit)
                    .await
                    .map_err(map_service_error)?
            } else if let Some(task_id) = request.target.task_id.as_deref() {
                session
                    .read_execution_activity(
                        task_id,
                        request.target.execution_id.clone(),
                        cursor,
                        limit,
                    )
                    .await
                    .map_err(map_service_error)?
            } else {
                return Err(BackendError::invalid_input(
                    "activity target needs a turn or task identity",
                ));
            };
            Ok(to_activity_batch(request.target, page))
        })
    }

    fn execute(
        &self,
        command: BackendCommand,
    ) -> BackendFuture<'_, BackendResult<BackendCommandResult>> {
        let session = Arc::clone(&self.session);
        Box::pin(async move {
            match command {
                BackendCommand::SelectAgent(request) => self.execute_agent_selection(request).await,
                BackendCommand::SendMessage(request) => {
                    let result = session
                        .send_message(SoloSendMessageInput {
                            content: request.content,
                            dedupe_key: request.idempotency_key.as_str().to_owned(),
                        })
                        .await
                        .map_err(map_service_error)?;
                    Ok(BackendCommandResult::MessageSent {
                        message: to_message_snapshot(&result.message),
                        turn: to_turn_snapshot(&result.turn),
                        replayed: false,
                    })
                }
                BackendCommand::AnswerInteraction(request) => {
                    let result = session
                        .answer_interaction(SoloInteractionAnswerInput {
                            interaction_id: request.interaction_id.clone(),
                            expected_version: request.expected_version,
                            values: request
                                .answers
                                .into_iter()
                                .map(|answer| SoloInteractionAnswerValue::FreeForm {
                                    question_id: answer.field_id,
                                    value: answer.value,
                                })
                                .collect(),
                        })
                        .await
                        .map_err(map_service_error)?;
                    match result {
                        SoloOperationOutcome::Applied(_) => {
                            let snapshot = session.refresh().await.map_err(map_service_error)?;
                            let turn = snapshot
                                .turns
                                .iter()
                                .find(|turn| {
                                    turn.pending_interaction_id.as_deref()
                                        == Some(request.interaction_id.as_str())
                                })
                                .or_else(|| {
                                    snapshot
                                        .turns
                                        .iter()
                                        .find(|turn| turn.status == SoloTurnStatus::AwaitingInput)
                                })
                                .ok_or_else(|| {
                                    BackendError::unavailable(
                                        "the answered interaction no longer has a visible turn",
                                    )
                                })?;
                            Ok(BackendCommandResult::InteractionAnswered {
                                turn: to_turn_snapshot(turn),
                                replayed: false,
                            })
                        }
                        SoloOperationOutcome::NotReady { reason, .. }
                        | SoloOperationOutcome::Unsupported { reason, .. } => {
                            Err(BackendError::unavailable(reason))
                        }
                    }
                }
                BackendCommand::CancelTurn(request) => {
                    let result = session
                        .cancel_turn(
                            request.turn_id,
                            request.expected_version,
                            request.idempotency_key.as_str().to_owned(),
                        )
                        .await
                        .map_err(map_service_error)?;
                    Ok(BackendCommandResult::TurnCancelled {
                        turn: to_turn_snapshot(&result),
                        replayed: false,
                    })
                }
                BackendCommand::RetryTurn(request) => {
                    let result = session
                        .retry_turn(SoloRetryTurnInput {
                            turn_job_id: request.turn_id,
                            expected_version: request.expected_version,
                            idempotency_key: request.idempotency_key.as_str().to_owned(),
                        })
                        .await
                        .map_err(map_service_error)?;
                    Ok(BackendCommandResult::TurnRetried {
                        turn: to_turn_snapshot(&result),
                        replayed: false,
                    })
                }
                BackendCommand::DecideReview(request) => {
                    let decision = match request.decision {
                        backend::ReviewDecision::Accept => SoloReviewDecision::Accept,
                        backend::ReviewDecision::Reject => SoloReviewDecision::RequestChanges,
                    };
                    let result = session
                        .decide_review(SoloReviewDecisionInput {
                            task_id: request.task_id.clone(),
                            expected_task_version: request.expected_version,
                            decision,
                            reason: None,
                        })
                        .await
                        .map_err(map_service_error)?;
                    let snapshot = session.refresh().await.map_err(map_service_error)?;
                    let task = snapshot
                        .tasks
                        .iter()
                        .find(|task| task.id == result.task_id)
                        .ok_or_else(|| BackendError::unavailable("reviewed Task is not visible"))?;
                    Ok(BackendCommandResult::ReviewDecided {
                        task: to_task_snapshot(task),
                        replayed: false,
                    })
                }
                BackendCommand::DecideApproval(request) => {
                    execute_approval(&session, request).await
                }
            }
        })
    }

    fn shutdown(
        &self,
        _intent: ShutdownIntent,
        deadline: Duration,
    ) -> BackendFuture<'_, BackendResult<ShutdownOutcome>> {
        Box::pin(async move { self.shutdown_runtime(deadline).await })
    }
}

async fn execute_approval(
    session: &SoloSessionService,
    request: ApprovalDecisionRequest,
) -> BackendResult<BackendCommandResult> {
    if request.action != ApprovalAction::Approve {
        return Err(BackendError::unavailable(
            "Project Charter adoption does not support rejection from Solo",
        ));
    }
    let current = session.refresh().await.map_err(map_service_error)?;
    let target = current
        .project
        .charter
        .as_ref()
        .and_then(|charter| charter.approval_target.clone())
        .ok_or_else(|| BackendError::unavailable("no pending Project Charter approval"))?;
    validate_approval_target(&request, &target)?;
    let outcome = session
        .approve_charter(SoloCharterApprovalInput {
            target,
            idempotency_key: request.idempotency_key.as_str().to_owned(),
            authorization: SoloCharterApprovalAuthorization {
                authorization_event_id: request.idempotency_key.as_str().to_owned(),
                authorization_basis: "explicit approval in Forge Solo".to_owned(),
                authorization_occurred_at: db::now_rfc3339(),
            },
        })
        .await
        .map_err(map_service_error)?;
    match outcome {
        SoloOperationOutcome::Applied(_) => {
            let snapshot = session.refresh().await.map_err(map_service_error)?;
            let project = to_backend_snapshot(
                &snapshot,
                &synthetic_bootstrap(&snapshot),
                RuntimeState::Ready,
            )
            .project;
            Ok(BackendCommandResult::ApprovalDecided {
                approval_id: request.approval_id,
                project,
                replayed: false,
            })
        }
        SoloOperationOutcome::NotReady { reason, .. }
        | SoloOperationOutcome::Unsupported { reason, .. } => {
            Err(BackendError::unavailable(reason))
        }
    }
}

fn validate_approval_target(
    request: &ApprovalDecisionRequest,
    target: &SoloCharterApprovalTarget,
) -> BackendResult<()> {
    if request.approval_id != target.revision_id {
        return Err(BackendError::conflict(
            "the Project Charter approval target changed",
            None,
        ));
    }
    if request.expected_version != target.expected_project_version {
        return Err(BackendError::conflict(
            "the Project Charter Project version changed",
            None,
        ));
    }
    // The UI confirms the exact rendered representation it displayed. The
    // underlying content digest is intentionally not an interchangeable
    // confirmation token because rendering can change independently.
    if request.target_digest != target.rendered_digest {
        return Err(BackendError::conflict(
            "the Project Charter approval digest changed",
            None,
        ));
    }
    Ok(())
}

fn synthetic_bootstrap(snapshot: &SoloSessionSnapshot) -> SoloBootstrapResult {
    let selected = snapshot
        .project
        .binding
        .as_ref()
        .and_then(|binding| binding.identity_id.clone());
    SoloBootstrapResult {
        idempotency_key: String::new(),
        input_digest: String::new(),
        repository_id: snapshot.scope.repo_id.clone(),
        canonical_repository: String::new(),
        data_root: String::new(),
        owner_id: snapshot.scope.owner_id.clone(),
        project_id: snapshot.scope.project_id.clone(),
        repo_id: snapshot.scope.repo_id.clone(),
        project_chat_id: snapshot.scope.project_chat_id.clone(),
        project_agent_binding_id: String::new(),
        project_agent_identity_id: selected.clone(),
        project_agent_profile_id: snapshot
            .project
            .binding
            .as_ref()
            .and_then(|binding| binding.profile_id.clone()),
        selected_project_agent_id: selected,
        worker_identity_id: None,
        selected_worker_agent_id: None,
        workflow_template_name: "autonomous_v1".to_owned(),
        readiness: SoloBootstrapReadiness::Ready,
        agent_candidates: Vec::new(),
        suggested_project_agent_id: None,
        adoption_required: false,
        mutation_authority_granted: true,
    }
}

fn to_backend_snapshot(
    snapshot: &SoloSessionSnapshot,
    bootstrap: &SoloBootstrapResult,
    runtime_state: RuntimeState,
) -> SoloSnapshot {
    let selected_identity = snapshot
        .project
        .binding
        .as_ref()
        .and_then(|binding| binding.identity_id.as_deref());
    let selected_agent = selected_identity.map(|identity_id| {
        bootstrap
            .agent_candidates
            .iter()
            .find(|candidate| candidate.identity_id == identity_id)
            .map_or_else(
                || AgentSnapshot {
                    id: identity_id.to_owned(),
                    name: identity_id.to_owned(),
                    harness: "unknown".to_owned(),
                },
                candidate_agent_snapshot,
            )
    });
    let setup_agents = bootstrap
        .agent_candidates
        .iter()
        .map(to_setup_agent_snapshot)
        .collect();
    let messages = snapshot.messages.iter().map(to_message_snapshot).collect();
    let turns = snapshot
        .turns
        .iter()
        .map(to_turn_snapshot)
        .collect::<Vec<_>>();
    // Keep current non-terminal turns plus the latest terminal turn in the
    // chat projection. The terminal row is needed to explain a durable
    // failure and carry its exact retry version; retaining only
    // non-terminal rows made a failed turn disappear after its last attempt.
    // A latest successful/cancelled row also fences off an older failure so
    // it cannot become the retry target.
    let active_turns = project_chat_turns(&turns);
    let live_activity = turns
        .last()
        .filter(|turn| turn_is_activity_visible(turn.state))
        .map(|turn| {
            to_live_activity_snapshot(
                snapshot.scope.project_id.as_str(),
                turn,
                selected_agent.clone(),
            )
        })
        .into_iter()
        .collect();
    let interactions = snapshot
        .interactions
        .iter()
        .map(|interaction| InteractionSnapshot {
            id: interaction.id.clone(),
            turn_id: snapshot
                .turns
                .iter()
                .find(|turn| {
                    turn.pending_interaction_id.as_deref() == Some(interaction.id.as_str())
                })
                .map(|turn| turn.id.clone())
                .unwrap_or_default(),
            prompt: interaction.prompt_redacted.clone(),
            fields: Vec::<InteractionField>::new(),
            expected_version: interaction.version,
        })
        .collect();
    let tasks = snapshot.tasks.iter().map(to_task_snapshot).collect();
    let attention = snapshot
        .attention
        .iter()
        .map(to_attention_snapshot)
        .collect();
    let approvals = snapshot
        .project
        .charter
        .as_ref()
        .filter(|charter| has_pending_charter_approval(charter))
        .and_then(|charter| charter.approval_target.as_ref())
        .map(|target| {
            vec![ApprovalSnapshot {
                id: target.revision_id.clone(),
                kind: ApprovalKind::CharterAdoption,
                title: "Project Charter adoption".to_owned(),
                summary: "Approve the proposed Project Charter".to_owned(),
                revision: target.revision_id.clone(),
                digest: target.rendered_digest.clone(),
                operating_skill_revision: Some(
                    target
                        .selected_project_agent_operating_skill_revision
                        .clone(),
                ),
                expected_version: target.expected_project_version,
                selected_agent: selected_agent.clone(),
                permitted_actions: vec![ApprovalAction::Approve],
            }]
        })
        .unwrap_or_default();

    SoloSnapshot {
        scope: SoloScope::new(
            snapshot.scope.owner_id.clone(),
            snapshot.scope.project_id.clone(),
            snapshot.scope.repo_id.clone(),
            snapshot.scope.project_chat_id.clone(),
        ),
        repository: RepositorySnapshot {
            id: snapshot.repository.id.clone(),
            name: snapshot.repository.name.clone(),
            root: snapshot.repository.local_path.clone().unwrap_or_default(),
            default_branch: snapshot.repository.default_branch.clone(),
        },
        project: ProjectSnapshot {
            id: snapshot.project.id.clone(),
            name: snapshot.project.name.clone(),
            readiness: backend_project_readiness(snapshot, bootstrap.readiness),
            runtime: runtime_state,
            workflow: "autonomous_v1".to_owned(),
            selected_agent,
        },
        chat: ChatSnapshot {
            id: snapshot.chat.id.clone(),
            messages,
            has_older_messages: false,
            active_turns,
            interactions,
        },
        setup_agents,
        live_activity,
        tasks,
        attention,
        approvals,
        refreshed_at: snapshot.authoritative_at.clone(),
    }
}

/// A Charter's current draft is actionable only while it differs from the
/// approved revision. Approval finalization intentionally leaves both
/// pointers on the approved revision, so projecting `approval_target` by
/// itself would create a phantom amendment card after a successful adoption.
fn has_pending_charter_approval(charter: &services::solo_session::SoloCharterSnapshot) -> bool {
    charter.approval_target.is_some()
        && charter.current_draft_revision_id.as_deref()
            != charter.current_approved_revision_id.as_deref()
}

fn backend_project_readiness(
    snapshot: &SoloSessionSnapshot,
    bootstrap_readiness: SoloBootstrapReadiness,
) -> ProjectReadiness {
    if matches!(
        bootstrap_readiness,
        SoloBootstrapReadiness::AgentSelectionRequired
            | SoloBootstrapReadiness::WorkerSelectionRequired
    ) {
        return ProjectReadiness::AwaitingAgent;
    }
    match snapshot.project.readiness {
        SoloProjectReadiness::Operational => ProjectReadiness::Operational,
        SoloProjectReadiness::Paused | SoloProjectReadiness::Unknown => {
            ProjectReadiness::RecoveryRequired
        }
        SoloProjectReadiness::SetupRequired => {
            if snapshot
                .project
                .binding
                .as_ref()
                .and_then(|binding| binding.identity_id.as_ref())
                .is_none()
            {
                ProjectReadiness::AwaitingAgent
            } else if snapshot
                .project
                .charter
                .as_ref()
                .and_then(|charter| charter.approval_target.as_ref())
                .is_some()
            {
                ProjectReadiness::AwaitingApproval
            } else {
                ProjectReadiness::AwaitingCharter
            }
        }
    }
}

/// Keep enough turn history to make the latest turn a durable projection
/// fence. Older terminal failures are intentionally omitted once a newer
/// turn exists, while the newest terminal row remains available to the UI.
fn project_chat_turns(turns: &[TurnSnapshot]) -> Vec<TurnSnapshot> {
    let mut projected = turns
        .iter()
        .filter(|turn| !turn_is_terminal(turn.state))
        .cloned()
        .collect::<Vec<_>>();
    if let Some(latest_terminal) = turns.last().filter(|turn| turn_is_terminal(turn.state)) {
        projected.push(latest_terminal.clone());
    }
    projected
}

fn turn_is_terminal(state: TurnState) -> bool {
    matches!(
        state,
        TurnState::Succeeded | TurnState::Failed | TurnState::Cancelled
    )
}

fn turn_is_activity_visible(state: TurnState) -> bool {
    matches!(
        state,
        TurnState::Queued
            | TurnState::Leased
            | TurnState::AwaitingInput
            | TurnState::RetryWait
            | TurnState::Failed
    )
}

fn to_live_activity_snapshot(
    project_id: &str,
    turn: &TurnSnapshot,
    worker: Option<AgentSnapshot>,
) -> LiveActivitySnapshot {
    let attempt = turn.attempt.max(1);
    let failure_summary = matches!(turn.state, TurnState::RetryWait | TurnState::Failed)
        .then(|| {
            turn.failure
                .as_ref()
                .map(|failure| failure.headline.clone())
        })
        .flatten();
    LiveActivitySnapshot {
        target: backend::ActivityTarget {
            project_id: project_id.to_owned(),
            turn_id: Some(turn.id.clone()),
            task_id: None,
            execution_id: turn.id.clone(),
            attempt,
        },
        state: turn.state,
        summary: failure_summary
            .or_else(|| turn.reply.clone())
            .unwrap_or_else(|| format!("Agent Chat turn {}", turn_state_label(turn.state))),
        worker,
        reviewer: None,
        entries: Vec::new(),
        cursor: backend::ActivityCursor {
            execution_id: turn.id.clone(),
            attempt,
            next_sequence: 0,
            file_size: 0,
        },
    }
}

fn candidate_agent_snapshot(candidate: &SoloAgentCandidate) -> AgentSnapshot {
    AgentSnapshot {
        id: candidate.identity_id.clone(),
        name: candidate.display_name.clone(),
        harness: candidate.executor_type.clone(),
    }
}

fn to_setup_agent_snapshot(candidate: &SoloAgentCandidate) -> SetupAgentSnapshot {
    SetupAgentSnapshot {
        id: candidate.identity_id.clone(),
        name: candidate.display_name.clone(),
        harness: candidate.executor_type.clone(),
        available: candidate.eligible,
        authenticated: matches!(candidate.availability, SoloAgentAvailability::Authenticated),
        detail: candidate
            .reason
            .clone()
            .or_else(|| candidate.next_step.clone())
            .unwrap_or_else(|| "authenticated local CLI harness".to_owned()),
    }
}

fn to_message_snapshot(
    message: &services::solo_session::SoloChatMessageSnapshot,
) -> MessageSnapshot {
    MessageSnapshot {
        id: message.id.clone(),
        sequence: message.sequence,
        role: match message.author {
            SoloMessageAuthor::User => MessageRole::User,
            SoloMessageAuthor::Agent => MessageRole::Assistant,
            SoloMessageAuthor::System | SoloMessageAuthor::Handoff => MessageRole::System,
        },
        content: if message.content_redacted {
            String::new()
        } else {
            message.content.clone()
        },
        created_at: message.created_at.clone(),
        turn_id: message.response_turn_id.clone(),
        status: match message.status {
            SoloMessageStatus::Complete => MessageStatus::Complete,
            SoloMessageStatus::Failed | SoloMessageStatus::Cancelled => MessageStatus::Failed,
        },
    }
}

fn to_turn_snapshot(turn: &SoloTurnSnapshot) -> TurnSnapshot {
    let retryable = matches!(
        turn.status,
        SoloTurnStatus::RetryWait | SoloTurnStatus::Failed
    );
    TurnSnapshot {
        id: turn.id.clone(),
        triggering_message_id: turn.triggering_message_id.clone(),
        state: to_turn_state(turn.status),
        version: turn.version,
        attempt: turn.attempt_count.max(0) as u32,
        reply: None,
        assistant_message_id: turn.response_message_id.clone(),
        failure: turn.error_code.as_ref().map(|kind| FailureSnapshot {
            kind: kind.clone(),
            headline: turn
                .error_message
                .clone()
                .unwrap_or_else(|| "Agent Chat turn failed".to_owned()),
            detail: turn.error_message.clone().unwrap_or_default(),
            retryable,
        }),
        retryable,
    }
}

fn to_turn_state(status: SoloTurnStatus) -> TurnState {
    match status {
        SoloTurnStatus::Queued => TurnState::Queued,
        SoloTurnStatus::Leased => TurnState::Leased,
        SoloTurnStatus::AwaitingInput => TurnState::AwaitingInput,
        SoloTurnStatus::RetryWait => TurnState::RetryWait,
        SoloTurnStatus::Succeeded => TurnState::Succeeded,
        SoloTurnStatus::Failed => TurnState::Failed,
        SoloTurnStatus::Cancelled => TurnState::Cancelled,
    }
}

fn turn_state_label(status: TurnState) -> &'static str {
    match status {
        TurnState::Queued => "queued",
        TurnState::Leased => "running",
        TurnState::AwaitingInput => "awaiting input",
        TurnState::RetryWait => "retry wait",
        TurnState::Succeeded => "succeeded",
        TurnState::Failed => "failed",
        TurnState::Cancelled => "cancelled",
    }
}

fn to_task_snapshot(task: &SoloTaskSnapshot) -> TaskSnapshot {
    let worker = task.assignee_id.as_ref().map(|id| AgentSnapshot {
        id: id.clone(),
        name: id.clone(),
        harness: "unknown".to_owned(),
    });
    let reviewer = task
        .roles
        .iter()
        .find(|role| role.role.to_ascii_lowercase().contains("review"))
        .and_then(|role| role.assignee_id.as_ref())
        .map(|id| AgentSnapshot {
            id: id.clone(),
            name: id.clone(),
            harness: "unknown".to_owned(),
        });
    let checks = task
        .latest_review
        .as_ref()
        .map(|review| {
            review
                .checks
                .iter()
                .map(|check| CheckSnapshot {
                    name: check
                        .command
                        .clone()
                        .unwrap_or_else(|| format!("check-{}", check.index)),
                    status: match check.success {
                        Some(true) => CheckStatus::Passed,
                        Some(false) => CheckStatus::Failed,
                        None => CheckStatus::Pending,
                    },
                    summary: None,
                })
                .collect()
        })
        .unwrap_or_default();
    // SoloSessionService returns executions newest first. Review evidence must
    // follow the latest completed attempt after rework, never the oldest SHA
    // still retained in the bounded history.
    let commit = task.executions.iter().find_map(|execution| {
        execution.after_sha.clone().map(|sha| CommitEvidence {
            commit: Some(sha),
            changed_files: Vec::new(),
            merged: false,
            summary: execution.summary.clone(),
        })
    });
    let blocker = task.interruption.as_ref().and_then(|interruption| {
        interruption.reason.clone().map(|detail| FailureSnapshot {
            kind: interruption
                .failure_kind
                .map(|kind| format!("{kind:?}"))
                .unwrap_or_else(|| "blocked".to_owned()),
            headline: "Task blocked".to_owned(),
            detail,
            retryable: !interruption.recovery_actions.is_empty(),
        })
    });
    TaskSnapshot {
        id: task.id.clone(),
        title: task.title.clone(),
        state: task_state(&task.status),
        version: task.version,
        worker,
        reviewer,
        checks,
        commit,
        blocker,
        retryable: task.available_actions.iter().any(|action| {
            matches!(
                action,
                services::solo_session::SoloTaskAction::RequestChanges
            )
        }),
    }
}

fn task_state(status: &str) -> TaskState {
    match status.to_ascii_lowercase().as_str() {
        "backlog" | "ready" | "todo" | "pending" | "queued" => TaskState::Todo,
        "planning" | "working" | "in_progress" | "running" | "claimed" => TaskState::InProgress,
        "blocked" => TaskState::Blocked,
        "review" | "awaiting_review" | "awaiting_human" => TaskState::Review,
        "merging" => TaskState::Merging,
        "cleaning_up" | "cleanup" => TaskState::CleaningUp,
        "done" | "completed" | "succeeded" => TaskState::Done,
        "cancelled" | "canceled" => TaskState::Cancelled,
        "failed" | "merge_failed" => TaskState::Failed,
        _ => TaskState::Failed,
    }
}

fn to_attention_snapshot(attention: &SoloAttentionSnapshot) -> backend::AttentionSnapshot {
    let kind = match attention.attention_type.to_ascii_lowercase().as_str() {
        value if value.contains("approval") => backend::AttentionKind::Approval,
        value if value.contains("check") => backend::AttentionKind::FailedCheck,
        value if value.contains("retry") => backend::AttentionKind::RetryExhausted,
        value if value.contains("agent") => backend::AttentionKind::AgentUnavailable,
        value if value.contains("recover") => backend::AttentionKind::RecoveryRequired,
        value if value.contains("block") => backend::AttentionKind::Blocked,
        _ => backend::AttentionKind::Blocked,
    };
    let permitted_action = match attention.recommended_action.to_ascii_lowercase().as_str() {
        "approve" => Some(backend::AttentionAction::Approve),
        "reject" => Some(backend::AttentionAction::Reject),
        "retry" => Some(backend::AttentionAction::Retry),
        "cancel" => Some(backend::AttentionAction::Cancel),
        "refresh" => Some(backend::AttentionAction::Refresh),
        "recover" => Some(backend::AttentionAction::Recover),
        "review" => Some(backend::AttentionAction::Review),
        _ => None,
    };
    backend::AttentionSnapshot {
        id: attention.id.clone(),
        kind,
        headline: attention.summary.clone(),
        detail: attention.details_json.clone().unwrap_or_default(),
        affected_id: Some(attention.scope_id.clone()),
        permitted_action,
    }
}

fn to_activity_batch(target: backend::ActivityTarget, page: SoloActivityPage) -> ActivityBatch {
    let attempt = target.attempt;
    let entries = page
        .entries
        .into_iter()
        .map(|entry| ActivityEntry {
            sequence: entry.sequence,
            attempt: entry.attempt.unwrap_or(attempt as i64).max(0) as u32,
            kind: match entry.kind {
                SoloActivityKind::AttemptDivider => ActivityKind::ExecutionState,
                SoloActivityKind::ToolCall => ActivityKind::ToolCall,
                SoloActivityKind::ToolResult => ActivityKind::ToolResult,
                SoloActivityKind::AssistantDelta => ActivityKind::AssistantDelta,
                SoloActivityKind::Assistant => ActivityKind::Assistant,
                SoloActivityKind::Thinking => ActivityKind::Other,
            },
            summary: entry
                .summary
                .clone()
                .or(entry.tool_name.clone())
                .unwrap_or_else(|| "activity".to_owned()),
            preview: entry.text.clone(),
        })
        .collect();
    ActivityBatch {
        target: target.clone(),
        entries,
        next_cursor: backend::ActivityCursor {
            execution_id: target.execution_id,
            attempt,
            next_sequence: page.cursor.next_sequence,
            file_size: page.cursor.file_size,
        },
        has_more: page.has_more,
        cursor_reset: page.reset,
        finished: !page.has_more,
    }
}

fn map_service_error(error: ServiceError) -> BackendError {
    match error {
        ServiceError::Db(DbError::VersionConflict)
        | ServiceError::Db(DbError::TaskVersionConflict { .. })
        | ServiceError::Db(DbError::BoardRevisionConflict { .. }) => {
            BackendError::conflict("the durable record changed; refresh and retry", None)
        }
        ServiceError::Conflict(message) => BackendError::conflict(
            safe_public_service_detail(message, "the durable Solo record changed"),
            None,
        ),
        ServiceError::AuthorizationDenied { .. } => {
            BackendError::scope_violation("authorization denied for this Solo scope")
        }
        ServiceError::InvalidOperation { message } => BackendError::invalid_input(
            safe_public_service_detail(message, "the Solo operation was not valid"),
        ),
        ServiceError::NotFound { .. } => {
            BackendError::unavailable("the requested Solo record is unavailable")
        }
        ServiceError::RateLimited { .. } => {
            BackendError::unavailable("the Solo operation is temporarily rate limited")
        }
        ServiceError::TaskActionUnavailable { .. } => {
            BackendError::unavailable("that Task action is not currently available")
        }
        ServiceError::DaemonUnavailable { .. } | ServiceError::DaemonTimeout { .. } => {
            BackendError::unavailable("the local Agent Runtime is unavailable")
        }
        _ => BackendError::protected(backend::BackendErrorKind::Internal),
    }
}

fn truncate_detail(detail: String) -> String {
    detail
        .chars()
        .filter(|character| !character.is_control())
        .take(MAX_SELECTION_DETAIL_CHARS)
        .collect()
}

fn safe_public_service_detail(detail: String, fallback: &str) -> String {
    let detail = truncate_detail(detail);
    let lower = detail.to_ascii_lowercase();
    if detail.trim().is_empty()
        || [
            "credential",
            "password",
            "secret",
            "token",
            "protected",
            "private key",
        ]
        .iter()
        .any(|marker| lower.contains(marker))
    {
        fallback.to_owned()
    } else {
        detail
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use services::solo_session::SoloTaskExecutionSnapshot;

    fn approval_target() -> SoloCharterApprovalTarget {
        SoloCharterApprovalTarget {
            kind: services::solo_session::SoloCharterApprovalKind::Adoption,
            project_id: "project".to_owned(),
            charter_id: "charter".to_owned(),
            revision_id: "revision".to_owned(),
            content_digest: "content-digest".to_owned(),
            rendered_digest: "rendered-digest".to_owned(),
            expected_charter_version: 2,
            expected_project_version: 3,
            approved_project_name: "Project".to_owned(),
            approved_project_slug: Some("project".to_owned()),
            project_mode: "autonomous".to_owned(),
            selected_project_agent_identity_id: "agent".to_owned(),
            selected_project_agent_profile_revision_id: "profile-revision".to_owned(),
            selected_project_agent_operating_skill_revision: "skill-revision".to_owned(),
            selected_project_agent_policy_digest: "policy-digest".to_owned(),
        }
    }

    fn charter_snapshot(
        draft: Option<&str>,
        approved: Option<&str>,
        with_target: bool,
    ) -> services::solo_session::SoloCharterSnapshot {
        services::solo_session::SoloCharterSnapshot {
            id: "charter".to_owned(),
            project_id: Some("project".to_owned()),
            version: 3,
            project_mode: "autonomous".to_owned(),
            maturity: "prototype".to_owned(),
            lifecycle: "attached".to_owned(),
            current_draft_revision_id: draft.map(str::to_owned),
            current_approved_revision_id: approved.map(str::to_owned),
            current_draft: None,
            current_approved: None,
            approval_target: with_target.then(approval_target),
        }
    }

    fn approval_request(target_digest: String) -> ApprovalDecisionRequest {
        ApprovalDecisionRequest {
            approval_id: "revision".to_owned(),
            expected_version: 3,
            target_digest,
            action: ApprovalAction::Approve,
            idempotency_key: "approval-1".into(),
        }
    }

    fn execution(id: &str, after_sha: Option<&str>) -> SoloTaskExecutionSnapshot {
        SoloTaskExecutionSnapshot {
            id: id.to_owned(),
            task_id: "task".to_owned(),
            role: "worker".to_owned(),
            agent_id: Some("agent".to_owned()),
            status: "succeeded".to_owned(),
            stop_reason: None,
            agent_session_id: None,
            before_sha: Some("base".to_owned()),
            after_sha: after_sha.map(str::to_owned),
            summary: Some(format!("{id} summary")),
            error: None,
            logs_available: false,
            execution_version: 1,
            last_activity_at: None,
            created_at: "2026-09-12T00:00:00Z".to_owned(),
            updated_at: "2026-09-12T00:00:00Z".to_owned(),
        }
    }

    fn task(status: &str, executions: Vec<SoloTaskExecutionSnapshot>) -> SoloTaskSnapshot {
        SoloTaskSnapshot {
            id: "task".to_owned(),
            project_id: "project".to_owned(),
            title: "Task".to_owned(),
            status: status.to_owned(),
            version: 3,
            assignee_type: None,
            assignee_id: None,
            priority: 0,
            interruption: None,
            available_actions: Vec::new(),
            roles: Vec::new(),
            executions,
            latest_review: None,
            created_at: "2026-09-12T00:00:00Z".to_owned(),
            updated_at: "2026-09-12T00:00:00Z".to_owned(),
        }
    }

    #[test]
    fn autonomous_workflow_states_project_without_false_failures() {
        for (status, expected) in [
            ("backlog", TaskState::Todo),
            ("ready", TaskState::Todo),
            ("working", TaskState::InProgress),
            ("review", TaskState::Review),
            ("merging", TaskState::Merging),
            ("done", TaskState::Done),
            ("merge_failed", TaskState::Failed),
        ] {
            assert_eq!(task_state(status), expected, "{status}");
        }
    }

    #[test]
    fn review_evidence_uses_the_newest_execution_commit_after_rework() {
        let projected = to_task_snapshot(&task(
            "review",
            vec![
                execution("newest", Some("new-sha")),
                execution("older", Some("old-sha")),
            ],
        ));
        let commit = projected.commit.expect("commit evidence");
        assert_eq!(commit.commit.as_deref(), Some("new-sha"));
        assert_eq!(commit.summary.as_deref(), Some("newest summary"));
    }

    fn turn(id: &str, state: TurnState, version: i64, retryable: bool) -> TurnSnapshot {
        TurnSnapshot {
            id: id.into(),
            triggering_message_id: format!("message-{id}"),
            state,
            version,
            attempt: 2,
            reply: None,
            assistant_message_id: None,
            failure: (state == TurnState::Failed).then(|| FailureSnapshot {
                kind: "executor".into(),
                headline: format!("{id} failed"),
                detail: "durable failure".into(),
                retryable,
            }),
            retryable,
        }
    }

    #[test]
    fn approval_requires_the_exact_rendered_digest() {
        let target = approval_target();
        assert!(validate_approval_target(
            &approval_request(target.rendered_digest.clone()),
            &target
        )
        .is_ok());
        let error =
            validate_approval_target(&approval_request(target.content_digest.clone()), &target)
                .expect_err("content digest must not substitute for rendered digest");
        assert_eq!(error.kind, backend::BackendErrorKind::Conflict);
    }

    #[test]
    fn approved_charter_does_not_project_a_phantom_approval_target() {
        assert!(!has_pending_charter_approval(&charter_snapshot(
            Some("revision"),
            Some("revision"),
            true,
        )));
        assert!(has_pending_charter_approval(&charter_snapshot(
            Some("draft"),
            Some("revision"),
            true,
        )));
        assert!(has_pending_charter_approval(&charter_snapshot(
            Some("draft"),
            None,
            true,
        )));
        assert!(!has_pending_charter_approval(&charter_snapshot(
            None, None, true,
        )));
    }

    #[test]
    fn latest_turn_projection_retains_failure_and_fences_older_retry() {
        let failed = turn("failed", TurnState::Failed, 3, true);
        let projected = project_chat_turns(std::slice::from_ref(&failed));
        assert_eq!(projected, vec![failed.clone()]);
        let activity = to_live_activity_snapshot("project", &failed, None);
        assert_eq!(activity.state, TurnState::Failed);
        assert_eq!(activity.target.turn_id.as_deref(), Some("failed"));

        let succeeded = turn("succeeded", TurnState::Succeeded, 4, false);
        let fenced = project_chat_turns(&[failed, succeeded.clone()]);
        assert_eq!(fenced, vec![succeeded]);
        assert!(fenced
            .last()
            .is_some_and(|turn| turn.state != TurnState::Failed));
    }

    #[test]
    fn live_turn_summary_does_not_reuse_a_stale_failure() {
        let mut leased = turn("leased", TurnState::Leased, 7, false);
        leased.failure = Some(FailureSnapshot {
            kind: "executor".into(),
            headline: "stale previous attempt".into(),
            detail: "must not be shown as live".into(),
            retryable: true,
        });

        let activity = to_live_activity_snapshot("project", &leased, None);
        assert_eq!(activity.summary, "Agent Chat turn running");
    }
}
