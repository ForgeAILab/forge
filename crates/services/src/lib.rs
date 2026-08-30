#![forbid(unsafe_code)]

pub mod adaptive_task_operations;
pub(crate) mod agent_capacity;
pub mod agent_chat_memory_consumer;
pub mod agent_chat_policy;
pub mod agent_chat_service;
pub mod agent_chat_turn_policy;
pub mod agent_chat_turn_worker;
pub mod agent_service;
pub mod agent_turn_admission;
pub mod attention_service;
pub mod auth_service;
pub mod command_boundary;
pub mod context_manifest;
pub mod coordination_consumer;
pub mod coordination_service;
pub mod daemon_monitor;
pub mod daemon_service;
pub mod daemon_transport;
pub mod default_agents;
pub(crate) mod deferred_dispatch;
pub mod demo;
pub mod diff;
pub mod domain_event_broadcast;
pub mod domain_event_service;
pub mod embedded_agent_service;
pub mod embedded_daemon;
pub mod embedded_task_executor;
pub mod execution_setup;
pub mod external_api;
pub mod external_sync;
pub mod integration_service;
pub mod lifecycle;
pub mod main_chat_topic;
pub mod main_genesis_commands;
pub mod main_orchestration_actions;
pub mod main_orchestration_queries;
pub mod memory;
pub mod memory_source;
pub mod merge_service;
pub mod milestone_orchestration;
pub mod milestone_runtime;
pub mod native_tools;
pub mod notification_service;
pub mod oauth_service;
pub mod operating_skills;
pub mod operator_status;
pub mod operator_status_emitter;
pub mod orchestration_authorization;
pub mod plan_artifact;
pub mod pr_service;
pub mod product_genesis;
pub mod project_agent_actions;
pub mod project_agent_selection;
pub mod project_artifact_commands;
pub mod project_charter_commands;
pub mod project_creation;
pub mod project_decision_commands;
pub mod project_documents;
pub mod project_execution_setup;
pub mod project_execution_setup_projection;
pub mod project_hooks;
pub mod project_member_service;
pub mod project_milestone_commands;
pub mod project_orchestration;
pub mod project_provisioning;
pub mod project_reconciliation;
pub mod project_runtime;
pub mod prompt_preview;
pub mod provider_authorization;
pub mod recovery;
pub mod shared_media_cleanup;
pub mod shutdown;
pub mod task_diagnostics;
pub mod task_dispatcher;
pub mod task_service;
pub mod terminal_service;
pub mod types;
pub mod wake_turn_consumer;
pub mod workflow;
pub mod workspace_cleanup;
pub mod workspace_execution_lock;

// Test-only failpoints used by the Gate A characterization suite.  These are
// compiled out of normal library builds; they let the suite model a process
// stop after a domain transaction commits but before the legacy action receipt
// transaction starts.
#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod receipt_characterization_tests;

pub use adaptive_task_operations::{
    adaptive_task_operation_supported_values, parse_persisted_adaptive_envelope,
    validate_adaptive_task_operations, ADAPTIVE_ALLOWED_TASK_OPERATIONS_FIELD,
};
pub use agent_chat_memory_consumer::{
    memory_consumer_lease_owner, memory_consumer_name, AgentChatMemoryConsumer,
};
pub use agent_chat_policy::{AgentChatOperation, AgentChatPolicyError, AgentChatScope};
pub use agent_chat_service::{
    append_system_chat_message, AdmittedAgentChatMessage, AgentChatHandoffOutcome,
    AgentChatService, CancelAgentChatTurnInput, CommittedAgentChatResponse,
    CreateAgentHandoffInput, RetryAgentChatTurnInput, SendAgentChatMessageInput,
    SetMainAgentBindingInput, SetProjectAgentBindingInput,
};
pub use agent_chat_turn_policy::{
    bounded_error as bounded_agent_chat_error, claim as claim_agent_chat_turn,
    failure as fail_agent_chat_turn, failure_after_claim as fail_agent_chat_turn_after_claim,
    recover_expired as recover_expired_agent_chat_turn,
    FailureDecision as AgentChatFailureDecision, LeaseDecision as AgentChatLeaseDecision,
};
pub use agent_chat_turn_worker::{
    AgentChatTurnRunner, AgentChatTurnWorker, CliAgentChatSessionBackend, CompletedAgentChatTurn,
    FederatedAgentChatTurnRunner,
};
pub use agent_service::AgentService;
pub use agent_turn_admission::{
    admission_digest, content_digest, handoff_admission_digest, handoff_content_digest,
    handoff_content_digest_with_sources, AgentResponderStore, AgentTurnAdmissionInput,
    AgentTurnAdmissionService, AgentTurnAdmitInput, AgentTurnPrepareInput, AgentTurnReadiness,
    AgentTurnTrigger, PreparedAgentTurnAdmission, ResolvedAgentResponder,
};
pub use attention_service::{
    wake_attention_incident_digest, AttentionProjectionRun, AttentionService, WakeAdmissionRequest,
    WakeAdmissionResult, WakeSetupReason, WakeSuppressionReason, MAX_WAKE_REACTION_DEPTH,
};
pub use auth_service::AuthService;
pub use command_boundary::*;
pub use context_manifest::{
    fragment_fingerprint, ContextManifestInput, ContextManifestService, ContextSourceInput,
};
pub use coordination_consumer::{
    coordination_consumer_lease_owner, coordination_consumer_name, CoordinationOutcomeConsumer,
    CoordinationOutcomeRun,
};
pub use coordination_service::{
    AgentActionService, AgentInboxService, ApproveActionInput, AskQuestionInput,
    CommitmentEvidenceInput, CommitmentService, CompleteCommitmentInput, CreateCommitmentInput,
    DeliverInboxInput, ExecuteActionInput, ExecuteTaskProposalInput, ExecutedTaskProposal,
    ProposeActionInput, TaskProposalPayload, TransferCommitmentInput, UpdateCommitmentInput,
};
pub use daemon_monitor::DaemonMonitor;
pub use daemon_service::{
    DaemonRegisterInput, DaemonRegistration, DaemonReportInput, DaemonService,
};
pub use daemon_transport::{
    execution_lease_owner, select_execution_provider, select_filesystem_provider, DaemonConnection,
    DaemonConnectionRegistry, EmbeddedExecutionProvider, EmbeddedFilesystemProvider,
    ExecutionProvider, FilesystemProvider, RemoteExecutionProvider, RemoteFilesystemProvider,
};
pub use default_agents::ensure_default_agents;
pub use deferred_dispatch::wake_task_dispatch;
pub use demo::install_demo_data;
pub use diff::DiffService;
pub use domain_event_broadcast::{
    domain_event_broadcast_consumer_name, domain_event_broadcast_lease_owner,
    DomainEventBroadcastConsumer,
};
pub use domain_event_service::DomainEventService;
pub use embedded_agent_service::{EmbeddedAgentService, ProviderEntryTestOutcome};
pub use embedded_daemon::EmbeddedDaemon;
pub use embedded_task_executor::{EmbeddedTaskExecutor, TaskExecutorRouter};
pub use execution_setup::{
    canonical_task_capability, classify_task_execution, eligible_project_execution_agents,
    ensure_execution_role_principal, is_eligible_execution_identity, is_read_only_capability,
    required_execution_roles, resolve_project_execution_roles,
    resolve_project_execution_roles_for_provisioning, ExecutionRoleResolution,
    RequiredExecutionRoles, TaskExecutionClass, READ_ONLY_CAPABILITY_PROFILES,
    SUPPORTED_CAPABILITY_PROFILES,
};
pub use external_sync::ExternalSyncService;
pub use integration_service::IntegrationService;
pub use main_chat_topic::{MainChatTopicRotation, MainChatTopicService, StartMainChatTopicInput};
pub use main_genesis_commands::{
    MainGenesisCharterDraftRequest, MainGenesisCharterDraftResult, MainGenesisCommandService,
    MainGenesisDraftCommandInput, MainGenesisDraftPrincipal,
    MainGenesisProjectAgentSelectCommandInput, MainGenesisProjectAgentSelectRequest,
    MainGenesisProjectAgentSelectResult, MainGenesisStartCommandInput, MainGenesisStartPrincipal,
    MainGenesisStartRequest, MainGenesisStartResult,
};
pub use main_orchestration_actions::{
    ExecuteMainOrchestrationActionInput, MainOrchestrationActionService,
};
pub use main_orchestration_queries::MainOrchestrationQueryService;
pub use memory::{
    BackfillSummary, BackfillTypeResult, MemoryAccessContext, MemoryCreator, MemoryItemInput,
    MemoryLifecycleInput, MemoryPublicationInput, MemoryReferences, MemorySearchResult,
    MemoryService,
};
pub use memory_source::{
    ForgeMemoryQuery, ForgeMemoryRecord, ForgeMemorySearch, ForgeMemorySource,
    MemorySourceBindingInput,
};
pub use merge_service::{MergeOutcome, MergeService};
pub use milestone_orchestration::{
    evaluate_readiness, milestone_identity, principals_equal, recompute_readiness_digest,
    release_identity, release_snapshot_digest, validate_definition_transition,
    validate_independent_principal, validate_milestone_transition, validate_primary_milestone,
    validate_project_agent_action, validate_release_actor, verify_release_candidate,
    MilestoneOrchestrationError, PrincipalAction, ReadinessDocumentState, ReadinessEvaluation,
    ReadinessEvaluationInput, ReadinessTaskState, ReleaseCandidateVerification,
    MILESTONE_READINESS_DIGEST_SCHEMA_VERSION, MILESTONE_RELEASE_DIGEST_SCHEMA_VERSION,
};
pub use milestone_runtime::MilestoneRuntime;
pub use native_tools::CoordinationToolProvider;
pub use notification_service::NotificationService;
pub use oauth_service::{OAuthError, OAuthService};
pub use operating_skills::{
    canonical_main_baseline_operating_skill_body, canonical_main_operating_skill_body,
    canonical_project_operating_skill_body, main_operating_skill_active,
    render_main_baseline_operating_skill, render_main_operating_skill,
    render_project_operating_skill, EffectiveProjectStateContext, MainBaselineSkillContext,
    MainOperatingSkillContext, ProjectOperatingSkillContext,
    MAIN_BASELINE_OPERATING_SKILL_CONTENT_DIGEST, MAIN_BASELINE_OPERATING_SKILL_KEY,
    MAIN_BASELINE_OPERATING_SKILL_REVISION, MAIN_OPERATING_SKILL_CONTENT_DIGEST,
    MAIN_OPERATING_SKILL_KEY, MAIN_OPERATING_SKILL_POLICY_DIGEST, MAIN_OPERATING_SKILL_POLICY_JSON,
    MAIN_OPERATING_SKILL_RENDER_VERSION, MAIN_OPERATING_SKILL_SCHEMA_VERSION,
    MAIN_OPERATING_SKILL_VERSION, PROJECT_OPERATING_SKILL_CONTENT_DIGEST,
    PROJECT_OPERATING_SKILL_KEY, PROJECT_OPERATING_SKILL_POLICY_DIGEST,
    PROJECT_OPERATING_SKILL_POLICY_JSON, PROJECT_OPERATING_SKILL_RENDER_VERSION,
    PROJECT_OPERATING_SKILL_SCHEMA_VERSION, PROJECT_OPERATING_SKILL_VERSION,
};
pub use operator_status::OperatorStatusService;
pub use operator_status_emitter::OperatorStatusEmitter;
pub use orchestration_authorization::OrchestrationAuthorizationService;
pub use product_genesis::{
    charter_approval_chat_message, charter_proposal_chat_message, render_product_genesis_prompt,
    validate_genesis_transition, GenesisLifecycleError, GenesisPromptContext,
    NewProductGenesisSession, ProductGenesisService, ProductGenesisStart, ProductGenesisStore,
    SqliteProductGenesisStore, TransitionProductGenesis, PRODUCT_GENESIS_PROMPT_VERSION,
};
pub use project_agent_actions::{
    DirectProjectCommandResult, ExecuteDirectProjectCommandInput,
    ExecuteProjectOrchestrationActionInput, ProjectOrchestrationActionService,
};
pub use project_agent_selection::{
    current_project_agent_operating_skill_revision, list_genesis_project_agents,
    project_agent_policy_digest, resolve_genesis_project_agent,
    resolve_requested_genesis_project_agent, resolve_requested_genesis_project_agent_for_account,
    GenesisAgentSelection,
};
pub use project_artifact_commands::{
    ProjectArtifactCommandService, ProjectCommandAuthorization, ProjectDocumentApprovalCommand,
    ProjectDocumentCreateCommand, ProjectDocumentRevisionCommand, ProjectEvidenceCommand,
    ProjectValidationCommand, PROJECT_EVIDENCE_COMMAND,
};
pub use project_charter_commands::{
    ProjectCharterApprovalCommand, ProjectCharterApprovalCommandOutcome,
    ProjectCharterCommandService, ProjectCharterRevisionCommand,
    ProjectCharterRevisionCommandOutcome, PROJECT_CHARTER_APPROVAL_COMMAND,
};
pub use project_creation::{
    create_project_from_charter_approval, CreateProjectAuthorization,
    CreateProjectFromCharterApprovalInput,
};
pub use project_decision_commands::{
    ProjectDecisionApprovalCommand, ProjectDecisionCandidateCommand, ProjectDecisionCommandService,
    ProjectDecisionEffectiveCommand, ProjectDecisionRejectionCommand,
    PROJECT_DECISION_CANDIDATE_APPROVE_COMMAND, PROJECT_DECISION_CANDIDATE_CREATE_COMMAND,
    PROJECT_DECISION_CANDIDATE_REJECT_COMMAND, PROJECT_DECISION_EFFECTIVE_COMMAND,
};
pub use project_documents::{
    diff_project_document_views, document_content_digest, document_kind_name,
    document_render_digest, parse_document_kind, parse_document_revision_lifecycle,
    render_project_document, render_project_document_json, PROJECT_DOCUMENT_RENDER_VERSION,
    PROJECT_DOCUMENT_SCHEMA_VERSION,
};
pub use project_execution_setup::{ExecutionPrincipalRole, ProjectExecutionSetupService};
pub use project_execution_setup_projection::{
    load_project_execution_setup, load_task_execution_blocker,
};
pub use project_hooks::ProjectHookService;
pub use project_member_service::ProjectMemberService;
pub use project_milestone_commands::{
    ProjectMilestoneCommandService, ProjectMilestoneDefinitionCommand,
    ProjectPrimaryMilestoneCommand, ProjectReadinessRequestCommand, ProjectReleaseRequestCommand,
    PROJECT_MILESTONE_COMMAND, PROJECT_READINESS_COMMAND, PROJECT_RELEASE_REQUEST_COMMAND,
};
pub use project_orchestration::{
    charter_change_summary, charter_content_digest, charter_render_digest, compute_charter_digests,
    diff_project_charter_content, evaluate_charter_readiness, evaluate_project_charter_readiness,
    render_and_digest_charter, render_charter, render_charter_markdown, render_project_charter,
    semantic_revision_diff, semantic_revision_diff_between, try_charter_content_digest,
    try_charter_render_digest, validate_approval_candidate, validate_charter_approval_candidate,
    CharterApprovalValidationError, CharterFieldChange, CharterRender, CharterRevisionDiff,
    CHARTER_DIFF_VERSION, CHARTER_READINESS_POLICY_VERSION, PROJECT_CHARTER_RENDER_VERSION,
};
pub use project_reconciliation::{
    ProjectReconciliationPage, ProjectReconciliationService,
    RESOLVE_PROJECT_RECONCILIATION_OPERATION,
};
pub use project_runtime::{
    load_effective_project_state, ProjectCommitmentProjection, ProjectCurrentStateResponse,
    ProjectEffectiveStateProjection, ProjectInboxProjection,
};
pub use prompt_preview::preview_effective_prompt;
pub use provider_authorization::ProviderAuthorizationService;
pub use recovery::{CrashRecovery, HeartbeatMonitor};
pub use shared_media_cleanup::SharedMediaCleanupScheduler;
pub use shutdown::GracefulShutdown;
pub use task_dispatcher::TaskDispatcher;
pub use task_service::{
    AdaptiveTaskCommand, AdaptiveTaskCommandResult, DirectTaskProposalInput, NewSubtaskInput,
    TaskProposalCommandResult, TaskService,
};
pub use terminal_service::{TerminalActivityTracker, TerminalService};
pub use types::Assignee;
pub use wake_turn_consumer::{
    wake_turn_consumer_lease_owner, wake_turn_consumer_name, WakeTurnConsumer, WakeTurnRun,
};
pub use workflow::template_service::WorkflowTemplateService;
pub use workspace_cleanup::WorkspaceCleanupScheduler;
pub use workspace_execution_lock::WorkspaceExecutionLockManager;

pub type Result<T> = std::result::Result<T, ServiceError>;

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("dependency gate")]
    DependencyGate,

    #[error("execution setup required: {message}")]
    ExecutionSetupRequired {
        message: String,
        requirements: Vec<api_types::SetupRequirement>,
    },

    #[error(transparent)]
    Db(db::DbError),

    #[error(transparent)]
    Git(git::GitError),

    #[error(transparent)]
    Review(review::ReviewError),

    #[error("{entity} not found: {id}")]
    NotFound { entity: &'static str, id: String },

    #[error("invalid operation: {message}")]
    InvalidOperation { message: String },

    #[error("authorization denied: {message}")]
    AuthorizationDenied { message: String },

    #[error("rate limited; retry after {retry_after_seconds} seconds")]
    RateLimited { retry_after_seconds: u64 },

    #[error("task action unavailable: {reason}")]
    TaskActionUnavailable {
        available_actions: Vec<api_types::TaskAction>,
        reason: String,
    },

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("Product Genesis session already active: {session_id}")]
    ProductGenesisActiveSession { session_id: String },

    #[error("daemon unavailable: {daemon_id}")]
    DaemonUnavailable { daemon_id: String },

    #[error("daemon command timed out for daemon {daemon_id}: {method}")]
    DaemonTimeout { daemon_id: String, method: String },

    #[error("{0}")]
    Domain(String),

    #[error("project {project_id} has no primary repo")]
    MissingPrimaryRepo { project_id: String },

    #[error("repo does not match primary repo for project {project_id}")]
    RepoMismatch { project_id: String },

    #[error("PR provider missing for repo {repo_id}")]
    PrProviderMissing { repo_id: String },

    #[error("PR provider token missing for repo {repo_id}")]
    PrProviderTokenMissing { repo_id: String },

    #[error("PR sync failure for task {task_id}: {details}")]
    PrSyncFailure { task_id: String, details: String },

    #[error("agent {agent_id} is paused and cannot accept new work")]
    AgentPaused { agent_id: String },

    #[error("project {project_id} is paused")]
    ProjectPaused { project_id: String },

    #[error("guard rejected: {guard}: {reason}")]
    GuardRejection { guard: String, reason: String },

    #[error("nested subtasks are unsupported")]
    NestedSubtaskUnsupported,

    #[error("subtask assignee unsupported: root coder {root_coder_id:?}, attempted {attempted}")]
    SubtaskAssigneeUnsupported {
        root_coder_id: Option<String>,
        attempted: String,
    },

    #[error("subtask sequence already started for task {task_id}")]
    SubtaskSequenceStarted { task_id: String },

    #[error("subtask {task_id} is managed by root {root_task_id}")]
    SubtaskManagedByRoot {
        task_id: String,
        root_task_id: String,
    },

    #[error("parent workspace required for task {parent_task_id}")]
    ParentWorkspaceRequired { parent_task_id: String },

    #[error("workspace reset required for task {task_id}: {reason}")]
    WorkspaceResetRequired { task_id: String, reason: String },

    #[error("task sequence already started for task {task_id}")]
    TaskSequenceAlreadyStarted { task_id: String },

    #[error("terminal access is disabled")]
    TerminalDisabled,

    #[error("terminal workspace is not ready")]
    TerminalWorkspaceNotReady,

    #[error("terminal session limit reached for {scope}")]
    TerminalSessionLimit { scope: String },

    #[error("terminal daemon unavailable: {daemon_id}")]
    TerminalDaemonUnavailable { daemon_id: String },

    #[error("terminal blocked by active execution in workspace {workspace_id}")]
    TerminalActiveExecution { workspace_id: String },

    #[error("terminal attach token is invalid")]
    TerminalAttachTokenInvalid,

    #[error("terminal path guardrail rejected the workspace path")]
    TerminalPathGuardrail,

    #[error("terminal session not found")]
    TerminalNotFound,

    #[error("invalid terminal input: {message}")]
    TerminalInvalidInput { message: String },
}

impl From<db::DbError> for ServiceError {
    fn from(error: db::DbError) -> Self {
        match error {
            db::DbError::DependencyGate => Self::DependencyGate,
            error => Self::Db(error),
        }
    }
}

impl From<sqlx::Error> for ServiceError {
    fn from(error: sqlx::Error) -> Self {
        Self::Db(error.into())
    }
}

impl From<git::GitError> for ServiceError {
    fn from(error: git::GitError) -> Self {
        Self::Git(error)
    }
}

impl From<review::ReviewError> for ServiceError {
    fn from(error: review::ReviewError) -> Self {
        Self::Review(error)
    }
}

impl From<executors::ExecutorError> for ServiceError {
    fn from(error: executors::ExecutorError) -> Self {
        Self::InvalidOperation {
            message: error.to_string(),
        }
    }
}

impl ServiceError {
    pub(crate) fn not_found(entity: &'static str, id: impl Into<String>) -> Self {
        Self::NotFound {
            entity,
            id: id.into(),
        }
    }

    pub(crate) fn invalid_operation(message: impl Into<String>) -> Self {
        Self::InvalidOperation {
            message: message.into(),
        }
    }

    pub(crate) fn execution_setup_required(
        message: impl Into<String>,
        requirements: Vec<api_types::SetupRequirement>,
    ) -> Self {
        Self::ExecutionSetupRequired {
            message: message.into(),
            requirements,
        }
    }

    pub(crate) fn terminal_invalid_input(message: impl Into<String>) -> Self {
        Self::TerminalInvalidInput {
            message: message.into(),
        }
    }

    pub(crate) fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict(message.into())
    }

    pub fn nested_subtask_unsupported() -> Self {
        Self::NestedSubtaskUnsupported
    }

    pub fn subtask_assignee_unsupported(root_coder_id: Option<String>, attempted: String) -> Self {
        Self::SubtaskAssigneeUnsupported {
            root_coder_id,
            attempted,
        }
    }

    pub fn subtask_sequence_started(task_id: impl Into<String>) -> Self {
        Self::SubtaskSequenceStarted {
            task_id: task_id.into(),
        }
    }

    pub fn subtask_managed_by_root(
        task_id: impl Into<String>,
        root_task_id: impl Into<String>,
    ) -> Self {
        Self::SubtaskManagedByRoot {
            task_id: task_id.into(),
            root_task_id: root_task_id.into(),
        }
    }

    pub fn parent_workspace_required(parent_task_id: impl Into<String>) -> Self {
        Self::ParentWorkspaceRequired {
            parent_task_id: parent_task_id.into(),
        }
    }

    pub fn task_sequence_already_started(task_id: impl Into<String>) -> Self {
        Self::TaskSequenceAlreadyStarted {
            task_id: task_id.into(),
        }
    }
}
