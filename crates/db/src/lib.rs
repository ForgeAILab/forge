#![forbid(unsafe_code)]

mod agent_chat_topic_repository;
pub mod budget;
mod chat_session_denials;
mod connection;
mod effective_authority;
pub use effective_authority::TaskRecordExecution;
mod environment_readiness;
mod error;
mod ids;
pub mod inherited_subtask;
pub mod integration_queue;
pub use integration_queue::*;
pub mod machine_capacity;
pub mod machine_disk;
mod migration;
mod models;
mod orchestration;
mod pagination;
mod repository;
mod review_conformance;
mod sqlite;
pub mod task_condition;
mod task_metadata;
pub use task_condition::{
    capacity_reason, legacy_annotation_blocks, map_legacy_condition, material_blocker,
    ConditionCapacityScope, ConditionChange, ConditionCheckPass, ConditionCheckState,
    ConditionCheckStatus, ConditionContinuation, ConditionEnvironmentKind, ConditionEvidence,
    ConditionFacts, ConditionRead, ConditionRefusal, ConditionRetry, ConditionSource,
    ConditionStatement, ConditionWitness, HumanBoundary, LegacyConditionField,
    LegacyConditionInput, MaterialBlocker, ParkReason, RetryCause, TaskCondition, TerminalOutcome,
    UnknownConditionProblem, COMMENT_KIND_HANDOFF, COMMENT_KIND_REQUEST_INPUT,
    CONDITION_CHECK_PAGE, EVIDENCE_VALUE_LIMIT, LEAD_INPUT_WAIT, LEGACY_BLOCKING_ANNOTATION_KINDS,
    MAPPING_REVISION, MAPPING_REVISION_KEY,
};
mod task_mutation;
mod task_step;
pub mod task_writer;
pub use task_mutation::*;
mod remote_cancel;
pub use remote_cancel::*;
pub use task_step::*;
#[cfg(test)]
mod tests;
mod time;
mod worker;
pub use worker::{
    clamp_worker_deferral, FailureState, HealthErrorKind, PoisonDecision, RetryPolicy, WorkItem,
    WorkerHealth, WorkerWaitState,
};

pub use agent_chat_topic_repository::*;
pub use chat_session_denials::*;
pub use connection::{
    begin_immediate, close_sqlite_pool, convert_sqlite_to_incremental, create_sqlite_pool,
    incremental_vacuum, open_write_transactions, sqlite_storage_status,
};
pub use environment_readiness::*;
pub use error::{DbError, Result};
pub use ids::{new_uuid_v4, validate_uuid_v4};
pub use migration::{run_migrations, run_migrations_from};
pub use models::*;
pub use orchestration::*;
pub use pagination::*;
pub use repository::*;
pub use review_conformance::*;

pub use sqlite::{
    integration_statement_in_tx, supported_main_baseline_revision, ActiveCheckWaiverSourceRow,
    ActiveExecutionOperatorRow, AgentOperatorPressureRow, AttemptCarryBase, BlockedTaskOperatorRow,
    CharterReadinessSourceRow, DaemonOperatorIssueRow, DeadLetter, DeadLetterAction,
    DeadLetterPage, EventSubscription, InsertMilestoneReadinessInput,
    InsertMilestoneReadinessSourceInput, InsertMilestoneReleaseInput,
    InsertReleaseDecisionReferenceInput, InsertReleaseDocumentReferenceInput,
    InsertReleaseEvidencePinInput, InsertReleaseGitReferenceInput,
    InsertReleaseReviewReferenceInput, InsertReleaseTaskReferenceInput,
    InsertReleaseValidationReferenceInput, MilestoneCharterAuthorityRow, MilestoneCheckResultRow,
    MilestoneDecisionRow, MilestoneDefinitionReadinessSourceRow, MilestoneDocumentSourceRow,
    MilestoneEvidenceRow, MilestoneReadinessSourceRow, MilestoneReleasePinMetadataRow,
    MilestoneRepositoryContextRow, MilestoneTaskSourceRow, MilestoneWaiverAffectedRow,
    OverdueWorkspaceCleanupRow, ProjectAgentIdentityRow, RecentFailedExecutionOperatorRow,
    RecordWorkspaceCleanupFailureInput, ReleaseCharterRevisionSourceRow, ReleaseEvidencePinRow,
    ReleaseReferenceRow, ScheduleRead, SqliteDb, TaskListRead, TaskOperatorConditionRow,
    TaskOperatorRetryRow, UpdateMilestoneReadinessProjectionInput, WorkerDeadLetterIssue,
    WorkerDiagnostic, WorkspaceCleanupAttentionRow, INTEGRATION_OWNED_SQL,
};
pub use sqlx::{Sqlite, SqlitePool};
pub use task_metadata::TaskMetadata;
pub use time::now_rfc3339;

pub use models::{
    CreateOAuthAuthorizationCode, CreateOAuthClient, CreateOAuthRefreshToken,
    CreatePersonalAccessToken, CreateProjectIntegration, CreateProjectMember,
    CreateTaskExternalLink, CreateTaskRoleAssignment, CreateTerminalSession, CreateTransitionLog,
    IntegrationPlatform, OAuthAuthorizationCode, OAuthClient, OAuthRefreshToken,
    PersonalAccessToken, ProjectIntegration, ProjectMember, RefreshToken, TaskExternalLink,
    TaskRoleAssignment, TerminalSession, TerminalSessionStatus, TransitionLog,
    UpdateProjectIntegration, UpdateTerminalSessionStatus, User,
};
pub use repository::{
    CiStepStats, ExternalLinkRepo, IntegrationRepo, OAuthAuthorizationCodeRepo, OAuthClientRepo,
    OAuthRefreshTokenRepo, PersonalAccessTokenRepo, ProjectAnalyticsRepo, ProjectMemberRepo,
    ProjectReviewSummary, RefreshTokenRepo, SystemSettingRepo, TaskRoleAssignmentRepo,
    TerminalSessionRepo, TransitionLogRepo, UserRepo,
};

pub mod check_run;
pub use check_run::*;

pub use sqlite::{
    ActiveIncidentWakeLeaseInTxInput, ActiveIncidentWakeLeaseRow, AdmitIncidentWakeLeaseInTxInput,
    AnnotateOwnerRecoveryInTxInput, AnnotateRemovedMachineTaskInput, AttentionAgentBindingRow,
    AttentionAgentSessionRow, AttentionApprovalCoordinationActionsRow, AttentionChatScopeRow,
    AttentionCoordinationScopesRow, AttentionDirectCoordinationReceiptsRow,
    AttentionIdentityOwnerRow, AttentionMemoryScopeRow, AttentionTaskContextRow,
    AttentionVisibleActiveExecutionCountRow, AttentionVisibleAgentHealthRow,
    AttentionVisibleQueuedTaskCountRow, AttentionVisibleRecentOutcomesRow,
    AttentionVisibleWorkItemsRow, AttentionWorkerHealthRow,
    ClearRemovedMachineCancelAnnotationInput, CloneVerificationLocationRow, FailedTurnIncidentRow,
    IncrementWakeBudgetWindowInTxInput, InsertCreatingAdmissionWorkspaceInTxInput,
    InsertPreparedServerRepoLocationInTxInput, InsertPreparedServerWorkspaceInTxInput,
    MarkPreparedAdmissionWorkspaceReadyInTxInput, PreparedServerRepoLocationRow,
    ReadyServerLocationRow, RecordLatestAttentionWakeInTxInput, RecordPlacementRefusalInTxInput,
    RecordedOwnerWorkspaceHeadInput, ResolveScopeProgressAttentionInTxInput,
    StartWakeBudgetWindowInTxInput, VisibleAttentionAgentBindingsInput, WakeBudgetWindowRow,
    WakeChatCanonicalScopeRow, WakeIncidentContextRow, WakeSweepCandidateRow,
};

pub use sqlite::{AgentStatusHealthRow, CliRuntimePolicyStatusRow, DaemonExecutorStatusRow};

pub use sqlite::{
    ActionApproverChatScopeRow, ActionApproverIdentityRow, ActionApproverProjectBindingRow,
    ActionOrchestrationCharterStateRow, TaskActionActorPolicyRow, TaskActionScopeAssignmentRow,
    TaskActionScopeRoleRow,
};

pub use sqlite::{
    EffectiveProjectApprovedDocumentSourceRow, EffectiveProjectCanonicalConflictSourceRow,
    EffectiveProjectCommitmentSourceRow, EffectiveProjectDecisionSourceRow,
    EffectiveProjectEventWatermarkRow, EffectiveProjectGoverningCharterSourceRow,
    EffectiveProjectIdentitySourceRow, EffectiveProjectInboxSourceRow,
    EffectiveProjectMilestoneSourceRow, EffectiveProjectReadinessSourceRow,
    EffectiveProjectReconciliationSourceRow, EffectiveProjectReleaseSourceRow,
    EffectiveProjectTaskStatusCountRow, EffectiveProjectValidationOutcomeCountRow,
    ProjectAdoptionCharterSourceRow,
};

pub use sqlite::{
    EscalationBlockerTurnSourceRow, InsertProjectEscalationInTxInput, ProjectEscalationSourceRow,
};

pub use sqlite::{
    blocker_recovery_transition_exists_in_tx, escalation_project_member_exists_on_connection,
    escalation_project_owner_on_connection,
};

pub use sqlite::{EnvironmentProvisionRetryDetailRow, UpsertEnvironmentWaitAttentionInTxInput};

pub use sqlite::{
    AdaptiveTaskGovernanceSourceRow, InsertIssuedWorkspaceLeaseInTxInput,
    TaskExecutionCharterGovernanceSourceRow, TaskGovernanceDocumentRevisionSourceRow,
    WorkspaceLeaseCharterGateRow, WorkspaceLeaseRoleAssignmentRow, WorkspaceLeaseTaskAssignmentRow,
};

pub use sqlite::task_governance_document_revision_source_on_pool;
