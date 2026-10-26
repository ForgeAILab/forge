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
    LegacyConditionInput, MaterialBlocker, ParkReason, ProjectWait, RetryCause, TaskCondition,
    TerminalOutcome, UnknownConditionProblem, COMMENT_KIND_HANDOFF, COMMENT_KIND_REQUEST_INPUT,
    CONDITION_CHECK_PAGE, EVIDENCE_VALUE_LIMIT, LEAD_INPUT_WAIT, LEGACY_BLOCKING_ANNOTATION_KINDS,
    MAPPING_REVISION, MAPPING_REVISION_KEY,
};
pub use task_condition::{
    DiagnosticOwner, RecordTaskDiagnosticText, TaskDiagnosticText, TaskDiagnosticTextPage,
    DIAGNOSTIC_TEXT_LIMIT,
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
pub use sqlite::{
    JournalCleanupBatch, JOURNAL_CLEANUP_BYTES, JOURNAL_CLEANUP_PACKS, JOURNAL_CLEANUP_PAGE,
};
pub use sqlx::{Sqlite, SqlitePool};
pub use task_metadata::TaskMetadata;
pub use time::now_rfc3339;
#[cfg(feature = "test-template")]
pub use time::test_clock;

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

pub use sqlite::{FrozenProjectTurnBindingRow, ProjectTurnBindingAuthorityRow};

pub use sqlite::{
    FrozenTurnOperatingSkillRow, MainTurnGenesisInstructionRow, MainTurnOperatingSkillRow,
    ProjectTurnAdmissionReceiptRow, ProjectTurnApprovalEventRow, ProjectTurnCharterApprovalRow,
    ProjectTurnOperatingSkillRow,
};

pub use sqlite::MainTurnPortfolioProjectRow;

pub use sqlite::{
    ChatTurnProtectedInteractionRow, ClaimFifoChatTurnInTxInput, ExpiredChatTurnLeaseRow,
    FailChatTurnExpiredInteractionInTxInput, FailChatTurnMissingInteractionInTxInput,
    FailChatTurnWithoutInteractionInTxInput, ParkedChatTurnInteractionRow,
    ProjectTurnAdmissionHandoffRow, RecoverExpiredChatTurnLeaseInTxInput,
};

pub use sqlite::GenesisControlTransferReceiptRow;

pub use sqlite::{
    InsertNativeProjectCommandObservationInput, NativeApprovedProjectCharterRow,
    NativeDiscoverySessionRow,
};

pub use sqlite::{NativeOwnedProjectSummaryRow, NativePortfolioProjectRow};

pub use sqlite::NativeProjectWorklogRow;

pub use sqlite::NativeProjectArtifactRow;

pub use sqlite::NativeWorkTaskRow;

pub use sqlite::NativeScopeEventRow;

pub use sqlite::NativeTaskDependencyRow;

pub use sqlite::NativeLatestTaskExecutionRow;

pub use sqlite::{NativeScopeSummaryField, NativeScopeSummaryRow};

pub use sqlite::TopicRotationIntentRow;

pub use sqlite::{ReserveTopicRotationSuccessorSessionInTxInput, TopicRotationScopeWorkspaceRow};

pub use sqlite::TopicRotationHistoryMessageRow;

pub use sqlite::{
    ApplyRepositoryProvisionLocationInTxInput, ClaimRepositoryProvisionAttemptInTxInput,
    InsertRepositoryProvisionLocationInTxInput, RecordRepositoryProvisionFailureInTxInput,
    ReleaseStaleRepositoryProvisionAttemptInput, RepositoryProvisionAttemptFailureInput,
    RepositoryProvisionAttemptIsCurrentInput,
};

pub use sqlite::provisioning_project_settings_on_connection;

pub use sqlite::RepositoryProvisionReadinessIsCurrentInTxInput;

pub use sqlite::RepositoryProvisionTaskWaitInput;

pub use sqlite::{InsertMainGenesisSessionInTxInput, SetGenesisPreferredProjectIdentityInTxInput};

pub use sqlite::{
    GenesisDraftMainChatScopeRow, GenesisStartLeasedMainTurnRow,
    InsertGenesisChatInstructionInTxInput,
};

pub use sqlite::GenesisDraftIdentityOwnerRow;

pub use sqlite::{
    ArtifactDocumentApprovalSourceRow, ValidationAcceptanceCheckSourceRow,
    ValidationCommandObservationSourceRow,
};

pub use sqlite::ValidationProjectCharterSourceRow;

pub use sqlite::{
    ArtifactCommandConflictingInputDigestInput, ArtifactReplayCommandInputDigestInput,
};

pub use sqlite::EvidenceAttachmentSourceRow;

pub use sqlite::{
    ApplyProductGenesisSetupInput, InsertProductGenesisSessionInTxInput,
    ProductGenesisSessionSourceRow, ProductGenesisSourceMessagesRow,
    TransitionProductGenesisSessionInput,
};

pub use sqlite::InsertProductGenesisInstructionInTxInput;

pub use sqlite::{
    usage_index_actual_rowids_on_connection, usage_index_affected_agents_on_connection,
    usage_index_changed_estimates_on_connection, usage_index_changed_invocations_on_connection,
    usage_index_citation_event_ids_on_connection,
    usage_index_effective_event_sources_on_connection, usage_index_watermarks_on_connection,
    UsageIndexActualRowidsRow, UsageIndexAffectedAgentRow,
    UsageIndexAffectedAgentsOnConnectionInput, UsageIndexCitationEventIdsOnConnectionInput,
    UsageIndexEffectiveEventRow, UsageIndexEstimateChangeRow, UsageIndexInvocationChangeRow,
    UsageIndexWatermarksRow,
};

pub use sqlite::{
    usage_index_agent_execution_stats_on_connection, usage_index_changed_executions_on_connection,
    usage_index_execution_domains_on_connection, usage_index_inserted_executions_on_connection,
    UsageIndexAgentExecutionStatsRow, UsageIndexChangedExecutionsOnConnectionInput,
    UsageIndexExecutionDomainRow, UsageIndexExecutionRow,
};

pub use sqlite::usage_index_source_batch_on_connection;

pub use sqlite::{UsageEventSourceMetadataRead, UsageIndexAffectedAgentField};

pub use sqlite::UsageIndexExecutionTextField;

pub use sqlite::ChatUsageProjectScopeRow;

pub use sqlite::ChatUsageHandedOffProjectIdInput;

pub use sqlite::ChatTopicSummaryUsageRow;

pub use sqlite::{
    MilestoneArtifactReferenceSourceRow, MilestoneReleaseReadinessCandidateRow,
    MilestoneReplayReadinessSourceRow,
};

pub use sqlite::{
    DecisionArtifactRenderSourceInput, DecisionArtifactRenderSourceRow,
    DecisionGoverningCharterSourceRow, DecisionGoverningDocumentSourceRow,
    ProjectDecisionCommandSourceRow,
};

pub use sqlite::DirectProjectActorStatusRow;

pub use sqlite::DirectProjectChatScopeRow;

pub use sqlite::SetRepositoryLocationDefaultInTxInput;

pub use sqlite::{RepoLocationPageSort, VisibleRepositoryLocationPageInput};

pub use sqlite::{
    ClearPlacementDispatchRefusalInput, PlacementEligibilityAssignmentRow,
    PlacementEligibilityLocationRow, PlacementEligibilityProvisionRetryRow,
    RecordPlacementDispatchRefusalInput, UpsertPlacementRefusalAttentionInTxInput,
};

pub use sqlite::{
    record_workspace_root_setting, WorkspaceRootEntity, WorkspaceRootPathRemaining,
    WorkspaceRootPathRewrite, WorkspaceRootStoredPath, WorkspaceRootTextPredicate,
    WORKSPACE_ROOT_STORED_PATHS,
};

pub use sqlite::{
    RecordExecutionPlanTaskWaitInTxInput, RecordExecutionPlanTransportRetryInTxInput,
    RememberExecutionPlanSeedInput,
};

pub use sqlite::TaskOutcomeActionOriginSourceRow;

pub use sqlite::ExecutionTerminalAuthorityRow;

pub use sqlite::{LeadSubtaskCommentRow, LeadSubtaskExecutionRow};

pub use sqlite::{TaskProposalChatScopeRow, TaskProposalReceiptDigestInput};

pub use sqlite::{RecordWorkspaceMergeEvidenceInTxInput, WorkspaceMutationOutstandingIntentInput};

pub use sqlite::CharterSelectedAgentEligibilityRow;

pub use sqlite::TaskRoleAssignmentSourceRow;

pub use sqlite::{
    AgentRunnableCliPolicyRow, AgentRunnableMachineSourceRow, AgentRunnableProfileHealthRow,
    EnvironmentMachineCatalogRow, EnvironmentReadinessJsonField,
    ProjectEnvironmentReadinessSourceRow,
};

pub use sqlite::FrozenProjectBindingPolicyRow;

pub use sqlite::{OrchestrationBoundProjectChatScopeRow, OrchestrationMainChatScopeRow};

pub use sqlite::{
    expired_workspace_reservations_in_tx, fail_expired_workspace_reservation_in_tx,
    ExpiredWorkspaceReservationRow, RecordWorkspaceWaitAttentionInTxInput,
    ResolveWorkspaceAdmissionAttentionInTxInput,
};

pub use sqlite::{
    PlacementExecutionDaemonSourceRow, PlacementProvisionRuntimeSourceRow,
    PlacementRepoLocationSourceRow,
};

pub use sqlite::{InsertInitialExplicitTaskRoleInTxInput, TaskCreationParentRoleRow};

pub use sqlite::{
    AgentUsageDomainRunRow, AgentUsageSourceRow, EffectiveUsageEventMetadataRow,
    OperationsUsageDomainRunRow, OperationsUsageSourceRow, TaskUsageDomainRunRow,
    TaskUsageSourceRow, TASK_USAGE_SOURCES_SQL,
};

pub use sqlite::ProjectCreationOperatingSkillRow;

pub use sqlite::{
    execution_admission_account_in_tx, execution_admission_default_account_in_tx,
    execution_admission_project_owner_in_tx, execution_pricing_selection_count_in_tx,
};

pub use sqlite::InsertEmbeddedWorkspaceRepoLocationInTxInput;

pub use sqlite::{
    MemoryBackfillCommentRow, MemoryBackfillExecutionRow, MemoryBackfillReviewRow,
    MemoryBackfillTransitionRow,
};

pub use sqlite::IntegrationTransferOwedReleaseRow;

pub use sqlite::{DeliveryFollowupCheckRow, DeliveryFollowupMilestoneRow};

pub use sqlite::{
    project_agent_workspace_authority_source, ProjectAgentWorkspaceAuthoritySourceRow,
};

pub use sqlite::{ExecutionSetupReconciliationConflictRow, ExecutionSetupTaskEvidenceRow};

pub use sqlite::IntegrationTargetLocationRow;

pub use sqlite::MainActionChatAccountRow;

pub use sqlite::MainCharterReadSourceRow;

pub use sqlite::ProjectHookCooldownRunRow;

pub use sqlite::{project_work_completion_sources_in_tx, ProjectWorkCompletionSourceRow};

pub use sqlite::{ShutdownExecutionSourceRow, ShutdownTaskSourceRow};

pub use sqlite::{WakeBlockerProjectResponderRow, WakeBlockerTurnSourceRow};

pub use sqlite::coordination_root_visible_children_in_connection;

pub use sqlite::TaskRepositoryAuthoritySourceRow;

pub use sqlite::LatestTaskStateEntrySourceRow;

pub use sqlite::{
    ConflictHandoffHistoryRow, ConflictHandoffSourceRow, InsertWorkflowTransitionLogInput,
    WorkflowProjectAuthorityRow, WriteWorkflowTransitionStateInput, CONFLICT_HANDOFF_QUERY,
};

pub use sqlite::LatestWorkflowTransitionSourceRow;

pub use sqlite::RecordReviewCiFailureInTxInput;

pub use sqlite::ClearDaemonUpgradeDispatchRefusalInput;

pub mod review_findings;

pub use sqlite::{
    SessionPersistenceFlags, SESSION_JOURNAL_ORDINARY_STORAGE_ID,
    SESSION_JOURNAL_PROTECTED_STORAGE_ID,
};
