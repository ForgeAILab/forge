//! Durable, typed Task persistence operations. Only a claimed step applies them.
use crate::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskMutation {
    ReviewCreateAttemptWithExecution {
        review: Box<CreateReview>,
        execution: Box<CreateExecution>,
        lease: ClaimExecutionLease,
        admission: Option<ExecutionAdmission>,
    },
    ReviewCreateManualPass {
        input: CreateManualReviewPass,
    },
    ReviewUpdateStatusWithTaskAuthority {
        id: String,
        status: ReviewStatus,
        step_results_json: String,
        finished_at: Option<String>,
        updated_at: String,
        expected_task_version: i64,
        review_passed_at: Option<String>,
        origin: ReviewEventOrigin,
    },
    ReviewUpdateStatusWithTaskAuthorityAndCandidate {
        id: String,
        status: ReviewStatus,
        step_results_json: String,
        finished_at: Option<String>,
        updated_at: String,
        expected_task_version: i64,
        review_passed_at: Option<String>,
        expected_candidate_execution_id: String,
    },
    ReviewUpdateStatusWithTaskAuthorityAndProjectCandidate {
        id: String,
        status: ReviewStatus,
        step_results_json: String,
        finished_at: Option<String>,
        updated_at: String,
        expected_task_version: i64,
        review_passed_at: Option<String>,
        expected_project_version: Option<i64>,
        expected_workflow_definition: Option<String>,
        expected_review_status: ReviewStatus,
        expected_review_updated_at: String,
        expected_candidate_execution_id: String,
    },
    ReviewUpdateStatusWithReviewAuthority {
        id: String,
        status: ReviewStatus,
        step_results_json: String,
        finished_at: Option<String>,
        updated_at: String,
        expected_task_version: i64,
        expected_task_status: String,
        expected_project_version: Option<i64>,
        expected_workflow_definition: Option<String>,
        expected_review_status: ReviewStatus,
        expected_review_updated_at: String,
        expected_candidate_execution_id: Option<String>,
    },
    ReviewUpdateStatusWithReviewAuthorityAndTaskProjection {
        id: String,
        status: ReviewStatus,
        step_results_json: String,
        finished_at: Option<String>,
        updated_at: String,
        expected_task_version: i64,
        expected_task_status: String,
        expected_project_version: Option<i64>,
        expected_workflow_definition: Option<String>,
        expected_review_status: ReviewStatus,
        expected_review_updated_at: String,
        expected_candidate_execution_id: String,
        #[serde(with = "crate::task_writer::nested_option")]
        task_projection: Option<Option<String>>,
    },
    ExecutionCreate {
        input: CreateExecution,
    },
    ExecutionCreateWithLeaseAndAdmission {
        input: Box<CreateExecution>,
        lease: ClaimExecutionLease,
        admission: Option<ExecutionAdmission>,
    },
    Sql {
        task_id: String,
        query: String,
        arguments: Vec<serde_json::Value>,
    },
    TaskUpdate {
        input: UpdateTask,
    },
    TaskUpdateIfAnnotation {
        input: UpdateTask,
        expected_annotation: Option<String>,
        expected_project_version: Option<i64>,
        expected_workflow_definition: Option<String>,
    },
    TaskUpdateWithRecoveryMarker {
        input: UpdateTask,
        marker: CreateTransitionLog,
    },
    TaskUpdateWithWorkflowAuthority {
        input: UpdateTask,
        expected_project_version: i64,
        expected_workflow_definition: String,
    },
    TaskUpdateWithWorkflowAuthorityAndRecoveryMarker {
        input: UpdateTask,
        marker: CreateTransitionLog,
        expected_project_version: i64,
        expected_workflow_definition: String,
    },
    TaskSetErrorAnnotationIfNoRunningExecution {
        id: String,
        expected_version: i64,
        expected_status: String,
        expected_state_entry_token: Option<String>,
        expected_workflow_definition: String,
        expected_assignment_role: Option<String>,
        expected_assignment: Option<TaskRoleAssignment>,
        annotation: String,
        updated_at: String,
        stopped_execution_id: String,
        workspace_id: Option<String>,
        overlapping_roles: Vec<String>,
    },
    TaskUpdateRecoveryMetadataIfNoRunningExecution {
        id: String,
        expected_version: i64,
        error_annotation: Option<String>,
        blocked_json: Option<String>,
        failed_json: Option<String>,
        updated_at: String,
        workspace_id: Option<String>,
        overlapping_roles: Vec<String>,
        metadata_mutations: Vec<TaskMetadataMutation>,
        /// What the writer states its write means. Absent on a queued
        /// mutation written before writers stated conditions.
        #[serde(default)]
        condition: Option<ConditionStatement>,
    },
    TaskRestoreQueuedRecovery {
        input: RestoreQueuedRecovery,
    },
    TaskSetReviewPassedAt {
        id: String,
        review_passed_at: Option<String>,
        updated_at: String,
    },
    TaskSetReviewPassedAtCas {
        id: String,
        expected_version: i64,
        review_passed_at: Option<String>,
        updated_at: String,
    },
    TaskSetReviewPassedAtCasForReview {
        id: String,
        expected_version: i64,
        review_passed_at: Option<String>,
        expected_review_updated_at: String,
        updated_at: String,
    },
    TaskMutateMetadata {
        id: String,
        expected_version: Option<i64>,
        mutations: Vec<TaskMetadataMutation>,
        updated_at: String,
    },
    TaskMutateMetadataWithChange {
        id: String,
        expected_version: Option<i64>,
        mutations: Vec<TaskMetadataMutation>,
        updated_at: String,
    },
    TaskMutateMetadataAndBumpVersion {
        id: String,
        expected_version: i64,
        mutations: Vec<TaskMetadataMutation>,
        updated_at: String,
    },
    TaskMutateMetadataAndBumpVersionWithProjectAuthority {
        id: String,
        expected_version: i64,
        expected_project_version: i64,
        mutations: Vec<TaskMetadataMutation>,
        updated_at: String,
    },
    TaskMutateMetadataAndBumpVersionForLatestExecution {
        id: String,
        expected_version: i64,
        authority: LatestExecutionAuthority,
        mutations: Vec<TaskMetadataMutation>,
        updated_at: String,
    },
    TaskClaimMetadataForLatestExecution {
        input: LatestExecutionMetadataClaim,
    },
    TaskWakeDispatchForTask {
        id: String,
        updated_at: String,
    },
    TaskSetEntryBarrier {
        id: String,
        expected_version: i64,
        entry_barrier_json: Option<String>,
        updated_at: String,
    },
    TaskSetEntryBarrierWithWorkflowAuthority {
        id: String,
        expected_version: i64,
        entry_barrier_json: Option<String>,
        updated_at: String,
        expected_project_version: i64,
        expected_workflow_definition: String,
    },
    TaskUpdateStatus {
        input: UpdateTaskStatus,
    },
    TaskUpdateStatusForLatestExecution {
        input: UpdateTaskStatus,
        authority: LatestExecutionAuthority,
    },
    TaskUpdateStatusWithRecoveryMarker {
        input: UpdateTaskStatus,
        marker: CreateTransitionLog,
    },
    TaskRoleAssignmentAssign {
        input: CreateTaskRoleAssignment,
    },
    TaskRoleAssignmentAssignIfUnchanged {
        input: CreateTaskRoleAssignment,
        expected_previous: Option<TaskRoleAssignment>,
    },
    TaskRoleAssignmentRemove {
        task_id: String,
        role_name: String,
    },
    TaskRoleAssignmentAssignAndClearReviewAuthority {
        input: CreateTaskRoleAssignment,
        expected_previous: Option<TaskRoleAssignment>,
        expected_task_version: i64,
        updated_at: String,
    },
    TaskRoleAssignmentRemoveAndClearReviewAuthority {
        expected_assignment: TaskRoleAssignment,
        expected_task_version: i64,
        updated_at: String,
    },
}
fn encode<T: Serialize>(value: T) -> Result<Value> {
    serde_json::to_value(value).map_err(|e| DbError::Check(e.to_string()))
}
impl TaskMutation {
    pub fn expected_task_version(&self) -> Option<i64> {
        match self {
            Self::ReviewCreateAttemptWithExecution { admission, .. } => {
                admission.as_ref().map(|a| a.expected_task_version)
            }
            Self::ReviewCreateManualPass { input } => Some(input.expected_task_version),
            Self::ReviewUpdateStatusWithTaskAuthority {
                expected_task_version,
                ..
            }
            | Self::ReviewUpdateStatusWithTaskAuthorityAndCandidate {
                expected_task_version,
                ..
            }
            | Self::ReviewUpdateStatusWithTaskAuthorityAndProjectCandidate {
                expected_task_version,
                ..
            }
            | Self::ReviewUpdateStatusWithReviewAuthority {
                expected_task_version,
                ..
            }
            | Self::ReviewUpdateStatusWithReviewAuthorityAndTaskProjection {
                expected_task_version,
                ..
            } => Some(*expected_task_version),
            Self::ExecutionCreateWithLeaseAndAdmission { admission, .. } => {
                admission.as_ref().map(|a| a.expected_task_version)
            }
            Self::TaskRestoreQueuedRecovery { input } => Some(input.expected_version),
            Self::TaskUpdate { input, .. } => Some(input.expected_version),
            Self::TaskUpdateWithRecoveryMarker { input, .. } => Some(input.expected_version),
            Self::TaskUpdateWithWorkflowAuthority { input, .. } => Some(input.expected_version),
            Self::TaskUpdateWithWorkflowAuthorityAndRecoveryMarker { input, .. } => {
                Some(input.expected_version)
            }
            Self::TaskSetErrorAnnotationIfNoRunningExecution {
                expected_version, ..
            } => Some(*expected_version),
            Self::TaskUpdateRecoveryMetadataIfNoRunningExecution {
                expected_version, ..
            } => Some(*expected_version),
            Self::TaskSetReviewPassedAtCas {
                expected_version, ..
            } => Some(*expected_version),
            Self::TaskSetReviewPassedAtCasForReview {
                expected_version, ..
            } => Some(*expected_version),
            Self::TaskMutateMetadata {
                expected_version, ..
            } => *expected_version,
            Self::TaskMutateMetadataWithChange {
                expected_version, ..
            } => *expected_version,
            Self::TaskMutateMetadataAndBumpVersion {
                expected_version, ..
            } => Some(*expected_version),
            Self::TaskMutateMetadataAndBumpVersionWithProjectAuthority {
                expected_version, ..
            } => Some(*expected_version),
            Self::TaskMutateMetadataAndBumpVersionForLatestExecution {
                expected_version, ..
            } => Some(*expected_version),
            Self::TaskSetEntryBarrier {
                expected_version, ..
            } => Some(*expected_version),
            Self::TaskSetEntryBarrierWithWorkflowAuthority {
                expected_version, ..
            } => Some(*expected_version),
            Self::TaskUpdateStatus { input, .. } => Some(input.expected_version),
            Self::TaskUpdateStatusForLatestExecution { input, .. } => Some(input.expected_version),
            Self::TaskUpdateStatusWithRecoveryMarker { input, .. } => Some(input.expected_version),
            _ => None,
        }
    }
    fn rebase_task_version(&mut self, version: i64) {
        match self {
            Self::ExecutionCreateWithLeaseAndAdmission {
                admission: Some(admission),
                ..
            } => admission.expected_task_version = version,
            Self::TaskUpdate { input, .. } => input.expected_version = version,
            Self::TaskUpdateWithRecoveryMarker { input, .. } => input.expected_version = version,
            Self::TaskUpdateWithWorkflowAuthority { input, .. } => input.expected_version = version,
            Self::TaskUpdateWithWorkflowAuthorityAndRecoveryMarker { input, .. } => {
                input.expected_version = version
            }
            Self::TaskSetErrorAnnotationIfNoRunningExecution {
                expected_version, ..
            } => *expected_version = version,
            Self::TaskUpdateRecoveryMetadataIfNoRunningExecution {
                expected_version, ..
            } => *expected_version = version,
            Self::TaskSetReviewPassedAtCas {
                expected_version, ..
            } => *expected_version = version,
            Self::TaskSetReviewPassedAtCasForReview {
                expected_version, ..
            } => *expected_version = version,
            Self::TaskMutateMetadata {
                expected_version, ..
            } => {
                if expected_version.is_some() {
                    *expected_version = Some(version);
                }
            }
            Self::TaskMutateMetadataWithChange {
                expected_version, ..
            } => {
                if expected_version.is_some() {
                    *expected_version = Some(version);
                }
            }
            Self::TaskMutateMetadataAndBumpVersion {
                expected_version, ..
            } => *expected_version = version,
            Self::TaskMutateMetadataAndBumpVersionWithProjectAuthority {
                expected_version, ..
            } => *expected_version = version,
            Self::TaskMutateMetadataAndBumpVersionForLatestExecution {
                expected_version, ..
            } => *expected_version = version,
            Self::TaskSetEntryBarrier {
                expected_version, ..
            } => *expected_version = version,
            Self::TaskSetEntryBarrierWithWorkflowAuthority {
                expected_version, ..
            } => *expected_version = version,
            Self::TaskUpdateStatus { input, .. } => input.expected_version = version,
            Self::TaskUpdateStatusForLatestExecution { input, .. } => {
                input.expected_version = version
            }
            Self::TaskUpdateStatusWithRecoveryMarker { input, .. } => {
                input.expected_version = version
            }
            _ => {}
        }
    }
    pub async fn apply(&self, db: &SqliteDb) -> Result<Value> {
        let mut mutation = self.clone();
        if let Some(step) = crate::task_writer::current_task_step() {
            let version: i64 = sqlx::query_scalar("SELECT version FROM task WHERE id=?")
                .bind(&step.task_id)
                .fetch_one(db.pool())
                .await?;
            mutation.rebase_task_version(version);
        }
        match mutation {
            Self::ReviewCreateAttemptWithExecution {
                review,
                execution,
                lease,
                admission,
            } => encode(
                Box::pin(ReviewRepo::create_attempt_with_execution_and_lease(
                    db, *review, *execution, lease, admission,
                ))
                .await?,
            ),
            Self::ReviewCreateManualPass { input } => encode(
                Box::pin(ReviewRepo::create_manual_pass_with_task_authority(
                    db, input,
                ))
                .await?,
            ),
            Self::ReviewUpdateStatusWithTaskAuthority {
                id,
                status,
                step_results_json,
                finished_at,
                updated_at,
                expected_task_version,
                review_passed_at,
                origin,
            } => encode(
                Box::pin(ReviewRepo::update_status_with_task_authority(
                    db,
                    &id,
                    status,
                    step_results_json,
                    finished_at,
                    &updated_at,
                    expected_task_version,
                    review_passed_at,
                    origin,
                ))
                .await?,
            ),
            Self::ReviewUpdateStatusWithTaskAuthorityAndCandidate {
                id,
                status,
                step_results_json,
                finished_at,
                updated_at,
                expected_task_version,
                review_passed_at,
                expected_candidate_execution_id,
            } => encode(
                Box::pin(ReviewRepo::update_status_with_task_authority_and_candidate(
                    db,
                    &id,
                    status,
                    step_results_json,
                    finished_at,
                    &updated_at,
                    expected_task_version,
                    review_passed_at,
                    &expected_candidate_execution_id,
                ))
                .await?,
            ),
            Self::ReviewUpdateStatusWithTaskAuthorityAndProjectCandidate {
                id,
                status,
                step_results_json,
                finished_at,
                updated_at,
                expected_task_version,
                review_passed_at,
                expected_project_version,
                expected_workflow_definition,
                expected_review_status,
                expected_review_updated_at,
                expected_candidate_execution_id,
            } => encode(
                Box::pin(
                    ReviewRepo::update_status_with_task_authority_and_project_candidate(
                        db,
                        &id,
                        status,
                        step_results_json,
                        finished_at,
                        &updated_at,
                        expected_task_version,
                        review_passed_at,
                        expected_project_version,
                        expected_workflow_definition.as_deref(),
                        expected_review_status,
                        &expected_review_updated_at,
                        &expected_candidate_execution_id,
                    ),
                )
                .await?,
            ),
            Self::ReviewUpdateStatusWithReviewAuthority {
                id,
                status,
                step_results_json,
                finished_at,
                updated_at,
                expected_task_version,
                expected_task_status,
                expected_project_version,
                expected_workflow_definition,
                expected_review_status,
                expected_review_updated_at,
                expected_candidate_execution_id,
            } => encode(
                Box::pin(ReviewRepo::update_status_with_review_authority(
                    db,
                    &id,
                    status,
                    step_results_json,
                    finished_at,
                    &updated_at,
                    expected_task_version,
                    &expected_task_status,
                    expected_project_version,
                    expected_workflow_definition.as_deref(),
                    expected_review_status,
                    &expected_review_updated_at,
                    expected_candidate_execution_id.as_deref(),
                ))
                .await?,
            ),
            Self::ReviewUpdateStatusWithReviewAuthorityAndTaskProjection {
                id,
                status,
                step_results_json,
                finished_at,
                updated_at,
                expected_task_version,
                expected_task_status,
                expected_project_version,
                expected_workflow_definition,
                expected_review_status,
                expected_review_updated_at,
                expected_candidate_execution_id,
                task_projection,
            } => encode(
                Box::pin(
                    ReviewRepo::update_status_with_review_authority_and_task_projection(
                        db,
                        &id,
                        status,
                        step_results_json,
                        finished_at,
                        &updated_at,
                        expected_task_version,
                        &expected_task_status,
                        expected_project_version,
                        expected_workflow_definition.as_deref(),
                        expected_review_status,
                        &expected_review_updated_at,
                        &expected_candidate_execution_id,
                        task_projection,
                    ),
                )
                .await?,
            ),
            Self::ExecutionCreate { input } => {
                encode(Box::pin(ExecutionRepo::create(db, input)).await?)
            }
            Self::ExecutionCreateWithLeaseAndAdmission {
                input,
                lease,
                admission,
            } => encode(
                Box::pin(ExecutionRepo::create_with_lease_and_admission(
                    db, *input, lease, admission,
                ))
                .await?,
            ),
            Self::Sql {
                task_id,
                query,
                arguments,
            } => encode(Box::pin(db.apply_task_sql(&task_id, &query, arguments)).await?),
            Self::TaskUpdate { input } => encode(Box::pin(TaskRepo::update(db, input)).await?),
            Self::TaskUpdateIfAnnotation {
                mut input,
                expected_annotation,
                expected_project_version,
                expected_workflow_definition,
            } => {
                let task = TaskRepo::get_by_id(db, &input.id, false)
                    .await?
                    .ok_or(DbError::NotFound)?;
                if task.error_annotation != expected_annotation {
                    return encode(());
                }
                input.expected_version = task.version;
                match (expected_project_version, expected_workflow_definition) {
                    (Some(version), Some(definition)) => {
                        Box::pin(TaskRepo::update_with_workflow_authority(
                            db, input, version, definition,
                        ))
                        .await?;
                    }
                    _ => {
                        Box::pin(TaskRepo::update(db, input)).await?;
                    }
                }
                encode(())
            }
            Self::TaskUpdateWithRecoveryMarker { input, marker } => {
                encode(Box::pin(TaskRepo::update_with_recovery_marker(db, input, marker)).await?)
            }
            Self::TaskUpdateWithWorkflowAuthority {
                input,
                expected_project_version,
                expected_workflow_definition,
            } => encode(
                Box::pin(TaskRepo::update_with_workflow_authority(
                    db,
                    input,
                    expected_project_version,
                    expected_workflow_definition,
                ))
                .await?,
            ),
            Self::TaskUpdateWithWorkflowAuthorityAndRecoveryMarker {
                input,
                marker,
                expected_project_version,
                expected_workflow_definition,
            } => encode(
                Box::pin(
                    TaskRepo::update_with_workflow_authority_and_recovery_marker(
                        db,
                        input,
                        marker,
                        expected_project_version,
                        expected_workflow_definition,
                    ),
                )
                .await?,
            ),
            Self::TaskSetErrorAnnotationIfNoRunningExecution {
                id,
                expected_version,
                expected_status,
                expected_state_entry_token,
                expected_workflow_definition,
                expected_assignment_role,
                expected_assignment,
                annotation,
                updated_at,
                stopped_execution_id,
                workspace_id,
                overlapping_roles,
            } => encode(
                Box::pin(TaskRepo::set_error_annotation_if_no_running_execution(
                    db,
                    &id,
                    expected_version,
                    &expected_status,
                    expected_state_entry_token.as_deref(),
                    &expected_workflow_definition,
                    expected_assignment_role.as_deref(),
                    expected_assignment,
                    &annotation,
                    &updated_at,
                    &stopped_execution_id,
                    workspace_id.as_deref(),
                    overlapping_roles,
                ))
                .await?,
            ),
            Self::TaskUpdateRecoveryMetadataIfNoRunningExecution {
                id,
                expected_version,
                error_annotation,
                blocked_json,
                failed_json,
                updated_at,
                workspace_id,
                overlapping_roles,
                metadata_mutations,
                condition,
            } => encode(
                Box::pin(TaskRepo::update_recovery_metadata_if_no_running_execution(
                    db,
                    &id,
                    expected_version,
                    error_annotation,
                    blocked_json,
                    failed_json,
                    &updated_at,
                    workspace_id.as_deref(),
                    overlapping_roles,
                    metadata_mutations,
                    condition,
                ))
                .await?,
            ),
            Self::TaskRestoreQueuedRecovery { input } => {
                encode(Box::pin(TaskRepo::restore_queued_recovery(db, input)).await?)
            }
            Self::TaskSetReviewPassedAt {
                id,
                review_passed_at,
                updated_at,
            } => encode(
                Box::pin(TaskRepo::set_review_passed_at(
                    db,
                    &id,
                    review_passed_at,
                    &updated_at,
                ))
                .await?,
            ),
            Self::TaskSetReviewPassedAtCas {
                id,
                expected_version,
                review_passed_at,
                updated_at,
            } => encode(
                Box::pin(TaskRepo::set_review_passed_at_cas(
                    db,
                    &id,
                    expected_version,
                    review_passed_at,
                    &updated_at,
                ))
                .await?,
            ),
            Self::TaskSetReviewPassedAtCasForReview {
                id,
                expected_version,
                review_passed_at,
                expected_review_updated_at,
                updated_at,
            } => encode(
                Box::pin(TaskRepo::set_review_passed_at_cas_for_review(
                    db,
                    &id,
                    expected_version,
                    review_passed_at,
                    &expected_review_updated_at,
                    &updated_at,
                ))
                .await?,
            ),
            Self::TaskMutateMetadata {
                id,
                expected_version,
                mutations,
                updated_at,
            } => encode(
                Box::pin(TaskRepo::mutate_metadata(
                    db,
                    &id,
                    expected_version,
                    mutations,
                    &updated_at,
                ))
                .await?,
            ),
            Self::TaskMutateMetadataWithChange {
                id,
                expected_version,
                mutations,
                updated_at,
            } => encode(
                Box::pin(TaskRepo::mutate_metadata_with_change(
                    db,
                    &id,
                    expected_version,
                    mutations,
                    &updated_at,
                ))
                .await?,
            ),
            Self::TaskMutateMetadataAndBumpVersion {
                id,
                expected_version,
                mutations,
                updated_at,
            } => encode(
                Box::pin(TaskRepo::mutate_metadata_and_bump_version(
                    db,
                    &id,
                    expected_version,
                    mutations,
                    &updated_at,
                ))
                .await?,
            ),
            Self::TaskMutateMetadataAndBumpVersionWithProjectAuthority {
                id,
                expected_version,
                expected_project_version,
                mutations,
                updated_at,
            } => encode(
                Box::pin(
                    TaskRepo::mutate_metadata_and_bump_version_with_project_authority(
                        db,
                        &id,
                        expected_version,
                        expected_project_version,
                        mutations,
                        &updated_at,
                    ),
                )
                .await?,
            ),
            Self::TaskMutateMetadataAndBumpVersionForLatestExecution {
                id,
                expected_version,
                authority,
                mutations,
                updated_at,
            } => encode(
                Box::pin(
                    TaskRepo::mutate_metadata_and_bump_version_for_latest_execution(
                        db,
                        &id,
                        expected_version,
                        authority,
                        mutations,
                        &updated_at,
                    ),
                )
                .await?,
            ),
            Self::TaskClaimMetadataForLatestExecution { input } => {
                encode(Box::pin(TaskRepo::claim_metadata_for_latest_execution(db, input)).await?)
            }
            Self::TaskWakeDispatchForTask { id, updated_at } => {
                encode(Box::pin(TaskRepo::wake_dispatch_for_task(db, &id, &updated_at)).await?)
            }
            Self::TaskSetEntryBarrier {
                id,
                expected_version,
                entry_barrier_json,
                updated_at,
            } => encode(
                Box::pin(TaskRepo::set_entry_barrier(
                    db,
                    &id,
                    expected_version,
                    entry_barrier_json,
                    &updated_at,
                ))
                .await?,
            ),
            Self::TaskSetEntryBarrierWithWorkflowAuthority {
                id,
                expected_version,
                entry_barrier_json,
                updated_at,
                expected_project_version,
                expected_workflow_definition,
            } => encode(
                Box::pin(TaskRepo::set_entry_barrier_with_workflow_authority(
                    db,
                    &id,
                    expected_version,
                    entry_barrier_json,
                    &updated_at,
                    expected_project_version,
                    expected_workflow_definition,
                ))
                .await?,
            ),
            Self::TaskUpdateStatus { input } => {
                encode(Box::pin(TaskRepo::update_status(db, input)).await?)
            }
            Self::TaskUpdateStatusForLatestExecution { input, authority } => encode(
                Box::pin(TaskRepo::update_status_for_latest_execution(
                    db, input, authority,
                ))
                .await?,
            ),
            Self::TaskUpdateStatusWithRecoveryMarker { input, marker } => encode(
                Box::pin(TaskRepo::update_status_with_recovery_marker(
                    db, input, marker,
                ))
                .await?,
            ),
            Self::TaskRoleAssignmentAssign { input } => {
                encode(Box::pin(TaskRoleAssignmentRepo::assign(db, input)).await?)
            }
            Self::TaskRoleAssignmentAssignIfUnchanged {
                input,
                expected_previous,
            } => encode(
                Box::pin(TaskRoleAssignmentRepo::assign_if_unchanged(
                    db,
                    input,
                    expected_previous.as_ref(),
                ))
                .await?,
            ),
            Self::TaskRoleAssignmentRemove { task_id, role_name } => {
                encode(Box::pin(TaskRoleAssignmentRepo::remove(db, &task_id, &role_name)).await?)
            }
            Self::TaskRoleAssignmentAssignAndClearReviewAuthority {
                input,
                expected_previous,
                expected_task_version,
                updated_at,
            } => encode(
                Box::pin(TaskRoleAssignmentRepo::assign_and_clear_review_authority(
                    db,
                    input,
                    expected_previous.as_ref(),
                    expected_task_version,
                    &updated_at,
                ))
                .await?,
            ),
            Self::TaskRoleAssignmentRemoveAndClearReviewAuthority {
                expected_assignment,
                expected_task_version,
                updated_at,
            } => encode(
                Box::pin(TaskRoleAssignmentRepo::remove_and_clear_review_authority(
                    db,
                    &expected_assignment,
                    expected_task_version,
                    &updated_at,
                ))
                .await?,
            ),
        }
    }
}
