//! Server-side admission for Charter-backed repository Tasks.
//!
//! The migration deliberately stores Project Task governance separately from
//! the legacy `task` row.  This module is the policy boundary: callers may
//! propose provenance, but Forge derives whether the Task is runnable from
//! the current Charter, baseline, approval, and Project-local artifact rows.

use super::*;
use crate::execution_setup::{
    canonical_task_capability, classify_task_execution, is_read_only_capability,
    TaskExecutionClass, SUPPORTED_CAPABILITY_PROFILES,
};
use crate::project_execution_setup_projection::{
    required_reconciliation_conflicts, ReconciliationConflictRow,
};
use api_types::{
    AdaptiveTaskOperation, ExecutionGate, ExecutionSetupState, RetryAction, SetupRequirement,
    TaskGovernanceRequest,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use db::{CreateProjectCanonicalConflict, CreateProjectReconciliation, ProjectOrchestrationRepo};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use sqlx::{Row, Sqlite, Transaction};

const WORKSPACE_LEASE_SECONDS: i64 = 15 * 60;
const CAPABILITY_PROFILE_REVISION: &str = "forge.capability-profile/v1";

/// Adaptive-boundary fields that describe a Task's fixed outcomes,
/// acceptance, risk, side effects, release policy, or elevated authority.
/// Shared by boundary-crossing detection (D14) and capability-aware review
/// admission (D16/8.2.5): a recorded conflict whose affected paths never
/// touch one of these fields does not change what an independent reviewer
/// is evaluating, so read-only review of an already-committed result may
/// continue even while repository mutation stays blocked.
const FIXED_BOUNDARY_FIELDS: [&str; 6] = [
    "fixed_outcomes",
    "fixed_acceptance",
    "fixed_risk_classes",
    "forbidden_side_effects",
    "elevated_operations",
    "fixed_boundary_digest",
];

/// Whether the requested WorkspaceLease capability is repository-mutating or
/// independent read-only review. Only the dedicated `reviewer` canonical
/// role is read-only; every other resolved role is a repository Worker
/// (D16/8.2.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExecutionGateCapability {
    RepositoryMutation,
    ReadOnlyReview,
}

/// Immutable traceability facts inherited by an adaptive Task reshape.
///
/// The current Charter authorizes split/sequence/replace. When the source
/// Task carries optional baseline references, the reshape preserves that
/// snapshot instead of manufacturing different references from caller text.
#[derive(Debug, Clone)]
pub(crate) struct AdaptiveTaskGovernance {
    pub charter_revision_id: Option<String>,
    pub plan_item_id: Option<String>,
    pub milestone_id: Option<String>,
    pub document_revisions_json: String,
    pub capability_class: Option<String>,
    pub risk_class: Option<String>,
    pub provenance_json: String,
}

#[derive(Debug)]
pub(super) struct PreparedTaskGovernance {
    pub charter_revision_id: Option<String>,
    pub plan_item_id: Option<String>,
    pub milestone_id: Option<String>,
    pub document_revisions_json: String,
    pub capability_class: Option<String>,
    pub risk_class: Option<String>,
    pub runnable: bool,
    pub replacement_of_task_id: Option<String>,
    pub provenance_json: String,
}

impl TaskService {
    /// Load the immutable governance projection that adaptive Task operations
    /// are allowed to inherit.  The baseline join is intentionally read from
    /// the current row rather than from caller-provided provenance.
    pub(crate) async fn adaptive_task_governance(
        &self,
        task: &db::Task,
    ) -> Result<Option<AdaptiveTaskGovernance>> {
        let row = sqlx::query(
            "SELECT g.project_id, g.charter_revision_id, g.plan_item_id,
                    g.milestone_id, g.document_revisions_json,
                    g.capability_class, g.risk_class, g.runnable,
                    g.provenance_json
             FROM project_task_governance g
             WHERE g.task_id = ? AND g.project_id = ?",
        )
        .bind(&task.id)
        .bind(&task.project_id)
        .fetch_optional(self.db.pool())
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(AdaptiveTaskGovernance {
            charter_revision_id: row.get("charter_revision_id"),
            plan_item_id: row.get("plan_item_id"),
            milestone_id: row.get("milestone_id"),
            document_revisions_json: row.get("document_revisions_json"),
            capability_class: row.get("capability_class"),
            risk_class: row.get("risk_class"),
            provenance_json: row.get("provenance_json"),
        }))
    }

    /// Admit one adaptive operation at the shared TaskService boundary.
    /// Split, sequence, and replace are normal Task-system operations. An
    /// optional execution baseline may contribute traceability, but its
    /// approval state and operation list do not grant implementation authority;
    /// the current approved Charter does.
    pub(crate) async fn authorize_adaptive_task_operation(
        &self,
        task: &db::Task,
        operation: &str,
    ) -> Result<Option<AdaptiveTaskGovernance>> {
        // Parse first. An unsupported verb is a caller/typed-input mistake —
        // it can never prove that any authoritative record diverged, so it
        // must never create a canonical conflict or reconciliation row.
        if AdaptiveTaskOperation::parse(operation).is_none() {
            return Err(ServiceError::invalid_operation(format!(
                "adaptive Task operation '{operation}' is not a supported verb; supported: {}",
                crate::adaptive_task_operations::adaptive_task_operation_supported_values()
            )));
        }
        let Some(governance) = self.adaptive_task_governance(task).await? else {
            return Ok(None);
        };
        let current_charter_revision_id: Option<String> = sqlx::query_scalar(
            "SELECT current_charter_revision_id FROM project
             WHERE id = ? AND charter_status = 'charter_backed'
               AND charter_setup_required = 0",
        )
        .bind(&task.project_id)
        .fetch_optional(self.db.pool())
        .await?
        .flatten();
        if current_charter_revision_id.is_some()
            && governance.charter_revision_id != current_charter_revision_id
        {
            return Err(ServiceError::Conflict(
                "reconciliation_required: Task Charter traceability is stale".to_owned(),
            ));
        }
        Ok(Some(governance))
    }

    /// A child/replacement must inherit the source Task's immutable
    /// governance references.  Caller-provided risk/capability or baseline
    /// changes are a boundary crossing, not an adaptive implementation
    /// choice, and are recorded as reconciliation-required.
    pub(crate) async fn validate_adaptive_child_governance(
        &self,
        source: &db::Task,
        requested: &TaskGovernanceRequest,
        operation: &str,
    ) -> Result<()> {
        let Some(governance) = self
            .authorize_adaptive_task_operation(source, operation)
            .await?
        else {
            return Ok(());
        };
        let source_provenance: Value =
            serde_json::from_str(&governance.provenance_json).map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "Task governance provenance is invalid: {error}"
                ))
            })?;
        let requested_provenance = requested.provenance.as_ref();
        // Record exactly which fields diverged rather than a generic list
        // unrelated to the detected difference (D14).
        let mut affected_paths: Vec<&str> = Vec::new();
        if requested.charter_revision_id.as_deref() != governance.charter_revision_id.as_deref() {
            affected_paths.push("charter_revision_id");
        }
        if requested.plan_item_id.as_deref() != governance.plan_item_id.as_deref() {
            affected_paths.push("plan_item_id");
        }
        if requested.milestone_id.as_deref() != governance.milestone_id.as_deref() {
            affected_paths.push("milestone_id");
        }
        if serde_json::to_string(&requested.document_revision_ids)
            .ok()
            .as_deref()
            != Some(governance.document_revisions_json.as_str())
        {
            affected_paths.push("document_revision_ids");
        }
        if requested.capability_class.as_deref() != governance.capability_class.as_deref() {
            affected_paths.push("capability_class");
        }
        if requested.risk_class.as_deref() != governance.risk_class.as_deref() {
            affected_paths.push("risk_class");
        }
        for field in FIXED_BOUNDARY_FIELDS {
            let mismatch =
                requested_provenance
                    .and_then(Value::as_object)
                    .is_some_and(|requested| {
                        requested.get(field).is_some()
                            && source_provenance.get(field) != requested.get(field)
                    });
            if mismatch {
                affected_paths.push(field);
            }
        }
        if affected_paths.is_empty() {
            return Ok(());
        }
        let reason = format!(
            "adaptive Task {operation} changes an approved outcome, acceptance, risk, side-effect, release, or elevated-operation boundary"
        );
        self.record_adaptive_boundary_reconciliation(
            source,
            &governance,
            operation,
            &reason,
            "adaptive_task_boundary_crossed",
            &affected_paths,
        )
        .await?;
        Err(ServiceError::Conflict(format!(
            "reconciliation_required: {reason}"
        )))
    }

    #[allow(clippy::too_many_arguments)]
    async fn record_adaptive_boundary_reconciliation(
        &self,
        task: &db::Task,
        governance: &AdaptiveTaskGovernance,
        operation: &str,
        reason: &str,
        conflict_code: &str,
        affected_paths: &[&str],
    ) -> Result<()> {
        use sha2::{Digest, Sha256};

        let task_digest = hex::encode(Sha256::digest(governance.provenance_json.as_bytes()));
        let idempotency_key = format!(
            "adaptive-boundary:{}:{}:{}:{}:{}",
            task.project_id, task.id, conflict_code, operation, task_digest
        );
        let now = now_rfc3339();
        let conflict = ProjectOrchestrationRepo::create_project_canonical_conflict(
            &*self.db,
            CreateProjectCanonicalConflict {
                id: new_uuid_v4(),
                project_id: task.project_id.clone(),
                domain: "execution".to_owned(),
                governing_record_type: "project_charter".to_owned(),
                governing_record_id: task.project_id.clone(),
                governing_record_revision: governance
                    .charter_revision_id
                    .clone()
                    .unwrap_or_else(|| "unknown".to_owned()),
                governing_record_digest: "unknown".to_owned(),
                conflicting_record_type: "task".to_owned(),
                conflicting_record_id: task.id.clone(),
                conflicting_record_revision: task.version.to_string(),
                conflicting_record_digest: task_digest.clone(),
                affected_paths_json: serde_json::to_string(affected_paths).map_err(|error| {
                    ServiceError::invalid_operation(format!(
                        "adaptive boundary affected paths are invalid: {error}"
                    ))
                })?,
                conflict_code: conflict_code.to_owned(),
                description: reason.to_owned(),
                detected_by_type: "system".to_owned(),
                detected_by_id: Some("task-service".to_owned()),
                authorization_basis: "adaptive_task_boundary".to_owned(),
                authorization_action: "task.adaptive.reject".to_owned(),
                explicit_event: format!("task.adaptive.{operation}.rejected"),
                authorization_occurred_at: now.clone(),
                idempotency_key,
                created_at: now.clone(),
            },
        )
        .await?;
        let existing =
            ProjectOrchestrationRepo::list_project_reconciliations(&*self.db, &task.project_id)
                .await?
                .into_iter()
                .find(|record| {
                    record.conflict_id == conflict.id
                        && record.record_type == "task"
                        && record.record_id == task.id
                        && record.state == "required"
                });
        if existing.is_none() {
            ProjectOrchestrationRepo::create_project_reconciliation(
                &*self.db,
                CreateProjectReconciliation {
                    id: new_uuid_v4(),
                    project_id: task.project_id.clone(),
                    conflict_id: conflict.id,
                    record_type: "task".to_owned(),
                    record_id: task.id.clone(),
                    record_revision: task.version.to_string(),
                    record_digest: task_digest,
                    governing_record_type: "project_charter".to_owned(),
                    governing_record_id: task.project_id.clone(),
                    governing_record_revision: governance
                        .charter_revision_id
                        .clone()
                        .unwrap_or_else(|| "unknown".to_owned()),
                    governing_record_digest: "unknown".to_owned(),
                    created_at: now.clone(),
                    updated_at: now,
                },
            )
            .await?;
        }
        Ok(())
    }

    /// Verify that an identity is currently usable for Project Task execution
    /// before preparing a repository workspace. Main/Project chat bindings do
    /// not disqualify it; Task authority still comes only from the explicit
    /// role assignment and scheduler lease.
    pub(super) async fn ensure_repository_worker_identity(
        &self,
        project_id: &str,
        principal_id: &str,
    ) -> Result<()> {
        let charter_backed: i64 = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM project
                 WHERE id = ? AND charter_status = 'charter_backed'
                   AND charter_setup_required = 0
             )",
        )
        .bind(project_id)
        .fetch_one(self.db.pool())
        .await?;
        if charter_backed == 1
            && !crate::is_eligible_execution_identity(&self.db, project_id, principal_id).await?
        {
            let mut requirement = SetupRequirement::new("role_assignment");
            requirement.role = Some("worker".to_owned());
            requirement.capability = Some("repository_write".to_owned());
            requirement.action = Some(RetryAction::SelectWorker);
            return Err(ServiceError::execution_setup_required(
                "repository WorkspaceLease requires an active Project-eligible execution identity",
                vec![requirement],
            ));
        }
        Ok(())
    }

    /// Validate and prepare the immutable governance row that accompanies a
    /// new Task. The approved Charter is the implementation authority. A
    /// baseline, when supplied, contributes immutable traceability but its
    /// lifecycle or approval status does not decide whether the Task can run.
    pub(super) async fn prepare_task_governance(
        &self,
        project: &db::Project,
        repo_id: Option<&String>,
        task_type: &str,
        requested: Option<TaskGovernanceRequest>,
    ) -> Result<Option<PreparedTaskGovernance>> {
        // A repository binding is capability-bearing regardless of the task
        // label. Planning/discovery labels may use the explicit pre-baseline
        // read-only branch, but they never infer a write profile or receive a
        // repository lease as an accidental side effect.
        let requested_capability = requested
            .as_ref()
            .and_then(|request| request.capability_class.as_deref());
        let execution_class = classify_task_execution(task_type, requested_capability)?;
        let repository_capable = repo_id.is_some();
        let implementation = execution_class == TaskExecutionClass::Implementation;
        // Implementation intent remains governed even while repository setup
        // is incomplete. A missing primary_repo_id must not downgrade it to
        // an ungoverned planning Task.
        let charter_backed = project.charter_status == "charter_backed"
            && !project.charter_setup_required
            && project.current_charter_revision_id.is_some();

        // Legacy/unverified Projects remain usable through the existing Task
        // API.  They have no fabricated Charter or baseline to bind.
        if !charter_backed {
            return Ok(None);
        }

        let default_capability = canonical_task_capability(task_type, requested_capability)?;
        let mut requested = requested.unwrap_or_else(|| TaskGovernanceRequest {
            // Mainstream Task creation surfaces do not need an orchestration
            // envelope. Bind directly to the current approved Charter.
            charter_revision_id: project.current_charter_revision_id.clone(),
            plan_item_id: None,
            milestone_id: None,
            document_revision_ids: Vec::new(),
            capability_class: (!implementation).then_some(default_capability.clone()),
            risk_class: (!implementation).then(|| "low".to_owned()),
            provenance: None,
        });
        if requested
            .capability_class
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
        {
            // Preserve the historical null capability for an implementation
            // plan while keeping the canonical server default available to
            // the classifier and lease boundary. Read-only intent must carry
            // its explicit non-mutating profile in durable governance.
            if !implementation {
                requested.capability_class = Some(default_capability);
            }
        }

        let current_charter_revision_id =
            project.current_charter_revision_id.clone().ok_or_else(|| {
                ServiceError::invalid_operation("Project Charter revision is missing")
            })?;
        if requested.charter_revision_id.as_deref() != Some(current_charter_revision_id.as_str()) {
            return Err(ServiceError::invalid_operation(
                "Task Charter revision must match the Project's current approved Charter revision",
            ));
        }

        // Charter authority governs a Task. A milestone reference is still
        // held to Project ownership -- a Task cannot claim a milestone from
        // another Project -- but it needs no separately approved artifact to
        // authorize it.
        if let Some(milestone_id) = requested.milestone_id.as_deref() {
            let milestone_project = sqlx::query_scalar::<_, String>(
                "SELECT project_id FROM project_milestone WHERE id = ?",
            )
            .bind(milestone_id)
            .fetch_optional(self.db.pool())
            .await?;
            if milestone_project.as_deref() != Some(project.id.as_str()) {
                return Err(ServiceError::invalid_operation(
                    "Task milestone must belong to the same Project",
                ));
            }
        }

        validate_document_revisions(
            self.db.pool(),
            &project.id,
            &requested.document_revision_ids,
        )
        .await?;

        if repository_capable && execution_class == TaskExecutionClass::ReadOnlyPlanning {
            if let Some(capability_class) = requested.capability_class.as_deref() {
                if !is_read_only_capability(capability_class) {
                    return Err(ServiceError::invalid_operation(
                        "discovery/planning Tasks require a read-only capability",
                    ));
                }
            } else {
                requested.capability_class = Some("repository_read".to_owned());
            }
            if requested.risk_class.is_none() {
                requested.risk_class = Some("low".to_owned());
            }
        }

        // A Charter may author classes the server cannot dispatch; fail the
        // proposal now with the allowed vocabulary instead of admitting a
        // Task whose every dispatch would be refused by the lease issuer.
        require_server_approved_capability_class(requested.capability_class.as_deref())?;

        // `runnable` now records Charter-backed repository readiness. The
        // workflow, role, availability, repository, capability, retry, and
        // lease checks are repeated immediately before dispatch.
        let runnable = repository_capable;
        let provenance_json = build_provenance(
            requested.provenance.clone(),
            requested.plan_item_id.as_deref(),
        )?;
        let replacement_of_task_id = requested
            .provenance
            .as_ref()
            .and_then(|value| value.get("replacement_of_task_id"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        if let Some(replacement_of_task_id) = replacement_of_task_id.as_deref() {
            let owning_project: Option<String> = sqlx::query_scalar(
                "SELECT project_id FROM task WHERE id = ? AND deleted_at IS NULL",
            )
            .bind(replacement_of_task_id)
            .fetch_optional(self.db.pool())
            .await?;
            if owning_project.as_deref() != Some(project.id.as_str()) {
                return Err(ServiceError::invalid_operation(
                    "Task replacement provenance must reference a Task in the same Project",
                ));
            }
        }

        Ok(Some(PreparedTaskGovernance {
            charter_revision_id: requested.charter_revision_id,
            plan_item_id: requested.plan_item_id,
            milestone_id: requested.milestone_id,
            document_revisions_json: serde_json::to_string(&requested.document_revision_ids)
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
            capability_class: requested.capability_class,
            risk_class: requested.risk_class,
            runnable,
            replacement_of_task_id,
            provenance_json,
        }))
    }

    pub(super) async fn insert_task_governance(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        task_id: &str,
        project_id: &str,
        governance: PreparedTaskGovernance,
        now: &str,
    ) -> Result<()> {
        let governance = db::CreateProjectTaskGovernance {
            task_id: task_id.to_owned(),
            project_id: project_id.to_owned(),
            charter_revision_id: governance.charter_revision_id,
            plan_item_id: governance.plan_item_id,
            milestone_id: governance.milestone_id,
            document_revisions_json: governance.document_revisions_json,
            capability_class: governance.capability_class,
            risk_class: governance.risk_class,
            runnable: governance.runnable,
            replacement_of_task_id: governance.replacement_of_task_id,
            provenance_json: governance.provenance_json,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        };
        db::ProjectOrchestrationRepo::insert_project_task_governance_in_tx(
            &*self.db,
            transaction,
            governance,
        )
        .await
        .map_err(ServiceError::from)
    }

    /// Fail closed immediately before any path can prepare a repository
    /// workspace.  This keeps claim, manual launch, role dispatch, retry, and
    /// follow-up execution behind the same gate. This is the repository-
    /// mutation capability; see [`Self::ensure_task_reviewable`] for the
    /// independent read-only review capability (D16, 8.2.5).
    pub(crate) async fn ensure_task_runnable(&self, task: &db::Task) -> Result<()> {
        self.ensure_task_runnable_for_capability(task, ExecutionGateCapability::RepositoryMutation)
            .await
    }

    /// Independent read-only review of an already-committed result (D16,
    /// 8.2.5). Repository mutation remains fully gated; this admits only the
    /// `reviewer` canonical role's read-only WorkspaceLease, and only while
    /// every currently blocking reconciliation is acceptance/evidence/risk/
    /// reviewer/release neutral.
    pub(crate) async fn ensure_task_reviewable(&self, task: &db::Task) -> Result<()> {
        self.ensure_task_runnable_for_capability(task, ExecutionGateCapability::ReadOnlyReview)
            .await
    }

    async fn ensure_task_runnable_for_capability(
        &self,
        task: &db::Task,
        capability: ExecutionGateCapability,
    ) -> Result<()> {
        let row = sqlx::query(
            "SELECT p.charter_status, p.charter_setup_required,
                    p.current_charter_revision_id,
                    t.task_type,
                    g.charter_revision_id,
                    g.capability_class
             FROM project p
             JOIN task t ON t.id = ? AND t.project_id = p.id
                 AND t.deleted_at IS NULL
             LEFT JOIN project_task_governance g ON g.project_id = p.id
                 AND g.task_id = t.id
             WHERE p.id = ?",
        )
        .bind(&task.id)
        .bind(&task.project_id)
        .fetch_optional(self.db.pool())
        .await?
        .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;

        let charter_status: String = row.get("charter_status");
        let charter_setup_required: i64 = row.get("charter_setup_required");
        if charter_status != "charter_backed" || charter_setup_required != 0 {
            // Legacy/unverified Projects retain the pre-Charter workflow.
            return Ok(());
        }
        let task_type: String = row.get("task_type");
        let capability_class = row
            .get::<Option<String>, _>("capability_class")
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(canonical_task_capability(&task_type, None)?);
        let execution_class = classify_task_execution(&task_type, Some(&capability_class))?;
        let bounded_read_only = execution_class == TaskExecutionClass::ReadOnlyPlanning
            && is_read_only_capability(&capability_class);

        if bounded_read_only {
            // Bounded discovery/planning remains admissible without repository
            // setup. It receives no repository WorkspaceLease.
            return Ok(());
        }

        if execution_class == TaskExecutionClass::Implementation {
            // The canonical Project setup projection is the authority for
            // durable repository readiness. A primary_repo_id check by itself
            // is insufficient because a failed provisioning operation must
            // not receive a write lease.
            let setup = crate::load_project_execution_setup(&self.db, &task.project_id).await?;
            if setup.execution_setup_state != ExecutionSetupState::Ready
                || setup.primary_repo.is_none()
            {
                let mut requirements = setup.setup_requirements.clone();
                if setup.primary_repo.is_none()
                    && !requirements
                        .iter()
                        .any(|requirement| requirement.requirement_type == "repository")
                {
                    let mut requirement = SetupRequirement::new("repository");
                    requirement.capability = Some("repository_write".to_owned());
                    requirement.action = Some(RetryAction::AttachRepository);
                    requirements.push(requirement);
                }
                if requirements.is_empty() {
                    requirements.push(SetupRequirement::new("execution_setup"));
                }
                return Err(ServiceError::execution_setup_required(
                    "repository implementation Task is not runnable: Project execution setup is incomplete",
                    requirements,
                ));
            }
            self.ensure_capability_permits_execution(task, capability, setup.execution_gate)
                .await?;
        }

        let governance_charter = row.get::<Option<String>, _>("charter_revision_id");
        if governance_charter.is_some()
            && governance_charter.as_deref()
                != row
                    .get::<Option<String>, _>("current_charter_revision_id")
                    .as_deref()
        {
            return Err(ServiceError::invalid_operation(
                "repository Task cannot start because its Charter traceability is stale",
            ));
        }
        Ok(())
    }

    /// Evaluate the Project's execution gate together with any reconciliation
    /// scoped specifically to this Task, capability-aware (D16, 8.2.5).
    ///
    /// Repository mutation requires the Project gate to be `Active` and no
    /// Task-scoped conflict outstanding. Independent read-only review may
    /// continue past either blocker as long as every currently applicable
    /// conflict is acceptance/evidence/risk/reviewer/release neutral — a
    /// mere baseline-governance pointer conflict must not unnecessarily
    /// prevent reviewing an already-committed result (F11).
    async fn ensure_capability_permits_execution(
        &self,
        task: &db::Task,
        capability: ExecutionGateCapability,
        _execution_gate: ExecutionGate,
    ) -> Result<()> {
        let conflicts = required_reconciliation_conflicts(&self.db, &task.project_id)
            .await?
            .into_iter()
            .filter(|conflict| conflict.is_scoped_to_task(&task.id))
            .collect::<Vec<_>>();

        match capability {
            ExecutionGateCapability::RepositoryMutation => {
                if let Some(conflict) = conflicts.first() {
                    return Err(ServiceError::Conflict(format!(
                        "reconciliation_required: {}",
                        conflict.description
                    )));
                }
                Ok(())
            }
            ExecutionGateCapability::ReadOnlyReview => {
                let Some(blocking) = conflicts.iter().find(|conflict| !review_neutral(conflict))
                else {
                    return Ok(());
                };
                Err(ServiceError::Conflict(format!(
                    "review_blocked: independent review is blocked because the recorded conflict affects acceptance, evidence, risk, reviewer, or release policy: {}",
                    blocking.description
                )))
            }
        }
    }

    /// Issue the scheduler's short-lived internal repository authority only
    /// after the same admission gate used by claim/launch/recovery.  The
    /// opaque lease is persisted by `WorkspaceLeaseRepo`; no route or chat
    /// context receives the row, its capability JSON, or a filesystem path.
    ///
    /// The database-side lease scope guard repeats the current-Charter and
    /// capability predicates, so a Charter supersession racing this call
    /// cannot turn a stale preflight into repository authority.
    pub(super) async fn issue_workspace_lease(
        &self,
        task: &db::Task,
        workspace: &db::Workspace,
        role: &str,
        principal_id: Option<&str>,
        execution_id: &str,
    ) -> Result<Option<db::WorkspaceLease>> {
        self.issue_workspace_lease_with_operation_key(
            task,
            workspace,
            role,
            principal_id,
            execution_id,
            execution_id,
        )
        .await
    }

    /// The issuance body with an explicit idempotency key.  Normal issuance
    /// keys on the execution id; a stale-lease reissue must use the
    /// version-derived key because `operation_idempotency_key` is globally
    /// UNIQUE across all lease rows (revoked ones included) and lease rows
    /// are immutable by trigger.
    async fn issue_workspace_lease_with_operation_key(
        &self,
        task: &db::Task,
        workspace: &db::Workspace,
        role: &str,
        principal_id: Option<&str>,
        execution_id: &str,
        operation_key: &str,
    ) -> Result<Option<db::WorkspaceLease>> {
        let Some(repo_id) = task.repo_id.as_deref() else {
            return Ok(None);
        };
        let canonical_role = canonical_workspace_lease_role(role)?;
        // The dedicated reviewer role is independent read-only review; every
        // other resolved role is repository-mutating and stays fully gated
        // (D16, 8.2.5).
        if canonical_role == "reviewer" {
            self.ensure_task_reviewable(task).await?;
        } else {
            self.ensure_task_runnable(task).await?;
        }
        let principal_id = self
            .validate_workspace_assignment(task, role, principal_id)
            .await?;
        let (_repo, capability_class, base_ref) = self
            .workspace_lease_inputs(task, workspace, repo_id)
            .await?;

        // A lease is reusable only while every binding remains exact.  This
        // also closes the race where two launchers observe no lease and one
        // of them inserts an authority row after the other has already done
        // so: the unique active-task constraint plus the verification below
        // make the winner authoritative and the loser fail closed.
        if let Some(existing) = WorkspaceLeaseRepo::get_active_for_task(&*self.db, &task.id).await?
        {
            if !workspace_lease_expired(&existing) {
                return self
                    .verify_active_workspace_lease(
                        task,
                        workspace,
                        role,
                        Some(&principal_id),
                        execution_id,
                    )
                    .await
                    .map(Some);
            }
            if let Err(error) = WorkspaceLeaseRepo::expire(&*self.db, &now_rfc3339(), 500).await {
                tracing::warn!(lease_id = %existing.id, %error, "failed to expire stale WorkspaceLease before reissue");
            }
        }

        let issued_at = now_rfc3339();
        let expires_at =
            (Utc::now() + ChronoDuration::seconds(WORKSPACE_LEASE_SECONDS)).to_rfc3339();
        let capabilities_json = serde_json::to_string(std::slice::from_ref(&capability_class))
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let input = CreateWorkspaceLease {
            id: new_uuid_v4(),
            project_id: task.project_id.clone(),
            task_id: task.id.clone(),
            task_version: task.version,
            execution_id: execution_id.to_owned(),
            operation_idempotency_key: operation_key.to_owned(),
            repository_binding_id: repo_id.to_owned(),
            base_ref,
            role: canonical_role.to_owned(),
            capabilities_json,
            assigned_principal_type: "agent".to_owned(),
            assigned_principal_id: principal_id.clone(),
            capability_profile_revision: CAPABILITY_PROFILE_REVISION.to_owned(),
            capability_profile_digest: capability_profile_digest(&capability_class),
            // The issuer is always the internal scheduler.  The assigned
            // worker/reviewer is checked separately and is never exposed as
            // a bearer token or chat-visible lease field.
            issuing_principal_type: "system".to_owned(),
            issuing_principal_id: "task-service-scheduler".to_owned(),
            issued_at: issued_at.clone(),
            expires_at,
            created_at: issued_at.clone(),
            updated_at: issued_at,
        };
        let _lease = match WorkspaceLeaseRepo::issue(&*self.db, input).await {
            Ok(lease) => lease,
            Err(error) => {
                // Another scheduler may have won the active-task race.  Only
                // accept its row after rechecking all bindings; otherwise the
                // insert error remains a hard admission failure.
                if WorkspaceLeaseRepo::get_active_for_task(&*self.db, &task.id)
                    .await?
                    .is_some()
                {
                    return self
                        .verify_active_workspace_lease(
                            task,
                            workspace,
                            role,
                            Some(&principal_id),
                            execution_id,
                        )
                        .await
                        .map(Some);
                }
                return Err(error.into());
            }
        };
        self.verify_active_workspace_lease(task, workspace, role, Some(&principal_id), execution_id)
            .await
            .map(Some)
    }

    /// Issue a lease while the task claim transaction is still open.  The
    /// TaskRepo claim updates assignment and creates the Running execution in
    /// the same transaction; this insert therefore cannot leave an authority
    /// for an unassigned Task after a process crash.
    pub(super) async fn issue_workspace_lease_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        task: &db::Task,
        workspace: &db::Workspace,
        role: &str,
        principal_id: Option<&str>,
        execution_id: &str,
    ) -> Result<db::WorkspaceLease> {
        let Some(repo_id) = task.repo_id.as_deref() else {
            return Err(ServiceError::invalid_operation(
                "WorkspaceLease requires a repository-backed Task",
            ));
        };
        let canonical_role = canonical_workspace_lease_role(role)?;
        let principal_id = principal_id
            .or(task.assignee_id.as_deref())
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "WorkspaceLease requires an assigned Task Worker or reviewer",
                )
            })?;
        let task_row = sqlx::query(
            "SELECT t.project_id, t.repo_id, t.assignee_type, t.assignee_id,
                    t.task_type, p.charter_status, p.charter_setup_required
             FROM task t
             JOIN project p ON p.id = t.project_id
             WHERE t.id = ? AND t.deleted_at IS NULL",
        )
        .bind(&task.id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
        let assigned_type: Option<String> = task_row.get("assignee_type");
        let assigned_id: Option<String> = task_row.get("assignee_id");
        let bound_repo_id: Option<String> = task_row.get("repo_id");
        let task_type: String = task_row.get("task_type");
        let charter_backed = task_row.get::<String, _>("charter_status") == "charter_backed"
            && task_row.get::<i64, _>("charter_setup_required") == 0;
        let has_task_assignment = assigned_type.is_some() || assigned_id.is_some();
        if bound_repo_id.as_deref() != Some(repo_id) || workspace.repo_id != repo_id {
            return Err(ServiceError::invalid_operation(
                "workspace repository does not match the Task repository binding",
            ));
        }
        let role_assignment = sqlx::query(
            "SELECT assignee_type, assignee_id
             FROM task_role_assignment WHERE task_id = ? AND role_name = ?",
        )
        .bind(&task.id)
        .bind(role.trim())
        .fetch_optional(&mut **transaction)
        .await?;
        if let Some(assignment) = role_assignment {
            let assignment_type: Option<String> = assignment.get("assignee_type");
            let assignment_id: Option<String> = assignment.get("assignee_id");
            if assignment_type.as_deref() != Some("agent")
                || assignment_id.as_deref() != Some(principal_id)
            {
                return Err(ServiceError::conflict(format!(
                    "role '{}' is assigned to a different principal",
                    role.trim()
                )));
            }
        } else if (charter_backed || has_task_assignment)
            && (assigned_type.as_deref() != Some("agent")
                || assigned_id.as_deref() != Some(principal_id))
        {
            return Err(ServiceError::invalid_operation(
                "WorkspaceLease requires the lease subject to be the assigned Task Worker/reviewer",
            ));
        }
        let repo_row = sqlx::query("SELECT project_id, default_branch FROM repo WHERE id = ?")
            .bind(repo_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or_else(|| ServiceError::not_found("repo", repo_id.to_owned()))?;
        let repo_project_id: String = repo_row.get("project_id");
        let default_branch: String = repo_row.get("default_branch");
        if repo_project_id != task.project_id {
            return Err(ServiceError::invalid_operation(
                "Task repository binding belongs to a different Project",
            ));
        }
        let capability_class = sqlx::query_scalar::<_, Option<String>>(
            "SELECT capability_class FROM project_task_governance
             WHERE task_id = ? AND project_id = ?",
        )
        .bind(&task.id)
        .bind(&task.project_id)
        .fetch_optional(&mut **transaction)
        .await?
        .flatten()
        .filter(|value| !value.trim().is_empty())
        .map_or_else(|| canonical_task_capability(&task_type, None), Ok)?;
        let execution_class = classify_task_execution(&task_type, Some(&capability_class))?;
        if execution_class == TaskExecutionClass::ReadOnlyPlanning
            && !is_read_only_capability(&capability_class)
        {
            return Err(ServiceError::invalid_operation(
                "discovery/planning WorkspaceLease requires a server-approved read-only capability",
            ));
        }

        // Repeat the current-Charter predicate inside the claim transaction.
        // The migration's scope trigger is the final database backstop.
        let gate = sqlx::query(
            "SELECT p.charter_status, p.charter_setup_required,
                    p.current_charter_revision_id, g.charter_revision_id
             FROM project p
             LEFT JOIN project_task_governance g
               ON g.project_id = p.id AND g.task_id = ?
             WHERE p.id = ?",
        )
        .bind(&task.id)
        .bind(&task.project_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let charter_backed = gate.get::<String, _>("charter_status") == "charter_backed"
            && gate.get::<i64, _>("charter_setup_required") == 0;
        let charter_governed = gate
            .get::<Option<String>, _>("charter_revision_id")
            .as_deref()
            == gate
                .get::<Option<String>, _>("current_charter_revision_id")
                .as_deref();
        if charter_backed && !charter_governed {
            return Err(ServiceError::invalid_operation(
                "WorkspaceLease requires the Task's current approved Charter revision",
            ));
        }
        let issued_at = now_rfc3339();
        let expires_at =
            (Utc::now() + ChronoDuration::seconds(WORKSPACE_LEASE_SECONDS)).to_rfc3339();
        let base_ref = workspace.before_sha.clone().unwrap_or(default_branch);
        let capabilities_json = serde_json::to_string(std::slice::from_ref(&capability_class))
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let lease_id = new_uuid_v4();
        sqlx::query(
            "INSERT INTO workspace_lease (
                id, project_id, task_id, task_version, execution_id,
                operation_idempotency_key,
                repository_binding_id, base_ref, role, capabilities_json,
                assigned_principal_type, assigned_principal_id,
                capability_profile_revision, capability_profile_digest,
                issuing_principal_type, issuing_principal_id, status, issued_at,
                expires_at, revoked_at, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'agent', ?, ?, ?,
                       'system', 'task-service-scheduler', 'active', ?, ?, NULL, 1, ?, ?)",
        )
        .bind(&lease_id)
        .bind(&task.project_id)
        .bind(&task.id)
        .bind(task.version)
        .bind(execution_id)
        .bind(execution_id)
        .bind(repo_id)
        .bind(&base_ref)
        .bind(canonical_role)
        .bind(&capabilities_json)
        .bind(principal_id)
        .bind(CAPABILITY_PROFILE_REVISION)
        .bind(capability_profile_digest(&capability_class))
        .bind(&issued_at)
        .bind(&expires_at)
        .bind(&issued_at)
        .bind(&issued_at)
        .execute(&mut **transaction)
        .await
        .map_err(db::DbError::from)?;
        let row = sqlx::query(
            "SELECT id, project_id, task_id, task_version, execution_id,
                    operation_idempotency_key,
                    repository_binding_id, base_ref, role, capabilities_json,
                    assigned_principal_type, assigned_principal_id,
                    capability_profile_revision, capability_profile_digest,
                    issuing_principal_type, issuing_principal_id, status,
                    issued_at, expires_at, revoked_at, version, created_at,
                    updated_at
             FROM workspace_lease WHERE id = ?",
        )
        .bind(&lease_id)
        .fetch_one(&mut **transaction)
        .await?;
        Ok(map_workspace_lease_row(row))
    }

    pub(super) async fn verify_active_workspace_lease(
        &self,
        task: &db::Task,
        workspace: &db::Workspace,
        role: &str,
        principal_id: Option<&str>,
        execution_id: &str,
    ) -> Result<db::WorkspaceLease> {
        let repo_id = task.repo_id.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation("WorkspaceLease requires a repository-backed Task")
        })?;
        let canonical_role = canonical_workspace_lease_role(role)?;
        if canonical_role == "reviewer" {
            self.ensure_task_reviewable(task).await?;
        } else {
            self.ensure_task_runnable(task).await?;
        }
        let principal_id = self
            .validate_workspace_assignment(task, role, principal_id)
            .await?;
        let (repo, capability_class, base_ref) = self
            .workspace_lease_inputs(task, workspace, repo_id)
            .await?;
        let lease = WorkspaceLeaseRepo::get_active_for_task(&*self.db, &task.id)
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "repository execution requires an active scheduler WorkspaceLease",
                )
            })?;
        if workspace_lease_expired(&lease) {
            if let Err(error) = WorkspaceLeaseRepo::expire(&*self.db, &now_rfc3339(), 500).await {
                tracing::warn!(lease_id = %lease.id, %error, "failed to expire stale WorkspaceLease");
            }
            return Err(ServiceError::invalid_operation(
                "scheduler WorkspaceLease has expired",
            ));
        }
        let capabilities =
            serde_json::from_str::<Vec<String>>(&lease.capabilities_json).map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "invalid WorkspaceLease capability set: {error}"
                ))
            })?;
        if lease.status != "active"
            || lease.project_id != task.project_id
            || lease.task_id != task.id
            || lease.task_version != task.version
            || lease.execution_id != execution_id
            // The lease is keyed either directly on the execution (claim /
            // launch issuance) or on the version-derived reissue key minted
            // after a Task-row movement; both stay bound to this execution.
            || (lease.operation_idempotency_key != execution_id
                && lease.operation_idempotency_key
                    != workspace_lease_reissue_key(execution_id, lease.task_version))
            || lease.repository_binding_id != repo_id
            || lease.base_ref != base_ref
            || lease.role != canonical_role
            || lease.issuing_principal_type != "system"
            || lease.issuing_principal_id != "task-service-scheduler"
            || lease.assigned_principal_type != "agent"
            || lease.assigned_principal_id != principal_id
            || lease.capability_profile_revision != CAPABILITY_PROFILE_REVISION
            || lease.capability_profile_digest != capability_profile_digest(&capability_class)
            || capabilities != vec![capability_class]
            || repo.project_id != task.project_id
        {
            return Err(ServiceError::invalid_operation(
                "active WorkspaceLease does not exactly match Task execution authority",
            ));
        }
        Ok(lease)
    }

    async fn workspace_lease_inputs(
        &self,
        task: &db::Task,
        workspace: &db::Workspace,
        repo_id: &str,
    ) -> Result<(db::Repo, String, String)> {
        if workspace.repo_id != repo_id {
            return Err(ServiceError::invalid_operation(
                "workspace repository does not match the Task repository binding",
            ));
        }
        let repo = RepoRepo::get_by_id(&*self.db, repo_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("repo", repo_id.to_owned()))?;
        if repo.project_id != task.project_id {
            return Err(ServiceError::invalid_operation(
                "Task repository binding belongs to a different Project",
            ));
        }
        let capability_class = sqlx::query_scalar::<_, Option<String>>(
            "SELECT capability_class FROM project_task_governance
             WHERE task_id = ? AND project_id = ?",
        )
        .bind(&task.id)
        .bind(&task.project_id)
        .fetch_optional(self.db.pool())
        .await?
        .flatten()
        .filter(|value| !value.trim().is_empty())
        .map_or_else(|| canonical_task_capability(&task.task_type, None), Ok)?;
        let execution_class = classify_task_execution(&task.task_type, Some(&capability_class))?;
        if !is_supported_capability_profile(&capability_class) {
            return Err(ServiceError::invalid_operation(format!(
                "Task capability profile '{}' is not server-approved",
                capability_class
            )));
        }
        if execution_class == TaskExecutionClass::ReadOnlyPlanning
            && !is_read_only_capability(&capability_class)
        {
            return Err(ServiceError::invalid_operation(
                "discovery/planning WorkspaceLease requires a server-approved read-only capability",
            ));
        }
        let base_ref = workspace
            .before_sha
            .clone()
            .unwrap_or_else(|| repo.default_branch.clone());
        Ok((repo, capability_class, base_ref))
    }

    async fn validate_workspace_assignment(
        &self,
        task: &db::Task,
        role: &str,
        principal_id: Option<&str>,
    ) -> Result<String> {
        let principal_id = principal_id
            .or(task.assignee_id.as_deref())
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "WorkspaceLease requires an assigned Task Worker or reviewer",
                )
            })?;
        let canonical_role = self.canonical_execution_role_for_task(task, role).await?;
        self.ensure_repository_worker_identity(&task.project_id, principal_id)
            .await?;
        crate::ensure_execution_role_principal(
            &self.db,
            &task.project_id,
            &canonical_role,
            principal_id,
        )
        .await?;
        let charter_backed: i64 = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM project
                 WHERE id = ? AND charter_status = 'charter_backed'
                   AND charter_setup_required = 0
             )",
        )
        .bind(&task.project_id)
        .fetch_one(self.db.pool())
        .await?;
        if let Some(assignment) =
            TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, &task.id, role.trim()).await?
        {
            if assignment.assignee_type != Some(db::AssigneeKind::Agent)
                || assignment.assignee_id.as_deref() != Some(principal_id)
            {
                return Err(ServiceError::conflict(format!(
                    "role '{}' is assigned to a different principal",
                    role.trim()
                )));
            }
            return Ok(principal_id.to_owned());
        }
        let has_task_assignment = task.assignee_type.is_some() || task.assignee_id.is_some();
        if (charter_backed == 1 || has_task_assignment)
            && (task.assignee_type.as_deref() != Some("agent")
                || task.assignee_id.as_deref() != Some(principal_id))
        {
            return Err(ServiceError::invalid_operation(
                "WorkspaceLease requires the lease subject to be the assigned Task Worker/reviewer",
            ));
        }
        Ok(principal_id.to_owned())
    }

    async fn canonical_execution_role_for_task(
        &self,
        task: &db::Task,
        requested_role: &str,
    ) -> Result<String> {
        let requested_role = requested_role.trim();
        if requested_role.is_empty() {
            return Err(ServiceError::invalid_operation(
                "WorkspaceLease requires a non-empty execution role",
            ));
        }
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Executor),
        );
        if workflow
            .states
            .iter()
            .any(|state| crate::workflow::effective_role(state) == Some(requested_role))
        {
            return Ok(requested_role.to_owned());
        }

        // Interactive/executor launch APIs carry a transport role rather than
        // a workflow role. Resolve it to the current state before checking the
        // canonical Project Worker/reviewer assignment.
        if matches!(requested_role, "interactive" | "executor") {
            if let Some(state) = workflow
                .states
                .iter()
                .find(|state| state.name == task.status)
            {
                if let Some(role) = crate::workflow::effective_role(state) {
                    return Ok(role.to_owned());
                }
            }
        }
        Ok(requested_role.to_owned())
    }

    pub(super) async fn verify_execution_workspace_authority(
        &self,
        execution: &db::Execution,
    ) -> Result<Option<db::WorkspaceLease>> {
        let task = TaskRepo::get_by_id(&*self.db, &execution.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", execution.task_id.clone()))?;
        let Some(workspace_id) = execution.workspace_id.as_deref() else {
            if task.repo_id.is_some() {
                return Err(ServiceError::invalid_operation(
                    "repository execution requires a scheduler WorkspaceLease-backed workspace",
                ));
            }
            return Ok(None);
        };
        let workspace = WorkspaceRepo::get_by_id(&*self.db, workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", workspace_id.to_owned()))?;
        self.verify_active_workspace_lease(
            &task,
            &workspace,
            &execution.role,
            execution.agent_id.as_deref(),
            &execution.id,
        )
        .await
        .map(Some)
    }

    /// Verify a running execution's active WorkspaceLease and, when the
    /// exact-match verification fails, recover once through the normal
    /// issuance path instead of hard-failing the dispatch.
    ///
    /// Any Task-row mutation between lease issuance and this re-verify — a
    /// role handoff, a retry-metadata clear, a concurrent transition — bumps
    /// `task.version` and makes the stale lease fail the exact match even
    /// though the execution's authority is otherwise intact. Recovery stays
    /// fail-closed: only a lease held by this execution is revoked, the
    /// reissue runs the full issuance validation (assignment authority is
    /// re-resolved fresh, and issuance verifies the new lease), and exactly
    /// one reissue attempt is made.
    ///
    /// Returns the fresh Task row the successful verification was performed
    /// against, so callers do not keep using a stale snapshot.
    pub(super) async fn verify_or_reissue_active_workspace_lease(
        &self,
        task: db::Task,
        workspace: &db::Workspace,
        role: &str,
        principal_id: Option<&str>,
        execution_id: &str,
    ) -> Result<db::Task> {
        let verify_error = match self
            .verify_active_workspace_lease(&task, workspace, role, principal_id, execution_id)
            .await
        {
            Ok(_lease) => return Ok(task),
            Err(error) => error,
        };
        let Some(stale) = WorkspaceLeaseRepo::get_active_for_task(&*self.db, &task.id).await?
        else {
            return Err(verify_error);
        };
        if stale.execution_id != execution_id {
            // Another execution owns the Task's active authority; this
            // attempt genuinely lost the race and must fail closed.
            return Err(verify_error);
        }
        tracing::info!(
            task_id = %task.id,
            execution_id,
            lease_id = %stale.id,
            %verify_error,
            "reissuing this execution's WorkspaceLease after the Task row moved during dispatch"
        );
        self.revoke_workspace_lease(&stale).await;
        let fresh_task = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
        // The reissue keeps the execution binding but must mint a distinct
        // idempotency key: the key column is globally UNIQUE (revoked rows
        // included) and lease rows are immutable, so the revoked lease's key
        // can never be reused. Verification accepts the derived key form.
        self.issue_workspace_lease_with_operation_key(
            &fresh_task,
            workspace,
            role,
            principal_id,
            execution_id,
            &workspace_lease_reissue_key(execution_id, fresh_task.version),
        )
        .await?
        .ok_or_else(|| {
            ServiceError::invalid_operation(
                "WorkspaceLease reissue did not produce an active lease",
            )
        })?;
        Ok(fresh_task)
    }

    /// `verify_execution_workspace_authority` with the one-shot stale-lease
    /// reissue above. Used by `run_execution`'s in-flight re-verifications;
    /// the initial `start_execution` admission keeps the strict verify.
    pub(super) async fn verify_or_reissue_execution_workspace_authority(
        &self,
        execution: &db::Execution,
    ) -> Result<()> {
        let task = TaskRepo::get_by_id(&*self.db, &execution.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", execution.task_id.clone()))?;
        let Some(workspace_id) = execution.workspace_id.as_deref() else {
            if task.repo_id.is_some() {
                return Err(ServiceError::invalid_operation(
                    "repository execution requires a scheduler WorkspaceLease-backed workspace",
                ));
            }
            return Ok(());
        };
        let workspace = WorkspaceRepo::get_by_id(&*self.db, workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", workspace_id.to_owned()))?;
        self.verify_or_reissue_active_workspace_lease(
            task,
            &workspace,
            &execution.role,
            execution.agent_id.as_deref(),
            &execution.id,
        )
        .await
        .map(|_task| ())
    }

    pub(super) async fn revoke_workspace_lease(&self, lease: &db::WorkspaceLease) {
        if let Err(error) =
            WorkspaceLeaseRepo::revoke(&*self.db, &lease.id, lease.version, &now_rfc3339()).await
        {
            tracing::warn!(
                lease_id = %lease.id,
                %error,
                "failed to revoke WorkspaceLease after execution admission failure"
            );
        }
    }
}

/// Idempotency key for a lease reissued after the Task row moved.
/// `operation_idempotency_key` is globally UNIQUE across every lease row
/// (revoked included), so a reissue for the same execution needs a distinct,
/// deterministic key; deriving it from the verified Task version keeps one
/// reissue identity per (execution, task-version) pair.
fn workspace_lease_reissue_key(execution_id: &str, task_version: i64) -> String {
    format!("{execution_id}::reissue::v{task_version}")
}

/// Whether a recorded conflict is neutral to independent read-only review
/// (D16, 8.2.5): its affected paths never touch a fixed outcome, acceptance,
/// risk, side-effect, release, or elevated-authority boundary. A conflict
/// limited to governance-pointer fields (`charter_revision_id`,
/// `charter_revision_id`, ...) does not change what a reviewer is evaluating
/// in an already-committed result.
fn review_neutral(conflict: &ReconciliationConflictRow) -> bool {
    !conflict
        .affected_paths
        .iter()
        .any(|path| FIXED_BOUNDARY_FIELDS.contains(&path.as_str()))
}

fn canonical_workspace_lease_role(role: &str) -> Result<&'static str> {
    match role.trim() {
        "reviewer" => Ok("reviewer"),
        // Workflow role names are user-configurable. Every scheduler-resolved
        // execution role other than the dedicated reviewer role is a bounded
        // Task Worker for lease purposes; the exact original role still has
        // to match the authoritative Task role assignment.
        _ => Ok("worker"),
    }
}

fn workspace_lease_expired(lease: &db::WorkspaceLease) -> bool {
    DateTime::parse_from_rfc3339(&lease.expires_at)
        .map(|expires_at| expires_at.with_timezone(&Utc) <= Utc::now())
        .unwrap_or(true)
}

fn capability_profile_digest(capability_class: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(CAPABILITY_PROFILE_REVISION.as_bytes());
    digest.update([0]);
    digest.update(capability_class.as_bytes());
    format!("sha256:{}", hex::encode(digest.finalize()))
}

/// The closed set of capability profiles the scheduler can turn into a
/// WorkspaceLease. Baselines may author narrower subsets, but nothing outside
/// this list is ever dispatchable, so Task admission validates against it up
/// front (`require_server_approved_capability_class`).
fn is_supported_capability_profile(capability_class: &str) -> bool {
    SUPPORTED_CAPABILITY_PROFILES.contains(&capability_class)
}

/// Reject a requested capability_class the lease issuer would refuse later.
/// Without this, a baseline authoring its own class vocabulary (e.g.
/// "implementation") admits the Task at creation and then every dispatch
/// fails; the authoring agent never sees a correctable error.
fn require_server_approved_capability_class(requested: Option<&str>) -> Result<()> {
    let Some(capability_class) = requested.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    if !is_supported_capability_profile(capability_class) {
        return Err(ServiceError::invalid_operation(format!(
            "Task capability_class '{}' is not server-approved; allowed values: {}",
            capability_class,
            SUPPORTED_CAPABILITY_PROFILES.join(", ")
        )));
    }
    Ok(())
}

fn map_workspace_lease_row(row: sqlx::sqlite::SqliteRow) -> db::WorkspaceLease {
    db::WorkspaceLease {
        id: row.get("id"),
        project_id: row.get("project_id"),
        task_id: row.get("task_id"),
        task_version: row.get("task_version"),
        execution_id: row.get("execution_id"),
        operation_idempotency_key: row.get("operation_idempotency_key"),
        repository_binding_id: row.get("repository_binding_id"),
        base_ref: row.get("base_ref"),
        role: row.get("role"),
        capabilities_json: row.get("capabilities_json"),
        assigned_principal_type: row.get("assigned_principal_type"),
        assigned_principal_id: row.get("assigned_principal_id"),
        capability_profile_revision: row.get("capability_profile_revision"),
        capability_profile_digest: row.get("capability_profile_digest"),
        issuing_principal_type: row.get("issuing_principal_type"),
        issuing_principal_id: row.get("issuing_principal_id"),
        status: row.get("status"),
        issued_at: row.get("issued_at"),
        expires_at: row.get("expires_at"),
        revoked_at: row.get("revoked_at"),
        version: row.get("version"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

async fn validate_document_revisions(
    pool: &sqlx::SqlitePool,
    project_id: &str,
    requested: &[String],
) -> Result<()> {
    for revision_id in requested {
        if revision_id.trim().is_empty() {
            return Err(ServiceError::invalid_operation(
                "Task Document revision reference is empty",
            ));
        }
        let row = sqlx::query(
            "SELECT d.project_id, r.lifecycle
             FROM project_document_revision r
             JOIN project_document d ON d.id = r.document_id
             WHERE r.id = ?",
        )
        .bind(revision_id)
        .fetch_optional(pool)
        .await?;
        let Some(row) = row else {
            return Err(ServiceError::invalid_operation(
                "Task references a missing Project Document revision",
            ));
        };
        let owning_project: String = row.get("project_id");
        let lifecycle: String = row.get("lifecycle");
        if owning_project != project_id || lifecycle != "approved" {
            return Err(ServiceError::invalid_operation(
                "Task Document revisions must be approved and belong to the same Project",
            ));
        }
    }
    Ok(())
}

fn build_provenance(requested: Option<Value>, plan_item_id: Option<&str>) -> Result<String> {
    let mut map = match requested {
        None => Map::new(),
        Some(Value::Object(map)) => map,
        Some(_) => {
            return Err(ServiceError::invalid_operation(
                "Task governance provenance must be a JSON object",
            ));
        }
    };
    if map.contains_key("fixed_risk_class") {
        return Err(ServiceError::invalid_operation(
            "singular fixed_risk_class provenance is unsupported",
        ));
    }
    let required = [("origin_plan_item_id", plan_item_id)];
    for (key, value) in required {
        if let Some(value) = value {
            if let Some(existing) = map.get(key).and_then(Value::as_str) {
                if existing != value {
                    return Err(ServiceError::invalid_operation(format!(
                        "Task governance provenance {key} does not match the authoritative reference"
                    )));
                }
            }
            map.insert(key.to_owned(), Value::String(value.to_owned()));
        }
    }
    // The approved Charter is what authorizes a Task. This marker records
    // that authority on the governance row itself.
    map.insert("charter_authority".to_owned(), Value::Bool(true));
    map.insert(
        "schema".to_owned(),
        Value::String("forge.task-governance/v1".to_owned()),
    );
    serde_json::to_string(&Value::Object(map))
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use events::EventBus;
    use std::sync::Arc;

    #[test]
    fn workspace_lease_roles_preserve_reviewer_and_bound_custom_workers() {
        assert_eq!(
            canonical_workspace_lease_role("reviewer").expect("reviewer role"),
            "reviewer"
        );
        assert_eq!(
            canonical_workspace_lease_role("implementer").expect("custom worker role"),
            "worker"
        );
        assert_eq!(
            canonical_workspace_lease_role("orchestrator").expect("workflow worker role"),
            "worker"
        );
    }
    fn charter_backed_project() -> db::Project {
        db::Project {
            id: "project-1".to_owned(),
            name: "Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            workflow_template_name: None,
            primary_repo_id: Some("repo-1".to_owned()),
            paused_at: None,
            system_pause_reason: None,
            owner_id: None,
            project_hooks_json: "[]".to_owned(),
            project_work_epoch: 0,
            charter_status: "charter_backed".to_owned(),
            charter_setup_required: false,
            current_charter_id: Some("charter-1".to_owned()),
            current_charter_revision_id: Some("charter-revision-1".to_owned()),
            current_charter_version: 1,
            primary_milestone_id: Some("milestone-1".to_owned()),
            version: 1,
            created_at: "2026-08-13T00:00:00Z".to_owned(),
            updated_at: "2026-08-13T00:00:00Z".to_owned(),
        }
    }

    #[tokio::test]
    async fn charter_backed_repository_task_is_runnable_without_a_baseline() {
        let pool = db::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        let service = TaskService::new(
            Arc::new(db::SqliteDb::new(pool)),
            Arc::new(EventBus::new(4)),
        );
        let governance = service
            .prepare_task_governance(
                &charter_backed_project(),
                Some(&"repo-1".to_owned()),
                "task",
                None,
            )
            .await
            .expect("implementation task can be recorded before the baseline")
            .expect("repository task receives a governance row");
        assert!(governance.runnable);
        assert_eq!(
            governance.charter_revision_id.as_deref(),
            Some("charter-revision-1")
        );
        assert!(governance.capability_class.is_none());
        assert!(governance.risk_class.is_none());
        assert!(governance.provenance_json.contains("charter_authority"));
    }

    fn minimal_task(id: &str, project_id: &str) -> db::Task {
        db::Task {
            id: id.to_owned(),
            project_id: project_id.to_owned(),
            repo_id: None,
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "Task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "todo".to_owned(),
            is_automation: false,
            priority: 1,
            board_position: 0.0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            metadata_json: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            entry_barrier_json: None,
            review_passed_at: None,
            archived_at: None,
            deleted_at: None,
            version: 1,
            created_at: "2026-08-13T00:00:00Z".to_owned(),
            updated_at: "2026-08-13T00:00:00Z".to_owned(),
        }
    }

    /// D14/8.1.4 characterization: a malformed or unknown adaptive operation
    /// name (for example a command name such as `task.propose` mistakenly
    /// passed as an operation) is a typed `validation_error`, never durable
    /// conflict/reconciliation truth. Parsing happens before any governance
    /// lookup, so this holds even for a Task/Project that do not exist.
    #[tokio::test]
    async fn authorize_adaptive_task_operation_rejects_malformed_verb_as_validation_error() {
        let pool = db::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        db::run_migrations(&pool).await.expect("migrations");
        let db = Arc::new(db::SqliteDb::new(pool));
        let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(4)));
        let task = minimal_task("task-does-not-exist", "project-does-not-exist");

        let error = service
            .authorize_adaptive_task_operation(&task, "task.propose")
            .await
            .expect_err("a command name is not an adaptive verb");
        match error {
            ServiceError::InvalidOperation { message } => {
                assert!(message.contains("task.propose"));
                assert!(message.contains("split"));
                assert!(message.contains("sequence"));
                assert!(message.contains("replace"));
                assert!(!message.contains("reconciliation_required"));
            }
            other => panic!("expected InvalidOperation (validation_error), got {other:?}"),
        }
        let reconciliation_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM project_reconciliation_record")
                .fetch_one(db.pool())
                .await
                .expect("reconciliation count");
        let conflict_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM project_canonical_conflict")
                .fetch_one(db.pool())
                .await
                .expect("conflict count");
        assert_eq!(reconciliation_count, 0);
        assert_eq!(conflict_count, 0);
    }

    #[tokio::test]
    async fn charter_backed_repository_planning_task_is_runnable_read_only_work() {
        let pool = db::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        let service = TaskService::new(
            Arc::new(db::SqliteDb::new(pool)),
            Arc::new(EventBus::new(4)),
        );
        let governance = service
            .prepare_task_governance(
                &charter_backed_project(),
                Some(&"repo-1".to_owned()),
                "discovery",
                None,
            )
            .await
            .expect("discovery plan can be recorded before baseline")
            .expect("repository discovery receives a governance row");
        assert!(governance.runnable);
        assert_eq!(
            governance.capability_class.as_deref(),
            Some("repository_read")
        );
        assert_eq!(governance.risk_class.as_deref(), Some("low"));
        assert!(governance.provenance_json.contains("charter_authority"));
    }

    #[tokio::test]
    async fn pre_baseline_repository_planning_cannot_claim_write_capability() {
        let pool = db::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        let service = TaskService::new(
            Arc::new(db::SqliteDb::new(pool)),
            Arc::new(EventBus::new(4)),
        );
        let error = service
            .prepare_task_governance(
                &charter_backed_project(),
                Some(&"repo-1".to_owned()),
                "planning_task",
                Some(TaskGovernanceRequest {
                    charter_revision_id: Some("charter-revision-1".to_owned()),
                    plan_item_id: None,
                    milestone_id: None,
                    document_revision_ids: Vec::new(),
                    capability_class: Some("repository_write".to_owned()),
                    risk_class: Some("low".to_owned()),
                    provenance: None,
                }),
            )
            .await
            .expect_err("pre-baseline planning must be read-only");
        assert!(error.to_string().contains("read-only"));
    }

    #[test]
    fn capability_class_must_be_server_approved() {
        assert!(require_server_approved_capability_class(None).is_ok());
        assert!(require_server_approved_capability_class(Some("")).is_ok());
        for approved in SUPPORTED_CAPABILITY_PROFILES {
            assert!(require_server_approved_capability_class(Some(approved)).is_ok());
        }
        let error = require_server_approved_capability_class(Some("implementation"))
            .expect_err("baseline-authored classes outside the server set are rejected");
        let message = error.to_string();
        assert!(message.contains("'implementation'"));
        for approved in SUPPORTED_CAPABILITY_PROFILES {
            assert!(
                message.contains(approved),
                "error must enumerate allowed value {approved}: {message}"
            );
        }
    }

    #[tokio::test]
    async fn unapproved_capability_class_is_rejected_at_task_creation() {
        let pool = db::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        let service = TaskService::new(
            Arc::new(db::SqliteDb::new(pool)),
            Arc::new(EventBus::new(4)),
        );
        let error = service
            .prepare_task_governance(
                &charter_backed_project(),
                Some(&"repo-1".to_owned()),
                "task",
                Some(TaskGovernanceRequest {
                    charter_revision_id: Some("charter-revision-1".to_owned()),
                    plan_item_id: None,
                    milestone_id: None,
                    document_revision_ids: Vec::new(),
                    capability_class: Some("implementation".to_owned()),
                    risk_class: None,
                    provenance: None,
                }),
            )
            .await
            .expect_err("a class the lease issuer would refuse fails at creation");
        let message = error.to_string();
        assert!(message.contains("not server-approved"));
        assert!(message.contains("repository_write"));
    }

    // -- Capability-aware review (D16/D17, 8.2.5) --
    //
    // Repository mutation and independent read-only review are evaluated
    // separately against the exact same recorded conflicts. Review of an
    // already-committed result may continue only while every currently
    // applicable conflict is acceptance/evidence/risk/reviewer/release
    // neutral (`review_neutral`); repository mutation stays fully gated by
    // any outstanding conflict regardless of neutrality.

    fn reconciliation_conflict_row(
        conflict_code: &str,
        affected_paths: &[&str],
    ) -> ReconciliationConflictRow {
        ReconciliationConflictRow {
            reconciliation_id: "reconciliation-1".to_owned(),
            conflict_code: conflict_code.to_owned(),
            description: format!("{conflict_code} for task-1"),
            record_type: "task".to_owned(),
            record_id: "task-1".to_owned(),
            governing_record_type: "execution_baseline".to_owned(),
            governing_record_id: "baseline-1".to_owned(),
            affected_paths: affected_paths
                .iter()
                .map(|path| (*path).to_owned())
                .collect(),
            updated_at: "2026-08-13T00:00:00Z".to_owned(),
        }
    }

    #[test]
    fn review_neutral_admits_plan_paths_but_blocks_every_fixed_boundary_field() {
        assert!(review_neutral(&reconciliation_conflict_row(
            "adaptive_task_governance_stale",
            &["plan_items", "milestone_id"],
        )));

        for field in FIXED_BOUNDARY_FIELDS {
            let blocking = reconciliation_conflict_row("adaptive_task_governance_stale", &[field]);
            assert!(
                !review_neutral(&blocking),
                "a conflict touching '{field}' must not be review-neutral"
            );
        }
    }

    /// Insert one Task-scoped canonical conflict + required reconciliation
    /// record directly (mirroring what `record_adaptive_boundary_reconciliation`
    /// persists), so `ensure_capability_permits_execution` reads it back
    /// through the same `required_reconciliation_conflicts` query the rest of
    /// D16/D17 relies on.
    async fn insert_task_scoped_conflict(
        db: &db::SqliteDb,
        project_id: &str,
        task_id: &str,
        conflict_code: &str,
        affected_paths: &[&str],
    ) {
        let now = now_rfc3339();
        let conflict_id = format!("{task_id}-conflict");
        sqlx::query(
            "INSERT INTO project_canonical_conflict (
                 id, project_id, domain, governing_record_type, governing_record_id,
                 governing_record_revision, governing_record_digest,
                 conflicting_record_type, conflicting_record_id, conflicting_record_revision,
                 conflicting_record_digest, affected_paths_json, conflict_code, description,
                 detected_by_type, detected_by_id, authorization_basis, authorization_action,
                 explicit_event, authorization_occurred_at, idempotency_key, created_at
             ) VALUES (?, ?, 'execution', 'execution_baseline', 'baseline-1', '1',
                       'test-baseline-content', 'task', ?, '1', 'task-digest', ?, ?, ?, 'system',
                       'test-fixture', 'adaptive_task_boundary', 'task.adaptive.reject',
                       'task.adaptive.split.rejected', ?, ?, ?)",
        )
        .bind(&conflict_id)
        .bind(project_id)
        .bind(task_id)
        .bind(serde_json::to_string(affected_paths).expect("affected paths encode"))
        .bind(conflict_code)
        .bind(format!("{conflict_code} for {task_id}"))
        .bind(&now)
        .bind(format!("{task_id}-idempotency"))
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("canonical conflict inserts");
        sqlx::query(
            "INSERT INTO project_reconciliation_record (
                 id, project_id, conflict_id, record_type, record_id, record_revision,
                 record_digest, governing_record_type, governing_record_id,
                 governing_record_revision, governing_record_digest, state,
                 current_resolution_id, version, created_at, updated_at
             ) VALUES (?, ?, ?, 'task', ?, '1', 'task-digest', 'execution_baseline', 'baseline-1',
                       '1', 'test-baseline-content', 'required', NULL, 1, ?, ?)",
        )
        .bind(format!("{task_id}-reconciliation"))
        .bind(project_id)
        .bind(&conflict_id)
        .bind(task_id)
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("reconciliation record inserts");
    }

    async fn capability_review_fixture(
        project_id: &str,
        task_id: &str,
    ) -> (Arc<db::SqliteDb>, TaskService, db::Task) {
        let pool = db::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        db::run_migrations(&pool).await.expect("migrations");
        let db = Arc::new(db::SqliteDb::new(pool));
        let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(4)));
        let now = now_rfc3339();
        db::ProjectRepo::create(
            &*db,
            db::CreateProject {
                id: project_id.to_owned(),
                name: "Capability review project".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("project creates");
        let task = minimal_task(task_id, project_id);
        (db, service, task)
    }

    #[tokio::test]
    async fn read_only_review_continues_past_a_review_neutral_conflict_that_blocks_mutation() {
        let (db, service, task) =
            capability_review_fixture("capability-review-neutral", "task-1").await;
        insert_task_scoped_conflict(
            &db,
            &task.project_id,
            &task.id,
            "adaptive_task_governance_stale",
            &["plan_items"],
        )
        .await;

        // Repository mutation stays blocked by the recorded conflict even
        // though the Project gate itself reports `Active`.
        let mutation_error = service
            .ensure_capability_permits_execution(
                &task,
                ExecutionGateCapability::RepositoryMutation,
                ExecutionGate::Active,
            )
            .await
            .expect_err("repository mutation stays blocked by the recorded conflict");
        assert!(matches!(mutation_error, ServiceError::Conflict(_)));

        // Independent read-only review of the already-committed result
        // continues: the conflict never touches an acceptance/evidence/
        // risk/reviewer/release boundary field (D16/8.2.5, F11).
        service
            .ensure_capability_permits_execution(
                &task,
                ExecutionGateCapability::ReadOnlyReview,
                ExecutionGate::Active,
            )
            .await
            .expect("a review-neutral conflict must never block independent review");
    }

    #[tokio::test]
    async fn read_only_review_is_blocked_when_the_conflict_affects_a_fixed_boundary_field() {
        let (db, service, task) =
            capability_review_fixture("capability-review-blocking", "task-1").await;
        insert_task_scoped_conflict(
            &db,
            &task.project_id,
            &task.id,
            "adaptive_task_governance_stale",
            &["fixed_acceptance"],
        )
        .await;

        let review_error = service
            .ensure_capability_permits_execution(
                &task,
                ExecutionGateCapability::ReadOnlyReview,
                ExecutionGate::Active,
            )
            .await
            .expect_err("a conflict touching acceptance must block independent review too");
        match review_error {
            ServiceError::Conflict(message) => {
                assert!(message.contains("review_blocked"), "message: {message}")
            }
            other => panic!("expected a review_blocked Conflict, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_only_review_does_not_require_a_baseline() {
        let (_db, service, task) =
            capability_review_fixture("capability-review-ungoverned", "task-1").await;

        service
            .ensure_capability_permits_execution(
                &task,
                ExecutionGateCapability::ReadOnlyReview,
                ExecutionGate::PreBaselineReadOnly,
            )
            .await
            .expect("Charter-governed work is reviewable without a baseline");
    }
}
