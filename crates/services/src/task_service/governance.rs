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
use api_types::{
    AdaptiveEnvelope, ExecutionGate, ExecutionSetupState, RetryAction, SetupRequirement,
    TaskGovernanceRequest,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use db::{CreateProjectCanonicalConflict, CreateProjectReconciliation, ProjectOrchestrationRepo};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use sqlx::{Row, Sqlite, Transaction};

const WORKSPACE_LEASE_SECONDS: i64 = 15 * 60;
const CAPABILITY_PROFILE_REVISION: &str = "forge.capability-profile/v1";
const FIXED_BOUNDARY_DIGEST_SCHEMA: &str = "forge.task-governance/fixed-boundary/v1";
const ADAPTIVE_ENVELOPE_DIGEST_SCHEMA: &str = "forge.task-governance/adaptive-envelope/v1";

/// Immutable governance facts inherited by an adaptive Task reshape.
///
/// The baseline revision is the authority for the envelope and its fixed
/// boundaries.  A split/sequence/replace operation may only reuse this
/// snapshot; it cannot manufacture a new one from caller text.
#[derive(Debug, Clone)]
pub(crate) struct AdaptiveTaskGovernance {
    pub charter_revision_id: Option<String>,
    pub baseline_id: Option<String>,
    pub baseline_revision_id: Option<String>,
    pub plan_item_id: Option<String>,
    pub milestone_id: Option<String>,
    pub document_revisions_json: String,
    pub capability_class: Option<String>,
    pub risk_class: Option<String>,
    pub provenance_json: String,
    pub adaptive_envelope_json: Option<String>,
    pub baseline_content_digest: Option<String>,
}

#[derive(Debug)]
pub(super) struct PreparedTaskGovernance {
    pub charter_revision_id: Option<String>,
    pub baseline_id: Option<String>,
    pub baseline_revision_id: Option<String>,
    pub plan_item_id: Option<String>,
    pub milestone_id: Option<String>,
    pub document_revisions_json: String,
    pub capability_class: Option<String>,
    pub risk_class: Option<String>,
    pub runnable: bool,
    pub replacement_of_task_id: Option<String>,
    pub provenance_json: String,
}

struct BaselineContext {
    lifecycle: String,
    current_revision_id: Option<String>,
    revision_lifecycle: String,
    charter_revision_id: String,
    document_revisions_json: String,
    plan_items_json: String,
    milestone_id: Option<String>,
    milestone_ids_json: String,
    milestone_definition_revision_ids_json: String,
    primary_milestone_id: Option<String>,
    capability_classes_json: String,
    risk_classes_json: String,
    adaptive_envelope_json: String,
    elevated_operations_json: String,
    release_policy_revision: String,
    release_policy_digest: String,
    content_digest: String,
    rendered_digest: String,
}

fn parse_adaptive_task_envelope(value: &str) -> Result<AdaptiveEnvelope> {
    let value: Value = serde_json::from_str(value).map_err(|error| {
        ServiceError::Conflict(format!(
            "reconciliation_required: governing adaptive envelope is invalid: {error}"
        ))
    })?;
    let object = value.as_object().ok_or_else(|| {
        ServiceError::Conflict(
            "reconciliation_required: governing adaptive envelope must be an object".to_owned(),
        )
    })?;
    const FIELDS: [&str; 6] = [
        "allowed_task_operations",
        "fixed_outcomes",
        "fixed_acceptance",
        "fixed_risk_classes",
        "forbidden_side_effects",
        "elevated_operations",
    ];
    if object.len() != FIELDS.len() || FIELDS.iter().any(|field| !object.contains_key(*field)) {
        return Err(ServiceError::Conflict(
            "reconciliation_required: governing adaptive envelope is incomplete".to_owned(),
        ));
    }
    if FIELDS.iter().any(|field| {
        !object.get(*field).is_some_and(Value::is_array)
            || object
                .get(*field)
                .and_then(Value::as_array)
                .is_some_and(|values| values.iter().any(|value| !value.is_string()))
    }) {
        return Err(ServiceError::Conflict(
            "reconciliation_required: governing adaptive envelope contains a non-string boundary"
                .to_owned(),
        ));
    }
    serde_json::from_value(value).map_err(|error| {
        ServiceError::Conflict(format!(
            "reconciliation_required: governing adaptive envelope is invalid: {error}"
        ))
    })
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
            "SELECT g.project_id, g.charter_revision_id,
                    g.baseline_id, g.baseline_revision_id, g.plan_item_id,
                    g.milestone_id, g.document_revisions_json,
                    g.capability_class, g.risk_class, g.runnable,
                    g.provenance_json,
                    r.adaptive_envelope_json,
                    r.content_digest AS baseline_content_digest
             FROM project_task_governance g
             LEFT JOIN project_execution_baseline_revision r
               ON r.id = g.baseline_revision_id
              AND r.baseline_id = g.baseline_id
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
            baseline_id: row.get("baseline_id"),
            baseline_revision_id: row.get("baseline_revision_id"),
            plan_item_id: row.get("plan_item_id"),
            milestone_id: row.get("milestone_id"),
            document_revisions_json: row.get("document_revisions_json"),
            capability_class: row.get("capability_class"),
            risk_class: row.get("risk_class"),
            provenance_json: row.get("provenance_json"),
            adaptive_envelope_json: row.get("adaptive_envelope_json"),
            baseline_content_digest: row.get("baseline_content_digest"),
        }))
    }

    /// Admit one adaptive operation at the shared TaskService boundary.
    /// Legacy Tasks have no governing baseline and retain their historical
    /// behavior.  Charter-backed Tasks require an active envelope entry and
    /// an empty reconciliation projection before they can be reshaped.
    pub(crate) async fn authorize_adaptive_task_operation(
        &self,
        task: &db::Task,
        operation: &str,
    ) -> Result<Option<AdaptiveTaskGovernance>> {
        if operation.trim().is_empty() {
            return Err(ServiceError::invalid_operation(
                "adaptive Task operation is required",
            ));
        }
        let Some(governance) = self.adaptive_task_governance(task).await? else {
            return Ok(None);
        };
        // A pre-baseline planning row is not an executable adaptive envelope.
        // Keep it on the existing planning path; repository-capable reshaping
        // still requires the exact baseline references below.
        let Some(baseline_id) = governance.baseline_id.as_deref() else {
            return Ok(Some(governance));
        };
        let Some(baseline_revision_id) = governance.baseline_revision_id.as_deref() else {
            return Ok(Some(governance));
        };
        let admitted: i64 = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                 FROM project p
                 JOIN project_execution_baseline b
                   ON b.id = ? AND b.project_id = p.id
                 JOIN project_execution_baseline_revision r
                   ON r.id = ? AND r.baseline_id = b.id
                 WHERE p.id = ?
                   AND p.current_charter_revision_id = ?
                   AND b.lifecycle = 'active'
                   AND b.current_revision_id = r.id
                   AND r.lifecycle = 'approved'
                   AND EXISTS (
                       SELECT 1
                       FROM project_execution_baseline_approval a
                       WHERE a.baseline_id = b.id AND a.revision_id = r.id
                         AND a.principal_type = 'user'
                         AND a.authorization_action = 'project.execution_baseline.approve'
                         AND a.content_digest = r.content_digest
                         AND a.rendered_digest = r.rendered_digest
                         AND a.lifecycle IN ('active', 'consumed')
                   )
             )",
        )
        .bind(baseline_id)
        .bind(baseline_revision_id)
        .bind(&task.project_id)
        .bind(governance.charter_revision_id.as_deref())
        .fetch_one(self.db.pool())
        .await?;
        if admitted != 1 {
            let reason =
                "adaptive Task operation references a stale or unapproved execution baseline";
            self.record_adaptive_boundary_reconciliation(
                task,
                &governance,
                baseline_id,
                baseline_revision_id,
                operation,
                reason,
            )
            .await?;
            return Err(ServiceError::Conflict(format!(
                "reconciliation_required: {reason}"
            )));
        }
        let unresolved: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM project_reconciliation_record
             WHERE project_id = ? AND state = 'required'",
        )
        .bind(&task.project_id)
        .fetch_one(self.db.pool())
        .await?;
        if unresolved > 0 {
            return Err(ServiceError::Conflict(
                "reconciliation_required: resolve the Project's active adaptive boundary conflict before reshaping Tasks"
                    .to_owned(),
            ));
        }
        let envelope_json = governance
            .adaptive_envelope_json
            .as_deref()
            .ok_or_else(|| {
                ServiceError::Conflict(
                    "reconciliation_required: governing baseline adaptive envelope is unavailable"
                        .to_owned(),
                )
            })?;
        let envelope = parse_adaptive_task_envelope(envelope_json)?;
        if envelope
            .allowed_task_operations
            .iter()
            .any(|allowed| allowed == operation)
        {
            return Ok(Some(governance));
        }
        let reason =
            format!("adaptive Task operation '{operation}' is outside the approved envelope");
        self.record_adaptive_boundary_reconciliation(
            task,
            &governance,
            baseline_id,
            baseline_revision_id,
            operation,
            &reason,
        )
        .await?;
        Err(ServiceError::Conflict(format!(
            "reconciliation_required: {reason}"
        )))
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
        let Some(baseline_id) = governance.baseline_id.as_deref() else {
            return Ok(());
        };
        let source_provenance: Value =
            serde_json::from_str(&governance.provenance_json).map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "Task governance provenance is invalid: {error}"
                ))
            })?;
        let requested_provenance = requested.provenance.as_ref();
        const FIXED_BOUNDARY_FIELDS: [&str; 8] = [
            "fixed_outcomes",
            "fixed_acceptance",
            "fixed_risk_classes",
            "forbidden_side_effects",
            "release_policy_revision",
            "release_policy_digest",
            "elevated_operations",
            "fixed_boundary_digest",
        ];
        let provenance_boundary_mismatch = requested_provenance
            .and_then(Value::as_object)
            .is_some_and(|requested| {
                FIXED_BOUNDARY_FIELDS.iter().any(|field| {
                    requested.get(*field).is_some()
                        && source_provenance.get(*field) != requested.get(*field)
                })
            });
        let boundary_mismatch = requested.baseline_id.as_deref() != Some(baseline_id)
            || requested.baseline_revision_id.as_deref()
                != governance.baseline_revision_id.as_deref()
            || requested.charter_revision_id.as_deref()
                != governance.charter_revision_id.as_deref()
            || requested.plan_item_id.as_deref() != governance.plan_item_id.as_deref()
            || requested.milestone_id.as_deref() != governance.milestone_id.as_deref()
            || serde_json::to_string(&requested.document_revision_ids)
                .ok()
                .as_deref()
                != Some(governance.document_revisions_json.as_str())
            || requested.capability_class.as_deref() != governance.capability_class.as_deref()
            || requested.risk_class.as_deref() != governance.risk_class.as_deref()
            || provenance_boundary_mismatch;
        if !boundary_mismatch {
            return Ok(());
        }
        let reason = format!(
            "adaptive Task {operation} changes an approved outcome, acceptance, risk, side-effect, release, or elevated-operation boundary"
        );
        self.record_adaptive_boundary_reconciliation(
            source,
            &governance,
            baseline_id,
            governance
                .baseline_revision_id
                .as_deref()
                .unwrap_or("unknown"),
            operation,
            &reason,
        )
        .await?;
        Err(ServiceError::Conflict(format!(
            "reconciliation_required: {reason}"
        )))
    }

    async fn record_adaptive_boundary_reconciliation(
        &self,
        task: &db::Task,
        governance: &AdaptiveTaskGovernance,
        baseline_id: &str,
        baseline_revision_id: &str,
        operation: &str,
        reason: &str,
    ) -> Result<()> {
        use sha2::{Digest, Sha256};

        let task_digest = hex::encode(Sha256::digest(governance.provenance_json.as_bytes()));
        let idempotency_key = format!(
            "adaptive-boundary:{}:{}:{}:{}",
            task.project_id, task.id, operation, task_digest
        );
        let now = now_rfc3339();
        let conflict = ProjectOrchestrationRepo::create_project_canonical_conflict(
            &*self.db,
            CreateProjectCanonicalConflict {
                id: new_uuid_v4(),
                project_id: task.project_id.clone(),
                domain: "execution".to_owned(),
                governing_record_type: "execution_baseline".to_owned(),
                governing_record_id: baseline_id.to_owned(),
                governing_record_revision: baseline_revision_id.to_owned(),
                governing_record_digest: governance
                    .baseline_content_digest
                    .clone()
                    .unwrap_or_else(|| "unknown".to_owned()),
                conflicting_record_type: "task".to_owned(),
                conflicting_record_id: task.id.clone(),
                conflicting_record_revision: task.version.to_string(),
                conflicting_record_digest: task_digest.clone(),
                affected_paths_json: serde_json::json!([
                    "outcome",
                    "acceptance",
                    "risk_class",
                    "side_effects",
                    "release_policy",
                    "elevated_operations"
                ])
                .to_string(),
                conflict_code: "adaptive_task_boundary_crossed".to_owned(),
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
                    governing_record_type: "execution_baseline".to_owned(),
                    governing_record_id: baseline_id.to_owned(),
                    governing_record_revision: baseline_revision_id.to_owned(),
                    governing_record_digest: governance
                        .baseline_content_digest
                        .clone()
                        .unwrap_or_else(|| "unknown".to_owned()),
                    created_at: now.clone(),
                    updated_at: now,
                },
            )
            .await?;
        }
        Ok(())
    }

    /// Reject orchestration identities before any repository workspace is
    /// prepared. The in-transaction lease guard repeats this check at the
    /// authority boundary, but callers use this preflight to avoid leaving a
    /// task branch/worktree behind for an identity that can never be leased.
    pub(super) async fn ensure_repository_worker_identity(
        &self,
        project_id: &str,
        principal_id: &str,
    ) -> Result<()> {
        let orchestration_binding_count: i64 = sqlx::query_scalar(
            "SELECT
                (SELECT COUNT(*) FROM project_agent_binding
                 WHERE project_id = ? AND identity_id = ? AND state = 'active')
              + (SELECT COUNT(*) FROM account_main_agent_binding
                 WHERE identity_id = ? AND state = 'active')",
        )
        .bind(project_id)
        .bind(principal_id)
        .bind(principal_id)
        .fetch_one(self.db.pool())
        .await?;
        if orchestration_binding_count > 0 {
            return Err(ServiceError::invalid_operation(
                "Main and Project Agent identities cannot receive repository WorkspaceLeases",
            ));
        }
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
    /// new Task.  A pre-baseline implementation Task is allowed only as a
    /// non-runnable plan with the current Charter provenance; a fully
    /// traceable Task becomes runnable only when its exact baseline revision is
    /// active and has a matching user approval receipt.
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
        let requires_execution_governance = implementation || repository_capable;
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
            // Mainstream Task creation surfaces do not carry an orchestration
            // envelope.  Bind those Tasks to the current Charter and keep
            // them non-runnable until a Project Agent supplies exact baseline
            // provenance.  Discovery/planning Tasks receive the only
            // pre-baseline repository capability admitted by the scheduler.
            charter_revision_id: project.current_charter_revision_id.clone(),
            baseline_id: None,
            baseline_revision_id: None,
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

        let mut baseline = None;
        match (
            requested.baseline_id.as_deref(),
            requested.baseline_revision_id.as_deref(),
        ) {
            (Some(baseline_id), Some(baseline_revision_id)) => {
                baseline = sqlx::query(
                    "SELECT b.lifecycle, b.current_revision_id,
                            r.lifecycle AS revision_lifecycle,
                            r.charter_revision_id AS baseline_charter_revision_id,
                            r.document_revisions_json, r.plan_items_json,
                            r.milestone_id, r.milestone_ids_json,
                            r.milestone_definition_revision_ids_json,
                            r.primary_milestone_id,
                            r.capability_classes_json, r.risk_classes_json,
                            r.adaptive_envelope_json, r.elevated_operations_json,
                            r.release_policy_revision, r.release_policy_digest,
                            r.content_digest, r.rendered_digest
                     FROM project_execution_baseline b
                     JOIN project_execution_baseline_revision r
                       ON r.baseline_id = b.id
                     WHERE b.id = ? AND r.id = ? AND b.project_id = ?",
                )
                .bind(baseline_id)
                .bind(baseline_revision_id)
                .bind(&project.id)
                .fetch_optional(self.db.pool())
                .await?
                .map(|row| BaselineContext {
                    lifecycle: row.get("lifecycle"),
                    current_revision_id: row.get("current_revision_id"),
                    revision_lifecycle: row.get("revision_lifecycle"),
                    charter_revision_id: row.get("baseline_charter_revision_id"),
                    document_revisions_json: row.get("document_revisions_json"),
                    plan_items_json: row.get("plan_items_json"),
                    milestone_id: row.get("milestone_id"),
                    milestone_ids_json: row.get("milestone_ids_json"),
                    milestone_definition_revision_ids_json: row
                        .get("milestone_definition_revision_ids_json"),
                    primary_milestone_id: row.get("primary_milestone_id"),
                    capability_classes_json: row.get("capability_classes_json"),
                    risk_classes_json: row.get("risk_classes_json"),
                    adaptive_envelope_json: row.get("adaptive_envelope_json"),
                    elevated_operations_json: row.get("elevated_operations_json"),
                    release_policy_revision: row.get("release_policy_revision"),
                    release_policy_digest: row.get("release_policy_digest"),
                    content_digest: row.get("content_digest"),
                    rendered_digest: row.get("rendered_digest"),
                });
                if baseline.is_none() {
                    return Err(ServiceError::invalid_operation(
                        "Task execution baseline or revision is not owned by this Project",
                    ));
                }
            }
            (None, None) => {}
            _ => {
                return Err(ServiceError::invalid_operation(
                    "baseline_id and baseline_revision_id must be supplied together",
                ));
            }
        }

        if let Some(baseline) = baseline.as_ref() {
            if baseline.charter_revision_id != current_charter_revision_id {
                return Err(ServiceError::invalid_operation(
                    "Task baseline Charter revision does not match the current Project Charter",
                ));
            }

            validate_document_revisions(
                self.db.pool(),
                &project.id,
                &requested.document_revision_ids,
                &baseline.document_revisions_json,
            )
            .await?;

            if implementation && requested.plan_item_id.is_none() {
                return Err(ServiceError::invalid_operation(
                    "repository implementation Tasks require a stable baseline plan_item_id",
                ));
            }
            if let Some(plan_item_id) = requested.plan_item_id.as_deref() {
                if !json_contains_identifier(&baseline.plan_items_json, plan_item_id) {
                    return Err(ServiceError::invalid_operation(
                        "Task plan_item_id is not present in the governing execution baseline",
                    ));
                }
            }

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
                let represented = baseline.milestone_id.as_deref() == Some(milestone_id)
                    || baseline.primary_milestone_id.as_deref() == Some(milestone_id)
                    || json_contains_identifier(&baseline.milestone_ids_json, milestone_id)
                    || json_contains_identifier(&baseline.plan_items_json, milestone_id);
                if !represented {
                    return Err(ServiceError::invalid_operation(
                        "Task milestone is not represented by the governing execution baseline",
                    ));
                }
            } else if implementation {
                return Err(ServiceError::invalid_operation(
                    "repository implementation Tasks require a Project milestone provenance",
                ));
            }
        } else if implementation {
            // This is a valid planning record before baseline approval, but it
            // is intentionally never runnable/write-capable. Only the
            // server-selected read-only profile may receive a discovery lease.
            if requested.plan_item_id.is_some() || requested.milestone_id.is_some() {
                return Err(ServiceError::invalid_operation(
                    "pre-baseline implementation plans cannot claim baseline or milestone authority",
                ));
            }
        }

        if repository_capable
            && baseline.is_none()
            && execution_class == TaskExecutionClass::ReadOnlyPlanning
        {
            if let Some(capability_class) = requested.capability_class.as_deref() {
                if !is_read_only_capability(capability_class) {
                    return Err(ServiceError::invalid_operation(
                        "pre-baseline discovery/planning Tasks require a server-approved read-only capability",
                    ));
                }
            } else {
                requested.capability_class = Some("repository_read".to_owned());
            }
            if requested.risk_class.is_none() {
                requested.risk_class = Some("low".to_owned());
            }
        }

        let baseline_active = baseline.as_ref().is_some_and(|baseline| {
            baseline.lifecycle == "active"
                && baseline.revision_lifecycle == "approved"
                && requested.baseline_revision_id.as_deref()
                    == baseline.current_revision_id.as_deref()
        });
        let approval_matches =
            if let (Some(baseline_id), Some(baseline_revision_id), Some(baseline)) = (
                requested.baseline_id.as_deref(),
                requested.baseline_revision_id.as_deref(),
                baseline.as_ref(),
            ) {
                let approved = sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM project_execution_baseline_approval
                 WHERE baseline_id = ? AND revision_id = ?
                   AND principal_type = 'user'
                   AND authorization_action = 'project.execution_baseline.approve'
                   AND length(trim(authorization_basis)) > 0
                   AND length(trim(authorization_occurred_at)) > 0
                   AND length(trim(explicit_event)) > 0
                   AND content_digest = ? AND rendered_digest = ?
                   AND lifecycle IN ('active', 'consumed')",
                )
                .bind(baseline_id)
                .bind(baseline_revision_id)
                .bind(&baseline.content_digest)
                .bind(&baseline.rendered_digest)
                .fetch_one(self.db.pool())
                .await?;
                approved > 0
            } else {
                false
            };

        if let Some(baseline) = baseline.as_ref().filter(|_| requires_execution_governance) {
            require_allowed_class(
                requested.capability_class.as_deref(),
                &baseline.capability_classes_json,
                "capability_class",
            )?;
            require_allowed_class(
                requested.risk_class.as_deref(),
                &baseline.risk_classes_json,
                "risk_class",
            )?;
        }
        // A baseline may author classes the server cannot dispatch; fail the
        // proposal now with the allowed vocabulary instead of admitting a
        // Task whose every dispatch would be refused by the lease issuer.
        require_server_approved_capability_class(requested.capability_class.as_deref())?;

        // Every repository-capable Task becomes runnable only after the exact
        // current baseline revision is active and has a user approval receipt.
        // Discovery/planning Tasks are admitted before that gate as
        // non-mutating plans; `ensure_task_runnable` verifies their read-only
        // capability profile immediately before workspace preparation.
        let runnable = repository_capable && baseline_active && approval_matches;
        let provenance_json = build_provenance(
            requested.provenance.clone(),
            requested.plan_item_id.as_deref(),
            requested.baseline_id.as_deref(),
            requested.baseline_revision_id.as_deref(),
            baseline.as_ref(),
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
            baseline_id: requested.baseline_id,
            baseline_revision_id: requested.baseline_revision_id,
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

    /// Promote pre-created implementation plans after the exact baseline
    /// activation transaction commits.  The immutable governing references
    /// remain untouched; only the derived runnable bit and row version move.
    /// The SQL repeats the active/current/approved and exact approval checks so
    /// a stale caller cannot promote work from a superseded baseline.
    pub async fn refresh_task_governance_for_baseline(
        &self,
        project_id: &str,
        baseline_id: &str,
        baseline_revision_id: &str,
        now: &str,
    ) -> Result<u64> {
        let result = sqlx::query(
            "UPDATE project_task_governance
             SET runnable = 1, version = version + 1, updated_at = ?
             WHERE project_id = ? AND baseline_id = ?
               AND baseline_revision_id = ? AND runnable = 0
               AND EXISTS (
                   SELECT 1
                   FROM task t
                   WHERE t.id = project_task_governance.task_id
                     AND t.project_id = project_task_governance.project_id
                     AND t.repo_id IS NOT NULL
               )
               AND EXISTS (
                   SELECT 1
                   FROM project_execution_baseline b
                   JOIN project_execution_baseline_revision r
                     ON r.id = project_task_governance.baseline_revision_id
                    AND r.baseline_id = b.id
                   WHERE b.id = project_task_governance.baseline_id
                     AND b.project_id = project_task_governance.project_id
                     AND EXISTS (
                         SELECT 1 FROM project p
                         WHERE p.id = b.project_id
                           AND p.charter_status = 'charter_backed'
                           AND p.charter_setup_required = 0
                           AND p.current_charter_revision_id = r.charter_revision_id
                     )
                     AND b.lifecycle = 'active'
                     AND b.current_revision_id = r.id
                     AND r.lifecycle = 'approved'
               )
               AND EXISTS (
                   SELECT 1
                   FROM project_execution_baseline_approval a
                   JOIN project_execution_baseline_revision r
                     ON r.id = a.revision_id AND r.baseline_id = a.baseline_id
                   WHERE a.baseline_id = project_task_governance.baseline_id
                     AND a.revision_id = project_task_governance.baseline_revision_id
                     AND a.principal_type = 'user'
                     AND a.authorization_action = 'project.execution_baseline.approve'
                     AND length(trim(a.authorization_basis)) > 0
                     AND length(trim(a.authorization_occurred_at)) > 0
                     AND length(trim(a.explicit_event)) > 0
                     AND a.content_digest = r.content_digest
                     AND a.rendered_digest = r.rendered_digest
                     AND a.lifecycle IN ('active', 'consumed')
               )",
        )
        .bind(now)
        .bind(project_id)
        .bind(baseline_id)
        .bind(baseline_revision_id)
        .execute(self.db.pool())
        .await?;
        Ok(result.rows_affected())
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
            baseline_id: governance.baseline_id,
            baseline_revision_id: governance.baseline_revision_id,
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
    /// follow-up execution behind the same gate.
    pub(crate) async fn ensure_task_runnable(&self, task: &db::Task) -> Result<()> {
        let row = sqlx::query(
            "SELECT p.charter_status, p.charter_setup_required,
                    p.current_charter_revision_id,
                    t.task_type,
                    g.runnable, g.charter_revision_id,
                    g.baseline_id, g.baseline_revision_id,
                    g.capability_class, g.risk_class,
                    b.lifecycle, b.current_revision_id,
                            r.charter_revision_id AS baseline_charter_revision_id,
                            r.capability_classes_json, r.risk_classes_json,
                            (SELECT COUNT(*) FROM project_execution_baseline_approval a
                     WHERE a.baseline_id = g.baseline_id
                       AND a.revision_id = g.baseline_revision_id
                       AND a.principal_type = 'user'
                       AND a.authorization_action = 'project.execution_baseline.approve'
                       AND length(trim(a.authorization_basis)) > 0
                       AND length(trim(a.authorization_occurred_at)) > 0
                       AND length(trim(a.explicit_event)) > 0
                       AND a.content_digest = r.content_digest
                       AND a.rendered_digest = r.rendered_digest
                       AND a.lifecycle IN ('active', 'consumed')) AS approval_count
             FROM project p
             JOIN task t ON t.id = ? AND t.project_id = p.id
                 AND t.deleted_at IS NULL
             LEFT JOIN project_task_governance g ON g.project_id = p.id
                 AND g.task_id = t.id
             LEFT JOIN project_execution_baseline b ON b.id = g.baseline_id
             LEFT JOIN project_execution_baseline_revision r
                 ON r.id = g.baseline_revision_id
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
        let prebaseline_read_only = execution_class == TaskExecutionClass::ReadOnlyPlanning
            && row.get::<Option<String>, _>("baseline_id").is_none()
            && row
                .get::<Option<String>, _>("baseline_revision_id")
                .is_none()
            && is_read_only_capability(&capability_class);

        if prebaseline_read_only {
            // Before baseline approval, bounded discovery/planning remains
            // admissible even when repository setup is incomplete. The Task
            // may be recorded or handled by a non-repository planning path,
            // but no WorkspaceLease can be issued without a repo binding.
            return Ok(());
        }

        if execution_class == TaskExecutionClass::Implementation {
            // The canonical Project setup projection is the authority for
            // durable repository/role readiness. A primary_repo_id check by
            // itself is insufficient: an active coordinator identity or a
            // failed provisioning operation must not receive a write lease.
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
            if setup.execution_gate != ExecutionGate::Active {
                return Err(ServiceError::invalid_operation(
                    "repository implementation Task is not runnable: an active user-approved execution baseline is required",
                ));
            }
        }

        if let Some(allowed) = row.get::<Option<String>, _>("capability_classes_json") {
            require_allowed_class(Some(&capability_class), &allowed, "capability_class")?;
        }
        if let Some(allowed) = row.get::<Option<String>, _>("risk_classes_json") {
            require_allowed_class(
                row.get::<Option<String>, _>("risk_class").as_deref(),
                &allowed,
                "risk_class",
            )?;
        }
        let runnable: Option<i64> = row.get("runnable");
        let admitted = runnable == Some(1)
            && row
                .get::<Option<String>, _>("charter_revision_id")
                .as_deref()
                == row
                    .get::<Option<String>, _>("current_charter_revision_id")
                    .as_deref()
            && row.get::<Option<String>, _>("baseline_id").is_some()
            && row
                .get::<Option<String>, _>("baseline_revision_id")
                .is_some()
            && row.get::<Option<String>, _>("lifecycle").as_deref() == Some("active")
            && row
                .get::<Option<String>, _>("current_revision_id")
                .as_deref()
                == row
                    .get::<Option<String>, _>("baseline_revision_id")
                    .as_deref()
            && row
                .get::<Option<String>, _>("charter_revision_id")
                .as_deref()
                == row
                    .get::<Option<String>, _>("baseline_charter_revision_id")
                    .as_deref()
            && row.get::<i64, _>("approval_count") > 0;
        if !admitted {
            return Err(ServiceError::invalid_operation(
                "repository Task is not runnable: an active user-approved execution baseline with matching traceability is required",
            ));
        }
        Ok(())
    }

    /// Issue the scheduler's short-lived internal repository authority only
    /// after the same admission gate used by claim/launch/recovery.  The
    /// opaque lease is persisted by `WorkspaceLeaseRepo`; no route or chat
    /// context receives the row, its capability JSON, or a filesystem path.
    ///
    /// The database-side lease scope guard repeats the current-baseline and
    /// read-only discovery predicates, so a baseline supersession racing this
    /// call cannot turn a stale preflight into repository authority.
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
        self.ensure_task_runnable(task).await?;
        let canonical_role = canonical_workspace_lease_role(role)?;
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
        let orchestration_binding_count: i64 = sqlx::query_scalar(
            "SELECT
                (SELECT COUNT(*) FROM project_agent_binding
                 WHERE project_id = ? AND identity_id = ? AND state = 'active')
              + (SELECT COUNT(*) FROM account_main_agent_binding
                 WHERE identity_id = ? AND state = 'active')",
        )
        .bind(&task.project_id)
        .bind(principal_id)
        .bind(principal_id)
        .fetch_one(&mut **transaction)
        .await?;
        if orchestration_binding_count > 0 {
            return Err(ServiceError::invalid_operation(
                "Main and Project Agent identities cannot receive repository WorkspaceLeases",
            ));
        }

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

        // Repeat the admission predicate inside the claim transaction.  The
        // migration's scope trigger is the final database backstop, but this
        // gives callers a stable service error and keeps the read-only
        // pre-baseline branch explicit.
        let gate = sqlx::query(
            "SELECT p.charter_status, p.charter_setup_required,
                    p.current_charter_revision_id, g.runnable,
                    g.charter_revision_id, g.baseline_id, g.baseline_revision_id,
                    b.lifecycle, b.current_revision_id,
                    r.lifecycle AS revision_lifecycle,
                    r.charter_revision_id AS baseline_charter_revision_id,
                    (SELECT COUNT(*) FROM project_execution_baseline_approval a
                     WHERE a.baseline_id = g.baseline_id
                       AND a.revision_id = g.baseline_revision_id
                       AND a.principal_type = 'user'
                       AND a.authorization_action = 'project.execution_baseline.approve'
                       AND length(trim(a.authorization_basis)) > 0
                       AND length(trim(a.authorization_occurred_at)) > 0
                       AND length(trim(a.explicit_event)) > 0
                       AND a.content_digest = r.content_digest
                       AND a.rendered_digest = r.rendered_digest
                       AND a.lifecycle IN ('active', 'consumed')) AS approval_count
             FROM project p
             LEFT JOIN project_task_governance g
               ON g.project_id = p.id AND g.task_id = ?
             LEFT JOIN project_execution_baseline b ON b.id = g.baseline_id
             LEFT JOIN project_execution_baseline_revision r
               ON r.id = g.baseline_revision_id
             WHERE p.id = ?",
        )
        .bind(&task.id)
        .bind(&task.project_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let charter_backed = gate.get::<String, _>("charter_status") == "charter_backed"
            && gate.get::<i64, _>("charter_setup_required") == 0;
        let prebaseline_read_only = charter_backed
            && execution_class == TaskExecutionClass::ReadOnlyPlanning
            && gate.get::<Option<String>, _>("baseline_id").is_none()
            && gate
                .get::<Option<String>, _>("baseline_revision_id")
                .is_none()
            && is_read_only_capability(&capability_class);
        let exact_baseline = charter_backed
            && gate.get::<Option<i64>, _>("runnable") == Some(1)
            && gate
                .get::<Option<String>, _>("charter_revision_id")
                .as_deref()
                == gate
                    .get::<Option<String>, _>("current_charter_revision_id")
                    .as_deref()
            && gate.get::<Option<String>, _>("baseline_id").is_some()
            && gate
                .get::<Option<String>, _>("baseline_revision_id")
                .as_deref()
                == gate
                    .get::<Option<String>, _>("current_revision_id")
                    .as_deref()
            && gate.get::<Option<String>, _>("lifecycle").as_deref() == Some("active")
            && gate
                .get::<Option<String>, _>("revision_lifecycle")
                .as_deref()
                == Some("approved")
            && gate
                .get::<Option<String>, _>("baseline_charter_revision_id")
                .as_deref()
                == gate
                    .get::<Option<String>, _>("current_charter_revision_id")
                    .as_deref()
            && gate.get::<i64, _>("approval_count") > 0;
        if charter_backed && !(exact_baseline || prebaseline_read_only) {
            return Err(ServiceError::invalid_operation(
                "WorkspaceLease requires the exact active user-approved execution baseline",
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
        self.ensure_task_runnable(task).await?;
        let canonical_role = canonical_workspace_lease_role(role)?;
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
    baseline_document_revisions_json: &str,
) -> Result<()> {
    let baseline_documents: Value = serde_json::from_str(baseline_document_revisions_json)
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "invalid baseline document references: {error}"
            ))
        })?;
    for revision_id in requested {
        if revision_id.trim().is_empty()
            || !json_contains_identifier_value(&baseline_documents, revision_id)
        {
            return Err(ServiceError::invalid_operation(
                "Task Document revision is not included in the governing execution baseline",
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

fn require_allowed_class(requested: Option<&str>, allowed_json: &str, field: &str) -> Result<()> {
    let allowed: Value = serde_json::from_str(allowed_json).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid baseline {field} list: {error}"))
    })?;
    // A baseline that declares no classes at all leaves the field
    // unconstrained: the lease issuer falls back to the server-selected
    // capability profile. Only a baseline that names classes turns the field
    // into a required, closed choice.
    let declares_classes = match &allowed {
        Value::Array(values) => !values.is_empty(),
        Value::Null => false,
        _ => true,
    };
    let Some(requested) = requested.filter(|value| !value.trim().is_empty()) else {
        if declares_classes {
            return Err(ServiceError::invalid_operation(format!(
                "repository implementation Tasks require {field}"
            )));
        }
        return Ok(());
    };
    if declares_classes && !json_contains_identifier_value(&allowed, requested) {
        return Err(ServiceError::invalid_operation(format!(
            "Task {field} is outside the approved execution baseline"
        )));
    }
    Ok(())
}

fn build_provenance(
    requested: Option<Value>,
    plan_item_id: Option<&str>,
    baseline_id: Option<&str>,
    baseline_revision_id: Option<&str>,
    baseline: Option<&BaselineContext>,
) -> Result<String> {
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
    let required = [
        ("origin_plan_item_id", plan_item_id),
        ("governing_baseline_id", baseline_id),
        ("governing_baseline_revision_id", baseline_revision_id),
    ];
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
    if let Some(baseline) = baseline {
        let envelope = parse_adaptive_task_envelope(&baseline.adaptive_envelope_json)?;
        let baseline_elevated_operations: Value =
            serde_json::from_str(&baseline.elevated_operations_json).map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "invalid baseline elevated operations: {error}"
                ))
            })?;
        if baseline_elevated_operations != serde_json::json!(envelope.elevated_operations) {
            return Err(ServiceError::conflict(
                "baseline elevated operations differ from its adaptive envelope",
            ));
        }
        let envelope_value = serde_json::to_value(&envelope)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let adaptive_envelope_digest = api_types::canonical_digest_with_schema(
            ADAPTIVE_ENVELOPE_DIGEST_SCHEMA,
            &envelope_value,
        )
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let fixed_boundary = serde_json::json!({
            "fixed_outcomes": &envelope.fixed_outcomes,
            "fixed_acceptance": &envelope.fixed_acceptance,
            "fixed_risk_classes": &envelope.fixed_risk_classes,
            "forbidden_side_effects": &envelope.forbidden_side_effects,
            "release_policy_revision": &baseline.release_policy_revision,
            "release_policy_digest": &baseline.release_policy_digest,
            "elevated_operations": &envelope.elevated_operations,
        });
        let fixed_boundary_digest =
            api_types::canonical_digest_with_schema(FIXED_BOUNDARY_DIGEST_SCHEMA, &fixed_boundary)
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let authoritative = [
            (
                "fixed_outcomes",
                serde_json::json!(&envelope.fixed_outcomes),
            ),
            (
                "fixed_acceptance",
                serde_json::json!(&envelope.fixed_acceptance),
            ),
            (
                "fixed_risk_classes",
                serde_json::json!(&envelope.fixed_risk_classes),
            ),
            (
                "forbidden_side_effects",
                serde_json::json!(&envelope.forbidden_side_effects),
            ),
            (
                "release_policy_revision",
                Value::String(baseline.release_policy_revision.clone()),
            ),
            (
                "release_policy_digest",
                Value::String(baseline.release_policy_digest.clone()),
            ),
            (
                "elevated_operations",
                serde_json::json!(&envelope.elevated_operations),
            ),
            (
                "fixed_boundary_digest",
                Value::String(fixed_boundary_digest),
            ),
            (
                "adaptive_envelope_digest",
                Value::String(adaptive_envelope_digest),
            ),
        ];
        for (field, value) in authoritative {
            if let Some(existing) = map.get(field) {
                if existing != &value {
                    return Err(ServiceError::conflict(format!(
                        "Task governance provenance {field} does not match the authoritative baseline"
                    )));
                }
            }
            map.insert(field.to_owned(), value);
        }
        for (field, value) in [
            (
                "governing_baseline_content_digest",
                Value::String(baseline.content_digest.clone()),
            ),
            (
                "governing_baseline_rendered_digest",
                Value::String(baseline.rendered_digest.clone()),
            ),
        ] {
            if let Some(existing) = map.get(field) {
                if existing != &value {
                    return Err(ServiceError::conflict(format!(
                        "Task governance provenance {field} does not match the authoritative baseline"
                    )));
                }
            }
            map.insert(field.to_owned(), value);
        }
        let milestone_definition_revision_ids: Value = serde_json::from_str(
            &baseline.milestone_definition_revision_ids_json,
        )
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "invalid baseline milestone definition references: {error}"
            ))
        })?;
        map.insert(
            "governing_milestone_definition_revision_ids".to_owned(),
            milestone_definition_revision_ids,
        );
    } else {
        map.insert("baseline_pending".to_owned(), Value::Bool(true));
    }
    map.insert(
        "schema".to_owned(),
        Value::String("forge.task-governance/v1".to_owned()),
    );
    serde_json::to_string(&Value::Object(map))
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))
}

fn json_contains_identifier(value: &str, identifier: &str) -> bool {
    serde_json::from_str::<Value>(value)
        .map(|value| json_contains_identifier_value(&value, identifier))
        .unwrap_or(false)
}

fn json_contains_identifier_value(value: &Value, identifier: &str) -> bool {
    match value {
        Value::String(value) => value == identifier,
        Value::Array(values) => values
            .iter()
            .any(|value| json_contains_identifier_value(value, identifier)),
        Value::Object(values) => {
            ["id", "plan_item_id", "document_revision_id", "milestone_id"]
                .iter()
                .any(|key| values.get(*key).and_then(Value::as_str) == Some(identifier))
                || values
                    .values()
                    .any(|value| json_contains_identifier_value(value, identifier))
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
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

    #[test]
    fn identifier_matching_accepts_plan_and_artifact_shapes() {
        assert!(json_contains_identifier(
            r#"[{"id":"plan-1"},{"document_revision_id":"doc-2"}]"#,
            "plan-1"
        ));
        assert!(json_contains_identifier(
            r#"[{"id":"plan-1"},{"document_revision_id":"doc-2"}]"#,
            "doc-2"
        ));
        assert!(!json_contains_identifier(r#"[{"id":"plan-1"}]"#, "plan-2"));
    }

    #[test]
    fn provenance_adds_authoritative_adaptive_envelope_digest() {
        let baseline = BaselineContext {
            lifecycle: "active".to_owned(),
            current_revision_id: Some("revision-1".to_owned()),
            revision_lifecycle: "approved".to_owned(),
            charter_revision_id: "charter-revision-1".to_owned(),
            document_revisions_json: "[]".to_owned(),
            plan_items_json: "[]".to_owned(),
            milestone_id: None,
            milestone_ids_json: "[]".to_owned(),
            milestone_definition_revision_ids_json: "[]".to_owned(),
            primary_milestone_id: None,
            capability_classes_json: "[]".to_owned(),
            risk_classes_json: "[]".to_owned(),
            adaptive_envelope_json: r#"{"allowed_task_operations":["split"],"fixed_outcomes":[],"fixed_acceptance":[],"fixed_risk_classes":[],"forbidden_side_effects":[],"elevated_operations":[]}"#.to_owned(),
            elevated_operations_json: "[]".to_owned(),
            release_policy_revision: "policy-r1".to_owned(),
            release_policy_digest: "policy-digest".to_owned(),
            content_digest: "content".to_owned(),
            rendered_digest: "rendered".to_owned(),
        };
        let value = build_provenance(
            None,
            Some("plan-1"),
            Some("baseline-1"),
            Some("revision-1"),
            Some(&baseline),
        )
        .expect("provenance should serialize");
        let value: Value = serde_json::from_str(&value).expect("valid provenance");
        assert_eq!(value["origin_plan_item_id"], "plan-1");
        assert_eq!(value["governing_baseline_content_digest"], "content");
        assert_eq!(value["schema"], "forge.task-governance/v1");
        assert!(value["adaptive_envelope_digest"].as_str().is_some());
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
    async fn charter_backed_repository_task_derives_pending_baseline_governance() {
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
        assert!(!governance.runnable);
        assert_eq!(
            governance.charter_revision_id.as_deref(),
            Some("charter-revision-1")
        );
        assert!(governance.baseline_id.is_none());
        assert!(governance.capability_class.is_none());
        assert!(governance.risk_class.is_none());
        assert!(governance.provenance_json.contains("baseline_pending"));
    }

    #[tokio::test]
    async fn charter_backed_repository_planning_task_is_admitted_only_as_non_runnable() {
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
        assert!(!governance.runnable);
        assert_eq!(
            governance.capability_class.as_deref(),
            Some("repository_read")
        );
        assert_eq!(governance.risk_class.as_deref(), Some("low"));
        assert!(governance.provenance_json.contains("baseline_pending"));
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
                    baseline_id: None,
                    baseline_revision_id: None,
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
                    baseline_id: None,
                    baseline_revision_id: None,
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
}
