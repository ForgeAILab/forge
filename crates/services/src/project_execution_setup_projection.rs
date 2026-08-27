//! Canonical Project execution-setup projection.
//!
//! Coordination, repository setup, and Charter-backed Task execution are
//! independent dimensions. Project role settings are optional defaults. A source read failure is surfaced as an explicit
//! unavailable dimension with a retry action; it is never converted into a
//! plausible `setup_required` or `ready` result.

use std::path::Path;

use api_types::{
    CoordinationState, ExecutionBlockerCode, ExecutionBlockerPrincipal, ExecutionBlockerProjection,
    ExecutionBlockerRecordRef, ExecutionBlockerScope, ExecutionBlockerStage,
    ExecutionEvidenceSummary, ExecutionGate, ExecutionPrincipalResponse, ExecutionSetupState,
    ProjectExecutionSetupAvailability, ProjectExecutionSetupResponse, ProjectionStatus,
    ProvisioningOperationResponse, RepoResponse, RetryAction, SetupRequirement,
};
use db::{
    Agent, AgentChatRepo, Project, ProjectAgentBindingRepo, ProjectProvisioningRepo, ProjectRepo,
    RepoRepo, SqliteDb,
};
use sha2::{Digest, Sha256};

use crate::{
    execution_setup::{
        classify_task_execution, eligible_project_execution_agents, is_read_only_capability,
        resolve_project_execution_roles, TaskExecutionClass,
    },
    Result, ServiceError,
};

/// One required reconciliation record joined with its canonical conflict.
/// Shared by the Project-wide gate computation, the per-Task blocker
/// projection, and the capability-aware review evaluation in
/// `task_service::governance` so all three agree on exactly the same set of
/// outstanding conflicts.
#[derive(Debug, Clone)]
pub(crate) struct ReconciliationConflictRow {
    pub reconciliation_id: String,
    pub conflict_code: String,
    pub description: String,
    pub record_type: String,
    pub record_id: String,
    pub governing_record_type: String,
    pub governing_record_id: String,
    pub affected_paths: Vec<String>,
    pub updated_at: String,
}

impl ReconciliationConflictRow {
    /// Whether this conflict attaches to the named Task specifically.
    pub(crate) fn is_scoped_to_task(&self, task_id: &str) -> bool {
        self.record_type == "task" && self.record_id == task_id
    }
}

/// Load every `required` reconciliation record for a Project, joined with
/// its canonical conflict. Ordered most-recently-updated first so the
/// exact same "current" conflict is chosen everywhere it is consulted.
pub(crate) async fn required_reconciliation_conflicts(
    db: &SqliteDb,
    project_id: &str,
) -> Result<Vec<ReconciliationConflictRow>> {
    let rows = sqlx::query(
        "SELECT r.id AS reconciliation_id, c.conflict_code, c.description,
                r.record_type, r.record_id,
                r.governing_record_type, r.governing_record_id,
                c.affected_paths_json, r.updated_at
         FROM project_reconciliation_record r
         JOIN project_canonical_conflict c ON c.id = r.conflict_id
         WHERE r.project_id = ? AND r.state = 'required'
         ORDER BY r.updated_at DESC, r.id DESC",
    )
    .bind(project_id)
    .fetch_all(db.pool())
    .await?;
    rows.into_iter()
        .map(|row| {
            let affected_paths_json: String = sqlx::Row::try_get(&row, "affected_paths_json")?;
            let affected_paths: Vec<String> =
                serde_json::from_str(&affected_paths_json).unwrap_or_default();
            Ok(ReconciliationConflictRow {
                reconciliation_id: sqlx::Row::try_get(&row, "reconciliation_id")?,
                conflict_code: sqlx::Row::try_get(&row, "conflict_code")?,
                description: sqlx::Row::try_get(&row, "description")?,
                record_type: sqlx::Row::try_get(&row, "record_type")?,
                record_id: sqlx::Row::try_get(&row, "record_id")?,
                governing_record_type: sqlx::Row::try_get(&row, "governing_record_type")?,
                governing_record_id: sqlx::Row::try_get(&row, "governing_record_id")?,
                affected_paths,
                updated_at: sqlx::Row::try_get(&row, "updated_at")?,
            })
        })
        .collect::<std::result::Result<Vec<_>, ServiceError>>()
}

/// Load the authoritative, scope-neutral Project setup projection used by
/// REST Project views and the Project current-state tool.
pub async fn load_project_execution_setup(
    db: &SqliteDb,
    project_id: &str,
) -> Result<ProjectExecutionSetupResponse> {
    let project = ProjectRepo::get_by_id(db, project_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;

    let (coordination_state, coordination_status) = match coordination_state(db, project_id).await {
        Ok(state) => (state, ProjectionStatus::current()),
        Err(_) => (
            CoordinationState::Unavailable,
            ProjectionStatus::unavailable(),
        ),
    };

    let provisioning_result =
        ProjectProvisioningRepo::get_provisioning_operation(db, project_id).await;
    let provisioning = match &provisioning_result {
        Ok(operation) => operation.clone().map(provisioning_response),
        Err(_) => None,
    };

    // These reads jointly establish execution setup. Keep them independent of
    // coordination/gate reads so one unavailable source does not erase the
    // other dimensions from the response.
    let primary_repo_result = load_primary_repo(db, &project).await;
    let roles_result = resolve_project_execution_roles(db, &project).await;
    let eligible_result = eligible_project_execution_agents(db, &project).await;
    let setup_source_available = provisioning_result.is_ok()
        && primary_repo_result.is_ok()
        && roles_result.is_ok()
        && eligible_result.is_ok();

    let primary_repo_record = primary_repo_result.ok().flatten();
    let primary_repo = primary_repo_record.as_ref().map(|(repo, _)| repo.clone());
    let primary_repo_ready = primary_repo_record
        .as_ref()
        .is_some_and(|(_, ready)| *ready);
    let roles = roles_result.ok();
    let eligible_agents = eligible_result.ok().unwrap_or_default();

    let mut setup_requirements = roles
        .as_ref()
        .map(|resolution| resolution.requirements.clone())
        .unwrap_or_default();
    // Project Agent coordination is useful but optional for Task execution.
    // A missing/unavailable binding is reported in `coordination_state`; it
    // does not turn an otherwise runnable Task into Project setup work.
    if !primary_repo_ready && setup_source_available {
        let mut requirement = SetupRequirement::new("repository");
        requirement.resource_type = Some("project_repository".to_owned());
        requirement.capability = Some("repository_write".to_owned());
        requirement.action = Some(RetryAction::AttachRepository);
        setup_requirements.push(requirement);
    }

    let provisioning_lease_expired = provisioning
        .as_ref()
        .is_some_and(provisioning_lease_expired);
    let mut execution_setup_state = if !setup_source_available {
        ExecutionSetupState::Unavailable
    } else if provisioning_lease_expired {
        // A dead lease must not make the UI look permanently provisioning.
        // Keep the durable operation untouched for the recovery worker, but
        // expose a finite, retryable projection immediately.
        ExecutionSetupState::Failed
    } else if primary_repo_ready {
        // Project role settings are optional defaults. Once the repository is
        // verifiably linked, Task-level assignments can satisfy workflow roles
        // without completing a separate Project-wide role checkpoint.
        ExecutionSetupState::Ready
    } else {
        provisioning_state(provisioning.as_ref())
    };

    // A durable ready marker is advisory until current repository and role
    // records agree with it. This protects against edits after provisioning
    // or a stale migration/backfill result.
    if setup_source_available
        && execution_setup_state == ExecutionSetupState::Ready
        && !primary_repo_ready
    {
        execution_setup_state = ExecutionSetupState::SetupRequired;
    }

    if let Some(requirement) =
        provisioning_requirement(execution_setup_state, provisioning.as_ref())
    {
        setup_requirements.push(requirement);
    }
    deduplicate_requirements(&mut setup_requirements);

    let worker_identity_id = roles
        .as_ref()
        .and_then(|resolution| resolution.worker_identity_id.as_deref());
    let worker = worker_identity_id
        .and_then(|identity_id| eligible_agents.iter().find(|agent| agent.id == identity_id))
        .map(|agent| execution_principal_response(agent.clone()));
    let reviewer_identity_id = roles
        .as_ref()
        .and_then(|resolution| resolution.reviewer_identity_id.as_deref());
    let independent_reviewer = reviewer_identity_id
        .and_then(|identity_id| eligible_agents.iter().find(|agent| agent.id == identity_id))
        .map(|agent| execution_principal_response(agent.clone()));
    let eligible_workers = eligible_agents
        .iter()
        .cloned()
        .map(execution_principal_response)
        .collect::<Vec<_>>();
    let eligible_reviewers = eligible_agents
        .iter()
        .cloned()
        .map(execution_principal_response)
        .collect::<Vec<_>>();

    let (execution_gate, gate_status, reconciliation_conflict) =
        match execution_gate(db, project_id).await {
            Ok((gate, conflict)) => (gate, ProjectionStatus::current(), conflict),
            Err(_) => (
                ExecutionGate::Unavailable,
                ProjectionStatus::unavailable(),
                None,
            ),
        };
    if gate_status.availability != api_types::ProjectionAvailability::Current {
        let mut requirement = SetupRequirement::new("execution_gate_projection");
        requirement.action = Some(RetryAction::RefreshAndRetry);
        setup_requirements.push(requirement);
    } else {
        add_gate_requirement(&mut setup_requirements, execution_gate);
    }
    deduplicate_requirements(&mut setup_requirements);

    let next_action = setup_requirements
        .iter()
        .find_map(|requirement| requirement.action)
        .or(match execution_gate {
            ExecutionGate::BaselineApprovalRequired => Some(RetryAction::Reauthorize),
            ExecutionGate::PreBaselineReadOnly => Some(RetryAction::Repropose),
            ExecutionGate::ReconciliationRequired => Some(RetryAction::ResolveReconciliation),
            ExecutionGate::Unavailable => Some(RetryAction::RefreshAndRetry),
            ExecutionGate::Active => None,
        });
    let leading_requirement = next_action.and_then(|action| {
        setup_requirements
            .iter()
            .find(|requirement| requirement.action == Some(action))
    });
    let execution_blocker = project_scope_execution_blocker(
        project.version,
        coordination_state,
        execution_setup_state,
        execution_gate,
        next_action,
        leading_requirement,
        provisioning.as_ref(),
        reconciliation_conflict.as_ref(),
    );

    Ok(ProjectExecutionSetupResponse {
        project_id: project.id,
        project_version: project.version,
        coordination_state,
        execution_setup_state,
        execution_gate,
        availability: ProjectExecutionSetupAvailability {
            coordination: coordination_status,
            execution_setup: if !setup_source_available {
                ProjectionStatus::unavailable()
            } else if provisioning_lease_expired {
                ProjectionStatus::stale()
            } else {
                ProjectionStatus::current()
            },
            execution_gate: gate_status,
        },
        primary_repo,
        worker,
        independent_reviewer,
        eligible_workers,
        eligible_reviewers,
        setup_requirements,
        next_action,
        provisioning,
        execution_blocker,
    })
}

fn provisioning_response(
    operation: db::ProjectProvisioningOperation,
) -> ProvisioningOperationResponse {
    ProvisioningOperationResponse {
        id: operation.id,
        status: operation.status,
        current_checkpoint: operation.current_checkpoint,
        attempt_count: operation.attempt_count,
        max_attempts: operation.max_attempts,
        lease_owner: operation.lease_owner,
        lease_expires_at: operation.lease_expires_at,
        next_retry_at: operation.next_retry_at,
        retryable: operation.retryable,
        last_error_code: operation.last_error_code,
        last_error_message: operation.last_error_message,
        version: operation.version,
    }
}

async fn load_primary_repo(
    db: &SqliteDb,
    project: &Project,
) -> Result<Option<(RepoResponse, bool)>> {
    let Some(repo_id) = project.primary_repo_id.as_deref() else {
        return Ok(None);
    };
    let repo = RepoRepo::get_by_id(db, repo_id).await?;
    let Some(repo) = repo.filter(|repo| repo.project_id == project.id) else {
        return Ok(None);
    };
    let repository_ready = verify_repository_state(&repo).await?;
    Ok(Some((repo_response(repo), repository_ready)))
}

async fn verify_repository_state(repo: &db::Repo) -> Result<bool> {
    let Some(local_path) = repo.local_path.as_deref() else {
        // A remote repository has no local filesystem claim to verify. Its
        // Project linkage is the authoritative DB-verifiable checkpoint.
        return Ok(true);
    };
    if repo.default_branch != "main" {
        return Ok(false);
    }
    let path = Path::new(local_path);
    if !git::is_git_repo(path).await || !git::branch_exists(path, "main").await? {
        return Ok(false);
    }
    Ok(!git::get_current_sha(path).await?.trim().is_empty())
}

async fn coordination_state(db: &SqliteDb, project_id: &str) -> Result<CoordinationState> {
    let binding = ProjectAgentBindingRepo::get_active_project_binding(db, project_id).await?;
    let chat = AgentChatRepo::get_project_chat(db, project_id).await?;
    match (binding, chat) {
        (Some(binding), Some(chat)) if binding.state == "active" && chat.status == "ready" => {
            Ok(CoordinationState::Ready)
        }
        _ => Ok(CoordinationState::SetupRequired),
    }
}

fn provisioning_state(provisioning: Option<&ProvisioningOperationResponse>) -> ExecutionSetupState {
    match provisioning.map(|operation| operation.status.as_str()) {
        Some("provisioning") => ExecutionSetupState::Provisioning,
        Some("ready") => ExecutionSetupState::Ready,
        Some("failed") => ExecutionSetupState::Failed,
        Some("setup_required") | None => ExecutionSetupState::SetupRequired,
        Some(_) => ExecutionSetupState::Failed,
    }
}

fn provisioning_lease_expired(operation: &ProvisioningOperationResponse) -> bool {
    operation.status == "provisioning"
        && operation
            .lease_expires_at
            .as_deref()
            .and_then(|expires| chrono::DateTime::parse_from_rfc3339(expires).ok())
            .is_some_and(|expires| expires <= chrono::Utc::now())
}

fn provisioning_requirement(
    state: ExecutionSetupState,
    operation: Option<&ProvisioningOperationResponse>,
) -> Option<SetupRequirement> {
    let (requirement_type, action) = match state {
        ExecutionSetupState::Failed if operation.is_some_and(provisioning_retry_allowed) => {
            ("provisioning", RetryAction::RetryProvisioning)
        }
        ExecutionSetupState::Failed => {
            // A terminal operation cannot be accepted by the retry endpoint:
            // advertising RetryProvisioning here would only produce a
            // guaranteed conflict. Keep the failure recoverable through the
            // setup/configuration flow instead.
            ("provisioning", RetryAction::CompleteSetup)
        }
        ExecutionSetupState::SetupRequired
            if operation.is_some_and(|operation| {
                operation.status == "setup_required" && provisioning_retry_allowed(operation)
            }) =>
        {
            // A durable backfill may have been conservative (for example, it
            // cannot inspect a local filesystem). Once the current repository
            // and role reads no longer explain the blocker, retain a concrete
            // retry action while attempts remain.
            ("provisioning", RetryAction::RetryProvisioning)
        }
        ExecutionSetupState::SetupRequired
            if operation.is_some_and(|operation| operation.status == "setup_required") =>
        {
            // A durable setup blocker that is not retryable needs user
            // configuration/remediation rather than a retry request that the
            // finite operation will reject.
            ("provisioning", RetryAction::CompleteSetup)
        }
        ExecutionSetupState::Unavailable => {
            ("execution_setup_projection", RetryAction::RefreshAndRetry)
        }
        ExecutionSetupState::Provisioning | ExecutionSetupState::Ready => return None,
        ExecutionSetupState::SetupRequired => return None,
    };
    let mut requirement = SetupRequirement::new(requirement_type);
    requirement.action = Some(action);
    Some(requirement)
}

fn provisioning_retry_allowed(operation: &ProvisioningOperationResponse) -> bool {
    // An expired lease is recoverable even when the previous attempt was
    // interrupted after claiming it. Reconciliation resumes that finite
    // operation; it does not create a second operation.
    if provisioning_lease_expired(operation) {
        return true;
    }
    operation.retryable && operation.attempt_count < operation.max_attempts
}

/// Charter approval is the Project-wide implementation authority. Baselines
/// and their reconciliations remain planning/readiness inputs and do not add a
/// second Task-dispatch gate; Task-scoped conflicts are projected separately.
async fn execution_gate(
    _db: &SqliteDb,
    _project_id: &str,
) -> Result<(ExecutionGate, Option<ReconciliationConflictRow>)> {
    Ok((ExecutionGate::Active, None))
}

fn execution_principal_response(agent: Agent) -> ExecutionPrincipalResponse {
    ExecutionPrincipalResponse {
        identity_id: agent.id,
        name: agent.name,
        profile_id: agent.profile_id,
        executor_type: agent.executor_type,
        provider: agent.provider,
        model: agent.model,
        status: agent.status.to_string(),
        paused: agent.paused,
        version: agent.version,
    }
}

fn repo_response(repo: db::Repo) -> RepoResponse {
    RepoResponse {
        id: repo.id,
        project_id: repo.project_id,
        name: repo.name,
        local_path: repo.local_path,
        remote_url: repo.remote_url,
        default_branch: repo.default_branch,
        work_mode: match repo.work_mode {
            db::WorkMode::DirectMerge => api_types::WorkMode::DirectMerge,
            db::WorkMode::PullRequest => api_types::WorkMode::PullRequest,
        },
        pr_provider: None,
        pr_provider_status: None,
        created_at: repo.created_at,
        updated_at: repo.updated_at,
    }
}

fn add_gate_requirement(requirements: &mut Vec<SetupRequirement>, gate: ExecutionGate) {
    let action = match gate {
        // Legacy enum values remain readable for historical projections, but
        // baseline state no longer creates an execution setup requirement.
        ExecutionGate::BaselineApprovalRequired | ExecutionGate::PreBaselineReadOnly => None,
        ExecutionGate::ReconciliationRequired => Some(RetryAction::ResolveReconciliation),
        ExecutionGate::Active | ExecutionGate::Unavailable => None,
    };
    if let Some(action) = action {
        let mut requirement = SetupRequirement::new("execution_baseline");
        requirement.action = Some(action);
        requirements.push(requirement);
    }
}

fn deduplicate_requirements(requirements: &mut Vec<SetupRequirement>) {
    let mut seen = std::collections::BTreeSet::new();
    requirements.retain(|requirement| {
        seen.insert((
            requirement.requirement_type.clone(),
            requirement.role.clone(),
            requirement.capability.clone(),
        ))
    });
}

/// Build the one Project-wide `ExecutionBlockerProjection`, or `None` when
/// the Project has no outstanding blocker. This is a pure translation of the
/// exact same state `load_project_execution_setup` already computed — it
/// never re-derives copy from raw enums on a different surface (D17).
#[allow(clippy::too_many_arguments)]
fn project_scope_execution_blocker(
    project_version: i64,
    coordination_state: CoordinationState,
    execution_setup_state: ExecutionSetupState,
    execution_gate: ExecutionGate,
    next_action: Option<RetryAction>,
    leading_requirement: Option<&SetupRequirement>,
    provisioning: Option<&ProvisioningOperationResponse>,
    reconciliation: Option<&ReconciliationConflictRow>,
) -> Option<ExecutionBlockerProjection> {
    let next_action = next_action?;
    let (code, stage, headline, safe_explanation, required_principal, governing_ref) = blocker_copy(
        coordination_state,
        execution_setup_state,
        execution_gate,
        leading_requirement,
        provisioning,
        reconciliation,
    );
    let affected_refs = Vec::new();
    let blocker_digest = compute_blocker_digest(
        code,
        ExecutionBlockerScope::Project,
        &affected_refs,
        governing_ref.as_ref(),
        project_version,
        reconciliation,
    );
    Some(ExecutionBlockerProjection {
        code,
        stage,
        scope: ExecutionBlockerScope::Project,
        affected_refs,
        governing_ref,
        headline,
        safe_explanation,
        evidence: None,
        required_principal,
        next_action,
        blocker_digest,
        observed_version: project_version,
    })
}

#[allow(clippy::too_many_lines)]
fn blocker_copy(
    coordination_state: CoordinationState,
    execution_setup_state: ExecutionSetupState,
    execution_gate: ExecutionGate,
    leading_requirement: Option<&SetupRequirement>,
    provisioning: Option<&ProvisioningOperationResponse>,
    reconciliation: Option<&ReconciliationConflictRow>,
) -> (
    ExecutionBlockerCode,
    ExecutionBlockerStage,
    String,
    String,
    ExecutionBlockerPrincipal,
    Option<ExecutionBlockerRecordRef>,
) {
    if coordination_state != CoordinationState::Ready {
        return (
            ExecutionBlockerCode::CoordinationSetupRequired,
            ExecutionBlockerStage::Define,
            "Waiting for Project Agent setup".to_owned(),
            "Project Agent Chat needs its authorized binding before the Project Agent can coordinate this Project.".to_owned(),
            ExecutionBlockerPrincipal::User,
            None,
        );
    }
    if let Some(requirement) = leading_requirement {
        if requirement.role.as_deref() == Some("worker") {
            return (
                ExecutionBlockerCode::WorkerAssignmentRequired,
                ExecutionBlockerStage::Build,
                "Waiting for a Worker".to_owned(),
                "Select or create a Worker before repository-backed execution can proceed."
                    .to_owned(),
                ExecutionBlockerPrincipal::User,
                None,
            );
        }
        if requirement.role.as_deref() == Some("independent_reviewer") {
            return (
                ExecutionBlockerCode::IndependentReviewerAssignmentRequired,
                ExecutionBlockerStage::Review,
                "Waiting for a reviewer".to_owned(),
                "Select any enabled Agent for the reviewer role.".to_owned(),
                ExecutionBlockerPrincipal::User,
                None,
            );
        }
        if requirement.requirement_type == "repository" {
            return (
                ExecutionBlockerCode::RepositorySetupRequired,
                ExecutionBlockerStage::Build,
                "Waiting for repository setup".to_owned(),
                "Attach the Project's primary repository before repository-backed execution can start.".to_owned(),
                ExecutionBlockerPrincipal::User,
                None,
            );
        }
        if requirement.requirement_type == "provisioning" {
            if execution_setup_state == ExecutionSetupState::Failed {
                let explanation = provisioning
                    .and_then(|operation| operation.last_error_message.clone())
                    .filter(|message| !message.trim().is_empty())
                    .unwrap_or_else(|| {
                        "The server recorded a provisioning failure without a user-facing detail."
                            .to_owned()
                    });
                return (
                    ExecutionBlockerCode::ProvisioningFailed,
                    ExecutionBlockerStage::Build,
                    "Provisioning failed".to_owned(),
                    explanation,
                    ExecutionBlockerPrincipal::User,
                    None,
                );
            }
            return (
                ExecutionBlockerCode::ProvisioningInProgress,
                ExecutionBlockerStage::Build,
                "Provisioning in progress".to_owned(),
                "Repository-backed execution is provisioning. No operational success is claimed yet."
                    .to_owned(),
                ExecutionBlockerPrincipal::System,
                None,
            );
        }
        if requirement.requirement_type == "execution_gate_projection"
            || requirement.requirement_type == "execution_setup_projection"
        {
            return (
                ExecutionBlockerCode::ProjectionUnavailable,
                ExecutionBlockerStage::Build,
                "Execution status unavailable".to_owned(),
                "Forge could not verify execution status. Refresh before acting.".to_owned(),
                ExecutionBlockerPrincipal::System,
                None,
            );
        }
        // Falls through for coordination and legacy execution-baseline rows.
    }
    match execution_gate {
        ExecutionGate::PreBaselineReadOnly => (
            ExecutionBlockerCode::PreBaselineReadOnly,
            ExecutionBlockerStage::Plan,
            "Legacy planning status".to_owned(),
            "This legacy status no longer blocks implementation; the approved Charter is the Task execution authority.".to_owned(),
            ExecutionBlockerPrincipal::ProjectAgent,
            None,
        ),
        ExecutionGate::BaselineApprovalRequired => (
            ExecutionBlockerCode::BaselineApprovalRequired,
            ExecutionBlockerStage::Plan,
            "Optional traceability plan review".to_owned(),
            "Implementation already follows the approved Charter; reviewing this optional plan does not start or stop Tasks.".to_owned(),
            ExecutionBlockerPrincipal::User,
            None,
        ),
        ExecutionGate::ReconciliationRequired => {
            let invalid_baseline = reconciliation
                .is_some_and(|conflict| conflict.conflict_code == "invalid_active_baseline");
            let governing_ref = reconciliation.map(|conflict| ExecutionBlockerRecordRef {
                record_type: conflict.governing_record_type.clone(),
                record_id: conflict.governing_record_id.clone(),
                label: None,
            });
            if invalid_baseline {
                (
                    ExecutionBlockerCode::InvalidActiveBaseline,
                    ExecutionBlockerStage::Build,
                    "Active plan needs repair".to_owned(),
                    "The optional traceability plan is invalid. Repairing it does not gate unrelated Task execution.".to_owned(),
                    ExecutionBlockerPrincipal::User,
                    governing_ref,
                )
            } else {
                (
                    ExecutionBlockerCode::ReconciliationRequired,
                    ExecutionBlockerStage::Build,
                    "Traceability conflict".to_owned(),
                    "Review the recorded difference. Only a conflict scoped to a Task can block that Task.".to_owned(),
                    ExecutionBlockerPrincipal::User,
                    governing_ref,
                )
            }
        }
        ExecutionGate::Active | ExecutionGate::Unavailable => (
            ExecutionBlockerCode::ProjectionUnavailable,
            ExecutionBlockerStage::Build,
            "Execution status unavailable".to_owned(),
            "Forge could not verify execution status. Refresh before acting.".to_owned(),
            ExecutionBlockerPrincipal::System,
            None,
        ),
    }
}

fn compute_blocker_digest(
    code: ExecutionBlockerCode,
    scope: ExecutionBlockerScope,
    affected_refs: &[ExecutionBlockerRecordRef],
    governing_ref: Option<&ExecutionBlockerRecordRef>,
    observed_version: i64,
    conflict: Option<&ReconciliationConflictRow>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(format!("{code:?}\0{scope:?}\0").as_bytes());
    for reference in affected_refs {
        hasher.update(format!("{}:{}\0", reference.record_type, reference.record_id).as_bytes());
    }
    if let Some(reference) = governing_ref {
        hasher.update(format!("g:{}:{}\0", reference.record_type, reference.record_id).as_bytes());
    }
    hasher.update(observed_version.to_le_bytes());
    if let Some(conflict) = conflict {
        // Fold in the exact reconciliation record identity and its last
        // update so a superseding/newly-recorded conflict for the same code
        // and scope is never mistaken for the same unchanged blocker.
        hasher.update(
            format!("r:{}:{}\0", conflict.reconciliation_id, conflict.updated_at).as_bytes(),
        );
    }
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

/// Canonical attempt/execution/commit evidence for one Task (D17, F12).
/// `has_commit` follows the same `after_sha.is_some()` convention used
/// elsewhere in this codebase to detect committed execution results.
async fn task_execution_evidence(db: &SqliteDb, task_id: &str) -> Result<ExecutionEvidenceSummary> {
    let row = sqlx::query(
        "SELECT
             COUNT(*) AS execution_count,
             SUM(CASE WHEN role != 'reviewer' THEN 1 ELSE 0 END) AS attempt_count,
             (SELECT after_sha FROM execution
                WHERE task_id = ? AND after_sha IS NOT NULL
                ORDER BY created_at DESC, id DESC LIMIT 1) AS latest_commit_sha
         FROM execution WHERE task_id = ?",
    )
    .bind(task_id)
    .bind(task_id)
    .fetch_one(db.pool())
    .await?;
    let execution_count: i64 = sqlx::Row::try_get(&row, "execution_count")?;
    let attempt_count: i64 =
        sqlx::Row::try_get::<Option<i64>, _>(&row, "attempt_count")?.unwrap_or(0);
    let latest_commit_sha: Option<String> = sqlx::Row::try_get(&row, "latest_commit_sha")?;
    Ok(ExecutionEvidenceSummary::from_counts(
        attempt_count,
        execution_count,
        latest_commit_sha.is_some(),
        latest_commit_sha,
    ))
}

/// Load the canonical execution blocker for one Task (D16/D17).
///
/// The returned evidence is always this Task's own attempt/execution/commit
/// history — it is never re-derived from the Project gate, so a Task with a
/// commit can never be described as "not started" (F12). When the Project
/// itself has no outstanding blocker, this also checks for a reconciliation
/// scoped specifically to this Task; that blocker attaches only here and
/// never widens the Project's execution gate for unrelated work (D16).
pub async fn load_task_execution_blocker(
    db: &SqliteDb,
    task: &db::Task,
) -> Result<(ExecutionEvidenceSummary, Option<ExecutionBlockerProjection>)> {
    let evidence = task_execution_evidence(db, &task.id).await?;

    if matches!(task.status.as_str(), "done" | "cancelled") {
        return Ok((evidence, None));
    }

    let Some(project) = ProjectRepo::get_by_id(db, &task.project_id).await? else {
        return Ok((evidence, None));
    };
    let charter_backed = project.charter_status == "charter_backed"
        && !project.charter_setup_required
        && project.current_charter_revision_id.is_some();
    if !charter_backed {
        return Ok((evidence, None));
    }

    let capability_class = sqlx::query_scalar::<_, Option<String>>(
        "SELECT capability_class FROM project_task_governance
         WHERE task_id = ? AND project_id = ?",
    )
    .bind(&task.id)
    .bind(&task.project_id)
    .fetch_optional(db.pool())
    .await?
    .flatten();
    let execution_class = classify_task_execution(&task.task_type, capability_class.as_deref())
        .unwrap_or(TaskExecutionClass::Implementation);
    if execution_class == TaskExecutionClass::ReadOnlyPlanning {
        return Ok((evidence, None));
    }
    if capability_class
        .as_deref()
        .is_some_and(is_read_only_capability)
    {
        return Ok((evidence, None));
    }

    let setup = load_project_execution_setup(db, &task.project_id).await?;
    if let Some(mut blocker) = setup.execution_blocker {
        blocker.evidence = Some(evidence.clone());
        return Ok((evidence, Some(blocker)));
    }

    // The Project itself is fully clear. A reconciliation scoped to exactly
    // this Task still blocks only this Task's repository mutation.
    let conflicts = required_reconciliation_conflicts(db, &task.project_id).await?;
    let Some(conflict) = conflicts
        .iter()
        .find(|conflict| conflict.is_scoped_to_task(&task.id))
    else {
        return Ok((evidence, None));
    };
    let governing_ref = Some(ExecutionBlockerRecordRef {
        record_type: conflict.governing_record_type.clone(),
        record_id: conflict.governing_record_id.clone(),
        label: None,
    });
    let affected_refs = vec![ExecutionBlockerRecordRef {
        record_type: "task".to_owned(),
        record_id: task.id.clone(),
        label: Some(task.title.clone()),
    }];
    let blocker_digest = compute_blocker_digest(
        ExecutionBlockerCode::ReconciliationRequired,
        ExecutionBlockerScope::Task,
        &affected_refs,
        governing_ref.as_ref(),
        task.version,
        Some(conflict),
    );
    let blocker = ExecutionBlockerProjection {
        code: ExecutionBlockerCode::ReconciliationRequired,
        stage: ExecutionBlockerStage::Build,
        scope: ExecutionBlockerScope::Task,
        affected_refs,
        governing_ref,
        headline: "Waiting for plan reconciliation".to_owned(),
        safe_explanation: "This Task's governance changed and must be reconciled before it can \
            safely resume. The rest of the Project's approved plan remains active."
            .to_owned(),
        evidence: Some(evidence.clone()),
        required_principal: ExecutionBlockerPrincipal::User,
        next_action: RetryAction::ResolveReconciliation,
        blocker_digest,
        observed_version: task.version,
    };
    Ok((evidence, Some(blocker)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{
        create_sqlite_pool, now_rfc3339, run_migrations, CreateProject, ProjectRepo, SqliteDb,
    };

    fn operation(status: &str, retryable: bool) -> ProvisioningOperationResponse {
        ProvisioningOperationResponse {
            id: "operation".to_owned(),
            status: status.to_owned(),
            current_checkpoint: "preflight".to_owned(),
            attempt_count: 0,
            max_attempts: 3,
            lease_owner: None,
            lease_expires_at: None,
            next_retry_at: None,
            retryable,
            last_error_code: None,
            last_error_message: None,
            version: 1,
        }
    }

    #[test]
    fn retryable_setup_required_has_finite_recovery_action() {
        let operation = operation("setup_required", true);
        let requirement =
            provisioning_requirement(ExecutionSetupState::SetupRequired, Some(&operation))
                .expect("retry action");
        assert_eq!(requirement.requirement_type, "provisioning");
        assert_eq!(requirement.action, Some(RetryAction::RetryProvisioning));
    }

    #[test]
    fn non_retryable_setup_required_requires_setup_instead_of_retry() {
        let operation = operation("setup_required", false);
        let requirement =
            provisioning_requirement(ExecutionSetupState::SetupRequired, Some(&operation))
                .expect("configuration action");
        assert_eq!(requirement.action, Some(RetryAction::CompleteSetup));
    }

    #[test]
    fn retryable_failed_operation_with_attempts_remaining_can_retry() {
        let operation = operation("failed", true);
        let requirement = provisioning_requirement(ExecutionSetupState::Failed, Some(&operation))
            .expect("retry action");
        assert_eq!(requirement.requirement_type, "provisioning");
        assert_eq!(requirement.action, Some(RetryAction::RetryProvisioning));
    }

    #[test]
    fn terminal_failed_operation_requires_setup_instead_of_retry() {
        let operation = operation("failed", false);
        let requirement = provisioning_requirement(ExecutionSetupState::Failed, Some(&operation))
            .expect("configuration action");
        assert_eq!(requirement.requirement_type, "provisioning");
        assert_eq!(requirement.action, Some(RetryAction::CompleteSetup));
    }

    #[test]
    fn exhausted_retryable_operation_requires_setup_instead_of_retry() {
        let mut operation = operation("failed", true);
        operation.attempt_count = operation.max_attempts;
        let requirement = provisioning_requirement(ExecutionSetupState::Failed, Some(&operation))
            .expect("configuration action");
        assert_eq!(requirement.action, Some(RetryAction::CompleteSetup));
    }

    #[test]
    fn unavailable_setup_source_has_refresh_action() {
        let requirement = provisioning_requirement(ExecutionSetupState::Unavailable, None)
            .expect("refresh action");
        assert_eq!(requirement.action, Some(RetryAction::RefreshAndRetry));
    }

    #[test]
    fn expired_lease_is_not_current_provisioning() {
        let mut response = operation("provisioning", true);
        response.lease_expires_at = Some("2020-01-01T00:00:00Z".to_owned());
        assert!(provisioning_lease_expired(&response));
        assert_eq!(
            provisioning_requirement(ExecutionSetupState::Failed, Some(&response))
                .and_then(|requirement| requirement.action),
            Some(RetryAction::RetryProvisioning)
        );
    }

    #[tokio::test]
    async fn baseline_state_does_not_gate_task_execution() {
        let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
        run_migrations(&pool).await.expect("migrations");
        let db = SqliteDb::new(pool);
        let now = now_rfc3339();
        ProjectRepo::create(
            &db,
            CreateProject {
                id: "projection-baseline-project".to_owned(),
                name: "Projection baseline project".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project");
        sqlx::query(
            "INSERT INTO project_execution_baseline
                (id, project_id, lifecycle, version, created_at, updated_at)
             VALUES ('baseline-active', 'projection-baseline-project', 'active', 1, ?, ?),
                    ('baseline-draft', 'projection-baseline-project', 'draft', 1, ?, ?)",
        )
        .bind(&now)
        .bind(&now)
        .bind(&now)
        .bind("9999-01-01T00:00:00Z")
        .execute(db.pool())
        .await
        .expect("baselines");

        assert_eq!(
            execution_gate(&db, "projection-baseline-project")
                .await
                .expect("gate")
                .0,
            ExecutionGate::Active
        );
    }

    #[tokio::test]
    async fn optional_gate_source_failure_does_not_block_execution() {
        let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
        run_migrations(&pool).await.expect("migrations");
        let db = SqliteDb::new(pool);
        let now = now_rfc3339();
        ProjectRepo::create(
            &db,
            CreateProject {
                id: "projection-unavailable-project".to_owned(),
                name: "Projection unavailable project".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("project");
        sqlx::query("DROP TABLE project_reconciliation_record")
            .execute(db.pool())
            .await
            .expect("reconciliation table drops");

        let projection = load_project_execution_setup(&db, "projection-unavailable-project")
            .await
            .expect("projection remains readable");
        assert_eq!(projection.execution_gate, ExecutionGate::Active);
        assert_eq!(
            projection.availability.execution_gate.availability,
            api_types::ProjectionAvailability::Current
        );
        assert!(!projection
            .setup_requirements
            .iter()
            .any(|requirement| { requirement.requirement_type == "execution_gate_projection" }));
    }

    #[tokio::test]
    async fn ready_marker_is_downgraded_when_repository_is_missing() {
        let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
        run_migrations(&pool).await.expect("migrations");
        let db = SqliteDb::new(pool);
        let now = now_rfc3339();
        ProjectRepo::create(
            &db,
            CreateProject {
                id: "projection-stale-ready-project".to_owned(),
                name: "Projection stale ready project".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project");
        sqlx::query(
            "UPDATE project_provisioning_operation
             SET status = 'ready', current_checkpoint = 'completed',
                 completed_at = ?, retryable = 0, updated_at = ?
             WHERE project_id = ?",
        )
        .bind(&now)
        .bind(&now)
        .bind("projection-stale-ready-project")
        .execute(db.pool())
        .await
        .expect("ready marker");

        let projection = load_project_execution_setup(&db, "projection-stale-ready-project")
            .await
            .expect("projection");
        assert_eq!(
            projection.execution_setup_state,
            ExecutionSetupState::SetupRequired
        );
        assert!(projection.setup_requirements.iter().any(|requirement| {
            requirement.requirement_type == "repository"
                && requirement.action == Some(RetryAction::AttachRepository)
        }));
    }

    #[tokio::test]
    async fn ready_marker_is_downgraded_when_local_repository_drifts() {
        let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
        run_migrations(&pool).await.expect("migrations");
        let db = SqliteDb::new(pool);
        let now = now_rfc3339();
        ProjectRepo::create(
            &db,
            CreateProject {
                id: "projection-drift-project".to_owned(),
                name: "Projection drift project".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project");
        sqlx::query(
            "INSERT INTO repo
                (id, project_id, name, remote_url, local_path, work_mode,
                 default_branch, created_at, updated_at)
             VALUES ('drift-repo', 'projection-drift-project', 'drift',
                     'file:///tmp/drift', '/tmp/forge-missing-repository',
                     'direct_merge', 'main', ?, ?)",
        )
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("repo");
        sqlx::query("UPDATE project SET primary_repo_id = ?, updated_at = ? WHERE id = ?")
            .bind("drift-repo")
            .bind(&now)
            .bind("projection-drift-project")
            .execute(db.pool())
            .await
            .expect("primary repo");
        sqlx::query(
            "UPDATE project_provisioning_operation
             SET status = 'ready', current_checkpoint = 'completed',
                 completed_at = ?, retryable = 0, updated_at = ?
             WHERE project_id = ?",
        )
        .bind(&now)
        .bind(&now)
        .bind("projection-drift-project")
        .execute(db.pool())
        .await
        .expect("ready marker");

        let projection = load_project_execution_setup(&db, "projection-drift-project")
            .await
            .expect("projection");
        assert_eq!(
            projection.execution_setup_state,
            ExecutionSetupState::SetupRequired
        );
        assert!(projection.primary_repo.is_some());
        assert!(projection.setup_requirements.iter().any(|requirement| {
            requirement.requirement_type == "repository"
                && requirement.action == Some(RetryAction::AttachRepository)
        }));
    }
}
