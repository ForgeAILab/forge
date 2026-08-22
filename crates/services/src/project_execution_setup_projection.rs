//! Canonical Project execution-setup projection.
//!
//! Coordination, repository/role setup, and the execution-baseline gate are
//! independent dimensions. A source read failure is surfaced as an explicit
//! unavailable dimension with a retry action; it is never converted into a
//! plausible `setup_required` or `ready` result.

use std::path::Path;

use api_types::{
    CoordinationState, ExecutionGate, ExecutionPrincipalResponse, ExecutionSetupState,
    ProjectExecutionSetupAvailability, ProjectExecutionSetupResponse, ProjectionStatus,
    ProvisioningOperationResponse, RepoResponse, RetryAction, SetupRequirement,
};
use db::{
    Agent, AgentChatRepo, Project, ProjectAgentBindingRepo, ProjectProvisioningRepo, ProjectRepo,
    RepoRepo, SqliteDb,
};

use crate::{
    execution_setup::{eligible_project_execution_agents, resolve_project_execution_roles},
    Result, ServiceError,
};

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
    if coordination_state != CoordinationState::Ready {
        let mut requirement = SetupRequirement::new("coordination");
        requirement.resource_type = Some("project_agent_chat".to_owned());
        requirement.action = Some(if coordination_state == CoordinationState::Unavailable {
            RetryAction::RefreshAndRetry
        } else {
            RetryAction::CompleteSetup
        });
        setup_requirements.insert(0, requirement);
    }
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
    } else {
        provisioning_state(provisioning.as_ref())
    };

    // A durable ready marker is advisory until current repository and role
    // records agree with it. This protects against edits after provisioning
    // or a stale migration/backfill result.
    if setup_source_available
        && execution_setup_state == ExecutionSetupState::Ready
        && (!primary_repo_ready
            || roles
                .as_ref()
                .is_none_or(|resolution| !resolution.requirements.is_empty()))
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
        .filter(|agent| Some(agent.id.as_str()) != worker_identity_id)
        .cloned()
        .map(execution_principal_response)
        .collect::<Vec<_>>();

    let (execution_gate, gate_status) = match execution_gate(db, project_id).await {
        Ok(gate) => (gate, ProjectionStatus::current()),
        Err(_) => (ExecutionGate::Unavailable, ProjectionStatus::unavailable()),
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
            ExecutionGate::ReconciliationRequired | ExecutionGate::Unavailable => {
                Some(RetryAction::RefreshAndRetry)
            }
            ExecutionGate::Active => None,
        });

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

async fn execution_gate(db: &SqliteDb, project_id: &str) -> Result<ExecutionGate> {
    let reconciliation_required = sqlx::query_scalar::<_, i64>(
        "SELECT EXISTS (
             SELECT 1 FROM project_reconciliation_record
             WHERE project_id = ? AND state = 'required'
         )",
    )
    .bind(project_id)
    .fetch_one(db.pool())
    .await?
        != 0;
    if reconciliation_required {
        return Ok(ExecutionGate::ReconciliationRequired);
    }

    let baseline = sqlx::query(
        "SELECT b.lifecycle, r.lifecycle AS revision_lifecycle
         FROM project_execution_baseline AS b
         LEFT JOIN project_execution_baseline_revision AS r
           ON r.id = b.current_revision_id AND r.baseline_id = b.id
         WHERE b.project_id = ?
         ORDER BY CASE b.lifecycle
             WHEN 'active' THEN 0
             WHEN 'proposed' THEN 1
             WHEN 'approved' THEN 2
             ELSE 3
         END,
         b.updated_at DESC, b.id DESC
         LIMIT 1",
    )
    .bind(project_id)
    .fetch_optional(db.pool())
    .await?;
    let Some(row) = baseline else {
        return Ok(ExecutionGate::PreBaselineReadOnly);
    };
    let lifecycle: String = sqlx::Row::try_get(&row, "lifecycle")?;
    let revision_lifecycle: Option<String> = sqlx::Row::try_get(&row, "revision_lifecycle")?;
    Ok(match (lifecycle.as_str(), revision_lifecycle.as_deref()) {
        ("active", Some("approved")) => ExecutionGate::Active,
        ("active", _) => ExecutionGate::ReconciliationRequired,
        ("proposed" | "approved", _) => ExecutionGate::BaselineApprovalRequired,
        _ => ExecutionGate::PreBaselineReadOnly,
    })
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
        ExecutionGate::BaselineApprovalRequired => Some(RetryAction::Reauthorize),
        ExecutionGate::PreBaselineReadOnly => Some(RetryAction::Repropose),
        ExecutionGate::ReconciliationRequired => Some(RetryAction::RefreshAndRetry),
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
    async fn active_baseline_wins_over_newer_draft() {
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
                .expect("gate"),
            ExecutionGate::ReconciliationRequired
        );
    }

    #[tokio::test]
    async fn gate_source_failure_is_explicitly_unavailable() {
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
        assert_eq!(projection.execution_gate, ExecutionGate::Unavailable);
        assert_eq!(
            projection.availability.execution_gate.availability,
            api_types::ProjectionAvailability::Unavailable
        );
        assert_eq!(projection.next_action, Some(RetryAction::CompleteSetup));
        assert!(projection.setup_requirements.iter().any(|requirement| {
            requirement.requirement_type == "execution_gate_projection"
                && requirement.action == Some(RetryAction::RefreshAndRetry)
        }));
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
