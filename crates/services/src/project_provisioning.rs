//! Post-creation provisioning for Charter-backed (Product Genesis) Projects.
//!
//! Genesis produces a Project with a Charter, Project Agent binding, and
//! handoff — but nothing executable: no repository and no executor role
//! defaults, so every proposed Task would sit in the backlog with the
//! dispatcher silently skipping it. This module closes that gap after the
//! atomic create commits: it initializes a local git repository under the
//! workspace root, registers it as the primary repo, and seeds default
//! coder/reviewer role assignments from the account's executor agents
//! (preferring the provider family the user selected for the Project Agent).
//!
//! Provisioning is a leased, finite, idempotent operation. A committed
//! Project create is never rolled back for a setup blocker; the durable
//! operation/checkpoint projection carries the blocker and its retry action.

use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
};

use api_types::{CharterScaffold, RetryAction, SetupRequirement};
use chrono::{Duration, Utc};
use db::{
    new_uuid_v4, now_rfc3339, CreateProjectProvisioningError, CreateProjectProvisioningOperation,
    CreateRepo, PageRequest, Project, ProjectOrchestrationRepo, ProjectProvisioningOperation,
    ProjectProvisioningRepo, ProjectRepo, ReconcileProjectProvisioningCheckpoint,
    ReconcileProjectProvisioningMetadata, Repo, RepoRepo, SortBy, SortOrder, SqliteDb,
    UpdateProject, UpdateProjectProvisioningOperation, UpsertProjectProvisioningCheckpoint,
    WorkMode,
};
use serde_json::{json, Value};
use tokio::process::Command;

use crate::{
    execution_setup::{
        resolve_project_execution_roles, resolve_project_execution_roles_for_provisioning,
    },
    Result, ServiceError,
};

const DEFAULT_BRANCH: &str = "main";
const MAX_ATTEMPTS: i64 = 3;
const LEASE_SECONDS: i64 = 300;
const CHECKPOINTS: [&str; 6] = [
    "preflight",
    "repository_scaffolded",
    "repository_initialized",
    "repository_registered",
    "repository_linked",
    "roles_assigned",
];
/// Environment variable `forge-cli` exports from `[scaffold] command`; the
/// services crate reads it the same way it reads `FORGE_WORKSPACE_ROOT`.
pub const SCAFFOLD_COMMAND_ENV: &str = "FORGE_SCAFFOLD_COMMAND";
/// Written by create-spark into every scaffold; its presence is how a
/// directory proves it already is one.
const SCAFFOLD_MARKER: &str = "spark.config.json";
const SCAFFOLD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(180);
const SCAFFOLD_OUTPUT_TAIL_CHARS: usize = 2_000;

/// Reconcile a setup action's already-committed repository/role view into the
/// durable provisioning projection.  User actions do not consume a
/// provisioning attempt, but they still have to prove the same repository,
/// role, and checkpoint invariants as Genesis provisioning before exposing a
/// `ready` operation.
pub(crate) async fn reconcile_project_setup_metadata(
    db: &Arc<SqliteDb>,
    project: &Project,
) -> Result<Option<ReconcileProjectProvisioningMetadata>> {
    let Some(operation) =
        ProjectProvisioningRepo::get_provisioning_operation(&**db, &project.id).await?
    else {
        // Project creation persists this operation atomically. Legacy rows
        // without one are left for the durable provisioning reconciler; a
        // setup action must not create durable metadata outside its command
        // transaction.
        return Ok(None);
    };
    if operation.status == "provisioning" {
        // A user action must not steal an active filesystem reconciler's
        // lease. Its owner will publish the next truthful projection.
        return Ok(None);
    }

    let Some(repo_id) = project.primary_repo_id.as_deref() else {
        return Ok(None);
    };
    let Some(repo) = RepoRepo::get_by_id(&**db, repo_id).await? else {
        return Ok(None);
    };
    if repo.project_id != project.id || repo.default_branch != DEFAULT_BRANCH {
        return Ok(None);
    }

    let now = now_rfc3339();
    let (repository_status, repository_details) =
        if let Some(local_path) = repo.local_path.as_deref() {
            let path = Path::new(local_path);
            if !git::is_git_repo(path).await
                || !git::branch_exists(path, DEFAULT_BRANCH).await?
                || git::get_current_sha(path).await?.trim().is_empty()
            {
                return Ok(None);
            }
            (
                "completed",
                json!({
                    "source": "execution_setup_action",
                    "path": local_path,
                    "filesystem_verified": true,
                }),
            )
        } else if !repo.remote_url.trim().is_empty() {
            // A remote-only binding has no local filesystem to inspect. Record
            // that it was explicitly verified as a remote binding rather than
            // pretending a local Git checkpoint completed.
            (
                "skipped",
                json!({
                    "source": "execution_setup_action",
                    "remote_url": repo.remote_url,
                    "filesystem_verified": false,
                    "remote_binding_verified": true,
                }),
            )
        } else {
            return Ok(None);
        };

    let roles = resolve_project_execution_roles(db, project).await?;
    if !roles.requirements.is_empty() {
        return Ok(None);
    }

    let checkpoints = ProjectProvisioningRepo::list_provisioning_checkpoints(&**db, &operation.id)
        .await?
        .into_iter()
        .map(|checkpoint| (checkpoint.checkpoint.clone(), checkpoint))
        .collect::<std::collections::HashMap<_, _>>();
    // A setup action supplies the repository itself, so the scaffold step is
    // moot unless Genesis provisioning already ran it: keep a completed or
    // skipped row verbatim, otherwise record that setup superseded it.
    let (scaffold_status, scaffold_details) = match checkpoints.get("repository_scaffolded") {
        Some(current) if current.status == "completed" || current.status == "skipped" => (
            current.status.clone(),
            serde_json::from_str::<Value>(&current.details_json).unwrap_or_else(|_| json!({})),
        ),
        _ => (
            "skipped".to_owned(),
            json!({
                "source": "execution_setup_action",
                "reason": "repository_supplied_by_setup_action",
            }),
        ),
    };
    let checkpoint_details = [
        (
            "preflight",
            "completed",
            json!({"source": "execution_setup_action", "verified": true}),
        ),
        (
            "repository_scaffolded",
            scaffold_status.as_str(),
            scaffold_details,
        ),
        (
            "repository_initialized",
            repository_status,
            repository_details,
        ),
        (
            "repository_registered",
            "completed",
            json!({"source": "execution_setup_action", "repo_id": repo.id}),
        ),
        (
            "repository_linked",
            "completed",
            json!({"source": "execution_setup_action", "repo_id": repo.id}),
        ),
        (
            "roles_assigned",
            "completed",
            json!({
                "source": "execution_setup_action",
                "assignments": project_role_assignments(project),
            }),
        ),
    ];
    let mut checkpoint_updates = Vec::with_capacity(checkpoint_details.len());
    for (checkpoint, status, details) in checkpoint_details {
        let Some(current) = checkpoints.get(checkpoint) else {
            // Missing rows are a durable provisioning repair concern. Keep
            // this guard so a concurrent schema/repository failure cannot
            // claim readiness from an incomplete snapshot.
            return Ok(None);
        };
        let completed_at = current.completed_at.clone().or_else(|| Some(now.clone()));
        checkpoint_updates.push(ReconcileProjectProvisioningCheckpoint {
            id: current.id.clone(),
            operation_id: operation.id.clone(),
            checkpoint: checkpoint.to_owned(),
            status: status.to_owned(),
            attempt_count: current.attempt_count,
            details_json: details.to_string(),
            started_at: current.started_at.clone().or_else(|| Some(now.clone())),
            completed_at,
            created_at: current.created_at.clone(),
            expected_version: current.version,
        });
    }

    Ok(Some(ReconcileProjectProvisioningMetadata {
        operation_id: operation.id,
        expected_version: operation.version,
        status: "ready".to_owned(),
        current_checkpoint: "completed".to_owned(),
        retryable: false,
        completed_at: Some(now.clone()),
        updated_at: now,
        checkpoints: checkpoint_updates,
    }))
}

/// Make a freshly created Genesis Project executable. Called after the
/// atomic Charter-approval create commits. The durable operation is the
/// source of truth for retries and response-loss replays; a setup blocker is
/// returned as a committed `setup_required` operation rather than being
/// reduced to an operator-only log line.
pub async fn provision_genesis_project(
    db: &Arc<SqliteDb>,
    project_id: &str,
) -> Result<ProjectProvisioningOperation> {
    let lease_owner = format!("genesis-provisioning:{}", new_uuid_v4());
    provision_genesis_project_with_lease(db, project_id, &lease_owner).await
}

/// Reconcile a caller-owned durable provisioning lease.  User-triggered
/// setup retries use this entry point after atomically scheduling the lease
/// with their command receipt; response loss therefore leaves recoverable
/// durable work rather than an unrecorded filesystem attempt.
pub async fn provision_genesis_project_with_lease(
    db: &Arc<SqliteDb>,
    project_id: &str,
    lease_owner: &str,
) -> Result<ProjectProvisioningOperation> {
    let project = ProjectRepo::get_by_id(&**db, project_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("project", project_id))?;
    let mut operation = ensure_operation(db, &project).await?;
    if operation.status == "ready" {
        // V087 deliberately backfilled repository checkpoints from the
        // Project link.  A link is not proof that the filesystem still has a
        // Git repository with a usable main history, so verify the durable
        // state before treating a backfilled ready operation as complete.
        if ready_operation_is_verified(db, &project, &operation).await? {
            return Ok(operation);
        }
        operation = reopen_ready_operation(db, operation).await?;
    }

    // A retry command commits the lease and increments the attempt before it
    // invokes the filesystem reconciler.  If the process dies after that
    // commit, replaying the same command must resume the same accepted lease,
    // even when its expiry has passed; claiming it again would consume a
    // second retry budget slot for one user command.
    let claimed = if operation.status == "provisioning"
        && operation.lease_owner.as_deref() == Some(lease_owner)
    {
        if lease_is_active(operation.lease_expires_at.as_deref()) {
            Some(operation.clone())
        } else {
            // The same accepted retry may outlive its lease while the
            // process is stopped. Renew its lease without incrementing the
            // attempt counter; a response-loss replay must resume the one
            // durable attempt rather than consume another budget slot.
            match renew_owned_lease(db, &operation, lease_owner).await {
                Ok(operation) => Some(operation),
                Err(ServiceError::Db(db::DbError::VersionConflict)) => {
                    let latest = ProjectProvisioningRepo::get_provisioning_operation_by_id(
                        &**db,
                        &operation.id,
                    )
                    .await?;
                    latest.and_then(|current| {
                        (current.status == "provisioning"
                            && current.lease_owner.as_deref() == Some(lease_owner)
                            && lease_is_active(current.lease_expires_at.as_deref()))
                        .then_some(current)
                    })
                }
                Err(error) => return Err(error),
            }
        }
    } else {
        match claim_operation(db, operation.clone(), lease_owner).await {
            Ok(claimed) => claimed,
            Err(error) => {
                tracing::warn!(
                    project_id = %project_id,
                    operation_id = %operation.id,
                    error = %error,
                    "Genesis Project provisioning claim failed"
                );
                let latest =
                    ProjectProvisioningRepo::get_provisioning_operation_by_id(&**db, &operation.id)
                        .await?
                        .unwrap_or(operation);
                return persist_unowned_provisioning_failure(db, latest).await;
            }
        }
    };
    let Some(claimed) = claimed else {
        return Ok(operation);
    };
    operation = claimed;
    if operation.status == "ready" || (operation.status == "failed" && !operation.retryable) {
        return Ok(operation);
    }

    let result = reconcile_operation(db, project.clone(), operation.clone(), lease_owner).await;
    match result {
        Ok(operation) => Ok(operation),
        Err(error) => {
            // Checkpoint updates advance the operation version.  Never pass
            // the pre-reconcile snapshot to failure handling: doing so leaves
            // a live lease behind after a VersionConflict.  A lease that has
            // expired or been reclaimed by another process is not ours to
            // finalize; the durable row is the truthful result for this call.
            tracing::warn!(
                project_id = %project_id,
                operation_id = %operation.id,
                error = %error,
                "Genesis Project provisioning reconciliation failed"
            );
            let latest =
                ProjectProvisioningRepo::get_provisioning_operation_by_id(&**db, &operation.id)
                    .await?
                    .unwrap_or(operation);
            if latest.lease_owner.as_deref() == Some(lease_owner)
                && lease_is_active(latest.lease_expires_at.as_deref())
            {
                let checkpoint = latest.current_checkpoint.clone();
                return fail_operation(
                    db,
                    latest,
                    lease_owner,
                    ProvisioningFailure {
                        checkpoint: &checkpoint,
                        code: checkpoint_failure_code(&checkpoint),
                        message: checkpoint_failure_message(&checkpoint),
                        retryable: true,
                        current_checkpoint: failure_current_checkpoint(&checkpoint),
                    },
                )
                .await;
            }

            // Errors before a lease is acquired (for example a database
            // failpoint during claim) still have one durable operation row.
            // Finalize that row as a typed retryable failure instead of
            // allowing the caller to report provisioning success.
            persist_unowned_provisioning_failure(db, latest).await
        }
    }
}

/// Finalize an operation that failed before this invocation acquired an active
/// lease.  The expected version protects the row from a concurrent claimant;
/// a conflict is returned rather than overwriting that owner's result.
async fn persist_unowned_provisioning_failure(
    db: &Arc<SqliteDb>,
    operation: ProjectProvisioningOperation,
) -> Result<ProjectProvisioningOperation> {
    if operation.status == "ready" || (operation.status == "failed" && !operation.retryable) {
        return Ok(operation);
    }

    let code = "provisioning_reconciliation_failed";
    let message = "Project provisioning could not be reconciled; retry is available";
    let checkpoint = operation.current_checkpoint.clone();
    let now = now_rfc3339();
    let terminal = operation.attempt_count >= operation.max_attempts;
    if let Some(checkpoint_row) =
        ProjectProvisioningRepo::get_provisioning_checkpoint(&**db, &operation.id, &checkpoint)
            .await?
    {
        ProjectProvisioningRepo::upsert_provisioning_checkpoint(
            &**db,
            UpsertProjectProvisioningCheckpoint {
                id: checkpoint_row.id.clone(),
                operation_id: operation.id.clone(),
                checkpoint: checkpoint.clone(),
                status: "failed".to_owned(),
                attempt_count: checkpoint_row.attempt_count,
                error_code: Some(code.to_owned()),
                error_message: Some(message.to_owned()),
                details_json: checkpoint_row.details_json,
                started_at: checkpoint_row.started_at,
                completed_at: None,
                created_at: checkpoint_row.created_at,
                updated_at: now.clone(),
            },
        )
        .await?;
    }
    let checkpoint_id =
        ProjectProvisioningRepo::get_provisioning_checkpoint(&**db, &operation.id, &checkpoint)
            .await?
            .map(|row| row.id);
    ProjectProvisioningRepo::record_provisioning_error(
        &**db,
        CreateProjectProvisioningError {
            id: new_uuid_v4(),
            operation_id: operation.id.clone(),
            checkpoint_id,
            code: code.to_owned(),
            message: message.to_owned(),
            retryable: !terminal,
            attempt_count: operation.attempt_count,
            created_at: now.clone(),
        },
    )
    .await?;
    ProjectProvisioningRepo::update_provisioning_operation(
        &**db,
        UpdateProjectProvisioningOperation {
            id: operation.id,
            expected_version: operation.version,
            status: Some("failed".to_owned()),
            current_checkpoint: Some(failure_current_checkpoint(&checkpoint).to_owned()),
            attempt_count: None,
            max_attempts: None,
            lease_owner: Some(None),
            lease_expires_at: Some(None),
            next_retry_at: Some(if terminal { None } else { Some(now.clone()) }),
            retryable: Some(!terminal),
            last_error_code: Some(Some(code.to_owned())),
            last_error_message: Some(Some(if terminal {
                "Project provisioning could not be reconciled; retry budget is exhausted".to_owned()
            } else {
                message.to_owned()
            })),
            completed_at: Some(None),
            updated_at: now,
        },
    )
    .await
    .map_err(Into::into)
}

async fn ensure_operation(
    db: &Arc<SqliteDb>,
    project: &Project,
) -> Result<ProjectProvisioningOperation> {
    let now = now_rfc3339();
    let operation = if let Some(operation) =
        ProjectProvisioningRepo::get_provisioning_operation(&**db, &project.id).await?
    {
        operation
    } else {
        let input = CreateProjectProvisioningOperation {
            id: new_uuid_v4(),
            project_id: project.id.clone(),
            idempotency_key: format!("project-provisioning:{}", project.id),
            status: "setup_required".to_owned(),
            current_checkpoint: "preflight".to_owned(),
            max_attempts: MAX_ATTEMPTS,
            lease_owner: None,
            lease_expires_at: None,
            next_retry_at: None,
            retryable: true,
            last_error_code: None,
            last_error_message: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        };
        match ProjectProvisioningRepo::create_provisioning_operation(&**db, input).await {
            Ok(operation) => operation,
            Err(db::DbError::Sqlx(original)) => {
                // SQLite reports both project_id and idempotency-key races as
                // Sqlx errors.  Reload first; only convert to a conflict if
                // the losing insert cannot be observed, preserving unrelated
                // database failures for the caller.
                if let Some(operation) =
                    ProjectProvisioningRepo::get_provisioning_operation(&**db, &project.id).await?
                {
                    operation
                } else {
                    return Err(db::DbError::Sqlx(original).into());
                }
            }
            Err(error) => return Err(error.into()),
        }
    };

    // A process can stop after the operation row commits but before all
    // checkpoint rows are inserted.  Repair only missing rows and preserve
    // completed/running/failed checkpoint state on every invocation.
    let existing = ProjectProvisioningRepo::list_provisioning_checkpoints(&**db, &operation.id)
        .await?
        .into_iter()
        .map(|checkpoint| (checkpoint.checkpoint.clone(), checkpoint))
        .collect::<std::collections::HashMap<_, _>>();
    for checkpoint in CHECKPOINTS {
        if existing.contains_key(checkpoint) {
            continue;
        }
        ProjectProvisioningRepo::upsert_provisioning_checkpoint(
            &**db,
            UpsertProjectProvisioningCheckpoint {
                id: new_uuid_v4(),
                operation_id: operation.id.clone(),
                checkpoint: checkpoint.to_owned(),
                status: "pending".to_owned(),
                attempt_count: 0,
                error_code: None,
                error_message: None,
                details_json: "{}".to_owned(),
                started_at: None,
                completed_at: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await?;
    }
    Ok(operation)
}

async fn claim_operation(
    db: &Arc<SqliteDb>,
    operation: ProjectProvisioningOperation,
    lease_owner: &str,
) -> Result<Option<ProjectProvisioningOperation>> {
    let now = now_rfc3339();
    if operation.status == "ready" {
        return Ok(Some(operation));
    }
    if operation.status == "failed" && !operation.retryable {
        return Ok(Some(operation));
    }
    if operation
        .lease_owner
        .as_deref()
        .is_some_and(|owner| owner != lease_owner)
        && lease_is_active(operation.lease_expires_at.as_deref())
    {
        return Ok(None);
    }

    // Do not increment past the finite retry budget.  A retryable operation
    // at the limit becomes terminal without taking a new lease, so a stale
    // process cannot keep extending the operation forever.
    if operation.attempt_count >= operation.max_attempts {
        let exhausted = ProjectProvisioningRepo::update_provisioning_operation(
            &**db,
            UpdateProjectProvisioningOperation {
                id: operation.id.clone(),
                expected_version: operation.version,
                status: Some("failed".to_owned()),
                current_checkpoint: Some(operation.current_checkpoint.clone()),
                attempt_count: None,
                max_attempts: None,
                lease_owner: Some(None),
                lease_expires_at: Some(None),
                next_retry_at: Some(None),
                retryable: Some(false),
                last_error_code: Some(Some("provisioning_retry_exhausted".to_owned())),
                last_error_message: Some(Some(
                    "Project provisioning exhausted its finite retry budget".to_owned(),
                )),
                completed_at: Some(None),
                updated_at: now,
            },
        )
        .await;
        return match exhausted {
            Ok(operation) => Ok(Some(operation)),
            Err(db::DbError::VersionConflict) => Ok(None),
            Err(error) => Err(error.into()),
        };
    }

    let lease_expires_at = lease_expiry();
    let next_attempt = operation.attempt_count + 1;
    match ProjectProvisioningRepo::update_provisioning_operation(
        &**db,
        UpdateProjectProvisioningOperation {
            id: operation.id,
            expected_version: operation.version,
            status: Some("provisioning".to_owned()),
            current_checkpoint: None,
            attempt_count: Some(next_attempt),
            max_attempts: None,
            lease_owner: Some(Some(lease_owner.to_owned())),
            lease_expires_at: Some(Some(lease_expires_at)),
            next_retry_at: Some(None),
            retryable: Some(false),
            last_error_code: Some(None),
            last_error_message: Some(None),
            completed_at: Some(None),
            updated_at: now,
        },
    )
    .await
    {
        Ok(operation) => Ok(Some(operation)),
        Err(db::DbError::VersionConflict) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

async fn reconcile_operation(
    db: &Arc<SqliteDb>,
    mut project: Project,
    mut operation: ProjectProvisioningOperation,
    lease_owner: &str,
) -> Result<ProjectProvisioningOperation> {
    operation = begin_checkpoint(db, operation, "preflight", lease_owner).await?;
    operation = complete_checkpoint(
        db,
        operation,
        "preflight",
        json!({"verified": true}),
        lease_owner,
    )
    .await?;

    operation = begin_checkpoint(db, operation, "repository_scaffolded", lease_owner).await?;
    operation = match scaffold_repository(db, &project, operation, lease_owner).await? {
        ScaffoldStep::Continue(operation) => operation,
        ScaffoldStep::Failed(operation) => return Ok(operation),
    };

    operation = begin_checkpoint(db, operation, "repository_initialized", lease_owner).await?;
    let repo_path = initialize_repository(db, &project, &operation, lease_owner).await?;
    operation = complete_checkpoint(
        db,
        operation,
        "repository_initialized",
        json!({"path": repo_path.to_string_lossy()}),
        lease_owner,
    )
    .await?;

    operation = begin_checkpoint(db, operation, "repository_registered", lease_owner).await?;
    let repo = find_or_register_repository(db, &project, &repo_path).await?;
    operation = complete_checkpoint(
        db,
        operation,
        "repository_registered",
        json!({"repo_id": repo.id}),
        lease_owner,
    )
    .await?;

    operation = begin_checkpoint(db, operation, "repository_linked", lease_owner).await?;
    project = link_primary_repository(db, project, &repo).await?;
    operation = complete_checkpoint(
        db,
        operation,
        "repository_linked",
        json!({"repo_id": repo.id}),
        lease_owner,
    )
    .await?;

    operation = begin_checkpoint(db, operation, "roles_assigned", lease_owner).await?;
    let resolution = resolve_project_execution_roles_for_provisioning(db, &project).await?;
    let assignments = resolution.default_role_assignments();
    let replace_roles = [
        resolution.worker_role.as_deref(),
        resolution.reviewer_role.as_deref(),
    ];
    project = update_role_assignments(db, project, assignments, &replace_roles).await?;
    if !resolution.requirements.is_empty() {
        let (code, message) = role_blocker(&resolution.requirements);
        return fail_operation(
            db,
            operation,
            lease_owner,
            ProvisioningFailure {
                checkpoint: "roles_assigned",
                code,
                message,
                retryable: true,
                current_checkpoint: "repository_linked",
            },
        )
        .await;
    }
    operation = complete_checkpoint(
        db,
        operation,
        "roles_assigned",
        json!({"assignments": project_role_assignments(&project)}),
        lease_owner,
    )
    .await?;

    let now = now_rfc3339();
    operation = load_owned_operation(db, &operation, lease_owner).await?;
    ProjectProvisioningRepo::update_provisioning_operation(
        &**db,
        UpdateProjectProvisioningOperation {
            id: operation.id,
            expected_version: operation.version,
            status: Some("ready".to_owned()),
            current_checkpoint: Some("completed".to_owned()),
            attempt_count: None,
            max_attempts: None,
            lease_owner: Some(None),
            lease_expires_at: Some(None),
            next_retry_at: Some(None),
            retryable: Some(false),
            last_error_code: Some(None),
            last_error_message: Some(None),
            completed_at: Some(Some(now.clone())),
            updated_at: now,
        },
    )
    .await
    .map_err(Into::into)
}

async fn begin_checkpoint(
    db: &Arc<SqliteDb>,
    operation: ProjectProvisioningOperation,
    checkpoint: &str,
    lease_owner: &str,
) -> Result<ProjectProvisioningOperation> {
    let operation = load_owned_operation(db, &operation, lease_owner).await?;
    let now = now_rfc3339();
    let current =
        ProjectProvisioningRepo::get_provisioning_checkpoint(&**db, &operation.id, checkpoint)
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation("Project provisioning checkpoint is missing")
            })?;
    ProjectProvisioningRepo::upsert_provisioning_checkpoint(
        &**db,
        UpsertProjectProvisioningCheckpoint {
            id: current.id,
            operation_id: operation.id.clone(),
            checkpoint: checkpoint.to_owned(),
            status: "running".to_owned(),
            attempt_count: current.attempt_count + 1,
            error_code: None,
            error_message: None,
            details_json: current.details_json,
            started_at: Some(now.clone()),
            completed_at: None,
            created_at: current.created_at,
            updated_at: now,
        },
    )
    .await?;
    update_operation_checkpoint(db, operation, checkpoint, lease_owner).await
}

async fn complete_checkpoint(
    db: &Arc<SqliteDb>,
    operation: ProjectProvisioningOperation,
    checkpoint: &str,
    details: Value,
    lease_owner: &str,
) -> Result<ProjectProvisioningOperation> {
    let operation = load_owned_operation(db, &operation, lease_owner).await?;
    let current =
        ProjectProvisioningRepo::get_provisioning_checkpoint(&**db, &operation.id, checkpoint)
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation("Project provisioning checkpoint is missing")
            })?;
    let now = now_rfc3339();
    ProjectProvisioningRepo::upsert_provisioning_checkpoint(
        &**db,
        UpsertProjectProvisioningCheckpoint {
            id: current.id,
            operation_id: operation.id.clone(),
            checkpoint: checkpoint.to_owned(),
            status: "completed".to_owned(),
            attempt_count: current.attempt_count,
            error_code: None,
            error_message: None,
            details_json: details.to_string(),
            started_at: current.started_at,
            completed_at: Some(now.clone()),
            created_at: current.created_at,
            updated_at: now,
        },
    )
    .await?;
    load_owned_operation(db, &operation, lease_owner).await
}

/// Mark a checkpoint `skipped`: nothing to do, recorded with the reason so
/// ready verification and the setup projection can tell it from `pending`.
async fn skip_checkpoint(
    db: &Arc<SqliteDb>,
    operation: ProjectProvisioningOperation,
    checkpoint: &str,
    details: Value,
    lease_owner: &str,
) -> Result<ProjectProvisioningOperation> {
    let operation = load_owned_operation(db, &operation, lease_owner).await?;
    let current =
        ProjectProvisioningRepo::get_provisioning_checkpoint(&**db, &operation.id, checkpoint)
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation("Project provisioning checkpoint is missing")
            })?;
    let now = now_rfc3339();
    ProjectProvisioningRepo::upsert_provisioning_checkpoint(
        &**db,
        UpsertProjectProvisioningCheckpoint {
            id: current.id,
            operation_id: operation.id.clone(),
            checkpoint: checkpoint.to_owned(),
            status: "skipped".to_owned(),
            attempt_count: current.attempt_count,
            error_code: None,
            error_message: None,
            details_json: details.to_string(),
            started_at: current.started_at,
            completed_at: Some(now.clone()),
            created_at: current.created_at,
            updated_at: now,
        },
    )
    .await?;
    load_owned_operation(db, &operation, lease_owner).await
}

/// Persist a running checkpoint's details without changing its status, so a
/// later failure record (which keeps the row's details) carries the plan and
/// the tool output that explain it.
async fn persist_checkpoint_details(
    db: &Arc<SqliteDb>,
    operation: ProjectProvisioningOperation,
    checkpoint: &str,
    details: Value,
    lease_owner: &str,
) -> Result<ProjectProvisioningOperation> {
    let operation = load_owned_operation(db, &operation, lease_owner).await?;
    let current =
        ProjectProvisioningRepo::get_provisioning_checkpoint(&**db, &operation.id, checkpoint)
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation("Project provisioning checkpoint is missing")
            })?;
    ProjectProvisioningRepo::upsert_provisioning_checkpoint(
        &**db,
        UpsertProjectProvisioningCheckpoint {
            id: current.id,
            operation_id: operation.id.clone(),
            checkpoint: checkpoint.to_owned(),
            status: current.status,
            attempt_count: current.attempt_count,
            error_code: current.error_code,
            error_message: current.error_message,
            details_json: details.to_string(),
            started_at: current.started_at,
            completed_at: current.completed_at,
            created_at: current.created_at,
            updated_at: now_rfc3339(),
        },
    )
    .await?;
    load_owned_operation(db, &operation, lease_owner).await
}

async fn update_operation_checkpoint(
    db: &Arc<SqliteDb>,
    operation: ProjectProvisioningOperation,
    checkpoint: &str,
    lease_owner: &str,
) -> Result<ProjectProvisioningOperation> {
    let operation = load_owned_operation(db, &operation, lease_owner).await?;
    ProjectProvisioningRepo::update_provisioning_operation(
        &**db,
        UpdateProjectProvisioningOperation {
            id: operation.id,
            expected_version: operation.version,
            status: None,
            current_checkpoint: Some(checkpoint.to_owned()),
            attempt_count: None,
            max_attempts: None,
            lease_owner: None,
            lease_expires_at: None,
            next_retry_at: None,
            retryable: None,
            last_error_code: None,
            last_error_message: None,
            completed_at: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .map_err(Into::into)
}

struct ProvisioningFailure<'a> {
    checkpoint: &'a str,
    code: &'a str,
    message: &'a str,
    retryable: bool,
    current_checkpoint: &'a str,
}

async fn fail_operation(
    db: &Arc<SqliteDb>,
    operation: ProjectProvisioningOperation,
    lease_owner: &str,
    failure: ProvisioningFailure<'_>,
) -> Result<ProjectProvisioningOperation> {
    let operation = load_owned_operation(db, &operation, lease_owner).await?;
    let checkpoint_row = ProjectProvisioningRepo::get_provisioning_checkpoint(
        &**db,
        &operation.id,
        failure.checkpoint,
    )
    .await?;
    let now = now_rfc3339();
    let code = bounded_text(failure.code, 96);
    let message = bounded_text(failure.message, 256);
    let checkpoint_id = checkpoint_row
        .as_ref()
        .map(|checkpoint| checkpoint.id.clone());
    if let Some(checkpoint_row) = checkpoint_row {
        ProjectProvisioningRepo::upsert_provisioning_checkpoint(
            &**db,
            UpsertProjectProvisioningCheckpoint {
                id: checkpoint_row.id.clone(),
                operation_id: operation.id.clone(),
                checkpoint: failure.checkpoint.to_owned(),
                status: "failed".to_owned(),
                attempt_count: checkpoint_row.attempt_count,
                error_code: Some(code.clone()),
                error_message: Some(message.clone()),
                details_json: checkpoint_row.details_json,
                started_at: checkpoint_row.started_at,
                completed_at: None,
                created_at: checkpoint_row.created_at,
                updated_at: now.clone(),
            },
        )
        .await?;
    }
    let operation = load_owned_operation(db, &operation, lease_owner).await?;
    let terminal = !failure.retryable || operation.attempt_count >= operation.max_attempts;
    let status = if terminal { "failed" } else { "setup_required" };
    // Record the typed error while this owner still holds an active lease;
    // finalization below is the only operation mutation that releases it.
    ProjectProvisioningRepo::record_provisioning_error(
        &**db,
        CreateProjectProvisioningError {
            id: new_uuid_v4(),
            operation_id: operation.id.clone(),
            checkpoint_id: checkpoint_id.clone(),
            code: code.clone(),
            message: message.clone(),
            retryable: !terminal,
            attempt_count: operation.attempt_count,
            created_at: now.clone(),
        },
    )
    .await?;
    let operation = ProjectProvisioningRepo::update_provisioning_operation(
        &**db,
        UpdateProjectProvisioningOperation {
            id: operation.id.clone(),
            expected_version: operation.version,
            status: Some(status.to_owned()),
            current_checkpoint: Some(failure.current_checkpoint.to_owned()),
            attempt_count: None,
            max_attempts: None,
            lease_owner: Some(None),
            lease_expires_at: Some(None),
            next_retry_at: Some(if terminal { None } else { Some(now.clone()) }),
            retryable: Some(!terminal),
            last_error_code: Some(Some(code.clone())),
            last_error_message: Some(Some(message.clone())),
            completed_at: Some(None),
            updated_at: now.clone(),
        },
    )
    .await?;
    Ok(operation)
}

async fn load_owned_operation(
    db: &Arc<SqliteDb>,
    operation: &ProjectProvisioningOperation,
    lease_owner: &str,
) -> Result<ProjectProvisioningOperation> {
    let current = ProjectProvisioningRepo::get_provisioning_operation_by_id(&**db, &operation.id)
        .await?
        .ok_or_else(|| {
            ServiceError::invalid_operation("Project provisioning operation is missing")
        })?;
    assert_lease(&current, lease_owner)?;
    Ok(current)
}

async fn ready_operation_is_verified(
    db: &Arc<SqliteDb>,
    project: &Project,
    operation: &ProjectProvisioningOperation,
) -> Result<bool> {
    let Some(repo_id) = project.primary_repo_id.as_deref() else {
        return Ok(false);
    };
    let Some(repo) = RepoRepo::get_by_id(&**db, repo_id).await? else {
        return Ok(false);
    };
    if repo.project_id != project.id || repo.default_branch != DEFAULT_BRANCH {
        return Ok(false);
    }
    if let Some(local_path) = repo.local_path.as_deref() {
        let path = Path::new(local_path);
        if !git::is_git_repo(path).await || !git::branch_exists(path, DEFAULT_BRANCH).await? {
            return Ok(false);
        }
        if git::get_current_sha(path).await?.trim().is_empty() {
            return Ok(false);
        }
    } else if repo.remote_url.trim().is_empty() {
        // A remote-only repository has no local filesystem checkpoint to
        // inspect, but its durable remote binding must still be non-empty.
        return Ok(false);
    }
    // Role settings are optional defaults. A ready repository remains ready
    // when defaults are absent or use the same identity; each Task enforces
    // its own workflow-role assignment before dispatch.
    let roles = resolve_project_execution_roles(db, project).await?;
    if !roles.requirements.is_empty() {
        return Ok(false);
    }

    let checkpoints = ProjectProvisioningRepo::list_provisioning_checkpoints(&**db, &operation.id)
        .await?
        .into_iter()
        .map(|checkpoint| (checkpoint.checkpoint.clone(), checkpoint))
        .collect::<std::collections::HashMap<_, _>>();
    for checkpoint_name in CHECKPOINTS {
        let Some(checkpoint) = checkpoints.get(checkpoint_name) else {
            return Ok(false);
        };
        if checkpoint_name == "repository_scaffolded" && checkpoint.status == "pending" {
            // The operation was ready before scaffolding existed (a V087
            // backfill, or rows repaired after the fact). Nothing was
            // skipped by mistake: record that and keep the operation ready
            // rather than reopening it to re-provision a verified repository.
            let now = now_rfc3339();
            ProjectProvisioningRepo::upsert_provisioning_checkpoint(
                &**db,
                UpsertProjectProvisioningCheckpoint {
                    id: checkpoint.id.clone(),
                    operation_id: operation.id.clone(),
                    checkpoint: checkpoint_name.to_owned(),
                    status: "skipped".to_owned(),
                    attempt_count: checkpoint.attempt_count,
                    error_code: None,
                    error_message: None,
                    details_json: json!({"reason": "ready_before_scaffold"}).to_string(),
                    started_at: checkpoint.started_at.clone(),
                    completed_at: Some(now.clone()),
                    created_at: checkpoint.created_at.clone(),
                    updated_at: now,
                },
            )
            .await?;
            continue;
        }
        let completed = match checkpoint_name {
            "repository_initialized" if repo.local_path.is_none() => checkpoint.status == "skipped",
            // Skipped is the normal outcome without a Charter scaffold, and
            // the backfilled state of every operation that predates it.
            "repository_scaffolded" => {
                checkpoint.status == "completed" || checkpoint.status == "skipped"
            }
            _ => checkpoint.status == "completed",
        };
        if !completed || checkpoint.completed_at.is_none() {
            return Ok(false);
        }
    }

    // Keep the durable target path populated for local V087 backfills. This is
    // a repair-only write and does not consume an operation attempt.
    if let Some(checkpoint) = ProjectProvisioningRepo::get_provisioning_checkpoint(
        &**db,
        &operation.id,
        "repository_initialized",
    )
    .await?
    {
        if let Some(local_path) = repo.local_path.as_deref() {
            if checkpoint_path(&checkpoint.details_json).is_none() {
                let mut details = serde_json::from_str::<Value>(&checkpoint.details_json)
                    .unwrap_or_else(|_| json!({}));
                if !details.is_object() {
                    details = json!({});
                }
                details["path"] = Value::String(local_path.to_owned());
                ProjectProvisioningRepo::upsert_provisioning_checkpoint(
                    &**db,
                    UpsertProjectProvisioningCheckpoint {
                        id: checkpoint.id,
                        operation_id: operation.id.clone(),
                        checkpoint: "repository_initialized".to_owned(),
                        status: checkpoint.status,
                        attempt_count: checkpoint.attempt_count,
                        error_code: checkpoint.error_code,
                        error_message: checkpoint.error_message,
                        details_json: details.to_string(),
                        started_at: checkpoint.started_at,
                        completed_at: checkpoint.completed_at,
                        created_at: checkpoint.created_at,
                        updated_at: now_rfc3339(),
                    },
                )
                .await?;
            }
        }
    }
    Ok(true)
}

async fn reopen_ready_operation(
    db: &Arc<SqliteDb>,
    operation: ProjectProvisioningOperation,
) -> Result<ProjectProvisioningOperation> {
    ProjectProvisioningRepo::update_provisioning_operation(
        &**db,
        UpdateProjectProvisioningOperation {
            id: operation.id,
            expected_version: operation.version,
            status: Some("setup_required".to_owned()),
            current_checkpoint: Some("repository_initialized".to_owned()),
            attempt_count: None,
            max_attempts: None,
            lease_owner: Some(None),
            lease_expires_at: Some(None),
            next_retry_at: Some(None),
            retryable: Some(true),
            last_error_code: Some(None),
            last_error_message: Some(None),
            completed_at: Some(None),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .map_err(Into::into)
}

fn lease_is_active(expires_at: Option<&str>) -> bool {
    let Some(expires_at) = expires_at else {
        return false;
    };
    chrono::DateTime::parse_from_rfc3339(expires_at)
        .map(|expiry| expiry > Utc::now())
        .unwrap_or(false)
}

async fn renew_owned_lease(
    db: &Arc<SqliteDb>,
    operation: &ProjectProvisioningOperation,
    lease_owner: &str,
) -> Result<ProjectProvisioningOperation> {
    ProjectProvisioningRepo::update_provisioning_operation(
        &**db,
        UpdateProjectProvisioningOperation {
            id: operation.id.clone(),
            expected_version: operation.version,
            status: None,
            current_checkpoint: None,
            attempt_count: None,
            max_attempts: None,
            lease_owner: Some(Some(lease_owner.to_owned())),
            lease_expires_at: Some(Some(lease_expiry())),
            next_retry_at: Some(None),
            retryable: None,
            last_error_code: None,
            last_error_message: None,
            completed_at: Some(None),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .map_err(Into::into)
}

fn assert_lease(operation: &ProjectProvisioningOperation, lease_owner: &str) -> Result<()> {
    if operation.lease_owner.as_deref() != Some(lease_owner)
        || !lease_is_active(operation.lease_expires_at.as_deref())
    {
        return Err(ServiceError::conflict(
            "Project provisioning lease is no longer active",
        ));
    }
    Ok(())
}

fn checkpoint_path(details_json: &str) -> Option<PathBuf> {
    serde_json::from_str::<Value>(details_json)
        .ok()
        .and_then(|details| {
            details
                .get("path")
                .and_then(Value::as_str)
                .map(PathBuf::from)
        })
}

fn checkpoint_failure_code(checkpoint: &str) -> &'static str {
    match checkpoint {
        "repository_scaffolded" => "repository_scaffold_failed",
        "repository_initialized" => "repository_initialization_failed",
        "repository_registered" => "repository_registration_failed",
        "repository_linked" => "repository_link_failed",
        "roles_assigned" => "role_assignment_failed",
        _ => "provisioning_failed",
    }
}

fn checkpoint_failure_message(checkpoint: &str) -> &'static str {
    match checkpoint {
        "repository_scaffolded" => {
            "Project repository could not be scaffolded from the Charter's spark template"
        }
        "repository_initialized" => "Project repository could not be initialized or verified",
        "repository_registered" => "Project repository could not be registered",
        "repository_linked" => "Project repository could not be linked",
        "roles_assigned" => "Project execution roles could not be assigned",
        _ => "Project provisioning could not complete; retry is available",
    }
}

fn failure_current_checkpoint(checkpoint: &str) -> &'static str {
    match checkpoint {
        "preflight" => "preflight",
        "repository_scaffolded" => "preflight",
        "repository_initialized" => "repository_scaffolded",
        "repository_registered" => "repository_initialized",
        "repository_linked" => "repository_registered",
        "roles_assigned" => "repository_linked",
        _ => "preflight",
    }
}

fn bounded_text(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn lease_expiry() -> String {
    (Utc::now() + Duration::seconds(LEASE_SECONDS)).to_rfc3339()
}

fn role_blocker(requirements: &[SetupRequirement]) -> (&'static str, &'static str) {
    if requirements.iter().any(|requirement| {
        requirement.role.as_deref() == Some("worker")
            || requirement.action == Some(RetryAction::SelectWorker)
    }) {
        (
            "worker_roles_required",
            "Project requires an eligible Worker and independent reviewer before execution setup is ready",
        )
    } else {
        (
            "independent_reviewer_required",
            "Project requires an independent reviewer before execution setup is ready",
        )
    }
}

async fn initialize_repository(
    db: &Arc<SqliteDb>,
    project: &Project,
    operation: &ProjectProvisioningOperation,
    lease_owner: &str,
) -> Result<PathBuf> {
    let operation = load_owned_operation(db, operation, lease_owner).await?;
    let checkpoint = ProjectProvisioningRepo::get_provisioning_checkpoint(
        &**db,
        &operation.id,
        "repository_initialized",
    )
    .await?
    .ok_or_else(|| ServiceError::invalid_operation("Project provisioning checkpoint is missing"))?;
    let persisted = match checkpoint_path(&checkpoint.details_json) {
        Some(path) => Some(path),
        None => persisted_repository_path(db, &operation.id).await?,
    };
    let repo_path = resolve_repository_path(db, project, persisted).await?;

    // Persist the target before touching the filesystem.  If the process
    // stops after this write, a renamed Project still resumes in the same
    // directory rather than deriving a second name on the next attempt.
    let mut details =
        serde_json::from_str::<Value>(&checkpoint.details_json).unwrap_or_else(|_| json!({}));
    if !details.is_object() {
        details = json!({});
    }
    details["path"] = Value::String(repo_path.to_string_lossy().into_owned());
    assert_lease(&operation, lease_owner)?;
    ProjectProvisioningRepo::upsert_provisioning_checkpoint(
        &**db,
        UpsertProjectProvisioningCheckpoint {
            id: checkpoint.id,
            operation_id: operation.id.clone(),
            checkpoint: "repository_initialized".to_owned(),
            status: "running".to_owned(),
            attempt_count: checkpoint.attempt_count,
            error_code: None,
            error_message: None,
            details_json: details.to_string(),
            started_at: checkpoint.started_at,
            completed_at: None,
            created_at: checkpoint.created_at,
            updated_at: now_rfc3339(),
        },
    )
    .await?;
    load_owned_operation(db, &operation, lease_owner).await?;

    tokio::fs::create_dir_all(&repo_path)
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "create Project repository directory {}: {error}",
                repo_path.display()
            ))
        })?;

    if !git::is_git_repo(&repo_path).await {
        git::init(&repo_path).await?;
        let scaffolded = repo_path.join(SCAFFOLD_MARKER).is_file();
        if !repo_path.join("README.md").exists() {
            let readme = format!(
                "# {}\n\nRepository created by Forge Product Genesis.\n",
                project.name
            );
            tokio::fs::write(repo_path.join("README.md"), readme)
                .await
                .map_err(|error| {
                    ServiceError::invalid_operation(format!(
                        "write Project repository README: {error}"
                    ))
                })?;
        }
        let message = if scaffolded {
            "Initialize repository from spark scaffold"
        } else {
            "Initialize repository"
        };
        git::commit_all(&repo_path, message).await?;
        if !git::branch_exists(&repo_path, DEFAULT_BRANCH).await? {
            git::rename_current_branch(&repo_path, DEFAULT_BRANCH).await?;
        }
    } else if !git::branch_exists(&repo_path, DEFAULT_BRANCH).await? {
        return Err(ServiceError::invalid_operation(format!(
            "existing repository at {} has no '{DEFAULT_BRANCH}' branch",
            repo_path.display()
        )));
    }
    let head = git::get_current_sha(&repo_path).await?;
    if head.trim().is_empty() {
        return Err(ServiceError::invalid_operation(
            "Project repository has no commit history",
        ));
    }
    Ok(repo_path)
}

/// The repository path a filesystem checkpoint of this operation already
/// persisted, if any. Both the scaffold and the init checkpoint record the
/// directory before touching it, and each must honour the other's record:
/// a scaffold that ran on attempt one and an init that runs on attempt two
/// have to land in the same directory even if the derived name changed.
async fn persisted_repository_path(
    db: &Arc<SqliteDb>,
    operation_id: &str,
) -> Result<Option<PathBuf>> {
    for checkpoint in ["repository_initialized", "repository_scaffolded"] {
        if let Some(row) =
            ProjectProvisioningRepo::get_provisioning_checkpoint(&**db, operation_id, checkpoint)
                .await?
        {
            if let Some(path) = checkpoint_path(&row.details_json) {
                return Ok(Some(path));
            }
        }
    }
    Ok(None)
}

/// The one deterministic directory every filesystem checkpoint agrees on:
/// a path already persisted by a checkpoint, else the primary or any durable
/// repository row's local path, else the slugged Project name plus id prefix.
async fn resolve_repository_path(
    db: &Arc<SqliteDb>,
    project: &Project,
    persisted: Option<PathBuf>,
) -> Result<PathBuf> {
    if let Some(path) = persisted {
        return Ok(path);
    }
    if let Some(primary_repo_id) = project.primary_repo_id.as_deref() {
        return Ok(RepoRepo::get_by_id(&**db, primary_repo_id)
            .await?
            .and_then(|repo| repo.local_path.map(PathBuf::from))
            .unwrap_or_else(|| {
                repos_root().join(repo_directory_name(&project.name, &project.id))
            }));
    }
    // A repository row can be durable even when the Project link was the
    // interrupted step. Reuse its local path before deriving a name from
    // the mutable Project name.
    let page = RepoRepo::list_by_project(
        &**db,
        &project.id,
        PageRequest {
            cursor: None,
            limit: 500,
            include_total: false,
            sort_by: SortBy::Id,
            sort_order: SortOrder::Asc,
        },
    )
    .await?;
    Ok(page
        .items
        .into_iter()
        .find_map(|repo| repo.local_path.map(PathBuf::from))
        .unwrap_or_else(|| repos_root().join(repo_directory_name(&project.name, &project.id))))
}

enum ScaffoldStep {
    Continue(ProjectProvisioningOperation),
    Failed(ProjectProvisioningOperation),
}

struct ApprovedCharterScaffold {
    revision_id: String,
    content_digest: String,
    rendered_view: String,
    scaffold: CharterScaffold,
}

struct ScaffoldFailure {
    code: &'static str,
    message: String,
    output_tail: String,
}

/// The scaffold named by the Project's current approved Charter revision.
/// Provisioning reads the approved revision, never a draft: amending the
/// Charter and retrying is how a bad template or pack set gets corrected.
async fn load_approved_charter_scaffold(
    db: &Arc<SqliteDb>,
    project: &Project,
) -> Result<Option<ApprovedCharterScaffold>> {
    let Some(charter) =
        ProjectOrchestrationRepo::get_project_charter_by_project_id(&**db, &project.id).await?
    else {
        return Ok(None);
    };
    let Some(revision_id) = charter.current_approved_revision_id.as_deref() else {
        return Ok(None);
    };
    let Some(revision) =
        ProjectOrchestrationRepo::get_project_charter_revision(&**db, revision_id).await?
    else {
        return Ok(None);
    };
    // Read only the block this checkpoint needs. Provisioning must keep
    // working for Charters saved under an earlier or later content schema,
    // so the full `ProjectCharterContent` (which denies unknown fields) is
    // deliberately not the parse target here.
    #[derive(serde::Deserialize)]
    struct ScaffoldOnly {
        #[serde(default)]
        scaffold: Option<CharterScaffold>,
    }
    let content: ScaffoldOnly = serde_json::from_str(&revision.content_json).map_err(|error| {
        ServiceError::invalid_operation(format!(
            "approved Charter revision {revision_id} content is unreadable: {error}"
        ))
    })?;
    Ok(content.scaffold.map(|scaffold| ApprovedCharterScaffold {
        revision_id: revision.id,
        content_digest: revision.content_digest,
        rendered_view: revision.rendered_view,
        scaffold,
    }))
}

/// `FORGE_SCAFFOLD_COMMAND` split on whitespace, falling back to the pinned
/// default when `forge-cli` did not export one (tests, embedded use).
fn scaffold_command() -> Vec<String> {
    std::env::var(SCAFFOLD_COMMAND_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| config::DEFAULT_SCAFFOLD_COMMAND.to_owned())
        .split_whitespace()
        .map(str::to_owned)
        .collect()
}

/// Stand the repository directory up from the approved Charter's spark
/// template and packs. Idempotent by inspection of the target: an existing
/// scaffold or git repository is never re-run, and a partial directory left
/// by a failed run is removed so the next attempt starts clean. create-spark
/// refuses an existing target, which is why the directory must not exist yet
/// when the command runs.
async fn scaffold_repository(
    db: &Arc<SqliteDb>,
    project: &Project,
    operation: ProjectProvisioningOperation,
    lease_owner: &str,
) -> Result<ScaffoldStep> {
    let Some(approved) = load_approved_charter_scaffold(db, project).await? else {
        let operation = skip_checkpoint(
            db,
            operation,
            "repository_scaffolded",
            json!({"reason": "no_scaffold"}),
            lease_owner,
        )
        .await?;
        return Ok(ScaffoldStep::Continue(operation));
    };

    let operation = load_owned_operation(db, &operation, lease_owner).await?;
    let persisted = persisted_repository_path(db, &operation.id).await?;
    let repo_path = resolve_repository_path(db, project, persisted).await?;
    let mut details = json!({
        "path": repo_path.to_string_lossy(),
        "template": approved.scaffold.template,
        "packs": approved.scaffold.packs,
        "charter_revision_id": approved.revision_id,
    });
    // Persist the target before touching the filesystem, like the init
    // checkpoint does, so an interrupted run resumes on the same directory.
    let operation = persist_checkpoint_details(
        db,
        operation,
        "repository_scaffolded",
        details.clone(),
        lease_owner,
    )
    .await?;

    if repo_path.join(SCAFFOLD_MARKER).is_file() {
        details["already_present"] = json!(true);
        let operation =
            complete_checkpoint(db, operation, "repository_scaffolded", details, lease_owner)
                .await?;
        return Ok(ScaffoldStep::Continue(operation));
    }
    if git::is_git_repo(&repo_path).await {
        details["reason"] = json!("existing_repository");
        let operation =
            skip_checkpoint(db, operation, "repository_scaffolded", details, lease_owner).await?;
        return Ok(ScaffoldStep::Continue(operation));
    }
    match tokio::fs::read_dir(&repo_path).await {
        Ok(mut entries) => {
            let occupied = entries
                .next_entry()
                .await
                .map_err(|error| {
                    ServiceError::invalid_operation(format!(
                        "inspect Project repository directory {}: {error}",
                        repo_path.display()
                    ))
                })?
                .is_some();
            if occupied {
                details["output_tail"] = json!(
                    "target directory exists and is neither a spark scaffold nor a git repository"
                );
                let operation = persist_checkpoint_details(
                    db,
                    operation,
                    "repository_scaffolded",
                    details,
                    lease_owner,
                )
                .await?;
                let operation = fail_operation(
                    db,
                    operation,
                    lease_owner,
                    ProvisioningFailure {
                        checkpoint: "repository_scaffolded",
                        code: "repository_scaffold_failed",
                        message: "Project repository directory already exists but is neither a spark scaffold nor a git repository; move it aside and retry",
                        retryable: true,
                        current_checkpoint: "preflight",
                    },
                )
                .await?;
                return Ok(ScaffoldStep::Failed(operation));
            }
            tokio::fs::remove_dir(&repo_path).await.map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "remove empty Project repository directory {}: {error}",
                    repo_path.display()
                ))
            })?;
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => {
            return Err(ServiceError::invalid_operation(format!(
                "inspect Project repository directory {}: {error}",
                repo_path.display()
            )));
        }
    }

    let parent = repo_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(repos_root);
    tokio::fs::create_dir_all(&parent).await.map_err(|error| {
        ServiceError::invalid_operation(format!(
            "create Project repositories directory {}: {error}",
            parent.display()
        ))
    })?;
    let dir_name = repo_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| repo_directory_name(&project.name, &project.id));
    let command = scaffold_command();
    details["command"] = json!(command.join(" "));

    // Dependency installation can take a while on a cold cache; keep the
    // lease current before the bounded run rather than risking expiry.
    let operation = renew_owned_lease(db, &operation, lease_owner).await?;
    match run_scaffold_command(&command, &parent, &dir_name, &approved.scaffold).await {
        Ok(output_tail) => {
            details["output_tail"] = json!(output_tail);
            write_scaffold_exports(&repo_path, project, &approved).await?;
            let operation =
                complete_checkpoint(db, operation, "repository_scaffolded", details, lease_owner)
                    .await?;
            Ok(ScaffoldStep::Continue(operation))
        }
        Err(failure) => {
            remove_partial_scaffold(&repo_path).await;
            details["output_tail"] = json!(failure.output_tail);
            let operation = persist_checkpoint_details(
                db,
                operation,
                "repository_scaffolded",
                details,
                lease_owner,
            )
            .await?;
            let operation = fail_operation(
                db,
                operation,
                lease_owner,
                ProvisioningFailure {
                    checkpoint: "repository_scaffolded",
                    code: failure.code,
                    message: &failure.message,
                    retryable: true,
                    current_checkpoint: "preflight",
                },
            )
            .await?;
            Ok(ScaffoldStep::Failed(operation))
        }
    }
}

async fn run_scaffold_command(
    command: &[String],
    cwd: &Path,
    dir_name: &str,
    scaffold: &CharterScaffold,
) -> std::result::Result<String, ScaffoldFailure> {
    let Some((program, base_args)) = command.split_first() else {
        return Err(ScaffoldFailure {
            code: "scaffold_runtime_unavailable",
            message: format!("scaffold command is empty; set {SCAFFOLD_COMMAND_ENV}"),
            output_tail: String::new(),
        });
    };
    let mut args = base_args.to_vec();
    args.push(dir_name.to_owned());
    args.push("--template".to_owned());
    args.push(scaffold.template.clone());
    args.push("--yes".to_owned());
    if scaffold.packs.is_empty() {
        args.push("--no-packs".to_owned());
    } else {
        args.push("--packs".to_owned());
        args.push(scaffold.packs.join(","));
    }

    let mut child = Command::new(program);
    child
        .args(&args)
        .current_dir(cwd)
        .env("NO_COLOR", "1")
        .env("CI", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let output = match tokio::time::timeout(SCAFFOLD_TIMEOUT, child.output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) if error.kind() == ErrorKind::NotFound => {
            return Err(ScaffoldFailure {
                code: "scaffold_runtime_unavailable",
                message: format!(
                    "scaffold command `{program}` is not installed on the Forge host; install bun or set {SCAFFOLD_COMMAND_ENV}"
                ),
                output_tail: String::new(),
            });
        }
        Ok(Err(error)) => {
            return Err(ScaffoldFailure {
                code: "repository_scaffold_failed",
                message: format!("scaffold command `{program}` could not start: {error}"),
                output_tail: String::new(),
            });
        }
        Err(_) => {
            return Err(ScaffoldFailure {
                code: "repository_scaffold_failed",
                message: format!(
                    "scaffold command exceeded {} seconds and was stopped",
                    SCAFFOLD_TIMEOUT.as_secs()
                ),
                output_tail: String::new(),
            });
        }
    };
    let output_tail = scaffold_output_tail(&output.stdout, &output.stderr);
    if output.status.success() {
        return Ok(output_tail);
    }
    let last_line = String::from_utf8_lossy(&output.stderr)
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| format!("exit status {}", output.status));
    Err(ScaffoldFailure {
        code: "repository_scaffold_failed",
        message: format!("create-spark failed: {last_line}"),
        output_tail,
    })
}

fn scaffold_output_tail(stdout: &[u8], stderr: &[u8]) -> String {
    let combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(stdout).trim_end(),
        String::from_utf8_lossy(stderr).trim_end()
    );
    let combined = combined.trim();
    let skip = combined
        .chars()
        .count()
        .saturating_sub(SCAFFOLD_OUTPUT_TAIL_CHARS);
    combined.chars().skip(skip).collect()
}

/// A directory the failed run created but never turned into a git repository
/// is this operation's own debris; leaving it would make every retry fail on
/// "target directory already exists".
async fn remove_partial_scaffold(repo_path: &Path) {
    if !repo_path.exists() || repo_path.join(".git").exists() {
        return;
    }
    if let Err(error) = tokio::fs::remove_dir_all(repo_path).await {
        tracing::warn!(
            path = %repo_path.display(),
            error = %error,
            "partial spark scaffold could not be removed; the next attempt will report it"
        );
    }
}

/// Put Forge's truth into the scaffold before the first commit: the approved
/// Charter as `docs/spark/project.md` (an export, stamped with its revision)
/// and a Forge section in `AGENTS.md` so every Task Worker reading the
/// repository knows planning and tracking live in Forge.
async fn write_scaffold_exports(
    repo_path: &Path,
    project: &Project,
    approved: &ApprovedCharterScaffold,
) -> Result<()> {
    let docs_dir = repo_path.join("docs").join("spark");
    tokio::fs::create_dir_all(&docs_dir)
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "create {} in the scaffold: {error}",
                docs_dir.display()
            ))
        })?;
    let project_md = format!(
        "# {}\n\n> Exported by Forge from the approved Project Charter (revision `{}`, content digest `{}`).\n> Forge is the source of truth for this Project: Tasks, reviews, milestones, and releases live in Forge, not in `docs/spark/changes/`.\n\n{}",
        project.name, approved.revision_id, approved.content_digest, approved.rendered_view
    );
    tokio::fs::write(docs_dir.join("project.md"), project_md)
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "write docs/spark/project.md in the scaffold: {error}"
            ))
        })?;

    let agents_path = repo_path.join("AGENTS.md");
    let mut agents = match tokio::fs::read_to_string(&agents_path).await {
        Ok(existing) => existing,
        Err(error) if error.kind() == ErrorKind::NotFound => String::new(),
        Err(error) => {
            return Err(ServiceError::invalid_operation(format!(
                "read AGENTS.md in the scaffold: {error}"
            )));
        }
    };
    if !agents.contains("\n## Forge\n") && !agents.starts_with("## Forge\n") {
        if !agents.is_empty() && !agents.ends_with('\n') {
            agents.push('\n');
        }
        if !agents.is_empty() {
            agents.push('\n');
        }
        agents.push_str(FORGE_AGENTS_SECTION);
        tokio::fs::write(&agents_path, agents)
            .await
            .map_err(|error| {
                ServiceError::invalid_operation(format!("write AGENTS.md in the scaffold: {error}"))
            })?;
    }
    Ok(())
}

const FORGE_AGENTS_SECTION: &str = "## Forge\n\nThis repository belongs to a Forge Project. Forge owns planning and execution: the approved Charter is exported to `docs/spark/project.md`, Tasks are created, reviewed, and merged in Forge, and the Task brief you receive is the plan. Do not create `docs/spark/changes/` folders or edit a `tasks.md`; report scope discoveries as follow-up work in your Task report. The `worker-guidelines` lens is in force.\n";

async fn find_or_register_repository(
    db: &Arc<SqliteDb>,
    project: &Project,
    repo_path: &Path,
) -> Result<Repo> {
    let local_path = repo_path.to_string_lossy().into_owned();
    if let Some(primary_repo_id) = project.primary_repo_id.as_deref() {
        let repo = RepoRepo::get_by_id(&**db, primary_repo_id)
            .await?
            .ok_or_else(|| ServiceError::conflict("Project primary repository row is missing"))?;
        return verify_repo_binding(repo, &project.id, &local_path);
    }
    let repos = RepoRepo::list_by_project(
        &**db,
        &project.id,
        PageRequest {
            cursor: None,
            limit: 500,
            include_total: false,
            sort_by: SortBy::Id,
            sort_order: SortOrder::Asc,
        },
    )
    .await?
    .items;
    let matching = repos
        .into_iter()
        .filter(|repo| {
            repo.local_path.as_deref() == Some(local_path.as_str()) || repo.remote_url == local_path
        })
        .collect::<Vec<_>>();
    if matching.len() > 1 {
        return Err(ServiceError::conflict(
            "multiple repository rows claim the deterministic Project repository path",
        ));
    }
    if let Some(repo) = matching.into_iter().next() {
        return verify_repo_binding(repo, &project.id, &local_path);
    }
    let now = now_rfc3339();
    let input = CreateRepo {
        id: new_uuid_v4(),
        project_id: project.id.clone(),
        name: repo_directory_name(&project.name, &project.id),
        local_path: Some(local_path.clone()),
        remote_url: local_path.clone(),
        work_mode: WorkMode::DirectMerge,
        default_branch: DEFAULT_BRANCH.to_owned(),
        created_at: now.clone(),
        updated_at: now,
    };
    match RepoRepo::create(&**db, input).await {
        Ok(repo) => Ok(repo),
        Err(error @ db::DbError::Sqlx(_)) | Err(error @ db::DbError::Check(_)) => {
            // The repository API predates a path uniqueness constraint.  A
            // concurrent or replayed create can therefore report either a
            // SQLite constraint error or a row already visible after the
            // insert race. Reload the deterministic row before surfacing the
            // error, and never create a second filesystem/row pair.
            let repos = RepoRepo::list_by_project(
                &**db,
                &project.id,
                PageRequest {
                    cursor: None,
                    limit: 500,
                    include_total: false,
                    sort_by: SortBy::Id,
                    sort_order: SortOrder::Asc,
                },
            )
            .await?;
            let matching = repos.items.into_iter().filter(|repo| {
                repo.local_path.as_deref() == Some(local_path.as_str())
                    || repo.remote_url == local_path
            });
            let mut matches = matching.collect::<Vec<_>>();
            if matches.len() == 1 {
                return verify_repo_binding(matches.remove(0), &project.id, &local_path);
            }
            Err(error.into())
        }
        Err(error) => Err(error.into()),
    }
}

fn verify_repo_binding(repo: Repo, project_id: &str, local_path: &str) -> Result<Repo> {
    if repo.project_id != project_id {
        return Err(ServiceError::conflict(
            "deterministic Project repository belongs to another Project",
        ));
    }
    if repo.local_path.as_deref() != Some(local_path) || repo.default_branch != DEFAULT_BRANCH {
        return Err(ServiceError::conflict(
            "Project repository row does not match the deterministic repository target",
        ));
    }
    Ok(repo)
}

async fn link_primary_repository(
    db: &Arc<SqliteDb>,
    mut project: Project,
    repo: &Repo,
) -> Result<Project> {
    if project.primary_repo_id.as_deref() == Some(repo.id.as_str()) {
        return Ok(project);
    }
    for _ in 0..2 {
        match ProjectRepo::update_at_version(
            &**db,
            UpdateProject {
                id: project.id.clone(),
                name: None,
                settings: None,
                primary_repo_id: Some(Some(repo.id.clone())),
                paused_at: None,
                updated_at: now_rfc3339(),
            },
            project.version,
            None,
        )
        .await
        {
            Ok(project) => return Ok(project),
            Err(db::DbError::VersionConflict) => {
                project = ProjectRepo::get_by_id(&**db, &project.id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("project", &project.id))?;
                if project.primary_repo_id.as_deref() == Some(repo.id.as_str()) {
                    return Ok(project);
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(ServiceError::conflict(
        "Project changed while linking its primary repository",
    ))
}

async fn update_role_assignments(
    db: &Arc<SqliteDb>,
    mut project: Project,
    assignments: Vec<Value>,
    replace_roles: &[Option<&str>],
) -> Result<Project> {
    for _ in 0..2 {
        let mut settings = serde_json::from_str::<Value>(&project.settings).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid Project settings: {error}"))
        })?;
        let mut merged = settings
            .get("default_role_assignments")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let role_names = replace_roles.iter().copied().flatten().collect::<Vec<_>>();
        merged.retain(|existing| {
            existing
                .get("role_name")
                .and_then(Value::as_str)
                .is_none_or(|role| !role_names.contains(&role))
        });
        merged.extend(
            assignments
                .iter()
                .filter(|&assignment| {
                    assignment
                        .get("role_name")
                        .and_then(Value::as_str)
                        .is_some_and(|role| role_names.contains(&role))
                })
                .cloned(),
        );
        if settings.get("default_role_assignments") == Some(&Value::Array(merged.clone())) {
            return Ok(project);
        }
        settings["default_role_assignments"] = Value::Array(merged);
        match ProjectRepo::update_at_version(
            &**db,
            UpdateProject {
                id: project.id.clone(),
                name: None,
                settings: Some(settings.to_string()),
                primary_repo_id: None,
                paused_at: None,
                updated_at: now_rfc3339(),
            },
            project.version,
            None,
        )
        .await
        {
            Ok(project) => return Ok(project),
            Err(db::DbError::VersionConflict) => {
                project = ProjectRepo::get_by_id(&**db, &project.id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("project", &project.id))?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(ServiceError::conflict(
        "Project changed while updating execution role assignments",
    ))
}

fn project_role_assignments(project: &Project) -> Value {
    serde_json::from_str::<Value>(&project.settings)
        .ok()
        .and_then(|settings| settings.get("default_role_assignments").cloned())
        .unwrap_or_else(|| Value::Array(Vec::new()))
}

fn repos_root() -> PathBuf {
    crate::task_service::workspace::default_workspace_root().join("repos")
}

/// Deterministic, collision-free directory name: sanitized project name plus
/// the first 8 characters of the project id (mirrors task branch naming).
fn repo_directory_name(project_name: &str, project_id: &str) -> String {
    let slug: String = project_name
        .to_lowercase()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '-'
            }
        })
        .collect::<String>()
        .split('-')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    let slug = if slug.is_empty() { "project" } else { &slug };
    let id_prefix: String = project_id.chars().take(8).collect();
    format!("{slug}-{id_prefix}")
}

#[cfg(test)]
mod tests {
    use super::repo_directory_name;

    #[test]
    fn repo_directory_name_is_slugged_and_deterministic() {
        assert_eq!(
            repo_directory_name("Simple Todo!", "ab591984-975f-4406"),
            "simple-todo-ab591984"
        );
        assert_eq!(repo_directory_name("---", "12345678-x"), "project-12345678");
    }
}
