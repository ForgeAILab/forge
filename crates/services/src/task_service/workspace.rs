use super::project_agent_workspace::{cleanup_repo_cache_if_authority_gone, resolve_repo_source};
use super::*;
use db::{
    CreateWorkspacePlacement, PlacementOwnerKind, PlacementSelectedBy, PlacementState,
    RepoLocationKind, UpdateWorkspacePlacement, WorkspacePlacementRepo,
};
use sqlx::{Sqlite, Transaction};
use std::path::{Path, PathBuf};
use tracing::{info, warn};

/// Preserve Git and filesystem failures as typed transient service errors.
/// Dispatch dispositions are for stable governance refusals; converting these
/// failures to `InvalidOperation` text would park a Task until an unrelated
/// version change even though the repository/process may recover on its own.
fn map_workspace_error(error: ::workspace::WorkspaceError) -> ServiceError {
    match error {
        ::workspace::WorkspaceError::Git(error) => ServiceError::Git(error),
        ::workspace::WorkspaceError::Io(error) => ServiceError::Git(git::GitError::Io(error)),
        error => ServiceError::invalid_operation(error.to_string()),
    }
}

async fn resolve_workspace_backend(
    db: &SqliteDb,
    workspace_root: &Path,
    workspace: &Workspace,
    router: &crate::workspace_backend::WorkspaceBackendRouter,
) -> Result<crate::workspace_backend::ResolvedWorkspace> {
    Ok(
        crate::workspace_backend::EmbeddedWorkspaceBackend::resolve_workspace(
            router,
            db,
            workspace,
            workspace_root,
        )
        .await?,
    )
}

const PLACEMENT_RESERVATION_SECONDS: i64 = 600;

pub(super) struct WorkspaceAdmission {
    pub workspace: Workspace,
    claiming_task: Task,
    pub placement: db::WorkspacePlacement,
    selection_context: Option<crate::placement::SelectionContext>,
    server_facts: crate::placement::ServerFacts,
    review_config: api_types::ReviewConfig,
    settings: ProjectSettings,
    task_owner_id: Option<String>,
}

impl TaskService {
    pub(super) fn check_placement_lease_owner(
        &self,
        placement: &db::WorkspacePlacement,
        lease: &ClaimExecutionLease,
    ) -> Result<()> {
        let owner = match placement
            .execution_daemon_id
            .as_deref()
            .or(placement.daemon_id.as_deref())
        {
            Some(daemon_id) => self
                .daemon_connections
                .as_ref()
                .and_then(|registry| registry.get(daemon_id))
                .filter(|connection| {
                    !connection.is_stale() && connection.protocol_allows_dispatch()
                })
                .map(|connection| {
                    crate::daemon_transport::execution_lease_owner(daemon_id, connection.id())
                })
                .unwrap_or_else(|| format!("dispatch-pending:{}", lease.execution_id)),
            None => format!("dispatch-pending:{}", lease.execution_id),
        };
        if lease.owner != owner {
            return Err(DbError::VersionConflict.into());
        }
        Ok(())
    }

    pub(crate) async fn defer_placement_refusal(
        &self,
        task: &Task,
        error: &ServiceError,
    ) -> Result<bool> {
        if !crate::placement::admission_refusal_is_retryable(&self.db, &task.id, error).await? {
            return Ok(false);
        }
        let daemon_id = match error {
            ServiceError::DaemonUnavailable { daemon_id }
            | ServiceError::DaemonTimeout { daemon_id, .. } => Some(daemon_id.clone()),
            ServiceError::PlacementUnavailable(refusal) => refusal
                .rejected_candidates
                .iter()
                .find(|candidate| {
                    candidate
                        .filter_codes
                        .contains(&crate::placement::PlacementFilterCode::OwnerUnreachable)
                })
                .and_then(|candidate| candidate.daemon_id.clone()),
            _ => None,
        };
        let reason = error.to_string();
        let metadata = db::TaskMetadata::parse(task.metadata_json.as_deref())
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let now = Utc::now();
        let started_at = metadata
            .extra
            .get("owner_wait")
            .and_then(|wait| wait["started_at"].as_str())
            .and_then(|at| DateTime::parse_from_rfc3339(at).ok())
            .map(|at| at.with_timezone(&Utc))
            .unwrap_or(now);
        let expired = daemon_id.is_some()
            && (now - started_at)
                .to_std()
                .is_ok_and(|elapsed| elapsed >= self.workspace_max_disconnect);
        let owner_wait = daemon_id.as_ref().map(|daemon_id| {
            json!({
                "started_at": started_at.to_rfc3339(), "daemon_id": daemon_id,
            })
            .to_string()
        });
        let deferral = (!expired).then(|| {
            json!({
                "target_state": task.status, "reason": reason,
                "not_before": (now + chrono::Duration::seconds(30)).to_rfc3339(),
            })
            .to_string()
        });
        let annotation = expired.then(|| {
            json!({"type": "recovery_required", "blocking_reason": "owner_disconnected_timeout",
            "message": reason, "blocked_at": now.to_rfc3339(), "blocked_by": "system:workflow",
            "recovery_actions": ["reexecute", "cancel_task"]})
            .to_string()
        });
        let unchanged_reason = !expired
            && crate::deferred_dispatch::pending_until(task).is_some_and(|pending| {
                pending.reason == reason && pending.target_state == task.status
            })
            && metadata
                .extra
                .get("owner_wait")
                .and_then(|wait| wait["daemon_id"].as_str())
                == daemon_id.as_deref();
        let blocked = expired.then(|| {
            json!({"reason": reason, "created_at": now.to_rfc3339(),
            "kind": api_types::FailureKind::RecoveryRequired, "execution_id": null})
            .to_string()
        });
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let result = sqlx::query("UPDATE task SET metadata_json = CASE WHEN ? IS NULL THEN
            json_remove(COALESCE(metadata_json, '{}'), '$.deferred_dispatch') ELSE
            json_set(COALESCE(metadata_json, '{}'), '$.deferred_dispatch', json(?)) END,
            error_annotation = COALESCE(?, error_annotation),
            blocked_json = COALESCE(?, blocked_json), updated_at = ?, version = version + ? WHERE id = ? AND version = ?")
            .bind(&deferral).bind(&deferral).bind(&annotation).bind(&blocked).bind(now.to_rfc3339()).bind(i64::from(!unchanged_reason))
            .bind(&task.id).bind(task.version).execute(&mut *tx).await?;
        if result.rows_affected() != 1 {
            return Err(DbError::VersionConflict.into());
        }
        if let Some(owner_wait) = owner_wait.filter(|_| !unchanged_reason) {
            sqlx::query("UPDATE task SET metadata_json = json_set(COALESCE(metadata_json, '{}'), '$.owner_wait', json(?)) WHERE id = ?")
                .bind(owner_wait).bind(&task.id).execute(&mut *tx).await?;
        }
        if daemon_id.is_some() && !unchanged_reason {
            crate::placement::admission::record_wait_attention_in_tx(
                &self.db,
                &mut tx,
                task,
                "runtime_offline",
                &if expired {
                    format!("Workspace owner wait expired: {reason}")
                } else {
                    format!("Waiting for workspace owner: {reason}")
                },
                &format!("task-owner-wait:{}", task.id),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(!expired)
    }

    pub(crate) async fn expire_owner_wait(&self, task: &Task) -> Result<bool> {
        let Ok(metadata) = db::TaskMetadata::parse(task.metadata_json.as_deref()) else {
            return Ok(false);
        };
        let Some(wait) = metadata.extra.get("owner_wait") else {
            return Ok(false);
        };
        let Some(daemon_id) = wait["daemon_id"].as_str() else {
            return Ok(false);
        };
        let expired = wait["started_at"]
            .as_str()
            .and_then(|at| DateTime::parse_from_rfc3339(at).ok())
            .and_then(|at| (Utc::now() - at.with_timezone(&Utc)).to_std().ok())
            .is_some_and(|elapsed| elapsed >= self.workspace_max_disconnect);
        if !expired || task.blocked_json.is_some() {
            return Ok(false);
        }
        let error = ServiceError::DaemonUnavailable {
            daemon_id: daemon_id.into(),
        };
        self.defer_placement_refusal(task, &error).await?;
        // A placement may become terminal between the scan and admission. Only
        // skip normal recovery if the expiry actually wrote its blocker.
        let Some(current) = TaskRepo::get_by_id(&*self.db, &task.id, false).await? else {
            return Ok(false);
        };
        if current.error_annotation.as_deref().is_some_and(|raw| {
            serde_json::from_str::<serde_json::Value>(raw).is_ok_and(|annotation| {
                annotation["blocking_reason"] == "owner_disconnected_timeout"
            })
        }) && current.blocked_json.is_some()
        {
            return Ok(true);
        }
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        sqlx::query("UPDATE task SET metadata_json = json_remove(metadata_json, '$.owner_wait', '$.deferred_dispatch') WHERE id = ? AND version = ?")
            .bind(&task.id).bind(current.version).execute(&mut *tx).await?;
        sqlx::query("UPDATE attention_projection SET status = 'resolved', resolved_at = ?, updated_at = ?, version = version + 1 WHERE dedupe_key = ? AND status <> 'resolved'")
            .bind(now_rfc3339()).bind(now_rfc3339()).bind(format!("task-owner-wait:{}", task.id)).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(false)
    }

    pub(super) async fn reserve_claim_workspace(
        &self,
        task: &Task,
        agent: Option<&Agent>,
        role: &str,
    ) -> Result<WorkspaceAdmission> {
        use crate::placement::{load_selection_context, select_placement, SelectionLoadInput};
        if agent.is_some() {
            self.ensure_project_not_paused(task).await?;
        }
        let authority = resolve_task_repository_authority(&self.db, task).await?;
        let repo = &authority.repo;
        let settings: ProjectSettings =
            serde_json::from_str(&authority.project.settings).map_err(|error| {
                ServiceError::invalid_operation(format!("invalid project settings: {error}"))
            })?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &authority.project.workflow_definition,
            &Actor::system(api_types::SystemComponent::General),
        );
        let review_source = json!({
            "workflow": workflow,
            "project_settings": parse_json_value("project settings", &authority.project.settings)?,
            "task_scope": { "task_type": task.task_type,
                "config": task.task_state_config.as_deref().map(|raw| parse_json_value("task state config", raw)).transpose()? },
        });
        let review_config: api_types::ReviewConfig = serde_json::from_value(
            api_types::effective_review_config(&review_source)
                .map_err(ServiceError::invalid_operation)?,
        )
        .map_err(|error| {
            ServiceError::invalid_operation(format!("invalid review config: {error}"))
        })?;
        let claiming_agent = agent.map(|agent| worktree_agent(role, agent.clone()));
        let mut worktree_agents = Vec::new();
        let mut assignments = TaskRoleAssignmentRepo::list_by_task(&*self.db, &task.id).await?;
        // Root roles also constrain the owner of their shared workspace, even
        // though only coder is an inherited child execution assignment.
        if let Some(root_id) = task.parent_task_id.as_deref() {
            for assignment in TaskRoleAssignmentRepo::list_by_task(&*self.db, root_id).await? {
                if assignment.role_name != "coder"
                    && !assignments
                        .iter()
                        .any(|existing| existing.role_name == assignment.role_name)
                {
                    assignments.push(assignment);
                }
            }
        }
        // An explicit empty child coder row overrides the root default.
        assignments.retain(|assignment| assignment.role_name != "coder");
        if let Some(coder) =
            crate::task_hierarchy::effective_coder_assignment(&self.db, task).await?
        {
            assignments.push(coder.assignment);
        }
        for assignment in assignments {
            if !matches!(
                assignment.role_name.as_str(),
                "coder" | "executor" | "reviewer" | "planner" | "auditor"
            ) || assignment.assignee_type != Some(AssigneeKind::Agent)
            {
                continue;
            }
            if let Some(id) = assignment.assignee_id.as_deref() {
                if agent.is_some_and(|agent| agent.id == id) {
                    continue;
                }
                let assigned_agent = AgentRepo::get_by_id(&*self.db, id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("agent", id.to_owned()))?;
                worktree_agents.push(worktree_agent(&assignment.role_name, assigned_agent));
            }
        }
        let server_facts = server_executor_facts(
            &self.db,
            claiming_agent.iter().chain(worktree_agents.iter()),
            self.placement_adapter_registry.as_deref(),
        )
        .await?;
        let workspace_task_id = task.parent_task_id.as_deref().unwrap_or(&task.id);
        // Verify an upgraded managed clone only when this admission can use
        // the embedded owner. An existing daemon placement stays on its owner.
        if claiming_agent
            .iter()
            .chain(worktree_agents.iter())
            .all(|role| {
                role.agent.daemon_id.is_none()
                    || role.agent.daemon_id == server_facts.execution_daemon_id
            })
        {
            let bound_location = sqlx::query_scalar::<_, String>(
                "SELECT p.repo_location_id FROM workspace_placement p
                 JOIN workspace w ON w.id = p.workspace_id WHERE w.task_id = ?
                 AND (p.state IN ('reserved', 'preparing', 'ready', 'disconnected', 'cleaning')
                      OR (p.state = 'failed' AND p.workspace_handle IS NOT NULL))",
            )
            .bind(workspace_task_id)
            .fetch_optional(self.db.pool())
            .await?;
            let lock_key = repo
                .local_path
                .as_deref()
                .map(str::trim)
                .filter(|path| !path.is_empty() && Path::new(path).exists())
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    self.workspace_root
                        .join(".repos")
                        .join(&repo.id)
                        .to_string_lossy()
                        .into_owned()
                });
            let locks = self.repo_cache_locks.clone().unwrap_or_default();
            // Use the same physical-source key as every clone/worktree writer.
            let locations: Vec<(String, String, i64, Option<String>)> = sqlx::query_as(
                "SELECT id, path, version, last_error FROM repo_location WHERE repo_id = ?
                 AND owner_kind = 'server' AND daemon_id IS NULL AND runtime_id IS NULL
                 AND kind = 'managed_clone' AND (status = 'unverified' OR (status = 'unavailable' AND last_error LIKE '{%')
                      OR (status = 'invalid' AND last_error = 'managed_clone_is_worktree'))
                 AND (? IS NULL OR id = ?)")
                .bind(&repo.id).bind(&bound_location).bind(&bound_location).fetch_all(self.db.pool()).await?;
            for (id, path, version, last_error) in locations {
                let original_path = path.clone();
                let retry: Value = last_error
                    .as_deref()
                    .and_then(|raw| serde_json::from_str(raw).ok())
                    .unwrap_or(Value::Null);
                if retry["retry_at"]
                    .as_str()
                    .and_then(|at| DateTime::parse_from_rfc3339(at).ok())
                    .is_some_and(|at| at > Utc::now())
                {
                    continue;
                }
                let _guard = locks.acquire(&lock_key).await;
                let source = if Path::new(&path).join(".git").is_file() {
                    resolve_repo_source(repo, &self.workspace_root).await
                } else if Path::new(&path).exists() {
                    Ok(path)
                } else {
                    resolve_repo_source(repo, &self.workspace_root).await
                };
                let (source, verification) = match source {
                    Ok(source) => {
                        let verification =
                            crate::repo_location::verify_managed_clone(repo, &source).await;
                        (source, verification)
                    }
                    Err(error) => {
                        let attempts = retry["attempts"].as_u64().unwrap_or(0) + 1;
                        let retry_at = (Utc::now()
                            + chrono::Duration::seconds(
                                30 * (1_i64 << attempts.saturating_sub(1).min(7)),
                            ))
                        .to_rfc3339();
                        (lock_key.clone(), crate::repo_location::LocationVerification::unavailable(json!({
                            "cause": "clone_failed", "attempts": attempts, "retry_at": retry_at,
                            "message": bounded_redacted_remote_diagnostic(&error.to_string()),
                        }).to_string()))
                    }
                };
                let mut expected_version = version;
                for _ in 0..3 {
                    let result = db::RepoLocationRepo::update(
                        &*self.db,
                        db::UpdateRepoLocation {
                            id: id.clone(),
                            expected_version,
                            path: Some(source.clone()),
                            kind: None,
                            is_default: None,
                            status: Some(verification.status.clone()),
                            last_verified_at: Some(Some(now_rfc3339())),
                            last_error: Some(verification.last_error.clone()),
                            updated_at: now_rfc3339(),
                        },
                    )
                    .await;
                    match result {
                        Ok(_) => break,
                        Err(DbError::VersionConflict) => {
                            let Some(current) =
                                db::RepoLocationRepo::get_by_id(&*self.db, &id).await?
                            else {
                                break;
                            };
                            // PATCH may have replaced the location, not just its default flag.
                            if (current.path != original_path && current.path != source)
                                || current.kind != RepoLocationKind::ManagedClone
                            {
                                break;
                            }
                            expected_version = current.version;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            }
        }

        let empty_registry = crate::daemon_transport::DaemonConnectionRegistry::without_handlers();
        let registry = self
            .daemon_connections
            .as_deref()
            .unwrap_or(&empty_registry);
        let handshakes = connection_handshakes(registry);
        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        crate::placement::admission::sweep_expired_reservations_in_tx(
            &mut transaction,
            &now_rfc3339(),
        )
        .await?;
        let current_authority = self
            .resolve_task_repository_in_tx(&mut transaction, task)
            .await?;
        if current_authority.repo_id != repo.id {
            return Err(DbError::VersionConflict.into());
        }
        let project_version =
            sqlx::query_scalar::<_, i64>("SELECT version FROM project WHERE id = ?")
                .bind(&task.project_id)
                .fetch_one(&mut *transaction)
                .await?;
        if project_version != authority.project.version {
            return Err(DbError::VersionConflict.into());
        }
        for role in claiming_agent.iter().chain(worktree_agents.iter()) {
            let current_version =
                sqlx::query_scalar::<_, i64>("SELECT version FROM agent_current WHERE id = ?")
                    .bind(&role.agent.id)
                    .fetch_optional(&mut *transaction)
                    .await?;
            if current_version != Some(role.agent.version) {
                return Err(DbError::VersionConflict.into());
            }
        }
        let workspace_id =
            sqlx::query_scalar::<_, String>("SELECT id FROM workspace WHERE task_id = ?")
                .bind(workspace_task_id)
                .fetch_optional(&mut *transaction)
                .await?;
        let existing = match workspace_id.as_deref() {
            Some(id) => {
                WorkspacePlacementRepo::get_by_workspace_id_in_tx(&*self.db, &mut transaction, id)
                    .await?
            }
            None => None,
        };
        if let Some(workspace_id) = workspace_id.as_deref() {
            if let Some(execution_id) = sqlx::query_scalar::<_, String>(
                "SELECT id FROM execution WHERE workspace_id = ? AND status = 'running' LIMIT 1",
            )
            .bind(workspace_id)
            .fetch_optional(&mut *transaction)
            .await?
            {
                return Err(ServiceError::execution_already_running(
                    "repository",
                    execution_id,
                ));
            }
        }
        if existing.as_ref().is_some_and(|placement| {
            matches!(
                placement.state,
                PlacementState::Reserved | PlacementState::Preparing
            )
        }) {
            return Err(ServiceError::conflict(
                "workspace preparation is already reserved",
            ));
        }
        if let Some(placement) = existing.as_ref().filter(|placement| {
            placement.state == PlacementState::Cleaning
                || (placement.state == PlacementState::Failed
                    && placement.workspace_handle.is_some())
        }) {
            return Err(ServiceError::WorkspaceResetRequired {
                task_id: task.id.clone(),
                reason: format!(
                    "workspace placement is {}: {:?}",
                    placement.state, placement.failure_cause
                ),
            });
        }
        let location_count =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM repo_location WHERE repo_id = ?")
                .bind(&repo.id)
                .fetch_one(&mut *transaction)
                .await?;
        if location_count == 0 {
            let source = repo
                .local_path
                .as_deref()
                .map(str::trim)
                .filter(|path| !path.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    self.workspace_root
                        .join(".repos")
                        .join(&repo.id)
                        .to_string_lossy()
                        .into_owned()
                });
            server_repo_location_in_tx(&mut transaction, repo, &source, &now_rfc3339()).await?;
        }
        let mut context = match claiming_agent.as_ref() {
            Some(claiming_agent) => Some(
                load_selection_context(
                    &self.db,
                    &mut transaction,
                    registry,
                    SelectionLoadInput {
                        task,
                        repo,
                        claiming_agent,
                        worktree_agents: &worktree_agents,
                        task_owner_id: authority.project.owner_id.as_deref(),
                        workspace_id: if task.parent_task_id.is_none() {
                            workspace_id.as_deref()
                        } else {
                            None
                        },
                        inherited_root_workspace_id: if task.parent_task_id.is_some() {
                            workspace_id.as_deref()
                        } else {
                            None
                        },
                        review_config: &review_config,
                        project_settings: &settings,
                        server: &server_facts,
                        handshakes: &handshakes,
                    },
                )
                .await?,
            ),
            None => None,
        };
        let (
            location_id,
            owner_kind,
            daemon_id,
            runtime_id,
            execution_daemon_id,
            selected_by,
            reason,
        ) = if let Some(context) = context.as_ref() {
            let selection = select_placement(context).into_result()?;
            let location = selection.candidate.location;
            (
                location.id,
                if location.owner_kind == db::RepoLocationOwnerKind::Daemon {
                    PlacementOwnerKind::Daemon
                } else {
                    PlacementOwnerKind::Server
                },
                if location.owner_kind == db::RepoLocationOwnerKind::Daemon {
                    location.daemon_id
                } else {
                    None
                },
                if location.owner_kind == db::RepoLocationOwnerKind::Daemon {
                    location.runtime_id
                } else {
                    None
                },
                selection.candidate.execution_daemon_id,
                selection.selected_by,
                serde_json::to_string(&selection.selection_reason)
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
            )
        } else {
            let row = sqlx::query("SELECT id FROM repo_location WHERE repo_id = ? AND owner_kind = 'server' AND status = 'ready'
                AND daemon_id IS NULL ORDER BY is_default DESC, created_at, id LIMIT 1")
                .bind(&repo.id).fetch_optional(&mut *transaction).await?;
            let location_id = existing
                .as_ref()
                .filter(|placement| placement.owner_kind == PlacementOwnerKind::Server)
                .map(|placement| placement.repo_location_id.clone())
                .or_else(|| row.map(|row| row.get("id")))
                .ok_or_else(|| {
                    ServiceError::PlacementUnavailable(crate::placement::PlacementUnavailable {
                        task_id: task.id.clone(),
                        repo_id: repo.id.clone(),
                        rejected_candidates: Vec::new(),
                    })
                })?;
            (
                location_id,
                PlacementOwnerKind::Server,
                None,
                None,
                None,
                PlacementSelectedBy::Scheduler,
                json!({"rule": "server_owned", "rejected_candidates": []}).to_string(),
            )
        };
        let now = now_rfc3339();
        let fresh = workspace_id.is_none();
        let workspace_id = workspace_id.unwrap_or_else(new_uuid_v4);
        if fresh {
            let server_path = if owner_kind == PlacementOwnerKind::Server {
                self.workspace_root
                    .join(workspace_task_id)
                    .join(&repo.name)
                    .to_string_lossy()
                    .into_owned()
            } else {
                String::new()
            };
            sqlx::query("INSERT INTO workspace (id, task_id, repo_id, worktree_path, branch, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'creating', ?, ?)")
                .bind(&workspace_id).bind(workspace_task_id).bind(&repo.id).bind(server_path)
                .bind(::workspace::task_branch_name(workspace_task_id)).bind(&now).bind(&now)
                .execute(&mut *transaction).await?;
        } else {
            let workspace_repo =
                sqlx::query_scalar::<_, String>("SELECT repo_id FROM workspace WHERE id = ?")
                    .bind(&workspace_id)
                    .fetch_one(&mut *transaction)
                    .await?;
            if workspace_repo != repo.id {
                return Err(ServiceError::WorkspaceResetRequired {
                    task_id: workspace_task_id.to_owned(),
                    reason: "workspace repository differs from current Project repository"
                        .to_owned(),
                });
            }
        }
        let placement = if let Some(placement) = existing {
            let prepared =
                placement.workspace_handle.is_some() && placement.state != PlacementState::Cleaned;
            if prepared && placement.state != PlacementState::Ready {
                if placement.state == PlacementState::Disconnected {
                    return Err(ServiceError::DaemonUnavailable {
                        daemon_id: placement
                            .daemon_id
                            .clone()
                            .or(placement.execution_daemon_id.clone())
                            .unwrap_or_default(),
                    });
                }
                return Err(ServiceError::WorkspaceResetRequired {
                    task_id: task.id.clone(),
                    reason: format!("workspace placement is {}", placement.state),
                });
            }
            let mut update = crate::placement::admission::placement_update(&placement);
            update.agent_id = Some(agent.map(|agent| agent.id.clone()));
            update.selected_by = Some(selected_by);
            update.selection_reason = Some(reason);
            if prepared
                && placement.owner_kind == PlacementOwnerKind::Server
                && placement.execution_daemon_id.is_none()
            {
                update.execution_daemon_id = Some(execution_daemon_id.clone());
            }
            if !prepared {
                update.owner_kind = Some(owner_kind.clone());
                update.daemon_id = Some(daemon_id);
                update.runtime_id = Some(runtime_id);
                update.repo_location_id = Some(location_id);
                update.execution_daemon_id = Some(execution_daemon_id);
                update.workspace_handle = Some(None);
                update.state = Some(PlacementState::Reserved);
                update.reserved_until = Some(Some(super::execution::rfc3339_after(
                    &now,
                    PLACEMENT_RESERVATION_SECONDS,
                )));
                update.failure_cause = Some(None);
                // A deliberately cleaned workspace is recreated on this owner.
                if placement.state == PlacementState::Cleaned
                    || (placement.state == PlacementState::Failed
                        && placement.failure_cause
                            == Some(db::PlacementFailureCause::PrepareFailed))
                {
                    // A failed prepare is retained by the owner's journal.
                    // Reselection starts a new attempt rather than replaying it.
                    update.generation = Some(placement.generation + 1);
                }
            }
            WorkspacePlacementRepo::update_in_tx(&*self.db, &mut transaction, update).await?
        } else {
            WorkspacePlacementRepo::create_in_tx(
                &*self.db,
                &mut transaction,
                CreateWorkspacePlacement {
                    id: new_uuid_v4(),
                    workspace_id: workspace_id.clone(),
                    task_id: workspace_task_id.to_owned(),
                    agent_id: agent.map(|agent| agent.id.clone()),
                    owner_kind: owner_kind.clone(),
                    daemon_id,
                    runtime_id,
                    repo_location_id: location_id,
                    execution_daemon_id,
                    workspace_handle: None,
                    generation: 1,
                    state: PlacementState::Reserved,
                    selected_by,
                    selection_reason: reason,
                    reserved_until: Some(super::execution::rfc3339_after(
                        &now,
                        PLACEMENT_RESERVATION_SECONDS,
                    )),
                    disconnected_at: None,
                    failure_cause: None,
                    created_at: now.clone(),
                    updated_at: now,
                },
            )
            .await?
        };
        if placement.state == PlacementState::Reserved {
            let path = if placement.owner_kind == PlacementOwnerKind::Server {
                self.workspace_root
                    .join(workspace_task_id)
                    .join(&repo.name)
                    .to_string_lossy()
                    .into_owned()
            } else {
                String::new()
            };
            sqlx::query("UPDATE workspace SET status = 'creating', worktree_path = ?, error = NULL, updated_at = ? WHERE id = ?")
                .bind(path).bind(&placement.updated_at).bind(&workspace_id).execute(&mut *transaction).await?;
        }
        if let Some(context) = context.as_mut() {
            context.existing_placement = Some(placement.clone());
            context.inherited_root_placement = None;
        }
        transaction.commit().await?;
        let workspace = WorkspaceRepo::get_by_id(&*self.db, &workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", workspace_id))?;
        Ok(WorkspaceAdmission {
            claiming_task: task.clone(),
            workspace,
            placement,
            selection_context: context,
            server_facts,
            review_config,
            settings,
            task_owner_id: authority.project.owner_id,
        })
    }

    pub(super) async fn prepare_claim_workspace(
        &self,
        mut admission: WorkspaceAdmission,
    ) -> Result<WorkspaceAdmission> {
        if admission.placement.state == PlacementState::Ready {
            let needs_recreation = match async {
                let backend = self
                    .workspace_backend_router
                    .for_placement(&admission.placement)?;
                backend.describe(&admission.placement).await
            }
            .await
            {
                Ok(state) => !state.exists,
                Err(error)
                    if admission.placement.owner_kind == PlacementOwnerKind::Server
                        && worktree_describe_needs_recreation(&error) =>
                {
                    true
                }
                Err(error) => {
                    let cause = match &error {
                        crate::workspace_backend::WorkspaceBackendError::StaleGeneration {
                            ..
                        } => Some(db::PlacementFailureCause::StaleGeneration),
                        crate::workspace_backend::WorkspaceBackendError::WrongOwner { .. } => {
                            Some(db::PlacementFailureCause::WrongOwner)
                        }
                        _ => None,
                    };
                    if let Some(cause) = cause {
                        let mut update =
                            crate::placement::admission::placement_update(&admission.placement);
                        update.failure_cause = Some(Some(cause.clone()));
                        match WorkspacePlacementRepo::update(&*self.db, update).await {
                            Ok(placement) => admission.placement = placement,
                            Err(DbError::VersionConflict) => {}
                            Err(error) => return Err(error.into()),
                        }
                        tracing::error!(placement_id = %admission.placement.id, failure_cause = %cause, %error, "owner rejected workspace describe fence");
                        crate::placement::admission::record_fence_rejection(
                            &self.db,
                            &admission.placement,
                            &cause,
                            &error.to_string(),
                        )
                        .await?;
                    }
                    return Err(error.into());
                }
            };
            if needs_recreation {
                admission.workspace = prepare_workspace(
                    &self.db,
                    &self.workspace_root,
                    &admission.claiming_task,
                    &admission.claiming_task.id,
                    self.repo_cache_locks.clone(),
                    &self.workspace_backend_router,
                )
                .await?;
                admission.placement =
                    WorkspacePlacementRepo::get_by_id(&*self.db, &admission.placement.id)
                        .await?
                        .ok_or(DbError::NotFound)?;
            }
            admission.workspace =
                clear_workspace_cleanup_after(&self.db, admission.workspace).await?;
            return Ok(admission);
        }
        let mut update = crate::placement::admission::placement_update(&admission.placement);
        update.state = Some(PlacementState::Preparing);
        admission.placement = WorkspacePlacementRepo::update(&*self.db, update).await?;
        let remaining = admission
            .placement
            .reserved_until
            .as_deref()
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
            .map(|value| value.with_timezone(&Utc))
            .map(|until| (until - Utc::now()).to_std().unwrap_or(Duration::ZERO))
            .unwrap_or(Duration::ZERO);
        let result = tokio::time::timeout(remaining, async {
            let backend = self
                .workspace_backend_router
                .for_placement(&admission.placement)?;
            let repo = RepoRepo::get_by_id(&*self.db, &admission.workspace.repo_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::not_found("repo", admission.workspace.repo_id.clone())
                })?;
            backend
                .prepare(
                    &admission.placement,
                    &crate::workspace_backend::PrepareSpec {
                        base_ref: repo.default_branch,
                    },
                )
                .await
        })
        .await;
        let prepared = match result {
            Ok(Ok(prepared)) => prepared,
            failure => {
                let (message, cause, refusal) = match failure {
                    Ok(Err(
                        error @ crate::workspace_backend::WorkspaceBackendError::StaleGeneration {
                            ..
                        },
                    )) => (
                        error.to_string(),
                        db::PlacementFailureCause::StaleGeneration,
                        Some(error),
                    ),
                    Ok(Err(
                        error @ crate::workspace_backend::WorkspaceBackendError::WrongOwner {
                            ..
                        },
                    )) => (
                        error.to_string(),
                        db::PlacementFailureCause::WrongOwner,
                        Some(error),
                    ),
                    Ok(Err(error)) => (
                        error.to_string(),
                        db::PlacementFailureCause::PrepareFailed,
                        None,
                    ),
                    Err(_) => (
                        "workspace reservation expired during preparation".to_owned(),
                        db::PlacementFailureCause::PrepareFailed,
                        None,
                    ),
                    _ => unreachable!(),
                };
                match crate::placement::admission::fail_preparation(
                    &self.db,
                    &admission.placement,
                    cause.clone(),
                )
                .await
                {
                    Ok(()) | Err(ServiceError::Db(DbError::VersionConflict)) => {}
                    Err(error) => return Err(error),
                }
                if let Some(error) = refusal {
                    tracing::error!(placement_id = %admission.placement.id, failure_cause = %cause, %error, "owner rejected workspace preparation fence");
                    crate::placement::admission::record_fence_rejection(
                        &self.db,
                        &admission.placement,
                        &cause,
                        &message,
                    )
                    .await?;
                    return Err(error.into());
                }
                return Err(ServiceError::PrepareFailed {
                    placement_id: admission.placement.id,
                    message,
                });
            }
        };
        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        let now = now_rfc3339();
        crate::placement::admission::sweep_expired_reservations_in_tx(&mut transaction, &now)
            .await?;
        let current = WorkspacePlacementRepo::get_by_id_in_tx(
            &*self.db,
            &mut transaction,
            &admission.placement.id,
        )
        .await?
        .ok_or(DbError::NotFound)?;
        if current.state == PlacementState::Failed
            && current.failure_cause == Some(db::PlacementFailureCause::PrepareFailed)
        {
            transaction.commit().await?;
            return Err(ServiceError::PrepareFailed {
                placement_id: current.id,
                message: "workspace reservation expired during preparation".to_owned(),
            });
        }
        let mut update = crate::placement::admission::placement_update(&admission.placement);
        update.state = Some(PlacementState::Ready);
        update.workspace_handle = Some(Some(prepared.handle.clone()));
        update.reserved_until = Some(None);
        admission.placement =
            WorkspacePlacementRepo::update_in_tx(&*self.db, &mut transaction, update).await?;
        sqlx::query("UPDATE workspace SET status = 'ready', branch = ?, before_sha = COALESCE(before_sha, ?),
                worktree_path = ?, error = NULL, cleanup_after = NULL, cleanup_attempts = 0,
                last_cleanup_error = NULL, updated_at = ? WHERE id = ?")
            .bind(prepared.branch).bind(prepared.base_sha)
            .bind(if admission.placement.owner_kind == PlacementOwnerKind::Server { prepared.handle } else { String::new() })
            .bind(now).bind(&admission.workspace.id).execute(&mut *transaction).await?;
        transaction.commit().await?;
        admission.workspace = WorkspaceRepo::get_by_id(&*self.db, &admission.workspace.id)
            .await?
            .ok_or(DbError::NotFound)?;
        Ok(admission)
    }

    pub(super) async fn check_claim_placement_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        task: &Task,
        admission: &WorkspaceAdmission,
    ) -> Result<()> {
        let current = WorkspacePlacementRepo::get_by_id_in_tx(
            &*self.db,
            transaction,
            &admission.placement.id,
        )
        .await?
        .ok_or(DbError::NotFound)?;
        if current.version != admission.placement.version || current.state != PlacementState::Ready
        {
            return Err(DbError::VersionConflict.into());
        }
        if let Some(previous) = admission.selection_context.as_ref() {
            for role in
                std::iter::once(&previous.claiming_agent).chain(previous.worktree_agents.iter())
            {
                let current_version =
                    sqlx::query_scalar::<_, i64>("SELECT version FROM agent_current WHERE id = ?")
                        .bind(&role.agent.id)
                        .fetch_optional(&mut **transaction)
                        .await?;
                if current_version != Some(role.agent.version) {
                    return Err(DbError::VersionConflict.into());
                }
            }
            let empty_registry =
                crate::daemon_transport::DaemonConnectionRegistry::without_handlers();
            let registry = self
                .daemon_connections
                .as_deref()
                .unwrap_or(&empty_registry);
            let handshakes = connection_handshakes(registry);
            let context = crate::placement::load_selection_context(
                &self.db,
                transaction,
                registry,
                crate::placement::SelectionLoadInput {
                    task,
                    repo: &previous.repo,
                    claiming_agent: &previous.claiming_agent,
                    worktree_agents: &previous.worktree_agents,
                    task_owner_id: admission.task_owner_id.as_deref(),
                    workspace_id: Some(&admission.workspace.id),
                    inherited_root_workspace_id: None,
                    review_config: &admission.review_config,
                    project_settings: &admission.settings,
                    server: &admission.server_facts,
                    handshakes: &handshakes,
                },
            )
            .await?;
            crate::placement::select_placement(&context).into_result()?;
        }
        sqlx::query(
            "UPDATE task SET metadata_json = json_remove(metadata_json, '$.owner_wait')
            WHERE id = ? AND json_type(metadata_json, '$.owner_wait') IS NOT NULL",
        )
        .bind(&task.id)
        .execute(&mut **transaction)
        .await?;
        sqlx::query("UPDATE attention_projection SET status = 'resolved', resolved_at = ?, updated_at = ?, version = version + 1
            WHERE dedupe_key = ? AND status <> 'resolved'")
            .bind(now_rfc3339()).bind(now_rfc3339()).bind(format!("task-owner-wait:{}", task.id))
            .execute(&mut **transaction).await?;
        Ok(())
    }
}

fn worktree_agent(role: &str, agent: Agent) -> crate::placement::WorktreeAgent {
    crate::placement::WorktreeAgent {
        role: role.to_owned(),
        agent,
        required_capabilities: api_types::ExecutorAdapterCapabilityFacts {
            cancel_ack: true,
            terminal_observed: true,
            ..Default::default()
        },
    }
}

fn connection_handshakes(
    registry: &crate::daemon_transport::DaemonConnectionRegistry,
) -> std::collections::BTreeMap<String, crate::placement::ConnectionHandshake> {
    registry
        .connection_snapshots()
        .into_iter()
        .map(|(id, facts)| {
            (
                id,
                crate::placement::ConnectionHandshake {
                    connection_id: facts.connection_id,
                    handshake: facts.handshake,
                },
            )
        })
        .collect()
}

async fn server_executor_facts<'a>(
    db: &SqliteDb,
    agents: impl Iterator<Item = &'a crate::placement::WorktreeAgent>,
    registry: Option<&executors::AdapterRegistry>,
) -> Result<crate::placement::ServerFacts> {
    let default_registry;
    let registry = match registry {
        Some(registry) => registry,
        None => {
            default_registry = cli_adapters::default_registry();
            &default_registry
        }
    };
    let mut server = crate::placement::ServerFacts {
        execution_daemon_id: sqlx::query_scalar::<_, String>(
            "SELECT id FROM daemon WHERE machine_id = ? AND status <> 'offline'",
        )
        .bind(crate::embedded_daemon::embedded_machine_id())
        .fetch_optional(db.pool())
        .await?,
        ..Default::default()
    };
    for role in agents {
        let agent = &role.agent;
        let available = if agent.backend_kind == "native" {
            db::AgentConnectionHealthRepo::get_connection_health(db, &agent.profile_id)
                .await?
                .is_some_and(|health| health.status == "healthy")
        } else {
            agent
                .executor_type
                .parse::<ExecutorKind>()
                .ok()
                .and_then(|kind| registry.get(&kind))
                .is_some_and(|adapter| {
                    matches!(
                        adapter.check_availability().status,
                        executors::AvailabilityStatus::Authenticated
                    )
                })
        };
        server.executors.insert(
            agent.id.clone(),
            crate::placement::ExecutorFacts {
                installed: available,
                authenticated: available,
                enabled: !agent.paused,
                capabilities:
                    crate::daemon_transport::EmbeddedExecutionProvider::adapter_capabilities(
                        &agent.executor_type,
                    ),
            },
        );
    }
    Ok(server)
}

#[cfg(test)]
pub(crate) async fn prepare_workspace_for_test(
    db: &SqliteDb,
    workspace_root: &Path,
    task: &Task,
    task_id: &str,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
) -> Result<Workspace> {
    let router = crate::lifecycle::context::embedded_workspace_router_for_test(
        Arc::new(db.clone()),
        workspace_root.to_path_buf(),
        repo_cache_locks.clone(),
    );
    prepare_workspace(db, workspace_root, task, task_id, repo_cache_locks, &router).await
}

pub(crate) async fn prepare_workspace(
    db: &SqliteDb,
    workspace_root: &Path,
    task: &Task,
    task_id: &str,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
    router: &crate::workspace_backend::WorkspaceBackendRouter,
) -> Result<Workspace> {
    Ok(
        prepare_workspace_owned(db, workspace_root, task, task_id, repo_cache_locks, router)
            .await?
            .0,
    )
}

/// Return the persisted workspace only after its recorded owner can use it.
///
/// Server-owned workspaces are checked as real Git worktrees and recovered
/// from their surviving Task branch when their directory or Git metadata is
/// missing. Daemon-owned placements retain the existing describe/prepare
/// behavior; Forge never interprets their opaque handle as a local path.
pub(crate) async fn ensure_valid(
    db: &SqliteDb,
    workspace_root: &Path,
    task: &Task,
    workspace: Workspace,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
    delete_missing_workspace: bool,
    router: &crate::workspace_backend::WorkspaceBackendRouter,
) -> Result<Workspace> {
    if let Some(placement) = WorkspacePlacementRepo::get_by_workspace_id(db, &workspace.id).await? {
        if placement.owner_kind == PlacementOwnerKind::Daemon {
            match placement.state {
                PlacementState::Ready => {}
                PlacementState::Disconnected => {
                    return Err(ServiceError::DaemonUnavailable {
                        daemon_id: placement.daemon_id.unwrap_or_default(),
                    })
                }
                _ => {
                    return Err(ServiceError::WorkspaceResetRequired {
                        task_id: task.id.clone(),
                        reason: format!("workspace placement is {}", placement.state),
                    })
                }
            }
            let resolved = router.resolve(db, &workspace).await?;
            let needs_recreation = match resolved.backend.describe(&resolved.placement).await {
                Ok(state) => !state.exists,
                Err(error) => return Err(error.into()),
            };
            if needs_recreation {
                if sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM execution WHERE workspace_id = ? AND status = 'running')")
                    .bind(&workspace.id).fetch_one(db.pool()).await? {
                    return Err(ServiceError::conflict("workspace still has a running execution"));
                }
                let recovered = resolved
                    .backend
                    .prepare(
                        &resolved.placement,
                        &crate::workspace_backend::PrepareSpec {
                            base_ref: workspace.before_sha.clone().ok_or_else(|| {
                                ServiceError::WorkspaceResetRequired {
                                    task_id: workspace.task_id.clone(),
                                    reason: "workspace has no recorded recovery base".to_owned(),
                                }
                            })?,
                        },
                    )
                    .await?;
                let mut update = crate::placement::admission::placement_update(&resolved.placement);
                update.workspace_handle = Some(Some(recovered.handle));
                WorkspacePlacementRepo::update(db, update).await?;
            }
            return clear_workspace_cleanup_after(db, workspace).await;
        }
    }

    if workspace.status != WorkspaceStatus::Ready {
        return Err(ServiceError::invalid_operation(format!(
            "workspace for task {} is not ready",
            workspace.task_id
        )));
    }
    let repo = RepoRepo::get_by_id(db, &workspace.repo_id)
        .await?
        .filter(|repo| repo.project_id == task.project_id)
        .ok_or_else(|| ServiceError::not_found("repo", workspace.repo_id.clone()))?;
    let resolved = resolve_workspace_backend(db, workspace_root, &workspace, router).await?;
    let worktree_path = resolved.embedded_path()?;
    match worktree_readiness(&worktree_path).await? {
        WorktreeReadiness::Ready => clear_workspace_cleanup_after(db, workspace).await,
        WorktreeReadiness::Missing | WorktreeReadiness::Invalid => {
            let owner_task_id = workspace.task_id.clone();
            clear_workspace_cleanup_after(
                db,
                recover_missing_worktree(
                    db,
                    workspace_root,
                    &repo,
                    &owner_task_id,
                    workspace,
                    repo_cache_locks,
                    delete_missing_workspace,
                    router,
                )
                .await?,
            )
            .await
        }
    }
}

fn worktree_describe_needs_recreation(
    error: &crate::workspace_backend::WorkspaceBackendError,
) -> bool {
    matches!(error, crate::workspace_backend::WorkspaceBackendError::Other(error)
        if matches!(&**error, ServiceError::Git(_)))
}

/// Prepare a workspace and report whether this call won creation ownership.
/// The ownership bit is consumed by admission-failure cleanup; callers must
/// never infer it from a racy preflight existence query.
pub(crate) async fn prepare_workspace_owned(
    db: &SqliteDb,
    workspace_root: &std::path::Path,
    task: &Task,
    task_id: &str,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
    router: &crate::workspace_backend::WorkspaceBackendRouter,
) -> Result<(Workspace, bool)> {
    let authority = resolve_task_repository_authority(db, task).await?;
    let owner_task_id = task.parent_task_id.as_deref().unwrap_or(task_id);
    if let Some(workspace) = WorkspaceRepo::get_by_task_id(db, owner_task_id).await? {
        if let Some(placement) =
            WorkspacePlacementRepo::get_by_workspace_id(db, &workspace.id).await?
        {
            if placement.owner_kind == PlacementOwnerKind::Daemon {
                ensure_workspace_repository_current(
                    db,
                    task,
                    &workspace,
                    &authority.repo,
                    owner_task_id,
                )
                .await?;
                return Ok((
                    ensure_valid(
                        db,
                        workspace_root,
                        task,
                        workspace,
                        repo_cache_locks,
                        false,
                        router,
                    )
                    .await?,
                    false,
                ));
            }
        }
    }
    if let Some(parent_task_id) = task.parent_task_id.as_deref() {
        let parent_task = TaskRepo::get_by_id(db, parent_task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", parent_task_id.to_owned()))?;
        if parent_task.project_id != authority.project.id {
            return Err(ServiceError::RepoMismatch {
                project_id: task.project_id.clone(),
            });
        }
        let Some(workspace) = WorkspaceRepo::get_by_task_id(db, parent_task_id).await? else {
            // The root is a coordination container, so its first runnable
            // child creates the shared root-owned worktree on demand. A child
            // admission failure must not clean up that shared workspace.
            let (workspace, _) = create_fresh_workspace(
                db,
                workspace_root,
                &authority.repo,
                parent_task_id,
                repo_cache_locks,
            )
            .await?;
            return Ok((workspace, false));
        };
        ensure_workspace_repository_current(db, task, &workspace, &authority.repo, parent_task_id)
            .await?;
        if workspace.status == WorkspaceStatus::Ready {
            return Ok((
                ensure_valid(
                    db,
                    workspace_root,
                    task,
                    workspace,
                    repo_cache_locks,
                    false,
                    router,
                )
                .await?,
                false,
            ));
        }
        return Err(ServiceError::parent_workspace_required(parent_task_id));
    }

    if let Some(workspace) = WorkspaceRepo::get_by_task_id(db, task_id).await? {
        ensure_workspace_repository_current(db, task, &workspace, &authority.repo, task_id).await?;
        if workspace.status == WorkspaceStatus::Cleaned {
            // A deliberate teardown (a reassignment reset, cancellation
            // cleanup) removes the worktree but keeps the row, so the Task's
            // next execution rebuilds it here instead of failing admission
            // on a row that can never become ready by itself.
            let repo_source = resolve_repo_source(&authority.repo, workspace_root).await?;
            let branch_exists =
                recovery_branch_exists(Path::new(&repo_source), &workspace.branch).await?;
            if !branch_exists {
                // Nothing left to recover: start over from the default
                // branch. Deleting the row only unlinks past executions
                // (`ON DELETE SET NULL`); their own records are kept.
                WorkspaceRepo::delete(db, &workspace.id).await?;
                return create_fresh_workspace(
                    db,
                    workspace_root,
                    &authority.repo,
                    task_id,
                    repo_cache_locks,
                )
                .await;
            }
            info!(
                task_id = task_id,
                workspace_id = %workspace.id,
                branch = %workspace.branch,
                "recreating cleaned workspace from its task branch"
            );
            return Ok((
                clear_workspace_cleanup_after(
                    db,
                    recover_missing_worktree(
                        db,
                        workspace_root,
                        &authority.repo,
                        task_id,
                        workspace,
                        repo_cache_locks,
                        true,
                        router,
                    )
                    .await?,
                )
                .await?,
                false,
            ));
        }
        if workspace.status == WorkspaceStatus::Ready {
            return Ok((
                ensure_valid(
                    db,
                    workspace_root,
                    task,
                    workspace,
                    repo_cache_locks,
                    true,
                    router,
                )
                .await?,
                false,
            ));
        }
        return Err(ServiceError::invalid_operation(format!(
            "workspace for task {task_id} is not ready"
        )));
    }

    create_fresh_workspace(
        db,
        workspace_root,
        &authority.repo,
        task_id,
        repo_cache_locks,
    )
    .await
}

async fn ensure_workspace_repository_current(
    db: &SqliteDb,
    task: &Task,
    workspace: &Workspace,
    current_repo: &db::Repo,
    reset_task_id: &str,
) -> Result<()> {
    if workspace.repo_id == current_repo.id {
        return Ok(());
    }

    // A lease tied to an old repository can never authorize work after the
    // Project selects a different primary Repo. Revoke only this Task's lease;
    // the Workspace itself remains intact until the normal guarded reset.
    if let Some(lease) = WorkspaceLeaseRepo::get_active_for_task(db, &task.id).await? {
        WorkspaceLeaseRepo::revoke(db, &lease.id, lease.version, &now_rfc3339()).await?;
    }
    Err(ServiceError::WorkspaceResetRequired {
        task_id: reset_task_id.to_owned(),
        reason: format!(
            "workspace repository {} differs from current Project repository {}; reset is required before a new execution",
            workspace.repo_id, current_repo.id
        ),
    })
}

#[allow(clippy::too_many_arguments)]
async fn recover_missing_worktree(
    db: &SqliteDb,
    workspace_root: &std::path::Path,
    repo: &db::Repo,
    task_id: &str,
    workspace: Workspace,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
    delete_missing_workspace: bool,
    router: &crate::workspace_backend::WorkspaceBackendRouter,
) -> Result<Workspace> {
    let resolved = resolve_workspace_backend(db, workspace_root, &workspace, router).await?;
    let existing_path = resolved.embedded_path()?;
    let readiness = worktree_readiness(&existing_path).await?;
    if matches!(readiness, WorktreeReadiness::Ready) {
        return Ok(workspace);
    }
    warn!(
        task_id = task_id,
        workspace_id = %workspace.id,
        worktree_path = %existing_path.display(),
        readiness = ?readiness,
        "workspace worktree path missing or unusable, attempting recovery"
    );

    let repo_source = resolve_repo_source(repo, workspace_root).await?;

    if !Path::new(&repo_source).exists() {
        return Err(ServiceError::invalid_operation(format!(
            "repo source path does not exist: {repo_source}"
        )));
    }

    if matches!(readiness, WorktreeReadiness::Invalid)
        && try_repair_worktree_gitdir(Path::new(&repo_source), &existing_path).await
    {
        info!(
            task_id = task_id,
            workspace_id = %workspace.id,
            worktree_path = %existing_path.display(),
            "workspace gitdir repaired"
        );
        return Ok(workspace);
    }

    let branch = &workspace.branch;
    let branch_exists = recovery_branch_exists(Path::new(&repo_source), branch).await?;

    if branch_exists {
        if !matches!(readiness, WorktreeReadiness::Missing) {
            move_unusable_worktree_aside(&existing_path).await?;
        }
        let mut manager = WorkspaceManager::new(workspace_root.to_path_buf());
        if let Some(locks) = repo_cache_locks {
            manager = manager.with_repo_cache_locks(locks);
        }
        let worktree_path = manager
            .recover_worktree_named(&repo_source, task_id, &repo.name, branch)
            .await
            .map_err(map_workspace_error)?;
        let before_sha = git::get_current_sha(&worktree_path).await.ok();
        let now = now_rfc3339();
        persist_recovered_workspace(db, repo, &repo_source, &workspace, &worktree_path, &now)
            .await?;

        info!(
            task_id = task_id,
            workspace_id = %workspace.id,
            branch = %branch,
            worktree_path = %worktree_path.display(),
            before_sha = ?before_sha,
            "workspace recovered from existing branch"
        );

        WorkspaceRepo::get_by_id(db, &workspace.id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", workspace.id))
    } else {
        if delete_missing_workspace {
            WorkspaceRepo::delete(db, &workspace.id).await?;
        } else {
            // Delivery and child admission checks keep the persisted row so
            // the required reset remains an explicit owner operation.
            warn!(
                task_id = task_id,
                workspace_id = %workspace.id,
                "task branch is gone; preserving workspace row for explicit reset"
            );
        }
        warn!(
            task_id = task_id,
            branch = %branch,
            "task branch no longer exists in repo, workspace reset required"
        );
        Err(ServiceError::WorkspaceResetRequired {
            task_id: task_id.to_owned(),
            reason: format!(
                "worktree and branch '{branch}' are both gone; workspace must be recreated from {}",
                repo.default_branch
            ),
        })
    }
}

#[derive(Debug)]
pub(crate) enum WorktreeReadiness {
    Ready,
    Missing,
    Invalid,
}

pub(crate) async fn worktree_readiness(worktree_path: &Path) -> Result<WorktreeReadiness> {
    if !tokio::fs::try_exists(worktree_path)
        .await
        .map_err(git::GitError::Io)?
    {
        return Ok(WorktreeReadiness::Missing);
    }
    if !tokio::fs::try_exists(worktree_path.join(".git"))
        .await
        .map_err(git::GitError::Io)?
    {
        return Ok(WorktreeReadiness::Invalid);
    }
    match probe_worktree_head(worktree_path).await {
        Ok(_) => Ok(WorktreeReadiness::Ready),
        Err(error) if git_error_reports_unusable_worktree(&error) => Ok(WorktreeReadiness::Invalid),
        Err(error) => Err(error.into()),
    }
}

fn git_error_reports_unusable_worktree(error: &git::GitError) -> bool {
    let git::GitError::CommandFailed { stdout, stderr, .. } = error else {
        return false;
    };
    [stdout, stderr].iter().any(|output| {
        let output = output.to_ascii_lowercase();
        output.contains("not a git repository") || output.contains("invalid gitfile format")
    })
}

async fn probe_worktree_head(worktree_path: &Path) -> git::Result<String> {
    #[cfg(test)]
    if let Some(failure) = take_injected_worktree_probe_failure(worktree_path) {
        return Err(match failure {
            InjectedWorktreeProbeFailure::Io => {
                git::GitError::Io(std::io::Error::other("injected worktree probe failure"))
            }
            InjectedWorktreeProbeFailure::NotRepository => git::GitError::CommandFailed {
                command: "git rev-parse HEAD".to_owned(),
                stdout: String::new(),
                stderr: "fatal: not a git repository".to_owned(),
            },
        });
    }
    git::get_current_sha(worktree_path).await
}

async fn recovery_branch_exists(repo_source: &Path, branch: &str) -> git::Result<bool> {
    #[cfg(test)]
    if take_injected_branch_lookup_failure(repo_source) {
        return Err(git::GitError::Io(std::io::Error::other(
            "injected branch lookup failure",
        )));
    }
    git::branch_exists(repo_source, branch).await
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum InjectedWorktreeProbeFailure {
    Io,
    NotRepository,
}

#[cfg(test)]
fn injected_worktree_probe_failures() -> &'static std::sync::Mutex<
    std::collections::HashMap<std::path::PathBuf, InjectedWorktreeProbeFailure>,
> {
    static FAILURES: std::sync::OnceLock<
        std::sync::Mutex<
            std::collections::HashMap<std::path::PathBuf, InjectedWorktreeProbeFailure>,
        >,
    > = std::sync::OnceLock::new();
    FAILURES.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[cfg(test)]
fn inject_worktree_probe_failure(path: &Path, failure: InjectedWorktreeProbeFailure) {
    injected_worktree_probe_failures()
        .lock()
        .expect("worktree probe injection lock")
        .insert(path.to_path_buf(), failure);
}

#[cfg(test)]
fn take_injected_worktree_probe_failure(path: &Path) -> Option<InjectedWorktreeProbeFailure> {
    injected_worktree_probe_failures()
        .lock()
        .expect("worktree probe injection lock")
        .remove(path)
}

#[cfg(test)]
fn injected_branch_lookup_failures(
) -> &'static std::sync::Mutex<std::collections::HashSet<std::path::PathBuf>> {
    static FAILURES: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashSet<std::path::PathBuf>>,
    > = std::sync::OnceLock::new();
    FAILURES.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
}

#[cfg(test)]
fn inject_branch_lookup_failure(path: &Path) {
    injected_branch_lookup_failures()
        .lock()
        .expect("branch lookup injection lock")
        .insert(path.to_path_buf());
}

#[cfg(test)]
fn take_injected_branch_lookup_failure(path: &Path) -> bool {
    injected_branch_lookup_failures()
        .lock()
        .expect("branch lookup injection lock")
        .remove(path)
}

async fn try_repair_worktree_gitdir(repo_source: &Path, worktree_path: &Path) -> bool {
    let output = Command::new("git")
        .args(["worktree", "repair", &worktree_path.to_string_lossy()])
        .current_dir(repo_source)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .await;
    match output {
        Ok(output) if output.status.success() => git::get_current_sha(worktree_path).await.is_ok(),
        Ok(output) => {
            warn!(
                repo_source = %repo_source.display(),
                worktree_path = %worktree_path.display(),
                stderr = %String::from_utf8_lossy(&output.stderr).trim(),
                "git worktree repair did not repair workspace"
            );
            false
        }
        Err(error) => {
            warn!(
                repo_source = %repo_source.display(),
                worktree_path = %worktree_path.display(),
                %error,
                "failed to run git worktree repair"
            );
            false
        }
    }
}

async fn move_unusable_worktree_aside(worktree_path: &Path) -> Result<()> {
    if !worktree_path.exists() {
        return Ok(());
    }
    let parent = worktree_path.parent().ok_or_else(|| {
        ServiceError::invalid_operation(format!(
            "worktree path has no parent: {}",
            worktree_path.display()
        ))
    })?;
    let name = worktree_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("worktree");
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    let backup_path: PathBuf = parent.join(format!("{name}.broken-{millis}"));
    tokio::fs::rename(worktree_path, &backup_path)
        .await
        .map_err(|error| ServiceError::Git(git::GitError::Io(error)))?;
    warn!(
        worktree_path = %worktree_path.display(),
        backup_path = %backup_path.display(),
        "moved unusable worktree aside before recovery"
    );
    Ok(())
}

async fn create_fresh_workspace(
    db: &SqliteDb,
    workspace_root: &std::path::Path,
    repo: &db::Repo,
    task_id: &str,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
) -> Result<(Workspace, bool)> {
    let repo_id = repo.id.as_str();
    let now = now_rfc3339();
    let lock_key = repo
        .local_path
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty() && Path::new(path).exists())
        .map(str::to_owned)
        .unwrap_or_else(|| {
            workspace_root
                .join(".repos")
                .join(repo_id)
                .to_string_lossy()
                .into_owned()
        });
    // The branch-existence check, Git worktree mutation, and unique Workspace
    // insert form one per-repository critical section. Without this outer
    // lock, two concurrent claims can both observe a missing branch and one
    // leaks Git's raw "branch already exists" failure instead of losing the
    // normal Task-version claim race.
    let _repo_cache_guard = if let Some(locks) = &repo_cache_locks {
        Some(locks.acquire(&lock_key).await)
    } else {
        None
    };

    if let Some(workspace) = WorkspaceRepo::get_by_task_id(db, task_id).await? {
        return Ok((clear_workspace_cleanup_after(db, workspace).await?, false));
    }

    // The outer guard already owns the same key WorkspaceManager would use.
    let manager = WorkspaceManager::new(workspace_root.to_path_buf());
    let repo_cache_path = workspace_root.join(".repos").join(repo_id);
    let worktree_source = match resolve_repo_source(repo, workspace_root).await {
        Ok(source) => source,
        Err(error) => {
            cleanup_repo_cache_if_authority_gone(db, &repo.project_id, repo_id, &repo_cache_path)
                .await;
            return Err(error);
        }
    };
    let branch = ::workspace::task_branch_name(task_id);
    let branch_exists = match git::branch_exists(Path::new(&worktree_source), &branch).await {
        Ok(branch_exists) => branch_exists,
        Err(error) => {
            cleanup_repo_cache_if_authority_gone(db, &repo.project_id, repo_id, &repo_cache_path)
                .await;
            return Err(ServiceError::from(error));
        }
    };
    let worktree_result = if branch_exists {
        // A rejected admission may have removed its fresh workspace row and
        // directory after Git created the task branch. Recover that exact
        // task-scoped branch so a corrected retry remains possible and no
        // potentially useful work is discarded.
        manager
            .recover_worktree_named(&worktree_source, task_id, &repo.name, &branch)
            .await
    } else {
        manager
            .create_worktree_named(&worktree_source, task_id, &repo.name, &repo.default_branch)
            .await
    };
    let worktree_path = match worktree_result.map_err(map_workspace_error) {
        Ok(worktree_path) => worktree_path,
        Err(error) => {
            if let Err(cleanup_error) = manager
                .cleanup_worktree(
                    task_id,
                    Path::new(&worktree_source),
                    &workspace_root.join(task_id).join(&repo.name),
                )
                .await
            {
                tracing::error!(
                    task_id,
                    error = %cleanup_error,
                    "workspace creation failed and its worktree cleanup also failed"
                );
            }
            cleanup_repo_cache_if_authority_gone(db, &repo.project_id, repo_id, &repo_cache_path)
                .await;
            return Err(error);
        }
    };
    let before_sha = git::get_current_sha(&worktree_path).await.ok();
    let workspace_id = new_uuid_v4();
    let workspace = match persist_prepared_workspace(
        db,
        repo,
        &worktree_source,
        CreateWorkspace {
            id: workspace_id,
            task_id: task_id.to_owned(),
            repo_id: repo_id.to_owned(),
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch,
            status: WorkspaceStatus::Ready,
            before_sha,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    {
        Ok(workspace) => workspace,
        Err(error) => {
            // The final Project deletion transaction uses BEGIN IMMEDIATE. If
            // it acquired the writer lock first, this INSERT waits for the
            // task/project cascade and then fails its FK admission; no
            // Workspace row exists, so the worktree created above belongs to
            // this failed admission and must be cleaned. If another creator
            // committed first, the row lookup proves the worktree is theirs,
            // so never remove it. The deletion transaction collects rows that
            // win this race and performs a post-commit path cleanup for the
            // opposite ordering.
            if WorkspaceRepo::get_by_task_id(db, task_id)
                .await
                .ok()
                .flatten()
                .is_none()
            {
                if let Err(cleanup_error) = manager
                    .cleanup_worktree(
                        task_id,
                        Path::new(&worktree_source),
                        &workspace_root.join(task_id).join(&repo.name),
                    )
                    .await
                {
                    tracing::error!(
                        task_id,
                        error = %cleanup_error,
                        "workspace admission failed and its worktree cleanup also failed"
                    );
                }
                cleanup_repo_cache_if_authority_gone(
                    db,
                    &repo.project_id,
                    repo_id,
                    &repo_cache_path,
                )
                .await;
            }
            return Err(error);
        }
    };

    info!(
        task_id = task_id,
        workspace_id = %workspace.id,
        repo_id = repo_id,
        workspace_root = %workspace_root.display(),
        worktree_path = %worktree_path.display(),
        branch = %workspace.branch,
        "workspace created"
    );

    Ok((workspace, true))
}

async fn server_repo_location_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    repo: &db::Repo,
    source: &str,
    now: &str,
) -> db::Result<String> {
    let kind = if repo.local_path.as_deref().map(str::trim) == Some(source) {
        RepoLocationKind::PrimaryCheckout
    } else {
        RepoLocationKind::ManagedClone
    };
    let existing = sqlx::query(
        "SELECT id, status, version FROM repo_location
         WHERE repo_id = ? AND owner_kind = 'server' AND daemon_id IS NULL
           AND runtime_id IS NULL AND kind = ? AND path = ?
         ORDER BY created_at ASC, id ASC LIMIT 1",
    )
    .bind(&repo.id)
    .bind(kind.to_string())
    .bind(source)
    .fetch_optional(&mut **transaction)
    .await?;
    if let Some(row) = existing {
        let id: String = row.try_get("id")?;
        let status: String = row.try_get("status")?;
        if status != "ready" {
            let version: i64 = row.try_get("version")?;
            let result = sqlx::query(
                "UPDATE repo_location SET status = 'ready', last_verified_at = ?,
                 last_error = NULL, version = version + 1, updated_at = ?
                 WHERE id = ? AND version = ?",
            )
            .bind(now)
            .bind(now)
            .bind(&id)
            .bind(version)
            .execute(&mut **transaction)
            .await?;
            if result.rows_affected() == 0 {
                return Err(DbError::VersionConflict);
            }
        }
        return Ok(id);
    }

    let id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO repo_location (
            id, repo_id, owner_kind, path, kind, is_default, status,
            last_verified_at, created_at, updated_at
         ) VALUES (?, ?, 'server', ?, ?, ?, 'ready', ?, ?, ?)",
    )
    .bind(&id)
    .bind(&repo.id)
    .bind(source)
    .bind(kind.to_string())
    .bind(kind == RepoLocationKind::PrimaryCheckout)
    .bind(now)
    .bind(now)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    Ok(id)
}

fn server_placement(
    workspace: &CreateWorkspace,
    repo_location_id: String,
) -> CreateWorkspacePlacement {
    CreateWorkspacePlacement {
        id: new_uuid_v4(),
        workspace_id: workspace.id.clone(),
        task_id: workspace.task_id.clone(),
        agent_id: None,
        owner_kind: PlacementOwnerKind::Server,
        daemon_id: None,
        runtime_id: None,
        repo_location_id,
        execution_daemon_id: None,
        workspace_handle: Some(workspace.worktree_path.clone()),
        generation: 1,
        state: match workspace.status {
            WorkspaceStatus::Creating => PlacementState::Preparing,
            WorkspaceStatus::Ready => PlacementState::Ready,
            WorkspaceStatus::Error => PlacementState::Failed,
            WorkspaceStatus::Cleaning => PlacementState::Cleaning,
            WorkspaceStatus::Cleaned => PlacementState::Cleaned,
        },
        selected_by: PlacementSelectedBy::Scheduler,
        selection_reason: json!({ "rule": "server_default" }).to_string(),
        reserved_until: None,
        disconnected_at: None,
        failure_cause: None,
        created_at: workspace.created_at.clone(),
        updated_at: workspace.updated_at.clone(),
    }
}

/// The workspace and its server placement become visible together. The writer
/// lock also serializes first-use managed-clone location registration.
async fn persist_prepared_workspace(
    db: &SqliteDb,
    repo: &db::Repo,
    source: &str,
    input: CreateWorkspace,
) -> Result<Workspace> {
    let mut transaction = db::begin_immediate(db.pool()).await?;
    sqlx::query(
        "INSERT INTO workspace (
            id, task_id, repo_id, worktree_path, branch, status, before_sha, created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&input.id)
    .bind(&input.task_id)
    .bind(&input.repo_id)
    .bind(&input.worktree_path)
    .bind(&input.branch)
    .bind(input.status.to_string())
    .bind(input.before_sha.as_deref())
    .bind(&input.created_at)
    .bind(&input.updated_at)
    .execute(&mut *transaction)
    .await?;
    let location_id =
        server_repo_location_in_tx(&mut transaction, repo, source, &input.updated_at).await?;
    WorkspacePlacementRepo::create_in_tx(
        db,
        &mut transaction,
        server_placement(&input, location_id),
    )
    .await?;
    transaction.commit().await?;
    WorkspaceRepo::get_by_id(db, &input.id)
        .await?
        .ok_or_else(|| ServiceError::not_found("workspace", input.id))
}

async fn persist_recovered_workspace(
    db: &SqliteDb,
    repo: &db::Repo,
    source: &str,
    workspace: &Workspace,
    worktree_path: &Path,
    now: &str,
) -> Result<()> {
    let mut transaction = db::begin_immediate(db.pool()).await?;
    let placement =
        WorkspacePlacementRepo::get_by_workspace_id_in_tx(db, &mut transaction, &workspace.id)
            .await?;
    if let Some(placement) = placement {
        if placement.owner_kind != PlacementOwnerKind::Server {
            return Err(
                crate::workspace_backend::WorkspaceBackendError::OwnerUnsupported {
                    owner_kind: placement.owner_kind,
                }
                .into(),
            );
        }
        WorkspacePlacementRepo::update_in_tx(
            db,
            &mut transaction,
            UpdateWorkspacePlacement {
                id: placement.id,
                expected_version: placement.version,
                agent_id: None,
                owner_kind: None,
                daemon_id: None,
                runtime_id: None,
                repo_location_id: None,
                execution_daemon_id: None,
                workspace_handle: Some(Some(worktree_path.to_string_lossy().into_owned())),
                generation: Some(placement.generation + 1),
                state: Some(PlacementState::Ready),
                selected_by: None,
                selection_reason: None,
                reserved_until: Some(None),
                disconnected_at: Some(None),
                failure_cause: Some(None),
                updated_at: now.to_owned(),
            },
        )
        .await?;
    } else {
        // Migration skips already-cleaned workspaces. Rebuilding one restores
        // its placement in the same transaction as the ready Workspace row.
        let location_id = server_repo_location_in_tx(&mut transaction, repo, source, now).await?;
        let input = CreateWorkspace {
            id: workspace.id.clone(),
            task_id: workspace.task_id.clone(),
            repo_id: workspace.repo_id.clone(),
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch: workspace.branch.clone(),
            status: WorkspaceStatus::Ready,
            before_sha: workspace.before_sha.clone(),
            created_at: workspace.created_at.clone(),
            updated_at: now.to_owned(),
        };
        WorkspacePlacementRepo::create_in_tx(
            db,
            &mut transaction,
            server_placement(&input, location_id),
        )
        .await?;
    }
    let result = sqlx::query(
        "UPDATE workspace SET status = 'ready', worktree_path = ?, error = NULL, updated_at = ?
         WHERE id = ?",
    )
    .bind(worktree_path.to_string_lossy().as_ref())
    .bind(now)
    .bind(&workspace.id)
    .execute(&mut *transaction)
    .await?;
    if result.rows_affected() == 0 {
        return Err(DbError::NotFound.into());
    }
    transaction.commit().await?;
    Ok(())
}

/// Reusing a workspace revives its shared delivery branch. Any cleanup
/// deadline left by a previously terminal child is stale and must be cleared
/// before a new execution can rely on the worktree.
async fn clear_workspace_cleanup_after(db: &SqliteDb, workspace: Workspace) -> Result<Workspace> {
    if workspace.cleanup_after.is_none() {
        return Ok(workspace);
    }
    Ok(WorkspaceRepo::set_cleanup_after(db, &workspace.id, None, &now_rfc3339()).await?)
}

pub(super) async fn reset_daemon_workspace(
    db: &SqliteDb,
    workspace: &Workspace,
    resolved: &crate::workspace_backend::ResolvedWorkspace,
) -> Result<Workspace> {
    use crate::workspace_backend::{PreparedWorkspace, ResetSpec, WorkspaceBackendError};
    let placement = &resolved.placement;
    let running: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM execution WHERE workspace_id = ? AND status = 'running')",
    )
    .bind(&workspace.id)
    .fetch_one(db.pool())
    .await?;
    if running {
        return Err(ServiceError::invalid_operation(
            "cannot reset an owner workspace with a running execution",
        ));
    }
    let base_sha = workspace
        .before_sha
        .as_deref()
        .ok_or_else(|| ServiceError::invalid_operation("workspace has no recorded reset base"))?;
    let generation = placement
        .generation
        .checked_add(1)
        .ok_or_else(|| ServiceError::invalid_operation("placement generation exhausted"))?;
    // A receipt can survive the owner reset while the placement CAS did
    // not. Apply that result before issuing any read with the old fence.
    let retained = sqlx::query_scalar::<_, String>(
        "SELECT outcome_json FROM command_receipt WHERE operation = 'daemon.workspace.reset'
         AND principal_id = 'workspace-backend'
         AND json_extract(outcome_json, '$.metadata.placement_id') = ?
         AND json_extract(outcome_json, '$.metadata.generation') = ?
         AND json_extract(outcome_json, '$.metadata.status') = 'result'
         ORDER BY committed_at DESC, id DESC LIMIT 1",
    )
    .bind(&placement.id)
    .bind(generation)
    .fetch_optional(db.pool())
    .await?;
    let prepared = if let Some(retained) = retained {
        let retained: serde_json::Value = serde_json::from_str(&retained).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid reset receipt: {error}"))
        })?;
        let result: api_types::WorkspaceResetResult =
            serde_json::from_value(retained["owner_result"].clone()).map_err(|error| {
                ServiceError::invalid_operation(format!("invalid reset result: {error}"))
            })?;
        if i64::try_from(result.workspace.generation).ok() != Some(generation) {
            return Err(WorkspaceBackendError::StaleGeneration {
                placement_id: placement.id.clone(),
                expected: generation,
                actual: i64::try_from(result.workspace.generation).unwrap_or(i64::MAX),
            }
            .into());
        }
        PreparedWorkspace {
            handle: result.workspace.workspace_handle,
            base_sha: result.workspace.base_sha,
            branch: result.workspace.branch,
        }
    } else {
        let state = resolved.backend.describe(placement).await?;
        let head_sha = if state.exists {
            state
                .head_sha
                .filter(|sha| !sha.is_empty())
                .ok_or_else(|| ServiceError::invalid_operation("owner returned no reset HEAD"))?
        } else {
            base_sha.to_owned()
        };
        let mut next = placement.clone();
        next.generation = generation;
        resolved
            .backend
            .reset(
                &next,
                &ResetSpec {
                    expected_head_sha: head_sha,
                    base_ref: base_sha.to_owned(),
                },
            )
            .await?
    };
    if prepared.handle.is_empty() || prepared.base_sha.is_empty() || prepared.branch.is_empty() {
        return Err(ServiceError::invalid_operation(
            "owner returned an incomplete reset workspace",
        ));
    }
    let mut transaction = db::begin_immediate(db.pool()).await?;
    let running: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM execution WHERE workspace_id = ? AND status = 'running')",
    )
    .bind(&workspace.id)
    .fetch_one(&mut *transaction)
    .await?;
    if running {
        return Err(db::DbError::VersionConflict.into());
    }
    let mut update = crate::placement::admission::placement_update(placement);
    update.generation = Some(generation);
    update.workspace_handle = Some(Some(prepared.handle));
    update.state = Some(db::PlacementState::Ready);
    update.disconnected_at = Some(None);
    update.failure_cause = Some(None);
    db::WorkspacePlacementRepo::update_in_tx(db, &mut transaction, update).await?;
    sqlx::query("UPDATE workspace SET status = 'ready', before_sha = ?, branch = ?, cleanup_after = NULL,
            cleanup_attempts = 0, last_cleanup_error = NULL, error = NULL, updated_at = ? WHERE id = ?")
        .bind(prepared.base_sha).bind(prepared.branch).bind(now_rfc3339()).bind(&workspace.id)
        .execute(&mut *transaction).await?;
    crate::placement::admission::resolve_workspace_attention_in_tx(
        &mut transaction,
        &placement.task_id,
    )
    .await?;
    transaction.commit().await?;
    WorkspaceRepo::get_by_id(db, &workspace.id)
        .await?
        .ok_or_else(|| ServiceError::not_found("workspace", &workspace.id))
}

pub(super) async fn reset_workspace(
    db: &SqliteDb,
    workspace_root: &std::path::Path,
    task: &Task,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
    router: &crate::workspace_backend::WorkspaceBackendRouter,
) -> Result<Workspace> {
    if task.parent_task_id.is_some() {
        return Err(ServiceError::invalid_operation(
            "subtask workspaces are shared with the coordination root; reset the root workspace instead",
        ));
    }
    let authority = resolve_task_repository_authority(db, task).await?;
    let mut daemon_reset = None;
    if let Some(workspace) = WorkspaceRepo::get_by_task_id(db, &task.id).await? {
        let resolved = resolve_workspace_backend(db, workspace_root, &workspace, router).await?;
        if resolved.placement.owner_kind == PlacementOwnerKind::Daemon {
            if workspace.repo_id != authority.repo.id {
                return Err(ServiceError::invalid_operation(
                    "cannot reset a daemon placement onto a different repository",
                ));
            }
            daemon_reset = Some(reset_daemon_workspace(db, &workspace, &resolved).await?);
        } else {
            // Cleanup follows the attempt-pinned Workspace repository. The fresh
            // replacement below follows the Project's current primary Repo.
            let repo = RepoRepo::get_by_id(db, &workspace.repo_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("repo", workspace.repo_id.clone()))?;
            let worktree_path = resolved.embedded_path()?;
            let repo_source = resolve_repo_source(&repo, workspace_root).await?;
            if worktree_path.exists() {
                let _ = resolved.backend.cleanup(&resolved.placement).await;
            }
            // Prune stale git worktree references
            let _ = tokio::process::Command::new("git")
                .args(["worktree", "prune"])
                .current_dir(&repo_source)
                .env_remove("GIT_DIR")
                .env_remove("GIT_WORK_TREE")
                .env_remove("GIT_INDEX_FILE")
                .output()
                .await;
            WorkspaceRepo::delete(db, &workspace.id).await?;
            info!(
                task_id = %task.id,
                workspace_id = %workspace.id,
                "old workspace deleted for reset"
            );
        }
    }

    // Clear error annotation
    db::TaskRepo::update(
        db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(None),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: db::now_rfc3339(),
        },
    )
    .await?;

    crate::placement::admission::resolve_workspace_attention(db, &task.id).await?;
    if let Some(workspace) = daemon_reset {
        return Ok(workspace);
    }

    let refreshed = db::TaskRepo::get_by_id(db, &task.id, false)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;

    let (workspace, _) = create_fresh_workspace(
        db,
        workspace_root,
        &authority.repo,
        &refreshed.id,
        repo_cache_locks,
    )
    .await?;
    Ok(workspace)
}

pub(crate) fn default_workspace_root() -> PathBuf {
    std::env::var("FORGE_WORKSPACE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("forge").join("worktrees"))
}

/// The account-owned scratch root, holding one directory per Main Agent
/// account. It sits beside the Project Agent workspaces rather than inside
/// any of them: nothing here belongs to a Project.
pub const MAIN_AGENT_WORKSPACES_DIR: &str = "main-agents";
/// The Main Agent's own durable notes, inside its scratch directory.
pub const MAIN_AGENT_NOTES_DIR: &str = "notes";
/// The parent of every ephemeral inquiry's private directory. It lives
/// inside the Main Agent's scratch root on purpose: the dispatcher can read
/// a sub-agent's findings file without the sub-agent's own directory ever
/// being writable by anything else.
pub const MAIN_AGENT_INQUIRIES_DIR: &str = "inquiries";

/// Ensure the Main Agent's scratch directory.
///
/// This is deliberately not a repository: no clone, no worktree, no remote,
/// nothing to push. It exists so the Main Agent and the ephemeral inquiries
/// it dispatches can keep findings and working notes on disk instead of
/// carrying them in a conversation that has to survive all day. Because no
/// repository is ever placed here, "cannot write to a repository" holds
/// because there is none, not because a tool was withheld.
pub async fn ensure_main_agent_workspace(
    workspaces_root: &Path,
    account_id: &str,
) -> Result<PathBuf> {
    let workspaces_root = std::fs::canonicalize(workspaces_root).unwrap_or_else(|_| {
        std::env::current_dir()
            .map(|cwd| cwd.join(workspaces_root))
            .unwrap_or_else(|_| workspaces_root.to_path_buf())
    });
    let workspace = workspaces_root
        .join(MAIN_AGENT_WORKSPACES_DIR)
        .join(account_id);
    std::fs::create_dir_all(workspace.join(MAIN_AGENT_NOTES_DIR)).map_err(|error| {
        ServiceError::invalid_operation(format!("Main Agent workspace is unavailable: {error}"))
    })?;
    std::fs::create_dir_all(workspace.join(MAIN_AGENT_INQUIRIES_DIR)).map_err(|error| {
        ServiceError::invalid_operation(format!("Main Agent workspace is unavailable: {error}"))
    })?;
    write_scratch_workspace_boundary(&workspace)?;
    Ok(workspace)
}

/// Ensure one inquiry's private directory inside the dispatching account's
/// scratch root. Each inquiry gets its own so that concurrent sub-agents
/// cannot overwrite each other's findings.
pub async fn ensure_inquiry_workspace(
    workspaces_root: &Path,
    account_id: &str,
    inquiry_id: &str,
) -> Result<PathBuf> {
    let workspace = ensure_main_agent_workspace(workspaces_root, account_id).await?;
    let inquiry = workspace.join(MAIN_AGENT_INQUIRIES_DIR).join(inquiry_id);
    std::fs::create_dir_all(&inquiry).map_err(|error| {
        ServiceError::invalid_operation(format!("inquiry workspace is unavailable: {error}"))
    })?;
    Ok(inquiry)
}

/// The same upward-search guard the verification workspace uses. A scratch
/// directory lives inside the Forge data directory, which on a development
/// server sits inside Forge's own repository, so a `cargo` invocation here
/// would otherwise climb into Forge's workspace and be refused.
fn write_scratch_workspace_boundary(workspace: &Path) -> Result<()> {
    let manifest = workspace.join("Cargo.toml");
    if manifest.exists() {
        return Ok(());
    }
    std::fs::write(
        &manifest,
        "# Forge Main Agent scratch boundary.\n\
         # Cargo searches upward for a workspace root; this manifest is that root, so\n\
         # a build here never resolves against whatever repository holds this data\n\
         # directory. Nothing in this tree is a repository.\n\
         [workspace]\n\
         members = []\n\
         resolver = \"2\"\n",
    )
    .map_err(|error| {
        ServiceError::invalid_operation(format!(
            "Main Agent workspace boundary could not be written: {error}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{create_sqlite_pool, run_migrations, CreateProject, CreateRepo, UpdateProject};
    use tempfile::TempDir;

    async fn sqlite_db() -> SqliteDb {
        let pool = create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        run_migrations(&pool).await.expect("migrations run");
        SqliteDb::new(pool)
    }

    async fn seed_project_repo(db: &SqliteDb) -> (String, String) {
        let now = now_rfc3339();
        let project_id = new_uuid_v4();
        let repo_id = new_uuid_v4();
        ProjectRepo::create(
            db,
            CreateProject {
                id: project_id.clone(),
                name: "Forge".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project creates");
        RepoRepo::create(
            db,
            CreateRepo {
                id: repo_id.clone(),
                project_id: project_id.clone(),
                name: "repo".to_owned(),
                remote_url: Some("/tmp/repo".to_owned()),
                local_path: Some("/tmp/repo".to_owned()),
                work_mode: db::WorkMode::DirectMerge,
                default_branch: "main".to_owned(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("repo creates");
        ProjectRepo::update_at_version(
            db,
            UpdateProject {
                id: project_id.clone(),
                name: None,
                settings: None,
                primary_repo_id: Some(Some(repo_id.clone())),
                paused_at: None,
                updated_at: now_rfc3339(),
            },
            ProjectRepo::get_by_id(db, &project_id)
                .await
                .expect("fixture Project lookup")
                .expect("fixture Project exists")
                .version,
            None,
        )
        .await
        .expect("project primary repo updates");
        (project_id, repo_id)
    }

    async fn seed_task(db: &SqliteDb, project_id: &str, parent_task_id: Option<String>) -> Task {
        let now = now_rfc3339();
        TaskRepo::create(
            db,
            CreateTask {
                id: new_uuid_v4(),
                project_id: project_id.to_owned(),
                parent_task_id,
                subtask_order: None,
                assignee_type: None,
                assignee_id: None,
                title: "task".to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: "todo".to_owned(),
                is_automation: false,
                priority: 0,
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("task creates")
    }

    fn init_test_checkout(path: &std::path::Path, branch: &str) {
        let run = |args: &[&str]| {
            let output = std::process::Command::new("git")
                .args(args)
                .current_dir(path)
                .env_remove("GIT_DIR")
                .env_remove("GIT_WORK_TREE")
                .env_remove("GIT_INDEX_FILE")
                .output()
                .expect("git command runs");
            assert!(
                output.status.success(),
                "git {} failed: {}",
                args.join(" "),
                String::from_utf8_lossy(&output.stderr)
            );
        };
        run(&["init", "-b", branch]);
        run(&["config", "user.email", "test@forge.dev"]);
        run(&["config", "user.name", "Forge Test"]);
        std::fs::write(path.join("README.md"), "# Test\n").expect("write README");
        run(&["add", "-A"]);
        run(&["commit", "-m", "initial"]);
    }

    async fn seed_workspace(
        db: &SqliteDb,
        task: &Task,
        status: WorkspaceStatus,
        worktree_dir: &std::path::Path,
    ) -> Workspace {
        let worktree_path = worktree_dir.join(&task.id);
        std::fs::create_dir_all(&worktree_path).expect("worktree dir creates");
        let branch = ::workspace::task_branch_name(&task.id);
        init_test_checkout(&worktree_path, &branch);
        let now = now_rfc3339();
        let repo_id = ProjectRepo::get_by_id(db, &task.project_id)
            .await
            .expect("Project loads")
            .expect("Project exists")
            .primary_repo_id
            .expect("Project has a primary Repo");
        let repo = RepoRepo::get_by_id(db, &repo_id)
            .await
            .expect("Repo loads")
            .expect("Repo exists");
        persist_prepared_workspace(
            db,
            &repo,
            repo.local_path.as_deref().expect("fixture checkout"),
            CreateWorkspace {
                id: new_uuid_v4(),
                task_id: task.id.clone(),
                repo_id,
                worktree_path: worktree_path.to_string_lossy().into_owned(),
                branch,
                status,
                before_sha: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("workspace creates")
    }

    async fn execution_count(db: &SqliteDb, task_id: &str) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM execution WHERE task_id = ?")
            .bind(task_id)
            .fetch_one(db.pool())
            .await
            .unwrap()
    }

    async fn seed_unpinned_claim_agent(db: &SqliteDb) -> Agent {
        AgentRepo::create(
            db,
            db::CreateAgent {
                id: new_uuid_v4(),
                name: "claim-agent".to_owned(),
                description: None,
                executor_type: "shell".to_owned(),
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "[]".to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id: None,
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: db::AgentStatus::Idle,
                last_heartbeat_at: None,
                is_default: false,
                paused: false,
                owner_id: None,
                visibility: "global".to_owned(),
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn placement_child_uses_effective_coder_and_keeps_root_workspace_roles() {
        let db = Arc::new(sqlite_db().await);
        let repo = TempDir::new().unwrap();
        let root = TempDir::new().unwrap();
        let (project_id, _) = seed_project_with_real_repo(&db, repo.path()).await;
        let parent = seed_task(&db, &project_id, None).await;
        let child = seed_task(&db, &project_id, Some(parent.id.clone())).await;
        let coder = seed_unpinned_claim_agent(&db).await;
        let reviewer = seed_unpinned_claim_agent(&db).await;
        // The root reviewer will use this same workspace after its children.
        sqlx::query("UPDATE agent_identity SET paused = 1 WHERE id = ?")
            .bind(&reviewer.id)
            .execute(db.pool())
            .await
            .unwrap();
        for (role, agent) in [("coder", &coder), ("reviewer", &reviewer)] {
            TaskRoleAssignmentRepo::assign(
                &*db,
                db::CreateTaskRoleAssignment {
                    id: new_uuid_v4(),
                    task_id: parent.id.clone(),
                    role_name: role.into(),
                    assignee_type: Some(AssigneeKind::Agent),
                    assignee_id: Some(agent.id.clone()),
                    created_at: now_rfc3339(),
                    updated_at: now_rfc3339(),
                },
            )
            .await
            .unwrap();
        }
        let effective = crate::task_hierarchy::effective_coder_assignment(&db, &child)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            effective.assignment.assignee_id.as_deref(),
            Some(coder.id.as_str())
        );
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_workspace_root(root.path().to_path_buf());
        assert!(service
            .reserve_claim_workspace(&child, Some(&coder), "coder")
            .await
            .is_err());
        sqlx::query("UPDATE agent_identity SET paused = 0 WHERE id = ?")
            .bind(&reviewer.id)
            .execute(db.pool())
            .await
            .unwrap();
        let admission = service
            .reserve_claim_workspace(&child, Some(&coder), "coder")
            .await
            .unwrap();
        assert_eq!(
            admission.placement.agent_id.as_deref(),
            Some(coder.id.as_str())
        );
        assert!(TaskRoleAssignmentRepo::list_by_task(&*db, &child.id)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn placement_migrated_remote_only_repo_first_claim_verifies_clone() {
        assert_migrated_clone_claim(false).await;
    }

    #[tokio::test]
    async fn placement_migrated_worktree_location_replaces_invalid_path_on_claim() {
        assert_migrated_clone_claim(true).await;
    }

    async fn assert_migrated_clone_claim(nonstandard: bool) {
        let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
        let migrations = Path::new(env!("CARGO_MANIFEST_DIR")).join("../db/migrations");
        let mut historical = std::fs::read_dir(migrations)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter_map(|path| {
                let name = path.file_name()?.to_str()?;
                let version: i64 = name.strip_prefix('V')?.split_once("__")?.0.parse().ok()?;
                (!matches!(version, 202610010400 | 202610010530)).then_some((version, path))
            })
            .collect::<Vec<_>>();
        historical.sort_by_key(|(version, _)| *version);
        for (_, path) in historical {
            sqlx::raw_sql(&std::fs::read_to_string(path).unwrap())
                .execute(&pool)
                .await
                .unwrap();
        }
        let db = Arc::new(SqliteDb::new(pool));
        let remote = TempDir::new().unwrap();
        let root = TempDir::new().unwrap();
        let (project_id, repo_id) = seed_project_with_real_repo(&db, remote.path()).await;
        let task = seed_task(&db, &project_id, None).await;
        let agent = seed_unpinned_claim_agent(&db).await;
        let daemon = db::DaemonRepo::upsert_by_machine_id(
            &*db,
            db::UpsertDaemon {
                id: new_uuid_v4(),
                machine_id: crate::embedded_daemon::embedded_machine_id(),
                hostname: "server".into(),
                os: "linux".into(),
                arch: "x86_64".into(),
                agent_version: None,
                labels_json: "{}".into(),
                status: db::DaemonStatus::Online,
                registration_token_hash: None,
                owner_id: None,
                visibility: "global".into(),
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
        db::DaemonRepo::update_report(
            &*db,
            db::UpdateDaemonReport {
                id: daemon.id,
                detected_clis_json: r#"[{"kind":"shell","availability":"authenticated"}]"#.into(),
                labels_json: None,
                status: db::DaemonStatus::Online,
                last_report_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();

        let base = git::get_current_sha(remote.path()).await.unwrap();
        // Legacy remote-only repository and a ready task worktree. Its clone
        // cache has disappeared, so first use must clone before selection.
        sqlx::query("UPDATE repo SET local_path = NULL WHERE id = ?")
            .bind(&repo_id)
            .execute(db.pool())
            .await
            .unwrap();
        let path = if nonstandard {
            let path = root.path().join("legacy-worktree");
            git::create_worktree(
                remote.path(),
                &::workspace::task_branch_name(&task.id),
                &path,
            )
            .await
            .unwrap();
            path
        } else {
            ::workspace::WorkspaceManager::new(root.path().to_path_buf())
                .create_worktree_named(remote.path().to_str().unwrap(), &task.id, "repo", "main")
                .await
                .unwrap()
        };
        WorkspaceRepo::create(
            &*db,
            CreateWorkspace {
                id: new_uuid_v4(),
                task_id: task.id.clone(),
                repo_id: repo_id.clone(),
                worktree_path: path.to_string_lossy().into_owned(),
                branch: ::workspace::task_branch_name(&task.id),
                status: WorkspaceStatus::Ready,
                before_sha: Some(base),
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
        sqlx::raw_sql(include_str!(
            "../../../db/migrations/V202610010400__daemon_owned_workspaces.sql"
        ))
        .execute(db.pool())
        .await
        .unwrap();
        let status: String =
            sqlx::query_scalar("SELECT status FROM repo_location WHERE repo_id = ?")
                .bind(&repo_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(status, "unverified");
        if nonstandard {
            sqlx::query("UPDATE repo_location SET status = 'invalid', last_error = 'managed_clone_is_worktree' WHERE repo_id = ?")
                .bind(&repo_id).execute(db.pool()).await.unwrap();
        }
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_workspace_root(root.path().to_path_buf());
        let claimed = service
            .claim_task(&task.id, Assignee::Agent(agent.id), None)
            .await
            .unwrap();
        assert_eq!(claimed.task.status, "in_progress");
        let location_path: String =
            sqlx::query_scalar("SELECT path FROM repo_location WHERE repo_id = ?")
                .bind(&repo_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(
            location_path,
            root.path().join(".repos").join(&repo_id).to_string_lossy()
        );
        let status: String =
            sqlx::query_scalar("SELECT status FROM repo_location WHERE repo_id = ?")
                .bind(&repo_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(status, "ready");
        assert!(root.path().join(".repos").join(repo_id).is_dir());
        assert!(path.is_dir());
    }

    #[tokio::test]
    async fn placement_remote_clone_failure_is_recorded_and_backed_off() {
        let db = Arc::new(sqlite_db().await);
        let remote = TempDir::new().unwrap();
        let root = TempDir::new().unwrap();
        let (project_id, repo_id) = seed_project_with_real_repo(&db, remote.path()).await;
        let task = seed_task(&db, &project_id, None).await;
        let agent = seed_unpinned_claim_agent(&db).await;
        let missing = root.path().join("missing-remote");
        sqlx::query("UPDATE repo SET local_path = NULL, remote_url = ? WHERE id = ?")
            .bind(missing.to_str().unwrap())
            .bind(&repo_id)
            .execute(db.pool())
            .await
            .unwrap();
        let location = db::RepoLocationRepo::create(
            &*db,
            db::CreateRepoLocation {
                id: new_uuid_v4(),
                repo_id: repo_id.clone(),
                owner_kind: db::RepoLocationOwnerKind::Server,
                daemon_id: None,
                runtime_id: None,
                path: root
                    .path()
                    .join(".repos")
                    .join(&repo_id)
                    .to_string_lossy()
                    .into_owned(),
                kind: RepoLocationKind::ManagedClone,
                is_default: true,
                status: db::RepoLocationStatus::Unverified,
                last_verified_at: None,
                last_error: None,
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
        let locks = Arc::new(RepoCacheLockManager::new());
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_workspace_root(root.path().to_path_buf())
            .with_repo_cache_locks(locks.clone());
        assert!(service
            .reserve_claim_workspace(&task, Some(&agent), "coder")
            .await
            .is_err());
        let failed = db::RepoLocationRepo::get_by_id(&*db, &location.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failed.status, db::RepoLocationStatus::Unavailable);
        let error: Value = serde_json::from_str(failed.last_error.as_deref().unwrap()).unwrap();
        assert_eq!(error["cause"], "clone_failed");
        assert_eq!(error["attempts"], 1);
        assert!(
            DateTime::parse_from_rfc3339(error["retry_at"].as_str().unwrap()).unwrap() > Utc::now()
        );
        let _source_lock = locks.acquire(&location.path).await;
        assert!(service
            .reserve_claim_workspace(&task, Some(&agent), "coder")
            .await
            .is_err());
        let deferred = db::RepoLocationRepo::get_by_id(&*db, &location.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            deferred.version, failed.version,
            "backoff must not retry the clone on every scan"
        );
        assert_eq!(deferred.last_error, failed.last_error);
    }

    #[tokio::test]
    async fn placement_claim_recreates_deleted_ready_worktree() {
        let db = Arc::new(sqlite_db().await);
        let repo = TempDir::new().unwrap();
        let root = TempDir::new().unwrap();
        let (project_id, _) = seed_project_with_real_repo(&db, repo.path()).await;
        let task = seed_task(&db, &project_id, None).await;
        let agent = seed_unpinned_claim_agent(&db).await;
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_workspace_root(root.path().to_path_buf());
        let admission = service
            .reserve_claim_workspace(&task, Some(&agent), "coder")
            .await
            .unwrap();
        let prepared = service.prepare_claim_workspace(admission).await.unwrap();
        let original = prepared.placement.clone();
        let sha = git::get_current_sha(Path::new(original.workspace_handle.as_deref().unwrap()))
            .await
            .unwrap();
        std::fs::remove_dir_all(original.workspace_handle.as_deref().unwrap()).unwrap();
        let admission = service
            .reserve_claim_workspace(&task, Some(&agent), "coder")
            .await
            .unwrap();
        let recovered = service.prepare_claim_workspace(admission).await.unwrap();
        assert_eq!(recovered.workspace.id, prepared.workspace.id);
        assert_eq!(recovered.workspace.branch, prepared.workspace.branch);
        assert_eq!(recovered.placement.generation, original.generation + 1);
        assert_eq!(
            git::get_current_sha(Path::new(
                recovered.placement.workspace_handle.as_deref().unwrap()
            ))
            .await
            .unwrap(),
            sha
        );
    }

    #[tokio::test]
    async fn placement_claim_recreates_invalid_ready_worktree() {
        let db = Arc::new(sqlite_db().await);
        let repo = TempDir::new().unwrap();
        let root = TempDir::new().unwrap();
        let (project_id, _) = seed_project_with_real_repo(&db, repo.path()).await;
        let task = seed_task(&db, &project_id, None).await;
        let agent = seed_unpinned_claim_agent(&db).await;
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_workspace_root(root.path().to_path_buf());
        let admission = service
            .reserve_claim_workspace(&task, Some(&agent), "coder")
            .await
            .unwrap();
        let prepared = service.prepare_claim_workspace(admission).await.unwrap();
        let original = prepared.placement.clone();
        let sha = git::get_current_sha(Path::new(original.workspace_handle.as_deref().unwrap()))
            .await
            .unwrap();
        std::fs::remove_file(Path::new(original.workspace_handle.as_deref().unwrap()).join(".git"))
            .unwrap();
        let admission = service
            .reserve_claim_workspace(&task, Some(&agent), "coder")
            .await
            .unwrap();
        let recovered = service.prepare_claim_workspace(admission).await.unwrap();
        assert_eq!(recovered.workspace.id, prepared.workspace.id);
        assert_eq!(recovered.workspace.branch, prepared.workspace.branch);
        assert_eq!(recovered.placement.generation, original.generation + 1);
        assert_eq!(
            git::get_current_sha(Path::new(
                recovered.placement.workspace_handle.as_deref().unwrap()
            ))
            .await
            .unwrap(),
            sha
        );
    }

    #[tokio::test]
    async fn placement_admission_reservation_is_durable_before_prepare() {
        let db = Arc::new(sqlite_db().await);
        let repo_dir = TempDir::new().unwrap();
        let workspace_root = TempDir::new().unwrap();
        let (project_id, _) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let task = seed_task(&db, &project_id, None).await;
        let agent = seed_unpinned_claim_agent(&db).await;
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_workspace_root(workspace_root.path().to_path_buf());
        let admission = service
            .reserve_claim_workspace(&task, Some(&agent), "coder")
            .await
            .unwrap();
        let stored = WorkspacePlacementRepo::get_by_id(&*db, &admission.placement.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, PlacementState::Reserved);
        assert!(stored.workspace_handle.is_none());
        assert!(!workspace_root.path().join(&task.id).join("repo").exists());
        assert!(execution_count(&db, &task.id).await == 0);
        assert_eq!(
            TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .unwrap()
                .unwrap(),
            task
        );
        let prepared = service.prepare_claim_workspace(admission).await.unwrap();
        assert_eq!(prepared.placement.state, PlacementState::Ready);
        assert!(
            std::path::Path::new(prepared.placement.workspace_handle.as_deref().unwrap()).exists()
        );
        assert!(execution_count(&db, &task.id).await == 0);
    }

    #[tokio::test]
    async fn placement_admission_embedded_pin_binds_an_existing_server_workspace() {
        let db = Arc::new(sqlite_db().await);
        let repo_dir = TempDir::new().unwrap();
        let workspace_root = TempDir::new().unwrap();
        let (project_id, _) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let task = seed_task(&db, &project_id, None).await;
        let agent = seed_unpinned_claim_agent(&db).await;
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_workspace_root(workspace_root.path().to_path_buf());
        let admission = service
            .reserve_claim_workspace(&task, Some(&agent), "coder")
            .await
            .unwrap();
        let prepared = service.prepare_claim_workspace(admission).await.unwrap();
        assert!(prepared.placement.execution_daemon_id.is_none());

        let now = now_rfc3339();
        let daemon = db::DaemonRepo::upsert_by_machine_id(
            &*db,
            db::UpsertDaemon {
                id: new_uuid_v4(),
                machine_id: crate::embedded_daemon::embedded_machine_id(),
                hostname: "server".into(),
                os: std::env::consts::OS.into(),
                arch: std::env::consts::ARCH.into(),
                agent_version: None,
                labels_json: "{}".into(),
                status: db::DaemonStatus::Online,
                registration_token_hash: None,
                owner_id: None,
                visibility: "global".into(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
        let pinned = AgentRepo::update(
            &*db,
            db::UpdateAgent {
                id: agent.id.clone(),
                expected_version: agent.version,
                name: None,
                description: None,
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: None,
                config_json: None,
                daemon_id: Some(Some(daemon.id.clone())),
                max_concurrent_tasks: None,
                heartbeat_interval_seconds: None,
                max_missed_heartbeats: None,
                status: None,
                last_heartbeat_at: None,
                is_default: None,
                paused: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
        let admission = service
            .reserve_claim_workspace(&task, Some(&pinned), "coder")
            .await
            .unwrap();
        assert_eq!(admission.placement.id, prepared.placement.id);
        assert_eq!(admission.placement.owner_kind, PlacementOwnerKind::Server);
        assert_eq!(
            admission.placement.workspace_handle,
            prepared.placement.workspace_handle
        );
        assert_eq!(
            admission.placement.execution_daemon_id.as_deref(),
            Some(daemon.id.as_str())
        );
        let admission = service.prepare_claim_workspace(admission).await.unwrap();
        let mut transaction = db::begin_immediate(db.pool()).await.unwrap();
        service
            .check_claim_placement_in_tx(&mut transaction, &task, &admission)
            .await
            .unwrap();
        transaction.rollback().await.unwrap();
        assert_eq!(execution_count(&db, &task.id).await, 0);
    }

    #[tokio::test]
    async fn revision_two_only_owner_waits_without_execution_with_upgrade_reason() {
        let db = Arc::new(sqlite_db().await);
        let (task, placement, execution) = crate::recovery::tests::daemon_owned_fixture(&db).await;
        sqlx::query("DELETE FROM execution WHERE id = ?")
            .bind(&execution.id)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM workspace_placement WHERE id = ?")
            .bind(&placement.id)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM workspace WHERE id = ?")
            .bind(&placement.workspace_id)
            .execute(db.pool())
            .await
            .unwrap();
        let agent = AgentRepo::get_by_id(&*db, placement.agent_id.as_deref().unwrap())
            .await
            .unwrap()
            .unwrap();
        let daemon_id = placement.daemon_id.as_deref().unwrap();
        sqlx::query("UPDATE daemon SET detected_clis_json = ? WHERE id = ?")
            .bind(r#"[{"kind":"shell","availability":"authenticated"}]"#)
            .bind(daemon_id)
            .execute(db.pool())
            .await
            .unwrap();
        let registry =
            Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
        let (connection, _outbound) =
            crate::daemon_transport::DaemonConnection::new(daemon_id.into());
        let id = connection.id();
        registry.register(daemon_id.into(), connection);
        registry.dispatch_incoming_for_connection(daemon_id, id, api_types::DaemonFrame::Notification {
            method: api_types::METHOD_DAEMON_HANDSHAKE.into(),
            params: json!({"protocol_revision":2,"capabilities":["execution.terminal.usage_reports","execution.terminal.ack"]}),
        });
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_daemon_connections(registry.clone());
        let error = service
            .reserve_claim_workspace(&task, Some(&agent), "coder")
            .await
            .err()
            .expect("revision two is refused");
        let ServiceError::PlacementUnavailable(ref refusal) = error else {
            panic!("placement refusal");
        };
        assert!(refusal.needs_daemon_upgrade(), "{refusal:?}");
        assert!(!crate::placement::is_retryable_admission_refusal(&error));
        assert_eq!(execution_count(&db, &task.id).await, 0);
        let stored = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.status, task.status);
        assert!(stored.failed_json.is_none());
        assert!(stored.error_annotation.is_none());
        crate::workflow::engine::annotate_upgrade_dispatch_refusal(
            &db,
            &task.id,
            &task.status,
            &error,
        )
        .await
        .unwrap();
        let stored = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let annotation: Value =
            serde_json::from_str(stored.error_annotation.as_deref().unwrap()).unwrap();
        assert_eq!(annotation["type"], "dispatch_failed");
        assert!(annotation["message"]
            .as_str()
            .unwrap()
            .contains("daemon_upgrade_required"));
        assert!(annotation["message"]
            .as_str()
            .unwrap()
            .contains("upgrade the daemon"));
        assert_eq!(
            crate::agent_service::compute_effective_status(&db, &agent, Some(&registry))
                .await
                .unwrap(),
            crate::agent_service::EffectiveStatus::DaemonUpgradeRequired
        );
        let location = db::RepoLocationRepo::get_by_id(&*db, &placement.repo_location_id)
            .await
            .unwrap()
            .unwrap();
        let repo = RepoRepo::get_by_id(&*db, &location.repo_id)
            .await
            .unwrap()
            .unwrap();
        let runtime = db::RuntimeRepo::get_by_id(&*db, placement.runtime_id.as_deref().unwrap())
            .await
            .unwrap()
            .unwrap();
        let verifier = crate::repo_location::RemoteDaemonLocationVerifier::new(
            registry.clone(),
            PathBuf::from("unused"),
        );
        let verification = crate::repo_location::DaemonLocationVerifier::verify(
            &verifier, &repo, &location, &runtime,
        )
        .await
        .unwrap();
        assert_eq!(verification.status, location.status);
        assert!(verification
            .last_error
            .unwrap()
            .contains("daemon_upgrade_required"));
        let location_service =
            crate::repo_location::RepoLocationService::new(db.clone(), Arc::new(verifier));
        location_service
            .retry_verification_on_reconnect(daemon_id)
            .await
            .unwrap();
        let stored_location = db::RepoLocationRepo::get_by_id(&*db, &location.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored_location.status, location.status);
        assert!(stored_location
            .last_error
            .unwrap()
            .contains("daemon_upgrade_required"));
        let error = service
            .reserve_claim_workspace(&stored, Some(&agent), "coder")
            .await
            .err()
            .unwrap();
        let ServiceError::PlacementUnavailable(refusal) = error else {
            panic!("placement refusal")
        };
        assert!(refusal.needs_daemon_upgrade());
    }

    #[tokio::test]
    async fn placement_admission_prepare_failure_keeps_retry_budget() {
        let db = Arc::new(sqlite_db().await);
        let repo_dir = TempDir::new().unwrap();
        let workspace_root = TempDir::new().unwrap();
        let (project_id, _) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let task = seed_task(&db, &project_id, None).await;
        sqlx::query("UPDATE task SET metadata_json = '{\"execution_retry_count\":2}' WHERE id = ?")
            .bind(&task.id)
            .execute(db.pool())
            .await
            .unwrap();
        let task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let agent = seed_unpinned_claim_agent(&db).await;
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_workspace_root(workspace_root.path().to_path_buf());
        let admission = service
            .reserve_claim_workspace(&task, Some(&agent), "coder")
            .await
            .unwrap();
        let placement_id = admission.placement.id.clone();
        std::fs::remove_dir_all(repo_dir.path()).unwrap();
        assert!(matches!(
            service.prepare_claim_workspace(admission).await,
            Err(ServiceError::PrepareFailed { .. })
        ));
        let stored = WorkspacePlacementRepo::get_by_id(&*db, &placement_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, PlacementState::Failed);
        assert_eq!(
            stored.failure_cause,
            Some(db::PlacementFailureCause::PrepareFailed)
        );
        assert!(execution_count(&db, &task.id).await == 0);
        assert!(WorkspaceLeaseRepo::get_active_for_task(&*db, &task.id)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .unwrap()
                .unwrap(),
            task
        );
        assert_eq!(
            crate::agent_capacity::count_occupied_agent_slots(&db, &agent.id)
                .await
                .unwrap(),
            0
        );
        let retry = service
            .reserve_claim_workspace(&task, Some(&agent), "coder")
            .await
            .unwrap();
        assert_eq!(retry.placement.id, stored.id);
        assert_eq!(retry.placement.generation, stored.generation + 1);
        assert_eq!(retry.placement.state, PlacementState::Reserved);
        assert!(retry.placement.failure_cause.is_none());
        assert!(matches!(
            service.prepare_claim_workspace(retry).await,
            Err(ServiceError::PrepareFailed { .. })
        ));
        assert_eq!(execution_count(&db, &task.id).await, 0);
        assert_eq!(
            TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .unwrap()
                .unwrap(),
            task
        );
    }

    #[tokio::test]
    async fn placement_admission_concurrent_claims_cannot_take_preparing_slot() {
        let db = Arc::new(sqlite_db().await);
        let repo_dir = TempDir::new().unwrap();
        let workspace_root = TempDir::new().unwrap();
        let (project_id, _) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let first = seed_task(&db, &project_id, None).await;
        let second = seed_task(&db, &project_id, None).await;
        let agent = seed_unpinned_claim_agent(&db).await;
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_workspace_root(workspace_root.path().to_path_buf());
        let (first_result, second_result) = tokio::join!(
            service.reserve_claim_workspace(&first, Some(&agent), "coder"),
            service.reserve_claim_workspace(&second, Some(&agent), "coder"),
        );
        let (mut admission, refused, error) = match (first_result, second_result) {
            (Ok(admission), Err(ServiceError::PlacementUnavailable(error))) => {
                (admission, &second, error)
            }
            (Err(ServiceError::PlacementUnavailable(error)), Ok(admission)) => {
                (admission, &first, error)
            }
            _ => panic!("only one concurrent reservation may take the last Agent slot"),
        };
        assert!(error.rejected_candidates.iter().all(|candidate| candidate
            .filter_codes
            .contains(&crate::placement::PlacementFilterCode::AgentCapacity)));
        let mut update = crate::placement::admission::placement_update(&admission.placement);
        update.state = Some(PlacementState::Preparing);
        admission.placement = WorkspacePlacementRepo::update(&*db, update).await.unwrap();
        let error = match service
            .reserve_claim_workspace(refused, Some(&agent), "coder")
            .await
        {
            Err(ServiceError::PlacementUnavailable(error)) => error,
            _ => panic!("a preparing reservation must hold the last Agent slot"),
        };
        assert!(error.rejected_candidates.iter().all(|candidate| candidate
            .filter_codes
            .contains(&crate::placement::PlacementFilterCode::AgentCapacity)));
        assert!(WorkspaceRepo::get_by_task_id(&*db, &refused.id)
            .await
            .unwrap()
            .is_none());
        assert!(execution_count(&db, &refused.id).await == 0);
    }

    #[tokio::test]
    async fn placement_admission_fence_refusal_creates_attention_without_an_execution() {
        let db = Arc::new(sqlite_db().await);
        let repo_dir = TempDir::new().unwrap();
        let workspace_root = TempDir::new().unwrap();
        let (project_id, _) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let task = seed_task(&db, &project_id, None).await;
        let agent = seed_unpinned_claim_agent(&db).await;
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_workspace_root(workspace_root.path().to_path_buf());
        let admission = service
            .reserve_claim_workspace(&task, Some(&agent), "coder")
            .await
            .unwrap();
        let cause = db::PlacementFailureCause::WrongOwner;
        crate::placement::admission::fail_preparation(&db, &admission.placement, cause.clone())
            .await
            .unwrap();
        crate::placement::admission::record_fence_rejection(
            &db,
            &admission.placement,
            &cause,
            "wrong_owner",
        )
        .await
        .unwrap();
        let details: String = sqlx::query_scalar(
            "SELECT details_json FROM attention_projection WHERE attention_type = 'execution_failed' AND dedupe_key LIKE 'workspace-fence:%'",
        ).fetch_one(db.pool()).await.unwrap();
        let details: Value = serde_json::from_str(&details).unwrap();
        assert_eq!(details["failure_cause"], "wrong_owner");
        assert_eq!(details["placement_id"], admission.placement.id);
        assert_eq!(
            TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .unwrap()
                .unwrap(),
            task
        );
        assert_eq!(execution_count(&db, &task.id).await, 0);
    }

    #[tokio::test]
    async fn placement_admission_reclaim_reuses_and_fences_stale_start() {
        let db = Arc::new(sqlite_db().await);
        let repo_dir = TempDir::new().unwrap();
        let workspace_root = TempDir::new().unwrap();
        let (project_id, _) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let task = seed_task(&db, &project_id, None).await;
        let agent = seed_unpinned_claim_agent(&db).await;
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_workspace_root(workspace_root.path().to_path_buf());
        let first = service
            .reserve_claim_workspace(&task, Some(&agent), "coder")
            .await
            .unwrap();
        let first = service.prepare_claim_workspace(first).await.unwrap();
        let second = service
            .reserve_claim_workspace(&task, Some(&agent), "coder")
            .await
            .unwrap();
        let second = service.prepare_claim_workspace(second).await.unwrap();
        assert_eq!(first.placement.id, second.placement.id);
        assert_eq!(
            first.placement.workspace_handle,
            second.placement.workspace_handle
        );
        assert_eq!(first.placement.generation, second.placement.generation);
        let mut transaction = db::begin_immediate(db.pool()).await.unwrap();
        assert!(matches!(
            service
                .check_claim_placement_in_tx(&mut transaction, &task, &first)
                .await,
            Err(ServiceError::Db(DbError::VersionConflict))
        ));
        assert!(service
            .check_claim_placement_in_tx(&mut transaction, &task, &second)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn placement_admission_expiry_releases_capacity() {
        let db = Arc::new(sqlite_db().await);
        let repo_dir = TempDir::new().unwrap();
        let workspace_root = TempDir::new().unwrap();
        let (project_id, _) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let task = seed_task(&db, &project_id, None).await;
        let agent = seed_unpinned_claim_agent(&db).await;
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_workspace_root(workspace_root.path().to_path_buf());
        let admission = service
            .reserve_claim_workspace(&task, Some(&agent), "coder")
            .await
            .unwrap();
        let mut update = crate::placement::admission::placement_update(&admission.placement);
        update.state = Some(PlacementState::Preparing);
        update.reserved_until = Some(Some("2020-01-01T00:00:00Z".to_owned()));
        WorkspacePlacementRepo::update(&*db, update).await.unwrap();
        assert_eq!(
            crate::placement::admission::sweep_expired_reservations(&db, &now_rfc3339())
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            crate::agent_capacity::count_occupied_agent_slots(&db, &agent.id)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .unwrap()
                .unwrap(),
            task
        );
        assert!(execution_count(&db, &task.id).await == 0);
    }

    #[tokio::test]
    async fn prepared_workspace_has_one_server_placement() {
        let db = Arc::new(sqlite_db().await);
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let task = seed_task(&db, &project_id, None).await;
        let workspace_root = TempDir::new().expect("workspace root creates");

        let workspace =
            prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
                .await
                .expect("workspace prepares");
        let placement = WorkspacePlacementRepo::get_by_workspace_id(&*db, &workspace.id)
            .await
            .expect("placement loads")
            .expect("workspace has a placement");
        assert_eq!(placement.owner_kind, PlacementOwnerKind::Server);
        assert_eq!(placement.state, PlacementState::Ready);
        assert_eq!(placement.selected_by, PlacementSelectedBy::Scheduler);
        assert_eq!(
            placement.workspace_handle.as_deref(),
            Some(workspace.embedded_worktree_path_for_backend())
        );
        assert_eq!(
            serde_json::from_str::<Value>(&placement.selection_reason).unwrap(),
            json!({ "rule": "server_default" })
        );
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM workspace_placement WHERE workspace_id = ?")
                .bind(&workspace.id)
                .fetch_one(db.pool())
                .await
                .expect("placement count loads");
        assert_eq!(count, 1);
        let location = db::RepoLocationRepo::get_by_id(&*db, &placement.repo_location_id)
            .await
            .expect("location loads")
            .expect("placement location exists");
        assert_eq!(location.repo_id, repo_id);
        assert_eq!(location.owner_kind, db::RepoLocationOwnerKind::Server);
        assert_eq!(location.kind, RepoLocationKind::PrimaryCheckout);
        assert_eq!(location.status, db::RepoLocationStatus::Ready);
        assert_eq!(location.path, repo_dir.path().to_string_lossy());

        let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(16)))
            .with_workspace_root(workspace_root.path().to_path_buf());
        let router = service.workspace_backend_router();
        assert!(Arc::ptr_eq(
            &router,
            &service.clone().workspace_backend_router()
        ));
        let resolved = router
            .resolve(&db, &workspace)
            .await
            .expect("workspace resolves");
        assert_eq!(resolved.placement, placement);
        assert_eq!(
            resolved.handle().unwrap(),
            workspace.embedded_worktree_path_for_backend()
        );
        assert_eq!(
            resolved.embedded_path().unwrap(),
            PathBuf::from(workspace.embedded_worktree_path_for_backend())
        );
        assert_eq!(
            router.embedded_path(&db, &workspace).await.unwrap(),
            PathBuf::from(workspace.embedded_worktree_path_for_backend())
        );

        let reused = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("workspace reuses");
        assert_eq!(reused.id, workspace.id);
        assert_eq!(
            WorkspacePlacementRepo::get_by_workspace_id(&*db, &reused.id)
                .await
                .unwrap()
                .unwrap(),
            placement
        );
    }

    #[tokio::test]
    async fn remote_repository_registers_managed_clone_once() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        RepoRepo::update(
            &db,
            db::UpdateRepo {
                id: repo_id.clone(),
                name: None,
                local_path: Some(None),
                remote_url: None,
                work_mode: None,
                default_branch: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .expect("repository becomes remote-only");
        let first = seed_task(&db, &project_id, None).await;
        let second = seed_task(&db, &project_id, None).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let locks = Arc::new(RepoCacheLockManager::default());
        let (first_workspace, second_workspace) = tokio::join!(
            prepare_workspace_for_test(
                &db,
                workspace_root.path(),
                &first,
                &first.id,
                Some(Arc::clone(&locks))
            ),
            prepare_workspace_for_test(
                &db,
                workspace_root.path(),
                &second,
                &second.id,
                Some(Arc::clone(&locks))
            ),
        );
        let first_workspace = first_workspace.expect("first workspace prepares");
        let second_workspace = second_workspace.expect("second workspace prepares");
        let first_placement = WorkspacePlacementRepo::get_by_workspace_id(&db, &first_workspace.id)
            .await
            .unwrap()
            .unwrap();
        let second_placement =
            WorkspacePlacementRepo::get_by_workspace_id(&db, &second_workspace.id)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            first_placement.repo_location_id,
            second_placement.repo_location_id
        );
        let locations = db::RepoLocationRepo::list_by_repo(
            &db,
            &repo_id,
            db::PageRequest {
                cursor: None,
                limit: 20,
                include_total: false,
                sort_by: db::SortBy::CreatedAt,
                sort_order: db::SortOrder::Asc,
            },
        )
        .await
        .expect("locations load");
        assert_eq!(locations.items.len(), 1);
        let location = &locations.items[0];
        assert_eq!(location.owner_kind, db::RepoLocationOwnerKind::Server);
        assert_eq!(location.kind, RepoLocationKind::ManagedClone);
        assert_eq!(location.status, db::RepoLocationStatus::Ready);
        assert_eq!(
            location.path,
            workspace_root
                .path()
                .join(".repos")
                .join(&repo_id)
                .to_string_lossy()
        );
    }

    #[tokio::test]
    async fn daemon_placement_has_no_embedded_path() {
        let db = Arc::new(sqlite_db().await);
        let (project_id, _) = seed_project_repo(&db).await;
        let task = seed_task(&db, &project_id, None).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let workspace =
            seed_workspace(&db, &task, WorkspaceStatus::Ready, workspace_root.path()).await;
        let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(16)));
        let router = service.workspace_backend_router();
        let resolved = router
            .resolve(&db, &workspace)
            .await
            .expect("server workspace resolves");
        let now = now_rfc3339();
        let daemon_id = new_uuid_v4();
        let runtime_id = new_uuid_v4();
        sqlx::query(
            "INSERT INTO daemon (id, machine_id, hostname, os, arch, status, created_at, updated_at)
             VALUES (?, ?, 'remote', 'macos', 'aarch64', 'online', ?, ?)",
        ).bind(&daemon_id).bind(&daemon_id).bind(&now).bind(&now)
            .execute(db.pool()).await.expect("daemon creates");
        sqlx::query(
            "INSERT INTO runtime (id, daemon_id, kind, workspace_root, status, created_at, updated_at)
             VALUES (?, ?, 'local', '/remote', 'ready', ?, ?)",
        ).bind(&runtime_id).bind(&daemon_id).bind(&now).bind(&now)
            .execute(db.pool()).await.expect("runtime creates");
        sqlx::query(
            "UPDATE repo_location SET owner_kind = 'daemon', daemon_id = ?, runtime_id = ?,
             path = '/remote/repo', version = version + 1 WHERE id = ? AND version = 1",
        )
        .bind(&daemon_id)
        .bind(&runtime_id)
        .bind(&resolved.placement.repo_location_id)
        .execute(db.pool())
        .await
        .expect("location moves to fixture daemon");
        sqlx::query(
            "UPDATE workspace_placement SET owner_kind = 'daemon', daemon_id = ?, runtime_id = ?,
             workspace_handle = 'opaque-daemon-handle', version = version + 1
             WHERE id = ? AND version = ?",
        )
        .bind(&daemon_id)
        .bind(&runtime_id)
        .bind(&resolved.placement.id)
        .bind(resolved.placement.version)
        .execute(db.pool())
        .await
        .expect("placement moves to fixture daemon");

        assert!(matches!(
            router.resolve(&db, &workspace).await,
            Err(
                crate::workspace_backend::WorkspaceBackendError::OwnerUnsupported {
                    owner_kind: PlacementOwnerKind::Daemon,
                }
            )
        ));
        assert!(matches!(
            router.embedded_path(&db, &workspace).await,
            Err(
                crate::workspace_backend::WorkspaceBackendError::OwnerUnsupported {
                    owner_kind: PlacementOwnerKind::Daemon,
                }
            )
        ));
        let daemon_resolved = crate::workspace_backend::ResolvedWorkspace {
            placement: WorkspacePlacementRepo::get_by_workspace_id(&*db, &workspace.id)
                .await
                .unwrap()
                .unwrap(),
            backend: resolved.backend,
        };
        assert_eq!(daemon_resolved.handle().unwrap(), "opaque-daemon-handle");
        assert!(matches!(
            daemon_resolved.embedded_path(),
            Err(
                crate::workspace_backend::WorkspaceBackendError::OwnerUnsupported {
                    owner_kind: PlacementOwnerKind::Daemon,
                }
            )
        ));
    }

    #[tokio::test]
    async fn root_task_reuses_ready_task_workspace() {
        let db = sqlite_db().await;
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let root = seed_task(&db, &project_id, None).await;
        let worktree_dir = TempDir::new().expect("worktree dir creates");
        let workspace =
            seed_workspace(&db, &root, WorkspaceStatus::Ready, worktree_dir.path()).await;
        let temp = TempDir::new().expect("temp dir creates");

        let prepared = prepare_workspace_for_test(&db, temp.path(), &root, &root.id, None)
            .await
            .expect("root workspace prepares");

        assert_eq!(prepared.id, workspace.id);
        assert_eq!(prepared.task_id, root.id);
    }

    #[tokio::test]
    async fn existing_workspace_for_old_primary_is_not_reused() {
        let db = sqlite_db().await;
        let (project_id, old_repo_id) = seed_project_repo(&db).await;
        let task = seed_task(&db, &project_id, None).await;
        let worktree_dir = TempDir::new().expect("worktree dir creates");
        let old_workspace =
            seed_workspace(&db, &task, WorkspaceStatus::Ready, worktree_dir.path()).await;
        assert_eq!(old_workspace.repo_id, old_repo_id);

        let replacement_repo_id = new_uuid_v4();
        let now = now_rfc3339();
        RepoRepo::create(
            &db,
            CreateRepo {
                id: replacement_repo_id.clone(),
                project_id: project_id.clone(),
                name: "replacement".to_owned(),
                remote_url: Some("/tmp/replacement-repo".to_owned()),
                local_path: Some("/tmp/replacement-repo".to_owned()),
                work_mode: db::WorkMode::DirectMerge,
                default_branch: "main".to_owned(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("replacement Repo creates");
        let project = ProjectRepo::get_by_id(&db, &project_id)
            .await
            .expect("Project loads")
            .expect("Project exists");
        ProjectRepo::update_at_version(
            &db,
            UpdateProject {
                id: project_id,
                name: None,
                settings: None,
                primary_repo_id: Some(Some(replacement_repo_id.clone())),
                paused_at: None,
                updated_at: now_rfc3339(),
            },
            project.version,
            None,
        )
        .await
        .expect("Project primary Repo changes");

        let result =
            prepare_workspace_for_test(&db, worktree_dir.path(), &task, &task.id, None).await;
        assert!(
            matches!(
                &result,
                Err(ServiceError::WorkspaceResetRequired { task_id, reason })
                    if task_id == &task.id
                        && reason.contains(&old_repo_id)
                        && reason.contains(&replacement_repo_id)
            ),
            "expected an explicit reset boundary, got: {result:?}"
        );
        let preserved = WorkspaceRepo::get_by_task_id(&db, &task.id)
            .await
            .expect("Workspace reload succeeds")
            .expect("historical Workspace remains until guarded reset");
        assert_eq!(preserved.id, old_workspace.id);
        assert_eq!(preserved.repo_id, old_repo_id);
    }

    #[tokio::test]
    async fn subtask_reuses_ready_parent_workspace() {
        let db = sqlite_db().await;
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let root = seed_task(&db, &project_id, None).await;
        let subtask = seed_task(&db, &project_id, Some(root.id.clone())).await;
        let worktree_dir = TempDir::new().expect("worktree dir creates");
        let workspace =
            seed_workspace(&db, &root, WorkspaceStatus::Ready, worktree_dir.path()).await;
        let placement = WorkspacePlacementRepo::get_by_workspace_id(&db, &workspace.id)
            .await
            .expect("root placement loads")
            .expect("root placement exists");
        let temp = TempDir::new().expect("temp dir creates");

        let prepared = prepare_workspace_for_test(&db, temp.path(), &subtask, &subtask.id, None)
            .await
            .expect("subtask workspace prepares");

        assert_eq!(prepared.id, workspace.id);
        assert_eq!(prepared.task_id, root.id);
        assert_eq!(
            WorkspacePlacementRepo::get_by_workspace_id(&db, &prepared.id)
                .await
                .expect("shared placement loads")
                .expect("shared placement exists"),
            placement,
        );
    }

    #[tokio::test]
    async fn subtask_rejects_not_ready_parent_workspace() {
        let db = sqlite_db().await;
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let root = seed_task(&db, &project_id, None).await;
        let subtask = seed_task(&db, &project_id, Some(root.id.clone())).await;
        let temp = TempDir::new().expect("temp dir creates");
        let worktree_dir = TempDir::new().expect("worktree dir creates");
        seed_workspace(&db, &root, WorkspaceStatus::Creating, worktree_dir.path()).await;
        let not_ready =
            prepare_workspace_for_test(&db, temp.path(), &subtask, &subtask.id, None).await;
        assert!(matches!(
            not_ready,
            Err(ServiceError::ParentWorkspaceRequired { parent_task_id }) if parent_task_id == root.id
        ));
    }

    #[tokio::test]
    async fn subtask_without_parent_workspace_creates_root_owned_workspace() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let root = seed_task(&db, &project_id, None).await;
        let subtask = seed_task(&db, &project_id, Some(root.id.clone())).await;
        let workspace_root = TempDir::new().expect("workspace root creates");

        let workspace =
            prepare_workspace_for_test(&db, workspace_root.path(), &subtask, &subtask.id, None)
                .await
                .expect("subtask creates the shared root workspace");

        assert_eq!(workspace.task_id, root.id);
        assert_eq!(workspace.repo_id, repo_id);
        assert_eq!(workspace.status, WorkspaceStatus::Ready);
        assert!(std::path::Path::new(workspace.embedded_worktree_path_for_backend()).exists());
        assert!(
            WorkspaceRepo::get_by_task_id(&db, &subtask.id)
                .await
                .expect("subtask Workspace lookup succeeds")
                .is_none(),
            "the shared Workspace must remain owned by the coordination root"
        );
    }

    async fn seed_project_with_real_repo(
        db: &SqliteDb,
        repo_path: &std::path::Path,
    ) -> (String, String) {
        init_test_checkout(repo_path, "main");

        let now = now_rfc3339();
        let project_id = new_uuid_v4();
        let repo_id = new_uuid_v4();
        ProjectRepo::create(
            db,
            CreateProject {
                id: project_id.clone(),
                name: "Forge".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project creates");
        RepoRepo::create(
            db,
            CreateRepo {
                id: repo_id.clone(),
                project_id: project_id.clone(),
                name: "repo".to_owned(),
                remote_url: Some(repo_path.to_string_lossy().into_owned()),
                local_path: Some(repo_path.to_string_lossy().into_owned()),
                work_mode: db::WorkMode::DirectMerge,
                default_branch: "main".to_owned(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("repo creates");
        ProjectRepo::update_at_version(
            db,
            UpdateProject {
                id: project_id.clone(),
                name: None,
                settings: None,
                primary_repo_id: Some(Some(repo_id.clone())),
                paused_at: None,
                updated_at: now_rfc3339(),
            },
            ProjectRepo::get_by_id(db, &project_id)
                .await
                .expect("fixture Project lookup")
                .expect("fixture Project exists")
                .version,
            None,
        )
        .await
        .expect("project primary repo updates");
        (project_id, repo_id)
    }

    #[tokio::test]
    async fn missing_worktree_with_branch_auto_recovers() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, None).await;

        let fresh = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("first workspace creates");
        assert_eq!(fresh.status, WorkspaceStatus::Ready);
        let branch = fresh.branch.clone();

        // Delete the worktree directory to simulate a stale workspace
        std::fs::remove_dir_all(fresh.embedded_worktree_path_for_backend())
            .expect("remove worktree dir");
        assert!(!std::path::Path::new(fresh.embedded_worktree_path_for_backend()).exists());

        // Branch still exists in repo — recovery should recreate the worktree
        let recovered =
            prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
                .await
                .expect("workspace auto-recovers");
        assert_eq!(recovered.id, fresh.id);
        assert_eq!(recovered.branch, branch);
        assert!(std::path::Path::new(recovered.embedded_worktree_path_for_backend()).exists());
    }

    #[tokio::test]
    async fn directory_without_git_metadata_is_recreated() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, None).await;

        let fresh = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("first workspace creates");
        let worktree_path = std::path::PathBuf::from(fresh.embedded_worktree_path_for_backend());
        std::fs::remove_file(worktree_path.join(".git")).expect("worktree metadata removes");
        assert!(worktree_path.exists());
        assert!(!worktree_path.join(".git").exists());

        let recovered =
            prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
                .await
                .expect("workspace with missing Git metadata recovers");

        assert_eq!(recovered.id, fresh.id);
        assert!(worktree_path.join(".git").exists());
        assert!(git::get_current_sha(&worktree_path).await.is_ok());
    }

    #[tokio::test]
    async fn transient_worktree_probe_failure_preserves_workspace() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, None).await;
        let fresh = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("first workspace creates");
        let worktree_path = std::path::PathBuf::from(fresh.embedded_worktree_path_for_backend());
        let sentinel = worktree_path.join("uncommitted.txt");
        std::fs::write(&sentinel, "keep me\n").expect("sentinel writes");
        inject_worktree_probe_failure(&worktree_path, InjectedWorktreeProbeFailure::Io);

        let result =
            prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None).await;

        assert!(matches!(
            result,
            Err(ServiceError::Git(git::GitError::Io(_)))
        ));
        assert_eq!(
            WorkspaceRepo::get_by_id(&db, &fresh.id)
                .await
                .expect("workspace reloads"),
            Some(fresh)
        );
        assert_eq!(
            std::fs::read_to_string(sentinel).expect("sentinel survives"),
            "keep me\n"
        );
    }

    #[tokio::test]
    async fn worktree_reprobe_ready_preserves_existing_directory() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, None).await;
        let fresh = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("first workspace creates");
        let worktree_path = std::path::PathBuf::from(fresh.embedded_worktree_path_for_backend());
        let sentinel = worktree_path.join("uncommitted.txt");
        std::fs::write(&sentinel, "keep me\n").expect("sentinel writes");
        inject_worktree_probe_failure(&worktree_path, InjectedWorktreeProbeFailure::NotRepository);

        let recovered =
            prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
                .await
                .expect("ready re-probe returns the existing workspace");

        assert_eq!(recovered.id, fresh.id);
        assert_eq!(
            std::fs::read_to_string(sentinel).expect("sentinel survives"),
            "keep me\n"
        );
        let parent = worktree_path.parent().expect("worktree has a parent");
        assert!(std::fs::read_dir(parent)
            .expect("worktree parent reads")
            .all(|entry| !entry
                .expect("directory entry reads")
                .file_name()
                .to_string_lossy()
                .contains(".broken-")));
    }

    #[tokio::test]
    async fn branch_lookup_io_failure_preserves_workspace() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, None).await;
        let fresh = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("first workspace creates");
        let worktree_path = std::path::PathBuf::from(fresh.embedded_worktree_path_for_backend());
        std::fs::remove_dir_all(&worktree_path).expect("worktree removes");
        let output = std::process::Command::new("git")
            .args(["worktree", "prune"])
            .current_dir(repo_dir.path())
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .expect("git worktree prune runs");
        assert!(output.status.success());
        std::fs::create_dir_all(&worktree_path).expect("unusable directory creates");
        let sentinel = worktree_path.join("uncommitted.txt");
        std::fs::write(&sentinel, "keep me\n").expect("sentinel writes");
        inject_branch_lookup_failure(repo_dir.path());

        let result =
            prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None).await;

        assert!(matches!(
            result,
            Err(ServiceError::Git(git::GitError::Io(_)))
        ));
        assert_eq!(
            WorkspaceRepo::get_by_id(&db, &fresh.id)
                .await
                .expect("workspace reloads"),
            Some(fresh)
        );
        assert_eq!(
            std::fs::read_to_string(sentinel).expect("sentinel survives"),
            "keep me\n"
        );
    }

    #[tokio::test]
    async fn cleaned_workspace_is_rebuilt_from_its_task_branch() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, None).await;

        let fresh = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("first workspace creates");
        // What a reassignment reset does: the cleanup scheduler removes the
        // worktree and marks the row cleaned, but the task branch survives.
        ::workspace::WorkspaceManager::new(workspace_root.path().to_path_buf())
            .cleanup_worktree(
                &task.id,
                repo_dir.path(),
                Path::new(&fresh.embedded_worktree_path_for_backend()),
            )
            .await
            .expect("worktree cleans");
        WorkspaceRepo::mark_cleaned(&db, &fresh.id, &now_rfc3339())
            .await
            .expect("workspace marks cleaned");

        let rebuilt = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("cleaned workspace is rebuilt");
        assert_eq!(rebuilt.id, fresh.id);
        assert_eq!(rebuilt.branch, fresh.branch);
        assert_eq!(rebuilt.status, WorkspaceStatus::Ready);
        assert!(std::path::Path::new(rebuilt.embedded_worktree_path_for_backend()).exists());
    }

    #[tokio::test]
    async fn cleaned_workspace_without_branch_starts_fresh() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, None).await;

        let fresh = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("first workspace creates");
        ::workspace::WorkspaceManager::new(workspace_root.path().to_path_buf())
            .cleanup_worktree(
                &task.id,
                repo_dir.path(),
                Path::new(&fresh.embedded_worktree_path_for_backend()),
            )
            .await
            .expect("worktree cleans");
        WorkspaceRepo::mark_cleaned(&db, &fresh.id, &now_rfc3339())
            .await
            .expect("workspace marks cleaned");
        for args in [
            vec!["worktree", "prune"],
            vec!["branch", "-D", fresh.branch.as_str()],
        ] {
            std::process::Command::new("git")
                .args(&args)
                .current_dir(repo_dir.path())
                .env_remove("GIT_DIR")
                .env_remove("GIT_WORK_TREE")
                .env_remove("GIT_INDEX_FILE")
                .output()
                .expect("git runs");
        }

        let rebuilt = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("cleaned workspace starts fresh");
        assert_ne!(rebuilt.id, fresh.id);
        assert_eq!(rebuilt.status, WorkspaceStatus::Ready);
        assert!(std::path::Path::new(rebuilt.embedded_worktree_path_for_backend()).exists());
    }

    #[tokio::test]
    async fn unusable_existing_worktree_with_branch_auto_recovers() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, None).await;

        let fresh = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("first workspace creates");
        std::fs::write(
            std::path::Path::new(fresh.embedded_worktree_path_for_backend()).join(".git"),
            "gitdir: /tmp/forge-missing-gitdir\n",
        )
        .expect("break gitdir reference");
        assert!(git::get_current_sha(std::path::Path::new(
            fresh.embedded_worktree_path_for_backend()
        ))
        .await
        .is_err());

        let recovered =
            prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
                .await
                .expect("workspace auto-recovers");

        assert_eq!(recovered.id, fresh.id);
        assert_eq!(recovered.branch, fresh.branch);
        assert!(git::get_current_sha(std::path::Path::new(
            recovered.embedded_worktree_path_for_backend()
        ))
        .await
        .is_ok());
    }

    #[tokio::test]
    async fn existing_worktree_with_missing_repo_source_errors_before_reuse() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, None).await;

        let fresh = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("first workspace creates");
        std::fs::remove_dir_all(repo_dir.path()).expect("remove repo dir");

        let result =
            prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None).await;
        assert!(
            matches!(&result, Err(ServiceError::InvalidOperation { message }) if message.contains("does not exist")),
            "expected InvalidOperation about missing repo, got: {result:?}"
        );
        assert!(
            std::path::Path::new(fresh.embedded_worktree_path_for_backend()).exists(),
            "unrecoverable worktree should be left in place"
        );
    }

    #[tokio::test]
    async fn missing_worktree_and_branch_returns_reset_required() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, None).await;

        let fresh = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("first workspace creates");
        let branch = fresh.branch.clone();

        // Delete worktree AND the branch
        std::fs::remove_dir_all(fresh.embedded_worktree_path_for_backend())
            .expect("remove worktree dir");
        std::process::Command::new("git")
            .args(["worktree", "prune"])
            .current_dir(repo_dir.path())
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .expect("git worktree prune");
        std::process::Command::new("git")
            .args(["branch", "-D", &branch])
            .current_dir(repo_dir.path())
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .expect("delete branch");

        let result =
            prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None).await;
        assert!(
            matches!(&result, Err(ServiceError::WorkspaceResetRequired { task_id, .. }) if *task_id == task.id),
            "expected WorkspaceResetRequired, got: {result:?}"
        );

        // Workspace record should have been deleted
        let ws = WorkspaceRepo::get_by_task_id(&db, &task.id)
            .await
            .expect("db query ok");
        assert!(ws.is_none(), "workspace record should be deleted");
    }

    #[tokio::test]
    async fn missing_repo_source_returns_io_error() {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let workspace_root = TempDir::new().expect("workspace root creates");
        let task = seed_task(&db, &project_id, None).await;

        let fresh = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
            .await
            .expect("first workspace creates");

        // Delete worktree AND the entire repo directory
        std::fs::remove_dir_all(fresh.embedded_worktree_path_for_backend())
            .expect("remove worktree dir");
        std::fs::remove_dir_all(repo_dir.path()).expect("remove repo dir");

        let result =
            prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None).await;
        assert!(
            matches!(&result, Err(ServiceError::InvalidOperation { message }) if message.contains("does not exist")),
            "expected InvalidOperation about missing repo, got: {result:?}"
        );
    }
    #[tokio::test]
    async fn missing_worktree_child_claim_missing_branch_preserves_parent_row() {
        let db = Arc::new(sqlite_db().await);
        let repo = TempDir::new().unwrap();
        let root = TempDir::new().unwrap();
        let (project_id, _) = seed_project_with_real_repo(&db, repo.path()).await;
        let parent = seed_task(&db, &project_id, None).await;
        let child = seed_task(&db, &project_id, Some(parent.id.clone())).await;
        let agent = seed_unpinned_claim_agent(&db).await;
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_workspace_root(root.path().to_path_buf());
        let admission = service
            .reserve_claim_workspace(&parent, Some(&agent), "coder")
            .await
            .unwrap();
        let prepared = service.prepare_claim_workspace(admission).await.unwrap();
        std::fs::remove_dir_all(prepared.placement.workspace_handle.as_deref().unwrap()).unwrap();
        let output = Command::new("git")
            .args(["worktree", "prune"])
            .current_dir(repo.path())
            .output()
            .await
            .unwrap();
        assert!(output.status.success());
        let output = Command::new("git")
            .args(["branch", "-D", &prepared.workspace.branch])
            .current_dir(repo.path())
            .output()
            .await
            .unwrap();
        assert!(output.status.success());
        let admission = service
            .reserve_claim_workspace(&child, Some(&agent), "coder")
            .await
            .unwrap();
        assert!(service.prepare_claim_workspace(admission).await.is_err());
        assert_eq!(
            WorkspaceRepo::get_by_id(&*db, &prepared.workspace.id)
                .await
                .unwrap()
                .unwrap()
                .task_id,
            parent.id
        );
        assert!(
            WorkspacePlacementRepo::get_by_id(&*db, &prepared.placement.id)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn placement_ready_embedded_claim_describes_without_source_lock_or_prepare() {
        let db = Arc::new(sqlite_db().await);
        let repo = TempDir::new().unwrap();
        let root = TempDir::new().unwrap();
        let (project_id, _) = seed_project_with_real_repo(&db, repo.path()).await;
        let task = seed_task(&db, &project_id, None).await;
        let agent = seed_unpinned_claim_agent(&db).await;
        let locks = Arc::new(RepoCacheLockManager::new());
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_workspace_root(root.path().to_path_buf())
            .with_repo_cache_locks(locks.clone());
        let admission = service
            .reserve_claim_workspace(&task, Some(&agent), "coder")
            .await
            .unwrap();
        sqlx::query(
            "UPDATE workspace
             SET cleanup_attempts = 3, last_cleanup_error = 'prior cleanup failure'
             WHERE id = ?",
        )
        .bind(&admission.workspace.id)
        .execute(db.pool())
        .await
        .unwrap();
        let prepared = service.prepare_claim_workspace(admission).await.unwrap();
        assert_eq!(prepared.workspace.cleanup_attempts, 0);
        assert!(prepared.workspace.last_cleanup_error.is_none());
        // Ready reclaims don't rerun history validation or clone/worktree creation.
        sqlx::query("UPDATE workspace SET before_sha = 'obsolete-base-object' WHERE id = ?")
            .bind(&prepared.workspace.id)
            .execute(db.pool())
            .await
            .unwrap();
        let _source_lock = locks.acquire(repo.path().to_str().unwrap()).await;
        let admission = service
            .reserve_claim_workspace(&task, Some(&agent), "coder")
            .await
            .unwrap();
        let admitted_version = admission.placement.version;
        let reclaimed = service.prepare_claim_workspace(admission).await.unwrap();
        assert_eq!(
            reclaimed.placement.workspace_handle,
            prepared.placement.workspace_handle
        );
        assert_eq!(reclaimed.placement.version, admitted_version);
        assert_eq!(
            reclaimed.workspace.before_sha.as_deref(),
            Some("obsolete-base-object")
        );
    }

    #[tokio::test]
    async fn placement_first_offline_owner_wait_creates_attention_and_expires_without_execution() {
        let db = Arc::new(sqlite_db().await);
        let repo = TempDir::new().unwrap();
        let (project_id, _) = seed_project_with_real_repo(&db, repo.path()).await;
        let mut task = seed_task(&db, &project_id, None).await;
        sqlx::query("UPDATE task SET status = 'in_progress' WHERE id = ?")
            .bind(&task.id)
            .execute(db.pool())
            .await
            .unwrap();
        task.status = "in_progress".into();
        sqlx::query("UPDATE task SET metadata_json = json_set(COALESCE(metadata_json, '{}'), '$.concurrent_note', 'retained') WHERE id = ?")
            .bind(&task.id).execute(db.pool()).await.unwrap();
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_workspace_max_disconnect(Duration::from_secs(1));
        let refusal = ServiceError::DaemonUnavailable {
            daemon_id: "offline-owner".into(),
        };
        assert!(service
            .defer_placement_refusal(&task, &refusal)
            .await
            .unwrap());
        let attention: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM attention_projection WHERE dedupe_key = ? AND status = 'open'",
        )
        .bind(format!("task-owner-wait:{}", task.id))
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(attention, 1);
        let waiting = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let attention_version: i64 =
            sqlx::query_scalar("SELECT version FROM attention_projection WHERE dedupe_key = ?")
                .bind(format!("task-owner-wait:{}", task.id))
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert!(service
            .defer_placement_refusal(&waiting, &refusal)
            .await
            .unwrap());
        let again = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(again.version, waiting.version);
        let unchanged: i64 =
            sqlx::query_scalar("SELECT version FROM attention_projection WHERE dedupe_key = ?")
                .bind(format!("task-owner-wait:{}", task.id))
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(unchanged, attention_version);
        sqlx::query("UPDATE task SET metadata_json = json_set(metadata_json, '$.owner_wait.started_at', ?) WHERE id = ?")
            .bind((Utc::now() - chrono::Duration::seconds(2)).to_rfc3339()).bind(&task.id).execute(db.pool()).await.unwrap();
        let dispatcher = crate::task_dispatcher::TaskDispatcher::new(
            db.clone(),
            Arc::new(EventBus::default()),
            Arc::new(service),
        );
        dispatcher.check_once().await.unwrap();
        let task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let blocked: api_types::InterruptionMetadata =
            serde_json::from_str(task.blocked_json.as_deref().unwrap()).unwrap();
        assert_eq!(blocked.kind, Some(api_types::FailureKind::RecoveryRequired));
        assert!(!blocked.reason.is_empty());
        assert!(!blocked.created_at.is_empty());
        let metadata: Value = serde_json::from_str(task.metadata_json.as_deref().unwrap()).unwrap();
        assert_eq!(metadata["concurrent_note"], "retained");
        assert!(task
            .error_annotation
            .as_deref()
            .unwrap()
            .contains("owner_disconnected_timeout"));
        assert!(crate::deferred_dispatch::pending_until(&task).is_none());
        assert_eq!(execution_count(&db, &task.id).await, 0);
    }
    #[tokio::test]
    async fn placement_expired_owner_wait_on_terminal_placement_falls_through() {
        let db = Arc::new(sqlite_db().await);
        let (task, placement, _) = crate::recovery::tests::daemon_owned_fixture(&db).await;
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_workspace_max_disconnect(Duration::ZERO);
        for state in [
            PlacementState::Failed,
            PlacementState::Cleaning,
            PlacementState::Cleaned,
        ] {
            sqlx::query("UPDATE workspace_placement SET state = ? WHERE id = ?")
                .bind(state.to_string())
                .bind(&placement.id)
                .execute(db.pool())
                .await
                .unwrap();
            sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
                .bind(json!({"owner_wait":{"daemon_id":placement.daemon_id,"started_at":"1970-01-01T00:00:00Z"}}).to_string())
                .bind(&task.id).execute(db.pool()).await.unwrap();
            let task = TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .unwrap()
                .unwrap();
            assert!(!service.expire_owner_wait(&task).await.unwrap());
            let current = TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .unwrap()
                .unwrap();
            let metadata: Value =
                serde_json::from_str(current.metadata_json.as_deref().unwrap()).unwrap();
            assert!(metadata.get("owner_wait").is_none());
            assert!(current.blocked_json.is_none());
        }
        let mut malformed = task;
        malformed.metadata_json = Some("{malformed".into());
        assert!(!service.expire_owner_wait(&malformed).await.unwrap());
    }
    #[tokio::test]
    async fn placement_daemon_describe_failure_is_transient_without_prepare() {
        let db = Arc::new(sqlite_db().await);
        let (task, placement, _) = crate::recovery::tests::daemon_owned_fixture(&db).await;
        sqlx::query(
            "UPDATE workspace_placement SET state = 'ready', disconnected_at = NULL WHERE id = ?",
        )
        .bind(&placement.id)
        .execute(db.pool())
        .await
        .unwrap();
        let registry =
            Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
        let daemon_id = placement.daemon_id.clone().unwrap();
        let (connection_id, mut outbound) =
            crate::recovery::tests::owner_connection(&registry, &daemon_id, false);
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_daemon_connections(registry.clone());
        let router = Arc::new(
            (*service.workspace_backend_router())
                .clone()
                .with_daemon(Arc::new(
                    crate::workspace_backend::DaemonWorkspaceBackend::new(
                        db.clone(),
                        registry.clone(),
                    ),
                )),
        );
        let responder = tokio::spawn(async move {
            let api_types::DaemonFrame::Request { id, method, .. } = outbound.recv().await.unwrap()
            else {
                panic!("describe request");
            };
            assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
            registry.dispatch_incoming_for_connection(
                &daemon_id,
                connection_id,
                api_types::DaemonFrame::Error {
                    id: Some(id),
                    error: api_types::DaemonErrorPayload {
                        code: "workspace_error".into(),
                        message: "temporary Git read failure".into(),
                        details: None,
                    },
                },
            );
            outbound
        });
        let error = prepare_workspace(
            &db,
            std::path::Path::new("/owner-only"),
            &task,
            &task.id,
            None,
            &router,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ServiceError::Git(_)), "{error:?}");
        assert!(
            responder.await.unwrap().try_recv().is_err(),
            "describe failure must not send prepare"
        );
    }
}
