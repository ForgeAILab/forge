//! Claim workspace preparation and transactional placement fencing.

use super::*;

impl TaskService {
    pub(in crate::task_service) async fn prepare_claim_workspace(
        &self,
        mut admission: WorkspaceAdmission,
    ) -> Result<WorkspaceAdmission> {
        if admission.placement.state == PlacementState::Ready
            && admission.placement.owner_kind == PlacementOwnerKind::Server
        {
            // A ready placement is admitted under disk pressure because its
            // worktree exists. One whose directory is gone (deleted, or
            // cleaned and waiting for its next run) would be made again
            // here: that is new disk, refused like a new worktree.
            // A server placement's handle is its worktree directory.
            if admission.workspace.status != WorkspaceStatus::Ready
                || !admission
                    .placement
                    .workspace_handle
                    .as_deref()
                    .is_some_and(|worktree| Path::new(worktree).is_dir())
            {
                self.refuse_recreation_under_disk_pressure(
                    &admission,
                    self.db.disk_admission.server_pressure(),
                )?;
            }
            // The launch guard every executor family shares: the workspace
            // manager checks the recorded worktree against disk and Git and
            // repairs what it can, so a CLI executor is never started in a
            // directory that is gone or off the Task branch.
            let workspace = if admission.workspace.status == WorkspaceStatus::Ready {
                // A claim that cannot have its worktree made again under
                // the disk floor takes the disk capacity wait.
                let manager = crate::workspace_manager::WorkspaceManager::new(
                    &self.db,
                    &self.workspace_root,
                    self.repo_cache_locks.clone(),
                    &self.workspace_backend_router,
                )
                .waiting_for_disk();
                let workspace = admission.workspace.clone();
                // Only the Task that owns the row may forget it when its
                // branch is gone too; a subtask never drops the shared root.
                let valid = if workspace.task_id == admission.claiming_task.id {
                    manager
                        .ensure_valid_or_forget(
                            &admission.claiming_task,
                            workspace,
                            Purpose::Execute,
                        )
                        .await
                } else {
                    manager
                        .ensure_valid(&admission.claiming_task, workspace, Purpose::Execute)
                        .await
                }?;
                if valid.repair() != crate::workspace_manager::Repair::None {
                    info!(
                        task_id = %admission.claiming_task.id,
                        workspace_id = %valid.workspace().id,
                        placement_id = %admission.placement.id,
                        repair = ?valid.repair(),
                        rescued_ref = valid.rescued_ref().unwrap_or(""),
                        "workspace repaired at claim"
                    );
                }
                valid.into_workspace()
            } else {
                // A row that is not `ready` (a cleaned workspace awaiting its
                // next run) is rebuilt by the create-or-reuse path.
                prepare_workspace(
                    &self.db,
                    &self.workspace_root,
                    &admission.claiming_task,
                    &admission.claiming_task.id,
                    self.repo_cache_locks.clone(),
                    &self.workspace_backend_router,
                )
                .await?
            };
            admission.workspace = clear_workspace_cleanup_after(&self.db, workspace).await?;
            admission.placement =
                WorkspacePlacementRepo::get_by_id(&*self.db, &admission.placement.id)
                    .await?
                    .ok_or(DbError::NotFound)?;
            return Ok(admission);
        }
        if admission.placement.state == PlacementState::Ready {
            // A daemon answers for its own placement; Forge never interprets
            // its handle.
            let needs_recreation = match async {
                let backend = self
                    .workspace_backend_router
                    .for_placement(&admission.placement)?;
                backend.describe(&admission.placement).await
            }
            .await
            {
                Ok(state) => !state.exists,
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
                let pressure = match admission.placement.daemon_id.as_deref() {
                    Some(daemon_id) => {
                        db::machine_disk::daemon_pressure(&self.db, daemon_id).await?
                    }
                    None => self.db.disk_admission.server_pressure(),
                };
                self.refuse_recreation_under_disk_pressure(&admission, pressure)?;
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
                // The owner's own disk reading is under the floor: it made
                // nothing. The reservation is given back and the Task takes
                // the disk wait, as when the server refuses from the
                // owner's report. No retry budget is spent on it.
                if let Ok(Err(crate::workspace_backend::WorkspaceBackendError::DiskPressure {
                    ..
                })) = &failure
                {
                    match crate::placement::admission::fail_preparation(
                        &self.db,
                        &admission.placement,
                        db::PlacementFailureCause::PrepareFailed,
                    )
                    .await
                    {
                        Ok(()) | Err(ServiceError::Db(DbError::VersionConflict)) => {}
                        Err(error) => return Err(error),
                    }
                    self.refuse_recreation_under_disk_pressure(
                        &admission,
                        Some(api_types::DiskPressureKind::Bytes),
                    )?;
                }
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
        self.db
            .mark_prepared_admission_workspace_ready_in_tx(
                &mut transaction,
                db::MarkPreparedAdmissionWorkspaceReadyInTxInput {
                    branch: prepared.branch,
                    before_sha: prepared.base_sha,
                    worktree_path: if admission.placement.owner_kind == PlacementOwnerKind::Server {
                        prepared.handle
                    } else {
                        String::new()
                    },
                    updated_at: now,
                    workspace_id: &admission.workspace.id,
                },
            )
            .await?;
        transaction.commit().await?;
        admission.workspace = WorkspaceRepo::get_by_id(&*self.db, &admission.workspace.id)
            .await?
            .ok_or(DbError::NotFound)?;
        Ok(admission)
    }

    pub(in crate::task_service) async fn check_claim_placement_in_tx(
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
                let current_version = self
                    .db
                    .workspace_claim_agent_version_in_tx(transaction, &role.agent.id)
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
            let mut context = crate::placement::load_selection_context(
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
                    fallback_server_location: None,
                },
            )
            .await?;
            context.environment_admission = admission.environment_admission;
            crate::placement::select_placement(&context).into_result()?;
        }
        self.db
            .clear_claim_placement_waits_in_tx(transaction, &task.id, &task.id)
            .await?
            .require_applied()?;
        self.db
            .resolve_claim_placement_wait_attention_in_tx(
                transaction,
                now_rfc3339(),
                now_rfc3339(),
                format!("task-owner-wait:{}", task.id),
                format!("task-environment-wait:{}", task.id),
            )
            .await?;
        Ok(())
    }
}
