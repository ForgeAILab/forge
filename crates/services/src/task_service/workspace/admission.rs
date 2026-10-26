//! Workspace selection, capacity waits, and reservations.

use super::*;
use db::task_condition::channels as channel;

impl TaskService {
    pub(crate) fn environment_daemon_connections(
        &self,
    ) -> Option<&Arc<crate::daemon_transport::DaemonConnectionRegistry>> {
        self.daemon_connections.as_ref()
    }

    pub(crate) async fn defer_initial_environment_probe(
        &self,
        task: &Task,
        agent: &Agent,
        role: &str,
    ) -> Result<bool> {
        let raw: String = self
            .db
            .workspace_admission_project_settings(&task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", &task.project_id))?;
        let settings = serde_json::from_str::<ProjectSettings>(&raw).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid project settings: {error}"))
        })?;
        if settings.environment.checks.is_empty() || !settings.environment.assets.is_empty() {
            return Ok(false);
        }
        let prepared = crate::placement::context::prepare_selection(
            &self.db,
            task,
            Some(agent),
            role,
            self.placement_adapter_registry.as_deref(),
        )
        .await?;
        let context = self.environment_selection_context(task, &prepared).await?;
        crate::placement::environment::start_context_probes(
            &self.db,
            &context,
            &self.daemon_connections.clone().unwrap_or_else(|| {
                Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers())
            }),
            &self.event_bus,
            self.dispatch_notify(),
        )
        .await?;
        let Err(refusal) = crate::placement::select_placement(&context).into_result() else {
            return Ok(false);
        };
        if refusal.is_deterministic() {
            return self
                .record_placement_dispatch_refusal(
                    task,
                    &ServiceError::PlacementUnavailable(refusal),
                )
                .await;
        }
        crate::placement::environment::handle_refusal(
            &self.db,
            &self.event_bus,
            task,
            &prepared.project,
            &context,
            &refusal,
            self.daemon_connections.as_deref(),
        )
        .await
    }

    pub(crate) async fn environment_selection_context(
        &self,
        task: &Task,
        prepared: &crate::placement::context::PreparedSelection,
    ) -> Result<crate::placement::SelectionContext> {
        let empty = crate::daemon_transport::DaemonConnectionRegistry::without_handlers();
        let registry = self.daemon_connections.as_deref().unwrap_or(&empty);
        let mut tx = self.db.pool().begin().await?;
        let workspace_id: Option<String> = self
            .db
            .workspace_admission_task_workspace_id_in_tx(
                &mut tx,
                task.parent_task_id.as_deref().unwrap_or(&task.id),
            )
            .await?;
        let path = prepared
            .repo
            .local_path
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| self.workspace_root.join(".repos").join(&prepared.repo.id));
        let context = prepared
            .load_context(
                &self.db,
                &mut tx,
                registry,
                task,
                workspace_id.as_deref(),
                Some(&path),
            )
            .await?
            .expect("environment selection has a claiming Agent");
        tx.rollback().await?;
        Ok(context)
    }

    pub(in crate::task_service) fn check_placement_lease_owner(
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
        if matches!(error,ServiceError::PlacementUnavailable(refusal) if refusal.rejected_candidates.iter().any(|candidate|candidate.filter_codes.contains(&crate::placement::PlacementFilterCode::ProvisionFailed)))
            && self.record_placement_dispatch_refusal(task, error).await?
        {
            return Ok(true);
        }
        if !crate::placement::is_machine_capacity_refusal(error) {
            crate::deferred_dispatch::clear_capacity_wait(&self.db, task).await?;
        }
        if let ServiceError::ProjectPaused { .. } = error {
            if ProjectRepo::get_by_id(&*self.db, &task.project_id)
                .await?
                .is_some_and(|project| {
                    project.system_pause_reason.as_deref() == Some("environment_not_ready")
                })
            {
                return Ok(true);
            }
        }
        if let Some(deferred) = crate::placement::environment::defer_refusal(
            &self.db,
            task,
            error,
            self.daemon_connections.as_deref(),
        )
        .await?
        {
            return Ok(deferred);
        }
        if !crate::placement::admission_refusal_is_retryable(&self.db, &task.id, error).await? {
            return Ok(false);
        }
        if let Some(wait) = crate::placement::capacity_wait_of(error) {
            crate::deferred_dispatch::record_capacity_wait(&self.db, task, wait).await?;
            return Ok(true);
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
            serde_json::to_string(&channel::OwnerWait {
                started_at: started_at.to_rfc3339(),
                daemon_id: daemon_id.clone(),
                plan_execution_id: None,
            })
            .expect("owner wait serializes")
        });
        let deferral = (!expired).then(|| {
            serde_json::to_string(&channel::DeferredDispatch {
                target_state: task.status.clone(),
                reason: reason.clone(),
                kind: None,
                not_before: (now + chrono::Duration::seconds(30)).to_rfc3339(),
            })
            .expect("admission deferral serializes")
        });
        let annotation = expired.then(|| {
            serde_json::to_string(&channel::ErrorAnnotation::new(
                channel::AnnotationShape::TimedBlock(channel::TimedBlockAnnotation {
                    kind: api_types::FailureKind::RecoveryRequired,
                    blocking_reason: "owner_disconnected_timeout".into(),
                    message: reason.clone(),
                    blocked_at: now.to_rfc3339(),
                    blocked_by: "system:workflow".into(),
                }),
            ))
            .expect("admission annotation serializes")
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
            serde_json::to_string(&channel::Interruption {
                reason: Some(reason.clone()),
                created_at: Some(Some(now.to_rfc3339())),
                kind: Some(Some(api_types::FailureKind::RecoveryRequired.into())),
                execution_id: Some(None),
                ..Default::default()
            })
            .expect("admission interruption serializes")
        });
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let result = self
            .db
            .record_placement_refusal_in_tx(
                &mut tx,
                &task.id,
                db::RecordPlacementRefusalInTxInput {
                    deferral: &deferral,
                    deferral_value: &deferral,
                    annotation: &annotation,
                    blocked: &blocked,
                    updated_at: now.to_rfc3339(),
                    version_increment: i64::from(!unchanged_reason),
                    task_id: &task.id,
                    expected_version: task.version,
                    expected_status: &task.status,
                },
            )
            .await?;
        // The dispatcher records refusals outside the Task lease, where this
        // blocking deferral is queued. It is identity-fenced to the status it
        // was computed for: it lands unless the Task moved on (then moot), so
        // the attention recorded below is not based on a dropped write.
        if result.applied().is_some_and(|rows| rows != 1) {
            return Err(DbError::VersionConflict.into());
        }
        if let Some(owner_wait) = owner_wait.filter(|_| !unchanged_reason) {
            let _queued_or_applied = self
                .db
                .record_placement_owner_wait_in_tx(
                    &mut tx,
                    &task.id,
                    owner_wait,
                    &task.id,
                    &task.status,
                )
                .await?;
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
        if !db::task_writer::owns_task(&task.id) {
            return self
                .request_task_command(
                    &task.id,
                    "expire_owner_wait",
                    serde_json::json!([task.id]),
                    false,
                )
                .await;
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
        self.db
            .clear_expired_placement_wait_in_tx(&mut tx, &task.id, &task.id, current.version)
            .await?
            .require_applied()?;
        self.db
            .resolve_expired_placement_wait_attention_in_tx(
                &mut tx,
                now_rfc3339(),
                now_rfc3339(),
                format!("task-owner-wait:{}", task.id),
                format!("task-environment-wait:{}", task.id),
            )
            .await?;
        tx.commit().await?;
        Ok(false)
    }

    /// What the Task would wait for if dispatched now: a run slot, or free
    /// disk for a new worktree. Before a machine is counted as short of
    /// disk, its garbage is collected once and its disk read again.
    pub(crate) async fn capacity_wait_for(
        &self,
        task: &Task,
        agent: &Agent,
        role: Option<&str>,
    ) -> Result<Option<crate::placement::CapacityWait>> {
        self.reclaim_disk_before_refusing().await;
        crate::placement::machine_precheck::task_blocked(
            &self.db,
            task,
            agent,
            self.daemon_connections.as_deref(),
            self.placement_adapter_registry.as_deref(),
            &self.workspace_root,
            role,
        )
        .await
    }

    pub(in crate::task_service) async fn reserve_claim_workspace(
        &self,
        task: &Task,
        agent: Option<&Agent>,
        role: &str,
    ) -> Result<WorkspaceAdmission> {
        self.reserve_workspace_admission(
            task,
            agent,
            role,
            crate::placement::selection::EnvironmentAdmission::Dispatcher,
        )
        .await
    }

    /// The server's own root only: a daemon collects its garbage itself
    /// before it reports a reading under the mark.
    pub(crate) async fn reclaim_disk_before_refusing(&self) {
        if let Some(scheduler) = self.cleanup_scheduler.as_ref() {
            scheduler.reclaim_before_refusing().await;
        }
    }

    pub(in crate::task_service) async fn reserve_workspace_admission(
        &self,
        task: &Task,
        agent: Option<&Agent>,
        role: &str,
        environment_admission: crate::placement::selection::EnvironmentAdmission,
    ) -> Result<WorkspaceAdmission> {
        use crate::placement::select_placement;
        self.reclaim_disk_before_refusing().await;
        if agent.is_some() {
            self.ensure_project_not_paused(task).await?;
        }
        let authority = crate::placement::context::prepare_selection(
            &self.db,
            task,
            agent,
            role,
            self.placement_adapter_registry.as_deref(),
        )
        .await?;
        let repo = &authority.repo;
        let settings = authority.settings.clone();
        let review_config = authority.review_config.clone();
        let claiming_agent = authority.claiming_agent.clone();
        let worktree_agents = authority.worktree_agents.clone();
        let server_facts = authority.server_facts.clone();
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
            let bound_location = self
                .db
                .workspace_admission_bound_repo_location(workspace_task_id)
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
            let locations: Vec<db::CloneVerificationLocationRow> = self
                .db
                .workspace_admission_clone_verification_locations(
                    &repo.id,
                    &bound_location,
                    &bound_location,
                )
                .await?;
            for db::CloneVerificationLocationRow {
                id,
                path,
                version,
                last_error,
            } in locations
            {
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
        let project_version = self
            .db
            .workspace_admission_project_version_in_tx(&mut transaction, &task.project_id)
            .await?;
        if project_version != authority.project.version {
            return Err(DbError::VersionConflict.into());
        }
        for role in claiming_agent.iter().chain(worktree_agents.iter()) {
            let current_version = self
                .db
                .workspace_admission_agent_version_in_tx(&mut transaction, &role.agent.id)
                .await?;
            if current_version != Some(role.agent.version) {
                return Err(DbError::VersionConflict.into());
            }
        }
        let workspace_id = self
            .db
            .workspace_admission_workspace_id_in_tx(&mut transaction, workspace_task_id)
            .await?;
        let existing = match workspace_id.as_deref() {
            Some(id) => {
                WorkspacePlacementRepo::get_by_workspace_id_in_tx(&*self.db, &mut transaction, id)
                    .await?
            }
            None => None,
        };
        if let Some(workspace_id) = workspace_id.as_deref() {
            if let Some(execution_id) = self
                .db
                .workspace_admission_running_execution_id_in_tx(&mut transaction, workspace_id)
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
        let location_count = self
            .db
            .workspace_admission_server_location_count_in_tx(&mut transaction, &repo.id)
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
        let mut context = authority
            .load_context(
                &self.db,
                &mut transaction,
                registry,
                task,
                workspace_id.as_deref(),
                None,
            )
            .await?;
        if let Some(context) = context.as_mut() {
            context.environment_admission = environment_admission;
        }
        let (
            location_id,
            owner_kind,
            daemon_id,
            runtime_id,
            execution_daemon_id,
            selected_by,
            reason,
        ) = if let Some(context) = context.as_ref() {
            let selection = match select_placement(context).into_result() {
                Ok(selection) => selection,
                Err(refusal) => {
                    transaction.rollback().await?;
                    crate::placement::environment::start_context_probes(
                        &self.db,
                        context,
                        &self.daemon_connections.clone().unwrap_or_else(|| {
                            Arc::new(
                                crate::daemon_transport::DaemonConnectionRegistry::without_handlers(
                                ),
                            )
                        }),
                        &self.event_bus,
                        self.dispatch_notify(),
                    )
                    .await?;
                    crate::placement::environment::handle_refusal(
                        &self.db,
                        &self.event_bus,
                        task,
                        &authority.project,
                        context,
                        &refusal,
                        self.daemon_connections.as_deref(),
                    )
                    .await?;
                    let error = ServiceError::PlacementUnavailable(refusal);
                    if environment_admission
                        == crate::placement::selection::EnvironmentAdmission::LaunchPreflight
                        && matches!(&error, ServiceError::PlacementUnavailable(refusal) if refusal.rejected_candidates.iter().any(|candidate| candidate.filter_codes.iter().any(|code|matches!(code,crate::placement::PlacementFilterCode::EnvironmentUnverified|crate::placement::PlacementFilterCode::ProvisionFailed))))
                    {
                        self.record_placement_dispatch_refusal(task, &error).await?;
                    }
                    return Err(error);
                }
            };
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
            let row = self
                .db
                .workspace_admission_ready_server_location_in_tx(&mut transaction, &repo.id)
                .await?;
            let location_id = existing
                .as_ref()
                .filter(|placement| placement.owner_kind == PlacementOwnerKind::Server)
                .map(|placement| placement.repo_location_id.clone())
                .or_else(|| row.map(|row| row.id()))
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
            self.db
                .insert_creating_admission_workspace_in_tx(
                    &mut transaction,
                    db::InsertCreatingAdmissionWorkspaceInTxInput {
                        id: &workspace_id,
                        task_id: workspace_task_id,
                        repo_id: &repo.id,
                        worktree_path: server_path,
                        branch: ::workspace::task_branch_name(workspace_task_id),
                        created_at: &now,
                        updated_at: &now,
                    },
                )
                .await?;
        } else {
            let workspace_repo = self
                .db
                .workspace_admission_repo_id_in_tx(&mut transaction, &workspace_id)
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
                update.reserved_until = Some(Some(crate::task_service::execution::rfc3339_after(
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
                    reserved_until: Some(crate::task_service::execution::rfc3339_after(
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
            self.db
                .mark_reserved_workspace_creating_in_tx(
                    &mut transaction,
                    path,
                    &placement.updated_at,
                    &workspace_id,
                )
                .await?;
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
            environment_admission,
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

    /// The refusal of a worktree that would have to be made again on a
    /// machine short of disk: the same `disk_pressure` placement refusal a
    /// new worktree gets, so the Task takes the same self-clearing wait.
    pub(super) fn refuse_recreation_under_disk_pressure(
        &self,
        admission: &WorkspaceAdmission,
        pressure: Option<api_types::DiskPressureKind>,
    ) -> Result<()> {
        if pressure.is_none() {
            return Ok(());
        }
        let placement = &admission.placement;
        Err(ServiceError::PlacementUnavailable(
            crate::placement::PlacementUnavailable {
                task_id: admission.claiming_task.id.clone(),
                repo_id: admission.workspace.repo_id.clone(),
                rejected_candidates: vec![crate::placement::CandidateRejection {
                    failing_checks: Vec::new(),
                    repo_location_id: placement.repo_location_id.clone(),
                    owner_kind: placement.owner_kind.to_string(),
                    daemon_id: placement.daemon_id.clone(),
                    runtime_id: placement.runtime_id.clone(),
                    filter_codes: vec![crate::placement::PlacementFilterCode::DiskPressure],
                }],
            },
        ))
    }
}
