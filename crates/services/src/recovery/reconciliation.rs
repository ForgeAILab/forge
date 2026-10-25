//! Workspace placement reconciliation and terminal cascades.

use super::*;

pub(crate) async fn reconcile_workspace_placement(
    db: &Arc<SqliteDb>,
    event_bus: &EventBus,
    registry: &Arc<DaemonConnectionRegistry>,
    task_service: Option<&TaskService>,
    placement: &WorkspacePlacement,
) -> Result<bool> {
    let Some(daemon_id) = placement_execution_daemon_id(placement) else {
        return Ok(false);
    };
    if !DaemonRepo::get_by_id(&**db, daemon_id)
        .await?
        .is_some_and(|daemon| daemon.status == db::DaemonStatus::Online)
    {
        return Ok(false);
    }
    let Some(connection) = registry.get(daemon_id) else {
        return Ok(false);
    };
    let owner = crate::daemon_transport::execution_lease_owner(daemon_id, connection.id());
    for execution in ExecutionRepo::list_running(&**db).await? {
        if execution.workspace_id.as_deref() == Some(placement.workspace_id.as_str())
            && execution.lease_owner.as_deref() == Some(owner.as_str())
            && placement
                .disconnected_at
                .as_deref()
                .is_some_and(|disconnected_at| {
                    !rfc3339_is_after(connection.connected_at(), disconnected_at)
                })
            && execution
                .lease_expires_at
                .as_deref()
                .is_none_or(|expiry| !rfc3339_is_after(expiry, &now_rfc3339()))
        {
            // A frozen process can retain the socket from before suspension.
            // A newer connection may reuse a numeric ID after server restart;
            // it must still be allowed to describe and resume the attempt.
            return Ok(false);
        }
    }
    let Some(facts) = connection.snapshot().filter(|facts| {
        placement.owner_kind == PlacementOwnerKind::Server || !facts.workspace_incapable
    }) else {
        return Ok(false);
    };
    if placement.owner_kind == PlacementOwnerKind::Server {
        // A shared-mount provider can replay its terminal journal, but its
        // workspace is owned and described by the server backend. Keep the
        // attempt suspended until its retained terminal report is settled.
        if ExecutionRepo::list_running(&**db)
            .await?
            .iter()
            .any(|execution| {
                execution.workspace_id.as_deref() == Some(placement.workspace_id.as_str())
            })
        {
            return Ok(false);
        }
        let Some(task_service) = task_service else {
            return Ok(false);
        };
        let state = task_service
            .workspace_backend_router()
            .for_placement(placement)?
            .describe(placement)
            .await?;
        if !state.exists || state.head_sha.is_none() {
            annotate_owner_workspace_reset(db, placement, "Server workspace is missing").await?;
            return Err(ServiceError::WorkspaceResetRequired {
                task_id: placement.task_id.clone(),
                reason: "server workspace is missing".to_owned(),
            });
        }
        let described = api_types::WorkspaceDescribeResult {
            workspace_handle: placement.workspace_handle.clone().ok_or_else(|| {
                ServiceError::invalid_operation("disconnected placement has no handle")
            })?,
            generation: u64::try_from(placement.generation)
                .map_err(|_| ServiceError::invalid_operation("invalid placement generation"))?,
            exists: state.exists,
            head_sha: state.head_sha,
            dirty: state.dirty,
            branch: state.branch,
            locked: state.locked,
            active_execution_ids: state.active_execution_ids,
            journaled_execution_ids: state.journaled_execution_ids,
        };
        return complete_workspace_reconciliation(
            db,
            event_bus,
            registry,
            Some(task_service),
            placement,
            described,
            false,
            facts.connection_id,
        )
        .await;
    }
    if !registry.is_current(daemon_id, facts.connection_id) {
        return Ok(false);
    }
    let owner_client =
        crate::daemon_transport::workspace_client::DaemonWorkspaceClient::new(Arc::clone(registry))
            .with_receipts(Arc::clone(db));
    let db = &**db;
    let handle = placement
        .workspace_handle
        .as_deref()
        .ok_or_else(|| ServiceError::invalid_operation("disconnected placement has no handle"))?;
    let generation = u64::try_from(placement.generation)
        .map_err(|_| ServiceError::invalid_operation("invalid placement generation"))?;
    let reference = api_types::WorkspaceHandleReference {
        daemon_id: daemon_id.to_owned(),
        runtime_id: placement
            .runtime_id
            .clone()
            .ok_or_else(|| ServiceError::invalid_operation("placement has no runtime"))?,
        placement_id: placement.id.clone(),
        workspace_handle: handle.to_owned(),
        generation,
    };
    let mut state: api_types::WorkspaceDescribeResult = registry
        .send_request_for_connection(
            daemon_id,
            facts.connection_id,
            api_types::METHOD_WORKSPACE_DESCRIBE,
            api_types::WorkspaceDescribeParams {
                workspace: reference.clone(),
            },
            api_types::DEFAULT_DAEMON_COMMAND_TIMEOUT_SECS,
        )
        .await?;
    if !registry.is_current(daemon_id, facts.connection_id) {
        return Ok(false);
    }
    if state.workspace_handle != handle || state.generation != generation {
        return Err(ServiceError::invalid_operation(
            "stale_generation: workspace describe returned a different placement",
        ));
    }
    if !registry
        .drain_execution_journal(
            daemon_id,
            facts.connection_id,
            &state.journaled_execution_ids,
        )
        .await?
    {
        return Ok(false);
    }
    let now = Utc::now();
    let owner = crate::daemon_transport::execution_lease_owner(daemon_id, facts.connection_id);
    let mut active = false;
    let mut reconciled = placement.clone();
    for execution in ExecutionRepo::list_running(db).await? {
        if execution.workspace_id.as_deref() != Some(placement.workspace_id.as_str()) {
            continue;
        }
        if state.journaled_execution_ids.contains(&execution.id) {
            // A spawned sink may still be committing, or an ACK/drain may
            // have been interrupted. Stay suspended and let the sweep retry.
            return Ok(false);
        }
        if state.active_execution_ids.contains(&execution.id) {
            let resumed = WorkspacePlacementRepo::resume_disconnected_execution_lease(
                db,
                &reconciled,
                &execution,
                db::RenewExecutionLease {
                    execution_id: execution.id.clone(),
                    expected_version: execution.execution_version,
                    owner: owner.clone(),
                    lease_expires_at: (now + ChronoDuration::seconds(60)).to_rfc3339(),
                    now: now.to_rfc3339(),
                },
            )
            .await?;
            if !matches!(resumed, db::ExecutionLeaseMutation::Updated(_)) {
                return Ok(false);
            }
            active = true;
        } else {
            if !fail_owned_execution(
                db,
                event_bus,
                &reconciled,
                &execution,
                PlacementFailureCause::OwnerLostExecution,
            )
            .await?
            {
                return Ok(false);
            }
            reconciled = WorkspacePlacementRepo::update(
                db,
                placement_update(
                    &reconciled,
                    PlacementState::Disconnected,
                    Some(PlacementFailureCause::OwnerLostExecution),
                ),
            )
            .await?;
        }
    }
    if repair_owner_recovery(db, &reconciled).await?
        && reconciled.failure_cause != Some(PlacementFailureCause::OwnerLostExecution)
    {
        reconciled = WorkspacePlacementRepo::update(
            db,
            placement_update(
                &reconciled,
                PlacementState::Disconnected,
                Some(PlacementFailureCause::OwnerLostExecution),
            ),
        )
        .await?;
    }
    if !state.exists || state.head_sha.is_none() {
        // Do not recreate a worktree while an owner still reports a writer.
        if active {
            return Ok(false);
        }
        let workspace = db::WorkspaceRepo::get_by_id(db, &placement.workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", &placement.workspace_id))?;
        let base_sha = workspace
            .before_sha
            .ok_or_else(|| ServiceError::invalid_operation("workspace has no recovery base"))?;
        let operation_id = uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_URL,
            format!(
                "forge:workspace.recover:{}:{}:{}",
                placement.id,
                placement.generation,
                placement.disconnected_at.as_deref().unwrap_or("")
            )
            .as_bytes(),
        )
        .to_string();
        let prepared = match owner_client
            .prepare(
                daemon_id,
                api_types::WorkspacePrepareParams {
                    fence: api_types::WorkspaceMutationFence {
                        integration: api_types::WorkspaceIntegrationBinding::TaskStep,
                        daemon_id: reference.daemon_id.clone(),
                        runtime_id: reference.runtime_id.clone(),
                        placement_id: placement.id.clone(),
                        operation_id,
                        generation,
                        expected: api_types::WorkspaceOperationExpected::BaseSha {
                            sha: base_sha.clone(),
                        },
                    },
                    repo_location_id: placement.repo_location_id.clone(),
                    workspace_id: workspace.id,
                    task_id: workspace.task_id,
                    base_ref: base_sha,
                    branch: workspace.branch,
                },
            )
            .await
        {
            Ok(prepared) => prepared,
            Err(crate::daemon_transport::workspace_client::WorkspaceClientError::Transport(
                error,
            )) => return Err(error),
            Err(crate::daemon_transport::workspace_client::WorkspaceClientError::Daemon(error)) => {
                if error.code != api_types::INVALID_INPUT && error.code != "version_conflict" {
                    return Err(ServiceError::invalid_operation(format!(
                        "{}: {}",
                        error.code, error.message
                    )));
                }
                annotate_owner_workspace_reset(db, placement, &error.message).await?;
                return Err(ServiceError::WorkspaceResetRequired {
                    task_id: placement.task_id.clone(),
                    reason: error.message,
                });
            }
        };
        if prepared.workspace.generation != generation
            || prepared.workspace.workspace_handle.is_empty()
        {
            return Err(ServiceError::invalid_operation(
                "owner returned an invalid recovered workspace",
            ));
        }
        let mut recovered_reference = reference;
        recovered_reference.workspace_handle = prepared.workspace.workspace_handle;
        state = registry
            .send_request_for_connection(
                daemon_id,
                facts.connection_id,
                api_types::METHOD_WORKSPACE_DESCRIBE,
                api_types::WorkspaceDescribeParams {
                    workspace: recovered_reference.clone(),
                },
                api_types::DEFAULT_DAEMON_COMMAND_TIMEOUT_SECS,
            )
            .await?;
        if !registry.is_current(daemon_id, facts.connection_id) {
            return Ok(false);
        }
        if state.workspace_handle != recovered_reference.workspace_handle
            || state.generation != generation
        {
            return Err(ServiceError::invalid_operation(
                "owner changed the recovered workspace fence",
            ));
        }
        if !state.exists || state.head_sha.is_none() {
            annotate_owner_workspace_reset(
                db,
                placement,
                "Workspace owner reports a missing branch",
            )
            .await?;
            return Err(ServiceError::WorkspaceResetRequired {
                task_id: placement.task_id.clone(),
                reason: "owner workspace branch is missing".to_owned(),
            });
        }
        if !state.active_execution_ids.is_empty() || !state.journaled_execution_ids.is_empty() {
            return Ok(false);
        }
    }
    complete_workspace_reconciliation(
        db,
        event_bus,
        registry,
        task_service,
        &reconciled,
        state,
        active,
        facts.connection_id,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn complete_workspace_reconciliation(
    db: &SqliteDb,
    event_bus: &EventBus,
    registry: &DaemonConnectionRegistry,
    task_service: Option<&TaskService>,
    placement: &WorkspacePlacement,
    state: api_types::WorkspaceDescribeResult,
    active: bool,
    connection_id: u64,
) -> Result<bool> {
    let Some(daemon_id) = placement_execution_daemon_id(placement) else {
        return Ok(false);
    };
    let recorded_head = db
        .recorded_owner_workspace_head(db::RecordedOwnerWorkspaceHeadInput {
            placement_id: &placement.id,
            generation: placement.generation,
            workspace_id: &placement.workspace_id,
            reset_placement_id: &placement.id,
            reset_generation: placement.generation,
        })
        .await?
        .flatten();
    let recorded_head = match recorded_head {
        Some(head) => Some(head),
        None => db::WorkspaceRepo::get_by_id(db, &placement.workspace_id)
            .await?
            .and_then(|workspace| workspace.before_sha),
    };
    if !active
        && recorded_head
            .as_ref()
            .is_some_and(|head| state.head_sha.as_ref() != Some(head))
    {
        annotate_owner_workspace_reset(
            db,
            placement,
            "Workspace owner HEAD differs from recorded execution evidence",
        )
        .await?;
        return Err(ServiceError::WorkspaceResetRequired {
            task_id: placement.task_id.clone(),
            reason: "owner head differs from recorded execution evidence".to_owned(),
        });
    }
    if !registry.is_current(daemon_id, connection_id) {
        return Ok(false);
    }
    let cause = (placement.failure_cause == Some(PlacementFailureCause::OwnerLostExecution))
        .then_some(PlacementFailureCause::OwnerLostExecution);
    let mut update = placement_update(placement, PlacementState::Ready, cause);
    update.disconnected_at = Some(None);
    update.workspace_handle = Some(Some(state.workspace_handle.clone()));
    let mut transaction = db::begin_immediate(db.pool()).await?;
    let ready = WorkspacePlacementRepo::update_in_tx(db, &mut transaction, update).await?;
    let event = db::DomainEventRepo::append_event_in_tx(db, &mut transaction, &db::CreateDomainEvent {
        id: db::new_uuid_v4(), event_type: "workspace.owner_reconciled".to_owned(),
        entity_type: "workspace_placement".to_owned(), entity_id: ready.id.clone(),
        actor_type: "system".to_owned(), actor_id: None, scope_type: "task".to_owned(),
        scope_id: ready.task_id.clone(), correlation_id: ready.id.clone(), causation_id: None,
        causation_depth: 0, dedupe_key: Some(format!("workspace-ready:{}:{}", ready.id, ready.version)),
        payload_json: json!({"task_id": ready.task_id, "placement_id": ready.id, "head_sha": state.head_sha}).to_string(),
        created_at: now_rfc3339(),
    }).await?;
    db.resolve_reconciled_owner_attention_in_tx(
        &mut transaction,
        now_rfc3339(),
        &event.id,
        now_rfc3339(),
        format!("workspace-owner:{}", ready.id),
    )
    .await?;
    db.clear_reconciled_owner_waits_in_tx(
        &mut transaction,
        now_rfc3339(),
        &ready.task_id,
        &ready.task_id,
    )
    .await?;
    let exhausted_tasks: Vec<String> = db
        .owner_reconnect_exhausted_review_task_ids_in_tx(
            &mut transaction,
            &ready.task_id,
            &ready.task_id,
        )
        .await?;
    for task in exhausted_tasks {
        let window = format!("reconnect:{}:{}", ready.id, ready.version);
        db::budget::reset(
            &mut transaction,
            &task,
            db::budget::Kind::ReviewCiInfrastructure.key(),
            &window,
        )
        .await?;
        db.restart_owner_reconnect_review_barrier_in_tx(&mut transaction, &window, &task)
            .await?;
        db.produce_condition_in_tx(&mut transaction, &task, db::ConditionChange::Legacy)
            .await?;
    }
    db.restore_owner_reconnect_review_retry_in_tx(
        &mut transaction,
        now_rfc3339(),
        &ready.task_id,
        &ready.task_id,
    )
    .await?;
    db.resolve_owner_reconnect_review_attention_in_tx(
        &mut transaction,
        now_rfc3339(),
        now_rfc3339(),
        format!("review-ci:{}", ready.task_id),
        format!("task-owner-wait:{}", ready.task_id),
    )
    .await?;
    transaction.commit().await?;
    if let Some(task_service) = task_service {
        // A merge that waited for this machine goes on now, not at the
        // worker's next retry. Best effort: the worker's sweep and the
        // reconciler find the same Task without this call.
        // It may queue a Task command and wait for it, so it does not run
        // inside this reconciliation.
        let (service, task_id, placement_id) = (
            task_service.clone(),
            ready.task_id.clone(),
            ready.id.clone(),
        );
        tokio::spawn(async move {
            if let Err(error) = service.integration_owner_ready(&task_id).await {
                tracing::warn!(%placement_id, %task_id, %error, "integration was not woken for the reconnected owner; the sweep retries");
            }
        });
        // Cascades and terminal ACK retries run outside the owner permit.
        // A ready-placement sweep can redrive a crash here.
        if let Err(error) =
            finish_reconciled_cascades(db, registry, Some(task_service), &ready).await
        {
            if matches!(
                error,
                ServiceError::DaemonTimeout { .. } | ServiceError::DaemonUnavailable { .. }
            ) {
                return Err(error);
            }
            tracing::warn!(placement_id = %ready.id, %daemon_id, %error, "reconciled cascade remains pending");
        }
    }
    event_bus.publish(ForgeEvent {
        event_type: "reconciliation.event".to_owned(),
        entity_id: ready.task_id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::ReconciliationEvent {
            task_id: Some(ready.task_id),
            execution_id: None,
            reason: "workspace_owner_ready".to_owned(),
        },
    });
    Ok(true)
}

pub(super) async fn finish_reconciled_cascades(
    db: &SqliteDb,
    registry: &DaemonConnectionRegistry,
    task_service: Option<&TaskService>,
    placement: &WorkspacePlacement,
) -> Result<()> {
    let Some(task_service) = task_service else {
        return Ok(());
    };
    for execution_id in db
        .latest_terminal_workspace_execution_ids(&placement.workspace_id)
        .await?
    {
        let Some(execution) = ExecutionRepo::get_by_id(db, &execution_id).await? else {
            continue;
        };
        if owner_execution_failure_cause(&execution).is_none() {
            task_service
                .try_cascade_executor_completion(&execution_id)
                .await?;
        }
    }
    if let Some(daemon_id) = placement_execution_daemon_id(placement) {
        let registry = registry.clone();
        let daemon_id = daemon_id.to_owned();
        tokio::spawn(async move {
            if let Err(error) = registry.retry_retained_terminals(&daemon_id).await {
                tracing::warn!(%daemon_id, %error, "reconciled terminal acknowledgement remains pending");
            }
        });
    }
    Ok(())
}
