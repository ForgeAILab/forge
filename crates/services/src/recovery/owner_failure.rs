//! Owner failure annotation and cleanup receipts.

use super::*;

pub(super) async fn fail_placement_executions(
    db: &SqliteDb,
    event_bus: &EventBus,
    placement: &WorkspacePlacement,
    cause: PlacementFailureCause,
) -> Result<u64> {
    let mut count = 0;
    for execution in ExecutionRepo::list_running(db).await? {
        if execution.workspace_id.as_deref() == Some(placement.workspace_id.as_str())
            && fail_owned_execution(db, event_bus, placement, &execution, cause.clone()).await?
        {
            count += 1;
        }
    }
    Ok(count)
}

pub(super) async fn fail_owned_execution(
    db: &SqliteDb,
    event_bus: &EventBus,
    placement: &WorkspacePlacement,
    execution: &Execution,
    cause: PlacementFailureCause,
) -> Result<bool> {
    let Some(execution) = terminalize_owner_lost(
        db,
        placement,
        execution,
        &cause.to_string(),
        &api_types::Actor::system(api_types::SystemComponent::HeartbeatMonitor).display(),
        None,
    )
    .await?
    else {
        return Ok(false);
    };
    // Infrastructure settlement parks the Task without entering work/review
    // retry settlement. An explicit same-owner retry is the next admission.
    annotate_owner_recovery(db, &execution, &cause).await?;
    event_bus.publish(ForgeEvent {
        event_type: "reconciliation.event".to_owned(),
        entity_id: execution.task_id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::ReconciliationEvent {
            task_id: Some(execution.task_id),
            execution_id: Some(execution.id),
            reason: cause.to_string(),
        },
    });
    Ok(true)
}

/// The one terminal write for a run whose workspace owner is gone. `user_id`
/// attributes an owner's own decision (machine removal); the heartbeat passes none.
pub(super) async fn terminalize_owner_lost(
    db: &SqliteDb,
    placement: &WorkspacePlacement,
    execution: &Execution,
    cause: &str,
    stopped_by: &str,
    user_id: Option<&str>,
) -> Result<Option<Execution>> {
    let now = now_rfc3339();
    let mut input = crate::task_service::execution::ledger::terminal_with_ledger(
        TerminalizeExecution {
            execution_id: execution.id.clone(),
            expected_version: execution.execution_version,
            lease_owner: execution.lease_owner.clone(),
            status: ExecutionStatus::Failed,
            stop_reason: Some(Some(StopReason::DaemonDisconnected)),
            stopped_by: Some(Some(stopped_by.to_owned())),
            stopped_at: Some(Some(now.clone())),
            resume_policy: Some(Some(ResumePolicy::Manual)),
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            last_progress_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some(Some(
                json!({"cause": cause, "placement_id": placement.id,
            "daemon_id": placement_execution_daemon_id(placement).or(placement.daemon_id.as_deref())})
                .to_string(),
            )),
            executor_config_snapshot_json: None,
            updated_at: now.clone(),
            actor_type: if user_id.is_some() { "user" } else { "system" }.to_owned(),
            actor_id: user_id.map(str::to_owned),
            correlation_id: Some(placement.id.clone()),
            causation_id: None,
            causation_depth: 0,
            lease_disposition: ExecutionLeaseDisposition::Expire,
        },
        Vec::new(),
        None,
        None,
    );
    input.mark_unreplayable_pending_unsettled = true;
    Ok(
        match ExecutionRepo::terminalize_with_ledger(db, input).await? {
            ExecutionTerminalOutcome::Committed { execution, .. } => Some(execution),
            _ => None,
        },
    )
}

async fn annotate_owner_recovery(
    db: &SqliteDb,
    execution: &Execution,
    cause: &PlacementFailureCause,
) -> Result<()> {
    let Some(task) = TaskRepo::get_by_id(db, &execution.task_id, false).await? else {
        return Ok(());
    };
    let project = ProjectRepo::get_by_id(db, &task.project_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("project", &task.project_id))?;
    let mut transaction = db::begin_immediate(db.pool()).await?;
    let dedupe_key = format!("workspace-execution-recovery:{}", execution.id);
    let recorded: bool = db
        .owner_recovery_event_exists_in_tx(&mut transaction, &dedupe_key)
        .await?;
    if recorded {
        transaction.rollback().await?;
        return Ok(());
    }
    let actor = api_types::Actor::system(api_types::SystemComponent::HeartbeatMonitor);
    let workflow =
        WorkflowEngine::resolve_workflow_for_task(&task, &project.workflow_definition, &actor);
    let existing_reason = task
        .error_annotation
        .as_deref()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .and_then(|annotation| annotation["blocking_reason"].as_str().map(str::to_owned));
    let can_annotate = task.failed_json.is_none()
        && workflow.state_kind(&task.status) != Some(api_types::StateKind::Terminal)
        && (task.error_annotation.is_none()
            || matches!(
                existing_reason.as_deref(),
                Some("owner_disconnected_timeout" | "owner_lost_execution")
            ));
    let annotation = json!({"type": api_types::FailureKind::RecoveryRequired,
            "blocking_reason": cause.to_string(), "blocked_by": actor.display(),
            "blocked_at": now_rfc3339(), "blocked_execution_id": execution.id,
            "message": format!("Workspace owner recovery required: {cause}"),
    });
    if can_annotate {
        // The heartbeat runs outside the Task lease, so this blocking
        // annotation is usually queued. It must never be dropped by a status
        // change, so it is identity-fenced and its predicate re-derives
        // `can_annotate` when it runs: no failure, a non-terminal status and
        // no newer foreign annotation.
        let terminal = workflow
            .states
            .iter()
            .filter(|state| state.kind == api_types::StateKind::Terminal)
            .map(|state| state.name.clone())
            .collect::<Vec<_>>();
        let updated = db
            .annotate_owner_recovery_in_tx(
                &mut transaction,
                &task.id,
                db::AnnotateOwnerRecoveryInTxInput {
                    annotation: annotation.to_string(),
                    updated_at: now_rfc3339(),
                    task_id: &task.id,
                    expected_version: task.version,
                    terminal_states_json: serde_json::to_string(&terminal)
                        .expect("terminal states serialize"),
                },
            )
            .await?;
        if updated.applied().is_some_and(|rows| rows != 1) {
            return Err(db::DbError::VersionConflict.into());
        }
    }
    // The marker commits with the annotation. A sweep can repair the earlier
    // terminal -> annotation crash window, without undoing a later user retry.
    db::DomainEventRepo::append_event_in_tx(
        db,
        &mut transaction,
        &db::CreateDomainEvent {
            id: db::new_uuid_v4(),
            event_type: "workspace.execution_recovery_required".to_owned(),
            entity_type: "execution".to_owned(),
            entity_id: execution.id.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "task".to_owned(),
            scope_id: task.id.clone(),
            correlation_id: execution.id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(dedupe_key),
            payload_json: json!({"task_id": task.id, "execution_id": execution.id,
            "failure_cause": cause.to_string(), "annotated": can_annotate})
            .to_string(),
            created_at: now_rfc3339(),
        },
    )
    .await?;
    transaction.commit().await?;
    Ok(())
}

pub(crate) fn owner_execution_failure_cause(
    execution: &Execution,
) -> Option<PlacementFailureCause> {
    if execution.status != ExecutionStatus::Failed
        || execution.stop_reason != Some(StopReason::DaemonDisconnected)
    {
        return None;
    }
    let error: serde_json::Value = serde_json::from_str(execution.error.as_deref()?).ok()?;
    match error["cause"].as_str()? {
        "owner_lost_execution" => Some(PlacementFailureCause::OwnerLostExecution),
        "owner_disconnected_timeout" => Some(PlacementFailureCause::OwnerDisconnectedTimeout),
        _ => None,
    }
}

pub(super) async fn repair_owner_recovery(
    db: &SqliteDb,
    placement: &WorkspacePlacement,
) -> Result<bool> {
    let execution_ids = db
        .latest_owner_failed_execution_ids(&placement.workspace_id)
        .await?;
    let mut lost_execution = false;
    for execution_id in execution_ids {
        let Some(execution) = ExecutionRepo::get_by_id(db, &execution_id).await? else {
            continue;
        };
        let Some(cause) = owner_execution_failure_cause(&execution) else {
            continue;
        };
        lost_execution |= cause == PlacementFailureCause::OwnerLostExecution;
        annotate_owner_recovery(db, &execution, &cause).await?;
    }
    Ok(lost_execution)
}

pub(super) async fn annotate_owner_workspace_reset(
    db: &SqliteDb,
    placement: &WorkspacePlacement,
    message: &str,
) -> Result<()> {
    let Some(task) = TaskRepo::get_by_id(db, &placement.task_id, false).await? else {
        return Ok(());
    };
    let project = ProjectRepo::get_by_id(db, &task.project_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("project", &task.project_id))?;
    let actor = api_types::Actor::system(api_types::SystemComponent::HeartbeatMonitor);
    let workflow =
        WorkflowEngine::resolve_workflow_for_task(&task, &project.workflow_definition, &actor);
    if task.failed_json.is_some()
        || workflow.state_kind(&task.status) == Some(api_types::StateKind::Terminal)
        || task
            .error_annotation
            .as_deref()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
            .is_some_and(|annotation| {
                annotation["blocking_reason"] == "owner_workspace_reset_required"
            })
    {
        return Ok(());
    }
    let annotation = db::task_condition::channels::ErrorAnnotation::WorkspaceResetRequired(
        db::task_condition::channels::WorkspaceResetAnnotation {
            blocked_at: now_rfc3339(),
            blocked_by: actor.display(),
            blocking_reason: "owner_workspace_reset_required".into(),
            message: message.into(),
        },
    );
    let annotation = serde_json::to_string(&annotation).map_err(|error| {
        ServiceError::invalid_operation(format!(
            "failed to serialize workspace-reset annotation: {error}"
        ))
    })?;
    TaskRepo::update_status(
        db,
        UpdateTaskStatus {
            id: task.id.clone(),
            expected_version: task.version,
            status: task.status,
            assignee_id: None,
            error_annotation: Some(Some(annotation)),
            blocked_json: None,
            failed_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await?;
    if db
        .record_task_diagnostic_text(
            &task.id,
            db::RecordTaskDiagnosticText {
                kind: "message",
                source: "diagnostic",
                owner: &db::DiagnosticOwner::Reason {
                    reason: db::ParkReason::Failure {
                        failure_kind: api_types::FailureKind::WorkspaceResetRequired,
                    },
                },
                text: message,
            },
        )
        .await
        .is_err()
    {
        tracing::warn!(task_id = %task.id, "Diagnostic text capture failed; the legacy annotation retains it");
    }
    tracing::warn!(placement_id = %placement.id, daemon_id = ?placement_execution_daemon_id(placement), message, "workspace owner reset required");
    Ok(())
}

pub(crate) async fn apply_owner_cleanup(
    db: &SqliteDb,
    daemon_id: &str,
    notification: &api_types::WorkspaceCleanupResult,
) -> Result<crate::daemon_transport::DaemonTerminalDisposition> {
    use crate::daemon_transport::DaemonTerminalDisposition;
    if !notification.cleaned
        || notification.entry_id.trim().is_empty()
        || notification.operation_id.trim().is_empty()
    {
        return Ok(DaemonTerminalDisposition::Ignore);
    }
    for state in [PlacementState::Cleaning, PlacementState::Cleaned] {
        for placement in
            WorkspacePlacementRepo::list_by_daemon_and_state(db, daemon_id, state).await?
        {
            if placement.workspace_handle.as_deref() != Some(notification.workspace_handle.as_str())
                || u64::try_from(placement.generation).ok() != Some(notification.generation)
            {
                continue;
            }
            if placement.state == PlacementState::Cleaned {
                return Ok(DaemonTerminalDisposition::Acknowledge);
            }
            let mut transaction = db::begin_immediate(db.pool()).await?;
            WorkspacePlacementRepo::update_in_tx(
                db,
                &mut transaction,
                placement_update(&placement, PlacementState::Cleaned, None),
            )
            .await?;
            db.mark_owner_workspace_cleaned_in_tx(
                &mut transaction,
                now_rfc3339(),
                &placement.workspace_id,
            )
            .await?;
            transaction.commit().await?;
            return Ok(DaemonTerminalDisposition::Acknowledge);
        }
    }
    Ok(DaemonTerminalDisposition::Ignore)
}

pub(super) async fn reconcile_owner_receipts(
    db: &Arc<SqliteDb>,
    registry: &Arc<DaemonConnectionRegistry>,
    daemon_id: &str,
) -> Result<()> {
    use crate::daemon_transport::workspace_client::{DaemonWorkspaceClient, WorkspaceClientError};
    let client = DaemonWorkspaceClient::new(Arc::clone(registry)).with_receipts(Arc::clone(db));
    let map_error = |error| match error {
        WorkspaceClientError::Transport(error) => error,
        WorkspaceClientError::Daemon(error) => ServiceError::invalid_operation(format!(
            "owner operation reconciliation failed: {}",
            error.message
        )),
    };
    client
        .reconcile_pending_operations(daemon_id)
        .await
        .map_err(map_error)?;
    client
        .retry_acknowledgements(daemon_id)
        .await
        .map_err(map_error)?;
    Ok(())
}

#[cfg(test)]
mod condition_tests;
