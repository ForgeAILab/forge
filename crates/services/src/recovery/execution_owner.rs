//! Daemon execution ownership and disconnect settlement.

use super::*;

pub(crate) async fn execution_is_remote_owned(
    db: &SqliteDb,
    execution: &Execution,
) -> Result<bool> {
    let Some((_, daemon)) = resolve_execution_daemon(db, execution).await? else {
        return Ok(false);
    };
    Ok(!is_embedded_daemon_machine(&daemon.machine_id))
}

pub(crate) async fn fail_execution_daemon_disconnected(
    db: &SqliteDb,
    event_bus: &EventBus,
    task_service: Option<&TaskService>,
    input: FailDaemonDisconnectedExecution<'_>,
) -> Result<Option<Execution>> {
    let FailDaemonDisconnectedExecution {
        execution,
        daemon_id,
        error_message,
        stopped_by,
        reconciliation_reason,
    } = input;
    let now = now_rfc3339();
    let mut terminal_input = crate::task_service::execution::ledger::terminal_with_ledger(
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
            error: Some(Some(error_message)),
            executor_config_snapshot_json: None,
            updated_at: now,
            actor_type: "system".to_owned(),
            actor_id: None,
            correlation_id: None,
            causation_id: None,
            causation_depth: 0,
            lease_disposition: ExecutionLeaseDisposition::Expire,
        },
        Vec::new(),
        None,
        None,
    );
    terminal_input.preserve_pending_settlement = true;
    let outcome = ExecutionRepo::terminalize_with_ledger(db, terminal_input).await?;

    let ExecutionTerminalOutcome::Committed {
        execution: updated, ..
    } = outcome
    else {
        // Another terminal caller won the version/owner CAS. Its durable
        // event, lease disposition, and Task cascade are authoritative; this
        // stale daemon observation must not emit a second failure.
        return Ok(None);
    };

    discard_execution_plan_artifacts(db, &updated).await;

    event_bus.publish(ForgeEvent {
        event_type: "execution.daemon_disconnected".to_owned(),
        entity_id: updated.id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::ExecutionDaemonDisconnected {
            task_id: updated.task_id.clone(),
            execution_id: updated.id.clone(),
            daemon_id: daemon_id.to_owned(),
        },
    });
    event_bus.publish(ForgeEvent {
        event_type: "reconciliation.event".to_owned(),
        entity_id: updated.task_id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::ReconciliationEvent {
            task_id: Some(updated.task_id.clone()),
            execution_id: Some(updated.id.clone()),
            reason: reconciliation_reason.to_owned(),
        },
    });

    if let Some(task_service) = task_service {
        if let Err(error) = cascade_recovered_execution(
            task_service,
            &updated,
            stalled_execution_should_block_task(&updated),
        )
        .await
        {
            tracing::warn!(
                execution_id = %updated.id,
                task_id = %updated.task_id,
                %error,
                "failed to cascade daemon-disconnected execution"
            );
        }
    }

    Ok(Some(updated))
}

pub(crate) async fn suspend_expired_remote_execution(
    db: &SqliteDb,
    event_bus: &EventBus,
    execution: &Execution,
) -> Result<bool> {
    let now = now_rfc3339();
    if execution.status != ExecutionStatus::Running
        || execution
            .hard_deadline_at
            .as_deref()
            .is_some_and(|deadline| !rfc3339_is_after(deadline, &now))
    {
        return Ok(false);
    }
    let Some(workspace_id) = execution.workspace_id.as_deref() else {
        return Ok(false);
    };
    let Some(placement) = WorkspacePlacementRepo::get_by_workspace_id(db, workspace_id).await?
    else {
        return Ok(false);
    };
    if placement.owner_kind == PlacementOwnerKind::Server {
        let Some(daemon_id) = placement.execution_daemon_id.as_deref() else {
            return Ok(false);
        };
        if DaemonRepo::get_by_id(db, daemon_id)
            .await?
            .is_none_or(|daemon| is_embedded_daemon_machine(&daemon.machine_id))
        {
            return Ok(false);
        }
    }
    if let Some(disconnected) =
        WorkspacePlacementRepo::suspend_expired_execution_lease(db, &placement, execution, &now)
            .await?
    {
        publish_disconnected_placement(db, event_bus, disconnected).await?;
    }
    // A renewal, socket disconnect, or terminal result may win the CAS. None
    // makes heartbeat expiry a work failure for this remote-owned attempt.
    Ok(true)
}

pub(crate) async fn execution_lease_is_suspended(
    db: &SqliteDb,
    execution: &Execution,
) -> Result<bool> {
    let Some(workspace_id) = execution.workspace_id.as_deref() else {
        return Ok(false);
    };
    Ok(
        WorkspacePlacementRepo::get_by_workspace_id(db, workspace_id)
            .await?
            .is_some_and(|placement| {
                placement.state == PlacementState::Disconnected
                    && execution
                        .hard_deadline_at
                        .as_deref()
                        .is_none_or(|deadline| rfc3339_is_after(deadline, &now_rfc3339()))
            }),
    )
}

pub(crate) async fn execution_placement_is_disconnected(
    db: &SqliteDb,
    execution: &Execution,
) -> Result<bool> {
    let Some(workspace_id) = execution.workspace_id.as_deref() else {
        return Ok(false);
    };
    Ok(
        WorkspacePlacementRepo::get_by_workspace_id(db, workspace_id)
            .await?
            .is_some_and(|placement| placement.state == PlacementState::Disconnected),
    )
}

pub(crate) async fn disconnect_daemon_placements(
    db: &SqliteDb,
    event_bus: &EventBus,
    daemon_id: &str,
) -> Result<u64> {
    if DaemonRepo::get_by_id(db, daemon_id)
        .await?
        .is_some_and(|daemon| is_embedded_daemon_machine(&daemon.machine_id))
    {
        return Ok(0);
    }
    let placements = WorkspacePlacementRepo::list_by_state(db, PlacementState::Ready).await?;
    let mut disconnected = 0;
    for placement in placements {
        if placement_execution_daemon_id(&placement) != Some(daemon_id) {
            continue;
        }
        let mut update = placement_update(
            &placement,
            PlacementState::Disconnected,
            Some(PlacementFailureCause::OwnerDisconnected),
        );
        update.disconnected_at = Some(Some(now_rfc3339()));
        match WorkspacePlacementRepo::update(db, update).await {
            Ok(placement) => {
                publish_disconnected_placement(db, event_bus, placement).await?;
                disconnected += 1;
            }
            Err(db::DbError::VersionConflict) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(disconnected)
}

async fn publish_disconnected_placement(
    db: &SqliteDb,
    event_bus: &EventBus,
    placement: WorkspacePlacement,
) -> Result<()> {
    record_disconnected_attention(db, &placement).await?;
    event_bus.publish(ForgeEvent {
        event_type: "workspace.disconnected".to_owned(),
        entity_id: placement.workspace_id,
        timestamp: event_timestamp(),
        context: EventContext::ReconciliationEvent {
            task_id: Some(placement.task_id),
            execution_id: None,
            reason: "owner_disconnected".to_owned(),
        },
    });
    Ok(())
}

pub(super) async fn record_disconnected_attention(
    db: &SqliteDb,
    placement: &WorkspacePlacement,
) -> Result<()> {
    let daemon_id = placement_execution_daemon_id(placement);
    let incident_current: bool = db
        .workspace_disconnect_incident_is_current(
            format!("workspace-owner:{}", placement.id),
            format!(
                "workspace-disconnect:{}:{}",
                placement.id,
                placement
                    .disconnected_at
                    .as_deref()
                    .unwrap_or(&placement.updated_at)
            ),
        )
        .await?;
    if incident_current {
        return Ok(());
    }
    let Some(task) = TaskRepo::get_by_id(db, &placement.task_id, false).await? else {
        return Ok(());
    };
    let event = db::DomainEventRepo::append_event(
        db,
        db::CreateDomainEvent {
            id: db::new_uuid_v4(),
            event_type: "workspace.owner_disconnected".to_owned(),
            entity_type: "workspace_placement".to_owned(),
            entity_id: placement.id.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: task.project_id.clone(),
            correlation_id: placement.id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!(
                "workspace-disconnect:{}:{}",
                placement.id,
                placement
                    .disconnected_at
                    .as_deref()
                    .unwrap_or(&placement.updated_at)
            )),
            payload_json: json!({"task_id": task.id, "placement_id": placement.id,
            "daemon_id": daemon_id, "failure_cause": "owner_disconnected"})
            .to_string(),
            created_at: now_rfc3339(),
        },
    )
    .await?;
    db::AttentionRepo::insert_attention(
        db,
        db::CreateAttentionProjection {
            id: db::new_uuid_v4(),
            attention_type: "runtime_offline".to_owned(),
            scope_type: "project".to_owned(),
            scope_id: task.project_id,
            identity_id: placement.agent_id.clone(),
            source_event_id: event.id,
            priority: 70,
            status: "open".to_owned(),
            summary: "Workspace owner disconnected; waiting on the same machine".to_owned(),
            details_json: json!({"task": {"id": task.id, "title": task.title},
            "placement_id": placement.id, "daemon_id": daemon_id,
            "disconnected_at": placement.disconnected_at, "failure_cause": "owner_disconnected",
            "recovery": {"actions": ["reexecute", "cancel_task"], "automatic_retry": false}})
            .to_string(),
            dedupe_key: format!("workspace-owner:{}", placement.id),
            occurred_at: event.created_at,
            updated_at: now_rfc3339(),
            acknowledged_at: None,
            snoozed_until: None,
            resolved_at: None,
            updated_by_user_id: None,
            recommended_action: "reconnect_owner".to_owned(),
            source_sequence: Some(event.sequence),
        },
    )
    .await?;
    Ok(())
}

pub(crate) async fn reconcile_daemon_report_executions(
    db: &SqliteDb,
    event_bus: &EventBus,
    task_service: Option<&TaskService>,
    daemon: &Daemon,
    active_execution_ids: &[String],
) -> Result<u64> {
    if is_embedded_daemon_machine(&daemon.machine_id) {
        return Ok(0);
    }

    let created_before = (Utc::now()
        - ChronoDuration::seconds(DAEMON_REPORT_RECONCILE_MIN_AGE.as_secs() as i64))
    .to_rfc3339();
    let execution_ids = db
        .unplaced_daemon_running_execution_ids(&created_before, &daemon.id)
        .await?;

    let mut interrupted = 0_u64;
    for execution_id in execution_ids {
        if active_execution_ids.contains(&execution_id) {
            continue;
        }
        let Some(execution) = ExecutionRepo::get_by_id(db, &execution_id).await? else {
            continue;
        };
        let Some(_updated) = fail_execution_daemon_disconnected(
            db,
            event_bus,
            task_service,
            FailDaemonDisconnectedExecution {
                execution: &execution,
                daemon_id: &daemon.id,
                error_message: "daemon no longer running this execution".to_owned(),
                stopped_by: &api_types::Actor::system(api_types::SystemComponent::DaemonReport)
                    .display(),
                reconciliation_reason: "daemon_disconnected",
            },
        )
        .await?
        else {
            continue;
        };
        interrupted += 1;
    }
    Ok(interrupted)
}
