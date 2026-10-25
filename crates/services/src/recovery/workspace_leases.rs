//! Workspace lease renewal and expiry.

use super::*;

const WORKSPACE_LEASE_RENEW_WINDOW_SECONDS: i64 = 5 * 60;
const WORKSPACE_LEASE_EXTENSION_SECONDS: i64 = 15 * 60;

pub(super) async fn renew_workspace_leases(db: &SqliteDb) -> Result<u64> {
    let now = chrono::Utc::now();
    let renewed = WorkspaceLeaseRepo::renew_active(
        db,
        &now.to_rfc3339(),
        &(now + chrono::Duration::seconds(WORKSPACE_LEASE_RENEW_WINDOW_SECONDS)).to_rfc3339(),
        &(now + chrono::Duration::seconds(WORKSPACE_LEASE_EXTENSION_SECONDS)).to_rfc3339(),
        500,
    )
    .await?;
    if !renewed.is_empty() {
        tracing::debug!(
            renewed_leases = renewed.len(),
            "renewed active WorkspaceLeases"
        );
    }
    Ok(renewed.len() as u64)
}

/// Expire scheduler grants and, during the live heartbeat pass, stop any
/// execution that lost its authority.  Startup recovery deliberately leaves
/// the execution running long enough for `recover_task` to apply its normal
/// requeue/block policy; the heartbeat path terminalizes it immediately.
pub(super) async fn expire_workspace_leases(
    db: &SqliteDb,
    event_bus: &EventBus,
    task_executor: Option<&dyn TaskExecutor>,
    task_service: Option<&TaskService>,
    terminalize_running: bool,
) -> Result<u64> {
    let expired = if terminalize_running {
        // The live monitor must not mutate a WorkspaceLease before it has
        // won the execution terminal CAS.  There is no bulk read API for
        // leases yet, so inspect the bounded running set and select only
        // active grants whose execution is still running.  `terminalize`
        // then closes the matching grant in its own transaction.  Startup
        // recovery may continue using the standalone expiry mutation because
        // it deliberately leaves running attempts for crash recovery.
        let now = now_rfc3339();
        let running = ExecutionRepo::list_running(db).await?;
        let mut candidates = Vec::new();
        for execution in running {
            if execution_lease_is_suspended(db, &execution).await? {
                continue;
            }
            let Some(lease) =
                WorkspaceLeaseRepo::get_active_for_task(db, &execution.task_id).await?
            else {
                continue;
            };
            if lease.execution_id == execution.id && lease.expires_at <= now {
                candidates.push(lease);
            }
        }
        candidates
    } else {
        WorkspacePlacementRepo::expire_unsuspended_workspace_leases(db, &now_rfc3339(), 500).await?
    };
    let mut expired_count = if terminalize_running {
        0
    } else {
        expired.len() as u64
    };
    if !terminalize_running {
        return Ok(expired_count);
    }

    for lease in expired {
        let Some(execution) = ExecutionRepo::get_by_id(db, &lease.execution_id).await? else {
            continue;
        };
        if execution.status != ExecutionStatus::Running {
            continue;
        }

        let now = now_rfc3339();
        let mut terminal_input = crate::task_service::execution::ledger::terminal_with_ledger(
            TerminalizeExecution {
                execution_id: execution.id.clone(),
                expected_version: execution.execution_version,
                lease_owner: execution.lease_owner.clone(),
                status: ExecutionStatus::Failed,
                stop_reason: Some(Some(StopReason::ExecutionStalled)),
                stopped_by: Some(Some(
                    api_types::Actor::system(api_types::SystemComponent::HeartbeatMonitor)
                        .display(),
                )),
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
                error: Some(Some("scheduler WorkspaceLease expired".to_owned())),
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
        terminal_input.mark_unreplayable_pending_unsettled = execution
            .lease_owner
            .as_deref()
            .is_none_or(|owner| !owner.starts_with("daemon:"));
        terminal_input.preserve_pending_settlement = execution
            .lease_owner
            .as_deref()
            .is_some_and(|owner| owner.starts_with("daemon:"));
        let outcome = match ExecutionRepo::terminalize_with_ledger(db, terminal_input).await {
            Ok(outcome) => outcome,
            Err(error) => {
                tracing::warn!(
                    execution_id = %execution.id,
                    %error,
                    "failed to stop execution after WorkspaceLease expiry"
                );
                continue;
            }
        };

        let ExecutionTerminalOutcome::Committed {
            execution: updated, ..
        } = outcome
        else {
            // A concurrent completion/cancellation/monitor winner owns the
            // terminal event and lease disposition. Do not duplicate any
            // downstream recovery side effect for this expired grant.
            continue;
        };

        discard_execution_plan_artifacts(db, &updated).await;

        expired_count += 1;

        if let Some(task_executor) = task_executor {
            if !execution_is_remote_owned(db, &updated)
                .await
                .unwrap_or(false)
            {
                if let Err(error) = task_executor.cancel(&updated.id).await {
                    tracing::warn!(
                        execution_id = %updated.id,
                        %error,
                        "failed to cancel execution after WorkspaceLease expiry"
                    );
                }
            }
        }

        event_bus.publish(ForgeEvent {
            event_type: "execution.stalled".to_owned(),
            entity_id: updated.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ExecutionStalled {
                task_id: updated.task_id.clone(),
                execution_id: updated.id.clone(),
                stale_before: lease.expires_at.clone(),
            },
        });
        event_bus.publish(ForgeEvent {
            event_type: "reconciliation.event".to_owned(),
            entity_id: updated.task_id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ReconciliationEvent {
                task_id: Some(updated.task_id.clone()),
                execution_id: Some(updated.id.clone()),
                reason: "workspace_lease_expired".to_owned(),
            },
        });
        if let Some(task_service) = task_service {
            if let Err(error) = cascade_recovered_execution(task_service, &updated, true).await {
                tracing::warn!(
                    execution_id = %updated.id,
                    task_id = %updated.task_id,
                    %error,
                    "failed to cascade expired WorkspaceLease execution"
                );
            }
        }
    }

    Ok(expired_count)
}
