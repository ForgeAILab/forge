//! Removed-machine Task settlement.

use super::*;
use db::task_condition::channels;

/// What the removal transaction recorded for one Task before it released the
/// machine's placements and retired its Agents.
#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct RemovedMachineFacts {
    pub actor_id: String,
    pub hostname: String,
    /// Runs that were live on the machine.
    #[serde(default)]
    pub executions: Vec<String>,
    /// The Task's own workspace lived on the machine and was released.
    #[serde(default)]
    pub workspace_lost: bool,
    /// The Task's workspace was owned by, or executing on, the machine.
    #[serde(default)]
    pub placed: bool,
}

impl RemovedMachineFacts {
    /// Removal is the owner's decision; settlement is attributed to them.
    pub(crate) fn actor(&self) -> api_types::Actor {
        api_types::Actor::User {
            user_id: Some(self.actor_id.clone()),
            source: api_types::UserActionSource::Api,
            delegated: false,
        }
    }
}

const OWNER_LOSS_REASONS: [&str; 3] = [
    "owner_disconnected_timeout",
    "owner_lost_execution",
    "owner_workspace_reset_required",
];

/// Runs under the affected Task's normal lease, including after a restart. The
/// removal transaction changes no Task row: it releases the machine's
/// placements and records the facts this step settles.
///
/// A removed owner is permanent loss, so nothing here waits for it. Live runs
/// fail as daemon-disconnected, an owner wait or owner-recovery annotation is
/// replaced by `machine_removed`, and a retired Agent is unassigned through the
/// archive sweep. The caller then admits the Task again through its queued
/// action, where placement selects another machine or parks it.
pub(crate) async fn settle_removed_machine_task(
    db: &SqliteDb,
    event_bus: &EventBus,
    task_id: &str,
    daemon_id: &str,
    facts: &RemovedMachineFacts,
) -> Result<RemovedMachineSettlement> {
    db::task_writer::debug_assert_task_lease(task_id, "machine removal settlement");
    let mut settlement = RemovedMachineSettlement {
        lost_work: false,
        role_events: Vec::new(),
    };
    if !db.daemon_removed(daemon_id).await? {
        return Ok(settlement);
    }
    db.clear_removed_machine_cancel_annotation(
        task_id,
        db::ClearRemovedMachineCancelAnnotationInput {
            updated_at: now_rfc3339(),
            task_id,
            workspace_task_id: task_id,
            step_task_id: task_id,
            execution_task_id: task_id,
        },
    )
    .await?;
    let remaining = db.task_pending_remote_cancel_machines(task_id).await?;
    if !remaining.is_empty() {
        if let Some(task) = TaskRepo::get_by_id(db, task_id, false).await? {
            if let Some(raw) = task.error_annotation.as_deref() {
                if let Ok(mut annotation) = serde_json::from_str::<serde_json::Value>(raw) {
                    if annotation["blocking_reason"] == "pending_remote_cancel" {
                        let machines = remaining.join(", ");
                        let blocked_by = format!("machine:{machines}");
                        let message = format!("Waiting for machine {machines} to confirm its remote work has stopped. Reconnect that machine to finish cleanup.");
                        let typed = serde_json::from_str::<channels::ErrorAnnotation>(raw)
                            .ok()
                            .and_then(|mut typed| {
                                match &mut typed.shape {
                                    channels::AnnotationShape::RemoteCancel(value) => {
                                        value.blocked_by = blocked_by.clone();
                                        value.message = message.clone();
                                    }
                                    channels::AnnotationShape::Blocking(value) => {
                                        value.blocked_by = Some(blocked_by.clone());
                                        value.message = Some(message.clone());
                                    }
                                    channels::AnnotationShape::TimedBlock(value) => {
                                        value.blocked_by = blocked_by.clone();
                                        value.message = message.clone();
                                    }
                                    channels::AnnotationShape::RecoveryExecution(value) => {
                                        value.blocked_by = blocked_by.clone();
                                        value.message = message.clone();
                                    }
                                    _ => return None,
                                }
                                Some(
                                    serde_json::to_value(typed)
                                        .expect("condition channel serializes"),
                                )
                            });
                        if let Some(typed) = typed {
                            annotation = typed;
                        } else {
                            annotation["blocked_by"] = json!(blocked_by);
                            annotation["message"] = json!(message);
                        }
                        db.refresh_remaining_machine_cancel_annotation(
                            task_id,
                            annotation.to_string(),
                            now_rfc3339(),
                            task_id,
                            raw,
                        )
                        .await?;
                    }
                }
            }
        }
    }
    // A Task may be waiting on this machine's environment, provisioning attempt
    // or reconnection. Wake its existing scheduler path instead of leaving a
    // delay that can no longer end.
    for (key, owner) in [
        ("environment_wait", "environment_wait.machine.daemon_id"),
        ("owner_wait", "owner_wait.daemon_id"),
    ] {
        db.clear_removed_machine_metadata_wait(task_id, key, owner, task_id, daemon_id)
            .await?;
    }
    let actor = facts.actor();
    for execution_id in &facts.executions {
        let Some(execution) = ExecutionRepo::get_by_id(db, execution_id).await? else {
            continue;
        };
        if execution.task_id != task_id {
            continue;
        }
        if execution.status != ExecutionStatus::Running {
            // A replayed step still reports the loss it already settled.
            settlement.lost_work |= execution.status == ExecutionStatus::Failed
                && execution.error.as_deref().is_some_and(|error| {
                    serde_json::from_str::<serde_json::Value>(error)
                        .is_ok_and(|error| error["cause"] == MACHINE_REMOVED_REASON)
                });
            continue;
        }
        let placement = match execution.workspace_id.as_deref() {
            Some(id) => WorkspacePlacementRepo::get_by_workspace_id(db, id).await?,
            None => None,
        };
        let failed = match placement {
            Some(placement) => {
                let failed = terminalize_owner_lost(
                    db,
                    &placement,
                    &execution,
                    MACHINE_REMOVED_REASON,
                    &actor.display(),
                    Some(&facts.actor_id),
                )
                .await?;
                if let Some(failed) = failed.as_ref() {
                    event_bus.publish(ForgeEvent {
                        event_type: "reconciliation.event".to_owned(),
                        entity_id: failed.task_id.clone(),
                        timestamp: event_timestamp(),
                        context: EventContext::ReconciliationEvent {
                            task_id: Some(failed.task_id.clone()),
                            execution_id: Some(failed.id.clone()),
                            reason: MACHINE_REMOVED_REASON.to_owned(),
                        },
                    });
                }
                failed
            }
            None => {
                fail_execution_daemon_disconnected(
                    db,
                    event_bus,
                    None,
                    FailDaemonDisconnectedExecution {
                        execution: &execution,
                        daemon_id,
                        error_message:
                            json!({"cause": MACHINE_REMOVED_REASON, "daemon_id": daemon_id})
                                .to_string(),
                        stopped_by: &actor.display(),
                        reconciliation_reason: MACHINE_REMOVED_REASON,
                    },
                )
                .await?
            }
        };
        settlement.lost_work |= failed.is_some();
    }
    let Some(task) = TaskRepo::get_by_id(db, task_id, false).await? else {
        return Ok(settlement);
    };
    let reason = task
        .error_annotation
        .as_deref()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .and_then(|annotation| annotation["blocking_reason"].as_str().map(str::to_owned));
    // An accepted "retry on the workspace owner" can never run: that owner is
    // gone. It is re-accepted below as an ordinary retry on another machine.
    let parked = crate::deferred_dispatch::queued_recovery(&task);
    let stale_owner_retry = parked
        .as_ref()
        .is_some_and(|queued| queued.request.offer.reason == "owner_reconcile");
    if stale_owner_retry {
        db.clear_removed_machine_queued_recovery(
            task_id,
            format!("$.{}", crate::deferred_dispatch::QUEUED_RECOVERY_KEY),
            now_rfc3339(),
            task_id,
        )
        .await?;
    }
    settlement.lost_work |= stale_owner_retry
        || (facts.placed
            && reason
                .as_deref()
                .is_some_and(|reason| OWNER_LOSS_REASONS.contains(&reason)));
    if settlement.lost_work && (parked.is_none() || stale_owner_retry) {
        annotate_machine_removed(db, &task, facts, &actor).await?;
    }
    // Retired Agents leave their Tasks through the archive sweep, so each Task
    // follows the same unassigned path as any other archived Agent's.
    let retired: Vec<String> = db
        .retired_machine_task_agent_ids(daemon_id, task_id, task_id)
        .await?;
    for agent_id in retired {
        let mut transaction = db::begin_immediate(db.pool()).await?;
        settlement
            .role_events
            .extend(TaskService::on_agent_deleted_in_tx(db, &mut transaction, &agent_id).await?);
        transaction.commit().await?;
    }
    Ok(settlement)
}

/// Replaces an owner wait with the fact that the owner is gone. It never
/// overwrites a failure, a terminal Task or an unrelated blocking condition.
async fn annotate_machine_removed(
    db: &SqliteDb,
    task: &Task,
    facts: &RemovedMachineFacts,
    actor: &api_types::Actor,
) -> Result<()> {
    let project = ProjectRepo::get_by_id(db, &task.project_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("project", &task.project_id))?;
    let workflow =
        WorkflowEngine::resolve_workflow_for_task(task, &project.workflow_definition, actor);
    let terminal = workflow
        .states
        .iter()
        .filter(|state| state.kind == api_types::StateKind::Terminal)
        .map(|state| state.name.clone())
        .collect::<Vec<_>>();
    let annotation = channels::ErrorAnnotation::new(channels::AnnotationShape::TimedBlock(
        channels::TimedBlockAnnotation {
            kind: api_types::FailureKind::RecoveryRequired,
            blocking_reason: MACHINE_REMOVED_REASON.into(),
            blocked_by: actor.display(),
            blocked_at: now_rfc3339(),
            message: if facts.workspace_lost {
                format!("Machine {} was removed. Its workspace and any work that was not pushed were abandoned; the Task continues on another machine from its last server-known branch.", facts.hostname)
            } else {
                format!("Machine {} was removed while it was running this Task. The run was stopped; the Task continues on another machine.", facts.hostname)
            },
        },
    ));
    let annotation = serde_json::to_string(&annotation).expect("condition channel serializes");
    // An expired owner wait also blocked the Task; that blocker goes with it.
    db.annotate_removed_machine_task(
        &task.id,
        db::AnnotateRemovedMachineTaskInput {
            annotation: annotation.to_string(),
            updated_at: now_rfc3339(),
            task_id: &task.id,
            terminal_states_json: json!(terminal).to_string(),
            owner_loss_reasons_json: json!([
                OWNER_LOSS_REASONS.as_slice(),
                &[MACHINE_REMOVED_REASON]
            ]
            .concat())
            .to_string(),
        },
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod lane_condition_tests;
