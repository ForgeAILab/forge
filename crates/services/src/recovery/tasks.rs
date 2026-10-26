//! Task recovery and running execution cancellation.

use super::*;
use db::task_condition::channels;

/// Every non-terminal Task that has a running execution, with those
/// executions: implementation states and the gates a role runs in (the
/// planning gate, review, a subtask's inherited step check, a custom
/// workflow's gate roles). One query for the executions and one Task read per
/// Task that has one; Tasks without a running execution are never read.
pub(super) async fn tasks_with_running_executions(
    db: &SqliteDb,
) -> Result<Vec<(Task, Vec<Execution>)>> {
    let mut by_task = std::collections::BTreeMap::<String, Vec<Execution>>::new();
    for execution in ExecutionRepo::list_running(db).await? {
        by_task
            .entry(execution.task_id.clone())
            .or_default()
            .push(execution);
    }
    let mut projects = HashMap::<String, Option<Project>>::new();
    let mut tasks = Vec::new();
    for (task_id, executions) in by_task {
        let Some(task) = TaskRepo::get_by_id(db, &task_id, false).await? else {
            continue;
        };
        if task.archived_at.is_some() || task.assignee_type.as_deref() == Some("user") {
            continue;
        }
        if !projects.contains_key(&task.project_id) {
            let project = ProjectRepo::get_by_id(db, &task.project_id).await?;
            projects.insert(task.project_id.clone(), project);
        }
        let Some(Some(project)) = projects.get(&task.project_id) else {
            continue;
        };
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::CrashRecovery),
        );
        // A state the effective workflow does not define has no recovery
        // (`recover_task` refuses it); a settled Task is not re-driven.
        if matches!(
            workflow.state_kind(&task.status),
            None | Some(api_types::StateKind::Terminal)
        ) {
            continue;
        }
        tasks.push((task, executions));
    }
    Ok(tasks)
}

/// The Tasks an Agent's heartbeat timeout stops: implementation states only,
/// where the running execution is the Task's own Agent's.
pub(super) async fn list_in_progress_tasks(
    db: &SqliteDb,
    agent_id: Option<&str>,
) -> Result<Vec<Task>> {
    let mut tasks = Vec::new();
    for project in list_projects(db).await? {
        let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
        let statuses: Vec<String> = workflow
            .states
            .iter()
            .filter(|state| state.kind == api_types::StateKind::Active)
            .map(|state| state.name.clone())
            .collect();
        if statuses.is_empty() {
            continue;
        }
        let mut cursor = None;
        loop {
            let page = TaskRepo::list(
                db,
                TaskListQuery {
                    project_id: project.id.clone(),
                    q: None,
                    statuses: statuses.clone(),
                    agent_ids: agent_id.map(str::to_owned).into_iter().collect(),
                    assignee_types: Vec::new(),
                    assignee_ids: Vec::new(),
                    priority: None,
                    include_archived: false,
                    include_cancelled: false,
                    include_deleted: false,
                    page: page_request(cursor),
                },
            )
            .await?;
            tasks.extend(
                page.items
                    .into_iter()
                    .filter(|task| task.assignee_type.as_deref() != Some("user")),
            );
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
    }
    Ok(tasks)
}

pub(crate) async fn recover_task(
    db: &SqliteDb,
    task: Task,
    stop_reason: StopReason,
    stopped_by: &api_types::Actor,
) -> Result<RecoverTaskOutcome> {
    let project = ProjectRepo::get_by_id(db, &task.project_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
    let workflow =
        WorkflowEngine::resolve_workflow_for_task(&task, &project.workflow_definition, stopped_by);
    if workflow
        .states
        .iter()
        .all(|state| state.name != task.status)
    {
        return Err(ServiceError::invalid_operation(format!(
            "workflow has no state named {}",
            task.status
        )));
    }
    let should_auto_resume = stop_reason == StopReason::CrashRecovery;
    let cancelled = cancel_running_executions_for_recovery(
        db,
        &task.id,
        stop_reason.clone(),
        stopped_by,
        if should_auto_resume {
            ResumePolicy::Auto
        } else {
            ResumePolicy::Manual
        },
        stop_reason == StopReason::AgentTimeout,
    )
    .await?;

    // Each committed terminal CAS disposes only the WorkspaceLease bound to
    // that execution. Never revoke by Task here: a zero-row CAS is a
    // concurrent winner, and a newer execution may already hold a healthy
    // successor lease for the same Task.

    if cancelled.is_empty() {
        return Ok(RecoverTaskOutcome {
            task,
            annotated: false,
            settled_executions: 0,
        });
    }

    if should_auto_resume {
        tracing::warn!(
            task_id = %task.id,
            execution_count = cancelled.len(),
            "recovered task left in current state for automatic redispatch"
        );
        return Ok(RecoverTaskOutcome {
            task,
            annotated: false,
            settled_executions: cancelled.len(),
        });
    }

    let has_resumable_execution = cancelled
        .iter()
        .any(|execution| execution.agent_session_id.is_some());
    if has_resumable_execution {
        tracing::warn!(
            task_id = %task.id,
            execution_count = cancelled.len(),
            "recovered task left in current state because a resumable execution was cancelled"
        );
        return Ok(RecoverTaskOutcome {
            task,
            annotated: false,
            settled_executions: cancelled.len(),
        });
    }

    let (blocking_reason, message) = if stop_reason == StopReason::AgentTimeout {
        ("agent_timeout", "Recovered after agent heartbeat timeout")
    } else {
        ("crash_recovery", "Recovered after server restart")
    };
    let blocked_execution_id = cancelled
        .first()
        .map(|execution| execution.execution_id.clone());
    let artifact = blocked_execution_id
        .as_ref()
        .map(|execution_id| channels::AnnotationArtifact {
            kind: "execution".into(),
            id: Some(execution_id.clone()),
            log_path: None,
        });
    let annotation = serde_json::to_string(&channels::ErrorAnnotation::sorted(
        channels::AnnotationShape::Blocking(channels::BlockingAnnotation {
            kind: api_types::FailureKind::RecoveryRequired,
            blocking_reason: blocking_reason.into(),
            blocked_by: Some(stopped_by.display()),
            blocked_at: Some(now_rfc3339()),
            blocked_execution_id,
            artifact,
            message: Some(message.into()),
            hook: None,
        }),
    ))
    .expect("condition channel serializes");
    // Applied under the recovery step's lease, or synchronously as the
    // Task's own step (bounded) from the heartbeat monitor. The outcome
    // reports a committed annotation only, never a queued one.
    let task = match TaskRepo::update_status(
        db,
        UpdateTaskStatus {
            id: task.id.clone(),
            expected_version: task.version,
            status: task.status.clone(),
            assignee_id: None,
            error_annotation: Some(Some(annotation)),
            blocked_json: None,
            failed_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    {
        Ok(task) => task,
        Err(db::DbError::TaskBusy { pending_steps, .. }) => {
            // Outside the lease only: the accepted annotation stays queued
            // behind running work and publishes task.updated when it lands.
            tracing::info!(task_id = %task.id, pending_steps, "recovery annotation queued behind running Task work");
            return Ok(RecoverTaskOutcome {
                task,
                annotated: false,
                settled_executions: cancelled.len(),
            });
        }
        Err(error) => return Err(error.into()),
    };

    Ok(RecoverTaskOutcome {
        task,
        annotated: true,
        settled_executions: cancelled.len(),
    })
}

pub(super) async fn sweep_stale_recovery_annotations(db: &Arc<SqliteDb>) -> Result<u64> {
    let mut cleared = 0_u64;

    for project in list_projects(db).await? {
        let mut cursor = None;
        loop {
            let page = TaskRepo::list(
                db.as_ref(),
                TaskListQuery {
                    project_id: project.id.clone(),
                    q: None,
                    statuses: vec![],
                    agent_ids: Vec::new(),
                    assignee_types: Vec::new(),
                    assignee_ids: Vec::new(),
                    priority: None,
                    include_archived: false,
                    include_cancelled: false,
                    include_deleted: false,
                    page: page_request(cursor),
                },
            )
            .await?;

            for task in page.items {
                let Some(annotation_json) = task.error_annotation.as_deref() else {
                    continue;
                };
                let Ok(annotation) = serde_json::from_str::<serde_json::Value>(annotation_json)
                else {
                    continue;
                };
                if annotation.get("type").and_then(serde_json::Value::as_str)
                    != Some("recovery_required")
                {
                    continue;
                }

                let blocked_execution_id =
                    annotation.get("blocked_execution_id").and_then(|value| {
                        if value.is_null() {
                            None
                        } else {
                            value.as_str()
                        }
                    });

                let should_clear = match blocked_execution_id {
                    None => true,
                    Some(execution_id) => {
                        match ExecutionRepo::get_by_id(db.as_ref(), execution_id).await? {
                            None => true,
                            Some(execution) => !execution_awaits_recovery(&execution),
                        }
                    }
                };

                if !should_clear {
                    continue;
                }

                db.enqueue_task_mutation(
                    &task.id,
                    db::TaskMutation::TaskUpdateStatus {
                        input: UpdateTaskStatus {
                            id: task.id.clone(),
                            expected_version: task.version,
                            status: task.status.clone(),
                            assignee_id: None,
                            error_annotation: Some(None),
                            blocked_json: None,
                            failed_json: None,
                            updated_at: now_rfc3339(),
                        },
                    },
                )
                .await?;

                cleared += 1;
            }

            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
    }

    Ok(cleared)
}

fn execution_awaits_recovery(execution: &db::Execution) -> bool {
    execution.status == ExecutionStatus::Cancelled
        && matches!(execution.resume_policy, Some(ResumePolicy::Manual))
}

pub(crate) async fn cancel_running_executions(
    db: &SqliteDb,
    task_id: &str,
    stop_reason: StopReason,
    stopped_by: &api_types::Actor,
    resume_policy: ResumePolicy,
) -> Result<Vec<CancelledExecution>> {
    cancel_running_executions_for_recovery(
        db,
        task_id,
        stop_reason,
        stopped_by,
        resume_policy,
        false,
    )
    .await
}

/// Stop every execution of the retired inline review re-run still `running`
/// for the Task (its reviewer execution and, if it had one, the auditor
/// child), whatever their leases say. Returns the ids it stopped.
pub(crate) async fn stop_retired_review_reruns(
    db: &SqliteDb,
    task_id: &str,
) -> Result<Vec<String>> {
    Ok(cancel_running_executions_matching(
        db,
        task_id,
        StopReason::CrashRecovery,
        &api_types::Actor::system(api_types::SystemComponent::CrashRecovery),
        ResumePolicy::Manual,
        false,
        true,
    )
    .await?
    .into_iter()
    .map(|execution| execution.execution_id)
    .collect())
}

async fn cancel_running_executions_for_recovery(
    db: &SqliteDb,
    task_id: &str,
    stop_reason: StopReason,
    stopped_by: &api_types::Actor,
    resume_policy: ResumePolicy,
    preserve_healthy_owner: bool,
) -> Result<Vec<CancelledExecution>> {
    cancel_running_executions_matching(
        db,
        task_id,
        stop_reason,
        stopped_by,
        resume_policy,
        preserve_healthy_owner,
        false,
    )
    .await
}

/// `only_retired_review_reruns` narrows the stop to executions of the retired
/// inline review re-run, which are settled on their own
/// ([`crate::TaskService::settle_retired_review_rerun`]).
async fn cancel_running_executions_matching(
    db: &SqliteDb,
    task_id: &str,
    stop_reason: StopReason,
    stopped_by: &api_types::Actor,
    resume_policy: ResumePolicy,
    preserve_healthy_owner: bool,
    only_retired_review_reruns: bool,
) -> Result<Vec<CancelledExecution>> {
    let executions = ExecutionRepo::list_running_by_task(db, task_id).await?;
    let mut cancelled = Vec::new();
    let now = now_rfc3339();
    for execution in executions {
        if only_retired_review_reruns && !crate::task_service::retired_review_rerun(&execution) {
            continue;
        }
        if matches!(
            stop_reason,
            StopReason::CrashRecovery | StopReason::AgentTimeout
        ) && execution_lease_is_suspended(db, &execution).await?
        {
            continue;
        }
        if preserve_healthy_owner && execution_owner_lease_is_healthy(&execution, &now) {
            // Legacy AgentStatus heartbeat timeout is not authoritative for
            // an attempt that still has a live execution owner lease. The
            // expiry/deadline monitor remains responsible for this execution.
            continue;
        }
        let mut terminal_input = crate::task_service::execution::ledger::terminal_with_ledger(
            TerminalizeExecution {
                execution_id: execution.id.clone(),
                expected_version: execution.execution_version,
                lease_owner: execution.lease_owner.clone(),
                status: ExecutionStatus::Cancelled,
                stop_reason: Some(Some(stop_reason.clone())),
                stopped_by: Some(Some(stopped_by.display())),
                stopped_at: Some(Some(now_rfc3339())),
                resume_policy: Some(Some(resume_policy.clone())),
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                last_progress_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: Some(Some("Recovered".to_owned())),
                executor_config_snapshot_json: None,
                updated_at: now_rfc3339(),
                actor_type: if stopped_by.is_system() {
                    "system".to_owned()
                } else if stopped_by.is_agent() {
                    "agent".to_owned()
                } else {
                    "user".to_owned()
                },
                actor_id: None,
                correlation_id: None,
                causation_id: None,
                causation_depth: 0,
                lease_disposition: ExecutionLeaseDisposition::Revoke,
            },
            Vec::new(),
            None,
            None,
        );
        // Local provider calls cannot survive a process crash with a
        // replayable result.  Ask the composite boundary to move their
        // pending invocations to `unsettled` before committing the terminal
        // CAS. Daemon-owned calls retain their pending state because the
        // daemon keeps its terminal report until it is acknowledged.
        terminal_input.mark_unreplayable_pending_unsettled = execution
            .lease_owner
            .as_deref()
            .is_none_or(|owner| !owner.starts_with("daemon:"));
        let outcome = ExecutionRepo::terminalize_with_ledger(db, terminal_input).await?;
        if let ExecutionTerminalOutcome::Committed {
            execution: cancelled_execution,
            ..
        } = outcome
        {
            discard_execution_plan_artifacts(db, &cancelled_execution).await;
            cancelled.push(CancelledExecution {
                execution_id: cancelled_execution.id,
                agent_session_id: execution.agent_session_id.clone(),
            });
        }
    }
    Ok(cancelled)
}

#[cfg(test)]
mod lane_condition_tests;
