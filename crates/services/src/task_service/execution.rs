use super::*;
use crate::agent_capacity::count_running_executions;
use crate::workflow::dispatch::{
    build_effective_prompt, dispatch_intent_from_workflow_dispatch, effective_prompt_selection,
    loader::load_agent_dispatch_context,
};
use db::{UpdateTask, UpdateTaskStatus};

mod cascade;
mod environment;
mod follow_up;
mod guards;
mod hooks;
mod launch;
pub(crate) mod ledger;
mod recovery;
mod runner;

const PLAN_PUBLICATION_CLAIM_KEY: &str = "plan_publication_claim";
const PLAN_PUBLICATION_CLEANUP_KEY: &str = "plan_publication_cleanup";
const TERMINAL_EXECUTION_SETTLEMENT_KEY: &str = "terminal_execution_settlement";

pub(super) use recovery::REPLAYING_RECOVERY;
pub(crate) use runner::discard_execution_plan_stage;
pub(super) use runner::{bounded_lease_expiry, execution_deadline_seconds, rfc3339_after};

pub(crate) use cascade::should_block_task_for_failed_execution;
pub(crate) use cascade::{
    exact_review_for_execution, reviewer_execution_lacks_exact_review_binding,
};

#[derive(Debug, Clone)]
struct PlanPublicationClaim {
    execution_id: String,
    state: String,
    state_entry_token: Option<String>,
    project_version: i64,
}

impl PlanPublicationClaim {
    fn value(&self) -> Value {
        json!({
            "execution_id": self.execution_id,
            "state": self.state,
            "state_entry_token": self.state_entry_token,
            "project_version": self.project_version,
        })
    }
}

fn parse_plan_publication_claim(task: &Task) -> Result<Option<PlanPublicationClaim>> {
    parse_plan_publication_record(task, PLAN_PUBLICATION_CLAIM_KEY)
}

fn parse_plan_publication_cleanup(task: &Task) -> Result<Option<PlanPublicationClaim>> {
    parse_plan_publication_record(task, PLAN_PUBLICATION_CLEANUP_KEY)
}

fn parse_plan_publication_record(task: &Task, key: &str) -> Result<Option<PlanPublicationClaim>> {
    let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid task metadata for {}: {error}", task.id))
    })?;
    let Some(claim) = metadata.extra.get(key) else {
        return Ok(None);
    };
    let execution_id = claim
        .get("execution_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ServiceError::invalid_operation("invalid plan publication claim"))?;
    let state = claim
        .get("state")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ServiceError::invalid_operation("invalid plan publication claim"))?;
    let project_version = claim
        .get("project_version")
        .and_then(Value::as_i64)
        .ok_or_else(|| ServiceError::invalid_operation("invalid plan publication claim"))?;
    let state_entry_token = match claim.get("state_entry_token") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) if !value.is_empty() => Some(value.clone()),
        _ => {
            return Err(ServiceError::invalid_operation(
                "invalid plan publication claim",
            ));
        }
    };
    Ok(Some(PlanPublicationClaim {
        execution_id: execution_id.to_owned(),
        state: state.to_owned(),
        state_entry_token,
        project_version,
    }))
}

pub(crate) async fn execution_completion_settled_for_current_state_entry(
    db: &SqliteDb,
    task: &Task,
    execution: &Execution,
) -> Result<bool> {
    let Some(settlement) = parse_plan_publication_record(task, TERMINAL_EXECUTION_SETTLEMENT_KEY)?
        .filter(|settlement| {
            settlement.execution_id == execution.id && settlement.state == task.status
        })
    else {
        return Ok(false);
    };
    let current = crate::task_service::action_resolver::latest_state_entry_authority(
        db,
        &task.id,
        &task.status,
    )
    .await?;
    Ok(settlement.state_entry_token.as_deref() == current.as_ref().map(|entry| entry.id.as_str()))
}

pub(crate) fn execution_uses_brokered_plan(execution: &Execution) -> bool {
    if !executors::task_role_can_write_plan(Some(execution.role.as_str())) {
        return false;
    }
    execution
        .executor_config_snapshot_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .is_some_and(|snapshot| {
            snapshot.get("plan_delivery").and_then(Value::as_str) == Some("execution_outbox")
                || crate::task_service::config::snapshot_uses_cli_backend(&snapshot)
        })
}

pub(crate) fn brokered_plan_dispatch_project_version(execution: &Execution) -> Option<i64> {
    execution
        .executor_config_snapshot_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .filter(|snapshot| {
            snapshot.get("plan_delivery").and_then(Value::as_str) == Some("execution_outbox")
        })
        .and_then(|snapshot| snapshot.get("project_version").and_then(Value::as_i64))
}

pub(crate) fn active_plan_publication_claim_owner(task: &Task) -> Result<Option<String>> {
    Ok(parse_plan_publication_claim(task)?
        .filter(|claim| claim.state == task.status)
        .map(|claim| claim.execution_id))
}

pub(crate) fn pending_plan_publication_cleanup_owner(task: &Task) -> Result<Option<String>> {
    Ok(parse_plan_publication_cleanup(task)?.map(|cleanup| cleanup.execution_id))
}

/// Remove one brokered execution's host-private plan files. Callers must keep
/// a durable claim or cleanup marker until this succeeds so a crash or I/O
/// failure remains recoverable by the dispatcher.
pub(crate) async fn cleanup_execution_plan_private_files(
    db: &SqliteDb,
    router: &WorkspaceBackendRouter,
    task: &Task,
    execution_id: &str,
) -> Result<()> {
    let execution = ExecutionRepo::get_by_id(db, execution_id).await?;
    let workspace = match execution
        .as_ref()
        .filter(|execution| execution.task_id == task.id)
        .and_then(|execution| execution.workspace_id.as_deref())
    {
        Some(workspace_id) => WorkspaceRepo::get_by_id(db, workspace_id).await?,
        None => {
            let workspace_task_id = task.parent_task_id.as_deref().unwrap_or(&task.id);
            WorkspaceRepo::get_by_task_id(db, workspace_task_id).await?
        }
    };
    let Some(workspace) = workspace else {
        // With no surviving workspace row there is no path through which the
        // private files can be reached or reused. The database marker can be
        // retired without inventing an untrusted filesystem path.
        return Ok(());
    };
    let resolved = router.resolve(db, &workspace).await?;
    crate::plan_artifact::ExecutionPlan::new(db, &resolved)
        .discard(execution_id)
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "failed to remove settled execution plan stage: {error}"
            ))
        })?;
    Ok(())
}

pub(crate) async fn clear_stale_plan_publication_claim(
    db: &SqliteDb,
    router: &WorkspaceBackendRouter,
    task: &Task,
) -> Result<Task> {
    let Some(claim) = parse_plan_publication_claim(task)? else {
        return Ok(task.clone());
    };
    if claim.state == task.status {
        return Ok(task.clone());
    }

    // The transition already committed, so these bytes can no longer be
    // published for the state that created them. Remove them before the
    // marker: a crash between these steps leaves a harmless marker that the
    // next dispatcher pass can finish clearing, never an unowned candidate.
    cleanup_execution_plan_private_files(db, router, task, &claim.execution_id).await?;

    TaskRepo::mutate_metadata(
        db,
        &task.id,
        Some(task.version),
        vec![db::TaskMetadataMutation::CompareAndMutate {
            key: PLAN_PUBLICATION_CLAIM_KEY.to_owned(),
            expected: claim.value(),
            mutations: vec![db::TaskMetadataMutation::Remove {
                key: PLAN_PUBLICATION_CLAIM_KEY.to_owned(),
            }],
        }],
        &now_rfc3339(),
    )
    .await
    .map_err(Into::into)
}

pub(crate) async fn execution_belongs_to_current_state_entry(
    db: &SqliteDb,
    task: &Task,
    execution: &Execution,
) -> Result<bool> {
    let snapshot = execution
        .executor_config_snapshot_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok());
    if let Some(snapshot) = snapshot.as_ref() {
        if snapshot
            .get("task_state")
            .and_then(Value::as_str)
            .is_some_and(|state| state != task.status)
        {
            return Ok(false);
        }
    }

    let current_entry = crate::task_service::action_resolver::latest_state_entry_authority(
        db,
        &task.id,
        &task.status,
    )
    .await?;
    if let Some(snapshot) = snapshot.as_ref() {
        if let Some(token) = snapshot.get("state_entry_token") {
            let snapshotted = match token {
                Value::Null => None,
                Value::String(value) => Some(value.as_str()),
                _ => return Ok(false),
            };
            return Ok(snapshotted == current_entry.as_ref().map(|entry| entry.id.as_str()));
        }
    }

    // v0.13.8 and earlier snapshots did not persist the entry token. They can
    // still be rejected once a newer transition into this state is visible.
    Ok(current_entry
        .as_ref()
        .is_none_or(|entry| execution.created_at >= entry.created_at))
}

pub(super) enum PlanPublicationClaimOutcome {
    Claimed(Box<Task>),
    VersionRace,
    OwnedByOther,
    StaleWorkflowAuthority,
}

pub(crate) fn ensure_plan_publication_transition_authority(
    task: &Task,
    execution_id: Option<&str>,
) -> Result<()> {
    let Some(claim) = parse_plan_publication_claim(task)? else {
        return Ok(());
    };
    if execution_id == Some(claim.execution_id.as_str()) && claim.state == task.status {
        return Ok(());
    }
    Err(ServiceError::invalid_operation(
        "the Task is settling a completed execution's plan artifact; retry after publication",
    ))
}

pub(crate) fn plan_publication_project_version(
    task: &Task,
    execution_id: &str,
) -> Result<Option<i64>> {
    Ok(parse_plan_publication_claim(task)?
        .filter(|claim| claim.state == task.status && claim.execution_id.as_str() == execution_id)
        .map(|claim| claim.project_version))
}

pub(crate) async fn plan_publication_matches_current_state_entry(
    db: &SqliteDb,
    task: &Task,
    execution_id: &str,
) -> Result<bool> {
    let Some(claim) = parse_plan_publication_claim(task)?
        .filter(|claim| claim.state == task.status && claim.execution_id.as_str() == execution_id)
    else {
        return Ok(true);
    };
    let current = crate::task_service::action_resolver::latest_state_entry_authority(
        db,
        &task.id,
        &task.status,
    )
    .await?;
    Ok(claim.state_entry_token.as_deref() == current.as_ref().map(|entry| entry.id.as_str()))
}

pub(super) async fn claim_plan_publication(
    db: &SqliteDb,
    task: &Task,
    execution: &Execution,
    project_version: i64,
) -> Result<PlanPublicationClaimOutcome> {
    if let Some(claim) = parse_plan_publication_claim(task)? {
        if claim.state == task.status {
            return Ok(if claim.execution_id == execution.id {
                if claim.project_version == project_version {
                    PlanPublicationClaimOutcome::Claimed(Box::new(task.clone()))
                } else {
                    PlanPublicationClaimOutcome::StaleWorkflowAuthority
                }
            } else {
                PlanPublicationClaimOutcome::OwnedByOther
            });
        }
    }
    let state_entry_token = crate::task_service::action_resolver::latest_state_entry_authority(
        db,
        &task.id,
        &task.status,
    )
    .await?
    .map(|entry| entry.id);
    let claim = PlanPublicationClaim {
        execution_id: execution.id.clone(),
        state: task.status.clone(),
        state_entry_token,
        project_version,
    };
    match TaskRepo::claim_metadata_for_latest_execution(
        db,
        db::LatestExecutionMetadataClaim {
            task_id: task.id.clone(),
            expected_task_version: task.version,
            authority: db::LatestExecutionAuthority {
                execution_id: execution.id.clone(),
                role: execution.role.clone(),
                agent_id: execution.agent_id.clone(),
                execution_updated_at: execution.updated_at.clone(),
                expected_project_version: project_version,
            },
            key: PLAN_PUBLICATION_CLAIM_KEY.to_owned(),
            value: claim.value(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    {
        Ok(task) => Ok(PlanPublicationClaimOutcome::Claimed(Box::new(task))),
        Err(DbError::VersionConflict) => {
            let Some(current) = TaskRepo::get_by_id(db, &task.id, false).await? else {
                return Ok(PlanPublicationClaimOutcome::OwnedByOther);
            };
            if let Some(current_claim) = parse_plan_publication_claim(&current)? {
                if current_claim.state == current.status {
                    return Ok(if current_claim.execution_id == execution.id {
                        if current_claim.project_version == project_version {
                            PlanPublicationClaimOutcome::Claimed(Box::new(current))
                        } else {
                            PlanPublicationClaimOutcome::StaleWorkflowAuthority
                        }
                    } else {
                        PlanPublicationClaimOutcome::OwnedByOther
                    });
                }
            }
            let roles: &[&str] = if matches!(execution.role.as_str(), "coder" | "executor") {
                &["coder", "executor"]
            } else {
                &[execution.role.as_str()]
            };
            let mut latest: Option<Execution> = None;
            for role in roles {
                let page = ExecutionRepo::list_by_task_and_role(
                    db,
                    &task.id,
                    role,
                    db::PageRequest {
                        cursor: None,
                        limit: 1,
                        include_total: false,
                        sort_by: db::SortBy::CreatedAt,
                        sort_order: db::SortOrder::Desc,
                    },
                )
                .await?;
                if let Some(candidate) = page.items.into_iter().next() {
                    if latest.as_ref().is_none_or(|current| {
                        (&candidate.created_at, &candidate.id) > (&current.created_at, &current.id)
                    }) {
                        latest = Some(candidate);
                    }
                }
            }
            if latest
                .as_ref()
                .is_some_and(|latest| latest.id != execution.id)
            {
                return Ok(PlanPublicationClaimOutcome::OwnedByOther);
            }
            let project_is_current = ProjectRepo::get_by_id(db, &task.project_id)
                .await?
                .is_some_and(|project| project.version == project_version);
            if !project_is_current {
                return Ok(PlanPublicationClaimOutcome::StaleWorkflowAuthority);
            }
            Ok(PlanPublicationClaimOutcome::VersionRace)
        }
        Err(error) => Err(error.into()),
    }
}

pub(super) async fn release_plan_publication(
    db: &SqliteDb,
    task: &Task,
    execution: &Execution,
) -> Result<Task> {
    release_plan_publication_for_execution_id(db, task, &execution.id).await
}

pub(super) async fn release_plan_publication_for_execution_id(
    db: &SqliteDb,
    task: &Task,
    execution_id: &str,
) -> Result<Task> {
    let claim = parse_plan_publication_claim(task)?
        .filter(|claim| claim.execution_id == execution_id && claim.state == task.status)
        .ok_or_else(|| {
            ServiceError::invalid_operation("plan publication claim was lost before release")
        })?;
    let updated = TaskRepo::mutate_metadata_and_bump_version(
        db,
        &task.id,
        task.version,
        vec![db::TaskMetadataMutation::CompareAndMutate {
            key: PLAN_PUBLICATION_CLAIM_KEY.to_owned(),
            expected: claim.value(),
            mutations: vec![db::TaskMetadataMutation::Remove {
                key: PLAN_PUBLICATION_CLAIM_KEY.to_owned(),
            }],
        }],
        &now_rfc3339(),
    )
    .await?;
    if updated.version == task.version {
        return Err(ServiceError::invalid_operation(
            "plan publication claim was lost before release",
        ));
    }
    Ok(updated)
}

pub(super) async fn settle_plan_publication_without_transition(
    db: &SqliteDb,
    task: &Task,
    execution: &Execution,
) -> Result<Task> {
    let claim = parse_plan_publication_claim(task)?
        .filter(|claim| claim.execution_id == execution.id && claim.state == task.status)
        .ok_or_else(|| {
            ServiceError::invalid_operation("plan publication claim was lost before settlement")
        })?;
    let updated = TaskRepo::mutate_metadata_and_bump_version_with_project_authority(
        db,
        &task.id,
        task.version,
        claim.project_version,
        vec![db::TaskMetadataMutation::CompareAndMutate {
            key: PLAN_PUBLICATION_CLAIM_KEY.to_owned(),
            expected: claim.value(),
            mutations: vec![
                db::TaskMetadataMutation::Remove {
                    key: PLAN_PUBLICATION_CLAIM_KEY.to_owned(),
                },
                db::TaskMetadataMutation::Set {
                    key: TERMINAL_EXECUTION_SETTLEMENT_KEY.to_owned(),
                    value: claim.value(),
                },
                db::TaskMetadataMutation::Set {
                    key: PLAN_PUBLICATION_CLEANUP_KEY.to_owned(),
                    value: claim.value(),
                },
            ],
        }],
        &now_rfc3339(),
    )
    .await?;
    if updated.version == task.version {
        return Err(ServiceError::invalid_operation(
            "plan publication claim was lost before settlement",
        ));
    }
    Ok(updated)
}

pub(crate) async fn clear_plan_publication_cleanup(
    db: &SqliteDb,
    task: &Task,
    execution_id: &str,
) -> Result<Task> {
    let Some(cleanup) = parse_plan_publication_cleanup(task)? else {
        return Ok(task.clone());
    };
    if cleanup.execution_id != execution_id {
        return Ok(task.clone());
    }
    // The private files have already been removed before this marker is
    // retired. Its exact value is sufficient CAS authority for housekeeping;
    // advancing the public Task version here can invalidate the version a
    // client just read with an `awaiting_human` planning decision.
    TaskRepo::mutate_metadata(
        db,
        &task.id,
        None,
        vec![db::TaskMetadataMutation::CompareAndMutate {
            key: PLAN_PUBLICATION_CLEANUP_KEY.to_owned(),
            expected: cleanup.value(),
            mutations: vec![db::TaskMetadataMutation::Remove {
                key: PLAN_PUBLICATION_CLEANUP_KEY.to_owned(),
            }],
        }],
        &now_rfc3339(),
    )
    .await
    .map_err(Into::into)
}

pub(super) async fn clear_settled_plan_publication(
    db: &SqliteDb,
    task_id: &str,
    execution_id: &str,
    state: &str,
) -> Result<()> {
    let Some(task) = TaskRepo::get_by_id(db, task_id, false).await? else {
        return Ok(());
    };
    let Some(claim) = parse_plan_publication_claim(&task)? else {
        return Ok(());
    };
    if claim.execution_id != execution_id || claim.state != state {
        return Ok(());
    }
    TaskRepo::mutate_metadata(
        db,
        task_id,
        None,
        vec![db::TaskMetadataMutation::CompareAndMutate {
            key: PLAN_PUBLICATION_CLAIM_KEY.to_owned(),
            expected: claim.value(),
            mutations: vec![db::TaskMetadataMutation::Remove {
                key: PLAN_PUBLICATION_CLAIM_KEY.to_owned(),
            }],
        }],
        &now_rfc3339(),
    )
    .await?;
    Ok(())
}

pub(super) async fn settle_plan_publication_for_review(
    db: &SqliteDb,
    task: &Task,
    execution: &Execution,
) -> Result<Task> {
    let claim = parse_plan_publication_claim(task)?
        .filter(|claim| claim.execution_id == execution.id && claim.state == task.status)
        .ok_or_else(|| {
            ServiceError::invalid_operation(
                "plan publication claim was lost before approval settlement",
            )
        })?;
    let updated = TaskRepo::mutate_metadata_and_bump_version_with_project_authority(
        db,
        &task.id,
        task.version,
        claim.project_version,
        vec![db::TaskMetadataMutation::CompareAndMutate {
            key: PLAN_PUBLICATION_CLAIM_KEY.to_owned(),
            expected: claim.value(),
            mutations: vec![
                db::TaskMetadataMutation::Remove {
                    key: PLAN_PUBLICATION_CLAIM_KEY.to_owned(),
                },
                db::TaskMetadataMutation::Set {
                    key: "awaiting_human".to_owned(),
                    value: json!(true),
                },
                db::TaskMetadataMutation::Set {
                    key: "awaiting_human_reason".to_owned(),
                    value: json!("plan_review"),
                },
                db::TaskMetadataMutation::Set {
                    key: "planning_completed_at".to_owned(),
                    value: Value::String(now_rfc3339()),
                },
                db::TaskMetadataMutation::Set {
                    key: "awaiting_human_marker_id".to_owned(),
                    value: Value::String(new_uuid_v4()),
                },
                db::TaskMetadataMutation::Set {
                    key: "planning_execution_id".to_owned(),
                    value: Value::String(execution.id.clone()),
                },
                db::TaskMetadataMutation::Set {
                    key: "planning_state_entry_token".to_owned(),
                    value: claim
                        .state_entry_token
                        .clone()
                        .map(Value::String)
                        .unwrap_or(Value::Null),
                },
                db::TaskMetadataMutation::Set {
                    key: TERMINAL_EXECUTION_SETTLEMENT_KEY.to_owned(),
                    value: claim.value(),
                },
                db::TaskMetadataMutation::Set {
                    key: PLAN_PUBLICATION_CLEANUP_KEY.to_owned(),
                    value: claim.value(),
                },
            ],
        }],
        &now_rfc3339(),
    )
    .await?;
    if updated.version == task.version {
        return Err(ServiceError::invalid_operation(
            "plan publication claim was lost before approval settlement",
        ));
    }
    Ok(updated)
}

pub(super) fn publish_terminal_execution_event(service: &TaskService, execution: &Execution) {
    match execution.status {
        ExecutionStatus::Completed => service.publish(ForgeEvent {
            event_type: "execution.completed".to_owned(),
            entity_id: execution.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ExecutionCompleted {
                task_id: execution.task_id.clone(),
            },
        }),
        ExecutionStatus::Failed => service.publish(ForgeEvent {
            event_type: "execution.failed".to_owned(),
            entity_id: execution.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ExecutionFailed {
                task_id: execution.task_id.clone(),
                error: execution
                    .error
                    .clone()
                    .unwrap_or_else(|| "execution failed".to_owned()),
            },
        }),
        ExecutionStatus::Cancelled => service.publish(ForgeEvent {
            event_type: "execution.cancelled".to_owned(),
            entity_id: execution.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ExecutionCancelled {
                task_id: execution.task_id.clone(),
                reason: execution
                    .error
                    .clone()
                    .unwrap_or_else(|| "execution cancelled".to_owned()),
            },
        }),
        ExecutionStatus::Running => {}
    };
}

pub(super) async fn clear_execution_retry_metadata(db: &SqliteDb, task: &Task) -> Result<()> {
    clear_execution_retry_metadata_inner(db, task, true).await
}

pub(super) async fn clear_execution_retry_metadata_preserving_dispatch(
    db: &SqliteDb,
    task: &Task,
) -> Result<()> {
    clear_execution_retry_metadata_inner(db, task, false).await
}

pub(super) async fn clear_execution_retry_metadata_for_latest_execution(
    db: &SqliteDb,
    task: &Task,
    execution: &Execution,
    project_version: i64,
) -> Result<bool> {
    let mutations = execution_retry_clear_mutations(task, true)?;
    Ok(
        TaskRepo::mutate_metadata_and_bump_version_for_latest_execution(
            db,
            &task.id,
            task.version,
            latest_execution_authority(execution, project_version),
            mutations,
            &now_rfc3339(),
        )
        .await?
        .is_some(),
    )
}

pub(super) fn latest_execution_authority(
    execution: &Execution,
    project_version: i64,
) -> db::LatestExecutionAuthority {
    db::LatestExecutionAuthority {
        execution_id: execution.id.clone(),
        role: execution.role.clone(),
        agent_id: execution.agent_id.clone(),
        execution_updated_at: execution.updated_at.clone(),
        expected_project_version: project_version,
    }
}

impl TaskService {
    pub(super) async fn ingest_terminal_execution_outbox(
        &self,
        task: &Task,
        execution: &Execution,
    ) -> Result<crate::native_tools::ExecutionOutboxReport> {
        let workspace = match execution.workspace_id.as_deref() {
            Some(workspace_id) => WorkspaceRepo::get_by_id(&*self.db, workspace_id).await?,
            None => {
                let workspace_task_id = task.parent_task_id.as_deref().unwrap_or(&task.id);
                WorkspaceRepo::get_by_task_id(&*self.db, workspace_task_id).await?
            }
        };
        let (Some(embedded), Some(agent_id), Some(workspace)) = (
            self.credential_env.as_ref(),
            execution.agent_id.as_deref(),
            workspace,
        ) else {
            return Ok(crate::native_tools::ExecutionOutboxReport::default());
        };
        let resolved = self
            .workspace_backend_router
            .resolve(&self.db, &workspace)
            .await?;
        // Daemon-owned outboxes are carried in the retained terminal report;
        // their handles do not name files on the Forge host.
        if resolved.placement.owner_kind == db::PlacementOwnerKind::Daemon {
            return Ok(crate::native_tools::ExecutionOutboxReport::default());
        }
        let path = resolved.embedded_path()?;
        let report = embedded
            .ingest_execution_outbox(&crate::native_tools::ExecutionOutboxInput {
                task_id: &task.id,
                execution_id: &execution.id,
                agent_id,
                role: Some(execution.role.as_str()),
                worktree_path: &path.to_string_lossy(),
            })
            .await;
        if report.worklog_entries > 0 || report.evidence_items > 0 {
            tracing::info!(
                execution_id = %execution.id,
                worklog_entries = report.worklog_entries,
                evidence_items = report.evidence_items,
                "terminal execution outbox ingested"
            );
        }
        if !report.rejected.is_empty() {
            tracing::warn!(
                execution_id = %execution.id,
                rejected = ?report.rejected,
                "terminal execution outbox entries were not ingested"
            );
        }
        Ok(report)
    }
}

async fn clear_execution_retry_metadata_inner(
    db: &SqliteDb,
    task: &Task,
    clear_deferred_dispatch: bool,
) -> Result<()> {
    let mutations = execution_retry_clear_mutations(task, clear_deferred_dispatch)?;
    if !mutations.is_empty() {
        TaskRepo::mutate_metadata(db, &task.id, None, mutations, &now_rfc3339()).await?;
    }
    Ok(())
}

pub(super) fn execution_retry_clear_mutations(
    task: &Task,
    clear_deferred_dispatch: bool,
) -> Result<Vec<db::TaskMetadataMutation>> {
    let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid task metadata for {}: {error}", task.id))
    })?;
    let mut mutations = Vec::new();
    for key in [
        "last_execution_failure_at",
        "last_execution_failure_execution_id",
    ] {
        if let Some(expected) = metadata.extra.get(key).cloned() {
            mutations.push(db::TaskMetadataMutation::RemoveIf {
                key: key.into(),
                expected,
            });
        }
    }
    if clear_deferred_dispatch {
        if let Some(expected) = metadata.extra.get("deferred_dispatch").cloned() {
            mutations.push(db::TaskMetadataMutation::RemoveIf {
                key: "deferred_dispatch".into(),
                expected,
            });
        }
    }
    if let Some(expected) = metadata
        .extra
        .get("executor_unavailable_execution_id")
        .cloned()
    {
        mutations.push(db::TaskMetadataMutation::RemoveIf {
            key: "executor_unavailable_execution_id".to_owned(),
            expected,
        });
    }
    Ok(mutations)
}

pub(super) async fn set_planning_awaiting_review_metadata(
    db: &SqliteDb,
    task: &Task,
    execution_id: Option<&str>,
    awaiting: bool,
) -> Result<Task> {
    let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid task metadata for {}: {error}", task.id))
    })?;
    let mutations = if awaiting {
        let completed_at = now_rfc3339();
        let marker_id = new_uuid_v4();
        let state_entry_token = crate::task_service::action_resolver::latest_state_entry_authority(
            db,
            &task.id,
            &task.status,
        )
        .await?
        .map(|entry| entry.id);
        let mut next = vec![
            db::TaskMetadataMutation::Set {
                key: "awaiting_human".to_owned(),
                value: json!(true),
            },
            db::TaskMetadataMutation::Set {
                key: "awaiting_human_reason".to_owned(),
                value: json!("plan_review"),
            },
            db::TaskMetadataMutation::Set {
                key: "planning_completed_at".to_owned(),
                value: Value::String(completed_at),
            },
            db::TaskMetadataMutation::Set {
                key: "awaiting_human_marker_id".to_owned(),
                value: Value::String(marker_id),
            },
            db::TaskMetadataMutation::Set {
                key: "planning_state_entry_token".to_owned(),
                value: state_entry_token.map(Value::String).unwrap_or(Value::Null),
            },
        ];
        if let Some(execution_id) = execution_id {
            next.push(db::TaskMetadataMutation::Set {
                key: "planning_execution_id".to_owned(),
                value: Value::String(execution_id.to_owned()),
            });
        }
        next
    } else if metadata
        .extra
        .get("awaiting_human_reason")
        .and_then(Value::as_str)
        == Some("plan_review")
    {
        let Some((identity_key, expected)) = metadata
            .extra
            .get("awaiting_human_marker_id")
            .cloned()
            .map(|value| ("awaiting_human_marker_id", value))
            .or_else(|| {
                metadata
                    .extra
                    .get("planning_execution_id")
                    .cloned()
                    .map(|value| ("planning_execution_id", value))
            })
        else {
            // Legacy markers have no stable identity. Leaving one in place is
            // safer than allowing a stale transition to clear a newer marker
            // with the same reason.
            return Ok(task.clone());
        };
        vec![db::TaskMetadataMutation::CompareAndMutate {
            key: identity_key.to_owned(),
            expected,
            mutations: vec![
                db::TaskMetadataMutation::Remove {
                    key: "awaiting_human".to_owned(),
                },
                db::TaskMetadataMutation::Remove {
                    key: "awaiting_human_reason".to_owned(),
                },
                db::TaskMetadataMutation::Remove {
                    key: "planning_completed_at".to_owned(),
                },
                db::TaskMetadataMutation::Remove {
                    key: "planning_execution_id".to_owned(),
                },
                db::TaskMetadataMutation::Remove {
                    key: "awaiting_human_marker_id".to_owned(),
                },
                db::TaskMetadataMutation::Remove {
                    key: "planning_state_entry_token".to_owned(),
                },
            ],
        }]
    } else {
        return Ok(task.clone());
    };

    TaskRepo::mutate_metadata(db, &task.id, Some(task.version), mutations, &now_rfc3339())
        .await
        .map_err(Into::into)
}

#[cfg(test)]
pub(crate) async fn planning_review_matches_current_state_entry(
    db: &SqliteDb,
    task: &Task,
) -> Result<bool> {
    let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid task metadata for {}: {error}", task.id))
    })?;
    if metadata
        .extra
        .get("awaiting_human_reason")
        .and_then(Value::as_str)
        != Some("plan_review")
    {
        return Ok(true);
    }
    let current = crate::task_service::action_resolver::latest_state_entry_authority(
        db,
        &task.id,
        &task.status,
    )
    .await?;
    if let Some(token) = metadata.extra.get("planning_state_entry_token") {
        let marker = match token {
            Value::Null => None,
            Value::String(value) if !value.is_empty() => Some(value.as_str()),
            _ => return Ok(false),
        };
        return Ok(marker == current.as_ref().map(|entry| entry.id.as_str()));
    }
    let Some(execution_id) = metadata
        .extra
        .get("planning_execution_id")
        .and_then(Value::as_str)
    else {
        return Ok(false);
    };
    let Some(execution) = ExecutionRepo::get_by_id(db, execution_id).await? else {
        return Ok(false);
    };
    if execution.task_id != task.id {
        return Ok(false);
    }
    execution_belongs_to_current_state_entry(db, task, &execution).await
}

/// Remove a legacy/default-workflow plan-review wait only while its reason is
/// still exactly `plan_review`. The dispatcher calls this after resolving the
/// current gate and confirming that it does not require human approval.
pub(crate) async fn clear_stale_planning_review_metadata(
    db: &SqliteDb,
    task: &Task,
) -> Result<Task> {
    TaskRepo::mutate_metadata(
        db,
        &task.id,
        Some(task.version),
        vec![db::TaskMetadataMutation::CompareAndMutate {
            key: "awaiting_human_reason".to_owned(),
            expected: json!("plan_review"),
            mutations: vec![
                db::TaskMetadataMutation::Remove {
                    key: "awaiting_human".to_owned(),
                },
                db::TaskMetadataMutation::Remove {
                    key: "awaiting_human_reason".to_owned(),
                },
                db::TaskMetadataMutation::Remove {
                    key: "planning_completed_at".to_owned(),
                },
                db::TaskMetadataMutation::Remove {
                    key: "planning_execution_id".to_owned(),
                },
                db::TaskMetadataMutation::Remove {
                    key: "awaiting_human_marker_id".to_owned(),
                },
                db::TaskMetadataMutation::Remove {
                    key: "planning_state_entry_token".to_owned(),
                },
            ],
        }],
        &now_rfc3339(),
    )
    .await
    .map_err(Into::into)
}
