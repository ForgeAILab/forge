use std::time::Duration;

use api_types::ProjectSettings;
use async_trait::async_trait;
use db::{now_rfc3339, RepoRepo, WorkspaceRepo};
use events::{event_timestamp, EventContext, ForgeEvent};
use serde_json::json;

use crate::{
    lifecycle::{LifecycleHookContext, LifecycleHookRun, LifecycleHookRunner},
    task_service::workspace::prepare_workspace,
    workflow::{effective_role, HookAction, HookContext, HookResult},
};

use super::common::{task, workspace_id};

pub struct RunBeforeWorkHooks;

#[async_trait]
impl HookAction for RunBeforeWorkHooks {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        let project = match db::ProjectRepo::get_by_id(&*ctx.db, &ctx.project_id).await {
            Ok(Some(project)) => project,
            Ok(None) => {
                return HookResult::Failed {
                    reason: format!("project not found: {}", ctx.project_id),
                };
            }
            Err(error) => {
                return HookResult::Failed {
                    reason: error.to_string(),
                };
            }
        };
        let settings = match serde_json::from_str::<ProjectSettings>(&project.settings) {
            Ok(settings) => settings,
            Err(error) => {
                return HookResult::Failed {
                    reason: format!("invalid project settings: {error}"),
                };
            }
        };
        let hooks = settings
            .lifecycle_hooks
            .get(&api_types::LifecycleEvent::BeforeWork)
            .cloned()
            .unwrap_or_default();
        let skip_once = ctx
            .state_config
            .get("skip_before_work_hook_once")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        if skip_once {
            return HookResult::Skipped {
                reason: "skip_before_work_hook_once override set".to_string(),
            };
        }
        if !hooks.iter().any(|hook| {
            matches!(
                hook,
                api_types::LifecycleHookDef::Script { blocking: true, .. }
            )
        }) {
            return HookResult::Skipped {
                reason: "no blocking before_work hooks".to_string(),
            };
        }

        let task = match task(ctx).await {
            Ok(task) => task,
            Err(reason) => return HookResult::Failed { reason },
        };
        let workspace = match workspace_id(ctx).await {
            Some(workspace_id) => match WorkspaceRepo::get_by_id(&*ctx.db, &workspace_id).await {
                Ok(Some(workspace)) => workspace,
                Ok(None) => {
                    return HookResult::Failed {
                        reason: format!("workspace not found: {workspace_id}"),
                    };
                }
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
            },
            None => match prepare_workspace(
                &ctx.db,
                &ctx.workspace_root,
                &task,
                &task.id,
                ctx.repo_cache_locks.clone(),
            )
            .await
            {
                Ok(workspace) => workspace,
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
            },
        };

        let repo_path = match task.repo_id.as_deref() {
            Some(id) => match RepoRepo::get_by_id(&*ctx.db, id).await {
                Ok(repo) => repo
                    .and_then(|repo| repo.local_path)
                    .unwrap_or_else(|| workspace.worktree_path.clone()),
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
            },
            None => workspace.worktree_path.clone(),
        };
        let assigned_agent_id = target_role_agent_id(ctx).await;
        let log_dir = std::env::temp_dir()
            .join("forge")
            .join("logs")
            .join(&task.id)
            .join("hooks");
        let hook_ctx = LifecycleHookContext {
            event: api_types::LifecycleEvent::BeforeWork,
            task_id: task.id.clone(),
            task_title: task.title.clone(),
            task_status: ctx.to_state.clone(),
            previous_status: ctx.from_state.clone(),
            project_id: project.id.clone(),
            project_name: project.name.clone(),
            repo_path,
            worktree_path: Some(workspace.worktree_path.clone()),
            agent_id: assigned_agent_id,
            execution_id: ctx.execution_id.clone(),
            log_dir: Some(log_dir),
        };

        match LifecycleHookRunner::run_blocking_before_work_hooks(hook_ctx, &hooks).await {
            Some(failure) => {
                if let Err(error) = annotate_before_work_hook_block(ctx, &task, &failure).await {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                HookResult::Failed {
                    reason: format!(
                        "blocking before_work hook failed: {}",
                        failure
                            .error
                            .as_deref()
                            .unwrap_or("script did not complete successfully")
                    ),
                }
            }
            None => HookResult::Ok,
        }
    }
}

async fn target_role_agent_id(ctx: &HookContext) -> Option<String> {
    let state = ctx
        .workflow
        .states
        .iter()
        .find(|state| state.name == ctx.to_state)?;
    let role = effective_role(state)?;
    db::TaskRoleAssignmentRepo::get_by_task_and_role(&*ctx.db, &ctx.task_id, role)
        .await
        .ok()
        .flatten()
        .and_then(|assignment| {
            (assignment.assignee_type == Some(db::AssigneeKind::Agent))
                .then_some(assignment.assignee_id)
                .flatten()
        })
}

async fn annotate_before_work_hook_block(
    ctx: &HookContext,
    task: &db::Task,
    failure: &LifecycleHookRun,
) -> db::Result<()> {
    let annotation_type = if failure.timed_out {
        api_types::FailureKind::BeforeWorkHookTimeout
    } else {
        api_types::FailureKind::BeforeWorkHookFailed
    };
    let annotation = json!({
        "type": annotation_type,
        "blocking_reason": annotation_type,
        "blocked_by": api_types::Actor::system(api_types::SystemComponent::LifecycleHook).display(),
        "blocked_at": now_rfc3339(),
        "blocked_execution_id": null,
        "message": failure.error.clone().unwrap_or_else(|| "blocking before_work hook failed".to_owned()),
        "hook": {
            "event": "before_work",
            "index": failure.index,
            "type": "script",
            "command": failure.command.clone().unwrap_or_default(),
            "exit_code": failure.exit_code,
            "timeout": failure.timed_out,
            "duration_ms": failure.duration_ms,
            "working_dir": failure.working_dir,
            "stdout": truncate_annotation_output(&failure.stdout),
            "stderr": truncate_annotation_output(&failure.stderr),
            "log_path": failure.log_path,
            "environment": failure.environment_preview,
        },
        "artifact": {
            "kind": "hook",
            "id": format!("before_work:{}", failure.index),
            "log_path": failure.log_path,
        },
        "recovery_actions": ["retry_hook", "update_workspace_and_retry_hook", "skip_hook_once", "cancel_task"],
    });

    // `before_enter` owns the Task version used by the surrounding workflow
    // transition, so this annotation deliberately retains that historical
    // version behavior. The recovery event still commits atomically with the
    // annotation and uses its own mutation id for dedupe.
    let updated_at = now_rfc3339();
    let mut transaction = db::begin_immediate(ctx.db.pool()).await?;
    let update = sqlx::query(
        "UPDATE task SET error_annotation = ?, updated_at = ? WHERE id = ? AND deleted_at IS NULL",
    )
    .bind(annotation.to_string())
    .bind(&updated_at)
    .bind(&task.id)
    .execute(&mut *transaction)
    .await?;
    if update.rows_affected() != 1 {
        return Err(db::DbError::NotFound);
    }
    let mut interruption_snapshot = task.clone();
    interruption_snapshot.error_annotation = Some(annotation.to_string());
    interruption_snapshot.updated_at = updated_at;
    let mut interruption_event =
        db::CreateDomainEvent::task_interruption_changed(&interruption_snapshot);
    interruption_event.dedupe_key = Some(format!(
        "task-interruption-hook:{}:{}",
        task.id, interruption_event.id
    ));
    db::DomainEventRepo::append_event_in_tx(&*ctx.db, &mut transaction, &interruption_event)
        .await?;
    transaction.commit().await?;
    Ok(())
}

fn truncate_annotation_output(output: &str) -> String {
    const LIMIT: usize = 2048;
    if output.len() <= LIMIT {
        return output.to_owned();
    }
    let mut end = LIMIT;
    while !output.is_char_boundary(end) {
        end -= 1;
    }
    output[..end].to_owned()
}

pub struct CleanupWorkspaceNow;

#[async_trait]
impl HookAction for CleanupWorkspaceNow {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        let Some(cleanup_scheduler) = ctx.cleanup_scheduler.as_ref() else {
            return HookResult::Skipped {
                reason: "cleanup scheduler not configured".to_string(),
            };
        };
        let Some(workspace_id) = workspace_id(ctx).await else {
            return HookResult::Skipped {
                reason: "nothing to clean up".to_string(),
            };
        };
        if let Err(error) = cleanup_scheduler.cleanup_now(workspace_id).await {
            return HookResult::Failed {
                reason: error.to_string(),
            };
        };
        HookResult::Ok
    }
}

pub struct ScheduleWorkspaceCleanup;

#[async_trait]
impl HookAction for ScheduleWorkspaceCleanup {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        ctx.event_bus.publish(ForgeEvent {
            event_type: "task.cancelled".to_string(),
            entity_id: ctx.task_id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskCancelled {
                project_id: ctx.project_id.clone(),
            },
        });

        let Some(cleanup_scheduler) = ctx.cleanup_scheduler.as_ref() else {
            return HookResult::Skipped {
                reason: "cleanup scheduler not configured".to_string(),
            };
        };
        let Some(workspace_id) = workspace_id(ctx).await else {
            return HookResult::Skipped {
                reason: "nothing to clean up".to_string(),
            };
        };
        let delay = ctx
            .workflow
            .cleanup_policy_for(&ctx.to_state)
            .and_then(|policy| match policy {
                api_types::CleanupPolicy::Immediate => None,
                api_types::CleanupPolicy::Delayed { seconds } => Some(Duration::from_secs(seconds)),
            })
            .unwrap_or(Duration::from_secs(24 * 60 * 60));
        if let Err(error) = cleanup_scheduler.schedule(&workspace_id, delay).await {
            return HookResult::Failed {
                reason: error.to_string(),
            };
        };
        HookResult::Ok
    }
}

pub struct PublishTaskBlocked;

#[async_trait]
impl HookAction for PublishTaskBlocked {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        let reason = task(ctx)
            .await
            .ok()
            .and_then(|task| task.error_annotation)
            .unwrap_or_else(|| "Task transitioned to blocked".to_string());
        ctx.event_bus.publish(ForgeEvent {
            event_type: "task.blocked".to_string(),
            entity_id: ctx.task_id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskBlocked {
                project_id: ctx.project_id.clone(),
                reason,
                kind: None,
                source: None,
                execution_id: None,
            },
        });
        HookResult::Ok
    }
}

pub struct AutoCascadeOnCompletion;

#[async_trait]
impl HookAction for AutoCascadeOnCompletion {
    async fn execute(&self, _ctx: &HookContext) -> HookResult {
        HookResult::Cascade {
            to: "done".to_string(),
            reason: "completed".to_string(),
        }
    }
}
