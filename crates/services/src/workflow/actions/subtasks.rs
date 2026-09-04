use async_trait::async_trait;
use db::{TaskDependencyRepo, TaskRepo};
use events::{event_timestamp, EventContext, ForgeEvent};

use crate::workflow::{default_states, HookAction, HookContext, HookResult};

use super::common::transition_subtask_with_inherited_workflow;

pub struct SatisfyDependents;

#[async_trait]
impl HookAction for SatisfyDependents {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        let dependents = match TaskDependencyRepo::list_dependents(&*ctx.db, &ctx.task_id).await {
            Ok(dependents) => dependents,
            Err(error) => {
                return HookResult::Failed {
                    reason: error.to_string(),
                };
            }
        };
        for dependent_id in dependents {
            let unsatisfied =
                match TaskDependencyRepo::unsatisfied_dependencies(&*ctx.db, &dependent_id).await {
                    Ok(unsatisfied) => unsatisfied,
                    Err(error) => {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        };
                    }
                };
            if unsatisfied.is_empty() {
                let timestamp = event_timestamp();
                ctx.event_bus.publish(ForgeEvent {
                    event_type: "task.dependency_satisfied".to_string(),
                    entity_id: dependent_id.clone(),
                    timestamp: timestamp.clone(),
                    context: EventContext::TaskDependencySatisfied {
                        task_id: dependent_id,
                        depends_on_id: ctx.task_id.clone(),
                        timestamp,
                    },
                });
            }
        }
        HookResult::Ok
    }
}

pub struct SubtaskSequenceComplete;

#[async_trait]
impl HookAction for SubtaskSequenceComplete {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        if ctx.to_state != default_states::REVIEW {
            return HookResult::Skipped {
                reason: "not entering review".to_owned(),
            };
        }

        let subtasks = match TaskRepo::list_subtasks_ordered(&*ctx.db, &ctx.task_id).await {
            Ok(subtasks) => subtasks,
            Err(error) => {
                return HookResult::Failed {
                    reason: error.to_string(),
                };
            }
        };
        let incomplete = subtasks
            .into_iter()
            .filter(|subtask| subtask.status != default_states::CANCELLED)
            .filter(|subtask| subtask.status != default_states::DONE)
            .map(|subtask| subtask.id)
            .collect::<Vec<_>>();

        if incomplete.is_empty() {
            HookResult::Ok
        } else {
            HookResult::Failed {
                reason: format!("SUBTASK_SEQUENCE_NOT_COMPLETE: {}", incomplete.join(",")),
            }
        }
    }
}

pub struct PropagateDoneToSubtasks;

#[async_trait]
impl HookAction for PropagateDoneToSubtasks {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        let subtasks = match TaskRepo::list_subtasks_ordered(&*ctx.db, &ctx.task_id).await {
            Ok(subtasks) => subtasks,
            Err(error) => {
                return HookResult::Failed {
                    reason: error.to_string(),
                };
            }
        };

        for subtask in subtasks {
            if matches!(
                subtask.status.as_str(),
                default_states::DONE | default_states::CANCELLED
            ) {
                continue;
            }
            if let Err(reason) =
                transition_subtask_with_inherited_workflow(ctx, subtask, default_states::DONE).await
            {
                return HookResult::Failed { reason };
            }
        }

        HookResult::Ok
    }
}

pub struct CancelPendingSubtasks;

#[async_trait]
impl HookAction for CancelPendingSubtasks {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        let subtasks = match TaskRepo::list_subtasks_ordered(&*ctx.db, &ctx.task_id).await {
            Ok(subtasks) => subtasks,
            Err(error) => {
                return HookResult::Failed {
                    reason: error.to_string(),
                };
            }
        };

        for subtask in subtasks {
            if matches!(
                subtask.status.as_str(),
                default_states::DONE | default_states::CANCELLED
            ) {
                continue;
            }
            if let Err(reason) =
                transition_subtask_with_inherited_workflow(ctx, subtask, default_states::CANCELLED)
                    .await
            {
                return HookResult::Failed { reason };
            }
        }

        HookResult::Ok
    }
}

#[cfg(test)]
mod subtask_hook_test_support {
    use std::sync::Arc;

    use super::*;
    use db::{
        create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, CreateProject, CreateRepo,
        CreateTask, ProjectRepo, RepoRepo, SqliteDb, UpdateProject,
    };
    use events::EventBus;
    use serde_json::json;

    use crate::workflow::default_workflow;

    pub async fn build_ctx(
        from_state: &str,
        to_state: &str,
        subtask_statuses: &[&str],
    ) -> (HookContext, Vec<String>) {
        let db = Arc::new(sqlite_db().await);
        let now = now_rfc3339();
        let project_id = new_uuid_v4();
        let repo_id = new_uuid_v4();
        let root_id = new_uuid_v4();

        ProjectRepo::create(
            &*db,
            CreateProject {
                id: project_id.clone(),
                name: "Forge".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project creates");

        RepoRepo::create(
            &*db,
            CreateRepo {
                id: repo_id.clone(),
                project_id: project_id.clone(),
                name: "forge".to_owned(),
                remote_url: "https://example.com/forge.git".to_owned(),
                local_path: None,
                work_mode: db::WorkMode::DirectMerge,
                default_branch: "main".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("repo creates");
        ProjectRepo::update_at_version(
            &*db,
            UpdateProject {
                id: project_id.clone(),
                name: None,
                settings: None,
                primary_repo_id: Some(Some(repo_id.clone())),
                paused_at: None,
                updated_at: now_rfc3339(),
            },
            ProjectRepo::get_by_id(&*db, &project_id)
                .await
                .expect("fixture Project lookup")
                .expect("fixture Project exists")
                .version,
            None,
        )
        .await
        .expect("project primary repo updates");

        TaskRepo::create(
            &*db,
            CreateTask {
                id: root_id.clone(),
                project_id: project_id.clone(),
                repo_id: Some(repo_id.clone()),
                parent_task_id: None,
                subtask_order: None,
                assignee_type: None,
                assignee_id: None,
                title: "root".to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: from_state.to_owned(),
                is_automation: false,
                priority: 0,
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("root task creates");

        let mut subtask_ids = Vec::new();
        for (index, status) in subtask_statuses.iter().enumerate() {
            let subtask_id = new_uuid_v4();
            TaskRepo::create(
                &*db,
                CreateTask {
                    id: subtask_id.clone(),
                    project_id: project_id.clone(),
                    repo_id: Some(repo_id.clone()),
                    parent_task_id: Some(root_id.clone()),
                    subtask_order: Some(index as i64),
                    assignee_type: None,
                    assignee_id: None,
                    title: format!("subtask {index}"),
                    description: None,
                    task_type: "task".to_owned(),
                    status: (*status).to_owned(),
                    is_automation: false,
                    priority: 0,
                    task_state_config: None,
                    merge_config: None,
                    plan: None,
                    created_at: now.clone(),
                    updated_at: now.clone(),
                },
            )
            .await
            .expect("subtask creates");
            subtask_ids.push(subtask_id);
        }

        (
            HookContext {
                task_id: root_id,
                project_id,
                from_state: from_state.to_owned(),
                to_state: to_state.to_owned(),
                db,
                event_bus: Arc::new(EventBus::new(16)),
                gate_config: None,
                workflow: Arc::new(default_workflow::default_workflow()),
                triggered_by: api_types::Actor::system(api_types::SystemComponent::Test),
                review_runner: None,
                merge_service: None,
                cleanup_scheduler: None,
                task_executor: None,
                daemon_connections: None,
                workspace_exec_locks: None,
                terminal_activity: None,
                workspace_root: std::path::PathBuf::new(),
                repo_cache_locks: None,
                workspace_id: None,
                agent_id: None,
                execution_id: None,
                state_config: json!({}),
            },
            subtask_ids,
        )
    }

    pub async fn assert_status(ctx: &HookContext, task_id: &str, expected: &str) {
        let task = TaskRepo::get_by_id(&*ctx.db, task_id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert_eq!(task.status, expected);
    }

    async fn sqlite_db() -> SqliteDb {
        let pool = create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        run_migrations(&pool).await.expect("migrations run");
        SqliteDb::new(pool)
    }

    pub const REVIEW: &str = default_states::REVIEW;
    pub const IN_PROGRESS: &str = default_states::IN_PROGRESS;
    pub const TODO: &str = default_states::TODO;
    pub const DONE: &str = default_states::DONE;
    pub const CANCELLED: &str = default_states::CANCELLED;
    pub const MERGING: &str = default_states::MERGING;
}

#[cfg(test)]
mod subtask_sequence_complete {
    use super::{subtask_hook_test_support::*, *};

    #[tokio::test]
    async fn fails_when_non_cancelled_subtask_is_not_done() {
        let (ctx, subtask_ids) =
            build_ctx(IN_PROGRESS, REVIEW, &[IN_PROGRESS, DONE, CANCELLED]).await;

        let result = SubtaskSequenceComplete.execute(&ctx).await;

        match result {
            HookResult::Failed { reason } => {
                assert!(reason.contains("SUBTASK_SEQUENCE_NOT_COMPLETE"));
                assert!(reason.contains(&subtask_ids[0]));
                assert!(!reason.contains(&subtask_ids[1]));
                assert!(!reason.contains(&subtask_ids[2]));
            }
            other => panic!("expected failed result, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn skips_when_not_entering_review() {
        let (ctx, _) = build_ctx(IN_PROGRESS, DONE, &[DONE]).await;

        let result = SubtaskSequenceComplete.execute(&ctx).await;

        assert!(matches!(result, HookResult::Skipped { .. }));
    }
}

#[cfg(test)]
mod propagate_done_to_subtasks {
    use super::{subtask_hook_test_support::*, *};

    #[tokio::test]
    async fn marks_non_cancelled_subtasks_done() {
        let (ctx, subtask_ids) = build_ctx(MERGING, DONE, &[TODO, IN_PROGRESS, CANCELLED]).await;

        let result = PropagateDoneToSubtasks.execute(&ctx).await;

        assert!(matches!(result, HookResult::Ok));
        assert_status(&ctx, &subtask_ids[0], DONE).await;
        assert_status(&ctx, &subtask_ids[1], DONE).await;
        assert_status(&ctx, &subtask_ids[2], CANCELLED).await;
    }
}

#[cfg(test)]
mod cancel_pending_subtasks {
    use super::{subtask_hook_test_support::*, *};

    #[tokio::test]
    async fn cancels_non_terminal_subtasks() {
        let (ctx, subtask_ids) =
            build_ctx(IN_PROGRESS, CANCELLED, &[TODO, IN_PROGRESS, DONE]).await;

        let result = CancelPendingSubtasks.execute(&ctx).await;

        assert!(matches!(result, HookResult::Ok));
        assert_status(&ctx, &subtask_ids[0], CANCELLED).await;
        assert_status(&ctx, &subtask_ids[1], CANCELLED).await;
        assert_status(&ctx, &subtask_ids[2], DONE).await;
    }
}
