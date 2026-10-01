//! Shared Project slot projection for admission and Project reads.

use std::collections::HashSet;

use api_types::{Actor, ProjectSettings, ProjectSlots, StateKind, SystemComponent};
use db::{
    PageRequest, Project, ReviewRepo, ReviewStatus, SortBy, SortOrder, TaskListQuery, TaskRepo,
};

use crate::{workflow::engine::WorkflowEngine, Result, ServiceError};

use super::helpers;

/// Count visible Tasks by their effective workflow state. Coordination roots
/// with visible subtasks consume no slot, and blocking annotations or the
/// latest awaiting-human Review park an active/gate Task instead.
///
/// Tasks are queried in bounded status-filtered pages; latest Reviews are
/// loaded once per page and coordination roots once per Project.
pub async fn load_project_slots(db: &db::SqliteDb, project: &Project) -> Result<ProjectSlots> {
    let settings: ProjectSettings = serde_json::from_str(&project.settings)
        .map_err(|error| ServiceError::invalid_operation(format!("invalid settings: {error}")))?;
    let mut slots = ProjectSlots {
        limit: settings.max_active_tasks,
        ..ProjectSlots::default()
    };
    let project_workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let subtask_workflow = WorkflowEngine::resolve_subtask_workflow();
    let statuses: Vec<String> = project_workflow
        .states
        .iter()
        .chain(&subtask_workflow.states)
        .filter(|state| {
            matches!(
                state.kind,
                StateKind::Initial | StateKind::Active | StateKind::Gate
            )
        })
        .map(|state| state.name.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if statuses.is_empty() {
        return Ok(slots);
    }
    let roots = crate::task_hierarchy::coordination_root_ids_with_subtasks(db, &project.id).await?;
    let actor = Actor::system(SystemComponent::TaskDispatcher);
    let mut cursor = None;
    loop {
        let page = TaskRepo::list(
            db,
            TaskListQuery {
                project_id: project.id.clone(),
                q: None,
                statuses: statuses.clone(),
                agent_ids: Vec::new(),
                assignee_types: Vec::new(),
                assignee_ids: Vec::new(),
                priority: None,
                include_archived: false,
                include_cancelled: false,
                include_deleted: false,
                page: PageRequest {
                    cursor,
                    limit: 200,
                    include_total: false,
                    sort_by: SortBy::CreatedAt,
                    sort_order: SortOrder::Asc,
                },
            },
        )
        .await?;
        let mut eligible = Vec::new();
        for task in &page.items {
            let workflow = WorkflowEngine::resolve_workflow_for_task(
                task,
                &project.workflow_definition,
                &actor,
            );
            match workflow.state_kind(&task.status) {
                Some(StateKind::Initial) => slots.queued += 1,
                Some(StateKind::Active | StateKind::Gate) => eligible.push(task),
                _ => {}
            }
        }
        let review_task_ids: Vec<_> = eligible
            .iter()
            .filter(|task| !helpers::has_blocking_annotation(task))
            .map(|task| task.id.as_str())
            .collect();
        let awaiting_human: HashSet<_> =
            ReviewRepo::list_latest_reviews_for_tasks(db, &review_task_ids)
                .await?
                .into_iter()
                .filter(|review| review.status == ReviewStatus::AwaitingHuman)
                .map(|review| review.task_id)
                .collect();
        for task in eligible {
            if helpers::has_blocking_annotation(task) || awaiting_human.contains(&task.id) {
                slots.parked += 1;
            } else if !roots.contains(&task.id) {
                slots.active += 1;
            }
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    Ok(slots)
}

#[cfg(test)]
mod tests {
    use db::{CreateProject, CreateTask, ExecutionRepo, ProjectRepo};

    use super::*;

    async fn fixture() -> (db::SqliteDb, Project) {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = db::SqliteDb::new(pool);
        let now = db::now_rfc3339();
        let project = ProjectRepo::create(
            &db,
            CreateProject {
                id: db::new_uuid_v4(),
                name: "Slots".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
        (db, project)
    }

    async fn task(
        db: &db::SqliteDb,
        project: &Project,
        status: &str,
        parent: Option<&str>,
    ) -> db::Task {
        let now = db::now_rfc3339();
        TaskRepo::create(
            db,
            CreateTask {
                id: db::new_uuid_v4(),
                project_id: project.id.clone(),
                parent_task_id: parent.map(str::to_owned),
                subtask_order: parent.map(|_| 0),
                assignee_type: None,
                assignee_id: None,
                title: status.to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: status.to_owned(),
                is_automation: false,
                priority: 0,
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap()
    }

    async fn review(db: &db::SqliteDb, task: &db::Task, attempt: i64, status: ReviewStatus) {
        let now = db::now_rfc3339();
        let execution = ExecutionRepo::create(
            db,
            db::CreateExecution {
                id: db::new_uuid_v4(),
                task_id: task.id.clone(),
                agent_id: None,
                role: "coder".to_owned(),
                status: db::ExecutionStatus::Completed,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                workspace_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .unwrap();
        ReviewRepo::create(
            db,
            db::CreateReview {
                id: db::new_uuid_v4(),
                task_id: task.id.clone(),
                execution_id: execution.id,
                attempt_number: attempt,
                status,
                step_results_json: "{}".to_owned(),
                started_at: now.clone(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn review_and_conflict_repair_hold_slots() {
        let (db, project) = fixture().await;
        for status in [
            "in_progress",
            "in_progress",
            "review",
            "review",
            "merge_failed",
        ] {
            let task = task(&db, &project, status, None).await;
            if status == "merge_failed" {
                // Ordinary conflict handoff still runs the repair role.
                sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
                    .bind(r#"{"type":"merge_conflict","recovery_actions":[]}"#)
                    .bind(&task.id)
                    .execute(db.pool())
                    .await
                    .unwrap();
            }
        }
        assert_eq!(
            load_project_slots(&db, &project).await.unwrap(),
            ProjectSlots {
                limit: 5,
                active: 5,
                parked: 0,
                queued: 0
            }
        );
    }

    #[tokio::test]
    async fn parked_tasks_and_coordination_roots_release_slots() {
        let (db, project) = fixture().await;
        let owner_review = task(&db, &project, "review", None).await;
        sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
            .bind(r#"{"type":"review_needs_owner","message":"needs another OS"}"#)
            .bind(&owner_review.id)
            .execute(db.pool())
            .await
            .unwrap();
        let human_review = task(&db, &project, "planning", None).await;
        review(&db, &human_review, 1, ReviewStatus::AwaitingHuman).await;
        let failed = task(&db, &project, "in_progress", None).await;
        sqlx::query("UPDATE task SET failed_json = '{}' WHERE id = ?")
            .bind(&failed.id)
            .execute(db.pool())
            .await
            .unwrap();
        let blocked = task(&db, &project, "merge_failed", None).await;
        sqlx::query("UPDATE task SET blocked_json = '{}' WHERE id = ?")
            .bind(&blocked.id)
            .execute(db.pool())
            .await
            .unwrap();

        let root = task(&db, &project, "in_progress", None).await;
        task(&db, &project, "done", Some(&root.id)).await;
        let deleted_child_root = task(&db, &project, "in_progress", None).await;
        let deleted_child = task(&db, &project, "done", Some(&deleted_child_root.id)).await;
        sqlx::query("UPDATE task SET deleted_at = ? WHERE id = ?")
            .bind(db::now_rfc3339())
            .bind(&deleted_child.id)
            .execute(db.pool())
            .await
            .unwrap();
        let resumed_review = task(&db, &project, "review", None).await;
        review(&db, &resumed_review, 1, ReviewStatus::AwaitingHuman).await;
        review(&db, &resumed_review, 2, ReviewStatus::Passed).await;
        task(&db, &project, "todo", None).await;
        task(&db, &project, "backlog", None).await;
        task(&db, &project, "done", None).await;
        for column in ["archived_at", "deleted_at"] {
            let hidden = task(&db, &project, "in_progress", None).await;
            sqlx::query(&format!("UPDATE task SET {column} = ? WHERE id = ?"))
                .bind(db::now_rfc3339())
                .bind(&hidden.id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        assert_eq!(
            load_project_slots(&db, &project).await.unwrap(),
            ProjectSlots {
                limit: 5,
                active: 2,
                parked: 4,
                queued: 1
            }
        );
    }

    #[tokio::test]
    async fn slot_counts_use_custom_and_inherited_subtask_state_kinds() {
        let (db, mut project) = fixture().await;
        let mut workflow = crate::workflow::default_workflow::default_workflow();
        let implementation = workflow
            .states
            .iter_mut()
            .find(|state| state.name == "in_progress")
            .unwrap();
        implementation.kind = StateKind::Custom;
        let planning = workflow
            .states
            .iter_mut()
            .find(|state| state.name == "planning")
            .unwrap();
        planning.name = "designing".to_owned();
        project.workflow_definition = serde_json::to_string(&workflow).unwrap();
        project.settings = r#"{"max_active_tasks":0}"#.to_owned();
        let root = task(&db, &project, "in_progress", None).await;
        task(&db, &project, "in_progress", Some(&root.id)).await;
        task(&db, &project, "in_progress", None).await;
        task(&db, &project, "designing", None).await;
        assert_eq!(
            load_project_slots(&db, &project).await.unwrap(),
            ProjectSlots {
                limit: 0,
                active: 2,
                parked: 0,
                queued: 0
            }
        );
    }

    #[tokio::test]
    async fn slot_counts_cover_all_status_filtered_pages() {
        let (db, project) = fixture().await;
        for _ in 0..201 {
            task(&db, &project, "todo", None).await;
        }
        task(&db, &project, "planning", None).await;
        assert_eq!(
            load_project_slots(&db, &project).await.unwrap(),
            ProjectSlots {
                limit: 5,
                active: 1,
                parked: 0,
                queued: 201
            }
        );
    }
}
