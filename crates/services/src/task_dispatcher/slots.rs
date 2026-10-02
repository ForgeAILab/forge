//! Shared Project slot projection for admission and Project reads.

use api_types::{CanonicalPhase, ProjectSettings, ProjectSlots, WorkflowDefinition};
use db::{Project, TaskRepo};

use crate::{workflow::engine::WorkflowEngine, Result, ServiceError};

use super::helpers;

fn slot_states(workflow: &WorkflowDefinition) -> String {
    let states: serde_json::Map<String, serde_json::Value> = workflow.states.iter().map(|state| {
        (state.name.clone(), serde_json::json!({
            "kind": state.kind,
            "owns_work": workflow.canonical_phase_for_state(&state.name) == CanonicalPhase::Review
                    || matches!(crate::workflow::effective_role(state), Some("reviewer" | "auditor"))
                    || state.hooks.on_enter.iter().any(|hook| hook.action == "run_merge"),
        }))
    }).collect();
    serde_json::Value::Object(states).to_string()
}

/// Each visible Task counts once by its effective workflow. A coordination
/// root holds a slot for its own execution/review/merge only while no child
/// holds an active slot. Unlimited Projects need no capacity projection.
pub async fn load_project_slots(db: &db::SqliteDb, project: &Project) -> Result<ProjectSlots> {
    let settings: ProjectSettings = serde_json::from_str(&project.settings)
        .map_err(|error| ServiceError::invalid_operation(format!("invalid settings: {error}")))?;
    let mut slots = ProjectSlots {
        limit: settings.max_active_tasks,
        ..ProjectSlots::default()
    };
    if slots.limit == 0 {
        return Ok(slots);
    }
    let project_states = slot_states(&WorkflowEngine::resolve_workflow(
        &project.workflow_definition,
    ));
    let subtask_states = slot_states(&WorkflowEngine::resolve_subtask_workflow());
    let (active, parked, queued) = TaskRepo::count_project_slots(
        db,
        &project.id,
        &project_states,
        &subtask_states,
        &serde_json::json!(helpers::BLOCKING_ANNOTATION_KINDS).to_string(),
    )
    .await?;
    slots.active = u32::try_from(active).unwrap_or(u32::MAX);
    slots.parked = u32::try_from(parked).unwrap_or(u32::MAX);
    slots.queued = u32::try_from(queued).unwrap_or(u32::MAX);
    Ok(slots)
}

/// Counts plus the revision observed by their statement. API readers may memoize
/// only when this fence matches the Project row they already loaded.
#[derive(Debug)]
pub struct ProjectSlotsRead {
    pub slots: ProjectSlots,
    pub revision: Option<(i64, i64)>,
}

/// Read all bounded Projects in one grouped statement. Unlimited Projects and
/// an empty input need no SQL. The dispatcher retains its uncached single read.
pub async fn load_projects_slots(
    db: &db::SqliteDb,
    projects: &[Project],
) -> Result<std::collections::HashMap<String, ProjectSlotsRead>> {
    let mut result = std::collections::HashMap::with_capacity(projects.len());
    let mut states = serde_json::Map::new();
    for project in projects {
        let settings: ProjectSettings =
            serde_json::from_str(&project.settings).map_err(|error| {
                ServiceError::invalid_operation(format!("invalid settings: {error}"))
            })?;
        result.insert(
            project.id.clone(),
            ProjectSlotsRead {
                slots: ProjectSlots {
                    limit: settings.max_active_tasks,
                    ..ProjectSlots::default()
                },
                revision: None,
            },
        );
        if settings.max_active_tasks != 0 {
            states.insert(
                project.id.clone(),
                serde_json::Value::String(slot_states(&WorkflowEngine::resolve_workflow(
                    &project.workflow_definition,
                ))),
            );
        }
    }
    if states.is_empty() {
        return Ok(result);
    }
    let mut pending: std::collections::HashSet<String> = states.keys().cloned().collect();
    for counts in TaskRepo::count_projects_slots(
        db,
        &serde_json::Value::Object(states).to_string(),
        &slot_states(&WorkflowEngine::resolve_subtask_workflow()),
        &serde_json::json!(helpers::BLOCKING_ANNOTATION_KINDS).to_string(),
    )
    .await?
    {
        pending.remove(&counts.project_id);
        let read = result.get_mut(&counts.project_id).expect("queried Project");
        read.revision = Some((counts.list_revision, counts.project_version));
        read.slots.active = u32::try_from(counts.active).unwrap_or(u32::MAX);
        read.slots.parked = u32::try_from(counts.parked).unwrap_or(u32::MAX);
        read.slots.queued = u32::try_from(counts.queued).unwrap_or(u32::MAX);
    }
    if let Some(id) = pending.into_iter().next() {
        return Err(ServiceError::not_found("project", id));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use db::{CreateProject, CreateTask, ExecutionRepo, ProjectRepo, ReviewRepo, ReviewStatus};

    use super::*;

    use api_types::{Actor, StateKind, SystemComponent};
    use db::{PageRequest, SortBy, SortOrder, TaskListQuery};
    use std::collections::HashSet;

    async fn assert_batch_matches(db: &db::SqliteDb, project: &Project) {
        let batch = load_projects_slots(db, std::slice::from_ref(project))
            .await
            .unwrap();
        assert_eq!(
            batch[&project.id].slots,
            load_project_slots(db, project).await.unwrap()
        );
    }

    async fn walk_project_slots(db: &db::SqliteDb, project: &Project) -> Result<ProjectSlots> {
        let settings: ProjectSettings =
            serde_json::from_str(&project.settings).map_err(|error| {
                ServiceError::invalid_operation(format!("invalid settings: {error}"))
            })?;
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
        let roots =
            crate::task_hierarchy::coordination_root_ids_with_subtasks(db, &project.id).await?;
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
                let machine_wait = crate::deferred_dispatch::current_dispatch_disposition(task).is_some_and(|d| matches!(d.capability.as_str(), "machine_capacity" | "project_capacity"))
                    && !sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM execution WHERE task_id = ? AND status = 'running')")
                        .bind(&task.id).fetch_one(db.pool()).await?;
                if helpers::has_blocking_annotation(task)
                    || awaiting_human.contains(&task.id)
                    || machine_wait
                {
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

    #[tokio::test]
    async fn machine_capacity_waiters_match_single_batch_and_walk() {
        let (db, project) = fixture().await;
        let waiter = task(&db, &project, "in_progress", None).await;
        crate::deferred_dispatch::record_dispatch_disposition(
            &db,
            &waiter,
            "machine_capacity",
            "waiting for a machine run slot",
        )
        .await
        .unwrap();
        let slots = load_project_slots(&db, &project).await.unwrap();
        assert_eq!((slots.active, slots.parked), (0, 1));
        assert_eq!(slots, walk_project_slots(&db, &project).await.unwrap());
        assert_batch_matches(&db, &project).await;
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
        assert_batch_matches(&db, &project).await;
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
        assert_batch_matches(&db, &project).await;
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
        project.settings = r#"{"max_active_tasks":5}"#.to_owned();
        let root = task(&db, &project, "in_progress", None).await;
        task(&db, &project, "in_progress", Some(&root.id)).await;
        task(&db, &project, "in_progress", None).await;
        task(&db, &project, "designing", None).await;
        assert_batch_matches(&db, &project).await;
        assert_eq!(
            load_project_slots(&db, &project).await.unwrap(),
            ProjectSlots {
                limit: 5,
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
        assert_batch_matches(&db, &project).await;
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
    #[tokio::test]
    async fn project_limit_aggregate_matches_row_walk_and_unlimited_skips_query() {
        let (db, mut project) = fixture().await;
        for status in [
            "planning",
            "in_progress",
            "review",
            "merging",
            "merge_failed",
            "todo",
            "todo",
            "backlog",
            "done",
            "cancelled",
        ] {
            task(&db, &project, status, None).await;
        }
        let parked = task(&db, &project, "review", None).await;
        sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
            .bind(r#"{"type":"review_needs_owner"}"#)
            .bind(&parked.id)
            .execute(db.pool())
            .await
            .unwrap();
        let human = task(&db, &project, "review", None).await;
        review(&db, &human, 1, ReviewStatus::AwaitingHuman).await;
        let resumed = task(&db, &project, "review", None).await;
        review(&db, &resumed, 1, ReviewStatus::AwaitingHuman).await;
        review(&db, &resumed, 2, ReviewStatus::Passed).await;
        let root = task(&db, &project, "in_progress", None).await;
        task(&db, &project, "in_progress", Some(&root.id)).await;
        task(&db, &project, "done", Some(&root.id)).await;
        for annotation in ["{}", "invalid", r#"{"type":"merge_conflict"}"#] {
            let active = task(&db, &project, "in_progress", None).await;
            sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
                .bind(annotation)
                .bind(&active.id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        for column in ["blocked_json", "failed_json"] {
            let parked = task(&db, &project, "planning", None).await;
            sqlx::query(&format!("UPDATE task SET {column} = '{{}}' WHERE id = ?"))
                .bind(&parked.id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        for column in ["archived_at", "deleted_at"] {
            let hidden = task(&db, &project, "review", None).await;
            sqlx::query(&format!("UPDATE task SET {column} = ? WHERE id = ?"))
                .bind(db::now_rfc3339())
                .bind(&hidden.id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        assert_batch_matches(&db, &project).await;
        assert_eq!(
            load_project_slots(&db, &project).await.unwrap(),
            walk_project_slots(&db, &project).await.unwrap()
        );
        project.settings = r#"{"max_active_tasks":0}"#.to_owned();
        // Even without a Task table, unlimited projection needs no SQL.
        let empty = db::SqliteDb::new(db::create_sqlite_pool("sqlite::memory:").await.unwrap());
        assert_eq!(
            load_project_slots(&empty, &project).await.unwrap(),
            ProjectSlots {
                limit: 0,
                active: 0,
                parked: 0,
                queued: 0
            }
        );
    }

    #[tokio::test]
    async fn project_limit_coordination_roots_count_own_work_once_after_children_settle() {
        let (db, project) = fixture().await;
        for status in ["review", "merging", "merge_failed"] {
            let root = task(&db, &project, status, None).await;
            task(&db, &project, "done", Some(&root.id)).await;
        }
        let running_root = task(&db, &project, "in_progress", None).await;
        task(&db, &project, "done", Some(&running_root.id)).await;
        review(&db, &running_root, 1, ReviewStatus::Passed).await;
        sqlx::query("UPDATE execution SET status = 'running' WHERE task_id = ?")
            .bind(&running_root.id)
            .execute(db.pool())
            .await
            .unwrap();
        let root_with_active_child = task(&db, &project, "review", None).await;
        task(
            &db,
            &project,
            "in_progress",
            Some(&root_with_active_child.id),
        )
        .await;
        let coordinating = task(&db, &project, "in_progress", None).await;
        task(&db, &project, "done", Some(&coordinating.id)).await;
        assert_batch_matches(&db, &project).await;
        assert_eq!(load_project_slots(&db, &project).await.unwrap().active, 5);
        let parked_root = task(&db, &project, "review", None).await;
        task(&db, &project, "done", Some(&parked_root.id)).await;
        review(&db, &parked_root, 1, ReviewStatus::AwaitingHuman).await;
        let slots = load_project_slots(&db, &project).await.unwrap();
        assert_eq!((slots.active, slots.parked), (5, 1));
    }
    #[tokio::test]
    async fn placement_waits_hold_slots_and_owner_blockers_share_the_aggregate() {
        let (db, project) = fixture().await;
        for key in ["owner_wait", "deferred_dispatch"] {
            let waiting = task(&db, &project, "in_progress", None).await;
            sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
                .bind(
                    serde_json::json!({key: {"daemon_id":"offline", "target_state":"in_progress"}})
                        .to_string(),
                )
                .bind(&waiting.id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        let annotations = [
            (
                "recovery_required",
                "owner_disconnected_timeout",
                serde_json::json!(["reexecute", "cancel_task"]),
            ),
            (
                "before_work_hook_failed",
                "review_ci_infrastructure",
                serde_json::json!(["retry_hook", "cancel_task"]),
            ),
            (
                "before_work_hook_failed",
                "review_ci_infrastructure_exhausted",
                serde_json::json!(["retry_hook", "cancel_task"]),
            ),
            (
                "before_work_hook_failed",
                "review_ci_unavailable",
                serde_json::json!(["retry_hook", "cancel_task"]),
            ),
            (
                "workspace_reset_required",
                "workspace_reset_required",
                serde_json::json!(["reset_to_initial", "cancel_task"]),
            ),
            (
                "recovery_required",
                "owner_lost_execution",
                serde_json::json!(["reexecute", "cancel_task"]),
            ),
            (
                "before_work_hook_failed",
                "before_work_hook_failed",
                serde_json::json!(["retry_hook", "cancel_task"]),
            ),
        ];
        for (kind, blocking_reason, actions) in &annotations {
            assert!(helpers::is_blocking_annotation_type(kind));
            let parked = task(&db, &project, "review", None).await;
            sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
                .bind(
                    serde_json::json!({
                        "type": kind, "blocking_reason": blocking_reason,
                        "blocked_at": db::now_rfc3339(), "blocked_by": "system:workflow",
                        "message": "workspace owner could not run the operation",
                        "recovery_actions": actions,
                    })
                    .to_string(),
                )
                .bind(&parked.id)
                .execute(db.pool())
                .await
                .unwrap();
            let parked = TaskRepo::get_by_id(&db, &parked.id, false)
                .await
                .unwrap()
                .unwrap();
            assert!(helpers::has_blocking_annotation(&parked));
        }
        let slots = load_project_slots(&db, &project).await.unwrap();
        assert_eq!((slots.active, slots.parked), (2, annotations.len() as u32));
        assert_eq!(slots, walk_project_slots(&db, &project).await.unwrap());
    }
    #[tokio::test]
    async fn batched_slots_match_multiple_workflows_and_cross_project_children() {
        let (db, mut first) = fixture().await;
        let now = db::now_rfc3339();
        let mut projects = vec![];
        for settings in ["{}", "{}", r#"{"max_active_tasks":0}"#] {
            projects.push(
                ProjectRepo::create(
                    &db,
                    CreateProject {
                        id: db::new_uuid_v4(),
                        name: "Batch".to_owned(),
                        settings: settings.to_owned(),
                        workflow_definition: "{}".to_owned(),
                        primary_repo_id: None,
                        owner_id: None,
                        created_at: now.clone(),
                        updated_at: now.clone(),
                    },
                )
                .await
                .unwrap(),
            );
        }
        let mut workflow = crate::workflow::default_workflow::default_workflow();
        workflow
            .states
            .iter_mut()
            .find(|s| s.name == "planning")
            .unwrap()
            .name = "designing".to_owned();
        workflow
            .states
            .iter_mut()
            .find(|s| s.name == "in_progress")
            .unwrap()
            .kind = StateKind::Custom;
        first.workflow_definition = serde_json::to_string(&workflow).unwrap();
        task(&db, &first, "designing", None).await;
        let custom = task(&db, &first, "in_progress", None).await;
        task(&db, &first, "in_progress", Some(&custom.id)).await;
        let root = task(&db, &first, "review", None).await;
        // The single-Project SQL sees the child for existence, but only its
        // own Project's visible children can displace root-owned work.
        task(&db, &projects[0], "in_progress", Some(&root.id)).await;
        for _ in 0..201 {
            task(&db, &projects[0], "todo", None).await;
        }
        projects.push(first);
        let batch = load_projects_slots(&db, &projects).await.unwrap();
        for project in &projects {
            assert_eq!(
                batch[&project.id].slots,
                load_project_slots(&db, project).await.unwrap()
            );
        }
        assert_eq!(batch[&projects[0].id].slots.queued, 201);
        assert_eq!(batch[&projects[3].id].slots.active, 3);
        assert_eq!(batch[&projects[1].id].slots.active, 0);
        let empty = db::SqliteDb::new(db::create_sqlite_pool("sqlite::memory:").await.unwrap());
        assert!(load_projects_slots(&empty, &[]).await.unwrap().is_empty());
        assert_eq!(
            load_projects_slots(&empty, &projects[2..3]).await.unwrap()[&projects[2].id]
                .slots
                .limit,
            0
        );
    }
}
