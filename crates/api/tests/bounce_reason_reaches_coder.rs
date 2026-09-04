#![allow(dead_code, clippy::assertions_on_constants)]
use std::sync::Arc;

use api_types::Actor;
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AssigneeKind, CreateProject,
    CreateRepo, CreateTask, CreateTaskRoleAssignment, ProjectRepo, RepoRepo, SqliteDb, TaskRepo,
    TaskRoleAssignmentRepo, UpdateProject,
};
use events::EventBus;
use serde_json::json;
use services::{
    task_service::TransitionOptions,
    workflow::{
        default_roles, default_states, default_workflow,
        dispatch::loader::load_agent_dispatch_context,
    },
    TaskService,
};

#[tokio::test]
async fn last_manual_bounce_reason_is_loaded_for_coder_dispatch() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let task_id = seed_project_repo_and_task(&db, default_states::IN_PROGRESS).await;
    assign_human_reviewer(&db, &task_id).await;

    let review = service
        .transition(
            task_id.clone(),
            default_states::REVIEW.to_owned(),
            TransitionOptions {
                version: 1,
                reason: Some("ready for review".to_owned()),
                triggered_by: Actor::agent("coder-agent"),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .expect("in_progress -> review succeeds")
        .task;

    let bounce_reason = "add tests for error path";
    let bounced = service
        .transition(
            task_id.clone(),
            default_states::IN_PROGRESS.to_owned(),
            (review.version, Some(bounce_reason.to_owned())),
        )
        .await
        .expect("manual review bounce succeeds")
        .task;
    assert_eq!(bounced.status, default_states::IN_PROGRESS);

    let workflow = default_workflow::default_workflow();
    let ctx = load_agent_dispatch_context(
        Arc::clone(&db),
        &task_id,
        default_roles::CODER,
        default_states::IN_PROGRESS,
        json!({}),
        Some("new_execution"),
        &workflow,
    )
    .await
    .expect("dispatch context loads");

    assert_eq!(
        ctx.last_manual_bounce_reason.as_deref(),
        Some(bounce_reason),
        "coder dispatch context should receive the latest non-rejection gate bounce reason"
    );
}

async fn assign_human_reviewer(db: &SqliteDb, task_id: &str) {
    let now = now_rfc3339();
    TaskRoleAssignmentRepo::assign(
        db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            role_name: default_roles::REVIEWER.to_owned(),
            assignee_type: Some(AssigneeKind::User),
            assignee_id: Some("human".to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("human reviewer assigned");
}

async fn sqlite_db() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    SqliteDb::new(pool)
}

async fn seed_project_repo_and_task(db: &SqliteDb, status: &str) -> String {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let task_id = new_uuid_v4();

    ProjectRepo::create(
        db,
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
        db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            local_path: None,
            work_mode: db::WorkMode::DirectMerge,
            remote_url: "https://example.com/repo.git".to_owned(),
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");

    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.clone(),
            project_id,
            repo_id: Some(repo_id),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "bounce reason task".to_owned(),
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
    .expect("task creates");

    task_id
}
