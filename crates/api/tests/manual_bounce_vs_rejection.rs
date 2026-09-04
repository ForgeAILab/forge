#![allow(dead_code, clippy::assertions_on_constants)]
use std::sync::Arc;

use api::AppState;
use api_types::RejectGateRequest;
use axum::extract::{Json, Path, State};
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AssigneeKind, CreateProject,
    CreateRepo, CreateTask, CreateTaskRoleAssignment, ProjectRepo, RepoRepo, SqliteDb, TaskRepo,
    TaskRoleAssignmentRepo, TransitionLog, TransitionLogRepo, UpdateProject,
};
use events::EventBus;
use services::{
    workflow::{default_roles, default_states, default_workflow},
    TaskService,
};

#[tokio::test]
async fn manual_bounce_is_not_a_rejection_but_gate_reject_is() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus));
    let api_state = AppState::new(Arc::clone(&db), Arc::clone(&event_bus), false);

    let manual_task_id = seed_project_repo_and_task(&db, default_states::IN_PROGRESS).await;
    assign_human_reviewer(&db, &manual_task_id).await;
    let manual_review = drive_to_review(&service, &manual_task_id).await;
    let retries_before_manual_bounce = remaining_review_retries(&db, &manual_task_id).await;

    let manual_bounce = service
        .transition(
            manual_task_id.clone(),
            default_states::IN_PROGRESS.to_owned(),
            (manual_review.version, Some("add tests".to_owned())),
        )
        .await
        .expect("manual bounce succeeds");

    assert_eq!(manual_bounce.task.status, default_states::IN_PROGRESS);
    let manual_bounce_log = transition_log_for_reason(&db, &manual_task_id, "add tests").await;
    assert_eq!(manual_bounce_log.from_state, default_states::REVIEW);
    assert_eq!(manual_bounce_log.to_state, default_states::IN_PROGRESS);
    assert!(
        !manual_bounce_log.rejection,
        "manual review -> in_progress transition must not spend rejection budget"
    );
    assert_eq!(
        remaining_review_retries(&db, &manual_task_id).await,
        retries_before_manual_bounce,
        "manual bounce should leave review retry budget unchanged"
    );

    let rejected_task_id = seed_project_repo_and_task(&db, default_states::IN_PROGRESS).await;
    assign_human_reviewer(&db, &rejected_task_id).await;
    let rejected_review = drive_to_review(&service, &rejected_task_id).await;
    let retries_before_reject = remaining_review_retries(&db, &rejected_task_id).await;

    let Json(rejection) = api::routes::tasks::reject_gate(
        State(api_state),
        Path((rejected_task_id.clone(), default_states::REVIEW.to_owned())),
        Json(RejectGateRequest {
            reason: "failed CI".to_owned(),
            version: rejected_review.version,
        }),
    )
    .await
    .expect("gate reject endpoint succeeds");

    assert_eq!(rejection.status, default_states::IN_PROGRESS);
    let rejection_log =
        transition_log_for_reason(&db, &rejected_task_id, "gate rejected: failed CI").await;
    assert_eq!(rejection_log.from_state, default_states::REVIEW);
    assert_eq!(rejection_log.to_state, default_states::IN_PROGRESS);
    assert!(
        rejection_log.rejection,
        "gate rejection must be recorded separately from a manual bounce"
    );
    assert_eq!(
        remaining_review_retries(&db, &rejected_task_id).await,
        retries_before_reject - 1,
        "gate rejection should decrement review retry budget"
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
            title: "manual bounce task".to_owned(),
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

async fn drive_to_review(service: &TaskService, task_id: &str) -> db::Task {
    service
        .transition(
            task_id.to_owned(),
            default_states::REVIEW.to_owned(),
            (1_i64, Some("ready for review".to_owned())),
        )
        .await
        .expect("in_progress -> review succeeds")
        .task
}

async fn remaining_review_retries(db: &SqliteDb, task_id: &str) -> i64 {
    let workflow = default_workflow::default_workflow();
    let review = workflow
        .states
        .iter()
        .find(|state| state.name == default_states::REVIEW)
        .expect("default review state exists");
    let max_rejections = review
        .gate_config
        .as_ref()
        .and_then(|config| config.max_rejections)
        .expect("review retry budget exists");
    let used = TransitionLogRepo::count_gate_rejections(db, task_id, default_states::REVIEW)
        .await
        .expect("rejection count loads");
    (i64::from(max_rejections) - used).max(0)
}

async fn transition_log_for_reason(db: &SqliteDb, task_id: &str, reason: &str) -> TransitionLog {
    TransitionLogRepo::list_by_task(db, task_id)
        .await
        .expect("transition logs load")
        .into_iter()
        .find(|entry| entry.trigger_reason == reason)
        .unwrap_or_else(|| panic!("transition log with reason '{reason}' exists"))
}
