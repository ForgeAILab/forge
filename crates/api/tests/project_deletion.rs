mod common;

use api_types::{ProjectResponse, TaskResponse};
use axum::http::{Method, StatusCode};
use db::{
    new_uuid_v4, now_rfc3339, CreateExecution, CreateRepo, CreateReview, ExecutionRepo,
    ExecutionStatus, RepoRepo, ReviewRepo, ReviewStatus, WorkMode,
};
use serde_json::json;

#[tokio::test]
async fn project_delete_removes_all_managed_workspaces_but_preserves_linked_repo() {
    let workspace = common::TestDir::new("project-delete-managed-repo");
    let harness = common::test_app(workspace.path(), "project-delete-managed-repo").await;
    let mut config = (*harness.state.effective_config).clone();
    config.forge.data_dir = workspace.path().join("data");
    config.workspace.root = workspace.path().to_path_buf();
    let state = (*harness.state).clone().with_effective_config(config);
    let web_dist = common::TestDir::new("project-delete-managed-repo-web");
    std::fs::write(web_dist.path().join("index.html"), "<html></html>").expect("web fixture");
    let app = api::build_router(state, web_dist.path().to_path_buf());
    let token = common::test_jwt();
    let project: ProjectResponse = common::json_request_with_bearer(
        &app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({"name": "Delete managed repository"}),
        StatusCode::OK,
    )
    .await;
    let task: TaskResponse = common::json_request_with_bearer(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{}/tasks", project.id),
        &token,
        json!({
            "title": "Project-owned Task workspace",
            "description": "Make filesystem cleanup observable"
        }),
        StatusCode::OK,
    )
    .await;

    let managed_repo = workspace.path().join("repos").join("managed-repo");
    let linked_repo = workspace.path().join("linked-repo");
    let managed_repo_id = new_uuid_v4();
    let linked_repo_id = new_uuid_v4();
    let managed_cache = workspace.path().join(".repos").join(&managed_repo_id);
    let linked_cache = workspace.path().join(".repos").join(&linked_repo_id);
    let task_workspace = workspace.path().join(&task.id);
    let project_agent_workspace = workspace
        .path()
        .join("data")
        .join("projects")
        .join(&project.id);
    std::fs::create_dir_all(&managed_repo).expect("managed repository directory");
    std::fs::create_dir_all(&linked_repo).expect("linked repository directory");
    std::fs::create_dir_all(&managed_cache).expect("managed repository cache");
    std::fs::create_dir_all(&linked_cache).expect("linked repository cache");
    std::fs::create_dir_all(task_workspace.join("forge")).expect("Task workspace directory");
    std::fs::create_dir_all(project_agent_workspace.join("checkout"))
        .expect("Project Agent workspace directory");
    std::fs::write(managed_repo.join("tracked"), "managed").expect("managed content");
    std::fs::write(linked_repo.join("tracked"), "linked").expect("linked content");
    std::fs::write(managed_cache.join("objects"), "managed-cache").expect("managed cache content");
    std::fs::write(linked_cache.join("objects"), "linked-cache").expect("linked cache content");
    std::fs::write(task_workspace.join("forge").join("build-cache"), "task")
        .expect("Task workspace content");
    std::fs::write(
        project_agent_workspace.join("checkout").join("build-cache"),
        "project-agent",
    )
    .expect("Project Agent workspace content");

    let now = now_rfc3339();
    for (id, name, path) in [
        (managed_repo_id, "managed", managed_repo.as_path()),
        (linked_repo_id, "linked", linked_repo.as_path()),
    ] {
        RepoRepo::create(
            &*harness.state.db,
            CreateRepo {
                id,
                project_id: project.id.clone(),
                name: name.to_owned(),
                remote_url: path.to_string_lossy().into_owned(),
                local_path: Some(path.to_string_lossy().into_owned()),
                work_mode: WorkMode::DirectMerge,
                default_branch: "main".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("repository record");
    }

    let execution = ExecutionRepo::create(
        &*harness.state.db,
        CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: "reviewer".to_owned(),
            status: ExecutionStatus::Completed,
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
            before_sha: Some("base".to_owned()),
            after_sha: Some("candidate".to_owned()),
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("review execution record");
    ReviewRepo::create(
        &*harness.state.db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: execution.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Failed,
            step_results_json: "{}".to_owned(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("review record");
    sqlx::query(
        "INSERT INTO execution_review_contract
         (execution_id, task_id, contract_digest, source_digest, contract_json, created_at)
         VALUES (?, ?, ?, 'source', '{}', ?)",
    )
    .bind(&execution.id)
    .bind(&task.id)
    .bind(format!("contract-{}", execution.id))
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("immutable review contract");
    sqlx::query(
        "INSERT INTO execution_review_assessment
         (execution_id, conformance_json, created_at) VALUES (?, '{}', ?)",
    )
    .bind(&execution.id)
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("immutable review assessment");

    let response = common::raw_empty_request(
        &app,
        Method::DELETE,
        &format!("/api/v1/projects/{}", project.id),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(!managed_repo.exists());
    assert!(!managed_cache.exists());
    assert!(!linked_cache.exists());
    assert!(!task_workspace.exists());
    assert!(!project_agent_workspace.exists());
    assert!(linked_repo.join("tracked").is_file());
    let remaining_contracts: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM execution_review_contract WHERE task_id = ?")
            .bind(&task.id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("review contracts count");
    let remaining_assessments: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution_review_assessment WHERE execution_id = ?",
    )
    .bind(&execution.id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("review assessments count");
    assert_eq!(remaining_contracts, 0);
    assert_eq!(remaining_assessments, 0);
}
