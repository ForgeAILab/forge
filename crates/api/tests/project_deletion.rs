mod common;

use api_types::{ProjectResponse, TaskResponse, METHOD_EXECUTION_CANCEL};
use axum::http::{Method, StatusCode};
use db::{
    new_uuid_v4, now_rfc3339, AgentRepo, AgentStatus, CreateAgent, CreateExecution, CreateRepo,
    CreateReview, CreateTask, CreateWorkspace, CreateWorkspaceLease, ExecutionRepo,
    ExecutionStatus, ProjectRepo, RepoRepo, ReviewRepo, ReviewStatus, TaskRepo, WorkMode,
    WorkspaceLeaseRepo, WorkspaceRepo, WorkspaceStatus,
};
use serde_json::{json, Value};

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

#[tokio::test]
async fn project_delete_preserves_a_repository_path_reused_by_another_project() {
    let workspace = common::TestDir::new("project-delete-reused-repository-path");
    let harness = common::test_app(workspace.path(), "project-delete-reused-repository-path").await;
    let token = common::test_jwt();
    let deleted_project: ProjectResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({"name": "Project losing shared path"}),
        StatusCode::OK,
    )
    .await;
    let surviving_project: ProjectResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({"name": "Project reusing shared path"}),
        StatusCode::OK,
    )
    .await;

    let shared_path = workspace
        .path()
        .join("repos")
        .join("shared-repository-path");
    std::fs::create_dir_all(&shared_path).expect("shared repository directory");
    std::fs::write(shared_path.join("owned-by-survivor"), "preserve")
        .expect("shared repository content");
    let now = now_rfc3339();
    let deleted_repo_id = new_uuid_v4();
    let surviving_repo_id = new_uuid_v4();
    for (repo_id, project_id, name) in [
        (
            deleted_repo_id.clone(),
            deleted_project.id.clone(),
            "deleted-project-repo",
        ),
        (
            surviving_repo_id.clone(),
            surviving_project.id.clone(),
            "surviving-project-repo",
        ),
    ] {
        RepoRepo::create(
            &*harness.state.db,
            CreateRepo {
                id: repo_id,
                project_id,
                name: name.to_owned(),
                remote_url: shared_path.to_string_lossy().into_owned(),
                local_path: Some(shared_path.to_string_lossy().into_owned()),
                work_mode: WorkMode::DirectMerge,
                default_branch: "main".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("repository path reuse fixture");
    }

    // The surviving Project has already claimed the captured path when the
    // first deletion commits. The post-commit ownership recheck must therefore
    // skip removal instead of deleting the surviving Project's directory.
    let response = common::raw_empty_request(
        &harness.app,
        Method::DELETE,
        &format!("/api/v1/projects/{}", deleted_project.id),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(
        shared_path.join("owned-by-survivor").is_file(),
        "a reused repository path must not be removed"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project WHERE id = ?")
            .bind(&surviving_project.id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("surviving project count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM repo WHERE id = ?")
            .bind(&surviving_repo_id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("surviving repository count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM repo WHERE id = ?")
            .bind(&deleted_repo_id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("deleted repository count"),
        0
    );
}

#[tokio::test]
async fn project_delete_checks_project_role_in_use_state_and_force_cancels() {
    let workspace = common::TestDir::new("project-delete-guards");
    let harness = common::test_app(workspace.path(), "project-delete-guards").await;
    let mut config = (*harness.state.effective_config).clone();
    config.forge.data_dir = workspace.path().join("data");
    config.workspace.root = workspace.path().to_path_buf();
    let state = (*harness.state).clone().with_effective_config(config);
    let web_dist = common::TestDir::new("project-delete-guards-web");
    std::fs::write(web_dist.path().join("index.html"), "<html></html>").expect("web fixture");
    let app = api::build_router(state, web_dist.path().to_path_buf());
    let token = common::test_jwt();
    let project: ProjectResponse = common::json_request_with_bearer(
        &app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({"name": "Delete guard project"}),
        StatusCode::OK,
    )
    .await;

    // A project member without owner/admin authority must not reach either
    // the in-use count or filesystem cleanup path.
    sqlx::query("UPDATE project_member SET role = 'member' WHERE project_id = ? AND user_id = ?")
        .bind(&project.id)
        .bind("test-user-id")
        .execute(harness.state.db.pool())
        .await
        .expect("downgrade project member");
    let forbidden = common::raw_empty_request(
        &app,
        Method::DELETE,
        &format!("/api/v1/projects/{}", project.id),
    )
    .await;
    let forbidden_body: Value = common::parse_response(forbidden, StatusCode::FORBIDDEN).await;
    assert_eq!(forbidden_body["code"], "insufficient_role");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project WHERE id = ?")
            .bind(&project.id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("project remains after authorization rejection"),
        1
    );

    sqlx::query("UPDATE project_member SET role = 'owner' WHERE project_id = ? AND user_id = ?")
        .bind(&project.id)
        .bind("test-user-id")
        .execute(harness.state.db.pool())
        .await
        .expect("restore project owner role");
    let task: TaskResponse = common::json_request_with_bearer(
        &app,
        Method::POST,
        &format!("/api/v1/projects/{}/tasks", project.id),
        &token,
        json!({"title": "In-use task", "description": "guard"}),
        StatusCode::OK,
    )
    .await;
    let task_workspace = workspace.path().join(&task.id);
    std::fs::create_dir_all(&task_workspace).expect("task workspace fixture");

    let now = now_rfc3339();
    let execution = ExecutionRepo::create(
        &*harness.state.db,
        CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: "reviewer".to_owned(),
            status: ExecutionStatus::Running,
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
            updated_at: now,
        },
    )
    .await
    .expect("running execution fixture");

    let blocked = common::raw_empty_request(
        &app,
        Method::DELETE,
        &format!("/api/v1/projects/{}", project.id),
    )
    .await;
    let blocked_body: Value = common::parse_response(blocked, StatusCode::CONFLICT).await;
    assert_eq!(blocked_body["code"], "project_in_use");
    assert_eq!(blocked_body["details"]["running_executions"], 1);
    assert_eq!(blocked_body["details"]["active_leases"], 0);
    assert!(
        task_workspace.exists(),
        "rejected delete must not mutate paths"
    );

    let forced = common::raw_empty_request(
        &app,
        Method::DELETE,
        &format!("/api/v1/projects/{}?force=true", project.id),
    )
    .await;
    assert_eq!(forced.status(), StatusCode::NO_CONTENT);
    assert!(!task_workspace.exists());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project WHERE id = ?")
            .bind(&project.id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("deleted project count"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM execution WHERE id = ?")
            .bind(&execution.id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("execution cascade count"),
        0
    );
}

#[tokio::test]
async fn project_delete_reports_active_lease_without_running_execution_and_force_revokes_it() {
    let workspace = common::TestDir::new("project-delete-active-lease");
    let harness = common::test_app(workspace.path(), "project-delete-active-lease").await;
    let token = common::test_jwt();
    let project: ProjectResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({"name": "Delete active lease project"}),
        StatusCode::OK,
    )
    .await;

    let now = now_rfc3339();
    let repository_id = new_uuid_v4();
    RepoRepo::create(
        &*harness.state.db,
        CreateRepo {
            id: repository_id.clone(),
            project_id: project.id.clone(),
            name: "active-lease-repository".to_owned(),
            remote_url: "file:///tmp/active-lease-repository".to_owned(),
            local_path: None,
            work_mode: WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repository fixture");
    let project_record = ProjectRepo::get_by_id(&*harness.state.db, &project.id)
        .await
        .expect("project lookup")
        .expect("project exists");
    ProjectRepo::update_at_version(
        &*harness.state.db,
        db::UpdateProject {
            id: project.id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repository_id.clone())),
            paused_at: None,
            updated_at: now.clone(),
        },
        project_record.version,
        None,
    )
    .await
    .expect("project repository binding");

    let agent_id = new_uuid_v4();
    AgentRepo::create(
        &*harness.state.db,
        CreateAgent {
            id: agent_id.clone(),
            name: "Active lease worker".to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Busy,
            last_heartbeat_at: Some(now.clone()),
            is_default: false,
            paused: false,
            owner_id: Some("test-user-id".to_owned()),
            visibility: "account".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("agent fixture");

    let task_id = new_uuid_v4();
    let task = TaskRepo::create(
        &*harness.state.db,
        CreateTask {
            id: task_id.clone(),
            project_id: project.id.clone(),
            parent_task_id: None,
            assignee_type: Some("agent".to_owned()),
            assignee_id: Some(agent_id.clone()),
            title: "Stale active lease".to_owned(),
            description: Some("A lease remains after execution completion".to_owned()),
            task_type: "task".to_owned(),
            status: "in_progress".to_owned(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task fixture");
    let workspace_id = new_uuid_v4();
    let task_workspace = workspace.path().join(&task_id);
    std::fs::create_dir_all(&task_workspace).expect("workspace path fixture");
    WorkspaceRepo::create(
        &*harness.state.db,
        CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task_id.clone(),
            repo_id: repository_id.clone(),
            worktree_path: task_workspace.to_string_lossy().into_owned(),
            branch: format!("forge/{task_id}"),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace fixture");

    let execution = ExecutionRepo::create(
        &*harness.state.db,
        CreateExecution {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "coder".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: Some(now.clone()),
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace_id),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("running execution fixture");
    let lease =
        WorkspaceLeaseRepo::issue(
            &*harness.state.db,
            CreateWorkspaceLease {
                id: new_uuid_v4(),
                project_id: project.id.clone(),
                task_id: task_id.clone(),
                task_version: task.version,
                execution_id: execution.id.clone(),
                operation_idempotency_key: new_uuid_v4(),
                repository_binding_id: repository_id,
                base_ref: "main".to_owned(),
                role: "worker".to_owned(),
                capabilities_json: r#"["repository_write"]"#.to_owned(),
                assigned_principal_type: "agent".to_owned(),
                assigned_principal_id: agent_id,
                capability_profile_revision: "forge.capability-profile/v1".to_owned(),
                capability_profile_digest:
                    "sha256:eeb061a14ab862e1a7b16989ef637293ba538f46122ff28b30313d330dbae4a8"
                        .to_owned(),
                issuing_principal_type: "system".to_owned(),
                issuing_principal_id: "task-service-scheduler".to_owned(),
                issued_at: now.clone(),
                expires_at: "2099-01-01T00:00:00+00:00".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("active lease fixture");

    // Model a stale active lease: the execution has reached a terminal state,
    // but the lease was not revoked by the losing cleanup path. The deletion
    // guard must count the lease independently of running executions.
    sqlx::query("UPDATE execution SET status = 'completed', updated_at = ? WHERE id = ?")
        .bind(&now)
        .bind(&execution.id)
        .execute(harness.state.db.pool())
        .await
        .expect("complete execution while retaining lease");
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM workspace_lease WHERE id = ?")
            .bind(&lease.id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("active lease remains before deletion"),
        "active"
    );

    let blocked = common::raw_empty_request(
        &harness.app,
        Method::DELETE,
        &format!("/api/v1/projects/{}", project.id),
    )
    .await;
    let blocked_body: Value = common::parse_response(blocked, StatusCode::CONFLICT).await;
    assert_eq!(blocked_body["code"], "project_in_use");
    assert_eq!(blocked_body["details"]["running_executions"], 0);
    assert_eq!(blocked_body["details"]["active_leases"], 1);

    let forced = common::raw_empty_request(
        &harness.app,
        Method::DELETE,
        &format!("/api/v1/projects/{}?force=true", project.id),
    )
    .await;
    assert_eq!(forced.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project WHERE id = ?")
            .bind(&project.id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("deleted project count"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM workspace_lease WHERE id = ?")
            .bind(&lease.id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("deleted lease count"),
        0
    );
    assert!(
        !task_workspace.exists(),
        "force delete cleans leased worktree"
    );
}

#[tokio::test]
async fn project_delete_collects_workspace_paths_at_the_final_db_boundary() {
    let workspace = common::TestDir::new("project-delete-final-workspace-snapshot");
    let harness =
        common::test_app(workspace.path(), "project-delete-final-workspace-snapshot").await;
    let project: ProjectResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        &common::test_jwt(),
        json!({"name": "Delete final workspace snapshot"}),
        StatusCode::OK,
    )
    .await;
    let task: TaskResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/tasks", project.id),
        &common::test_jwt(),
        json!({"title": "Late workspace admission", "description": "guard"}),
        StatusCode::OK,
    )
    .await;
    let repository_id = new_uuid_v4();
    let repository_path = workspace.path().join("repos").join("late-repository");
    let repository_cache = workspace.path().join(".repos").join(&repository_id);
    std::fs::create_dir_all(&repository_path).expect("repository fixture");
    std::fs::create_dir_all(&repository_cache).expect("repository cache fixture");
    let now = now_rfc3339();
    RepoRepo::create(
        &*harness.state.db,
        CreateRepo {
            id: repository_id.clone(),
            project_id: project.id.clone(),
            name: "late-repository".to_owned(),
            remote_url: repository_path.to_string_lossy().into_owned(),
            local_path: Some(repository_path.to_string_lossy().into_owned()),
            work_mode: WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repository fixture");
    let worktree_path = workspace.path().join(&task.id);
    std::fs::create_dir_all(&worktree_path).expect("worktree fixture");
    WorkspaceRepo::create(
        &*harness.state.db,
        CreateWorkspace {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            repo_id: repository_id.clone(),
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch: format!("forge/{}", task.id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("workspace fixture");

    let collected = ProjectRepo::delete_with_workspace_paths(&*harness.state.db, &project.id)
        .await
        .expect("project deletion collects workspace paths");
    assert_eq!(collected.task_ids, vec![task.id.clone()]);
    assert_eq!(
        collected.workspace_paths,
        vec![worktree_path.to_string_lossy().into_owned()]
    );
    assert_eq!(
        collected.repository_paths,
        vec![db::ProjectDeletionRepositoryPath {
            id: repository_id.clone(),
            local_path: Some(repository_path.to_string_lossy().into_owned()),
        }]
    );
    // Deterministic loser side of the admission race: once the guarded
    // Project delete commits, a creator that only held a stale Task/Repo
    // snapshot cannot insert a Workspace (and therefore cannot launch a
    // provider against the captured worktree). The service admission path
    // removes its just-created worktree/cache after this FK failure.
    let late_admission = WorkspaceRepo::create(
        &*harness.state.db,
        CreateWorkspace {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            repo_id: repository_id.clone(),
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch: format!("forge/{}", task.id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await;
    assert!(
        late_admission.is_err(),
        "a Workspace admitted after Project deletion must fail its foreign keys"
    );
    assert!(
        worktree_path.exists(),
        "the DB boundary returns paths so the API can perform post-commit cleanup"
    );
    std::fs::remove_dir_all(worktree_path).expect("late workspace cleanup fixture");
    std::fs::remove_dir_all(repository_path).expect("late repository cleanup fixture");
    std::fs::remove_dir_all(repository_cache).expect("late repository cache cleanup fixture");
}

#[tokio::test]
async fn force_delete_retries_provider_failure_instead_of_deleting_on_retry() {
    let workspace = common::TestDir::new("project-delete-provider-failure");
    let harness = common::test_app(workspace.path(), "project-delete-provider-failure").await;
    let registration = common::fake_daemon::register_daemon(
        &harness.app,
        "project-delete-provider-failure-machine",
        "project_delete_provider_failure",
    )
    .await;
    let server = common::fake_daemon::TestServer::start(harness.state.clone()).await;
    let mut socket = common::fake_daemon::connect_daemon(
        &server,
        &registration.daemon_id,
        Some(&registration.registration_token),
    )
    .await
    .expect("daemon websocket upgrade succeeds");
    common::fake_daemon::wait_until_connected(&harness.state, &registration.daemon_id).await;
    let execution = common::fake_daemon::seed_running_execution_for_daemon(
        &harness.state,
        &registration.daemon_id,
    )
    .await;
    let project_id: String = sqlx::query_scalar(
        "SELECT project_id FROM task WHERE id = (SELECT task_id FROM execution WHERE id = ?)",
    )
    .bind(&execution.id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("execution project lookup");
    let force_uri = format!("/api/v1/projects/{project_id}?force=true");

    let blocked = common::raw_empty_request(
        &harness.app,
        Method::DELETE,
        &format!("/api/v1/projects/{project_id}"),
    )
    .await;
    let blocked_body: Value = common::parse_response(blocked, StatusCode::CONFLICT).await;
    assert_eq!(blocked_body["code"], "project_in_use");
    assert_eq!(blocked_body["details"]["running_executions"], 1);

    // Return an invalid success payload for the cancellation command. The
    // remote provider therefore fails before the execution terminal CAS, so
    // the first request must leave the execution running.
    let first_app = harness.app.clone();
    let first_uri = force_uri.clone();
    let first_delete = tokio::spawn(async move {
        common::raw_empty_request(&first_app, Method::DELETE, &first_uri).await
    });
    let (cancel_id, cancel_params) =
        common::fake_daemon::next_daemon_request(&mut socket, METHOD_EXECUTION_CANCEL).await;
    assert_eq!(cancel_params["execution_id"], execution.id);
    common::fake_daemon::send_daemon_response(&mut socket, cancel_id, json!({})).await;
    let first_response = first_delete.await.expect("first delete request joins");
    assert_eq!(first_response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM execution WHERE id = ?")
            .bind(&execution.id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("running execution remains after provider failure"),
        "running"
    );

    // A retry must make the same stop request again. The old
    // terminalize-before-provider ordering made this request skip the
    // provider entirely and delete the Project after the first failure.
    let second_app = harness.app.clone();
    let second_uri = force_uri;
    let second_delete = tokio::spawn(async move {
        common::raw_empty_request(&second_app, Method::DELETE, &second_uri).await
    });
    let (cancel_id, cancel_params) =
        common::fake_daemon::next_daemon_request(&mut socket, METHOD_EXECUTION_CANCEL).await;
    assert_eq!(cancel_params["execution_id"], execution.id);
    common::fake_daemon::send_daemon_response(&mut socket, cancel_id, json!({})).await;
    let second_response = second_delete.await.expect("second delete request joins");
    assert_eq!(second_response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project WHERE id = ?")
            .bind(&project_id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("project remains after repeated provider failure"),
        1
    );
}
