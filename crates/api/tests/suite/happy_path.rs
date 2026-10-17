#![allow(dead_code, clippy::assertions_on_constants)]
use crate::common;

use std::{path::Path, sync::Arc, time::Duration};

use api::{build_router, AppState};
use api_types::{
    AgentResponse, CreateTerminalSessionResponse, ExecutionStatus, ExecutionSummaryResponse,
    PaginatedResponse, ProjectResponse, RepoResponse, TaskResponse, TaskStatus,
    TerminalAttachTokenResponse, TerminalServerFrame, TerminalSessionResponse,
    TerminalSessionStatus,
};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use db::{ProjectHookRunRepo, ReviewRepo, TaskRepo};
use events::{EventBus, EventContext, ForgeEvent, PROJECT_HOOK_RUN_CHANGED_EVENT};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tower::ServiceExt;

#[tokio::test]
async fn open_item_4_durable_task_command_error_keeps_its_rest_status_and_fields() {
    let directory = common::TestDir::new("durable-command-error");
    let harness = common::test_app(directory.path(), "durable-command-error").await;
    let project: ProjectResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({"name":"Durable errors"}),
        StatusCode::OK,
    )
    .await;
    let task: TaskResponse = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/tasks", project.id),
        json!({"title":"error transport"}),
        StatusCode::OK,
    )
    .await;
    let errors: Vec<(StatusCode, Box<dyn Fn() -> services::ServiceError>)> = vec![
        (
            StatusCode::CONFLICT,
            Box::new(|| db::DbError::VersionConflict.into()),
        ),
        (
            StatusCode::CONFLICT,
            Box::new(|| {
                db::DbError::TaskVersionConflict {
                    expected: 2,
                    actual: 3,
                }
                .into()
            }),
        ),
        (
            StatusCode::NOT_FOUND,
            Box::new(|| services::ServiceError::not_found("execution", "missing")),
        ),
        (
            StatusCode::BAD_REQUEST,
            Box::new(|| services::ServiceError::invalid_operation("deterministic refusal")),
        ),
        (
            StatusCode::FORBIDDEN,
            Box::new(|| services::ServiceError::AuthorizationDenied {
                message: "policy refusal".into(),
            }),
        ),
        (
            StatusCode::PRECONDITION_FAILED,
            Box::new(|| services::ServiceError::GuardRejection {
                guard: "dependency_gate".into(),
                reason: "unfinished dependency".into(),
            }),
        ),
        (
            StatusCode::CONFLICT,
            Box::new(|| services::ServiceError::TaskBusy {
                pending_steps: 3,
                retry_after_ms: 250,
            }),
        ),
    ];
    for (status, make_error) in errors {
        let mut replies = Vec::new();
        for durable in [false, true] {
            services::worker_runtime::queue::test_hooks::inject_command_error(
                &harness.state.task_service,
                &task.id,
                make_error(),
                durable,
            );
            let response = common::raw_json_request(
                &harness.app,
                Method::POST,
                &format!("/api/v1/tasks/{}/actions", task.id),
                json!({"version":task.version,"action":{"verb":"hold"}}),
            )
            .await;
            assert_eq!(response.status(), status, "durable={durable}");
            let mut body: Value = common::parse_response(response, status).await;
            body.as_object_mut().unwrap().remove("request_id");
            replies.push(body);
        }
        assert_eq!(
            replies[0], replies[1],
            "durable replies preserve status, code, message and public fields"
        );
    }
}

#[tokio::test]
async fn forge_happy_path_end_to_end() {
    let repo_dir = common::TestDir::new("forge-happy-repo");
    let repo_path = setup_git_repo(repo_dir.path()).await;
    let default_branch = run_git(&repo_path, &["symbolic-ref", "--short", "HEAD"]);

    let workspaces_root = common::TestDir::new("forge-happy-workspaces");
    let harness = test_app(workspaces_root.path()).await;
    let mut events_rx = harness.event_bus.subscribe();

    let project: ProjectResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Happy Path" }),
        StatusCode::OK,
    )
    .await;
    let project_id = project.id;

    let repo: RepoResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/repos"),
        json!({
            "name": "repo",
            "local_path": repo_path.to_string_lossy(),
            "remote_url": repo_path.to_string_lossy(),
            "default_branch": default_branch
        }),
        StatusCode::OK,
    )
    .await;
    let repo_id = repo.id;
    assert!(repo.local_path.is_some());
    let expected_local_path = repo_path.canonicalize().expect("canonical repo path");
    let returned_local_path = repo
        .local_path
        .as_deref()
        .map(std::path::PathBuf::from)
        .and_then(|path| path.canonicalize().ok());
    assert_eq!(
        returned_local_path.as_deref(),
        Some(expected_local_path.as_path())
    );
    assert_eq!(
        repo.remote_url.as_deref(),
        Some(repo_path.to_string_lossy().as_ref())
    );

    let daemon_id = register_daemon_and_report_shell(&harness.state, workspaces_root.path()).await;
    let agent: AgentResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/agents",
        json!({
            "name": "shell-agent",
            "executor_type": "shell",
            "daemon_id": daemon_id,
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(agent.effective_status.as_deref(), Some("active"));
    let agent_id = agent.id;
    common::configure_execution_test_setup(
        &harness.state.db,
        &project_id,
        &repo_id,
        &agent_id,
        &agent_id,
    )
    .await;

    let created_task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Happy path task",
            "description": "echo hello > greeting.txt && git add . && git commit -m 'hi'",
            "review_config": { "ci_steps": ["test -f greeting.txt"], "review_prompt": r#"python3 - <<'PY'
import json, os
c = json.loads(os.environ['FORGE_REVIEW_CONTRACT'])
print(f"All {len(c['context']['requirements'])} requirement(s) met; check ci:0 verifies the greeting.")
print(json.dumps({'result': 'pass', 'reason': 'The configured check verifies the greeting deliverable'}))
PY"# }
        }),
        StatusCode::OK,
    )
    .await;
    let task_id = created_task.id;
    assert_eq!(created_task.status, "todo".to_owned());
    // Creation applies the Project's default role assignments in the Task's
    // birth transaction, so they spend no extra version. What matters is that
    // the response is not a pre-assignment snapshot: a client's next optimistic
    // write must succeed against the version it was handed.
    let persisted_task: TaskResponse = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{task_id}"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(created_task.version, persisted_task.version);

    let claimed: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/claim"),
        json!({ "agent_id": agent_id, "overrides": null }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(claimed.status, "in_progress".to_owned());
    // The Task is born at version 1 with its default roles, and the claim
    // (coder assignment plus transition) commits as one versioned write. The
    // exact count is asserted so a change to what a claim commits shows up
    // here rather than silently.
    assert_eq!(created_task.version, 1);
    assert_eq!(claimed.version, 2);

    let execution = single_execution_for_task(&harness.app, &task_id).await;
    let execution_id = execution.id.clone();

    let worktree_path = workspaces_root.path().join(&task_id).join("repo");
    let greeting_path = worktree_path.join("greeting.txt");
    poll_until_workspace_written(&harness.app, &task_id, &greeting_path).await;
    poll_until_execution_completed(&harness.state.db, &execution_id).await;

    let completed = drain_until_task_status(&harness, &task_id, "done".to_owned()).await;
    assert_eq!(
        completed.status,
        "done".to_owned(),
        "review-pass auto-cascades through merging to done"
    );

    let latest_subject = run_git(&repo_path, &["log", "-1", "--format=%s"]);
    assert!(
        latest_subject.contains("hi") || latest_subject.contains(&task_id),
        "latest git subject references the task change: {latest_subject}"
    );
    let workspace = db::WorkspaceRepo::get_by_task_id(&*harness.state.db, &task_id)
        .await
        .expect("workspace loads")
        .expect("workspace exists");
    assert_eq!(workspace.status, db::WorkspaceStatus::Ready);
    assert!(workspace.cleanup_after.is_some(), "cleanup is scheduled");
    assert!(worktree_path.exists(), "transition does not delete inline");

    let registry = harness.state.operator_status_service.periodic_workers();
    let cleanup_worker = if registry
        .status()
        .await
        .unwrap()
        .iter()
        .any(|row| row.worker_name == "workspace-cleanup")
    {
        None
    } else {
        let (shutdown, receiver) = tokio::sync::watch::channel(false);
        Some((
            shutdown,
            Arc::clone(&harness.state.cleanup_scheduler).spawn(&registry, receiver),
        ))
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while workspaces_root.path().join(&task_id).exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("cleanup worker removes the workspace");
    if let Some((shutdown, handle)) = cleanup_worker {
        shutdown.send(true).expect("cleanup worker stops");
        handle.await.expect("cleanup worker joins");
    }
    assert!(
        !workspaces_root.path().join(&task_id).exists(),
        "workspace task directory is cleaned"
    );

    let reviews = ReviewRepo::list_by_task(&*harness.state.db, &task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, db::ReviewStatus::Passed);
    assert_eq!(reviews[0].attempt_number, 1);
    let step_results: serde_json::Value =
        serde_json::from_str(&reviews[0].step_results_json).expect("step results parse");
    let ci_steps = step_results
        .get("ci_steps")
        .cloned()
        .unwrap_or(step_results.clone());
    assert_eq!(ci_steps.as_array().expect("step results array").len(), 1);
    assert_eq!(ci_steps[0]["exit_code"], 0);

    let listed_tasks: PaginatedResponse<api_types::TaskListItemResponse> = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}/tasks"),
        StatusCode::OK,
    )
    .await;
    assert!(
        listed_tasks.items.iter().any(|task| task.id == task_id),
        "normal non-automation task remains visible in project task list"
    );
    // 3.2 stage D2b: the merge went through the integration queue. The
    // entry of `merging` admitted exactly one attempt, bound to where the
    // candidate lives, and the queue's `result` step completed it.
    #[allow(clippy::type_complexity)]
    let (state, current, key, workspace_bound, placement_bound, location_bound, owner_kind, candidate, integrated): (
        String,
        bool,
        String,
        bool,
        bool,
        bool,
        Option<String>,
        Option<String>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT state, current, admission_key, workspace_id IS NOT NULL, placement_id IS NOT NULL, \
                repo_location_id IS NOT NULL, owner_kind, original_candidate_sha, integrated_sha \
         FROM integration_attempt WHERE task_ref = ?",
    )
    .bind(&task_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("the merge admitted exactly one integration attempt");
    assert_eq!((state.as_str(), current), ("completed", false));
    assert!(
        key.starts_with("admit:"),
        "admitted by the entry hook: {key}"
    );
    assert!(
        workspace_bound && placement_bound && location_bound,
        "admission records where the candidate lives"
    );
    assert_eq!(owner_kind.as_deref(), Some("server"));
    let merged = run_git(&repo_path, &["rev-parse", "HEAD"]);
    assert_eq!(
        integrated.as_deref(),
        Some(merged.as_str()),
        "the queue fast-forwarded the default checkout to the attempt's commit"
    );
    assert_eq!(
        candidate, integrated,
        "an unchanged target merges the reviewed commit as it is"
    );
    let merge_transitions: Vec<(String, String)> = sqlx::query_as(
        "SELECT from_state, to_state FROM transition_log WHERE task_id = ? AND from_state = 'merging'",
    )
    .bind(&task_id)
    .fetch_all(harness.state.db.pool())
    .await
    .expect("transitions load");
    assert_eq!(
        merge_transitions,
        vec![("merging".to_owned(), "done".to_owned())],
        "the Task stays in merging while the queue works and leaves it once"
    );

    let persisted_task = TaskRepo::get_by_id(&*harness.state.db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(
        !persisted_task.is_automation,
        "happy-path user-created task defaults to non-automation"
    );

    let hook_runs =
        ProjectHookRunRepo::list_recent_for_project(&*harness.state.db, &project_id, 10)
            .await
            .expect("project hook runs load");
    assert!(
        hook_runs.is_empty(),
        "projects without configured hooks must not create project hook runs"
    );

    exercise_terminal_session(&harness, workspaces_root.path(), &project_id, &repo_id).await;

    let events = drain_events(&mut events_rx).await;
    assert_event_type(&events, "task.created");
    assert_event_type(&events, "task.assigned");
    assert_status_event(&events, &task_id, "in_progress");
    assert_event_type(&events, "task.auto_transitioned");
    assert_event_type(&events, "review.passed");
    assert_status_event(&events, &task_id, "review");
    assert_status_event(&events, &task_id, "merging");
    assert_status_event(&events, &task_id, "done");
    assert_event_type(&events, "workspace.cleaned");
    assert!(
        events
            .iter()
            .all(|event| event.event_type != PROJECT_HOOK_RUN_CHANGED_EVENT),
        "no project_hook.run_changed events are emitted when no hooks are configured"
    );
    assert_task_conditions_match_legacy(&harness.state.db).await;
    common::assert_scheduler_clean(&harness.state).await;
}

/// Every producer in the end-to-end flow agrees with its legacy fields and
/// the durable entry, execution, ledger, operation and child witnesses.
async fn assert_task_conditions_match_legacy(db: &db::SqliteDb) {
    assert_eq!(
        db.task_condition_violations()
            .await
            .expect("condition sweep runs"),
        Vec::<String>::new(),
        "a Task writer skipped the condition dual-write seam"
    );
}

#[tokio::test]
async fn autonomous_workflow_requires_human_review_and_resumes_worker_on_reject() {
    let repo_dir = common::TestDir::new("forge-autonomous-repo");
    let repo_path = setup_git_repo(repo_dir.path()).await;
    let default_branch = run_git(&repo_path, &["symbolic-ref", "--short", "HEAD"]);
    let workspaces_root = common::TestDir::new("forge-autonomous-workspaces");
    let harness = test_app(workspaces_root.path()).await;
    harness
        .state
        .workflow_template_service
        .initialize()
        .await
        .expect("builtin workflow templates initialize");

    let project: ProjectResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Autonomous End to End" }),
        StatusCode::OK,
    )
    .await;
    let project_id = project.id;
    let repo: RepoResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/repos"),
        json!({
            "name": "repo",
            "local_path": repo_path.to_string_lossy(),
            "remote_url": repo_path.to_string_lossy(),
            "default_branch": default_branch
        }),
        StatusCode::OK,
    )
    .await;
    let _: Value = json_request(
        &harness.app,
        Method::PUT,
        &format!("/api/v1/projects/{project_id}/workflow"),
        json!({ "template_name": "autonomous_v1" }),
        StatusCode::OK,
    )
    .await;

    let daemon_id = register_daemon_and_report_shell(&harness.state, workspaces_root.path()).await;
    let agent: AgentResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/agents",
        json!({
            "name": "autonomous-shell-agent",
            "executor_type": "shell",
            "daemon_id": daemon_id,
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(agent.effective_status.as_deref(), Some("active"));
    common::configure_execution_test_setup(
        &harness.state.db,
        &project_id,
        &repo.id,
        &agent.id,
        &agent.id,
    )
    .await;

    let task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({
            "title": "Autonomous delivery",
            "description": "printf 'autonomous\\n' > autonomous.txt && git add autonomous.txt && git commit -m autonomous",
            "review_config": { "ci_steps": [] }
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(task.status, "ready".to_owned());

    let claimed: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/claim", task.id),
        json!({ "agent_id": agent.id, "overrides": null }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(claimed.status, "working".to_owned());
    let first_execution = single_execution_for_task(&harness.app, &task.id).await;
    assert_eq!(first_execution.role.to_string(), "worker");

    let review_task = poll_until_task_awaiting_human(&harness.app, &task.id).await;

    let approved: TaskResponse = versioned_task_request(
        &harness.app,
        &format!("/api/v1/tasks/{}/actions", task.id),
        &task.id,
        json!({ "version": review_task.version, "action": { "verb":"approve", "override":false } }),
    )
    .await;
    assert!(matches!(approved.status.as_str(), "merging" | "done"));
    let completed = drain_until_task_status(&harness, &task.id, "done".to_owned()).await;
    assert_eq!(completed.status, "done".to_owned());

    let ci_failure_task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({
            "title": "Autonomous validation retry",
            "description": "printf 'validation retry\\n' > autonomous-ci-retry.txt && git add autonomous-ci-retry.txt && git commit -m autonomous-ci-retry",
            "review_config": { "ci_steps": ["false"] }
        }),
        StatusCode::OK,
    )
    .await;
    let claimed_ci_failure: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/claim", ci_failure_task.id),
        json!({ "agent_id": agent.id, "overrides": null }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(claimed_ci_failure.status, "working".to_owned());
    let first_ci_execution = single_execution_for_task(&harness.app, &ci_failure_task.id).await;

    let failed_follow_up =
        drain_until_follow_up_execution(&harness, &ci_failure_task.id, &first_ci_execution.id)
            .await;
    assert_eq!(
        failed_follow_up.parent_execution_id.as_deref(),
        Some(first_ci_execution.id.as_str())
    );
    poll_until_execution_stopped(&harness.state.db, &failed_follow_up.id).await;
    let after_ci_failure: TaskResponse = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", ci_failure_task.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        after_ci_failure.status,
        "working".to_owned(),
        "Task after CI failure: {after_ci_failure:?}"
    );
    assert!(!after_ci_failure.awaiting_human);

    let passing_review_config = serde_json::to_string(&json!({
        "review": { "ci_steps": ["true"] }
    }))
    .expect("passing review config serializes");
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(passing_review_config)
        .bind(&ci_failure_task.id)
        .execute(harness.state.db.pool())
        .await
        .expect("passing review config updates");
    let _: Value = versioned_task_request(
        &harness.app,
        &format!("/api/v1/tasks/{}/transition", ci_failure_task.id),
        &ci_failure_task.id,
        json!({
            "status": "review",
            "version": after_ci_failure.version,
            "reason": "retry validation after CI configuration was corrected"
        }),
    )
    .await;

    let ci_review = poll_until_task_awaiting_human(&harness.app, &ci_failure_task.id).await;
    let ci_approved: TaskResponse = versioned_task_request(
        &harness.app,
        &format!("/api/v1/tasks/{}/actions", ci_failure_task.id),
        &ci_failure_task.id,
        json!({ "version": ci_review.version, "action": { "verb": "approve", "override": false } }),
    )
    .await;
    assert!(matches!(ci_approved.status.as_str(), "merging" | "done"));
    let ci_completed =
        drain_until_task_status(&harness, &ci_failure_task.id, "done".to_owned()).await;
    assert_eq!(ci_completed.status, "done".to_owned());
    let ci_reviews = ReviewRepo::list_by_task(&*harness.state.db, &ci_failure_task.id)
        .await
        .expect("CI retry reviews load");
    assert!(
        ci_reviews
            .iter()
            .any(|review| review.status == db::ReviewStatus::Failed),
        "the first validation attempt should be recorded as failed"
    );

    let second_task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({
            "title": "Autonomous requested changes",
            "description": "printf 'requested changes\\n' > autonomous-rejected.txt && git add autonomous-rejected.txt && git commit -m requested-changes",
            "review_config": { "ci_steps": [] }
        }),
        StatusCode::OK,
    )
    .await;
    let claimed_second: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/claim", second_task.id),
        json!({ "agent_id": agent.id, "overrides": null }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(claimed_second.status, "working".to_owned());
    let second_execution = single_execution_for_task(&harness.app, &second_task.id).await;
    let second_review = poll_until_task_awaiting_human(&harness.app, &second_task.id).await;

    // Production consumes condition commands through this dispatcher. A long
    // interval makes this also prove that the post-commit wake starts the worker.
    let dispatcher = Arc::new(services::TaskDispatcher::with_check_interval(
        Arc::clone(&harness.state.db),
        Arc::clone(&harness.event_bus),
        Arc::clone(&harness.state.task_service),
        std::time::Duration::from_secs(3600),
    ));
    let dispatch_loop = Arc::clone(&dispatcher).start();
    let rejected: TaskResponse = versioned_task_request(
        &harness.app,
        &format!("/api/v1/tasks/{}/actions", second_task.id),
        &second_task.id,
        json!({
            "version": second_review.version,
            "action": { "verb": "send_back", "guidance": "Please add evidence for the requested behavior" }
        }),
    )
    .await;
    assert_eq!(rejected.status, "working".to_owned());

    let resumed =
        drain_until_follow_up_execution(&harness, &second_task.id, &second_execution.id).await;
    assert_eq!(resumed.role.to_string(), "worker");
    assert_eq!(
        resumed.parent_execution_id.as_deref(),
        Some(second_execution.id.as_str())
    );
    dispatcher.stop();
    dispatch_loop.await.expect("dispatcher exits");
    assert_task_conditions_match_legacy(&harness.state.db).await;
    common::assert_scheduler_clean(&harness.state).await;
}

struct TestHarness {
    _step_worker: common::StepWorkerGuard,
    app: Router,
    state: Arc<AppState>,
    event_bus: Arc<EventBus>,
    _web_dist_dir: common::TestDir,
}

async fn test_app(workspace_root: &Path) -> TestHarness {
    test_app_with_registry(workspace_root, cli_adapters::test_support::test_registry()).await
}

async fn test_app_with_registry(
    workspace_root: &Path,
    registry: executors::AdapterRegistry,
) -> TestHarness {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");

    let db = Arc::new(db::SqliteDb::new(pool));
    let now = db::now_rfc3339();
    db::UserRepo::create_user(
        &*db,
        &db::User {
            id: "test-user-id".to_owned(),
            email: "test@example.com".to_owned(),
            password_hash: "$2b$04$placeholder".to_owned(),
            display_name: None,
            is_admin: true,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("seed test user");
    let adapter_registry = Arc::new(registry);
    services::ensure_default_agents(db.as_ref(), &adapter_registry)
        .await
        .expect("default agents upsert");
    let event_bus = Arc::new(EventBus::new(256));
    let merge_service = Arc::new(services::MergeService::new_for_test(
        Arc::clone(&db),
        workspace_root.to_path_buf(),
    ));
    let cleanup_scheduler = Arc::new(services::WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        workspace_root.to_path_buf(),
    ));
    let mut state = AppState::with_adapter_registry_services_and_shutdown(
        db,
        Arc::clone(&event_bus),
        true,
        adapter_registry,
        merge_service,
        cleanup_scheduler,
        api::state::ShutdownSignal::new(),
        api::state::test_workflows_dir(),
        api::state::test_jwt_secret(),
        api::state::test_bcrypt_cost(),
    );
    let mut config = (*state.effective_config).clone();
    config.terminal.enabled = true;
    state = state.with_effective_config(config);
    let state = Arc::new(state);

    let web_dist_dir = common::TestDir::new("forge-happy-web");
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").expect("write index");
    let app = build_router((*state).clone(), web_dist_dir.path().to_path_buf());

    TestHarness {
        _step_worker: common::StepWorkerGuard::start(&state),
        app,
        state,
        event_bus,
        _web_dist_dir: web_dist_dir,
    }
}

async fn exercise_terminal_session(
    harness: &TestHarness,
    workspace_root: &Path,
    project_id: &str,
    repo_id: &str,
) {
    let task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "Terminal happy path", "description": "terminal smoke" }),
        StatusCode::OK,
    )
    .await;
    seed_ready_workspace(&harness.state.db, workspace_root, &task.id, repo_id).await;

    let created: CreateTerminalSessionResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/terminals", task.id),
        json!({ "rows": 24, "cols": 80 }),
        StatusCode::CREATED,
    )
    .await;
    let refreshed: TerminalAttachTokenResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/terminals/{}/attach-token", created.session.id),
        json!({}),
        StatusCode::OK,
    )
    .await;
    assert_ne!(created.attach.attach_token, refreshed.attach_token);

    let mut live_rx = harness
        .state
        .terminal_service
        .attach_client(&created.session.id)
        .await;
    harness
        .state
        .terminal_service
        .handle_terminal_input(&created.session.id, "ZWNobyBmb3JnZS10ZXJtaW5hbC1vawo=")
        .await
        .expect("terminal input accepted");
    wait_for_terminal_output(&mut live_rx, "forge-terminal-ok").await;
    drop(live_rx);

    let mut replay_rx = harness
        .state
        .terminal_service
        .attach_client(&created.session.id)
        .await;
    wait_for_terminal_output(&mut replay_rx, "forge-terminal-ok").await;

    let terminated: TerminalSessionResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/terminals/{}/terminate", created.session.id),
        json!({ "reason": "happy_path" }),
        StatusCode::OK,
    )
    .await;
    assert!(matches!(
        terminated.status,
        TerminalSessionStatus::Terminated
    ));
}

async fn seed_ready_workspace(
    db: &Arc<db::SqliteDb>,
    workspace_root: &Path,
    task_id: &str,
    repo_id: &str,
) {
    let now = db::now_rfc3339();
    let worktree_path = workspace_root.join(task_id).join("terminal-repo");
    std::fs::create_dir_all(&worktree_path).expect("terminal worktree creates");
    db::WorkspaceRepo::create(
        &**db,
        db::CreateWorkspace {
            id: db::new_uuid_v4(),
            task_id: task_id.to_owned(),
            repo_id: repo_id.to_owned(),
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch: workspace::task_branch_name(task_id),
            status: db::WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("terminal workspace creates");
}

async fn wait_for_terminal_output(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<TerminalServerFrame>,
    needle: &str,
) {
    let mut collected = String::new();
    for _ in 0..20 {
        if let Ok(Some(TerminalServerFrame::Output { data })) =
            tokio::time::timeout(Duration::from_millis(250), rx.recv()).await
        {
            collected.push_str(&String::from_utf8_lossy(&decode_base64_standard(&data)));
            if collected.contains(needle) {
                return;
            }
        }
    }
    panic!("terminal output did not contain {needle}; collected {collected:?}");
}

fn decode_base64_standard(input: &str) -> Vec<u8> {
    let mut out = Vec::new();
    let mut buffer = 0_u32;
    let mut bits = 0_u8;
    for byte in input.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => break,
            _ => continue,
        };
        buffer = (buffer << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(((buffer >> bits) & 0xff) as u8);
        }
    }
    out
}

async fn setup_git_repo(path: &Path) -> std::path::PathBuf {
    let repo_path = path.join("repo");
    std::fs::create_dir_all(&repo_path).expect("repo dir creates");
    run_git(&repo_path, &["init", "--initial-branch=main"]);
    run_git(&repo_path, &["config", "user.email", "test@forge.dev"]);
    run_git(&repo_path, &["config", "user.name", "Forge Test"]);
    std::fs::write(repo_path.join("README.md"), "# Happy Path\n").expect("README writes");
    run_git(&repo_path, &["add", "-A"]);
    run_git(&repo_path, &["commit", "-m", "initial commit"]);
    repo_path
}

async fn register_daemon_and_report_shell(state: &AppState, workspace_root: &Path) -> String {
    common::register_embedded_daemon(state, workspace_root, "happy-path-host", "shell")
        .await
        .daemon_id
}

async fn poll_until_execution_completed(db: &Arc<db::SqliteDb>, execution_id: &str) {
    for _ in 0..100 {
        if let Some(execution) = db::ExecutionRepo::get_by_id(&**db, execution_id)
            .await
            .expect("execution lookup")
        {
            if execution.status == db::ExecutionStatus::Completed {
                return;
            }
            if execution.status != db::ExecutionStatus::Running {
                panic!(
                    "execution ended in unexpected status: {:?}; error: {:?}",
                    execution.status, execution.error
                );
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("execution did not complete within timeout");
}

async fn poll_until_execution_stopped(db: &Arc<db::SqliteDb>, execution_id: &str) {
    for _ in 0..100 {
        if let Some(execution) = db::ExecutionRepo::get_by_id(&**db, execution_id)
            .await
            .expect("execution lookup")
        {
            if execution.status != db::ExecutionStatus::Running {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("execution did not stop within timeout");
}

async fn poll_until_workspace_written(app: &Router, task_id: &str, greeting_path: &Path) {
    for _ in 0..100 {
        if greeting_path.exists() {
            return;
        }
        let execution = single_execution_for_task(app, task_id).await;
        if execution.status != ExecutionStatus::Running {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        greeting_path.exists(),
        "greeting.txt was not written before execution stopped"
    );
}

async fn drain_until_task_status(
    harness: &TestHarness,
    task_id: &str,
    expected_status: TaskStatus,
) -> TaskResponse {
    // Executions finish independently; every observation settles the durable
    // cascade queue instead of polling for inline cascade side effects.
    for _ in 0..100 {
        let task = common::drain(&harness.state, &harness.app, task_id).await;
        if task.status == expected_status {
            return task;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let last = common::drain(&harness.state, &harness.app, task_id).await;
    panic!("task did not reach {expected_status:?}; last Task: {last:?}");
}

async fn poll_until_task_awaiting_human(app: &Router, task_id: &str) -> TaskResponse {
    for _ in 0..100 {
        let task: TaskResponse = empty_request(
            app,
            Method::GET,
            &format!("/api/v1/tasks/{task_id}"),
            StatusCode::OK,
        )
        .await;
        if task.status == "review" && task.awaiting_human {
            return task;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("task did not reach human review within timeout");
}

async fn single_execution_for_task(app: &Router, task_id: &str) -> ExecutionSummaryResponse {
    let executions: PaginatedResponse<ExecutionSummaryResponse> = empty_request(
        app,
        Method::GET,
        &format!("/api/v1/tasks/{task_id}/executions"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(executions.items.len(), 1);
    executions.items.into_iter().next().unwrap()
}

async fn drain_until_follow_up_execution(
    harness: &TestHarness,
    task_id: &str,
    parent_execution_id: &str,
) -> ExecutionSummaryResponse {
    for _ in 0..100 {
        common::drain(&harness.state, &harness.app, task_id).await;
        let executions: PaginatedResponse<ExecutionSummaryResponse> = empty_request(
            &harness.app,
            Method::GET,
            &format!("/api/v1/tasks/{task_id}/executions"),
            StatusCode::OK,
        )
        .await;
        if let Some(execution) = executions
            .items
            .into_iter()
            .find(|execution| execution.parent_execution_id.as_deref() == Some(parent_execution_id))
        {
            return execution;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("worker follow-up execution was not recorded");
}

async fn drain_events(rx: &mut tokio::sync::broadcast::Receiver<ForgeEvent>) -> Vec<ForgeEvent> {
    let mut events = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Ok(event)) => events.push(event),
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => continue,
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) | Err(_) => break,
        }
    }
    events
}

fn assert_event_type(events: &[ForgeEvent], event_type: &str) {
    assert!(
        events.iter().any(|event| event.event_type == event_type),
        "missing event {event_type}; got {:?}",
        events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>()
    );
}

fn assert_status_event(events: &[ForgeEvent], task_id: &str, expected_status: &str) {
    assert!(
        events.iter().any(|event| {
            event.event_type == "task.status_changed"
                && event.entity_id == task_id
                && matches!(
                    &event.context,
                    EventContext::TaskStatusChanged { new_status, .. }
                        if new_status == expected_status
                )
        }),
        "missing task.status_changed to {expected_status}"
    );
}

async fn json_request<T>(
    app: &Router,
    method: Method,
    uri: &str,
    body: Value,
    expected_status: StatusCode,
) -> T
where
    T: DeserializeOwned,
{
    let response = raw_json_request(app, method, uri, body).await;
    parse_response(response, expected_status).await
}

async fn json_request_with_bearer<T>(
    app: &Router,
    method: Method,
    uri: &str,
    token: &str,
    body: Value,
    expected_status: StatusCode,
) -> T
where
    T: DeserializeOwned,
{
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .expect("build authorized JSON request"),
        )
        .await
        .expect("router response");
    parse_response(response, expected_status).await
}

/// Posts a versioned Task mutation, refetching the version when a background
/// writer wins the race. Task writes are optimistically concurrent, so a 409
/// here means the snapshot this call was built from went stale — exactly what a
/// real client refetches and resubmits — rather than a product failure.
async fn versioned_task_request<T>(app: &Router, uri: &str, task_id: &str, mut body: Value) -> T
where
    T: DeserializeOwned,
{
    let mut last_snapshot = None;
    for _ in 0..40 {
        let response = raw_json_request(app, Method::POST, uri, body.clone()).await;
        if response.status() != StatusCode::CONFLICT {
            return parse_response(response, StatusCode::OK).await;
        }
        let current: TaskResponse = empty_request(
            app,
            Method::GET,
            &format!("/api/v1/tasks/{task_id}"),
            StatusCode::OK,
        )
        .await;
        body["version"] = json!(current.version);
        last_snapshot = Some(current);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "versioned request to {uri} never won the version race; last snapshot: {last_snapshot:?}"
    );
}

async fn empty_request<T>(app: &Router, method: Method, uri: &str, expected_status: StatusCode) -> T
where
    T: DeserializeOwned,
{
    let response = raw_empty_request(app, method, uri).await;
    parse_response(response, expected_status).await
}

fn test_jwt() -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let claims = serde_json::json!({
        "sub": "test-user-id",
        "email": "test@example.com",
        "is_admin": true,
        "iat": now,
        "exp": now + 900,
    });
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(b"test-jwt-secret-for-development"),
    )
    .expect("encode test jwt")
}

async fn raw_json_request(
    app: &Router,
    method: Method,
    uri: &str,
    body: Value,
) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {}", test_jwt()))
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .expect("build JSON request"),
        )
        .await
        .expect("router response")
}

async fn raw_empty_request(app: &Router, method: Method, uri: &str) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {}", test_jwt()))
                .body(Body::empty())
                .expect("build empty request"),
        )
        .await
        .expect("router response")
}

async fn parse_response<T>(response: axum::response::Response, expected_status: StatusCode) -> T
where
    T: DeserializeOwned,
{
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    assert_eq!(
        status,
        expected_status,
        "unexpected response status with body: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("parse JSON response")
}

fn run_git(path: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .expect("git command runs");
    assert!(
        output.status.success(),
        "git {} failed\nstdout: {}\nstderr: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

#[tokio::test]
async fn single_machine_environment_recheck_resumes_task_dispatch() {
    let root = tempfile::tempdir().unwrap();
    let repo = common::setup_git_repo(root.path());
    let harness = test_app(root.path()).await;
    let (project_id, repo_id) =
        common::create_project_and_repo(&harness.app, "Environment smoke", &repo).await;
    let (agent_id, _) = common::create_shell_agents(
        &harness.app,
        &harness.state,
        root.path(),
        "environment-smoke",
    )
    .await;
    let agent: AgentResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/agents/{agent_id}"),
        StatusCode::OK,
    )
    .await;
    common::configure_execution_test_setup(
        &harness.state.db,
        &project_id,
        &repo_id,
        &agent.id,
        &agent.id,
    )
    .await;
    let project = db::ProjectRepo::get_by_id(&*harness.state.db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let mut settings: Value = serde_json::from_str(&project.settings).unwrap();
    settings["environment"] = json!({"checks":[{"name":"disk","command":"test -f recovered"}]});
    db::ProjectRepo::update_at_version(
        &*harness.state.db,
        db::UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: Some(settings.to_string()),
            primary_repo_id: None,
            paused_at: None,
            updated_at: db::now_rfc3339(),
        },
        project.version,
        None,
    )
    .await
    .unwrap();
    let task: TaskResponse = json_request(&harness.app, Method::POST, &format!("/api/v1/projects/{project_id}/tasks"), json!({"title":"Environment smoke","description":"sleep 3","role_assignments":[{"role_name":"coder","assignee_type":"agent","assignee_id":agent.id}]}), StatusCode::OK).await;
    // Direct claim reaches preflight on an unknown host, as in the single-machine base.
    let _: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/claim", task.id),
        json!({"agent_id":agent.id}),
        StatusCode::OK,
    )
    .await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let paused = db::ProjectRepo::get_by_id(&*harness.state.db, &project_id)
            .await
            .unwrap()
            .unwrap();
        if paused.system_pause_reason.as_deref() == Some("environment_not_ready") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "environment did not pause"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let before = db::ExecutionRepo::list_by_task(
        &*harness.state.db,
        &task.id,
        db::PageRequest {
            cursor: None,
            limit: 20,
            include_total: false,
            sort_by: db::SortBy::CreatedAt,
            sort_order: db::SortOrder::Desc,
        },
    )
    .await
    .unwrap();
    assert_eq!(before.items.len(), 1);
    assert_eq!(before.items[0].status, db::ExecutionStatus::Failed);
    // Restore the same host's primary checkout and existing Task workspace.
    std::fs::write(repo.join("recovered"), "ready").unwrap();
    let workspace = db::WorkspaceRepo::get_by_task_id(&*harness.state.db, &task.id)
        .await
        .unwrap()
        .unwrap();
    let placement =
        db::WorkspacePlacementRepo::get_by_workspace_id(&*harness.state.db, &workspace.id)
            .await
            .unwrap()
            .unwrap();
    std::fs::write(
        std::path::Path::new(placement.workspace_handle.as_deref().unwrap()).join("recovered"),
        "ready",
    )
    .unwrap();
    let checked: api_types::ProjectEnvironmentRecheckResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/environment/recheck"),
        json!({"machine":"server"}),
        StatusCode::OK,
    )
    .await;
    assert!(!checked.project.paused);
    assert!(checked.machines[0].checks[0].passed);
    let dispatcher = services::TaskDispatcher::new(
        harness.state.db.clone(),
        harness.event_bus.clone(),
        harness.state.task_service.clone(),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        dispatcher.check_once().await.unwrap();
        let executions = db::ExecutionRepo::list_by_task(
            &*harness.state.db,
            &task.id,
            db::PageRequest {
                cursor: None,
                limit: 20,
                include_total: false,
                sort_by: db::SortBy::CreatedAt,
                sort_order: db::SortOrder::Desc,
            },
        )
        .await
        .unwrap();
        if executions.items.len() > 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Task did not re-dispatch"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_task_conditions_match_legacy(&harness.state.db).await;
    common::assert_scheduler_clean(&harness.state).await;
}

/// Like model_workflow's scripted native lead: the native profile names the
/// test adapter, so planning and step checks run without a provider. The kickoff
/// stays live until the test releases it; the checks return fail, pass, pass.
struct HappyPathLead {
    kickoff: Arc<tokio::sync::Semaphore>,
    checks: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl executors::CodingExecutorAdapter for HappyPathLead {
    fn kind(&self) -> executors::ExecutorKind {
        executors::ExecutorKind::Codex
    }

    fn check_availability(&self) -> executors::AvailabilityInfo {
        executors::AvailabilityInfo {
            status: executors::AvailabilityStatus::Authenticated,
            authenticated_at: None,
            config_path: None,
        }
    }

    async fn discover_options(
        &self,
        _ctx: executors::DiscoverContext,
    ) -> Result<executors::DiscoveredOptions, executors::ExecutorError> {
        Ok(executors::DiscoveredOptions::default())
    }

    async fn execute(
        &self,
        ctx: executors::ExecutionContext,
    ) -> Result<executors::ExecutionResult, executors::ExecutorError> {
        let reply = if ctx.description.contains("Step-check doctrine:") {
            if self
                .checks
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                == 0
            {
                "The first step needs its second line.\n{\"result\":\"fail\",\"reason\":\"append the second line\"}"
            } else {
                "The step is accepted.\n{\"result\":\"pass\",\"reason\":\"deliverable verified\"}"
            }
        } else {
            assert!(ctx
                .description
                .contains("You are the lead of this Forge coordination root"));
            self.kickoff.acquire().await.unwrap().forget();
            "The breakdown is ready; run the two steps in order."
        };
        if let Some(parent) = Path::new(&ctx.logs_path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        executors::LogWriter::new(&ctx.logs_path, ctx.execution_id, 1024 * 1024)
            .write(
                executors::LogKind::Assistant,
                executors::LogStream::Main,
                json!({"text": reply}),
            )
            .await?;
        Ok(executors::ExecutionResult {
            status: executors::ExecutionOutcome::Completed,
            summary: Some(reply.to_owned()),
            agent_session_id: Some("happy-path-lead-session".to_owned()),
            ..Default::default()
        })
    }

    async fn cancel(&self, _execution_id: &str) -> Result<(), executors::ExecutorError> {
        Ok(())
    }
}

async fn drive_lead_happy_path(
    harness: &TestHarness,
    dispatcher: &services::TaskDispatcher,
    task_id: &str,
    status: &str,
) -> TaskResponse {
    for _ in 0..200 {
        dispatcher
            .check_once()
            .await
            .expect("dispatcher reconciles");
        let task = common::drain(&harness.state, &harness.app, task_id).await;
        if task.status == status {
            return task;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let last = common::drain(&harness.state, &harness.app, task_id).await;
    panic!("lead happy path did not reach {status}; last Task: {last:?}");
}

#[tokio::test]
async fn root_lead_kickoff_returned_step_and_aggregate_review_finish() {
    let repo_dir = common::TestDir::new("forge-lead-happy-repo");
    let repo_path = setup_git_repo(repo_dir.path()).await;
    let workspaces = common::TestDir::new("forge-lead-happy-workspaces");
    let kickoff = Arc::new(tokio::sync::Semaphore::new(0));
    let checks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = cli_adapters::test_support::test_registry();
    registry.register(Box::new(HappyPathLead {
        kickoff: Arc::clone(&kickoff),
        checks: Arc::clone(&checks),
    }));
    let harness = test_app_with_registry(workspaces.path(), registry).await;
    let dispatcher = services::TaskDispatcher::new(
        Arc::clone(&harness.state.db),
        Arc::clone(&harness.event_bus),
        Arc::clone(&harness.state.task_service),
    );
    let project: ProjectResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({"name": "Lead happy path"}),
        StatusCode::OK,
    )
    .await;
    let repo: RepoResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/repos", project.id),
        json!({"name": "repo", "local_path": repo_path, "remote_url": repo_path,
            "default_branch": run_git(&repo_path, &["symbolic-ref", "--short", "HEAD"])}),
        StatusCode::OK,
    )
    .await;
    let daemon = register_daemon_and_report_shell(&harness.state, workspaces.path()).await;
    let worker: AgentResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/agents",
        json!({"name": "Default worker", "executor_type": "shell", "daemon_id": daemon}),
        StatusCode::OK,
    )
    .await;
    common::configure_execution_test_setup(
        &harness.state.db,
        &project.id,
        &repo.id,
        &worker.id,
        &worker.id,
    )
    .await;

    // Same native-profile seam used by model_workflow::Action::AssignLead.
    let now = db::now_rfc3339();
    let lead_id = db::new_uuid_v4();
    let lead = db::AgentRepo::create_identity_with_profile(
        &*harness.state.db,
        db::CreateAgentIdentity {
            id: lead_id.clone(),
            name: "Happy path lead".to_owned(),
            description: None,
            max_concurrent_tasks: 2,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: db::AgentStatus::Idle,
            last_heartbeat_at: Some(now.clone()),
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        db::CreateAgentProfile {
            id: db::new_uuid_v4(),
            identity_id: lead_id.clone(),
            backend_kind: "native".to_owned(),
            executor_type: "codex".to_owned(),
            provider: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(daemon),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("scripted native lead creates");
    db::AgentConnectionHealthRepo::upsert_connection_health(
        &*harness.state.db,
        db::UpsertAgentConnectionHealth {
            profile_id: lead.profile_id,
            status: "healthy".to_owned(),
            capability_status_json: "{}".to_owned(),
            checked_at: Some(now.clone()),
            error_code: None,
            updated_at: now,
        },
    )
    .await
    .expect("lead connection is healthy");
    let root: TaskResponse = json_request(
        &harness.app, Method::POST, &format!("/api/v1/projects/{}/tasks", project.id),
        json!({"title": "Root with lead", "description": "Two checked deliverables",
            "role_assignments": [
                {"role_name": "planner", "assignee_type": "agent", "assignee_id": lead_id},
                {"role_name": "coder", "assignee_type": "agent", "assignee_id": worker.id},
                {"role_name": "reviewer", "assignee_type": "agent", "assignee_id": worker.id}
            ],
            "review_config": {"ci_steps": ["test $(wc -l < first.txt) -eq 2 && test -f second.txt"],
                "review_prompt": r#"python3 - <<'PY'
import json, os
contract = json.loads(os.environ['FORGE_REVIEW_CONTRACT'])
print(f"Checked {len(contract['context']['requirements'])} requirements against both deliverables.")
print(json.dumps({'result': 'pass', 'reason': 'Both ordered steps are present and the returned step is fixed'}))
PY"#}}),
        StatusCode::OK,
    ).await;
    let mut children = Vec::new();
    for (title, command) in [
        ("first", "printf 'first\\n' >> first.txt && git add first.txt && git commit -m 'first step'"),
        ("second", "test $(wc -l < first.txt) -eq 2 && printf 'second\\n' > second.txt && git add second.txt && git commit -m 'second step'"),
    ] {
        let child: TaskResponse = json_request(
            &harness.app, Method::POST, &format!("/api/v1/projects/{}/tasks", project.id),
            json!({"title": title, "description": command, "parent_task_id": root.id}), StatusCode::OK,
        ).await;
        assert!(child.role_assignments.iter().all(|row| row.role_name != "coder"), "the subtask inherits its default worker");
        assert_eq!(child.effective_coder.as_ref().and_then(|row| row.assignee_id.as_deref()), Some(worker.id.as_str()));
        children.push(child);
    }
    let planning = drive_lead_happy_path(&harness, &dispatcher, &root.id, "planning").await;
    assert_eq!(
        planning
            .effective_lead
            .unwrap()
            .assignment
            .assignee_id
            .as_deref(),
        Some(lead_id.as_str())
    );
    let child_runs: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution e JOIN task t ON t.id=e.task_id WHERE t.parent_task_id=?",
    )
    .bind(&root.id)
    .fetch_one(harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(child_runs, 0, "children cannot run during kickoff planning");
    kickoff.add_permits(1);
    let done = drive_lead_happy_path(&harness, &dispatcher, &root.id, "done").await;
    assert_eq!(done.status, "done");
    for child in &children {
        let task = common::drain(&harness.state, &harness.app, &child.id).await;
        assert_eq!(task.status, "done");
        assert!(
            ReviewRepo::list_by_task(&*harness.state.db, &child.id)
                .await
                .unwrap()
                .is_empty(),
            "a step check is not aggregate review"
        );
    }
    assert_eq!(
        checks.load(std::sync::atomic::Ordering::SeqCst),
        3,
        "fail, pass, pass"
    );
    let returned: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transition_log WHERE task_id=? AND from_state='lead_check' AND to_state='in_progress' AND rejection=1 AND bridge_kind='gate_rejected'")
        .bind(&children[0].id).fetch_one(harness.state.db.pool()).await.unwrap();
    assert_eq!(returned, 1, "the lead returned the first step exactly once");
    let spent: i64 = sqlx::query_scalar(
        "SELECT spent FROM task_budget WHERE task_id=? AND kind='gate:lead_check'",
    )
    .bind(&children[0].id)
    .fetch_one(harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(spent, 1);
    let runs: Vec<(String, String, String)> = sqlx::query_as("SELECT task_id, role, agent_id FROM execution WHERE task_id=? OR task_id IN (?,?) ORDER BY created_at,rowid")
        .bind(&root.id).bind(&children[0].id).bind(&children[1].id).fetch_all(harness.state.db.pool()).await.unwrap();
    assert_eq!(
        runs.iter()
            .filter(|(id, role, agent)| id == &root.id && role == "planner" && agent == &lead_id)
            .count(),
        1
    );
    assert_eq!(
        runs.iter()
            .filter(|(_, role, agent)| role == "coder" && agent == &worker.id)
            .count(),
        3,
        "the default worker runs the first step twice and the second once"
    );
    assert_eq!(
        runs.iter()
            .filter(|(id, role, agent)| id != &root.id && role == "planner" && agent == &lead_id)
            .count(),
        3
    );
    let reviews = ReviewRepo::list_by_task(&*harness.state.db, &root.id)
        .await
        .unwrap();
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, db::ReviewStatus::Passed);
    assert_eq!(
        std::fs::read_to_string(repo_path.join("first.txt"))
            .unwrap()
            .lines()
            .count(),
        2
    );
    assert!(
        repo_path.join("second.txt").exists(),
        "aggregate merge delivered both steps"
    );
    common::assert_scheduler_clean(&harness.state).await;
}

/// Real shell workers and the durable review/integration owners, with no model
/// provider: each named case supplies its own candidate and reviewer scripts.
async fn repair_release_project(
    harness: &TestHarness,
    repo_path: &Path,
    workspaces: &Path,
) -> (ProjectResponse, AgentResponse) {
    let project: ProjectResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({"name":"Repair release"}),
        StatusCode::OK,
    )
    .await;
    let repo: RepoResponse = json_request(&harness.app, Method::POST, &format!("/api/v1/projects/{}/repos", project.id), json!({"name":"repo","local_path":repo_path.to_string_lossy(),"remote_url":repo_path.to_string_lossy(),"default_branch":"main"}), StatusCode::OK).await;
    let daemon = register_daemon_and_report_shell(&harness.state, workspaces).await;
    let agent: AgentResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/agents",
        json!({"name":"repair-release-shell","executor_type":"shell","daemon_id":daemon}),
        StatusCode::OK,
    )
    .await;
    common::configure_execution_test_setup(
        &harness.state.db,
        &project.id,
        &repo.id,
        &agent.id,
        &agent.id,
    )
    .await;
    (project, agent)
}

#[tokio::test]
async fn structured_review_findings_are_repaired_resolved_and_merged() {
    let repo_dir = common::TestDir::new("findings-release-repo");
    let repo_path = setup_git_repo(repo_dir.path()).await;
    let workspaces = common::TestDir::new("findings-release-workspaces");
    let harness = test_app(workspaces.path()).await;
    let (project, agent) = repair_release_project(&harness, &repo_path, workspaces.path()).await;
    let task: TaskResponse = json_request(&harness.app, Method::POST, &format!("/api/v1/projects/{}/tasks", project.id), json!({
        "title":"Deliver a working greeting",
        "description":"if test -f greeting.txt; then printf 'fixed\\n' > greeting.txt; else printf 'broken\\n' > greeting.txt; fi; git add greeting.txt && git commit -m greeting",
        "review_config":{"ci_steps":["test -f greeting.txt"], "review_prompt":r#"python3 - <<'PY'
import json
fixed=open('greeting.txt').read().strip()=='fixed'
if fixed:
    print('Read greeting.txt and confirmed the repaired greeting.')
    print(json.dumps({'result':'pass','reason':'Greeting works','resolved':['F1']}))
else:
    print('Read greeting.txt: expected fixed, observed broken.')
    print(json.dumps({'result':'fail','reason':'Greeting is broken','findings':[{'summary':'Greeting is broken','where':'greeting.txt','expected':'fixed','observed':'broken'}]}))
PY"#}
    }), StatusCode::OK).await;
    let _: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/claim", task.id),
        json!({"agent_id":agent.id}),
        StatusCode::OK,
    )
    .await;
    let completed = drain_until_task_status(&harness, &task.id, "done".into()).await;
    assert_eq!(completed.status, "done");
    let mut reviews = ReviewRepo::list_by_task(&*harness.state.db, &task.id)
        .await
        .unwrap();
    reviews.sort_by_key(|r| r.attempt_number);
    assert_eq!(reviews.len(), 2, "{reviews:?}");
    assert_eq!(reviews[0].status, db::ReviewStatus::Failed);
    assert_eq!(reviews[1].status, db::ReviewStatus::Passed);
    let first: Value = serde_json::from_str(&reviews[0].step_results_json).unwrap();
    assert_eq!(
        first["conformance"]["assessment"]["findings"][0]["summary"], "Greeting is broken",
        "{first}"
    );
    let findings: api_types::TaskFindingsResponse = empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/findings", task.id),
        StatusCode::OK,
    )
    .await;
    let finding = findings.items.iter().find(|f| f.id == "F1").unwrap();
    assert_eq!(finding.status, api_types::ReviewFindingStatus::Resolved);
    assert_eq!(finding.resolved_attempt, Some(2));
    let coders: Vec<(String, Option<String>)> = sqlx::query_as("SELECT id,parent_execution_id FROM execution WHERE task_id=? AND role='coder' ORDER BY created_at,id").bind(&task.id).fetch_all(harness.state.db.pool()).await.unwrap();
    assert_eq!(coders.len(), 2);
    assert!(coders.iter().any(|(_, parent)| parent
        .as_ref()
        .is_some_and(|p| coders.iter().any(|(id, _)| id == p))));
    assert_eq!(run_git(&repo_path, &["show", "HEAD:greeting.txt"]), "fixed");
    let merged: String = sqlx::query_scalar(
        "SELECT integrated_sha FROM integration_attempt WHERE task_ref=? AND state='completed'",
    )
    .bind(&task.id)
    .fetch_one(harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(merged, run_git(&repo_path, &["rev-parse", "HEAD"]));
    assert_task_conditions_match_legacy(&harness.state.db).await;
    common::assert_scheduler_clean(&harness.state).await;
}

#[tokio::test]
async fn pre_existing_base_test_failure_is_triaged_reviewed_and_merged() {
    let repo_dir = common::TestDir::new("triage-release-repo");
    let repo_path = setup_git_repo(repo_dir.path()).await;
    // One actual failing shell test, with cargo-style output for its stable id.
    std::fs::write(repo_path.join("base.txt"), "broken\n").unwrap();
    std::fs::write(repo_path.join("base-test.sh"), "if test \"$(cat base.txt)\" = fixed; then printf 'test suite::base ... ok\\n'; else printf 'test suite::base ... FAILED\\n'; exit 1; fi\n").unwrap();
    run_git(&repo_path, &["add", "base.txt", "base-test.sh"]);
    run_git(&repo_path, &["commit", "-m", "one known failing base test"]);
    let base = run_git(&repo_path, &["rev-parse", "HEAD"]);
    let workspaces = common::TestDir::new("triage-release-workspaces");
    let harness = test_app(workspaces.path()).await;
    let (project, agent) = repair_release_project(&harness, &repo_path, workspaces.path()).await;
    let task: TaskResponse = json_request(&harness.app, Method::POST, &format!("/api/v1/projects/{}/tasks", project.id), json!({
        "title":"Deliver alongside the known base failure",
        "description":"printf 'delivered\\n' > greeting.txt && git add greeting.txt && git commit -m delivered",
        "review_config":{"setup_steps":["true"],"ci_steps":["bash base-test.sh"], "review_prompt":r#"python3 - <<'PY'
import json,os
contract=json.loads(os.environ['FORGE_REVIEW_CONTRACT'])
assert open('greeting.txt').read().strip()=='delivered'
print('Verified the greeting; ci:0 is the known unchanged base failure.')
print(json.dumps({'result':'pass','reason':'Deliverable verified; existing base failure is triaged'}))
PY"#}
    }), StatusCode::OK).await;
    let _: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/claim", task.id),
        json!({"agent_id":agent.id}),
        StatusCode::OK,
    )
    .await;
    let completed = drain_until_task_status(&harness, &task.id, "done".into()).await;
    assert_eq!(completed.status, "done");
    let reviews = ReviewRepo::list_by_task(&*harness.state.db, &task.id)
        .await
        .unwrap();
    assert_eq!(reviews.len(), 1, "{reviews:?}");
    assert_eq!(reviews[0].status, db::ReviewStatus::Passed);
    assert!(reviews[0].reviewer_execution_id.is_some());
    let details: Value = serde_json::from_str(&reviews[0].step_results_json).unwrap();
    assert_ne!(details["ci_steps"][0]["exit_code"], 0);
    assert_eq!(
        details["ci_steps"][0]["triage"]["blocking"], false,
        "{details}"
    );
    assert_eq!(
        details["ci_steps"][0]["triage"]["failures"][0]["class"],
        "pre_existing"
    );
    assert_eq!(
        details["conformance"]["assessment"]["result"], "pass",
        "{details}"
    );
    let coders: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM execution WHERE task_id=? AND role='coder'")
            .bind(&task.id)
            .fetch_one(harness.state.db.pool())
            .await
            .unwrap();
    assert_eq!(coders, 1, "base failure must not dispatch a repair");
    let failed_checks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM check_result WHERE outcome='fail' AND run_id IN (SELECT run_id FROM check_consumer WHERE task_id=?)").bind(&task.id).fetch_one(harness.state.db.pool()).await.unwrap();
    assert!(
        failed_checks >= 3,
        "triage retains original, rerun and base failures"
    );
    assert_eq!(
        run_git(&repo_path, &["show", "HEAD:greeting.txt"]),
        "delivered"
    );
    assert_eq!(run_git(&repo_path, &["show", "HEAD:base.txt"]), "broken");
    assert_ne!(base, run_git(&repo_path, &["rev-parse", "HEAD"]));
    let merged: String = sqlx::query_scalar(
        "SELECT integrated_sha FROM integration_attempt WHERE task_ref=? AND state='completed'",
    )
    .bind(&task.id)
    .fetch_one(harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(merged, run_git(&repo_path, &["rev-parse", "HEAD"]));
    assert_task_conditions_match_legacy(&harness.state.db).await;
    common::assert_scheduler_clean(&harness.state).await;
}
