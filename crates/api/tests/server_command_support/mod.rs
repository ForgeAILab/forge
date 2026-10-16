use axum::http::{Method, StatusCode};
use serde_json::{json, Value};
use std::{path::Path, sync::Arc};

use crate::common;

pub async fn fixture(name: &str) -> (common::TestDir, common::Harness, String, String, Value) {
    let root = common::TestDir::new(name);
    let harness = common::test_app(root.path(), name).await;
    let repo = common::setup_git_repo(root.path());
    let (project, repo_id) = common::create_project_and_repo(&harness.app, name, &repo).await;
    let task: Value = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project}/tasks"),
        json!({"title":name}),
        StatusCode::OK,
    )
    .await;
    (root, harness, project, repo_id, task)
}

pub async fn store_settings(harness: &common::Harness, project: &str, settings: Value) {
    // Bypass write validation deliberately: upgraded/template/seed rows must
    // still be refused at execution. Production services do not use this SQL.
    sqlx::query("UPDATE project SET settings = ? WHERE id = ?")
        .bind(settings.to_string())
        .bind(project)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
}

pub async fn registered_owner_can_save_hook(disabled: bool) {
    let root = common::TestDir::new("registered-command-owner");
    let harness = common::test_app(root.path(), "registered-command-owner").await;
    let _: api_types::AuthResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/auth/register",
        json!({"email":"owner@example.org","password":"OwnerPassword123!"}),
        StatusCode::CREATED,
    )
    .await;
    let auth: api_types::AuthResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/auth/register",
        json!({"email":"visitor@example.org","password":"VisitorPassword123!"}),
        StatusCode::CREATED,
    )
    .await;
    let user: api_types::UserResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/auth/me",
        &auth.access_token,
        StatusCode::OK,
    )
    .await;
    assert!(!user.is_admin, "the visitor must be a registered non-admin");
    let project: Value = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        &auth.access_token,
        json!({"name":"Visitor's own Project"}),
        StatusCode::OK,
    )
    .await;
    let response: Value = common::json_request_with_bearer(
        &harness.app,
        Method::PATCH,
        &format!("/api/v1/projects/{}", project["id"].as_str().unwrap()),
        &auth.access_token,
        json!({"version":project["version"],"settings":{"lifecycle_hooks":{"before_work":[
            {"type":"script","command":"printf allowed","timeout_seconds":2,"blocking":true}
        ]}}}),
        if disabled {
            StatusCode::UNPROCESSABLE_ENTITY
        } else {
            StatusCode::OK
        },
    )
    .await;
    if disabled {
        assert_eq!(response["code"], "server_commands_disabled");
        assert_eq!(response["message"], "This demo does not run commands");
    } else {
        assert_eq!(
            response["settings"]["lifecycle_hooks"]["before_work"][0]["command"],
            "printf allowed"
        );
    }
}

pub async fn stored_lifecycle_event(disabled: bool) {
    let (root, harness, project, _, task) = fixture("stored-lifecycle-command").await;
    let marker = root.path().join("hook-marker");
    store_settings(&harness,&project,json!({"lifecycle_hooks":{"on_task_done":[
        {"type":"script","command":format!("touch '{}'",marker.display()),"timeout_seconds":2,"blocking":false}
    ]}})).await;
    // The stored script is loaded by the actual event consumer, without an
    // Agent/daemon or an execution being present.
    let emitter = services::lifecycle::LifecycleEventEmitter::new_for_test(
        Arc::clone(&harness.state.db),
        Arc::new(services::lifecycle::PluginRegistry::default()),
    );
    let bus = events::EventBus::new(8);
    let receiver = bus.subscribe();
    let (stop, signal) = tokio::sync::watch::channel(false);
    let job = tokio::spawn(async move { emitter.run_with_shutdown(receiver, signal).await });
    bus.publish(events::ForgeEvent {
        event_type: "task.status_changed".into(),
        entity_id: task["id"].as_str().unwrap().into(),
        timestamp: db::now_rfc3339(),
        context: events::EventContext::TaskStatusChanged {
            project_id: project,
            old_status: "review".into(),
            new_status: "done".into(),
        },
    });
    tokio::time::timeout(std::time::Duration::from_secs(5),async {
        loop {
            if disabled {
                let count:i64=sqlx::query_scalar("SELECT COUNT(*) FROM task_comment WHERE task_id = ? AND content LIKE '%server_commands_disabled%'")
                    .bind(task["id"].as_str().unwrap()).fetch_one(harness.state.db.pool()).await.unwrap();
                if count>0 {break;}
            } else if marker.exists() {break;}
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.expect("lifecycle event settles visibly");
    stop.send(true).unwrap();
    job.await.unwrap();
    assert_eq!(marker.exists(), !disabled);
}

pub async fn blocking_hook(disabled: bool) {
    let root = common::TestDir::new("blocking-command-policy");
    let marker = root.path().join("blocking-marker");
    let ctx = services::lifecycle::LifecycleHookContext {
        event: api_types::LifecycleEvent::BeforeWork,
        task_id: db::new_uuid_v4(),
        task_title: "hook".into(),
        task_status: "in_progress".into(),
        previous_status: "todo".into(),
        project_id: db::new_uuid_v4(),
        project_name: "hook".into(),
        repo_path: root.path().to_string_lossy().into_owned(),
        worktree_path: None,
        agent_id: None,
        execution_id: None,
        log_dir: Some(root.path().join("logs")),
        env: Default::default(),
    };
    let result = services::lifecycle::LifecycleHookRunner::run_blocking_before_work_hooks(
        ctx,
        &[api_types::LifecycleHookDef::Script {
            command: format!("touch '{}'", marker.display()),
            timeout_seconds: 2,
            blocking: true,
        }],
    )
    .await;
    if disabled {
        let result = result.expect("failed hook result");
        assert_eq!(result.entry["code"], "server_commands_disabled");
        let log = std::fs::read_to_string(result.log_path.unwrap()).unwrap();
        assert!(log.contains("server_commands_disabled"));
        assert!(result.exit_code.is_none());
    } else {
        assert!(result.is_none());
    }
    assert_eq!(marker.exists(), !disabled);
}

pub async fn environment_setup(disabled: bool) {
    let root = common::TestDir::new("environment-command-policy");
    let marker = root.path().join("environment-marker");
    let spec = services::workspace_backend::RunSpec {
        purpose: api_types::WorkspaceRunPurpose::EnvironmentSetup,
        command: format!("touch '{}'", marker.display()),
        env: Default::default(),
        timeout_secs: 2,
        max_output_bytes: 4096,
    };
    let result = services::integration_effects::check::run_at(root.path(), &spec).await;
    if disabled {
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("server_commands_disabled"));
    } else {
        assert_eq!(result.unwrap().exit_code, 0);
    }
    assert_eq!(marker.exists(), !disabled);
    let check_marker = root.path().join("check-marker");
    let check = api_types::EnvironmentCheck {
        scope: Default::default(),
        name: "probe".into(),
        command: format!("touch '{}'", check_marker.display()),
        roles: vec![],
        timeout_seconds: 2,
    };
    let result =
        executors::environment::run_environment_check(root.path(), &Default::default(), &check)
            .await;
    assert_eq!(result.passed, !disabled);
    if disabled {
        assert!(result.output_tail.contains("server_commands_disabled"));
        assert!(result.exit_code.is_none());
    }
    assert_eq!(check_marker.exists(), !disabled);
}

pub async fn seed_candidate(
    harness: &common::Harness,
    task: &str,
    repo: &str,
    root: &Path,
) -> String {
    let path = root.join("worktrees").join(task).join("repo");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    // Local, Forge-built Git arguments only; no remote or user hook runs.
    let status = std::process::Command::new("git")
        .args(["-C"])
        .arg(root.join("repo"))
        .args(["worktree", "add", "-b"])
        .arg(workspace::task_branch_name(task))
        .arg(&path)
        .status()
        .unwrap();
    assert!(status.success());
    let sha = git::get_current_sha(&path).await.unwrap();
    let now = db::now_rfc3339();
    let workspace_id = db::new_uuid_v4();
    db::WorkspaceRepo::create(
        &*harness.state.db,
        db::CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task.into(),
            repo_id: repo.into(),
            worktree_path: path.to_string_lossy().into_owned(),
            branch: workspace::task_branch_name(task),
            status: db::WorkspaceStatus::Ready,
            before_sha: Some(sha.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    db::ExecutionRepo::create(
        &*harness.state.db,
        db::CreateExecution {
            id: db::new_uuid_v4(),
            task_id: task.into(),
            agent_id: None,
            role: "executor".into(),
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
            before_sha: Some(sha.clone()),
            after_sha: Some(sha),
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace_id),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    path.to_string_lossy().into_owned()
}

pub async fn stored_ci_review_entry(disabled: bool) {
    let (root, harness, project, repo, task) = fixture("stored-ci-command").await;
    let task_id = task["id"].as_str().unwrap();
    let marker = root.path().join("ci-marker");
    seed_candidate(&harness, task_id, &repo, root.path()).await;
    store_settings(
        &harness,
        &project,
        json!({"default_review_config":{"ci_steps":[format!("touch '{}'",marker.display())]}}),
    )
    .await;
    // Exercise the actual durable review-entry hooks, stopping at owner approval
    // so a normal passing check does not enter the unrelated integration queue.
    let row = db::ProjectRepo::get_by_id(&*harness.state.db, &project)
        .await
        .unwrap()
        .unwrap();
    let mut workflow: api_types::WorkflowDefinition =
        serde_json::from_str(&row.workflow_definition).unwrap();
    workflow
        .states
        .iter_mut()
        .find(|s| s.name == "review")
        .unwrap()
        .gate_config
        .as_mut()
        .unwrap()
        .requires_user_approval = Some(true);
    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(serde_json::to_string(&workflow).unwrap())
        .bind(&project)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task SET status = 'in_progress' WHERE id = ?")
        .bind(task_id)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let task = db::TaskRepo::get_by_id(&*harness.state.db, task_id, false)
        .await
        .unwrap()
        .unwrap();
    harness
        .state
        .task_service
        .transition(task_id, "review".to_owned(), task.version)
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            harness.state.task_service.drain(task_id).await.unwrap();
            let task = db::TaskRepo::get_by_id(&*harness.state.db, task_id, false)
                .await
                .unwrap()
                .unwrap();
            if disabled {
                if task
                    .error_annotation
                    .as_deref()
                    .is_some_and(|s| s.contains("server_commands_disabled"))
                {
                    assert!(task
                        .blocked_json
                        .as_deref()
                        .unwrap()
                        .contains("server_commands_disabled"));
                    assert_eq!(task.status, "review");
                    break;
                }
            } else if db::ReviewRepo::list_by_task(&*harness.state.db, task_id)
                .await
                .unwrap()
                .iter()
                .any(|review| {
                    review.step_results_json.contains("exit_code")
                        && review.step_results_json.contains("ci-marker")
                })
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("review-entry CI settles");
    assert_eq!(marker.exists(), !disabled);
    if disabled {
        let checks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM check_run")
            .fetch_one(harness.state.db.pool())
            .await
            .unwrap();
        assert_eq!(
            checks, 0,
            "refusal must precede queueing and login-shell probing"
        );
    }
}

pub async fn send(
    app: &axum::Router,
    method: Method,
    uri: &str,
    body: Value,
) -> (StatusCode, Value) {
    let response = common::raw_json_request(app, method, uri, body).await;
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn assert_refused(response: &(StatusCode, Value), code: &str, what: &str) {
    assert_eq!(
        response.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{what}: {}",
        response.1
    );
    assert_eq!(response.1["code"], code, "{what}: {}", response.1);
}

/// Provider credentials (API key and OAuth) are what a server-side agent turn
/// would use; Chat turn admission runs one.
pub async fn provider_and_chat_turns(disabled: bool) {
    let (_root, harness, project, _, _) = fixture("demo-agent-turns").await;
    let provider = send(
        &harness.app,
        Method::POST,
        "/api/v1/providers",
        json!({"provider":"openai_compatible","label":"visitor key",
            "credential":"sk-visitor","base_url":"https://8.8.8.8"}),
    )
    .await;
    let chat = harness
        .state
        .agent_chat_service
        .ensure_project_chat(&project)
        .await
        .unwrap();
    let message = send(
        &harness.app,
        Method::POST,
        &format!("/api/v1/agent-chats/{}/messages", chat.id),
        json!({"content":"hello"}),
    )
    .await;
    if disabled {
        assert_refused(&provider, "server_agents_disabled", "provider entry");
        assert_eq!(
            provider.1["message"],
            "This demo does not run agents or connect model providers"
        );
        // No provider is contacted: the refusal precedes the OAuth flow.
        let authorization = send(
            &harness.app,
            Method::POST,
            "/api/v1/provider-authorizations",
            json!({"provider":"openai","method":"device_oauth",
                "redirect_origin":"http://localhost","credential_label":"visitor"}),
        )
        .await;
        assert_refused(
            &authorization,
            "server_agents_disabled",
            "provider authorization",
        );
        assert_refused(&message, "server_agents_disabled", "chat message");
        let turns: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_turn_job")
            .fetch_one(harness.state.db.pool())
            .await
            .unwrap();
        assert_eq!(turns, 0, "no turn is queued");
    } else {
        assert_eq!(provider.0, StatusCode::OK, "{}", provider.1);
        assert_ne!(message.1["code"], "server_agents_disabled", "{}", message.1);
    }
}
