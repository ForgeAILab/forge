//! Execution environments, completion and native worker retries.
use super::*;

#[tokio::test]
async fn run_execution_dispatches_shell_adapter_and_updates_execution() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    // This case executes through the adapter registry, so the agent has to be the
    // shell harness: it runs the Task's own command and logs its output.
    // Any other harness shells out to a CLI that is not on the host.
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Run shell",
            Some("printf service-run-ok".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id, Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    assert_eq!(
        claimed.execution.hard_deadline_at, None,
        "Task executions have no wall-clock limit unless one is explicitly configured"
    );
    ExecutionRepo::update(
        &*db,
        db::UpdateExecution {
            id: claimed.execution.id.clone(),
            status: None,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            agent_session_id: Some(Some("test-session".to_owned())),
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("seed executor session id");
    let workspace =
        WorkspaceRepo::get_by_id(&*db, claimed.execution.workspace_id.as_deref().unwrap())
            .await
            .expect("workspace loads")
            .expect("workspace exists");
    std::fs::create_dir_all(
        service
            .workspace_backend_router()
            .embedded_path(&db, &workspace)
            .await
            .expect("workspace path resolves"),
    )
    .expect("workspace dir creates");

    let registry = Arc::new(cli_adapters::test_support::test_registry());
    let executor = executors::AdapterExecutor::new(registry);
    let execution = service
        .run_execution(claimed.execution.id, &executor)
        .await
        .expect("execution runs");

    assert_eq!(execution.status, ExecutionStatus::Completed);
    let logs_path = execution.logs_path.expect("logs path recorded");
    assert!(
        logs_path.contains(&format!(
            "/.forge/logs/{}/{}/",
            task.project_id, workspace.task_id
        )),
        "logs path should live under durable project/task log dir, got {logs_path}"
    );
    let logs = executors::LogReader::read(std::path::Path::new(&logs_path), 0, 100)
        .await
        .expect("logs read");
    assert!(logs.entries.iter().any(|entry| {
        entry.payload.get("line").and_then(|line| line.as_str()) == Some("service-run-ok")
    }));
}

#[tokio::test]
async fn run_execution_applies_the_project_environment() {
    let db = Arc::new(sqlite_db().await);
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(16)));
    let assets = tempfile::tempdir().expect("asset dir");
    std::fs::write(assets.path().join("fountain.png"), "art").expect("asset writes");
    let (_task, execution, workspace) = claim_shell_task_with_environment(
        &db,
        &service,
        "printf \"$PROJECT_TOOL:\"; cat vendor/fountain.png",
        json!({
            "env": { "PROJECT_TOOL": "godot-4.7.2" },
            "assets": [{
                "source": assets.path().to_string_lossy(),
                "target": "vendor",
            }],
            "checks": [{ "name": "tool", "command": "test -n \"$PROJECT_TOOL\"" }],
        }),
    )
    .await;

    let registry = Arc::new(cli_adapters::test_support::test_registry());
    let executor = executors::AdapterExecutor::new(registry);
    let execution = service
        .run_execution(execution.id, &executor)
        .await
        .expect("execution runs");

    assert_eq!(execution.status, ExecutionStatus::Completed);
    assert!(std::path::Path::new(
        &service
            .workspace_backend_router()
            .embedded_path(&db, &workspace)
            .await
            .expect("workspace path resolves")
    )
    .join("vendor/fountain.png")
    .exists());
    let logs = executors::LogReader::read(
        std::path::Path::new(&execution.logs_path.expect("logs path recorded")),
        0,
        100,
    )
    .await
    .expect("logs read");
    assert!(
        logs.entries.iter().any(|entry| {
            entry.payload.get("line").and_then(|line| line.as_str()) == Some("godot-4.7.2:art")
        }),
        "the executor sees the Project env and the copied asset: {:?}",
        logs.entries
            .iter()
            .filter_map(|entry| entry.payload.get("line"))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn run_execution_passes_checks_configured_before_direct_claim() {
    let db = Arc::new(sqlite_db().await);
    let service = TaskService::new(db.clone(), Arc::new(EventBus::new(16)));
    let (_, execution, _) = claim_shell_task_with_environment(
        &db,
        &service,
        "true",
        json!({"checks":[{"name":"tool","command":"true"}]}),
    )
    .await;
    let executor =
        executors::AdapterExecutor::new(Arc::new(cli_adapters::test_support::test_registry()));
    let completed = service
        .run_execution(execution.id, &executor)
        .await
        .unwrap();
    assert_eq!(completed.status, ExecutionStatus::Completed);
}

#[tokio::test]
async fn a_failed_environment_check_pauses_the_project_without_blocking_the_task() {
    let db = Arc::new(sqlite_db().await);
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(16)));
    let (task, execution, workspace) = claim_shell_task_with_environment(
        &db,
        &service,
        "touch agent-ran",
        json!({
            "checks": [{
                "name": "browser",
                "command": "echo chromium is not installed >&2; exit 4",
            }],
        }),
    )
    .await;
    let before = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads before dispatch")
        .expect("task exists before dispatch");

    let registry = Arc::new(cli_adapters::test_support::test_registry());
    let executor = executors::AdapterExecutor::new(registry);
    let execution = service
        .run_execution(execution.id, &executor)
        .await
        .expect("dispatch settles");

    assert_eq!(execution.status, ExecutionStatus::Failed);
    assert_eq!(execution.resume_policy, Some(db::ResumePolicy::Auto));
    assert!(
        !std::path::Path::new(
            &service
                .workspace_backend_router()
                .embedded_path(&db, &workspace)
                .await
                .expect("workspace path resolves")
        )
        .join("agent-ran")
        .exists(),
        "no agent run is spent on a known-broken environment"
    );
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, before.status);
    assert_eq!(
        current.version, before.version,
        "environment failure does not mutate the Task"
    );
    assert!(current.error_annotation.is_none());
    assert!(current.blocked_json.is_none());
    assert!(current.failed_json.is_none());
    assert!(execution
        .error
        .as_deref()
        .unwrap()
        .starts_with("environment not ready: "));
    let project = ProjectRepo::get_by_id(&*db, &task.project_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        project.system_pause_reason.as_deref(),
        Some("environment_not_ready")
    );
    let detail: api_types::ProjectEnvironmentPause =
        serde_json::from_str(project.environment_pause_json.as_deref().unwrap()).unwrap();
    assert_eq!(detail.checks, vec!["browser"]);
    assert_eq!(detail.role.as_deref(), Some(execution.role.as_str()));
    assert!(detail.output.contains("chromium is not installed"));
    assert_eq!(
        project.paused_at.as_deref(),
        Some(detail.paused_at.as_str())
    );
    assert_eq!(detail.paused_at, detail.last_checked_at);
    let last = chrono::DateTime::parse_from_rfc3339(&detail.last_checked_at).unwrap();
    let next = chrono::DateTime::parse_from_rfc3339(&detail.next_check_at).unwrap();
    assert_eq!((next - last).num_seconds(), 600);
}

#[tokio::test]
async fn workflow_dispatched_commitless_completion_fails_and_schedules_retry() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "No-op completion",
            Some("narrate without changing anything".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    sqlx::query("UPDATE execution SET role = 'coder' WHERE id = ?")
        .bind(&claimed.execution.id)
        .execute(db.pool())
        .await
        .expect("execution uses the autonomous coder role");
    // Stamp the dispatcher metadata the workflow dispatch path records, so
    // this run is measured as an autonomous role dispatch (user-claimed runs
    // without the metadata stay exempt).
    let execution = ExecutionRepo::get_by_id(&*db, &claimed.execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    let mut snapshot: serde_json::Value = serde_json::from_str(
        execution
            .executor_config_snapshot_json
            .as_deref()
            .expect("claimed execution has a snapshot"),
    )
    .expect("snapshot parses");
    snapshot["dispatch"] = json!({ "target_role": execution.role });
    ExecutionRepo::update(
        &*db,
        db::UpdateExecution {
            id: claimed.execution.id.clone(),
            status: None,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(Some(snapshot.to_string())),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("dispatch metadata stamps");

    let updated = service
        .run_execution(claimed.execution.id.clone(), &NoDiffExecutor)
        .await
        .expect("execution runs");

    assert_eq!(updated.status, ExecutionStatus::Failed);
    assert_eq!(updated.role, "coder");
    assert!(
        updated
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("nothing committed on the Task branch"),
        "error names the commit-less completion: {:?}",
        updated.error
    );

    // The failure enters the normal executor-failure machinery: the retry
    // budget is consumed and the execution becomes dispatcher-resumable.
    let after = ExecutionRepo::get_by_id(&*db, &claimed.execution.id)
        .await
        .expect("execution reloads")
        .expect("execution exists");
    assert_eq!(after.resume_policy, Some(db::ResumePolicy::Auto));
    let task_after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let metadata: serde_json::Value =
        serde_json::from_str(task_after.metadata_json.as_deref().unwrap_or("{}"))
            .expect("task metadata parses");
    assert_eq!(
        json!(
            db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
                .await
                .unwrap()
        ),
        1
    );
    assert!(
        metadata.get("deferred_dispatch").is_some(),
        "a deferred redispatch is scheduled: {metadata}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn finalized_completion_does_not_run_repository_fsmonitor_diagnostic() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Finalize without a second status",
            Some("write and finalize the implementation".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id, Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    sqlx::query("UPDATE execution SET role = 'coder' WHERE id = ?")
        .bind(&claimed.execution.id)
        .execute(db.pool())
        .await
        .expect("execution uses the autonomous coder role");
    let execution = ExecutionRepo::get_by_id(&*db, &claimed.execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    let mut snapshot: serde_json::Value = serde_json::from_str(
        execution
            .executor_config_snapshot_json
            .as_deref()
            .expect("claimed execution has a snapshot"),
    )
    .expect("snapshot parses");
    snapshot["dispatch"] = json!({ "target_role": execution.role });
    ExecutionRepo::update(
        &*db,
        db::UpdateExecution {
            id: claimed.execution.id.clone(),
            status: None,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(Some(snapshot.to_string())),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("dispatch metadata stamps");

    let workspace = WorkspaceRepo::get_by_id(
        &*db,
        claimed
            .execution
            .workspace_id
            .as_deref()
            .expect("workspace id exists"),
    )
    .await
    .expect("workspace loads")
    .expect("workspace exists");
    let workspace_path = service
        .workspace_backend_router()
        .embedded_path(&db, &workspace)
        .await
        .expect("workspace path resolves");
    let worktree = workspace_path.as_path();
    let marker = repo_dir.path().join("fsmonitor-ran");
    let monitor = worktree.join("fsmonitor.sh");
    process_supervisor::executable::write_executable(
        &monitor,
        format!(
            "#!/bin/sh\nprintf ran > \"{}\"\nprintf 'token\\n'\n",
            marker.display()
        ),
    )
    .expect("fsmonitor script writes");
    let config = std::process::Command::new("git")
        .arg("-C")
        .arg(worktree)
        .args(["config", "core.fsmonitor"])
        .arg(&monitor)
        .output()
        .expect("git config runs");
    assert!(
        config.status.success(),
        "git config failed: {}",
        String::from_utf8_lossy(&config.stderr)
    );

    let updated = service
        .run_execution(claimed.execution.id, &HostFinalizingExecutor)
        .await
        .expect("execution runs");

    assert_eq!(updated.status, ExecutionStatus::Completed);
    assert!(updated.after_sha.is_some());
    assert!(
        !marker.exists(),
        "runner must not execute fsmonitor after host finalization"
    );
}

#[tokio::test]
async fn uncommitted_native_worker_failure_prompts_a_retry_and_preserves_the_diff() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Commit the preserved implementation",
            Some("write the implementation and commit it".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    let workspace = WorkspaceRepo::get_by_id(
        &*db,
        claimed
            .execution
            .workspace_id
            .as_deref()
            .expect("workspace id exists"),
    )
    .await
    .expect("workspace loads")
    .expect("workspace exists");

    let updated = service
        .run_execution(
            claimed.execution.id.clone(),
            &UncommittedWorktreeFailureExecutor,
        )
        .await
        .expect("execution failure is persisted");

    let expected_error = ExecutorError::Other(
        crate::task_service::execution::UNCOMMITTED_WORKTREE_FAILURE.to_owned(),
    )
    .to_string();
    assert_eq!(updated.status, ExecutionStatus::Failed);
    assert_eq!(updated.error.as_deref(), Some(expected_error.as_str()));
    assert!(std::path::Path::new(
        &service
            .workspace_backend_router()
            .embedded_path(&db, &workspace)
            .await
            .expect("workspace path resolves")
    )
    .join("uncommitted.txt")
    .exists());
    assert!(!git::is_worktree_clean(std::path::Path::new(
        &service
            .workspace_backend_router()
            .embedded_path(&db, &workspace)
            .await
            .expect("workspace path resolves")
    ))
    .await
    .expect("worktree cleanliness reads"));

    let comments = db::TaskCommentRepo::list_comments(
        &*db,
        &task.id,
        db::PageRequest {
            cursor: None,
            limit: 100,
            include_total: false,
            sort_by: db::SortBy::CreatedAt,
            sort_order: db::SortOrder::Asc,
        },
    )
    .await
    .expect("comments load");
    assert!(comments.items.iter().any(|comment| {
        comment.content.contains("worktree was preserved")
            && comment.content.contains("commit the result")
    }));

    let task_after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let metadata: serde_json::Value =
        serde_json::from_str(task_after.metadata_json.as_deref().unwrap_or("{}"))
            .expect("task metadata parses");
    assert_eq!(
        json!(
            db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
                .await
                .unwrap()
        ),
        1
    );
    assert!(metadata.get("deferred_dispatch").is_some());
    let execution_after = ExecutionRepo::get_by_id(&*db, &claimed.execution.id)
        .await
        .expect("execution reloads")
        .expect("execution exists");
    assert_eq!(execution_after.resume_policy, Some(db::ResumePolicy::Auto));
}

#[tokio::test]
async fn user_claimed_commitless_completion_stays_completed() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Claimed no-op",
            Some("user-invoked run".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id, Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");

    // No dispatcher metadata: the claim-launched run keeps the historical
    // completion semantics even when it changes nothing.
    let updated = service
        .run_execution(claimed.execution.id, &NoDiffExecutor)
        .await
        .expect("execution runs");

    assert_eq!(updated.status, ExecutionStatus::Completed);
}
