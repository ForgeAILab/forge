//! Shared fixtures and assertions for task executions.
use super::*;

/// Claim a shell-harness Task whose command is `command` in a Project whose
/// settings declare `environment`, and return the claimed execution.
pub(super) async fn claim_shell_task_with_environment(
    db: &Arc<SqliteDb>,
    service: &TaskService,
    command: &str,
    environment: Value,
) -> (Task, db::Execution, db::Workspace) {
    let (project_id, _repo_id, repo_dir) = seed_project_repo(db).await;
    // The repository directory must outlive the test's executions.
    std::mem::forget(repo_dir);
    let agent_id = seed_agent(db).await;
    let task = service
        .create_task(
            project_id,
            "Needs its environment",
            Some(command.to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    sqlx::query("UPDATE project SET settings = ? WHERE id = ?")
        .bind(json!({ "environment": environment }).to_string())
        .bind(&task.project_id)
        .execute(db.pool())
        .await
        .expect("environment sets");
    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    let workspace =
        WorkspaceRepo::get_by_id(&**db, claimed.execution.workspace_id.as_deref().unwrap())
            .await
            .expect("workspace loads")
            .expect("workspace exists");
    std::fs::create_dir_all(
        service
            .workspace_backend_router()
            .embedded_path(db, &workspace)
            .await
            .expect("workspace path resolves"),
    )
    .expect("workspace dir creates");
    (task, claimed.execution, workspace)
}

/// A Task in review with a running reviewer execution whose contract is
/// admitted, ready for the reviewer's final reply.
pub(super) async fn seed_admitted_review(
) -> (Arc<SqliteDb>, TaskService, Task, db::Execution, TempDir) {
    seed_admitted_review_with(r#"{"retry_budgets":{"execution":3,"review":3}}"#).await
}

pub(super) async fn seed_admitted_review_with(
    task_state_config: &str,
) -> (Arc<SqliteDb>, TaskService, Task, db::Execution, TempDir) {
    seed_admitted_review_with_workflow(task_state_config, None).await
}

pub(super) async fn seed_admitted_review_with_workflow(
    task_state_config: &str,
    workflow: Option<WorkflowDefinition>,
) -> (Arc<SqliteDb>, TaskService, Task, db::Execution, TempDir) {
    // `run_ci_steps` records each configured step before the contract admits.
    let pre_review_ci_steps: Vec<Value> = serde_json::from_str::<Value>(task_state_config)
        .expect("task state config parses")
        .pointer("/review/ci_steps")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
        .map(|(index, command)| {
            json!({"index": index, "command": command, "exit_code": 0, "stderr_tail": ""})
        })
        .collect();
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, repo_dir) = seed_project_repo(&db).await;
    if let Some(workflow) = workflow {
        sqlx::query("UPDATE project SET workflow_definition=? WHERE id=?")
            .bind(serde_json::to_string(&workflow).unwrap())
            .bind(&project_id)
            .execute(db.pool())
            .await
            .unwrap();
    }

    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    sqlx::query("UPDATE task SET task_state_config = ?, metadata_json = ? WHERE id = ?")
        .bind(task_state_config)
        .bind("{}")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("retry policy sets");

    let now = now_rfc3339();
    let workspace = WorkspaceRepo::create(
        &*db,
        CreateWorkspace {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            repo_id: repo_id.clone(),
            worktree_path: repo_dir.path().to_string_lossy().into_owned(),
            branch: "review-candidate".to_owned(),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("review workspace creates");
    let candidate = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: crate::workflow::default_roles::CODER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("candidate implementation".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace.id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("candidate execution creates");
    let execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: Some(candidate.id.clone()),
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                workflow_execution_snapshot(&db, &project_id).await,
            ),
            workspace_id: Some(workspace.id),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("running reviewer execution creates");
    let review = ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate.id,
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": pre_review_ci_steps }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("running review creates");
    sqlx::query("UPDATE review SET reviewer_execution_id = ? WHERE id = ?")
        .bind(&execution.id)
        .bind(&review.id)
        .execute(db.pool())
        .await
        .expect("reviewer execution binds");

    ::review::contract::admit(&db, &execution.id, &task.id, repo_dir.path())
        .await
        .expect("review contract admits");
    (db, service, task, execution, repo_dir)
}

/// Complete the reviewer execution with `reply` and run the review cascade.
pub(super) async fn complete_review_with(
    db: &SqliteDb,
    service: &TaskService,
    execution: &db::Execution,
    reply: &str,
) {
    sqlx::query(
        "UPDATE execution
         SET status = 'completed', summary = ?, updated_at = ?
         WHERE id = ?",
    )
    .bind(reply)
    .bind(now_rfc3339())
    .bind(&execution.id)
    .execute(db.pool())
    .await
    .expect("reviewer execution completes");
    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("completed review cascades");
}

/// Insert the preceding attempt without changing the current execution binding.
pub(super) async fn seed_previous_review_attempt(
    db: &SqliteDb,
    task_id: &str,
    status: ReviewStatus,
) {
    let failed = status == ReviewStatus::Failed;
    let current = ReviewRepo::list_by_task(db, task_id)
        .await
        .unwrap()
        .remove(0);
    sqlx::query("UPDATE review SET attempt_number = 2 WHERE id = ?")
        .bind(&current.id)
        .execute(db.pool())
        .await
        .unwrap();
    ReviewRepo::create(
        db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            execution_id: current.execution_id,
            attempt_number: 1,
            status,
            step_results_json: json!({"conformance": {"status":"failed", "contract":null,
                "checks":[], "reason":"previous finding", "assessment":{"result":"fail", "reason":"previous finding"}}}).to_string(),
            started_at: current.started_at,
            created_at: current.created_at,
            updated_at: current.updated_at,
        },
    )
    .await
    .unwrap();
    if failed {
        TransitionLogRepo::insert(
            db,
            db::CreateTransitionLog {
                id: new_uuid_v4(),
                task_id: task_id.to_owned(),
                from_state: crate::workflow::default_states::REVIEW.to_owned(),
                to_state: crate::workflow::default_states::IN_PROGRESS.to_owned(),
                trigger_name: Some("reject".to_owned()),
                triggered_by: "system:workflow".to_owned(),
                bridge: Default::default(),
                trigger_reason: "previous review failed".to_owned(),
                hook_results_json: None,
                rejection: true,
                created_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
    }
}

pub(super) async fn assert_owner_review_park(db: &SqliteDb, task_id: &str, message: &str) {
    let task = TaskRepo::get_by_id(db, task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.status, crate::workflow::default_states::REVIEW);
    assert!(task.review_passed_at.is_none());
    let annotation: api_types::TaskBlockingAnnotation =
        serde_json::from_str(task.error_annotation.as_deref().expect("owner annotation")).unwrap();
    assert_eq!(
        annotation.annotation_type,
        api_types::FailureKind::ReviewNeedsOwner
    );
    assert_eq!(annotation.blocking_reason, "review_needs_owner");
    assert_eq!(annotation.message.as_deref(), Some(message));
    assert!(serde_json::to_value(&annotation)
        .unwrap()
        .get("recovery_actions")
        .is_none());
    let blocked: Value = serde_json::from_str(task.blocked_json.as_deref().unwrap()).unwrap();
    assert_eq!(blocked["kind"], "review_needs_owner");
    assert!(task.failed_json.is_none());
    let reviews = ReviewRepo::list_by_task(db, task_id).await.unwrap();
    let latest = reviews
        .iter()
        .max_by_key(|review| review.attempt_number)
        .unwrap();
    assert_eq!(latest.status, ReviewStatus::Failed);
    assert!(latest.finished_at.is_some());
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(db, task_id, crate::workflow::default_roles::CODER,)
            .await
            .unwrap(),
        1,
        "only the seeded candidate coder execution exists"
    );
    let snapshot = crate::task_actions::load_snapshot(
        db,
        task.clone(),
        crate::workflow::default_workflow::default_workflow(),
        &Actor::user(UserActionSource::Test),
        None,
    )
    .await
    .unwrap();
    let offers = crate::available_actions(&snapshot);
    let exception = crate::task_diagnostics::task_exception(&snapshot, offers.clone())
        .expect("owner exception");
    assert_eq!(exception.actions, offers);
    assert!(exception
        .actions
        .iter()
        .any(|offer| offer.reason == "review_needs_owner"));
}

pub(super) async fn assert_failed_reviewer_disposition(
    retry_count: u64,
    budget: u64,
    should_retry: bool,
    protocol: bool,
) {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut events = event_bus.subscribe();
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::REVIEWER,
        Some(&agent_id),
    )
    .await;
    seed_budget_spent(
        &db,
        &task.id,
        db::budget::Kind::Execution,
        retry_count as i64,
    )
    .await;
    sqlx::query("UPDATE task SET task_state_config = ?, metadata_json = ? WHERE id = ?")
        .bind(json!({ "retry_budgets": { "execution": budget } }).to_string())
        .bind("{}")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("retry policy sets");
    let now = now_rfc3339();
    let execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: if protocol {
                ExecutionStatus::Completed
            } else {
                ExecutionStatus::Failed
            },
            stop_reason: Some(db::StopReason::ExecutorFailed),
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: Some(db::ResumePolicy::Manual),
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: Some("reviewer-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some(
                if protocol {
                    "===REVIEW: PASS==="
                } else {
                    "reviewer quit before verdict"
                }
                .to_owned(),
            ),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some("claude-code exited with status exit status: 1".to_owned()),
            executor_config_snapshot_json: Some(
                workflow_execution_snapshot(&db, &project_id).await,
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("failed reviewer execution creates");
    ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: execution.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("running review creates");

    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("reviewer failure cascade succeeds");

    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(
        reviews[0].status,
        if should_retry {
            ReviewStatus::Running
        } else {
            ReviewStatus::Failed
        }
    );
    assert_eq!(reviews[0].finished_at.is_some(), !should_retry);
    let details: serde_json::Value =
        serde_json::from_str(&reviews[0].step_results_json).expect("details parse");
    assert_eq!(details["auditor"]["verdict"], "fail");
    assert_eq!(
        details["execution"]["error"],
        if protocol {
            "review execution has no workspace evidence"
        } else {
            "claude-code exited with status exit status: 1"
        }
    );
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, "review");
    let current_execution = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    let metadata: serde_json::Value =
        serde_json::from_str(current.metadata_json.as_deref().unwrap_or("{}"))
            .expect("metadata parses");
    if should_retry {
        assert_eq!(details["execution_retry"]["execution_id"], execution.id);
        assert_eq!(details["execution_retry"]["status"], "scheduled");
        assert_eq!(
            current_execution.resume_policy,
            Some(db::ResumePolicy::Auto)
        );
        assert!(current.blocked_json.is_none());
        assert!(current.error_annotation.is_none());
        assert_eq!(
            json!(
                db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
                    .await
                    .unwrap()
            ),
            retry_count + 1
        );
        assert!(metadata.get("deferred_dispatch").is_some());
    } else {
        assert_eq!(
            current_execution.resume_policy,
            Some(db::ResumePolicy::Manual)
        );
        let blocked: serde_json::Value = serde_json::from_str(
            current
                .blocked_json
                .as_deref()
                .expect("task has a durable blocker"),
        )
        .expect("blocker parses");
        assert_eq!(blocked["kind"], "internal_command_failed");
        assert_eq!(blocked["execution_id"], execution.id);
        let annotation: api_types::TaskBlockingAnnotation = serde_json::from_str(
            current
                .error_annotation
                .as_deref()
                .expect("recovery annotation exists"),
        )
        .expect("annotation parses");
        assert_eq!(
            annotation.annotation_type,
            api_types::FailureKind::ExecutorFailed
        );
        assert_eq!(
            annotation.blocked_execution_id.as_deref(),
            Some(execution.id.as_str())
        );
        assert!(serde_json::to_value(&annotation)
            .unwrap()
            .get("recovery_actions")
            .is_none());
        assert!(serde_json::to_value(&annotation)
            .unwrap()
            .get("recovery_actions")
            .is_none());
        assert_eq!(
            json!(
                db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
                    .await
                    .unwrap()
            ),
            retry_count
        );
        assert!(metadata.get("deferred_dispatch").is_none());
        assert!(current.failed_json.is_none());
    }

    // Repeated completion delivery must not emit another blocker or spend budget.
    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("duplicate completion succeeds");
    let repeated = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(repeated.version, current.version);
    assert_eq!(repeated.blocked_json, current.blocked_json);
    assert_eq!(repeated.metadata_json, current.metadata_json);
    let mut blocker_events = 0;
    while let Ok(event) = events.try_recv() {
        if event.event_type == "task.blocked" && event.entity_id == task.id {
            blocker_events += 1;
        }
    }
    assert_eq!(blocker_events, usize::from(!should_retry));
}

pub(super) struct PendingExecutor;

#[async_trait::async_trait]
impl TaskExecutor for PendingExecutor {
    async fn execute(
        &self,
        _ctx: ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutorError> {
        std::future::pending::<std::result::Result<ExecutionResult, ExecutorError>>().await
    }

    async fn cancel(&self, _execution_id: &str) -> std::result::Result<(), ExecutorError> {
        Ok(())
    }
}

/// A failed coder run dispatched under the Project revision before the
/// current one. `snapshot` adds to its executor snapshot.
pub(super) async fn seed_failed_coder_execution_from_an_older_revision(
    db: &SqliteDb,
    task: &Task,
    agent_id: &str,
    snapshot: Value,
) -> Execution {
    let execution = seed_completed_coder_execution(db, task, agent_id, None).await;
    let mut config = json!({
        "executor_type": "shell",
        "config": {},
        "project_version": ProjectRepo::get_by_id(db, &task.project_id)
            .await
            .expect("project loads")
            .expect("project exists")
            .version,
    });
    for (key, value) in snapshot.as_object().into_iter().flatten() {
        config[key] = value.clone();
    }
    sqlx::query(
        "UPDATE execution
         SET status = 'failed', error = 'executor failed', resume_policy = 'manual',
             executor_config_snapshot_json = ?
         WHERE id = ?",
    )
    .bind(config.to_string())
    .bind(&execution.id)
    .execute(db.pool())
    .await
    .expect("execution fails");
    sqlx::query("UPDATE project SET version = version + 1 WHERE id = ?")
        .bind(&task.project_id)
        .execute(db.pool())
        .await
        .expect("project revision advances");
    ExecutionRepo::get_by_id(db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists")
}

pub(super) async fn failure_fence_fixture() -> (Arc<SqliteDb>, TaskService, Task, String) {
    let db = Arc::new(sqlite_db().await);
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(16)));
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        Some(&agent_id),
    )
    .await;
    (db, service, task, agent_id)
}

/// The failure left no trace on the Task: no retry, no charge, no blocker.
pub(super) async fn assert_failure_was_inert(db: &SqliteDb, task: &Task, why: &str) {
    let current = TaskRepo::get_by_id(db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let metadata = current.metadata().expect("metadata parses");
    assert!(metadata.extra.get("deferred_dispatch").is_none(), "{why}");
    assert!(
        metadata
            .extra
            .get("last_execution_failure_execution_id")
            .is_none(),
        "{why}"
    );
    assert_eq!(
        db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
            .await
            .expect("budget loads"),
        0,
        "{why}"
    );
    assert!(current.blocked_json.is_none(), "{why}");
    assert!(current.error_annotation.is_none(), "{why}");
    assert_eq!(current.version, task.version, "{why}");
}

pub(super) fn failed_terminal_notification(
    execution: &Execution,
) -> api_types::ExecutionTerminalNotification {
    serde_json::from_value(json!({
        "terminal_report_id": new_uuid_v4(),
        "execution_id": execution.id,
        "exit_code": 1,
        "signal": null,
        "error": "executor failed",
        "ts": now_rfc3339(),
        "status": "failed",
        "usage_reports": [],
    }))
    .expect("notification parses")
}

pub(super) async fn seed_workspace_with_plan(
    db: &SqliteDb,
    task: &Task,
    repo_id: &str,
    plan: &str,
) -> Workspace {
    let workspace_dir = std::env::temp_dir()
        .join(format!("forge-guard-plan-{}", new_uuid_v4()))
        .join(&task.id);
    let worktree_path = workspace_dir.join("worktree");
    std::fs::create_dir_all(&worktree_path).expect("worktree creates");
    std::fs::write(workspace_dir.join("plan.md"), plan).expect("plan writes");
    git::init(&worktree_path).await.expect("git init succeeds");
    std::fs::write(worktree_path.join("README.md"), "# Test\n").expect("readme writes");
    git::commit_all(&worktree_path, "initial commit")
        .await
        .expect("initial commit creates");
    WorkspaceRepo::create(
        db,
        CreateWorkspace {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            repo_id: repo_id.to_owned(),
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch: ::workspace::task_branch_name(&task.id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("workspace creates")
}

pub(super) async fn seed_completed_coder_execution(
    db: &SqliteDb,
    task: &Task,
    agent_id: &str,
    workspace_id: Option<&str>,
) -> Execution {
    let now = now_rfc3339();
    let project_version = ProjectRepo::get_by_id(db, &task.project_id)
        .await
        .expect("project loads")
        .expect("project exists")
        .version;
    ExecutionRepo::create(
        db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.to_owned()),
            role: crate::workflow::default_roles::CODER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("implemented the change".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                json!({
                    "executor_type": "shell",
                    "config": {},
                    "project_version": project_version,
                })
                .to_string(),
            ),
            workspace_id: workspace_id.map(str::to_owned),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("execution creates")
}

pub(super) async fn workflow_execution_snapshot(db: &SqliteDb, project_id: &str) -> String {
    let project_version = ProjectRepo::get_by_id(db, project_id)
        .await
        .expect("project loads")
        .expect("project exists")
        .version;
    json!({
        "executor_type": "shell",
        "config": {},
        "project_version": project_version,
    })
    .to_string()
}

pub(super) struct StartedClaimExecutor(pub(super) Arc<tokio::sync::Notify>);
#[async_trait]
impl TaskExecutor for StartedClaimExecutor {
    async fn execute(
        &self,
        _context: ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutorError> {
        self.0.notify_one();
        std::future::pending().await
    }
    async fn cancel(&self, _id: &str) -> std::result::Result<(), ExecutorError> {
        Ok(())
    }
}

/// The one-time 2.4 backfill exactly as shipped: everything after the
/// column/purpose DDL.
pub(super) async fn run_shipped_bridge_backfill(db: &SqliteDb) {
    let migration = include_str!(
        "../../../../../../../db/migrations/V202610051343__typed_workflow_bridges.sql"
    );
    let start = migration.find("CREATE TEMP TABLE bridge_backfill").unwrap();
    sqlx::raw_sql(&migration[start..])
        .execute(db.pool())
        .await
        .unwrap();
}

pub(super) async fn pre_upgrade_cascade(
    base_reason: &str,
    expected_kind: Option<api_types::TransitionBridgeKind>,
    expected_payload: Option<serde_json::Value>,
) {
    let db = Arc::new(sqlite_db().await);
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()));
    let (project_id, _, _repo) = seed_project_repo(&db).await;
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    for state in &mut workflow.states {
        state.hooks = api_types::StateHooks::default();
    }
    sqlx::query("UPDATE project SET workflow_definition=?,version=version+1 WHERE id=?")
        .bind(serde_json::to_string(&workflow).unwrap())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .unwrap();
    let task = seed_task_with_status(&db, &project_id, "merging".into()).await;
    // A real producer shape, then reduced to the pre-2.4 payload: no typed
    // fields, marker prose in `reason`.
    let mut cascade = service
        .workflow_execution()
        .cascade_step_input(
            &task,
            &workflow,
            "merge_failed".into(),
            "placeholder".into(),
            Default::default(),
            false,
            false,
            None,
            None,
            db::new_uuid_v4(),
            None,
        )
        .await
        .unwrap();
    let mut payload: serde_json::Value = serde_json::from_str(&cascade.payload_json).unwrap();
    let object = payload.as_object_mut().unwrap();
    object.remove("bridge_kind");
    object.remove("bridge_payload");
    object.insert("reason".into(), serde_json::json!(base_reason));
    cascade.payload_json = payload.to_string();
    db::TaskStepRepo::enqueue_step(&*db, &cascade)
        .await
        .unwrap();
    run_shipped_bridge_backfill(&db).await;
    let migrated: String = sqlx::query_scalar("SELECT payload_json FROM task_step WHERE id=?")
        .bind(&cascade.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    let migrated: serde_json::Value = serde_json::from_str(&migrated).unwrap();
    assert_eq!(migrated["reason"], base_reason, "prose preserved");
    let task = service.drain(&task.id).await.unwrap();
    assert_eq!(task.status, "merge_failed");
    let entries = db::TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap();
    let committed = entries
        .iter()
        .rev()
        .find(|e| e.from_state == "merging" && e.to_state == "merge_failed")
        .expect("queued cascade committed after upgrade");
    assert_eq!(committed.trigger_reason, base_reason);
    assert_eq!(committed.bridge.bridge_kind, expected_kind, "{base_reason}");
    assert_eq!(
        committed.bridge.bridge_payload, expected_payload,
        "{base_reason}"
    );
    assert!(!committed.rejection);
}

/// A pre-2.4 binary committed `merging -> merge_failed` for a clean target
/// rebase (marker prose, no typed columns) and queued its hooks step, and,
/// with `checkpoint`, had already saved the dispatch hook's Cascade result in
/// the pre-2.4 shape. After the shipped backfill the replay must do what base
/// did: route to a fresh review without a merge-fix Worker, commit the refresh
/// as a non-rejection, and let review carry see a clean rebase.
pub(super) async fn pre_upgrade_hooks_step_replays(checkpoint: Option<&str>) -> db::TransitionLog {
    let db = Arc::new(sqlite_db().await);
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()));
    let (project_id, _, _repo) = seed_project_repo(&db).await;
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    // Only merge_failed's dispatch hook remains, at checkpoint index 0.
    for state in &mut workflow.states {
        let dispatch = state
            .hooks
            .on_enter
            .iter()
            .filter(|hook| {
                state.name == crate::workflow::default_states::MERGE_FAILED
                    && hook.action == "dispatch_role_agent"
            })
            .cloned()
            .collect();
        state.hooks = api_types::StateHooks {
            on_enter: dispatch,
            ..Default::default()
        };
    }
    sqlx::query("UPDATE project SET workflow_definition=?,version=version+1 WHERE id=?")
        .bind(serde_json::to_string(&workflow).unwrap())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .unwrap();
    let task = seed_task_with_status(&db, &project_id, "merging".into()).await;
    let base_reason =
        "[review-refresh] [target-moved-rebase] main advanced; rebased onto main, re-review required";
    // Produce the real committed row and queued hooks step, then reduce both
    // to what the pre-2.4 binary stored: no typed columns or payload fields.
    let committed = service
        .workflow_execution()
        .transition(
            &task.id,
            crate::workflow::default_states::MERGE_FAILED,
            task.version,
            &workflow,
            &crate::worker_runtime::queue::cascade_actor(),
            base_reason,
            false,
            api_types::TransitionBridge::new(api_types::TransitionBridgeKind::TargetMovedRebase),
        )
        .await
        .unwrap();
    assert_eq!(committed.pending_steps, 1);
    sqlx::query("UPDATE transition_log SET bridge_kind=NULL, bridge_payload=NULL WHERE task_id=?")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_step SET payload_json=json_remove(payload_json,'$.bridge_kind','$.bridge_payload')
         WHERE task_id=? AND kind='hooks'",
    )
    .bind(&task.id)
    .execute(db.pool())
    .await
    .unwrap();
    let step_id: String =
        sqlx::query_scalar("SELECT id FROM task_step WHERE task_id=? AND kind='hooks'")
            .bind(&task.id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    if let Some(reason) = checkpoint {
        sqlx::query("INSERT INTO task_hook_checkpoint(step_id,hook_index,started_at,result_json) VALUES(?,0,'now',?)")
            .bind(&step_id)
            .bind(serde_json::json!({"Cascade":{"to":"review","reason":reason}}).to_string())
            .execute(db.pool())
            .await
            .unwrap();
    }
    run_shipped_bridge_backfill(&db).await;
    let payload: String = sqlx::query_scalar("SELECT payload_json FROM task_step WHERE id=?")
        .bind(&step_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(payload["reason"], base_reason, "queued prose preserved");
    assert_eq!(payload["bridge_kind"], "target_moved_rebase");

    let settled = service.drain(&task.id).await.unwrap();
    assert_eq!(settled.status, crate::workflow::default_states::REVIEW);
    let executions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution WHERE task_id=?")
        .bind(&task.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(
        executions, 0,
        "a review refresh never dispatches a merge-fix Worker"
    );
    let entries = db::TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap();
    let [.., bridge, refresh] = entries.as_slice() else {
        panic!("bridge and refresh rows: {entries:?}");
    };
    assert_eq!(bridge.trigger_reason, base_reason);
    assert_eq!(
        bridge.bridge.bridge_kind,
        Some(api_types::TransitionBridgeKind::TargetMovedRebase)
    );
    assert_eq!(
        (refresh.from_state.as_str(), refresh.to_state.as_str()),
        ("merge_failed", "review")
    );
    assert_eq!(
        refresh.triggered_by,
        crate::worker_runtime::queue::cascade_actor().display()
    );
    assert!(!refresh.rejection);
    assert_eq!(
        refresh.bridge.bridge_kind,
        Some(api_types::TransitionBridgeKind::ReviewRefresh)
    );
    assert_eq!(
        crate::workflow::review_carry_entry_kind(
            &entries,
            &crate::workflow::default_workflow::default_workflow()
        ),
        Some(db::ReviewCarryKind::CleanRebase)
    );
    refresh.clone()
}

pub(super) async fn seed_budget_spent(
    db: &SqliteDb,
    task: &str,
    kind: db::budget::Kind,
    spent: i64,
) {
    sqlx::query("INSERT INTO task_budget(task_id,kind,window_id,spent) VALUES(?,?,'fixture',?) ON CONFLICT(task_id,kind) DO UPDATE SET spent=excluded.spent,window_id=excluded.window_id").bind(task).bind(kind.key()).bind(spent).execute(db.pool()).await.unwrap();
}

pub(super) async fn review_comment_contents(db: &SqliteDb, task_id: &str) -> Vec<String> {
    TaskCommentRepo::list_comments(
        db,
        task_id,
        PageRequest {
            cursor: None,
            limit: 100,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Asc,
            include_total: false,
        },
    )
    .await
    .unwrap()
    .items
    .into_iter()
    .map(|comment| comment.content)
    .collect()
}

pub(super) async fn structured_round_previous(db: &SqliteDb, task_id: &str, count: usize) {
    seed_previous_review_attempt(db, task_id, ReviewStatus::Failed).await;
    let findings: Vec<_> = (1..=count)
        .map(|i| json!({"summary":format!("issue {i}")}))
        .collect();
    sqlx::query("UPDATE review SET step_results_json=? WHERE task_id=? AND attempt_number=1")
        .bind(json!({"conformance":{"status":"failed","checks":[],"reason":"previous finding","assessment":{"result":"fail","reason":"previous finding","findings":findings}}}).to_string())
        .bind(task_id).execute(db.pool()).await.unwrap();
}

pub(super) async fn round_spending(db: &SqliteDb, task_id: &str) -> (i64, i64) {
    (
        db::budget::spent(db.pool(), task_id, "review")
            .await
            .unwrap(),
        db::budget::spent(db.pool(), task_id, "review_progress")
            .await
            .unwrap(),
    )
}
