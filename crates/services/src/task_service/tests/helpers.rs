use super::*;

use db::{
    create_sqlite_pool, run_migrations, AgentRepo, AgentStatus, CreateAgent, CreateExecution,
    CreateProject, CreateRepo, CreateReview, CreateTask, CreateTaskRoleAssignment,
    CreateTransitionLog, DaemonRepo, DaemonStatus, ExecutionRepo, ProjectRepo, RepoRepo,
    ReviewRepo, TaskRepo, TaskRoleAssignmentRepo, TransitionLogRepo, UpdateProject, UpsertDaemon,
};
use tempfile::TempDir;

pub(super) async fn sqlite_db() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    SqliteDb::new(pool)
}

pub(super) async fn seed_project_repo(db: &SqliteDb) -> (String, String, TempDir) {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let repo_dir = TempDir::new().expect("repo temp dir creates");
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
            name: "forge".to_owned(),
            remote_url: repo_dir.path().to_string_lossy().into_owned(),
            local_path: Some(repo_dir.path().to_string_lossy().into_owned()),
            work_mode: db::WorkMode::DirectMerge,
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
    (project_id, repo_id, repo_dir)
}

pub(super) async fn seed_agent(db: &SqliteDb) -> String {
    let now = now_rfc3339();
    let daemon_id = new_uuid_v4();
    DaemonRepo::upsert_by_machine_id(
        db,
        UpsertDaemon {
            id: daemon_id.clone(),
            machine_id: format!("machine-{daemon_id}"),
            hostname: "test-host".to_owned(),
            os: "linux".to_owned(),
            arch: "x86_64".to_owned(),
            agent_version: None,
            labels_json: "{}".to_owned(),
            status: DaemonStatus::Online,
            registration_token_hash: None,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("daemon creates");
    DaemonRepo::update_report(
        db,
        db::UpdateDaemonReport {
            id: daemon_id.clone(),
            detected_clis_json: r#"[{"kind":"shell","availability":"authenticated"}]"#.to_owned(),
            labels_json: None,
            status: DaemonStatus::Online,
            last_report_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("daemon report updates");

    let agent_id = new_uuid_v4();
    AgentRepo::create(
        db,
        CreateAgent {
            id: agent_id.clone(),
            name: "shell".to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: r#"["rust"]"#.to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(daemon_id),
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("agent creates");
    agent_id
}

pub(super) async fn seed_task_with_status(
    db: &SqliteDb,
    project_id: &str,
    repo_id: &str,
    status: &str,
) -> Task {
    seed_task_with_status_at(db, project_id, repo_id, status, &now_rfc3339()).await
}

pub(super) async fn seed_task_with_status_at(
    db: &SqliteDb,
    project_id: &str,
    repo_id: &str,
    status: &str,
    timestamp: &str,
) -> Task {
    TaskRepo::create(
        db,
        CreateTask {
            id: new_uuid_v4(),
            project_id: project_id.to_owned(),
            repo_id: Some(repo_id.to_owned()),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: format!("task-{status}"),
            description: Some("seeded task".to_owned()),
            task_type: "task".to_owned(),
            status: status.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: timestamp.to_owned(),
            updated_at: timestamp.to_owned(),
        },
    )
    .await
    .expect("task creates")
}

pub(super) async fn seed_role_assignment(
    db: &SqliteDb,
    task_id: &str,
    role_name: &str,
    agent_id: Option<&str>,
) {
    let now = now_rfc3339();
    TaskRoleAssignmentRepo::assign(
        db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            role_name: role_name.to_owned(),
            assignee_type: agent_id.map(|_| db::AssigneeKind::Agent),
            assignee_id: agent_id.map(str::to_owned),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("role assignment creates");
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn seed_execution(
    db: &SqliteDb,
    task_id: &str,
    agent_id: Option<&str>,
    role: &str,
    status: ExecutionStatus,
    session_id: Option<&str>,
    created_at: &str,
) -> Execution {
    ExecutionRepo::create(
        db,
        CreateExecution {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            agent_id: agent_id.map(str::to_owned),
            role: role.to_owned(),
            status,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: session_id.map(str::to_owned),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some(format!("{role} execution")),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: created_at.to_owned(),
            updated_at: created_at.to_owned(),
        },
    )
    .await
    .expect("execution creates")
}

pub(super) async fn seed_failed_review(
    db: &SqliteDb,
    task_id: &str,
    execution_id: &str,
    attempt_number: i64,
    step_results_json: serde_json::Value,
) -> Review {
    let now = now_rfc3339();
    ReviewRepo::create(
        db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            execution_id: execution_id.to_owned(),
            attempt_number,
            status: ReviewStatus::Failed,
            step_results_json: step_results_json.to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("failed review creates")
}

pub(super) async fn seed_passed_review(
    db: &SqliteDb,
    task_id: &str,
    execution_id: &str,
    attempt_number: i64,
) -> Review {
    let now = now_rfc3339();
    ReviewRepo::create(
        db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            execution_id: execution_id.to_owned(),
            attempt_number,
            status: ReviewStatus::Passed,
            step_results_json: "[]".to_owned(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("passed review creates")
}

pub(super) async fn seed_review_rejection_log(
    db: &SqliteDb,
    task_id: &str,
    reason: &str,
) -> String {
    let log = TransitionLogRepo::insert(
        db,
        CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            from_state: crate::workflow::default_states::REVIEW.to_owned(),
            to_state: crate::workflow::default_states::IN_PROGRESS.to_owned(),
            trigger_name: Some("reject".to_owned()),
            triggered_by: api_types::Actor::system(api_types::SystemComponent::Test).display(),
            trigger_reason: reason.to_owned(),
            hook_results_json: None,
            rejection: true,
            created_at: now_rfc3339(),
        },
    )
    .await
    .expect("transition log creates");
    log.id
}

pub(super) async fn set_retry_exhausted_metadata(db: &SqliteDb, task: &Task) -> Task {
    let annotation = api_types::TaskAnnotation::Blocking(api_types::TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::ReviewBudgetExhausted,
        blocking_reason: "review retry budget exhausted".to_owned(),
        blocked_by: Some("system".to_owned()),
        blocked_at: Some(now_rfc3339()),
        blocked_execution_id: None,
        artifact: None,
        message: Some("review retry budget exhausted".to_owned()),
        hook: None,
        recovery_actions: vec![
            api_types::RecoveryAction::ResetRetryWindow,
            api_types::RecoveryAction::ProceedOnce,
            api_types::RecoveryAction::CancelTask,
        ],
    });
    TaskRepo::update(
        db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(
                serde_json::to_string(&annotation).expect("annotation serializes"),
            )),
            blocked_json: Some(Some(
                json!({
                    "reason": "review retry budget exhausted",
                    "created_at": now_rfc3339(),
                    "kind": "review_gate_failed"
                })
                .to_string(),
            )),
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("retry-exhausted metadata sets")
}
