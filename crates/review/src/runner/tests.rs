use super::*;
use async_trait::async_trait;
use db::{
    create_sqlite_pool, run_migrations, AgentRepo, AgentStatus, CreateAgent, CreateProject,
    CreateRepo, CreateTask, CreateWorkspace, DaemonRepo, DaemonStatus, ProjectRepo, RepoRepo,
    TaskRepo, UpdateProject, UpsertDaemon, WorkspaceRepo, WorkspaceStatus,
};
use serde_json::{json, Value};
use std::path::Path;
use tempfile::TempDir;

struct SeededReview {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
    task_id: Uuid,
    executor_execution_id: Uuid,
    auditor_agent_id: String,
    workspace: TempDir,
    logs_path: String,
}

async fn seeded_review(ci_steps: Vec<&str>) -> SeededReview {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let event_bus = Arc::new(EventBus::with_default_capacity());
    let workspace = tempfile::tempdir().expect("workspace creates");
    let logs_path = workspace.path().join("review.jsonl").display().to_string();

    let now = now_rfc3339();
    let daemon_id = Uuid::new_v4().to_string();
    let project_id = Uuid::new_v4().to_string();
    let repo_id = Uuid::new_v4().to_string();
    let agent_id = Uuid::new_v4().to_string();
    let task_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4().to_string();
    let executor_execution_id = Uuid::new_v4();

    DaemonRepo::upsert_by_machine_id(
        &*db,
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

    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.clone(),
            name: "Forge".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_string(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");

    RepoRepo::create(
        &*db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "forge".to_owned(),
            remote_url: "https://example.com/forge.git".to_owned(),
            local_path: None,
            work_mode: db::WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("project primary repo updates");

    AgentRepo::create(
        &*db,
        CreateAgent {
            id: agent_id.clone(),
            name: "shell".to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
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
            updated_at: now.clone(),
        },
    )
    .await
    .expect("agent creates");

    let task_state_config = serde_json::to_string(&json!({
        "review": {
            "ci_steps": ci_steps,
        },
    }))
    .expect("task state config serializes");

    TaskRepo::create(
        &*db,
        CreateTask {
            id: task_id.to_string(),
            project_id,
            repo_id: Some(repo_id.clone()),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Review me".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "in_progress".to_string(),
            is_automation: false,
            priority: 0,
            task_state_config: Some(task_state_config),
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task creates");

    WorkspaceRepo::create(
        &*db,
        CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task_id.to_string(),
            repo_id,
            worktree_path: workspace.path().display().to_string(),
            branch: format!("forge/{task_id}"),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");

    ExecutionRepo::create(
        &*db,
        CreateExecution {
            id: executor_execution_id.to_string(),
            task_id: task_id.to_string(),
            agent_id: Some(agent_id.clone()),
            role: "executor".to_owned(),
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
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace_id),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("executor execution creates");

    SeededReview {
        db,
        event_bus,
        task_id,
        executor_execution_id,
        auditor_agent_id: agent_id,
        workspace,
        logs_path,
    }
}

fn request(seed: &SeededReview) -> ReviewRequest {
    ReviewRequest {
        task_id: seed.task_id,
        executor_execution_id: seed.executor_execution_id,
        workspace_path: seed.workspace.path().to_path_buf(),
        ci_steps: Vec::new(),
        logs_path: seed.logs_path.clone(),
        auditor_agent_id: None,
        review_prompt: None,
        executor_thread_id: None,
    }
}

#[tokio::test]
async fn reviewer_execution_is_owner_bound_at_creation() {
    let seed = seeded_review(Vec::new()).await;
    let runner = ReviewRunner::new(
        Arc::clone(&seed.db),
        Arc::clone(&seed.event_bus),
        Arc::new(AdapterRegistry::new()),
    );
    let workspace = WorkspaceRepo::get_by_task_id(&*seed.db, &seed.task_id.to_string())
        .await
        .expect("workspace lookup succeeds")
        .expect("seed workspace exists");

    let (execution, owner) = runner
        .create_reviewer_execution(
            &seed.task_id.to_string(),
            &seed.executor_execution_id.to_string(),
            workspace.id,
            &request(&seed),
        )
        .await
        .expect("reviewer execution creates with lease");

    assert_eq!(execution.status, ExecutionStatus::Running);
    assert_eq!(execution.lease_owner.as_deref(), Some(owner.as_str()));
    assert!(execution.lease_expires_at.is_some());
    assert!(execution.hard_deadline_at.is_some());
}

#[test]
fn review_hard_deadline_uses_timeout_terminal_policy() {
    let policy = review_terminal_policy(&ReviewError::ExecutionHardDeadline {
        execution_id: "review-execution".to_owned(),
    });

    assert_eq!(policy.stop_reason, Some(db::StopReason::AgentTimeout));
    assert_eq!(
        policy.stopped_by.as_deref(),
        Some("system:heartbeat_monitor")
    );
    assert_eq!(policy.resume_policy, Some(db::ResumePolicy::Manual));
}

struct MutatingAuditor;

#[async_trait]
impl TaskExecutor for MutatingAuditor {
    async fn execute(
        &self,
        ctx: ExecutionContext,
    ) -> Result<executors::ExecutionResult, executors::ExecutorError> {
        let worktree = Path::new(&ctx.worktree_path);
        tokio::fs::write(worktree.join("reviewer-created.txt"), "must be discarded\n").await?;
        let committed_sha = git::commit_all(worktree, "reviewer mutation")
            .await
            .map_err(|error| executors::ExecutorError::Other(error.to_string()))?;

        let mut writer = LogWriter::new(&ctx.logs_path, ctx.execution_id, MAX_LOG_BYTES);
        writer
            .write(
                LogKind::Assistant,
                LogStream::Main,
                json!({ "text": "No issues.\n===REVIEW: PASS===" }),
            )
            .await?;

        Ok(executors::ExecutionResult {
            status: ExecutionOutcome::Completed,
            after_sha: Some(committed_sha),
            agent_session_id: Some("auditor-session".to_owned()),
            summary: Some("review passed".to_owned()),
            ..Default::default()
        })
    }

    async fn cancel(&self, _execution_id: &str) -> Result<(), executors::ExecutorError> {
        Ok(())
    }
}

#[tokio::test]
async fn auditor_worktree_mutations_are_discarded_before_review_completes() {
    let seed = seeded_review(Vec::new()).await;
    git::init(seed.workspace.path()).await.unwrap();
    tokio::fs::write(seed.workspace.path().join("baseline.txt"), "baseline\n")
        .await
        .unwrap();
    let original_sha = git::commit_all(seed.workspace.path(), "baseline")
        .await
        .unwrap();
    let logs = tempfile::tempdir().expect("logs tempdir creates");
    let mut req = request(&seed);
    req.logs_path = logs.path().join("review.jsonl").display().to_string();
    req.auditor_agent_id = Some(seed.auditor_agent_id.clone());
    let runner = ReviewRunner::new_for_tests(
        Arc::clone(&seed.db),
        Arc::clone(&seed.event_bus),
        Arc::new(MutatingAuditor),
    );

    let (_review, outcome) = runner.run(req).await.unwrap();

    assert_eq!(outcome, ReviewOutcome::Passed);
    assert_eq!(
        git::get_current_sha(seed.workspace.path()).await.unwrap(),
        original_sha
    );
    assert!(!seed.workspace.path().join("reviewer-created.txt").exists());
    assert!(git::is_worktree_clean(seed.workspace.path()).await.unwrap());
}

async fn write_jsonl_log(path: &Path, entries: Vec<(LogKind, Value)>) {
    let mut lines = Vec::new();
    for (sequence, (kind, payload)) in entries.into_iter().enumerate() {
        let entry = LogEntry {
            schema_version: 1,
            sequence: sequence as u64,
            timestamp: "2026-04-15T00:00:00Z".to_owned(),
            execution_id: "auditor-exec".to_owned(),
            kind,
            stream: LogStream::Main,
            payload,
            truncated: false,
        };
        lines.push(serde_json::to_string(&entry).expect("entry serializes"));
    }
    tokio::fs::write(path, format!("{}\n", lines.join("\n")))
        .await
        .expect("log writes");
}

#[tokio::test]
async fn assistant_entries_are_concatenated_for_verdict_text() {
    let tempdir = tempfile::tempdir().expect("tempdir creates");
    let logs_path = tempdir.path().join("auditor.jsonl");
    write_jsonl_log(
        &logs_path,
        vec![
            (LogKind::Assistant, json!({ "text": "Verifying...\n" })),
            (
                LogKind::Assistant,
                json!({ "text": "All clear.\n===REVIEW: PASS===" }),
            ),
        ],
    )
    .await;

    let message = last_assistant_message(logs_path.to_str().unwrap())
        .await
        .unwrap();

    assert_eq!(message, "Verifying...\nAll clear.\n===REVIEW: PASS===");
}

#[tokio::test]
async fn shell_auditor_stdout_marker_parses_as_passed() {
    // A shell auditor has no assistant channel. Its verdict marker arrives as
    // ordinary stdout, and reading only assistant text made every shell
    // auditor fail as "verdict marker missing" while its own log contained
    // the marker.
    let tempdir = tempfile::tempdir().expect("tempdir creates");
    let logs_path = tempdir.path().join("auditor.jsonl");
    write_jsonl_log(
        &logs_path,
        vec![
            (LogKind::Stdout, json!({ "line": "checked index.html" })),
            (LogKind::Stdout, json!({ "line": "===REVIEW: PASS===" })),
        ],
    )
    .await;

    let message = last_assistant_message(logs_path.to_str().unwrap())
        .await
        .unwrap();

    assert_eq!(auditor::parse_verdict(&message), AuditorVerdict::Passed);
}

#[tokio::test]
async fn shell_auditor_stdout_fail_marker_keeps_its_reason() {
    let tempdir = tempfile::tempdir().expect("tempdir creates");
    let logs_path = tempdir.path().join("auditor.jsonl");
    write_jsonl_log(
        &logs_path,
        vec![(
            LogKind::Stdout,
            json!({ "line": "===REVIEW: FAIL: no localStorage persistence===" }),
        )],
    )
    .await;

    let message = last_assistant_message(logs_path.to_str().unwrap())
        .await
        .unwrap();

    assert_eq!(
        auditor::parse_verdict(&message),
        AuditorVerdict::Failed {
            reason: "no localStorage persistence".to_owned()
        }
    );
}

#[tokio::test]
async fn stdout_lines_stay_separated_so_a_marker_cannot_be_glued_together() {
    // Two stdout lines must not concatenate into a marker neither one printed.
    let tempdir = tempfile::tempdir().expect("tempdir creates");
    let logs_path = tempdir.path().join("auditor.jsonl");
    write_jsonl_log(
        &logs_path,
        vec![
            (LogKind::Stdout, json!({ "line": "===REVIEW: P" })),
            (LogKind::Stdout, json!({ "line": "ASS===" })),
        ],
    )
    .await;

    let message = last_assistant_message(logs_path.to_str().unwrap())
        .await
        .unwrap();

    assert_eq!(
        auditor::parse_verdict(&message),
        AuditorVerdict::Failed {
            reason: "verdict marker missing".to_owned()
        }
    );
}

#[tokio::test]
async fn assistant_entry_with_pass_marker_parses_as_passed() {
    let tempdir = tempfile::tempdir().expect("tempdir creates");
    let logs_path = tempdir.path().join("auditor.jsonl");
    write_jsonl_log(
        &logs_path,
        vec![(LogKind::Assistant, json!({ "text": "===REVIEW: PASS===" }))],
    )
    .await;

    let message = last_assistant_message(logs_path.to_str().unwrap())
        .await
        .unwrap();

    assert_eq!(auditor::parse_verdict(&message), AuditorVerdict::Passed);
}

#[tokio::test]
async fn claude_assistant_message_content_parses_as_passed() {
    let tempdir = tempfile::tempdir().expect("tempdir creates");
    let logs_path = tempdir.path().join("auditor.jsonl");
    write_jsonl_log(
        &logs_path,
        vec![(
            LogKind::Assistant,
            json!({
                "message": {
                    "content": [{
                        "type": "text",
                        "text": "No issues found.\n===REVIEW: PASS==="
                    }]
                }
            }),
        )],
    )
    .await;

    let message = last_assistant_message(logs_path.to_str().unwrap())
        .await
        .unwrap();

    assert_eq!(auditor::parse_verdict(&message), AuditorVerdict::Passed);
}

#[tokio::test]
async fn claude_success_result_parses_as_passed() {
    let tempdir = tempfile::tempdir().expect("tempdir creates");
    let logs_path = tempdir.path().join("auditor.jsonl");
    write_jsonl_log(
        &logs_path,
        vec![(
            LogKind::SessionInfo,
            json!({
                "subtype": "success",
                "result": "No issues found.\n===REVIEW: PASS==="
            }),
        )],
    )
    .await;

    let message = last_assistant_message(logs_path.to_str().unwrap())
        .await
        .unwrap();

    assert_eq!(auditor::parse_verdict(&message), AuditorVerdict::Passed);
}

#[tokio::test]
async fn assistant_delta_entries_alone_do_not_count_for_verdict_text() {
    let tempdir = tempfile::tempdir().expect("tempdir creates");
    let logs_path = tempdir.path().join("auditor.jsonl");
    write_jsonl_log(
        &logs_path,
        vec![
            (LogKind::AssistantDelta, json!({ "delta": "===" })),
            (LogKind::AssistantDelta, json!({ "delta": "REVIEW: PASS" })),
            (LogKind::AssistantDelta, json!({ "delta": "===" })),
        ],
    )
    .await;

    let message = last_assistant_message(logs_path.to_str().unwrap())
        .await
        .unwrap();

    assert_eq!(
        auditor::parse_verdict(&message),
        AuditorVerdict::Failed {
            reason: "verdict marker missing".to_owned()
        }
    );
}

#[tokio::test]
async fn codex_auditor_snapshot_carries_resume_thread_hint_for_codex_executor() {
    let now = now_rfc3339();
    let auditor_agent = Agent {
        id: "auditor-agent".to_owned(),
        name: "auditor".to_owned(),
        description: None,
        profile_id: "auditor-agent-profile".to_owned(),
        backend_kind: "cli".to_owned(),
        executor_type: "codex".to_owned(),
        provider: None,
        model: None,
        reasoning_effort: None,
        permission_policy: None,
        prompt_template: None,
        capabilities_json: "[]".to_owned(),
        tool_policy_json: "{}".to_owned(),
        config_json: "{}".to_owned(),
        credential_ref: None,
        daemon_id: Some("daemon".to_owned()),
        max_concurrent_tasks: 1,
        heartbeat_interval_seconds: 30,
        max_missed_heartbeats: 3,
        status: AgentStatus::Idle,
        last_heartbeat_at: None,
        is_default: false,
        paused: false,
        owner_id: None,
        visibility: "global".to_owned(),
        version: 1,
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    let executor_execution = Execution {
        id: "executor-exec".to_owned(),
        task_id: "task".to_owned(),
        agent_id: Some("executor-agent".to_owned()),
        role: "executor".to_owned(),
        status: ExecutionStatus::Completed,
        stop_reason: None,
        stopped_by: None,
        resume_policy: None,
        stopped_at: None,
        parent_execution_id: None,
        agent_session_id: Some("thread-123".to_owned()),
        agent_message_id: None,
        last_activity_at: None,
        prompt: None,
        summary: None,
        logs_path: None,
        before_sha: None,
        after_sha: None,
        error: None,
        executor_config_snapshot_json: None,
        workspace_id: Some("workspace".to_owned()),
        execution_version: 1,
        lease_owner: None,
        lease_expires_at: None,
        hard_deadline_at: None,
        last_heartbeat_at: None,
        last_progress_at: None,
        created_at: now.clone(),
        updated_at: now,
    };

    let extra_config =
        auditor_resume_thread_extra_config(&executor_execution, Some("codex"), &auditor_agent);
    let snapshot = build_auditor_config_snapshot(&auditor_agent, extra_config)
        .await
        .expect("snapshot builds");
    let snapshot: Value = serde_json::from_str(&snapshot).expect("snapshot parses");

    assert_eq!(
        snapshot["config"][RESUME_THREAD_ID_CONFIG_KEY],
        json!("thread-123")
    );
}

#[tokio::test]
async fn empty_steps_auto_passes() {
    let seed = seeded_review(vec![]).await;
    let runner = ReviewRunner::new(
        seed.db.clone(),
        seed.event_bus.clone(),
        Arc::new(AdapterRegistry::new()),
    );

    let (review, outcome) = runner.run(request(&seed)).await.unwrap();

    assert_eq!(outcome, ReviewOutcome::Passed);
    assert_eq!(review.status, ReviewStatus::Passed);
    assert_eq!(review.step_results_json, "[]");
    assert!(review.finished_at.is_some());
}

#[tokio::test]
async fn passing_step_records_pass() {
    let seed = seeded_review(vec!["true"]).await;
    let runner = ReviewRunner::new(
        seed.db.clone(),
        seed.event_bus.clone(),
        Arc::new(AdapterRegistry::new()),
    );

    let (review, outcome) = runner.run(request(&seed)).await.unwrap();

    assert_eq!(outcome, ReviewOutcome::Passed);
    assert_eq!(review.status, ReviewStatus::Passed);
    let results: Vec<Value> = serde_json::from_str(&review.step_results_json).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["index"], 0);
    assert_eq!(results[0]["exit_code"], 0);
}

#[tokio::test]
async fn failing_step_records_fail() {
    let seed = seeded_review(vec!["true", "false", "echo never"]).await;
    let runner = ReviewRunner::new(
        seed.db.clone(),
        seed.event_bus.clone(),
        Arc::new(AdapterRegistry::new()),
    );

    let (review, outcome) = runner.run(request(&seed)).await.unwrap();

    assert!(matches!(
        outcome,
        ReviewOutcome::CiFailed {
            ref failing_steps
        } if failing_steps.len() == 1 && failing_steps[0].index == 1
    ));
    assert_eq!(review.status, ReviewStatus::Failed);
    let results: Vec<Value> = serde_json::from_str(&review.step_results_json).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[1]["index"], 1);
    assert_ne!(results[1]["exit_code"], 0);
    assert!(!review.step_results_json.contains("echo never"));
}

#[tokio::test]
async fn attempt_numbers_increment() {
    let seed = seeded_review(vec![]).await;
    let runner = ReviewRunner::new(
        seed.db.clone(),
        seed.event_bus.clone(),
        Arc::new(AdapterRegistry::new()),
    );

    let (first, first_outcome) = runner.run(request(&seed)).await.unwrap();
    let (second, second_outcome) = runner.run(request(&seed)).await.unwrap();

    assert_eq!(first_outcome, ReviewOutcome::Passed);
    assert_eq!(second_outcome, ReviewOutcome::PassedCiOnly);
    assert_eq!(first.attempt_number, 1);
    assert_eq!(second.attempt_number, 2);
}
