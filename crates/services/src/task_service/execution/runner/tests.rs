//! Execution role, owner preparation, heartbeat, and terminal settlement tests.

use super::*;
use db::{
    AgentRepo, AgentStatus, CreateAgent, CreateExecution, CreateProject, CreateRepo, CreateTask,
    ExecutionRepo, ProjectRepo, RepoRepo, TaskRepo,
};

const T0: &str = "2025-01-01T00:00:00+00:00";
const T1: &str = "2025-01-01T00:00:01+00:00";
const T20: &str = "2025-01-01T00:00:20+00:00";

/// The one set of roles that never get worktree write access: the runner
/// (launch config, discarded-changes message) and the native Task session
/// both call `is_read_only_task_role`, so this table is the contract.
#[test]
fn read_only_roles_are_the_reviewer_and_the_planner() {
    for (role, read_only) in [
        (crate::workflow::default_roles::PLANNER, true),
        (crate::workflow::default_roles::REVIEWER, true),
        (crate::workflow::default_roles::CODER, false),
        ("worker", false),
        ("executor", false),
        ("auditor", false),
        ("interactive", false),
        ("system", false),
        ("custom", false),
        ("Planner", false),
        ("", false),
    ] {
        assert_eq!(is_read_only_task_role(role), read_only, "{role:?}");
    }
}

/// No second definition of the set: every use in the services crate goes
/// through the one function, and the role names appear as a read-only
/// pair nowhere else.
#[test]
fn read_only_role_set_has_one_definition() {
    fn sources(dir: &std::path::Path, out: &mut Vec<(std::path::PathBuf, String)>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                sources(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let text = std::fs::read_to_string(&path).unwrap();
                out.push((path, text));
            }
        }
    }
    let mut files = Vec::new();
    sources(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut files,
    );
    let definitions: Vec<_> = files
        .iter()
        .filter(|(_, text)| {
            text.lines().any(|line| {
                let line = line.trim_start();
                (line.starts_with("fn ") || line.contains(" fn "))
                    && line.contains("read_only")
                    && line.contains("role")
                    && line.contains("-> bool")
            })
        })
        .map(|(path, _)| path.clone())
        .collect();
    assert_eq!(
        definitions.len(),
        1,
        "one read-only role predicate, found in {definitions:?}"
    );
    assert!(definitions[0].ends_with("embedded_task_executor.rs"));
}

#[test]
fn placement_path_uses_fenced_owner_preparation_result() {
    let placement = db::WorkspacePlacement {
        id: "placement".to_owned(),
        workspace_id: "workspace".to_owned(),
        task_id: "task".to_owned(),
        agent_id: None,
        owner_kind: db::PlacementOwnerKind::Daemon,
        daemon_id: Some("daemon".to_owned()),
        runtime_id: Some("runtime".to_owned()),
        repo_location_id: "location".to_owned(),
        execution_daemon_id: Some("daemon".to_owned()),
        workspace_handle: Some("opaque-handle".to_owned()),
        generation: 3,
        state: db::PlacementState::Ready,
        selected_by: db::PlacementSelectedBy::Scheduler,
        selection_reason: "{}".to_owned(),
        reserved_until: None,
        disconnected_at: None,
        failure_cause: None,
        version: 4,
        created_at: T0.to_owned(),
        updated_at: T0.to_owned(),
    };
    let mut state = json!({
        "workspace_handle": "opaque-handle", "generation": 3,
        "workspace_path": "/owner/worktrees/task", "base_sha": "base", "branch": "task",
    });
    assert_eq!(
        prepared_owner_execution_path(&placement, &state).unwrap(),
        "/owner/worktrees/task",
    );
    state["generation"] = json!(2);
    assert!(prepared_owner_execution_path(&placement, &state).is_err());
    state["generation"] = json!(3);
    state["workspace_handle"] = json!("another-handle");
    assert!(prepared_owner_execution_path(&placement, &state).is_err());
    state["workspace_handle"] = json!("opaque-handle");
    state["workspace_path"] = json!("");
    assert!(prepared_owner_execution_path(&placement, &state).is_err());
}

#[test]
fn shell_coder_runs_task_command_not_workflow_role_prompt() {
    let task: Task = serde_json::from_value(json!({
        "id": "task", "project_id": "project", "parent_task_id": null,
        "assignee_type": null, "assignee_id": null, "title": "title",
        "description": "printf ok", "task_type": "task", "status": "in_progress",
        "is_automation": false, "priority": 0, "board_position": 0.0,
        "subtask_order": null, "task_state_config": null, "merge_config": null,
        "metadata_json": null, "plan": null, "error_annotation": null,
        "blocked_json": null, "failed_json": null, "entry_barrier_json": null,
        "review_passed_at": null, "archived_at": null, "deleted_at": null,
        "version": 1, "created_at": T0, "updated_at": T0,
    }))
    .unwrap();
    let role_prompt = Some("Forge role contract (authoritative): ...");
    assert_eq!(
        execution_description(
            "coder",
            role_prompt,
            &task,
            &json!({"executor_type": "shell"})
        ),
        "printf ok",
    );
    assert_eq!(
        execution_description(
            "coder",
            role_prompt,
            &task,
            &json!({"executor_type": "codex"})
        ),
        "Forge role contract (authoritative): ...",
    );
}

async fn heartbeat_fixture() -> (
    Arc<db::SqliteDb>,
    String,
    Arc<crate::task_service::execution::EmbeddedExecutionLease>,
) {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("in-memory sqlite pool");
    db::run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(db::SqliteDb::new(pool));
    let project_id = db::new_uuid_v4();
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.clone(),
            name: "embedded heartbeat test".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: T0.to_owned(),
            updated_at: T0.to_owned(),
        },
    )
    .await
    .expect("project creates");
    let repo_id = db::new_uuid_v4();
    RepoRepo::create(
        &*db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "heartbeat-test".to_owned(),
            remote_url: Some("https://example.invalid/heartbeat.git".to_owned()),
            local_path: None,
            default_branch: "main".to_owned(),
            created_at: T0.to_owned(),
            updated_at: T0.to_owned(),
        },
    )
    .await
    .expect("repo creates");
    let agent_id = db::new_uuid_v4();
    AgentRepo::create(
        &*db,
        CreateAgent {
            id: agent_id.clone(),
            name: "heartbeat-agent".to_owned(),
            description: None,
            executor_type: "embedded".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 20,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: T0.to_owned(),
            updated_at: T0.to_owned(),
        },
    )
    .await
    .expect("agent creates");
    let task_id = db::new_uuid_v4();
    TaskRepo::create(
        &*db,
        CreateTask {
            id: task_id.clone(),
            project_id,
            parent_task_id: None,
            assignee_type: Some("agent".to_owned()),
            assignee_id: Some(agent_id.clone()),
            title: "heartbeat test".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "in_progress".to_owned(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: T0.to_owned(),
            updated_at: T0.to_owned(),
        },
    )
    .await
    .expect("task creates");
    let execution_id = db::new_uuid_v4();
    ExecutionRepo::create(
        &*db,
        CreateExecution {
            id: execution_id.clone(),
            task_id,
            agent_id: Some(agent_id),
            role: "executor".to_owned(),
            status: db::ExecutionStatus::Running,
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
            created_at: T0.to_owned(),
            updated_at: T0.to_owned(),
        },
    )
    .await
    .expect("execution creates");
    let claimed = match ExecutionRepo::claim_lease(
        &*db,
        db::ClaimExecutionLease {
            execution_id: execution_id.clone(),
            expected_version: 1,
            owner: "embedded-heartbeat-test".to_owned(),
            lease_expires_at: "2025-01-01T00:00:05+00:00".to_owned(),
            hard_deadline_at: Some(T20.to_owned()),
            now: T0.to_owned(),
        },
    )
    .await
    .expect("execution claims")
    {
        db::ExecutionLeaseMutation::Updated(execution) => execution,
        other => panic!("claim must succeed, got {other:?}"),
    };
    let lease = Arc::new(crate::task_service::execution::EmbeddedExecutionLease::new(
        Arc::clone(&db),
        execution_id.clone(),
        claimed.lease_owner.expect("owner persisted"),
        claimed.execution_version,
    ));
    (db, execution_id, lease)
}

/// Wait for a heartbeat renewal to be durable instead of assuming a fixed
/// number of scheduler yields is enough for the write to land. The whole
/// budget is only ever spent on a failure.
async fn wait_for_renewed_heartbeat(
    db: &db::SqliteDb,
    execution_id: &str,
    expected: &str,
) -> db::Execution {
    for _ in 0..200 {
        let execution = ExecutionRepo::get_by_id(db, execution_id)
            .await
            .expect("execution reads")
            .expect("execution exists");
        if execution.last_heartbeat_at.as_deref() == Some(expected) {
            return execution;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("heartbeat never renewed to {expected}");
}

#[tokio::test(start_paused = true)]
async fn embedded_heartbeat_renews_without_semantic_progress_then_stops_at_deadline() {
    // Renewal writes to the database, and sqlx's pool acquire is a Tokio
    // timer: under a paused scheduler it expires instantly whenever no
    // connection is already idle, which downgrades the renewal to a logged
    // transient failure and leaves `last_heartbeat_at` at T0. So run this
    // phase on the real clock and wait for the durable write rather than
    // yielding a fixed number of times and hoping the write got there —
    // that is a coin flip on a saturated runner. `interval` ticks
    // immediately and then waits a full 20s cadence, so exactly one
    // renewal lands inside this window.
    tokio::time::resume();
    let (db, execution_id, lease) = heartbeat_fixture().await;
    let first_stop = CancellationToken::new();
    let (first_signal_tx, _first_signal_rx) = mpsc::unbounded_channel();
    let first_task = tokio::spawn(embedded_execution_heartbeat_with_clock(
        Arc::clone(&lease),
        Some(T20.to_owned()),
        first_stop.clone(),
        first_signal_tx,
        Arc::new(|| T1.to_owned()),
    ));
    let renewed = wait_for_renewed_heartbeat(&db, &execution_id, T1).await;
    first_stop.cancel();
    first_task.await.expect("heartbeat task joins");
    assert_eq!(renewed.last_heartbeat_at.as_deref(), Some(T1));
    assert_eq!(renewed.last_progress_at, None);
    let renewed_version = renewed.execution_version;

    // Move the paused scheduler to the immutable hard deadline. The next
    // server tick must report the typed deadline without another renewal.
    // This phase never touches the database before it breaks, so a paused
    // clock is safe here.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(20)).await;
    let stop = CancellationToken::new();
    let (signal_tx, mut signal_rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(embedded_execution_heartbeat_with_clock(
        Arc::clone(&lease),
        Some(T20.to_owned()),
        stop.clone(),
        signal_tx,
        Arc::new(|| T20.to_owned()),
    ));
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        signal_rx.recv().await,
        Some(EmbeddedLeaseSignal::HardDeadline)
    );
    task.await.expect("heartbeat task joins");
    stop.cancel();

    tokio::time::resume();
    let at_deadline = ExecutionRepo::get_by_id(&*db, &execution_id)
        .await
        .expect("execution reads")
        .expect("execution exists");
    assert_eq!(at_deadline.execution_version, renewed_version);
    assert_eq!(at_deadline.last_heartbeat_at.as_deref(), Some(T1));
    assert_eq!(at_deadline.last_progress_at, None);
}

#[test]
fn execution_deadline_uses_only_the_explicit_execution_setting() {
    let snapshot = serde_json::json!({
        "config": {"timeout_seconds": 900},
        "capabilities": {"max_execution_seconds": 120},
        "hard_deadline_seconds": 3600,
    });
    assert_eq!(execution_deadline_seconds(&snapshot), Some(3600));
    assert_eq!(
        execution_deadline_seconds(&serde_json::json!({
            "config": {"timeout_seconds": 900},
            "capabilities": {"max_execution_seconds": 120}
        })),
        None
    );
    assert_eq!(
        execution_deadline_seconds(&serde_json::json!({"hard_deadline_seconds": 0})),
        None
    );
    assert_eq!(execution_deadline_seconds(&serde_json::json!({})), None);
}

#[test]
fn only_the_terminal_cas_winner_may_ingest_a_completed_outbox() {
    assert_eq!(
        execution_outbox_disposition(&ExecutionStatus::Completed, true),
        ExecutionOutboxDisposition::Ingest
    );
    assert_eq!(
        execution_outbox_disposition(&ExecutionStatus::Completed, false),
        ExecutionOutboxDisposition::Preserve
    );
    for status in [
        ExecutionStatus::Running,
        ExecutionStatus::Failed,
        ExecutionStatus::Cancelled,
    ] {
        assert_eq!(
            execution_outbox_disposition(&status, true),
            ExecutionOutboxDisposition::Discard
        );
        assert_eq!(
            execution_outbox_disposition(&status, false),
            ExecutionOutboxDisposition::Discard
        );
    }
}

#[test]
fn non_completed_execution_discards_outbox_and_frozen_plan() {
    let root = tempfile::tempdir().expect("temporary root creates");
    let worktree = root.path().join("task").join("forge");
    std::fs::create_dir_all(&worktree).expect("worktree creates");
    let outbox =
        executors::prepare_execution_outbox(&worktree, "exec-1").expect("execution outbox creates");
    std::fs::write(
        outbox.join(executors::OUTBOX_PLAN_FILE),
        "- [ ] private plan\n",
    )
    .expect("plan candidate writes");
    assert!(
        crate::plan_artifact::stage_execution_outbox_plan(&outbox, &worktree, "exec-1")
            .expect("plan candidate stages")
    );
    let stage = worktree
        .parent()
        .expect("Task root exists")
        .join(".forge-plan-staging")
        .join("exec-1.md");
    assert!(outbox.exists());
    assert!(stage.exists());

    discard_execution_outbox_and_plan_stage(&worktree.to_string_lossy(), "exec-1");

    assert!(!outbox.exists());
    assert!(!stage.exists());
}

#[tokio::test]
async fn late_terminal_diagnostic_is_bounded_and_deduped_by_current_version() {
    let (db, execution_id, _lease) = heartbeat_fixture().await;
    let current = ExecutionRepo::get_by_id(&*db, &execution_id)
        .await
        .expect("execution reads")
        .expect("execution exists");
    let error = "x".repeat(2_000);
    let event = late_terminal_diagnostic(
        &execution_id,
        &current.task_id,
        "project-id",
        &ExecutionStatus::Completed,
        Some(&error),
        &current,
    );
    let expected_dedupe = format!(
        "execution-late-terminal-rejected:{}:{}:completed",
        execution_id, current.execution_version
    );
    assert_eq!(event.dedupe_key.as_deref(), Some(expected_dedupe.as_str()));
    let payload: Value = serde_json::from_str(&event.payload_json).expect("valid payload");
    assert_eq!(payload["attempted_status"], "completed");
    assert_eq!(payload["current_status"], "running");
    assert_eq!(payload["attempted_error"].as_str().map(str::len), Some(500));
    db::DomainEventRepo::append_event(&*db, event.clone())
        .await
        .expect("late terminal diagnostic appends");
    db::DomainEventRepo::append_event(&*db, event)
        .await
        .expect("duplicate late terminal diagnostic dedupes");
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'execution.late_terminal_rejected' AND dedupe_key = ?",
    )
    .bind(expected_dedupe)
    .fetch_one(db.pool())
    .await
    .expect("late terminal diagnostic count");
    assert_eq!(count, 1);
}
