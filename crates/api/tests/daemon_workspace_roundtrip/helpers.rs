//! Shared review, integration and queue helpers for daemon roundtrips.

use super::*;

pub(super) async fn assert_outage_order(socket_first: bool) {
    use db::AgentRepo;
    let mut fixture = Fixture::new(if socket_first {
        "forge-workspace-disconnect-first"
    } else {
        "forge-workspace-lease-first"
    })
    .await;
    let database = Arc::clone(&fixture.harness.state.db);
    let placement = fixture.resolved.placement.clone();
    let now = db::now_rfc3339();
    let agent = AgentRepo::create(
        &*database,
        db::CreateAgent {
            id: db::new_uuid_v4(),
            name: "outage-shell".into(),
            description: None,
            executor_type: "shell".into(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            capabilities_json: "[]".into(),
            config_json: "{}".into(),
            credential_ref: None,
            daemon_id: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: db::AgentStatus::Idle,
            last_heartbeat_at: Some(now.clone()),
            is_default: false,
            paused: false,
            owner_id: Some(common::fake_daemon::FAKE_DAEMON_USER_ID.into()),
            visibility: "global".into(),
            prompt_template: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    TaskRoleAssignmentRepo::assign(
        &*database,
        CreateTaskRoleAssignment {
            id: db::new_uuid_v4(),
            task_id: placement.task_id.clone(),
            role_name: "coder".into(),
            assignee_type: Some(AssigneeKind::Agent),
            assignee_id: Some(agent.id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task SET status = 'in_progress', assignee_type = 'agent', assignee_id = ?,
        version = version + 1, updated_at = ? WHERE id = ?",
    )
    .bind(&agent.id)
    .bind(&now)
    .bind(&placement.task_id)
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE workspace SET before_sha = ? WHERE id = ?")
        .bind(&fixture.base_sha)
        .bind(&placement.workspace_id)
        .execute(database.pool())
        .await
        .unwrap();
    // Stop at the Review gate so this test can inspect reconciliation before
    // another Agent or cleanup starts. The coder completion cascade is real.
    let mut workflow = services::workflow::default_workflow::default_workflow();
    workflow
        .states
        .iter_mut()
        .find(|state| state.name == "review")
        .unwrap()
        .hooks = StateHooks::default();
    sqlx::query(
        "UPDATE project SET workflow_definition = ?, version = version + 1 WHERE id =
        (SELECT project_id FROM task WHERE id = ?)",
    )
    .bind(serde_json::to_string(&workflow).unwrap())
    .bind(&placement.task_id)
    .execute(database.pool())
    .await
    .unwrap();
    let owner_connection = fixture
        .harness
        .state
        .daemon_connections
        .get(&fixture.daemon_id)
        .unwrap()
        .id();
    let execution_id = db::new_uuid_v4();
    let mut input = execution_input(
        &execution_id,
        &placement.task_id,
        &placement.workspace_id,
        "coder",
        ExecutionStatus::Running,
    );
    input.agent_id = Some(agent.id);
    input.before_sha = Some(fixture.base_sha.clone());
    // Dispatch stamps the Project revision it ran under; terminal effects from
    // a superseded revision are ignored.
    let project_version: i64 = sqlx::query_scalar(
        "SELECT version FROM project WHERE id = (SELECT project_id FROM task WHERE id = ?)",
    )
    .bind(&placement.task_id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    input.executor_config_snapshot_json = Some(
        json!({"executor_type": "shell", "config": {},
        "placement_id": placement.id, "project_version": project_version})
        .to_string(),
    );
    ExecutionRepo::create_with_lease(
        &*database,
        input,
        db::ClaimExecutionLease {
            execution_id: execution_id.clone(),
            expected_version: 1,
            owner: services::daemon_transport::execution_lease_owner(
                &fixture.daemon_id,
                owner_connection,
            ),
            lease_expires_at: (chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339(),
            hard_deadline_at: Some((chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339()),
            now,
        },
    )
    .await
    .unwrap();
    // Dispatch freezes the candidate pricing selections before it sends
    // `execution.start`; the owner's terminal usage report settles against them.
    assert!(fixture
        .harness
        .state
        .task_service
        .admit_execution_for_test(&execution_id)
        .await
        .unwrap());
    let (workspace_path, _) = fixture.resolved.owner_paths().await.unwrap();
    fixture.runtime.start(ExecutionStartParams {
            plan_text: None,
        task_id: placement.task_id.clone(), execution_id: execution_id.clone(),
            workspace_path,
        executor_type: "shell".into(), executor_config: json!({"executor_type": "shell", "config": {}}),
        prompt: json!({"description": format!(
            "sleep 1; printf 'outage line\\n' >> feature.txt; git add feature.txt; git commit -m 'outage line'; mkdir -p ../.forge-outbox/{execution_id}; printf '%s\\n' '{{\"kind\":\"validation\",\"summary\":\"Outage work committed\"}}' > ../.forge-outbox/{execution_id}/worklog.jsonl"
        )}), max_turns: None,
    }).await.unwrap();
    fixture.link.as_ref().unwrap().stop_heartbeats();
    // Freeze daemon frames while the CLI child keeps running. The server's
    // socket remains open for the lease-first ordering.
    let (frozen_outbound, _frozen_frames) = mpsc::unbounded_channel();
    fixture.runtime.attach(frozen_outbound);
    tokio::time::sleep(Duration::from_millis(250)).await;
    if socket_first {
        fixture.link.take();
        common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id)
            .await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while WorkspacePlacementRepo::get_by_id(&*database, &placement.id)
            .await
            .unwrap()
            .unwrap()
            .state
            != PlacementState::Disconnected
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "socket disconnect suspends before lease expiry"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    sqlx::query(
        "UPDATE execution SET lease_expires_at = ?, last_heartbeat_at = ?,
        execution_version = execution_version + 1 WHERE id = ?",
    )
    .bind((chrono::Utc::now() - chrono::Duration::minutes(9)).to_rfc3339())
    .bind((chrono::Utc::now() - chrono::Duration::minutes(10)).to_rfc3339())
    .bind(&execution_id)
    .execute(database.pool())
    .await
    .unwrap();
    let monitor =
        services::HeartbeatMonitor::new(database.clone(), fixture.harness.state.event_bus.clone())
            .with_daemon_connections(fixture.harness.state.daemon_connections.clone())
            .with_task_service(fixture.harness.state.task_service.clone());
    tokio::time::timeout(Duration::from_secs(5), monitor.check_once())
        .await
        .unwrap()
        .unwrap();
    let disconnected = WorkspacePlacementRepo::get_by_id(&*database, &placement.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(disconnected.state, PlacementState::Disconnected);
    assert_eq!(
        disconnected.failure_cause,
        Some(db::PlacementFailureCause::OwnerDisconnected)
    );
    assert!(disconnected.disconnected_at.is_some());
    assert_eq!(disconnected.version, placement.version + 1);
    assert_eq!(
        ExecutionRepo::get_by_id(&*database, &execution_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ExecutionStatus::Running
    );
    assert_eq!(
        TaskRepo::get_by_id(&*database, &placement.task_id, false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "in_progress"
    );
    if !socket_first {
        assert!(fixture
            .harness
            .state
            .daemon_connections
            .is_connected(&fixture.daemon_id));
        fixture.link.take();
        common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id)
            .await;
    }
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*database, &placement.id)
            .await
            .unwrap()
            .unwrap(),
        disconnected
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let report = loop {
        if let Some(report) = fixture
            .runtime
            .journal()
            .pending()
            .unwrap()
            .into_iter()
            .find_map(|entry| match entry {
                JournalEntry::Terminal { report } if report.execution_id == execution_id => {
                    Some(report)
                }
                _ => None,
            })
        {
            break report;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "CLI finishes and journals its committed head while frozen"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(report.exit_code, Some(0));
    assert!(report
        .after_sha
        .as_deref()
        .is_some_and(|head| head != fixture.base_sha));
    fixture.reconnect().await;
    // Bounded by progress, with its own budget: the wait above may have used
    // most of the shared deadline on a loaded machine.
    let mut stall = Stall::new(Duration::from_secs(10));
    loop {
        monitor.check_once().await.unwrap();
        let observed_placement = WorkspacePlacementRepo::get_by_id(&*database, &placement.id)
            .await
            .unwrap()
            .unwrap();
        let observed_task = TaskRepo::get_by_id(&*database, &placement.task_id, false)
            .await
            .unwrap()
            .unwrap();
        let observed_execution = ExecutionRepo::get_by_id(&*database, &execution_id)
            .await
            .unwrap()
            .unwrap();
        let pending = fixture.runtime.journal().pending().unwrap();
        if observed_placement.state == PlacementState::Ready
            && observed_task.status == "review"
            && pending.is_empty()
        {
            break;
        }
        let observed = format!(
            "{:?}|{}|{}|{}|{:?}|{}|{:?}|{}|{}",
            observed_placement.state,
            observed_placement.version,
            observed_task.status,
            observed_task.version,
            observed_execution.status,
            observed_execution.execution_version,
            pending
                .iter()
                .map(JournalEntry::entry_id)
                .collect::<Vec<_>>(),
            fixture
                .link
                .as_ref()
                .map_or(0, |link| link.requests.lock().unwrap().len()),
            fixture.server_activity().await
        );
        if stall.stalled(observed) {
            panic!(
                "retained terminal reconciles and advances the Task to Review: placement_state={:?}, placement_version={}, task_status={}, task_version={}, execution_status={:?}, execution_version={}, pending_journal_count={}, pending_entry_ids={:?}, last_server_frame={:?}",
                observed_placement.state,
                observed_placement.version,
                observed_task.status,
                observed_task.version,
                observed_execution.status,
                observed_execution.execution_version,
                pending.len(),
                pending.iter().map(JournalEntry::entry_id).collect::<Vec<_>>(),
                fixture.link.as_ref().and_then(DaemonLink::last_request),
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let finished = ExecutionRepo::get_by_id(&*database, &execution_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(finished.status, ExecutionStatus::Completed);
    assert_eq!(finished.after_sha, report.after_sha);
    let link = fixture.link.as_ref().unwrap();
    let acknowledgements = link.requests(METHOD_JOURNAL_ACK).len();
    link.outbound
        .send(DaemonFrame::Notification {
            method: METHOD_EXECUTION_TERMINAL.into(),
            params: serde_json::to_value(&report).unwrap(),
        })
        .unwrap();
    while link.requests(METHOD_JOURNAL_ACK).len() <= acknowledgements {
        assert!(
            tokio::time::Instant::now() < deadline,
            "duplicate terminal is acknowledged"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let terminals: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM domain_event WHERE event_type = 'execution.completed'
        AND json_extract(payload_json, '$.execution_id') = ?",
    )
    .bind(&execution_id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(terminals, 1);
    let outbox_entries: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM task_comment WHERE task_id = ? AND idempotency_key = ?",
    )
    .bind(&placement.task_id)
    .bind(format!("outbox:{execution_id}:worklog:1"))
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(outbox_entries, 1);
    let executions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM execution WHERE workspace_id = ?")
            .bind(&placement.workspace_id)
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(
        executions, 2,
        "only the fixture's historical execution and this attempt exist"
    );
    let placements: i64 =
        sqlx::query_scalar("SELECT count(*) FROM workspace_placement WHERE workspace_id = ?")
            .bind(&placement.workspace_id)
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(placements, 1);
    assert!(!link.requests(METHOD_WORKSPACE_DESCRIBE).is_empty());
    fixture.assert_server_cannot_resolve_workspace();
}

// Snapshot every table, including rows the effect must leave alone. SQLite's
// quote() preserves NULL/blob/text distinctions; sorting removes row order.
pub(super) async fn table_digests(db: &db::SqliteDb) -> BTreeMap<String, String> {
    use sha2::{Digest, Sha256};
    let tables: Vec<String> =
        sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .fetch_all(db.pool())
            .await
            .unwrap();
    let mut digests = BTreeMap::new();
    for table in tables {
        let columns: Vec<String> = sqlx::query_scalar("SELECT name FROM pragma_table_info(?)")
            .bind(&table)
            .fetch_all(db.pool())
            .await
            .unwrap();
        let expression = columns
            .iter()
            .map(|column| format!("quote(\"{}\")", column.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(" || '|' || ");
        async fn digest(db: &db::SqliteDb, table: &str, expression: &str) -> String {
            let mut rows: Vec<String> = sqlx::query_scalar(&format!(
                "SELECT {expression} FROM \"{}\"",
                table.replace('"', "\"\""),
            ))
            .fetch_all(db.pool())
            .await
            .unwrap();
            rows.sort();
            hex::encode(Sha256::digest(serde_json::to_vec(&rows).unwrap()))
        }
        digests.insert(table.clone(), digest(db, &table, &expression).await);
        let volatile: &[&str] = match table.as_str() {
            "execution" => &["before_sha", "after_sha", "updated_at"],
            "project" => &["list_revision"],
            "usage_ledger_revision" => &["execution_revision"],
            _ => &[],
        };
        if !volatile.is_empty() {
            let stable = columns
                .iter()
                .filter(|column| !volatile.contains(&column.as_str()))
                .map(|column| format!("quote(\"{}\")", column.replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(" || '|' || ");
            digests.insert(
                format!("{table}::stable"),
                digest(db, &table, &stable).await,
            );
        }
        if let Some(column) = match table.as_str() {
            "project" => Some("list_revision"),
            "usage_ledger_revision" => Some("execution_revision"),
            _ => None,
        } {
            let revision: i64 = sqlx::query_scalar(&format!("SELECT {column} FROM {table}"))
                .fetch_one(db.pool())
                .await
                .unwrap();
            digests.insert(format!("{table}::{column}"), revision.to_string());
        }
    }
    digests
}

// These backend effects leave Task-step projections to their caller.
pub(super) async fn task_projection(fixture: &Fixture) -> (String, Vec<(String, String)>, i64) {
    let db = &fixture.harness.state.db;
    let id = &fixture.resolved.placement.task_id;
    let task: String = sqlx::query_scalar("SELECT status || '|' || version || '|' || status_epoch || '|' || COALESCE(review_passed_at,'') || '|' || condition_json FROM task WHERE id=?")
        .bind(id).fetch_one(db.pool()).await.unwrap();
    let reviews =
        sqlx::query_as("SELECT status, step_results_json FROM review WHERE task_id=? ORDER BY id")
            .bind(id)
            .fetch_all(db.pool())
            .await
            .unwrap();
    let comments = sqlx::query_scalar("SELECT COUNT(*) FROM task_comment WHERE task_id=?")
        .bind(id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    (task, reviews, comments)
}

/// What a Cancel or Hold during remote review-entry CI must leave behind:
/// no consumer still waits (a late result is stale), the review attempt the
/// superseded step opened is closed, and the run is settled so it holds no
/// machine slot.
pub(super) async fn assert_remote_ci_abandoned(db: &db::SqliteDb, task_id: &str) {
    let waiting: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM check_consumer WHERE task_id=? AND cancelled_at IS NULL",
    )
    .bind(task_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(waiting, 0, "no consumer waits for the abandoned run");
    let review: String = sqlx::query_scalar(
        "SELECT status FROM review WHERE task_id=? ORDER BY attempt_number DESC LIMIT 1",
    )
    .bind(task_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(review, "cancelled");
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            let live: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM check_run r WHERE r.id IN (SELECT run_id FROM check_consumer WHERE task_id=?) AND r.state NOT IN ('succeeded','failed','cancelled')")
                .bind(task_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
            if live == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the abandoned remote run settles and frees its slot");
    let applied: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM check_consumer WHERE task_id=? AND applied_at IS NOT NULL",
    )
    .bind(task_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(applied, 0, "the late result is applied to nothing");
}

/// A Task in `review` whose entry CI runs on the fixture's daemon; returns
/// once the step has started there.
pub(super) async fn start_remote_review_ci(fixture: &Fixture, name: &str) -> std::path::PathBuf {
    let db = &fixture.harness.state.db;
    let task_id = fixture.resolved.placement.task_id.clone();
    let started = fixture._daemon_root.path().join(format!("{name}-started"));
    let completed = fixture
        ._daemon_root
        .path()
        .join(format!("{name}-completed"));
    sqlx::query("UPDATE task SET task_state_config=?,status='merge_failed' WHERE id=?").bind(json!({"review":{"ci_steps":[format!("touch {}; sleep 120; touch {}",quote(&started.to_string_lossy()),quote(&completed.to_string_lossy()))]}}).to_string()).bind(&task_id).execute(db.pool()).await.unwrap();
    let task = TaskRepo::get_by_id(&**db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    fixture
        .harness
        .state
        .task_service
        .transition(
            &task_id,
            "review".to_owned(),
            services::task_service::TransitionOptions {
                bridge: Default::default(),
                version: task.version,
                triggered_by: Actor::system(SystemComponent::Workflow),
                reason: Some("remote CI".into()),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        while !started.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    completed
}

pub(super) async fn live_check_runs(db: &db::SqliteDb, project_id: &str) -> Vec<String> {
    sqlx::query_scalar("SELECT state FROM check_run WHERE project_id=? AND state NOT IN ('succeeded','failed','cancelled')")
        .bind(project_id)
        .fetch_all(db.pool())
        .await
        .unwrap()
}

/// A claimed queue attempt for the fixture's Task: its fence and attempt id.
pub(super) async fn claimed_attempt(
    fixture: &Fixture,
    key: &str,
) -> (IntegrationOwnerFence, String) {
    use db::IntegrationQueueRepo;
    let database = fixture.harness.state.db.clone();
    let placement = &fixture.resolved.placement;
    let workspace = WorkspaceRepo::get_by_id(&*database, &placement.workspace_id)
        .await
        .unwrap()
        .unwrap();
    let task = TaskRepo::get_by_id(&*database, &placement.task_id, false)
        .await
        .unwrap()
        .unwrap();
    let epoch: i64 = sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=?")
        .bind(&task.id)
        .fetch_one(database.pool())
        .await
        .unwrap();
    let queue = database
        .create_or_get_integration_queue(&workspace.repo_id, "main")
        .await
        .unwrap();
    let mut attempt = db::IntegrationAttempt::new(
        Some(queue.id.clone()),
        task.id,
        task.project_id,
        key.into(),
        task.status,
        epoch,
        task.version,
    );
    // What admission records about the owner of the Task's checkout. An
    // effect performed off the target's owner is admitted only against it.
    attempt.repo_location_id = Some(placement.repo_location_id.clone());
    attempt.owner_kind = Some(if placement.daemon_id.is_some() {
        db::IntegrationOwnerKind::Daemon
    } else {
        db::IntegrationOwnerKind::Server
    });
    attempt.daemon_id = placement.daemon_id.clone();
    attempt.runtime_id = placement.runtime_id.clone();
    let attempt = database.admit_integration_attempt(attempt).await.unwrap();
    let queue = database
        .integration_queue(&queue.id)
        .await
        .unwrap()
        .unwrap();
    database
        .claim_integration_queue(
            &queue.id,
            queue.revision,
            "wire-worker",
            &db::now_rfc3339(),
            "2099-01-01T00:00:00Z",
        )
        .await
        .unwrap();
    let fence = database
        .integration_owner_fence(&attempt.id)
        .await
        .unwrap()
        .unwrap();
    let fence = serde_json::from_value(serde_json::to_value(fence).unwrap()).unwrap();
    (fence, attempt.id)
}

pub(super) fn git_sync(path: &std::path::Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(path)
        .env("GIT_AUTHOR_NAME", "Forge")
        .env("GIT_AUTHOR_EMAIL", "forge@example.invalid")
        .env("GIT_COMMITTER_NAME", "Forge")
        .env("GIT_COMMITTER_EMAIL", "forge@example.invalid")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

pub(super) fn transfer_staging(root: &std::path::Path) -> Vec<String> {
    std::fs::read_dir(root.join(".forge/transfer"))
        .map(|entries| {
            entries
                .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default()
}

/// Stage a started attempt effect the owner never received.
pub(super) async fn lost_fast_forward(
    fixture: &Fixture,
    fence: &IntegrationOwnerFence,
    candidate: &str,
) -> db::IntegrationEffectRequest {
    let database = &fixture.harness.state.db;
    let placement = &fixture.resolved.placement;
    let witness = json!({"workspace":{"workspace_id":placement.workspace_id,"placement_id":placement.id,"generation":placement.generation,"handle":placement.workspace_handle,"owner":{"kind":"daemon","daemon_id":placement.daemon_id,"runtime_id":placement.runtime_id}},"target_branch":"main","expected_head_sha":candidate,"expected_target_sha":fixture.base_sha,"reviewed":{"commit_sha":candidate,"base_sha":fixture.base_sha}});
    let request = db::IntegrationEffectRequest {
        fence: serde_json::from_value(serde_json::to_value(fence).unwrap()).unwrap(),
        kind: db::IntegrationOperationKind::FastForward,
        witness,
    };
    let db::IntegrationEffectAdmission::Started(mut guard) = database
        .begin_integration_effect(request.clone())
        .await
        .unwrap()
    else {
        panic!("effect admitted")
    };
    assert!(guard.start().await.unwrap().is_none());
    drop(guard);
    request
}

// ----- the integration queue worker on a daemon-owned target (3.2 D2a) -----

pub(super) struct QueueHead {
    pub(super) attempt_id: String,
    pub(super) queue_id: String,
    pub(super) task_id: String,
    pub(super) candidate: String,
}

/// The fixture's Task, with a candidate commit, in `merging` with a passed
/// review and admitted to its repository's `main` queue.
pub(super) async fn admit_queue_head(fixture: &Fixture, ci_steps: &[&str]) -> QueueHead {
    let candidate = fixture.candidate().await;
    admit_placed_head(fixture, &fixture.resolved.placement, candidate, ci_steps).await
}

/// The Task of `placement`, whose checkout holds `candidate`, in `merging`
/// with a passed review and admitted to its repository's `main` queue.
pub(super) async fn admit_placed_head(
    fixture: &Fixture,
    placement: &db::WorkspacePlacement,
    candidate: String,
    ci_steps: &[&str],
) -> QueueHead {
    use db::IntegrationQueueRepo;
    let database = fixture.harness.state.db.clone();
    let placement = placement.clone();
    let workspace = WorkspaceRepo::get_by_id(&*database, &placement.workspace_id)
        .await
        .unwrap()
        .unwrap();
    let now = db::now_rfc3339();
    sqlx::query("UPDATE task SET status='merging',review_passed_at=?,task_state_config=?,updated_at=? WHERE id=?")
        .bind(&now)
        .bind(json!({"review":{"ci_steps":ci_steps}}).to_string())
        .bind(&now)
        .bind(&placement.task_id)
        .execute(database.pool())
        .await
        .unwrap();
    let task = TaskRepo::get_by_id(&*database, &placement.task_id, false)
        .await
        .unwrap()
        .unwrap();
    let epoch: i64 = sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=?")
        .bind(&task.id)
        .fetch_one(database.pool())
        .await
        .unwrap();
    let queue = database
        .create_or_get_integration_queue(&workspace.repo_id, "main")
        .await
        .unwrap();
    let mut attempt = db::IntegrationAttempt::new(
        Some(queue.id.clone()),
        task.id.clone(),
        task.project_id,
        format!("admit-{}", task.id),
        task.status,
        epoch,
        task.version,
    );
    attempt.original_candidate_sha = Some(candidate.clone());
    // What admission records about where the candidate lives.
    attempt.workspace_id = Some(workspace.id.clone());
    attempt.workspace_ref = Some(workspace.id.clone());
    attempt.placement_id = Some(placement.id.clone());
    attempt.placement_ref = Some(placement.id.clone());
    attempt.repo_location_id = Some(placement.repo_location_id.clone());
    attempt.repo_location_ref = Some(placement.repo_location_id.clone());
    attempt.owner_kind = Some(if placement.daemon_id.is_some() {
        db::IntegrationOwnerKind::Daemon
    } else {
        db::IntegrationOwnerKind::Server
    });
    attempt.daemon_id = placement.daemon_id.clone();
    attempt.runtime_id = placement.runtime_id.clone();
    attempt.placement_generation = Some(placement.generation);
    let attempt = database.admit_integration_attempt(attempt).await.unwrap();
    QueueHead {
        attempt_id: attempt.id,
        queue_id: queue.id,
        task_id: task.id,
        candidate,
    }
}

/// `build_integration_worker` over the harness's router and daemon registry,
/// running, with fast timers.
pub(super) fn run_queue_worker(
    fixture: &Fixture,
    lease: Duration,
) -> (
    tokio::sync::watch::Sender<bool>,
    JoinHandle<services::Result<()>>,
) {
    use services::integration_worker::IntegrationWorkerConfig;
    let state = &fixture.harness.state;
    let worker = services::integration_ports::build_integration_worker(
        state.db.clone(),
        state.workspace_backend_router.clone(),
        state.daemon_connections.clone(),
        state.repo_location_service.clone(),
        IntegrationWorkerConfig {
            poll: Duration::from_millis(20),
            sweep_interval: Duration::from_millis(100),
            conflict_backoff: Duration::from_millis(5),
            reconcile_interval: Duration::from_millis(300),
            // A head left with an unknown result is taken up again when its
            // lease runs out.
            lease,
            renew_every: lease / 4,
            ..Default::default()
        },
    );
    let (stop, stopped) = tokio::sync::watch::channel(false);
    (stop, tokio::spawn(worker.run(stopped)))
}

pub(super) async fn queue_attempt(fixture: &Fixture, head: &QueueHead) -> db::IntegrationAttempt {
    use db::IntegrationQueueRepo;
    fixture
        .harness
        .state
        .db
        .integration_attempt(&head.attempt_id)
        .await
        .unwrap()
        .unwrap()
}

pub(super) async fn task_status(fixture: &Fixture, task_id: &str) -> String {
    sqlx::query_scalar("SELECT status FROM task WHERE id=?")
        .bind(task_id)
        .fetch_one(fixture.harness.state.db.pool())
        .await
        .unwrap()
}

/// Polls `done` for up to a minute, naming the attempt's state on a timeout.
macro_rules! queue_eventually {
    ($fixture:expr, $head:expr, $what:literal, $done:expr) => {{
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        while !$done {
            if tokio::time::Instant::now() >= deadline {
                let attempt = queue_attempt($fixture, $head).await;
                let receipts: Vec<String> = serde_json::from_value::<
                    Vec<db::IntegrationEffectReceipt>,
                >(attempt.effect_receipts_json.clone())
                .unwrap_or_default()
                .iter()
                .map(|receipt| {
                    format!(
                        "{:?}@{} {:?} {}",
                        receipt.request.kind,
                        receipt.request.fence.generation,
                        receipt.operation_state,
                        receipt.result
                    )
                })
                .collect();
                panic!(
                    "timed out waiting for {}: attempt {} ({:?}: {:?}), Task {}, receipts {:#?}",
                    $what,
                    attempt.state,
                    attempt.failure_kind,
                    attempt.failure_message,
                    task_status($fixture, &$head.task_id).await,
                    receipts
                );
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }};
}

/// Requests of `method` this link saw that were bound to a queue attempt.
pub(super) fn attempt_requests(link: &DaemonLink, method: &str) -> Vec<Value> {
    link.requests(method)
        .into_iter()
        .filter(|params| params["integration"]["kind"] == "attempt")
        .collect()
}

pub(super) fn fast_forward_receipts(attempt: &db::IntegrationAttempt) -> usize {
    serde_json::from_value::<Vec<db::IntegrationEffectReceipt>>(
        attempt.effect_receipts_json.clone(),
    )
    .unwrap()
    .into_iter()
    .filter(|receipt| {
        receipt.request.kind == db::IntegrationOperationKind::FastForward
            && receipt.operation_state == db::IntegrationOperationState::Succeeded
    })
    .count()
}

// ----- cross-owner queue heads (3.2 D2a part 2) -----
//
// The Task's checkout and the repository's default checkout are held by
// different owners. The rebase and the head check run on the Task's owner,
// the fast-forward on the owner of the default checkout; each effect is
// requested from, journaled by and reconciled with the owner that ran it.

/// The check worker, as the server runtime starts it. `$stop` names the
/// sender that stops it.
macro_rules! start_check_worker {
    ($fixture:expr, $stop:ident) => {
        let check_state = $fixture.harness.state.clone();
        let check_periodic = services::worker_runtime::PeriodicWorkers::new(check_state.db.clone());
        let ($stop, checks_stopped) = tokio::sync::watch::channel(false);
        let _checks = check_state
            .task_service
            .compose_checks(
                check_state.daemon_connections.clone(),
                Duration::from_secs(60),
                &services::integration_worker::IntegrationWorkerConfig::default(),
            )
            .worker
            .start(&check_periodic, checks_stopped);
    };
}

/// Makes a server-owned clone of the fixture's repository its default
/// checkout. The daemon-owned location the Task is placed in stays, no
/// longer the default. Call before the candidate is committed: the clone
/// then holds `main` only.
pub(super) async fn server_owned_default(fixture: &Fixture) -> PathBuf {
    let database = fixture.harness.state.db.clone();
    let clone = fixture.server_root.path().join("default-checkout");
    git_sync(
        fixture.server_root.path(),
        &[
            "clone",
            "-q",
            "--single-branch",
            "--branch",
            "main",
            fixture.checkout.to_str().unwrap(),
            "default-checkout",
        ],
    );
    for (key, value) in [
        ("user.name", "Forge"),
        ("user.email", "forge@example.invalid"),
    ] {
        git_sync(&clone, &["config", key, value]);
    }
    let placement = &fixture.resolved.placement;
    let repo_id = WorkspaceRepo::get_by_id(&*database, &placement.workspace_id)
        .await
        .unwrap()
        .unwrap()
        .repo_id;
    sqlx::query("UPDATE repo_location SET is_default=0 WHERE id=?")
        .bind(&placement.repo_location_id)
        .execute(database.pool())
        .await
        .unwrap();
    let now = db::now_rfc3339();
    RepoLocationRepo::create(
        &*database,
        CreateRepoLocation {
            id: db::new_uuid_v4(),
            repo_id,
            owner_kind: RepoLocationOwnerKind::Server,
            daemon_id: None,
            runtime_id: None,
            path: clone.to_string_lossy().into_owned(),
            kind: RepoLocationKind::PrimaryCheckout,
            is_default: true,
            status: RepoLocationStatus::Ready,
            last_verified_at: Some(now.clone()),
            last_error: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("server-owned default location creates");
    clone
}

pub(super) fn integration_refs(path: &std::path::Path) -> String {
    git_sync(
        path,
        &["for-each-ref", "--format=%(refname)", "refs/forge/"],
    )
}

pub(super) fn attempt_receipts(
    attempt: &db::IntegrationAttempt,
) -> Vec<db::IntegrationEffectReceipt> {
    serde_json::from_value(attempt.effect_receipts_json.clone()).unwrap()
}

/// Receipts of `kind` and the daemon (or `None` for the server) that
/// performed each.
pub(super) fn receipt_owners(
    attempt: &db::IntegrationAttempt,
    kind: db::IntegrationOperationKind,
) -> Vec<(db::IntegrationOperationState, Option<String>)> {
    attempt_receipts(attempt)
        .into_iter()
        .filter(|receipt| receipt.request.kind == kind)
        .map(|receipt| {
            (
                receipt.operation_state,
                receipt.request.effect_owner()["daemon_id"]
                    .as_str()
                    .map(str::to_owned),
            )
        })
        .collect()
}

/// A second Task of the fixture's Project, in `review`, with its workspace
/// row. The worktree path is filled in by the caller's placement.
pub(super) async fn second_task(
    fixture: &Fixture,
    worktree_path: &str,
) -> (db::Task, db::Workspace) {
    let database = fixture.harness.state.db.clone();
    let first = TaskRepo::get_by_id(&*database, &fixture.resolved.placement.task_id, false)
        .await
        .unwrap()
        .unwrap();
    let repo_id = WorkspaceRepo::get_by_id(&*database, &fixture.resolved.placement.workspace_id)
        .await
        .unwrap()
        .unwrap()
        .repo_id;
    let now = db::now_rfc3339();
    let task = TaskRepo::create(
        &*database,
        CreateTask {
            id: db::new_uuid_v4(),
            project_id: first.project_id,
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Task in another checkout".into(),
            description: None,
            task_type: "task".into(),
            status: "review".into(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("second task creates");
    let workspace = WorkspaceRepo::create(
        &*database,
        CreateWorkspace {
            id: db::new_uuid_v4(),
            task_id: task.id.clone(),
            repo_id,
            worktree_path: worktree_path.to_owned(),
            branch: workspace::task_branch_name(&task.id),
            status: WorkspaceStatus::Creating,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("second workspace creates");
    // Queue admission in production always comes from an implementation
    // execution; cross-checkout fixtures need that evidence parent too.
    ExecutionRepo::create(
        &*database,
        execution_input(
            &db::new_uuid_v4(),
            &task.id,
            &workspace.id,
            "executor",
            ExecutionStatus::Completed,
        ),
    )
    .await
    .unwrap();
    (task, workspace)
}

pub(super) fn clone_main(from: &std::path::Path, parent: &std::path::Path, name: &str) -> PathBuf {
    git_sync(
        parent,
        &[
            "clone",
            "-q",
            "--single-branch",
            "--branch",
            "main",
            from.to_str().unwrap(),
            name,
        ],
    );
    let clone = parent.join(name);
    for (key, value) in [
        ("user.name", "Forge"),
        ("user.email", "forge@example.invalid"),
    ] {
        git_sync(&clone, &["config", key, value]);
    }
    clone
}

/// A Task placed on the server, in a worktree of a server-owned clone of the
/// fixture's repository (not the default checkout), with a candidate commit.
/// Returns its placement, the clone and the candidate.
pub(super) async fn server_placed_task(
    fixture: &Fixture,
) -> (db::WorkspacePlacement, PathBuf, String) {
    let database = fixture.harness.state.db.clone();
    let clone = clone_main(&fixture.checkout, fixture.server_root.path(), "task-clone");
    let worktree = fixture
        .server_root
        .path()
        .join("workspaces")
        .join("cross-owner-task");
    std::fs::create_dir_all(worktree.parent().unwrap()).unwrap();
    let (task, workspace) = second_task(fixture, worktree.to_str().unwrap()).await;
    git_sync(
        &clone,
        &[
            "worktree",
            "add",
            "-q",
            "-b",
            &workspace.branch,
            worktree.to_str().unwrap(),
            "main",
        ],
    );
    let now = db::now_rfc3339();
    let location = RepoLocationRepo::create(
        &*database,
        CreateRepoLocation {
            id: db::new_uuid_v4(),
            repo_id: workspace.repo_id.clone(),
            owner_kind: RepoLocationOwnerKind::Server,
            daemon_id: None,
            runtime_id: None,
            path: clone.to_string_lossy().into_owned(),
            kind: RepoLocationKind::PrimaryCheckout,
            is_default: false,
            status: RepoLocationStatus::Ready,
            last_verified_at: Some(now.clone()),
            last_error: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("server-owned Task location creates");
    let placement = WorkspacePlacementRepo::create(
        &*database,
        CreateWorkspacePlacement {
            id: db::new_uuid_v4(),
            workspace_id: workspace.id.clone(),
            task_id: task.id.clone(),
            agent_id: None,
            owner_kind: PlacementOwnerKind::Server,
            daemon_id: None,
            runtime_id: None,
            repo_location_id: location.id,
            execution_daemon_id: None,
            workspace_handle: Some(worktree.to_string_lossy().into_owned()),
            generation: 1,
            state: PlacementState::Ready,
            selected_by: PlacementSelectedBy::Scheduler,
            selection_reason: "{}".into(),
            reserved_until: None,
            disconnected_at: None,
            failure_cause: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("server placement creates");
    WorkspaceRepo::update_status(
        &*database,
        &workspace.id,
        WorkspaceStatus::Ready,
        None,
        &now,
    )
    .await
    .expect("workspace is ready");
    std::fs::write(worktree.join("feature.txt"), "candidate\n").unwrap();
    git_sync(&worktree, &["add", "feature.txt"]);
    git_sync(&worktree, &["commit", "-q", "-m", "candidate"]);
    let candidate = git_sync(&worktree, &["rev-parse", "HEAD"]);
    (placement, clone, candidate)
}

/// A second connected daemon with its own workspace root.
pub(super) struct ExtraDaemon {
    pub(super) link: Option<DaemonLink>,
    pub(super) runtime: Arc<DaemonRuntime>,
    pub(super) daemon_id: String,
    pub(super) runtime_id: String,
    pub(super) token: String,
    pub(super) root: TestDir,
}

pub(super) async fn extra_daemon(fixture: &Fixture, suite: &str) -> ExtraDaemon {
    let harness = &fixture.harness;
    let root = TestDir::new(&format!("{suite}-second-owner"));
    let registration =
        common::fake_daemon::register_daemon(&harness.app, &db::new_uuid_v4(), suite).await;
    common::fake_daemon::report_remote_daemon_shell(
        &harness.app,
        &registration.daemon_id,
        &registration.registration_token,
        root.path(),
        suite,
    )
    .await;
    let runtime_id = RuntimeRepo::get_by_daemon_id(&*harness.state.db, &registration.daemon_id)
        .await
        .expect("runtime loads")
        .expect("runtime exists")
        .id;
    let (outbound, _discarded) = mpsc::unbounded_channel();
    let runtime = DaemonRuntime::new_owned(
        outbound,
        root.path().to_path_buf(),
        ActiveExecutionTracker::default(),
        registration.daemon_id.clone(),
        WorkspaceRunPolicy {
            allowed_purposes: vec![
                WorkspaceRunPurpose::CiStep,
                WorkspaceRunPurpose::Hook,
                WorkspaceRunPurpose::EnvironmentSetup,
            ],
        },
    )
    .expect("second owner runtime starts");
    let link = DaemonLink::connect(
        &fixture.server,
        &registration.daemon_id,
        &registration.registration_token,
        Arc::clone(&runtime),
    )
    .await;
    ExtraDaemon {
        link: Some(link),
        runtime,
        daemon_id: registration.daemon_id,
        runtime_id,
        token: registration.registration_token,
        root,
    }
}

/// A Task placed on a daemon, in a worktree of a clone of the fixture's
/// repository under `root` (a location of that daemon, not the default
/// checkout), prepared by the daemon, with a candidate commit.
pub(super) async fn daemon_clone_task(
    fixture: &Fixture,
    daemon_id: &str,
    runtime_id: &str,
    root: &std::path::Path,
) -> (ResolvedWorkspace, PathBuf, String) {
    daemon_task(fixture, daemon_id, runtime_id, Some(root), "feature.txt").await
}

/// A second Task placed on a daemon with a candidate commit that adds
/// `file`: in a worktree of a new clone under `clone_root` (a location of
/// that daemon, not the default checkout), or, without one, in another
/// worktree of the fixture's own location.
pub(super) async fn daemon_task(
    fixture: &Fixture,
    daemon_id: &str,
    runtime_id: &str,
    clone_root: Option<&std::path::Path>,
    file: &str,
) -> (ResolvedWorkspace, PathBuf, String) {
    let database = fixture.harness.state.db.clone();
    let clone = match clone_root {
        Some(root) => clone_main(&fixture.checkout, root, "task-clone"),
        None => fixture.checkout.clone(),
    };
    let (task, workspace) = second_task(
        fixture,
        &fixture
            .server_root
            .path()
            .join("missing-checkout/second")
            .to_string_lossy(),
    )
    .await;
    let now = db::now_rfc3339();
    let location_id = match clone_root {
        None => fixture.resolved.placement.repo_location_id.clone(),
        Some(_) => {
            RepoLocationRepo::create(
                &*database,
                CreateRepoLocation {
                    id: db::new_uuid_v4(),
                    repo_id: workspace.repo_id.clone(),
                    owner_kind: RepoLocationOwnerKind::Daemon,
                    daemon_id: Some(daemon_id.to_owned()),
                    runtime_id: Some(runtime_id.to_owned()),
                    path: clone.to_string_lossy().into_owned(),
                    kind: RepoLocationKind::PrimaryCheckout,
                    is_default: false,
                    status: RepoLocationStatus::Ready,
                    last_verified_at: Some(now.clone()),
                    last_error: None,
                    created_at: now.clone(),
                    updated_at: now.clone(),
                },
            )
            .await
            .expect("second daemon location creates")
            .id
        }
    };
    let location = RepoLocationRepo::get_by_id(&*database, &location_id)
        .await
        .unwrap()
        .unwrap();
    let placement = WorkspacePlacementRepo::create(
        &*database,
        CreateWorkspacePlacement {
            id: db::new_uuid_v4(),
            workspace_id: workspace.id.clone(),
            task_id: task.id.clone(),
            agent_id: None,
            owner_kind: PlacementOwnerKind::Daemon,
            daemon_id: Some(daemon_id.to_owned()),
            runtime_id: Some(runtime_id.to_owned()),
            repo_location_id: location.id.clone(),
            execution_daemon_id: Some(daemon_id.to_owned()),
            workspace_handle: None,
            generation: 1,
            state: PlacementState::Preparing,
            selected_by: PlacementSelectedBy::Scheduler,
            selection_reason: "{}".into(),
            reserved_until: None,
            disconnected_at: None,
            failure_cause: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("second daemon placement creates");
    let backend = fixture
        .harness
        .state
        .workspace_backend_router
        .for_placement(&placement)
        .expect("daemon backend is installed");
    if clone_root.is_some() {
        backend
            .daemon_client()
            .expect("owner client")
            .verify_location(
                daemon_id,
                RepoLocationVerifyParams {
                    repo_location_id: location.id,
                    daemon_id: daemon_id.to_owned(),
                    runtime_id: runtime_id.to_owned(),
                    path: clone.to_string_lossy().into_owned(),
                    kind: DaemonRepoLocationKind::PrimaryCheckout,
                    default_branch: "main".into(),
                    remote_url: None,
                    expected_version: location.version,
                    probe: None,
                },
            )
            .await
            .expect("owner verifies the Task's clone");
    }
    let prepared = backend
        .prepare(
            &placement,
            &PrepareSpec {
                base_ref: "main".into(),
            },
        )
        .await
        .expect("owner prepares a worktree in the clone");
    let placement = WorkspacePlacementRepo::update(
        &*database,
        UpdateWorkspacePlacement {
            id: placement.id,
            expected_version: placement.version,
            agent_id: None,
            owner_kind: None,
            daemon_id: None,
            runtime_id: None,
            repo_location_id: None,
            execution_daemon_id: None,
            workspace_handle: Some(Some(prepared.handle)),
            generation: None,
            state: Some(PlacementState::Ready),
            selected_by: None,
            selection_reason: None,
            reserved_until: None,
            disconnected_at: None,
            failure_cause: None,
            updated_at: db::now_rfc3339(),
        },
    )
    .await
    .expect("prepared handle persists");
    WorkspaceRepo::update_status(
        &*database,
        &workspace.id,
        WorkspaceStatus::Ready,
        None,
        &now,
    )
    .await
    .expect("workspace is ready");
    let resolved = ResolvedWorkspace { placement, backend };
    let committed = resolved
        .backend
        .run(
            &resolved.placement,
            &RunSpec {
                purpose: WorkspaceRunPurpose::CiStep,
                command: format!(
                    "printf 'candidate\\n' > {file}; git add {file}; git commit -m candidate"
                ),
                env: BTreeMap::new(),
                timeout_secs: 0,
                max_output_bytes: usize::MAX,
            },
        )
        .await
        .expect("owner command succeeds");
    assert_eq!(committed.exit_code, 0);
    let candidate = resolved
        .git_query(WorkspaceGitQuery::Head, false)
        .await
        .unwrap()
        .unwrap()
        .trim()
        .to_owned();
    (resolved, clone, candidate)
}

/// What every merged cross-checkout head must show: the default checkout
/// holds exactly the checked candidate, and the transfer refs are gone.
pub(super) async fn assert_landed_exactly(
    fixture: &Fixture,
    head: &QueueHead,
    default_checkout: &std::path::Path,
    moved_tip: &str,
) -> db::IntegrationAttempt {
    let attempt = queue_attempt(fixture, head).await;
    assert!(
        attempt.failure_message.is_none(),
        "never parked: {:?}",
        attempt.failure_message
    );
    let tip = git_sync(default_checkout, &["rev-parse", "refs/heads/main"]);
    assert_eq!(attempt.candidate_sha.as_deref(), Some(tip.as_str()));
    assert_eq!(attempt.integrated_sha.as_deref(), Some(tip.as_str()));
    assert_eq!(attempt.integrated_before_sha.as_deref(), Some(moved_tip));
    assert_ne!(tip, head.candidate, "the queue rebased the candidate");
    assert_eq!(git_sync(default_checkout, &["rev-parse", "HEAD"]), tip);
    for file in ["feature.txt", "outside.txt"] {
        assert!(default_checkout.join(file).exists(), "{file}");
    }
    assert!(integration_refs(default_checkout).is_empty());
    attempt
}

pub(super) fn succeeded(
    attempt: &db::IntegrationAttempt,
    kind: db::IntegrationOperationKind,
) -> Vec<Option<String>> {
    receipt_owners(attempt, kind)
        .into_iter()
        .filter(|(state, _)| *state == db::IntegrationOperationState::Succeeded)
        .map(|(_, owner)| owner)
        .collect()
}

/// The head's claim, taken by this test instead of a running worker, and the
/// production owner and facts ports to drive it by hand.
pub(super) async fn claimed_head(
    fixture: &Fixture,
    head: &QueueHead,
) -> (
    IntegrationOwnerFence,
    services::integration_ports::RoutingIntegrationOwner,
    services::integration_worker::HeadFacts,
) {
    use db::IntegrationQueueRepo;
    use services::{
        daemon_transport::workspace_client::DaemonWorkspaceClient,
        integration_owner::ServerIntegrationOwner,
        integration_ports::{DaemonFences, RoutingIntegrationOwner, WorkspaceHeadFacts},
        integration_worker::IntegrationFactsPort,
    };
    let state = fixture.harness.state.clone();
    let database = state.db.clone();
    let queue = database
        .integration_queue(&head.queue_id)
        .await
        .unwrap()
        .unwrap();
    let queue = database
        .claim_integration_queue(
            &queue.id,
            queue.revision,
            "wire-worker",
            &db::now_rfc3339(),
            "2099-01-01T00:00:00Z",
        )
        .await
        .unwrap();
    let fence = database
        .integration_owner_fence(&head.attempt_id)
        .await
        .unwrap()
        .unwrap();
    let client = DaemonWorkspaceClient::new(state.daemon_connections.clone())
        .with_receipts(database.clone());
    let owner = RoutingIntegrationOwner::new(
        Arc::new(ServerIntegrationOwner::new(database.clone())),
        client.clone(),
        Arc::new(DaemonFences::new(database.clone(), client.clone())),
    );
    let facts = WorkspaceHeadFacts::new(database.clone(), state.workspace_backend_router.clone())
        .with_daemons(client)
        .head_facts(&queue_attempt(fixture, head).await, &queue)
        .await
        .unwrap();
    (fence, owner, facts)
}

pub(super) use queue_eventually;
pub(super) use start_check_worker;
