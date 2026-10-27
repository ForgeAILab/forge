//! Operator status fixtures and payload tests.

async fn sync_fixtures(service: &super::OperatorStatusService) {
    let ids: Vec<String> = sqlx::query_scalar("SELECT id FROM task")
        .fetch_all(service.db.pool())
        .await
        .unwrap();
    service.db.check_task_conditions_of(&ids).await.unwrap();
}

// Fixture tests for operator status payloads. Each test sets up specific DB rows and
// workspace state, calls compute_status(), and asserts the resulting payload shape.
// To add a new fixture: insert rows using the helpers in this module, call compute_status(),
// and assert the fields you care about. Tests are independent (in-memory SQLite per test).

use super::*;
use serde_json::json;
use std::fs;

use db::{create_sqlite_pool, new_uuid_v4, run_migrations};
use tempfile::tempdir;

#[tokio::test]
async fn machine_capacity_operations_includes_server_host() {
    let (db, service) = test_service().await;
    db.server_run_cap.set(
        Some(4),
        config::resolved_run_cap(Some(4)),
        &config::embedded_machine_id(),
    );
    let pressure = service.daemon_pressure().await.unwrap();
    assert_eq!(pressure.len(), 1);
    assert_eq!(pressure[0].daemon_id, "server_host");
    assert_eq!(pressure[0].active_runs, 0);
    assert_eq!(pressure[0].max_concurrent_runs, Some(4));
    assert_eq!(
        pressure[0].logical_cores,
        Some(config::logical_cores() as u32)
    );
    assert_eq!(
        pressure[0].build_jobs_per_run,
        Some(executors::run_process::machine_policy().get().build_jobs())
    );
    assert_eq!(pressure[0].run_nice, Some(10));
    assert!(!pressure[0].at_capacity);
}

async fn test_service() -> (Arc<SqliteDb>, OperatorStatusService) {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let service = OperatorStatusService::new(Arc::clone(&db)).with_agent_event_consumer_names(
        Arc::new(crate::test_support::TestAgentEventConsumerNames),
    );
    service.set_runtime_workers(&crate::worker_runtime::assembly::COMMON_WORKERS);
    (db, service)
}

#[tokio::test]
async fn integration_status_reports_ages_and_the_timings_of_completed_merges() {
    let (db, service) = test_service().await;
    let now = Utc::now();
    let at = |seconds_ago: i64| (now - Duration::seconds(seconds_ago)).to_rfc3339();
    sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES('timing-project','timing-project',?,?)").bind(at(0)).bind(at(0)).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO repo(id,project_id,name,default_branch,created_at,updated_at) VALUES('timing-repo','timing-project','timing-repo','main',?,?)").bind(at(0)).bind(at(0)).execute(db.pool()).await.unwrap();
    let queue = db
        .create_or_get_integration_queue("timing-repo", "main")
        .await
        .unwrap();
    let mut attempts = Vec::new();
    for index in 0..24 {
        let task = format!("timing-task-{index}");
        sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES(?,'timing-project',?,'merging',?,?)").bind(&task).bind(&task).bind(at(0)).bind(at(0)).execute(db.pool()).await.unwrap();
        attempts.push(
            db.admit_integration_attempt(db::IntegrationAttempt::new(
                Some(queue.id.clone()),
                task,
                "timing-project".into(),
                format!("admission-{index}"),
                "merging".into(),
                0,
                1,
            ))
            .await
            .unwrap()
            .id,
        );
    }
    // Twenty merges completed in the window: merge n waited n seconds in
    // line and spent 10 n seconds at the head. The even ones rebased and
    // ran a check; the odd ones found the target unmoved.
    for (index, attempt) in attempts.iter().take(20).enumerate() {
        let n = index as i64 + 1;
        let mut timings =
            json!({"rounds":1,"queued_ms":n*1000,"head_total_ms":n*10_000,"ff_ms":n*100});
        if n % 2 == 0 {
            timings["rebase_ms"] = json!(n * 200);
            timings["check"] = json!({"kind":"ran","slot_wait_ms":5,"run_ms":n*5000});
        } else {
            timings["check"] = json!({"kind":"skipped","reason":"target_unchanged"});
        }
        sqlx::query("UPDATE integration_attempt SET state='completed',current=0,completed_at=?,phase_timings_json=? WHERE id=?")
                .bind(at(60))
                .bind(timings.to_string())
                .bind(attempt)
                .execute(db.pool())
                .await
                .unwrap();
    }
    // One merge from before the window is not a sample.
    sqlx::query("UPDATE integration_attempt SET state='completed',current=0,completed_at=?,phase_timings_json=? WHERE id=?")
            .bind(at(2 * 24 * 60 * 60))
            .bind(json!({"rounds":1,"queued_ms":9_999_999,"head_total_ms":9_999_999}).to_string())
            .bind(&attempts[20])
            .execute(db.pool())
            .await
            .unwrap();
    // Now: one head for two minutes, one waiter for five, one parked for ten.
    sqlx::query("UPDATE integration_queue SET head_attempt_id=? WHERE id=?")
        .bind(&attempts[21])
        .bind(&queue.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE integration_attempt SET state='checking',started_at=? WHERE id=?")
        .bind(at(120))
        .bind(&attempts[21])
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE integration_attempt SET enqueued_at=? WHERE id=?")
        .bind(at(300))
        .bind(&attempts[22])
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE integration_attempt SET state='parked',updated_at=? WHERE id=?")
        .bind(at(600))
        .bind(&attempts[23])
        .execute(db.pool())
        .await
        .unwrap();

    let status = service.compute_status().await.unwrap().integration_queues;
    let near = |age: Option<i64>, expected: i64| {
        assert!(
            age.is_some_and(|age| (expected..expected + 30).contains(&age)),
            "{age:?} is not about {expected} s"
        );
    };
    assert_eq!(status.heads.count, 1);
    near(status.heads.oldest_age_seconds, 120);
    assert_eq!(status.waiting.count, 1);
    near(status.waiting.oldest_age_seconds, 300);
    assert_eq!(status.parked.count, 1);
    near(status.parked.oldest_age_seconds, 600);

    let timings = status.timings;
    assert_eq!(timings.window_seconds, 86_400);
    assert_eq!(timings.completed, 20);
    let percentiles = |samples, p50_ms, p95_ms, max_ms| api_types::IntegrationPercentiles {
        samples,
        p50_ms: Some(p50_ms),
        p95_ms: Some(p95_ms),
        max_ms: Some(max_ms),
    };
    // Nearest rank over 1..=20: the 10th, the 19th and the last.
    assert_eq!(timings.queue_wait, percentiles(20, 10_000, 19_000, 20_000));
    assert_eq!(
        timings.head_time,
        percentiles(20, 100_000, 190_000, 200_000)
    );
    assert_eq!(timings.fast_forward, percentiles(20, 1000, 1900, 2000));
    // The ten merges that rebased (n = 2, 4, .. 20): the 5th, the 10th.
    assert_eq!(timings.rebase, percentiles(10, 2000, 4000, 4000));
    assert_eq!(timings.check, percentiles(10, 50_000, 100_000, 100_000));

    // Nothing completed: every percentile is absent, not zero.
    sqlx::query("DELETE FROM integration_attempt WHERE state='completed'")
        .execute(db.pool())
        .await
        .unwrap();
    let empty = service
        .compute_status()
        .await
        .unwrap()
        .integration_queues
        .timings;
    assert_eq!(empty.completed, 0);
    assert_eq!(
        empty.queue_wait,
        api_types::IntegrationPercentiles::default()
    );
}

#[tokio::test]
async fn integration_status_counts_passive_queues_current_members_and_quarantined_imports() {
    let (db, service) = test_service().await;
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES('queue-project','queue-project',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO repo(id,project_id,name,default_branch,created_at,updated_at) VALUES('queue-repo','queue-project','queue-repo','main',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    for id in ["queue-task", "quarantined-task"] {
        sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES(?,'queue-project',?,'merging',?,?)").bind(id).bind(id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    }
    let queue = db
        .create_or_get_integration_queue("queue-repo", "main")
        .await
        .unwrap();
    let attempt = db::IntegrationAttempt::new(
        Some(queue.id),
        "queue-task".into(),
        "queue-project".into(),
        "admission".into(),
        "merging".into(),
        0,
        1,
    );
    db.admit_integration_attempt(attempt).await.unwrap();
    db.import_integration_pass(100).await.unwrap();
    let status = service.compute_status().await.unwrap();
    assert_eq!(status.integration_queues.queues_by_state["suspended"], 1);
    assert_eq!(status.integration_queues.queues_by_state["open"], 0);
    assert_eq!(
        status.integration_queues.current_attempts_by_state["queued"],
        1
    );
    // Only the Task without an attempt is imported (and quarantined: it
    // has no execution). The admitted Task's attempt is its record.
    assert_eq!(status.integration_queues.quarantined_imports, 1);
}

#[tokio::test]
async fn periodic_workers_are_listed_with_running_tick_error_and_restart_health() {
    let (db, service) = test_service().await;
    let registry = service.periodic_workers();
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let entered = Arc::new(tokio::sync::Notify::new());
    let handle = registry.worker("daemon-monitor").start(rx, || false, {
        let entered = Arc::clone(&entered);
        move |worker, _| {
            let entered = Arc::clone(&entered);
            async move {
                let _ = worker
                    .tick(async {
                        Err::<(), _>(ServiceError::invalid_operation("fixture tick failure"))
                    })
                    .await;
                entered.notify_one();
                std::future::pending::<crate::Result<()>>().await
            }
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    db::WorkerHealth::new(db, "daemon-monitor")
        .record_restart("fixture restart")
        .await
        .unwrap();
    sync_fixtures(&service).await;
    let status = service.compute_status().await.unwrap();
    assert_eq!(status.overall_severity, OperatorSeverity::Attention);
    assert!(status
        .recent_errors
        .iter()
        .any(|error| error.entity_id == "daemon-monitor"));
    let row = &status.periodic_workers[0];
    assert_eq!(row.worker_name, "daemon-monitor");
    assert!(row.running);
    assert!(row.last_tick_at.is_some());
    assert!(row.last_error.is_some());
    assert!(row.last_error_at.is_some());
    assert_eq!(row.restart_count, 1);
    handle.abort();
    let _ = handle.await;
}

#[tokio::test]
async fn machine_capacity_agent_pressure_excludes_expired_reservations() {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    sqlx::raw_sql("CREATE TABLE agent_current (id TEXT, name TEXT, daemon_id TEXT, max_concurrent_tasks INTEGER);
            CREATE TABLE workspace_placement (workspace_id TEXT, agent_id TEXT, state TEXT, reserved_until TEXT, updated_at TEXT);
            CREATE TABLE execution (workspace_id TEXT, agent_id TEXT, status TEXT);
            INSERT INTO agent_current VALUES ('agent','Agent',NULL,2);
            INSERT INTO workspace_placement VALUES ('expired','agent','reserved','2020-01-01T00:00:00Z',CURRENT_TIMESTAMP),
              ('orphan','agent','preparing',NULL,'2020-01-01T00:00:00Z'),
              ('live','agent','reserved','2099-01-01T00:00:00Z',CURRENT_TIMESTAMP);")
            .execute(&pool).await.unwrap();
    let service = OperatorStatusService::new_for_test(Arc::new(SqliteDb::new(pool)));
    let pressure = service.agent_pressure().await.unwrap();
    assert_eq!(pressure.len(), 1);
    assert_eq!(pressure[0].active_tasks, 1);
    assert!(!pressure[0].at_capacity);
}

#[tokio::test]
async fn machine_capacity_operations_uses_handle_identity() {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    sqlx::raw_sql(r#"CREATE TABLE daemon (id TEXT, hostname TEXT, machine_id TEXT, max_concurrent_runs INTEGER, run_limit INTEGER, removed_at TEXT);
            CREATE TABLE workspace_placement (task_id TEXT, workspace_id TEXT, agent_id TEXT, daemon_id TEXT, execution_daemon_id TEXT, state TEXT, reserved_until TEXT, updated_at TEXT);
            CREATE TABLE execution (task_id TEXT, workspace_id TEXT, agent_id TEXT, status TEXT, executor_config_snapshot_json TEXT);
            CREATE TABLE agent_current (id TEXT, daemon_id TEXT);
            CREATE TABLE agent_chat_turn_job (responder_identity_id TEXT, status TEXT);
            CREATE TABLE check_run (id TEXT, machine_id TEXT, state TEXT, admitted_at TEXT, created_at TEXT);
            CREATE TABLE check_consumer (task_id TEXT, status_epoch INTEGER, run_id TEXT, cancelled_at TEXT, origin TEXT);
            CREATE TABLE task (id TEXT, status_epoch INTEGER, deleted_at TEXT);
            INSERT INTO daemon VALUES ('provider','Host','capacity-test-host',100,NULL,NULL);
            INSERT INTO execution VALUES (NULL,NULL,NULL,'running','{"daemon_id":"provider"}');"#)
            .execute(&pool).await.unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    db.server_run_cap.set(Some(1), 1, "capacity-test-host");
    let service = OperatorStatusService::new_for_test(db.clone());
    let pressure = service.daemon_pressure().await.unwrap();
    assert_eq!(pressure.len(), 1);
    assert_eq!(pressure[0].daemon_id, "server_host");
    assert_eq!(pressure[0].active_runs, 1);
    assert_eq!(pressure[0].max_concurrent_runs, Some(1));
    assert!(pressure[0].at_capacity);
}

async fn seed_project_repo(db: &SqliteDb) -> (String, String) {
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let now = Utc::now().to_rfc3339();

    sqlx::query(
        "INSERT INTO project (id, name, settings, workflow_definition, created_at, updated_at)
             VALUES (?, ?, '{}', '{}', ?, ?)",
    )
    .bind(&project_id)
    .bind(format!("Project {project_id}"))
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("project inserts");

    sqlx::query(
            "INSERT INTO repo (id, project_id, name, remote_url, local_path, default_branch, created_at, updated_at)
             VALUES (?, ?, ?, ?, NULL, 'main', ?, ?)",
        )
        .bind(&repo_id)
        .bind(&project_id)
        .bind(format!("Repo {repo_id}"))
        .bind(format!("https://example.com/{repo_id}.git"))
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("repo inserts");
    sqlx::query("UPDATE project SET primary_repo_id = ? WHERE id = ?")
        .bind(&repo_id)
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project primary repo updates");

    (project_id, repo_id)
}

async fn insert_task(db: &SqliteDb, status: &str) -> String {
    let (project_id, _repo_id) = seed_project_repo(db).await;
    let task_id = new_uuid_v4();
    let now = Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO task (id, project_id, title, status, priority, created_at, updated_at)
             VALUES (?, ?, ?, ?, 0, ?, ?)",
    )
    .bind(&task_id)
    .bind(&project_id)
    .bind(format!("Task {task_id}"))
    .bind(status)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("task inserts");
    task_id
}

async fn insert_execution(
    db: &SqliteDb,
    task_id: &str,
    status: &str,
    error: Option<&str>,
    updated_at: DateTime<Utc>,
) -> String {
    let execution_id = new_uuid_v4();
    let now = updated_at.to_rfc3339();
    sqlx::query(
            "INSERT INTO execution (id, task_id, role, status, error, created_at, updated_at, stopped_at)
             VALUES (?, ?, 'executor', ?, ?, ?, ?, ?)",
        )
        .bind(&execution_id)
        .bind(task_id)
        .bind(status)
        .bind(error)
        .bind(&now)
        .bind(&now)
        .bind((status != "running").then_some(now.as_str()))
        .execute(db.pool())
        .await
        .expect("execution inserts");
    execution_id
}

async fn insert_rejected_transition(db: &SqliteDb, task_id: &str) {
    let transition_id = new_uuid_v4();
    let now = Utc::now().to_rfc3339();
    sqlx::query(
            "INSERT INTO transition_log (id, task_id, from_state, to_state, trigger_name, triggered_by, trigger_reason, rejection, created_at)
             VALUES (?, ?, 'review', 'in_progress', NULL, 'system', 'fixture rejection', 1, ?)",
        )
        .bind(&transition_id)
        .bind(task_id)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("transition log inserts");
}

async fn insert_daemon(db: &SqliteDb, status: &str) -> String {
    let daemon_id = new_uuid_v4();
    let now = Utc::now().to_rfc3339();
    sqlx::query(
            "INSERT INTO daemon (id, machine_id, hostname, os, arch, labels_json, status, detected_clis_json, created_at, updated_at)
             VALUES (?, ?, 'host', 'macos', 'aarch64', '{}', ?, '[]', ?, ?)",
        )
        .bind(&daemon_id)
        .bind(format!("machine-{daemon_id}"))
        .bind(status)
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("daemon inserts");
    daemon_id
}

async fn insert_workspace_at_path(
    db: &SqliteDb,
    task_id: &str,
    status: &str,
    cleanup_after: &str,
    worktree_path: &str,
) -> String {
    let repo_id = sqlx::query_scalar::<_, String>(
        "SELECT p.primary_repo_id
             FROM task t
             JOIN project p ON p.id = t.project_id
             WHERE t.id = ?",
    )
    .bind(task_id)
    .fetch_one(db.pool())
    .await
    .expect("project repo exists");
    let workspace_id = new_uuid_v4();
    let now = Utc::now().to_rfc3339();
    sqlx::query(
            "INSERT INTO workspace (id, task_id, repo_id, worktree_path, branch, status, cleanup_after, created_at, updated_at)
             VALUES (?, ?, ?, ?, 'main', ?, ?, ?, ?)",
        )
        .bind(&workspace_id)
        .bind(task_id)
        .bind(&repo_id)
        .bind(worktree_path)
        .bind(status)
        .bind(cleanup_after)
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("workspace inserts");
    workspace_id
}

async fn insert_workspace(db: &SqliteDb, task_id: &str, status: &str, cleanup_after: &str) {
    let workspace_path = format!("/tmp/worktree-{}", new_uuid_v4());
    insert_workspace_at_path(db, task_id, status, cleanup_after, &workspace_path).await;
}

#[tokio::test]
async fn notify_and_project_hook_workers_expose_health_and_live_lag() {
    let (db, operator) = test_service().await;
    let bus = Arc::new(events::EventBus::new(1));
    let notifications = Arc::new(crate::NotificationService::new(
        Arc::clone(&db),
        Arc::clone(&bus),
    ));
    let hooks = Arc::new(crate::ProjectHookService::new(
        Arc::clone(&db),
        Arc::clone(&bus),
        Arc::new(crate::TaskService::new(Arc::clone(&db), bus)),
        Arc::clone(&notifications),
    ));
    crate::worker_runtime::WorkerRuntime::new(Arc::clone(&db), notifications)
        .run_once(1)
        .await
        .unwrap();
    crate::worker_runtime::WorkerRuntime::new(Arc::clone(&db), hooks)
        .run_once(1)
        .await
        .unwrap();
    operator.set_runtime_workers(&[
        crate::RuntimeWorker::NotificationProjection,
        crate::RuntimeWorker::ProjectHooks,
    ]);
    let event_id = new_uuid_v4();
    db::DomainEventRepo::append_event(
        &*db,
        db::CreateDomainEvent::task_transition(
            event_id,
            "lag-task",
            "lag-project",
            "review",
            "done",
            Some("complete"),
            "system",
            "done",
            false,
            db::now_rfc3339(),
            serde_json::json!({}),
        ),
    )
    .await
    .unwrap();
    let status = operator.compute_status().await.unwrap();
    assert_eq!(status.event_consumers.len(), 2);
    for name in ["notifications", "project-hooks"] {
        let consumer = status
            .event_consumers
            .iter()
            .find(|consumer| consumer.consumer_name == name)
            .unwrap();
        assert_eq!(consumer.lag, 1);
        let subscription: Option<String> =
            sqlx::query_scalar("SELECT subscription_json FROM worker_health WHERE worker_name = ?")
                .bind(name)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert!(subscription.unwrap().contains("task.transitioned"));
    }
}

// A Task configured before v0.14 can name an auditor, which nothing runs
// any more. The owner is told which open Tasks still carry the key; a
// finished Task, or one whose key was removed, is not reported.
#[tokio::test]
async fn open_tasks_that_still_name_an_auditor_are_reported() {
    let (db, service) = test_service().await;
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES('p','p',?,?)")
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .unwrap();
    for (id, status, config) in [
        (
            "names-auditor",
            "review",
            Some(r#"{"review":{"auditor_agent_id":"agent"}}"#),
        ),
        (
            "names-auditor-flat",
            "todo",
            Some(r#"{"auditor_agent_id":"agent"}"#),
        ),
        (
            "finished",
            "done",
            Some(r#"{"review":{"auditor_agent_id":"agent"}}"#),
        ),
        (
            "removed",
            "review",
            Some(r#"{"review":{"auditor_agent_id":null,"ci_steps":[]}}"#),
        ),
        ("plain", "review", None),
    ] {
        sqlx::query("INSERT INTO task(id,project_id,title,status,task_state_config,created_at,updated_at) VALUES(?,'p',?,?,?,?,?)")
                .bind(id)
                .bind(id)
                .bind(status)
                .bind(config)
                .bind(&now)
                .bind(&now)
                .execute(db.pool())
                .await
                .unwrap();
    }
    sync_fixtures(&service).await;
    let status = service.compute_status().await.expect("status computes");
    let reported = status
        .recent_errors
        .iter()
        .filter(|issue| issue.entity_type == "retired_review_auditor_config")
        .collect::<Vec<_>>();
    assert_eq!(reported.len(), 1, "{:?}", status.recent_errors);
    assert_eq!(reported[0].severity, OperatorSeverity::Attention);
    assert!(
        reported[0].error.starts_with(
            "2 open Task(s) still name an auditor (names-auditor, names-auditor-flat)"
        ) && reported[0].error.contains("`reviewer` role"),
        "{}",
        reported[0].error
    );
    for id in ["finished", "removed", "plain"] {
        assert!(!reported[0].error.contains(id), "{}", reported[0].error);
    }

    // The same Tasks state it to their owner: one non-blocking notice
    // each in the Project's Attention feed, stated once.
    let notices = || async {
        sqlx::query_as::<_, (String, String, String, String)>(
                "SELECT dedupe_key, attention_type, status, json_extract(details_json,'$.task.id') FROM attention_projection WHERE dedupe_key LIKE 'retired-auditor:%' ORDER BY dedupe_key",
            )
            .fetch_all(db.pool())
            .await
            .unwrap()
    };
    assert_eq!(
        crate::task_service::state_retired_auditor_notices(&db)
            .await
            .unwrap(),
        2
    );
    let stated = notices().await;
    assert_eq!(
        stated,
        ["names-auditor", "names-auditor-flat"].map(|id| (
            crate::task_service::retired_auditor_notice_key(id),
            "review_risk".to_owned(),
            "acknowledged".to_owned(),
            id.to_owned()
        ))
    );
    // Listed to the owner like an open item, and no wake source: the
    // wake sweep reads `status = 'open'` only, so the Project Agent is
    // not woken for it and none of its wake budget is spent.
    // (The Attention list reads `status <> 'resolved'`.)
    let listed: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM attention_projection WHERE status <> 'resolved' AND (snoozed_until IS NULL OR snoozed_until <= ?)",
        )
        .bind(db::now_rfc3339())
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(listed, 2);
    let wake_sources: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM attention_projection WHERE status = 'open'")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(wake_sources, 0);
    assert_eq!(
        crate::task_service::state_retired_auditor_notices(&db)
            .await
            .unwrap(),
        0,
        "a notice is stated once"
    );
    let blocked: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task WHERE blocked_json IS NOT NULL OR error_annotation IS NOT NULL",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(blocked, 0, "the notice blocks nothing");

    sqlx::query("UPDATE task SET task_state_config = NULL")
        .execute(db.pool())
        .await
        .unwrap();
    let status = service.compute_status().await.expect("status computes");
    assert!(status
        .recent_errors
        .iter()
        .all(|issue| issue.entity_type != "retired_review_auditor_config"));
    assert_eq!(
        crate::task_service::state_retired_auditor_notices(&db)
            .await
            .unwrap(),
        0
    );
    assert!(notices()
        .await
        .iter()
        .all(|(_, _, status, _)| status == "resolved"));
}

#[tokio::test]
async fn a_failing_integration_import_is_an_operator_error_until_it_completes() {
    let (_db, service) = test_service().await;
    sync_fixtures(&service).await;
    service.set_integration_import_failure(Some("database is locked".into()));
    let status = service.compute_status().await.expect("status computes");
    let reported = status
        .recent_errors
        .iter()
        .filter(|issue| issue.entity_type == "integration_import")
        .collect::<Vec<_>>();
    assert_eq!(reported.len(), 1, "{:?}", status.recent_errors);
    assert_eq!(reported[0].severity, OperatorSeverity::Error);
    assert!(
        reported[0].error.contains("database is locked")
            && reported[0].error.contains("merge queue is not running"),
        "{}",
        reported[0].error
    );
    let since = reported[0].occurred_at.clone();
    // A later failure keeps the time of the first.
    service.set_integration_import_failure(Some("still failing".into()));
    let status = service.compute_status().await.expect("status computes");
    let again = status
        .recent_errors
        .iter()
        .find(|issue| issue.entity_type == "integration_import")
        .expect("still reported");
    assert_eq!(again.occurred_at, since);
    assert!(again.error.contains("still failing"));

    service.set_integration_import_failure(None);
    let status = service.compute_status().await.expect("status computes");
    assert!(status
        .recent_errors
        .iter()
        .all(|issue| issue.entity_type != "integration_import"));
}

#[tokio::test]
async fn empty_db_is_healthy() {
    let (_db, service) = test_service().await;

    sync_fixtures(&service).await;
    let status = service.compute_status().await.expect("status computes");

    assert_eq!(status.overall_severity, OperatorSeverity::Healthy);
    assert!(status.active_executions.is_empty());
    assert!(status.blocked_tasks.is_empty());
    assert!(status.daemon_issues.is_empty());
    assert!(status.workspace_cleanup.is_empty());
    assert!(status.retry_pressure.is_empty());
    assert!(status.recent_errors.is_empty());
}

/// The `checks` section: zero and silent on an idle server; a run that
/// waits for a slot and the check worker's last error are reported.
#[tokio::test]
async fn checks_section_reports_backlog_and_the_check_workers_last_error() {
    let (db, service) = test_service().await;
    sync_fixtures(&service).await;
    let idle = service
        .compute_status()
        .await
        .expect("status computes")
        .checks;
    assert_eq!(idle.queued, api_types::CheckBacklog::default());
    assert_eq!(idle.running, api_types::CheckBacklog::default());
    assert_eq!(idle.waiting_for_slot, api_types::CheckBacklog::default());
    assert_eq!(idle.waiting_for_disk, api_types::CheckBacklog::default());
    assert_eq!((idle.runs_24h, idle.reuse_hits_24h), (0, 0));
    assert!(idle.last_worker_error.is_none());

    let now = db::now_rfc3339();
    let waiting_since = (Utc::now() - chrono::Duration::seconds(300)).to_rfc3339();
    sqlx::raw_sql("INSERT INTO project(id,name,created_at,updated_at) VALUES('checks-p','p','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z'); INSERT INTO repo(id,project_id,name,default_branch,created_at,updated_at) VALUES('checks-r','checks-p','repo','main','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');").execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO check_run(id,project_id,repo_id,commit_sha,spec_digest,identity_key,input_json,cacheable,state,operation_id,created_at,updated_at,capacity_wait_since) VALUES('waiting','checks-p','checks-r','c','d','i','{}',1,'queued','op',?,?,?)")
            .bind(&waiting_since).bind(&now).bind(&waiting_since)
            .execute(db.pool()).await.unwrap();
    // The supervised worker has failed a tick.
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let entered = Arc::new(tokio::sync::Notify::new());
    let _handle = service
        .periodic_workers()
        .worker(crate::check_runner::worker::WORKER_NAME)
        .start(rx, || false, {
            let entered = Arc::clone(&entered);
            move |worker, _| {
                let entered = Arc::clone(&entered);
                async move {
                    let _ = worker
                        .tick(async {
                            Err::<(), _>(ServiceError::invalid_operation("owner port refused"))
                        })
                        .await;
                    entered.notify_one();
                    std::future::pending::<crate::Result<()>>().await
                }
            }
        });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let checks = service
        .compute_status()
        .await
        .expect("status computes")
        .checks;
    assert_eq!(checks.waiting_for_slot.count, 1);
    assert!(checks
        .waiting_for_slot
        .oldest_age_seconds
        .is_some_and(|age| (300..310).contains(&age)));
    assert_eq!(checks.queued.count, 0);
    assert_eq!(checks.runs_24h, 1);
    let error = checks.last_worker_error.expect("the worker's last error");
    assert!(
        error.error.contains("owner port refused"),
        "{}",
        error.error
    );
    assert!(!error.occurred_at.is_empty());
}

#[tokio::test]
async fn usage_summary_keeps_active_count_live_on_a_memo_hit() {
    let (_db, service) = test_service().await;
    let first = service.usage_summary(0).await.unwrap();
    let second = service.usage_summary(17).await.unwrap();
    assert_eq!(first.active_execution_count, 0);
    assert_eq!(second.active_execution_count, 17);
    assert_eq!(
        serde_json::to_value(first.cost).unwrap(),
        serde_json::to_value(second.cost).unwrap()
    );
}

#[tokio::test]
async fn running_execution_healthy() {
    let (db, service) = test_service().await;
    let task_id = insert_task(&db, "todo").await;
    insert_execution(&db, &task_id, "running", None, Utc::now()).await;

    sync_fixtures(&service).await;
    let status = service.compute_status().await.expect("status computes");

    assert_eq!(status.overall_severity, OperatorSeverity::Healthy);
    assert_eq!(status.active_executions.len(), 1);
    assert_eq!(status.active_executions[0].task_id, task_id);
}

#[tokio::test]
async fn running_execution_reports_plan_progress() {
    let workspace_parent = tempdir().expect("tempdir creates");
    let workspace_root = workspace_parent.path().join("worktree");
    fs::create_dir_all(&workspace_root).expect("workspace dir creates");
    fs::write(
        workspace_parent.path().join("plan.md"),
        "- [x] Task one\n- [x] Task two\n- [x] Task three\n- [ ] Task four\n- [ ] Task five\n",
    )
    .expect("plan writes");

    let (db, service) = test_service().await;
    let task_id = insert_task(&db, "in_progress").await;
    let execution_id = insert_execution(&db, &task_id, "running", None, Utc::now()).await;
    let cleanup_after = (Utc::now() + Duration::hours(1)).to_rfc3339();
    let workspace_id = insert_workspace_at_path(
        &db,
        &task_id,
        "ready",
        &cleanup_after,
        workspace_root.to_str().expect("workspace path is utf-8"),
    )
    .await;

    sqlx::query("UPDATE execution SET workspace_id = ? WHERE id = ?")
        .bind(&workspace_id)
        .bind(&execution_id)
        .execute(db.pool())
        .await
        .expect("execution workspace updates");

    sync_fixtures(&service).await;
    let status = service.compute_status().await.expect("status computes");

    assert_eq!(status.active_executions.len(), 1);
    let progress = status.active_executions[0]
        .plan_progress
        .as_ref()
        .expect("plan progress exists");
    assert_eq!(progress.total, 5);
    assert_eq!(progress.completed, 3);
    assert_eq!(progress.remaining, 2);
    assert!(progress.available);
}

#[tokio::test]
async fn blocked_task_severity() {
    let (db, service) = test_service().await;
    let task_id = insert_task(&db, "blocked").await;

    sync_fixtures(&service).await;
    let status = service.compute_status().await.expect("status computes");

    assert_eq!(status.overall_severity, OperatorSeverity::Blocked);
    assert_eq!(status.blocked_tasks.len(), 1);
    assert_eq!(status.blocked_tasks[0].task_id, task_id);
}

#[tokio::test]
async fn revision_two_daemon_waits_for_upgrade_and_operator_sees_action() {
    let (db, service) = test_service().await;
    let daemon_id = insert_daemon(&db, "online").await;
    let registry = Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
    let (connection, mut outbound) =
        crate::daemon_transport::DaemonConnection::new(daemon_id.clone());
    let id = connection.id();
    registry.register(daemon_id.clone(), connection);
    registry.dispatch_incoming_for_connection(&daemon_id, id, api_types::DaemonFrame::Notification {
            method: api_types::METHOD_DAEMON_HANDSHAKE.into(),
            params: serde_json::json!({"protocol_revision":2, "capabilities":["execution.terminal.usage_reports", "execution.terminal.ack"]}),
        });
    let api_types::DaemonFrame::Error { error, .. } = outbound.recv().await.unwrap() else {
        panic!("upgrade error expected")
    };
    assert_eq!(error.code, api_types::DAEMON_UPGRADE_REQUIRED);
    assert_eq!(error.message, api_types::DAEMON_UPGRADE_REQUIRED_MESSAGE);
    let result: Result<Value, ServiceError> = registry
        .send_request(
            &daemon_id,
            api_types::METHOD_EXECUTION_START,
            serde_json::json!({}),
            1,
        )
        .await;
    assert!(matches!(
        result,
        Err(ServiceError::DaemonUpgradeRequired { .. })
    ));
    assert!(outbound.try_recv().is_err());
    let service = service.with_daemon_connections(registry);
    service.set_runtime_workers(&crate::worker_runtime::assembly::COMMON_WORKERS);
    sqlx::query("INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type, scope_type, scope_id, correlation_id, created_at) VALUES ('upgrade-lag', 'test', 'test', 'test', 'system', 'system', 'system', 'test', '2000-01-01T00:00:00Z')").execute(db.pool()).await.unwrap();
    sync_fixtures(&service).await;
    let status = service.compute_status().await.unwrap();
    assert!(status
        .recent_errors
        .iter()
        .any(|issue| issue.entity_type == "event_consumer"));
    assert!(status.database.incremental_vacuum);
    let issue = status
        .daemon_issues
        .iter()
        .find(|issue| issue.daemon_id == daemon_id)
        .unwrap();
    assert_eq!(issue.issue, format!("upgrade_required: {}", error.message));
    assert_eq!(status.overall_severity, OperatorSeverity::Attention);
}

#[tokio::test]
async fn offline_daemon_attention() {
    let (db, service) = test_service().await;
    let daemon_id = insert_daemon(&db, "offline").await;

    sync_fixtures(&service).await;
    let status = service.compute_status().await.expect("status computes");

    assert_eq!(status.overall_severity, OperatorSeverity::Attention);
    assert_eq!(status.daemon_issues.len(), 1);
    assert_eq!(status.daemon_issues[0].daemon_id, daemon_id);
    assert_eq!(status.daemon_issues[0].issue, "offline");
    assert_eq!(
        status.daemon_issues[0].severity,
        OperatorSeverity::Attention
    );
}

#[tokio::test]
async fn error_daemon_reports_daemon_issue() {
    let (db, service) = test_service().await;
    // This fixture exercises a forward-compatible status value consumed by
    // operator status even though current daemon lifecycle only emits
    // online/offline.
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(db.pool())
        .await
        .expect("check constraints disabled for fixture");
    let daemon_id = insert_daemon(&db, "error").await;
    sqlx::query("PRAGMA ignore_check_constraints = OFF")
        .execute(db.pool())
        .await
        .expect("check constraints restored");

    sync_fixtures(&service).await;
    let status = service.compute_status().await.expect("status computes");

    assert_eq!(status.overall_severity, OperatorSeverity::Error);
    assert_eq!(status.daemon_issues.len(), 1);
    assert_eq!(status.daemon_issues[0].daemon_id, daemon_id);
    assert_eq!(status.daemon_issues[0].issue, "error");
    assert_eq!(status.daemon_issues[0].severity, OperatorSeverity::Error);
}

#[tokio::test]
async fn cleanup_backlog_attention() {
    let (db, service) = test_service().await;
    let task_id = insert_task(&db, "done").await;
    let cleanup_after = (Utc::now() - Duration::hours(1)).to_rfc3339();
    insert_workspace(&db, &task_id, "ready", &cleanup_after).await;

    sync_fixtures(&service).await;
    let status = service.compute_status().await.expect("status computes");

    assert_eq!(status.overall_severity, OperatorSeverity::Attention);
    assert_eq!(status.workspace_cleanup.len(), 1);
    assert_eq!(status.workspace_cleanup[0].task_id, task_id);
}

#[tokio::test]
async fn failed_plus_blocked_error() {
    let (db, service) = test_service().await;
    let blocked_task_id = insert_task(&db, "blocked").await;
    let failed_task_id = insert_task(&db, "todo").await;
    insert_execution(
        &db,
        &failed_task_id,
        "failed",
        Some("executor failed"),
        Utc::now(),
    )
    .await;

    sync_fixtures(&service).await;
    let status = service.compute_status().await.expect("status computes");

    assert_eq!(status.overall_severity, OperatorSeverity::Error);
    assert_eq!(status.blocked_tasks.len(), 1);
    assert_eq!(status.blocked_tasks[0].task_id, blocked_task_id);
    assert_eq!(status.recent_errors.len(), 1);
    assert_eq!(status.recent_errors[0].entity_type, "task");
    assert_eq!(status.recent_errors[0].entity_id, failed_task_id);
}

#[tokio::test]
async fn terminal_tasks_do_not_report_stale_execution_errors() {
    let (db, service) = test_service().await;
    let task_id = insert_task(&db, "done").await;
    insert_execution(
        &db,
        &task_id,
        "failed",
        Some("superseded by successful retry"),
        Utc::now(),
    )
    .await;

    sync_fixtures(&service).await;
    let status = service.compute_status().await.expect("status computes");

    assert!(status.recent_errors.is_empty());
    assert_eq!(status.overall_severity, OperatorSeverity::Healthy);
}

#[tokio::test]
async fn terminal_tasks_do_not_report_retry_pressure() {
    let (db, service) = test_service().await;
    let done_task_id = insert_task(&db, "done").await;
    let active_task_id = insert_task(&db, "review").await;
    insert_rejected_transition(&db, &done_task_id).await;
    insert_rejected_transition(&db, &active_task_id).await;

    sync_fixtures(&service).await;
    let status = service.compute_status().await.expect("status computes");

    assert_eq!(status.retry_pressure.len(), 1);
    assert_eq!(status.retry_pressure[0].task_id, active_task_id);
}

#[tokio::test]
async fn mixed_operational_indicators_use_highest_severity() {
    let (db, service) = test_service().await;
    let running_task_id = insert_task(&db, "in_progress").await;
    insert_execution(&db, &running_task_id, "running", None, Utc::now()).await;

    let blocked_task_id = insert_task(&db, "blocked").await;
    let daemon_id = insert_daemon(&db, "offline").await;

    let cleanup_task_id = insert_task(&db, "done").await;
    let cleanup_after = (Utc::now() - Duration::hours(1)).to_rfc3339();
    insert_workspace(&db, &cleanup_task_id, "ready", &cleanup_after).await;

    let failed_task_id = insert_task(&db, "todo").await;
    insert_execution(
        &db,
        &failed_task_id,
        "failed",
        Some("executor failed"),
        Utc::now(),
    )
    .await;

    sync_fixtures(&service).await;
    let status = service.compute_status().await.expect("status computes");

    assert_eq!(status.overall_severity, OperatorSeverity::Error);
    assert!(!status.active_executions.is_empty());
    assert!(!status.blocked_tasks.is_empty());
    assert_eq!(status.blocked_tasks[0].task_id, blocked_task_id);
    assert!(!status.daemon_issues.is_empty());
    assert_eq!(status.daemon_issues[0].daemon_id, daemon_id);
    assert!(!status.workspace_cleanup.is_empty());
}

#[tokio::test]
async fn blocked_annotation_preserves_workflow_phase_and_clears_independently() {
    let (db, service) = test_service().await;
    let task_id = insert_task(&db, "review").await;
    sqlx::query("UPDATE task SET blocked_json = ?, error_annotation = ? WHERE id = ?")
        .bind(r#"{"reason":"dependency unavailable"}"#)
        .bind("old diagnostic")
        .bind(&task_id)
        .execute(db.pool())
        .await
        .unwrap();
    sync_fixtures(&service).await;
    let status = service.compute_status().await.unwrap();
    assert_eq!(status.overall_severity, OperatorSeverity::Blocked);
    assert_eq!(status.blocked_tasks.len(), 1);
    assert_eq!(status.blocked_tasks[0].task_id, task_id);
    assert_eq!(
        status.blocked_tasks[0].blocked_reason.as_deref(),
        Some("old diagnostic")
    );
    let phase: String = sqlx::query_scalar("SELECT status FROM task WHERE id = ?")
        .bind(&task_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(phase, "review");
    sqlx::query("UPDATE task SET blocked_json = NULL WHERE id = ?")
        .bind(&task_id)
        .execute(db.pool())
        .await
        .unwrap();
    sync_fixtures(&service).await;
    assert!(service
        .compute_status()
        .await
        .unwrap()
        .blocked_tasks
        .is_empty());
}

#[tokio::test]
async fn inactive_tasks_do_not_report_stale_blocked_annotations() {
    let (db, service) = test_service().await;
    for phase in ["done", "cancelled", "review", "todo"] {
        let task_id = insert_task(&db, phase).await;
        sqlx::query(
            "UPDATE task SET blocked_json = '{\"reason\":\"old blocker\"}',
                 archived_at = CASE WHEN status = 'review' THEN ? ELSE NULL END,
                 deleted_at = CASE WHEN status = 'todo' THEN ? ELSE NULL END WHERE id = ?",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(Utc::now().to_rfc3339())
        .bind(&task_id)
        .execute(db.pool())
        .await
        .unwrap();
    }
    sync_fixtures(&service).await;
    assert!(service
        .compute_status()
        .await
        .unwrap()
        .blocked_tasks
        .is_empty());
}
#[tokio::test]
async fn consumer_lag_counts_live_rows_and_stalls_use_existing_attention_alerts() {
    let (db, service) = test_service().await;
    let now = Utc::now();
    let old = (now - Duration::minutes(10)).to_rfc3339();
    sqlx::query("INSERT INTO domain_event (sequence, id, event_type, entity_type, entity_id, actor_type, scope_type, scope_id, correlation_id, created_at) VALUES (1, 'processed', 'test', 'test', 'test', 'system', 'system', 'system', 'test', ?), (10, 'pending', 'test', 'test', 'test', 'system', 'system', 'system', 'test', ?)")
            .bind(&old).bind(&old).execute(db.pool()).await.unwrap();
    let consumers = service
        .expected_event_consumers
        .read()
        .expect("event consumer set lock")
        .clone();
    for consumer in consumers {
        sqlx::query("INSERT INTO event_consumer_cursor (consumer_name, last_sequence, updated_at) VALUES (?, 10, ?) ON CONFLICT(consumer_name) DO UPDATE SET last_sequence = 10, updated_at = excluded.updated_at")
                .bind(consumer).bind(&old).execute(db.pool()).await.unwrap();
    }
    sqlx::query("UPDATE event_consumer_cursor SET last_sequence = 1 WHERE consumer_name = 'attention_projection'").execute(db.pool()).await.unwrap();
    sync_fixtures(&service).await;
    let status = service.compute_status().await.unwrap();
    let stalled = status
        .event_consumers
        .iter()
        .find(|c| c.consumer_name == "attention_projection")
        .unwrap();
    assert_eq!(stalled.lag, 1); // Deleted sequence gaps do not count.
    assert!(stalled.stalled);
    assert!(stalled.oldest_unprocessed_age_seconds.unwrap() >= 600.0);
    assert_eq!(status.overall_severity, OperatorSeverity::Attention);
    assert_eq!(status.recent_errors.len(), 1);
    assert_eq!(status.recent_errors[0].entity_type, "event_consumer");
    assert_eq!(
        status.recent_errors[0].severity,
        OperatorSeverity::Attention
    );
    assert!(status.database.incremental_vacuum);
    assert_eq!(status.event_consumers.len(), 7);
    assert!(status
        .event_consumers
        .iter()
        .filter(|c| c.consumer_name != "attention_projection")
        .all(|c| c.lag == 0
            && !c.stalled
            && c.oldest_unprocessed_at.is_none()
            && c.last_advanced_at.as_deref() == Some(old.as_str())));
    sqlx::query("UPDATE event_consumer_cursor SET last_sequence = 10, updated_at = ? WHERE consumer_name = 'attention_projection'").bind(now.to_rfc3339()).execute(db.pool()).await.unwrap();
    sync_fixtures(&service).await;
    let recovered = service.compute_status().await.unwrap();
    assert_eq!(recovered.overall_severity, OperatorSeverity::Healthy);
    assert!(recovered.recent_errors.is_empty());
    assert!(recovered
        .event_consumers
        .iter()
        .all(|c| c.lag == 0 && !c.stalled));
}

#[tokio::test]
async fn consumer_migrated_health_reports_live_lag_and_age_without_worker_updates() {
    let (db, service) = test_service().await;
    service.set_runtime_workers(&[crate::worker_runtime::assembly::RuntimeWorker::Memory]);
    let now = Utc::now();
    let old = (now - Duration::minutes(10)).to_rfc3339();
    let name = crate::test_support::memory_consumer_name();
    sqlx::query("INSERT INTO event_consumer_cursor (consumer_name, last_sequence, updated_at) VALUES (?, 0, ?)")
            .bind(name).bind(&old).execute(db.pool()).await.unwrap();
    crate::test_support::initialize_agent_event_consumer(&db, name).await;
    // Model a worker that has been running for ten minutes; a newly
    // initialized worker gets time to drain an upgraded checkpoint.
    sqlx::query("UPDATE worker_health SET created_at = ? WHERE worker_name = ?")
        .bind(&old)
        .bind(name)
        .execute(db.pool())
        .await
        .unwrap();
    // No health update follows either append, as when a handler is hung.
    sqlx::query("INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type,
            scope_type, scope_id, correlation_id, created_at) VALUES
            ('wanted', 'agent_chat.message.admitted', 'test', 'test', 'system', 'system', 'system', 'test', ?),
            ('ignored', 'irrelevant', 'test', 'test', 'system', 'system', 'system', 'test', ?)")
            .bind(&old).bind(&old).execute(db.pool()).await.unwrap();
    let status = service.event_consumers(now).await.unwrap();
    let memory = status.iter().find(|c| c.consumer_name == name).unwrap();
    assert_eq!(memory.lag, 1);
    assert_eq!(memory.oldest_unprocessed_at.as_deref(), Some(old.as_str()));
    assert!(memory.oldest_unprocessed_age_seconds.unwrap() >= 600.0);
    assert!(memory.stalled);
    let later = service
        .event_consumers(now + Duration::minutes(1))
        .await
        .unwrap();
    let memory = later.iter().find(|c| c.consumer_name == name).unwrap();
    assert!(memory.oldest_unprocessed_age_seconds.unwrap() >= 660.0);
    crate::worker_runtime::WorkerHealth::new(Arc::clone(&db), name)
        .report_error("persistent worker failure")
        .await
        .unwrap();
    sync_fixtures(&service).await;
    let status = service.compute_status().await.unwrap();
    assert!(status
        .recent_errors
        .iter()
        .any(|issue| issue.entity_id == name && issue.error.contains("persistent worker failure")));
    assert_eq!(status.overall_severity, OperatorSeverity::Attention);
    crate::worker_runtime::WorkerHealth::new(Arc::clone(&db), name)
        .clear_error_if_set(db::HealthErrorKind::Runtime)
        .await
        .unwrap();
    sync_fixtures(&service).await;
    let status = service.compute_status().await.unwrap();
    assert!(!status
        .recent_errors
        .iter()
        .any(|issue| issue.error.contains("persistent worker failure")));
    // Skip has a lazy checkpoint. Simulate that subsequent flush to test
    // recovery against the sole cursor authority, not a health copy.
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    db.advance_domain_event_cursor_in_tx(&mut tx, name, 0, 2, &now.to_rfc3339())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    sync_fixtures(&service).await;
    let status = service.compute_status().await.unwrap();
    assert!(status.recent_errors.is_empty());
    assert_eq!(status.overall_severity, OperatorSeverity::Healthy);
}

#[tokio::test]
async fn consumer_deferral_text_and_dead_letters_have_distinct_operator_lifetimes() {
    let (db, service) = test_service().await;
    service.set_runtime_workers(&[crate::worker_runtime::assembly::RuntimeWorker::Memory]);
    let name = crate::test_support::memory_consumer_name();
    crate::test_support::initialize_agent_event_consumer(&db, name).await;
    let health = db::WorkerHealth::new(Arc::clone(&db), name);
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    health
        .defer_in_tx(
            &mut tx,
            "1",
            std::time::Duration::from_secs(3600),
            "waiting for settlement",
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();
    sync_fixtures(&service).await;
    let status = service.compute_status().await.unwrap();
    let issue = status
        .recent_errors
        .iter()
        .find(|i| i.entity_id == name)
        .unwrap();
    assert!(issue.error.contains("waiting for settlement"));
    assert!(issue.error.contains("Deferred since"));
    assert_eq!(issue.severity, OperatorSeverity::Healthy);
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    health.clear_pending_in_tx(&mut tx).await.unwrap();
    health
        .dead_letter_in_tx(
            &mut tx,
            db::WorkItem {
                source_key: "1",
                item_type: "test",
            },
            db::FailureState {
                attempts: 8,
                first_failed_at: "2000-01-01T00:00:00Z",
            },
            "poison event",
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();
    sync_fixtures(&service).await;
    let status = service.compute_status().await.unwrap();
    assert!(status
        .recent_errors
        .iter()
        .any(|i| i.entity_type == "worker_dead_letter" && i.error.contains("poison event")));
    sqlx::query("UPDATE worker_dead_letter SET dead_lettered_at = ?")
        .bind((Utc::now() - Duration::hours(2)).to_rfc3339())
        .execute(db.pool())
        .await
        .unwrap();
    sync_fixtures(&service).await;
    let status = service.compute_status().await.unwrap();
    assert!(status.recent_errors.is_empty());
    assert_eq!(status.overall_severity, OperatorSeverity::Healthy);
}

#[tokio::test]
async fn consumer_stall_threshold_missing_cursors_and_empty_outbox_are_visible() {
    let (db, service) = test_service().await;
    let now = Utc::now();
    let empty = service.event_consumers(now).await.unwrap();
    assert_eq!(empty.len(), 7);
    assert!(empty.iter().all(|c| c.lag == 0 && !c.stalled));
    let old = (now - Duration::seconds(301)).to_rfc3339();
    sqlx::query("INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type, scope_type, scope_id, correlation_id, created_at) VALUES ('old', 'test', 'test', 'test', 'system', 'system', 'system', 'test', ?)").bind(&old).execute(db.pool()).await.unwrap();
    let missing = service.event_consumers(now).await.unwrap();
    let consumer = missing
        .iter()
        .find(|c| c.consumer_name == "attention_projection")
        .unwrap();
    assert_eq!(consumer.lag, 1);
    assert_eq!(consumer.last_advanced_at, None);
    assert!(consumer.stalled);
    sqlx::query("INSERT INTO event_consumer_cursor (consumer_name, last_sequence, updated_at) VALUES ('attention_projection', 0, ?)").bind((now - Duration::seconds(300)).to_rfc3339()).execute(db.pool()).await.unwrap();
    let at_threshold = service.event_consumers(now).await.unwrap();
    assert!(
        !at_threshold
            .iter()
            .find(|c| c.consumer_name == "attention_projection")
            .unwrap()
            .stalled
    );
    let shorter = service
        .with_consumer_stall_seconds(299)
        .event_consumers(now)
        .await
        .unwrap();
    assert!(
        shorter
            .iter()
            .find(|c| c.consumer_name == "attention_projection")
            .unwrap()
            .stalled
    );
}

#[tokio::test]
async fn idle_consumer_with_old_cursor_is_never_stalled() {
    let (db, service) = test_service().await;
    service.set_runtime_workers(&[crate::worker_runtime::assembly::RuntimeWorker::Attention]);
    sqlx::query("INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type, scope_type, scope_id, correlation_id, created_at) VALUES ('processed', 'test', 'test', 'test', 'system', 'system', 'system', 'test', '2000-01-01T00:00:00Z')").execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO event_consumer_cursor (consumer_name, last_sequence, updated_at) VALUES ('attention_projection', 1, '2000-01-01T00:00:00Z')").execute(db.pool()).await.unwrap();
    sync_fixtures(&service).await;
    let status = service.compute_status().await.unwrap();
    assert_eq!(status.event_consumers.len(), 1);
    let consumer = &status.event_consumers[0];
    assert_eq!(consumer.lag, 0);
    assert!(!consumer.stalled);
    assert_eq!(consumer.oldest_unprocessed_age_seconds, None);
    assert_eq!(
        consumer.last_advanced_at.as_deref(),
        Some("2000-01-01T00:00:00Z")
    );
    assert!(status.recent_errors.is_empty());
    assert_eq!(status.overall_severity, OperatorSeverity::Healthy);

    // An event that just arrived is pending, not a stall, even though the
    // idle cursor's last advance is decades old.
    sqlx::query("INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type, scope_type, scope_id, correlation_id, created_at) VALUES ('fresh', 'test', 'test', 'test', 'system', 'system', 'system', 'test', ?)")
            .bind(Utc::now().to_rfc3339())
            .execute(db.pool())
            .await
            .unwrap();
    sync_fixtures(&service).await;
    let status = service.compute_status().await.unwrap();
    assert_eq!(status.event_consumers[0].lag, 1);
    assert!(!status.event_consumers[0].stalled);
    assert!(status.recent_errors.is_empty());
}
#[tokio::test]
async fn consumer_tail_has_no_durable_lag_entry_or_stall() {
    let (_, service) = test_service().await;
    service.set_runtime_workers(&[
        crate::worker_runtime::assembly::RuntimeWorker::DomainEventBroadcast,
    ]);
    assert!(service
        .compute_status()
        .await
        .unwrap()
        .event_consumers
        .is_empty());
}
#[tokio::test]
async fn audit2_dead_letter_history_does_not_expire_or_degrade_old_failures() {
    let (db, service) = test_service().await;
    service.set_runtime_workers(&[crate::worker_runtime::assembly::RuntimeWorker::Coordination]);
    let name = crate::coordination_consumer_name();
    let health = db::WorkerHealth::new(Arc::clone(&db), name);
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    health.ensure_in_tx(&mut tx).await.unwrap();
    for n in 0..7 {
        let key = format!("event:{n}:commitment:c-{n}");
        health
            .dead_letter_in_tx(
                &mut tx,
                db::WorkItem {
                    source_key: &key,
                    item_type: "coordination_commitment",
                },
                db::FailureState {
                    attempts: 0,
                    first_failed_at: "2000-01-01T00:00:00Z",
                },
                "domain rejection",
            )
            .await
            .unwrap();
    }
    sqlx::query("UPDATE worker_dead_letter SET dead_lettered_at = '2000-01-01T00:00:00Z'")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    sync_fixtures(&service).await;
    let status = service.compute_status().await.unwrap();
    let worker = &status.event_consumers[0];
    assert_eq!(worker.dead_letter_count, 7);
    assert_eq!(worker.recent_dead_letters.len(), 5);
    assert!(worker
        .recent_dead_letters
        .iter()
        .all(|item| !item.id.is_empty() && item.event_sequence.is_some()));
    let ids: Vec<_> = worker
        .recent_dead_letters
        .iter()
        .map(|item| item.id.clone())
        .collect();
    sync_fixtures(&service).await;
    let again = service.compute_status().await.unwrap();
    assert_eq!(
        again.event_consumers[0]
            .recent_dead_letters
            .iter()
            .map(|item| item.id.clone())
            .collect::<Vec<_>>(),
        ids
    );
    assert!(status.recent_errors.is_empty());
    assert_eq!(status.overall_severity, OperatorSeverity::Healthy);
}
#[tokio::test]
async fn resolved_dead_letters_are_excluded_from_counts_recent_history_and_alerts() {
    let (db, service) = test_service().await;
    service.set_runtime_workers(&[crate::worker_runtime::assembly::RuntimeWorker::Coordination]);
    let name = crate::coordination_consumer_name();
    let health = db::WorkerHealth::new(Arc::clone(&db), name);
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    health.ensure_in_tx(&mut tx).await.unwrap();
    health
        .dead_letter_in_tx(
            &mut tx,
            db::WorkItem {
                source_key: "12",
                item_type: "task.done",
            },
            db::FailureState {
                attempts: 8,
                first_failed_at: &db::now_rfc3339(),
            },
            "old failure",
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();
    sync_fixtures(&service).await;
    let status = service.compute_status().await.unwrap();
    assert_eq!(status.event_consumers[0].dead_letter_count, 1);
    assert_eq!(status.event_consumers[0].recent_dead_letters[0].attempts, 8);
    assert!(status
        .recent_errors
        .iter()
        .any(|error| error.entity_type == "worker_dead_letter"));
    let id = status.event_consumers[0].recent_dead_letters[0].id.clone();
    crate::dead_letter_service::DeadLetterService::new(db.clone())
        .dismiss(
            crate::dead_letter_service::DeadLetterActor {
                user_id: "admin",
                is_admin: true,
            },
            &id,
            None,
        )
        .await
        .unwrap();
    sync_fixtures(&service).await;
    let status = service.compute_status().await.unwrap();
    assert_eq!(status.event_consumers[0].dead_letter_count, 0);
    assert!(status.event_consumers[0].recent_dead_letters.is_empty());
    assert!(!status
        .recent_errors
        .iter()
        .any(|error| error.entity_type == "worker_dead_letter"));
}
#[tokio::test]
async fn task_step_queue_is_visible_with_counts_and_age() {
    use db::TaskStepRepo;
    let (db, service) = test_service().await;
    let now = db::now_rfc3339();
    sqlx::query(
        "INSERT INTO project(id,name,created_at,updated_at) VALUES ('queue-project','queue',?,?)",
    )
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES ('queue-task','queue-project','queue','todo',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    db.enqueue_step(&db::EnqueueTaskStep {
        kind: "cascade".into(),
        id: "queue-step".into(),
        task_id: "queue-task".into(),
        payload_json: "{}".into(),
        causation_step_id: None,
        causation_key: "queue".into(),
        chain_id: "queue".into(),
        chain_position: 1,
        expected_status: "todo".into(),
        expected_version: 1,
        expected_epoch: None,
        lane: "fast".into(),
        available_at: now.clone(),
    })
    .await
    .unwrap();
    // Resolved history does not count; a step the Task still points at does.
    for (id, status) in [("old-failure", "failed"), ("live-park", "parked")] {
        sqlx::query("INSERT INTO task_step(id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,status,available_at,created_at,updated_at,completed_at) VALUES (?,'queue-task',(SELECT MAX(seq)+1 FROM task_step),'cascade','{}',?,?,1,'todo',1,?,?,?,?,?)")
                .bind(id).bind(id).bind(id).bind(status).bind(&now).bind(&now).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    }
    sqlx::query("UPDATE task SET error_annotation=? WHERE id='queue-task'")
        .bind(r#"{"type":"workflow_loop","task_step_id":"live-park"}"#)
        .execute(db.pool())
        .await
        .unwrap();
    sync_fixtures(&service).await;
    let status = service.compute_status().await.unwrap();
    assert_eq!(status.task_steps.worker_name, "task_steps");
    assert_eq!(status.task_steps.pending, 1);
    assert_eq!(status.task_steps.claimed, 0);
    assert_eq!(status.task_steps.failed, 0);
    assert_eq!(status.task_steps.parked, 1);
    assert!(status.task_steps.oldest_pending_age_seconds.is_some());
}
