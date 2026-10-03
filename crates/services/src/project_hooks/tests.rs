use std::sync::Arc;

use api_types::{parse_project_hooks_json, ProjectHookAction, ProjectHookRule, ProjectHookTrigger};
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentRepo, AgentStatus,
    CreateAgent, CreateProject, CreateProjectHookRun, CreateTask, DaemonRepo, DaemonStatus,
    ProjectHookRun, ProjectHookRunRepo, ProjectHookRunStatus, ProjectRepo, SqliteDb, Task,
    TaskRepo, UpdateDaemonReport, UpdateTaskStatus, UpsertDaemon,
};
use events::EventBus;
use serde_json::json;
use tokio::time::{timeout, Duration};

use crate::{NotificationService, TaskService};

use super::{
    engine::ProjectHookEngine,
    triggers::{
        all_work_completed::{AllWorkCompletedTrigger, ALL_WORK_COMPLETED_TRIGGER_TYPE},
        HookTrigger, TriggerContext, TriggerMatch,
    },
    EvaluationCause, ProjectHookService,
};

#[test]
fn parse_project_hooks_json_rejects_unknown_trigger() {
    let error = parse_project_hooks_json(
        &json!([{
            "id": "unknown-trigger",
            "enabled": true,
            "name": "Unknown trigger",
            "trigger": { "type": "project.nope" },
            "filters": null,
            "action": {
                "type": "notify",
                "title": "Done",
                "message": "All work completed",
                "severity": null
            },
            "cooldown_seconds": null,
            "max_concurrent_runs": 1
        }])
        .to_string(),
    )
    .expect_err("unknown trigger is rejected");

    assert!(error.contains("unsupported project hook trigger type `project.nope`"));
}

#[test]
fn parse_project_hooks_json_rejects_task_stuck_until_persisted_signal_exists() {
    let error = parse_project_hooks_json(
        &json!([{
            "id": "stuck-trigger",
            "enabled": true,
            "name": "Stuck trigger",
            "trigger": { "type": "task.stuck" },
            "filters": null,
            "action": {
                "type": "notify",
                "title": "Stuck",
                "message": "Task appears stuck",
                "severity": null
            },
            "cooldown_seconds": null,
            "max_concurrent_runs": 1
        }])
        .to_string(),
    )
    .expect_err("task.stuck is rejected in v1");

    assert_eq!(
        error,
        "project hook rule at index 0 trigger requires a future persisted stuck signal"
    );
}

#[test]
fn parse_project_hooks_json_rejects_empty_required_action_fields() {
    let error = parse_project_hooks_json(
        &json!([{
            "id": "empty-title",
            "enabled": true,
            "name": "Empty title",
            "trigger": { "type": "project.all_work_completed" },
            "filters": null,
            "action": {
                "type": "create_task",
                "title": " ",
                "description": null,
                "task_type": null,
                "priority": null
            },
            "cooldown_seconds": null,
            "max_concurrent_runs": 1
        }])
        .to_string(),
    )
    .expect_err("empty create_task title is rejected");

    assert_eq!(
        error,
        "project hook rule `empty-title` create_task.title must be non-empty"
    );
}

#[tokio::test]
async fn concurrent_duplicate_evaluation_claims_one_run_and_executes_one_action() {
    let (db, service) = test_service().await;
    let project = seed_project(&db).await;
    let rule = create_task_rule("completion", "Concurrent hook follow-up", None, 1);
    let trigger_match = trigger_match("project.all_work_completed:1");

    let first_engine = ProjectHookEngine::new(&service);
    let second_engine = ProjectHookEngine::new(&service);
    let (first, second) = tokio::join!(
        first_engine.run(&project, rule.clone(), trigger_match.clone()),
        second_engine.run(&project, rule, trigger_match)
    );
    first.expect("first evaluator completes");
    second.expect("second evaluator completes");

    let runs = hook_runs(&db, &project.id).await;
    assert_eq!(
        runs.len(),
        1,
        "duplicate claim must not create a second run"
    );
    assert_eq!(runs[0].status, ProjectHookRunStatus::Completed);
    assert_eq!(
        task_count_by_title(&db, &project.id, "Concurrent hook follow-up").await,
        1,
        "only the winning evaluator executes the action"
    );
}

#[tokio::test]
async fn dispatch_agent_launch_failure_links_created_automation_task() {
    let (db, service) = test_service().await;
    let project = seed_project(&db).await;
    let agent_id = seed_available_agent(&db).await;
    let rule = ProjectHookRule {
        id: "dispatch".to_owned(),
        enabled: true,
        name: "Dispatch".to_owned(),
        trigger: ProjectHookTrigger::AllWorkCompleted,
        filters: None,
        action: ProjectHookAction::DispatchAgent {
            agent_id: agent_id.clone(),
            prompt: None,
            follow_up: None,
        },
        cooldown_seconds: None,
        max_concurrent_runs: 1,
    };

    ProjectHookEngine::new(&service)
        .run(
            &project,
            rule,
            trigger_match("project.all_work_completed:1"),
        )
        .await
        .expect("dispatch evaluation completes");

    let runs = hook_runs(&db, &project.id).await;
    assert_eq!(runs.len(), 1);
    let run = &runs[0];
    assert_eq!(run.status, ProjectHookRunStatus::Failed);
    assert_eq!(run.agent_id.as_deref(), Some(agent_id.as_str()));
    assert!(run.execution_id.is_none());
    assert!(run
        .reason
        .as_deref()
        .unwrap_or_default()
        .contains("execution launch failed"));
    let automation_task_id = run
        .automation_task_id
        .as_deref()
        .expect("failed launch still records the created automation task");
    let automation_task = TaskRepo::get_by_id(&*db, automation_task_id, false)
        .await
        .expect("automation task loads")
        .expect("automation task exists");
    assert!(automation_task.is_automation);
}

#[tokio::test]
async fn rule_inside_cooldown_records_skipped_run_without_action() {
    let (db, service) = test_service().await;
    let project = seed_project(&db).await;
    let rule = create_task_rule("completion", "Cooldown follow-up", Some(3600), 1);
    insert_hook_run(
        &db,
        &project.id,
        &rule.id,
        "project.all_work_completed:1",
        ProjectHookRunStatus::Completed,
        Some(now_rfc3339()),
    )
    .await;

    ProjectHookEngine::new(&service)
        .run(
            &project,
            rule,
            trigger_match("project.all_work_completed:2"),
        )
        .await
        .expect("cooldown evaluation completes");

    let runs = hook_runs(&db, &project.id).await;
    assert_eq!(runs.len(), 2);
    let skipped = runs
        .iter()
        .find(|run| run.dedupe_key == "project.all_work_completed:2")
        .expect("new dedupe run is recorded");
    assert_eq!(skipped.status, ProjectHookRunStatus::Skipped);
    assert!(
        skipped
            .reason
            .as_deref()
            .unwrap_or_default()
            .contains("cooldown"),
        "skip reason should mention cooldown: {:?}",
        skipped.reason
    );
    assert_eq!(
        task_count_by_title(&db, &project.id, "Cooldown follow-up").await,
        0
    );
}

#[tokio::test]
async fn rule_at_concurrency_limit_records_skipped_run_without_action() {
    let (db, service) = test_service().await;
    let project = seed_project(&db).await;
    let rule = create_task_rule("completion", "Concurrency follow-up", None, 2);
    insert_hook_run(
        &db,
        &project.id,
        &rule.id,
        "project.all_work_completed:1",
        ProjectHookRunStatus::Running,
        None,
    )
    .await;
    insert_hook_run(
        &db,
        &project.id,
        &rule.id,
        "project.all_work_completed:2",
        ProjectHookRunStatus::Running,
        None,
    )
    .await;

    ProjectHookEngine::new(&service)
        .run(
            &project,
            rule,
            trigger_match("project.all_work_completed:3"),
        )
        .await
        .expect("concurrency-limit evaluation completes");

    let runs = hook_runs(&db, &project.id).await;
    assert_eq!(runs.len(), 3);
    let skipped = runs
        .iter()
        .find(|run| run.dedupe_key == "project.all_work_completed:3")
        .expect("new dedupe run is recorded");
    assert_eq!(skipped.status, ProjectHookRunStatus::Skipped);
    let reason = skipped.reason.as_deref().unwrap_or_default();
    assert!(
        reason.contains("max_concurrent_runs") || reason.contains("concurrency"),
        "skip reason should mention the concurrency limit: {reason}"
    );
    assert_eq!(
        task_count_by_title(&db, &project.id, "Concurrency follow-up").await,
        0
    );
}

#[tokio::test]
async fn new_dedupe_key_permits_new_run_after_completed_run() {
    let (db, service) = test_service().await;
    let project = seed_project(&db).await;
    let rule = create_task_rule("completion", "Dedupe follow-up", None, 1);
    let engine = ProjectHookEngine::new(&service);

    engine
        .run(
            &project,
            rule.clone(),
            trigger_match("project.all_work_completed:1"),
        )
        .await
        .expect("first dedupe run completes");
    engine
        .run(
            &project,
            rule,
            trigger_match("project.all_work_completed:2"),
        )
        .await
        .expect("second dedupe run completes");

    let runs = hook_runs(&db, &project.id).await;
    assert_eq!(runs.len(), 2);
    assert!(runs
        .iter()
        .all(|run| run.status == ProjectHookRunStatus::Completed));
    assert_eq!(
        task_count_by_title(&db, &project.id, "Dedupe follow-up").await,
        2
    );
}

#[tokio::test]
async fn all_work_completed_ignores_running_automation_task_and_automation_does_not_advance_epoch()
{
    let (db, service) = test_service().await;
    let project = seed_project(&db).await;
    let done_task = seed_task(&db, &project.id, "done", false).await;
    let before_automation = ProjectRepo::get_by_id(&*db, &project.id)
        .await
        .expect("project loads")
        .expect("project exists");

    let automation_task = service
        .task_service
        .create_automation_task(
            project.id.clone(),
            "Automation: completion",
            Some("hook-run automation task".to_owned()),
            Some("task".to_owned()),
            None,
            None,
        )
        .await
        .expect("automation task creates");
    TaskRepo::update_status(
        &*db,
        UpdateTaskStatus {
            id: automation_task.id,
            expected_version: automation_task.version,
            status: "in_progress".to_owned(),
            assignee_id: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("automation task is marked running");

    let project_after_automation = ProjectRepo::get_by_id(&*db, &project.id)
        .await
        .expect("project loads")
        .expect("project exists");
    assert_eq!(
        project_after_automation.project_work_epoch, before_automation.project_work_epoch,
        "automation task creation must not advance project_work_epoch"
    );

    let cause = EvaluationCause::TaskTransitioned {
        task_id: done_task.id.clone(),
    };
    let trigger_context = TriggerContext {
        db: db.as_ref(),
        project: &project_after_automation,
        cause: &cause,
    };
    let trigger_match = AllWorkCompletedTrigger
        .evaluate(&trigger_context)
        .await
        .expect("trigger evaluates")
        .expect("automation task is excluded from completion eligibility");

    assert_eq!(trigger_match.trigger_type, ALL_WORK_COMPLETED_TRIGGER_TYPE);
    assert_eq!(
        trigger_match.dedupe_key,
        format!(
            "{}:{}",
            ALL_WORK_COMPLETED_TRIGGER_TYPE, project_after_automation.project_work_epoch
        )
    );
}

#[tokio::test]
async fn all_work_completed_matches_when_all_visible_tasks_are_cancelled() {
    let (db, _service) = test_service().await;
    let project = seed_project(&db).await;
    let cancelled_task = seed_task(&db, &project.id, "cancelled", false).await;
    let cause = EvaluationCause::TaskTransitioned {
        task_id: cancelled_task.id.clone(),
    };
    let trigger_context = TriggerContext {
        db: db.as_ref(),
        project: &project,
        cause: &cause,
    };

    let trigger_match = AllWorkCompletedTrigger
        .evaluate(&trigger_context)
        .await
        .expect("trigger evaluates")
        .expect("all-terminal visible work is eligible");

    assert_eq!(trigger_match.trigger_type, ALL_WORK_COMPLETED_TRIGGER_TYPE);
    assert_eq!(
        trigger_match.source_task_id.as_deref(),
        Some(cancelled_task.id.as_str())
    );
}

async fn test_service() -> (Arc<SqliteDb>, ProjectHookService) {
    test_service_with_event_capacity(128).await
}

async fn test_service_with_event_capacity(
    event_capacity: usize,
) -> (Arc<SqliteDb>, ProjectHookService) {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let event_bus = Arc::new(EventBus::new(event_capacity));
    let task_service = Arc::new(TaskService::new(Arc::clone(&db), Arc::clone(&event_bus)));
    let notification_service = Arc::new(NotificationService::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
    ));
    let service = ProjectHookService::new(
        Arc::clone(&db),
        event_bus,
        task_service,
        notification_service,
    );
    (db, service)
}

async fn seed_available_agent(db: &SqliteDb) -> String {
    let now = now_rfc3339();
    let daemon_id = new_uuid_v4();
    DaemonRepo::upsert_by_machine_id(
        db,
        UpsertDaemon {
            max_concurrent_runs: None,
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
        UpdateDaemonReport {
            max_concurrent_runs: None,
            id: daemon_id.clone(),
            last_report_at: now.clone(),
            status: DaemonStatus::Online,
            detected_clis_json: r#"[{"kind":"shell","availability":"authenticated"}]"#.to_owned(),
            labels_json: None,
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
            prompt_template: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("agent creates");
    agent_id
}

async fn seed_project(db: &SqliteDb) -> db::Project {
    let now = now_rfc3339();
    ProjectRepo::create(
        db,
        CreateProject {
            id: new_uuid_v4(),
            name: "Hooks".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("project creates")
}

async fn seed_task(db: &SqliteDb, project_id: &str, status: &str, is_automation: bool) -> Task {
    let now = now_rfc3339();
    TaskRepo::create(
        db,
        CreateTask {
            id: new_uuid_v4(),
            project_id: project_id.to_owned(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: format!("{status} task"),
            description: None,
            task_type: "task".to_owned(),
            status: status.to_owned(),
            is_automation,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("task creates")
}

fn create_task_rule(
    id: &str,
    title: &str,
    cooldown_seconds: Option<u64>,
    max_concurrent_runs: u8,
) -> ProjectHookRule {
    ProjectHookRule {
        id: id.to_owned(),
        enabled: true,
        name: "Completion hook".to_owned(),
        trigger: ProjectHookTrigger::AllWorkCompleted,
        filters: None,
        action: ProjectHookAction::CreateTask {
            title: title.to_owned(),
            description: Some("created by project hook".to_owned()),
            task_type: None,
            priority: None,
        },
        cooldown_seconds,
        max_concurrent_runs,
    }
}

fn trigger_match(dedupe_key: &str) -> TriggerMatch {
    TriggerMatch {
        trigger_type: ALL_WORK_COMPLETED_TRIGGER_TYPE.to_owned(),
        dedupe_key: dedupe_key.to_owned(),
        source_task_id: None,
        source_execution_id: None,
        reason: Some(format!("matched {dedupe_key}")),
    }
}

async fn insert_hook_run(
    db: &SqliteDb,
    project_id: &str,
    rule_id: &str,
    dedupe_key: &str,
    status: ProjectHookRunStatus,
    completed_at: Option<String>,
) -> ProjectHookRun {
    let now = now_rfc3339();
    ProjectHookRunRepo::try_claim(
        db,
        CreateProjectHookRun {
            id: new_uuid_v4(),
            project_id: project_id.to_owned(),
            rule_id: rule_id.to_owned(),
            trigger_type: ALL_WORK_COMPLETED_TRIGGER_TYPE.to_owned(),
            dedupe_key: dedupe_key.to_owned(),
            status,
            source_task_id: None,
            source_execution_id: None,
            automation_task_id: None,
            execution_id: None,
            agent_id: None,
            reason: Some("seeded run".to_owned()),
            created_at: now.clone(),
            updated_at: now,
            completed_at,
        },
    )
    .await
    .expect("hook run inserts")
    .expect("hook run is claimed")
}

async fn hook_runs(db: &SqliteDb, project_id: &str) -> Vec<ProjectHookRun> {
    ProjectHookRunRepo::list_recent_for_project(db, project_id, 20)
        .await
        .expect("hook runs load")
}

async fn task_count_by_title(db: &SqliteDb, project_id: &str, title: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM task WHERE project_id = ? AND title = ? AND deleted_at IS NULL",
    )
    .bind(project_id)
    .bind(title)
    .fetch_one(db.pool())
    .await
    .expect("task count loads")
}

#[tokio::test]
async fn start_with_shutdown_stops_and_releases_the_parent_receiver() {
    let (_db, service) = test_service().await;
    let event_bus = Arc::clone(&service.event_bus);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = Arc::new(service).start_with_shutdown(shutdown_rx);

    tokio::task::yield_now().await;
    shutdown_tx.send(true).expect("shutdown receiver is alive");

    timeout(Duration::from_secs(1), handle)
        .await
        .expect("project hook worker stops promptly")
        .expect("project hook worker joins");
    assert_eq!(event_bus.receiver_count(), 0);
}

#[tokio::test]
async fn worker_robustness_project_hook_receiver_continues_after_lag() {
    let (db, service) = test_service_with_event_capacity(1).await;
    let project = seed_project(&db).await;
    let task = seed_task(&db, &project.id, "done", false).await;
    let rules = serde_json::to_string(&vec![create_task_rule(
        "after-lag",
        "Created after lag",
        None,
        1,
    )])
    .unwrap();
    ProjectRepo::set_project_hooks_json(&*db, &project.id, &rules, &now_rfc3339())
        .await
        .unwrap();
    let event_bus = Arc::clone(&service.event_bus);
    let mut receiver = event_bus.subscribe();
    for entity_id in ["first", "second"] {
        event_bus.publish(events::ForgeEvent {
            event_type: "test.event".to_owned(),
            entity_id: entity_id.to_owned(),
            timestamp: events::event_timestamp(),
            context: events::EventContext::Empty {},
        });
    }
    assert!(matches!(
        receiver.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_))
    ));
    // Ignore fixture creation so this proves transition delivery itself.
    sqlx::query("UPDATE event_consumer_cursor SET last_sequence = (SELECT MAX(sequence) FROM domain_event) WHERE consumer_name = 'project-hooks'").execute(db.pool()).await.unwrap();
    let event = append_transition(&db, &task).await;
    let service = Arc::new(service);
    let runtime = crate::worker_runtime::WorkerRuntime::new(Arc::clone(&db), Arc::clone(&service));
    assert_eq!(runtime.run_once(100).await.unwrap(), 1);

    timeout(Duration::from_secs(10), async {
        loop {
            if task_count_by_title(&db, &project.id, "Created after lag").await == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("post-lag event is delivered");
    assert_eq!(
        crate::worker_runtime::WorkerRuntime::new(Arc::clone(&db), Arc::clone(&service))
            .run_once(100)
            .await
            .unwrap(),
        0
    );
    assert_eq!(hook_runs(&db, &project.id).await.len(), 1);
    assert!(
        db::DomainEventRepo::get_consumer_cursor(&*db, "project-hooks")
            .await
            .unwrap()
            .unwrap()
            .last_sequence
            >= event.sequence
    );
}

async fn append_transition(db: &SqliteDb, task: &Task) -> db::DomainEvent {
    db::DomainEventRepo::append_event(
        db,
        db::CreateDomainEvent::task_transition(
            new_uuid_v4(),
            &task.id,
            &task.project_id,
            "review",
            "done",
            Some("complete"),
            "system",
            "completed",
            false,
            now_rfc3339(),
            json!({}),
        ),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn durable_database_hook_and_cursor_commit_atomically_before_publication() {
    use crate::worker_runtime::{Outcome, Worker, WorkerRuntime};
    let (db, service) = test_service().await;
    let project = seed_project(&db).await;
    let task = seed_task(&db, &project.id, "done", false).await;
    let mut rule = create_task_rule("notify", "unused", None, 1);
    rule.action = ProjectHookAction::Notify {
        title: "Finished".to_owned(),
        message: "All work completed".to_owned(),
        severity: Some("info".to_owned()),
    };
    ProjectRepo::set_project_hooks_json(
        &*db,
        &project.id,
        &serde_json::to_string(&vec![rule]).unwrap(),
        &now_rfc3339(),
    )
    .await
    .unwrap();
    let event = append_transition(&db, &task).await;
    let Outcome::Done(prepared) = service.handle(&event).await.unwrap() else {
        panic!("hook matches");
    };
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    service.commit(&mut tx, &event, &prepared).await.unwrap();
    tx.rollback().await.unwrap();
    assert!(hook_runs(&db, &project.id).await.is_empty());
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    service.commit(&mut tx, &event, &prepared).await.unwrap();
    db.advance_domain_event_cursor_in_tx(
        &mut tx,
        "project-hooks",
        0,
        event.sequence,
        &now_rfc3339(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    // Crash here: no after_commit call. Restart cannot repeat the DB action.
    let service = Arc::new(service);
    assert_eq!(
        WorkerRuntime::new(Arc::clone(&db), service)
            .run_once(100)
            .await
            .unwrap(),
        0
    );
    let notifications: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM notification WHERE event_type = 'project_hook.notify'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(notifications, 1);
    assert_eq!(
        hook_runs(&db, &project.id).await[0].status,
        ProjectHookRunStatus::Completed
    );
}

#[tokio::test]
async fn durable_external_started_marker_is_not_relaunched_after_restart() {
    use crate::worker_runtime::{Outcome, Worker, WorkerRuntime};
    let (db, service) = test_service().await;
    let project = seed_project(&db).await;
    let task = seed_task(&db, &project.id, "done", false).await;
    let mut rule = create_task_rule("dispatch", "unused", None, 1);
    rule.action = ProjectHookAction::DispatchAgent {
        agent_id: seed_available_agent(&db).await,
        prompt: Some("unchanged prompt".to_owned()),
        follow_up: None,
    };
    ProjectRepo::set_project_hooks_json(
        &*db,
        &project.id,
        &serde_json::to_string(&vec![rule]).unwrap(),
        &now_rfc3339(),
    )
    .await
    .unwrap();
    let event = append_transition(&db, &task).await;
    let Outcome::Done(prepared) = service.handle(&event).await.unwrap() else {
        panic!("hook matches");
    };
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    service.commit(&mut tx, &event, &prepared).await.unwrap();
    db.advance_domain_event_cursor_in_tx(
        &mut tx,
        "project-hooks",
        0,
        event.sequence,
        &now_rfc3339(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let service = Arc::new(service);
    WorkerRuntime::new(Arc::clone(&db), Arc::clone(&service))
        .run_once(100)
        .await
        .unwrap();
    // Even an explicit duplicate evaluation cannot admit the same external run.
    service
        .evaluate_for_project(
            &project.id,
            EvaluationCause::TaskTransitioned { task_id: task.id },
        )
        .await
        .unwrap();
    let runs = hook_runs(&db, &project.id).await;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].status, ProjectHookRunStatus::Running);
    assert!(runs[0].automation_task_id.is_some());
    assert!(runs[0].execution_id.is_none());
    let tasks: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task WHERE project_id = ? AND is_automation = 1")
            .bind(&project.id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(tasks, 1);
}

#[tokio::test]
async fn upgrade_head_seed_prevents_historical_hook_delivery() {
    let (db, service) = test_service().await;
    let project = seed_project(&db).await;
    let task = seed_task(&db, &project.id, "done", false).await;
    let mut rule = create_task_rule("old-completion", "unused", None, 1);
    rule.action = ProjectHookAction::Notify {
        title: "Finished".to_owned(),
        message: "All work completed".to_owned(),
        severity: None,
    };
    ProjectRepo::set_project_hooks_json(
        &*db,
        &project.id,
        &serde_json::to_string(&vec![rule]).unwrap(),
        &now_rfc3339(),
    )
    .await
    .unwrap();
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    for index in 0..3000 {
        let event = db::CreateDomainEvent::task_transition(
            format!("old-transition-{index}"),
            &task.id,
            &project.id,
            "review",
            "done",
            Some("complete"),
            "system",
            "completed",
            false,
            now_rfc3339(),
            json!({}),
        );
        db::DomainEventRepo::append_event_in_tx(&*db, &mut tx, &event)
            .await
            .unwrap();
    }
    sqlx::query("DELETE FROM event_consumer_cursor WHERE consumer_name IN ('project-hooks', 'notifications')").execute(&mut *tx).await.unwrap();
    sqlx::raw_sql(
        include_str!("../../../db/migrations/V202610030200__notify_hooks_consumers.sql")
            .split("-- These mutations")
            .next()
            .unwrap(),
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let service = Arc::new(service);
    assert_eq!(
        crate::worker_runtime::WorkerRuntime::new(Arc::clone(&db), Arc::clone(&service))
            .run_once(100)
            .await
            .unwrap(),
        0
    );
    assert!(hook_runs(&db, &project.id).await.is_empty());
    append_transition(&db, &task).await;
    assert_eq!(
        crate::worker_runtime::WorkerRuntime::new(Arc::clone(&db), service)
            .run_once(100)
            .await
            .unwrap(),
        1
    );
    assert_eq!(hook_runs(&db, &project.id).await.len(), 1);
}

#[tokio::test]
async fn durable_rules_recheck_completion_after_a_rule_creates_visible_work() {
    let (db, service) = test_service().await;
    let project = seed_project(&db).await;
    let task = seed_task(&db, &project.id, "done", false).await;
    let create = create_task_rule("create-first", "New visible work", None, 1);
    let mut notify = create_task_rule("notify-second", "unused", None, 1);
    notify.action = ProjectHookAction::Notify {
        title: "Finished".to_owned(),
        message: "All work completed".to_owned(),
        severity: None,
    };
    ProjectRepo::set_project_hooks_json(
        &*db,
        &project.id,
        &serde_json::to_string(&vec![create, notify]).unwrap(),
        &now_rfc3339(),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE event_consumer_cursor SET last_sequence = (SELECT MAX(sequence) FROM domain_event) WHERE consumer_name = 'project-hooks'").execute(db.pool()).await.unwrap();
    append_transition(&db, &task).await;
    crate::worker_runtime::WorkerRuntime::new(Arc::clone(&db), Arc::new(service))
        .run_once(100)
        .await
        .unwrap();
    assert_eq!(
        task_count_by_title(&db, &project.id, "New visible work").await,
        1
    );
    assert_eq!(hook_runs(&db, &project.id).await.len(), 1);
    let notifications: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM notification WHERE event_type = 'project_hook.notify'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(notifications, 0);
}

#[tokio::test]
async fn durable_hook_created_task_keeps_project_default_roles_and_current_version() {
    let (db, service) = test_service().await;
    let project = seed_project(&db).await;
    let settings = json!({"default_role_assignments": [
        {"role_name": "coder", "assignee_type": "user", "assignee_id": "hook-owner"},
        {"role_name": "reviewer", "assignee_type": "user", "assignee_id": "hook-reviewer"}
    ]});
    sqlx::query("UPDATE project SET settings = ? WHERE id = ?")
        .bind(settings.to_string())
        .bind(&project.id)
        .execute(db.pool())
        .await
        .unwrap();
    let project = ProjectRepo::get_by_id(&*db, &project.id)
        .await
        .unwrap()
        .unwrap();
    ProjectHookEngine::new(&service)
        .run(
            &project,
            create_task_rule("default-roles", "Follow-up with defaults", None, 1),
            trigger_match("project.all_work_completed:1"),
        )
        .await
        .unwrap();
    let roles: Vec<(String, String)> = sqlx::query_as("SELECT a.role_name, a.assignee_id FROM task_role_assignment a JOIN task t ON t.id = a.task_id WHERE t.project_id = ? ORDER BY a.role_name").bind(&project.id).fetch_all(db.pool()).await.unwrap();
    assert_eq!(
        roles,
        vec![
            ("coder".to_owned(), "hook-owner".to_owned()),
            ("reviewer".to_owned(), "hook-reviewer".to_owned())
        ]
    );
    let version: i64 = sqlx::query_scalar(
        "SELECT version FROM task WHERE project_id = ? AND title = 'Follow-up with defaults'",
    )
    .bind(&project.id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(version, 3);
}

#[tokio::test]
async fn durable_comments_commit_before_publication_and_keep_missing_target_reason() {
    use crate::worker_runtime::{Outcome, Worker, WorkerRuntime};
    let (db, service) = test_service().await;
    let project = seed_project(&db).await;
    let task = seed_task(&db, &project.id, "done", false).await;
    let mut comment = create_task_rule("comment", "unused", None, 1);
    comment.action = ProjectHookAction::AddComment {
        target_task_id: None,
        content: "Saved comment".to_owned(),
    };
    let mut missing = create_task_rule("missing", "unused", None, 1);
    missing.action = ProjectHookAction::AddComment {
        target_task_id: Some("missing-target".to_owned()),
        content: "Uncreated comment".to_owned(),
    };
    ProjectRepo::set_project_hooks_json(
        &*db,
        &project.id,
        &serde_json::to_string(&vec![comment, missing]).unwrap(),
        &now_rfc3339(),
    )
    .await
    .unwrap();
    let event = append_transition(&db, &task).await;
    let Outcome::Done(prepared) = service.handle(&event).await.unwrap() else {
        panic!("hook matches");
    };
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    service.commit(&mut tx, &event, &prepared).await.unwrap();
    db.advance_domain_event_cursor_in_tx(
        &mut tx,
        "project-hooks",
        0,
        event.sequence,
        &now_rfc3339(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    // Crash before either post-commit publication or memory indexing.
    WorkerRuntime::new(Arc::clone(&db), Arc::new(service))
        .run_once(100)
        .await
        .unwrap();
    let comments: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_comment WHERE task_id = ?")
        .bind(&task.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(comments, 1);
    let runs = hook_runs(&db, &project.id).await;
    assert_eq!(runs.len(), 2);
    let failed = runs.iter().find(|run| run.rule_id == "missing").unwrap();
    assert_eq!(failed.status, ProjectHookRunStatus::Failed);
    assert_eq!(
        failed.reason.as_deref(),
        Some(
            crate::ServiceError::not_found("task", "missing-target")
                .to_string()
                .as_str()
        )
    );
}

async fn started_external_run(
    db: &Arc<SqliteDb>,
    service: &ProjectHookService,
    project: &db::Project,
) -> (super::engine::PreparedHook, super::engine::CommittedHook) {
    let mut rule = create_task_rule("recover-dispatch", "unused", None, 1);
    rule.action = ProjectHookAction::DispatchAgent {
        agent_id: seed_available_agent(db).await,
        prompt: None,
        follow_up: None,
    };
    let engine = ProjectHookEngine::new(service);
    let prepared = engine
        .prepare(
            project,
            rule,
            trigger_match("project.all_work_completed:recover"),
        )
        .await
        .unwrap();
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    let committed = engine.commit(&mut tx, &prepared).await.unwrap().unwrap();
    tx.commit().await.unwrap();
    (prepared, committed)
}

async fn age_started_run(db: &SqliteDb, project: &str) {
    sqlx::query(
        "UPDATE project_hook_run SET updated_at = ? WHERE project_id = ? AND status = 'running'",
    )
    .bind((chrono::Utc::now() - chrono::Duration::minutes(11)).to_rfc3339())
    .bind(project)
    .execute(db.pool())
    .await
    .unwrap();
}

#[tokio::test]
async fn tick_fails_abandoned_dispatch_without_replaying_and_releases_rule_limit() {
    use crate::worker_runtime::Worker;
    let (db, service) = test_service().await;
    let project = seed_project(&db).await;
    started_external_run(&db, &service, &project).await;
    service.tick().await.unwrap();
    assert_eq!(
        hook_runs(&db, &project.id).await[0].status,
        ProjectHookRunStatus::Running,
        "fresh launch retains grace"
    );
    age_started_run(&db, &project.id).await;
    let mut hints = service.event_bus.subscribe();
    service.tick().await.unwrap();
    let runs = hook_runs(&db, &project.id).await;
    assert_eq!(runs[0].status, ProjectHookRunStatus::Failed);
    assert!(runs[0].completed_at.is_some());
    assert!(runs[0]
        .reason
        .as_deref()
        .unwrap()
        .contains("without an execution record"));
    assert_eq!(
        ProjectHookRunRepo::count_active_for_rule(&*db, &project.id, "recover-dispatch")
            .await
            .unwrap(),
        0
    );
    assert!(
        matches!(hints.try_recv().unwrap().context, events::EventContext::ProjectHookRunChanged { status, .. } if status == "failed")
    );
    service.tick().await.unwrap();
    assert!(hints.try_recv().is_err(), "settled run is not swept again");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution WHERE task_id = ?")
        .bind(&runs[0].automation_task_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn tick_recovers_successful_dispatch_after_status_write_failed() {
    use crate::worker_runtime::Worker;
    let (db, service) = test_service().await;
    let project = seed_project(&db).await;
    let (_, committed) = started_external_run(&db, &service, &project).await;
    let run = hook_runs(&db, &project.id).await.remove(0);
    let execution = new_uuid_v4();
    sqlx::query("INSERT INTO execution (id, task_id, role, status, created_at, updated_at) VALUES (?, ?, 'executor', 'running', ?, ?)")
        .bind(&execution).bind(&run.automation_task_id).bind(now_rfc3339()).bind(now_rfc3339()).execute(db.pool()).await.unwrap();
    sqlx::raw_sql("CREATE TRIGGER reject_dispatch_status BEFORE UPDATE OF status ON project_hook_run WHEN NEW.status = 'dispatched' BEGIN SELECT RAISE(ABORT, 'injected status failure'); END;").execute(db.pool()).await.unwrap();
    let engine = ProjectHookEngine::new(&service);
    assert!(engine
        .finish_external_launch(
            &committed,
            async { Ok(execution.clone()) },
            Duration::from_secs(1)
        )
        .await
        .is_err());
    assert_eq!(
        hook_runs(&db, &project.id).await[0].status,
        ProjectHookRunStatus::Running
    );
    sqlx::query("DROP TRIGGER reject_dispatch_status")
        .execute(db.pool())
        .await
        .unwrap();
    age_started_run(&db, &project.id).await;
    service.tick().await.unwrap();
    let repaired = hook_runs(&db, &project.id).await.remove(0);
    assert_eq!(repaired.status, ProjectHookRunStatus::Dispatched);
    assert_eq!(repaired.execution_id.as_deref(), Some(execution.as_str()));
    assert!(repaired
        .reason
        .as_deref()
        .unwrap()
        .contains("launch was not replayed"));
    assert_eq!(
        ProjectHookRunRepo::count_active_for_rule(&*db, &project.id, "recover-dispatch")
            .await
            .unwrap(),
        0
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution WHERE task_id = ?")
        .bind(&run.automation_task_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn slow_external_launch_is_bounded_and_later_hook_can_publish() {
    let (db, service) = test_service().await;
    let project = seed_project(&db).await;
    let (_, committed) = started_external_run(&db, &service, &project).await;
    let engine = ProjectHookEngine::new(&service);
    let result = timeout(
        Duration::from_secs(1),
        engine.finish_external_launch(
            &committed,
            std::future::pending::<crate::Result<String>>(),
            Duration::from_millis(10),
        ),
    )
    .await
    .unwrap();
    assert!(result.unwrap_err().to_string().contains("timed out"));
    let marker = hook_runs(&db, &project.id).await.remove(0);
    assert_eq!(marker.status, ProjectHookRunStatus::Running);
    assert!(marker
        .reason
        .as_deref()
        .unwrap()
        .contains("launch will not be replayed"));
    let mut notify = create_task_rule("later-rule", "unused", None, 1);
    notify.action = ProjectHookAction::Notify {
        title: "Later hook".to_owned(),
        message: "Still delivered".to_owned(),
        severity: None,
    };
    let mut hints = service.event_bus.subscribe();
    engine
        .run(
            &project,
            notify,
            trigger_match("project.all_work_completed:recover"),
        )
        .await
        .unwrap();
    let mut notified = false;
    while let Ok(event) = hints.try_recv() {
        notified |= matches!(event.context, events::EventContext::NotificationCreated { title, .. } if title == "Later hook");
    }
    assert!(notified);
}

struct FirstTriggerReadFails(std::sync::atomic::AtomicBool);
#[async_trait::async_trait]
impl HookTrigger for FirstTriggerReadFails {
    async fn evaluate(&self, context: &TriggerContext<'_>) -> crate::Result<Option<TriggerMatch>> {
        if self.0.swap(false, std::sync::atomic::Ordering::SeqCst) {
            return Err(db::DbError::Sqlx(sqlx::Error::ColumnNotFound(
                "injected trigger read failure".to_owned(),
            ))
            .into());
        }
        AllWorkCompletedTrigger.evaluate(context).await
    }
}

#[tokio::test]
async fn failed_trigger_read_records_one_rule_and_continues_to_the_next() {
    let (db, service) = test_service().await;
    let project = seed_project(&db).await;
    let task = seed_task(&db, &project.id, "done", false).await;
    let rules = ["failed-read", "healthy-rule"].map(|id| {
        let mut rule = create_task_rule(id, "unused", None, 1);
        rule.action = ProjectHookAction::Notify {
            title: id.to_owned(),
            message: "Completed".to_owned(),
            severity: None,
        };
        rule
    });
    let prepared = super::evaluator::prepare_rules(
        &service,
        &project,
        EvaluationCause::TaskTransitioned { task_id: task.id },
        "read-failure-event",
        rules.into(),
        &FirstTriggerReadFails(std::sync::atomic::AtomicBool::new(true)),
    )
    .await
    .unwrap();
    let engine = ProjectHookEngine::new(&service);
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    for rule in &prepared {
        assert!(engine.still_matches(&mut tx, rule).await.unwrap());
        engine.commit(&mut tx, rule).await.unwrap();
    }
    tx.commit().await.unwrap();
    let runs = hook_runs(&db, &project.id).await;
    assert_eq!(runs.len(), 2);
    assert_eq!(
        runs.iter()
            .find(|run| run.rule_id == "failed-read")
            .unwrap()
            .status,
        ProjectHookRunStatus::Failed
    );
    assert_eq!(
        runs.iter()
            .find(|run| run.rule_id == "healthy-rule")
            .unwrap()
            .status,
        ProjectHookRunStatus::Completed
    );
    let titles: Vec<String> = sqlx::query_scalar("SELECT title FROM notification")
        .fetch_all(db.pool())
        .await
        .unwrap();
    assert_eq!(titles, vec!["healthy-rule"]);
}
