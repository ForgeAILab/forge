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
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let event_bus = Arc::new(EventBus::new(128));
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
