//! Condition-only targeted reproductions for queueing, stored data and send-back.
use super::helpers::*;
use super::*;
use api_types::TaskAction;
use std::fmt::Write as _;

#[derive(Default)]
struct ConditionScenarioExecutor {
    inputs: std::sync::Mutex<std::collections::HashMap<String, String>>,
}
#[async_trait::async_trait]
impl TaskExecutor for ConditionScenarioExecutor {
    async fn execute(
        &self,
        context: ExecutionContext,
    ) -> std::result::Result<executors::ExecutionResult, executors::ExecutorError> {
        self.inputs
            .lock()
            .unwrap()
            .insert(context.task_id, context.description);
        std::future::pending().await
    }
    async fn cancel(
        &self,
        _execution_id: &str,
    ) -> std::result::Result<(), executors::ExecutorError> {
        Ok(())
    }
}

pub(super) async fn scenario_agent(db: &SqliteDb) -> String {
    let agent = seed_agent(db).await;
    let row = AgentRepo::get_by_id(db, &agent).await.unwrap().unwrap();
    AgentRepo::update(
        db,
        db::UpdateAgent {
            id: row.id,
            expected_version: row.version,
            name: None,
            description: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: None,
            config_json: None,
            daemon_id: Some(None),
            max_concurrent_tasks: None,
            heartbeat_interval_seconds: None,
            max_missed_heartbeats: None,
            status: None,
            last_heartbeat_at: None,
            is_default: None,
            paused: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    agent
}

async fn scenario_task(
    db: &SqliteDb,
    project: &str,
    agent: &str,
    state: &str,
    annotation: Option<Value>,
) -> Task {
    let task = seed_task_with_status(db, project, state).await;
    for role in ["coder", "planner", "reviewer", "worker"] {
        seed_role_assignment(db, &task.id, role, Some(agent)).await;
    }
    if let Some(annotation) = annotation {
        sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
            .bind(annotation.to_string())
            .bind(&task.id)
            .execute(db.pool())
            .await
            .unwrap();
    }
    TaskRepo::get_by_id(db, &task.id, false)
        .await
        .unwrap()
        .unwrap()
}

async fn reload(db: &SqliteDb, id: &str) -> Task {
    TaskRepo::get_by_id(db, id, false).await.unwrap().unwrap()
}

fn has_marker(task: &Task) -> bool {
    task.metadata_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .is_some_and(|metadata| metadata.get("queued_recovery").is_some())
}

async fn offers_text(service: &TaskService, id: &str) -> String {
    match service
        .task_action_offers(id, &Actor::user(UserActionSource::Test))
        .await
    {
        Ok(response) => response
            .available_actions
            .iter()
            .map(|offer| format!("{}[{}]", offer.action.verb(), offer.reason))
            .collect::<Vec<_>>()
            .join(","),
        Err(error) => format!("OFFERS_ERR:{error}"),
    }
}

fn result_text(result: &Result<bool>) -> String {
    match result {
        Ok(value) => format!("Ok({value})"),
        Err(error) => format!(
            "Err({})",
            format!("{error}").chars().take(200).collect::<String>()
        ),
    }
}

async fn latest_execution(db: &SqliteDb, task_id: &str) -> Option<Execution> {
    let id: Option<String> = sqlx::query_scalar(
        "SELECT id FROM execution WHERE task_id = ? ORDER BY created_at DESC, rowid DESC LIMIT 1",
    )
    .bind(task_id)
    .fetch_optional(db.pool())
    .await
    .unwrap();
    match id {
        Some(id) => ExecutionRepo::get_by_id(db, &id).await.unwrap(),
        None => None,
    }
}

async fn running_count(db: &SqliteDb, task_id: &str) -> usize {
    ExecutionRepo::list_running_by_task(db, task_id)
        .await
        .unwrap()
        .len()
}

async fn free_slots(db: &SqliteDb) {
    sqlx::query("UPDATE execution SET status = 'cancelled' WHERE status = 'running'")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE workspace_lease SET status = 'revoked', revoked_at = ?, version = version + 1 WHERE status = 'active'")
        .bind(now_rfc3339()).execute(db.pool()).await.unwrap();
    sqlx::query("UPDATE task SET status = 'done' WHERE status NOT IN ('done','cancelled')")
        .execute(db.pool())
        .await
        .unwrap();
}

fn executor_failed() -> Value {
    json!({"type":"executor_failed","blocking_reason":"executor_failed","message":"boom","recovery_actions":["reexecute","reset_to_initial","cancel_task"]})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn condition_queue_scenarios() {
    let db = Arc::new(sqlite_db().await);
    let (project, _, repo_dir) = seed_project_repo(&db).await;
    initialize_primary_repository(&repo_dir);
    let bus = Arc::new(EventBus::new(4096));
    let executor = Arc::new(ConditionScenarioExecutor::default());
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&bus))
        .with_workspace_root(repo_dir.path().join("workspaces"))
        .with_task_executor(executor.clone());
    let mut out = String::new();

    // S1: agent paused after the command committed.
    Box::pin(async {
        let agent = scenario_agent(&db).await;
        let task = scenario_task(
            &db,
            &project,
            &agent,
            "in_progress",
            Some(executor_failed()),
        )
        .await;
        let command = service
            .perform_task_action(&task.id, TaskAction::retry(), task.version)
            .await;
        let _ = writeln!(
            out,
            "S1 command={:?}",
            command
                .as_ref()
                .map(|result| result.task.version)
                .map_err(|error| error.to_string())
        );
        sqlx::query("UPDATE agent_identity SET paused = 1 WHERE id = ?")
            .bind(&agent)
            .execute(db.pool())
            .await
            .unwrap();
        for round in 0..2 {
            let current = reload(&db, &task.id).await;
            let result = service.dispatch_queued_recovery(&current).await;
            let after = reload(&db, &task.id).await;
            assert!(has_marker(&after));
            assert_eq!(
                after.version, current.version,
                "paused replay must wait without rewriting the Task"
            );
            let _ = writeln!(out, "S1 paused-agent round={round} dispatch={} marker={} running={} annotation={:?} offers={}", result_text(&result), has_marker(&after), running_count(&db, &task.id).await, after.error_annotation.is_some(), offers_text(&service, &task.id).await);
        }
        free_slots(&db).await;
    }).await;

    // S2: agent deleted after the command committed.
    Box::pin(async {
        let agent = scenario_agent(&db).await;
        let task = scenario_task(
            &db,
            &project,
            &agent,
            "in_progress",
            Some(executor_failed()),
        )
        .await;
        let command = service
            .perform_task_action(&task.id, TaskAction::retry(), task.version)
            .await;
        let _ = writeln!(out, "S2 command_ok={}", command.is_ok());
        let deleted = AgentRepo::archive(&*db, &agent, &now_rfc3339()).await;
        let _ = writeln!(
            out,
            "S2 agent archive={:?}",
            deleted.map(|_| ()).map_err(|error| error.to_string())
        );
        for round in 0..2 {
            let current = reload(&db, &task.id).await;
            let result = service.dispatch_queued_recovery(&current).await;
            let after = reload(&db, &task.id).await;
            assert!(
                !has_marker(&after),
                "archived Agent permanently refuses replay"
            );
            assert!(
                after.error_annotation.is_some(),
                "saved condition must be restored"
            );
            let _ = writeln!(out, "S2 deleted-agent round={round} dispatch={} marker={} running={} annotation={:?} offers={}", result_text(&result), has_marker(&after), running_count(&db, &task.id).await, after.error_annotation.is_some(), offers_text(&service, &task.id).await);
        }
        free_slots(&db).await;
    }).await;

    // S3: pinned resumable execution loses its session between commit and dispatch.
    Box::pin(async {
        let agent = scenario_agent(&db).await;
        let task = scenario_task(&db, &project, &agent, "in_progress", None).await;
        let execution = seed_execution(
            &db,
            &task.id,
            Some(&agent),
            "coder",
            ExecutionStatus::Failed,
            Some("thread-1"),
            "2026-10-02T00:00:00Z",
        )
        .await;
        sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
            .bind(json!({"type":"executor_failed","blocking_reason":"executor_failed","blocked_execution_id":execution.id}).to_string())
            .bind(&task.id).execute(db.pool()).await.unwrap();
        let task = reload(&db, &task.id).await;
        let before = service
            .task_action_offers(&task.id, &Actor::user(UserActionSource::Test))
            .await
            .unwrap()
            .available_actions;
        let retry = before
            .iter()
            .find(|offer| offer.action.verb() == "retry")
            .cloned();
        let _ = writeln!(
            out,
            "S3 retry offer={:?}",
            retry.as_ref().map(|offer| (
                offer.action.clone(),
                offer.target_execution_id.clone(),
                offer.reason.clone()
            ))
        );
        let command = service
            .perform_task_action(
                &task.id,
                TaskAction::Retry {
                    reason: None,
                    fresh_session: Some(false),
                    refresh_workspace: None,
                    reset_budget: None,
                    guidance: Some("continue please".to_owned()),
                },
                task.version,
            )
            .await;
        let _ = writeln!(
            out,
            "S3 command={:?}",
            command
                .as_ref()
                .map(|_| ())
                .map_err(|error| error.to_string())
        );
        sqlx::query("UPDATE execution SET agent_session_id = NULL WHERE id = ?")
            .bind(&execution.id)
            .execute(db.pool())
            .await
            .unwrap();
        for round in 0..2 {
            let current = reload(&db, &task.id).await;
            if !has_marker(&current) {
                break;
            }
            let result = service.dispatch_queued_recovery(&current).await;
            let after = reload(&db, &task.id).await;
            let latest = latest_execution(&db, &task.id).await;
            let _ = writeln!(out, "S3 session-gone round={round} dispatch={} marker={} running={} annotation={:?} latest_parent={:?} offers={}", result_text(&result), has_marker(&after), running_count(&db, &task.id).await, after.error_annotation.is_some(), latest.and_then(|execution| execution.parent_execution_id), offers_text(&service, &task.id).await);
        }
        free_slots(&db).await;
    }).await;

    // S4: two dispatcher passes race on one marker.
    Box::pin(async {
        let agent = scenario_agent(&db).await;
        let task = scenario_task(
            &db,
            &project,
            &agent,
            "in_progress",
            Some(executor_failed()),
        )
        .await;
        service
            .perform_task_action(&task.id, TaskAction::retry(), task.version)
            .await
            .unwrap();
        let current = reload(&db, &task.id).await;
        let (left, right) = tokio::join!(
            service.dispatch_queued_recovery(&current),
            service.dispatch_queued_recovery(&current)
        );
        let after = reload(&db, &task.id).await;
        let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution WHERE task_id = ?")
            .bind(&task.id)
            .fetch_one(db.pool())
            .await
            .unwrap();
        let _ = writeln!(
            out,
            "S4 race left={} right={} marker={} running={} executions_total={total}",
            result_text(&left),
            result_text(&right),
            has_marker(&after),
            running_count(&db, &task.id).await
        );
        let stale = service.dispatch_queued_recovery(&current).await;
        let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution WHERE task_id = ?")
            .bind(&task.id)
            .fetch_one(db.pool())
            .await
            .unwrap();
        let _ = writeln!(
            out,
            "S4 stale replay={} executions_total={total}",
            result_text(&stale)
        );
        free_slots(&db).await;
    })
    .await;

    // S5: cancel while queued behind a busy agent.
    Box::pin(async {
        let agent = scenario_agent(&db).await;
        let busy = scenario_task(&db, &project, &agent, "in_progress", None).await;
        let occupied = seed_execution(
            &db,
            &busy.id,
            Some(&agent),
            "coder",
            ExecutionStatus::Running,
            None,
            "2026-10-02T00:00:00Z",
        )
        .await;
        let task = scenario_task(
            &db,
            &project,
            &agent,
            "in_progress",
            Some(executor_failed()),
        )
        .await;
        let queued = service
            .perform_task_action(&task.id, TaskAction::retry(), task.version)
            .await
            .unwrap()
            .task;
        let waiting = service.dispatch_queued_recovery(&queued).await;
        let second = service
            .perform_task_action(
                &task.id,
                TaskAction::Restart { reason: None },
                queued.version,
            )
            .await;
        let _ = writeln!(
            out,
            "S5 at-capacity dispatch={} offers_while_queued={} second_command(restart)={:?}",
            result_text(&waiting),
            offers_text(&service, &task.id).await,
            second.as_ref().map(|_| ()).map_err(|error| error
                .to_string()
                .chars()
                .take(80)
                .collect::<String>())
        );
        let current = reload(&db, &task.id).await;
        let cancelled = service
            .perform_task_action(
                &task.id,
                TaskAction::Cancel { reason: None },
                current.version,
            )
            .await;
        let _ = writeln!(
            out,
            "S5 cancel={:?}",
            cancelled
                .as_ref()
                .map(|result| result.task.status.clone())
                .map_err(|error| error.to_string())
        );
        sqlx::query("UPDATE execution SET status = 'cancelled' WHERE id = ?")
            .bind(&occupied.id)
            .execute(db.pool())
            .await
            .unwrap();
        let current = reload(&db, &task.id).await;
        let result = service.dispatch_queued_recovery(&current).await;
        let after = reload(&db, &task.id).await;
        assert_eq!(after.status, "cancelled");
        assert!(!has_marker(&after));
        assert_eq!(running_count(&db, &task.id).await, 0);
        let _ = writeln!(
            out,
            "S5 after-cancel dispatch={} status={} marker={} running={}",
            result_text(&result),
            after.status,
            has_marker(&after),
            running_count(&db, &task.id).await
        );
        free_slots(&db).await;
    })
    .await;

    // S6: restart of a hard-failed Task: events and timing.
    Box::pin(async {
        let agent = scenario_agent(&db).await;
        let task = scenario_task(&db, &project, &agent, "in_progress", None).await;
        sqlx::query("UPDATE task SET failed_json = ?, blocked_json = ? WHERE id = ?")
            .bind(json!({"kind":"executor_failed","reason":"hard failure","created_at":now_rfc3339()}).to_string())
            .bind(json!({"kind":"executor_failed","reason":"blocked too","created_at":now_rfc3339()}).to_string())
            .bind(&task.id).execute(db.pool()).await.unwrap();
        let task = reload(&db, &task.id).await;
        let mut events = bus.subscribe();
        let command = service
            .perform_task_action(&task.id, TaskAction::Restart { reason: None }, task.version)
            .await;
        let after_command = reload(&db, &task.id).await;
        let _ = writeln!(out, "S6 command={:?} status_after_command={} failed_json_after_command={} blocked_json_after_command={} marker={}", command.as_ref().map(|_| ()).map_err(|error| error.to_string()), after_command.status, after_command.failed_json.is_some(), after_command.blocked_json.is_some(), has_marker(&after_command));
        let result = service.dispatch_queued_recovery(&after_command).await;
        let after = reload(&db, &task.id).await;
        let mut types = Vec::new();
        while let Ok(event) = events.try_recv() {
            types.push(event.event_type);
        }
        assert!(types.iter().any(|kind| kind == "task.restarted"));
        assert!(types.iter().any(|kind| kind == "task.unblocked"));
        assert!(after_command.failed_json.is_none() && after_command.blocked_json.is_none());
        assert!(
            !has_marker(&after_command),
            "restart applies its condition immediately"
        );
        let _ = writeln!(
            out,
            "S6 dispatch={} status={} marker={} events={types:?}",
            result_text(&result),
            after.status,
            has_marker(&after)
        );
        free_slots(&db).await;
    }).await;

    // S7: paused Project: command accepted, dispatcher never visits it.
    Box::pin(async {
        let (paused_project, _, paused_repo) = seed_project_repo(&db).await;
        initialize_primary_repository(&paused_repo);
        let agent = scenario_agent(&db).await;
        let task = scenario_task(&db, &paused_project, &agent, "in_progress", None).await;
        sqlx::query("UPDATE task SET failed_json = ? WHERE id = ?")
            .bind(json!({"kind":"executor_failed","reason":"hard failure","created_at":now_rfc3339()}).to_string())
            .bind(&task.id).execute(db.pool()).await.unwrap();
        sqlx::query("UPDATE project SET paused_at = ? WHERE id = ?")
            .bind(now_rfc3339())
            .bind(&paused_project)
            .execute(db.pool())
            .await
            .unwrap();
        let task = reload(&db, &task.id).await;
        let command = service
            .perform_task_action(&task.id, TaskAction::Restart { reason: None }, task.version)
            .await;
        let dispatcher = crate::task_dispatcher::TaskDispatcher::new(
            Arc::clone(&db),
            Arc::clone(&bus),
            Arc::new(service.clone()),
        );
        let pass = dispatcher.check_once().await;
        let after = reload(&db, &task.id).await;
        let paused: Option<String> =
            sqlx::query_scalar("SELECT paused_at FROM project WHERE id = ?")
                .bind(&paused_project)
                .fetch_one(db.pool())
                .await
                .unwrap();
        let _ = writeln!(out, "S7 paused-project command={:?} check_once={:?} project_still_paused={} status={} failed_json={} marker={} offers={}", command.as_ref().map(|_| ()).map_err(|error| error.to_string()), pass.map_err(|error| error.to_string()), paused.is_some(), after.status, after.failed_json.is_some(), has_marker(&after), offers_text(&service, &task.id).await);
        free_slots(&db).await;
    }).await;

    // S8: what prompt does a fresh retry / release / start hand to the agent?
    Box::pin(async {
        for (label, state, annotation, action) in [
            (
                "fresh-retry-in_progress",
                "in_progress",
                Some(executor_failed()),
                TaskAction::retry(),
            ),
            (
                "fresh-retry-planning",
                "planning",
                Some(executor_failed()),
                TaskAction::retry(),
            ),
            (
                "fresh-retry-merge_failed",
                "merge_failed",
                Some(executor_failed()),
                TaskAction::retry(),
            ),
            (
                "retry-with-guidance-in_progress",
                "in_progress",
                Some(executor_failed()),
                TaskAction::Retry {
                    reason: None,
                    fresh_session: None,
                    refresh_workspace: None,
                    reset_budget: None,
                    guidance: Some("CONDITION-GUIDANCE".to_owned()),
                },
            ),
            (
                "release-held-in_progress",
                "in_progress",
                Some(json!({"type":"manual_stop","blocking_reason":"user_paused"})),
                TaskAction::Release { reason: None },
            ),
            (
                "retry-active-no-condition",
                "in_progress",
                None,
                TaskAction::retry(),
            ),
            ("start-todo", "todo", None, TaskAction::Start),
            (
                "fresh-retry-review-blocked",
                "review",
                Some(json!({"type":"review_blocked","blocking_reason":"review_blocked"})),
                TaskAction::retry(),
            ),
        ] {
            let agent = scenario_agent(&db).await;
            let task = scenario_task(&db, &project, &agent, state, annotation).await;
            sqlx::query("UPDATE task SET description = 'CONDITION-TASK-DESCRIPTION implement the frobnicator' WHERE id = ?").bind(&task.id).execute(db.pool()).await.unwrap();
            if state == "review" {
                let candidate = seed_execution(
                    &db,
                    &task.id,
                    Some(&agent),
                    "coder",
                    ExecutionStatus::Completed,
                    Some("candidate"),
                    "2026-10-02T00:00:00Z",
                )
                .await;
                seed_failed_review(&db, &task.id, &candidate.id, 1, json!({"ci_steps":[]})).await;
            }
            let task = reload(&db, &task.id).await;
            let offers = offers_text(&service, &task.id).await;
            let command = service
                .perform_task_action(&task.id, action, task.version)
                .await;
            let mut rounds = Vec::new();
            for _ in 0..2 {
                let current = reload(&db, &task.id).await;
                if !has_marker(&current) {
                    break;
                }
                rounds.push(result_text(
                    &service.dispatch_queued_recovery(&current).await,
                ));
            }
            let after = reload(&db, &task.id).await;
            let latest = latest_execution(&db, &task.id).await;
            let summary = latest
                .as_ref()
                .and_then(|execution| execution.summary.clone())
                .unwrap_or_default()
                .replace('\n', "\\n");
            assert!(command.is_ok(), "{label}: command must be accepted");
            assert!(
                !has_marker(&after),
                "{label}: dispatch must consume the intent"
            );
            assert_eq!(
                running_count(&db, &task.id).await,
                1,
                "{label}: one role must launch"
            );
            if label != "start-todo" {
                assert!(summary.contains("Forge role contract (authoritative):"), "{label}: role contract must survive retry");
            }
            let role_input = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let Some(input) = executor.inputs.lock().unwrap().get(&task.id).cloned() { break input; }
                    tokio::task::yield_now().await;
                }
            }).await.expect("executor receives the committed role input");
            assert!(role_input.contains("CONDITION-TASK-DESCRIPTION") || role_input.contains("task-"), "{label}: dispatched input must retain Task context");
            if label.contains("with-guidance") {
                assert!(summary.contains("CONDITION-GUIDANCE"));
            }
            let _ = writeln!(out, "S8 {label}: offers={offers} command={:?} dispatch={rounds:?} status={} marker={} running={} latest_role={:?} summary_len={} summary_mentions_task={} summary_head={}", command.as_ref().map(|_| ()).map_err(|error| error.to_string().chars().take(120).collect::<String>()), after.status, has_marker(&after), running_count(&db, &task.id).await, latest.as_ref().map(|execution| execution.role.clone()), summary.len(), summary.contains("CONDITION-TASK-DESCRIPTION") || summary.contains("task-"), summary.chars().take(240).collect::<String>());
            // Base-equivalent fresh re-execution of the same role for comparison.
            if let Some(parent) = latest
                .as_ref()
                .filter(|execution| execution.status == ExecutionStatus::Running)
            {
                sqlx::query("UPDATE execution SET status = 'failed' WHERE id = ?")
                    .bind(&parent.id)
                    .execute(db.pool())
                    .await
                    .unwrap();
                match service
                    .re_execute_execution_with_context(parent.id.clone(), None)
                    .await
                {
                    Ok(result) => {
                        let summary = result
                            .execution
                            .summary
                            .clone()
                            .unwrap_or_default()
                            .replace('\n', "\\n");
                        let _ = writeln!(
                            out,
                            "S8 {label}: BASE-STYLE re_execute summary_len={} summary_head={}",
                            summary.len(),
                            summary.chars().take(240).collect::<String>()
                        );
                    }
                    Err(error) => {
                        let _ = writeln!(
                            out,
                            "S8 {label}: BASE-STYLE re_execute error={}",
                            error.to_string().chars().take(160).collect::<String>()
                        );
                    }
                }
            }
            free_slots(&db).await;
        }
    }).await;

    // S9: the engine's own dispatch_failed annotation.
    Box::pin(async {
        for state in ["todo", "planning", "in_progress", "review", "merge_failed"] {
            let agent = scenario_agent(&db).await;
            let task = scenario_task(&db, &project, &agent, state, Some(json!({"type":"dispatch_failed","message":"agent unavailable","state":state,"detected_at":now_rfc3339()}))).await;
            let agent_actor = Actor::agent(agent.clone());
            let agent_offers = service
                .task_action_offers(&task.id, &agent_actor)
                .await
                .map(|response| response.available_actions.len())
                .unwrap_or(999);
            let _ = writeln!(out, "S9 dispatch_failed state={state} owner_offers={} assigned_agent_offer_count={agent_offers}", offers_text(&service, &task.id).await);
        }
    }).await;

    // Corrupt and stale durable intents restore their condition and error.
    for (target, saved_failure) in [
        ("in_progress", None),
        ("review", None),
        ("in_progress", Some("[]")),
        ("in_progress", Some("\"invalid failure\"")),
    ] {
        let agent = scenario_agent(&db).await;
        let task = scenario_task(&db, &project, &agent, "in_progress", None).await;
        sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
            .bind(json!({"queued_recovery":{"id":new_uuid_v4(),"target_state":target,
                "request":{"offer":{},"action":{"verb":"retry"}}, "error_annotation":executor_failed().to_string(),"blocked_json":null,
                "failed_json":saved_failure}}).to_string())
            .bind(&task.id).execute(db.pool()).await.unwrap();
        let current = reload(&db, &task.id).await;
        let _ = service.dispatch_queued_recovery(&current).await;
        let after = reload(&db, &task.id).await;
        assert!(!has_marker(&after));
        let annotation: Value =
            serde_json::from_str(after.error_annotation.as_deref().unwrap()).unwrap();
        assert_eq!(annotation["type"], "executor_failed");
        assert!(!annotation["message"].as_str().unwrap().is_empty());
        if saved_failure.is_some() {
            let failed: Value =
                serde_json::from_str(after.failed_json.as_deref().unwrap()).unwrap();
            assert_eq!(failed["kind"], "executor_failed");
            assert!(!failed["reason"].as_str().unwrap().is_empty());
        }
        assert!(service
            .task_action_offers(&task.id, &Actor::user(UserActionSource::Test))
            .await
            .unwrap()
            .available_actions
            .iter()
            .any(|offer| offer.action.verb() != "cancel"));
    }
    std::fs::create_dir_all(std::env::temp_dir().join("task-actions-fixA")).unwrap();
    std::fs::write(
        std::env::temp_dir()
            .join("task-actions-fixA")
            .join("scenarios-queue.txt"),
        out,
    )
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn condition_stored_data_scenarios() {
    let db = Arc::new(sqlite_db().await);
    let (project, _, repo_dir) = seed_project_repo(&db).await;
    initialize_primary_repository(&repo_dir);
    let bus = Arc::new(EventBus::new(4096));
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&bus))
        .with_workspace_root(repo_dir.path().join("workspaces"))
        .with_task_executor(Arc::new(ConditionScenarioExecutor::default()));
    let mut out = String::new();

    // D1: retry-window rows written by the base (old marker names).
    {
        let agent = scenario_agent(&db).await;
        let task = scenario_task(&db, &project, &agent, "review", None).await;
        let mut stamp = 0;
        let mut log = |from: &str, to: &str, trigger: &str, rejection: bool| {
            stamp += 1;
            db::CreateTransitionLog {
                id: new_uuid_v4(),
                task_id: task.id.clone(),
                from_state: from.to_owned(),
                to_state: to.to_owned(),
                trigger_name: Some(trigger.to_owned()),
                triggered_by: "user".to_owned(),
                trigger_reason: "base row".to_owned(),
                hook_results_json: None,
                rejection,
                created_at: format!("2026-09-30T00:00:{stamp:02}Z"),
            }
        };
        let rows = vec![
            log("review", "in_progress", "reject", true),
            log("review", "in_progress", "reject", true),
        ];
        for row in rows {
            db::TransitionLogRepo::insert(&*db, row).await.unwrap();
        }
        let memory =
            crate::task_diagnostics::count_gate_rejections_for_task(&db, &task.id, "review")
                .await
                .unwrap();
        let sql = db::TransitionLogRepo::count_gate_rejections(&*db, &task.id, "review")
            .await
            .unwrap();
        let _ = writeln!(out, "D1 two rejections: memory={memory} sql={sql}");
        for name in [
            "proceed_once",
            "resume_process",
            "retry_hook",
            "mark_reviewed",
            "skip_hook_once",
        ] {
            let row = log("review", "review", name, false);
            db::TransitionLogRepo::insert(&*db, row).await.unwrap();
            let memory =
                crate::task_diagnostics::count_gate_rejections_for_task(&db, &task.id, "review")
                    .await
                    .unwrap();
            let sql = db::TransitionLogRepo::count_gate_rejections(&*db, &task.id, "review")
                .await
                .unwrap();
            let _ = writeln!(
                out,
                "D1 after old non-boundary marker {name}: memory={memory} sql={sql}"
            );
        }
        for name in ["reset_retry_window", "reset_to_initial"] {
            let row = log("review", "in_progress", "reject", true);
            db::TransitionLogRepo::insert(&*db, row).await.unwrap();
            let row = log("review", "review", name, false);
            db::TransitionLogRepo::insert(&*db, row).await.unwrap();
            let memory =
                crate::task_diagnostics::count_gate_rejections_for_task(&db, &task.id, "review")
                    .await
                    .unwrap();
            let sql = db::TransitionLogRepo::count_gate_rejections(&*db, &task.id, "review")
                .await
                .unwrap();
            let _ = writeln!(
                out,
                "D1 after old boundary marker {name}: memory={memory} sql={sql}"
            );
        }
        let row = log("review", "in_progress", "reject", true);
        db::TransitionLogRepo::insert(&*db, row).await.unwrap();
        let row = log("review", "review", "retry", false);
        db::TransitionLogRepo::insert(&*db, row).await.unwrap();
        let memory =
            crate::task_diagnostics::count_gate_rejections_for_task(&db, &task.id, "review")
                .await
                .unwrap();
        let sql = db::TransitionLogRepo::count_gate_rejections(&*db, &task.id, "review")
            .await
            .unwrap();
        let _ = writeln!(
            out,
            "D1 after new plain retry marker (no reset): memory={memory} sql={sql}"
        );
    }

    // D2: budget semantics of reset_budget true/false on an exhausted review gate.
    for reset in [true, false] {
        let agent = scenario_agent(&db).await;
        let task = scenario_task(&db, &project, &agent, "review", None).await;
        let execution = seed_execution(
            &db,
            &task.id,
            Some(&agent),
            "coder",
            ExecutionStatus::Completed,
            Some("candidate"),
            "2026-10-02T00:00:00Z",
        )
        .await;
        seed_failed_review(&db, &task.id, &execution.id, 1, json!({"ci_steps":[]})).await;
        seed_review_rejection_log(&db, &task.id, "first").await;
        seed_review_rejection_log(&db, &task.id, "second").await;
        let task = set_retry_exhausted_metadata(&db, &reload(&db, &task.id).await).await;
        let before =
            crate::task_diagnostics::count_gate_rejections_for_task(&db, &task.id, "review")
                .await
                .unwrap();
        let offers = offers_text(&service, &task.id).await;
        let command = service
            .perform_task_action(
                &task.id,
                TaskAction::Retry {
                    reason: Some("Scenario one-shot retry reason".to_owned()),
                    fresh_session: None,
                    refresh_workspace: None,
                    reset_budget: Some(reset),
                    guidance: Some("try again".to_owned()),
                },
                task.version,
            )
            .await;
        let mut rounds = Vec::new();
        for _ in 0..2 {
            let current = reload(&db, &task.id).await;
            if !has_marker(&current) {
                break;
            }
            rounds.push(result_text(
                &service.dispatch_queued_recovery(&current).await,
            ));
        }
        let after = reload(&db, &task.id).await;
        let memory =
            crate::task_diagnostics::count_gate_rejections_for_task(&db, &task.id, "review")
                .await
                .unwrap();
        let sql = db::TransitionLogRepo::count_gate_rejections(&*db, &task.id, "review")
            .await
            .unwrap();
        let logs = db::TransitionLogRepo::list_by_task(&*db, &task.id)
            .await
            .unwrap();
        let tail: Vec<String> = logs
            .iter()
            .rev()
            .take(3)
            .map(|entry| {
                format!(
                    "{}->{} trig={:?} rej={} by={} hooks={:?}",
                    entry.from_state,
                    entry.to_state,
                    entry.trigger_name,
                    entry.rejection,
                    entry.triggered_by,
                    entry.hook_results_json
                )
            })
            .collect();
        let _ = writeln!(out, "D2 reset_budget={reset} offers_before={offers} count_before={before} command={:?} dispatch={rounds:?} status={} annotation={:?} blocked={} count_after memory={memory} sql={sql} running={} offers_after={} last_logs={tail:?}", command.as_ref().map(|_| ()).map_err(|error| error.to_string()), after.status, after.error_annotation.as_deref().map(|raw| raw.chars().take(60).collect::<String>()), after.blocked_json.is_some(), running_count(&db, &task.id).await, offers_text(&service, &task.id).await);
        free_slots(&db).await;
    }

    // D3: queued payloads written by the base.
    let agent = scenario_agent(&db).await;
    let old_annotation = executor_failed().to_string();
    let cases: Vec<(&str, &str, Value)> = vec![
        (
            "recover/reexecute",
            "in_progress",
            json!({"id":"old-1","target_state":"in_progress","request":{"action":"reexecute","reason":"why","context":"ctx"},"error_annotation":old_annotation,"blocked_json":null}),
        ),
        (
            "recover/resume_session",
            "in_progress",
            json!({"id":"old-2","target_state":"in_progress","request":{"action":"resume_session","reason":"why","context":null},"error_annotation":old_annotation,"blocked_json":null}),
        ),
        (
            "recover/reset_to_initial",
            "in_progress",
            json!({"id":"old-3","target_state":"in_progress","request":{"action":"reset_to_initial","reason":"why","context":null},"error_annotation":old_annotation,"blocked_json":null}),
        ),
        (
            "resume-shape",
            "in_progress",
            json!({"id":"old-4","target_state":"in_progress","request":{"resume_reason":"continue","agent_id":agent},"error_annotation":null,"blocked_json":null}),
        ),
        (
            "recover/retry_hook-merge_failed",
            "merge_failed",
            json!({"id":"old-5","target_state":"merge_failed","request":{"action":"retry_hook","reason":"why","context":null},"error_annotation":json!({"type":"merge_conflict","blocking_reason":"merge_conflict","recovery_actions":["retry_hook","reexecute","open_interactive","cancel_task"]}).to_string(),"blocked_json":null}),
        ),
        (
            "missing-id",
            "in_progress",
            json!({"target_state":"in_progress","request":{"action":"reexecute"},"error_annotation":old_annotation,"blocked_json":null}),
        ),
        ("garbage-string", "in_progress", json!("not an object")),
    ];
    for (name, state, payload) in cases {
        let task = scenario_task(&db, &project, &agent, state, None).await;
        sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
            .bind(json!({"queued_recovery":payload,"deferred_dispatch":{"not_before":"2026-09-30T00:00:00Z","reason":"recovery queued: waiting for execution capacity or workspace owner","target_state":state}}).to_string())
            .bind(&task.id).execute(db.pool()).await.unwrap();
        let offers_before = offers_text(&service, &task.id).await;
        let mut rounds = Vec::new();
        for _ in 0..3 {
            let current = reload(&db, &task.id).await;
            if !has_marker(&current) {
                break;
            }
            rounds.push(result_text(
                &service.dispatch_queued_recovery(&current).await,
            ));
        }
        let after = reload(&db, &task.id).await;
        let latest = latest_execution(&db, &task.id).await;
        let _ = writeln!(out, "D3 {name}: offers_before={offers_before} dispatch={rounds:?} status={} marker={} annotation={:?} running={} latest_exec_role={:?} offers_after={}", after.status, has_marker(&after), after.error_annotation.as_deref().map(|raw| raw.chars().take(50).collect::<String>()), running_count(&db, &task.id).await, latest.map(|execution| execution.role), offers_text(&service, &task.id).await);
        free_slots(&db).await;
    }

    // D4: stored shapes with legacy lists deserialize.
    let blocked: std::result::Result<api_types::InterruptionMetadata, _> = serde_json::from_value(
        json!({"reason":"r","created_at":"2026-09-30T00:00:00Z","kind":"retry_exhausted","recovery_actions":["return_to_implementation","retry_pr_publication","resume_process"]}),
    );
    let annotation: std::result::Result<api_types::TaskAnnotation, _> = serde_json::from_value(
        json!({"type":"merge_conflict","blocking_reason":"x","recovery_actions":["return_to_implementation","retry_pr_publication"]}),
    );
    let _ = writeln!(
        out,
        "D4 blocked_json_parse_ok={} annotation_parse={:?}",
        blocked.is_ok(),
        annotation
            .map(|value| matches!(value, api_types::TaskAnnotation::Blocking(_)))
            .map_err(|error| error.to_string())
    );

    // D5: the only agent is paused and nothing is assigned: is `start` advertised and accepted?
    {
        let db2 = Arc::new(sqlite_db().await);
        let (project2, _, repo2) = seed_project_repo(&db2).await;
        initialize_primary_repository(&repo2);
        let agent2 = scenario_agent(&db2).await;
        sqlx::query("UPDATE agent_identity SET paused = 1 WHERE id = ?")
            .bind(&agent2)
            .execute(db2.pool())
            .await
            .unwrap();
        let service2 = TaskService::new(Arc::clone(&db2), Arc::new(EventBus::new(64)))
            .with_workspace_root(repo2.path().join("workspaces"))
            .with_task_executor(Arc::new(ConditionScenarioExecutor::default()));
        for (state, annotation) in [
            ("todo", None),
            ("in_progress", None),
            ("in_progress", Some(executor_failed())),
        ] {
            let task = seed_task_with_status(&db2, &project2, state).await;
            if let Some(annotation) = annotation.clone() {
                sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
                    .bind(annotation.to_string())
                    .bind(&task.id)
                    .execute(db2.pool())
                    .await
                    .unwrap();
            }
            let task = reload(&db2, &task.id).await;
            let offers = service2
                .task_action_offers(&task.id, &Actor::user(UserActionSource::Test))
                .await
                .unwrap()
                .available_actions;
            if let Some(offer) = offers
                .iter()
                .find(|offer| offer.action.verb() != "cancel" && offer.action.verb() != "restart")
            {
                let result = service2
                    .perform_task_action(&task.id, offer.action.clone(), task.version)
                    .await;
                let _ = writeln!(out, "D5 only-agent-paused unassigned state={state} annotated={} offered={}[{}] command={:?}", annotation.is_some(), offer.action.verb(), offer.reason, result.map(|_| ()).map_err(|error| error.to_string().chars().take(140).collect::<String>()));
            }
        }
    }

    std::fs::write(
        std::env::temp_dir()
            .join("task-actions-fixA")
            .join("scenarios-stored.txt"),
        out,
    )
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn condition_send_back_prompt_scenarios() {
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(4096));
    let mut out = String::new();
    for workflow in ["auto", "std"] {
        let (project, _, repo_dir) = seed_project_repo(&db).await;
        initialize_primary_repository(&repo_dir);
        if workflow == "auto" {
            let definition =
                crate::workflow::default_autonomous_workflow::default_autonomous_workflow();
            sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
                .bind(serde_json::to_string(&definition).unwrap())
                .bind(&project)
                .execute(db.pool())
                .await
                .unwrap();
        }
        let service = TaskService::new(Arc::clone(&db), Arc::clone(&bus))
            .with_workspace_root(repo_dir.path().join("workspaces"))
            .with_task_executor(Arc::new(ConditionScenarioExecutor::default()));
        let (role, target) = if workflow == "auto" {
            ("worker", "working")
        } else {
            ("coder", "in_progress")
        };
        for path in ["new_command", "base_transition"] {
            for review_status in ["none", "awaiting_human"] {
                let agent = scenario_agent(&db).await;
                let task = scenario_task(&db, &project, &agent, "review", None).await;
                let candidate = seed_execution(
                    &db,
                    &task.id,
                    Some(&agent),
                    role,
                    ExecutionStatus::Completed,
                    Some("thread-1"),
                    "2026-10-02T00:00:00Z",
                )
                .await;
                if review_status == "awaiting_human" {
                    let now = now_rfc3339();
                    db::ReviewRepo::create(
                        &*db,
                        db::CreateReview {
                            id: new_uuid_v4(),
                            task_id: task.id.clone(),
                            execution_id: candidate.id.clone(),
                            attempt_number: 1,
                            status: ReviewStatus::AwaitingHuman,
                            step_results_json: json!({"ci_steps":[]}).to_string(),
                            started_at: now.clone(),
                            created_at: now.clone(),
                            updated_at: now,
                        },
                    )
                    .await
                    .unwrap();
                }
                let task = reload(&db, &task.id).await;
                let offers = offers_text(&service, &task.id).await;
                let mut rounds = Vec::new();
                let command = if path == "new_command" {
                    let result = service
                        .perform_task_action(
                            &task.id,
                            TaskAction::SendBack {
                                guidance: "CONDITION-GUIDANCE add evidence".to_owned(),
                            },
                            task.version,
                        )
                        .await
                        .map(|result| result.task.status)
                        .map_err(|error| error.to_string());
                    for _ in 0..2 {
                        let current = reload(&db, &task.id).await;
                        if !has_marker(&current) {
                            break;
                        }
                        rounds.push(result_text(
                            &service.dispatch_queued_recovery(&current).await,
                        ));
                    }
                    result
                } else if workflow == "std" && review_status == "awaiting_human" {
                    service
                        .reject_review_as(
                            task.id.clone(),
                            Some("CONDITION-GUIDANCE add evidence".to_owned()),
                            Actor::user(UserActionSource::Test),
                        )
                        .await
                        .map(|result| result.0.status)
                        .map_err(|error| error.to_string())
                } else {
                    service
                        .transition(
                            task.id.clone(),
                            target.to_owned(),
                            TransitionOptions {
                                version: task.version,
                                reason: Some("CONDITION-GUIDANCE add evidence".to_owned()),
                                triggered_by: Actor::user(UserActionSource::Test),
                                rejection: true,
                                defer_dispatch_seconds: None,
                            },
                        )
                        .await
                        .map(|result| result.task.status)
                        .map_err(|error| error.to_string())
                };
                tokio::time::sleep(Duration::from_millis(300)).await;
                let after = reload(&db, &task.id).await;
                let latest = latest_execution(&db, &task.id).await;
                let review: Option<String> = sqlx::query_scalar("SELECT status FROM review WHERE task_id = ? ORDER BY attempt_number DESC LIMIT 1").bind(&task.id).fetch_optional(db.pool()).await.unwrap();
                let prompt = latest
                    .as_ref()
                    .and_then(|execution| execution.prompt.clone())
                    .unwrap_or_default()
                    .replace('\n', "\\n");
                if path == "new_command" {
                    assert!(command.is_ok());
                    assert!(!has_marker(&after));
                    assert_eq!(running_count(&db, &task.id).await, 1);
                    assert!(prompt.contains("Forge role contract (authoritative):"));
                    assert!(prompt.contains("CONDITION-GUIDANCE"));
                    assert!(
                        prompt.len() > 3000,
                        "review-fix contract must accompany guidance"
                    );
                }
                let _ = writeln!(out, "P {workflow} path={path} review={review_status} offers={offers} command={command:?} dispatch={rounds:?} status={} marker={} deferred={} review_after={review:?} running={} latest_role={:?} latest_parent_is_candidate={:?} latest_session={:?} prompt_len={} prompt_has_guidance={} prompt_head={}", after.status, has_marker(&after), after.metadata_json.as_deref().is_some_and(|raw| raw.contains("deferred_dispatch")), running_count(&db, &task.id).await, latest.as_ref().map(|execution| execution.role.clone()), latest.as_ref().map(|execution| execution.parent_execution_id.as_deref() == Some(candidate.id.as_str())), latest.as_ref().map(|execution| execution.agent_session_id.clone()), prompt.len(), prompt.contains("CONDITION-GUIDANCE"), prompt.chars().take(700).collect::<String>());
                free_slots(&db).await;
            }
        }
    }
    std::fs::write(
        std::env::temp_dir()
            .join("task-actions-fixA")
            .join("scenarios-sendback.txt"),
        out,
    )
    .unwrap();
}
