use super::*;
use crate::worker_runtime::queue::TaskStepWorker;
use crate::workflow::HookResult;
use db::TaskStepRepo;

#[tokio::test]
async fn crash_after_cas_keeps_hooks_and_checkpointed_cascade_durable() {
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(64));
    seed_project_repo_and_task(&db, "crash-cas", "start").await;
    let workflow = cascade_chain_workflow(2);
    let result = engine(db.clone(), bus.clone())
        .workflow_execution()
        .transition(
            "crash-cas",
            "step_0",
            1,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "start",
            false,
            Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(result.task.status, "step_0");
    assert_eq!(result.pending_steps, 1);
    let step = db
        .claim_step(
            "crashed",
            Some("crash-cas"),
            &(chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(step.kind, "hooks");
    // The action finished but the process died before the settlement/enqueue.
    db.start_hook(&step, 0).await.unwrap();
    db.finish_hook(
        &step,
        0,
        &serde_json::to_string(&HookResult::Cascade {
            to: "step_1".into(),
            reason: "completed".into(),

            bridge: Default::default(),
        })
        .unwrap(),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE task_step SET lease_until='2000-01-01T00:00:00Z' WHERE id=?")
        .bind(&step.id)
        .execute(db.pool())
        .await
        .unwrap();
    let settled = TaskStepWorker::new(engine(db.clone(), bus))
        .drain("crash-cas")
        .await
        .unwrap();
    assert_eq!(settled.status, "done");
    let rows = db.task_steps("crash-cas").await.unwrap();
    assert!(rows.iter().all(|row| row.status == "done"));
    let targets: Vec<String> = sqlx::query_scalar(
        "SELECT to_state FROM transition_log WHERE task_id='crash-cas' ORDER BY created_at,rowid",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(targets, vec!["step_0", "step_1", "done"]);
    assert_eq!(
        rows.iter().find(|row| row.id == step.id).unwrap().attempts,
        2
    );
}

#[tokio::test]
async fn configured_hooks_count_as_pending_even_when_actor_audience_skips_them() {
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(8));
    seed_project_repo_and_task(&db, "hook-audience", "start").await;
    let mut workflow = cascade_chain_workflow(1);
    workflow.states[1].hooks.on_enter[0].applies_to = api_types::HookAudience::UserOnly;
    let engine = engine(db.clone(), bus);
    let result = engine
        .workflow_execution()
        .transition(
            "hook-audience",
            "step_0",
            1,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "start",
            false,
            Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(result.pending_steps, 1);
    let settled = TaskStepWorker::new(engine)
        .drain("hook-audience")
        .await
        .unwrap();
    assert_eq!(settled.status, "step_0", "audience still skips the action");
    assert_eq!(
        db.task_steps("hook-audience").await.unwrap()[0].status,
        "done"
    );
}

#[tokio::test]
async fn hooks_enqueue_failure_rolls_back_status_cas() {
    let db = Arc::new(sqlite_db().await);
    seed_project_repo_and_task(&db, "atomic-hook", "start").await;
    sqlx::query("CREATE TRIGGER reject_hooks BEFORE INSERT ON task_step WHEN NEW.kind='hooks' BEGIN SELECT RAISE(ABORT,'hook enqueue failed'); END").execute(db.pool()).await.unwrap();
    assert!(engine(db.clone(), Arc::new(EventBus::new(8)))
        .workflow_execution()
        .transition(
            "atomic-hook",
            "step_0",
            1,
            &cascade_chain_workflow(1),
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "start",
            false,
            Default::default(),
        )
        .await
        .is_err());
    assert_eq!(
        TaskRepo::get_by_id(&*db, "atomic-hook", false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "start"
    );
    assert!(TransitionLogRepo::list_by_task(&*db, "atomic-hook")
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn workflow_writer_waits_for_hook_lease_and_old_owner_stays_fenced() {
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(8));
    seed_project_repo_and_task(&db, "hook-fence", "start").await;
    let engine = engine(db.clone(), bus.clone());
    engine
        .workflow_execution()
        .transition(
            "hook-fence",
            "step_0",
            1,
            &cascade_chain_workflow(1),
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "start",
            false,
            Default::default(),
        )
        .await
        .unwrap();
    let step = db
        .claim_step(
            "old",
            Some("hook-fence"),
            &(chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339(),
        )
        .await
        .unwrap()
        .unwrap();
    db.start_hook(&step, 0).await.unwrap();
    db.enqueue_task_mutation(
        "hook-fence",
        db::TaskMutation::Sql {
            task_id: "hook-fence".into(),
            query: "UPDATE task SET status='start',version=version+1 WHERE id=?".into(),
            arguments: vec![json!("hook-fence")],
        },
    )
    .await
    .unwrap();
    assert_eq!(
        TaskRepo::get_by_id(&*db, "hook-fence", false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "step_0"
    );
    db.finish_hook(&step, 0, "\"Ok\"").await.unwrap();
    sqlx::query("UPDATE task_step SET lease_until='2000-01-01T00:00:00Z' WHERE id=?")
        .bind(&step.id)
        .execute(db.pool())
        .await
        .unwrap();
    TaskStepWorker::new(engine)
        .drain("hook-fence")
        .await
        .unwrap();
    assert!(matches!(
        db.finish_hook(&step, 0, "late").await,
        Err(db::DbError::VersionConflict)
    ));
    assert_eq!(
        TaskRepo::get_by_id(&*db, "hook-fence", false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "start"
    );
}

#[tokio::test]
async fn resumed_before_work_script_logs_step_and_rerun() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    let marker = TempDir::new().unwrap();
    let ran = marker.path().join("ran");
    let quote = |p: &std::path::Path| format!("'{}'", p.to_string_lossy().replace('\'', "'\"'\"'"));
    let scripts = json!([{"type":"script","command":format!("echo run >> {}",quote(&ran)),"timeout_seconds":30,"blocking":true}]);
    sqlx::query("UPDATE project SET settings=? WHERE id=?")
        .bind(json!({"lifecycle_hooks":{"before_work":scripts}}).to_string())
        .bind(&fixture.task.project_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let result = fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &fixture.task.id,
            "review",
            fixture.task.version,
            &fixture.workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "start",
            false,
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    assert!(!ran.exists(), "CAS does not run scripts");
    let step = fixture
        .db
        .claim_step(
            "crashed",
            Some(&fixture.task.id),
            &(chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339(),
        )
        .await
        .unwrap()
        .unwrap();
    fixture.db.start_hook(&step, 0).await.unwrap();
    fixture
        .db
        .record_hook_effect(
            &step,
            0,
            "before_work_input",
            &json!({"hooks":scripts,"env":{}}).to_string(),
        )
        .await
        .unwrap();
    fixture.db.start_hook_script(&step, 0, 0).await.unwrap();
    // An unrecorded first execution is repeated after restart.
    std::fs::write(&ran, "first attempt\n").unwrap();
    // A later settings edit must not rebind the interrupted script's index.
    sqlx::query("UPDATE project SET settings='{}' WHERE id=?")
        .bind(&fixture.task.project_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task_step SET lease_until='2000-01-01T00:00:00Z' WHERE id=?")
        .bind(&step.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    TaskStepWorker::new(fixture.engine.clone())
        .drain(&fixture.task.id)
        .await
        .unwrap();
    assert!(std::fs::read_to_string(&ran)
        .unwrap()
        .starts_with("first attempt\nrun\n"));
    let log_dir = crate::task_service::logs::task_hook_logs_dir(
        &fixture.engine.workspace_root,
        &fixture.task.project_id,
        &fixture.task.id,
    );
    let entries = std::fs::read_dir(log_dir)
        .unwrap()
        .map(|e| std::fs::read_to_string(e.unwrap().path()).unwrap())
        .collect::<Vec<_>>();
    assert!(entries.iter().any(
        |entry| entry.contains("\"rerun_after_interruption\":true") && entry.contains(&step.id)
    ));
    assert_eq!(result.task.status, "review");
}

#[tokio::test]
async fn resumed_dispatch_reuses_a_terminal_execution_for_its_hook() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    assign_agent_role(
        &fixture.db,
        &fixture.task.id,
        "coder",
        "already-dispatched-agent",
    )
    .await;
    let task = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &fixture.task.id,
            "in_progress",
            task.version,
            &fixture.workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "resume",
            false,
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    let step = fixture
        .db
        .claim_step(
            "crashed-dispatch",
            Some(&fixture.task.id),
            &(chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339(),
        )
        .await
        .unwrap()
        .unwrap();
    // The execution and association already committed; its provider finished
    // before the hook's outcome was recorded. Running-only guards miss this.
    let execution: String =
        sqlx::query_scalar("SELECT id FROM execution WHERE task_id=? AND role='coder'")
            .bind(&fixture.task.id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    fixture.db.start_hook(&step, 1).await.unwrap();
    sqlx::query("UPDATE task_hook_checkpoint SET execution_id=? WHERE step_id=? AND hook_index=1")
        .bind(&execution)
        .bind(&step.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task_step SET lease_until='2000-01-01T00:00:00Z' WHERE id=?")
        .bind(&step.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let settled = TaskStepWorker::new(fixture.engine.clone())
        .drain(&fixture.task.id)
        .await
        .unwrap();
    assert_eq!(settled.status, "in_progress");
    assert!(settled.error_annotation.is_none());
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution WHERE task_id=?")
        .bind(&fixture.task.id)
        .fetch_one(fixture.db.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(
        fixture.db.task_steps(&fixture.task.id).await.unwrap()[0].status,
        "done"
    );
}

// Review-entry CI runs on the durable check runner while its hooks step is
// suspended. A Cancel does not wait for the run: the suspended step is
// superseded, the review attempt it opened is closed, and the check worker
// stops the run once its only consumer left the status entry.
#[tokio::test]
async fn cancel_while_review_ci_runs_supersedes_the_suspended_step_and_stops_the_run() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    let control = TempDir::new().unwrap();
    let started = control.path().join("started");
    let completed = control.path().join("completed");
    let quote = |p: &std::path::Path| format!("'{}'", p.to_string_lossy().replace('\'', "'\"'\"'"));
    sqlx::query("UPDATE task SET task_state_config=? WHERE id=?").bind(json!({"review":{"ci_steps":[format!("touch {}; sleep 120; touch {}",quote(&started),quote(&completed))]}}).to_string()).bind(&fixture.task.id).execute(fixture.db.pool()).await.unwrap();
    let result = fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &fixture.task.id,
            "review",
            fixture.task.version,
            &fixture.workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "CI entry",
            false,
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    assert_eq!(result.pending_steps, 1);
    let checks = fixture.engine.check_worker_or_embedded();
    let (stop, signal) = tokio::sync::watch::channel(false);
    let worker = Arc::new(TaskStepWorker::new(fixture.engine.clone())).start(signal.clone());
    let periodic = crate::worker_runtime::PeriodicWorkers::new(fixture.db.clone());
    let check_worker = checks.start(&periodic, signal);
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        while !started.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the check runner starts the CI step");
    let hooks = |status: &'static str| {
        let db = fixture.db.clone();
        let id = fixture.task.id.clone();
        async move {
            db.task_steps(&id)
                .await
                .unwrap()
                .into_iter()
                .filter(|step| {
                    step.kind == "hooks"
                        && step.expected_status == "review"
                        && step.status == status
                })
                .count()
        }
    };
    assert_eq!(
        hooks("suspended").await,
        1,
        "the hooks step waits suspended"
    );
    assert_eq!(
        fixture.db.pending_steps(&fixture.task.id).await.unwrap(),
        0,
        "a suspended step is not a pending step"
    );
    assert!(fixture
        .db
        .entry_hooks_pending(&fixture.task.id)
        .await
        .unwrap());
    let reviews = db::ReviewRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap();
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, db::ReviewStatus::Running);
    let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    let result = fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &current.id,
            "cancelled",
            current.version,
            &fixture.workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "Cancel during CI",
            false,
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    assert_eq!(result.task.status, "cancelled");
    assert!(!completed.exists());
    // The step worker wakes the step that left its entry and supersedes it.
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while hooks("superseded").await != 1 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the suspended step is superseded");
    assert_eq!(hooks("suspended").await, 0);
    // No review attempt stays running, and the run gives its slot back.
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let reviews = db::ReviewRepo::list_by_task(&*fixture.db, &fixture.task.id)
                .await
                .unwrap();
            let live: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM check_run WHERE state NOT IN ('succeeded','failed','timed_out','cancelled','infrastructure_failed')",
            )
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
            if reviews.len() == 1 && reviews[0].status == db::ReviewStatus::Cancelled && live == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the review attempt is cancelled and the check run stops");
    assert!(!completed.exists());
    let cancelled: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM check_consumer WHERE cancelled_at IS NOT NULL")
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    assert_eq!(cancelled, 1);
    stop.send(true).unwrap();
    worker.await.unwrap();
    check_worker.await.unwrap();
}

async fn enter_review(
    fixture: &FailedCiFixture,
    reason: &str,
) -> crate::workflow::engine::TransitionResult {
    let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &current.id,
            "review",
            current.version,
            &fixture.workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            reason,
            false,
            fixture.workflow_authority().await,
        )
        .await
        .unwrap()
}
async fn count(fixture: &FailedCiFixture, sql: &str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(fixture.db.pool())
        .await
        .unwrap()
}

// A red review entry through the runner: one run, one review attempt, the
// evidence the inline path wrote, and no attempt charged for the wait.
#[tokio::test]
async fn red_review_ci_through_the_runner_keeps_the_review_evidence() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    let result = enter_review(&fixture, "CI entry").await;
    let result = drain_result(fixture.engine.clone(), result).await;
    let review = result.review.expect("the entry opened a review attempt");
    assert_eq!(review.status, db::ReviewStatus::Failed);
    let details: serde_json::Value = serde_json::from_str(&review.step_results_json).unwrap();
    let steps = details["ci_steps"].as_array().unwrap();
    assert_eq!(steps.len(), 1);
    let step = &steps[0];
    assert_eq!(step["index"], 0);
    assert_eq!(
        step["command"],
        "echo 'entry-ci-failed [conflict-handoff]'; exit 101"
    );
    assert_eq!(step["exit_code"], 101);
    assert!(step["output_tail"]
        .as_str()
        .unwrap()
        .contains("entry-ci-failed [conflict-handoff]"));
    assert!(step["stderr_tail"].is_string());
    assert!(step["started_at"].as_str().unwrap() <= step["finished_at"].as_str().unwrap());
    assert!(step.get("rerun_after_interruption").is_none());
    let hooks: Vec<_> = fixture
        .db
        .task_steps(&fixture.task.id)
        .await
        .unwrap()
        .into_iter()
        .filter(|step| step.kind == "hooks" && step.expected_status == "review")
        .collect();
    assert_eq!(hooks.len(), 1);
    assert_eq!(step["step_id"], hooks[0].id.as_str());
    assert_ne!(hooks[0].status, "suspended");
    assert_eq!(hooks[0].attempts, 1, "the suspension charged no attempt");
    assert!(hooks[0].suspended_until.is_none());
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 1);
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM check_result WHERE outcome='fail'"
        )
        .await,
        1
    );
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM review").await, 1);
    // The settled entry is nobody's authority any more: a late or repeated
    // delivery finds no review-entry attempt to answer, and the integration
    // family never names one for a Task in review.
    use crate::check_runner::consumer::CheckConsumerFamily;
    let epoch = db::CheckDeliveryRepo::live_task_epoch(&*fixture.db, &fixture.task.id)
        .await
        .unwrap()
        .unwrap();
    let entry = crate::check_runner::review_entry::ReviewEntryChecks::new(fixture.db.clone());
    assert_eq!(
        entry
            .current_authority(&fixture.task.id, epoch)
            .await
            .unwrap(),
        None
    );
    let integration = crate::integration_steps::IntegrationCheckFamily::new(Arc::new(
        crate::integration_steps::IntegrationSteps::new(fixture.engine.clone()),
    ));
    assert_eq!(
        integration
            .current_authority(&fixture.task.id, epoch)
            .await
            .unwrap(),
        None
    );
}

// The server stops while the hooks step waits for its check. The rebuilt
// runtime finishes the same run and the woken step settles the same review
// attempt: one attempt, one run.
#[tokio::test]
async fn restart_while_review_ci_is_suspended_settles_the_same_review_attempt() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    let result = enter_review(&fixture, "CI entry").await;
    assert_eq!(result.pending_steps, 1);
    // Compose the check runtime but run no check worker: the step suspends
    // and its run stays queued, as if the server died right there.
    let _ = fixture.engine.check_worker_or_embedded();
    let (stop, signal) = tokio::sync::watch::channel(false);
    let worker = Arc::new(TaskStepWorker::new(fixture.engine.clone())).start(signal);
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let suspended = fixture
                .db
                .task_steps(&fixture.task.id)
                .await
                .unwrap()
                .iter()
                .any(|step| step.kind == "hooks" && step.status == "suspended");
            if suspended {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the hooks step suspends on its check");
    stop.send(true).unwrap();
    worker.await.unwrap();
    use crate::check_runner::consumer::CheckConsumerFamily;
    let epoch = db::CheckDeliveryRepo::live_task_epoch(&*fixture.db, &fixture.task.id)
        .await
        .unwrap()
        .unwrap();
    let waiting = db::ReviewRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap();
    assert_eq!(waiting.len(), 1);
    assert_eq!(waiting[0].status, db::ReviewStatus::Running);
    // While it waits, the entry family names this attempt and the
    // integration family names nothing: neither can take the other's answer.
    assert_eq!(
        crate::check_runner::review_entry::ReviewEntryChecks::new(fixture.db.clone())
            .current_authority(&fixture.task.id, epoch)
            .await
            .unwrap(),
        Some(waiting[0].id.clone())
    );
    assert_eq!(
        crate::integration_steps::IntegrationCheckFamily::new(Arc::new(
            crate::integration_steps::IntegrationSteps::new(fixture.engine.clone()),
        ))
        .current_authority(&fixture.task.id, epoch)
        .await
        .unwrap(),
        None
    );
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 1);
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_result").await,
        0
    );

    // A new runtime on the same database.
    let mut restarted = engine(fixture.db.clone(), Arc::new(EventBus::new(32)));
    restarted.workspace_root = fixture._workspace_root.path().to_path_buf();
    restarted = restarted.with_workspace_root(fixture._workspace_root.path().to_path_buf());
    drain(restarted, &fixture.task.id).await;
    let reviews = db::ReviewRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap();
    assert_eq!(reviews.len(), 1, "the woken step opens no second attempt");
    assert_eq!(reviews[0].id, waiting[0].id);
    assert_eq!(reviews[0].status, db::ReviewStatus::Failed);
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 1);
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_result").await,
        1
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_step WHERE status='suspended'"
        )
        .await,
        0
    );
}

// The machine's only run slot is held by another Task's run when this Task
// enters review. Its CI run is queued, the Task states that it waits for a
// slot, and a drain ends there instead of sweeping for a slot nothing in it
// can free (on a 4-core CI runner, run cap 2, that sweep ran out the drain's
// clock: next/v0.14 a3a08e50). When the other run ends, the same consumer's
// run is admitted, runs and settles the same review attempt.
#[tokio::test]
async fn review_ci_behind_a_full_machine_waits_for_a_slot_and_runs_when_one_frees() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    fixture
        .db
        .server_run_cap
        .set(Some(1), 1, "slot-test-machine");
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO task(id,project_id,title,status,priority,created_at,updated_at) VALUES ('slot-holder',?,'holder','in_progress',0,?,?)")
        .bind(&fixture.task.project_id)
        .bind(&now)
        .bind(&now)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES ('slot-holder-run','slot-holder','coder','running',?,?)")
        .bind(&now)
        .bind(&now)
        .execute(fixture.db.pool())
        .await
        .unwrap();

    let result = enter_review(&fixture, "CI entry").await;
    assert_eq!(result.pending_steps, 1);
    let started = std::time::Instant::now();
    let waiting = drain(fixture.engine.clone(), &fixture.task.id).await;
    assert!(
        started.elapsed() < std::time::Duration::from_secs(120),
        "the drain ends on the stated slot wait, not on its clock"
    );
    assert_eq!(waiting.status, "review");
    let (wait, _) = waiting
        .condition
        .check_witness()
        .expect("the Task states its check wait");
    assert_eq!(wait.phase, api_types::CheckWaitPhase::Slot);
    assert!(fixture
        .db
        .awaited_check_waits_for_slot(&fixture.task.id)
        .await
        .unwrap());
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM check_run WHERE state='queued' AND capacity_wait_since IS NOT NULL"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_step WHERE status='suspended'"
        )
        .await,
        1
    );
    let reviews = db::ReviewRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap();
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, db::ReviewStatus::Running);
    // Still full: another drain changes nothing and ends as quickly.
    drain(fixture.engine.clone(), &fixture.task.id).await;
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 1);
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_result").await,
        0
    );

    // The other run ends: the slot is free.
    sqlx::query("UPDATE execution SET status='completed' WHERE id='slot-holder-run'")
        .execute(fixture.db.pool())
        .await
        .unwrap();
    drain(fixture.engine.clone(), &fixture.task.id).await;
    let settled = db::ReviewRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap();
    assert_eq!(settled.len(), 1, "the same review attempt is settled");
    assert_eq!(settled[0].id, reviews[0].id);
    assert_eq!(settled[0].status, db::ReviewStatus::Failed);
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 1);
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM check_result WHERE outcome='fail'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_step WHERE status='suspended'"
        )
        .await,
        0
    );
    assert!(!fixture
        .db
        .awaited_check_waits_for_slot(&fixture.task.id)
        .await
        .unwrap());
}

// A second review entry of the same commit in the same worktree asks again
// and is answered from the stored result: no second run.
#[tokio::test]
async fn re_review_of_an_unchanged_commit_runs_no_ci() {
    let mut fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    fixture
        .workflow
        .states
        .iter_mut()
        .find(|state| state.name == "review")
        .unwrap()
        .gate_config
        .as_mut()
        .unwrap()
        .requires_user_approval = Some(true);
    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(serde_json::to_string(&fixture.workflow).unwrap())
        .bind(&fixture.task.project_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(
            json!({"retry_budgets":{"review":3},"review":{"ci_steps":["echo reviewed"]}})
                .to_string(),
        )
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let first = enter_review(&fixture, "first entry").await;
    let first = drain_result(fixture.engine.clone(), first).await;
    assert_eq!(first.task.status, "review");
    let review = first.review.unwrap();
    assert_eq!(review.status, db::ReviewStatus::AwaitingHuman);
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 1);

    let back = fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &first.task.id,
            "in_progress",
            first.task.version,
            &fixture.workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "send back",
            true,
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    drain_result(fixture.engine.clone(), back).await;
    let second = enter_review(&fixture, "second entry").await;
    let second = drain_result(fixture.engine.clone(), second).await;
    let reviews = db::ReviewRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap();
    assert_eq!(reviews.len(), 2, "state {}", second.task.status);
    let latest = reviews.iter().max_by_key(|r| r.attempt_number).unwrap();
    let details: serde_json::Value = serde_json::from_str(&latest.step_results_json).unwrap();
    assert_eq!(details["ci_steps"][0]["exit_code"], 0);
    assert_eq!(details["ci_steps"][0]["output_tail"], "reviewed\n");
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        2,
        "each entry asked"
    );
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_run").await,
        1,
        "the second entry reused the first result"
    );
    // A new commit in the worktree is a new identity: it runs.
    let worktree = fixture.workspace.embedded_worktree_path_for_backend();
    std::fs::write(std::path::Path::new(&worktree).join("change.txt"), "x\n").unwrap();
    for args in [vec!["add", "-A"], vec!["commit", "-m", "change"]] {
        assert!(std::process::Command::new("git")
            .args(["-c", "user.name=t", "-c", "user.email=t@example.com"])
            .args(&args)
            .current_dir(worktree)
            .status()
            .unwrap()
            .success());
    }
    let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    let back = fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &current.id,
            "in_progress",
            current.version,
            &fixture.workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "send back again",
            true,
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    drain_result(fixture.engine.clone(), back).await;
    let third = enter_review(&fixture, "third entry").await;
    drain_result(fixture.engine.clone(), third).await;
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_run").await,
        2,
        "a changed commit runs"
    );
}

/// A fixture whose review entry stays in `review` after green CI (the gate
/// asks for the owner's approval), with the given CI steps.
async fn approval_ci_fixture(ci_steps: serde_json::Value) -> FailedCiFixture {
    let mut fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    fixture
        .workflow
        .states
        .iter_mut()
        .find(|state| state.name == "review")
        .unwrap()
        .gate_config
        .as_mut()
        .unwrap()
        .requires_user_approval = Some(true);
    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(serde_json::to_string(&fixture.workflow).unwrap())
        .bind(&fixture.task.project_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(json!({"retry_budgets":{"review":3},"review":{"ci_steps":ci_steps}}).to_string())
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    fixture
}

/// Send the Task back to `in_progress` and into `review` again, drained.
async fn re_enter_review(fixture: &FailedCiFixture, reason: &str) {
    let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    let back = fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &current.id,
            "in_progress",
            current.version,
            &fixture.workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            reason,
            true,
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    drain_result(fixture.engine.clone(), back).await;
    let entered = enter_review(fixture, reason).await;
    let entered = drain_result(fixture.engine.clone(), entered).await;
    assert_eq!(entered.task.status, "review", "{reason}");
}

// Green evidence parity: the Review row carries what the inline run wrote.
// `output_tail` is stdout followed by stderr, each tail is at most 4096
// bytes, and a blank configured step is neither run nor listed.
#[tokio::test]
async fn green_review_ci_through_the_runner_keeps_the_review_evidence() {
    let fixture = approval_ci_fixture(json!([
        "printf out; printf err >&2",
        "   ",
        "head -c 6000 /dev/zero | tr '\\0' a"
    ]))
    .await;
    let result = enter_review(&fixture, "CI entry").await;
    let result = drain_result(fixture.engine.clone(), result).await;
    assert_eq!(result.task.status, "review");
    let review = result.review.expect("the entry opened a review attempt");
    assert_eq!(review.status, db::ReviewStatus::AwaitingHuman);
    let details: serde_json::Value = serde_json::from_str(&review.step_results_json).unwrap();
    let steps = details["ci_steps"].as_array().unwrap();
    assert_eq!(steps.len(), 2, "the blank step is not listed: {details}");
    assert_eq!(steps[0]["command"], "printf out; printf err >&2");
    assert_eq!(steps[0]["exit_code"], 0);
    let tail = steps[0]["output_tail"].as_str().unwrap();
    assert!(
        tail.starts_with("out") && tail.ends_with("err"),
        "stdout then stderr: {tail:?}"
    );
    assert_eq!(steps[0]["stderr_tail"], "err");
    assert_eq!(steps[1]["exit_code"], 0);
    let long = steps[1]["output_tail"].as_str().unwrap();
    assert_eq!(long.len(), 4096, "a tail keeps the last 4096 bytes");
    assert!(long.bytes().all(|byte| byte == b'a'));
    for step in steps {
        assert!(step["started_at"].as_str().unwrap() <= step["finished_at"].as_str().unwrap());
        assert!(step.get("rerun_after_interruption").is_none());
    }
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 1);
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM check_result WHERE outcome='pass'"
        )
        .await,
        1
    );
}

// The wall limit of a review-entry run, through the runner: the run is
// stopped at its limit, the result is `timed_out`, the review attempt is
// cancelled and the hook fails; nothing stays suspended.
#[tokio::test]
async fn review_ci_that_exceeds_its_wall_limit_times_out_through_the_runner() {
    let fixture = approval_ci_fixture(json!(["sleep 60"])).await;
    crate::workflow::actions::set_entry_check_wall_seconds_for_test(&fixture.task.id, 2);
    let began = std::time::Instant::now();
    let result = enter_review(&fixture, "CI entry").await;
    let result = drain_result(fixture.engine.clone(), result).await;
    assert!(
        began.elapsed() < std::time::Duration::from_secs(45),
        "the run stops at its wall limit, not when the command ends"
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM check_run WHERE applied_timeout_seconds=2"
        )
        .await,
        1,
        "the run carries the entry's wall limit"
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM check_result WHERE outcome='timed_out'"
        )
        .await,
        1
    );
    let review = result.review.expect("the entry opened a review attempt");
    assert_eq!(review.status, db::ReviewStatus::Cancelled);
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM review").await, 1);
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_step WHERE status='suspended'"
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_hook_checkpoint WHERE result_json LIKE '%review command timed out%'"
        )
        .await,
        1,
        "the hook failed as a CI timeout does"
    );
}

// What makes a re-review run again: a changed Project environment, and a
// dirty tree, whose result is a verdict but is never reused.
#[tokio::test]
async fn re_review_runs_again_for_a_changed_project_environment_and_a_dirty_tree() {
    let fixture = approval_ci_fixture(json!(["echo reviewed"])).await;
    let first = enter_review(&fixture, "first entry").await;
    drain_result(fixture.engine.clone(), first).await;
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 1);
    re_enter_review(&fixture, "unchanged").await;
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_run").await,
        1,
        "an unchanged entry is answered from the stored result"
    );

    sqlx::query("UPDATE project SET settings=json_set(CASE WHEN json_valid(settings) THEN settings ELSE '{}' END,'$.environment',json('{\"env\":{\"REVIEW_ENTRY_FLAG\":\"changed\"}}')) WHERE id=?")
        .bind(&fixture.task.project_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    re_enter_review(&fixture, "changed Project environment").await;
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_run").await,
        2,
        "a changed Project environment runs"
    );
    re_enter_review(&fixture, "same Project environment").await;
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_run").await,
        2,
        "the new environment's result is reused in turn"
    );

    let worktree = fixture.workspace.embedded_worktree_path_for_backend();
    std::fs::write(
        std::path::Path::new(&worktree).join("untracked.txt"),
        "dirty\n",
    )
    .unwrap();
    re_enter_review(&fixture, "dirty tree").await;
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_run").await,
        3,
        "a dirty tree runs"
    );
    re_enter_review(&fixture, "still dirty").await;
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_run").await,
        4,
        "a dirty tree's result is never reused"
    );
    let reviews = db::ReviewRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap();
    assert_eq!(reviews.len(), 6, "every entry opened its own attempt");
    assert!(reviews
        .iter()
        .all(|review| review.status != db::ReviewStatus::Running));
}

// Hold while review-entry CI runs: the suspended step is superseded and the
// wait is cleaned up in the Hold's own step. The consumer is cancelled (a
// late result is stale by authority and applied to nothing), the review
// attempt is closed, the Task no longer states a check wait, and the run is
// stopped so it holds no slot.
#[tokio::test]
async fn hold_while_review_ci_is_suspended_closes_the_wait_and_frees_the_slot() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    let control = TempDir::new().unwrap();
    let started = control.path().join("started");
    let completed = control.path().join("completed");
    let quote = |p: &std::path::Path| format!("'{}'", p.to_string_lossy().replace('\'', "'\"'\"'"));
    sqlx::query("UPDATE task SET task_state_config=? WHERE id=?").bind(json!({"review":{"ci_steps":[format!("touch {}; sleep 120; touch {}",quote(&started),quote(&completed))]}}).to_string()).bind(&fixture.task.id).execute(fixture.db.pool()).await.unwrap();
    enter_review(&fixture, "CI entry").await;
    let checks = fixture.engine.check_worker_or_embedded();
    let (stop, signal) = tokio::sync::watch::channel(false);
    let worker = Arc::new(TaskStepWorker::new(fixture.engine.clone())).start(signal.clone());
    let periodic = crate::worker_runtime::PeriodicWorkers::new(fixture.db.clone());
    let check_worker = checks.start(&periodic, signal);
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        while !started.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the check runner starts the CI step");
    let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(current.condition.check_witness().is_some());
    let held = fixture
        .engine
        .perform_task_action(
            &current.id,
            api_types::TaskAction::Hold { reason: None },
            current.version,
        )
        .await
        .expect("Hold is accepted while CI runs");
    assert_ne!(held.task.status, "cancelled");
    assert!(
        held.task.condition.check_witness().is_none(),
        "the held Task no longer waits for the check: {:?}",
        held.task.condition
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_step WHERE kind='hooks' AND expected_status='review' AND status='superseded'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_step WHERE status='suspended'"
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM check_consumer WHERE cancelled_at IS NULL"
        )
        .await,
        0,
        "the consumer is cancelled with the step"
    );
    let reviews = db::ReviewRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap();
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, db::ReviewStatus::Cancelled);
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while count(
            &fixture,
            "SELECT COUNT(*) FROM check_run WHERE state NOT IN ('succeeded','failed','cancelled')",
        )
        .await
            != 0
        {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the run nobody waits for is stopped and frees its slot");
    assert!(!completed.exists());
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM check_consumer WHERE applied_at IS NOT NULL"
        )
        .await,
        0,
        "the late result is applied to nothing"
    );
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM review").await, 1);
    stop.send(true).unwrap();
    worker.await.unwrap();
    check_worker.await.unwrap();
}

// A wait nothing will answer does not last: when the consumer is cancelled
// under a suspended step (and nobody superseded the step), the step's
// deadline wakes it, it finds the consumer lost, cancels the review attempt
// and fails the hook instead of suspending again.
#[tokio::test]
async fn a_suspended_step_whose_consumer_is_lost_fails_instead_of_waiting_forever() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    enter_review(&fixture, "CI entry").await;
    // Composed, but no check worker runs: the run stays queued.
    let _ = fixture.engine.check_worker_or_embedded();
    let (stop, signal) = tokio::sync::watch::channel(false);
    let worker = Arc::new(TaskStepWorker::new(fixture.engine.clone())).start(signal);
    let suspended = "SELECT COUNT(*) FROM task_step WHERE kind='hooks' AND status='suspended'";
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while count(&fixture, suspended).await != 1 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the hooks step suspends on its check");
    let until: String =
        sqlx::query_scalar("SELECT suspended_until FROM task_step WHERE status='suspended'")
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    assert!(
        until > (chrono::Utc::now() + chrono::Duration::seconds(3600)).to_rfc3339(),
        "the deadline lies past the run's wall limit: {until}"
    );
    sqlx::query("UPDATE check_consumer SET cancelled_at=?")
        .bind(now_rfc3339())
        .execute(fixture.db.pool())
        .await
        .unwrap();
    // The deadline passes.
    sqlx::query(
        "UPDATE task_step SET suspended_until='2000-01-01T00:00:00Z' WHERE status='suspended'",
    )
    .execute(fixture.db.pool())
    .await
    .unwrap();
    fixture.db.domain_event_notify().notify_waiters();
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        loop {
            let reviews = db::ReviewRepo::list_by_task(&*fixture.db, &fixture.task.id)
                .await
                .unwrap();
            if count(&fixture, suspended).await == 0
                && reviews[0].status == db::ReviewStatus::Cancelled
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the woken step gives up on the lost consumer");
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM review").await, 1);
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_hook_checkpoint WHERE result_json LIKE '%review check was lost%'"
        )
        .await,
        1,
        "the failure is on the Task's history"
    );
    stop.send(true).unwrap();
    worker.await.unwrap();
}

/// Sweep the check worker until `consumer_id` has its delivery step, and
/// return that step's id and envelope. The step is not executed.
async fn delivery_of(
    fixture: &FailedCiFixture,
    checks: &Arc<crate::check_runner::worker::CheckRunWorker>,
    consumer_id: &str,
) -> (String, db::CheckResultDelivery) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let step: Option<String> =
            sqlx::query_scalar("SELECT delivery_step_id FROM check_consumer WHERE id=?")
                .bind(consumer_id)
                .fetch_one(fixture.db.pool())
                .await
                .unwrap();
        if let Some(step) = step {
            let payload: String =
                sqlx::query_scalar("SELECT payload_json FROM task_step WHERE id=?")
                    .bind(&step)
                    .fetch_one(fixture.db.pool())
                    .await
                    .unwrap();
            let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();
            assert_eq!(payload["operation"], "apply_check_result");
            return (
                step,
                serde_json::from_value(payload["arguments"].clone()).unwrap(),
            );
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no delivery step for {consumer_id}"
        );
        let mut jobs = tokio::task::JoinSet::new();
        checks.sweep(&mut jobs).await.unwrap();
        while let Some(job) = jobs.join_next().await {
            job.unwrap().unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

// The cross-family fence, at the delivery: review entry and the integration
// queue share the one check-witness slot of a Task status entry. A delivery
// is applied only through its own consumer's family and step, under that
// family's current authority. An integration delivery that arrives while
// review entry waits is stale and leaves the entry untouched; neither
// envelope can be re-labelled for, or carried by the step of, the other.
#[tokio::test]
async fn a_check_delivery_is_never_consumed_by_the_other_family() {
    use crate::check_runner::consumer::{CheckApplyOutcome, CheckStaleReason, TaskCheckRequest};
    let fixture = approval_ci_fixture(json!(["echo reviewed"])).await;
    enter_review(&fixture, "CI entry").await;
    let checks = fixture.engine.check_worker_or_embedded();
    let consumers = fixture
        .engine
        .check_consumers()
        .expect("the check runtime is composed");
    let (stop, signal) = tokio::sync::watch::channel(false);
    let worker = Arc::new(TaskStepWorker::new(fixture.engine.clone())).start(signal);
    let suspended = "SELECT COUNT(*) FROM task_step WHERE kind='hooks' AND status='suspended'";
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while count(&fixture, suspended).await != 1 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the hooks step suspends on its check");
    stop.send(true).unwrap();
    worker.await.unwrap();

    let entry_id: String = sqlx::query_scalar("SELECT id FROM check_consumer WHERE origin='entry'")
        .fetch_one(fixture.db.pool())
        .await
        .unwrap();
    let (entry_step, entry_delivery) = delivery_of(&fixture, &checks, &entry_id).await;
    assert_eq!(entry_delivery.origin, db::CheckConsumerOrigin::Entry);

    // The integration family asks for the same commit under its own
    // authority, from a claimed step of the Task, in the same status entry.
    let task = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    let epoch = db::CheckDeliveryRepo::live_task_epoch(&*fixture.db, &task.id)
        .await
        .unwrap()
        .unwrap();
    let run = db::CheckWorkerRepo::check_worker_record(&*fixture.db, &entry_delivery.run_id)
        .await
        .unwrap()
        .run;
    let asking = new_uuid_v4();
    db::TaskStepRepo::enqueue_step(
        &*fixture.db,
        &db::EnqueueTaskStep {
            kind: "command".to_owned(),
            id: asking.clone(),
            task_id: task.id.clone(),
            payload_json: "{}".to_owned(),
            causation_step_id: None,
            causation_key: asking.clone(),
            chain_id: asking.clone(),
            chain_position: 1,
            expected_status: task.status.clone(),
            expected_version: task.version,
            expected_epoch: None,
            lane: "fast".to_owned(),
            available_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE task_step SET status='claimed',claimed_by='integration-asker',lease_until=? WHERE id=?")
        .bind(db::task_writer::lease_deadline())
        .bind(&asking)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let step = fixture
        .db
        .task_steps(&task.id)
        .await
        .unwrap()
        .into_iter()
        .find(|step| step.id == asking)
        .unwrap();
    let requested = db::task_writer::in_task_step(
        step,
        consumers.request(TaskCheckRequest {
            task_id: task.id.clone(),
            status_epoch: epoch,
            authority: "integration-attempt-1".to_owned(),
            origin: db::CheckConsumerOrigin::Integration,
            purpose: api_types::CheckPurpose::QueueHeadCi,
            identity: run.identity.clone(),
            workspace_id: run.workspace_id.clone(),
            machine_id: None,
            wall_timeout_seconds: 60,
        }),
    )
    .await
    .expect("the integration family asks");
    sqlx::query("DELETE FROM task_step WHERE id=?")
        .bind(&asking)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let integration_id = requested.consumer.id.clone();
    assert_ne!(
        integration_id, entry_id,
        "one consumer per family and authority"
    );
    let (integration_step, integration_delivery) =
        delivery_of(&fixture, &checks, &integration_id).await;
    assert_ne!(integration_step, entry_step);

    // Neither envelope can be re-labelled for the other family ...
    assert!(consumers
        .apply(
            &entry_step,
            &db::CheckResultDelivery {
                origin: db::CheckConsumerOrigin::Integration,
                ..entry_delivery.clone()
            },
        )
        .await
        .is_err());
    assert!(consumers
        .apply(
            &integration_step,
            &db::CheckResultDelivery {
                origin: db::CheckConsumerOrigin::Entry,
                ..integration_delivery.clone()
            },
        )
        .await
        .is_err());
    // ... nor carried by the other consumer's delivery step.
    assert!(consumers
        .apply(&integration_step, &entry_delivery)
        .await
        .is_err());
    assert!(consumers
        .apply(&entry_step, &integration_delivery)
        .await
        .is_err());

    // The integration delivery arrives while review entry waits: the Task is
    // in `review`, the integration family names no authority, so it is stale
    // and the waiting entry is not touched by it.
    assert_eq!(
        consumers
            .apply(&integration_step, &integration_delivery)
            .await
            .unwrap(),
        CheckApplyOutcome::Stale(CheckStaleReason::Authority)
    );
    assert_eq!(count(&fixture, suspended).await, 1, "the entry still waits");
    let reviews = db::ReviewRepo::list_by_task(&*fixture.db, &task.id)
        .await
        .unwrap();
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, db::ReviewStatus::Running);
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM check_consumer WHERE applied_at IS NOT NULL"
        )
        .await,
        0
    );

    // The entry's own delivery wakes the entry's step, and only that.
    assert_eq!(
        consumers.apply(&entry_step, &entry_delivery).await.unwrap(),
        CheckApplyOutcome::Applied
    );
    assert_eq!(count(&fixture, suspended).await, 0);
    assert_eq!(
        consumers.apply(&entry_step, &entry_delivery).await.unwrap(),
        CheckApplyOutcome::AlreadyApplied
    );
    let settled = drain(fixture.engine.clone(), &task.id).await;
    assert_eq!(settled.status, "review");
    let reviews = db::ReviewRepo::list_by_task(&*fixture.db, &task.id)
        .await
        .unwrap();
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, db::ReviewStatus::AwaitingHuman);
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM check_consumer WHERE origin='integration' AND applied_at IS NOT NULL"
        )
        .await,
        0,
        "the integration answer was applied to nothing"
    );
    assert!(
        settled.condition.check_witness().is_none(),
        "the shared witness slot is free once both deliveries are handled: {:?}",
        settled.condition
    );
}

// A Log-policy effect failure settles its step `failed` and is logged; it
// writes no Task annotation and does not block, as before durable hooks.
#[tokio::test]
async fn log_policy_effect_failure_settles_step_failed_without_blocking_task() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "log-policy-failure";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    assign_agent_role_without_agent(&db, task_id, default_roles::CODER).await;
    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state(
                    default_states::TODO,
                    StateKind::Initial,
                    None,
                    StateHooks::default(),
                ),
                WorkflowTrigger::Accept,
                default_states::IN_PROGRESS,
            ),
            state(
                default_states::IN_PROGRESS,
                StateKind::Active,
                Some(default_roles::CODER),
                StateHooks {
                    on_enter: vec![hook("notify_role_holder", FailurePolicy::Log)],
                    ..StateHooks::default()
                },
            ),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    let current = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .unwrap()
        .unwrap();
    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .transition(
            task_id,
            default_states::IN_PROGRESS,
            current.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work",
            false,
            Default::default(),
        )
        .await
        .unwrap();
    let result = drain_result(engine(db.clone(), Arc::new(EventBus::new(32))), result).await;
    let steps: Vec<_> = db
        .task_steps(task_id)
        .await
        .unwrap()
        .into_iter()
        .filter(|step| step.kind == "hooks")
        .collect();
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].status, "failed");
    assert!(steps[0]
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("invalid coder role assignment")));
    assert!(result.task.blocked_json.is_none());
    assert!(result.task.entry_barrier_json.is_none());
    assert!(
        result.task.error_annotation.is_none(),
        "Log-policy failure installed an annotation: {:?}",
        result.task.error_annotation
    );
}
