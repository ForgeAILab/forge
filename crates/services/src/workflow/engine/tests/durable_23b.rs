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
async fn failed_transition_leaves_no_workflow_snapshot_for_board_or_ordinary_entry() {
    for board_move in [false, true] {
        let db = Arc::new(sqlite_db().await);
        seed_project_repo_and_task(&db, "snapshot-rollback", "start").await;
        let task = TaskRepo::get_by_id(&*db, "snapshot-rollback", false)
            .await
            .unwrap()
            .unwrap();
        // Fail after enqueue, so rollback must include the frozen definition,
        // the hooks step and the transition itself.
        sqlx::query("CREATE TRIGGER reject_transition_event BEFORE INSERT ON domain_event WHEN NEW.event_type='task.transitioned' BEGIN SELECT RAISE(ABORT,'transition event failed'); END")
            .execute(db.pool()).await.unwrap();
        let service = engine(db.clone(), Arc::new(EventBus::new(8)));
        let workflow = cascade_chain_workflow(1);
        let actor = api_types::Actor::user(api_types::UserActionSource::Api);
        let execution = service.workflow_execution();
        let result = if board_move {
            execution
                .move_task(
                    &task.id,
                    "step_0",
                    task.version,
                    &workflow,
                    &actor,
                    "snapshot rollback",
                    super::super::BoardMoveRequest {
                        operation_id: new_uuid_v4(),
                        project_id: task.project_id.clone(),
                        board_revision: db::TaskBoardRepo::board_revision(&*db, &task.project_id)
                            .await
                            .unwrap(),
                        target_column_statuses: vec!["step_0".into()],
                        before_id: None,
                        after_id: None,
                    },
                )
                .await
        } else {
            execution
                .transition(
                    &task.id,
                    "step_0",
                    task.version,
                    &workflow,
                    &actor,
                    "snapshot rollback",
                    false,
                    Default::default(),
                )
                .await
        };
        let Err(error) = result else {
            panic!("the injected event write must fail")
        };
        assert!(
            error.to_string().contains("transition event failed"),
            "{error}"
        );
        assert_eq!(
            TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .unwrap()
                .unwrap()
                .status,
            "start"
        );
        // The outer command retains its durable failure reply. The rolled
        // back transition must leave no hooks or cascade step behind.
        assert!(db
            .task_steps(&task.id)
            .await
            .unwrap()
            .iter()
            .all(|step| step.kind == "command"));
        assert!(TransitionLogRepo::list_by_task(&*db, &task.id)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_step_workflow")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            0,
            "board_move={board_move}"
        );
    }
}

#[tokio::test]
async fn transition_without_hooks_creates_no_workflow_snapshot() {
    let db = Arc::new(sqlite_db().await);
    seed_project_repo_and_task(&db, "snapshot-unused", "start").await;
    let mut workflow = cascade_chain_workflow(1);
    for state in &mut workflow.states {
        state.hooks = StateHooks::default();
    }
    let service = engine(db.clone(), Arc::new(EventBus::new(8)));
    let result = service
        .workflow_execution()
        .transition(
            "snapshot-unused",
            "step_0",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Api),
            "entry without hooks",
            false,
            Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(result.task.status, "step_0");
    assert_eq!(result.pending_steps, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_step_workflow")
            .fetch_one(db.pool())
            .await
            .unwrap(),
        0
    );
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

pub(super) async fn enter_review(
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
pub(super) async fn count(fixture: &FailedCiFixture, sql: &str) -> i64 {
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
pub(super) async fn approval_ci_fixture(ci_steps: serde_json::Value) -> FailedCiFixture {
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
// deadline wakes it and it finds the consumer lost. It closes the attempt
// that asked and asks again for its check instead of waiting on the lost
// consumer (the bound on asking again is tested by
// `an_entry_whose_check_keeps_being_withdrawn_stops_asking_after_three_times`,
// where the hook then fails with "review check was lost").
#[tokio::test]
async fn a_suspended_step_whose_consumer_is_lost_asks_again_instead_of_waiting_forever() {
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
            if reviews.len() == 2
                && count(
                    &fixture,
                    "SELECT COUNT(*) FROM task_step s JOIN check_consumer c ON c.id=s.awaited_consumer_id WHERE s.status='suspended' AND c.cancelled_at IS NULL",
                )
                .await
                    == 1
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the woken step asks again for the lost check");
    let mut reviews = db::ReviewRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap();
    reviews.sort_by_key(|review| review.attempt_number);
    assert_eq!(reviews[0].status, db::ReviewStatus::Cancelled);
    assert!(reviews[0]
        .step_results_json
        .contains("superseded_by_withdrawn_check"));
    assert_eq!(reviews[1].status, db::ReviewStatus::Running);
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        2
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

/// Suspend the hooks step on its queued check, with no check worker running.
pub(super) async fn suspend_on_entry_check(fixture: &FailedCiFixture) {
    let result = enter_review(fixture, "CI entry").await;
    assert_eq!(result.pending_steps, 1);
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
}

async fn review_summaries(fixture: &FailedCiFixture) -> Vec<(db::ReviewStatus, String)> {
    db::ReviewRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap()
        .into_iter()
        .map(|review| (review.status, review.step_results_json))
        .collect()
}

async fn set_project_review_commands(fixture: &FailedCiFixture, command: &str) {
    sqlx::query("UPDATE project SET settings=json_set(CASE WHEN json_valid(settings) THEN settings ELSE '{}' END,'$.default_review_config',json(?)), version=version+1 WHERE id=?")
        .bind(json!({"ci_steps":[command]}).to_string())
        .bind(&fixture.task.project_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
}

/// Wake the suspended hooks step as its deadline would and run the step
/// worker (no check worker) until it waits on another consumer than `before`.
async fn wake_and_resuspend(fixture: &FailedCiFixture, before: &str) -> String {
    fixture
        .db
        .wake_suspended_hooks(&fixture.task.id, before)
        .await
        .unwrap();
    let (stop, signal) = tokio::sync::watch::channel(false);
    let worker = Arc::new(TaskStepWorker::new(fixture.engine.clone())).start(signal);
    let awaited = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let awaited = awaited_consumer(fixture).await;
            if awaited.as_deref().is_some_and(|id| id != before) {
                break awaited.unwrap();
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the hooks step asks again and suspends");
    stop.send(true).unwrap();
    worker.await.unwrap();
    awaited
}

async fn awaited_consumer(fixture: &FailedCiFixture) -> Option<String> {
    fixture
        .db
        .task_steps(&fixture.task.id)
        .await
        .unwrap()
        .into_iter()
        .find(|step| step.kind == "hooks" && step.status == "suspended")
        .and_then(|step| step.awaited_consumer_id)
}

// The Project's review commands change while the entry check of the old
// commands is pending. The finished check answers a question nobody asks any
// more: its verdict is applied to nothing and its attempt is closed. The
// woken step asks again for the current commands inside the same review
// entry, without the owner and without spending review budget.
#[tokio::test]
async fn a_review_config_change_while_the_entry_check_is_pending_asks_again_for_the_new_commands() {
    let fixture = approval_ci_fixture(json!(["echo first-commands"])).await;
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(json!({"retry_budgets":{"review":3}}).to_string())
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    set_project_review_commands(&fixture, "echo first-commands").await;
    suspend_on_entry_check(&fixture).await;
    let entered: i64 = sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=?")
        .bind(&fixture.task.id)
        .fetch_one(fixture.db.pool())
        .await
        .unwrap();
    set_project_review_commands(&fixture, "echo other-commands").await;
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    let reviews = review_summaries(&fixture).await;
    assert_eq!(reviews.len(), 2, "{reviews:?}");
    assert_eq!(reviews[0].0, db::ReviewStatus::Cancelled, "{reviews:?}");
    assert!(reviews[0].1.contains("superseded_by_config_change"));
    assert!(
        !reviews[0].1.contains("first-commands"),
        "the old commands' verdict was written to the attempt: {reviews:?}"
    );
    assert_eq!(
        reviews[1].0,
        db::ReviewStatus::AwaitingHuman,
        "{reviews:?} {:?}",
        task.error_annotation
    );
    assert!(reviews[1].1.contains("other-commands"), "{reviews:?}");
    assert!(!reviews[1].1.contains("first-commands"), "{reviews:?}");
    assert_eq!(task.status, "review");
    assert!(
        task.error_annotation.is_none(),
        "{:?}",
        task.error_annotation
    );
    assert!(task.condition.check_witness().is_none());
    // One new consumer and one new run, in the same status entry.
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 2);
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        2
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(DISTINCT status_epoch) FROM check_consumer"
        )
        .await,
        1
    );
    let now: i64 = sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=?")
        .bind(&fixture.task.id)
        .fetch_one(fixture.db.pool())
        .await
        .unwrap();
    assert_eq!(now, entered, "the Task did not leave and re-enter review");
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_step WHERE kind='hooks'"
        )
        .await,
        1
    );
    assert_eq!(
        db::budget::spent(fixture.db.pool(), &fixture.task.id, "review")
            .await
            .unwrap(),
        0,
        "asking again spends no review budget"
    );
}

// The Task's own review commands change while the entry check is pending. No
// Project version moves, so nothing but the comparison of what the check was
// asked under notices. The stale run is still queued: its consumer is
// cancelled (the run is stopped by the worker) and the entry asks again.
#[tokio::test]
async fn a_task_review_config_change_while_the_entry_check_is_pending_asks_again() {
    let fixture = approval_ci_fixture(json!(["echo first-commands"])).await;
    suspend_on_entry_check(&fixture).await;
    let first = awaited_consumer(&fixture).await.unwrap();
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(
            json!({"retry_budgets":{"review":3},"review":{"ci_steps":["echo other-commands"]}})
                .to_string(),
        )
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let second = wake_and_resuspend(&fixture, &first).await;
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM check_consumer WHERE cancelled_at IS NOT NULL"
        )
        .await,
        1,
        "the stale consumer is cancelled"
    );
    let waiting = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        waiting
            .condition
            .check_witness()
            .map(|(wait, _)| wait.consumer_id.clone()),
        Some(second),
        "the Task states the wait for the check it asked for now"
    );
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    let reviews = review_summaries(&fixture).await;
    assert_eq!(reviews.len(), 2, "{reviews:?}");
    assert_eq!(reviews[0].0, db::ReviewStatus::Cancelled, "{reviews:?}");
    assert!(!reviews[0].1.contains("first-commands"), "{reviews:?}");
    assert_eq!(reviews[1].0, db::ReviewStatus::AwaitingHuman, "{reviews:?}");
    assert!(reviews[1].1.contains("other-commands"), "{reviews:?}");
    assert!(
        task.error_annotation.is_none(),
        "{:?}",
        task.error_annotation
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM check_run WHERE state='succeeded'"
        )
        .await,
        1,
        "only the current commands ran to a verdict"
    );
}

// A configuration that keeps changing does not make the entry ask forever:
// after three re-requests the next change ends the step as before. The
// verdict is applied to nothing and the Task parks typed, with its actions.
#[tokio::test]
async fn an_entry_asks_again_at_most_three_times_then_parks_for_its_owner() {
    let fixture = approval_ci_fixture(json!(["echo commands-0"])).await;
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(json!({"retry_budgets":{"review":3}}).to_string())
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    set_project_review_commands(&fixture, "echo commands-0").await;
    suspend_on_entry_check(&fixture).await;
    let mut awaited = awaited_consumer(&fixture).await.unwrap();
    for round in 1..=3 {
        set_project_review_commands(&fixture, &format!("echo commands-{round}")).await;
        awaited = wake_and_resuspend(&fixture, &awaited).await;
    }
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        4
    );
    // The count is durable and belongs to this entry: it is an effect of
    // the entry's own hooks step (every round above ran on a new worker and
    // read it back from there), so the step of a later entry starts at zero.
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_hook_checkpoint k JOIN task_step s ON s.id=k.step_id WHERE s.status='suspended' AND s.kind='hooks' AND CAST(json_extract(k.effects_json,'$.ci_rerequests') AS INTEGER)=3"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_hook_checkpoint WHERE json_extract(effects_json,'$.ci_rerequests') IS NOT NULL"
        )
        .await,
        1
    );
    set_project_review_commands(&fixture, "echo commands-4").await;
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    let reviews = review_summaries(&fixture).await;
    assert_eq!(reviews.len(), 4, "{reviews:?}");
    assert!(
        reviews
            .iter()
            .all(|review| review.0 == db::ReviewStatus::Cancelled),
        "{reviews:?}"
    );
    assert!(reviews.iter().all(|review| !review.1.contains("commands-")));
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        4,
        "no fifth request"
    );
    assert_eq!(task.status, "review");
    assert!(task.review_passed_at.is_none());
    assert!(task
        .error_annotation
        .as_deref()
        .is_some_and(|annotation| annotation.contains("cascade_failed")));
    match fixture
        .engine
        .perform_task_action(&task.id, api_types::TaskAction::retry(), task.version)
        .await
    {
        Err(ServiceError::TaskActionUnavailable {
            available_actions, ..
        }) => {
            let offered: Vec<String> = available_actions
                .iter()
                .map(|offer| offer.label.clone())
                .collect();
            for label in ["Restart Task", "Request changes", "Cancel Task"] {
                assert!(offered.iter().any(|o| o == label), "{label}: {offered:?}");
            }
        }
        other => panic!("retry on a parked review entry: {:?}", other.map(|_| ())),
    }
}

// A forced Project deletion cancels the Project's check consumers and is
// then refused (a run did not settle in time). From the Task's point of view
// nothing happened: the review entry that lost its check is woken and asks
// again, instead of failing with "review check was lost".
#[tokio::test]
async fn a_refused_project_deletion_re_requests_the_entry_checks_it_cancelled() {
    let fixture = approval_ci_fixture(json!(["echo entry-commands"])).await;
    suspend_on_entry_check(&fixture).await;
    let first = awaited_consumer(&fixture).await.unwrap();
    // Phase two of the forced delete, then the refusal.
    assert_eq!(
        fixture
            .db
            .cancel_project_check_consumers(&fixture.task.project_id)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        fixture
            .engine
            .resume_project_check_waits(&fixture.task.project_id)
            .await
            .unwrap(),
        1
    );
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    let reviews = review_summaries(&fixture).await;
    assert_eq!(reviews.len(), 2, "{reviews:?}");
    assert_eq!(reviews[0].0, db::ReviewStatus::Cancelled, "{reviews:?}");
    assert!(reviews[0].1.contains("superseded_by_withdrawn_check"));
    assert_eq!(
        reviews[1].0,
        db::ReviewStatus::AwaitingHuman,
        "{reviews:?} {:?}",
        task.error_annotation
    );
    assert!(reviews[1].1.contains("entry-commands"), "{reviews:?}");
    assert_eq!(task.status, "review");
    assert!(
        task.error_annotation.is_none(),
        "{:?}",
        task.error_annotation
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM check_consumer WHERE cancelled_at IS NULL"
        )
        .await,
        1
    );
    let second: String =
        sqlx::query_scalar("SELECT id FROM check_consumer WHERE cancelled_at IS NULL")
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    assert_ne!(second, first);
    // The re-ask got a fresh run. The withdrawn run was cancelled in the
    // withdrawal (it never ran and has no result row); the run the second
    // consumer waited for is another one, and it ran.
    let runs: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT c.id,r.state,(SELECT COUNT(*) FROM check_result x WHERE x.run_id=r.id) FROM check_consumer c JOIN check_run r ON r.id=c.run_id ORDER BY c.created_at,c.rowid",
    )
    .fetch_all(fixture.db.pool())
    .await
    .unwrap();
    assert_eq!(
        runs,
        vec![
            (first.clone(), "cancelled".to_owned(), 0),
            (second.clone(), "succeeded".to_owned(), 1),
        ]
    );
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 2);
    assert_eq!(
        db::budget::spent(fixture.db.pool(), &fixture.task.id, "review")
            .await
            .unwrap(),
        0
    );
    // Nothing is left to wake.
    assert_eq!(
        fixture
            .engine
            .resume_project_check_waits(&fixture.task.project_id)
            .await
            .unwrap(),
        0
    );
}

// The usual refusal: the forced delete cancelled the consumers, the run did
// not stop within the bound, and the run is still being stopped when the
// entries are woken. A request made now would join that run and be handed
// its cancellation (a second re-request for one refusal, and after two
// refused deletes a parked Task). The entry waits for the run to end and
// asks once: one new consumer, one new run, one re-request counted.
#[tokio::test]
async fn a_refused_deletion_whose_run_is_still_stopping_asks_once_when_the_run_has_ended() {
    let fixture = approval_ci_fixture(json!(["echo entry-commands"])).await;
    suspend_on_entry_check(&fixture).await;
    let first = awaited_consumer(&fixture).await.unwrap();
    assert_eq!(
        fixture
            .db
            .cancel_project_check_consumers(&fixture.task.project_id)
            .await
            .unwrap(),
        1
    );
    sqlx::query("UPDATE check_run SET state='cancelling'")
        .execute(fixture.db.pool())
        .await
        .unwrap();
    assert_eq!(
        fixture
            .engine
            .resume_project_check_waits(&fixture.task.project_id)
            .await
            .unwrap(),
        1
    );
    let (stop, signal) = tokio::sync::watch::channel(false);
    let worker = Arc::new(TaskStepWorker::new(fixture.engine.clone())).start(signal);
    // Three rechecks pass while the run is being stopped: nothing is asked.
    tokio::time::sleep(std::time::Duration::from_millis(3500)).await;
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        1,
        "the entry joined a run that is being stopped"
    );
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM review").await, 1);
    assert_eq!(awaited_consumer(&fixture).await.as_deref(), Some(&*first));
    let steps = count(&fixture, "SELECT COUNT(*) FROM task_step").await;
    // The run ends: the next recheck asks again.
    sqlx::query("UPDATE check_run SET state='cancelled', finished_at=updated_at, lease_owner=NULL, lease_until=NULL")
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let second = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let awaited = awaited_consumer(&fixture).await;
            if awaited.as_deref().is_some_and(|id| id != first) {
                break awaited.unwrap();
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the entry asks again once the run has ended");
    stop.send(true).unwrap();
    worker.await.unwrap();
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        2
    );
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 2);
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM check_run r JOIN check_consumer c ON c.run_id=r.id WHERE c.id<>'' AND r.state='queued' AND c.cancelled_at IS NULL"
        )
        .await,
        1,
        "{second}"
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT CAST(json_extract(effects_json,'$.ci_rerequests') AS INTEGER) FROM task_hook_checkpoint WHERE json_extract(effects_json,'$.ci_rerequests') IS NOT NULL"
        )
        .await,
        1,
        "one refusal used one re-request"
    );
    // Waiting wrote no new step: the same step was suspended again.
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM task_step").await,
        steps
    );
    assert_eq!(
        db::budget::spent(fixture.db.pool(), &fixture.task.id, "review")
            .await
            .unwrap(),
        0
    );
}

// A check that keeps being withdrawn does not make the entry ask forever:
// after three re-requests the hook fails as an unanswered check always did.
#[tokio::test]
async fn an_entry_whose_check_keeps_being_withdrawn_stops_asking_after_three_times() {
    let fixture = approval_ci_fixture(json!(["echo entry-commands"])).await;
    suspend_on_entry_check(&fixture).await;
    let mut awaited = awaited_consumer(&fixture).await.unwrap();
    for _ in 0..3 {
        fixture
            .db
            .cancel_project_check_consumers(&fixture.task.project_id)
            .await
            .unwrap();
        awaited = wake_and_resuspend(&fixture, &awaited).await;
    }
    fixture
        .db
        .cancel_project_check_consumers(&fixture.task.project_id)
        .await
        .unwrap();
    fixture
        .db
        .wake_suspended_hooks(&fixture.task.id, &awaited)
        .await
        .unwrap();
    drain(fixture.engine.clone(), &fixture.task.id).await;
    let reviews = review_summaries(&fixture).await;
    assert_eq!(reviews.len(), 4, "{reviews:?}");
    assert!(reviews
        .iter()
        .all(|review| review.0 == db::ReviewStatus::Cancelled));
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        4
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_step WHERE kind='hooks' AND status='suspended'"
        )
        .await,
        0,
        "the entry does not wait for a check nobody will answer"
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_hook_checkpoint WHERE result_json LIKE '%review check was lost%'"
        )
        .await,
        1,
        "the failure is on the Task's history"
    );
}

/// The Task's worktree directory is gone and the disk is under its floor:
/// whoever needs the worktree would have to make it again and may not.
/// Returns the injected free-bytes reading.
async fn worktree_gone_under_the_disk_floor(
    fixture: &FailedCiFixture,
) -> Arc<std::sync::atomic::AtomicU64> {
    let path: String = sqlx::query_scalar("SELECT worktree_path FROM workspace WHERE task_id=?")
        .bind(&fixture.task.id)
        .fetch_one(fixture.db.pool())
        .await
        .unwrap();
    std::fs::remove_dir_all(&path).expect("the worktree directory is removed");
    let free = Arc::new(std::sync::atomic::AtomicU64::new(10));
    let reading = Arc::clone(&free);
    fixture.db.disk_admission.configure(
        api_types::DiskFloor::of_bytes(100, 0),
        Arc::new(move || {
            Some(api_types::MachineDiskFacts {
                free_bytes: reading.load(std::sync::atomic::Ordering::SeqCst),
                total_bytes: 1_000,
                free_inodes: None,
                total_inodes: None,
                measured_at: now_rfc3339(),
                gc_state: Some("unclaimed".to_owned()),
                compiler_cache_bytes: None,
            })
        }),
    );
    free
}

/// Run the step worker (no check worker) until the hooks step is suspended
/// on the disk wait.
async fn suspend_on_disk(fixture: &FailedCiFixture) {
    let (stop, signal) = tokio::sync::watch::channel(false);
    let worker = Arc::new(TaskStepWorker::new(fixture.engine.clone())).start(signal);
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while awaited_consumer(fixture).await.as_deref() != Some("disk") {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the hooks step suspends on the disk wait");
    stop.send(true).unwrap();
    worker.await.unwrap();
}

/// The disk recheck passes: the running step worker wakes the step at its
/// deadline (nobody delivers anything to a disk wait), the hook runs again
/// and the entry goes on to ask for its check.
async fn resume_after_the_disk_recheck(fixture: &FailedCiFixture) {
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let (stop, signal) = tokio::sync::watch::channel(false);
    let worker = Arc::new(TaskStepWorker::new(fixture.engine.clone())).start(signal);
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            fixture.db.domain_event_notify().notify_waiters();
            if awaited_consumer(fixture)
                .await
                .is_some_and(|awaited| awaited != "disk")
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the step leaves the disk wait and asks for its check");
    stop.send(true).unwrap();
    worker.await.unwrap();
}

async fn waits_for_disk(fixture: &FailedCiFixture) -> bool {
    let task = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(
        task.error_annotation.is_none(),
        "{:?}",
        task.error_annotation
    );
    assert!(task.blocked_json.is_none() && task.entry_barrier_json.is_none());
    crate::deferred_dispatch::dispatch_disposition_for_test(&task).is_some_and(|wait| {
        wait.capability == "machine_capacity"
            && wait.capacity_scope.as_deref() == Some(api_types::CAPACITY_SCOPE_DISK)
    })
}

// The review entry's check needs the Task worktree, which is gone, and the
// disk is under its floor. That is a wait, never a failed review: the hooks
// step suspends on the disk wait, the Task shows the disk capacity wait, no
// review attempt and no check exist. When the disk recovers the step's
// recheck runs the hook again and the entry settles with its verdict.
#[tokio::test]
async fn a_review_entry_whose_worktree_cannot_be_recreated_under_the_disk_floor_waits_for_disk() {
    let fixture = approval_ci_fixture(json!(["echo entry-commands"])).await;
    let free = worktree_gone_under_the_disk_floor(&fixture).await;
    let entered = enter_review(&fixture, "CI entry").await;
    assert_eq!(entered.pending_steps, 1);
    let _ = fixture.engine.check_worker_or_embedded();
    suspend_on_disk(&fixture).await;
    assert!(
        waits_for_disk(&fixture).await,
        "the Task states the disk wait"
    );
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM review").await, 0);
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        0
    );
    let attempts: i64 =
        sqlx::query_scalar("SELECT attempts FROM task_step WHERE kind='hooks' AND task_id=?")
            .bind(&fixture.task.id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    assert_eq!(attempts, 0, "the wait is not charged to the step");

    free.store(900, std::sync::atomic::Ordering::SeqCst);
    resume_after_the_disk_recheck(&fixture).await;
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    let reviews = review_summaries(&fixture).await;
    assert_eq!(reviews.len(), 1, "{reviews:?} {:?}", task.error_annotation);
    assert_eq!(reviews[0].0, db::ReviewStatus::AwaitingHuman, "{reviews:?}");
    assert!(reviews[0].1.contains("entry-commands"));
    assert_eq!(task.status, "review");
    assert!(
        task.error_annotation.is_none(),
        "{:?}",
        task.error_annotation
    );
    assert!(
        crate::deferred_dispatch::dispatch_disposition_for_test(&task).is_none(),
        "the disk wait is over"
    );
}

// A blocking before-work hook needs the worktree too. It used to record the
// disk wait and then report the hook as failed, which parked the Task as a
// failed transition hook for its owner. Now the hook waits: nothing ran,
// nothing failed, and the script runs once the disk recovers.
#[tokio::test]
async fn a_before_work_hook_whose_worktree_cannot_be_recreated_waits_for_disk_and_does_not_fail() {
    let fixture = approval_ci_fixture(json!(["echo entry-commands"])).await;
    let marker = TempDir::new().unwrap();
    let ran = marker.path().join("ran");
    let scripts = json!([{"type":"script","command":format!("echo run >> '{}'", ran.display()),"timeout_seconds":30,"blocking":true}]);
    sqlx::query("UPDATE project SET settings=? WHERE id=?")
        .bind(json!({"lifecycle_hooks":{"before_work":scripts}}).to_string())
        .bind(&fixture.task.project_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let free = worktree_gone_under_the_disk_floor(&fixture).await;
    enter_review(&fixture, "entry with a before-work hook").await;
    let _ = fixture.engine.check_worker_or_embedded();
    suspend_on_disk(&fixture).await;
    assert!(!ran.exists(), "the script ran without its worktree");
    assert!(
        waits_for_disk(&fixture).await,
        "the Task states the disk wait"
    );
    // The built-in no-change guard records its decision at its reserved
    // index before any configured hook; that is not a hook result.
    assert_eq!(
        count(
            &fixture,
            &format!(
                "SELECT COUNT(*) FROM task_hook_checkpoint WHERE result_json IS NOT NULL AND hook_index<>{}",
                db::budget::REPAIR_NO_CHANGE_CHECKPOINT
            )
        )
        .await,
        0,
        "no hook result (and no failure) was recorded"
    );

    free.store(900, std::sync::atomic::Ordering::SeqCst);
    resume_after_the_disk_recheck(&fixture).await;
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    assert_eq!(std::fs::read_to_string(&ran).unwrap(), "run\n");
    assert_eq!(task.status, "review");
    assert!(
        task.error_annotation.is_none(),
        "{:?}",
        task.error_annotation
    );
    let reviews = review_summaries(&fixture).await;
    assert_eq!(reviews.len(), 1, "{reviews:?}");
    assert_eq!(reviews[0].0, db::ReviewStatus::AwaitingHuman, "{reviews:?}");
}

// A held Task is its owner's decision to stop. A system re-entry of its
// review (recovery settling a retired inline re-run, a queued `rerun_review`
// command) does nothing while the hold stands: no attempt is superseded, no
// status entry, hooks step, check or review attempt is created. The release
// runs the review entry by itself.
#[tokio::test]
async fn a_system_review_re_entry_on_a_held_task_does_nothing_until_release() {
    let fixture = approval_ci_fixture(json!(["echo entry-commands"])).await;
    // Held while its entry check is queued: the wait is abandoned.
    suspend_on_entry_check(&fixture).await;
    let entered = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    let held = fixture
        .engine
        .perform_task_action(
            &fixture.task.id,
            api_types::TaskAction::Hold { reason: None },
            entered.version,
        )
        .await
        .expect("the Task is held")
        .task;
    assert_eq!(held.status, "review");
    drain(fixture.engine.clone(), &fixture.task.id).await;
    let snapshot = || async {
        let epoch: i64 = sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=?")
            .bind(&fixture.task.id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
        (
            epoch,
            review_summaries(&fixture).await,
            count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
            count(
                &fixture,
                "SELECT COUNT(*) FROM task_step WHERE kind='hooks'",
            )
            .await,
            count(&fixture, "SELECT COUNT(*) FROM transition_log").await,
        )
    };
    let before = snapshot().await;
    let id = uuid::Uuid::parse_str(&fixture.task.id).unwrap();
    let recovery = api_types::Actor::system(api_types::SystemComponent::CrashRecovery);
    let refused = fixture.engine.rerun_review(id, recovery).await;
    assert!(
        matches!(&refused, Err(ServiceError::Conflict(_))),
        "a system re-entry ran on a held Task: {:?}",
        refused.map(|task| task.status)
    );
    drain(fixture.engine.clone(), &fixture.task.id).await;
    assert_eq!(
        snapshot().await,
        before,
        "the refused re-entry changed the Task"
    );
    // The owner is not offered the re-run either while the hold stands.
    assert!(matches!(
        fixture
            .engine
            .perform_task_action(
                &fixture.task.id,
                api_types::TaskAction::retry(),
                held.version
            )
            .await,
        Err(ServiceError::TaskActionUnavailable { .. })
    ));

    let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    fixture
        .engine
        .perform_task_action(
            &fixture.task.id,
            api_types::TaskAction::Release { reason: None },
            current.version,
        )
        .await
        .expect("the Task is released");
    // Nobody asks again: the release itself runs the entry the hold stopped
    // (the hold reached the Task while its entry check was pending and
    // superseded the hooks step). Without that the released Task sat in
    // `review` with no check and no attempt, its approval on offer.
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    assert_eq!(task.status, "review");
    let after = snapshot().await;
    assert!(after.0 > before.0, "a new review entry ran after release");
    assert_eq!(after.2, before.2 + 1, "the entry asked for its check once");
    assert!(after
        .1
        .iter()
        .any(|review| review.0 == db::ReviewStatus::AwaitingHuman
            && review.1.contains("entry-commands")));
    assert_eq!(
        after
            .1
            .iter()
            .filter(|review| matches!(
                review.0,
                db::ReviewStatus::Running | db::ReviewStatus::AwaitingHuman
            ))
            .count(),
        1,
        "one open attempt: {:?}",
        after.1
    );
    // A second release has nothing to resume.
    assert!(!fixture
        .db
        .entry_hooks_preempted(&fixture.task.id)
        .await
        .unwrap());
}

// Red CI with no remediation target. A review gate without a
// `reject_target` is not that case: the target falls back to `in_progress`
// (`review_failure_target`), which is why nulling it still sent the Task
// back to work. The case exists only when the state the failed review would
// go to is not in the workflow. Then the transition cannot be made, and the
// failed entry parks typed in `review` with its owner's actions.
#[tokio::test]
async fn red_review_ci_with_no_remediation_state_parks_typed_with_an_offered_action() {
    let mut fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    // No `reject_target`: the fallback is `in_progress`, as documented.
    fixture
        .workflow
        .states
        .iter_mut()
        .find(|state| state.name == "review")
        .unwrap()
        .gate_config
        .as_mut()
        .unwrap()
        .reject_target = Some("nowhere".to_owned());
    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(serde_json::to_string(&fixture.workflow).unwrap())
        .bind(&fixture.task.project_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let entered = enter_review(&fixture, "CI entry").await;
    let settled = drain_result(fixture.engine.clone(), entered).await.task;
    assert_eq!(settled.status, "review", "{:?}", settled.error_annotation);
    let reviews = review_summaries(&fixture).await;
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].0, db::ReviewStatus::Failed, "{reviews:?}");
    let blocked: serde_json::Value = serde_json::from_str(
        settled
            .blocked_json
            .as_deref()
            .expect("the Task is blocked"),
    )
    .unwrap();
    assert_eq!(
        blocked["reason"],
        "review failed and the workflow has no remediation state"
    );
    assert_eq!(blocked["kind"], "review_gate_failed");
    assert!(settled
        .error_annotation
        .as_deref()
        .is_some_and(
            |annotation| annotation.contains("nowhere") && !annotation.contains("cascade_failed")
        ));
    match fixture
        .engine
        .perform_task_action(&settled.id, api_types::TaskAction::Start, settled.version)
        .await
    {
        Err(ServiceError::TaskActionUnavailable {
            available_actions, ..
        }) => {
            let offered: Vec<String> = available_actions
                .iter()
                .map(|offer| offer.label.clone())
                .collect();
            assert!(
                offered.iter().any(|label| label == "Cancel Task") && offered.len() > 1,
                "a blocked review entry offers its owner a way on: {offered:?}"
            );
        }
        other => panic!("start on a blocked review entry: {:?}", other.map(|_| ())),
    }
}

// The Project environment changes while the entry check is pending (with
// the pause, the rename or the limit change that moves the Project version).
// The run that ran is the run that was asked for: its verdict is applied.
// Its result answers the old environment only: the next entry runs again.
#[tokio::test]
async fn a_project_environment_change_while_the_entry_check_is_pending_applies_the_verdict_once() {
    let fixture = approval_ci_fixture(json!(["echo first-commands"])).await;
    suspend_on_entry_check(&fixture).await;
    sqlx::query("UPDATE project SET settings=json_set(CASE WHEN json_valid(settings) THEN settings ELSE '{}' END,'$.environment',json('{\"env\":{\"REVIEW_ENTRY_FLAG\":\"changed\"}}')), version=version+1 WHERE id=?")
        .bind(&fixture.task.project_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    let reviews = review_summaries(&fixture).await;
    assert_eq!(reviews.len(), 1);
    assert_eq!(
        reviews[0].0,
        db::ReviewStatus::AwaitingHuman,
        "{reviews:?} {:?}",
        task.error_annotation
    );
    assert!(reviews[0].1.contains("first-commands"));
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 1);
    re_enter_review(&fixture, "after the environment change").await;
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_run").await,
        2,
        "the old environment's result was reused for the new one"
    );
}

// While its entry waits for disk the Task offers `cancel` and no `hold`. A
// hold used to be on offer (the disk wait is a capacity wait like a dispatch
// wait): it superseded the hooks step, and its release left the Task in
// `review` with its entry never run: no check, no attempt, and the gate's
// approval on offer. Cancel works: the step is superseded, nothing failed.
#[tokio::test]
async fn a_task_whose_entry_waits_for_disk_offers_cancel_and_no_hold() {
    let fixture = approval_ci_fixture(json!(["echo entry-commands"])).await;
    let _free = worktree_gone_under_the_disk_floor(&fixture).await;
    enter_review(&fixture, "CI entry").await;
    let _ = fixture.engine.check_worker_or_embedded();
    suspend_on_disk(&fixture).await;
    assert!(waits_for_disk(&fixture).await);
    let offers = fixture
        .engine
        .task_action_offers(
            &fixture.task.id,
            &api_types::Actor::user(api_types::UserActionSource::Api),
        )
        .await
        .unwrap();
    let verbs: Vec<&str> = offers
        .available_actions
        .iter()
        .map(|offer| offer.action.verb())
        .collect();
    assert_eq!(verbs, ["cancel"]);
    // Two more rechecks: the wait is one step that is suspended again, not
    // a new step (or a review attempt) per recheck.
    let rows = |fixture: &FailedCiFixture| {
        let pool = fixture.db.pool().clone();
        async move {
            sqlx::query_as::<_, (i64, i64, i64)>(
                "SELECT (SELECT COUNT(*) FROM task_step),(SELECT COUNT(*) FROM review),(SELECT COALESCE(SUM(attempts),0) FROM task_step WHERE kind='hooks')",
            )
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    let before = rows(&fixture).await;
    let (stop, signal) = tokio::sync::watch::channel(false);
    let worker = Arc::new(TaskStepWorker::new(fixture.engine.clone())).start(signal);
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    stop.send(true).unwrap();
    worker.await.unwrap();
    assert_eq!(rows(&fixture).await, before, "a recheck added rows");
    assert_eq!(awaited_consumer(&fixture).await.as_deref(), Some("disk"));

    let cancelled = fixture
        .engine
        .perform_task_action(
            &fixture.task.id,
            api_types::TaskAction::Cancel { reason: None },
            TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
                .await
                .unwrap()
                .unwrap()
                .version,
        )
        .await
        .expect("cancel is accepted while the entry waits for disk");
    assert_eq!(cancelled.task.status, "cancelled");
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    assert_eq!(task.status, "cancelled");
    assert!(
        task.error_annotation.is_none(),
        "{:?}",
        task.error_annotation
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_step WHERE status IN ('pending','claimed','suspended')"
        )
        .await,
        0
    );
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM review").await, 0);
}

// A hold command reaches the Task while its entry waits for disk (the offer
// list does not have it; the command is accepted all the same, because a
// hold may stop hooks in flight). It supersedes the hooks step. The release
// runs the entry again by itself: the Task does not sit in `review` with its
// checks unrun.
#[tokio::test]
async fn a_hold_while_the_entry_waits_for_disk_is_resumed_by_its_release() {
    let fixture = approval_ci_fixture(json!(["echo entry-commands"])).await;
    let free = worktree_gone_under_the_disk_floor(&fixture).await;
    enter_review(&fixture, "CI entry").await;
    let _ = fixture.engine.check_worker_or_embedded();
    suspend_on_disk(&fixture).await;
    let version = |fixture: &FailedCiFixture| {
        let (db, id) = (fixture.db.clone(), fixture.task.id.clone());
        async move {
            TaskRepo::get_by_id(&*db, &id, false)
                .await
                .unwrap()
                .unwrap()
                .version
        }
    };
    fixture
        .engine
        .perform_task_action(
            &fixture.task.id,
            api_types::TaskAction::Hold { reason: None },
            version(&fixture).await,
        )
        .await
        .expect("the hold command is accepted");
    drain(fixture.engine.clone(), &fixture.task.id).await;
    assert!(fixture
        .db
        .entry_hooks_preempted(&fixture.task.id)
        .await
        .unwrap());
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM review").await, 0);

    free.store(900, std::sync::atomic::Ordering::SeqCst);
    fixture
        .engine
        .perform_task_action(
            &fixture.task.id,
            api_types::TaskAction::Release { reason: None },
            version(&fixture).await,
        )
        .await
        .expect("the Task is released");
    // The release ran the entry again: a new hooks step of a new status
    // entry, which reads the disk again at its recheck and asks for its
    // check.
    assert!(!fixture
        .db
        .entry_hooks_preempted(&fixture.task.id)
        .await
        .unwrap());
    resume_after_the_disk_recheck(&fixture).await;
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    let reviews = review_summaries(&fixture).await;
    assert_eq!(reviews.len(), 1, "{reviews:?} {:?}", task.error_annotation);
    assert_eq!(reviews[0].0, db::ReviewStatus::AwaitingHuman, "{reviews:?}");
    assert!(reviews[0].1.contains("entry-commands"));
    assert_eq!(task.status, "review");
    assert!(
        task.error_annotation.is_none(),
        "{:?}",
        task.error_annotation
    );
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        1
    );
}

// The Project workflow is edited while the entry check is pending. An edit
// that leaves the entered state as it was does not concern the entry: the
// check that ran is applied, nothing is asked again and nobody is needed.
// An edit of the entered state itself (here: its hooks) still ends the
// step: the hook list the step froze is no longer that state's, the verdict
// is applied to nothing and the Task parks for its owner.
#[tokio::test]
async fn a_workflow_edit_that_leaves_the_entered_state_alone_keeps_the_entry_check() {
    let store = |fixture: &FailedCiFixture, workflow: &api_types::WorkflowDefinition| {
        let (pool, id) = (fixture.db.pool().clone(), fixture.task.project_id.clone());
        let workflow = serde_json::to_string(workflow).unwrap();
        async move {
            sqlx::query(
                "UPDATE project SET workflow_definition = ?, version = version + 1 WHERE id = ?",
            )
            .bind(workflow)
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
        }
    };

    let fixture = approval_ci_fixture(json!(["echo first-commands"])).await;
    suspend_on_entry_check(&fixture).await;
    let mut edited = fixture.workflow.clone();
    let other = edited
        .states
        .iter_mut()
        .find(|state| state.name != "review" && state.name != "in_progress")
        .expect("a state the entry does not touch");
    other.config = json!({ "edited_while_the_check_ran": true });
    store(&fixture, &edited).await;
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    let reviews = review_summaries(&fixture).await;
    assert_eq!(reviews.len(), 1, "{reviews:?}");
    assert_eq!(
        reviews[0].0,
        db::ReviewStatus::AwaitingHuman,
        "{reviews:?} {:?}",
        task.error_annotation
    );
    assert!(reviews[0].1.contains("first-commands"));
    assert!(
        task.error_annotation.is_none(),
        "{:?}",
        task.error_annotation
    );
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        1
    );
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 1);

    let fixture = approval_ci_fixture(json!(["echo first-commands"])).await;
    suspend_on_entry_check(&fixture).await;
    let mut edited = fixture.workflow.clone();
    let review = edited
        .states
        .iter_mut()
        .find(|state| state.name == "review")
        .unwrap();
    let hook = review
        .hooks
        .on_enter
        .first()
        .cloned()
        .expect("the review state has entry hooks");
    review.hooks.on_enter.push(hook);
    store(&fixture, &edited).await;
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    let reviews = review_summaries(&fixture).await;
    assert!(
        reviews
            .iter()
            .all(|review| review.0 == db::ReviewStatus::Cancelled),
        "a verdict was applied under an edited entry: {reviews:?}"
    );
    assert_eq!(task.status, "review");
    assert!(task
        .error_annotation
        .as_deref()
        .is_some_and(|annotation| annotation.contains("cascade_failed")));
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        1,
        "an entry whose state was edited asked again"
    );
}
