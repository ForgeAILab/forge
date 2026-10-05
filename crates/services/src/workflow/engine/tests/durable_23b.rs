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
        .transition(
            "crash-cas",
            "step_0",
            1,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "start",
            false,
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
        .transition(
            "hook-audience",
            "step_0",
            1,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "start",
            false,
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
        .transition(
            "atomic-hook",
            "step_0",
            1,
            &cascade_chain_workflow(1),
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "start",
            false
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
        .transition(
            "hook-fence",
            "step_0",
            1,
            &cascade_chain_workflow(1),
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "start",
            false,
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
    let log_dir = std::env::temp_dir()
        .join("forge")
        .join("logs")
        .join(&fixture.task.id)
        .join("hooks");
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

#[tokio::test]
async fn cancel_preempts_long_ci_step_and_cancels_the_task() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    let control = TempDir::new().unwrap();
    let started = control.path().join("started");
    let completed = control.path().join("completed");
    let quote = |p: &std::path::Path| format!("'{}'", p.to_string_lossy().replace('\'', "'\"'\"'"));
    sqlx::query("UPDATE task SET task_state_config=? WHERE id=?").bind(json!({"review":{"ci_steps":[format!("touch {}; sleep 120; touch {}",quote(&started),quote(&completed))]}}).to_string()).bind(&fixture.task.id).execute(fixture.db.pool()).await.unwrap();
    let result = fixture
        .engine
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
    let (stop, signal) = tokio::sync::watch::channel(false);
    let worker = Arc::new(TaskStepWorker::new(fixture.engine.clone())).start(signal);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !started.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    let result = fixture
        .engine
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
    fixture
        .engine
        .task_service
        .drain(&current.id)
        .await
        .unwrap();
    assert!(fixture
        .db
        .task_steps(&current.id)
        .await
        .unwrap()
        .iter()
        .any(|step| step.kind == "hooks"
            && step.expected_status == "review"
            && step.status == "superseded"));
    stop.send(true).unwrap();
    worker.await.unwrap();
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
        .transition(
            task_id,
            default_states::IN_PROGRESS,
            current.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work",
            false,
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
