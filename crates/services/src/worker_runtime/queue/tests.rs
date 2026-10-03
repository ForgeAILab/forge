use super::*;
use api_types::{
    StateDefinition, StateHooks, StateKind, WorkflowDefinition, WorkflowTrigger,
    WorkflowTriggerDefinition,
};
use db::{EnqueueTaskStep, TaskStepRepo};

async fn fixture() -> (Arc<db::SqliteDb>, TaskStepWorker) {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(db::SqliteDb::new(pool));
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES ('p','p',?,?)")
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES ('t','p','task','todo',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    let service = crate::TaskService::new(db.clone(), Arc::new(events::EventBus::new(32)));
    let worker = TaskStepWorker::new(service.workflow_engine());
    (db, worker)
}
fn workflow() -> WorkflowDefinition {
    let mut from = StateDefinition {
        name: "todo".into(),
        column: "todo".into(),
        display_name: "Todo".into(),
        kind: StateKind::Initial,
        role: None,
        hooks: StateHooks::default(),
        triggers: Default::default(),
        gate_config: None,
        config: serde_json::json!({}),
        dispatch: None,
        cleanup: None,
        canonical_phase: None,
    };
    from.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "done".into(),
            dispatch: None,
        },
    );
    let mut done = from.clone();
    done.name = "done".into();
    done.kind = StateKind::Terminal;
    done.triggers.clear();
    WorkflowDefinition {
        roles: vec![],
        states: vec![from, done],
        configuration: vec![],
        cancellation_state: None,
    }
}
fn step(to: &str) -> EnqueueTaskStep {
    EnqueueTaskStep {
        id: db::new_uuid_v4(),
        task_id: "t".into(),
        payload_json: serde_json::to_string(&CascadePayload {
            to: to.into(),
            reason: "automatic".into(),
            rejection: false,
            skip_before_exit: false,
            workflow: workflow(),
            authority: None,
            evidence: None,
        })
        .unwrap(),
        causation_step_id: None,
        causation_key: "commit".into(),
        chain_id: "chain".into(),
        chain_position: 1,
        expected_status: "todo".into(),
        expected_version: 1,
        available_at: db::now_rfc3339(),
    }
}
#[tokio::test]
async fn deterministic_refusal_fails_and_records_recovery_annotation() {
    let (db, worker) = fixture().await;
    db.enqueue_step(&step("undefined")).await.unwrap();
    let task = worker.drain("t").await.unwrap();
    assert_eq!(task.status, "todo");
    let annotation: serde_json::Value =
        serde_json::from_str(task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["type"], "dispatch_failed");
    assert!(annotation["message"]
        .as_str()
        .unwrap()
        .contains("undefined"));
    let steps = db.task_steps("t").await.unwrap();
    assert_eq!(steps[0].status, "failed");
    assert_eq!(steps[0].attempts, 1);
    assert!(steps[0].completed_at.is_some());
    assert_eq!(db.pending_steps("t").await.unwrap(), 0);
}
#[tokio::test]
async fn reclaimed_step_drains_after_worker_restart_and_completes_atomically() {
    let (db, _worker) = fixture().await;
    let input = step("done");
    db.enqueue_step(&input).await.unwrap();
    db.claim_step("dead-process", Some("t"), "2000-01-01T00:00:00+00:00")
        .await
        .unwrap()
        .unwrap();
    let service = crate::TaskService::new(db.clone(), Arc::new(events::EventBus::new(32)));
    let restarted = TaskStepWorker::new(service.workflow_engine());
    assert_eq!(restarted.drain("t").await.unwrap().status, "done");
    let completed = &db.task_steps("t").await.unwrap()[0];
    assert_eq!(completed.status, "done");
    assert_eq!(completed.attempts, 2);
    assert!(completed.claimed_by.is_none());
    assert_eq!(
        db::TransitionLogRepo::list_by_task(&*db, "t")
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn drain_waits_for_committed_steps_inline_hook_lease_to_release() {
    let (db, worker) = fixture().await;
    db.enqueue_step(&step("done")).await.unwrap();
    let claimed = db
        .claim_step("owner", Some("t"), &lease_deadline())
        .await
        .unwrap()
        .unwrap();
    let payload = serde_json::from_str(&claimed.payload_json).unwrap();
    worker
        .engine
        .transition_step(&claimed, &payload)
        .await
        .unwrap();
    assert_eq!(db.pending_steps("t").await.unwrap(), 0);
    // Keep SQLx acquisition polling on its own task while the test waits;
    // cancelling it could recycle the sole in-memory database connection.
    let mut drain = tokio::spawn(async move { worker.drain("t").await });
    assert!(tokio::time::timeout(Duration::from_millis(100), &mut drain)
        .await
        .is_err());
    db.release_step(&claimed.id, "owner").await.unwrap();
    assert_eq!(drain.await.unwrap().unwrap().status, "done");
}
#[test]
fn retry_policy_distinguishes_availability_and_deterministic_refusals() {
    assert!(retryable(&ServiceError::RateLimited {
        retry_after_seconds: 1
    }));
    assert!(retryable(&ServiceError::Db(db::DbError::VersionConflict)));
    assert!(!retryable(&ServiceError::invalid_operation(
        "undefined state"
    )));
    assert!(!retryable(&ServiceError::GuardRejection {
        guard: "check".into(),
        reason: "failed".into()
    }));
}

#[tokio::test]
async fn failed_done_write_rolls_back_engine_status_cas_and_transition_log() {
    let (db, worker) = fixture().await;
    db.enqueue_step(&step("done")).await.unwrap();
    sqlx::query("CREATE TRIGGER reject_step_done BEFORE UPDATE OF status ON task_step WHEN NEW.status='done' BEGIN SELECT RAISE(ABORT,'injected completion failure'); END")
        .execute(db.pool()).await.unwrap();
    let task = worker.drain("t").await.unwrap();
    assert_eq!(task.status, "todo");
    assert!(db::TransitionLogRepo::list_by_task(&*db, "t")
        .await
        .unwrap()
        .is_empty());
    assert_eq!(db.task_steps("t").await.unwrap()[0].status, "failed");
    assert!(task
        .error_annotation
        .as_deref()
        .unwrap()
        .contains("injected completion failure"));
}

#[tokio::test]
async fn queued_step_resolves_current_project_authority_at_its_own_entry() {
    let (db, worker) = fixture().await;
    let mut input = step("done");
    let mut payload: CascadePayload = serde_json::from_str(&input.payload_json).unwrap();
    let definition = serde_json::to_string(&workflow()).unwrap();
    payload.authority = Some(WorkflowAuthority {
        project_version: 1,
        workflow_definition: definition.clone(),
        clear_review_passed_at_on_commit: false,
    });
    input.payload_json = serde_json::to_string(&payload).unwrap();
    db.enqueue_step(&input).await.unwrap();
    sqlx::query("UPDATE project SET workflow_definition=?,version=version+1 WHERE id='p'")
        .bind(definition)
        .execute(db.pool())
        .await
        .unwrap();
    let task = worker.drain("t").await.unwrap();
    assert_eq!(task.status, "done");
    assert!(task.error_annotation.is_none());
    assert_eq!(db.task_steps("t").await.unwrap()[0].status, "done");
}

#[tokio::test]
async fn supervised_worker_consumes_commit_kick_and_stops_on_shutdown() {
    let (db, worker) = fixture().await;
    let (shutdown, receiver) = watch::channel(false);
    let handle = Arc::new(worker).start(receiver);
    tokio::task::yield_now().await;
    db.enqueue_step(&step("done")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if db.task_steps("t").await.unwrap()[0].status == "done" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("background worker consumes committed cascade");
    assert_eq!(
        TaskRepo::get_by_id(&*db, "t", false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "done"
    );
    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("supervisor stops")
        .unwrap();
}

#[tokio::test]
async fn throwing_inline_hook_records_committed_failure_without_replaying_transition() {
    let (db, worker) = fixture().await;
    let mut input = step("done");
    let mut payload: CascadePayload = serde_json::from_str(&input.payload_json).unwrap();
    payload.workflow.states[1]
        .hooks
        .on_enter
        .push(api_types::HookSpec {
            action: "undefined_action".into(),
            params: serde_json::json!({}),
            applies_to: api_types::HookAudience::All,
            on_failure: api_types::FailurePolicy::Log,
        });
    input.payload_json = serde_json::to_string(&payload).unwrap();
    db.enqueue_step(&input).await.unwrap();
    let task = worker.drain("t").await.unwrap();
    assert_eq!(task.status, "done");
    assert!(task
        .error_annotation
        .as_deref()
        .unwrap()
        .contains("Transition committed; inline hook failed"));
    let failed = &db.task_steps("t").await.unwrap()[0];
    assert_eq!(failed.status, "failed");
    assert_eq!(failed.attempts, 1);
    assert!(failed
        .last_error
        .as_deref()
        .unwrap()
        .contains("undefined_action"));
    let logs = db::TransitionLogRepo::list_by_task(&*db, "t")
        .await
        .unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].id, input.id);
}

#[tokio::test]
async fn interrupted_committed_hook_does_not_annotate_later_task_entry() {
    let (db, worker) = fixture().await;
    db.enqueue_step(&step("done")).await.unwrap();
    let claimed = db
        .claim_step("owner", Some("t"), &lease_deadline())
        .await
        .unwrap()
        .unwrap();
    worker.execute_inner(&claimed).await.unwrap();
    let now = db::now_rfc3339();
    sqlx::query("UPDATE task SET status='todo',version=version+1 WHERE id='t'")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,rejection,created_at) VALUES (?,'t','done','todo','user:api','restart',0,?)")
        .bind(db::new_uuid_v4()).bind(now).execute(db.pool()).await.unwrap();
    worker
        .fail_committed_hook_phase(&claimed, "predecessor was interrupted")
        .await
        .unwrap();
    let task = TaskRepo::get_by_id(&*db, "t", false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.status, "todo");
    assert!(task.error_annotation.is_none());
    let predecessor = &db.task_steps("t").await.unwrap()[0];
    assert_eq!(predecessor.status, "done");
    assert!(predecessor
        .last_error
        .as_deref()
        .unwrap()
        .contains("predecessor was interrupted"));
}
