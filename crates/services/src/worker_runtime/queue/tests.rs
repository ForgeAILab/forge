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
    sqlx::query(
        "INSERT INTO task_step_workflow(id,definition_json,last_used_at) VALUES ('fixture',?,?)",
    )
    .bind(serde_json::to_string(&workflow()).unwrap())
    .bind(db::now_rfc3339())
    .execute(db.pool())
    .await
    .unwrap();
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
            workflow_ref: WorkflowReference::Snapshot("fixture".into()),
            clear_review_passed_at_on_commit: false,
            admission_agent_id: None,
            evidence: None,
        })
        .unwrap(),
        causation_step_id: None,
        causation_key: "commit".into(),
        chain_id: "chain".into(),
        chain_position: 1,
        expected_status: "todo".into(),
        expected_version: 1,
        expected_epoch: None,
        lane: "fast".into(),
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
    assert_eq!(annotation["type"], "cascade_failed");
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
        .transition_step(&claimed, &payload, &workflow(), None)
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
    payload.workflow_ref = WorkflowReference::Project;
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
    let mut definition = workflow();
    definition.states[1]
        .hooks
        .on_enter
        .push(api_types::HookSpec {
            action: "undefined_action".into(),
            params: serde_json::json!({}),
            applies_to: api_types::HookAudience::All,
            on_failure: api_types::FailurePolicy::Log,
        });
    payload.workflow_ref = WorkflowReference::Snapshot(
        db.store_step_workflow(&serde_json::to_string(&definition).unwrap())
            .await
            .unwrap(),
    );
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

/// R5: a hook error after a sleep that expired the wall-clock lease is still
/// recorded while the local owner is alive.
#[tokio::test]
async fn committed_hook_failure_after_sleep_is_recorded_for_live_owner() {
    let (db, worker) = fixture().await;
    db.enqueue_step(&step("done")).await.unwrap();
    let claimed = db
        .claim_step("owner", Some("t"), &lease_deadline())
        .await
        .unwrap()
        .unwrap();
    let _activity = db.hold_task_step(&claimed);
    worker.execute_inner(&claimed).await.unwrap();
    sqlx::query("UPDATE task_step SET lease_until='2000-01-01T00:00:00+00:00' WHERE id=?")
        .bind(&claimed.id)
        .execute(db.pool())
        .await
        .unwrap();
    worker
        .fail_committed_hook_phase(&claimed, "hook failed after wake")
        .await
        .unwrap();
    let row = &db.task_steps("t").await.unwrap()[0];
    assert_eq!(row.status, "failed");
    let task = TaskRepo::get_by_id(&*db, "t", false)
        .await
        .unwrap()
        .unwrap();
    let annotation: serde_json::Value =
        serde_json::from_str(task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["type"], "cascade_failed");
    assert_eq!(annotation["task_step_id"], claimed.id.as_str());
}

#[tokio::test]
async fn crashed_producer_reservation_expires_and_version_edit_does_not_drop_cascade() {
    let (db, worker) = fixture().await;
    let mut input = step("done");
    input.available_at = lease_deadline();
    db.enqueue_step(&input).await.unwrap();
    let producer = ProducerReservation::hold(db.clone(), Some(&input.id));
    drop(producer);
    sqlx::query("UPDATE task SET title='edited',version=version+1 WHERE id='t'")
        .execute(db.pool())
        .await
        .unwrap();
    assert!(db
        .claim_step("early", Some("t"), &lease_deadline())
        .await
        .unwrap()
        .is_none());
    sqlx::query("UPDATE task_step SET available_at='2000-01-01T00:00:00Z'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(worker.drain("t").await.unwrap().status, "done");
    assert_eq!(db.task_steps("t").await.unwrap()[0].status, "done");
}

#[tokio::test]
async fn background_worker_retries_version_conflict_with_backoff() {
    let (db, worker) = fixture().await;
    sqlx::query("CREATE TRIGGER temporarily_refuse BEFORE UPDATE OF status ON task WHEN NEW.status='done' BEGIN SELECT RAISE(IGNORE); END").execute(db.pool()).await.unwrap();
    let (stop, rx) = watch::channel(false);
    let handle = Arc::new(worker).start(rx);
    db.enqueue_step(&step("done")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let row = &db.task_steps("t").await.unwrap()[0];
            if row.status == "pending" && row.attempts == 1 {
                assert!(row.last_error.is_some());
                assert!(row.available_at > db::now_rfc3339());
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    sqlx::query("DROP TRIGGER temporarily_refuse")
        .execute(db.pool())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(db.task_steps("t").await.unwrap()[0].attempts, 1);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if db.task_steps("t").await.unwrap()[0].status == "done" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(db.task_steps("t").await.unwrap()[0].attempts, 2);
    stop.send(true).unwrap();
    handle.await.unwrap();
}

#[tokio::test]
async fn changed_gate_approval_supersedes_with_durable_diagnostic() {
    let (db, worker) = fixture().await;
    let mut wf = workflow();
    wf.states[0].kind = StateKind::Gate;
    let mut gate = crate::workflow::default_workflow::default_workflow()
        .states
        .into_iter()
        .find(|s| s.name == "review")
        .unwrap()
        .gate_config
        .unwrap();
    gate.requires_user_approval = Some(true);
    wf.states[0].gate_config = Some(gate);
    sqlx::query("UPDATE project SET workflow_definition=? WHERE id='p'")
        .bind(serde_json::to_string(&wf).unwrap())
        .execute(db.pool())
        .await
        .unwrap();
    let mut input = step("done");
    let mut payload: CascadePayload = serde_json::from_str(&input.payload_json).unwrap();
    payload.workflow_ref = WorkflowReference::Project;
    input.payload_json = serde_json::to_string(&payload).unwrap();
    db.enqueue_step(&input).await.unwrap();
    assert_eq!(worker.drain("t").await.unwrap().status, "todo");
    assert_eq!(db.task_steps("t").await.unwrap()[0].status, "superseded");
    let events:Vec<(String,String)>=sqlx::query_as("SELECT event_type,payload_json FROM domain_event WHERE event_type='transition.step_superseded'").fetch_all(db.pool()).await.unwrap();
    assert_eq!(events.len(), 1);
    assert!(events[0].1.contains("requires approval"));
}

#[tokio::test]
async fn queued_hop_propagates_producer_review_clear_flag() {
    let (db, worker) = fixture().await;
    sqlx::query("UPDATE project SET workflow_definition=? WHERE id='p'")
        .bind(serde_json::to_string(&workflow()).unwrap())
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task SET review_passed_at=? WHERE id='t'")
        .bind(db::now_rfc3339())
        .execute(db.pool())
        .await
        .unwrap();
    let mut input = step("done");
    let mut payload: CascadePayload = serde_json::from_str(&input.payload_json).unwrap();
    payload.workflow_ref = WorkflowReference::Project;
    payload.clear_review_passed_at_on_commit = true;
    input.payload_json = serde_json::to_string(&payload).unwrap();
    db.enqueue_step(&input).await.unwrap();
    let result = worker.drain("t").await.unwrap();
    assert_eq!(result.status, "done");
    assert!(result.review_passed_at.is_none());
}

#[tokio::test]
async fn loop_event_is_broadcast_once_with_durable_audit_envelope() {
    let (db, worker) = fixture().await;
    let mut events = worker.engine.event_bus.subscribe();
    let relay = crate::domain_event_broadcast::DomainEventBroadcastConsumer::new(
        db.clone(),
        worker.engine.event_bus.clone(),
        Some(db.domain_event_head().await.unwrap()),
    );
    let mut input = step("done");
    input.chain_position = CHAIN_LIMIT + 1;
    db.enqueue_step(&input).await.unwrap();
    let task = worker.drain("t").await.unwrap();
    assert!(task.blocked_json.is_some());
    relay.broadcast_once(100).await.unwrap();
    relay.broadcast_once(100).await.unwrap();
    let collected = std::iter::from_fn(|| events.try_recv().ok()).collect::<Vec<_>>();
    assert_eq!(
        collected
            .iter()
            .filter(|event| event.event_type == "transition.loop_detected")
            .count(),
        1
    );
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type='transition.loop_detected'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(count, 1);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(task.error_annotation.as_deref().unwrap())
            .unwrap()["type"],
        "workflow_loop"
    );
    let snapshot = worker
        .engine
        .task_service
        .task_action_snapshot("t", &Actor::user(api_types::UserActionSource::Test))
        .await
        .unwrap();
    let offers = crate::available_actions(&snapshot);
    assert!(offers.iter().any(|offer| matches!(
        offer.action,
        api_types::TaskAction::Restart { .. } | api_types::TaskAction::Approve { .. }
    )));
}
