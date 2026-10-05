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
        kind: "cascade".into(),
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
async fn drain_waits_for_status_step_lease_handover() {
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
async fn durable_hook_failure_keeps_the_committed_status_step() {
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
        .contains("Transition committed; hook failed"));
    let rows = db.task_steps("t").await.unwrap();
    assert_eq!(rows[0].status, "done");
    let failed = rows.last().unwrap();
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

async fn claim_committed_hooks(db: &Arc<db::SqliteDb>, worker: &TaskStepWorker) -> TaskStep {
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
    let status = db
        .claim_step("status-owner", Some("t"), &lease_deadline())
        .await
        .unwrap()
        .unwrap();
    worker.execute_inner(&status).await.unwrap();
    db.release_step(&status.id, "status-owner").await.unwrap();
    db.claim_step("hook-owner", Some("t"), &lease_deadline())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn interrupted_committed_hook_does_not_annotate_later_task_entry() {
    let (db, worker) = fixture().await;
    let claimed = claim_committed_hooks(&db, &worker).await;
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
    let rows = db.task_steps("t").await.unwrap();
    assert_eq!(rows[0].status, "done");
    let predecessor = rows.last().unwrap();
    assert_eq!(predecessor.status, "superseded");
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
    let claimed = claim_committed_hooks(&db, &worker).await;
    let _activity = db.hold_task_step(&claimed);
    sqlx::query("UPDATE task_step SET lease_until='2000-01-01T00:00:00+00:00' WHERE id=?")
        .bind(&claimed.id)
        .execute(db.pool())
        .await
        .unwrap();
    worker
        .fail_committed_hook_phase(&claimed, "hook failed after wake")
        .await
        .unwrap();
    let rows = db.task_steps("t").await.unwrap();
    let row = rows.last().unwrap();
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
async fn delayed_head_and_metadata_edit_preserve_the_cascade() {
    let (db, worker) = fixture().await;
    let mut input = step("done");
    input.available_at = lease_deadline();
    db.enqueue_step(&input).await.unwrap();
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

#[tokio::test]
async fn owner_command_drives_fast_predecessors_before_returning_its_task() {
    let (db, worker) = fixture().await;
    let mutation = db::TaskMutation::TaskSetEntryBarrier {
        id: "t".into(),
        expected_version: 1,
        entry_barrier_json: None,
        updated_at: db::now_rfc3339(),
    };
    db.enqueue_task_mutation("t", mutation).await.unwrap();
    let command = crate::task_service::commands::TaskCommand {
        operation: "engine_transition".into(),
        preempt: false,
        arguments: serde_json::json!({"task_id":"t","target_state":"done","version":1,"workflow":workflow(),"actor":cascade_actor(),"reason":"owner queued behind fast work","rejection":false,"skip_before_exit":false,"defer_dispatch_until":null,"board_move":null,"authority":null,"entry_retry":false}),
    };
    let result: crate::workflow::engine::TransitionResult = Arc::new(worker)
        .request_command("t", command)
        .await
        .unwrap();
    assert_eq!(result.task.status, "done");
    let rows = db.task_steps("t").await.unwrap();
    assert_eq!(rows[0].kind, "mutation");
    assert_eq!(rows[0].status, "done");
    assert_eq!(rows[1].kind, "command");
    assert_eq!(rows[1].status, "done");
}

async fn remote_hook_fixture() -> (
    Arc<db::SqliteDb>,
    TaskStepWorker,
    db::Task,
    db::WorkspacePlacement,
    db::TaskStep,
) {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(db::SqliteDb::new(pool));
    let (task, placement, _) = crate::recovery::tests::daemon_owned_fixture(&db).await;
    let mut input = step("done");
    input.task_id = task.id.clone();
    input.expected_status = task.status.clone();
    input.expected_version = task.version;
    input.kind = "hooks".into();
    input.lane = "long".into();
    db.enqueue_step(&input).await.unwrap();
    let claimed = db
        .claim_step("remote-ci", Some(&task.id), &lease_deadline())
        .await
        .unwrap()
        .unwrap();
    db.register_remote_task_operation(&claimed, &placement, "remote-ci-operation")
        .await
        .unwrap();
    let service = crate::TaskService::new(db.clone(), Arc::new(events::EventBus::new(32)));
    let worker = TaskStepWorker::new(service.workflow_engine());
    (db, worker, task, placement, claimed)
}

async fn cancel_remote_task(worker: &TaskStepWorker, task: &db::Task) -> db::Task {
    let mut definition = workflow();
    definition.states[0].name = task.status.clone();
    definition.states[0].kind = StateKind::Active;
    definition.states[1].name = "cancelled".into();
    definition.cancellation_state = Some("cancelled".into());
    let current = TaskRepo::get_by_id(&*worker.db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    worker
        .engine
        .transition(
            &task.id,
            "cancelled",
            current.version,
            &definition,
            &Actor::user(api_types::UserActionSource::Test),
            "Cancel remote CI",
            false,
        )
        .await
        .unwrap()
        .task
}

#[tokio::test]
async fn connected_remote_cancel_ack_supersedes_hook_then_cancels_task() {
    let (db, mut worker, task, placement, claimed) = remote_hook_fixture().await;
    let registry = Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
    let daemon_id = placement.daemon_id.clone().unwrap();
    let (connection, mut outbound) =
        crate::recovery::tests::owner_connection(&registry, &daemon_id, false);
    worker.engine.daemon_connections = Some(registry.clone());
    let responder = tokio::spawn(async move {
        let api_types::DaemonFrame::Request { id, method, params } = outbound.recv().await.unwrap()
        else {
            panic!("cancel request")
        };
        assert_eq!(method, api_types::METHOD_WORKSPACE_CANCEL);
        assert_eq!(params["operation_id"], "remote-ci-operation");
        registry.dispatch_incoming_for_connection(
            &daemon_id,
            connection,
            api_types::DaemonFrame::Response {
                id,
                result: serde_json::json!({"operation_id":params["operation_id"],"state":"killed"}),
            },
        );
    });
    worker.preempt_step(&claimed).await.unwrap();
    db.release_step(&claimed.id, "remote-ci").await.unwrap();
    responder.await.unwrap();
    assert_eq!(cancel_remote_task(&worker, &task).await.status, "cancelled");
    assert!(db
        .pending_remote_cancels(None, None)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        db.task_steps(&task.id)
            .await
            .unwrap()
            .into_iter()
            .find(|s| s.id == claimed.id)
            .unwrap()
            .status,
        "superseded"
    );
    assert!(matches!(
        db.finish_remote_task_operation(&claimed, "remote-ci-operation")
            .await,
        Err(db::DbError::VersionConflict)
    ));
}

#[tokio::test]
async fn disconnected_remote_cancel_fences_workspace_until_reconnect_ack() {
    let (db, worker, task, placement, claimed) = remote_hook_fixture().await;
    worker.preempt_step(&claimed).await.unwrap();
    db.release_step(&claimed.id, "remote-ci").await.unwrap();
    assert_eq!(cancel_remote_task(&worker, &task).await.status, "cancelled");
    assert!(db.task_has_pending_remote_cancel(&task.id).await.unwrap());
    let rows = db.task_steps(&task.id).await.unwrap();
    assert_eq!(
        rows.iter()
            .find(|s| s.id == claimed.id)
            .unwrap()
            .last_error
            .as_deref(),
        Some("remote_operation_unconfirmed")
    );
    let registry = Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
    let backend =
        crate::workspace_backend::DaemonWorkspaceBackend::new(db.clone(), registry.clone());
    use crate::workspace_backend::WorkspaceBackend;
    let spec = crate::workspace_backend::RunSpec {
        purpose: crate::workspace_backend::WorkspaceRunPurpose::CiStep,
        command: "restart CI".into(),
        env: Default::default(),
        timeout_secs: 0,
        max_output_bytes: 4096,
    };
    assert!(backend
        .run(&placement, &spec)
        .await
        .unwrap_err()
        .to_string()
        .contains("pending_remote_cancel"));
    let daemon_id = placement.daemon_id.clone().unwrap();
    let (connection, mut outbound) =
        crate::recovery::tests::owner_connection(&registry, &daemon_id, false);
    let responses = registry.clone();
    let responder = tokio::spawn(async move {
        let api_types::DaemonFrame::Request { id, method, params } = outbound.recv().await.unwrap()
        else {
            panic!("reconnect cancel")
        };
        assert_eq!(method, api_types::METHOD_WORKSPACE_CANCEL);
        responses.dispatch_incoming_for_connection(&daemon_id,connection,api_types::DaemonFrame::Response{id,result:serde_json::json!({"operation_id":params["operation_id"],"state":"unknown"})});
    });
    crate::remote_cancel::reconcile(&db, registry, &placement.daemon_id.clone().unwrap())
        .await
        .unwrap();
    responder.await.unwrap();
    assert!(!db.task_has_pending_remote_cancel(&task.id).await.unwrap());
    assert!(db
        .task_steps(&task.id)
        .await
        .unwrap()
        .iter()
        .any(|s| s.kind == "mutation" && s.status == "pending"));
    assert!(matches!(
        db.finish_remote_task_operation(&claimed, "remote-ci-operation")
            .await,
        Err(db::DbError::VersionConflict)
    ));
}

#[tokio::test]
async fn protected_merge_finishes_before_preempting_cancel_can_claim() {
    let (db, worker) = fixture().await;
    let mut input = step("done");
    input.kind = "hooks".into();
    db.enqueue_step(&input).await.unwrap();
    let merge = db
        .claim_step("push", Some("t"), &lease_deadline())
        .await
        .unwrap()
        .unwrap();
    db::task_writer::in_task_step(merge.clone(), db.protect_step_integration())
        .await
        .unwrap();
    let mut cancel = step("done");
    cancel.kind = "command".into();
    cancel.causation_key = "cancel".into();
    cancel.payload_json =
        serde_json::json!({"operation":"perform_task_action_as","arguments":[],"preempt":true})
            .to_string();
    db.enqueue_step(&cancel).await.unwrap();
    db.request_task_preemption("t").await.unwrap();
    assert!(db
        .claim_step("cancel", Some("t"), &lease_deadline())
        .await
        .unwrap()
        .is_none());
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    sqlx::query("UPDATE task SET status='done',version=version+1 WHERE id='t'")
        .execute(&mut *tx)
        .await
        .unwrap();
    db.finish_step_in_tx(&mut tx, &merge, "done", None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db.release_step(&merge.id, "push").await.unwrap();
    let cancel = db
        .claim_step("cancel", Some("t"), &lease_deadline())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cancel.kind, "command");
    assert_eq!(
        TaskRepo::get_by_id(&*worker.db, "t", false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "done"
    );
}

#[tokio::test]
async fn older_daemon_rejection_still_cancels_task_and_fences_workspace() {
    let (db, mut worker, task, placement, claimed) = remote_hook_fixture().await;
    let registry = Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
    let daemon_id = placement.daemon_id.clone().unwrap();
    let (connection, mut outbound) =
        crate::recovery::tests::owner_connection(&registry, &daemon_id, false);
    worker.engine.daemon_connections = Some(registry.clone());
    let responder = tokio::spawn(async move {
        let api_types::DaemonFrame::Request { id, method, .. } = outbound.recv().await.unwrap()
        else {
            panic!("cancel request")
        };
        assert_eq!(method, api_types::METHOD_WORKSPACE_CANCEL);
        registry.dispatch_incoming_for_connection(
            &daemon_id,
            connection,
            api_types::DaemonFrame::Error {
                id: Some(id),
                error: api_types::DaemonErrorPayload {
                    code: api_types::UNSUPPORTED_METHOD.into(),
                    message: "older daemon".into(),
                    details: None,
                },
            },
        );
    });
    worker.preempt_step(&claimed).await.unwrap();
    db.release_step(&claimed.id, "remote-ci").await.unwrap();
    responder.await.unwrap();
    assert_eq!(cancel_remote_task(&worker, &task).await.status, "cancelled");
    assert!(db.task_has_pending_remote_cancel(&task.id).await.unwrap());
    assert_eq!(
        db.task_steps(&task.id)
            .await
            .unwrap()
            .into_iter()
            .find(|s| s.id == claimed.id)
            .unwrap()
            .last_error
            .as_deref(),
        Some("remote_operation_unconfirmed")
    );
}

#[tokio::test]
async fn owner_command_busy_retains_its_write_until_predecessor_releases() {
    let (db, worker) = fixture().await;
    let predecessor = db
        .enqueue_task_mutation(
            "t",
            db::TaskMutation::TaskSetEntryBarrier {
                id: "t".into(),
                expected_version: 1,
                entry_barrier_json: None,
                updated_at: db::now_rfc3339(),
            },
        )
        .await
        .unwrap();
    let claimed = db
        .claim_step("other-worker", Some("t"), &lease_deadline())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.id, predecessor);
    let worker = Arc::new(worker);
    let command = crate::task_service::commands::TaskCommand {
        operation: "engine_transition".into(),
        preempt: false,
        arguments: serde_json::json!({"task_id":"t","target_state":"done","version":1,"workflow":workflow(),"actor":cascade_actor(),"reason":"busy owner write","rejection":false,"skip_before_exit":false,"defer_dispatch_until":null,"board_move":null,"authority":null,"entry_retry":false}),
    };
    let result = worker
        .request_command::<crate::workflow::engine::TransitionResult>("t", command)
        .await;
    assert!(matches!(
        result,
        Err(ServiceError::TaskBusy {
            pending_steps: 2,
            ..
        })
    ));
    assert_eq!(
        TaskRepo::get_by_id(&*db, "t", false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "todo"
    );
    db::task_writer::in_task_step(claimed.clone(), db.execute_task_mutation(&claimed))
        .await
        .unwrap();
    db.release_step(&claimed.id, "other-worker").await.unwrap();
    assert_eq!(worker.drain("t").await.unwrap().status, "done");
    assert!(db
        .task_steps("t")
        .await
        .unwrap()
        .iter()
        .all(|s| s.status == "done"));
}
