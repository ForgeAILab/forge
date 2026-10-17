//! Accepted Retry of an imported merge whose checkout belongs to an absent
//! machine. No local checkout exists: all candidate reads must use its owner.
use super::*;
use crate::{daemon_transport::DaemonConnectionRegistry, recovery::HeartbeatMonitor, TaskService};
use api_types::{Actor, TaskAction, UserActionSource};
use db::{DaemonRepo, WorkspacePlacementRepo};
use serde_json::{json, Value};
use std::time::Duration;

struct OfflineRetry {
    _dir: tempfile::TempDir,
    url: String,
    db: Arc<SqliteDb>,
    service: TaskService,
    task: db::Task,
    placement: db::WorkspacePlacement,
}

fn runtime(db: Arc<SqliteDb>) -> (TaskService, Arc<DaemonConnectionRegistry>) {
    let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
    let service = TaskService::new(db.clone(), Arc::new(events::EventBus::default()))
        .with_daemon_connections(registry.clone());
    let router = Arc::new(
        (*service.workspace_backend_router())
            .clone()
            .with_daemon(Arc::new(
                crate::workspace_backend::DaemonWorkspaceBackend::new(db.clone(), registry.clone()),
            )),
    );
    let merge = Arc::new(crate::MergeService::new_for_test(
        db,
        std::path::PathBuf::from("/no-server-checkout"),
    ));
    merge.set_workspace_backend_router(router.clone());
    (
        service
            .with_workspace_backend_router(router)
            .with_merge_service(merge),
        registry,
    )
}

async fn task(db: &SqliteDb, id: &str) -> db::Task {
    TaskRepo::get_by_id(db, id, false).await.unwrap().unwrap()
}

fn wait(task: &db::Task) -> Value {
    json!(task.condition.integration_reason())
}

async fn fixture() -> OfflineRetry {
    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("restart.sqlite").display()
    );
    let pool = db::create_sqlite_pool(&url).await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    let (seed, placement, execution) = crate::recovery::tests::daemon_owned_fixture(&db).await;
    sqlx::query("DELETE FROM execution WHERE id=?")
        .bind(execution.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task SET status='merging',review_passed_at=? WHERE id=?")
        .bind(db::now_rfc3339())
        .bind(&seed.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE workspace_placement SET disconnected_at=? WHERE id=?")
        .bind(db::now_rfc3339())
        .bind(&placement.id)
        .execute(db.pool())
        .await
        .unwrap();
    DaemonRepo::mark_offline(
        &*db,
        placement.daemon_id.as_deref().unwrap(),
        &db::now_rfc3339(),
    )
    .await
    .unwrap();
    db.check_task_conditions_of(std::slice::from_ref(&seed.id))
        .await
        .unwrap();
    let (service, _) = runtime(db.clone());
    super::import::import_at_start(&db).await.unwrap();
    service.drain(&seed.id).await.unwrap();
    let parked = task(&db, &seed.id).await;
    assert_eq!(wait(&parked)["cause"], "owner_required");
    let offers = service
        .task_action_offers(&seed.id, &Actor::user(UserActionSource::Api))
        .await
        .unwrap();
    assert!(offers
        .available_actions
        .iter()
        .any(|offer| offer.reason == "integration_retry"));
    service
        .perform_task_action_as(
            &seed.id,
            TaskAction::retry(),
            offers.version,
            Actor::user(UserActionSource::Api),
        )
        .await
        .unwrap();
    service.drain(&seed.id).await.unwrap();
    let waiting = task(&db, &seed.id).await;
    assert_eq!(wait(&waiting)["cause"], "owner_offline");
    assert_eq!(
        wait(&waiting)["owner_id"],
        placement.daemon_id.as_deref().unwrap()
    );
    assert!(db
        .pending_quarantined_import_retry(&seed.id)
        .await
        .unwrap()
        .is_some());
    OfflineRetry {
        _dir: dir,
        url,
        db,
        service,
        task: waiting,
        placement,
    }
}

#[tokio::test]
async fn offline_checkout_retry_survives_restart_and_reconnect_admits_on_the_same_owner() {
    let OfflineRetry {
        _dir,
        url,
        db,
        service,
        task: waiting,
        placement,
    } = tokio::time::timeout(Duration::from_secs(30), fixture())
        .await
        .expect("fixture reaches accepted offline Retry");
    let import = db
        .pending_quarantined_import_retry(&waiting.id)
        .await
        .unwrap()
        .unwrap();
    drop(service);
    db.pool().close().await;
    drop(db);
    let db = Arc::new(SqliteDb::new(db::create_sqlite_pool(&url).await.unwrap()));
    let (service, registry) = runtime(db.clone());
    crate::recovery::CrashRecovery::new(db.clone(), Arc::new(events::EventBus::default()))
        .run_recovery()
        .await
        .unwrap();
    super::import::import_at_start(&db).await.unwrap();
    service.drain(&waiting.id).await.unwrap();
    let still_waiting = task(&db, &waiting.id).await;
    assert_eq!(still_waiting.status, "merging");
    assert_eq!(wait(&still_waiting)["cause"], "owner_offline");
    assert_eq!(
        db.pending_quarantined_import_retry(&waiting.id)
            .await
            .unwrap()
            .unwrap()
            .import_source_json,
        import.import_source_json
    );
    service.integration_owner_ready(&waiting.id).await.unwrap();
    assert!(
        db.current_integration_attempt(&waiting.id)
            .await
            .unwrap()
            .is_none(),
        "an offline machine cannot resume admission"
    );

    let daemon = placement.daemon_id.clone().unwrap();
    sqlx::query("UPDATE daemon SET status='online' WHERE id=?")
        .bind(&daemon)
        .execute(db.pool())
        .await
        .unwrap();
    let (connection, mut outbound) =
        crate::recovery::tests::owner_connection(&registry, &daemon, false);
    let responder_registry = registry.clone();
    let responder_daemon = daemon.clone();
    let responder = tokio::spawn(async move {
        let mut methods = Vec::new();
        while let Some(api_types::DaemonFrame::Request { id, method, params }) =
            outbound.recv().await
        {
            let result = match method.as_str() {
                api_types::METHOD_WORKSPACE_DESCRIBE => {
                    json!({"workspace_handle":params["workspace_handle"],"generation":1,
                    "exists":true,"head_sha":"base-head","dirty":false,"branch":"task/remote","locked":false,
                    "active_execution_ids":[],"journaled_execution_ids":[]})
                }
                api_types::METHOD_WORKSPACE_READ => json!({"kind":"git","output":"base-head"}),
                other => panic!("unexpected owner request: {other}: {params}"),
            };
            methods.push(method.clone());
            responder_registry.dispatch_incoming_for_connection(
                &responder_daemon,
                connection,
                api_types::DaemonFrame::Response { id, result },
            );
            if method == api_types::METHOD_WORKSPACE_READ {
                return methods;
            }
        }
        panic!("owner connection ended before the candidate read");
    });
    let disconnected = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
        .await
        .unwrap()
        .unwrap();
    assert!(crate::recovery::reconcile_workspace_placement(
        &db,
        &events::EventBus::default(),
        &registry,
        None,
        &disconnected
    )
    .await
    .unwrap());
    service.integration_owner_ready(&waiting.id).await.unwrap();
    // Drive the accepted command through the admission sweep, before the
    // queue worker starts validating the newly admitted candidate. `drain`
    // also pumps that worker and would cross the boundary under test.
    service
        .reconcile_integration_admission(&task(&db, &waiting.id).await)
        .await
        .unwrap();
    let methods = responder.await.unwrap();
    assert_eq!(
        methods,
        [
            api_types::METHOD_WORKSPACE_DESCRIBE,
            api_types::METHOD_WORKSPACE_READ
        ]
    );
    let admitted = db
        .current_integration_attempt(&waiting.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(admitted.state, S::Queued, "{admitted:#?}");
    assert_eq!(
        admitted.predecessor_attempt_id.as_deref(),
        Some(import.id.as_str())
    );
    assert_eq!(admitted.daemon_id, placement.daemon_id);
    assert_eq!(
        admitted.placement_id.as_deref(),
        Some(placement.id.as_str())
    );
    assert_eq!(
        admitted.original_candidate_sha.as_deref(),
        Some("base-head")
    );
    assert_eq!(wait(&task(&db, &waiting.id).await)["kind"], "waiting");
    assert!(db
        .pending_quarantined_import_retry(&waiting.id)
        .await
        .unwrap()
        .is_none());
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution WHERE task_id=?")
        .bind(&waiting.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(
        count, 0,
        "resume reads the candidate; it invents no execution"
    );
}

#[tokio::test]
async fn offline_checkout_retry_that_never_reconnects_has_attention_and_a_disconnect_bound() {
    let fixture = tokio::time::timeout(Duration::from_secs(30), fixture())
        .await
        .expect("fixture reaches accepted offline Retry");
    let monitor = HeartbeatMonitor::new(fixture.db.clone(), Arc::new(events::EventBus::default()))
        .with_max_disconnect(Duration::from_secs(300));
    monitor.check_once().await.unwrap();
    fixture.service.drain(&fixture.task.id).await.unwrap();
    let attention: (String, String, String) = sqlx::query_as("SELECT attention_type,status,recommended_action FROM attention_projection WHERE dedupe_key=?")
        .bind(format!("workspace-owner:{}", fixture.placement.id)).fetch_one(fixture.db.pool()).await.unwrap();
    assert_eq!(
        attention,
        (
            "runtime_offline".into(),
            "open".into(),
            "reconnect_owner".into()
        )
    );
    // Age the persisted timestamp, never sleep to cross the production deadline.
    sqlx::query("UPDATE workspace_placement SET disconnected_at='2000-01-01T00:00:00Z' WHERE id=?")
        .bind(&fixture.placement.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    monitor.check_once().await.unwrap();
    fixture.service.drain(&fixture.task.id).await.unwrap();
    let failed = WorkspacePlacementRepo::get_by_id(&*fixture.db, &fixture.placement.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.state, db::PlacementState::Failed);
    assert_eq!(
        failed.failure_cause,
        Some(db::PlacementFailureCause::OwnerDisconnectedTimeout)
    );
    let attention: String =
        sqlx::query_scalar("SELECT status FROM attention_projection WHERE dedupe_key=?")
            .bind(format!("workspace-owner:{}", fixture.placement.id))
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    assert_eq!(attention, "open");
    let offers = fixture
        .service
        .task_action_offers(&fixture.task.id, &Actor::user(UserActionSource::Api))
        .await
        .unwrap();
    assert!(offers
        .available_actions
        .iter()
        .any(|offer| offer.action.verb() == "cancel"));
    assert_eq!(task(&fixture.db, &fixture.task.id).await.status, "merging");
    let charges: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_budget_charge WHERE task_id=?")
            .bind(&fixture.task.id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    assert_eq!(charges, 0);
}
