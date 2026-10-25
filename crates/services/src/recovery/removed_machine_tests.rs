//! Removed-machine settlement and action cases.

use super::*;
use api_types::TaskAction;

#[tokio::test]
async fn removed_owner_settles_running_execution_and_releases_its_workspace() {
    use crate::workspace_backend::WorkspaceBackend;
    let db = Arc::new(SqliteDb::new(
        db::create_sqlite_pool("sqlite::memory:").await.unwrap(),
    ));
    db::run_migrations(db.pool()).await.unwrap();
    let (task, placement, execution) = tests::daemon_owned_fixture(&db).await;
    let daemon_id = placement.daemon_id.as_deref().unwrap();
    DaemonRepo::mark_offline(&*db, daemon_id, &now_rfc3339())
        .await
        .unwrap();
    let bus = Arc::new(EventBus::new(64));
    let service = TaskService::new(db.clone(), bus.clone());
    let original_name = DaemonRepo::get_by_id(&*db, daemon_id)
        .await
        .unwrap()
        .unwrap()
        .hostname;
    sqlx::query("INSERT INTO repo_provision_retry(repo_id,runtime_id,next_attempt_at) SELECT w.repo_id,p.runtime_id,? FROM workspace w JOIN workspace_placement p ON p.workspace_id=w.id WHERE p.id=?")
            .bind(now_rfc3339()).bind(&placement.id).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO project_machine_readiness(project_id,owner_kind,daemon_id,runtime_id,status,checks_digest) VALUES (?,'daemon',?,?,'not_ready','old-digest')")
            .bind(&task.project_id).bind(daemon_id).bind(&placement.runtime_id).execute(db.pool()).await.unwrap();
    let preview = db
        .daemon_removal_preview(daemon_id, "admin", true)
        .await
        .unwrap();
    assert_eq!((preview.tasks_to_replace, preview.agents_to_retire), (1, 1));
    let result = db
        .remove_daemon(daemon_id, "admin", true, "local", false)
        .await
        .unwrap();
    assert_eq!(result.provisioning_attempts_cleared, 1);
    assert_eq!(result.readiness_records_cleared, 1);
    assert_eq!(result.placements_failed, 1);
    assert_eq!(result.tasks_to_replace, 1);
    assert_eq!(result.agents_retired, 1);
    assert_eq!(result.tasks_queued, 1);
    assert_eq!(
        ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ExecutionStatus::Running,
        "the removal transaction writes no Task or execution row"
    );
    let settled = service.drain(&task.id).await.unwrap();
    let failed = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.status, ExecutionStatus::Failed);
    assert_eq!(failed.stop_reason, Some(StopReason::DaemonDisconnected));
    assert_eq!(failed.resume_policy, Some(ResumePolicy::Manual));
    assert_eq!(
        failed.stopped_by.as_deref(),
        Some("user:api"),
        "settlement is attributed to the removing user, not the heartbeat"
    );
    assert_eq!(
        settled.status, task.status,
        "owner loss does not spend workflow retries"
    );
    let annotation: serde_json::Value =
        serde_json::from_str(settled.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["blocking_reason"], "machine_removed");
    assert_eq!(annotation["blocked_by"], "user:api");
    assert_eq!(
        settled.assignee_id, None,
        "the retired Agent's Task follows the archive sweep"
    );
    let current = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        current.state,
        PlacementState::Cleaned,
        "the workspace is released, not pinned to the removed owner"
    );
    assert!(current.workspace_handle.is_none());
    assert_eq!(
        current.failure_cause,
        Some(PlacementFailureCause::OwnerDisconnected),
        "an existing failure cause is kept"
    );
    let workspace = WorkspaceRepo::get_by_id(&*db, &placement.workspace_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(workspace.status, db::WorkspaceStatus::Cleaned);
    let actor = api_types::Actor::user(api_types::UserActionSource::Api);
    let snapshot = service
        .task_action_snapshot(&task.id, &actor)
        .await
        .unwrap();
    assert!(!snapshot.owner_disconnected);
    assert!(!crate::available_actions(&snapshot)
        .iter()
        .any(|offer| offer.reason == "owner_reconcile"));
    let name:String=sqlx::query_scalar("SELECT d.hostname FROM execution e JOIN workspace_placement p ON p.workspace_id=e.workspace_id JOIN daemon d ON d.id=p.daemon_id WHERE e.id=?")
            .bind(&execution.id).fetch_one(db.pool()).await.unwrap();
    assert_eq!(name, original_name);
    let backend = crate::workspace_backend::DaemonWorkspaceBackend::new(
        db.clone(),
        Arc::new(DaemonConnectionRegistry::without_handlers()),
    );
    assert!(
        !backend.cleanup(&current).await.unwrap().removed,
        "removal abandons physical files without an RPC"
    );
    let status = crate::OperatorStatusService::new_for_test(db.clone());
    let status = status.compute_status().await.unwrap();
    assert!(!status
        .daemon_issues
        .iter()
        .any(|row| row.daemon_id == daemon_id));
    assert!(!status
        .daemon_pressure
        .iter()
        .any(|row| row.daemon_id == daemon_id));
}

/// A server-owned workspace that only executed on the removed machine is
/// not released, and a failure it already recorded is not overwritten.
#[tokio::test]
async fn removal_keeps_a_server_owned_workspace_that_only_executed_there() {
    let db = Arc::new(SqliteDb::new(
        db::create_sqlite_pool("sqlite::memory:").await.unwrap(),
    ));
    db::run_migrations(db.pool()).await.unwrap();
    let (task, placement, execution) = tests::daemon_owned_fixture(&db).await;
    let daemon_id = placement.daemon_id.clone().unwrap();
    DaemonRepo::mark_offline(&*db, &daemon_id, &now_rfc3339())
        .await
        .unwrap();
    sqlx::query("UPDATE workspace_placement SET owner_kind='server',daemon_id=NULL,runtime_id=NULL,state='failed',failure_cause='prepare_failed',disconnected_at=NULL WHERE id=?")
            .bind(&placement.id).execute(db.pool()).await.unwrap();
    let service = TaskService::new(db.clone(), Arc::new(EventBus::new(64)));
    let result = db
        .remove_daemon(&daemon_id, "admin", true, "local", false)
        .await
        .unwrap();
    assert_eq!(result.placements_failed, 0);
    assert_eq!(result.tasks_to_replace, 0);
    service.drain(&task.id).await.unwrap();
    let current = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.state, PlacementState::Failed);
    assert_eq!(
        current.failure_cause,
        Some(PlacementFailureCause::PrepareFailed)
    );
    assert_eq!(current.workspace_handle, placement.workspace_handle);
    assert_eq!(current.execution_daemon_id, None);
    assert_eq!(
        ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ExecutionStatus::Failed,
        "the run that was live on the machine still settles"
    );
}

/// A server-owned workspace stranded by the vanished executor is not
/// released, but its wait for that executor ends: left `failed` on an owner
/// that no longer exists it would offer only a retry that can never run.
#[tokio::test]
async fn removal_ends_a_server_owned_workspaces_wait_for_the_removed_executor() {
    let db = Arc::new(SqliteDb::new(
        db::create_sqlite_pool("sqlite::memory:").await.unwrap(),
    ));
    db::run_migrations(db.pool()).await.unwrap();
    let (task, placement, _execution) = tests::daemon_owned_fixture(&db).await;
    let daemon_id = placement.daemon_id.clone().unwrap();
    DaemonRepo::mark_offline(&*db, &daemon_id, &now_rfc3339())
        .await
        .unwrap();
    sqlx::query("UPDATE workspace_placement SET owner_kind='server',daemon_id=NULL,runtime_id=NULL,state='failed',failure_cause='owner_disconnected_timeout' WHERE id=?")
            .bind(&placement.id).execute(db.pool()).await.unwrap();
    let service = TaskService::new(db.clone(), Arc::new(EventBus::new(64)));
    let result = db
        .remove_daemon(&daemon_id, "admin", true, "local", false)
        .await
        .unwrap();
    assert_eq!(result.placements_failed, 0);
    service.drain(&task.id).await.unwrap();
    let current = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.state, PlacementState::Ready);
    assert_eq!(current.failure_cause, None);
    assert_eq!(current.execution_daemon_id, None);
    assert_eq!(current.workspace_handle, placement.workspace_handle);
    let after = offers(&service, &task.id).await;
    assert!(
        !after.iter().any(|offer| offer.contains("owner_reconcile")),
        "{after:?}"
    );
}

async fn offers(service: &TaskService, task_id: &str) -> Vec<String> {
    let actor = api_types::Actor::user(api_types::UserActionSource::Api);
    let snapshot = service.task_action_snapshot(task_id, &actor).await.unwrap();
    crate::available_actions(&snapshot)
        .into_iter()
        .map(|offer| format!("{}[{}]", offer.action.verb(), offer.reason))
        .collect()
}

/// A Task whose workspace lived on the removed machine must have some
/// action that moves it off that machine.
#[tokio::test]
async fn task_on_removed_machine_has_an_exit() {
    let db = Arc::new(SqliteDb::new(
        db::create_sqlite_pool("sqlite::memory:").await.unwrap(),
    ));
    db::run_migrations(db.pool()).await.unwrap();
    let (task, placement, _execution) = tests::daemon_owned_fixture(&db).await;
    let daemon_id = placement.daemon_id.clone().unwrap();
    DaemonRepo::mark_offline(&*db, &daemon_id, &now_rfc3339())
        .await
        .unwrap();
    let service = TaskService::new(db.clone(), Arc::new(EventBus::new(64)));
    db.remove_daemon(&daemon_id, "admin", true, "local", false)
        .await
        .unwrap();
    let settled = service.drain(&task.id).await.unwrap();
    let before = offers(&service, &task.id).await;
    assert!(
        !before.iter().any(|offer| offer.contains("owner_reconcile")),
        "Retry on Workspace Owner is never offered for a removed owner: {before:?}"
    );
    // The proof's follow-up actions: neither may leave the Task pinned.
    let _retry = service
        .perform_task_action(
            &task.id,
            TaskAction::Retry {
                reason: None,
                fresh_session: Some(true),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            },
            settled.version,
        )
        .await;
    let drained = service.drain(&task.id).await.unwrap();
    let _restart = service
        .perform_task_action(
            &task.id,
            TaskAction::Restart { reason: None },
            drained.version,
        )
        .await;
    service.drain(&task.id).await.unwrap();
    let after = offers(&service, &task.id).await;
    let current = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !(current.state == PlacementState::Failed
            && current.daemon_id.as_deref() == Some(daemon_id.as_str())
            && after == before),
        "Task is still pinned to the removed machine with the same no-op offers: {after:?}"
    );
}

/// The brief's primary case on the Task's LIVE workspace: a parked action
/// fenced by a pending remote cancel on the removed machine.
#[tokio::test]
async fn parked_action_on_live_workspace_proceeds_after_removal() {
    let db = Arc::new(SqliteDb::new(
        db::create_sqlite_pool("sqlite::memory:").await.unwrap(),
    ));
    db::run_migrations(db.pool()).await.unwrap();
    let (task, placement, execution) = tests::daemon_owned_fixture(&db).await;
    let daemon_id = placement.daemon_id.clone().unwrap();
    DaemonRepo::mark_offline(&*db, &daemon_id, &now_rfc3339())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE workspace_placement SET state='disconnected', disconnected_at=? WHERE id=?",
    )
    .bind(now_rfc3339())
    .bind(&placement.id)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query("INSERT INTO pending_remote_cancel VALUES ('lost-op','lost-step',?,?,?,?,1,0,?)")
        .bind(&placement.workspace_id)
        .bind(&placement.id)
        .bind(&daemon_id)
        .bind(placement.runtime_id.as_deref().unwrap())
        .bind(now_rfc3339())
        .execute(db.pool())
        .await
        .unwrap();
    // The owner Cancel/Hold already terminalized the run locally.
    sqlx::query("UPDATE execution SET status='cancelled' WHERE id=?")
        .bind(&execution.id)
        .execute(db.pool())
        .await
        .unwrap();
    let service = TaskService::new(db.clone(), Arc::new(EventBus::new(64)));
    let parked = service
        .perform_task_action(
            &task.id,
            TaskAction::Retry {
                reason: None,
                fresh_session: Some(true),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            },
            task.version,
        )
        .await
        .unwrap()
        .task;
    assert!(crate::deferred_dispatch::queued_recovery(&parked).is_some());
    let result = db
        .remove_daemon(&daemon_id, "admin", true, "local", false)
        .await
        .unwrap();
    assert_eq!(result.pending_remote_cancels_cleared, 1);
    let current = service.drain(&task.id).await.unwrap();
    let after = offers(&service, &task.id).await;
    let p = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !(p.state == PlacementState::Failed && p.daemon_id.as_deref() == Some(daemon_id.as_str())),
        "parked action did not proceed: Task still pinned to the removed machine, offers {after:?}"
    );
    assert!(
        !after.iter().any(|offer| offer.contains("owner_reconcile")),
        "{after:?}"
    );
    assert!(
        !current
            .error_annotation
            .as_deref()
            .is_some_and(|raw| raw.contains("no apply plan")),
        "the parked owner retry must not be replayed without a plan: {:?}",
        current.error_annotation
    );
}

/// Agents pinned to the removed machine are retired with it.
#[tokio::test]
async fn removed_machine_agents_are_retired() {
    let db = Arc::new(SqliteDb::new(
        db::create_sqlite_pool("sqlite::memory:").await.unwrap(),
    ));
    db::run_migrations(db.pool()).await.unwrap();
    let (_task, placement, _execution) = tests::daemon_owned_fixture(&db).await;
    let daemon_id = placement.daemon_id.clone().unwrap();
    DaemonRepo::mark_offline(&*db, &daemon_id, &now_rfc3339())
        .await
        .unwrap();
    db.remove_daemon(&daemon_id, "admin", true, "local", false)
        .await
        .unwrap();
    let pinned: Vec<(String, String)> =
        sqlx::query_as("SELECT id,status FROM agent_current WHERE daemon_id=?")
            .bind(&daemon_id)
            .fetch_all(db.pool())
            .await
            .unwrap();
    assert!(
        pinned.is_empty(),
        "agents still pinned to the tombstone: {pinned:?}"
    );
    let retired: Vec<(i64, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT i.paused,i.archived_at,i.description FROM agent_identity i JOIN agent_profile p ON p.id=i.selected_profile_id WHERE p.daemon_id=?",
        )
        .bind(&daemon_id)
        .fetch_all(db.pool())
        .await
        .unwrap();
    assert_eq!(retired.len(), 1, "the row is kept for history");
    assert_eq!(retired[0].0, 1);
    assert!(retired[0].1.is_some());
    assert!(retired[0].2.as_deref().unwrap().contains("was removed"));
}
