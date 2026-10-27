//! Tests for charter dispatch in the task dispatcher.

use super::*;
#[tokio::test]
async fn dispatcher_parks_charter_task_with_missing_governance_before_execution() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    make_project_charter_backed(&db, &project_id).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(
        &db,
        &project_id,
        "Charter work missing governance",
        "in_progress",
        0,
    )
    .await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    assert_eq!(
        dispatcher
            .check_once_and_drain()
            .await
            .expect("dispatcher runs"),
        0
    );
    assert!(rx.try_recv().is_err());
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER,
        )
        .await
        .expect("execution count loads"),
        0
    );
    assert!(
        WorkspaceRepo::get_by_task_id(&*db, &task.id)
            .await
            .expect("Workspace lookup succeeds")
            .is_none(),
        "governance admission must fail before Workspace creation"
    );
    let parked = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("Task reloads")
        .expect("Task exists");
    let disposition = deferred_dispatch::dispatch_disposition_for_test(&parked)
        .expect("missing governance records a visible deterministic blocker");
    assert!(disposition.safe_message.contains("Charter governance"));
    assert!(disposition.safe_message.contains("missing or stale"));
}

#[tokio::test]
async fn dispatcher_starts_charter_backed_work_without_a_baseline_gate() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    make_project_charter_backed(&db, &project_id).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "charter work", "in_progress", 0).await;
    make_task_charter_governed(&db, &task).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let mut total_progress = 0;
    let mut execution_received = false;
    // The loop exits on the first dispatch, so the budget only matters on a
    // runner slow enough to starve the spawned executor. Keep it generous: a
    // dispatch that never lands is a real refusal, and the report below says
    // which one rather than leaving a bare timeout.
    for _ in 0..DISPATCH_WAIT_ATTEMPTS {
        total_progress += dispatcher
            .check_once_and_drain()
            .await
            .expect("dispatcher runs");
        if tokio::time::timeout(DISPATCH_WAIT_STEP, rx.recv())
            .await
            .is_ok()
        {
            execution_received = true;
            break;
        }
    }
    if total_progress == 0 {
        panic!(
            "Charter-backed work leaves its initial state: {}",
            describe_stalled_dispatch(&db, &task.id).await
        );
    }
    assert!(
        execution_received,
        "the workflow reaches its configured worker"
    );

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, "in_progress");
    assert!(deferred_dispatch::dispatch_disposition_for_test(&current).is_none());
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER
        )
        .await
        .expect("execution count loads"),
        1
    );
}

#[tokio::test]
async fn wake_does_not_duplicate_already_dispatched_charter_work() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    make_project_charter_backed(&db, &project_id).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "charter work", "in_progress", 0).await;
    make_task_charter_governed(&db, &task).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let mut execution_received = false;
    for _ in 0..DISPATCH_WAIT_ATTEMPTS {
        dispatcher
            .check_once_and_drain()
            .await
            .expect("dispatcher runs");
        if tokio::time::timeout(DISPATCH_WAIT_STEP, rx.recv())
            .await
            .is_ok()
        {
            execution_received = true;
            break;
        }
    }
    if !execution_received {
        panic!(
            "executor receives initial dispatch: {}",
            describe_stalled_dispatch(&db, &task.id).await
        );
    }
    deferred_dispatch::wake_task_dispatch(&db, &task.id, "test: redundant wake")
        .await
        .expect("wake succeeds");

    let woken = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(
        deferred_dispatch::dispatch_disposition_for_test(&woken).is_none(),
        "an already-dispatched Task has no deferred disposition"
    );

    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");
    assert_eq!(dispatched, 0, "the wake does not duplicate active work");

    // Restart: a fresh dispatcher over the same database must not re-dispatch
    // the Task the previous instance already moved on.
    let (restarted, mut restarted_rx) =
        build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    assert_eq!(
        restarted
            .check_once_and_drain()
            .await
            .expect("dispatcher runs"),
        0,
        "restart must not duplicate the dispatch"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), restarted_rx.recv())
            .await
            .is_err()
    );
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER
        )
        .await
        .expect("execution count loads"),
        1,
        "exactly one execution exists after wake + restart"
    );
}

#[test]
fn a_running_execution_slot_is_not_a_deterministic_dispatch_refusal() {
    // A slot held by a concurrently running execution frees itself when that
    // execution terminalises, and nothing about that touches the Task row. If
    // this were classified as deterministic, the dispatcher would record a
    // disposition and never reconsider the Task — the wedge this guards.
    assert!(!helpers::is_deterministic_dispatch_refusal(
        &crate::ServiceError::ExecutionAlreadyRunning {
            scope: "repository".to_owned(),
            execution_id: new_uuid_v4(),
        }
    ));
    assert!(!helpers::is_deterministic_dispatch_refusal(
        &crate::ServiceError::ExecutionAlreadyRunning {
            scope: crate::workflow::default_roles::REVIEWER.to_owned(),
            execution_id: new_uuid_v4(),
        }
    ));

    // The contrast that must keep working: a governance refusal is durable and
    // still earns a disposition.
    assert!(helpers::is_deterministic_dispatch_refusal(
        &crate::ServiceError::GuardRejection {
            guard: "dependency_gate".to_owned(),
            reason: "task has 1 unsatisfied dependency".to_owned(),
        }
    ));
}
