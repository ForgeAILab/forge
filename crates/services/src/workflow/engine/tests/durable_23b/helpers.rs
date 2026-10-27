//! Shared fixtures and helpers for durable workflow tests.

use super::*;

pub(in super::super) async fn enter_review(
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
pub(in super::super) async fn count(fixture: &FailedCiFixture, sql: &str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(fixture.db.pool())
        .await
        .unwrap()
}

/// A fixture whose review entry stays in `review` after green CI (the gate
/// asks for the owner's approval), with the given CI steps.
pub(in super::super) async fn approval_ci_fixture(ci_steps: serde_json::Value) -> FailedCiFixture {
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
pub(super) async fn re_enter_review(fixture: &FailedCiFixture, reason: &str) {
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

/// Sweep the check worker until `consumer_id` has its delivery step, and
/// return that step's id and envelope. The step is not executed.
pub(super) async fn delivery_of(
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

/// Suspend the hooks step on its queued check, with no check worker running.
pub(in super::super) async fn suspend_on_entry_check(fixture: &FailedCiFixture) {
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

pub(super) async fn review_summaries(fixture: &FailedCiFixture) -> Vec<(db::ReviewStatus, String)> {
    db::ReviewRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap()
        .into_iter()
        .map(|review| (review.status, review.step_results_json))
        .collect()
}

pub(super) async fn set_project_review_commands(fixture: &FailedCiFixture, command: &str) {
    sqlx::query("UPDATE project SET settings=json_set(CASE WHEN json_valid(settings) THEN settings ELSE '{}' END,'$.default_review_config',json(?)), version=version+1 WHERE id=?")
        .bind(json!({"ci_steps":[command]}).to_string())
        .bind(&fixture.task.project_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
}

/// Wake the suspended hooks step as its deadline would and run the step
/// worker (no check worker) until it waits on another consumer than `before`.
pub(super) async fn wake_and_resuspend(fixture: &FailedCiFixture, before: &str) -> String {
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

pub(super) async fn awaited_consumer(fixture: &FailedCiFixture) -> Option<String> {
    fixture
        .db
        .task_steps(&fixture.task.id)
        .await
        .unwrap()
        .into_iter()
        .find(|step| step.kind == "hooks" && step.status == "suspended")
        .and_then(|step| step.awaited_consumer_id)
}

/// The Task's worktree directory is gone and the disk is under its floor:
/// whoever needs the worktree would have to make it again and may not.
/// Returns the injected free-bytes reading.
pub(super) async fn worktree_gone_under_the_disk_floor(
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
pub(super) async fn suspend_on_disk(fixture: &FailedCiFixture) {
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
pub(super) async fn resume_after_the_disk_recheck(fixture: &FailedCiFixture) {
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

pub(super) async fn waits_for_disk(fixture: &FailedCiFixture) -> bool {
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
