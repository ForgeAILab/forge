//! Claims that wait on disk pressure instead of failing the Task.

use super::*;

/// A launch after the claim finds its worktree gone under the disk
/// floor. That is the claim's wait, not a failed run: the execution ends
/// before dispatch, auto-resumable and without blocking the Task, and the
/// Task takes the disk capacity wait.
#[tokio::test]
async fn a_launch_refused_for_disk_takes_the_claims_disk_wait_instead_of_failing_the_task() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let (project_id, _) = seed_project_with_real_repo(&db, repo.path()).await;
    let task = seed_task(&db, &project_id, None).await;
    let agent = seed_unpinned_claim_agent(&db).await;
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(root.path().to_path_buf());
    db.disk_admission.configure(
        api_types::DiskFloor::of_bytes(100, 0),
        Arc::new(move || {
            Some(api_types::MachineDiskFacts {
                free_bytes: 10,
                total_bytes: 1_000,
                free_inodes: None,
                total_inodes: None,
                measured_at: now_rfc3339(),
                gc_state: Some("unclaimed".to_owned()),
                compiler_cache_bytes: None,
            })
        }),
    );
    let error = service
        .reserve_claim_workspace(&task, Some(&agent), "coder")
        .await
        .err()
        .expect("a new worktree is refused under the floor");
    let now = now_rfc3339();
    sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES('launch',?,'coder','running',?,?)")
            .bind(&task.id)
            .bind(&now)
            .bind(&now)
            .execute(db.pool())
            .await
            .unwrap();

    // Any other launch error is not this wait.
    assert!(
        !service
            .defer_launch_for_disk("launch", &ServiceError::invalid_operation("boom"))
            .await
    );
    assert_eq!(execution_count(&db, &task.id).await, 1);
    assert!(service.defer_launch_for_disk("launch", &error).await);

    let (status, resume_policy, message): (String, Option<String>, Option<String>) =
        sqlx::query_as("SELECT status,resume_policy,error FROM execution WHERE id='launch'")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(status, "failed");
    assert_eq!(resume_policy.as_deref(), Some("auto"), "{message:?}");
    assert!(message.is_some_and(|message| message
        .starts_with(crate::task_service::execution::DISK_PRE_DISPATCH_ERROR_PREFIX)));
    let waiting = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(waiting.blocked_json.is_none(), "{:?}", waiting.blocked_json);
    assert!(
        waiting.error_annotation.is_none(),
        "{:?}",
        waiting.error_annotation
    );
    let disposition = crate::deferred_dispatch::dispatch_disposition(&waiting)
        .expect("the Task takes the disk wait");
    assert_eq!(disposition.capability, "machine_capacity");
    assert_eq!(
        disposition.capacity_scope.as_deref(),
        Some(api_types::CAPACITY_SCOPE_DISK)
    );
}

/// The whole path of a disk refusal with an injected reading: refused for
/// a new worktree, recorded as the machine-capacity wait with scope
/// `disk`, kept by the dispatcher's precheck while the disk is short,
/// cleared by the same precheck once the reading recovers, and never
/// applied to a Task that already has its worktree.
#[tokio::test]
async fn disk_pressure_waits_with_scope_disk_and_clears_when_the_reading_recovers() {
    use std::sync::atomic::{AtomicU64, Ordering};
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let (project_id, _) = seed_project_with_real_repo(&db, repo.path()).await;
    let task = seed_task(&db, &project_id, None).await;
    let agent = seed_unpinned_claim_agent(&db).await;
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(root.path().to_path_buf());
    let free = Arc::new(AtomicU64::new(10));
    let reading = Arc::clone(&free);
    db.disk_admission.configure(
        api_types::DiskFloor::of_bytes(100, 0),
        Arc::new(move || {
            Some(api_types::MachineDiskFacts {
                free_bytes: reading.load(Ordering::SeqCst),
                total_bytes: 1_000,
                free_inodes: None,
                total_inodes: None,
                measured_at: now_rfc3339(),
                gc_state: Some("unclaimed".to_owned()),
                compiler_cache_bytes: None,
            })
        }),
    );
    let reload = || async {
        TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap()
    };

    // Under the floor: no new worktree, and nothing was created.
    let error = service
        .reserve_claim_workspace(&task, Some(&agent), "coder")
        .await
        .err()
        .expect("a new worktree is refused under the floor");
    let ServiceError::PlacementUnavailable(refusal) = &error else {
        panic!("{error:?}");
    };
    assert!(refusal.rejected_candidates.iter().all(|candidate| {
        candidate.filter_codes == [crate::placement::PlacementFilterCode::DiskPressure]
    }));
    assert_eq!(
        crate::placement::capacity_wait_of(&error),
        Some(crate::placement::CapacityWait::Disk)
    );
    assert_eq!(execution_count(&db, &task.id).await, 0);

    // The machine-capacity path, naming the disk.
    assert!(service
        .defer_placement_refusal(&task, &error)
        .await
        .unwrap());
    let waiting = reload().await;
    let disposition = crate::deferred_dispatch::dispatch_disposition(&waiting).unwrap();
    assert_eq!(
        (
            disposition.capability.as_str(),
            disposition.capacity_scope.as_deref()
        ),
        ("machine_capacity", Some("disk"))
    );
    assert!(
        disposition.safe_message.starts_with(
            "disk_pressure: waiting for free space on the workspace filesystem of Server host"
        ),
        "{}",
        disposition.safe_message
    );
    // Nobody collects garbage on this root: the wait says so.
    assert!(disposition
        .safe_message
        .contains("workspace garbage collection is not running on Server host"));
    assert!(
        format!("{:?}", waiting.condition).contains("Disk"),
        "{:?}",
        waiting.condition
    );
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    assert!(project.system_pause_reason.is_none(), "no Project pause");

    // Still short: the dispatcher's precheck keeps the wait.
    assert!(crate::placement::machine_precheck::wait_before_dispatch(
        &db,
        &service,
        &waiting,
        &agent,
        Some("coder")
    )
    .await
    .unwrap());
    assert_eq!(
        crate::deferred_dispatch::dispatch_disposition(&reload().await),
        Some(disposition)
    );

    // The reading recovers. No human action: the next precheck clears
    // the wait and the Task is admitted.
    free.store(500, Ordering::SeqCst);
    db.disk_admission.refresh();
    let waiting = reload().await;
    assert!(!crate::placement::machine_precheck::wait_before_dispatch(
        &db,
        &service,
        &waiting,
        &agent,
        Some("coder")
    )
    .await
    .unwrap());
    let cleared = reload().await;
    assert!(crate::deferred_dispatch::dispatch_disposition(&cleared).is_none());
    assert!(!format!("{:?}", cleared.condition).contains("Disk"));
    let admission = service
        .reserve_claim_workspace(&cleared, Some(&agent), "coder")
        .await
        .expect("admitted once the disk has room");
    let prepared = service.prepare_claim_workspace(admission).await.unwrap();
    assert_eq!(prepared.placement.state, PlacementState::Ready);

    // Short again: a run in the worktree that already exists is admitted.
    free.store(10, Ordering::SeqCst);
    db.disk_admission.refresh();
    let again = service
        .reserve_claim_workspace(&cleared, Some(&agent), "coder")
        .await
        .expect("an existing valid worktree is never refused for disk");
    assert_eq!(again.placement.id, prepared.placement.id);
    let again = service.prepare_claim_workspace(again).await.unwrap();
    // The same Task with its worktree gone: making it again is new disk,
    // refused with the same code; it is made once the disk has room.
    let worktree = PathBuf::from(again.placement.workspace_handle.as_deref().unwrap());
    std::fs::remove_dir_all(&worktree).unwrap();
    let gone = service
        .reserve_claim_workspace(&cleared, Some(&agent), "coder")
        .await
        .expect("the ready placement itself is still admitted");
    let recreate = service
        .prepare_claim_workspace(gone)
        .await
        .err()
        .expect("a recreate is refused under the floor");
    assert_eq!(
        crate::placement::capacity_wait_of(&recreate),
        Some(crate::placement::CapacityWait::Disk),
        "{recreate:?}"
    );
    assert!(!worktree.exists());
    free.store(500, Ordering::SeqCst);
    db.disk_admission.refresh();
    let gone = service
        .reserve_claim_workspace(&cleared, Some(&agent), "coder")
        .await
        .unwrap();
    service.prepare_claim_workspace(gone).await.unwrap();
    assert!(worktree.is_dir());
    free.store(10, Ordering::SeqCst);
    db.disk_admission.refresh();
    // A second Task, which would need a new worktree, still waits.
    let other = seed_task(&db, &project_id, None).await;
    assert!(matches!(
        service
            .reserve_claim_workspace(&other, Some(&agent), "coder")
            .await,
        Err(ServiceError::PlacementUnavailable(_))
    ));
    // Unreadable disk: nothing is refused.
    db.disk_admission
        .configure(api_types::DiskFloor::of_bytes(100, 0), Arc::new(|| None));
    service
        .reserve_claim_workspace(&other, Some(&agent), "coder")
        .await
        .expect("an unreadable disk refuses nothing");
}
