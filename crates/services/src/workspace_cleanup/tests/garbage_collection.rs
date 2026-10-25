//! Garbage collection cases.

use super::*;

#[tokio::test]
async fn gc_does_nothing_on_a_root_no_running_server_adopted() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let orphan = forge_made_root(&root);
    // Built the way every test and tool builds one: nothing adopts.
    let scheduler =
        WorkspaceCleanupScheduler::new(Arc::clone(&db), Arc::new(EventBus::new(16)), root.clone());
    let report = gc_at(&scheduler, 30 * 24 * HOUR).await;
    assert_eq!(report, executors::gc::GcReport::default());
    scheduler.sweep().await.unwrap();
    assert!(orphan.exists());
    // Not even the marker is written.
    assert!(!root.join(".forge").exists());
}

#[tokio::test]
async fn gc_refuses_a_root_that_is_a_repository_and_reports_it() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let root = temp.path().canonicalize().unwrap();
    std::fs::create_dir_all(root.join(".git")).unwrap();
    let orphan = forge_made_root(&root);
    let scheduler =
        WorkspaceCleanupScheduler::new(Arc::clone(&db), Arc::new(EventBus::new(16)), root.clone());
    assert!(matches!(
        scheduler.adopt_workspace_root().await.unwrap(),
        executors::gc::Ownership::Refused(_)
    ));
    assert_eq!(
        gc_at(&scheduler, 30 * 24 * HOUR).await,
        executors::gc::GcReport::default()
    );
    assert!(orphan.exists() && !root.join(".forge").exists());
    // The operator can see why nothing is reclaimed.
    let status = sqlx::query_scalar::<_, String>(
        "SELECT value FROM system_setting WHERE key = 'workspace_gc_status'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert!(status.contains("\"state\":\"refused\"") && status.contains("git repository"));
}

#[tokio::test]
async fn gc_leaves_a_task_root_whose_create_is_in_flight_however_old() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let slow = forge_made_root(&root);
    let scheduler = gc_scheduler(&db, &root).await;
    // A worktree create that has been running for a month.
    let creating = workspace::creating(&slow);
    assert_eq!(
        gc_at(&scheduler, 30 * 24 * HOUR).await,
        executors::gc::GcReport::default()
    );
    assert!(slow.join("repo/file").exists());
    drop(creating);
    assert_eq!(gc_at(&scheduler, 30 * 24 * HOUR).await.quarantined, 1);
}

#[tokio::test]
async fn gc_pages_through_more_roots_than_one_pass_takes() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let total = SWEEP_LIMIT as usize + 6;
    let orphans: Vec<PathBuf> = (0..total).map(|_| forge_made_root(&root)).collect();
    let scheduler = gc_scheduler(&db, &root).await;
    let mut cursor = SweepCursor::default();
    let later = std::time::SystemTime::now() + 25 * HOUR;

    let first = scheduler
        .gc_pass_at(&mut cursor, GC_TEST_BUDGET, later)
        .await;
    assert_eq!(first.quarantined, SWEEP_LIMIT as usize);
    assert!(!cursor.root_name.is_empty());
    let second = scheduler
        .gc_pass_at(&mut cursor, GC_TEST_BUDGET, later)
        .await;
    assert_eq!(second.quarantined, 6);
    assert!(cursor.root_name.is_empty());
    assert!(orphans.iter().all(|orphan| !orphan.exists()));

    // Out of time from the start: a pass still finishes one entry and
    // the cursor moves past it, so no entry can starve the ones after it.
    let more: Vec<PathBuf> = (0..3).map(|_| forge_made_root(&root)).collect();
    let mut names: Vec<String> = more
        .iter()
        .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    names.sort();
    let starved = scheduler
        .gc_pass_at(&mut cursor, Duration::ZERO, later)
        .await;
    assert_eq!(starved.quarantined, 1);
    assert_eq!(cursor.root_name, names[0]);
    let next = scheduler
        .gc_pass_at(&mut cursor, Duration::ZERO, later)
        .await;
    assert_eq!(next.quarantined, 1);
    assert_eq!(cursor.root_name, names[1]);
}

#[tokio::test]
async fn gc_evicts_build_output_under_the_floor_but_never_of_a_busy_task() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let mut builds = Vec::new();
    let mut hook = None;
    for kind in ["idle", "running", "hook", "done"] {
        let (workspace_id, worktree_path) =
            seed_workspace(&db, &root, WorkspaceStatus::Ready).await;
        let task = fixture_task(&db, &workspace_id).await;
        let task =
            set_fixture_status(&db, &task, if kind == "done" { "done" } else { "todo" }).await;
        let task_root = worktree_path.parent().unwrap().to_path_buf();
        executors::sandbox::TaskRoot::reserve(&task_root).unwrap();
        let build = task_root.join(".forge-task/build");
        std::fs::create_dir_all(build.join("cargo")).unwrap();
        std::fs::write(build.join("cargo/marker"), "x").unwrap();
        match kind {
            "running" => {
                fixture_execution(&db, &task, &workspace_id, db::ExecutionStatus::Running).await;
            }
            "hook" => {
                hook = Some(executors::sandbox::SandboxEnv::for_command(
                    &worktree_path,
                    executors::sandbox::RunPurpose::Hook,
                ));
            }
            _ => {}
        }
        builds.push(build);
    }
    let hook = hook.unwrap();
    let scheduler = gc_scheduler(&db, &root).await;
    let under = executors::gc::FreeFloor::of_bytes(u64::MAX, 100);

    // Plenty of room: nothing is evicted.
    scheduler.set_gc_limits(30, executors::gc::FreeFloor::of_bytes(0, 0));
    assert_eq!(gc_at(&scheduler, Duration::ZERO).await.builds_evicted, 0);

    // Under the floor while a check runs somewhere: nothing is evicted.
    scheduler.set_gc_limits(30, under);
    scheduler.set_live_check_counter(Arc::new(|| 1));
    assert_eq!(gc_at(&scheduler, Duration::ZERO).await.builds_evicted, 0);
    assert!(builds
        .iter()
        .all(|build| build.join("cargo/marker").exists()));

    // Under the floor: only the idle, non-terminal Task loses its build.
    scheduler.set_live_check_counter(Arc::new(|| 0));
    let report = gc_at(&scheduler, Duration::ZERO).await;
    assert_eq!((report.builds_evicted, report.errors), (1, 0));
    assert!(!builds[0].exists());
    assert!(builds[1].join("cargo/marker").exists());
    if hook.env().tmp_dir().is_some() {
        assert!(builds[2].join("cargo/marker").exists());
    }
    assert!(builds[3].join("cargo/marker").exists());
}

/// Plan 3.4 F: under the floor the server's collector trims the shared
/// compiler cache, also while a check runs (an sccache store tolerates
/// it), and every pass measures the cache for the server's disk facts.
#[tokio::test]
async fn gc_trims_the_shared_compiler_cache_under_the_floor_and_measures_it() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let store = root
        .join(executors::compiler_cache::CACHE_DIR)
        .join("repo-a");
    std::fs::create_dir_all(store.join("0")).unwrap();
    std::fs::write(
        store.join(executors::compiler_cache::MARKER_FILE),
        "sccache",
    )
    .unwrap();
    let entry = store.join("0").join("entry");
    std::fs::write(&entry, vec![0_u8; 4096]).unwrap();
    let scheduler = gc_scheduler(&db, &root).await;

    scheduler.set_gc_limits(30, executors::gc::FreeFloor::of_bytes(0, 0));
    let report = gc_at(&scheduler, Duration::ZERO).await;
    assert_eq!(report.cache_entries_evicted, 0);
    assert!(entry.exists());
    assert!(executors::compiler_cache::measured_bytes(&root).is_some_and(|bytes| bytes >= 4096));

    scheduler.set_gc_limits(30, executors::gc::FreeFloor::of_bytes(u64::MAX, 100));
    scheduler.set_live_check_counter(Arc::new(|| 1));
    let report = gc_at(&scheduler, Duration::ZERO).await;
    assert_eq!((report.cache_entries_evicted, report.errors), (1, 0));
    assert!(!entry.exists() && store.join("0").is_dir());
}

/// The store of a repository whose row is gone is removed a day after
/// its last run start; a store whose repository still has a row stays.
#[tokio::test]
async fn gc_removes_the_compiler_cache_store_of_a_deleted_repository() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let (workspace_id, worktree) = seed_workspace(&db, &root, WorkspaceStatus::Ready).await;
    let repo_id = WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .unwrap()
        .unwrap()
        .repo_id;
    // What a run in the worktree is given: named after the clone's Git
    // directory, which is not the bare row id.
    let run_store = executors::compiler_cache::repository_id(&worktree).unwrap();
    assert_ne!(run_store, repo_id);
    let cache_dir = root.join(executors::compiler_cache::CACHE_DIR);
    let store = |repository: &str| {
        let store = cache_dir.join(repository);
        std::fs::create_dir_all(store.join("0")).unwrap();
        std::fs::write(store.join(executors::compiler_cache::MARKER_FILE), "kache").unwrap();
        std::fs::write(store.join("0").join("entry"), "x").unwrap();
        store
    };
    let (kept, gone) = (store(&repo_id), store(&new_uuid_v4()));
    // A repository the operator keeps as a checkout: its store is named
    // after the checkout, not after the row.
    let checkout = root.join("operator").join("checkout");
    std::fs::create_dir_all(checkout.join(".git")).unwrap();
    let project_id: String = sqlx::query_scalar("SELECT project_id FROM repo WHERE id = ?")
        .bind(&repo_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    RepoRepo::create(
        &*db,
        CreateRepo {
            id: new_uuid_v4(),
            project_id,
            name: "operator".to_owned(),
            remote_url: None,
            local_path: Some(checkout.to_str().unwrap().to_owned()),
            default_branch: "main".to_owned(),
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let local = store(&executors::compiler_cache::repository_id_of(&checkout).unwrap());
    let in_use = store(&run_store);
    // The same repository with no worktree on record any more.
    let other = seed_workspace(&db, &root, WorkspaceStatus::Cleaned).await;
    let idle = store(&executors::compiler_cache::repository_id(&other.1).unwrap());
    let scheduler = gc_scheduler(&db, &root).await;
    scheduler.set_gc_limits(30, executors::gc::FreeFloor::of_bytes(0, 0));

    let report = gc_at(&scheduler, Duration::from_secs(60 * 60)).await;
    assert_eq!(report.cache_stores_removed, 0, "inside the grace period");
    assert!(gone.exists());
    let report = gc_at(&scheduler, Duration::from_secs(25 * 60 * 60)).await;
    assert_eq!((report.cache_stores_removed, report.errors), (1, 0));
    assert!(!gone.exists() && kept.join("0").join("entry").exists());
    for (store, what) in [
        (&local, "a repository the operator keeps"),
        (&in_use, "the repository of a worktree on record"),
        (&idle, "a repository with a row and no worktree"),
    ] {
        assert!(store.join("0").join("entry").exists(), "{what}");
    }
}

#[tokio::test]
async fn gc_quarantines_unknown_task_roots_then_deletes_them_a_day_later() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let (workspace_id, worktree_path) = seed_workspace(&db, &root, WorkspaceStatus::Ready).await;
    let task = fixture_task(&db, &workspace_id).await;
    let task = set_fixture_status(&db, &task, "todo").await;
    // A create in progress: the Task exists, its workspace row does not yet.
    let creating = seed_log_only_task(&db, &root, &task, new_uuid_v4(), None, "in_progress").await;
    let orphan = forge_made_root(&root);
    // A directory with a Task-shaped name that a user made.
    let users = root.join(new_uuid_v4());
    let kept = [
        root.join(&creating.id).join("repo"),
        root.join(&task.project_id).join("checkout"),
        root.join("main-agents").join("agent"),
        root.join(".forge").join("probes"),
        root.join("notes").join("mine"),
        users.join("photos"),
        orphan.join("repo"),
    ];
    for path in &kept {
        std::fs::create_dir_all(path).unwrap();
        std::fs::write(path.join("file"), "content").unwrap();
    }
    let scheduler = gc_scheduler(&db, &root).await;

    // First sight of a directory made a moment ago: nothing moves.
    assert_eq!(gc_at(&scheduler, Duration::ZERO).await.quarantined, 0);
    assert!(orphan.exists());

    // Eleven hours old: a slow clone may still be filling it.
    assert_eq!(gc_at(&scheduler, 11 * HOUR).await.quarantined, 0);
    assert!(orphan.exists());

    // Older than the creation grace: only the unknown Task root is moved
    // aside, and it is not deleted.
    let report = gc_at(&scheduler, 25 * HOUR).await;
    assert_eq!(
        (report.quarantined, report.removed, report.errors),
        (1, 0, 0)
    );
    assert!(!orphan.exists());
    let held = quarantined(&root);
    assert_eq!(held.len(), 1);
    assert_eq!(
        std::fs::read_to_string(
            root.join(executors::gc::GC_DIR)
                .join(&held[0])
                .join("repo/file")
        )
        .unwrap(),
        "content"
    );
    for path in &kept[..6] {
        assert!(path.join("file").exists(), "{} was touched", path.display());
    }
    assert!(worktree_path.exists());
    assert!(root.join(".repos").exists());

    // Still held 23 hours after it was quarantined; gone after 24.
    assert_eq!(gc_at(&scheduler, 48 * HOUR).await.removed, 0);
    assert_eq!(quarantined(&root).len(), 1);
    assert_eq!(gc_at(&scheduler, 50 * HOUR).await.removed, 1);
    assert!(quarantined(&root).is_empty());
    for path in &kept[..6] {
        assert!(path.join("file").exists(), "{} was touched", path.display());
    }
    assert!(worktree_path.exists());
}

#[tokio::test]
async fn gc_restores_a_quarantined_root_whose_task_appears() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let (workspace_id, _) = seed_workspace(&db, &root, WorkspaceStatus::Ready).await;
    let task = fixture_task(&db, &workspace_id).await;
    let late = forge_made_root(&root);
    let late_id = late.file_name().unwrap().to_string_lossy().into_owned();
    let scheduler = gc_scheduler(&db, &root).await;
    assert_eq!(gc_at(&scheduler, 25 * HOUR).await.quarantined, 1);
    assert!(!late.exists());

    // Its record shows up while it sits in quarantine.
    seed_log_only_task(&db, &root, &task, late_id, None, "in_progress").await;
    let report = gc_at(&scheduler, 30 * 24 * HOUR).await;
    assert_eq!((report.restored, report.removed), (1, 0));
    assert_eq!(
        std::fs::read_to_string(late.join("repo/file")).unwrap(),
        "content"
    );
    assert!(quarantined(&root).is_empty());
}

#[tokio::test]
async fn gc_never_sweeps_a_root_another_database_claimed() {
    let first = sqlite_db().await;
    let second = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let root = temp.path().canonicalize().unwrap();
    // A live Task root of the first server, unknown to the second.
    let (_, worktree_path) = seed_workspace(&first, &root, WorkspaceStatus::Ready).await;
    let owner = gc_scheduler(&first, &root).await;
    assert_eq!(
        gc_at(&owner, HOUR).await,
        executors::gc::GcReport::default()
    );

    // A second server on the same root with another database: adopting
    // changes nothing, and it never sweeps.
    let stranger = WorkspaceCleanupScheduler::new(
        Arc::clone(&second),
        Arc::new(EventBus::new(16)),
        root.clone(),
    );
    assert_eq!(
        stranger.adopt_workspace_root().await.unwrap(),
        executors::gc::Ownership::Other
    );
    let report = gc_at(&stranger, 30 * 24 * HOUR).await;
    assert_eq!(report, executors::gc::GcReport::default());
    stranger.sweep().await.unwrap();
    let status = sqlx::query_scalar::<_, String>(
        "SELECT value FROM system_setting WHERE key = 'workspace_gc_status'",
    )
    .fetch_one(second.pool())
    .await
    .unwrap();
    assert!(status.contains("claimed_by_other"), "{status}");
    // The owner is still the owner.
    assert_eq!(
        owner.adopt_workspace_root().await.unwrap(),
        executors::gc::Ownership::Mine
    );
    assert!(worktree_path.join("node_modules/build-output").exists());
    assert!(quarantined(&root).is_empty());
}

#[tokio::test]
async fn gc_removes_leftovers_of_a_cleaned_terminal_task_only() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let scheduler = gc_scheduler(&db, &root).await;
    let mut roots = Vec::new();
    for _ in 0..2 {
        let (workspace_id, worktree_path) =
            seed_workspace(&db, &root, WorkspaceStatus::Ready).await;
        clean_due(&scheduler, &workspace_id).await;
        let task_root = worktree_path.parent().unwrap().to_path_buf();
        assert!(!task_root.exists());
        // Something wrote into the Task root after it was reclaimed.
        std::fs::create_dir_all(task_root.join(".forge-outbox/late")).unwrap();
        std::fs::write(task_root.join(".forge-outbox/late/result.json"), "{}").unwrap();
        roots.push((workspace_id, task_root));
    }
    // The second Task was reopened: its root is about to be used again.
    let reopened = fixture_task(&db, &roots[1].0).await;
    set_fixture_status(&db, &reopened, "todo").await;

    let report = gc_at(&scheduler, Duration::ZERO).await;
    assert_eq!((report.removed, report.errors), (1, 0));
    assert!(!roots[0].1.exists());
    assert!(roots[1].1.join(".forge-outbox/late/result.json").exists());
}

#[tokio::test]
async fn gc_keeps_broken_copies_seven_days_and_live_run_directories_always() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let (workspace_id, worktree_path) = seed_workspace(&db, &root, WorkspaceStatus::Ready).await;
    let task = fixture_task(&db, &workspace_id).await;
    set_fixture_status(&db, &task, "todo").await;
    let task_root = worktree_path.parent().unwrap().to_path_buf();
    executors::sandbox::TaskRoot::reserve(&task_root).unwrap();
    let millis = chrono::Utc::now().timestamp_millis();
    let broken = task_root.join(format!("repo.broken-{millis}"));
    std::fs::create_dir_all(broken.join("src")).unwrap();
    let hook = executors::sandbox::SandboxEnv::for_command(
        &worktree_path,
        executors::sandbox::RunPurpose::Hook,
    );
    let scheduler = gc_scheduler(&db, &root).await;

    gc_at(&scheduler, 6 * 24 * HOUR).await;
    assert!(broken.exists());
    let report = gc_at(&scheduler, 8 * 24 * HOUR).await;
    assert_eq!((report.removed, report.run_dirs_removed), (1, 0));
    assert!(!broken.exists());
    assert!(worktree_path.join("node_modules/build-output").exists());
    if let Some(tmp) = hook.env().tmp_dir() {
        assert!(tmp.exists(), "a live hook lost its temp directory");
    }
}

#[tokio::test]
async fn gc_measures_live_task_roots() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let (workspace_id, worktree_path) = seed_workspace(&db, &root, WorkspaceStatus::Ready).await;
    std::fs::write(worktree_path.join("blob"), vec![7_u8; 256 * 1024]).unwrap();
    let before = WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((before.disk_bytes, before.disk_measured_at), (None, None));

    gc_at(&gc_scheduler(&db, &root).await, Duration::ZERO).await;

    let measured = WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .unwrap()
        .unwrap();
    assert!(measured.disk_bytes.is_some_and(|bytes| bytes >= 256 * 1024));
    assert!(measured.disk_measured_at.is_some());
    assert_eq!(measured.updated_at, before.updated_at);
}

#[tokio::test]
async fn gc_eviction_takes_the_task_lifecycle_lock_and_reads_the_task_again() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let (workspace_id, worktree_path) = seed_workspace(&db, &root, WorkspaceStatus::Ready).await;
    let task = fixture_task(&db, &workspace_id).await;
    let task = set_fixture_status(&db, &task, "todo").await;
    let task_root = worktree_path.parent().unwrap().to_path_buf();
    executors::sandbox::TaskRoot::reserve(&task_root).unwrap();
    let marker = task_root.join(".forge-task/build/cargo/marker");
    std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
    std::fs::write(&marker, "x").unwrap();
    let scheduler = Arc::new(gc_scheduler(&db, &root).await);
    scheduler.set_gc_limits(30, executors::gc::FreeFloor::of_bytes(u64::MAX, 100));

    // The pass saw an idle Task. Something holds the Task's lifecycle
    // lock; a run starts before the pass gets its turn.
    let guard = scheduler.lock_task(&task).await;
    let pass = tokio::spawn({
        let scheduler = Arc::clone(&scheduler);
        async move { gc_at(&scheduler, Duration::ZERO).await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!pass.is_finished(), "eviction waits for the lifecycle lock");
    assert!(marker.exists());
    let run = fixture_execution(&db, &task, &workspace_id, db::ExecutionStatus::Running).await;
    drop(guard);
    let report = pass.await.unwrap();
    assert_eq!((report.builds_evicted, report.errors), (0, 0));
    assert!(
        marker.exists(),
        "the run that started keeps its build output"
    );

    // Once the run is over the same Task is evicted.
    sqlx::query("UPDATE execution SET status = 'failed' WHERE id = ?")
        .bind(&run.id)
        .execute(db.pool())
        .await
        .unwrap();
    // A lock that stays held (the claim that asked for this very pass,
    // a reopen in flight) never hangs the pass: the Task is skipped, and
    // the pass does not run out its budget waiting.
    let guard = scheduler.lock_task(&task).await;
    let started = std::time::Instant::now();
    let report = tokio::time::timeout(Duration::from_secs(10), gc_at(&scheduler, Duration::ZERO))
        .await
        .expect("a held lifecycle lock does not hang the pass");
    assert_eq!((report.builds_evicted, report.errors), (0, 0));
    assert!(started.elapsed() >= GC_LOCK_WAIT);
    assert!(marker.exists());
    drop(guard);

    let report = gc_at(&scheduler, Duration::ZERO).await;
    assert_eq!((report.builds_evicted, report.errors), (1, 0));
    assert!(!marker.exists());
    assert!(worktree_path.exists(), "only the build output goes");
}

/// A burst of admissions on a short disk runs one collection, and the
/// rest neither run their own nor wait behind it.
#[tokio::test]
async fn a_burst_of_admissions_under_the_floor_runs_one_reclaim_and_nobody_queues() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let scheduler = Arc::new(gc_scheduler(&db, &root).await);
    let free = Arc::new(std::sync::atomic::AtomicU64::new(150));
    let reading = Arc::clone(&free);
    db.disk_admission.configure(
        api_types::DiskFloor::of_bytes(100, 0),
        Arc::new(move || {
            Some(api_types::MachineDiskFacts {
                free_bytes: reading.load(std::sync::atomic::Ordering::SeqCst),
                total_bytes: 1_000,
                free_inodes: None,
                total_inodes: None,
                measured_at: now_rfc3339(),
                gc_state: Some("owned".to_owned()),
                compiler_cache_bytes: None,
            })
        }),
    );
    // Over the floor but under the collector mark: an admission would
    // not be refused, so it does not collect (the cleanup tick does).
    assert!(!scheduler.reclaim_before_refusing().await);
    // Under the floor: fifty admissions at once.
    free.store(10, std::sync::atomic::Ordering::SeqCst);
    db.disk_admission.refresh();
    let started = std::time::Instant::now();
    let mut burst = tokio::task::JoinSet::new();
    for _ in 0..50 {
        let scheduler = Arc::clone(&scheduler);
        burst.spawn(async move { scheduler.reclaim_before_refusing().await });
    }
    let mut ran = 0;
    while let Some(result) = burst.join_next().await {
        ran += usize::from(result.unwrap());
    }
    assert_eq!(ran, 1, "one pass for the whole burst");
    assert!(started.elapsed() < Duration::from_secs(15));
    // The same minute: nothing more, from an admission or from the tick.
    assert!(!scheduler.reclaim_before_refusing().await);
    assert!(!scheduler.reclaim_under_pressure().await);
}

/// Before the server is counted as short of disk its garbage is
/// collected once and its disk read again: reclaimable trash present
/// means the machine is admitted after the pass.
#[tokio::test]
async fn disk_pressure_collects_garbage_once_then_reads_the_disk_again() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let scheduler = gc_scheduler(&db, &root).await;
    let trash = root.join(executors::gc::TRASH_DIR);
    std::fs::create_dir_all(trash.join("condemned-1-0/target")).unwrap();
    std::fs::write(trash.join("condemned-1-0/target/big"), "x").unwrap();
    // The injected disk: short exactly while the trash holds something.
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (counted, watched) = (Arc::clone(&reads), trash.clone());
    db.disk_admission.configure(
        api_types::DiskFloor::of_bytes(100, 0),
        Arc::new(move || {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let full =
                std::fs::read_dir(&watched).is_ok_and(|mut entries| entries.next().is_some());
            Some(api_types::MachineDiskFacts {
                free_bytes: if full { 10 } else { 900 },
                total_bytes: 1_000,
                free_inodes: None,
                total_inodes: None,
                measured_at: now_rfc3339(),
                gc_state: Some("owned".to_owned()),
                compiler_cache_bytes: None,
            })
        }),
    );
    assert_eq!(
        db.disk_admission.server_pressure(),
        Some(api_types::DiskPressureKind::Bytes)
    );
    assert!(scheduler.reclaim_under_pressure().await, "a pass ran");
    assert!(!trash.join("condemned-1-0").exists());
    assert_eq!(
        db.disk_admission.server_pressure(),
        None,
        "read again after the pass"
    );
    // Asked again at once: no second pass, whatever the disk says.
    std::fs::create_dir_all(trash.join("condemned-2-0")).unwrap();
    db.disk_admission.refresh();
    assert!(!scheduler.reclaim_under_pressure().await);
    assert!(trash.join("condemned-2-0").exists());
    // With room to spare nothing is collected ahead of the timer.
    let unconfigured = sqlite_db().await;
    let idle = gc_scheduler(&unconfigured, &root).await;
    assert!(!idle.reclaim_under_pressure().await);
    assert!(reads.load(std::sync::atomic::Ordering::SeqCst) >= 3);
}

#[tokio::test]
async fn deleted_task_is_reclaimed_like_a_terminal_one_once_its_run_has_stopped() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let (workspace_id, worktree_path) =
        seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let task = fixture_task(&db, &workspace_id).await;
    let task = set_fixture_status(&db, &task, "todo").await;
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );
    let status = || async {
        WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap()
            .status
    };

    // Not terminal and not deleted: untouched.
    scheduler.cleanup_terminal_task(&task.id).await.unwrap();
    assert!(worktree_path.exists());
    assert_eq!(status().await, WorkspaceStatus::Ready);

    // Deleting stops nothing by itself: a run still in flight keeps the
    // directory until it has stopped.
    let run = fixture_execution(&db, &task, &workspace_id, db::ExecutionStatus::Running).await;
    let now = now_rfc3339();
    TaskRepo::soft_delete(
        &*db,
        db::SoftDeleteTask {
            id: task.id.clone(),
            expected_version: task.version,
            deleted_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("Task is deleted");
    scheduler.cleanup_terminal_task(&task.id).await.unwrap();
    assert!(worktree_path.exists(), "a running execution defers cleanup");
    assert_eq!(status().await, WorkspaceStatus::Ready);

    // The run stops; a lease on the workspace still holds it.
    sqlx::query("UPDATE execution SET status = 'cancelled' WHERE id = ?")
        .bind(&run.id)
        .execute(db.pool())
        .await
        .unwrap();
    let due = || async {
        sqlx::query("UPDATE workspace SET cleanup_after = ? WHERE id = ?")
            .bind(now_rfc3339())
            .bind(&workspace_id)
            .execute(db.pool())
            .await
            .unwrap();
    };
    // A hook, check or tool command of this server that is live in the
    // Task root (in no table at all) keeps it as well, with or without
    // a temp directory of its own.
    let live = executors::sandbox::SandboxEnv::for_run(
        &worktree_path,
        "deleted-task-hook",
        executors::sandbox::RunPurpose::Hook,
    )
    .without_tmp()
    .prepared();
    due().await;
    scheduler.tick().await.unwrap();
    assert!(worktree_path.exists(), "a live run defers cleanup");
    assert_eq!(status().await, WorkspaceStatus::Ready);
    assert!(
        WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap()
            .cleanup_after
            .is_some(),
        "and the cleanup comes due again"
    );
    live.settle();

    // Everything has stopped; the deferred cleanup comes due.
    due().await;
    scheduler.tick().await.unwrap();
    assert!(!worktree_path.exists());
    assert_eq!(status().await, WorkspaceStatus::Cleaned);
}

#[cfg(unix)]
#[tokio::test]
async fn gc_removes_only_legacy_temp_locations_that_are_provably_its_own_and_long_untouched() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let (root, legacy, outside) = (base.join("root"), base.join("tmp"), base.join("outside"));
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(outside.join("user")).unwrap();
    // Two Tasks of this database; the third id belongs to another Forge
    // on the same machine.
    let (workspace_id, _) = seed_workspace(&db, &root, WorkspaceStatus::Cleaned).await;
    let mine = fixture_task(&db, &workspace_id).await;
    let (workspace_id, _) = seed_workspace(&db, &root, WorkspaceStatus::Cleaned).await;
    let linked = fixture_task(&db, &workspace_id).await;
    let (old_task, other_task, linked_task) = (mine.id.clone(), new_uuid_v4(), linked.id.clone());
    let logs = legacy.join("forge/logs");
    for path in [
        legacy.join("forge-gemini-api-key-home/.gemini"),
        logs.join(&old_task).join("hooks"),
        logs.join(&other_task).join("hooks"),
        logs.join("not-a-task").join("hooks"),
        legacy.join("forge/worktrees/someone"),
        legacy.join("unrelated"),
    ] {
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("file"), "content").unwrap();
    }
    // A planted link where a hook-log directory used to be.
    std::fs::create_dir_all(logs.join(&linked_task)).unwrap();
    std::os::unix::fs::symlink(&outside, logs.join(&linked_task).join("hooks")).unwrap();
    let scheduler = gc_scheduler(&db, &root).await;

    // Not told about a temp directory: nothing outside the root is read.
    gc_at(&scheduler, 30 * 24 * HOUR).await;
    assert!(legacy.join("forge-gemini-api-key-home").exists());

    // Touched within a week: another Forge on the machine may be using
    // it, so everything stays.
    scheduler.set_legacy_temp_dir(legacy.clone());
    gc_at(&scheduler, 6 * 24 * HOUR).await;
    assert!(legacy
        .join("forge-gemini-api-key-home/.gemini/file")
        .exists());
    assert!(logs.join(&old_task).join("hooks/file").exists());

    gc_at(&scheduler, 8 * 24 * HOUR).await;
    assert!(!legacy.join("forge-gemini-api-key-home").exists());
    assert!(!logs.join(&old_task).exists());
    // Not a Task of this database: not ours.
    assert!(logs.join(&other_task).join("hooks/file").exists());
    assert!(logs.join("not-a-task/hooks/file").exists());
    assert!(legacy.join("forge/worktrees/someone/file").exists());
    assert!(legacy.join("unrelated/file").exists());
    assert!(!logs.join(&linked_task).exists());
    assert!(outside.join("user").exists());
}
