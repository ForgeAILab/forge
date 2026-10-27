//! Tests for repository readiness in the task dispatcher.

use super::*;
#[tokio::test]
async fn dispatcher_pauses_a_project_with_no_primary_repository() {
    let db = Arc::new(sqlite_db().await);
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let project_id = seed_unprovisioned_project(&db, "Unprovisioned").await;
    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    assert!(project.paused_at.is_some());
    assert_eq!(
        project.system_pause_reason.as_deref(),
        Some(super::repo_pause_sync::MISSING_REPOSITORY)
    );
}

#[tokio::test]
async fn dispatcher_pauses_a_project_with_cross_project_primary_repository() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let project_id = seed_unprovisioned_project(&db, "Invalid repository owner").await;
    let (_other_project_id, other_repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(other_repo_id)),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        project.version,
        None,
    )
    .await
    .expect("cross-Project pointer stores for legacy-corruption fixture");
    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    assert_eq!(
        dispatcher
            .check_once_and_drain()
            .await
            .expect("dispatcher runs"),
        0
    );

    let paused = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project reloads")
        .expect("project exists");
    assert!(paused.paused_at.is_some());
    assert_eq!(
        paused.system_pause_reason.as_deref(),
        Some(super::repo_pause_sync::INVALID_REPOSITORY)
    );
}

#[tokio::test]
async fn dispatcher_dispatches_pre_repository_task_after_primary_repo_is_attached() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let project_id = seed_unprovisioned_project(&db, "Unprovisioned").await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "created before repository", "todo", 1).await;
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
    let paused = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    assert!(paused.paused_at.is_some());

    // Provisioning succeeds later and attaches the repository, the way a
    // retried scaffold does.
    setup_git_repo(repo_dir.path());
    let repo_id = new_uuid_v4();
    RepoRepo::create(
        &*db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "forge".to_owned(),
            remote_url: Some(repo_dir.path().to_string_lossy().into_owned()),
            local_path: Some(repo_dir.path().to_string_lossy().into_owned()),
            default_branch: "main".to_owned(),
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(&*db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project repository attaches");

    // The first scan after attachment only clears the system-owned pause so
    // this check never dispatches from the stale in-memory Project snapshot.
    assert_eq!(
        dispatcher
            .check_once_and_drain()
            .await
            .expect("dispatcher runs"),
        0
    );

    let resumed = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    assert!(resumed.paused_at.is_none());
    assert!(resumed.system_pause_reason.is_none());

    let (first_scan, second_scan) = tokio::join!(
        dispatcher.check_once_and_drain(),
        dispatcher.check_once_and_drain()
    );
    assert_eq!(
        first_scan.expect("first concurrent dispatcher scan runs")
            + second_scan.expect("second concurrent dispatcher scan runs"),
        1,
        "concurrent scans must accept exactly one dispatch"
    );
    let execution_ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, task.id);

    let dispatched_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("Task reloads")
        .expect("Task exists");
    assert_eq!(
        dispatched_task.status,
        crate::workflow::default_states::IN_PROGRESS
    );
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER,
        )
        .await
        .expect("execution count loads"),
        1
    );
    let workspace = WorkspaceRepo::get_by_task_id(&*db, &task.id)
        .await
        .expect("Workspace loads")
        .expect("Workspace exists");
    assert_eq!(workspace.repo_id, repo_id);
    let lease = WorkspaceLeaseRepo::get_active_for_task(&*db, &task.id)
        .await
        .expect("Workspace lease loads")
        .expect("Workspace lease exists");
    assert_eq!(lease.repository_binding_id, workspace.repo_id);

    assert_eq!(
        dispatcher
            .check_once_and_drain()
            .await
            .expect("dispatcher runs"),
        0,
        "an already-running Task must not be dispatched twice"
    );
    assert!(rx.try_recv().is_err());
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER,
        )
        .await
        .expect("execution count reloads"),
        1
    );
}

#[tokio::test]
async fn dispatcher_leaves_a_deliberately_paused_project_alone() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let project_id = seed_unprovisioned_project(&db, "Manually paused").await;
    // A user pause carries no system reason, exactly like the real
    // `POST /projects/{id}/pause` route.
    let paused_at = now_rfc3339();
    ProjectRepo::set_paused_at(&*db, &project_id, Some(paused_at.clone()))
        .await
        .expect("manual pause sets paused_at");
    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    let still_paused = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    assert_eq!(still_paused.paused_at.as_deref(), Some(paused_at.as_str()));
    assert!(still_paused.system_pause_reason.is_none());

    // Attaching a repository must not auto-resume a pause the dispatcher
    // never issued.
    let default_branch = setup_git_repo(repo_dir.path());
    let repo_id = new_uuid_v4();
    RepoRepo::create(
        &*db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "manual-pause-repo".to_owned(),
            remote_url: Some(repo_dir.path().to_string_lossy().into_owned()),
            local_path: Some(repo_dir.path().to_string_lossy().into_owned()),
            default_branch,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("same-Project Repo creates");
    ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id)),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(&*db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project repository attaches");

    dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    let untouched = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    assert_eq!(untouched.paused_at.as_deref(), Some(paused_at.as_str()));
}

#[tokio::test]
async fn stale_repository_resume_cannot_clear_a_later_manual_pause() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let project_id = seed_unprovisioned_project(&db, "stale repository resume").await;
    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher pauses project");
    let auto_paused = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("auto-paused Project loads")
        .expect("Project exists");
    let auto_pause_reason = auto_paused
        .system_pause_reason
        .clone()
        .expect("dispatcher records its pause reason");
    let auto_pause_at = auto_paused
        .paused_at
        .clone()
        .expect("dispatcher records its pause timestamp");

    // Attaching a repository makes the stale snapshot eligible for an
    // automatic resume, but deliberately keep that snapshot in hand while a
    // user pause wins the race.
    let default_branch = setup_git_repo(repo_dir.path());
    let repo_id = new_uuid_v4();
    RepoRepo::create(
        &*db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "stale-resume-repo".to_owned(),
            remote_url: Some(repo_dir.path().to_string_lossy().into_owned()),
            local_path: Some(repo_dir.path().to_string_lossy().into_owned()),
            default_branch,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("repository creates");
    let attached = ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id)),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        auto_paused.version,
        None,
    )
    .await
    .expect("repository attaches");
    assert_eq!(attached.version, auto_paused.version + 1);
    assert_eq!(attached.paused_at.as_deref(), Some(auto_pause_at.as_str()));
    assert_eq!(
        attached.system_pause_reason.as_deref(),
        Some(auto_pause_reason.as_str())
    );

    let manual_pause_at = "2099-01-01T00:00:00Z".to_owned();
    ProjectRepo::set_paused_at(&*db, &project_id, Some(manual_pause_at.clone()))
        .await
        .expect("manual pause wins race");

    // The dispatcher still has the exact post-attachment snapshot, but its
    // CAS must reject the now-stale system pause rather than clearing the
    // user's pause.
    assert!(!dispatcher
        .sync_repository_pause(&attached)
        .await
        .expect("stale auto-resume is benign"));
    let current = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("Project reloads")
        .expect("Project exists");
    assert_eq!(current.paused_at.as_deref(), Some(manual_pause_at.as_str()));
    assert!(current.system_pause_reason.is_none());
    assert_eq!(current.version, attached.version + 1);
}

#[tokio::test]
async fn dispatcher_holds_a_project_with_an_unborn_repository_until_its_first_commit() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let project_id = seed_unprovisioned_project(&db, "Unborn repository").await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(
        &db,
        &project_id,
        "created before the first commit",
        "todo",
        1,
    )
    .await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;

    // A scaffold links its Repo before `main` has a commit.
    run_git(repo_dir.path(), &["init", "--initial-branch=main"]);
    run_git(
        repo_dir.path(),
        &["symbolic-ref", "HEAD", "refs/heads/main"],
    );
    run_git(repo_dir.path(), &["config", "user.email", "test@forge.dev"]);
    run_git(repo_dir.path(), &["config", "user.name", "Forge Test"]);
    let repo_id = new_uuid_v4();
    RepoRepo::create(
        &*db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "unborn".to_owned(),
            remote_url: Some(repo_dir.path().to_string_lossy().into_owned()),
            local_path: Some(repo_dir.path().to_string_lossy().into_owned()),
            default_branch: "main".to_owned(),
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("repo creates");
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id)),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        project.version,
        None,
    )
    .await
    .expect("repository attaches");
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    assert_eq!(
        dispatcher
            .check_once_and_drain()
            .await
            .expect("dispatcher runs"),
        0
    );
    let paused = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    assert!(paused.paused_at.is_some());
    assert_eq!(
        paused.system_pause_reason.as_deref(),
        Some(super::repo_pause_sync::REPOSITORY_NOT_READY)
    );
    // Held at the Project, not parked on a setup refusal nothing would wake.
    assert_eq!(
        dispatcher
            .check_once_and_drain()
            .await
            .expect("dispatcher runs"),
        0
    );
    let held = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("Task reloads")
        .expect("Task exists");
    assert!(deferred_dispatch::dispatch_disposition_for_test(&held).is_none());

    // The first commit lands outside Forge.
    std::fs::write(repo_dir.path().join("README.md"), "# Forge\n").expect("README writes");
    run_git(repo_dir.path(), &["add", "-A"]);
    run_git(repo_dir.path(), &["commit", "-m", "initial commit"]);

    assert_eq!(
        dispatcher
            .check_once_and_drain()
            .await
            .expect("dispatcher runs"),
        0
    );
    let resumed = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    assert!(resumed.paused_at.is_none());
    assert!(resumed.system_pause_reason.is_none());

    assert_eq!(
        dispatcher
            .check_once_and_drain()
            .await
            .expect("dispatcher runs"),
        1
    );
    let execution_ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, task.id);
}

#[tokio::test]
async fn stale_repository_pause_cannot_pause_after_repository_attachment() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let project_id = seed_unprovisioned_project(&db, "stale repository pause").await;
    let stale_active = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("Project loads")
        .expect("Project exists");
    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let default_branch = setup_git_repo(repo_dir.path());
    let repo_id = new_uuid_v4();
    RepoRepo::create(
        &*db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "stale-pause-repo".to_owned(),
            remote_url: Some(repo_dir.path().to_string_lossy().into_owned()),
            local_path: Some(repo_dir.path().to_string_lossy().into_owned()),
            default_branch,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("repository creates");
    ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id)),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        stale_active.version,
        None,
    )
    .await
    .expect("repository attaches");

    // This is the old scanner snapshot: it observed no primary repository,
    // but the repository authority changed before its write boundary.
    assert!(!dispatcher
        .sync_repository_pause(&stale_active)
        .await
        .expect("stale auto-pause is benign"));
    let current = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("Project reloads")
        .expect("Project exists");
    assert!(current.paused_at.is_none());
    assert!(current.system_pause_reason.is_none());
}
