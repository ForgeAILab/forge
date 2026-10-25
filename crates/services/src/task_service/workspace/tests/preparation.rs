//! Preparation cases.

use super::*;

#[tokio::test]
async fn placement_child_uses_effective_coder_and_keeps_root_workspace_roles() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let (project_id, _) = seed_project_with_real_repo(&db, repo.path()).await;
    let parent = seed_task(&db, &project_id, None).await;
    let child = seed_task(&db, &project_id, Some(parent.id.clone())).await;
    let coder = seed_unpinned_claim_agent(&db).await;
    let reviewer = seed_unpinned_claim_agent(&db).await;
    // The root reviewer will use this same workspace after its children.
    sqlx::query("UPDATE agent_identity SET paused = 1 WHERE id = ?")
        .bind(&reviewer.id)
        .execute(db.pool())
        .await
        .unwrap();
    for (role, agent) in [("coder", &coder), ("reviewer", &reviewer)] {
        TaskRoleAssignmentRepo::assign(
            &*db,
            db::CreateTaskRoleAssignment {
                id: new_uuid_v4(),
                task_id: parent.id.clone(),
                role_name: role.into(),
                assignee_type: Some(AssigneeKind::Agent),
                assignee_id: Some(agent.id.clone()),
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
    }
    let effective = crate::task_hierarchy::effective_coder_assignment(&db, &child)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        effective.assignment.assignee_id.as_deref(),
        Some(coder.id.as_str())
    );
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(root.path().to_path_buf());
    assert!(service
        .reserve_claim_workspace(&child, Some(&coder), "coder")
        .await
        .is_err());
    sqlx::query("UPDATE agent_identity SET paused = 0 WHERE id = ?")
        .bind(&reviewer.id)
        .execute(db.pool())
        .await
        .unwrap();
    let admission = service
        .reserve_claim_workspace(&child, Some(&coder), "coder")
        .await
        .unwrap();
    assert_eq!(
        admission.placement.agent_id.as_deref(),
        Some(coder.id.as_str())
    );
    assert!(TaskRoleAssignmentRepo::list_by_task(&*db, &child.id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn placement_claim_recreates_deleted_ready_worktree() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let (project_id, _) = seed_project_with_real_repo(&db, repo.path()).await;
    let task = seed_task(&db, &project_id, None).await;
    let agent = seed_unpinned_claim_agent(&db).await;
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(root.path().to_path_buf());
    let admission = service
        .reserve_claim_workspace(&task, Some(&agent), "coder")
        .await
        .unwrap();
    let prepared = service.prepare_claim_workspace(admission).await.unwrap();
    let original = prepared.placement.clone();
    let sha = git::get_current_sha(Path::new(original.workspace_handle.as_deref().unwrap()))
        .await
        .unwrap();
    std::fs::remove_dir_all(original.workspace_handle.as_deref().unwrap()).unwrap();
    let admission = service
        .reserve_claim_workspace(&task, Some(&agent), "coder")
        .await
        .unwrap();
    let recovered = service.prepare_claim_workspace(admission).await.unwrap();
    assert_eq!(recovered.workspace.id, prepared.workspace.id);
    assert_eq!(recovered.workspace.branch, prepared.workspace.branch);
    assert_eq!(recovered.placement.generation, original.generation + 1);
    assert_eq!(
        git::get_current_sha(Path::new(
            recovered.placement.workspace_handle.as_deref().unwrap()
        ))
        .await
        .unwrap(),
        sha
    );
}

/// The claim is where a server-owned Task root becomes Forge-shaped:
/// `.forge-task` is reserved beside the worktree, outside Git's view. A
/// recorded path that is not `<root>/<task_id>/<name>` is never reserved.
#[tokio::test]
async fn placement_claim_reserves_the_task_root_and_leaves_a_legacy_path_alone() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let (project_id, _) = seed_project_with_real_repo(&db, repo.path()).await;
    let task = seed_task(&db, &project_id, None).await;
    let agent = seed_unpinned_claim_agent(&db).await;
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(root.path().to_path_buf());
    let admission = service
        .reserve_claim_workspace(&task, Some(&agent), "coder")
        .await
        .unwrap();
    let prepared = service.prepare_claim_workspace(admission).await.unwrap();
    let worktree = PathBuf::from(prepared.placement.workspace_handle.as_deref().unwrap());
    let task_root = root.path().join(&task.id);
    assert_eq!(worktree.parent(), Some(task_root.as_path()));
    assert!(task_root.join(".forge-task").is_dir());
    assert!(executors::sandbox::TaskRoot::of_worktree(&worktree).is_some());
    let status = std::process::Command::new("git")
        .args(["status", "--porcelain", "--ignored"])
        .current_dir(&worktree)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .output()
        .unwrap();
    assert!(status.status.success() && status.stdout.is_empty());

    let legacy_project = seed_project_repo(&db).await.0;
    let legacy_task = seed_task(&db, &legacy_project, None).await;
    let legacy = TempDir::new().unwrap();
    let legacy_worktree = legacy.path().join("projects").join("repo");
    std::fs::create_dir_all(&legacy_worktree).unwrap();
    seed_workspace(&db, &legacy_task, WorkspaceStatus::Ready, &legacy_worktree).await;
    let _ = prepare_workspace_for_test(&db, root.path(), &legacy_task, &legacy_task.id, None).await;
    assert!(!legacy.path().join("projects/.forge-task").exists());
    assert!(executors::sandbox::TaskRoot::of_worktree(&legacy_worktree).is_none());
}

/// The claim is the launch guard every executor family shares (a CLI
/// executor has no later check of its own): a worktree left off the
/// Task branch is put back on it before anything is started there, and
/// no commit is lost.
#[tokio::test]
async fn placement_claim_returns_an_off_branch_worktree_to_the_task_branch() {
    fn git_in(cwd: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(cwd)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .env("GIT_AUTHOR_NAME", "Forge Test")
            .env("GIT_AUTHOR_EMAIL", "forge@example.com")
            .env("GIT_COMMITTER_NAME", "Forge Test")
            .env("GIT_COMMITTER_EMAIL", "forge@example.com")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    }

    for diverged in [false, true] {
        let db = Arc::new(sqlite_db().await);
        let repo = TempDir::new().unwrap();
        let root = TempDir::new().unwrap();
        let (project_id, _) = seed_project_with_real_repo(&db, repo.path()).await;
        let task = seed_task(&db, &project_id, None).await;
        let agent = seed_unpinned_claim_agent(&db).await;
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_workspace_root(root.path().to_path_buf());
        let admission = service
            .reserve_claim_workspace(&task, Some(&agent), "coder")
            .await
            .unwrap();
        let prepared = service.prepare_claim_workspace(admission).await.unwrap();
        let path = PathBuf::from(prepared.placement.workspace_handle.clone().unwrap());
        let branch = prepared.workspace.branch.clone();
        let task_ref = format!("refs/heads/{branch}");
        let base = git_in(&path, &["rev-parse", "HEAD"]);
        if diverged {
            git_in(&path, &["commit", "-q", "--allow-empty", "-m", "task work"]);
        }
        let branch_tip = git_in(&path, &["rev-parse", "HEAD"]);
        git_in(&path, &["checkout", "-q", "--detach", &base]);
        git_in(&path, &["commit", "-q", "--allow-empty", "-m", "stray"]);
        let stray = git_in(&path, &["rev-parse", "HEAD"]);
        let before = TaskRepo::get_by_id(&*db, &task.id, false).await.unwrap();

        let admission = service
            .reserve_claim_workspace(&task, Some(&agent), "coder")
            .await
            .unwrap();
        let claimed = service.prepare_claim_workspace(admission).await.unwrap();

        assert_eq!(claimed.workspace.id, prepared.workspace.id);
        assert_eq!(claimed.placement.generation, prepared.placement.generation);
        assert_eq!(
            git_in(&path, &["rev-parse", "--symbolic-full-name", "HEAD"]),
            task_ref
        );
        let rescued = git_in(
            &path,
            &[
                "for-each-ref",
                "--format=%(objectname)",
                "refs/forge/rescued",
            ],
        );
        if diverged {
            assert_eq!(git_in(&path, &["rev-parse", "HEAD"]), branch_tip);
            assert_eq!(rescued, stray, "the stray commit stays reachable");
        } else {
            assert_eq!(git_in(&path, &["rev-parse", "HEAD"]), stray);
            assert_eq!(rescued, "", "a fast-forward needs no rescue");
        }
        assert_eq!(
            TaskRepo::get_by_id(&*db, &task.id, false).await.unwrap(),
            before,
            "the repair spends no budget and changes no Task row"
        );
    }
}

#[tokio::test]
async fn placement_claim_recreates_invalid_ready_worktree() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let (project_id, _) = seed_project_with_real_repo(&db, repo.path()).await;
    let task = seed_task(&db, &project_id, None).await;
    let agent = seed_unpinned_claim_agent(&db).await;
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(root.path().to_path_buf());
    let admission = service
        .reserve_claim_workspace(&task, Some(&agent), "coder")
        .await
        .unwrap();
    let prepared = service.prepare_claim_workspace(admission).await.unwrap();
    let original = prepared.placement.clone();
    let sha = git::get_current_sha(Path::new(original.workspace_handle.as_deref().unwrap()))
        .await
        .unwrap();
    std::fs::remove_file(Path::new(original.workspace_handle.as_deref().unwrap()).join(".git"))
        .unwrap();
    let admission = service
        .reserve_claim_workspace(&task, Some(&agent), "coder")
        .await
        .unwrap();
    let recovered = service.prepare_claim_workspace(admission).await.unwrap();
    assert_eq!(recovered.workspace.id, prepared.workspace.id);
    assert_eq!(recovered.workspace.branch, prepared.workspace.branch);
    assert_eq!(recovered.placement.generation, original.generation + 1);
    assert_eq!(
        git::get_current_sha(Path::new(
            recovered.placement.workspace_handle.as_deref().unwrap()
        ))
        .await
        .unwrap(),
        sha
    );
}

#[tokio::test]
async fn prepared_workspace_has_one_server_placement() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let (project_id, repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    let task = seed_task(&db, &project_id, None).await;
    let workspace_root = TempDir::new().expect("workspace root creates");

    let workspace = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
        .await
        .expect("workspace prepares");
    let placement = WorkspacePlacementRepo::get_by_workspace_id(&*db, &workspace.id)
        .await
        .expect("placement loads")
        .expect("workspace has a placement");
    assert_eq!(placement.owner_kind, PlacementOwnerKind::Server);
    assert_eq!(placement.state, PlacementState::Ready);
    assert_eq!(placement.selected_by, PlacementSelectedBy::Scheduler);
    assert_eq!(
        placement.workspace_handle.as_deref(),
        Some(workspace.embedded_worktree_path_for_backend())
    );
    assert_eq!(
        serde_json::from_str::<Value>(&placement.selection_reason).unwrap(),
        json!({ "rule": "server_default" })
    );
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM workspace_placement WHERE workspace_id = ?")
            .bind(&workspace.id)
            .fetch_one(db.pool())
            .await
            .expect("placement count loads");
    assert_eq!(count, 1);
    let location = db::RepoLocationRepo::get_by_id(&*db, &placement.repo_location_id)
        .await
        .expect("location loads")
        .expect("placement location exists");
    assert_eq!(location.repo_id, repo_id);
    assert_eq!(location.owner_kind, db::RepoLocationOwnerKind::Server);
    assert_eq!(location.kind, RepoLocationKind::PrimaryCheckout);
    assert_eq!(location.status, db::RepoLocationStatus::Ready);
    assert_eq!(location.path, repo_dir.path().to_string_lossy());

    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(16)))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let router = service.workspace_backend_router();
    assert!(Arc::ptr_eq(
        &router,
        &service.clone().workspace_backend_router()
    ));
    let resolved = router
        .resolve(&db, &workspace)
        .await
        .expect("workspace resolves");
    assert_eq!(resolved.placement, placement);
    assert_eq!(
        resolved.handle().unwrap(),
        workspace.embedded_worktree_path_for_backend()
    );
    assert_eq!(
        resolved.embedded_path().unwrap(),
        PathBuf::from(workspace.embedded_worktree_path_for_backend())
    );
    assert_eq!(
        router.embedded_path(&db, &workspace).await.unwrap(),
        PathBuf::from(workspace.embedded_worktree_path_for_backend())
    );

    let reused = prepare_workspace_for_test(&db, workspace_root.path(), &task, &task.id, None)
        .await
        .expect("workspace reuses");
    assert_eq!(reused.id, workspace.id);
    assert_eq!(
        WorkspacePlacementRepo::get_by_workspace_id(&*db, &reused.id)
            .await
            .unwrap()
            .unwrap(),
        placement
    );
}

#[tokio::test]
async fn remote_repository_registers_managed_clone_once() {
    let db = sqlite_db().await;
    let repo_dir = TempDir::new().expect("repo dir creates");
    let (project_id, repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    RepoRepo::update(
        &db,
        db::UpdateRepo {
            id: repo_id.clone(),
            name: None,
            local_path: Some(None),
            remote_url: None,
            default_branch: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("repository becomes remote-only");
    let first = seed_task(&db, &project_id, None).await;
    let second = seed_task(&db, &project_id, None).await;
    let workspace_root = TempDir::new().expect("workspace root creates");
    let locks = Arc::new(RepoCacheLockManager::default());
    let (first_workspace, second_workspace) = tokio::join!(
        prepare_workspace_for_test(
            &db,
            workspace_root.path(),
            &first,
            &first.id,
            Some(Arc::clone(&locks))
        ),
        prepare_workspace_for_test(
            &db,
            workspace_root.path(),
            &second,
            &second.id,
            Some(Arc::clone(&locks))
        ),
    );
    let first_workspace = first_workspace.expect("first workspace prepares");
    let second_workspace = second_workspace.expect("second workspace prepares");
    let first_placement = WorkspacePlacementRepo::get_by_workspace_id(&db, &first_workspace.id)
        .await
        .unwrap()
        .unwrap();
    let second_placement = WorkspacePlacementRepo::get_by_workspace_id(&db, &second_workspace.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        first_placement.repo_location_id,
        second_placement.repo_location_id
    );
    let locations = db::RepoLocationRepo::list_by_repo(
        &db,
        &repo_id,
        db::PageRequest {
            cursor: None,
            limit: 20,
            include_total: false,
            sort_by: db::SortBy::CreatedAt,
            sort_order: db::SortOrder::Asc,
        },
    )
    .await
    .expect("locations load");
    assert_eq!(locations.items.len(), 1);
    let location = &locations.items[0];
    assert_eq!(location.owner_kind, db::RepoLocationOwnerKind::Server);
    assert_eq!(location.kind, RepoLocationKind::ManagedClone);
    assert_eq!(location.status, db::RepoLocationStatus::Ready);
    assert_eq!(
        location.path,
        workspace_root
            .path()
            .join(".repos")
            .join(&repo_id)
            .to_string_lossy()
    );
}

#[tokio::test]
async fn daemon_placement_has_no_embedded_path() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _) = seed_project_repo(&db).await;
    let task = seed_task(&db, &project_id, None).await;
    let workspace_root = TempDir::new().expect("workspace root creates");
    let workspace = seed_workspace(&db, &task, WorkspaceStatus::Ready, workspace_root.path()).await;
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(16)));
    let router = service.workspace_backend_router();
    let resolved = router
        .resolve(&db, &workspace)
        .await
        .expect("server workspace resolves");
    let now = now_rfc3339();
    let daemon_id = new_uuid_v4();
    let runtime_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO daemon (id, machine_id, hostname, os, arch, status, created_at, updated_at)
             VALUES (?, ?, 'remote', 'macos', 'aarch64', 'online', ?, ?)",
    )
    .bind(&daemon_id)
    .bind(&daemon_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("daemon creates");
    sqlx::query(
        "INSERT INTO runtime (id, daemon_id, kind, workspace_root, status, created_at, updated_at)
             VALUES (?, ?, 'local', '/remote', 'ready', ?, ?)",
    )
    .bind(&runtime_id)
    .bind(&daemon_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("runtime creates");
    sqlx::query(
        "UPDATE repo_location SET owner_kind = 'daemon', daemon_id = ?, runtime_id = ?,
             path = '/remote/repo', version = version + 1 WHERE id = ? AND version = 1",
    )
    .bind(&daemon_id)
    .bind(&runtime_id)
    .bind(&resolved.placement.repo_location_id)
    .execute(db.pool())
    .await
    .expect("location moves to fixture daemon");
    sqlx::query(
        "UPDATE workspace_placement SET owner_kind = 'daemon', daemon_id = ?, runtime_id = ?,
             workspace_handle = 'opaque-daemon-handle', version = version + 1
             WHERE id = ? AND version = ?",
    )
    .bind(&daemon_id)
    .bind(&runtime_id)
    .bind(&resolved.placement.id)
    .bind(resolved.placement.version)
    .execute(db.pool())
    .await
    .expect("placement moves to fixture daemon");

    assert!(matches!(
        router.resolve(&db, &workspace).await,
        Err(
            crate::workspace_backend::WorkspaceBackendError::OwnerUnsupported {
                owner_kind: PlacementOwnerKind::Daemon,
            }
        )
    ));
    assert!(matches!(
        router.embedded_path(&db, &workspace).await,
        Err(
            crate::workspace_backend::WorkspaceBackendError::OwnerUnsupported {
                owner_kind: PlacementOwnerKind::Daemon,
            }
        )
    ));
    let daemon_resolved = crate::workspace_backend::ResolvedWorkspace {
        placement: WorkspacePlacementRepo::get_by_workspace_id(&*db, &workspace.id)
            .await
            .unwrap()
            .unwrap(),
        backend: resolved.backend,
    };
    assert_eq!(daemon_resolved.handle().unwrap(), "opaque-daemon-handle");
    assert!(matches!(
        daemon_resolved.embedded_path(),
        Err(
            crate::workspace_backend::WorkspaceBackendError::OwnerUnsupported {
                owner_kind: PlacementOwnerKind::Daemon,
            }
        )
    ));
}

#[tokio::test]
async fn root_task_reuses_ready_task_workspace() {
    let db = sqlite_db().await;
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let root = seed_task(&db, &project_id, None).await;
    let worktree_dir = TempDir::new().expect("worktree dir creates");
    let workspace = seed_workspace(&db, &root, WorkspaceStatus::Ready, worktree_dir.path()).await;
    let temp = TempDir::new().expect("temp dir creates");

    let prepared = prepare_workspace_for_test(&db, temp.path(), &root, &root.id, None)
        .await
        .expect("root workspace prepares");

    assert_eq!(prepared.id, workspace.id);
    assert_eq!(prepared.task_id, root.id);
}

#[tokio::test]
async fn existing_workspace_for_old_primary_is_not_reused() {
    let db = sqlite_db().await;
    let (project_id, old_repo_id) = seed_project_repo(&db).await;
    let task = seed_task(&db, &project_id, None).await;
    let worktree_dir = TempDir::new().expect("worktree dir creates");
    let old_workspace =
        seed_workspace(&db, &task, WorkspaceStatus::Ready, worktree_dir.path()).await;
    assert_eq!(old_workspace.repo_id, old_repo_id);

    let replacement_repo_id = new_uuid_v4();
    let now = now_rfc3339();
    RepoRepo::create(
        &db,
        CreateRepo {
            id: replacement_repo_id.clone(),
            project_id: project_id.clone(),
            name: "replacement".to_owned(),
            remote_url: Some("/tmp/replacement-repo".to_owned()),
            local_path: Some("/tmp/replacement-repo".to_owned()),
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("replacement Repo creates");
    let project = ProjectRepo::get_by_id(&db, &project_id)
        .await
        .expect("Project loads")
        .expect("Project exists");
    ProjectRepo::update_at_version(
        &db,
        UpdateProject {
            id: project_id,
            name: None,
            settings: None,
            primary_repo_id: Some(Some(replacement_repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        project.version,
        None,
    )
    .await
    .expect("Project primary Repo changes");

    let result = prepare_workspace_for_test(&db, worktree_dir.path(), &task, &task.id, None).await;
    assert!(
        matches!(
            &result,
            Err(ServiceError::WorkspaceResetRequired { task_id, reason })
                if task_id == &task.id
                    && reason.contains(&old_repo_id)
                    && reason.contains(&replacement_repo_id)
        ),
        "expected an explicit reset boundary, got: {result:?}"
    );
    let preserved = WorkspaceRepo::get_by_task_id(&db, &task.id)
        .await
        .expect("Workspace reload succeeds")
        .expect("historical Workspace remains until guarded reset");
    assert_eq!(preserved.id, old_workspace.id);
    assert_eq!(preserved.repo_id, old_repo_id);
}

#[tokio::test]
async fn subtask_reuses_ready_parent_workspace() {
    let db = sqlite_db().await;
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let root = seed_task(&db, &project_id, None).await;
    let subtask = seed_task(&db, &project_id, Some(root.id.clone())).await;
    let worktree_dir = TempDir::new().expect("worktree dir creates");
    let workspace = seed_workspace(&db, &root, WorkspaceStatus::Ready, worktree_dir.path()).await;
    let placement = WorkspacePlacementRepo::get_by_workspace_id(&db, &workspace.id)
        .await
        .expect("root placement loads")
        .expect("root placement exists");
    let temp = TempDir::new().expect("temp dir creates");

    let prepared = prepare_workspace_for_test(&db, temp.path(), &subtask, &subtask.id, None)
        .await
        .expect("subtask workspace prepares");

    assert_eq!(prepared.id, workspace.id);
    assert_eq!(prepared.task_id, root.id);
    assert_eq!(
        WorkspacePlacementRepo::get_by_workspace_id(&db, &prepared.id)
            .await
            .expect("shared placement loads")
            .expect("shared placement exists"),
        placement,
    );
}

#[tokio::test]
async fn subtask_rejects_not_ready_parent_workspace() {
    let db = sqlite_db().await;
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let root = seed_task(&db, &project_id, None).await;
    let subtask = seed_task(&db, &project_id, Some(root.id.clone())).await;
    let temp = TempDir::new().expect("temp dir creates");
    let worktree_dir = TempDir::new().expect("worktree dir creates");
    seed_workspace(&db, &root, WorkspaceStatus::Creating, worktree_dir.path()).await;
    let not_ready = prepare_workspace_for_test(&db, temp.path(), &subtask, &subtask.id, None).await;
    assert!(matches!(
        not_ready,
        Err(ServiceError::ParentWorkspaceRequired { parent_task_id }) if parent_task_id == root.id
    ));
}

#[tokio::test]
async fn subtask_without_parent_workspace_creates_root_owned_workspace() {
    let db = sqlite_db().await;
    let repo_dir = TempDir::new().expect("repo dir creates");
    let (project_id, repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    let root = seed_task(&db, &project_id, None).await;
    let subtask = seed_task(&db, &project_id, Some(root.id.clone())).await;
    let workspace_root = TempDir::new().expect("workspace root creates");

    let workspace =
        prepare_workspace_for_test(&db, workspace_root.path(), &subtask, &subtask.id, None)
            .await
            .expect("subtask creates the shared root workspace");

    assert_eq!(workspace.task_id, root.id);
    assert_eq!(workspace.repo_id, repo_id);
    assert_eq!(workspace.status, WorkspaceStatus::Ready);
    assert!(std::path::Path::new(workspace.embedded_worktree_path_for_backend()).exists());
    assert!(
        WorkspaceRepo::get_by_task_id(&db, &subtask.id)
            .await
            .expect("subtask Workspace lookup succeeds")
            .is_none(),
        "the shared Workspace must remain owned by the coordination root"
    );
}
