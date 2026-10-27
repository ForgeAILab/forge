//! Repositories repository tests.
use super::*;

/// Regression test for the F11 finding: `workspace.repo_id` used to carry
/// `ON DELETE CASCADE` to `repo(id)` (V021), so deleting a Repo silently
/// deleted every Workspace row for it -- destroying exactly the
/// per-execution repository pin the Unreleased changelog promises Workspaces
/// retain. V139 rebuilds `workspace` so `repo_id` survives as a plain
/// historical column while the Repo delete itself keeps succeeding.
#[tokio::test]
async fn workspace_survives_repo_delete_with_repo_id_retained() {
    let db = sqlite_db().await;
    let (project_id, repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        None,
        "todo".to_owned(),
        "Workspace survives repo delete",
    )
    .await;
    let now = now_rfc3339();
    let workspace_id = new_uuid_v4();

    WorkspaceRepo::create(
        &db,
        CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task_id.clone(),
            repo_id: repo_id.clone(),
            worktree_path: "/tmp/forge-workspace-survives-repo-delete".to_owned(),
            branch: workspace::task_branch_name(&task_id),
            status: WorkspaceStatus::Ready,
            before_sha: Some("f9bcd03".to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");

    RepoRepo::delete(&db, &repo_id)
        .await
        .expect("repo delete succeeds without being blocked by its Workspaces");

    let workspace = WorkspaceRepo::get_by_id(&db, &workspace_id)
        .await
        .expect("workspace lookup succeeds")
        .expect("workspace row survives repository deletion");
    assert_eq!(workspace.repo_id, repo_id);
    assert_eq!(workspace.before_sha.as_deref(), Some("f9bcd03"));
    assert_eq!(
        workspace.worktree_path,
        "/tmp/forge-workspace-survives-repo-delete"
    );

    let repo = RepoRepo::get_by_id(&db, &repo_id)
        .await
        .expect("repo lookup succeeds");
    assert!(repo.is_none(), "repo row itself is gone");
}

#[tokio::test]
async fn repo_delete_rejects_a_running_execution_without_partial_changes() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "in_progress".to_owned(),
        "Active repo delete guard",
    )
    .await;
    let workspace_id = seed_workspace_for_task(&db, &task_id, &repo_id).await;
    let now = now_rfc3339();
    ExecutionRepo::create(
        &db,
        CreateExecution {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            agent_id: Some(agent_id),
            role: "executor".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace_id),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("running execution creates");

    assert!(matches!(
        RepoRepo::delete(&db, &repo_id).await,
        Err(DbError::RepoInUse { repo_id: blocked }) if blocked == repo_id
    ));
    assert!(RepoRepo::get_by_id(&db, &repo_id)
        .await
        .expect("repo reloads")
        .is_some());
    assert_eq!(
        ProjectRepo::get_by_id(&db, &project_id)
            .await
            .expect("project reloads")
            .expect("project remains")
            .primary_repo_id
            .as_deref(),
        Some(repo_id.as_str())
    );
}

#[tokio::test]
async fn sqlite_repo_create_round_trips_local_path() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();

    ProjectRepo::create(
        &db,
        CreateProject {
            id: project_id.clone(),
            name: "Forge".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_string(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");

    let repo = RepoRepo::create(
        &db,
        CreateRepo {
            id: repo_id,
            project_id,
            name: "forge".to_owned(),
            remote_url: Some("https://example.com/forge.git".to_owned()),
            local_path: Some("/tmp/forge-test-repo".to_owned()),
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("local repo creates");

    assert_eq!(repo.local_path, Some("/tmp/forge-test-repo".to_owned()));
    assert_eq!(
        repo.remote_url.as_deref(),
        Some("https://example.com/forge.git")
    );
}

#[tokio::test]
async fn sqlite_repo_create_round_trips_remote_url() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();

    ProjectRepo::create(
        &db,
        CreateProject {
            id: project_id.clone(),
            name: "Forge".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_string(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");

    let repo = RepoRepo::create(
        &db,
        CreateRepo {
            id: repo_id,
            project_id,
            name: "forge".to_owned(),
            remote_url: Some("https://example.com/forge.git".to_owned()),
            local_path: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("remote repo creates");

    assert_eq!(repo.local_path, None);
    assert_eq!(
        repo.remote_url.as_deref(),
        Some("https://example.com/forge.git")
    );
}

#[tokio::test]
async fn sqlite_repo_create_accepts_missing_remote_url() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let project_id = new_uuid_v4();

    ProjectRepo::create(
        &db,
        CreateProject {
            id: project_id.clone(),
            name: "Forge".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_string(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");

    let result = sqlx::query("INSERT INTO repo (id, project_id, name, remote_url, local_path, work_mode, default_branch, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(new_uuid_v4())
        .bind(project_id)
        .bind("forge")
        .bind(None::<String>)
        .bind(None::<String>)
        .bind("direct_merge")
        .bind("main")
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .map_err(crate::DbError::from);

    result.expect("local-only repository accepts a NULL remote URL");
}

#[tokio::test]
async fn sqlite_repo_create_normalizes_blank_remote_url() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    for remote_url in [None, Some(""), Some(" \t\r\n"), Some("\u{2003}")] {
        let project = ProjectRepo::create(
            &db,
            CreateProject {
                id: new_uuid_v4(),
                name: "Local Project".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project creates");
        for primary in [false, true] {
            let input = CreateRepo {
                id: new_uuid_v4(),
                project_id: project.id.clone(),
                name: "Local Repo".to_owned(),
                remote_url: remote_url.map(str::to_owned),
                local_path: Some("/tmp/local-repo".to_owned()),
                default_branch: "main".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            };
            let repo = if primary {
                RepoRepo::create_primary_for_project(&db, input, project.version, now.clone()).await
            } else {
                RepoRepo::create(&db, input).await
            }
            .expect("local repo creates");
            let stored: Option<String> =
                sqlx::query_scalar("SELECT remote_url FROM repo WHERE id = ?")
                    .bind(&repo.id)
                    .fetch_one(db.pool())
                    .await
                    .expect("stored remote loads");
            assert_eq!(stored, None);
            assert_eq!(repo.remote_url, None);
        }
    }
}

#[tokio::test]
async fn sqlite_repo_update_and_read_normalize_blank_remote_url() {
    let db = sqlite_db().await;
    let (_, repo_id, _) = seed_project_repo_agent(&db).await;
    let original = Some("https://example.com/forge.git".to_owned());
    for (input, expected) in [
        (None, original.clone()),
        (Some(None), None),
        (Some(Some(String::new())), None),
        (Some(Some(" \t\r\n".to_owned())), None),
        (Some(original.clone()), original.clone()),
    ] {
        sqlx::query("UPDATE repo SET remote_url = ? WHERE id = ?")
            .bind(&original)
            .bind(&repo_id)
            .execute(db.pool())
            .await
            .expect("remote resets");
        let repo = RepoRepo::update(
            &db,
            UpdateRepo {
                id: repo_id.clone(),
                name: None,
                local_path: None,
                remote_url: input,
                default_branch: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .expect("remote updates");
        let stored: Option<String> = sqlx::query_scalar("SELECT remote_url FROM repo WHERE id = ?")
            .bind(&repo_id)
            .fetch_one(db.pool())
            .await
            .expect("stored remote loads");
        assert_eq!(stored, expected);
        assert_eq!(repo.remote_url, expected);
    }

    sqlx::query("UPDATE repo SET remote_url = '  ' WHERE id = ?")
        .bind(&repo_id)
        .execute(db.pool())
        .await
        .expect("unnormalized remote seeds");
    let repo = RepoRepo::get_by_id(&db, &repo_id)
        .await
        .expect("repo loads")
        .expect("repo exists");
    assert_eq!(repo.remote_url, None);
    assert_eq!(
        RepoRepo::list_by_project(&db, &repo.project_id, page(10))
            .await
            .expect("repos list")
            .items[0]
            .remote_url,
        None
    );
}
