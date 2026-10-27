//! Workspaces repository tests.
use super::*;

#[tokio::test]
async fn clearing_workspace_cleanup_deadline_clears_retry_state() {
    let db = sqlite_db().await;
    let (project_id, repo_id, _) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        None,
        "done".to_owned(),
        "cleanup retry reset",
    )
    .await;
    let workspace_id = seed_workspace_for_task(&db, &task_id, &repo_id).await;
    sqlx::query(
        "UPDATE workspace
         SET cleanup_attempts = 3, last_cleanup_error = 'cleanup failed'
         WHERE id = ?",
    )
    .bind(&workspace_id)
    .execute(db.pool())
    .await
    .unwrap();

    let workspace = WorkspaceRepo::set_cleanup_after(
        &db,
        &workspace_id,
        Some("2026-10-01T12:00:00Z".to_owned()),
        &now_rfc3339(),
    )
    .await
    .unwrap();
    assert_eq!(workspace.cleanup_attempts, 3);
    assert_eq!(
        workspace.last_cleanup_error.as_deref(),
        Some("cleanup failed")
    );

    let workspace = WorkspaceRepo::set_cleanup_after(&db, &workspace_id, None, &now_rfc3339())
        .await
        .unwrap();
    assert!(workspace.cleanup_after.is_none());
    assert_eq!(workspace.cleanup_attempts, 0);
    assert!(workspace.last_cleanup_error.is_none());
}

#[tokio::test]
async fn active_workspace_lease_can_be_renewed_while_execution_is_running() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "in_progress".to_owned(),
        "Long-running repository task",
    )
    .await;
    let workspace_id = seed_workspace_for_task(&db, &task_id, &repo_id).await;
    let task = TaskRepo::get_by_id(&db, &task_id, true)
        .await
        .expect("task lookup")
        .expect("task exists");
    let now = chrono::Utc::now();
    let execution = ExecutionRepo::create(
        &db,
        CreateExecution {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "coder".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: Some(now.to_rfc3339()),
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace_id),
            created_at: now.to_rfc3339(),
            updated_at: now.to_rfc3339(),
        },
    )
    .await
    .expect("running execution");
    let issued_at = (now - chrono::Duration::minutes(14)).to_rfc3339();
    let original_expiry = (now + chrono::Duration::minutes(1)).to_rfc3339();
    let lease =
        WorkspaceLeaseRepo::issue(
            &db,
            CreateWorkspaceLease {
                id: new_uuid_v4(),
                project_id,
                task_id,
                task_version: task.version,
                execution_id: execution.id,
                operation_idempotency_key: new_uuid_v4(),
                repository_binding_id: repo_id,
                base_ref: "main".to_owned(),
                role: "worker".to_owned(),
                capabilities_json: r#"["repository_write"]"#.to_owned(),
                assigned_principal_type: "agent".to_owned(),
                assigned_principal_id: agent_id,
                capability_profile_revision: "forge.capability-profile/v1".to_owned(),
                capability_profile_digest:
                    "sha256:eeb061a14ab862e1a7b16989ef637293ba538f46122ff28b30313d330dbae4a8"
                        .to_owned(),
                issuing_principal_type: "system".to_owned(),
                issuing_principal_id: "task-service-scheduler".to_owned(),
                issued_at: issued_at.clone(),
                expires_at: original_expiry,
                created_at: issued_at.clone(),
                updated_at: issued_at,
            },
        )
        .await
        .expect("lease issue");
    let renewed_expiry = (now + chrono::Duration::minutes(15)).to_rfc3339();
    let renewed = WorkspaceLeaseRepo::renew_active(
        &db,
        &now.to_rfc3339(),
        &(now + chrono::Duration::minutes(5)).to_rfc3339(),
        &renewed_expiry,
        10,
    )
    .await
    .expect("lease renewal");
    assert_eq!(renewed.len(), 1);
    assert_eq!(renewed[0].id, lease.id);
    assert_eq!(renewed[0].expires_at, renewed_expiry);
    assert_eq!(renewed[0].version, lease.version + 1);

    sqlx::query("UPDATE task SET version = version + 1 WHERE id = ?")
        .bind(&renewed[0].task_id)
        .execute(db.pool())
        .await
        .expect("make the lease's Task version stale");
    let retry_now = now + chrono::Duration::seconds(1);
    let rejected = WorkspaceLeaseRepo::renew_active(
        &db,
        &retry_now.to_rfc3339(),
        &(now + chrono::Duration::minutes(20)).to_rfc3339(),
        &(now + chrono::Duration::minutes(30)).to_rfc3339(),
        10,
    )
    .await
    .expect("stale lease is skipped instead of poisoning the heartbeat pass");
    assert!(rejected.is_empty());
}

#[tokio::test]
async fn prebaseline_discovery_task_is_admitted_to_running_execution() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let now = now_rfc3339();
    let user_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, created_at, updated_at)
         VALUES (?, ?, 'test', ?, ?)",
    )
    .bind(&user_id)
    .bind(format!("{user_id}@example.test"))
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("user fixture");
    sqlx::query("UPDATE project SET owner_id = ? WHERE id = ?")
        .bind(&user_id)
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("Project owner fixture");
    let charter_id = new_uuid_v4();
    let charter_revision_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO project_charter
         (id, account_id, project_id, project_mode, maturity, lifecycle, created_at, updated_at)
         VALUES (?, ?, ?, 'standard', 'mvp', 'attached', ?, ?)",
    )
    .bind(&charter_id)
    .bind(&user_id)
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("charter fixture");
    sqlx::query(
        "INSERT INTO project_charter_revision
         (id, charter_id, revision, lifecycle, schema_version, render_version,
          content_json, rendered_view, author_type, author_id, content_digest,
          rendered_digest, created_at)
         VALUES (?, ?, 1, 'approved', 'test', 'test', '{}', 'charter',
                 'user', ?, 'content', 'rendered', ?)",
    )
    .bind(&charter_revision_id)
    .bind(&charter_id)
    .bind(&user_id)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("charter revision fixture");
    sqlx::query("UPDATE project_charter SET current_approved_revision_id = ? WHERE id = ?")
        .bind(&charter_revision_id)
        .bind(&charter_id)
        .execute(db.pool())
        .await
        .expect("charter pointer");
    sqlx::query(
        "UPDATE project
         SET charter_status = 'charter_backed', charter_setup_required = 0,
             current_charter_id = ?, current_charter_revision_id = ?,
             current_charter_version = 1
         WHERE id = ?",
    )
    .bind(&charter_id)
    .bind(&charter_revision_id)
    .bind(&project_id)
    .execute(db.pool())
    .await
    .expect("Project Charter binding");
    let task_id = new_uuid_v4();
    TaskRepo::create(
        &db,
        CreateTask {
            id: task_id.clone(),
            project_id: project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Read-only discovery".to_owned(),
            description: None,
            task_type: "discovery".to_owned(),
            status: "in_progress".to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("discovery Task");
    sqlx::query(
        "INSERT INTO project_task_governance
         (task_id, project_id, charter_revision_id, capability_class, risk_class,
          runnable, created_at, updated_at)
         VALUES (?, ?, ?, 'repository_read', 'low', 0, ?, ?)",
    )
    .bind(&task_id)
    .bind(&project_id)
    .bind(&charter_revision_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("prebaseline governance");

    let execution = ExecutionRepo::create(
        &db,
        CreateExecution {
            id: new_uuid_v4(),
            task_id,
            agent_id: Some(agent_id),
            role: "coder".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: Some(now.clone()),
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("prebaseline discovery execution is admitted");
    assert_eq!(execution.status, ExecutionStatus::Running);
}

#[tokio::test]
async fn workspace_task_id_unique_is_preserved() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "todo".to_owned(),
        "Workspace owner",
    )
    .await;

    WorkspaceRepo::create(
        &db,
        CreateWorkspace {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            repo_id: repo_id.clone(),
            worktree_path: "/tmp/forge/worktrees/task/one".to_owned(),
            branch: format!("task/{task_id}"),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");

    let duplicate = WorkspaceRepo::create(
        &db,
        CreateWorkspace {
            id: new_uuid_v4(),
            task_id,
            repo_id,
            worktree_path: "/tmp/forge/worktrees/task/two".to_owned(),
            branch: "task/duplicate".to_owned(),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await;

    assert!(duplicate.is_err());
}
