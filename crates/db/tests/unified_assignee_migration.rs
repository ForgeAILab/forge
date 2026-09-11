use db::{create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations};

#[tokio::test]
async fn all_migrations_apply_and_task_role_assignment_post_sweep_shape_is_valid() {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations apply");

    let applied_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _migration")
        .fetch_one(&pool)
        .await
        .expect("migration count loads");
    let expected_count = std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"))
        .expect("migration directory reads")
        .filter(|entry| {
            entry
                .as_ref()
                .ok()
                .and_then(|entry| entry.path().file_name()?.to_str().map(str::to_owned))
                .is_some_and(|filename| filename.starts_with('V') && filename.ends_with(".sql"))
        })
        .count() as i64;
    assert_eq!(applied_count, expected_count);

    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let task_id = new_uuid_v4();

    sqlx::query(
        "INSERT INTO project (id, name, settings, workflow_definition, created_at, updated_at) VALUES (?, 'Forge', '{}', '{}', ?, ?)",
    )
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(&pool)
    .await
    .expect("project inserts");

    sqlx::query(
        "INSERT INTO repo (id, project_id, name, remote_url, local_path, work_mode, default_branch, created_at, updated_at) VALUES (?, ?, 'forge', 'https://example.com/forge.git', NULL, 'direct_merge', 'main', ?, ?)",
    )
    .bind(&repo_id)
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(&pool)
    .await
    .expect("repo inserts");

    sqlx::query("UPDATE project SET primary_repo_id = ? WHERE id = ?")
        .bind(&repo_id)
        .bind(&project_id)
        .execute(&pool)
        .await
        .expect("project primary repo attaches");

    sqlx::query(
        "INSERT INTO task (id, project_id, title, created_at, updated_at) VALUES (?, ?, 'Task', ?, ?)",
    )
    .bind(&task_id)
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(&pool)
    .await
    .expect("task inserts");

    let invalid_insert = sqlx::query(
        "INSERT INTO task_role_assignment (id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at) VALUES (?, ?, 'coder', 'agent', NULL, ?, ?)",
    )
    .bind(new_uuid_v4())
    .bind(&task_id)
    .bind(&now)
    .bind(&now)
    .execute(&pool)
    .await;
    assert!(invalid_insert.is_err());

    let assignment_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO task_role_assignment (id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at) VALUES (?, ?, 'coder', 'agent', 'agent-a', ?, ?)",
    )
    .bind(&assignment_id)
    .bind(&task_id)
    .bind(&now)
    .bind(&now)
    .execute(&pool)
    .await
    .expect("valid role assignment inserts");

    sqlx::query("UPDATE task_role_assignment SET assignee_id = NULL WHERE id = ?")
        .bind(&assignment_id)
        .execute(&pool)
        .await
        .expect("post-sweep update accepts deleted agent marker");

    let assignee_id: Option<String> =
        sqlx::query_scalar("SELECT assignee_id FROM task_role_assignment WHERE id = ?")
            .bind(&assignment_id)
            .fetch_one(&pool)
            .await
            .expect("assignee_id loads");
    assert_eq!(assignee_id, None);
}
