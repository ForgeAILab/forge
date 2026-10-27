use super::*;
use db::DbError;

async fn usage_sample(db: &SqliteDb) -> api_types::UsageAggregate {
    let usage = db::UsageAnalyticsRepo::get_project_usage_analytics(db, "project", None, None)
        .await
        .unwrap();
    api_types::UsageAggregate {
        counts: usage.counts,
        tokens: usage.tokens,
        cost: usage.cost,
    }
}

#[tokio::test]
async fn cli_runtime_policy_cas_and_scoped_reads_preserve_other_policies() {
    let db = database().await;
    for id in ["owner", "other"] {
        user(&db, id).await;
    }
    for id in ["daemon", "other-daemon"] {
        sqlx::query("INSERT INTO daemon (id, machine_id, hostname, os, arch, status, created_at, updated_at) VALUES (?, ?, 'test', 'test', 'test', 'online', ?, ?)")
            .bind(id).bind(id).bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
    }
    assert!(db
        .cli_runtime_policies_for_daemon("owner", "daemon")
        .await
        .unwrap()
        .is_empty());
    db.set_cli_runtime_enabled("owner", "daemon", "codex", false, 0, NOW)
        .await
        .unwrap();
    db.set_cli_runtime_enabled("owner", "daemon", "claude_code", true, 0, NOW)
        .await
        .unwrap();
    db.set_cli_runtime_enabled("other", "daemon", "codex", true, 0, NOW)
        .await
        .unwrap();
    db.set_cli_runtime_enabled("owner", "other-daemon", "codex", true, 0, NOW)
        .await
        .unwrap();
    for version in [0, 2] {
        assert!(matches!(
            db.set_cli_runtime_enabled("owner", "daemon", "codex", true, version, LATER)
                .await,
            Err(DbError::VersionConflict)
        ));
    }
    assert!(matches!(
        db.set_cli_runtime_enabled("owner", "daemon", "gemini", true, 3, LATER)
            .await,
        Err(DbError::VersionConflict)
    ));
    let policies = db
        .cli_runtime_policies_for_daemon("owner", "daemon")
        .await
        .unwrap();
    assert_eq!(policies.len(), 2);
    assert_eq!(policies["codex"], (false, 1));
    assert_eq!(policies["claude_code"], (true, 1));
    db.set_cli_runtime_enabled("owner", "daemon", "codex", true, 1, LATER)
        .await
        .unwrap();
    let row: (bool, i64, String, String) = sqlx::query_as("SELECT enabled, version, created_at, updated_at FROM cli_runtime_policy WHERE owner_user_id = 'owner' AND daemon_id = 'daemon' AND executor_type = 'codex'")
        .fetch_one(db.pool()).await.unwrap();
    assert_eq!(row, (true, 2, NOW.into(), LATER.into()));
    assert_eq!(
        db.cli_runtime_policies_for_daemon("other", "daemon")
            .await
            .unwrap()["codex"],
        (true, 1)
    );
    assert_eq!(
        db.cli_runtime_policies_for_daemon("owner", "other-daemon")
            .await
            .unwrap()["codex"],
        (true, 1)
    );
}

#[tokio::test]
async fn dependency_created_at_reads_the_exact_directed_edge() {
    let db = database().await;
    project(&db, "project", None).await;
    task(&db, "task", "project", "todo").await;
    task(&db, "prerequisite", "project", "todo").await;
    sqlx::query("INSERT INTO task_dependency (task_id, depends_on_id, created_at) VALUES ('task', 'prerequisite', ?)")
        .bind(LATER).execute(db.pool()).await.unwrap();
    assert_eq!(
        db.task_dependency_created_at("task", "prerequisite")
            .await
            .unwrap(),
        LATER
    );
    assert!(matches!(
        db.task_dependency_created_at("prerequisite", "task").await,
        Err(DbError::Sqlx(sqlx::Error::RowNotFound))
    ));
}

#[tokio::test]
async fn memory_project_visibility_includes_unowned_owned_and_member_projects() {
    let db = database().await;
    user(&db, "owner").await;
    user(&db, "other").await;
    project(&db, "public", None).await;
    project(&db, "owned", Some("owner")).await;
    project(&db, "member", Some("other")).await;
    project(&db, "hidden", Some("other")).await;
    sqlx::query("INSERT INTO project_member (id, project_id, user_id, role, created_at, updated_at) VALUES ('membership', 'member', 'owner', 'viewer', ?, ?)")
        .bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
    let mut visible = db.memory_visible_project_ids("owner").await.unwrap();
    visible.sort();
    assert_eq!(visible, ["member", "owned", "public"]);
    assert_eq!(
        db.memory_visible_project_ids("unknown").await.unwrap(),
        ["public"]
    );
}

#[tokio::test]
async fn workflow_task_counts_exclude_deleted_and_foreign_project_tasks() {
    let db = database().await;
    project(&db, "project", None).await;
    project(&db, "other", None).await;
    for (id, project_id, status) in [
        ("a", "project", "todo"),
        ("b", "project", "todo"),
        ("c", "project", "qa"),
        ("deleted", "project", "qa"),
        ("foreign", "other", "qa"),
    ] {
        task(&db, id, project_id, status).await;
    }
    sqlx::query("UPDATE task SET deleted_at = ? WHERE id = 'deleted'")
        .bind(LATER)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        db.project_live_task_status_counts("project").await.unwrap(),
        [("qa".to_owned(), 1), ("todo".to_owned(), 2)]
    );
    assert!(db
        .project_live_task_status_counts("missing")
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn execution_timing_preserves_runtime_fallbacks_and_latest_tie_breaking() {
    let db = database().await;
    project(&db, "project", None).await;
    task(&db, "task", "project", "todo").await;
    task(&db, "other", "project", "todo").await;
    let empty = db
        .task_execution_timing("task")
        .await
        .unwrap()
        .with_usage(usage_sample(&db).await)
        .unwrap();
    assert_eq!(empty.total_runtime_seconds, 0.0);
    assert!(empty.active_execution_id.is_none());
    assert!(empty.latest_execution_id.is_none());
    for (id, task_id, created_at, updated_at, stopped_at) in [
        (
            "completed",
            "task",
            NOW,
            LATER,
            Some("2026-10-01T00:00:30Z"),
        ),
        ("latest-a", "task", LATER, "2026-10-01T00:01:20Z", None),
        ("latest-z", "task", LATER, "2026-10-01T00:01:10Z", None),
        (
            "negative",
            "task",
            "2026-09-30T23:59:59Z",
            "2026-09-30T23:59:58Z",
            None,
        ),
        ("foreign", "other", NOW, LATER, None),
    ] {
        sqlx::query("INSERT INTO execution (id, task_id, role, status, created_at, updated_at, stopped_at) VALUES (?, ?, 'coder', 'completed', ?, ?, ?)")
            .bind(id).bind(task_id).bind(created_at).bind(updated_at).bind(stopped_at)
            .execute(db.pool()).await.unwrap();
    }
    let mut usage = usage_sample(&db).await;
    usage.tokens.input_tokens = 17;
    let timing = db.task_execution_timing("task").await.unwrap();
    let response = timing.with_usage(usage).unwrap();
    assert_eq!(response.total_runtime_seconds, 60.0);
    assert_eq!(response.latest_execution_id.as_deref(), Some("latest-z"));
    assert_eq!(
        response.latest_execution_status.as_deref(),
        Some("completed")
    );
    assert_eq!(response.latest_role.as_deref(), Some("coder"));
    assert_eq!(response.latest_started_at.as_deref(), Some(LATER));
    assert!(response.latest_stopped_at.is_none());
    assert_eq!(response.latest_runtime_seconds, Some(10.0));
    assert_eq!(response.tokens.input_tokens, 17);
    assert!(response.active_elapsed_seconds.is_none());
    sqlx::query("INSERT INTO execution (id, task_id, role, status, created_at, updated_at) VALUES ('running', 'task', 'reviewer', 'running', strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-5 seconds'), ?)")
        .bind(NOW).execute(db.pool()).await.unwrap();
    let snapshot = db.task_execution_timing("task").await.unwrap();
    sqlx::query("UPDATE execution SET status = 'completed', updated_at = ? WHERE id = 'running'")
        .bind(NOW)
        .execute(db.pool())
        .await
        .unwrap();
    let response = snapshot.with_usage(usage_sample(&db).await).unwrap();
    assert_eq!(response.active_execution_id.as_deref(), Some("running"));
    assert_eq!(response.active_role.as_deref(), Some("reviewer"));
    assert!((5.0..=6.0).contains(&response.active_elapsed_seconds.unwrap()));
    assert_eq!(response.latest_execution_id.as_deref(), Some("running"));
    assert!((65.0..=66.0).contains(&response.total_runtime_seconds));
}
