use db::{
    create_sqlite_pool, run_migrations, run_migrations_from, CreateProject, ProjectRepo, SqliteDb,
};
use sqlx::Row;
use std::{fs, path::Path};

#[tokio::test]
async fn migration_clears_only_legacy_environment_task_blocks() {
    let migration_dir =
        std::env::temp_dir().join(format!("forge-v202610010410-{}", db::new_uuid_v4()));
    fs::create_dir_all(&migration_dir).unwrap();
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    for entry in fs::read_dir(&source).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        let name = name.to_str().unwrap();
        let Some(version) = name
            .strip_prefix('V')
            .and_then(|name| name.split_once("__"))
            .and_then(|(version, _)| version.parse::<i64>().ok())
        else {
            continue;
        };
        if version < 202610010410 {
            fs::copy(entry.path(), migration_dir.join(name)).unwrap();
        }
    }
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations_from(&pool, &migration_dir).await.unwrap();
    let old_time = "2026-01-01T00:00:00Z";
    sqlx::query("INSERT INTO project (id, name, settings, workflow_definition, created_at, updated_at) VALUES ('p', 'Project', '{}', '{}', ?, ?)")
        .bind(old_time).bind(old_time).execute(&pool).await.unwrap();
    for index in 0..6 {
        sqlx::query("INSERT INTO task (id, project_id, title, status, task_type, error_annotation, blocked_json, version, created_at, updated_at) VALUES (?, 'p', 'Stranded', 'in_progress', 'task', '{\"type\":\"environment_not_ready\"}', '{\"reason\":\"disk full\"}', 3, ?, ?)")
            .bind(format!("env-{index}")).bind(old_time).bind(old_time).execute(&pool).await.unwrap();
    }
    for (id, annotation) in [
        ("other", r#"{"type":"executor_failed"}"#),
        ("malformed", "not json"),
    ] {
        sqlx::query("INSERT INTO task (id, project_id, title, status, task_type, error_annotation, blocked_json, version, created_at, updated_at) VALUES (?, 'p', 'Other', 'in_progress', 'task', ?, '{\"reason\":\"keep\"}', 3, ?, ?)")
            .bind(id).bind(annotation).bind(old_time).bind(old_time).execute(&pool).await.unwrap();
    }
    fs::copy(
        source.join("V202610010410__project_environment_pause.sql"),
        migration_dir.join("V202610010410__project_environment_pause.sql"),
    )
    .unwrap();
    run_migrations_from(&pool, &migration_dir).await.unwrap();
    for index in 0..6 {
        let row = sqlx::query("SELECT status, error_annotation, blocked_json, version, updated_at FROM task WHERE id = ?")
            .bind(format!("env-{index}")).fetch_one(&pool).await.unwrap();
        assert_eq!(row.get::<String, _>("status"), "in_progress");
        assert!(row.get::<Option<String>, _>("error_annotation").is_none());
        assert!(row.get::<Option<String>, _>("blocked_json").is_none());
        assert_eq!(row.get::<i64, _>("version"), 4);
        assert_ne!(row.get::<String, _>("updated_at"), old_time);
    }
    for id in ["other", "malformed"] {
        let row = sqlx::query(
            "SELECT error_annotation, blocked_json, version, updated_at FROM task WHERE id = ?",
        )
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(row.get::<Option<String>, _>("error_annotation").is_some());
        assert_eq!(row.get::<String, _>("blocked_json"), r#"{"reason":"keep"}"#);
        assert_eq!(row.get::<i64, _>("version"), 3);
        assert_eq!(row.get::<String, _>("updated_at"), old_time);
    }
    let detail: Option<String> =
        sqlx::query_scalar("SELECT environment_pause_json FROM project WHERE id = 'p'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(detail.is_none());
    pool.close().await;
    fs::remove_dir_all(migration_dir).unwrap();
}

#[tokio::test]
async fn environment_pause_cas_preserves_user_and_repository_pauses() {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    let now = db::now_rfc3339();
    let project = ProjectRepo::create(
        &db,
        CreateProject {
            id: db::new_uuid_v4(),
            name: "Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    ProjectRepo::set_paused_at(&db, &project.id, Some(now.clone()))
        .await
        .unwrap();
    assert!(!ProjectRepo::set_environment_pause_if_unchanged(
        &db,
        &project.id,
        project.version,
        &now,
        "{}"
    )
    .await
    .unwrap());
    let user_paused = ProjectRepo::get_by_id(&db, &project.id)
        .await
        .unwrap()
        .unwrap();
    assert!(!ProjectRepo::set_environment_pause_if_unchanged(
        &db,
        &project.id,
        user_paused.version,
        &now,
        "{}"
    )
    .await
    .unwrap());
    assert!(user_paused.system_pause_reason.is_none());
    assert!(user_paused.environment_pause_json.is_none());
    ProjectRepo::set_paused_at(&db, &project.id, None)
        .await
        .unwrap();
    let active = ProjectRepo::get_by_id(&db, &project.id)
        .await
        .unwrap()
        .unwrap();
    assert!(ProjectRepo::set_system_pause_reason_if_unchanged(
        &db,
        &project.id,
        active.version,
        None,
        false,
        &now,
        "missing_repository"
    )
    .await
    .unwrap());
    let repository_paused = ProjectRepo::get_by_id(&db, &project.id)
        .await
        .unwrap()
        .unwrap();
    assert!(!ProjectRepo::set_environment_pause_if_unchanged(
        &db,
        &project.id,
        repository_paused.version,
        &now,
        "{}"
    )
    .await
    .unwrap());
    assert!(!ProjectRepo::update_environment_pause_if_unchanged(
        &db,
        &project.id,
        repository_paused.version,
        &now,
        "{}"
    )
    .await
    .unwrap());
    let current = ProjectRepo::get_by_id(&db, &project.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current, repository_paused);
}
