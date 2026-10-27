//! Lifecycle repository tests.
use super::*;

#[tokio::test]
async fn repo_deletion_cascades_all_three_evidence_tables() {
    let db = fixture().await;
    complete(
        &db,
        request("one"),
        CheckResultOutcome::Pass,
        CheckCleanup::Success,
    )
    .await;
    WorkspaceRepo::delete(&db, "w").await.unwrap();
    RepoRepo::delete(&db, "r").await.unwrap();
    assert_eq!(counts(&db).await, (0, 0, 0));
}

#[tokio::test]
async fn project_deletion_cascades_all_three_evidence_tables() {
    let db = fixture().await;
    complete(
        &db,
        request("one"),
        CheckResultOutcome::Pass,
        CheckCleanup::Success,
    )
    .await;
    ProjectRepo::delete(&db, "p").await.unwrap();
    assert_eq!(counts(&db).await, (0, 0, 0));
}

#[tokio::test]
async fn task_deletion_nulls_consumers_and_preserves_result_history() {
    let db = fixture().await;
    complete(
        &db,
        request("one"),
        CheckResultOutcome::Pass,
        CheckCleanup::Success,
    )
    .await;
    sqlx::query("DELETE FROM task WHERE id='t'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(counts(&db).await, (1, 1, 1));
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>("SELECT task_id FROM check_consumer")
            .fetch_one(db.pool())
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn workspace_deletion_nulls_run_reference_and_preserves_history() {
    let db = fixture().await;
    let result = complete(
        &db,
        request("one"),
        CheckResultOutcome::Pass,
        CheckCleanup::Success,
    )
    .await;
    WorkspaceRepo::delete(&db, "w").await.unwrap();
    assert_eq!(counts(&db).await, (1, 1, 1));
    assert_eq!(
        db.check_run(&result.run_id)
            .await
            .unwrap()
            .unwrap()
            .workspace_id,
        None
    );
}

#[tokio::test]
async fn machine_removal_and_deletion_do_not_block_and_preserve_attested_identity() {
    let db = fixture().await;
    let result = complete(
        &db,
        request("one"),
        CheckResultOutcome::Pass,
        CheckCleanup::Success,
    )
    .await;
    db.remove_daemon("m", "admin", true, "local-machine", false)
        .await
        .unwrap();
    assert_eq!(counts(&db).await, (1, 1, 1));
    sqlx::query("DELETE FROM daemon WHERE id='m'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(counts(&db).await, (1, 1, 1));
    let run = db.check_run(&result.run_id).await.unwrap().unwrap();
    assert_eq!(run.machine_id, None);
    assert_eq!(run.identity, identity());
}

#[tokio::test]
async fn counts_are_zero_on_a_fresh_database() {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    let counts = db.check_run_counts().await.unwrap();
    assert_eq!(counts.by_state.len(), CheckRunState::ALL.len());
    assert!(counts.by_state.values().all(|n| *n == 0));
    assert_eq!(counts.reusable_results, 0);
}

#[tokio::test]
async fn upgrade_preserves_old_rows_and_never_seeds_cache() {
    let dir = tempfile::tempdir().unwrap();
    let migrations = dir.path().join("migrations");
    std::fs::create_dir(&migrations).unwrap();
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    for entry in std::fs::read_dir(&source).unwrap() {
        let entry = entry.unwrap();
        if entry
            .file_name()
            .to_string_lossy()
            .ends_with("__check_runs.sql")
            || entry
                .file_name()
                .to_string_lossy()
                .ends_with("__check_run_identity.sql")
            || entry
                .file_name()
                .to_string_lossy()
                .ends_with("__check_runner.sql")
            // Triggers on `check_run`: it cannot precede the table either.
            || entry
                .file_name()
                .to_string_lossy()
                .ends_with("__check_slot_release.sql")
            // Adds a column to `check_run`.
            || entry
                .file_name()
                .to_string_lossy()
                .ends_with("__slot_wait_age.sql")
            // Rebuilds an index of `check_consumer`.
            || entry
                .file_name()
                .to_string_lossy()
                .ends_with("__check_consumer_answered_index.sql")
            // Rebuilds `check_consumer` and its triggers.
            || entry
                .file_name()
                .to_string_lossy()
                .ends_with("__review_verify_purpose.sql")
        {
            continue;
        }
        std::fs::copy(entry.path(), migrations.join(entry.file_name())).unwrap();
    }
    let pool = create_sqlite_pool(&format!(
        "sqlite://{}",
        dir.path().join("upgrade.db").display()
    ))
    .await
    .unwrap();
    crate::run_migrations_from(&pool, &migrations)
        .await
        .unwrap();
    let db = SqliteDb::new(pool);
    seed(&db).await;
    sqlx::query("INSERT INTO execution(id,task_id,role,status,workspace_id,created_at,updated_at) VALUES('e','t','executor','completed','w',?,?)").bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,created_at,updated_at) VALUES('old-review','t','e',1,'passed','{\"ci_steps\":[]}',?,?,?)").bind(NOW).bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
    let before = legacy_snapshot(&db).await;
    crate::run_migrations_from(db.pool(), &source)
        .await
        .unwrap();
    assert_eq!(before, legacy_snapshot(&db).await);
    assert_eq!(counts(&db).await, (0, 0, 0));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task WHERE id='t'")
            .fetch_one(db.pool())
            .await
            .unwrap(),
        1
    );
    db.pool().close().await;
}

#[test]
fn mutable_refs_and_abbreviated_objects_are_not_check_identities() {
    let mut input = identity();
    for invalid in [
        "main",
        "refs/heads/main",
        "abc123",
        "",
        &"A".repeat(40),
        &"g".repeat(40),
    ] {
        input.commit_sha = invalid.into();
        assert!(matches!(input.key(), Err(DbError::Check(_))), "{invalid}");
    }
    for valid in ["a".repeat(40), "b".repeat(64)] {
        input.commit_sha = valid;
        assert!(input.key().is_ok());
    }
}
