//! Bundled migrations, progress pruning, and migration runner safeguards.

use super::*;

#[tokio::test]
async fn v146_prunes_progress_history_with_indexed_cascades() {
    let (_fixture, migration_dir, pool) = v145_file_backed_database().await;
    let mut tx = pool.begin().await.unwrap();
    sqlx::raw_sql(
        "WITH RECURSIVE numbers(n) AS (
             VALUES(1) UNION ALL SELECT n + 1 FROM numbers WHERE n < 6000
         ), kinds(kind, event_type) AS (
             VALUES('progress', 'execution.progressed'), ('other', 'execution.started')
         )
         INSERT INTO domain_event (
             id, event_type, entity_type, entity_id, actor_type,
             scope_type, scope_id, correlation_id, dedupe_key, created_at
         )
         SELECT 'v146-' || kind || '-' || n, event_type, 'execution', 'execution',
                'system', 'system', 'system', 'correlation', kind || '-' || n, 'now'
         FROM numbers CROSS JOIN kinds;

         WITH consumers(name) AS (VALUES('a'), ('b'), ('c'), ('d'), ('e'))
         INSERT INTO event_projection_receipt (consumer_name, event_id, dedupe_key, processed_at)
         SELECT name, id, id, 'now' FROM domain_event CROSS JOIN consumers;
         INSERT INTO event_processing_lease (
             consumer_name, event_sequence, lease_owner, leased_until, updated_at
         )
         SELECT 'a', sequence, 'owner', 'later', 'now' FROM domain_event;

         INSERT INTO attention_projection (
             id, attention_type, scope_type, scope_id, source_event_id,
             priority, summary, dedupe_key, occurred_at, updated_at
         )
         SELECT id, 'test', 'system', 'system', id, 0, 'retained attention', id, 'now', 'now'
         FROM domain_event WHERE event_type = 'execution.started' OR id = 'v146-progress-1';
         INSERT INTO command_receipt (
             id, principal_type, principal_id, scope_type, scope_id, operation,
             idempotency_key, input_digest, policy_result, correlation_id,
             event_id, outcome_json, committed_at
         )
         SELECT id, 'system', 'owner', 'account', 'account', 'test', id, 'digest',
                'allowed', 'correlation', id, '{}', 'now'
         FROM domain_event WHERE event_type = 'execution.started' OR id = 'v146-progress-2';
         INSERT INTO agent_wake_disposition (
             id, consumer_name, source_event_id, source_event_sequence,
             attempt_number, max_attempts, disposition, reason, created_at, updated_at
         )
         SELECT id, 'wake', id, sequence, 1, 1, 'deterministically_suppressed',
                'retained wake', 'now', 'now'
         FROM domain_event
         WHERE event_type = 'execution.started' OR id IN ('v146-progress-3', 'v146-progress-4');
         INSERT INTO agent_wake_disposition_current (
             consumer_name, source_event_id, disposition_id, attempt_number, updated_at
         )
         SELECT consumer_name, source_event_id, id, 1, 'now' FROM agent_wake_disposition
         WHERE source_event_id != 'v146-progress-3';
         INSERT INTO memory_item_legacy (
             id, scope_type, scope_id, source_event_id, source_type, kind, title, body, created_at
         )
         SELECT id, 'project', 'project', id, 'event', 'observation', 'Memory', 'kept body', 'now'
         FROM domain_event
         WHERE event_type = 'execution.started' OR id IN ('v146-progress-1', 'v146-progress-5');",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let schema_before: Vec<(String, Option<String>)> =
        sqlx::query_as("SELECT name, sql FROM sqlite_schema ORDER BY name")
            .fetch_all(&pool)
            .await
            .unwrap();
    let patched_sql = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("migrations")
            .join(V146_FILENAME),
    )
    .unwrap();
    assert!(
        patched_sql.contains(ORIGINAL_V146_SQL),
        "original DELETE must stay byte-for-byte unchanged"
    );
    fs::write(migration_dir.join(V146_FILENAME), patched_sql).unwrap();
    let started = Instant::now();
    run_migrations_from(&pool, &migration_dir).await.unwrap();
    let elapsed = started.elapsed();
    eprintln!("V146 upgrade: {elapsed:?} (6,000 progress events, 60,000 receipts)");
    assert!(
        elapsed < Duration::from_secs(5),
        "V146 took {elapsed:?}; event child lookups must use indexes"
    );

    let retained_progress: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM domain_event WHERE event_type = 'execution.progressed' ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        retained_progress,
        [
            "v146-progress-1",
            "v146-progress-2",
            "v146-progress-3",
            "v146-progress-4"
        ]
    );
    for (table, expected) in [
        ("domain_event", 6004),
        ("event_projection_receipt", 30_020),
        ("event_processing_lease", 6004),
        ("attention_projection", 6001),
        ("command_receipt", 6001),
        ("agent_wake_disposition", 6002),
        ("agent_wake_disposition_current", 6001),
        ("memory_item_legacy", 6002),
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, expected, "{table} retains exactly its expected rows");
    }
    let memory: Vec<(String, Option<String>, String)> = sqlx::query_as(
        "SELECT id, source_event_id, body FROM memory_item_legacy
         WHERE id LIKE 'v146-progress-%' ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        memory,
        vec![
            (
                "v146-progress-1".into(),
                Some("v146-progress-1".into()),
                "kept body".into()
            ),
            ("v146-progress-5".into(), None, "kept body".into()),
        ]
    );
    let schema_after: Vec<(String, Option<String>)> =
        sqlx::query_as("SELECT name, sql FROM sqlite_schema ORDER BY name")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        schema_before, schema_after,
        "temporary indexes leave no schema changes"
    );
    assert!(sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&pool)
        .await
        .unwrap()
        .is_empty());
    pool.close().await;
}

#[tokio::test]
async fn v146_already_applied_original_sql_is_skipped_by_patched_bundle() {
    let (_fixture, migration_dir, pool) = v145_file_backed_database().await;
    let insert_progress = "INSERT INTO domain_event (
        id, event_type, entity_type, entity_id, actor_type,
        scope_type, scope_id, correlation_id, created_at
    ) VALUES (?, 'execution.progressed', 'execution', 'execution', 'system',
              'system', 'system', 'correlation', 'now')";
    sqlx::query(insert_progress)
        .bind("before-original")
        .execute(&pool)
        .await
        .unwrap();
    fs::write(migration_dir.join(V146_FILENAME), ORIGINAL_V146_SQL).unwrap();
    run_migrations_from(&pool, &migration_dir).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event WHERE id = 'before-original'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    let original_record: (i64, String, String) =
        sqlx::query_as("SELECT version, name, applied_at FROM _migration WHERE version = 146")
            .fetch_one(&pool)
            .await
            .unwrap();
    sqlx::query(insert_progress)
        .bind("after-original")
        .execute(&pool)
        .await
        .unwrap();

    // A populated V146 database must use the production replay path, including
    // its version/name identity check, rather than a fresh test template.
    run_migrations(&pool)
        .await
        .expect("patched embedded bundle accepts old V146");
    let current_record: (i64, String, String) =
        sqlx::query_as("SELECT version, name, applied_at FROM _migration WHERE version = 146")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(original_record, current_record);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event WHERE id = 'after-original'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1,
        "V146 must not delete a newly inserted progress event on restart"
    );
    pool.close().await;
}

#[tokio::test]
async fn file_backed_migrations_apply_cleanly() {
    let db_path = unique_temp_path("migtest").with_extension("db");
    let _ = std::fs::remove_file(&db_path);
    let url = format!("sqlite://{}", db_path.display());
    let pool = create_sqlite_pool(&url).await.expect("pool");
    // Test builds copy a migrated snapshot into a fresh database, so replay
    // the bundled directory explicitly: this is the one place every migration
    // body runs against a real file with a multi-connection WAL pool.
    let migrations = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    run_migrations_from(&pool, &migrations)
        .await
        .expect("migrations");
    let applied: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _migration")
        .fetch_one(&pool)
        .await
        .expect("history loads");
    let bundled = fs::read_dir(&migrations)
        .expect("migration directory reads")
        .filter(|entry| {
            entry
                .as_ref()
                .is_ok_and(|entry| entry.path().extension().is_some_and(|ext| ext == "sql"))
        })
        .count() as i64;
    assert_eq!(applied, bundled);
    assert_eq!(
        fs::metadata(format!("{}-wal", db_path.display()))
            .unwrap()
            .len(),
        0
    );
    pool.close().await;
    let _ = fs::remove_file(db_path);
}

#[tokio::test]
async fn comment_rebuild_bundle_truncates_wal_and_preserves_comment_content() {
    let migration_dir = unique_temp_path("comment-checkpoint-migrations");
    fs::create_dir_all(&migration_dir).unwrap();
    copy_migrations_up_to(202610122352, &migration_dir);
    let db_path = unique_temp_path("comment-checkpoint-db").with_extension("db");
    let pool = create_sqlite_pool(&format!("sqlite://{}", db_path.display()))
        .await
        .unwrap();
    run_migrations_from(&pool, &migration_dir).await.unwrap();
    sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES('p','p','now','now')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t','p','t','done','now','now')").execute(&pool).await.unwrap();
    let content = "preserved Unicode: 数据\n".repeat(65_536);
    sqlx::query("INSERT INTO task_comment(id,task_id,author_type,author_name,content,created_at,updated_at,worklog_kind,idempotency_key) VALUES('c','t','system','Forge',?,'now','now','progress','retained')")
        .bind(&content).execute(&pool).await.unwrap();
    run_migrations_from(
        &pool,
        Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations"),
    )
    .await
    .unwrap();
    assert_eq!(
        fs::metadata(format!("{}-wal", db_path.display()))
            .unwrap()
            .len(),
        0
    );
    let retained: (String, String, String) = sqlx::query_as(
        "SELECT content,worklog_kind,idempotency_key FROM task_comment WHERE id='c'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(retained, (content, "progress".into(), "retained".into()));
    let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(integrity, "ok");
    pool.close().await;
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(migration_dir);
}

#[tokio::test]
async fn migration_runner_refuses_two_files_with_the_same_version() {
    let dir = unique_temp_path("duplicate-version-migrations");
    fs::create_dir_all(&dir).expect("temp migration dir creates");
    write_migration(
        &dir,
        "V001__first.sql",
        "CREATE TABLE first_table (id TEXT);",
    );
    write_migration(&dir, "V002__left.sql", "CREATE TABLE left_table (id TEXT);");
    write_migration(
        &dir,
        "V002__right.sql",
        "CREATE TABLE right_table (id TEXT);",
    );
    let (pool, db_path) = migration_test_pool("duplicate-version-db").await;

    let error = run_migrations_from(&pool, &dir)
        .await
        .expect_err("two V002 files are refused");

    assert!(
        matches!(
            error,
            db::DbError::DuplicateMigrationVersion { version: 2, .. }
        ),
        "unexpected error: {error}"
    );
    assert!(
        !table_exists(&pool, "first_table").await,
        "nothing runs before the bundle is validated"
    );
    pool.close().await;
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(dir);
}

#[tokio::test]
async fn migration_runner_refuses_a_version_applied_under_another_name() {
    let applied_dir = unique_temp_path("applied-name-migrations");
    fs::create_dir_all(&applied_dir).expect("temp migration dir creates");
    write_migration(
        &applied_dir,
        "V001__base.sql",
        "CREATE TABLE base_table (id TEXT);",
    );
    write_migration(
        &applied_dir,
        "V002__hotfix_feature.sql",
        "CREATE TABLE hotfix_table (id TEXT);",
    );
    let (pool, db_path) = migration_test_pool("applied-name-db").await;
    run_migrations_from(&pool, &applied_dir)
        .await
        .expect("hotfix build migrates");

    let release_dir = unique_temp_path("release-name-migrations");
    fs::create_dir_all(&release_dir).expect("temp migration dir creates");
    write_migration(
        &release_dir,
        "V001__base.sql",
        "CREATE TABLE base_table (id TEXT);",
    );
    write_migration(
        &release_dir,
        "V002__release_feature.sql",
        "CREATE TABLE release_table (id TEXT);",
    );
    let error = run_migrations_from(&pool, &release_dir)
        .await
        .expect_err("a different V002 is refused instead of skipped");

    match error {
        db::DbError::AppliedMigrationMismatch {
            version,
            applied,
            bundled,
        } => {
            assert_eq!(version, 2);
            assert_eq!(applied, "hotfix_feature");
            assert_eq!(bundled, "release_feature");
        }
        other => panic!("unexpected error: {other}"),
    }
    assert!(!table_exists(&pool, "release_table").await);
    pool.close().await;
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(applied_dir);
    let _ = fs::remove_dir_all(release_dir);
}

#[tokio::test]
async fn migration_runner_refuses_newer_database_before_pending_migration() {
    let (pool, db_path) = migration_test_pool("newer-than-build-db").await;
    run_migrations(&pool)
        .await
        .expect("embedded bundle migrates");
    let pending_version: i64 = sqlx::query_scalar("SELECT MAX(version) FROM _migration")
        .fetch_one(&pool)
        .await
        .expect("latest bundled version loads");
    sqlx::query("DELETE FROM _migration WHERE version = ?")
        .bind(pending_version)
        .execute(&pool)
        .await
        .expect("pending state creates");
    let future_version = pending_version + 1;
    sqlx::query(
        "INSERT INTO _migration (version, name, applied_at)
         VALUES (?, 'future_schema', 'future')",
    )
    .bind(future_version)
    .execute(&pool)
    .await
    .expect("future migration records");

    let error = run_migrations(&pool)
        .await
        .expect_err("an older embedded bundle must refuse the database");
    match error {
        db::DbError::DatabaseNewerThanBuild { unknown } => {
            assert_eq!(unknown, vec![(future_version, "future_schema".to_owned())]);
        }
        other => panic!("unexpected error: {other}"),
    }
    let pending_still_missing: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM _migration WHERE version = ?")
            .bind(pending_version)
            .fetch_one(&pool)
            .await
            .expect("pending migration history reads");
    assert_eq!(pending_still_missing, 0, "nothing pending may run");

    pool.close().await;
    let _ = fs::remove_file(db_path);
}

#[tokio::test]
async fn timestamp_migration_merged_after_a_newer_one_still_runs() {
    let dir = unique_temp_path("timestamp-version-migrations");
    fs::create_dir_all(&dir).expect("temp migration dir creates");
    write_migration(&dir, "V001__base.sql", "CREATE TABLE base_table (id TEXT);");
    write_migration(
        &dir,
        "V202610011200__landed_first.sql",
        "CREATE TABLE landed_first (id TEXT);",
    );
    let (pool, db_path) = migration_test_pool("timestamp-version-db").await;
    run_migrations_from(&pool, &dir)
        .await
        .expect("first release migrates");

    // A branch written earlier merges later with an older timestamp.
    write_migration(
        &dir,
        "V202610010900__landed_second.sql",
        "CREATE TABLE landed_second (id TEXT);",
    );
    run_migrations_from(&pool, &dir)
        .await
        .expect("late branch migrates");

    assert!(table_exists(&pool, "landed_second").await);
    let applied: Vec<i64> = sqlx::query_scalar("SELECT version FROM _migration ORDER BY version")
        .fetch_all(&pool)
        .await
        .expect("applied versions read");
    assert_eq!(applied, vec![1, 202_610_010_900, 202_610_011_200]);
    pool.close().await;
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(dir);
}

/// The bundle comment that makes Cargo rebuild the embedded migrations names
/// one head revision, the newest migration file.
#[test]
fn embedded_bundle_comment_has_one_head_naming_the_newest_migration() {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let newest = fs::read_dir(&source)
        .unwrap()
        .filter_map(|entry| migration_version(entry.unwrap().file_name().to_str().unwrap()))
        .max()
        .unwrap();
    let heads: Vec<&str> = include_str!("../../../src/migration.rs")
        .lines()
        .filter(|line| line.contains("Embedded migration bundle revision:"))
        .collect();
    assert_eq!(heads.len(), 1, "one bundle revision comment: {heads:?}");
    assert!(
        heads[0].starts_with(&format!(
            "// Embedded migration bundle revision: V{newest} "
        )),
        "the bundle revision names V{newest}: {}",
        heads[0]
    );
}
