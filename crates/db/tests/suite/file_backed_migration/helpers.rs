//! Shared fixtures for file-backed migrations tests.

use super::*;

pub(super) const ORIGINAL_V146_SQL: &str = include_str!("../../fixtures/v146_original.sql");

pub(super) const V146_FILENAME: &str = "V146__drop_execution_progress_events.sql";

pub(super) async fn v145_file_backed_database() -> (tempfile::TempDir, PathBuf, sqlx::SqlitePool) {
    let fixture = tempfile::tempdir().expect("fixture directory creates");
    let migration_dir = fixture.path().join("migrations");
    fs::create_dir(&migration_dir).unwrap();
    copy_migrations_up_to(145, &migration_dir);
    let pool = create_sqlite_pool(&format!(
        "sqlite://{}",
        fixture.path().join("forge.db").display()
    ))
    .await
    .unwrap();
    run_migrations_from(&pool, &migration_dir).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    (fixture, migration_dir, pool)
}

pub(super) fn unique_temp_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("forge-{name}-{}-{nanos}", std::process::id()))
}

pub(super) async fn migrate_wake_cursor_from_v087(initial_sequence: i64) -> (i64, i64, i64) {
    let migration_dir = unique_temp_path("wake-cutover-migrations");
    fs::create_dir_all(&migration_dir).expect("temp migration dir creates");
    copy_migrations_up_to(87, &migration_dir);

    let db_path = unique_temp_path("wake-cutover-db").with_extension("db");
    let url = format!("sqlite://{}", db_path.display());
    let pool = create_sqlite_pool(&url).await.expect("pool");
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("pre-V088 migrations apply");

    for (id, dedupe_key) in [
        ("wake-cutover-event-1", "wake-cutover-dedupe-1"),
        ("wake-cutover-event-2", "wake-cutover-dedupe-2"),
    ] {
        sqlx::query(
            "INSERT INTO domain_event (
                id, event_type, entity_type, entity_id, actor_type,
                scope_type, scope_id, correlation_id, causation_depth,
                dedupe_key, payload_json, created_at
             ) VALUES (?, 'wake.test', 'project', 'wake-cutover-project',
                       'system', 'project', 'wake-cutover-project', ?, 0, ?, '{}', ?)",
        )
        .bind(id)
        .bind(id)
        .bind(dedupe_key)
        .bind("2026-08-21T00:00:00Z")
        .execute(&pool)
        .await
        .expect("pre-cutover event inserts");
    }
    let cutover_max: i64 = sqlx::query_scalar("SELECT MAX(sequence) FROM domain_event")
        .fetch_one(&pool)
        .await
        .expect("pre-cutover sequence loads");
    sqlx::query(
        "INSERT INTO event_consumer_cursor (
            consumer_name, last_sequence, version, updated_at
         ) VALUES ('agent-wake-turns', ?, 7, '2026-08-21T00:00:00Z')",
    )
    .bind(initial_sequence)
    .execute(&pool)
    .await
    .expect("legacy wake cursor inserts");

    run_migrations(&pool).await.expect("V088 applies");
    let cursor: (i64, i64) = sqlx::query_as(
        "SELECT last_sequence, version
         FROM event_consumer_cursor
         WHERE consumer_name = 'agent-wake-turns'",
    )
    .fetch_one(&pool)
    .await
    .expect("wake cursor loads");
    let cutover: i64 = sqlx::query_scalar(
        "SELECT cutover_sequence
         FROM event_consumer_cutover
         WHERE consumer_name = 'agent-wake-turns'",
    )
    .fetch_one(&pool)
    .await
    .expect("wake cutover loads");

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(migration_dir);
    assert_eq!(cutover, cutover_max);
    (cursor.0, cursor.1, cutover)
}

pub(super) fn copy_migrations_up_to(max_version: i64, destination: &Path) {
    let source_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    for entry in fs::read_dir(source_dir).expect("migration dir reads") {
        let entry = entry.expect("migration entry reads");
        let path = entry.path();
        let Some(filename) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(version) = migration_version(filename) else {
            continue;
        };
        if version <= max_version {
            fs::copy(&path, destination.join(filename)).expect("migration copies");
        }
    }
}

pub(super) fn migration_version(filename: &str) -> Option<i64> {
    filename.strip_prefix('V')?.split_once("__")?.0.parse().ok()
}

pub(super) async fn merged_chat_id_for(pool: &db::SqlitePool, project_id: &str) -> String {
    sqlx::query_scalar("SELECT id FROM agent_chat WHERE kind = 'project' AND project_id = ?")
        .bind(project_id)
        .fetch_one(pool)
        .await
        .expect("Project Chat id lookup")
}

pub(super) async fn migration_test_pool(name: &str) -> (sqlx::SqlitePool, PathBuf) {
    let db_path = unique_temp_path(name).with_extension("db");
    let url = format!("sqlite://{}", db_path.display());
    (create_sqlite_pool(&url).await.expect("pool"), db_path)
}

pub(super) fn write_migration(dir: &Path, filename: &str, sql: &str) {
    fs::write(dir.join(filename), sql).expect("migration file writes");
}

pub(super) async fn table_exists(pool: &sqlx::SqlitePool, table: &str) -> bool {
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
    )
    .bind(table)
    .fetch_one(pool)
    .await
    .expect("sqlite_master reads")
        > 0
}
