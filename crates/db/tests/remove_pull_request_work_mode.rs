use db::{create_sqlite_pool, run_migrations, RepoRepo, SqliteDb};
use sqlx::Row;
use std::{fs, path::Path};

const MIGRATION: &str =
    include_str!("../migrations/V202610010520__remove_pull_request_work_mode.sql");

#[tokio::test]
async fn migration_preserves_digest_provenance_and_scrubs_legacy_tokens() {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let mut migrations = fs::read_dir(source)
        .expect("migration directory reads")
        .map(|entry| entry.expect("migration entry reads").path())
        .filter_map(|path| {
            let name = path.file_name()?.to_str()?;
            let version: i64 = name.strip_prefix('V')?.split_once("__")?.0.parse().ok()?;
            (version < 202610010520).then_some((version, path))
        })
        .collect::<Vec<_>>();
    migrations.sort_by_key(|(version, _)| *version);
    for (_, path) in migrations {
        let sql = fs::read_to_string(&path).expect("historical migration reads");
        sqlx::raw_sql(&sql)
            .execute(&pool)
            .await
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    }

    sqlx::query(
        "INSERT INTO project (id, name, created_at, updated_at)
         VALUES ('project', 'Project', '2026-09-30T12:00:00Z', '2026-09-30T12:00:00Z')",
    )
    .execute(&pool)
    .await
    .expect("project inserts");
    sqlx::query(
        "INSERT INTO repo (
            id, project_id, name, remote_url, local_path, work_mode,
            default_branch, created_at, updated_at
         ) VALUES (
            'repo', 'project', 'Repository', 'https://example.test/repo.git',
            '/srv/repo', 'pull_request', 'release',
            '2026-09-30T12:01:00Z', '2026-09-30T12:02:00Z'
         )",
    )
    .execute(&pool)
    .await
    .expect("pull-request repository inserts");
    sqlx::query(
        "INSERT INTO repo (
            id, project_id, name, remote_url, local_path, work_mode,
            default_branch, created_at, updated_at
         ) VALUES (
            'repo-direct', 'project', 'Direct Repository',
            'https://example.test/direct.git', '/srv/direct', 'direct_merge', 'main',
            '2026-09-30T12:01:00Z', '2026-09-30T12:02:00Z'
         )",
    )
    .execute(&pool)
    .await
    .expect("direct-merge repository inserts");
    sqlx::query(
        "INSERT INTO pr_provider_config (
            id, repo_id, provider_type, token_secret_ref, created_at, updated_at
         ) VALUES (
            'provider', 'repo', 'github', 'raw-token-value',
            '2026-09-30T12:01:00Z', '2026-09-30T12:02:00Z'
         )",
    )
    .execute(&pool)
    .await
    .expect("legacy provider configuration inserts");

    let before = sqlx::query(
        "SELECT id, project_id, name, remote_url, local_path, default_branch, created_at, updated_at
         FROM repo WHERE id = 'repo'",
    )
    .fetch_one(&pool)
    .await
    .expect("repository loads before migration");
    let before = (
        before.get::<String, _>("id"),
        before.get::<String, _>("project_id"),
        before.get::<String, _>("name"),
        before.get::<Option<String>, _>("remote_url"),
        before.get::<Option<String>, _>("local_path"),
        before.get::<String, _>("default_branch"),
        before.get::<String, _>("created_at"),
        before.get::<String, _>("updated_at"),
    );

    sqlx::raw_sql(MIGRATION)
        .execute(&pool)
        .await
        .expect("work-mode migration applies");

    let after = sqlx::query(
        "SELECT id, project_id, name, remote_url, local_path, work_mode,
                default_branch, created_at, updated_at
         FROM repo WHERE id = 'repo'",
    )
    .fetch_one(&pool)
    .await
    .expect("repository loads after migration");
    assert_eq!(after.get::<String, _>("work_mode"), "pull_request");
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT work_mode FROM repo WHERE id = 'repo-direct'")
            .fetch_one(&pool)
            .await
            .expect("direct-merge provenance loads"),
        "direct_merge"
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT token_secret_ref FROM pr_provider_config WHERE id = 'provider'",
        )
        .fetch_one(&pool)
        .await
        .expect("scrubbed token loads"),
        None
    );
    assert_eq!(
        before,
        (
            after.get::<String, _>("id"),
            after.get::<String, _>("project_id"),
            after.get::<String, _>("name"),
            after.get::<Option<String>, _>("remote_url"),
            after.get::<Option<String>, _>("local_path"),
            after.get::<String, _>("default_branch"),
            after.get::<String, _>("created_at"),
            after.get::<String, _>("updated_at"),
        )
    );
}

#[tokio::test]
async fn repository_delete_removes_legacy_provider_configuration() {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations run");
    sqlx::query(
        "INSERT INTO project (id, name, created_at, updated_at)
         VALUES ('project', 'Project', '2026-09-30T12:00:00Z', '2026-09-30T12:00:00Z')",
    )
    .execute(&pool)
    .await
    .expect("project inserts");
    sqlx::query(
        "INSERT INTO repo (
            id, project_id, name, remote_url, work_mode, default_branch, created_at, updated_at
         ) VALUES (
            'repo', 'project', 'Repository', 'https://example.test/repo.git',
            'pull_request', 'main', '2026-09-30T12:01:00Z', '2026-09-30T12:02:00Z'
         )",
    )
    .execute(&pool)
    .await
    .expect("repository inserts");
    sqlx::query(
        "INSERT INTO pr_provider_config (
            id, repo_id, provider_type, token_secret_ref, created_at, updated_at
         ) VALUES (
            'provider', 'repo', 'github', NULL,
            '2026-09-30T12:01:00Z', '2026-09-30T12:02:00Z'
         )",
    )
    .execute(&pool)
    .await
    .expect("legacy provider configuration inserts");

    let db = SqliteDb::new(pool.clone());
    RepoRepo::delete(&db, "repo")
        .await
        .expect("repository deletes with legacy provider configuration");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM pr_provider_config WHERE repo_id = 'repo'"
        )
        .fetch_one(&pool)
        .await
        .expect("provider configuration count loads"),
        0
    );
}
