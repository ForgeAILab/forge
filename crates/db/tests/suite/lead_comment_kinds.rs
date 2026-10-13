//! V202610130038 rebuilds `task_comment` for the two comment kinds a
//! coordination root's lead writes. Every existing row and column value
//! survives, the three indexes are there again, and the new kinds (and only
//! known kinds) are accepted.
use db::{create_sqlite_pool, run_migrations_from};
use sqlx::Row;
use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

/// The migration before this one in the bundle.
const BEFORE: i64 = 202610122352;
const THIS: i64 = 202610130038;

fn temp(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("forge-{name}-{}-{nanos}", std::process::id()))
}

fn copy_up_to(max_version: i64, destination: &Path) {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    for entry in fs::read_dir(source).expect("migration dir reads") {
        let path = entry.expect("migration entry reads").path();
        let Some(filename) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let version = filename
            .strip_prefix('V')
            .and_then(|rest| rest.split_once("__"))
            .and_then(|(version, _)| version.parse::<i64>().ok());
        if version.is_some_and(|version| version <= max_version) {
            fs::copy(&path, destination.join(filename)).expect("migration copies");
        }
    }
}

async fn rows(pool: &db::SqlitePool) -> Vec<String> {
    sqlx::query(
        "SELECT id||'|'||task_id||'|'||author_type||'|'||COALESCE(author_id,'-')||'|'||author_name||'|'||content||'|'||created_at||'|'||updated_at||'|'||COALESCE(execution_id,'-')||'|'||COALESCE(role,'-')||'|'||COALESCE(worklog_kind,'-')||'|'||COALESCE(idempotency_key,'-') AS line
         FROM task_comment ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("comments read")
    .into_iter()
    .map(|row| row.get::<String, _>("line"))
    .collect()
}

/// The shape of `task_comment` that must not change: columns (order, type,
/// NOT NULL, default, primary key), foreign keys, and every index and trigger
/// with its definition. The table's own SQL is left out: its CHECK is what
/// the migration changes.
async fn shape(pool: &db::SqlitePool) -> Vec<String> {
    let mut lines = Vec::new();
    for row in sqlx::query("PRAGMA table_info(task_comment)")
        .fetch_all(pool)
        .await
        .expect("columns read")
    {
        lines.push(format!(
            "column {} {} {} notnull={} default={:?} pk={}",
            row.get::<i64, _>("cid"),
            row.get::<String, _>("name"),
            row.get::<String, _>("type"),
            row.get::<i64, _>("notnull"),
            row.get::<Option<String>, _>("dflt_value"),
            row.get::<i64, _>("pk"),
        ));
    }
    let mut keys = Vec::new();
    for row in sqlx::query("PRAGMA foreign_key_list(task_comment)")
        .fetch_all(pool)
        .await
        .expect("foreign keys read")
    {
        keys.push(format!(
            "fk {}.{} -> {}.{} on_delete={}",
            "task_comment",
            row.get::<String, _>("from"),
            row.get::<String, _>("table"),
            row.get::<Option<String>, _>("to").unwrap_or_default(),
            row.get::<String, _>("on_delete"),
        ));
    }
    keys.sort();
    lines.extend(keys);
    for row in sqlx::query(
        "SELECT type, name, COALESCE(sql, '') AS sql FROM sqlite_master
         WHERE tbl_name='task_comment' AND type IN ('index', 'trigger') ORDER BY type, name",
    )
    .fetch_all(pool)
    .await
    .expect("indexes and triggers read")
    {
        lines.push(format!(
            "{} {} {}",
            row.get::<String, _>("type"),
            row.get::<String, _>("name"),
            row.get::<String, _>("sql")
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" "),
        ));
    }
    // Nothing else in the database refers to the table by name.
    let referrers: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE tbl_name <> 'task_comment' AND sql LIKE '%task_comment%'",
    )
    .fetch_one(pool)
    .await
    .expect("referrers read");
    lines.push(format!("referrers {referrers}"));
    lines
}

#[tokio::test]
async fn the_lead_comment_kinds_migration_keeps_every_comment_and_its_indexes() {
    let before_dir = temp("lead-kinds-before");
    let all_dir = temp("lead-kinds-all");
    fs::create_dir_all(&before_dir).expect("dir creates");
    fs::create_dir_all(&all_dir).expect("dir creates");
    copy_up_to(BEFORE, &before_dir);
    copy_up_to(THIS, &all_dir);
    let path = temp("lead-kinds-db").with_extension("db");
    let pool = create_sqlite_pool(&format!("sqlite://{}", path.display()))
        .await
        .expect("pool");
    run_migrations_from(&pool, &before_dir)
        .await
        .expect("the bundle before this migration applies");

    let now = "2026-10-12T00:00:00Z";
    sqlx::query("INSERT INTO project (id, name, settings, workflow_definition, created_at, updated_at) VALUES ('project', 'P', '{}', '{}', ?, ?)")
        .bind(now).bind(now).execute(&pool).await.expect("project inserts");
    sqlx::query("INSERT INTO task (id, project_id, title, task_type, status, created_at, updated_at) VALUES ('task', 'project', 'T', 'task', 'todo', ?, ?)")
        .bind(now).bind(now).execute(&pool).await.expect("task inserts");
    sqlx::query("INSERT INTO execution (id, task_id, role, status, created_at, updated_at) VALUES ('execution', 'task', 'coder', 'completed', ?, ?)")
        .bind(now).bind(now).execute(&pool).await.expect("execution inserts");
    for (id, author, author_id, execution, role, kind, key) in [
        ("c1", "user", None, None, None, None, None),
        (
            "c2",
            "agent",
            Some("agent"),
            Some("execution"),
            Some("coder"),
            Some("progress"),
            Some("key-1"),
        ),
        (
            "c3",
            "agent",
            Some("agent"),
            Some("execution"),
            Some("coder"),
            Some("blocker"),
            Some("key-2"),
        ),
        (
            "c4",
            "system",
            None,
            None,
            None,
            None,
            Some("review-result:1"),
        ),
        (
            "c5",
            "agent",
            Some("agent"),
            None,
            Some("reviewer"),
            Some("validation"),
            None,
        ),
        (
            "c6",
            "agent",
            None,
            Some("execution"),
            Some("coder"),
            Some("decision"),
            None,
        ),
    ] {
        sqlx::query("INSERT INTO task_comment (id, task_id, author_type, author_id, author_name, content, execution_id, role, worklog_kind, idempotency_key, created_at, updated_at) VALUES (?, 'task', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(id).bind(author).bind(author_id).bind(format!("name of {id}")).bind(format!("content of {id}\nsecond line | with a bar"))
            .bind(execution).bind(role).bind(kind).bind(key).bind(now).bind("2026-10-12T00:00:01Z")
            .execute(&pool).await.expect("comment inserts");
    }
    // Before the migration the two kinds are refused.
    assert!(sqlx::query("INSERT INTO task_comment (id, task_id, author_type, author_name, content, worklog_kind, created_at, updated_at) VALUES ('early', 'task', 'agent', 'lead', 'note', 'handoff', ?, ?)")
        .bind(now).bind(now).execute(&pool).await.is_err());
    // A long body, and a comment of a Task that was deleted (soft: its
    // comments stay; a hard delete cascades and leaves none to migrate).
    sqlx::query("INSERT INTO task (id, project_id, title, task_type, status, deleted_at, created_at, updated_at) VALUES ('gone', 'project', 'G', 'task', 'todo', ?, ?, ?)")
        .bind(now).bind(now).bind(now).execute(&pool).await.expect("deleted task inserts");
    sqlx::query("INSERT INTO task_comment (id, task_id, author_type, author_name, content, created_at, updated_at) VALUES ('c7', 'gone', 'user', 'owner', ?, ?, ?)")
        .bind("long \u{e9}\u{4e2d} ".repeat(40_000)).bind(now).bind(now).execute(&pool).await.expect("long comment inserts");
    let before = rows(&pool).await;
    assert_eq!(before.len(), 7);
    let shape_before = shape(&pool).await;
    assert!(
        shape_before.contains(&"referrers 0".to_owned()),
        "{shape_before:?}"
    );

    run_migrations_from(&pool, &all_dir)
        .await
        .expect("the migration applies to a database with comments");
    run_migrations_from(&pool, &all_dir)
        .await
        .expect("applying the bundle again changes nothing");
    assert_eq!(
        rows(&pool).await,
        before,
        "every row and column value is kept"
    );

    assert_eq!(
        shape(&pool).await,
        shape_before,
        "columns, foreign keys, indexes and triggers are what they were"
    );
    let ddl: String =
        sqlx::query_scalar("SELECT sql FROM sqlite_master WHERE type='table' AND name='task_comment'")
            .fetch_one(&pool)
            .await
            .expect("table reads");
    assert!(ddl.contains("'handoff', 'request_input'"), "{ddl}");

    let indexes: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='task_comment' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .fetch_all(&pool)
    .await
    .expect("indexes read");
    assert_eq!(
        indexes,
        [
            "idx_task_comment_execution",
            "idx_task_comment_idempotency",
            "idx_task_comment_task_id"
        ]
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM sqlite_master WHERE name='task_comment_new'"
        )
        .fetch_one(&pool)
        .await
        .expect("leftover reads"),
        0
    );
    let violations = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&pool)
        .await
        .expect("foreign keys check");
    assert!(violations.is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
            .fetch_one(&pool)
            .await
            .expect("pragma reads"),
        1,
        "foreign keys are on again"
    );

    // The new kinds are accepted, an unknown kind is not, the idempotency
    // index still refuses a second row for one key, and the foreign keys
    // still act (execution SET NULL, task CASCADE).
    for (id, kind) in [("n1", "handoff"), ("n2", "request_input")] {
        sqlx::query("INSERT INTO task_comment (id, task_id, author_type, author_name, content, execution_id, role, worklog_kind, created_at, updated_at) VALUES (?, 'task', 'agent', 'lead', 'note', 'execution', 'planner', ?, ?, ?)")
            .bind(id).bind(kind).bind(now).bind(now).execute(&pool).await.expect("lead kind inserts");
    }
    assert!(sqlx::query("INSERT INTO task_comment (id, task_id, author_type, author_name, content, worklog_kind, created_at, updated_at) VALUES ('bad', 'task', 'agent', 'lead', 'note', 'escalate', ?, ?)")
        .bind(now).bind(now).execute(&pool).await.is_err());
    assert!(sqlx::query("INSERT INTO task_comment (id, task_id, author_type, author_name, content, idempotency_key, created_at, updated_at) VALUES ('dup', 'task', 'agent', 'coder', 'again', 'key-1', ?, ?)")
        .bind(now).bind(now).execute(&pool).await.is_err());
    sqlx::query("DELETE FROM execution WHERE id='execution'")
        .execute(&pool)
        .await
        .expect("execution deletes");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task_comment WHERE execution_id IS NOT NULL"
        )
        .fetch_one(&pool)
        .await
        .expect("count reads"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_comment")
            .fetch_one(&pool)
            .await
            .expect("count reads"),
        9
    );
    pool.close().await;
    let _ = fs::remove_file(path);
    let _ = fs::remove_dir_all(before_dir);
    let _ = fs::remove_dir_all(all_dir);
}
