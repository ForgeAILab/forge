//! Replay the scheduler migration on a database written before it, with
//! queued and claimed steps and running executions in flight: every legacy
//! row is byte-identical afterwards, nothing is marked dirty by the upgrade
//! itself, and a second run is a no-op.
use db::{create_sqlite_pool, new_uuid_v4, run_migrations_from};
use std::path::PathBuf;

const TABLES: &[&str] = &[
    "project",
    "task",
    "task_step",
    "execution",
    "transition_log",
    "task_dependency",
];

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("forge-task-schedule-upgrade")
        .join(format!("{name}-{}", new_uuid_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A fingerprint of every column of every row, in rowid order.
async fn digest(pool: &db::SqlitePool) -> Vec<(String, i64, String)> {
    let mut out = Vec::new();
    for table in TABLES {
        let columns: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT name FROM pragma_table_info('{table}') ORDER BY cid"
        ))
        .fetch_all(pool)
        .await
        .unwrap();
        let expr = columns
            .iter()
            .map(|c| format!("quote({c})"))
            .collect::<Vec<_>>()
            .join("||'|'||");
        let rows: Vec<String> =
            sqlx::query_scalar(&format!("SELECT {expr} FROM {table} ORDER BY rowid"))
                .fetch_all(pool)
                .await
                .unwrap();
        let mut hash: u64 = 1469598103934665603;
        for b in rows.join("\n").bytes() {
            hash ^= b as u64;
            hash = hash.wrapping_mul(1099511628211);
        }
        out.push((
            table.to_string(),
            rows.len() as i64,
            format!("{hash:016x}/{}", columns.len()),
        ));
    }
    out
}

#[tokio::test]
async fn upgrade_keeps_every_legacy_row_and_marks_nothing() {
    let full = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"));
    let base = scratch("base-migrations");
    for entry in std::fs::read_dir(&full).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        if name.as_str() < "V202610060838" {
            std::fs::copy(entry.path(), base.join(&name)).unwrap();
        }
    }
    let db_dir = scratch("upgrade-db");
    let pool = create_sqlite_pool(&format!(
        "sqlite://{}",
        db_dir.join("forge.sqlite").display()
    ))
    .await
    .unwrap();
    run_migrations_from(&pool, &base).await.unwrap();

    let now = "2026-10-06T00:00:00Z";
    sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES ('p','p',?,?)")
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();
    for (i, status) in [
        "todo",
        "in_progress",
        "review",
        "merging",
        "done",
        "in_progress",
    ]
    .iter()
    .enumerate()
    {
        let id = format!("t{i}");
        sqlx::query("INSERT INTO task(id,project_id,parent_task_id,subtask_order,title,status,priority,created_at,updated_at) VALUES (?,'p',?,?,'t',?,?,?,?)")
            .bind(&id)
            .bind((i == 5).then_some("t1"))
            .bind((i == 5).then_some(1))
            .bind(status)
            .bind(i as i64)
            .bind(now)
            .bind(now)
            .execute(&pool)
            .await
            .unwrap();
        if i % 2 == 1 {
            sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at,lease_owner,lease_expires_at) VALUES (?,?,'coder','running',?,?,'worker','2026-10-06T00:01:00Z')")
                .bind(format!("e{i}"))
                .bind(&id)
                .bind(now)
                .bind(now)
                .execute(&pool)
                .await
                .unwrap();
        }
        for (n, kind) in ["hooks", "command"].iter().enumerate() {
            // One queued step per kind; the review Task's first is claimed.
            let claimed = i == 2 && n == 0;
            sqlx::query("INSERT INTO task_step(id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,status,claimed_by,lease_until,available_at,created_at,updated_at) VALUES (?,?,?,?,'{}',?,?,1,?,1,?,?,?,?,?,?)")
                .bind(format!("s{i}-{n}"))
                .bind(&id)
                .bind(n as i64 + 1)
                .bind(kind)
                .bind(format!("s{i}-{n}"))
                .bind(format!("s{i}-{n}"))
                .bind(status)
                .bind(if claimed { "claimed" } else { "pending" })
                .bind(claimed.then_some("old-worker"))
                .bind(claimed.then_some("2026-10-06T00:05:00Z"))
                .bind(now)
                .bind(now)
                .bind(now)
                .execute(&pool)
                .await
                .unwrap();
        }
    }
    sqlx::query("INSERT INTO task_dependency VALUES ('t0','t1',?)")
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE task SET metadata_json='{\"queued_recovery\":{\"x\":1},\"deferred_dispatch\":{\"not_before\":\"2030-01-01T00:00:00Z\"}}', error_annotation='{\"type\":\"merge_conflict\"}' WHERE id='t3'")
        .execute(&pool)
        .await
        .unwrap();

    let before = digest(&pool).await;
    assert_eq!(before[1].1, 6, "six Tasks");
    assert_eq!(before[2].1, 12, "twelve steps in flight");
    run_migrations_from(&pool, &full).await.unwrap();
    assert_eq!(
        before,
        digest(&pool).await,
        "legacy rows are byte-identical after the migration"
    );
    // The upgrade marks nothing: the first lap of the sweep finds every open
    // Task, in pages, behind dispatch.
    for table in [
        "task_schedule_dirty",
        "task_schedule_park",
        "task_schedule_wait",
        "project_schedule_dirty",
    ] {
        let rows: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(rows, 0, "{table}");
    }
    let open: Vec<String> = db::SqliteDb::new(pool.clone())
        .open_schedule_tasks(None, 100)
        .await
        .unwrap();
    assert_eq!(
        open.len(),
        6,
        "every Task is still to be classified: {open:?}"
    );
    let violations: Vec<String> =
        sqlx::query_scalar("SELECT \"table\" FROM pragma_foreign_key_check")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(violations.is_empty(), "{violations:?}");
    let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(integrity, "ok");
    // A second run is a no-op, and the kicks work on the upgraded rows.
    run_migrations_from(&pool, &full).await.unwrap();
    assert_eq!(before, digest(&pool).await);
    sqlx::query("UPDATE task SET status='done', version=version+1 WHERE id='t1'")
        .execute(&pool)
        .await
        .unwrap();
    let mut dirty: Vec<String> =
        sqlx::query_scalar("SELECT task_id FROM task_schedule_dirty WHERE dirty=1")
            .fetch_all(&pool)
            .await
            .unwrap();
    dirty.sort();
    assert_eq!(
        dirty,
        ["t0", "t1", "t5"],
        "the Task, its dependant and its child"
    );
}
