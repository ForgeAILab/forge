//! Workspace and task step migrations on populated databases.

use super::*;

#[tokio::test]
async fn daemon_migrations_arriving_after_integration_preserve_existing_workspaces() {
    let dir = unique_temp_path("daemon-late-migrations");
    fs::create_dir_all(&dir).unwrap();
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    for entry in fs::read_dir(&source).unwrap() {
        let path = entry.unwrap().path();
        let filename = path.file_name().unwrap().to_str().unwrap();
        // The integration branch held migrations up to 202610010600 when the
        // two daemon migrations arrived late. Everything written after that
        // point may depend on them and has only ever run after them.
        if matches!(
            migration_version(filename),
            Some(202_610_010_400 | 202_610_010_530)
        ) || migration_version(filename).is_some_and(|version| version > 202_610_010_600)
        {
            continue;
        }
        fs::copy(&path, dir.join(filename)).unwrap();
    }
    let (pool, db_path) = migration_test_pool("daemon-late-db").await;
    run_migrations_from(&pool, &dir).await.unwrap();
    sqlx::raw_sql("INSERT INTO project (id, name, settings, workflow_definition, created_at, updated_at) VALUES ('late-project', 'keep', '{}', '{}', '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z');
        INSERT INTO repo (id, project_id, name, local_path, work_mode, default_branch, created_at, updated_at) VALUES ('late-repo', 'late-project', 'repo', '/existing/checkout', 'direct_merge', 'main', '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z');
        INSERT INTO task (id, project_id, title, task_type, status, created_at, updated_at) VALUES ('late-task', 'late-project', 'keep task', 'task', 'in_progress', '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z');
        INSERT INTO workspace (id, task_id, repo_id, worktree_path, branch, status, created_at, updated_at) VALUES ('late-workspace', 'late-task', 'late-repo', '/existing/worktree', 'task/keep', 'ready', '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z');")
        .execute(&pool).await.unwrap();
    for entry in fs::read_dir(&source).unwrap() {
        let path = entry.unwrap().path();
        let filename = path.file_name().unwrap().to_str().unwrap();
        if !dir.join(filename).exists() {
            fs::copy(&path, dir.join(filename)).unwrap();
        }
    }
    run_migrations_from(&pool, &dir).await.unwrap();
    let preserved: (String, String, String) = sqlx::query_as("SELECT t.title, p.workspace_handle, l.path FROM task t JOIN workspace_placement p ON p.task_id = t.id JOIN repo_location l ON l.id = p.repo_location_id WHERE t.id = 'late-task'")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(
        preserved,
        (
            "keep task".into(),
            "/existing/worktree".into(),
            "/existing/checkout".into()
        )
    );
    assert!(table_exists(&pool, "workspace_expected_head").await);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _migration WHERE version IN (202610010400,202610010410,202610010500,202610010530,202610010550,202610010600)").fetch_one(&pool).await.unwrap();
    assert_eq!(count, 6);
    assert!(sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&pool)
        .await
        .unwrap()
        .is_empty());
    pool.close().await;
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(dir);
}

#[tokio::test]
async fn conflict_hotspot_migration_seeds_current_head_on_migrated_clone() {
    let migration_dir = unique_temp_path("conflict-hotspot-migrations");
    fs::create_dir_all(&migration_dir).unwrap();
    copy_migrations_up_to(202610032059, &migration_dir);
    let db_path = unique_temp_path("conflict-hotspot-db").with_extension("db");
    let url = format!("sqlite://{}", db_path.display());
    let pool = create_sqlite_pool(&url).await.unwrap();
    run_migrations_from(&pool, &migration_dir).await.unwrap();
    let log_time = (chrono::Utc::now() - chrono::Duration::minutes(1)).to_rfc3339();
    sqlx::query("INSERT INTO project (id, name, settings, workflow_definition, created_at, updated_at) VALUES ('hotspot-format-project', 'format', '{}', '{}', ?, ?)")
        .bind(&log_time).bind(&log_time).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO task (id, project_id, title, created_at, updated_at) VALUES ('hotspot-format-task', 'hotspot-format-project', 'format', ?, ?)")
        .bind(&log_time).bind(&log_time).execute(&pool).await.unwrap();
    // Seed the pre-upgrade schema with its own columns; the current repository
    // intentionally requires the new typed history schema.
    sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,rejection,created_at) VALUES('hotspot-format-log','hotspot-format-task','merging','merge_failed','system:workflow','format probe',0,?)")
        .bind(&log_time).execute(&pool).await.unwrap();
    let stored: String =
        sqlx::query_scalar("SELECT created_at FROM transition_log WHERE id = 'hotspot-format-log'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(stored, log_time);
    assert!(stored.ends_with("+00:00"));
    sqlx::query("INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type, scope_type, scope_id, correlation_id, payload_json, created_at) VALUES ('hotspot-before-upgrade', 'task.transitioned', 'task', 'task-before', 'system', 'task', 'task-before', 'upgrade-test', '{\"project_id\":\"project-before\"}', '2026-10-03T20:00:00Z')")
        .execute(&pool).await.unwrap();
    let head: i64 = sqlx::query_scalar("SELECT MAX(sequence) FROM domain_event")
        .fetch_one(&pool)
        .await
        .unwrap();
    // Close and reopen the already-migrated file, as a clone upgrade does.
    pool.close().await;
    let pool = create_sqlite_pool(&url).await.unwrap();
    run_migrations(&pool).await.unwrap();
    let cursor: (i64, i64) = sqlx::query_as("SELECT last_sequence, version FROM event_consumer_cursor WHERE consumer_name = 'conflict-hotspots'")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(cursor, (head, 1));
    let cutover: i64 = sqlx::query_scalar("SELECT cutover_sequence FROM event_consumer_cutover WHERE consumer_name = 'conflict-hotspots'")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(cutover, head);
    let cutover_time: String = sqlx::query_scalar(
        "SELECT created_at FROM event_consumer_cutover WHERE consumer_name = 'conflict-hotspots'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(cutover_time.ends_with("+00:00"));
    assert!(stored < cutover_time);
    assert_eq!(
        chrono::DateTime::parse_from_rfc3339(&cutover_time)
            .unwrap()
            .offset()
            .local_minus_utc(),
        0
    );
    let columns: Vec<String> =
        sqlx::query_scalar("SELECT name FROM pragma_table_info('conflict_hotspot_boundary')")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(columns.contains(&"open_since".to_owned()));
    let cascade: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pragma_foreign_key_list('conflict_hotspot_boundary') WHERE [table] = 'project' AND on_delete = 'CASCADE'")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(cascade, 1);
    let historical: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM domain_event WHERE id = 'hotspot-before-upgrade'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(historical, 1);
    sqlx::query("UPDATE event_consumer_cursor SET last_sequence = last_sequence + 1, version = version + 1 WHERE consumer_name = 'conflict-hotspots'")
        .execute(&pool).await.unwrap();
    run_migrations(&pool).await.unwrap();
    let cursor_after: (i64, i64) = sqlx::query_as("SELECT last_sequence, version FROM event_consumer_cursor WHERE consumer_name = 'conflict-hotspots'")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(cursor_after, (head + 1, 2));
    let index: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_domain_event_type_sequence'")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(index, 1);
    let boundaries: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM conflict_hotspot_boundary")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(boundaries, 0);
    pool.close().await;
    fs::remove_file(db_path).unwrap();
    fs::remove_dir_all(migration_dir).unwrap();
}

/// The `task_step` rebuild that adds the `integration` step kind
/// (V202610100137), on a populated database file that is closed and opened
/// again around the migration, the way an upgraded install meets it: every
/// row, the self-reference, the dependent checkpoint, every index and trigger
/// survive, and the new kind is accepted afterwards.
#[tokio::test]
async fn integration_step_kind_rebuild_preserves_a_populated_file_database() {
    const NEW: &str = "V202610100137__integration_task_step.sql";
    const NEW_VERSION: i64 = 202_610_100_137;
    let dir = unique_temp_path("integration-step-kind-migrations");
    fs::create_dir_all(&dir).unwrap();
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    for entry in fs::read_dir(&source).unwrap() {
        let path = entry.unwrap().path();
        let filename = path.file_name().unwrap().to_str().unwrap();
        if migration_version(filename).is_some_and(|version| version < NEW_VERSION) {
            fs::copy(&path, dir.join(filename)).unwrap();
        }
    }
    let (pool, db_path) = migration_test_pool("integration-step-kind-db").await;
    run_migrations_from(&pool, &dir).await.unwrap();
    sqlx::raw_sql("INSERT INTO project (id, name, settings, workflow_definition, created_at, updated_at) VALUES ('step-project', 'keep', '{}', '{}', '2026-10-09T00:00:00Z', '2026-10-09T00:00:00Z');
        INSERT INTO task (id, project_id, title, task_type, status, created_at, updated_at) VALUES ('step-a', 'step-project', 'a', 'task', 'merging', '2026-10-09T00:00:00Z', '2026-10-09T00:00:00Z');
        INSERT INTO task (id, project_id, title, task_type, status, created_at, updated_at) VALUES ('step-b', 'step-project', 'b', 'task', 'review', '2026-10-09T00:00:00Z', '2026-10-09T00:00:00Z');")
        .execute(&pool).await.unwrap();
    // One step of every kind and every status the old CHECK allowed, with
    // every optional column filled on at least one row.
    let kinds = ["cascade", "hooks", "command", "mutation"];
    let statuses = [
        "pending",
        "claimed",
        "done",
        "superseded",
        "parked",
        "failed",
    ];
    let mut seq = 0;
    let mut parent: Option<String> = None;
    for task in ["step-a", "step-b"] {
        for (index, status) in statuses.iter().enumerate() {
            seq += 1;
            let id = format!("{task}-{seq}");
            let kind = kinds[index % kinds.len()];
            let claimed = *status == "claimed";
            let settled = matches!(*status, "done" | "superseded" | "failed");
            sqlx::query("INSERT INTO task_step (id,task_id,seq,kind,payload_json,causation_step_id,causation_key,chain_id,chain_position,expected_status,expected_version,status,claimed_by,lease_until,available_at,attempts,last_error,created_at,updated_at,completed_at,result_json,priority,preempt_requested_at,integration_started_at,expected_epoch,lane,entry_fenced) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
                .bind(&id)
                .bind(task)
                .bind(seq)
                .bind(kind)
                .bind(if kind == "hooks" { r#"{"workflow_ref":{"kind":"snapshot","id":"wf-1"}}"# } else { r#"{"preempt":false}"# })
                .bind(parent.as_deref())
                .bind(format!("key-{seq}"))
                .bind(format!("chain-{task}"))
                .bind(index as i64 + 1)
                .bind("merging")
                .bind(seq + 10)
                .bind(status)
                .bind(claimed.then_some("worker-1"))
                .bind(claimed.then_some("2099-01-01T00:00:00Z"))
                .bind("2026-10-09T00:00:01Z")
                .bind(index as i64)
                .bind((*status == "failed").then_some("boom"))
                .bind("2026-10-09T00:00:00Z")
                .bind("2026-10-09T00:00:02Z")
                .bind(settled.then_some("2026-10-09T00:00:03Z"))
                .bind(settled.then_some(r#"{"ok":true}"#))
                .bind(i64::from(kind == "command"))
                .bind((kind == "command").then_some("2026-10-09T00:00:04Z"))
                .bind(claimed.then_some("2026-10-09T00:00:05Z"))
                .bind(seq % 3)
                .bind(if index % 2 == 0 { "fast" } else { "long" })
                .bind(i64::from(!claimed))
                .execute(&pool)
                .await
                .unwrap();
            parent = Some(id);
        }
    }
    sqlx::query("INSERT INTO task_hook_checkpoint(step_id,hook_index,started_at) VALUES('step-a-2',0,'2026-10-09T00:00:06Z')")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        sqlx::query("INSERT INTO task_step (id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,status,available_at,created_at,updated_at) VALUES ('refused','step-a',99,'integration','{}','refused','refused',1,'merging',1,'pending','2026-10-09T00:00:00Z','2026-10-09T00:00:00Z','2026-10-09T00:00:00Z')")
            .execute(&pool)
            .await
            .is_err(),
        "the old CHECK refuses the kind"
    );
    async fn rows(pool: &sqlx::SqlitePool) -> String {
        sqlx::query_scalar("SELECT json_group_array(json_array(id,task_id,seq,kind,payload_json,causation_step_id,causation_key,chain_id,chain_position,expected_status,expected_version,status,claimed_by,lease_until,available_at,attempts,last_error,created_at,updated_at,completed_at,result_json,priority,preempt_requested_at,integration_started_at,expected_epoch,lane,entry_fenced,workflow_ref_id)) FROM (SELECT * FROM task_step ORDER BY task_id,seq)")
            .fetch_one(pool)
            .await
            .unwrap()
    }
    async fn schema(pool: &sqlx::SqlitePool) -> Vec<(String, String, Option<String>)> {
        sqlx::query_as("SELECT type,name,sql FROM sqlite_schema WHERE tbl_name='task_step' AND name NOT LIKE 'sqlite_autoindex%' ORDER BY type,name")
            .fetch_all(pool)
            .await
            .unwrap()
    }
    let before = rows(&pool).await;
    let before_schema = schema(&pool).await;
    let dirty_before: i64 =
        sqlx::query_scalar("SELECT COALESCE(SUM(generation),0) FROM task_schedule_dirty")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_step")
            .fetch_one(&pool)
            .await
            .unwrap(),
        12
    );
    // The install stops, is upgraded, and starts on the same file.
    pool.close().await;
    fs::copy(source.join(NEW), dir.join(NEW)).unwrap();
    let url = format!("sqlite://{}", db_path.display());
    let pool = create_sqlite_pool(&url).await.unwrap();
    run_migrations_from(&pool, &dir).await.unwrap();

    assert_eq!(rows(&pool).await, before, "every row and column value");
    let after_schema = schema(&pool).await;
    assert_eq!(before_schema.len(), after_schema.len());
    for (old, new) in before_schema.iter().zip(&after_schema) {
        assert_eq!((&old.0, &old.1), (&new.0, &new.1));
        if old.0 != "table" {
            assert_eq!(old.2, new.2, "{}", old.1);
        }
    }
    // The rebuild copies rows without firing the scheduler triggers.
    let dirty_after: i64 =
        sqlx::query_scalar("SELECT COALESCE(SUM(generation),0) FROM task_schedule_dirty")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(dirty_after, dirty_before);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task_hook_checkpoint WHERE step_id='step-a-2'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert!(sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&pool)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1,
        "the rebuild leaves foreign keys on"
    );
    // The new kind is accepted, the self-reference still cascades to NULL,
    // and the dependent checkpoint still follows its step.
    sqlx::query("INSERT INTO task_step (id,task_id,seq,kind,payload_json,causation_step_id,causation_key,chain_id,chain_position,expected_status,expected_version,status,available_at,created_at,updated_at) VALUES ('accepted','step-a',99,'integration','{}','step-a-1','integration:a:1:settle','integration:a:1:settle',1,'merging',1,'pending','2026-10-10T00:00:00Z','2026-10-10T00:00:00Z','2026-10-10T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM task_step WHERE id='step-a-1'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT causation_step_id FROM task_step WHERE id='accepted'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        None
    );
    sqlx::query("DELETE FROM task_step WHERE id='step-a-2'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_hook_checkpoint")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    // A second start is a no-op.
    pool.close().await;
    let pool = create_sqlite_pool(&url).await.unwrap();
    run_migrations_from(&pool, &dir).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _migration WHERE version=202610100137")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    pool.close().await;
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(dir);
}

/// `V202610101235` adds the nullable `daemon.disk_json` and nothing else: a
/// daemon registered before it keeps every value and has no reading (which
/// refuses nothing) until its next report.
#[tokio::test]
async fn daemon_disk_facts_migration_is_additive_and_keeps_existing_daemons() {
    let migration_dir = unique_temp_path("daemon-disk-migrations");
    fs::create_dir_all(&migration_dir).expect("migration dir creates");
    copy_migrations_up_to(202610100719, &migration_dir);
    let db_path = unique_temp_path("daemon-disk-db").with_extension("db");
    let pool = create_sqlite_pool(&format!("sqlite://{}", db_path.display()))
        .await
        .expect("pool creates");
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("migrations before the disk facts apply");
    let columns = |pool: db::SqlitePool| async move {
        sqlx::query("PRAGMA table_info(daemon)")
            .fetch_all(&pool)
            .await
            .expect("columns read")
            .into_iter()
            .map(|row| (row.get::<String, _>("name"), row.get::<i64, _>("notnull")))
            .collect::<Vec<_>>()
    };
    let before = columns(pool.clone()).await;
    assert!(before.iter().all(|(name, _)| name != "disk_json"));
    let now = "2026-10-10T00:00:00Z";
    sqlx::query("INSERT INTO daemon (id,machine_id,hostname,os,arch,status,created_at,updated_at,max_concurrent_runs) VALUES ('disk-daemon','disk-machine','builder','linux','aarch64','online',?,?,3)")
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .expect("daemon inserts before the migration");
    let row_before: String = sqlx::query_scalar(
        "SELECT group_concat(quote(id)||quote(machine_id)||quote(hostname)||quote(status)||quote(max_concurrent_runs)||quote(updated_at)) FROM daemon",
    )
    .fetch_one(&pool)
    .await
    .expect("row reads");

    let all = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    run_migrations_from(&pool, &all)
        .await
        .expect("the disk facts migration applies to a database with daemons");
    let after = columns(pool.clone()).await;
    // Every column that was there is unchanged and in place; the new one is
    // nullable.
    assert_eq!(&after[..before.len()], &before[..]);
    assert!(after.contains(&("disk_json".to_owned(), 0)));
    let row_after: String = sqlx::query_scalar(
        "SELECT group_concat(quote(id)||quote(machine_id)||quote(hostname)||quote(status)||quote(max_concurrent_runs)||quote(updated_at)) FROM daemon",
    )
    .fetch_one(&pool)
    .await
    .expect("row reads");
    assert_eq!(row_before, row_after);
    let reading: Option<String> =
        sqlx::query_scalar("SELECT disk_json FROM daemon WHERE id = 'disk-daemon'")
            .fetch_one(&pool)
            .await
            .expect("reading reads");
    assert_eq!(reading, None, "no reading until the next report");
    pool.close().await;
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(migration_dir);
}

/// `V202610110112` closes the queued Task mutations of the retired inline
/// review re-run (`ReviewCreateAttemptWithExecution`), which no operation can
/// apply any more, and nothing else: the rows stay, with their payload, and
/// every other step, settled or queued, is exactly as it was.
#[tokio::test]
async fn retired_inline_review_rerun_steps_are_superseded_and_every_other_step_is_kept() {
    let migration_dir = unique_temp_path("retired-rerun-migrations");
    fs::create_dir_all(&migration_dir).expect("migration dir creates");
    copy_migrations_up_to(202610101906, &migration_dir);
    let db_path = unique_temp_path("retired-rerun-db").with_extension("db");
    let pool = create_sqlite_pool(&format!("sqlite://{}", db_path.display()))
        .await
        .expect("pool creates");
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("migrations before the retirement apply");
    let now = "2026-10-10T00:00:00Z";
    sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES('p','Project',?,?)")
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .expect("project inserts");
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t','p','Task','review',?,?)")
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .expect("task inserts");
    let retired = r#"{"ReviewCreateAttemptWithExecution":{"review":{"id":"r"},"execution":{"id":"e"},"lease":{"owner":"review-owner:x"},"admission":null}}"#;
    let live = r#"{"ReviewCreateManualPass":{"input":{}}}"#;
    for (seq, id, kind, status, payload, claimed_by) in [
        (1, "retired-pending", "mutation", "pending", retired, None),
        (
            2,
            "retired-claimed",
            "mutation",
            "claimed",
            retired,
            Some("worker"),
        ),
        (3, "retired-done", "mutation", "done", retired, None),
        (4, "live-pending", "mutation", "pending", live, None),
        (
            5,
            "command-pending",
            "command",
            "pending",
            r#"{"operation":"rerun_review","arguments":["t"],"preempt":false}"#,
            None,
        ),
    ] {
        sqlx::query("INSERT INTO task_step(id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,status,claimed_by,lease_until,available_at,created_at,updated_at) VALUES(?,'t',?,?,?,?,?,1,'review',1,?,?,?,?,?,?)")
            .bind(id)
            .bind(seq)
            .bind(kind)
            .bind(payload)
            .bind(id)
            .bind(id)
            .bind(status)
            .bind(claimed_by)
            .bind(claimed_by.map(|_| "2026-10-10T00:05:00Z"))
            .bind(now)
            .bind(now)
            .bind(now)
            .execute(&pool)
            .await
            .expect("step inserts before the migration");
    }
    let snapshot = |pool: db::SqlitePool, filter: &'static str| async move {
        sqlx::query_scalar::<_, String>(&format!(
            "SELECT group_concat(quote(id)||quote(status)||quote(payload_json)||quote(claimed_by)||quote(lease_until)||quote(last_error)||quote(updated_at), ';') FROM (SELECT * FROM task_step WHERE {filter} ORDER BY seq)"
        ))
        .fetch_one(&pool)
        .await
        .expect("steps read")
    };
    let untouched = "id NOT IN ('retired-pending','retired-claimed')";
    let before = snapshot(pool.clone(), untouched).await;

    let all = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    run_migrations_from(&pool, &all)
        .await
        .expect("the retirement migration applies to a database with queued steps");

    assert_eq!(snapshot(pool.clone(), untouched).await, before);
    type ClosedStep = (
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        String,
    );
    let closed: Vec<ClosedStep> = sqlx::query_as(
        "SELECT id,status,claimed_by,lease_until,last_error,payload_json FROM task_step WHERE id IN ('retired-pending','retired-claimed') ORDER BY seq",
    )
    .fetch_all(&pool)
    .await
    .expect("closed steps read");
    assert_eq!(closed.len(), 2, "the rows are kept");
    for (id, status, claimed_by, lease_until, last_error, payload) in closed {
        assert_eq!(status, "superseded", "{id}");
        assert_eq!((claimed_by, lease_until), (None, None), "{id}");
        assert!(
            last_error.is_some_and(|error| error.contains("inline review re-run was retired")),
            "{id}"
        );
        assert_eq!(payload, retired, "{id}");
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task_step WHERE task_id='t' AND status IN ('pending','claimed')"
        )
        .fetch_one(&pool)
        .await
        .expect("queue reads"),
        2,
        "the live mutation and the queued re-run command still wait"
    );
    pool.close().await;
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(migration_dir);
}

/// V202610100820 rebuilds `task_step` for the `suspended` status on a
/// populated database: every row survives as it was (pending, leased,
/// superseded, done, with its causation link), the new columns start empty,
/// and every index and trigger of the table exists afterwards.
#[tokio::test]
async fn task_step_suspended_migration_preserves_populated_steps_indexes_and_triggers() {
    const BEFORE: i64 = 202_610_100_719;
    const SUSPENDED: i64 = 202_610_100_820;
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let mut versions: Vec<i64> = fs::read_dir(&source)
        .unwrap()
        .filter_map(|entry| migration_version(entry.unwrap().file_name().to_str().unwrap()))
        .collect();
    versions.sort_unstable();
    let at = versions.iter().position(|v| *v == SUSPENDED).unwrap();
    assert_eq!(
        versions[at - 1],
        BEFORE,
        "the rebuild follows the workspace disk-usage migration directly"
    );
    assert!(
        include_str!("../../../src/migration.rs").contains(&format!(
            "Embedded migration bundle revision: V{}",
            versions.last().unwrap()
        )),
        "the bundle comment names the newest migration"
    );

    let dir = unique_temp_path("task-step-suspended-migrations");
    fs::create_dir_all(&dir).unwrap();
    copy_migrations_up_to(BEFORE, &dir);
    let (pool, db_path) = migration_test_pool("task-step-suspended-db").await;
    run_migrations_from(&pool, &dir).await.unwrap();
    sqlx::raw_sql("INSERT INTO project (id, name, settings, workflow_definition, created_at, updated_at) VALUES ('susp-project', 'keep', '{}', '{}', '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z');
        INSERT INTO task (id, project_id, title, task_type, status, created_at, updated_at) VALUES ('susp-task', 'susp-project', 'keep task', 'task', 'review', '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z');
        INSERT INTO task_step (id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,status,available_at,created_at,updated_at,completed_at,result_json) VALUES ('step-done','susp-task',1,'cascade','{\"workflow_ref\":{\"id\":\"wf-1\"}}','k-done','chain',1,'in_progress',1,'done','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z','2026-10-01T00:00:01Z','2026-10-01T00:00:01Z','{\"ok\":true}');
        INSERT INTO task_step (id,task_id,seq,kind,payload_json,causation_step_id,causation_key,chain_id,chain_position,expected_status,expected_version,status,available_at,created_at,updated_at,completed_at,last_error) VALUES ('step-superseded','susp-task',2,'hooks','{}','step-done','k-superseded','chain',2,'review',2,'superseded','2026-10-01T00:00:02Z','2026-10-01T00:00:02Z','2026-10-01T00:00:03Z','2026-10-01T00:00:03Z','preempted by owner command');
        INSERT INTO task_step (id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,status,claimed_by,lease_until,available_at,attempts,created_at,updated_at,lane,expected_epoch,priority) VALUES ('step-leased','susp-task',3,'hooks','{}','k-leased','chain-2',1,'review',3,'claimed','worker-1','2099-01-01T00:00:00Z','2026-10-01T00:00:04Z',2,'2026-10-01T00:00:04Z','2026-10-01T00:00:05Z','long',7,0);
        INSERT INTO task_step (id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,status,available_at,created_at,updated_at,priority,entry_fenced) VALUES ('step-pending','susp-task',4,'command','{\"operation\":\"noop\"}','k-pending','chain-3',1,'review',3,'pending','2026-10-01T00:00:06Z','2026-10-01T00:00:06Z','2026-10-01T00:00:06Z',1,0);")
        .execute(&pool).await.unwrap();
    const ROWS: &str = "SELECT group_concat(id||'|'||seq||'|'||kind||'|'||payload_json||'|'||COALESCE(causation_step_id,'-')||'|'||causation_key||'|'||chain_id||'|'||chain_position||'|'||expected_status||'|'||expected_version||'|'||status||'|'||COALESCE(claimed_by,'-')||'|'||COALESCE(lease_until,'-')||'|'||available_at||'|'||attempts||'|'||COALESCE(last_error,'-')||'|'||created_at||'|'||updated_at||'|'||COALESCE(completed_at,'-')||'|'||COALESCE(result_json,'-')||'|'||priority||'|'||expected_epoch||'|'||lane||'|'||entry_fenced||'|'||COALESCE(workflow_ref_id,'-'), char(10)) FROM (SELECT * FROM task_step ORDER BY seq)";
    let before: String = sqlx::query_scalar(ROWS).fetch_one(&pool).await.unwrap();
    let objects = "SELECT group_concat(name, ',') FROM (SELECT name FROM sqlite_master WHERE tbl_name='task_step' AND type IN ('index','trigger') AND name NOT LIKE 'sqlite_%' ORDER BY name)";
    let objects_before: String = sqlx::query_scalar(objects).fetch_one(&pool).await.unwrap();

    fs::copy(
        source.join("V202610100820__task_step_suspended.sql"),
        dir.join("V202610100820__task_step_suspended.sql"),
    )
    .unwrap();
    run_migrations_from(&pool, &dir).await.unwrap();

    let after: String = sqlx::query_scalar(ROWS).fetch_one(&pool).await.unwrap();
    assert_eq!(
        after, before,
        "every step row survives the rebuild unchanged"
    );
    assert_eq!(after.lines().count(), 4);
    let empty: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_step WHERE suspended_until IS NULL AND awaited_consumer_id IS NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(empty, 4, "the new columns start empty");
    let objects_after: String = sqlx::query_scalar(objects).fetch_one(&pool).await.unwrap();
    assert_eq!(
        objects_after,
        format!("{objects_before},task_step_suspended")
            .split(',')
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join(","),
        "every earlier index and trigger is re-created, plus the suspended index"
    );
    for name in [
        "task_step_next",
        "task_step_lane_ready",
        "task_step_suspended",
        "task_schedule_step_insert",
        "task_schedule_step_update",
        "task_schedule_step_delete",
        "task_schedule_admission_release",
    ] {
        assert!(
            objects_after.split(',').any(|object| object == name),
            "{name} exists after the rebuild: {objects_after}"
        );
    }
    // The status is accepted, the scheduler trigger still fires on it, and
    // the wake query reads the partial index.
    let dirty = "SELECT COALESCE((SELECT generation FROM task_schedule_dirty WHERE task_id='susp-task'),-1)";
    let generation: i64 = sqlx::query_scalar(dirty).fetch_one(&pool).await.unwrap();
    sqlx::query("UPDATE task_step SET status='suspended',claimed_by=NULL,lease_until=NULL,suspended_until='2099-01-01T00:00:00Z',awaited_consumer_id='consumer-1' WHERE id='step-leased'")
        .execute(&pool)
        .await
        .unwrap();
    let bumped: i64 = sqlx::query_scalar(dirty).fetch_one(&pool).await.unwrap();
    assert!(bumped > generation, "the step-update trigger survived");
    assert!(
        sqlx::query("UPDATE task_step SET status='waiting' WHERE id='step-pending'")
            .execute(&pool)
            .await
            .is_err(),
        "the status CHECK still refuses unknown values"
    );
    let plan: Vec<String> = sqlx::query("EXPLAIN QUERY PLAN SELECT EXISTS(SELECT 1 FROM task_step WHERE status='suspended' AND (suspended_until IS NULL OR suspended_until<='2026-10-10T00:00:00Z' OR NOT EXISTS(SELECT 1 FROM task t WHERE t.id=task_step.task_id AND t.status=task_step.expected_status AND t.status_epoch=task_step.expected_epoch AND t.deleted_at IS NULL)))")
        .fetch_all(&pool)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect();
    assert!(
        plan.iter()
            .any(|detail| detail.contains("SEARCH task_step USING") && detail.contains("INDEX"))
            && !plan.iter().any(|detail| detail.contains("SCAN task_step")),
        "the wake query before every claim is an index search on the suspended rows, never a table scan: {plan:?}"
    );
    assert!(sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&pool)
        .await
        .unwrap()
        .is_empty());
    let kept: (String, String) = sqlx::query_as(
        "SELECT causation_step_id, status FROM task_step WHERE id='step-superseded'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(kept, ("step-done".into(), "superseded".into()));
    pool.close().await;
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(dir);
}

/// `V202610110650` adds the age of a run-slot wait and nothing else: wait
/// rows, dirty marks and check runs are exactly as they were, a Task that
/// already waited for a run slot gets an age that starts at the upgrade, and
/// from then on the triggers keep the age (begin, real releases, lapse, end).
#[tokio::test]
async fn slot_wait_age_migration_preserves_waits_and_checks_and_ages_existing_waiters() {
    let migration_dir = unique_temp_path("slot-age-migrations");
    fs::create_dir_all(&migration_dir).expect("migration dir creates");
    copy_migrations_up_to(202610110112, &migration_dir);
    let db_path = unique_temp_path("slot-age-db").with_extension("db");
    let pool = create_sqlite_pool(&format!("sqlite://{}", db_path.display()))
        .await
        .expect("pool creates");
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("migrations before the slot-wait age apply");
    let now = "2026-10-10T00:00:00Z";
    sqlx::raw_sql(
        "INSERT INTO project(id,name,created_at,updated_at) VALUES('p','Project','2026-10-10T00:00:00Z','2026-10-10T00:00:00Z');
         INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES
           ('slot','p','slot','todo','2026-10-10T00:00:00Z','2026-10-10T00:00:00Z'),
           ('named','p','named','todo','2026-10-10T00:00:00Z','2026-10-10T00:00:00Z'),
           ('later','p','later','todo','2026-10-10T00:00:00Z','2026-10-10T00:00:00Z');
         INSERT INTO task_schedule_wait(task_id,project_id,daemon_id,deadline) VALUES
           ('slot','p','*','2026-10-10T00:01:00Z'),('named','p','machine',NULL);",
    )
    .execute(&pool)
    .await
    .expect("rows before the migration insert");
    let _ = now;
    let waits = |pool: db::SqlitePool| async move {
        sqlx::query_scalar::<_, String>("SELECT group_concat(quote(task_id)||quote(project_id)||quote(agent_id)||quote(daemon_id)||quote(deadline)||quote(project_capacity),';') FROM (SELECT * FROM task_schedule_wait ORDER BY task_id)")
            .fetch_one(&pool)
            .await
            .expect("waits read")
    };
    let before = waits(pool.clone()).await;
    let check_columns_before: Vec<String> =
        sqlx::query_scalar("SELECT name FROM pragma_table_info('check_run') ORDER BY cid")
            .fetch_all(&pool)
            .await
            .expect("columns read");

    let all = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    run_migrations_from(&pool, &all)
        .await
        .expect("the slot-wait age migration applies");

    assert_eq!(waits(pool.clone()).await, before);
    let check_columns: Vec<String> =
        sqlx::query_scalar("SELECT name FROM pragma_table_info('check_run') ORDER BY cid")
            .fetch_all(&pool)
            .await
            .expect("columns read");
    assert_eq!(
        check_columns[..check_columns_before.len()],
        check_columns_before[..]
    );
    assert!(check_columns.iter().any(|name| name == "yielded_since"));
    let age = |pool: db::SqlitePool, id: &'static str| async move {
        sqlx::query_as::<_, (i64, bool)>(
            "SELECT releases, lapsed_at IS NOT NULL FROM task_slot_wait_age WHERE task_id=?",
        )
        .bind(id)
        .fetch_optional(&pool)
        .await
        .expect("age reads")
    };
    assert_eq!(age(pool.clone(), "slot").await, Some((0, false)));
    assert_eq!(age(pool.clone(), "named").await, None);

    // A dirty mark wakes a waiter and is not a release: a Task receives
    // those for its own writes too and must not age its own wait with them.
    let mark = "INSERT INTO task_schedule_dirty(task_id,external) VALUES(?,1) ON CONFLICT(task_id) DO UPDATE SET generation=generation+1,dirty=1,external=1";
    for _ in 0..5 {
        sqlx::query(mark).bind("slot").execute(&pool).await.unwrap();
    }
    assert_eq!(age(pool.clone(), "slot").await, Some((0, false)));
    // A wait that begins later starts its own age. A run of another Task
    // that ends is a release for every waiter but that Task itself.
    sqlx::raw_sql(
        "INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES
           ('other','named','coder','running','2026-10-10T00:00:00Z','2026-10-10T00:00:00Z'),
           ('own','later','interactive','running','2026-10-10T00:00:00Z','2026-10-10T00:00:00Z');
         INSERT INTO task_schedule_wait(task_id,project_id,daemon_id) VALUES('later','p','*');",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(age(pool.clone(), "later").await, Some((0, false)));
    sqlx::query("UPDATE execution SET status='completed' WHERE id='other'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(age(pool.clone(), "slot").await, Some((1, false)));
    assert_eq!(age(pool.clone(), "later").await, Some((1, false)));
    sqlx::query("UPDATE execution SET status='completed' WHERE id='own'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(age(pool.clone(), "slot").await, Some((2, false)));
    assert_eq!(
        age(pool.clone(), "later").await,
        Some((1, false)),
        "a Task aged its own wait by ending its own run"
    );
    sqlx::query("UPDATE task_schedule_wait SET daemon_id='*' WHERE task_id='named'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(age(pool.clone(), "named").await, Some((0, false)));
    sqlx::query("UPDATE task_schedule_wait SET daemon_id=NULL WHERE task_id='named'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(age(pool.clone(), "named").await, Some((0, true)));
    sqlx::query("DELETE FROM task_schedule_wait WHERE task_id='slot'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(age(pool.clone(), "slot").await, Some((2, true)));
    sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES('e','slot','coder','running','2026-10-10T00:00:00Z','2026-10-10T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(age(pool.clone(), "slot").await, None);

    pool.close().await;
    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_dir_all(migration_dir);
}
