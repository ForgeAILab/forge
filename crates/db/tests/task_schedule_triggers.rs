//! The scheduler's kicks: what a commit marks dirty, what it must not, and
//! what the fan-out costs.
use std::time::{Duration, Instant};

async fn database() -> db::SqlitePool {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    pool
}

async fn dirty(pool: &db::SqlitePool) -> Vec<String> {
    sqlx::query_scalar("SELECT task_id FROM task_schedule_dirty WHERE dirty=1 ORDER BY task_id")
        .fetch_all(pool)
        .await
        .unwrap()
}

const FIXTURE: &str = "INSERT INTO project(id,name,settings,workflow_definition,created_at,updated_at) VALUES ('p','p','{}','{}','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z'),('q','q','{}','{}','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z');
    INSERT INTO task(id,project_id,title,task_type,status,created_at,updated_at) VALUES ('work','p','work','task','in_progress','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z'),('root','p','root','task','in_progress','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z'),('lonely','q','lonely','task','in_progress','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z'),('agent-wait','p','w','task','todo','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z'),('machine-wait','p','w','task','todo','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z'),('project-wait','p','w','task','todo','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z'),('other-project-wait','q','w','task','todo','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z'),('timer-wait','p','w','task','todo','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z');
    INSERT INTO execution(id,task_id,agent_id,role,status,created_at,updated_at,lease_owner,lease_expires_at) VALUES ('e','work',NULL,'coder','running','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z','o','2026-10-06T00:01:00Z'),('eq','lonely',NULL,'coder','running','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z','o','2026-10-06T00:01:00Z');
    INSERT INTO task_schedule_wait(task_id,project_id,agent_id,daemon_id,deadline,project_capacity) VALUES ('agent-wait','p','agent-a',NULL,NULL,0),('machine-wait','p',NULL,'*',NULL,0),('project-wait','p',NULL,NULL,NULL,1),('other-project-wait','q',NULL,NULL,NULL,1),('timer-wait','p',NULL,NULL,'2030-01-01T00:00:00Z',0);
    DELETE FROM task_schedule_dirty;";

/// A heartbeat renews an execution's lease every 20 seconds. It changes no
/// dispatch decision, so it marks nothing: not its own Task, not a waiter in
/// its Project and not a waiter anywhere else.
#[tokio::test]
async fn an_execution_heartbeat_dirties_nothing() {
    let pool = database().await;
    sqlx::raw_sql(FIXTURE).execute(&pool).await.unwrap();
    for id in ["e", "eq"] {
        sqlx::query("UPDATE execution SET lease_expires_at='2026-10-06T00:02:00Z', lease_owner='o', last_heartbeat_at='2026-10-06T00:01:40Z', execution_version=execution_version+1, updated_at='2026-10-06T00:01:40Z' WHERE id=?")
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
    }
    assert_eq!(dirty(&pool).await, Vec::<String>::new());
}

/// Freed capacity kicks the waiters it can admit: the Tasks waiting on that
/// Project's limit and the Tasks waiting for a machine run slot. A waiter on
/// another Project's limit, on an Agent or on a timer is left alone.
#[tokio::test]
async fn a_finished_execution_kicks_only_the_waiters_it_can_admit() {
    let pool = database().await;
    sqlx::raw_sql(FIXTURE).execute(&pool).await.unwrap();
    sqlx::query("UPDATE execution SET status='completed' WHERE id='e'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(dirty(&pool).await, ["machine-wait", "project-wait", "work"]);
    sqlx::query("DELETE FROM task_schedule_dirty")
        .execute(&pool)
        .await
        .unwrap();
    // A Task's own metadata note is not a scheduling fact.
    sqlx::query("UPDATE task SET metadata_json=json_object('note','x') WHERE id='work'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(dirty(&pool).await, Vec::<String>::new());
    // A queued Task write does not dirty the Task it is queued for: the write
    // it carries does, when it changes a scheduling fact.
    sqlx::query("INSERT INTO task_step(id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,status,available_at,created_at,updated_at) VALUES ('m','work',1,'mutation','{}','m','m',1,'in_progress',1,'pending','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE task_step SET status='done' WHERE id='m'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(dirty(&pool).await, Vec::<String>::new());
}

/// Trigger bodies are compiled when a statement on their table is prepared.
/// A wrong column or table name would make every such write fail.
#[tokio::test]
async fn every_scheduler_trigger_compiles() {
    let pool = database().await;
    let triggers: Vec<(String, String, String)> = sqlx::query_as("SELECT name,tbl_name,sql FROM sqlite_master WHERE type='trigger' AND (name LIKE 'task_schedule_%' OR name LIKE 'project_schedule_%') ORDER BY name")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert!(triggers.len() >= 40, "{}", triggers.len());
    let mut failures = Vec::new();
    for (name, table, sql) in &triggers {
        let upper = sql.to_ascii_uppercase();
        let head = upper.split(" ON ").next().unwrap_or("");
        let statement = if head.contains("INSERT") {
            // One real column, no rows: generated columns cannot be inserted.
            let column: String = sqlx::query_scalar(&format!(
                "SELECT name FROM pragma_table_info('{table}') ORDER BY cid LIMIT 1"
            ))
            .fetch_one(&pool)
            .await
            .unwrap();
            format!("INSERT INTO {table}({column}) SELECT {column} FROM {table} WHERE 0")
        } else if head.contains("DELETE") {
            format!("DELETE FROM {table} WHERE 0")
        } else {
            format!("UPDATE {table} SET rowid=rowid WHERE 0")
        };
        if let Err(error) = sqlx::query(&statement).execute(&pool).await {
            failures.push(format!("{name}: {statement}: {error}"));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// The reads the reconciler and the sweep issue use an index: the sweep never
/// visits a settled Task, and the dirty set is read from its partial index.
#[tokio::test]
async fn scheduler_reads_use_their_indexes() {
    let pool = database().await;
    for (label, sql, index) in [
        (
            "sweep page",
            "EXPLAIN QUERY PLAN SELECT id FROM task WHERE json_extract(condition_json,'$.kind') != 'settled' AND (?1 IS NULL OR id>?1) ORDER BY id LIMIT ?2",
            "idx_task_schedule_open",
        ),
        (
            "dirty set",
            "EXPLAIN QUERY PLAN SELECT task_id FROM task_schedule_dirty WHERE dirty=1 LIMIT ?",
            "task_schedule_dirty_ready",
        ),
        (
            "machine waiters",
            "EXPLAIN QUERY PLAN SELECT task_id FROM task_schedule_wait WHERE daemon_id='*'",
            "task_schedule_wait_machine",
        ),
    ] {
        let rows = sqlx::query(sql).fetch_all(&pool).await.unwrap();
        let plan: Vec<String> = rows
            .iter()
            .map(|r| sqlx::Row::get::<String, _>(r, 3))
            .collect();
        assert!(
            plan.iter().any(|step| step.contains(index)),
            "{label}: {plan:?}"
        );
    }
}

async fn timed(pool: &db::SqlitePool, reps: u32, sql: &str) -> (Duration, i64) {
    let mut total = Duration::ZERO;
    let mut marked = 0;
    for i in 0..reps {
        sqlx::query("UPDATE task_schedule_dirty SET dirty=0")
            .execute(pool)
            .await
            .unwrap();
        let mut c = pool.acquire().await.unwrap();
        let t = Instant::now();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *c)
            .await
            .unwrap();
        sqlx::query(sql)
            .bind(i as i64)
            .execute(&mut *c)
            .await
            .unwrap();
        sqlx::query("COMMIT").execute(&mut *c).await.unwrap();
        total += t.elapsed();
        drop(c);
        marked = sqlx::query_scalar("SELECT COUNT(*) FROM task_schedule_dirty WHERE dirty=1")
            .fetch_one(pool)
            .await
            .unwrap_or(-1);
    }
    (total / reps, marked)
}

/// Write cost of the kicks under fan-out, against the same statements with
/// every scheduler trigger dropped.
/// `cargo test -p db --release --test task_schedule_triggers -- --ignored --nocapture`
#[tokio::test]
#[ignore = "measurement"]
async fn trigger_fanout_cost() {
    let n: i64 = std::env::var("FANOUT_WAITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5000);
    let path =
        std::env::temp_dir().join(format!("forge-schedule-triggers-{}.db", std::process::id()));
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
    let pool = db::create_sqlite_pool(&format!("sqlite:{}?mode=rwc", path.display()))
        .await
        .unwrap();
    db::run_migrations(&pool).await.unwrap();
    sqlx::raw_sql("INSERT INTO project(id,name,settings,workflow_definition,created_at,updated_at) VALUES ('p','p','{}','{}','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z'),('q','q','{}','{}','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z');
        INSERT INTO task(id,project_id,title,task_type,status,created_at,updated_at) VALUES ('work','p','work','task','in_progress','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z'),('root','p','root','task','in_progress','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z'),('lonely','q','lonely','task','in_progress','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z');
        INSERT INTO execution(id,task_id,role,status,created_at,updated_at,lease_owner,lease_expires_at) VALUES ('e','work','coder','running','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z','o','2026-10-06T00:01:00Z'),('eq','lonely','coder','running','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z','o','2026-10-06T00:01:00Z');")
        .execute(&pool).await.unwrap();
    sqlx::query("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?) INSERT INTO task(id,project_id,title,task_type,status,created_at,updated_at) SELECT 'w-'||i,'p','w','task','todo','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z' FROM n")
        .bind(n).execute(&pool).await.unwrap();
    sqlx::query("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<200) INSERT INTO task(id,project_id,parent_task_id,subtask_order,title,task_type,status,created_at,updated_at) SELECT 'c-'||i,'p','root',i,'c','task','todo','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z' FROM n")
        .execute(&pool).await.unwrap();
    println!("FANOUT {n} waiters in Project p, 200 children under one root, Project q has one unrelated running Task");
    let ops = [
        ("execution heartbeat (lease renew)", "UPDATE execution SET lease_expires_at='2026-10-06T00:02:'||printf('%02d',?1 % 60)||'Z', execution_version=execution_version+1 WHERE id='e'"),
        ("execution heartbeat, other Project", "UPDATE execution SET lease_expires_at='2026-10-06T00:02:'||printf('%02d',?1 % 60)||'Z', execution_version=execution_version+1 WHERE id='eq'"),
        ("task status change", "UPDATE task SET status=CASE WHEN ?1 % 2=0 THEN 'review' ELSE 'in_progress' END, version=version+1 WHERE id='work'"),
        ("task metadata (ordinary key)", "UPDATE task SET metadata_json=json_object('note',?1) WHERE id='work'"),
        ("task version bump only", "UPDATE task SET version=version+1, updated_at='x'||?1 WHERE id='work'"),
        ("child status change (200 siblings)", "UPDATE task SET status=CASE WHEN ?1 % 2=0 THEN 'in_progress' ELSE 'todo' END, version=version+1 WHERE id='c-7'"),
        ("project settings edit", "UPDATE project SET settings=json_object('n',?1), version=version+1 WHERE id='p'"),
    ];
    for (name, sql) in ops {
        let (t, marked) = timed(&pool, 30, sql).await;
        println!("FANOUT no wait rows          {name:36} {t:>12?} marked={marked}");
    }
    // Every waiter waits on its Agent, as waiters behind a full Agent do.
    sqlx::query("INSERT INTO task_schedule_wait(task_id,project_id,agent_id) SELECT id,'p','agent-a' FROM task WHERE id LIKE 'w-%'")
        .execute(&pool).await.unwrap();
    for (name, sql) in ops {
        let (t, marked) = timed(&pool, 30, sql).await;
        println!("FANOUT {n:>5} agent waiters    {name:36} {t:>12?} marked={marked}");
    }
    // Every waiter waits on the Project limit and a machine run slot.
    sqlx::query("UPDATE task_schedule_wait SET project_capacity=1, daemon_id='*'")
        .execute(&pool)
        .await
        .unwrap();
    for (name, sql) in ops {
        let (t, marked) = timed(&pool, 30, sql).await;
        println!("FANOUT {n:>5} capacity waiters {name:36} {t:>12?} marked={marked}");
    }
    let triggers: Vec<String> = sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='trigger' AND (name LIKE 'task_schedule_%' OR name LIKE 'project_schedule_%')")
        .fetch_all(&pool).await.unwrap();
    for name in &triggers {
        sqlx::query(&format!("DROP TRIGGER {name}"))
            .execute(&pool)
            .await
            .unwrap();
    }
    for (name, sql) in ops {
        let (t, marked) = timed(&pool, 30, sql).await;
        println!(
            "FANOUT {} triggers dropped  {name:36} {t:>12?} marked={marked}",
            triggers.len()
        );
    }
    drop(pool);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}
