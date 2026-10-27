//! Check history retention and reference preservation tests.
use super::*;

const OLD: &str = "2026-08-01T00:00:00Z";
const CUTOFF: &str = "2026-09-08T00:00:00Z";
const RECENT: &str = "2026-10-07T00:00:00Z";

async fn run(db: &SqliteDb, id: &str, state: &str, at: &str) {
    let finished = matches!(state, "succeeded" | "failed" | "cancelled").then_some(at);
    sqlx::query("INSERT INTO check_run(id,project_id,repo_id,commit_sha,spec_digest,identity_key,input_json,cacheable,state,operation_id,created_at,updated_at,finished_at) VALUES(?,'p','r','c','d',?,'{}',1,?,?,?,?,?)")
        .bind(id).bind(format!("identity-{id}")).bind(state).bind(format!("op-{id}")).bind(at).bind(at).bind(finished)
        .execute(db.pool()).await.unwrap();
}
async fn result(db: &SqliteDb, id: &str, run: &str, at: &str) {
    sqlx::query("INSERT INTO check_result(id,run_id,identity_key,outcome,cleanup,certified,cacheable,steps_json,output_truncated,created_at) VALUES(?,?,?,'pass','success',1,1,'[]',0,?)")
        .bind(id).bind(run).bind(format!("identity-{run}")).bind(at).execute(db.pool()).await.unwrap();
}
/// A consumer of Task `t` at `epoch`; `settled` is `cancelled`,
/// `applied` or `live`.
async fn consumer(
    db: &SqliteDb,
    id: &str,
    run: Option<&str>,
    result: Option<&str>,
    epoch: i64,
    settled: &str,
    at: &str,
) {
    sqlx::query("INSERT INTO check_consumer(id,project_id,repo_id,task_id,status_epoch,origin,request_key,identity_key,run_id,result_id,created_at,cancelled_at,applied_at,delivery_step_id) VALUES(?,'p','r','t',?,'entry',?,'identity',?,?,?,?,?,?)")
        .bind(id).bind(epoch).bind(format!("{epoch}|{id:0>36}")).bind(run).bind(result).bind(at)
        .bind((settled == "cancelled").then_some(at))
        .bind((settled == "applied").then_some(at))
        .bind((settled == "applied").then_some(format!("delivery-{id}")))
        .execute(db.pool()).await.unwrap();
}
async fn ids(db: &SqliteDb, table: &str) -> Vec<String> {
    sqlx::query_scalar(&format!("SELECT id FROM {table} ORDER BY id"))
        .fetch_all(db.pool())
        .await
        .unwrap()
}

/// What goes and, case by case, what stays. The Task `t` is in status
/// epoch 0 (the fixture's), so a consumer of epoch 0 that is neither
/// cancelled nor applied is live.
#[tokio::test]
async fn retention_deletes_finished_history_and_keeps_everything_still_readable() {
    let db = fixture().await;
    // Goes: an old terminal run, its result and its applied consumer.
    run(&db, "gone", "succeeded", OLD).await;
    result(&db, "gone-result", "gone", OLD).await;
    consumer(
        &db,
        "gone-applied",
        Some("gone"),
        Some("gone-result"),
        0,
        "applied",
        OLD,
    )
    .await;
    // Goes: an old cancelled consumer, and a consumer of an entry the
    // Task left (another epoch), with their run.
    run(&db, "gone-too", "cancelled", OLD).await;
    consumer(
        &db,
        "gone-cancelled",
        Some("gone-too"),
        None,
        0,
        "cancelled",
        OLD,
    )
    .await;
    consumer(&db, "gone-left", Some("gone-too"), None, 7, "live", OLD).await;

    // Stays: everything recent, whatever its state.
    run(&db, "recent", "succeeded", RECENT).await;
    result(&db, "recent-result", "recent", RECENT).await;
    consumer(
        &db,
        "recent-applied",
        Some("recent"),
        Some("recent-result"),
        0,
        "applied",
        RECENT,
    )
    .await;
    // Stays: an old live consumer (its Task still waits in that entry),
    // with its queued run.
    run(&db, "queued", "queued", OLD).await;
    consumer(&db, "live", Some("queued"), None, 0, "live", OLD).await;
    // Stays: an old reusable result that a recent request hit. The
    // recent consumer names it, so run and result are kept.
    run(&db, "reused", "succeeded", OLD).await;
    result(&db, "reused-result", "reused", OLD).await;
    consumer(
        &db,
        "reuse-old",
        Some("reused"),
        Some("reused-result"),
        0,
        "applied",
        OLD,
    )
    .await;
    consumer(
        &db,
        "reuse-hit",
        Some("reused"),
        Some("reused-result"),
        0,
        "applied",
        RECENT,
    )
    .await;
    // Stays: an old consumer a suspended hooks step still awaits, one
    // whose delivery step is not finished, one with an undelivered
    // result, and one that belongs to an open Review attempt.
    run(&db, "held", "succeeded", OLD).await;
    result(&db, "held-result", "held", OLD).await;
    consumer(&db, "awaited", Some("held"), None, 0, "cancelled", OLD).await;
    consumer(
        &db,
        "delivering",
        Some("held"),
        Some("held-result"),
        0,
        "applied",
        OLD,
    )
    .await;
    consumer(
        &db,
        "undelivered",
        Some("held"),
        Some("held-result"),
        7,
        "live",
        OLD,
    )
    .await;
    consumer(
        &db,
        "reviewed",
        Some("held"),
        Some("held-result"),
        0,
        "applied",
        OLD,
    )
    .await;
    for (id, status, awaited) in [
        ("awaiting-step", "suspended", Some("awaited")),
        ("delivery-delivering", "pending", None),
        ("delivery-gone-applied", "done", None),
    ] {
        sqlx::query("INSERT INTO task_step(id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,status,available_at,created_at,updated_at,awaited_consumer_id,suspended_until) VALUES(?,'t',(SELECT COALESCE(MAX(seq),0)+1 FROM task_step),'hooks','{}',?,?,1,'todo',1,?,?,?,?,?,?)")
            .bind(id).bind(id).bind(id).bind(status).bind(OLD).bind(OLD).bind(OLD).bind(awaited)
            .bind((status == "suspended").then_some(RECENT))
            .execute(db.pool()).await.unwrap();
    }
    sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES('e','t','coder','completed',?,?)").bind(OLD).bind(OLD).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,created_at,updated_at) VALUES(?,'t','e',1,'awaiting_human','{}',?,?,?)")
        .bind(format!("{:0>36}", "reviewed")).bind(OLD).bind(OLD).bind(OLD)
        .execute(db.pool()).await.unwrap();

    let pruned = db.prune_check_history(CUTOFF, 100).await.unwrap();
    assert_eq!(
        pruned,
        CheckHistoryPruned {
            consumers: 4,
            runs: 2
        }
    );
    assert_eq!(
        ids(&db, "check_consumer").await,
        [
            "awaited",
            "delivering",
            "live",
            "recent-applied",
            "reuse-hit",
            "reviewed",
            "undelivered"
        ]
    );
    assert_eq!(
        ids(&db, "check_run").await,
        ["held", "queued", "recent", "reused"]
    );
    assert_eq!(
        ids(&db, "check_result").await,
        ["held-result", "recent-result", "reused-result"],
        "a deleted run takes its results and output tails with it"
    );
    // Nothing more goes on a second pass, and nothing dangles.
    assert_eq!(
        db.prune_check_history(CUTOFF, 100).await.unwrap(),
        CheckHistoryPruned::default()
    );
    let dangling: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM check_consumer c WHERE (c.run_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM check_run r WHERE r.id=c.run_id)) OR (c.result_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM check_result x WHERE x.id=c.result_id))")
        .fetch_one(db.pool()).await.unwrap();
    assert_eq!(dangling, 0);

    // Once the things that held them are over, they go too.
    sqlx::query("UPDATE task_step SET status='done', suspended_until=NULL")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE review SET status='passed'")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE check_consumer SET delivery_step_id='delivery-undelivered', applied_at=created_at WHERE id='undelivered'").execute(db.pool()).await.unwrap();
    assert_eq!(
        db.prune_check_history(CUTOFF, 100).await.unwrap(),
        CheckHistoryPruned {
            consumers: 4,
            runs: 1
        }
    );
    assert_eq!(ids(&db, "check_run").await, ["queued", "recent", "reused"]);
}

/// Batches are bounded, and a run finished inside the owner
/// acknowledgement window is kept however short the retention.
#[tokio::test]
async fn retention_is_batched_and_never_deletes_a_run_inside_the_acknowledgement_window() {
    let db = fixture().await;
    for n in 0..5 {
        let id = format!("old-{n}");
        run(&db, &id, "failed", OLD).await;
        consumer(&db, &format!("c-{n}"), Some(&id), None, 0, "cancelled", OLD).await;
    }
    let first = db.prune_check_history(CUTOFF, 2).await.unwrap();
    assert_eq!(first.consumers, 2);
    assert!(first.runs <= 2);
    let mut passes = 1;
    while db.prune_check_history(CUTOFF, 2).await.unwrap() != CheckHistoryPruned::default() {
        passes += 1;
        assert!(passes < 10);
    }
    assert!(ids(&db, "check_run").await.is_empty());
    assert!(ids(&db, "check_consumer").await.is_empty());

    // Finished an hour ago, with a cutoff in the future (a retention
    // shorter than the acknowledgement window): the run stays.
    let hour_ago = (chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
    let future = (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
    run(&db, "fresh", "succeeded", &hour_ago).await;
    assert_eq!(
        db.prune_check_history(&future, 100).await.unwrap(),
        CheckHistoryPruned::default()
    );
    assert_eq!(ids(&db, "check_run").await, ["fresh"]);
}

async fn hooks_step(db: &SqliteDb, id: &str, status: &str, ci_consumer: &str) {
    sqlx::query("INSERT INTO task_step(id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,status,available_at,created_at,updated_at) VALUES(?,'t',(SELECT COALESCE(MAX(seq),0)+1 FROM task_step),'hooks','{}',?,?,1,'todo',1,?,?,?,?)")
        .bind(id).bind(id).bind(id).bind(status).bind(OLD).bind(OLD).bind(OLD)
        .execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO task_hook_checkpoint(step_id,hook_index,started_at,effects_json) VALUES(?,0,?,json_object('ci_consumer',?,'ci_rerequests',1))")
        .bind(id).bind(OLD).bind(ci_consumer).execute(db.pool()).await.unwrap();
}
async fn attempt(db: &SqliteDb, id: &str, task: Option<&str>, state: &str) {
    sqlx::query("INSERT OR IGNORE INTO integration_queue(id,repo_id,target_branch,state,updated_at,created_at) VALUES('q','r','main','open',?,?)")
        .bind(OLD).bind(OLD).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO integration_attempt(id,queue_id,task_id,task_ref,project_ref,queue_seq,current,admission_key,expected_status,expected_epoch,observed_task_version,enqueued_at,state,updated_at,created_at) VALUES(?,'q',?,'t','p',(SELECT COUNT(*)+1 FROM integration_attempt),0,?,'merging',0,1,?,?,?,?)")
        .bind(id).bind(task).bind(id).bind(OLD).bind(state).bind(OLD).bind(OLD)
        .execute(db.pool()).await.unwrap();
}

/// References that are not columns of their own. A consumer a hook
/// checkpoint of an unfinished step names (the step was woken and then
/// parked, so it no longer awaits the consumer and its owner can still
/// drive it again), an integration
/// consumer whose attempt is not final, and a run or result that such an
/// attempt names by operation id or inside its JSON: all are kept until
/// their reader is finished, and go afterwards.
#[tokio::test]
async fn retention_keeps_what_a_checkpoint_or_an_unfinished_integration_attempt_names() {
    let db = fixture().await;
    // The entry's hook recorded this consumer; its step is pending again.
    run(&db, "hook-run", "cancelled", OLD).await;
    consumer(&db, "hook-c", Some("hook-run"), None, 0, "cancelled", OLD).await;
    hooks_step(&db, "woken", "parked", "hook-c").await;
    // An integration consumer (applied) of an attempt still `checking`.
    run(&db, "queue-run", "succeeded", OLD).await;
    result(&db, "queue-result", "queue-run", OLD).await;
    consumer(
        &db,
        "queue-c",
        Some("queue-run"),
        Some("queue-result"),
        0,
        "applied",
        OLD,
    )
    .await;
    sqlx::query("UPDATE check_consumer SET origin='integration', request_key='v1|integration|t|0|attempt-checking' WHERE id='queue-c'").execute(db.pool()).await.unwrap();
    attempt(&db, "attempt-checking", Some("t"), "checking").await;
    // Runs nobody consumes that an unfinished attempt names: by its
    // operation, by run id and by result id in its JSON.
    attempt(&db, "attempt-names", None, "awaiting_task_step").await;
    for id in ["by-operation", "by-run-id", "by-result-id", "unnamed"] {
        run(&db, id, "succeeded", OLD).await;
        result(&db, &format!("{id}-result"), id, OLD).await;
    }
    sqlx::query("UPDATE integration_attempt SET operation_id='op-by-operation', checks_json=json_object('run','by-run-id'), operation_receipts_json=json_array(json_object('result','by-result-id-result')) WHERE id='attempt-names'").execute(db.pool()).await.unwrap();

    let mut passes = 0;
    while db.prune_check_history(CUTOFF, 100).await.unwrap() != CheckHistoryPruned::default() {
        passes += 1;
        assert!(passes < 5);
    }
    assert_eq!(ids(&db, "check_consumer").await, ["hook-c", "queue-c"]);
    assert_eq!(
        ids(&db, "check_run").await,
        [
            "by-operation",
            "by-result-id",
            "by-run-id",
            "hook-run",
            "queue-run"
        ]
    );
    assert_eq!(ids(&db, "check_result").await.len(), 4);

    // Their readers finish: everything goes.
    sqlx::query("UPDATE task_step SET status='done'")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE integration_attempt SET state='completed'")
        .execute(db.pool())
        .await
        .unwrap();
    while db.prune_check_history(CUTOFF, 100).await.unwrap() != CheckHistoryPruned::default() {
        passes += 1;
        assert!(passes < 10);
    }
    assert!(ids(&db, "check_consumer").await.is_empty());
    assert!(ids(&db, "check_run").await.is_empty());
    assert!(ids(&db, "check_result").await.is_empty());
}

/// The first pass after an upgrade, on a database with months of check
/// history: 20,000 old runs with their results and consumers and 2,000
/// that must stay. Every batch is one short write (nothing here may hold
/// the write lock for long: other writers wait behind it), the whole
/// history goes in a bounded number of batches, and what must stay stays.
#[tokio::test]
async fn retention_clears_months_of_history_in_short_batches() {
    let db = fixture().await;
    sqlx::raw_sql(&format!(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<22000)
             INSERT INTO check_run(id,project_id,repo_id,commit_sha,spec_digest,identity_key,input_json,cacheable,state,operation_id,created_at,updated_at,finished_at)
             SELECT 'run-'||i,'p','r','c','d','identity-'||i,'{{}}',1,'succeeded','op-'||i,
                    CASE WHEN i<=20000 THEN '{OLD}' ELSE '{RECENT}' END,CASE WHEN i<=20000 THEN '{OLD}' ELSE '{RECENT}' END,CASE WHEN i<=20000 THEN '{OLD}' ELSE '{RECENT}' END FROM n;
             INSERT INTO check_result(id,run_id,identity_key,outcome,cleanup,certified,cacheable,steps_json,output_truncated,created_at)
             SELECT 'result-'||substr(id,5),id,identity_key,'pass','success',1,1,json_array(json_object('tail',printf('%.*c',2000,'x'))),0,created_at FROM check_run;
             INSERT INTO check_consumer(id,project_id,repo_id,task_id,status_epoch,origin,request_key,identity_key,run_id,result_id,created_at,applied_at,delivery_step_id)
             SELECT 'consumer-'||substr(id,5),'p','r','t',7,'entry','7|'||id,identity_key,id,'result-'||substr(id,5),created_at,created_at,'delivery-'||id FROM check_run;"
    ))
    .execute(db.pool())
    .await
    .unwrap();
    let started = std::time::Instant::now();
    let (mut batches, mut slowest, mut total) =
        (0, std::time::Duration::ZERO, CheckHistoryPruned::default());
    loop {
        let batch = std::time::Instant::now();
        let pruned = db.prune_check_history(CUTOFF, 100).await.unwrap();
        slowest = slowest.max(batch.elapsed());
        batches += 1;
        total.consumers += pruned.consumers;
        total.runs += pruned.runs;
        if pruned == CheckHistoryPruned::default() {
            break;
        }
        assert!(batches <= 201, "retention did not finish in 200 batches");
    }
    eprintln!(
        "retention of 20000 runs: {batches} batches, {:?} in all, slowest batch {slowest:?}",
        started.elapsed()
    );
    assert_eq!(
        total,
        CheckHistoryPruned {
            consumers: 20_000,
            runs: 20_000
        }
    );
    assert!(
        slowest < std::time::Duration::from_secs(1),
        "one retention batch held the write lock for {slowest:?}"
    );
    for table in ["check_run", "check_result", "check_consumer"] {
        let left: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(left, 2_000, "{table}");
    }
}

/// Every column that points at a check table must be accounted for by
/// the retention predicate. This enumerates them from the schema: every
/// foreign key into `check_run`, `check_result` or `check_consumer`, and
/// every column anywhere whose name says it holds one of their ids. A
/// reference added later (the integration queue will add its own) fails
/// here until `prune_check_history` keeps the rows it can still read
/// and the column is added to `CHECK_REFERENCES`.
#[tokio::test]
async fn every_reference_to_a_check_row_is_known_to_the_retention_predicate() {
    let db = fixture().await;
    let foreign: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT m.name, f.\"from\", f.\"table\" FROM sqlite_master m, pragma_foreign_key_list(m.name) f
             WHERE m.type='table' AND f.\"table\" IN ('check_run','check_result','check_consumer')",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    // Id columns without a foreign key, by name. The check tables' own
    // keys and the columns that point away from them are not references.
    let named: Vec<(String, String)> = sqlx::query_as(
        "SELECT m.name, c.name FROM sqlite_master m, pragma_table_info(m.name) c
             WHERE m.type='table'
               AND (c.name LIKE '%check\\_run\\_id%' ESCAPE '\\' OR c.name LIKE '%check\\_result\\_id%' ESCAPE '\\'
                    OR c.name LIKE '%check\\_consumer\\_id%' ESCAPE '\\' OR c.name LIKE '%check\\_id%' ESCAPE '\\'
                    OR c.name LIKE '%consumer\\_id%' ESCAPE '\\'
                    OR (m.name LIKE 'integration\\_%' ESCAPE '\\' AND (c.name LIKE '%run\\_id%' ESCAPE '\\' OR c.name LIKE '%result\\_id%' ESCAPE '\\')))",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    // Columns the name patterns match that hold something else.
    // These three name a Project milestone's acceptance check, which is
    // a different object from a check run.
    const NOT_A_CHECK_REFERENCE: &[(&str, &str)] = &[
        ("project_media_attachment", "acceptance_check_ids_json"),
        ("project_media_attachment", "milestone_check_id"),
        ("project_milestone_check_result", "check_id"),
    ];
    let mut found: Vec<(String, String, String)> = foreign;
    for (table, column) in named {
        if NOT_A_CHECK_REFERENCE.contains(&(table.as_str(), column.as_str()))
            || found.iter().any(|(t, c, _)| *t == table && *c == column)
        {
            continue;
        }
        let target = if column.contains("consumer") {
            "check_consumer"
        } else if column.contains("result") {
            "check_result"
        } else {
            "check_run"
        };
        found.push((table, column, target.to_owned()));
    }
    found.sort();
    found.dedup();
    let known: Vec<(String, String, String)> = CHECK_REFERENCES
        .iter()
        .map(|(table, column, target)| {
            (
                (*table).to_owned(),
                (*column).to_owned(),
                (*target).to_owned(),
            )
        })
        .collect();
    assert_eq!(
        found, known,
        "a column points at a check row that `prune_check_history` does not know: make the predicate keep what it can still read, then list it in CHECK_REFERENCES (or in NOT_A_CHECK_REFERENCE when it holds something else)"
    );

    // Ids inside JSON or free text have no column to enumerate. The
    // tables that consume checks are the ones that can carry one: every
    // JSON or payload column they have is classified in
    // CHECK_JSON_COLUMNS, so a new one fails here until somebody decides
    // whether it can name a check row and the predicate honours it.
    let mut json_columns: Vec<(String, String)> = sqlx::query_as(
        "SELECT m.name, c.name FROM sqlite_master m, pragma_table_info(m.name) c
             WHERE m.type='table'
               AND (m.name IN ('task_step','task_hook_checkpoint','review') OR m.name LIKE 'integration\\_attempt%' ESCAPE '\\')
               AND (c.name LIKE '%json%' OR c.name LIKE '%payload%' OR c.name LIKE '%receipt%' OR c.name LIKE '%effects%')",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    json_columns.sort();
    let mut classified: Vec<(String, String)> = CHECK_JSON_COLUMNS
        .iter()
        .map(|(table, column, _)| ((*table).to_owned(), (*column).to_owned()))
        .collect();
    classified.sort();
    assert_eq!(
        json_columns, classified,
        "a JSON column of a table that consumes checks is not classified in CHECK_JSON_COLUMNS"
    );
}
