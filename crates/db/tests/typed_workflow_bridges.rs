use api_types::{ExecutionPurpose, TransitionBridgeKind as Kind};
use db::{
    create_sqlite_pool, run_migrations, run_migrations_from, ExecutionRepo, SqliteDb,
    TransitionLogRepo,
};
use serde_json::{json, Value};
use sqlx::Row;

type BridgeCase<'a> = (
    &'a str,
    String,
    &'a str,
    &'a str,
    &'a str,
    Option<&'a str>,
    Option<Value>,
    Option<Kind>,
    Option<Value>,
);

/// Upgrade a real populated file, including pending work and completed hook
/// checkpoints. Audit prose, queue identity and execution summaries survive.
#[tokio::test]
async fn populated_upgrade_backfills_bridges_queue_checkpoints_and_execution_purpose() {
    let temp = tempfile::tempdir().unwrap();
    let old_migrations = temp.path().join("old-migrations");
    std::fs::create_dir(&old_migrations).unwrap();
    let migrations = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    for entry in std::fs::read_dir(&migrations).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        if name.to_string_lossy().ends_with(".sql")
            && !name
                .to_string_lossy()
                .ends_with("__typed_workflow_bridges.sql")
            && !name.to_string_lossy().ends_with("__task_budgets.sql")
        {
            std::fs::copy(entry.path(), old_migrations.join(name)).unwrap();
        }
    }
    let url = format!("sqlite://{}", temp.path().join("upgrade.db").display());
    let pool = create_sqlite_pool(&url).await.unwrap();
    run_migrations_from(&pool, &old_migrations).await.unwrap();
    sqlx::query(
        "INSERT INTO project(id,name,created_at,updated_at) VALUES('p','Project','now','now')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t','p','Task','merging','now','now')")
        .execute(&pool).await.unwrap();
    let paths = vec![
        "src/a,b.rs".to_owned(),
        "日本語/[review-refresh].rs".to_owned(),
    ];
    let handoff = format!("[conflict-handoff] committed; paths_json={}", json!(paths));
    let cases: Vec<BridgeCase<'_>> = vec![
        (
            "refresh",
            "[review-refresh] fresh review".into(),
            "merging",
            "merge_failed",
            "system:workflow",
            None,
            None,
            Some(Kind::ReviewRefresh),
            None,
        ),
        (
            "rebase",
            "[review-refresh] [target-moved-rebase] rebased".into(),
            "merging",
            "merge_failed",
            "system:workflow",
            None,
            None,
            Some(Kind::TargetMovedRebase),
            None,
        ),
        (
            "handoff",
            handoff.clone(),
            "merging",
            "merge_failed",
            "system:workflow",
            None,
            None,
            Some(Kind::ConflictHandoff),
            Some(json!({"paths": paths})),
        ),
        (
            "malformed",
            "[conflict-handoff]; paths_json=broken".into(),
            "merging",
            "merge_failed",
            "system:workflow",
            None,
            None,
            Some(Kind::ConflictHandoff),
            None,
        ),
        (
            "wrong-shape",
            "[conflict-handoff]; paths_json=[42]".into(),
            "merging",
            "merge_failed",
            "system:workflow",
            None,
            None,
            Some(Kind::ConflictHandoff),
            None,
        ),
        (
            "missing",
            "[conflict-handoff] no paths".into(),
            "merging",
            "merge_failed",
            "system:workflow",
            None,
            None,
            Some(Kind::ConflictHandoff),
            None,
        ),
        (
            "empty",
            "[conflict-handoff]; paths_json=[]".into(),
            "merging",
            "merge_failed",
            "system:workflow",
            None,
            None,
            Some(Kind::ConflictHandoff),
            Some(json!({"paths": []})),
        ),
        (
            "last-suffix",
            "[conflict-handoff]; paths_json=discarded; paths_json=[\"last.rs\"]".into(),
            "merging",
            "merge_failed",
            "system:workflow",
            None,
            None,
            Some(Kind::ConflictHandoff),
            Some(json!({"paths": ["last.rs"]})),
        ),
        (
            "invalid-last",
            "[conflict-handoff]; paths_json=[\"first.rs\"]; paths_json=bad".into(),
            "merging",
            "merge_failed",
            "system:workflow",
            None,
            None,
            Some(Kind::ConflictHandoff),
            None,
        ),
        (
            "multiple",
            "[review-refresh] [conflict-handoff]; paths_json=[\"both.rs\"]".into(),
            "merging",
            "merge_failed",
            "system:workflow",
            None,
            None,
            Some(Kind::ConflictHandoff),
            Some(json!({"paths": ["both.rs"]})),
        ),
        (
            "reset",
            "Owner says [conflict-handoff]".into(),
            "merging",
            "merging",
            "user:action:retry",
            Some("retry"),
            Some(json!([{"action":"retry","phase":"action","outcome":"reset_budget"}])),
            Some(Kind::RetryWindowReset),
            Some(json!({"verb":"retry"})),
        ),
        (
            "restart",
            "restart".into(),
            "merging",
            "merging",
            "user:action:restart",
            Some("restart"),
            None,
            Some(Kind::RetryWindowReset),
            Some(json!({"verb":"restart"})),
        ),
        (
            "recovery",
            "one attempt".into(),
            "review",
            "review",
            "user:action:retry",
            Some("retry"),
            None,
            Some(Kind::Recovery),
            Some(json!({"verb":"retry"})),
        ),
        (
            "skip",
            "gate skipped: no role assigned".into(),
            "planning",
            "in_progress",
            "system:workflow",
            None,
            None,
            Some(Kind::GateSkipped),
            None,
        ),
        (
            "approve",
            "gate approved".into(),
            "review",
            "merging",
            "user:api",
            None,
            None,
            Some(Kind::GateApproved),
            None,
        ),
        (
            "reject",
            "gate rejected: fix this".into(),
            "review",
            "in_progress",
            "user:api",
            None,
            None,
            Some(Kind::GateRejected),
            None,
        ),
        (
            // Base read only the "gate approved" prefix as a decision.
            "custom-approve",
            "Looks good".into(),
            "review",
            "merging",
            "user:action:approve",
            None,
            None,
            None,
            None,
        ),
        (
            "send-back",
            "add tests".into(),
            "review",
            "in_progress",
            "user:action:send_back",
            None,
            None,
            None,
            None,
        ),
        (
            "agent-recovery",
            "agent grants one attempt".into(),
            "review",
            "review",
            "agent:project-agent",
            Some("retry"),
            None,
            Some(Kind::Recovery),
            Some(json!({"verb":"retry"})),
        ),
        (
            // The gate->target move after a recovery marker is a rejection.
            "recovery-target",
            "one attempt".into(),
            "review",
            "in_progress",
            "user:action:retry",
            Some("reject"),
            None,
            None,
            None,
        ),
        (
            "ci",
            "CI-only re-review passed".into(),
            "review",
            "merging",
            "system:workflow",
            None,
            None,
            Some(Kind::CiOnlyReviewPassed),
            None,
        ),
        (
            "carry",
            "[review-carry] carried authority".into(),
            "review",
            "merging",
            "system:workflow",
            None,
            None,
            Some(Kind::ReviewCarry),
            None,
        ),
        (
            "incidental",
            "CI command failed: '[conflict-handoff]'".into(),
            "merging",
            "merge_failed",
            "system:workflow",
            None,
            None,
            None,
            None,
        ),
        (
            "user-tag",
            "[conflict-handoff]; paths_json=[\"not-authorized.rs\"]".into(),
            "merging",
            "merge_failed",
            "user:api",
            None,
            None,
            None,
            None,
        ),
        (
            "unclassified-reset",
            "retry window reset".into(),
            "review",
            "review",
            "user:api",
            None,
            None,
            None,
            None,
        ),
    ];
    for (id, reason, from, to, actor, trigger, hooks, _, _) in &cases {
        sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_name,trigger_reason,hook_results_json,rejection,created_at) VALUES(?,'t',?,?,?,?,?,?,1,'now')")
            .bind(id).bind(from).bind(to).bind(actor).bind(trigger).bind(reason)
            .bind(hooks.as_ref().map(ToString::to_string)).execute(&pool).await.unwrap();
    }
    let saved = [
        (
            "cascade",
            "cascade",
            json!({"to":"merge_failed","reason":handoff}),
        ),
        (
            "hooks",
            "hooks",
            json!({"from":"merging","to":"merge_failed","reason":handoff,"transition_log_id":"handoff"}),
        ),
        (
            "command",
            "command",
            json!({"operation":"engine_transition","arguments":{"target_state":"merge_failed","actor":{"System":{"component":"Workflow"}},"reason":handoff}}),
        ),
        (
            "options",
            "command",
            json!({"operation":"transition","arguments":["t","merge_failed",{"triggered_by":{"System":{"component":"Workflow"}},"reason":handoff}]}),
        ),
    ];
    for (seq, (id, kind, payload)) in saved.iter().enumerate() {
        sqlx::query("INSERT INTO task_step(id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,expected_epoch,status,available_at,created_at,updated_at) VALUES(?,'t',?,?,?, ?,?,1,'merging',1,1,'pending','now','now','now')")
            .bind(id).bind(seq as i64 + 1).bind(kind).bind(payload.to_string()).bind(id).bind(id)
            .execute(&pool).await.unwrap();
    }
    let marker = json!({"id":"pending-reset","task_id":"t","from_state":"review","to_state":"review",
        "trigger_name":"retry","triggered_by":"user:action:retry","trigger_reason":"Owner starts a window",
        "hook_results_json":json!([{"action":"retry","phase":"action","outcome":"reset_budget"}]).to_string(),
        "rejection":false,"created_at":"now"});
    let input = db::UpdateTask {
        id: "t".into(),
        expected_version: 1,
        title: None,
        description: None,
        priority: None,
        merge_config: None,
        plan: None,
        error_annotation: None,
        blocked_json: None,
        failed_json: None,
        task_state_config: None,
        parent_task_id: None,
        updated_at: "now".into(),
    };
    let mutation = json!({"TaskUpdateWithRecoveryMarker":{"input":input,"marker":marker}});
    sqlx::query("INSERT INTO task_step(id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,expected_epoch,status,available_at,created_at,updated_at) VALUES('mutation','t',5,'mutation',?,'mutation','mutation',1,'merging',1,1,'pending','now','now','now')")
        .bind(mutation.to_string()).execute(&pool).await.unwrap();
    let checkpoint =
        json!({"Cascade":{"to":"merging","reason":"[review-carry] resumed authority"}});
    sqlx::query("INSERT INTO task_hook_checkpoint(step_id,hook_index,started_at,result_json) VALUES('hooks',0,'now',?)")
        .bind(checkpoint.to_string()).execute(&pool).await.unwrap();
    for (id, status, summary) in [
        (
            "e1",
            "completed",
            "[Forge automatic review recovery] old attempt",
        ),
        (
            "e2",
            "running",
            "[Forge automatic review recovery] running attempt",
        ),
        ("e3", "failed", "ordinary attempt"),
    ] {
        sqlx::query("INSERT INTO execution(id,task_id,role,status,summary,created_at,updated_at) VALUES(?,'t','coder',?,?, 'now','now')")
            .bind(id).bind(status).bind(summary).execute(&pool).await.unwrap();
    }
    pool.close().await;
    let pool = create_sqlite_pool(&url).await.unwrap();
    run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool.clone());
    let rows = TransitionLogRepo::list_by_task(&db, "t").await.unwrap();
    for (id, reason, _, _, _, _, _, expected_kind, expected_payload) in &cases {
        let row = rows.iter().find(|row| row.id == *id).unwrap();
        assert_eq!(&row.trigger_reason, reason, "prose changed for {id}");
        assert_eq!(&row.bridge.bridge_kind, expected_kind, "kind for {id}");
        assert_eq!(
            &row.bridge.bridge_payload, expected_payload,
            "payload for {id}"
        );
        assert!(row.rejection, "historical rejection changed for {id}");
        assert_eq!(row.created_at, "now");
    }
    for (id, _, original) in &saved {
        let row = sqlx::query("SELECT payload_json,status,seq FROM task_step WHERE id=?")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let updated: Value = serde_json::from_str(row.get::<&str, _>("payload_json")).unwrap();
        let pointer = match *id {
            "command" => "/arguments",
            "options" => "/arguments/2",
            _ => "",
        };
        let content = updated.pointer(pointer).unwrap();
        assert_eq!(content["bridge_kind"], "conflict_handoff");
        assert_eq!(content["bridge_payload"], json!({"paths":paths}));
        assert_eq!(
            content["reason"],
            original.pointer(pointer).unwrap()["reason"]
        );
        assert_eq!(row.get::<&str, _>("status"), "pending");
    }
    let mutation: String =
        sqlx::query_scalar("SELECT payload_json FROM task_step WHERE id='mutation'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let mutation: Value = serde_json::from_str(&mutation).unwrap();
    assert_eq!(
        mutation["TaskUpdateWithRecoveryMarker"]["marker"]["bridge_kind"],
        "retry_window_reset"
    );
    assert_eq!(
        mutation["TaskUpdateWithRecoveryMarker"]["marker"]["bridge_payload"],
        json!({"verb":"retry"})
    );
    assert_eq!(
        mutation["TaskUpdateWithRecoveryMarker"]["marker"]["trigger_reason"],
        "Owner starts a window"
    );
    assert_eq!(
        mutation["TaskUpdateWithRecoveryMarker"]["input"]["expected_version"],
        1
    );
    let _: db::TaskMutation = serde_json::from_value(mutation).unwrap();
    let checkpoint: String =
        sqlx::query_scalar("SELECT result_json FROM task_hook_checkpoint WHERE step_id='hooks'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let checkpoint: Value = serde_json::from_str(&checkpoint).unwrap();
    assert_eq!(checkpoint["Cascade"]["bridge_kind"], "review_carry");
    assert_eq!(
        checkpoint["Cascade"]["reason"],
        "[review-carry] resumed authority"
    );
    // Base counted only attempts whose prompt summary survived, i.e. the
    // unfinished ones: the running attempt, not the completed one.
    let purposes: Vec<(String, Option<String>)> =
        sqlx::query_as("SELECT id, purpose FROM execution ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        purposes,
        vec![
            ("e1".into(), Some("automatic_review_recovery".into())),
            ("e2".into(), Some("automatic_review_recovery".into())),
            ("e3".into(), None),
        ]
    );
    let count = || {
        sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM execution WHERE task_id='t' AND purpose='automatic_review_recovery' AND status='running'").fetch_one(db.pool())
    };
    assert_eq!(count().await.unwrap(), 1);
    assert!(ExecutionRepo::has_running_by_task_and_purpose(
        &db,
        "t",
        ExecutionPurpose::AutomaticReviewRecovery
    )
    .await
    .unwrap());
    sqlx::query("UPDATE execution SET summary='user replaced summary' WHERE id IN ('e1','e2')")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(count().await.unwrap(), 1, "summary text never counts");
    sqlx::query("UPDATE execution SET status='completed' WHERE id='e2'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        count().await.unwrap(),
        0,
        "a finished attempt stops counting"
    );
    run_migrations(&pool).await.unwrap();
    assert_eq!(
        TransitionLogRepo::list_by_task(&db, "t")
            .await
            .unwrap()
            .len(),
        cases.len()
    );
}

async fn pre_bridge_pool(name: &str) -> (tempfile::TempDir, String, sqlx::SqlitePool) {
    let temp = tempfile::tempdir().unwrap();
    let old = temp.path().join("old");
    std::fs::create_dir(&old).unwrap();
    let migrations = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    for entry in std::fs::read_dir(&migrations).unwrap() {
        let entry = entry.unwrap();
        let file = entry.file_name().to_string_lossy().to_string();
        if file.ends_with(".sql")
            && !file.ends_with("__typed_workflow_bridges.sql")
            && !file.ends_with("__task_budgets.sql")
        {
            std::fs::copy(entry.path(), old.join(&file)).unwrap();
        }
    }
    let url = format!(
        "sqlite://{}",
        temp.path().join(format!("{name}.db")).display()
    );
    let pool = create_sqlite_pool(&url).await.unwrap();
    run_migrations_from(&pool, &old).await.unwrap();
    sqlx::query(
        "INSERT INTO project(id,name,created_at,updated_at) VALUES('p','Project','now','now')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t','p','Task','review','now','now')")
        .execute(&pool)
        .await
        .unwrap();
    (temp, url, pool)
}

async fn log(pool: &sqlx::SqlitePool, id: &str, from: &str, to: &str, actor: &str, reason: &str) {
    sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,rejection,created_at) VALUES(?,'t',?,?,?,?,0,'now')")
        .bind(id).bind(from).bind(to).bind(actor).bind(reason)
        .execute(pool).await.unwrap();
}

/// Base `gate_decision_since_entry` was `starts_with("gate approved")` /
/// `starts_with("gate rejected")` (case-sensitive, any actor). History that
/// base did not treat as a gate decision must not become one after backfill.
#[tokio::test]
async fn backfill_gate_decisions_match_base_reader() {
    let (_temp, url, pool) = pre_bridge_pool("gate").await;
    // Base: not a decision (capitalised; SQLite LIKE is ASCII case-insensitive).
    log(
        &pool,
        "capital",
        "review",
        "merging",
        "user:api",
        "Gate approved by owner",
    )
    .await;
    // Base: not a decision (approve with custom guidance).
    log(
        &pool,
        "guided",
        "review",
        "merging",
        "user:action:approve",
        "LGTM, ship it",
    )
    .await;
    // Base: not a decision (send_back guidance).
    log(
        &pool,
        "sendback",
        "review",
        "in_progress",
        "user:action:send_back",
        "add tests",
    )
    .await;
    pool.close().await;
    let pool = create_sqlite_pool(&url).await.unwrap();
    run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    let rows = TransitionLogRepo::list_by_task(&db, "t").await.unwrap();
    let kinds: Vec<_> = rows
        .iter()
        .map(|row| (row.id.clone(), row.bridge.bridge_kind))
        .collect();
    assert!(
        rows.iter().all(|row| row.bridge.bridge_kind.is_none()),
        "rows base did not classify as gate decisions were backfilled as decisions: {kinds:?}"
    );
}

/// Huge reasons and many path delimiters: the backfill must stay bounded.
#[tokio::test]
async fn backfill_huge_reasons_stays_bounded() {
    let (_temp, url, pool) = pre_bridge_pool("huge").await;
    sqlx::query("UPDATE task SET status='merge_failed' WHERE id='t'")
        .execute(&pool)
        .await
        .unwrap();
    let paths: Vec<String> = (0..20_000)
        .map(|i| format!("src/module_{i}/file_{i}.rs"))
        .collect();
    let one = format!(
        "[conflict-handoff] rebased onto main; conflicts were committed with markers in: {}; paths_json={}",
        paths.join(", "),
        serde_json::to_string(&paths).unwrap()
    );
    log(
        &pool,
        "one-delimiter",
        "merging",
        "merge_failed",
        "system:workflow",
        &one,
    )
    .await;
    let many = format!(
        "[conflict-handoff] x{}; paths_json=[\"last.rs\"]",
        "; paths_json=junk-padding-padding-padding-padding".repeat(2_000)
    );
    log(
        &pool,
        "many-delimiters",
        "merging",
        "merge_failed",
        "system:workflow",
        &many,
    )
    .await;
    pool.close().await;
    let pool = create_sqlite_pool(&url).await.unwrap();
    let started = std::time::Instant::now();
    run_migrations(&pool).await.unwrap();
    let elapsed = started.elapsed();
    eprintln!(
        "bytes one={} many={} migration={elapsed:?}",
        one.len(),
        many.len()
    );
    let db = SqliteDb::new(pool);
    let rows = TransitionLogRepo::list_by_task(&db, "t").await.unwrap();
    let one_row = rows.iter().find(|row| row.id == "one-delimiter").unwrap();
    assert_eq!(one_row.trigger_reason, one);
    assert_eq!(one_row.bridge.conflict_paths().unwrap().len(), paths.len());
    let many_row = rows.iter().find(|row| row.id == "many-delimiters").unwrap();
    assert_eq!(
        many_row.bridge.conflict_paths().unwrap(),
        vec!["last.rs".to_owned()]
    );
    assert!(elapsed < std::time::Duration::from_secs(10), "{elapsed:?}");
}

/// Base counted automatic review-recovery attempts with
/// `summary LIKE '[Forge automatic review recovery]%'`. The runner's
/// terminalization (`runner.rs` `summary: Some(result.summary)`) replaces the
/// prompt with the agent's summary, so base stopped counting a finished
/// attempt. The typed purpose count must agree with base for the same rows.
#[tokio::test]
async fn recovery_attempt_count_matches_base_after_terminal_summary() {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool.clone());
    sqlx::query(
        "INSERT INTO project(id,name,created_at,updated_at) VALUES('p','Project','now','now')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t','p','Task','review','now','now')")
        .execute(&pool)
        .await
        .unwrap();
    // A new attempt as 2.4 admits it: purpose set, prompt summary without the
    // old bracket prefix.
    sqlx::query("INSERT INTO execution(id,task_id,role,status,summary,purpose,created_at,updated_at) VALUES('r1','t','coder','running','[Forge automatic review recovery]\n\n...','automatic_review_recovery','now','now')")
        .execute(&pool)
        .await
        .unwrap();
    // Terminalization replaces the summary with the agent's own text.
    sqlx::query(
        "UPDATE execution SET status='completed', summary='Fixed the failing check' WHERE id='r1'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let base_equivalent: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution WHERE task_id='t' AND summary LIKE '[Forge automatic review recovery]%'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let typed = sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM execution WHERE task_id='t' AND purpose='automatic_review_recovery' AND status='running'").fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        typed, base_equivalent,
        "typed recovery count diverges from base once the attempt finishes"
    );
}

/// Kind and purpose columns are open TEXT. A value this build does not know
/// (for example from a newer binary) is stored, and reading it back is a
/// typed error rather than a panic or a silently unclassified row.
#[tokio::test]
async fn unknown_stored_kind_and_purpose_are_typed_not_panics() {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool.clone());
    sqlx::query(
        "INSERT INTO project(id,name,created_at,updated_at) VALUES('p','Project','now','now')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t','p','Task','review','now','now')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,rejection,created_at,bridge_kind) VALUES('future','t','review','merging','system:workflow','later kind',0,'now','owner_fixable_fail')")
        .execute(&pool)
        .await
        .unwrap();
    match TransitionLogRepo::list_by_task(&db, "t").await {
        Err(db::DbError::TransitionBridgeCorrupt {
            transition_log_id,
            reason,
        }) => {
            assert_eq!(transition_log_id, "future");
            assert!(reason.contains("owner_fixable_fail"), "{reason}");
        }
        other => panic!("expected a typed bridge error, got {other:?}"),
    }
    assert!(matches!(
        db::TransitionLogRepo::list_by_tasks(&db, &["t"]).await,
        Err(db::DbError::TransitionBridgeCorrupt { .. })
    ));
    // The payload keeps its JSON check.
    assert!(sqlx::query(
        "UPDATE transition_log SET bridge_kind=NULL, bridge_payload='not json' WHERE id='future'"
    )
    .execute(&pool)
    .await
    .is_err());
    // A purpose this build does not know is stored and never counted.
    sqlx::query("INSERT INTO execution(id,task_id,role,status,purpose,created_at,updated_at) VALUES('x','t','coder','running','later_purpose','now','now')")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM execution WHERE task_id='t' AND purpose='automatic_review_recovery' AND status='running'").fetch_one(db.pool())
        .await
        .unwrap(),
        0
    );
    assert!("later_purpose".parse::<ExecutionPurpose>().is_err());
}
