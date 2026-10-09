use super::*;
use crate::{create_sqlite_pool, run_migrations, TaskStepRepo};
use sha2::{Digest, Sha256};

pub(super) const FIXED_TIME: &str = "2026-10-07T00:00:00Z";
pub(super) async fn fixture() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    seed(db.pool()).await;
    db
}
pub(super) async fn seed(pool: &crate::SqlitePool) {
    sqlx::query("INSERT INTO project(id,name,settings,workflow_definition,created_at,updated_at) VALUES('p','project','{}','{}',?,?)").bind(FIXED_TIME).bind(FIXED_TIME).execute(pool).await.unwrap();
    let path = std::env::temp_dir()
        .join("integration-target")
        .to_str()
        .unwrap()
        .to_owned();
    sqlx::query("INSERT INTO repo(id,project_id,name,local_path,default_branch,created_at,updated_at) VALUES('r','p','repo',?,'main',?,?)").bind(&path).bind(FIXED_TIME).bind(FIXED_TIME).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) VALUES('l','r','server',?,'primary_checkout',1,'ready',?,?)").bind(&path).bind(FIXED_TIME).bind(FIXED_TIME).execute(pool).await.unwrap();
    for id in ["a", "b"] {
        sqlx::query("INSERT INTO task(id,project_id,title,status,review_passed_at,created_at,updated_at) VALUES(?,'p',?,'merging',?,?,?)").bind(id).bind(id).bind(FIXED_TIME).bind(FIXED_TIME).bind(FIXED_TIME).execute(pool).await.unwrap();
    }
}
pub(super) fn admission(q: &IntegrationQueue, task: &str, key: &str) -> IntegrationAttempt {
    IntegrationAttempt::new(
        Some(q.id.clone()),
        task.into(),
        "p".into(),
        key.into(),
        "merging".into(),
        0,
        1,
    )
}

#[test]
fn attempt_graph_is_total_and_terminal_states_have_no_exit() {
    let mut seen = std::collections::HashSet::new();
    for (state, exits) in INTEGRATION_TRANSITIONS {
        assert!(
            seen.insert(state.to_string()),
            "duplicate graph row {state}"
        );
        assert_eq!(exits.is_empty(), state.terminal(), "state {state}");
        assert!(exits
            .iter()
            .all(|exit| exit != state && IntegrationAttemptState::ALL.contains(exit)));
    }
    assert_eq!(seen.len(), IntegrationAttemptState::ALL.len());
    assert!(!IntegrationAttemptState::FfInflight
        .exits()
        .contains(&IntegrationAttemptState::Cancelled));
    assert!(!IntegrationAttemptState::Reconciling
        .exits()
        .contains(&IntegrationAttemptState::Cancelled));
}
#[test]
fn stored_enums_round_trip_and_reject_unknown_values() {
    macro_rules! round_trip { ($($name:ident),+) => {$(for value in $name::ALL {assert_eq!(value.to_string().parse::<$name>().unwrap(),*value);assert_eq!(serde_json::from_str::<$name>(&serde_json::to_string(value).unwrap()).unwrap(),*value);}assert!("future".parse::<$name>().is_err());)+}; }
    round_trip!(
        IntegrationQueueState,
        IntegrationAttemptState,
        IntegrationOutcomeKind,
        IntegrationFailureKind,
        IntegrationOwnerKind,
        IntegrationOperationKind,
        IntegrationOperationState,
        IntegrationImportDisposition,
        IntegrationNeededFact
    );
}
#[test]
fn path_identity_keeps_rename_endpoints_case_and_unicode() {
    validate_integration_paths(&serde_json::json!([
        "Old/name", "new/name", "A", "a", "é", "e\u{301}"
    ]))
    .unwrap();
    for invalid in [
        serde_json::json!(["../escape"]),
        serde_json::json!(["a//b"]),
        serde_json::json!(["/absolute"]),
        serde_json::json!([null]),
        serde_json::json!({"encoding":"bytes"}),
    ] {
        assert!(validate_integration_paths(&invalid).is_err());
    }
    assert!(validate_branch("refs/heads/refs/heads/main").is_ok()); // strip one prefix, not repeated normalization
    for branch in [
        "",
        "../main",
        "main.lock",
        "main@{1}",
        "refs/heads/",
        "main\n",
        "a//b",
    ] {
        assert!(validate_branch(branch).is_err(), "{branch}");
    }
}
#[tokio::test]
async fn queue_creation_and_admission_are_idempotent_and_current_is_unique() {
    let db = fixture().await;
    let q = db
        .create_or_get_integration_queue("r", "refs/heads/main")
        .await
        .unwrap();
    assert_eq!(
        q,
        db.create_or_get_integration_queue("r", "main")
            .await
            .unwrap()
    );
    assert_eq!(q.target_location_id.as_deref(), Some("l"));
    let input = admission(&q, "a", "one");
    let a = db.admit_integration_attempt(input.clone()).await.unwrap();
    assert_eq!(a, db.admit_integration_attempt(input).await.unwrap());
    assert!(matches!(
        db.admit_integration_attempt(admission(&q, "a", "another"))
            .await,
        Err(DbError::VersionConflict)
    ));
    let b = db
        .admit_integration_attempt(admission(&q, "b", "two"))
        .await
        .unwrap();
    assert_eq!((a.queue_seq, b.queue_seq), (1, 2));
    assert_eq!(db.integration_members(&q.id, 10).await.unwrap(), vec![a, b]);
}
#[tokio::test]
async fn target_resolution_suspends_unconfigured_and_ambiguous_targets() {
    let db = fixture().await;
    sqlx::query("UPDATE repo_location SET is_default=0")
        .execute(db.pool())
        .await
        .unwrap();
    let unconfigured = db
        .create_or_get_integration_queue("r", "unconfigured")
        .await
        .unwrap();
    assert_eq!(unconfigured.state, IntegrationQueueState::Suspended);
    assert_eq!(
        unconfigured.last_error_kind,
        Some(IntegrationFailureKind::TargetUnconfigured)
    );
    sqlx::query("UPDATE repo_location SET is_default=1")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) SELECT 'second',repo_id,owner_kind,path,kind,1,status,created_at,updated_at FROM repo_location WHERE id='l'").execute(db.pool()).await.unwrap();
    let ambiguous = db
        .create_or_get_integration_queue("r", "ambiguous")
        .await
        .unwrap();
    assert_eq!(ambiguous.state, IntegrationQueueState::Suspended);
    assert_eq!(
        ambiguous.last_error_kind,
        Some(IntegrationFailureKind::TargetAmbiguous)
    );
    assert_eq!(ambiguous.target_location_id, None);
}
#[tokio::test]
async fn concurrent_head_claim_takeover_and_renewal_are_fenced() {
    let db = fixture().await;
    let q = db
        .create_or_get_integration_queue("r", "main")
        .await
        .unwrap();
    let a = db
        .admit_integration_attempt(admission(&q, "a", "one"))
        .await
        .unwrap();
    db.admit_integration_attempt(admission(&q, "b", "two"))
        .await
        .unwrap();
    let q = db.integration_queue(&q.id).await.unwrap().unwrap();
    let (first, second) = tokio::join!(
        db.claim_integration_queue(
            &q.id,
            q.revision,
            "first",
            "2026-10-08T00:00:00Z",
            "2026-10-08T00:01:00Z"
        ),
        db.claim_integration_queue(
            &q.id,
            q.revision,
            "second",
            "2026-10-08T00:00:00Z",
            "2026-10-08T00:01:00Z"
        )
    );
    assert_ne!(first.is_ok(), second.is_ok());
    let winner = first.or(second).unwrap();
    assert_eq!(winner.head_attempt_id, Some(a.id.clone()));
    let mut stale = a.clone();
    stale.state = IntegrationAttemptState::Validating;
    assert!(matches!(
        db.transition_integration_attempt(stale).await,
        Err(DbError::VersionConflict)
    ));

    let renewed = db
        .renew_integration_queue(
            &q.id,
            winner.revision,
            winner.lease_owner.as_deref().unwrap(),
            winner.fence_generation,
            "2026-10-08T00:00:30Z",
            "2026-10-08T00:02:00Z",
        )
        .await
        .unwrap();
    assert_eq!(renewed.fence_generation, 1);
    assert!(matches!(
        db.renew_integration_queue(
            &q.id,
            winner.revision,
            "stale",
            1,
            "2026-10-08T00:00:30Z",
            "2026-10-08T00:03:00Z"
        )
        .await,
        Err(DbError::VersionConflict)
    ));
    let takeover = db
        .claim_integration_queue(
            &q.id,
            renewed.revision,
            "takeover",
            "2026-10-08T00:02:01Z",
            "2026-10-08T00:03:00Z",
        )
        .await
        .unwrap();
    assert_eq!(takeover.fence_generation, 2);
    assert_eq!(takeover.head_attempt_id, Some(a.id.clone()));
    assert_eq!(db.integration_head(&q.id).await.unwrap().unwrap().id, a.id);
}
#[tokio::test]
async fn attempt_cas_and_successor_keep_reservation_and_history() {
    let db = fixture().await;
    let q = db
        .create_or_get_integration_queue("r", "main")
        .await
        .unwrap();
    let a = db
        .admit_integration_attempt(admission(&q, "a", "one"))
        .await
        .unwrap();
    let mut changed = a.clone();
    changed.state = IntegrationAttemptState::Parked;
    let changed = db.transition_integration_attempt(changed).await.unwrap();
    assert_eq!(changed.revision, 2);
    assert!(matches!(
        db.transition_integration_attempt(a.clone()).await,
        Err(DbError::VersionConflict)
    ));
    let successor = db
        .supersede_integration_attempt(
            &a.id,
            changed.revision,
            admission(&q, "a", "repair-successor"),
        )
        .await
        .unwrap();
    assert_eq!(successor.queue_seq, a.queue_seq);
    assert_eq!(successor.attempt_number, 2);
    assert_eq!(successor.predecessor_attempt_id, Some(a.id.clone()));
    assert_eq!(
        db.integration_attempt(&a.id).await.unwrap().unwrap().state,
        IntegrationAttemptState::Superseded
    );
    let mut terminal = successor.clone();
    terminal.state = IntegrationAttemptState::Cancelled;
    let terminal = db.transition_integration_attempt(terminal).await.unwrap();
    assert!(!terminal.current);
    assert!(matches!(
        db.transition_integration_attempt(terminal).await,
        Err(DbError::InvalidTransition)
    ));
}
#[tokio::test]
async fn observations_are_typed_idempotent_and_preserve_unknown_path_sets() {
    let db = fixture().await;
    let q = db
        .create_or_get_integration_queue("r", "main")
        .await
        .unwrap();
    let a = db
        .admit_integration_attempt(admission(&q, "a", "one"))
        .await
        .unwrap();
    let observation =
        IntegrationObservation::new("effect-1".into(), IntegrationOutcomeKind::Admission);
    db.record_integration_observation(&a.id, observation.clone())
        .await
        .unwrap();
    let before = db.integration_attempt(&a.id).await.unwrap().unwrap();
    db.record_integration_observation(&a.id, observation.clone())
        .await
        .unwrap();
    assert_eq!(
        before,
        db.integration_attempt(&a.id).await.unwrap().unwrap()
    );
    let mut conflict = observation;
    conflict.kind = IntegrationOutcomeKind::Done;
    assert!(matches!(
        db.record_integration_observation(&a.id, conflict).await,
        Err(DbError::IdempotencyConflict)
    ));
    assert!(before.changed_paths_json.is_none());
    assert_eq!(before.state, IntegrationAttemptState::Queued);
}

/// Hash the literal SQL values of every row in every pre-existing table.
/// `quote()` preserves TEXT bytes and BLOB identity; ordering is canonical.
pub(super) async fn legacy_digest(pool: &crate::SqlitePool, exclude_migrations: bool) -> String {
    let tables:Vec<String>=sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT IN ('integration_queue','integration_attempt') ORDER BY name").fetch_all(pool).await.unwrap();
    let mut digest = Sha256::new();
    for table in tables {
        if exclude_migrations && table == "_migration" {
            continue;
        }
        let quote = |name: &str| format!("\"{}\"", name.replace('"', "\"\""));
        let columns: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT name FROM pragma_table_info({}) ORDER BY cid",
            quote(&table)
        ))
        .fetch_all(pool)
        .await
        .unwrap();
        let select = columns
            .iter()
            .map(|c| format!("quote({})", quote(c)))
            .collect::<Vec<_>>()
            .join(",");
        let rows = sqlx::query(&format!("SELECT {select} FROM {}", quote(&table)))
            .fetch_all(pool)
            .await
            .unwrap();
        let mut values = rows
            .into_iter()
            .map(|row| {
                (0..columns.len())
                    .map(|i| row.try_get::<String, _>(i).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        values.sort();
        digest.update(serde_json::to_vec(&(table, columns, values)).unwrap());
    }
    hex::encode(digest.finalize())
}

fn import_fixture(priority: u8) -> IntegrationImportSnapshot {
    let mut s = IntegrationImportSnapshot {
        task: serde_json::json!({"id":"a","project_id":"p","status":"merging","status_epoch":0,"version":1,"review_passed_at":FIXED_TIME,"metadata":"{}"}),
        project: serde_json::json!({"id":"p"}),
        execution: Some(serde_json::json!({"id":"execution","workspace_id":"workspace"})),
        workspace: Some(
            serde_json::json!({"id":"workspace","repo_id":"r","default_branch":"main"}),
        ),
        ..Default::default()
    };
    let effect = |effects: Value| {
        let effects = effects
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::json!(v.to_string())))
            .collect::<serde_json::Map<String, Value>>();
        serde_json::json!({"effects_json":Value::Object(effects).to_string()})
    };
    match priority {
        1=>s.checkpoints.push(effect(serde_json::json!({"merge_intent":{"candidate_sha":"candidate","target_branch":"main"},"merge_outcome":{"Done":{"before_sha":"base","after_sha":"candidate","branch":"main"}}}))),
        2=>s.remote_operations.push(serde_json::json!({"operation_id":"original","state":"running"})),
        3=>s.task["status"]=serde_json::json!("cancelled"),
        4=>s.steps.push(serde_json::json!({"kind":"hooks","status":"claimed","expected_epoch":0})),
        5=>s.checkpoints.push(effect(serde_json::json!({"rebase_target":"base"}))),
        6=>s.checkpoints.push(effect(serde_json::json!({"rebase_outcome":{"kind":"rebased"}}))),
        7=>{s.task["status"]=serde_json::json!("merge_failed");s.transitions.push(serde_json::json!({"bridge_kind":"conflict_handoff","bridge_payload":"{\"paths\":[\"old.rs\",\"new.rs\"]}"}));},
        8=>s.transitions.push(serde_json::json!({"bridge_kind":"review_refresh"})),
        9=>s.carries.push(serde_json::json!({"commit_sha":"carried","base_sha":"base"})),
        10=>{s.task["metadata"]=serde_json::json!("{\"paused_integration\":{\"state\":\"merging\"}}");s.project["paused_at"]=serde_json::json!(FIXED_TIME);},
        11=>s.task["metadata"]=serde_json::json!("{\"paused_integration\":{\"state\":\"merging\"}}"),
        12=>s.task["metadata"]=serde_json::json!("{\"paused_integration\":{\"state\":\"review\"}}"),
        13=>s.task["blocked_json"]=serde_json::json!("{\"reason\":\"held\"}"),
        14=>s.task["metadata"]=serde_json::json!("{\"deferred_dispatch\":{\"not_before\":\"2099-01-01T00:00:00Z\",\"target_state\":\"merging\"}}"),
        15=>s.task["metadata"]=serde_json::json!("{\"owner_wait\":{\"daemon_id\":\"owner\"}}"),
        16=>s.workspace.as_mut().unwrap()["error"]=serde_json::json!("machine_removed"),
        17=>s.task["metadata"]=serde_json::json!("{\"queued_recovery\":{\"id\":\"accepted-command\"}}"),
        18=>{},
        19=>{s.task["status"]=serde_json::json!("merge_failed");s.task["error_annotation"]=serde_json::json!("{\"type\":\"dirty_worktree\"}");},
        20=>s.task["metadata"]=serde_json::json!("{corrupt"),
        _=>panic!("unknown fixture")
    }
    s
}
#[test]
fn all_twenty_import_priorities_have_explicit_dispositions_and_evidence() {
    let dispositions = [
        "classified",
        "needs_fact",
        "history",
        "needs_fact",
        "needs_fact",
        "needs_fact",
        "needs_fact",
        "classified",
        "needs_fact",
        "classified",
        "needs_fact",
        "obsolete",
        "classified",
        "classified",
        "classified",
        "needs_fact",
        "classified",
        "needs_fact",
        "classified",
        "quarantined",
    ];
    for priority in 1..=20 {
        let s = import_fixture(priority);
        let d = classify_integration_import(&s, FIXED_TIME);
        assert_eq!(d.priority, priority, "fixture {priority}: {d:?}");
        assert_eq!(
            d.disposition.to_string(),
            dispositions[priority as usize - 1],
            "fixture {priority}"
        );
        if d.disposition == IntegrationImportDisposition::NeedsFact {
            assert!(!d.needed_facts.is_empty());
        }
        if priority == 7 {
            assert_eq!(d.guard_paths, Some(vec!["new.rs".into(), "old.rs".into()]));
        }
    }
}
#[test]
fn contradictory_proofs_and_unsupported_paths_quarantine_before_lower_priority() {
    let mut s = import_fixture(1);
    s.task["blocked_json"] = serde_json::json!("{\"reason\":\"held\"}");
    s.checkpoints.push(serde_json::json!({"effects_json":"{\"merge_outcome\":{\"Done\":{\"after_sha\":\"different\",\"branch\":\"main\"}}}"}));
    assert_eq!(
        classify_integration_import(&s, FIXED_TIME).disposition,
        IntegrationImportDisposition::Quarantined
    );
    let mut s = import_fixture(7);
    s.transitions[0]["bridge_payload"] = serde_json::json!("{\"paths\":[null]}");
    assert_eq!(
        classify_integration_import(&s, FIXED_TIME).disposition,
        IntegrationImportDisposition::Quarantined
    );
    let mut s = import_fixture(18);
    s.task["metadata"] = serde_json::json!("{\"paused_integration_generation\":8}");
    assert_eq!(classify_integration_import(&s, FIXED_TIME).priority, 18);
    s.transitions
        .push(serde_json::json!({"bridge_kind":"future_bridge"}));
    assert_eq!(classify_integration_import(&s, FIXED_TIME).priority, 20);
}

#[tokio::test]
async fn importer_is_bounded_resumable_idempotent_and_writes_only_two_tables() {
    let db = fixture().await;
    // Missing executions are retained quarantine rows, never dropped. Two
    // one-row calls model an interrupted process between committed slices.
    let before = legacy_digest(db.pool(), false).await;
    let first = db.import_integration_pass(1).await.unwrap();
    assert_eq!(
        (first.imported, first.quarantined, first.remaining),
        (1, 1, true)
    );
    let saved_first: Vec<String> =
        sqlx::query_scalar("SELECT import_source_json FROM integration_attempt ORDER BY task_ref")
            .fetch_all(db.pool())
            .await
            .unwrap();
    let second = db.import_integration_pass(1).await.unwrap();
    assert_eq!((second.imported, second.remaining), (1, false));
    let third = db.import_integration_pass(100).await.unwrap();
    assert_eq!(third.imported, 0);
    assert_eq!(before, legacy_digest(db.pool(), false).await);
    let saved: Vec<String> =
        sqlx::query_scalar("SELECT import_source_json FROM integration_attempt ORDER BY task_ref")
            .fetch_all(db.pool())
            .await
            .unwrap();
    assert_eq!(saved_first[0], saved[0]);
    assert_eq!(
        db.integration_queue_counts()
            .await
            .unwrap()
            .quarantined_imports,
        2
    );
    assert!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM integration_queue")
            .fetch_one(db.pool())
            .await
            .unwrap()
            == 0
    );
}

pub(super) async fn seed_delivery(db: &SqliteDb, task: &str) {
    let path = std::env::temp_dir()
        .join("integration-worktree")
        .to_str()
        .unwrap()
        .to_owned();
    sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES(?,?,'r',?,'task/branch','ready',?,?)").bind(format!("w-{task}")).bind(task).bind(path).bind(FIXED_TIME).bind(FIXED_TIME).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO execution(id,task_id,role,status,workspace_id,created_at,updated_at) VALUES(?,?,'executor','completed',?,?,?)").bind(format!("e-{task}")).bind(task).bind(format!("w-{task}")).bind(FIXED_TIME).bind(FIXED_TIME).execute(db.pool()).await.unwrap();
}
async fn seeded_step(db: &SqliteDb, task: &str, status: &str) -> crate::TaskStep {
    let state: String = sqlx::query_scalar("SELECT status FROM task WHERE id=?")
        .bind(task)
        .fetch_one(db.pool())
        .await
        .unwrap();
    let input = crate::EnqueueTaskStep {
        id: format!("step-{task}"),
        task_id: task.into(),
        kind: "hooks".into(),
        payload_json: "{}".into(),
        causation_step_id: None,
        causation_key: format!("hook-{task}"),
        chain_id: format!("chain-{task}"),
        chain_position: 1,
        expected_status: state,
        expected_version: 1,
        expected_epoch: Some(0),
        lane: "long".into(),
        available_at: FIXED_TIME.into(),
    };
    db.enqueue_step(&input).await.unwrap();
    if status == "claimed" {
        db.claim_step("owner", Some(task), "2099-01-01T00:00:00Z")
            .await
            .unwrap()
            .unwrap()
    } else {
        sqlx::query("UPDATE task_step SET status=? WHERE id=?")
            .bind(status)
            .bind(&input.id)
            .execute(db.pool())
            .await
            .unwrap();
        db.task_steps(task).await.unwrap().pop().unwrap()
    }
}
#[tokio::test]
async fn importer_persists_each_priority_fixture_and_preserves_all_legacy_bytes() {
    for priority in 1..=20 {
        let db = fixture().await;
        sqlx::query("DELETE FROM task WHERE id='b'")
            .execute(db.pool())
            .await
            .unwrap();
        seed_delivery(&db, "a").await;
        let snapshot = import_fixture(priority);
        for (json_key, column) in [
            ("status", "status"),
            ("metadata", "metadata_json"),
            ("blocked_json", "blocked_json"),
            ("error_annotation", "error_annotation"),
        ] {
            if let Some(value) = snapshot.task.get(json_key) {
                sqlx::query(&format!("UPDATE task SET {column}=? WHERE id='a'"))
                    .bind(value.as_str())
                    .execute(db.pool())
                    .await
                    .unwrap();
            }
        }
        if priority == 10 {
            sqlx::query("UPDATE project SET paused_at=? WHERE id='p'")
                .bind(FIXED_TIME)
                .execute(db.pool())
                .await
                .unwrap();
        }
        if priority == 16 {
            sqlx::query("UPDATE workspace SET error='machine_removed' WHERE task_id='a'")
                .execute(db.pool())
                .await
                .unwrap();
        }
        let step = if priority == 4 {
            Some(seeded_step(&db, "a", "claimed").await)
        } else if !snapshot.checkpoints.is_empty() || priority == 2 {
            Some(seeded_step(&db, "a", "done").await)
        } else {
            None
        };
        if let Some(step) = &step {
            for checkpoint in &snapshot.checkpoints {
                sqlx::query("INSERT INTO task_hook_checkpoint(step_id,hook_index,started_at,effects_json) VALUES(?,0,?,?)").bind(&step.id).bind(FIXED_TIME).bind(checkpoint["effects_json"].as_str()).execute(db.pool()).await.unwrap();
            }
        }
        if priority == 2 {
            // An unresolved retained merge intent is the same row-2 safety
            // evidence as a running task_remote_operation, without guessing
            // a daemon result or installing a new owner.
            sqlx::query("INSERT INTO domain_event(id,event_type,entity_type,entity_id,actor_type,scope_type,scope_id,correlation_id,payload_json,created_at) VALUES('receipt-event','fixture','task','a','system','task','a','original','{}',?)").bind(FIXED_TIME).execute(db.pool()).await.unwrap();
            sqlx::query("INSERT INTO command_receipt(id,principal_type,principal_id,scope_type,scope_id,operation,idempotency_key,input_digest,policy_result,correlation_id,event_id,outcome_json,committed_at) VALUES('receipt','system','fixture','task','a','daemon.workspace.merge.intent','original','digest','allowed','original','receipt-event','{\"metadata\":{\"operation_id\":\"original\"}}',?)").bind(FIXED_TIME).execute(db.pool()).await.unwrap();
        }
        for transition in &snapshot.transitions {
            sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,bridge_kind,bridge_payload,created_at) VALUES('log','a','merging','merge_failed','workflow','fixture',?,?,?)").bind(transition["bridge_kind"].as_str()).bind(transition["bridge_payload"].as_str()).bind(FIXED_TIME).execute(db.pool()).await.unwrap();
        }
        if priority == 3 {
            sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,created_at) VALUES('history','a','merging','cancelled','workflow','fixture',?)").bind(FIXED_TIME).execute(db.pool()).await.unwrap();
        }
        if priority == 9 {
            sqlx::query("INSERT INTO review_authority_carry(id,task_id,contract_execution_id,commit_sha,base_sha,kind,changed_paths_json,created_at) VALUES('carry','a','contract','carried','base','clean_rebase','[]',?)").bind(FIXED_TIME).execute(db.pool()).await.unwrap();
        }
        let before = legacy_digest(db.pool(), false).await;
        let pass = db.import_integration_pass(1).await.unwrap();
        assert_eq!(pass.imported, 1, "row {priority}");
        let source: String = sqlx::query_scalar(
            "SELECT import_source_json FROM integration_attempt WHERE task_ref='a'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        let source: Value = serde_json::from_str(&source).unwrap();
        assert_eq!(
            source["priority"],
            serde_json::json!(priority),
            "row {priority}: {source}"
        );
        assert!(source["evidence"]["task"].is_object());
        assert_eq!(
            before,
            legacy_digest(db.pool(), false).await,
            "row {priority} changed legacy bytes"
        );
        assert_eq!(db.import_integration_pass(1).await.unwrap().imported, 0);
    }
}

#[tokio::test]
async fn importer_crash_mid_slice_rolls_back_progress_and_resumes() {
    let db = fixture().await;
    let before = legacy_digest(db.pool(), false).await;
    sqlx::query("CREATE TRIGGER fail_second_import BEFORE INSERT ON integration_attempt WHEN NEW.task_ref='b' BEGIN SELECT RAISE(ABORT,'interrupted slice'); END").execute(db.pool()).await.unwrap();
    assert!(db.import_integration_pass(2).await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM integration_attempt")
            .fetch_one(db.pool())
            .await
            .unwrap(),
        0
    );
    sqlx::query("DROP TRIGGER fail_second_import")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(db.import_integration_pass(2).await.unwrap().imported, 2);
    assert_eq!(before, legacy_digest(db.pool(), false).await);
}

#[tokio::test]
async fn additive_migration_preserves_every_status_and_legacy_marker_then_replays_as_noop() {
    let dir = tempfile::tempdir().unwrap();
    let source = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"));
    for file in std::fs::read_dir(source).unwrap() {
        let file = file.unwrap();
        if ![
            "V202610080123__integration_queue.sql",
            "V202610082317__integration_fencing.sql",
        ]
        .contains(&file.file_name().to_str().unwrap())
        {
            std::fs::copy(file.path(), dir.path().join(file.file_name())).unwrap();
        }
    }
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    crate::run_migrations_from(&pool, dir.path()).await.unwrap();
    seed(&pool).await;
    let markers=serde_json::json!({"paused_integration":{"state":"merging","deferred_at":FIXED_TIME},"paused_integration_generation":2,"deferred_dispatch":{"not_before":"2099-01-01T00:00:00Z","target_state":"merging"},"dispatch_disposition":{"task_version":1,"capability":"integrate","blocker_digest":"digest"},"queued_recovery":{"id":"accepted","request":"retry"},"awaiting_human":true,"awaiting_human_reason":"decision","awaiting_human_marker_id":"marker","owner_wait":{"daemon_id":"owner","started_at":FIXED_TIME},"environment_wait":{"reason":"setup"},"placement_refusal":{"reason":"setup"},"daemon_upgrade_refusal":{"reason":"upgrade"},"last_execution_failure_at":FIXED_TIME,"last_execution_failure_execution_id":"execution","executor_unavailable_execution_id":"execution","last_workflow_guard_rejection_at":FIXED_TIME,"last_workflow_guard_name":"guard","last_workflow_guard_reason":"reason","last_workflow_guard_execution_id":"execution"}).to_string();
    for (i, status) in [
        "backlog",
        "todo",
        "planning",
        "in_progress",
        "review",
        "merging",
        "merge_failed",
        "done",
        "cancelled",
        "custom",
    ]
    .into_iter()
    .enumerate()
    {
        sqlx::query("INSERT INTO task(id,project_id,title,status,metadata_json,merge_config,error_annotation,blocked_json,failed_json,entry_barrier_json,condition_json,review_passed_at,created_at,updated_at) VALUES(?,'p',?,?,?,'{\"target_branch\":\"main\"}','{\"type\":\"merge_conflict\",\"blocking_reason\":\"conflict\"}','{\"reason\":\"held\"}','{\"reason\":\"failed\"}','{\"status\":\"blocked\",\"state\":\"merging\"}','{\"future_encoding\":99}',?,?,?)").bind(format!("migration-{i}")).bind(status).bind(status).bind(&markers).bind(FIXED_TIME).bind(FIXED_TIME).bind(FIXED_TIME).execute(&pool).await.unwrap();
    }
    let before = legacy_digest(&pool, true).await;
    let migration_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _migration")
        .fetch_one(&pool)
        .await
        .unwrap();
    run_migrations(&pool).await.unwrap();
    assert_eq!(before, legacy_digest(&pool, true).await);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _migration")
            .fetch_one(&pool)
            .await
            .unwrap(),
        migration_count + 2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM integration_queue")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM integration_attempt")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    let migrated = legacy_digest(&pool, false).await;
    run_migrations(&pool).await.unwrap();
    assert_eq!(migrated, legacy_digest(&pool, false).await);
    let triggers:i64=sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND tbl_name IN ('integration_queue','integration_attempt')").fetch_one(&pool).await.unwrap();
    assert_eq!(triggers, 0);
}

#[tokio::test]
async fn shadow_five_result_transactions_preserve_every_other_table_when_recording_fails() {
    // Twin database images freeze generated identities and timestamps. The
    // existing result boundary runs with identical input on each image; only
    // the optional observer's SQL UPDATE fails on the second.
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.db");
    let pool = create_sqlite_pool(&format!("sqlite://{}", base.display()))
        .await
        .unwrap();
    run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    seed(db.pool()).await;
    seed_delivery(&db, "a").await;
    sqlx::query("INSERT INTO task_step(id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,expected_epoch,status,claimed_by,lease_until,available_at,created_at,updated_at) VALUES('step-a','a',1,'hooks','{}','shadow','chain',1,'merging',1,0,'claimed','owner','2099-01-01T00:00:00Z',?,?,?)").bind(FIXED_TIME).bind(FIXED_TIME).bind(FIXED_TIME).execute(db.pool()).await.unwrap();
    sqlx::query(
        "INSERT INTO task_hook_checkpoint(step_id,hook_index,started_at) VALUES('step-a',0,?)",
    )
    .bind(FIXED_TIME)
    .execute(db.pool())
    .await
    .unwrap();
    let step = db.task_steps("a").await.unwrap().pop().unwrap();
    let intent = serde_json::json!({"execution_id":"e-a","workspace_id":"w-a","candidate_sha":"candidate","target_branch":"main"});
    db.record_hook_effect(&step, 0, "merge_intent", &intent.to_string())
        .await
        .unwrap();
    sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,started_at,created_at,updated_at) VALUES('review','a','e-a',1,'running',?,?,?)").bind(FIXED_TIME).bind(FIXED_TIME).bind(FIXED_TIME).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO task_budget(task_id,kind,window_id,spent) VALUES('a','target_moved_rebase','window',1)").execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO task_budget_charge(task_id,kind,window_id,step_id) VALUES('a','target_moved_rebase','window','previous-step')").execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,created_at) VALUES('previous-transition','a','review','merging','workflow','review passed',?)").bind(FIXED_TIME).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO domain_event(id,event_type,entity_type,entity_id,actor_type,scope_type,scope_id,correlation_id,payload_json,created_at) VALUES('previous-event','fixture.baseline','task','a','workflow','task','a','a','{}',?)").bind(FIXED_TIME).execute(db.pool()).await.unwrap();
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(db.pool())
        .await
        .unwrap();
    db.pool().close().await;
    for (case, key, value, kind) in [
        (
            "happy_path",
            Some("merge_outcome"),
            serde_json::json!({"Done":{"before_sha":"target","after_sha":"candidate","branch":"main"}}),
            IntegrationOutcomeKind::Done,
        ),
        (
            "clean_rebase",
            Some("rebase_outcome"),
            serde_json::json!({"kind":"rebased"}),
            IntegrationOutcomeKind::CleanRebase,
        ),
        (
            "conflict_handoff",
            Some("rebase_outcome"),
            serde_json::json!({"kind":"conflict","details":"conflict","conflict_paths":["old.rs","new.rs"]}),
            IntegrationOutcomeKind::ConflictHandoff,
        ),
        (
            "ci_failure",
            None,
            serde_json::json!({"ci_steps":[{"exit_code":1}]}),
            IntegrationOutcomeKind::CiFailed,
        ),
        (
            "cancelled",
            None,
            Value::Null,
            IntegrationOutcomeKind::Cancelled,
        ),
    ] {
        let mut digests = Vec::new();
        for force_failure in [false, true] {
            let path = dir.path().join(format!("{case}-{force_failure}.db"));
            std::fs::copy(&base, &path).unwrap();
            let pool = create_sqlite_pool(&format!("sqlite://{}", path.display()))
                .await
                .unwrap();
            let db = SqliteDb::new(pool);
            if force_failure {
                sqlx::query("CREATE TRIGGER force_shadow_failure BEFORE UPDATE ON integration_attempt BEGIN SELECT RAISE(ABORT,'forced shadow failure'); END").execute(db.pool()).await.unwrap();
            }
            let step = db.task_steps("a").await.unwrap().pop().unwrap();
            if let Some(key) = key {
                crate::task_writer::in_task_step(step.clone(), async {
                    note_integration_target(&step.id, 0, Some("candidate"), "target");
                    db.record_hook_effect(&step, 0, key, &value.to_string())
                        .await
                        .unwrap();
                })
                .await;
            } else {
                let mut tx = begin_immediate(db.pool()).await.unwrap();
                if case == "ci_failure" {
                    sqlx::query("UPDATE review SET status='failed',step_results_json=?,finished_at=?,updated_at=? WHERE id='review'").bind(value.to_string()).bind(FIXED_TIME).bind(FIXED_TIME).execute(&mut *tx).await.unwrap();
                    crate::task_writer::in_task_step(
                        step.clone(),
                        db.observe_integration_review_best_effort(
                            &mut tx,
                            "a",
                            "review",
                            &crate::ReviewStatus::Failed,
                            &value,
                            None,
                        ),
                    )
                    .await;
                } else {
                    sqlx::query("UPDATE task SET status='cancelled',version=version+1,updated_at=? WHERE id='a'").bind(FIXED_TIME).execute(&mut *tx).await.unwrap();
                    db.observe_integration_terminal_best_effort(&mut tx, &step)
                        .await;
                }
                tx.commit().await.unwrap();
            }
            let row = db
                .integration_attempt(
                    &db.current_integration_attempt("a")
                        .await
                        .unwrap()
                        .map(|a| a.id)
                        .unwrap_or_else(|| "".into()),
                )
                .await
                .unwrap();
            let observations: Value = if let Some(row) = row {
                row.observations_json
            } else {
                parse_json(sqlx::query_scalar("SELECT observations_json FROM integration_attempt WHERE task_ref='a' ORDER BY created_at DESC LIMIT 1").fetch_one(db.pool()).await.unwrap()).unwrap()
            };
            assert_eq!(
                observations
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|o| o["kind"] == serde_json::json!(kind)),
                !force_failure,
                "{case}"
            );
            if case == "conflict_handoff" && !force_failure {
                assert_eq!(
                    observations.as_array().unwrap().last().unwrap()["conflict_paths"],
                    serde_json::json!(["old.rs", "new.rs"])
                );
            }
            if case == "happy_path" && !force_failure {
                let o = observations.as_array().unwrap().last().unwrap();
                assert_eq!(o["candidate_sha"], "candidate");
                assert_eq!(o["target_tip_sha"], "target");
            }
            digests.push(legacy_digest(db.pool(), false).await);
            db.pool().close().await;
        }
        assert_eq!(
            digests[0], digests[1],
            "{case}: observer changed legacy results, Tasks, logs, events or budgets"
        );
    }
}

#[tokio::test]
async fn oversized_import_evidence_requires_a_fact_without_trusting_its_prefix() {
    let db = fixture().await;
    sqlx::query("DELETE FROM task WHERE id='b'")
        .execute(db.pool())
        .await
        .unwrap();
    seed_delivery(&db, "a").await;
    let source =
        serde_json::json!({"debug":"x".repeat(70_000),"paused_integration":{"state":"merging"}})
            .to_string();
    sqlx::query("UPDATE task SET metadata_json=? WHERE id='a'")
        .bind(&source)
        .execute(db.pool())
        .await
        .unwrap();
    let before = legacy_digest(db.pool(), false).await;
    let pass = db.import_integration_pass(1).await.unwrap();
    assert_eq!(pass.needs_fact, 1);
    let imported: Value = parse_json(
        sqlx::query_scalar("SELECT import_source_json FROM integration_attempt WHERE task_ref='a'")
            .fetch_one(db.pool())
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(imported["disposition"], "needs_fact");
    assert_eq!(
        imported["needed_facts"],
        serde_json::json!(["complete_legacy_evidence"])
    );
    assert_eq!(
        imported["evidence"]["task"]["__source_bytes"]["metadata_json"],
        serde_json::json!(source.len())
    );
    assert_eq!(before, legacy_digest(db.pool(), false).await);
}

#[test]
fn ready_ff_data_requires_candidate_bound_permit_and_checks() {
    let mut a = IntegrationAttempt::new(
        Some("queue".into()),
        "task".into(),
        "project".into(),
        "entry".into(),
        "merging".into(),
        4,
        9,
    );
    a.state = IntegrationAttemptState::ReadyFf;
    assert!(validate_attempt(&a).is_err());
    a.candidate_sha = Some("candidate".into());
    a.target_tip_sha = Some("target".into());
    a.permit_json = Some(
        serde_json::json!({"candidate_sha":"candidate","target_tip_sha":"target","task_ref":"task","expected_epoch":4,"slot_generation":0}),
    );
    validate_attempt(&a).unwrap();
    a.checks_json = Some(serde_json::json!([]));
    assert!(validate_attempt(&a).is_err());
    a.checks_commit_sha = Some("candidate".into());
    validate_attempt(&a).unwrap();
    a.permit_json.as_mut().unwrap()["expected_epoch"] = serde_json::json!(3);
    assert!(validate_attempt(&a).is_err());
    assert!(!IntegrationAttemptState::Ejected
        .exits()
        .contains(&IntegrationAttemptState::Queued));
    assert!(!IntegrationAttemptState::NeedsReview
        .exits()
        .contains(&IntegrationAttemptState::Queued));
}

#[tokio::test]
async fn takeover_reauthorizes_ready_permits_and_reconciles_original_inflight_identity() {
    let db = fixture().await;
    let q = db
        .create_or_get_integration_queue("r", "main")
        .await
        .unwrap();
    db.admit_integration_attempt(admission(&q, "a", "one"))
        .await
        .unwrap();
    let q = db.integration_queue(&q.id).await.unwrap().unwrap();
    let q = db
        .claim_integration_queue(
            &q.id,
            q.revision,
            "first",
            "2026-10-08T00:00:00Z",
            "2026-10-08T00:01:00Z",
        )
        .await
        .unwrap();
    let mut a = db.integration_head(&q.id).await.unwrap().unwrap();
    for state in [
        IntegrationAttemptState::Validating,
        IntegrationAttemptState::Rebasing,
        IntegrationAttemptState::Checking,
        IntegrationAttemptState::AwaitingTaskStep,
    ] {
        a.state = state;
        a = db.transition_integration_attempt(a).await.unwrap();
    }
    a.state = IntegrationAttemptState::ReadyFf;
    a.candidate_sha = Some("candidate".into());
    a.target_tip_sha = Some("target".into());
    a.permit_json = Some(
        serde_json::json!({"candidate_sha":"candidate","target_tip_sha":"target","task_ref":"a","expected_epoch":0,"slot_generation":1}),
    );
    a = db.transition_integration_attempt(a).await.unwrap();
    let q = db
        .claim_integration_queue(
            &q.id,
            q.revision,
            "second",
            "2026-10-08T00:02:00Z",
            "2026-10-08T00:03:00Z",
        )
        .await
        .unwrap();
    let next = db.integration_head(&q.id).await.unwrap().unwrap();
    assert_eq!(next.id, a.id);
    assert_eq!(next.state, IntegrationAttemptState::AwaitingTaskStep);
    assert!(next.permit_json.is_none());
    assert_eq!(next.effect_seq, 1);
    let mut next = next;
    next.state = IntegrationAttemptState::ReadyFf;
    next.permit_json = Some(
        serde_json::json!({"candidate_sha":"candidate","target_tip_sha":"target","task_ref":"a","expected_epoch":0,"slot_generation":2}),
    );
    next = db.transition_integration_attempt(next).await.unwrap();
    next.state = IntegrationAttemptState::FfInflight;
    next.operation_id = Some("original-op".into());
    next.current_operation_state = Some(IntegrationOperationState::Running);
    let next = db.transition_integration_attempt(next).await.unwrap();
    let q = db
        .claim_integration_queue(
            &q.id,
            q.revision,
            "third",
            "2026-10-08T00:04:00Z",
            "2026-10-08T00:05:00Z",
        )
        .await
        .unwrap();
    let reconciled = db.integration_head(&q.id).await.unwrap().unwrap();
    assert_eq!(reconciled.id, next.id);
    assert_eq!(reconciled.state, IntegrationAttemptState::Reconciling);
    assert_eq!(reconciled.operation_id.as_deref(), Some("original-op"));
    assert_eq!(q.fence_generation, 3);
}

#[test]
fn import_retry_deadlines_compare_instants_instead_of_offset_spelling() {
    let mut s = import_fixture(18);
    s.task["metadata"]=serde_json::json!("{\"deferred_dispatch\":{\"not_before\":\"2026-10-07T01:00:00+02:00\",\"target_state\":\"merging\"}}");
    assert_eq!(classify_integration_import(&s, FIXED_TIME).priority, 18);
}

#[test]
fn malformed_checkpoint_results_and_contradictory_current_entries_are_quarantined() {
    let mut s = import_fixture(4);
    s.checkpoints
        .push(serde_json::json!({"effects_json":"{}","result_json":"{corrupt"}));
    assert_eq!(
        classify_integration_import(&s, FIXED_TIME).disposition,
        IntegrationImportDisposition::Quarantined
    );
    s.checkpoints.clear();
    s.steps[0]["expected_status"] = serde_json::json!("review");
    assert_eq!(
        classify_integration_import(&s, FIXED_TIME).disposition,
        IntegrationImportDisposition::Quarantined
    );
}
