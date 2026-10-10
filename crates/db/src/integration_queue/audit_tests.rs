//! Storage audit: the queue tables never block a deletion, an attempt's
//! observation row is bounded, and the schema matches the enums.
use super::tests::{admission, fixture, seed, seed_delivery, FIXED_TIME};
use super::*;
use crate::{
    create_sqlite_pool, run_migrations, ProjectRepo, RepoLocationRepo, RepoRepo, WorkspaceRepo,
};
use std::sync::Arc;

async fn count(db: &SqliteDb, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(db.pool()).await.unwrap()
}
async fn assert_foreign_keys_hold(db: &SqliteDb) {
    let broken = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(db.pool())
        .await
        .unwrap();
    assert!(broken.is_empty(), "dangling foreign keys remain");
}
/// One queue holding a superseded predecessor, its current successor that is
/// also the reserved queue head, and a second Task's current attempt.
async fn graph(
    db: &SqliteDb,
) -> (
    IntegrationQueue,
    IntegrationAttempt,
    IntegrationAttempt,
    IntegrationAttempt,
) {
    let q = db
        .create_or_get_integration_queue("r", "main")
        .await
        .unwrap();
    let first = db
        .admit_integration_attempt(admission(&q, "a", "one"))
        .await
        .unwrap();
    let mut parked = first.clone();
    parked.state = IntegrationAttemptState::Parked;
    let parked = db.transition_integration_attempt(parked).await.unwrap();
    let head = db
        .supersede_integration_attempt(&first.id, parked.revision, admission(&q, "a", "two"))
        .await
        .unwrap();
    let other = db
        .admit_integration_attempt(admission(&q, "b", "b-one"))
        .await
        .unwrap();
    let q = db.integration_queue(&q.id).await.unwrap().unwrap();
    let q = db
        .claim_integration_queue(
            &q.id,
            q.revision,
            "worker",
            "2026-10-08T00:00:00Z",
            "2099-01-01T00:00:00Z",
        )
        .await
        .unwrap();
    assert_eq!(q.head_attempt_id.as_deref(), Some(head.id.as_str()));
    assert_eq!(head.predecessor_attempt_id.as_deref(), Some(&*first.id));
    (q, first, head, other)
}

#[tokio::test]
async fn deleting_a_repo_removes_its_queues_and_attempts() {
    let db = fixture().await;
    graph(&db).await;
    RepoRepo::delete(&db, "r").await.unwrap();
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM integration_queue").await,
        0
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM integration_attempt").await,
        0
    );
    assert_foreign_keys_hold(&db).await;
}

#[tokio::test]
async fn deleting_a_project_removes_queues_attempts_and_unresolved_import_evidence() {
    let db = fixture().await;
    graph(&db).await;
    let mut orphan = IntegrationAttempt::new(
        None,
        "b".into(),
        "p".into(),
        "import:b:0".into(),
        "merging".into(),
        0,
        1,
    );
    orphan.state = IntegrationAttemptState::Quarantined;
    orphan.current = false;
    orphan.import_source_json = Some(serde_json::json!({"disposition":"quarantined"}));
    let orphan = db.admit_integration_attempt(orphan).await.unwrap();
    assert!(orphan.queue_id.is_none() && !orphan.current);
    ProjectRepo::delete(&db, "p").await.unwrap();
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM integration_queue").await,
        0
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM integration_attempt").await,
        0
    );
    assert_eq!(count(&db, "SELECT COUNT(*) FROM repo").await, 0);
    assert_foreign_keys_hold(&db).await;
}

#[tokio::test]
async fn deleting_the_target_location_suspends_the_queue_and_keeps_its_members() {
    let db = fixture().await;
    let (q, _, head, _) = graph(&db).await;
    sqlx::query(
        "UPDATE integration_attempt SET repo_location_id='l',repo_location_ref='l' WHERE id=?",
    )
    .bind(&head.id)
    .execute(db.pool())
    .await
    .unwrap();
    RepoLocationRepo::delete(&db, "l").await.unwrap();
    let after = db.integration_queue(&q.id).await.unwrap().unwrap();
    assert_eq!(after.target_location_id, None);
    assert_eq!(after.target_owner_json, None);
    assert_eq!(after.state, IntegrationQueueState::Suspended);
    assert_eq!(
        after.last_error_kind,
        Some(IntegrationFailureKind::TargetUnconfigured)
    );
    assert_eq!(after.head_attempt_id, q.head_attempt_id);
    assert_eq!(after.revision, q.revision + 1);
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM integration_attempt").await,
        3
    );
    let head = db.integration_attempt(&head.id).await.unwrap().unwrap();
    assert_eq!(head.repo_location_id, None);
    assert_eq!(head.repo_location_ref.as_deref(), Some("l"));
    // A queue without a target is never claimable, whatever its state says.
    sqlx::query("UPDATE integration_queue SET state='open',lease_owner=NULL,lease_until=NULL")
        .execute(db.pool())
        .await
        .unwrap();
    assert!(matches!(
        db.claim_integration_queue(
            &q.id,
            after.revision,
            "next",
            "2026-10-08T00:00:00Z",
            "2026-10-08T00:01:00Z"
        )
        .await,
        Err(DbError::VersionConflict)
    ));
    assert_foreign_keys_hold(&db).await;
}

#[tokio::test]
async fn deleting_a_task_detaches_its_attempts_and_keeps_the_evidence() {
    let db = fixture().await;
    let (q, first, head, other) = graph(&db).await;
    sqlx::query("DELETE FROM task WHERE id='a'")
        .execute(db.pool())
        .await
        .unwrap();
    for id in [&first.id, &head.id] {
        let attempt = db.integration_attempt(id).await.unwrap().unwrap();
        assert_eq!(attempt.task_id, None);
        assert_eq!(attempt.task_ref, "a");
    }
    assert_eq!(
        db.integration_head(&q.id).await.unwrap().unwrap().id,
        head.id
    );
    assert_eq!(
        db.current_integration_attempt("b")
            .await
            .unwrap()
            .unwrap()
            .id,
        other.id
    );
    assert_foreign_keys_hold(&db).await;
}

#[tokio::test]
async fn workspace_and_execution_cleanup_detach_but_keep_opaque_references() {
    let db = fixture().await;
    let (_, _, head, _) = graph(&db).await;
    seed_delivery(&db, "a").await;
    sqlx::query("UPDATE integration_attempt SET workspace_id='w-a',workspace_ref='w-a',execution_id='e-a',execution_ref='e-a' WHERE id=?")
        .bind(&head.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM execution WHERE id='e-a'")
        .execute(db.pool())
        .await
        .unwrap();
    WorkspaceRepo::delete(&db, "w-a").await.unwrap();
    let head = db.integration_attempt(&head.id).await.unwrap().unwrap();
    assert_eq!((head.workspace_id, head.execution_id), (None, None));
    assert_eq!(head.workspace_ref.as_deref(), Some("w-a"));
    assert_eq!(head.execution_ref.as_deref(), Some("e-a"));
    assert!(head.current);
    assert_foreign_keys_hold(&db).await;
}

async fn seed_daemon_repo(db: &SqliteDb) {
    sqlx::query("INSERT INTO daemon (id,machine_id,hostname,os,arch,status,created_at,updated_at) VALUES ('d','remote-machine','host','linux','aarch64','offline',?,?)").bind(FIXED_TIME).bind(FIXED_TIME).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO runtime (id,daemon_id,kind,workspace_root,status,labels_json,created_at,updated_at) VALUES ('rt','d','codex','root','offline','{}',?,?)").bind(FIXED_TIME).bind(FIXED_TIME).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO repo(id,project_id,name,default_branch,created_at,updated_at) VALUES('remote','p','remote','main',?,?)").bind(FIXED_TIME).bind(FIXED_TIME).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,daemon_id,runtime_id,path,kind,is_default,status,created_at,updated_at) VALUES('dl','remote','daemon','d','rt','checkout','primary_checkout',1,'ready',?,?)").bind(FIXED_TIME).bind(FIXED_TIME).execute(db.pool()).await.unwrap();
}

#[tokio::test]
async fn removing_a_machine_leaves_its_queue_evidence_and_later_deletion_working() {
    let db = fixture().await;
    seed_daemon_repo(&db).await;
    let q = db
        .create_or_get_integration_queue("remote", "main")
        .await
        .unwrap();
    assert_eq!(q.state, IntegrationQueueState::Open);
    assert_eq!(q.target_location_id.as_deref(), Some("dl"));
    assert_eq!(q.target_owner_json.as_ref().unwrap()["daemon_id"], "d");
    let attempt = db
        .admit_integration_attempt(admission(&q, "a", "one"))
        .await
        .unwrap();
    let q = db.integration_queue(&q.id).await.unwrap().unwrap();
    db.claim_integration_queue(
        &q.id,
        q.revision,
        "worker",
        "2026-10-08T00:00:00Z",
        "2099-01-01T00:00:00Z",
    )
    .await
    .unwrap();
    db.remove_daemon("d", "admin", true, "local-machine", false)
        .await
        .unwrap();
    assert_eq!(
        db.integration_head(&q.id).await.unwrap().unwrap().id,
        attempt.id
    );
    assert_foreign_keys_hold(&db).await;
    RepoRepo::delete(&db, "remote").await.unwrap();
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM integration_attempt").await,
        0
    );
    assert_foreign_keys_hold(&db).await;
}

#[test]
fn every_check_enum_in_the_migration_matches_its_rust_enum() {
    // The activation migration adds no enum column (its typed values live in
    // the timings document), so the same scan must find nothing new in it.
    let sql = concat!(
        include_str!("../../migrations/V202610080123__integration_queue.sql"),
        include_str!("../../migrations/V202610091612__integration_activation.sql")
    );
    fn all<T: ToString>(values: &[T]) -> Vec<String> {
        values.iter().map(ToString::to_string).collect()
    }
    let expected: std::collections::BTreeMap<&str, Vec<String>> = [
        ("state", all(IntegrationAttemptState::ALL)),
        ("resume_state", all(IntegrationAttemptState::ALL)),
        ("failure_kind", all(IntegrationFailureKind::ALL)),
        ("last_error_kind", all(IntegrationFailureKind::ALL)),
        ("owner_kind", all(IntegrationOwnerKind::ALL)),
        ("operation_kind", all(IntegrationOperationKind::ALL)),
        (
            "current_operation_state",
            all(IntegrationOperationState::ALL),
        ),
    ]
    .into_iter()
    .collect();
    let mut seen = 0;
    let mut table = "";
    for line in sql.lines() {
        if let Some(name) = line.strip_prefix("CREATE TABLE ") {
            table = name.trim_end_matches(" (");
        }
        let Some((column, rest)) = line.trim().split_once(" TEXT") else {
            continue;
        };
        let Some(list) = rest
            .split_once(&format!("CHECK({column} IN ("))
            .and_then(|(_, list)| list.split_once("))"))
            .map(|(list, _)| list)
        else {
            continue;
        };
        let stored: Vec<String> = list
            .split(',')
            .map(|value| value.trim().trim_matches('\'').to_owned())
            .collect();
        let rust = if (table, column) == ("integration_queue", "state") {
            all(IntegrationQueueState::ALL)
        } else {
            expected
                .get(column)
                .unwrap_or_else(|| panic!("{table}.{column} has a CHECK enum with no Rust enum"))
                .clone()
        };
        assert_eq!(stored, rust, "{table}.{column}");
        seen += 1;
    }
    // queue: state, last_error_kind; attempt: owner_kind, state, resume_state,
    // failure_kind, operation_kind, current_operation_state, last_error_kind.
    assert_eq!(seen, 9);
    assert!(!sql.contains("RESTRICT") && !sql.contains("TRIGGER"));
}

/// Spawned tasks run in parallel on the multi-thread runtime; this only
/// collects their results.
async fn join_all<T>(
    handles: impl Iterator<Item = tokio::task::JoinHandle<T>>,
) -> Vec<std::result::Result<T, tokio::task::JoinError>> {
    let handles: Vec<_> = handles.collect();
    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await);
    }
    results
}
async fn file_db(dir: &tempfile::TempDir) -> Arc<SqliteDb> {
    let pool = create_sqlite_pool(&format!("sqlite://{}", dir.path().join("q.db").display()))
        .await
        .unwrap();
    run_migrations(&pool).await.unwrap();
    seed(&pool).await;
    Arc::new(SqliteDb::new(pool))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_connections_admit_once_and_claim_one_head() {
    let dir = tempfile::tempdir().unwrap();
    let db = file_db(&dir).await;
    let queues = join_all((0..8).map(|_| {
        let db = db.clone();
        tokio::spawn(async move { db.create_or_get_integration_queue("r", "main").await })
    }))
    .await;
    let q = queues[0].as_ref().unwrap().as_ref().unwrap().clone();
    assert!(queues
        .iter()
        .all(|other| other.as_ref().unwrap().as_ref().unwrap().id == q.id));
    let input = admission(&q, "a", "one");
    let admitted = join_all((0..8).map(|_| {
        let (db, input) = (db.clone(), input.clone());
        tokio::spawn(async move { db.admit_integration_attempt(input).await })
    }))
    .await;
    assert!(admitted
        .iter()
        .all(|a| a.as_ref().unwrap().as_ref().unwrap().id == input.id));
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM integration_attempt").await,
        1
    );
    db.admit_integration_attempt(admission(&q, "b", "two"))
        .await
        .unwrap();
    let q = db.integration_queue(&q.id).await.unwrap().unwrap();
    let claims = join_all((0..8).map(|i| {
        let (db, id, revision) = (db.clone(), q.id.clone(), q.revision);
        tokio::spawn(async move {
            db.claim_integration_queue(
                &id,
                revision,
                &format!("worker-{i}"),
                "2026-10-08T00:00:00Z",
                "2026-10-08T00:01:00Z",
            )
            .await
        })
    }))
    .await;
    let won: Vec<_> = claims
        .iter()
        .filter_map(|claim| claim.as_ref().unwrap().as_ref().ok())
        .collect();
    assert_eq!(won.len(), 1);
    assert!(claims
        .iter()
        .filter_map(|claim| claim.as_ref().unwrap().as_ref().err())
        .all(|error| matches!(error, DbError::VersionConflict)));
    let after = db.integration_queue(&q.id).await.unwrap().unwrap();
    assert_eq!(after.fence_generation, 1);
    assert_eq!(after.lease_owner, won[0].lease_owner);
    assert_eq!(after.head_attempt_id.as_deref(), Some(input.id.as_str()));
}

async fn observations(db: &SqliteDb, task: &str) -> (Vec<Value>, i64) {
    let (json, dropped): (String, i64) = sqlx::query_as("SELECT observations_json,observations_dropped FROM integration_attempt WHERE task_ref=? ORDER BY created_at DESC LIMIT 1").bind(task).fetch_one(db.pool()).await.unwrap();
    (serde_json::from_str::<Vec<Value>>(&json).unwrap(), dropped)
}

#[tokio::test]
async fn target_resolver_follows_the_repo_setting_and_suspends_instead_of_guessing() {
    let db = fixture().await;
    seed_daemon_repo(&db).await;
    let resolve = |repo: &'static str, branch: &'static str| {
        let db = &db;
        async move {
            let q = db
                .create_or_get_integration_queue(repo, branch)
                .await
                .unwrap();
            (q.state, q.target_location_id, q.last_error_kind)
        }
    };
    use IntegrationFailureKind::*;
    use IntegrationQueueState::*;
    // Server-only: repo.local_path and its matching default server checkout.
    assert_eq!(
        resolve("r", "server-only").await,
        (Open, Some("l".into()), None)
    );
    // Daemon-only: no local_path, one default daemon checkout.
    assert_eq!(
        resolve("remote", "daemon-only").await,
        (Open, Some("dl".into()), None)
    );
    // Both, server is the default: a non-default daemon copy never selects.
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,daemon_id,runtime_id,path,kind,is_default,status,created_at,updated_at) VALUES('copy','r','daemon','d','rt','copy','managed_clone',0,'ready',?,?)").bind(FIXED_TIME).bind(FIXED_TIME).execute(db.pool()).await.unwrap();
    assert_eq!(
        resolve("r", "both-server-default").await,
        (Open, Some("l".into()), None)
    );
    // Both, but the default is a daemon checkout while local_path is set:
    // the repo location is authoritative regardless of the local_path hint.
    sqlx::query("UPDATE repo_location SET is_default=0 WHERE id='l'")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE repo_location SET is_default=1,kind='primary_checkout' WHERE id='copy'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        resolve("r", "both-daemon-default").await,
        (Open, Some("copy".into()), None)
    );
    // A default server checkout at a different path than local_path.
    sqlx::query("UPDATE repo_location SET is_default=0 WHERE id='copy'")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE repo_location SET is_default=1,path='elsewhere' WHERE id='l'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        resolve("r", "path-mismatch").await,
        (Open, Some("l".into()), None)
    );
    // The configured location exists but is not ready: named, not usable.
    sqlx::query("UPDATE repo_location SET status='unavailable' WHERE id='dl'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        resolve("remote", "not-ready").await,
        (Suspended, Some("dl".into()), Some(TargetUnavailable))
    );
    // No default / two defaults.
    sqlx::query("UPDATE repo_location SET is_default=0 WHERE repo_id='remote'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        resolve("remote", "no-default").await,
        (Suspended, None, Some(TargetUnconfigured))
    );
    sqlx::query("UPDATE repo_location SET is_default=1,status='ready' WHERE id='dl'")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,daemon_id,runtime_id,path,kind,is_default,status,created_at,updated_at) VALUES('dl2','remote','daemon','d','rt','second','primary_checkout',1,'ready',?,?)").bind(FIXED_TIME).bind(FIXED_TIME).execute(db.pool()).await.unwrap();
    assert_eq!(
        resolve("remote", "two-defaults").await,
        (Suspended, None, Some(TargetAmbiguous))
    );
    // Merely reading a queue preserves its snapshot; claim refreshes it.
    assert_eq!(
        resolve("r", "server-only").await,
        (Open, Some("l".into()), None)
    );
}

#[tokio::test]
async fn an_attempt_observed_a_thousand_times_keeps_a_bounded_observation_row() {
    let db = fixture().await;
    let q = db
        .create_or_get_integration_queue("r", "main")
        .await
        .unwrap();
    let attempt = db
        .admit_integration_attempt(admission(&q, "a", "bounded"))
        .await
        .unwrap();
    let paths: Vec<String> = (0..400).map(|i| format!("src/file-{i}.rs")).collect();
    let observation = |index: usize| {
        let mut observation = IntegrationObservation::new(
            format!("observed:{index}"),
            if index == 0 {
                IntegrationOutcomeKind::Admission
            } else {
                IntegrationOutcomeKind::ConflictHandoff
            },
        );
        if index > 0 {
            observation.conflict_paths = Some(paths.clone());
        }
        observation
    };
    for index in 0..=1000 {
        db.record_integration_observation(&attempt.id, observation(index))
            .await
            .unwrap();
    }
    let (kept, dropped) = observations(&db, "a").await;
    let retained = INTEGRATION_OBSERVATION_RECENT as usize + 1;
    assert_eq!(kept.len(), retained);
    assert_eq!(dropped, 1001 - retained as i64);
    // The first observation is kept; then the most recent ones.
    assert_eq!(kept[0]["kind"], "admission");
    assert_eq!(kept[0]["identity"], "observed:0");
    for (offset, observation) in kept[1..].iter().enumerate() {
        let index = 1000 - INTEGRATION_OBSERVATION_RECENT as usize + 1 + offset;
        assert_eq!(observation["identity"], format!("observed:{index}"));
        assert_eq!(
            observation["conflict_paths"].as_array().unwrap().len(),
            INTEGRATION_OBSERVATION_PATHS
        );
        assert_eq!(observation["paths_truncated"], true);
    }
    let bytes = count(
        &db,
        "SELECT length(CAST(observations_json AS BLOB)) FROM integration_attempt",
    )
    .await;
    assert!(bytes as usize <= retained * INTEGRATION_OBSERVATION_BYTES);
    // A replayed retained identity is not appended again.
    let mut replay = observation(1000);
    replay.recorded_at = kept[retained - 1]["recorded_at"]
        .as_str()
        .unwrap()
        .to_owned();
    db.record_integration_observation(&attempt.id, replay)
        .await
        .unwrap();
    assert_eq!(observations(&db, "a").await.1, dropped);
    // Observations never drive state.
    let attempt = db.current_integration_attempt("a").await.unwrap().unwrap();
    assert_eq!(attempt.state, IntegrationAttemptState::Queued);
}

#[tokio::test]
async fn import_rows_are_disposable_and_an_existing_attempt_is_not_replaced() {
    let db = fixture().await;
    assert_eq!(db.import_integration_pass(100).await.unwrap().imported, 2);
    assert_eq!(db.import_integration_pass(100).await.unwrap().imported, 0);
    // Discarding every row this stage wrote is two statements, and the next
    // pass classifies from the legacy columns again.
    sqlx::raw_sql("DELETE FROM integration_attempt; DELETE FROM integration_queue;")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(db.import_integration_pass(100).await.unwrap().imported, 2);
    // Without the discard, an existing current attempt (the one admission
    // wrote for the Task) is NOT replaced: the import row is retained
    // non-current and asks for the fact.
    sqlx::raw_sql("DELETE FROM integration_attempt; DELETE FROM integration_queue;")
        .execute(db.pool())
        .await
        .unwrap();
    seed_delivery(&db, "a").await;
    let q = db
        .create_or_get_integration_queue("r", "main")
        .await
        .unwrap();
    let admitted = db
        .admit_integration_attempt(admission(&q, "a", "admit:a:0"))
        .await
        .unwrap();
    db.import_integration_pass(100).await.unwrap();
    assert_eq!(
        db.current_integration_attempt("a")
            .await
            .unwrap()
            .unwrap()
            .id,
        admitted.id
    );
    let (current, facts): (bool, String) = sqlx::query_as("SELECT current,json_extract(import_source_json,'$.needed_facts') FROM integration_attempt WHERE task_ref='a' AND import_source_json IS NOT NULL").fetch_one(db.pool()).await.unwrap();
    assert!(!current);
    assert!(facts.contains("existing_attempt_identity"));
}

#[tokio::test]
async fn claim_refreshes_default_location_and_recovers_a_suspended_queue_under_cas() {
    let db = fixture().await;
    let q = db
        .create_or_get_integration_queue("r", "main")
        .await
        .unwrap();
    db.admit_integration_attempt(admission(&q, "a", "refresh"))
        .await
        .unwrap();
    let q = db.integration_queue(&q.id).await.unwrap().unwrap();
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) SELECT 'new',repo_id,owner_kind,'new-target',kind,1,status,created_at,updated_at FROM repo_location WHERE id='l'").execute(db.pool()).await.unwrap();
    sqlx::query("UPDATE repo_location SET is_default=0 WHERE id='l'")
        .execute(db.pool())
        .await
        .unwrap();
    assert!(matches!(
        db.claim_integration_queue(
            &q.id,
            q.revision - 1,
            "stale",
            "2026-10-08T00:00:00Z",
            "2026-10-08T00:01:00Z"
        )
        .await,
        Err(DbError::VersionConflict)
    ));
    assert_eq!(db.integration_queue(&q.id).await.unwrap().unwrap(), q);
    let claimed = db
        .claim_integration_queue(
            &q.id,
            q.revision,
            "winner",
            "2026-10-08T00:00:00Z",
            "2026-10-08T00:01:00Z",
        )
        .await
        .unwrap();
    assert_eq!(claimed.target_location_id.as_deref(), Some("new"));
    assert_eq!(
        claimed.target_owner_json.as_ref().unwrap()["location_id"],
        "new"
    );
    assert_eq!(claimed.revision, q.revision + 1);
    // Unavailable resolution is durable even though the claim is refused.
    sqlx::query("UPDATE repo_location SET status='unavailable' WHERE id='new'")
        .execute(db.pool())
        .await
        .unwrap();
    assert!(matches!(
        db.claim_integration_queue(
            &q.id,
            claimed.revision,
            "next",
            "2026-10-08T00:02:00Z",
            "2026-10-08T00:03:00Z"
        )
        .await,
        Err(DbError::VersionConflict)
    ));
    let suspended = db.integration_queue(&q.id).await.unwrap().unwrap();
    assert_eq!(suspended.state, IntegrationQueueState::Suspended);
    assert_eq!(
        suspended.last_error_kind,
        Some(IntegrationFailureKind::TargetUnavailable)
    );
    assert_eq!(suspended.fence_generation, claimed.fence_generation);
    sqlx::query("UPDATE repo_location SET status='ready' WHERE id='new'")
        .execute(db.pool())
        .await
        .unwrap();
    let recovered = db
        .claim_integration_queue(
            &q.id,
            suspended.revision,
            "next",
            "2026-10-08T00:02:00Z",
            "2026-10-08T00:03:00Z",
        )
        .await
        .unwrap();
    assert_eq!(recovered.state, IntegrationQueueState::Open);
    assert_eq!(recovered.last_error_kind, None);
    assert_eq!(recovered.head_attempt_id, claimed.head_attempt_id);
    assert_eq!(recovered.fence_generation, claimed.fence_generation + 1);
}

async fn assert_resolver_row(row: &str) {
    let db = fixture().await;
    let mut repo = "r";
    let mut expected_location = Some("l");
    let mut expected_failure = None;
    match row {
        "server_only" => {}
        "daemon_only" => {
            seed_daemon_repo(&db).await;
            repo = "remote";
            expected_location = Some("dl");
        }
        "both_server_default" | "both_daemon_default" => {
            seed_daemon_repo(&db).await;
            sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,daemon_id,runtime_id,path,kind,is_default,status,created_at,updated_at) VALUES('copy','r','daemon','d','rt','copy','primary_checkout',0,'ready',?,?)").bind(FIXED_TIME).bind(FIXED_TIME).execute(db.pool()).await.unwrap();
            if row == "both_daemon_default" {
                sqlx::query("UPDATE repo_location SET is_default=CASE WHEN id='copy' THEN 1 ELSE 0 END WHERE repo_id='r'").execute(db.pool()).await.unwrap();
                expected_location = Some("copy");
            }
        }
        "path_mismatch" => {
            sqlx::query("UPDATE repo_location SET path='different-checkout' WHERE id='l'")
                .execute(db.pool())
                .await
                .unwrap();
        }
        "no_default" => {
            sqlx::query("UPDATE repo_location SET is_default=0 WHERE repo_id='r'")
                .execute(db.pool())
                .await
                .unwrap();
            sqlx::query("UPDATE repo SET local_path=NULL WHERE id='r'")
                .execute(db.pool())
                .await
                .unwrap();
            expected_location = None;
            expected_failure = Some(IntegrationFailureKind::TargetUnconfigured);
        }
        "two_defaults" => {
            sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) SELECT 'second',repo_id,owner_kind,'second',kind,1,status,created_at,updated_at FROM repo_location WHERE id='l'").execute(db.pool()).await.unwrap();
            expected_location = None;
            expected_failure = Some(IntegrationFailureKind::TargetAmbiguous);
        }
        "not_ready" => {
            sqlx::query("UPDATE repo_location SET status='unavailable' WHERE id='l'")
                .execute(db.pool())
                .await
                .unwrap();
            expected_failure = Some(IntegrationFailureKind::TargetUnavailable);
        }
        _ => panic!("unknown resolver row"),
    }
    let q = db
        .create_or_get_integration_queue(repo, "main")
        .await
        .unwrap();
    assert_eq!(q.target_location_id.as_deref(), expected_location);
    assert_eq!(q.last_error_kind, expected_failure);
    assert_eq!(
        q.state,
        if expected_failure.is_some() {
            IntegrationQueueState::Suspended
        } else {
            IntegrationQueueState::Open
        }
    );
}
macro_rules! resolver_row {
    ($name:ident,$row:literal) => {
        #[tokio::test]
        async fn $name() {
            assert_resolver_row($row).await;
        }
    };
}
resolver_row!(resolver_server_only, "server_only");
resolver_row!(resolver_daemon_only, "daemon_only");
resolver_row!(resolver_both_server_default, "both_server_default");
resolver_row!(
    resolver_both_daemon_default_with_local_path,
    "both_daemon_default"
);
resolver_row!(
    resolver_server_default_at_different_local_path,
    "path_mismatch"
);
resolver_row!(resolver_no_default, "no_default");
resolver_row!(resolver_two_defaults, "two_defaults");
resolver_row!(resolver_not_ready, "not_ready");
