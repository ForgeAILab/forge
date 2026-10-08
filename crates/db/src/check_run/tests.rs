use super::*;
use crate::{create_sqlite_pool, run_migrations, ProjectRepo, RepoRepo, WorkspaceRepo};
use api_types::*;
use std::sync::Arc;

const NOW: &str = "2026-10-08T00:00:00Z";
const UNTIL: &str = "2026-10-08T00:01:00Z";
async fn seed(db: &SqliteDb) {
    sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES('p','project',?,?)")
        .bind(NOW)
        .bind(NOW)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO repo(id,project_id,name,default_branch,created_at,updated_at) VALUES('r','p','repo','main',?,?)").bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t','p','task','todo',?,?)").bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES('w','t','r',?,'task/branch','ready',?,?)")
        .bind(std::env::temp_dir().join("check-contract-workspace").to_str().unwrap()).bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO daemon(id,machine_id,hostname,os,arch,status,created_at,updated_at) VALUES('m','check-machine','test','linux','aarch64','offline',?,?)").bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
}
async fn fixture() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    seed(&db).await;
    db
}
fn identity() -> CheckRunIdentity {
    CheckRunIdentity {
        project_id: "p".into(),
        repo_id: "r".into(),
        commit_sha: "a".repeat(40),
        inputs: CheckDigestInput {
            spec: CheckSpec {
                schema_revision: 1,
                purpose: CheckPurpose::EntryCi,
                commands: vec![CheckCommandSpec {
                    id: "ci:0".into(),
                    shell_text: "cargo test".into(),
                    shell: "bash -lc".into(),
                    working_directory: CheckWorkingDirectory::TaskRoot,
                    environment_keys: Default::default(),
                    timeout_seconds: None,
                    failure_policy: CheckFailurePolicy::StopBundle,
                    cacheability: CheckCacheability::DeclaredControlledInputs,
                    requirement_ids: Default::default(),
                }],
                whole_run_timeout_seconds: 1800,
                execution_policy: "controlled/1".into(),
            },
            environment: Default::default(),
            environment_identity: CheckEnvironmentIdentity::Attested {
                input_digest: "a".repeat(64),
            },
            execution_revision: CheckExecutionRevision {
                number: 0,
                audit_ref: None,
            },
        },
    }
}
fn request(key: &str) -> CheckRunRequest {
    CheckRunRequest {
        identity: identity(),
        request_key: key.into(),
        task_id: Some("t".into()),
        status_epoch: 0,
        origin: CheckConsumerOrigin::Entry,
        workspace_id: Some("w".into()),
        machine_id: Some("m".into()),
    }
}
fn fence(run: &StoredCheckRun) -> CheckRunFence {
    CheckRunFence {
        run_id: run.id.clone(),
        version: run.version,
        lease_generation: run.lease_generation,
        lease_owner: run.lease_owner.clone().unwrap(),
    }
}
fn evidence(outcome: CheckResultOutcome, cleanup: CheckCleanup) -> CheckResultEvidence {
    CheckResultEvidence {
        outcome,
        cleanup,
        commands: vec![CheckCommandOutcome {
            index: 0,
            command: "cargo test".into(),
            exit_code: if outcome == CheckResultOutcome::Pass {
                0
            } else {
                1
            },
            stderr_tail: String::new(),
            output_tail: String::new(),
            started_at: NOW.into(),
            finished_at: NOW.into(),
        }],
        output_truncated: false,
        redaction_values: vec![],
    }
}
async fn cleaning(db: &SqliteDb, req: CheckRunRequest) -> StoredCheckRun {
    let response = db.request_check_run(req).await.unwrap();
    let run = db
        .claim_check_run(
            response.consumer.run_id.as_deref().unwrap(),
            1,
            "worker",
            NOW,
            UNTIL,
        )
        .await
        .unwrap();
    db.transition_check_run(&fence(&run), CheckRunState::Cleaning, NOW)
        .await
        .unwrap()
}
async fn complete(
    db: &SqliteDb,
    req: CheckRunRequest,
    outcome: CheckResultOutcome,
    cleanup: CheckCleanup,
) -> StoredCheckResult {
    let run = cleaning(db, req).await;
    db.finish_check_run(&fence(&run), evidence(outcome, cleanup), NOW)
        .await
        .unwrap()
}
#[test]
fn state_machine_is_total_and_terminals_have_no_exit() {
    for state in CheckRunState::ALL {
        let exits: Vec<_> = CHECK_RUN_TRANSITIONS
            .iter()
            .filter(|(from, _)| from == state)
            .collect();
        assert_eq!(exits.is_empty(), state.terminal(), "{state}");
        assert!(exits
            .iter()
            .all(|(from, to)| from != to && CheckRunState::ALL.contains(to)));
    }
    assert!(!CHECK_RUN_TRANSITIONS.contains(&(CheckRunState::Uncertain, CheckRunState::Running)));
}
#[tokio::test]
async fn sql_check_enums_equal_rust_enums() {
    let db = fixture().await;
    async fn values(db: &SqliteDb, table: &str, column: &str) -> Vec<String> {
        let sql: String =
            sqlx::query_scalar("SELECT sql FROM sqlite_master WHERE type='table' AND name=?")
                .bind(table)
                .fetch_one(db.pool())
                .await
                .unwrap();
        let needle = format!("{column} IN (");
        sql.split(&needle)
            .nth(1)
            .unwrap()
            .split(')')
            .next()
            .unwrap()
            .split(',')
            .map(|s| s.trim().trim_matches('\'').into())
            .collect()
    }
    macro_rules! compare {
        ($table:literal,$column:literal,$enum:ident) => {
            assert_eq!(
                values(&db, $table, $column).await,
                $enum::ALL
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            );
            for state in $enum::ALL {
                assert_eq!(state.to_string().parse::<$enum>().unwrap(), *state);
            }
            assert!("future".parse::<$enum>().is_err());
        };
    }
    compare!("check_run", "state", CheckRunState);
    compare!("check_result", "outcome", CheckResultOutcome);
    compare!("check_result", "cleanup", CheckCleanup);
    compare!("check_consumer", "origin", CheckConsumerOrigin);
}
#[tokio::test]
async fn request_is_idempotent_and_conflicting_request_keys_are_refused() {
    let db = fixture().await;
    let first = db.request_check_run(request("one")).await.unwrap();
    assert!(matches!(
        first.disposition,
        CheckRequestDisposition::Scheduled
    ));
    let replay = db.request_check_run(request("one")).await.unwrap();
    assert_eq!(replay.consumer.id, first.consumer.id);
    assert!(matches!(
        replay.disposition,
        CheckRequestDisposition::Idempotent
    ));
    let mut changed = request("one");
    changed.identity.commit_sha = "b".repeat(40);
    assert!(matches!(
        db.request_check_run(changed).await,
        Err(DbError::IdempotencyConflict)
    ));
    let joined = db.request_check_run(request("two")).await.unwrap();
    assert_eq!(joined.consumer.run_id, first.consumer.run_id);
    assert!(matches!(
        joined.disposition,
        CheckRequestDisposition::Joined
    ));
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_backed_concurrent_requests_and_claims_have_one_live_run() {
    let dir = tempfile::tempdir().unwrap();
    let pool = create_sqlite_pool(&format!(
        "sqlite://{}",
        dir.path().join("checks.db").display()
    ))
    .await
    .unwrap();
    run_migrations(&pool).await.unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    seed(&db).await;
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut tasks = Vec::new();
    for n in 0..8 {
        let db = db.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            db.request_check_run(request(&format!("concurrent-{n}")))
                .await
                .unwrap()
        }));
    }
    let mut run_ids = std::collections::BTreeSet::new();
    let mut scheduled = 0;
    for task in tasks {
        let response = task.await.unwrap();
        run_ids.insert(response.consumer.run_id.unwrap());
        scheduled += usize::from(matches!(
            response.disposition,
            CheckRequestDisposition::Scheduled
        ));
    }
    assert_eq!(run_ids.len(), 1);
    assert_eq!(scheduled, 1);
    let id = run_ids.first().unwrap().clone();
    let mut claims = Vec::new();
    let barrier = Arc::new(tokio::sync::Barrier::new(4));
    for n in 0..4 {
        let db = db.clone();
        let id = id.clone();
        let barrier = barrier.clone();
        claims.push(tokio::spawn(async move {
            barrier.wait().await;
            db.claim_check_run(&id, 1, &format!("worker-{n}"), NOW, UNTIL)
                .await
        }));
    }
    let mut wins = 0;
    for claim in claims {
        match claim.await.unwrap() {
            Ok(_) => wins += 1,
            Err(DbError::VersionConflict) => {}
            other => panic!("unexpected claim {other:?}"),
        }
    }
    assert_eq!(wins, 1);
    db.pool().close().await;
}
#[tokio::test]
async fn lease_takeover_renews_generation_and_fences_stale_finish() {
    let db = fixture().await;
    let response = db.request_check_run(request("one")).await.unwrap();
    let id = response.consumer.run_id.unwrap();
    let first = db
        .claim_check_run(&id, 1, "first", NOW, UNTIL)
        .await
        .unwrap();
    let takeover = db
        .claim_check_run(
            &id,
            first.version,
            "second",
            "2026-10-08T00:01:01Z",
            "2026-10-08T00:02:00Z",
        )
        .await
        .unwrap();
    assert_eq!(takeover.lease_generation, first.lease_generation + 1);
    assert_eq!(takeover.operation_id, first.operation_id);
    assert!(matches!(
        db.renew_check_run(&fence(&first), NOW, UNTIL).await,
        Err(DbError::VersionConflict)
    ));
    assert!(matches!(
        db.finish_check_run(
            &fence(&first),
            evidence(CheckResultOutcome::Pass, CheckCleanup::Success),
            NOW
        )
        .await,
        Err(DbError::VersionConflict)
    ));
    let renewed = db
        .renew_check_run(
            &fence(&takeover),
            "2026-10-08T00:01:02Z",
            "2026-10-08T00:03:00Z",
        )
        .await
        .unwrap();
    assert_eq!(renewed.lease_generation, takeover.lease_generation);
    assert_eq!(renewed.version, takeover.version + 1);
}
#[tokio::test]
async fn uncertain_run_blocks_duplicate_and_reconciliation_claim_never_relaunches_it() {
    let db = fixture().await;
    let run = cleaning(&db, request("one")).await;
    let uncertain = db
        .mark_check_run_uncertain(&fence(&run), NOW)
        .await
        .unwrap();
    let joined = db.request_check_run(request("two")).await.unwrap();
    assert!(matches!(
        joined.disposition,
        CheckRequestDisposition::Joined
    ));
    assert_eq!(
        joined.consumer.run_id.as_deref(),
        Some(uncertain.id.as_str())
    );
    assert!(db
        .reusable_check_result(&identity())
        .await
        .unwrap()
        .is_none());
    let reconciler = db
        .claim_check_run(
            &uncertain.id,
            uncertain.version,
            "reconciler",
            "2026-10-08T00:01:01Z",
            "2026-10-08T00:02:00Z",
        )
        .await
        .unwrap();
    assert_eq!(reconciler.state, CheckRunState::Uncertain);
    assert_eq!(reconciler.operation_id, run.operation_id);
    assert_eq!(reconciler.lease_generation, uncertain.lease_generation + 1);
}
#[tokio::test]
async fn reusable_result_requires_exact_identity_certified_pass_and_successful_cleanup() {
    let db = fixture().await;
    let result = complete(
        &db,
        request("one"),
        CheckResultOutcome::Pass,
        CheckCleanup::Success,
    )
    .await;
    assert!(result.certified && result.cacheable);
    assert_eq!(
        db.reusable_check_result(&identity())
            .await
            .unwrap()
            .unwrap()
            .id,
        result.id
    );
    let reused = db.request_check_run(request("two")).await.unwrap();
    assert!(matches!(
        reused.disposition,
        CheckRequestDisposition::Reused
    ));
    assert_eq!(
        reused.consumer.result_id.as_deref(),
        Some(result.id.as_str())
    );
    for change in [
        |i: &mut CheckRunIdentity| {
            i.inputs.spec.commands[0].cacheability = CheckCacheability::Uncacheable
        },
        |i: &mut CheckRunIdentity| {
            i.inputs.execution_revision = CheckExecutionRevision {
                number: 1,
                audit_ref: Some("force-action".into()),
            }
        },
        |i: &mut CheckRunIdentity| {
            i.inputs.environment_identity = CheckEnvironmentIdentity::Attested {
                input_digest: "b".repeat(64),
            }
        },
        |i: &mut CheckRunIdentity| {
            i.inputs.environment_identity = CheckEnvironmentIdentity::NotAttested
        },
        |i: &mut CheckRunIdentity| i.commit_sha = "b".repeat(40),
        |i: &mut CheckRunIdentity| i.repo_id = "different".into(),
        |i: &mut CheckRunIdentity| i.project_id = "different".into(),
    ] {
        let mut changed = identity();
        change(&mut changed);
        assert!(db.reusable_check_result(&changed).await.unwrap().is_none());
    }
    let counts = db.check_run_counts().await.unwrap();
    assert_eq!(counts.reusable_results, 1);
    assert_eq!(counts.by_state["succeeded"], 1);
}
#[tokio::test]
async fn uncacheable_and_unattested_results_always_miss_and_repeat_after_completion() {
    for unattested in [false, true] {
        let db = fixture().await;
        let mut req = request("one");
        if unattested {
            req.identity.inputs.environment_identity = CheckEnvironmentIdentity::NotAttested;
        } else {
            req.identity.inputs.spec.commands[0].cacheability = CheckCacheability::Uncacheable;
        }
        let input = req.identity.clone();
        let result = complete(&db, req, CheckResultOutcome::Pass, CheckCleanup::Success).await;
        assert!(!result.cacheable);
        assert!(db.reusable_check_result(&input).await.unwrap().is_none());
        let mut again = request("two");
        again.identity = input;
        assert!(matches!(
            db.request_check_run(again).await.unwrap().disposition,
            CheckRequestDisposition::Scheduled
        ));
    }
}
#[tokio::test]
async fn failed_timeout_cancelled_and_cleanup_failure_results_never_reuse() {
    for (outcome, cleanup) in [
        (CheckResultOutcome::Fail, CheckCleanup::Success),
        (CheckResultOutcome::TimedOut, CheckCleanup::Success),
        (CheckResultOutcome::Cancelled, CheckCleanup::Success),
        (
            CheckResultOutcome::InfrastructureFailed,
            CheckCleanup::Success,
        ),
        (CheckResultOutcome::Pass, CheckCleanup::Failed),
        (CheckResultOutcome::Pass, CheckCleanup::NotPerformed),
        (CheckResultOutcome::Pass, CheckCleanup::Uncertain),
    ] {
        let db = fixture().await;
        let result = complete(&db, request("one"), outcome, cleanup).await;
        assert!(!result.certified);
        assert!(db
            .reusable_check_result(&identity())
            .await
            .unwrap()
            .is_none());
        assert_eq!(db.check_run_counts().await.unwrap().reusable_results, 0);
        assert_eq!(
            db.check_result(&result.id).await.unwrap().unwrap().cleanup,
            cleanup
        );
    }
}
#[tokio::test]
async fn immutable_uncertain_receipt_can_be_followed_by_reconciled_cleanup_receipt() {
    let db = fixture().await;
    let run = cleaning(&db, request("one")).await;
    let uncertain = db
        .finish_check_run(
            &fence(&run),
            evidence(CheckResultOutcome::Pass, CheckCleanup::Uncertain),
            NOW,
        )
        .await
        .unwrap();
    let run = db.check_run(&run.id).await.unwrap().unwrap();
    assert_eq!(run.state, CheckRunState::Uncertain);
    let run = db
        .transition_check_run(&fence(&run), CheckRunState::Cleaning, NOW)
        .await
        .unwrap();
    let reconciled = db
        .finish_check_run(
            &fence(&run),
            evidence(CheckResultOutcome::Pass, CheckCleanup::Success),
            NOW,
        )
        .await
        .unwrap();
    assert_ne!(uncertain.id, reconciled.id);
    assert_eq!(
        db.check_result(&uncertain.id)
            .await
            .unwrap()
            .unwrap()
            .cleanup,
        CheckCleanup::Uncertain
    );
    assert_eq!(
        db.reusable_check_result(&identity())
            .await
            .unwrap()
            .unwrap()
            .id,
        reconciled.id
    );
    assert_eq!(
        db.request_check_run(request("one"))
            .await
            .unwrap()
            .consumer
            .result_id
            .as_deref(),
        Some(reconciled.id.as_str())
    );
}
#[tokio::test]
async fn output_is_redacted_bounded_utf8_tail_and_flagged_and_json_caps_are_enforced() {
    let db = fixture().await;
    let run = cleaning(&db, request("one")).await;
    let mut output = evidence(CheckResultOutcome::Pass, CheckCleanup::Success);
    output.commands[0].output_tail = format!("{}long-secret", "é".repeat(5000));
    output.commands[0].stderr_tail = "long-secret".into();
    output.redaction_values = vec!["secret".into(), "long-secret".into()];
    let result = db
        .finish_check_run(&fence(&run), output, NOW)
        .await
        .unwrap();
    assert!(result.output_truncated);
    assert!(result.commands[0].output_tail.len() <= CHECK_OUTPUT_TAIL_BYTES);
    assert!(result.commands[0].output_tail.ends_with("[REDACTED]"));
    assert_eq!(result.commands[0].stderr_tail, "[REDACTED]");
    let serialized = serde_json::to_string(&result.commands).unwrap();
    assert!(!serialized.contains("long-secret"));
    let mut oversized = request("big");
    oversized.identity.inputs.spec.commands[0].shell_text = "x".repeat(CHECK_INPUT_BYTES);
    assert!(matches!(
        db.request_check_run(oversized).await,
        Err(DbError::Check(_))
    ));
    assert!(sqlx::query("UPDATE check_run SET input_json=? WHERE id=?")
        .bind(format!("\"{}\"", "x".repeat(CHECK_INPUT_BYTES)))
        .bind(&run.id)
        .execute(db.pool())
        .await
        .is_err());
    assert!(
        sqlx::query("UPDATE check_result SET steps_json=? WHERE id=?")
            .bind(format!("\"{}\"", "x".repeat(CHECK_STEPS_BYTES)))
            .bind(&result.id)
            .execute(db.pool())
            .await
            .is_err()
    );
    assert!(json(&"x".repeat(CHECK_STEPS_BYTES), CHECK_STEPS_BYTES).is_err());
}
#[tokio::test]
async fn pass_requires_complete_ordered_successful_commands_and_atomic_cas() {
    let db = fixture().await;
    let run = cleaning(&db, request("one")).await;
    for malformed in [0, 1, 2] {
        let mut output = evidence(CheckResultOutcome::Pass, CheckCleanup::Success);
        match malformed {
            0 => output.commands.clear(),
            1 => output.commands[0].command = "different".into(),
            _ => output.commands[0].exit_code = 1,
        }
        assert!(matches!(
            db.finish_check_run(&fence(&run), output, NOW).await,
            Err(DbError::Check(_))
        ));
        assert_eq!(
            db.check_run(&run.id).await.unwrap().unwrap().version,
            run.version
        );
    }
    db.finish_check_run(
        &fence(&run),
        evidence(CheckResultOutcome::Pass, CheckCleanup::Success),
        NOW,
    )
    .await
    .unwrap();
    assert!(matches!(
        db.finish_check_run(
            &fence(&run),
            evidence(CheckResultOutcome::Pass, CheckCleanup::Success),
            NOW
        )
        .await,
        Err(DbError::VersionConflict)
    ));
}
async fn counts(db: &SqliteDb) -> (i64, i64, i64) {
    let run = sqlx::query_scalar("SELECT COUNT(*) FROM check_run")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let result = sqlx::query_scalar("SELECT COUNT(*) FROM check_result")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let consumer = sqlx::query_scalar("SELECT COUNT(*) FROM check_consumer")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert!(sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(db.pool())
        .await
        .unwrap()
        .is_empty());
    (run, result, consumer)
}
#[tokio::test]
async fn repo_deletion_cascades_all_three_evidence_tables() {
    let db = fixture().await;
    complete(
        &db,
        request("one"),
        CheckResultOutcome::Pass,
        CheckCleanup::Success,
    )
    .await;
    WorkspaceRepo::delete(&db, "w").await.unwrap();
    RepoRepo::delete(&db, "r").await.unwrap();
    assert_eq!(counts(&db).await, (0, 0, 0));
}
#[tokio::test]
async fn project_deletion_cascades_all_three_evidence_tables() {
    let db = fixture().await;
    complete(
        &db,
        request("one"),
        CheckResultOutcome::Pass,
        CheckCleanup::Success,
    )
    .await;
    ProjectRepo::delete(&db, "p").await.unwrap();
    assert_eq!(counts(&db).await, (0, 0, 0));
}
#[tokio::test]
async fn task_deletion_nulls_consumers_and_preserves_result_history() {
    let db = fixture().await;
    complete(
        &db,
        request("one"),
        CheckResultOutcome::Pass,
        CheckCleanup::Success,
    )
    .await;
    sqlx::query("DELETE FROM task WHERE id='t'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(counts(&db).await, (1, 1, 1));
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>("SELECT task_id FROM check_consumer")
            .fetch_one(db.pool())
            .await
            .unwrap(),
        None
    );
}
#[tokio::test]
async fn workspace_deletion_nulls_run_reference_and_preserves_history() {
    let db = fixture().await;
    let result = complete(
        &db,
        request("one"),
        CheckResultOutcome::Pass,
        CheckCleanup::Success,
    )
    .await;
    WorkspaceRepo::delete(&db, "w").await.unwrap();
    assert_eq!(counts(&db).await, (1, 1, 1));
    assert_eq!(
        db.check_run(&result.run_id)
            .await
            .unwrap()
            .unwrap()
            .workspace_id,
        None
    );
}
#[tokio::test]
async fn machine_removal_and_deletion_do_not_block_and_preserve_attested_identity() {
    let db = fixture().await;
    let result = complete(
        &db,
        request("one"),
        CheckResultOutcome::Pass,
        CheckCleanup::Success,
    )
    .await;
    db.remove_daemon("m", "admin", true, "local-machine", false)
        .await
        .unwrap();
    assert_eq!(counts(&db).await, (1, 1, 1));
    sqlx::query("DELETE FROM daemon WHERE id='m'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(counts(&db).await, (1, 1, 1));
    let run = db.check_run(&result.run_id).await.unwrap().unwrap();
    assert_eq!(run.machine_id, None);
    assert_eq!(run.identity, identity());
}
#[tokio::test]
async fn counts_are_zero_on_a_fresh_database() {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    let counts = db.check_run_counts().await.unwrap();
    assert_eq!(counts.by_state.len(), CheckRunState::ALL.len());
    assert!(counts.by_state.values().all(|n| *n == 0));
    assert_eq!(counts.reusable_results, 0);
}
#[tokio::test]
async fn upgrade_preserves_old_rows_and_never_seeds_cache() {
    let dir = tempfile::tempdir().unwrap();
    let migrations = dir.path().join("migrations");
    std::fs::create_dir(&migrations).unwrap();
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    for entry in std::fs::read_dir(&source).unwrap() {
        let entry = entry.unwrap();
        if entry
            .file_name()
            .to_string_lossy()
            .ends_with("__check_runs.sql")
        {
            continue;
        }
        std::fs::copy(entry.path(), migrations.join(entry.file_name())).unwrap();
    }
    let pool = create_sqlite_pool(&format!(
        "sqlite://{}",
        dir.path().join("upgrade.db").display()
    ))
    .await
    .unwrap();
    crate::run_migrations_from(&pool, &migrations)
        .await
        .unwrap();
    let db = SqliteDb::new(pool);
    seed(&db).await;
    sqlx::query("INSERT INTO execution(id,task_id,role,status,workspace_id,created_at,updated_at) VALUES('e','t','executor','completed','w',?,?)").bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,created_at,updated_at) VALUES('old-review','t','e',1,'passed','{\"ci_steps\":[]}',?,?,?)").bind(NOW).bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
    let before = legacy_snapshot(&db).await;
    crate::run_migrations_from(db.pool(), &source)
        .await
        .unwrap();
    assert_eq!(before, legacy_snapshot(&db).await);
    assert_eq!(counts(&db).await, (0, 0, 0));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task WHERE id='t'")
            .fetch_one(db.pool())
            .await
            .unwrap(),
        1
    );
    db.pool().close().await;
}

async fn legacy_snapshot(db: &SqliteDb) -> Vec<Vec<Vec<Option<String>>>> {
    let mut snapshot = Vec::new();
    for table in [
        "project",
        "repo",
        "task",
        "workspace",
        "daemon",
        "execution",
        "review",
        "domain_event",
    ] {
        let columns = sqlx::query(&format!("PRAGMA table_info({table})"))
            .fetch_all(db.pool())
            .await
            .unwrap();
        let projection = columns
            .iter()
            .map(|row| {
                let name: String = row.get("name");
                format!("CAST(\"{name}\" AS TEXT)")
            })
            .collect::<Vec<_>>()
            .join(",");
        let rows = sqlx::query(&format!("SELECT {projection} FROM {table} ORDER BY id"))
            .fetch_all(db.pool())
            .await
            .unwrap();
        snapshot.push(
            rows.iter()
                .map(|row| {
                    (0..columns.len())
                        .map(|i| row.get::<Option<String>, _>(i))
                        .collect()
                })
                .collect(),
        );
    }
    snapshot
}

#[test]
fn mutable_refs_and_abbreviated_objects_are_not_check_identities() {
    let mut input = identity();
    for invalid in [
        "main",
        "refs/heads/main",
        "abc123",
        "",
        &"A".repeat(40),
        &"g".repeat(40),
    ] {
        input.commit_sha = invalid.into();
        assert!(matches!(input.key(), Err(DbError::Check(_))), "{invalid}");
    }
    for valid in ["a".repeat(40), "b".repeat(64)] {
        input.commit_sha = valid;
        assert!(input.key().is_ok());
    }
}
