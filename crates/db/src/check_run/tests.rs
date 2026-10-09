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
                schema_revision: CHECK_SPEC_REVISION,
                scope: CheckScope::Commit,
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
                declares_cleanup: false,
                configured_commands: 1,
                blank_commands: vec![],
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
        purpose: CheckPurpose::EntryCi,
        workspace_id: Some("w".into()),
        machine_id: Some("m".into()),
        wall_timeout_seconds: 1800,
    }
}
fn fence(run: &StoredCheckRun) -> CheckRunFence {
    CheckRunFence {
        run_id: run.id.clone(),
        version: run.version,
        lease_generation: run.lease_generation,
        lease_owner: run.lease_owner.clone(),
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
/// A fresh run driven to `state` through the repository alone.
async fn run_in(db: &SqliteDb, state: CheckRunState) -> StoredCheckRun {
    let id = db
        .request_check_run(request("edge"))
        .await
        .unwrap()
        .consumer
        .run_id
        .unwrap();
    if state == CheckRunState::Queued {
        return db.check_run(&id).await.unwrap().unwrap();
    }
    let run = db
        .claim_check_run(&id, 1, "worker", NOW, UNTIL)
        .await
        .unwrap();
    if state == CheckRunState::Running {
        return run;
    }
    db.transition_check_run(&fence(&run), state, NOW)
        .await
        .unwrap()
}
#[tokio::test]
async fn every_listed_transition_is_performed_by_the_repository_and_no_other_is() {
    for (from, to) in CHECK_RUN_TRANSITIONS {
        let db = fixture().await;
        let run = run_in(&db, *from).await;
        let reached = match (from, to) {
            (CheckRunState::Queued, CheckRunState::Running) => {
                db.claim_check_run(&run.id, run.version, "worker", NOW, UNTIL)
                    .await
                    .unwrap()
                    .state
            }
            (CheckRunState::Cleaning, CheckRunState::Succeeded | CheckRunState::Failed) => {
                let outcome = if *to == CheckRunState::Succeeded {
                    CheckResultOutcome::Pass
                } else {
                    CheckResultOutcome::Fail
                };
                db.finish_check_run(&fence(&run), evidence(outcome, CheckCleanup::Success), NOW)
                    .await
                    .unwrap();
                db.check_run(&run.id).await.unwrap().unwrap().state
            }
            _ => {
                db.transition_check_run(&fence(&run), *to, NOW)
                    .await
                    .unwrap()
                    .state
            }
        };
        assert_eq!(reached, *to, "{from} -> {to}");
    }
    for from in CheckRunState::ALL.iter().filter(|state| !state.terminal()) {
        let db = fixture().await;
        let run = run_in(&db, *from).await;
        for to in CheckRunState::ALL {
            // Evidence-bearing and claim-only edges are refused here too.
            let listed = CHECK_RUN_TRANSITIONS.contains(&(*from, *to))
                && !matches!(
                    to,
                    CheckRunState::Running | CheckRunState::Succeeded | CheckRunState::Failed
                );
            if listed {
                continue;
            }
            assert!(
                matches!(
                    db.transition_check_run(&fence(&run), *to, NOW).await,
                    Err(DbError::InvalidTransition)
                ),
                "{from} -> {to}"
            );
        }
        assert_eq!(db.check_run(&run.id).await.unwrap().unwrap().state, *from);
    }
}
#[tokio::test]
async fn queued_run_is_cancelled_by_version_alone_and_frees_its_identity() {
    let db = fixture().await;
    let queued = run_in(&db, CheckRunState::Queued).await;
    assert_eq!(queued.lease_owner, None);
    let mut stale = fence(&queued);
    stale.version += 1;
    assert!(matches!(
        db.transition_check_run(&stale, CheckRunState::Cancelled, NOW)
            .await,
        Err(DbError::VersionConflict)
    ));
    // A fence that invents an owner for an unclaimed run is not this run's.
    let mut invented = fence(&queued);
    invented.lease_owner = Some("worker".into());
    assert!(matches!(
        db.transition_check_run(&invented, CheckRunState::Cancelled, NOW)
            .await,
        Err(DbError::VersionConflict)
    ));
    // An unleased fence cannot settle or renew anything.
    assert!(matches!(
        db.finish_check_run(
            &fence(&queued),
            evidence(CheckResultOutcome::Pass, CheckCleanup::Success),
            NOW
        )
        .await,
        Err(DbError::VersionConflict)
    ));
    assert!(matches!(
        db.renew_check_run(&fence(&queued), NOW, UNTIL).await,
        Err(DbError::VersionConflict)
    ));
    let cancelled = db
        .transition_check_run(&fence(&queued), CheckRunState::Cancelled, NOW)
        .await
        .unwrap();
    assert_eq!(cancelled.state, CheckRunState::Cancelled);
    assert_eq!(cancelled.finished_at.as_deref(), Some(NOW));
    assert!(db.find_live_check_run(&identity()).await.unwrap().is_none());
    let next = db.request_check_run(request("two")).await.unwrap();
    assert!(matches!(
        next.disposition,
        CheckRequestDisposition::Scheduled
    ));
    assert_ne!(next.consumer.run_id.as_deref(), Some(queued.id.as_str()));
}
#[tokio::test]
async fn expired_lease_cannot_transition_and_unknown_scope_is_not_found() {
    let db = fixture().await;
    let run = run_in(&db, CheckRunState::Running).await;
    assert!(matches!(
        db.transition_check_run(&fence(&run), CheckRunState::Cleaning, UNTIL)
            .await,
        Err(DbError::VersionConflict)
    ));
    for change in [
        |r: &mut CheckRunRequest| r.identity.repo_id = "missing".into(),
        |r: &mut CheckRunRequest| r.task_id = Some("missing".into()),
        |r: &mut CheckRunRequest| r.workspace_id = Some("missing".into()),
    ] {
        let mut missing = request("missing");
        change(&mut missing);
        assert!(matches!(
            db.request_check_run(missing).await,
            Err(DbError::NotFound)
        ));
    }
}
#[tokio::test]
async fn unique_indexes_refuse_a_second_live_run_and_a_second_reusable_result() {
    let db = fixture().await;
    let result = complete(
        &db,
        request("one"),
        CheckResultOutcome::Pass,
        CheckCleanup::Success,
    )
    .await;
    // The repository serializes requests, so only raw rows reach the indexes.
    let copy_run = "INSERT INTO check_run(id,project_id,repo_id,commit_sha,spec_digest,identity_key,input_json,cacheable,state,operation_id,created_at,updated_at) SELECT ?,project_id,repo_id,commit_sha,spec_digest,identity_key,input_json,cacheable,?,?,created_at,updated_at FROM check_run WHERE id=?";
    for (id, state, accepted) in [
        ("live-1", "queued", true),
        ("live-2", "uncertain", false),
        ("done-2", "succeeded", true),
    ] {
        let inserted = sqlx::query(copy_run)
            .bind(id)
            .bind(state)
            .bind(format!("operation-{id}"))
            .bind(&result.run_id)
            .execute(db.pool())
            .await;
        match inserted {
            Ok(_) => assert!(accepted, "{id}"),
            Err(error) => assert!(
                !accepted
                    && error
                        .to_string()
                        .contains("UNIQUE constraint failed: check_run.identity_key"),
                "{id}: {error}"
            ),
        }
    }
    let second_pass = sqlx::query("INSERT INTO check_result(id,run_id,identity_key,outcome,cleanup,certified,cacheable,steps_json,output_truncated,created_at) SELECT 'second',?,identity_key,outcome,cleanup,certified,cacheable,steps_json,output_truncated,created_at FROM check_result WHERE id=?")
        .bind("done-2")
        .bind(&result.id)
        .execute(db.pool())
        .await;
    assert!(second_pass
        .unwrap_err()
        .to_string()
        .contains("UNIQUE constraint failed: check_result.identity_key"));
}
#[tokio::test]
async fn a_long_bundle_shrinks_its_tails_to_fit_the_row_instead_of_refusing_to_settle() {
    let db = fixture().await;
    let mut req = request("many");
    let template = req.identity.inputs.spec.commands[0].clone();
    req.identity.inputs.spec.commands = (0..64)
        .map(|index| CheckCommandSpec {
            id: format!("ci:{index}"),
            ..template.clone()
        })
        .collect();
    req.identity.inputs.spec.configured_commands = 64;
    let run = cleaning(&db, req).await;
    let mut output = evidence(CheckResultOutcome::Pass, CheckCleanup::Success);
    let step = output.commands[0].clone();
    output.commands = (0..64)
        .map(|index| CheckCommandOutcome {
            index,
            output_tail: "o".repeat(CHECK_OUTPUT_TAIL_BYTES),
            stderr_tail: format!("{}end-{index}", "e".repeat(CHECK_OUTPUT_TAIL_BYTES)),
            ..step.clone()
        })
        .collect();
    let result = db
        .finish_check_run(&fence(&run), output, NOW)
        .await
        .unwrap();
    assert!(result.output_truncated && result.certified);
    assert_eq!(result.commands.len(), 64);
    assert!(serde_json::to_string(&result.commands).unwrap().len() <= CHECK_STEPS_BYTES);
    for (index, step) in result.commands.iter().enumerate() {
        assert_eq!(step.output_tail.len(), CHECK_OUTPUT_TAIL_BYTES / 4);
        assert!(step.stderr_tail.ends_with(&format!("end-{index}")));
    }
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
    assert_eq!(takeover.state, CheckRunState::Uncertain);
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
        // The bundle declares a cleanup step, so an unperformed one is incomplete.
        let mut req = request("one");
        req.identity.inputs.spec.declares_cleanup = true;
        let declared = req.identity.clone();
        let result = complete(&db, req, outcome, cleanup).await;
        assert!(!result.certified);
        assert!(db.reusable_check_result(&declared).await.unwrap().is_none());
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
            || entry
                .file_name()
                .to_string_lossy()
                .ends_with("__check_run_identity.sql")
            || entry
                .file_name()
                .to_string_lossy()
                .ends_with("__check_runner.sql")
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

/// A second Task `t2` with its own worktree `w2`, at the same commit.
async fn seed_second_task(db: &SqliteDb) {
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t2','p','task two','todo',?,?)").bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES('w2','t2','r',?,'task/branch-two','ready',?,?)")
        .bind(std::env::temp_dir().join("check-contract-workspace-two").to_str().unwrap()).bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
}
/// The same blocking before-work script, asked for by `task` in `workspace`.
fn before_work(key: &str, task: &str, workspace: &str) -> CheckRunRequest {
    let mut req = request(key);
    req.task_id = Some(task.into());
    req.workspace_id = Some(workspace.into());
    req.origin = CheckConsumerOrigin::BeforeWork;
    req.purpose = CheckPurpose::BeforeWork;
    let spec = &mut req.identity.inputs.spec;
    spec.scope = CheckScope::Workspace {
        workspace_id: workspace.into(),
        generation: 1,
    };
    spec.commands[0].id = "hook:0".into();
    spec.commands[0].shell_text = "./prepare-worktree".into();
    spec.commands[0].cacheability = CheckCacheability::Uncacheable;
    req
}
async fn rows(db: &SqliteDb, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
        .fetch_one(db.pool())
        .await
        .unwrap()
}
#[tokio::test]
async fn two_tasks_at_one_commit_each_prepare_their_worktree_and_share_one_commit_check() {
    let db = fixture().await;
    seed_second_task(&db).await;
    let first = db
        .request_check_run(before_work("prepare-1", "t", "w"))
        .await
        .unwrap();
    let second = db
        .request_check_run(before_work("prepare-2", "t2", "w2"))
        .await
        .unwrap();
    for response in [&first, &second] {
        assert!(matches!(
            response.disposition,
            CheckRequestDisposition::Scheduled
        ));
    }
    assert_ne!(first.consumer.run_id, second.consumer.run_id);
    assert_ne!(first.consumer.identity_key, second.consumer.identity_key);
    assert_eq!(rows(&db, "check_run").await, 2);
    // The same worktree asking twice still joins its own run.
    let again = db
        .request_check_run(before_work("prepare-1-again", "t", "w"))
        .await
        .unwrap();
    assert_eq!(again.consumer.run_id, first.consumer.run_id);
    // A re-placed worktree (next generation) is a new target.
    let mut regenerated = before_work("prepare-1-regenerated", "t", "w");
    regenerated.identity.inputs.spec.scope = CheckScope::Workspace {
        workspace_id: "w".into(),
        generation: 2,
    };
    assert!(matches!(
        db.request_check_run(regenerated).await.unwrap().disposition,
        CheckRequestDisposition::Scheduled
    ));
    // A requester cannot ask for another worktree's or Task's scoped run.
    let mut foreign = before_work("foreign", "t", "w");
    foreign.workspace_id = Some("w2".into());
    assert!(matches!(
        db.request_check_run(foreign).await,
        Err(DbError::Check(_))
    ));
    let mut foreign = before_work("foreign-task", "t2", "w2");
    foreign.identity.inputs.spec.scope = CheckScope::Task {
        task_id: "t".into(),
    };
    assert!(matches!(
        db.request_check_run(foreign).await,
        Err(DbError::Check(_))
    ));

    // CI that only reads the commit carries no Task or workspace: both Tasks
    // share one run, whichever worktree it is placed in.
    let ci_first = db.request_check_run(request("ci-1")).await.unwrap();
    let mut other = request("ci-2");
    other.task_id = Some("t2".into());
    other.workspace_id = Some("w2".into());
    let ci_second = db.request_check_run(other).await.unwrap();
    assert!(matches!(
        ci_second.disposition,
        CheckRequestDisposition::Joined
    ));
    assert_eq!(ci_first.consumer.run_id, ci_second.consumer.run_id);
    assert_eq!(rows(&db, "check_run").await, 4);
}
#[tokio::test]
async fn purpose_is_recorded_on_the_consumer_and_never_splits_a_run() {
    let db = fixture().await;
    let entry = db.request_check_run(request("entry")).await.unwrap();
    let mut review = request("review");
    review.origin = CheckConsumerOrigin::ManualReview;
    review.purpose = CheckPurpose::ReviewCi;
    let review = db.request_check_run(review).await.unwrap();
    assert!(matches!(
        review.disposition,
        CheckRequestDisposition::Joined
    ));
    assert_eq!(entry.consumer.run_id, review.consumer.run_id);
    assert_eq!(entry.consumer.identity_key, review.consumer.identity_key);
    assert_eq!(entry.consumer.purpose, Some(CheckPurpose::EntryCi));
    assert_eq!(review.consumer.purpose, Some(CheckPurpose::ReviewCi));
    assert_eq!(
        (
            rows(&db, "check_run").await,
            rows(&db, "check_consumer").await
        ),
        (1, 2)
    );
    // Every purpose is storable, and all of them share the one run.
    for purpose in CheckPurpose::ALL {
        let mut req = request(purpose.as_str());
        req.purpose = *purpose;
        let consumer = db.request_check_run(req).await.unwrap().consumer;
        assert_eq!(consumer.purpose, Some(*purpose));
        assert_eq!(consumer.run_id, entry.consumer.run_id);
    }
    assert_eq!(rows(&db, "check_run").await, 1);
    // The request key still pins what its consumer asked for.
    let mut changed = request("entry");
    changed.purpose = CheckPurpose::QueueHeadCi;
    assert!(matches!(
        db.request_check_run(changed).await,
        Err(DbError::IdempotencyConflict)
    ));
}
/// A certified reusable pass for `identity()`, then a request that differs
/// only by `change`: it must neither reuse that result nor join a live run.
async fn assert_never_shared(change: fn(&mut CheckRunIdentity)) {
    let db = fixture().await;
    let result = complete(
        &db,
        request("one"),
        CheckResultOutcome::Pass,
        CheckCleanup::Success,
    )
    .await;
    assert!(result.certified && result.cacheable);
    let mut changed = request("two");
    change(&mut changed.identity);
    let other = changed.identity.clone();
    assert!(db.reusable_check_result(&other).await.unwrap().is_none());
    let scheduled = db.request_check_run(changed).await.unwrap();
    assert!(matches!(
        scheduled.disposition,
        CheckRequestDisposition::Scheduled
    ));
    assert_eq!(scheduled.consumer.result_id, None);
    assert_ne!(
        scheduled.consumer.run_id.as_deref(),
        Some(result.run_id.as_str())
    );
    // While that second run is live, the original identity does not join it.
    let original = db.request_check_run(request("three")).await.unwrap();
    assert!(matches!(
        original.disposition,
        CheckRequestDisposition::Reused
    ));
    assert_eq!(
        db.find_live_check_run(&other).await.unwrap().unwrap().id,
        scheduled.consumer.run_id.unwrap()
    );
    assert!(db.find_live_check_run(&identity()).await.unwrap().is_none());
}
#[tokio::test]
async fn a_result_never_crosses_environment_attestations() {
    // Another machine or environment attests different inputs.
    assert_never_shared(|identity| {
        identity.inputs.environment_identity = CheckEnvironmentIdentity::Attested {
            input_digest: "b".repeat(64),
        }
    })
    .await;
}
#[tokio::test]
async fn a_result_never_survives_a_forced_rerun_revision() {
    assert_never_shared(|identity| {
        identity.inputs.execution_revision = CheckExecutionRevision {
            number: 1,
            audit_ref: Some("owner-forced-rerun".into()),
        }
    })
    .await;
}
const AFTER: &str = "2026-10-08T00:01:01Z";
const AFTER_UNTIL: &str = "2026-10-08T00:02:00Z";
#[tokio::test]
async fn taking_over_an_expired_lease_makes_a_dispatched_run_uncertain() {
    for state in [
        CheckRunState::Running,
        CheckRunState::Cancelling,
        CheckRunState::Cleaning,
    ] {
        let db = fixture().await;
        let lost = run_in(&db, state).await;
        // An unexpired lease is not taken over at all.
        assert!(matches!(
            db.claim_check_run(&lost.id, lost.version, "second", NOW, UNTIL)
                .await,
            Err(DbError::VersionConflict)
        ));
        let taken = db
            .claim_check_run(&lost.id, lost.version, "second", AFTER, AFTER_UNTIL)
            .await
            .unwrap();
        assert_eq!(taken.state, CheckRunState::Uncertain, "{state}");
        assert_eq!(taken.lease_owner.as_deref(), Some("second"));
        assert_eq!(taken.operation_id, lost.operation_id);
        assert_eq!(taken.finished_at, None);
        // The lost owner is fenced out; the new one cannot continue the run.
        assert!(matches!(
            db.transition_check_run(&fence(&lost), CheckRunState::Cleaning, AFTER)
                .await,
            Err(DbError::VersionConflict)
        ));
        assert!(matches!(
            db.transition_check_run(&fence(&taken), CheckRunState::Running, AFTER)
                .await,
            Err(DbError::InvalidTransition)
        ));
        assert!(matches!(
            db.finish_check_run(
                &fence(&taken),
                evidence(CheckResultOutcome::Pass, CheckCleanup::Success),
                AFTER
            )
            .await,
            Err(DbError::InvalidTransition)
        ));
        // It still occupies the identity: nobody launches a duplicate.
        assert!(matches!(
            db.request_check_run(request("late"))
                .await
                .unwrap()
                .disposition,
            CheckRequestDisposition::Joined
        ));
        // The only exits are stage A's: reconcile with evidence, or cancel.
        let reconciling = db
            .transition_check_run(&fence(&taken), CheckRunState::Cleaning, AFTER)
            .await
            .unwrap();
        let settled = db
            .finish_check_run(
                &fence(&reconciling),
                evidence(CheckResultOutcome::Pass, CheckCleanup::Success),
                AFTER,
            )
            .await
            .unwrap();
        assert!(settled.certified);
    }
    // Nobody started a queued run: claiming it simply starts it.
    let db = fixture().await;
    let queued = run_in(&db, CheckRunState::Queued).await;
    let started = db
        .claim_check_run(&queued.id, queued.version, "second", AFTER, AFTER_UNTIL)
        .await
        .unwrap();
    assert_eq!(started.state, CheckRunState::Running);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_takeover_of_an_expired_running_lease_has_one_winner_and_is_uncertain() {
    let dir = tempfile::tempdir().unwrap();
    let pool = create_sqlite_pool(&format!(
        "sqlite://{}",
        dir.path().join("takeover.db").display()
    ))
    .await
    .unwrap();
    run_migrations(&pool).await.unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    seed(&db).await;
    let running = run_in(&db, CheckRunState::Running).await;
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut claims = Vec::new();
    for n in 0..8 {
        let (db, barrier, id) = (db.clone(), barrier.clone(), running.id.clone());
        claims.push(tokio::spawn(async move {
            barrier.wait().await;
            db.claim_check_run(
                &id,
                running.version,
                &format!("taker-{n}"),
                AFTER,
                AFTER_UNTIL,
            )
            .await
        }));
    }
    let mut winners = Vec::new();
    for claim in claims {
        match claim.await.unwrap() {
            Ok(run) => winners.push(run),
            Err(DbError::VersionConflict) => {}
            other => panic!("unexpected takeover {other:?}"),
        }
    }
    assert_eq!(winners.len(), 1);
    let stored = db.check_run(&running.id).await.unwrap().unwrap();
    assert_eq!(winners[0].state, CheckRunState::Uncertain);
    assert_eq!(stored.state, CheckRunState::Uncertain);
    assert_eq!(stored.lease_owner, winners[0].lease_owner);
    assert_eq!(stored.lease_generation, running.lease_generation + 1);
    assert_eq!(stored.version, running.version + 1);
    db.pool().close().await;
}
#[tokio::test]
async fn a_pass_without_a_cleanup_step_settles_passed_and_a_declared_cleanup_must_run() {
    for (declares_cleanup, cleanup, passed) in [
        (false, CheckCleanup::NotPerformed, true),
        (false, CheckCleanup::Success, true),
        (true, CheckCleanup::NotPerformed, false),
        (true, CheckCleanup::Success, true),
        (false, CheckCleanup::Failed, false),
    ] {
        let db = fixture().await;
        let mut req = request("one");
        req.identity.inputs.spec.declares_cleanup = declares_cleanup;
        let asked = req.identity.clone();
        let result = complete(&db, req, CheckResultOutcome::Pass, cleanup).await;
        let case = format!("declares_cleanup={declares_cleanup} cleanup={cleanup}");
        assert_eq!(result.certified, passed, "{case}");
        assert_eq!(result.cleanup, cleanup, "{case}");
        assert_eq!(
            db.check_run(&result.run_id).await.unwrap().unwrap().state,
            if passed {
                CheckRunState::Succeeded
            } else {
                CheckRunState::Failed
            },
            "{case}"
        );
        assert_eq!(
            db.reusable_check_result(&asked).await.unwrap().is_some(),
            passed,
            "{case}"
        );
        assert_eq!(
            db.check_run_counts().await.unwrap().reusable_results,
            i64::from(passed),
            "{case}"
        );
    }
    // Only a pass is ever certified without a cleanup.
    let db = fixture().await;
    let failed = complete(
        &db,
        request("one"),
        CheckResultOutcome::Fail,
        CheckCleanup::NotPerformed,
    )
    .await;
    assert!(!failed.certified);
}
#[tokio::test]
async fn the_wall_timeout_is_recorded_on_the_run_and_is_not_part_of_its_identity() {
    let db = fixture().await;
    let applied = |id: String| {
        let db = &db;
        async move {
            db.check_run(&id)
                .await
                .unwrap()
                .unwrap()
                .applied_timeout_seconds
        }
    };
    let first = db.request_check_run(request("one")).await.unwrap();
    let run_id = first.consumer.run_id.clone().unwrap();
    assert_eq!(applied(run_id.clone()).await, Some(1800));
    // The setting changed while the run is live: same run, same recorded limit.
    let mut joined = request("two");
    joined.wall_timeout_seconds = 60;
    let joined = db.request_check_run(joined).await.unwrap();
    assert_eq!(joined.consumer.identity_key, first.consumer.identity_key);
    assert_eq!(joined.consumer.run_id.as_deref(), Some(run_id.as_str()));
    assert_eq!(applied(run_id.clone()).await, Some(1800));
    let run = db
        .claim_check_run(&run_id, 1, "worker", NOW, UNTIL)
        .await
        .unwrap();
    let run = db
        .transition_check_run(&fence(&run), CheckRunState::Cleaning, NOW)
        .await
        .unwrap();
    let passed = db
        .finish_check_run(
            &fence(&run),
            evidence(CheckResultOutcome::Pass, CheckCleanup::Success),
            NOW,
        )
        .await
        .unwrap();
    // Raising or lowering the setting never invalidates the reusable result.
    for (key, seconds) in [("raised", 7200), ("lowered", 30)] {
        let mut later = request(key);
        later.wall_timeout_seconds = seconds;
        let later = db.request_check_run(later).await.unwrap();
        assert!(matches!(later.disposition, CheckRequestDisposition::Reused));
        assert_eq!(
            later.consumer.result_id.as_deref(),
            Some(passed.id.as_str())
        );
    }
    assert_eq!(rows(&db, "check_run").await, 1);

    // A run that ended by timeout is never a reusable result, so the same
    // identity runs again under the limit in force then.
    let mut short = request("short");
    short.identity.commit_sha = "b".repeat(40);
    short.wall_timeout_seconds = 60;
    let identity = short.identity.clone();
    let timed_out = complete(
        &db,
        short,
        CheckResultOutcome::TimedOut,
        CheckCleanup::Success,
    )
    .await;
    assert!(!timed_out.certified);
    assert!(db.reusable_check_result(&identity).await.unwrap().is_none());
    let mut raised = request("after-raise");
    raised.identity = identity;
    raised.wall_timeout_seconds = 3600;
    let raised = db.request_check_run(raised).await.unwrap();
    assert!(matches!(
        raised.disposition,
        CheckRequestDisposition::Scheduled
    ));
    assert_eq!(raised.consumer.identity_key, timed_out.identity_key);
    assert_eq!(applied(raised.consumer.run_id.unwrap()).await, Some(3600));
    assert_eq!(applied(timed_out.run_id).await, Some(60));

    let mut unbounded = request("zero");
    unbounded.wall_timeout_seconds = 0;
    assert!(matches!(
        db.request_check_run(unbounded).await,
        Err(DbError::Check(_))
    ));
}
#[tokio::test]
async fn identity_migration_carries_stage_a_rows_and_their_result_links() {
    let dir = tempfile::tempdir().unwrap();
    let migrations = dir.path().join("migrations");
    std::fs::create_dir(&migrations).unwrap();
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    for entry in std::fs::read_dir(&source).unwrap() {
        let entry = entry.unwrap();
        if !entry
            .file_name()
            .to_string_lossy()
            .ends_with("__check_run_identity.sql")
        {
            std::fs::copy(entry.path(), migrations.join(entry.file_name())).unwrap();
        }
    }
    let pool = create_sqlite_pool(&format!(
        "sqlite://{}",
        dir.path().join("identity.db").display()
    ))
    .await
    .unwrap();
    crate::run_migrations_from(&pool, &migrations)
        .await
        .unwrap();
    let db = SqliteDb::new(pool);
    seed(&db).await;
    // Rows as stage A's tables would have held them.
    sqlx::query("INSERT INTO check_run(id,project_id,repo_id,commit_sha,spec_digest,identity_key,input_json,cacheable,state,operation_id,created_at,updated_at) VALUES('run','p','r',?,'digest','key','{}',1,'succeeded','operation',?,?)")
        .bind("a".repeat(40)).bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
    for (id, outcome, certified) in [("pass", "pass", 1), ("fail", "fail", 0)] {
        sqlx::query("INSERT INTO check_result(id,run_id,identity_key,outcome,cleanup,certified,cacheable,steps_json,output_truncated,created_at) VALUES(?,'run','key',?,'success',?,1,'[]',0,?)")
            .bind(id).bind(outcome).bind(certified).bind(NOW).execute(db.pool()).await.unwrap();
    }
    for (id, result) in [("linked", Some("pass")), ("unlinked", None)] {
        sqlx::query("INSERT INTO check_consumer(id,project_id,repo_id,task_id,status_epoch,origin,request_key,identity_key,run_id,result_id,created_at) VALUES(?,'p','r','t',0,'entry',?,'key','run',?,?)")
            .bind(id).bind(id).bind(result).bind(NOW).execute(db.pool()).await.unwrap();
    }
    let before = legacy_snapshot(&db).await;
    crate::run_migrations_from(db.pool(), &source)
        .await
        .unwrap();
    assert_eq!(before, legacy_snapshot(&db).await);
    assert_eq!(counts(&db).await, (1, 2, 2));
    let links: Vec<(String, Option<String>, Option<String>)> =
        sqlx::query_as("SELECT id,result_id,purpose FROM check_consumer ORDER BY id")
            .fetch_all(db.pool())
            .await
            .unwrap();
    assert_eq!(
        links,
        [
            ("linked".into(), Some("pass".into()), None),
            ("unlinked".into(), None, None)
        ]
    );
    let results: Vec<(String, String, bool)> =
        sqlx::query_as("SELECT id,outcome,certified FROM check_result ORDER BY id")
            .fetch_all(db.pool())
            .await
            .unwrap();
    assert_eq!(
        results,
        [
            ("fail".into(), "fail".into(), false),
            ("pass".into(), "pass".into(), true)
        ]
    );
    let run = sqlx::query("SELECT applied_timeout_seconds FROM check_run WHERE id='run'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(run.get::<Option<i64>, _>(0), None);
    // The rebuilt table keeps its guards and its link to the run.
    let insert = "INSERT INTO check_result(id,run_id,identity_key,outcome,cleanup,certified,cacheable,steps_json,output_truncated,created_at) VALUES(?,'run','key',?,?,1,0,'[]',0,?)";
    for (id, outcome, cleanup, accepted) in [
        ("no-cleanup", "pass", "not_performed", true),
        ("failed-cleanup", "pass", "failed", false),
        ("uncertain-cleanup", "pass", "uncertain", false),
        ("timed-out", "timed_out", "success", false),
    ] {
        let inserted = sqlx::query(insert)
            .bind(id)
            .bind(outcome)
            .bind(cleanup)
            .bind(NOW)
            .execute(db.pool())
            .await;
        assert_eq!(inserted.is_ok(), accepted, "{id}");
    }
    sqlx::query("DELETE FROM check_run WHERE id='run'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(rows(&db, "check_result").await, 0);
    assert_eq!(rows(&db, "check_consumer").await, 2);
    db.pool().close().await;
}
