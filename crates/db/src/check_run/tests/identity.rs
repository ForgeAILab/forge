//! Identity repository tests.
use super::*;

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
        let name = entry.file_name().to_string_lossy().into_owned();
        // The verify purpose rebuild needs the identity migration's
        // `purpose` column, so it follows it in the upgrade.
        if !name.ends_with("__check_run_identity.sql")
            && !name.ends_with("__review_verify_purpose.sql")
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
