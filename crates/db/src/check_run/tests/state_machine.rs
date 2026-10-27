//! State machine repository tests.
use super::*;

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
