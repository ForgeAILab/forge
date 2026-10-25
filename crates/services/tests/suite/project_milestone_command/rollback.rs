//! Milestone, readiness, and release receipt failures roll back atomically.

use super::*;

#[tokio::test]
async fn milestone_define_receipt_failpoint_rolls_back_checks_pointer_and_version() {
    let db = fixture().await;
    let command = definition_command(DefinitionCommandInput {
        project_version: 1,
        milestone_id: None,
        milestone_version: 1,
        base_revision_id: None,
        lifecycle: MilestoneDefinitionLifecycle::Proposed,
        key: "failpoint-milestone-define",
        name: "Failpoint milestone",
        check_id: Some("failpoint-define-check"),
    });
    let service = ProjectMilestoneCommandService::new(Arc::clone(&db));
    let before: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT version FROM project WHERE id = ?),
            (SELECT COUNT(*) FROM project_milestone WHERE project_id = ?),
            (SELECT COUNT(*) FROM project_milestone_revision WHERE milestone_id = ?),
            (SELECT COUNT(*) FROM project_milestone_check WHERE project_id = ?),
            (SELECT COUNT(*) FROM command_receipt WHERE operation = ? AND scope_id = ?)",
    )
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .bind(MILESTONE_ID)
    .bind(PROJECT_ID)
    .bind(PROJECT_MILESTONE_OPERATION)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("define pre-state");
    assert_eq!(before, (1, 1, 1, 0, 0));

    arm_receipt_failpoint(
        &db,
        "milestone_define_receipt_failpoint",
        "milestone define receipt failpoint",
    )
    .await;
    let failed = service
        .define_milestone(command.clone(), None)
        .await
        .expect_err("receipt failpoint aborts milestone define");
    assert!(failed.to_string().contains("failpoint"));
    drop_receipt_failpoint(&db, "milestone_define_receipt_failpoint").await;

    let after_failure: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT version FROM project WHERE id = ?),
            (SELECT COUNT(*) FROM project_milestone WHERE project_id = ?),
            (SELECT COUNT(*) FROM project_milestone_revision WHERE milestone_id = ?),
            (SELECT COUNT(*) FROM project_milestone_check WHERE project_id = ?),
            (SELECT COUNT(*) FROM command_receipt WHERE operation = ? AND scope_id = ?)",
    )
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .bind(MILESTONE_ID)
    .bind(PROJECT_ID)
    .bind(PROJECT_MILESTONE_OPERATION)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("define post-failure state");
    assert_eq!(after_failure, before);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'milestone.definition.created'",
        )
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("define event absence"),
        0
    );

    let first = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .define_milestone(command.clone(), None)
        .await
        .expect("define retry after receipt failpoint");
    assert_eq!(first.revision, 1);
    assert_eq!(first.lifecycle, "proposed");
    let pointer: (String, i64) = sqlx::query_as(
        "SELECT current_definition_revision_id, version
         FROM project_milestone WHERE id = ?",
    )
    .bind(&first.milestone_id)
    .fetch_one(db.pool())
    .await
    .expect("defined milestone pointer");
    assert_eq!(pointer, (first.id.clone(), 2));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_milestone_check
             WHERE project_id = ? AND definition_revision_id = ?",
        )
        .bind(PROJECT_ID)
        .bind(&first.id)
        .fetch_one(db.pool())
        .await
        .expect("defined acceptance check"),
        1
    );

    // Simulate a lost response and a restarted service. The receipt is the
    // only source of the returned immutable revision, so no second shell,
    // revision, check, pointer update, or event may appear.
    let replay = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .define_milestone(command, None)
        .await
        .expect("define replay after service recreation");
    assert_eq!(replay, first);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_milestone
             WHERE project_id = ?",
        )
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("define milestone count"),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE scope_id = ? AND operation = ? AND idempotency_key = ?",
        )
        .bind(PROJECT_ID)
        .bind(PROJECT_MILESTONE_OPERATION)
        .bind("failpoint-milestone-define")
        .fetch_one(db.pool())
        .await
        .expect("define receipt count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'milestone.definition.created'",
        )
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("define event count"),
        1
    );
}

#[tokio::test]
async fn milestone_revise_receipt_failpoint_rolls_back_checks_pointer_and_version() {
    let db = fixture().await;
    let command = definition_command(DefinitionCommandInput {
        project_version: 0,
        milestone_id: Some(MILESTONE_ID),
        milestone_version: 1,
        base_revision_id: Some(MILESTONE_REVISION_ID),
        lifecycle: MilestoneDefinitionLifecycle::Proposed,
        key: "failpoint-milestone-revise",
        name: "Failpoint revision",
        check_id: Some("failpoint-revise-check"),
    });
    let before: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT version FROM project_milestone WHERE id = ?),
            (SELECT COUNT(*) FROM project_milestone_revision WHERE milestone_id = ?),
            (SELECT COUNT(*) FROM project_milestone_check WHERE project_id = ?),
            (SELECT COUNT(*) FROM command_receipt WHERE operation = ? AND scope_id = ?),
            (SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'milestone.definition.revised')",
    )
    .bind(MILESTONE_ID)
    .bind(MILESTONE_ID)
    .bind(PROJECT_ID)
    .bind(PROJECT_MILESTONE_OPERATION)
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("revise pre-state");
    assert_eq!(before, (1, 1, 0, 0, 0));

    arm_receipt_failpoint(
        &db,
        "milestone_revise_receipt_failpoint",
        "milestone revise receipt failpoint",
    )
    .await;
    let failed = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .revise_milestone(command.clone(), None)
        .await
        .expect_err("receipt failpoint aborts milestone revise");
    assert!(failed.to_string().contains("failpoint"));
    drop_receipt_failpoint(&db, "milestone_revise_receipt_failpoint").await;

    let after_failure: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT version FROM project_milestone WHERE id = ?),
            (SELECT COUNT(*) FROM project_milestone_revision WHERE milestone_id = ?),
            (SELECT COUNT(*) FROM project_milestone_check WHERE project_id = ?),
            (SELECT COUNT(*) FROM command_receipt WHERE operation = ? AND scope_id = ?),
            (SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'milestone.definition.revised')",
    )
    .bind(MILESTONE_ID)
    .bind(MILESTONE_ID)
    .bind(PROJECT_ID)
    .bind(PROJECT_MILESTONE_OPERATION)
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("revise post-failure state");
    assert_eq!(after_failure, before);

    let first = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .revise_milestone(command.clone(), None)
        .await
        .expect("revise retry after receipt failpoint");
    assert_eq!(first.revision, 2);
    let pointer: (String, i64) = sqlx::query_as(
        "SELECT current_definition_revision_id, version
         FROM project_milestone WHERE id = ?",
    )
    .bind(MILESTONE_ID)
    .fetch_one(db.pool())
    .await
    .expect("revised milestone pointer");
    assert_eq!(pointer, (first.id.clone(), 2));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_milestone_check
             WHERE project_id = ? AND definition_revision_id = ?",
        )
        .bind(PROJECT_ID)
        .bind(&first.id)
        .fetch_one(db.pool())
        .await
        .expect("revised acceptance check"),
        1
    );

    sqlx::query("UPDATE project_milestone SET version = 9 WHERE id = ?")
        .bind(MILESTONE_ID)
        .execute(db.pool())
        .await
        .expect("mutate milestone after revise");
    let replay = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .revise_milestone(command, None)
        .await
        .expect("revise replay after service recreation");
    assert_eq!(replay, first);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_milestone_revision WHERE milestone_id = ?",
        )
        .bind(MILESTONE_ID)
        .fetch_one(db.pool())
        .await
        .expect("revise revision count"),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE scope_id = ? AND operation = ? AND idempotency_key = ?",
        )
        .bind(PROJECT_ID)
        .bind(PROJECT_MILESTONE_OPERATION)
        .bind("failpoint-milestone-revise")
        .fetch_one(db.pool())
        .await
        .expect("revise receipt count"),
        1
    );
}

#[tokio::test]
async fn milestone_primary_receipt_failpoint_rolls_back_pointer_and_version() {
    let db = fixture().await;
    let command = ProjectPrimaryMilestoneCommand {
        project_id: PROJECT_ID.to_owned(),
        primary_milestone_id: Some(MILESTONE_ID.to_owned()),
        expected_project_version: 1,
        idempotency_key: "failpoint-milestone-primary".to_owned(),
        authorization: user_authorization(
            "project.milestone.primary.set",
            "failpoint-milestone-primary",
        ),
    };
    let before: (i64, Option<String>, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT version FROM project WHERE id = ?),
            (SELECT primary_milestone_id FROM project WHERE id = ?),
            (SELECT COUNT(*) FROM command_receipt WHERE operation = ? AND scope_id = ?),
            (SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'milestone.primary.set')",
    )
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .bind(PROJECT_MILESTONE_OPERATION)
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("primary pre-state");
    assert_eq!(before, (1, None, 0, 0));

    arm_receipt_failpoint(
        &db,
        "milestone_primary_receipt_failpoint",
        "milestone primary receipt failpoint",
    )
    .await;
    let failed = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .set_primary_milestone(command.clone(), None)
        .await
        .expect_err("receipt failpoint aborts primary milestone set");
    assert!(failed.to_string().contains("failpoint"));
    drop_receipt_failpoint(&db, "milestone_primary_receipt_failpoint").await;

    let after_failure: (i64, Option<String>, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT version FROM project WHERE id = ?),
            (SELECT primary_milestone_id FROM project WHERE id = ?),
            (SELECT COUNT(*) FROM command_receipt WHERE operation = ? AND scope_id = ?),
            (SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'milestone.primary.set')",
    )
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .bind(PROJECT_MILESTONE_OPERATION)
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("primary post-failure state");
    assert_eq!(after_failure, before);

    let first = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .set_primary_milestone(command.clone(), None)
        .await
        .expect("primary retry after receipt failpoint");
    assert_eq!(first.version, 2);
    assert_eq!(first.primary_milestone_id.as_deref(), Some(MILESTONE_ID));

    sqlx::query("UPDATE project SET name = 'Mutated after primary' WHERE id = ?")
        .bind(PROJECT_ID)
        .execute(db.pool())
        .await
        .expect("mutate primary project after commit");
    let replay = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .set_primary_milestone(command, None)
        .await
        .expect("primary replay after service recreation");
    assert_eq!(replay, first);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE scope_id = ? AND operation = ?
               AND idempotency_key = 'failpoint-milestone-primary'",
        )
        .bind(PROJECT_ID)
        .bind(PROJECT_MILESTONE_OPERATION)
        .fetch_one(db.pool())
        .await
        .expect("primary receipt count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'milestone.primary.set'",
        )
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("primary event count"),
        1
    );
}

#[tokio::test]
async fn readiness_receipt_failpoint_rolls_back_snapshot_lifecycle_and_version() {
    let db = fixture().await;
    let command = readiness_command("failpoint-readiness");
    let before: (i64, String, i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT version FROM project_milestone WHERE id = ?),
            (SELECT lifecycle FROM project_milestone WHERE id = ?),
            (SELECT COUNT(*) FROM project_readiness_snapshot WHERE project_id = ?),
            (SELECT COUNT(*) FROM command_receipt WHERE operation = ? AND scope_id = ?),
            (SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'milestone.readiness.evaluated')",
    )
    .bind(MILESTONE_ID)
    .bind(MILESTONE_ID)
    .bind(PROJECT_ID)
    .bind(PROJECT_READINESS_OPERATION)
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("readiness pre-state");
    assert_eq!(before, (1, "active".to_owned(), 0, 0, 0));

    arm_receipt_failpoint(
        &db,
        "readiness_receipt_failpoint",
        "readiness receipt failpoint",
    )
    .await;
    let failed = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_readiness(command.clone(), None)
        .await
        .expect_err("receipt failpoint aborts readiness request");
    assert!(failed.to_string().contains("failpoint"));
    drop_receipt_failpoint(&db, "readiness_receipt_failpoint").await;

    let after_failure: (i64, String, i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT version FROM project_milestone WHERE id = ?),
            (SELECT lifecycle FROM project_milestone WHERE id = ?),
            (SELECT COUNT(*) FROM project_readiness_snapshot WHERE project_id = ?),
            (SELECT COUNT(*) FROM command_receipt WHERE operation = ? AND scope_id = ?),
            (SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'milestone.readiness.evaluated')",
    )
    .bind(MILESTONE_ID)
    .bind(MILESTONE_ID)
    .bind(PROJECT_ID)
    .bind(PROJECT_READINESS_OPERATION)
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("readiness post-failure state");
    assert_eq!(after_failure, before);

    let first = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_readiness(command.clone(), None)
        .await
        .expect("readiness retry after receipt failpoint");
    assert_eq!(first.outcome, "ready");
    let state: (i64, String) =
        sqlx::query_as("SELECT version, lifecycle FROM project_milestone WHERE id = ?")
            .bind(MILESTONE_ID)
            .fetch_one(db.pool())
            .await
            .expect("readiness committed lifecycle");
    assert_eq!(state, (2, "ready_for_release".to_owned()));

    sqlx::query("UPDATE project_milestone SET version = 8, lifecycle = 'active' WHERE id = ?")
        .bind(MILESTONE_ID)
        .execute(db.pool())
        .await
        .expect("mutate readiness milestone after commit");
    let replay = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_readiness(command, None)
        .await
        .expect("readiness replay after service recreation");
    assert_eq!(replay, first);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_readiness_snapshot WHERE project_id = ?",
        )
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("readiness snapshot count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE scope_id = ? AND operation = ? AND idempotency_key = 'failpoint-readiness'",
        )
        .bind(PROJECT_ID)
        .bind(PROJECT_READINESS_OPERATION)
        .fetch_one(db.pool())
        .await
        .expect("readiness receipt count"),
        1
    );
}

#[tokio::test]
async fn release_request_receipt_failpoint_rolls_back_event_without_advancing_milestone() {
    let db = fixture().await;
    let readiness = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_readiness(readiness_command("release-setup-readiness"), None)
        .await
        .expect("release readiness setup");
    assert_eq!(readiness.outcome, "ready");
    let command = release_command("failpoint-release-request", &readiness);
    let before: (i64, String, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT version FROM project_milestone WHERE id = ?),
            (SELECT lifecycle FROM project_milestone WHERE id = ?),
            (SELECT COUNT(*) FROM command_receipt WHERE operation = ? AND scope_id = ?),
            (SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'project_release.candidate_requested')",
    )
    .bind(MILESTONE_ID)
    .bind(MILESTONE_ID)
    .bind(PROJECT_RELEASE_OPERATION)
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("release pre-state");
    assert_eq!(before, (2, "ready_for_release".to_owned(), 0, 0));

    arm_receipt_failpoint(
        &db,
        "release_request_receipt_failpoint",
        "release request receipt failpoint",
    )
    .await;
    let failed = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_release(command.clone(), None)
        .await
        .expect_err("receipt failpoint aborts release request");
    assert!(failed.to_string().contains("failpoint"));
    drop_receipt_failpoint(&db, "release_request_receipt_failpoint").await;

    let after_failure: (i64, String, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT version FROM project_milestone WHERE id = ?),
            (SELECT lifecycle FROM project_milestone WHERE id = ?),
            (SELECT COUNT(*) FROM command_receipt WHERE operation = ? AND scope_id = ?),
            (SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'project_release.candidate_requested')",
    )
    .bind(MILESTONE_ID)
    .bind(MILESTONE_ID)
    .bind(PROJECT_RELEASE_OPERATION)
    .bind(PROJECT_ID)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("release post-failure state");
    assert_eq!(after_failure, before);

    let first = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_release(command.clone(), None)
        .await
        .expect("release retry after receipt failpoint");
    assert_eq!(first.status, "pending_user_release_approval");
    assert_eq!(first.milestone_id, MILESTONE_ID);
    sqlx::query("UPDATE project_milestone SET version = 7 WHERE id = ?")
        .bind(MILESTONE_ID)
        .execute(db.pool())
        .await
        .expect("mutate release milestone after commit");
    let replay = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_release(command, None)
        .await
        .expect("release replay after service recreation");
    assert_eq!(replay, first);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE scope_id = ? AND operation = ?
               AND idempotency_key = 'failpoint-release-request'",
        )
        .bind(PROJECT_ID)
        .bind(PROJECT_RELEASE_OPERATION)
        .fetch_one(db.pool())
        .await
        .expect("release receipt count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'project_release.candidate_requested'",
        )
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("release event count"),
        1
    );
}
