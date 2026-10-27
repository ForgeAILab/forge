use super::*;

async fn milestone(db: &SqliteDb, id: &str, sequence: i64, lifecycle: &str) {
    sqlx::query("INSERT INTO project_milestone (id, project_id, milestone_sequence, milestone_key, lifecycle, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
        .bind(id).bind(PROJECT_ID).bind(sequence).bind(format!("M{sequence:03}"))
        .bind(lifecycle).bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
}

async fn revision(db: &SqliteDb, id: &str, milestone: &str, number: i64, lifecycle: &str) {
    sqlx::query("INSERT INTO project_milestone_revision (id, milestone_id, revision, lifecycle, outcome, schema_version, render_version, rendered_view, content_digest, rendered_digest, author_type, author_id, created_at) VALUES (?, ?, ?, ?, 'Bounded outcome', 'test', 'test', 'Outcome', 'content', 'render', 'user', 'author', ?)")
        .bind(id).bind(milestone).bind(number).bind(lifecycle).bind(NOW).execute(db.pool()).await.unwrap();
}

fn check_input(revision: &str) -> db::MaterializeMilestoneCheckInput<'_> {
    db::MaterializeMilestoneCheckInput {
        project_id: PROJECT_ID,
        milestone_id: "milestone",
        definition_revision_id: revision,
        check_id: "check",
        description: "Verify outcome",
        required: false,
        source_kind: "manual",
        expected_result: "passed",
        evidence_required: false,
        updated_at: NOW,
    }
}

async fn check_fixture(db: &SqliteDb) {
    project(db).await;
    milestone(db, "milestone", 1, "active").await;
    revision(db, "revision", "milestone", 1, "proposed").await;
    sqlx::query("UPDATE project_milestone SET current_definition_revision_id = 'revision' WHERE id = 'milestone'")
        .execute(db.pool()).await.unwrap();
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    db.materialize_milestone_definition_check_in_tx(&mut tx, check_input("revision"))
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn id_pages_keep_scope_keyset_order_and_the_undecoded_lookahead() {
    let db = database().await;
    project(&db).await;
    for (id, sequence) in [("a", 1), ("b", 2), ("c", 3)] {
        milestone(&db, id, sequence, "planned").await;
    }
    let page = db
        .project_milestone_id_page(PROJECT_ID, None, 1)
        .await
        .unwrap();
    assert_eq!(page.len(), 2);
    assert_eq!(page[0].id().unwrap(), "a");
    assert_eq!(page[1].id().unwrap(), "b");
    let page = db
        .project_milestone_id_page(PROJECT_ID, Some((2, "b".into())), 1)
        .await
        .unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].id().unwrap(), "c");
    assert!(db
        .project_milestone_id_page("other-project", None, 1)
        .await
        .unwrap()
        .is_empty());
    revision(&db, "r1", "a", 1, "draft").await;
    revision(&db, "r2", "a", 2, "draft").await;
    revision(&db, "r3", "a", 3, "draft").await;
    let page = db
        .project_milestone_revision_id_page(PROJECT_ID, "a", None, 1)
        .await
        .unwrap();
    assert_eq!(page.len(), 2);
    assert_eq!(page[0].id().unwrap(), "r1");
    assert_eq!(page[1].id().unwrap(), "r2");
    let page = db
        .project_milestone_revision_id_page(PROJECT_ID, "a", Some((2, "r2".into())), 1)
        .await
        .unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].id().unwrap(), "r3");
    assert!(db
        .project_milestone_revision_id_page("other-project", "a", None, 1)
        .await
        .unwrap()
        .is_empty());
    sqlx::query("INSERT INTO project_milestone (id, project_id, milestone_sequence, milestone_key, created_at, updated_at) VALUES (CAST('broken' AS BLOB), ?, 4, 'M004', ?, ?)")
        .bind(PROJECT_ID).bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
    let page = db
        .project_milestone_id_page(PROJECT_ID, Some((2, "b".into())), 1)
        .await
        .unwrap();
    assert_eq!(page.len(), 2);
    assert_eq!(page[0].id().unwrap(), "c");
    assert!(matches!(page[1].id(), Err(DbError::Sqlx(_))));
}

#[tokio::test]
async fn lifecycle_cas_and_primary_repair_share_the_callers_transaction() {
    let db = database().await;
    project(&db).await;
    milestone(&db, "milestone", 1, "active").await;
    milestone(&db, "replacement", 2, "active").await;
    milestone(&db, "later", 3, "active").await;
    sqlx::query("UPDATE project SET primary_milestone_id = 'milestone' WHERE id = ?")
        .bind(PROJECT_ID)
        .execute(db.pool())
        .await
        .unwrap();
    let input = |expected_version| db::TransitionMilestoneLifecycleInput {
        project_id: PROJECT_ID,
        milestone_id: "milestone",
        expected_version,
        from_lifecycle: "active",
        lifecycle: "cancelled",
        updated_at: NOW,
    };
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    assert_eq!(
        db.lock_milestone_version_in_tx(&mut tx, "milestone", PROJECT_ID, 1)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        db.lock_milestone_version_in_tx(&mut tx, "milestone", PROJECT_ID, 2)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        db.lock_milestone_version_in_tx(&mut tx, "milestone", "other-project", 1)
            .await
            .unwrap(),
        0
    );
    assert!(matches!(
        db.transition_milestone_lifecycle_in_tx(&mut tx, input(2))
            .await,
        Err(DbError::VersionConflict)
    ));
    tx.rollback().await.unwrap();
    sqlx::query("CREATE TRIGGER fail_primary BEFORE UPDATE OF primary_milestone_id ON project BEGIN SELECT RAISE(ABORT, 'injected pointer failure'); END")
        .execute(db.pool()).await.unwrap();
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    assert!(matches!(
        db.transition_milestone_lifecycle_in_tx(&mut tx, input(1))
            .await,
        Err(DbError::Sqlx(_))
    ));
    tx.rollback().await.unwrap();
    let row: (String, i64) =
        sqlx::query_as("SELECT lifecycle, version FROM project_milestone WHERE id = 'milestone'")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(row, ("active".into(), 1));
    sqlx::query("DROP TRIGGER fail_primary")
        .execute(db.pool())
        .await
        .unwrap();
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    db.transition_milestone_lifecycle_in_tx(&mut tx, input(1))
        .await
        .unwrap();
    assert_eq!(
        db.primary_milestone_id_in_tx(&mut tx, PROJECT_ID)
            .await
            .unwrap()
            .as_deref(),
        Some("replacement")
    );
    tx.commit().await.unwrap();
    let project = ProjectRepo::get_by_id(&db, PROJECT_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(project.primary_milestone_id.as_deref(), Some("replacement"));
    assert_eq!(project.version, 2);
    // Every lifecycle retained by the original repair guard keeps the pointer and Project version.
    for lifecycle in ["planned", "active", "ready_for_release", "released"] {
        sqlx::query("UPDATE project_milestone SET lifecycle = ? WHERE id = 'replacement'")
            .bind(lifecycle)
            .execute(db.pool())
            .await
            .unwrap();
        let mut tx = db::begin_immediate(db.pool()).await.unwrap();
        db.repair_milestone_primary_pointer_in_tx(&mut tx, PROJECT_ID, "replacement")
            .await
            .unwrap();
        db.repair_milestone_primary_pointer_in_tx(&mut tx, PROJECT_ID, "later")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(
            ProjectRepo::get_by_id(&db, PROJECT_ID)
                .await
                .unwrap()
                .unwrap()
                .version,
            2
        );
    }
}

#[tokio::test]
async fn definition_materialization_keeps_scope_noop_supersession_and_cas() {
    let db = database().await;
    check_fixture(&db).await;
    revision(&db, "next", "milestone", 2, "proposed").await;
    sqlx::query(
        "UPDATE project_milestone_revision SET lifecycle = 'approved' WHERE id = 'revision'",
    )
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE project_milestone_check SET current_result_id = 'old-result' WHERE id = 'check'",
    )
    .execute(db.pool())
    .await
    .unwrap();
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    assert!(db
        .milestone_definition_transition_source_in_tx(&mut tx, "next", "milestone", "other-project")
        .await
        .unwrap()
        .is_none());
    let source = db
        .milestone_definition_transition_source_in_tx(&mut tx, "next", "milestone", PROJECT_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source.lifecycle().unwrap(), "proposed");
    assert_eq!(source.acceptance_checks_json().unwrap(), "[]");
    assert_eq!(source.evidence_requirements_json().unwrap(), "[]");
    assert_eq!(
        db.transition_milestone_definition_lifecycle_in_tx(
            &mut tx,
            "next",
            "milestone",
            "draft",
            "approved"
        )
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        db.transition_milestone_definition_lifecycle_in_tx(
            &mut tx,
            "next",
            "milestone",
            "proposed",
            "approved"
        )
        .await
        .unwrap(),
        1
    );
    db.materialize_milestone_definition_check_in_tx(&mut tx, check_input("next"))
        .await
        .unwrap();
    db.materialize_milestone_definition_check_in_tx(&mut tx, check_input("next"))
        .await
        .unwrap();
    let mut cross_scope = check_input("next");
    cross_scope.project_id = "other-project";
    assert_eq!(
        db.materialize_milestone_definition_check_in_tx(&mut tx, cross_scope)
            .await
            .unwrap(),
        db::MaterializeMilestoneCheckOutcome::ScopeMismatch
    );
    assert_eq!(
        db.advance_milestone_definition_in_tx(
            &mut tx,
            PROJECT_ID,
            "milestone",
            "next",
            1,
            "approved",
            NOW
        )
        .await
        .unwrap(),
        1
    );
    tx.commit().await.unwrap();
    let check: (String, i64, Option<String>) = sqlx::query_as("SELECT definition_revision_id, version, current_result_id FROM project_milestone_check WHERE id = 'check'").fetch_one(db.pool()).await.unwrap();
    assert_eq!(check, ("next".into(), 2, None));
    let previous: String = sqlx::query_scalar(
        "SELECT lifecycle FROM project_milestone_revision WHERE id = 'revision'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(previous, "superseded");
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    assert_eq!(
        db.advance_milestone_definition_in_tx(
            &mut tx,
            PROJECT_ID,
            "milestone",
            "revision",
            1,
            "proposed",
            NOW
        )
        .await
        .unwrap(),
        0
    );
    tx.rollback().await.unwrap();
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    assert_eq!(
        db.advance_milestone_definition_in_tx(
            &mut tx,
            PROJECT_ID,
            "milestone",
            "next",
            2,
            "superseded",
            NOW
        )
        .await
        .unwrap(),
        1
    );
    tx.commit().await.unwrap();
    let milestone: (i64, String) = sqlx::query_as("SELECT version, current_definition_revision_id FROM project_milestone WHERE id = 'milestone'").fetch_one(db.pool()).await.unwrap();
    assert_eq!(milestone, (3, "next".into()));
}

fn manual_result() -> db::CreateProjectMilestoneCheckResult {
    db::CreateProjectMilestoneCheckResult {
        id: "result".into(),
        project_id: PROJECT_ID.into(),
        milestone_id: "milestone".into(),
        check_id: "check".into(),
        definition_revision_id: "revision".into(),
        outcome: "passed".into(),
        source_kind: "manual".into(),
        source_manifest_json: "{\"result\":\"Observed outcome\"}".into(),
        input_digest: "input".into(),
        governing_charter_revision_id: None,
        principal_type: "user".into(),
        principal_id: "user-1".into(),
        authorization_basis: "explicit".into(),
        authorization_action: "project.milestone.check.record".into(),
        authorization_occurred_at: NOW.into(),
        expected_version: 1,
        explicit_event: "user-event".into(),
        idempotency_key: "manual-result".into(),
        created_at: NOW.into(),
    }
}

#[tokio::test]
async fn manual_result_targets_cas_replay_and_unique_errors_keep_their_contract() {
    let db = database().await;
    check_fixture(&db).await;
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    let source = db
        .milestone_manual_check_target_in_tx(&mut tx, "check", PROJECT_ID, "milestone")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source.version().unwrap(), 1);
    assert_eq!(source.definition_revision_id().unwrap(), "revision");
    assert_eq!(source.source_kind().unwrap(), "manual");
    assert_eq!(
        source.current_definition_revision_id().unwrap().as_deref(),
        Some("revision")
    );
    assert_eq!(source.author_id().unwrap().as_deref(), Some("author"));
    assert!(db
        .milestone_manual_check_target_in_tx(&mut tx, "check", "other-project", "milestone")
        .await
        .unwrap()
        .is_none());
    assert!(db
        .milestone_waiver_check_target_in_tx(
            &mut tx,
            "check",
            PROJECT_ID,
            "milestone",
            "revision",
            1
        )
        .await
        .unwrap()
        .is_some());
    assert!(db
        .milestone_waiver_check_target_in_tx(
            &mut tx,
            "check",
            PROJECT_ID,
            "milestone",
            "revision",
            2
        )
        .await
        .unwrap()
        .is_none());
    assert!(db
        .milestone_waiver_check_target_in_tx(
            &mut tx,
            "check",
            "other-project",
            "milestone",
            "revision",
            1
        )
        .await
        .unwrap()
        .is_none());
    assert!(db
        .milestone_waiver_check_target_in_tx(
            &mut tx,
            "check",
            PROJECT_ID,
            "milestone",
            "other-revision",
            1
        )
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        db.milestone_check_charter_in_tx(&mut tx, PROJECT_ID)
            .await
            .unwrap(),
        Some(None)
    );
    assert_eq!(
        db.milestone_check_charter_in_tx(&mut tx, "other-project")
            .await
            .unwrap(),
        None
    );
    let mut stale = manual_result();
    stale.expected_version = 2;
    assert!(matches!(
        db.record_manual_milestone_result_in_tx(&mut tx, &stale, 2)
            .await,
        Err(DbError::VersionConflict)
    ));
    tx.rollback().await.unwrap();
    assert!(db
        .milestone_manual_result_replay("manual-result")
        .await
        .unwrap()
        .is_none());
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    db.record_manual_milestone_result_in_tx(&mut tx, &manual_result(), 1)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let replay = db
        .milestone_manual_result_replay("manual-result")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replay.id().unwrap(), "result");
    assert_eq!(replay.project_id().unwrap(), PROJECT_ID);
    assert_eq!(replay.milestone_id().unwrap(), "milestone");
    assert_eq!(replay.check_id().unwrap(), "check");
    assert_eq!(replay.definition_revision_id().unwrap(), "revision");
    assert_eq!(replay.source_kind().unwrap(), "manual");
    assert_eq!(replay.outcome().unwrap(), "passed");
    assert_eq!(
        replay.source_manifest_json().unwrap(),
        manual_result().source_manifest_json
    );
    assert_eq!(replay.governing_charter_revision_id().unwrap(), None);
    assert_eq!(replay.principal_type().unwrap(), "user");
    assert_eq!(replay.principal_id().unwrap(), "user-1");
    assert_eq!(replay.input_digest().unwrap(), "input");
    assert_eq!(replay.expected_version().unwrap(), 1);
    assert_eq!(replay.authorization_basis().unwrap(), "explicit");
    assert_eq!(
        replay.authorization_action().unwrap(),
        "project.milestone.check.record"
    );
    assert_eq!(replay.explicit_event().unwrap(), "user-event");
    assert_eq!(replay.authorization_occurred_at().unwrap(), NOW);
    assert_eq!(replay.created_at().unwrap(), NOW);
    let mut duplicate = manual_result();
    duplicate.id = "duplicate".into();
    duplicate.expected_version = 2;
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    assert!(matches!(
        db.record_manual_milestone_result_in_tx(&mut tx, &duplicate, 2)
            .await,
        Err(DbError::IdempotencyConflict)
    ));
    tx.rollback().await.unwrap();
    let check: (i64, String) = sqlx::query_as(
        "SELECT version, current_result_id FROM project_milestone_check WHERE id = 'check'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(check, (2, "result".into()));
}

#[tokio::test]
async fn manual_result_cas_uses_the_explicit_check_version() {
    let db = database().await;
    check_fixture(&db).await;
    let mut result = manual_result();
    result.expected_version = 7;
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    db.record_manual_milestone_result_in_tx(&mut tx, &result, 1)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let check: (i64, String) = sqlx::query_as(
        "SELECT version, current_result_id FROM project_milestone_check WHERE id = 'check'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(check, (2, "result".into()));
    assert_eq!(
        db.milestone_manual_result_replay("manual-result")
            .await
            .unwrap()
            .unwrap()
            .expected_version()
            .unwrap(),
        7
    );
}

#[tokio::test]
async fn waiver_decision_insert_and_existing_scoped_read_share_rollback() {
    let db = database().await;
    project(&db).await;
    sqlx::query("INSERT INTO project_charter (id, account_id, project_id, project_mode, maturity, lifecycle, created_at, updated_at) VALUES ('charter', 'user-1', ?, 'compact', 'mvp', 'attached', ?, ?)")
        .bind(PROJECT_ID).bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO project_charter_revision (id, charter_id, revision, lifecycle, schema_version, render_version, content_json, rendered_view, author_type, author_id, content_digest, rendered_digest, created_at) VALUES ('charter-revision', 'charter', 1, 'approved', 'test', 'test', '{}', 'Charter', 'user', 'user-1', 'content', 'render', ?)")
        .bind(NOW).execute(db.pool()).await.unwrap();
    let input = || db::InsertMilestoneWaiverDecisionInput {
        waiver_id: "waiver",
        project_id: PROJECT_ID,
        check_id: "check",
        reason: "Exception",
        user_id: "user-1",
        authorization_basis: "explicit",
        authorization_action: "project.milestone.check.waive",
        explicit_event: "waiver-event",
        authorization_occurred_at: NOW,
        charter_revision_id: "charter-revision",
        affected_records_json: "{\"check_id\":\"check\"}",
        created_at: NOW,
    };
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    db.insert_milestone_waiver_decision_in_tx(&mut tx, input())
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(db
        .project_decision_command_source("waiver", PROJECT_ID)
        .await
        .unwrap()
        .is_none());
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    db.insert_milestone_waiver_decision_in_tx(&mut tx, input())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(db
        .project_decision_command_source("waiver", "other-project")
        .await
        .unwrap()
        .is_none());
    let decision = db
        .project_decision_command_source("waiver", PROJECT_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(decision.state().unwrap(), "active");
    assert_eq!(decision.decision_class().unwrap(), "waiver");
    assert_eq!(decision.question().unwrap(), "Waive milestone check check");
    assert_eq!(decision.selected_outcome().unwrap(), "waived");
    assert_eq!(decision.rationale().unwrap(), "Exception");
    assert_eq!(decision.principal_type().unwrap(), "user");
    assert_eq!(decision.principal_id().unwrap(), "user-1");
    assert_eq!(decision.authority_basis().unwrap(), "explicit");
    assert_eq!(
        decision.authorization_action().unwrap(),
        "project.milestone.check.waive"
    );
    assert_eq!(decision.explicit_event().unwrap(), "waiver-event");
    assert_eq!(decision.authorization_occurred_at().unwrap(), NOW);
    assert_eq!(
        decision.required_charter_revision_id().unwrap(),
        "charter-revision"
    );
    assert_eq!(
        decision.affected_records_json().unwrap(),
        "{\"check_id\":\"check\"}"
    );
    assert_eq!(decision.created_at().unwrap(), NOW);
}
