//! Readiness freshness across project events, correction, and release.

use super::*;

#[tokio::test]
async fn readiness_freshness_rechecks_inputs_not_project_event_watermarks() {
    let db = fixture().await;
    let mut readiness_request = readiness_command("freshness-recheck");
    readiness_request.authorization.authorization_occurred_at = db::now_rfc3339();
    let readiness = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_readiness(readiness_request, None)
        .await
        .expect("readiness candidate persists");
    assert_eq!(readiness.outcome, "ready");

    let runtime = MilestoneRuntime::new(Arc::clone(&db));
    let fresh = runtime
        .readiness_freshness(PROJECT_ID, MILESTONE_ID, &readiness.id)
        .await
        .expect("fresh candidate rechecks");
    assert_eq!(
        fresh.status,
        api_types::ReadinessFreshnessStatus::Current,
        "the one-step readiness transition is accepted"
    );

    // Both a Project Agent release recommendation and an unrelated Project
    // event are attention/diagnostic records. Neither changes the governed
    // definition, baseline, policy, checks, evidence, waivers, Tasks,
    // Documents, or repository context.
    for (event_id, event_type) in [
        (
            "freshness-candidate-event",
            "project_release.candidate_requested",
        ),
        ("freshness-unrelated-event", "project.note.recorded"),
    ] {
        DomainEventRepo::append_event(
            &*db,
            CreateDomainEvent {
                id: event_id.to_owned(),
                event_type: event_type.to_owned(),
                entity_type: "project".to_owned(),
                entity_id: PROJECT_ID.to_owned(),
                actor_type: if event_type == "project_release.candidate_requested" {
                    "agent".to_owned()
                } else {
                    "user".to_owned()
                },
                actor_id: Some(if event_type == "project_release.candidate_requested" {
                    AGENT_ID.to_owned()
                } else {
                    USER_ID.to_owned()
                }),
                scope_type: "project".to_owned(),
                scope_id: PROJECT_ID.to_owned(),
                correlation_id: format!("correlation-{event_id}"),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: Some(format!("dedupe-{event_id}")),
                payload_json: "{}".to_owned(),
                created_at: NOW.to_owned(),
            },
        )
        .await
        .expect("diagnostic event appends");
    }
    let fresh_after_diagnostic_events = runtime
        .readiness_freshness(PROJECT_ID, MILESTONE_ID, &readiness.id)
        .await
        .expect("diagnostic events do not stale candidate");
    assert_eq!(
        fresh_after_diagnostic_events.status,
        api_types::ReadinessFreshnessStatus::Current
    );
    assert_ne!(
        fresh_after_diagnostic_events.snapshot_source_event_watermark,
        fresh_after_diagnostic_events.current_source_event_watermark,
        "watermarks remain diagnostic and may move independently"
    );

    // A second mutable milestone transition is an exact governed input
    // change and must invalidate the overlay even though the immutable
    // readiness row remains. (Definition/baseline revisions are immutable by
    // contract; the live milestone version is the canonical freshness CAS.)
    sqlx::query(
        "UPDATE project_milestone
         SET version = version + 1, updated_at = ?
         WHERE id = ?",
    )
    .bind(db::now_rfc3339())
    .bind(MILESTONE_ID)
    .execute(db.pool())
    .await
    .expect("mutate governed readiness input");
    let stale = runtime
        .readiness_freshness(PROJECT_ID, MILESTONE_ID, &readiness.id)
        .await
        .expect("changed candidate rechecks");
    assert_eq!(stale.status, api_types::ReadinessFreshnessStatus::Stale);
}

#[tokio::test]
async fn released_correction_freshness_accepts_terminal_version_without_increment() {
    let db = fixture().await;
    let mut initial_request = readiness_command("released-correction-initial");
    initial_request.authorization.authorization_occurred_at = db::now_rfc3339();
    ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_readiness(initial_request, None)
        .await
        .expect("initial readiness candidate persists");
    sqlx::query(
        "UPDATE project_milestone
         SET lifecycle = 'released'
         WHERE id = ? AND version = 2",
    )
    .bind(MILESTONE_ID)
    .execute(db.pool())
    .await
    .expect("promote fixture milestone to terminal release");

    let mut correction_request = readiness_command("released-correction");
    correction_request.expected_milestone_version = 2;
    correction_request.authorization.authorization_occurred_at = db::now_rfc3339();
    let correction = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_readiness(correction_request, None)
        .await
        .expect("released correction persists observational readiness");
    let version: i64 = sqlx::query_scalar(
        "SELECT version FROM project_milestone WHERE id = ? AND lifecycle = 'released'",
    )
    .bind(MILESTONE_ID)
    .fetch_one(db.pool())
    .await
    .expect("released milestone version");
    assert_eq!(version, correction.expected_milestone_version);

    let freshness = MilestoneRuntime::new(Arc::clone(&db))
        .readiness_freshness(PROJECT_ID, MILESTONE_ID, &correction.id)
        .await
        .expect("released correction freshness");
    assert_eq!(
        freshness.status,
        api_types::ReadinessFreshnessStatus::Current
    );
}

#[tokio::test]
async fn released_pinned_snapshot_stays_current_across_the_release_transition() {
    // The snapshot a release pins is two milestone versions old by the time
    // anyone reads it: readiness moved the milestone to `ready_for_release`
    // and `release()` moved it to `released`. The Overview must not call that
    // snapshot stale forever -- every UI release did until this was counted.
    let db = fixture().await;
    let mut readiness_request = readiness_command("release-pinned-readiness");
    readiness_request.authorization.authorization_occurred_at = db::now_rfc3339();
    let readiness = ProjectMilestoneCommandService::new(Arc::clone(&db))
        .request_readiness(readiness_request, None)
        .await
        .expect("readiness candidate persists");
    assert_eq!(readiness.outcome, "ready");
    assert_eq!(readiness.expected_milestone_version, 1);

    let releasing_user = PrincipalRef {
        kind: PrincipalKind::User,
        id: USER_ID.to_owned(),
        display_name: Some("Release approver".to_owned()),
    };
    let runtime = MilestoneRuntime::new(Arc::clone(&db));
    let release = runtime
        .release(
            PROJECT_ID,
            &releasing_user,
            &AuthorizationProvenance {
                principal: releasing_user.clone(),
                authorization_basis: "interactive_user_release".to_owned(),
                action: "project.milestone.release".to_owned(),
                event_id: "release-pinned-event".to_owned(),
                occurred_at: db::now_rfc3339(),
            },
            MILESTONE_ID,
            2,
            &readiness.id,
            &readiness.readiness_digest,
            "release-pinned-key",
        )
        .await
        .expect("the ready milestone releases");
    assert_eq!(release.snapshot.readiness_snapshot_id, readiness.id);
    let state: (i64, String) =
        sqlx::query_as("SELECT version, lifecycle FROM project_milestone WHERE id = ?")
            .bind(MILESTONE_ID)
            .fetch_one(db.pool())
            .await
            .expect("released milestone state");
    assert_eq!(state, (3, "released".to_owned()));

    let freshness = runtime
        .readiness_freshness(PROJECT_ID, MILESTONE_ID, &readiness.id)
        .await
        .expect("pinned snapshot freshness");
    assert_eq!(
        freshness.status,
        api_types::ReadinessFreshnessStatus::Current,
        "{:?}",
        freshness.reason
    );
}
