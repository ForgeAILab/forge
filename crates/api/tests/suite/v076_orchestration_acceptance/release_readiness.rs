//! Milestone readiness, release scope, and concurrent mutations.

use super::*;

#[tokio::test]
async fn v076_ready_milestone_releases_once_and_rejects_cross_project_scope() {
    let workspace = common::TestDir::new("v076-readiness-release");
    let harness = common::test_app(workspace.path(), "v076-readiness-release").await;
    let app = &harness.app;
    let token = common::test_jwt();
    let fixture = create_genesis_project(app, &token, "v076-release").await;
    record_passed_check(app, &harness, &token, &fixture).await;
    let current_milestone = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/milestones/{}",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;

    let readiness = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/readiness",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        json!({
            "mutation": {
                "expected_version": current_milestone["version"],
                "idempotency_key": "v076-readiness",
                "authorization": user_authorization("project.milestone.readiness", "v076-readiness-event")
            },
            "milestone_id": fixture.milestone_id
        }),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(readiness["result"], json!("ready"));
    let snapshot_id = required_string(&readiness, &["id"]);
    let readiness_digest = required_string(&readiness, &["readiness_digest"]);
    let ready_milestone = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/milestones/{}",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(ready_milestone["lifecycle"], json!("ready_for_release"));
    let ready_version = ready_milestone["version"]
        .as_i64()
        .expect("ready milestone version");

    // The Project Agent may only submit a release candidate. This audited
    // attention/projection must not change the exact readiness inputs that a
    // user release re-authorizes in the transaction below.
    let candidate = services::ProjectMilestoneCommandService::new(harness.state.db.clone())
        .request_release(
            services::ProjectReleaseRequestCommand {
                project_id: fixture.project_id.clone(),
                milestone_id: fixture.milestone_id.clone(),
                expected_milestone_version: ready_version,
                readiness_snapshot_id: snapshot_id.clone(),
                readiness_digest: readiness_digest.clone(),
                status: "pending_user_release_approval".to_owned(),
                idempotency_key: "v076-release-candidate".to_owned(),
                authorization: services::ProjectCommandAuthorization {
                    principal_type: "agent".to_owned(),
                    principal_id: fixture.project_identity_id.clone(),
                    policy_result: "allowed".to_owned(),
                    policy_revision: Some("project-agent-policy@1".to_owned()),
                    policy_digest: Some("project-agent-policy-digest".to_owned()),
                    requested_permission: Some("propose_project".to_owned()),
                    correlation_id: "v076-release-candidate-correlation".to_owned(),
                    causation_id: None,
                    causation_depth: 0,
                    authorization_event_id: "v076-release-candidate-event".to_owned(),
                    authorization_basis: "bound Project Agent authorization".to_owned(),
                    authorization_action: "project.milestone.release.request".to_owned(),
                    authorization_occurred_at: Utc::now().to_rfc3339(),
                    authorization_json: json!({
                        "principal": {
                            "kind": "agent",
                            "id": fixture.project_identity_id
                        },
                        "action": "project.milestone.release.request"
                    })
                    .to_string(),
                },
            },
            None,
        )
        .await
        .expect("Project Agent release candidate is admitted");
    assert_eq!(candidate.status, "pending_user_release_approval");
    assert!(!candidate.event_id.is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'project_release.candidate_requested'",
        )
        .bind(&fixture.project_id)
        .fetch_one(harness.state.db.pool())
        .await
        .expect("candidate request event count"),
        1
    );

    let mut release_body = json!({
        "mutation": {
            "expected_version": ready_version,
            "idempotency_key": "v076-release",
            "authorization": user_authorization("project.milestone.release", "v076-release-event")
        },
        "milestone_id": fixture.milestone_id,
        "readiness_snapshot_id": snapshot_id,
        "readiness_digest": readiness_digest
    });
    // The web client sends the signed-in user's display name inside the
    // authorization receipt; the immutable snapshot must round-trip it.
    release_body["mutation"]["authorization"]["principal"]["display_name"] =
        json!("Release approver");
    let release = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/release",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        release_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(release["release_identity"], json!("M001-r1"));
    let replay = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/release",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        release_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(replay, release);

    // A stale dialog can submit the same immutable readiness target with a
    // fresh UI key after the first release committed. Return the existing
    // release instead of recomputing against the released lifecycle.
    let mut fresh_key_release = release_body.clone();
    fresh_key_release["mutation"]["idempotency_key"] = json!("v076-release-fresh-ui-key");
    fresh_key_release["mutation"]["authorization"] =
        user_authorization("project.milestone.release", "v076-release-fresh-ui-event");
    let semantic_replay = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/release",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        fresh_key_release,
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(semantic_replay, release);
    let primary_milestone_id: Option<String> =
        sqlx::query_scalar("SELECT primary_milestone_id FROM project WHERE id = ?")
            .bind(&fixture.project_id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("released Project primary milestone");
    assert_eq!(
        primary_milestone_id.as_deref(),
        Some(fixture.milestone_id.as_str())
    );
    let overview = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}/overview", fixture.project_id),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    assert!(overview["active_milestones"]
        .as_array()
        .is_some_and(|milestones| milestones.iter().any(|entry| {
            entry["milestone"]["id"] == fixture.milestone_id
                && entry["milestone"]["lifecycle"] == "released"
        })));

    // The release idempotency key is bound to the complete candidate. A
    // replay after the milestone has moved to `released` is exact, while the
    // same key with a changed readiness digest is a conflict.
    let mut altered_release = release_body;
    altered_release["readiness_digest"] = json!("v076-altered-readiness-digest");
    let altered = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/release",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        altered_release,
        &[StatusCode::CONFLICT],
    )
    .await;
    assert!(altered.get("message").is_some());

    // Inspect the public immutable release projection as well as the
    // canonical row. The row must retain the exact readiness and closed
    // release-policy references used by the release transaction.
    let release_id = required_string(&release, &["id"]);
    let inspected = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/releases/{release_id}",
            fixture.project_id
        ),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(inspected, release);
    assert_eq!(
        inspected["snapshot"]["released_by"]["display_name"],
        json!("Release approver")
    );
    assert_eq!(
        inspected["snapshot"]["authorization"]["principal"]["display_name"],
        json!("Release approver")
    );
    assert_eq!(
        inspected["snapshot"]["readiness_snapshot_id"],
        json!(snapshot_id)
    );
    assert_eq!(
        inspected["snapshot"]["readiness_digest"],
        json!(readiness_digest)
    );
    assert!(!required_string(&inspected["snapshot"], &["snapshot_digest"]).is_empty());

    let release_row = sqlx::query(
        "SELECT project_id, milestone_id, release_sequence, release_revision,
                release_identifier, readiness_snapshot_id, readiness_digest,
                releasing_principal_type, releasing_principal_id,
                authorization_basis, explicit_event, schema_version,
                snapshot_digest, idempotency_key
         FROM project_release WHERE id = ?",
    )
    .bind(&release_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("immutable release row");
    assert_eq!(
        release_row.get::<String, _>("project_id"),
        fixture.project_id
    );
    assert_eq!(
        release_row.get::<String, _>("milestone_id"),
        fixture.milestone_id
    );
    assert_eq!(release_row.get::<i64, _>("release_sequence"), 1);
    assert_eq!(release_row.get::<i64, _>("release_revision"), 1);
    assert_eq!(
        release_row.get::<String, _>("release_identifier"),
        "M001-r1"
    );
    assert_eq!(
        release_row.get::<String, _>("readiness_snapshot_id"),
        snapshot_id
    );
    assert_eq!(
        release_row.get::<String, _>("readiness_digest"),
        readiness_digest
    );
    assert_eq!(
        release_row.get::<String, _>("releasing_principal_type"),
        "user"
    );
    assert_eq!(
        release_row.get::<String, _>("releasing_principal_id"),
        "test-user-id"
    );
    assert_eq!(
        release_row.get::<String, _>("authorization_basis"),
        "explicit_user_authorization"
    );
    assert_eq!(
        release_row.get::<String, _>("explicit_event"),
        "v076-release-event"
    );
    assert_eq!(
        release_row.get::<String, _>("idempotency_key"),
        "v076-release"
    );
    assert!(!release_row.get::<String, _>("schema_version").is_empty());
    assert_eq!(
        release_row.get::<String, _>("snapshot_digest"),
        inspected["snapshot"]["snapshot_digest"]
            .as_str()
            .expect("release snapshot digest")
    );
    let release_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_release WHERE project_id = ? AND milestone_id = ?",
    )
    .bind(&fixture.project_id)
    .bind(&fixture.milestone_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("one release row");
    assert_eq!(release_count, 1);

    // A different Project cannot use this milestone identity, even though
    // the same authenticated user owns both Projects.
    let other_project = request_json(
        app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({"name": "V076 unrelated project"}),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await;
    let other_project_id = required_string(&other_project, &["id"]);
    let cross_scope = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/release",
            other_project_id, fixture.milestone_id
        ),
        &token,
        json!({
            "mutation": {
                "expected_version": ready_version,
                "idempotency_key": "v076-cross-project-release",
                "authorization": user_authorization("project.milestone.release", "v076-cross-project-release-event")
            },
            "milestone_id": fixture.milestone_id,
            "readiness_snapshot_id": snapshot_id,
            "readiness_digest": readiness_digest
        }),
        &[StatusCode::NOT_FOUND, StatusCode::CONFLICT],
    )
    .await;
    assert!(cross_scope.get("message").is_some());
}

#[tokio::test]
async fn v076_relevant_post_readiness_mutation_conflicts_with_release() {
    let workspace = common::TestDir::new("v076-readiness-mutation-conflict");
    let harness = common::test_app(workspace.path(), "v076-readiness-mutation-conflict").await;
    let app = &harness.app;
    let token = common::test_jwt();
    let fixture = create_genesis_project(app, &token, "v076-readiness-mutation").await;
    record_passed_check(app, &harness, &token, &fixture).await;

    let current_milestone = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/milestones/{}",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let readiness = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/readiness",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        json!({
            "mutation": {
                "expected_version": current_milestone["version"],
                "idempotency_key": "v076-readiness-mutation-readiness",
                "authorization": user_authorization(
                    "project.milestone.readiness",
                    "v076-readiness-mutation-readiness-event"
                )
            },
            "milestone_id": fixture.milestone_id
        }),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(readiness["result"], json!("ready"));
    let snapshot_id = required_string(&readiness, &["id"]);
    let readiness_digest = required_string(&readiness, &["readiness_digest"]);
    let ready_milestone = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/milestones/{}",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let ready_version = ready_milestone["version"]
        .as_i64()
        .expect("ready milestone version");
    // A newer validation result is a governed readiness input. Unlike a
    // release-candidate attention event, it must invalidate the immutable
    // readiness candidate and force a fresh evaluation before release.
    let check = sqlx::query(
        "SELECT id, definition_revision_id, version FROM project_milestone_check
         WHERE project_id = ? AND milestone_id = ? ORDER BY id LIMIT 1",
    )
    .bind(&fixture.project_id)
    .bind(&fixture.milestone_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("current acceptance check");
    let check_id: String = check.get("id");
    let definition_revision_id: String = check.get("definition_revision_id");
    let check_version: i64 = check.get("version");
    let replacement_result = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/checks/{check_id}/result",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        json!({
            "mutation": {
                "expected_version": check_version,
                "idempotency_key": "v076-readiness-mutation-check",
                "authorization": user_authorization(
                    "project.milestone.check.record",
                    "v076-readiness-mutation-check-event"
                )
            },
            "check_id": check_id,
            "definition_revision_id": definition_revision_id,
            "status": "pass",
            "result": "passed after readiness with new authoritative input",
            "input_digest": "v076-check-input-after-readiness",
            "governing_revision_ids": [fixture.charter_revision_id]
        }),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(replacement_result["status"], json!("pass"));

    let conflict = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/release",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        json!({
            "mutation": {
                "expected_version": ready_version,
                "idempotency_key": "v076-readiness-mutation-release",
                "authorization": user_authorization(
                    "project.milestone.release",
                    "v076-readiness-mutation-release-event"
                )
            },
            "milestone_id": fixture.milestone_id,
            "readiness_snapshot_id": snapshot_id,
            "readiness_digest": readiness_digest
        }),
        &[StatusCode::CONFLICT],
    )
    .await;
    assert!(conflict.get("message").is_some());
}
