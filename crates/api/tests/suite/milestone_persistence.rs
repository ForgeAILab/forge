use crate::common;
use api_types::ProjectResponse;
use axum::http::{Method, StatusCode};
use serde_json::{json, Value};

async fn fixture() -> (tempfile::TempDir, common::Harness, String) {
    let root = tempfile::tempdir().unwrap();
    let harness = common::test_app(root.path(), "milestone-persistence").await;
    harness.stop_step_worker();
    let project: ProjectResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({"name": "Milestone persistence"}),
        StatusCode::OK,
    )
    .await;
    (root, harness, project.id)
}

fn mutation(action: &str, key: &str, version: i64) -> Value {
    json!({
        "expected_version": version,
        "idempotency_key": key,
        "authorization": {
            "principal": {"kind": "user", "id": "test-user-id"},
            "authorization_basis": "explicit_user_action",
            "action": action,
            "event_id": format!("event-{key}"),
            "occurred_at": db::now_rfc3339(),
        },
    })
}

fn check_request(waive: bool) -> Value {
    let action = if waive { "waive" } else { "record" };
    let mut request = json!({
        "mutation": mutation(&format!("project.milestone.check.{action}"), action, 1),
        "check_id": "check", "definition_revision_id": "revision",
        "input_digest": "input", "governing_revision_ids": ["charter-revision"],
    });
    if waive {
        request["reason"] = json!("User-approved exception");
    } else {
        request["status"] = json!("pass");
        request["result"] = json!("Observed the result");
    }
    request
}

fn assert_database_error(body: Value) {
    assert!(!body["request_id"].as_str().unwrap().is_empty());
    assert_eq!(
        body,
        json!({
            "code": "internal_error", "message": "Project artifact mutation failed",
            "details": null, "request_id": body["request_id"],
        })
    );
}

async fn seed_milestone(harness: &common::Harness, project_id: &str, checks: bool) {
    let pool = harness.state.db.pool();
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO project_milestone (id, project_id, milestone_sequence, milestone_key, display_label, lifecycle, created_at, updated_at) VALUES ('milestone', ?, 1, 'M001', 'Bounded outcome', 'planned', ?, ?)")
        .bind(project_id).bind(&now).bind(&now).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO project_milestone_revision (id, milestone_id, revision, lifecycle, display_label, outcome, acceptance_checks_json, schema_version, render_version, rendered_view, content_digest, rendered_digest, author_type, author_id, created_at) VALUES ('revision', 'milestone', 1, ?, 'Bounded outcome', 'Bounded outcome', ?, 'test', 'test', 'Outcome', 'content', 'render', 'user', 'definition-author', ?)")
        .bind(if checks { "proposed" } else { "draft" })
        .bind(json!([{"id": "check", "description": "Verify outcome", "required": false, "source_kind": "manual", "expected_result": "passed"}]).to_string())
        .bind(&now).execute(pool).await.unwrap();
    if !checks {
        return;
    }
    sqlx::query("UPDATE project_milestone SET current_definition_revision_id = 'revision' WHERE id = 'milestone'")
        .execute(pool).await.unwrap();
    sqlx::query("INSERT INTO project_milestone_check (id, project_id, milestone_id, definition_revision_id, check_key, description, required, source_kind, expected_result, evidence_required, created_at, updated_at) VALUES ('check', ?, 'milestone', 'revision', 'check', 'Verify outcome', 0, 'manual', 'passed', 0, ?, ?)")
        .bind(project_id).bind(&now).bind(&now).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO project_charter (id, account_id, project_id, project_mode, maturity, lifecycle, created_at, updated_at) VALUES ('charter', 'test-user-id', ?, 'compact', 'mvp', 'attached', ?, ?)")
        .bind(project_id).bind(&now).bind(&now).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO project_charter_revision (id, charter_id, revision, lifecycle, schema_version, render_version, content_json, rendered_view, author_type, author_id, content_digest, rendered_digest, created_at) VALUES ('charter-revision', 'charter', 1, 'approved', 'test', 'test', '{}', 'Charter', 'user', 'test-user-id', 'content', 'render', ?)")
        .bind(&now).execute(pool).await.unwrap();
    sqlx::query("UPDATE project_charter SET current_approved_revision_id = 'charter-revision' WHERE id = 'charter'")
        .execute(pool).await.unwrap();
    sqlx::query("UPDATE project SET current_charter_id = 'charter', current_charter_revision_id = 'charter-revision' WHERE id = ?")
        .bind(project_id).execute(pool).await.unwrap();
}

#[tokio::test]
async fn milestone_database_errors_preserve_body() {
    let (_root, harness, project) = fixture().await;
    sqlx::query("DROP TABLE project_milestone")
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    for suffix in [
        "milestones".to_owned(),
        format!("milestones?cursor={}", hex::encode("1\0milestone")),
    ] {
        assert_database_error(
            common::empty_request(
                &harness.app,
                Method::GET,
                &format!("/api/v1/projects/{project}/{suffix}"),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .await,
        );
    }
    assert_database_error(common::json_request(&harness.app, Method::POST,
        &format!("/api/v1/projects/{project}/milestones/milestone/transition"),
        json!({"mutation": mutation("project.milestone.transition", "transition", 1), "lifecycle": "active"}),
        StatusCode::INTERNAL_SERVER_ERROR).await);
}

#[tokio::test]
async fn definition_database_errors_preserve_body() {
    let (_root, harness, project) = fixture().await;
    // The no-op milestone lock succeeds before the scoped revision read fails.
    sqlx::query("INSERT INTO project_milestone (id, project_id, milestone_sequence, milestone_key, created_at, updated_at) VALUES ('milestone', ?, 1, 'M001', 'now', 'now')")
        .bind(&project).execute(harness.state.db.pool()).await.unwrap();
    sqlx::query("DROP TABLE project_milestone_revision")
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let url = format!("/api/v1/projects/{project}/milestones/milestone/revisions");
    for suffix in [
        String::new(),
        format!("?cursor={}", hex::encode("1\0revision")),
    ] {
        assert_database_error(
            common::empty_request(
                &harness.app,
                Method::GET,
                &format!("{url}{suffix}"),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .await,
        );
    }
    assert_database_error(common::json_request(&harness.app, Method::POST,
        &format!("{url}/revision/transition"),
        json!({"mutation": mutation("project.milestone.revision.transition", "definition", 1), "lifecycle": "proposed"}),
        StatusCode::INTERNAL_SERVER_ERROR).await);
}

#[tokio::test]
async fn check_database_errors_preserve_body() {
    let (_root, harness, project) = fixture().await;
    sqlx::query("DROP TABLE project_milestone_check_result")
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let url = format!("/api/v1/projects/{project}/milestones/milestone/checks/check");
    assert_database_error(
        common::json_request(
            &harness.app,
            Method::POST,
            &format!("{url}/result"),
            check_request(false),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await,
    );
    sqlx::query("DROP TABLE project_milestone_check")
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    assert_database_error(
        common::json_request(
            &harness.app,
            Method::POST,
            &format!("{url}/waive"),
            check_request(true),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await,
    );
}

#[tokio::test]
async fn write_failures_preserve_body_and_roll_back_prior_writes() {
    for operation in ["lifecycle", "definition", "result", "waiver"] {
        let (_root, harness, project) = fixture().await;
        seed_milestone(&harness, &project, matches!(operation, "result" | "waiver")).await;
        let (trigger, suffix, request) = match operation {
            "lifecycle" => (
                "CREATE TRIGGER fail_write BEFORE UPDATE OF lifecycle ON project_milestone BEGIN SELECT RAISE(ABORT, 'injected lifecycle failure'); END",
                "transition",
                json!({"mutation": mutation("project.milestone.transition", "transition", 1), "lifecycle": "active"}),
            ),
            "definition" => (
                "CREATE TRIGGER fail_write BEFORE UPDATE OF current_definition_revision_id ON project_milestone BEGIN SELECT RAISE(ABORT, 'injected definition failure'); END",
                "revisions/revision/transition",
                json!({"mutation": mutation("project.milestone.revision.transition", "definition", 1), "lifecycle": "proposed"}),
            ),
            "result" => (
                "CREATE TRIGGER fail_write BEFORE UPDATE OF current_result_id ON project_milestone_check BEGIN SELECT RAISE(ABORT, 'injected result failure'); END",
                "checks/check/result", check_request(false),
            ),
            "waiver" => (
                "CREATE TRIGGER fail_write BEFORE INSERT ON project_decision BEGIN SELECT RAISE(ABORT, 'injected waiver failure'); END",
                "checks/check/waive", check_request(true),
            ),
            _ => unreachable!(),
        };
        sqlx::query(trigger)
            .execute(harness.state.db.pool())
            .await
            .unwrap();
        assert_database_error(
            common::json_request(
                &harness.app,
                Method::POST,
                &format!("/api/v1/projects/{project}/milestones/milestone/{suffix}"),
                request,
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .await,
        );
        let pool = harness.state.db.pool();
        let milestone: (String, i64) = sqlx::query_as(
            "SELECT lifecycle, version FROM project_milestone WHERE id = 'milestone'",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(milestone, ("planned".into(), 1), "{operation}");
        let definition: String = sqlx::query_scalar(
            "SELECT lifecycle FROM project_milestone_revision WHERE id = 'revision'",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(
            definition,
            if matches!(operation, "result" | "waiver") {
                "proposed"
            } else {
                "draft"
            },
            "{operation}"
        );
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM project_milestone_check_result")
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(count, 0, "{operation}");
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM project_decision")
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(count, 0, "{operation}");
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM domain_event WHERE event_type LIKE 'milestone.%'",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(count, 0, "{operation}");
        if operation == "definition" {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM project_milestone_check")
                .fetch_one(pool)
                .await
                .unwrap();
            assert_eq!(count, 0, "check materialization must roll back");
        }
        if operation == "result" {
            let check: (i64, Option<String>) = sqlx::query_as(
                "SELECT version, current_result_id FROM project_milestone_check WHERE id = 'check'",
            )
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(check, (1, None));
        }
    }
}

#[tokio::test]
async fn transitions_keep_version_refusals_and_replay_without_a_second_event() {
    let (_root, harness, project) = fixture().await;
    seed_milestone(&harness, &project, false).await;
    let base = format!("/api/v1/projects/{project}/milestones/milestone");
    let definition_request = json!({
        "mutation": mutation("project.milestone.revision.transition", "propose", 1),
        "lifecycle": "proposed",
    });
    let first: Value = common::json_request(
        &harness.app,
        Method::POST,
        &format!("{base}/revisions/revision/transition"),
        definition_request.clone(),
        StatusCode::OK,
    )
    .await;
    let replay: Value = common::json_request(
        &harness.app,
        Method::POST,
        &format!("{base}/revisions/revision/transition"),
        definition_request,
        StatusCode::OK,
    )
    .await;
    assert_eq!(replay, first);
    let conflict: Value = common::json_request(&harness.app, Method::POST,
        &format!("{base}/revisions/revision/transition"),
        json!({"mutation": mutation("project.milestone.revision.transition", "stale-definition", 1), "lifecycle": "approved"}),
        StatusCode::CONFLICT).await;
    assert_eq!(conflict["code"], "version_conflict");
    assert_eq!(
        conflict["message"],
        "the milestone changed before its definition transition"
    );
    let lifecycle_request = json!({"mutation": mutation("project.milestone.transition", "activate", 2), "lifecycle": "active"});
    let first: Value = common::json_request(
        &harness.app,
        Method::POST,
        &format!("{base}/transition"),
        lifecycle_request.clone(),
        StatusCode::OK,
    )
    .await;
    let replay: Value = common::json_request(
        &harness.app,
        Method::POST,
        &format!("{base}/transition"),
        lifecycle_request,
        StatusCode::OK,
    )
    .await;
    assert_eq!(replay, first);
    let conflict: Value = common::json_request(&harness.app, Method::POST,
        &format!("{base}/transition"),
        json!({"mutation": mutation("project.milestone.transition", "stale-lifecycle", 2), "lifecycle": "cancelled"}),
        StatusCode::CONFLICT).await;
    assert_eq!(conflict["code"], "version_conflict");
    assert_eq!(
        conflict["message"],
        "the milestone changed before its lifecycle transition"
    );
    sqlx::query("UPDATE project SET primary_milestone_id = 'milestone' WHERE id = ?")
        .bind(&project)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let conflict: Value = common::json_request(&harness.app, Method::POST,
        &format!("{base}/transition"),
        json!({"mutation": mutation("project.milestone.transition", "cancel-primary", 3), "lifecycle": "cancelled"}),
        StatusCode::CONFLICT).await;
    assert_eq!(conflict["code"], "primary_milestone_required");
    assert_eq!(
        conflict["message"],
        "choose another active primary milestone before cancelling this milestone"
    );
    let events: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM domain_event WHERE event_type LIKE 'milestone.%'")
            .fetch_one(harness.state.db.pool())
            .await
            .unwrap();
    assert_eq!(events, 2);
}

#[tokio::test]
async fn definition_validation_still_precedes_later_column_decoding() {
    let (_root, harness, project) = fixture().await;
    seed_milestone(&harness, &project, false).await;
    sqlx::query("DROP TABLE project_milestone_revision")
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    sqlx::query("CREATE VIEW project_milestone_revision AS SELECT 'revision' AS id, 'milestone' AS milestone_id, 'draft' AS lifecycle, CAST('[]' AS BLOB) AS acceptance_checks_json, CAST('[]' AS BLOB) AS evidence_requirements_json")
        .execute(harness.state.db.pool()).await.unwrap();
    let url =
        format!("/api/v1/projects/{project}/milestones/milestone/revisions/revision/transition");
    let conflict: Value = common::json_request(&harness.app, Method::POST, &url,
        json!({"mutation": mutation("project.milestone.revision.transition", "invalid-transition", 1), "lifecycle": "approved"}),
        StatusCode::CONFLICT).await;
    assert_eq!(conflict["code"], "invalid_transition");
    assert_database_error(common::json_request(&harness.app, Method::POST, &url,
        json!({"mutation": mutation("project.milestone.revision.transition", "valid-transition", 1), "lifecycle": "proposed"}),
        StatusCode::INTERNAL_SERVER_ERROR).await);
}

#[tokio::test]
async fn manual_replay_conflict_still_precedes_unneeded_column_decoding() {
    let (_root, harness, project) = fixture().await;
    seed_milestone(&harness, &project, true).await;
    let key = format!(
        "forge-idem-v1:{}:{}:{}:record",
        hex::encode("milestone-check"),
        hex::encode(&project),
        hex::encode("test-user-id")
    );
    sqlx::query("INSERT INTO project_milestone_check_result (id, project_id, milestone_id, check_id, definition_revision_id, outcome, source_kind, source_manifest_json, input_digest, governing_charter_revision_id, principal_type, principal_id, authorization_basis, authorization_action, authorization_occurred_at, expected_version, explicit_event, idempotency_key, created_at) VALUES ('result', ?, 'milestone', 'check', 'revision', 'passed', 'manual', ?, 'input', 'charter-revision', CAST('user' AS BLOB), 'test-user-id', 'explicit_user_action', 'project.milestone.check.record', ?, 1, 'event-record', ?, ?)")
        .bind(&project)
        .bind(json!({"result": "Observed the result", "check_definition_revision_id": "revision", "governing_revision_ids": ["charter-revision"]}).to_string())
        .bind(db::now_rfc3339()).bind(key).bind(db::now_rfc3339())
        .execute(harness.state.db.pool()).await.unwrap();
    let url = format!("/api/v1/projects/{project}/milestones/milestone/checks/check/result");
    let mut request = check_request(false);
    request["check_id"] = json!("different-check");
    let conflict: Value = common::json_request(
        &harness.app,
        Method::POST,
        &url,
        request,
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(conflict["code"], "idempotency_conflict");
    assert_eq!(
        conflict["message"],
        "the manual check idempotency key was already used for a different result"
    );
    assert_database_error(
        common::json_request(
            &harness.app,
            Method::POST,
            &url,
            check_request(false),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await,
    );
}
