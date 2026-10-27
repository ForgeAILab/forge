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

fn assert_command_error(body: Value, code: &str, message: &str) {
    assert!(!body["request_id"].as_str().unwrap().is_empty());
    assert_eq!(
        body,
        json!({
            "code": code, "message": message,
            "details": null, "request_id": body["request_id"],
        })
    );
}

async fn assert_manual_result_rolled_back(harness: &common::Harness) {
    let pool = harness.state.db.pool();
    let check: (i64, Option<String>) = sqlx::query_as(
        "SELECT version, current_result_id FROM project_milestone_check WHERE id = 'check'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(check, (1, None));
    let results: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM project_milestone_check_result")
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(results, 0);
    let events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'milestone.check.recorded'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(events, 0);
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

enum ReceiptShape {
    New,
    WithoutPolicy,
    ReleasePolicy,
}

async fn seed_legacy_receipt(
    harness: &common::Harness,
    project: &str,
    waive: bool,
    request: &Value,
    policy_fields: &Value,
) -> Value {
    let pool = harness.state.db.pool();
    let now = db::now_rfc3339();
    let authorization: api_types::AuthorizationProvenance =
        serde_json::from_value(request["mutation"]["authorization"].clone()).unwrap();
    let outcome = match request["status"].as_str() {
        Some("pass") => "passed",
        Some("fail") => "failed",
        Some("stale") => "stale",
        Some("blocked" | "unavailable") | None => "missing",
        other => panic!("unsupported legacy fixture status: {other:?}"),
    };
    let mut tx = db::begin_immediate(pool).await.unwrap();
    let response = if waive {
        let mut affected = json!({
            "milestone_id": "milestone", "check_id": "check",
            "definition_revision_id": "revision", "input_digest": "input",
            "expected_version": 1, "governing_revision_ids": request["governing_revision_ids"],
        });
        affected
            .as_object_mut()
            .unwrap()
            .extend(policy_fields.as_object().unwrap().clone());
        harness
            .state
            .db
            .insert_milestone_waiver_decision_in_tx(
                &mut tx,
                db::InsertMilestoneWaiverDecisionInput {
                    waiver_id: "legacy-receipt",
                    project_id: project,
                    check_id: "check",
                    reason: request["reason"].as_str().unwrap(),
                    user_id: "test-user-id",
                    authorization_basis: &authorization.authorization_basis,
                    authorization_action: &authorization.action,
                    explicit_event: &authorization.event_id,
                    authorization_occurred_at: &authorization.occurred_at,
                    charter_revision_id: "charter-revision",
                    affected_records_json: &affected.to_string(),
                    created_at: &now,
                },
            )
            .await
            .unwrap();
        let mut response = json!({
            "waiver_id": "legacy-receipt", "project_id": project,
            "milestone_id": "milestone", "check_id": "check",
            "definition_revision_id": "revision", "reason": request["reason"],
            "input_digest": "input", "expected_version": 1,
            "principal_id": "test-user-id", "authorization": authorization,
            "governing_revision_ids": request["governing_revision_ids"], "created_at": now,
        });
        response
            .as_object_mut()
            .unwrap()
            .extend(policy_fields.as_object().unwrap().clone());
        response
    } else {
        let mut manifest = json!({
            "result": request["result"], "check_definition_revision_id": "revision",
            "governing_revision_ids": request["governing_revision_ids"],
        });
        manifest
            .as_object_mut()
            .unwrap()
            .extend(policy_fields.as_object().unwrap().clone());
        let key = format!(
            "forge-idem-v1:{}:{}:{}:record",
            hex::encode("milestone-check"),
            hex::encode(project),
            hex::encode("test-user-id"),
        );
        sqlx::query("INSERT INTO project_milestone_check_result (id, project_id, milestone_id, check_id, definition_revision_id, outcome, source_kind, source_manifest_json, input_digest, governing_charter_revision_id, principal_type, principal_id, authorization_basis, authorization_action, authorization_occurred_at, expected_version, explicit_event, idempotency_key, created_at) VALUES ('legacy-receipt', ?, 'milestone', 'check', 'revision', ?, 'manual', ?, 'input', 'charter-revision', 'user', 'test-user-id', ?, ?, ?, 1, ?, ?, ?)")
            .bind(project).bind(outcome)
            .bind(manifest.to_string())
            .bind(&authorization.authorization_basis).bind(&authorization.action)
            .bind(&authorization.occurred_at).bind(&authorization.event_id)
            .bind(key).bind(&now).execute(&mut *tx).await.unwrap();
        sqlx::query("UPDATE project_milestone_check SET current_result_id = 'legacy-receipt', version = version + 1 WHERE id = 'check'")
            .execute(&mut *tx).await.unwrap();
        let digest_input = json!({
            "id": "legacy-receipt", "check_id": "check", "status": request["status"],
            "result": request["result"], "input_digest": "input",
            "governing_revision_ids": request["governing_revision_ids"],
            "authorization": authorization, "evaluated_at": now,
            "source_manifest": manifest,
        });
        let digest = api_types::canonical_digest_with_schema(
            services::MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
            &digest_input,
        )
        .unwrap();
        json!({
            "id": "legacy-receipt", "project_id": project, "check_id": "check",
            "status": request["status"], "result": request["result"],
            "principal": {"kind": "user", "id": "test-user-id", "display_name": null},
            "authorization": authorization,
            "input_digest": "input", "governing_revision_ids": request["governing_revision_ids"],
            "expected_version": 1, "event_id": authorization.event_id,
            "evaluated_at": now, "result_digest": digest,
        })
    };
    let event_type = if waive {
        "milestone.check.waived"
    } else {
        "milestone.check.recorded"
    };
    let event_payload = if waive {
        response.clone()
    } else {
        let mut payload = json!({
            "project_id": project, "milestone_id": "milestone", "check_id": "check",
            "result_id": "legacy-receipt",
            "status": outcome,
            "authorization": authorization,
            "governing_revision_ids": request["governing_revision_ids"],
        });
        payload
            .as_object_mut()
            .unwrap()
            .extend(policy_fields.as_object().unwrap().clone());
        payload
    };
    db::DomainEventRepo::append_event_in_tx(
        &*harness.state.db,
        &mut tx,
        &db::CreateDomainEvent {
            id: "legacy-event".to_owned(),
            event_type: event_type.to_owned(),
            entity_type: if waive {
                "milestone_check_waiver"
            } else {
                "milestone_check_result"
            }
            .to_owned(),
            entity_id: "legacy-receipt".to_owned(),
            actor_type: "user".to_owned(),
            actor_id: Some("test-user-id".to_owned()),
            scope_type: "project".to_owned(),
            scope_id: project.to_owned(),
            correlation_id: authorization.event_id.clone(),
            causation_id: Some(authorization.event_id),
            causation_depth: 0,
            dedupe_key: Some(format!(
                "{event_type}:{project}:{}",
                request["mutation"]["idempotency_key"].as_str().unwrap()
            )),
            payload_json: event_payload.to_string(),
            created_at: now,
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    response
}

async fn assert_committed_check_replays(waive: bool, receipt: ReceiptShape, status: &str) {
    let (_root, harness, project) = fixture().await;
    seed_milestone(&harness, &project, true).await;
    let suffix = if waive { "waive" } else { "result" };
    let url = format!("/api/v1/projects/{project}/milestones/milestone/checks/check/{suffix}");
    let mut request = check_request(waive);
    if !waive {
        request["status"] = json!(status);
    }
    let policy_fields = if matches!(receipt, ReceiptShape::ReleasePolicy) {
        // e304ec83 pinned the Charter and baseline, with a separate release-policy pair.
        request["governing_revision_ids"] = json!(["charter-revision", "baseline-revision"]);
        json!({
            "governing_policy_revision": "v076-policy-r1",
            "governing_policy_digest": "v076-policy-digest",
        })
    } else {
        json!({})
    };
    let first: Value = if matches!(receipt, ReceiptShape::New) {
        common::json_request(
            &harness.app,
            Method::POST,
            &url,
            request.clone(),
            StatusCode::OK,
        )
        .await
    } else {
        seed_legacy_receipt(&harness, &project, waive, &request, &policy_fields).await
    };
    if !waive && matches!(receipt, ReceiptShape::ReleasePolicy) {
        let payload: String = sqlx::query_scalar(
            "SELECT payload_json FROM domain_event WHERE event_type = 'milestone.check.recorded'",
        )
        .fetch_one(harness.state.db.pool())
        .await
        .unwrap();
        let payload: Value = serde_json::from_str(&payload).unwrap();
        for field in ["governing_policy_revision", "governing_policy_digest"] {
            assert_eq!(payload[field], policy_fields[field]);
        }
        assert!(payload.get("result_status").is_none());
    }
    let replay: Value = common::json_request(
        &harness.app,
        Method::POST,
        &url,
        request.clone(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(replay, first);

    if matches!(receipt, ReceiptShape::New) {
        let queries = if waive {
            vec![
                "SELECT payload_json FROM domain_event WHERE event_type = 'milestone.check.waived'",
                "SELECT affected_records_json FROM project_decision WHERE decision_class = 'waiver'",
            ]
        } else {
            vec!["SELECT source_manifest_json FROM project_milestone_check_result"]
        };
        for query in queries {
            let stored: String = sqlx::query_scalar(query)
                .fetch_one(harness.state.db.pool())
                .await
                .unwrap();
            let stored: Value = serde_json::from_str(&stored).unwrap();
            assert!(stored.get("governing_policy_revision").is_none());
            assert!(stored.get("governing_policy_digest").is_none());
        }
    }

    sqlx::query("UPDATE project SET current_charter_id = NULL, current_charter_revision_id = NULL WHERE id = ?")
        .bind(&project)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE project_milestone SET current_definition_revision_id = NULL WHERE id = 'milestone'",
    )
    .execute(harness.state.db.pool())
    .await
    .unwrap();
    let replay: Value = common::json_request(
        &harness.app,
        Method::POST,
        &url,
        request.clone(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(replay, first, "replay must retain the committed governance");

    let mut changed = request;
    changed[if waive { "reason" } else { "result" }] = json!("Different body");
    let conflict: Value = common::json_request(
        &harness.app,
        Method::POST,
        &url,
        changed,
        StatusCode::CONFLICT,
    )
    .await;
    assert_command_error(
        conflict,
        "idempotency_conflict",
        if waive {
            "the waiver idempotency key was already used for a different waiver"
        } else {
            "the manual check idempotency key was already used for a different result"
        },
    );
    let pool = harness.state.db.pool();
    let results: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM project_milestone_check_result")
        .fetch_one(pool)
        .await
        .unwrap();
    let decisions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM project_decision")
        .fetch_one(pool)
        .await
        .unwrap();
    let events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type LIKE 'milestone.check.%'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(results, i64::from(!waive));
    assert_eq!(decisions, i64::from(waive));
    assert_eq!(events, 1);
}

#[tokio::test]
async fn committed_manual_result_replays_without_a_second_row_or_event() {
    for status in ["pass", "fail", "blocked", "stale", "unavailable"] {
        assert_committed_check_replays(false, ReceiptShape::New, status).await;
    }
}

async fn assert_manual_response_replays(status: &str, display_name: Option<&str>) {
    let (_root, harness, project) = fixture().await;
    seed_milestone(&harness, &project, true).await;
    let url = format!("/api/v1/projects/{project}/milestones/milestone/checks/check/result");
    let mut request = check_request(false);
    request["status"] = json!(status);
    request["mutation"]["authorization"]["principal"]["display_name"] = json!(display_name);
    let first: Value = common::json_request(
        &harness.app,
        Method::POST,
        &url,
        request.clone(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(first["status"], status);
    assert_eq!(
        first["authorization"]["principal"]["display_name"],
        json!(display_name),
    );
    let replay: Value = common::json_request(
        &harness.app,
        Method::POST,
        &url,
        request.clone(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(replay, first);

    let (outcome, manifest): (String, String) =
        sqlx::query_as("SELECT outcome, source_manifest_json FROM project_milestone_check_result")
            .fetch_one(harness.state.db.pool())
            .await
            .unwrap();
    assert_eq!(
        outcome,
        if status == "unavailable" {
            "missing"
        } else {
            "passed"
        }
    );
    let event: String = sqlx::query_scalar(
        "SELECT payload_json FROM domain_event WHERE event_type = 'milestone.check.recorded'",
    )
    .fetch_one(harness.state.db.pool())
    .await
    .unwrap();
    let event: Value = serde_json::from_str(&event).unwrap();
    assert_eq!(event["status"], outcome);
    assert_eq!(event["result_status"], first["status"]);
    let manifest: Value = serde_json::from_str(&manifest).unwrap();
    assert_eq!(
        manifest,
        json!({
            "result": request["result"],
            "governing_revision_ids": request["governing_revision_ids"],
            "check_definition_revision_id": request["definition_revision_id"],
        })
    );
    let digest = api_types::canonical_digest_with_schema(
        services::MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
        &json!({
            "id": first["id"], "check_id": first["check_id"],
            "status": first["status"], "result": first["result"],
            "input_digest": first["input_digest"],
            "governing_revision_ids": first["governing_revision_ids"],
            "authorization": first["authorization"], "evaluated_at": first["evaluated_at"],
            "source_manifest": manifest,
        }),
    )
    .unwrap();
    assert_eq!(first["result_digest"], digest);

    for field in ["status", "display_name"] {
        let mut changed = request.clone();
        if field == "status" {
            changed["status"] = json!("blocked");
        } else {
            changed["mutation"]["authorization"]["principal"]["display_name"] =
                json!("Different reviewer");
        }
        let conflict: Value = common::json_request(
            &harness.app,
            Method::POST,
            &url,
            changed,
            StatusCode::CONFLICT,
        )
        .await;
        assert_command_error(
            conflict,
            "idempotency_conflict",
            "the manual check idempotency key was already used for a different result",
        );
    }
}

#[tokio::test]
async fn committed_manual_response_replays_display_name() {
    assert_manual_response_replays("pass", Some("Manual check reviewer")).await;
}

#[tokio::test]
async fn committed_manual_response_replays_unavailable() {
    assert_manual_response_replays("unavailable", None).await;
}

#[tokio::test]
async fn legacy_manual_response_replays_stored_display_name_and_rejects_changed_name() {
    let (_root, harness, project) = fixture().await;
    seed_milestone(&harness, &project, true).await;
    let mut request = check_request(false);
    request["mutation"]["authorization"]["principal"]["display_name"] = json!("Original reviewer");
    let first = seed_legacy_receipt(&harness, &project, false, &request, &json!({})).await;
    let replay: Value = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project}/milestones/milestone/checks/check/result"),
        request.clone(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(replay, first);
    request["mutation"]["authorization"]["principal"]["display_name"] = json!("Different reviewer");
    let conflict: Value = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project}/milestones/milestone/checks/check/result"),
        request,
        StatusCode::CONFLICT,
    )
    .await;
    assert_command_error(
        conflict,
        "idempotency_conflict",
        "the manual check idempotency key was already used for a different result",
    );
}

async fn second_project_writer(harness: &common::Harness, project: &str) -> String {
    let now = db::now_rfc3339();
    db::UserRepo::create_user(
        &*harness.state.db,
        &db::User {
            id: "second-writer".into(),
            email: "second@milestone.test".into(),
            password_hash: "unused".into(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    db::ProjectMemberRepo::add_member(
        &*harness.state.db,
        db::CreateProjectMember {
            id: db::new_uuid_v4(),
            project_id: project.into(),
            user_id: "second-writer".into(),
            role: "member".into(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
        &json!({"sub":"second-writer", "email":"second@milestone.test", "is_admin":false, "iat":now, "exp":now+900}),
        &jsonwebtoken::EncodingKey::from_secret(b"test-jwt-secret-for-development"),
    ).unwrap()
}

#[tokio::test]
async fn manual_result_and_waiver_keys_are_user_scoped_with_exact_replays() {
    for waive in [false, true] {
        for legacy in [false, true] {
            let (_root, harness, project) = fixture().await;
            seed_milestone(&harness, &project, true).await;
            let token = second_project_writer(&harness, &project).await;
            let mut a = check_request(waive);
            a["mutation"]["authorization"]["principal"]["display_name"] = json!("Reviewer A");
            let suffix = if waive { "waive" } else { "result" };
            let url =
                format!("/api/v1/projects/{project}/milestones/milestone/checks/check/{suffix}");
            let first: Value = if legacy {
                seed_legacy_receipt(&harness, &project, waive, &a, &json!({})).await
            } else {
                common::json_request(&harness.app, Method::POST, &url, a.clone(), StatusCode::OK)
                    .await
            };
            let mut b = a.clone();
            b["mutation"]["authorization"]["principal"]["id"] = json!("second-writer");
            b["mutation"]["authorization"]["principal"]["display_name"] = json!("Reviewer B");
            if !waive {
                b["mutation"]["expected_version"] = json!(2);
            }
            let second: Value = common::json_request_with_bearer(
                &harness.app,
                Method::POST,
                &url,
                &token,
                b.clone(),
                StatusCode::OK,
            )
            .await;
            let id_field = if waive { "waiver_id" } else { "id" };
            assert_ne!(first[id_field], second[id_field]);
            assert_eq!(
                first["authorization"]["principal"]["display_name"],
                "Reviewer A"
            );
            assert_eq!(
                second["authorization"]["principal"]["display_name"],
                "Reviewer B"
            );
            let a_retry: Value =
                common::json_request(&harness.app, Method::POST, &url, a, StatusCode::OK).await;
            let b_retry: Value = common::json_request_with_bearer(
                &harness.app,
                Method::POST,
                &url,
                &token,
                b,
                StatusCode::OK,
            )
            .await;
            assert_eq!(a_retry, first);
            assert_eq!(b_retry, second);
            let event_type = if waive {
                "milestone.check.waived"
            } else {
                "milestone.check.recorded"
            };
            let events: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM domain_event WHERE event_type=?")
                    .bind(event_type)
                    .fetch_one(harness.state.db.pool())
                    .await
                    .unwrap();
            assert_eq!(events, 2);
            if !waive {
                let results: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM project_milestone_check_result")
                        .fetch_one(harness.state.db.pool())
                        .await
                        .unwrap();
                assert_eq!(results, 2);
                let keys: (String, String) = sqlx::query_as("SELECT e.dedupe_key,r.idempotency_key FROM domain_event e JOIN project_milestone_check_result r ON r.id=e.entity_id WHERE r.principal_id='second-writer'")
                    .fetch_one(harness.state.db.pool()).await.unwrap();
                assert_eq!(keys.0, format!("milestone.check.recorded:{}", keys.1));
            }
        }
    }
}

#[tokio::test]
async fn legacy_manual_event_for_another_result_is_ignored() {
    let (_root, harness, project) = fixture().await;
    seed_milestone(&harness, &project, true).await;
    let mut request = check_request(false);
    request["mutation"]["authorization"]["principal"]["display_name"] = json!("Original reviewer");
    let first = seed_legacy_receipt(&harness, &project, false, &request, &json!({})).await;
    sqlx::query("UPDATE domain_event SET payload_json=json_set(payload_json,'$.result_id','another-result','$.authorization.principal.id','another-user') WHERE id='legacy-event'")
        .execute(harness.state.db.pool()).await.unwrap();
    let replay: Value = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project}/milestones/milestone/checks/check/result"),
        request,
        StatusCode::OK,
    )
    .await;
    assert_eq!(replay["id"], first["id"]);
    assert_eq!(replay["result"], first["result"]);
    assert_eq!(replay["authorization"]["principal"]["id"], "test-user-id");
    assert!(replay["authorization"]["principal"]["display_name"].is_null());
}

#[tokio::test]
async fn legacy_missing_outcome_keeps_blocked_reconstruction() {
    let (_root, harness, project) = fixture().await;
    seed_milestone(&harness, &project, true).await;
    let mut request = check_request(false);
    request["status"] = json!("blocked");
    let expected = seed_legacy_receipt(&harness, &project, false, &request, &json!({})).await;
    for status in ["blocked", "unavailable", "pending"] {
        request["status"] = json!(status);
        let replay: Value = common::json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/projects/{project}/milestones/milestone/checks/check/result"),
            request.clone(),
            StatusCode::OK,
        )
        .await;
        assert_eq!(replay, expected);
    }
}

#[tokio::test]
async fn manual_result_without_event_keeps_row_only_reconstruction() {
    let (_root, harness, project) = fixture().await;
    seed_milestone(&harness, &project, true).await;
    let mut request = check_request(false);
    request["mutation"]["authorization"]["principal"]["display_name"] = json!("Original reviewer");
    let mut expected = seed_legacy_receipt(&harness, &project, false, &request, &json!({})).await;
    let pool = harness.state.db.pool();
    sqlx::query("DELETE FROM domain_event WHERE event_type = 'milestone.check.recorded'")
        .execute(pool)
        .await
        .unwrap();
    let manifest: String = sqlx::query_scalar(
        "SELECT source_manifest_json FROM project_milestone_check_result WHERE id = 'legacy-receipt'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    expected["authorization"]["principal"]["display_name"] = Value::Null;
    expected["result_digest"] = json!(api_types::canonical_digest_with_schema(
        services::MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
        &json!({
            "id": expected["id"], "check_id": expected["check_id"],
            "status": expected["status"], "result": expected["result"],
            "input_digest": expected["input_digest"],
            "governing_revision_ids": expected["governing_revision_ids"],
            "authorization": expected["authorization"], "evaluated_at": expected["evaluated_at"],
            "source_manifest": serde_json::from_str::<Value>(&manifest).unwrap(),
        }),
    )
    .unwrap());
    let replay: Value = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project}/milestones/milestone/checks/check/result"),
        request,
        StatusCode::OK,
    )
    .await;
    assert_eq!(replay, expected);
    let results: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM project_milestone_check_result")
        .fetch_one(pool)
        .await
        .unwrap();
    let events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'milestone.check.recorded'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(results, 1);
    assert_eq!(events, 0);
}

#[tokio::test]
async fn manual_replay_refuses_unparseable_event_payload() {
    let (_root, harness, project) = fixture().await;
    seed_milestone(&harness, &project, true).await;
    let request = check_request(false);
    seed_legacy_receipt(&harness, &project, false, &request, &json!({})).await;
    sqlx::query("UPDATE domain_event SET payload_json = json_remove(payload_json, '$.authorization') WHERE event_type = 'milestone.check.recorded'")
        .execute(harness.state.db.pool()).await.unwrap();
    // HEAD decodes the event; main (79f2191b) ignored it and returned 200.
    let error: Value = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project}/milestones/milestone/checks/check/result"),
        request,
        StatusCode::INTERNAL_SERVER_ERROR,
    )
    .await;
    assert_command_error(
        error,
        "internal_error",
        "persisted manual check event is invalid",
    );
}

#[tokio::test]
async fn manual_replay_event_conflict_precedes_unneeded_column_decoding() {
    let (_root, harness, project) = fixture().await;
    seed_milestone(&harness, &project, true).await;
    let url = format!("/api/v1/projects/{project}/milestones/milestone/checks/check/result");
    let mut request = check_request(false);
    request["status"] = json!("unavailable");
    request["mutation"]["authorization"]["principal"]["display_name"] = json!("Original reviewer");
    let _: Value = common::json_request(
        &harness.app,
        Method::POST,
        &url,
        request.clone(),
        StatusCode::OK,
    )
    .await;
    // Permit deliberate corruption of the append-only result in this isolated database.
    sqlx::query("DROP TRIGGER project_milestone_check_result_immutable_update")
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE project_milestone_check_result SET created_at = CAST(created_at AS BLOB)")
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    assert_database_error(
        common::json_request(
            &harness.app,
            Method::POST,
            &url,
            request.clone(),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await,
    );
    for field in ["display_name", "status"] {
        let mut changed = request.clone();
        if field == "display_name" {
            changed["mutation"]["authorization"]["principal"]["display_name"] =
                json!("Different reviewer");
        } else {
            changed["status"] = json!("blocked");
        }
        let conflict: Value = common::json_request(
            &harness.app,
            Method::POST,
            &url,
            changed,
            StatusCode::CONFLICT,
        )
        .await;
        assert_command_error(
            conflict,
            "idempotency_conflict",
            "the manual check idempotency key was already used for a different result",
        );
    }
}

#[tokio::test]
async fn manual_replay_refuses_event_details_that_disagree_with_row_or_request() {
    let (_root, harness, project) = fixture().await;
    seed_milestone(&harness, &project, true).await;
    let url = format!("/api/v1/projects/{project}/milestones/milestone/checks/check/result");
    let request = check_request(false);
    let _: Value = common::json_request(
        &harness.app,
        Method::POST,
        &url,
        request.clone(),
        StatusCode::OK,
    )
    .await;
    let payload: String = sqlx::query_scalar(
        "SELECT payload_json FROM domain_event WHERE event_type = 'milestone.check.recorded'",
    )
    .fetch_one(harness.state.db.pool())
    .await
    .unwrap();
    let payload: Value = serde_json::from_str(&payload).unwrap();
    for (pointer, value, status, code, message) in [
        (
            "/result_id",
            json!("other-result"),
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "persisted manual check event disagrees with its result",
        ),
        (
            "/result_status",
            json!("unavailable"),
            StatusCode::CONFLICT,
            "idempotency_conflict",
            "the manual check idempotency key was already used for a different result",
        ),
        (
            "/authorization/principal/id",
            json!("other-user"),
            StatusCode::CONFLICT,
            "idempotency_conflict",
            "the manual check idempotency key was already used for a different result",
        ),
    ] {
        let mut corrupted = payload.clone();
        *corrupted.pointer_mut(pointer).unwrap() = value;
        sqlx::query("UPDATE domain_event SET payload_json = ? WHERE event_type = 'milestone.check.recorded'")
            .bind(corrupted.to_string()).execute(harness.state.db.pool()).await.unwrap();
        let error: Value =
            common::json_request(&harness.app, Method::POST, &url, request.clone(), status).await;
        assert_command_error(error, code, message);
    }
}

#[tokio::test]
async fn committed_waiver_replays_without_a_second_row_or_event() {
    assert_committed_check_replays(true, ReceiptShape::New, "").await;
}

#[tokio::test]
async fn legacy_manual_result_replays_without_policy_fields() {
    for status in ["pass", "fail"] {
        assert_committed_check_replays(false, ReceiptShape::WithoutPolicy, status).await;
    }
}

#[tokio::test]
async fn legacy_waiver_replays_without_policy_fields() {
    assert_committed_check_replays(true, ReceiptShape::WithoutPolicy, "").await;
}

#[tokio::test]
async fn v08_manual_result_replays_with_release_policy_fields() {
    for status in ["pass", "fail"] {
        assert_committed_check_replays(false, ReceiptShape::ReleasePolicy, status).await;
    }
}

#[tokio::test]
async fn v08_waiver_replays_with_original_release_policy_fields() {
    assert_committed_check_replays(true, ReceiptShape::ReleasePolicy, "").await;
}

#[tokio::test]
async fn replay_refuses_partial_or_empty_policy_fields() {
    for waive in [false, true] {
        for (policy_fields, missing_field) in [
            (
                json!({"governing_policy_revision": "v076-policy-r1"}),
                "digest",
            ),
            (
                json!({"governing_policy_digest": "v076-policy-digest"}),
                "revision",
            ),
            (
                json!({"governing_policy_revision": "", "governing_policy_digest": "v076-policy-digest"}),
                "revision",
            ),
            (
                json!({"governing_policy_revision": "v076-policy-r1", "governing_policy_digest": ""}),
                "digest",
            ),
            (
                json!({"governing_policy_revision": "  ", "governing_policy_digest": "v076-policy-digest"}),
                "revision",
            ),
            (
                json!({"governing_policy_revision": "v076-policy-r1", "governing_policy_digest": "  "}),
                "digest",
            ),
        ] {
            let (_root, harness, project) = fixture().await;
            seed_milestone(&harness, &project, true).await;
            let mut request = check_request(waive);
            request["governing_revision_ids"] = json!(["charter-revision", "baseline-revision"]);
            seed_legacy_receipt(&harness, &project, waive, &request, &policy_fields).await;
            let suffix = if waive { "waive" } else { "result" };
            let error = common::json_request(
                &harness.app,
                Method::POST,
                &format!("/api/v1/projects/{project}/milestones/milestone/checks/check/{suffix}"),
                request,
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .await;
            if waive {
                assert_command_error(
                    error,
                    "internal_error",
                    &format!("persisted milestone waiver policy {missing_field} is missing"),
                );
            } else {
                assert_command_error(
                    error,
                    "internal_error",
                    "persisted manual check authority manifest is incomplete",
                );
            }
        }
    }
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
async fn definition_rejects_a_check_owned_by_another_project() {
    let (_root, harness, project) = fixture().await;
    seed_milestone(&harness, &project, false).await;
    let other: ProjectResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({"name": "Other Project"}),
        StatusCode::OK,
    )
    .await;
    let pool = harness.state.db.pool();
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO project_milestone (id, project_id, milestone_sequence, milestone_key, lifecycle, created_at, updated_at) VALUES ('other-milestone', ?, 1, 'M001', 'planned', ?, ?)")
        .bind(&other.id).bind(&now).bind(&now).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO project_milestone_revision (id, milestone_id, revision, lifecycle, outcome, schema_version, render_version, rendered_view, content_digest, rendered_digest, author_type, author_id, created_at) VALUES ('other-revision', 'other-milestone', 1, 'proposed', 'Other outcome', 'test', 'test', 'Other outcome', 'content', 'render', 'user', 'definition-author', ?)")
        .bind(&now).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO project_milestone_check (id, project_id, milestone_id, definition_revision_id, check_key, description, required, source_kind, expected_result, evidence_required, created_at, updated_at) VALUES ('check', ?, 'other-milestone', 'other-revision', 'check', 'Other check', 0, 'manual', 'passed', 0, ?, ?)")
        .bind(&other.id).bind(&now).bind(&now).execute(pool).await.unwrap();
    let error = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project}/milestones/milestone/revisions/revision/transition"),
        json!({"mutation": mutation("project.milestone.revision.transition", "cross-project", 1), "lifecycle": "proposed"}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_command_error(
        error,
        "bad_request",
        "milestone acceptance check belongs to another Project or milestone",
    );
    let milestone: (i64, Option<String>) = sqlx::query_as(
        "SELECT version, current_definition_revision_id FROM project_milestone WHERE id = 'milestone'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(milestone, (1, None));
    let lifecycle: String = sqlx::query_scalar(
        "SELECT lifecycle FROM project_milestone_revision WHERE id = 'revision'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(lifecycle, "draft");
    let check: (String, String, String, i64) = sqlx::query_as(
        "SELECT project_id, milestone_id, definition_revision_id, version FROM project_milestone_check WHERE id = 'check'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        check,
        (
            other.id,
            "other-milestone".into(),
            "other-revision".into(),
            1
        )
    );
    let events: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM domain_event WHERE event_type LIKE 'milestone.%'")
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(events, 0);
}

#[tokio::test]
async fn manual_result_unique_conflict_preserves_in_progress_response() {
    let (_root, harness, project) = fixture().await;
    seed_milestone(&harness, &project, true).await;
    // Insert a duplicate only after the replay lookup, exercising a real UNIQUE failure.
    sqlx::query(
        "CREATE TRIGGER duplicate_result AFTER INSERT ON project_milestone_check_result
         BEGIN
             INSERT INTO project_milestone_check_result (
                 id, project_id, milestone_id, check_id, definition_revision_id,
                 outcome, source_kind, source_manifest_json, input_digest,
                 governing_charter_revision_id, principal_type, principal_id,
                 authorization_basis, authorization_action, authorization_occurred_at,
                 expected_version, explicit_event, idempotency_key, created_at
             ) SELECT id || '-duplicate', project_id, milestone_id, check_id,
                 definition_revision_id, outcome, source_kind, source_manifest_json,
                 input_digest, governing_charter_revision_id, principal_type,
                 principal_id, authorization_basis, authorization_action,
                 authorization_occurred_at, expected_version, explicit_event,
                 idempotency_key, created_at
             FROM project_milestone_check_result WHERE id = NEW.id;
         END",
    )
    .execute(harness.state.db.pool())
    .await
    .unwrap();
    let error = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project}/milestones/milestone/checks/check/result"),
        check_request(false),
        StatusCode::CONFLICT,
    )
    .await;
    assert_command_error(
        error,
        "idempotency_in_progress",
        "another manual check result is being committed; retry with the same key",
    );
    assert_manual_result_rolled_back(&harness).await;
}

#[tokio::test]
async fn manual_result_final_version_conflict_preserves_response_and_rolls_back() {
    let (_root, harness, project) = fixture().await;
    seed_milestone(&harness, &project, true).await;
    // Change the version between the target read and the final CAS within the transaction.
    sqlx::query(
        "CREATE TRIGGER change_check_version AFTER INSERT ON project_milestone_check_result
         BEGIN
             UPDATE project_milestone_check SET version = version + 1 WHERE id = NEW.check_id;
         END",
    )
    .execute(harness.state.db.pool())
    .await
    .unwrap();
    let error = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project}/milestones/milestone/checks/check/result"),
        check_request(false),
        StatusCode::CONFLICT,
    )
    .await;
    assert_command_error(
        error,
        "version_conflict",
        "the acceptance check changed before its result was committed",
    );
    assert_manual_result_rolled_back(&harness).await;
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
