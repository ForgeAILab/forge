//! Scoped, pinned, and user-attested Project evidence.

use super::*;

#[tokio::test]
async fn v076_project_evidence_is_scoped_pinned_and_user_attested() {
    let workspace = common::TestDir::new("v076-project-evidence");
    let harness = common::test_app(workspace.path(), "v076-project-evidence").await;
    let app = &harness.app;
    let token = common::test_jwt();
    let fixture = create_genesis_project(app, &token, "v076-evidence").await;

    let project = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}", fixture.project_id),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    // Project ownership is authoritative.  A lost/repaired membership row
    // must not lock the owner out of Project media routes.
    sqlx::query("DELETE FROM project_member WHERE project_id = ?")
        .bind(&fixture.project_id)
        .execute(harness.state.db.pool())
        .await
        .expect("remove redundant owner membership");
    let expected_project_version = project["version"]
        .as_i64()
        .expect("project version for media upload");
    let proof_bytes: &[u8] = b"\x89PNG\r\n\x1a\nV076 proof";
    let proof_checksum = hex::encode(Sha256::digest(proof_bytes));
    let upload_authorization =
        user_authorization("project.media.upload", "v076-project-proof-event");
    let asset = upload_project_media(
        app,
        &token,
        &fixture.project_id,
        expected_project_version,
        "v076-project-proof-upload",
        &upload_authorization,
        proof_bytes,
        StatusCode::CREATED,
    )
    .await;
    let asset_id = required_string(&asset, &["id"]);
    assert_eq!(asset["project_id"], json!(fixture.project_id));
    assert!(
        asset.get("storage_key").is_none(),
        "Project media responses must not expose internal storage handles"
    );
    assert_eq!(asset["checksum"], json!(proof_checksum));
    assert_eq!(asset["availability"], json!("available"));
    assert_eq!(
        asset["stable_project_url"],
        json!(format!(
            "/api/v1/projects/{}/media/{asset_id}",
            fixture.project_id
        ))
    );

    // A lost upload response is replay-safe even after the Project version is
    // unchanged only by the upload itself: the same idempotency key returns
    // the original asset identity and storage metadata.
    let upload_replay = upload_project_media(
        app,
        &token,
        &fixture.project_id,
        expected_project_version,
        "v076-project-proof-upload",
        &upload_authorization,
        proof_bytes,
        StatusCode::OK,
    )
    .await;
    assert_eq!(upload_replay, asset);

    let media_list = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}/media", fixture.project_id),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(media_list["items"].as_array().map(Vec::len), Some(1));
    assert_eq!(media_list["items"][0]["id"], json!(asset_id));

    let (media_status, media_body) = raw_request(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}/media/{asset_id}", fixture.project_id),
        &token,
        Body::empty(),
        None,
    )
    .await;
    assert_eq!(media_status, StatusCode::OK);
    assert_eq!(media_body, proof_bytes);

    // Media disposition is an audited user-only mutation. The tombstone
    // receipt is replay-exact even though the public response is only the
    // current media projection.
    let asset_version: i64 = sqlx::query_scalar("SELECT version FROM media_asset WHERE id = ?")
        .bind(&asset_id)
        .fetch_one(harness.state.db.pool())
        .await
        .expect("project media asset version");
    let tombstone_body = json!({
        "mutation": {
            "expected_version": asset_version,
            "idempotency_key": "v076-project-media-redact",
            "authorization": user_authorization(
                "project.media.redact",
                "v076-project-media-redact-event"
            )
        },
        "reason": "The user removed this bounded proof from active media."
    });
    let tombstoned = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/media/{asset_id}/redact",
            fixture.project_id
        ),
        &token,
        tombstone_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(tombstoned["availability"], json!("redacted"));
    let tombstone_replay = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/media/{asset_id}/redact",
            fixture.project_id
        ),
        &token,
        tombstone_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(tombstone_replay, tombstoned);
    for (label, altered) in user_authorization_replay_variants(&tombstone_body, false) {
        let conflict = request_json(
            app,
            Method::POST,
            &format!(
                "/api/v1/projects/{}/media/{asset_id}/redact",
                fixture.project_id
            ),
            &token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }
    for (label, altered) in [
        ("media tombstone version", {
            let mut value = tombstone_body.clone();
            value["mutation"]["expected_version"] = json!(asset_version + 1);
            value
        }),
        ("media tombstone reason", {
            let mut value = tombstone_body.clone();
            value["reason"] = json!("A different audited removal reason.");
            value
        }),
        ("media tombstone target asset", tombstone_body.clone()),
    ] {
        let target = if label == "media tombstone target asset" {
            format!(
                "/api/v1/projects/{}/media/{}/redact",
                fixture.project_id, "v076-different-media-asset"
            )
        } else {
            format!(
                "/api/v1/projects/{}/media/{asset_id}/redact",
                fixture.project_id
            )
        };
        let conflict = request_json(
            app,
            Method::POST,
            &target,
            &token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }

    // A Task upload is the same Project-owned asset, not a second byte copy.
    // The later evidence attachment deliberately references this existing
    // Task asset to cover the shared-media path used by release proof.
    let task = request_json(
        app,
        Method::POST,
        &format!("/api/v1/projects/{}/tasks", fixture.project_id),
        &token,
        json!({
            "title": "V076 shared evidence task",
            "description": "The Task upload is reused by Project evidence.",
            "governance": {
                "charter_revision_id": fixture.charter_revision_id,
                "plan_item_id": "v076-plan-item-1",
                "milestone_id": fixture.milestone_id,
                "document_revision_ids": [],
                "capability_class": "repository_write",
                "risk_class": "low"
            }
        }),
        &[StatusCode::OK, StatusCode::CREATED],
    )
    .await;
    let task_id = required_string(&task, &["id"]);
    let task_asset = upload_task_media(
        app,
        &token,
        &task_id,
        "v076-task-proof.png",
        "image/png",
        proof_bytes,
        StatusCode::CREATED,
    )
    .await;
    let task_asset_id = required_string(&task_asset, &["id"]);
    assert_eq!(task_asset["id"], json!(task_asset_id));
    let task_media_body = raw_request(
        app,
        Method::GET,
        &format!("/api/v1/media/{task_asset_id}"),
        &token,
        Body::empty(),
        None,
    )
    .await;
    assert_eq!(task_media_body.0, StatusCode::OK);
    assert_eq!(task_media_body.1, proof_bytes);
    let media_after_task_upload = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}/media", fixture.project_id),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let task_asset_projection = media_after_task_upload["items"]
        .as_array()
        .and_then(|items| items.iter().find(|item| item["id"] == task_asset["id"]))
        .cloned()
        .expect("Task media is projected as a Project asset");
    assert_eq!(task_asset_projection["checksum"], json!(proof_checksum));
    assert_eq!(
        task_asset_projection["task_media_ids"],
        json!([task_asset_id.clone()])
    );

    // Governed Tasks gate milestone readiness; complete the helper Task so
    // the readiness evaluation below can compute "ready".
    let task_version = task["version"].as_i64().expect("task version");
    let done_task = request_json(
        app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/transition"),
        &token,
        json!({
            "status": "done",
            "version": task_version,
            "reason": "V076 evidence helper Task completed"
        }),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(done_task["task"]["status"], json!("done"));
    let milestone = request_json(
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
    let milestone_version = milestone["version"]
        .as_i64()
        .expect("milestone version before evidence attach");
    let attach_body = json!({
        "mutation": {
            "expected_version": milestone_version,
            "idempotency_key": "v076-project-proof-attach",
            "authorization": user_authorization(
                "project.evidence.attach",
                "v076-project-proof-attach-event"
            )
        },
        "milestone_id": fixture.milestone_id,
        "asset_id": task_asset_id,
        "acceptance_check_ids": [fixture.milestone_acceptance_check_id],
        "caption": "A bounded Project proof screenshot.",
        "kind": "screenshot",
        "checksum": proof_checksum
    });

    // Evidence attachment is an explicit user action. A Project Agent
    // principal cannot attest or attach a proof item through the user API.
    let mut agent_attach = attach_body.clone();
    agent_attach["mutation"]["authorization"] = json!({
        "principal": {"kind": "agent", "id": fixture.project_identity_id},
        "authorization_basis": "project_agent",
        "action": "project.evidence.attach",
        "event_id": "v076-agent-evidence-event",
        "occurred_at": Utc::now().to_rfc3339()
    });
    let agent_denial = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/evidence",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        agent_attach,
        &[StatusCode::FORBIDDEN],
    )
    .await;
    assert_eq!(agent_denial["code"], json!("authorization.invalid"));

    let evidence = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/evidence",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        attach_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    let evidence_id = required_string(&evidence, &["id"]);
    assert_eq!(evidence["project_id"], json!(fixture.project_id));
    assert_eq!(evidence["milestone_id"], json!(fixture.milestone_id));
    assert_eq!(evidence["asset_id"], json!(task_asset_id));
    assert_eq!(evidence["checksum"], json!(proof_checksum));
    assert_eq!(evidence["author"]["kind"], json!("user"));
    let shared_asset_rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM media_asset WHERE id = ? AND project_id = ?")
            .bind(&task_asset_id)
            .bind(&fixture.project_id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("shared Project asset count");
    let shared_attachment_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_media_attachment
         WHERE asset_id = ? AND project_id = ? AND deleted_at IS NULL",
    )
    .bind(&task_asset_id)
    .bind(&fixture.project_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("shared Project attachment count");
    assert_eq!(shared_asset_rows, 1);
    assert_eq!(shared_attachment_rows, 2);

    let evidence_replay = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/evidence",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        attach_body,
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(evidence_replay, evidence);
    let evidence_list = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/milestones/{}/evidence",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(evidence_list["items"], json!([evidence.clone()]));
    let evidence_get = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/milestones/{}/evidence/{evidence_id}",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(evidence_get, evidence);

    // A Project-owned asset cannot be attached to another Project's
    // milestone, and the failure must not reveal its metadata.
    let other_project = request_json(
        app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({"name": "V076 evidence boundary project"}),
        &[StatusCode::OK, StatusCode::CREATED],
    )
    .await;
    let other_project_id = required_string(&other_project, &["id"]);
    let cross_scope = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{other_project_id}/milestones/{}/evidence",
            fixture.milestone_id
        ),
        &token,
        json!({
            "mutation": {
                "expected_version": milestone_version,
                "idempotency_key": "v076-cross-project-evidence",
                "authorization": user_authorization(
                    "project.evidence.attach",
                    "v076-cross-project-evidence-event"
                )
            },
            "milestone_id": fixture.milestone_id,
            "asset_id": task_asset_id,
            "acceptance_check_ids": [],
            "caption": "must not cross Project scope",
            "kind": "screenshot",
            "checksum": proof_checksum
        }),
        &[StatusCode::NOT_FOUND],
    )
    .await;
    assert_eq!(cross_scope["code"], json!("not_found"));
    assert!(!cross_scope.to_string().contains(&proof_checksum));

    // The evidence participates in the exact readiness candidate and is
    // pinned by the user-approved immutable release.
    record_passed_check(app, &harness, &token, &fixture).await;
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
                "expected_version": milestone_version,
                "idempotency_key": "v076-evidence-readiness",
                "authorization": user_authorization(
                    "project.milestone.readiness",
                    "v076-evidence-readiness-event"
                )
            },
            "milestone_id": fixture.milestone_id
        }),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(readiness["result"], json!("ready"));
    assert_eq!(
        readiness["evidence_attachment_ids"],
        json!([evidence_id.clone()])
    );
    let readiness_snapshot_id = required_string(&readiness, &["id"]);
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
    let release = request_json(
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
                "idempotency_key": "v076-evidence-release",
                "authorization": user_authorization(
                    "project.milestone.release",
                    "v076-evidence-release-event"
                )
            },
            "milestone_id": fixture.milestone_id,
            "readiness_snapshot_id": readiness_snapshot_id,
            "readiness_digest": readiness_digest
        }),
        &[StatusCode::OK],
    )
    .await;
    let release_id = required_string(&release, &["id"]);
    let pin_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_release_media_pin
         WHERE release_id = ? AND asset_id = ? AND attachment_id = ?",
    )
    .bind(&release_id)
    .bind(&task_asset_id)
    .bind(&evidence_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("release evidence pin count");
    assert_eq!(pin_count, 1);

    let (remove_status, remove_body) = raw_request(
        app,
        Method::DELETE,
        &format!(
            "/api/v1/projects/{}/milestones/{}/evidence/{evidence_id}",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        Body::from(
            serde_json::to_vec(&json!({
            "expected_version": evidence["version"],
            "idempotency_key": "v076-evidence-remove",
            "authorization": user_authorization(
                "project.evidence.remove",
                "v076-evidence-remove-event"
            )
            }))
            .expect("evidence removal serializes"),
        ),
        Some("application/json"),
    )
    .await;
    assert_eq!(remove_status, StatusCode::NO_CONTENT);
    assert!(remove_body.is_empty());
    let removed_evidence = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/milestones/{}/evidence/{evidence_id}",
            fixture.project_id, fixture.milestone_id
        ),
        &token,
        Value::Null,
        &[StatusCode::NOT_FOUND],
    )
    .await;
    assert_eq!(removed_evidence["code"], json!("not_found"));

    // Removing the evidence attachment does not erase the immutable release
    // pin. The old Task URL can then disappear independently of the retained
    // Project-authorized proof URL.
    let delete_task_media = common::raw_empty_request(
        app,
        Method::DELETE,
        &format!("/api/v1/media/{task_asset_id}"),
    )
    .await;
    assert_eq!(delete_task_media.status(), StatusCode::NO_CONTENT);
    let deleted_task_media = raw_request(
        app,
        Method::GET,
        &format!("/api/v1/media/{task_asset_id}"),
        &token,
        Body::empty(),
        None,
    )
    .await;
    assert_eq!(deleted_task_media.0, StatusCode::NOT_FOUND);
    let (retained_status, retained_body) = raw_request(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/media/{task_asset_id}",
            fixture.project_id
        ),
        &token,
        Body::empty(),
        None,
    )
    .await;
    assert_eq!(retained_status, StatusCode::OK);
    assert_eq!(retained_body, proof_bytes);
}
