//! Direct command classification native denials tests.

use super::*;

#[tokio::test]
async fn terminal_native_denial_offers_only_a_held_owner_escalation_operation() {
    let fixture =
        fixture_with_permissions(r#"{"permissions":["read_project","propose_message"]}"#).await;
    sqlx::query("UPDATE project_agent_binding SET permission_ceiling_json = '{\"allowed\":[\"read_project\",\"propose_message\"]}' WHERE project_id = ? AND state = 'active'")
        .bind(PROJECT_ID).execute(fixture.db.pool()).await.unwrap();
    let deny = |key| {
        fixture.provider.propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            PROJECT_DOCUMENT_OPERATION,
            document_arguments(key),
        )
    };
    let held = structured_error(deny("held-alternative").await.unwrap_err());
    assert_eq!(
        held.denied_by.as_ref().map(ToString::to_string).as_deref(),
        Some("permission_missing(propose_project)")
    );
    assert_eq!(held.alternatives, Some(vec!["message.send".to_owned()]));
    sqlx::query("UPDATE project_agent_binding SET permission_ceiling_json = '{\"allowed\":[\"read_project\"]}' WHERE project_id = ? AND state = 'active'")
        .bind(PROJECT_ID).execute(fixture.db.pool()).await.unwrap();
    let revoked = structured_error(deny("revoked-alternative").await.unwrap_err());
    assert!(revoked.alternatives.is_none());
}

#[tokio::test]
async fn terminal_native_task_commands_preserve_evaluator_permission_and_identity_denials() {
    let fixture = fixture().await;
    let task = db::TaskRepo::create(
        &*fixture.db,
        db::CreateTask {
            id: db::new_uuid_v4(),
            project_id: PROJECT_ID.to_owned(),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "Blocked Task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "blocked".to_owned(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .unwrap();
    // Revoke the canonical Task permission after catalog composition.
    // Exercise the real evaluator rather than a fabricated adapter error.
    sqlx::query("UPDATE project_agent_binding SET permission_ceiling_json = '{\"allowed\":[\"read_project\"]}' WHERE project_id = ? AND state = 'active'")
        .bind(PROJECT_ID).execute(fixture.db.pool()).await.unwrap();
    let review = fixture.provider.propose(AGENT_ID, &fixture.project_scope, RUNTIME_SESSION_ID,
        "task.action", json!({"operation":"task.action", "payload":{
            "task_id":task.id, "action":{"verb":"approve","override":false,"reason":"Check refusal"}, "version":task.version}, "dedupe_key":"denied-review", "correlation_id":"denied-review"})
    ).await.unwrap_err();
    let review = structured_error(review);
    assert_eq!(
        review
            .denied_by
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("permission_missing(propose_task)")
    );
    assert_eq!(review.retry.unwrap().action, api_types::RetryAction::None);
    sqlx::query("UPDATE project_agent_binding SET permission_ceiling_json = '{\"allowed\":[\"read_project\",\"propose_task\"]}' WHERE project_id = ? AND state = 'active'")
        .bind(PROJECT_ID).execute(fixture.db.pool()).await.unwrap();
    sqlx::query("UPDATE agent_identity SET paused = 1 WHERE id = ?")
        .bind(AGENT_ID)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let recover = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            "task.action",
            json!({"operation":"task.action", "payload":{
            "task_id":task.id, "action":{"verb":"retry","fresh_session":true}, "version":task.version},
            "dedupe_key":"denied-recover", "correlation_id":"denied-recover"}),
        )
        .await
        .unwrap_err();
    let recover = structured_error(recover);
    assert_eq!(
        recover
            .denied_by
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("identity_paused")
    );
    assert_eq!(recover.retry.unwrap().action, api_types::RetryAction::None);
    assert!(recover
        .safe_message
        .contains("repeating this call will be refused again"));
}

#[tokio::test]
async fn terminal_native_charter_denial_preserves_evaluator_cause() {
    let fixture = fixture().await;
    let unadopted_project = "terminal-denial-unadopted-project";
    seed_project(
        &fixture.db,
        unadopted_project,
        "terminal-denial-unadopted-repo",
    )
    .await;
    let scope = CanonicalScope {
        scope_id: unadopted_project.to_owned(),
        ..fixture.project_scope.clone()
    };
    let outcome = fixture
        .provider
        .propose(
            AGENT_ID,
            &scope,
            RUNTIME_SESSION_ID,
            PROJECT_DOCUMENT_OPERATION,
            document_arguments("charter-denial"),
        )
        .await
        .unwrap_err();
    let outcome = structured_error(outcome);
    assert_eq!(outcome.code, api_types::OutcomeCode::PolicyDenied);
    assert_eq!(
        outcome
            .denied_by
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("charter_not_adopted")
    );
    assert_eq!(outcome.retry.unwrap().action, api_types::RetryAction::None);
    assert!(outcome.safe_message.contains("Do not retry"));
}

#[tokio::test]
async fn terminal_denial_per_document_refusal_does_not_block_a_different_document_in_the_same_turn()
{
    let fixture = fixture().await;
    let (scope, _) = project_chat_session(&fixture).await;
    let tool = native_tool(
        &fixture,
        scope,
        true,
        forge_agent_host::FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
    );
    let draft = invoke_native(
        &*tool,
        document_arguments("first-draft"),
        "same-turn",
        "draft",
    )
    .await;
    assert!(!draft.is_error, "{draft:?}");
    let (document_id, revision_id, version): (String, String, i64) = sqlx::query_as(
        "SELECT id, current_draft_revision_id, version FROM project_document WHERE project_id = 'direct-classification-project'",
    ).fetch_one(fixture.db.pool()).await.unwrap();
    let (content_digest, render_digest): (String, String) = sqlx::query_as(
        "SELECT content_digest, rendered_digest FROM project_document_revision WHERE id = ?",
    )
    .bind(&revision_id)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE project_document SET approval_policy = 'user' WHERE id = ?")
        .bind(&document_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let denied = invoke_native(&*tool, json!({"operation":"project.document", "payload":{
        "action":"approve", "document_id":document_id, "revision_id":revision_id,
        "content_digest":content_digest, "render_digest":render_digest,
        "expected_document_version":version}, "dedupe_key":"forbidden-approval", "correlation_id":"forbidden-approval"}), "same-turn", "approve").await;
    assert!(denied.is_error, "{denied:?}");
    assert_eq!(denied.value["denied_by"], "unspecified");
    assert_eq!(denied.value["retry"]["action"], "none");
    assert!(!denied.value["safe_message"]
        .as_str()
        .unwrap()
        .contains("Do not retry"));
    let mut second = document_arguments("second-document");
    second["payload"]["document_id"] = json!("different-document");
    assert!(
        !invoke_native(&*tool, second, "same-turn", "second")
            .await
            .is_error
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM chat_session_denied_operation"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn terminal_denial_current_state_load_error_is_internal_and_not_recorded() {
    // `native_tool` composes the Project Chat with `read_agent_chat`; the
    // registered read enforces what that surface advertises, so the identity
    // and binding must really hold it.
    let fixture = fixture_with_permissions(
        r#"{"permissions":["read_project","read_agent_chat","propose_project","propose_task","propose_commitment"]}"#,
    )
    .await;
    sqlx::query(
        "UPDATE project_agent_binding SET permission_ceiling_json = ?
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(r#"{"allowed":["read_project","read_agent_chat","propose_project","propose_task","propose_commitment"]}"#)
    .bind(PROJECT_ID)
    .execute(fixture.db.pool())
    .await
    .unwrap();
    let (scope, _) = project_chat_session(&fixture).await;
    let tool = native_tool(
        &fixture,
        scope,
        true,
        forge_agent_host::FORGE_PROJECT_ORCHESTRATION_READ_TOOL,
    );
    // Authorization still succeeds; the state projection query fails.
    sqlx::query("ALTER TABLE task RENAME COLUMN status TO unavailable_status")
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let failed = invoke_native(
        &*tool,
        json!({"operation":"project.current_state"}),
        "same-turn",
        "failed-load",
    )
    .await;
    assert!(failed.is_error);
    assert_eq!(failed.value["code"], "internal_failure", "{failed:?}");
    assert!(failed.value.get("denied_by").is_none());
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM chat_session_denied_operation"
        )
        .await,
        0
    );
    sqlx::query("ALTER TABLE task RENAME COLUMN unavailable_status TO status")
        .execute(fixture.db.pool())
        .await
        .unwrap();
    assert!(
        !invoke_native(
            &*tool,
            json!({"operation":"project.current_state"}),
            "same-turn",
            "restored-load"
        )
        .await
        .is_error
    );
}

#[tokio::test]
async fn terminal_denial_permission_grant_invalidates_reminder_and_next_turn_call_succeeds() {
    use db::ChatSessionDenialRepo;
    let fixture = fixture().await;
    let (scope, session_id) = project_chat_session(&fixture).await;
    let tool = native_tool(
        &fixture,
        scope.clone(),
        true,
        forge_agent_host::FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
    );
    sqlx::query("UPDATE project_agent_binding SET permission_ceiling_json = '{\"allowed\":[\"read_project\"]}' WHERE project_id = ? AND state = 'active'")
        .bind(PROJECT_ID).execute(fixture.db.pool()).await.unwrap();
    let denied = invoke_native(&*tool, document_arguments("denied"), "first-turn", "denied").await;
    assert_eq!(
        denied.value["denied_by"],
        "permission_missing(propose_project)"
    );
    assert_eq!(
        fixture
            .provider
            .chat_session_denials(AGENT_ID, PROFILE_ID, &scope.scope_id, Some(&session_id))
            .await
            .len(),
        1
    );
    let created_at: String =
        sqlx::query_scalar("SELECT created_at FROM chat_session_denied_operation")
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    assert!(!created_at.is_empty());
    sqlx::query("UPDATE project_agent_binding SET permission_ceiling_json = '{\"allowed\":[\"read_project\",\"propose_project\"]}' WHERE project_id = ? AND state = 'active'")
        .bind(PROJECT_ID).execute(fixture.db.pool()).await.unwrap();
    assert!(fixture
        .provider
        .chat_session_denials(AGENT_ID, PROFILE_ID, &scope.scope_id, Some(&session_id))
        .await
        .is_empty());
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM chat_session_denied_operation"
        )
        .await,
        0
    );
    // Only a reminder read into this turn's card is cleared by success.
    fixture
        .db
        .record_chat_session_denial(
            AGENT_ID,
            &scope.scope_id,
            RUNTIME_SESSION_ID,
            "project.document",
            &api_types::DeniedBy::PermissionMissing("propose_project".to_owned()),
        )
        .await
        .unwrap();
    // The card still sees a refusal; authority changes before invocation.
    sqlx::query("UPDATE project_agent_binding SET permission_ceiling_json = '{\"allowed\":[\"read_project\"]}' WHERE project_id = ? AND state = 'active'")
        .bind(PROJECT_ID).execute(fixture.db.pool()).await.unwrap();
    assert_eq!(
        fixture
            .provider
            .chat_session_denials(AGENT_ID, PROFILE_ID, &scope.scope_id, Some(&session_id))
            .await
            .len(),
        1
    );
    sqlx::query("UPDATE project_agent_binding SET permission_ceiling_json = '{\"allowed\":[\"read_project\",\"propose_project\"]}' WHERE project_id = ? AND state = 'active'")
        .bind(PROJECT_ID).execute(fixture.db.pool()).await.unwrap();
    assert!(
        !invoke_native(
            &*tool,
            document_arguments("granted"),
            "next-turn",
            "granted"
        )
        .await
        .is_error
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM chat_session_denied_operation"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn terminal_denial_bad_project_summary_id_does_not_withdraw_operation() {
    // `project.summary` is a registry read now, so the stored ceiling must
    // carry the Agent Chat read permission the composed tool already names
    // (as `agent_chat.summary` always required).
    let fixture = fixture_with_permissions(
        r#"{"permissions":["read_project","read_agent_chat","propose_project","propose_task","propose_commitment"]}"#,
    )
    .await;
    sqlx::query("INSERT INTO account_main_agent_binding (id, account_id, identity_id, profile_id, created_at, updated_at) VALUES ('summary-main', ?, ?, ?, ?, ?)")
        .bind(USER_ID).bind(AGENT_ID).bind(PROFILE_ID).bind(NOW).bind(NOW).execute(fixture.db.pool()).await.unwrap();
    let chat_id: String = sqlx::query_scalar(
        "SELECT id FROM agent_chat WHERE kind = 'account_main' AND account_id = ?",
    )
    .bind(USER_ID)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::AgentChat,
        scope_id: chat_id,
        workspace_access: WorkspaceAccess::Deny,
    };
    let tool = native_tool(&fixture, scope, false, "forge_scope_read");
    for (call, args) in [
        ("missing", json!({"operation":"project.summary"})),
        (
            "wrong",
            json!({"operation":"project.summary", "arguments":{"project_id":"deleted-or-wrong"}}),
        ),
    ] {
        let failed = invoke_native(&*tool, args, "same-turn", call).await;
        assert_eq!(failed.value["code"], "validation_error", "{failed:?}");
        assert!(failed.value.get("denied_by").is_none());
    }
    let success = invoke_native(
        &*tool,
        json!({"operation":"project.summary", "arguments":{"project_id":PROJECT_ID}}),
        "same-turn",
        "corrected",
    )
    .await;
    assert!(!success.is_error, "{success:?}");
    assert_eq!(success.value["id"], PROJECT_ID);
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM chat_session_denied_operation"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn terminal_denial_scope_permission_reminder_uses_real_policy_and_read_failures_are_advisory()
{
    use db::ChatSessionDenialRepo;
    let fixture = fixture_with_permissions(
        r#"{"permissions":["read_project","propose_project","propose_review"]}"#,
    )
    .await;
    let (scope, session_id) = project_chat_session(&fixture).await;
    // All editable ceilings allow propose_review. The static Project Chat
    // scope set does not, so this cause must remain in the card.
    sqlx::query("UPDATE project_agent_binding SET permission_ceiling_json = '{\"allowed\":[\"read_project\",\"propose_project\",\"propose_review\"]}' WHERE project_id = ? AND state = 'active'")
        .bind(PROJECT_ID).execute(fixture.db.pool()).await.unwrap();
    fixture
        .db
        .record_chat_session_denial(
            AGENT_ID,
            &scope.scope_id,
            RUNTIME_SESSION_ID,
            "review.request",
            &api_types::DeniedBy::PermissionMissing("propose_review".to_owned()),
        )
        .await
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            fixture
                .provider
                .chat_session_denials(AGENT_ID, PROFILE_ID, &scope.scope_id, Some(&session_id))
                .await,
            vec![(
                "review.request".to_owned(),
                api_types::DeniedBy::PermissionMissing("propose_review".to_owned())
            )]
        );
    }
    sqlx::query("DROP TABLE chat_session_denied_operation")
        .execute(fixture.db.pool())
        .await
        .unwrap();
    assert!(fixture
        .provider
        .chat_session_denials(AGENT_ID, PROFILE_ID, &scope.scope_id, Some(&session_id))
        .await
        .is_empty());
}

#[tokio::test]
async fn terminal_denial_success_without_card_read_does_not_delete_and_unknown_permissions_are_not_recorded(
) {
    use db::ChatSessionDenialRepo;
    let fixture = fixture().await;
    let (scope, _) = project_chat_session(&fixture).await;
    let tool = native_tool(
        &fixture,
        scope.clone(),
        true,
        forge_agent_host::FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
    );
    fixture
        .db
        .record_chat_session_denial(
            AGENT_ID,
            &scope.scope_id,
            RUNTIME_SESSION_ID,
            "project.document",
            &api_types::DeniedBy::OperationNotInScope,
        )
        .await
        .unwrap();
    assert!(
        !invoke_native(
            &*tool,
            document_arguments("no-card-read"),
            "same-turn",
            "success"
        )
        .await
        .is_error
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM chat_session_denied_operation"
        )
        .await,
        1
    );
    for permission in ["unknown", "invented_permission"] {
        fixture
            .provider
            .record_terminal_denial(
                AGENT_ID,
                &scope,
                RUNTIME_SESSION_ID,
                "task.action",
                &api_types::DeniedBy::PermissionMissing(permission.to_owned()),
            )
            .await
            .unwrap();
    }
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM chat_session_denied_operation"
        )
        .await,
        1
    );
}
