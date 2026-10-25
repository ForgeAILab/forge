//! Terminal denial state cards, evaluator causes, and permissions.

use super::*;

#[tokio::test]
async fn terminal_denial_next_turn_card_keeps_nonclearing_causes_and_rechecks_pauses() {
    use db::{AgentChatTurnJobRepo, ChatSessionDenialRepo};
    use std::sync::Arc;
    let workspace = common::TestDir::new("terminal-denial-state-card");
    let harness = common::test_app(workspace.path(), "terminal-denial-state-card").await;
    let fixture = create_genesis_project(
        &harness.app,
        &common::test_jwt(),
        "terminal-denial-state-card",
    )
    .await;
    let first_turn = admit_preview_turn(
        &harness,
        &fixture.project_chat_id,
        "Check current operations",
    )
    .await;
    let first = chat_preview(&harness, &fixture.project_chat_id, &first_turn).await;
    let runner = services_agents::FederatedAgentChatTurnRunner::new(
        Arc::clone(&harness.state.db),
        Arc::clone(&harness.state.embedded_agent_service),
        Arc::clone(&harness.state.task_executor),
        harness.state.agent_chat_turn_logs.clone(),
    );
    let job = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*harness.state.db, &first_turn)
        .await
        .unwrap()
        .unwrap();
    runner.record_prompt_manifest_for_test(&job).await.unwrap();
    let (session_id, runtime_id): (String, String) = sqlx::query_as(
        "SELECT s.id, s.runtime_session_id FROM agent_session s JOIN agent_context_scope c ON c.id = s.context_scope_id WHERE c.scope_id = ? AND s.identity_id = ? AND s.status IN ('starting', 'ready', 'running', 'degraded')"
    ).bind(&fixture.project_chat_id).bind(&fixture.project_identity_id)
        .fetch_one(harness.state.db.pool()).await.unwrap();
    let provider = services_agents::CoordinationToolProvider::new(Arc::clone(&harness.state.db));
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::AgentChat,
        scope_id: fixture.project_chat_id.clone(),
        workspace_access: WorkspaceAccess::ProjectVerify,
    };
    for (operation, cause) in [
        ("task.action", "permission_missing(propose_task)"),
        ("task.review", "operation_not_in_scope"),
        ("task.propose", "charter_not_adopted"),
        ("task.action", "project_paused(old detail)"),
        ("task.adaptive", "identity_paused"),
    ] {
        provider
            .record_terminal_denial(
                &fixture.project_identity_id,
                &scope,
                &runtime_id,
                operation,
                &cause.parse::<api_types::DeniedBy>().unwrap(),
            )
            .await
            .unwrap();
    }
    // A forged chat/session pairing cannot record a denial for another chat.
    harness
        .state
        .db
        .record_chat_session_denial(
            &fixture.project_identity_id,
            &fixture.main_chat_id,
            &session_id,
            "message.send",
            &api_types::DeniedBy::OperationNotInScope,
        )
        .await
        .unwrap();
    sqlx::query("UPDATE project SET paused_at = 'now', system_pause_reason = 'environment_not_ready' WHERE id = ?")
        .bind(&fixture.project_id).execute(harness.state.db.pool()).await.unwrap();
    sqlx::query("UPDATE agent_identity SET paused = 1 WHERE id = ?")
        .bind(&fixture.project_identity_id)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let paused = chat_preview(&harness, &fixture.project_chat_id, &first_turn).await;
    assert!(paused.input_parts[1].contains("task.action (project_paused(environment_not_ready))"));
    assert!(paused.input_parts[1].contains("task.adaptive (identity_paused)"));
    assert!(!paused.input_parts[1].contains("old detail"));
    assert_eq!(first.system_prompt, paused.system_prompt);
    sqlx::query("UPDATE project SET paused_at = NULL, system_pause_reason = NULL WHERE id = ?")
        .bind(&fixture.project_id)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE agent_identity SET paused = 0 WHERE id = ?")
        .bind(&fixture.project_identity_id)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let second_turn = admit_preview_turn(
        &harness,
        &fixture.project_chat_id,
        "Check current operations again",
    )
    .await;
    let second = chat_preview(&harness, &fixture.project_chat_id, &second_turn).await;
    for entry in [
        "task.action (permission_missing(propose_task))",
        "task.propose (charter_not_adopted)",
    ] {
        assert!(
            !second.input_parts[1].contains(entry),
            "live authority already permits {entry}"
        );
    }
    assert!(second.input_parts[1].contains("### Unavailable in this session"));
    assert!(second.input_parts[1].contains("task.review (operation_not_in_scope)"));
    assert!(!second.input_parts[1].contains("task.action (project_paused"));
    assert!(!second.input_parts[1].contains("task.adaptive (identity_paused)"));
    assert!(!second.input_parts[1].contains("message.send (operation_not_in_scope)"));
    assert_eq!(
        first.system_prompt.as_ref().unwrap().as_bytes(),
        second.system_prompt.as_ref().unwrap().as_bytes()
    );
    assert_preview_state_is_data(&second);
    // Session rotation/profile replacement must not inherit old reminders.
    sqlx::query("UPDATE agent_session SET status = 'replaced' WHERE id = ?")
        .bind(session_id)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let rotated = chat_preview(&harness, &fixture.project_chat_id, &second_turn).await;
    assert!(!rotated.input_parts[1].contains("### Unavailable in this session"));
}

#[tokio::test]
async fn terminal_denial_main_genesis_guards_keep_real_evaluator_causes() {
    use forge_agent_host::{
        CanonicalScope, CanonicalScopeType, ForgeToolProvider, WorkspaceAccess,
    };
    use std::sync::Arc;
    let workspace = common::TestDir::new("terminal-denial-genesis-causes");
    let harness = common::test_app(workspace.path(), "terminal-denial-genesis-causes").await;
    let fixture = create_genesis_project(
        &harness.app,
        &common::test_jwt(),
        "terminal-denial-genesis-causes",
    )
    .await;
    let main = connect_agent(
        &harness.app,
        &common::test_jwt(),
        "genesis-guard-main",
        &["read_account", "read_agent_chat", "propose_discovery"],
    )
    .await;
    let main_identity = required_string(&main, &["agent", "id"]);
    let binding_version: i64 = sqlx::query_scalar("SELECT version FROM account_main_agent_binding WHERE account_id = 'test-user-id' AND state = 'active'")
        .fetch_one(harness.state.db.pool()).await.unwrap();
    request_json(&harness.app, Method::PUT, "/api/v1/account/main-agent", &common::test_jwt(),
        json!({"identity_id":main_identity, "profile_id":main["profile"]["id"], "expected_version":binding_version, "autonomy_policy":{}}),
        &[StatusCode::OK]).await;
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::AgentChat,
        scope_id: fixture.main_chat_id.clone(),
        workspace_access: WorkspaceAccess::Deny,
    };
    let provider = services_agents::CoordinationToolProvider::new(Arc::clone(&harness.state.db));
    let arguments = json!({"operation":"genesis.start", "payload":{}, "dedupe_key":"genesis-guard-denial", "correlation_id":"genesis-guard-denial"});
    let refusal = provider
        .propose(
            &main_identity,
            &scope,
            "main-runtime",
            "genesis.start",
            arguments.clone(),
        )
        .await
        .unwrap_err();
    let forge_agent_host::AgentHostError::StructuredOutcome(refusal) = refusal else {
        panic!("typed denial")
    };
    assert_eq!(
        refusal.denied_by,
        Some(api_types::DeniedBy::LeasedTurnRequired)
    );
    let turn_id = admit_preview_turn(
        &harness,
        &fixture.main_chat_id,
        "Explain the current Project status",
    )
    .await;
    sqlx::query("UPDATE agent_chat_turn_job SET status = 'leased', lease_owner = 'test', leased_until = '2099-01-01T00:00:00Z' WHERE id = ?")
        .bind(turn_id).execute(harness.state.db.pool()).await.unwrap();
    let refusal = provider
        .propose(
            &main_identity,
            &scope,
            "main-runtime",
            "genesis.start",
            arguments,
        )
        .await
        .unwrap_err();
    let forge_agent_host::AgentHostError::StructuredOutcome(refusal) = refusal else {
        panic!("typed denial")
    };
    assert_eq!(
        refusal.denied_by,
        Some(api_types::DeniedBy::UserRequestRequired)
    );
    assert_eq!(
        refusal.retry.unwrap().scope,
        Some(api_types::RetryScope::Turn)
    );
}

#[tokio::test]
async fn terminal_denial_permission_grant_removes_next_turn_card_entry_and_preserves_system_bytes()
{
    use db::{AgentChatTurnJobRepo, ChatSessionDenialRepo};
    use forge_agent_host::{
        CanonicalScope, CanonicalScopeType, ForgeToolProvider, WorkspaceAccess,
    };
    use std::sync::Arc;
    let workspace = common::TestDir::new("terminal-denial-permission-grant");
    let harness = common::test_app(workspace.path(), "terminal-denial-permission-grant").await;
    let fixture = create_genesis_project(
        &harness.app,
        &common::test_jwt(),
        "terminal-denial-permission-grant",
    )
    .await;
    let original_ceiling: String = sqlx::query_scalar("SELECT permission_ceiling_json FROM project_agent_binding WHERE project_id = ? AND state = 'active'")
        .bind(&fixture.project_id).fetch_one(harness.state.db.pool()).await.unwrap();
    sqlx::query("UPDATE project_agent_binding SET permission_ceiling_json = '{\"permissions\":[\"read_project\"]}' WHERE project_id = ? AND state = 'active'")
        .bind(&fixture.project_id).execute(harness.state.db.pool()).await.unwrap();
    let first_turn =
        admit_preview_turn(&harness, &fixture.project_chat_id, "Check available work").await;
    let job = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*harness.state.db, &first_turn)
        .await
        .unwrap()
        .unwrap();
    let runner = services_agents::FederatedAgentChatTurnRunner::new(
        Arc::clone(&harness.state.db),
        Arc::clone(&harness.state.embedded_agent_service),
        Arc::clone(&harness.state.task_executor),
        harness.state.agent_chat_turn_logs.clone(),
    );
    runner.record_prompt_manifest_for_test(&job).await.unwrap();
    let runtime_id: String = sqlx::query_scalar("SELECT s.runtime_session_id FROM agent_session s JOIN agent_context_scope c ON c.id = s.context_scope_id WHERE c.scope_id = ? AND s.status IN ('starting','ready','running','degraded')")
        .bind(&fixture.project_chat_id).fetch_one(harness.state.db.pool()).await.unwrap();
    let provider = services_agents::CoordinationToolProvider::new(Arc::clone(&harness.state.db));
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::AgentChat,
        scope_id: fixture.project_chat_id.clone(),
        workspace_access: WorkspaceAccess::Deny,
    };
    let arguments = json!({"operation":"project.document", "payload":{
        "action":"draft_revision", "document_id":"permission-grant-document", "kind":"research",
        "title":"Permission grant check", "expected_document_version":1, "base_revision_id":null,
        "content":{"question":"Is the permission granted?", "decision_informed":"Next available work",
            "scope":"This Project", "stopping_condition":"The document is recorded", "sources":[],
            "findings":[], "evidence":[], "inferences":[], "alternatives":[], "recommendation":null,
            "uncertainty":[], "unresolved_questions":[], "affected_artifact_ids":[], "affected_decision_ids":[]}},
        "dedupe_key":"permission-grant-document", "correlation_id":"permission-grant-document"});
    let refusal = provider
        .propose(
            &fixture.project_identity_id,
            &scope,
            &runtime_id,
            "project.document",
            arguments.clone(),
        )
        .await
        .unwrap_err();
    let forge_agent_host::AgentHostError::StructuredOutcome(refusal) = refusal else {
        panic!("typed policy refusal")
    };
    assert_eq!(
        refusal.denied_by,
        Some(api_types::DeniedBy::PermissionMissing(
            "propose_project".to_owned()
        ))
    );
    provider
        .record_terminal_denial(
            &fixture.project_identity_id,
            &scope,
            &runtime_id,
            "project.document",
            refusal.denied_by.as_ref().unwrap(),
        )
        .await
        .unwrap();
    let denied_preview = chat_preview(&harness, &fixture.project_chat_id, &first_turn).await;
    assert!(denied_preview.input_parts[1].contains("### Unavailable in this session"));
    assert!(denied_preview.input_parts[1]
        .contains("project.document (permission_missing(propose_project))"));
    sqlx::query("UPDATE project_agent_binding SET permission_ceiling_json = ? WHERE project_id = ? AND state = 'active'")
        .bind(original_ceiling).bind(&fixture.project_id).execute(harness.state.db.pool()).await.unwrap();
    let second_turn = admit_preview_turn(
        &harness,
        &fixture.project_chat_id,
        "Continue with the granted permission",
    )
    .await;
    let granted_preview = chat_preview(&harness, &fixture.project_chat_id, &second_turn).await;
    assert!(!granted_preview.input_parts[1].contains("project.document (permission_missing"));
    assert_eq!(denied_preview.system_prompt, granted_preview.system_prompt);
    let remaining: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM chat_session_denied_operation WHERE operation = 'project.document'",
    )
    .fetch_one(harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(remaining, 0);
    let success = provider
        .propose(
            &fixture.project_identity_id,
            &scope,
            &runtime_id,
            "project.document",
            arguments,
        )
        .await
        .unwrap();
    assert_eq!(success["code"], "ok");
    // Turn-only causes never enter durable state.
    harness
        .state
        .db
        .record_chat_session_denial(
            &fixture.project_identity_id,
            &scope.scope_id,
            &runtime_id,
            "project.document",
            &api_types::DeniedBy::TaskTerminal,
        )
        .await
        .unwrap();
    assert!(harness
        .state
        .db
        .chat_session_denials(
            &fixture.project_identity_id,
            &job.profile_id.unwrap(),
            &scope.scope_id,
            None
        )
        .await
        .unwrap()
        .is_empty());
}
