//! Direct command classification command receipts tests.

use super::*;

#[test]
fn canonical_catalog_classifies_query_direct_approval_and_denied_operations() {
    assert_eq!(
        classify_operation(PROJECT_CURRENT_STATE_OPERATION, None),
        OperationClassification::Query
    );
    assert_eq!(
        classify_operation("task.propose", None),
        OperationClassification::DirectCommand
    );
    assert_eq!(
        classify_operation(
            PROJECT_DOCUMENT_OPERATION,
            Some(&json!({"action": "draft_revision"}))
        ),
        OperationClassification::DirectCommand
    );
    assert_eq!(
        classify_operation("commitment.update", None),
        OperationClassification::ApprovalRequiredAction
    );
    let denied = operation_descriptor(CanonicalScopeType::Project, "release.execute", None);
    assert_eq!(denied.classification, OperationClassification::Denied);
    assert!(!denied.is_exposed());
    assert_eq!(denied.required_permission, None);
}

#[tokio::test]
async fn named_native_operations_return_stable_success_envelopes() {
    let fixture = fixture().await;

    let query = fixture
        .provider
        .read(
            AGENT_ID,
            &fixture.project_scope,
            PROJECT_CURRENT_STATE_OPERATION,
            json!({"limit": 10}),
        )
        .await
        .expect("structured Project query");
    assert_success_envelope(
        &query,
        PROJECT_CURRENT_STATE_OPERATION,
        "project",
        PROJECT_ID,
        "",
    );
    assert_eq!(
        query["result"]["effective_state"]["project"]["id"],
        PROJECT_ID
    );

    let direct_arguments = document_arguments("structured-direct-success-key");
    let direct = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            PROJECT_DOCUMENT_OPERATION,
            direct_arguments.clone(),
        )
        .await
        .expect("structured direct Project command");
    assert_success_envelope(
        &direct,
        PROJECT_DOCUMENT_OPERATION,
        "project",
        PROJECT_ID,
        direct_arguments["correlation_id"].as_str().unwrap(),
    );
    assert_eq!(direct["result"]["domain_committed"], true);
    assert!(direct["result"]["receipt_id"].as_str().is_some());

    let approval_arguments = approval_required_release_arguments("structured-approval-key");
    let approval = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            "project.release.request",
            approval_arguments.clone(),
        )
        .await
        .expect("structured approval proposal");
    assert_eq!(approval["code"], "approval_required");
    assert_eq!(approval["status"], "approval_required");
    assert_eq!(approval["operation"], "project.release.request");
    assert_eq!(approval["scope"]["scope_type"], "project");
    assert_eq!(approval["scope"]["scope_id"], PROJECT_ID);
    assert_eq!(
        approval["correlation_id"],
        approval_arguments["correlation_id"]
    );
    let target = &approval["approval_target"];
    assert!(target["target_type"].as_str().is_some());
    assert!(target["target_id"].as_str().is_some());
    assert_eq!(target["operation"], "project.release.request");
    assert_eq!(target["requires_user_authorization"], true);
    assert!(
        approval["result"].is_null(),
        "approval has no domain result"
    );
    assert!(approval["domain_result"].is_null());
}

#[tokio::test]
async fn query_operations_do_not_create_action_receipt_or_event() {
    let fixture = fixture().await;
    let action_count_before = count(&fixture.db, "SELECT COUNT(*) FROM agent_action").await;
    let receipt_count_before = count(&fixture.db, "SELECT COUNT(*) FROM command_receipt").await;
    let event_count_before = count(&fixture.db, "SELECT COUNT(*) FROM domain_event").await;
    let result = fixture
        .provider
        .read(
            AGENT_ID,
            &fixture.project_scope,
            PROJECT_CURRENT_STATE_OPERATION,
            json!({"limit": 10}),
        )
        .await
        .expect("Project current-state query");
    assert_success_envelope(
        &result,
        PROJECT_CURRENT_STATE_OPERATION,
        "project",
        PROJECT_ID,
        "",
    );
    assert_eq!(result["result"]["scope"], "project");
    assert_eq!(
        result["result"]["effective_state"]["project"]["id"],
        PROJECT_ID
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM agent_action").await,
        action_count_before,
        "queries never enter the approval queue"
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM command_receipt").await,
        receipt_count_before,
        "queries have no side-effecting command receipt"
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM domain_event").await,
        event_count_before,
        "queries do not append a domain event"
    );
}

#[tokio::test]
async fn shared_authorization_resolves_project_and_project_chat_targets() {
    let fixture = fixture().await;
    let authorization = OrchestrationAuthorizationService::new(Arc::clone(&fixture.db));

    assert_eq!(
        authorization
            .direct_project_target(&fixture.project_scope)
            .await
            .expect("Project direct target"),
        PROJECT_ID
    );
    assert_eq!(
        authorization
            .project_orchestration_target(AGENT_ID, &fixture.project_scope)
            .await
            .expect("Project orchestration target"),
        PROJECT_ID
    );

    let chat_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM agent_chat WHERE project_id = ? AND kind = 'project' LIMIT 1",
    )
    .bind(PROJECT_ID)
    .fetch_one(fixture.db.pool())
    .await
    .expect("Project chat");
    let chat_scope = CanonicalScope {
        scope_type: CanonicalScopeType::AgentChat,
        scope_id: chat_id,
        workspace_access: WorkspaceAccess::Deny,
    };
    assert_eq!(
        authorization
            .direct_project_target(&chat_scope)
            .await
            .expect("Project chat direct target"),
        PROJECT_ID
    );
    assert_eq!(
        authorization
            .project_orchestration_target(AGENT_ID, &chat_scope)
            .await
            .expect("Project chat orchestration target"),
        PROJECT_ID
    );

    assert!(
        authorization
            .project_orchestration_target("unknown-agent", &fixture.project_scope)
            .await
            .is_err(),
        "scope authorization must reject an actor without an active Project binding"
    );
}

#[tokio::test]
async fn allowed_project_write_and_task_proposal_use_direct_receipts_without_actions() {
    let fixture = fixture().await;
    let document = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            PROJECT_DOCUMENT_OPERATION,
            document_arguments("direct-document-key"),
        )
        .await
        .expect("direct Project document command");
    assert_success_envelope(
        &document,
        PROJECT_DOCUMENT_OPERATION,
        "project",
        PROJECT_ID,
        "correlation-direct-document-key",
    );
    assert_eq!(document["result"]["domain_committed"], true);
    assert!(result_field(&document, "receipt_id")
        .and_then(Value::as_str)
        .is_some());
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM agent_action").await,
        0,
        "allowed Project writes do not create an AgentAction"
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'project.document'",
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM project_document_revision",
        )
        .await,
        1
    );
    let document_receipt = sqlx::query(
        "SELECT event_id, agent_action_execution_id, outcome_json
         FROM command_receipt
         WHERE operation = 'project.document' AND idempotency_key = 'direct-document-key'",
    )
    .fetch_one(fixture.db.pool())
    .await
    .expect("direct document receipt");
    assert!(document_receipt
        .get::<Option<String>, _>("agent_action_execution_id")
        .is_none());
    let document_event = sqlx::query(
        "SELECT event_type, entity_type, scope_type, scope_id
         FROM domain_event WHERE id = ?",
    )
    .bind(document_receipt.get::<String, _>("event_id"))
    .fetch_one(fixture.db.pool())
    .await
    .expect("direct document event");
    assert_eq!(
        document_event.get::<String, _>("event_type"),
        "project.document.revision_created"
    );
    assert_eq!(
        document_event.get::<String, _>("entity_type"),
        "project_document_revision"
    );
    assert_eq!(document_event.get::<String, _>("scope_type"), "project");
    assert_eq!(document_event.get::<String, _>("scope_id"), PROJECT_ID);

    let task = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.task_project_scope,
            RUNTIME_SESSION_ID,
            "task.propose",
            task_arguments("direct-task-key"),
        )
        .await
        .expect("direct Task proposal command");
    assert_success_envelope(
        &task,
        "task.propose",
        "project",
        TASK_PROJECT_ID,
        "correlation-direct-task-key",
    );
    assert_eq!(task["result"]["domain_committed"], true);
    let task_id = result_field(&task, "task_id")
        .and_then(Value::as_str)
        .expect("direct Task result contains task id");
    assert!(!task_id.is_empty());
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM agent_action").await,
        0,
        "task.propose is a direct command when policy admits it"
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'task.propose'",
        )
        .await,
        1
    );
    let task_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task WHERE id = ?")
        .bind(task_id)
        .fetch_one(fixture.db.pool())
        .await
        .expect("Task count");
    assert_eq!(task_count, 1);
    let task_receipt = sqlx::query(
        "SELECT event_id, agent_action_execution_id, outcome_json
         FROM command_receipt
         WHERE operation = 'task.propose' AND idempotency_key = 'direct-task-key'",
    )
    .fetch_one(fixture.db.pool())
    .await
    .expect("direct Task receipt");
    assert!(task_receipt
        .get::<Option<String>, _>("agent_action_execution_id")
        .is_none());
    let task_event = sqlx::query(
        "SELECT event_type, entity_type, entity_id, scope_type, scope_id
         FROM domain_event WHERE id = ?",
    )
    .bind(task_receipt.get::<String, _>("event_id"))
    .fetch_one(fixture.db.pool())
    .await
    .expect("direct Task event");
    assert_eq!(task_event.get::<String, _>("event_type"), "task.created");
    assert_eq!(task_event.get::<String, _>("entity_type"), "task");
    assert_eq!(task_event.get::<String, _>("entity_id"), task_id);
    assert_eq!(task_event.get::<String, _>("scope_type"), "project");
    assert_eq!(task_event.get::<String, _>("scope_id"), TASK_PROJECT_ID);
}

#[tokio::test]
async fn direct_command_retry_returns_frozen_result_without_duplicate_effect() {
    let fixture = fixture().await;
    let first = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            PROJECT_DOCUMENT_OPERATION,
            document_arguments("direct-replay-key"),
        )
        .await
        .expect("first direct document command");
    assert_success_envelope(
        &first,
        PROJECT_DOCUMENT_OPERATION,
        "project",
        PROJECT_ID,
        "correlation-direct-replay-key",
    );
    assert_eq!(first["replayed"], false);
    let first_document_id = result_field(&first, "document_id")
        .and_then(Value::as_str)
        .expect("first document id")
        .to_owned();
    let first_revision_id = result_field(&first, "revision_id")
        .and_then(Value::as_str)
        .expect("first revision id")
        .to_owned();
    let first_receipt_id = result_field(&first, "receipt_id")
        .and_then(Value::as_str)
        .expect("first receipt id")
        .to_owned();

    // Change only the live projection.  Exact replay must return the frozen
    // command outcome rather than reconstructing a new revision or adopting
    // a changed current row.
    sqlx::query("UPDATE project_document SET title = 'Live projection changed' WHERE id = ?")
        .bind(&first_document_id)
        .execute(fixture.db.pool())
        .await
        .expect("mutate live document projection");
    let second = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            PROJECT_DOCUMENT_OPERATION,
            document_arguments("direct-replay-key"),
        )
        .await
        .expect("exact direct replay");
    assert_success_envelope(
        &second,
        PROJECT_DOCUMENT_OPERATION,
        "project",
        PROJECT_ID,
        "correlation-direct-replay-key",
    );
    assert_eq!(second["replayed"], true);
    assert_eq!(
        result_field(&second, "document_id").and_then(Value::as_str),
        Some(first_document_id.as_str())
    );
    assert_eq!(
        result_field(&second, "revision_id").and_then(Value::as_str),
        Some(first_revision_id.as_str())
    );
    assert_eq!(
        result_field(&second, "receipt_id").and_then(Value::as_str),
        Some(first_receipt_id.as_str())
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM project_document_revision",
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'project.document'",
        )
        .await,
        1
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM agent_action").await,
        0
    );
}

#[tokio::test]
async fn approval_required_release_candidate_stays_pending_without_domain_result() {
    let fixture = fixture().await;
    let pending = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            "project.release.request",
            approval_required_release_arguments("approval-required-release-key"),
        )
        .await
        .expect("approval-required operation creates pending action");
    assert_eq!(pending["code"], "approval_required");
    assert_eq!(pending["status"], "approval_required");
    assert_eq!(pending["operation"], "project.release.request");
    assert_eq!(pending["scope"]["scope_type"], "project");
    assert_eq!(pending["scope"]["scope_id"], PROJECT_ID);
    assert_eq!(
        pending["correlation_id"],
        "correlation-approval-required-release-key"
    );
    assert!(pending["approval_target"]["target_type"].as_str().is_some());
    assert!(pending["approval_target"]["target_id"].as_str().is_some());
    assert_eq!(
        pending["approval_target"]["operation"],
        "project.release.request"
    );
    assert_eq!(
        pending["approval_target"]["requires_user_authorization"],
        true
    );
    assert!(pending["result"].is_null());
    assert!(pending["domain_result"].is_null());
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM agent_action WHERE operation = 'project.release.request'",
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM agent_action WHERE status = 'pending_approval'",
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'project.release.request'",
        )
        .await,
        0,
        "pending approval has no successful command receipt"
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM project_release").await,
        0,
        "approval admission does not create a release candidate"
    );
}

/// Before the release request was registered its queueing step wrote the
/// action row first, so a request the policy denies stayed on the ledger as
/// `denied`. The registered path refuses it at admission and must still
/// leave that row; a refused call that was never queueable leaves none.
#[tokio::test]
async fn denied_release_request_is_recorded_on_the_action_ledger() {
    let fixture = fixture().await;
    sqlx::query(
        "UPDATE project_agent_binding
         SET permission_ceiling_json = '{\"allowed\":[\"read_project\"]}'
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(PROJECT_ID)
    .execute(fixture.db.pool())
    .await
    .expect("restrict Project policy");

    let mut malformed = approval_required_release_arguments("denied-release-malformed");
    malformed["payload"]["unexpected"] = json!(true);
    for (arguments, rows) in [
        (malformed, 0),
        (approval_required_release_arguments("denied-release-key"), 1),
        // A retry of the same denied call does not add a second row.
        (approval_required_release_arguments("denied-release-key"), 1),
    ] {
        let denied = fixture
            .provider
            .propose(
                AGENT_ID,
                &fixture.project_scope,
                RUNTIME_SESSION_ID,
                "project.release.request",
                arguments,
            )
            .await
            .expect_err("a release request without propose_project is denied");
        let denied = structured_error(denied);
        assert_eq!(denied.code, api_types::OutcomeCode::PolicyDenied);
        assert_eq!(
            denied.denied_by,
            Some(api_types::DeniedBy::PermissionMissing(
                "propose_project".to_owned()
            ))
        );
        assert_eq!(
            count(
                &fixture.db,
                "SELECT COUNT(*) FROM agent_action WHERE operation = 'project.release.request'",
            )
            .await,
            rows
        );
    }
    let (status, policy_result, permission, target_type, target_id): (
        String,
        String,
        String,
        String,
        String,
    ) = sqlx::query_as(
        "SELECT status, policy_result, requested_permission, target_type, target_id
         FROM agent_action WHERE dedupe_key = 'denied-release-key'",
    )
    .fetch_one(fixture.db.pool())
    .await
    .expect("denied release request row");
    assert_eq!(
        (
            status.as_str(),
            policy_result.as_str(),
            permission.as_str(),
            target_type.as_str(),
            target_id.as_str()
        ),
        ("denied", "denied", "propose_project", "project", PROJECT_ID)
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM command_receipt").await,
        0
    );
}

#[tokio::test]
async fn denied_operation_is_not_admitted_and_cannot_mutate() {
    let fixture = fixture().await;
    let revision_count_before = count(
        &fixture.db,
        "SELECT COUNT(*) FROM project_document_revision",
    )
    .await;
    let receipt_count_before = count(
        &fixture.db,
        "SELECT COUNT(*) FROM command_receipt WHERE idempotency_key = 'denied-document-key'",
    )
    .await;
    let event_count_before = count(&fixture.db, "SELECT COUNT(*) FROM domain_event").await;
    sqlx::query(
        "UPDATE project_agent_binding
         SET permission_ceiling_json = '{\"allowed\":[\"read_project\"]}'
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(PROJECT_ID)
    .execute(fixture.db.pool())
    .await
    .expect("restrict Project policy");

    let denied = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            PROJECT_DOCUMENT_OPERATION,
            document_arguments("denied-document-key"),
        )
        .await;
    let denied = match denied {
        Err(error) => structured_error(error),
        Ok(value) => panic!("policy denial must not be a successful envelope: {value}"),
    };
    assert_eq!(denied.code, api_types::OutcomeCode::PolicyDenied);
    assert_eq!(denied.status, api_types::OutcomeStatus::Failed);
    assert_eq!(denied.operation, PROJECT_DOCUMENT_OPERATION);
    assert_eq!(
        denied.scope.scope_type,
        api_types::OutcomeScopeType::Project
    );
    assert_eq!(denied.scope.scope_id, PROJECT_ID);
    assert_eq!(denied.correlation_id, "correlation-denied-document-key");
    assert!(denied.result.is_none());
    assert!(denied.approval_target.is_none());
    assert!(
        !denied.safe_message.contains("read_project"),
        "policy details must not reach the model"
    );
    assert_eq!(
        denied
            .denied_by
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("permission_missing(propose_project)")
    );
    assert!(denied.safe_message.contains("Do not retry"));
    assert_eq!(
        denied.retry.as_ref().unwrap().action,
        api_types::RetryAction::None
    );
    assert_eq!(
        denied.retry.as_ref().unwrap().scope,
        Some(api_types::RetryScope::Session)
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM project_document_revision",
        )
        .await,
        revision_count_before
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt WHERE idempotency_key = 'denied-document-key'",
        )
        .await,
        receipt_count_before
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM domain_event").await,
        event_count_before
    );
}

#[tokio::test]
async fn authorized_stale_milestone_returns_typed_version_correction() {
    let fixture = fixture().await;
    sqlx::query("UPDATE project_milestone SET version = 2 WHERE id = ?")
        .bind(MILESTONE_ID)
        .execute(fixture.db.pool())
        .await
        .expect("advance authorized milestone version");

    let arguments = stale_milestone_arguments("stale-milestone-key", 1);
    let error = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            PROJECT_MILESTONE_OPERATION,
            arguments.clone(),
        )
        .await
        .expect_err("stale milestone must be a structured conflict");
    let outcome = structured_error(error);

    assert_eq!(outcome.code, api_types::OutcomeCode::VersionConflict);
    assert_eq!(outcome.status, api_types::OutcomeStatus::Failed);
    assert_eq!(outcome.operation, PROJECT_MILESTONE_OPERATION);
    assert_eq!(
        outcome.scope.scope_type,
        api_types::OutcomeScopeType::Project
    );
    assert_eq!(outcome.scope.scope_id, PROJECT_ID);
    assert_eq!(outcome.correlation_id, "correlation-stale-milestone-key");
    let current = outcome
        .current_version_or_revision
        .expect("authorized current milestone state");
    assert_eq!(current.resource_type, "project_milestone");
    assert_eq!(current.resource_id, MILESTONE_ID);
    assert_eq!(current.version, Some(2));
    assert_eq!(current.revision_id.as_deref(), Some(MILESTONE_REVISION_ID));
    assert_eq!(current.revision, Some(1));
    let retry = outcome.retry.expect("typed stale retry");
    assert_eq!(retry.action, api_types::RetryAction::RefreshAndRetry);
    assert!(retry.retryable);
    assert_eq!(
        retry.arguments.get("expected_milestone_version"),
        Some(&json!(2))
    );
    assert!(outcome.result.is_none());
    assert!(
        !outcome.safe_message.contains("milestone-content"),
        "raw persistence details must not reach the model"
    );
}

#[tokio::test]
async fn idempotency_mismatch_has_no_current_state_disclosure() {
    let fixture = fixture().await;
    let first_arguments = document_arguments("idempotency-mismatch-key");
    fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            PROJECT_DOCUMENT_OPERATION,
            first_arguments,
        )
        .await
        .expect("initial direct document command");

    let mut changed_arguments = document_arguments("idempotency-mismatch-key");
    changed_arguments["payload"]["title"] = json!("Changed command input");
    let error = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.project_scope,
            RUNTIME_SESSION_ID,
            PROJECT_DOCUMENT_OPERATION,
            changed_arguments,
        )
        .await
        .expect_err("changed input on a bound key must conflict");
    let outcome = structured_error(error);

    assert_eq!(outcome.code, api_types::OutcomeCode::IdempotencyConflict);
    assert_eq!(outcome.status, api_types::OutcomeStatus::Failed);
    assert_eq!(outcome.operation, PROJECT_DOCUMENT_OPERATION);
    assert_eq!(
        outcome.scope.scope_type,
        api_types::OutcomeScopeType::Project
    );
    assert_eq!(outcome.scope.scope_id, PROJECT_ID);
    assert_eq!(
        outcome.correlation_id,
        "correlation-idempotency-mismatch-key"
    );
    assert!(outcome.current_version_or_revision.is_none());
    assert!(outcome.result.is_none());
    let retry = outcome.retry.expect("new key retry");
    assert_eq!(retry.action, api_types::RetryAction::UseNewIdempotencyKey);
    assert!(!retry.retryable);
    assert!(retry.arguments.is_empty());
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM project_document_revision",
        )
        .await,
        1,
        "an idempotency mismatch must not create another revision"
    );
}

#[tokio::test]
async fn queued_retry_for_paused_worker_does_not_withdraw_cancellation_for_another_task() {
    let (fixture, _temp, worker, task_a, task_b) = recovery_fixture().await;
    let (scope, _) = project_chat_session(&fixture).await;
    let tool = native_tool(&fixture, scope, true, "forge_scope_propose");
    sqlx::query("UPDATE agent_identity SET paused = 1 WHERE id = ?")
        .bind(worker)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let refused = invoke_native(
        &*tool,
        recovery_arguments(&fixture, &task_a, "retry", "paused-worker").await,
        "same-turn",
        "paused-worker",
    )
    .await;
    assert!(refused.is_error, "paused worker refuses retry: {refused:?}");
    assert_eq!(refused.value["denied_by"], "target_agent_paused");
    assert_eq!(refused.value["retry"]["scope"], "turn");
    assert!(refused.value["details"]["available_actions"]
        .as_array()
        .unwrap()
        .iter()
        .all(|offer| offer["action"]["verb"] != "retry"));
    let recovered = invoke_native(
        &*tool,
        recovery_arguments(&fixture, &task_b, "cancel", "other-task").await,
        "same-turn",
        "other-task",
    )
    .await;
    assert!(!recovered.is_error, "{recovered:?}");
    assert_eq!(recovered.value["result"]["task_status"], "cancelled");
}

#[tokio::test]
async fn queued_retry_for_paused_project_does_not_withdraw_cancellation() {
    let (fixture, _temp, _worker, task_a, _) = recovery_fixture().await;
    let (scope, _) = project_chat_session(&fixture).await;
    let tool = native_tool(&fixture, scope, true, "forge_scope_propose");
    sqlx::query("UPDATE project SET paused_at = 'now', system_pause_reason = 'environment_not_ready' WHERE id = ?")
        .bind(PROJECT_ID).execute(fixture.db.pool()).await.unwrap();
    let refused = invoke_native(
        &*tool,
        recovery_arguments(&fixture, &task_a, "retry", "paused-project").await,
        "same-turn",
        "paused-project",
    )
    .await;
    assert!(
        refused.is_error,
        "paused Project refuses retry: {refused:?}"
    );
    assert_eq!(
        refused.value["denied_by"],
        "project_paused(environment_not_ready)"
    );
    assert_eq!(refused.value["retry"]["scope"], "turn");
    let recovered = invoke_native(
        &*tool,
        recovery_arguments(&fixture, &task_a, "cancel", "cancel-paused-task").await,
        "same-turn",
        "cancel-paused-task",
    )
    .await;
    assert!(!recovered.is_error, "{recovered:?}");
    assert_eq!(recovered.value["result"]["task_status"], "cancelled");
}
