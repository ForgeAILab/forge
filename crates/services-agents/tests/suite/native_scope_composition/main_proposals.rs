//! Tests for main proposals in native scope composition.

use super::*;
/// The baseline is the unchanged command boundary called with the exact old
/// adapter's decoded request/envelope (gate main_genesis_commands.rs:3482).
/// The registry must replay that receipt, rather than create a second effect.
#[tokio::test]
async fn main_selection_registry_replays_base_command_receipt_and_preserves_effect() {
    use forge_agent_host::ForgeToolProvider;
    let f = fixture(false).await;
    let base = capture_base_selection(&f).await;
    let frozen_base_receipt: String =
        sqlx::query_scalar("SELECT outcome_json FROM command_receipt WHERE id = ?")
            .bind(&base.receipt_id)
            .fetch_one(f.db.pool())
            .await
            .unwrap();
    // Written from the gate, not emitted by new normalization/preparation.
    let stored: Value = serde_json::from_str(r#"{"operation":"genesis.project_agent.select","payload":{"action":"select","genesis_session_id":"scope-composition-genesis","expected_session_version":1,"project_agent_identity_id":"scope-composition-project-agent-candidate"},"dedupe_key":"pre-change-selection","correlation_id":"pre-change-correlation","causation_id":"pre-change-cause","causation_depth":1}"#).unwrap();
    for prepared in [false, true] {
        let outcome = if prepared {
            ForgeToolProvider::propose_prepared(
                &f.provider,
                AGENT_ID,
                &f.main_scope,
                "session",
                MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
                stored.clone(),
            )
            .await
        } else {
            ForgeToolProvider::propose(
                &f.provider,
                AGENT_ID,
                &f.main_scope,
                "session",
                MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
                stored.clone(),
            )
            .await
        }
        .unwrap();
        assert_eq!(outcome["receipt_id"], base.receipt_id);
        assert_eq!(outcome["result"]["event_id"], base.event_id);
        let mut base_replay = base.result.clone();
        base_replay["replayed"] = json!(true);
        assert_eq!(outcome["result"]["domain_result"], base_replay);
        let frozen: String =
            sqlx::query_scalar("SELECT outcome_json FROM command_receipt WHERE id = ?")
                .bind(&base.receipt_id)
                .fetch_one(f.db.pool())
                .await
                .unwrap();
        assert_eq!(frozen, frozen_base_receipt);
        assert_eq!(outcome["replayed"], true);
    }
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM command_receipt WHERE operation = 'genesis.project_agent.select'",
    )
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert_eq!(count, 1);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_event WHERE event_type = 'product_genesis.project_agent_selected'").fetch_one(f.db.pool()).await.unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn main_proposal_dispatch_denies_unbound_and_forged_callers_before_contract_detail() {
    use forge_agent_host::ForgeToolProvider;
    let f = fixture(false).await;
    for operation in operation_registry::main_proposals::IDS {
        for actor in [AGENT_ID, PROJECT_AGENT_CANDIDATE_ID] {
            for field in ["identity_id", "authority", "unexpected"] {
                let mut args = json!({"operation":operation,"payload":{field:"forged"},"dedupe_key":"denial-key","correlation_id":"corr"});
                if field == "unexpected" {
                    args["payload"] = json!(null);
                }
                let composition = ScopeToolComposition::for_scope_with_permissions(
                    actor,
                    f.main_scope.clone(),
                    None,
                    None,
                    &broad_permissions(),
                    Some(Arc::new(f.provider.clone())),
                )
                .unwrap();
                let tool = composition
                    .tools()
                    .into_iter()
                    .find(|tool| tool.spec().name == FORGE_MAIN_ORCHESTRATION_PROPOSE_TOOL)
                    .unwrap();
                let prepared_error = tool
                    .prepare(args.clone(), &preparation_context("denial-order"))
                    .await
                    .unwrap_err()
                    .to_string();
                assert_eq!(
                    prepared_error.contains("expected "),
                    actor == AGENT_ID && field == "unexpected",
                    "{operation} {actor} {field}: {prepared_error}"
                );
                let error = ForgeToolProvider::propose(
                    &f.provider,
                    actor,
                    &f.main_scope,
                    "session",
                    operation,
                    args,
                )
                .await
                .unwrap_err();
                let text = format!("{error:?}");
                if actor != AGENT_ID || field != "unexpected" {
                    assert!(
                        !text.contains("expected "),
                        "{operation} {actor} {field}: {text}"
                    );
                    let forge_agent_host::AgentHostError::StructuredOutcome(outcome) = error else {
                        panic!("{text}");
                    };
                    assert_eq!(
                        serde_json::to_value(outcome).unwrap()["code"],
                        "policy_denied"
                    );
                } else {
                    assert!(
                        text.contains(operation) && text.contains("expected "),
                        "{text}"
                    );
                }
            }
        }
    }
    // A revoked current policy also denies valid and malformed payloads equally.
    sqlx::query("UPDATE agent_identity SET account_permission_ceiling = '{}' WHERE id = ?")
        .bind(AGENT_ID)
        .execute(f.db.pool())
        .await
        .unwrap();
    for operation in operation_registry::main_proposals::IDS {
        let error = ForgeToolProvider::propose(
            &f.provider,
            AGENT_ID,
            &f.main_scope,
            "session",
            operation,
            json!({"payload":{},"dedupe_key":"denial-key","correlation_id":"corr"}),
        )
        .await
        .unwrap_err();
        assert!(!format!("{error:?}").contains("expected "));
    }
}

#[tokio::test]
async fn main_create_registry_preserves_base_pending_proposal_and_exact_dedupe() {
    use forge_agent_host::ForgeToolProvider;
    let f = fixture(false).await;
    // Capture the original enqueue boundary's values, without the registry.
    let payload = r#"{"action":"create_from_approval","approval_id":"pre-change-approval"}"#;
    let actions = services::AgentActionService::new(f.db.clone());
    let base = actions
        .propose(services::ProposeActionInput {
            id: Some("pre-change-main-create-action".into()),
            actor_identity_id: AGENT_ID.into(),
            scope_type: "account".into(),
            scope_id: USER_ID.into(),
            operation: MAIN_PROJECT_CREATE_OPERATION.into(),
            payload_json: payload.into(),
            dedupe_key: "pre-change-create".into(),
            correlation_id: "pre-change-correlation".into(),
            causation_id: Some("pre-change-cause".into()),
            causation_depth: 1,
            requested_permission: "propose_project".into(),
            policy_reason: None,
            target_type: Some("account".into()),
            target_id: Some(USER_ID.into()),
        })
        .await
        .unwrap();
    assert_eq!(
        base.policy_result,
        db::AgentActionPolicyResult::ApprovalRequired
    );
    assert_eq!(base.status, db::AgentActionStatus::PendingApproval);
    let stored: Value = serde_json::from_str(r#"{"operation":"project.create","payload":{"action":"create_from_approval","approval_id":"pre-change-approval"},"dedupe_key":"pre-change-create","correlation_id":"pre-change-correlation","causation_id":"pre-change-cause","causation_depth":1}"#).unwrap();
    for prepared in [false, true] {
        let outcome = if prepared {
            ForgeToolProvider::propose_prepared(
                &f.provider,
                AGENT_ID,
                &f.main_scope,
                "session",
                MAIN_PROJECT_CREATE_OPERATION,
                stored.clone(),
            )
            .await
        } else {
            ForgeToolProvider::propose(
                &f.provider,
                AGENT_ID,
                &f.main_scope,
                "session",
                MAIN_PROJECT_CREATE_OPERATION,
                stored.clone(),
            )
            .await
        }
        .unwrap();
        assert_eq!(outcome["code"], "approval_required");
        assert_eq!(outcome["status"], "approval_required");
        assert_eq!(outcome["approval_target"]["target_id"], USER_ID);
        assert!(outcome["receipt_id"].is_null());
        let replay = db::AgentActionRepo::get_action(&*f.db, &base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay, base);
    }
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_action WHERE operation = 'project.create'")
            .fetch_one(f.db.pool())
            .await
            .unwrap();
    assert_eq!(count, 1);
    // There is no command receipt or domain creation from a direct proposal.
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM command_receipt WHERE operation = 'project.create'",
    )
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert_eq!(count, 0);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM project_charter_approval")
        .fetch_one(f.db.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn main_selection_literal_checkpoints_resume_real_provider_and_same_base_receipt() {
    let fixtures: Value = serde_json::from_str(include_str!(
        "../../../../services/tests/common/main_proposal_checkpoints_pre_change.json"
    ))
    .unwrap();
    for form in ["prepared", "approval_pending", "approval_edited"] {
        let f = fixture(false).await;
        let base = capture_base_selection(&f).await;
        let original: (String, String, String) =
            sqlx::query_as("SELECT id,event_id,outcome_json FROM command_receipt WHERE id = ?")
                .bind(&base.receipt_id)
                .fetch_one(f.db.pool())
                .await
                .unwrap();
        let composition = ScopeToolComposition::for_scope_with_permissions(
            AGENT_ID,
            f.main_scope.clone(),
            None,
            None,
            &BTreeSet::from(["propose_discovery".into()]),
            Some(Arc::new(f.provider.clone())),
        )
        .unwrap();
        pre_change_main_runtime::resume(
            composition,
            &fixtures[format!("genesis.project_agent.select:{form}")]["checkpoint"],
        )
        .await;
        let replay: (String, String, String) =
            sqlx::query_as("SELECT id,event_id,outcome_json FROM command_receipt WHERE id = ?")
                .bind(&base.receipt_id)
                .fetch_one(f.db.pool())
                .await
                .unwrap();
        assert_eq!(original, replay, "{form}: exact base receipt bytes");
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'genesis.project_agent.select'",
        )
        .fetch_one(f.db.pool())
        .await
        .unwrap();
        assert_eq!(count, 1);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_event WHERE event_type = 'product_genesis.project_agent_selected'").fetch_one(f.db.pool()).await.unwrap();
        assert_eq!(count, 1);
    }
}

/// The base provider read authority replacements only from payload. These
/// envelope fields were ignored by direct provider callers; the registry's
/// dispatch guard now rejects them, just as named native preparation already did.
#[tokio::test]
async fn main_proposal_root_authority_fields_are_denied_before_payload_contracts() {
    use forge_agent_host::ForgeToolProvider;
    let f = fixture(false).await;
    let fields = [
        "actor_identity_id",
        "identity_id",
        "scope_type",
        "scope_id",
        "project_id",
        "authority",
        "permission",
        "workspace",
        "workspace_path",
        "workspace_lease",
        "repository_path",
        "repository_url",
        "credential",
        "target_type",
        "target_id",
    ];
    for operation in operation_registry::main_proposals::IDS {
        for actor in [AGENT_ID, PROJECT_AGENT_CANDIDATE_ID] {
            let composition = ScopeToolComposition::for_scope_with_permissions(
                actor,
                f.main_scope.clone(),
                None,
                None,
                &broad_permissions(),
                Some(Arc::new(f.provider.clone())),
            )
            .unwrap();
            let tool = composition
                .tools()
                .into_iter()
                .find(|tool| tool.spec().name == FORGE_MAIN_ORCHESTRATION_PROPOSE_TOOL)
                .unwrap();
            for field in fields {
                let mut arguments = json!({"operation":operation,"payload":null,"dedupe_key":"root-authority","correlation_id":"root-correlation"});
                arguments[field] = json!("ignored-envelope-value");
                // Even a malformed payload does not reveal contract details.
                let error = tool
                    .prepare(arguments.clone(), &preparation_context("root-authority"))
                    .await
                    .unwrap_err()
                    .to_string();
                assert!(
                    !error.contains("expected "),
                    "{operation} {actor} {field}: {error}"
                );
                let error = ForgeToolProvider::propose(
                    &f.provider,
                    actor,
                    &f.main_scope,
                    "session",
                    operation,
                    arguments,
                )
                .await
                .unwrap_err();
                let forge_agent_host::AgentHostError::StructuredOutcome(outcome) = error else {
                    panic!("unexpected error");
                };
                assert_eq!(
                    serde_json::to_value(&*outcome).unwrap()["code"],
                    "policy_denied"
                );
                assert!(!outcome.safe_message.contains("expected "));
            }
        }
    }
}

/// Selection through the provider: the former `action` is optional, an
/// integer spelling of the session version reaches the command as the integer,
/// both forms hit the one receipt, and a stale version is refused by the
/// unchanged command service with no second receipt.
#[tokio::test]
async fn main_selection_dispatch_accepts_spellings_and_refuses_a_stale_version() {
    use forge_agent_host::ForgeToolProvider;
    let f = fixture(false).await;
    let call = |version: Value, action: Option<&str>, key: &str| {
        let mut payload = json!({
            "genesis_session_id": MAIN_GENESIS_ID,
            "expected_session_version": version,
            "project_agent_identity_id": PROJECT_AGENT_CANDIDATE_ID
        });
        if let Some(action) = action {
            payload["action"] = json!(action);
        }
        json!({"operation":MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,"payload":payload,"dedupe_key":key,"correlation_id":"spelling-correlation"})
    };
    let mut receipts = Vec::new();
    for (version, action) in [
        (json!(1), None),
        (json!("1"), Some("select")),
        (json!(1.0), Some("anything")),
    ] {
        let outcome = ForgeToolProvider::propose(
            &f.provider,
            AGENT_ID,
            &f.main_scope,
            "session",
            MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
            call(version, action, "spelling-key"),
        )
        .await
        .unwrap();
        assert_eq!(outcome["code"], "ok", "{outcome}");
        receipts.push((
            outcome["receipt_id"].clone(),
            outcome["result"]["event_id"].clone(),
        ));
    }
    assert!(receipts[0].0.is_string());
    assert!(receipts.iter().all(|receipt| receipt == &receipts[0]));
    // The session moved to version 2. A new key at version 1 is stale.
    let stale = ForgeToolProvider::propose(
        &f.provider,
        AGENT_ID,
        &f.main_scope,
        "session",
        MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
        call(json!("1"), None, "stale-key"),
    )
    .await
    .unwrap_err();
    let text = format!("{stale:?}");
    assert!(
        !text.contains("expected genesis.project_agent.select"),
        "{text}"
    );
    for malformed in [json!("one"), json!(1.5), json!(true), json!(0)] {
        let error = ForgeToolProvider::propose(
            &f.provider,
            AGENT_ID,
            &f.main_scope,
            "session",
            MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
            call(malformed, None, "malformed-key"),
        )
        .await
        .unwrap_err();
        let text = format!("{error:?}");
        assert!(text.contains("expected_session_version"), "{text}");
    }
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM command_receipt WHERE operation = 'genesis.project_agent.select'",
    )
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert_eq!(count, 1);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_event WHERE event_type = 'product_genesis.project_agent_selected'").fetch_one(f.db.pool()).await.unwrap();
    assert_eq!(count, 1);
}

/// A new Project-create call without a usable Charter approval reference is
/// refused at preparation and at dispatch and queues nothing. A reference the
/// caller invents still only queues a pending action: nothing executes.
#[tokio::test]
async fn main_create_requires_an_approval_reference_and_only_ever_queues() {
    use forge_agent_host::ForgeToolProvider;
    let f = fixture(false).await;
    let composition = ScopeToolComposition::for_scope_with_permissions(
        AGENT_ID,
        f.main_scope.clone(),
        None,
        None,
        &broad_permissions(),
        Some(Arc::new(f.provider.clone())),
    )
    .unwrap();
    let tool = composition
        .tools()
        .into_iter()
        .find(|tool| tool.spec().name == FORGE_MAIN_ORCHESTRATION_PROPOSE_TOOL)
        .unwrap();
    for payload in [
        json!({}),
        json!({"action":"create_from_approval"}),
        json!({"approval_id":null}),
        json!({"approval_id":""}),
        json!({"approval_id":7}),
    ] {
        let arguments = json!({"operation":MAIN_PROJECT_CREATE_OPERATION,"payload":payload,"dedupe_key":"no-reference","correlation_id":"no-reference"});
        let error = tool
            .prepare(arguments.clone(), &preparation_context("no-reference"))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("argument `approval_id`")
                && error.ends_with("expected project.create: {approval_id}"),
            "{error}"
        );
        let error = ForgeToolProvider::propose(
            &f.provider,
            AGENT_ID,
            &f.main_scope,
            "session",
            MAIN_PROJECT_CREATE_OPERATION,
            arguments,
        )
        .await
        .unwrap_err();
        let text = format!("{error:?}");
        assert!(text.contains("approval_id"), "{text}");
    }
    let queued = |f: &Fixture| {
        let pool = f.db.pool().clone();
        async move {
            sqlx::query_as::<_, (String, String)>(
                "SELECT status, policy_result FROM agent_action WHERE operation = 'project.create'",
            )
            .fetch_all(&pool)
            .await
            .unwrap()
        }
    };
    assert!(queued(&f).await.is_empty());
    // `project_id` is refused at preparation too, not only at dispatch.
    let error = tool
        .prepare(
            json!({"operation":MAIN_PROJECT_CREATE_OPERATION,"payload":{"approval_id":"invented","project_id":"forged"},"dedupe_key":"forged","correlation_id":"forged"}),
            &preparation_context("forged-project"),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("server-derived"), "{error}");
    let outcome = ForgeToolProvider::propose(
        &f.provider,
        AGENT_ID,
        &f.main_scope,
        "session",
        MAIN_PROJECT_CREATE_OPERATION,
        json!({"operation":MAIN_PROJECT_CREATE_OPERATION,"payload":{"approval_id":"invented"},"dedupe_key":"invented","correlation_id":"invented"}),
    )
    .await
    .unwrap();
    assert_eq!(outcome["code"], "approval_required", "{outcome}");
    assert_eq!(
        queued(&f).await,
        vec![(
            "pending_approval".to_owned(),
            "approval_required".to_owned()
        )]
    );
    for query in [
        "SELECT COUNT(*) FROM project_charter_approval",
        "SELECT COUNT(*) FROM command_receipt WHERE operation = 'project.create'",
    ] {
        let count: i64 = sqlx::query_scalar(query)
            .fetch_one(f.db.pool())
            .await
            .unwrap();
        assert_eq!(count, 0, "{query}");
    }
}
