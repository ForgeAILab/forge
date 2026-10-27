//! Tests for receipt replay in native scope composition.

use super::*;
/// Each case still uses the hand path when its literal fixture is captured.
#[tokio::test]
async fn project_registry_base_project_review_config() {
    capture_project_proposal_case("project.review_config").await;
}
#[tokio::test]
async fn project_registry_base_project_document() {
    capture_project_proposal_case("project.document").await;
}
#[tokio::test]
async fn project_registry_base_project_decision() {
    capture_project_proposal_case("project.decision").await;
}
#[tokio::test]
async fn project_registry_base_project_milestone() {
    capture_project_proposal_case("project.milestone").await;
}
#[tokio::test]
async fn project_registry_base_project_validation() {
    capture_project_proposal_case("project.validation").await;
}
#[tokio::test]
async fn project_registry_base_project_release_request() {
    capture_project_proposal_case("project.release.request").await;
}
#[tokio::test]
async fn project_registry_base_project_escalate() {
    capture_project_proposal_case("project.escalate").await;
}
#[tokio::test]
async fn project_registry_base_message_send() {
    capture_project_proposal_case("message.send").await;
}
#[tokio::test]
async fn project_registry_base_commitment_update() {
    capture_project_proposal_case("commitment.update").await;
}
#[tokio::test]
async fn project_registry_base_memory_publish() {
    capture_project_proposal_case("memory.publish").await;
}
#[tokio::test]
async fn project_registry_base_memory_supersede() {
    capture_project_proposal_case("memory.supersede").await;
}
#[tokio::test]
async fn project_registry_base_review_request() {
    capture_project_proposal_case("review.request").await;
}
#[tokio::test]
async fn project_registry_base_session_action() {
    capture_project_proposal_case("session.action").await;
}

#[tokio::test]
async fn pending_legacy_replay_cannot_claim_a_receiptless_completed_effect() {
    use forge_agent_host::ForgeToolProvider;
    let f = fixture(false).await;
    let args = json!({"operation":"message.send","payload":{"content":"Pending intent"},"dedupe_key":"legacy-completion","correlation_id":"legacy-completion"});
    let pending = f
        .provider
        .propose(
            AGENT_ID,
            &f.project_scope,
            "session",
            "message.send",
            args.clone(),
        )
        .await
        .unwrap();
    assert_eq!(pending["status"], "pending_approval");
    sqlx::query("UPDATE agent_action SET status='executed',version=version+1 WHERE id=?")
        .bind(pending["id"].as_str().unwrap())
        .execute(f.db.pool())
        .await
        .unwrap();
    let error = f
        .provider
        .propose(AGENT_ID, &f.project_scope, "session", "message.send", args)
        .await
        .unwrap_err();
    assert!(!format!("{error:?}").contains("status: Succeeded"));
}

/// Frozen hand-adapter fields on 433a00ab. Bypass new normalization to commit
/// the old command input, then replay it through the new native boundary.
#[tokio::test]
async fn task_registry_replays_hand_proposal_and_adaptive_receipts_exactly() {
    use forge_agent_host::ForgeToolProvider;
    let f = fixture(true).await;
    let service = TaskService::new_for_test(f.db.clone(), Arc::new(EventBus::new(256)));
    let stored: Value = serde_json::from_str(r#"{"operation":"task.propose","payload":{"title":"Scope composition task","description":"A native Task proposal exercised through Project scope.","task_type":"planning_task","review_requirement_ids":[],"priority":3,"merge_config":null,"role_assignments":null,"governance":null},"dedupe_key":"pre-change-task-proposal","correlation_id":"correlation-pre-change-task-proposal"}"#).unwrap();
    let payload: services::TaskProposalPayload =
        serde_json::from_value(stored["payload"].clone()).unwrap();
    let base = service
        .execute_task_proposal_direct(services::DirectTaskProposalInput {
            actor_identity_id: AGENT_ID.into(),
            executor_type: "agent".into(),
            executor_id: AGENT_ID.into(),
            source_scope_type: "project".into(),
            source_scope_id: PROJECT_ID.into(),
            project_id: PROJECT_ID.into(),
            payload,
            idempotency_key: "pre-change-task-proposal".into(),
            correlation_id: "correlation-pre-change-task-proposal".into(),
            causation_id: None,
            causation_depth: 0,
            policy_result: "allowed".into(),
            preflight_policy_result: Some("allowed".into()),
            preflight_policy_reason: None,
            policy_revision: None,
            policy_digest: None,
            requested_permission: "propose_task".into(),
        })
        .await
        .unwrap();
    let frozen: String = sqlx::query_scalar("SELECT outcome_json FROM command_receipt WHERE id=?")
        .bind(&base.receipt.id)
        .fetch_one(f.db.pool())
        .await
        .unwrap();
    for prepared in [false, true] {
        let result = if prepared {
            ForgeToolProvider::propose_prepared(
                &f.provider,
                AGENT_ID,
                &f.project_scope,
                "session",
                TASK_PROPOSE_OPERATION,
                stored.clone(),
            )
            .await
        } else {
            ForgeToolProvider::propose(
                &f.provider,
                AGENT_ID,
                &f.project_scope,
                "session",
                TASK_PROPOSE_OPERATION,
                stored.clone(),
            )
            .await
        }
        .unwrap();
        assert_eq!(result["receipt_id"], base.receipt.id);
        assert_eq!(result["result"]["input_digest"], base.receipt.input_digest);
        assert_eq!(result["result"]["event_id"], base.receipt.event_id);
        assert_eq!(result["result"]["domain_result"]["task_id"], base.task.id);
        assert_eq!(result["replayed"], true);
        let current: String =
            sqlx::query_scalar("SELECT outcome_json FROM command_receipt WHERE id=?")
                .bind(&base.receipt.id)
                .fetch_one(f.db.pool())
                .await
                .unwrap();
        assert_eq!(current, frozen);
    }
    let board = db::TaskBoardRepo::board_revision(&*f.db, PROJECT_ID)
        .await
        .unwrap();
    let adaptive = json!({"operation":"task.adaptive","payload":{"action":"replace","source_task_id":base.task.id,
        "expected_task_version":base.task.version,"expected_board_revision":board,"rationale":"reshape","title":"Replacement"},
        "dedupe_key":"pre-change-adaptive","correlation_id":"correlation-pre-change-adaptive"});
    let base_adaptive = service
        .execute_adaptive_task_command(services::AdaptiveTaskCommand {
            project_id: PROJECT_ID.into(),
            source_task_id: base.task.id.clone(),
            expected_task_version: base.task.version,
            expected_board_revision: board,
            operation: services::task_service::AdaptiveTaskOperation::Replace {
                title: "Replacement".into(),
                description: None,
            },
            rationale: "reshape".into(),
            actor_type: "agent".into(),
            actor_id: AGENT_ID.into(),
            policy_result: "allowed".into(),
            policy_revision: None,
            policy_digest: None,
            requested_permission: Some("propose_task".into()),
            idempotency_key: "pre-change-adaptive".into(),
            correlation_id: "correlation-pre-change-adaptive".into(),
            causation_id: None,
            causation_depth: 0,
        })
        .await
        .unwrap();
    let frozen: String = sqlx::query_scalar("SELECT outcome_json FROM command_receipt WHERE id=?")
        .bind(&base_adaptive.receipt.id)
        .fetch_one(f.db.pool())
        .await
        .unwrap();
    for prepared in [false, true] {
        let result = if prepared {
            ForgeToolProvider::propose_prepared(
                &f.provider,
                AGENT_ID,
                &f.project_scope,
                "session",
                TASK_ADAPTIVE_OPERATION,
                adaptive.clone(),
            )
            .await
        } else {
            ForgeToolProvider::propose(
                &f.provider,
                AGENT_ID,
                &f.project_scope,
                "session",
                TASK_ADAPTIVE_OPERATION,
                adaptive.clone(),
            )
            .await
        }
        .unwrap();
        assert_eq!(result["receipt_id"], base_adaptive.receipt.id);
        assert_eq!(
            result["result"]["input_digest"],
            base_adaptive.receipt.input_digest
        );
        assert_eq!(result["result"]["event_id"], base_adaptive.receipt.event_id);
        assert_eq!(
            result["result"]["domain_result"]["board_revision"],
            base_adaptive.board_revision
        );
        assert_eq!(result["replayed"], true);
        let current: String =
            sqlx::query_scalar("SELECT outcome_json FROM command_receipt WHERE id=?")
                .bind(&base_adaptive.receipt.id)
                .fetch_one(f.db.pool())
                .await
                .unwrap();
        assert_eq!(current, frozen);
    }
}

#[tokio::test]
async fn receipt_project_commands_replay_through_the_native_wrapper_after_revocation() {
    use forge_agent_host::{authority_bound_provider, ForgeToolProvider, RuntimeScopeBinding};
    for operation in [
        PROJECT_CHARTER_ADOPTION_OPERATION,
        PROJECT_EVIDENCE_OPERATION,
        PROJECT_READINESS_OPERATION,
    ] {
        let f = fixture_with_charter(false, operation != PROJECT_CHARTER_ADOPTION_OPERATION).await;
        let payload = if operation == PROJECT_CHARTER_ADOPTION_OPERATION {
            let mut payload = adoption_arguments("receipt-adoption")["payload"].clone();
            payload["expected_charter_version"] = json!(0);
            payload
        } else {
            sqlx::query("INSERT INTO project_milestone (id,project_id,milestone_sequence,milestone_key,lifecycle,created_at,updated_at) VALUES ('receipt-milestone',?,1,'M001','active',?,?)").bind(PROJECT_ID).bind(NOW).bind(NOW).execute(f.db.pool()).await.unwrap();
            sqlx::query("INSERT INTO project_milestone_revision (id,milestone_id,revision,lifecycle,outcome,display_label,schema_version,render_version,rendered_view,content_digest,rendered_digest,author_type,author_id,created_at) VALUES ('receipt-definition','receipt-milestone',1,'approved','Deliver','Delivery','milestone-v1','render-v1','Delivered','content','render','user',?,?)").bind(USER_ID).bind(NOW).execute(f.db.pool()).await.unwrap();
            sqlx::query("UPDATE project_milestone SET current_definition_revision_id='receipt-definition' WHERE id='receipt-milestone'").execute(f.db.pool()).await.unwrap();
            sqlx::query("INSERT INTO project_milestone_check (id,project_id,milestone_id,definition_revision_id,check_key,description,source_kind,expected_result,created_at,updated_at) VALUES ('receipt-check',?,'receipt-milestone','receipt-definition','delivery','Observe delivery','task_validation','Delivered',?,?)").bind(PROJECT_ID).bind(NOW).bind(NOW).execute(f.db.pool()).await.unwrap();
            if operation == PROJECT_EVIDENCE_OPERATION {
                let checksum = "a".repeat(64);
                sqlx::query("INSERT INTO media_asset (id,project_id,display_filename,content_type,byte_size,storage_key,checksum,availability,gc_state,created_at,updated_at) VALUES ('receipt-asset',?,'proof.txt','text/plain',5,'proof.txt',?,'available','referenced',?,?)").bind(PROJECT_ID).bind(&checksum).bind(NOW).bind(NOW).execute(f.db.pool()).await.unwrap();
                json!({"action":"attach","milestone_id":"receipt-milestone","expected_milestone_version":1,"asset_id":"receipt-asset","checksum":checksum,"acceptance_check_ids":["receipt-check"],"caption":"Observed delivery","kind":"report"})
            } else {
                json!({"action":"evaluate","milestone_id":"receipt-milestone","milestone_version":1})
            }
        };
        let arguments = json!({"operation":operation,"payload":payload,"dedupe_key":"receipt-key","correlation_id":"receipt-correlation"});
        let first = f
            .provider
            .propose(
                AGENT_ID,
                &f.project_scope,
                "session",
                operation,
                arguments.clone(),
            )
            .await
            .unwrap();
        assert!(first["receipt_id"].is_string(), "{operation}: {first}");
        let authority =
            f.db.resolve_effective_authority(AGENT_ID, None, "project", PROJECT_ID, "deny")
                .await
                .unwrap();
        let binding = RuntimeScopeBinding {
            identity_id: AGENT_ID.into(),
            scope: f.project_scope.clone(),
            task_role: None,
            workspace_path: None,
            agent_chat_project_id: None,
            project_charter_setup_required: authority.setup_required,
            allowed_permissions: authority.ceiling.clone(),
            authority: Some(authority),
        };
        let provider = authority_bound_provider(f.db.clone(), &binding, Arc::new(f.provider));
        let composition = ScopeToolComposition::for_scope_with_permissions_and_project_context(
            AGENT_ID,
            f.project_scope.clone(),
            None,
            None,
            &binding.allowed_permissions,
            ProjectChatToolContext {
                is_project_agent_chat: true,
                charter_setup_required: binding.project_charter_setup_required,
            },
            Some(provider.clone()),
            ScopeToolRuntime::default(),
        )
        .unwrap();
        sqlx::query("UPDATE agent_identity SET paused=1 WHERE id=?")
            .bind(AGENT_ID)
            .execute(f.db.pool())
            .await
            .unwrap();
        let replay = invoke_tool(
            &composition,
            FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
            arguments.clone(),
            "receipt-replay",
        )
        .await
        .unwrap();
        assert!(!replay.is_error, "{operation}: {}", replay.value);
        assert_eq!(replay.value["receipt_id"], first["receipt_id"]);
        assert_eq!(
            replay.value["result"]["domain_result"],
            first["result"]["domain_result"]
        );
        assert_eq!(replay.value["replayed"], true);
        let mut different = arguments.clone();
        different["payload"]["caption"] = json!("different input");
        assert!(
            provider
                .propose(AGENT_ID, &f.project_scope, "session", operation, different)
                .await
                .is_err(),
            "a reused key with another digest needs fresh authority"
        );
        let malformed =
            json!({"operation":operation,"payload":{},"dedupe_key":"new","correlation_id":"new"});
        let error = provider
            .propose(AGENT_ID, &f.project_scope, "session", operation, malformed)
            .await
            .unwrap_err();
        assert!(
            matches!(error, forge_agent_host::AgentHostError::StructuredOutcome(outcome) if outcome.denied_by==Some(api_types::DeniedBy::AuthorityRevoked)),
            "revocation precedes new contract detail"
        );
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM command_receipt WHERE operation=?")
                .bind(operation)
                .fetch_one(f.db.pool())
                .await
                .unwrap();
        assert_eq!(count, 1, "{operation}: replay never executes again");
    }
}

#[tokio::test]
async fn native_eight_action_verbs_match_offers_across_work_review_merge_hold_and_done() {
    use db::TaskRepo;
    use forge_agent_host::ForgeToolProvider;
    for state in ["in_progress", "review", "merging", "held", "done"] {
        for verb in [
            "start",
            "hold",
            "release",
            "retry",
            "send_back",
            "approve",
            "restart",
            "cancel",
        ] {
            let f = fixture(true).await;
            let service = TaskService::new_for_test(f.db.clone(), Arc::new(EventBus::new(32)));
            let task = service
                .create_task(
                    PROJECT_ID.to_owned(),
                    "Action matrix".to_owned(),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();
            let status = if state == "held" {
                "in_progress"
            } else {
                state
            };
            sqlx::query("UPDATE task SET status=? WHERE id=?")
                .bind(status)
                .bind(&task.id)
                .execute(f.db.pool())
                .await
                .unwrap();
            if state == "held" {
                let annotation =
                    json!({"type":"manual_stop","blocking_reason":"held","blocked_by":"user"})
                        .to_string();
                let condition = db::compose_condition(&db::ConditionInput {
                    error_annotation: Some(annotation.clone()),
                    ..Default::default()
                });
                sqlx::query("UPDATE task SET error_annotation=?,condition_json=? WHERE id=?")
                    .bind(annotation)
                    .bind(serde_json::to_string(&condition).unwrap())
                    .bind(&task.id)
                    .execute(f.db.pool())
                    .await
                    .unwrap();
            }
            if state == "merging" {
                sqlx::query("INSERT INTO integration_queue(id,repo_id,target_branch,next_seq,state,created_at,updated_at) VALUES ('matrix-queue',?,'main',2,'open',?,?)").bind(REPO_ID).bind(NOW).bind(NOW).execute(f.db.pool()).await.unwrap();
                sqlx::query("INSERT INTO integration_attempt(id,queue_id,task_id,task_ref,project_ref,queue_seq,current,admission_key,expected_status,expected_epoch,observed_task_version,enqueued_at,state,created_at,updated_at) SELECT 'matrix-attempt','matrix-queue',id,id,project_id,1,1,'matrix-admission','merging',status_epoch,version,?,'queued',?,? FROM task WHERE id=?").bind(NOW).bind(NOW).bind(NOW).bind(&task.id).execute(f.db.pool()).await.unwrap();
            }
            if state == "merging" {
                let condition = db::ConditionFacts {
                    task_id: task.id.clone(),
                    state: "merging".into(),
                    integration: Some(api_types::IntegrationReason::Waiting {
                        attempt_id: api_types::IntegrationAttemptId::new("matrix-attempt"),
                    }),
                    ..Default::default()
                }
                .condition(&db::ConditionInput::default());
                sqlx::query("UPDATE task SET condition_json=? WHERE id=?")
                    .bind(serde_json::to_string(&condition).unwrap())
                    .bind(&task.id)
                    .execute(f.db.pool())
                    .await
                    .unwrap();
            }
            let snapshot = service
                .task_action_snapshot(&task.id, &api_types::Actor::agent(AGENT_ID))
                .await
                .unwrap();
            let offers = services::available_actions(&snapshot);
            let offered = offers.iter().find(|offer| offer.action.verb() == verb);
            let mut action = match verb {
                "start" => json!({"verb":"start"}),
                "send_back" => {
                    json!({"verb":"send_back","guidance":"Repair the requested behavior"})
                }
                "approve" => json!({"verb":"approve","override":false,"reason":"Matrix decision"}),
                _ => json!({"verb":verb,"reason":"Matrix decision"}),
            };
            if let Some(offer) = offered {
                if verb == "retry" {
                    action = serde_json::to_value(&offer.action).unwrap();
                    action["reason"] = json!("Matrix recovery");
                    for parameter in &offer.parameters {
                        if let Some(values) = &parameter.boolean_values {
                            if let Some(value) = values.first() {
                                action[&parameter.name] = json!(value);
                            }
                        }
                    }
                    if offer
                        .parameters
                        .iter()
                        .any(|p| p.name == "guidance" && p.required)
                    {
                        action["guidance"] = json!("Repair the requested behavior");
                    }
                }
            }
            let payload =
                json!({"task_id":task.id,"action":action,"version":snapshot.task.version});
            let schema = operation_registry::task_proposals::offered_action_schema(&offers, false);
            let advertised = schema["anyOf"].as_array().is_some_and(|variants| {
                variants
                    .iter()
                    .any(|variant| variant["properties"]["verb"]["enum"][0] == verb)
            });
            assert_eq!(
                advertised,
                offered.is_some(),
                "schema/offer {state}/{verb}: {schema}"
            );
            let arguments = json!({"operation":TASK_ACTION_OPERATION,"payload":payload,"dedupe_key":"matrix","correlation_id":"matrix"});
            let result = f
                .provider
                .propose(
                    AGENT_ID,
                    &f.project_scope,
                    "matrix",
                    TASK_ACTION_OPERATION,
                    arguments,
                )
                .await;
            assert_eq!(
                result.is_ok(),
                advertised,
                "advertised == accepted {state}/{verb}: {result:?}"
            );
            if state == "merging" && matches!(verb, "hold" | "cancel") {
                assert_eq!(
                    advertised,
                    verb == "cancel",
                    "Cancel is offered; Hold remains owner-only for this native Project Agent"
                );
                let after = TaskRepo::get_by_id(&*f.db, &task.id, false)
                    .await
                    .unwrap()
                    .unwrap();
                if verb == "cancel" {
                    assert_eq!(after.status, "cancelled");
                } else {
                    assert_eq!(after.status, "merging");
                    assert_eq!(
                        after.version, snapshot.task.version,
                        "a refused native Hold writes nothing"
                    );
                    assert!(!services::available_actions(
                        &service
                            .task_action_snapshot(&task.id, &api_types::Actor::agent(AGENT_ID))
                            .await
                            .unwrap()
                    )
                    .iter()
                    .any(|offer| offer.action.verb() == "release"));
                    assert!(services::integration_reads::task_integration(&f.db, &after)
                        .await
                        .unwrap()
                        .is_some());
                }
            }
        }
    }
}
