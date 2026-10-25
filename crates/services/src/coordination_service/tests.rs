//! Coordination policy, commitment, and action tests.
use super::*;
use db::{
    create_sqlite_pool, run_migrations, AgentRepo, AgentStatus, CreateAgentIdentity,
    CreateAgentProfile, CreateProject, CreateProjectAgentBinding, ProjectAgentBindingRepo,
    ProjectRepo, ReplaceProjectAgentBinding,
};
use forge_agent_host::TASK_ACTION_OPERATION;
use forge_agent_host::{
    MIGRATED_OPERATION_CONTRACTS, PROJECT_DECISION_OPERATION, PROJECT_DOCUMENT_OPERATION,
    PROJECT_EVIDENCE_OPERATION, PROJECT_MILESTONE_OPERATION, PROJECT_READINESS_OPERATION,
    PROJECT_VALIDATION_OPERATION,
};

#[test]
fn generic_executor_authority_names_are_not_executable() {
    for operation in [
        "project.lifecycle",
        "handoff.publish",
        "decision.request",
        "web.search",
    ] {
        assert!(
            is_non_executable_authority_operation(operation),
            "generic executor must reject authority operation {operation}"
        );
    }
    assert!(!is_non_executable_authority_operation("message.send"));
}

#[test]
fn generic_executor_rejects_every_typed_orchestration_operation() {
    for contract in MIGRATED_OPERATION_CONTRACTS.iter() {
        let operation = contract.operation;
        assert!(
            is_typed_orchestration_operation(operation),
            "generic executor must route typed operation {operation} to its domain executor"
        );
    }
    assert!(!is_typed_orchestration_operation("message.send"));
}

#[test]
fn main_genesis_start_is_admitted_as_a_direct_discovery_command() {
    assert!(is_allowed_main_orchestration_operation(
        "propose_discovery",
        MAIN_GENESIS_START_OPERATION,
    ));
    assert!(!is_allowed_main_orchestration_operation(
        "propose_project",
        MAIN_GENESIS_START_OPERATION,
    ));
}

#[test]
fn main_project_agent_selection_is_admitted_as_a_direct_discovery_command() {
    assert!(is_allowed_main_orchestration_operation(
        "propose_discovery",
        MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
    ));
    assert!(!is_allowed_main_orchestration_operation(
        "propose_project",
        MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
    ));
}

#[test]
fn task_proposal_payload_rejects_unknown_fields() {
    let payload = serde_json::json!({
        "title": "Bounded task",
        "unexpected_field": true,
    });
    assert!(serde_json::from_value::<TaskProposalPayload>(payload).is_err());
}

#[test]
fn nested_artifact_scope_is_not_a_project_authority_override() {
    assert!(!forge_agent_host::contains_authority_override(
        &serde_json::json!({
            "action": "draft_revision",
            "content": {"scope": {"included": ["checkout"]}},
        })
    ));
    assert!(forge_agent_host::contains_authority_override(
        &serde_json::json!({
            "project_id": "forged-project",
            "content": {},
        })
    ));
}

#[tokio::test]
async fn invalid_task_payload_is_rejected_before_action_admission() {
    let db = db().await;
    let result = AgentActionService::new(Arc::clone(&db))
        .propose(ProposeActionInput {
            id: None,
            actor_identity_id: "agent-a".to_owned(),
            scope_type: "project".to_owned(),
            scope_id: "project-1".to_owned(),
            operation: TASK_PROPOSE_OPERATION.to_owned(),
            payload_json: serde_json::json!({
                "title": "Bounded task",
                "project_id": "caller-authored-project",
            })
            .to_string(),
            dedupe_key: "invalid-task-payload".to_owned(),
            correlation_id: "corr-invalid-task-payload".to_owned(),
            causation_id: None,
            causation_depth: 0,
            requested_permission: "propose_task".to_owned(),
            policy_reason: None,
            target_type: None,
            target_id: None,
        })
        .await;
    assert!(result.is_err());
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_action WHERE dedupe_key = 'invalid-task-payload'",
    )
    .fetch_one(db.pool())
    .await
    .expect("action count");
    assert_eq!(count, 0);
}

async fn db() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    let db = Arc::new(SqliteDb::new(pool));
    for id in ["agent-a", "agent-b"] {
        let now = now_rfc3339();
        AgentRepo::create_identity_with_profile(
                &*db,
                CreateAgentIdentity {
                    id: id.to_owned(),
                    name: id.to_owned(),
                    description: None,
                    max_concurrent_tasks: 1,
                    heartbeat_interval_seconds: 30,
                    max_missed_heartbeats: 3,
                    status: AgentStatus::Idle,
                    last_heartbeat_at: None,
                    is_default: false,
                    paused: false,
                    owner_id: Some("user-1".to_owned()),
                    visibility: "account".to_owned(),
                    account_permission_ceiling:
                        r#"{"permissions":["read_account","propose_discovery","propose_project","propose_task","propose_message","propose_review","task_write","approve_actions"]}"#
                            .to_owned(),
                    created_at: now.clone(),
                    updated_at: now.clone(),
                },
                CreateAgentProfile {
                    id: new_uuid_v4(),
                    identity_id: id.to_owned(),
                    backend_kind: "native".to_owned(),
                    executor_type: "native".to_owned(),
                    provider: None,
                    model: None,
                    reasoning_effort: None,
                    permission_policy: None,
                    prompt_template: None,
                    capabilities_json: "{}".to_owned(),
                    tool_policy_json: r#"{"permissions":["read_account","propose_discovery","propose_project","propose_task","propose_message","propose_review","task_write","approve_actions"]}"#
                        .to_owned(),
                    config_json: "{}".to_owned(),
                    credential_ref: None,
                    daemon_id: None,
                    created_at: now.clone(),
                    updated_at: now,
                },
            )
            .await
            .expect("agent");
    }
    let now = now_rfc3339();
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: "project-1".to_owned(),
            name: "Project 1".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some("user-1".to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project");
    // Project Agent authority is singular: the binding is the only
    // durable source of project scope and permission.
    let agent = AgentRepo::get_by_id(&*db, "agent-a")
        .await
        .expect("agent lookup")
        .expect("agent-a");
    let setup = ProjectAgentBindingRepo::get_active_project_binding(&*db, "project-1")
        .await
        .expect("binding lookup")
        .expect("setup binding");
    ProjectAgentBindingRepo::replace_project_binding(
        &*db,
        ReplaceProjectAgentBinding {
            project_id: "project-1".to_owned(),
            expected_version: setup.version,
            replacement: CreateProjectAgentBinding {
                id: new_uuid_v4(),
                project_id: "project-1".to_owned(),
                identity_id: Some(agent.id),
                profile_id: Some(agent.profile_id),
                state: "active".to_owned(),
                autonomy_policy_json: "{}".to_owned(),
                permission_ceiling_json: r#"{"permissions":["propose_task"]}"#.to_owned(),
                subscriptions_json: "[]".to_owned(),
                wake_budget: 10,
                operating_skill_revision_id: None,
                policy_revision: "default".to_owned(),
                policy_digest: String::new(),
                charter_id: None,
                charter_revision_id: None,
                charter_setup_required: true,
                admission_receipt_id: None,
                charter_approval_id: None,
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
            replacement_reason: Some("coordination policy fixture".to_owned()),
        },
    )
    .await
    .expect("project binding");
    db
}

#[test]
fn project_agent_policy_admits_only_bounded_subactions() {
    let allowed = [
        (PROJECT_CHARTER_ADOPTION_OPERATION, "draft_revision"),
        (PROJECT_DOCUMENT_OPERATION, "draft_revision"),
        (PROJECT_DOCUMENT_OPERATION, "propose_approval"),
        (PROJECT_DOCUMENT_OPERATION, "approve"),
        (PROJECT_DECISION_OPERATION, "record_candidate"),
        (PROJECT_DECISION_OPERATION, "record_effective"),
        (PROJECT_MILESTONE_OPERATION, "define"),
        (PROJECT_MILESTONE_OPERATION, "revise"),
        (PROJECT_MILESTONE_OPERATION, "set_primary"),
        (PROJECT_EVIDENCE_OPERATION, "attach"),
        (PROJECT_VALIDATION_OPERATION, "record"),
        (PROJECT_READINESS_OPERATION, "evaluate"),
    ];
    for (operation, action) in allowed {
        let mut payload = serde_json::json!({"action": action});
        if operation == PROJECT_DECISION_OPERATION {
            payload["decision_class"] = serde_json::json!("project_implementation");
        }
        assert!(
            is_allowed_project_draft_operation(operation, &payload.to_string()),
            "expected bounded action {operation}/{action} to be admitted"
        );
    }

    for (operation, action) in [
        (PROJECT_CHARTER_ADOPTION_OPERATION, "approve"),
        (PROJECT_CHARTER_ADOPTION_OPERATION, "attach"),
        (PROJECT_EVIDENCE_OPERATION, "attest"),
        (PROJECT_RELEASE_OPERATION, "release"),
        (PROJECT_RELEASE_OPERATION, "approve"),
        (PROJECT_DECISION_OPERATION, "supersede"),
        (PROJECT_DECISION_OPERATION, "invalidate"),
        // A release-candidate request is intentionally approval-backed:
        // it enters the Action queue and must not be treated as a direct
        // receipt-backed Project coordination write.
        (PROJECT_RELEASE_OPERATION, "propose_candidate"),
    ] {
        let payload = serde_json::json!({"action": action});
        assert!(
            !is_allowed_project_draft_operation(operation, &payload.to_string()),
            "authority action {operation}/{action} must not be admitted"
        );
    }

    assert_eq!(
        classify_operation(
            PROJECT_RELEASE_OPERATION,
            Some(&serde_json::json!({"action": "propose_candidate"}))
        ),
        OperationClassification::ApprovalRequiredAction,
        "release-candidate requests retain their audited approval boundary"
    );

    for decision_class in ["user_scope", "policy", "waiver", "manual_approval"] {
        let payload = serde_json::json!({
            "action": "record_effective",
            "decision_class": decision_class,
        });
        assert!(
            !is_allowed_project_draft_operation(PROJECT_DECISION_OPERATION, &payload.to_string()),
            "decision class {decision_class} must remain user/system-only"
        );
    }

    let cross_scope_override = serde_json::json!({
        "action": "record_candidate",
        "decision_class": "project_implementation",
        "project_id": "another-project",
        "scope_id": "another-project",
        "actor_identity_id": "another-agent",
    });
    // Scope and identity are server-derived and are never made valid by
    // caller-supplied fields. The native validator rejects these fields;
    // the policy helper must not treat them as authority either.
    assert!(!is_allowed_project_draft_operation(
        PROJECT_DECISION_OPERATION,
        &cross_scope_override.to_string()
    ));
}

#[tokio::test]
async fn completion_requires_authorized_evidence_and_is_idempotent() {
    let db = db().await;
    let commitments = CommitmentService::new(Arc::clone(&db));
    let commitment = commitments
        .create(CreateCommitmentInput {
            id: None,
            owner_identity_id: "agent-a".to_owned(),
            scope_type: "account".to_owned(),
            scope_id: "user-1".to_owned(),
            title: "Deliver result".to_owned(),
            description: None,
            status: AgentCommitmentStatus::Open,
            due_at: None,
            correlation_id: "corr-1".to_owned(),
            originating_action_id: None,
            originating_task_id: None,
            evidence_required: true,
        })
        .await
        .expect("commitment");
    let no_evidence = commitments
        .complete(CompleteCommitmentInput {
            id: commitment.id.clone(),
            expected_version: commitment.version,
            evidence: CommitmentEvidenceInput {
                id: None,
                commitment_id: commitment.id.clone(),
                evidence_type: String::new(),
                evidence_id: String::new(),
                scope_type: "account".to_owned(),
                scope_id: "user-1".to_owned(),
                description: None,
                metadata_json: "{}".to_owned(),
                authorized_by_type: "agent".to_owned(),
                authorized_by_id: "agent-a".to_owned(),
                dedupe_key: "complete-1".to_owned(),
            },
            actor_type: "agent".to_owned(),
            actor_id: "agent-a".to_owned(),
            reason: None,
            dedupe_key: "complete-1".to_owned(),
        })
        .await;
    assert!(no_evidence.is_err());
    let completed = commitments
        .complete(CompleteCommitmentInput {
            id: commitment.id.clone(),
            expected_version: commitment.version,
            evidence: CommitmentEvidenceInput {
                id: None,
                commitment_id: commitment.id.clone(),
                evidence_type: "task_delivery".to_owned(),
                evidence_id: "task-1".to_owned(),
                scope_type: "account".to_owned(),
                scope_id: "user-1".to_owned(),
                description: Some("accepted delivery".to_owned()),
                metadata_json: "{}".to_owned(),
                authorized_by_type: "agent".to_owned(),
                authorized_by_id: "agent-a".to_owned(),
                dedupe_key: "complete-1".to_owned(),
            },
            actor_type: "agent".to_owned(),
            actor_id: "agent-a".to_owned(),
            reason: None,
            dedupe_key: "complete-1".to_owned(),
        })
        .await
        .expect("completion");
    assert_eq!(completed.status, AgentCommitmentStatus::Completed);
    let replay = commitments
        .complete(CompleteCommitmentInput {
            id: commitment.id,
            expected_version: completed.version,
            evidence: CommitmentEvidenceInput {
                id: None,
                commitment_id: completed.id.clone(),
                evidence_type: "task_delivery".to_owned(),
                evidence_id: "task-1".to_owned(),
                scope_type: "account".to_owned(),
                scope_id: "user-1".to_owned(),
                description: None,
                metadata_json: "{}".to_owned(),
                authorized_by_type: "agent".to_owned(),
                authorized_by_id: "agent-a".to_owned(),
                dedupe_key: "complete-1".to_owned(),
            },
            actor_type: "agent".to_owned(),
            actor_id: "agent-a".to_owned(),
            reason: None,
            dedupe_key: "complete-1".to_owned(),
        })
        .await
        .expect("replay");
    assert_eq!(replay.version, completed.version);
}

#[tokio::test]
async fn self_approval_is_denied_and_approved_actions_execute_once() {
    let db = db().await;
    let actions = AgentActionService::new(Arc::clone(&db));
    let allowed = actions
        .propose(ProposeActionInput {
            id: None,
            actor_identity_id: "agent-a".to_owned(),
            scope_type: "agent".to_owned(),
            scope_id: "agent-a".to_owned(),
            operation: "message.send".to_owned(),
            payload_json: "{}".to_owned(),
            dedupe_key: "allowed-1".to_owned(),
            correlation_id: "corr-allowed".to_owned(),
            causation_id: None,
            causation_depth: 0,
            requested_permission: "propose_message".to_owned(),
            policy_reason: None,
            target_type: None,
            target_id: None,
        })
        .await
        .expect("allowed proposal");
    assert_eq!(
        allowed.policy_result,
        AgentActionPolicyResult::ApprovalRequired
    );
    let denied = actions
        .propose(ProposeActionInput {
            id: None,
            actor_identity_id: "agent-a".to_owned(),
            scope_type: "project".to_owned(),
            scope_id: "project-1".to_owned(),
            operation: "project.lifecycle".to_owned(),
            payload_json: "{}".to_owned(),
            dedupe_key: "denied-1".to_owned(),
            correlation_id: "corr-denied".to_owned(),
            causation_id: None,
            causation_depth: 0,
            requested_permission: "read_agent_chat".to_owned(),
            policy_reason: None,
            target_type: None,
            target_id: None,
        })
        .await;
    assert!(matches!(
        denied,
        Err(ServiceError::AuthorizationDenied { .. })
    ));
    // Reuse the admitted identity-scoped proposal. Newly-created Projects
    // are Charter-setup-required and must not admit Task proposals; that
    // boundary is covered separately by Product Genesis acceptance.
    let action = allowed;
    let self_approval = actions
        .approve(ApproveActionInput {
            action_id: action.id.clone(),
            expected_version: action.version,
            approver_identity_id: "agent-a".to_owned(),
            decision: AgentActionApprovalDecision::Approved,
            reason: None,
        })
        .await;
    assert!(self_approval.is_err());
    assert_eq!(
        action.policy_result,
        AgentActionPolicyResult::ApprovalRequired
    );
    assert_eq!(action.status, AgentActionStatus::PendingApproval);
    actions
        .approve(ApproveActionInput {
            action_id: action.id.clone(),
            expected_version: action.version,
            approver_identity_id: "agent-b".to_owned(),
            decision: AgentActionApprovalDecision::Approved,
            reason: None,
        })
        .await
        .expect("independent approval");
    let current = actions.get(&action.id).await.expect("current action");
    let first = actions
        .execute(ExecuteActionInput {
            action_id: action.id.clone(),
            expected_version: current.version,
            attempt: 1,
            result_json: Some(r#"{"task_id":"task-1"}"#.to_owned()),
            error: None,
            executed_by_type: "forge".to_owned(),
            executed_by_id: "forge".to_owned(),
            idempotency_key: "execute-1".to_owned(),
        })
        .await
        .expect("execution");
    let replay = actions
        .execute(ExecuteActionInput {
            action_id: action.id,
            expected_version: current.version,
            attempt: 2,
            result_json: Some(r#"{"task_id":"different"}"#.to_owned()),
            error: None,
            executed_by_type: "forge".to_owned(),
            executed_by_id: "forge".to_owned(),
            idempotency_key: "execute-1".to_owned(),
        })
        .await
        .expect("replay execution");
    assert_eq!(first.id, replay.id);
}

#[tokio::test]
async fn policy_denies_cross_project_and_membership_role_escalation() {
    let db = db().await;
    let now = now_rfc3339();
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: "project-2".to_owned(),
            name: "Project 2".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some("user-1".to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project");
    let actions = AgentActionService::new(Arc::clone(&db));
    let (policy_result, policy_reason) = actions
        .evaluate_direct_command_policy(
            "agent-a",
            "project",
            "project-2",
            "propose_task",
            TASK_PROPOSE_OPERATION,
            Some(r#"{"title":"outside role"}"#),
        )
        .await
        .expect("direct-command policy evaluation");
    assert_eq!(policy_result, AgentActionPolicyResult::Denied);
    assert!(policy_reason
        .as_deref()
        .is_some_and(|reason| reason.contains("active binding")));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_action WHERE dedupe_key = 'role-escalation-1'",
        )
        .fetch_one(db.pool())
        .await
        .expect("direct-command action count"),
        0
    );

    ProjectRepo::create(
        &*db,
        CreateProject {
            id: "project-3".to_owned(),
            name: "Project 3".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some("user-1".to_owned()),
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("project");
    let project_3_agent = AgentRepo::get_by_id(&*db, "agent-a")
        .await
        .expect("agent lookup")
        .expect("agent-a");
    let project_3_setup = ProjectAgentBindingRepo::get_active_project_binding(&*db, "project-3")
        .await
        .expect("binding lookup")
        .expect("setup binding");
    ProjectAgentBindingRepo::replace_project_binding(
        &*db,
        ReplaceProjectAgentBinding {
            project_id: "project-3".to_owned(),
            expected_version: project_3_setup.version,
            replacement: CreateProjectAgentBinding {
                id: new_uuid_v4(),
                project_id: "project-3".to_owned(),
                identity_id: Some(project_3_agent.id),
                profile_id: Some(project_3_agent.profile_id),
                state: "active".to_owned(),
                autonomy_policy_json: "{}".to_owned(),
                permission_ceiling_json: r#"{"permissions":["propose_message"]}"#.to_owned(),
                subscriptions_json: "[]".to_owned(),
                wake_budget: 10,
                operating_skill_revision_id: None,
                policy_revision: "default".to_owned(),
                policy_digest: String::new(),
                charter_id: None,
                charter_revision_id: None,
                charter_setup_required: true,
                admission_receipt_id: None,
                charter_approval_id: None,
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
            replacement_reason: Some("coordination policy fixture".to_owned()),
        },
    )
    .await
    .expect("project binding");
    let protected = actions
        .propose(ProposeActionInput {
            id: None,
            actor_identity_id: "agent-a".to_owned(),
            scope_type: "project".to_owned(),
            scope_id: "project-3".to_owned(),
            operation: "message.send".to_owned(),
            payload_json: r#"{"message":"needs approval"}"#.to_owned(),
            dedupe_key: "cross-project-approval-1".to_owned(),
            correlation_id: "corr-cross-project-approval".to_owned(),
            causation_id: None,
            causation_depth: 0,
            requested_permission: "propose_message".to_owned(),
            policy_reason: None,
            target_type: Some("project".to_owned()),
            target_id: Some("project-3".to_owned()),
        })
        .await
        .expect("protected proposal");
    let unrelated = actions
        .approve(ApproveActionInput {
            action_id: protected.id,
            expected_version: protected.version,
            approver_identity_id: "agent-b".to_owned(),
            decision: AgentActionApprovalDecision::Approved,
            reason: None,
        })
        .await;
    assert!(unrelated.is_err());
}

/// Every operation the catalog classifies as a Coordination
/// `DirectCommand` must also be admitted by the direct-command gate with
/// its own canonical permission. These are two independent lists, and
/// when they drifted the result was silent and expensive: `task.recover`
/// was declared, implemented, and documented, yet every call was refused
/// as "not admitted for this permission or payload" — so a Task that
/// fails by construction retried indefinitely with no reachable remedy.
#[test]
fn every_coordination_direct_command_is_admitted_with_its_own_permission() {
    use forge_agent_host::{OperationExposure, OperationSurface, MIGRATED_OPERATION_CONTRACTS};

    let mut checked = 0;
    for contract in MIGRATED_OPERATION_CONTRACTS.iter() {
        // Only `GenericProposal` operations reach this gate: they are
        // composed into `forge_scope_propose` and executed through the
        // shared direct-command path. A `TypedProposal` (task.worklog,
        // task.evidence) is dispatched earlier to its own handler, which
        // carries its own scope authorization, so it never consults this
        // list and must not be required to appear in it.
        if contract.surface != OperationSurface::Coordination
            || contract.classification != OperationClassification::DirectCommand
            || contract.exposure != OperationExposure::GenericProposal
        {
            continue;
        }
        let permission = contract
            .supported_scopes
            .iter()
            .find_map(|scope| contract.permission.for_scope(*scope))
            .unwrap_or_else(|| {
                panic!(
                    "{} has no permission for any supported scope",
                    contract.operation
                )
            });
        assert!(
            is_admitted_direct_command(contract.operation, permission, None),
            "{} is a Coordination direct command with permission `{permission}` but the \
                 direct-command gate refuses it; add it to `is_admitted_direct_command`",
            contract.operation
        );
        checked += 1;
    }
    assert!(
        checked >= 4,
        "expected task.propose/adaptive/review/recover, saw {checked}"
    );
}

/// The gate is a permission check, not a name check: the right operation
/// with someone else's permission stays refused.
#[test]
fn a_direct_command_is_refused_under_the_wrong_permission() {
    assert!(!is_admitted_direct_command(
        TASK_ACTION_OPERATION,
        "read_project",
        None
    ));
    assert!(!is_admitted_direct_command(
        "task.nonexistent",
        "propose_task",
        None
    ));
}
/// Literal policy ranks captured on 49839d1c before replacing its checks.
#[tokio::test]
async fn action_policy_is_equal_or_stricter_than_base_table() {
    let db = db().await;
    let permissions = serde_json::json!({"permissions":["read_account","read_agent_chat","read_project","propose_discovery","propose_project","propose_task","propose_message","propose_commitment","propose_memory","propose_review","propose_decision","propose_session"]}).to_string();
    sqlx::query("UPDATE agent_identity SET account_permission_ceiling = ?")
        .bind(&permissions)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE project_agent_binding SET permission_ceiling_json = ? WHERE state = 'active'",
    )
    .bind(&permissions)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query("INSERT OR IGNORE INTO user (id,email,password_hash,created_at,updated_at) VALUES ('user-1','policy@example.test','test','now','now')").execute(db.pool()).await.unwrap();
    sqlx::query("INSERT OR IGNORE INTO agent_chat (id,kind,account_id,created_at,updated_at) VALUES ('main-chat','account_main','user-1','now','now')").execute(db.pool()).await.unwrap();
    sqlx::query("INSERT OR IGNORE INTO agent_chat (id,kind,project_id,created_at,updated_at) VALUES ('project-chat','project','project-1','now','now')").execute(db.pool()).await.unwrap();
    let main_chat: String = sqlx::query_scalar(
        "SELECT id FROM agent_chat WHERE kind='account_main' AND account_id='user-1'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    let project_chat: String = sqlx::query_scalar(
        "SELECT id FROM agent_chat WHERE kind='project' AND project_id='project-1'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    let operations = [
        ("project.create", "propose_project", "create_from_approval"),
        (
            "genesis.project_agent.select",
            "propose_discovery",
            "select",
        ),
        ("genesis.start", "propose_discovery", "start"),
        ("charter.draft", "propose_discovery", "save_revision"),
        (
            "project.charter.adoption",
            "propose_project",
            "draft_revision",
        ),
        ("project.review_config", "propose_project", "set_ci_steps"),
        ("project.document", "propose_project", "draft_revision"),
        ("project.document", "propose_project", "propose_approval"),
        ("project.document", "propose_project", "approve"),
        ("project.decision", "propose_project", "record_candidate"),
        ("project.decision", "propose_project", "record_effective"),
        ("project.milestone", "propose_project", "define"),
        ("project.milestone", "propose_project", "revise"),
        ("project.milestone", "propose_project", "set_primary"),
        ("project.evidence", "propose_project", "attach"),
        ("project.evidence", "propose_project", "capture"),
        ("project.validation", "propose_project", "record"),
        ("project.escalate", "propose_project", ""),
        ("project.readiness", "propose_project", "evaluate"),
        (
            "project.release.request",
            "propose_project",
            "propose_candidate",
        ),
        ("message.send", "propose_message", ""),
        ("message.propose", "propose_message", ""),
        ("commitment.propose", "propose_commitment", ""),
        ("review.propose", "propose_review", ""),
        ("commitment.update", "propose_commitment", ""),
        ("memory.publish", "propose_memory", ""),
        ("memory.supersede", "propose_memory", ""),
        ("session.action", "propose_session", "cancel"),
        ("session.action", "propose_session", "steer"),
        ("review.request", "propose_review", ""),
    ];
    // This table compares policy on loaded facts, not the Charter writer.
    // Its minimal fixture deliberately has no authoritative Charter rows.
    sqlx::query("DROP TRIGGER project_charter_pointer_guard_update")
        .execute(db.pool())
        .await
        .unwrap();
    let mut table = serde_json::Map::new();
    let mut narrowed = Vec::new();
    let mut admitted = 0;
    for (state, setup, paused, restricted) in [
        ("setup", 1, 0, false),
        ("ready", 0, 0, false),
        ("paused", 0, 1, false),
        ("restricted", 0, 0, true),
    ] {
        sqlx::query("UPDATE project SET charter_setup_required = ?, charter_status = ?, current_charter_id = ?, current_charter_revision_id = ? WHERE id = 'project-1'")
                .bind(setup).bind(if setup == 1 { "legacy_unverified" } else { "charter_backed" }).bind((setup==0).then_some("charter")).bind((setup==0).then_some("revision")).execute(db.pool()).await.unwrap();
        sqlx::query("UPDATE agent_identity SET paused = ?")
            .bind(paused)
            .execute(db.pool())
            .await
            .unwrap();
        for actor in ["agent-a", "agent-b"] {
            let profile_id = format!("policy-{state}-{actor}");
            db::AgentProfileRepo::create_profile(
                &*db,
                db::CreateAgentProfile {
                    id: profile_id.clone(),
                    identity_id: actor.into(),
                    backend_kind: "native".into(),
                    executor_type: "native".into(),
                    provider: None,
                    model: None,
                    reasoning_effort: None,
                    permission_policy: None,
                    prompt_template: None,
                    capabilities_json: "{}".into(),
                    tool_policy_json: if restricted {
                        "{}".into()
                    } else {
                        permissions.clone()
                    },
                    config_json: "{}".into(),
                    credential_ref: None,
                    daemon_id: None,
                    created_at: now_rfc3339(),
                    updated_at: now_rfc3339(),
                },
            )
            .await
            .unwrap();
            sqlx::query("UPDATE agent_identity SET selected_profile_id = ? WHERE id = ?")
                .bind(profile_id)
                .bind(actor)
                .execute(db.pool())
                .await
                .unwrap();
        }
        for actor in ["agent-a", "agent-b"] {
            for (scope, id) in [
                ("account", "user-1"),
                ("account", "other"),
                ("project", "project-1"),
                ("agent_chat", main_chat.as_str()),
                ("agent_chat", project_chat.as_str()),
                ("agent", actor),
            ] {
                for (operation, permission, action) in operations {
                    let payload = serde_json::json!({"action":action}).to_string();
                    let (policy, _) = evaluate_action_policy(
                        &db,
                        actor,
                        scope,
                        id,
                        permission,
                        operation,
                        Some(&payload),
                    )
                    .await
                    .unwrap();
                    let rank = match policy {
                        AgentActionPolicyResult::Denied => 0,
                        AgentActionPolicyResult::ApprovalRequired => 1,
                        AgentActionPolicyResult::Allowed => 2,
                    };
                    let (base, _) = super::base_policy::evaluate_action_policy(
                        &db,
                        actor,
                        scope,
                        id,
                        permission,
                        operation,
                        Some(&payload),
                    )
                    .await
                    .unwrap();
                    let base_rank = match base {
                        AgentActionPolicyResult::Denied => 0,
                        AgentActionPolicyResult::ApprovalRequired => 1,
                        AgentActionPolicyResult::Allowed => 2,
                    };
                    assert!(
                            rank <= base_rank,
                            "widened {state}/{actor}/{scope}/{id}/{operation}/{action}: base={base_rank} new={rank}"
                        );
                    if rank < base_rank {
                        narrowed.push(format!("{state}/{actor}/{scope}/{id}/{operation}/{action}: base={base_rank} new={rank}"));
                    }
                    admitted += usize::from(rank > 0);
                    if matches!(operation, "project.create" | "genesis.project_agent.select")
                        && scope == "account"
                        && id == "user-1"
                        && state == "ready"
                    {
                        assert_eq!(
                            rank, base_rank,
                            "an owned unbound identity must keep proposal authority"
                        );
                        assert!(rank > 0);
                    }
                    table.insert(
                        format!("{state}/{actor}/{scope}/{id}/{operation}/{action}"),
                        Value::from(rank),
                    );
                }
            }
        }
    }
    assert_eq!(table.len(), 4 * 2 * 6 * operations.len());
    // Never wider is not enough: a table that denies everything would
    // pass. No cell may silently lose what base admitted either.
    assert!(admitted > 0, "the fixture must admit some cells");
    // The one intended loss: a Project Agent can no longer queue
    // `project.create` from its Project scope or Project Chat. It was
    // never advertised there; the operation belongs to the Main Agent.
    let expected = ["setup", "ready"]
        .into_iter()
        .flat_map(|state| {
            [
                "project/project-1".to_owned(),
                format!("agent_chat/{project_chat}"),
            ]
            .map(|scope| {
                format!("{state}/agent-a/{scope}/project.create/create_from_approval: base=1 new=0")
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(narrowed, expected, "cells narrower than base");
}
