//! Shared milestone command fixtures, authorization, receipts, and conflict inputs.

use super::*;

pub(super) const USER_ID: &str = "milestone-command-user";
pub(super) const AGENT_ID: &str = "milestone-command-agent";
pub(super) const PROFILE_ID: &str = "milestone-command-profile";
pub(super) const PROJECT_ID: &str = "milestone-command-project";
pub(super) const MILESTONE_ID: &str = "milestone-command-milestone";
pub(super) const MILESTONE_REVISION_ID: &str = "milestone-command-revision";
pub(super) const CHARTER_ID: &str = "milestone-command-charter";
pub(super) const CHARTER_REVISION_ID: &str = "milestone-command-charter-revision";
pub(super) const NOW: &str = "2026-08-20T00:00:00.000Z";

pub(super) async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("SQLite pool creates");
    run_migrations(&pool).await.expect("migrations run");
    Arc::new(SqliteDb::new(pool))
}

pub(super) async fn fixture() -> Arc<SqliteDb> {
    fixture_with_acceptance_matrix("[]").await
}

pub(super) async fn fixture_with_acceptance_matrix(_acceptance_matrix_json: &str) -> Arc<SqliteDb> {
    let db = database().await;
    UserRepo::create_user(
        &*db,
        &User {
            id: USER_ID.to_owned(),
            email: "milestone-command@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Milestone command user".to_owned()),
            is_admin: false,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("user creates");
    AgentRepo::create_identity_with_profile(
        &*db,
        CreateAgentIdentity {
            id: AGENT_ID.to_owned(),
            name: "Milestone command agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: db::AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some(USER_ID.to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        CreateAgentProfile {
            id: PROFILE_ID.to_owned(),
            identity_id: AGENT_ID.to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test-model".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("agent identity creates");
    ProjectRepo::create_with_agent_binding(
        &*db,
        CreateProject {
            id: PROJECT_ID.to_owned(),
            name: "Milestone command project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(USER_ID.to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        Some(AGENT_ID.to_owned()),
        Some(PROFILE_ID.to_owned()),
    )
    .await
    .expect("Project creates");

    // The acceptance commands consume an already active definition and an
    // approved active baseline.  These are immutable fixture rows, not an
    // alternate materializer implementation.
    sqlx::query(
        "INSERT INTO project_charter
            (id, account_id, genesis_session_id, project_id,
             current_draft_revision_id, current_approved_revision_id,
             project_mode, maturity, lifecycle, version, created_at, updated_at)
         VALUES (?, ?, NULL, ?, NULL, NULL, 'compact', 'mvp', 'attached', 1, ?, ?)",
    )
    .bind(CHARTER_ID)
    .bind(USER_ID)
    .bind(PROJECT_ID)
    .bind(CHARTER_REVISION_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("Charter creates");
    sqlx::query(
        "INSERT INTO project_charter_revision
            (id, charter_id, revision, base_revision, base_revision_id,
             lifecycle, schema_version, render_version, content_json,
             rendered_view, change_summary, author_type, author_id,
             source_message_id, source_turn_job_id, source_refs_json,
             content_digest, rendered_digest, created_at)
         VALUES (?, ?, 1, 0, NULL, 'approved', 'charter@1', 'render@1', '{}',
                 '# Charter', 'fixture', 'user', ?, NULL, NULL, '[]',
                 'charter-content', 'charter-rendered', ?)",
    )
    .bind(CHARTER_REVISION_ID)
    .bind(CHARTER_ID)
    .bind(USER_ID)
    .bind(PROJECT_ID)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("Charter revision creates");
    sqlx::query("UPDATE project_charter SET current_approved_revision_id = ? WHERE id = ?")
        .bind(CHARTER_REVISION_ID)
        .bind(CHARTER_ID)
        .execute(db.pool())
        .await
        .expect("Charter pointer updates");
    sqlx::query(
        "INSERT INTO project_milestone
            (id, project_id, milestone_sequence, milestone_key, display_label,
             lifecycle, blocker_reason_json, stale_reason_json,
             reconciliation_reason_json, version, created_at, updated_at)
         VALUES (?, ?, 1, 'M001', 'Release milestone', 'active', '[]', '[]',
                 '[]', 1, ?, ?)",
    )
    .bind(MILESTONE_ID)
    .bind(PROJECT_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("milestone creates");
    sqlx::query(
        "INSERT INTO project_milestone_revision
            (id, milestone_id, revision, base_revision, base_revision_id,
             lifecycle, display_label, outcome, included_scope_json,
             excluded_scope_json, charter_revision_id, document_revisions_json,
             task_selection_json, dependencies_json, risks_json,
             acceptance_checks_json, evidence_requirements_json,
             known_issues_json, change_summary, schema_version, render_version,
             rendered_view, content_digest, rendered_digest, author_type,
             author_id, source_refs_json, created_at)
         VALUES (?, ?, 1, 0, NULL, 'approved', 'Release milestone',
                 'The release outcome is delivered', '[]', '[]', ?, '[]',
                 '[]', '[]', '[]', '[]', '[]', '[]', 'fixture',
                 'milestone@1', 'milestone-render@1', '# Milestone',
                 'milestone-content', 'milestone-rendered', 'user', ?, '[]', ?)",
    )
    .bind(MILESTONE_REVISION_ID)
    .bind(MILESTONE_ID)
    .bind(CHARTER_REVISION_ID)
    .bind(USER_ID)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("milestone revision creates");
    sqlx::query(
        "UPDATE project_milestone
         SET current_definition_revision_id = ? WHERE id = ?",
    )
    .bind(MILESTONE_REVISION_ID)
    .bind(MILESTONE_ID)
    .execute(db.pool())
    .await
    .expect("milestone pointer updates");
    db
}

pub(super) struct ReceiptInput<'a> {
    pub(super) operation: &'a str,
    pub(super) principal_type: &'a str,
    pub(super) principal_id: &'a str,
    pub(super) key: &'a str,
    pub(super) digest: &'a str,
    pub(super) correlation_id: &'a str,
    pub(super) outcome: &'a Value,
    pub(super) execution_id: Option<&'a str>,
}

pub(super) fn receipt(input: ReceiptInput<'_>) -> CreateCommandReceipt {
    let ReceiptInput {
        operation,
        principal_type,
        principal_id,
        key,
        digest,
        correlation_id,
        outcome,
        execution_id,
    } = input;
    CreateCommandReceipt {
        id: format!("receipt-{key}"),
        principal_type: principal_type.to_owned(),
        principal_id: principal_id.to_owned(),
        scope_type: "project".to_owned(),
        scope_id: PROJECT_ID.to_owned(),
        operation: operation.to_owned(),
        idempotency_key: key.to_owned(),
        input_digest: digest.to_owned(),
        policy_result: "allowed".to_owned(),
        correlation_id: correlation_id.to_owned(),
        causation_id: None,
        causation_depth: 0,
        event_id: String::new(),
        agent_action_execution_id: execution_id.map(str::to_owned),
        outcome_json: outcome.to_string(),
        committed_at: NOW.to_owned(),
    }
}

pub(super) fn user_authorization(action: &str, key: &str) -> ProjectCommandAuthorization {
    ProjectCommandAuthorization {
        principal_type: "user".to_owned(),
        principal_id: USER_ID.to_owned(),
        policy_result: "allowed".to_owned(),
        policy_revision: None,
        policy_digest: None,
        requested_permission: Some(action.to_owned()),
        correlation_id: format!("correlation-{key}"),
        causation_id: None,
        causation_depth: 0,
        authorization_event_id: format!("authorization-{key}"),
        authorization_basis: "explicit user authorization".to_owned(),
        authorization_action: action.to_owned(),
        authorization_occurred_at: db::now_rfc3339(),
        authorization_json: json!({"action": action, "key": key}).to_string(),
    }
}

pub(super) fn agent_authorization(action: &str, key: &str) -> ProjectCommandAuthorization {
    ProjectCommandAuthorization {
        principal_type: "agent".to_owned(),
        principal_id: AGENT_ID.to_owned(),
        policy_result: "allowed".to_owned(),
        policy_revision: Some("agent-policy@1".to_owned()),
        policy_digest: Some("agent-policy-digest".to_owned()),
        requested_permission: Some(action.to_owned()),
        correlation_id: format!("correlation-{key}"),
        causation_id: None,
        causation_depth: 0,
        authorization_event_id: format!("authorization-{key}"),
        authorization_basis: "bound Project Agent authorization".to_owned(),
        authorization_action: action.to_owned(),
        authorization_occurred_at: db::now_rfc3339(),
        authorization_json: json!({
            "principal": {"kind": "agent", "id": AGENT_ID},
            "action": action,
            "event_id": format!("authorization-{key}"),
        })
        .to_string(),
    }
}

pub(super) async fn arm_receipt_failpoint(db: &SqliteDb, trigger_name: &str, message: &str) {
    sqlx::query(&format!(
        "CREATE TEMP TRIGGER {trigger_name}
         BEFORE INSERT ON command_receipt
         BEGIN SELECT RAISE(ABORT, '{message}'); END"
    ))
    .execute(db.pool())
    .await
    .expect("command receipt failpoint creates");
}

pub(super) async fn drop_receipt_failpoint(db: &SqliteDb, trigger_name: &str) {
    sqlx::query(&format!("DROP TRIGGER {trigger_name}"))
        .execute(db.pool())
        .await
        .expect("command receipt failpoint drops");
}

pub(super) fn definition_content(name: &str, check_id: Option<&str>) -> MilestoneDefinitionContent {
    MilestoneDefinitionContent {
        name: name.to_owned(),
        outcome: format!("{name} outcome is delivered"),
        included_scope: vec!["implementation".to_owned()],
        excluded_scope: Vec::new(),
        charter_revision: None,
        document_revisions: Vec::new(),
        task_ids: Vec::new(),
        dependencies: Vec::new(),
        risks: Vec::new(),
        acceptance_checks: check_id
            .map(|id| {
                vec![MilestoneAcceptanceCheck {
                    id: id.to_owned(),
                    description: format!("Verify {name}"),
                    required: true,
                    source_kind: AcceptanceCheckSourceKind::Manual,
                    expected_result: "pass".to_owned(),
                    latest_result: None,
                    latest_result_id: None,
                    latest_result_digest: None,
                }]
            })
            .unwrap_or_default(),
        evidence_requirements: check_id
            .map(|id| {
                vec![AcceptanceEvidenceRequirement {
                    id: id.to_owned(),
                    description: format!("Evidence for {name}"),
                    required: true,
                    evidence_kind: None,
                }]
            })
            .unwrap_or_default(),
        known_issues: Vec::new(),
        target_date: None,
    }
}

pub(super) struct DefinitionCommandInput<'a> {
    pub(super) project_version: i64,
    pub(super) milestone_id: Option<&'a str>,
    pub(super) milestone_version: i64,
    pub(super) base_revision_id: Option<&'a str>,
    pub(super) lifecycle: MilestoneDefinitionLifecycle,
    pub(super) key: &'a str,
    pub(super) name: &'a str,
    pub(super) check_id: Option<&'a str>,
}

pub(super) fn definition_command(
    input: DefinitionCommandInput<'_>,
) -> ProjectMilestoneDefinitionCommand {
    let DefinitionCommandInput {
        project_version,
        milestone_id,
        milestone_version,
        base_revision_id,
        lifecycle,
        key,
        name,
        check_id,
    } = input;
    let content = definition_content(name, check_id);
    ProjectMilestoneDefinitionCommand {
        project_id: PROJECT_ID.to_owned(),
        milestone_id: milestone_id.map(str::to_owned),
        display_label: Some(name.to_owned()),
        lifecycle,
        rendered_view: api_types::canonical_json(&content).expect("canonical milestone content"),
        render_version: "forge.milestone-definition-render/v1".to_owned(),
        change_summary: format!("{name} command"),
        provenance: RevisionProvenance {
            author: PrincipalRef {
                kind: PrincipalKind::User,
                id: USER_ID.to_owned(),
                display_name: None,
            },
            profile_revision: None,
            operating_skill_revision: None,
            source_refs: Vec::new(),
            change_summary: format!("{name} command"),
            material_diff: None,
        },
        content,
        base_revision_id: base_revision_id.map(str::to_owned),
        expected_project_version: project_version,
        expected_milestone_version: milestone_version,
        idempotency_key: key.to_owned(),
        authorization: user_authorization(
            if milestone_id.is_some() {
                "project.milestone.revision.save"
            } else {
                "project.milestone.create"
            },
            key,
        ),
    }
}

pub(super) async fn action(
    db: &SqliteDb,
    action_id: &str,
    operation: &str,
    correlation_id: &str,
) -> db::AgentAction {
    AgentActionRepo::create_action(
        db,
        CreateAgentAction {
            id: action_id.to_owned(),
            actor_identity_id: AGENT_ID.to_owned(),
            scope_type: "project".to_owned(),
            scope_id: PROJECT_ID.to_owned(),
            operation: operation.to_owned(),
            payload_json: "{}".to_owned(),
            payload_hash: format!("hash-{action_id}"),
            dedupe_key: format!("dedupe-{action_id}"),
            correlation_id: correlation_id.to_owned(),
            causation_id: None,
            causation_depth: 0,
            requested_permission: format!("{operation}:execute"),
            policy_result: AgentActionPolicyResult::Allowed,
            policy_reason: None,
            status: AgentActionStatus::Proposed,
            target_type: Some("project".to_owned()),
            target_id: Some(PROJECT_ID.to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("native action creates")
}

pub(super) fn action_execution(
    action: &db::AgentAction,
    execution_id: &str,
    key: &str,
    outcome: &Value,
) -> CreateAgentActionExecution {
    CreateAgentActionExecution {
        id: execution_id.to_owned(),
        action_id: action.id.clone(),
        expected_action_version: action.version,
        attempt: 1,
        status: AgentActionExecutionStatus::Succeeded,
        result_json: Some(outcome.to_string()),
        error: None,
        executed_by_type: "agent".to_owned(),
        executed_by_id: AGENT_ID.to_owned(),
        idempotency_key: key.to_owned(),
        action_status: AgentActionStatus::Executed,
        action_outcome_json: Some(outcome.to_string()),
        created_at: NOW.to_owned(),
        completed_at: Some(NOW.to_owned()),
        updated_at: NOW.to_owned(),
    }
}

pub(super) fn milestone_revision(
    id: &str,
    milestone_id: &str,
    expected_version: i64,
    base_revision: i64,
    base_revision_id: Option<&str>,
    lifecycle: &str,
) -> CreateProjectMilestoneRevision {
    CreateProjectMilestoneRevision {
        id: id.to_owned(),
        milestone_id: milestone_id.to_owned(),
        expected_milestone_version: expected_version,
        base_revision,
        base_revision_id: base_revision_id.map(str::to_owned),
        lifecycle: lifecycle.to_owned(),
        display_label: Some("Release milestone".to_owned()),
        outcome: "The release outcome is delivered".to_owned(),
        included_scope_json: "[]".to_owned(),
        excluded_scope_json: "[]".to_owned(),
        charter_revision_id: None,
        document_revisions_json: "[]".to_owned(),
        task_selection_json: "[]".to_owned(),
        dependencies_json: "[]".to_owned(),
        risks_json: "[]".to_owned(),
        acceptance_checks_json: "[]".to_owned(),
        evidence_requirements_json: "[]".to_owned(),
        known_issues_json: "[]".to_owned(),
        change_summary: "command acceptance".to_owned(),
        schema_version: "milestone@1".to_owned(),
        render_version: "milestone-render@1".to_owned(),
        rendered_view: "# Release milestone".to_owned(),
        content_digest: format!("content-{id}"),
        rendered_digest: format!("rendered-{id}"),
        author_type: "user".to_owned(),
        author_id: Some(USER_ID.to_owned()),
        source_refs_json: "[]".to_owned(),
        created_at: NOW.to_owned(),
    }
}

pub(super) fn readiness_snapshot(id: &str, key: &str) -> CreateProjectReadinessSnapshotCommand {
    CreateProjectReadinessSnapshotCommand {
        snapshot: CreateProjectReadinessSnapshot {
            id: id.to_owned(),
            project_id: PROJECT_ID.to_owned(),
            milestone_id: MILESTONE_ID.to_owned(),
            definition_revision_id: MILESTONE_REVISION_ID.to_owned(),
            input_manifest_json: "[]".to_owned(),
            event_watermark: "watermark-1".to_owned(),
            outcome: "ready".to_owned(),
            blocking_reasons_json: "[]".to_owned(),
            blocker_projection_json: "[]".to_owned(),
            stale_projection_json: "[]".to_owned(),
            reconciliation_projection_json: "[]".to_owned(),
            check_results_json: "[]".to_owned(),
            waiver_manifest_json: "[]".to_owned(),
            evidence_manifest_json: "[]".to_owned(),
            commit_context_json: "{}".to_owned(),
            computing_policy_revision: "forge.readiness.compute/v1".to_owned(),
            readiness_digest: "readiness-digest-1".to_owned(),
            principal_type: "user".to_owned(),
            principal_id: USER_ID.to_owned(),
            authorization_basis: "explicit user authorization".to_owned(),
            authorization_action: "project.milestone.readiness".to_owned(),
            authorization_occurred_at: db::now_rfc3339(),
            expected_milestone_version: 1,
            explicit_event: "readiness-evaluated".to_owned(),
            idempotency_key: key.to_owned(),
            created_at: NOW.to_owned(),
        },
        command_receipt: None,
        action_execution: None,
    }
}

pub(super) fn readiness_command(key: &str) -> ProjectReadinessRequestCommand {
    ProjectReadinessRequestCommand {
        project_id: PROJECT_ID.to_owned(),
        milestone_id: MILESTONE_ID.to_owned(),
        expected_milestone_version: 1,
        idempotency_key: key.to_owned(),
        authenticated_user_id: Some(USER_ID.to_owned()),
        authorization: user_authorization("project.milestone.readiness", key),
    }
}

pub(super) fn release_command(
    key: &str,
    snapshot: &ProjectReadinessSnapshotRecord,
) -> ProjectReleaseRequestCommand {
    ProjectReleaseRequestCommand {
        project_id: PROJECT_ID.to_owned(),
        milestone_id: MILESTONE_ID.to_owned(),
        expected_milestone_version: 2,
        readiness_snapshot_id: snapshot.id.clone(),
        readiness_digest: snapshot.readiness_digest.clone(),
        status: "pending_user_release_approval".to_owned(),
        idempotency_key: key.to_owned(),
        authorization: agent_authorization("project.milestone.release.request", key),
    }
}

// These small conflict wrappers keep the assertions above focused on the
// durable receipt identity while preserving the command's full typed input.
pub(super) struct MilestoneConflictInput {
    pub(super) input_digest: String,
    pub(super) principal_id_for_test: Option<String>,
    pub(super) command: CreateProjectMilestoneCommand,
}

impl MilestoneConflictInput {
    pub(super) fn into_command(mut self) -> CreateProjectMilestoneCommand {
        if let Some(principal_id) = self.principal_id_for_test {
            if let Some(receipt) = self.command.command_receipt.as_mut() {
                receipt.principal_id = principal_id;
            }
        }
        if let Some(receipt) = self.command.command_receipt.as_mut() {
            receipt.input_digest = self.input_digest;
        }
        self.command
    }
}

pub(super) fn define_input_for_conflict(
    _defined: &db::ProjectMilestoneRevisionRecord,
    stored: &db::CommandReceipt,
) -> MilestoneConflictInput {
    let outcome: Value = serde_json::from_str(&stored.outcome_json).expect("stored outcome");
    MilestoneConflictInput {
        input_digest: stored.input_digest.clone(),
        principal_id_for_test: None,
        command: CreateProjectMilestoneCommand {
            milestone: CreateProjectMilestone {
                id: "native-milestone".to_owned(),
                project_id: PROJECT_ID.to_owned(),
                expected_project_version: 1,
                milestone_sequence: 2,
                milestone_key: "M002".to_owned(),
                display_label: Some("Native milestone".to_owned()),
                created_at: NOW.to_owned(),
                updated_at: NOW.to_owned(),
            },
            revision: milestone_revision(
                "native-milestone-revision",
                "native-milestone",
                1,
                0,
                None,
                "proposed",
            ),
            allocate_project_sequence: false,
            check_definitions: Vec::new(),
            command_receipt: Some(receipt(ReceiptInput {
                operation: PROJECT_MILESTONE_OPERATION,
                principal_type: "agent",
                principal_id: AGENT_ID,
                key: "native-milestone-define",
                digest: &stored.input_digest,
                correlation_id: "native-milestone-correlation",
                outcome: &outcome,
                execution_id: Some("native-milestone-execution"),
            })),
            action_execution: Some(action_execution_placeholder(
                "native-milestone-action",
                "native-milestone-execution",
                "native-milestone-define",
                &outcome,
            )),
        },
    }
}

pub(super) fn action_execution_placeholder(
    action_id: &str,
    execution_id: &str,
    key: &str,
    outcome: &Value,
) -> CreateAgentActionExecution {
    CreateAgentActionExecution {
        id: execution_id.to_owned(),
        action_id: action_id.to_owned(),
        expected_action_version: 1,
        attempt: 1,
        status: AgentActionExecutionStatus::Succeeded,
        result_json: Some(outcome.to_string()),
        error: None,
        executed_by_type: "agent".to_owned(),
        executed_by_id: AGENT_ID.to_owned(),
        idempotency_key: key.to_owned(),
        action_status: AgentActionStatus::Executed,
        action_outcome_json: Some(outcome.to_string()),
        created_at: NOW.to_owned(),
        completed_at: Some(NOW.to_owned()),
        updated_at: NOW.to_owned(),
    }
}

pub(super) struct ReleaseConflictInput {
    pub(super) input_digest: String,
    pub(super) command: CreateProjectReleaseRequestCommand,
}

impl ReleaseConflictInput {
    pub(super) fn into_command(mut self) -> CreateProjectReleaseRequestCommand {
        if let Some(receipt) = self.command.command_receipt.as_mut() {
            receipt.input_digest = self.input_digest;
        }
        self.command
    }
}

pub(super) fn release_input_for_conflict(
    request: &db::ProjectReleaseRequestRecord,
    stored: &db::CommandReceipt,
) -> ReleaseConflictInput {
    let outcome: Value =
        serde_json::from_str(&stored.outcome_json).expect("stored release outcome");
    ReleaseConflictInput {
        input_digest: stored.input_digest.clone(),
        command: CreateProjectReleaseRequestCommand {
            request: CreateProjectReleaseRequest {
                event_id: request.event_id.clone(),
                project_id: PROJECT_ID.to_owned(),
                milestone_id: MILESTONE_ID.to_owned(),
                expected_milestone_version: request.expected_milestone_version,
                readiness_snapshot_id: request.readiness_snapshot_id.clone(),
                readiness_digest: request.readiness_digest.clone(),
                status: request.status.clone(),
                idempotency_key: request.idempotency_key.clone(),
                created_at: NOW.to_owned(),
            },
            command_receipt: Some(receipt(ReceiptInput {
                operation: PROJECT_RELEASE_OPERATION,
                principal_type: "agent",
                principal_id: AGENT_ID,
                key: request.idempotency_key.as_str(),
                digest: &stored.input_digest,
                correlation_id: "native-release-correlation",
                outcome: &outcome,
                execution_id: stored.agent_action_execution_id.as_deref(),
            })),
            action_execution: Some(action_execution_placeholder(
                "native-release-action",
                "native-release-execution",
                request.idempotency_key.as_str(),
                &outcome,
            )),
        },
    }
}

pub(super) async fn charter_binding(db: &SqliteDb, revision_id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT charter_revision_id FROM project_milestone_revision WHERE id = ?")
        .bind(revision_id)
        .fetch_one(db.pool())
        .await
        .expect("revision charter binding reads")
}
