//! Tests for native denials, proposal validation, and structured outcomes.

use super::*;

async fn assert_terminal_native_denial(reason: &str, expected: &str) {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let provider = CoordinationToolProvider::new(Arc::new(SqliteDb::new(pool)));
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::AgentChat,
        scope_id: "own-chat".to_owned(),
        workspace_access: WorkspaceAccess::Deny,
    };
    let error = provider
        .structured_boundary_error(
            "own-identity",
            &scope,
            TASK_ACTION_OPERATION,
            &json!({}),
            service_error(crate::ServiceError::AuthorizationDenied {
                message: reason.to_owned(),
            }),
        )
        .await;
    let AgentHostError::StructuredOutcome(outcome) = error else {
        panic!("typed denial")
    };
    assert_eq!(outcome.code, OutcomeCode::PolicyDenied);
    assert_eq!(
        outcome
            .denied_by
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some(expected)
    );
    let generic = expected == "unspecified";
    assert_eq!(outcome.safe_message.contains("Do not retry"), !generic);
    let retry = outcome.retry.unwrap();
    assert_eq!(retry.action, RetryAction::None);
    assert!(!retry.retryable);
}

#[tokio::test]
async fn terminal_denial_names_missing_permission() {
    assert_terminal_native_denial(
        "permission propose_task is outside the server-issued identity/profile/scope ceiling",
        "permission_missing(propose_task)",
    )
    .await;
    assert_terminal_native_denial(
        "requested permission is outside the Agent Chat binding ceiling",
        "permission_missing(propose_task)",
    )
    .await;
}

#[tokio::test]
async fn terminal_denial_names_paused_identity() {
    assert_terminal_native_denial("actor identity is paused or archived", "identity_paused").await;
    assert_terminal_native_denial("agent principal is paused or archived", "identity_paused").await;
}

#[tokio::test]
async fn terminal_denial_names_unadopted_charter() {
    assert_terminal_native_denial(
        "Project orchestration remains blocked until a user-approved Charter adoption is committed",
        "charter_not_adopted",
    )
    .await;
}

#[tokio::test]
async fn terminal_denial_names_operation_scope() {
    assert_terminal_native_denial(
        "operation is denied by the canonical native operation catalog",
        "operation_not_in_scope",
    )
    .await;
}

#[tokio::test]
async fn terminal_denial_names_other_safe_evaluator_causes() {
    for (reason, cause) in [
        (
            "the Task workflow no longer admits writes in its terminal state",
            "task_terminal",
        ),
        (
            "reviewer assignments cannot perform Task writes",
            "reviewer_read_only",
        ),
        (
            "protected mutation requires an independent approval",
            "independent_approval_required",
        ),
        (
            "genesis.start requires an explicit user request to create or start a new Project",
            "user_request_required",
        ),
        (
            "genesis.start requires the currently leased Main Chat turn",
            "leased_turn_required",
        ),
        (
            "Project Charter adoption is not valid for the current Project state",
            "charter_adoption_not_applicable",
        ),
        (
            "query operations execute through the read boundary",
            "read_boundary_required",
        ),
        (
            "direct command is not admitted for this permission or payload",
            "direct_command_not_admitted",
        ),
        (
            "the Task assignment does not admit review proposals",
            "review_assignment_required",
        ),
        (
            "direct Project command has no selected active profile",
            "profile_not_selected",
        ),
    ] {
        assert_terminal_native_denial(reason, cause).await;
    }
}

#[tokio::test]
async fn terminal_denial_maps_real_policy_evaluator_causes() {
    // These reasons come from the real evaluator, so a producer wording
    // change cannot silently turn a capability denial into an unknown one.
    for (operation, permission, paused, permissions, expected) in [
        (
            "task.action",
            "propose_task",
            false,
            r#"{"permissions":[]}"#,
            DeniedBy::PermissionMissing("propose_task".to_owned()),
        ),
        (
            "task.action",
            "propose_task",
            true,
            r#"{"permissions":["read_project","propose_task","propose_project"]}"#,
            DeniedBy::IdentityPaused,
        ),
        (
            "project.document",
            "propose_project",
            false,
            r#"{"permissions":["read_project","propose_task","propose_project"]}"#,
            DeniedBy::CharterNotAdopted,
        ),
        (
            "project.charter.adoption",
            "propose_project",
            false,
            r#"{"permissions":["read_project","propose_task","propose_project"]}"#,
            DeniedBy::CharterAdoptionNotApplicable,
        ),
        (
            "unlisted.operation",
            "read_project",
            false,
            r#"{"permissions":["read_project","propose_task","propose_project"]}"#,
            DeniedBy::OperationNotInScope,
        ),
        (
            "project.current_state",
            "read_project",
            false,
            r#"{"permissions":["read_project","propose_task","propose_project"]}"#,
            DeniedBy::ReadBoundaryRequired,
        ),
        (
            "task.action",
            "read_project",
            false,
            r#"{"permissions":["read_project","propose_task","propose_project"]}"#,
            DeniedBy::DirectCommandNotAdmitted,
        ),
        (
            "review.request",
            "read_project",
            false,
            r#"{"permissions":["read_project","propose_task","propose_project"]}"#,
            DeniedBy::IndependentApprovalRequired,
        ),
    ] {
        let fixture = native_plan_fixture_with_permissions("worker", "coder", permissions).await;
        let profile_id: String =
            sqlx::query_scalar("SELECT selected_profile_id FROM agent_identity WHERE id = ?")
                .bind(&fixture.agent_id)
                .fetch_one(fixture.db.pool())
                .await
                .unwrap();
        let project = db::ProjectRepo::create_with_agent_binding(
            &*fixture.db,
            db::CreateProject {
                id: db::new_uuid_v4(),
                name: "Policy Project".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: db::now_rfc3339(),
                updated_at: db::now_rfc3339(),
            },
            Some(fixture.agent_id.clone()),
            Some(profile_id.clone()),
        )
        .await
        .unwrap();
        sqlx::query(
            "UPDATE agent_identity SET paused = ?, account_permission_ceiling = ? WHERE id = ?",
        )
        .bind(i64::from(paused))
        .bind(permissions)
        .bind(&fixture.agent_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
        sqlx::query("UPDATE project_agent_binding SET permission_ceiling_json = ? WHERE project_id = ? AND state = 'active'")
                .bind(permissions).bind(&project.id).execute(fixture.db.pool()).await.unwrap();
        if expected == DeniedBy::CharterAdoptionNotApplicable {
            // Exercise the evaluator's inconsistent-state guard rather
            // than inventing its prose. Normal writes enforce this invariant.
            sqlx::query("DROP TRIGGER project_charter_pointer_guard_update")
                .execute(fixture.db.pool())
                .await
                .unwrap();
            sqlx::query("UPDATE project SET charter_setup_required = 0 WHERE id = ?")
                .bind(&project.id)
                .execute(fixture.db.pool())
                .await
                .unwrap();
        }
        let (_, reason) = fixture
            .provider
            .actions
            .evaluate_direct_command_policy(
                &fixture.agent_id,
                "project",
                &project.id,
                permission,
                operation,
                Some(r#"{"action":"draft_revision"}"#),
            )
            .await
            .unwrap();
        let scope = CanonicalScope {
            scope_type: CanonicalScopeType::Project,
            scope_id: project.id,
            workspace_access: WorkspaceAccess::Deny,
        };
        let error =
            fixture
                .provider
                .structured_boundary_error(
                    &fixture.agent_id,
                    &scope,
                    operation,
                    &json!({}),
                    AgentHostError::Authority(reason.unwrap_or_else(|| {
                        panic!("real evaluator allowed {operation}/{permission}")
                    })),
                )
                .await;
        let AgentHostError::StructuredOutcome(outcome) = error else {
            panic!("typed denial")
        };
        assert_eq!(outcome.denied_by, Some(expected));
        assert_eq!(outcome.retry.unwrap().action, RetryAction::None);
    }
}

#[tokio::test]
async fn terminal_denial_maps_real_repository_profile_guard() {
    let fixture = native_plan_fixture_with_permissions(
        "worker",
        "coder",
        r#"{"permissions":["propose_project"]}"#,
    )
    .await;
    let project_id: String = sqlx::query_scalar("SELECT project_id FROM task WHERE id = ?")
        .bind(&fixture.task_id)
        .fetch_one(fixture.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE agent_identity SET selected_profile_id = NULL WHERE id = ?")
        .bind(&fixture.agent_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let refusal = db::ProjectOrchestrationRepo::create_project_document_shell_command(
        &*fixture.db,
        db::CreateProjectDocumentShellCommand {
            document: db::CreateProjectDocument {
                id: db::new_uuid_v4(),
                project_id: project_id.clone(),
                kind: "research".to_owned(),
                title: "Profile guard".to_owned(),
                approval_policy: "user".to_owned(),
                created_at: db::now_rfc3339(),
                updated_at: db::now_rfc3339(),
            },
            expected_project_version: 1,
            action_execution: None,
            command_receipt: Some(db::CreateCommandReceipt {
                id: db::new_uuid_v4(),
                principal_type: "agent".to_owned(),
                principal_id: fixture.agent_id.clone(),
                scope_type: "project".to_owned(),
                scope_id: project_id.clone(),
                operation: "project.document".to_owned(),
                idempotency_key: "missing-profile".to_owned(),
                input_digest: "input-digest".to_owned(),
                policy_result: "allowed".to_owned(),
                correlation_id: "missing-profile".to_owned(),
                causation_id: None,
                causation_depth: 0,
                event_id: db::new_uuid_v4(),
                agent_action_execution_id: None,
                outcome_json: "{}".to_owned(),
                committed_at: db::now_rfc3339(),
            }),
        },
    )
    .await
    .expect_err("real repository evaluator rejects the missing selected Profile");
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::Project,
        scope_id: project_id,
        workspace_access: WorkspaceAccess::Deny,
    };
    let error = fixture
        .provider
        .structured_boundary_error(
            &fixture.agent_id,
            &scope,
            "project.document",
            &json!({}),
            service_error(refusal.into()),
        )
        .await;
    let AgentHostError::StructuredOutcome(outcome) = error else {
        panic!("typed denial")
    };
    assert_eq!(outcome.denied_by, Some(DeniedBy::ProfileNotSelected));
    assert_eq!(
        outcome.retry.unwrap().scope,
        Some(api_types::RetryScope::Turn)
    );
}

#[tokio::test]
async fn terminal_denial_maps_real_task_assignment_causes() {
    for (status, role, permission, expected) in [
        ("done", "worker", "task_write", DeniedBy::TaskTerminal),
        ("todo", "reviewer", "task_write", DeniedBy::ReviewerReadOnly),
        (
            "todo",
            "worker",
            "propose_review",
            DeniedBy::ReviewAssignmentRequired,
        ),
    ] {
        let fixture = native_plan_fixture("worker", "coder").await;
        sqlx::query("UPDATE task SET status = ? WHERE id = ?")
            .bind(status)
            .bind(&fixture.task_id)
            .execute(fixture.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO task_role_assignment (id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at) VALUES (?, ?, ?, 'agent', ?, ?, ?)")
                .bind(db::new_uuid_v4()).bind(&fixture.task_id).bind(role).bind(&fixture.agent_id)
                .bind(db::now_rfc3339()).bind(db::now_rfc3339()).execute(fixture.db.pool()).await.unwrap();
        let (_, reason) = fixture
            .provider
            .actions
            .evaluate_direct_command_policy(
                &fixture.agent_id,
                "task",
                &fixture.task_id,
                permission,
                "review.request",
                None,
            )
            .await
            .unwrap();
        let error = fixture
            .provider
            .structured_boundary_error(
                &fixture.agent_id,
                &fixture.scope,
                "review.request",
                &json!({}),
                AgentHostError::Authority(reason.expect("assignment evaluator reason")),
            )
            .await;
        let AgentHostError::StructuredOutcome(outcome) = error else {
            panic!("typed denial")
        };
        assert_eq!(outcome.denied_by, Some(expected));
        assert_eq!(
            outcome.retry.unwrap().scope,
            Some(api_types::RetryScope::Turn)
        );
    }
}

#[tokio::test]
async fn terminal_denial_names_own_project_pause() {
    let fixture = native_plan_fixture("worker", "coder").await;
    let profile_id: String =
        sqlx::query_scalar("SELECT selected_profile_id FROM agent_identity WHERE id = ?")
            .bind(&fixture.agent_id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    let now = db::now_rfc3339();
    let project = db::ProjectRepo::create_with_agent_binding(
        &*fixture.db,
        db::CreateProject {
            id: db::new_uuid_v4(),
            name: "own paused Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
        Some(fixture.agent_id.clone()),
        Some(profile_id),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE project SET paused_at = 'now', system_pause_reason = 'environment_not_ready' WHERE id = ?")
            .bind(&project.id).execute(fixture.db.pool()).await.unwrap();
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::Project,
        scope_id: project.id,
        workspace_access: WorkspaceAccess::Deny,
    };
    let paused_task = db::new_uuid_v4();
    sqlx::query("INSERT INTO task (id, project_id, title, status, created_at, updated_at) VALUES (?, ?, 'Paused work', 'todo', ?, ?)")
            .bind(&paused_task).bind(&scope.scope_id).bind(db::now_rfc3339()).bind(db::now_rfc3339())
            .execute(fixture.db.pool()).await.unwrap();
    let refusal =
        TaskService::new_for_test(fixture.db.clone(), Arc::new(events::EventBus::new(16)))
            .claim_task(
                paused_task,
                crate::Assignee::Agent(fixture.agent_id.clone()),
                None,
            )
            .await
            .expect_err("real claim evaluator rejects the paused Project");
    let error = fixture
        .provider
        .structured_boundary_error(
            &fixture.agent_id,
            &scope,
            TASK_ACTION_OPERATION,
            &json!({}),
            service_error(refusal),
        )
        .await;
    let AgentHostError::StructuredOutcome(outcome) = error else {
        panic!("typed denial")
    };
    assert_eq!(
        outcome
            .denied_by
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("project_paused(environment_not_ready)")
    );
    assert!(outcome.safe_message.contains("Do not retry"));
    assert_eq!(
        outcome.retry.unwrap().scope,
        Some(api_types::RetryScope::Turn)
    );
}

#[tokio::test]
async fn terminal_denial_keeps_cross_scope_reasons_redacted() {
    assert_terminal_native_denial(
        "another Project private-project-secret is paused: confidential-detail",
        "unspecified",
    )
    .await;
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let provider = CoordinationToolProvider::new(Arc::new(SqliteDb::new(pool)));
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::Project,
        scope_id: "opaque-target".to_owned(),
        workspace_access: WorkspaceAccess::Deny,
    };
    let mut values = Vec::new();
    for error in [
        native_scope_error(crate::ServiceError::not_found("project", "opaque-target")),
        native_scope_error(crate::ServiceError::AuthorizationDenied {
            message: "Project Agent binding does not own this Project scope".to_owned(),
        }),
    ] {
        let AgentHostError::StructuredOutcome(outcome) = provider
            .structured_boundary_error(
                "actor",
                &scope,
                PROJECT_CURRENT_STATE_OPERATION,
                &json!({"correlation_id":"same-correlation"}),
                error,
            )
            .await
        else {
            panic!("typed redacted outcome")
        };
        values.push(serde_json::to_value(outcome).unwrap());
    }
    assert_eq!(
        values[0], values[1],
        "missing and inaccessible scopes remain indistinguishable"
    );
    assert!(!values[0].to_string().contains("binding"));
}

#[test]
fn daemon_target_refusals_are_specific_tool_results_without_withdrawal() {
    for (error, cause) in [
        (
            crate::ServiceError::PlacementUnavailable(crate::placement::PlacementUnavailable {
                task_id: "target".into(),
                repo_id: "repo".into(),
                rejected_candidates: vec![],
            }),
            DeniedBy::PlacementUnavailable,
        ),
        (
            crate::ServiceError::DaemonUpgradeRequired {
                daemon_id: "target".into(),
            },
            DeniedBy::DaemonUpgradeRequired,
        ),
        (
            crate::ServiceError::WorkspaceResetRequired {
                task_id: "target".into(),
                reason: "lost".into(),
            },
            DeniedBy::WorkspaceResetRequired,
        ),
    ] {
        let AgentHostError::StructuredOutcome(outcome) = service_error(error) else {
            panic!("tool result expected")
        };
        assert_eq!(outcome.denied_by, Some(cause.clone()));
        assert!(!cause.withdraws_operation());
        assert_eq!(
            outcome.retry.unwrap().scope,
            Some(api_types::RetryScope::Turn)
        );
        assert_eq!(cause.to_string().parse::<DeniedBy>().unwrap(), cause);
    }
}

#[test]
fn argument_shape_rejections_are_correctable_validation_outcomes() {
    // A Project Agent that sends `task.action` with an unknown action
    // must learn which field to fix; an opaque internal failure leaves it
    // retrying the same shape or abandoning a repair its doctrine requires.
    let error = invalid_arguments("action must use a current closed Task verb".to_owned());
    match error {
        AgentHostError::StructuredOutcome(outcome) => {
            assert_eq!(outcome.code, OutcomeCode::ValidationError);
            assert_eq!(outcome.status, OutcomeStatus::Failed);
            assert!(outcome
                .safe_message
                .contains("action must use a current closed Task verb"));
            assert_eq!(
                outcome.retry.as_ref().map(|retry| retry.action),
                Some(RetryAction::CorrectInput)
            );
        }
        other => panic!("argument rejections must be structured, got {other:?}"),
    }

    // A task_id naming no Task in this Project is the same class of
    // mistake. Raising it as an authority refusal told the Agent its
    // scope had been denied recovery, so one transposed character in a
    // UUID stalled a Project behind a blocker that did not exist.
    let unresolved = invalid_arguments("task_id must name a Task in this Project".to_owned());
    match unresolved {
        AgentHostError::StructuredOutcome(outcome) => {
            assert_eq!(outcome.code, OutcomeCode::ValidationError);
            assert_eq!(
                outcome.retry.as_ref().map(|retry| retry.action),
                Some(RetryAction::CorrectInput)
            );
        }
        other => panic!("an unresolved task_id must be correctable, got {other:?}"),
    }
}

#[test]
fn generic_conflicts_are_typed_and_keep_the_actionable_reason() {
    let error = service_error(crate::ServiceError::Conflict(
        "send expected_charter_version = 2".to_owned(),
    ));
    match error {
        AgentHostError::StructuredOutcome(outcome) => {
            assert_eq!(outcome.code, OutcomeCode::ValidationError);
            assert_eq!(outcome.status, OutcomeStatus::Failed);
            assert!(outcome.safe_message.contains("expected_charter_version"));
            assert!(outcome.safe_message.contains("correct the typed input"));
        }
        other => panic!("conflict must be structured, got {other:?}"),
    }
}

#[test]
fn database_version_conflicts_have_typed_retry_without_prose() {
    let error = service_error(crate::ServiceError::Db(db::DbError::VersionConflict));
    match error {
        AgentHostError::StructuredOutcome(outcome) => {
            assert_eq!(outcome.code, OutcomeCode::VersionConflict);
            assert_eq!(outcome.status, OutcomeStatus::Failed);
            assert_eq!(
                outcome.safe_message,
                "the authorized resource changed; refresh current state and retry"
            );
            assert_eq!(
                outcome.retry.as_ref().map(|retry| retry.action),
                Some(RetryAction::RefreshAndRetry)
            );
            assert!(outcome.current_version_or_revision.is_none());
        }
        other => panic!("version conflicts must be structured, got {other:?}"),
    }
}

#[test]
fn idempotency_conflicts_do_not_query_or_expose_current_state() {
    let error = service_error(crate::ServiceError::Db(db::DbError::IdempotencyConflict));
    match error {
        AgentHostError::StructuredOutcome(outcome) => {
            assert_eq!(outcome.code, OutcomeCode::IdempotencyConflict);
            assert!(outcome.current_version_or_revision.is_none());
            let retry = outcome.retry.expect("fresh key guidance");
            assert_eq!(retry.action, RetryAction::UseNewIdempotencyKey);
            assert!(!retry.retryable);
        }
        other => panic!("idempotency conflicts must be structured, got {other:?}"),
    }
}

#[test]
fn genesis_start_failures_keep_stable_structured_codes() {
    let cases = [
        (
            crate::ServiceError::ExecutionSetupRequired {
                message: "private setup detail".to_owned(),
                requirements: vec![SetupRequirement::new("main_agent")],
            },
            OutcomeCode::SetupRequired,
        ),
        (
            crate::ServiceError::ProductGenesisActiveSession {
                session_id: "private-session-id".to_owned(),
            },
            OutcomeCode::ActiveSessionConflict,
        ),
        (
            crate::ServiceError::Db(db::DbError::IdempotencyConflict),
            OutcomeCode::IdempotencyConflict,
        ),
        (
            crate::ServiceError::Domain("private storage failure".to_owned()),
            OutcomeCode::InternalFailure,
        ),
    ];
    for (error, expected) in cases {
        match service_error(error) {
            AgentHostError::StructuredOutcome(outcome) => {
                assert_eq!(outcome.code, expected);
                assert!(!outcome.safe_message.contains("private"));
            }
            other => panic!("Genesis start failure must be structured, got {other:?}"),
        }
    }
}

#[test]
fn proposal_targets_are_derived_from_scope() {
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::Project,
        scope_id: "project-1".to_owned(),
        workspace_access: WorkspaceAccess::Deny,
    };
    let arguments = json!({
        "payload": {"title":"bounded"},
        "dedupe_key":"dedupe",
        "correlation_id":"corr",
    });
    assert_eq!(
        scope_type_name(scope.scope_type),
        "project",
        "the operation target is taken from the canonical scope"
    );
    assert_eq!(arguments["payload"]["title"], "bounded");
}

#[test]
fn adaptive_payload_is_closed_to_the_three_bounded_actions() {
    let split = json!({
        "action": "split",
        "source_task_id": "task-1",
        "expected_task_version": 1,
        "expected_board_revision": 2,
        "rationale": "separate bounded work",
        "items": [{"title": "child", "description": null, "assignee_id": null}]
    });
    let sequence = json!({
        "action": "sequence",
        "source_task_id": "task-1",
        "expected_task_version": 1,
        "expected_board_revision": 2,
        "rationale": "order bounded work",
        "ordered_task_ids": ["task-2", "task-3"]
    });
    let replace = json!({
        "action": "replace",
        "source_task_id": "task-1",
        "expected_task_version": 1,
        "expected_board_revision": 2,
        "rationale": "replace bounded work",
        "title": "replacement",
        "description": "updated outcome"
    });
    for payload in [split, sequence, replace] {
        assert!(validate_proposal_payload(TASK_ADAPTIVE_OPERATION, &payload).is_ok());
    }
}

#[test]
fn adaptive_payload_rejects_unknown_and_server_owned_fields() {
    let base = json!({
        "action": "replace",
        "source_task_id": "task-1",
        "expected_task_version": 1,
        "expected_board_revision": 2,
        "rationale": "bounded",
        "title": "replacement",
        "description": null
    });
    for field in [
        "project_id",
        "scope_id",
        "actor_id",
        "governance",
        "fixed_boundary_digest",
        "unknown",
    ] {
        let mut payload = base.clone();
        payload[field] = json!("forbidden");
        assert!(
            registered_proposals::CATALOG
                .lookup(TASK_ADAPTIVE_OPERATION)
                .unwrap()
                .normalize_arguments(&payload)
                .is_err(),
            "adaptive payload field {field} must be rejected"
        );
    }
}
#[test]
fn session_action_payload_is_bounded_and_allowlisted() {
    assert!(validate_proposal_payload("session.action", &json!({"action":"cancel"}),).is_ok());
    assert!(validate_proposal_payload(
        "session.action",
        &json!({"action":"steer","content":"continue"}),
    )
    .is_ok());
    assert!(validate_proposal_payload("session.action", &json!({"action":"execute"}),).is_err());
    assert!(validate_proposal_payload("session.action", &json!({"action":"steer"}),).is_err());
}

#[test]
fn generic_project_lifecycle_cannot_create_projects() {
    assert!(validate_proposal_payload("project.lifecycle", &json!({"action":"create"}),).is_err());
    assert!(validate_proposal_payload(
        "project.lifecycle",
        &json!({"action":"organize","project_id":"project-1"}),
    )
    .is_ok());
    assert!(validate_proposal_payload(
        MAIN_PROJECT_CREATE_OPERATION,
        &json!({"action":"create_from_approval","approval_id":"approval-1"}),
    )
    .is_ok());
}
#[tokio::test]
async fn terminal_denial_project_pause_uses_bound_project_and_task_own_project() {
    let fixture = native_plan_fixture("worker", "coder").await;
    let project_id: String = sqlx::query_scalar("SELECT project_id FROM task WHERE id = ?")
        .bind(&fixture.task_id)
        .fetch_one(fixture.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE project SET paused_at = 'now', system_pause_reason = 'environment_not_ready' WHERE id = ?")
            .bind(&project_id).execute(fixture.db.pool()).await.unwrap();
    for (paused_project, expected) in [
        (
            project_id.as_str(),
            db::project_pause_denial(Some("environment_not_ready")),
        ),
        ("unrelated-project", DeniedBy::Unspecified),
    ] {
        let error = fixture
            .provider
            .structured_boundary_error(
                &fixture.agent_id,
                &fixture.scope,
                TASK_EVIDENCE_OPERATION,
                &json!({}),
                service_error(crate::ServiceError::ProjectPaused {
                    project_id: paused_project.to_owned(),
                }),
            )
            .await;
        let AgentHostError::StructuredOutcome(outcome) = error else {
            panic!("structured refusal")
        };
        assert_eq!(outcome.denied_by, Some(expected));
        assert!(!outcome.safe_message.contains("unrelated-project"));
    }
}

#[tokio::test]
async fn native_search_validation_and_observation_refusal_have_boundary_metadata() {
    let fixture = native_plan_fixture("worker", "coder").await;
    let search = fixture
        .provider
        .public_search(
            &fixture.agent_id,
            &fixture.scope,
            PublicSearchScope::Project,
            "",
            5,
        )
        .await
        .unwrap_err();
    let AgentHostError::StructuredOutcome(search) = search else {
        panic!("structured validation")
    };
    assert_eq!(search.code, OutcomeCode::ValidationError);
    assert_eq!(
        search.operation,
        forge_agent_host::FORGE_PUBLIC_WEB_SEARCH_TOOL
    );
    assert_eq!(search.scope, outcome_scope(&fixture.scope));
    assert!(!search.correlation_id.is_empty());
    let observation = fixture
        .provider
        .observe_command(
            &fixture.agent_id,
            &fixture.scope,
            CommandObservation {
                session_id: fixture.runtime_session_id.clone(),
                turn_id: Some("turn".to_owned()),
                program: "git".to_owned(),
                args: vec![],
                exit_code: Some(0),
                success: true,
                output_digest: "digest".to_owned(),
                stdout_excerpt: String::new(),
                stderr_excerpt: String::new(),
            },
        )
        .await
        .unwrap_err();
    let AgentHostError::StructuredOutcome(observation) = observation else {
        panic!("structured refusal")
    };
    assert_eq!(observation.denied_by, Some(DeniedBy::Unspecified));
    assert_eq!(observation.operation, "observe_command");
    assert_eq!(observation.scope, outcome_scope(&fixture.scope));
    assert!(!observation.correlation_id.is_empty());
    assert!(!observation.safe_message.contains("corrected input"));
}

#[tokio::test]
async fn setup_completed_denial_names_adoption_not_applicable() {
    use operation_registry::authority::{AuthorityFacts, EffectiveAuthority, Principal};
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    let provider = CoordinationToolProvider::new(Arc::new(SqliteDb::new(pool)));
    let mut spec = operation_registry::project_reads::specs::<AgentHostError>()
        .into_iter()
        .find(|spec| spec.id == "project.charter")
        .unwrap();
    spec.availability = operation_registry::AvailabilityRule::SetupOnly;
    let mut authority = EffectiveAuthority::resolve(AuthorityFacts {
        principal: Principal::ProjectAgent {
            identity_id: "agent".into(),
            project_id: "project".into(),
        },
        scope_type: "project".into(),
        scope_id: "project".into(),
        profile_id: "profile".into(),
        layers: vec![["read_project".into()].into_iter().collect()],
        binding_id: Some("binding".into()),
        setup_required: false,
        active: true,
    });
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::Project,
        scope_id: "project".into(),
        workspace_access: WorkspaceAccess::Deny,
    };
    let AgentHostError::StructuredOutcome(outcome) = provider
        .evaluate_registered_authority(&authority, &scope, &spec)
        .unwrap_err()
    else {
        panic!("typed denial")
    };
    assert_eq!(
        outcome.denied_by,
        Some(DeniedBy::CharterAdoptionNotApplicable)
    );
    spec.availability = operation_registry::AvailabilityRule::ReadyOnly;
    authority.setup_required = true;
    let AgentHostError::StructuredOutcome(outcome) = provider
        .evaluate_registered_authority(&authority, &scope, &spec)
        .unwrap_err()
    else {
        panic!("typed denial")
    };
    assert_eq!(outcome.denied_by, Some(DeniedBy::CharterNotAdopted));
}
