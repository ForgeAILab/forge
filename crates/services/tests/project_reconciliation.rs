//! Regression coverage for finding F10: a `project_reconciliation_record` in
//! state `required` previously had no reachable product exit anywhere in
//! Forge -- `resolve_project_reconciliation` had exactly one caller, a
//! database test.  This fixture mirrors the preserved failed run exactly
//! (`docs/spec/changes/refactor-agent-orchestration-boundaries-2026-08-20/artifacts/gate8-preserved-run/evidence.json`):
//! a `task` record conflicting with a Project's governing `execution_baseline`
//! under conflict code `adaptive_task_boundary_crossed`.  Every test below
//! goes through `ProjectReconciliationService`, the shared query/command
//! boundary added for design D15 -- not through the raw repository.

use std::sync::Arc;

use api_types::{
    AuthorizationProvenance, MutationEnvelope, PrincipalKind, PrincipalRef,
    ReconciliationReplacementRef, ReconciliationResolutionAction, ReconciliationState,
    ResolveProjectReconciliationRequest,
};
use db::{
    create_sqlite_pool, run_migrations, AgentRepo, AgentStatus, CreateAgentIdentity,
    CreateAgentProfile, CreateProject, CreateProjectCanonicalConflict, CreateProjectReconciliation,
    CreateTask, DbError, ProjectOrchestrationRepo, ProjectRepo, SqliteDb, TaskRepo, User, UserRepo,
};
use services::{ProjectReconciliationService, ServiceError};

const USER_ID: &str = "reconciliation-user";
const AGENT_ID: &str = "reconciliation-agent";
const PROFILE_ID: &str = "reconciliation-profile";
const PROJECT_ID: &str = "reconciliation-project";
const TASK_ID: &str = "reconciliation-task";
const BASELINE_ID: &str = "reconciliation-baseline";
const NOW: &str = "2026-08-24T23:14:48.000Z";

async fn fixture() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("SQLite pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));

    UserRepo::create_user(
        &*db,
        &User {
            id: USER_ID.to_owned(),
            email: "reconciliation@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Reconciliation Test User".to_owned()),
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
            name: "Reconciliation Project Agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
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
    .expect("Project Agent identity creates");

    ProjectRepo::create_with_agent_binding(
        &*db,
        CreateProject {
            id: PROJECT_ID.to_owned(),
            name: "TaskBoard".to_owned(),
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

    TaskRepo::create(
        &*db,
        CreateTask {
            id: TASK_ID.to_owned(),
            project_id: PROJECT_ID.to_owned(),
            repo_id: None,
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "Implement TaskBoard Single-Page Application".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "backlog".to_owned(),
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
    .expect("Task creates");

    db
}

/// Record the exact preserved-run conflict: an adaptive `replace` outside
/// the approved envelope, governed by the active execution baseline,
/// conflicting with the Task that requested it.
async fn seed_adaptive_boundary_reconciliation(db: &SqliteDb) -> String {
    let conflict = ProjectOrchestrationRepo::create_project_canonical_conflict(
        db,
        CreateProjectCanonicalConflict {
            id: "reconciliation-conflict".to_owned(),
            project_id: PROJECT_ID.to_owned(),
            domain: "execution".to_owned(),
            governing_record_type: "execution_baseline".to_owned(),
            governing_record_id: BASELINE_ID.to_owned(),
            governing_record_revision: "2".to_owned(),
            governing_record_digest: "digest-governing".to_owned(),
            conflicting_record_type: "task".to_owned(),
            conflicting_record_id: TASK_ID.to_owned(),
            conflicting_record_revision: "1".to_owned(),
            conflicting_record_digest: "digest-conflicting".to_owned(),
            affected_paths_json: r#"["outcome","acceptance","risk_class","side_effects","release_policy","elevated_operations"]"#.to_owned(),
            conflict_code: "adaptive_task_boundary_crossed".to_owned(),
            description: "adaptive Task operation 'replace' is outside the approved envelope"
                .to_owned(),
            detected_by_type: "system".to_owned(),
            detected_by_id: Some("task-service".to_owned()),
            authorization_basis: "adaptive_task_boundary".to_owned(),
            authorization_action: "task.adaptive.reject".to_owned(),
            explicit_event: "task.adaptive.replace.rejected".to_owned(),
            authorization_occurred_at: NOW.to_owned(),
            idempotency_key: "adaptive-boundary-reconciliation-test".to_owned(),
            created_at: NOW.to_owned(),
        },
    )
    .await
    .expect("canonical conflict creates");

    let reconciliation = ProjectOrchestrationRepo::create_project_reconciliation(
        db,
        CreateProjectReconciliation {
            id: "reconciliation-record".to_owned(),
            project_id: PROJECT_ID.to_owned(),
            conflict_id: conflict.id,
            record_type: "task".to_owned(),
            record_id: TASK_ID.to_owned(),
            record_revision: "1".to_owned(),
            record_digest: "digest-conflicting".to_owned(),
            governing_record_type: "execution_baseline".to_owned(),
            governing_record_id: BASELINE_ID.to_owned(),
            governing_record_revision: "2".to_owned(),
            governing_record_digest: "digest-governing".to_owned(),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("reconciliation record creates");
    assert_eq!(reconciliation.state, "required");
    reconciliation.id
}

fn user_authorization(action: &str, event_id: &str) -> AuthorizationProvenance {
    AuthorizationProvenance {
        principal: PrincipalRef {
            kind: PrincipalKind::User,
            id: USER_ID.to_owned(),
            display_name: None,
        },
        authorization_basis: "interactive_user_reconciliation_resolution".to_owned(),
        action: action.to_owned(),
        event_id: event_id.to_owned(),
        occurred_at: NOW.to_owned(),
    }
}

fn resolve_request(
    action: ReconciliationResolutionAction,
    reason: &str,
    idempotency_key: &str,
    replacement_ref: Option<ReconciliationReplacementRef>,
) -> ResolveProjectReconciliationRequest {
    ResolveProjectReconciliationRequest {
        mutation: MutationEnvelope {
            expected_version: 1,
            expected_digest: None,
            idempotency_key: idempotency_key.to_owned(),
            deduplication_key: None,
            authorization: user_authorization("project.reconciliation.resolve", idempotency_key),
        },
        action,
        replacement_ref,
        reason: reason.to_owned(),
    }
}

/// F10: the reconciliation this run got stuck on now has a real, registered
/// product exit -- list, detail, and resolve all go through the shared
/// service, and a successful resolve wakes the exact affected Task.
#[tokio::test]
async fn required_reconciliation_has_a_reachable_resolve_target() {
    let db = fixture().await;
    let reconciliation_id = seed_adaptive_boundary_reconciliation(&db).await;
    let service =
        ProjectReconciliationService::new(Arc::clone(&db), Arc::new(events::EventBus::new(16)));

    let listed = service
        .list(PROJECT_ID, USER_ID, None, 20)
        .await
        .expect("list succeeds");
    assert_eq!(listed.items.len(), 1);
    assert_eq!(listed.items[0].id, reconciliation_id);
    assert_eq!(listed.items[0].state, ReconciliationState::Required);
    assert_eq!(listed.items[0].allowed_actions.len(), 5);
    assert_eq!(
        listed.items[0].conflict.conflict_code,
        "adaptive_task_boundary_crossed"
    );
    assert_eq!(listed.items[0].affected.record_type, "task");
    assert_eq!(listed.items[0].affected.record_id, TASK_ID);
    assert_eq!(listed.items[0].governing.record_type, "execution_baseline");

    let detail = service
        .get(PROJECT_ID, USER_ID, &reconciliation_id)
        .await
        .expect("get succeeds");
    assert_eq!(detail.id, reconciliation_id);

    let response = service
        .resolve(
            PROJECT_ID,
            USER_ID,
            &reconciliation_id,
            resolve_request(
                ReconciliationResolutionAction::Retained,
                "The approved envelope remains authoritative; the adaptive replace is rejected.",
                "resolve-retained-1",
                None,
            ),
        )
        .await
        .expect("resolve succeeds");
    assert_eq!(response.reconciliation.state, ReconciliationState::Retained);
    assert!(response.reconciliation.allowed_actions.is_empty());
    assert!(!response.receipt_id.is_empty());
    assert!(!response.event_id.is_empty());
    assert!(
        response.dispatch_woken,
        "resolving a Task-scoped reconciliation must wake that Task's dispatch"
    );

    let resolved = service
        .get(PROJECT_ID, USER_ID, &reconciliation_id)
        .await
        .expect("get after resolve succeeds");
    assert_eq!(resolved.state, ReconciliationState::Retained);
    let resolution = resolved.resolution.expect("resolution recorded");
    assert_eq!(resolution.action, ReconciliationResolutionAction::Retained);
    assert_eq!(resolution.principal.id, USER_ID);
    assert_eq!(
        resolution.reason,
        "The approved envelope remains authoritative; the adaptive replace is rejected."
    );
}

#[tokio::test]
async fn resolve_requires_an_interactive_user_principal() {
    let db = fixture().await;
    let reconciliation_id = seed_adaptive_boundary_reconciliation(&db).await;
    let service =
        ProjectReconciliationService::new(Arc::clone(&db), Arc::new(events::EventBus::new(16)));

    let mut request = resolve_request(
        ReconciliationResolutionAction::Retained,
        "An agent should never be able to self-resolve this.",
        "resolve-agent-1",
        None,
    );
    request.mutation.authorization.principal = PrincipalRef {
        kind: PrincipalKind::Agent,
        id: AGENT_ID.to_owned(),
        display_name: None,
    };

    let error = service
        .resolve(PROJECT_ID, USER_ID, &reconciliation_id, request)
        .await
        .expect_err("an agent principal must be rejected");
    assert!(matches!(error, ServiceError::AuthorizationDenied { .. }));
}

#[tokio::test]
async fn resolve_requires_an_exact_replacement_ref_for_revised_and_rejects_it_otherwise() {
    let db = fixture().await;
    let reconciliation_id = seed_adaptive_boundary_reconciliation(&db).await;
    let service =
        ProjectReconciliationService::new(Arc::clone(&db), Arc::new(events::EventBus::new(16)));

    let missing_replacement = service
        .resolve(
            PROJECT_ID,
            USER_ID,
            &reconciliation_id,
            resolve_request(
                ReconciliationResolutionAction::Revised,
                "A corrected baseline revision now governs this Task.",
                "resolve-revised-missing-ref",
                None,
            ),
        )
        .await
        .expect_err("revised without a replacement_ref must be rejected");
    assert!(matches!(
        missing_replacement,
        ServiceError::InvalidOperation { .. }
    ));

    let unexpected_replacement = service
        .resolve(
            PROJECT_ID,
            USER_ID,
            &reconciliation_id,
            resolve_request(
                ReconciliationResolutionAction::Retained,
                "Retaining the governing record needs no replacement.",
                "resolve-retained-with-ref",
                Some(ReconciliationReplacementRef {
                    record_type: "execution_baseline".to_owned(),
                    record_id: "should-not-be-here".to_owned(),
                    record_revision: None,
                }),
            ),
        )
        .await
        .expect_err("retained with a replacement_ref must be rejected");
    assert!(matches!(
        unexpected_replacement,
        ServiceError::InvalidOperation { .. }
    ));

    let response = service
        .resolve(
            PROJECT_ID,
            USER_ID,
            &reconciliation_id,
            resolve_request(
                ReconciliationResolutionAction::Revised,
                "A corrected baseline revision now governs this Task.",
                "resolve-revised-2",
                Some(ReconciliationReplacementRef {
                    record_type: "execution_baseline".to_owned(),
                    record_id: "reconciliation-baseline-successor".to_owned(),
                    record_revision: Some("3".to_owned()),
                }),
            ),
        )
        .await
        .expect("revised with an exact replacement_ref succeeds");
    assert_eq!(response.reconciliation.state, ReconciliationState::Revised);
    let resolution = response
        .reconciliation
        .resolution
        .expect("resolution recorded");
    let replacement_ref = resolution
        .replacement_ref
        .expect("replacement_ref recorded");
    assert_eq!(replacement_ref.record_type, "execution_baseline");
    assert_eq!(
        replacement_ref.record_id,
        "reconciliation-baseline-successor"
    );
    assert_eq!(replacement_ref.record_revision.as_deref(), Some("3"));
}

#[tokio::test]
async fn resolve_is_replay_exact_and_conflicts_on_a_stale_version() {
    let db = fixture().await;
    let reconciliation_id = seed_adaptive_boundary_reconciliation(&db).await;
    let service =
        ProjectReconciliationService::new(Arc::clone(&db), Arc::new(events::EventBus::new(16)));

    let request = resolve_request(
        ReconciliationResolutionAction::Cancelled,
        "The affected Task was cancelled instead.",
        "resolve-cancelled-replay",
        None,
    );
    let first = service
        .resolve(PROJECT_ID, USER_ID, &reconciliation_id, request.clone())
        .await
        .expect("first resolve succeeds");
    let replay = service
        .resolve(PROJECT_ID, USER_ID, &reconciliation_id, request)
        .await
        .expect("identical replay returns the same committed outcome, not a failure");
    assert_eq!(first.reconciliation.state, replay.reconciliation.state);
    assert_eq!(first.receipt_id, replay.receipt_id);

    let stale = service
        .resolve(
            PROJECT_ID,
            USER_ID,
            &reconciliation_id,
            resolve_request(
                ReconciliationResolutionAction::Invalidated,
                "A second, different resolution should not silently apply.",
                "resolve-stale-version",
                None,
            ),
        )
        .await
        .expect_err("a resolved reconciliation resolved again must conflict");
    assert!(matches!(stale, ServiceError::Db(DbError::VersionConflict)));
}

#[tokio::test]
async fn list_and_get_are_scoped_to_the_authorized_project() {
    let db = fixture().await;
    let reconciliation_id = seed_adaptive_boundary_reconciliation(&db).await;
    let service =
        ProjectReconciliationService::new(Arc::clone(&db), Arc::new(events::EventBus::new(16)));

    let wrong_project = service
        .get("not-this-project", USER_ID, &reconciliation_id)
        .await;
    assert!(matches!(
        wrong_project,
        Err(ServiceError::NotFound {
            entity: "project",
            ..
        })
    ));

    let wrong_user = service
        .get(PROJECT_ID, "not-this-user", &reconciliation_id)
        .await;
    assert!(matches!(
        wrong_user,
        Err(ServiceError::NotFound {
            entity: "project",
            ..
        })
    ));
}
