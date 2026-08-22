//! TASK-02 acceptance coverage.
//!
//! A Project Agent's free-form output is untrusted data.  It may be retained
//! as a pending proposal/diagnostic, but it cannot make a Task terminal, create
//! review/validation/git/evidence records, or rewrite Task history.  Reads back
//! to the planner contain only bounded, server-owned metadata.

use std::sync::Arc;

use db::{
    create_sqlite_pool, run_migrations, AgentActionApprovalDecision, AgentRepo, AgentStatus,
    CreateAgentIdentity, CreateAgentProfile, CreateProject, CreateTask, ProjectRepo, SqliteDb,
    TaskRepo,
};
use forge_agent_host::{
    AgentHostError, CanonicalScope, CanonicalScopeType, ForgeToolProvider, WorkspaceAccess,
};
use serde_json::{json, Value};
use services::{AgentActionService, ApproveActionInput, CoordinationToolProvider, TaskService};
use sqlx::Row;

const USER_ID: &str = "task02-prose-user";
const AGENT_ID: &str = "task02-prose-project-agent";
const PROFILE_ID: &str = "task02-prose-project-agent-profile";
const PROJECT_ID: &str = "task02-prose-project";
const TASK_ID: &str = "task02-prose-task";
const NOW: &str = "2026-08-21T00:00:00.000Z";
const CLAIM: &str =
    "I edited the repository, tested it, merged the change, deployed it, and validated the result.";

struct Fixture {
    db: Arc<SqliteDb>,
    provider: CoordinationToolProvider,
    scope: CanonicalScope,
}

async fn fixture() -> Fixture {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("SQLite pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));

    sqlx::query(
        "INSERT INTO user
         (id, email, password_hash, display_name, created_at, updated_at)
         VALUES (?, ?, 'test', 'TASK-02 user', ?, ?)",
    )
    .bind(USER_ID)
    .bind("task02-prose@example.test")
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("user creates");

    AgentRepo::create_identity_with_profile(
        &*db,
        CreateAgentIdentity {
            id: AGENT_ID.to_owned(),
            name: "TASK-02 Project Agent".to_owned(),
            description: None,
            max_concurrent_tasks: 2,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some(USER_ID.to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling:
                r#"{"permissions":["read_project","propose_message","propose_review","propose_task"]}"#
                    .to_owned(),
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
            tool_policy_json:
                r#"{"permissions":["read_project","propose_message","propose_review","propose_task"]}"#
                    .to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("Project Agent identity/profile creates");

    ProjectRepo::create_with_agent_binding(
        &*db,
        CreateProject {
            id: PROJECT_ID.to_owned(),
            name: "TASK-02 prose boundary".to_owned(),
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
    .expect("Project and active Project Agent binding create");

    // The broad proposal permissions are intentionally explicit so the test
    // exercises the shared service boundary rather than a missing-permission
    // shortcut.  They still do not grant execution, validation, or release.
    sqlx::query(
        "UPDATE project_agent_binding
         SET permission_ceiling_json = ?
         WHERE project_id = ? AND identity_id = ? AND state = 'active'",
    )
    .bind(r#"{"permissions":["read_project","propose_message","propose_review","propose_task"]}"#)
    .bind(PROJECT_ID)
    .bind(AGENT_ID)
    .execute(db.pool())
    .await
    .expect("Project Agent permissions set");

    TaskRepo::create(
        &*db,
        CreateTask {
            id: TASK_ID.to_owned(),
            project_id: PROJECT_ID.to_owned(),
            repo_id: None,
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "Awaiting authoritative repository delivery".to_owned(),
            description: Some(
                "The Task remains pending until a Worker/reviewer result arrives.".to_owned(),
            ),
            task_type: "task".to_owned(),
            status: "todo".to_owned(),
            is_automation: false,
            priority: 3,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("pending Task creates");

    let provider = CoordinationToolProvider::new(Arc::clone(&db));
    provider.set_task_service(Arc::new(TaskService::new(
        Arc::clone(&db),
        Arc::new(events::EventBus::new(16)),
    )));
    Fixture {
        db,
        provider,
        scope: CanonicalScope {
            scope_type: CanonicalScopeType::Project,
            scope_id: PROJECT_ID.to_owned(),
            workspace_access: WorkspaceAccess::Deny,
        },
    }
}

fn claim_prose_payload() -> Value {
    json!({
        "summary": CLAIM,
        "status": "done",
        "delivery": "merged and deployed",
        "review": "passed",
        "validation": "passed",
        "git": {"commit": "model-claimed-commit", "merged": true},
        "evidence": ["model-claimed-proof"],
    })
}

fn proposal_arguments(payload: Value, key: &str) -> Value {
    json!({
        "payload": payload,
        "dedupe_key": key,
        "correlation_id": format!("correlation-{key}"),
    })
}

async fn count(db: &SqliteDb, sql: &str, bind: &str) -> i64 {
    sqlx::query_scalar(sql)
        .bind(bind)
        .fetch_one(db.pool())
        .await
        .expect("count query")
}

async fn assert_no_authoritative_records(fixture: &Fixture) {
    // Task delivery/workflow history.
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM execution WHERE task_id = ?",
            TASK_ID,
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM review WHERE task_id = ?",
            TASK_ID,
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM transition_log WHERE task_id = ?",
            TASK_ID,
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM workspace_lease WHERE task_id = ?",
            TASK_ID,
        )
        .await,
        0
    );

    // Validation, repository/merge metadata, and Task evidence.
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM project_milestone_check_result WHERE project_id = ?",
            PROJECT_ID,
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM pr_metadata WHERE task_id = ?",
            TASK_ID,
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM task_media WHERE task_id = ?",
            TASK_ID,
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM project_media_attachment WHERE project_id = ?",
            PROJECT_ID,
        )
        .await,
        0
    );

    // Readiness/release and governance are also untouched by prose.
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM project_readiness_snapshot WHERE project_id = ?",
            PROJECT_ID,
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM project_release WHERE project_id = ?",
            PROJECT_ID,
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM project_task_governance WHERE task_id = ?",
            TASK_ID,
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM agent_action_execution WHERE action_id IN
             (SELECT id FROM agent_action WHERE scope_type = 'project' AND scope_id = ?)",
            PROJECT_ID,
        )
        .await,
        0,
        "untrusted claims have no executed authoritative action"
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt
             WHERE scope_type = 'project' AND scope_id = ?",
            PROJECT_ID,
        )
        .await,
        0,
        "no typed domain command was committed"
    );
}

#[tokio::test]
async fn project_agent_prose_stays_pending_and_reads_are_sanitized() {
    let fixture = fixture().await;

    // A model can submit prose through a generic proposal surface, but this
    // remains a pending action.  The response does not echo the claim or
    // expose a caller-supplied status/result field.
    let message = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.scope,
            "message.send",
            proposal_arguments(claim_prose_payload(), "task02-message-claim"),
        )
        .await
        .expect("prose proposal is retained as an untrusted pending action");
    assert_eq!(message["operation"], "message.send");
    assert_eq!(message["status"], "pending_approval");
    assert!(!message.to_string().contains(CLAIM));

    // A second claim-shaped proposal tries to make review/validation/git
    // fields look authoritative.  Project orchestration is not Charter-backed
    // in this fixture, so the server records the attempt as denied; in either
    // case no review or validation materializer is available on this path.
    let review = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.scope,
            "review.request",
            proposal_arguments(
                json!({
                    "task_id": TASK_ID,
                    "result": "passed",
                    "validation": "passed",
                    "commit": "model-claimed-commit",
                    "summary": CLAIM,
                }),
                "task02-review-claim",
            ),
        )
        .await
        .expect("review claim is retained as an untrusted action result");
    assert_eq!(review["operation"], "review.request");
    assert_eq!(review["status"], "denied");
    assert_eq!(review["policy_result"], "denied");
    assert!(!review.to_string().contains(CLAIM));

    // Even if a broad policy accidentally included approval permission, the
    // proposer cannot approve its own protected action.  With the normal
    // Project Agent ceiling this fails earlier at the same authority boundary.
    let self_approval = AgentActionService::new(Arc::clone(&fixture.db))
        .approve(ApproveActionInput {
            action_id: review["id"].as_str().expect("review action id").to_owned(),
            expected_version: review["version"].as_i64().expect("review action version"),
            approver_identity_id: AGENT_ID.to_owned(),
            decision: AgentActionApprovalDecision::Approved,
            reason: Some("model prose says it is already validated".to_owned()),
        })
        .await;
    assert!(self_approval.is_err(), "Project Agent cannot self-attest");

    // The typed Task boundary rejects status/delivery/review/validation/git/
    // evidence fields rather than treating them as a Task result.  The
    // structured error is sanitized and does not echo the model's prose.
    let malformed_task = fixture
        .provider
        .propose(
            AGENT_ID,
            &fixture.scope,
            "task.propose",
            proposal_arguments(
                json!({
                    "title": "Claimed completed Task",
                    "description": CLAIM,
                    "task_type": "implementation",
                    "priority": 3,
                    "merge_config": null,
                    "role_assignments": null,
                    "governance": null,
                    "status": "done",
                    "delivery": "deployed",
                    "review": "passed",
                    "validation": "passed",
                    "git": "merged",
                    "evidence": ["claimed-proof"],
                }),
                "task02-malformed-claim",
            ),
        )
        .await;
    let malformed_task = match malformed_task {
        Err(AgentHostError::StructuredOutcome(outcome)) => *outcome,
        other => panic!("claim-shaped Task payload must be rejected safely: {other:?}"),
    };
    assert_eq!(malformed_task.code, api_types::OutcomeCode::ValidationError);
    assert_eq!(malformed_task.status, api_types::OutcomeStatus::Failed);
    assert!(!serde_json::to_string(&malformed_task)
        .expect("structured error serializes")
        .contains(CLAIM));

    // The only Project-Agent-visible delivery is bounded metadata.  Neither
    // inbox/action projections nor work/events may leak the raw claim.
    let delivery = fixture
        .provider
        .read(
            AGENT_ID,
            &fixture.scope,
            "delivery.read",
            json!({"limit": 20}),
        )
        .await
        .expect("sanitized delivery projection");
    assert!(!delivery.to_string().contains(CLAIM));
    assert!(delivery["actions"].as_array().is_some());
    for action in delivery["actions"].as_array().expect("action list") {
        assert!(action.get("payload_json").is_none());
        assert!(action.get("result_json").is_none());
        assert!(action.get("outcome_json").is_none());
    }

    let work = fixture
        .provider
        .read(AGENT_ID, &fixture.scope, "work.read", json!({"limit": 20}))
        .await
        .expect("sanitized work projection");
    assert_eq!(work["items"][0]["id"], TASK_ID);
    assert_eq!(work["items"][0]["status"], "todo");
    assert!(!work.to_string().contains(CLAIM));

    let events = fixture
        .provider
        .read(
            AGENT_ID,
            &fixture.scope,
            "events.read",
            json!({"limit": 20}),
        )
        .await
        .expect("sanitized event projection");
    assert!(!events.to_string().contains(CLAIM));
    for event in events["items"].as_array().expect("event list") {
        assert!(event.get("payload_json").is_none());
        assert!(event.get("result_json").is_none());
    }

    let task = TaskRepo::get_by_id(&*fixture.db, TASK_ID, false)
        .await
        .expect("Task lookup")
        .expect("Task remains present");
    assert_eq!(task.status, "todo");
    assert_eq!(task.version, 1);
    assert_no_authoritative_records(&fixture).await;

    // The generic proposal rows are intentionally retained as untrusted
    // diagnostic input, while their exposed projection remains sanitized.
    let stored_claim: String = sqlx::query_scalar(
        "SELECT payload_json FROM agent_action
         WHERE scope_type = 'project' AND scope_id = ? AND operation = 'message.send'",
    )
    .bind(PROJECT_ID)
    .fetch_one(fixture.db.pool())
    .await
    .expect("untrusted proposal payload remains auditable");
    assert!(stored_claim.contains(CLAIM));
    let status_row = sqlx::query("SELECT status, version FROM task WHERE id = ?")
        .bind(TASK_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("Task status/version row");
    assert_eq!(status_row.get::<String, _>("status"), "todo");
    assert_eq!(status_row.get::<i64, _>("version"), 1);
}
