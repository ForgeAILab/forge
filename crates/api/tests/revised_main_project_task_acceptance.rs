#![allow(dead_code)]

//! Acceptance coverage for the singular Main/Project Agent model.

mod common;

use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use db::AgentContextScopeRepo;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tower::ServiceExt;

const PROVIDER_SECRET: &str = "revised-acceptance-provider-secret";

struct ProjectAuthorityFixture {
    project_id: String,
    charter_id: String,
    charter_revision_id: String,
    charter_content_digest: String,
    charter_render_digest: String,
    milestone_id: String,
    milestone_definition_revision_id: String,
}

struct TaskGovernanceFixture {
    charter_revision_id: String,
    baseline_id: String,
    baseline_revision_id: String,
    milestone_id: String,
}

#[tokio::test]
async fn main_project_handoff_project_task_worker_and_main_denial() {
    let workspace = common::TestDir::new("revised-main-project-task-acceptance");
    let harness = common::test_app(workspace.path(), "revised-main-project-task").await;
    harness
        .state
        .workflow_template_service
        .initialize()
        .await
        .expect("builtin workflow templates initialize");
    let app = &harness.app;
    let token = common::test_jwt();

    // Main, Project, and Worker are separate account-owned identities.  The
    // Worker is deliberately left unbound; Task assignment is its only
    // route into a repository Workspace.
    let main = connect_embedded(
        app,
        &token,
        "acceptance-main",
        &["read_account", "read_project", "handoff"],
    )
    .await;
    let project_agent = connect_embedded(
        app,
        &token,
        "acceptance-project",
        &["read_project", "propose_task", "read_task"],
    )
    .await;
    let worker = connect_embedded(
        app,
        &token,
        "acceptance-worker",
        &[
            "read_project",
            "read_task",
            "task_read",
            "task_write",
            "approve_actions",
        ],
    )
    .await;

    let main_identity = required_string(&main, &["agent", "id"]);
    let main_profile = required_string(&main, &["profile", "id"]);
    let project_identity = required_string(&project_agent, &["agent", "id"]);
    let project_profile = required_string(&project_agent, &["profile", "id"]);
    let worker_identity = required_string(&worker, &["agent", "id"]);
    let worker_profile = required_string(&worker, &["profile", "id"]);

    let main_binding = request_json(
        app,
        Method::PUT,
        "/api/v1/account/main-agent",
        &token,
        json!({
            "identity_id": main_identity,
            "profile_id": main_profile,
            "expected_version": 0,
            "autonomy_policy": {}
        }),
        &[StatusCode::OK, StatusCode::CREATED],
    )
    .await;
    let main_chat_id = required_string(&main_binding, &["chat_id"]);

    // Product Genesis binds approved intent, the selected Project Agent, the
    // Project, and its Project Chat in one replay-safe transaction.
    let genesis = request_json(
        app,
        Method::POST,
        "/api/v1/account/main-agent/product-genesis",
        &token,
        json!({
            "maturity": "mvp",
            "initial_idea": "A bounded Todo list with auditable Task delegation.",
            "preferred_project_agent_identity_id": project_identity
        }),
        &[StatusCode::CREATED],
    )
    .await;
    let genesis_id = required_string(&genesis, &["session", "id"]);
    let charter_id = "revised-acceptance-charter";
    let charter_content = todo_charter_content();
    let rendered_charter = services::render_and_digest_charter(&charter_content);
    let saved_charter = request_json(
        app,
        Method::POST,
        &format!("/api/v1/account/main-agent/product-genesis/{genesis_id}/charter/revisions"),
        &token,
        json!({
            "mutation": {
                "expected_version": 1,
                "idempotency_key": "revised-acceptance-charter-save",
                "authorization": user_authorization(
                    "project_charter.revision.save",
                    "revised-acceptance-charter-save-event"
                )
            },
            "charter_id": charter_id,
            "project_mode": "compact",
            "maturity": "mvp",
            "content": charter_content,
            "rendered_view": rendered_charter.rendered_view,
            "render_version": rendered_charter.render_version,
            "provenance": user_provenance("Approved Todo Charter")
        }),
        &[StatusCode::CREATED],
    )
    .await;
    let charter_revision_id = required_string(&saved_charter, &["id"]);
    let charter_projection = request_json(
        app,
        Method::GET,
        &format!("/api/v1/account/main-agent/product-genesis/{genesis_id}/charter"),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let charter_version = charter_projection["charter"]["version"]
        .as_i64()
        .expect("Charter version");
    let charter_approval = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/account/main-agent/product-genesis/{genesis_id}/charter/revisions/{charter_revision_id}/approve"
        ),
        &token,
        json!({
            "mutation": {
                "expected_version": charter_version,
                "expected_digest": rendered_charter.content_digest,
                "idempotency_key": "revised-acceptance-charter-approve",
                "authorization": user_authorization(
                    "product_genesis.charter_approval",
                    "revised-acceptance-charter-approve-event"
                )
            },
            "charter_id": charter_id,
            "revision_id": charter_revision_id,
            "content_digest": rendered_charter.content_digest,
            "render_digest": rendered_charter.render_digest,
            "expected_charter_version": charter_version,
            "approved_project_name": "Todo acceptance project",
            "approved_project_slug": "todo-acceptance-project",
            "project_mode": "compact",
            "selected_project_agent_identity_id": project_identity,
            "selected_project_agent_profile_revision_id": project_profile,
            "selected_project_agent_operating_skill_revision": "forge.project.orchestration/v1@1",
            "selected_project_agent_policy_digest": project_policy_digest(
                &project_agent["profile"]["tool_policy"]
            )
        }),
        &[StatusCode::CREATED],
    )
    .await;
    let approval_id = required_string(&charter_approval, &["id"]);
    let created_project = request_json(
        app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({
            "approval_id": approval_id,
            "idempotency_key": "revised-acceptance-project-create",
            "authorization": user_authorization(
                "product_genesis.create_project_from_approval",
                "revised-acceptance-project-create-event"
            )
        }),
        &[StatusCode::CREATED],
    )
    .await;
    let project_id = required_string(&created_project, &["project_id"]);
    let project_before_update = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}"),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    request_json(
        app,
        Method::PATCH,
        &format!("/api/v1/projects/{project_id}"),
        &token,
        json!({
            "version": project_before_update["version"],
            "default_review_config": {
                "ci_steps": ["python3 -m unittest -v"],
                "review_prompt": "Independently verify the Todo CLI."
            }
        }),
        &[StatusCode::OK],
    )
    .await;
    let project = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}"),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let milestone_id = required_string(&project, &["primary_milestone_id"]);
    let milestone = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}/milestones/{milestone_id}"),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let milestone_definition_revision_id = required_string(&milestone, &["definition_revision_id"]);

    let project_binding = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}/project-agent"),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let project_chat_id = required_string(&project_binding, &["chat_id"]);
    assert_eq!(
        required_string(&project_binding, &["identity_id"]),
        required_string(&project_agent, &["agent", "id"])
    );

    // Core Main/Project chats are continuity scopes, not repository sessions.
    for (identity_id, profile_id, chat_id) in [
        (
            main_identity.as_str(),
            main_profile.as_str(),
            main_chat_id.as_str(),
        ),
        (
            project_identity.as_str(),
            project_profile.as_str(),
            project_chat_id.as_str(),
        ),
    ] {
        let session = request_json(
            app,
            Method::POST,
            &format!("/api/v1/agents/{identity_id}/sessions"),
            &token,
            json!({
                "profile_id": profile_id,
                "scope": { "type": "agent_chat", "chat_id": chat_id }
            }),
            &[StatusCode::OK, StatusCode::CREATED],
        )
        .await;
        let scope_id = required_string(&session, &["context_scope_id"]);
        let scope_row = AgentContextScopeRepo::get_context_scope(&*harness.state.db, &scope_id)
            .await
            .expect("core chat scope reads")
            .expect("core chat scope exists");
        assert_eq!(scope_row.workspace_access, "deny");
    }

    // Genesis provisioning already registered a primary repository with an
    // initial commit, which keeps the subsequent TaskService claim on the
    // normal Workspace path instead of reducing the worker assertion to
    // metadata.
    let repos = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}/repos"),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let provisioned_path = required_string(&repos["items"][0], &["local_path"]);
    assert_eq!(
        git_default_branch(std::path::Path::new(&provisioned_path)),
        "main",
        "provisioned Project repository is initialized on main"
    );

    // The autonomous_v1 template has a canonical Worker role, which lets the
    // native Task session prove task_write without pretending that Project
    // Chat itself has repository authority.
    let _workflow = request_json(
        app,
        Method::PUT,
        &format!("/api/v1/projects/{project_id}/workflow"),
        &token,
        json!({ "template_name": "autonomous_v1" }),
        &[StatusCode::OK],
    )
    .await;
    let task_governance = create_active_execution_baseline(
        app,
        &token,
        ProjectAuthorityFixture {
            project_id: project_id.clone(),
            charter_id: charter_id.to_owned(),
            charter_revision_id: charter_revision_id.clone(),
            charter_content_digest: rendered_charter.content_digest.clone(),
            charter_render_digest: rendered_charter.render_digest.clone(),
            milestone_id: milestone_id.clone(),
            milestone_definition_revision_id,
        },
    )
    .await;

    let chats = request_json(
        app,
        Method::GET,
        "/api/v1/agent-chats",
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let chat_items = chats
        .get("items")
        .and_then(Value::as_array)
        .expect("chat switcher items");
    assert_eq!(
        chat_items
            .iter()
            .filter(|item| item.get("kind").and_then(Value::as_str) == Some("main"))
            .count(),
        1
    );
    assert_eq!(
        chat_items
            .iter()
            .filter(|item| item.get("project_id").and_then(Value::as_str) == Some(&project_id))
            .count(),
        1
    );
    assert!(chat_items.iter().all(|item| {
        let identity = item.get("identity_id").and_then(Value::as_str);
        identity != Some(&worker_identity)
    }));

    let handoff = request_json(
        app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/agent-handoffs"),
        &token,
        json!({
            "content": "Approved Todo brief: implement the smallest useful slice.",
            "dedupe_key": "revised-acceptance-handoff-1"
        }),
        &[StatusCode::OK, StatusCode::CREATED],
    )
    .await;
    assert_eq!(required_string(&handoff, &["source_chat_id"]), main_chat_id);
    assert_eq!(
        required_string(&handoff, &["target_chat_id"]),
        project_chat_id
    );
    assert!(matches!(
        required_string(&handoff, &["status"]).as_str(),
        "pending" | "delivered"
    ));
    assert!(handoff
        .get("target_message_id")
        .and_then(Value::as_str)
        .is_some_and(|id| !id.is_empty()));
    assert!(handoff
        .get("target_turn_job_id")
        .and_then(Value::as_str)
        .is_some_and(|id| !id.is_empty()));

    // A Main binding may discover and hand off, but it cannot submit the same
    // Task proposal operation even when it names a real Project.
    let denied = request_json(
        app,
        Method::POST,
        &format!("/api/v1/agents/{main_identity}/task-proposals"),
        &token,
        task_proposal_body(
            &project_id,
            "Main must not create this Task",
            "revised-acceptance-main-task-denial",
            &worker_identity,
            &task_governance,
        ),
        &[StatusCode::FORBIDDEN, StatusCode::NOT_FOUND],
    )
    .await;
    assert!(
        denied.get("id").is_none(),
        "denial must not create an action"
    );

    // Project Agent Task proposal remains an action envelope until the
    // existing approval/TaskService path commits the authoritative Task.
    let proposal = request_json(
        app,
        Method::POST,
        &format!("/api/v1/agents/{project_identity}/task-proposals"),
        &token,
        task_proposal_body(
            &project_id,
            "Implement Todo item",
            "revised-acceptance-project-task",
            &worker_identity,
            &task_governance,
        ),
        &[StatusCode::OK, StatusCode::CREATED],
    )
    .await;
    assert_eq!(required_string(&proposal, &["operation"]), "task.propose");
    let proposal_id = required_string(&proposal, &["id"]);
    let mut proposal_version = proposal
        .get("version")
        .and_then(Value::as_i64)
        .expect("proposal version");

    if proposal.get("status").and_then(Value::as_str) == Some("pending_approval") {
        let approved = request_json(
            app,
            Method::POST,
            &format!("/api/v1/actions/{proposal_id}/approve"),
            &token,
            json!({
                "expected_version": proposal_version,
                "approver_identity_id": worker_identity,
                "decision": "approved",
                "reason": "acceptance worker approval"
            }),
            &[StatusCode::OK],
        )
        .await;
        proposal_version = approved
            .get("version")
            .and_then(Value::as_i64)
            .expect("approved proposal version");
    }

    let executed = request_json(
        app,
        Method::POST,
        &format!("/api/v1/actions/{proposal_id}/execute-task"),
        &token,
        json!({
            "expected_version": proposal_version,
            "idempotency_key": "revised-acceptance-task-execution"
        }),
        &[StatusCode::OK],
    )
    .await;
    let task_id = required_string(&executed, &["task", "id"]);
    assert_eq!(
        executed["task"]["task_state_config"]["review"]["ci_steps"][0],
        "python3 -m unittest -v",
        "typed Task proposals must inherit the Project review policy just like direct Task creation"
    );
    assert_eq!(
        required_string(&executed, &["task", "project_id"]),
        project_id
    );

    // Enter through the existing TaskService claim/workspace path.  Calling
    // the service directly avoids the HTTP claim handler's provider start,
    // which would make a clean-data test depend on an external model.
    let claimed = harness
        .state
        .task_service
        .claim_task(
            task_id.clone(),
            services::Assignee::Agent(worker_identity.clone()),
            None,
        )
        .await
        .expect("TaskService claim creates the running Worker execution");
    assert_eq!(claimed.execution.status.to_string(), "running");

    let session = request_json(
        app,
        Method::POST,
        &format!("/api/v1/agents/{worker_identity}/sessions"),
        &token,
        json!({
            "profile_id": worker_profile,
            "scope": { "type": "task", "task_id": task_id, "role": "worker" }
        }),
        &[StatusCode::OK, StatusCode::CREATED],
    )
    .await;
    let scope_id = required_string(&session, &["context_scope_id"]);
    let scope = AgentContextScopeRepo::get_context_scope(&*harness.state.db, &scope_id)
        .await
        .expect("Task scope reads")
        .expect("Task scope exists");
    assert_eq!(scope.identity_id, worker_identity);
    assert_eq!(scope.scope_type, "task");
    assert_eq!(scope.scope_id, task_id);
    assert_eq!(scope.task_role.as_deref(), Some("worker"));
    assert_eq!(scope.workspace_access, "task_write");

    // The same Task scope is not transferable to the Main identity, even
    // though that identity owns the account and can see the Project chat.
    let main_task_session = request_json(
        app,
        Method::POST,
        &format!("/api/v1/agents/{main_identity}/sessions"),
        &token,
        json!({
            "profile_id": required_string(&main, &["profile", "id"]),
            "scope": { "type": "task", "task_id": task_id, "role": "worker" }
        }),
        &[StatusCode::FORBIDDEN, StatusCode::NOT_FOUND],
    )
    .await;
    assert!(
        main_task_session.get("context_scope_id").is_none(),
        "Main denial must not create a Task session"
    );
}

async fn create_active_execution_baseline(
    app: &Router,
    token: &str,
    authority: ProjectAuthorityFixture,
) -> TaskGovernanceFixture {
    let project = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}", authority.project_id),
        token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let project_version = project["version"]
        .as_i64()
        .expect("baseline Project version");
    let baseline_id = "revised-acceptance-baseline";
    let proposed = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-baseline",
            authority.project_id
        ),
        token,
        json!({
            "mutation": {
                "expected_version": project_version,
                "idempotency_key": "revised-acceptance-baseline-propose",
                "authorization": user_authorization(
                    "project.execution_baseline.propose",
                    "revised-acceptance-baseline-propose-event"
                )
            },
            "baseline_id": baseline_id
        }),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await;
    let baseline_version = proposed["baseline"]["version"]
        .as_i64()
        .expect("proposed baseline version");

    let release_policy: api_types::ExecutionBaselineReleasePolicy = serde_json::from_value(json!({
        "schema_version": services::EXECUTION_BASELINE_RELEASE_POLICY_SCHEMA,
        "revision": "revised-acceptance-policy-r1",
        "required_check_definition_revisions": [
            authority.milestone_definition_revision_id
        ],
        "reviewer_independence_rules": ["independent-reviewer"],
        "manual_attestation_rules": ["manual-attestation"],
        "waiver_rules": ["user-waiver"],
        "evidence_kinds": ["ci-log", "media"],
        "evidence_contexts": ["milestone"],
        "evidence_freshness_rules": ["current-milestone"],
        "dependency_rules": ["dependencies-green"],
        "stale_input_rules": ["stale-baseline-blocks"],
        "forbidden_side_effects": ["cross-project-write"],
        "known_issue_rules": ["known-issue-blocks"],
        "correction_rules": ["correction-required"],
        "purge_rules": ["purge-stale-evidence"]
    }))
    .expect("closed release policy parses");
    let release_policy_digest = api_types::canonical_digest_with_schema(
        services::EXECUTION_BASELINE_RELEASE_POLICY_SCHEMA,
        &release_policy,
    )
    .expect("release policy digest");
    let content = json!({
        "charter_revision": {
            "artifact_id": authority.charter_id,
            "revision_id": authority.charter_revision_id,
            "content_digest": authority.charter_content_digest,
            "render_version": "forge.project-charter/v1",
            "render_digest": authority.charter_render_digest
        },
        "document_revisions": [],
        "plan_item_ids": ["revised-acceptance-plan-item-1"],
        "milestone_ids": [authority.milestone_id],
        "milestone_definition_revision_ids": [
            authority.milestone_definition_revision_id
        ],
        "primary_milestone_id": authority.milestone_id,
        "release_policy_revision": "revised-acceptance-policy-r1",
        "release_policy_digest": release_policy_digest,
        "release_policy": release_policy,
        "acceptance_evidence_matrix": [],
        "capability_classes": ["repository_write"],
        "risk_classes": ["low"],
        "reviewer_independence_rules": ["independent-reviewer"],
        "elevated_operations": [],
        "adaptive_envelope": {
            "allowed_task_operations": ["split"],
            "fixed_outcomes": [],
            "fixed_acceptance": [],
            "fixed_risk_classes": ["low"],
            "forbidden_side_effects": [],
            "elevated_operations": []
        },
        "rollback_and_recovery": ["retry"],
        "exclusions": []
    });
    let typed_content: api_types::ExecutionBaselineContent =
        serde_json::from_value(content.clone()).expect("baseline content parses");
    let rendered =
        services::render_execution_baseline(&typed_content).expect("execution baseline renders");
    let saved = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-baseline/{baseline_id}/revisions",
            authority.project_id
        ),
        token,
        json!({
            "mutation": {
                "expected_version": baseline_version,
                "idempotency_key": "revised-acceptance-baseline-revision",
                "authorization": user_authorization(
                    "project.execution_baseline.revise",
                    "revised-acceptance-baseline-revision-event"
                )
            },
            "base_revision_id": null,
            "content": content,
            "rendered_view": rendered.rendered_view,
            "render_version": services::EXECUTION_BASELINE_RENDER_VERSION,
            "content_digest": rendered.content_digest,
            "render_digest": rendered.render_digest,
            "provenance": user_provenance("Todo execution baseline")
        }),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await;
    let baseline_revision_id = required_string(&saved, &["current_revision", "id"]);
    let revised_version = saved["baseline"]["version"]
        .as_i64()
        .expect("revised baseline version");
    let approved = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-baseline/{baseline_id}/revisions/{baseline_revision_id}/approve",
            authority.project_id
        ),
        token,
        json!({
            "mutation": {
                "expected_version": revised_version,
                "idempotency_key": "revised-acceptance-baseline-approve",
                "authorization": user_authorization(
                    "project.execution_baseline.approve",
                    "revised-acceptance-baseline-approve-event"
                )
            },
            "revision_id": baseline_revision_id,
            "content_digest": rendered.content_digest,
            "render_digest": rendered.render_digest,
            "expected_project_version": project_version + 1
        }),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await;
    let approval_id = required_string(&approved, &["approval", "id"]);
    let activated = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/execution-baseline/{baseline_id}/activate",
            authority.project_id
        ),
        token,
        json!({
            "mutation": {
                "expected_version": project_version + 1,
                "idempotency_key": "revised-acceptance-baseline-activate",
                "authorization": user_authorization(
                    "project.execution_baseline.activate",
                    "revised-acceptance-baseline-activate-event"
                )
            },
            "baseline_id": baseline_id,
            "revision_id": baseline_revision_id,
            "approval_id": approval_id,
            "content_digest": rendered.content_digest,
            "render_digest": rendered.render_digest
        }),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(activated["baseline"]["lifecycle"], json!("active"));
    TaskGovernanceFixture {
        charter_revision_id: authority.charter_revision_id,
        baseline_id: baseline_id.to_owned(),
        baseline_revision_id,
        milestone_id: authority.milestone_id,
    }
}

async fn connect_embedded(app: &Router, token: &str, name: &str, permissions: &[&str]) -> Value {
    let entry = request_json(
        app,
        Method::POST,
        "/api/v1/providers",
        token,
        json!({
            "provider": "openai_compatible",
            "label": name,
            "credential": PROVIDER_SECRET,
            "base_url": "https://8.8.8.8"
        }),
        &[StatusCode::OK],
    )
    .await;
    request_json(
        app,
        Method::POST,
        "/api/v1/embedded-agents",
        token,
        json!({
            "name": name,
            "description": "V071 acceptance identity",
            "credential_id": entry["id"],
            "model": "acceptance-model",
            "account_permission_ceiling": { "permissions": permissions },
            "tool_policy": { "allowed": permissions }
        }),
        &[StatusCode::OK],
    )
    .await
}

fn task_proposal_body(
    project_id: &str,
    title: &str,
    dedupe_key: &str,
    worker_id: &str,
    governance: &TaskGovernanceFixture,
) -> Value {
    json!({
        "project_id": project_id,
        "title": title,
        "description": "Acceptance task",
        "role_assignments": [{
            "role_name": "worker",
            "assignee_type": "agent",
            "assignee_id": worker_id
        }],
        "governance": {
            "charter_revision_id": governance.charter_revision_id,
            "baseline_id": governance.baseline_id,
            "baseline_revision_id": governance.baseline_revision_id,
            "plan_item_id": "revised-acceptance-plan-item-1",
            "milestone_id": governance.milestone_id,
            "document_revision_ids": [],
            "capability_class": "repository_write",
            "risk_class": "low",
            "provenance": {"source": "revised-main-project-task-acceptance"}
        },
        "dedupe_key": dedupe_key,
        "correlation_id": format!("{dedupe_key}-correlation")
    })
}

fn todo_charter_content() -> api_types::ProjectCharterContent {
    serde_json::from_value(json!({
        "identity": {
            "working_name": "Todo acceptance project",
            "slug_proposal": "todo-acceptance-project",
            "one_line_vision": "A bounded Todo workflow delegated through Forge.",
            "maturity": "mvp"
        },
        "problem_and_people": {
            "problem_or_opportunity": "Todo work needs a durable, auditable agent handoff.",
            "target_users": ["Forge users"],
            "beneficiaries": ["Project collaborators"]
        },
        "core_experience": {
            "primary_outcome": "A Project Agent creates and delegates one bounded Todo Task."
        },
        "scope": {
            "must_have_outcomes": ["Create, execute, and validate one Todo Task."],
            "explicit_non_goals": ["Cross-Project mutation"]
        },
        "success": {
            "success_signals": ["The Worker receives only a Task-scoped WorkspaceLease."],
            "acceptance_statements": ["Main cannot manage Project Tasks."]
        },
        "constraints_and_risks": {
            "product": ["Single-user local-first operation."],
            "technology": ["Use the existing Forge workflow and repository runtime."],
            "security_privacy_compliance": ["Explicit user approval is required."]
        },
        "knowledge_ledger": {"items": []}
    }))
    .expect("Todo Charter content parses")
}

fn user_authorization(action: &str, event_id: &str) -> Value {
    json!({
        "principal": {"kind": "user", "id": "test-user-id"},
        "authorization_basis": "explicit_user_authorization",
        "action": action,
        "event_id": event_id,
        "occurred_at": db::now_rfc3339()
    })
}

fn user_provenance(summary: &str) -> Value {
    json!({
        "author": {"kind": "user", "id": "test-user-id"},
        "operating_skill_revision": "forge.project.orchestration/v1@1",
        "source_refs": [],
        "change_summary": summary
    })
}

fn project_policy_digest(policy: &Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"forge.project-agent-policy/v1\0");
    hasher.update(policy.to_string().as_bytes());
    hex::encode(hasher.finalize())
}

async fn request_json(
    app: &Router,
    method: Method,
    uri: &str,
    token: &str,
    body: Value,
    expected_statuses: &[StatusCode],
) -> Value {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::from(
            serde_json::to_vec(&body).expect("request JSON serializes"),
        ))
        .expect("request builds");
    let response = app.clone().oneshot(request).await.expect("router responds");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body reads");
    assert!(
        expected_statuses.contains(&status),
        "unexpected {status} from {uri}: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("response JSON parses")
}

fn required_string(value: &Value, path: &[&str]) -> String {
    let mut current = value;
    for segment in path {
        current = current
            .get(*segment)
            .unwrap_or_else(|| panic!("missing JSON field {}", path.join(".")));
    }
    current
        .as_str()
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| panic!("JSON field {} is not a non-empty string", path.join(".")))
        .to_owned()
}

fn git_default_branch(path: &std::path::Path) -> String {
    let output = std::process::Command::new("git")
        .args(["symbolic-ref", "--short", "HEAD"])
        .current_dir(path)
        .output()
        .expect("git default branch reads");
    assert!(output.status.success(), "git branch command succeeds");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}
