//! Deterministic probe for the Project Agent execution-baseline tool path.
//!
//! Replays the exact `ForgeToolProvider::propose` call an agent turn makes,
//! against the SQLite database named by `FORGE_PROBE_DB` — no model involved.
//! Run: FORGE_PROBE_DB=/path/to/copy.db cargo run -p services --example baseline_probe
use std::sync::Arc;

use db::{create_sqlite_pool, SqliteDb};
use forge_agent_host::{CanonicalScope, CanonicalScopeType, ForgeToolProvider, WorkspaceAccess};
use serde_json::json;
use services::CoordinationToolProvider;

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(run());
}

async fn run() {
    let db_path = std::env::var("FORGE_PROBE_DB").expect("set FORGE_PROBE_DB");
    let pool = create_sqlite_pool(&format!("sqlite:{db_path}"))
        .await
        .expect("pool opens");
    let db = Arc::new(SqliteDb::new(pool));
    let provider = CoordinationToolProvider::new(Arc::clone(&db));
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::AgentChat,
        scope_id: "84762861-af55-475d-9e7f-3582c61fbafe".to_owned(),
        workspace_access: WorkspaceAccess::Deny,
    };
    let baseline_id = uuid::Uuid::new_v4().to_string();
    let payload = json!({
        "action": "draft_revision",
        "baseline_id": baseline_id,
        "charter_revision_id": "d5ac6227-e532-40e0-b311-a2219f9a29a0",
        "content": {
            "charter_revision": {
                "artifact_id": "42c125f7-0be9-4a51-92b1-f81dc62b91ac",
                "revision_id": "d5ac6227-e532-40e0-b311-a2219f9a29a0",
                "content_digest": "981a5130476ec71e141ba858dd1f71bddd51b982c77731f6572eb0961ce61eaf",
                "render_version": "forge.project-charter/v1",
                "render_digest": "3ab3c575b2215b3833061429a7055dfe15f8806fd681c9737ede7ccf4e0e0d98"
            },
            "document_revisions": [],
            "plan_item_ids": ["pi-1","pi-2","pi-3","pi-4","pi-5","pi-6"],
            "milestone_ids": ["64d547fa-68dd-4900-9b2d-695f66dcb4c5"],
            "milestone_definition_revision_ids": ["88c10fd3-9e54-4ecc-af7e-bebd00762f05"],
            "primary_milestone_id": "64d547fa-68dd-4900-9b2d-695f66dcb4c5",
            "release_policy_revision": "forge.release-policy/v1",
            "release_policy": {
                "schema_version": "forge.execution-baseline-release-policy/v1",
                "revision": "forge.release-policy/v1",
                "required_check_definition_revisions": ["forge.check.e2e-acceptance:v1"],
                "reviewer_independence_rules": ["independent-reviewer"],
                "manual_attestation_rules": ["manual-attestation"],
                "waiver_rules": ["user-waiver"],
                "evidence_kinds": ["artifact","ci-log","media","review-report","test-report"],
                "evidence_contexts": ["commit","external","milestone","project","repository","task"],
                "evidence_freshness_rules": ["current-baseline","current-charter","current-commit","current-milestone"],
                "dependency_rules": ["dependencies-green","dependencies-reviewed","no-blocked-dependencies"],
                "stale_input_rules": ["stale-baseline-blocks","stale-evidence-blocks"],
                "forbidden_side_effects": ["credential-access","cross-project-write","force-push","merge","publish","release"],
                "known_issue_rules": ["known-issue-blocks","record-known-issue"],
                "correction_rules": ["correct-before-release","correction-required","rerun-failed-checks"],
                "purge_rules": ["purge-invalid-evidence","purge-revoked-evidence","purge-stale-evidence"]
            },
            "acceptance_evidence_matrix": [{
                "id": "acc-1",
                "description": "End-to-end todo workflow with localStorage persistence verified",
                "required": true,
                "evidence_kind": "test-report",
                "check_definition_revision": "forge.check.e2e-acceptance:v1"
            }],
            "adaptive_envelope": {
                "allowed_task_operations": [],
                "fixed_outcomes": [],
                "fixed_acceptance": [],
                "fixed_risk_classes": [],
                "forbidden_side_effects": [],
                "elevated_operations": []
            },
            "rollback_and_recovery": ["Revert to the previous commit"],
            "exclusions": []
        },
        "provenance": {
            "author": {"kind": "agent", "id": "cceb9983-0265-42c8-98d7-98e86097eb4f"},
            "change_summary": "Initial execution baseline for SimpleTodo M001"
        }
    });
    let arguments = json!({
        "operation": "project.execution_baseline",
        "payload": payload,
        "dedupe_key": format!("baseline-probe-{baseline_id}"),
        "correlation_id": format!("baseline-probe-correlation-{baseline_id}"),
    });
    match provider
        .propose(
            "cceb9983-0265-42c8-98d7-98e86097eb4f",
            &scope,
            "project.execution_baseline",
            arguments,
        )
        .await
    {
        Ok(result) => println!("OK: {result:#}"),
        Err(error) => println!("ERR: {error}"),
    }
}
