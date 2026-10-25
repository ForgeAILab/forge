//! Tests for Project canonical digests and closed orchestration contracts.

use super::*;
use serde_json::json;

#[test]
fn canonical_json_sorts_nested_object_keys_without_reordering_arrays() {
    let left = json!({
        "z": {"b": 2, "a": 1},
        "items": [{"second": true, "first": false}]
    });
    let right = json!({
        "items": [{"first": false, "second": true}],
        "z": {"a": 1, "b": 2}
    });

    assert_eq!(
        canonical_json(&left).unwrap(),
        canonical_json(&right).unwrap()
    );
    assert_eq!(
        canonical_digest(&left).unwrap(),
        canonical_digest(&right).unwrap()
    );

    let reversed = json!({"items": [{"first": true, "second": false}], "z": {"a": 1, "b": 2}});
    assert_ne!(
        canonical_digest(&left).unwrap(),
        canonical_digest(&reversed).unwrap()
    );
}

#[test]
fn schema_version_is_part_of_the_digest_domain() {
    let value = json!({"name": "forge"});
    assert_ne!(
        canonical_digest_with_schema("schema/a", &value).unwrap(),
        canonical_digest_with_schema("schema/b", &value).unwrap()
    );
}

#[test]
fn rendered_view_digest_includes_render_version() {
    assert_ne!(
        canonical_render_digest("render/v1", "# Forge").unwrap(),
        canonical_render_digest("render/v2", "# Forge").unwrap()
    );
}

#[test]
fn canonical_digest_is_sha256_hex() {
    let digest = canonical_digest(&json!({"a": 1})).unwrap();
    assert_eq!(digest.len(), 64);
    assert!(digest.bytes().all(|byte| byte.is_ascii_hexdigit()));
}

#[test]
fn nested_authorization_unknown_fields_are_rejected() {
    let value = json!({
        "expected_version": 1,
        "idempotency_key": "mutation-1",
        "authorization": {
            "principal": {
                "kind": "user",
                "id": "user-1",
                "unexpected": "must fail"
            },
            "authorization_basis": "explicit_user_action",
            "action": "project.document.approve",
            "event_id": "event-1",
            "occurred_at": "2026-08-13T00:00:00Z"
        }
    });

    assert!(serde_json::from_value::<MutationEnvelope>(value).is_err());
}

#[test]
fn nested_charter_document_and_milestone_unknown_fields_are_rejected() {
    let mut charter = json!({
        "identity": {
            "working_name": "Forge",
            "one_line_vision": "A bounded project",
            "maturity": "mvp"
        },
        "problem_and_people": {"problem_or_opportunity": "A problem"},
        "core_experience": {"primary_outcome": "An outcome"},
        "scope": {},
        "success": {},
        "constraints_and_risks": {},
        "knowledge_ledger": {}
    });
    charter["identity"]["unexpected"] = json!(true);
    assert!(serde_json::from_value::<ProjectCharterContent>(charter).is_err());

    let mut document = json!({
        "kind": "Research",
        "content": {
            "question": "What is known?",
            "decision_informed": "A bounded decision",
            "scope": "Public sources",
            "stopping_condition": "One authoritative source"
        }
    });
    document["content"]["unexpected"] = json!("must fail");
    assert!(serde_json::from_value::<ProjectDocumentContent>(document).is_err());

    let mut milestone = json!({
        "name": "M1",
        "outcome": "A measurable outcome"
    });
    milestone["unexpected"] = json!("must fail");
    assert!(serde_json::from_value::<MilestoneDefinitionContent>(milestone).is_err());
}

#[test]
fn mutation_and_governance_envelopes_reject_unknown_fields() {
    let task_governance = json!({
        "milestone_id": "milestone-1",
        "unknown": "must fail"
    });
    assert!(serde_json::from_value::<TaskGovernanceRequest>(task_governance).is_err());
}
