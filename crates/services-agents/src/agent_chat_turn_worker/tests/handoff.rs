//! Tests for Agent Chat Charter handoff validation and provenance.

use super::*;

#[cfg(test)]
const MAX_HANDOFF_BOUNDED_CHARS: usize = 12_000;
#[cfg(test)]
const REQUIRED_HANDOFF_REDACTION_CATEGORIES: [&str; 6] = [
    "full_main_chat_history",
    "hidden_memory_bodies",
    "credentials",
    "protected_runtime_or_browser_state",
    "unrelated_projects",
    "authority_bearing_text",
];
#[cfg(test)]
struct ProjectHandoffExpectation<'a> {
    handoff_id: &'a str,
    deduplication_key: &'a str,
    correlation_id: &'a str,
    causation_id: &'a str,
    source_chat_id: &'a str,
    source_identity_id: &'a str,
    source_profile_revision_id: &'a str,
    source_instruction_revision_id: &'a str,
    source_instruction_revision: i64,
    source_message_ids: Vec<String>,
    source_turn_id: Option<&'a str>,
    project_id: &'a str,
    project_name: &'a str,
    project_mode: &'a str,
    approved_slug: Option<&'a str>,
    target_chat_id: &'a str,
    target_binding_id: &'a str,
    target_message_id: &'a str,
    target_turn_id: &'a str,
    charter_id: &'a str,
    charter_revision_id: &'a str,
    charter_revision_number: i64,
    charter_schema_version: &'a str,
    charter_content_digest: &'a str,
    charter_render_version: &'a str,
    charter_render_digest: &'a str,
    approval_id: &'a str,
    approval_event_id: &'a str,
    approval_authorization_basis: &'a str,
    approval_authorization_action: &'a str,
    approval_authorization_event_id: &'a str,
    approval_authorization_occurred_at: &'a str,
    approval_principal_kind: &'a str,
    approval_principal_id: &'a str,
    approval_created_at: &'a str,
    create_authorization_principal_type: &'a str,
    create_authorization_principal_id: &'a str,
    create_authorization_basis: &'a str,
    create_authorization_action: &'a str,
    create_authorization_event_id: &'a str,
    create_authorization_occurred_at: &'a str,
    identity_id: &'a str,
    profile_revision_id: &'a str,
    policy_revision: &'a str,
    policy_digest: &'a str,
    created_at: &'a str,
    delivered_at: &'a str,
}

#[cfg(test)]
fn validate_project_handoff_packet(
    source_revisions_json: &str,
    expected: &ProjectHandoffExpectation<'_>,
    approval_lifecycle: &str,
    consumed_project_id: Option<&str>,
    project_id: &str,
) -> Result<ProjectCharterHandoffPacket> {
    if approval_lifecycle != "consumed" || consumed_project_id != Some(project_id) {
        return Err(ServiceError::invalid_operation(
            "Project Agent handoff requires the consumed Charter approval for this Project",
        ));
    }
    let packet = parse_project_handoff_packet(source_revisions_json)?;
    let packet_value: Value = serde_json::from_str(source_revisions_json).map_err(|_| {
        ServiceError::invalid_operation("Project Agent handoff has an invalid source manifest")
    })?;
    let source_manifest: Value = serde_json::from_str(&packet.request.source_revisions_json)
        .map_err(|_| {
            ServiceError::invalid_operation(
                "Project Agent handoff request source manifest is invalid",
            )
        })?;
    if !source_manifest.is_object() {
        return Err(ServiceError::invalid_operation(
            "Project Agent handoff request source manifest must be an object",
        ));
    }
    if !handoff_source_manifest_matches_packet(&source_manifest, &packet) {
        return Err(ServiceError::invalid_operation(
            "Project Agent handoff request source manifest does not match its canonical packet",
        ));
    }
    let recomputed_source_revisions_digest =
        handoff_request_fingerprint(&packet_value, &packet.request.authorization)?;
    let redaction_categories = {
        let mut categories = packet.redaction_manifest.excluded_categories.clone();
        categories.sort();
        categories
    };
    let mut required_categories = REQUIRED_HANDOFF_REDACTION_CATEGORIES
        .iter()
        .map(|category| (*category).to_owned())
        .collect::<Vec<_>>();
    required_categories.sort();
    let values_are_bounded = packet.bounded_summary.chars().count() <= MAX_HANDOFF_BOUNDED_CHARS
        && packet.settled_decision_ids.len() <= 64
        && packet.unresolved_items.len() <= 64
        && packet.research_references.len() <= 64
        && packet
            .unresolved_items
            .iter()
            .chain(packet.research_references.iter())
            .all(handoff_value_is_bounded)
        && handoff_text_is_safe(&packet.bounded_summary)
        && packet
            .settled_decision_ids
            .iter()
            .all(|value| non_empty(value) && handoff_text_is_safe(value));
    let redaction_shape_is_valid = redaction_categories == required_categories
        && packet
            .redaction_manifest
            .excluded_knowledge_item_ids
            .iter()
            .all(|value| non_empty(value))
        && packet
            .redaction_manifest
            .excluded_knowledge_item_ids
            .iter()
            .collect::<BTreeSet<_>>()
            .len()
            == packet.redaction_manifest.excluded_knowledge_item_ids.len();
    let matches = values_are_bounded
        && redaction_shape_is_valid
        && packet.schema_version == PROJECT_HANDOFF_SCHEMA_VERSION
        && packet.handoff_id == expected.handoff_id
        && packet.deduplication_key == expected.deduplication_key
        && packet.correlation_id == expected.correlation_id
        && packet.causation_id == expected.causation_id
        && packet.approval_id == expected.approval_id
        && non_empty(&packet.request.policy_revision)
        && packet.request.policy_revision == expected.policy_revision
        && non_empty(&packet.request.policy_digest)
        && packet.request.policy_digest == expected.policy_digest
        && non_empty(&packet.request.source_revisions_digest)
        && packet.request.source_revisions_digest == recomputed_source_revisions_digest
        && non_empty(&packet.request.source_revisions_json)
        && non_empty(&packet.request.authorization.principal_type)
        && packet.request.authorization.principal_type == expected.approval_principal_kind
        && non_empty(&packet.request.authorization.principal_id)
        && packet.request.authorization.principal_id == expected.approval_principal_id
        && packet.request.authorization.principal_type
            == expected.create_authorization_principal_type
        && packet.request.authorization.principal_id == expected.create_authorization_principal_id
        && packet.request.authorization.authorization_basis == expected.create_authorization_basis
        && packet.request.authorization.action == expected.create_authorization_action
        && packet.request.authorization.event_id == expected.create_authorization_event_id
        && packet.request.authorization.occurred_at == expected.create_authorization_occurred_at
        && packet.source.chat_id == expected.source_chat_id
        && packet.source.identity_id == expected.source_identity_id
        && packet.source.profile_revision_id == expected.source_profile_revision_id
        && packet.source.instruction_revision_id == expected.source_instruction_revision_id
        && packet.source.instruction_revision == expected.source_instruction_revision
        && packet.source.message_ids == expected.source_message_ids
        && packet
            .source
            .message_ids
            .last()
            .is_some_and(|last| last == &packet.source.message_id)
        && packet.source.turn_id.as_deref() == expected.source_turn_id
        && packet.project.id == expected.project_id
        && packet.project.name == expected.project_name
        && packet.project.lifecycle == "active"
        && packet.project.mode == expected.project_mode
        && packet.project.approved_slug.as_deref() == expected.approved_slug
        && packet.target.chat_id == expected.target_chat_id
        && packet.target.binding_id == expected.target_binding_id
        && packet.target.message_id == expected.target_message_id
        && packet.target.turn_id == expected.target_turn_id
        && packet.target.identity_id == expected.identity_id
        && packet.target.profile_revision_id == expected.profile_revision_id
        && packet.charter.id == expected.charter_id
        && packet.charter.revision_id == expected.charter_revision_id
        && packet.charter.revision_number == expected.charter_revision_number
        && packet.charter.schema_version == expected.charter_schema_version
        && packet.charter.content_digest == expected.charter_content_digest
        && packet.charter.render_version == expected.charter_render_version
        && packet.charter.render_digest == expected.charter_render_digest
        && packet.approval.id == expected.approval_id
        && packet.approval.event_id == expected.approval_event_id
        && packet.approval.authorization_basis == expected.approval_authorization_basis
        && packet.approval.authorization_action == expected.approval_authorization_action
        && packet.approval.authorization_event_id == expected.approval_authorization_event_id
        && packet.approval.authorization_occurred_at == expected.approval_authorization_occurred_at
        && packet.approval.approved_by.kind == expected.approval_principal_kind
        && packet.approval.approved_by.id == expected.approval_principal_id
        && packet.approval.approved_at == expected.approval_created_at
        && packet.project_agent.identity_id == expected.identity_id
        && packet.project_agent.profile_revision_id == expected.profile_revision_id
        // The immutable packet records the skill revision current at handoff
        // time. The skill body is server-owned; the binding must carry the
        // exact current server contract (validated separately), so the packet
        // only needs to name a revision of the same server-owned skill key.
        && packet
            .project_agent
            .operating_skill_revision
            .starts_with(&format!("{PROJECT_OPERATING_SKILL_KEY}@"))
        && packet.project_agent.policy_revision == expected.policy_revision
        && packet.project_agent.policy_digest == expected.policy_digest
        && packet.content_classification == "approved_project_charter"
        && packet.created_at == expected.created_at
        && packet.delivery.delivered_at == expected.delivered_at
        && non_empty(&packet.handoff_id)
        && non_empty(&packet.deduplication_key)
        && non_empty(&packet.correlation_id)
        && non_empty(&packet.causation_id)
        && non_empty(&packet.source.chat_id)
        && non_empty(&packet.source.identity_id)
        && non_empty(&packet.source.profile_revision_id)
        && non_empty(&packet.source.instruction_revision_id)
        && non_empty(&packet.source.message_id)
        && packet
            .source
            .message_ids
            .iter()
            .all(|value| non_empty(value))
        && non_empty(&packet.project.id)
        && non_empty(&packet.project.name)
        && non_empty(&packet.target.chat_id)
        && non_empty(&packet.target.binding_id)
        && non_empty(&packet.target.identity_id)
        && non_empty(&packet.target.profile_revision_id)
        && non_empty(&packet.target.message_id)
        && non_empty(&packet.target.turn_id)
        && non_empty(&packet.charter.id)
        && non_empty(&packet.charter.revision_id)
        && non_empty(&packet.charter.schema_version)
        && non_empty(&packet.charter.content_digest)
        && non_empty(&packet.charter.render_version)
        && non_empty(&packet.charter.render_digest)
        && non_empty(&packet.approval.id)
        && non_empty(&packet.approval.event_id)
        && non_empty(&packet.approval.authorization_basis)
        && non_empty(&packet.approval.authorization_action)
        && non_empty(&packet.approval.authorization_event_id)
        && non_empty(&packet.approval.authorization_occurred_at)
        && non_empty(&packet.approval.approved_by.kind)
        && non_empty(&packet.approval.approved_by.id)
        && non_empty(&packet.approval.approved_at)
        && non_empty(&packet.project_agent.identity_id)
        && non_empty(&packet.project_agent.profile_revision_id)
        && non_empty(&packet.project_agent.operating_skill_revision)
        && non_empty(&packet.project_agent.policy_revision)
        && non_empty(&packet.project_agent.policy_digest)
        && non_empty(&packet.content_classification)
        && non_empty(&packet.created_at)
        && non_empty(&packet.delivery.delivered_at);
    if !matches {
        return Err(ServiceError::invalid_operation(
            "Project Agent handoff Charter, approval, selected agent, or policy references are stale or mismatched",
        ));
    }
    Ok(packet)
}

#[cfg(test)]
fn handoff_source_manifest_matches_packet(
    source_manifest: &Value,
    packet: &ProjectCharterHandoffPacket,
) -> bool {
    let Ok(mut expected) = serde_json::to_value(packet) else {
        return false;
    };
    let Some(expected) = expected.as_object_mut() else {
        return false;
    };
    expected.remove("request");
    expected.remove("approval_id");
    if let Some(target) = expected.get_mut("target").and_then(Value::as_object_mut) {
        target.insert("chat_id".to_owned(), Value::Null);
    }
    if let Some(source) = expected.get_mut("source").and_then(Value::as_object_mut) {
        source.remove("message_id");
    }
    if let Some(delivery) = expected.get_mut("delivery").and_then(Value::as_object_mut) {
        delivery.insert("delivered_at".to_owned(), Value::Null);
    }
    canonicalize_project_handoff_json(&Value::Object(expected.clone()))
        == canonicalize_project_handoff_json(source_manifest)
}

/// Reproduce the DB transaction's immutable handoff-request fingerprint after
/// removing values allocated by that transaction.  The fingerprint therefore
/// proves the complete bounded packet shape and the exact authorization
/// envelope without treating a caller-controlled digest as authority.
#[cfg(test)]
fn handoff_request_fingerprint(
    value: &Value,
    authorization: &ProjectCharterHandoffAuthorization,
) -> Result<String> {
    let source_revisions_json = value
        .pointer("/request/source_revisions_json")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project Agent handoff request source manifest is required",
            )
        })?;
    let authorization = serde_json::to_value(authorization).map_err(|error| {
        ServiceError::invalid_operation(format!(
            "Project Agent handoff authorization is invalid: {error}"
        ))
    })?;
    project_handoff_request_fingerprint(value, source_revisions_json, &authorization)
        .map_err(ServiceError::invalid_operation)
}

#[cfg(test)]
fn non_empty(value: &str) -> bool {
    !value.trim().is_empty()
}

#[cfg(test)]
fn handoff_text_is_safe(value: &str) -> bool {
    if value.chars().count() > MAX_HANDOFF_BOUNDED_CHARS {
        return false;
    }
    if value.trim().is_empty() {
        return true;
    }
    guard_agent_chat_content(value).is_ok()
}

#[cfg(test)]
fn handoff_value_is_bounded(value: &Value) -> bool {
    let Ok(serialized) = serde_json::to_string(value) else {
        return false;
    };
    serialized.chars().count() <= MAX_HANDOFF_BOUNDED_CHARS && handoff_text_is_safe(&serialized)
}

fn handoff_packet_json() -> String {
    let mut packet = serde_json::json!({
        "schema_version": PROJECT_HANDOFF_SCHEMA_VERSION,
        "handoff_id": "handoff-1",
        "deduplication_key": "dedupe-1",
        "correlation_id": "correlation-1",
        "causation_id": "event-1",
        "approval_id": "approval-3",
        "request": {
            "policy_revision": "forge.project-agent-policy/v1",
            "policy_digest": "policy-digest-7",
            "source_revisions_digest": "source-revisions-digest-1",
            "source_revisions_json": "{\"schema_version\":\"forge.project-charter-handoff/v1\"}",
            "authorization": {
                "principal_type": "user",
                "principal_id": "account-1",
                "authorization_basis": "approved_charter",
                "action": "create_project_from_charter_approval",
                "event_id": "approval-event-4",
                "occurred_at": "2026-08-13T00:00:00Z"
            }
        },
        "source": {
            "chat_id": "main-chat-1",
            "message_ids": ["message-1", "message-2"],
            "message_id": "message-2",
            "turn_id": "source-turn-1",
            "identity_id": "main-identity-1",
            "profile_revision_id": "main-profile-1",
            "instruction_revision_id": "instruction-1",
            "instruction_revision": 3
        },
        "project": {
            "id": "project-8",
            "name": "Project Eight",
            "lifecycle": "active",
            "mode": "standard",
            "approved_slug": "project-eight"
        },
        "target": {
            "chat_id": "project-chat-8",
            "binding_id": "binding-8",
            "identity_id": "identity-5",
            "profile_revision_id": "profile-6",
            "message_id": "target-message-8",
            "turn_id": "target-turn-8"
        },
        "charter": {
            "id": "charter-1",
            "revision_id": "revision-2",
            "revision_number": 2,
            "schema_version": "forge.project-charter/v1",
            "content_digest": "content-digest-2",
            "render_version": "forge-project-charter/v1",
            "render_digest": "render-digest-2"
        },
        "approval": {
            "id": "approval-3",
            "event_id": "approval-event-4",
            "authorization_basis": "approved_charter",
            "authorization_action": "project.charter.approve",
            "authorization_event_id": "charter-authorization-event-4",
            "authorization_occurred_at": "2026-08-12T23:59:59Z",
            "approved_by": {
                "kind": "user",
                "id": "account-1"
            },
            "approved_at": "2026-08-13T00:00:00Z"
        },
        "project_agent": {
            "identity_id": "identity-5",
            "profile_revision_id": "profile-6",
            "operating_skill_revision": "forge.project.orchestration/v1@2",
            "policy_revision": "forge.project-agent-policy/v1",
            "policy_digest": "policy-digest-7"
        },
        "bounded_summary": "Approved Project handoff summary.",
        "settled_decision_ids": ["decision-1"],
        "unresolved_items": [{"id": "unresolved-1", "question": "Which low-risk option should be tested?"}],
        "research_references": [{"url": "https://example.test/reference", "claim": "bounded public reference"}],
        "content_classification": "approved_project_charter",
        "redaction_manifest": {
            "excluded_knowledge_item_ids": ["memory-1"],
            "excluded_categories": [
                "full_main_chat_history",
                "hidden_memory_bodies",
                "credentials",
                "protected_runtime_or_browser_state",
                "unrelated_projects",
                "authority_bearing_text"
            ]
        },
        "created_at": "2026-08-13T00:00:00Z",
        "delivery": {
            "delivered_at": "2026-08-13T00:00:01Z"
        }
    });
    let authorization = ProjectCharterHandoffAuthorization {
        principal_type: "user".to_owned(),
        principal_id: "account-1".to_owned(),
        authorization_basis: "create_project_from_approved_charter".to_owned(),
        action: "create_project_from_charter_approval".to_owned(),
        event_id: "approval-event-4".to_owned(),
        occurred_at: "2026-08-13T00:00:00Z".to_owned(),
    };
    packet["request"]["authorization"] = serde_json::to_value(&authorization).expect("auth");
    let mut source_manifest = packet.clone();
    source_manifest
        .as_object_mut()
        .expect("source manifest object")
        .remove("request");
    source_manifest
        .as_object_mut()
        .expect("source manifest object")
        .remove("approval_id");
    source_manifest["target"]["chat_id"] = Value::Null;
    source_manifest["source"]
        .as_object_mut()
        .expect("source object")
        .remove("message_id");
    source_manifest["delivery"]["delivered_at"] = Value::Null;
    packet["request"]["source_revisions_json"] = Value::String(source_manifest.to_string());
    packet["request"]["source_revisions_digest"] =
        Value::String(handoff_request_fingerprint(&packet, &authorization).expect("digest"));
    packet.to_string()
}

fn handoff_expectation<'a>() -> ProjectHandoffExpectation<'a> {
    ProjectHandoffExpectation {
        handoff_id: "handoff-1",
        deduplication_key: "dedupe-1",
        correlation_id: "correlation-1",
        causation_id: "event-1",
        source_chat_id: "main-chat-1",
        source_identity_id: "main-identity-1",
        source_profile_revision_id: "main-profile-1",
        source_instruction_revision_id: "instruction-1",
        source_instruction_revision: 3,
        source_message_ids: vec!["message-1".to_owned(), "message-2".to_owned()],
        source_turn_id: Some("source-turn-1"),
        project_id: "project-8",
        project_name: "Project Eight",
        project_mode: "standard",
        approved_slug: Some("project-eight"),
        target_chat_id: "project-chat-8",
        target_binding_id: "binding-8",
        target_message_id: "target-message-8",
        target_turn_id: "target-turn-8",
        charter_id: "charter-1",
        charter_revision_id: "revision-2",
        charter_revision_number: 2,
        charter_schema_version: "forge.project-charter/v1",
        charter_content_digest: "content-digest-2",
        charter_render_version: "forge-project-charter/v1",
        charter_render_digest: "render-digest-2",
        approval_id: "approval-3",
        approval_event_id: "approval-event-4",
        approval_authorization_basis: "approved_charter",
        approval_authorization_action: "project.charter.approve",
        approval_authorization_event_id: "charter-authorization-event-4",
        approval_authorization_occurred_at: "2026-08-12T23:59:59Z",
        approval_principal_kind: "user",
        approval_principal_id: "account-1",
        approval_created_at: "2026-08-13T00:00:00Z",
        create_authorization_principal_type: "user",
        create_authorization_principal_id: "account-1",
        create_authorization_basis: "create_project_from_approved_charter",
        create_authorization_action: "create_project_from_charter_approval",
        create_authorization_event_id: "approval-event-4",
        create_authorization_occurred_at: "2026-08-13T00:00:00Z",
        identity_id: "identity-5",
        profile_revision_id: "profile-6",
        policy_revision: "forge.project-agent-policy/v1",
        policy_digest: "policy-digest-7",
        created_at: "2026-08-13T00:00:00Z",
        delivered_at: "2026-08-13T00:00:01Z",
    }
}

#[test]
fn project_handoff_accepts_the_full_approved_packet_shape() {
    let packet = validate_project_handoff_packet(
        &handoff_packet_json(),
        &handoff_expectation(),
        "consumed",
        Some("project-8"),
        "project-8",
    )
    .expect("the full server-owned packet should parse and authenticate");
    assert_eq!(packet.handoff_id, "handoff-1");
    assert_eq!(packet.target.chat_id, "project-chat-8");
    assert_eq!(packet.redaction_manifest.excluded_categories.len(), 6);
}

#[test]
fn project_handoff_fingerprint_matches_database_materialized_transport_fields() {
    let packet: Value = serde_json::from_str(&handoff_packet_json()).expect("packet");
    let authorization: ProjectCharterHandoffAuthorization =
        serde_json::from_value(packet["request"]["authorization"].clone()).expect("authorization");
    let expected = packet["request"]["source_revisions_digest"]
        .as_str()
        .expect("source digest");

    let mut candidate = packet.clone();
    candidate["handoff_id"] = Value::String("handoff-allocated-later".to_owned());
    candidate["correlation_id"] = Value::String("correlation-allocated-later".to_owned());
    candidate["created_at"] = Value::String("2026-08-14T00:00:00Z".to_owned());
    candidate["project"]["id"] = Value::String("project-allocated-later".to_owned());
    candidate["target"]["chat_id"] = Value::String("chat-allocated-later".to_owned());
    candidate["target"]["binding_id"] = Value::String("binding-allocated-later".to_owned());
    candidate["target"]["message_id"] = Value::String("message-allocated-later".to_owned());
    candidate["target"]["turn_id"] = Value::String("turn-allocated-later".to_owned());
    candidate["source"]["message_id"] = Value::String("source-allocated-later".to_owned());
    candidate["delivery"]["delivered_at"] = Value::String("2026-08-14T00:00:01Z".to_owned());

    let mut nested: Value = serde_json::from_str(
        candidate["request"]["source_revisions_json"]
            .as_str()
            .expect("source manifest"),
    )
    .expect("source manifest JSON");
    nested["handoff_id"] = Value::String("nested-handoff-allocated-later".to_owned());
    nested["correlation_id"] = Value::String("nested-correlation-allocated-later".to_owned());
    nested["created_at"] = Value::String("2026-08-14T00:00:02Z".to_owned());
    nested["project"]["id"] = Value::String("nested-project-allocated-later".to_owned());
    nested["target"]["binding_id"] = Value::String("nested-binding-allocated-later".to_owned());
    nested["target"]["message_id"] = Value::String("nested-message-allocated-later".to_owned());
    nested["target"]["turn_id"] = Value::String("nested-turn-allocated-later".to_owned());
    candidate["request"]["source_revisions_json"] = Value::String(nested.to_string());

    assert_eq!(
        expected,
        handoff_request_fingerprint(&candidate, &authorization)
            .expect("database transport allocations are excluded")
    );
}

#[test]
fn project_handoff_canonicalization_is_stable_for_nested_object_order() {
    let left = serde_json::json!({
        "outer": {"z": 1, "a": [{"right": true, "left": false}]},
        "first": "value"
    });
    let right = serde_json::json!({
        "first": "value",
        "outer": {"a": [{"left": false, "right": true}], "z": 1}
    });

    assert_eq!(
        canonicalize_project_handoff_json(&left),
        canonicalize_project_handoff_json(&right)
    );
    assert_eq!(
        serde_json::to_string(&canonicalize_project_handoff_json(&left)).expect("canonical"),
        serde_json::to_string(&canonicalize_project_handoff_json(&right)).expect("canonical")
    );
}

#[test]
fn project_handoff_rejects_mismatched_charter_revision() {
    let mut packet: Value = serde_json::from_str(&handoff_packet_json()).expect("packet");
    packet["charter"]["revision_id"] = Value::String("revision-stale".to_owned());
    let error = validate_project_handoff_packet(
        &packet.to_string(),
        &handoff_expectation(),
        "consumed",
        Some("project-8"),
        "project-8",
    )
    .expect_err("stale Charter revision must fail closed");
    assert!(error.to_string().contains("Project Agent handoff"));
}

#[test]
fn project_handoff_rejects_mismatched_policy_digest() {
    let mut packet: Value = serde_json::from_str(&handoff_packet_json()).expect("packet");
    packet["project_agent"]["policy_digest"] = Value::String("policy-stale".to_owned());
    let error = validate_project_handoff_packet(
        &packet.to_string(),
        &handoff_expectation(),
        "consumed",
        Some("project-8"),
        "project-8",
    )
    .expect_err("stale policy digest must fail closed");
    assert!(error.to_string().contains("Project Agent handoff"));
}

#[test]
fn project_handoff_rejects_tampered_source_manifest_digest() {
    let mut packet: Value = serde_json::from_str(&handoff_packet_json()).expect("packet");
    packet["request"]["source_revisions_json"] =
        Value::String("{\"schema_version\":\"tampered\"}".to_owned());
    let error = validate_project_handoff_packet(
        &packet.to_string(),
        &handoff_expectation(),
        "consumed",
        Some("project-8"),
        "project-8",
    )
    .expect_err("a source-manifest mutation must invalidate its digest");
    assert!(error.to_string().contains("Project Agent handoff"));
}

#[test]
fn project_handoff_rejects_semantically_tampered_source_manifest_even_with_new_digest() {
    let mut packet: Value = serde_json::from_str(&handoff_packet_json()).expect("packet");
    let mut source_manifest: Value = serde_json::from_str(
        packet["request"]["source_revisions_json"]
            .as_str()
            .expect("source manifest"),
    )
    .expect("source manifest JSON");
    source_manifest["bounded_summary"] = Value::String("tampered summary".to_owned());
    packet["request"]["source_revisions_json"] = Value::String(source_manifest.to_string());
    let authorization: ProjectCharterHandoffAuthorization =
        serde_json::from_value(packet["request"]["authorization"].clone()).expect("authorization");
    packet["request"]["source_revisions_digest"] =
        Value::String(handoff_request_fingerprint(&packet, &authorization).expect("digest"));
    let error = validate_project_handoff_packet(
        &packet.to_string(),
        &handoff_expectation(),
        "consumed",
        Some("project-8"),
        "project-8",
    )
    .expect_err("a semantically altered source manifest must fail closed");
    assert!(error.to_string().contains("Project Agent handoff"));
}

#[test]
fn project_handoff_rejects_tampered_approval_authorization() {
    let mut packet: Value = serde_json::from_str(&handoff_packet_json()).expect("packet");
    packet["approval"]["authorization_action"] = Value::String("project.charter.revoke".to_owned());
    let error = validate_project_handoff_packet(
        &packet.to_string(),
        &handoff_expectation(),
        "consumed",
        Some("project-8"),
        "project-8",
    )
    .expect_err("approval authorization changes must fail closed");
    assert!(error.to_string().contains("Project Agent handoff"));
}

#[test]
fn project_handoff_rejects_tampered_create_authorization_even_with_new_digest() {
    let mut packet: Value = serde_json::from_str(&handoff_packet_json()).expect("packet");
    packet["request"]["authorization"]["action"] =
        Value::String("project.handoff.replay".to_owned());
    let authorization: ProjectCharterHandoffAuthorization =
        serde_json::from_value(packet["request"]["authorization"].clone()).expect("authorization");
    packet["request"]["source_revisions_digest"] =
        Value::String(handoff_request_fingerprint(&packet, &authorization).expect("digest"));
    let error = validate_project_handoff_packet(
        &packet.to_string(),
        &handoff_expectation(),
        "consumed",
        Some("project-8"),
        "project-8",
    )
    .expect_err("create authorization changes must fail closed");
    assert!(error.to_string().contains("Project Agent handoff"));
}

#[test]
fn project_handoff_rejects_tampered_delivery_and_redaction_shape() {
    let mut packet: Value = serde_json::from_str(&handoff_packet_json()).expect("packet");
    packet["delivery"]["delivered_at"] = Value::String("delivery-stale".to_owned());
    packet["redaction_manifest"]["excluded_categories"] =
        serde_json::json!(["full_main_chat_history"]);
    let error = validate_project_handoff_packet(
        &packet.to_string(),
        &handoff_expectation(),
        "consumed",
        Some("project-8"),
        "project-8",
    )
    .expect_err("delivery and redaction provenance must fail closed");
    assert!(error.to_string().contains("Project Agent handoff"));
}

#[test]
fn project_handoff_rejects_unknown_fields_without_discarding_them() {
    let mut packet: Value = serde_json::from_str(&handoff_packet_json()).expect("packet");
    packet["source"]["untrusted_extra"] = Value::String("must reject".to_owned());
    let error = validate_project_handoff_packet(
        &packet.to_string(),
        &handoff_expectation(),
        "consumed",
        Some("project-8"),
        "project-8",
    )
    .expect_err("unknown packet fields must not be discarded");
    assert!(error.to_string().contains("invalid typed Charter packet"));
}

#[test]
fn project_handoff_requires_the_v076_approval_and_request_envelope() {
    for field in ["approval_id", "request"] {
        let mut packet: Value = serde_json::from_str(&handoff_packet_json()).expect("packet");
        packet.as_object_mut().expect("object").remove(field);
        let error = validate_project_handoff_packet(
            &packet.to_string(),
            &handoff_expectation(),
            "consumed",
            Some("project-8"),
            "project-8",
        )
        .expect_err("required V076 handoff fields must not be optional");
        assert!(error.to_string().contains("invalid typed Charter packet"));
    }
}

#[test]
fn legacy_setup_instruction_only_allows_read_and_adoption_proposals() {
    assert!(PROJECT_SETUP_RESTRICTIONS.contains("propose an adoption Charter"));
    assert!(PROJECT_SETUP_RESTRICTIONS.contains("Do not create or dispatch implementation Tasks"));
    assert!(PROJECT_SETUP_RESTRICTIONS.contains("repository Workspace"));
    assert_eq!(
        PROJECT_SETUP_PERMISSION_CEILING,
        "read_project,read_agent_chat,read_memory,propose_message,propose_project"
    );
    assert!(!PROJECT_SETUP_PERMISSION_CEILING.contains("propose_task"));
    // The card shows the same five names, folded.
    assert_eq!(
        readable_permission_ceiling(PROJECT_SETUP_PERMISSION_CEILING),
        "propose_{message,project}, read_{agent_chat,memory,project}"
    );
}

#[test]
fn permission_ceiling_folds_shared_prefixes_and_keeps_every_name() {
    // Expanding `prefix_{a,b}` gives back exactly the listed names.
    fn expand(rendered: &str) -> Vec<String> {
        let mut names = Vec::new();
        let mut rest = rendered;
        while !rest.is_empty() {
            let (entry, tail) = match (rest.find('{'), rest.find(", ")) {
                (Some(open), comma) if comma.is_none_or(|comma| open < comma) => {
                    let close = rest.find('}').expect("closed group");
                    let prefix = &rest[..open];
                    for member in rest[open + 1..close].split(',') {
                        names.push(format!("{prefix}{member}"));
                    }
                    (None, rest[close + 1..].trim_start_matches(", "))
                }
                (_, Some(comma)) => (Some(&rest[..comma]), &rest[comma + 2..]),
                (_, None) => (Some(rest), ""),
            };
            names.extend(entry.map(str::to_owned));
            rest = tail;
        }
        names
    }

    let granted = [
        "read_task",
        "propose_task",
        "read_account",
        "task_write",
        "task_read",
        "propose_memory",
        "solo",
        "read_agent_chat",
    ];
    let json = serde_json::json!({ "allowed": granted }).to_string();
    let rendered = readable_permission_ceiling(&json);
    assert_eq!(
        rendered,
        "allowed: propose_{memory,task}, read_{account,agent_chat,task}, solo, task_{read,write}"
    );
    let mut expected = granted.map(str::to_owned).to_vec();
    expected.sort();
    assert_eq!(expand(rendered.trim_start_matches("allowed: ")), expected);

    // One name under a prefix stays as written, in both stored forms.
    assert_eq!(
        readable_permission_ceiling(r#"["read_project","propose_task"]"#),
        "propose_task, read_project"
    );
    assert_eq!(
        readable_permission_ceiling("read_project, propose_task"),
        "propose_task, read_project"
    );
    // Entries that are not plain permission names are never folded.
    assert_eq!(
        readable_permission_ceiling(r#"["read a","read b"]"#),
        "read a, read b"
    );
    assert_eq!(readable_permission_ceiling("{}"), "");
    assert_eq!(readable_permission_ceiling("[]"), "");
}

#[test]
fn main_genesis_context_provenance_names_skill_instruction_and_charter() {
    let sources = main_operating_context_sources(
        MAIN_OPERATING_SKILL_KEY,
        "forge.main.project-discovery/v2@1",
        "main-skill-content-digest",
        "server_owned_main_genesis_operating_skill",
        "main_genesis_context",
        &[
            OperatingContextReference::included(
                "genesis-1",
                "main_genesis",
                "v2",
                "genesis-digest",
                "test",
            ),
            OperatingContextReference::included(
                "instruction-1",
                "main_genesis_instruction",
                "3",
                "instruction-digest",
                "test",
            ),
            OperatingContextReference::included(
                "charter-1",
                "main_charter",
                "revision-2",
                "charter-digest",
                "test",
            ),
            OperatingContextReference::included(
                "project-1",
                "main_portfolio_projection",
                "v4:2026-08-13T00:00:00Z",
                "portfolio-digest",
                "bounded_account_portfolio_projection",
            ),
        ],
    )
    .expect("context source ordinals fit");
    assert_eq!(
        sources[0].source_id,
        "operating_skill:forge.main.project-discovery/v2"
    );
    assert_eq!(
        sources[0].source_revision,
        "forge.main.project-discovery/v2@1"
    );
    assert_eq!(sources[0].fragment_fingerprint, "main-skill-content-digest");
    assert!(sources
        .iter()
        .any(|source| source.source_type == "main_genesis_instruction"));
    assert!(sources.iter().any(|source| {
        source.source_type == "main_portfolio_projection"
            && source.disposition == "included"
            && source.sensitivity != "secret"
    }));
}

#[test]
fn main_baseline_context_provenance_names_skill_profile_and_portfolio() {
    let sources = main_operating_context_sources(
        MAIN_BASELINE_OPERATING_SKILL_KEY,
        MAIN_BASELINE_OPERATING_SKILL_REVISION,
        MAIN_BASELINE_OPERATING_SKILL_CONTENT_DIGEST,
        "server_owned_main_baseline_operating_skill",
        "main_baseline_context",
        &[
            OperatingContextReference::included(
                "profile-1",
                "main_profile",
                "v2",
                "profile-digest",
                "authenticated_main_profile",
            ),
            OperatingContextReference::included(
                "project-1",
                "main_portfolio_projection",
                "v4:2026-08-13T00:00:00Z",
                "portfolio-digest",
                "bounded_account_portfolio_projection",
            ),
        ],
    )
    .expect("context source ordinals fit");
    assert_eq!(
        sources[0].source_id,
        "operating_skill:forge.main.baseline/v1"
    );
    assert_eq!(
        sources[0].source_revision,
        MAIN_BASELINE_OPERATING_SKILL_REVISION
    );
    assert_eq!(
        sources[0].selection_reason,
        "server_owned_main_baseline_operating_skill"
    );
    assert_eq!(
        sources[1].source_id,
        "main_baseline_context:main_profile:profile-1"
    );
    assert!(sources.iter().any(|source| {
        source.source_type == "main_portfolio_projection" && source.disposition == "included"
    }));
}

#[test]
fn server_context_manifest_identity_is_stable_per_turn_job() {
    let first = agent_chat_server_manifest_id("identity-1", "session-1", "job-1");
    let replay = agent_chat_server_manifest_id("identity-1", "session-1", "job-1");
    let next_job = agent_chat_server_manifest_id("identity-1", "session-1", "job-2");
    let other_session = agent_chat_server_manifest_id("identity-1", "session-2", "job-1");

    assert_eq!(first, replay);
    assert_ne!(first, next_job);
    assert_ne!(first, other_session);
}

#[test]
fn server_context_manifest_fingerprint_shape_excludes_context_bodies() {
    let source = ContextSourceInput {
        ordinal: 0,
        source_id: "main_genesis_context:main_genesis:genesis-1@v2".to_owned(),
        source_type: "main_genesis".to_owned(),
        source_revision: "v2:discovering".to_owned(),
        selection_reason: "active_product_genesis".to_owned(),
        disposition: "included".to_owned(),
        retention_priority: 100,
        fragment_fingerprint: "digest-1".to_owned(),
        sensitivity: "internal".to_owned(),
    };
    let encoded = serde_json::to_string(&canonical_operating_context_sources(&[source]))
        .expect("canonical source metadata serializes");

    assert!(encoded.contains("main_genesis_context:main_genesis:genesis-1@v2"));
    assert!(encoded.contains("digest-1"));
    assert!(!encoded.contains("body"));
    assert!(!encoded.contains("private transcript"));
}

#[test]
fn project_handoff_rejects_active_approval() {
    let error = validate_project_handoff_packet(
        &handoff_packet_json(),
        &handoff_expectation(),
        "active",
        None,
        "project-8",
    )
    .expect_err("an active approval must not admit a Project turn");
    assert!(error.to_string().contains("consumed Charter approval"));
}
